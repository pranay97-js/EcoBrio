package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"ecobrio/internal/ledger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestService() (*ledger.Service, *ledger.MemoryRepository) {
	repo := ledger.NewMemoryRepository()
	service := ledger.NewService(repo)
	return service, repo
}

func TestPostBalancedTransaction_Success(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	citizenID := uuid.New()
	walletCode := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())

	req := ledger.PostTransactionRequest{
		IdempotencyKey: "tx-test-001",
		Description:    "Test credit deposit",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 100, IsCredit: false},
			{AccountCode: walletCode, Points: 100, IsCredit: true},
		},
	}

	entry, err := service.PostBalancedTransaction(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, req.IdempotencyKey, entry.IdempotencyKey)
	assert.Len(t, entry.Postings, 2)

	// Check citizen balance
	bal, err := service.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(100), bal)
}

func TestPostBalancedTransaction_UnbalancedRejected(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	citizenID := uuid.New()
	walletCode := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())

	req := ledger.PostTransactionRequest{
		IdempotencyKey: "tx-unbalanced-001",
		Description:    "Unbalanced transaction test",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 100, IsCredit: false},
			{AccountCode: walletCode, Points: 50, IsCredit: true}, // Missing 50 points
		},
	}

	_, err := service.PostBalancedTransaction(ctx, req)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ledger.ErrUnbalancedTransaction), "expected ErrUnbalancedTransaction, got: %v", err)
}

func TestPostBalancedTransaction_InvalidAmount(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	req := ledger.PostTransactionRequest{
		IdempotencyKey: "tx-invalid-amount",
		Description:    "Zero amount test",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 0, IsCredit: false},
			{AccountCode: "CITIZEN_WALLET:test", Points: 0, IsCredit: true},
		},
	}

	_, err := service.PostBalancedTransaction(ctx, req)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ledger.ErrInvalidAmount))
}

func TestPostBalancedTransaction_InsufficientBalance(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	citizenID := uuid.New()
	walletCode := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())

	// Attempting to debit 50 points from a wallet with 0 balance
	req := ledger.PostTransactionRequest{
		IdempotencyKey: "tx-insufficient",
		Description:    "Overdraw test",
		Postings: []ledger.PostingDraft{
			{AccountCode: walletCode, Points: 50, IsCredit: false},
			{AccountCode: "ESCROW_HOLD:test", Points: 50, IsCredit: true},
		},
	}

	_, err := service.PostBalancedTransaction(ctx, req)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ledger.ErrInsufficientBalance), "expected ErrInsufficientBalance, got: %v", err)
}

func TestPostBalancedTransaction_Idempotency(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	citizenID := uuid.New()
	walletCode := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())

	req := ledger.PostTransactionRequest{
		IdempotencyKey: "tx-idempotent-repeat",
		Description:    "Repeated transaction",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 150, IsCredit: false},
			{AccountCode: walletCode, Points: 150, IsCredit: true},
		},
	}

	entry1, err := service.PostBalancedTransaction(ctx, req)
	require.NoError(t, err)

	entry2, err := service.PostBalancedTransaction(ctx, req)
	require.NoError(t, err)

	assert.Equal(t, entry1.ID, entry2.ID)

	// Balance must remain strictly 150, NOT 300
	bal, err := service.GetCitizenBalance(ctx, citizenID)
	require.NoError(t, err)
	assert.Equal(t, int64(150), bal)
}

func TestListCitizenPostings(t *testing.T) {
	service, _ := setupTestService()
	ctx := context.Background()

	citizenID := uuid.New()
	walletCode := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())

	// Two separate transactions
	_, err := service.PostBalancedTransaction(ctx, ledger.PostTransactionRequest{
		IdempotencyKey: "tx-postings-1",
		Description:    "Grant 1",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 40, IsCredit: false},
			{AccountCode: walletCode, Points: 40, IsCredit: true},
		},
	})
	require.NoError(t, err)

	_, err = service.PostBalancedTransaction(ctx, ledger.PostTransactionRequest{
		IdempotencyKey: "tx-postings-2",
		Description:    "Grant 2",
		Postings: []ledger.PostingDraft{
			{AccountCode: "SYSTEM_ISSUANCE_RESERVE", Points: 60, IsCredit: false},
			{AccountCode: walletCode, Points: 60, IsCredit: true},
		},
	})
	require.NoError(t, err)

	postings, err := service.ListCitizenPostings(ctx, citizenID)
	require.NoError(t, err)
	assert.Len(t, postings, 2)
}
