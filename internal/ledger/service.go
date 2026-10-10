package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// PostBalancedTransaction validates and commits an immutable double-entry journal entry
func (s *Service) PostBalancedTransaction(ctx context.Context, req PostTransactionRequest) (*JournalEntry, error) {
	key := strings.TrimSpace(req.IdempotencyKey)
	if key == "" {
		return nil, fmt.Errorf("idempotency_key is required")
	}

	// 1. Idempotency Check: Return existing journal entry if already committed
	existing, err := s.repo.GetJournalEntryByIdempotencyKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("error checking idempotency: %w", err)
	}
	if existing != nil {
		return existing, nil
	}

	if len(req.Postings) < 2 {
		return nil, fmt.Errorf("%w: at least two postings are required for double-entry", ErrUnbalancedTransaction)
	}

	var sumCredits, sumDebits int64
	var postings []Posting

	journalID := uuid.New()
	now := time.Now()

	// 2. Validate amounts and verify balanced debit/credit sums
	for _, draft := range req.Postings {
		if draft.Points <= 0 {
			return nil, ErrInvalidAmount
		}
		if draft.IsCredit {
			sumCredits += draft.Points
		} else {
			sumDebits += draft.Points
		}

		acc, err := s.repo.GetAccountByCode(ctx, draft.AccountCode)
		if err != nil {
			if errors.Is(err, ErrAccountNotFound) {
				accType := AccountLiability
				if strings.HasPrefix(draft.AccountCode, "SYSTEM_") {
					accType = AccountEquity
				}
				var uid *uuid.UUID
				if strings.HasPrefix(draft.AccountCode, "CITIZEN_WALLET:") || strings.HasPrefix(draft.AccountCode, "ESCROW_HOLD:") {
					parts := strings.Split(draft.AccountCode, ":")
					if len(parts) == 2 {
						if parsed, pErr := uuid.Parse(parts[1]); pErr == nil {
							uid = &parsed
						}
					}
				}
				acc, err = s.repo.GetOrCreateAccount(ctx, draft.AccountCode, accType, uid)
				if err != nil {
					return nil, fmt.Errorf("failed auto-creating account '%s': %w", draft.AccountCode, err)
				}
			} else {
				return nil, fmt.Errorf("account '%s': %w", draft.AccountCode, err)
			}
		}

		// Balance safety check: If debiting a citizen wallet, ensure sufficient balance
		if !draft.IsCredit && strings.HasPrefix(acc.Code, "CITIZEN_WALLET:") {
			bal, err := s.repo.GetBalance(ctx, acc.ID)
			if err != nil {
				return nil, err
			}
			if bal < draft.Points {
				return nil, fmt.Errorf("%w: account has %d, attempted debit %d", ErrInsufficientBalance, bal, draft.Points)
			}
		}

		postings = append(postings, Posting{
			ID:             uuid.New(),
			JournalEntryID: journalID,
			AccountID:      acc.ID,
			AmountPoints:   draft.Points,
			IsCredit:       draft.IsCredit,
			CreatedAt:      now,
		})
	}

	// 3. Strict Double-Entry Invariant Check: Sum(Credits) == Sum(Debits)
	if sumCredits != sumDebits {
		return nil, fmt.Errorf("%w: credits total %d, debits total %d", ErrUnbalancedTransaction, sumCredits, sumDebits)
	}

	entry := &JournalEntry{
		ID:             journalID,
		IdempotencyKey: key,
		Description:    req.Description,
		PostedAt:       now,
		Postings:       postings,
	}

	if err := s.repo.PostJournalEntry(ctx, entry); err != nil {
		return nil, fmt.Errorf("failed committing journal entry: %w", err)
	}

	return entry, nil
}

// GetCitizenBalance returns the spendable integer points balance for a citizen
func (s *Service) GetCitizenBalance(ctx context.Context, citizenID uuid.UUID) (int64, error) {
	code := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())
	acc, err := s.repo.GetOrCreateAccount(ctx, code, AccountLiability, &citizenID)
	if err != nil {
		return 0, err
	}
	return s.repo.GetBalance(ctx, acc.ID)
}

// ListCitizenPostings retrieves recent ledger audit history for a citizen
func (s *Service) ListCitizenPostings(ctx context.Context, citizenID uuid.UUID) ([]Posting, error) {
	code := fmt.Sprintf("CITIZEN_WALLET:%s", citizenID.String())
	acc, err := s.repo.GetOrCreateAccount(ctx, code, AccountLiability, &citizenID)
	if err != nil {
		return nil, err
	}
	return s.repo.ListPostingsByAccount(ctx, acc.ID)
}
