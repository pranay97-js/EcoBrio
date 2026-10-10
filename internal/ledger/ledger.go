package ledger

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// AccountType defines standard financial ledger account types
type AccountType string

const (
	AccountAsset     AccountType = "ASSET"
	AccountLiability AccountType = "LIABILITY"
	AccountEquity    AccountType = "EQUITY"
	AccountExpense   AccountType = "EXPENSE"
)

// JournalEntry root record of an atomic financial event
type JournalEntry struct {
	ID             uuid.UUID `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Description    string    `json:"description"`
	PostedAt       time.Time `json:"posted_at"`
}

// Posting represents one leg of a balanced transaction
type Posting struct {
	AccountID uuid.UUID `json:"account_id"`
	Points    int64     `json:"points"`
	IsCredit  bool      `json:"is_credit"`
}

// Service provides strict balanced double-entry mutations
type Service interface {
	PostTransaction(ctx context.Context, idempotencyKey, description string, postings []Posting) (*JournalEntry, error)
	GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error)
}
