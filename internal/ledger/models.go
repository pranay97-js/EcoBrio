package ledger

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrUnbalancedTransaction = errors.New("unbalanced transaction: sum of credits must equal sum of debits")
	ErrAccountNotFound       = errors.New("ledger account not found")
	ErrInsufficientBalance   = errors.New("insufficient account balance for debit posting")
	ErrInvalidAmount         = errors.New("posting amount must be a positive integer")
)

type AccountType string

const (
	AccountAsset     AccountType = "ASSET"
	AccountLiability AccountType = "LIABILITY"
	AccountEquity    AccountType = "EQUITY"
	AccountExpense   AccountType = "EXPENSE"
)

// Account represents a balance holder in the double-entry ledger
type Account struct {
	ID          uuid.UUID   `json:"id"`
	UserID      *uuid.UUID  `json:"user_id,omitempty"`
	Code        string      `json:"code"` // e.g. "SYSTEM_ISSUANCE_RESERVE", "CITIZEN_WALLET:<uuid>", "ESCROW_HOLD:<uuid>"
	AccountType AccountType `json:"account_type"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Posting represents an individual debit or credit leg of a balanced transaction
type Posting struct {
	ID             uuid.UUID `json:"id"`
	JournalEntryID uuid.UUID `json:"journal_entry_id"`
	AccountID      uuid.UUID `json:"account_id"`
	AmountPoints   int64     `json:"amount_points"` // Stored in exact integer points
	IsCredit       bool      `json:"is_credit"`
	CreatedAt      time.Time `json:"created_at"`
}

// JournalEntry represents the root of an atomic, immutable balanced transaction
type JournalEntry struct {
	ID             uuid.UUID `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Description    string    `json:"description"`
	PostedAt       time.Time `json:"posted_at"`
	Postings       []Posting `json:"postings"`
}

// PostTransactionRequest input DTO
type PostTransactionRequest struct {
	IdempotencyKey string
	Description    string
	Postings       []PostingDraft
}

// PostingDraft holds input for creating a balanced posting
type PostingDraft struct {
	AccountCode string
	Points      int64
	IsCredit    bool
}
