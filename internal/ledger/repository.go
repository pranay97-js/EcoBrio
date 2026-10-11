package ledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	GetOrCreateAccount(ctx context.Context, code string, accType AccountType, userID *uuid.UUID) (*Account, error)
	GetAccountByCode(ctx context.Context, code string) (*Account, error)
	PostJournalEntry(ctx context.Context, entry *JournalEntry) error
	GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error)
	GetJournalEntryByIdempotencyKey(ctx context.Context, key string) (*JournalEntry, error)
	ListPostingsByAccount(ctx context.Context, accountID uuid.UUID) ([]Posting, error)
}

// PostgresRepository implements Repository using PostgreSQL
type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) GetOrCreateAccount(ctx context.Context, code string, accType AccountType, userID *uuid.UUID) (*Account, error) {
	querySelect := `
		SELECT id, user_id, code, account_type, created_at
		FROM ledger_accounts
		WHERE code = $1
	`
	var acc Account
	err := r.pool.QueryRow(ctx, querySelect, code).Scan(&acc.ID, &acc.UserID, &acc.Code, &acc.AccountType, &acc.CreatedAt)
	if err == nil {
		return &acc, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	// Insert if not exists
	newID := uuid.New()
	queryInsert := `
		INSERT INTO ledger_accounts (id, user_id, code, account_type, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (code) DO UPDATE SET code = EXCLUDED.code
		RETURNING id, user_id, code, account_type, created_at
	`
	err = r.pool.QueryRow(ctx, queryInsert, newID, userID, code, string(accType)).Scan(
		&acc.ID, &acc.UserID, &acc.Code, &acc.AccountType, &acc.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating ledger account: %w", err)
	}
	return &acc, nil
}

func (r *PostgresRepository) GetAccountByCode(ctx context.Context, code string) (*Account, error) {
	query := `
		SELECT id, user_id, code, account_type, created_at
		FROM ledger_accounts
		WHERE code = $1
	`
	var acc Account
	err := r.pool.QueryRow(ctx, query, code).Scan(&acc.ID, &acc.UserID, &acc.Code, &acc.AccountType, &acc.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	if err != nil {
		return nil, err
	}
	return &acc, nil
}

func (r *PostgresRepository) PostJournalEntry(ctx context.Context, entry *JournalEntry) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Insert Journal Entry
	insertJournal := `
		INSERT INTO journal_entries (id, idempotency_key, description, posted_at)
		VALUES ($1, $2, $3, $4)
	`
	if _, err := tx.Exec(ctx, insertJournal, entry.ID, entry.IdempotencyKey, entry.Description, entry.PostedAt); err != nil {
		return fmt.Errorf("failed inserting journal entry: %w", err)
	}

	// 2. Insert Balanced Postings
	insertPosting := `
		INSERT INTO ledger_postings (id, journal_entry_id, account_id, amount_points, is_credit, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	for _, p := range entry.Postings {
		if _, err := tx.Exec(ctx, insertPosting, p.ID, entry.ID, p.AccountID, p.AmountPoints, p.IsCredit, p.CreatedAt); err != nil {
			return fmt.Errorf("failed inserting posting: %w", err)
		}
	}

	// Verify no citizen wallet goes negative after this transaction
	for _, p := range entry.Postings {
		if !p.IsCredit {
			// Check the running balance of the debited account within the transaction
			var balance int64
			balQuery := `SELECT COALESCE(SUM(CASE WHEN is_credit THEN amount_points ELSE -amount_points END), 0) FROM ledger_postings WHERE account_id = $1`
			if err := tx.QueryRow(ctx, balQuery, p.AccountID).Scan(&balance); err != nil {
				return fmt.Errorf("failed checking post-debit balance: %w", err)
			}
			if balance < 0 {
				return fmt.Errorf("%w: post-transaction balance would be %d for account %s", ErrInsufficientBalance, balance, p.AccountID)
			}
		}
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	// For liability/equity accounts: credits increase balance, debits decrease balance
	query := `
		SELECT COALESCE(SUM(CASE WHEN is_credit THEN amount_points ELSE -amount_points END), 0)
		FROM ledger_postings
		WHERE account_id = $1
	`
	var balance int64
	err := r.pool.QueryRow(ctx, query, accountID).Scan(&balance)
	if err != nil {
		return 0, err
	}
	return balance, nil
}

func (r *PostgresRepository) GetJournalEntryByIdempotencyKey(ctx context.Context, key string) (*JournalEntry, error) {
	queryEntry := `
		SELECT id, idempotency_key, description, posted_at
		FROM journal_entries
		WHERE idempotency_key = $1
	`
	var entry JournalEntry
	err := r.pool.QueryRow(ctx, queryEntry, key).Scan(&entry.ID, &entry.IdempotencyKey, &entry.Description, &entry.PostedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // Not found
	}
	if err != nil {
		return nil, err
	}

	// Fetch postings
	queryPostings := `
		SELECT id, journal_entry_id, account_id, amount_points, is_credit, created_at
		FROM ledger_postings
		WHERE journal_entry_id = $1
		ORDER BY created_at ASC
	`
	rows, err := r.pool.Query(ctx, queryPostings, entry.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.ID, &p.JournalEntryID, &p.AccountID, &p.AmountPoints, &p.IsCredit, &p.CreatedAt); err != nil {
			return nil, err
		}
		entry.Postings = append(entry.Postings, p)
	}
	return &entry, nil
}

func (r *PostgresRepository) ListPostingsByAccount(ctx context.Context, accountID uuid.UUID) ([]Posting, error) {
	query := `
		SELECT id, journal_entry_id, account_id, amount_points, is_credit, created_at
		FROM ledger_postings
		WHERE account_id = $1
		ORDER BY created_at DESC
		LIMIT 100
	`
	rows, err := r.pool.Query(ctx, query, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var postings []Posting
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.ID, &p.JournalEntryID, &p.AccountID, &p.AmountPoints, &p.IsCredit, &p.CreatedAt); err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}
	return postings, nil
}

// MemoryRepository provides a thread-safe in-memory double-entry store for tests
type MemoryRepository struct {
	mu             sync.RWMutex
	accounts       map[uuid.UUID]Account
	accountsByCode map[string]uuid.UUID
	journalEntries map[string]JournalEntry // by idempotency_key
	postings       map[uuid.UUID][]Posting // by accountID
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		accounts:       make(map[uuid.UUID]Account),
		accountsByCode: make(map[string]uuid.UUID),
		journalEntries: make(map[string]JournalEntry),
		postings:       make(map[uuid.UUID][]Posting),
	}
}

func (m *MemoryRepository) GetOrCreateAccount(ctx context.Context, code string, accType AccountType, userID *uuid.UUID) (*Account, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if id, exists := m.accountsByCode[code]; exists {
		acc := m.accounts[id]
		return &acc, nil
	}

	newID := uuid.New()
	acc := Account{
		ID:          newID,
		UserID:      userID,
		Code:        code,
		AccountType: accType,
		CreatedAt:   time.Now(),
	}
	m.accounts[newID] = acc
	m.accountsByCode[code] = newID
	return &acc, nil
}

func (m *MemoryRepository) GetAccountByCode(ctx context.Context, code string) (*Account, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	id, exists := m.accountsByCode[code]
	if !exists {
		return nil, ErrAccountNotFound
	}
	acc := m.accounts[id]
	return &acc, nil
}

func (m *MemoryRepository) PostJournalEntry(ctx context.Context, entry *JournalEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.journalEntries[entry.IdempotencyKey] = *entry
	for _, p := range entry.Postings {
		m.postings[p.AccountID] = append(m.postings[p.AccountID], p)
	}
	return nil
}

func (m *MemoryRepository) GetBalance(ctx context.Context, accountID uuid.UUID) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var balance int64
	for _, p := range m.postings[accountID] {
		if p.IsCredit {
			balance += p.AmountPoints
		} else {
			balance -= p.AmountPoints
		}
	}
	return balance, nil
}

func (m *MemoryRepository) GetJournalEntryByIdempotencyKey(ctx context.Context, key string) (*JournalEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.journalEntries[key]
	if !exists {
		return nil, nil
	}
	return &entry, nil
}

func (m *MemoryRepository) ListPostingsByAccount(ctx context.Context, accountID uuid.UUID) ([]Posting, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	posts, exists := m.postings[accountID]
	if !exists {
		return []Posting{}, nil
	}
	res := make([]Posting, len(posts))
	copy(res, posts)
	return res, nil
}
