package booking

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Redis Lua script for atomic admission reservation
const reserveLuaScript = `
local tokens_key = KEYS[1]
local token_id = ARGV[1]
local total_capacity = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])

-- Purge expired tokens from the set before counting
local members = redis.call('SMEMBERS', tokens_key)
for _, mid in ipairs(members) do
    if redis.call('EXISTS', 'token:' .. mid) == 0 then
        redis.call('SREM', tokens_key, mid)
    end
end

local current_reserved = tonumber(redis.call('SCARD', tokens_key))
if current_reserved >= total_capacity then
    return 0
end

redis.call('SADD', tokens_key, token_id)
redis.call('SETEX', 'token:' .. token_id, ttl_seconds, 'PENDING')
return 1
`

// Redis Lua script for idempotent release
const releaseLuaScript = `
local tokens_key = KEYS[1]
local token_id = ARGV[1]

local token_key = 'token:' .. token_id
local status = redis.call('GET', token_key)

if status == 'PENDING' then
    redis.call('SREM', tokens_key, token_id)
    redis.call('DEL', token_key)
    return 1 -- Released successfully
end

-- If already CONFIRMED or already deleted, do nothing (idempotent release)
return 0
`

// Redis Lua script for token confirmation
const confirmLuaScript = `
local token_id = ARGV[1]
local token_key = 'token:' .. token_id

local status = redis.call('GET', token_key)
if status == 'PENDING' then
    redis.call('SET', token_key, 'CONFIRMED')
    return 1
end
return 0
`

// RedisTokenStore implements TokenStore using Redis 7 and Lua
type RedisTokenStore struct {
	client *redis.Client
}

func NewRedisTokenStore(client *redis.Client) *RedisTokenStore {
	return &RedisTokenStore{client: client}
}

func (r *RedisTokenStore) AcquireToken(ctx context.Context, slotID uuid.UUID, totalCapacity int, ttl time.Duration) (*ReservationToken, error) {
	tokenID := uuid.New().String()
	tokensKey := fmt.Sprintf("slot:%s:tokens", slotID.String())
	ttlSec := int(ttl.Seconds())
	if ttlSec <= 0 {
		ttlSec = 30
	}

	res, err := r.client.Eval(ctx, reserveLuaScript, []string{tokensKey}, tokenID, totalCapacity, ttlSec).Result()
	if err != nil {
		return nil, fmt.Errorf("redis lua error on acquire: %w", err)
	}

	granted, _ := strconv.Atoi(fmt.Sprintf("%v", res))
	if granted == 0 {
		return nil, ErrCapacityExceeded
	}

	return &ReservationToken{
		TokenID:   tokenID,
		SlotID:    slotID,
		ExpiresAt: time.Now().Add(ttl),
		Confirmed: false,
	}, nil
}

func (r *RedisTokenStore) ReleaseToken(ctx context.Context, tokenID string, slotID uuid.UUID) error {
	tokensKey := fmt.Sprintf("slot:%s:tokens", slotID.String())
	_, err := r.client.Eval(ctx, releaseLuaScript, []string{tokensKey}, tokenID).Result()
	return err
}

func (r *RedisTokenStore) ConfirmToken(ctx context.Context, tokenID string, slotID uuid.UUID) error {
	_, err := r.client.Eval(ctx, confirmLuaScript, []string{}, tokenID).Result()
	return err
}

func (r *RedisTokenStore) Reconcile(ctx context.Context, slotID uuid.UUID, actualConfirmedCount int) error {
	tokensKey := fmt.Sprintf("slot:%s:tokens", slotID.String())
	// Purge expired/stale tokens from the set
	members, err := r.client.SMembers(ctx, tokensKey).Result()
	if err != nil {
		return err
	}
	for _, mid := range members {
		tokenKey := fmt.Sprintf("token:%s", mid)
		exists, _ := r.client.Exists(ctx, tokenKey).Result()
		if exists == 0 {
			// Token expired or was released; remove from set
			_ = r.client.SRem(ctx, tokensKey, mid).Err()
		}
	}
	return nil
}

// MemoryTokenStore provides a thread-safe atomic in-memory implementation
type MemoryTokenStore struct {
	mu     sync.Mutex
	tokens map[string]*ReservationToken
	slots  map[uuid.UUID]map[string]bool // slotID -> set of active tokenIDs
}

func NewMemoryTokenStore() *MemoryTokenStore {
	return &MemoryTokenStore{
		tokens: make(map[string]*ReservationToken),
		slots:  make(map[uuid.UUID]map[string]bool),
	}
}

func (m *MemoryTokenStore) AcquireToken(ctx context.Context, slotID uuid.UUID, totalCapacity int, ttl time.Duration) (*ReservationToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	activeTokens, ok := m.slots[slotID]
	if !ok {
		activeTokens = make(map[string]bool)
		m.slots[slotID] = activeTokens
	}

	// Purge expired tokens first
	now := time.Now()
	for tID := range activeTokens {
		t := m.tokens[tID]
		if t != nil && !t.Confirmed && now.After(t.ExpiresAt) {
			delete(activeTokens, tID)
			delete(m.tokens, tID)
		}
	}

	if len(activeTokens) >= totalCapacity {
		return nil, ErrCapacityExceeded
	}

	tokenID := uuid.New().String()
	token := &ReservationToken{
		TokenID:   tokenID,
		SlotID:    slotID,
		ExpiresAt: now.Add(ttl),
		Confirmed: false,
	}

	activeTokens[tokenID] = true
	m.tokens[tokenID] = token
	return token, nil
}

func (m *MemoryTokenStore) ReleaseToken(ctx context.Context, tokenID string, slotID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	token, ok := m.tokens[tokenID]
	if !ok || token.Confirmed {
		return nil // idempotent release
	}

	delete(m.tokens, tokenID)
	if active, exists := m.slots[slotID]; exists {
		delete(active, tokenID)
	}
	return nil
}

func (m *MemoryTokenStore) ConfirmToken(ctx context.Context, tokenID string, slotID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	token, ok := m.tokens[tokenID]
	if ok {
		token.Confirmed = true
	}
	return nil
}

func (m *MemoryTokenStore) Reconcile(ctx context.Context, slotID uuid.UUID, actualConfirmedCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	active, exists := m.slots[slotID]
	if !exists {
		return nil
	}

	// Keep only confirmed tokens
	for tID := range active {
		t := m.tokens[tID]
		if t == nil || !t.Confirmed {
			delete(active, tID)
			delete(m.tokens, tID)
		}
	}
	return nil
}
