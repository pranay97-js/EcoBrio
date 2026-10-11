# Concurrency Booking Engine Protocol & Mathematical Invariants

**Reference:** `EcoBrio_Blueprint.pdf` Section 7  
**Guaranteed Invariant:** **0% Over-Allocation** under arbitrary concurrency ($N \gg \text{Capacity}$).

---

## 1. Sequence Diagram: Two-Tier Reservation Protocol

```mermaid
sequenceDiagram
    autonumber
    actor Citizen as Client / Mobile
    participant Ingress as HTTP Middleware
    participant Idemp as SHA-256 Idempotency Store
    participant TokenStore as Layer 1: Redis Lua Token Store
    participant DB as Layer 2: PostgreSQL Canonical DB

    Citizen->>Ingress: POST /api/v1/bookings (X-Idempotency-Key)
    Ingress->>Idemp: Check Key (Scope="booking", Key=X-Key)
    alt Cached & Committed
        Idemp-->>Citizen: 201 Created (Cached Payload)
    else First In-Flight Attempt
        Ingress->>TokenStore: EVAL Lua Script (AcquireToken, TTL=30s)
        alt Redis Slot Full
            TokenStore-->>Ingress: ErrCapacityExceeded
            Ingress-->>Citizen: 409 Conflict ("Slot capacity exhausted")
        else Lease Granted (TokenID)
            Ingress->>DB: BEGIN TX
            Ingress->>DB: UPDATE collection_slots SET booked = booked + 1, version = version + 1 WHERE id = $1 AND booked < total
            alt SQL Update Count == 0 (Capacity Exceeded / Stale Read)
                DB-->>Ingress: 0 Rows Updated
                Ingress->>TokenStore: ReleaseToken(TokenID)
                Ingress-->>Citizen: 409 Conflict ("Slot capacity exhausted")
            else SQL Update Count == 1
                Ingress->>DB: INSERT INTO bookings ...
                Ingress->>DB: INSERT INTO idempotency_records (status=COMMITTED) ...
                Ingress->>DB: COMMIT TX
                Ingress->>TokenStore: ConfirmToken(TokenID)
                Ingress-->>Citizen: 201 Created (Booking Details)
            end
        end
    end
```

---

## 2. Invariant Proofs

### Invariant 1: Slot Capacity Safety
Let $C_{\text{total}}$ be the slot's total capacity, and $B_{\text{committed}}$ be the number of confirmed bookings.
$$\forall t \ge 0, \quad B_{\text{committed}}(t) \le C_{\text{total}}$$

**Proof:**
1. Suppose $B_{\text{committed}} = C_{\text{total}}$.
2. A new transaction executes the atomic decrement query with condition:
   $$\text{WHERE booked\_count} < \text{total\_capacity}$$
3. Because $B_{\text{committed}} = C_{\text{total}}$, the conditional evaluation in Postgres evaluates to `FALSE`.
4. Postgres row update count is $0$.
5. The transaction rolls back and aborts without inserting a booking record.
6. Therefore, $B_{\text{committed}}(t) \le C_{\text{total}}$ holds invariant under all interleavings.

### Invariant 2: Drift Reconciliation
If an unhandled server crash occurs between step 5 (Redis token leased) and step 8 (PostgreSQL transaction commit):
1. The 30-second Redis TTL expires, automatically freeing the uncommitted lease.
2. Background reconciler executes every 30 seconds:
   $$\text{reconcile}(\text{slotID}) = \text{SetRedisBookedCount}(\text{Count}_{\text{Postgres}}(\text{slotID}))$$
3. Prevents permanent capacity leakage.
