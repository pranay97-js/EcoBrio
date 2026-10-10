# ECOBrio Data Model & Invariants

ECOBrio enforces data integrity through PostgreSQL constraints, transactional outbox patterns, and immutable double-entry ledger bookkeeping.

---

## 1. Core Invariants

1. **Zero Overselling Invariant:**
   `collection_slots.booked_count <= collection_slots.total_capacity` enforced via check constraint `chk_slot_capacity`.
2. **Double-Entry Balance Invariant:**
   For every `journal_entries` record, the sum of credits must strictly balance the sum of debits across all child `ledger_postings`. Points are represented as `BIGINT` integer units.
3. **Idempotency Invariant:**
   Any request with a matching `(scope, key)` pair in `idempotency_records` returns the persisted prior response code and body without re-executing state transitions.
4. **Outbox Invariant:**
   All domain events are written to `outbox_events` within the same database transaction as the business entity mutation.

---

## 2. Relational Schema Summary

| Table | Primary Responsibilities | Key Constraints |
| :--- | :--- | :--- |
| `users` | Citizen, Collector, Admin identities | Unique email, role checks |
| `collection_hubs` | Physical drop-off facilities | Lat/Lon geographic coordinates |
| `collection_slots` | Time windows and capacity limits | Capacity invariant, time ordering |
| `bookings` | Confirmed citizen reservations | Unique idempotency key, Foreign Keys |
| `recycling_submissions`| QR scan material surrender | Unique QR identity, FSM state |
| `ledger_accounts` | Balance owners (system/citizen/escrow) | Unique account code, account type |
| `journal_entries` | Transaction roots | Unique idempotency key |
| `ledger_postings` | Debits/Credits | Positive amounts, journal FK |
| `idempotency_records` | API gateway replay safety | Unique scoped idempotency key |
| `incidents` | Geospatial waste tracking | Geographic index, valid state |
| `outbox_events` | Asynchronous cloud event streaming | Pending status index |
| `processed_events` | Consumer inbox deduplication | Composite primary key `(consumer, event_id)` |
| `sensor_events` | Replay destination for custom WAL | Unique `(sensor_id, sequence_id)` |
| `wal_checkpoints` | Durability recovery marker | Segment file and sequence tracking |
