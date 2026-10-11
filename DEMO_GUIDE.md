# ECOBrio — 5-Minute Demonstration Plan & Interview Guide

**Document Purpose:** Exact 5-minute technical demo script and interview positioning for Tier-1 systems engineering roles (Rubrik, JusPay, Booking.com).  
**Blueprint Reference:** `EcoBrio_Blueprint.pdf` Section 20.

---

## 1. Five-Minute Technical Walkthrough Script

| Timestamp | Phase & Focus | Exact Action / Command | Expected Output & Architectural Talking Point |
|---|---|---|---|
| **0:00 - 0:40** | **System Overview & Ingress** | Open `http://localhost:8080` in browser. Show Leaflet live map, smart hubs, and KachraSeth AI assistant. | Explain the reverse-logistics platform architecture: 4 decoupled engines solving concurrency, ledger balance, hardware WAL durability, and real-time geospatial fan-out. |
| **0:40 - 1:30** | **Concurrency Safety Race Test** | `go test -v -run TestBenchmark_BookingRace ./benchmarks` | Show 100 concurrent callers racing for 5 slot units. **Result:** Exactly 5 succeed, 95 receive 409 Conflict. **Invariant:** 0% over-allocation guaranteed by Redis Lua leases + PostgreSQL atomic capacity decrement. |
| **1:30 - 2:20** | **Ledger Invariant & Voucher Saga** | `go test -v -run TestSaga ./internal/credits/...` | Demonstrate double-entry balance ($\sum \text{Debits} = \sum \text{Credits}$). Trigger downstream voucher failure: demonstrate the multi-step saga executing an immediate compensating reversal that restores escrow points to citizen wallet with zero drift. |
| **2:20 - 3:20** | **Segmented WAL & Crash Recovery** | `go test -v -run TestBenchmark_WAL_Chaos ./benchmarks` | Ingest 5,000 telemetry records. Inject abrupt crash (`engine.Close()`). Run on-boot recovery daemon: **5,000 / 5,000 records replayed**, 0.00% data loss, verified with IEEE 802.3 CRC-32 checksums. |
| **3:20 - 4:10** | **KachraSeth AI Async Intake** | Type into web chat: *"Leaking acid and toxic battery pack at 28.6139, 77.2090"*. | Show `202 Accepted` queue dispatch. Background worker consumes message, validates guardrails, creates incident, indexes in Redis GEO, and live pin appears on Leaflet map in sub-second time. |
| **4:10 - 5:00** | **Observability & Benchmarks** | Open Grafana at `http://localhost:3000` (admin/admin). | Show real-time dashboard: 13,300+ in-process RPS, sub-1ms WebSocket broadcast latency, Prometheus histogram buckets, and zero WAL corruptions. |

---

## 2. Technical Interview Deep-Dive Talking Points

### 1. For Storage / Durability Focus (e.g. Rubrik)
> *"In ECOBrio, sensor telemetry frames cannot rely on standard database write queues because network outages or power cuts cause silent data corruption. I built a custom segmented write-ahead log (WAL) with an 8-byte framing layout containing length and hardware IEEE 802.3 CRC-32 checksums. We enforce segment rotation at 10 MB and snapshot state compaction. Upon reboot, the recovery daemon performs forward replay while gracefully truncating torn writes at the tail, proving 100% byte integrity under chaos crash injection."*

### 2. For Financial Systems & Concurrency Focus (e.g. JusPay)
> *"For high-contention booking, we solved the classic race condition using a two-tier defense: a Redis Lua script provides fast atomic token leases with a 30-second TTL, backed by PostgreSQL atomic conditional updates (`WHERE booked_count < total_capacity`). For citizen incentives, we reject floating-point currency and implement double-entry bookkeeping with strict zero-sum balancing. When third-party voucher providers fail, a compensating transaction automatically unwinds the escrow hold, guaranteeing financial consistency."*

### 3. For Real-Time Geospatial Systems (e.g. Booking.com)
> *"For operational incident tracking, we maintain a dual projection: PostgreSQL as the canonical write store and Redis GEO (`GEOADD`, `GEORADIUS`) for sub-millisecond radius filtering. Real-time updates fan out through a native Go WebSocket hub with bounded 256-message ring buffers that automatically drop slow consumers to prevent head-of-line blocking. Reconnecting clients pass their last sequence ID to resync missed events from an in-memory ring buffer, eliminating redundant full-table queries."*
