# ECOBrio

> **Distributed Reverse-Logistics • Geospatial Telemetry • Green Incentive Clearing**

ECOBrio is a high-throughput, crash-resilient distributed platform built in Go (1.22+). It coordinates municipal drop-off hubs, enforces zero slot overselling under high concurrency, verifies surrendered recyclable material through finite-state workflows, settles incentives via an auditable double-entry ledger, and ingests telemetry via a custom segmented Write-Ahead Log (WAL).

---

## 1. System Architecture

```
                                  +-------------------------------------------------------------+
                                  |                 ECOBRIO APPLICATION (GO)                    |
                                  |  HTTP Router • Request IDs • Middleware • WebSocket Hub    |
                                  +-------------------------------------------------------------+
                                                                 |
            +--------------------------+-------------------------+-------------------------+
            |                          |                         |                         |
            v                          v                         v                         v
+-----------------------+  +-----------------------+  +---------------------+  +-----------------------+
|    BOOKING ENGINE     |  |     CUSTOM WAL        |  |   CREDIT LEDGER     |  |   GEO & WEBSOCKETS    |
| (Redis Lua + Postgres)|  | (Binary Log + Replay) |  | (Double-Entry FSM)  |  |  (Spatial + Fan-out)  |
+-----------------------+  +-----------------------+  +---------------------+  +-----------------------+
| Dual-layer admission  |  | Length + CRC32 framing|  | Strict FSM states   |  | Redis GEO index       |
| Atomic SQL Decrement  |  | Batch fsync flushes   |  | Integer point units |  | Bounded client queues |
| Reconcile on timeout  |  | Torn-tail recovery    |  | Idempotency keys    |  | Viewport subscriptions|
+-----------------------+  +-----------------------+  +---------------------+  +-----------------------+
                                       |                         |
                                       +------------+------------+
                                                    |
                                                    v
                                      +---------------------------+
                                      |    POSTGRESQL 16 (ACID)   |
                                      |  Bookings • Ledger Entries|
                                      |  Outbox • Idempotency     |
                                      +---------------------------+
```

---

## 2. Technology Stack

* **Core Backend:** Go 1.22+ (`net/http`, `pgx/v5`, `go-redis/v9`)
* **Relational Database:** PostgreSQL 16 Alpine (ACID double-entry ledger, table constraints)
* **In-Memory & Cache:** Redis 7 Alpine (Lua scripts, GEO spatial indexing, Pub/Sub)
* **Cloud Emulation:** LocalStack (Offline SQS event buffering, S3 buckets)
* **Durable Storage:** Custom Segmented WAL (Binary length-prefixed frames with CRC32)
* **Observability:** Prometheus + Grafana (latency histograms, throughput, connection gauges)
* **Visual Platform:** Three.js 3D Spatial Intelligence canvas, TailwindCSS, Plus Jakarta Sans & Inter

---

## 3. Quickstart & Local Setup

### Prerequisites
* Go 1.22+
* Docker & Docker Compose

### Starting Infrastructure & Server
```bash
# 1. Start Docker dependencies (Postgres, Redis, LocalStack, Prometheus, Grafana)
docker compose -f deploy/docker-compose.yml up -d

# 2. Run database migrations
# Migrations run automatically on server startup from ./migrations

# 3. Build and launch the ECOBrio server
go build -o bin/server.exe ./cmd/server
./bin/server.exe
```

Access the application:
* **Interactive Experience Platform:** [http://localhost:8080/](http://localhost:8080/)
* **Health Check:** [http://localhost:8080/healthz](http://localhost:8080/healthz)
* **Readiness Check:** [http://localhost:8080/readyz](http://localhost:8080/readyz)
* **Prometheus Metrics:** [http://localhost:8080/metrics](http://localhost:8080/metrics)

---

## 4. Day 1 Deliverables Verification

* [x] **Go Scaffolding & Toolchain:** Go 1.22 module pinned and verified (`go version go1.22.8`).
* [x] **Docker Compose Stack:** PostgreSQL 16, Redis 7, LocalStack (S3/SQS), Prometheus, and Grafana declared in `deploy/docker-compose.yml`.
* [x] **Database Migrations:** Complete relational schema in `migrations/000001_init_schema.up.sql` covering users, collection hubs, slots, bookings, submissions, double-entry ledger, outbox events, and WAL checkpoints.
* [x] **Zero-Dependency Native Migrator:** Automated transaction runner in `internal/database/migrate.go`.
* [x] **Observability Baseline:** Prometheus metric registry in `internal/observability/metrics.go` exporting request latency histograms, counts, and connection gauges.
* [x] **Structured Middleware:** Request-ID correlation, structured logging (`slog`), and panic recovery in `internal/middleware/middleware.go`.
* [x] **Compiled Server Binary:** `cmd/server/main.go` compiling cleanly to `bin/server.exe`.
* [x] **Visual Experience Platform Integration:** Cleaned and mounted 3D spatial interface into `web/` served directly by Go at `http://localhost:8080/`.
* [x] **CI/CD Quality Gate:** Automated GitHub Actions pipeline configured in `.github/workflows/ci.yml`.
