# ECOBrio Distributed Reverse-Logistics Platform Automation
# Reference: EcoBrio_Blueprint.pdf Section 19.1

.PHONY: help up down ps logs build run test test-race test-integration bench chaos-wal migrate clean

help:
	@echo "ECOBrio Developer & Evaluation Commands:"
	@echo "  make up               - Start local Docker infrastructure (Postgres, Redis, LocalStack, Prometheus, Grafana)"
	@echo "  make down             - Stop all Docker infrastructure and volumes"
	@echo "  make ps               - Display status of Docker infrastructure containers"
	@echo "  make logs             - Tail infrastructure logs"
	@echo "  make build            - Compile server and benchmark binaries"
	@echo "  make run              - Run the ECOBrio server locally"
	@echo "  make migrate          - Apply pending PostgreSQL schema migrations"
	@echo "  make test             - Execute all unit tests across the repository"
	@echo "  make test-integration - Execute end-to-end integration test suites"
	@echo "  make test-race        - Execute tests under Go race detector"
	@echo "  make bench            - Execute full PRD benchmark suite (throughput, concurrency race, websocket, WAL)"
	@echo "  make bench-booking    - Execute booking concurrency race test against 5 slots"
	@echo "  make chaos-wal        - Execute WAL crash injection and fault recovery test"
	@echo "  make clean            - Clean compiled binaries, temporary logs, and snapshot artifacts"

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

ps:
	docker compose -f deploy/docker-compose.yml ps

logs:
	docker compose -f deploy/docker-compose.yml logs -f

build:
	go build -o bin/server ./cmd/server
	go build -o bin/benchmarks ./cmd/benchmarks

run:
	go run ./cmd/server/main.go

test:
	go test -count=1 ./...

test-integration:
	go test -v ./tests/integration/...

test-race:
	go test -race ./...

bench:
	go run ./cmd/benchmarks/main.go

bench-booking:
	go test -v -run=^TestBenchmark_BookingRace_ZeroOverAllocation ecobrio/benchmarks

migrate:
	go run ./cmd/server/main.go --migrate-only || true

chaos-wal:
	go test -v ./tests/chaos/...

clean:
	rm -rf bin/ data/wal/*.log data/snapshots/*.json
