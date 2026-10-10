# ECOBrio Project Management Automation

.PHONY: help up down ps logs test test-race build run migrate lint clean

help:
	@echo "ECOBrio Engineering Commands:"
	@echo "  make up          - Start local Docker Compose infrastructure (Postgres, Redis, LocalStack, Prometheus, Grafana)"
	@echo "  make down        - Stop all Docker infrastructure"
	@echo "  make logs        - Tail infrastructure logs"
	@echo "  make build       - Compile the Go backend server binary"
	@echo "  make run         - Run the Go server locally"
	@echo "  make test        - Run all unit tests"
	@echo "  make test-race   - Run unit tests with race detection"
	@echo "  make clean       - Clean temporary build artifacts and test outputs"

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

ps:
	docker compose -f deploy/docker-compose.yml ps

logs:
	docker compose -f deploy/docker-compose.yml logs -f

build:
	go build -o bin/server.exe ./cmd/server

run:
	go run ./cmd/server/main.go

test:
	go test -v ./...

test-race:
	go test -race -v ./...

clean:
	rm -rf bin/ data/wal/*.log
