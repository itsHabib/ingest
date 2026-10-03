.PHONY: build test test-all test-integration test-chaos vet vet-all lint docker-up docker-down run-gateway run-ingester run-querier smoke

build:
	go build -o bin/ingest .

test:
	go test -race -count=1 -v ./...

test-all: test test-integration test-chaos smoke
	@echo "All tests passed"

test-integration:
	./scripts/e2e.sh

test-chaos:
	./scripts/chaos.sh

vet:
	go vet ./...

vet-all:
	go vet -tags "smoke,integration,chaos" ./...

lint:
	golangci-lint run

docker-up:
	docker compose up -d

docker-down:
	docker compose down

run-gateway:
	go run . gateway

run-ingester:
	go run . ingester

run-querier:
	go run . querier

smoke:
	./scripts/smoke.sh
