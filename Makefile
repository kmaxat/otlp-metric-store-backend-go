# Shortcuts for docker compose commands (see docker-compose.yml), so Docker is
# the only requirement. `make check` runs what CI runs.
GO          := docker compose run --rm go
STATICCHECK := go run honnef.co/go/tools/cmd/staticcheck@v0.8.2
DEADCODE    := go run golang.org/x/tools/cmd/deadcode@v0.52.0

.PHONY: check build test test-integration test-all fmt vet lint deadcode tidy up down

check: vet lint deadcode test-all
	$(GO) sh -c 'out=$$(gofmt -l .); echo "$$out"; test -z "$$out"'

build:
	$(GO) go build ./...

test:
	$(GO) go test -race -count=1 ./...

test-integration:
	$(GO) go test -tags integration -count=1 ./...

test-all: test test-integration

fmt:
	$(GO) go fmt ./...

vet:
	$(GO) sh -c 'go vet ./... && go vet -tags integration ./...'

lint:
	$(GO) sh -c '$(STATICCHECK) ./... && $(STATICCHECK) -tags integration ./...'

# Fails if any function is unreachable from main (e.g. code only tests use).
deadcode:
	$(GO) sh -c 'out=$$($(DEADCODE) ./...); echo "$$out"; test -z "$$out"'

tidy:
	$(GO) go mod tidy

# ClickHouse + metric store + telemetrygen.
up:
	docker compose up -d --build

down:
	docker compose down -v
