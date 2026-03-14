.PHONY: build run test clean tidy lint docker-build agent-check compat

BINARY := salvobase
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -s -w"

build:
	go build $(LDFLAGS) -o bin/$(BINARY) ./cmd/mongod

run: build
	./bin/$(BINARY) --port 27017 --datadir ./data --logLevel debug

test:
	go test -race -count=1 ./...

test-verbose:
	go test -race -v -count=1 ./...

test-integration:
	go test -race -v -count=1 -tags integration ./...

bench:
	go test -bench=. -benchmem ./...

tidy:
	go mod tidy

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ data/

docker-build:
	docker build -t salvobase:$(VERSION) .

# Convenience: run with a test database and auth disabled
dev:
	mkdir -p ./data
	go run ./cmd/mongod --port 27017 --datadir ./data --logLevel debug --noauth

# Run with TLS (requires certs)
run-tls:
	./bin/$(BINARY) --port 27017 --datadir ./data --tls --tlsCert ./certs/server.crt --tlsKey ./certs/server.key

# Run the MongoDB compatibility matrix probe and regenerate docs/compat_report.json + docs/compatibility.md.
# Set SALVOBASE_URI to point at a running Salvobase instance (defaults to localhost:27017).
SALVOBASE_URI ?= mongodb://localhost:27017
compat:
	go run ./tools/compat/... -uri $(SALVOBASE_URI) -outdir docs

# Verify agent prerequisites (Git, Go 1.22+, gh CLI)
agent-check:
	@echo "Checking agent prerequisites..."
	@echo ""
	@printf "  Git:        " && (command -v git >/dev/null 2>&1 && git --version | head -1 || (echo "MISSING — https://git-scm.com/downloads" && false))
	@printf "  Go:         " && (command -v go >/dev/null 2>&1 && go version | head -1 || (echo "MISSING — https://go.dev/dl/" && false))
	@printf "  GitHub CLI: " && (command -v gh >/dev/null 2>&1 && gh --version 2>/dev/null | head -1 || (echo "MISSING — https://cli.github.com/" && false))
	@printf "  gh auth:    " && (gh auth token >/dev/null 2>&1 && echo "authenticated" || (echo "NOT AUTHENTICATED — run: gh auth login" && false))
	@echo ""
	@echo "All prerequisites met. Read AGENT_PROTOCOL.md Section 12 to start contributing."
