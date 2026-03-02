.PHONY: build run test clean tidy lint docker-build

BINARY := mongoclone
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
	docker build -t mongoclone:$(VERSION) .

# Convenience: run with a test database and auth disabled
dev:
	mkdir -p ./data
	go run ./cmd/mongod --port 27017 --datadir ./data --logLevel debug --noauth

# Run with TLS (requires certs)
run-tls:
	./bin/$(BINARY) --port 27017 --datadir ./data --tls --tlsCert ./certs/server.crt --tlsKey ./certs/server.key
