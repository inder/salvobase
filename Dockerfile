# Build stage
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o salvobase ./cmd/mongod

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 salvobase && \
    adduser -D -u 1000 -G salvobase salvobase

RUN mkdir -p /var/lib/salvobase /etc/salvobase && \
    chown -R salvobase:salvobase /var/lib/salvobase

COPY --from=builder /build/salvobase /usr/local/bin/salvobase
COPY configs/mongod.yaml /etc/salvobase/mongod.yaml

USER salvobase

# MongoDB wire protocol
EXPOSE 27017
# HTTP API + Prometheus metrics
EXPOSE 27080

VOLUME ["/var/lib/salvobase"]

ENTRYPOINT ["salvobase"]
CMD ["--datadir", "/var/lib/salvobase", "--port", "27017", "--httpPort", "27080", "--bind_ip", "0.0.0.0"]
