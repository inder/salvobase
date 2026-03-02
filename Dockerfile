# Build stage
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o mongoclone ./cmd/mongod

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 mongoclone && \
    adduser -D -u 1000 -G mongoclone mongoclone

RUN mkdir -p /var/lib/mongoclone /etc/mongoclone && \
    chown -R mongoclone:mongoclone /var/lib/mongoclone

COPY --from=builder /build/mongoclone /usr/local/bin/mongoclone
COPY configs/mongod.yaml /etc/mongoclone/mongod.yaml

USER mongoclone

# MongoDB wire protocol
EXPOSE 27017
# HTTP API + Prometheus metrics
EXPOSE 27080

VOLUME ["/var/lib/mongoclone"]

ENTRYPOINT ["mongoclone"]
CMD ["--datadir", "/var/lib/mongoclone", "--port", "27017", "--httpPort", "27080", "--bind_ip", "0.0.0.0"]
