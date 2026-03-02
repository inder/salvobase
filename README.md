# MongClone

A MongoDB-compatible document database server written in Go.

[![Go 1.22+](https://img.shields.io/badge/go-1.22%2B-blue)](https://golang.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

## What it is

MongClone implements the MongoDB Wire Protocol and is compatible with existing MongoDB drivers (Go, Python, Node.js, Java, etc.). You point your driver at `mongodb://localhost:27017` and it works.

**Production-ready for:** multi-tenant SaaS, embedded databases, test fixtures, air-gapped deployments where MongoDB licensing is a concern, and anywhere you want a MongoDB-compatible database without the MongoDB overhead.

## Quick Start

```bash
# Build
make build

# Start (no auth, dev mode)
make dev

# Or with Docker
docker-compose up mongoclone

# Connect with mongosh
mongosh mongodb://localhost:27017

# Connect with the Go driver
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
```

## Improvements Over MongoDB Community

| Feature | MongoDB Community | MongClone |
|---------|------------------|-----------|
| Prometheus metrics | Requires separate exporter | **Built-in** at `:27080/metrics` |
| HTTP/JSON REST API | Atlas Data API (paid) | **Built-in** at `:27080/api/v1/` |
| Per-tenant rate limiting | Not available | **Built-in**, config per DB |
| Audit logging | MongoDB Enterprise only | **Built-in**, free |
| TTL index precision | 60-second granularity | **1-second** granularity |
| Explain cost estimates | Opaque query planner | **Exposed** with timing |
| Config reload | Requires restart | **SIGHUP** hot reload |
| License | SSPL (copyleft) | **Apache 2.0** |

## Supported MongoDB Operations

**CRUD:** `find`, `insert`, `update`, `delete`, `findAndModify`, `count`, `distinct`

**Indexes:** single-field, compound, unique, sparse, text, TTL, partial, wildcard

**Aggregation:** `$match`, `$project`, `$group`, `$sort`, `$limit`, `$skip`, `$unwind`, `$lookup`, `$addFields`, `$replaceRoot`, `$count`, `$facet`, `$bucket`, `$out`, `$merge`, `$sortByCount`, `$sample`

**Admin:** `createCollection`, `drop`, `dropDatabase`, `listDatabases`, `listCollections`, `renameCollection`

**Auth:** SCRAM-SHA-256 (`createUser`, `dropUser`, `updateUser`), role-based access control

**Diagnostics:** `ping`, `hello`, `buildInfo`, `serverStatus`, `dbStats`, `collStats`

## Architecture

```
Client (mongosh / driver)
    │ TCP :27017 (MongoDB Wire Protocol)
    ▼
server.Server → server.Connection (goroutine per client)
    │
    ▼ wire.ReadMessage (OP_MSG / OP_QUERY)
commands.Dispatcher.Dispatch(ctx, cmd)
    │
    ├─ handleFind → storage.Collection.Find → query.Filter
    ├─ handleInsert → storage.Collection.InsertMany
    ├─ handleAggregate → aggregation.Execute
    ├─ handleSASLStart → auth.Manager.SASLStart
    └─ ...
    │
    ▼ storage.BBoltEngine (one bbolt .db file per database)
    └─ Bucket "col.<collection>" → documents (Snappy-compressed BSON)
    └─ Bucket "idx.<collection>.<name>" → secondary index
```

## Configuration

```bash
# CLI flags
mongoclone --port 27017 --datadir ./data --noauth --logLevel debug

# Environment variables
MONGOCLONE_PORT=27017
MONGOCLONE_DATADIR=/var/lib/mongoclone
MONGOCLONE_NOAUTH=false

# Config file
mongoclone --config /etc/mongoclone/mongod.yaml
```

See `configs/mongod.yaml` for all options.

## Authentication

```bash
# Create admin user
./bin/mongoclone admin create-user admin supersecret

# Start with auth
./bin/mongoclone --datadir ./data --port 27017

# Connect
mongosh "mongodb://admin:supersecret@localhost:27017/admin"
```

## Monitoring

```
# Prometheus metrics
curl http://localhost:27080/metrics

# Health check
curl http://localhost:27080/health

# REST API
curl -X POST http://localhost:27080/api/v1/db/mydb/collection/users/find \
  -H "Content-Type: application/json" \
  -d '{"filter": {"age": {"$gt": 18}}}'
```

## Production Deployment

```bash
# Docker
docker-compose up -d mongoclone

# Systemd
cp deployments/mongoclone.service /etc/systemd/system/
systemctl enable --now mongoclone

# Backup: just copy the data directory
rsync -av /var/lib/mongoclone/ /backup/mongoclone/
```

## Limitations

- **No replication** — single-node only (replica sets not implemented)
- **No sharding** — single-node only
- **No change streams** — not implemented
- **No transactions** — single-document atomicity only (multi-doc transactions stubbed)
- **No JavaScript eval** — `$where` and `mapReduce` are disabled (security + complexity)
- **No Atlas-specific features** — vector search, online archive, etc.

## License

Apache 2.0. See [LICENSE](LICENSE).

Unlike MongoDB Community (SSPL), MongClone can be used in commercial products, SaaS, and embedded systems without any licensing restrictions.
