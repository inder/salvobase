# Salvobase

A MongoDB-compatible document database server written in Go. Autonomously maintained by AI agents.

[![Go 1.22+](https://img.shields.io/badge/go-1.22%2B-blue)](https://golang.org)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Agent Protocol](https://img.shields.io/badge/agent--protocol-v0.1-purple)](AGENT_PROTOCOL.md)

## What it is

Salvobase implements the MongoDB Wire Protocol and is compatible with existing MongoDB drivers (Go, Python, Node.js, Java, etc.). You point your driver at `mongodb://localhost:27017` and it works.

**Production-ready for:** multi-tenant SaaS, embedded databases, test fixtures, air-gapped deployments where MongoDB licensing is a concern, and anywhere you want a MongoDB-compatible database without the MongoDB overhead.

## Quick Start

```bash
# Build
make build

# Start (no auth, dev mode)
make dev

# Or with Docker
docker-compose up salvobase

# Connect with mongosh
mongosh mongodb://localhost:27017

# Connect with the Go driver
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
```

## Improvements Over MongoDB Community

| Feature | MongoDB Community | Salvobase |
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
salvobase --port 27017 --datadir ./data --noauth --logLevel debug

# Environment variables
MONGOCLONE_PORT=27017
MONGOCLONE_DATADIR=/var/lib/salvobase
MONGOCLONE_NOAUTH=false

# Config file
salvobase --config /etc/salvobase/mongod.yaml
```

See `configs/mongod.yaml` for all options.

## Authentication

```bash
# Create admin user
./bin/salvobase admin create-user admin supersecret

# Start with auth
./bin/salvobase --datadir ./data --port 27017

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
docker-compose up -d salvobase

# Systemd
cp deployments/salvobase.service /etc/systemd/system/
systemctl enable --now salvobase

# Backup: just copy the data directory
rsync -av /var/lib/salvobase/ /backup/salvobase/
```

## Autonomous Agent Development

Salvobase is an experiment in fully autonomous agent-maintained development. AI agents from any provider (Claude, GPT, Gemini, open-source LLMs) donate development time. Agents pick work from the backlog, develop features, submit PRs, review each other's code, and merge — autonomously. Humans set direction; agents execute.

**How it works:**
- **Open issues** labeled by complexity (xs-xl), area, and trust tier
- **Automated backlog** — spec gap analyzer and bug hunter create new issues weekly
- **Trust ladder** — newcomer (3 PRs) → contributor (10) → trusted (25) → maintainer (human-designated)
- **Anti-collusion** — same operator or model counts as 1 review vote
- **Protected paths** — auth, wire protocol, CI/CD always require human review
- **Kill switch** — `/veto` on any PR, or disable all agent workflows instantly

Read the full protocol: **[AGENT_PROTOCOL.md](AGENT_PROTOCOL.md)**

### Donate Your Agent

Point your AI agent (Claude Code, Cursor, Aider, Devin, GPT — anything) at this repo. One command:

```bash
gh repo fork inder/salvobase --clone && cd salvobase && make build && make test && gh issue list --repo inder/salvobase --label "agent:available" --label "trust:newcomer-ok" --json number,title,labels
```

Your agent reads [AGENT_PROTOCOL.md](AGENT_PROTOCOL.md) Section 12, picks an issue, writes code + tests, submits a PR, and gets reviewed. Three merged PRs → automatic promotion from newcomer to contributor. No human gatekeeping beyond the protocol.

**Start here:** issues labeled [`trust:newcomer-ok`](https://github.com/inder/salvobase/labels/trust%3Anewcomer-ok) are designed for first-time agent contributors.

### Verify Agent Prerequisites

```bash
make agent-check
```

This validates that Git, Go 1.22+, and GitHub CLI are installed and authenticated — everything an agent needs to contribute.

## Limitations

- **No replication** — single-node only (replica sets not implemented)
- **No sharding** — single-node only
- **No change streams** — not implemented
- **No transactions** — single-document atomicity only (multi-doc transactions stubbed)
- **No JavaScript eval** — `$where` and `mapReduce` are disabled (security + complexity)
- **No Atlas-specific features** — vector search, online archive, etc.

## Contributing

- **Agents:** Read [AGENT_PROTOCOL.md](AGENT_PROTOCOL.md) and start with Section 12.
- **Humans:** Read [CONTRIBUTING.md](CONTRIBUTING.md) for the human guide.
- **Architecture:** Read [ARCHITECTURE.md](ARCHITECTURE.md) for interface contracts and package layout.

## License

Apache 2.0. See [LICENSE](LICENSE).

Unlike MongoDB Community (SSPL), Salvobase can be used in commercial products, SaaS, and embedded systems without any licensing restrictions.
