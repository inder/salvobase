# MongClone Developer Guide

> This guide is for contributors, embedders, and anyone who wants to understand
> the internals, extend the system, or plug in a custom storage backend.

**Module:** `github.com/inder/mongoclone`
**Go version:** 1.22+
**License:** Apache 2.0

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
   - [Package Layout](#11-package-layout)
   - [Data Flow Diagram](#12-data-flow-diagram)
2. [Project Setup](#2-project-setup)
3. [Package Reference](#3-package-reference)
   - [internal/wire](#31-internalwire)
   - [internal/storage](#32-internalstorage)
   - [internal/query](#33-internalquery)
   - [internal/aggregation](#34-internalaggregation)
   - [internal/auth](#35-internalauth)
   - [internal/commands](#36-internalcommands)
   - [internal/server](#37-internalserver)
4. [Storage Engine Internals](#4-storage-engine-internals)
   - [bbolt B-tree Structure](#41-bbolt-b-tree-structure)
   - [Bucket Naming Convention](#42-bucket-naming-convention)
   - [Key Encoding](#43-key-encoding)
   - [Index Storage Format](#44-index-storage-format)
   - [Transaction Model](#45-transaction-model)
5. [Wire Protocol Details](#5-wire-protocol-details)
   - [OP_MSG Binary Format](#51-op_msg-binary-format)
   - [OP_QUERY and OP_REPLY (Legacy)](#52-op_query-and-op_reply-legacy)
   - [Checksum Handling](#53-checksum-handling)
6. [Query Evaluation](#6-query-evaluation)
   - [Filter Evaluation](#61-filter-evaluation)
   - [Type Comparison Order](#62-type-comparison-order)
   - [Dot Notation Traversal](#63-dot-notation-traversal)
7. [BSON Handling](#7-bson-handling)
   - [bson.Raw vs bson.D](#71-bsonraw-vs-bsond)
   - [Marshaling Patterns](#72-marshaling-patterns)
8. [Adding a New Command](#8-adding-a-new-command)
9. [Adding a New Query Operator](#9-adding-a-new-query-operator)
10. [Adding a New Aggregation Stage](#10-adding-a-new-aggregation-stage)
11. [Plugging in a Different Storage Engine](#11-plugging-in-a-different-storage-engine)
12. [Testing](#12-testing)
    - [Unit Test Approach](#121-unit-test-approach)
    - [Integration Tests](#122-integration-tests)
13. [Performance Notes](#13-performance-notes)
14. [Security Notes](#14-security-notes)
15. [Roadmap and Known Limitations](#15-roadmap-and-known-limitations)

---

## 1. Architecture Overview

MongClone is structured as a set of independent internal packages that communicate through well-defined interfaces. The server binary (`cmd/mongod`) wires them together.

### 1.1 Package Layout

```
cmd/mongod/
    main.go                 Entry point, cobra CLI, signal handling, server bootstrap

internal/wire/
    constants.go            Opcode constants, wire version numbers, flag bit masks
    protocol.go             Header parsing, ReadMessage dispatcher, low-level I/O helpers
    op_msg.go               OP_MSG read/write (current primary format)
    op_query.go             OP_QUERY read (legacy, MongoDB pre-3.6)
    op_reply.go             OP_REPLY write (legacy response format)
    op_legacy.go            OP_GETMORE, OP_KILL_CURSORS, OP_DELETE read (legacy)

internal/storage/
    types.go                Engine/Collection/Cursor interfaces + all data types + error codes
    (engine.go)             BBoltEngine implementation (not yet scaffolded — see interface)

internal/query/
    query.go                Update operator application (Apply), projection (Project),
                            sort function (SortFunc), IsUpdateDoc
    filter.go               Filter evaluation engine + all comparison/logical/array operators
    regexp.go               Regexp helper (matchString wrapper)

internal/aggregation/
    (pipeline.go)           Execute() — pipeline runner + stage implementations

internal/auth/
    users.go                SCRAM-SHA-256 key derivation (HashPassword)
    (manager.go)            SASLStart/SASLContinue conversation management

internal/commands/
    (dispatcher.go)         Dispatcher + all command handlers
    (context.go)            Context type, Session type

internal/server/
    (server.go)             Server struct, acceptLoop, per-connection goroutine
    (http.go)               HTTP server, /healthz, /readyz, /metrics, REST API
    (metrics.go)            Prometheus metric registration

configs/
    mongod.yaml             Default YAML configuration

docs/
    user-guide.md           End-user documentation
    developer-guide.md      This file
```

### 1.2 Data Flow Diagram

A complete round trip from client to storage and back:

```
┌──────────────────────────────────────────────────────────────────┐
│  Client (mongosh / any MongoDB driver)                           │
└──────────────────┬───────────────────────────────────────────────┘
                   │  TCP / TLS connection (port 27017)
                   │  Binary: MongoDB wire protocol messages
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  server.Server.acceptLoop()                                      │
│  • Maintains connection count                                    │
│  • Rate limiter check (per database)                             │
│  • Spawns one goroutine per connection                           │
└──────────────────┬───────────────────────────────────────────────┘
                   │  goroutine per connection
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  server.Connection.serve()   [read loop]                         │
│  • wire.ReadMessage(conn)  →  *OpMsgMessage | *OpQueryMessage    │
│  • Extracts: command name, target DB, session ID                 │
│  • Builds commands.Context{DB, Engine, Auth, Session, Logger}    │
└──────────────────┬───────────────────────────────────────────────┘
                   │  commands.Context + raw BSON command doc
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  commands.Dispatcher.Dispatch(ctx, cmd)                          │
│  • Looks up handler by command name (lowercase)                  │
│  • Auth check: Manager.HasPermission(user, db, action)           │
│  • Calls handler(ctx, cmd)  →  bson.Raw response                 │
└──────────┬──────────────────────────┬────────────────────────────┘
           │ CRUD commands            │ Aggregation commands
           ▼                          ▼
┌──────────────────┐       ┌──────────────────────────────────────┐
│  storage.Engine  │       │  aggregation.Execute(coll, engine,   │
│  .Collection()   │       │      db, pipeline, opts)             │
│  .InsertOne()    │       │  • Iterates pipeline stages          │
│  .Find()         │       │  • $lookup crosses collections       │
│  .UpdateOne()    │       │  • $out/$merge write back            │
│  .DeleteOne()    │       └──────────────┬───────────────────────┘
│  .CountDocuments │                      │
└──────────┬───────┘                      │
           │                              │
           └──────────────┬───────────────┘
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  BBoltEngine  (internal/storage)                                 │
│  • Opens/creates <dataDir>/<dbname>.db (bbolt file)              │
│  • Read tx: bbolt.View() — concurrent readers allowed            │
│  • Write tx: bbolt.Update() — single writer at a time           │
│  • Documents stored compressed (Snappy/Zstd) in col.<name>       │
│  • Index entries written to idx.<coll>.<indexName> buckets       │
└──────────────────────────────────────────────────────────────────┘
                          │
                          │  bson.Raw response document
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  wire.WriteOpMsg(conn, requestID, responseTo, 0, responseDoc)    │
│  Encodes: 16-byte header + 4-byte flagBits + 1-byte kind + BSON  │
└──────────────────┬───────────────────────────────────────────────┘
                   │  TCP write
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  Client receives response                                        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Project Setup

### Prerequisites

- **Go 1.22 or later** (`go version` to check)
- **golangci-lint** (optional, for linting): `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`
- **mongosh** (optional, for manual integration testing)

### Clone and Build

```bash
git clone https://github.com/inder/mongoclone.git
cd mongoclone

# Download dependencies
go mod download

# Build the binary
make build
# Outputs: ./bin/mongoclone

# Run in dev mode (no auth, debug logging, ./data directory)
make dev

# Run tests
make test

# Run with race detector (always during development)
go test -race ./...

# Run integration tests (requires a running mongosh or MongoDB driver)
make test-integration

# Benchmarks
make bench

# Lint
make lint
```

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make build` | Compile with version/buildTime ldflags |
| `make dev` | Build and run with `--noauth --logLevel debug` |
| `make run` | Build and run with defaults |
| `make run-tls` | Run with TLS (requires `./certs/`) |
| `make test` | `go test -race -count=1 ./...` |
| `make test-verbose` | Test with `-v` flag |
| `make test-integration` | Tests tagged `integration` |
| `make bench` | `go test -bench=. -benchmem ./...` |
| `make tidy` | `go mod tidy` |
| `make lint` | `golangci-lint run ./...` |
| `make clean` | Remove `bin/` and `data/` |
| `make docker-build` | Build Docker image |

### Version Injection

The binary version and build time are injected at build time via ldflags:

```bash
go build \
  -ldflags "-X main.version=$(git describe --tags --always) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -o bin/mongoclone ./cmd/mongod
```

These values appear in `buildInfo` command responses and server startup logs.

---

## 3. Package Reference

### 3.1 `internal/wire`

The `wire` package implements the MongoDB binary wire protocol. It is the boundary layer between raw TCP bytes and Go structs.

**Key files:**

| File | Purpose |
|------|---------|
| `constants.go` | Opcode values, flag bit masks, wire version constants |
| `protocol.go` | `ReadMessage` dispatcher, header parser, low-level I/O primitives |
| `op_msg.go` | OP_MSG read/write — the current primary message format |
| `op_query.go` | OP_QUERY read — legacy format, still used by some drivers |
| `op_reply.go` | OP_REPLY write — legacy response format |
| `op_legacy.go` | OP_GETMORE, OP_KILL_CURSORS, OP_DELETE read |

**Opcode constants (`constants.go`):**

```go
OpReply       = int32(1)    // server → client, legacy
OpUpdate      = int32(2001) // deprecated
OpInsert      = int32(2002) // deprecated
OpQuery       = int32(2004) // client → server, legacy commands
OpGetMore     = int32(2005) // client → server, legacy cursor iteration
OpDelete      = int32(2006) // deprecated
OpKillCursors = int32(2007) // client → server, legacy cursor cleanup
OpCompressed  = int32(2012) // compressed message wrapper (parsed but body discarded)
OpMsg         = int32(2013) // client ↔ server, current format
```

**The `ReadMessage` function (`protocol.go`):**

`ReadMessage(conn net.Conn) (interface{}, error)` is the top-level entry point. It:
1. Reads the 16-byte header
2. Creates an `io.LimitedReader` scoped to `messageLength - 16` bytes (prevents overread)
3. Switches on the opcode and delegates to the appropriate parser
4. Returns one of: `*OpMsgMessage`, `*OpQueryMessage`, `*OpGetMoreMessage`, `*OpKillCursorsMessage`, `*OpDeleteMessage`, or `nil` for unknown opcodes

For unknown opcodes the body is drained (so the connection stays synchronized) and `nil` is returned without an error.

**How to add a new opcode:**

1. Add the opcode constant to `constants.go`
2. Create a new message struct and `readOpXxx` function in a new file (e.g., `op_new.go`)
3. Add a `case OpNew:` branch in the `ReadMessage` switch in `protocol.go`
4. Add `WriteOpNew` if responses use this opcode

### 3.2 `internal/storage`

The `storage` package owns the `Engine`, `Collection`, and `Cursor` interfaces plus all shared data types.

**Interface summary:**

```go
// Engine — top-level, one instance per server
type Engine interface {
    // Database lifecycle
    CreateDatabase(name string) error
    DropDatabase(name string) error
    ListDatabases() ([]DatabaseInfo, error)
    HasDatabase(name string) bool

    // Collection lifecycle
    CreateCollection(db, coll string, opts CreateCollectionOptions) error
    DropCollection(db, coll string) error
    ListCollections(db string) ([]CollectionInfo, error)
    HasCollection(db, coll string) bool

    // Returns (and lazily creates) a collection handle
    Collection(db, coll string) (Collection, error)

    // Index management
    CreateIndex(db, coll string, spec IndexSpec) (string, error)
    DropIndex(db, coll, indexName string) error
    ListIndexes(db, coll string) ([]IndexInfo, error)

    // Stats
    DatabaseStats(db string) (DatabaseStats, error)
    CollectionStats(db, coll string) (CollectionStats, error)
    ServerStats() (ServerStats, error)

    // Shared cursor store (for getMore across collections)
    Cursors() CursorStore

    // User store (backed by admin._users bucket)
    Users() UserStore

    RenameCollection(fromDB, fromColl, toDB, toColl string, dropTarget bool) error
    Close() error
}

// Collection — CRUD on a single collection
type Collection interface {
    InsertOne(doc bson.Raw) (bson.ObjectID, error)
    InsertMany(docs []bson.Raw, opts InsertOptions) ([]bson.ObjectID, error)
    Find(filter bson.Raw, opts FindOptions) (Cursor, error)
    FindOne(filter bson.Raw, opts FindOptions) (bson.Raw, error)
    UpdateOne(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)
    UpdateMany(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)
    ReplaceOne(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error)
    DeleteOne(filter bson.Raw) (int64, error)
    DeleteMany(filter bson.Raw) (int64, error)
    CountDocuments(filter bson.Raw) (int64, error)
    Distinct(field string, filter bson.Raw) ([]interface{}, error)
    FindOneAndUpdate(filter, update bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)
    FindOneAndReplace(filter, replacement bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)
    FindOneAndDelete(filter bson.Raw, opts FindAndModifyOptions) (bson.Raw, error)
    ForEach(filter bson.Raw, opts FindOptions, fn func(bson.Raw) error) error
    Name() string
    Database() string
}
```

**Error codes (`types.go`):**

```go
const (
    ErrCodeBadValue                = int32(2)
    ErrCodeIllegalOperation        = int32(20)
    ErrCodeNamespaceNotFound       = int32(26)
    ErrCodeIndexNotFound           = int32(27)
    ErrCodeConflictingUpdateOps    = int32(40)
    ErrCodeCollectionAlreadyExists = int32(48)
    ErrCodeInvalidBSON             = int32(22)
    ErrCodeDuplicateKey            = int32(11000)
    ErrCodeUnauthorized            = int32(13)
    ErrCodeAuthenticationFailed    = int32(18)
    ErrCodeUserNotFound            = int32(11)
    ErrCodeUserAlreadyExists       = int32(51003)
    ErrCodeCursorNotFound          = int32(43)
    ErrCodeCommandFailed           = int32(125)
    ErrCodeNotImplemented          = int32(238)
)
```

Use `storage.Errorf(code, "message: %s", arg)` to construct typed errors. The command dispatcher converts `*storage.MongoError` into a properly shaped `{ok: 0, code: N, errmsg: "..."}` response.

### 3.3 `internal/query`

The `query` package implements stateless filter evaluation, update application, projection, and sort. Every function is a pure function over `bson.Raw` — no state, no I/O.

**Public API:**

```go
// Filter checks whether doc matches the MongoDB filter document.
// An empty or nil filter matches all documents.
func Filter(doc bson.Raw, filter bson.Raw) (bool, error)

// Apply applies an update document to doc and returns the modified document.
// isUpsert=true enables $setOnInsert processing.
func Apply(doc bson.Raw, update bson.Raw, isUpsert bool) (bson.Raw, error)

// Project applies a projection to doc, returning a new document.
func Project(doc bson.Raw, projection bson.Raw) (bson.Raw, error)

// SortFunc returns a comparison function for use with sort.Slice.
// sort is a bson.Raw like {"field": 1, "other": -1}.
func SortFunc(sort bson.Raw) (func(a, b bson.Raw) int, error)

// IsUpdateDoc returns true if doc contains update operators ($set, $inc, etc.)
// rather than being a replacement document.
func IsUpdateDoc(doc bson.Raw) bool
```

**Internal call graph (filter evaluation):**

```
Filter(doc, filter)
  ├── for each top-level key starting with "$":
  │     evalLogical(doc, op, val)
  │       ├── evalLogicalAnd / evalLogicalOr / evalLogicalNor
  │       ├── evalExpr(doc, val)          — $expr with field references
  │       └── evalTextFilter(doc, val)    — $text substring search
  │
  └── for each field key:
        getField(doc, "dot.notation.path")  — recursive dot traversal
        evalFieldCondition(doc, fieldVal, condVal)
          ├── [no "$" prefix] → compareValues(fieldVal, condVal) == 0  (implicit $eq)
          └── [has "$" prefix] → evalOperator(doc, fieldVal, op, opVal)
                ├── $eq/$ne/$gt/$gte/$lt/$lte  → compareValues()
                ├── $in/$nin                   → evalIn()
                ├── $not                       → !evalFieldCondition()
                ├── $exists                    → type check
                ├── $type                      → evalType()
                ├── $regex / $options          → evalRegexWithOptions()
                ├── $mod                       → evalMod()
                ├── $all                       → evalAll()
                ├── $elemMatch                 → evalElemMatch()
                ├── $size                      → evalSize()
                └── $bitsAllSet etc.           → evalBits()
```

### 3.4 `internal/aggregation`

The `aggregation` package executes MongoDB aggregation pipelines.

**Public API:**

```go
func Execute(
    coll storage.Collection,
    engine storage.Engine,
    db string,
    pipeline []bson.Raw,
    opts PipelineOptions,
) (storage.Cursor, error)

type PipelineOptions struct {
    AllowDiskUse bool
    BatchSize    int32
    MaxTimeMS    int64
    Comment      string
    Let          bson.Raw  // variables for $lookup/$merge
}
```

**Pipeline execution model:**

Each stage is represented by a `Stage` interface:

```go
type Stage interface {
    // Process receives the input documents channel and returns output documents.
    Process(ctx context.Context, input <-chan bson.Raw) (<-chan bson.Raw, error)
}
```

The pipeline runner chains stages together:

```
inputCursor → stage[0].Process() → stage[1].Process() → ... → outputCursor
```

Stages that require full materialization (e.g., `$sort`, `$group`) buffer all input documents in memory before emitting output. `AllowDiskUse` controls whether they can spill to a temp file.

`$lookup` stages call back into `engine.Collection()` to access the foreign collection, which is why `Execute` receives the `Engine` reference.

`$out` and `$merge` stages call `engine.Collection()` for the destination collection and use the `InsertMany` / `UpdateOne` paths to write results.

### 3.5 `internal/auth`

The `auth` package handles SCRAM-SHA-256 authentication.

**`users.go` — Password Hashing:**

```go
func HashPassword(password string) (storedKey, serverKey, salt []byte, iterCount int, err error)
```

Implements RFC 5802 key derivation:
1. `SaltedPassword = PBKDF2-SHA256(password, salt, 15000, 32)`
2. `ClientKey = HMAC-SHA256(SaltedPassword, "Client Key")`
3. `StoredKey = SHA256(ClientKey)`
4. `ServerKey = HMAC-SHA256(SaltedPassword, "Server Key")`

The `User` struct in `storage.types` stores `StoredKey`, `ServerKey`, `Salt`, and `IterCount`. The raw password is never stored anywhere.

**`Manager` (authentication conversation):**

```go
func NewManager(users storage.UserStore) *Manager

// saslStart: client sends mechanism + clientFirstMessage
func (m *Manager) SASLStart(db, mechanism string, payload []byte) ([]byte, int32, error)
// Returns: serverFirstMessage, conversationID

// saslContinue: client sends clientFinalMessage
func (m *Manager) SASLContinue(conversationID int32, payload []byte) ([]byte, bool, error)
// Returns: serverFinalMessage, done
```

The `Manager` maintains in-flight SCRAM conversations keyed by `conversationID`. A conversation is cleaned up after completion or timeout.

**SCRAM-SHA-256 flow:**

```
Client                              Server
  │                                   │
  │── saslStart ──────────────────────►│  payload = "n,,n=user,r=clientNonce"
  │                                   │  SASLStart → ServerFirstMessage
  │◄─ response ───────────────────────│  "r=clientNonce+serverNonce,s=salt,i=15000"
  │                                   │
  │── saslContinue ──────────────────►│  payload = clientFinalMessage (with proof)
  │                                   │  Verify ClientProof using StoredKey
  │◄─ response ───────────────────────│  "v=serverSignature" (done=true)
  │                                   │
  │── saslContinue (done ack) ────────►│  optional final round
```

### 3.6 `internal/commands`

The `commands` package routes command names to handler functions and provides the `Context` type that handlers receive.

**`Context` type:**

```go
type Context struct {
    DB       string          // The target database name
    Engine   storage.Engine  // Storage engine reference
    Auth     *auth.Manager   // For permission checks
    Session  *Session        // nil if no session
    Logger   *zap.Logger     // Structured logger
    ConnID   int64           // Connection identifier
    Username string          // Authenticated user (empty in noauth mode)
    UserDB   string          // Database the user authenticated against
}
```

**`Dispatcher`:**

```go
type Dispatcher struct {
    handlers map[string]Handler   // lowercase command name → Handler
}

type Handler func(ctx *Context, cmd bson.Raw) (bson.Raw, error)

func (d *Dispatcher) Dispatch(ctx *Context, cmd bson.Raw) bson.Raw
```

`Dispatch`:
1. Extracts the command name from the first key of `cmd`
2. Looks up the handler (case-insensitive — normalized to lowercase)
3. Checks `ctx.Auth.HasPermission(ctx.Username, ctx.DB, requiredAction)`
4. Calls the handler
5. On `*storage.MongoError`: converts to `{ok: 0, code: N, errmsg: ...}`
6. On any other error: converts to `{ok: 0, errmsg: ...}`

### 3.7 `internal/server`

The `server` package is the top-level coordinator.

**`Config` struct:**

```go
type Config struct {
    Port           int
    HTTPPort       int
    BindIP         string
    MaxConnections int
    DataDir        string
    NoAuth         bool
    LogLevel       string
    LogFormat      string
    AuditLog       string
    Compression    string
    SyncOnWrite    bool
    MaxDocSize     int
    RequestsPerSec int
    TLS            bool
    TLSCert        string
    TLSKey         string
    Version        string
    BuildTime      string
}
```

**Startup sequence (`server.New` → `server.Run`):**

```
server.New(cfg, logger):
  1. Open BBoltEngine(cfg.DataDir, cfg.Compression, cfg.SyncOnWrite)
  2. Initialize auth.Manager(engine.Users())
  3. Initialize commands.Dispatcher(engine, auth, logger)
  4. Start HTTP server (Prometheus, healthz, REST API) on cfg.HTTPPort
  5. net.Listen("tcp", bindIP:port) or tls.Listen if TLS enabled
  6. Return *Server

server.Run():
  7. Loop: accept connections
  8. For each conn: goroutine → Connection.serve(conn)

Connection.serve(conn):
  9. Loop: wire.ReadMessage(conn)
  10. Build Context
  11. dispatcher.Dispatch(ctx, cmd)
  12. wire.WriteOpMsg(conn, ..., response)
```

**SIGHUP handling:**

The main process catches `SIGHUP` and logs a notice (config reload not yet fully wired to the running server — it's a planned feature).

---

## 4. Storage Engine Internals

### 4.1 bbolt B-tree Structure

bbolt is an embedded, pure-Go B-tree key/value store (a fork of BoltDB). It provides:
- **ACID transactions** via copy-on-write B-trees
- **Multiple readers / single writer** (MRSW) — reads never block reads, but writes are serialized
- **Memory-mapped I/O** — the OS page cache handles buffering
- **Nested buckets** — arbitrary hierarchy within a single file

MongClone creates **one bbolt file per database**:

```
<dataDir>/admin.db
<dataDir>/myapp.db
<dataDir>/tenant_acme.db
```

### 4.2 Bucket Naming Convention

Within each `.db` file:

| Bucket Name | Contents |
|-------------|----------|
| `col.<collectionName>` | Documents — key: `_id` bytes, value: BSON (optionally compressed) |
| `idx.<collectionName>.<indexName>` | Index entries — see format below |
| `_meta.collections` | Collection metadata — key: collection name, value: JSON-encoded `CollectionInfo` |
| `_meta.indexes` | Index metadata — key: `<collName>.<indexName>`, value: JSON-encoded `IndexSpec` |

In `admin.db` only:

| Bucket Name | Contents |
|-------------|----------|
| `_users` | User records — key: `<db>.<username>`, value: JSON-encoded `User` |

### 4.3 Key Encoding

**Collection documents:**

Keys are the raw `_id` bytes:
- For `bson.ObjectID`: 12-byte binary representation
- For string `_id`: UTF-8 bytes
- For integer `_id`: little-endian int64 bytes
- For custom `_id`: BSON-encoded value bytes

The bbolt B-tree orders keys lexicographically, so ObjectID range scans (which embed a timestamp) naturally iterate in insertion order.

**Users (`_users` bucket):**

Key is `<db>\x00<username>` (null-separated for uniqueness without collisions when db names contain dots).

### 4.4 Index Storage Format

Index buckets use a composite key to allow multiple documents with the same index value:

```
Key:   <indexed-value-bytes> + <_id-bytes>
Value: empty (for non-unique indexes)
       <_id-bytes> (for unique indexes — enables fast existence check)
```

**Indexed value encoding:**
- Scalar values: type-prefixed bytes ensuring the correct BSON comparison order
- Compound indexes: concatenation of encoded fields, delimited by a fixed-width separator
- Null/missing fields: encoded as a type-0 marker byte so they sort before all real values (matching MongoDB's sort order)

Index traversal for a query uses a bbolt cursor with `Seek()` to jump to the first matching key, then iterates forward (for ascending) or uses `Prev()` (for descending) until the key prefix no longer matches.

### 4.5 Transaction Model

bbolt exposes two transaction types:

```go
// Read-only — multiple concurrent readers
db.View(func(tx *bbolt.Tx) error { ... })

// Read-write — serialized (one at a time)
db.Update(func(tx *bbolt.Tx) error { ... })
```

All write operations (`InsertOne`, `UpdateOne`, etc.) use `db.Update()`. This means:

- Write throughput is bounded by the single-writer model
- Read throughput scales with CPU (concurrent `View` calls)
- There are no multi-document transactions in the ACID sense — each `Update` call is its own atomic transaction

`syncOnWrite: true` (the default) calls `db.Sync()` after each transaction, which issues an `fsync`/`fdatasync` to guarantee durability. Set `syncOnWrite: false` for higher write throughput at the cost of potential data loss on a crash (up to the last unfsynced transaction).

---

## 5. Wire Protocol Details

### 5.1 OP_MSG Binary Format

OP_MSG (opcode 2013) is the primary message format since MongoDB 3.6. All modern drivers use it exclusively.

**Complete byte-by-byte layout:**

```
Offset  Size  Field
------  ----  -----
 0       4    messageLength    (int32, little-endian) — total message size including header
 4       4    requestID        (int32, little-endian) — client-assigned message ID
 8       4    responseTo       (int32, little-endian) — requestID of message being responded to
12       4    opCode           (int32, little-endian) — always 2013 (0x07D1) for OP_MSG
16       4    flagBits         (uint32, little-endian)
               Bit 0: checksumPresent — trailing 4-byte CRC-32C is present
               Bit 1: moreToCome      — sender won't wait for a reply
               Bit 16: exhaustAllowed — client accepts multiple replies
20       1+   sections[]       — one or more sections

Section Kind 0 (Body):
  kind       1    section type byte (0x00)
  document   N    BSON document — the command or response

Section Kind 1 (Document Sequence):
  kind       1    section type byte (0x01)
  size       4    int32 — total bytes of this section including the size field
  identifier N    cstring (null-terminated UTF-8) — e.g., "documents", "updates"
  documents  M    one or more BSON documents filling remaining bytes

[Optional CRC-32C]:
  checksum   4    uint32, little-endian (only present if flagBits bit 0 is set)
```

**Example: an `insertMany` OP_MSG with document sequences**

The driver packs the command document in a Body section and the documents array in a DocumentSeq section:

```
Body section (kind=0):
  {"insert": "users", "ordered": true, "$db": "myapp"}

DocumentSeq section (kind=1):
  identifier: "documents"
  documents:  [{name: "Alice"}, {name: "Bob"}]
```

This is more efficient than embedding all documents in the Body BSON because it avoids a double-copy through the BSON encoder.

**MongClone's handling:**

`readOpMsg` populates `OpMsgMessage.Body` from the first kind-0 section and appends kind-1 sections to `OpMsgMessage.Sequences`. The command dispatcher accesses them as:

```go
// Get the command document
cmdDoc := msg.Body

// Get bulk write documents (for insert, update, delete commands)
for _, seq := range msg.Sequences {
    if seq.Identifier == "documents" {
        // bulk insert documents
    }
    if seq.Identifier == "updates" {
        // bulk update specs
    }
}
```

**WriteOpMsg:**

Responses always use a single kind-0 body section (no document sequences in responses). The layout is:

```
header (16 bytes) + flagBits (4 bytes) + kind byte (1 byte, 0x00) + BSON response
Total = 21 + len(bsonDoc)
```

### 5.2 OP_QUERY and OP_REPLY (Legacy)

OP_QUERY (opcode 2004) is used by older drivers and by mongosh's initial handshake. It sends commands against the `<db>.$cmd` namespace.

**OP_QUERY layout:**

```
Offset  Size  Field
------  ----  -----
 0      16    header
16       4    flags            int32
20       N    fullCollectionName  cstring ("mydb.$cmd")
 +       4    numberToSkip     int32
 +       4    numberToReturn   int32 (-1 = first only)
 +       M    query            BSON — the command document
 +       P    returnFieldsSelector  BSON (optional projection)
```

`IsCommandQuery(msg)` returns true when `fullCollectionName` ends in `.$cmd`. `GetCommandDB(msg)` extracts the database name.

**OP_REPLY layout (server response to OP_QUERY):**

```
Offset  Size  Field
------  ----  -----
 0      16    header
16       4    responseFlags    int32
20       8    cursorID         int64 (0 = no cursor / exhausted)
28       4    startingFrom     int32
32       4    numberReturned   int32
36       N    documents[]      BSON docs × numberReturned
```

`WriteOpReply` encodes this layout and writes it to the connection in two writes: the fixed header + fields, then each document body.

### 5.3 Checksum Handling

When the `MsgFlagChecksumPresent` bit (bit 0) is set in OP_MSG `flagBits`, the last 4 bytes of the message are a CRC-32C checksum of the preceding bytes.

MongClone **reads but does not validate** the checksum (the MongoDB spec calls it optional). The parser accounts for the checksum by reducing the available section bytes by 4, then drains the 4 CRC bytes from the underlying reader to keep the connection synchronized.

Responses from MongClone set `flagBits = 0` (no checksum). This is valid and accepted by all drivers.

---

## 6. Query Evaluation

### 6.1 Filter Evaluation

The `query.Filter` function is called for every document during a collection scan (when no index covers the query). The logic is:

1. Iterate top-level keys in the filter document
2. Keys starting with `$` are logical operators (`$and`, `$or`, `$nor`, `$expr`, `$text`)
3. Other keys are field paths — look up the field in the document, then evaluate the condition

**Field condition evaluation:**

A field condition can be:
- A bare value: implicit `$eq` — `{name: "Alice"}` is equivalent to `{name: {$eq: "Alice"}}`
- An operator document: `{age: {$gt: 25}}` — each key is evaluated independently
- A mixed document with `$regex` + `$options`: handled as a unit via `evalRegexWithOptions`

**Short-circuit evaluation:**

`$and` short-circuits on the first false condition. `$or` short-circuits on the first true condition. `Filter` itself returns false as soon as any field condition fails, without evaluating remaining conditions.

### 6.2 Type Comparison Order

`compareValues(a, b bson.RawValue)` follows MongoDB's BSON comparison order exactly:

| Order | Type(s) |
|-------|---------|
| 1 | MinKey |
| 2 | Null, Undefined |
| 3 | Numbers (Double, Int32, Int64, Decimal128) — compared numerically |
| 4 | Symbol |
| 5 | String — lexicographic |
| 6 | Object/EmbeddedDocument — field-by-field comparison |
| 7 | Array — element-by-element comparison |
| 8 | BinData — by subtype, then by bytes |
| 9 | ObjectID — byte comparison (embeds timestamp) |
| 10 | Boolean — false < true |
| 11 | DateTime — milliseconds since epoch |
| 12 | Timestamp — by T, then by I |
| 13 | Regex — by pattern string, then by options string |
| 14 | MaxKey |

Numbers of different types are coerced to `float64` for comparison. This matches MongoDB behavior where `{x: {$eq: 1}}` matches `{x: 1}` (int32), `{x: 1.0}` (double), and `{x: NumberLong(1)}` (int64).

### 6.3 Dot Notation Traversal

`getField(doc bson.Raw, path string)` implements MongoDB dot notation:

```
"address.city"          → doc["address"]["city"]
"scores.0"              → doc["scores"][0]  (array index)
"items.name"            → collect doc["items"][*]["name"] for array traversal
"a.b.c"                 → doc["a"]["b"]["c"]  (arbitrary depth)
```

The implementation uses `strings.SplitN(path, ".", 2)` recursively:
- If the current value is a document: recurse into subdocument
- If the current value is an array and the next path segment is a valid integer: return that element
- If the current value is an array and the next segment is not an integer: traverse into embedded documents inside the array

Missing fields return `bson.RawValue{Type: bson.TypeUndefined}`.

---

## 7. BSON Handling

MongClone uses `go.mongodb.org/mongo-driver/v2/bson` throughout.

### 7.1 `bson.Raw` vs `bson.D`

| Type | Description | Use When |
|------|-------------|----------|
| `bson.Raw` | Raw BSON bytes, zero-copy | Storage, wire protocol, passing documents through the system |
| `bson.D` | Ordered slice of `{Key, Value}` pairs | Building response documents, constructing filters/updates in code |
| `bson.M` | `map[string]interface{}` | Quick unmarshaling in tests, non-performance-critical code |
| `bson.A` | `[]interface{}` | Arrays in `bson.D` values |

**Critical rule:** `bson.Raw` is just a byte slice — it aliases the underlying buffer. If you need to retain a `bson.Raw` beyond the current transaction or read buffer, copy it:

```go
// WRONG — raw may be backed by a reused buffer
savedDoc = doc

// CORRECT — explicit copy
savedDoc = make(bson.Raw, len(doc))
copy(savedDoc, doc)
```

### 7.2 Marshaling Patterns

**Building a response document:**

```go
import "go.mongodb.org/mongo-driver/v2/bson"

// Use bson.D for ordered keys (required for wire protocol command responses)
response, err := bson.Marshal(bson.D{
    {"ok", int32(1)},
    {"n", int64(matchedCount)},
    {"nModified", int64(modifiedCount)},
})
```

**Marshaling a Go struct:**

```go
type UserInfo struct {
    User   string   `bson:"user"`
    DB     string   `bson:"db"`
    Roles  []RoleDoc `bson:"roles"`
}

raw, err := bson.Marshal(UserInfo{User: "alice", DB: "myapp", Roles: roles})
```

**Unmarshaling from `bson.Raw`:**

```go
// Into a struct
var result UserInfo
if err := bson.Unmarshal(doc, &result); err != nil {
    return err
}

// Into bson.D (preserves order)
var d bson.D
if err := bson.Unmarshal(doc, &d); err != nil {
    return err
}

// Direct field access without full unmarshal (fast path)
val, err := doc.LookupErr("fieldName")  // returns bson.RawValue
```

**Accessing a specific field type:**

```go
val, err := cmd.LookupErr("find")
if err != nil {
    return nil, fmt.Errorf("find: missing 'find' field")
}
collName := val.StringValue()  // panics if not a string — use val.Type check first
```

---

## 8. Adding a New Command

This is a step-by-step walkthrough for adding a hypothetical `copydb` command.

**Step 1: Add the handler function**

Create or add to `internal/commands/admin_handlers.go`:

```go
package commands

import (
    "fmt"
    "go.mongodb.org/mongo-driver/v2/bson"
)

// handleCopyDB implements the copydb command.
// Command format: {copydb: 1, fromdb: "source", todb: "destination"}
func handleCopyDB(ctx *Context, cmd bson.Raw) (bson.Raw, error) {
    // Extract required fields
    fromDBVal, err := cmd.LookupErr("fromdb")
    if err != nil {
        return nil, &storage.MongoError{Code: storage.ErrCodeBadValue, Message: "copydb requires 'fromdb'"}
    }
    toDB Val, err := cmd.LookupErr("todb")
    if err != nil {
        return nil, &storage.MongoError{Code: storage.ErrCodeBadValue, Message: "copydb requires 'todb'"}
    }

    fromDB := fromDBVal.StringValue()
    toDB := toDBVal.StringValue()

    // Authorization check
    if !ctx.Auth.HasPermission(ctx.Username, fromDB, "read") {
        return nil, &storage.MongoError{Code: storage.ErrCodeUnauthorized, Message: "not authorized"}
    }
    if !ctx.Auth.HasPermission(ctx.Username, toDB, "readWrite") {
        return nil, &storage.MongoError{Code: storage.ErrCodeUnauthorized, Message: "not authorized"}
    }

    // Business logic
    colls, err := ctx.Engine.ListCollections(fromDB)
    if err != nil {
        return nil, fmt.Errorf("copydb: list collections: %w", err)
    }

    for _, coll := range colls {
        if err := copyCollection(ctx, fromDB, toDB, coll.Name); err != nil {
            return nil, fmt.Errorf("copydb: copy %s: %w", coll.Name, err)
        }
    }

    // Build success response — always include "ok": 1
    resp, err := bson.Marshal(bson.D{{"ok", int32(1)}})
    if err != nil {
        return nil, err
    }
    return resp, nil
}
```

**Step 2: Register the handler in the dispatcher**

In `internal/commands/dispatcher.go` (or wherever `NewDispatcher` registers handlers):

```go
func NewDispatcher(engine storage.Engine, auth *auth.Manager, logger *zap.Logger) *Dispatcher {
    d := &Dispatcher{
        handlers: make(map[string]Handler),
    }

    // ... existing registrations ...
    d.handlers["copydb"] = handleCopyDB

    return d
}
```

**Step 3: Write a test**

```go
// internal/commands/admin_handlers_test.go
package commands_test

import (
    "testing"
    "go.mongodb.org/mongo-driver/v2/bson"
    "github.com/inder/mongoclone/internal/commands"
    "github.com/inder/mongoclone/internal/storage"
)

func TestHandleCopyDB(t *testing.T) {
    engine := storage.NewMemoryEngine()  // or bbolt engine pointed at t.TempDir()
    dispatcher := commands.NewDispatcher(engine, nil, zaptest.NewLogger(t))
    ctx := &commands.Context{DB: "admin", Engine: engine}

    // Seed source database
    src, _ := engine.Collection("sourcedb", "items")
    src.InsertOne(bson.D{{"name", "test"}})

    // Execute
    cmd, _ := bson.Marshal(bson.D{
        {"copydb", 1},
        {"fromdb", "sourcedb"},
        {"todb", "destdb"},
    })
    resp := dispatcher.Dispatch(ctx, cmd)

    // Assert
    ok, _ := resp.LookupErr("ok")
    if ok.Int32() != 1 {
        t.Fatalf("expected ok=1, got %v", ok)
    }

    dst, _ := engine.Collection("destdb", "items")
    count, _ := dst.CountDocuments(nil)
    if count != 1 {
        t.Fatalf("expected 1 document in destdb.items, got %d", count)
    }
}
```

**Step 4: Document in ARCHITECTURE.md**

Add `copydb` to the "Supported MongoDB Commands" section.

---

## 9. Adding a New Query Operator

This walkthrough adds a hypothetical `$containsKey` operator: `{doc: {$containsKey: "fieldName"}}` — matches documents where `doc` is an embedded document containing the key `fieldName`.

**Step 1: Add the evaluation function in `internal/query/filter.go`**

```go
// evalContainsKey checks if fieldVal (an embedded document) contains the given key.
func evalContainsKey(fieldVal bson.RawValue, opVal bson.RawValue) (bool, error) {
    if fieldVal.Type != bson.TypeEmbeddedDocument {
        return false, nil  // non-document never contains a key
    }
    if opVal.Type != bson.TypeString {
        return false, fmt.Errorf("$containsKey requires a string argument")
    }
    keyName := opVal.StringValue()

    doc, err := fieldVal.Document()
    if err != nil {
        return false, err
    }
    _, err = doc.LookupErr(keyName)
    return err == nil, nil
}
```

**Step 2: Add the case to `evalOperator`**

In the `switch op` block in `evalOperator`:

```go
case "$containsKey":
    return evalContainsKey(fieldVal, opVal)
```

**Step 3: Write a test**

```go
// internal/query/filter_test.go
func TestContainsKey(t *testing.T) {
    doc, _ := bson.Marshal(bson.D{
        {"meta", bson.D{{"author", "alice"}, {"version", 2}}},
    })

    // Should match
    filter1, _ := bson.Marshal(bson.D{{"meta", bson.D{{"$containsKey", "author"}}}})
    match, err := query.Filter(doc, filter1)
    assert.NoError(t, err)
    assert.True(t, match)

    // Should not match
    filter2, _ := bson.Marshal(bson.D{{"meta", bson.D{{"$containsKey", "nonexistent"}}}})
    match, err = query.Filter(doc, filter2)
    assert.NoError(t, err)
    assert.False(t, match)
}
```

That's it. The operator is immediately available in `find`, `updateOne` (filter side), `deleteMany`, and everywhere else `query.Filter` is called.

---

## 10. Adding a New Aggregation Stage

This walkthrough adds a `$reverse` stage that reverses the order of documents in the pipeline (a simplified example).

**Step 1: Define the stage struct in `internal/aggregation/`**

Create `internal/aggregation/stage_reverse.go`:

```go
package aggregation

import (
    "context"
    "go.mongodb.org/mongo-driver/v2/bson"
)

// reverseStage reverses the order of all input documents.
// It must fully buffer input — not suitable for unbounded streams.
type reverseStage struct{}

func newReverseStage(spec bson.Raw) (*reverseStage, error) {
    // $reverse takes no arguments (or a spec with options)
    return &reverseStage{}, nil
}

func (s *reverseStage) Process(ctx context.Context, input <-chan bson.Raw) (<-chan bson.Raw, error) {
    out := make(chan bson.Raw)

    go func() {
        defer close(out)

        // Buffer all input
        var docs []bson.Raw
        for doc := range input {
            docs = append(docs, doc)
        }

        // Emit in reverse order
        for i := len(docs) - 1; i >= 0; i-- {
            select {
            case out <- docs[i]:
            case <-ctx.Done():
                return
            }
        }
    }()

    return out, nil
}
```

**Step 2: Register the stage in the pipeline executor**

In `internal/aggregation/pipeline.go`, in the stage factory switch:

```go
func buildStage(stageName string, spec bson.Raw) (Stage, error) {
    switch stageName {
    case "$match":
        return newMatchStage(spec)
    case "$project":
        return newProjectStage(spec)
    // ... other stages ...
    case "$reverse":
        return newReverseStage(spec)
    default:
        return nil, fmt.Errorf("unrecognized pipeline stage: %s", stageName)
    }
}
```

**Step 3: Write a test**

```go
func TestReverseStage(t *testing.T) {
    engine := storage.NewMemoryEngine()
    coll, _ := engine.Collection("testdb", "nums")
    coll.InsertMany([]bson.Raw{
        mustMarshal(bson.D{{"n", 1}}),
        mustMarshal(bson.D{{"n", 2}}),
        mustMarshal(bson.D{{"n", 3}}),
    }, storage.InsertOptions{Ordered: true})

    pipeline := []bson.Raw{
        mustMarshal(bson.D{{"$reverse", bson.D{}}}),
    }
    cursor, err := aggregation.Execute(coll, engine, "testdb", pipeline, aggregation.PipelineOptions{})
    require.NoError(t, err)

    docs, _, _ := cursor.NextBatch(100)
    require.Len(t, docs, 3)
    // docs should be [{n:3}, {n:2}, {n:1}]
    assert.Equal(t, int32(3), mustLookupInt32(docs[0], "n"))
    assert.Equal(t, int32(1), mustLookupInt32(docs[2], "n"))
}
```

---

## 11. Plugging in a Different Storage Engine

The `storage.Engine` interface is the seam between command handling and persistence. You can swap the bbolt implementation for anything — an in-memory engine for tests, a RocksDB backend, a remote storage adapter, etc.

**Minimum viable implementation checklist:**

1. Implement `storage.Engine` — all methods
2. Implement `storage.Collection` — all CRUD methods
3. Implement `storage.Cursor` — `NextBatch`, `Close`, `ID`
4. Implement `storage.CursorStore` — `Register`, `Get`, `Delete`, `DeleteMany`, `Cleanup`
5. Implement `storage.UserStore` — `CreateUser`, `GetUser`, `UpdateUser`, `DeleteUser`, `ListUsers`, `HasUser`

**In-memory engine skeleton (useful for tests):**

```go
package storage

import (
    "sync"
    "go.mongodb.org/mongo-driver/v2/bson"
)

type MemoryEngine struct {
    mu   sync.RWMutex
    dbs  map[string]map[string][]bson.Raw  // db → collection → documents
    // ... index state, cursor store, user store ...
}

func NewMemoryEngine() *MemoryEngine {
    return &MemoryEngine{dbs: make(map[string]map[string][]bson.Raw)}
}

func (e *MemoryEngine) Collection(db, coll string) (Collection, error) {
    e.mu.Lock()
    defer e.mu.Unlock()
    if e.dbs[db] == nil {
        e.dbs[db] = make(map[string][]bson.Raw)
    }
    return &memCollection{engine: e, db: db, name: coll}, nil
}

// ... implement remaining Engine methods ...
```

**Wiring a custom engine:**

In `cmd/mongod/main.go`, replace the `server.New(cfg, log)` call — `server.New` currently constructs the bbolt engine internally. To use a custom engine, you'd need to extend `server.New` (or add a `server.NewWithEngine(cfg, engine, log)` variant) that accepts a pre-built engine:

```go
myEngine := mypackage.NewCustomEngine(customConfig)
srv, err := server.NewWithEngine(cfg, myEngine, log)
```

---

## 12. Testing

### 12.1 Unit Test Approach

Unit tests live alongside the code they test in `_test.go` files. All packages except `internal/server` (which requires a network) are fully testable without any external dependencies.

**Testing query evaluation:**

```go
// internal/query/filter_test.go
package query_test

import (
    "testing"
    "github.com/inder/mongoclone/internal/query"
    "go.mongodb.org/mongo-driver/v2/bson"
)

func mustMarshal(v interface{}) bson.Raw {
    b, err := bson.Marshal(v)
    if err != nil {
        panic(err)
    }
    return b
}

func TestFilterGt(t *testing.T) {
    doc := mustMarshal(bson.D{{"age", int32(30)}})
    filter := mustMarshal(bson.D{{"age", bson.D{{"$gt", int32(25)}}}})

    match, err := query.Filter(doc, filter)
    if err != nil {
        t.Fatal(err)
    }
    if !match {
        t.Error("expected match")
    }
}
```

**Testing the wire protocol:**

```go
// internal/wire/op_msg_test.go
package wire_test

import (
    "bytes"
    "testing"
    "github.com/inder/mongoclone/internal/wire"
    "go.mongodb.org/mongo-driver/v2/bson"
)

func TestOpMsgRoundTrip(t *testing.T) {
    body, _ := bson.Marshal(bson.D{{"ping", int32(1)}, {"$db", "admin"}})

    // Write
    var buf bytes.Buffer
    err := wire.WriteOpMsg(&buf, 1, 0, 0, body)
    if err != nil {
        t.Fatal(err)
    }

    // Read back
    msg, err := wire.ReadMessage(bytes.NewReader(buf.Bytes()))
    // ... assertions ...
}
```

**Running tests:**

```bash
# All tests with race detector
go test -race ./...

# Specific package
go test -race ./internal/query/...

# With verbose output
go test -race -v ./internal/wire/...

# Benchmarks
go test -bench=. -benchmem ./internal/query/...
```

### 12.2 Integration Tests

Integration tests are tagged with `//go:build integration` and require a running MongClone instance. They use the official Go MongoDB driver to exercise the full stack.

**Setup:**

```go
//go:build integration

package integration_test

import (
    "context"
    "testing"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func newTestClient(t *testing.T) *mongo.Client {
    t.Helper()
    uri := "mongodb://localhost:27017/?directConnection=true"
    client, err := mongo.Connect(options.Client().ApplyURI(uri))
    if err != nil {
        t.Fatalf("connect: %v", err)
    }
    t.Cleanup(func() { client.Disconnect(context.Background()) })
    return client
}

func TestInsertAndFind(t *testing.T) {
    client := newTestClient(t)
    coll := client.Database("integration_test").Collection("things")

    // Clean up before test
    coll.Drop(context.Background())

    // Insert
    _, err := coll.InsertOne(context.Background(), bson.D{{"name", "Alice"}, {"age", 30}})
    if err != nil {
        t.Fatal(err)
    }

    // Find
    var result bson.M
    err = coll.FindOne(context.Background(), bson.D{{"name", "Alice"}}).Decode(&result)
    if err != nil {
        t.Fatal(err)
    }
    if result["age"] != int32(30) {
        t.Errorf("expected age 30, got %v", result["age"])
    }
}
```

**Run integration tests:**

```bash
# Start MongClone in the background
make dev &
sleep 1

# Run integration tests
make test-integration
# Equivalent to: go test -race -v -count=1 -tags integration ./...
```

---

## 13. Performance Notes

### bbolt Single-Writer Limitation

The most significant bottleneck is bbolt's single-writer model. All write operations (`insert`, `update`, `delete`, `findAndModify`) serialize through a single `db.Update()` call per database file.

**Implication:** Write throughput for a single database tops out at roughly `1 / (average_write_latency)` operations/second. On SSDs with `syncOnWrite: false`, expect ~10,000–50,000 writes/second. With `syncOnWrite: true` (default), throughput is limited by `fsync` latency (~1,000–5,000 ops/s on most disks).

**Mitigations:**
- Use `syncOnWrite: false` if you can tolerate ~1 transaction of potential data loss on crash
- Distribute writes across multiple databases (each has its own bbolt file and write lock)
- Batch inserts via `insertMany` — a single `db.Update()` call handles the entire batch

### Cursor Memory

Each open cursor holds a slice of `bson.Raw` documents buffered in memory. For large result sets, use pagination (limit/skip) or streaming via driver cursor iteration rather than fetching all documents at once.

The `CursorStore.Cleanup(maxIdleSecs)` goroutine removes cursors idle longer than `maxIdleSecs`. By default, cursors timeout after `LogicalSessionTimeoutMinutes * 60` seconds (1800 seconds = 30 minutes). Long-idle cursors that are never closed are a memory leak vector — always close cursors in application code.

### Aggregation Memory

Stages that require full materialization (`$sort`, `$group`, `$facet`, `$bucket`) buffer all input documents. The `maxAggregationMemory` limit (default 100MB) is a soft limit per pipeline execution. Exceeding it returns an error unless `allowDiskUse: true` is set, in which case results spill to a temporary file.

For large aggregations, consider:
- Adding a `$match` stage early in the pipeline to reduce document count
- Using indexes to avoid collection scans in the `$match` stage
- Breaking complex pipelines into two stages: aggregate into a temp collection with `$out`, then aggregate the temp collection

### Index Traversal

Index scans are significantly faster than collection scans for selective queries. The query planner selects an index if:
- The query filter references the first field of a compound index
- A `hint` is specified

Without index hints, MongClone currently uses the first applicable index it finds. There is no cost-based optimizer — if you need a specific index, use `.hint()`.

### Connection Overhead

Each connection spawns a goroutine (~2KB stack initially, grows on demand). With `maxConnections: 1000`, expect ~50MB of goroutine overhead at full capacity. The Go scheduler handles this efficiently; 1000 concurrent goroutines is not a problem on modern hardware.

---

## 14. Security Notes

### Authentication Implementation

- SCRAM-SHA-256 is the only supported mechanism
- `$2` (SCRAM-SHA-1) is not implemented — any driver that forces SHA-1 will fail authentication
- Passwords are derived via PBKDF2-SHA256 with 15,000 iterations; brute-forcing stored credentials is computationally expensive
- `StoredKey` = SHA256(HMAC-SHA256(SaltedPassword, "Client Key")) — even if the database is compromised, attackers cannot directly authenticate without brute-forcing
- Conversations expire: incomplete SASL conversations that are never finished are cleaned up to prevent memory accumulation

### Why `$where` is Disabled

`$where` executes arbitrary JavaScript via an interpreter. MongClone intentionally returns:

```
{"ok": 0, "errmsg": "$where is not supported for security reasons", "code": 2}
```

Reasons:
1. MongClone has no embedded JavaScript engine
2. `$where` has been a source of injection vulnerabilities historically
3. Every legitimate `$where` use case can be replaced with native operators

If you're migrating from MongoDB and have `$where` in your queries, convert them. The native operators are faster anyway.

### TLS

- Mutual TLS (client certificate verification) is not yet implemented
- TLS 1.2+ is used by Go's `crypto/tls` by default
- In production, use a certificate from a trusted CA — self-signed certs require `tlsAllowInvalidCertificates: true` on clients, which disables certificate chain validation

### Audit Logging

When `auditLog` is configured, MongClone writes structured JSON records for:
- Authentication attempts (success and failure)
- `createUser`, `updateUser`, `dropUser`
- `createCollection`, `drop`, `dropDatabase`
- `createIndexes`, `dropIndexes`

Audit logs do not include query predicates or document contents — only metadata about the operation. This is intentional to avoid logging sensitive data while still providing a compliance-grade audit trail.

### Input Validation

- Maximum document size is enforced at the wire layer (default 16MB, configurable)
- Maximum message size is 48MB — messages exceeding this are rejected before parsing
- BSON documents with negative length or length < 5 are rejected immediately
- Unknown opcodes have their bodies drained and are silently ignored (prevents connection desync)

---

## 15. Roadmap and Known Limitations

### Not Implemented

| Feature | Notes |
|---------|-------|
| Replication | No replica set protocol, no oplog, no change streams |
| Sharding | Single-node only — no mongos, no config servers |
| Change streams | No `$changeStream` aggregation stage |
| Multi-document transactions | `startTransaction`/`commitTransaction`/`abortTransaction` are accepted (for driver compatibility) but operate as no-ops — each operation is independently atomic |
| `$jsonSchema` validation | `createCollection` with a `validator` field stores the spec but doesn't enforce it |
| Geospatial operators | `$near`, `$geoWithin`, `$geoIntersects`, and all related operators return `not implemented` |
| Capped collections | `capped: true` is stored in collection metadata but overflow eviction is not implemented |
| OP_COMPRESSED | The wrapper is recognized (opcode 2012) but the body is discarded; compressed connections from drivers will not work |
| GridFS | Not a server-side feature — use the driver's GridFS layer over standard collections |
| Atlas Search | No Lucene-backed full-text or vector search |
| SCRAM-SHA-1 | Only SCRAM-SHA-256 is supported |
| `$where` | Intentionally disabled |
| `$jsonSchema` | Not implemented |
| Client-side field-level encryption (CSFLE) | Not implemented |
| Queryable encryption | Not implemented |
| Config reload (SIGHUP) | The signal is caught but full config diffing and live reload are not yet wired |
| Slow query log | Not yet implemented (use Prometheus metrics for latency monitoring) |
| Index build progress reporting | Indexes are always built synchronously within the write transaction |

### Near-Term Roadmap

1. **Geospatial operators** — 2dsphere index + `$geoWithin`/`$near` using Haversine
2. **Full config reload on SIGHUP** — live log level changes, rate limit updates without restart
3. **Slow query log** — log queries exceeding a configurable threshold to the audit log
4. **OP_COMPRESSED support** — parse the wrapper and decompress the inner OP_MSG
5. **Read preference** — `readPreference` parsing in the wire protocol (for drivers that always send it)
6. **`$jsonSchema` validation** — enforce validators defined on collections
7. **Capped collection eviction** — size-bounded collections with O(1) oldest-document eviction
8. **Transactions (real)** — bbolt supports multi-bucket atomic writes; multi-statement transactions over a single database are feasible
9. **Cross-database transactions** — requires two-phase commit across multiple bbolt files; hard but possible

### Contributing

1. Fork the repo
2. Create a branch: `git checkout -b feature/my-feature`
3. Write code + tests
4. Run `make test` and `make lint` — both must pass
5. Open a PR with a description of what, why, and how you tested it

The code review bar: clean interfaces, tests for new operators/stages, no `panic` in library code, and respect the single-writer constraint (don't try to hold bbolt write transactions across network round-trips).
