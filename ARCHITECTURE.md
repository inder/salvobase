# Salvobase Architecture

A MongoDB-compatible document database server written in Go.

## Module

```
github.com/inder/salvobase
```

## Package Layout

```
cmd/mongod/               Entry point, CLI flags, server bootstrap
internal/wire/            MongoDB wire protocol (binary encoding/decoding)
internal/storage/         Storage engine (bbolt-backed, per-database files)
internal/query/           Query evaluation (filter, update, projection, sort)
internal/aggregation/     Aggregation pipeline and expressions
internal/auth/            SCRAM-SHA-256 authentication + user management
internal/commands/        Command handlers (dispatched from connection handler)
internal/server/          TCP server, per-connection goroutine, metrics
configs/                  Default YAML config
docs/                     User + developer guides
```

## Core Interfaces

### Storage Engine (`internal/storage`)

```go
package storage

import (
    "go.mongodb.org/mongo-driver/v2/bson"
    "time"
)

// Engine is the top-level storage interface. One instance per server.
// Thread-safe: all methods may be called concurrently.
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

    // Get a collection handle (creates db+coll implicitly if they don't exist)
    Collection(db, coll string) (Collection, error)

    // Index management
    CreateIndex(db, coll string, spec IndexSpec) (string, error)
    DropIndex(db, coll, indexName string) error
    ListIndexes(db, coll string) ([]IndexInfo, error)

    // Stats
    DatabaseStats(db string) (DatabaseStats, error)
    CollectionStats(db, coll string) (CollectionStats, error)
    ServerStats() (ServerStats, error)

    // Cursor store (shared across all collections)
    Cursors() CursorStore

    // User management (stored in admin.$users)
    Users() UserStore

    Close() error
}

// Collection performs CRUD on a single collection. Thread-safe.
type Collection interface {
    InsertOne(doc bson.Raw) (bson.ObjectID, error)
    InsertMany(docs []bson.Raw) ([]bson.ObjectID, error)

    // Find returns a Cursor. Caller must close the cursor.
    Find(filter bson.Raw, opts FindOptions) (Cursor, error)

    UpdateOne(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)
    UpdateMany(filter, update bson.Raw, opts UpdateOptions) (UpdateResult, error)

    ReplaceOne(filter, replacement bson.Raw, opts UpdateOptions) (UpdateResult, error)

    DeleteOne(filter bson.Raw) (int64, error)
    DeleteMany(filter bson.Raw) (int64, error)

    CountDocuments(filter bson.Raw) (int64, error)
    Distinct(field string, filter bson.Raw) ([]interface{}, error)

    Name() string
    Database() string
}

// Cursor is an iterator over query results.
type Cursor interface {
    // Returns the next batch of documents.
    // If firstBatch is true, returns up to batchSize from the start.
    // Returns (docs, cursorID, err). cursorID==0 means cursor is exhausted.
    NextBatch(batchSize int) ([]bson.Raw, int64, error)

    // Close releases cursor resources.
    Close() error

    ID() int64
}

// CursorStore manages server-side cursors (for getMore).
type CursorStore interface {
    Register(c Cursor) int64
    Get(id int64) (Cursor, bool)
    Delete(id int64)
    DeleteMany(ids []int64)
}

// UserStore manages database users.
type UserStore interface {
    CreateUser(u User) error
    UpdateUser(db, username string, update UserUpdate) error
    DeleteUser(db, username string) error
    GetUser(db, username string) (User, error)
    ListUsers(db string) ([]User, error)
}

// -- Data types --

type DatabaseInfo struct {
    Name       string
    SizeOnDisk int64
    Empty      bool
}

type CollectionInfo struct {
    Name    string
    Type    string // "collection" or "view"
    Options bson.Raw
    IDIndex bson.Raw
}

type IndexSpec struct {
    Name       string
    Keys       bson.Raw // e.g. {"field": 1} or {"field": -1} or {"field": "text"}
    Unique     bool
    Sparse     bool
    Background bool
    ExpireAfterSeconds *int32 // TTL index
    PartialFilterExpression bson.Raw
    Weights    bson.Raw // for text indexes
    Default_language string
    Language_override string
    TextIndexVersion int32
    WildcardProjection bson.Raw
    Hidden bool
}

type IndexInfo struct {
    Name       string
    Keys       bson.Raw
    Unique     bool
    Sparse     bool
    ExpireAfterSeconds *int32
    NS         string // "db.collection"
    V          int32  // index version (always 2)
}

type FindOptions struct {
    Skip       int64
    Limit      int64
    BatchSize  int32
    Sort       bson.Raw
    Projection bson.Raw
    Hint       bson.Raw
    Comment    string
    MaxTimeMS  int64
    Tailable   bool
    NoCursorTimeout bool
    AllowPartialResults bool
    AllowDiskUse bool
}

type UpdateOptions struct {
    Upsert       bool
    Multi        bool // true = updateMany
    ArrayFilters []bson.Raw
    BypassDocumentValidation bool
}

type InsertOptions struct {
    Ordered bool // default true
    BypassDocumentValidation bool
}

type CreateCollectionOptions struct {
    Capped   bool
    Size     int64
    Max      int64
    Validator bson.Raw
    ValidationLevel string
    ValidationAction string
}

type UpdateResult struct {
    MatchedCount  int64
    ModifiedCount int64
    UpsertedCount int64
    UpsertedID    interface{}
}

type DatabaseStats struct {
    DB          string
    Collections int32
    Views       int32
    Objects     int64
    AvgObjSize  float64
    DataSize    float64
    StorageSize float64
    Indexes     int32
    IndexSize   float64
}

type CollectionStats struct {
    NS          string
    Count       int64
    Size        int64
    AvgObjSize  float64
    StorageSize int64
    Capped      bool
    Nindexes    int32
    IndexSizes  map[string]int64
}

type ServerStats struct {
    Host            string
    Version         string
    Process         string
    PID             int64
    Uptime          int64
    UptimeMillis    int64
    UptimeEstimate  int64
    LocalTime       time.Time
    Connections     ConnectionStats
    OpCounters      OpCounters
    Mem             MemStats
}

type ConnectionStats struct {
    Current      int32
    Available    int32
    TotalCreated int64
}

type OpCounters struct {
    Insert  int64
    Query   int64
    Update  int64
    Delete  int64
    GetMore int64
    Command int64
}

type MemStats struct {
    Bits              int32
    Resident          int32
    Virtual           int32
}

type User struct {
    ID       string
    DB       string
    Username string
    PasswordHash string // SCRAM-SHA-256 stored key
    Roles    []Role
    CustomData bson.Raw
}

type UserUpdate struct {
    Password *string
    Roles    []Role
    CustomData bson.Raw
}

type Role struct {
    Role string
    DB   string
}
```

### Wire Protocol (`internal/wire`)

```go
package wire

import (
    "go.mongodb.org/mongo-driver/v2/bson"
    "io"
    "net"
)

// Opcode constants
const (
    OpReply        = 1
    OpUpdate       = 2001
    OpInsert       = 2002
    OpQuery        = 2004
    OpGetMore      = 2005
    OpDelete       = 2006
    OpKillCursors  = 2007
    OpMsg          = 2013
    OpCompressed   = 2012
)

// Header is the 16-byte MongoDB message header.
type Header struct {
    MessageLength int32
    RequestID     int32
    ResponseTo    int32
    OpCode        int32
}

// Message is the common interface for all wire protocol messages.
type Message interface {
    Header() Header
}

// OpMsgMessage represents an OP_MSG message (MongoDB 3.6+).
type OpMsgMessage struct {
    Hdr       Header
    FlagBits  uint32
    Body      bson.Raw        // Section Type 0 (the command document)
    Sequences []DocumentSeq   // Section Type 1 (document sequences)
}

// DocumentSeq is an OP_MSG Section Type 1 payload.
type DocumentSeq struct {
    Identifier string
    Documents  []bson.Raw
}

// OpQueryMessage represents a legacy OP_QUERY message.
type OpQueryMessage struct {
    Hdr                  Header
    Flags                int32
    FullCollectionName   string
    NumberToSkip         int32
    NumberToReturn       int32
    Query                bson.Raw
    ReturnFieldsSelector bson.Raw
}

// OpReplyMessage represents an OP_REPLY response (legacy).
type OpReplyMessage struct {
    Hdr            Header
    ResponseFlags  int32
    CursorID       int64
    StartingFrom   int32
    NumberReturned int32
    Documents      []bson.Raw
}

// OpGetMoreMessage represents OP_GETMORE.
type OpGetMoreMessage struct {
    Hdr                Header
    FullCollectionName string
    NumberToReturn     int32
    CursorID           int64
}

// OpKillCursorsMessage represents OP_KILL_CURSORS.
type OpKillCursorsMessage struct {
    Hdr       Header
    CursorIDs []int64
}

// ReadMessage reads and parses one message from the reader.
func ReadMessage(r io.Reader) (Message, error)

// WriteOpMsg encodes and writes an OP_MSG response to the writer.
func WriteOpMsg(w io.Writer, requestID, responseTo int32, flagBits uint32, body bson.Raw) error

// WriteOpReply encodes and writes an OP_REPLY (legacy) to the writer.
func WriteOpReply(w io.Writer, requestID, responseTo int32, responseFlags int32, cursorID int64, startingFrom int32, docs []bson.Raw) error
```

### Query Engine (`internal/query`)

```go
package query

import (
    "go.mongodb.org/mongo-driver/v2/bson"
)

// Filter evaluates a MongoDB filter against a document.
// Returns true if the document matches the filter.
// filter may be nil (matches all documents).
func Filter(doc bson.Raw, filter bson.Raw) (bool, error)

// Apply applies a MongoDB update document to a document.
// Supports $set, $unset, $inc, $push, $pull, $addToSet, $pop,
// $rename, $mul, $min, $max, $currentDate, $setOnInsert, $bit.
// Returns the updated document.
func Apply(doc bson.Raw, update bson.Raw, isUpsert bool) (bson.Raw, error)

// Project applies a MongoDB projection to a document.
// Returns a new document with only (or excluding) the specified fields.
func Project(doc bson.Raw, projection bson.Raw) (bson.Raw, error)

// Sort returns a comparator function for sorting documents.
// sort is a bson.Raw like {"field": 1, "other": -1}.
func SortFunc(sort bson.Raw) (func(a, b bson.Raw) int, error)

// IsUpdateDoc returns true if doc uses update operators ($set, etc.)
// rather than being a replacement document.
func IsUpdateDoc(doc bson.Raw) bool
```

### Aggregation Pipeline (`internal/aggregation`)

```go
package aggregation

import (
    "go.mongodb.org/mongo-driver/v2/bson"
    "github.com/inder/salvobase/internal/storage"
)

// Pipeline executes a MongoDB aggregation pipeline against a collection.
// Returns a Cursor over the results.
func Execute(coll storage.Collection, engine storage.Engine, db string, pipeline []bson.Raw, opts PipelineOptions) (storage.Cursor, error)

type PipelineOptions struct {
    AllowDiskUse bool
    BatchSize    int32
    MaxTimeMS    int64
    Comment      string
    Let          bson.Raw // variables for $lookup/$merge
}
```

### Authentication (`internal/auth`)

```go
package auth

import (
    "go.mongodb.org/mongo-driver/v2/bson"
    "github.com/inder/salvobase/internal/storage"
)

// Manager handles SCRAM-SHA-256 authentication.
type Manager struct {
    users storage.UserStore
    // internal conversation tracking
}

func NewManager(users storage.UserStore) *Manager

// SASLStart handles the initial saslStart command.
// Returns (serverFirstMessage, conversationID, error).
func (m *Manager) SASLStart(db, mechanism string, payload []byte) ([]byte, int32, error)

// SASLContinue handles saslContinue commands.
// Returns (serverFinalMessage, done, error).
func (m *Manager) SASLContinue(conversationID int32, payload []byte) ([]byte, bool, error)

// HasPermission checks if a user has the required permission on a database.
func (m *Manager) HasPermission(username, db, action string) bool
```

### Commands (`internal/commands`)

```go
package commands

import (
    "go.mongodb.org/mongo-driver/v2/bson"
    "github.com/inder/salvobase/internal/storage"
    "github.com/inder/salvobase/internal/auth"
    "go.uber.org/zap"
)

// Context is the execution context for a command.
type Context struct {
    DB       string          // target database
    Engine   storage.Engine
    Auth     *auth.Manager
    Session  *Session        // may be nil if no session
    Logger   *zap.Logger
    ConnID   int64
    // Authenticated user (empty if noAuth mode)
    Username string
    UserDB   string
}

// Session represents a client session (for transactions).
type Session struct {
    ID    bson.Raw
    LSID  string
    TxnNumber int64
    InTransaction bool
}

// Handler is a function that handles a named MongoDB command.
type Handler func(ctx *Context, cmd bson.Raw) (bson.Raw, error)

// Dispatcher routes commands to their handlers.
type Dispatcher struct {
    handlers map[string]Handler
}

func NewDispatcher(engine storage.Engine, auth *auth.Manager, logger *zap.Logger) *Dispatcher

// Dispatch finds and executes the handler for a command document.
// cmd must contain a key whose name is the command name.
// Returns the response document (always including "ok": 1 or "ok": 0).
func (d *Dispatcher) Dispatch(ctx *Context, cmd bson.Raw) bson.Raw
```

### Server (`internal/server`)

```go
package server

import (
    "net"
    "go.uber.org/zap"
    "github.com/inder/salvobase/internal/storage"
    "github.com/inder/salvobase/internal/auth"
    "github.com/inder/salvobase/internal/commands"
)

// Config holds all server configuration.
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

// Server is the main server struct.
type Server struct {
    cfg        Config
    engine     storage.Engine
    auth       *auth.Manager
    dispatcher *commands.Dispatcher
    listener   net.Listener
    logger     *zap.Logger
    // metrics, rate limiters, connection tracking...
}

func New(cfg Config) (*Server, error)
func (s *Server) Run() error        // blocks until stopped
func (s *Server) Shutdown() error
```

## Wire Protocol Command Flow

```
Client (mongosh / driver)
  │
  │  TCP connection
  ▼
server.Server.acceptLoop()
  │  goroutine per connection
  ▼
server.Connection.serve()
  │  ReadMessage() → wire.OpMsgMessage
  ▼
commands.Dispatcher.Dispatch(ctx, cmd)
  │  routes by command name
  ▼
handler(ctx, cmd) → bson.Raw response
  │
  ▼
wire.WriteOpMsg()
  │
  ▼
Client receives response
```

## Storage Layout (bbolt)

Each database = one file: `<dataDir>/<dbname>.db`

Within each bbolt database:
```
Bucket "col.<collectionName>"
    Key: _id (12-byte ObjectID or user-provided key bytes)
    Value: BSON document (optionally compressed: snappy or zstd; default: none)

Bucket "idx.<collectionName>.<indexName>"
    Key: <indexed value bytes> + <_id bytes>
    Value: empty (or _id for non-unique lookup)

Bucket "_meta.collections"
    Key: collection name (UTF-8)
    Value: JSON-encoded CollectionInfo

Bucket "_meta.indexes"
    Key: "<collectionName>.<indexName>"
    Value: JSON-encoded IndexSpec
```

`admin.db` also contains:
```
Bucket "_users"
    Key: "<db>.<username>"
    Value: JSON-encoded User
```

## Supported MongoDB Commands

### Query / Write
- `find`, `insert`, `update`, `delete`
- `findAndModify`
- `getMore`, `killCursors`
- `count`, `distinct`
- `aggregate`

### Administration
- `createCollection`, `drop`, `dropDatabase`
- `listDatabases`, `listCollections`
- `renameCollection`

### Indexes
- `createIndexes`, `dropIndexes`, `listIndexes`

### Authentication
- `hello`, `isMaster`, `ismaster`
- `saslStart`, `saslContinue`, `logout`
- `createUser`, `updateUser`, `dropUser`, `usersInfo`

### Diagnostics
- `ping`, `buildInfo`, `serverStatus`
- `dbStats`, `collStats`, `indexStats`
- `explain`
- `whatsmyuri`, `connectionStatus`

### Session / Transaction
- `startSession`, `endSessions`
- `commitTransaction`, `abortTransaction`

## Improvements Over MongoDB Community

1. **Native Prometheus metrics** at `http://:<httpPort>/metrics` — no exporter needed
2. **HTTP/REST API** at `http://:<httpPort>/api/v1/` — JSON REST alongside wire protocol
3. **Per-tenant rate limiting** — configurable requests/second per database
4. **Built-in audit logging** — JSON log of all auth events + DDL (no Enterprise needed)
5. **Transparent compression** — Snappy/Zstd for stored documents (configurable per server)
6. **Better explain** — cost estimates and stage-level timing exposed in explain output
7. **Millisecond TTL precision** — TTL index cleanup runs every second (not MongoDB's 60s)
8. **Connection-level DB isolation** — `x-salvobase-tenant-db` header locks a connection to a DB
9. **Zero-downtime config reload** — SIGHUP reloads config without restart

## Version Constants

```go
const (
    MongoDBMaxWireVersion = 21   // MongoDB 7.0 compatibility
    MongoDBMinWireVersion = 0
    MaxBSONObjectSize     = 16 * 1024 * 1024  // 16MB
    MaxMessageSizeBytes   = 48 * 1024 * 1024  // 48MB
    MaxWriteBatchSize     = 100000
    LogicalSessionTimeoutMinutes = 30
)
```
