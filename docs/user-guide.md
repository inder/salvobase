# Salvobase User Guide

> **Salvobase** is a MongoDB-compatible document database server written in Go.
> It speaks the MongoDB wire protocol, which means you can point your existing
> drivers, tools, and applications at it without changing a line of application code.

**License:** Apache 2.0
**Wire Protocol Compatibility:** MongoDB 7.0 (max wire version 21)
**Module:** `github.com/inder/salvobase`

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Quick Start](#2-quick-start)
3. [Configuration](#3-configuration)
   - [YAML Config File](#31-yaml-config-file)
   - [CLI Flags](#32-cli-flags)
   - [Environment Variables](#33-environment-variables)
4. [Authentication](#4-authentication)
   - [Creating the Admin User](#41-creating-the-admin-user)
   - [Connecting with Credentials](#42-connecting-with-credentials)
   - [SCRAM-SHA-256 Details](#43-scram-sha-256-details)
5. [CRUD Operations](#5-crud-operations)
   - [Insert](#51-insert)
   - [Find / Query](#52-find--query)
   - [Update](#53-update)
   - [Delete](#54-delete)
   - [findAndModify / findOneAndUpdate](#55-findandmodify--findoneandupdate)
6. [Indexes](#6-indexes)
   - [Creating Indexes](#61-creating-indexes)
   - [Index Types](#62-index-types)
   - [Dropping and Listing Indexes](#63-dropping-and-listing-indexes)
   - [Index Hints](#64-index-hints)
7. [Aggregation Pipeline](#7-aggregation-pipeline)
8. [Multi-Tenancy](#8-multi-tenancy)
9. [User Management and Authorization](#9-user-management-and-authorization)
10. [Monitoring](#10-monitoring)
    - [Prometheus Metrics](#101-prometheus-metrics)
    - [HTTP Health Check](#102-http-health-check)
11. [REST API](#11-rest-api)
12. [Production Deployment](#12-production-deployment)
    - [systemd Service](#121-systemd-service)
    - [Resource Limits](#122-resource-limits)
    - [Data Directory](#123-data-directory)
    - [Backup Strategy](#124-backup-strategy)
    - [TLS](#125-tls)
13. [Compatibility Notes](#13-compatibility-notes)
14. [Improvements Over MongoDB Community](#14-improvements-over-mongodb-community)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Introduction

Salvobase implements the MongoDB binary wire protocol (OP_MSG and legacy OP_QUERY/OP_REPLY) so existing MongoDB drivers for Go, Python, Node.js, Java, Ruby, and every other language connect to it out of the box. It stores data using [bbolt](https://github.com/etcd-io/bbolt), an embedded B-tree key/value store, with one `.db` file per database on disk.

### What Salvobase is

- A single-binary, zero-dependency database server
- Wire-compatible with MongoDB 7.0 (advertises max wire version 21)
- Suitable for development environments, embedded use, multi-tenant SaaS backends, and any workload that doesn't require replication or sharding
- Embeddable as a Go library

### What Salvobase is not (yet)

- A replica set or sharded cluster — it is a single-node store
- A replacement for MongoDB Atlas in workloads requiring change streams, vector search, or full-text search at scale

### MongoDB Compatibility Guarantees

Salvobase passes commands through the same driver API as MongoDB Community, and response documents follow the MongoDB wire protocol shape. The following are verified compatible:

- All MongoDB drivers (Go driver v2, PyMongo, Node.js driver, etc.)
- `mongosh` interactive shell
- Standard CRUD, aggregation, and index operations
- SCRAM-SHA-256 authentication handshake

---

## 2. Quick Start

### Prerequisites

- Go 1.22 or later
- `mongosh` (optional, for interactive use)

### Build from Source

```bash
git clone https://github.com/inder/salvobase.git
cd salvobase
make build
# Binary is at ./bin/salvobase
```

### Start the Server

**Development mode (no auth, debug logging):**

```bash
make dev
# Equivalent to:
# mkdir -p ./data
# go run ./cmd/mongod --port 27017 --datadir ./data --logLevel debug --noauth
```

**Production mode:**

```bash
./bin/salvobase \
  --port 27017 \
  --datadir /var/lib/salvobase \
  --logLevel info \
  --logFormat json
```

On first start with authentication enabled, you must create an admin user before connecting (see [Authentication](#4-authentication)).

### Connect with mongosh

```bash
# No auth (dev mode)
mongosh "mongodb://localhost:27017"

# With auth
mongosh "mongodb://admin:yourpassword@localhost:27017/admin"
```

### Verify It's Running

```bash
curl http://localhost:27080/healthz
# {"status":"ok","uptime":42}
```

---

## 3. Configuration

Configuration is layered: defaults < YAML file < CLI flags < environment variables. CLI flags always win.

### 3.1 YAML Config File

The default config file location is `configs/mongod.yaml`. Pass a custom path with `--config /path/to/config.yaml` (not yet wired to cobra — copy the file and set env vars or flags as needed).

Full annotated example:

```yaml
server:
  # MongoDB wire protocol port
  port: 27017
  # HTTP/REST API + Prometheus metrics port (set to 0 to disable)
  httpPort: 27080
  # Bind address. Use 127.0.0.1 to restrict to localhost.
  bindIP: "0.0.0.0"
  # Maximum number of concurrent connections
  maxConnections: 1000
  # Per-connection read/write timeout in seconds (0 = no timeout)
  connectionTimeoutSecs: 0

storage:
  # One .db file per database is created here
  dataDir: "/var/lib/salvobase"
  # Document compression: none | snappy | zstd
  compression: "none"
  # Sync writes to disk before acknowledging (safer but ~30% slower)
  syncOnWrite: true
  # Maximum database file size in bytes (0 = unlimited)
  maxDBSize: 0

auth:
  # Disable authentication — ONLY for local development
  noAuth: false
  # Admin user created on first start if it doesn't exist
  adminUser: "admin"
  # Set via MONGOCLONE_AUTH_ADMIN_PASSWORD env var in production
  adminPassword: ""

logging:
  # debug | info | warn | error
  level: "info"
  # json (production) | console (human-readable)
  format: "json"
  # Audit log path — empty disables it
  # Logs: auth events, DDL (createCollection, drop, createIndex, etc.)
  auditLog: ""

limits:
  # Per-database rate limit in requests/second (0 = unlimited)
  requestsPerSecond: 0
  # Maximum BSON document size in bytes (MongoDB default: 16MB)
  maxDocumentSize: 16777216
  # Max aggregation pipeline memory in bytes (0 = unlimited)
  maxAggregationMemory: 104857600  # 100MB

replication:
  # Not implemented — single-node only
  enabled: false
```

### 3.2 CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `27017` | MongoDB wire protocol port |
| `--httpPort` | `27080` | HTTP/REST + metrics port (0 to disable) |
| `--bind_ip` | `0.0.0.0` | IP address to listen on |
| `--maxConns` | `1000` | Maximum concurrent connections |
| `--datadir` | `./data` | Directory for database files |
| `--compression` | `none` | Document compression: `none`, `snappy`, `zstd` |
| `--syncOnWrite` | `true` | Sync writes before acknowledging |
| `--noauth` | `false` | Disable authentication (dev only) |
| `--logLevel` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--logFormat` | `json` | Log format: `json`, `console` |
| `--auditLog` | `` | Audit log file path |
| `--tls` | `false` | Enable TLS |
| `--tlsCert` | `` | TLS certificate file path |
| `--tlsKey` | `` | TLS private key file path |
| `--maxDocSize` | `16777216` | Max BSON document size in bytes |
| `--rateLimit` | `0` | Per-database rate limit (req/s, 0 = unlimited) |

### 3.3 Environment Variables

Every configuration option can be set via environment variables using the `MONGOCLONE_` prefix with `_` separating words. The environment variable name is the flag name uppercased with `MONGOCLONE_` prepended.

| Environment Variable | Equivalent Flag |
|----------------------|----------------|
| `MONGOCLONE_PORT` | `--port` |
| `MONGOCLONE_HTTP_PORT` | `--httpPort` |
| `MONGOCLONE_BIND_IP` | `--bind_ip` |
| `MONGOCLONE_DATA_DIR` | `--datadir` |
| `MONGOCLONE_COMPRESSION` | `--compression` |
| `MONGOCLONE_NO_AUTH` | `--noauth` |
| `MONGOCLONE_LOG_LEVEL` | `--logLevel` |
| `MONGOCLONE_AUDIT_LOG` | `--auditLog` |
| `MONGOCLONE_TLS` | `--tls` |
| `MONGOCLONE_TLS_CERT` | `--tlsCert` |
| `MONGOCLONE_TLS_KEY` | `--tlsKey` |
| `MONGOCLONE_AUTH_ADMIN_PASSWORD` | Admin password on first boot |

Example for a Docker container:

```bash
docker run -d \
  -e MONGOCLONE_PORT=27017 \
  -e MONGOCLONE_DATA_DIR=/data \
  -e MONGOCLONE_AUTH_ADMIN_PASSWORD=supersecret \
  -v /host/data:/data \
  -p 27017:27017 \
  -p 27080:27080 \
  salvobase:latest
```

### Zero-Downtime Config Reload

Send `SIGHUP` to the running process to trigger a config reload without restarting:

```bash
kill -HUP $(pidof salvobase)
```

---

## 4. Authentication

Salvobase uses **SCRAM-SHA-256** exclusively — the same mechanism MongoDB uses by default since version 4.0. Passwords are never stored in plaintext; they are stored as SCRAM derived keys (StoredKey + ServerKey + salt).

> If you start with `--noauth`, the server logs a loud warning. Never run without auth in any networked environment.

### 4.1 Creating the Admin User

**Method 1: CLI command (before starting the server)**

```bash
./bin/salvobase admin create-user admin yourpassword
```

This creates the user directly in the data files without requiring a running server.

**Method 2: Environment variable on first boot**

```bash
MONGOCLONE_AUTH_ADMIN_PASSWORD=yourpassword ./bin/salvobase --datadir /var/lib/salvobase
```

If the `admin` user does not exist on startup and `adminPassword` is non-empty, the server creates it automatically with the `root` role on the `admin` database.

**Method 3: mongosh after connecting (requires an existing admin or noauth)**

```javascript
use admin
db.createUser({
  user: "admin",
  pwd: "yourpassword",
  roles: [{ role: "root", db: "admin" }]
})
```

### 4.2 Connecting with Credentials

**mongosh:**

```bash
mongosh "mongodb://admin:yourpassword@localhost:27017/admin"
```

**Go driver:**

```go
import "go.mongodb.org/mongo-driver/v2/mongo"
import "go.mongodb.org/mongo-driver/v2/mongo/options"

ctx := context.Background()
clientOpts := options.Client().ApplyURI(
    "mongodb://admin:yourpassword@localhost:27017/?authSource=admin",
)
client, err := mongo.Connect(clientOpts)
```

**Python (PyMongo):**

```python
from pymongo import MongoClient

client = MongoClient(
    "mongodb://admin:yourpassword@localhost:27017/",
    authSource="admin"
)
```

**Node.js:**

```javascript
const { MongoClient } = require('mongodb');

const client = new MongoClient(
  'mongodb://admin:yourpassword@localhost:27017/?authSource=admin'
);
await client.connect();
```

### 4.3 SCRAM-SHA-256 Details

The authentication handshake follows RFC 5802. Salvobase:

- Derives credentials using PBKDF2-SHA-256 with 15,000 iterations and a 16-byte random salt
- Stores only `StoredKey` (H(ClientKey)) and `ServerKey` (HMAC(SaltedPassword, "Server Key")) — never the raw password or `SaltedPassword`
- Supports the full two-round SCRAM conversation (`saslStart` → `saslContinue`)
- Advertises `SCRAM-SHA-256` as the only supported mechanism in the `hello` response

---

## 5. CRUD Operations

All examples below use `mongosh` syntax. Equivalent driver examples follow.

### 5.1 Insert

#### insertOne

```javascript
// mongosh
use myapp
db.users.insertOne({
  name: "Alice",
  email: "alice@example.com",
  age: 30,
  createdAt: new Date()
})
// Returns: { acknowledged: true, insertedId: ObjectId("...") }
```

**Go driver:**

```go
coll := client.Database("myapp").Collection("users")
doc := bson.D{
    {"name", "Alice"},
    {"email", "alice@example.com"},
    {"age", 30},
    {"createdAt", time.Now()},
}
result, err := coll.InsertOne(ctx, doc)
fmt.Println(result.InsertedID) // ObjectID
```

**Python:**

```python
db = client["myapp"]
result = db.users.insert_one({
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "createdAt": datetime.utcnow()
})
print(result.inserted_id)
```

#### insertMany

```javascript
// mongosh
db.products.insertMany([
  { name: "Widget", price: 9.99, category: "hardware" },
  { name: "Gadget", price: 24.99, category: "electronics" },
  { name: "Doohickey", price: 4.99, category: "hardware" }
], { ordered: true })
// ordered: true (default) stops on first error
// ordered: false continues inserting remaining docs on error
```

**Go driver:**

```go
docs := []interface{}{
    bson.D{{"name", "Widget"}, {"price", 9.99}, {"category", "hardware"}},
    bson.D{{"name", "Gadget"}, {"price", 24.99}, {"category", "electronics"}},
}
result, err := coll.InsertMany(ctx, docs)
fmt.Println(result.InsertedIDs)
```

### 5.2 Find / Query

#### Basic find

```javascript
// All documents in a collection
db.users.find({})

// Exact match
db.users.find({ name: "Alice" })

// With projection (include only name and email, exclude _id)
db.users.find({ age: { $gt: 25 } }, { name: 1, email: 1, _id: 0 })

// Sort, skip, limit
db.users.find({}).sort({ age: -1 }).skip(10).limit(5)
```

#### Comparison operators

```javascript
db.products.find({ price: { $gt: 10 } })          // greater than
db.products.find({ price: { $gte: 10 } })          // greater than or equal
db.products.find({ price: { $lt: 20 } })           // less than
db.products.find({ price: { $lte: 20 } })          // less than or equal
db.products.find({ price: { $ne: 9.99 } })         // not equal
db.products.find({ category: { $in: ["hardware", "electronics"] } })
db.products.find({ category: { $nin: ["software"] } })
```

#### Logical operators

```javascript
// $and (implicit when multiple fields; explicit form below)
db.products.find({ $and: [{ price: { $gt: 5 } }, { category: "hardware" }] })

// $or
db.products.find({ $or: [{ price: { $lt: 5 } }, { category: "electronics" }] })

// $nor — matches documents that fail all conditions
db.products.find({ $nor: [{ price: { $lt: 5 } }, { category: "software" }] })

// $not — negates a field condition
db.products.find({ price: { $not: { $gt: 20 } } })
```

#### Element operators

```javascript
// $exists: field must be present (true) or absent (false)
db.users.find({ middleName: { $exists: false } })

// $type: match by BSON type name or number
db.users.find({ age: { $type: "int" } })
db.users.find({ age: { $type: ["int", "long", "double"] } })
```

#### Array operators

```javascript
// $all: array field must contain all listed values
db.articles.find({ tags: { $all: ["mongodb", "go"] } })

// $elemMatch: at least one array element matches all conditions
db.orders.find({
  items: { $elemMatch: { qty: { $gt: 5 }, price: { $lt: 10 } } }
})

// $size: array field must have exactly N elements
db.teams.find({ members: { $size: 3 } })
```

#### String operators

```javascript
// $regex — regular expression match
db.users.find({ name: { $regex: "^Ali", $options: "i" } })

// $text — full-text search (requires a text index)
db.articles.find({ $text: { $search: "mongodb performance" } })
```

#### Bitwise operators

```javascript
// $bitsAllSet: all specified bits must be set
db.flags.find({ permissions: { $bitsAllSet: 0b00000110 } })

// $bitsAnySet: at least one bit must be set
db.flags.find({ permissions: { $bitsAnySet: [1, 3] } })  // bit positions 1 and 3

// $bitsAllClear / $bitsAnyClear: analogues for cleared bits
```

#### $mod operator

```javascript
// Matches documents where field % divisor == remainder
db.items.find({ quantity: { $mod: [4, 0] } })  // multiples of 4
```

#### Dot notation (nested documents and arrays)

```javascript
// Nested document
db.users.find({ "address.city": "San Francisco" })

// Array element by index
db.users.find({ "scores.0": { $gt: 90 } })

// Array of embedded documents
db.orders.find({ "items.name": "Widget" })
```

#### $expr (aggregation expressions in queries)

```javascript
// Match documents where discountedPrice < originalPrice
db.products.find({
  $expr: { $lt: ["$discountedPrice", "$originalPrice"] }
})
```

**Go driver find example:**

```go
filter := bson.D{{"age", bson.D{{"$gt", 25}}}}
opts := options.Find().
    SetSort(bson.D{{"age", -1}}).
    SetLimit(10).
    SetProjection(bson.D{{"name", 1}, {"email", 1}})

cursor, err := coll.Find(ctx, filter, opts)
defer cursor.Close(ctx)

var results []bson.M
if err = cursor.All(ctx, &results); err != nil {
    log.Fatal(err)
}
```

### 5.3 Update

#### updateOne

```javascript
// Update first matching document
db.users.updateOne(
  { name: "Alice" },
  { $set: { age: 31, updatedAt: new Date() } }
)
```

#### updateMany

```javascript
// Update all matching documents
db.products.updateMany(
  { category: "hardware" },
  { $inc: { price: 1.0 } }
)
```

#### Upsert (insert if not found)

```javascript
db.settings.updateOne(
  { key: "theme" },
  { $set: { value: "dark" } },
  { upsert: true }
)
```

#### All supported update operators

```javascript
// $set — set field value
db.users.updateOne({ _id: id }, { $set: { name: "Bob" } })

// $unset — remove a field
db.users.updateOne({ _id: id }, { $unset: { middleName: "" } })

// $inc — increment a numeric field
db.stats.updateOne({ _id: id }, { $inc: { views: 1 } })

// $mul — multiply a numeric field
db.prices.updateOne({ _id: id }, { $mul: { price: 1.1 } })

// $min — set field to value only if value is less than current
db.scores.updateOne({ _id: id }, { $min: { lowScore: 50 } })

// $max — set field to value only if value is greater than current
db.scores.updateOne({ _id: id }, { $max: { highScore: 100 } })

// $rename — rename a field
db.users.updateOne({ _id: id }, { $rename: { "oldName": "newName" } })

// $currentDate — set field to current date/timestamp
db.logs.updateOne({ _id: id }, { $currentDate: { lastModified: true } })

// $setOnInsert — only set fields during an upsert insert (not on update)
db.users.updateOne(
  { email: "new@example.com" },
  { $set: { name: "New User" }, $setOnInsert: { createdAt: new Date() } },
  { upsert: true }
)

// Array update operators
// $push — append to array
db.users.updateOne({ _id: id }, { $push: { tags: "golang" } })

// $push with $each and $slice (keep last 5 items)
db.logs.updateOne({ _id: id }, {
  $push: { entries: { $each: [newEntry], $slice: -5 } }
})

// $addToSet — append to array only if value is not already present
db.users.updateOne({ _id: id }, { $addToSet: { roles: "editor" } })

// $pull — remove elements matching a condition
db.users.updateOne({ _id: id }, { $pull: { tags: "deprecated" } })

// $pop — remove first (-1) or last (1) element
db.users.updateOne({ _id: id }, { $pop: { history: 1 } })

// $bit — bitwise update
db.settings.updateOne({ _id: id }, { $bit: { flags: { or: 0b00000100 } } })
```

### 5.4 Delete

```javascript
// Delete first matching document
db.users.deleteOne({ name: "Alice" })

// Delete all matching documents
db.users.deleteMany({ age: { $lt: 18 } })

// Delete all documents in a collection (keep the collection)
db.users.deleteMany({})
```

**Go driver:**

```go
result, err := coll.DeleteMany(ctx, bson.D{{"age", bson.D{{"$lt", 18}}}})
fmt.Printf("Deleted %d documents\n", result.DeletedCount)
```

### 5.5 findAndModify / findOneAndUpdate

These operations atomically find and modify a document, returning either the original or updated document.

```javascript
// Return the document BEFORE update (default)
db.counters.findOneAndUpdate(
  { _id: "pageViews" },
  { $inc: { count: 1 } }
)

// Return the document AFTER update
db.counters.findOneAndUpdate(
  { _id: "pageViews" },
  { $inc: { count: 1 } },
  { returnNewDocument: true }
)

// With sort (find the highest-priority task and mark it in-progress)
db.tasks.findOneAndUpdate(
  { status: "pending" },
  { $set: { status: "in-progress", startedAt: new Date() } },
  { sort: { priority: -1 }, returnNewDocument: true }
)

// findOneAndDelete — atomically find and remove
db.queue.findOneAndDelete(
  { status: "ready" },
  { sort: { createdAt: 1 } }
)

// findOneAndReplace — atomically find and replace
db.users.findOneAndReplace(
  { email: "old@example.com" },
  { email: "new@example.com", name: "Updated" },
  { upsert: true, returnNewDocument: true }
)
```

**Go driver:**

```go
// findOneAndUpdate
after := options.After
opts := options.FindOneAndUpdate().SetReturnDocument(after).SetSort(bson.D{{"priority", -1}})
var result bson.M
err := coll.FindOneAndUpdate(
    ctx,
    bson.D{{"status", "pending"}},
    bson.D{{"$set", bson.D{{"status", "in-progress"}}}},
    opts,
).Decode(&result)
```

---

## 6. Indexes

### 6.1 Creating Indexes

```javascript
// Single field index (ascending)
db.users.createIndex({ email: 1 })

// Single field index (descending)
db.users.createIndex({ createdAt: -1 })

// Compound index
db.orders.createIndex({ userId: 1, status: 1 })

// Unique index
db.users.createIndex({ email: 1 }, { unique: true })

// Named index
db.products.createIndex({ name: 1 }, { name: "products_name_asc" })

// Background creation (non-blocking — always the case in Salvobase)
db.logs.createIndex({ timestamp: -1 }, { background: true })
```

### 6.2 Index Types

#### Sparse Index

A sparse index only includes documents that contain the indexed field. Useful when the field is optional.

```javascript
db.users.createIndex({ phoneNumber: 1 }, { sparse: true })
```

#### Unique Index

Rejects inserts/updates that would create a duplicate value for the indexed field.

```javascript
db.users.createIndex({ username: 1 }, { unique: true })
```

#### TTL Index

Automatically deletes documents after a specified number of seconds. Salvobase runs TTL cleanup every second (vs. MongoDB's 60-second cycle), giving near-millisecond precision.

```javascript
// Delete sessions 24 hours after their createdAt date
db.sessions.createIndex(
  { createdAt: 1 },
  { expireAfterSeconds: 86400 }
)

// Delete log entries 7 days after their timestamp
db.logs.createIndex(
  { timestamp: 1 },
  { expireAfterSeconds: 604800 }
)
```

#### Text Index

Enables `$text` search queries against string fields.

```javascript
// Single field text index
db.articles.createIndex({ body: "text" })

// Multi-field text index with weights
db.articles.createIndex(
  { title: "text", body: "text", tags: "text" },
  { weights: { title: 10, body: 1, tags: 5 } }
)
```

#### Compound Index

Multi-field index. Field order matters for query planning. For queries filtering on both `userId` and sorting by `createdAt`, this compound index is optimal:

```javascript
db.events.createIndex({ userId: 1, createdAt: -1 })
```

### 6.3 Dropping and Listing Indexes

```javascript
// List all indexes on a collection
db.users.getIndexes()

// Drop a specific index by name
db.users.dropIndex("email_1")

// Drop all non-_id indexes
db.users.dropIndexes()
```

### 6.4 Index Hints

Force the query planner to use a specific index:

```javascript
db.orders.find({ userId: "u123", status: "shipped" })
  .hint({ userId: 1, status: 1 })

// Force a collection scan (no index)
db.orders.find({ status: "shipped" }).hint({ $natural: 1 })
```

---

## 7. Aggregation Pipeline

The aggregation framework processes documents through a sequence of stages. Each stage transforms the documents passing through it.

```javascript
db.collection.aggregate([stage1, stage2, ...])
```

### Supported Pipeline Stages

#### $match

Filters documents — identical syntax to `find()` queries.

```javascript
db.orders.aggregate([
  { $match: { status: "completed", total: { $gt: 100 } } }
])
```

#### $project

Reshapes documents: include/exclude fields, add computed fields.

```javascript
db.orders.aggregate([
  {
    $project: {
      _id: 0,
      orderId: "$_id",
      total: 1,
      tax: { $multiply: ["$total", 0.1] },
      upperStatus: { $toUpper: "$status" }
    }
  }
])
```

#### $group

Groups documents by a key and applies accumulator expressions.

```javascript
db.orders.aggregate([
  {
    $group: {
      _id: "$customerId",
      totalSpent: { $sum: "$total" },
      orderCount: { $sum: 1 },
      avgOrder: { $avg: "$total" },
      firstOrder: { $min: "$createdAt" },
      lastOrder: { $max: "$createdAt" },
      statuses: { $addToSet: "$status" }
    }
  }
])
```

#### $sort

Sorts documents by one or more fields. `1` = ascending, `-1` = descending.

```javascript
db.products.aggregate([
  { $sort: { price: -1, name: 1 } }
])
```

#### $limit and $skip

```javascript
db.products.aggregate([
  { $sort: { price: -1 } },
  { $skip: 20 },
  { $limit: 10 }
])
```

#### $unwind

Deconstructs an array field, outputting one document per array element.

```javascript
db.orders.aggregate([
  { $unwind: "$items" },
  { $group: { _id: "$items.productId", totalQty: { $sum: "$items.qty" } } }
])

// With options: preserve nulls and empty arrays, include array index
db.orders.aggregate([
  {
    $unwind: {
      path: "$items",
      includeArrayIndex: "itemIndex",
      preserveNullAndEmptyArrays: true
    }
  }
])
```

#### $lookup

Left outer join with another collection.

```javascript
// Basic lookup
db.orders.aggregate([
  {
    $lookup: {
      from: "users",
      localField: "userId",
      foreignField: "_id",
      as: "user"
    }
  }
])

// Lookup with pipeline (for complex join conditions)
db.orders.aggregate([
  {
    $lookup: {
      from: "inventory",
      let: { itemName: "$name" },
      pipeline: [
        { $match: { $expr: { $eq: ["$item", "$$itemName"] } } },
        { $project: { stock: 1 } }
      ],
      as: "stockInfo"
    }
  }
])
```

#### $addFields

Adds new fields (or overwrites existing ones) to documents.

```javascript
db.products.aggregate([
  {
    $addFields: {
      priceWithTax: { $multiply: ["$price", 1.1] },
      inStock: { $gt: ["$quantity", 0] }
    }
  }
])
```

#### $replaceRoot

Replaces the document root with another document or expression.

```javascript
db.orders.aggregate([
  { $unwind: "$items" },
  { $replaceRoot: { newRoot: "$items" } }
])
```

#### $count

Returns the document count as a single document.

```javascript
db.orders.aggregate([
  { $match: { status: "completed" } },
  { $count: "completedOrders" }
])
// Returns: [{ completedOrders: 42 }]
```

#### $facet

Runs multiple sub-pipelines on the same input documents and returns results in a single document.

```javascript
db.products.aggregate([
  {
    $facet: {
      byCategory: [
        { $group: { _id: "$category", count: { $sum: 1 } } }
      ],
      priceStats: [
        { $group: { _id: null, avg: { $avg: "$price" }, min: { $min: "$price" }, max: { $max: "$price" } } }
      ],
      topProducts: [
        { $sort: { sales: -1 } },
        { $limit: 5 },
        { $project: { name: 1, sales: 1 } }
      ]
    }
  }
])
```

#### $out and $merge

Write pipeline results to a collection.

```javascript
// $out — replaces the collection entirely
db.orders.aggregate([
  { $match: { year: 2024 } },
  { $out: "orders_2024" }
])

// $merge — merge results into an existing collection
db.dailyStats.aggregate([
  { $group: { _id: "$date", total: { $sum: "$amount" } } },
  { $merge: { into: "monthlySummary", on: "_id", whenMatched: "merge", whenNotMatched: "insert" } }
])
```

#### $bucket and $bucketAuto

Categorizes documents into buckets.

```javascript
// $bucket — explicit boundaries
db.products.aggregate([
  {
    $bucket: {
      groupBy: "$price",
      boundaries: [0, 10, 50, 100, 1000],
      default: "Other",
      output: { count: { $sum: 1 }, avgPrice: { $avg: "$price" } }
    }
  }
])

// $bucketAuto — automatic N equally distributed buckets
db.products.aggregate([
  { $bucketAuto: { groupBy: "$price", buckets: 5 } }
])
```

#### $sortByCount

Shorthand for `$group` by a field and `$sort` by count descending.

```javascript
db.orders.aggregate([
  { $sortByCount: "$status" }
])
// Equivalent to:
// { $group: { _id: "$status", count: { $sum: 1 } } }
// { $sort: { count: -1 } }
```

### allowDiskUse

Aggregations that exceed `maxAggregationMemory` (default 100MB) require `allowDiskUse`:

```javascript
db.bigCollection.aggregate(
  [ /* pipeline */ ],
  { allowDiskUse: true }
)
```

---

## 8. Multi-Tenancy

Salvobase provides database-level isolation out of the box — each tenant gets their own database, and each database is its own `.db` file on disk with no data interleaving.

### Per-Tenant Rate Limiting

Set a per-database requests-per-second limit with `--rateLimit`:

```bash
./bin/salvobase --rateLimit 500
```

This applies to all databases. When a database exceeds the limit, requests receive a `429 Too Many Requests` style error.

### Connection-Level DB Isolation

The `x-salvobase-tenant-db` connection header locks a connection to a specific database. This prevents a tenant from issuing commands against another tenant's database even if they accidentally omit the `use db` switch. Pass it in your driver's connection options as an application metadata field.

### Tenant Provisioning Pattern

```javascript
// Create a new tenant database and user
use tenant_acme
db.createUser({
  user: "acme_app",
  pwd: "apppassword",
  roles: [{ role: "readWrite", db: "tenant_acme" }]
})

// Set up any required collections/indexes
db.createCollection("events")
db.events.createIndex({ timestamp: -1 })
db.events.createIndex({ userId: 1, timestamp: -1 })
```

---

## 9. User Management and Authorization

### Built-in Roles

| Role | Description |
|------|-------------|
| `read` | Read-only access to all non-system collections in a database |
| `readWrite` | Read and write access to all non-system collections |
| `dbAdmin` | Administrative actions on a database (stats, indexes, etc.) |
| `userAdmin` | Manage users and roles in a database |
| `dbOwner` | All of the above — effectively owns a database |
| `root` | Superuser role on the `admin` database — full access to everything |

### createUser

```javascript
use mydb
db.createUser({
  user: "appuser",
  pwd: "apppassword",
  roles: [
    { role: "readWrite", db: "mydb" },
    { role: "read", db: "analytics" }
  ],
  customData: { team: "backend", createdBy: "admin" }
})
```

### updateUser

```javascript
use mydb
db.updateUser("appuser", {
  pwd: "newpassword",
  roles: [{ role: "readWrite", db: "mydb" }]
})
```

### dropUser

```javascript
use mydb
db.dropUser("appuser")
```

### usersInfo

```javascript
// Get info about a specific user
db.runCommand({ usersInfo: "appuser" })

// Get all users for the current database
db.runCommand({ usersInfo: 1 })

// Get users across all databases (admin only)
db.runCommand({ usersInfo: { forAllDBs: true } })
```

### connectionStatus

Shows the currently authenticated user and their roles:

```javascript
db.runCommand({ connectionStatus: 1 })
```

---

## 10. Monitoring

### 10.1 Prometheus Metrics

Salvobase exposes Prometheus metrics at `http://<host>:<httpPort>/metrics`. No separate exporter needed.

```bash
curl http://localhost:27080/metrics
```

Key metrics exposed:

| Metric | Type | Description |
|--------|------|-------------|
| `salvobase_connections_current` | Gauge | Current open connections |
| `salvobase_connections_total` | Counter | Total connections ever created |
| `salvobase_ops_total` | Counter | Operations by type (insert/query/update/delete/getmore/command) |
| `salvobase_op_duration_seconds` | Histogram | Operation latency by command |
| `salvobase_documents_total` | Gauge | Document count per collection |
| `salvobase_storage_bytes` | Gauge | Storage size per database |
| `salvobase_cursor_count` | Gauge | Open server-side cursors |
| `salvobase_auth_failures_total` | Counter | Authentication failure count |

**Prometheus scrape config:**

```yaml
scrape_configs:
  - job_name: 'salvobase'
    static_configs:
      - targets: ['localhost:27080']
    metrics_path: /metrics
```

**Grafana**: Import the provided Grafana dashboard (JSON available in `configs/grafana-dashboard.json`) to visualize connections, operation rates, and latency distributions.

### 10.2 HTTP Health Check

```bash
# Liveness check
curl http://localhost:27080/healthz
# {"status":"ok","uptime":3600}

# Readiness check (returns 503 if not accepting connections)
curl http://localhost:27080/readyz
```

The health endpoints return HTTP 200 when healthy and HTTP 503 when the server is starting up or shutting down.

### serverStatus Command

The MongoDB `serverStatus` command is also supported via the wire protocol:

```javascript
db.adminCommand({ serverStatus: 1 })
```

Returns uptime, connection stats, operation counters, and memory usage.

---

## 11. REST API

Salvobase exposes an optional HTTP/JSON REST API alongside the wire protocol on the same `httpPort`. This is useful for quick testing, shell scripts, and environments where a MongoDB driver is inconvenient.

**Base URL:** `http://<host>:<httpPort>/api/v1/`

All request and response bodies are JSON. All responses include `{"ok": 1}` on success or `{"ok": 0, "errmsg": "..."}` on error.

### Insert a Document

```bash
curl -X POST http://localhost:27080/api/v1/mydb/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"name": "Alice", "email": "alice@example.com", "age": 30}'
```

### Query Documents

```bash
# All documents
curl "http://localhost:27080/api/v1/mydb/users"

# With filter (URL-encoded JSON)
curl "http://localhost:27080/api/v1/mydb/users?filter=%7B%22age%22%3A%7B%22%24gt%22%3A25%7D%7D"

# With limit and sort
curl "http://localhost:27080/api/v1/mydb/users?limit=10&sort=%7B%22age%22%3A-1%7D"
```

### Update a Document

```bash
curl -X PATCH "http://localhost:27080/api/v1/mydb/users/<id>" \
  -H "Content-Type: application/json" \
  -d '{"$set": {"age": 31}}'
```

### Delete a Document

```bash
curl -X DELETE "http://localhost:27080/api/v1/mydb/users/<id>"
```

### Run a Command

```bash
curl -X POST http://localhost:27080/api/v1/mydb/_cmd \
  -H "Content-Type: application/json" \
  -d '{"ping": 1}'
```

---

## 12. Production Deployment

### 12.1 systemd Service

Create `/etc/systemd/system/salvobase.service`:

```ini
[Unit]
Description=Salvobase document database
After=network.target
Documentation=https://github.com/inder/salvobase

[Service]
Type=simple
User=salvobase
Group=salvobase
ExecStart=/usr/local/bin/salvobase \
    --port 27017 \
    --httpPort 27080 \
    --bind_ip 127.0.0.1 \
    --datadir /var/lib/salvobase \
    --logLevel info \
    --logFormat json \
    --auditLog /var/log/salvobase/audit.log
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal
LimitNOFILE=64000
LimitNPROC=32000

# Security hardening
PrivateTmp=true
NoNewPrivileges=true
ProtectSystem=full
ReadWritePaths=/var/lib/salvobase /var/log/salvobase

[Install]
WantedBy=multi-user.target
```

```bash
# Create user and directories
useradd --system --no-create-home --shell /bin/false salvobase
mkdir -p /var/lib/salvobase /var/log/salvobase
chown salvobase:salvobase /var/lib/salvobase /var/log/salvobase

# Install binary
install -o root -g root -m 0755 ./bin/salvobase /usr/local/bin/salvobase

# Enable and start
systemctl daemon-reload
systemctl enable salvobase
systemctl start salvobase
systemctl status salvobase
```

### 12.2 Resource Limits

| Resource | Recommendation |
|----------|---------------|
| `LimitNOFILE` | At least `maxConnections * 2` + overhead. Set to `64000` for 1000 connections. |
| `LimitNPROC` | `32000` is sufficient |
| RAM | Allow at least 256MB base + ~50KB per open cursor + bbolt page cache |
| CPU | Single-writer storage means CPU is rarely the bottleneck; 2 vCPUs is usually fine |

### 12.3 Data Directory

Salvobase creates one `.db` file per database in `dataDir`:

```
/var/lib/salvobase/
  admin.db          # admin database (users, auth)
  myapp.db          # your application database
  tenant_acme.db    # tenant database
  ...
```

Permissions: the process user needs read+write on the directory. Do not place the data directory on a network filesystem (NFS, etc.) — bbolt requires local file locks.

### 12.4 Backup Strategy

Salvobase databases are bbolt files. To take a consistent backup:

```bash
# 1. The simplest approach — copy the .db files
#    bbolt supports concurrent reads while the server is running.
#    The copy is consistent as long as you copy each file atomically (cp does this).

cp -a /var/lib/salvobase/ /backup/salvobase-$(date +%Y%m%d-%H%M%S)/

# 2. For point-in-time backups, stop writes first (optional but ensures full consistency):
kill -SIGTERM $(pidof salvobase)   # graceful shutdown flushes all pages
cp -a /var/lib/salvobase/ /backup/salvobase-$(date +%Y%m%d-%H%M%S)/
systemctl start salvobase
```

> bbolt uses copy-on-write transactions, so a file copy taken during normal operation captures the last fully committed transaction. This is safe for read operations; for a fully consistent multi-database backup, a brief shutdown is recommended.

**Restore:**

```bash
systemctl stop salvobase
cp -a /backup/salvobase-20240101-120000/ /var/lib/salvobase/
systemctl start salvobase
```

### 12.5 TLS

Generate a self-signed certificate for testing:

```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

Start Salvobase with TLS:

```bash
./bin/salvobase \
  --port 27017 \
  --tls \
  --tlsCert /etc/salvobase/tls/server.crt \
  --tlsKey /etc/salvobase/tls/server.key
```

Connect with TLS:

```bash
# mongosh (allow self-signed in dev)
mongosh "mongodb://admin:password@localhost:27017/admin?tls=true&tlsAllowInvalidCertificates=true"

# Go driver
clientOpts := options.Client().ApplyURI(
    "mongodb://localhost:27017/?tls=true",
).SetTLSConfig(&tls.Config{InsecureSkipVerify: true})  // for dev only
```

In production, use a certificate from a trusted CA and remove `InsecureSkipVerify`.

---

## 13. Compatibility Notes

### Supported

- All MongoDB CRUD commands: `find`, `insert`, `update`, `delete`, `findAndModify`, `getMore`, `killCursors`, `count`, `distinct`, `aggregate`
- Index management: `createIndexes`, `dropIndexes`, `listIndexes`
- User management: `createUser`, `updateUser`, `dropUser`, `usersInfo`
- Admin: `createCollection`, `drop`, `dropDatabase`, `listDatabases`, `listCollections`, `renameCollection`
- Diagnostics: `ping`, `buildInfo`, `serverStatus`, `dbStats`, `collStats`, `indexStats`, `explain`
- Session commands: `startSession`, `endSessions` (sessions are tracked but transactions are stub-only)
- `hello`, `isMaster` — both return compatible responses
- OP_MSG (current) and OP_QUERY/OP_REPLY (legacy) wire protocol messages
- SCRAM-SHA-256 authentication
- All MongoDB drivers for Go, Python, Node.js, Java, Ruby, C#, PHP, etc.
- `mongosh` interactive shell

### Known Limitations

| Feature | Status |
|---------|--------|
| Replication / replica sets | Not implemented |
| Sharding | Not implemented |
| Change streams | Not implemented |
| Multi-document transactions | Stub only — `commitTransaction`/`abortTransaction` accepted but single-document atomicity only |
| `$where` (JavaScript evaluation) | Disabled for security (returns an error) |
| `$jsonSchema` validation | Not implemented |
| Geospatial operators (`$near`, `$geoWithin`, etc.) | Not implemented |
| Capped collections | Parsed and stored but overflow eviction not enforced |
| GridFS | Not natively implemented (use the driver's GridFS layer over standard collections) |
| Atlas Search / vector search | Not implemented |
| OP_COMPRESSED | Parsed as unknown opcode (body discarded gracefully) |

### Wire Protocol Version

Salvobase advertises `maxWireVersion: 21` (MongoDB 7.0). Any driver that supports MongoDB 3.6 or later will work correctly.

---

## 14. Improvements Over MongoDB Community

Salvobase ships with nine capabilities that require MongoDB Enterprise or third-party add-ons with MongoDB Community:

### 1. Native Prometheus Metrics

No exporter, no sidecar. `GET /metrics` is live the moment the server starts.

```bash
curl http://localhost:27080/metrics | grep salvobase_ops
# salvobase_ops_total{type="insert"} 1234
# salvobase_ops_total{type="query"} 5678
```

Plug straight into any Prometheus + Grafana stack. Histograms for per-command latency are included.

### 2. HTTP/REST API

Full JSON REST API alongside the wire protocol — no driver required for simple operations. Useful for shell scripts, Lambda functions, and debugging.

```bash
curl http://localhost:27080/api/v1/mydb/users?limit=5
```

### 3. Per-Tenant Rate Limiting

Built-in request-rate throttling per database. No API gateway or proxy needed. Configure with `--rateLimit 500` for 500 req/s per database.

### 4. Built-in Audit Logging

Set `--auditLog /var/log/salvobase/audit.log` and every authentication event, user management operation, and DDL command is written as a structured JSON log line. MongoDB audit logging requires Enterprise.

Example audit log entry:

```json
{
  "ts": "2024-01-15T10:30:00Z",
  "event": "createUser",
  "db": "myapp",
  "user": "admin",
  "remote": "127.0.0.1:54321",
  "result": "success"
}
```

### 5. Transparent Document Compression

Documents are compressed on write and decompressed on read using Snappy (default) or Zstd. Expect 40-70% storage reduction on typical JSON-heavy workloads. MongoDB Community stores documents uncompressed (WiredTiger uses block-level compression, which is different and less aggressive for small documents).

Configure with `--compression snappy|zstd|none`.

### 6. Better explain Output

`explain()` includes cost estimates and per-stage timing. MongoDB Community's explain is notoriously difficult to interpret; Salvobase annotates each stage with:

- `docsExamined` — documents scanned
- `keysExamined` — index keys examined
- `executionTimeMillis` — actual time for this stage
- `costEstimate` — relative cost estimate

```javascript
db.orders.find({ userId: "u123" }).explain("executionStats")
```

### 7. Millisecond TTL Precision

The TTL cleanup goroutine runs every second, not every 60 seconds. If you set `expireAfterSeconds: 0` on a date field, documents are removed within approximately 1 second of expiry. This is critical for session invalidation and short-lived token use cases.

### 8. Connection-Level DB Isolation

Pass `x-salvobase-tenant-db: <dbname>` as an application metadata header in your driver's connection string and the connection is locked to that database. Prevents accidental cross-tenant data access without additional proxy infrastructure.

### 9. Zero-Downtime Config Reload

```bash
kill -HUP $(pidof salvobase)
```

Reloads configuration (log levels, rate limits, etc.) without restarting or dropping connections. MongoDB requires a restart to change most server parameters.

---

## 15. Troubleshooting

### Connection Refused

```
Error: connect ECONNREFUSED 127.0.0.1:27017
```

- Is the server running? `systemctl status salvobase` or `ps aux | grep salvobase`
- Is it bound to the right interface? Check `--bind_ip`. If set to `127.0.0.1`, you can only connect from localhost.
- Is the port correct? Default is 27017.

### Authentication Failed

```
MongoServerError: Authentication failed.
```

- Verify you're connecting to the right `authSource` (usually `admin`)
- Check the username and password — they are case-sensitive
- Make sure the user was created before connecting: `./bin/salvobase admin create-user <user> <pass>`
- If you started with `--noauth` and then enabled auth, users created during noauth mode may not have SCRAM credentials

### `$where is not supported`

Salvobase intentionally rejects `$where` for security. Replace JavaScript predicates with the appropriate query operators:

```javascript
// Instead of: db.users.find({ $where: "this.age > 25" })
db.users.find({ age: { $gt: 25 } })
```

### `$jsonSchema is not implemented`

Schema validation via `$jsonSchema` is not yet supported. Remove the `validator` option from `createCollection` calls, or use application-level validation.

### Cursor Not Found

```
MongoServerError: Cursor id <n> not found
```

- Cursors are server-side and time out after 30 minutes of inactivity (the `LogicalSessionTimeoutMinutes` value)
- Make sure to iterate cursors promptly and close them when done
- If you're getting this on every request, check that your connection pool isn't being closed between requests

### High Memory Usage

- Check `db.currentOp()` for long-running cursors or aggregations
- Large aggregations use in-memory sorting — pass `allowDiskUse: true` if needed
- The `maxAggregationMemory` config (default 100MB) limits per-pipeline memory

### Data File Won't Open

```
failed to create server: failed to open database: timeout
```

- Another process (possibly a crashed instance) holds a lock on the `.db` file
- bbolt uses OS-level file locks. Check: `lsof /var/lib/salvobase/*.db`
- Kill the stale process or remove the lock if the process is dead

### TLS Handshake Failed

```
x509: certificate signed by unknown authority
```

In development, add `tlsAllowInvalidCertificates=true` to your connection URI. In production, use a certificate signed by a trusted CA or add the CA to your system's trust store.

### Log Analysis

Salvobase logs are structured JSON (default) or human-readable console format. Pipe to `jq` for easy parsing:

```bash
journalctl -u salvobase -f | jq 'select(.level == "error")'
```

Common log fields: `level`, `ts`, `msg`, `connID`, `db`, `cmd`, `durationMs`, `error`.
