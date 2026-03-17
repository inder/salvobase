# Design: Change Streams

**Status:** Accepted
**Issue:** #126
**Implementation sub-issues:** #113 (event bus), #114 (tailable cursor), #115 (resume tokens), #116 (aggregate $changeStream stage)

---

## Overview

Change streams let a client subscribe to a real-time feed of insert/update/delete events on a
collection without polling. The client opens a change stream cursor via `aggregate` +
`$changeStream`, then calls `getMore` in a loop. Each `getMore` blocks until at least one event
arrives (or a timeout elapses), then returns a batch of change event documents.

This document is scoped to **Phase 1**: collection-level watch, insert/update/delete events only,
no pre/post-image lookup, no cluster-level or database-level watch.

---

## 1. Event Bus

### Where it lives

The event bus lives inside `BBoltEngine` (`internal/storage/engine.go`), as a new field:

```go
type BBoltEngine struct {
    // ... existing fields ...
    eventBus *EventBus
}
```

`EventBus` is a standalone type in a new file `internal/storage/event_bus.go`. It is
engine-scoped (one instance per server) and manages per-namespace ring buffers.

### Structure

```go
// ChangeEventType is the type of a mutation event.
type ChangeEventType string

const (
    ChangeInsert  ChangeEventType = "insert"
    ChangeUpdate  ChangeEventType = "update"
    ChangeDelete  ChangeEventType = "delete"
    ChangeReplace ChangeEventType = "replace"
)

// ChangeEvent is a single mutation event published to the bus.
type ChangeEvent struct {
    // ResumeToken uniquely identifies this event within a namespace.
    ResumeToken ResumeToken

    // Namespace is "db.collection".
    Namespace string

    // OperationType is the kind of mutation.
    OperationType ChangeEventType

    // DocumentKey holds the _id of the affected document.
    DocumentKey bson.Raw

    // FullDocument is only populated for insert and replace; nil for update/delete in Phase 1.
    FullDocument bson.Raw

    // UpdateDescription is populated for update events (set/unset fields).
    // Nil in Phase 1 — use a placeholder: {updatedFields: {}, removedFields: []}.
    UpdateDescription bson.Raw
}

// EventBus manages per-namespace ring buffers and fan-out to subscribers.
type EventBus struct {
    mu      sync.RWMutex
    streams map[string]*nsStream // keyed by "db.coll"
}

// nsStream holds the ring buffer and subscriber list for one namespace.
type nsStream struct {
    mu          sync.Mutex
    cond        *sync.Cond
    buf         []ChangeEvent // ring buffer, capacity = EventBusBufferSize
    head        int           // next write position (mod cap)
    seq         atomic.Int64  // global monotone sequence counter
    closed      bool
}
```

**Default ring buffer capacity:** 1 000 events per namespace (configurable via server config
field `ChangeStreamBufferSize int`, defaulting to 1000). Each event is small (< 512 bytes
typically); 1 000 events ≈ 500 KB max per watched namespace.

### How mutations publish to the bus

After every successful bbolt write transaction, the mutation methods call `engine.eventBus.Publish`:

```
InsertMany   → publishes one ChangeInsert event per inserted document
UpdateOne    → publishes one ChangeUpdate event
UpdateMany   → publishes one ChangeUpdate event per modified document
ReplaceOne   → publishes one ChangeReplace event
DeleteOne    → publishes one ChangeDelete event
DeleteMany   → publishes one ChangeDelete event per deleted document
```

The call happens **after** `tx.Commit()` returns nil, so partial writes (due to `ordered:false`
errors) only produce events for committed documents.

Example in `InsertMany` (simplified):

```go
// after bbolt tx.Commit()
for _, p := range committed {
    engine.eventBus.Publish(db, coll, ChangeEvent{
        OperationType: ChangeInsert,
        DocumentKey:   bsonDoc{"_id": p.id},
        FullDocument:  p.finalDoc,
    })
}
```

### Backpressure handling

`Publish` never blocks. When the ring buffer is full (i.e., a subscriber is so slow that the
write head has lapped the ring), the **oldest event is silently overwritten**. Any subscriber
whose read position has been lapped receives an `ErrBufOverflow` sentinel on the next `Recv`
call. The server then closes that subscriber's change stream cursor with a "change stream
history lost" error (code `286` — `ChangeStreamHistoryLost`), matching MongoDB behaviour.

Fan-out is achieved by waking all waiters via `nsStream.cond.Broadcast()` after every `Publish`.
Subscribers each track their own read position (a sequence number); they re-read from the ring
without copying — the `mu` lock serialises ring reads.

---

## 2. Tailable Cursor

### Wire protocol changes

Change streams are opened with a normal `aggregate` command whose first pipeline stage is
`$changeStream`:

```json
{ "aggregate": "orders", "$db": "mydb",
  "pipeline": [{"$changeStream": {}}],
  "cursor": {"batchSize": 0} }
```

The server recognises `$changeStream` as the first stage and returns a **tailable cursor**
instead of a regular one. The `firstBatch` in the response is always empty (`[]`) when no
events are ready at open time.

The client then calls `getMore` in a loop, supplying the optional `maxAwaitTimeMS` field:

```json
{ "getMore": <cursorID>, "collection": "orders", "maxAwaitTimeMS": 1000 }
```

**OP_MSG flag bits:** Phase 1 does not use the `moreToCome` (bit 1) flag. Each `getMore` is a
normal request/response pair. The `awaitData` semantic is implemented at the command-handler
level by blocking the goroutine (see below).

No changes to `wire/op_msg.go` or any other wire file are required for Phase 1. The existing
OP_MSG read/write path is used as-is.

### How the server holds the connection open

The `getMore` command handler (`internal/commands/get_more.go`) distinguishes tailable cursors
from regular ones by checking whether the registered `Cursor` implements a new interface:

```go
// TailableCursor extends Cursor for change streams.
// Implemented by changeStreamCursor in internal/storage/change_stream_cursor.go.
type TailableCursor interface {
    Cursor
    // NextBatchWait blocks until at least one event is available or the deadline
    // passes, then returns the next batch.
    NextBatchWait(ctx context.Context, batchSize int, maxWaitMS int64) ([]bson.Raw, int64, error)
}
```

When `getMore` receives a `TailableCursor`, it calls `NextBatchWait` instead of `NextBatch`.
`NextBatchWait` internally waits on `nsStream.cond` with a timeout derived from `maxAwaitTimeMS`
(default: 1 000 ms; max: 60 000 ms). The goroutine is parked for up to `maxAwaitTimeMS` rather
than returning immediately.

**Connection lifecycle:** The standard `Connection.serve()` loop in `server/connection.go` is
unchanged. Each `getMore` call is one full request/response cycle; the connection goroutine
blocks inside `getMore`'s handler during the wait. This matches MongoDB driver behaviour: the
driver holds the socket busy during `getMore` and expects a response within the configured
`socketTimeoutMS`.

### changeStreamCursor

`internal/storage/change_stream_cursor.go` implements `TailableCursor`:

```go
type changeStreamCursor struct {
    id        int64
    mu        sync.Mutex
    ns        string         // "db.coll"
    bus       *EventBus
    readSeq   int64          // last sequence number consumed
    pipeline  []bson.Raw     // post-$changeStream stages for client-side filtering
    closed    bool
}

func (c *changeStreamCursor) NextBatchWait(
    ctx context.Context, batchSize int, maxWaitMS int64,
) ([]bson.Raw, int64, error)
```

Inside `NextBatchWait`:
1. Lock `nsStream.mu`.
2. If events are available since `readSeq`, drain up to `batchSize` and return immediately.
3. Otherwise, call `nsStream.cond.Wait()` with a deadline goroutine that fires after `maxWaitMS`.
4. On wake, drain available events.
5. Return the batch as BSON-encoded change event documents (see §Change Event Document Shape).

---

## 3. Resume Tokens

### Format

A resume token is an opaque string carried in the `_id` field of every change event document
and also returned in the cursor's `postBatchResumeToken` field. Clients pass it back as
`resumeAfter` or `startAfter` in the `$changeStream` options.

**Encoding:** base64url (no padding) of a 16-byte big-endian binary:

```
bytes  0–7   : Unix timestamp in nanoseconds (int64, big-endian)
bytes  8–15  : global sequence number (int64, big-endian)
```

The timestamp comes from `time.Now().UnixNano()` at event publication time.
The sequence number is `nsStream.seq.Add(1)` — monotonically increasing per namespace.

Big-endian encoding ensures lexicographic order matches chronological order, enabling efficient
range queries in future index-based implementations.

BSON representation in the change event `_id` field:

```go
bson.D{{"_data", base64urlToken}}
```

This matches the shape MongoDB drivers expect. Drivers treat `_data` as opaque.

### How reconnect works without missing events

When the client reconnects and creates a new change stream with `resumeAfter: {_data: "<token>"}`:

1. Decode the token to extract `(ns, seq)`.
2. Scan the `nsStream` ring buffer for events with `seq > resumeSeq`.
3. If found, start streaming from that point — **no events missed**.
4. If not found (token is older than the ring buffer's oldest event), return error code `286`
   (`ChangeStreamHistoryLost`): the client must re-snapshot and re-open.

`startAtOperationTime` (a BSON Timestamp) is also supported: decode the timestamp, find the
first event whose token timestamp ≥ that value.

**Important invariant:** The ring buffer retains the last `ChangeStreamBufferSize` events per
namespace regardless of whether any clients are watching. This means a client that disconnects
and reconnects within the buffer window loses nothing, as long as the write rate is below
`ChangeStreamBufferSize` events between disconnect and reconnect.

---

## 4. Memory Model

| Parameter | Default | Notes |
|---|---|---|
| Ring buffer size | 1 000 events/namespace | Configurable: `changeStreamBufferSize` in server config |
| Max event document size | Same as `MaxBSONObjectSize` (16 MB) | In practice change events are < 1 KB |
| Max concurrent watchers | Unlimited (one goroutine per `getMore` call) | Governed by `MaxConnections` server config |
| `maxAwaitTimeMS` range | 1 ms – 60 000 ms | Default: 1 000 ms |
| Idle cursor timeout | 30 min (same as regular cursor) | Enforced by existing `cursorStore.Cleanup` |

**What happens when a client is slow:**

1. The ring buffer overwrites old events. The slow subscriber's `readSeq` pointer is now
   behind the oldest event in the ring.
2. On the next `NextBatchWait` call, the cursor returns `ErrBufOverflow`.
3. The `getMore` handler returns `{ ok: 0, code: 286, errmsg: "change stream history lost" }`.
4. The cursor is automatically deleted from `cursorStore`.
5. The driver receives the error and surfaces it to the application. The application must
   decide whether to re-snapshot or simply re-open with `startAtOperationTime: now`.

**Memory footprint estimate:**
A 1 000-event ring buffer where each event averages 256 bytes ≈ **256 KB per watched namespace**.
With 100 concurrently watched namespaces ≈ **25 MB total** — well within the default Go heap.

**No disk persistence:** Phase 1 stores events only in memory. The ring buffer is lost on
server restart; clients that reconnect after a restart receive `ChangeStreamHistoryLost` and
must re-open.

---

## 5. Scope Boundary

### Phase 1 (this design, issues #113–#116)

| Feature | Included |
|---|---|
| Collection-level watch | Yes |
| `insert` events | Yes |
| `update` events | Yes (no `updateDescription` detail — placeholder `{updatedFields:{}, removedFields:[]}`) |
| `delete` events | Yes |
| `replace` events | Yes |
| `$changeStream` aggregate stage | Yes |
| Resume via `resumeAfter` token | Yes |
| Resume via `startAtOperationTime` | Yes |
| `maxAwaitTimeMS` in `getMore` | Yes |
| `postBatchResumeToken` in cursor response | Yes |
| In-memory ring buffer backpressure | Yes |
| `ChangeStreamHistoryLost` error (code 286) | Yes |

### Deferred (not Phase 1)

| Feature | Rationale |
|---|---|
| `fullDocument: "updateLookup"` | Requires synchronous read-after-write; deferred to Phase 2 |
| `fullDocumentBeforeChange` (pre-images) | Requires storing pre-image at write time; deferred |
| `$changeStream.showExpandedEvents` | Covers DDL events; deferred |
| Database-level watch (`db.watch()`) | Requires fan-out across namespaces; deferred |
| Cluster-level watch | Deferred |
| `updateDescription` field detail (`updatedFields`, `removedFields`) | Requires diff logic; deferred to Phase 2 |
| Persistent ring buffer (survive restart) | Requires WAL integration; deferred |
| Compression of ring buffer events | Low priority; deferred |
| `startAfter` (vs `resumeAfter`) | Semantically equivalent for Phase 1 purposes |

---

## Change Event Document Shape (Phase 1)

```json
{
  "_id":            { "_data": "<base64url-resume-token>" },
  "operationType":  "insert" | "update" | "delete" | "replace",
  "clusterTime":    { "$timestamp": { "t": <unix-secs>, "i": <seq-low32> } },
  "ns":             { "db": "mydb", "coll": "orders" },
  "documentKey":    { "_id": <value> },
  "fullDocument":   { ... }   // insert and replace only; omitted for update/delete in Phase 1
}
```

For `update` events, `updateDescription` is present as a placeholder:

```json
"updateDescription": { "updatedFields": {}, "removedFields": [], "truncatedArrays": [] }
```

This is a known Phase 1 limitation documented in the server's `hello` response extras (future:
`changeStreamPreAndPostImages` capability).

---

## Implementation Notes for Agent Contributors

1. **Start with the event bus** (issue #113): implement `EventBus`, `nsStream`, and the
   `Publish` call sites in `collection.go`. No wire changes needed. Unit-test with a
   single-namespace producer/consumer.

2. **Add the tailable cursor** (issue #114): implement `changeStreamCursor` and the
   `TailableCursor` interface. Modify `getMore` handler to detect and call `NextBatchWait`.
   Integration-test: open a change stream, insert a document, verify the event arrives.

3. **Add resume token support** (issue #115): implement encoding/decoding and the ring-buffer
   scan on reconnect. Test: disconnect mid-stream, reconnect with resume token, verify no
   missed events.

4. **Add the `$changeStream` aggregate stage** (issue #116): parse `$changeStream` options
   (`resumeAfter`, `startAtOperationTime`, `fullDocument`) in `internal/aggregation/`,
   construct a `changeStreamCursor`, return it as the pipeline result. This wires everything
   together end-to-end.

---

*Last updated: 2026-03-17*
