### `bbolt.(*page).fastCheck` allocation investigation — issue #731

**Surface area in v3 profile:** `bbolt.(*page).fastCheck` accounts for 3.6% of allocated objects and 3.6% of allocated bytes in `docs/profiles/workload-a-16t-v3-alloc.txt`, with cumulative CPU around 1.5%. Workload A is 50/50 read/update, 16 threads, 100k records.

**Bbolt version pinned:** `go.etcd.io/bbolt v1.3.11` (`go.mod`).

---

#### Why the allocation exists

`fastCheck` is a per-page sanity check that runs inside `(*Tx).page(id)`. Every time a transaction resolves a `pgid` to a `*page` — either by hitting the dirty-pages map or by falling through to the mmap — bbolt asserts that the returned page is self-consistent before handing it back. The two branches are an if/else, so exactly one `fastCheck` call fires per `(*Tx).page` invocation.

Source: `go.etcd.io/bbolt@v1.3.11/page.go:56-64` —

```go
func (p *page) fastCheck(id pgid) {
    _assert(p.id == id, "Page expected to be: %v, but self identifies as %v", id, p.id)
    // Only one flag of page-type can be set.
    _assert(p.flags == branchPageFlag ||
        p.flags == leafPageFlag ||
        p.flags == metaPageFlag ||
        p.flags == freelistPageFlag,
        "page %v: has unexpected type/flags: %x", p.id, p.flags)
}
```

And `_assert` itself (`db.go:1387`) —

```go
func _assert(condition bool, msg string, v ...interface{}) {
    if !condition {
        panic(fmt.Sprintf("assertion failed: "+msg, v...))
    }
}
```

The allocation is the variadic `v ...interface{}` parameter. Every call to `_assert` boxes its arguments into an `[]interface{}` slice on the heap before the `if !condition` check runs. The two assert calls in `fastCheck` each pass two or three boxed `pgid` (uint64) / `uint16` flag values. The boxing happens unconditionally on every page resolution, even though the panic branch is never taken during healthy operation.

So this is not a debug-only check, it is not gated by a build tag, and it is not configurable. It runs on every `(*Tx).page` call, which is on the hot path of every cursor seek and every page-by-page scan.

#### Why it shows up so heavily under Workload A

The salvobase side uses `bolt.Bucket.Cursor()` and `bolt.Bucket.ForEach` in the standard places — see `internal/storage/collection.go`:

- `Find()` falls through to `bboltScanCursor` (line 614-616) which iterates every key in the collection bucket via `Cursor().Next()`.
- Index range scans use `cur.Seek` then `cur.Next` (`rangeIndexScanTx`, `indexScanFwd*` near lines 1091, 1163, 1279).
- Capped-collection eviction walks the bucket with `b.ForEach` (line 489).
- Metadata bucket reads (`meta.Cursor()` in `index.go`) hit it on every collection open.

Every cursor `Next`, every `Seek` descent through branch pages, and every `node.read` for a B-tree node calls `(*Tx).page(pgid)`. Each `(*Tx).page` call triggers exactly one `fastCheck`, which in turn runs two `_assert` invocations. Each `_assert` heap-allocates an `[]interface{}` of size 2 or 3 with the page id and flags boxed inside.

For 100k records with one document per leaf page slot, a single `Find` scan touches O(leaves) pages and O(log n) branch pages on top of any seek operations. Multiply by 16 concurrent clients and 100k operations across the 30-second sample window and you land at exactly the 3.6% object / 3.6% byte share observed in v3.

#### Is there a salvobase-side amortization?

No, not without restructuring how we use bbolt cursors. `fastCheck` runs inside `(*Tx).page`, which is the only documented path from `pgid` to `*page`. We can't bypass it without forking bbolt. Reducing the number of pages we touch per query is the wrong shape of work for this issue — that is the storage engine ceiling discussed in `docs/perf-plan-state.json`, not a sanity-check optimization.

#### Is there an upstream fix?

Yes, and it is small. The fix is to inline the asserts so the variadic boxing only happens on the failure path:

```go
func (p *page) fastCheck(id pgid) {
    if p.id != id {
        panic(fmt.Sprintf("assertion failed: Page expected to be: %v, but self identifies as %v", id, p.id))
    }
    if p.flags != branchPageFlag &&
        p.flags != leafPageFlag &&
        p.flags != metaPageFlag &&
        p.flags != freelistPageFlag {
        panic(fmt.Sprintf("assertion failed: page %v: has unexpected type/flags: %x", p.id, p.flags))
    }
}
```

This removes the `_assert` indirection for the two hottest call sites in the project. The healthy path is now pure stack: one `pgid` compare and four flag equality checks ORed together. No variadic boxing, no `[]interface{}` allocation. The unhealthy path still produces the same panic message. There is no behavior change.

A broader cleanup would also rewrite `_assert` itself to take a `func() string` for the message, so all other call sites stop boxing on the happy path. That is larger surgery and would touch every `_assert` caller in bbolt.

#### Recommendation

**File upstream.** This is a five-line patch against `go.etcd.io/bbolt/page.go` that helps every bbolt user, not just salvobase. We should not fork or vendor for this.

Tracking:

- Open an upstream issue at `etcd-io/bbolt` describing the variadic-boxing allocation in `fastCheck` and attach the v3 alloc profile excerpt as evidence.
- Follow with a PR implementing the inlined version above.
- Hold a salvobase-side workaround in reserve only if upstream declines or stalls beyond two release cycles. A vendor fork for a 1.5% CPU / 3.6% alloc win is not worth the maintenance tax.

**Do not fix this in salvobase.** The cost is real but small, the fix lives at the wrong layer, and the upstream path is cheap.

---

#### Numbers for the upstream report

From `docs/profiles/workload-a-16t-v3-alloc.txt` and `workload-a-16t-v3.txt`:

| Metric | Value |
|---|---|
| Allocated objects | 3.6% of total |
| Allocated bytes | 3.6% of total (~200 MB of the 5.58 GB / 30s sample) |
| Cumulative CPU | ~1.5% |
| Workload | A (50/50 R/U), 16 clients, 100k records, 100k ops |
| Host | `ubuntu-latest` x86_64 |
| Bbolt version | v1.3.11 |

Closes #731.
