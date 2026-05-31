### Workload A v3 profile analysis — post-#720, post-#723/#725

**Profile files:**
- `docs/profiles/workload-a-16t-v3.txt` — CPU profile (gzipped pprof binary)
- `docs/profiles/workload-a-16t-v3-alloc.txt` — alloc profile (gzipped pprof binary)

**Capture:** Workload A (50/50 read/update), 16 threads, 100k records, 100k ops. 30-second CPU sample fired during the run phase.

**Host:** `ubuntu-latest` x86_64 via `.github/workflows/profile-capture.yml` (PR #726, run [26718214004](https://github.com/inder/salvobase/actions/runs/26718214004)).

**Caveat on the v2 baseline.** `workload-a-16t-v2.txt` was captured on macOS arm64 on 2026-05-18 (the `kevent` / `pthread_cond_wait` calls in the v2 top are macOS-specific). v3 was captured on Linux x86_64 via the new dedicated workflow. **Absolute numbers do not compare directly across hosts** — the kqueue vs epoll attribution alone shifts the runtime accounting by ~30 percentage points. v3 is the new canonical baseline; v4 onwards will be apples-to-apples against v3.

---

#### Top CPU consumers (v3, cumulative)

| Function | cum% | Notes |
|---|---|---|
| `bbolt.(*batch).run` + `.trigger` + `sync.Once.Do` | 29.5% | Write coalescing batch hot path. Largest single subsystem cost on writes. |
| `runtime/internal/syscall.Syscall6` (flat) | 22.4% | Vast majority is `fdatasync` from bbolt commit + the futex syscalls beneath `runtime.findRunnable`. |
| `runtime.mallocgc` | 19.9% | Heap allocation pressure. Driven primarily by BSON marshal/unmarshal and per-message bufio.Reader (see alloc section). |
| `commands.(*Dispatcher).Dispatch` | 18.1% | Top-level command dispatch — distributed across all handlers, not a single hotspot. |
| `bbolt.(*batch).run.func1` → `storage.updateDocs.func1` | 14.7% | Inside the batch closure; bbolt write tx body. |
| `commands.handleFind` | 12.4% | Read path. |
| `storage.(*bboltCollection).Find` → `scanFilter` | 10.8% / 10.5% | Per-doc filter scan inside bbolt View tx. |
| **`wire.WriteOpMsg`** | **10.5%** | **Down from 28.3% in v2 — #720 net.Buffers fix landed.** |
| `net.(*Buffers).WriteTo` → `internal/poll.(*FD).Writev` | 10.3% / 9.8% | Confirms the net.Buffers code path is exercised. |
| `bson.Marshal` | 9.8% | Reply encoding. |

#### Top CPU consumers (v3, flat)

| Function | flat% | Why it's hot |
|---|---|---|
| `runtime/internal/syscall.Syscall6` | 22.4% | fdatasync + futex. |
| `runtime.mallocgc` | 6.2% | One-third of mallocgc cost is direct (the rest is in callees like `nextFreeFast`, `heapSetType`). |
| `runtime.futex` | 2.6% | Goroutine scheduling under 16-client concurrency. |
| `runtime.memmove` | 2.3% | BSON copy. |
| `runtime.memclrNoHeapPointers` | 2.3% | Allocation zeroing. |
| `bsoncore.ReadElement` | 1.9% | BSON parse. |
| `runtime.scanobject` | 1.8% | GC. |
| `runtime.growslice` (cum 9.3%) | 1.0% | Document slice growth on read responses. |

---

#### Top allocations (v3)

**By object count:**

| Function | obj% | Notes |
|---|---|---|
| `bsoncore.Document.Elements` | 14.0% | Per-document BSON element iteration. Allocates the element slice each call. |
| `bsoncore.Element.KeyErr` | 11.6% | Per-element key lookup. Inlinable but still allocating. |
| `server.extractAndStripMeta` | 4.9% | Strips `$db`, `$readPreference`, etc. from incoming OpMsg. |
| `bson.Raw.Elements` | 4.0% | Wrapper around `bsoncore.Document.Elements`. |
| `bson.newDocumentWriter` | 4.0% | Reply construction. |
| `bbolt.(*page).fastCheck` | 3.6% | bbolt page-corruption sanity check. |
| `bbolt.(*Cursor).search` | 3.2% | Index seek. |
| `bson.dDecodeValue` | 3.0% | bson.D decoder allocations. |

**By bytes allocated (alloc_space, 5.58 GB total over 30s):**

| Function | bytes% | Notes |
|---|---|---|
| **`bufio.NewReaderSize`** | **11.0% (0.61 GB)** | **Misattributed in this baseline.** pprof charged the allocation to `bufio.NewReaderSize` at a call site, but the actual hot caller is **not** `wire.ReadMessage` — it's `bson.newDocumentReader` inside the BSON library, called via `bson.Unmarshal` from `server.injectField` (and `server.extractAndStripMeta`). Resolved by PR #732 (`injectField` byte-splice fast path) and PR #733 (`extractAndStripMeta` byte-splice). The wire-layer `bufio.Reader` was already moved to the `Connection` struct in PR #720, visible in v3 as `wire.WriteOpMsg` dropping 28.3%→10.5%. |
| `bson.(*valueWriter).writeValueBytes` | 10.5% | Reply encoder. |
| `bsoncore.Document.Elements` | 9.6% | Document iteration. |
| `bson.Marshal` | 8.6% | Cumulative reply marshal cost is 1.52 GB / 30s — ~27% of all allocation. |
| `bbolt.(*node).read` | 7.4% | bbolt page → node decode. |
| `bson.newDocumentWriter` | 4.4% | Per-reply writer allocation. |
| `bson.Raw.Elements` | 4.1% | Wrapper allocations. |
| `bbolt.(*DB).allocate` | 3.3% | bbolt page allocation for writes. |
| `query.Project` | 3.2% | Projection allocations. |

---

#### Key findings

**1. #720 net.Buffers fix delivered.** `wire.WriteOpMsg` dropped from 28.3% cum (v2) to 10.5% cum (v3). The `net.(*Buffers).WriteTo` → `internal/poll.(*FD).Writev` path appears explicitly in the v3 profile at 10.3% / 9.8%, confirming the optimization is hot. This is the largest single perf win in the recent sprint and the profile confirms it.

**2. `bufio.NewReaderSize` at 0.61 GB / 30s was misdiagnosed — the real source was BSON, not the wire layer.** The original reading of this baseline blamed `wire.ReadMessage` for per-message `bufio.NewReaderSize(lr, 4096)` calls. That fix had already landed in PR #720 (Connection-resident `*bufio.Reader`), and v3 shows the result: `wire.WriteOpMsg` dropped from 28.3% to 10.5% cum CPU. PR #732 traced the remaining `bufio.NewReaderSize` allocation back to `bson.newDocumentReader` inside the BSON library, called via `bson.Unmarshal` 100% from `server.injectField`. The byte-splice fast path in #732 (and the companion fix in #733 for `extractAndStripMeta`) eliminated this allocation by avoiding the `bson.D` round-trip entirely: 32× fewer allocs, 44× less memory on `injectField`; 35× fewer allocs, 71% less memory on `extractAndStripMeta`. The lesson — see the bottom of this doc — is that pprof attribution to a call site is not proof of where the work is being done.

**3. bbolt write batching is the single largest CPU sink at 29.5% cum.** Combined with the underlying `fdatasync` syscall cost, write-path bbolt costs dominate. The strategic ceiling note in `docs/perf-plan-state.json` ("bbolt is a single-writer B-tree, realistic ceiling 30-50% on writes") is the most likely architectural ceiling.

**4. BSON encode/decode is the per-request hot loop.** `bsoncore.Document.Elements` shows up at both 9.6% bytes and 14.0% objects. Combined with `bson.Marshal` (8.6%) and `bson.Raw.Elements` (4.1%), BSON-related allocation totals ~37% of all bytes allocated. The driver library is what it is. The `server.extractAndStripMeta` (4.9% objects, 14.3% bytes when injectField is included) attribution was the salvobase-side leverage point; PRs #732 and #733 replaced the `bson.D` round-trip with bsoncore byte-splice paths in both `injectField` and `extractAndStripMeta`. The next v4 capture will quantify the cumulative impact.

**5. `bbolt.(*page).fastCheck` at 3.6% objects + 3.6% bytes is unexpected.** This is a per-page sanity check that can be made cheaper or skipped in release builds. Worth investigating.

---

#### Follow-ups to file

- ~~**HIGH:** `perf(wire): per-message bufio.NewReaderSize is allocating 0.6 GB / 30s — move *bufio.Reader to Connection struct` (Plan V3 P0-B, never landed).~~ **Misdiagnosed. Resolved by PR #732 (`server.injectField` byte-splice) and PR #733 (`server.extractAndStripMeta` byte-splice). The wire-layer `bufio.Reader` already moved to the `Connection` struct in PR #720.**
- ~~**MEDIUM:** `perf(server): extractAndStripMeta + injectField re-iterate BSON document — single-pass refactor` (4.9% + 1.1% obj alloc on every request).~~ **Resolved by PR #732 + #733.**
- **LOW:** `perf(bbolt): investigate cost of (*page).fastCheck in hot read path` (3.6% obj alloc, may be skippable). Filed as #731.

---

#### Lesson

**pprof attribution to a call site is not proof — the actual caller may be inside an external library.** In this baseline, `bufio.NewReaderSize` was charged to a salvobase call site, but the real hot caller was inside the BSON library (`bson.newDocumentReader`), invoked indirectly via `bson.Unmarshal` from `server.injectField`. Diagnoses based on the symbol alone (without tracing the actual call graph) sent the original analysis after a non-existent wire-layer bug. Always confirm the caller before naming a hotspot.

Closes #721. Doc correction filed as #734.
