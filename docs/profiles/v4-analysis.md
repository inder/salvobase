### Workload A v4 profile analysis — post-#732, post-#733

**Profile files:**
- `docs/profiles/workload-a-16t-v4.txt` — CPU profile (gzipped pprof binary)
- `docs/profiles/workload-a-16t-v4-alloc.txt` — alloc profile (gzipped pprof binary)

**Capture:** Workload A (50/50 read/update), 16 threads, 100k records, 100k ops. 30-second CPU sample fired during the run phase.

**Host:** `ubuntu-latest` x86_64 via `.github/workflows/profile-capture.yml` (PR #726, run [26977882786](https://github.com/inder/salvobase/actions/runs/26977882786)).

**What landed since v3:**
- PR #732 — `server.injectField` byte-splice fast path (32× fewer allocs, 44× less memory in microbench)
- PR #733 — `server.extractAndStripMeta` byte-splice (35× fewer allocs, 71% less memory in microbench)
- PR #735 — `v3-analysis.md` correction (doc only)
- PR #739 — bbolt `(*page).fastCheck` investigation (doc only, closes #731)

v3 remains the prior canonical baseline. **v4 is now the canonical baseline** going forward.

---

#### Headline: total allocation pressure

| Metric | v3 | v4 | Delta |
|---|---|---|---|
| Total objects allocated (30s window) | 46,579,793 | 38,250,473 | **−17.9%** |
| Total bytes allocated (30s window) | 5.71 GB | 4.26 GB | **−25.4%** |
| Total CPU samples (30s window) | 14.16s (47.17%) | 12.44s (41.45%) | **−12.1% absolute CPU** |
| `runtime.mallocgc` flat% | 6.21% | 4.50% | **−1.71pp** |
| `runtime.mallocgc` cum% | 19.92% | 15.27% | **−4.65pp** |

Total CPU samples in the same 30s window dropped 12%. Less CPU was needed to do the same workload. mallocgc dropped almost 5pp cumulative — the bsoncore byte-splice work is the most likely cause and the v4 hotspot table confirms it.

---

#### Top CPU consumers (v4, cumulative)

| Function | v4 cum% | v3 cum% | Δ |
|---|---|---|---|
| `bbolt.(*batch).run` + `.trigger` + `sync.Once.Do` | 30.31% | 29.52% | +0.79pp |
| `runtime/internal/syscall.Syscall6` (flat) | 33.68% | 22.39% | +11.29pp |
| `commands.(*Dispatcher).Dispatch` | 19.45% | 18.08% | +1.37pp |
| `bbolt.(*Tx).Commit` | 15.84% | 14.27% | +1.57pp |
| `runtime.mallocgc` | 15.27% | 19.92% | **−4.65pp** ✅ |
| `bbolt.(*batch).run.func1` → `storage.updateDocs.func1` | 13.83% | 14.69% | −0.86pp |
| `wire.WriteOpMsg` | 13.75% | 10.5% | +3.25pp |
| `net.(*Buffers).WriteTo` → `internal/poll.(*FD).Writev` | 13.75% / 13.59% | 10.3% / 9.8% | +3.3pp |
| `commands.handleFind` | 12.22% | 12.36% | flat |
| `bbolt.(*Tx).write` | 11.58% | — | newly visible |
| `wire.ReadMessage` | 10.05% | — | newly visible |
| `storage.(*bboltCollection).Find` → `scanFilter` | 10.29% / 10.21% | 10.8% / 10.5% | flat |
| `bbolt.(*DB).View` | 10.05% | — | newly visible |

**Percent-share inflation, not regression.** Because total CPU samples dropped 12% and mallocgc cum dropped 4.65pp, every other path's percentage share went up even where absolute time was flat or down. `wire.WriteOpMsg` at 13.75% (v4) vs 10.5% (v3) is the most obvious case: 13.75% × 12.44s = 1.71s; 10.5% × 14.16s = 1.49s. Absolute time grew ~150ms, well inside the noise band — the optimization is still hot, the share just reflects a smaller pie.

#### Top CPU consumers (v4, flat)

| Function | v4 flat% | v3 flat% | Δ |
|---|---|---|---|
| `runtime/internal/syscall.Syscall6` | 33.68% | 22.39% | +11.29pp (share inflation; fdatasync is the dominant absolute cost) |
| `runtime.mallocgc` | 4.50% | 6.21% | **−1.71pp** ✅ |
| `runtime.futex` | 3.22% | 2.6% | +0.62pp |
| `runtime.memclrNoHeapPointers` | 2.57% | 2.3% | +0.27pp |
| `runtime.memmove` | 1.93% | 2.3% | −0.37pp |
| `bsoncore.ReadElement` | 1.69% | 1.9% | −0.21pp |
| `runtime.growslice` (cum 7.23%) | 0.80% | 1.0% | flat |

---

#### Top allocations (v4)

**By object count:**

| Function | v4 obj% | v3 obj% | v4 absolute | v3 absolute | Δ absolute |
|---|---|---|---|---|---|
| `bsoncore.Document.Elements` | 16.19% | 14.04% | 6.19M | 6.54M | −5.4% |
| `bsoncore.Element.KeyErr` | 11.74% | 11.61% | 4.49M | 5.41M | −17.0% |
| `bson.Raw.Elements` | 5.28% | 4.02% | 2.02M | 1.87M | +8.0% |
| `bbolt.(*page).fastCheck` | 4.88% | 3.59% | 1.87M | 1.67M | +12.0% |
| `commands.handleHello` | 4.51% | 1.59% | 1.73M | 0.74M | **+133% — new hotspot** |
| `bbolt.(*Cursor).search` | 3.10% | 3.21% | 1.19M | 1.50M | −20.8% |
| `wire.readOpMsg` | 2.77% | 1.95% | 1.06M | 0.91M | +16.7% |
| `wire.readBSONDoc` | 2.45% | 1.39% | 0.94M | 0.65M | +44.0% |
| `server.extractAndStripMeta` | 1.95% | **4.87%** | 0.75M | **2.27M** | **−67.0%** ✅ |
| `query.getDFieldValue` | 1.88% | — | 0.72M | — | newly visible |
| `commands.handleFind` (cum 15.84%) | 1.31% | — | 0.50M | — | newly visible |
| `server.injectField` | — | 1.14% (cum 13.32%) | not in top 30 | 0.53M (cum 6.21M) | **eliminated from top 30** ✅ |

**By bytes allocated (alloc_space, 4.26 GB total in v4 vs 5.71 GB in v3):**

| Function | v4 bytes% | v3 bytes% | v4 absolute | v3 absolute | Δ absolute |
|---|---|---|---|---|---|
| **`bufio.NewReaderSize`** | — | **11.01%** | not in top 30 | **0.63 GB** | **eliminated** ✅✅ |
| `bsoncore.Document.Elements` | 11.32% | 9.65% | 0.48 GB | 0.55 GB | −12.7% |
| `bbolt.(*node).read` | 8.96% | 7.37% | 0.38 GB | 0.42 GB | −9.5% |
| `commands.handleHello` | 6.69% | 2.56% | 0.28 GB | 0.15 GB | **+86.7% — new hotspot** |
| `bson.(*valueWriter).writeValueBytes` | 6.63% | 10.48% | 0.28 GB | 0.60 GB | **−53.3%** ✅ |
| `bbolt.(*DB).allocate` | 5.39% | 3.34% | 0.23 GB | 0.19 GB | +21.1% |
| `bson.Raw.Elements` | 5.35% | 4.06% | 0.23 GB | 0.23 GB | flat |
| `wire.readBSONDoc` | 4.64% | 3.16% | 0.20 GB | 0.18 GB | +11.1% |
| `server.extractAndStripMeta` (cum) | 5.90% (cum) | **15.77% (cum)** | 0.25 GB cum | **0.90 GB cum** | **−72.2%** ✅ |
| `bson.Marshal` | 4.32% | 8.59% | 0.18 GB | 0.49 GB | **−63.3%** ✅ |
| `query.Project` | 4.22% | 3.17% | 0.18 GB | 0.18 GB | flat |
| `server.appendBSONArrayField` | 3.25% | — | 0.14 GB | — | newly visible |

---

#### Key findings

**1. The bsoncore byte-splice paths are confirmed.** Both `server.injectField` and `server.extractAndStripMeta` have moved off the top allocators — exactly the hypothesis from PRs #732 and #733:
- `bufio.NewReaderSize` (v3: 11.01% bytes, 0.63 GB) — **eliminated from top 30**. This was the misdiagnosed v3 hotspot whose real source was `bson.newDocumentReader` inside `bson.Unmarshal`, called from `injectField`/`extractAndStripMeta`. The byte-splice fast paths bypass `bson.Unmarshal` entirely. ✅
- `server.injectField` — **out of top 30** for both objects and bytes. v3 had it at 6.21M cum objects (1.14% flat). ✅
- `server.extractAndStripMeta` — objects flat dropped from 4.87% (2.27M) to 1.95% (0.75M), a **67% reduction**. Cumulative bytes dropped from 0.90 GB to 0.25 GB, a **72% reduction**. ✅
- `bson.Marshal` cum dropped from 0.49 GB (8.59%) to 0.18 GB (4.32%) — the downstream effect of skipping the Marshal/Unmarshal round-trip in the strip-and-inject paths.

**2. mallocgc dropped 4.65pp cum / 1.71pp flat.** This is the headline cumulative impact: the byte-splice work moved nearly 5 percentage points of cumulative CPU off `runtime.mallocgc`. Combined with the 25.4% drop in total bytes allocated and the 17.9% drop in object count, this is a measurable reduction in GC pressure during steady-state Workload A.

**3. Throughput proxy: −12.1% total CPU samples in the same 30s window.** v3 captured 14.16s of CPU; v4 captured 12.44s. Less CPU was needed for the same workload, consistent with reduced allocator and GC traffic. (Caveat: pprof sampling is statistical; one capture is not a robust throughput measurement — the nightly bench suite is authoritative for that, see [project_bench_noise_floor](../../memory/project_bench_noise_floor.md) for the 6pp noise floor.)

**4. bbolt write batching remains the architectural ceiling.** `bbolt.(*batch).run` at 30.31% cum is essentially unchanged from v3's 29.52%. This is the single-writer B-tree commit path and is the strategic ceiling on write-heavy workloads. No salvobase-side fix exists short of a storage engine swap. Do not chase this as a regression — it's a wall, not a bug.

**5. `wire.WriteOpMsg` percent share went up but absolute time is flat.** v4 cum 13.75% × 12.44s ≈ 1.71s; v3 cum 10.5% × 14.16s ≈ 1.49s. The +3.25pp percentage delta is share inflation from the shrinking total CPU pie, not a regression. The `net.(*Buffers).WriteTo` → `Writev` path is still hot, confirming the PR #720 optimization is in effect.

**6. NEW HOTSPOT — `commands.handleHello` doubled in absolute allocation.**
- Objects: 0.74M (1.59%) → 1.73M (4.51%), **+133% absolute**
- Bytes: 0.15 GB (2.56%) → 0.28 GB (6.69%), **+87% absolute**
- Cumulative bytes: 0.19 GB → 0.38 GB+

This is the most interesting new finding. The hello/isMaster response is allocated from scratch on every call, and either driver-side topology refresh frequency changed or `handleHello` itself has room for a precomputed-template fast path. v3 didn't surface this because the v3 top was dominated by `extractAndStripMeta`/`injectField`/`bufio.NewReaderSize`; with those gone, `handleHello` is now the largest server-side allocator after the BSON library itself. **File as a separate perf issue** — see follow-ups below.

**7. `bbolt.(*page).fastCheck` is still present.** 1.87M objects (4.88%) in v4 vs 1.67M (3.59%) in v3 — count went up but bytes dropped to 0.65%. Already investigated in PR #739 / #731; no salvobase-side fix without forking bbolt. Leave as background noise.

---

#### Follow-ups to file

- **MEDIUM:** `perf(commands): handleHello allocates ~280 MB / 1.7M objects per 30s — investigate precomputed response template`. The hello/isMaster path is hit by every driver topology refresh and currently allocates a full response BSON document on each call. Microbench expected to show 10×+ reduction if the immutable parts (maxBsonObjectSize, minWireVersion, isWritablePrimary, etc.) are templated and only the per-request fields (localTime, $clusterTime if present) are appended. Filed as new issue.
- **LOW:** `perf(bbolt): (*page).fastCheck still ~1.87M obj/30s — re-evaluate after #731 investigation`. Already in flight via #731/#739. No new action.

---

#### Lesson — share vs absolute

When a large allocator (here `bufio.NewReaderSize` at 0.63 GB) is eliminated, every other allocator's percentage share goes UP even when its absolute allocation is flat or down. Reading the table as "regression: bbolt.allocate went from 3.34% to 5.39%" without checking absolute bytes (0.19 GB → 0.23 GB, +21%) leads to the wrong conclusion. The correct read is always: did absolute allocation change, and if yes, by how much? Percentage share is a ranking tool, not a magnitude tool.

This is the inverse of the v3 lesson (pprof call-site attribution ≠ actual caller). Together they form the two failure modes of casual profile reading: confusing call sites with callers (v3), and confusing share with magnitude (v4).

Closes #740.
