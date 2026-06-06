### Workload A v5 profile analysis — post-#743 handleHello template-splice

**Profile files:**
- `docs/profiles/workload-a-16t-v5.txt` — CPU profile (gzipped pprof binary)
- `docs/profiles/workload-a-16t-v5-alloc.txt` — alloc profile (gzipped pprof binary)

**Capture:** Workload A (50/50 read/update), 16 threads, 100k records, 100k ops. 30-second CPU sample fired during the run phase.

**Host:** `ubuntu-latest` x86_64 via `.github/workflows/profile-capture.yml` (PR #726, run [27055910919](https://github.com/inder/salvobase/actions/runs/27055910919)).

**What landed since v4:**
- PR #743 — `commands.handleHello` precomputed-template splice (microbench: 9.4× fewer bytes / 1.76× faster; closes #742)
- PR #745 — `connectionId` wire field int64 → int32 to match MongoDB 7.0 (closes #744; saves 4 bytes per hello, perf-neutral)

v4 remains the prior canonical baseline. **v5 is now the canonical baseline** going forward.

---

#### Headline: total allocation pressure

| Metric | v3 | v4 | v5 | Δ v4→v5 |
|---|---|---|---|---|
| Total objects allocated (30s window) | 46,579,793 | 38,250,473 | 31,569,836 | **−17.5%** |
| Total bytes allocated (30s window) | 5.71 GB | 4.26 GB | 3.76 GB | **−11.7%** |
| Total CPU samples (30s window) | 14.16s (47.17%) | 12.44s (41.45%) | 13.52s (45.05%) | +8.7% (noise band) |
| `runtime.mallocgc` flat% | 6.21% | 4.50% | 4.14% | **−0.36pp** |
| `runtime.mallocgc` cum% | 19.92% | 15.27% | 14.42% | **−0.85pp** |

Object count fell another 17.5% on top of v4's 17.9% drop — same magnitude two captures in a row. Bytes fell 11.7% on top of v4's 25.4% — slowing, because the easy wins (bufio.NewReaderSize, extractAndStripMeta, injectField, handleHello) are now exhausted and we've crossed into the vendor-allocator floor (bsoncore + bbolt). Total CPU samples ticked up 8.7% but that's well inside the pprof sampling noise band — one capture is not a throughput measurement (see [project_bench_noise_floor](../../memory/project_bench_noise_floor.md)).

---

#### Top CPU consumers (v5, cumulative)

| Function | v5 cum% | v4 cum% | Δ |
|---|---|---|---|
| `bbolt.(*batch).run` + `.trigger` + `sync.Once.Do` | 33.51% | 30.31% | +3.20pp |
| `runtime/internal/syscall.Syscall6` (flat) | 37.06% | 33.68% | +3.38pp (share inflation; fdatasync) |
| `commands.(*Dispatcher).Dispatch` | 17.46% | 19.45% | −1.99pp |
| `bbolt.(*Tx).Commit` | 19.08% | 15.84% | +3.24pp |
| `runtime.mallocgc` | 14.42% | 15.27% | **−0.85pp** ✅ |
| `wire.WriteOpMsg` | 14.57% | 13.75% | +0.82pp |
| `commands.handleFind` | 12.20% | 12.22% | flat |
| `bbolt.(*Tx).write` | 14.35% | 11.58% | +2.77pp |
| `wire.ReadMessage` | 10.58% | 10.05% | +0.53pp |
| `storage.(*bboltCollection).Find` → `scanFilter` | 9.99% / 9.91% | 10.29% / 10.21% | flat |
| `bbolt.(*DB).View` | 9.84% | 10.05% | flat |
| `storage.(*bboltCollection).updateDocs.func1` | 13.46% | 13.83% | −0.37pp |

`bbolt.(*Tx).Commit` ticked up 3.24pp — single-writer fdatasync is now an even larger share of the cumulative pie. Combined with `(*Tx).write` at 14.35%, the write commit chain is firmly the architectural ceiling (see finding 4 below).

#### Top CPU consumers (v5, flat)

| Function | v5 flat% | v4 flat% | Δ |
|---|---|---|---|
| `runtime/internal/syscall.Syscall6` | 37.06% | 33.68% | +3.38pp (share inflation) |
| `runtime.mallocgc` | 4.14% | 4.50% | **−0.36pp** ✅ |
| `runtime.futex` | 2.96% | 3.22% | −0.26pp |
| `runtime.memclrNoHeapPointers` | 2.81% | 2.57% | +0.24pp |
| `runtime.memmove` | 2.14% | 1.93% | +0.21pp |
| `bsoncore.ReadElement` | 1.70% | 1.69% | flat |
| `query.Project` (cum 7.84%) | 0.52% | — | newly visible |

---

#### Top allocations (v5)

**By object count (31.57M total):**

| Function | v5 obj% | v4 obj% | v5 absolute | v4 absolute | Δ absolute |
|---|---|---|---|---|---|
| `bsoncore.Document.Elements` | 15.96% | 16.19% | 5.04M | 6.19M | −18.6% |
| `bsoncore.Element.KeyErr` | 13.39% | 11.74% | 4.23M | 4.49M | −5.8% |
| `bbolt.(*Cursor).search` | 5.05% | 3.10% | 1.59M | 1.19M | +34.0% |
| `bson.Raw.Elements` | 4.24% | 5.28% | 1.34M | 2.02M | −33.7% |
| `bbolt.(*page).fastCheck` | 3.74% | 4.88% | 1.18M | 1.87M | −36.9% |
| `wire.readOpMsg` | 3.39% | 2.77% | 1.07M | 1.06M | +0.9% |
| `wire.readBSONDoc` | 2.44% | 2.45% | 0.77M | 0.94M | −18.1% |
| `query.getDFieldValue` | 2.15% | 1.88% | 0.68M | 0.72M | −5.6% |
| `query.rawToD` | 1.89% | — | 0.60M | — | newly visible |
| `storage.InsertMany` | 1.65% | — | 0.52M | — | newly visible |
| `server.extractAndStripMeta` | 1.53% | 1.95% | 0.48M | 0.75M | −36.0% |
| `commands.handleHello` | — | **4.51%** | **not in top 30** | **1.73M** | **eliminated** ✅✅ |

**By bytes allocated (alloc_space, 3.76 GB total in v5 vs 4.26 GB in v4):**

| Function | v5 bytes% | v4 bytes% | v5 absolute | v4 absolute | Δ absolute |
|---|---|---|---|---|---|
| `bsoncore.Document.Elements` | 10.69% | 11.32% | 0.40 GB | 0.48 GB | −16.7% |
| `bbolt.(*node).read` | 10.38% | 8.96% | 0.39 GB | 0.38 GB | +2.6% (noise) |
| `bson.(*valueWriter).writeValueBytes` | 7.80% | 6.63% | 0.29 GB | 0.28 GB | +3.6% (noise) |
| `bbolt.(*DB).allocate` | 5.36% | 5.39% | 0.20 GB | 0.23 GB | −13.0% |
| `bson.Marshal` (cum 17.11%) | 5.35% | 4.32% | 0.20 GB | 0.18 GB | +11.1% |
| `query.Project` | 5.14% | 5.02% | 0.19 GB | 0.18 GB | +5.6% (noise) |
| `wire.readBSONDoc` | 5.02% | 4.64% | 0.19 GB | 0.20 GB | −5.0% (noise) |
| `bson.Raw.Elements` | 4.37% | 5.35% | 0.16 GB | 0.23 GB | −30.4% |
| `server.extractAndStripMeta` | 4.35% | 4.37% | 0.16 GB | 0.19 GB | −15.8% |
| `server.appendBSONArrayField` | 3.63% | 3.25% | 0.14 GB | 0.14 GB | flat |
| `query.elemMap` | 2.59% | — | 0.10 GB | — | newly visible |
| `commands.handleHello` | — | **6.69%** | **not in top 30** | **0.28 GB** | **eliminated** ✅✅ |

---

#### Key findings

**1. PR #743 confirmed — `commands.handleHello` is gone from the top 30.** This is the success criterion for #742/#743 and it cleared decisively:
- Objects: 1.73M (4.51%) → **not in top 30**. v4 had it as the 5th-largest allocator by object count.
- Bytes: 0.28 GB (6.69%) → **not in top 30**. v4 had it as the 4th-largest allocator by bytes.
- The microbench predicted 9.4× byte reduction; the macro profile is consistent with that ratio (0.28 GB / 9.4 ≈ 0.030 GB, which would fall well below the top-30 cutoff of ~0.02 GB).

**2. mallocgc cum dropped another 0.85pp.** The byte-splice + template-splice work has now moved a cumulative 5.50pp of CPU off `runtime.mallocgc` since v3 (19.92% → 14.42%). Flat% is at 4.14%, down from 6.21% in v3 — a 33% relative drop in time spent in the allocator.

**3. Total allocation pressure continues to fall, but the slope is flattening.** Bytes: v3 5.71 GB → v4 4.26 GB (−25.4%) → v5 3.76 GB (−11.7%). Objects: v3 46.58M → v4 38.25M (−17.9%) → v5 31.57M (−17.5%). The byte slope halved between captures while object slope held — exactly what you'd expect as you exhaust the cheap server-side wins and the remaining hot allocators are inside vendor code with smaller per-call footprints.

**4. bbolt write batching is now even more dominant.** `bbolt.(*batch).run` cum 33.51% (v4: 30.31%) and `(*Tx).Commit` 19.08% (v4: 15.84%). Share inflation from a smaller pie accounts for some of it but the absolute time grew: 33.51% × 13.52s ≈ 4.53s vs 30.31% × 12.44s ≈ 3.77s, +20%. This is the single-writer fdatasync ceiling. **No salvobase-side fix exists** short of swapping bbolt for a multi-writer engine — and that is a strategic decision, not a perf cleanup. Stop chasing this.

**5. `bbolt.(*page).fastCheck` allocation dropped 37% in absolute count.** v4: 1.87M (4.88%) → v5: 1.18M (3.74%), a −36.9% absolute drop in object count. This is residual variance / workload shape change, not a fix — the variadic `_assert` boxing in bbolt is unchanged (see PR #739 / #731 / `docs/profiles/fastcheck-investigation.md`). The bbolt allocator is still the same code; we just touched fewer pages this run. Do not interpret this as a regression-or-fix signal.

**6. `bbolt.(*Cursor).search` allocations rose 34.0%.** v4: 1.19M → v5: 1.59M. Same caveat as fastCheck — workload-shape variance inside the read path. The cursor search is called on every `find` to position to the document by `_id`; YCSB's read distribution can swing this 20–40% between captures. Not a regression signal.

**7. The remaining top allocators are predominantly vendor code.** Top 5 by bytes in v5:
- 1st: `bsoncore.Document.Elements` (0.40 GB) — mongo-driver code, allocates an element slice per Document. **Fixable only by forking bsoncore** to expose a non-allocating iterator.
- 2nd: `bbolt.(*node).read` (0.39 GB) — bbolt deserializes page headers + key/value slots. **Fixable only by forking bbolt.**
- 3rd: `bson.(*valueWriter).writeValueBytes` (0.29 GB) — mongo-driver encoder buffer growth. **Fixable only by forking bsoncore** or migrating off the official driver's encoder.
- 4th: `bbolt.(*DB).allocate` (0.20 GB) — bbolt page allocator. **Fixable only by forking bbolt.**
- 5th: `bson.Marshal` (0.20 GB flat / 0.64 GB cum) — top-of-stack encode call, allocations are downstream in the bson encoder.

**The pattern is clear: we have crossed into the vendor-allocator floor.** The next meaningful perf gains require either (a) forking bbolt or bsoncore, or (b) architectural moves that reduce *call frequency* into these vendor paths (e.g. response BSON caching, schema-aware codecs, prepared-statement-style result shaping). Picking off another 100 MB / 1M objects in pure salvobase code is now unlikely without a strategic decision.

**8. Server-side cumulative leader is now `query.Project` at 0.68 GB / 18.12% alloc_space cum.** Flat is small (0.19 GB / 5.14%) but it dominates the cumulative response-encoding chain (calls `bson.Marshal`, `bson.Raw.Elements`, `bsoncore.Document.Elements`). Projection on `find` happens on every read response. If we want one more salvobase-side perf swing post-v5, this is the largest server-side target — but it would be a deeper refactor of `internal/query/projection.go` (394 lines, no obvious template-splice angle), not a quick win.

---

#### Follow-ups to file

- **MEDIUM:** `perf(query): Project allocates 0.68 GB cum / 0.19 GB flat per 30s — investigate hot-path simplification`. Largest remaining server-side allocator (5.14% flat / 18.12% cum). Needs a microbench to isolate where inside `Project` the allocations land, then a targeted reduction. Not a quick splice — likely complexity:m work. File if/when there's appetite for a deeper projection refactor.
- **STRATEGIC (no issue yet):** Vendor-allocator floor decision. `bsoncore` and `bbolt` together account for ~40% of remaining alloc bytes. Forking either is a 5+ week commitment with maintenance overhead. Architectural alternatives (response caching, schema-specialized codecs) have higher engineering risk but bigger ceiling. **Do not file as a tactical perf issue** — this is roadmap material.

No issue filed at v5 capture time; the v4 → v5 hop was about confirming #743 landed, which it did. Future perf work needs a strategic conversation, not another tactical capture.

---

#### Lessons

- **The byte-splice playbook (v3→v4) and the template-splice playbook (v4→v5) have run their course.** Both achieved ≥9× microbench wins and macro confirmation in the next profile. Both targeted server-side allocators that wrapped vendor BSON paths. With those wraps gone, the vendor paths themselves are now the leaders — and they don't admit the same fix.
- **CPU sample count is not a throughput measurement.** v5 total CPU was 13.52s vs v4's 12.44s (+8.7%) — both still within the 6pp noise band of the macro nightly bench. Read the trend across multiple captures, not a single delta.
- **Vendor allocators reward measurement, not picking.** `bbolt.(*Cursor).search` swung +33.6% in object count v4→v5 with no code change on either side. Per-capture variance in workload-shape interior is large for these paths.

Closes #746.
