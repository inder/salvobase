# `wire.WriteOpMsg` — BSON vs I/O cost analysis

**Issue:** #677  
**Parent epic:** #397  
**Profile source:** `docs/profiles/workload-a-16t-v2.txt` (binary `pprof` despite the `.txt` extension)  
**Captured:** 2026-05-18, Apple M1 Pro, darwin/arm64, Go 1.26.2, salvobase commit `503407d`  
**Workload:** YCSB Workload A (50% read / 50% update), 16 threads, 100k ops, 30.16s wall, 10k record dataset

## TL;DR

**Hypothesis B (network/syscall) wins decisively.** Of the 4.79s cumulative CPU in `wire.WriteOpMsg`:

| Component | Time | % of WriteOpMsg | % of total CPU |
|-----------|-----:|----------------:|---------------:|
| Total `WriteOpMsg` (cumulative) | 4.79s | 100.00% | 28.33% |
| `syscall.write` (incl. `rawsyscalln` kernel time) | 4.77s |  99.58% | 28.21% |
| `WriteOpMsg` flat (header assembly + pool fetch) | 0.02s |   0.42% |  0.12% |
| BSON marshaling | 0.00s |   0.00% |  0.00% |

Hypothesis A (BSON marshaling) is false. `OpMsgMessage.Body` is already an encoded `bson.Raw` slice by the time it reaches `WriteOpMsg`; the function does no marshaling, only a single `copy` into a pooled assembly buffer followed by one `net.Conn.Write`.

## Methodology

The profile was captured against the same binary that is now in `master` (post-sprint optimizations). `WriteOpMsg` does not appear in the global flat-time top-30 because all of its work is in a kernel-side syscall — pprof attributes flat time to `syscall.rawsyscalln` rather than `WriteOpMsg`. To recover the call tree:

```
go tool pprof -focus=WriteOpMsg -top -cum docs/profiles/workload-a-16t-v2.txt
go tool pprof -focus=WriteOpMsg -tree -cum docs/profiles/workload-a-16t-v2.txt
```

The `-tree` output shows a clean single-path tree:

```
WriteOpMsg (4.79s cum, 0.01s flat)
  → net.(*conn).Write (4.77s cum)
    → net.(*netFD).Write
      → internal/poll.(*FD).Write
        → syscall.Write
          → syscall.write
            → syscall.syscall → syscall.syscalln
              → syscall.rawsyscalln (4.77s flat)
```

No branch enters BSON code. The in-Go work in `WriteOpMsg` is ~10–20 ms (see footnote on sampling rounding below) across four `binary.LittleEndian.PutUint32` calls, one byte store, and one `copy(buf[offset:], body)` (`internal/wire/op_msg.go:237-275`). `pprof -top` rounds the function-level flat to one 10 ms sample; `pprof -list` attributes two distinct 10 ms samples to lines 240 (`getOpMsgBuf`) and 256 (header `PutUint32`). Either way it is at the noise floor.

## Why workload A is sufficient

Issue #677 originally proposed YCSB workload B (95% read) on the theory that read-heavy traffic would be more `WriteOpMsg`-intensive. In practice:

- Every wire operation — read **or** write — triggers exactly one `WriteOpMsg` response per request. Workload A's 50/50 mix already exercises the function 100k times in 30s.
- The BSON-vs-syscall ratio is a property of the implementation, not the response payload. Reads have larger bodies (more bytes to `copy`, more bytes to `write`), which would shift the absolute numbers up, but not redistribute the split between Go-side memcpy and kernel-side syscall.
- Workload A's profile shows the in-`WriteOpMsg` Go work at 0.01s — already at the noise floor. A read-heavy run would need to push this above ~0.05s to even register, and would still leave kernel `write` dominant.

A workload B profile capture is **not blocking** the optimization decision. (If we want one for completeness later, capture is gated on installing `go-ycsb` and writing a `workloads/workloadb` spec — neither of which exists in the repo today.)

## What `WriteOpMsg` is already doing well

`internal/wire/op_msg.go:237-275` — current implementation:

1. **Bucketed buffer pool** (`opMsgBufPool`, sizes 512 / 4096 / 16384 / 65536) eliminates per-call allocation.
2. **Single `Write` syscall** assembles header + flags + section kind + body into one pooled buffer and issues one `net.Conn.Write` — no scatter writes, no Nagle stalls.
3. **No BSON marshaling.** `body` is `bson.Raw`; we just `copy` the encoded bytes.
4. **TCP_NODELAY is set** on inbound connections (`internal/server/server.go:146-150`), so the kernel sends the response immediately rather than batching it.

This is why the Go-side flat cost is 0.01s. There is no remaining application-level work to optimize.

## Where the 4.77s actually goes

The kernel time in `syscall.rawsyscalln` is the cost of:

- Copying ~21 bytes of header + N bytes of body from user space into the socket send buffer (`tcp_sendmsg` on Linux; `mbuf_alloc`+`sosend` on Darwin).
- Driving the TCP state machine (sequence numbers, ACK tracking, congestion window).
- Waking `kevent` (which separately accounts for 3.06s in the global flat profile) to schedule the next read.

At 100k ops / 30.16s ≈ 3,316 responses/s aggregated across 16 connections, the **per-syscall CPU time** (not wall-clock latency) is 4.77s / 100k = ~48 µs. That is the kernel-mode CPU charged to one `write(2)` — not the round-trip latency of one client op, which is closer to 1 / 3,316 / s ≈ 301 µs wall-clock and includes scheduler, kevent, and userspace work. The 48 µs CPU figure is consistent with a healthy darwin loopback TCP stack; there is no pathology here. The cost is largely **irreducible at the wire layer**.

## Recommended follow-on

One meaningful application-layer win remains: **eliminate the `copy(buf[offset:], body)` memcpy** for the BSON body. For large responses (multi-document aggregate batches, find batches of 100+ docs), the body can be tens of KB, and the memcpy into the assembly buffer is pure overhead.

`net.Buffers{header, body}` issues a single `writev(2)` syscall that takes a vector of iovecs, removing the need to coalesce header and body in user space. The kernel cost is the same (one syscall, same bytes written), but the user-space memcpy of the body goes away.

This is filed as a separate follow-on issue. **Expected win is small** (the memcpy is fast cache-resident work), but it is the only remaining application-level lever for `WriteOpMsg`. Bigger throughput wins for the read/write path live elsewhere — response shape (cursor batch tuning), connection concurrency, or kernel-level (`SO_SNDBUF`) tuning — and should be tracked under epic #397 broadly, not under `WriteOpMsg` specifically.

## Reproducing

```bash
# From repo root:
go tool pprof -focus=WriteOpMsg -top  -cum docs/profiles/workload-a-16t-v2.txt
go tool pprof -focus=WriteOpMsg -tree -cum docs/profiles/workload-a-16t-v2.txt
go tool pprof -focus=WriteOpMsg -list 'WriteOpMsg$' docs/profiles/workload-a-16t-v2.txt
```

The `-list` view splits the in-Go cost as ~10 ms at line 240 (`getOpMsgBuf`, the pool fetch) and ~10 ms at line 256 (header `PutUint32`), totalling ~20 ms. `pprof -top` rounds the function-level flat down to a single 10 ms bucket — same data, different rounding. Sampling noise smears attribution slightly, but the headline is unchanged: Go-side cost is at the noise floor and 4.77s sits squarely on line 270's `w.Write(buf)`.
