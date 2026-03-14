#!/usr/bin/env python3
"""
print_ratios.py — Read a JSONL results file and print a Markdown table of
salvobase/mongodb performance ratios grouped by workload and thread count.

Usage:
  python3 scripts/bench/print_ratios.py benchmarks/results/2026-03-14.jsonl
"""

import sys
import json
from collections import defaultdict


def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: skipping malformed line: {e}", file=sys.stderr)
    return rows


def build_lookup(rows):
    """
    Returns dict: (workload, threads, target) -> {ops_per_sec, p50_ms, p99_ms}
    """
    lookup = {}
    for r in rows:
        key = (r["workload"], r["threads"], r["target"])
        lookup[key] = r
    return lookup


def ratio_str(a, b, higher_is_better=True):
    """
    Return ratio string with directional arrow.
    higher_is_better=True  → ratio > 1 means salvobase wins (green arrow up)
    higher_is_better=False → ratio < 1 means salvobase wins (latency)
    """
    if b == 0:
        return "N/A"
    r = a / b
    if higher_is_better:
        arrow = "▲" if r >= 1.0 else "▼"
    else:
        arrow = "▲" if r <= 1.0 else "▼"
    return f"{r:.2f}x {arrow}"


def main():
    if len(sys.argv) < 2:
        print("Usage: print_ratios.py <results.jsonl>", file=sys.stderr)
        sys.exit(1)

    path = sys.argv[1]
    rows = load_jsonl(path)

    if not rows:
        print("No data found.", file=sys.stderr)
        sys.exit(1)

    lookup = build_lookup(rows)

    workloads = sorted(set(r["workload"] for r in rows))
    thread_counts = sorted(set(r["threads"] for r in rows))

    print()
    print("## Salvobase vs MongoDB Community — Performance Ratios")
    print()
    print(f"Results from: `{path}`")
    print()

    # OPS table
    print("### Throughput (OPS) — salvobase / mongodb (higher = salvobase wins)")
    print()
    header = "| Workload | " + " | ".join(f"{t}T" for t in thread_counts) + " |"
    sep = "| --- |" + " --- |" * len(thread_counts)
    print(header)
    print(sep)

    for wl in workloads:
        cells = []
        for t in thread_counts:
            sb = lookup.get((wl, t, "salvobase"))
            mg = lookup.get((wl, t, "mongodb"))
            if sb and mg:
                cells.append(ratio_str(sb["ops_per_sec"], mg["ops_per_sec"], higher_is_better=True))
            else:
                cells.append("—")
        print(f"| {wl} | " + " | ".join(cells) + " |")

    print()

    # P99 table
    print("### P99 Latency (ms) — salvobase / mongodb (lower ratio = salvobase wins)")
    print()
    print(header)
    print(sep)

    for wl in workloads:
        cells = []
        for t in thread_counts:
            sb = lookup.get((wl, t, "salvobase"))
            mg = lookup.get((wl, t, "mongodb"))
            if sb and mg:
                cells.append(ratio_str(sb["p99_ms"], mg["p99_ms"], higher_is_better=False))
            else:
                cells.append("—")
        print(f"| {wl} | " + " | ".join(cells) + " |")

    print()

    # Raw numbers
    print("### Raw Numbers")
    print()
    raw_header = "| Workload | Threads | Target | OPS | P50 (ms) | P99 (ms) |"
    raw_sep = "| --- | --- | --- | --- | --- | --- |"
    print(raw_header)
    print(raw_sep)

    for wl in workloads:
        for t in thread_counts:
            for target in ("salvobase", "mongodb"):
                r = lookup.get((wl, t, target))
                if r:
                    print(
                        f"| {wl} | {t} | {target} | {r['ops_per_sec']:,.0f} | {r['p50_ms']:.3f} | {r['p99_ms']:.3f} |"
                    )

    print()


if __name__ == "__main__":
    main()
