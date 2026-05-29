#!/usr/bin/env python3
"""
print_ratios.py — Read a JSONL results file and print a Markdown table of
salvobase/mongodb performance ratios grouped by workload and thread count.

Per-night sampling note (see #715):
  benchmark.yml runs each (workload, threads, target) combination 3 times. This
  script aggregates those samples into per-night median + stddev before computing
  the salvobase/mongodb ratio, so the reported number reflects central tendency
  rather than a single noisy observation.

Usage:
  python3 scripts/bench/print_ratios.py benchmarks/results/2026-03-14.jsonl
"""

import sys
import json
import statistics
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
    Returns dict: (workload, threads, target) -> dict with aggregated
    {ops_med, ops_std, p50_med, p99_med, n} across all samples seen.
    """
    grouped = defaultdict(lambda: {"ops": [], "p50": [], "p99": []})
    for r in rows:
        key = (r["workload"], r["threads"], r["target"])
        grouped[key]["ops"].append(r["ops_per_sec"])
        grouped[key]["p50"].append(r["p50_ms"])
        grouped[key]["p99"].append(r["p99_ms"])

    lookup = {}
    for key, vals in grouped.items():
        n = len(vals["ops"])
        lookup[key] = {
            "ops_med": statistics.median(vals["ops"]),
            "ops_std": statistics.stdev(vals["ops"]) if n >= 2 else 0.0,
            "p50_med": statistics.median(vals["p50"]),
            "p99_med": statistics.median(vals["p99"]),
            "n": n,
        }
    return lookup


def ratio_cell(sb, mg, metric, higher_is_better=True):
    """
    Compute ratio with first-order error propagation. For r = a/b,
        σ_r/r ≈ sqrt((σ_a/a)² + (σ_b/b)²)
    Returns string like "1.25x ± 0.10 ▲" or "—" when data is missing.
    """
    a = sb[f"{metric}_med"]
    b = mg[f"{metric}_med"]
    if b == 0 or a == 0:
        return "N/A"
    r = a / b

    # Only ops has stddev tracked. For p99 we just show the median ratio.
    if metric == "ops":
        sigma_a = sb["ops_std"]
        sigma_b = mg["ops_std"]
        # First-order error propagation: σ_r/r ≈ √((σ_a/a)² + (σ_b/b)²)
        rel_var = 0.0
        if a > 0:
            rel_var += (sigma_a / a) ** 2
        if b > 0:
            rel_var += (sigma_b / b) ** 2
        sigma_r = r * (rel_var ** 0.5)
        err_part = f" ± {sigma_r:.2f}"
    else:
        err_part = ""

    if higher_is_better:
        arrow = "▲" if r >= 1.0 else "▼"
    else:
        arrow = "▲" if r <= 1.0 else "▼"
    return f"{r:.2f}x{err_part} {arrow}"


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

    # Surface the per-night sample count so readers know whether stddev is meaningful.
    sample_counts = sorted({v["n"] for v in lookup.values()})

    print()
    print("## Salvobase vs MongoDB Community — Performance Ratios")
    print()
    print(f"Results from: `{path}`")
    if sample_counts:
        if len(sample_counts) == 1:
            print(f"Samples per combination: **{sample_counts[0]}** (median + stddev)")
        else:
            print(f"Samples per combination: **{min(sample_counts)}–{max(sample_counts)}** (varies)")
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
                cells.append(ratio_cell(sb, mg, "ops", higher_is_better=True))
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
                cells.append(ratio_cell(sb, mg, "p99", higher_is_better=False))
            else:
                cells.append("—")
        print(f"| {wl} | " + " | ".join(cells) + " |")

    print()

    # Raw numbers — aggregated per-night
    print("### Raw Numbers (per-night median across samples)")
    print()
    raw_header = "| Workload | Threads | Target | OPS (med) | OPS (σ) | P50 (ms) | P99 (ms) | n |"
    raw_sep = "| --- | --- | --- | --- | --- | --- | --- | --- |"
    print(raw_header)
    print(raw_sep)

    for wl in workloads:
        for t in thread_counts:
            for target in ("salvobase", "mongodb"):
                r = lookup.get((wl, t, target))
                if r:
                    print(
                        f"| {wl} | {t} | {target} | "
                        f"{r['ops_med']:,.0f} | {r['ops_std']:,.0f} | "
                        f"{r['p50_med']:.3f} | {r['p99_med']:.3f} | {r['n']} |"
                    )

    print()


if __name__ == "__main__":
    main()
