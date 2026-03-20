#!/usr/bin/env python3
"""
should_run_benchmark.py — Adaptive benchmark gate.

Reads the latest results from bench-data and decides whether a new full
YCSB run is warranted based on the current performance gap. Outputs
'should_run=true/false' to GITHUB_OUTPUT (or prints when run locally).

Tier table:
  gap > 50pp  (ratio < 0.40)  →  throttle 3h   (crisis mode)
  gap > 25pp  (ratio < 0.65)  →  throttle 6h   (active sprint)
  gap > 10pp  (ratio < 0.80)  →  throttle 12h  (tuning phase)
  gap ≤ 10pp  (ratio ≥ 0.80)  →  throttle 24h  (original cadence)

Always runs on workflow_dispatch (bypass flag via --force).

Usage:
  python3 scripts/bench/should_run_benchmark.py \
    [--results-dir benchmarks/results] \
    [--north-star-file configs/perf_north_star] \
    [--force]
"""

import sys
import json
import os
import argparse
import statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta

VALID_WORKLOADS = {"A", "B", "C", "D", "F"}

# (min_gap_exclusive, throttle_hours, label)
TIERS = [
    (0.50, 3,  "crisis  — gap >50pp"),
    (0.25, 6,  "sprint  — gap 25-50pp"),
    (0.10, 12, "tuning  — gap 10-25pp"),
    (0.00, 24, "nominal — gap ≤10pp"),
]


def load_north_star(path: str) -> float:
    with open(path) as f:
        return float(f.read().strip())


def load_results(results_dir: str, max_files: int = 5) -> list:
    p = Path(results_dir)
    if not p.exists():
        return []
    files = sorted(p.glob("*.jsonl"), reverse=True)[:max_files]
    rows = []
    for f in files:
        with open(f) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return rows


def compute_median_ratio(rows: list, last_n_dates: int = 3) -> float | None:
    dates = sorted({r["date"] for r in rows}, reverse=True)[:last_n_dates]
    all_ratios = []
    for date in dates:
        date_rows = [r for r in rows if r["date"] == date]
        by_key: dict = {}
        for r in date_rows:
            wl = r.get("workload", "")
            if wl not in VALID_WORKLOADS:
                continue
            key = f"{wl}-{r.get('threads', 0)}"
            by_key.setdefault(key, {})[r.get("target", "")] = r.get("ops_per_sec", 0)
        for targets in by_key.values():
            sb = targets.get("salvobase", 0)
            mg = targets.get("mongodb", 0)
            if mg > 0 and sb > 0:
                all_ratios.append(sb / mg)
    return statistics.median(all_ratios) if all_ratios else None


def latest_result_time(results_dir: str) -> datetime | None:
    """Return the datetime of the most recent JSONL file, inferred from filename."""
    p = Path(results_dir)
    if not p.exists():
        return None
    files = sorted(p.glob("*.jsonl"), reverse=True)
    if not files:
        return None
    # Filename is YYYY-MM-DD.jsonl — treat as midnight UTC of that date.
    try:
        date_str = files[0].stem  # e.g. "2026-03-19"
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def set_output(key: str, value: str):
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"{key}={value}\n")
    else:
        print(f"OUTPUT: {key}={value}")


def main():
    p = argparse.ArgumentParser(description="Adaptive benchmark gate")
    p.add_argument("--results-dir", default="benchmarks/results")
    p.add_argument("--north-star-file", default="configs/perf_north_star")
    p.add_argument("--force", action="store_true",
                   help="Always output should_run=true (for workflow_dispatch)")
    args = p.parse_args()

    if args.force:
        print("Gate: --force flag set (workflow_dispatch) — running unconditionally.")
        set_output("should_run", "true")
        sys.exit(0)

    north_star = load_north_star(args.north_star_file)
    rows = load_results(args.results_dir)

    if not rows:
        print("Gate: no historical results found — running to establish baseline.")
        set_output("should_run", "true")
        sys.exit(0)

    median_ratio = compute_median_ratio(rows)
    if median_ratio is None:
        print("Gate: could not compute ratio — running to gather data.")
        set_output("should_run", "true")
        sys.exit(0)

    gap = north_star - median_ratio

    # Determine throttle from tier table
    throttle_hours = 24  # default
    tier_label = "nominal"
    for min_gap, hours, label in TIERS:
        if gap > min_gap:
            throttle_hours = hours
            tier_label = label
            break

    # Check time since last run
    last_run = latest_result_time(args.results_dir)
    now = datetime.now(timezone.utc)

    if last_run is None:
        print("Gate: no last-run timestamp — running.")
        set_output("should_run", "true")
        sys.exit(0)

    hours_since = (now - last_run).total_seconds() / 3600
    should_run = hours_since >= throttle_hours

    print(f"Gate summary:")
    print(f"  North star:    {north_star*100:.0f}%")
    print(f"  Current ratio: {median_ratio*100:.1f}%")
    print(f"  Gap:           {gap*100:.1f}pp")
    print(f"  Tier:          {tier_label}")
    print(f"  Throttle:      {throttle_hours}h")
    print(f"  Last run:      {last_run.strftime('%Y-%m-%d')} ({hours_since:.1f}h ago)")
    print(f"  Decision:      {'RUN ✅' if should_run else f'SKIP ⏭ (next run in ~{throttle_hours - hours_since:.0f}h)'}")

    set_output("should_run", "true" if should_run else "false")


if __name__ == "__main__":
    main()
