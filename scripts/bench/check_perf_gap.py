#!/usr/bin/env python3
"""
check_perf_gap.py — Evaluate Salvobase vs MongoDB performance gap against the north star.

Runs after every nightly benchmark. Three conditions:

  Condition 0: gap > 0.10  → upsert priority:critical issue (every run — daily pressure)
  Condition 1: 0 < gap ≤ 0.10  → file/update issue every 3 days
  Condition 2: gap ≤ 0  → north star achieved → file achievement issue, close p0

gap = north_star - current_3run_median_ratio

Usage:
  python3 scripts/bench/check_perf_gap.py \
    [--results-dir benchmarks/results] \
    [--north-star-file configs/perf_north_star] \
    [--dry-run]
"""

import sys
import os
import json
import argparse
import subprocess
import statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta

REPO = "inder/salvobase"

# Workload E excluded — scan not yet implemented in Salvobase
VALID_WORKLOADS = {"A", "B", "C", "D", "F"}

LABEL_P0 = "priority:critical,area:performance,area:benchmark,agent:available,complexity:m,trust:contributor+"
LABEL_C1 = "priority:medium,area:performance,area:benchmark,agent:available,complexity:s,trust:newcomer-ok"
LABEL_ACHIEVED = "area:performance,area:benchmark,trust:maintainer-only"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_north_star(path: str) -> float:
    with open(path) as f:
        val = f.read().strip()
    return float(val)


def load_results(results_dir: str, max_files: int = 5) -> list:
    """Load JSONL files from the last max_files days, newest first."""
    p = Path(results_dir)
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


def compute_median_ratio(rows: list, last_n_dates: int = 3) -> tuple:
    """
    Returns (overall_median_ratio, per_workload_median_dict).
    Uses last N unique dates to smooth noise.
    Only workloads in VALID_WORKLOADS are included.
    """
    dates = sorted({r["date"] for r in rows}, reverse=True)[:last_n_dates]

    ratios_by_workload: dict = {}

    for date in dates:
        date_rows = [r for r in rows if r["date"] == date]
        by_key: dict = {}
        for r in date_rows:
            wl = r.get("workload", "")
            if wl not in VALID_WORKLOADS:
                continue
            key = f"{wl}-{r.get('threads', 0)}"
            target = r.get("target", "")
            if key not in by_key:
                by_key[key] = {}
            by_key[key][target] = r.get("ops_per_sec", 0)

        for key, targets in by_key.items():
            sb = targets.get("salvobase", 0)
            mg = targets.get("mongodb", 0)
            if mg > 0 and sb > 0:
                wl = key.split("-")[0]
                ratio = sb / mg
                ratios_by_workload.setdefault(wl, []).append(ratio)

    all_ratios = [r for ratios in ratios_by_workload.values() for r in ratios]
    if not all_ratios:
        return None, {}

    per_wl = {wl: statistics.median(rs) for wl, rs in ratios_by_workload.items()}
    overall = statistics.median(all_ratios)
    return overall, per_wl


# ---------------------------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------------------------

def gh(args: list, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh"] + args, capture_output=True, text=True, check=check)


def find_open_issue(title_fragment: str, first_label: str) -> dict | None:
    """Find the first open issue whose title contains title_fragment."""
    result = gh(
        ["issue", "list", "--repo", REPO, "--state", "open",
         "--label", first_label, "--json", "number,title,updatedAt,body", "--limit", "20"],
        check=False,
    )
    if result.returncode != 0:
        return None
    issues = json.loads(result.stdout or "[]")
    for issue in issues:
        if title_fragment.lower() in issue["title"].lower():
            return issue
    return None


def updated_within(issue: dict, days: int) -> bool:
    updated = datetime.fromisoformat(issue["updatedAt"].replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - updated) < timedelta(days=days)


def create_issue(title: str, labels: str, body: str):
    result = gh(["issue", "create", "--repo", REPO,
                 "--title", title, "--label", labels, "--body", body])
    print(f"  → Created: {result.stdout.strip()}")


def comment_on_issue(number: int, comment: str):
    gh(["issue", "comment", str(number), "--repo", REPO, "--body", comment])
    print(f"  → Commented on #{number}")


def close_issue(number: int, comment: str):
    gh(["issue", "close", str(number), "--repo", REPO, "--comment", comment])
    print(f"  → Closed #{number}")


# ---------------------------------------------------------------------------
# Issue bodies
# ---------------------------------------------------------------------------

def _wl_table(per_wl: dict, north_star: float) -> str:
    rows = []
    for wl in sorted(per_wl):
        ratio = per_wl[wl]
        status = "✅" if ratio >= north_star else "❌"
        rows.append(f"| {wl} | {ratio*100:.1f}% | {status} |")
    return "\n".join(rows)


def p0_body(median_ratio: float, north_star: float, per_wl: dict) -> tuple:
    gap_pp = (north_star - median_ratio) * 100
    current_pct = median_ratio * 100
    target_pct = north_star * 100
    worst_wl = min(per_wl, key=per_wl.get) if per_wl else "N/A"
    worst_pct = per_wl.get(worst_wl, 0) * 100
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    title = (f"perf: close gap to north star — "
             f"currently {current_pct:.1f}% of {target_pct:.0f}% target")
    body = f"""## Performance Gap — Priority 0

**Signal:** Salvobase is at **{current_pct:.1f}%** of MongoDB throughput. \
North star: **{target_pct:.0f}%**. Gap: **{gap_pp:.1f} percentage points**.

### Per-Workload Breakdown (3-run median)

| Workload | Salvobase/MongoDB | Passes? |
|----------|-------------------|---------|
{_wl_table(per_wl, north_star)}

**Biggest gap:** Workload {worst_wl} at {worst_pct:.1f}%

### Where to Look

- **Read-heavy (B/C):** Index lookup path, BSON decode overhead, unnecessary allocations
- **Write-heavy (A/F):** bbolt write amplification, sync frequency, lock contention
- **Mixed (A/D):** Concurrent reader/writer lock contention, batch sizes

### Definition of Done

Close when 3-day median ratio ≥ {target_pct:.0f}% across all workloads A/B/C/D/F.

### How to suppress noise

If this is actively being worked on:
```
/dismiss reason:"in progress" expires:3d
```

---
*Filed by `check_perf_gap.py` (nightly benchmark). Updates every run while gap > 10pp.*
*Last updated: {now}*
"""
    return title, body


def c1_body(median_ratio: float, north_star: float, per_wl: dict) -> tuple:
    gap_pp = (north_star - median_ratio) * 100
    current_pct = median_ratio * 100
    target_pct = north_star * 100
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    title = (f"perf: approaching north star — "
             f"{current_pct:.1f}% of {target_pct:.0f}% target")
    wl_lines = "\n".join(f"- Workload {wl}: {v*100:.1f}%"
                         for wl, v in sorted(per_wl.items()))
    body = f"""## Performance Within 10pp of North Star

**Signal:** Salvobase is at **{current_pct:.1f}%** of MongoDB throughput. \
North star: **{target_pct:.0f}%**. Gap: **{gap_pp:.1f}pp** — within striking distance.

### Per-Workload (3-run median)

{wl_lines}

### Suggested micro-optimizations

- Profile workload B (95% reads) with `go tool pprof` — find the hot path
- Check for unnecessary heap allocations in BSON decode (`go test -benchmem`)
- Verify index scans use bbolt cursor seeks, not full bucket scans

### Definition of Done

Close when 3-day median ≥ {target_pct:.0f}% — north star will then advance to {target_pct+5:.0f}%.

---
*Filed by `check_perf_gap.py`. Updates every 3 days while 0 < gap ≤ 10pp.*
*Last updated: {now}*
"""
    return title, body


def achieved_body(median_ratio: float, north_star: float) -> tuple:
    current_pct = median_ratio * 100
    target_pct = north_star * 100
    next_target = north_star + 0.05

    title = f"perf: north star achieved — {current_pct:.1f}% ≥ {target_pct:.0f}% target"
    body = f"""## North Star Achieved

Salvobase has reached **{current_pct:.1f}%** of MongoDB throughput, \
meeting the **{target_pct:.0f}%** north star.

### Founder action required

1. Bump `configs/perf_north_star` from `{north_star}` → `{next_target:.2f}`
2. Commit and push to master
3. Post announcement to General discussions

This is a `trust:maintainer-only` issue — only the founder agent should close it.

---
*Filed by `check_perf_gap.py` (nightly benchmark).*
"""
    return title, body


# ---------------------------------------------------------------------------
# Condition handlers
# ---------------------------------------------------------------------------

def handle_condition_0(median_ratio: float, north_star: float, per_wl: dict, dry_run: bool):
    """Gap > 10pp — upsert p0 issue every run."""
    title, body = p0_body(median_ratio, north_star, per_wl)
    existing = find_open_issue("close gap to north star", "priority:critical")

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} p0 issue: {title}")
        return

    if existing:
        gap_pp = (north_star - median_ratio) * 100
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Ratio {median_ratio*100:.1f}% · Gap {gap_pp:.1f}pp · "
            f"Worst: {min(per_wl, key=per_wl.get)} at {min(per_wl.values())*100:.1f}%"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_P0, body)


def handle_condition_1(median_ratio: float, north_star: float, per_wl: dict, dry_run: bool):
    """Gap 0–10pp — file/update issue every 3 days."""
    # First, close any lingering p0 issue (we've improved past the 10pp threshold)
    existing_p0 = find_open_issue("close gap to north star", "priority:critical")
    if existing_p0:
        if not dry_run:
            close_issue(existing_p0["number"],
                        "Gap has dropped below 10pp — downgrading to normal priority. "
                        "See the new condition-1 issue.")
        else:
            print(f"[DRY RUN] Would close p0 issue #{existing_p0['number']}")

    title, body = c1_body(median_ratio, north_star, per_wl)
    existing = find_open_issue("approaching north star", "priority:medium")

    if existing and updated_within(existing, 3):
        print(f"  Condition 1: issue #{existing['number']} updated within 3 days — skipping")
        return

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} condition-1 issue")
        return

    if existing:
        gap_pp = (north_star - median_ratio) * 100
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Ratio {median_ratio*100:.1f}% · Gap {gap_pp:.1f}pp"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_C1, body)


def handle_condition_2(median_ratio: float, north_star: float, dry_run: bool):
    """North star hit — file achievement issue, close p0."""
    # Close p0 if open
    existing_p0 = find_open_issue("close gap to north star", "priority:critical")
    if existing_p0:
        if not dry_run:
            close_issue(existing_p0["number"],
                        "North star achieved. Closing this issue.")
        else:
            print(f"[DRY RUN] Would close p0 #{existing_p0['number']}")

    # File achievement issue (only if not already open)
    existing_ach = find_open_issue("north star achieved", "area:performance")
    if existing_ach:
        print(f"  Achievement issue #{existing_ach['number']} already open — skipping")
        return

    title, body = achieved_body(median_ratio, north_star)
    if dry_run:
        print(f"[DRY RUN] Would file north-star-achieved issue: {title}")
        return

    create_issue(title, LABEL_ACHIEVED, body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Check Salvobase perf gap vs north star")
    p.add_argument("--results-dir", default="benchmarks/results",
                   help="Directory containing JSONL result files")
    p.add_argument("--north-star-file", default="configs/perf_north_star",
                   help="File containing north star ratio (e.g. 0.90)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print actions without filing GitHub issues")
    args = p.parse_args()

    north_star = load_north_star(args.north_star_file)
    rows = load_results(args.results_dir)

    if not rows:
        print("check_perf_gap: no result data found — skipping", file=sys.stderr)
        sys.exit(0)

    median_ratio, per_wl = compute_median_ratio(rows)

    if median_ratio is None:
        print("check_perf_gap: no paired salvobase/mongodb results — skipping", file=sys.stderr)
        sys.exit(0)

    gap = north_star - median_ratio

    print(f"North star:   {north_star*100:.0f}%")
    print(f"Median ratio: {median_ratio*100:.1f}%")
    print(f"Gap:          {gap*100:.1f}pp")
    print(f"Per workload: { {k: f'{v*100:.1f}%' for k, v in sorted(per_wl.items())} }")
    print()

    if gap > 0.10:
        print("→ Condition 0: EMERGENCY — gap > 10pp. Upserting p0 issue.")
        handle_condition_0(median_ratio, north_star, per_wl, args.dry_run)
    elif gap > 0:
        print("→ Condition 1: Normal — gap ≤ 10pp. Filing/updating every 3 days.")
        handle_condition_1(median_ratio, north_star, per_wl, args.dry_run)
    else:
        print("→ Condition 2: NORTH STAR ACHIEVED. Filing achievement issue.")
        handle_condition_2(median_ratio, north_star, args.dry_run)


if __name__ == "__main__":
    main()
