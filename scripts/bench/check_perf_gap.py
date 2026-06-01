#!/usr/bin/env python3
"""
check_perf_gap.py — Evaluate Salvobase vs MongoDB performance gap against the north star.

Runs after every nightly benchmark. Three conditions:

  Condition 0: gap > 0.10  → upsert priority:critical issue (every run — daily pressure)
  Condition 1: 0 < gap ≤ 0.10  → file/update issue every 3 days
  Condition 2: gap ≤ 0  → north star achieved → file achievement issue, close p0

gap = north_star - worst_workload_3_night_median_ratio  (see #737)

Per-workload gate (see #737):
  Overall median masks bad workloads when others beat MongoDB. Routing uses the
  worst per-workload median so the p0 issue fires whenever ANY workload is more
  than 10pp behind. Overall median is still printed/displayed for context.

Per-night sampling (see #715):
  benchmark.yml emits 3 result rows per (workload, threads, target) per night.
  This script aggregates samples → per-night median first, then computes the
  per-night ratio. Condition 0 is variance-gated: if the current 3-night
  overall median is within 2σ of the trailing 7-night median, the alert is
  suppressed.

Usage:
  python3 scripts/bench/check_perf_gap.py \
    [--results-dir benchmarks/results] \
    [--north-star-file configs/perf_north_star] \
    [--dry-run]
"""

import sys
import json
import argparse
import subprocess
import statistics
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone, timedelta

REPO = "inder/salvobase"

# Workload E excluded — scan not yet implemented in Salvobase
VALID_WORKLOADS = {"A", "B", "C", "D", "F"}

LABEL_P0 = "priority:critical,area:performance,area:benchmark,agent:available,complexity:m,trust:contributor+"
LABEL_C1 = "priority:medium,area:performance,area:benchmark,agent:available,complexity:s,trust:newcomer-ok"
LABEL_ACHIEVED = "area:performance,area:benchmark,trust:maintainer-only"

# Variance gate parameters. See `memory/project_bench_noise_floor.md` and #715.
CURRENT_WINDOW_NIGHTS = 3
TRAILING_WINDOW_NIGHTS = 7
SIGMA_THRESHOLD = 2.0
# Min trailing nights below which the gate bypasses (insufficient history).
MIN_TRAILING_FOR_GATE = 3


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_north_star(path: str) -> float:
    with open(path) as f:
        val = f.read().strip()
    return float(val)


def load_results(results_dir: str, max_files: int = 14) -> list:
    """Load JSONL files from the last max_files days, newest first.

    Need at least CURRENT_WINDOW_NIGHTS + TRAILING_WINDOW_NIGHTS nights to gate;
    pad by 4 to absorb gaps where a nightly run was skipped or had no valid pairs.
    """
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


def compute_per_night(rows: list) -> tuple[dict, dict]:
    """
    Aggregate raw sample rows into per-night ratios.

    Returns (night_overall, night_per_wl):
      night_overall: dict[date_str -> overall_ratio]   (median across all wl/thread ratios)
      night_per_wl:  dict[date_str -> dict[wl -> per-workload median ratio]]

    Each per-night value is built by:
      1. median ops_per_sec across samples per (workload, threads, target)
      2. ratio = salvobase / mongodb per (workload, threads)
      3. per_wl[wl] = median of all per-thread ratios for that workload
      4. overall = median of all (workload, threads) ratios
    """
    # date -> (wl, threads, target) -> [ops_per_sec, ...]
    grouped: dict = defaultdict(lambda: defaultdict(list))
    for r in rows:
        wl = r.get("workload", "")
        if wl not in VALID_WORKLOADS:
            continue
        date = r.get("date", "")
        if not date:
            continue
        key = (wl, r.get("threads", 0), r.get("target", ""))
        ops = r.get("ops_per_sec", 0)
        if ops and ops > 0:
            grouped[date][key].append(ops)

    night_overall: dict = {}
    night_per_wl: dict = {}

    for date, key_to_samples in grouped.items():
        # Median per (wl, threads, target) across samples
        agg = {k: statistics.median(v) for k, v in key_to_samples.items() if v}

        # Pivot: (wl, threads) -> {target: ops}
        by_wt: dict = defaultdict(dict)
        for (wl, threads, target), ops in agg.items():
            by_wt[(wl, threads)][target] = ops

        # ratio per (wl, threads)
        ratios_by_wl: dict = defaultdict(list)
        for (wl, threads), targets in by_wt.items():
            sb = targets.get("salvobase", 0)
            mg = targets.get("mongodb", 0)
            if sb > 0 and mg > 0:
                ratios_by_wl[wl].append(sb / mg)

        if not ratios_by_wl:
            continue

        per_wl = {wl: statistics.median(rs) for wl, rs in ratios_by_wl.items()}
        all_ratios = [r for rs in ratios_by_wl.values() for r in rs]
        night_per_wl[date] = per_wl
        night_overall[date] = statistics.median(all_ratios)

    return night_overall, night_per_wl


def compute_median_ratio(
    rows: list, last_n_dates: int = CURRENT_WINDOW_NIGHTS
) -> tuple:
    """
    Returns (overall_median_ratio, per_workload_median_dict) over the last N
    unique dates. Aggregates samples → per-night first to neutralize noise.
    """
    night_overall, night_per_wl = compute_per_night(rows)
    dates = sorted(night_overall.keys(), reverse=True)[:last_n_dates]
    if not dates:
        return None, {}

    overall = statistics.median([night_overall[d] for d in dates])

    per_wl_collected: dict = defaultdict(list)
    for d in dates:
        for wl, ratio in night_per_wl[d].items():
            per_wl_collected[wl].append(ratio)
    per_wl = {wl: statistics.median(rs) for wl, rs in per_wl_collected.items()}
    return overall, per_wl


def determine_routing(
    median_ratio: float | None,
    per_wl: dict[str, float],
    north_star: float,
) -> tuple[int, float, float, str | None]:
    """
    Decide which condition to route to (#737).

    Returns (condition_idx, gap, worst_ratio, worst_wl).

      condition_idx: 0 (gap > 10pp), 1 (0 < gap ≤ 10pp), 2 (gap ≤ 0)
      gap         : north_star - worst_ratio
      worst_ratio : min(per_wl.values()) — the binding constraint
      worst_wl    : workload name with the minimum ratio (None if per_wl empty)

    Falls back to median_ratio when per_wl is empty (defensive — should only
    happen when paired salvobase/mongodb data is missing).
    """
    if per_wl:
        worst_wl = min(per_wl, key=per_wl.get)
        worst_ratio = per_wl[worst_wl]
    else:
        worst_ratio = median_ratio if median_ratio is not None else 0.0
        worst_wl = None

    gap = north_star - worst_ratio
    if gap > 0.10:
        cond = 0
    elif gap > 0:
        cond = 1
    else:
        cond = 2
    return cond, gap, worst_ratio, worst_wl


def variance_gate(
    rows: list,
    current_n: int = CURRENT_WINDOW_NIGHTS,
    trailing_n: int = TRAILING_WINDOW_NIGHTS,
    sigma: float = SIGMA_THRESHOLD,
) -> tuple[bool, str]:
    """
    Decide whether a Condition 0 alert should fire given the history.

    Returns (should_alert, human-readable reason).

    Logic:
      - Need at least `current_n` nights — otherwise we can't even compute current.
      - Need at least MIN_TRAILING_FOR_GATE prior nights — otherwise we don't
        trust the stddev estimate; bypass gate (alert proceeds).
      - Otherwise: alert only if current median is more than `sigma`·stddev
        BELOW the trailing median. Sideways and upward moves are silenced
        (upward "regressions" mean the gap closed — not something to alert on,
        and Conditions 1/2 will pick them up via the gap calculation upstream).

    Uses `statistics.pstdev` (population stddev) because the trailing window IS
    the population we care about — we are not sampling from a larger distribution.
    """
    night_overall, _ = compute_per_night(rows)
    dates = sorted(night_overall.keys(), reverse=True)

    if len(dates) < current_n:
        return True, (
            f"only {len(dates)} night(s) of data; need {current_n} to compute "
            f"current median — bypassing gate"
        )

    current_dates = dates[:current_n]
    trailing_dates = dates[current_n:current_n + trailing_n]

    if len(trailing_dates) < MIN_TRAILING_FOR_GATE:
        return True, (
            f"trailing window has only {len(trailing_dates)} night(s) "
            f"(< {MIN_TRAILING_FOR_GATE}) — bypassing gate, alert proceeds"
        )

    current_vals = [night_overall[d] for d in current_dates]
    trailing_vals = [night_overall[d] for d in trailing_dates]

    current_med = statistics.median(current_vals)
    trailing_med = statistics.median(trailing_vals)
    trailing_std = statistics.pstdev(trailing_vals)

    # Degenerate case: zero stddev → trailing window is identical → any drop is "significant"
    if trailing_std == 0:
        if current_med < trailing_med:
            return True, (
                f"trailing 7-night median {trailing_med*100:.1f}% (σ=0); "
                f"current 3-night median {current_med*100:.1f}% is below — alert proceeds"
            )
        return False, (
            f"trailing 7-night median {trailing_med*100:.1f}% (σ=0); "
            f"current 3-night median {current_med*100:.1f}% — no drop, no alert"
        )

    threshold = trailing_med - sigma * trailing_std
    sigmas_below = (trailing_med - current_med) / trailing_std

    if current_med < threshold:
        return True, (
            f"current 3-night median {current_med*100:.1f}% is "
            f"{sigmas_below:.2f}σ below trailing 7-night median "
            f"{trailing_med*100:.1f}% (σ={trailing_std*100:.1f}pp) — significant regression"
        )

    return False, (
        f"current 3-night median {current_med*100:.1f}% is "
        f"{sigmas_below:+.2f}σ of trailing 7-night median "
        f"{trailing_med*100:.1f}% (σ={trailing_std*100:.1f}pp) — "
        f"within {sigma}σ noise floor, no significant change"
    )


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


def p0_body(
    median_ratio: float,
    north_star: float,
    per_wl: dict[str, float],
    worst_wl: str | None = None,
    worst_ratio: float | None = None,
) -> tuple:
    """Render the p0 issue title + body.

    worst_wl / worst_ratio default to the routing decision from
    determine_routing — pass them explicitly to keep title text in sync with
    the gate that fired. If both are None, fall back to computing from per_wl,
    or to overall-median wording when per_wl is empty.
    """
    overall_pct = median_ratio * 100
    target_pct = north_star * 100
    if worst_wl is None or worst_ratio is None:
        if per_wl:
            worst_wl = min(per_wl, key=per_wl.get)
            worst_ratio = per_wl[worst_wl]
        else:
            worst_wl = None
            worst_ratio = median_ratio
    worst_pct = worst_ratio * 100
    gap_pp = target_pct - worst_pct
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    headline = (f"worst workload {worst_wl} at {worst_pct:.1f}%"
                if worst_wl else f"overall median {worst_pct:.1f}%")

    title = (f"perf: close gap to north star — "
             f"{headline} of {target_pct:.0f}% target")
    body = f"""## Performance Gap — Priority 0

**Signal:** {('Workload **' + worst_wl + '**') if worst_wl else 'Overall median'} \
is at **{worst_pct:.1f}%** of MongoDB throughput \
(overall median across workloads: {overall_pct:.1f}%). \
North star: **{target_pct:.0f}%**. Worst-workload gap: **{gap_pp:.1f} percentage points**.

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
*Filed by `check_perf_gap.py` (nightly benchmark). Updates every run while gap > 10pp \
AND the move clears the 2σ noise-floor gate. See #715 for variance-gating logic.*
*Last updated: {now}*
"""
    return title, body


def c1_body(
    median_ratio: float,
    north_star: float,
    per_wl: dict[str, float],
    worst_wl: str | None = None,
    worst_ratio: float | None = None,
) -> tuple:
    overall_pct = median_ratio * 100
    target_pct = north_star * 100
    if worst_wl is None or worst_ratio is None:
        if per_wl:
            worst_wl = min(per_wl, key=per_wl.get)
            worst_ratio = per_wl[worst_wl]
        else:
            worst_wl = None
            worst_ratio = median_ratio
    worst_pct = worst_ratio * 100
    gap_pp = target_pct - worst_pct
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    headline = (f"worst workload {worst_wl} at {worst_pct:.1f}%"
                if worst_wl else f"overall median {worst_pct:.1f}%")
    signal_label = (f"Worst workload **{worst_wl}** at **{worst_pct:.1f}%**"
                    if worst_wl else f"Overall median at **{worst_pct:.1f}%**")

    title = (f"perf: approaching north star — "
             f"{headline} of {target_pct:.0f}% target")
    wl_lines = "\n".join(f"- Workload {wl}: {v*100:.1f}%"
                         for wl, v in sorted(per_wl.items()))
    body = f"""## Performance Within 10pp of North Star

**Signal:** {signal_label} of MongoDB throughput \
(overall median across workloads: {overall_pct:.1f}%). \
North star: **{target_pct:.0f}%**. Worst-workload gap: **{gap_pp:.1f}pp** — within striking distance.

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


def achieved_body(median_ratio: float, north_star: float, per_wl: dict | None = None) -> tuple:
    current_pct = median_ratio * 100
    target_pct = north_star * 100
    next_target = north_star + 0.05
    if per_wl:
        worst_wl = min(per_wl, key=per_wl.get)
        worst_pct = per_wl[worst_wl] * 100
        worst_line = (f"\n\nWorst workload: **{worst_wl}** at **{worst_pct:.1f}%** — "
                      f"every workload now meets the bar.")
    else:
        worst_line = ""

    title = f"perf: north star achieved — {current_pct:.1f}% ≥ {target_pct:.0f}% target"
    body = f"""## North Star Achieved

Salvobase has reached **{current_pct:.1f}%** of MongoDB throughput, \
meeting the **{target_pct:.0f}%** north star.{worst_line}

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

def handle_condition_0(
    median_ratio: float,
    north_star: float,
    per_wl: dict,
    rows: list,
    dry_run: bool,
    worst_wl: str | None = None,
    worst_ratio: float | None = None,
):
    """Gap > 10pp — upsert p0 issue every run, gated on variance."""
    should_alert, reason = variance_gate(rows)
    print(f"  variance gate: {'ALERT' if should_alert else 'SILENCE'} — {reason}")
    if not should_alert:
        # Don't file, don't comment, don't update. Just trace and exit clean.
        return

    title, body = p0_body(median_ratio, north_star, per_wl, worst_wl, worst_ratio)
    existing = find_open_issue("close gap to north star", "priority:critical")

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} p0 issue: {title}")
        return

    if existing:
        wpct = (worst_ratio if worst_ratio is not None else median_ratio) * 100
        wname = worst_wl if worst_wl else "overall"
        gap_pp = north_star * 100 - wpct
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Worst {wname} at {wpct:.1f}% · Gap {gap_pp:.1f}pp · "
            f"Overall median {median_ratio*100:.1f}% · "
            f"Gate: {reason}"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_P0, body)


def handle_condition_1(
    median_ratio: float,
    north_star: float,
    per_wl: dict,
    dry_run: bool,
    worst_wl: str | None = None,
    worst_ratio: float | None = None,
):
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

    title, body = c1_body(median_ratio, north_star, per_wl, worst_wl, worst_ratio)
    existing = find_open_issue("approaching north star", "priority:medium")

    if existing and updated_within(existing, 3):
        print(f"  Condition 1: issue #{existing['number']} updated within 3 days — skipping")
        return

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} condition-1 issue")
        return

    if existing:
        wpct = (worst_ratio if worst_ratio is not None else median_ratio) * 100
        wname = worst_wl if worst_wl else "overall"
        gap_pp = north_star * 100 - wpct
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Worst {wname} at {wpct:.1f}% · Gap {gap_pp:.1f}pp · "
            f"Overall median {median_ratio*100:.1f}%"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_C1, body)


def handle_condition_2(
    median_ratio: float,
    north_star: float,
    dry_run: bool,
    per_wl: dict | None = None,
):
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

    title, body = achieved_body(median_ratio, north_star, per_wl)
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

    cond, gap, worst_ratio, worst_wl = determine_routing(median_ratio, per_wl, north_star)

    print(f"North star:    {north_star*100:.0f}%")
    print(f"Overall median:{median_ratio*100:6.1f}%")
    print(f"Worst workload:{worst_ratio*100:6.1f}% ({worst_wl})")
    print(f"Worst-wl gap:  {gap*100:6.1f}pp")
    print(f"Per workload:  { {k: f'{v*100:.1f}%' for k, v in sorted(per_wl.items())} }")
    print()

    if cond == 0:
        print(f"→ Condition 0: worst-wl gap > 10pp ({worst_wl}). "
              f"Evaluating variance gate before upserting p0.")
        handle_condition_0(median_ratio, north_star, per_wl, rows, args.dry_run,
                           worst_wl, worst_ratio)
    elif cond == 1:
        print(f"→ Condition 1: Normal — worst-wl gap ≤ 10pp ({worst_wl}). "
              f"Filing/updating every 3 days.")
        handle_condition_1(median_ratio, north_star, per_wl, args.dry_run,
                           worst_wl, worst_ratio)
    else:
        print("→ Condition 2: NORTH STAR ACHIEVED on every workload. "
              "Filing achievement issue.")
        handle_condition_2(median_ratio, north_star, args.dry_run, per_wl)


if __name__ == "__main__":
    main()
