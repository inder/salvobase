#!/usr/bin/env python3
"""
check_compat_gap.py — Evaluate Salvobase MongoDB compatibility against a threshold.

Runs after every compat probe. Three conditions:

  Condition 0: gap > 0.05  → upsert priority:critical issue (every run — daily pressure)
  Condition 1: 0 < gap ≤ 0.05  → file/update issue every 3 days
  Condition 2: gap ≤ 0  → threshold achieved → file achievement issue, close p0

gap = threshold - current_compat_score
score = pass_count / total_count  (partial counts as 0.5)

Usage:
  python3 tools/compat/check_compat_gap.py \
    [--report-file docs/compat_report.json] \
    [--threshold-file configs/compat_threshold] \
    [--dry-run]
"""

import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta

REPO = "inder/salvobase"

LABEL_P0 = "priority:critical,area:compat,agent:available,complexity:m,trust:contributor+"
LABEL_C1 = "priority:medium,area:compat,agent:available,complexity:s,trust:newcomer-ok"
LABEL_ACHIEVED = "area:compat,trust:maintainer-only"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_threshold(path: str) -> float:
    with open(path) as f:
        return float(f.read().strip())


def load_report(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def compute_score(report: dict) -> tuple:
    """
    Returns (overall_score, per_category_dict).
    pass=1.0, partial=0.5, fail=0.0.
    Score = weighted average across all probes.
    """
    results = report.get("results", [])
    if not results:
        return None, {}

    by_category: dict = {}
    for r in results:
        cat = r.get("category", "Other")
        status = r.get("status", "fail")
        score = 1.0 if status == "pass" else (0.5 if status == "partial" else 0.0)
        by_category.setdefault(cat, []).append(score)

    per_cat = {cat: sum(scores) / len(scores) for cat, scores in by_category.items()}
    all_scores = [s for scores in by_category.values() for s in scores]
    overall = sum(all_scores) / len(all_scores)
    return overall, per_cat


# ---------------------------------------------------------------------------
# GitHub helpers (mirrors check_perf_gap.py)
# ---------------------------------------------------------------------------

def gh(args: list, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh"] + args, capture_output=True, text=True, check=check)


def find_open_issue(title_fragment: str, first_label: str) -> dict | None:
    result = gh(
        ["issue", "list", "--repo", REPO, "--state", "open",
         "--label", first_label, "--json", "number,title,updatedAt", "--limit", "20"],
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

def _cat_table(per_cat: dict, threshold: float) -> str:
    rows = []
    for cat in sorted(per_cat):
        score = per_cat[cat]
        status = "✅" if score >= threshold else "❌"
        rows.append(f"| {cat} | {score*100:.1f}% | {status} |")
    return "\n".join(rows)


def p0_body(score: float, threshold: float, per_cat: dict) -> tuple:
    gap_pp = (threshold - score) * 100
    score_pct = score * 100
    thresh_pct = threshold * 100
    worst_cat = min(per_cat, key=per_cat.get) if per_cat else "N/A"
    worst_pct = per_cat.get(worst_cat, 0) * 100
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    title = (f"compat: close gap to threshold — "
             f"currently {score_pct:.1f}% of {thresh_pct:.0f}% target")
    body = f"""## Compatibility Gap — Priority 0

**Signal:** Salvobase compatibility is at **{score_pct:.1f}%**. \
Threshold: **{thresh_pct:.0f}%**. Gap: **{gap_pp:.1f} percentage points**.

### Per-Category Breakdown

| Category | Score | Passes? |
|----------|-------|---------|
{_cat_table(per_cat, threshold)}

**Weakest category:** {worst_cat} at {worst_pct:.1f}%

### Where to Look

- Check `docs/compat_report.json` for the specific failing probes
- Run `cd tools/compat && go run -mod=mod . -uri mongodb://localhost:27017 -outdir ../../docs` locally
- Each failing probe maps to a command, query operator, update operator, or aggregation stage

### Definition of Done

Close when overall compat score ≥ {thresh_pct:.0f}% across all categories.

### How to suppress noise

If this is actively being worked on:
```
/dismiss reason:"in progress" expires:3d
```

---
*Filed by `check_compat_gap.py` (compat probe on every push). Updates every run while gap > 5pp.*
*Last updated: {now}*
"""
    return title, body


def c1_body(score: float, threshold: float, per_cat: dict) -> tuple:
    gap_pp = (threshold - score) * 100
    score_pct = score * 100
    thresh_pct = threshold * 100
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    title = (f"compat: approaching threshold — "
             f"{score_pct:.1f}% of {thresh_pct:.0f}% target")
    cat_lines = "\n".join(f"- {cat}: {v*100:.1f}%"
                          for cat, v in sorted(per_cat.items()))
    body = f"""## Compatibility Within 5pp of Threshold

**Signal:** Salvobase compatibility is at **{score_pct:.1f}%**. \
Threshold: **{thresh_pct:.0f}%**. Gap: **{gap_pp:.1f}pp** — within striking distance.

### Per-Category

{cat_lines}

### Suggested next steps

- Run the compat probe locally and check which probes are `partial` or `fail`
- Each partial/fail entry in `docs/compat_report.json` is a specific operator or command to implement
- Small operators (single-field, no aggregation) are good first PRs

### Definition of Done

Close when overall compat score ≥ {thresh_pct:.0f}% — threshold will then advance to {thresh_pct+2:.0f}%.

---
*Filed by `check_compat_gap.py`. Updates every 3 days while 0 < gap ≤ 5pp.*
*Last updated: {now}*
"""
    return title, body


def achieved_body(score: float, threshold: float) -> tuple:
    score_pct = score * 100
    thresh_pct = threshold * 100
    next_threshold = threshold + 0.02

    title = f"compat: threshold achieved — {score_pct:.1f}% ≥ {thresh_pct:.0f}% target"
    body = f"""## Compatibility Threshold Achieved

Salvobase has reached **{score_pct:.1f}%** compatibility, \
meeting the **{thresh_pct:.0f}%** threshold.

### Founder action required

1. Bump `configs/compat_threshold` from `{threshold}` → `{next_threshold:.2f}`
2. Commit and push to master
3. Post announcement to General discussions

This is a `trust:maintainer-only` issue — only the founder agent should close it.

---
*Filed by `check_compat_gap.py` (compat probe on push).*
"""
    return title, body


# ---------------------------------------------------------------------------
# Condition handlers
# ---------------------------------------------------------------------------

def handle_condition_0(score: float, threshold: float, per_cat: dict, dry_run: bool):
    """Gap > 5pp — upsert p0 issue every run."""
    title, body = p0_body(score, threshold, per_cat)
    existing = find_open_issue("close gap to threshold", "priority:critical")

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} p0 issue: {title}")
        return

    if existing:
        gap_pp = (threshold - score) * 100
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Score {score*100:.1f}% · Gap {gap_pp:.1f}pp · "
            f"Weakest: {min(per_cat, key=per_cat.get)} at {min(per_cat.values())*100:.1f}%"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_P0, body)


def handle_condition_1(score: float, threshold: float, per_cat: dict, dry_run: bool):
    """Gap 0–5pp — file/update issue every 3 days."""
    existing_p0 = find_open_issue("close gap to threshold", "priority:critical")
    if existing_p0:
        if not dry_run:
            close_issue(existing_p0["number"],
                        "Gap has dropped below 5pp — downgrading to normal priority.")
        else:
            print(f"[DRY RUN] Would close p0 issue #{existing_p0['number']}")

    title, body = c1_body(score, threshold, per_cat)
    existing = find_open_issue("approaching threshold", "priority:medium")

    if existing and updated_within(existing, 3):
        print(f"  Condition 1: issue #{existing['number']} updated within 3 days — skipping")
        return

    if dry_run:
        print(f"[DRY RUN] Would {'update' if existing else 'create'} condition-1 issue")
        return

    if existing:
        gap_pp = (threshold - score) * 100
        comment = (
            f"**Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}:** "
            f"Score {score*100:.1f}% · Gap {gap_pp:.1f}pp"
        )
        comment_on_issue(existing["number"], comment)
    else:
        create_issue(title, LABEL_C1, body)


_THRESHOLD_CEILING = 1.0  # Compatibility cannot exceed 100%; stop advancing here.


def handle_condition_2(score: float, threshold: float, dry_run: bool):
    """Threshold hit — file achievement issue (or note ceiling), close p0."""
    existing_p0 = find_open_issue("close gap to threshold", "priority:critical")
    if existing_p0:
        if not dry_run:
            close_issue(existing_p0["number"], "Compatibility threshold achieved. Closing.")
        else:
            print(f"[DRY RUN] Would close p0 #{existing_p0['number']}")

    if threshold >= _THRESHOLD_CEILING:
        print(f"  Threshold is already at the ceiling ({_THRESHOLD_CEILING*100:.0f}%) — "
              "no achievement issue filed and no further bumps needed.")
        return

    existing_ach = find_open_issue("threshold achieved", "area:compat")
    if existing_ach:
        print(f"  Achievement issue #{existing_ach['number']} already open — skipping")
        return

    title, body = achieved_body(score, threshold)
    if dry_run:
        print(f"[DRY RUN] Would file threshold-achieved issue: {title}")
        return

    create_issue(title, LABEL_ACHIEVED, body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Check Salvobase compat gap vs threshold")
    p.add_argument("--report-file", default="docs/compat_report.json",
                   help="Path to compat_report.json from the probe run")
    p.add_argument("--threshold-file", default="configs/compat_threshold",
                   help="File containing threshold ratio (e.g. 0.95)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print actions without filing GitHub issues")
    args = p.parse_args()

    threshold = load_threshold(args.threshold_file)
    report = load_report(args.report_file)
    score, per_cat = compute_score(report)

    if score is None:
        print("check_compat_gap: no probe results found — skipping", file=sys.stderr)
        sys.exit(0)

    gap = threshold - score

    print(f"Threshold:    {threshold*100:.0f}%")
    print(f"Compat score: {score*100:.1f}%")
    print(f"Gap:          {gap*100:.1f}pp")
    print(f"Per category: { {k: f'{v*100:.1f}%' for k, v in sorted(per_cat.items())} }")
    print()

    if gap > 0.05:
        print("→ Condition 0: gap > 5pp. Upserting p0 issue.")
        handle_condition_0(score, threshold, per_cat, args.dry_run)
    elif gap > 0:
        print("→ Condition 1: gap ≤ 5pp. Filing/updating every 3 days.")
        handle_condition_1(score, threshold, per_cat, args.dry_run)
    else:
        print("→ Condition 2: THRESHOLD ACHIEVED. Filing achievement issue.")
        handle_condition_2(score, threshold, args.dry_run)


if __name__ == "__main__":
    main()
