#!/usr/bin/env python3
"""
check_regression.py — CI regression gate for the Salvobase compatibility matrix.

Usage:
    python3 tools/compat/check_regression.py <base_report.json> <current_report.json>

Reads two CompatReport JSON files produced by tools/compat/main.go.

- Prints any items that regressed from 'pass' → 'fail' (or 'pass' → 'partial').
- Prints improvements (fail/partial → pass) as informational.
- Exits with code 1 if any regressions are found, 0 otherwise.
"""

import json
import sys


def load_report(path: str) -> dict[str, str]:
    """Load a compat report and return {name: status} dict."""
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[warn] {path} not found — treating as empty baseline", file=sys.stderr)
        return {}
    except json.JSONDecodeError as e:
        print(f"[error] Failed to parse {path}: {e}", file=sys.stderr)
        sys.exit(2)

    results = data.get("results", [])
    return {r["name"]: r["status"] for r in results}


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <base_report.json> <current_report.json}", file=sys.stderr)
        sys.exit(2)

    base_path, current_path = sys.argv[1], sys.argv[2]
    base = load_report(base_path)
    current = load_report(current_path)

    regressions: list[tuple[str, str, str]] = []
    improvements: list[tuple[str, str, str]] = []
    new_items: list[tuple[str, str]] = []

    for name, cur_status in current.items():
        base_status = base.get(name)
        if base_status is None:
            new_items.append((name, cur_status))
            continue
        if base_status == "pass" and cur_status in ("fail", "partial"):
            regressions.append((name, base_status, cur_status))
        elif base_status in ("fail", "partial") and cur_status == "pass":
            improvements.append((name, base_status, cur_status))

    # Items in base but not in current (removed probes — informational only).
    removed: list[str] = [n for n in base if n not in current]

    # ── Print report ──────────────────────────────────────────────────────────
    print("=" * 60)
    print("Compatibility Matrix Regression Check")
    print("=" * 60)
    print(f"Base:    {base_path}  ({len(base)} probes)")
    print(f"Current: {current_path}  ({len(current)} probes)")
    print()

    if improvements:
        print(f"✅ Improvements ({len(improvements)}):")
        for name, old, new in improvements:
            print(f"   {name}  {old} → {new}")
        print()

    if new_items:
        print(f"🆕 New probes ({len(new_items)}):")
        for name, status in new_items:
            icon = {"pass": "✅", "fail": "❌", "partial": "⚠️"}.get(status, "?")
            print(f"   {icon} {name}  ({status})")
        print()

    if removed:
        print(f"🗑️  Removed probes ({len(removed)}):")
        for name in removed:
            print(f"   {name}")
        print()

    if regressions:
        print(f"❌ REGRESSIONS ({len(regressions)}) — CI FAIL:")
        for name, old, new in regressions:
            print(f"   {name}  {old} → {new}")
        print()
        print("ERROR: Compatibility regressions detected. Fix them before merging.")
        sys.exit(1)
    else:
        print("✅ No regressions. All clear.")
        sys.exit(0)


if __name__ == "__main__":
    main()
