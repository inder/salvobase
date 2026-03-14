#!/usr/bin/env python3
"""
update_index.py — Prepend today's date to benchmarks/index.json, keep last 365 unique dates.

Format:
  {"dates": ["2026-03-15", "2026-03-14", ...]}
"""

import json
import os
import sys
from datetime import date

INDEX_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "benchmarks",
    "index.json",
)


def main():
    today = date.today().isoformat()

    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {"dates": []}
    else:
        data = {"dates": []}

    existing = data.get("dates", [])

    # Prepend today, deduplicate preserving order, keep last 365
    seen = set()
    merged = []
    for d in [today] + existing:
        if d not in seen:
            seen.add(d)
            merged.append(d)

    data["dates"] = merged[:365]

    with open(INDEX_PATH, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")

    print(f"update_index: {INDEX_PATH} updated — {len(data['dates'])} dates, latest={data['dates'][0]}", file=sys.stderr)


if __name__ == "__main__":
    main()
