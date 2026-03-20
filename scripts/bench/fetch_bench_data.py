#!/usr/bin/env python3
"""
fetch_bench_data.py — Pull recent JSONL results from the bench-data branch.

Called by the adaptive gate job before running should_run_benchmark.py.
Reads benchmarks/index.json (already fetched via git show) and pulls the
last 5 dates' JSONL files into benchmarks/results/.
"""
import json
import subprocess
import os
from pathlib import Path

try:
    idx = json.loads(open("benchmarks/index.json").read())
    dates = sorted(idx.get("dates", []), reverse=True)[:5]
except Exception:
    dates = []

os.makedirs("benchmarks/results", exist_ok=True)
for date in dates:
    fname = f"benchmarks/results/{date}.jsonl"
    result = subprocess.run(
        ["git", "show", f"bench-data:{fname}"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        Path(fname).write_text(result.stdout)
