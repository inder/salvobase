#!/usr/bin/env python3
"""
parse_ycsb.py — Parse go-ycsb stdout and emit a single JSONL result line.

Expected input format (from go-ycsb run):
  Run finished, takes 12.345s
  READ   - Takes(s): 12.3, Count: 50000, OPS: 4065.0, Avg(us): 2461, 50th(us): 2100, 99th(us): 8700
  UPDATE - Takes(s): 12.3, Count: 50000, OPS: 4067.0, Avg(us): 2459, 50th(us): 2200, 99th(us): 8900

Output JSON:
  {"workload":"A","target":"salvobase","threads":4,"ops_per_sec":8132,"p50_ms":2.1,"p99_ms":8.7,...}
"""

import sys
import re
import json
import argparse


def parse_args():
    p = argparse.ArgumentParser(description="Parse go-ycsb output into JSONL")
    p.add_argument("--target", default="salvobase", help="Target DB label")
    p.add_argument("--workload", default="A", help="YCSB workload letter")
    p.add_argument("--threads", type=int, default=4, help="Thread count")
    p.add_argument("--commit", default="", help="Git commit SHA (short)")
    p.add_argument("--date", default="", help="Date string YYYY-MM-DD")
    return p.parse_args()


# Matches lines like (go-ycsb includes extra percentile/min/max fields that vary by version):
#   READ   - Takes(s): 12.3, Count: 50000, OPS: 4065.0, Avg(us): 2461, Min(us): 100,
#            Max(us): 9000, 50th(us): 2100, 90th(us): 5000, 95th(us): 7000, 99th(us): 8700, ...
# Strategy: match the prefix loosely, then extract named fields anywhere in the rest of the line.
OP_LINE_PREFIX_RE = re.compile(
    r"^(?P<op>\w+)\s+-\s+"
    r"Takes\(s\):\s*(?P<takes>[\d.]+).*?"
    r"OPS:\s*(?P<ops>[\d.]+)"
)
FIELD_RE = {
    "p50_us": re.compile(r"(?<!\d)50th\(us\):\s*([\d.]+)"),
    "p99_us": re.compile(r"(?<!\d)99th\(us\):\s*([\d.]+)"),
}

FINISHED_RE = re.compile(r"Run finished, takes\s*([\d.]+)s")


def main():
    args = parse_args()

    ops_list = []
    p50_us_list = []
    p99_us_list = []
    total_duration_s = None

    for line in sys.stdin:
        line = line.strip()

        m = FINISHED_RE.search(line)
        if m:
            total_duration_s = float(m.group(1))
            continue

        m = OP_LINE_PREFIX_RE.match(line)
        if m:
            # Skip error/total pseudo-ops (READ_ERROR, TOTAL, CLEANUP, etc.)
            op = m.group("op")
            if "_" in op or op == "TOTAL" or op == "CLEANUP":
                continue
            p50_m = FIELD_RE["p50_us"].search(line)
            p99_m = FIELD_RE["p99_us"].search(line)
            if p50_m and p99_m:
                ops_list.append(float(m.group("ops")))
                p50_us_list.append(float(p50_m.group(1)))
                p99_us_list.append(float(p99_m.group(1)))

    if not ops_list:
        sys.stderr.write("parse_ycsb: no op lines found in input\n")
        sys.exit(1)

    total_ops = sum(ops_list)
    # Average p50 across all op types, worst-case (max) p99
    avg_p50_ms = (sum(p50_us_list) / len(p50_us_list)) / 1000.0
    max_p99_ms = max(p99_us_list) / 1000.0

    result = {
        "workload": args.workload.upper(),
        "target": args.target,
        "threads": args.threads,
        "ops_per_sec": round(total_ops, 2),
        "p50_ms": round(avg_p50_ms, 3),
        "p99_ms": round(max_p99_ms, 3),
    }

    if args.commit:
        result["commit"] = args.commit
    if args.date:
        result["date"] = args.date
    if total_duration_s is not None:
        result["duration_s"] = round(total_duration_s, 3)

    print(json.dumps(result))


if __name__ == "__main__":
    main()
