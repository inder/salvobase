#!/usr/bin/env python3
"""
file_failure_issue.py — Create a GitHub issue when the nightly benchmark fails.

Called by .github/workflows/benchmark.yml on workflow failure.
Reads context from environment variables so the workflow YAML stays simple.

Required env vars:
  GITHUB_REPOSITORY   e.g. "inder/salvobase"
  GH_TOKEN            GitHub token with issues:write
  RUN_URL             URL to the failed Actions run
  COMMIT_SHA          Full commit SHA
  COMMIT_URL          URL to the commit
"""

import os
import subprocess
import sys
from datetime import datetime, timezone


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "inder/salvobase")
    run_url = os.environ.get("RUN_URL", "")
    commit_sha = os.environ.get("COMMIT_SHA", "")[:7]
    commit_url = os.environ.get("COMMIT_URL", "")

    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    body = f"""\
## Nightly Benchmark Failed

The nightly benchmark workflow failed on **{date_str}**.

**Commit:** `{commit_sha}` — {commit_url}
**Run:** {run_url}

---

### How to investigate

1. Open the run URL above and check which step failed.
2. Common failure modes:
   - **Build salvobase** — compilation error introduced since last run
   - **Wait for services** — Docker/Salvobase failed to start; check service logs in the run
   - **Install go-ycsb** — upstream network issue; usually transient, re-run the workflow
   - **Run benchmarks** — parse errors in individual workloads; check `=== stderr ===` blocks in the run log
   - **Print ratio summary** — all workloads failed to parse (zero results); check run benchmarks step
   - **Commit results** — git/push issue with bench-data branch

3. Re-trigger the workflow once the issue is fixed:
   ```
   gh workflow run benchmark.yml --repo {repo}
   ```

### Context

- Benchmark script: `scripts/bench/run_ycsb.sh`
- Parser: `scripts/bench/parse_ycsb.py`
- Workflow: `.github/workflows/benchmark.yml`
- Results branch: `bench-data`

*Filed automatically by the Nightly Benchmark workflow.*"""

    result = subprocess.run(
        [
            "gh", "issue", "create",
            "--repo", repo,
            "--title", f"bug: nightly benchmark failed ({date_str})",
            "--label", "bug,agent:available,priority:high,area:testing,trust:newcomer-ok",
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: gh issue create failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Filed benchmark failure issue: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
