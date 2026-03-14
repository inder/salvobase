#!/usr/bin/env python3
"""
file_regression_issue.py — Create a GitHub issue for a compatibility regression.

Called by .github/workflows/compat.yml when check_regression.py exits 1.
Reads context from environment variables so the workflow YAML stays simple.

Required env vars:
  GITHUB_REPOSITORY   e.g. "inder/salvobase"
  GH_TOKEN            GitHub token with issues:write
  REGRESSION_OUTPUT   Full output from check_regression.py
  COMMIT_SHA          Full commit SHA
  COMMIT_URL          URL to the commit
Optional env vars:
  PR_NUMBER           Pull request number (if triggered from a PR)
  PR_URL              Pull request URL
"""

import os
import subprocess
import sys
from datetime import datetime, timezone


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "inder/salvobase")
    regression_output = os.environ.get("REGRESSION_OUTPUT", "(no output captured)")
    commit_sha = os.environ.get("COMMIT_SHA", "")[:7]
    commit_url = os.environ.get("COMMIT_URL", "")
    pr_number = os.environ.get("PR_NUMBER", "")
    pr_url = os.environ.get("PR_URL", "")

    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    if pr_number:
        context = f"PR #{pr_number}: {pr_url}"
    else:
        context = f"push to master: {commit_url}"

    body = f"""\
## Compatibility Regression Detected

The compatibility matrix CI gate caught a regression on **{date_str}**.

**{context}**
Commit: `{commit_sha}`

---

### Regression output

```
{regression_output}
```

---

### How to fix

1. Read the failing probe name(s) above.
2. Find the relevant handler in `internal/commands/`, `internal/query/`, or `internal/aggregation/`.
3. Fix the implementation so the probe passes.
4. Run locally to verify: `make compat SALVOBASE_URI=mongodb://localhost:27017`
5. Ensure `check_regression.py` exits 0 before submitting a PR.

### Context

- Compatibility matrix: `docs/compatibility.md`
- Probe definitions: `tools/compat/main.go`
- Regression script: `tools/compat/check_regression.py`

*Filed automatically by the Compatibility Matrix workflow.*"""

    result = subprocess.run(
        [
            "gh", "issue", "create",
            "--repo", repo,
            "--title", f"bug: compatibility regression detected ({date_str})",
            "--label", "bug,agent:available,priority:high,area:testing,trust:newcomer-ok",
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: gh issue create failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Filed regression issue: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
