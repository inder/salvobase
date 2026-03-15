#!/usr/bin/env python3
"""
file_failure_issue.py — Create a GitHub issue when the headless founder cycle fails.

Called by .github/workflows/founder-scheduled.yml on workflow failure.
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
## Headless Founder Agent Failed

The scheduled founder cycle failed on **{date_str}**.

**Commit:** `{commit_sha}` — {commit_url}
**Run:** {run_url}

---

### How to investigate

1. Open the run URL above and check which step failed.
2. Common failure modes:
   - **Install Claude Code** — npm registry issue or version yanked; update the pinned version in the workflow
   - **Run founder cycle** — Claude returned a non-zero exit; check the log for the last tool call before failure
   - **ANTHROPIC_API_KEY** — key expired, revoked, or hit quota limit; rotate at console.anthropic.com
   - **FOUNDER_TOKEN** — PAT expired (90-day default); regenerate at github.com/settings/tokens

3. Run the founder manually to verify it's working:
   ```
   gh workflow run founder-scheduled.yml --repo {repo}
   ```

4. Or run locally:
   ```
   /founder
   ```

### Context

- Workflow: `.github/workflows/founder-scheduled.yml`
- Prompt: `.github/founder-prompt.md`
- Runs every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)

*Filed automatically by the Founder Agent workflow.*"""

    result = subprocess.run(
        [
            "gh", "issue", "create",
            "--repo", repo,
            "--title", f"bug: headless founder cycle failed ({date_str})",
            "--label", "bug,priority:high,area:testing",
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: gh issue create failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Filed founder failure issue: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
