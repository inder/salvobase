#!/usr/bin/env python3
"""
file_failure_issue.py — Create a GitHub issue when the introspector cycle fails.

Called by .github/workflows/introspector.yml on workflow failure.
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


def has_open_failure_issue(repo: str, search_term: str) -> bool:
    """Check if there's already an open failure issue matching search_term."""
    try:
        result = subprocess.run(
            [
                "gh", "issue", "list",
                "--repo", repo,
                "--state", "open",
                "--label", "bug",
                "--search", search_term,
                "--json", "number",
                "--jq", "length",
            ],
            capture_output=True,
            text=True,
        )
        return int(result.stdout.strip()) > 0
    except (OSError, ValueError, AttributeError):
        return False


def main() -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "inder/salvobase")
    run_url = os.environ.get("RUN_URL", "")
    commit_sha = os.environ.get("COMMIT_SHA", "")[:7]
    commit_url = os.environ.get("COMMIT_URL", "")

    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    if has_open_failure_issue(repo, "headless introspector cycle failed"):
        print(f"Open failure issue already exists for {repo} — skipping duplicate.")
        return

    body = f"""\
## Headless Introspector Agent Failed

The weekly introspector cycle failed on **{date_str}**.

**Commit:** `{commit_sha}` — {commit_url}
**Run:** {run_url}

---

### How to investigate

1. Open the run URL above and check which step failed.
2. Common failure modes:
   - **Install Claude Code** — npm registry issue or version yanked; update the pinned version in the workflow
   - **Run introspector cycle** — Claude returned a non-zero exit; check the log for the last tool call before failure
   - **ANTHROPIC_API_KEY** — key expired, revoked, or hit quota limit; rotate at console.anthropic.com
   - **gh auth** — GITHUB_TOKEN scope issue (introspector uses GITHUB_TOKEN, not FOUNDER_TOKEN)

3. Run the introspector manually to verify it's working:
   ```
   gh workflow run introspector.yml --repo {repo}
   ```

### Context

- Workflow: `.github/workflows/introspector.yml`
- Prompt: `.github/introspector-prompt.md`
- Runs weekly (Monday 09:00 UTC)
- Read-only agent — does not need FOUNDER_TOKEN

*Filed automatically by the Introspector workflow.*"""

    result = subprocess.run(
        [
            "gh", "issue", "create",
            "--repo", repo,
            "--title", f"bug: headless introspector cycle failed ({date_str})",
            "--label", "bug,priority:high,area:testing",
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: gh issue create failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Filed introspector failure issue: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
