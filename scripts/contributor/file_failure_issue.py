#!/usr/bin/env python3
"""
file_failure_issue.py — Create a GitHub issue when the contributor agent cycle fails.

Called by .github/workflows/contributor.yml on workflow failure.
Files the issue into THIS FORK's repo (GITHUB_REPOSITORY = the fork).

Required env vars:
  GITHUB_REPOSITORY   e.g. "salvobase-contrib-1/salvobase" (the fork)
  GH_TOKEN            CONTRIBUTOR_PAT with issues:write on the fork
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
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    run_url = os.environ.get("RUN_URL", "")
    commit_sha = os.environ.get("COMMIT_SHA", "")[:7]
    commit_url = os.environ.get("COMMIT_URL", "")
    agent_id = os.environ.get("AGENT_ID", "unknown-agent")
    operator = os.environ.get("OPERATOR_HANDLE", "unknown-operator")

    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    if has_open_failure_issue(repo, "contributor agent cycle failed"):
        print(f"Open failure issue already exists for {repo} — skipping duplicate.")
        return

    body = f"""\
## Contributor Agent Cycle Failed

The scheduled contributor cycle for agent **{agent_id}** (operator: @{operator}) \
failed on **{date_str}**.

**Commit:** `{commit_sha}` — {commit_url}
**Run:** {run_url}

---

### How to investigate

1. Open the run URL above and check which step failed.
2. Common failure modes:
   - **Sync fork** — fork is ahead of upstream (conflict) or PAT lacks repo scope
   - **Install Claude Code** — npm registry issue or pinned version yanked
   - **Run contributor cycle** — agent returned non-zero; check the log for the last tool call
   - **ANTHROPIC_API_KEY** — key expired, revoked, or hit quota; rotate at console.anthropic.com
   - **CONTRIBUTOR_PAT** — PAT expired (default 90 days) or lacks `repo` scope; regenerate at github.com/settings/tokens

3. Trigger a manual run to verify fix:
   ```
   gh workflow run contributor.yml --repo {repo}
   ```

4. Check upstream repo access:
   ```
   gh repo view inder/salvobase --json name
   ```

### Configuration

- Workflow: `.github/workflows/contributor.yml`
- Prompt: `.github/contributor-prompt.md`
- Dispatcher: `scripts/contributor/run.py`
- Runs every 8 hours (00:00, 08:00, 16:00 UTC)

*Filed automatically by the contributor agent workflow.*"""

    result = subprocess.run(
        [
            "gh", "issue", "create",
            "--repo", repo,
            "--title", f"bug: contributor agent cycle failed ({date_str})",
            "--label", "bug,priority:high",
            "--body", body,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: gh issue create failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Filed contributor failure issue: {result.stdout.strip()}")


if __name__ == "__main__":
    main()
