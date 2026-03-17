#!/usr/bin/env python3
"""
Self-hosted contributor agent runner.
Builds the prompt from env vars and invokes claude.
"""
import os
import subprocess
import sys

slot = os.environ.get("AGENT_SLOT", "1")
target = os.environ.get("TARGET_ISSUE", "").strip()

if target and target != "none":
    issue_directive = (
        f"Your target issue is #{target}. It has already been claimed for you. "
        f"Go directly to it — read the issue body, implement it, open a PR, merge it."
    )
else:
    issue_directive = (
        "No issue was assigned to this slot (backlog is empty or fully claimed). "
        "Post a summary to General discussions saying the backlog is clear, then exit."
    )

prompt = f"""You are a founder-tier contributor agent (slot {slot}) for Salvobase, running headlessly in GitHub Actions.
You are operating as @inder with full maintainer trust. You can push branches and merge PRs directly.

{issue_directive}

## Your job (one issue per run)
1. Read the issue body in full — `gh issue view NUMBER --repo inder/salvobase`
2. Read ARCHITECTURE.md and the relevant source files. Do not guess at structure.
3. Implement the fix and tests.
4. Create a branch: `founder-s{slot}/$(date +%Y%m%d)-issue-NUMBER`
5. Commit with: `git add <specific files>` (never `git add .`)
6. Push and open a PR to master.
7. Merge immediately: `gh pr merge PR_NUMBER --repo inder/salvobase --squash --admin --body "Auto-merged by founder agent (slot {slot})."`
8. Comment on the issue summarising what changed.

## PR body template
```
Closes #NUMBER

## What
<what changed>

## Why
<why this matters>

```yaml
agent:
  id: founder-agent-s{slot}
  type: claude-code
  model: claude-sonnet-4-6
  operator: inder
  trust_tier: maintainer
  issues: ["#NUMBER"]
```

*Posted by the founder agent on behalf of @inder*
```

## Step 9: Post run summary to General discussions

After merging (or if you found nothing to do), post a one-line summary to General discussions:

```bash
BODY="YOUR BODY HERE"
gh api graphql -f query="
  mutation {{
    createDiscussion(input: {{
      repositoryId: \\"R_kgDORc_F6A\\",
      categoryId: \\"DIC_kwDORc_F6M4C4C6z\\",
      title: \\"Agent run: $(date -u +%Y-%m-%d) slot {slot}\\",
      body: $(echo "$BODY" | jq -Rs .)
    }}) {{
      discussion {{ url }}
    }}
  }}
"
```

Body format:
```
**Slot {slot} — $(date -u +%Y-%m-%dT%H:%MZ)**

Issue: #NUMBER — <title>
PR: #NUMBER (merged)

<one sentence on what changed>

*Posted by the founder agent on behalf of @inder*
```

If there were no available issues, body should be:
```
**Slot {slot} — $(date -u +%Y-%m-%dT%H:%MZ)**

No available issues found. Backlog is clear or all issues are claimed.

*Posted by the founder agent on behalf of @inder*
```

## Notes
- `make test` verifies the build — run it before committing
- `gh pr review --approve` will fail (can't approve own PR) — skip it, use `--admin` merge
- One issue per run. Stop after merging one PR.
- Every GitHub comment must end with: *Posted by the founder agent on behalf of @inder*
"""

result = subprocess.run(
    [
        "claude", "-p", prompt,
        "--allowedTools", "Bash,Read,Write,Edit,Glob,Grep,Agent",
        "--output-format", "text",
    ],
    capture_output=False,
)
sys.exit(result.returncode)
