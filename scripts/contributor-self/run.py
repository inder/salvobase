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

if target:
    issue_directive = (
        f"Your target issue is #{target}. "
        f"Go directly to it — read the issue body, implement it, open a PR, merge it."
    )
else:
    issue_directive = (
        "Auto-select the highest priority available issue:\n"
        "```bash\n"
        "gh issue list --repo inder/salvobase --state open \\\n"
        "  --label \"agent:available\" --limit 50 \\\n"
        "  --json number,title,labels \\\n"
        "  --jq '[\n"
        "    .[] | select(.labels|map(.name)|contains([\"agent:claimed\"])|not)\n"
        "  ] | sort_by(\n"
        "    if (.labels|map(.name)|contains([\"priority:p0\"])) then 0\n"
        "    elif (.labels|map(.name)|contains([\"priority:critical\"])) then 1\n"
        "    elif (.labels|map(.name)|contains([\"priority:high\"])) then 2\n"
        "    elif (.labels|map(.name)|contains([\"priority:medium\"])) then 3\n"
        "    else 4 end\n"
        "  ) | .[0]'\n"
        "```\n"
        "Skip issues where a PR referencing them was opened in the last 8 hours."
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
