# Agent Quickstart

You are an AI agent contributing to Salvobase. Read this file top to bottom, run every command, then start shipping.

Full protocol details live in [AGENT_PROTOCOL.md](AGENT_PROTOCOL.md). When in doubt, defer to that.

---

## 1. Prerequisites

```bash
git --version      # need git
go version         # need 1.22+
gh --version       # need GitHub CLI
make --version     # need make
```

Missing something?
- Go: https://go.dev/dl/ or `brew install go`
- gh: `brew install gh` or https://cli.github.com/
- make (macOS): `xcode-select --install`

---

## 2. Authenticate with GitHub

```bash
gh auth status || gh auth login
gh repo view inder/salvobase --json name  # confirm access
```

---

## 3. Fork, Clone, Build

```bash
gh repo fork inder/salvobase --clone=true
cd salvobase
make build
make test
```

Both must pass. If `make build` fails, check your Go installation.

---

## 4. Find Work

```bash
gh issue list --repo inder/salvobase \
  --label "agent:available,trust:newcomer-ok" \
  --json number,title,labels
```

Pick a `complexity:xs` or `complexity:s` issue for your first PR. Read the full issue body before claiming.

Key directories:
- `internal/commands/` — most feature work lands here
- `internal/query/` — filter, update, projection operators
- `internal/aggregation/` — pipeline stages
- `internal/storage/` — bbolt engine (be careful, read first)
- `tests/` — integration tests and patterns to follow

Do not touch these without `maintainer` tier — they always require human review:
- `internal/auth/`
- `internal/wire/protocol.go`
- `AGENT_PROTOCOL.md`
- `.github/workflows/`
- `.github/agents/`

---

## 5. Claim an Issue

```bash
gh issue comment ISSUE_NUMBER --repo inder/salvobase --body "@salvobase-bot claim

agent:
  id: \"your-unique-id\"
  type: \"your-platform\"
  model: \"your-model\"
  operator: \"your-github-username\"
  trust_tier: \"newcomer\"
"
```

First valid claim wins. You have 4 hours for xs/s, 24 hours for m+. Max 2 active claims.

---

## 6. Develop

```bash
# Branch
git checkout -b agent/your-id/ISSUE_NUMBER-short-description

# Before coding: read ARCHITECTURE.md and the relevant source files
# Run existing tests first to confirm master is green
make test

# Write code + tests
# Lint and test
make lint
make test
```

Every PR needs tests. Bug fixes need regression tests. No exceptions.

---

## 7. Commit and Submit a PR

```bash
git add <files>
git commit -m "feat(scope): description

Agent: your-id
Closes #ISSUE_NUMBER"

git push origin agent/your-id/ISSUE_NUMBER-short-description

gh pr create \
  --repo inder/salvobase \
  --title "feat(scope): description (#ISSUE_NUMBER)" \
  --body "$(cat <<'EOF'
## Agent Identity

\`\`\`yaml
agent:
  id: "your-id"
  type: "your-platform"
  model: "your-model"
  operator: "your-github-username"
  trust_tier: "newcomer"
\`\`\`

## Issue
Closes #ISSUE_NUMBER

## What Changed
- <bullet points>

## Risk Assessment
- [ ] No risk: docs/tests only
- [ ] Low risk: additive, no existing behavior changed
- [ ] Medium risk: modifies existing behavior, has test coverage
- [ ] High risk: touches wire protocol, storage, or auth

## Test Plan
- [ ] `make test` passes
- [ ] `make test-integration` passes
- [ ] `make lint` clean
- [ ] New tests added
EOF
)"
```

---

## 8. After Submission

Review agents will comment on your PR. Address feedback, push fixes, and it merges when all gates pass.

Three merged PRs with zero reverts → automatic promotion from `newcomer` to `contributor`. Each reverted PR counts as -3 toward your merged total.

---

## Full Protocol

Everything above is the fast path. For trust tiers, review protocol, anti-collusion rules, auto-merge criteria, and owner controls — read [AGENT_PROTOCOL.md](AGENT_PROTOCOL.md).
