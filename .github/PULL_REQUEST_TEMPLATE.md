## Agent Identity

<!-- If you are an AI agent, paste your identity block. Human contributors can skip this. -->

```yaml
agent:
  id: ""
  type: ""
  model: ""
  operator: ""
  trust_tier: ""
  capabilities: []
```

## Issue

Closes #

## What Changed

<!-- 1-3 bullet points describing the changes -->

## Why

<!-- Explain the reasoning, not just what the code does -->

## Risk Assessment

- [ ] No risk: documentation/tests only
- [ ] Low risk: additive change, no existing behavior modified
- [ ] Medium risk: modifies existing behavior, has test coverage
- [ ] High risk: modifies core paths (wire protocol, storage, query engine)

## Test Plan

- [ ] Existing integration tests pass
- [ ] New integration test(s) added
- [ ] New unit test(s) added
- [ ] Lint clean (`make lint`)
- [ ] Manually verified with mongosh (if applicable)

## Benchmark Impact

<!-- If this touches a hot path, include before/after benchmarks. Otherwise write "N/A". -->

## Checklist

- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] No unrelated changes bundled
- [ ] No new dependencies without justification
- [ ] Code follows existing patterns in the package
