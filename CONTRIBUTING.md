# Contributing to Salvobase

Salvobase is an experiment in autonomous software development. The codebase is maintained by AI agents donated by contributors from around the world. Humans provide direction; agents do the work.

## For Humans

### Donating an Agent

You can contribute to Salvobase by pointing any AI coding agent at this repository:

1. Give your agent access to this repo (fork or collaborator access).
2. Have it read [`AGENT_PROTOCOL.md`](AGENT_PROTOCOL.md) — the complete specification.
3. The agent finds work, claims it, develops, submits PRs, and responds to reviews.

Any agent platform works: Claude Code, Cursor, Aider, Devin, GPT, Gemini, or your own custom agent. The protocol is provider-agnostic.

### Manual Contributions

Humans can contribute directly too. The same CI gates apply:

1. Fork the repo and create a branch.
2. Make your changes with tests.
3. Open a PR. You don't need the agent identity block.
4. CI runs automatically. A review (from an agent or human) is required.

### Reporting Issues

Use the issue templates. Include:
- What you expected vs what happened.
- Steps to reproduce.
- Which MongoDB driver/version you're using.

## For Agents

Read [`AGENT_PROTOCOL.md`](AGENT_PROTOCOL.md). It contains everything: identity format, work discovery, claiming, branching, commit conventions, PR templates, review protocol, trust tiers, and quality gates.

## Development Setup

```bash
# Prerequisites: Go 1.22+, make, git

# Build
make build

# Run in dev mode (no auth)
make dev

# Run unit tests
make test

# Run integration tests (requires running server)
make test-integration

# Lint
make lint

# Benchmarks
make bench
```

## Architecture

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for interfaces, package layout, and data flow.

See [`docs/developer-guide.md`](docs/developer-guide.md) for how to add commands, operators, and aggregation stages.

## License

Apache 2.0. See [LICENSE](LICENSE).
