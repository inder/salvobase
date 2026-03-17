#!/usr/bin/env python3
"""
run.py — LLM-agnostic dispatcher for the contributor agent.

Reads LLM_PROVIDER env var and dispatches to the appropriate runner:

  anthropic (default) — claude -p "$(cat .github/contributor-prompt.md)"
                          --allowedTools Bash,Read,Write,Edit,Glob,Grep
                          --output-format text

  aider               — aider --model $LLM_MODEL --message <prompt>
                          --yes --no-auto-commits
                          (Supports any provider: anthropic/..., openai/..., gemini/...)

  openai              — stub, not implemented (use aider with openai/gpt-4o instead)

  custom              — exec $CUSTOM_RUNNER_CMD with prompt piped to stdin
"""

import os
import subprocess
import sys
from pathlib import Path

PROMPT_FILE = Path(".github/contributor-prompt.md")


def read_prompt() -> str:
    if not PROMPT_FILE.exists():
        print(f"ERROR: prompt file not found: {PROMPT_FILE}", file=sys.stderr)
        sys.exit(1)
    return PROMPT_FILE.read_text()


def run_anthropic(prompt: str) -> None:
    model = os.environ.get("AGENT_MODEL", "claude-sonnet-4-6")
    cmd = [
        "claude", "-p", prompt,
        "--allowedTools", "Bash,Read,Write,Edit,Glob,Grep",
        "--output-format", "text",
        "--model", model,
    ]
    result = subprocess.run(cmd, check=False)
    sys.exit(result.returncode)


def run_aider(prompt: str) -> None:
    # LLM_MODEL overrides AGENT_MODEL so operators can use "openai/gpt-4o"
    # or "anthropic/claude-sonnet-4-6" or "gemini/gemini-pro" etc.
    model = os.environ.get("LLM_MODEL", os.environ.get("AGENT_MODEL", "claude-sonnet-4-6"))
    cmd = [
        "aider",
        "--model", model,
        "--message", prompt,
        "--yes",
        "--no-auto-commits",
    ]
    result = subprocess.run(cmd, check=False)
    sys.exit(result.returncode)


def run_openai(_prompt: str) -> None:
    print("openai provider: not implemented.", file=sys.stderr)
    print("Use aider with LLM_MODEL=openai/gpt-4o for OpenAI support.", file=sys.stderr)
    sys.exit(1)


def run_custom(prompt: str) -> None:
    cmd = os.environ.get("CUSTOM_RUNNER_CMD")
    if not cmd:
        print("ERROR: CUSTOM_RUNNER_CMD not set", file=sys.stderr)
        sys.exit(1)
    result = subprocess.run(
        cmd,
        input=prompt,
        text=True,
        shell=True,
        check=False,
    )
    sys.exit(result.returncode)


def main() -> None:
    provider = os.environ.get("LLM_PROVIDER", "anthropic").lower()
    prompt = read_prompt()

    dispatch = {
        "anthropic": run_anthropic,
        "aider": run_aider,
        "openai": run_openai,
        "custom": run_custom,
    }

    runner = dispatch.get(provider)
    if runner is None:
        print(
            f"ERROR: Unknown LLM_PROVIDER '{provider}'. "
            f"Valid values: {', '.join(dispatch)}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"contributor dispatch: provider={provider} agent={os.environ.get('AGENT_ID', '?')}")
    runner(prompt)


if __name__ == "__main__":
    main()
