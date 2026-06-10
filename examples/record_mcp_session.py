#!/usr/bin/env python3
"""Record a real ``mcp_message`` (Direction-B) session into a *decision-preserving* cassette.

``mcp_message`` is the third Direction-B subtype: the CLI tunnels JSON-RPC to an
**in-process SDK MCP server** (``create_sdk_mcp_server``) through the control plane —
``initialize``, ``tools/list``, then a ``tools/call`` per tool use — and the SDK
writes each JSON-RPC response back as the control_response's ``mcp_response``.
Those frames only exist if a real session registered an SDK MCP server *and* the
model called its tools — so we record one.

Adapted from the SDK's ``examples/mcp_calculator.py``, trimmed to a deterministic
two-tool server and a prompt that exercises both a normal result and an
``is_error`` tool result in one tape.

Run (spends a small API call; needs ANTHROPIC_API_KEY + the bundled claude CLI):

    uv run python examples/record_mcp_session.py
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    create_sdk_mcp_server,
    tool,
)

from claude_agent_cassette import (
    default_replacements,
    record,
    save_tape,
    scrub_init_inventory,
    scrub_tape,
)

_OUT = Path(__file__).parent / "cassettes" / "mcp_session.jsonl"
# Pin a non-Covered (zero-data-retention-OK) model: the default rotated to Fable 5,
# a Covered Model that requires data retention enabled. Direction-B control is
# model-agnostic, so any capable model yields a valid fixture.
_MODEL = "claude-haiku-4-5-20251001"

_PROMPT = (
    "Use the calculator tools for both steps, in order, then stop:\n"
    "1. Add 15 and 27.\n"
    "2. Divide 100 by 0.\n"
    "Report what the tools returned."
)


@tool("add", "Add two numbers", {"a": float, "b": float})
async def add_numbers(args: dict[str, Any]) -> dict[str, Any]:
    result = args["a"] + args["b"]
    return {"content": [{"type": "text", "text": f"{args['a']} + {args['b']} = {result}"}]}


@tool("divide", "Divide one number by another", {"a": float, "b": float})
async def divide_numbers(args: dict[str, Any]) -> dict[str, Any]:
    if args["b"] == 0:
        return {
            "content": [{"type": "text", "text": "Error: Division by zero is not allowed"}],
            "is_error": True,
        }
    result = args["a"] / args["b"]
    return {"content": [{"type": "text", "text": f"{args['a']} / {args['b']} = {result}"}]}


def _summary(scrubbed: list[dict]) -> None:
    """Print the Direction-B mcp_message exchanges recorded, with their JSON-RPC shape."""
    resp_by_id: dict[str, Any] = {}
    for e in scrubbed:
        if e.get("dir") == "write":
            try:
                d = json.loads(e["data"])
            except ValueError:
                continue
            if d.get("type") == "control_response":
                resp_by_id[d["response"]["request_id"]] = d["response"].get("response")

    print("\nDirection-B mcp_message exchanges captured:")
    n = 0
    for e in scrubbed:
        f = e.get("frame") if e.get("dir") == "read" else None
        if f and f.get("type") == "control_request" and (f.get("request") or {}).get("subtype") == "mcp_message":
            n += 1
            msg = f["request"].get("message") or {}
            decision = resp_by_id.get(f["request_id"]) or {}
            rpc = (decision.get("mcp_response") or {}) if isinstance(decision, dict) else {}
            print(f"  #{n} {msg.get('method') or '(notification?)'} "
                  f"params={json.dumps(msg.get('params'))[:80]} "
                  f"-> {json.dumps(rpc)[:120]}")
    if n == 0:
        print("  (none — the model did not call the MCP tools; adjust the prompt and re-run)")


async def main() -> None:
    calculator = create_sdk_mcp_server(
        name="calculator", version="1.0.0", tools=[add_numbers, divide_numbers]
    )
    # Isolate the CLI from the operator's ~/.claude: a fresh config dir means the
    # recorded system/init inventory (slash commands, plugins, skills, MCP servers,
    # hooks) is the CLI's builtin baseline, not this machine's fingerprint.
    config_dir = tempfile.mkdtemp(prefix="cassette-clean-config-")
    with tempfile.TemporaryDirectory() as cwd:
        options = ClaudeAgentOptions(
            mcp_servers={"calc": calculator},
            # Pre-approve the tools so the tape stays mcp_message-only (no can_use_tool).
            allowed_tools=["mcp__calc__add", "mcp__calc__divide"],
            model=_MODEL,
            cwd=cwd,  # never the operator's project dir (it rides the wire slug-encoded)
            env={"CLAUDE_CONFIG_DIR": config_dir},
        )
        print("Recording mcp session ...\n")
        with record() as tape:
            async with ClaudeSDKClient(options) as client:
                await client.query(_PROMPT)
                async for message in client.receive_response():
                    if type(message).__name__ == "ResultMessage":
                        break

        scrubbed = scrub_init_inventory(
            scrub_tape(tape, default_replacements(cwd=cwd, config_dir=config_dir, username=True))
        )
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    save_tape(scrubbed, _OUT)
    print(f"\nWrote {len(scrubbed)} frames -> {_OUT}")
    _summary(scrubbed)


if __name__ == "__main__":
    asyncio.run(main())
