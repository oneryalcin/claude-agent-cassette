#!/usr/bin/env python3
"""Record a real ``can_use_tool`` (Direction-B) session into a *decision-preserving* cassette.

Direction B is the control plane where the **CLI asks the SDK** ("can I use this
tool?") and the SDK invokes the consumer's ``can_use_tool`` callback, then writes
the decision back. Those frames only exist if a real session registered the
callback *and* tried to use tools — so we record one.

This runs a permission-gated agent task under ``record()``, which tees the
full duplex wire (including the ``can_use_tool`` control_request and our decision
control_response) into a tape.

Why a *new* recording and not the existing websearch fixture: that fixture scrubbed
the Direction-B **decisions** to ``"<scrubbed>"`` (and has no ``can_use_tool`` at
all). A control-replay stub hands back the recorded *decision*, so the decision is
exactly the payload we must keep. The scrub here therefore blanks filesystem PII
(home / temp paths, API key) while leaving every control-plane decision field
(``behavior`` / ``updatedInput`` / deny ``message``) untouched.

Adapted from the SDK's ``examples/tool_permission_callback.py``:
- a *deterministic* callback (no interactive ``input()``, so recording never blocks)
- a prompt crafted to exercise all three decision shapes in one tape:
  **allow**, **allow + updatedInput** (redirect), and **deny**.

Run (spends a small API call; needs ANTHROPIC_API_KEY + the bundled claude CLI):

    uv run python examples/record_permission_session.py
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

from claude_agent_cassette import record, save_tape, scrub_tape

_OUT = Path(__file__).parent / "cassettes" / "permission_session.jsonl"
# Pin a non-Covered (zero-data-retention-OK) model: the default rotated to Fable 5,
# a Covered Model that requires data retention enabled. Direction-B control is
# model-agnostic, so any capable model yields a valid fixture.
_MODEL = "claude-haiku-4-5-20251001"

# A task ordered to deterministically trigger one of each decision shape:
#   1. list files            -> read-only tool   -> ALLOW
#   2. write hello.py        -> non-safe path    -> ALLOW + updatedInput (redirect)
#   3. write /etc/...        -> system directory -> DENY
_PROMPT = (
    "Do these steps in order, each using a tool, and stop after the last one:\n"
    "1. List the files in the current directory.\n"
    "2. Create a file named hello.py containing a Python hello-world print.\n"
    "3. Run the bash command: echo hello\n"
    "4. Create a file at /etc/cassette_probe.txt containing the text hi.\n"
)


async def deterministic_permission(
    tool_name: str, input_data: dict[str, Any], context: ToolPermissionContext
) -> PermissionResultAllow | PermissionResultDeny:
    """A fully deterministic permission policy — every branch returns without prompting.

    Produces the three decision shapes we want recorded: a plain allow, an allow
    that rewrites the tool input (``updated_input``), and a deny.
    """
    # Read-only tools: always allow.
    if tool_name in {"Read", "Glob", "Grep", "LS"}:
        print(f"  ALLOW            {tool_name}")
        return PermissionResultAllow()

    if tool_name in {"Write", "Edit", "MultiEdit"}:
        file_path = str(input_data.get("file_path", ""))
        # Deny writes into system directories.
        if file_path.startswith(("/etc/", "/usr/", "/bin/", "/sbin/")):
            print(f"  DENY             {tool_name} -> {file_path}")
            return PermissionResultDeny(message=f"Refusing to write to system path: {file_path}")
        # Redirect any other write into a sandbox dir -> decision carries updatedInput.
        safe = f"./safe_output/{os.path.basename(file_path) or 'file'}"
        print(f"  ALLOW+updatedInput {tool_name} {file_path} -> {safe}")
        modified = {**input_data, "file_path": safe}
        return PermissionResultAllow(updated_input=modified)

    if tool_name == "Bash":
        command = str(input_data.get("command", ""))
        if any(p in command for p in ("rm -rf", "sudo", "mkfs", "dd if=")):
            print(f"  DENY             Bash -> {command!r}")
            return PermissionResultDeny(message="Refusing dangerous command")
        print(f"  ALLOW            Bash -> {command!r}")
        return PermissionResultAllow()

    # Anything else: deny deterministically (never prompt).
    print(f"  DENY (default)   {tool_name}")
    return PermissionResultDeny(message=f"Tool not permitted in recording: {tool_name}")


def _pii_replacements(cwd: str) -> list[tuple[str, str]]:
    """The (needle, mask) pairs that blank this recording's filesystem fingerprint."""
    replacements = [
        (os.path.realpath(cwd), "<CWD>"),
        (cwd, "<CWD>"),
        (os.path.expanduser("~"), "<HOME>"),
    ]
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        replacements.append((key, "<REDACTED_API_KEY>"))
    return replacements


def _summary(scrubbed: list[dict]) -> None:
    """Print the Direction-B can_use_tool exchanges recorded, with their (kept) decisions."""
    resp_by_id: dict[str, Any] = {}
    for e in scrubbed:
        if e.get("dir") == "write":
            try:
                d = json.loads(e["data"])
            except ValueError:
                continue
            if d.get("type") == "control_response":
                resp_by_id[d["response"]["request_id"]] = d["response"].get("response")

    print("\nDirection-B can_use_tool exchanges captured:")
    n = 0
    for e in scrubbed:
        f = e.get("frame") if e.get("dir") == "read" else None
        if f and f.get("type") == "control_request" and (f.get("request") or {}).get("subtype") == "can_use_tool":
            n += 1
            req = f["request"]
            decision = resp_by_id.get(f["request_id"])
            print(f"  #{n} {req.get('tool_name')} -> {json.dumps(decision)}")
    if n == 0:
        print("  (none — the model did not trigger a permission prompt; "
              "adjust the prompt / permission_mode and re-run)")


async def main() -> None:
    with tempfile.TemporaryDirectory() as cwd:
        options = ClaudeAgentOptions(
            can_use_tool=deterministic_permission,
            permission_mode="default",  # ensures the callback is consulted
            cwd=cwd,
            model=_MODEL,
        )
        print(f"Recording permission session in {cwd} ...\n")
        with record() as tape:
            async with ClaudeSDKClient(options) as client:
                await client.query(_PROMPT)
                async for message in client.receive_response():
                    if type(message).__name__ == "ResultMessage":
                        break

        scrubbed = scrub_tape(tape, _pii_replacements(cwd))
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        save_tape(scrubbed, _OUT)
        print(f"\nWrote {len(scrubbed)} frames -> {_OUT}")
        _summary(scrubbed)


if __name__ == "__main__":
    asyncio.run(main())
