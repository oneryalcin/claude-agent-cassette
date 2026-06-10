#!/usr/bin/env python3
"""Record a real ``hook_callback`` (Direction-B) session into a decision-preserving cassette.

A ``hook_callback`` is the CLI asking the SDK to run a registered hook; the SDK calls
the consumer's hook callback and writes its output back. Like ``can_use_tool`` these
frames only exist if the session registered hooks and used a tool that fires them — so
we record one with a single PreToolUse hook that returns a rich, observable output.

Recorded so we can build + validate the hook stub (incl. the recorded->live
``callback_id`` correlation). Scrub preserves the control-plane decision (the hook
output + callback_id) while masking filesystem PII. Adapted from the SDK's
``examples/hooks.py``.

Run:  uv run python examples/record_hooks_session.py
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher

from claude_agent_cassette import record_sdk_wire, serialize_tape

_OUT = Path(__file__).parent / "cassettes" / "hooks_session.jsonl"
_PROMPT = "Run the bash command: echo hello-from-hooks"
# Pin a non-Covered (zero-data-retention-OK) model: the default rotated to Fable 5,
# a Covered Model that requires data retention enabled. The Direction-B control
# protocol is model-agnostic, so any capable model yields a valid fixture.
_MODEL = "claude-haiku-4-5-20251001"


async def pretooluse_hook(input_data: Any, tool_use_id: Any, context: Any) -> dict[str, Any]:
    """A deterministic PreToolUse hook returning a rich, observable output."""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "permissionDecisionReason": "recorded-hook-approved",
        }
    }


def _redact(obj: Any, replacements: list[tuple[str, str]]) -> Any:
    if isinstance(obj, str):
        for needle, mask in replacements:
            if needle:
                obj = obj.replace(needle, mask)
        return obj
    if isinstance(obj, list):
        return [_redact(v, replacements) for v in obj]
    if isinstance(obj, dict):
        return {k: _redact(v, replacements) for k, v in obj.items()}
    return obj


def _scrub_tape(tape: list[dict], cwd: str) -> list[dict]:
    replacements = [
        (os.path.realpath(cwd), "<CWD>"),
        (cwd, "<CWD>"),
        (os.path.expanduser("~"), "<HOME>"),
    ]
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        replacements.append((key, "<REDACTED_API_KEY>"))
    replacements.sort(key=lambda r: len(r[0]), reverse=True)

    scrubbed: list[dict] = []
    for entry in tape:
        if entry.get("dir") == "write" and isinstance(entry.get("data"), str):
            try:
                payload = json.loads(entry["data"])
            except ValueError:
                scrubbed.append(_redact(entry, replacements))
                continue
            scrubbed.append({"dir": "write", "data": json.dumps(_redact(payload, replacements))})
        else:
            scrubbed.append(_redact(entry, replacements))
    return scrubbed


def _summary(scrubbed: list[dict]) -> None:
    resp_by_id: dict[str, Any] = {}
    init_hooks = None
    for e in scrubbed:
        if e.get("dir") == "write":
            try:
                d = json.loads(e["data"])
            except ValueError:
                continue
            if d.get("type") == "control_response":
                resp_by_id[d["response"]["request_id"]] = d["response"].get("response")
            elif d.get("type") == "control_request" and d["request"].get("subtype") == "initialize":
                init_hooks = d["request"].get("hooks")

    print("\nRecorded initialize hooks structure (callback ids the SDK assigned):")
    print("  ", json.dumps(init_hooks))
    print("\nhook_callback exchanges captured:")
    n = 0
    for e in scrubbed:
        f = e.get("frame") if e.get("dir") == "read" else None
        if f and f.get("type") == "control_request" and (f.get("request") or {}).get("subtype") == "hook_callback":
            n += 1
            req = f["request"]
            print(f"  #{n} callback_id={req.get('callback_id')!r} "
                  f"input_keys={list((req.get('input') or {}).keys())} "
                  f"-> output={json.dumps(resp_by_id.get(f['request_id']))}")
    if n == 0:
        print("  (none — the tool did not fire the hook; adjust the prompt and re-run)")


async def main() -> None:
    with tempfile.TemporaryDirectory() as cwd:
        options = ClaudeAgentOptions(
            allowed_tools=["Bash"],
            hooks={"PreToolUse": [HookMatcher(matcher="Bash", hooks=[pretooluse_hook])]},
            cwd=cwd,
            model=_MODEL,
        )
        print(f"Recording hooks session in {cwd} ...")
        with record_sdk_wire() as tape:
            async with ClaudeSDKClient(options=options) as client:
                await client.query(_PROMPT)
                async for message in client.receive_response():
                    if type(message).__name__ == "ResultMessage":
                        break

        scrubbed = _scrub_tape(tape, cwd)
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        _OUT.write_text(serialize_tape(scrubbed))
        print(f"\nWrote {len(scrubbed)} frames -> {_OUT}")
        _summary(scrubbed)


if __name__ == "__main__":
    asyncio.run(main())
