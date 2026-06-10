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
import tempfile
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, HookMatcher

from claude_agent_cassette import (
    default_replacements,
    path_replacements,
    record,
    save_tape,
    scrub_init_inventory,
    scrub_tape,
)

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


def _replacements(config_dir: str, cwd: str) -> list[tuple[str, str]]:
    """The recording's full fingerprint: cwd/home/key (raw + slug forms via the
    library defaults), the recording-specific dirs, the whole temp root (its path
    embeds a stable per-user hash on macOS), and the bare username (tool output
    like ``ls -la`` prints it outside any path)."""
    import getpass

    return (
        default_replacements()
        + path_replacements(cwd, "<CWD>")
        + path_replacements(config_dir, "<CONFIG>")
        + path_replacements(tempfile.gettempdir(), "<TMP>")
        + [(getpass.getuser(), "<USER>")]
    )


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
    # Isolate the CLI from the operator's ~/.claude: a fresh config dir means the
    # recorded system/init inventory (slash commands, plugins, skills, MCP servers,
    # hooks) is the CLI's builtin baseline, not this machine's fingerprint.
    config_dir = tempfile.mkdtemp(prefix="cassette-clean-config-")
    with tempfile.TemporaryDirectory() as cwd:
        options = ClaudeAgentOptions(
            allowed_tools=["Bash"],
            hooks={"PreToolUse": [HookMatcher(matcher="Bash", hooks=[pretooluse_hook])]},
            cwd=cwd,
            model=_MODEL,
            env={"CLAUDE_CONFIG_DIR": config_dir},
        )
        print(f"Recording hooks session in {cwd} ...")
        with record() as tape:
            async with ClaudeSDKClient(options=options) as client:
                await client.query(_PROMPT)
                async for message in client.receive_response():
                    if type(message).__name__ == "ResultMessage":
                        break

        scrubbed = scrub_init_inventory(scrub_tape(tape, _replacements(config_dir, cwd)))
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        save_tape(scrubbed, _OUT)
        print(f"\nWrote {len(scrubbed)} frames -> {_OUT}")
        _summary(scrubbed)


if __name__ == "__main__":
    asyncio.run(main())
