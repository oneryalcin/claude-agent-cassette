"""scrub_tape: blank PII values while preserving structure + control decisions."""
from __future__ import annotations

import json

from claude_agent_cassette import direction_b_exchanges, scrub_init_inventory, scrub_tape


def _perm_pair(path: str):
    return [
        {"dir": "read", "frame": {"type": "control_request", "request_id": "r1", "request": {
            "subtype": "can_use_tool", "tool_name": "Write", "input": {"file_path": path}}}},
        {"dir": "write", "data": json.dumps({"type": "control_response", "response": {
            "subtype": "success", "request_id": "r1",
            "response": {"behavior": "allow", "updatedInput": {"file_path": path}}}})},
    ]


def test_scrub_masks_values_but_preserves_the_decision():
    out = scrub_tape(_perm_pair("/home/alice/secret/x.py"), [("/home/alice", "<HOME>")])
    exchange = direction_b_exchanges(out)["can_use_tool"][0]
    assert exchange.request["input"]["file_path"] == "<HOME>/secret/x.py"
    # the control decision survives the scrub — only the path inside it is masked
    assert exchange.decision == {"behavior": "allow", "updatedInput": {"file_path": "<HOME>/secret/x.py"}}


def test_scrub_applies_longest_needle_first():
    # /var/t is a prefix of /private/var/t; the longer must mask first, else "/private<CWD>/f"
    tape = [{"dir": "read", "frame": {"type": "x", "path": "/private/var/t/f"}}]
    out = scrub_tape(tape, [("/var/t", "<CWD>"), ("/private/var/t", "<CWD>")])
    assert out[0]["frame"]["path"] == "<CWD>/f"


def test_scrub_reserializes_write_payload_so_no_raw_value_leaks():
    tape = [{"dir": "write", "data": json.dumps({"cwd": "/home/alice/p"})}]
    out = scrub_tape(tape, [("/home/alice", "<HOME>")])
    assert "/home/alice" not in out[0]["data"]
    assert json.loads(out[0]["data"]) == {"cwd": "<HOME>/p"}


def test_scrub_ignores_empty_needle_and_non_strings():
    tape = [{"dir": "read", "frame": {"count": 5, "keep": "untouched"}}]
    assert scrub_tape(tape, [("", "X"), ("nomatch", "Y")]) == tape


def test_scrub_does_not_mutate_input_tape():
    tape = _perm_pair("/home/alice/x")
    before = json.dumps(tape)
    scrub_tape(tape, [("/home/alice", "<HOME>")])
    assert json.dumps(tape) == before  # original untouched


# --- scrub_init_inventory: blank the recording environment's fingerprint ---


def _tape_with_inventory():
    return [
        {"dir": "read", "frame": {
            "type": "system", "subtype": "init", "session_id": "s1", "model": "claude-x",
            "slash_commands": ["internal-deploy", "sentry:seer"],
            "plugins": [{"name": "sentry", "path": "/home/alice/.claude/plugins/sentry"}],
            "skills": ["internal-skill"], "agents": ["custom-agent"],
            "mcp_servers": [{"name": "internal-api", "status": "connected"}],
            "memory_paths": ["/home/alice/.claude/memory"],
            "tools": ["Bash", "mcp__internal__query"],
        }},
        {"dir": "read", "frame": {"type": "assistant", "message": {"content": []}}},
    ]


def test_init_inventory_is_blanked_but_frame_survives():
    """A tape recorded in a real environment leaks the operator's (or company's)
    tooling inventory through system/init — committing it publishes that inventory."""
    out = scrub_init_inventory(_tape_with_inventory())
    init = out[0]["frame"]
    for key in ("slash_commands", "plugins", "skills", "agents",
                "mcp_servers", "memory_paths", "tools"):
        assert init[key] == [], key
    # non-inventory keys and other frames are untouched
    assert init["session_id"] == "s1" and init["model"] == "claude-x"
    assert out[1] == _tape_with_inventory()[1]


def test_init_inventory_scrub_does_not_mutate_input_tape():
    tape = _tape_with_inventory()
    before = json.dumps(tape)
    scrub_init_inventory(tape)
    assert json.dumps(tape) == before


async def test_init_inventory_scrubbed_tape_still_replays():
    """The decision-preserving contract, end-to-end: replay never reads init
    inventory, so the scrub must not cost a tape its replayability."""
    import asyncio
    from pathlib import Path

    from claude_agent_cassette import load_tape, replay_tape

    mcp = Path(__file__).parent.parent / "examples" / "cassettes" / "mcp_session.jsonl"

    async def drive() -> int:
        n = 0
        async with replay_tape(scrub_init_inventory(load_tape(mcp)), mode="stub") as client:
            async for message in client.receive_messages():
                n += 1
                if type(message).__name__ == "ResultMessage":
                    break
        return n

    assert await asyncio.wait_for(drive(), 20) > 0
