"""scrub_tape: blank PII values while preserving structure + control decisions."""
from __future__ import annotations

import json

from claude_agent_cassette import (
    default_replacements,
    direction_b_exchanges,
    path_replacements,
    scrub_init_inventory,
    scrub_tape,
)


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
    """Mirrors the REAL wire shapes: memory_paths is a dict on current CLIs, and the
    initialize handshake's control_response carries its own inventory."""
    return [
        {"dir": "read", "frame": {"type": "control_response", "response": {
            "subtype": "success", "request_id": "req_1",
            "response": {
                "commands": [{"name": "internal-deploy"}],
                "agents": [{"name": "custom-agent", "description": "internal"}],
                "models": [{"id": "m"}],
                "available_output_styles": ["default", "internal-style"],
                "account": {"organization": "acme"},
                "output_style": "default",
            }}}},
        {"dir": "read", "frame": {
            "type": "system", "subtype": "init", "session_id": "s1", "model": "claude-x",
            "slash_commands": ["internal-deploy", "sentry:seer"],
            "plugins": [{"name": "sentry", "path": "/home/alice/.claude/plugins/sentry"}],
            "skills": ["internal-skill"], "agents": ["custom-agent"],
            "mcp_servers": [{"name": "internal-api", "status": "connected"}],
            "memory_paths": {"auto": "/home/alice/.claude/projects/-home-alice-proj/memory/"},
            "tools": ["Bash", "mcp__internal__query"],
        }},
        {"dir": "read", "frame": {"type": "assistant", "message": {"content": []}}},
        {"dir": "write", "data": json.dumps({"type": "control_response", "response": {
            "subtype": "success", "request_id": "b1", "response": {"behavior": "allow"}}})},
    ]


def test_init_frame_inventory_is_blanked_including_dict_valued_keys():
    """memory_paths is a DICT on current CLIs — a list-only blank silently leaks the
    one inventory key that carries absolute operator paths (review finding)."""
    init = scrub_init_inventory(_tape_with_inventory())[1]["frame"]
    assert init["memory_paths"] == {}
    for key in ("slash_commands", "plugins", "skills", "agents", "mcp_servers", "tools"):
        assert init[key] == [], key
    # non-inventory keys are untouched
    assert init["session_id"] == "s1" and init["model"] == "claude-x"


def test_handshake_response_inventory_is_blanked_decisions_untouched():
    """The initialize control_response carries the inventory a second time
    (commands/agents/models/account) — blanking only system/init leaves it
    published (review finding). Direction-B decision writes must ride through."""
    out = scrub_init_inventory(_tape_with_inventory())
    inner = out[0]["frame"]["response"]["response"]
    assert inner["commands"] == [] and inner["agents"] == [] and inner["models"] == []
    assert inner["available_output_styles"] == [] and inner["account"] == {}
    # routing envelope and non-inventory keys survive
    assert out[0]["frame"]["response"]["request_id"] == "req_1"
    assert inner["output_style"] == "default"
    # the Direction-B decision write is byte-identical
    assert out[3] == _tape_with_inventory()[3]


def test_init_inventory_scrub_output_shares_nothing_with_input():
    """The result must be a true copy: mutating the scrubbed output (e.g. follow-up
    anonymization) must not corrupt the original recording (review finding)."""
    tape = _tape_with_inventory()
    out = scrub_init_inventory(tape)
    out[2]["frame"]["message"]["content"].append({"type": "text", "text": "mutated"})
    out[1]["frame"]["session_id"] = "mutated"
    assert tape == _tape_with_inventory()  # original untouched


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


# --- path_replacements / default_replacements: the slug-encoding blind spot ---


def test_path_replacements_cover_the_slug_encoded_form():
    """The CLI embeds paths slug-encoded (/Users/alice/proj -> -Users-alice-proj) in
    projects/… strings; a literal path needle can never match them, so an
    'all-paths-masked' scrub still leaked the project path (review finding)."""
    pairs = path_replacements("/Users/alice/my_proj", "<CWD>")
    tape = [{"dir": "read", "frame": {
        "type": "system", "subtype": "init",
        "memory_paths": {"auto": "/x/projects/-Users-alice-my-proj/memory/"},
    }}]
    out = scrub_tape(tape, pairs)
    assert out[0]["frame"]["memory_paths"]["auto"] == "/x/projects/<CWD>/memory/"


def test_default_replacements_mask_cwd_home_and_key(monkeypatch):
    import os

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-needle")
    pairs = dict(default_replacements())
    assert pairs.get(os.getcwd()) == "<CWD>"
    assert pairs.get(os.path.expanduser("~")) == "<HOME>"
    assert pairs.get("sk-ant-test-needle") == "<REDACTED_API_KEY>"
