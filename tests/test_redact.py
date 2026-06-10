"""scrub_tape: blank PII values while preserving structure + control decisions."""
from __future__ import annotations

import json

from claude_agent_cassette import direction_b_exchanges, scrub_tape


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
