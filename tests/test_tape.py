"""Control-protocol view of the tape — pure data, no SDK.

These cover the wire-format knowledge that ``ReplayTransport.from_tape`` builds
on, as plain functions over plain dicts (so they need no client and assert on
return values, not transport internals).
"""
from __future__ import annotations

import json

from claude_agent_cassette import (
    control_request_subtype,
    control_responses_by_subtype,
    replayable_messages,
)


def _write(subtype: str, request_id: str) -> dict:
    return {"dir": "write", "data": json.dumps(
        {"type": "control_request", "request_id": request_id,
         "request": {"subtype": subtype}})}


def _response(request_id: str, body: dict | None = None) -> dict:
    return {"dir": "read", "frame": {"type": "control_response", "response": {
        "subtype": "success", "request_id": request_id, "response": body or {}}}}


def _tape() -> list[dict]:
    return [
        _write("initialize", "w1"),
        _response("w1", {"commands": ["x"]}),
        {"dir": "read", "frame": {"type": "control_request", "request_id": "b1",
                                  "request": {"subtype": "mcp_message"}}},  # Direction B
        {"dir": "read", "frame": {"type": "assistant", "message": {}}},
        _write("mcp_status", "w2"),
        _response("w2"),
        {"dir": "read", "frame": {"type": "result", "subtype": "success"}},
    ]


def test_replayable_messages_keeps_conversation_drops_all_control():
    msgs = replayable_messages(_tape())
    assert [m["type"] for m in msgs] == ["assistant", "result"]


def test_control_responses_keyed_by_request_subtype():
    by_subtype = control_responses_by_subtype(_tape())
    assert set(by_subtype) == {"initialize", "mcp_status"}
    # correlation is by recorded request_id, so initialize gets w1's body
    assert by_subtype["initialize"][0]["response"]["response"] == {"commands": ["x"]}


def test_control_responses_preserve_order_within_subtype():
    tape = [_write("mcp_status", "a"), _response("a", {"n": 1}),
            _write("mcp_status", "b"), _response("b", {"n": 2})]
    bucket = control_responses_by_subtype(tape)["mcp_status"]
    assert [r["response"]["response"]["n"] for r in bucket] == [1, 2]


def test_unmatched_writes_and_responses_are_dropped():
    # a write with no response, and a response with no write -> neither bucketed
    tape = [_write("initialize", "w1"), _response("orphan")]
    assert control_responses_by_subtype(tape) == {}


def test_empty_tape():
    assert replayable_messages([]) == []
    assert control_responses_by_subtype([]) == {}


def test_control_request_subtype_accessor():
    assert control_request_subtype({"request": {"subtype": "initialize"}}) == "initialize"
    assert control_request_subtype({}) is None
    assert control_request_subtype({"request": None}) is None
