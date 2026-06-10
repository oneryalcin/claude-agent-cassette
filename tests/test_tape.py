"""Control-protocol view of the tape — pure data, no SDK.

These cover the wire-format knowledge that ``ReplayTransport.from_tape`` builds
on, as plain functions over plain dicts (so they need no client and assert on
return values, not transport internals).
"""
from __future__ import annotations

import json
from pathlib import Path

from claude_agent_cassette import (
    conversation_frames,
    direction_b_exchanges,
    load_tape,
)
from claude_agent_cassette.tape import (
    control_request_subtype,
    control_responses_by_subtype,
    direction_b_read_frames,
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


def test_conversation_frames_keeps_conversation_drops_all_control():
    msgs = conversation_frames(_tape())
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
    assert conversation_frames([]) == []
    assert control_responses_by_subtype([]) == {}


def test_control_request_subtype_accessor():
    assert control_request_subtype({"request": {"subtype": "initialize"}}) == "initialize"
    assert control_request_subtype({}) is None
    assert control_request_subtype({"request": None}) is None


# --- Direction-B exchanges: inbound control_request (CLI->SDK) paired with the
# outbound control_response (SDK's decision). Mirror of control_responses_by_subtype. ---


def _b_request(subtype: str, request_id: str, extra: dict | None = None) -> dict:
    """An inbound (read) Direction-B control_request: CLI -> SDK."""
    return {"dir": "read", "frame": {
        "type": "control_request", "request_id": request_id,
        "request": {"subtype": subtype, **(extra or {})}}}


def _b_decision(request_id: str, decision: dict, envelope: str = "success") -> dict:
    """An outbound (write) control_response carrying the SDK's decision."""
    return {"dir": "write", "data": json.dumps({"type": "control_response", "response": {
        "subtype": envelope, "request_id": request_id, "response": decision}})}


def test_direction_b_pairs_request_with_decision_by_subtype():
    tape = [
        _b_request("can_use_tool", "b1", {"tool_name": "Write", "input": {"file_path": "x"}}),
        _b_decision("b1", {"behavior": "allow", "updatedInput": {"file_path": "y"}}),
    ]
    by = direction_b_exchanges(tape)
    assert set(by) == {"can_use_tool"}
    ex = by["can_use_tool"][0]
    assert ex.subtype == "can_use_tool"
    assert ex.request["tool_name"] == "Write"
    assert ex.decision == {"behavior": "allow", "updatedInput": {"file_path": "y"}}
    assert ex.succeeded is True
    assert ex.request_id == "b1"


def test_direction_b_preserves_order_within_subtype():
    tape = [
        _b_request("can_use_tool", "b1"), _b_decision("b1", {"behavior": "allow"}),
        _b_request("can_use_tool", "b2"), _b_decision("b2", {"behavior": "deny", "message": "no"}),
    ]
    bucket = direction_b_exchanges(tape)["can_use_tool"]
    assert [e.decision.get("behavior") for e in bucket] == ["allow", "deny"]


def test_direction_b_unmatched_request_is_dropped():
    # inbound request with no recorded response -> can't stub it -> dropped
    assert direction_b_exchanges([_b_request("can_use_tool", "b1")]) == {}


def test_direction_b_error_envelope_marked_not_succeeded():
    tape = [_b_request("hook_callback", "b1"), _b_decision("b1", {}, envelope="error")]
    ex = direction_b_exchanges(tape)["hook_callback"][0]
    assert ex.succeeded is False


def test_direction_b_ignores_direction_a_exchanges():
    # _tape() has Direction-A (initialize/mcp_status: outbound request + inbound
    # response) and one Direction-B mcp_message read with NO outbound response.
    # None should surface as a Direction-B exchange.
    assert direction_b_exchanges(_tape()) == {}


def test_direction_b_empty_tape():
    assert direction_b_exchanges([]) == {}


# --- Against the real recorded fixtures (decisions must survive intact) ---

_PERMISSION = Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
_WEBSEARCH = Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl"


def test_direction_b_real_permission_fixture_has_both_decision_shapes():
    by = direction_b_exchanges(load_tape(_PERMISSION))
    assert set(by) == {"can_use_tool"}
    allow, deny = by["can_use_tool"]
    assert [allow.decision["behavior"], deny.decision["behavior"]] == ["allow", "deny"]
    assert "updatedInput" in allow.decision        # the redirect decision survived the scrub
    assert deny.decision.get("message")            # the deny reason survived the scrub


def test_direction_b_real_websearch_fixture_counts():
    by = direction_b_exchanges(load_tape(_WEBSEARCH))
    assert {k: len(v) for k, v in by.items()} == {"mcp_message": 20, "hook_callback": 3}


# --- Direction-B read-view: keep inbound control_requests, drop only control_response ---


def test_direction_b_read_frames_keeps_control_requests_drops_responses():
    frames = direction_b_read_frames(_tape())
    types = [f["type"] for f in frames]
    assert "control_request" in types   # Direction-B request kept (SDK must receive it)
    assert "control_response" not in types  # Direction-A answer delivered out-of-band
    assert "assistant" in types and "result" in types  # conversation kept


def test_direction_b_read_frames_vs_conversation_frames_differ_only_on_requests():
    # The two views agree on conversation but disagree on control_requests: the
    # Direction-B view keeps them, the inert view drops them.
    b = direction_b_read_frames(_tape())
    inert = conversation_frames(_tape())
    assert [f["type"] for f in b if f["type"] == "control_request"]  # B keeps
    assert not [f["type"] for f in inert if f["type"] == "control_request"]  # inert drops

    # same conversation frames in both
    def conv_types(frames):
        return [f["type"] for f in frames if f["type"] not in ("control_request", "control_response")]

    assert conv_types(b) == conv_types(inert)


def test_direction_b_read_frames_on_real_websearch_keeps_23_requests():
    frames = direction_b_read_frames(load_tape(_WEBSEARCH))
    n_requests = sum(1 for f in frames if f.get("type") == "control_request")
    n_responses = sum(1 for f in frames if f.get("type") == "control_response")
    assert n_requests == 23 and n_responses == 0


def test_direction_b_read_frames_empty_tape():
    assert direction_b_read_frames([]) == []


def test_conversation_frames_is_conversation_only():
    # regression: conversation_frames must drop ALL control frames (it once kept
    # control_requests, contradicting its name) — it's a synonym of conversation_frames.
    convo = conversation_frames(load_tape(_WEBSEARCH))
    types = {f.get("type") for f in convo}
    assert "control_request" not in types and "control_response" not in types
    assert convo == conversation_frames(load_tape(_WEBSEARCH))
