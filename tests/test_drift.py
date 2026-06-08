"""Drift detection — re-parse cassette frames through the installed SDK.

Includes a contract test pinning the SDK ``parse_message`` behaviour the detector
relies on (the SDK's own docstring mis-states it), so a future SDK bump that flips
None-vs-raise turns *this* test red instead of silently mis-classifying drift.
"""
from __future__ import annotations

from pathlib import Path

from claude_agent_sdk._internal.message_parser import parse_message

from claude_agent_cassette import check_tape, parse_drift
from claude_agent_cassette.tape import load_tape

_TAPE = Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl"

_VALID_ASSISTANT = {
    "type": "assistant",
    "message": {"role": "assistant", "content": [{"type": "text", "text": "x"}], "model": "m"},
    "session_id": "s",
}


# --- contract: the undocumented parse_message behaviour the detector keys off ---

def test_parse_message_returns_none_for_unknown_type():
    assert parse_message({"type": "definitely_not_a_real_type"}) is None


def test_parse_message_raises_for_known_type_missing_field():
    import pytest
    with pytest.raises(Exception):
        parse_message({"type": "result", "subtype": "success"})  # missing duration_ms etc.


def test_parse_message_can_raise_non_messageparseerror():
    """A malformed-but-present field escapes as TypeError, NOT MessageParseError —
    which is exactly why parse_drift must catch broadly."""
    import pytest
    with pytest.raises(Exception) as exc:
        parse_message({"type": "assistant", "message": []})  # message should be a dict
    assert exc.type.__name__ != "MessageParseError"


# --- the detector ---

def test_clean_fixture_has_no_drift():
    assert check_tape(load_tape(_TAPE)) == []


def test_renamed_type_is_unrecognized_drift():
    findings = parse_drift([dict(_VALID_ASSISTANT, type="assistant_v2")])
    assert [f.reason for f in findings] == ["unrecognized_type"]
    assert findings[0].frame_type == "assistant_v2"


def test_shape_break_is_parse_error_drift():
    findings = parse_drift([{"type": "result", "subtype": "success"}])  # missing required fields
    assert [f.reason for f in findings] == ["parse_error"]


def test_malformed_field_is_reported_not_crashed():
    """The bug the broad except guards: a TypeError-raising frame must become a
    finding, not propagate out of parse_drift."""
    findings = parse_drift([{"type": "assistant", "message": []}])
    assert len(findings) == 1
    assert findings[0].reason == "parse_error"
    assert "TypeError" in findings[0].detail


def test_valid_frame_has_no_finding():
    assert parse_drift([_VALID_ASSISTANT]) == []


def test_findings_carry_frame_index():
    frames = [_VALID_ASSISTANT, {"type": "x"}, _VALID_ASSISTANT, {"type": "result"}]
    findings = parse_drift(frames)
    assert [f.frame_index for f in findings] == [1, 3]


# --- content-block drift: the SDK parser silently drops unknown blocks ---

def test_unknown_assistant_content_block_is_content_dropped():
    frame = {"type": "assistant", "session_id": "s",
             "message": {"model": "m", "content": [{"type": "new_block", "payload": 1}]}}
    findings = parse_drift([frame])
    assert [f.reason for f in findings] == ["content_dropped"]
    assert "new_block" in findings[0].detail


def test_partial_content_drop_is_flagged():
    frame = {"type": "assistant", "session_id": "s", "message": {"model": "m", "content": [
        {"type": "text", "text": "kept"}, {"type": "new_block"}]}}
    findings = parse_drift([frame])
    assert [f.reason for f in findings] == ["content_dropped"]
    assert "1 of 2" in findings[0].detail


def test_valid_content_blocks_no_drift():
    frame = {"type": "assistant", "session_id": "s",
             "message": {"model": "m", "content": [{"type": "text", "text": "x"}]}}
    assert parse_drift([frame]) == []


def test_unknown_user_content_block_is_content_dropped():
    frame = {"type": "user", "session_id": "s", "parent_tool_use_id": None,
             "message": {"role": "user", "content": [{"type": "weird_block"}]}}
    findings = parse_drift([frame])
    assert [f.reason for f in findings] == ["content_dropped"]


def test_string_content_is_not_block_checked():
    """User content can be a plain string (no blocks) — must not false-positive."""
    frame = {"type": "user", "session_id": "s", "parent_tool_use_id": None,
             "message": {"role": "user", "content": "just a string"}}
    assert parse_drift([frame]) == []


def test_check_tape_excludes_control_frames():
    """Control frames return None from parse_message; check_tape must not flag them
    (they are excluded via replayable_messages, not message-parsed)."""
    tape = load_tape(_TAPE)
    # sanity: the tape really does carry control frames that would be None
    assert any(
        e.get("dir") == "read" and e["frame"].get("type") == "control_response"
        for e in tape
    )
    assert check_tape(tape) == []  # yet no drift, because control frames are excluded
