"""Field-level drift: unmodeled_fields (the absolute set) + field_drift (the gate).

What production bug each test prevents: a new CLI wire field the installed SDK
silently discards must FAIL the gate (data your tooling should react to is
vanishing); fields the SDK *keeps* — passthrough subtrees like tool input, or a
wholesale-retained frame like SystemMessage.data — must NOT be flagged (false
positives would train consumers to ignore the gate). No exact-set assertions
against real fixtures: the unmodeled set legitimately differs across the SDK
matrix; only controlled synthetic injections are asserted.
"""
from __future__ import annotations

import copy
from pathlib import Path

from claude_agent_cassette import (
    conversation_frames,
    field_drift,
    load_tape,
    parse_drift,
    unmodeled_fields,
)

_MCP = Path(__file__).parent.parent / "examples" / "cassettes" / "mcp_session.jsonl"


def _frames():
    return conversation_frames(load_tape(_MCP))


def _with_assistant_field(frames, key, value):
    out = copy.deepcopy(frames)
    assistant = next(f for f in out if f.get("type") == "assistant")
    assistant["message"][key] = value
    return out


def test_synthetic_extra_field_is_flagged():
    """The issue-#9 acceptance test: a field the installed SDK doesn't model."""
    mutated = _with_assistant_field(_frames(), "shiny_new_field", {"x": 1})
    assert "assistant message.shiny_new_field" in unmodeled_fields(mutated)


def test_field_drift_clean_against_own_baseline():
    frames = _frames()
    assert field_drift(frames, unmodeled_fields(frames)) == []


def test_field_drift_reports_only_new_keys_with_first_index():
    frames = _frames()
    baseline = unmodeled_fields(frames)
    mutated = _with_assistant_field(frames, "shiny_new_field", True)
    findings = field_drift(mutated, baseline)
    assert len(findings) == 1
    f = findings[0]
    assert f.reason == "unmodeled_field"
    assert f.frame_type == "assistant"
    assert "message.shiny_new_field" in f.detail
    assert mutated[f.frame_index].get("type") == "assistant"  # points at the evidence


def test_stale_baseline_entries_are_not_findings():
    """SDK now models MORE than the baseline recorded — that's progress, not drift."""
    frames = _frames()
    baseline = unmodeled_fields(frames) + ["assistant message.gone_in_new_sdk"]
    assert field_drift(frames, baseline) == []


def test_retained_tool_input_subtree_not_flagged():
    """ToolUseBlock keeps the raw input dict — extra keys inside it are preserved,
    not dropped, so flagging them would be a false positive."""
    frames = copy.deepcopy(_frames())
    injected = False
    for frame in frames:
        if frame.get("type") != "assistant":
            continue
        for block in frame["message"].get("content", []):
            if block.get("type") == "tool_use":
                block["input"]["extra_inner_key"] = "kept-wholesale"
                injected = True
    assert injected
    assert "assistant message.content.[].input.extra_inner_key" not in unmodeled_fields(frames)


def test_wholesale_retained_system_frame_not_flagged():
    """SystemMessage keeps the whole frame as .data — nothing in it is dropped."""
    frames = copy.deepcopy(_frames())
    system = next(f for f in frames if f.get("type") == "system")
    system["brand_new_system_key"] = True
    assert all("brand_new_system_key" not in key for key in unmodeled_fields(frames))


def test_list_indices_collapse_to_one_baseline_entry():
    """The same field on block 0 and block 3 is one schema fact, not four entries."""
    frames = copy.deepcopy(_frames())
    for frame in frames:
        if frame.get("type") == "assistant":
            for block in frame["message"].get("content", []):
                block["per_block_novelty"] = 1
    keys = [k for k in unmodeled_fields(frames) if "per_block_novelty" in k]
    assert keys == ["assistant message.content.[].per_block_novelty"]


def test_parse_failing_frame_is_skipped_not_double_reported():
    """Composition contract: parse_drift owns broken frames; the field layer skips them."""
    broken = [{"type": "assistant", "message": ["not", "a", "dict"], "novel_field": 1}]
    assert unmodeled_fields(broken) == []  # no field findings for it...
    assert [f.reason for f in parse_drift(broken)] == ["parse_error"]  # ...parse_drift reports it


def test_unmodeled_fields_is_sorted_and_deduplicated():
    frames = _frames() + _frames()  # duplicate frames must not duplicate entries
    keys = unmodeled_fields(frames)
    assert keys == sorted(set(keys))
