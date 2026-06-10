"""Direction-B replay: stub callbacks + end-to-end fail-closed enforcement.

Two layers of tests, because divergence has to be caught in two places:

- **Unit** tests drive the stubs directly — they raise ``CassetteMismatchError`` on
  divergence (the direct-call fail-closed path).
- **End-to-end** tests drive divergence through ``replay_tape(mode="stub")`` over a real
  ``ClaudeSDKClient``. This is the path that matters: the SDK swallows a stub's exception
  into a benign error response, so the *only* way divergence surfaces to a consumer is the
  ledger check on context exit. The original implementation passed every unit test while
  being fail-**open** end-to-end; these tests pin the real guarantee.
"""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
)

from claude_agent_cassette import (
    CassetteMismatchError,
    ReplayTransport,
    direction_b_exchanges,
    lint_tape,
    load_tape,
    replay_tape,
)
from claude_agent_cassette.direction_b import (
    ControlReplayLedger,
    build_hook_stubs,
    build_permission_stub,
    control_stub_bundle,
)
from claude_agent_cassette.tape import recorded_hook_config

_PERMISSION = Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
_HOOKS = Path(__file__).parent.parent / "examples" / "cassettes" / "hooks_session.jsonl"
_WEBSEARCH = Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl"
_TIMEOUT_S = 20
_CTX = None  # the stubs never read the permission/hook context


def _perm():
    return load_tape(_PERMISSION)


def _hooks():
    return load_tape(_HOOKS)


def _perm_exchanges():
    return direction_b_exchanges(_perm())["can_use_tool"]


async def _drive(tape, mode="stub", options=None):
    """Replay to the terminal ResultMessage; raises if replay_tape surfaces divergence."""
    async with replay_tape(tape, options=options, mode=mode) as client:
        async for message in client.receive_messages():
            if type(message).__name__ == "ResultMessage":
                break


# --- Unit: stubs reconstruct the recorded decision; raise (directly) on divergence ---


async def test_permission_stub_replays_allow_with_updated_input():
    allow_ex, _deny = _perm_exchanges()
    stub = build_permission_stub(_perm(), ControlReplayLedger())
    result = await stub(allow_ex.request["tool_name"], allow_ex.request["input"], _CTX)
    assert isinstance(result, PermissionResultAllow)
    assert result.updated_input == allow_ex.decision["updatedInput"]


async def test_permission_stub_replays_deny_with_message():
    _allow, deny_ex = _perm_exchanges()
    stub = build_permission_stub(_perm(), ControlReplayLedger())
    result = await stub(deny_ex.request["tool_name"], deny_ex.request["input"], _CTX)
    assert isinstance(result, PermissionResultDeny)
    assert result.message == deny_ex.decision["message"]


async def test_permission_stub_raises_and_records_on_unrecorded_request():
    ledger = ControlReplayLedger()
    stub = build_permission_stub(_perm(), ledger)
    with pytest.raises(CassetteMismatchError):
        await stub("Bash", {"command": "echo never recorded"}, _CTX)
    assert ledger.diverged()  # also recorded for the swallowed end-to-end path


async def test_permission_stub_raises_when_exhausted():
    exchanges = _perm_exchanges()
    stub = build_permission_stub(_perm(), ControlReplayLedger())
    for ex in exchanges:
        await stub(ex.request["tool_name"], ex.request["input"], _CTX)
    with pytest.raises(CassetteMismatchError):
        await stub(exchanges[0].request["tool_name"], exchanges[0].request["input"], _CTX)


async def test_hook_stub_replays_recorded_output():
    hooks = build_hook_stubs(_hooks(), ControlReplayLedger())
    assert list(hooks) == ["PreToolUse"]
    output = await hooks["PreToolUse"][0].hooks[0]({}, None, {})
    assert output["hookSpecificOutput"]["permissionDecision"] == "allow"


async def test_hook_stub_raises_when_exhausted():
    stub = build_hook_stubs(_hooks(), ControlReplayLedger())["PreToolUse"][0].hooks[0]
    await stub({}, None, {})
    with pytest.raises(CassetteMismatchError):
        await stub({}, None, {})


def test_build_hook_stubs_none_when_no_hooks():
    assert build_hook_stubs(_perm(), ControlReplayLedger()) is None


def test_recorded_hook_config_reads_initialize_structure():
    assert recorded_hook_config(_hooks()) == {
        "PreToolUse": [{"matcher": "Bash", "hookCallbackIds": ["hook_0"]}]
    }


# --- control_stub_bundle: bundle shape, non-mutation, fail-closed on unsupported ---


def test_control_stub_options_installs_stub_and_clears_prompt_tool_name():
    base = ClaudeAgentOptions(can_use_tool=None, permission_prompt_tool_name="x")
    bundle = control_stub_bundle(_perm(), base)
    assert bundle.keep_subtypes == {"can_use_tool"}
    assert bundle.options.can_use_tool is not None  # stub installed on the copy
    assert bundle.options.permission_prompt_tool_name is None  # cleared (SDK-incompatible)
    assert base.can_use_tool is None and base.permission_prompt_tool_name == "x"  # base untouched


def test_control_stub_options_fails_closed_on_unsupported_subtype():
    # A Direction-B subtype a future SDK adds has no stub builder -> raise, not silent.
    future = [
        {"dir": "read", "frame": {"type": "control_request", "request_id": "f1",
                                  "request": {"subtype": "telepathy"}}},
        {"dir": "write", "data": json.dumps({"type": "control_response", "response": {
            "subtype": "success", "request_id": "f1", "response": {}}})},
    ]
    with pytest.raises(CassetteMismatchError, match="telepathy"):
        control_stub_bundle(future)


# --- lint_tape: lint a tape for replayability ---


def test_replay_findings_empty_for_replayable_fixtures():
    assert lint_tape(_perm()) == []
    assert lint_tape(_hooks()) == []


def test_replay_findings_flag_scrubbed_decisions_not_supported_subtypes():
    findings = lint_tape(load_tape(_WEBSEARCH))
    assert not any("not yet replayable" in f for f in findings)  # all 3 subtypes supported now
    assert any("scrubbed" in f for f in findings)  # decisions/ids scrubbed away


# --- Behavioral: stub mode delivers the requests (SDK writes decisions); inert drops them ---


def _written_decisions(writes, key):
    out = []
    for data in writes:
        try:
            frame = json.loads(data)
        except ValueError:
            continue
        if frame.get("type") == "control_response":
            decision = (frame.get("response") or {}).get("response") or {}
            if key in decision:
                out.append(decision)
    return out


async def _drive_transport(options, transport):
    client = ClaudeSDKClient(options=options, transport=transport)
    await client.connect()
    async for message in client.receive_messages():
        if type(message).__name__ == "ResultMessage":
            break
    await client.disconnect()


async def test_stub_mode_delivers_permission_requests_inert_drops_them():
    tape = _perm()
    bundle = control_stub_bundle(tape)
    transport = ReplayTransport.from_tape(tape, keep_subtypes=bundle.keep_subtypes)
    await asyncio.wait_for(_drive_transport(bundle.options, transport), _TIMEOUT_S)
    assert [d["behavior"] for d in _written_decisions(transport.writes, "behavior")] == ["allow", "deny"]

    inert = ReplayTransport.from_tape(tape)
    await asyncio.wait_for(_drive_transport(ClaudeAgentOptions(), inert), _TIMEOUT_S)
    assert _written_decisions(inert.writes, "behavior") == []


async def test_stub_mode_delivers_hook_requests_inert_drops_them():
    tape = _hooks()
    bundle = control_stub_bundle(tape)
    assert bundle.keep_subtypes == {"hook_callback"}
    transport = ReplayTransport.from_tape(tape, keep_subtypes=bundle.keep_subtypes)
    await asyncio.wait_for(_drive_transport(bundle.options, transport), _TIMEOUT_S)
    answered = _written_decisions(transport.writes, "hookSpecificOutput")
    assert len(answered) == 1 and answered[0]["hookSpecificOutput"]["permissionDecision"] == "allow"

    inert = ReplayTransport.from_tape(tape)
    await asyncio.wait_for(_drive_transport(ClaudeAgentOptions(), inert), _TIMEOUT_S)
    assert _written_decisions(inert.writes, "hookSpecificOutput") == []


# --- End-to-end through replay_tape: clean completes, divergence RAISES, inert is inert ---


async def test_replay_tape_stub_mode_clean_completes_without_false_divergence():
    await asyncio.wait_for(_drive(_perm(), "stub"), _TIMEOUT_S)
    await asyncio.wait_for(_drive(_hooks(), "stub"), _TIMEOUT_S)


async def test_replay_tape_inert_mode_leaves_consumer_callback_unused():
    fired: list[str] = []

    async def consumer(tool_name, tool_input, context):
        fired.append(tool_name)
        return PermissionResultAllow()

    await asyncio.wait_for(
        _drive(_perm(), "inert", options=ClaudeAgentOptions(can_use_tool=consumer)), _TIMEOUT_S
    )
    assert fired == []  # Direction-B dropped -> consumer callback never consulted


def _inject_unmatched_permission(tape):
    """Add a can_use_tool request with a fresh id and no recorded response (divergence)."""
    extra = {"dir": "read", "frame": {
        "type": "control_request", "request_id": "INJECTED",
        "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": {"command": "x"}}}}
    out = copy.deepcopy(tape)
    for i, entry in enumerate(out):
        frame = entry.get("frame") if entry.get("dir") == "read" else None
        if frame and frame.get("type") == "control_request" \
                and frame["request"].get("subtype") == "can_use_tool":
            out.insert(i, extra)
            return out
    raise AssertionError("no can_use_tool request to anchor injection")


async def test_replay_tape_stub_mode_raises_on_unmatched_request_end_to_end():
    """The regression test for the original fail-open bug: a live can_use_tool request
    with no recorded decision must surface as CassetteMismatchError through replay_tape,
    even though the SDK swallows the stub's raise into an error response."""
    diverged = _inject_unmatched_permission(_perm())
    with pytest.raises(CassetteMismatchError, match="Bash"):
        await asyncio.wait_for(_drive(diverged, "stub"), _TIMEOUT_S)


def _make_hook_error_envelope(tape):
    out = copy.deepcopy(tape)
    for entry in out:
        if entry.get("dir") == "write":
            frame = json.loads(entry["data"])
            resp = frame.get("response") or {}
            if frame.get("type") == "control_response" and "hookSpecificOutput" in json.dumps(resp):
                resp["subtype"] = "error"
                resp.pop("response", None)
                resp["error"] = "recorded hook failed"
                entry["data"] = json.dumps(frame)
    return out


async def test_replay_tape_stub_mode_raises_on_recorded_hook_error_envelope():
    """A recorded hook *error* must not replay as a successful empty output."""
    diverged = _make_hook_error_envelope(_hooks())
    with pytest.raises(CassetteMismatchError, match="error envelope"):
        await asyncio.wait_for(_drive(diverged, "stub"), _TIMEOUT_S)


def _scrub_hook_ids(tape):
    out = copy.deepcopy(tape)
    for entry in out:
        if entry.get("dir") == "write":
            frame = json.loads(entry["data"])
            if frame.get("type") == "control_request" and frame["request"].get("subtype") == "initialize":
                for matchers in (frame["request"].get("hooks") or {}).values():
                    for matcher in matchers:
                        matcher["hookCallbackIds"] = ["<scrubbed>"]
                entry["data"] = json.dumps(frame)
        frame = entry.get("frame") if entry.get("dir") == "read" else None
        if frame and frame.get("type") == "control_request" \
                and frame["request"].get("subtype") == "hook_callback":
            frame["request"]["callback_id"] = "<scrubbed>"
    return out


async def test_replay_tape_stub_mode_raises_when_hook_ids_not_reproduced():
    """Scrubbed/unreproducible hook ids: the wire-level initialize check must catch that
    the live SDK assigned different ids, so the hooks-never-fired case isn't silent."""
    diverged = _scrub_hook_ids(_hooks())
    with pytest.raises(CassetteMismatchError, match="callback ids"):
        await asyncio.wait_for(_drive(diverged, "stub"), _TIMEOUT_S)


async def test_replay_tape_stub_mode_clears_prompt_tool_name_so_connect_succeeds():
    """A caller passing permission_prompt_tool_name (SDK-incompatible with can_use_tool)
    must still connect: the stub install clears it on the copy."""
    await asyncio.wait_for(
        _drive(_perm(), "stub", options=ClaudeAgentOptions(permission_prompt_tool_name="x")),
        _TIMEOUT_S,
    )
