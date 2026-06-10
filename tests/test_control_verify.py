"""Direction-B verify mode: the consumer's REAL callbacks run, decisions diffed at the wire.

``mode="verify"`` is the complement of ``mode="stub"``: instead of replaying recorded
decisions through stubs (certifying the wire), it delivers the recorded requests to the
consumer's real ``can_use_tool`` / ``hooks`` and diffs their answers against the recording
(certifying the *policy*). The diff is wire-to-wire — the SDK converts the live result via
its real conversion path and the comparison matches by ``request_id`` — so these tests pin
exactly the divergences a policy regression produces: a changed decision, a callback that
now raises, a hook structure that no longer matches.
"""
from __future__ import annotations

import asyncio
import json
import posixpath
from pathlib import Path

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookMatcher,
    PermissionResultAllow,
    PermissionResultDeny,
)

from claude_agent_cassette import (
    CassetteMismatchError,
    ControlReplayLedger,
    control_verify_options,
    load_tape,
    replay_tape,
    verify_direction_b_decisions,
)

_PERMISSION = Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
_HOOKS = Path(__file__).parent.parent / "examples" / "cassettes" / "hooks_session.jsonl"
_WEBSEARCH = Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl"
_TIMEOUT_S = 20

# The recorded hook output (see examples/record_hooks_session.py).
_RECORDED_HOOK_OUTPUT = {
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "permissionDecisionReason": "recorded-hook-approved",
    }
}


def _recorded_permission_policy():
    """The policy the permission fixture was recorded with (record_permission_session.py)."""

    async def can_use_tool(tool_name, tool_input, context):
        path = tool_input.get("file_path", "")
        if path.startswith("/etc/"):
            return PermissionResultDeny(message=f"Refusing to write to system path: {path}")
        return PermissionResultAllow(
            updated_input={**tool_input, "file_path": "./safe_output/" + posixpath.basename(path)}
        )

    return can_use_tool


def _hook_options(*outputs, raising=False):
    """Options registering one PreToolUse/Bash hook per output (mirrors the recording)."""

    def make(output):
        async def hook(input_data, tool_use_id, context):
            if raising:
                raise RuntimeError("hook policy exploded")
            return output

        return hook

    return ClaudeAgentOptions(
        hooks={"PreToolUse": [HookMatcher(matcher="Bash", hooks=[make(o) for o in outputs])]}
    )


async def _drive(tape, options):
    async with replay_tape(tape, options=options, mode="verify") as client:
        async for message in client.receive_messages():
            if type(message).__name__ == "ResultMessage":
                break


# --- End-to-end: a policy matching the recording verifies green; a regression raises ---


async def test_verify_green_when_policy_reproduces_recorded_decisions():
    options = ClaudeAgentOptions(can_use_tool=_recorded_permission_policy())
    await asyncio.wait_for(_drive(load_tape(_PERMISSION), options), _TIMEOUT_S)


async def test_verify_green_when_hook_reproduces_recorded_output():
    await asyncio.wait_for(
        _drive(load_tape(_HOOKS), _hook_options(_RECORDED_HOOK_OUTPUT)), _TIMEOUT_S
    )


async def test_verify_raises_when_permission_policy_changed():
    """The recorded deny is now an allow — the policy regressed; verify must fail."""

    async def allow_everything(tool_name, tool_input, context):
        return PermissionResultAllow()

    with pytest.raises(CassetteMismatchError, match="diverged from the recording"):
        await asyncio.wait_for(
            _drive(load_tape(_PERMISSION), ClaudeAgentOptions(can_use_tool=allow_everything)),
            _TIMEOUT_S,
        )


async def test_verify_raises_when_hook_now_errors():
    """A callback that now raises is swallowed by the SDK into an error envelope —
    verify must still surface it (the fail-open class of bug, verify-mode edition)."""
    with pytest.raises(CassetteMismatchError, match="produced an error"):
        await asyncio.wait_for(
            _drive(load_tape(_HOOKS), _hook_options(None, raising=True)), _TIMEOUT_S
        )


async def test_verify_raises_when_hook_structure_differs():
    """An extra registered hook shifts the live callback ids off the recording."""
    two_hooks = _hook_options(_RECORDED_HOOK_OUTPUT, _RECORDED_HOOK_OUTPUT)
    with pytest.raises(CassetteMismatchError, match="callback ids"):
        await asyncio.wait_for(_drive(load_tape(_HOOKS), two_hooks), _TIMEOUT_S)


# --- Preconditions: verify needs the consumer's real handlers; unsupported fails closed ---


def test_verify_requires_can_use_tool_callback():
    with pytest.raises(CassetteMismatchError, match="options.can_use_tool"):
        control_verify_options(load_tape(_PERMISSION), ClaudeAgentOptions())


def test_verify_requires_hooks():
    with pytest.raises(CassetteMismatchError, match="options.hooks"):
        control_verify_options(load_tape(_HOOKS), ClaudeAgentOptions())


def test_verify_fails_closed_on_unsupported_subtype():
    with pytest.raises(CassetteMismatchError, match="mcp_message"):
        control_verify_options(load_tape(_WEBSEARCH), ClaudeAgentOptions())


async def test_verify_tape_without_direction_b_needs_no_callbacks():
    """A conversation-only tape verifies trivially — no handlers required."""

    def keep(entry):
        if entry.get("dir") == "read":
            return (entry.get("frame") or {}).get("type") != "control_request"
        payload = json.loads(entry["data"])
        return payload.get("type") != "control_response"

    stripped = [e for e in load_tape(_PERMISSION) if keep(e)]
    await asyncio.wait_for(_drive(stripped, ClaudeAgentOptions()), _TIMEOUT_S)


# --- Unit: the wire-to-wire comparator (cases not reachable deterministically e2e) ---


def _exchange_tape(decision, envelope="success"):
    return [
        {"dir": "read", "frame": {"type": "control_request", "request_id": "r1", "request": {
            "subtype": "can_use_tool", "tool_name": "Write", "input": {"file_path": "x"}}}},
        {"dir": "write", "data": json.dumps({"type": "control_response", "response": {
            "subtype": envelope, "request_id": "r1",
            **({"response": decision} if envelope == "success" else {"error": "boom"})}})},
    ]


def _live_write(decision, envelope="success"):
    return json.dumps({"type": "control_response", "response": {
        "subtype": envelope, "request_id": "r1",
        **({"response": decision} if envelope == "success" else {"error": "live boom"})}})


def test_verify_decisions_unanswered_exchange_is_divergence():
    """A recorded exchange the live side never answered must not certify green."""
    ledger = ControlReplayLedger()
    verify_direction_b_decisions([], _exchange_tape({"behavior": "allow"}), ledger)
    with pytest.raises(CassetteMismatchError, match="never answered"):
        ledger.raise_if_diverged()


def test_verify_decisions_matching_error_envelopes_are_not_divergence():
    """Recorded error + live error = the callback still raises here; exception text
    is not part of the contract."""
    ledger = ControlReplayLedger()
    tape = _exchange_tape(None, envelope="error")
    verify_direction_b_decisions([_live_write(None, envelope="error")], tape, ledger)
    assert not ledger.diverged()


def test_verify_decisions_equal_decision_is_clean():
    ledger = ControlReplayLedger()
    tape = _exchange_tape({"behavior": "deny", "message": "no"})
    verify_direction_b_decisions(
        [_live_write({"behavior": "deny", "message": "no"})], tape, ledger
    )
    assert not ledger.diverged()
