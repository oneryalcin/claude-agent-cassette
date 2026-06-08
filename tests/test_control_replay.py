"""Control-aware replay (Direction A) from a full duplex tape.

Drives a real ``ClaudeSDKClient`` over ``ReplayTransport.from_tape`` against a
recorded (PII-scrubbed) web-search session whose wire includes the control plane
(initialize + 20 mcp_message + hook_callback exchanges). Proves:

- the SDK's ``initialize`` handshake completes from the *recorded* control_response
  (id-remapped), not a synthesised one;
- inbound Direction-B control_requests (mcp_message/hook_callback) are dropped, so
  no live MCP/hook callback fires and the SDK never errors on a missing handler;
- the conversation replays through the real parser to its terminal ``ResultMessage``.

The fixture is a scrubbed real capture (see tests/fixtures/); only the control
*structure* is load-bearing here, never its (blanked) content.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from claude_agent_cassette import CassetteMismatchError, ReplayTransport
from claude_agent_cassette.tape import load_tape

_TAPE = Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl"
_DRIVE_TIMEOUT_S = 20

# The typed-message sequence the real parser yields for this recording.
_EXPECTED_TYPES = [
    "HookEventMessage", "HookEventMessage", "SystemMessage",
    "AssistantMessage", "AssistantMessage", "TaskStartedMessage",
    "TaskNotificationMessage", "UserMessage", "AssistantMessage", "ResultMessage",
]


async def _drive(transport) -> list[str]:
    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    await client.connect()
    types: list[str] = []
    async for message in client.receive_messages():
        types.append(type(message).__name__)
        if type(message).__name__ == "ResultMessage":
            break
    await client.disconnect()
    return types


async def test_from_tape_replays_full_recording_through_real_parser():
    tape = load_tape(_TAPE)
    transport = ReplayTransport.from_tape(tape)
    types = await asyncio.wait_for(_drive(transport), _DRIVE_TIMEOUT_S)
    assert types == _EXPECTED_TYPES


# (The tape-shaping invariants — Direction-B dropped, responses correlated by
# subtype, body fidelity — are covered as pure-data tests in test_tape.py. Here
# we only assert end-to-end behaviour through a real client.)


# Minimal frames for the inertness test (synthetic — the mechanism is identical
# for the tape's mcp_message/hook_callback, but can_use_tool is the cheapest
# Direction-B request to register a callback for).
_CAN_USE_TOOL = {
    "type": "control_request", "request_id": "rec_1",
    "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": {"command": "x"}},
}
_RESULT = {
    "type": "result", "subtype": "success", "duration_ms": 1, "duration_api_ms": 1,
    "is_error": False, "num_turns": 1, "session_id": "s1", "result": "done",
}


async def test_direction_b_drop_keeps_replay_inert_for_registered_callbacks():
    """The payoff of dropping Direction B: a registered permission/hook/MCP callback
    must NOT fire during replay. Bare clients tolerate Direction-B frames (errors
    swallowed in side-tasks), so this only bites when a consumer registers callbacks
    — exactly the task-service adapter case. Proven both ways below."""
    from claude_agent_sdk import PermissionResultAllow

    async def make_options():
        fired: list[str] = []

        async def can_use_tool(tool_name, tool_input, context):
            fired.append(tool_name)
            return PermissionResultAllow()

        return ClaudeAgentOptions(can_use_tool=can_use_tool), fired

    async def drive(transport, options):
        client = ClaudeSDKClient(options=options, transport=transport)
        await client.connect()
        async for m in client.receive_messages():
            if type(m).__name__ == "ResultMessage":
                break
        await client.disconnect()

    # WITHOUT the drop: plain replay streams the can_use_tool frame -> callback fires.
    opts, fired = await make_options()
    await asyncio.wait_for(drive(ReplayTransport([_CAN_USE_TOOL, _RESULT]), opts), _DRIVE_TIMEOUT_S)
    assert fired == ["Bash"], "control: callback should fire when Direction-B is replayed"

    # WITH from_tape: the can_use_tool frame is dropped -> callback never fires (inert).
    # (A valid initialize exchange is required now that tape mode is fail-closed.)
    init_w, init_r = _init_pair()
    tape = [init_w, init_r,
            {"dir": "read", "frame": _CAN_USE_TOOL}, {"dir": "read", "frame": _RESULT}]
    opts, fired = await make_options()
    await asyncio.wait_for(drive(ReplayTransport.from_tape(tape), opts), _DRIVE_TIMEOUT_S)
    assert fired == [], "from_tape must drop Direction-B so no live callback fires"


# --- Fail-closed: tape mode must surface divergence, never absorb it silently ---


def _init_pair(subtype: str = "success", body: dict | None = None):
    """A recorded (initialize request write, control_response read) pair."""
    write = {"dir": "write", "data": json.dumps(
        {"type": "control_request", "request_id": "rec_init",
         "request": {"subtype": "initialize"}})}
    read = {"dir": "read", "frame": {"type": "control_response", "response": {
        "subtype": subtype, "request_id": "rec_init", "response": body or {}}}}
    return write, read


async def test_empty_tape_fails_closed():
    """from_tape([]) has no recorded initialize response -> connect() must raise,
    not synthesise success (that would be fail-open — the drift a cassette catches)."""
    transport = ReplayTransport.from_tape([])
    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    with pytest.raises(CassetteMismatchError):
        await asyncio.wait_for(client.connect(), _DRIVE_TIMEOUT_S)


async def test_missing_initialize_response_fails_closed():
    """A tape with conversation but no recorded initialize exchange -> fail-closed."""
    tape = [{"dir": "read", "frame": _RESULT}]
    transport = ReplayTransport.from_tape(tape)
    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    with pytest.raises(CassetteMismatchError):
        await asyncio.wait_for(client.connect(), _DRIVE_TIMEOUT_S)


async def test_error_subtype_recorded_response_is_replayed_faithfully():
    """A recorded error-subtype initialize response is replayed as-is; the SDK
    raises on it (faithful replay of a failed handshake, not swallowed)."""
    write, read = _init_pair(subtype="error")
    read["frame"]["response"]["error"] = "recorded handshake failure"
    transport = ReplayTransport.from_tape([write, read])
    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    with pytest.raises(Exception) as exc:  # SDK raises (not CassetteMismatchError)
        await asyncio.wait_for(client.connect(), _DRIVE_TIMEOUT_S)
    assert not isinstance(exc.value, CassetteMismatchError)


async def test_legacy_messages_path_still_synthesises_success():
    """Backward compat: ReplayTransport(messages) (no tape) keeps generic-success;
    an empty message list connects and ends cleanly, never fail-closed."""
    transport = ReplayTransport([_RESULT])
    types = await asyncio.wait_for(_drive(transport), _DRIVE_TIMEOUT_S)
    assert types == ["ResultMessage"]


# --- Flow control: the supported pattern (drain, optionally with concurrent
# control) must work even for tapes larger than the SDK's inbound buffer (~100).
# A control call issued WITHOUT a concurrent drain back-pressures and hangs, the
# same as the real CLI with an undrained stdout — that misuse is documented, not
# tested here. ---

_ASSISTANT = {"type": "assistant", "message": {
    "role": "assistant", "content": [{"type": "text", "text": "x"}], "model": "m"},
    "session_id": "s"}


def _big_tape():
    """initialize + 101 conversation frames (> the SDK buffer) + a recorded
    mcp_status response + terminal result."""
    w, r = _init_pair()
    mcp_w = {"dir": "write", "data": json.dumps(
        {"type": "control_request", "request_id": "rm",
         "request": {"subtype": "mcp_status"}})}
    mcp_r = {"dir": "read", "frame": {"type": "control_response", "response": {
        "subtype": "success", "request_id": "rm", "response": {"servers": []}}}}
    convo = [{"dir": "read", "frame": _ASSISTANT}] * 101 + [{"dir": "read", "frame": _RESULT}]
    return [w, r, *convo, mcp_w, mcp_r]


async def test_large_tape_replays_when_drained():
    """A tape larger than the SDK inbound buffer replays to completion when drained."""
    transport = ReplayTransport.from_tape(_big_tape())
    types = await asyncio.wait_for(_drive(transport), _DRIVE_TIMEOUT_S)
    assert types[-1] == "ResultMessage"
    assert types.count("AssistantMessage") == 101


async def test_control_call_resolves_while_draining_large_tape():
    """get_mcp_status() resolves mid-replay as long as messages are being drained
    concurrently — the recorded mcp_status answer is delivered, no deadlock."""
    transport = ReplayTransport.from_tape(_big_tape())
    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    await asyncio.wait_for(client.connect(), _DRIVE_TIMEOUT_S)

    async def drain():
        async for m in client.receive_messages():
            if type(m).__name__ == "ResultMessage":
                break

    drainer = asyncio.create_task(drain())
    await asyncio.wait_for(client.get_mcp_status(), _DRIVE_TIMEOUT_S)  # must not hang
    await asyncio.wait_for(drainer, _DRIVE_TIMEOUT_S)
    await client.disconnect()
