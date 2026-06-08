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

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from claude_agent_cassette import ReplayTransport
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


async def test_initialize_answered_from_recorded_response():
    """connect() completes from the recorded control_response (not generic success)."""
    tape = load_tape(_TAPE)
    transport = ReplayTransport.from_tape(tape)
    # The fixture records 3 control_responses; the bare client consumes the first
    # (initialize). Pre-condition: they were actually loaded.
    assert transport._recorded_responses
    await asyncio.wait_for(_drive(transport), _DRIVE_TIMEOUT_S)

    def _is_initialize(raw: str) -> bool:
        try:
            return json.loads(raw).get("request", {}).get("subtype") == "initialize"
        except (ValueError, AttributeError):
            return False

    assert any(_is_initialize(w) for w in transport.writes)
    assert transport._response_idx >= 1  # at least the initialize response was used


async def test_from_tape_drops_direction_b_control_requests():
    """mcp_message/hook_callback inbound frames must NOT be replayed to the SDK."""
    tape = load_tape(_TAPE)
    transport = ReplayTransport.from_tape(tape)
    kinds = {m.get("type") for m in transport._messages}
    assert "control_request" not in kinds  # Direction B dropped
    assert "control_response" not in kinds  # answered separately, not streamed
    # but the conversation survived
    assert "assistant" in kinds and "result" in kinds


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
    tape = [{"dir": "read", "frame": _CAN_USE_TOOL}, {"dir": "read", "frame": _RESULT}]
    opts, fired = await make_options()
    await asyncio.wait_for(drive(ReplayTransport.from_tape(tape), opts), _DRIVE_TIMEOUT_S)
    assert fired == [], "from_tape must drop Direction-B so no live callback fires"
