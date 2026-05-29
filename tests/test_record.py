"""RecordingTransport tees the full duplex wire; record -> replay round-trips."""

from __future__ import annotations

import asyncio
import json

from claude_agent_cassette import RecordingTransport, ReplayTransport, replay, serialize_tape
from claude_agent_cassette.tape import conversation_messages

SESSION = [
    {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}], "model": "m"}, "session_id": "s1"},
    {"type": "result", "subtype": "success", "duration_ms": 1, "duration_api_ms": 1, "is_error": False, "num_turns": 1, "session_id": "s1", "result": "done"},
]


async def _drive(transport):
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

    client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
    await client.connect()
    kinds = []
    async for msg in client.receive_messages():
        kinds.append(type(msg).__name__)
        if type(msg).__name__ == "ResultMessage":
            break
    await client.disconnect()
    return kinds


async def test_recording_transport_tees_both_directions():
    tape: list[dict] = []
    kinds = await asyncio.wait_for(_drive(RecordingTransport(ReplayTransport(SESSION), tape)), 15)
    assert kinds == ["AssistantMessage", "ResultMessage"]

    dirs = {e["dir"] for e in tape}
    assert dirs == {"read", "write"}  # both directions captured
    # outbound includes the initialize control_request...
    assert any(
        e["dir"] == "write" and json.loads(e["data"]).get("request", {}).get("subtype") == "initialize"
        for e in tape
    )
    # ...inbound includes the control_response (control plane) + conversation.
    read_types = [e["frame"].get("type") for e in tape if e["dir"] == "read"]
    assert "control_response" in read_types and "assistant" in read_types and "result" in read_types


async def test_record_then_replay_round_trip():
    tape: list[dict] = []
    await asyncio.wait_for(_drive(RecordingTransport(ReplayTransport(SESSION), tape)), 15)

    # serialize round-trips
    parsed = [json.loads(line) for line in serialize_tape(tape).splitlines() if line.strip()]
    assert parsed == tape

    # the recorded conversation (minus the synthesised handshake) replays identically
    replayed = await asyncio.wait_for(_kinds_via_replay(conversation_messages(tape)), 15)
    assert replayed == ["AssistantMessage", "ResultMessage"]


async def _kinds_via_replay(messages):
    async with replay(messages) as client:
        kinds = []
        async for msg in client.receive_messages():
            kinds.append(type(msg).__name__)
            if type(msg).__name__ == "ResultMessage":
                break
        return kinds
