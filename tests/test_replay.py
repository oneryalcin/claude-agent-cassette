"""Replay drives a real ClaudeSDKClient through the real parser — no API key."""

from __future__ import annotations

import asyncio
import json

from claude_agent_cassette import replay

# A recorded conversation as raw stream-json frames (the wire shape the CLI emits).
# Task* messages ride the wire as type:"system" + subtype:"task_*", NOT a flat
# type:"task_notification".
SESSION = [
    {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}], "model": "m"}, "session_id": "s1"},
    {"type": "result", "subtype": "success", "duration_ms": 1, "duration_api_ms": 1, "is_error": False, "num_turns": 1, "session_id": "s1", "result": "done"},
]


async def _kinds(messages):
    async with replay(messages) as client:
        kinds = []
        async for msg in client.receive_messages():
            kinds.append(type(msg).__name__)
            if type(msg).__name__ == "ResultMessage":
                break
        return kinds


async def test_replay_yields_typed_messages_through_real_parser():
    kinds = await asyncio.wait_for(_kinds(SESSION), 15)
    assert kinds == ["AssistantMessage", "ResultMessage"]


async def test_replay_answers_initialize_handshake():
    # If the handshake weren't answered, connect() would hang. Assert the client
    # actually sent the initialize control_request (nested under "request").
    from claude_agent_cassette import ReplayTransport

    transport = ReplayTransport(SESSION)
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

    async def drive():
        client = ClaudeSDKClient(options=ClaudeAgentOptions(), transport=transport)
        await client.connect()
        async for msg in client.receive_messages():
            if type(msg).__name__ == "ResultMessage":
                break
        await client.disconnect()

    await asyncio.wait_for(drive(), 15)
    assert any(
        json.loads(w).get("request", {}).get("subtype") == "initialize"
        for w in transport.writes
        if w.strip().startswith("{")
    )
