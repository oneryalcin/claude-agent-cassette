"""RecordingTransport tees the full duplex wire; record -> replay round-trips."""

from __future__ import annotations

import asyncio
import json

from claude_agent_cassette import (
    RecordingTransport,
    ReplayTransport,
    record_sdk_wire,
    replay,
    serialize_tape,
)
from claude_agent_cassette.tape import conversation_messages


def test_record_sdk_wire_intercepts_both_query_and_client_paths():
    """record_sdk_wire must wrap the transport for BOTH reach-the-transport paths.

    ClaudeSDKClient._connect_inner does a call-time import from the source module;
    one-shot query()/InternalClient.process_query uses the name bound in
    _internal.client. Patching one silently misses the other (it did, once).
    Construction here does not spawn the CLI, so no API key is needed.
    """
    from claude_agent_sdk import ClaudeAgentOptions

    with record_sdk_wire():
        # ClaudeSDKClient path: call-time import from the source module
        from claude_agent_sdk._internal.transport.subprocess_cli import (
            SubprocessCLITransport as ViaSource,
        )
        import claude_agent_sdk._internal.client as client_mod

        via_client_path = ViaSource(prompt="probe", options=ClaudeAgentOptions())
        via_query_path = client_mod.SubprocessCLITransport(prompt="probe", options=ClaudeAgentOptions())
        assert isinstance(via_client_path, RecordingTransport)  # ClaudeSDKClient
        assert isinstance(via_query_path, RecordingTransport)  # query()

    # constructor restored on exit
    from claude_agent_sdk._internal.transport.subprocess_cli import (
        SubprocessCLITransport as AfterExit,
    )

    assert AfterExit.__name__ == "SubprocessCLITransport"

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
