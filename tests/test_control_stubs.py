"""Direction-B ``can_use_tool`` stub — replay recorded permission decisions (piece 3).

Unit tests drive the stub directly; the integration test installs it in a real
``ClaudeSDKClient`` over a read-view that keeps the recorded ``can_use_tool``
requests (``direction_b_read_frames``), so the SDK's real control machinery invokes
the stub and the recorded decisions flow back — proving the mechanism end to end
without yet needing the ``from_tape`` Direction-B wiring (piece 4).
"""
from __future__ import annotations

import asyncio
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
    build_permission_stub,
    direction_b_exchanges,
    direction_b_read_frames,
    load_tape,
)

_PERMISSION = Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
_TIMEOUT_S = 20

# The stub never reads the permission context, so a placeholder is fine here.
_CTX = None


def _tape():
    return load_tape(_PERMISSION)


def _exchanges():
    return direction_b_exchanges(_tape())["can_use_tool"]


# --- Unit: the stub reconstructs the recorded decision for a matching request ---


async def test_stub_replays_recorded_allow_with_updated_input():
    allow_ex, _deny = _exchanges()
    stub = build_permission_stub(_tape())
    result = await stub(allow_ex.request["tool_name"], allow_ex.request["input"], _CTX)
    assert isinstance(result, PermissionResultAllow)
    assert result.updated_input == allow_ex.decision["updatedInput"]


async def test_stub_replays_recorded_deny_with_message():
    _allow, deny_ex = _exchanges()
    stub = build_permission_stub(_tape())
    result = await stub(deny_ex.request["tool_name"], deny_ex.request["input"], _CTX)
    assert isinstance(result, PermissionResultDeny)
    assert result.message == deny_ex.decision["message"]


# --- Fail-closed: never invent or reuse a decision ---


async def test_stub_fails_closed_on_unrecorded_request():
    stub = build_permission_stub(_tape())
    with pytest.raises(CassetteMismatchError):
        await stub("Bash", {"command": "echo never recorded"}, _CTX)


async def test_stub_fails_closed_when_decisions_exhausted():
    exchanges = _exchanges()
    stub = build_permission_stub(_tape())
    for ex in exchanges:  # consume every recorded decision
        await stub(ex.request["tool_name"], ex.request["input"], _CTX)
    # the same request again has nothing left -> divergence, not a silent re-use
    with pytest.raises(CassetteMismatchError):
        await stub(exchanges[0].request["tool_name"], exchanges[0].request["input"], _CTX)


# --- Integration: the stub drives a real client to completion with recorded decisions ---


async def test_stub_drives_real_client_end_to_end():
    tape = _tape()
    base = build_permission_stub(tape)
    fired: list[tuple[str, str]] = []

    async def spy(tool_name, tool_input, context):
        result = await base(tool_name, tool_input, context)
        fired.append((tool_name, type(result).__name__))
        return result

    async def drive():
        client = ClaudeSDKClient(
            options=ClaudeAgentOptions(can_use_tool=spy),
            transport=ReplayTransport(direction_b_read_frames(tape)),
        )
        await client.connect()
        types: list[str] = []
        async for message in client.receive_messages():
            types.append(type(message).__name__)
            if type(message).__name__ == "ResultMessage":
                break
        await client.disconnect()
        return types

    types = await asyncio.wait_for(drive(), _TIMEOUT_S)
    assert types[-1] == "ResultMessage"
    # the recorded can_use_tool requests were answered, in order, from the tape
    assert fired == [("Write", "PermissionResultAllow"), ("Write", "PermissionResultDeny")]
