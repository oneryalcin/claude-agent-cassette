"""Lockstep replay (issue #7): ``interrupt`` is causally ordered on the real wire.

A Stop session's terminal result is a *consequence* of the interrupt — it cannot
exist before the interrupt was issued. The demux model delivers frames
independently of the control plane, so it can produce that impossible ordering;
lockstep gates everything recorded after the ``interrupt`` write on the live
write. Each test pins one production bug: a Stop-handling state machine
exercised against orderings the real system can never produce, or a divergence
(no interrupt, wrong call, post-tape call) absorbed silently instead of failing
closed.

The fixture is a real recorded Stop session (``examples/record_stop_session.py``):
five ``stream_event`` deltas, then ``interrupt`` → success → synthetic user frame
→ ``result subtype=error_during_execution is_error=True``.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import posixpath

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
)

from claude_agent_cassette import (
    CassetteMismatchError,
    load_tape,
    replay_tape,
)

_STOP = Path(__file__).parent.parent / "examples" / "cassettes" / "stop_session.jsonl"
_TIMEOUT_S = 20


def _stop_tape():
    return load_tape(_STOP)


async def _drain(client: ClaudeSDKClient, seen: list[str]) -> None:
    async for message in client.receive_messages():
        seen.append(type(message).__name__)
        if isinstance(message, ResultMessage):
            return


async def test_result_withheld_until_live_interrupt():
    """The issue-#7 acceptance: no lockstep param — an interrupt tape auto-selects
    lockstep, and the terminal result is not delivered until interrupt() is issued."""

    async def scenario() -> list[str]:
        seen: list[str] = []
        async with replay_tape(_stop_tape()) as client:
            task = asyncio.create_task(_drain(client, seen))
            await asyncio.sleep(0.3)  # everything deliverable has long since been pulled
            assert "ResultMessage" not in seen
            assert "StreamEvent" in seen  # the pre-interrupt frames did flow
            await client.interrupt()
            await task
        return seen

    seen = await asyncio.wait_for(scenario(), _TIMEOUT_S)
    assert seen[-1] == "ResultMessage"


async def test_interrupt_replay_preserves_terminal_classification():
    """The DES-5724 shape survives replay: interrupt mid-stream from within the
    receive loop, then a terminal error_during_execution result — in that order."""

    async def scenario():
        order: list[str] = []
        events = 0
        result = None
        async with replay_tape(_stop_tape()) as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "StreamEvent":
                    events += 1
                    if events == 5:
                        await client.interrupt()
                        order.append("interrupt-resolved")
                if isinstance(message, ResultMessage):
                    order.append("result")
                    result = message
                    break
        return order, result

    order, result = await asyncio.wait_for(scenario(), _TIMEOUT_S)
    assert order == ["interrupt-resolved", "result"]
    assert result is not None
    assert result.subtype == "error_during_execution"
    assert result.is_error is True


async def test_consumer_that_never_interrupts_fails_closed():
    """An interrupt tape replayed by a consumer that never calls interrupt() must
    not hang (or worse, silently complete) — the sync wait times out loudly."""

    async def scenario() -> None:
        async with replay_tape(_stop_tape(), sync_timeout=0.3) as client:
            async for message in client.receive_messages():
                if isinstance(message, ResultMessage):
                    break

    with pytest.raises(Exception, match="cassette mismatch.*'interrupt'"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_wrong_control_call_at_sync_point_fails_closed():
    """A live control call of a different subtype where the tape records interrupt
    is divergence — surfaced as the typed error on the failing call itself."""

    async def scenario() -> None:
        async with replay_tape(_stop_tape(), sync_timeout=2) as client:
            events = 0
            async for message in client.receive_messages():
                if type(message).__name__ == "StreamEvent":
                    events += 1
                    if events == 5:  # where the recording interrupted — call wrong
                        await client.set_model("claude-haiku-4-5-20251001")

    with pytest.raises(CassetteMismatchError, match="'interrupt'.*'set_model'"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_control_call_after_tape_end_fails_closed():
    """A control call past the tape's end has no recorded answer — it must raise
    immediately, not hang into the SDK's 60s control timeout."""

    async def scenario() -> None:
        async with replay_tape(_stop_tape()) as client:
            events = 0
            async for message in client.receive_messages():
                if type(message).__name__ == "StreamEvent":
                    events += 1
                    if events == 5:
                        await client.interrupt()
                if isinstance(message, ResultMessage):
                    break
            with pytest.raises(CassetteMismatchError, match="after the tape ended"):
                await client.set_model("claude-haiku-4-5-20251001")

    await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_early_disconnect_mid_tape_is_clean():
    """Disconnecting before the recorded interrupt is the consumer's prerogative
    (same semantics as demux replay) — no error, no hang on close."""

    async def scenario() -> None:
        async with replay_tape(_stop_tape()) as client:
            events = 0
            async for message in client.receive_messages():
                if type(message).__name__ == "StreamEvent":
                    events += 1
                    if events == 2:
                        break

    await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_direction_b_stub_replay_identical_under_lockstep():
    """Lockstep composes with Direction-B stub replay: the mcp tape (7 recorded
    mcp_message exchanges) yields the same message sequence as the demux model."""
    mcp = Path(__file__).parent.parent / "examples" / "cassettes" / "mcp_session.jsonl"

    async def run(lockstep: bool) -> list[str]:
        seen: list[str] = []
        async with replay_tape(load_tape(mcp), mode="stub", lockstep=lockstep) as client:
            await _drain(client, seen)
        return seen

    assert await asyncio.wait_for(run(True), _TIMEOUT_S) == await asyncio.wait_for(
        run(False), _TIMEOUT_S
    )


def _control_write(request_id: str, subtype: str) -> dict:
    return {
        "dir": "write",
        "data": json.dumps(
            {"type": "control_request", "request_id": request_id, "request": {"subtype": subtype}}
        ),
    }


def _control_read(request_id: str, response: dict) -> dict:
    return {
        "dir": "read",
        "frame": {
            "type": "control_response",
            "response": {"request_id": request_id, "subtype": "success", "response": response},
        },
    }


async def test_control_call_resolves_against_undelivered_tail():
    """The flow-control lift: a mid-tape control response can be starved only by
    frames recorded *before* it. Demux queues it behind the whole remaining tape,
    so with >100 undrained frames (the SDK's inbound buffer) the call hangs."""
    tape = [_control_write("req_1_rec", "initialize"), _control_read("req_1_rec", {})]
    tape += [
        {"dir": "read", "frame": {"type": "system", "subtype": "status", "i": i}}
        for i in range(20)
    ]
    tape += [
        _control_write("req_2_rec", "mcp_status"),
        _control_read("req_2_rec", {"servers": ["recorded-answer"]}),
    ]
    tape += [
        {"dir": "read", "frame": {"type": "system", "subtype": "status", "i": 100 + i}}
        for i in range(130)
    ]

    async def scenario():
        async with replay_tape(tape, lockstep=True) as client:
            # No draining at all — the 130-frame tail must not block the answer.
            return await client.get_mcp_status()

    assert await asyncio.wait_for(scenario(), _TIMEOUT_S) == {"servers": ["recorded-answer"]}


async def test_same_subtype_different_arguments_fails_closed():
    """Handing the recorded success to a same-subtype call with different arguments
    would certify a session the recording never had (review finding)."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        {
            "dir": "write",
            "data": json.dumps(
                {
                    "type": "control_request",
                    "request_id": "req_2_rec",
                    "request": {"subtype": "set_model", "model": "recorded-model"},
                }
            ),
        },
        _control_read("req_2_rec", {}),
    ]

    async def scenario() -> None:
        async with replay_tape(tape, lockstep=True) as client:
            await client.set_model("different-model")

    with pytest.raises(CassetteMismatchError, match="does not match the recorded arguments"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_same_subtype_same_arguments_replays():
    """The strictness twin: matching arguments get the recorded response."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        {
            "dir": "write",
            "data": json.dumps(
                {
                    "type": "control_request",
                    "request_id": "req_2_rec",
                    "request": {"subtype": "set_model", "model": "recorded-model"},
                }
            ),
        },
        _control_read("req_2_rec", {}),
    ]

    async def scenario() -> None:
        async with replay_tape(tape, lockstep=True) as client:
            await client.set_model("recorded-model")

    await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_direction_b_answer_is_a_sync_point():
    """verify-mode + lockstep with a slow real callback: the result must not be
    delivered while the decision is still pending — otherwise the consumer breaks
    at the result, disconnect() cancels the callback task mid-flight, and verify
    reports a false "never answered" divergence (review finding)."""
    permission = (
        Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
    )

    async def slow_recorded_policy(tool_name, tool_input, context):
        await asyncio.sleep(0.2)  # a real policy that takes its time deciding
        path = tool_input.get("file_path", "")
        if path.startswith("/etc/"):
            return PermissionResultDeny(message=f"Refusing to write to system path: {path}")
        return PermissionResultAllow(
            updated_input={**tool_input, "file_path": "./safe_output/" + posixpath.basename(path)}
        )

    async def scenario() -> None:
        options = ClaudeAgentOptions(can_use_tool=slow_recorded_policy)
        async with replay_tape(
            load_tape(permission), options=options, mode="verify", lockstep=True
        ) as client:
            async for message in client.receive_messages():
                if isinstance(message, ResultMessage):
                    break

    await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_response_without_preceding_request_write_fails_closed():
    """A recorded control_response whose request_id never passed a sync point would
    be silently dropped by the SDK's demux — a truncated/reordered tape must raise."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_OTHER", {}),  # answers a request the tape never recorded
    ]

    async def scenario() -> None:
        async with replay_tape(tape, lockstep=True):
            pass

    with pytest.raises(CassetteMismatchError, match="req_OTHER"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)
