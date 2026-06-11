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
from claude_agent_sdk import types as sdk_types
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
)

from claude_agent_cassette import (
    CassetteMismatchError,
    LockstepReplayTransport,
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


def _control_write(request_id: str, subtype: str, **request_args: str) -> dict:
    return {
        "dir": "write",
        "data": json.dumps(
            {
                "type": "control_request",
                "request_id": request_id,
                "request": {"subtype": subtype, **request_args},
            }
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


# --- tolerate_subtypes: foreign tapes vs consumer side-calls (issue #30) ---
#
# A tape is not consumer-neutral: a consumer whose connect/turn path adds its
# own read-only side-calls (a get_mcp_status() health check) can never replay a
# tape recorded by a different consumer — strict lockstep fails closed on the
# first unrecorded call. tolerate_subtypes answers those synthetically, governed
# by the remaining tape (a subtype the tape still records is never tolerated).


async def _health_check_then_drain(tolerate) -> tuple[dict, list[str]]:
    """The motivating consumer shape (desia-task-service): get_mcp_status()
    awaited *before* any draining — exactly what a connect()-time health check
    does — then the normal drain-and-interrupt of the Stop fixture."""
    seen: list[str] = []
    async with replay_tape(_stop_tape(), tolerate_subtypes=tolerate) as client:
        status = await client.get_mcp_status()
        events = 0
        async for message in client.receive_messages():
            seen.append(type(message).__name__)
            if type(message).__name__ == "StreamEvent":
                events += 1
                if events == 5:
                    await client.interrupt()
            if isinstance(message, ResultMessage):
                break
    return status, seen


async def test_foreign_tape_side_call_answered_synthetically():
    """A consumer health-checking on connect must be able to replay a foreign
    interrupt tape: the unrecorded mcp_status gets a synthetic empty answer
    (never recorded data) and the replay then completes in recorded order."""
    status, seen = await asyncio.wait_for(
        _health_check_then_drain({"mcp_status"}), _TIMEOUT_S
    )
    assert status == {"mcpServers": []}
    assert seen[-1] == "ResultMessage"


async def test_side_call_tolerance_defaults_off():
    """Tolerance is opt-in — without it the same consumer fails closed, so a
    gate can never silently absorb an unrecorded control call."""
    with pytest.raises(CassetteMismatchError, match="'interrupt'.*'mcp_status'"):
        await asyncio.wait_for(_health_check_then_drain(None), _TIMEOUT_S)


async def test_intent_bearing_subtype_rejected_at_construction():
    """Synthetically answering an intent-bearing call (interrupt, set_model)
    would certify a session the recording never had — refused up front, not at
    some later sync point."""
    with pytest.raises(ValueError, match="interrupt"):
        LockstepReplayTransport([], tolerate_subtypes={"interrupt"})


async def test_tolerated_subtype_recorded_later_is_not_stolen():
    """The remaining tape is the arbiter: a live call of a tolerated subtype the
    tape records *later* must not be answered synthetically — that would consume
    the write its recorded sync point is waiting for and orphan it. Issuing it
    before the recorded order is a true divergence."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        _control_write("req_2_rec", "set_model", model="recorded-model"),
        _control_read("req_2_rec", {}),
        _control_write("req_3_rec", "mcp_status"),
        _control_read("req_3_rec", {"mcpServers": [{"name": "recorded"}]}),
    ]

    async def scenario() -> None:
        async with replay_tape(
            tape, lockstep=True, sync_timeout=2, tolerate_subtypes={"mcp_status"}
        ) as client:
            await client.get_mcp_status()  # before the recorded set_model

    with pytest.raises(CassetteMismatchError, match="'set_model'.*'mcp_status'"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)


async def test_recorded_sync_of_tolerated_subtype_still_replays_recorded_content():
    """Tolerance never shadows the tape: when the tape records the subtype at
    this position, strict matching wins and the consumer gets the *recorded*
    response, not the synthetic empty one."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        _control_write("req_2_rec", "mcp_status"),
        _control_read("req_2_rec", {"mcpServers": [{"name": "recorded"}]}),
    ]

    async def scenario() -> dict:
        async with replay_tape(
            tape, lockstep=True, tolerate_subtypes={"mcp_status"}
        ) as client:
            return await client.get_mcp_status()

    status = await asyncio.wait_for(scenario(), _TIMEOUT_S)
    assert status == {"mcpServers": [{"name": "recorded"}]}


async def test_post_tape_side_call_tolerated():
    """A consumer may health-check again after the recorded session ended — the
    post-tape fail-closed loop applies the same tolerance."""

    async def scenario() -> dict:
        async with replay_tape(
            _stop_tape(), tolerate_subtypes={"mcp_status"}
        ) as client:
            events = 0
            async for message in client.receive_messages():
                if type(message).__name__ == "StreamEvent":
                    events += 1
                    if events == 5:
                        await client.interrupt()
                if isinstance(message, ResultMessage):
                    break
            return await client.get_mcp_status()

    assert await asyncio.wait_for(scenario(), _TIMEOUT_S) == {"mcpServers": []}


async def test_get_context_usage_canned_shape_is_consumable():
    """The synthetic get_context_usage answer must carry every required key of
    the SDK's ContextUsageResponse — a missing one surfaces as a consumer
    KeyError on a tolerated call. Asserted against the *installed* SDK's
    TypedDict so the CI matrix turns an SDK bump that adds a required key into
    a signal (review finding)."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        _control_write("req_2_rec", "interrupt"),
        _control_read("req_2_rec", {}),
    ]

    async def scenario() -> dict:
        async with replay_tape(
            tape, lockstep=True, tolerate_subtypes={"get_context_usage"}
        ) as client:
            usage = await client.get_context_usage()
            await client.interrupt()
            return usage

    usage = await asyncio.wait_for(scenario(), _TIMEOUT_S)
    response_type = getattr(sdk_types, "ContextUsageResponse", None)
    if response_type is not None:  # SDK versions in the matrix may predate it
        assert set(usage) >= response_type.__required_keys__
    assert usage["percentage"] == 0.0
    assert usage["categories"] == []


async def test_subtype_becomes_tolerable_once_its_recorded_sync_passes():
    """The enabling half of decrement-after-match: a second live call of a
    subtype the tape recorded *once* must be tolerated after that sync passes —
    without the decrement, the stale count holds the call for matching and the
    next sync point reports a false divergence (mutation-testing finding)."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
        _control_write("req_2_rec", "mcp_status"),
        _control_read("req_2_rec", {"mcpServers": [{"name": "recorded"}]}),
        _control_write("req_3_rec", "interrupt"),
        _control_read("req_3_rec", {}),
    ]

    async def scenario() -> tuple[dict, dict]:
        async with replay_tape(
            tape, lockstep=True, tolerate_subtypes={"mcp_status"}
        ) as client:
            first = await client.get_mcp_status()  # matches the recorded sync
            second = await client.get_mcp_status()  # past it — tolerated
            await client.interrupt()
            return first, second

    first, second = await asyncio.wait_for(scenario(), _TIMEOUT_S)
    assert first == {"mcpServers": [{"name": "recorded"}]}
    assert second == {"mcpServers": []}


async def test_side_call_resolves_during_long_pre_sync_section():
    """The adversarial-review regression: a connect-time health check on a tape
    with more pre-sync frames than the SDK's inbound buffer (100). The walker
    suspends on backpressure long before the next sync point, so a park-only
    tolerance never answers and the blocked consumer deadlocks — the call must
    be serviced while the walk is still pumping the section."""
    tape = [_control_write("req_1_rec", "initialize"), _control_read("req_1_rec", {})]
    tape += [
        {"dir": "read", "frame": {"type": "system", "subtype": "status", "i": i}}
        for i in range(130)
    ]
    tape += [_control_write("req_2_rec", "interrupt"), _control_read("req_2_rec", {})]

    async def scenario() -> dict:
        async with replay_tape(
            tape, lockstep=True, tolerate_subtypes={"mcp_status"}
        ) as client:
            status = await client.get_mcp_status()  # before any draining
            count = 0
            async for _message in client.receive_messages():
                count += 1
                if count == 130:
                    await client.interrupt()
                    break
            return status

    assert await asyncio.wait_for(scenario(), _TIMEOUT_S) == {"mcpServers": []}


async def test_direction_b_park_services_tolerated_side_calls():
    """A can_use_tool callback that health-checks before deciding: the walk is
    parked on the pending Direction-B answer, which the callback can't produce
    until its tolerated side-call is answered. Pre-fix this starved until
    sync_timeout and surfaced as a misleading "callback never answered"
    divergence (review finding)."""
    permission = (
        Path(__file__).parent.parent / "examples" / "cassettes" / "permission_session.jsonl"
    )
    holder: dict = {}
    statuses: list[dict] = []

    async def health_checking_policy(tool_name, tool_input, context):
        statuses.append(await holder["client"].get_mcp_status())
        path = tool_input.get("file_path", "")
        if path.startswith("/etc/"):
            return PermissionResultDeny(message=f"Refusing to write to system path: {path}")
        return PermissionResultAllow(
            updated_input={**tool_input, "file_path": "./safe_output/" + posixpath.basename(path)}
        )

    async def scenario() -> None:
        options = ClaudeAgentOptions(can_use_tool=health_checking_policy)
        async with replay_tape(
            load_tape(permission),
            options=options,
            mode="verify",
            lockstep=True,
            tolerate_subtypes={"mcp_status"},
        ) as client:
            holder["client"] = client
            async for message in client.receive_messages():
                if isinstance(message, ResultMessage):
                    break

    await asyncio.wait_for(scenario(), _TIMEOUT_S)
    assert statuses == [{"mcpServers": []}, {"mcpServers": []}]


async def test_tolerate_subtypes_requires_lockstep():
    """Demux has no sync points to tolerate at — passing tolerate_subtypes with
    a tape that resolves to demux must raise, not silently do nothing."""
    tape = [
        _control_write("req_1_rec", "initialize"),
        _control_read("req_1_rec", {}),
    ]

    async def scenario() -> None:
        async with replay_tape(tape, tolerate_subtypes={"mcp_status"}):
            pass

    with pytest.raises(ValueError, match="lockstep"):
        await asyncio.wait_for(scenario(), _TIMEOUT_S)
