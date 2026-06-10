"""Direction-B mcp_message replay: tape-synthesized SDK MCP servers (stub) + wire diff (verify).

The stub path reconstructs a *real* ``create_sdk_mcp_server`` per recorded server —
identity from the recorded ``initialize``, tool defs from the recorded ``tools/list``,
results from the recorded ``tools/call``s — so ``initialize`` / ``tools/list`` are
answered by the SDK's own routing and only ``tools/call`` replays from the tape. The
strongest test here drives a stub replay and diffs its live writes against the recording
with the verify comparator: the synthesized server must reproduce the recorded wire
exactly. The verify path runs the consumer's real server and diffs the same way.
"""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    create_sdk_mcp_server,
    tool,
)

from claude_agent_cassette import (
    CassetteMismatchError,
    ControlReplayLedger,
    ReplayTransport,
    build_mcp_stub_servers,
    control_stub_options,
    control_verify_options,
    direction_b_replay_findings,
    load_tape,
    replay_tape,
    verify_direction_b_decisions,
)

_MCP = Path(__file__).parent.parent / "examples" / "cassettes" / "mcp_session.jsonl"
_TIMEOUT_S = 20


def _tape():
    return load_tape(_MCP)


async def _drive(tape, mode="stub", options=None):
    async with replay_tape(tape, options=options, mode=mode) as client:
        async for message in client.receive_messages():
            if type(message).__name__ == "ResultMessage":
                break


# The calculator the fixture was recorded with (see examples/record_mcp_session.py).


@tool("add", "Add two numbers", {"a": float, "b": float})
async def _add(args: dict[str, Any]) -> dict[str, Any]:
    result = args["a"] + args["b"]
    return {"content": [{"type": "text", "text": f"{args['a']} + {args['b']} = {result}"}]}


@tool("divide", "Divide one number by another", {"a": float, "b": float})
async def _divide(args: dict[str, Any]) -> dict[str, Any]:
    if args["b"] == 0:
        return {"content": [{"type": "text", "text": "Error: Division by zero is not allowed"}],
                "is_error": True}
    result = args["a"] / args["b"]
    return {"content": [{"type": "text", "text": f"{args['a']} / {args['b']} = {result}"}]}


def _recorded_calculator():
    return create_sdk_mcp_server(name="calculator", version="1.0.0", tools=[_add, _divide])


# --- Stub mode: zero-config replay; the synthesized server must reproduce the wire ---


async def test_replay_tape_stub_mode_mcp_clean_completes():
    await asyncio.wait_for(_drive(_tape(), "stub"), _TIMEOUT_S)


async def test_stub_mcp_reproduces_recorded_wire_exactly():
    """The acid test: diff the synthesized server's live answers against the recording
    with the verify comparator — every recorded exchange, byte-for-byte."""
    tape = _tape()
    bundle = control_stub_options(tape)
    assert bundle.keep_subtypes == {"mcp_message"}
    transport = ReplayTransport.from_tape(tape, keep_control_requests=bundle.keep_subtypes)
    client = ClaudeSDKClient(options=bundle.options, transport=transport)
    await client.connect()
    async for message in client.receive_messages():
        if type(message).__name__ == "ResultMessage":
            break
    await client.disconnect()

    ledger = ControlReplayLedger()
    verify_direction_b_decisions(transport.writes, tape, ledger)
    assert not ledger.diverged()


async def test_stub_mode_survives_truncated_recording_without_tools_list():
    """tools/list lost from the tape -> minimal tool defs; the calls still replay."""

    def is_tools_list(entry):
        if entry.get("dir") == "read":
            frame = entry.get("frame") or {}
            if frame.get("type") == "control_request":
                return ((frame.get("request") or {}).get("message") or {}).get(
                    "method"
                ) == "tools/list"
        return False

    tape = _tape()
    list_ids = {e["frame"]["request_id"] for e in tape if is_tools_list(e)}

    def keep(entry):
        if is_tools_list(entry):
            return False
        if entry.get("dir") == "write":
            payload = json.loads(entry["data"])
            if payload.get("type") == "control_response":
                return (payload.get("response") or {}).get("request_id") not in list_ids
        return True

    await asyncio.wait_for(_drive([e for e in tape if keep(e)], "stub"), _TIMEOUT_S)


# --- Stub-mode divergence: fail-closed end-to-end through replay_tape ---


def _find_call(tape, name):
    for i, entry in enumerate(tape):
        frame = entry.get("frame") if entry.get("dir") == "read" else None
        if frame and frame.get("type") == "control_request":
            message = (frame.get("request") or {}).get("message") or {}
            if message.get("method") == "tools/call" and (message.get("params") or {}).get(
                "name"
            ) == name:
                return i, frame
    raise AssertionError(f"no recorded tools/call for {name!r}")


def _inject_call(tape, name, offset):
    """Insert an extra tools/call request (fresh id, no recorded response)."""
    out = copy.deepcopy(tape)
    i, frame = _find_call(out, name)
    extra = copy.deepcopy(frame)
    extra["request_id"] = "INJECTED"
    extra["request"]["message"]["params"]["arguments"] = {"a": 99, "b": 1}
    out.insert(i + offset, {"dir": "read", "frame": extra})
    return out


async def test_stub_mode_raises_on_injected_call_desyncing_arguments():
    """An unrecorded call ahead of a recorded one desyncs the FIFO — the argument
    guard must catch it (and surface through replay_tape despite SDK swallowing)."""
    with pytest.raises(CassetteMismatchError, match="arguments"):
        await asyncio.wait_for(_drive(_inject_call(_tape(), "add", 0), "stub"), _TIMEOUT_S)


async def test_stub_mode_raises_when_tool_called_more_than_recorded():
    with pytest.raises(CassetteMismatchError, match="more times than recorded"):
        await asyncio.wait_for(_drive(_inject_call(_tape(), "add", 1), "stub"), _TIMEOUT_S)


def _with_jsonrpc_error(tape, name):
    """Rewrite the recorded tools/call response for ``name`` into a JSON-RPC error."""
    out = copy.deepcopy(tape)
    _, frame = _find_call(out, name)
    request_id = frame["request_id"]
    for entry in out:
        if entry.get("dir") != "write":
            continue
        payload = json.loads(entry["data"])
        if payload.get("type") == "control_response" and (
            payload.get("response") or {}
        ).get("request_id") == request_id:
            payload["response"]["response"]["mcp_response"] = {
                "jsonrpc": "2.0", "id": 3, "error": {"code": -32603, "message": "boom"}}
            entry["data"] = json.dumps(payload)
    return out


async def test_stub_mode_raises_on_recorded_jsonrpc_error():
    """A tools/call that recorded a JSON-RPC error can't replay as a success."""
    with pytest.raises(CassetteMismatchError, match="JSON-RPC error"):
        await asyncio.wait_for(
            _drive(_with_jsonrpc_error(_tape(), "divide"), "stub"), _TIMEOUT_S
        )


# --- Verify mode: the consumer's real server diffs against the recording ---


async def test_verify_green_when_real_server_reproduces_recording():
    options = ClaudeAgentOptions(mcp_servers={"calc": _recorded_calculator()})
    await asyncio.wait_for(_drive(_tape(), "verify", options), _TIMEOUT_S)


async def test_verify_raises_when_tool_result_changed():
    @tool("add", "Add two numbers", {"a": float, "b": float})
    async def broken_add(args):
        return {"content": [{"type": "text", "text": "forty-two-ish"}]}

    broken = create_sdk_mcp_server(name="calculator", version="1.0.0", tools=[broken_add, _divide])
    with pytest.raises(CassetteMismatchError, match="diverged from the recording"):
        await asyncio.wait_for(
            _drive(_tape(), "verify", ClaudeAgentOptions(mcp_servers={"calc": broken})),
            _TIMEOUT_S,
        )


def test_verify_requires_matching_sdk_server():
    with pytest.raises(CassetteMismatchError, match="mcp_servers"):
        control_verify_options(_tape(), ClaudeAgentOptions())


# --- Builder + lint ---


def test_build_mcp_stub_servers_reconstructs_recorded_identity():
    servers = build_mcp_stub_servers(_tape(), ControlReplayLedger())
    assert set(servers) == {"calc"}  # keyed by the recorded server_name
    config = servers["calc"]
    assert config["type"] == "sdk"
    assert config["name"] == "calculator"  # serverInfo.name from the recorded initialize


def test_findings_clean_for_mcp_fixture():
    assert direction_b_replay_findings(_tape()) == []


def test_findings_flag_recorded_jsonrpc_error():
    findings = direction_b_replay_findings(_with_jsonrpc_error(_tape(), "divide"))
    assert any("JSON-RPC error" in f for f in findings)
