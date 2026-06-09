"""The cassette tape format: an ordered, both-directions recording of the wire.

A *tape* is a list of :class:`TapeEntry`, one per frame, in the exact order it
crossed the transport — so it preserves the read/write interleaving that two
separate lists would lose. Inbound frames carry ``frame`` (a raw stream-json
dict); outbound frames carry ``data`` (the raw payload string the SDK wrote).
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Literal, NamedTuple

from typing_extensions import NotRequired, TypedDict

RawMessage = dict[str, Any]
Direction = Literal["read", "write"]


class TapeEntry(TypedDict):
    """One frame of a duplex recording. Exactly one payload, selected by ``dir``."""

    dir: Direction
    frame: NotRequired[RawMessage]  # inbound (read)
    data: NotRequired[str]  # outbound (write)


def serialize_tape(tape: list[TapeEntry]) -> str:
    """Render a tape as JSONL — one tagged frame per line, in capture order."""
    return "".join(json.dumps(entry) + "\n" for entry in tape)


def load_tape(path: str | Path) -> list[TapeEntry]:
    """Read a tape back from a JSONL file."""
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def read_frames(tape: list[TapeEntry]) -> list[RawMessage]:
    """Inbound frames (CLI -> SDK), in order — the stream a ReplayTransport replays."""
    return [entry["frame"] for entry in tape if entry.get("dir") == "read"]


# --- Control-protocol view of the wire (the single place that knows the shape of
# control_request / control_response frames; everything else reads via these). ---


def control_request_subtype(frame: RawMessage) -> str | None:
    """The subtype of a control_request frame (``initialize``, ``mcp_message``, …)."""
    return (frame.get("request") or {}).get("subtype")


def _write_payload(entry: TapeEntry) -> RawMessage | None:
    """The parsed JSON object of an outbound ``write`` entry, or None.

    Outbound frames store the raw payload *string* the SDK wrote; control-protocol
    code needs it back as a dict. Returns None for non-``write`` entries, non-string
    or non-JSON ``data``, or a JSON value that isn't an object.
    """
    if entry.get("dir") != "write":
        return None
    data = entry.get("data")
    if not isinstance(data, str):
        return None
    try:
        payload = json.loads(data)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


_CONTROL_FRAME_TYPES = ("control_request", "control_response")


def message_frames(frames: list[RawMessage]) -> list[RawMessage]:
    """The frames that should parse to typed messages — i.e. not control frames.

    Control frames (``control_request``/``control_response``) ride the same stream
    but are handled by the control protocol, not ``message_parser`` (which returns
    ``None`` for them); excluding them keeps message-level consumers (replay,
    drift) from treating control frames as conversation.
    """
    return [f for f in frames if f.get("type") not in _CONTROL_FRAME_TYPES]


def replayable_messages(tape: list[TapeEntry]) -> list[RawMessage]:
    """Inbound conversation/system frames to feed the SDK's receive loop.

    Every inbound frame that is neither a ``control_request`` (Direction B —
    dropped so a consumer's registered callbacks stay inert on replay) nor a
    ``control_response`` (answered out-of-band, see ``control_responses_by_subtype``).
    """
    return message_frames(read_frames(tape))


def control_responses_by_subtype(tape: list[TapeEntry]) -> dict[str, deque[RawMessage]]:
    """Recorded Direction-A ``control_response`` frames, keyed by the subtype of the
    request they answered (per-subtype FIFO).

    Correlates each recorded SDK ``control_request`` (an outbound ``write``) with
    its ``control_response`` on the recorded ``request_id`` — so a replay can hand
    the right recorded answer to the right live request by subtype, rather than
    trusting arrival order.
    """
    response_by_id = {
        (f.get("response") or {}).get("request_id"): f
        for f in read_frames(tape)
        if f.get("type") == "control_response"
    }
    by_subtype: dict[str, deque[RawMessage]] = defaultdict(deque)
    for entry in tape:
        request = _write_payload(entry)
        if request is None or request.get("type") != "control_request":
            continue
        subtype = control_request_subtype(request)
        response = response_by_id.get(request.get("request_id"))
        if subtype is not None and response is not None:
            by_subtype[subtype].append(response)
    return dict(by_subtype)


class ControlExchange(NamedTuple):
    """One recorded Direction-B control exchange — the CLI's request and the SDK's answer.

    Direction B is the mirror of Direction A: the **CLI sends a control_request to the
    SDK** (``can_use_tool`` / ``hook_callback`` / ``mcp_message``), the SDK invokes the
    consumer's registered callback, and writes the **decision** back. So in the tape an
    inbound ``read`` request is paired with an outbound ``write`` response on the same
    ``request_id``.
    """

    subtype: str  # the request subtype: can_use_tool / hook_callback / mcp_message
    request: RawMessage  # the inbound control_request's ``request`` payload (tool_name, input, …)
    decision: RawMessage  # the recorded answer — the control_response's inner ``response`` payload
    succeeded: bool  # whether the recorded response envelope was ``success`` (vs ``error``)
    request_id: str  # the CLI-minted id correlating request↔decision (unused for stub matching)


def direction_b_exchanges(tape: list[TapeEntry]) -> dict[str, deque[ControlExchange]]:
    """Recorded Direction-B exchanges, keyed by request subtype, in recorded order.

    The mirror of :func:`control_responses_by_subtype`: there the SDK *sends* the
    request (outbound write) and the CLI answers (inbound read); here the CLI sends
    the request (inbound read) and the SDK answers (outbound write). Each inbound
    ``control_request`` is paired with its outbound ``control_response`` by
    ``request_id`` so a replay stub can hand back the recorded ``decision`` instead
    of running the consumer's live callback. An inbound request with no recorded
    response is dropped (mirrors the Direction-A helper's unmatched handling).

    Matching note: the stub callbacks the SDK invokes never see ``request_id``
    (e.g. ``can_use_tool`` gets ``(tool_name, input, context)``), so consumers
    correlate by subtype + payload shape + order — which is why this is keyed by
    subtype and preserves recorded order within each subtype.
    """
    response_by_id: dict[str, RawMessage] = {}
    for entry in tape:
        payload = _write_payload(entry)
        if payload is None or payload.get("type") != "control_response":
            continue
        envelope = payload.get("response") or {}
        rid = envelope.get("request_id")
        if rid is not None:
            response_by_id[rid] = envelope

    by_subtype: dict[str, deque[ControlExchange]] = defaultdict(deque)
    for entry in tape:
        if entry.get("dir") != "read":
            continue
        frame = entry.get("frame") or {}
        if frame.get("type") != "control_request":
            continue
        subtype = control_request_subtype(frame)
        request_id = frame.get("request_id")
        if subtype is None or request_id is None:
            continue
        envelope = response_by_id.get(request_id)
        if envelope is None:
            continue
        by_subtype[subtype].append(
            ControlExchange(
                subtype=subtype,
                request=frame.get("request") or {},
                decision=envelope.get("response") or {},
                succeeded=envelope.get("subtype") == "success",
                request_id=request_id,
            )
        )
    return dict(by_subtype)


def conversation_messages(tape: list[TapeEntry]) -> list[RawMessage]:
    """The inbound frames a conversation replay needs.

    Inbound frames minus the ``control_response`` to the initialize handshake,
    which :class:`~claude_agent_cassette.ReplayTransport` synthesises itself on
    replay. Control-protocol frames (``control_request`` for mcp/hooks, etc.) are
    retained in the full tape but not in this conversation view.
    """
    return [f for f in read_frames(tape) if f.get("type") != "control_response"]


def load_cassette(path: str | Path) -> list[RawMessage]:
    """Load a replay cassette: a JSONL file of raw inbound frames to replay."""
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
