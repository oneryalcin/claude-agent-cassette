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
from typing import Any, Literal

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


def replayable_messages(tape: list[TapeEntry]) -> list[RawMessage]:
    """Inbound conversation/system frames to feed the SDK's receive loop.

    Every inbound frame that is neither a ``control_request`` (Direction B —
    dropped so a consumer's registered callbacks stay inert on replay) nor a
    ``control_response`` (answered out-of-band, see ``control_responses_by_subtype``).
    """
    return [
        f for f in read_frames(tape)
        if f.get("type") not in ("control_request", "control_response")
    ]


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
        data = entry.get("data")
        if entry.get("dir") != "write" or not isinstance(data, str):
            continue
        try:
            request = json.loads(data)
        except ValueError:
            continue
        if request.get("type") != "control_request":
            continue
        subtype = control_request_subtype(request)
        response = response_by_id.get(request.get("request_id"))
        if subtype is not None and response is not None:
            by_subtype[subtype].append(response)
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
