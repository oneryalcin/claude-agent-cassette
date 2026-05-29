"""The cassette tape format: an ordered, both-directions recording of the wire.

A *tape* is a list of :class:`TapeEntry`, one per frame, in the exact order it
crossed the transport — so it preserves the read/write interleaving that two
separate lists would lose. Inbound frames carry ``frame`` (a raw stream-json
dict); outbound frames carry ``data`` (the raw payload string the SDK wrote).
"""

from __future__ import annotations

import json
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
