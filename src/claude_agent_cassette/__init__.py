"""claude-agent-cassette — record & replay the claude-agent-sdk wire for tests.

Replay recorded SDK message streams through the *real* parser and client, with
no API key and no subprocess, so your tests are deterministic and offline — and
catch the "the SDK sent a slightly different shape than we expected" class of
bug that mocked tests can't.
"""

from .record import record_sdk_wire
from .replay import replay
from .tape import (
    Direction,
    RawMessage,
    TapeEntry,
    control_request_subtype,
    control_responses_by_subtype,
    conversation_messages,
    load_cassette,
    load_tape,
    read_frames,
    replayable_messages,
    serialize_tape,
)
from .transport import CassetteMismatchError, RecordingTransport, ReplayTransport

__version__ = "0.2.0"

__all__ = [
    "ReplayTransport",
    "RecordingTransport",
    "CassetteMismatchError",
    "replay",
    "record_sdk_wire",
    "TapeEntry",
    "Direction",
    "RawMessage",
    "serialize_tape",
    "load_tape",
    "load_cassette",
    "read_frames",
    "conversation_messages",
    "replayable_messages",
    "control_responses_by_subtype",
    "control_request_subtype",
]
