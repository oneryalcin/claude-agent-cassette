"""claude-agent-cassette — record & replay the claude-agent-sdk wire for tests.

Replay recorded SDK message streams through the *real* parser and client, with
no API key and no subprocess, so your tests are deterministic and offline — and
catch the "the SDK sent a slightly different shape than we expected" class of
bug that mocked tests can't.
"""

from .record import record_sdk_wire
from .replay import replay, replay_tape
from .tape import (
    ControlExchange,
    Direction,
    RawMessage,
    TapeEntry,
    control_request_subtype,
    control_responses_by_subtype,
    conversation_messages,
    direction_b_exchanges,
    direction_b_read_frames,
    load_cassette,
    load_tape,
    message_frames,
    read_frames,
    recorded_hook_config,
    replayable_messages,
    serialize_tape,
)
from .control_stubs import (
    ControlReplayLedger,
    ControlStubBundle,
    build_mcp_stub_servers,
    control_stub_options,
    control_verify_options,
    direction_b_replay_findings,
    verify_direction_b_decisions,
)
from .redact import scrub_tape
from .drift import DriftFinding, check_tape, parse_drift
from .transport import CassetteMismatchError, RecordingTransport, ReplayTransport

__version__ = "0.2.1"

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
    "message_frames",
    "control_responses_by_subtype",
    "control_request_subtype",
    "direction_b_exchanges",
    "direction_b_read_frames",
    "ControlExchange",
    "recorded_hook_config",
    "control_stub_options",
    "control_verify_options",
    "verify_direction_b_decisions",
    "build_mcp_stub_servers",
    "ControlStubBundle",
    "ControlReplayLedger",
    "direction_b_replay_findings",
    "scrub_tape",
    "replay_tape",
    "parse_drift",
    "check_tape",
    "DriftFinding",
]
