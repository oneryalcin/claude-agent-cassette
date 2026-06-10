"""claude-agent-cassette — record & replay the claude-agent-sdk wire for tests.

Replay recorded SDK message streams through the *real* parser and client, with
no API key and no subprocess, so your tests are deterministic and offline — and
catch the "the SDK sent a slightly different shape than we expected" class of
bug that mocked tests can't.

Vocabulary: a **frame** is a raw stream-json dict on the wire; a **message** is
the typed object the SDK parses a frame into. A **tape** is a duplex recording
(both directions, control plane included); a frames file is the conversation-only
artifact derived from one. **Direction A** is SDK→CLI control (``initialize``,
``mcp_status``); **Direction B** is CLI→SDK control (``can_use_tool``,
``hook_callback``, ``mcp_message``).
"""

from .record import record
from .replay import ReplayMode, replay, replay_tape
from .tape import (
    ControlExchange,
    Frame,
    TapeEntry,
    conversation_frames,
    direction_b_exchanges,
    inbound_frames,
    load_frames,
    load_tape,
    save_tape,
)
from .direction_b import lint_tape
from .scrub import (
    Replacements,
    default_replacements,
    path_replacements,
    scrub_init_inventory,
    scrub_tape,
)
from .drift import (
    DriftFinding,
    DriftReason,
    check_drift,
    field_drift,
    parse_drift,
    unmodeled_fields,
)
from .transport import (
    CassetteMismatchError,
    LockstepReplayTransport,
    RecordingTransport,
    ReplayTransport,
)

__version__ = "0.4.0"

__all__ = [
    # record & replay (the core loop)
    "record",
    "replay",
    "replay_tape",
    "ReplayMode",
    # tape & frames
    "TapeEntry",
    "Frame",
    "save_tape",
    "load_tape",
    "load_frames",
    "inbound_frames",
    "conversation_frames",
    "direction_b_exchanges",
    "ControlExchange",
    "scrub_tape",
    "scrub_init_inventory",
    "Replacements",
    "default_replacements",
    "path_replacements",
    # tape health: drift (does the installed SDK still parse it?) and
    # replayability (is it still Direction-B replayable after a scrub?)
    "check_drift",
    "parse_drift",
    "unmodeled_fields",
    "field_drift",
    "DriftFinding",
    "DriftReason",
    "lint_tape",
    # transport-level integration
    "ReplayTransport",
    "LockstepReplayTransport",
    "RecordingTransport",
    "CassetteMismatchError",
]
