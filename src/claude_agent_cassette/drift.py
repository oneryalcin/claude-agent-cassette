"""Detect when a cassette's recorded frames have drifted from the installed SDK.

Re-parse each recorded conversation frame through the **installed** SDK's own
``message_parser``. Because it reuses the SDK's parser, the check cannot disagree
with what the SDK actually accepts — there is no hand-maintained schema to rot.

Two drift signals (kept distinct so a human can adjudicate):

- ``parse_error`` — ``parse_message`` raised. A *known* message type whose
  required shape changed (a renamed/removed field, a field that changed type).
- ``unrecognized_type`` — ``parse_message`` returned ``None``. The frame's top
  level ``type`` is one the installed SDK no longer models (renamed/removed, or a
  newer type an older SDK skips). Forward-compatible skip in the SDK → drift here.

**Contract.** This runs over a cassette's *message-producing* frames
(:func:`~claude_agent_cassette.replayable_messages` — control_request/_response
are excluded because the control plane is not message-parsed and would return
``None`` spuriously). Cassette inputs are curated to be message producers, so a
``None`` among them is real drift, not noise.

**Scope (a deliberate lower bound).** This catches *parse-level* drift — frames
the installed SDK rejects or skips. It does NOT catch *field-level* drift: a frame
that still parses but whose meaning changed (a new optional field, a changed enum
value). That needs a recorded-expectations diff, not a re-parse.

Note: imports the SDK's private ``_internal.message_parser`` — version-sensitive
by nature (drift detection *is* about the installed parser); pin your SDK.
"""

from __future__ import annotations

from typing import NamedTuple

from claude_agent_sdk._internal.message_parser import parse_message

from .tape import RawMessage, TapeEntry, replayable_messages


class DriftFinding(NamedTuple):
    """One frame that no longer parses cleanly under the installed SDK."""

    frame_index: int  # position among the checked (message-producing) frames
    frame_type: str | None  # the recorded frame's top-level ``type``
    reason: str  # "parse_error" | "unrecognized_type"
    detail: str  # exception "Type: msg" for parse_error; the type for unrecognized


def parse_drift(frames: list[RawMessage]) -> list[DriftFinding]:
    """Findings for every frame that does not parse to a typed message.

    A frame drifts if ``parse_message`` raises (``parse_error``) or returns
    ``None`` (``unrecognized_type``). The catch is intentionally broad: a malformed
    field can surface as ``TypeError``/``KeyError`` rather than ``MessageParseError``
    (e.g. ``message`` recorded as a list), and a drift detector must *report* any
    malformation, never crash on it.
    """
    findings: list[DriftFinding] = []
    for index, frame in enumerate(frames):
        frame_type = frame.get("type") if isinstance(frame, dict) else None
        try:
            message = parse_message(frame)
        except Exception as exc:  # noqa: BLE001 — report arbitrary malformation, don't crash
            findings.append(
                DriftFinding(index, frame_type, "parse_error", f"{type(exc).__name__}: {exc}")
            )
            continue
        if message is None:
            findings.append(
                DriftFinding(index, frame_type, "unrecognized_type", str(frame_type))
            )
    return findings


def check_tape(tape: list[TapeEntry]) -> list[DriftFinding]:
    """Drift findings for a full duplex tape (checks its message-producing frames)."""
    return parse_drift(replayable_messages(tape))
