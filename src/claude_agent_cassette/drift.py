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
(:func:`~claude_agent_cassette.conversation_frames` — control_request/_response
are excluded because the control plane is not message-parsed and would return
``None`` spuriously). Cassette inputs are curated to be message producers, so a
``None`` among them is real drift, not noise.

**Scope (a deliberate lower bound).** This catches *parse-level* drift — frames
the installed SDK rejects/skips — plus *silently dropped content blocks* (a
renamed/new block type the parser omits while the message still parses; detected
by the parser's own surviving-block count). It does NOT catch additive
*field-level* drift: a new optional field inside a still-recognised block, or a
changed-but-tolerated enum value. That needs a recorded-expectations / typed-shape
diff, not a re-parse (tracked separately).

Note: imports the SDK's private ``_internal.message_parser`` — version-sensitive
by nature (drift detection *is* about the installed parser); pin your SDK.
"""

from __future__ import annotations

from typing import Literal, NamedTuple

from claude_agent_sdk._internal.message_parser import parse_message

from .tape import Frame, TapeEntry, conversation_frames

# The closed set of drift reasons this detector emits. A Literal (not an Enum):
# it's a code-controlled vocabulary consumers branch on, so it earns a type — but
# the values stay plain strings (zero runtime ceremony), matching tape.Direction.
DriftReason = Literal["parse_error", "unrecognized_type", "content_dropped"]


class DriftFinding(NamedTuple):
    """One frame that no longer survives the installed SDK intact."""

    frame_index: int  # position among the checked (message-producing) frames
    frame_type: str | None  # the recorded frame's top-level ``type``
    reason: DriftReason
    detail: str  # human-readable specifics for the reason


def _dropped_content_blocks(frame: Frame, message: object) -> str | None:
    """Detail string if the parse silently dropped content blocks, else None.

    ``parse_message`` matches each content block's ``type`` and appends only the
    *known* ones — an unrecognised block (renamed/new type) is silently omitted
    while the message still parses. That is invisible to the raise/None checks but
    means a replay loses recorded content. Detected with zero maintenance by the
    SDK's own count: fewer parsed blocks than recorded blocks ⇒ some were dropped.
    """
    raw_message = frame.get("message")
    raw_content = raw_message.get("content") if isinstance(raw_message, dict) else None
    if not isinstance(raw_content, list):
        return None  # string content / no content — no blocks to lose
    parsed_content = getattr(message, "content", None)
    if not isinstance(parsed_content, list) or len(parsed_content) >= len(raw_content):
        return None
    raw_types = [b.get("type") for b in raw_content if isinstance(b, dict)]
    return (
        f"{len(raw_content) - len(parsed_content)} of {len(raw_content)} content "
        f"block(s) dropped on parse; recorded types={raw_types}"
    )


def parse_drift(frames: list[Frame]) -> list[DriftFinding]:
    """Findings for every frame that doesn't survive the installed SDK intact.

    A frame drifts if ``parse_message`` raises (``parse_error``), returns ``None``
    (``unrecognized_type``), or parses but *silently drops content blocks*
    (``content_dropped`` — a renamed/new content block the parser omits). The catch
    is intentionally broad: a malformed field can surface as ``TypeError``/``KeyError``
    rather than ``MessageParseError`` (e.g. ``message`` recorded as a list), and a
    drift detector must *report* any malformation, never crash on it.
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
            continue
        dropped = _dropped_content_blocks(frame, message)
        if dropped is not None:
            findings.append(DriftFinding(index, frame_type, "content_dropped", dropped))
    return findings


def check_drift(tape: list[TapeEntry]) -> list[DriftFinding]:
    """Drift findings for a full duplex tape (checks its message-producing frames)."""
    return parse_drift(conversation_frames(tape))
