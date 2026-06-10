"""Decision-preserving scrub: blank PII *values* in a tape, keep structure + decisions.

A recorded tape carries real prompts, file paths, and the recording machine's
fingerprint, but a Direction-B replay *consumes* the control decisions (permission
behavior, ``updatedInput``, hook output, ``callback_id``). So a scrub for sharing must
blank PII values while leaving every key, frame, and decision intact — the opposite of
the older websearch fixture, which scrubbed its decisions and made itself unreplayable.

:func:`scrub_tape` does value-substitution only: it replaces known-sensitive substrings
(absolute paths, API keys) in string values and never drops or reshapes a frame, so
control-plane decisions ride through unless a needle literally occurs inside one. Pair it
with :func:`~claude_agent_cassette.direction_b_replay_findings` to confirm the scrubbed
tape is still replayable.
"""

from __future__ import annotations

import json
from typing import Any

from .tape import TapeEntry


def _redact(obj: Any, replacements: list[tuple[str, str]]) -> Any:
    """Recursively substitute needle→mask in every string value; structure untouched."""
    if isinstance(obj, str):
        for needle, mask in replacements:
            obj = obj.replace(needle, mask)
        return obj
    if isinstance(obj, list):
        return [_redact(v, replacements) for v in obj]
    if isinstance(obj, dict):
        return {k: _redact(v, replacements) for k, v in obj.items()}
    return obj


def scrub_tape(tape: list[TapeEntry], replacements: list[tuple[str, str]]) -> list[TapeEntry]:
    """A copy of ``tape`` with each ``(needle, mask)`` substring blanked in string values.

    ``replacements`` is applied **longest-needle-first**, so a specific path
    (``/private/var/…/tmpX``) is masked before a shorter prefix of it (``/var/…/tmpX``)
    can match inside it. Outbound ``write`` payloads are parsed, redacted, and
    re-serialized so a masked value can't survive as raw JSON text. Empty needles are
    ignored. Nothing is dropped or reshaped — only string values are substituted, so
    control-plane decisions (``behavior`` / ``updatedInput`` / hook output /
    ``callback_id``) are preserved unless a needle literally occurs in them.
    """
    ordered = sorted((r for r in replacements if r[0]), key=lambda r: len(r[0]), reverse=True)
    scrubbed: list[TapeEntry] = []
    for entry in tape:
        data = entry.get("data")
        if entry.get("dir") == "write" and isinstance(data, str):
            try:
                payload = json.loads(data)
            except ValueError:
                scrubbed.append(_redact(entry, ordered))
                continue
            scrubbed.append({"dir": "write", "data": json.dumps(_redact(payload, ordered))})
        else:
            scrubbed.append(_redact(entry, ordered))
    return scrubbed
