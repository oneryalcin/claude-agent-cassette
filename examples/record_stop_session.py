#!/usr/bin/env python3
"""Record a real **Stop** session — an ``interrupt`` mid-generation — into a cassette.

``interrupt`` is the one Direction-A subtype where conversation/control ordering
is load-bearing: the wire is ``conversation → interrupt (SDK write) → recorded
interrupt response → terminal result``. Replaying it needs lockstep delivery
(the terminal result must land *after* the interrupt exchange), and lockstep
can't be tested without a tape that actually contains one — so we record it.

``include_partial_messages`` makes the CLI stream ``stream_event`` deltas, so
the interrupt is issued genuinely mid-generation (without it, the first
assistant message only arrives once generation is already complete).

Run (spends a small API call; needs ANTHROPIC_API_KEY + the bundled claude CLI):

    uv run python examples/record_stop_session.py
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from claude_agent_cassette import record, save_tape, scrub_tape

_OUT = Path(__file__).parent / "cassettes" / "stop_session.jsonl"
# Pin a non-Covered (zero-data-retention-OK) model: the default rotated to Fable 5,
# a Covered Model that requires data retention enabled. Interrupt handling is
# model-agnostic, so any capable model yields a valid fixture.
_MODEL = "claude-haiku-4-5-20251001"

_PROMPT = (
    "Count from 1 to 200, one number per line. Do not stop early, do not "
    "summarize, just count."
)

# Interrupt after this many stream_event deltas — far enough in that generation
# is demonstrably underway, early enough that the tape stays small.
_EVENTS_BEFORE_INTERRUPT = 5


def _pii_replacements() -> list[tuple[str, str]]:
    """The (needle, mask) pairs that blank this recording's filesystem fingerprint."""
    replacements = [
        (os.path.realpath(os.getcwd()), "<CWD>"),
        (os.getcwd(), "<CWD>"),
        (os.path.expanduser("~"), "<HOME>"),
    ]
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        replacements.append((key, "<REDACTED_API_KEY>"))
    return replacements


def _summary(scrubbed: list[dict]) -> None:
    """Print the tape's tail interleaving — the interrupt exchange is the point."""
    print("\nTape tail (the ordering lockstep must preserve):")
    interrupt_at = None
    for i, e in enumerate(scrubbed):
        if e.get("dir") == "write":
            try:
                d = json.loads(e["data"])
            except ValueError:
                continue
            if d.get("type") == "control_request" and (d.get("request") or {}).get(
                "subtype"
            ) == "interrupt":
                interrupt_at = i
    if interrupt_at is None:
        print("  (no interrupt control_request recorded — re-run; the model may "
              "have finished before the interrupt was issued)")
        return
    for i, e in enumerate(scrubbed[max(0, interrupt_at - 2):], start=max(0, interrupt_at - 2)):
        if e.get("dir") == "write":
            d = json.loads(e["data"])
            label = f"write {d.get('type')}"
            if d.get("type") == "control_request":
                label += f" subtype={d['request'].get('subtype')}"
        else:
            f = e.get("frame") or {}
            label = f"read  {f.get('type')}"
            if f.get("type") == "result":
                label += f" subtype={f.get('subtype')} is_error={f.get('is_error')}"
            if f.get("type") == "control_response":
                label += f" subtype={(f.get('response') or {}).get('subtype')}"
        print(f"  [{i}] {label}")


async def main() -> None:
    options = ClaudeAgentOptions(
        model=_MODEL,
        include_partial_messages=True,
    )
    print("Recording stop session ...\n")
    with record() as tape:
        async with ClaudeSDKClient(options) as client:
            await client.query(_PROMPT)
            events = 0
            interrupted = False
            async for message in client.receive_messages():
                name = type(message).__name__
                if name == "StreamEvent":
                    events += 1
                    if events >= _EVENTS_BEFORE_INTERRUPT and not interrupted:
                        interrupted = True
                        print(f"... interrupting after {events} stream events")
                        await client.interrupt()
                if name == "ResultMessage":
                    break

    scrubbed = scrub_tape(tape, _pii_replacements())
    save_tape(scrubbed, _OUT)
    print(f"\nWrote {len(scrubbed)} frames -> {_OUT}")
    _summary(scrubbed)


if __name__ == "__main__":
    asyncio.run(main())
