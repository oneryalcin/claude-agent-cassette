"""Leak regression over the committed example cassettes (issue #22).

Every fixture is recorded on a real machine and published in the repo. The
recorders isolate ``CLAUDE_CONFIG_DIR`` and scrub paths/inventory, but a
re-record can silently reintroduce a leak (this happened: an ``ls -la`` tool
result printed the operator's username; the slug-encoded project path survived
a literal-path scrub). These static patterns are operator-path indicators that
must never survive scrubbing — raw AND slug-encoded forms, because the CLI
embeds paths both ways.
"""
from __future__ import annotations

from pathlib import Path

import pytest

_CASSETTES = sorted((Path(__file__).parent.parent / "examples" / "cassettes").glob("*.jsonl"))

# Each pattern indicates an unscrubbed operator path (or credential). Slug forms
# (-Users-…) are how the CLI encodes paths inside ~/.claude/projects/… strings.
_FORBIDDEN = (
    "/Users/",        # macOS home prefix
    "-Users-",        # …slug-encoded
    "/home/",         # Linux home prefix
    "/var/folders/",  # macOS temp root: embeds a stable per-user machine hash
    "-var-folders-",  # …slug-encoded
    "sk-ant-",        # an Anthropic API key
)


@pytest.mark.parametrize("cassette", _CASSETTES, ids=lambda p: p.stem)
def test_committed_cassette_carries_no_operator_path(cassette: Path):
    raw = cassette.read_text()
    leaked = [pattern for pattern in _FORBIDDEN if pattern in raw]
    assert not leaked, (
        f"{cassette.name} contains unscrubbed operator fingerprints {leaked} — "
        "re-record with the recorder's isolation/scrub (examples/record_*.py) "
        "or extend its replacements"
    )
