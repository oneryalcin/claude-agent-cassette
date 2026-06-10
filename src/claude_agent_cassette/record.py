"""Opt-in capture of the full duplex SDK wire for a query."""

from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .scrub import Replacements, scrub_tape
from .tape import TapeEntry, serialize_tape
from .transport import RecordingTransport


@contextmanager
def record(
    path: str | Path | None = None,
    scrub: Replacements | None = None,
) -> Iterator[list[TapeEntry]]:
    """Tee the SDK wire for any query run inside the ``with`` block.

    Yields the tape (a growing list of :class:`TapeEntry`); on exit the SDK's
    transport constructor is restored. Wrapping *at construction* avoids
    replicating the SDK's internal transport setup.

    With ``path``, the tape is also written there — but only on **clean exit**
    (atomically, via a temp file): an exception inside the block propagates and
    writes nothing, so a crashed session can't leave a torn fixture that looks
    real. ``scrub`` (``(needle, mask)`` pairs, see
    :func:`~claude_agent_cassette.scrub_tape`) is applied before the write, so
    PII never touches disk; for recordings that will be committed or shared,
    prefer passing it here over scrubbing later.

    Usage::

        from claude_agent_cassette import record

        with record("session.jsonl", scrub=[(str(Path.home()), "<HOME>")]) as tape:
            async for _ in query(prompt="...", options=...):
                pass

    For full control (e.g. inspect before writing), take the tape and save it
    yourself: ``with record() as tape: ...`` then
    :func:`~claude_agent_cassette.save_tape`.

    Patches BOTH transport reference sites, because the SDK reaches the transport
    two ways: ``ClaudeSDKClient._connect_inner`` (the interactive client) does a
    *call-time* import from ``...transport.subprocess_cli``, while the one-shot
    ``query()`` (``InternalClient.process_query``) uses the name bound in
    ``_internal.client``. Patching only one silently misses the other.

    Caveats:
      - Touches ``claude_agent_sdk._internal`` — version-sensitive; pin your SDK.
      - Module-global patch — assumes one in-flight query per process.
    """
    if scrub is not None and path is None:
        raise ValueError("record(scrub=...) requires path=... (nothing is written without it)")

    import claude_agent_sdk._internal.client as sdk_client
    import claude_agent_sdk._internal.transport.subprocess_cli as sdk_subprocess

    tape: list[TapeEntry] = []
    real_cls = sdk_subprocess.SubprocessCLITransport

    def _factory(*args: Any, **kwargs: Any) -> RecordingTransport:
        return RecordingTransport(real_cls(*args, **kwargs), tape)

    sdk_subprocess.SubprocessCLITransport = _factory  # type: ignore[misc,assignment]
    patched_client = hasattr(sdk_client, "SubprocessCLITransport")
    if patched_client:
        sdk_client.SubprocessCLITransport = _factory  # type: ignore[misc,assignment]
    try:
        yield tape
    finally:
        sdk_subprocess.SubprocessCLITransport = real_cls  # type: ignore[misc]
        if patched_client:
            sdk_client.SubprocessCLITransport = real_cls  # type: ignore[misc]

    # Only reached on clean exit — an exception in the block propagates above.
    if path is not None:
        _atomic_write(scrub_tape(tape, scrub) if scrub else tape, Path(path))


def _atomic_write(tape: list[TapeEntry], path: Path) -> None:
    """Write the tape via temp-file + rename, so a crash can't leave a torn file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(serialize_tape(tape))
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise
