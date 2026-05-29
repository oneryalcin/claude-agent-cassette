"""Opt-in capture of the full duplex SDK wire for a query."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from .tape import TapeEntry
from .transport import RecordingTransport


@contextmanager
def record_sdk_wire() -> Iterator[list[TapeEntry]]:
    """Tee the SDK wire for any query run inside the ``with`` block.

    Yields the tape (a growing list of :class:`TapeEntry`); on exit the SDK's
    transport constructor is restored. Wrapping *at construction* avoids
    replicating the SDK's internal transport setup.

    Usage::

        from claude_agent_cassette import record_sdk_wire, serialize_tape

        with record_sdk_wire() as tape:
            async for _ in query(prompt="...", options=...):
                pass
        Path("session.jsonl").write_text(serialize_tape(tape))

    Patches BOTH transport reference sites, because the SDK reaches the transport
    two ways: ``ClaudeSDKClient._connect_inner`` (the interactive client) does a
    *call-time* import from ``...transport.subprocess_cli``, while the one-shot
    ``query()`` (``InternalClient.process_query``) uses the name bound in
    ``_internal.client``. Patching only one silently misses the other.

    Caveats:
      - Touches ``claude_agent_sdk._internal`` — version-sensitive; pin your SDK.
      - Module-global patch — assumes one in-flight query per process.
    """
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
