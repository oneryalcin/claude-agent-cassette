"""High-level replay helper: drive a real ClaudeSDKClient over a cassette."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from .control_stubs import control_stub_options
from .tape import RawMessage, TapeEntry
from .transport import ReplayTransport


@asynccontextmanager
async def replay(
    messages: list[RawMessage],
    options: Optional[ClaudeAgentOptions] = None,
) -> AsyncIterator[ClaudeSDKClient]:
    """Drive a real ``ClaudeSDKClient`` over a ``ReplayTransport`` fed ``messages``.

    No API key, no subprocess — the recorded frames flow through the SDK's real
    parser. Iterate ``receive_messages()`` and assert on the typed messages your
    app would see.

    Break out of ``receive_messages()`` at the terminal ``ResultMessage`` — like the
    real wire, the replay stream stays open after it (so a mid-drain control call can
    still get its answer) and only ends on ``disconnect()``.

    Usage::

        from claude_agent_cassette import replay, load_cassette

        async with replay(load_cassette("session.jsonl")) as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    client = ClaudeSDKClient(
        options=options or ClaudeAgentOptions(),
        transport=ReplayTransport(messages),
    )
    await client.connect()
    try:
        yield client
    finally:
        await client.disconnect()


@asynccontextmanager
async def replay_tape(
    tape: list[TapeEntry],
    options: Optional[ClaudeAgentOptions] = None,
    control: bool = False,
) -> AsyncIterator[ClaudeSDKClient]:
    """Drive a real ``ClaudeSDKClient`` over a full **duplex tape** via ``from_tape``.

    Unlike :func:`replay` (which takes a list of inbound frames), this takes a whole
    recorded tape and replays its control plane too.

    - ``control=False`` (default): conversation + **Direction-A** control replay —
      the ``initialize`` / ``mcp_status`` handshakes are answered from the recording;
      inbound **Direction-B** requests are dropped so registered callbacks stay inert.
    - ``control=True``: also replay **Direction-B** — the recorded ``can_use_tool``
      requests are delivered to the SDK and answered from the tape by an
      auto-installed stub (it **replaces** any ``can_use_tool`` in ``options``). The
      replay is deterministic and inert: your live permission logic does not run. Only
      ``can_use_tool`` is wired today; other Direction-B subtypes stay dropped/inert.

    As with :func:`replay`, break at the terminal ``ResultMessage``; the stream stays
    open after it and ends on ``disconnect()``.

    Usage::

        async with replay_tape(load_tape("session.jsonl"), control=True) as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    if control:
        options, keep = control_stub_options(tape, options)
        transport = ReplayTransport.from_tape(tape, keep_control_requests=keep)
    else:
        options = options or ClaudeAgentOptions()
        transport = ReplayTransport.from_tape(tape)
    client = ClaudeSDKClient(options=options, transport=transport)
    await client.connect()
    try:
        yield client
    finally:
        await client.disconnect()
