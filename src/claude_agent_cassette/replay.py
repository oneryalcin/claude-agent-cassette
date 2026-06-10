"""High-level replay helper: drive a real ClaudeSDKClient over a cassette."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal, Optional

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from .control_stubs import control_stub_options, verify_initialize_hook_ids
from .tape import RawMessage, TapeEntry
from .transport import ReplayTransport

ReplayMode = Literal["inert", "stub"]


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
    mode: ReplayMode = "inert",
) -> AsyncIterator[ClaudeSDKClient]:
    """Drive a real ``ClaudeSDKClient`` over a full **duplex tape** via ``from_tape``.

    Unlike :func:`replay` (which takes a list of inbound frames), this takes a whole
    recorded tape and replays its control plane too.

    - ``mode="inert"`` (default): conversation + **Direction-A** control replay — the
      ``initialize`` / ``mcp_status`` handshakes are answered from the recording; inbound
      **Direction-B** requests are dropped so registered callbacks stay inert.
    - ``mode="stub"``: also replay **Direction-B** — the recorded ``can_use_tool`` /
      ``hook_callback`` requests are delivered to the SDK and answered from the tape by
      auto-installed stubs that **replace** the corresponding callbacks in ``options``.
      Deterministic and inert: your live permission/hook logic does not run; this
      certifies the recorded *wire*, not your policy. (A future ``"verify"`` mode will
      run your real callbacks and assert they match the recording.)

    **Fail-closed end-to-end.** In ``"stub"`` mode, divergence from the tape — a live
    request with no recorded match, an exhausted or error-envelope decision, hook ids
    the SDK didn't reproduce, or recorded exchanges left unreplayed — raises
    :class:`~claude_agent_cassette.CassetteMismatchError` when the ``async with`` block
    exits cleanly. (The raise happens here, not inside the stub: the SDK swallows
    callback exceptions into error responses, so the divergence is collected and
    surfaced on exit.) A tape carrying a Direction-B subtype with no stub builder yet
    (``mcp_message``) raises up front — use ``mode="inert"`` for it.

    As with :func:`replay`, break at the terminal ``ResultMessage``; the stream stays
    open after it and ends on ``disconnect()``.

    Usage::

        async with replay_tape(load_tape("session.jsonl"), mode="stub") as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    if mode == "stub":
        bundle = control_stub_options(tape, options)
        transport = ReplayTransport.from_tape(tape, keep_control_requests=bundle.keep_subtypes)
        client = ClaudeSDKClient(options=bundle.options, transport=transport)
        await client.connect()
        if "hook_callback" in bundle.keep_subtypes:
            # The live initialize is now on the wire; confirm the SDK reproduced the
            # recorded hook ids (else the stubs won't be invoked).
            verify_initialize_hook_ids(transport.writes, tape, bundle.ledger)
        try:
            yield client
        finally:
            await client.disconnect()
        # Only on clean exit (a consumer error propagates through the finally above and
        # skips this) — surface any divergence the SDK swallowed during replay.
        bundle.ledger.raise_if_diverged()
    else:
        client = ClaudeSDKClient(
            options=options or ClaudeAgentOptions(),
            transport=ReplayTransport.from_tape(tape),
        )
        await client.connect()
        try:
            yield client
        finally:
            await client.disconnect()
