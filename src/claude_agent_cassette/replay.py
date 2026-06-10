"""High-level replay helper: drive a real ClaudeSDKClient over a cassette."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal, Optional

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from .control_stubs import (
    control_stub_options,
    control_verify_options,
    verify_direction_b_decisions,
    verify_initialize_hook_ids,
)
from .tape import RawMessage, TapeEntry
from .transport import ReplayTransport

ReplayMode = Literal["inert", "stub", "verify"]


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
      ``hook_callback`` / ``mcp_message`` requests are delivered to the SDK and answered
      from the tape by auto-installed stubs that **replace** the corresponding callbacks
      (and SDK MCP servers) in ``options``. Deterministic and inert: your live
      permission/hook/tool logic does not run; this certifies the recorded *wire*, not
      your policy.
    - ``mode="verify"``: the recorded Direction-B requests are delivered to **your
      real** ``can_use_tool`` / ``hooks`` / SDK MCP servers from ``options`` (nothing is
      replaced), and on exit each live decision is diffed against the recorded one —
      matched exactly by ``request_id``, at the wire. This certifies that your policy
      still produces the recorded decisions; a changed decision, a callback that now
      raises (or no longer does), or an unanswered exchange is divergence.

    **Fail-closed end-to-end.** In ``"stub"`` and ``"verify"`` modes, divergence from
    the tape — a live request with no recorded match, an exhausted or error-envelope
    decision, hook ids the SDK didn't reproduce, a live decision that differs from the
    recording, or recorded exchanges left unreplayed — raises
    :class:`~claude_agent_cassette.CassetteMismatchError` when the ``async with`` block
    exits cleanly. (The raise happens here, not inside the callback: the SDK swallows
    callback exceptions into error responses, so the divergence is collected and
    surfaced on exit.) A tape carrying a Direction-B subtype with no replay support
    (one a future SDK adds) raises up front — use ``mode="inert"`` for it.

    As with :func:`replay`, break at the terminal ``ResultMessage``; the stream stays
    open after it and ends on ``disconnect()``.

    Usage::

        async with replay_tape(load_tape("session.jsonl"), mode="stub") as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    if mode in ("stub", "verify"):
        build = control_stub_options if mode == "stub" else control_verify_options
        bundle = build(tape, options)
        transport = ReplayTransport.from_tape(tape, keep_control_requests=bundle.keep_subtypes)
        client = ClaudeSDKClient(options=bundle.options, transport=transport)
        await client.connect()
        if "hook_callback" in bundle.keep_subtypes:
            # The live initialize is now on the wire; confirm the SDK reproduced the
            # recorded hook ids (else the recorded requests won't route to the
            # replay's hooks).
            verify_initialize_hook_ids(transport.writes, tape, bundle.ledger)
        try:
            yield client
        finally:
            await client.disconnect()
        if mode == "verify":
            # All answers the consumer's real callbacks produced are now in
            # transport.writes — diff them against the recording.
            verify_direction_b_decisions(transport.writes, tape, bundle.ledger)
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
