"""High-level replay helper: drive a real ClaudeSDKClient over a cassette."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Literal, Optional

from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

from .direction_b import (
    control_stub_bundle,
    control_verify_bundle,
    verify_direction_b_decisions,
    verify_initialize_hook_ids,
)
from .tape import Frame, TapeEntry, records_interrupt
from .transport import LockstepReplayTransport, ReplayTransport

ReplayMode = Literal["inert", "stub", "verify"]


@asynccontextmanager
async def replay(
    frames: list[Frame],
    options: Optional[ClaudeAgentOptions] = None,
) -> AsyncIterator[ClaudeSDKClient]:
    """Drive a real ``ClaudeSDKClient`` over a ``ReplayTransport`` fed ``frames``.

    No API key, no subprocess — the recorded frames flow through the SDK's real
    parser. Iterate ``receive_messages()`` and assert on the typed messages your
    app would see.

    Break out of ``receive_messages()`` at the terminal ``ResultMessage`` — like the
    real wire, the replay stream stays open after it (so a mid-drain control call can
    still get its answer) and only ends on ``disconnect()``.

    Usage::

        from claude_agent_cassette import replay, load_frames

        async with replay(load_frames("session.jsonl")) as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    client = ClaudeSDKClient(
        options=options or ClaudeAgentOptions(),
        transport=ReplayTransport(frames),
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
    *,
    lockstep: Optional[bool] = None,
    sync_timeout: float = 5.0,
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

    **Delivery model.** By default frames are delivered by the *demux* model
    (order-independent: any recorded Direction-A response answers the matching live
    request whenever it arrives) — unless the tape records an ``interrupt``, in which
    case **lockstep** delivery is auto-selected: reads are delivered in recorded
    interleaving and each recorded SDK control_request gates everything after it on
    the matching live write. ``interrupt`` is causally ordered on the real wire (the
    terminal result is a *consequence* of it), so demux could deliver an impossible
    ordering — e.g. a Stop session's result before the consumer issues the Stop.
    Pass ``lockstep=True``/``False`` to force either model; ``sync_timeout`` bounds
    how long lockstep waits at a recorded control write for the live one (then
    :class:`~claude_agent_cassette.CassetteMismatchError`, e.g. an interrupt tape
    replayed by a consumer that never interrupts). Lockstep is strict: the live
    session must issue control calls in recorded order.

    As with :func:`replay`, break at the terminal ``ResultMessage``; the stream stays
    open after it and ends on ``disconnect()``.

    Usage::

        async with replay_tape(load_tape("session.jsonl"), mode="stub") as client:
            async for message in client.receive_messages():
                if type(message).__name__ == "ResultMessage":
                    break
    """
    use_lockstep = records_interrupt(tape) if lockstep is None else lockstep

    def build_transport(keep_subtypes: Optional[set[str]] = None):
        if use_lockstep:
            return LockstepReplayTransport(
                tape, keep_subtypes=keep_subtypes, sync_timeout=sync_timeout
            )
        return ReplayTransport.from_tape(tape, keep_subtypes=keep_subtypes)

    if mode in ("stub", "verify"):
        build = control_stub_bundle if mode == "stub" else control_verify_bundle
        bundle = build(tape, options)
        transport = build_transport(keep_subtypes=bundle.keep_subtypes)
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
            transport=build_transport(),
        )
        await client.connect()
        try:
            yield client
        finally:
            await client.disconnect()
