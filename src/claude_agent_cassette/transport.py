"""Record & replay transports for the claude-agent-sdk wire.

``ReplayTransport`` feeds recorded raw stream-json dicts into a real
``ClaudeSDKClient`` — through the SDK's *real* message parser, so replay
exercises the actual parse path (a mock that hands over pre-typed objects would
not). ``RecordingTransport`` is a passive man-in-the-middle that tees every
frame, both directions, into a tape.

Both implement the public ``claude_agent_sdk.Transport`` ABC, so the SDK drives
them exactly as it would the real subprocess transport.
"""

from __future__ import annotations

import asyncio
import copy
import json
from collections import deque
from typing import Any, AsyncIterator, Optional

from claude_agent_sdk import Transport

from .tape import (
    Frame,
    TapeEntry,
    _write_payload,
    control_request_subtype,
    control_responses_by_subtype,
    direction_b_read_frames,
    conversation_frames,
)

# End-of-stream sentinel on the internal queue — a module-level singleton so
# identity comparison is unambiguous and never collides with a real frame.
_END = object()


class CassetteMismatchError(Exception):
    """A tape replay diverged from the recording.

    Raised by ``ReplayTransport.from_tape`` when the live SDK issues a Direction-A
    control_request whose ``subtype`` has no (remaining) recorded response — i.e.
    the live control sequence no longer matches the tape (SDK drift, a broadened
    caller, or a truncated recording). Fail-closed: surfacing the divergence is
    the point of a cassette, so it is never silently absorbed.
    """


class ReplayTransport(Transport):
    """Replays a recorded raw-dict stream into the SDK, in-process (no subprocess).

    Args:
        frames: recorded inbound frames (raw stream-json dicts) in order — the
            conversation to replay. Do NOT include the initialize
            ``control_response``; it is synthesised here from the live id.

    The non-obvious part: ``ClaudeSDKClient.connect()`` always runs the
    control-protocol ``initialize`` handshake — it ``write()``s a
    ``control_request`` with a freshly-minted ``request_id`` and blocks until
    ``read_messages()`` yields a ``control_response`` echoing that exact id. So
    this transport is not purely passive: it reads the live id off ``write()``
    and answers before streaming the recorded frames.

    Two construction paths:

    - ``ReplayTransport(frames)`` — conversation-only replay. Every
      ``control_request`` is answered with a synthesised generic success; no
      control-plane fidelity.
    - ``ReplayTransport.from_tape(tape)`` — replay a full duplex recording.
      Direction-A control_requests the SDK sends (``initialize``, ``mcp_status``,
      …) are answered from the *recorded* ``control_response`` for that request's
      ``subtype`` (id-remapped to the live request); inbound Direction-B
      control_requests (``mcp_message``/``hook_callback``/``can_use_tool``) are
      dropped so no live callback fires — replay stays inert.

      Matching is by **subtype** (per-subtype FIFO), not arrival order: the SDK
      demuxes responses by ``request_id`` (so the transport could hand back any
      payload without the SDK noticing), which means *the transport itself* is
      responsible for handing the right recorded response to the right request.
      A live Direction-A request whose subtype has no remaining recorded response
      is **fail-closed** — it raises :class:`CassetteMismatchError` rather than
      synthesising success, because silently absorbing divergence is exactly the
      drift a cassette exists to catch. Recorded responses for requests the live
      SDK never issues are simply unused (not an error) — replay must not be
      coupled to the *recording environment's* control sequence.

      Ordering-sensitive control (``interrupt``), where a conversation frame must
      land after a control exchange, needs lockstep interleaving — use
      :class:`LockstepReplayTransport` (what :func:`~claude_agent_cassette.replay_tape`
      auto-selects when the tape records an interrupt).

      **Flow-control constraint (same as the real transport):** a control method
      issued *during* replay (``client.get_mcp_status()``, ``set_model()``, …)
      only resolves while ``receive_messages()`` is being drained concurrently.
      The SDK forwards replayed frames into a bounded inbound buffer; if a tape
      has more replayable frames than that buffer and nothing is draining them,
      the read loop blocks and a later control response cannot be delivered — the
      control call then hangs. This is the real CLI's back-pressure faithfully
      reproduced (an undrained stdout pipe stalls the same way), not a replay
      artefact. The supported pattern is: drive ``connect()`` then drain
      ``receive_messages()`` (optionally issuing control calls from a concurrent
      task). :class:`LockstepReplayTransport` narrows the constraint to the real
      wire's (a response can be starved only by frames recorded *before* it,
      never by the rest of the tape).
    """

    def __init__(
        self,
        frames: list[Frame],
        recorded_responses_by_subtype: Optional[dict[str, deque[Frame]]] = None,
    ) -> None:
        self._frames = frames
        # Recorded Direction-A control_responses, keyed by the subtype of the
        # request they answered (per-subtype FIFO). None -> legacy generic-success
        # behaviour; a dict -> tape mode (subtype-matched, fail-closed).
        self._responses_by_subtype = recorded_responses_by_subtype
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._ready = False
        self._streamed = False
        self._ended = False
        # Exposed for write-side assertions (e.g. that initialize was sent).
        self.writes: list[str] = []

    @classmethod
    def from_tape(
        cls, tape: list[TapeEntry], keep_subtypes: set[str] | None = None
    ) -> ReplayTransport:
        """Build a control-aware replay from a full duplex tape.

        A thin assembler over the tape's control-protocol view: the conversation
        to stream and the recorded Direction-A answers keyed by request subtype
        (:func:`control_responses_by_subtype`).

        ``keep_subtypes`` selects the inbound-stream view:

        - ``None`` (default) — conversation only (:func:`conversation_frames`):
          Direction-B ``control_request``s are dropped so a consumer's registered
          callbacks stay **inert** on replay.
        - a set of subtypes — **Direction-B mode**: those ``control_request``s are
          kept (:func:`direction_b_read_frames`) so the SDK receives them and
          invokes its callbacks. The caller must install matching stubs (see
          :func:`~claude_agent_cassette.replay_tape`, which wires both), or those
          live callbacks will run.
        """
        if keep_subtypes is None:
            frames = conversation_frames(tape)
        else:
            frames = direction_b_read_frames(tape, keep_subtypes=keep_subtypes)
        return cls(frames, control_responses_by_subtype(tape))

    async def connect(self) -> None:
        self._ready = True

    def is_ready(self) -> bool:
        return self._ready

    async def write(self, data: str) -> None:
        self.writes.append(data)
        try:
            message = json.loads(data)
        except (TypeError, ValueError):
            return
        if isinstance(message, dict) and message.get("type") == "control_request":
            await self._answer_control_request(message)

    async def _answer_control_request(self, request: Frame) -> None:
        # Answer for the live request id (the SDK demuxes the response by
        # request_id), then stream the recorded conversation once.
        live_id = request.get("request_id")
        subtype = control_request_subtype(request)
        await self._queue.put(self._response_for(subtype, live_id))
        if not self._streamed:
            self._streamed = True
            for raw in self._frames:
                await self._queue.put(raw)

    def _response_for(self, subtype: Optional[str], live_id: Optional[str]) -> Frame:
        """The control_response to return for a live Direction-A request.

        Legacy mode (no tape) synthesises a generic success. Tape mode hands back
        the next recorded response for this ``subtype`` (deep-copied so the stored
        recording is never mutated, with ``request_id`` remapped to the live one);
        if none remains, it is **fail-closed** — :class:`CassetteMismatchError`.
        """
        if self._responses_by_subtype is None:
            return _control_response(live_id)
        bucket = self._responses_by_subtype.get(subtype or "")
        if not bucket:
            raise CassetteMismatchError(
                f"no recorded control_response for Direction-A request subtype "
                f"{subtype!r}; the live control sequence diverged from the tape"
            )
        rec = copy.deepcopy(bucket.popleft())
        if isinstance(rec.get("response"), dict):
            rec["response"]["request_id"] = live_id
        return rec

    async def read_messages(self):
        while True:
            item = await self._queue.get()
            if item is _END:
                return
            yield item

    async def end_input(self) -> None:
        await self._signal_end()

    async def close(self) -> None:
        await self._signal_end()

    async def _signal_end(self) -> None:
        # Guarded so the normal end_input()-then-close() sequence enqueues a
        # single sentinel. The client closes stdin after the terminal
        # ResultMessage, which is when a real transport's stdout would end.
        if not self._ended:
            self._ended = True
            await self._queue.put(_END)


class LockstepReplayTransport(Transport):
    """Replays a duplex tape **in recorded interleaving**: reads are delivered in
    tape order, and each recorded SDK ``control_request`` write is a *sync point*
    — delivery pauses until the live SDK writes the matching request.

    This is what ``interrupt`` needs: on the real wire the terminal result is
    *caused by* the interrupt, so a replay that delivers it independently (the
    :class:`ReplayTransport` demux model) can produce orderings the real system
    cannot — e.g. a Stop session's result arriving before the Stop was issued.
    Here the recorded response (and everything after it) is gated on the live
    write; the live ``request_id`` is learned at the sync point and the recorded
    ``control_response`` is remapped to it.

    The walk runs *inside* ``read_messages()``, so back-pressure is the real
    wire's: a frame is produced only when the SDK pulls it, and a control
    response can be starved only by frames recorded **before** it (the SDK
    routes ``control_response`` frames without buffering them), never by the
    rest of the tape.

    Strict by design (the trade against the demux model's order-independence):
    the live client must issue control calls in recorded order. Fail-closed
    divergences, all :class:`CassetteMismatchError`:

    - the live client writes a control_request whose subtype differs from the
      recorded one at the sync point;
    - the recorded control_request is never issued live within ``sync_timeout``
      seconds of the walk reaching it;
    - a control_request is written after the tape is exhausted.

    Inside a control callback the SDK converts the raised error into the failing
    control call's exception (``interrupt()`` raises it directly); a consumer
    blocked in ``receive_messages()`` sees it as the stream error message.

    ``keep_subtypes`` selects the Direction-B view exactly as in
    :meth:`ReplayTransport.from_tape`: ``None`` drops every inbound
    ``control_request`` (inert), a set keeps those subtypes so the (stubbed or
    real) callbacks fire. Early ``close()``/``end_input()`` stops the walk
    cleanly — like the demux model, disconnecting before the tape ends is the
    consumer's prerogative, not divergence.
    """

    def __init__(
        self,
        tape: list[TapeEntry],
        keep_subtypes: set[str] | None = None,
        sync_timeout: float = 5.0,
    ) -> None:
        self._tape = tape
        self._keep_subtypes = keep_subtypes
        self._sync_timeout = sync_timeout
        # Live outbound control_requests, in write order; _END on close.
        self._live_control_writes: asyncio.Queue[Any] = asyncio.Queue()
        self._ready = False
        self._ended = False
        # Exposed for write-side assertions and the verify-mode comparator.
        self.writes: list[str] = []

    async def connect(self) -> None:
        self._ready = True

    def is_ready(self) -> bool:
        return self._ready

    async def write(self, data: str) -> None:
        self.writes.append(data)
        try:
            message = json.loads(data)
        except (TypeError, ValueError):
            return
        # Only control_requests participate in sync-point matching; conversation
        # writes (user messages) and Direction-B control_response answers are
        # recorded in ``writes`` but never block or advance the walk.
        if isinstance(message, dict) and message.get("type") == "control_request":
            await self._live_control_writes.put(message)

    async def read_messages(self) -> AsyncIterator[Frame]:
        live_id_by_recorded: dict[Any, Any] = {}
        for entry in self._tape:
            if entry.get("dir") == "write":
                recorded = _write_payload(entry)
                if recorded is None or recorded.get("type") != "control_request":
                    continue
                live = await self._matching_live_request(recorded)
                if live is _END:
                    return
                live_id_by_recorded[recorded.get("request_id")] = live.get("request_id")
                continue
            frame = entry.get("frame") or {}
            frame_type = frame.get("type")
            if frame_type == "control_response":
                yield self._remapped_response(frame, live_id_by_recorded)
                continue
            if frame_type == "control_request" and (
                self._keep_subtypes is None
                or control_request_subtype(frame) not in self._keep_subtypes
            ):
                continue
            yield frame
        # Tape exhausted. Like the real wire, the stream stays open until the
        # client disconnects — but a further control_request has no recorded
        # answer, so fail closed instead of letting the call hit the SDK's 60s
        # control timeout.
        while True:
            live = await self._live_control_writes.get()
            if live is _END:
                return
            raise CassetteMismatchError(
                f"cassette mismatch: live control_request "
                f"{control_request_subtype(live)!r} after the tape ended — no "
                "recorded response remains"
            )

    async def _matching_live_request(self, recorded: Frame) -> Any:
        """Block until the live SDK writes the control_request this sync point records.

        Returns the live request frame (or ``_END`` if the consumer disconnected
        while the walk waited). A different live subtype, or no live write within
        ``sync_timeout``, is divergence.
        """
        subtype = control_request_subtype(recorded)
        try:
            live = await asyncio.wait_for(
                self._live_control_writes.get(), self._sync_timeout
            )
        except asyncio.TimeoutError:
            raise CassetteMismatchError(
                f"cassette mismatch: tape records a control_request {subtype!r} "
                f"here, but the live client wrote none within {self._sync_timeout}s "
                "— the replay reached the recorded exchange and the live session "
                "never issued it (e.g. an interrupt tape replayed by a consumer "
                "that never calls interrupt())"
            ) from None
        if live is _END:
            return live
        live_subtype = control_request_subtype(live)
        if live_subtype != subtype:
            raise CassetteMismatchError(
                f"cassette mismatch: tape records a control_request {subtype!r} "
                f"here, but the live client wrote {live_subtype!r} — the live "
                "control sequence diverged from the recorded order"
            )
        return live

    def _remapped_response(
        self, frame: Frame, live_id_by_recorded: dict[Any, Any]
    ) -> Frame:
        """The recorded Direction-A control_response, re-addressed to the live request.

        The recorded ``request_id`` was minted by the *recording* session's SDK; the
        live SDK demuxes by its own id, learned at the sync point. A response whose
        recorded id never passed a sync point would be silently dropped by the SDK —
        fail closed instead (a truncated or reordered tape).
        """
        recorded_id = (frame.get("response") or {}).get("request_id")
        if recorded_id not in live_id_by_recorded:
            raise CassetteMismatchError(
                f"cassette mismatch: recorded control_response for request_id "
                f"{recorded_id!r} has no preceding recorded control_request — "
                "truncated or reordered tape"
            )
        remapped = copy.deepcopy(frame)
        remapped["response"]["request_id"] = live_id_by_recorded[recorded_id]
        return remapped

    async def end_input(self) -> None:
        await self._signal_end()

    async def close(self) -> None:
        await self._signal_end()

    async def _signal_end(self) -> None:
        if not self._ended:
            self._ended = True
            await self._live_control_writes.put(_END)


class RecordingTransport(Transport):
    """Passive man-in-the-middle: delegate to ``inner``, tee every frame to ``tape``.

    Nothing is altered, dropped, or reordered — frames are appended to ``tape``
    in the order they cross the wire, so the recording stays byte-faithful.
    ``tape`` captures BOTH directions (incl. control), so one recording can feed
    conversation replay and control-protocol replay alike.
    """

    def __init__(self, inner: Transport, tape: list[TapeEntry]) -> None:
        self._inner = inner
        self._tape = tape

    async def connect(self) -> None:
        await self._inner.connect()

    def is_ready(self) -> bool:
        return self._inner.is_ready()

    async def write(self, data: str) -> None:
        self._tape.append({"dir": "write", "data": data})
        await self._inner.write(data)

    async def read_messages(self):
        async for raw in self._inner.read_messages():
            self._tape.append({"dir": "read", "frame": raw})
            yield raw

    async def end_input(self) -> None:
        await self._inner.end_input()

    async def close(self) -> None:
        await self._inner.close()


def _control_response(request_id: Optional[str]) -> Frame:
    """The success control_response the SDK's read loop matches by request id.

    Shape verified against claude-agent-sdk 0.2.x: ``Query._read_messages`` routes
    a ``control_response`` by ``message["response"]["request_id"]`` and treats
    ``subtype != "error"`` as success. If a future SDK changes this control schema,
    ``connect()`` will block on the handshake — re-verify this shape on bumps.
    """
    return {
        "type": "control_response",
        "response": {"request_id": request_id, "subtype": "success", "response": {}},
    }
