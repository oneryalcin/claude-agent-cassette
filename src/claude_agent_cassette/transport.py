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
from collections import Counter, deque
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


# Synthetic answers for tolerated side-calls (issue #30). Only read-only
# telemetry subtypes are tolerable: answering an intent-bearing call
# (interrupt, set_model, set_permission_mode, ...) synthetically would certify
# a session the recording never had. Shapes are the minimal truthful-empty
# form of the installed SDK's response TypedDicts (``McpStatusResponse``,
# ``ContextUsageResponse``); a future SDK that adds a required key would
# surface as a consumer KeyError on a tolerated call — acceptable for a
# foreign-tape convenience.
_TOLERABLE_CONTROL_RESPONSES: dict[str, dict[str, Any]] = {
    "mcp_status": {"mcpServers": []},
    "get_context_usage": {
        "categories": [],
        "totalTokens": 0,
        "maxTokens": 0,
        "rawMaxTokens": 0,
        "percentage": 0.0,
        "model": "",
        "isAutoCompactEnabled": False,
        "memoryFiles": [],
        "mcpTools": [],
        "agents": [],
        "gridRows": [],
    },
}


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

    Recorded **Direction-B** ``control_response`` writes (the SDK answering a
    delivered ``can_use_tool`` / ``hook_callback`` / ``mcp_message`` request) are
    sync points too: the walk waits for the live SDK to answer that
    ``request_id`` before advancing — on the real wire the CLI does not proceed
    past a pending decision, and skipping the wait would let the terminal result
    arrive while a callback task is still running (the consumer then breaks,
    ``disconnect()`` cancels the callback, and verify mode reports a false
    "never answered" divergence). A recorded response whose request was *not*
    delivered (a subtype outside ``keep_subtypes``) is skipped — no live answer
    can come.

    Strict by design (the trade against the demux model's order-independence):
    the live client must issue control calls in recorded order, with recorded
    arguments. Fail-closed divergences, all :class:`CassetteMismatchError`:

    - the live client writes a control_request whose subtype differs from the
      recorded one at the sync point;
    - the subtypes match but the request payloads differ (``initialize`` is
      exempt: its payload encodes the replay environment's wiring — options,
      hook registrations — not consumer intent; stub/verify modes check hook
      ids separately);
    - the recorded control_request is never issued live within ``sync_timeout``
      seconds of the walk reaching it (and the same bound on a pending
      Direction-B answer);
    - a control_request is written after the tape is exhausted.

    Inside a control callback the SDK converts the raised error into the failing
    control call's exception (``interrupt()`` raises it directly); a consumer
    blocked in ``receive_messages()`` sees it as the stream error message.

    ``tolerate_subtypes`` (issue #30) relaxes exactly one of those divergences,
    opt-in, for **foreign tapes** — a tape recorded by a different consumer
    cannot contain the side-calls *this* consumer's connect/turn path adds
    (e.g. a ``get_mcp_status()`` health check). A live control_request whose
    subtype is in the set is answered with a synthetic **empty** success (never
    recorded data: ``mcp_status`` → ``{"mcpServers": []}``) instead of failing
    the walk — but only when **no remaining recorded Direction-A sync point
    records that subtype**: if the tape records the subtype later, the live
    write is held for strict matching there (answering it synthetically would
    orphan the recorded sync point; issuing it *before* the tape's recorded
    order remains divergence). Only read-only telemetry subtypes are accepted —
    an intent-bearing subtype in the set raises ``ValueError`` at construction.
    Tolerated calls are answered wherever the walk can run: between
    deliveries, parked at a Direction-A sync point or on a pending Direction-B
    answer (whose callback may itself be blocked on the side-call), and after
    the tape ends. The one state that cannot answer is the walk suspended on
    conversation back-pressure (full SDK message buffer, consumer not
    draining): the call then resolves at the caller's own timeout, harmlessly.
    Note that "read-only" describes the wire, not the consumer: a consumer
    that branches on the canned answer (e.g. memoising "no MCP servers") has
    its state shaped by it — usually exactly the inert behavior replay wants,
    but it is the consumer's real code running on synthetic data, which is why
    tolerance is opt-in. For first-party fidelity, record with your own
    consumer instead.

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
        tolerate_subtypes: set[str] | None = None,
    ) -> None:
        self._tape = tape
        self._keep_subtypes = keep_subtypes
        self._sync_timeout = sync_timeout
        self._tolerate = frozenset(tolerate_subtypes or ())
        unknown = self._tolerate - set(_TOLERABLE_CONTROL_RESPONSES)
        if unknown:
            raise ValueError(
                f"tolerate_subtypes {sorted(unknown)} are not tolerable — only "
                f"read-only telemetry subtypes "
                f"{sorted(_TOLERABLE_CONTROL_RESPONSES)} can be answered "
                "synthetically; tolerating an intent-bearing control call would "
                "certify a session the recording never had"
            )
        # Multiset of recorded Direction-A sync subtypes the walk has not yet
        # passed — the arbiter for tolerance (issue #30): a live tolerated
        # subtype is answered synthetically iff no remaining sync point records
        # it. Decremented as each sync point is matched, so at any park the
        # current sync's own subtype still counts as remaining (a live write of
        # that subtype goes to strict matching, never to tolerance).
        self._remaining_syncs = Counter(
            control_request_subtype(payload)
            for entry in tape
            if (payload := _write_payload(entry)) is not None
            and payload.get("type") == "control_request"
        )
        # Live outbound control_requests, in write order; _END on close. Items
        # the tolerance drain popped but could not answer wait in _held, ahead
        # of the queue, so FIFO order is preserved for sync matching.
        self._live_control_writes: asyncio.Queue[Any] = asyncio.Queue()
        self._held: deque[Any] = deque()
        # request_ids of live outbound control_responses (Direction-B answers);
        # the event wakes a walk parked on a pending answer whenever *anything*
        # is written (the answer itself, or a tolerated side-call issued from
        # inside the still-deciding callback — which the park must service, or
        # the callback deadlocks) and on close.
        self._live_b_response_ids: set[Any] = set()
        self._wire_activity = asyncio.Event()
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
        if not isinstance(message, dict):
            return
        # Conversation writes (user messages) never block or advance the walk.
        if message.get("type") == "control_request":
            await self._live_control_writes.put(message)
            self._wire_activity.set()
        elif message.get("type") == "control_response":
            self._live_b_response_ids.add((message.get("response") or {}).get("request_id"))
            self._wire_activity.set()

    async def read_messages(self) -> AsyncIterator[Frame]:
        live_id_by_recorded: dict[Any, Any] = {}
        delivered_b_requests: set[Any] = set()
        for entry in self._tape:
            # Service tolerated side-calls while the walk is still pumping: a
            # connect-time health check on a tape with a long pre-sync section
            # would otherwise wait for a park the walk may never reach — the
            # SDK's bounded message buffer suspends this generator first
            # (review finding).
            async for answer in self._drain_tolerated():
                yield answer
            if entry.get("dir") == "write":
                recorded = _write_payload(entry)
                if recorded is None:
                    continue
                if recorded.get("type") == "control_request":
                    # Park until the live SDK writes the request this sync point
                    # records, answering tolerated side-calls in the meantime
                    # (inline: a synthetic answer must be *yielded* while still
                    # parked — its caller may be what blocks the consumer from
                    # ever issuing the sync call, e.g. a health check inside
                    # connect()).
                    live = None
                    while live is None:
                        candidate = await self._next_live_write(recorded)
                        if candidate is _END:
                            return
                        if self._tolerable(candidate):
                            yield self._synthetic_success(candidate)
                            continue
                        self._require_match(recorded, candidate)
                        live = candidate
                    self._remaining_syncs[control_request_subtype(recorded)] -= 1
                    live_id_by_recorded[recorded.get("request_id")] = live.get("request_id")
                elif recorded.get("type") == "control_response":
                    # The SDK's recorded answer to a Direction-B request: wait for
                    # the live answer before advancing — unless the request was
                    # dropped (not kept), in which case none can come. While
                    # parked, service tolerated side-calls: the still-deciding
                    # callback may be blocked on one (a policy that
                    # health-checks before answering), and without an answer it
                    # can never produce the response this park waits for. A
                    # delivered request the SDK never answers within
                    # ``sync_timeout`` is divergence — a hung or cancelled
                    # callback would otherwise surface as a confusing
                    # downstream failure (e.g. a false "never answered" in
                    # verify mode).
                    rid = (recorded.get("response") or {}).get("request_id")
                    if rid not in delivered_b_requests:
                        continue
                    while rid not in self._live_b_response_ids:
                        if self._ended:
                            return
                        async for answer in self._drain_tolerated():
                            yield answer
                        self._wire_activity.clear()
                        # Re-check after clear: the answer (or close) may have
                        # arrived while the drain above was yielding.
                        if rid in self._live_b_response_ids or self._ended:
                            continue
                        try:
                            await asyncio.wait_for(
                                self._wire_activity.wait(), self._sync_timeout
                            )
                        except asyncio.TimeoutError:
                            raise CassetteMismatchError(
                                f"cassette mismatch: tape records the SDK's "
                                f"control_response to Direction-B request {rid!r} "
                                f"here, but the live SDK wrote none within "
                                f"{self._sync_timeout}s — the callback never "
                                "answered (hung, cancelled, or not invoked)"
                            ) from None
                continue
            frame = entry.get("frame") or {}
            frame_type = frame.get("type")
            if frame_type == "control_response":
                yield self._remapped_response(frame, live_id_by_recorded)
                continue
            if frame_type == "control_request":
                if (
                    self._keep_subtypes is None
                    or control_request_subtype(frame) not in self._keep_subtypes
                ):
                    continue
                delivered_b_requests.add(frame.get("request_id"))
            yield frame
        # Tape exhausted. Like the real wire, the stream stays open until the
        # client disconnects — but a further control_request has no recorded
        # answer, so fail closed instead of letting the call hit the SDK's 60s
        # control timeout.
        while True:
            live = (
                self._held.popleft()
                if self._held
                else await self._live_control_writes.get()
            )
            if live is _END:
                return
            if self._tolerable(live):
                # Every recorded sync point has passed, so any tolerated subtype
                # is by definition unrecorded here — e.g. a health check after
                # the recorded session ended.
                yield self._synthetic_success(live)
                continue
            raise CassetteMismatchError(
                f"cassette mismatch: live control_request "
                f"{control_request_subtype(live)!r} after the tape ended — no "
                "recorded response remains"
            )

    async def _next_live_write(self, recorded: Frame) -> Any:
        """The next live control_request write (or ``_END``), bounded by ``sync_timeout``.

        The walk is parked at the sync point recording ``recorded``; the timeout
        message names it. Each tolerated side-call answered while parked resets
        the bound — progress is being made.
        """
        if self._held:
            return self._held.popleft()
        try:
            return await asyncio.wait_for(
                self._live_control_writes.get(), self._sync_timeout
            )
        except asyncio.TimeoutError:
            raise CassetteMismatchError(
                f"cassette mismatch: tape records a control_request "
                f"{control_request_subtype(recorded)!r} here, but the live client "
                f"wrote none within {self._sync_timeout}s "
                "— the replay reached the recorded exchange and the live session "
                "never issued it (e.g. an interrupt tape replayed by a consumer "
                "that never calls interrupt())"
            ) from None

    def _require_match(self, recorded: Frame, live: Frame) -> None:
        """Fail closed unless ``live`` is the control_request this sync point records."""
        subtype = control_request_subtype(recorded)
        live_subtype = control_request_subtype(live)
        if live_subtype != subtype:
            raise CassetteMismatchError(
                f"cassette mismatch: tape records a control_request {subtype!r} "
                f"here, but the live client wrote {live_subtype!r} — the live "
                "control sequence diverged from the recorded order"
            )
        # Same subtype, different arguments is divergence too — handing the
        # recorded success to e.g. a set_model with a different model would
        # certify a session the recording never had. initialize is exempt: its
        # payload encodes the replay environment's wiring (options, hook
        # registrations), not consumer intent.
        if subtype != "initialize":
            recorded_args = recorded.get("request") or {}
            live_args = live.get("request") or {}
            if recorded_args != live_args:
                raise CassetteMismatchError(
                    f"cassette mismatch: live control_request {subtype!r} does "
                    f"not match the recorded arguments — recorded "
                    f"{recorded_args!r}, live {live_args!r}"
                )

    def _tolerable(self, live: Any) -> bool:
        """Tolerated subtype that no remaining recorded Direction-A sync point
        records — see ``_remaining_syncs``."""
        subtype = control_request_subtype(live)
        return subtype in self._tolerate and not self._remaining_syncs[subtype]

    def _synthetic_success(self, live: Frame) -> Frame:
        """A synthetic empty success for a tolerated side-call — never recorded
        data. Deep-copied so a consumer mutating one answer cannot contaminate
        the next."""
        subtype = control_request_subtype(live)
        assert subtype is not None  # _tolerable() admitted it, so it's allowlisted
        return {
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": live.get("request_id"),
                "response": copy.deepcopy(_TOLERABLE_CONTROL_RESPONSES[subtype]),
            },
        }

    async def _drain_tolerated(self) -> AsyncIterator[Frame]:
        """Answer every queued tolerated side-call, without blocking.

        Anything else the queue holds (a sync-point arrival, ``_END``) moves to
        ``_held`` in order, for the next park to consume. A held item is
        re-checked by the park's own tolerance loop, so one that becomes
        tolerable later (its recorded sync passed by matching an earlier write)
        is still answered.
        """
        while True:
            try:
                live = self._live_control_writes.get_nowait()
            except asyncio.QueueEmpty:
                return
            if live is not _END and self._tolerable(live):
                yield self._synthetic_success(live)
            else:
                self._held.append(live)

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
            self._wire_activity.set()  # wake a walk parked on a Direction-B answer


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
