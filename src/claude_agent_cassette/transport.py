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
from collections import defaultdict, deque
from typing import Any, Optional

from claude_agent_sdk import Transport

from .tape import RawMessage, TapeEntry, read_frames

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
        messages: recorded inbound frames (raw stream-json dicts) in order — the
            conversation to replay. Do NOT include the initialize
            ``control_response``; it is synthesised here from the live id.

    The non-obvious part: ``ClaudeSDKClient.connect()`` always runs the
    control-protocol ``initialize`` handshake — it ``write()``s a
    ``control_request`` with a freshly-minted ``request_id`` and blocks until
    ``read_messages()`` yields a ``control_response`` echoing that exact id. So
    this transport is not purely passive: it reads the live id off ``write()``
    and answers before streaming the recorded frames.

    Two construction paths:

    - ``ReplayTransport(messages)`` — conversation-only replay. Every
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
      land after a control exchange, needs lockstep interleaving and is out of
      scope here — see the issue tracker.
    """

    def __init__(
        self,
        messages: list[RawMessage],
        recorded_responses_by_subtype: Optional[dict[str, "deque[RawMessage]"]] = None,
    ) -> None:
        self._messages = messages
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
        # Observable for tests: the subtype of each Direction-A request answered.
        self.answered_subtypes: list[Optional[str]] = []

    @classmethod
    def from_tape(cls, tape: list["TapeEntry"]) -> "ReplayTransport":
        """Build a control-aware replay from a full duplex tape.

        - ``messages``: inbound conversation/system frames — every inbound frame
          that is neither a ``control_request`` (Direction B, dropped to stay
          inert) nor a ``control_response`` (answered separately, below).
        - ``recorded_responses_by_subtype``: the recorded ``control_response`` for
          each Direction-A request, keyed by that request's ``subtype``. Built by
          correlating each recorded SDK control_request (an outbound ``write``)
          with its recorded ``control_response`` on the recorded ``request_id``.
        """
        inbound = read_frames(tape)
        messages = [
            f for f in inbound
            if f.get("type") not in ("control_request", "control_response")
        ]
        # recorded control_responses, indexed by their recorded request_id
        responses_by_recorded_id = {
            (f.get("response") or {}).get("request_id"): f
            for f in inbound
            if f.get("type") == "control_response"
        }
        # join each recorded SDK control_request (write) to its response by id,
        # bucketed by the request subtype (preserving order within a subtype)
        by_subtype: dict[str, deque[RawMessage]] = defaultdict(deque)
        for entry in tape:
            if entry.get("dir") != "write":
                continue
            try:
                req = json.loads(entry["data"])
            except (TypeError, ValueError, KeyError):
                continue
            if req.get("type") != "control_request":
                continue
            subtype = (req.get("request") or {}).get("subtype")
            resp = responses_by_recorded_id.get(req.get("request_id"))
            if subtype is not None and resp is not None:
                by_subtype[subtype].append(resp)
        return cls(messages, recorded_responses_by_subtype=dict(by_subtype))

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

    async def _answer_control_request(self, request: RawMessage) -> None:
        # Answer for the live request id (the SDK demuxes the response by
        # request_id), then stream the recorded conversation once.
        live_id = request.get("request_id")
        subtype = (request.get("request") or {}).get("subtype")
        self.answered_subtypes.append(subtype)
        await self._queue.put(self._response_for(subtype, live_id))
        if not self._streamed:
            self._streamed = True
            for raw in self._messages:
                await self._queue.put(raw)

    def _response_for(self, subtype: Optional[str], live_id: Optional[str]) -> RawMessage:
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


def _control_response(request_id: Optional[str]) -> RawMessage:
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
