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
import json
from typing import Any, Optional

from claude_agent_sdk import Transport

from .tape import RawMessage, TapeEntry

# End-of-stream sentinel on the internal queue — a module-level singleton so
# identity comparison is unambiguous and never collides with a real frame.
_END = object()


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
      …) are answered from the *recorded* ``control_response`` (id-remapped to the
      live request), and inbound Direction-B control_requests
      (``mcp_message``/``hook_callback``/``can_use_tool``) are dropped so no live
      callback fires — replay stays inert. Control_responses are demuxed by the
      SDK on ``request_id`` independent of conversation order, so the recorded
      responses are answered as the SDK issues each request; no lockstep needed.
      Ordering-sensitive control (``interrupt``) is a separate concern — see the
      module docs / issue tracker.
    """

    def __init__(
        self,
        messages: list[RawMessage],
        recorded_responses: Optional[list[RawMessage]] = None,
    ) -> None:
        self._messages = messages
        # Recorded Direction-A control_responses, in order. When present, each
        # SDK control_request is answered from the next one (id-remapped) instead
        # of a synthesised success. None -> legacy generic-success behaviour.
        self._recorded_responses = recorded_responses
        self._response_idx = 0
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._ready = False
        self._streamed = False
        self._ended = False
        # Exposed for write-side assertions (e.g. that initialize was sent).
        self.writes: list[str] = []

    @classmethod
    def from_tape(cls, tape: list["TapeEntry"]) -> "ReplayTransport":
        """Build a control-aware replay from a full duplex tape.

        - ``messages``: inbound conversation/system frames — every inbound frame
          that is neither a ``control_request`` (Direction B, dropped to stay
          inert) nor a ``control_response`` (answered separately, below).
        - ``recorded_responses``: inbound ``control_response`` frames, in order —
          the recorded answers to the SDK's Direction-A requests.
        """
        inbound = [e["frame"] for e in tape if e.get("dir") == "read"]
        messages = [
            f for f in inbound
            if f.get("type") not in ("control_request", "control_response")
        ]
        recorded_responses = [
            f for f in inbound if f.get("type") == "control_response"
        ]
        return cls(messages, recorded_responses=recorded_responses)

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
        # Answer for the live request id (unblocks the SDK, which demuxes the
        # response by request_id), then stream the recorded conversation once.
        live_id = request.get("request_id")
        await self._queue.put(self._response_for(live_id))
        if not self._streamed:
            self._streamed = True
            for raw in self._messages:
                await self._queue.put(raw)

    def _response_for(self, live_id: Optional[str]) -> RawMessage:
        """The control_response to return for an SDK request id.

        With a recorded tape, hand back the next recorded ``control_response``
        (deep-copied, with its ``request_id`` remapped to the live one the SDK
        minted this run); otherwise synthesise a generic success. Falls back to
        generic success if the SDK issues more requests than were recorded.
        """
        if self._recorded_responses and self._response_idx < len(self._recorded_responses):
            rec = json.loads(json.dumps(self._recorded_responses[self._response_idx]))
            self._response_idx += 1
            if isinstance(rec.get("response"), dict):
                rec["response"]["request_id"] = live_id
            return rec
        return _control_response(live_id)

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
