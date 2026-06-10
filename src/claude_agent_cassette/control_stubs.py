"""Stub callbacks that replay recorded Direction-B decisions.

Direction-B replay needs the SDK to invoke *some* callback when it receives a
recorded ``control_request`` — but running the consumer's live callback would
defeat the point (real permission checks, hooks, in-process MCP execution, all
with side effects). These builders turn the recorded decisions
(:func:`~claude_agent_cassette.direction_b_exchanges`) into callbacks that hand
back the *recorded* outcome.

**Where divergence is surfaced.** The SDK runs each Direction-B callback inside a
``try/except Exception`` that converts *any* exception into a benign ``error``
``control_response`` (``Query._handle_control_request``). So a stub that raises on
divergence **cannot** fail the consumer's ``receive_messages()`` loop — the error is
swallowed and replay completes green. Stubs therefore record divergence into a
:class:`ControlReplayLedger` as well as raising (the raise still fail-closes the
*direct*-call path used in unit tests), and :func:`~claude_agent_cassette.replay_tape`
calls :meth:`ControlReplayLedger.raise_if_diverged` on clean exit. That makes the
"never certify what the tape didn't record" guarantee hold through the real path,
not just in isolation.

Matching: the SDK strips the control ``request_id`` before calling the callback
(``can_use_tool`` only sees ``(tool_name, input, context)``), so a stub correlates a
live request to a recorded exchange by ``(tool_name, input)`` — FIFO among identical
requests — which is exact because Direction-B replay feeds the SDK the *recorded*
requests. Hooks correlate by ``callback_id`` (FIFO per id); the live SDK must
re-assign the recorded ids, which :func:`verify_initialize_hook_ids` checks at the
wire.
"""

from __future__ import annotations

import dataclasses
import json
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, NamedTuple

from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookMatcher,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

from .tape import (
    ControlExchange,
    TapeEntry,
    direction_b_exchanges,
    recorded_hook_config,
)
from .transport import CassetteMismatchError

CanUseTool = Callable[
    [str, dict[str, Any], ToolPermissionContext],
    Awaitable["PermissionResultAllow | PermissionResultDeny"],
]

# Direction-B subtypes this module can replay today. A tape carrying any other
# Direction-B subtype (e.g. ``mcp_message``) is not yet stub-replayable.
SUPPORTED_SUBTYPES = frozenset({"can_use_tool", "hook_callback"})


class ControlReplayLedger:
    """Collects Direction-B replay divergence so it can be surfaced *after* replay.

    Two kinds of divergence are recorded: a stub mismatch (a live request with no
    recorded match, an exhausted decision, or a recorded *error* envelope), via
    :meth:`record`; and recorded exchanges that were never replayed — registered with
    :meth:`track` and counted as leftovers at exit. :func:`~claude_agent_cassette.replay_tape`
    calls :meth:`raise_if_diverged` on clean completion.
    """

    def __init__(self) -> None:
        self._mismatches: list[str] = []
        self._tracked: list[tuple[str, "deque[Any] | list[Any]"]] = []

    def record(self, message: str) -> None:
        """Record a divergence the SDK would otherwise have swallowed."""
        self._mismatches.append(message)

    def track(self, label: str, remaining: "deque[Any] | list[Any]") -> None:
        """Register a collection of recorded exchanges; whatever is left at exit diverged."""
        self._tracked.append((label, remaining))

    def diverged(self) -> bool:
        return bool(self._mismatches) or any(remaining for _, remaining in self._tracked)

    def raise_if_diverged(self) -> None:
        problems = list(self._mismatches)
        for label, remaining in self._tracked:
            if remaining:
                problems.append(
                    f"{len(remaining)} recorded {label} exchange(s) were never replayed"
                )
        if problems:
            raise CassetteMismatchError(
                "Direction-B replay diverged from the recording (the SDK swallows stub "
                "errors into error responses, so this is surfaced on exit):\n  - "
                + "\n  - ".join(problems)
            )


class ControlStubBundle(NamedTuple):
    """What :func:`~claude_agent_cassette.replay_tape` needs to drive a Direction-B replay."""

    options: ClaudeAgentOptions  # a copy of the caller's options with replay stubs installed
    keep_subtypes: set[str]  # control_request subtypes to keep in the inbound stream
    ledger: ControlReplayLedger  # divergence sink; raise_if_diverged() after replay


def _permission_result(
    exchange: ControlExchange, ledger: ControlReplayLedger
) -> PermissionResultAllow | PermissionResultDeny:
    """Reconstruct the SDK ``PermissionResult`` from a recorded can_use_tool decision.

    A recorded *error* envelope (the original callback raised) or an unrecognized
    behavior is divergence: recorded into ``ledger`` and raised (the raise fail-closes
    the direct-call path; the ledger fail-closes the swallowed end-to-end path).
    """
    if not exchange.succeeded:
        msg = (
            f"recorded can_use_tool response for {exchange.request_id!r} was an error "
            "envelope, not a decision — cannot replay it as a permission result"
        )
        ledger.record(msg)
        raise CassetteMismatchError(msg)
    decision = exchange.decision
    behavior = decision.get("behavior")
    if behavior == "allow":
        kwargs: dict[str, Any] = {}
        if "updatedInput" in decision:
            kwargs["updated_input"] = decision["updatedInput"]
        if decision.get("updatedPermissions") is not None:
            from claude_agent_sdk import PermissionUpdate

            kwargs["updated_permissions"] = [
                PermissionUpdate.from_dict(p) for p in decision["updatedPermissions"]
            ]
        return PermissionResultAllow(**kwargs)
    if behavior == "deny":
        return PermissionResultDeny(
            message=decision.get("message", ""),
            interrupt=bool(decision.get("interrupt", False)),
        )
    msg = f"unrecognized recorded permission behavior {behavior!r} for {exchange.request_id!r}"
    ledger.record(msg)
    raise CassetteMismatchError(msg)


def build_permission_stub(tape: list[TapeEntry], ledger: ControlReplayLedger) -> CanUseTool:
    """A ``can_use_tool`` callback that replays the tape's recorded permission decisions.

    Each recorded ``can_use_tool`` request the SDK receives is answered with the
    decision recorded for it, matched by ``(tool_name, input)``. A live request with no
    remaining recorded match is divergence — recorded into ``ledger`` and raised. The
    recorded exchanges are tracked so any left unconsumed at exit also count as
    divergence.
    """
    remaining: list[ControlExchange] = list(direction_b_exchanges(tape).get("can_use_tool", ()))
    ledger.track("can_use_tool", remaining)

    async def can_use_tool(
        tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
    ) -> PermissionResultAllow | PermissionResultDeny:
        for i, exchange in enumerate(remaining):
            request = exchange.request
            if request.get("tool_name") == tool_name and request.get("input") == tool_input:
                return _permission_result(remaining.pop(i), ledger)
        msg = (
            f"no recorded can_use_tool decision for live request tool={tool_name!r} "
            f"input={tool_input!r}; the live permission sequence diverged from the tape"
        )
        ledger.record(msg)
        raise CassetteMismatchError(msg)

    return can_use_tool


def _make_hook_stub(
    callback_id: str, recorded: deque[ControlExchange], ledger: ControlReplayLedger
) -> Callable[..., Any]:
    """A hook callback that replays the recorded outputs for one ``callback_id`` (FIFO).

    Firing more times than recorded, or a recorded *error* envelope, is divergence —
    recorded into ``ledger`` and raised.
    """

    async def hook(input_data: Any, tool_use_id: Any, context: Any) -> dict[str, Any]:
        if not recorded:
            msg = (
                f"hook {callback_id!r} fired more times than recorded; "
                "the live hook sequence diverged from the tape"
            )
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        exchange = recorded.popleft()
        if not exchange.succeeded:
            msg = (
                f"recorded hook_callback for {callback_id!r} was an error envelope, not an "
                "output — cannot replay it as a hook result"
            )
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        return exchange.decision

    return hook


def build_hook_stubs(
    tape: list[TapeEntry], ledger: ControlReplayLedger
) -> dict[str, list[HookMatcher]] | None:
    """A ``ClaudeAgentOptions.hooks`` structure that replays the tape's hook outputs.

    Mirrors the recorded ``initialize`` hook structure (:func:`recorded_hook_config`),
    preserving matcher and timeout, and registers a stub per hook keyed by its recorded
    ``callback_id``. Returns ``None`` if the tape recorded no hooks.

    Reproducibility of the ``callback_id``s (the SDK re-assigns them at ``initialize``)
    is **not** predicted here — that was a brittle coupling to the SDK's numbering. It is
    instead checked against the live ``initialize`` write by :func:`verify_initialize_hook_ids`,
    which catches both tape-side corruption *and* a replay-time SDK scheme change.
    """
    config = recorded_hook_config(tape)
    if not config:
        return None

    outputs: dict[str, deque[ControlExchange]] = defaultdict(deque)
    for exchange in direction_b_exchanges(tape).get("hook_callback", ()):
        callback_id = exchange.request.get("callback_id")
        if callback_id is not None:
            outputs[callback_id].append(exchange)
    for callback_id, recorded in outputs.items():
        ledger.track(f"hook_callback[{callback_id}]", recorded)

    hooks: dict[str, list[HookMatcher]] = {}
    for event, matchers in config.items():
        matcher_list: list[HookMatcher] = []
        for matcher in matchers:
            stubs = [
                _make_hook_stub(callback_id, outputs[callback_id], ledger)
                for callback_id in matcher.get("hookCallbackIds") or []
            ]
            matcher_list.append(
                HookMatcher(
                    matcher=matcher.get("matcher"),
                    hooks=stubs,
                    timeout=matcher.get("timeout"),  # the SDK mirrors this at initialize
                )
            )
        hooks[event] = matcher_list
    return hooks


def _flatten_hook_ids(config: dict[str, Any] | None) -> list[str]:
    """The hookCallbackIds across a hook config, in event/matcher/hook order."""
    ids: list[str] = []
    for matchers in (config or {}).values():
        for matcher in matchers:
            ids.extend(matcher.get("hookCallbackIds") or [])
    return ids


def verify_initialize_hook_ids(
    writes: list[str], tape: list[TapeEntry], ledger: ControlReplayLedger
) -> None:
    """Check the *live* SDK re-assigned the recorded hook ``callback_id``s — at the wire.

    The hook stubs only resolve if the live SDK numbers the hooks exactly as the
    recording did. Rather than predict the SDK's numbering (a coupling to an internal
    that the realistic break — an SDK scheme change — would defeat), compare the
    recorded ids against the ones in the SDK's live ``initialize`` write. A mismatch
    (scrubbed recorded ids, or a changed SDK scheme) is recorded as divergence so the
    hooks-never-fired case surfaces with a clear message instead of a bare leftover.
    """
    recorded_ids = _flatten_hook_ids(recorded_hook_config(tape))
    live_config = None
    for data in writes:
        try:
            frame = json.loads(data)
        except ValueError:
            continue
        if frame.get("type") == "control_request" and (frame.get("request") or {}).get(
            "subtype"
        ) == "initialize":
            live_config = frame["request"].get("hooks")
            break
    live_ids = _flatten_hook_ids(live_config)
    if recorded_ids != live_ids:
        ledger.record(
            f"hook callback ids not reproduced on replay: recorded {recorded_ids}, the live "
            f"SDK assigned {live_ids} — the recorded hook_callback requests won't resolve to "
            "the stubs (scrubbed recording, or the SDK changed its id scheme)"
        )


def control_stub_options(
    tape: list[TapeEntry], base_options: ClaudeAgentOptions | None = None
) -> ControlStubBundle:
    """Wire Direction-B replay stubs for every subtype a tape contains.

    Returns a :class:`ControlStubBundle` — a copy of ``base_options`` with replay stubs
    installed, the ``control_request`` subtypes to keep in the inbound stream, and a
    :class:`ControlReplayLedger` to surface divergence after replay.
    :func:`~claude_agent_cassette.replay_tape` calls this; advanced callers can use it to
    wire the transport and options by hand.

    **Fail-closed, not best-effort.** If the tape contains a Direction-B subtype that
    can't be faithfully replayed — one with no stub builder yet (``mcp_message``) — this
    raises :class:`~claude_agent_cassette.CassetteMismatchError` rather than silently
    replaying a subset. Use ``mode="inert"`` to replay the conversation without the
    control plane. (Hooks whose ids can't be reproduced are caught at replay by
    :func:`verify_initialize_hook_ids`, not dropped silently.)

    Installing the ``can_use_tool`` stub also clears ``permission_prompt_tool_name`` on
    the copy: the SDK rejects a client where both are set, so a caller's
    prompt-tool-style options would otherwise fail at ``connect()`` before replay.
    """
    exchanges = direction_b_exchanges(tape)
    unsupported = set(exchanges) - SUPPORTED_SUBTYPES
    if unsupported:
        raise CassetteMismatchError(
            f"tape contains Direction-B subtype(s) not yet replayable: {sorted(unsupported)}. "
            "Replay with mode='inert' to play the conversation without the control plane, "
            "or re-record without them."
        )

    options = base_options or ClaudeAgentOptions()
    ledger = ControlReplayLedger()
    keep: set[str] = set()
    if exchanges.get("can_use_tool"):
        options = dataclasses.replace(
            options,
            can_use_tool=build_permission_stub(tape, ledger),
            permission_prompt_tool_name=None,  # mutually exclusive with can_use_tool
        )
        keep.add("can_use_tool")
    if exchanges.get("hook_callback"):
        hooks = build_hook_stubs(tape, ledger)
        if hooks:
            # hooks keys are recorded event strings (e.g. "PreToolUse"); the SDK types
            # them as a HookEvent literal and validates at runtime.
            options = dataclasses.replace(options, hooks=hooks)  # type: ignore[arg-type]
            keep.add("hook_callback")
    return ControlStubBundle(options=options, keep_subtypes=keep, ledger=ledger)
