"""Stub callbacks that replay recorded Direction-B decisions.

Direction-B replay needs the SDK to invoke *some* callback when it receives a
recorded ``control_request`` — but running the consumer's live callback would
defeat the point (real permission checks, hooks, in-process MCP execution, all
with side effects). These builders turn the recorded decisions
(:func:`~claude_agent_cassette.direction_b_exchanges`) into callbacks that hand
back the *recorded* outcome.

Matching: the SDK strips the control ``request_id`` before calling the callback
(``can_use_tool`` only sees ``(tool_name, input, context)``), so a stub cannot
correlate by id. It matches a live request to a recorded exchange by
``(tool_name, input)`` — FIFO among identical requests — which is exact because
Direction-B replay feeds the SDK the *recorded* requests, so the input the stub
receives is byte-for-byte the one in the tape. A live request with no recorded
match (or more requests than recorded) is **fail-closed**:
:class:`~claude_agent_cassette.CassetteMismatchError`, never an invented decision.
"""

from __future__ import annotations

import dataclasses
import warnings
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable

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


def _permission_result(exchange: ControlExchange) -> PermissionResultAllow | PermissionResultDeny:
    """Reconstruct the SDK ``PermissionResult`` from a recorded can_use_tool decision.

    The recorded ``decision`` is the wire payload the SDK wrote back at record time
    (``{behavior: allow, updatedInput, …}`` or ``{behavior: deny, message, …}``); this
    rebuilds the typed result the live callback would have returned to produce it.
    """
    if not exchange.succeeded:
        raise CassetteMismatchError(
            f"recorded can_use_tool response for {exchange.request_id!r} was an error "
            "envelope, not a decision — cannot replay it as a permission result"
        )
    decision = exchange.decision
    behavior = decision.get("behavior")
    if behavior == "allow":
        kwargs: dict[str, Any] = {}
        if "updatedInput" in decision:
            kwargs["updated_input"] = decision["updatedInput"]
        if decision.get("updatedPermissions") is not None:
            # Reconstruct typed permission updates only when the recording carried them.
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
    raise CassetteMismatchError(
        f"unrecognized recorded permission behavior {behavior!r} for {exchange.request_id!r}"
    )


def build_permission_stub(tape: list[TapeEntry]) -> CanUseTool:
    """A ``can_use_tool`` callback that replays the tape's recorded permission decisions.

    Install it as ``ClaudeAgentOptions(can_use_tool=build_permission_stub(tape))`` for a
    Direction-B replay: each recorded ``can_use_tool`` request the SDK receives is
    answered with the decision recorded for it, matched by ``(tool_name, input)``.
    Fail-closed on any live request with no remaining recorded match.
    """
    remaining: list[ControlExchange] = list(
        direction_b_exchanges(tape).get("can_use_tool", ())
    )

    async def can_use_tool(
        tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
    ) -> PermissionResultAllow | PermissionResultDeny:
        for i, exchange in enumerate(remaining):
            request = exchange.request
            if request.get("tool_name") == tool_name and request.get("input") == tool_input:
                return _permission_result(remaining.pop(i))
        raise CassetteMismatchError(
            f"no recorded can_use_tool decision for live request tool={tool_name!r} "
            f"input={tool_input!r}; the live permission sequence diverged from the tape "
            f"({len(remaining)} recorded decision(s) unused)"
        )

    return can_use_tool


def _make_hook_stub(callback_id: str, recorded: deque[ControlExchange]) -> Callable[..., Any]:
    """A hook callback that replays the recorded outputs for one ``callback_id`` (FIFO).

    We feed the SDK the recorded ``hook_callback`` requests in order, so this stub is
    invoked exactly when its ``callback_id``'s recorded exchange comes up. Firing more
    times than recorded is **fail-closed** (divergence, not a silent empty output).
    """

    async def hook(input_data: Any, tool_use_id: Any, context: Any) -> dict[str, Any]:
        if not recorded:
            raise CassetteMismatchError(
                f"hook {callback_id!r} fired more times than recorded; "
                "the live hook sequence diverged from the tape"
            )
        return recorded.popleft().decision

    return hook


def build_hook_stubs(tape: list[TapeEntry]) -> dict[str, list[HookMatcher]] | None:
    """A ``ClaudeAgentOptions.hooks`` structure that replays the tape's hook outputs.

    Mirrors the recorded ``initialize`` hook structure (:func:`recorded_hook_config`)
    so the SDK re-assigns the same ``callback_id``s, and registers a stub per hook that
    returns the recorded output for its id (FIFO). Returns ``None`` if the tape recorded
    no hooks.

    Fail-closed on a recording whose hook ids can't be reproduced: the SDK numbers
    hooks ``hook_0``, ``hook_1``, … in registration order from a fresh counter, so this
    verifies each mirrored position lands on the recorded id (e.g. a tape with scrubbed
    or non-contiguous ids raises rather than silently mis-routing a hook).
    """
    config = recorded_hook_config(tape)
    if not config:
        return None

    outputs: dict[str, deque[ControlExchange]] = defaultdict(deque)
    for exchange in direction_b_exchanges(tape).get("hook_callback", ()):
        callback_id = exchange.request.get("callback_id")
        if callback_id is not None:
            outputs[callback_id].append(exchange)

    hooks: dict[str, list[HookMatcher]] = {}
    counter = 0
    for event, matchers in config.items():
        matcher_list: list[HookMatcher] = []
        for matcher in matchers:
            stubs = []
            for callback_id in matcher.get("hookCallbackIds") or []:
                live_id = f"hook_{counter}"
                counter += 1
                if live_id != callback_id:
                    raise CassetteMismatchError(
                        f"recorded hook callback_id {callback_id!r} would be re-assigned "
                        f"{live_id!r} on replay; the recording's hook ids can't be "
                        "reproduced (scrubbed or non-contiguous from hook_0)"
                    )
                stubs.append(_make_hook_stub(callback_id, outputs[callback_id]))
            matcher_list.append(HookMatcher(matcher=matcher.get("matcher"), hooks=stubs))
        hooks[event] = matcher_list
    return hooks


def control_stub_options(
    tape: list[TapeEntry], base_options: ClaudeAgentOptions | None = None
) -> tuple[ClaudeAgentOptions, set[str]]:
    """Wire Direction-B replay stubs for the subtypes a tape actually contains.

    Returns ``(options, keep_subtypes)``: a copy of ``base_options`` with a replay
    stub installed for every *supported* Direction-B subtype present in the tape, and
    the set of those subtypes — so the caller keeps exactly the ``control_request``s
    it can answer (via :meth:`ReplayTransport.from_tape`) and drops the rest as inert.

    ``can_use_tool`` and ``hook_callback`` are supported today; ``mcp_message`` will
    join here as its stub builder lands. A stub is installed only when the tape has
    that subtype, so this never clobbers a consumer callback for a subtype the
    recording never exercised. :func:`~claude_agent_cassette.replay_tape` calls this;
    advanced callers can use it to wire the transport and options by hand.

    Best-effort: a subtype this can't faithfully reproduce (e.g. ``hook_callback`` on
    a tape with scrubbed callback ids) is left out of the keep-set — its requests stay
    inert (dropped), the same safe fallback as ``control=False`` — and a warning is
    emitted so the gap isn't silent. The conversation still replays faithfully.
    """
    options = base_options or ClaudeAgentOptions()
    keep: set[str] = set()
    exchanges = direction_b_exchanges(tape)
    if exchanges.get("can_use_tool"):
        options = dataclasses.replace(options, can_use_tool=build_permission_stub(tape))
        keep.add("can_use_tool")
    if exchanges.get("hook_callback"):
        try:
            hooks = build_hook_stubs(tape)
        except CassetteMismatchError as exc:
            warnings.warn(
                f"hook_callback replay unavailable; leaving hooks inert: {exc}",
                stacklevel=2,
            )
            hooks = None
        if hooks:
            # hooks keys are recorded event strings (e.g. "PreToolUse"); the SDK types
            # them as a HookEvent literal and validates at runtime.
            options = dataclasses.replace(options, hooks=hooks)  # type: ignore[arg-type]
            keep.add("hook_callback")
    return options, keep
