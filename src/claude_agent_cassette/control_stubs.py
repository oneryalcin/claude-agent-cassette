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

from typing import Any, Awaitable, Callable

from claude_agent_sdk import (
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)

from .tape import ControlExchange, TapeEntry, direction_b_exchanges
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
