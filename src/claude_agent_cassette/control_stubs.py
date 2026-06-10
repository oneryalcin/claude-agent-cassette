"""Direction-B replay: stub callbacks (``mode="stub"``) and decision verification
(``mode="verify"``).

**Stub mode** needs the SDK to invoke *some* callback when it receives a recorded
``control_request`` — but running the consumer's live callback would defeat the
point (real permission checks, hooks, in-process MCP execution, all with side
effects). The stub builders turn the recorded decisions
(:func:`~claude_agent_cassette.direction_b_exchanges`) into callbacks that hand
back the *recorded* outcome — certifying the recorded wire, not the consumer's
policy.

**Verify mode** is the complement: the consumer's *real* callbacks answer the
recorded requests, and :func:`verify_direction_b_decisions` diffs their answers
against the recording at the wire — certifying that the consumer's policy still
produces the recorded decisions.

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
from typing import Any, Awaitable, Callable, Iterator, NamedTuple

from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookMatcher,
    McpSdkServerConfig,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
    create_sdk_mcp_server,
    tool,
)

from .tape import (
    ControlExchange,
    RawMessage,
    TapeEntry,
    control_request_subtype,
    direction_b_exchanges,
    read_frames,
    recorded_hook_config,
)
from .transport import CassetteMismatchError

CanUseTool = Callable[
    [str, dict[str, Any], ToolPermissionContext],
    Awaitable["PermissionResultAllow | PermissionResultDeny"],
]

# Direction-B subtypes this module can replay today — all three the SDK handles.
# The check stays as future-proofing: a tape carrying a subtype a future SDK adds
# fails closed instead of replaying a subset.
SUPPORTED_SUBTYPES = frozenset({"can_use_tool", "hook_callback", "mcp_message"})


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
                "Direction-B replay diverged from the recording (the SDK swallows callback "
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


def _mcp_rpc(exchange: ControlExchange) -> RawMessage:
    """The recorded JSON-RPC response payload of an mcp_message exchange."""
    return exchange.decision.get("mcp_response") or {}


def _make_mcp_tool_stub(
    server_name: str, tool_name: str, recorded: deque[ControlExchange], ledger: ControlReplayLedger
) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
    """A tool handler that replays the recorded ``tools/call`` results (FIFO).

    Firing more times than recorded, arguments that differ from the recording, a
    recorded control-level *error* envelope, or a recorded JSON-RPC *error* (the
    original tool blew up below the handler) is divergence — recorded into ``ledger``
    and raised.
    """

    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        label = f"mcp_message tools/call {server_name}.{tool_name}"
        if not recorded:
            msg = f"{label} called more times than recorded; the live MCP sequence diverged"
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        exchange = recorded.popleft()
        recorded_args = ((exchange.request.get("message") or {}).get("params") or {}).get(
            "arguments"
        )
        if args != recorded_args:
            msg = f"{label}: live arguments {args!r} diverged from recorded {recorded_args!r}"
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        if not exchange.succeeded:
            msg = f"{label}: recorded an error envelope, not a result — cannot replay it"
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        rpc = _mcp_rpc(exchange)
        if "result" not in rpc:
            msg = (
                f"{label}: recorded a JSON-RPC error ({rpc.get('error')!r}), not a result — "
                "cannot replay it through a real tool handler"
            )
            ledger.record(msg)
            raise CassetteMismatchError(msg)
        result = rpc["result"] or {}
        out: dict[str, Any] = {"content": result.get("content") or []}
        if result.get("isError"):
            out["is_error"] = True
        return out

    return handler


def build_mcp_stub_servers(
    tape: list[TapeEntry], ledger: ControlReplayLedger
) -> dict[str, McpSdkServerConfig]:
    """In-process SDK MCP servers that replay the tape's recorded ``mcp_message`` traffic.

    One *real* ``create_sdk_mcp_server`` per recorded ``server_name``, reconstructed
    entirely from the tape: the server identity from the recorded ``initialize``
    response's ``serverInfo``, the tool list (names, descriptions, raw JSON-Schema
    ``inputSchema``) from the recorded ``tools/list`` response, and each tool's results
    from the recorded ``tools/call`` exchanges (FIFO per tool, tracked for leftovers).

    Going through the real server machinery — rather than faking the SDK's internal
    routing — means ``initialize`` / ``notifications/initialized`` / ``tools/list`` are
    answered by the SDK's own (stateless) paths, and the replay survives the SDK's
    planned MCP-transport refactor. A tool called at replay that the recording never
    listed gets a minimal definition (empty schema) so a truncated recording — calls
    recorded, ``tools/list`` lost — still replays its calls.
    """
    by_server: dict[str, list[ControlExchange]] = defaultdict(list)
    for exchange in direction_b_exchanges(tape).get("mcp_message", ()):
        server_name = exchange.request.get("server_name")
        if server_name is not None:
            by_server[server_name].append(exchange)

    servers: dict[str, McpSdkServerConfig] = {}
    for server_name, exchanges in by_server.items():
        info: dict[str, Any] = {}
        tool_defs: list[RawMessage] = []
        calls: dict[str, deque[ControlExchange]] = defaultdict(deque)
        for exchange in exchanges:
            message = exchange.request.get("message") or {}
            method = message.get("method")
            if method == "initialize" and not info:
                info = (_mcp_rpc(exchange).get("result") or {}).get("serverInfo") or {}
            elif method == "tools/list" and not tool_defs:
                tool_defs = (_mcp_rpc(exchange).get("result") or {}).get("tools") or []
            elif method == "tools/call":
                name = (message.get("params") or {}).get("name")
                if name is not None:
                    calls[name].append(exchange)

        listed = {td.get("name") for td in tool_defs}
        # New list — tool_defs aliases the recorded payload inside the tape.
        tool_defs = list(tool_defs) + [{"name": name} for name in calls if name not in listed]

        stubs = []
        for td in tool_defs:
            name = td["name"]
            ledger.track(f"mcp_message tools/call [{server_name}.{name}]", calls[name])
            stubs.append(
                tool(
                    name,
                    td.get("description") or "",
                    td.get("inputSchema") or {"type": "object", "properties": {}},
                )(_make_mcp_tool_stub(server_name, name, calls[name], ledger))
            )
        servers[server_name] = create_sdk_mcp_server(
            name=info.get("name") or server_name,
            version=info.get("version") or "1.0.0",
            tools=stubs,
        )
    return servers


def _iter_write_frames(writes: list[str]) -> Iterator[RawMessage]:
    """The parsed JSON objects of a transport's captured writes, skipping non-JSON."""
    for data in writes:
        try:
            frame = json.loads(data)
        except ValueError:
            continue
        if isinstance(frame, dict):
            yield frame


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

    The recorded ``hook_callback`` requests only route to the replay's hooks (stubs in
    ``mode="stub"``, the consumer's real hooks in ``mode="verify"``) if the live SDK
    numbers them exactly as the recording did. Rather than predict the SDK's numbering
    (a coupling to an internal that the realistic break — an SDK scheme change — would
    defeat), compare the recorded ids against the ones in the SDK's live ``initialize``
    write. A mismatch (scrubbed recorded ids, a changed SDK scheme, or a consumer whose
    hook structure no longer matches the recording) is recorded as divergence so the
    hooks-never-fired case surfaces with a clear message instead of a bare leftover.
    """
    recorded_ids = _flatten_hook_ids(recorded_hook_config(tape))
    live_config = None
    for frame in _iter_write_frames(writes):
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
            "the replay's hooks (scrubbed recording, a changed SDK id scheme, or a hook "
            "structure that no longer matches the recording)"
        )


def direction_b_replay_findings(tape: list[TapeEntry]) -> list[str]:
    """Reasons a tape isn't fully Direction-B replayable (``mode="stub"``), or ``[]`` if it is.

    A lint, not a gate: run it after :func:`~claude_agent_cassette.scrub_tape` to confirm a
    scrub didn't break replay (the classic mistake — scrubbing ``hookCallbackIds`` or the
    decision payload), or over committed fixtures in CI. Checks that every Direction-B
    request has a recorded response, no decision is scrubbed or an error envelope, hook ids
    reproduce (``hook_0``, ``hook_1``, … contiguous), and no subtype lacks a stub builder.
    """
    findings: list[str] = []
    exchanges = direction_b_exchanges(tape)

    for subtype in sorted(set(exchanges) - SUPPORTED_SUBTYPES):
        findings.append(f"{subtype}: not yet replayable (no stub builder)")

    paired_ids = {ex.request_id for group in exchanges.values() for ex in group}
    for frame in read_frames(tape):
        if frame.get("type") == "control_request" and frame.get("request_id") not in paired_ids:
            findings.append(
                f"{control_request_subtype(frame)} request {frame.get('request_id')!r}: "
                "no recorded response (unpaired) — can't replay a decision for it"
            )

    for subtype, group in exchanges.items():
        for exchange in group:
            if not exchange.succeeded:
                findings.append(
                    f"{subtype} {exchange.request_id!r}: recorded an error envelope, not a decision"
                )
            elif "<scrubbed>" in json.dumps(exchange.decision):
                findings.append(
                    f"{subtype} {exchange.request_id!r}: decision is scrubbed — not replayable"
                )
            elif (
                subtype == "mcp_message"
                and ((exchange.request.get("message") or {}).get("method")) == "tools/call"
                and "result" not in _mcp_rpc(exchange)
            ):
                findings.append(
                    f"mcp_message {exchange.request_id!r}: tools/call recorded a JSON-RPC "
                    "error, not a result — not replayable through a real tool handler"
                )

    config = recorded_hook_config(tape)
    if config:
        ids = _flatten_hook_ids(config)
        expected = [f"hook_{i}" for i in range(len(ids))]
        if ids != expected:
            findings.append(
                f"hook callback ids {ids} can't be reproduced (expected {expected}; "
                "scrubbed or non-contiguous from hook_0)"
            )
    return findings


def control_stub_options(
    tape: list[TapeEntry], base_options: ClaudeAgentOptions | None = None
) -> ControlStubBundle:
    """Wire Direction-B replay stubs for every subtype a tape contains.

    Returns a :class:`ControlStubBundle` — a copy of ``base_options`` with replay stubs
    installed, the ``control_request`` subtypes to keep in the inbound stream, and a
    :class:`ControlReplayLedger` to surface divergence after replay.
    :func:`~claude_agent_cassette.replay_tape` calls this; advanced callers can use it to
    wire the transport and options by hand.

    **Fail-closed, not best-effort.** If the tape contains a Direction-B subtype with
    no stub builder (one a future SDK adds), this raises
    :class:`~claude_agent_cassette.CassetteMismatchError` rather than silently
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
    if exchanges.get("mcp_message"):
        # Wholesale replacement, like the other stubs: in stub mode no live MCP
        # server may run, and only the recorded server_names are ever routed to.
        stub_servers: dict[str, Any] = dict(build_mcp_stub_servers(tape, ledger))
        options = dataclasses.replace(options, mcp_servers=stub_servers)
        keep.add("mcp_message")
    return ControlStubBundle(options=options, keep_subtypes=keep, ledger=ledger)


# --- Verify mode: run the consumer's REAL callbacks and diff their decisions
# against the recording, at the wire. ---


def control_verify_options(
    tape: list[TapeEntry], base_options: ClaudeAgentOptions | None = None
) -> ControlStubBundle:
    """Precondition check for a Direction-B **verify** replay (``mode="verify"``).

    Unlike :func:`control_stub_options`, nothing is replaced: the consumer's *real*
    ``can_use_tool`` / ``hooks`` / SDK MCP servers stay installed, the recorded
    Direction-B requests are delivered to them, and :func:`verify_direction_b_decisions`
    diffs their answers against the recording after replay. This builder only validates
    that every recorded subtype has a live handler to run — a recording with permission
    exchanges but no live ``can_use_tool`` (hook exchanges but no live ``hooks``,
    ``mcp_message`` exchanges but no matching in-process SDK MCP server) is itself
    divergence from the recorded session, surfaced up front with a clear message
    rather than as N swallowed "callback is not provided" errors.

    Fail-closed on unsupported subtypes exactly like stub mode.
    """
    exchanges = direction_b_exchanges(tape)
    unsupported = set(exchanges) - SUPPORTED_SUBTYPES
    if unsupported:
        raise CassetteMismatchError(
            f"tape contains Direction-B subtype(s) not yet verifiable: {sorted(unsupported)}. "
            "Replay with mode='inert' to play the conversation without the control plane, "
            "or re-record without them."
        )

    options = base_options or ClaudeAgentOptions()
    keep: set[str] = set()
    if exchanges.get("can_use_tool"):
        if options.can_use_tool is None:
            raise CassetteMismatchError(
                "tape records can_use_tool exchanges but options.can_use_tool is not set — "
                "mode='verify' runs YOUR callback and diffs its decisions against the "
                "recording (use mode='stub' to replay the recorded decisions instead)"
            )
        keep.add("can_use_tool")
    if exchanges.get("hook_callback"):
        if not options.hooks:
            raise CassetteMismatchError(
                "tape records hook_callback exchanges but options.hooks is empty — "
                "mode='verify' runs YOUR hooks and diffs their outputs against the "
                "recording (use mode='stub' to replay the recorded outputs instead)"
            )
        keep.add("hook_callback")
    if exchanges.get("mcp_message"):
        recorded_servers = {
            name
            for ex in exchanges["mcp_message"]
            if isinstance(name := ex.request.get("server_name"), str)
        }
        configured = options.mcp_servers if isinstance(options.mcp_servers, dict) else {}
        live_sdk_servers = {
            name
            for name, config in configured.items()
            if isinstance(config, dict) and config.get("type") == "sdk"
        }
        missing = recorded_servers - live_sdk_servers
        if missing:
            raise CassetteMismatchError(
                f"tape records mcp_message exchanges for SDK MCP server(s) {sorted(missing)} "
                "but options.mcp_servers has no such in-process server — mode='verify' runs "
                "YOUR server's tools and diffs their results against the recording (use "
                "mode='stub' to replay the recorded results instead)"
            )
        keep.add("mcp_message")
    return ControlStubBundle(options=options, keep_subtypes=keep, ledger=ControlReplayLedger())


def verify_direction_b_decisions(
    writes: list[str], tape: list[TapeEntry], ledger: ControlReplayLedger
) -> None:
    """Diff the live SDK's Direction-B answers against the recorded ones — at the wire.

    Verify mode replays the recorded ``control_request``s verbatim (same
    ``request_id``s), the consumer's real callbacks answer them, and the SDK converts
    those answers into ``control_response`` writes through its *real* conversion path
    (``Query._handle_control_request``). So live and recorded decisions are directly
    comparable as wire dicts, matched exactly by ``request_id`` — no shape conversion,
    no order heuristics. Every divergence is recorded into ``ledger``:

    - the live decision payload differs from the recorded one (the policy changed);
    - the envelopes disagree — the recording has a decision but the live callback
      raised (the SDK writes an ``error`` envelope), or vice versa. Two error
      envelopes match without comparing messages: the contract is "the callback
      still raises here", not the exception text;
    - a recorded exchange the live side never answered (not delivered, or still in
      flight at exit).
    """
    recorded = {
        ex.request_id: ex
        for group in direction_b_exchanges(tape).values()
        for ex in group
    }
    live: dict[str, RawMessage] = {}
    for frame in _iter_write_frames(writes):
        if frame.get("type") != "control_response":
            continue
        envelope = frame.get("response") or {}
        request_id = envelope.get("request_id")
        if request_id in recorded:
            live[request_id] = envelope

    for request_id, exchange in recorded.items():
        envelope = live.get(request_id)
        if envelope is None:
            ledger.record(
                f"{exchange.subtype} {request_id!r}: never answered on replay "
                "(request not delivered, or the callback was still in flight at exit)"
            )
            continue
        live_succeeded = envelope.get("subtype") == "success"
        if live_succeeded != exchange.succeeded:
            recorded_kind = "a decision" if exchange.succeeded else "an error envelope"
            live_kind = (
                "a decision"
                if live_succeeded
                else f"an error ({envelope.get('error')!r})"
            )
            ledger.record(
                f"{exchange.subtype} {request_id!r}: the recording has {recorded_kind} "
                f"but your callback produced {live_kind}"
            )
        elif live_succeeded and (envelope.get("response") or {}) != exchange.decision:
            ledger.record(
                f"{exchange.subtype} {request_id!r}: your callback's decision diverged "
                "from the recording\n"
                f"      recorded: {json.dumps(exchange.decision, sort_keys=True)}\n"
                f"      live:     {json.dumps(envelope.get('response') or {}, sort_keys=True)}"
            )
