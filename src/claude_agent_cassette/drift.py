"""Detect when a cassette's recorded frames have drifted from the installed SDK.

Re-parse each recorded conversation frame through the **installed** SDK's own
``message_parser``. Because it reuses the SDK's parser, the check cannot disagree
with what the SDK actually accepts — there is no hand-maintained schema to rot.

Two drift signals (kept distinct so a human can adjudicate):

- ``parse_error`` — ``parse_message`` raised. A *known* message type whose
  required shape changed (a renamed/removed field, a field that changed type).
- ``unrecognized_type`` — ``parse_message`` returned ``None``. The frame's top
  level ``type`` is one the installed SDK no longer models (renamed/removed, or a
  newer type an older SDK skips). Forward-compatible skip in the SDK → drift here.

**Contract.** This runs over a cassette's *message-producing* frames
(:func:`~claude_agent_cassette.conversation_frames` — control_request/_response
are excluded because the control plane is not message-parsed and would return
``None`` spuriously). Cassette inputs are curated to be message producers, so a
``None`` among them is real drift, not noise.

**Scope.** :func:`parse_drift` catches *parse-level* drift — frames the installed
SDK rejects/skips — plus *silently dropped content blocks* (a renamed/new block
type the parser omits while the message still parses; detected by the parser's
own surviving-block count). *Field-level* drift — a recorded field the parser
tolerates but neither reads nor retains — is covered by :func:`unmodeled_fields`
(the absolute set) and :func:`field_drift` (the gate: diff against a committed
baseline, since most unmodeled fields are steady-state wire noise, not drift).

Note: imports the SDK's private ``_internal.message_parser`` — version-sensitive
by nature (drift detection *is* about the installed parser); pin your SDK.
"""

from __future__ import annotations

from typing import Any, Collection, Literal, NamedTuple

from claude_agent_sdk._internal.message_parser import parse_message

from .tape import Frame, TapeEntry, conversation_frames

# The closed set of drift reasons this detector emits. A Literal (not an Enum):
# it's a code-controlled vocabulary consumers branch on, so it earns a type — but
# the values stay plain strings (zero runtime ceremony).
DriftReason = Literal["parse_error", "unrecognized_type", "content_dropped", "unmodeled_field"]


class DriftFinding(NamedTuple):
    """One frame that no longer survives the installed SDK intact."""

    frame_index: int  # position among the checked (message-producing) frames
    frame_type: str | None  # the recorded frame's top-level ``type``
    reason: DriftReason
    detail: str  # human-readable specifics for the reason


def _dropped_content_blocks(frame: Frame, message: object) -> str | None:
    """Detail string if the parse silently dropped content blocks, else None.

    ``parse_message`` matches each content block's ``type`` and appends only the
    *known* ones — an unrecognised block (renamed/new type) is silently omitted
    while the message still parses. That is invisible to the raise/None checks but
    means a replay loses recorded content. Detected with zero maintenance by the
    SDK's own count: fewer parsed blocks than recorded blocks ⇒ some were dropped.
    """
    raw_message = frame.get("message")
    raw_content = raw_message.get("content") if isinstance(raw_message, dict) else None
    if not isinstance(raw_content, list):
        return None  # string content / no content — no blocks to lose
    parsed_content = getattr(message, "content", None)
    if not isinstance(parsed_content, list) or len(parsed_content) >= len(raw_content):
        return None
    raw_types = [b.get("type") for b in raw_content if isinstance(b, dict)]
    return (
        f"{len(raw_content) - len(parsed_content)} of {len(raw_content)} content "
        f"block(s) dropped on parse; recorded types={raw_types}"
    )


def parse_drift(frames: list[Frame]) -> list[DriftFinding]:
    """Findings for every frame that doesn't survive the installed SDK intact.

    A frame drifts if ``parse_message`` raises (``parse_error``), returns ``None``
    (``unrecognized_type``), or parses but *silently drops content blocks*
    (``content_dropped`` — a renamed/new content block the parser omits). The catch
    is intentionally broad: a malformed field can surface as ``TypeError``/``KeyError``
    rather than ``MessageParseError`` (e.g. ``message`` recorded as a list), and a
    drift detector must *report* any malformation, never crash on it.
    """
    findings: list[DriftFinding] = []
    for index, frame in enumerate(frames):
        frame_type = frame.get("type") if isinstance(frame, dict) else None
        try:
            message = parse_message(frame)
        except Exception as exc:  # noqa: BLE001 — report arbitrary malformation, don't crash
            findings.append(
                DriftFinding(index, frame_type, "parse_error", f"{type(exc).__name__}: {exc}")
            )
            continue
        if message is None:
            findings.append(
                DriftFinding(index, frame_type, "unrecognized_type", str(frame_type))
            )
            continue
        dropped = _dropped_content_blocks(frame, message)
        if dropped is not None:
            findings.append(DriftFinding(index, frame_type, "content_dropped", dropped))
    return findings


def check_drift(tape: list[TapeEntry]) -> list[DriftFinding]:
    """Drift findings for a full duplex tape (checks its message-producing frames)."""
    return parse_drift(conversation_frames(tape))


# --- Field-level drift: fields the installed SDK silently ignores. ---
#
# parse_drift is blind to ADDITIVE drift by construction: the SDK's parser is
# forward-compatible, so a recorded field the installed SDK doesn't model parses
# clean and vanishes from the typed message. Detection, with the same
# zero-schema-maintenance property as the re-parse: run the SDK's real parser
# over an access-tracking view of the frame, then subtract what the parser READ
# (any key it touched) and what the typed message RETAINED (any frame subtree
# reachable from the parsed object — e.g. SystemMessage keeps the whole frame as
# .data, ToolUseBlock keeps the raw input dict). What's left is recorded data
# the installed SDK neither uses nor preserves — the parser itself is the
# schema, so this can't rot.


def _wrap(value: Any, path: tuple[Any, ...], accessed: set[tuple[Any, ...]]) -> Any:
    if isinstance(value, dict):
        return _SpyDict(value, path, accessed)
    if isinstance(value, list):
        return [_wrap(v, path + (i,), accessed) for i, v in enumerate(value)]
    return value


class _SpyDict(dict):
    """A dict that records which keys the parser touches (by path, recursively).

    Reads go through ``__getitem__``/``get``/``__contains__``; whole-dict views
    (``items``/``keys``/iteration) mark every key — a parser that iterates has
    seen everything, so this errs toward fewer findings (a lower bound, like
    parse_drift). The analysis walk must bypass these via ``dict.keys(node)``.
    """

    def __init__(self, data: dict, path: tuple[Any, ...], accessed: set[tuple[Any, ...]]):
        super().__init__({k: _wrap(v, path + (k,), accessed) for k, v in data.items()})
        self._path = path
        self._accessed = accessed

    def _mark(self, key: Any) -> None:
        self._accessed.add(self._path + (key,))

    def _mark_all(self) -> None:
        for key in dict.keys(self):
            self._mark(key)

    def __getitem__(self, key: Any) -> Any:
        self._mark(key)
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        self._mark(key)
        return super().get(key, default)

    def __contains__(self, key: Any) -> bool:
        self._mark(key)
        return super().__contains__(key)

    def items(self):  # type: ignore[override]
        self._mark_all()
        return super().items()

    def keys(self):  # type: ignore[override]
        self._mark_all()
        return super().keys()

    def values(self):  # type: ignore[override]
        self._mark_all()
        return super().values()

    def __iter__(self):
        self._mark_all()
        return super().__iter__()


def _retained_ids(obj: Any, seen: set[int] | None = None) -> set[int]:
    """ids of every container reachable from the parsed message — kept, not dropped."""
    import dataclasses

    seen = seen if seen is not None else set()
    if id(obj) in seen:
        return seen
    seen.add(id(obj))
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        for field in dataclasses.fields(obj):
            _retained_ids(getattr(obj, field.name), seen)
    elif isinstance(obj, dict):
        for value in dict.values(obj):
            _retained_ids(value, seen)
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            _retained_ids(value, seen)
    return seen


def _unread_paths(
    node: Any, retained: set[int], accessed: set[tuple[Any, ...]], path: tuple[Any, ...] = ()
) -> list[tuple[Any, ...]]:
    """Paths present in the frame that the parser neither read nor retained.

    A dict is only judged if the parser examined at least one of its keys
    individually (else it was taken wholesale — a passthrough, fully preserved).
    A subtree whose object the typed message retains (identity) is preserved by
    definition. Must use raw ``dict.*`` access — spy methods would mark keys and
    erase the very evidence being collected.
    """
    out: list[tuple[Any, ...]] = []
    if id(node) in retained:
        return out
    if isinstance(node, dict):
        if any(path + (k,) in accessed for k in dict.keys(node)):
            for key in dict.keys(node):
                child_path = path + (key,)
                if child_path not in accessed:
                    out.append(child_path)
                else:
                    child = dict.__getitem__(node, key)
                    out.extend(_unread_paths(child, retained, accessed, child_path))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            out.extend(_unread_paths(value, retained, accessed, path + (i,)))
    return out


def _canonical(frame_type: Any, path: tuple[Any, ...]) -> str:
    # List indices collapse to [] so one baseline entry covers every position.
    dotted = ".".join("[]" if isinstance(part, int) else str(part) for part in path)
    return f"{frame_type} {dotted}"


def _unmodeled(frames: list[Frame]) -> dict[str, int]:
    """Canonical ``"frame_type dotted.path"`` keys -> first frame index exhibiting them.

    Frames that fail to parse are skipped — those are parse_drift's findings
    (composition without double-reporting).
    """
    found: dict[str, int] = {}
    for index, frame in enumerate(frames):
        if not isinstance(frame, dict):
            continue
        accessed: set[tuple[Any, ...]] = set()
        spy = _SpyDict(frame, (), accessed)
        try:
            message = parse_message(spy)
        except Exception:  # noqa: BLE001 — parse_drift reports these
            continue
        if message is None:
            continue
        retained = _retained_ids(message)
        for path in _unread_paths(spy, retained, set(accessed)):
            found.setdefault(_canonical(frame.get("type"), path), index)
    return found


def unmodeled_fields(frames: list[Frame]) -> list[str]:
    """Recorded fields the installed SDK neither reads nor retains, as sorted
    canonical ``"frame_type dotted.path"`` keys.

    This is an *absolute* property of (recording, installed SDK) — most entries
    are steady-state wire fields the SDK has always ignored (``message.role``,
    ``timestamp``), not drift. Save the output as a baseline and gate with
    :func:`field_drift`, which reports only what *changed*.
    """
    return sorted(_unmodeled(frames))


def field_drift(frames: list[Frame], baseline: Collection[str]) -> list[DriftFinding]:
    """Findings for recorded fields the installed SDK ignores that are NOT in ``baseline``.

    ``baseline`` is a previous :func:`unmodeled_fields` output (typically committed
    alongside the cassette). A new entry means the installed SDK stopped reading or
    retaining a recorded field — the additive/field-level drift ``parse_drift`` is
    blind to. Baseline entries no longer observed are *not* findings (the SDK now
    models more than it used to); refresh the baseline to drop them.
    """
    known = set(baseline)
    return [
        DriftFinding(
            index,
            key.split(" ", 1)[0],
            "unmodeled_field",
            f"{key.split(' ', 1)[1]}: recorded but neither read nor retained by the "
            "installed SDK (new since the baseline)",
        )
        for key, index in sorted(_unmodeled(frames).items(), key=lambda kv: kv[1])
        if key not in known
    ]
