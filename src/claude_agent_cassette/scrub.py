"""Decision-preserving scrub: blank PII *values* in a tape, keep structure + decisions.

A recorded tape carries real prompts, file paths, and the recording machine's
fingerprint, but a Direction-B replay *consumes* the control decisions (permission
behavior, ``updatedInput``, hook output, ``callback_id``). So a scrub for sharing must
blank PII values while leaving every key, frame, and decision intact — the opposite of
the older websearch fixture, which scrubbed its decisions and made itself unreplayable.

:func:`scrub_tape` does value-substitution only: it replaces known-sensitive substrings
(absolute paths, API keys) in string values and never drops or reshapes a frame, so
control-plane decisions ride through unless a needle literally occurs inside one. Pair it
with :func:`~claude_agent_cassette.lint_tape` to confirm the scrubbed
tape is still replayable.
"""

from __future__ import annotations

import copy
import getpass
import json
import os
import re
from typing import Any, Optional, Sequence

from .tape import TapeEntry

# One (needle, mask) substitution pair per entry — the vocabulary of scrub_tape,
# record(scrub=...), and the pytest plugin's cassette_scrub fixture.
Replacements = Sequence[tuple[str, str]]


def path_replacements(path: str, mask: str) -> list[tuple[str, str]]:
    """Every wire form of ``path`` as (needle, mask) pairs: raw, realpath, and the
    CLI's **slug encoding**.

    The CLI embeds paths slug-encoded in ``~/.claude/projects/<slug>/…`` strings —
    ``system/init.memory_paths``, hook-input ``transcript_path``. A literal
    substring scrub can never match the slug (``/Users/alice/proj`` rides the wire
    as ``-Users-alice-proj``), so a path's slug forms must be needles of their own.
    The encoding (every non-alphanumeric character becomes ``-``) is derived from
    observed CLI output, not a spec — the committed-fixture hygiene test backstops
    it for this repo's tapes.
    """
    slug = re.sub(r"[^A-Za-z0-9]", "-", path)
    real = os.path.realpath(path)
    pairs = [(path, mask), (real, mask), (slug, mask)]
    if real != path:
        pairs.append((re.sub(r"[^A-Za-z0-9]", "-", real), mask))
    return pairs


def default_replacements(
    *,
    cwd: Optional[str] = None,
    config_dir: Optional[str] = None,
    username: bool = False,
) -> list[tuple[str, str]]:
    """The standard recording scrub: cwd, home (raw + realpath + slug forms via
    :func:`path_replacements`), and ``ANTHROPIC_API_KEY``.

    The keyword extensions cover a *recording session's* full fingerprint (what
    the example recorders use):

    - ``cwd`` — the session's working dir when it differs from the process cwd
      (the recorders run the CLI in a temp dir);
    - ``config_dir`` — the isolated ``CLAUDE_CONFIG_DIR`` (hook transcript and
      memory paths live under it);
    - ``username`` — also mask the bare username: tool output like ``ls -la``
      prints it outside any path. Opt-in because a short or common username
      ("test") would over-mask legitimate content.
    """
    pairs = path_replacements(os.getcwd(), "<CWD>") + path_replacements(
        os.path.expanduser("~"), "<HOME>"
    )
    if cwd is not None:
        pairs += path_replacements(cwd, "<CWD>")
    if config_dir is not None:
        pairs += path_replacements(config_dir, "<CONFIG>")
    if username:
        pairs.append((getpass.getuser(), "<USER>"))
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        pairs.append((key, "<REDACTED_API_KEY>"))
    return pairs


def _scrub_values(obj: Any, replacements: Replacements) -> Any:
    """Recursively substitute needle→mask in every string value; structure untouched."""
    if isinstance(obj, str):
        for needle, mask in replacements:
            obj = obj.replace(needle, mask)
        return obj
    if isinstance(obj, list):
        return [_scrub_values(v, replacements) for v in obj]
    if isinstance(obj, dict):
        return {k: _scrub_values(v, replacements) for k, v in obj.items()}
    return obj


def scrub_tape(tape: list[TapeEntry], replacements: Replacements) -> list[TapeEntry]:
    """A copy of ``tape`` with each ``(needle, mask)`` substring blanked in string values.

    ``replacements`` is applied **longest-needle-first**, so a specific path
    (``/private/var/…/tmpX``) is masked before a shorter prefix of it (``/var/…/tmpX``)
    can match inside it. Outbound ``write`` payloads are parsed, scrubbed, and
    re-serialized so a masked value can't survive as raw JSON text. Empty needles are
    ignored. Nothing is dropped or reshaped — only string values are substituted, so
    control-plane decisions (``behavior`` / ``updatedInput`` / hook output /
    ``callback_id``) are preserved unless a needle literally occurs in them.
    """
    ordered = sorted((r for r in replacements if r[0]), key=lambda r: len(r[0]), reverse=True)
    scrubbed: list[TapeEntry] = []
    for entry in tape:
        data = entry.get("data")
        if entry.get("dir") == "write" and isinstance(data, str):
            try:
                payload = json.loads(data)
            except ValueError:
                scrubbed.append(_scrub_values(entry, ordered))
                continue
            scrubbed.append({"dir": "write", "data": json.dumps(_scrub_values(payload, ordered))})
        else:
            scrubbed.append(_scrub_values(entry, ordered))
    return scrubbed


# The keys that enumerate the recording environment's inventory, at the two
# places the CLI emits it: the ``system/init`` conversation frame, and the
# ``initialize`` handshake's control_response (``response.response``). Names of
# slash commands, plugins, skills, agents, MCP servers, memory paths, and tools
# fingerprint the operator's machine (or a company's internal tooling) — and for
# replay they are inert: the SDK keeps the init frame wholesale as
# SystemMessage.data and only surfaces the handshake result via
# ``get_server_info()``, without reading these keys, so blanking is
# decision-preserving by construction.
_INIT_INVENTORY_KEYS = (
    "slash_commands",
    "plugins",
    "skills",
    "agents",
    "mcp_servers",
    "memory_paths",
    "tools",
)
_HANDSHAKE_INVENTORY_KEYS = (
    "commands",
    "agents",
    "models",
    "available_output_styles",
    "account",
)


def _blank(value: Any) -> Any:
    """Type-preserving blank: a list empties to [], a dict to {} (memory_paths is
    a dict on current CLIs), anything else passes through."""
    if isinstance(value, list):
        return []
    if isinstance(value, dict):
        return {}
    return value


def scrub_init_inventory(tape: list[TapeEntry]) -> list[TapeEntry]:
    """A copy of ``tape`` with the recording environment's inventory blanked.

    The CLI enumerates the environment in two frames: ``system/init`` (slash
    commands, plugins with cache paths, skills, agents, MCP servers, memory
    paths, tool names) and the ``initialize`` handshake's ``control_response``
    (command/agent/model inventories, account metadata). :func:`scrub_tape`
    masks *values* you name; this blanks those inventory containers wholesale,
    for tapes recorded in real environments where the inventory itself is the
    leak.

    Keys are blanked only where present, type-preserved (list → ``[]``, dict →
    ``{}``); nothing is dropped or reshaped, and no other frame is touched —
    Direction-B decision writes in particular ride through untouched. The best
    fix is recording in an isolated environment in the first place
    (``env={"CLAUDE_CONFIG_DIR": ...}`` — see the example recorders, which apply
    both); this is the after-the-fact repair.

    Note: paths *outside* these inventory keys (a hook input's
    ``transcript_path``, the slug-encoded project dir) are value scrubs —
    :func:`scrub_tape` with :func:`default_replacements` /
    :func:`path_replacements` covers them. ``claude_code_version`` /
    ``apiKeySource`` / ``session_id`` / ``uuid`` are deliberately kept: not
    operator-identifying (random or generic), and version is load-bearing
    context for drift triage.
    """
    # deepcopy up front: like scrub_tape, the result must share NO substructure
    # with the input — a caller mutating the scrubbed copy must not corrupt the
    # original recording.
    scrubbed: list[TapeEntry] = []
    for entry in copy.deepcopy(tape):
        frame = entry.get("frame")
        if entry.get("dir") != "read" or not isinstance(frame, dict):
            scrubbed.append(entry)
            continue
        if frame.get("type") == "system" and frame.get("subtype") == "init":
            for key in _INIT_INVENTORY_KEYS:
                if key in frame:
                    frame[key] = _blank(frame[key])
            scrubbed.append(entry)
            continue
        if frame.get("type") == "control_response":
            inner = (frame.get("response") or {}).get("response")
            if isinstance(inner, dict):
                for key in _HANDSHAKE_INVENTORY_KEYS:
                    if key in inner:
                        inner[key] = _blank(inner[key])
        scrubbed.append(entry)
    return scrubbed
