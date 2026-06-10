"""``claude-agent-cassette`` command line — drift detection for SDK-bump PRs.

    claude-agent-cassette drift <tape.jsonl | dir> ...

Re-parses each tape's message frames through the installed SDK and reports frames
that no longer parse (renamed/removed type, changed shape). Exits non-zero when
any drift is found, so it can gate a bump PR.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import NamedTuple, TextIO

import claude_agent_sdk

from .drift import field_drift, parse_drift, unmodeled_fields
from .tape import Frame, message_frames, conversation_frames


_DEFAULT_INPUT_NAME = "input.jsonl"

# Exit codes (named so the gate's contract is explicit, not magic ints).
EXIT_OK = 0       # checked, no drift
EXIT_DRIFT = 1    # at least one cassette drifted
EXIT_MISUSE = 2   # nothing checked / ambiguous layout — fail closed


class Cassette(NamedTuple):
    """A cassette selected for drift-checking."""

    path: Path
    label: str  # how it reads in a drift row: file path (flat) or dir name (nested)
    fields_path: Path  # the field-baseline sidecar (committed next to the cassette)


class MixedLayoutError(Exception):
    """A directory holds both flat ``*.jsonl`` and nested ``*/input.jsonl`` cassettes.

    The layout is ambiguous, and silently checking only one set would be silent
    coverage loss — a false green on an SDK bump, the exact failure this gate
    exists to prevent. Fail closed and make the caller pick a layout. The
    exception carries its own human-readable message (single source of truth).
    """

    def __init__(self, directory: Path, nested_name: str) -> None:
        super().__init__(
            f"{directory} holds both flat *.jsonl and nested */{nested_name} "
            "cassettes — ambiguous layout. Point at one layout, or pass "
            "--input-name to select the nested recording file explicitly."
        )


def _collect_tapes(paths: list[str], input_name: str | None = None) -> list[Cassette]:
    """:class:`Cassette` selections to drift-check, from files and directories.

    A directory is expanded by layout:

    - **flat** — top-level ``*.jsonl`` (label: the file path); or
    - **nested** — ``*/<input_name>`` exactly one level down, where each cassette
      is a directory holding the recording plus sidecars (label: the cassette dir
      name, so a drift row reads ``des6250`` rather than ``input.jsonl``).

    Only ``<input_name>`` is ever treated as a cassette — sidecars such as
    ``expected.jsonl`` / ``meta.json`` are ignored **by construction** (allowlist,
    not a denylist of names to skip). Auto-detect (``input_name is None``) blesses
    ``input.jsonl`` as the nested name; a directory that holds *both* layouts is a
    :class:`MixedLayoutError` (never a silent partial run). Passing ``input_name``
    explicitly selects nested-only mode (no flat globbing).
    """
    explicit = input_name is not None
    nested_name = input_name or _DEFAULT_INPUT_NAME

    def flat_cassette(p: Path, label: str) -> Cassette:
        # Sidecar is .json, not .jsonl, so the flat glob can never collect it.
        return Cassette(p, label, p.with_name(f"{p.stem}.fields.json"))

    tapes: list[Cassette] = []
    for raw in paths:
        path = Path(raw)
        if not path.is_dir():
            tapes.append(flat_cassette(path, str(path)))
            continue
        nested = sorted(path.glob(f"*/{nested_name}"))
        flat = [] if explicit else sorted(path.glob("*.jsonl"))
        if flat and nested:
            raise MixedLayoutError(path, nested_name)
        if nested:
            # The cassette dir already holds sidecars — fields.json joins them.
            tapes.extend(Cassette(p, p.parent.name, p.parent / "fields.json") for p in nested)
        else:
            tapes.extend(flat_cassette(p, str(p)) for p in flat)
    return tapes


def _load_frames(path: Path) -> list[Frame]:
    """Message frames to drift-check from a cassette file, auto-detecting format:
    a full duplex tape (entries carry ``dir``) or a raw inbound-frame cassette
    (``examples/cassettes/*.jsonl``). Treating every file as a tape silently
    yielded zero frames for raw cassettes — and so 'no drift' for anything."""
    entries = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if entries and isinstance(entries[0], dict) and "dir" in entries[0]:
        return conversation_frames(entries)  # full duplex tape
    return message_frames(entries)  # raw inbound-frame cassette


def _read_baseline(path: Path) -> list[str] | None:
    """The committed field baseline, or None if absent. Corrupt ⇒ ValueError (fail closed)."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        unmodeled = data["unmodeled"]
        if not isinstance(unmodeled, list) or not all(isinstance(s, str) for s in unmodeled):
            raise TypeError("'unmodeled' must be a list of strings")
        return unmodeled
    except Exception as exc:
        raise ValueError(f"unreadable field baseline {path}: {exc}") from exc


def _write_baseline(path: Path, unmodeled: list[str], sdk_version: str) -> None:
    path.write_text(
        json.dumps({"sdk_version": sdk_version, "unmodeled": unmodeled}, indent=2) + "\n"
    )


def _drift(
    paths: list[str], out: TextIO,
    allow_empty: bool = False, input_name: str | None = None,
    fields: bool = False, update_baselines: bool = False,
) -> int:
    sdk_version = getattr(claude_agent_sdk, "__version__", "?")
    try:
        tapes = _collect_tapes(paths, input_name)
    except MixedLayoutError as e:
        # Fail closed on an ambiguous layout: checking only one set would silently
        # drop the other's coverage (a false green). Make the caller disambiguate.
        print(f"drift: {e}", file=sys.stderr)
        return EXIT_MISUSE
    if not tapes and not allow_empty:
        # Fail closed: a gate that checked nothing must not report success — an
        # empty/mispointed path would otherwise be a false green on an SDK bump.
        print(
            f"drift: no cassette files found in {paths} — nothing checked.\n"
            "(expected tape *.jsonl files or directories containing them; "
            "pass --allow-empty to treat this as success)",
            file=sys.stderr,
        )
        return EXIT_MISUSE
    print(f"drift: {len(tapes)} cassette(s) vs claude-agent-sdk {sdk_version}\n", file=out)

    drifted_frames = 0
    drifted_tapes = 0
    missing_baselines = 0
    for cassette in tapes:
        frames = _load_frames(cassette.path)
        findings = parse_drift(frames)
        notes: list[str] = []

        if update_baselines:
            current = unmodeled_fields(frames)
            try:
                previous = _read_baseline(cassette.fields_path)
            except ValueError:
                previous = None  # corrupt — overwrite is the requested repair
            if previous != current:
                _write_baseline(cassette.fields_path, current, sdk_version)
                verb = "written" if previous is None else "updated"
                notes.append(f"field baseline {verb} ({len(current)} field(s))")
        elif fields:
            try:
                baseline = _read_baseline(cassette.fields_path)
            except ValueError as exc:
                print(f"drift: {exc}", file=sys.stderr)
                return EXIT_MISUSE
            if baseline is None:
                # Fail closed: an unbaselined cassette is a coverage gap, not a pass.
                missing_baselines += 1
                notes.append(
                    f"no field baseline ({cassette.fields_path}) — create it with "
                    "--update-field-baselines"
                )
            else:
                findings = findings + field_drift(frames, baseline)
                stale = sorted(set(baseline) - set(unmodeled_fields(frames)))
                if stale:
                    notes.append(
                        f"{len(stale)} stale baseline entr{'y' if len(stale) == 1 else 'ies'} "
                        "(the installed SDK now models them) — refresh with "
                        "--update-field-baselines"
                    )

        if findings:
            drifted_tapes += 1
            drifted_frames += len(findings)
            print(f"  DRIFT {cassette.label} — {len(findings)} frame(s):", file=out)
            for f in findings:
                print(f"          frame[{f.frame_index}] {f.frame_type}: {f.reason} — {f.detail}", file=out)
        else:
            print(f"  ok    {cassette.label}", file=out)
        for note in notes:
            print(f"          note: {note}", file=out)

    if drifted_frames:
        print(
            f"\n{len(tapes)} checked, {drifted_tapes} drifted "
            f"({drifted_frames} frame(s)) — re-record the drifted cassettes.",
            file=out,
        )
        return EXIT_DRIFT
    if missing_baselines:
        print(
            f"\n{len(tapes)} checked, no drift — but {missing_baselines} cassette(s) "
            "have no field baseline (nothing field-checked for them).",
            file=out,
        )
        return EXIT_MISUSE
    print(f"\n{len(tapes)} checked, no drift.", file=out)
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="claude-agent-cassette")
    sub = parser.add_subparsers(dest="command", required=True)
    drift = sub.add_parser("drift", help="re-parse cassettes through the installed SDK")
    drift.add_argument("paths", nargs="+", help="tape .jsonl files, or dirs of them")
    drift.add_argument(
        "--allow-empty", action="store_true",
        help="exit 0 instead of failing when no cassette files are found",
    )
    drift.add_argument(
        "--input-name", default=None, metavar="FILE",
        help="nested-layout cassette suites: name the recording file inside each "
             "cassette dir (default auto-detect '*/input.jsonl' when a dir has no "
             "top-level *.jsonl); passing it selects nested-only mode",
    )
    drift.add_argument(
        "--fields", action="store_true",
        help="also gate field-level drift against each cassette's committed "
             "fields baseline (*.fields.json / fields.json sidecar); a cassette "
             "without a baseline fails closed",
    )
    drift.add_argument(
        "--update-field-baselines", action="store_true",
        help="(re)write each cassette's fields baseline from the installed SDK "
             "instead of gating against it",
    )
    args = parser.parse_args(argv)
    if args.command == "drift":
        return _drift(
            args.paths, sys.stdout,
            allow_empty=args.allow_empty, input_name=args.input_name,
            fields=args.fields, update_baselines=args.update_field_baselines,
        )
    parser.error(f"unknown command {args.command!r}")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
