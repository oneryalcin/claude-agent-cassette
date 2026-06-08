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

import claude_agent_sdk

from .drift import parse_drift
from .tape import RawMessage, message_frames, replayable_messages


def _collect_tapes(paths: list[str]) -> list[Path]:
    """Tape files from the given paths: a directory contributes its ``*.jsonl``."""
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        else:
            files.append(path)
    return files


def _load_frames(path: Path) -> list[RawMessage]:
    """Message frames to drift-check from a cassette file, auto-detecting format:
    a full duplex tape (entries carry ``dir``) or a raw inbound-frame cassette
    (``examples/cassettes/*.jsonl``). Treating every file as a tape silently
    yielded zero frames for raw cassettes — and so 'no drift' for anything."""
    entries = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if entries and isinstance(entries[0], dict) and "dir" in entries[0]:
        return replayable_messages(entries)  # full duplex tape
    return message_frames(entries)  # raw inbound-frame cassette


def _drift(paths: list[str], out, allow_empty: bool = False) -> int:
    sdk_version = getattr(claude_agent_sdk, "__version__", "?")
    tapes = _collect_tapes(paths)
    if not tapes and not allow_empty:
        # Fail closed: a gate that checked nothing must not report success — an
        # empty/mispointed path would otherwise be a false green on an SDK bump.
        print(
            f"drift: no cassette files found in {paths} — nothing checked.\n"
            "(expected tape *.jsonl files or directories containing them; "
            "pass --allow-empty to treat this as success)",
            file=sys.stderr,
        )
        return 2
    print(f"drift: {len(tapes)} cassette(s) vs claude-agent-sdk {sdk_version}\n", file=out)

    drifted_frames = 0
    drifted_tapes = 0
    for tape_path in tapes:
        findings = parse_drift(_load_frames(tape_path))
        if not findings:
            print(f"  ok    {tape_path}", file=out)
            continue
        drifted_tapes += 1
        drifted_frames += len(findings)
        print(f"  DRIFT {tape_path} — {len(findings)} frame(s):", file=out)
        for f in findings:
            print(f"          frame[{f.frame_index}] {f.frame_type}: {f.reason} — {f.detail}", file=out)

    print(
        f"\n{len(tapes)} checked, {drifted_tapes} drifted "
        f"({drifted_frames} frame(s)) — re-record the drifted cassettes."
        if drifted_frames
        else f"\n{len(tapes)} checked, no drift.",
        file=out,
    )
    return 1 if drifted_frames else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="claude-agent-cassette")
    sub = parser.add_subparsers(dest="command", required=True)
    drift = sub.add_parser("drift", help="re-parse cassettes through the installed SDK")
    drift.add_argument("paths", nargs="+", help="tape .jsonl files, or dirs of them")
    drift.add_argument(
        "--allow-empty", action="store_true",
        help="exit 0 instead of failing when no cassette files are found",
    )
    args = parser.parse_args(argv)
    if args.command == "drift":
        return _drift(args.paths, sys.stdout, allow_empty=args.allow_empty)
    parser.error(f"unknown command {args.command!r}")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
