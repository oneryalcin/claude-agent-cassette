"""``claude-agent-cassette`` command line — drift detection for SDK-bump PRs.

    claude-agent-cassette drift <tape.jsonl | dir> ...

Re-parses each tape's message frames through the installed SDK and reports frames
that no longer parse (renamed/removed type, changed shape). Exits non-zero when
any drift is found, so it can gate a bump PR.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import claude_agent_sdk

from .drift import check_tape
from .tape import load_tape


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


def _drift(paths: list[str], out) -> int:
    sdk_version = getattr(claude_agent_sdk, "__version__", "?")
    tapes = _collect_tapes(paths)
    print(f"drift: {len(tapes)} cassette(s) vs claude-agent-sdk {sdk_version}\n", file=out)

    drifted_frames = 0
    drifted_tapes = 0
    for tape_path in tapes:
        findings = check_tape(load_tape(tape_path))
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
    args = parser.parse_args(argv)
    if args.command == "drift":
        return _drift(args.paths, sys.stdout)
    parser.error(f"unknown command {args.command!r}")  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
