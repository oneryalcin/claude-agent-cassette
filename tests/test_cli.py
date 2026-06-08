"""The ``claude-agent-cassette drift`` command."""
from __future__ import annotations

import json
from pathlib import Path

from claude_agent_cassette.cli import main

_FIXTURE = str(Path(__file__).parent / "fixtures" / "websearch_control_tape.jsonl")


def _drifted_tape(dest: Path) -> Path:
    """A copy of the fixture with one frame's type renamed (simulated SDK drift)."""
    out = dest / "drifted.jsonl"
    lines = []
    for line in open(_FIXTURE):
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        if entry.get("dir") == "read" and entry["frame"].get("type") == "assistant":
            entry["frame"]["type"] = "assistant_v2"
        lines.append(json.dumps(entry))
    out.write_text("\n".join(lines) + "\n")
    return out


def test_clean_fixture_exits_zero(capsys):
    assert main(["drift", _FIXTURE]) == 0
    assert "no drift" in capsys.readouterr().out


def test_drift_exits_nonzero_and_reports(capsys):
    assert main(["drift", _FIXTURE]) == 0  # baseline
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        _drifted_tape(Path(d))
        rc = main(["drift", d])  # directory: collects *.jsonl
    out = capsys.readouterr().out
    assert rc == 1
    assert "DRIFT" in out
    assert "unrecognized_type" in out
    assert "re-record" in out


def test_reports_installed_sdk_version(capsys):
    main(["drift", _FIXTURE])
    assert "claude-agent-sdk" in capsys.readouterr().out


def test_no_command_errors():
    import pytest
    with pytest.raises(SystemExit):
        main([])
