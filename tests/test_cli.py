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


def test_raw_frame_cassette_is_detected_not_silently_passed(capsys, tmp_path):
    """A raw inbound-frame cassette (no dir/frame envelope) with a renamed type must
    be flagged — previously load_tape filtered it to nothing and reported 'no drift'."""
    raw = tmp_path / "raw_cassette.jsonl"
    raw.write_text(json.dumps({"type": "assistant_RENAMED", "message": {}}) + "\n")
    rc = main(["drift", str(raw)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "unrecognized_type" in out


def test_raw_frame_cassette_clean_passes(capsys, tmp_path):
    raw = tmp_path / "ok.jsonl"
    raw.write_text(json.dumps(
        {"type": "assistant", "session_id": "s",
         "message": {"model": "m", "content": [{"type": "text", "text": "x"}]}}) + "\n")
    assert main(["drift", str(raw)]) == 0
    assert "no drift" in capsys.readouterr().out


def test_no_cassettes_found_fails_closed(capsys, tmp_path):
    """A gate that checked nothing must NOT report success (empty/mispointed dir)."""
    rc = main(["drift", str(tmp_path)])  # existing dir, no *.jsonl
    err = capsys.readouterr().err
    assert rc == 2
    assert "nothing checked" in err


def test_allow_empty_opt_in(capsys, tmp_path):
    assert main(["drift", str(tmp_path), "--allow-empty"]) == 0


def test_no_command_errors():
    import pytest
    with pytest.raises(SystemExit):
        main([])
