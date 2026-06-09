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


# --- nested <name>/input.jsonl layout (issue #11) -------------------------------

_CLEAN = {"type": "assistant", "session_id": "s",
          "message": {"model": "m", "content": [{"type": "text", "text": "x"}]}}
_DRIFTED = {"type": "assistant_RENAMED", "message": {}}


def _nested_cassette(parent: Path, frame: dict, name: str = "input.jsonl") -> Path:
    """A nested cassette dir: <parent>/<name> holding one raw frame."""
    parent.mkdir(parents=True, exist_ok=True)
    (parent / name).write_text(json.dumps(frame) + "\n")
    return parent


def test_nested_layout_clean_passes(capsys, tmp_path):
    _nested_cassette(tmp_path / "des6250", _CLEAN)
    assert main(["drift", str(tmp_path)]) == 0
    assert "no drift" in capsys.readouterr().out


def test_nested_layout_drift_detected(capsys, tmp_path):
    _nested_cassette(tmp_path / "error_result", _DRIFTED)
    rc = main(["drift", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "unrecognized_type" in out


def test_nested_sibling_expected_never_drift_checked(capsys, tmp_path):
    """The break-test: a drifted expected.jsonl sidecar must NOT be checked —
    only input.jsonl is a cassette (allowlist, not a denylist of names to skip)."""
    cass = _nested_cassette(tmp_path / "des6250", _CLEAN)        # recording: clean
    (cass / "expected.jsonl").write_text(json.dumps(_DRIFTED) + "\n")  # answer key: would drift
    (cass / "meta.json").write_text("{}")
    assert main(["drift", str(tmp_path)]) == 0                   # clean → sidecar ignored
    assert "no drift" in capsys.readouterr().out


def test_nested_drift_row_names_directory_not_input_file(capsys, tmp_path):
    _nested_cassette(tmp_path / "des6250", _DRIFTED)
    main(["drift", str(tmp_path)])
    out = capsys.readouterr().out
    assert "des6250" in out          # identified by cassette dir name
    assert "input.jsonl" not in out  # not the ambiguous filename


def test_mixed_layout_fails_closed(capsys, tmp_path):
    """Flat *.jsonl + nested */input.jsonl in one dir is ambiguous; checking only
    one set would silently drop the other's coverage. Must fail, not half-run."""
    (tmp_path / "flat.jsonl").write_text(json.dumps(_CLEAN) + "\n")
    _nested_cassette(tmp_path / "des6250", _CLEAN)
    rc = main(["drift", str(tmp_path)])
    err = capsys.readouterr().err
    assert rc == 2
    assert "ambiguous layout" in err


def test_input_name_selects_custom_nested_file(capsys, tmp_path):
    """--input-name selects nested-only mode with a custom recording filename."""
    _nested_cassette(tmp_path / "des6250", _DRIFTED, name="wire.jsonl")
    rc = main(["drift", str(tmp_path), "--input-name", "wire.jsonl"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "unrecognized_type" in out


def test_input_name_is_nested_only_no_flat_fallback(capsys, tmp_path):
    """Explicit --input-name means nested intent: a top-level *.jsonl is not a
    fallback, so a dir with only flat tapes matches nothing and fails closed."""
    (tmp_path / "flat.jsonl").write_text(json.dumps(_DRIFTED) + "\n")
    rc = main(["drift", str(tmp_path), "--input-name", "input.jsonl"])
    assert rc == 2
    assert "nothing checked" in capsys.readouterr().err
