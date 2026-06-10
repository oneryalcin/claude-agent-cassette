"""The pytest plugin, tested through real nested pytest runs (pytester).

Each test writes a miniature consumer test suite into a tmp dir and runs pytest
over it in-process with the plugin auto-loaded via its pytest11 entry point, so these cover
the consumer-visible contract from issue #4: marker/fixture discovery, the
record-on-miss gate (a missing cassette must FAIL without the flag — CI can
never record), and timeout-not-hang on a truncated recording.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

pytest_plugins = ["pytester"]

_EXAMPLES = Path(__file__).parent.parent / "examples" / "cassettes"


_INI = "[pytest]\nasyncio_mode = auto\nasyncio_default_fixture_loop_scope = function\n"


def _setup(pytester, cassettes: dict[str, Path] | None = None) -> None:
    pytester.makeini(_INI)
    for name, src in (cassettes or {}).items():
        dest = pytester.path / "cassettes"
        dest.mkdir(exist_ok=True)
        shutil.copy(src, dest / f"{name}.jsonl")


def test_marker_replays_existing_cassette(pytester):
    _setup(pytester, {"mcp": _EXAMPLES / "mcp_session.jsonl"})
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("mcp")
        async def test_replay(cassette):
            messages = await cassette.run("ignored on replay")
            assert type(messages[-1]).__name__ == "ResultMessage"
        """
    )
    pytester.runpytest().assert_outcomes(passed=1)


def test_cassette_name_defaults_to_test_name(pytester):
    _setup(pytester, {"hello": _EXAMPLES / "hello_world.jsonl"})  # frames file, no marker
    pytester.makepyfile(
        """
        async def test_hello(cassette):
            messages = await cassette.run("ignored")
            assert type(messages[-1]).__name__ == "ResultMessage"
        """
    )
    pytester.runpytest().assert_outcomes(passed=1)


def test_missing_cassette_fails_with_instructions_not_records(pytester):
    _setup(pytester)
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("nope")
        async def test_missing(cassette):
            await cassette.run("prompt")
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*cassette missing*", "*--record-cassettes*"])
    assert not (pytester.path / "cassettes" / "nope.jsonl").exists()  # nothing recorded


def test_record_flag_without_api_key_fails_closed(pytester):
    _setup(pytester)
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("nope")
        async def test_gate(cassette, monkeypatch):
            monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
            await cassette.run("prompt")
        """
    )
    result = pytester.runpytest("--record-cassettes")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*ANTHROPIC_API_KEY is not set*"])


def test_truncated_cassette_times_out_instead_of_hanging(pytester):
    # Strip the terminal result frame: the replay stream then never ends.
    src = _EXAMPLES / "mcp_session.jsonl"
    entries = [json.loads(line) for line in src.read_text().splitlines()]
    truncated = [
        e for e in entries
        if not (e.get("dir") == "read" and (e.get("frame") or {}).get("type") == "result")
    ]
    _setup(pytester)
    dest = pytester.path / "cassettes"
    dest.mkdir(exist_ok=True)
    (dest / "cut.jsonl").write_text("".join(json.dumps(e) + "\n" for e in truncated))

    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("cut", timeout=2)
        async def test_truncated(cassette):
            await cassette.run("ignored")
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*no terminal ResultMessage within 2s*"])


def test_verify_mode_runs_consumer_options_from_fixture(pytester):
    _setup(pytester, {"perm": _EXAMPLES / "permission_session.jsonl"})
    pytester.makeconftest(
        """
        import posixpath
        import pytest
        from claude_agent_sdk import (
            ClaudeAgentOptions, PermissionResultAllow, PermissionResultDeny,
        )

        @pytest.fixture
        def cassette_options():
            async def policy(tool_name, tool_input, context):
                path = tool_input.get("file_path", "")
                if path.startswith("/etc/"):
                    return PermissionResultDeny(
                        message=f"Refusing to write to system path: {path}")
                return PermissionResultAllow(updated_input={
                    **tool_input,
                    "file_path": "./safe_output/" + posixpath.basename(path)})

            return ClaudeAgentOptions(can_use_tool=policy)
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("perm", mode="verify")
        async def test_policy_still_matches_recording(cassette):
            messages = await cassette.run("ignored")
            assert messages
        """
    )
    pytester.runpytest().assert_outcomes(passed=1)


def test_verify_mode_surfaces_policy_divergence_as_failure(pytester):
    _setup(pytester, {"perm": _EXAMPLES / "permission_session.jsonl"})
    pytester.makeconftest(
        """
        import pytest
        from claude_agent_sdk import ClaudeAgentOptions, PermissionResultAllow

        @pytest.fixture
        def cassette_options():
            async def allow_everything(tool_name, tool_input, context):
                return PermissionResultAllow()

            return ClaudeAgentOptions(can_use_tool=allow_everything)
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("perm", mode="verify")
        async def test_regressed_policy(cassette):
            await cassette.run("ignored")
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*CassetteMismatchError*"])


def test_cassette_dir_ini_overrides_test_relative_default(pytester):
    _setup(pytester)
    pytester.makeini(_INI + "cassette_dir = fixtures/tapes\n")
    dest = pytester.path / "fixtures" / "tapes"
    dest.mkdir(parents=True)
    shutil.copy(_EXAMPLES / "mcp_session.jsonl", dest / "mcp.jsonl")
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.cassette("mcp")
        async def test_custom_dir(cassette):
            messages = await cassette.run("ignored")
            assert type(messages[-1]).__name__ == "ResultMessage"
        """
    )
    pytester.runpytest().assert_outcomes(passed=1)
