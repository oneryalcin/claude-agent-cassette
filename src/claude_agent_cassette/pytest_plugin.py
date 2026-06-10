"""pytest plugin: cassette discovery, record-on-miss, and a hang-proof replay driver.

Auto-registered via the ``pytest11`` entry point when both this package and
pytest are installed; inert unless a test requests the ``cassette`` fixture.

The seam (deliberate): the plugin finds the recording, replays it through a real
``ClaudeSDKClient`` (or records it on first run), bounds the whole thing with a
timeout, and hands back the typed messages. **Assertions stay with the
consumer** — this is not an assertion framework.

Usage::

    @pytest.mark.cassette("happy_path", mode="stub")
    async def test_happy_path(cassette):
        messages = await cassette.run("List the files in this directory")
        assert "ResultMessage" in [type(m).__name__ for m in messages]

- The recording lives at ``<test file's dir>/cassettes/<name>.jsonl`` (override
  the base dir with the ``cassette_dir`` ini option, rootdir-relative). Without
  a marker name, the test's name (minus ``test_``) is used.
- **Replay** (recording exists): the tape replays via
  :func:`~claude_agent_cassette.replay_tape` in the marker's ``mode``
  (default ``"stub"``; ``"verify"`` runs the real callbacks from your options —
  override the ``cassette_options`` fixture to supply them). The ``prompt``
  argument is ignored on replay (the recorded session already answered it).
- **Record-on-miss** (no recording): opt-in via ``--record-cassettes`` — runs a
  real session (spends an API call, needs ``ANTHROPIC_API_KEY``), scrubs it
  (``cassette_scrub`` fixture; defaults mask cwd/home/the API key), and saves on
  success. Without the flag a missing cassette **fails** with instructions, so
  CI can never record or spend money.
- **Timeout, not hang**: a malformed/truncated recording (no terminal
  ``ResultMessage``) fails fast with a clear message (``cassette_timeout`` ini,
  default 30s; per-test override via the marker's ``timeout=``).
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, Message, ResultMessage

from .record import record
from .replay import ReplayMode, replay, replay_tape
from .scrub import Replacements, default_replacements
from .tape import load_tape

_HELP_RECORD = "record missing cassettes by running real sessions (spends API calls)"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--record-cassettes", action="store_true", help=_HELP_RECORD)
    parser.addini(
        "cassette_dir",
        help="rootdir-relative cassette directory (default: <test file's dir>/cassettes)",
        default="",
    )
    parser.addini("cassette_timeout", help="seconds to drain a replay (default 30)", default="30")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "cassette(name=None, mode='stub', timeout=None): replay <name>.jsonl through a real "
        "ClaudeSDKClient (record on miss with --record-cassettes); mode is a ReplayMode",
    )


@dataclass
class Cassette:
    """Handle the ``cassette`` fixture yields; ``run()`` is the whole API."""

    path: Path
    mode: ReplayMode
    timeout: float
    record_enabled: bool
    scrub: Replacements
    default_options: ClaudeAgentOptions | None  # the consumer's cassette_options fixture
    nodeid: str

    async def run(
        self, prompt: str, options: ClaudeAgentOptions | None = None
    ) -> list[Message]:
        """Replay the cassette (or record it on first run) and return the typed messages.

        On replay, ``prompt`` is ignored — the recorded session already answered it.
        Divergence in ``stub``/``verify`` mode raises ``CassetteMismatchError``.
        """
        options = options if options is not None else self.default_options
        if self.path.exists():
            return await self._replay(options)
        if not self.record_enabled:
            pytest.fail(
                f"cassette missing: {self.path}\n"
                f"Record it (spends a real API call; needs ANTHROPIC_API_KEY):\n"
                f"  pytest {self.nodeid} --record-cassettes",
                pytrace=False,
            )
        if not os.environ.get("ANTHROPIC_API_KEY"):
            pytest.fail(
                "--record-cassettes given but ANTHROPIC_API_KEY is not set", pytrace=False
            )
        return await self._record(prompt, options)

    async def _replay(self, options: ClaudeAgentOptions | None) -> list[Message]:
        tape = load_tape(self.path)
        if tape and "dir" not in tape[0]:
            # A frames file (inbound frames only) — no control plane to replay.
            context = replay(tape, options=options)  # type: ignore[arg-type]
        else:
            context = replay_tape(tape, options=options, mode=self.mode)
        async with context as client:
            try:
                return await asyncio.wait_for(_drain(client), self.timeout)
            except asyncio.TimeoutError:
                pytest.fail(
                    f"cassette {self.path.name}: no terminal ResultMessage within "
                    f"{self.timeout:g}s — truncated or malformed recording? "
                    f"(re-record with --record-cassettes, or raise the marker's timeout=)",
                    pytrace=False,
                )

    async def _record(self, prompt: str, options: ClaudeAgentOptions | None) -> list[Message]:
        # Generous bound: a real session is as slow as the live model.
        record_timeout = max(self.timeout, 120.0)
        with record(self.path, scrub=self.scrub) as _tape:
            async with ClaudeSDKClient(options or ClaudeAgentOptions()) as client:
                await client.query(prompt)
                messages = await asyncio.wait_for(_drain(client), record_timeout)
        return messages


async def _drain(client: ClaudeSDKClient) -> list[Message]:
    """Collect typed messages up to and including the terminal ResultMessage."""
    messages: list[Message] = []
    async for message in client.receive_messages():
        messages.append(message)
        if isinstance(message, ResultMessage):
            break
    return messages


@pytest.fixture
def cassette_options() -> ClaudeAgentOptions | None:
    """Override in your conftest to supply ClaudeAgentOptions (e.g. for mode='verify')."""
    return None


@pytest.fixture
def cassette_scrub() -> Replacements:
    """The (needle, mask) pairs applied before a recording touches disk. Override to extend.

    Defaults to :func:`~claude_agent_cassette.default_replacements` — cwd, home,
    and the API key, in raw, realpath, AND the CLI's slug-encoded path forms (the
    project dir rides the wire as ``-Users-alice-proj`` inside
    ``~/.claude/projects/…`` strings, which a literal path needle can't match).
    """
    return default_replacements()


@pytest.fixture
def cassette(
    request: pytest.FixtureRequest,
    cassette_options: ClaudeAgentOptions | None,
    cassette_scrub: Replacements,
) -> Cassette:
    marker = request.node.get_closest_marker("cassette")
    name = (
        marker.args[0]
        if marker and marker.args
        else request.node.originalname.removeprefix("test_")
    )
    mode: ReplayMode = marker.kwargs.get("mode", "stub") if marker else "stub"
    default_timeout = float(request.config.getini("cassette_timeout"))
    timeout = float(marker.kwargs.get("timeout", default_timeout)) if marker else default_timeout

    dir_ini = str(request.config.getini("cassette_dir"))
    base = request.config.rootpath / dir_ini if dir_ini else Path(request.path).parent / "cassettes"
    return Cassette(
        path=base / f"{name}.jsonl",
        mode=mode,
        timeout=timeout,
        record_enabled=bool(request.config.getoption("--record-cassettes")),
        scrub=cassette_scrub,
        default_options=cassette_options,
        nodeid=request.node.nodeid,
    )


__all__ = ["Cassette", "cassette", "cassette_options", "cassette_scrub"]
