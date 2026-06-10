# Claude Agent Cassette

Record & replay the [`claude-agent-sdk`](https://github.com/anthropics/claude-agent-sdk-python)
wire for **deterministic, offline tests** — no API key, no subprocess, no mocks.

## Why

Apps built on `claude-agent-sdk` read a stream of typed messages (assistant turns,
tool results, task notifications, control-protocol frames) and drive logic off
them. The nasty bugs live at that **stream → your-handler seam**: the SDK emits a
slightly different *shape* than you expected, and your handler quietly does the
wrong thing.

Mocked tests can't catch this — you build the mock, so you only test your
understanding of your own mock. A cassette records the **real** wire once and
replays it through the SDK's **real** parser, so:

- a shape change in the SDK turns your test red instead of shipping to prod;
- tests run with no API cost, no network, no `claude` subprocess;
- the replayed frames go through the genuine `message_parser`, not a stand-in.

```
  PRODUCTION:   real CLI ──raw frames──► SDK parser ──► your code
                                              ▲
  REPLAY:       ReplayTransport ──raw frames──┘   (same parser, same code)
```

## Install

```bash
pip install claude-agent-cassette   # (or: uv add claude-agent-cassette)
```

## Replay (the common case — offline, no key)

```python
from claude_agent_cassette import replay, load_frames

async def test_my_handler():
    async with replay(load_frames("tests/cassettes/happy_path.jsonl")) as client:
        kinds = []
        async for m in client.receive_messages():
            kinds.append(type(m).__name__)
            if kinds[-1] == "ResultMessage":
                break  # stream stays open after the result; break like the real wire
        assert "ResultMessage" in kinds
        # ...or feed client.receive_messages() into your own dispatcher and
        #    assert on what it produces.
```

A frames file is JSONL of raw inbound stream-json frames — the exact dicts the
CLI emits. `replay()` injects them into a real `ClaudeSDKClient` and answers the
SDK's `initialize` control handshake for you. (Vocabulary: a **frame** is a raw
wire dict; a **message** is the typed object the SDK parses it into; a **tape**
is a full duplex recording.)

## Record (capture a real session)

`record()` works with **both** SDK entry points — the one-shot `query()`
and the interactive `ClaudeSDKClient` (it patches both transport-construction
sites the SDK uses):

```python
from claude_agent_cassette import record, save_tape

# one-shot query()
from claude_agent_sdk import query

with record() as tape:                  # tees the full duplex wire
    async for _ in query(prompt="...", options=...):
        pass
save_tape(tape, "session.jsonl")
```

```python
# interactive ClaudeSDKClient
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

with record() as tape:
    async with ClaudeSDKClient(options=ClaudeAgentOptions()) as client:
        await client.query("...")
        async for _ in client.receive_messages():
            pass
save_tape(tape, "session.jsonl")
```

`record()` captures **both directions, including the control plane**
(`control_request`/`control_response`, `mcp_message`, `hook_callback`, the
handshake), so one recording can feed both conversation replay and
control-protocol replay. Derive the conversation-only frames with
`conversation_frames(tape)`.

## Drift detection (gate SDK bumps)

Re-parse a cassette's message frames through the **installed** SDK's own
`message_parser`. A frame that no longer parses — or whose content blocks the
parser silently drops — is flagged. Because it reuses the SDK's own parser, there
is no schema to maintain: the judge is the thing being judged.

```bash
claude-agent-cassette drift tests/cassettes/      # *.jsonl files, or dirs of them
```

```text
drift: 5 cassette(s) vs claude-agent-sdk 0.2.87

  ok    happy_path.jsonl
  DRIFT stop_midtask.jsonl — 1 frame(s):
          frame[3] assistant: content_dropped — 1 of 2 content block(s) dropped on parse
  ok    notification.jsonl

5 checked, 1 drifted (1 frame) — re-record the drifted cassettes.
```

- Exits **non-zero on drift** — use it to gate an SDK-bump PR in CI.
- **Fails closed**: if no cassette files are found it exits non-zero (a mispointed
  path can't pass as a false green); pass `--allow-empty` to override.
- **Two cassette layouts** in a directory: *flat* (top-level `*.jsonl`) or *nested*
  (`<name>/input.jsonl`, where each cassette is a dir holding the recording plus
  sidecars). Nested is auto-detected; only `input.jsonl` is checked, so sibling
  `expected.jsonl` / `meta.json` are ignored, and a drift row is named by the
  cassette dir. Use `--input-name FILE` for a different recording filename. A dir
  mixing both layouts is rejected (it can't silently check only half).
- Four drift signals: `parse_error` (the parser rejected the frame), `unrecognized_type`
  (the message type is gone), `content_dropped` (a content block silently vanished),
  and `unmodeled_field` (field-level drift, opt-in — below).
- **Field-level drift** (`--fields`): catches the *additive* changes the parser
  tolerates — a recorded field the installed SDK silently ignores. Detection runs the
  SDK's real parser over an access-tracking view of each frame: anything the parser
  neither **read** nor **retained** in the typed message is unmodeled. Since most
  unmodeled fields are steady-state wire noise (`message.role`, `timestamp`), the gate
  diffs against a committed baseline sidecar (`<name>.fields.json`, or `fields.json`
  inside a nested cassette dir):

  ```bash
  claude-agent-cassette drift tests/cassettes/ --update-field-baselines  # author + commit
  claude-agent-cassette drift tests/cassettes/ --fields                  # the CI gate
  ```

  Fail-closed: a cassette without a baseline (or with a corrupt one) exits non-zero.
  Baselines are per-SDK-pin artifacts — refresh them when you bump the SDK and the
  gate notes stale entries.

In Python: `parse_drift(frames)` / `check_drift(tape)` → `list[DriftFinding]`;
`unmodeled_fields(frames)` → baseline keys; `field_drift(frames, baseline)` → findings.

## Control-protocol replay (the duplex wire)

`replay_tape(tape, mode=...)` replays a full duplex recording, including the control
plane, through a real `ClaudeSDKClient`. Break at the terminal `ResultMessage` (the
stream stays open after it, like the real wire):

```python
from claude_agent_cassette import replay_tape, load_tape

async def test_permission_flow():
    async with replay_tape(load_tape("session.jsonl"), mode="stub") as client:
        async for m in client.receive_messages():
            if type(m).__name__ == "ResultMessage":
                break
```

- **`mode="inert"`** (default) — conversation + **Direction-A** control replay: the
  `initialize` / `mcp_status` handshakes are answered from the recording; inbound
  **Direction-B** requests (`can_use_tool` / `hook_callback` / `mcp_message`) are
  dropped, so your registered callbacks stay inert.
- **`mode="stub"`** — also replay **Direction-B**: the recorded requests are delivered
  to the SDK and answered from the tape by stubs that **replace** your `can_use_tool` /
  hooks / SDK MCP servers (for `mcp_message`, a real in-process MCP server is
  synthesized from the recorded `initialize` / `tools/list` / `tools/call` traffic).
  Deterministic and inert — it certifies the recorded *wire*, not your policy.
- **`mode="verify"`** — the recorded Direction-B requests are delivered to **your real**
  `can_use_tool` / `hooks` / SDK MCP servers (nothing is replaced), and on exit each
  live decision is diffed against the recorded one — matched by `request_id`, at the
  wire. This certifies your *policy* still produces the recorded decisions: a changed
  decision or tool result, a callback that now raises (or no longer does), or an
  unanswered exchange is divergence.
- **Fail-closed end-to-end.** In `"stub"` and `"verify"` modes, any divergence from the
  tape — a live request with no recorded match, an exhausted or error decision, hook ids
  the SDK didn't reproduce, a live decision that differs from the recording, or recorded
  exchanges left unreplayed — raises `CassetteMismatchError` when the `async with` exits.
  (The SDK swallows callback exceptions into error responses, so the divergence is
  collected and surfaced on exit, not inside the callback.) A Direction-B subtype with
  no replay support (one a future SDK adds) raises up front — use `mode="inert"`.
- **Recording** a Direction-B tape needs the control decisions preserved. `scrub_tape(tape,
  replacements)` blanks PII *values* while keeping decisions intact; `lint_tape(tape)`
  lints whether a tape is still replayable (run it after scrubbing). See
  [`examples/record_permission_session.py`](examples/record_permission_session.py).

### Interrupt replay (lockstep)

`interrupt` is causally ordered on the real wire — a Stop session's terminal result is a
*consequence* of the interrupt, so it must never be delivered before the live client
issues one. When a tape records an `interrupt`, `replay_tape` automatically switches to
**lockstep** delivery: reads arrive in recorded interleaving, and each recorded SDK
`control_request` write gates everything after it on the matching live write.

```python
async def test_stop_classifies_terminal_state():
    async with replay_tape(load_tape("stop_session.jsonl")) as client:  # lockstep auto
        async for m in client.receive_messages():
            if is_my_stop_condition(m):
                await client.interrupt()        # answered from the recording
            if type(m).__name__ == "ResultMessage":
                assert m.subtype == "error_during_execution"  # arrives AFTER the interrupt
                break
```

Lockstep is strict (the trade against the default demux model's order-independence):
the live session must issue control calls in recorded order, **with recorded
arguments** (`initialize` is exempt — its payload encodes the replay environment's
wiring, not consumer intent). A consumer that never interrupts (caught after
`sync_timeout`, default 5s), a control call of the wrong subtype or arguments at a
sync point, or one issued after the tape ends raises `CassetteMismatchError` — never
a hang, never a silently impossible ordering. In `stub`/`verify` modes, a delivered
Direction-B request must be **answered** before the replay advances (on the real wire
the CLI doesn't proceed past a pending decision), so the terminal result can't race a
still-running callback. Force either model with `replay_tape(..., lockstep=True/False)`.
Recorder: [`examples/record_stop_session.py`](examples/record_stop_session.py).

## pytest plugin (record-on-miss, VCR-style)

Installing the package registers a pytest plugin (inert unless used). One marker
line per cassette — no loader code:

```python
import pytest

@pytest.mark.cassette("happy_path", mode="stub")
async def test_happy_path(cassette):
    messages = await cassette.run("List the files in this directory")
    assert type(messages[-1]).__name__ == "ResultMessage"
    # assertions stay yours — feed `messages` to your own adapter/dispatcher
```

- **Replay** — if `<test file's dir>/cassettes/happy_path.jsonl` exists, it
  replays through a real `ClaudeSDKClient` in the marker's `mode` (default
  `"stub"`; the `prompt` is ignored — the recording already answered it).
  Without a marker name, the test's name is used.
- **Record-on-miss** — if it doesn't exist, `pytest --record-cassettes` runs a
  real session (needs `ANTHROPIC_API_KEY`), scrubs it (cwd/home/API key masked
  by default — override the `cassette_scrub` fixture to extend), and saves it on
  success. *Without* the flag, a missing cassette **fails with instructions** —
  CI can never record or spend money.
- **Timeout, not hang** — a truncated recording (no terminal `ResultMessage`)
  fails fast with a clear message instead of hanging the suite
  (`cassette_timeout` ini, default 30s; per-test `timeout=` on the marker).
- **`mode="verify"`** — override the `cassette_options` fixture in your conftest
  to supply real `can_use_tool`/hooks/MCP servers; the replay then diffs your
  policy's decisions against the recording.
- Ini options: `cassette_dir` (rootdir-relative; default is `cassettes/` next to
  the test file), `cassette_timeout`.

## Examples

[`examples/`](examples/) has a runnable, no-key demo:

```bash
python examples/replay_cassette.py
# AssistantMessage:
# ResultMessage: Hello! How can I help?
```

It replays the saved [`examples/cassettes/hello_world.jsonl`](examples/cassettes/hello_world.jsonl)
through a real `ClaudeSDKClient`. (That cassette is a small, illustrative
hand-written sample with realistic wire shapes; real cassettes are *recorded* —
see above.)

The three recorder scripts each capture one Direction-B subtype as a
decision-preserving, scrubbed fixture (they spend a small API call to re-record;
the committed fixtures in [`examples/cassettes/`](examples/cassettes/) replay
offline):

- [`record_permission_session.py`](examples/record_permission_session.py) — `can_use_tool`
  (allow, allow + `updatedInput` redirect, deny)
- [`record_hooks_session.py`](examples/record_hooks_session.py) — `hook_callback` (PreToolUse)
- [`record_mcp_session.py`](examples/record_mcp_session.py) — `mcp_message`
  (in-process MCP calculator; one normal + one `is_error` tool result)

## API

| | |
| --- | --- |
| `record()` | CM that wraps the SDK's transport to capture a session's full duplex wire as a tape |
| `replay(frames, options=None)` | async CM → a connected `ClaudeSDKClient` replaying raw frames |
| `replay_tape(tape, options=None, mode=..., lockstep=None, sync_timeout=5.0)` | async CM → replay a full duplex tape incl. the control plane; `ReplayMode = "inert" \| "stub" \| "verify"`; `lockstep=None` auto-selects lockstep for interrupt tapes |
| `save_tape(tape, path)` / `load_tape(path)` | tape I/O (JSONL) |
| `load_frames(path)` | load a frames file for `replay()` |
| `inbound_frames(tape)` / `conversation_frames(tape)` | derive frame views from a tape (all inbound / conversation-only) |
| `direction_b_exchanges(tape)` → `{subtype: [ControlExchange]}` | inspect the recorded Direction-B decisions (what was allowed/denied/answered) |
| `scrub_tape(tape, replacements)` | decision-preserving PII scrub for sharing a recording |
| `lint_tape(tape)` | lint a tape for Direction-B replayability (run after scrubbing) |
| `check_drift(tape)` / `parse_drift(frames)` → `list[DriftFinding]` | drift findings vs the installed SDK |
| `unmodeled_fields(frames)` / `field_drift(frames, baseline)` | field-level drift: recorded fields the installed SDK silently ignores |
| `ReplayTransport(frames)` / `.from_tape(tape, keep_subtypes=None)` | the transport under `replay`/`replay_tape`, for wiring a client by hand |
| `LockstepReplayTransport(tape, keep_subtypes=None, sync_timeout=5.0)` | recorded-interleaving replay — sync points at recorded control writes (interrupt tapes) |
| `RecordingTransport(inner, tape)` | passive MITM tee, both directions |
| `CassetteMismatchError` | replay diverged from the recording (always fail-closed) |
| `TapeEntry` / `Frame` | the tape entry and raw-frame types |
| `claude-agent-cassette drift <path…>` | CLI drift gate (non-zero on drift / empty) |

## How it works (the non-obvious bits)

- **Replay rides the public `Transport` ABC** (`ClaudeSDKClient(transport=...)`,
  stable since SDK 0.0.22). It's solid across versions.
- **The initialize handshake**: `connect()` writes a `control_request` with a
  fresh `request_id` and blocks until it sees a `control_response` echoing it. So
  `ReplayTransport` reads that id off `write()` and synthesises the response —
  otherwise replay hangs.
- **Record patches two sites**: `ClaudeSDKClient` does a call-time import of the
  transport from its source module, while one-shot `query()` uses the name bound
  in `_internal.client`. Patching only one silently misses the other.

## Compatibility

Replay uses only the public `Transport` API. **Record and drift reach into
`claude_agent_sdk._internal`** (the subprocess transport, control-protocol shape,
and `message_parser`), so they are version-sensitive — this release targets
`claude-agent-sdk 0.2.x`. Pin your SDK and re-verify on bumps. (Drift being
version-sensitive is the point: it tells you *when* a bump broke a cassette.)

## Roadmap

See [ROADMAP.md](ROADMAP.md). Shipped: conversation replay, recording,
**Direction-A control replay** (`ReplayTransport.from_tape`), **drift detection**,
**Direction-B replay for all three subtypes** (`can_use_tool` / `hook_callback` /
`mcp_message`, in both `mode="stub"` and `mode="verify"`), and a
**decision-preserving scrub** (`scrub_tape`), a **pytest plugin** (marker/fixture,
record-on-miss, timeout-not-hang), **field-level drift** (`drift --fields`), and
**interrupt lockstep replay** (recorded interleaving, auto-selected for Stop tapes).
Next up: curation tooling, assertion helpers.

## License

MIT.
