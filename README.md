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
from claude_agent_cassette import replay, load_cassette

async def test_my_handler():
    async with replay(load_cassette("tests/cassettes/happy_path.jsonl")) as client:
        kinds = []
        async for m in client.receive_messages():
            kinds.append(type(m).__name__)
            if kinds[-1] == "ResultMessage":
                break  # stream stays open after the result; break like the real wire
        assert "ResultMessage" in kinds
        # ...or feed client.receive_messages() into your own dispatcher and
        #    assert on what it produces.
```

A **cassette** is a JSONL file of raw inbound stream-json frames — the exact dicts
the CLI emits. `replay()` injects them into a real `ClaudeSDKClient` and answers
the SDK's `initialize` control handshake for you.

## Record (capture a real session)

`record_sdk_wire()` works with **both** SDK entry points — the one-shot `query()`
and the interactive `ClaudeSDKClient` (it patches both transport-construction
sites the SDK uses):

```python
from pathlib import Path
from claude_agent_cassette import record_sdk_wire, serialize_tape

# one-shot query()
from claude_agent_sdk import query

with record_sdk_wire() as tape:                  # tees the full duplex wire
    async for _ in query(prompt="...", options=...):
        pass
Path("session.jsonl").write_text(serialize_tape(tape))
```

```python
# interactive ClaudeSDKClient
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

with record_sdk_wire() as tape:
    async with ClaudeSDKClient(options=ClaudeAgentOptions()) as client:
        await client.query("...")
        async for _ in client.receive_messages():
            pass
Path("session.jsonl").write_text(serialize_tape(tape))
```

`record_sdk_wire()` captures **both directions, including the control plane**
(`control_request`/`control_response`, `mcp_message`, `hook_callback`, the
handshake), so one recording can feed both conversation replay and
control-protocol replay. Derive a conversation cassette with
`conversation_messages(tape)`.

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
- Three drift signals: `parse_error` (the parser rejected the frame), `unrecognized_type`
  (the message type is gone), `content_dropped` (a content block silently vanished).
- **Scope**: catches *parse-level* drift (rejected/skipped frames) + dropped content
  blocks. It does **not** catch additive *field-level* drift (a still-parsing frame
  that gained a field) — see [ROADMAP.md](ROADMAP.md).

In Python: `parse_drift(frames)` / `check_tape(tape)` → `list[DriftFinding]`.

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
  hooks. Deterministic and inert — it certifies the recorded *wire*, not your policy.
  (A future `"verify"` mode will run your real callbacks and assert they match.)
- **Fail-closed end-to-end.** In `"stub"` mode, any divergence from the tape — a live
  request with no recorded match, an exhausted or error decision, hook ids the SDK
  didn't reproduce, or recorded exchanges left unreplayed — raises `CassetteMismatchError`
  when the `async with` exits. (The SDK swallows callback exceptions into error
  responses, so the divergence is collected and surfaced on exit, not inside the stub.)
  A subtype with no stub builder yet (`mcp_message`) raises up front — use `mode="inert"`.
- **Recording** a Direction-B tape needs the control decisions preserved. `scrub_tape(tape,
  replacements)` blanks PII *values* while keeping decisions intact; `direction_b_replay_findings(tape)`
  lints whether a tape is still replayable (run it after scrubbing). See
  [`examples/record_permission_session.py`](examples/record_permission_session.py).

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

## API

| | |
| --- | --- |
| `replay(messages, options=None)` | async CM → a connected `ClaudeSDKClient` over a `ReplayTransport` (conversation cassette) |
| `replay_tape(tape, options=None, mode="inert"\|"stub")` | async CM → replay a full duplex tape incl. the control plane |
| `ReplayTransport(messages)` / `.from_tape(tape, keep_control_requests=None)` | raw frames → real parser (answers the initialize handshake) |
| `RecordingTransport(inner, tape)` | passive MITM tee, both directions |
| `record_sdk_wire()` | CM that wraps the SDK's transport to capture a query's wire |
| `serialize_tape` / `load_tape` / `load_cassette` | tape & cassette I/O |
| `read_frames(tape)` / `conversation_messages(tape)` | derive replay views from a tape |
| `control_stub_options(tape, base=None)` → `ControlStubBundle` | wire Direction-B stubs + keep-set + divergence ledger by hand |
| `scrub_tape(tape, replacements)` | decision-preserving PII scrub for sharing a recording |
| `direction_b_replay_findings(tape)` | lint a tape for Direction-B replayability |
| `parse_drift(frames)` / `check_tape(tape)` | drift findings vs the installed SDK |
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
**Direction-B stub replay** (`replay_tape(mode="stub")` for `can_use_tool` /
`hook_callback`), and a **decision-preserving scrub** (`scrub_tape`). Next up:
`mcp_message` stubbing, a **`verify` mode** (run your real callbacks, assert they match
the tape), `interrupt` lockstep, a pytest plugin with record-on-miss, and field-level
drift.

## License

MIT.
