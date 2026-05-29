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
        kinds = [type(m).__name__ async for m in client.receive_messages()]
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
| `replay(messages, options=None)` | async CM → a connected `ClaudeSDKClient` over a `ReplayTransport` |
| `ReplayTransport(messages)` | raw frames → real parser (answers the initialize handshake) |
| `RecordingTransport(inner, tape)` | passive MITM tee, both directions |
| `record_sdk_wire()` | CM that wraps the SDK's transport to capture a query's wire |
| `serialize_tape` / `load_tape` / `load_cassette` | tape & cassette I/O |
| `read_frames(tape)` / `conversation_messages(tape)` | derive replay views from a tape |

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

Replay uses only the public `Transport` API. **Record reaches into
`claude_agent_sdk._internal`** (the subprocess transport + control-protocol
shape), so it is version-sensitive — this release targets `claude-agent-sdk
0.2.x`. Pin your SDK and re-verify on bumps.

## License

MIT.
