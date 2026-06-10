# Roadmap

Directional, not a promise. Issues/PRs welcome on any of these.

## Shipped (v0.1)

- **Replay** — inject `ReplayTransport` into a real `ClaudeSDKClient`; recorded
  raw frames flow through the SDK's real parser. Answers the `initialize`
  control handshake. No API key, no subprocess.
- **Record** — `RecordingTransport` (passive MITM tee) + `record_sdk_wire()`,
  which works with both `query()` and `ClaudeSDKClient`. Captures the **full
  duplex** wire, including the control plane.
- **Tape format** — ordered, both-directions `TapeEntry` JSONL; serialize/load;
  `conversation_messages()` to derive a conversation view.
- Runnable example, tests (replay + record, no key).

## Planned

### 1. Control-protocol replay
- **Direction A** (SDK→CLI: `initialize`, `mcp_status`, …) — **shipped**
  (`ReplayTransport.from_tape`): the recorded response is replayed, id-remapped to the
  live request.
- **Direction B** (CLI→SDK: `can_use_tool`, `hook_callback`, `mcp_message`) — **stub
  replay shipped** for `can_use_tool` + `hook_callback` (`replay_tape(mode="stub")`):
  the recorded requests are delivered and answered from the tape by stubs, fail-closed
  end-to-end on divergence.
- **`verify` mode** — **shipped** (`replay_tape(mode="verify")`): the consumer's *real*
  `can_use_tool` / `hooks` answer the recorded requests, and each live decision is
  diffed against the recording at the wire, matched by `request_id` (tests the policy,
  not just the wire). Fail-closed on a changed decision, a callback that now raises,
  or an unanswered exchange. Remaining:
  - **`mcp_message`** stubbing — synthesize an in-process MCP server from the recorded
    `initialize` / `tools/list` / `tools/call` results. (For verify mode, the recorded
    MCP payloads embed environment-dependent bits — e.g. `serverInfo` versions — so the
    diff needs more than dict equality; study with a real recording.)
  - **`interrupt` lockstep** — ordering-sensitive Direction-A control where a conversation
    frame must land after a control exchange.

### 2. pytest integration
- Cassette discovery + a fixture/marker (`@pytest.mark.cassette("name")`).
- **Record-on-miss** ergonomics (VCR-style): replay if a cassette exists, else
  record it on first run.
- A per-test timeout default so a malformed cassette fails fast instead of hanging.

### 3. Drift detection
- Re-parse a cassette's frames through the *installed* SDK's `message_parser`; a
  parse failure (or a `None` result for a now-unrecognised type) means the wire
  shape drifted → re-record. Reuses the SDK's own parser, so it can't disagree
  with what the SDK actually accepts.
- A CLI (`claude-agent-cassette drift <dir>`) for SDK-bump PRs: report stale
  cassettes + `sdk_version` skew.

### 4. Cassette tooling
- Curation helpers: trim a recorded tape to an essential conversation; CLI to
  turn a recorded tape into a replayable cassette.
- **Redaction/scrub helper** — **shipped** (`scrub_tape`): blanks PII *values* while
  keeping frame structure and control decisions intact. `direction_b_replay_findings`
  lints whether a scrubbed tape is still replayable.

### 5. Assertion helpers (optional, light)
- Ordered-subsequence + exhaustive-type matching over emitted messages, so users
  who want it can assert "these messages, in this order, and no extra X" without
  hand-rolling it. Kept optional — the library's core is record/replay; you bring
  your own assertions.

### 6. Compatibility
- CI matrix across `claude-agent-sdk` versions. **Replay** rides the public
  `Transport` ABC and is stable; **record** touches `_internal` and is
  version-sensitive — pin and re-verify on bumps. Track the 0.2.x line; watch for
  0.3 breaking the `_internal` layout.

## Non-goals

- **Not an app/test framework.** It records and replays the wire; assertions and
  app wiring are yours.
- **No durable/centralised recording store, no service integration.** Where
  recordings live and how they're collected across processes is an application
  concern — the library just produces and consumes tapes.
- **Never reshape captured frames.** Tooling may drop or blank, never rewrite —
  a reshaped cassette is fiction, which defeats the point.
