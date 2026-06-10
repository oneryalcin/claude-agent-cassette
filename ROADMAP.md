# Roadmap

Directional, not a promise. Issues/PRs welcome on any of these. Every planned
item has a tracking issue; shipped items reference the issue that tracked them.

## Shipped (through v0.4.0)

- **Replay** — inject `ReplayTransport` into a real `ClaudeSDKClient`; recorded
  raw frames flow through the SDK's real parser. Answers the `initialize`
  control handshake. No API key, no subprocess.
- **Record** — `RecordingTransport` (passive MITM tee) + `record()`, which works
  with both `query()` and `ClaudeSDKClient`. Captures the **full duplex** wire,
  including the control plane.
- **Tape format** — ordered, both-directions `TapeEntry` JSONL; `save_tape` /
  `load_tape`; `conversation_frames()` to derive the conversation-only view.
- **Direction-A control replay** (#2) — `ReplayTransport.from_tape`: the recorded
  `initialize` / `mcp_status` responses replayed, id-remapped to the live
  request, fail-closed on a request the tape can't answer.
- **Direction-B replay, all three subtypes** (#2) — `replay_tape(mode="stub")`
  answers recorded `can_use_tool` / `hook_callback` / `mcp_message` requests from
  the tape (for MCP: a real in-process server synthesized from the recording);
  `mode="verify"` runs the consumer's *real* callbacks/servers and diffs each
  live decision against the recording at the wire, by `request_id`. Both modes
  fail closed end-to-end via a divergence ledger (the SDK swallows callback
  exceptions mid-replay, so divergence is surfaced on context exit).
- **Decision-preserving scrub** — `scrub_tape` blanks PII *values* while keeping
  frame structure and control decisions intact; `lint_tape` checks a (scrubbed)
  tape is still Direction-B replayable. `scrub_init_inventory` blanks the
  `system/init` environment inventory (#22); every example recorder isolates
  `CLAUDE_CONFIG_DIR`, so the committed fixtures carry no operator fingerprint.
- **Drift detection** (#3) — re-parse a tape's message frames through the
  *installed* SDK's `message_parser`; flags `parse_error`, `unrecognized_type`,
  and `content_dropped`. CLI gate for SDK-bump PRs (`claude-agent-cassette
  drift`), fail-closed on empty input, flat + nested cassette layouts (#11).
- **CI matrix across `claude-agent-sdk` versions** (#5) — replay rides the
  public `Transport` ABC; record/drift touch `_internal` and are
  version-sensitive, which the matrix trips on early.
- **pytest plugin** (#4) — `@pytest.mark.cassette` + `cassette` fixture;
  **record-on-miss** behind `--record-cassettes` (a missing cassette *fails*
  without the flag, so CI never records); recordings scrubbed before disk
  (`cassette_scrub`); timeout-not-hang on truncated recordings.
- **`record(path, scrub=...)`** — clean-exit-only atomic write; scrub applied
  before anything touches disk.
- **Field-level drift** (#9) — `unmodeled_fields` runs the SDK's real parser
  over an access-tracking view of each frame (the parser itself is the schema);
  `field_drift` gates against a committed baseline; CLI `drift --fields` /
  `--update-field-baselines`, fail-closed on missing baselines.
- **`interrupt` lockstep replay** (#7) — `LockstepReplayTransport` replays a
  tape in recorded interleaving (each recorded SDK control write is a sync
  point, with argument matching; Direction-B answers gate the walk too);
  auto-selected by `replay_tape` for interrupt tapes; fail-closed on a
  never-issued, wrong, or post-tape live control call.
- **Fixture hygiene** (#22) — recorders isolate `CLAUDE_CONFIG_DIR` + temp cwd;
  `path_replacements` covers the CLI's slug-encoded path forms; a static
  leak-regression test gates every committed cassette.
- **PEP 561** `py.typed` (#1); single-sourced version (hatchling dynamic).

## Planned

### 1. Verify outbound conversation writes ([#24](https://github.com/oneryalcin/claude-agent-cassette/issues/24))
The missing flagship: nothing today checks what the app *sends* — a changed
prompt still replays green against a stale cassette. Diff recorded vs live
outbound `user` writes (normalized for session ids/timestamps) with the same
wire-level comparator machinery Direction-B verify uses; fail closed. With it,
a cassette certifies **both** directions of the wire.

### 2. Cassette curation tooling ([#17](https://github.com/oneryalcin/claude-agent-cassette/issues/17))
Trim a recorded tape to an essential conversation (dropping whole turns, never
reshaping kept frames) and a CLI to derive a conversation-only frames file. A
trimmed tape must stay Direction-B coherent (`lint_tape` passes afterwards).

### 3. Assertion helpers ([#18](https://github.com/oneryalcin/claude-agent-cassette/issues/18))
Ordered-subsequence + exhaustive-type matching over replayed messages, for users
who want "these messages, in this order, and no extra X" without hand-rolling
it. Optional and thin — the library's core is record/replay.

### 4. Failure-path replay ([#25](https://github.com/oneryalcin/claude-agent-cassette/issues/25))
A tape always ends cleanly, so the error-handling/retry paths in a consumer app
are exactly the paths cassettes can't test. An option to end the stream with a
transport error so the SDK exercises its *real* error path.

### 5. Concurrent-session recording ([#26](https://github.com/oneryalcin/claude-agent-cassette/issues/26))
`record()` is a module-global patch; concurrent clients silently interleave into
one tape. Fail loudly at minimum; tape-per-session as the real fix.

### 6. Tape provenance + format version ([#27](https://github.com/oneryalcin/claude-agent-cassette/issues/27))
An optional metadata header (library/SDK/CLI/model versions, recorded-at,
format version) so drift reports can say what a tape was recorded under, and a
future format change has something to migrate on.

### 7. Drift-aware replay failures ([#28](https://github.com/oneryalcin/claude-agent-cassette/issues/28))
A parse failure mid-replay should diagnose itself as drift ("run the drift
gate / re-record"), not surface as a raw `MessageParseError`.

### 8. CLI: `lint` and `scrub` subcommands ([#29](https://github.com/oneryalcin/claude-agent-cassette/issues/29))
The hygiene APIs exist; CI shouldn't need a Python scratch script to call them.

## Non-goals

- **Not an app/test framework.** It records and replays the wire; assertions and
  app wiring are yours.
- **No durable/centralised recording store, no service integration.** Where
  recordings live and how they're collected across processes is an application
  concern — the library just produces and consumes tapes.
- **Never reshape captured frames.** Tooling may drop or blank, never rewrite —
  a reshaped cassette is fiction, which defeats the point.
