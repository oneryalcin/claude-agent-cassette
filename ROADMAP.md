# Roadmap

Directional, not a promise. Issues/PRs welcome on any of these. Every planned
item has a tracking issue; shipped items reference the issue that tracked them.

## Shipped (through v0.3.0)

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
- **PEP 561** `py.typed` (#1); single-sourced version (hatchling dynamic).

## Planned

### 1. `interrupt` lockstep ([#7](https://github.com/oneryalcin/claude-agent-cassette/issues/7)) — **shipped on main**
`LockstepReplayTransport` replays a tape in **recorded interleaving**: each
recorded SDK `control_request` write is a sync point gating everything after it
on the matching live write. `replay_tape` auto-selects it when the tape records
an `interrupt` (the one Direction-A subtype where ordering is load-bearing — the
terminal result is *caused by* the Stop, so demux could deliver orderings the
real system can't produce). Fail-closed on a never-issued or wrong-subtype live
call and on post-tape control calls; narrows the flow-control constraint to the
real wire's (a control response can be starved only by frames recorded before
it, never by the rest of the tape). Fixture: a real recorded Stop session
(`examples/record_stop_session.py`).

### 2. pytest integration ([#4](https://github.com/oneryalcin/claude-agent-cassette/issues/4)) — **shipped on main**
`@pytest.mark.cassette("name", mode=..., timeout=...)` + `cassette` fixture
(`await cassette.run(prompt)` → typed messages); **record-on-miss** behind
`--record-cassettes` (a missing cassette *fails* without the flag, so CI never
records; recordings are scrubbed before disk via the `cassette_scrub` fixture);
timeout-not-hang on truncated recordings (`cassette_timeout` ini).

### 3. Field-level drift ([#9](https://github.com/oneryalcin/claude-agent-cassette/issues/9)) — **shipped on main**
`unmodeled_fields(frames)` runs the SDK's real parser over an access-tracking
view of each frame: anything neither read nor retained in the typed message is a
recorded field the installed SDK silently ignores — the parser itself is the
schema, so nothing rots. `field_drift(frames, baseline)` gates against a
committed baseline (steady-state ignored fields are facts, not drift); CLI:
`drift --fields` / `--update-field-baselines`, fail-closed on missing baselines.

### 4. Cassette curation tooling ([#17](https://github.com/oneryalcin/claude-agent-cassette/issues/17))
Trim a recorded tape to an essential conversation (dropping whole turns, never
reshaping kept frames) and a CLI to derive a conversation-only frames file. A
trimmed tape must stay Direction-B coherent (`lint_tape` passes afterwards).

### 5. Assertion helpers ([#18](https://github.com/oneryalcin/claude-agent-cassette/issues/18))
Ordered-subsequence + exhaustive-type matching over replayed messages, for users
who want "these messages, in this order, and no extra X" without hand-rolling
it. Optional and thin — the library's core is record/replay.

## Non-goals

- **Not an app/test framework.** It records and replays the wire; assertions and
  app wiring are yours.
- **No durable/centralised recording store, no service integration.** Where
  recordings live and how they're collected across processes is an application
  concern — the library just produces and consumes tapes.
- **Never reshape captured frames.** Tooling may drop or blank, never rewrite —
  a reshaped cassette is fiction, which defeats the point.
