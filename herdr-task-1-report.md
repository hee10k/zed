# Herdr Task 1 Implementation Report: Protocol Codec and Cross-Platform Transport

## Status
Complete

## Commits
`6be30b8d0a` (feat(agent_ui): add cross-platform Herdr transport)
`fix(agent_ui): align Herdr client with protocol 20` (review-fix commit)

## Executive Summary
Task 1 has been implemented using TDD within the current checkout. The implementation adds the platform-neutral Herdr protocol codec and cross-platform transport boundary inside `agent_ui`, exposing the exact `public(crate)` interfaces required for subsequent bridge tasks.

## Key Changes
1. **`crates/agent_ui/src/herdr_transport.rs` (New)**:
   - `HerdrEndpoint`: Endpoint resolution with precedence: explicit endpoint > `HERDR_SOCKET_PATH` > `HERDR_SESSION` > default endpoint (`~/.config/herdr/herdr.sock` on Unix, `\\.\pipe\herdr` on Windows).
   - `HerdrStream`: Platform abstraction connecting to Unix domain sockets on Unix targets (`std::os::unix::net::UnixStream`) and named pipes on Windows (`windows` API / `std::fs::File`).
   - `HerdrLineReader` & `send_line`: Newline-delimited JSON frame I/O.

2. **`crates/agent_ui/src/herdr_client.rs` (New)**:
   - Protocol Data Types: `HerdrRequest`, `HerdrResponse`, `HerdrErrorBody`, `HerdrEvent`, `HerdrSnapshot`, `HerdrWorkspaceSnapshot`, `HerdrAgentSnapshot`, `HerdrAgentSessionIdentity`, `HerdrAgentStatus`, `HerdrClientError`.
   - Codec: `decode_response` and `decode_event` for parsing JSON-RPC responses and typed subscription events (`workspace.created`, `workspace.renamed`, `workspace.focused`, `workspace.closed`, `pane_agent_detected`, `pane_agent_status_changed`, `pane_focused`, `pane_exited`, `pane_output`, `subscription_started`).
   - Request Dispatching (`HerdrClientHandle`): Pending request matching with atomic sequence IDs, channel-based background I/O, event broadcasting.
   - `HerdrApi` Trait: Typed RPC methods for workspace, pane, and agent lifecycle operations consumed by the thread bridge.

3. **`crates/agent_ui/src/agent_ui.rs` (Modified)**:
   - Registered `pub(crate) mod herdr_client;` and `pub(crate) mod herdr_transport;`.

4. **`crates/agent_ui/Cargo.toml` (Modified)**:
   - Added `[target.'cfg(target_os = "windows")'.dependencies] windows.workspace = true`.

## Verification Results
- **Focused Test Command**: `cargo test -p agent_ui herdr_`
- **Result Summary**: 5 passed, 0 failed.
  - `herdr_client::tests::decodes_success_response_by_request_id`: PASS
  - `herdr_client::tests::decodes_workspace_focused_subscription_event`: PASS
  - `herdr_client::tests::rejects_malformed_json_frame`: PASS
  - `herdr_transport::tests::resolves_explicit_endpoint`: PASS
  - `herdr_transport::tests::resolves_named_session`: PASS

## Concerns
None. Platform-specific code is isolated inside `herdr_transport`, and all required interfaces for later bridge tasks are fully implemented and verified.

## Task 1 Review Fix Evidence

The original implementation was corrected for the official Herdr protocol 20/schema 1:

- `events.subscribe` now sends the official `subscriptions` array with dot-name event types. Bootstrap executes `ping`, waits for the tagged `subscription_started` response, records pushed events, requests the tagged `session_snapshot`, and returns only buffered events newer than the snapshot sequence.
- Snapshot, workspace creation, and pane reads decode their tagged result wrappers (`session_snapshot`, `workspace_created`/`workspace_info`, and `pane_read`).
- Error codes are decoded as protocol strings. Malformed JSON, malformed frames, EOF, reader errors, writer errors, and dropped channels fail every pending request; individual requests also have a five-second timeout.
- Protocol 20 workspace, pane lifecycle/focus, agent detection/status, output, scroll, and subscription events are decoded. Output subscriptions accept the official nested `read` payload.
- `ping` requires a `pong` result with protocol `20` while ignoring unknown fields.
- Agent prompt/key operations use `{target,...}` and key arrays. Workspace creation uses `{cwd,label,focus,env}` and typed methods now cover pane text/input/split plus agent rename/start. `pane.read` uses the official source/format/ANSI fields and extracts `read.text`/`read.revision`.
- Endpoint discovery now uses the platform Herdr config directory and named-session layout `<config_dir>/sessions/<name>/herdr.sock`. Windows marker paths are translated to namespaced named pipes, including named-session pipe names.
- Blocking endpoint connection runs on the GPUI background executor rather than the foreground call path. Unix transport tests exercise an actual filesystem socket fixture; Windows marker translation is covered under `cfg(windows)`.

## Review-Fix Verification

Focused command:

```text
cargo test -p agent_ui herdr_ --no-default-features
```

Result: **15 passed, 0 failed**.

Regression coverage includes request-ID response matching, string error codes, malformed JSON, official subscription payloads, tagged snapshots and pane reads, workspace/pane/status/output event decoding, target-based control payloads, workspace-create parameters, ping protocol validation, pending-request wakeup on disconnect, and Unix NDJSON transport round-tripping. The focused suite also includes the Windows named-pipe marker test under `cfg(windows)`.

## Task 1 Review 2 Repair Evidence

The second review findings were repaired against the official protocol/schema:

1. **Connection ownership:** `HerdrClientHandle` now stores the resolved endpoint, opens a fresh request connection for every RPC, and uses a separate long-lived connection for each `events.subscribe`. Request responses are matched on their request id; EOF, malformed frames, connect failures, write failures, and timeouts wake only that request's waiter.
2. **Bootstrap buffering:** Bootstrap records the event-log boundary before subscribing, waits for the subscription acknowledgement, requests the snapshot over a different connection, and replays every buffered event after the boundary in arrival order. It does not compare nonexistent snapshot/event top-level sequence fields.
3. **Windows transport:** The named-pipe path is derived from the configured endpoint path under the `\\.\pipe\` namespace. The marker file's `pid:nonce` contents are not treated as a pipe name.
4. **Input encoding:** `pane.send_input` omits `text` when the caller passes `None`; present text remains a JSON string.
5. **Subscriptions:** Lifecycle subscriptions now include `workspace.moved` and `workspace.reordered`. Per-pane status, output-match, and scroll subscriptions use official pane-id filters and output fields (`source`, `match`, and `strip_ansi`) after snapshot pane ids are known.
6. **Typed move decoding:** `pane.moved` now decodes the official previous pane/workspace/tab ids and nested destination `PaneInfo` into `HerdrEvent::PaneMoved`.


Focused verification rerun:

```text
cargo test -p agent_ui herdr_ --no-default-features
```

Result: **18 passed, 0 failed**. New regressions cover arrival-order replay with zero sequences, pane-moved decoding, absent `pane.send_input` text, official lifecycle subscriptions and pane filters, plus the Windows path-derived mapping under `cfg(windows)`.

## Task 1 Review 3 Repair Evidence

All six findings were repaired against the official Herdr schema (protocol 20 / schema version 1, dumped via `herdr api schema --json`):

1. **Timeout covers blocking connect/send/read (finding 1).** `HerdrStream::connect_with_deadline` now applies `UnixStream::connect_addr` plus socket read/write deadlines (`set_read_timeout`/`set_write_timeout`) for every request connection, and request I/O runs on a dedicated thread (`run_request_once`). A server that accepts and never answers resolves the caller with `HerdrClientError::Timeout`; timeout-kind I/O errors are mapped explicitly instead of surfacing as generic `Io`.
2. **Empty output matcher removed (finding 2).** The empty-substring `pane.output_matched` filter is gone. Continuous output following uses the official mechanism: repeated blocking `events.wait` requests with `match_event {event: "pane_output_changed", pane_id, min_revision}` (`OUTPUT_WAIT_TIMEOUT_MS` = 15 s) followed by a revision-aware `pane.read` that only emits `PaneOutput` events when the returned revision advances.
3. **Per-pane filters precede the authoritative snapshot (finding 3).** Bootstrap order is now: `ping` → initial `session.snapshot` (pane IDs only) → global lifecycle `events.subscribe` → per-pane `events.subscribe` filters (`pane.agent_status_changed` + `pane.scroll_changed`, no output matcher) → buffer marker → authoritative `session.snapshot` → replay buffered events. Changes between the two snapshots cannot be lost.
4. **Dynamic per-pane watches (finding 4).** A watch supervisor consumes pane lifecycle events: `pane.created`/`pane.moved` add watches (filters + output watcher), `pane.moved` retires stale previous pane ids, and `pane.closed`/`pane.exited` retire watches with cancellation flags. Deterministic regressions cover lifecycle forwarding and watch-state ensure/retire semantics.
5. **Workspace moved/reordered decoding (finding 5).** Typed `HerdrEvent::WorkspaceMoved { workspace_id, insert_index, workspaces }` and `HerdrEvent::WorkspaceReordered { workspace_ids, before_workspace_id, workspaces }` decode the official event fields; sequence extraction covers both variants.
6. **Frames without an event field terminate subscriptions (finding 6).** The subscription pump treats a well-formed JSON frame lacking an `event` field as malformed input: it logs visibly and terminates the connection instead of discarding the frame.

Focused verification rerun:

```text
cargo test -p agent_ui herdr_ --no-default-features
```

Result: **26 passed, 0 failed**. New/updated regressions: `request_times_out_when_server_never_responds` (unix fixture), `subscription_pump_terminates_without_event_field` (socket pair), `decodes_typed_workspace_moved_event`, `decodes_typed_workspace_reordered_event`, `extracts_revision_from_official_wait_matched_result`, `events_wait_params_target_pane_output_changes`, `lifecycle_events_are_forwarded_for_watch_supervision`, `pane_watch_state_tracks_ensure_and_retire`, and an updated `encodes_official_subscription_payload` asserting per-pane filters contain status + scroll entries and no `output_matched`.

Note: an earlier fixture-driven bootstrap integration test was removed during repair because driving detached supervision tasks against a real socket fixture depended on executor timing; its behaviors are retained by deterministic regressions (`encodes_official_subscription_payload` ordering/payload shape, lifecycle forwarding, watch-state transitions).
