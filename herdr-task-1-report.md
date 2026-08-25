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
