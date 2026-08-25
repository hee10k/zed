# Herdr–Zed Thread Synchronization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a bidirectional, cross-platform bridge that maps one Herdr session's workspaces to Zed root threads and recognized Herdr agent panes to Zed Herdr-backed subthreads.

**Architecture:** Add a platform-neutral Herdr protocol client and a platform-specific local transport boundary inside `agent_ui`. Persist session-qualified workspace/pane mappings, reconcile them through a per-window bridge, and expose Herdr-backed root/subthread views through the existing AgentPanel/sidebar activation paths. Herdr remains authoritative for process execution, prompts, status, output, and closure; Zed owns presentation and forwards control requests.

**Tech Stack:** Rust, GPUI entities and background tasks, `serde`/`serde_json`, Zed's `db::kvp::KeyValueStore`, existing `ThreadMetadataStore` and `AgentPanel`, newline-delimited JSON, Unix domain sockets on macOS/Linux, Windows named pipes.

## Global Constraints

- Support macOS, Linux, and Windows desktop builds.
- Bind one Zed window to one Herdr session; qualify every mapping by Herdr session identity.
- Use Herdr's local Unix socket or Windows named pipe; do not add arbitrary TCP endpoints.
- Keep ACP-native Zed threads unchanged and do not map ordinary shell/log panes to subthreads.
- Herdr is authoritative for process execution, prompts, agent state, output, cancellation, and closure.
- Never fabricate an ACP connection or ACP session for a Herdr-backed view.
- Suppress reflected focus/lifecycle events with operation origin and monotonic sequence fencing.
- Preserve mappings through restart and retain tombstones long enough to reject late events.
- Propagate fallible errors or log them visibly; do not use `unwrap()` on live paths or silently discard errors.
- Use GPUI executor timers for GPUI tests that depend on `run_until_parked()`.
- Do not create `mod.rs`; use descriptive Rust file paths.

---

## File Map

### New files

- `crates/agent_ui/src/herdr_transport.rs` — platform-specific endpoint connection and newline-delimited frame I/O.
- `crates/agent_ui/src/herdr_client.rs` — Herdr request/response/event types, protocol codec, request routing, subscription handling.
- `crates/agent_ui/src/herdr_mapping_store.rs` — session-qualified persisted workspace/pane mappings and tombstones.
- `crates/agent_ui/src/herdr_bridge.rs` — per-window session binding, snapshot reconciliation, lifecycle state machine, focus fencing, and UI-facing bridge events.
- `crates/agent_ui/src/herdr_conversation_view.rs` — Herdr-backed root conversation surface and child subthread collection.
- `crates/agent_ui/src/herdr_thread_view.rs` — Herdr-backed subthread rendering, output/status display, prompt routing, and lifecycle controls.

### Modified files

- `crates/agent_ui/src/agent_ui.rs` — register new modules, global bridge registry, and initialization hooks.
- `crates/agent_ui/src/agent_panel.rs` — register AgentPanel with the window bridge, activate Herdr-backed roots, forward focus/actions, and render the Herdr surface.
- `crates/agent_ui/src/thread_metadata_store.rs` — reserve the Herdr agent identity marker and prevent Herdr root records from being treated as ordinary drafts.
- `crates/sidebar/src/sidebar.rs` — keep Herdr root rows activatable, route title/close actions through Herdr-backed metadata, and expose connection/session state.
- `crates/sidebar/src/thread_switcher.rs` — include Herdr-backed root rows using the existing MRU path without treating them as ACP sessions.
- `crates/agent_ui/Cargo.toml` — add the existing Windows API dependency required by the named-pipe transport.

### Existing seams to reuse

- `AgentPanel::new`, `AgentPanel::load_agent_thread`, `AgentPanel::set_base_view`, `AgentPanel::active_thread_id`, `AgentPanel::serialize`, and `AgentPanelEvent::ActiveViewChanged` in `crates/agent_ui/src/agent_panel.rs`.
- `ThreadMetadataStore::{global,save,archive,unarchive,set_title_override,set_generated_title}` and `ThreadMetadata::is_draft` in `crates/agent_ui/src/thread_metadata_store.rs`.
- `Sidebar::{activate_thread_locally,activate_thread_in_other_window,sync_active_entry_from_panel}` and its `AgentPanelEvent` subscription in `crates/sidebar/src/sidebar.rs`.
- `ConversationView`/`ThreadView` focus and child navigation patterns in `crates/agent_ui/src/conversation_view.rs` and `crates/agent_ui/src/conversation_view/thread_view.rs`.
- `KeyValueStore::global`, `ScopedKeyValueStore`, and `MultiWorkspace`'s per-window persistence pattern in `crates/db/src/kvp.rs` and `crates/workspace/src/persistence.rs`.

---

### Task 1: Implement the Herdr protocol codec and cross-platform transport

**Files:**
- Create: `crates/agent_ui/src/herdr_transport.rs`
- Create: `crates/agent_ui/src/herdr_client.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`

- Modify: `crates/agent_ui/Cargo.toml` — add a Windows-only `windows.workspace` dependency for the named-pipe transport.

**Interfaces:**
- Produces `HerdrEndpoint`, `HerdrRequest`, `HerdrResponse`, `HerdrEvent`, `HerdrSnapshot`, `HerdrWorkspaceSnapshot`, `HerdrAgentSnapshot`, `HerdrAgentSessionIdentity`, `HerdrAgentStatus`, `HerdrClientHandle`, `HerdrApi`, and `HerdrClientError` for later bridge tasks.
- `HerdrEndpoint` must represent a default session, named session, or explicit platform endpoint without exposing Unix-only path types to Windows code.
- `HerdrClientHandle::request(method, params)` returns a GPUI `Task<anyhow::Result<Box<RawValue>>>`; `subscribe(types)` returns a stream/channel of decoded `HerdrEvent` values, and `HerdrApi` exposes the typed workspace/pane/agent operations used by the bridge.

- [ ] **Step 1: Add failing codec tests**

Add tests in `herdr_client.rs` for:

```rust
#[test]
fn decodes_success_response_by_request_id() {
    let response = decode_response(
        r#"{"id":"req-1","result":{"type":"pong"}}"#,
    )
    .unwrap();

    assert_eq!(response.id, "req-1");
    assert!(response.error.is_none());
}

#[test]
fn decodes_workspace_focused_subscription_event() {
    let event = decode_event(
        r#"{"event":"workspace.focused","data":{"event":"workspace_focused","workspace_id":"w1"}}"#,
    )
    .unwrap();

    assert_eq!(event.workspace_id(), Some("w1"));
}

#[test]
fn rejects_malformed_json_frame() {
    assert!(decode_response("not-json").is_err());
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test -p agent_ui herdr_client::tests::decodes_success_response_by_request_id -- --exact
```

Expected: FAIL because the codec types and functions do not exist.

- [ ] **Step 3: Implement the shared protocol types and codec**

Use `serde_json::value::RawValue` for unrecognized result payloads, typed enums for supported lifecycle events, and explicit error variants for malformed frames and protocol errors:

```rust
#[derive(Debug, Clone, Serialize)]
struct HerdrRequest<'a> {
    id: String,
    method: &'a str,
    params: RawValue,
}

#[derive(Debug, Deserialize)]
struct HerdrResponse {
    id: String,
    #[serde(default)]
    result: Option<Box<RawValue>>,
    #[serde(default)]
    error: Option<HerdrErrorBody>,
}

#[derive(Debug, Deserialize)]
struct HerdrEventEnvelope {
    event: String,
    data: Box<RawValue>,
}
```

Decode only the event families required by the approved design: workspace lifecycle/focus, pane lifecycle/focus/output, agent detection/status/session, and subscription acknowledgements.

- [ ] **Step 4: Implement platform endpoint discovery and transport**

Use a single transport enum with platform-specific variants:

```rust
enum HerdrStream {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    NamedPipe(std::fs::File),
}
```

Resolve endpoint precedence exactly as the spec requires: explicit selection, `HERDR_SOCKET_PATH`, `HERDR_SESSION`, then the default endpoint. On Unix, connect to Herdr's filesystem Unix socket. On Windows, resolve Herdr's endpoint marker and open the corresponding namespaced pipe with the existing Windows pipe APIs; do not pass the marker path to `net::async_net::UnixStream`. Keep blocking stream reads off the GPUI foreground thread by using the existing background executor/thread patterns. Send one JSON request per line and preserve one response/event per line.

The first connection sequence is ordered to avoid Herdr's non-replaying subscription gap: send `ping`, send `events.subscribe`, wait for `subscription_started`, start buffering pushed events, then request `session.snapshot`. After snapshot reconciliation, replay buffered events newer than the snapshot sequence.

- [ ] **Step 5: Implement request matching and subscription delivery**

Maintain a request ID map, route matching responses to their waiters, route event lines to a bounded channel, and terminate the connection on malformed frames, EOF, protocol mismatch, or write failure. Ensure connection shutdown wakes all pending requests with the same concrete error. Treat unknown JSON fields as forward-compatible and reject only unsupported protocol versions or malformed required fields.


- [ ] **Step 6: Run focused codec and transport tests**

Run:

```bash
cargo test -p agent_ui herdr_client::tests
cargo test -p agent_ui herdr_transport::tests
```

Expected: PASS on the host platform; platform-specific transport tests run under their respective target CI jobs.

- [ ] **Step 7: Commit the transport slice**

```bash
git add Cargo.toml crates/agent_ui/Cargo.toml crates/agent_ui/src/agent_ui.rs crates/agent_ui/src/herdr_client.rs crates/agent_ui/src/herdr_transport.rs
git commit -m "feat(agent_ui): add cross-platform Herdr transport"
```

---

### Task 2: Add persisted mappings and pure reconciliation state

**Files:**
- Create: `crates/agent_ui/src/herdr_mapping_store.rs`
- Create: `crates/agent_ui/src/herdr_state.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`

**Interfaces:**
- Consumes the protocol types from Task 1.
- Produces `HerdrMappingKey`, `HerdrMappingRecord`, `HerdrLifecycleState`, `HerdrOperationOrigin`, `ReconciliationAction`, and pure snapshot/event reconciliation functions for Task 3.
- Persists with `db::kvp::KeyValueStore` under a dedicated namespace; do not add a database migration for the first mapping implementation.

- [ ] **Step 1: Add failing mapping and reconciliation tests**

Add tests covering session-qualified identity, restoration by agent session, ambiguous worktree matching, tombstones, and reflected focus suppression:

```rust
#[test]
fn same_workspace_id_in_different_sessions_never_collides() {
    let first = HerdrMappingKey::workspace("alpha", "w1");
    let second = HerdrMappingKey::workspace("beta", "w1");
    assert_ne!(first, second);
}

#[test]
fn snapshot_restores_existing_workspace_mapping() {
    let mapping = mapping_for_workspace("alpha", "w1", ThreadId::new());
    let actions = reconcile_snapshot(&[workspace("alpha", "w1")], &[mapping]);
    assert_eq!(actions, vec![ReconciliationAction::RestoreWorkspaceRoot(mapping)]);
}

#[test]
fn reflected_focus_operation_is_not_emitted_again() {
    let state = BridgeState::default().with_pending_focus("op-1", FocusTarget::Workspace("w1"));
    let actions = apply_event(state, focused_event("w1", "op-1"));
    assert!(actions.outbound.is_empty());
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test -p agent_ui herdr_state::tests -- --exact
```

Expected: FAIL because the mapping and reconciliation types do not exist.

- [ ] **Step 3: Implement the mapping model and persistence**

Define the composite key and record explicitly:

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) struct HerdrMappingKey {
    pub session: String,
    pub workspace_id: String,
    pub pane_id: Option<String>,
    pub agent_session: Option<HerdrAgentSessionIdentity>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HerdrMappingRecord {
    pub key: HerdrMappingKey,
    pub zed_root_thread_id: ThreadId,
    pub zed_subthread_session_id: Option<String>,
    pub worktree_or_cwd_identity: Option<String>,
    pub last_seen_sequence: u64,
    pub lifecycle: HerdrLifecycleState,
}
```

Persist one serialized session map per Herdr session, use atomic replacement through `ScopedKeyValueStore::write`, and preserve tombstones instead of deleting records immediately.

- [ ] **Step 4: Implement pure snapshot/event reconciliation**

Return explicit actions rather than mutating GPUI entities from pure state code:

```rust
pub(crate) enum ReconciliationAction {
    CreateWorkspaceRoot(HerdrWorkspaceSnapshot),
    RestoreWorkspaceRoot(HerdrMappingRecord),
    CreateAgentSubthread(HerdrAgentSnapshot),
    RestoreAgentSubthread(HerdrMappingRecord),
    UpdateTitle(HerdrMappingKey, String),
    UpdateStatus(HerdrMappingKey, HerdrAgentStatus),
    Activate(HerdrMappingKey),
    Archive(HerdrMappingKey),
    RecordConflict(HerdrMappingKey, String),
}
```

Apply exact session-qualified matches first, then agent-session restoration, and never reuse a mapping based only on cwd/worktree. Reject reversed sequence values and identify reflected operations by origin plus operation ID.

- [ ] **Step 5: Run mapping and state tests**

Run:

```bash
cargo test -p agent_ui herdr_mapping_store::tests
cargo test -p agent_ui herdr_state::tests
```

Expected: PASS.

- [ ] **Step 6: Commit the mapping slice**

```bash
git add crates/agent_ui/src/agent_ui.rs crates/agent_ui/src/herdr_mapping_store.rs crates/agent_ui/src/herdr_state.rs
git commit -m "feat(agent_ui): persist Herdr thread mappings"
```

---

### Task 3: Build the per-window Herdr bridge and root-thread lifecycle

**Files:**
- Create: `crates/agent_ui/src/herdr_bridge.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/agent_ui/src/thread_metadata_store.rs`

**Interfaces:**
- Consumes `HerdrClientHandle`, mappings, and reconciliation actions from Tasks 1–2.
- Produces `HerdrBridgeRegistry`, `HerdrThreadBridge`, `HerdrBridgeEvent`, `HerdrConnectionStatus`, and methods for root focus, Herdr-backed action requests, session rebinding, and status reporting.
- The registry is keyed by `gpui::WindowId`; each bridge has exactly one Herdr session binding.

- [ ] **Step 1: Add failing bridge lifecycle tests**

Add tests for workspace creation, rename, focus, close, reconnection, and session rebinding using a fake `HerdrApi` implementation:

```rust
#[test]
fn workspace_created_creates_a_herdr_root_mapping() {
    let mut bridge = test_bridge();
    bridge.apply_event(workspace_created("w1", "/repo", "Review"));
    assert!(bridge.root_mapping("w1").is_some());
}

#[test]
fn session_rebind_disconnects_old_session_before_loading_new_snapshot() {
    let mut bridge = test_bridge_in_session("alpha");
    bridge.rebind_session("beta").unwrap();
    assert_eq!(bridge.session_name(), "beta");
    assert_eq!(bridge.status(), HerdrConnectionStatus::Synchronizing);
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test -p agent_ui herdr_bridge::tests::workspace_created_creates_a_herdr_root_mapping -- --exact
```

Expected: FAIL because the bridge and fake API do not exist.

- [ ] **Step 3: Register the per-window bridge**

Add the registry to `agent_ui::init`, and expose a lookup that uses the current `WindowId`:

```rust
pub(crate) struct HerdrBridgeRegistry {
    bridges: HashMap<WindowId, Entity<HerdrThreadBridge>>,
}

impl HerdrBridgeRegistry {
    pub(crate) fn for_window(
        &mut self,
        window_id: WindowId,
        session: HerdrSessionSelection,
        cx: &mut App,
    ) -> Entity<HerdrThreadBridge>;
}
```

`AgentPanel::new` registers its window and subscribes to bridge events. When the last panel for a window is dropped, the registry stops that bridge and closes the event subscription.

- [ ] **Step 4: Reconcile Herdr workspaces into root metadata**

For every workspace in the initial snapshot and every `workspace.created` event, create or restore a `ThreadMetadata` record with:

```rust
ThreadMetadata {
    thread_id: ThreadId::new(),
    session_id: None,
    agent_id: HERDR_AGENT_ID.clone(),
    title: Some(workspace.label.into()),
    title_override: None,
    updated_at: Utc::now(),
    created_at: Some(Utc::now()),
    interacted_at: None,
    worktree_paths: WorktreePaths::from_path_lists(workspace_paths, PathList::default()),
    remote_connection: None,
    archived: false,
    user_order: None,
}
```

Reserve `HERDR_AGENT_ID` in `thread_metadata_store.rs` and update `ThreadMetadata::is_draft` so a Herdr root with `session_id: None` is not treated as a disposable ACP draft. Keep ACP-native metadata behavior unchanged.

- [ ] **Step 5: Wire root focus and lifecycle events into AgentPanel**

Add `AgentPanel::load_herdr_thread` and route `load_agent_thread` through the Herdr mapping check before ACP loading. On `AgentPanelEvent::ActiveViewChanged`, report only Herdr-backed root activation to the bridge. On bridge root events, locate the matching open workspace panel and activate it using the existing `set_base_view`/focus path.

- [ ] **Step 6: Implement reconnect and operation fencing**

The bridge must transition through `Unavailable`, `Reconnecting`, `Synchronizing`, and `Ready`. On reconnect, send `ping`, register a fresh `events.subscribe`, buffer pushed events, request `session.snapshot`, apply Task 2 reconciliation, replay buffered events newer than the snapshot sequence, replace the old subscription, and only then emit the authoritative current focus. All outgoing requests carry an operation ID and origin; reflected events are acknowledged without a second outgoing request.

- [ ] **Step 7: Run bridge and metadata tests**

Run:

```bash
cargo test -p agent_ui herdr_bridge::tests
cargo test -p agent_ui thread_metadata_store::tests
```

Expected: PASS.

- [ ] **Step 8: Commit the root bridge slice**

```bash
git add crates/agent_ui/src/agent_ui.rs crates/agent_ui/src/agent_panel.rs crates/agent_ui/src/thread_metadata_store.rs crates/agent_ui/src/herdr_bridge.rs
git commit -m "feat(agent_ui): synchronize Herdr workspaces with threads"
```

---

### Task 4: Add Herdr-backed root and subthread views

**Files:**
- Create: `crates/agent_ui/src/herdr_conversation_view.rs`
- Create: `crates/agent_ui/src/herdr_thread_view.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/agent_ui/src/herdr_bridge.rs`

**Interfaces:**
- Consumes Herdr bridge events and the Herdr API from Tasks 1–3.
- Produces `HerdrConversationView`, `HerdrThreadView`, and `HerdrSubthreadState` entities.
- Prompt, cancel, focus, rename, and close methods must call the bridge; they must not call ACP or write directly to terminal processes.

- [ ] **Step 1: Add failing view-model tests**

Test session identity arrival, child activation, status updates, output revision ordering, prompt forwarding, and close confirmation:

```rust
#[test]
fn agent_session_identity_creates_a_selectable_subthread() {
    let mut view = test_view();
    view.apply(agent_detected("w1:p1", "omp", "session-1"));
    assert_eq!(view.subthreads().len(), 1);
}

#[test]
fn older_output_revision_cannot_replace_newer_output() {
    let mut view = test_view();
    view.apply(output("w1:p1", 4, "new"));
    view.apply(output("w1:p1", 3, "old"));
    assert_eq!(view.output("w1:p1"), "new");
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test -p agent_ui herdr_conversation_view::tests::agent_session_identity_creates_a_selectable_subthread -- --exact
```

Expected: FAIL because the Herdr-backed view entities do not exist.

- [ ] **Step 3: Implement the Herdr-backed state and child views**

Keep the root/subthread relationship explicit:

```rust
pub(crate) struct HerdrConversationView {
    pub(crate) thread_id: ThreadId,
    pub(crate) workspace_id: String,
    pub(crate) subthreads: IndexMap<String, Entity<HerdrThreadView>>,
    pub(crate) active_pane_id: Option<String>,
    pub(crate) focus_handle: FocusHandle,
    bridge: WeakEntity<HerdrThreadBridge>,
}

pub(crate) struct HerdrThreadView {
    pub(crate) pane_id: String,
    pub(crate) session: HerdrAgentSessionIdentity,
    pub(crate) title: SharedString,
    pub(crate) status: HerdrAgentStatus,
    pub(crate) output: String,
    pub(crate) output_revision: u64,
    pub(crate) focus_handle: FocusHandle,
}
```

Render the workspace title, connection state, child agent cards, status, recent output, focus action, prompt editor, cancel action, and close action. Hydrate each child with `pane.read`/`agent.read` and apply only revisions newer than the current view. The prompt editor submits structured text to `HerdrThreadBridge::prompt_agent`; cancellation sends the supported `agent.send_keys` or `pane.send_keys` sequence because Herdr has no `agent.cancel` method; closing uses `pane.close`. Do not emulate terminal typing unless the Herdr API explicitly requires a pane input operation.

- [ ] **Step 4: Integrate the view into AgentPanel**

Add the Herdr-backed root view to `BaseView`/`VisibleSurface` and update every exhaustive match in `AgentPanel` so these methods work for both ACP and Herdr roots:

- `serialize` and active-thread restoration;
- `set_base_view` and retained-view handling;
- `active_thread_id`, title rendering, focus handling, and render;
- title editing, close actions, and panel menu context;
- `AgentPanelEvent::ActiveViewChanged` and sidebar synchronization.

Do not make the Herdr surface satisfy ACP-only methods by returning fake sessions. Add explicit `active_herdr_view`/backend checks where existing methods are ACP-specific.

- [ ] **Step 5: Wire child activation in both directions**

When Herdr emits `pane_focused`, call `HerdrConversationView::activate_subthread`. When a user selects a child card or full-screen subthread in Zed, call `HerdrThreadBridge::focus_pane` and update the active child only after Herdr confirms focus. Creating a Herdr-backed child uses `pane.split`/existing pane attachment and `agent.start` where the pane is a shell; it persists the returned pane/session identity before exposing the child. Preserve loop suppression from Task 3.

- [ ] **Step 6: Run view tests**

Run:

```bash
cargo test -p agent_ui herdr_conversation_view::tests
cargo test -p agent_ui herdr_thread_view::tests
cargo test -p agent_ui agent_panel::tests::herdr
```

Expected: PASS.

- [ ] **Step 7: Commit the Herdr-backed view slice**

```bash
git add crates/agent_ui/src/agent_ui.rs crates/agent_ui/src/agent_panel.rs crates/agent_ui/src/herdr_bridge.rs crates/agent_ui/src/herdr_conversation_view.rs crates/agent_ui/src/herdr_thread_view.rs
git commit -m "feat(agent_ui): render Herdr agents as subthreads"
```

---

### Task 5: Wire sidebar activation, session selection, and lifecycle controls

**Files:**
- Modify: `crates/sidebar/src/sidebar.rs`
- Modify: `crates/sidebar/src/thread_switcher.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`

**Interfaces:**
- Consumes Herdr root metadata and bridge events from Tasks 2–4.
- Produces user-facing session connection status, explicit session rebinding action, and sidebar/thread-switcher activation that works for Herdr-backed roots.

- [ ] **Step 1: Add failing sidebar activation tests**

Add tests that activate a Herdr root through the sidebar and assert the bridge receives `workspace.focus`, while ACP-native activation does not call Herdr:

```rust
#[test]
fn activating_herdr_root_requests_herdr_workspace_focus() {
    let fixture = SidebarFixture::with_herdr_root("alpha", "w1");
    fixture.activate_thread();
    assert_eq!(fixture.herdr_calls(), [HerdrCall::FocusWorkspace("w1")]);
}

#[test]
fn activating_acp_root_does_not_request_herdr_focus() {
    let fixture = SidebarFixture::with_acp_root();
    fixture.activate_thread();
    assert!(fixture.herdr_calls().is_empty());
}
```

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact
```

Expected: FAIL because sidebar fixtures and bridge routing do not know the Herdr backend.

- [ ] **Step 3: Preserve Herdr-backed rows through sidebar rebuilds**

Update `Sidebar::rebuild_contents` and row classification so Herdr roots use their persisted worktree/cwd identity, remain visible when disconnected, and are not treated as ACP draft rows. Keep existing ordering and project-group behavior. Add a session label only when needed to distinguish disconnected historical sessions.

- [ ] **Step 4: Route activation and close/title actions**

Update `activate_thread_locally`, `activate_thread_in_other_window`, title editing, archive/close, new Herdr-backed root creation, and the thread switcher to inspect the Herdr backend marker/mapping before choosing ACP paths. Creating a Herdr-backed root calls `workspace.create` and persists the returned workspace identity before the Zed row becomes active. Herdr-backed activation must invoke the window bridge; ACP-native activation must retain existing behavior.

- [ ] **Step 5: Add explicit session rebinding UI**

Register a `Connect to Herdr Session` action from `agent_ui::init`/`AgentPanel` and show `Ready`, `Synchronizing`, `Reconnecting`, `Unavailable`, and `Conflict` states in the existing agent/thread surface. Session selection must call `HerdrBridgeRegistry::rebind_session` only after explicit user choice.

- [ ] **Step 6: Run sidebar and thread-switcher tests**

Run:

```bash
cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact
cargo test -p sidebar sidebar::tests::activating_acp_root_does_not_request_herdr_focus -- --exact
cargo test -p agent_ui agent_panel::tests::herdr
```

Expected: PASS.

- [ ] **Step 7: Commit the navigation/UI slice**

```bash
git add crates/sidebar/src/sidebar.rs crates/sidebar/src/thread_switcher.rs crates/agent_ui/src/agent_panel.rs crates/agent_ui/src/agent_ui.rs
git commit -m "feat(sidebar): add Herdr session navigation"
```

---

**Files:**
- Modify: `crates/agent_ui/src/herdr_client.rs`
- Modify: `crates/agent_ui/src/herdr_transport.rs`
- Modify: `crates/agent_ui/src/herdr_bridge.rs`
- Modify: `crates/agent_ui/src/herdr_conversation_view.rs`
- Modify: `crates/sidebar/src/sidebar.rs`
- Create: `crates/agent_ui/src/herdr_test_support.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs` to register the test-support module

**Interfaces:**
- Consumes the complete bridge and UI from Tasks 1–5.
- Produces deterministic fake-server coverage and a documented manual smoke command for real Herdr.

- [ ] **Step 1: Add a fake Herdr NDJSON server fixture**

Implement a deterministic fixture in `herdr_test_support.rs` that responds to `ping`, accepts `events.subscribe` and returns `subscription_started`, buffers and emits subscription events, then responds to `session.snapshot`. Cover `workspace.focus`, `workspace.rename`, `workspace.close`, `pane.focus`, `agent.prompt`, `agent.send_keys`, `pane.send_keys`, `pane.close`, and pane reads. The fixture must expose recorded calls, controlled sequence values, revisioned output, and disconnect/reconnect hooks without depending on a real Herdr installation.

- [ ] **Step 2: Add end-to-end focus tests**

Cover all four directions:

```text
Herdr workspace.focused -> Zed root activation
Zed root activation -> Herdr workspace.focus
Herdr pane_focused -> Zed subthread activation
Zed subthread activation -> Herdr pane.focus
```

Assert exactly one outbound focus request per user action and zero reflected-loop requests.

- [ ] **Step 3: Add lifecycle/reconnect tests**

Run fake-server scenarios for workspace/agent creation, rename, close, pane exit, session ID arrival, Herdr disconnect, subscribe-before-snapshot reconnect, session rebinding, stale event rejection, and ambiguous mapping conflict. Assert that ACP-native threads remain usable while Herdr is unavailable and that no `agent.cancel`/`agent.close` request is emitted.

- [ ] **Step 4: Add Unix transport fixture coverage**

On Unix, bind a temporary Unix domain socket and run the real `HerdrTransport` against it. Assert newline framing, concurrent request matching, subscription event delivery, EOF handling, and reconnect.

- [ ] **Step 5: Add Windows named-pipe coverage**

Under `cfg(windows)`, create a temporary Herdr-compatible named-pipe server and marker endpoint using the existing Windows pipe APIs, then run the real named-pipe transport against it. Assert the same framing, request, event, and reconnect behavior without importing Unix socket modules or treating the marker file as a byte stream.

- [ ] **Step 6: Run platform-specific verification**

Run on the host platform:

```bash
cargo test -p agent_ui herdr_client
cargo test -p agent_ui herdr_transport
cargo test -p agent_ui herdr_bridge
cargo test -p agent_ui herdr_conversation_view
cargo test -p agent_ui agent_panel::tests::herdr
cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact
```

Run the Windows named-pipe tests and target compile in Windows CI, and the Unix socket tests on macOS and Linux CI. Use `./script/clippy` for the final clippy pass, not `cargo clippy`.

- [ ] **Step 7: Execute the real Herdr smoke scenario**

With Herdr running, create two workspaces and two recognized agent panes. Verify workspace focus in both directions, pane/subthread focus in both directions, prompt/cancel/rename/close forwarding, Herdr restart recovery, and rebinding one Zed window between two named sessions. Repeat on macOS, Linux, and Windows.

- [ ] **Step 8: Commit the verification slice**

```bash
git add crates/agent_ui crates/sidebar
git commit -m "test(agent_ui): cover Herdr thread synchronization"
```

---

## Final Verification Checklist

- [ ] `cargo test -p agent_ui herdr_client`
- [ ] `cargo test -p agent_ui herdr_transport`
- [ ] `cargo test -p agent_ui herdr_mapping_store`
- [ ] `cargo test -p agent_ui herdr_state`
- [ ] `cargo test -p agent_ui herdr_bridge`
- [ ] `cargo test -p agent_ui herdr_conversation_view`
- [ ] `cargo test -p agent_ui herdr_thread_view`
- [ ] `cargo test -p agent_ui agent_panel::tests::herdr`
- [ ] `cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact`
- [ ] macOS Unix-socket smoke test
- [ ] Linux Unix-socket smoke test
- [ ] Windows named-pipe smoke test
- [ ] Windows target compile
- [ ] `./script/clippy`
- [ ] Real Herdr lifecycle smoke test with two workspaces, two agent panes, restart, and named-session rebinding
