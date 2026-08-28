# Zed-Owned Herdr Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Herdr synchronization begin only for a Herdr process launched from a local Zed terminal or by an explicit Zed action, while preserving a named session and root-thread mappings across Zed restarts.

**Architecture:** Persist a UUID-based Herdr session reservation and ownership history in the existing per-window `MultiWorkspaceState`. Add a generic local-terminal process snapshot/event seam and a window-scoped ownership observer; the observer activates one named Herdr bridge only after an accepted server-launch command. Keep the bridge retry loop gated by the observed owner process, persist Herdr-to-Zed workspace ownership, and reject ambiguous routing instead of selecting the first path match.

**Tech Stack:** Rust, GPUI entities/tasks/events, Zed `MultiWorkspace` persistence, `KeyValueStore`, existing Herdr NDJSON Unix-socket/named-pipe transport, `sysinfo` process inspection, Cargo unit/GPUI tests.

## Global Constraints

- One Zed window owns one Zed-dedicated named Herdr session.
- Herdr synchronization begins only when Herdr is launched from inside Zed.
- A Herdr process launched in another terminal is independent and is not synchronized with Zed.
- Zed does not automatically launch Herdr when Zed starts.
- Zed restart restores the name and mappings but requires a new in-Zed Herdr launch before reconnecting.
- Herdr remains running when its owning Zed window closes; Zed detaches.
- Owner terminal/process identity gates retry; owner process exit returns the bridge to `Dormant`.
- The `herdr` server-launch executable is accepted; CLI subcommands, aliases/wrappers, remote terminals, mismatched sessions, and unrelated processes are rejected.
- The Herdr session name is generated independently of runtime `WindowId` and persisted across restart.
- The canonical root mapping is `Herdr session + Herdr workspace_id <-> Zed root ThreadId`.
- Persisted owning Zed `WorkspaceId` takes precedence; zero or multiple path matches produce a disconnected/conflict state, never a first-match binding.
- Herdr unavailable during an owned launch is a supported state: retry with bounded backoff, update connection status, and do not show repeated automatic-failure Toasts.
- Explicit user action failures remain actionable Toasts.
- Remote Zed terminals cannot own a local Herdr session.
- Preserve Herdr's Unix socket, Windows named-pipe, NDJSON, and structured request contracts.

---

## Repository Map

| File | Responsibility in this change |
| --- | --- |
| `crates/workspace/src/persistence/model.rs` | Add serde-defaulted Herdr session reservation and ownership history to `MultiWorkspaceState`. |
| `crates/workspace/src/persistence.rs` | Preserve new state through per-window KVP serialization and restored-window assembly. |
| `crates/workspace/src/multi_workspace.rs` | Hold runtime Herdr window state, expose reservation/ownership accessors, serialize it, and flush it. |
| `crates/workspace/src/workspace.rs` | Restore the state before the restored window creates terminal/panel behavior. |
| `crates/project/src/project.rs` | Add a terminal-added event so window observers can discover newly created project terminals. |
| `crates/project/src/terminals.rs` | Apply a window-scoped `HERDR_SESSION` environment overlay to local terminal creation. |
| `crates/terminal/src/pty_info.rs` | Expose a safe foreground-process snapshot and compare argv/PID changes. |
| `crates/terminal/src/terminal.rs` | Add local/remote and foreground-process accessors plus a process-change event. |
| `crates/terminal_view/src/terminal_panel.rs` | Pass the owning window ID into project terminal creation paths. |
| `crates/terminal_view/src/terminal_view.rs` | Preserve terminal process-change subscriptions when a terminal is replaced. |
| `crates/zed_actions/src/lib.rs` | No change required for the new action: the existing agent action namespace lives in `agent_ui`. |
| `crates/agent_ui/src/agent_ui.rs` | Register the new `OpenHerdr` action and the window observer for local terminal events. |
| `crates/agent_ui/src/herdr_ownership.rs` | New pure parser/runtime owner value for accepted Herdr server launches. |
| `crates/agent_ui/src/herdr_bridge.rs` | Make bridge acquisition lazy, add owner-process gating, and expose window activation/release APIs. |
| `crates/agent_ui/src/herdr_mapping_store.rs` | Persist optional owning Zed `WorkspaceId` on Herdr mapping records. |
| `crates/agent_ui/src/herdr_state.rs` | Carry owner metadata through root reconciliation without changing session-qualified keys. |
| `crates/agent_ui/src/agent_panel.rs` | Stop eager default binding, attach active bridges, launch Herdr explicitly, and handle owner process events. |
| `crates/workspace/src/persistence.rs` tests | State round-trip, legacy decode, restored-window transfer, and flush ordering. |
| `crates/terminal/src/terminal.rs` tests | Process snapshot/argv change and local/remote filtering. |
| `crates/agent_ui/src/herdr_ownership.rs` tests | Accepted/rejected command forms and session validation. |
| `crates/agent_ui/src/herdr_bridge.rs` tests | Lazy activation, owner gating, process exit, and quiet reconnect. |
| `crates/agent_ui/src/agent_panel.rs` tests | Action launch, terminal-triggered activation, restart state, and panel attachment. |
| `crates/sidebar/src/sidebar_tests.rs` | Root routing, ambiguity conflicts, external-process isolation, and restored ThreadId behavior. |

---

### Task 1: Persist window-scoped Herdr ownership state

**Files:**
- Modify: `crates/workspace/src/persistence/model.rs:108-126`
- Modify: `crates/workspace/src/multi_workspace.rs:306-390,1468-1503`
- Modify: `crates/workspace/src/workspace.rs` restored multi-workspace application path
- Test: `crates/workspace/src/persistence.rs` existing state serialization tests

**Interfaces:**
- `MultiWorkspaceState` produces two serde-defaulted fields:

```rust
#[serde(default)]
pub herdr_session_name: Option<String>,
#[serde(default)]
pub herdr_owned: bool,
```

- `MultiWorkspace` provides these window-scoped methods:

```rust
pub fn herdr_session_name(&self) -> Option<&str>;
pub fn reserve_herdr_session_name(&mut self, cx: &mut Context<Self>) -> String;
pub fn set_herdr_owned(&mut self, owned: bool, cx: &mut Context<Self>);
pub fn restore_herdr_state(
    &mut self,
    session_name: Option<String>,
    owned: bool,
    cx: &mut Context<Self>,
);
```

`reserve_herdr_session_name` generates `zed-<uuid>` with `uuid::Uuid::new_v4()` only when no name is present, calls `serialize`, and returns the existing name otherwise. It never starts a bridge.

- `MultiWorkspace::serialize` includes both fields in the state payload.
- `apply_restored_multiworkspace_state` calls `restore_herdr_state` before restoring sidebar state or allowing terminal launch observers to run.

- [ ] **Step 1: Write the failing state round-trip tests**

Add tests beside the existing `read_multi_workspace_state` coverage:

```rust
#[test]
fn multi_workspace_state_round_trips_herdr_ownership() {
    let state = MultiWorkspaceState {
        active_workspace_id: None,
        sidebar_open: false,
        project_groups: Vec::new(),
        sidebar_state: None,
        herdr_session_name: Some("zed-1234".to_string()),
        herdr_owned: true,
    };

    let encoded = serde_json::to_string(&state).expect("encode state");
    let decoded: MultiWorkspaceState =
        serde_json::from_str(&encoded).expect("decode state");
    assert_eq!(decoded.herdr_session_name.as_deref(), Some("zed-1234"));
    assert!(decoded.herdr_owned);
}

#[test]
fn legacy_multi_workspace_state_defaults_herdr_fields() {
    let decoded: MultiWorkspaceState = serde_json::from_str(
        r#"{"active_workspace_id":null,"sidebar_open":false,"project_groups":[]}"#,
    )
    .expect("decode legacy state");
    assert_eq!(decoded.herdr_session_name, None);
    assert!(!decoded.herdr_owned);
}
```

- [ ] **Step 2: Run the tests and confirm the expected red result**

Run:

```bash
cargo test -p workspace --lib multi_workspace_state_round_trips_herdr_ownership -- --exact
cargo test -p workspace --lib legacy_multi_workspace_state_defaults_herdr_fields -- --exact
```

Expected: compilation fails because the new fields do not yet exist.

- [ ] **Step 3: Add runtime fields and state accessors**

Add `herdr_session_name: Option<String>` and `herdr_owned: bool` to `MultiWorkspace`, initialize them to `None`/`false` in `MultiWorkspace::new`, and implement the methods from the interface block. Generate names with:

```rust
let name = format!("zed-{}", uuid::Uuid::new_v4());
```

Do not derive a name from `WindowId`.

- [ ] **Step 4: Wire serialize and restore paths**

Extend the `MultiWorkspaceState` literal in `MultiWorkspace::serialize` with the runtime fields. In the restored-window path, call `restore_herdr_state` from the same `MultiWorkspace` update that restores `project_groups`, `sidebar_open`, and `sidebar_state`. Preserve `#[serde(default)]` behavior for older records.

- [ ] **Step 5: Add restart transfer coverage**

Extend `test_read_serialized_multi_workspaces_with_state` or add a neighboring test that writes a state under an old persisted window key, calls `read_serialized_multi_workspaces`, and asserts the returned `SerializedMultiWorkspace.state` contains `herdr_session_name` and `herdr_owned`. Then assert the new `MultiWorkspace::serialize` writes those values under the new runtime window key.

- [ ] **Step 6: Run the focused workspace tests**

Run:

```bash
cargo test -p workspace --lib persistence::tests::multi_workspace_state_round_trips_herdr_ownership -- --exact
cargo test -p workspace --lib persistence::tests::legacy_multi_workspace_state_defaults_herdr_fields -- --exact
cargo test -p workspace --lib persistence::tests::test_read_serialized_multi_workspaces_with_state -- --exact
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit the persistence slice**

```bash
git add crates/workspace/src/persistence/model.rs crates/workspace/src/persistence.rs crates/workspace/src/multi_workspace.rs crates/workspace/src/workspace.rs
git commit -m "feat(workspace): persist Herdr window ownership"
```

---

### Task 2: Persist owning Zed workspace metadata on Herdr roots

**Files:**
- Modify: `crates/agent_ui/src/herdr_mapping_store.rs:108-144,237-377`
- Modify: `crates/agent_ui/src/herdr_state.rs root reconciliation actions`
- Modify: `crates/agent_ui/src/herdr_bridge.rs root metadata and owner setters`
- Test: `crates/agent_ui/src/herdr_mapping_store.rs`
- Test: `crates/agent_ui/src/herdr_state.rs`

**Interfaces:**
- Add an optional field to `HerdrMappingRecord`:

```rust
#[serde(default)]
pub zed_workspace_id: Option<workspace::WorkspaceId>,
```

Keep `HerdrMappingKey` unchanged so existing session/workspace/pane identity
keys remain compatible. Add the field to all record literals and preserve it
through `root`, `CreateWorkspaceRoot`, `RestoreWorkspaceRoot`, and mapping
serialization.

- Add bridge accessors/mutators:

```rust
pub(crate) fn root_zed_workspace_id(
    &self,
    herdr_workspace_id: &str,
) -> Option<workspace::WorkspaceId>;

pub(crate) fn set_root_zed_workspace_id(
    &mut self,
    herdr_workspace_id: &str,
    zed_workspace_id: workspace::WorkspaceId,
    cx: &mut Context<Self>,
) -> bool;
```

The setter updates the session-qualified root record, marks mappings dirty, and
persists through the existing mapping path. It refuses to overwrite a different
non-`None` owner without producing a conflict event.

- [ ] **Step 1: Write failing mapping compatibility tests**

```rust
#[test]
fn mapping_record_round_trips_owning_zed_workspace() {
    let workspace_id = workspace::WorkspaceId::from_i64(42);
    let mut record = root_mapping("alpha", "herdr-workspace");
    record.zed_workspace_id = Some(workspace_id);
    let encoded = encode_session_map(&SessionMappings::from([(
        record.key.to_key_string(),
        record.clone(),
    )]))
    .expect("encode mapping");
    let decoded = decode_session_map(Some(&encoded)).expect("decode mapping");
    assert_eq!(
        decoded.values().next().and_then(|record| record.zed_workspace_id),
        Some(workspace_id)
    );
}

#[test]
fn mapping_record_without_owner_defaults_to_none() {
    let json = r#"{"version":1,"records":{}}"#;
    let decoded = decode_session_map(Some(json)).expect("decode empty legacy map");
    assert!(decoded.is_empty());
}
```

Add an event-state test that a root record with an owner preserves that owner
when a newer snapshot restores the same `session + workspace_id` key.

- [ ] **Step 2: Run the mapping tests and confirm red**

```bash
cargo test -p agent_ui --lib mapping_record_round_trips_owning_zed_workspace -- --exact
```

Expected: compilation fails because `zed_workspace_id` is absent.

- [ ] **Step 3: Add the optional record field and update literals**

Import `workspace::WorkspaceId`, add the serde-defaulted field, and update every
`HerdrMappingRecord` literal in production and tests. Keep the serialized map
format version and canonical key encoding unchanged.

- [ ] **Step 4: Propagate owner metadata through root reconciliation**

Preserve `zed_workspace_id` when restoring existing records. Add the bridge
setter and expose the owner lookup needed by `AgentPanel::route_herdr_workspace`.

- [ ] **Step 5: Run mapping and state tests**

```bash
cargo test -p agent_ui --lib herdr_mapping_store -- --nocapture
cargo test -p agent_ui --lib herdr_state -- --nocapture
```

Expected: all mapping/state tests pass.

- [ ] **Step 6: Commit the mapping slice**

```bash
git add crates/agent_ui/src/herdr_mapping_store.rs crates/agent_ui/src/herdr_state.rs crates/agent_ui/src/herdr_bridge.rs
git commit -m "feat(agent-ui): persist Herdr workspace owners"
```

---

### Task 3: Expose complete local terminal process snapshots

**Files:**
- Modify: `crates/terminal/src/pty_info.rs:68-244`
- Modify: `crates/terminal/src/terminal.rs:667-684,2822-2832,2945-2988`
- Modify: `crates/terminal_view/src/terminal_view.rs` terminal event forwarding
- Test: `crates/terminal/src/terminal.rs`
- Test: `crates/terminal/src/pty_info.rs`

**Interfaces:**
- Add a public terminal-level snapshot type:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForegroundProcess {
    pub name: String,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    pub pid: Option<u32>,
}
```

- Add a terminal event:

```rust
pub enum Event {
    // existing variants
    ForegroundProcessChanged(Option<ForegroundProcess>),
}
```

- Add accessors:

```rust
pub fn foreground_process(&self) -> Option<ForegroundProcess>;
pub fn is_remote(&self) -> bool;
```

`foreground_process` returns `None` for display-only terminals and remote
terminals. The snapshot copies the cached `PtyProcessInfo.current` value and
converts the process ID to `u32` without exposing `sysinfo` internals.

- `PtyProcessInfo::emit_title_changed_if_changed` must compare `cwd`, `name`,
  `argv`, and PID. When any process field changes, emit
  `ForegroundProcessChanged(Some(snapshot))` in addition to the existing title
  event. Emit `ForegroundProcessChanged(None)` when the foreground process
  disappears. Keep `ProcessExited` unchanged.

- [ ] **Step 1: Write failing process snapshot/event tests**

Add pure tests for command/argv changes using the existing test process helpers:

```rust
#[test]
fn foreground_process_snapshot_includes_arguments_and_pid() {
    let snapshot = ForegroundProcess {
        name: "herdr".to_string(),
        cwd: PathBuf::from("/repo"),
        argv: vec!["/usr/local/bin/herdr".to_string(), "--session".to_string(), "zed-x".to_string()],
        pid: Some(1234),
    };
    assert_eq!(snapshot.argv[0], "/usr/local/bin/herdr");
    assert_eq!(snapshot.pid, Some(1234));
}
```

Extend the terminal process event test so changing only argv emits a process
change event. Add a remote-terminal assertion that `foreground_process()` is
`None`.

- [ ] **Step 2: Run the tests and confirm red**

```bash
cargo test -p terminal --lib foreground_process_snapshot_includes_arguments_and_pid -- --exact
```

Expected: compilation fails because the type/accessor/event is absent.

- [ ] **Step 3: Implement the snapshot and event**

Make the minimal `ProcessInfo` copy available to `Terminal`, add the public
`ForegroundProcess`, add `Event::ForegroundProcessChanged`, and update the
background process-change comparison to include argv/PID. Preserve existing
`TitleChanged` and cwd history behavior.

- [ ] **Step 4: Preserve replacement subscriptions**

Update `TerminalView::subscribe_for_terminal_events` so replacing a terminal
unsubscribes from the old entity and subscribes to the new entity's
`ForegroundProcessChanged` and `ProcessExited` events. Do not rely on the
AgentPanel-only subscription because `TerminalPanel::replace_terminal` swaps
its underlying terminal.

- [ ] **Step 5: Run terminal tests**

```bash
cargo test -p terminal --lib -- --nocapture
cargo test -p terminal_view --lib -- --nocapture
```

Expected: selected terminal and terminal-view suites pass.

- [ ] **Step 6: Commit the process-observation slice**

```bash
git add crates/terminal/src/pty_info.rs crates/terminal/src/terminal.rs crates/terminal_view/src/terminal_view.rs
git commit -m "feat(terminal): expose foreground process changes"
```

---

### Task 4: Inject a per-window named Herdr session into local terminals

**Files:**
- Modify: `crates/terminal/src/terminal.rs` or a new terminal-local environment registry
- Modify: `crates/project/src/terminals.rs:65-289,291-459`
- Modify: `crates/terminal_view/src/terminal_panel.rs:560-760,880-960,1100-1120`
- Modify: `crates/agent_ui/src/agent_panel.rs:2522-2735`
- Test: `crates/project/src/terminals.rs`
- Test: `crates/terminal_view/src/terminal_panel.rs`

**Interfaces:**
- Add a generic terminal crate registry for a window-scoped Herdr env value:

```rust
pub fn set_herdr_session_for_window(window_id: u64, session_name: String, cx: &mut App);
pub fn clear_herdr_session_for_window(window_id: u64, cx: &mut App);
pub fn herdr_session_for_window(window_id: u64, cx: &App) -> Option<String>;
```

Store it as a GPUI global in the terminal crate so `project` and
`terminal_view` do not depend on `agent_ui`.

- Add window-aware project wrappers while preserving existing callers:

```rust
pub fn create_terminal_task_in_window(
    &mut self,
    spawn_task: SpawnInTerminal,
    window_id: u64,
    cx: &mut Context<Self>,
) -> Task<Result<Entity<Terminal>>>;

pub fn create_terminal_shell_in_window(
    &mut self,
    cwd: Option<PathBuf>,
    window_id: u64,
    cx: &mut Context<Self>,
) -> Task<Result<Entity<Terminal>>>;

pub fn create_local_terminal_in_window(
    &mut self,
    window_id: u64,
    cx: &mut Context<Self>,
) -> Task<Result<Entity<Terminal>>>;
```

Existing `create_terminal_task`, `create_terminal_shell`, and
`create_local_terminal` delegate with no window overlay.

- Apply the overlay after project/settings/task environment composition and
  before `TerminalBuilder::new` only when `is_via_remote == false`:

```rust
if let Some(session_name) = terminal::herdr_session_for_window(window_id, cx) {
    env.insert("HERDR_SESSION".to_string(), session_name);
}
```

The window-scoped value wins over an inherited/project value so bare `herdr`
uses the reserved named session.

- [ ] **Step 1: Write failing env-overlay tests**

Test that a local window-aware shell receives `HERDR_SESSION`, an ordinary
window-unaware shell does not, and a remote shell never receives the overlay.
Use the existing fake filesystem/environment capture in `project/src/terminals.rs`.

- [ ] **Step 2: Run the tests and confirm red**

```bash
cargo test -p project --lib terminal -- --nocapture
```

Expected: compilation fails because the window-aware APIs and registry are absent.

- [ ] **Step 3: Implement the registry and project wrappers**

Add the terminal GPUI global, thread `Option<u64>` through the private shell
creation path, overlay only local environments, and keep existing public
wrapper behavior unchanged.

- [ ] **Step 4: Pass the actual window ID from terminal surfaces**

Update every `TerminalPanel` project creation closure to use
`window.window_handle().window_id().as_u64()`. Update `AgentPanel::spawn_terminal_with_session`
and explicit Herdr launch creation to use the same ID. Preserve remote terminal
paths and `create_local_terminal` semantics.

- [ ] **Step 5: Run env and terminal-panel tests**

```bash
cargo test -p project --lib terminals -- --nocapture
cargo test -p terminal_view --lib terminal_panel -- --nocapture
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit the environment slice**

```bash
git add crates/terminal/src/terminal.rs crates/project/src/terminals.rs crates/terminal_view/src/terminal_panel.rs crates/agent_ui/src/agent_panel.rs
git commit -m "feat(terminal): scope Herdr sessions to Zed windows"
```

---

### Task 5: Add pure Herdr launch parsing and window terminal observation

**Files:**
- Create: `crates/agent_ui/src/herdr_ownership.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs` module registration and initialization
- Modify: `crates/project/src/project.rs:335-435`
- Modify: `crates/project/src/terminals.rs` terminal creation completion paths
- Test: `crates/agent_ui/src/herdr_ownership.rs`
- Test: `crates/project/src/terminals.rs`

**Interfaces:**
- New pure parser:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HerdrLaunch {
    pub session_name: String,
    pub server_mode: bool,
}

pub(crate) fn parse_server_launch(
    argv: &[String],
    expected_session: &str,
) -> Option<HerdrLaunch>;
```

The parser normalizes the first argv element to a basename and accepts only:

```text
herdr
herdr --session <expected>
herdr server
herdr server --session <expected>
```

For bare/server forms, the expected session comes from the Zed-injected
`HERDR_SESSION`. Reject explicit sessions with a different value, all listed
CLI client subcommands (`status`, `workspace`, `tab`, `pane`, `agent`,
`notification`, `session`, `api`, `update`, `completion`), unknown subcommands,
extra wrapper commands, and empty argv. Accept `herdr.exe` after normalization.

- Add `ProjectEvent::TerminalAdded` as a unit event. Emit it after every
  `Terminal` entity is inserted into `Project::terminals.local_handles` in
  `create_terminal_task`, `create_terminal_shell_internal`, and `clone_terminal`.

- Add a window-scoped ownership observer in `agent_ui::init`:
  - observe new `MultiWorkspace` entities;
  - register each current workspace's project and existing local terminal handles;
  - subscribe to `MultiWorkspaceEvent::WorkspaceAdded/WorkspaceRemoved`;
  - on `ProjectEvent::TerminalAdded`, enumerate `local_terminal_handles`, filter
    `!terminal.is_remote()` and `terminal.is_pty()`, and subscribe once per
    terminal entity;
  - on `ForegroundProcessChanged`, call `parse_server_launch` with the window's
    reserved session name;
  - on `ProcessExited`, release only the matching owner process.

Store terminal subscriptions and owner state by runtime `WindowId`; release all
subscriptions when the `MultiWorkspace` entity releases. The observer never
creates a Herdr bridge directly; it calls the registry activation API from Task
6.

- [ ] **Step 1: Write failing parser tests**

```rust
#[test]
fn accepts_bare_herdr_for_reserved_session() {
    assert_eq!(
        parse_server_launch(&["herdr".into()], "zed-x"),
        Some(HerdrLaunch { session_name: "zed-x".into(), server_mode: false })
    );
}

#[test]
fn accepts_expected_named_server_and_rejects_other_commands() {
    assert!(parse_server_launch(
        &["herdr".into(), "server".into()],
        "zed-x"
    ).is_some());
    assert!(parse_server_launch(
        &["herdr".into(), "--session".into(), "other".into()],
        "zed-x"
    ).is_none());
    assert!(parse_server_launch(
        &["herdr".into(), "status".into()],
        "zed-x"
    ).is_none());
}
```

- [ ] **Step 2: Run parser tests and confirm red**

```bash
cargo test -p agent_ui --lib herdr_ownership::tests::accepts_bare_herdr_for_reserved_session -- --exact
```

Expected: compilation fails because the module/parser is absent.

- [ ] **Step 3: Implement parser and process-event project seam**

Create the module, register it from `agent_ui.rs`, add the exact parser rules,
add `ProjectEvent::TerminalAdded`, and emit it at all three terminal entity
creation sites. Do not add Herdr-specific dependencies to `project`.

- [ ] **Step 4: Implement observer registration and cleanup**

Register existing project terminals when a window observer starts, subscribe to
new terminal events, deduplicate subscriptions by entity ID, and remove the
subscription on terminal release. Filter remote/display-only terminals before
parsing. Keep owner-process state per window and send only accepted launches to
the bridge registry.

- [ ] **Step 5: Test process surface coverage**

Add tests for:

```text
standard local TerminalPanel terminal -> accepted
AgentPanel terminal -> accepted
remote terminal -> ignored
herdr status -> ignored
herdr --session other -> ignored
terminal replacement -> old terminal ignored, new terminal observed
owner ProcessExited -> release event emitted once
```

Run:

```bash
cargo test -p agent_ui --lib herdr_ownership -- --nocapture
cargo test -p project --lib terminals -- --nocapture
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit the parser/observer slice**

```bash
git add crates/agent_ui/src/herdr_ownership.rs crates/agent_ui/src/agent_ui.rs crates/project/src/project.rs crates/project/src/terminals.rs
git commit -m "feat(agent-ui): detect Herdr launches in local terminals"
```

---

### Task 6: Make Herdr bridge activation lazy and owner-process gated

**Files:**
- Modify: `crates/agent_ui/src/herdr_bridge.rs:172-268,1939-2250,2440-2550`
- Modify: `crates/agent_ui/src/agent_panel.rs:1926-2132,5597-5733`
- Modify: `crates/agent_ui/src/agent_ui.rs` window observer callback integration
- Test: `crates/agent_ui/src/herdr_bridge.rs`

**Interfaces:**
- Add runtime owner state:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HerdrOwnerProcess {
    pub terminal_id: gpui::EntityId,
    pub process_id: Option<u32>,
    pub session_name: String,
}
```

- Add lazy registry methods:

```rust
pub(crate) fn activate_window(
    &mut self,
    window_id: WindowId,
    selection: HerdrSessionSelection,
    owner: HerdrOwnerProcess,
    cx: &mut App,
) -> Result<Entity<HerdrThreadBridge>>;

pub(crate) fn release_owner_process(
    &mut self,
    window_id: WindowId,
    terminal_id: gpui::EntityId,
    process_id: Option<u32>,
    cx: &mut App,
);

pub(crate) fn release_window(&mut self, window_id: WindowId, cx: &mut App);
```

`activate_window` loads the named session mappings, creates the client and
bridge, stores the owner, and starts `begin_sync`. Repeated activation for the
same window/session reuses the active bridge. A different owner process cannot
replace an active owner; return a deterministic conflict.

`AgentPanel::new` must stop calling `for_window(window_id, Default)` and must
not call `begin_sync`. It attaches only to an already-active bridge returned by
`bridge_for_window`. The window observer attaches an activated bridge to loaded
AgentPanels in that `MultiWorkspace`; panels loaded later attach during
construction.

- Add owner gating to `HerdrThreadBridge`:

```rust
pub(crate) fn set_owner(&mut self, owner: HerdrOwnerProcess);
pub(crate) fn clear_owner(&mut self) -> Option<HerdrOwnerProcess>;
pub(crate) fn owner(&self) -> Option<&HerdrOwnerProcess>;
```

`start_sync` retries only while `owner.is_some()` or the initial in-Zed launch
claim is pending. `stop` clears the owner and cancels all subscriptions. Process
exit must stop the bridge and set `Dormant` without deleting persisted mappings.

- Keep `HerdrSessionSelection::Named` for owned bridges. Remove all implicit
  Default activation from AgentPanel construction. Restrict the existing
  `ConnectHerdrSession` rebind path so it cannot attach an arbitrary external
  default session; the explicit Open Herdr action supplies the persisted name.

- [ ] **Step 1: Write failing lazy-activation and owner-gate tests**

Add tests that construct a panel/window without a trigger and assert no Herdr
bridge client/retry starts. Add an owner-gate test that starts a failing
bootstrap, releases the owner process, pumps the executor, and asserts no later
bootstrap request is attempted. Add a duplicate-owner test that rejects a
second terminal/process for the same window.

Initialize `HerdrBridgeRegistry` in the existing GPUI workspace fixture before
constructing `AgentPanel`, then construct the panel through
`AgentPanel::new`. Keep the fixture's `MultiWorkspace` handle and
`VisualTestContext` so the test can inspect the window-scoped registry:

```rust
let window_id = visual_cx.update(|window, _cx| window.window_handle().window_id());
let bridge = visual_cx.update(|_, cx| {
    cx.global::<HerdrBridgeRegistry>()
        .bridge_for_window(window_id, cx)
});
assert!(
    bridge.is_none(),
    "constructing an AgentPanel must not create an active Herdr bridge"
);
```

- [ ] **Step 2: Run the tests and confirm red**

```bash
cargo test -p agent_ui --lib agent_panel_creation_does_not_start_herdr_sync -- --exact
```

Expected: the existing eager bridge makes the assertion fail.

- [ ] **Step 3: Implement lazy registry activation**

Move client/mapping creation out of the AgentPanel constructor and into
`activate_window`. Keep per-window bridge lookup and session-qualified mapping
loading. Add runtime owner storage and deterministic duplicate-owner handling.

- [ ] **Step 4: Gate retry and stop on process exit**

Thread an owner snapshot into `start_sync`. Check the owner/cancellation state
before each bootstrap, before each backoff wait, and before setting
`Reconnecting`. On owner exit, cancel subscriptions, clear runtime owner, and
return without starting a new retry cycle. Preserve the already-persisted root
and subthread state.

- [ ] **Step 5: Remove automatic AgentPanel binding and attach active bridges**

Make panel construction side-effect-free with respect to Herdr. Add the helper
that attaches an active bridge to every loaded AgentPanel in the owning window,
then subscribe those panels to bridge events exactly once. Ensure releasing one
panel does not stop a bridge while the window owner process remains active;
window release stops the bridge.

- [ ] **Step 6: Run bridge and panel tests**

```bash
cargo test -p agent_ui --lib herdr_bridge -- --nocapture
cargo test -p agent_ui --lib agent_panel::tests::herdr -- --nocapture
```

Expected: all selected tests pass, including lazy activation and owner exit.

- [ ] **Step 7: Commit the lazy lifecycle slice**

```bash
git add crates/agent_ui/src/herdr_bridge.rs crates/agent_ui/src/agent_panel.rs crates/agent_ui/src/agent_ui.rs
git commit -m "feat(agent-ui): gate Herdr sync by process ownership"
```

---

### Task 7: Add explicit Open Herdr action and deterministic workspace routing

**Files:**
- Modify: `crates/agent_ui/src/agent_ui.rs:231-350`
- Modify: `crates/agent_ui/src/agent_panel.rs:518-924,2522-2735,5435-5565`
- Modify: `crates/workspace/src/multi_workspace.rs` workspace lookup accessors
- Modify: `crates/agent_ui/src/herdr_bridge.rs` launch-context owner assignment
- Test: `crates/agent_ui/src/agent_panel.rs`
- Test: `crates/sidebar/src/sidebar_tests.rs`

**Interfaces:**
- Add the agent action next to `NewTerminalThread`:

```rust
/// Launches Herdr in this Zed window's dedicated named session.
OpenHerdr,
```

- Register the action from the existing `AgentPanel::init` workspace observer.
The action must:
  1. require a local project/worktree;
  2. obtain `MultiWorkspace::reserve_herdr_session_name`;
  3. persist the session reservation before spawning;
  4. set the terminal crate window env overlay;
  5. launch `herdr --session <name>` using `SpawnInTerminal` with argv and env,
     not shell-concatenated untrusted text;
  6. call `HerdrBridgeRegistry::activate_window` with a pending owner tied to
     the created terminal;
  7. focus the created terminal without changing unrelated workspace selection.

- Add an AgentPanel helper:

```rust
pub fn open_herdr(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool;
```

Return `false` when there is no local project or an owner conflict. Show one
specific action error for those cases; do not start an ambient Default bridge.

- Update `route_herdr_workspace`:
  1. look up `bridge.root_zed_workspace_id(herdr_workspace_id)`;
  2. resolve that ID through a new `MultiWorkspace` accessor;
  3. if absent, compare canonical `ProjectGroupKey`/path identity and require
     exactly one match;
  4. on zero matches, leave the root disconnected;
  5. on multiple matches, emit a mapping conflict and do not activate either;
  6. when a root is created from the explicit action/current workspace, call
     `set_root_zed_workspace_id` before publishing the root event.

- Add `MultiWorkspace::workspace_for_database_id(WorkspaceId) -> Option<Entity<Workspace>>`
  as a read-only lookup over `workspaces()`.

- Remove or restrict the old free-form session editor/rebind behavior so an
  arbitrary named external session cannot be silently attached. If retained
  for diagnostics, it must only rebind the current persisted Zed-owned name.

- [ ] **Step 1: Write failing action and routing tests**

Add a GPUI test that dispatches `OpenHerdr` and asserts the generated command
contains the persisted name and the terminal env contains the same name. Add
routing tests for:

```text
persisted WorkspaceId -> exact owner
one canonical path match -> owner populated
zero matches -> disconnected root
multiple matches -> conflict and no activation
```

- [ ] **Step 2: Run tests and confirm red**

```bash
cargo test -p agent_ui --lib open_herdr -- --exact
cargo test -p sidebar --lib herdr_workspace_routing -- --nocapture
```

Expected: compilation fails because `OpenHerdr`, owner routing, and lookup APIs
are absent.

- [ ] **Step 3: Implement action and command launch**

Add `OpenHerdr`, register it, reserve/flush the named session, build a
`SpawnInTerminal` with `command: Some("herdr".into())`, `args:
vec!["--session".into(), session_name.clone()]`, and
`env.insert("HERDR_SESSION".into(), session_name)`. Use the existing
`Project::create_terminal_task_in_window` path so the terminal is tracked by
the generic observer.

- [ ] **Step 4: Implement owner-aware routing**

Add the `WorkspaceId` lookup and update root creation/activation/routing as
specified. Preserve lazy owner-panel loading and event queue behavior after the
owner workspace is selected. Replace first-match path selection with exact-one
match semantics and explicit conflict handling.

- [ ] **Step 5: Add terminal-trigger activation coverage**

Use a fake `ForegroundProcessChanged` event for a standard local terminal and
an AgentPanel terminal. Assert both activate the same per-window bridge. Assert
`herdr status`, mismatched session, remote terminal, and external-process paths
do not activate it.

- [ ] **Step 6: Run action, routing, and Herdr UI tests**

```bash
cargo test -p agent_ui --lib open_herdr -- --nocapture
cargo test -p agent_ui --lib herdr_bridge -- --nocapture
cargo test -p sidebar --lib herdr -- --nocapture
```

Expected: selected tests pass and no default bridge starts in unrelated panels.

- [ ] **Step 7: Commit the action/routing slice**

```bash
git add crates/agent_ui/src/agent_ui.rs crates/agent_ui/src/agent_panel.rs crates/workspace/src/multi_workspace.rs crates/agent_ui/src/herdr_bridge.rs crates/sidebar/src/sidebar_tests.rs
git commit -m "feat(agent-ui): launch and route owned Herdr sessions"
```

---

### Task 8: Add restart, shutdown, and external-process regression coverage

**Files:**
- Test: `crates/workspace/src/persistence.rs`
- Test: `crates/agent_ui/src/herdr_bridge.rs`
- Test: `crates/agent_ui/src/agent_panel.rs`
- Test: `crates/sidebar/src/sidebar_tests.rs`
- Test: `crates/terminal/src/terminal.rs`

**Interfaces:**
- Tests must exercise the public behavior, not private map layout:
  - `MultiWorkspaceState` serde/restoration;
  - `HerdrBridgeRegistry::activate_window/release_owner_process/release_window`;
  - `HerdrThreadBridge` status and event subscriptions;
  - `AgentPanel::open_herdr`;
  - terminal `ForegroundProcessChanged` and `ProcessExited` events;
  - sidebar visible root/thread selection.

- [ ] **Step 1: Add restart persistence test**

Create a state with `herdr_session_name: Some("zed-stable")` and
`herdr_owned: true`, restore it through `read_serialized_multi_workspaces`,
construct a new runtime window, and assert the restored window keeps the same
name while its bridge remains inactive until a new launch trigger.

- [ ] **Step 2: Add late-start reconnect test**

Start an owned bridge against a fake endpoint that initially rejects bootstrap.
Advance the test scheduler without producing a user Toast event, then make the
fake server accept the named endpoint. Assert the bridge reaches `Ready`, the
persisted root `ThreadId` is reused, and the snapshot is applied.

- [ ] **Step 3: Add owner-exit/external-restart test**

Activate a bridge from terminal identity A, emit `ProcessExited` for A, and
assert the bridge is dormant and no further bootstrap retries occur. Start a
fake Herdr endpoint from a terminal not observed by the Zed window and assert no
bridge activation or sync event occurs.

- [ ] **Step 4: Add Zed-close/Herdr-retention test**

Release the owning `MultiWorkspace`/window and assert the bridge stops and
subscriptions are removed without sending `workspace.close` or deleting the
named-session mapping. Restore the state and require a new in-window launch
before activation.

- [ ] **Step 5: Add duplicate-owner and CLI filtering tests**

Assert a second accepted server process in the same window cannot replace the
first owner, and all CLI subcommands listed in the specification remain
unbound.

- [ ] **Step 6: Run the complete affected test suites**

```bash
cargo test -p terminal --lib
cargo test -p project --lib
cargo test -p workspace --lib
cargo test -p agent_ui --lib
cargo test -p sidebar --lib
```

Expected: all tests pass with no cross-test scheduler activity or leaked owner
subscriptions.

- [ ] **Step 7: Commit regression coverage**

```bash
git add crates/terminal/src/terminal.rs crates/project/src/terminals.rs crates/workspace/src/persistence.rs crates/agent_ui/src/herdr_bridge.rs crates/agent_ui/src/agent_panel.rs crates/sidebar/src/sidebar_tests.rs
git commit -m "test: cover owned Herdr lifecycle recovery"
```

---

## Final Verification

After all implementation slices are committed:

- [ ] Run the exact fake-server late-start smoke scenario:

```bash
cargo test -p agent_ui --lib herdr_test_support -- --nocapture
cargo test -p agent_ui --lib herdr_bridge -- --nocapture
```

Expected: Unix transport/fixture and bridge suites pass, including delayed
endpoint availability and owner-process exit behavior.

- [ ] Run all affected UI suites:

```bash
cargo test -p agent_ui --lib
cargo test -p sidebar --lib
cargo test -p workspace --lib
```

Expected: zero failures, zero cross-test scheduler activity, and no leaked
handles.

- [ ] Run release compilation for desktop binary targets:

```bash
CARGO_BUILD_JOBS=2 cargo check --release \
  --package zed \
  --package cli \
  --package auto_update_helper \
  --package explorer_command_injector \
  --package remote_server
```

Expected: exit code 0 on the host; Windows named-pipe compilation remains a
Windows CI responsibility.

- [ ] Run the repository-required lint command:

```bash
./script/clippy
```

Expected: no new diagnostics from the Herdr lifecycle changes. If an existing
unrelated warning still blocks the workspace, record its exact file and line
without weakening lint policy.

- [ ] Run the manual lifecycle smoke flow:
  1. Start Zed without Herdr; confirm no Herdr bridge retry or Toast.
  2. Use `Open Herdr`; confirm `herdr --session <zed-name>` starts in Zed and
     the bridge reaches `Ready`.
  3. Close Herdr and confirm retries stop after the owner process exits.
  4. Start `herdr` in a separate terminal; confirm the dormant Zed window does
     not sync it.
  5. Close Zed; confirm Herdr remains running.
  6. Restart Zed; confirm the named session/mappings restore but remain dormant.
  7. Run `herdr` again inside Zed; confirm the existing root ThreadIds and
     workspace mappings return.
  8. Switch Herdr spaces/agents and Zed roots/subthreads in both directions.
  9. Run `herdr status` and `herdr workspace list` inside Zed; confirm these
     commands do not activate or replace ownership.

## Plan Self-Review

- **Spec coverage:** Persistence, explicit/direct launch, all-local terminal
  observation, named session injection, restart behavior, external-process
  isolation, process-exit gating, Herdr retention after Zed close, deterministic
  WorkspaceId/path routing, CLI filtering, errors, tests, and platform checks
  each have a task above.
- **Placeholder scan:** No task depends on `TODO`, `TBD`, a future follow-up, or
  an unspecified error-handling step. Every acceptance command names a concrete
  package/test or manual scenario.
- **Type consistency:** `HerdrOwnerProcess`, `HerdrLaunch`, the
  `MultiWorkspace` state methods, terminal window-aware project methods, mapping
  owner field, and registry lifecycle methods are defined before later tasks
  consume them.
- **Known implementation seam:** The current terminal API exposes only a
  normalized process command and no terminal-added event. Tasks 3–5 explicitly
  add the missing process snapshot, argv-change event, and project terminal
  registration needed for all local terminal surfaces.
