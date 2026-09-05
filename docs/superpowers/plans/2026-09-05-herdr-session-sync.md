# herdr Session Selection and Agent Synchronization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace automatic herdr startup with explicit session selection and make selected herdr sessions synchronize worktrees, attached agent terminal threads, and focus across Zed windows on Windows, macOS, and Linux.

**Architecture:** Add authoritative CLI session discovery and typed workspace/pane events to the `herdr` crate. Keep one application-wide Zed registry connection per selected session, bind individual Zed windows to that registry, and reduce agent events by stable `(session, terminal_id)` identity before applying GPUI effects. Mirror each live herdr agent through an Agent Panel terminal attached to its current pane; keep the existing central herdr view as a session-parameterized presentation adapter rather than a connection owner.

**Tech Stack:** Rust, GPUI entities and globals, `Picker`/`ModalView`, Zed `MultiWorkspace` and Agent Panel, local-socket JSON-RPC, structured `SpawnInTerminal`, deterministic Rust/GPUI tests, native Windows smoke verification.

## Global Constraints

- Every user-visible spelling is exactly `herdr`; internal Rust type names may use conventional casing.
- A Zed window binds to a session only after picker confirmation or causal inheritance from a previously confirmed session.
- Session endpoint paths come from `herdr session list --json`; the UI never guesses `.config/herdr`, a default session, or an environment-selected session.
- The application registry owns at most one client connection and event stream per herdr session, shared by all bound windows.
- A herdr space event alone never opens or repurposes a Zed window; a detected running agent with a valid worktree is the opening trigger.
- Mirrored agent identity is `(session identity, terminal_id)`. Store the latest `pane_id` separately because pane moves do not duplicate threads and future attach/focus calls use the current pane.
- Closing a mirrored Zed terminal detaches only that client, keeps the herdr agent alive, and suppresses automatic reopening until resync or terminal exit.
- A normal herdr exit leaves Zed windows, worktrees, editors, and terminal output intact, moves bindings to `Unselected`, and starts no reconnect loop.
- Use structured executable/argument vectors for every herdr process; never interpolate session names or pane IDs into shell command strings.
- Startup connection attempts are bounded to 15 seconds. A lost stream performs one session-list refresh and then becomes `Unselected` or `Failed`; it never reconnects automatically.
- Preserve the existing persistent central-view layout and editor-entity lifetime behavior.
- Do not add ACP import/resume, agent termination propagation, space creation propagation, or unrelated Agent Panel refactors.
- Before changing or removing exported actions or client constructors, use LSP references and migrate every caller in the same task.
- herdr 0.8.2 does not support direct terminal attach on Windows: `run_terminal_attach` is a Windows stub returning `Unsupported` with `direct terminal attach is not supported on Windows yet` (also on `master`), and the official docs list `Direct terminal attach (herdr terminal attach)` as unsupported on Windows. The mirror command `herdr --session <name> agent attach <pane-id>` therefore fails for every agent on Windows 0.8.2. The plan keeps the mirror mechanism because it is the correct Unix path (and the future Windows path once herdr ships a semantic attach), and the per-revision failure gate turns the Windows outcome into one actionable notification per agent revision with continued synchronization and no re-spawn loop. Everything else in this plan — session selection, the application registry, shared connections, exit/disconnect liveness, focus, and branding — is platform-neutral. The Windows smoke asserts the graceful degradation; live-attach verification is gated on a herdr build whose capability enables it.

## File and responsibility map

- `crates/herdr/src/herdr.rs` — CLI session catalog, authoritative endpoints, typed snapshot/pane data, typed event stream, `pane.get`, workspace focus, and agent focus.
- `crates/herdr/Cargo.toml` — remove `dirs` after guessed session paths are deleted; add no dependency.
- `crates/agent_ui/src/agent_panel.rs` — public external-terminal thread input, stable external identity index, structured task terminal creation, activation, close detection, and non-persistence.
- `crates/agent_ui/src/agent_ui.rs` — re-export the external-terminal input alongside `AgentPanel` and `TerminalId`.
- `crates/zed/src/zed/herdr_agent_sync.rs` — pure, GPUI-free reducer for stable terminal identity, pane revisions/moves, dismissal, resync, and focus-echo suppression.
- `crates/zed/src/zed/herdr_session_registry.rs` — GPUI global registry, per-session client tasks, per-window bindings, startup/liveness state machine, worktree routing, Agent Panel mappings, and focus synchronization.
- `crates/zed/src/zed/herdr_session_picker.rs` — running/stopped/new session picker and confirmed new-name mode.
- `crates/zed/src/zed/herdr_host.rs` — central-view and status adapter for a selected session; no endpoint ownership, API snapshot, focus reducer, or reconnect loop.
- `crates/zed/src/zed.rs` — module declarations, registry initialization, action registration, startup prompt observation, status item registration, and Agent Panel observation.
- `crates/zed/src/main.rs` — restoration hook that restores only view state and lets the registry prompt or inherit safely.
- `crates/zed_actions/src/lib.rs` — clean herdr action surface and lowercase action labels.
- `docs/superpowers/specs/2026-09-02-herdr-persistent-central-view-design.md` and `docs/superpowers/plans/2026-09-02-herdr-persistent-central-view.md` — lowercase naming and explicit supersession notes for old session-ownership rules.
- `docs/superpowers/specs/2026-09-05-herdr-session-sync-design.md` — approved behavioral source of truth; only update if implementation evidence exposes a factual correction.

---

### Task 1: Add authoritative herdr session and event APIs

**Files:**
- Modify: `crates/herdr/src/herdr.rs:1-356,757-871`
- Test: `crates/herdr/src/herdr.rs:774-871`

**Interfaces:**
- Produces `SessionInfo`, `SessionList`, `AgentSessionInfo`, `AgentSessionKind`, `PaneInfo`, `AgentInfo`, `HerdrEvent`, `WorkspaceEvent`, `PaneEvent`, and `PaneEventKind`.
- Produces `pub async fn list_sessions(program: PathBuf) -> Result<Vec<SessionInfo>>`.
- Produces `pub async fn validate_session_name(program: PathBuf, name: String) -> Result<()>`, using the CLI's own parser without starting a session.
- Extends `HerdRClient` with `pane`, `focus_agent`, and a typed `subscribe` stream.
- Retains `Endpoint::session`, `Endpoint::from_environment`, and `ClientConfig::default` temporarily so the current Zed caller still compiles; Task 4 removes them after migration.

- [ ] **Step 1: Add failing session-catalog and typed-wire tests**

Add tests with real herdr 0.8.2 JSON shapes:

```rust
#[test]
fn parses_cli_session_list_with_reported_windows_socket() {
    let list: SessionList = serde_json::from_str(
        r#"{"sessions":[{"name":"main","default":false,"running":true,"session_dir":"C:\\Users\\me\\AppData\\Roaming\\herdr\\sessions\\main","socket_path":"C:\\Users\\me\\AppData\\Roaming\\herdr\\sessions\\main\\herdr.sock"}]}"#,
    )
    .expect("session list");

    let session = &list.sessions[0];
    assert_eq!(session.name, "main");
    assert!(session.running);
    assert_eq!(
        session.endpoint(),
        Endpoint::Filesystem(PathBuf::from(
            r"C:\Users\me\AppData\Roaming\herdr\sessions\main\herdr.sock"
        ))
    );
}

#[test]
fn parses_pane_identity_and_revision() {
    let pane: PaneInfo = serde_json::from_value(serde_json::json!({
        "workspace_id": "workspace-1",
        "tab_id": "tab-1",
        "pane_id": "pane-7",
        "terminal_id": "terminal-stable",
        "focused": true,
        "revision": 9,
        "agent": "claude",
        "agent_status": "working",
        "cwd": "/repo/worktree",
        "foreground_cwd": "/repo/worktree/src",
        "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
    }))
    .expect("pane");

    assert_eq!(pane.terminal_id, "terminal-stable");
    assert_eq!(pane.pane_id, "pane-7");
    assert_eq!(pane.revision, 9);
}
```

Add event parser cases for all subscribed names against the real 0.8.2 envelope (`event` is the snake_case `EventKind`, `data` is the tagged `EventData`). The pane-move case carries `previous_*` ids plus a full nested `pane`; the test must assert both identities:

```rust
let Some(HerdrEvent::Pane(PaneEvent {
    kind: PaneEventKind::Moved {
        previous_pane_id,
        pane,
    },
})) = parse_event(serde_json::json!({
    "event": "pane_moved",
    "data": {
        "previous_pane_id": "pane-old",
        "previous_workspace_id": "workspace-1",
        "previous_tab_id": "tab-1",
        "pane": {
            "workspace_id": "workspace-2",
            "tab_id": "tab-2",
            "pane_id": "pane-new",
            "terminal_id": "terminal-stable",
            "focused": true,
            "revision": 10,
            "agent": "claude",
            "agent_status": "working",
            "cwd": "/repo/worktree",
            "foreground_cwd": "/repo/worktree/src",
            "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
        }
    }
}))
else {
    panic!("expected a moved pane event");
};
assert_eq!(previous_pane_id, "pane-old");
assert_eq!(pane.pane_id, "pane-new");
assert_eq!(pane.terminal_id, "terminal-stable");
assert_eq!(pane.revision, 10);
assert_eq!(pane.workspace_id, "workspace-2");
```

Add one `pane.get` response test using the real result envelope `{"type": "pane_info", "pane": {…}}` to pin the client-side unwrap.

- [ ] **Step 2: Run the focused tests and confirm red output**

Run:

```bash
cargo test -p herdr session_list
cargo test -p herdr pane_identity
cargo test -p herdr herdr_event
```

Before implementation, the commands fail because the catalog, pane types, and event parser do not exist.

- [ ] **Step 3: Add catalog and typed protocol models**

Define the catalog and pane models with exact serde field mapping:

```rust
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionList {
    #[serde(default)]
    pub sessions: Vec<SessionInfo>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionInfo {
    pub name: String,
    #[serde(rename = "default")]
    pub is_default: bool,
    pub running: bool,
    pub session_dir: PathBuf,
    pub socket_path: PathBuf,
}

impl SessionInfo {
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::Filesystem(self.socket_path.clone())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AgentSessionKind {
    Id,
    Path,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AgentSessionInfo {
    pub agent: String,
    pub kind: AgentSessionKind,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct PaneInfo {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub focused: bool,
    pub revision: u64,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub agent_status: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub foreground_cwd: Option<String>,
    #[serde(default)]
    pub agent_session: Option<AgentSessionInfo>,
}

pub type AgentInfo = PaneInfo;
```

Define the typed event envelope mirroring `EventEnvelope` + `EventData`. `PaneEventKind` carries full pane payloads where 0.8.2 nests them:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum HerdrEvent {
    Workspace(WorkspaceEvent),
    Pane(PaneEvent),
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkspaceEvent {
    Created(WorkspaceInfo),
    Updated(WorkspaceInfo),
    Closed { workspace_id: String },
    Focused { workspace_id: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PaneEvent {
    pub kind: PaneEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PaneEventKind {
    Created(PaneInfo),
    Updated(PaneInfo),
    Closed { pane_id: String, workspace_id: String },
    Focused { pane_id: String, workspace_id: String },
    Moved { previous_pane_id: String, pane: PaneInfo },
    Exited { pane_id: String, workspace_id: String },
    AgentDetected { pane_id: String, workspace_id: String },
}
```

Parse by matching the snake_case `event` field and deserializing `data` per kind. `pane.created`/`pane.updated`/`pane.moved` always carry the full `PaneInfo`; closed/exited/focused/agent_detected carry only ids. Skip unknown event kinds. The parser runs on the subscription stream, which uses the same envelope.

Replace `SessionSnapshot.panes: Vec<Value>` and `SessionSnapshot.agents: Vec<Value>` with `Vec<PaneInfo>` and `Vec<AgentInfo>`. Keep tabs/layouts untyped because this feature does not consume them.

- [ ] **Step 4: Implement CLI session discovery**

Run the CLI off the async executor and preserve stdout/stderr separately:

```rust
pub async fn list_sessions(program: PathBuf) -> Result<Vec<SessionInfo>> {
    smol::unblock(move || {
        let output = std::process::Command::new(program)
            .args(["session", "list", "--json"])
            .output()?;
        if !output.status.success() {
            return Err(Error::SessionListCommand {
                message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        let list: SessionList = serde_json::from_slice(&output.stdout)?;
        Ok(list.sessions)
    })
    .await
}

Implement `validate_session_name` with the same command runner but structured arguments `["--session", name, "session", "list", "--json"]`. `herdr` 0.8.2 validates `--session` in `src/session.rs::configure_from_args` (`apply_explicit_name` → `normalize_name`, max 64 bytes, ASCII letters/digits/`.`/`_`/`-`, not `.`/`..`) before the side-effect-free list subcommand runs, so a rejected name yields one session-specific stderr line with exit 2 and creates no session directory. Zed therefore neither copies a version-sensitive name grammar nor starts a partial session. A non-zero exit becomes `Error::SessionNameRejected { message }`; preserve trimmed stderr and use `herdr rejected the session name` when it is empty.
```

Add `Error::SessionListCommand { message: String }` and `Error::SessionNameRejected { message: String }` with lowercase herdr messages. Empty session-list stderr becomes `herdr session list exited unsuccessfully` so the picker never shows a blank failure.

- [ ] **Step 5: Add typed requests and the multi-event subscription**

Add these client methods:

```rust
pub async fn pane(&self, pane_id: impl Into<String>) -> Result<PaneInfo>;
pub async fn focus_agent(&self, pane_id: impl Into<String>) -> Result<()>;
pub async fn subscribe(&self) -> Result<SubscribeStream>;
```

Their synchronous requests are:

```rust
self.request_sync("pane.get", serde_json::json!({"pane_id": pane_id}))?;
self.request_sync("agent.focus", serde_json::json!({"target": pane_id}))?;
```

Subscribe once with these exact types, serialized as tagged `{"type": "…"}` values:

```rust
const EVENT_TYPES: &[&str] = &[
    "workspace.created",
    "workspace.updated",
    "workspace.closed",
    "workspace.focused",
    "pane.created",
    "pane.updated",
    "pane.closed",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
];
```

Do not include `pane.agent_status_changed`: in 0.8.2 that subscription requires `pane_id` (`Subscription::PaneAgentStatusChanged { pane_id, agent_status }`), so an unscoped entry fails the whole `events.subscribe` request and the session event stream never starts. Status is not consumed by any mirror decision, so it stays out of the fixed subscription. If status tracking is ever needed, add per-pane scoped subscriptions after detection.

Change `SubscribeStream::next` to `Result<Option<HerdrEvent>>` and `pane()` to unwrap the tagged result: `pane.get` returns `{"type": "pane_info", "pane": {…}}`, so extract `result.pane` before deserializing (`Error::MissingResult` when absent). For `pane.agent_detected`, return the reference event and let the registry call `client.pane`; do not fabricate missing full pane fields. The stream ack is `{"result": {"type": "subscription_started"}}`; subsequent frames are `{"event": "<snake_case>", "data": …}`.

- [ ] **Step 6: Run the herdr crate tests**

Run:

```bash
cargo test -p herdr
```

Expected: all tests pass, including the existing frame-limit, named-pipe, canonical-path, and generation tests.

- [ ] **Step 7: Commit the protocol slice**

```bash
git add crates/herdr/src/herdr.rs
git commit -m "feat(herdr): expose sessions and agent events"
```

---

### Task 2: Add Agent Panel external terminal threads

**Files:**
- Modify: `crates/agent_ui/src/agent_panel.rs:969-1275,1997-2351,3363-3372,6880-7790`
- Modify: `crates/agent_ui/src/agent_ui.rs:71-76`
- Test: `crates/agent_ui/src/agent_panel.rs` filtered by `external_terminal_thread`

**Interfaces:**
- Consumes `Project::create_terminal_task(SpawnInTerminal, cx)`.
- Produces `ExternalTerminalThread { identity, executable, args, title, working_directory }`.
- Produces `AgentPanel::open_external_terminal_thread(spec, focus, window, cx) -> TerminalId`.
- Produces `AgentPanel::{external_terminal_identity, close_external_terminal_thread}` for lookup and explicit local detach.
- Existing shell terminal creation and persistence behavior remain unchanged.

- [ ] **Step 1: Add failing deduplication and persistence tests**

Use `setup_panel` and an executable that exists in the test process:

```rust
fn test_external_terminal(identity: &str) -> ExternalTerminalThread {
    ExternalTerminalThread {
        identity: identity.into(),
        executable: std::env::current_exe()
            .expect("test executable")
            .to_string_lossy()
            .into_owned(),
        args: vec!["--help".into()],
        title: "herdr · claude".into(),
        working_directory: std::env::temp_dir(),
    }
}

#[gpui::test]
async fn test_external_terminal_thread_reuses_stable_identity(cx: &mut TestAppContext) {
    let (panel, mut cx) = setup_panel(cx).await;
    let (first, second) = panel.update_in(&mut cx, |panel, window, cx| {
        let first = panel.open_external_terminal_thread(
            test_external_terminal("session-a:terminal-1"),
            true,
            window,
            cx,
        );
        let second = panel.open_external_terminal_thread(
            test_external_terminal("session-a:terminal-1"),
            true,
            window,
            cx,
        );
        (first, second)
    });
    assert_eq!(first, second);
}
```

Add a second test that serializes and reloads while the external terminal is active, then asserts the reloaded panel has no terminal with that external identity and does not reactivate the original terminal ID. Add a third test that calls `close_external_terminal_thread`, asserts it returns `true`, and confirms `external_terminal_identity(first).is_none()` without invoking any remote-agent API. Add a fourth race test: open, close immediately before the spawn settles, run the executor until the spawn completes, and assert `external_terminal_identity` stays `None` and the closed terminal is not resurrected.

- [ ] **Step 2: Run the focused test and confirm red output**

Run:

```bash
cargo test -p agent_ui --lib external_terminal_thread
```

Expected: compilation fails because `ExternalTerminalThread` and the new methods do not exist.

- [ ] **Step 3: Add the external-terminal input and identity index**

Add and re-export:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalTerminalThread {
    pub identity: SharedString,
    pub executable: String,
    pub args: Vec<String>,
    pub title: SharedString,
    pub working_directory: PathBuf,
}
```

Extend `AgentTerminal` with `persistent: bool` and `external_identity: Option<SharedString>`. Extend `AgentPanel` with `external_terminals: HashMap<SharedString, TerminalId>`.

Keep ordinary and restored terminal construction at `persistent: true`. External terminals use `persistent: false`. Filter a non-persistent active terminal out of `last_active_terminal_id`; make `persist_terminal_metadata` itself return before writing any non-persistent terminal; remove the external index entry in `close_terminal_internal`.

- [ ] **Step 4: Spawn the external terminal from structured arguments**

Implement the public entry point so identity is reserved before async spawn and repeated calls activate the existing terminal:

```rust
pub fn open_external_terminal_thread(
    &mut self,
    spec: ExternalTerminalThread,
    focus: bool,
    window: &mut Window,
    cx: &mut Context<Self>,
) -> TerminalId {
    if let Some(terminal_id) = self.external_terminals.get(&spec.identity).copied() {
        self.activate_terminal(terminal_id, focus, window, cx);
        return terminal_id;
    }

    let terminal_id = TerminalId::new();
    self.external_terminals
        .insert(spec.identity.clone(), terminal_id);
    self.spawn_external_terminal(terminal_id, spec, focus, window, cx);
    terminal_id
}
```

`spawn_external_terminal` calls `Project::create_terminal_task` with:

```rust
SpawnInTerminal {
    id: TaskId(format!("agent-panel-external:{}", spec.identity)),
    full_label: spec.title.to_string(),
    label: spec.title.to_string(),
    command: Some(spec.executable),
    args: spec.args,
    command_label: spec.title.to_string(),
    cwd: Some(spec.working_directory.clone()),
    use_new_terminal: true,
    allow_concurrent_runs: true,
    reveal: RevealStrategy::Never,
    reveal_target: RevealTarget::Dock,
    shell: Shell::System,
    show_command: false,
    show_summary: false,
    ..Default::default()
}
```

Extract the shared `TerminalView::new` and `insert_terminal` completion path instead of duplicating subscriptions. Pass persistence and external identity explicitly into `insert_terminal`.

The shared completion path is the single outcome channel for the registry. Emit one generic `AgentPanelEvent::ExternalTerminalSpawnFinished { identity: SharedString, message: Option<SharedString> }` per attempt (`None` = success, `Some` = spawn error). On success, insert the terminal only if `external_terminals` still maps the identity to this `TerminalId`; a close that raced the async spawn removed the entry, so the completion must not resurrect the terminal. On failure, emit the event and remove the reserved identity before showing the existing workspace error.

- [ ] **Step 5: Expose lookup, activation, explicit local detach, and spawn completion**

Implement:

```rust
pub fn external_terminal_identity(&self, terminal_id: TerminalId) -> Option<&str> {
    self.terminals
        .get(&terminal_id)
        .and_then(|terminal| terminal.external_identity.as_deref())
}
```

Add `close_external_terminal_thread(identity, window, cx) -> bool`. It resolves the indexed `TerminalId`, removes the identity from `external_terminals`, and delegates to `close_terminal_internal`; it never invokes herdr or any remote-agent stop path. A missing identity returns `false`. Removing the index entry before delegating guarantees a concurrently completing spawn cannot re-insert the terminal and that a second close returns `false`.

Keep using `AgentPanelEvent::ActiveViewFocused`, `ActiveViewChanged`, and `EntryChanged` for activation and presence, plus the one new generic `ExternalTerminalSpawnFinished` variant. Do not add any herdr-specific variant; the registry distinguishes mirror spawns by reading `external_terminal_identity` after those events.

- [ ] **Step 6: Run focused tests and the crate check**

Run:

```bash
cargo test -p agent_ui --lib external_terminal_thread
cargo check -p agent_ui
```

Expected: external identity is deduplicated, closed identities disappear, external mirrors are not restored, and existing terminals still compile unchanged.

- [ ] **Step 7: Commit the Agent Panel seam**

```bash
git add crates/agent_ui/src/agent_panel.rs crates/agent_ui/src/agent_ui.rs
git commit -m "feat(agent-ui): open external terminal threads"
```

---

### Task 3: Implement the pure herdr agent synchronization reducer

**Files:**
- Create: `crates/zed/src/zed/herdr_agent_sync.rs`
- Modify: `crates/zed/src/zed.rs:1-4`
- Test: `crates/zed/src/zed/herdr_agent_sync.rs`

**Interfaces:**
- Consumes typed `herdr::WorkspaceInfo`, `herdr::PaneInfo`, and `herdr::PaneEvent` from Task 1.
- Produces stable `SessionIdentity`, `AgentKey`, `AgentRecord`, `AgentSyncEffect`, `AgentSyncState`, `FocusTarget`, `FocusEcho`, and `FocusObservation`.
- Produces `SessionIdentity::from_info`, `AgentKey::new`, `AgentSyncState::{apply_workspace, upsert, exit, forget_session, dismiss, record_failure, clear_failure, resync, len}`, and `FocusEcho::{request, is_current, observe}`.
- Contains no GPUI entities, windows, Agent Panel types, process handles, or filesystem I/O.

- [ ] **Step 1: Add failing reducer tests**

Create tests for initial open, stale revision rejection, pane move, dismissal, resync, exit, session forget, per-revision failed-open retry, and reflected focus (single and concurrent targets):

```rust
fn session(name: &str) -> SessionIdentity {
    SessionIdentity {
        name: Arc::from(name),
        session_dir: Arc::from(PathBuf::from(format!("/sessions/{name}"))),
    }
}

fn pane(terminal_id: &str, pane_id: &str, revision: u64) -> PaneInfo {
    PaneInfo {
        workspace_id: "workspace-1".into(),
        tab_id: "tab-1".into(),
        pane_id: pane_id.into(),
        terminal_id: terminal_id.into(),
        focused: false,
        revision,
        agent: Some("claude".into()),
        agent_status: Some("working".into()),
        cwd: Some("/repo/worktree".into()),
        foreground_cwd: None,
        agent_session: None,
    }
}

#[test]
fn pane_move_keeps_one_agent_by_terminal_identity() {
    let mut state = AgentSyncState::default();
    let first = state.upsert(session("main"), pane("terminal-1", "pane-a", 1));
    assert!(matches!(first.as_slice(), [AgentSyncEffect::Open(_)]));

    let moved = state.upsert(session("main"), pane("terminal-1", "pane-b", 2));
    assert!(matches!(
        moved.as_slice(),
        [AgentSyncEffect::PaneMoved { pane_id, .. }] if pane_id.as_ref() == "pane-b"
    ));
    assert_eq!(state.len(), 1);
}

#[test]
fn dismissed_terminal_stays_closed_until_resync() {
    let mut state = AgentSyncState::default();
    let key = AgentKey::new(session("main"), "terminal-1");
    state.dismiss(key.clone());
    assert!(state
        .upsert(session("main"), pane("terminal-1", "pane-a", 3))
        .is_empty());
    assert_eq!(state.resync(&session("main")).len(), 1);
}

#[test]
fn stopped_session_forgets_every_agent() {
    let mut state = AgentSyncState::default();
    let identity = session("main");
    state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));

    let effects = state.forget_session(&identity);
    assert!(matches!(
        effects.as_slice(),
        [AgentSyncEffect::Forget(key)] if key.terminal_id.as_ref() == "terminal-1"
    ));
    assert_eq!(state.len(), 0);
}

#[test]
fn failed_open_retries_once_on_a_new_revision() {
    let mut state = AgentSyncState::default();
    let identity = session("main");
    let key = AgentKey::new(identity.clone(), "terminal-1");
    assert!(matches!(
        state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1)).as_slice(),
        [AgentSyncEffect::Open(_)]
    ));
    assert!(state.record_failure(&key, 1));
    assert!(!state.record_failure(&key, 1));
    assert!(state
        .upsert(identity.clone(), pane("terminal-1", "pane-a", 1))
        .is_empty());
    assert!(matches!(
        state.upsert(identity, pane("terminal-1", "pane-a", 2)).as_slice(),
        [AgentSyncEffect::Open(_)]
    ));
}


#[test]
fn reflected_focus_is_acknowledged_once() {
    let mut focus = FocusEcho::default();
    let target = FocusTarget::Workspace {
        session: session("session-a"),
        workspace_id: Arc::from("space-1"),
    };
    let token = focus.request(target.clone());
    assert!(focus.is_current(token));
    assert_eq!(focus.observe(target.clone()), FocusObservation::Echo);
    assert_eq!(focus.observe(target.clone()), FocusObservation::External);
}

#[test]
fn concurrent_focus_transitions_echo_independently() {
    let mut focus = FocusEcho::default();
    let workspace = FocusTarget::Workspace {
        session: session("session-a"),
        workspace_id: Arc::from("space-1"),
    };
    let agent = FocusTarget::Agent(AgentKey::new(session("session-a"), "terminal-1"));

    let workspace_token = focus.request(workspace.clone());
    let _agent_token = focus.request(agent.clone());

    // Each pending transition acknowledges its own target and never a peer.
    assert_eq!(focus.observe(workspace), FocusObservation::Echo);
    assert!(focus.is_current(workspace_token) == false);
    assert_eq!(focus.observe(agent), FocusObservation::Echo);

    let stale = focus.request(FocusTarget::Workspace {
        session: session("session-a"),
        workspace_id: Arc::from("space-2"),
    });
    assert_eq!(
        focus.observe(FocusTarget::Workspace {
            session: session("session-a"),
            workspace_id: Arc::from("space-3"),
        }),
        FocusObservation::External
    );
    assert!(!focus.is_current(stale));
}
```


- [ ] **Step 2: Run the reducer test and confirm red output**

Run:

```bash
cargo test -p zed --bin zed herdr_agent_sync
```

Expected: compilation fails because the module and reducer types do not exist.

- [ ] **Step 3: Define stable identities, records, and effects**

Use the CLI session directory as the stable session discriminator and terminal ID as the stable agent discriminator:

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SessionIdentity {
    pub(crate) name: Arc<str>,
    pub(crate) session_dir: Arc<Path>,
}

impl SessionIdentity {
    pub(crate) fn from_info(info: &SessionInfo) -> Self {
        Self {
            name: Arc::from(info.name.as_str()),
            session_dir: Arc::from(info.session_dir.clone()),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AgentKey {
    pub(crate) session: SessionIdentity,
    pub(crate) terminal_id: Arc<str>,
}

impl AgentKey {
    pub(crate) fn new(
        session: SessionIdentity,
        terminal_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            session,
            terminal_id: terminal_id.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentRecord {
    pub(crate) key: AgentKey,
    pub(crate) workspace_id: Arc<str>,
    pub(crate) pane_id: Arc<str>,
    pub(crate) revision: u64,
    pub(crate) focused: bool,
    pub(crate) checkout_path: Option<PathBuf>,
    pub(crate) effective_cwd: Option<PathBuf>,
    pub(crate) agent_name: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentSyncEffect {
    Open(AgentRecord),
    PaneMoved { key: AgentKey, pane_id: Arc<str> },
    Focus(AgentKey),
    Forget(AgentKey),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FocusTarget {
    Workspace {
        session: SessionIdentity,
        workspace_id: Arc<str>,
    },
    Agent(AgentKey),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FocusObservation {
    Echo,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FocusToken(u64);

#[derive(Default)]
pub(crate) struct FocusEcho<T> {
    generation: u64,
    pending: HashMap<T, FocusToken>,
}

impl<T: Clone + Eq + Hash> FocusEcho<T> {
    pub(crate) fn request(&mut self, target: T) -> FocusToken {
        self.generation = self.generation.saturating_add(1);
        let token = FocusToken(self.generation);
        self.pending.insert(target, token);
        token
    }

    pub(crate) fn is_current(&self, token: FocusToken) -> bool {
        self.pending.values().any(|current| *current == token)
    }

    pub(crate) fn observe(&mut self, target: T) -> FocusObservation {
        if self.pending.remove(&target).is_some() {
            FocusObservation::Echo
        } else {
            FocusObservation::External
        }
    }
}
```

`FocusEcho<FocusTarget>` keeps one pending token per target so concurrent workspace and agent requests never overwrite each other. `FocusTarget::Workspace` is `(session, workspace_id)` and `FocusTarget::Agent` is the stable `(session, terminal_id)` identity; the current pane ID remains only the outbound `agent.focus` RPC target. The saturating global generation rejects stale completions; observing a target removes its entry and returns `Echo`, and any other target is `External`.

`AgentSyncState` owns workspace paths keyed by `(SessionIdentity, workspace_id)`, `HashMap<AgentKey, AgentRecord>`, a pane-to-key lookup for exit events that lack terminal IDs, `HashSet<AgentKey>` dismissal suppression, and the last failed revision per agent.

- [ ] **Step 4: Implement revision, move, dismissal, and resync rules**

Apply workspace create/update data before panes. Updating a space path records the new path but emits no `Open` effect by itself. When a pane later becomes a detected agent, `upsert` copies the latest workspace checkout path into `AgentRecord`; `effective_cwd` uses `foreground_cwd` when present and otherwise uses `cwd`.

`upsert` follows this order:

```rust
if pane.agent.is_none() || pane.terminal_id.is_empty() {
    return Vec::new();
}
if current.is_some_and(|record| record.revision >= pane.revision) {
    return Vec::new();
}
if self.dismissed.contains(&key) {
    self.records.insert(key, record);
    return Vec::new();
}
```

A new key emits `Open`. A newer record with a changed `pane_id` emits `PaneMoved` without `Open`, except that a record whose last open failed emits one retrying `Open` when its revision advances. `record_failure(key, revision)` returns `true` only for the first failure at that revision so the driver emits one actionable notification; `clear_failure` runs after a successful mirror open. A false-to-true focus transition emits `Focus`. Pane exit removes record, pane lookup, dismissal, and failed revision and emits `Forget`; `forget_session(session)` performs the same bookkeeping for every live record in a stopped session. `Forget` means the driver drops synchronization ownership only—it never closes the Agent Panel terminal, preserving the exited process's final output. `resync(session)` clears dismissal and failed-revision suppression for that session and emits `Open` for every live record; the Agent Panel's external identity index deduplicates terminals that are already open.

- [ ] **Step 5: Run reducer tests**

Run:

```bash
cargo test -p zed --bin zed herdr_agent_sync
```

Expected: all reducer tests pass without opening windows, terminals, sockets, or processes.

- [ ] **Step 6: Commit the reducer**

```bash
git add crates/zed/src/zed.rs crates/zed/src/zed/herdr_agent_sync.rs
git commit -m "feat(herdr): reduce agent synchronization events"
```

---

### Task 4: Replace automatic startup with the session registry and picker

**Files:**
- Create: `crates/zed/src/zed/herdr_session_registry.rs`
- Create: `crates/zed/src/zed/herdr_session_picker.rs`
- Modify: `crates/zed/src/zed.rs:1-4,193-224,640-685,888-945`
- Modify: `crates/zed/src/zed/herdr_host.rs:24-55,91-840,952-1215`
- Modify: `crates/zed/src/main.rs:76-80,1418-1468`
- Modify: `crates/zed_actions/src/lib.rs:1011-1043`
- Modify: `crates/herdr/src/herdr.rs:25-108,852-860`
- Modify: `crates/herdr/Cargo.toml:15-21`
- Test: `crates/zed/src/zed/herdr_session_registry.rs`
- Test: `crates/zed/src/zed/herdr_session_picker.rs`
- Test: `crates/zed/src/zed/herdr_host.rs:1216-1235`

**Interfaces:**
- Consumes `herdr::list_sessions`, `SessionInfo`, `HerdRClient`, and Task 3's reducer.
- Produces the application-global `HerdrSessionRegistry`, per-window `BindingState`, `BindingEvent`, `SessionSelection`, `SelectionTarget`, `PendingBinding`, and pending-binding reservation/consumption.
- Produces `transition`, `selection_target`, `show_session_picker`, startup prompt-once behavior, resync, disconnect, and clean lowercase actions.
- Reduces `HerdRHost` to a selected-session central-view adapter.

- [ ] **Step 1: Use LSP references before the action and client cutover**

Request LSP references for these exported symbols before editing:

```text
zed_actions::herdr::OpenHerdR
zed_actions::herdr::OpenHerdRInNewWindow
zed_actions::herdr::ToggleHerdR
zed_actions::herdr::FocusHerdR
herdr::Endpoint::session
herdr::Endpoint::from_environment
herdr::ClientConfig::default
```

Record every source callsite in this task. Use LSP rename for the surviving action types:

```text
ToggleHerdR -> ToggleHerdr
FocusHerdR -> FocusHerdr
ToggleHerdRMaximize -> ToggleHerdrMaximize
ToggleHerdRCollapse -> ToggleHerdrCollapse
CloseHerdR -> CloseHerdr
ShowHerdRStatus -> ShowHerdrStatus
```

Remove the two obsolete open actions only after their handlers are migrated to `SelectOrCreateSession`.

- [ ] **Step 2: Add failing lifecycle, target-window, and picker tests**

Cover transitions without a real herdr process:

```rust
fn session(name: &str) -> SessionIdentity {
    SessionIdentity {
        name: Arc::from(name),
        session_dir: Arc::from(PathBuf::from(format!("/sessions/{name}"))),
    }
}

#[test]
fn normal_exit_returns_to_unselected_without_retry() {
    let state = transition(
        BindingState::Connected(session("main")),
        BindingEvent::SessionNotRunning,
    );
    assert_eq!(state, BindingState::Unselected);
}

#[test]
fn lost_stream_for_running_session_becomes_failed() {
    let state = transition(
        BindingState::Connected(session("main")),
        BindingEvent::SessionStillRunning("socket closed".into()),
    );
    assert!(matches!(state, BindingState::Failed { .. }));
}

#[test]
fn selection_from_connected_window_always_targets_new_window() {
    assert_eq!(selection_target(&BindingState::Connected(session("main"))), SelectionTarget::NewWindow);
    assert_eq!(selection_target(&BindingState::Unselected), SelectionTarget::InvokingWindow);
}

```

Add registry routing tests for the initial focused worktree: an unselected invocation reuses an exact eligible window; a mismatched invoking window remains `Unselected` while a new focused-worktree window inherits the session; and a selection from `Connected` keeps its already-created target window even when another window already has the same root.

Picker tests construct running and stopped `SessionInfo` rows, assert both produce `SessionSelection::Existing`, assert cancel emits no selection, and assert new-session confirmation emits the edited name rather than silently accepting the workspace-derived suggestion.

Add a pending-binding test: consuming a pending workspace entity ID returns its session exactly once, and the startup observer does not request a picker for that inherited window.

Add connection-lifetime tests with a fake `list_sessions`/connect harness: two windows binding the same session open exactly one client and one event stream; the second window reuses both; disconnecting the first window keeps the client; disconnecting the last window drops it, and a later selection creates a fresh client. Add a lost-stream test that refreshes the session list, finds the session no longer running, and asserts the binding becomes `Unselected` with no reconnect task created.

Add a picker test asserting a `validate_session_name` rejection keeps the modal in `NewName` mode showing the CLI message with no registry binding, and a second test asserting a session-list error leaves the picker open with one retry row. Assert stopped rows render the stopped indicator.

Add a host render test: build a `Failed` binding, render the host, and assert the `Retry` and `Choose Session` actions are present; assert `Starting` renders `herdr: Starting <session>…` without retry.

- [ ] **Step 3: Run focused tests and confirm red output**

Run:

```bash
cargo test -p zed --bin zed herdr_session_registry
cargo test -p zed --bin zed herdr_session_picker
```

Expected: compilation fails because the registry, picker, state machine, and target policy do not exist.

- [ ] **Step 4: Define and initialize the application registry**

Use a GPUI entity global:

```rust
struct GlobalHerdrSessionRegistry(Entity<HerdrSessionRegistry>);
impl gpui::Global for GlobalHerdrSessionRegistry {}

pub(crate) fn init(cx: &mut App) {
    let registry = cx.new(HerdrSessionRegistry::new);
    cx.set_global(GlobalHerdrSessionRegistry(registry));
}

impl HerdrSessionRegistry {
    pub(crate) fn global(cx: &App) -> Entity<Self> {
        cx.global::<GlobalHerdrSessionRegistry>().0.clone()
    }
}
```

The registry owns:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingState {
    Unselected,
    Starting { session_name: Arc<str> },
    Connected(SessionIdentity),
    Failed { session_name: Arc<str>, message: SharedString },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingEvent {
    Selected { session_name: Arc<str> },
    Connected(SessionIdentity),
    SessionNotRunning,
    SessionStillRunning(SharedString),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionSelection {
    Existing(SessionInfo),
    New { name: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectionTarget {
    InvokingWindow,
    NewWindow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingBinding {
    ConfirmedSelection(SessionSelection),
    Inherited(SessionIdentity),
}

struct WindowBinding {
    window: WindowHandle<MultiWorkspace>,
    state: BindingState,
    workspace_id: Option<Arc<str>>,
    checkout_path: Option<CanonicalPath>,
}

struct SessionConnection {
    info: SessionInfo,
    client: HerdRClient,
    bound_windows: HashSet<u64>,
    event_task: Task<()>,
}

fn transition(current: BindingState, event: BindingEvent) -> BindingState {
    match event {
        BindingEvent::Selected { session_name } => BindingState::Starting { session_name },
        BindingEvent::Connected(session) => BindingState::Connected(session),
        BindingEvent::SessionNotRunning => BindingState::Unselected,
        BindingEvent::SessionStillRunning(message) => {
            let session_name = match current {
                BindingState::Starting { session_name }
                | BindingState::Failed { session_name, .. } => session_name,
                BindingState::Connected(session) => session.name,
                BindingState::Unselected => return BindingState::Unselected,
            };
            BindingState::Failed {
                session_name,
                message,
            }
        }
    }
}

fn selection_target(state: &BindingState) -> SelectionTarget {
    match state {
        BindingState::Unselected => SelectionTarget::InvokingWindow,
        BindingState::Starting { .. }
        | BindingState::Connected(_)
        | BindingState::Failed { .. } => SelectionTarget::NewWindow,
    }
}
```

Key connections by `SessionIdentity { name, session_dir }`. Key window bindings by `window.window_id().as_u64()`. Store `PendingBinding` by the new workspace entity ID so `observe_new::<MultiWorkspace>` consumes a confirmed picker result or inherited session before deciding whether to prompt.

Initialize the global in `zed::init`. Observe new `MultiWorkspace` roots once: inherited roots bind silently; ordinary roots schedule the picker after restoration and window creation settle. Cancellation writes no binding and starts no task.

- [ ] **Step 5: Implement the session picker**

Follow the existing `Picker<Delegate>` plus `ModalView` pattern. Use two delegate modes:

```rust
#[derive(Clone)]
enum PickerEntry {
    Session(SessionInfo),
    NewSession,
}

enum PickerMode {
    Sessions,
    NewName { suggested_name: String },
}

pub(crate) enum SessionPickerEvent {
    Confirmed(SessionSelection),
}
```

`Sessions` includes running entries, stopped entries, and `NewSession`. Stopped rows render a distinct visible stopped indicator (for example `stopped` suffix or muted status chip). Selecting `NewSession` changes to `NewName`, prefills the query from `Project::first_project_directory().file_name()`, and requires a second confirm. On confirm, the delegate calls `herdr::validate_session_name` while the modal stays open; rejection keeps the picker in `NewName` mode showing the trimmed CLI message, and only a successful validation emits `Confirmed`. Empty trimmed names remain in name mode and show a validation message. `DismissEvent` without `Confirmed` leaves the registry untouched.

A session-list error keeps the modal open with one retry row. Do not create a synthetic default row.

- [ ] **Step 6: Parameterize the central host and remove connection ownership**

Change host installation to accept a confirmed session:

```rust
pub(crate) struct HerdrLaunch {
    pub(crate) session_name: Arc<str>,
    pub(crate) executable: PathBuf,
}

fn install_host(
    multi_workspace: &mut MultiWorkspace,
    workspace: Entity<Workspace>,
    launch: HerdrLaunch,
    window: &mut Window,
    cx: &mut Context<MultiWorkspace>,
);
```

The host's task terminal uses:

```rust
SpawnInTerminal {
    id: TaskId(format!("herdr-session:{}", launch.session_name)),
    full_label: format!("herdr · {}", launch.session_name),
    label: "herdr".to_owned(),
    command: Some(launch.executable.to_string_lossy().into_owned()),
    args: vec!["--session".to_owned(), launch.session_name.to_string()],
    command_label: format!("herdr --session {}", launch.session_name),
    cwd: Some(fixed_worktree),
    use_new_terminal: true,
    reveal: RevealStrategy::Never,
    reveal_target: RevealTarget::Dock,
    shell: Shell::System,
    show_command: false,
    show_summary: false,
    ..Default::default()
}
```

Delete `SESSION_OWNERS`, endpoint claims, `HerdRHost.client`, snapshot ownership, workspace/agent focus code, generation, and both retry loops from `herdr_host.rs`. The host renders registry state and safe actions; raw endpoint paths and OS errors remain in logs.

The host renders exactly one surface per binding state, matching the approved wording:

- `Unselected` → `herdr: Select a session` plus `Choose Session` action;
- `Starting { session_name }` → `herdr: Starting <session>…` (no retry);
- `Connected(session)` → `herdr: Connected to <session>`;
- `Failed { session_name, message }` → `herdr: Could not connect to <session>` plus `Retry` and `Choose Session` actions; the raw socket path and OS error text stay in logs.

The status item mirrors the same label and opens the picker or focuses the host. `Retry` reruns the bounded connect loop for the failed session; `Choose Session` opens the picker. Both actions are always reachable from the host surface in `Failed`.

Toggling/focusing herdr in an unselected window opens the picker instead of installing a default host. Restoring `herdr_visible` with no inherited binding shows `herdr: Select a session`; it never starts a process.

- [ ] **Step 7: Implement selection, bounded connection, and liveness**

Selection flow:

```text
picker confirmation
→ for a New selection, call validate_session_name first and keep the picker open with the error on rejection (no binding, host, or window)
→ choose a bootstrap window from BindingState
→ reserve PendingBinding before creating a bootstrap window
→ install/show the selected central host and set Starting { session_name }
→ refresh the session list until the selected entry and socket appear
→ construct SessionIdentity from the reported SessionInfo
→ connect with ClientConfig::new(session.endpoint())
→ open the event stream before the snapshot; buffer frames while fetching one snapshot
→ drain the buffer, then resolve the snapshot's focused worktree and finalize the target binding
→ synchronize every running agent from the snapshot, then set Connected(identity)
```

`SelectionTarget::NewWindow` creates one blank target with `Workspace::new_local(..., OpenMode::NewWindow, ...)`. Its init closure obtains `cx.entity_id()` and reserves `PendingBinding::ConfirmedSelection(selection)` before the `MultiWorkspace` observer can schedule a startup picker. After the snapshot arrives, add/activate the normalized focused worktree inside that same target with `OpenMode::Activate`; do not reuse another window. The picker remains in the invoking window throughout.

For `SelectionTarget::InvokingWindow`, resolve the snapshot's focused worktree before finalizing the binding. Reuse and focus an exact normalized root only when that candidate is `Unselected` or already bound to the same session. Otherwise open a new focused-worktree window with `PendingBinding::Inherited(identity)` and leave the mismatched invoking window `Unselected`. With no focused worktree, bind the invoking window without guessing a path. If routing changes the target after bootstrap, install the selected host in the final window before detaching the local bootstrap client; detaching must not terminate the herdr session.

If `SessionIdentity` already has a registry connection, reuse that client and event stream, fetch a fresh snapshot through it for this binding, and do not create a second subscription.

Mechanically, the connection task spawns a child loop that polls `SubscribeStream::next` and forwards `HerdrEvent`s into a bounded channel, awaits `snapshot()`, then drains the channel in order before importing; further frames continue through the same channel as the steady-state event stream.

Use one 15-second deadline and a short timer between catalog/connect attempts. Stop on target-window release, user disconnect, host process exit, or deadline. A connection is not `Connected` until the subscription is open, the snapshot is fetched, the buffered events are drained, the final target binding is installed, and initial agents have been scheduled through the normal deduplicated synchronization path.

On stream loss, call `list_sessions` once. Missing/not-running calls `forget_session`, drops registry synchronization ownership without closing mirrored terminals, and becomes `Unselected`; still-running becomes `Failed { message }`. Do not create another timer or connection task.

`DisconnectSession` removes only the invoking window binding, closes that binding's local `herdr ... agent attach` mirrors and its local `herdr --session` central-host client terminal (both are herdr client processes; no agent-stop RPC is sent and the session server is untouched), and detaches host presentation to the unselected surface. While another window stays bound to the session, the shared registry client and event stream remain open. When the last bound window disconnects, the registry drops its client and cancels the event task: the session socket belongs to the independent herdr server, so closing every local client never terminates it. A later selection of the same session creates a fresh client; two windows sharing a session always share exactly one client plus one event stream. `ResyncAgents` clears Task 3 suppression and applies a fresh snapshot.

- [ ] **Step 8: Replace the action surface and startup hooks**

The final action list is:

```rust
actions!(
    herdr,
    [
        #[action(name = "Select or Create Session")]
        SelectOrCreateSession,
        #[action(name = "Resync Agents")]
        ResyncAgents,
        #[action(name = "Disconnect Session")]
        DisconnectSession,
        #[action(name = "Toggle herdr")]
        ToggleHerdr,
        #[action(name = "Focus herdr")]
        FocusHerdr,
        #[action(name = "Toggle herdr Maximize")]
        ToggleHerdrMaximize,
        #[action(name = "Toggle herdr Collapse")]
        ToggleHerdrCollapse,
        #[action(name = "Close herdr")]
        CloseHerdr,
        #[action(name = "Show herdr Status")]
        ShowHerdrStatus,
    ]
);
```

Register selection/resync/disconnect against the registry in `zed::init`; route view-only actions through `herdr_host`. Replace `main.rs` calls to `restore_if_visible` with a view-only restore hook that never picks or starts a session. Startup prompting belongs exclusively to the registry's new-window observer.

- [ ] **Step 9: Remove guessed endpoint APIs after all callers migrate**

Use LSP references again. When no UI caller remains, delete `Endpoint::session`, `Endpoint::from_environment`, `session_socket_path`, `impl Default for ClientConfig`, the obsolete `.config/herdr` endpoint test, and `dirs.workspace = true` from `crates/herdr/Cargo.toml`.

Keep `ClientConfig::new(Endpoint)` for explicit tests and non-UI callers. Do not add aliases or deprecated wrappers.

- [ ] **Step 10: Run lifecycle tests and compile the migrated application**

Run:

```bash
cargo test -p herdr
cargo test -p zed --bin zed herdr_session_registry
cargo test -p zed --bin zed herdr_session_picker
cargo test -p zed --bin zed herdr_host
cargo check -p zed_actions
cargo check -p zed --bin zed
```

Expected: all commands pass; no Zed UI path constructs a default endpoint or reconnects indefinitely.

- [ ] **Step 11: Commit the explicit-session cutover**

```bash
git add crates/herdr/Cargo.toml crates/herdr/src/herdr.rs crates/zed_actions/src/lib.rs crates/zed/src/main.rs crates/zed/src/zed.rs crates/zed/src/zed/herdr_host.rs crates/zed/src/zed/herdr_session_picker.rs crates/zed/src/zed/herdr_session_registry.rs
git commit -m "feat(herdr): require explicit session selection"
```

---

### Task 5: Synchronize agent worktrees, terminal threads, and focus

**Files:**
- Modify: `crates/zed/src/zed/herdr_agent_sync.rs`
- Modify: `crates/zed/src/zed/herdr_session_registry.rs`
- Modify: `crates/zed/src/zed.rs:888-945`
- Test: `crates/zed/src/zed/herdr_agent_sync.rs`
- Test: `crates/zed/src/zed/herdr_session_registry.rs`

**Interfaces:**
- Consumes `ExternalTerminalThread` and `AgentPanel::open_external_terminal_thread` from Task 2.
- Consumes Task 3 effects and the Task 4 application registry/window bindings.
- Makes `ensure_agent_panel_for_workspace` `pub(crate)` so registry-driven workspaces can await panel installation through the existing path.
- Produces initial snapshot import, new-agent mirroring, path routing, close suppression, explicit resync, and bidirectional focus.
- Produces `external_terminal_spec(record, herdr_program) -> ExternalTerminalThread` and a pure `MirrorIndex` for Agent Panel presence reconciliation.

- [ ] **Step 1: Add failing driver-policy tests**

Add tests for mirror arguments and Agent Panel close detection:

```rust
fn test_session() -> SessionIdentity {
    SessionIdentity {
        name: Arc::from("main"),
        session_dir: Arc::from(PathBuf::from(r"C:\sessions\main")),
    }
}

fn agent_record(terminal_id: &str, pane_id: &str, worktree: &str) -> AgentRecord {
    AgentRecord {
        key: AgentKey::new(test_session(), terminal_id),
        workspace_id: Arc::from("workspace-1"),
        pane_id: Arc::from(pane_id),
        revision: 4,
        focused: true,
        checkout_path: Some(PathBuf::from(worktree)),
        effective_cwd: Some(PathBuf::from(worktree)),
        agent_name: Arc::from("claude"),
    }
}

#[test]
fn attach_spec_uses_latest_pane_and_structured_args() {
    let record = agent_record("terminal-1", "pane-new", r"C:\repo\worktree");
    let spec = external_terminal_spec(&record, Path::new("herdr.exe"));

    assert_eq!(spec.executable, "herdr.exe");
    assert_eq!(
        spec.args,
        vec!["--session", "main", "agent", "attach", "pane-new"]
    );
    assert_eq!(spec.working_directory, PathBuf::from(r"C:\repo\worktree"));
}

#[test]
fn missing_panel_terminal_returns_the_live_agent_key() {
    let key = AgentKey::new(test_session(), "terminal-1");
    let terminal_id = TerminalId::new();
    let mut mirrors = MirrorIndex::default();
    mirrors.insert(key.clone(), terminal_id);

    assert_eq!(mirrors.missing(|_| false), vec![key]);
    assert!(mirrors.missing(|id| id == terminal_id).is_empty());
}
```

Add a GPUI test with two `MultiWorkspace` handles and canonical roots. Assert an agent matching the first root selects the existing first workspace, while a second unmatched root requests `OpenMode::NewWindow` with a pending inherited binding. Add a nested-path test: an agent whose checkout path is a subdirectory of the first root reuses that root window instead of opening a second window. Add an attach-failure test: a spawn failure resolves through `handle_external_spawn_finished`, `record_failure` returns `true` exactly once, one notification is emitted, other agents keep synchronizing, and a later revision/resync retries once. Use a recording window router in the registry test constructor; production uses the real workspace APIs.

- [ ] **Step 2: Run focused synchronization tests and confirm red output**

Run:

```bash
cargo test -p zed --bin zed herdr_agent_sync
cargo test -p zed --bin zed herdr_session_registry
```

Expected: new driver tests fail because effects are not connected to workspaces or Agent Panel terminals.

- [ ] **Step 3: Process initial snapshots and pane refresh events**

Bootstrap order is subscribe → buffer → snapshot → drain → import. The connection task opens `subscribe()` first (starting at the current hub sequence), buffers every incoming frame, then fetches `snapshot()`, then drains the buffer before importing. Events that fired between the subscribe ack and the snapshot are in the buffer; with subscribe after snapshot they would be missing and no later event would re-sync a quiet agent. After the snapshot and final target binding exist, apply every running agent in `SessionSnapshot.agents` before the registry publishes `Connected`.

Event application: `pane.created` and `pane.updated` reduce the nested full `PaneInfo` directly; `pane.moved` reduces the nested `pane` and updates the stored `pane_id` (the previous pane ID replaces the old pane-to-key entry); the id-only reference events `pane.focused` and `pane.agent_detected` call `client.pane(event.pane_id)` and reduce the returned full `PaneInfo`, whose revision guard and focus-transition logic apply. A `pane.get` error that reports the pane no longer exists is treated as `Forget` from the lookup (the pane exited between the event and the fetch); any other error is logged and the event task continues. For closed/exited panes, resolve the stable key through the reducer's pane lookup and apply `Forget` without closing its Agent Panel terminal. Reject a stale pane revision. A pane move never spawns another attached terminal.

- [ ] **Step 4: Resolve and route the agent worktree**

Use the workspace checkout path associated with `AgentRecord.workspace_id`; fall back to `foreground_cwd`, then `cwd`. Require an existing absolute directory, then resolve the containing worktree root with `worktree::discover_root_repo_common_dir(path, fs)` (the design's "normal project/worktree APIs"); normalize that root with `canonical_checkout_path`, whose Windows branch lowercases drive/components, normalizes separators, and removes trailing separators before equality checks. When no repo root is found, fall back to the normalized directory itself.

Search registry-known `MultiWorkspace` windows for an exact canonical root. A nested checkout path inside an existing root window reuses that root window; it never opens a second window for the subdirectory. If absent, invoke the existing local workspace path with `OpenMode::NewWindow` and an init closure that records the inherited session before the root observer can show a picker:

```rust
let open_task = multi_workspace.find_or_create_local_workspace(
    PathList::new(&[worktree_path.clone()]),
    None,
    Some(Box::new(move |_workspace, _window, cx| {
        let workspace_id = cx.entity_id();
        HerdrSessionRegistry::global(cx).update(cx, |registry, _| {
            registry.reserve_inherited_workspace(workspace_id, session.clone());
        });
    })),
    OpenMode::NewWindow,
    Some(source_workspace),
    window,
    cx,
);
```

Use `cx.entity_id()` from the `Context<Workspace>` passed to the init closure. Store that actual entity ID and consume it once when `observe_new::<MultiWorkspace>` sees the new root.

For an invalid, missing, or unopenable path, call `record_failure(key, revision)`. Emit the actionable notification only when it returns `true`; continue applying effects for other agents. A later pane revision or explicit resync permits one new attempt.

- [ ] **Step 5: Open or focus the mirrored terminal thread**

After `ensure_agent_panel_for_workspace` completes, call:

```rust
let terminal_id = panel.update_in(cx, |panel, window, cx| {
    panel.open_external_terminal_thread(
        ExternalTerminalThread {
            identity: format!(
                "herdr:{}:{}",
                record.key.session.session_dir.display(),
                record.key.terminal_id
            )
            .into(),
            executable: herdr_program.to_string_lossy().into_owned(),
            args: vec![
                "--session".into(),
                record.key.session.name.to_string(),
                "agent".into(),
                "attach".into(),
                record.pane_id.to_string(),
            ],
            title: format!("herdr · {}", record.agent_name).into(),
            working_directory: worktree_path,
        },
        record.focused,
        window,
        cx,
    )
})?;
```

Store `AgentKey -> (WindowHandle<MultiWorkspace>, WeakEntity<AgentPanel>, TerminalId)`. Repeated `Open` effects activate the existing terminal.

Spawn outcomes cross the module boundary through `AgentPanelEvent::ExternalTerminalSpawnFinished`. In the panel observer: `message: None` resolves the identity to the `AgentKey` via the mirror index and calls `clear_failure(key)`; `message: Some(_)` resolves the current `record_failure(key, revision)` gate, emits the one actionable notification when it returns `true`, and leaves the session event task running. Never treat a failed spawn as a user close. On Windows with herdr 0.8.2 the attach CLI exits with `direct terminal attach is not supported on Windows yet` (see Global Constraints); that surfaces as this same one-per-revision failure, not as a loop or a close.

- [ ] **Step 6: Wire two-way focus and close suppression**

On herdr workspace focus, activate an already mapped worktree window only; never open one from that event. On herdr agent focus, activate the mapped window and call `AgentPanel::activate_terminal(terminal_id, true, window, cx)`.

Observe `AgentPanelEvent`:

```rust
match event {
    AgentPanelEvent::ActiveViewFocused | AgentPanelEvent::ActiveViewChanged => {
        if let Some(terminal_id) = panel.active_terminal_id()
            && let Some(identity) = panel.external_terminal_identity(terminal_id)
            && let Some(agent) = registry.agent_for_external_identity(identity)
        {
            registry.focus_herdr_agent(agent);
        }
    }
    AgentPanelEvent::EntryChanged => registry.detect_closed_mirrors(panel),
    AgentPanelEvent::ExternalTerminalSpawnFinished { identity, message } => {
        registry.handle_external_spawn_finished(identity, message, panel);
    }
    AgentPanelEvent::TerminalCloseRequested { .. }
    | AgentPanelEvent::ThreadInteracted { .. } => {}
}
```

`focus_herdr_agent` uses the latest stored pane ID:

```rust
client.focus_agent(record.pane_id.to_string()).await
```

Workspace focus calls the existing `client.focus_workspace(workspace_id)`. Agent focus calls `client.focus_agent(latest_pane_id)`. One shared `FocusEcho<FocusTarget>` per session keys pending state per target, so concurrent workspace and agent transitions never overwrite each other. Each outbound request captures a `FocusToken` and ignores a stale completion unless `FocusEcho::is_current(token)`; the matching reflected remote event removes its own target entry and does not activate again.

On `EntryChanged`, a mapped `TerminalId` that is no longer present while its herdr record is live becomes dismissed. `ResyncAgents` clears dismissal/retry suppression and replays current records. Pane exit clears dismissal permanently.

- [ ] **Step 7: Run synchronization and Agent Panel regressions**

Run:

```bash
cargo test -p agent_ui --lib external_terminal_thread
cargo test -p zed --bin zed herdr_agent_sync
cargo test -p zed --bin zed herdr_session_registry
cargo check -p zed --bin zed
```

Expected: all commands pass; pane moves keep one thread, closed mirrors stay closed, resync restores them, and focus reflection terminates after one acknowledgement.

- [ ] **Step 8: Commit agent synchronization**

```bash
git add crates/zed/src/zed.rs crates/zed/src/zed/herdr_agent_sync.rs crates/zed/src/zed/herdr_session_registry.rs
git commit -m "feat(herdr): mirror agents into Zed worktrees"
```

---

### Task 6: Normalize branding, verify Windows behavior, and finish documentation

**Files:**
- Modify: user-visible strings in `crates/herdr/src/herdr.rs`
- Modify: user-visible strings and action references in `crates/zed/src/zed/herdr_host.rs`
- Modify: user-visible action names in `crates/zed_actions/src/lib.rs`
- Modify: affected assertion prose in `crates/workspace/src/multi_workspace_tests.rs` only when it describes rendered UI
- Modify: `docs/superpowers/specs/2026-09-02-herdr-persistent-central-view-design.md`
- Modify: `docs/superpowers/plans/2026-09-02-herdr-persistent-central-view.md`
- Verify: `docs/superpowers/specs/2026-09-05-herdr-session-sync-design.md`

**Interfaces:**
- No new runtime interface.
- Removes obsolete labels, comments that prescribe default endpoint ownership, unused imports/types, and temporary test helpers.
- Preserves internal names such as `HerdRClient` and `HerdRHost` when they are not displayed.

- [ ] **Step 1: Normalize every displayed label and diagnostic**

Use the repository Grep tool with:

```text
HerdR|herdR|herd R
```

Classify every match. Change action display names, aria labels, tooltips, status/header text, notifications, and error strings to lowercase `herdr`. Internal Rust identifiers may remain; comments and assertion prose must use lowercase when they describe the rendered product.

Do not add wording-only tests. Existing behavior tests remain state-based.

- [ ] **Step 2: Remove obsolete lifecycle weight**

Use compiler diagnostics plus LSP references to remove all code made unreachable by the registry cutover:

```text
SESSION_OWNERS
claim_session
release_session
OpenHerdR
OpenHerdRInNewWindow
open_current_from_app
open_new_window_from_app
session_socket_path
Endpoint::from_environment
ClientConfig::default
```

Remove unused retry constants, endpoint imports, snapshots, focus origins, generation fields, and compatibility comments. Keep no aliases or forwarding wrappers.

- [ ] **Step 3: Run the complete affected Rust verification**

Run:

```bash
cargo test -p herdr
cargo test -p agent_ui --lib external_terminal_thread
cargo test -p zed --bin zed herdr_agent_sync
cargo test -p zed --bin zed herdr_session_registry
cargo test -p zed --bin zed herdr_host
cargo check -p zed_actions
cargo check -p zed --bin zed
cargo build -p zed --bin zed
```

Expected: every command exits successfully. Treat a warning introduced by this change as a defect; do not suppress it.

- [ ] **Step 4: Run the native Windows smoke scenario**

Use a separate stateless Zed user-data directory and the installed herdr CLI. Start the built `target/debug/zed.exe` as a managed long-running process, then inspect and operate the actual Zed window with the computer-use workflow.

Exercise this exact sequence:

1. Confirm `herdr session list --json` reports `%APPDATA%\herdr` socket paths and Zed selects those paths without creating `%USERPROFILE%\.config\herdr`.
2. Cancel the startup picker; verify Zed remains usable, shows `herdr: Select a session`, starts no herdr process, and performs no reconnect.
3. Select a running session, select a stopped session, and create a session after editing the workspace-derived suggested name.
4. From a connected window, invoke selection again; verify the current binding remains and the confirmed selection opens a separate Zed window.
5. Select an existing session with running agents; verify every agent opens immediately in the exact existing/new worktree window.
6. Start agents in two worktrees. On a platform whose `herdr` supports direct terminal attach, verify each Agent Panel terminal is attached to the original live pane and no second agent process appears. On Windows 0.8.2 (attach unsupported, see Global Constraints), verify the degraded path instead: each agent revision gets exactly one actionable notification containing the CLI's `direct terminal attach is not supported` message, synchronization of other agents continues, and no re-spawn loop starts. A future herdr build with Windows attach flips this step to the live-attach assertion.
7. Move a herdr pane; verify its existing Zed terminal thread remains single and focus uses the new pane ID.
8. Focus worktrees and agents from both herdr and Zed; verify one stable transition per action without flicker.
9. With two Zed windows bound to one session, invoke `herdr: Disconnect Session` from one window; verify its window/worktree/editor state, the herdr agent panes, and the shared connection remain intact while that window's mirrored terminals detach. Then disconnect the last bound window; verify the herdr process and its agents stay alive, Zed stops mirroring them, and the status reverts to `herdr: Select a session`.
10. Close one mirrored terminal; verify the herdr agent continues and the terminal stays closed. Run `herdr: Resync Agents`; verify it reopens once.
11. Terminate herdr; verify Zed windows/editors remain usable, terminal output remains visible, bindings become unselected, and no reconnect loop starts.
12. Inspect the central header, picker, status item, tooltips, actions, and errors; every visible spelling is `herdr`.

A failure stops completion. Follow systematic debugging against the smallest failing scenario, fix the root cause, and rerun this smoke sequence from step 1.

- [ ] **Step 5: Update superseded documentation after the smoke passes**

In `docs/superpowers/specs/2026-09-02-herdr-persistent-central-view-design.md`:

- change user-facing `HerdR` spelling to `herdr`;
- add a top note linking `[herdr session selection design](2026-09-05-herdr-session-sync-design.md)`;
- state that default-session ownership and automatic startup/retry are superseded;
- retain the still-valid central-view layout and persistence history.

In `docs/superpowers/plans/2026-09-02-herdr-persistent-central-view.md`:

- change user-facing `HerdR` spelling to `herdr`;
- add a top note linking `[herdr session implementation plan](2026-09-05-herdr-session-sync.md)`;
- state that default-session ownership, automatic startup/retry, and the old open actions are superseded;
- retain the historical completed central-view tasks.

Do not add a new changelog or duplicate the approved design.

- [ ] **Step 6: Perform final cleanup checks**

Use the Grep tool again for `HerdR|herdR|herd R`. Remaining matches are allowed only for internal Rust identifiers or quoted forbidden spellings in the approved design's naming/verification requirements. Remove temporary user-data directories and any throwaway scripts created for the smoke run.

Run:

```bash
git diff --check
cargo check -p zed --bin zed
```

Expected: no whitespace errors and a successful final compile.

- [ ] **Step 7: Commit the verified cleanup and documentation**

```bash
git add crates/herdr/src/herdr.rs crates/zed/src/zed/herdr_host.rs crates/zed_actions/src/lib.rs crates/workspace/src/multi_workspace_tests.rs docs/superpowers/specs/2026-09-02-herdr-persistent-central-view-design.md docs/superpowers/plans/2026-09-02-herdr-persistent-central-view.md
git commit -m "docs(herdr): document explicit session workflow"
```

If smoke verification required source fixes outside these paths, stage those exact reviewed files in the same final commit only when they are inseparable from the verified behavior; otherwise amend the owning implementation task's commit before this documentation commit.
