//! Application-global herdr session registry.
//!
//! Owns at most one client connection and event stream per herdr session,
//! shared by every Zed window bound to that session. Windows bind only after
//! an explicit picker confirmation; nothing here connects to a session on
//! installation.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use super::herdr_agent_sync::{
    AgentKey, AgentRecord, AgentSyncEffect, AgentSyncState, FocusEcho, FocusObservation,
    FocusTarget, MirrorIndex, SessionIdentity,
};
use agent_ui::{AgentPanel, AgentPanelEvent, TerminalId};
use fs::Fs;
use futures::channel::mpsc;
use futures::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures::lock::Mutex;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, EntityId, Global, SharedString, Subscription,
    Task, WeakEntity, WindowHandle, WindowId,
};
use herdr::{
    ClientConfig, HerdRClient, HerdrEvent, PaneEvent, PaneEventKind, SessionInfo, SessionSnapshot,
    WorkspaceEvent, canonical_checkout_path,
};
use project::discover_root_repo_common_dir;
use workspace::notifications::{NotificationId, simple_message_notification::MessageNotification};
use workspace::{AppState, MultiWorkspace, OpenMode, Workspace};

/// How long a startup connection attempt may take in total (catalog polling,
/// connect, subscribe, and the first snapshot included).
pub(crate) const CONNECTION_DEADLINE: Duration = Duration::from_secs(15);
/// Short pause between catalog attempts inside the bounded window.
const ATTEMPT_INTERVAL: Duration = Duration::from_millis(100);
/// Bounded event channel between the subscription pump and the registry.
const EVENT_CHANNEL_CAPACITY: usize = 64;
/// Delay that lets window restoration and creation settle before the startup
/// picker is scheduled.
const PROMPT_SETTLE_DELAY: Duration = Duration::from_millis(50);
/// How often the steady-state loop re-checks whether its connection entry is
/// still registered (the last bound window may have disconnected).
const OWNERSHIP_POLL: Duration = Duration::from_millis(250);

/// Explicit launch parameters passed to the central host adapter.
///
/// The registry owns the session choice; the host only renders this launch
/// request and never creates or owns the registry connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HerdrLaunch {
    pub(crate) session_name: Arc<str>,
    pub(crate) executable: PathBuf,
}

/// Adapter seam for the central `herdr --session` host presentation.
///
/// The default implementation is deliberately inert until the host cutover
/// wires its UI adapter. Keeping this seam here lets the registry lifecycle
/// and routing tests run without real windows or host entities.
pub(crate) trait HerdrHostSink {
    fn install(
        &self,
        window: WindowHandle<MultiWorkspace>,
        launch: HerdrLaunch,
        cx: &mut Context<HerdrSessionRegistry>,
    );
    fn detach(&self, window: WindowHandle<MultiWorkspace>, cx: &mut Context<HerdrSessionRegistry>);
}

#[derive(Default)]
struct NoopHerdrHostSink;

impl HerdrHostSink for NoopHerdrHostSink {
    fn install(
        &self,
        _window: WindowHandle<MultiWorkspace>,
        _launch: HerdrLaunch,
        _cx: &mut Context<HerdrSessionRegistry>,
    ) {
    }

    fn detach(
        &self,
        _window: WindowHandle<MultiWorkspace>,
        _cx: &mut Context<HerdrSessionRegistry>,
    ) {
    }
}
/// Find the checkout root containing an agent's current directory. Git's
/// common directory identifies the repository, but linked worktrees must keep
/// their own checkout root for window routing; walking to the nearest `.git`
/// entry preserves that distinction and also handles nested agent cwd paths.
async fn discover_agent_checkout_root(candidate: &Path, fs: &dyn Fs) -> PathBuf {
    let mut root = candidate.to_path_buf();
    loop {
        let dot_git = root.join(".git");
        if fs
            .metadata(&dot_git)
            .await
            .is_ok_and(|metadata| metadata.is_some())
        {
            // Exercise the repository metadata parser here so malformed git
            // metadata follows the same plain-directory fallback as before.
            let _ = discover_root_repo_common_dir(&root, fs).await;
            return root;
        }
        if !root.pop() {
            return candidate.to_path_buf();
        }
    }
}

/// Notification marker for herdr mirror failures. The id is composite per
/// (session, terminal), so two agents failing at the same time produce two
/// visible toasts instead of replacing one another.
struct HerdrMirrorFailureNotification;
/// Notification marker for failures while importing session workspace roots.
struct HerdrWorkspaceImportFailureNotification;

/// Adapter seam for the modal session picker. The picker module supplies the
/// implementation in the explicit-session cutover; cancellation does
/// nothing, so the registry never receives a binding.
pub(crate) trait SessionPickerSink {
    fn show(
        &self,
        window: WindowHandle<MultiWorkspace>,
        registry: Entity<HerdrSessionRegistry>,
        cx: &mut AsyncApp,
    );
}

#[derive(Default)]
struct NoopSessionPickerSink;

impl SessionPickerSink for NoopSessionPickerSink {
    fn show(
        &self,
        _window: WindowHandle<MultiWorkspace>,
        _registry: Entity<HerdrSessionRegistry>,
        _cx: &mut AsyncApp,
    ) {
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingState {
    Unselected,
    Starting {
        session_name: Arc<str>,
    },
    Connected(SessionIdentity),
    Failed {
        session_name: Arc<str>,
        message: SharedString,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingEvent {
    Selected { session_name: Arc<str> },
    SessionNotRunning,
    SessionStillRunning(SharedString),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionSelection {
    Existing(SessionInfo),
    New { name: String },
}

impl SessionSelection {
    pub(crate) fn name(&self) -> &str {
        match self {
            SessionSelection::Existing(info) => &info.name,
            SessionSelection::New { name } => name,
        }
    }
}


struct WindowBinding {
    window: WindowHandle<MultiWorkspace>,
    state: BindingState,
    checkout_path: Option<herdr::CanonicalPath>,
    /// Canonical roots of every worktree this window currently holds.
    roots: Vec<herdr::CanonicalPath>,
}

/// One shared connection per session identity. Dropping this entry releases
/// the client and cancels the event task; the session socket belongs to the
/// independent herdr server, so closing every local client never terminates
/// it.
struct SessionConnection {
    client: Rc<dyn HerdrSessionHandle>,
    bound_windows: HashSet<u64>,
    /// Held purely to keep the pump task (and therefore the subscription)
    /// alive for as long as the connection entry exists; dropping this
    /// struct cancels it.
    _event_task: Task<()>,
    generation: u64,
}


/// The window slot selected for a finalization snapshot. Its generation ties
/// the target to the connection that owns the in-flight mirror effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MirrorTarget {
    window_id: WindowId,
    generation: u64,
}

#[derive(Clone)]
struct AgentMirror {
    window: WindowHandle<MultiWorkspace>,
    panel: WeakEntity<AgentPanel>,
    terminal_id: TerminalId,
}

enum FinalizeResult {
    Ready {
        window_id: WindowId,
        task: Task<anyhow::Result<()>>,
    },
}

/// Pure binding-state machine. `SessionNotRunning` is a normal exit: it
/// returns to `Unselected` and starts no reconnect task.
fn transition(current: BindingState, event: BindingEvent) -> BindingState {
    match event {
        BindingEvent::Selected { session_name } => BindingState::Starting { session_name },
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



/// Workspace work the reducer asked for. `Forget` releases synchronization
/// ownership for one agent; regular shells stay open independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkspaceEffect {
    Open(AgentRecord),
    PaneMoved { key: AgentKey, pane_id: Arc<str> },
    Focus(AgentKey),
    Forget(AgentKey),
}

/// Pure effect classification for the workspace/terminal consumer.
fn plan_effects(effects: Vec<AgentSyncEffect>) -> Vec<WorkspaceEffect> {
    effects
        .into_iter()
        .map(|effect| match effect {
            AgentSyncEffect::Open(record) => WorkspaceEffect::Open(record),
            AgentSyncEffect::PaneMoved { key, pane_id } => {
                WorkspaceEffect::PaneMoved { key, pane_id }
            }
            AgentSyncEffect::Focus(key) => WorkspaceEffect::Focus(key),
            AgentSyncEffect::Forget(key) => WorkspaceEffect::Forget(key),
        })
        .collect()
}

/// Test-only observation seam: recorded *in addition to* the production
/// application path in `dispatch_effects`, never instead of it.
#[cfg(test)]
pub(crate) trait AgentEffectSink {
    fn push(&self, identity: &SessionIdentity, effect: &WorkspaceEffect);
}

/// Pure initial-snapshot import: workspaces first (their checkout paths feed
/// later records), then every running agent through the reducer's upsert so
/// repeated imports stay deduplicated by revision.
fn import_snapshot(
    sync: &mut AgentSyncState,
    identity: &SessionIdentity,
    snapshot: &SessionSnapshot,
) -> Vec<AgentSyncEffect> {
    let mut effects = import_snapshot_workspace_effects(sync, identity, snapshot);
    for pane in &snapshot.agents {
        effects.extend(sync.upsert(identity.clone(), pane.clone()));
    }
    effects
}

fn import_snapshot_workspace_effects(
    sync: &mut AgentSyncState,
    identity: &SessionIdentity,
    snapshot: &SessionSnapshot,
) -> Vec<AgentSyncEffect> {
    let mut effects = Vec::new();
    for workspace in &snapshot.workspaces {
        effects.extend(sync.apply_workspace(identity, workspace));
    }
    effects
}

/// Imports a snapshot after reconciling agents that disappeared while the
/// previous connection generation was unavailable. Retained records dedupe by
/// revision, so their checkout derivation is refreshed from the new
/// workspace map before anything replays them.
fn import_snapshot_for_new_generation(
    sync: &mut AgentSyncState,
    identity: &SessionIdentity,
    snapshot: &SessionSnapshot,
) -> Vec<AgentSyncEffect> {
    let mut effects = sync.reconcile_snapshot(identity, &snapshot.agents);
    effects.extend(import_snapshot(sync, identity, snapshot));
    sync.refresh_record_paths(identity);
    effects
}

/// Pure steady-state event reduction.
fn apply_event_effects(
    sync: &mut AgentSyncState,
    identity: &SessionIdentity,
    event: &HerdrEvent,
) -> Vec<AgentSyncEffect> {
    match event {
        HerdrEvent::Workspace(WorkspaceEvent::Created(workspace))
        | HerdrEvent::Workspace(WorkspaceEvent::Updated(workspace)) => {
            sync.apply_workspace(identity, workspace);
            Vec::new()
        }
        HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Created(pane),
        })
        | HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Updated(pane),
        })
        | HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Moved { pane, .. },
        }) => sync.upsert(identity.clone(), pane.clone()),
        HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Closed { pane_id, .. },
        })
        | HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Exited { pane_id, .. },
        }) => sync.exit(identity, pane_id),
        // `Focused`/`AgentDetected` carry only an id and need a `pane.get`
        // refresh; Task 5 completes that fetch and the focus echo.
        HerdrEvent::Workspace(WorkspaceEvent::Closed { .. })
        | HerdrEvent::Workspace(WorkspaceEvent::Focused { .. })
        | HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Focused { .. } | PaneEventKind::AgentDetected { .. },
        }) => Vec::new(),
    }
}

/// Locate the herdr CLI through `PATH` without extra dependencies. The
/// returned path is only a launch candidate: session endpoints always come
/// from `herdr session list --json`.
pub(crate) fn herdr_program() -> Option<PathBuf> {
    let executable_name = if cfg!(windows) { "herdr.exe" } else { "herdr" };
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).find_map(|directory| {
        let candidate = directory.join(executable_name);
        candidate.is_file().then_some(candidate)
    })
}

/// The transport seam the registry drives. Production wraps the typed
/// `herdr` client; tests inject a fake catalog/connect harness so the full
/// lifecycle runs without a real herdr process.
pub(crate) trait HerdrSessionHandle {
    fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>>;
    fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>>;
    fn pane(&self, pane_id: String) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>>;
    fn focus_agent(&self, pane_id: String) -> LocalBoxFuture<'static, anyhow::Result<()>>;
    fn focus_workspace(&self, workspace_id: String) -> LocalBoxFuture<'static, anyhow::Result<()>>;
}

pub(crate) trait HerdrEventStream {
    fn next(&mut self) -> LocalBoxFuture<'_, anyhow::Result<Option<HerdrEvent>>>;
}

#[derive(Clone)]
pub(crate) struct HerdrGateway {
    list_sessions: Rc<dyn Fn() -> LocalBoxFuture<'static, anyhow::Result<Vec<SessionInfo>>>>,
    connect: Rc<
        dyn Fn(&SessionInfo) -> LocalBoxFuture<'static, anyhow::Result<Rc<dyn HerdrSessionHandle>>>,
    >,
    validate_session_name: Rc<dyn Fn(String) -> LocalBoxFuture<'static, anyhow::Result<()>>>,
}

impl HerdrGateway {
    fn production(program: PathBuf) -> Self {
        let list_program = program.clone();
        let validate_program = program;
        Self {
            list_sessions: Rc::new(move || {
                let program = list_program.clone();
                async move { herdr::list_sessions(program).await.map_err(Into::into) }.boxed_local()
            }),
            connect: Rc::new(move |info: &SessionInfo| {
                let config = ClientConfig::new(info.endpoint());
                async move {
                    let client = HerdRClient::connect(config).await?;
                    Ok(Rc::new(RealHerdrHandle { client }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            }),
            validate_session_name: Rc::new(move |name: String| {
                let program = validate_program.clone();
                async move {
                    herdr::validate_session_name(program, name)
                        .await
                        .map_err(Into::into)
                }
                .boxed_local()
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn fake(
        list_sessions: impl Fn() -> LocalBoxFuture<'static, anyhow::Result<Vec<SessionInfo>>> + 'static,
        connect: impl Fn(
            &SessionInfo,
        ) -> LocalBoxFuture<'static, anyhow::Result<Rc<dyn HerdrSessionHandle>>>
        + 'static,
        validate: impl Fn(String) -> LocalBoxFuture<'static, anyhow::Result<()>> + 'static,
    ) -> Self {
        Self {
            list_sessions: Rc::new(list_sessions),
            connect: Rc::new(connect),
            validate_session_name: Rc::new(validate),
        }
    }

    pub(crate) fn list_sessions(
        &self,
    ) -> LocalBoxFuture<'static, anyhow::Result<Vec<SessionInfo>>> {
        (self.list_sessions)()
    }

    pub(crate) fn connect(
        &self,
        info: &SessionInfo,
    ) -> LocalBoxFuture<'static, anyhow::Result<Rc<dyn HerdrSessionHandle>>> {
        (self.connect)(info)
    }

    pub(crate) fn validate_session_name(
        &self,
        name: String,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        (self.validate_session_name)(name)
    }
}

struct RealHerdrHandle {
    client: HerdRClient,
}

impl HerdrSessionHandle for RealHerdrHandle {
    fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
        let client = self.client.clone();
        async move {
            let stream = client.subscribe().await?;
            Ok(Box::new(RealHerdrStream { stream }) as Box<dyn HerdrEventStream>)
        }
        .boxed_local()
    }

    fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
        let client = self.client.clone();
        async move { Ok(client.snapshot().await?) }.boxed_local()
    }

    fn pane(&self, pane_id: String) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
        let client = self.client.clone();
        async move { Ok(client.pane(pane_id).await?) }.boxed_local()
    }
    fn focus_agent(&self, pane_id: String) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let client = self.client.clone();
        async move { Ok(client.focus_agent(pane_id).await?) }.boxed_local()
    }

    fn focus_workspace(&self, workspace_id: String) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let client = self.client.clone();
        async move { Ok(client.focus_workspace(workspace_id).await?) }.boxed_local()
    }
}
struct RealHerdrStream {
    stream: herdr::SubscribeStream,
}

impl HerdrEventStream for RealHerdrStream {
    fn next(&mut self) -> LocalBoxFuture<'_, anyhow::Result<Option<HerdrEvent>>> {
        async move {
            match self.stream.next().await {
                Ok(event) => Ok(event),
                Err(error) => Err(error.into()),
            }
        }
        .boxed_local()
    }
}

struct GlobalHerdrSessionRegistry(Entity<HerdrSessionRegistry>);
impl Global for GlobalHerdrSessionRegistry {}

pub(crate) struct HerdrSessionRegistry {
    windows: HashMap<u64, WindowBinding>,
    /// Windows that already received (or declined) the startup prompt.
    prompted: HashSet<WindowId>,
    /// Prompts scheduled by the restoration observer but not shown yet.
    prompt_pending: HashSet<WindowId>,
    connections: HashMap<SessionIdentity, SessionConnection>,
    /// A generation per window invalidates detached connection work after a
    /// disconnect, re-selection, or retry.
    attempts: HashMap<u64, u64>,
    /// In-flight reservations coalesce concurrent connection attempts for an
    /// identity before a client or stream is installed.
    in_flight: HashMap<SessionIdentity, u64>,
    /// Fresh-generation snapshot bootstrap is claimed once by whichever
    /// bound window completes finalization first.
    snapshot_bootstrapped: HashSet<(SessionIdentity, u64)>,
    next_generation: u64,
    /// finalization must not tear down a newer shared attachment.
    slot_generations: HashMap<(SessionIdentity, u64), u64>,
    /// Finalization token for a target window awaiting its Connected gate.
    finalize_tokens: HashMap<u64, u64>,
    /// Abort handles for snapshot imports currently finalizing each target.
    snapshot_import_abort_handles: HashMap<u64, AbortHandle>,
    /// Per-window/root gates serialize unmatched workspace additions. A
    /// waiter keeps its own `Arc` clone, so pruning on window release can
    /// never dangle; two waiters racing a pruned entry at worst both add,
    /// which the double-check under the lock reduces.
    workspace_root_locks: HashMap<(u64, herdr::CanonicalPath), Arc<Mutex<()>>>,
    sync: AgentSyncState,
    /// Latest mirror state for every live herdr agent.
    mirror_index: MirrorIndex,
    mirrors: HashMap<AgentKey, AgentMirror>,
    observed_panels: HashSet<EntityId>,
    panel_subscriptions: Vec<Subscription>,
    /// Agents whose mirror open is still in flight, scoped to the connection
    /// generation so stale completion cannot clear a successor.
    mirroring_in_flight: HashSet<(AgentKey, u64)>,
    /// Latest record and target seen for an in-flight mirror key and
    /// generation. A completion replays this newest record once.
    pending_mirror_replay: HashMap<(AgentKey, u64), (AgentRecord, Option<MirrorTarget>)>,
    /// Focus echoes are keyed by session and target so workspace and agent
    /// transitions never overwrite one another.
    focus_echoes: HashMap<SessionIdentity, FocusEcho<FocusTarget>>,
    #[cfg(test)]
    sink_effects: Option<Rc<dyn AgentEffectSink>>,
    #[cfg(test)]
    mirror_target_sink: Option<Rc<std::cell::RefCell<Vec<(AgentKey, WindowId)>>>>,
    host_sink: Rc<dyn HerdrHostSink>,
    picker_sink: Rc<dyn SessionPickerSink>,
    gateway: Option<HerdrGateway>,
    /// Monotonic witness that a bounded connection task was started. Stream
    /// loss never increments it: liveness performs its one catalog refresh
    /// inline instead.
    connection_tasks_started: u64,
    _window_observer: Option<Subscription>,
    _window_close_subscription: Option<Subscription>,
}
impl HerdrSessionRegistry {
    pub(crate) fn global(cx: &App) -> Entity<Self> {
        cx.global::<GlobalHerdrSessionRegistry>().0.clone()
    }

    pub(crate) fn try_global(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<GlobalHerdrSessionRegistry>()
            .map(|global| global.0.clone())
    }

    pub(crate) fn init(cx: &mut App) {
        let registry = cx.new(HerdrSessionRegistry::new);
        let close_subscription = cx.on_window_closed({
            let registry = registry.clone();
            move |cx, window_id| {
                registry.update(cx, |registry, cx| {
                    registry.note_window_released(window_id, cx);
                });
            }
        });
        registry.update(cx, |registry, _| {
            registry._window_close_subscription = Some(close_subscription);
        });
        cx.set_global(GlobalHerdrSessionRegistry(registry));
    }

    pub(crate) fn new(cx: &mut Context<Self>) -> Self {
        let registry = cx.entity();
        let observer = cx.observe_new(move |_multi_workspace: &mut MultiWorkspace, window, cx| {
            let Some(window) = window else {
                return;
            };
            let Some(handle) = window.window_handle().downcast::<MultiWorkspace>() else {
                return;
            };
            registry.update(cx, |registry, cx| registry.handle_new_window(handle, cx));
        });
        Self {
            windows: HashMap::default(),
            prompted: HashSet::default(),
            prompt_pending: HashSet::default(),
            connections: HashMap::default(),
            attempts: HashMap::default(),
            in_flight: HashMap::default(),
            snapshot_bootstrapped: HashSet::default(),
            slot_generations: HashMap::default(),
            finalize_tokens: HashMap::default(),
            snapshot_import_abort_handles: HashMap::default(),
            workspace_root_locks: HashMap::default(),
            next_generation: 0,
            sync: AgentSyncState::default(),
            mirror_index: MirrorIndex::default(),
            mirrors: HashMap::default(),
            observed_panels: HashSet::default(),
            panel_subscriptions: Vec::new(),
            mirroring_in_flight: HashSet::default(),
            pending_mirror_replay: HashMap::default(),
            focus_echoes: HashMap::default(),
            #[cfg(test)]
            sink_effects: None,
            #[cfg(test)]
            mirror_target_sink: None,
            host_sink: Rc::new(NoopHerdrHostSink),
            picker_sink: Rc::new(NoopSessionPickerSink),
            gateway: None,
            connection_tasks_started: 0,
            _window_observer: Some(observer),
            _window_close_subscription: None,
        }
    }

    pub(crate) fn gateway(&mut self) -> anyhow::Result<HerdrGateway> {
        if let Some(gateway) = self.gateway.clone() {
            return Ok(gateway);
        }
        let program = herdr_program()
            .ok_or_else(|| anyhow::anyhow!("herdr executable was not found in PATH"))?;
        let gateway = HerdrGateway::production(program);
        self.gateway = Some(gateway.clone());
        Ok(gateway)
    }
    fn program(&self) -> PathBuf {
        herdr_program().unwrap_or_else(|| PathBuf::from("herdr"))
    }

    // ------------------------------------------------------------ lifecycle

    pub(crate) fn binding_state(&self, window_id: WindowId) -> BindingState {
        self.windows
            .get(&window_id.as_u64())
            .map(|binding| binding.state.clone())
            .unwrap_or(BindingState::Unselected)
    }


    fn handle_new_window(
        &mut self,
        handle: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        let _window_id = self.register_window(handle);
        // Read the new window's roots once construction finishes. The
        // entity is still being built while this observer runs, so reading
        // it here would panic; the spawned update lands after the flush.
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            let _ = registry.update(cx, |registry, cx| registry.refresh_window_roots(cx));
        })
        .detach();
        self.schedule_startup_prompt(handle, cx);
    }

    fn register_window(&mut self, handle: WindowHandle<MultiWorkspace>) -> WindowId {
        let window_id = handle.window_id();
        self.windows
            .entry(window_id.as_u64())
            .or_insert_with(|| WindowBinding {
                window: handle,
                state: BindingState::Unselected,
                checkout_path: None,
                roots: Vec::new(),
            });
        window_id
    }

    /// Startup prompt: exactly once per ordinary window, after restoration
    /// and window creation settle. Cancellation writes no binding and starts
    /// no task.
    fn schedule_startup_prompt(
        &mut self,
        handle: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        if self.prompted.contains(&handle.window_id()) {
            return;
        }
        self.prompted.insert(handle.window_id());
        self.prompt_pending.insert(handle.window_id());
        let registry = cx.entity();
        cx.spawn(async move |_this, cx: &mut AsyncApp| {
            cx.background_executor().timer(PROMPT_SETTLE_DELAY).await;
            let state = registry.read_with(cx, |registry, _| {
                (
                    registry
                        .windows
                        .get(&handle.window_id().as_u64())
                        .map(|binding| binding.state.clone()),
                    registry.prompt_pending.contains(&handle.window_id()),
                )
            });
            let (state, pending) = state;
            if !pending {
                return;
            }
            match state {
                Some(BindingState::Unselected) | None => {}
                Some(_) => return,
            }
            let should_show = registry.update(cx, |registry, _| {
                registry.prompt_pending.remove(&handle.window_id())
            });
            if !should_show {
                return;
            }
            let picker_sink = registry.read_with(cx, |registry, _| registry.picker_sink.clone());
            picker_sink.show(handle, registry, cx);
        })
        .detach();
    }
    fn prompt_completed(&mut self, window_id: WindowId) {
        self.prompt_pending.remove(&window_id);
        self.prompted.insert(window_id);
    }

    fn apply_binding_event(
        &mut self,
        window_id: WindowId,
        event: BindingEvent,
        cx: &mut Context<Self>,
    ) {
        if !matches!(&event, BindingEvent::Selected { .. }) {
            self.abort_snapshot_import(window_id);
            self.finalize_tokens.remove(&window_id.as_u64());
        }
        let Some(binding) = self.windows.get_mut(&window_id.as_u64()) else {
            return;
        };
        binding.state = transition(binding.state.clone(), event);
        cx.notify();
    }

    /// Show the selected central host in a window. The host reads binding
    /// state from the registry and owns no connection itself.
    fn install_host(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
        cx: &mut Context<Self>,
    ) {
        let Some(handle) = self
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window)
        else {
            return;
        };
        let launch = HerdrLaunch {
            session_name,
            executable: self.program(),
        };
        self.host_sink.install(handle, launch, cx);
    }

    // ------------------------------------------------------------- actions

    /// `herdr: Select or Create Session`: open the picker in the invoking
    /// window. An unselected window is never given a default host here.
    pub(crate) fn select_or_create_from_app(cx: &mut App) {
        let Some(window) = cx
            .active_window()
            .and_then(|window| window.downcast::<MultiWorkspace>())
        else {
            return;
        };
        Self::select_or_create_for_window(window.window_id(), cx);
    }

    pub(crate) fn select_or_create_for_window(window_id: WindowId, cx: &mut App) {
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        let Some(handle) = registry.read_with(cx, |registry, _| {
            registry
                .windows
                .get(&window_id.as_u64())
                .map(|binding| binding.window)
        }) else {
            return;
        };
        let picker_sink = registry.update(cx, |registry, _| {
            // Any explicit selection attempt is this window's once-prompted
            // interaction; the startup observer will not ask again.
            registry.prompt_completed(window_id);
            registry.picker_sink.clone()
        });
        let mut async_cx = cx.to_async();
        cx.foreground_executor()
            .spawn(async move {
                picker_sink.show(handle, registry, &mut async_cx);
            })
            .detach();
    }

    /// `herdr: Resync Agents` from the active window.
    pub(crate) fn resync_from_app(cx: &mut App) {
        let Some(window) = cx
            .active_window()
            .and_then(|window| window.downcast::<MultiWorkspace>())
        else {
            return;
        };
        let window_id = window.window_id();
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        registry.update(cx, |registry, cx| registry.resync_agents(window_id, cx));
    }

    /// `herdr: Disconnect Session` from the active window.
    pub(crate) fn disconnect_from_app(cx: &mut App) {
        let Some(window) = cx
            .active_window()
            .and_then(|window| window.downcast::<MultiWorkspace>())
        else {
            return;
        };
        let window_id = window.window_id();
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        registry.update(cx, |registry, cx| {
            registry.disconnect_session(window_id, cx)
        });
    }

    /// The `herdr --session` central client terminal of a window exited
    /// while the connection attempt was still starting.
    pub(crate) fn note_host_exit_for_window(window_id: WindowId, cx: &mut App) {
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        registry.update(cx, |registry, cx| {
            if !matches!(
                registry.binding_state(window_id),
                BindingState::Starting { .. }
            ) {
                return;
            }
            registry.apply_binding_event(
                window_id,
                BindingEvent::SessionStillRunning("herdr process exited".into()),
                cx,
            );
        });
    }

    /// Apply a picker confirmation after the picker has validated the
    /// selection. Cancellation never calls this method, so it cannot create
    /// a binding, host, or connection task.
    pub(crate) fn apply_selection_for_window(
        &mut self,
        invoking: WindowHandle<MultiWorkspace>,
        selection: SessionSelection,
        cx: &mut Context<Self>,
    ) {
        let window_id = invoking.window_id();
        self.prompt_completed(window_id);
        if matches!(
            self.binding_state(window_id),
            BindingState::Starting { .. }
                | BindingState::Connected(_)
                | BindingState::Failed { .. }
        ) {
            self.disconnect_session(window_id, cx);
        }
        let name: Arc<str> = Arc::from(selection.name());
        self.start_binding_for_window(window_id, name, cx);
    }

    fn start_binding_for_window(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
        cx: &mut Context<Self>,
    ) {
        self.abort_snapshot_import(window_id);
        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        self.attempts.insert(window_id.as_u64(), generation);
        self.finalize_tokens.remove(&window_id.as_u64());
        let name = session_name;
        self.apply_binding_event(
            window_id,
            BindingEvent::Selected {
                session_name: name.clone(),
            },
            cx,
        );
        // The window entity may still be under construction (root observer);
        // the host install and the connection task's first read both run one
        // cycle later.
        let registry = cx.entity();
        cx.defer(move |cx| {
            registry.update(cx, |registry, cx| {
                if !registry.attempt_is_current(window_id, generation, &name) {
                    return;
                }
                registry.install_host(window_id, name.clone(), cx);
                let gateway = match registry.gateway() {
                    Ok(gateway) => gateway,
                    Err(error) => {
                        log::error!("herdr gateway unavailable: {error:#}");
                        registry.apply_binding_event(
                            window_id,
                            BindingEvent::SessionStillRunning(
                                "herdr executable was not found in PATH".into(),
                            ),
                            cx,
                        );
                        return;
                    }
                };
                registry.begin_connection(window_id, name, generation, gateway, cx);
            });
        });
    }


    // ---------------------------------------------------------- connection

    /// Start (or restart) the bounded connection loop for one window. This
    /// is the only place a connection task is spawned; stream loss never
    /// calls it again.
    fn begin_connection(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
        generation: u64,
        gateway: HerdrGateway,
        cx: &mut Context<Self>,
    ) {
        let Some(handle) = self
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window)
        else {
            return;
        };
        self.connection_tasks_started = self.connection_tasks_started.saturating_add(1);
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            run_connection(
                registry,
                gateway,
                handle,
                window_id,
                session_name,
                generation,
                cx.clone(),
            )
            .await;
        })
        .detach();
    }

    fn attempt_is_current(&self, window_id: WindowId, generation: u64, session_name: &str) -> bool {
        self.attempts.get(&window_id.as_u64()) == Some(&generation)
            && matches!(
                self.windows.get(&window_id.as_u64()).map(|binding| &binding.state),
                Some(BindingState::Starting { session_name: current }) if current.as_ref() == session_name
            )
    }
    /// A snapshot import may mutate a window only while this exact binding
    /// attempt still owns its finalize token, connection slot, and window.
    fn snapshot_import_is_current(
        &self,
        window_id: WindowId,
        identity: &SessionIdentity,
        session_name: &str,
        generation: u64,
    ) -> bool {
        self.attempt_is_current(window_id, generation, session_name)
            && self.finalize_tokens.get(&window_id.as_u64()) == Some(&generation)
            && self
                .connections
                .get(identity)
                .is_some_and(|connection| {
                    connection.bound_windows.contains(&window_id.as_u64())
                        && self
                            .slot_generations
                            .get(&(identity.clone(), window_id.as_u64()))
                            == Some(&generation)
                })
    }
    fn abort_snapshot_import(&mut self, window_id: WindowId) {
        if let Some(handle) = self
            .snapshot_import_abort_handles
            .remove(&window_id.as_u64())
        {
            handle.abort();
        }
    }

    fn clear_snapshot_import_abort_if_current(
        &mut self,
        window_id: WindowId,
        generation: u64,
    ) {
        if self.attempts.get(&window_id.as_u64()) == Some(&generation)
            && self.finalize_tokens.get(&window_id.as_u64()) == Some(&generation)
        {
            self.snapshot_import_abort_handles
                .remove(&window_id.as_u64());
        }
    }



    /// `Retry` re-runs the bounded connect loop for the failed session.
    pub(crate) fn retry(
        &mut self,
        window_id: WindowId,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        let BindingState::Failed { session_name, .. } = self.binding_state(window_id) else {
            return Ok(());
        };
        let _ = self.gateway()?;
        self.start_binding_for_window(window_id, session_name, cx);
        Ok(())
    }

    /// `DisconnectSession` removes only the invoking window binding, closes
    /// that binding's local client terminals, and detaches host
    /// presentation to the unselected surface. While another window stays
    /// bound, the shared client and event stream remain open; the last
    /// disconnect drops them. No agent-stop RPC is sent, and regular shells
    /// opened for Herdr agents remain available.
    pub(crate) fn disconnect_session(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        if !self.windows.contains_key(&window_id.as_u64()) {
            return;
        }
        self.abort_snapshot_import(window_id);
        self.attempts.remove(&window_id.as_u64());
        self.finalize_tokens.remove(&window_id.as_u64());
        let identities: Vec<SessionIdentity> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.bound_windows.contains(&window_id.as_u64()))
            .map(|(identity, _)| identity.clone())
            .collect();
        if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
            binding.state = BindingState::Unselected;
            binding.checkout_path = None;
        }
        let handle = self
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window);
        if let Some(handle) = handle {
            self.host_sink.detach(handle, cx);
        }
        for identity in identities {
            self.detach_window(&identity, window_id, cx);
        }
        cx.notify();
    }

    /// Remove one window's binding from a session; when it was the last
    /// bound window, dropping the connection releases the client and
    /// synchronization ownership.
    fn detach_window(
        &mut self,
        identity: &SessionIdentity,
        window_id: WindowId,
        cx: &mut Context<Self>,
    ) {
        self.slot_generations
            .remove(&(identity.clone(), window_id.as_u64()));
        self.prune_mirrors_for_window(window_id, cx);
        self.workspace_root_locks
            .retain(|(lock_window, _), _| *lock_window != window_id.as_u64());
        let connection_generation = self
            .connections
            .get(identity)
            .map(|connection| connection.generation);
        let released = self
            .connections
            .get_mut(identity)
            .is_some_and(|connection| {
                connection.bound_windows.remove(&window_id.as_u64());
                connection.bound_windows.is_empty()
            });
        if released {
            if let Some(generation) = connection_generation {
                self.snapshot_bootstrapped
                    .remove(&(identity.clone(), generation));
            }
            self.connections.remove(identity);
            self.focus_echoes.remove(identity);
            let effects = self.sync.forget_session(identity);
            self.dispatch_effects(identity, effects, None, cx);
            cx.notify();
        }
    }

    /// `herdr: Resync Agents`: clear suppression and replay the session's
    /// live records through the deduplicated synchronization path.
    pub(crate) fn resync_agents(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        let BindingState::Connected(identity) = self.binding_state(window_id) else {
            return;
        };
        let Some(client) = self
            .connections
            .get(&identity)
            .map(|connection| connection.client.clone())
        else {
            return;
        };
        let effects = self.sync.resync(&identity);
        self.dispatch_effects(&identity, effects, None, cx);
        cx.notify();
        let registry = cx.entity();
        // A fresh snapshot through the existing shared connection keeps the
        // mirror index current; reducer revision guards deduplicate it.
        cx.spawn(async move |_this, cx| {
            let Ok(snapshot) = client.snapshot().await else {
                return;
            };
            let _ = registry.update(cx, |registry, cx| {
                let effects = import_snapshot(&mut registry.sync, &identity, &snapshot);
                registry.dispatch_effects(&identity, effects, None, cx);
                cx.notify();
            });
        })
        .detach();
    }

    fn dispatch_effects(
        &mut self,
        identity: &SessionIdentity,
        effects: Vec<AgentSyncEffect>,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        for effect in plan_effects(effects) {
            #[cfg(test)]
            if let Some(sink) = self.sink_effects.as_ref() {
                sink.push(identity, &effect);
            }
            self.apply_workspace_effect(identity, effect, target, cx);
        }
    }
    /// Snapshot bootstrap is claimed once per connection generation by the
    /// first bound window that completes finalization. This makes replay
    /// ownership transferable when the owner's workspace import times out.
    fn dispatch_snapshot_effects(
        &mut self,
        identity: &SessionIdentity,
        snapshot: &SessionSnapshot,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        let generation = target
            .map(|target| target.generation)
            .or_else(|| {
                self.connections
                    .get(identity)
                    .map(|connection| connection.generation)
            });
        let is_fresh_generation = generation.is_some_and(|generation| {
            self.snapshot_bootstrapped
                .insert((identity.clone(), generation))
        });
        let effects = if is_fresh_generation {
            import_snapshot_for_new_generation(&mut self.sync, identity, snapshot)
        } else {
            Vec::new()
        };
        let routed_keys: HashSet<AgentKey> = effects
            .iter()
            .filter_map(|effect| match effect {
                AgentSyncEffect::Open(record) => Some(record.key.clone()),
                AgentSyncEffect::PaneMoved { key, .. } => Some(key.clone()),
                AgentSyncEffect::Focus(_) | AgentSyncEffect::Forget(_) => None,
            })
            .collect();
        self.dispatch_effects(identity, effects, target, cx);
        if !is_fresh_generation {
            return;
        }
        for record in self
            .sync
            .live_records(identity)
            .into_iter()
            .filter(|record| !routed_keys.contains(&record.key))
        {
            self.mirror_agent(record, target, cx);
        }
    }



    fn mirror_target_window(
        &self,
        key: &AgentKey,
        target: MirrorTarget,
    ) -> Option<WindowHandle<MultiWorkspace>> {
        let connection = self.connections.get(&key.session)?;
        if connection.generation != target.generation
            || !connection
                .bound_windows
                .contains(&target.window_id.as_u64())
        {
            return None;
        }
        self.windows
            .get(&target.window_id.as_u64())
            .map(|binding| binding.window)
    }

    /// Resolve a fallback workspace for a mirror failure. Exact connection
    /// ownership wins over any same-name in-progress selection. A `Starting`
    /// fallback is valid only for this connection generation, so a stale
    /// attempt cannot steal a notification from its successor.
    fn source_window_for_session(
        &self,
        identity: &SessionIdentity,
        generation: u64,
    ) -> Option<WindowHandle<MultiWorkspace>> {
        if let Some(connection) = self.connections.get(identity) {
            if connection.generation == generation {
                if let Some(window) = connection.bound_windows.iter().find_map(|window_id| {
                    self.windows.get(window_id).and_then(|binding| {
                        matches!(
                            &binding.state,
                            BindingState::Connected(bound) if bound == identity
                        )
                        .then_some(binding.window)
                    })
                }) {
                    return Some(window);
                }
            }
        }
        if let Some(window) = self.windows.values().find_map(|binding| {
            matches!(
                &binding.state,
                BindingState::Connected(bound) if bound == identity
            )
            .then_some(binding.window)
        }) {
            return Some(window);
        }
        self.windows.iter().find_map(|(window_id, binding)| {
            (self.attempts.get(window_id) == Some(&generation)
                && matches!(
                    &binding.state,
                    BindingState::Starting { session_name }
                        if session_name.as_ref() == identity.name.as_ref()
                ))
            .then_some(binding.window)
        })
    }



    fn register_panel_observer(
        &mut self,
        panel: Entity<AgentPanel>,
        window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        if !self.observed_panels.insert(panel.entity_id()) {
            return;
        }
        let subscription = cx.subscribe(
            &panel,
            move |registry: &mut HerdrSessionRegistry,
                  panel: Entity<AgentPanel>,
                  event: &AgentPanelEvent,
                  cx: &mut Context<HerdrSessionRegistry>| {
                match event {
                    AgentPanelEvent::ActiveViewFocused | AgentPanelEvent::ActiveViewChanged => {
                        let identity =
                            panel.read(cx).active_terminal_id().and_then(|terminal_id| {
                                panel
                                    .read(cx)
                                    .external_terminal_identity(terminal_id)
                                    .map(str::to_owned)
                            });
                        if let Some(identity) = identity {
                            if let Some(agent) = registry.agent_for_external_identity(&identity) {
                                registry.focus_herdr_agent(agent, cx);
                            }
                        }
                    }
                    AgentPanelEvent::EntryChanged => {
                        registry.detect_closed_mirrors(&panel, window, cx);
                    }
                    AgentPanelEvent::ExternalTerminalSpawnFinished { identity, message } => {
                        registry.handle_external_spawn_finished(
                            identity.clone(),
                            message.clone(),
                            &panel,
                            window,
                            cx,
                        );
                    }
                    AgentPanelEvent::ExternalTerminalAttachFinished { identity, message } => {
                        registry.handle_external_attach_finished(
                            identity.clone(),
                            message.clone(),
                            &panel,
                            window,
                            cx,
                        );
                    }
                    AgentPanelEvent::TerminalCloseRequested { .. }
                    | AgentPanelEvent::ThreadInteracted { .. } => {}
                }
            },
        );
        self.panel_subscriptions.push(subscription);
    }

    fn external_identity(key: &AgentKey) -> String {
        format!(
            "herdr:{}:{}",
            key.session.session_dir.display(),
            key.terminal_id
        )
    }

    pub(crate) fn agent_for_external_identity(&self, identity: &str) -> Option<AgentKey> {
        self.mirror_index
            .iter()
            .find_map(|(key, _)| (Self::external_identity(key) == identity).then_some(key.clone()))
    }

    fn focus_local_agent(&mut self, key: &AgentKey, cx: &mut Context<Self>) {
        let Some(mirror) = self.mirrors.get(key).cloned() else {
            return;
        };
        // Arm the echo before activating: the activation emits
        // `ActiveViewChanged`, whose observer must observe this pending
        // Agent target and acknowledge it instead of re-initiating an
        // outbound `agent.focus` RPC (symmetric to the workspace half).
        let token = {
            let echo = self.focus_echoes.entry(key.session.clone()).or_default();
            echo.request(FocusTarget::Agent(key.clone()))
        };
        let _ = mirror.window.update(cx, |_, window, cx| {
            if let Some(panel) = mirror.panel.upgrade() {
                panel.update(cx, |panel, cx| {
                    panel.activate_terminal(mirror.terminal_id, true, window, cx);
                });
            }
        });
        // Panel events are delivered after this update returns. If the
        // activation reflected no `ActiveViewChanged` at all (the terminal
        // was already the active view), nothing consumes the arm; release
        // it in a follow-up update so it cannot swallow a later genuinely
        // external focus of the same agent.
        let registry = cx.entity();
        let session = key.session.clone();
        cx.spawn(async move |_this, cx| {
            let _ = registry.update(cx, |registry, _| {
                if let Some(echo) = registry.focus_echoes.get_mut(&session) {
                    echo.resolve(token);
                }
            });
        })
        .detach();
    }

    /// Reconcile only the mirrors that live in the emitting panel, and only
    /// while the herdr record is still live: a missing terminal whose agent
    /// still exists in the reducer is a user close and gets dismissed.
    /// Mirrors in other windows are never touched by this event.
    fn detect_closed_mirrors(
        &mut self,
        panel: &Entity<AgentPanel>,
        _window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        let panel_id = panel.entity_id();
        let missing: Vec<AgentKey> = self
            .mirrors
            .iter()
            .filter(|(_, mirror)| mirror.panel.entity_id() == panel_id)
            .filter(|(key, _)| self.sync.record(key).is_some())
            .filter(|(_, mirror)| !panel.read(cx).has_terminal(mirror.terminal_id))
            .map(|(key, _)| key.clone())
            .collect();
        for key in missing {
            self.forget_mirror(&key);
            self.sync.dismiss(key);
        }
    }

    /// Drop every trace of one mirror's panel state without touching the
    /// reducer (the caller decides dismissal vs. live suppression).
    fn forget_mirror(&mut self, key: &AgentKey) {
        self.mirror_index.remove(key);
        self.mirrors.remove(key);
    }

    fn handle_external_spawn_finished(
        &mut self,
        identity: SharedString,
        message: Option<SharedString>,
        panel: &Entity<AgentPanel>,
        _window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        let Some(key) = self.agent_for_external_identity(&identity) else {
            return;
        };
        let Some(record) = self.sync.record(&key) else {
            return;
        };
        match message {
            None => {
                if self
                    .mirrors
                    .get(&key)
                    .is_some_and(|mirror| panel.read(cx).has_terminal(mirror.terminal_id))
                {
                    self.sync.clear_failure(&key);
                }
            }
            Some(message) => {
                // The registry owns mirror-failure reporting end to end: one
                // composite-id notification per failing agent. The panel's
                // generic error slot is per-workspace and would collapse two
                // agents' failures into one toast.
                let revision = record.revision;
                self.report_mirror_failure(&key, revision, message.to_string(), None, cx);
            }
        }
    }

    /// The attach process behind a tracked mirror terminal exited. A clean
    /// exit means the pane is gone (the server event releases ownership
    /// separately); a failed exit is the degraded-CLI case and must gate
    /// retries on the current revision and surface exactly one actionable
    /// notification, never a re-spawn loop.
    fn handle_external_attach_finished(
        &mut self,
        identity: SharedString,
        message: Option<SharedString>,
        _panel: &Entity<AgentPanel>,
        _window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        let Some(key) = self.agent_for_external_identity(&identity) else {
            return;
        };
        let Some(record) = self.sync.record(&key) else {
            return;
        };
        let Some(message) = message else {
            return;
        };
        let revision = record.revision;
        self.report_mirror_failure(&key, revision, message.to_string(), None, cx);
    }

    pub(crate) fn focus_herdr_agent(&mut self, key: AgentKey, cx: &mut Context<Self>) {
        let Some(record) = self.sync.record(&key) else {
            return;
        };
        let Some(client) = self
            .connections
            .get(&key.session)
            .map(|connection| connection.client.clone())
        else {
            return;
        };
        let target = FocusTarget::Agent(key.clone());
        let echo = self.focus_echoes.entry(key.session.clone()).or_default();
        if echo.observe(target.clone()) == FocusObservation::Echo {
            return;
        }
        let token = echo.request(target);
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            if let Err(error) = client.focus_agent(record.pane_id.to_string()).await {
                log::debug!("herdr agent focus failed: {error:#}");
            }
            let _ = registry.update(cx, |registry, _| {
                if let Some(echo) = registry.focus_echoes.get_mut(&key.session) {
                    echo.resolve(token);
                }
            });
        })
        .detach();
    }

    /// Forward a local worktree activation to herdr. `workspace_id` must be
    /// the server's `WorkspaceInfo.workspace_id` for the window's checkout;
    /// an unmapped workspace never addresses a herdr session. The checkout
    /// is re-read from the window's live worktree set first so a workspace
    /// switch or worktree add is reflected immediately.
    pub(crate) fn focus_herdr_workspace(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        self.refresh_window_roots(cx);
        let Some((identity, checkout)) =
            self.windows
                .get(&window_id.as_u64())
                .and_then(|binding| match &binding.state {
                    BindingState::Connected(identity) => {
                        Some((identity.clone(), binding.checkout_path.clone()?))
                    }
                    _ => None,
                })
        else {
            return;
        };
        let Some(workspace_id) = self
            .sync
            .workspace_id_for_checkout(&identity, Path::new(checkout.as_str()))
        else {
            return;
        };
        let target = FocusTarget::Workspace {
            session: identity.clone(),
            workspace_id: workspace_id.clone(),
        };
        let echo = self.focus_echoes.entry(identity.clone()).or_default();
        if echo.observe(target.clone()) == FocusObservation::Echo {
            return;
        }
        let token = echo.request(target);
        let Some(client) = self
            .connections
            .get(&identity)
            .map(|connection| connection.client.clone())
        else {
            return;
        };
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            if let Err(error) = client.focus_workspace(workspace_id.to_string()).await {
                log::debug!("herdr workspace focus failed: {error:#}");
            }
            let _ = registry.update(cx, |registry, _| {
                if let Some(echo) = registry.focus_echoes.get_mut(&identity) {
                    echo.resolve(token);
                }
            });
        })
        .detach();
    }

    /// A remote workspace focus activates the mapped worktree window exactly
    /// once. The mapping is the workspace's recorded checkout resolved
    /// against live window roots: a connected window holding *no* detected
    /// agents is still focusable. The reflected event either resolves the
    /// pending echo for the matching target or, when none exists, records
    /// nothing: activation never loops because it sends no RPC back.
    fn focus_remote_workspace(
        &mut self,
        identity: &SessionIdentity,
        workspace_id: &str,
        cx: &mut Context<Self>,
    ) {
        let target = FocusTarget::Workspace {
            session: identity.clone(),
            workspace_id: Arc::from(workspace_id),
        };
        let echo = self.focus_echoes.get_mut(identity);
        if let Some(echo) = echo {
            if echo.observe(target) == FocusObservation::Echo {
                return;
            }
        }
        let Some(checkout) = self.sync.workspace_checkout(identity, workspace_id) else {
            return;
        };
        let Ok(root) = canonical_checkout_path(&checkout) else {
            return;
        };
        self.refresh_window_roots(cx);
        let window = self
            .windows
            .values()
            .filter(|binding| {
                matches!(&binding.state, BindingState::Connected(bound) if bound == identity)
            })
            .find(|binding| binding.roots.contains(&root))
            .map(|binding| binding.window);
        if let Some(window) = window {
            let _ = window.update(cx, |_, window, _| window.activate_window());
        }
    }

    /// checkout there in order, then refresh routing roots before agents run.
    fn apply_workspace_effect(
        &mut self,
        identity: &SessionIdentity,
        effect: WorkspaceEffect,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        match effect {
            WorkspaceEffect::Open(record) => self.mirror_agent(record, target, cx),
            WorkspaceEffect::PaneMoved { key, .. } => {
                // A pane move never creates a second thread and never
                // leaves zero tracked ones: an existing tracked mirror is
                // re-activated in place; otherwise the move re-enters the
                // normal open path, whose in-flight gate replays the
                // latest record when a concurrent attempt completes.
                if let Some(mirror) = self.mirrors.get(&key).cloned() {
                    let _ = mirror.window.update(cx, |_, window, cx| {
                        if let Some(panel) = mirror.panel.upgrade() {
                            panel.update(cx, |panel, cx| {
                                panel.activate_terminal(mirror.terminal_id, false, window, cx);
                            });
                        }
                    });
                } else if let Some(record) = self.sync.record(&key) {
                    self.mirror_agent(record, target, cx);
                }
            }
            WorkspaceEffect::Focus(key) => self.focus_local_agent(&key, cx),
            WorkspaceEffect::Forget(key) => {
                // Ownership was already dropped by the reducer; releasing the
                // panel bookkeeping must never close the mirrored terminal.
                self.forget_mirror(&key);
            }
        }
        let _ = identity;
    }

    fn workspace_root_lock(
        &mut self,
        window_id: WindowId,
        root: &herdr::CanonicalPath,
    ) -> Arc<Mutex<()>> {
        self.workspace_root_locks
            .entry((window_id.as_u64(), root.clone()))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Open (or re-activate) the mirrored Agent Panel terminal for one live
    /// agent record. The captured connection generation invalidates the
    /// whole attempt once its session connection is superseded or released.
    fn mirror_agent(
        &mut self,
        record: AgentRecord,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        let Some(generation) = self
            .connections
            .get(&record.key.session)
            .map(|connection| connection.generation)
        else {
            log::debug!(
                "herdr agent {} lost its connection before mirroring",
                record.key.terminal_id
            );
            return;
        };
        let target = target.filter(|target| target.generation == generation);
        let candidate = record
            .checkout_path
            .clone()
            .or_else(|| record.effective_cwd.clone());
        let Some(candidate) = candidate else {
            self.report_mirror_failure(
                &record.key,
                record.revision,
                "herdr agent has no checkout path or current directory".into(),
                target,
                cx,
            );
            return;
        };
        if !candidate.is_absolute() {
            self.report_mirror_failure(
                &record.key,
                record.revision,
                format!("herdr agent path is not absolute: {}", candidate.display()),
                target,
                cx,
            );
            return;
        }
        let flight_key = (record.key.clone(), generation);
        if !self.mirroring_in_flight.insert(flight_key.clone()) {
            match self.pending_mirror_replay.get(&flight_key) {
                Some((pending, _)) if pending.revision >= record.revision => {}
                _ => {
                    self.pending_mirror_replay
                        .insert(flight_key, (record.clone(), target));
                }
            }
            return;
        }

        let fs = <dyn Fs>::global(cx).clone();
        let registry = cx.entity();
        cx.spawn(async move |_this, mut cx| {
            let fail =
                |registry: &Entity<HerdrSessionRegistry>, cx: &mut AsyncApp, message: String| {
                    let key = record.key.clone();
                    let revision = record.revision;
                    let _ = registry.update(cx, |registry, cx| {
                        if registry.connection_matches(&key.session, generation) {
                            registry.report_mirror_failure(&key, revision, message, target, cx);
                        }
                        registry.finish_mirror_flight(&key, generation, target, cx);
                    });
                };
            let owned = registry.read_with(cx, |registry, _| {
                registry.connection_matches(&record.key.session, generation)
            });
            if !owned {
                let _ = registry.update(cx, |registry, cx| {
                    registry.finish_mirror_flight(&record.key, generation, target, cx);
                });
                return;
            }
            let metadata = fs.metadata(&candidate).await;
            let valid_directory = metadata
                .as_ref()
                .is_ok_and(|metadata| metadata.as_ref().is_some_and(|metadata| metadata.is_dir));
            if !valid_directory {
                let message = match metadata {
                    Ok(None) => format!(
                        "herdr agent worktree does not exist: {}",
                        candidate.display()
                    ),
                    Ok(Some(_)) => format!(
                        "herdr agent worktree is not a directory: {}",
                        candidate.display()
                    ),
                    Err(error) => format!("could not inspect herdr agent worktree: {error:#}"),
                };
                fail(&registry, &mut cx, message);
                return;
            }
            // Resolve the nearest checkout root rather than routing through
            // Git's common `.git` directory. This preserves linked-worktree
            // identity and lets nested agent cwd paths reuse their checkout
            // window.
            let root = discover_agent_checkout_root(&candidate, fs.as_ref()).await;
            let Ok(root) = canonical_checkout_path(&root) else {
                fail(
                    &registry,
                    &mut cx,
                    "herdr agent worktree could not be canonicalized".into(),
                );
                return;
            };
            let Some((window, root_lock)) = registry.update(cx, |registry, cx| {
                if !registry.connection_matches(&record.key.session, generation) {
                    return None;
                }
                registry.refresh_window_roots(cx);
                let window = target
                    .and_then(|target| registry.mirror_target_window(&record.key, target))
                    .or_else(|| registry.source_window_for_session(&record.key.session, generation))?;
                let needs_add = registry
                    .windows
                    .get(&window.window_id().as_u64())
                    .map(|binding| {
                        !binding.roots.iter().any(|candidate| {
                            candidate == &root
                                || Path::new(root.as_str())
                                    .strip_prefix(Path::new(candidate.as_str()))
                                    .is_ok_and(|suffix| !suffix.as_os_str().is_empty())
                        })
                    })
                    .unwrap_or(true);
                let root_lock =
                    needs_add.then(|| registry.workspace_root_lock(window.window_id(), &root));
                Some((window, root_lock))
            }) else {
                fail(
                    &registry,
                    &mut cx,
                    "no invoking Zed worktree window is available for herdr agent".into(),
                );
                return;
            };
            if let Some(root_lock) = root_lock {
                let _root_lock = root_lock.lock().await;
                if let Err(error) = add_workspace_root(window, root.clone(), &mut cx).await {
                    fail(
                        &registry,
                        &mut cx,
                        format!(
                            "could not import herdr agent worktree {}: {error:#}",
                            root.as_str()
                        ),
                    );
                    return;
                }
                let _ = registry.update(cx, |registry, cx| {
                    if registry.connection_matches(&record.key.session, generation) {
                        registry.refresh_window_roots(cx);
                    }
                });
            }
            let _ = registry.update(cx, |registry, cx| {
                if registry.connection_matches(&record.key.session, generation) {
                    registry.open_mirror_in_window(record, window, generation, target, cx);
                }
            });
        })
        .detach();
    }

    /// Install (or re-activate) one agent's mirrored terminal inside an
    /// already-open window. Repeated calls for the same key while one is in
    /// flight are dropped; the connection generation invalidates the attempt
    /// once its session was superseded or released.
    fn open_mirror_in_window(
        &mut self,
        record: AgentRecord,
        window: WindowHandle<MultiWorkspace>,
        generation: u64,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        #[cfg(test)]
        if let Some(sink) = &self.mirror_target_sink {
            sink.borrow_mut()
                .push((record.key.clone(), window.window_id()));
        }
        let registry = cx.entity();
        let herdr_program = self.program();
        cx.spawn(async move |_this, mut cx| {
            let done = |registry: &Entity<HerdrSessionRegistry>, cx: &mut AsyncApp| {
                let key = record.key.clone();
                let _ = registry.update(cx, |registry, cx| {
                    registry.finish_mirror_flight(&key, generation, target, cx);
                });
            };
            let fail =
                |registry: &Entity<HerdrSessionRegistry>, cx: &mut AsyncApp, message: String| {
                    let key = record.key.clone();
                    let revision = record.revision;
                    let _ = registry.update(cx, |registry, cx| {
                        if registry.connection_matches(&key.session, generation) {
                            registry.report_mirror_failure(&key, revision, message, target, cx);
                        }
                        registry.finish_mirror_flight(&key, generation, target, cx);
                    });
                };
            let ensure = window.update(cx, |multi_workspace, window, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    if workspace.panel::<AgentPanel>(cx).is_some() {
                        Task::ready(Ok::<(), anyhow::Error>(()))
                    } else {
                        super::ensure_agent_panel_for_workspace(workspace, None, window, cx)
                    }
                })
            });
            let owned = registry.read_with(cx, |registry, _| {
                registry.connection_matches(&record.key.session, generation)
            });
            if !owned {
                done(&registry, &mut cx);
                return;
            }
            let Ok(ensure) = ensure else {
                fail(
                    &registry,
                    &mut cx,
                    "could not initialize the Agent Panel for herdr agent".into(),
                );
                return;
            };
            if ensure.await.is_err() {
                fail(
                    &registry,
                    &mut cx,
                    "could not initialize the Agent Panel for herdr agent".into(),
                );
                return;
            }
            let live = registry.read_with(cx, |registry, _| {
                registry.connection_matches(&record.key.session, generation)
                    && registry.sync.record(&record.key).is_some_and(|live| {
                        live.revision == record.revision && !registry.sync.is_dismissed(&record.key)
                    })
            });
            if !live {
                done(&registry, &mut cx);
                return;
            }
            let opened = window
                .update(cx, |multi_workspace, window, cx| {
                    let workspace = multi_workspace.workspace().clone();
                    workspace.update(cx, |workspace, cx| {
                        let panel = workspace.panel::<AgentPanel>(cx)?;
                        let panel_weak = panel.downgrade();
                        let target_window = window.window_handle().downcast::<MultiWorkspace>()?;
                        let terminal_id = panel.update(cx, |panel, cx| {
                            panel.open_external_terminal_thread(
                                super::herdr_agent_sync::external_terminal_spec(
                                    &record,
                                    &herdr_program,
                                ),
                                record.focused,
                                window,
                                cx,
                            )
                        });
                        Some((panel, panel_weak, terminal_id, target_window))
                    })
                })
                .ok()
                .flatten();
            let owned = registry.read_with(cx, |registry, _| {
                registry.connection_matches(&record.key.session, generation)
            });
            if !owned {
                done(&registry, &mut cx);
                return;
            }
            let _ = registry.update(cx, |registry, cx| {
                // A pane exit or dismissal during the async open must not
                // resurrect a mirror the reducer no longer owns. An equal-or-
                // newer live revision is committed under the latest record.
                let Some(live) = registry.sync.record(&record.key) else {
                    registry.finish_mirror_flight(&record.key, generation, target, cx);
                    return;
                };
                if live.revision < record.revision || registry.sync.is_dismissed(&record.key) {
                    registry.finish_mirror_flight(&record.key, generation, target, cx);
                    return;
                }
                let Some((panel, panel_weak, terminal_id, target_window)) = opened else {
                    registry.report_mirror_failure(
                        &record.key,
                        record.revision,
                        "could not initialize the Agent Panel for herdr agent".into(),
                        target,
                        cx,
                    );
                    registry.finish_mirror_flight(&record.key, generation, target, cx);
                    return;
                };
                registry.register_panel_observer(panel, target_window, cx);
                registry
                    .mirror_index
                    .insert(record.key.clone(), terminal_id);
                registry.mirrors.insert(
                    record.key.clone(),
                    AgentMirror {
                        window: target_window,
                        panel: panel_weak,
                        terminal_id,
                    },
                );
                registry.finish_mirror_flight(&record.key, generation, target, cx);
            });
        })
        .detach();
    }

    fn finish_mirror_flight(
        &mut self,
        key: &AgentKey,
        generation: u64,
        _target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        let flight_key = (key.clone(), generation);
        self.mirroring_in_flight.remove(&flight_key);
        if !self.connection_matches(&key.session, generation)
            || self.sync.record(key).is_none()
        {
            self.pending_mirror_replay.remove(&flight_key);
            return;
        }
        let Some((replay, replay_target)) = self.pending_mirror_replay.remove(&flight_key) else {
            return;
        };
        self.mirror_agent(replay, replay_target, cx);
    }

    fn record_mirror_failure(&mut self, key: &AgentKey, revision: u64) -> bool {
        if !self.sync.record_failure(key, revision) {
            return false;
        }
        self.forget_mirror(key);
        true
    }

    /// One actionable, actionable-text notification per agent revision. A
    /// failure is a retryable per-revision state, never a user close: the
    /// mirror bookkeeping is dropped (the dead terminal must not be
    /// reconciled as "closed by the user" later) but the reducer is not
    /// dismissed, so a later pane revision or explicit resync retries once.
    fn report_mirror_failure(
        &mut self,
        key: &AgentKey,
        revision: u64,
        message: String,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        if !self
            .sync
            .record(key)
            .is_some_and(|live| live.revision == revision)
        {
            return;
        }
        if !self.record_mirror_failure(key, revision) {
            return;
        }
        let generation = self
            .connections
            .get(&key.session)
            .map(|connection| connection.generation);
        let window = target
            .and_then(|target| self.mirror_target_window(key, target))
            .or_else(|| self.mirrors.get(key).map(|mirror| mirror.window))
            .or_else(|| {
                generation
                    .and_then(|generation| self.source_window_for_session(&key.session, generation))
            });
        if let Some(window) = window {
            let id = NotificationId::composite::<HerdrMirrorFailureNotification>(format!(
                "{}:{}",
                key.session.session_dir.display(),
                key.terminal_id
            ));
            let message: SharedString = message.into();
            let _ = window.update(cx, |multi_workspace, _, cx| {
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    workspace.show_notification(id, cx, |cx| {
                        cx.new(|cx| MessageNotification::new(message.clone(), cx))
                    });
                })
            });
        } else {
            log::error!("herdr mirror failure for {}: {message}", key.terminal_id);
        }
    }

    // --------------------------------------------------------- routing core


    /// Install the binding in the invoking window, import every snapshot
    fn finalize_binding(
        &mut self,
        bootstrap: WindowId,
        identity: &SessionIdentity,
        session_name: &str,
        generation: u64,
        snapshot: &SessionSnapshot,
        cx: &mut Context<Self>,
    ) -> FinalizeResult {
        self.refresh_window_roots(cx);
        let focused = focused_checkout_path(snapshot);
        if let Some(binding) = self.windows.get_mut(&bootstrap.as_u64()) {
            binding.checkout_path = focused;
        }
        self.install_binding_target(bootstrap, identity, generation, cx);
        let roots = snapshot_checkout_paths(snapshot);
        self.abort_snapshot_import(bootstrap);
        let task = self
            .windows
            .get(&bootstrap.as_u64())
            .map(|binding| binding.window)
            .map(|window| {
                let (abort_handle, abort_registration) = AbortHandle::new_pair();
                self.snapshot_import_abort_handles
                    .insert(bootstrap.as_u64(), abort_handle);
                let registry = cx.entity();
                let identity = identity.clone();
                let session_name: Arc<str> = Arc::from(session_name);
                cx.spawn(async move |_this, mut cx| {
                    let import = import_snapshot_workspaces_for_binding(
                        registry.clone(),
                        window,
                        bootstrap,
                        identity.clone(),
                        session_name.clone(),
                        generation,
                        roots,
                        &mut cx,
                    );
                    let result = match Abortable::new(import, abort_registration).await {
                        Ok(result) => result,
                        Err(_) => Ok(()),
                    };
                    let _ = registry.update(cx, |registry, cx| {
                        registry.clear_snapshot_import_abort_if_current(bootstrap, generation);
                        if registry.snapshot_import_is_current(
                            bootstrap,
                            &identity,
                            &session_name,
                            generation,
                        ) {
                            registry.refresh_window_roots(cx);
                        }
                    });
                    result
                })
            })
            .unwrap_or_else(|| {
                Task::ready(Err(anyhow::anyhow!(
                    "invoking MultiWorkspace disappeared during session binding"
                )))
            });
        FinalizeResult::Ready {
            window_id: bootstrap,
            task,
        }
    }


    /// Install host presentation and connection ownership without publishing
    /// Connected. The state transition is a separate final lifecycle gate.
    fn install_binding_target(
        &mut self,
        window_id: WindowId,
        identity: &SessionIdentity,
        generation: u64,
        cx: &mut Context<Self>,
    ) {
        self.install_host(window_id, identity.name.clone(), cx);
        self.finalize_tokens.insert(window_id.as_u64(), generation);
        if !self.attach_connection_window_for_generation(identity, window_id, generation) {
            log::error!(
                "herdr connection for session {} was released during binding",
                identity.name
            );
        }
    }
    fn finish_connected(
        &mut self,
        window_id: WindowId,
        identity: &SessionIdentity,
        generation: u64,
        cx: &mut Context<Self>,
    ) {
        let owned = self
            .connections
            .get(identity)
            .is_some_and(|connection| connection.bound_windows.contains(&window_id.as_u64()));
        let slot_generation = self
            .slot_generations
            .get(&(identity.clone(), window_id.as_u64()))
            .copied();
        let token_matches = self.finalize_tokens.get(&window_id.as_u64()) == Some(&generation);
        if !owned || !token_matches || slot_generation != Some(generation) {
            log::error!(
                "herdr binding for session {} changed before it connected; rolling back its stale slot",
                identity.name
            );
            // A stale completion must never remove a newer target token. Only
            // the attempt that owns both the token and slot may roll itself
            // back.
            if token_matches && slot_generation == Some(generation) {
                self.finalize_tokens.remove(&window_id.as_u64());
                if let Some(handle) = self
                    .windows
                    .get(&window_id.as_u64())
                    .map(|binding| binding.window)
                {
                    self.host_sink.detach(handle, cx);
                }
                self.detach_window(identity, window_id, cx);
            }
            return;
        }
        {
            let Some(binding) = self.windows.get_mut(&window_id.as_u64()) else {
                self.detach_window(identity, window_id, cx);
                return;
            };
            if !binding_accepts_connected(&binding.state, identity) {
                log::error!(
                    "herdr binding for session {} changed before it connected; rolling back its stale slot",
                    identity.name
                );
                self.finalize_tokens.remove(&window_id.as_u64());
                if let Some(handle) = self
                    .windows
                    .get(&window_id.as_u64())
                    .map(|binding| binding.window)
                {
                    self.host_sink.detach(handle, cx);
                }
                self.detach_window(identity, window_id, cx);
                return;
            }
            binding.state = BindingState::Connected(identity.clone());
        }
        self.finalize_tokens.remove(&window_id.as_u64());
        self.rebuild_panel_observers(cx);
        cx.notify();
    }

    #[cfg(test)]
    fn attach_connection_window(
        &mut self,
        identity: &SessionIdentity,
        window_id: WindowId,
    ) -> bool {
        let generation = self
            .connections
            .get(identity)
            .map(|connection| connection.generation);
        generation.is_some_and(|generation| {
            self.attach_connection_window_for_generation(identity, window_id, generation)
        })
    }

    fn attach_connection_window_for_generation(
        &mut self,
        identity: &SessionIdentity,
        window_id: WindowId,
        generation: u64,
    ) -> bool {
        let Some(connection) = self.connections.get_mut(identity) else {
            return false;
        };
        connection.bound_windows.insert(window_id.as_u64());
        self.slot_generations
            .insert((identity.clone(), window_id.as_u64()), generation);
        true
    }



    /// Re-read every window's checkout roots from its live worktree set.
    /// `roots` is the union of all projects in the window (routing input);
    /// `checkout_path` tracks the *displayed* workspace's first root so
    /// outbound focus addresses the workspace the user is looking at even
    /// after a worktree add or a workspace switch.
    fn refresh_window_roots(&mut self, cx: &App) {
        let handles: Vec<(WindowId, WindowHandle<MultiWorkspace>)> = self
            .windows
            .iter()
            .map(|(window_id, binding)| (WindowId::from(*window_id), binding.window))
            .collect();
        for (window_id, handle) in handles {
            let Ok((display_root, roots)) = handle.read_with(cx, |multi_workspace, cx| {
                let roots = multi_workspace
                    .workspaces()
                    .flat_map(|workspace| {
                        workspace
                            .read(cx)
                            .project()
                            .read(cx)
                            .worktrees(cx)
                            .filter_map(|worktree| {
                                let root = worktree.read(cx).root_dir()?;
                                canonical_checkout_path(root.as_ref()).ok()
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                let display_root = multi_workspace
                    .workspace()
                    .read(cx)
                    .project()
                    .read(cx)
                    .worktrees(cx)
                    .filter_map(|worktree| worktree.read(cx).root_dir())
                    .next()
                    .and_then(|root| canonical_checkout_path(root.as_ref()).ok());
                (display_root, roots)
            }) else {
                continue;
            };
            let Some(binding) = self.windows.get_mut(&window_id.as_u64()) else {
                continue;
            };
            binding.checkout_path = display_root.or_else(|| roots.first().cloned());
            binding.roots = roots;
        }
    }

    // ------------------------------------------------------------- liveness

    /// Stream loss: refresh the catalog exactly once, inline — no new timer
    /// and no new connection task. Missing/not-running is a normal exit
    /// (`Unselected`, ownership dropped); regular shells remain untouched.
    /// Still-running is `Failed`. Either way the registry releases the dead
    /// connection so a later `Retry` or selection creates a fresh client.
    fn classify_loss(
        &mut self,
        identity: &SessionIdentity,
        message: SharedString,
        sessions: Vec<SessionInfo>,
        cx: &mut Context<Self>,
    ) {
        let event = stream_loss_event(identity, message, &sessions);
        if matches!(event, BindingEvent::SessionNotRunning) {
            // Drops synchronization ownership only; the `Forget` effects stay
            // out of the workspace queue and no terminal is closed.
            let effects = self.sync.forget_session(identity);
            self.dispatch_effects(identity, effects, None, cx);
        }
        self.mark_state(identity, event, cx);
        let generation = self
            .connections
            .remove(identity)
            .map(|connection| connection.generation);
        if let Some(generation) = generation {
            self.snapshot_bootstrapped
                .remove(&(identity.clone(), generation));
        }
        self.focus_echoes.remove(identity);
        self.in_flight.remove(identity);
    }

    fn classify_refresh_error(
        &mut self,
        identity: &SessionIdentity,
        message: SharedString,
        cx: &mut Context<Self>,
    ) {
        self.mark_state(identity, BindingEvent::SessionStillRunning(message), cx);
        let generation = self
            .connections
            .remove(identity)
            .map(|connection| connection.generation);
        if let Some(generation) = generation {
            self.snapshot_bootstrapped
                .remove(&(identity.clone(), generation));
        }
        self.focus_echoes.remove(identity);
        self.in_flight.remove(identity);
    }

    fn mark_state(
        &mut self,
        identity: &SessionIdentity,
        event: BindingEvent,
        cx: &mut Context<Self>,
    ) {
        let window_ids: Vec<WindowId> = self
            .windows
            .iter()
            .filter(|(_, binding)| match &binding.state {
                BindingState::Connected(bound) => bound == identity,
                BindingState::Starting { session_name } => session_name == &identity.name,
                _ => false,
            })
            .map(|(window_id, _)| WindowId::from(*window_id))
            .collect();
        if matches!(event, BindingEvent::SessionNotRunning) {
            for window_id in &window_ids {
                if let Some(handle) = self
                    .windows
                    .get(&window_id.as_u64())
                    .map(|binding| binding.window)
                {
                    // Detach the central host before publishing Unselected so
                    // a subsequent selection can spawn a fresh client.
                    self.host_sink.detach(handle, cx);
                }
            }
        }
        for window_id in window_ids {
            self.abort_snapshot_import(window_id);
            self.finalize_tokens.remove(&window_id.as_u64());
            if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
                binding.state = transition(binding.state.clone(), event.clone());
                if matches!(event, BindingEvent::SessionNotRunning) {
                    self.attempts.remove(&window_id.as_u64());
                }
            }
        }
        cx.notify();
    }

    fn note_window_released(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        self.abort_snapshot_import(window_id);
        self.windows.remove(&window_id.as_u64());
        self.prune_mirrors_for_window(window_id, cx);
        self.prompted.remove(&window_id);
        self.prompt_pending.remove(&window_id);
        self.attempts.remove(&window_id.as_u64());
        self.finalize_tokens.remove(&window_id.as_u64());
        self.workspace_root_locks
            .retain(|(lock_window, _), _| *lock_window != window_id.as_u64());
        let identities: Vec<SessionIdentity> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.bound_windows.contains(&window_id.as_u64()))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in identities {
            self.detach_window(&identity, window_id, cx);
        }
        // A connection that never gained a bound window (routing was
        // abandoned with its window) is released too.
        self.connections
            .retain(|_, connection| !connection.bound_windows.is_empty());
    }

    fn shared_connection(
        &self,
        identity: &SessionIdentity,
    ) -> Option<(Rc<dyn HerdrSessionHandle>, u64)> {
        self.connections
            .get(identity)
            .map(|connection| (connection.client.clone(), connection.generation))
    }

    /// Register the established client for a session exactly once. Returns
    /// `false` when another connection task won the race; the caller then
    /// drops its own client and stream so the registry never holds two.
    fn install_connection(
        &mut self,
        identity: SessionIdentity,
        client: Rc<dyn HerdrSessionHandle>,
        event_task: Task<()>,
        generation: u64,
    ) -> bool {
        if self.connections.contains_key(&identity) {
            return false;
        }
        self.connections.insert(
            identity,
            SessionConnection {
                client,
                bound_windows: HashSet::default(),
                _event_task: event_task,
                generation,
            },
        );
        true
    }

    fn shared_client(&self, identity: &SessionIdentity) -> Option<Rc<dyn HerdrSessionHandle>> {
        self.connections
            .get(identity)
            .map(|connection| connection.client.clone())
    }

    fn connection_matches(&self, identity: &SessionIdentity, generation: u64) -> bool {
        self.connections
            .get(identity)
            .is_some_and(|connection| connection.generation == generation)
    }

    fn has_live_snapshot_finalizer(
        &self,
        identity: &SessionIdentity,
        generation: u64,
    ) -> bool {
        let Some(connection) = self.connections.get(identity) else {
            return false;
        };
        if connection.generation != generation {
            return false;
        }
        connection.bound_windows.iter().any(|window_id| {
            let Some(slot_generation) = self
                .slot_generations
                .get(&(identity.clone(), *window_id))
                .copied()
            else {
                return false;
            };
            self.finalize_tokens.get(window_id) == Some(&slot_generation)
                && self.attempts.get(window_id) == Some(&slot_generation)
                && matches!(
                    self.windows.get(window_id).map(|binding| &binding.state),
                    Some(BindingState::Starting { session_name })
                        if session_name.as_ref() == identity.name.as_ref()
                )
        })
    }

    fn prune_mirrors_for_window(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        let reroute: Vec<AgentRecord> = self
            .mirrors
            .iter()
            .filter(|(_, mirror)| mirror.window.window_id() == window_id)
            .filter_map(|(key, _)| self.sync.record(key))
            .collect();
        let keys: Vec<AgentKey> = self
            .mirrors
            .iter()
            .filter(|(_, mirror)| mirror.window.window_id() == window_id)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &keys {
            self.forget_mirror(key);
        }
        self.mirroring_in_flight
            .retain(|(key, _)| !keys.contains(key));
        for record in reroute {
            let identity = record.key.session.clone();
            self.dispatch_effects(&identity, vec![AgentSyncEffect::Open(record)], None, cx);
        }
    }

    fn rebuild_panel_observers(&mut self, cx: &mut Context<Self>) {
        let mut seen: HashSet<EntityId> = HashSet::default();
        let panels: Vec<(Entity<AgentPanel>, WindowHandle<MultiWorkspace>)> = self
            .mirrors
            .values()
            .filter_map(|mirror| {
                let panel = mirror.panel.upgrade()?;
                seen.insert(panel.entity_id())
                    .then_some((panel, mirror.window))
            })
            .collect();
        self.panel_subscriptions.clear();
        self.observed_panels.clear();
        for (panel, window) in panels {
            self.register_panel_observer(panel, window, cx);
        }
    }

    fn clear_in_flight(&mut self, identity: &SessionIdentity, generation: u64) {
        if self.in_flight.get(identity) == Some(&generation) {
            self.in_flight.remove(identity);
        }
    }


    // ------------------------------------------------------------ test seams

    #[cfg(test)]
    pub(crate) fn test(_cx: &mut Context<Self>, gateway: HerdrGateway) -> Self {
        Self {
            windows: HashMap::default(),
            prompted: HashSet::default(),
            prompt_pending: HashSet::default(),
            connections: HashMap::default(),
            attempts: HashMap::default(),
            in_flight: HashMap::default(),
            snapshot_bootstrapped: HashSet::default(),
            next_generation: 0,
            #[cfg(test)]
            slot_generations: HashMap::default(),
            finalize_tokens: HashMap::default(),
            snapshot_import_abort_handles: HashMap::default(),
            workspace_root_locks: HashMap::default(),
            sync: AgentSyncState::default(),
            mirror_index: MirrorIndex::default(),
            mirrors: HashMap::default(),
            observed_panels: HashSet::default(),
            panel_subscriptions: Vec::new(),
            mirroring_in_flight: HashSet::default(),
            pending_mirror_replay: HashMap::default(),
            focus_echoes: HashMap::default(),
            #[cfg(test)]
            sink_effects: None,
            #[cfg(test)]
            mirror_target_sink: None,
            host_sink: Rc::new(NoopHerdrHostSink),
            picker_sink: Rc::new(NoopSessionPickerSink),
            gateway: Some(gateway),
            connection_tasks_started: 0,
            _window_observer: None,
            _window_close_subscription: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn install_as_global(entity: Entity<Self>, cx: &mut App) {
        cx.set_global(GlobalHerdrSessionRegistry(entity));
    }

    #[cfg(test)]
    pub(crate) fn register_window_for_test(
        &mut self,
        handle: WindowHandle<MultiWorkspace>,
        state: BindingState,
        roots: Vec<herdr::CanonicalPath>,
    ) -> WindowId {
        let window_id = self.register_window(handle);
        if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
            binding.state = state;
            binding.roots = roots;
        }
        window_id
    }

    #[cfg(test)]
    pub(crate) fn set_effect_sink(&mut self, sink: Rc<dyn AgentEffectSink>) {
        self.sink_effects = Some(sink);
    }

    #[cfg(test)]
    pub(crate) fn set_mirror_target_sink_for_test(
        &mut self,
        sink: Rc<std::cell::RefCell<Vec<(AgentKey, WindowId)>>>,
    ) {
        self.mirror_target_sink = Some(sink);
    }

    /// Register an established connection for a session exactly once, the
    /// same way the bounded connect loop does, so routing tests can drive
    /// one effect without replaying the whole lifecycle.
    #[cfg(test)]
    pub(crate) fn install_connection_for_test(
        &mut self,
        identity: SessionIdentity,
        client: Rc<dyn HerdrSessionHandle>,
        generation: u64,
    ) {
        let installed =
            self.install_connection(identity, client, Task::ready(()), generation);
        assert!(installed, "test connection is installed exactly once");
    }

    #[cfg(test)]
    pub(crate) fn connection_tasks_started(&self) -> u64 {
        self.connection_tasks_started
    }

    #[cfg(test)]
    pub(crate) fn bound_window_count(&self, identity: &SessionIdentity) -> usize {
        self.connections
            .get(identity)
            .map(|connection| connection.bound_windows.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn has_connection(&self, identity: &SessionIdentity) -> bool {
        self.connections.contains_key(identity)
    }
    pub(crate) fn set_host_sink(&mut self, sink: Rc<dyn HerdrHostSink>) {
        self.host_sink = sink;
    }

    pub(crate) fn set_picker_sink(&mut self, sink: Rc<dyn SessionPickerSink>) {
        self.picker_sink = sink;
    }

    #[cfg(test)]
    pub(crate) fn confirm_selection_for_test(
        &mut self,
        invoking: WindowHandle<MultiWorkspace>,
        selection: SessionSelection,
        cx: &mut Context<Self>,
    ) {
        self.apply_selection_for_window(invoking, selection, cx);
    }
    #[cfg(test)]
    pub(crate) fn start_binding_for_test(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
        cx: &mut Context<Self>,
    ) {
        self.start_binding_for_window(window_id, session_name, cx);
    }

}

fn snapshot_checkout_paths(snapshot: &SessionSnapshot) -> Vec<herdr::CanonicalPath> {
    let mut seen = HashSet::<herdr::CanonicalPath>::default();
    let mut checkout_paths = Vec::new();
    for workspace in &snapshot.workspaces {
        let Some(checkout_path) = workspace.checkout_path() else {
            continue;
        };
        let Ok(checkout_path) = canonical_checkout_path(checkout_path) else {
            continue;
        };
        if seen.insert(checkout_path.clone()) {
            checkout_paths.push(checkout_path);
        }
    }
    checkout_paths
}

fn focused_checkout_path(snapshot: &SessionSnapshot) -> Option<herdr::CanonicalPath> {
    let workspace = snapshot.workspaces.iter().find(|workspace| {
        snapshot
            .focused_workspace_id
            .as_deref()
            .is_some_and(|focused| focused == workspace.workspace_id)
    })?;
    let checkout = workspace.checkout_path()?;
    canonical_checkout_path(checkout).ok()
}
async fn add_workspace_root(
    window: WindowHandle<MultiWorkspace>,
    root: herdr::CanonicalPath,
    cx: &mut AsyncApp,
) -> anyhow::Result<()> {
    let already_present = window
        .read_with(cx, |multi_workspace, cx| {
            multi_workspace.workspaces().any(|workspace| {
                workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .worktrees(cx)
                    .filter_map(|worktree| worktree.read(cx).root_dir())
                    .any(|candidate| {
                        canonical_checkout_path(candidate.as_ref())
                            .is_ok_and(|candidate| candidate == root)
                    })
            })
        })
        .map_err(|error| anyhow::anyhow!("could not inspect invoking workspace: {error:#}"))?;
    if already_present {
        return Ok(());
    }
    let Some(app_state) = cx.update(|cx| AppState::try_global(cx)) else {
        return Err(anyhow::anyhow!("Zed app state is unavailable"));
    };
    let open = cx.update(|cx| {
        Workspace::new_local(
            vec![PathBuf::from(root.as_str())],
            app_state,
            Some(window),
            None,
            None,
            OpenMode::Add,
            cx,
        )
    });
    open.await.map(|_| ())
}

fn report_workspace_import_failure(
    window: WindowHandle<MultiWorkspace>,
    root: &herdr::CanonicalPath,
    error: &anyhow::Error,
    cx: &mut AsyncApp,
) {
    let id = NotificationId::composite::<HerdrWorkspaceImportFailureNotification>(
        root.as_str().to_owned(),
    );
    let message: SharedString = format!(
        "could not import herdr workspace {}: {error:#}",
        root.as_str()
    )
    .into();
    if let Err(update_error) = window.update(cx, |multi_workspace, _, cx| {
        multi_workspace.workspace().update(cx, |workspace, cx| {
            workspace.show_notification(id, cx, |cx| {
                cx.new(|cx| MessageNotification::new(message.clone(), cx))
            });
        })
    }) {
        log::debug!(
            "could not report herdr workspace import failure for {}: {update_error:#}",
            root.as_str()
        );
    }
}

/// Sequential snapshot import for a binding. Stale attempts stop before the
/// next root and never report a failure from an old session selection.
async fn import_snapshot_workspaces_for_binding(
    registry: Entity<HerdrSessionRegistry>,
    window: WindowHandle<MultiWorkspace>,
    window_id: WindowId,
    identity: SessionIdentity,
    session_name: Arc<str>,
    generation: u64,
    roots: Vec<herdr::CanonicalPath>,
    cx: &mut AsyncApp,
) -> anyhow::Result<()> {
    let mut first_error = None;
    for root in roots {
        let current = registry.read_with(cx, |registry, _| {
            registry.snapshot_import_is_current(window_id, &identity, &session_name, generation)
        });
        if !current {
            return Ok(());
        }
        match add_workspace_root(window, root.clone(), cx).await {
            Ok(()) => {}
            Err(error) => {
                let current = registry.read_with(cx, |registry, _| {
                    registry.snapshot_import_is_current(
                        window_id,
                        &identity,
                        &session_name,
                        generation,
                    )
                });
                if !current {
                    return Ok(());
                }
                report_workspace_import_failure(window, &root, &error, cx);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        let current = registry.read_with(cx, |registry, _| {
            registry.snapshot_import_is_current(window_id, &identity, &session_name, generation)
        });
        if !current {
            return Ok(());
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}



/// `Connected` for a session while its current binding still describes the
/// window this routing decision may complete. A window that failed or started
/// a different session in the meantime keeps its newer state.
fn binding_accepts_connected(state: &BindingState, identity: &SessionIdentity) -> bool {
    match state {
        BindingState::Unselected => true,
        BindingState::Starting { session_name } => *session_name == identity.name,
        BindingState::Connected(bound) => bound == identity,
        BindingState::Failed { .. } => false,
    }
}
fn stream_loss_event(
    identity: &SessionIdentity,
    message: SharedString,
    sessions: &[SessionInfo],
) -> BindingEvent {
    if sessions
        .iter()
        .any(|session| session.running && SessionIdentity::from_info(session) == *identity)
    {
        BindingEvent::SessionStillRunning(message)
    } else {
        BindingEvent::SessionNotRunning
    }
}

// -------------------------------------------------------------------- task

/// One bounded connection + steady-state stream for a session, spawned for
/// the first window that binds it. A connection is not `Connected` until the
/// subscription is open, the snapshot is fetched, the buffered events are
/// drained, the final target binding is installed, and the initial agents
/// have been scheduled through the deduplicated synchronization path.
macro_rules! await_before_deadline {
    ($future:expr, $cx:expr, $deadline:expr) => {{
        let future = $future;
        let remaining = $deadline.saturating_duration_since($cx.background_executor().now());
        if remaining.is_zero() {
            None
        } else {
            let timer = $cx.background_executor().timer(remaining);
            futures::pin_mut!(future, timer);
            futures::select_biased! {
                value = future.fuse() => Some(value),
                _ = timer.fuse() => None,
            }
        }
    }};
}

/// A timed-out owner must not drain its buffered stream frames until another
/// bound window has claimed the generation's snapshot bootstrap. Otherwise an
/// older survivor snapshot can reconcile against post-snapshot events in the
/// wrong order.
async fn wait_for_generation_snapshot_bootstrap(
    registry: Entity<HerdrSessionRegistry>,
    identity: SessionIdentity,
    generation: u64,
    cx: &mut AsyncApp,
) -> bool {
    loop {
        match registry.read_with(cx, |registry, _| {
            if registry
                .snapshot_bootstrapped
                .contains(&(identity.clone(), generation))
            {
                Some(true)
            } else if registry.has_live_snapshot_finalizer(&identity, generation) {
                Some(false)
            } else {
                None
            }
        }) {
            Some(true) => return true,
            Some(false) => {
                cx.background_executor().timer(ATTEMPT_INTERVAL).await;
            }
            None => return false,
        }
    }
}

async fn run_connection(
    registry: Entity<HerdrSessionRegistry>,
    gateway: HerdrGateway,
    bootstrap: WindowHandle<MultiWorkspace>,
    window_id: WindowId,
    session_name: Arc<str>,
    generation: u64,
    mut cx: AsyncApp,
) {
    let deadline = cx.background_executor().now() + CONNECTION_DEADLINE;
    let starting = registry.read_with(&mut cx, |registry, _| {
        registry.attempt_is_current(window_id, generation, &session_name)
    });
    if !starting {
        return;
    }

    // 1. Refresh the session list until the selected entry and its reported
    //    socket appear — inside the single 15-second deadline.
    let mut last_error = None;
    let info = loop {
        let current = registry.read_with(&mut cx, |registry, _| {
            registry.attempt_is_current(window_id, generation, &session_name)
        });
        if !current || bootstrap.read_with(&mut cx, |_, _| ()).is_err() {
            return;
        }
        let Some(result) = await_before_deadline!(gateway.list_sessions(), &mut cx, deadline)
        else {
            let message =
                last_error.unwrap_or_else(|| "session did not appear within 15 seconds".to_owned());
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                message,
                &mut cx,
            );
            return;
        };
        let sessions = match result {
            Ok(sessions) => sessions,
            Err(error) => {
                last_error = Some(format!("herdr session list failed: {error:#}"));
                Vec::new()
            }
        };
        if let Some(info) = sessions
            .into_iter()
            .find(|session| session.running && session.name == session_name.as_ref())
        {
            break info;
        }
        if cx.background_executor().now() >= deadline {
            let message =
                last_error.unwrap_or_else(|| "session did not appear within 15 seconds".to_owned());
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                message,
                &mut cx,
            );
            return;
        }
        let remaining = deadline.saturating_duration_since(cx.background_executor().now());
        cx.background_executor()
            .timer(remaining.min(ATTEMPT_INTERVAL))
            .await;
    };
    let identity = SessionIdentity::from_info(&info);

    if !registry.read_with(&mut cx, |registry, _| {
        registry.attempt_is_current(window_id, generation, &session_name)
    }) {
        return;
    }

    enum ConnectionRole {
        Shared(Rc<dyn HerdrSessionHandle>, u64),
        Owner,
        Wait,
    }

    // Reserve the identity before opening any client or subscription. A
    // concurrent binding that reaches this point waits for the owner instead
    // of opening a second socket.
    let role = registry.update(&mut cx, |registry, _| {
        if let Some((client, owner_generation)) = registry.shared_connection(&identity) {
            ConnectionRole::Shared(client, owner_generation)
        } else if registry.in_flight.contains_key(&identity) {
            ConnectionRole::Wait
        } else {
            registry.in_flight.insert(identity.clone(), generation);
            ConnectionRole::Owner
        }
    });

    if matches!(role, ConnectionRole::Wait) {
        loop {
            let current = registry.read_with(&mut cx, |registry, _| {
                registry.attempt_is_current(window_id, generation, &session_name)
            });
            if !current {
                return;
            }
            if let Some((client, owner_generation)) =
                registry.read_with(&mut cx, |registry, _| registry.shared_connection(&identity))
            {
                let Some(snapshot) = await_before_deadline!(client.snapshot(), &mut cx, deadline)
                else {
                    fail(
                        registry,
                        window_id,
                        &session_name,
                        generation,
                        "connection deadline reached while waiting for shared snapshot".into(),
                        &mut cx,
                    );
                    return;
                };
                let Ok(snapshot) = snapshot else {
                    continue;
                };
                // Finalize against the freshly-read registry state in one
                // update: a connection dropped while the snapshot was
                // pending must not gain a new bound window.
                let outcome = registry.update(&mut cx, |registry, cx| {
                    if !registry.connection_matches(&identity, owner_generation)
                        || !registry.attempt_is_current(window_id, generation, &session_name)
                    {
                        return None;
                    }
                    Some(registry.finalize_binding(
                        window_id,
                        &identity,
                        &session_name,
                        generation,
                        &snapshot,
                        cx,
                    ))
                });
                let Some(outcome) = outcome else {
                    fail(
                        registry.clone(),
                        window_id,
                        &session_name,
                        generation,
                        "herdr shared connection ended before snapshot workspace import".into(),
                        &mut cx,
                    );
                    return;
                };
                let (final_window, mirror_target_window) = match outcome {
                    FinalizeResult::Ready { window_id, task } => {
                        match await_before_deadline!(task, &mut cx, deadline) {
                            Some(Ok(())) => {}
                            Some(Err(error)) => {
                                log::warn!(
                                    "herdr snapshot workspace import had failures: {error:#}"
                                );
                            }
                            None => {
                                fail_after_finalize(
                                    registry,
                                    window_id,
                                    &identity,
                                    &session_name,
                                    generation,
                                    "herdr snapshot workspace import timed out before agent effects"
                                        .into(),
                                    &mut cx,
                                );
                                return;
                            }
                        }
                        (
                            Some(window_id),
                            Some(MirrorTarget {
                                window_id,
                                generation: owner_generation,
                            }),
                        )
                    }
                };
                let _ = registry.update(&mut cx, |registry, cx| {
                    if !registry.connection_matches(&identity, owner_generation)
                        || !registry.snapshot_import_is_current(
                            window_id,
                            &identity,
                            &session_name,
                            generation,
                        )
                    {
                        return;
                    }
                    registry.dispatch_snapshot_effects(
                        &identity,
                        &snapshot,
                        mirror_target_window,
                        cx,
                    );
                    if let Some(final_window) = final_window {
                        registry.finish_connected(final_window, &identity, generation, cx);
                    }
                });
                return;
            }
            if cx.background_executor().now() >= deadline {
                fail(
                    registry,
                    window_id,
                    &session_name,
                    generation,
                    "connection deadline reached while waiting for shared connection".into(),
                    &mut cx,
                );
                return;
            }
            cx.background_executor().timer(ATTEMPT_INTERVAL).await;
        }
    }

    let client = if let ConnectionRole::Shared(client, _) = &role {
        client.clone()
    } else {
        let mut last_error = None;
        let client = loop {
            let current = registry.read_with(&mut cx, |registry, _| {
                registry.attempt_is_current(window_id, generation, &session_name)
            });
            if !current {
                let _ = registry.update(&mut cx, |registry, _| {
                    registry.clear_in_flight(&identity, generation);
                });
                return;
            }
            let Some(result) = await_before_deadline!(gateway.connect(&info), &mut cx, deadline)
            else {
                let message =
                    last_error.unwrap_or_else(|| "connection deadline reached".to_owned());
                let _ = registry.update(&mut cx, |registry, _| {
                    registry.clear_in_flight(&identity, generation);
                });
                fail(
                    registry,
                    window_id,
                    &session_name,
                    generation,
                    message,
                    &mut cx,
                );
                return;
            };
            match result {
                Ok(client) => {
                    let current = registry.read_with(&mut cx, |registry, _| {
                        registry.attempt_is_current(window_id, generation, &session_name)
                    });
                    if !current {
                        let _ = registry.update(&mut cx, |registry, _| {
                            registry.clear_in_flight(&identity, generation);
                        });
                        drop(client);
                        return;
                    }
                    break client;
                }
                Err(error) => {
                    last_error = Some(format!("herdr connect failed: {error:#}"));
                    if cx.background_executor().now() >= deadline {
                        let _ = registry.update(&mut cx, |registry, _| {
                            registry.clear_in_flight(&identity, generation);
                        });
                        fail(
                            registry,
                            window_id,
                            &session_name,
                            generation,
                            last_error.unwrap(),
                            &mut cx,
                        );
                        return;
                    }
                    cx.background_executor().timer(ATTEMPT_INTERVAL).await;
                }
            }
        };
        client
    };

    // Shared clients only fetch a fresh snapshot; the owner opens the one
    // subscription before fetching its snapshot.
    if let ConnectionRole::Shared(_, owner_generation) = &role {
        let Some(result) = await_before_deadline!(client.snapshot(), &mut cx, deadline) else {
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                "shared snapshot deadline reached".into(),
                &mut cx,
            );
            return;
        };
        let Ok(snapshot) = result else {
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                "shared snapshot failed".into(),
                &mut cx,
            );
            return;
        };
        // Attach, finalize, and publish Connected against the live shared
        // connection: the owner's loop may drop the connection entry between
        // awaits, and attaching into an already-removed entry would strand a
        // "Connected" window with no pump and no stream-loss detection.
        let outcome = registry.update(&mut cx, |registry, cx| {
            if !registry.connection_matches(&identity, *owner_generation)
                || !registry.attempt_is_current(window_id, generation, &session_name)
            {
                return None;
            }
            Some(registry.finalize_binding(
                window_id,
                &identity,
                &session_name,
                generation,
                &snapshot,
                cx,
            ))
        });
        let Some(outcome) = outcome else {
            fail(
                registry.clone(),
                window_id,
                &session_name,
                generation,
                "herdr shared connection ended before snapshot workspace import".into(),
                &mut cx,
            );
            return;
        };
        let (final_window, mirror_target_window) = match outcome {
            FinalizeResult::Ready { window_id, task } => {
                match await_before_deadline!(task, &mut cx, deadline) {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        log::warn!("herdr snapshot workspace import had failures: {error:#}");
                    }
                    None => {
                        fail_after_finalize(
                            registry,
                            window_id,
                            &identity,
                            &session_name,
                            generation,
                            "herdr snapshot workspace import timed out before agent effects".into(),
                            &mut cx,
                        );
                        return;
                    }
                }
                (
                    Some(window_id),
                    Some(MirrorTarget {
                        window_id,
                        generation: *owner_generation,
                    }),
                )
            }
        };
        let _ = registry.update(&mut cx, |registry, cx| {
            if !registry.connection_matches(&identity, *owner_generation)
                || !registry.snapshot_import_is_current(
                    window_id,
                    &identity,
                    &session_name,
                    generation,
                )
            {
                return;
            }
            registry.dispatch_snapshot_effects(
                &identity,
                &snapshot,
                mirror_target_window,
                cx,
            );
            if let Some(final_window) = final_window {
                registry.finish_connected(final_window, &identity, generation, cx);
            }
        });
        return;
    }

    let mut stream = loop {
        let Some(result) = await_before_deadline!(client.subscribe(), &mut cx, deadline) else {
            let _ = registry.update(&mut cx, |registry, _| {
                registry.clear_in_flight(&identity, generation);
            });
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                "herdr subscribe deadline reached".into(),
                &mut cx,
            );
            return;
        };
        match result {
            Ok(stream) => {
                let current = registry.read_with(&mut cx, |registry, _| {
                    registry.attempt_is_current(window_id, generation, &session_name)
                });
                if !current {
                    let _ = registry.update(&mut cx, |registry, _| {
                        registry.clear_in_flight(&identity, generation);
                    });
                    return;
                }
                break stream;
            }
            Err(error) => {
                if cx.background_executor().now() >= deadline {
                    let _ = registry.update(&mut cx, |registry, _| {
                        registry.clear_in_flight(&identity, generation);
                    });
                    fail(
                        registry,
                        window_id,
                        &session_name,
                        generation,
                        format!("herdr subscribe failed: {error:#}"),
                        &mut cx,
                    );
                    return;
                }
                cx.background_executor().timer(ATTEMPT_INTERVAL).await;
            }
        }
    };
    let (mut tx, mut rx) = mpsc::channel::<HerdrEvent>(EVENT_CHANNEL_CAPACITY);
    let pump = cx.spawn(async move |_cx| {
        loop {
            match stream.next().await {
                Ok(Some(event)) => {
                    // Backpressure keeps the subscription alive while snapshot
                    // retrieval is slow; send fails only when the receiver drops.
                    if tx.send(event).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    log::debug!("herdr event stream error: {error:#}");
                    break;
                }
            }
        }
    });

    let Some(result) = await_before_deadline!(client.snapshot(), &mut cx, deadline) else {
        drop(pump);
        let _ = registry.update(&mut cx, |registry, _| {
            registry.clear_in_flight(&identity, generation);
        });
        fail(
            registry,
            window_id,
            &session_name,
            generation,
            "herdr snapshot deadline reached".into(),
            &mut cx,
        );
        return;
    };
    let snapshot = match result {
        Ok(snapshot) => snapshot,
        Err(error) => {
            drop(pump);
            let _ = registry.update(&mut cx, |registry, _| {
                registry.clear_in_flight(&identity, generation);
            });
            fail(
                registry,
                window_id,
                &session_name,
                generation,
                format!("herdr snapshot failed: {error:#}"),
                &mut cx,
            );
            return;
        }
    };

    let current = registry.read_with(&mut cx, |registry, _| {
        registry.attempt_is_current(window_id, generation, &session_name)
    });
    if !current {
        drop(pump);
        let _ = registry.update(&mut cx, |registry, _| {
            registry.clear_in_flight(&identity, generation);
        });
        return;
    }
    // Drain the buffered frames in order before importing.
    let mut buffered = Vec::new();
    while let Ok(event) = rx.try_recv() {
        buffered.push(event);
    }

    let installed = registry.update(&mut cx, |registry, _| {
        registry.install_connection(identity.clone(), client.clone(), pump, generation)
    });
    if !installed {
        drop(client);
        return;
    }
    let _ = registry.update(&mut cx, |registry, _| {
        registry.clear_in_flight(&identity, generation);
    });

    let outcome = registry.update(&mut cx, |registry, cx| {
        registry.finalize_binding(
            window_id,
            &identity,
            &session_name,
            generation,
            &snapshot,
            cx,
        )
    });
    let (final_window, mirror_target_window, import_effects) = match outcome {
        FinalizeResult::Ready { window_id, task } => {
            match await_before_deadline!(task, &mut cx, deadline) {
                Some(Ok(())) => (
                    Some(window_id),
                    Some(MirrorTarget {
                        window_id,
                        generation,
                    }),
                    true,
                ),
                Some(Err(error)) => {
                    log::warn!("herdr snapshot workspace import had failures: {error:#}");
                    (
                        Some(window_id),
                        Some(MirrorTarget {
                            window_id,
                            generation,
                        }),
                        true,
                    )
                }
                None => {
                    fail_after_finalize(
                        registry.clone(),
                        window_id,
                        &identity,
                        &session_name,
                        generation,
                        "herdr snapshot workspace import timed out before agent effects".into(),
                        &mut cx,
                    );
                    let connection_survives = registry.read_with(&mut cx, |registry, _| {
                        registry.connections.get(&identity).is_some_and(|connection| {
                            connection.generation == generation
                                && !connection.bound_windows.is_empty()
                        })
                    });
                    if !connection_survives {
                        return;
                    }
                    (None, None, false)
                }
            }
        }
    };
    if import_effects {
        let connection_survives = registry.read_with(&mut cx, |registry, _| {
            registry.connections.get(&identity).is_some_and(|connection| {
                connection.generation == generation && !connection.bound_windows.is_empty()
            })
        });
        if connection_survives {
            let _ = registry.update(&mut cx, |registry, cx| {
                if !registry.connection_matches(&identity, generation)
                    || !registry.snapshot_import_is_current(
                        window_id,
                        &identity,
                        &session_name,
                        generation,
                    )
                {
                    return;
                }
                registry.dispatch_snapshot_effects(
                    &identity,
                    &snapshot,
                    mirror_target_window,
                    cx,
                );
            });
        }
    }
    if !wait_for_generation_snapshot_bootstrap(
        registry.clone(),
        identity.clone(),
        generation,
        &mut cx,
    )
    .await
    {
        return;
    }
    for event in buffered {
        process_stream_event(&registry, &identity, generation, event, &mut cx).await;
    }
    let _ = registry.update(&mut cx, |registry, cx| {
        if !registry.connection_matches(&identity, generation) {
            return;
        }
        if let Some(final_window) = final_window {
            if final_window == window_id
                && !registry.attempt_is_current(final_window, generation, &session_name)
            {
                return;
            }
            registry.finish_connected(final_window, &identity, generation, cx);
        }
    });
    drop(client);

    // 6. Steady state: further frames continue through the same channel.
    //    The ownership poll lets a last-window disconnect (which drops the
    //    connection entry, its client, and the pump task) end this loop.
    loop {
        futures::select_biased! {
            event = rx.next().fuse() => {
                match event {
                    Some(event) => {
                        process_stream_event(&registry, &identity, generation, event, &mut cx).await;
                    }
                    None => break,
                }
                let owned = registry.read_with(&mut cx, |registry, _| {
                    registry.connection_matches(&identity, generation)
                });
                if !owned {
                    return;
                }
            }
            _ = cx.background_executor().timer(OWNERSHIP_POLL).fuse() => {
                let owned = registry.read_with(&mut cx, |registry, _| {
                    registry.connection_matches(&identity, generation)
                });
                if !owned {
                    return;
                }
            }
        }
    }

    // 7. Stream loss: exactly one catalog refresh, then `Unselected` (normal
    //    exit) or `Failed`. No new timer and no new connection task.
    let refresh = gateway.list_sessions().await;
    registry.update(&mut cx, move |registry, cx| {
        if !registry.connection_matches(&identity, generation) {
            return;
        }
        match refresh {
            Ok(sessions) => {
                let message: SharedString = "herdr event stream ended".into();
                registry.classify_loss(&identity, message, sessions, cx);
            }
            Err(error) => {
                registry.classify_refresh_error(
                    &identity,
                    format!("herdr session refresh failed: {error:#}").into(),
                    cx,
                );
            }
        }
    });
}



/// Fail a finalized snapshot-import attempt and release only its window.
///
/// Unlike `fail`, this path runs after the target was attached to a shared
/// connection and a finalize token was installed. The attempt and token
/// generations must still match before any cleanup so an older timeout cannot
/// detach a newer retry or shared binding.
fn fail_after_finalize(
    registry: Entity<HerdrSessionRegistry>,
    window_id: WindowId,
    identity: &SessionIdentity,
    session_name: &str,
    generation: u64,
    message: String,
    cx: &mut AsyncApp,
) {
    let _ = registry.update(cx, |registry, cx| {
        if !registry.attempt_is_current(window_id, generation, session_name)
            || registry.finalize_tokens.get(&window_id.as_u64()) != Some(&generation)
        {
            return;
        }
        registry.abort_snapshot_import(window_id);
        registry.apply_binding_event(
            window_id,
            BindingEvent::SessionStillRunning(message.into()),
            cx,
        );
        if let Some(handle) = registry
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window)
        {
            registry.host_sink.detach(handle, cx);
        }
        registry.finalize_tokens.remove(&window_id.as_u64());
        registry.detach_window(identity, window_id, cx);
    });
}

/// Transition the binding to `Failed`, but only while this exact attempt is
/// still current: a stale `run_connection` whose deadline fires after a
/// disconnect + re-selection must not fail the newer `Starting` binding.
fn fail(
    registry: Entity<HerdrSessionRegistry>,
    window_id: WindowId,
    session_name: &str,
    generation: u64,
    message: String,
    cx: &mut AsyncApp,
) {
    let _ = registry.update(cx, |registry, cx| {
        if !registry.attempt_is_current(window_id, generation, session_name) {
            return;
        }
        registry.apply_binding_event(
            window_id,
            BindingEvent::SessionStillRunning(message.into()),
            cx,
        );
    });
}

/// Process one stream frame. Id-only reference events refresh through
/// `client.pane`, workspace focus events route to the mapped window, and the
/// rest reduce directly. The connection generation gates every mutation, so
/// a superseded stream never moves the current windows. Both the bootstrap
/// replay of buffered frames and the steady-state loop use this path.
async fn process_stream_event(
    registry: &Entity<HerdrSessionRegistry>,
    identity: &SessionIdentity,
    generation: u64,
    event: HerdrEvent,
    cx: &mut AsyncApp,
) {
    if let HerdrEvent::Workspace(WorkspaceEvent::Focused { workspace_id }) = &event {
        let owned = registry.read_with(cx, |registry, _| {
            registry.connection_matches(identity, generation)
        });
        if owned {
            registry.update(cx, |registry, cx| {
                if !registry.connection_matches(identity, generation) {
                    return;
                }
                registry.focus_remote_workspace(identity, workspace_id, cx);
            });
        }
    }
    let fetched = match &event {
        HerdrEvent::Pane(PaneEvent {
            kind:
                PaneEventKind::Focused { pane_id, .. } | PaneEventKind::AgentDetected { pane_id, .. },
        }) => {
            let client = registry.read_with(cx, |registry, _| registry.shared_client(identity));
            match client {
                Some(client) => Some(client.pane(pane_id.clone()).await),
                None => None,
            }
        }
        _ => None,
    };
    let _ = registry.update(cx, |registry, cx| {
        if !registry.connection_matches(identity, generation) {
            return;
        }
        let effects = match fetched {
            Some(Ok(pane)) => registry.sync.upsert(identity.clone(), pane),
            Some(Err(error)) => {
                let missing = error.to_string().to_ascii_lowercase();
                if missing.contains("not found")
                    || missing.contains("not_found")
                    || missing.contains("no such pane")
                    || missing.contains("does not exist")
                {
                    if let HerdrEvent::Pane(PaneEvent {
                        kind:
                            PaneEventKind::Focused { pane_id, .. }
                            | PaneEventKind::AgentDetected { pane_id, .. },
                    }) = &event
                    {
                        registry.sync.exit(identity, pane_id)
                    } else {
                        Vec::new()
                    }
                } else {
                    log::debug!("herdr pane refresh failed: {error:#}");
                    Vec::new()
                }
            }
            None => apply_event_effects(&mut registry.sync, identity, &event),
        };
        registry.dispatch_effects(identity, effects, None, cx);
        cx.notify();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use agent::ThreadStore;

    use gpui::TestAppContext;
    use settings::Settings as _;

    fn session(name: &str) -> SessionIdentity {
        SessionIdentity {
            name: Arc::from(name),
            session_dir: Arc::from(PathBuf::from(format!("/sessions/{name}"))),
        }
    }

    fn session_info(name: &str, running: bool) -> SessionInfo {
        SessionInfo {
            name: name.to_owned(),
            is_default: false,
            running,
            session_dir: PathBuf::from(format!("/sessions/{name}")),
            socket_path: PathBuf::from(format!("/sessions/{name}/herdr.sock")),
        }
    }

    fn checkout(path: &str) -> herdr::CanonicalPath {
        canonical_checkout_path(std::path::Path::new(path)).expect("absolute checkout path")
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
        let identity = session("main");
        let sessions = [session_info("main", true)];
        let event = stream_loss_event(&identity, "socket closed".into(), &sessions);
        let state = transition(BindingState::Connected(identity), event);
        assert!(matches!(state, BindingState::Failed { .. }));
    }

    #[gpui::test]
    fn released_window_prunes_its_workspace_root_locks(cx: &mut TestAppContext) {
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let identity = session("main");
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                WindowHandle::new(WindowId::from(77)),
                BindingState::Unselected,
                Vec::new(),
            )
        });
        registry.update(cx, |registry, _| {
            registry.install_connection_for_test(identity.clone(), Rc::new(FakeHandle), 1);
            registry.attach_connection_window(&identity, window_id);
            let _lock = registry.workspace_root_lock(window_id, &checkout("C:/locked"));
        });
        assert!(
            !registry.read_with(cx, |registry, _| registry
                .workspace_root_locks
                .is_empty()),
            "an open attempt registers its window/root lock"
        );

        registry.update(cx, |registry, cx| registry.note_window_released(window_id, cx));

        assert!(
            registry.read_with(cx, |registry, _| registry
                .workspace_root_locks
                .is_empty()),
            "a released window must not retain per-root locks forever"
        );
    }



    struct FakeHandle;

    impl HerdrSessionHandle for FakeHandle {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            async { Err::<Box<dyn HerdrEventStream>, _>(anyhow::anyhow!("unused fake stream")) }
                .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            async { Err::<SessionSnapshot, _>(anyhow::anyhow!("unused fake snapshot")) }
                .boxed_local()
        }

        fn pane(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
            async { Err::<herdr::PaneInfo, _>(anyhow::anyhow!("unused fake pane")) }.boxed_local()
        }
        fn focus_agent(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            async { Ok(()) }.boxed_local()
        }

        fn focus_workspace(
            &self,
            _workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            async { Ok(()) }.boxed_local()
        }
    }

    struct FakeStream {
        /// When true the stream never yields; when false it ends immediately,
        /// driving the registry's stream-loss path.
        idle: bool,
        event: Option<HerdrEvent>,
    }

    impl HerdrEventStream for FakeStream {
        fn next(&mut self) -> LocalBoxFuture<'_, anyhow::Result<Option<HerdrEvent>>> {
            if let Some(event) = self.event.take() {
                async move { Ok(Some(event)) }.boxed_local()
            } else if self.idle {
                async {
                    futures::future::pending::<()>().await;
                    Ok(None)
                }
                .boxed_local()
            } else {
                async { Ok(None) }.boxed_local()
            }
        }
    }

    /// Handle with a configurable snapshot and idle stream; counts client
    /// drops through the shared `dropped` counter so tests can observe
    /// connection teardown.
    struct FakeConnection {
        snapshot: SessionSnapshot,
        dropped: Rc<Cell<usize>>,
        subscribe_calls: Rc<Cell<usize>>,
        idle_stream: bool,
        event: Option<HerdrEvent>,
    }
    impl Drop for FakeConnection {
        fn drop(&mut self) {
            self.dropped.set(self.dropped.get() + 1);
        }
    }

    impl HerdrSessionHandle for FakeConnection {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            self.subscribe_calls
                .set(self.subscribe_calls.get().saturating_add(1));
            let idle = self.idle_stream;
            let event = self.event.clone();
            async move { Ok(Box::new(FakeStream { idle, event }) as Box<dyn HerdrEventStream>) }
                .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            let snapshot = self.snapshot.clone();
            async move { Ok(snapshot) }.boxed_local()
        }
        fn pane(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
            async { Err::<herdr::PaneInfo, _>(anyhow::anyhow!("unused fake pane")) }.boxed_local()
        }
        fn focus_agent(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            async { Ok(()) }.boxed_local()
        }

        fn focus_workspace(
            &self,
            _workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            async { Ok(()) }.boxed_local()
        }
    }

    fn empty_snapshot() -> SessionSnapshot {
        SessionSnapshot {
            version: "test".to_owned(),
            protocol: 1,
            focused_workspace_id: None,
            focused_tab_id: None,
            focused_pane_id: None,
            workspaces: Vec::new(),
            tabs: Vec::new(),
            panes: Vec::new(),
            layouts: Vec::new(),
            agents: Vec::new(),
        }
    }

    fn workspace_without_checkout() -> herdr::WorkspaceInfo {
        herdr::WorkspaceInfo {
            workspace_id: "workspace".to_owned(),
            number: 1,
            label: "test".to_owned(),
            focused: false,
            pane_count: 0,
            tab_count: 0,
            active_tab_id: None,
            agent_status: "idle".to_owned(),
            worktree: None,
        }
    }

    fn workspace_with_checkout(path: &str) -> herdr::WorkspaceInfo {
        let mut workspace = workspace_without_checkout();
        workspace.worktree = Some(herdr::WorkspaceWorktreeInfo {
            checkout_path: path.to_owned(),
            repo_root: None,
            repo_key: None,
            repo_name: None,
            is_linked_worktree: false,
        });
        workspace
    }

    #[test]
    fn snapshot_checkout_paths_preserve_order_and_deduplicate() {
        let mut snapshot = empty_snapshot();
        snapshot.workspaces = vec![
            workspace_with_checkout("C:/repo/one"),
            workspace_with_checkout("C:/repo/two"),
            workspace_with_checkout("c:/repo/one"),
            workspace_without_checkout(),
        ];

        assert_eq!(
            snapshot_checkout_paths(&snapshot),
            vec![checkout("C:/repo/one"), checkout("C:/repo/two")]
        );
    }

    struct RecordingEffects {
        effects: Rc<RefCell<Vec<WorkspaceEffect>>>,
    }

    impl AgentEffectSink for RecordingEffects {
        fn push(&self, _identity: &SessionIdentity, effect: &WorkspaceEffect) {
            self.effects.borrow_mut().push(effect.clone());
        }
    }
    struct RecordingHost {
        detached: Rc<RefCell<Vec<WindowId>>>,
    }

    impl HerdrHostSink for RecordingHost {
        fn install(
            &self,
            _window: WindowHandle<MultiWorkspace>,
            _launch: HerdrLaunch,
            _cx: &mut Context<HerdrSessionRegistry>,
        ) {
        }

        fn detach(
            &self,
            window: WindowHandle<MultiWorkspace>,
            _cx: &mut Context<HerdrSessionRegistry>,
        ) {
            self.detached.borrow_mut().push(window.window_id());
        }
    }

    #[gpui::test]
    async fn finalized_timeout_detaches_only_timed_out_window(cx: &mut TestAppContext) {
        init_app(cx);
        let _app_state = import_test_app(cx).await;
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let identity = session("main");
        let target = WindowHandle::<MultiWorkspace>::new(WindowId::from(41));
        let owner = WindowHandle::<MultiWorkspace>::new(WindowId::from(42));
        let detached = Rc::new(RefCell::new(Vec::new()));
        let effects = Rc::new(RefCell::new(Vec::new()));
        let (target_id, owner_id) = registry.update(cx, |registry, _| {
            registry.set_host_sink(Rc::new(RecordingHost {
                detached: detached.clone(),
            }));
            registry.set_effect_sink(Rc::new(RecordingEffects {
                effects: effects.clone(),
            }));
            let target_id = registry.register_window_for_test(
                target,
                BindingState::Starting {
                    session_name: Arc::from("main"),
                },
                Vec::new(),
            );
            let owner_id = registry.register_window_for_test(
                owner,
                BindingState::Connected(identity.clone()),
                Vec::new(),
            );
            registry.attempts.insert(target_id.as_u64(), 2);
            registry.install_connection(identity.clone(), Rc::new(FakeHandle), Task::ready(()), 1);
            assert!(registry.attach_connection_window(&identity, owner_id));
            assert!(registry.attach_connection_window(&identity, target_id));
            registry
                .slot_generations
                .insert((identity.clone(), target_id.as_u64()), 2);
            registry.finalize_tokens.insert(target_id.as_u64(), 2);
            let root = if cfg!(windows) { "C:/root" } else { "/root" };
            assert_eq!(
                registry
                    .sync
                    .upsert(identity.clone(), test_pane("terminal-1", "pane-1", 1, root))
                    .len(),
                1
            );
            (target_id, owner_id)
        });

        let mut async_cx = cx.to_async();
        fail_after_finalize(
            registry.clone(),
            target_id,
            &identity,
            "main",
            2,
            "snapshot workspace import timed out".into(),
            &mut async_cx,
        );
        drop(async_cx);

        assert!(matches!(
            registry.read_with(cx, |registry, _| registry.binding_state(target_id)),
            BindingState::Failed { .. }
        ));
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry.finalize_tokens.get(&target_id.as_u64()).copied()
            }),
            None
        );
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.bound_window_count(&identity)),
            1,
            "timing out a shared window must preserve the owner's connection"
        );
        assert_eq!(&*detached.borrow(), &[target_id]);
        assert!(
            effects.borrow().is_empty(),
            "timeout cleanup must not dispatch agent effects"
        );
        let root = if cfg!(windows) { "C:/root" } else { "/root" };
        let mut snapshot = empty_snapshot();
        snapshot
            .agents
            .push(test_pane("terminal-1", "pane-1", 1, root));
        let mut snapshot_workspace = workspace_without_checkout();
        snapshot_workspace.workspace_id = "workspace-1".to_owned();
        snapshot_workspace.worktree = Some(herdr::WorkspaceWorktreeInfo {
            checkout_path: root.to_owned(),
            repo_root: None,
            repo_key: None,
            repo_name: None,
            is_linked_worktree: false,
        });
        snapshot.workspaces.push(snapshot_workspace);
        let other = if cfg!(windows) {
            "C:/other-root"
        } else {
            "/other-root"
        };
        registry.update(cx, |registry, cx| {
            registry.dispatch_snapshot_effects(
                &identity,
                &snapshot,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        let key = AgentKey::new(identity.clone(), "terminal-1");
        assert!(registry.read_with(cx, |registry, _| {
            registry
                .snapshot_bootstrapped
                .contains(&(identity.clone(), 1))
                && registry.mirroring_in_flight.contains(&(key.clone(), 1))
        }));
        let mut updated_workspace = workspace_without_checkout();
        updated_workspace.workspace_id = "workspace-1".to_owned();
        updated_workspace.worktree = Some(herdr::WorkspaceWorktreeInfo {
            checkout_path: other.to_owned(),
            repo_root: None,
            repo_key: None,
            repo_name: None,
            is_linked_worktree: false,
        });
        registry.update(cx, |registry, _| {
            registry
                .sync
                .apply_workspace(&identity, &updated_workspace);
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry.sync.workspace_checkout(&identity, "workspace-1")
            }),
            Some(PathBuf::from(other))
        );
        registry.update(cx, |registry, cx| {
            registry.dispatch_snapshot_effects(
                &identity,
                &snapshot,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry.sync.workspace_checkout(&identity, "workspace-1")
            }),
            Some(PathBuf::from(other)),
            "a stale same-generation snapshot must not regress checkout routing"
        );
        registry.update(cx, |registry, cx| {
            let effects = registry.sync.exit(&identity, "pane-1");
            assert_eq!(effects.len(), 1);
            registry.dispatch_effects(
                &identity,
                effects,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        assert!(registry.read_with(cx, |registry, _| {
            registry.sync.record(&key).is_none()
        }));
        registry.update(cx, |registry, cx| {
            registry.dispatch_snapshot_effects(
                &identity,
                &snapshot,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        assert!(registry.read_with(cx, |registry, _| {
            registry.sync.record(&key).is_none()
        }));
        registry.update(cx, |registry, cx| {
            let effects =
                registry
                    .sync
                    .upsert(identity.clone(), test_pane("terminal-1", "pane-1", 2, other));
            assert_eq!(effects.len(), 1);
            registry.dispatch_effects(
                &identity,
                effects,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry
                    .pending_mirror_replay
                    .get(&(key.clone(), 1))
                    .map(|(record, _)| record.revision)
            }),
            Some(2)
        );
        registry.update(cx, |registry, cx| {
            registry.dispatch_snapshot_effects(
                &identity,
                &snapshot,
                Some(MirrorTarget {
                    window_id: owner_id,
                    generation: 1,
                }),
                cx,
            );
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry
                    .pending_mirror_replay
                    .get(&(key.clone(), 1))
                    .map(|(record, _)| record.revision)
            }),
            Some(2),
            "a later stale snapshot must not reorder buffered events"
        );
    }



    fn test_pane(terminal_id: &str, pane_id: &str, revision: u64, path: &str) -> herdr::PaneInfo {
        herdr::PaneInfo {
            workspace_id: "workspace-1".to_owned(),
            tab_id: "tab-1".to_owned(),
            pane_id: pane_id.to_owned(),
            terminal_id: terminal_id.to_owned(),
            focused: true,
            revision,
            agent: Some("claude".to_owned()),
            agent_status: Some("working".to_owned()),
            cwd: Some(path.to_owned()),
            foreground_cwd: None,
            agent_session: None,
        }
    }

    #[gpui::test]
    async fn mirror_spawn_failure_notifies_once_and_resyncs_live_agents(cx: &mut TestAppContext) {
        init_agent_app(cx);
        let fs = fs::FakeFs::new(cx.executor());
        fs.insert_tree("C:/root", serde_json::json!({"file.txt": ""}))
            .await;
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        let project = project::Project::test(fs, [std::path::Path::new("C:/root")], cx).await;
        let (_workspace, window, panel, terminal_id) = add_agent_window(cx, &project).await;
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-failed");
        let other_key = AgentKey::new(identity.clone(), "terminal-other");
        let registry = cx.update(|cx| {
            cx.new(|cx| {
                HerdrSessionRegistry::test(
                    cx,
                    HerdrGateway::fake(
                        || async { Ok(Vec::new()) }.boxed_local(),
                        |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
                        |_name| async { Ok(()) }.boxed_local(),
                    ),
                )
            })
        });
        let workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .expect("test window remains open");
        registry.update(cx, |registry, cx| {
            registry.install_connection_for_test(identity.clone(), Rc::new(FakeHandle), 1);
            registry.register_window_for_test(
                window,
                BindingState::Connected(identity.clone()),
                vec![checkout("C:/root")],
            );
            registry.sync.upsert(
                identity.clone(),
                test_pane("terminal-failed", "pane-failed", 1, "C:/root"),
            );
            registry.mirror_index.insert(key.clone(), terminal_id);
            registry.mirrors.insert(
                key.clone(),
                AgentMirror {
                    window,
                    panel: panel.downgrade(),
                    terminal_id,
                },
            );
            registry.register_panel_observer(panel.clone(), window, cx);
        });
        panel.update(cx, |_, cx| {
            cx.emit(AgentPanelEvent::ExternalTerminalSpawnFinished {
                identity: HerdrSessionRegistry::external_identity(&key).into(),
                message: Some("direct terminal attach is not supported on Windows yet".into()),
            });
        });
        cx.run_until_parked();
        assert_eq!(
            workspace.read_with(cx, |workspace, _| workspace.notification_ids().len()),
            1,
            "the registry emits exactly one per-agent actionable notification"
        );
        // A second failing agent gets its own notification (composite ids
        // are per session-dir/terminal), and the attach-exit path reports
        // through the same registry-owned channel.
        let third_key = AgentKey::new(identity.clone(), "terminal-third");
        registry.update(cx, |registry, _| {
            registry.sync.upsert(
                identity.clone(),
                test_pane("terminal-third", "pane-third", 1, "C:/root"),
            );
            registry.mirror_index.insert(third_key.clone(), terminal_id);
        });
        panel.update(cx, |_, cx| {
            cx.emit(AgentPanelEvent::ExternalTerminalAttachFinished {
                identity: HerdrSessionRegistry::external_identity(&third_key).into(),
                message: Some("direct terminal attach is not supported on Windows yet".into()),
            });
        });
        cx.run_until_parked();
        assert_eq!(
            workspace.read_with(cx, |workspace, _| {
                let ids = workspace.notification_ids();
                let unique = ids.iter().collect::<std::collections::HashSet<_>>();
                (ids.len(), unique.len())
            }),
            (2, 2),
            "two failing agents produce two distinct notifications"
        );
        assert!(
            registry.read_with(cx, |registry, _| !registry.mirrors.contains_key(&key)),
            "a failed terminal is forgotten without dismissing its live reducer record"
        );
        assert!(
            !registry.update(cx, |registry, _| registry.sync.record_failure(&key, 1)),
            "record_failure gates the same revision after handle_external_spawn_finished"
        );
        let other_effects = registry.update(cx, |registry, _| {
            registry.sync.upsert(
                identity.clone(),
                test_pane("terminal-other", "pane-other", 1, "C:/root"),
            )
        });
        assert!(matches!(
            other_effects.as_slice(),
            [AgentSyncEffect::Open(record)] if record.key == other_key
        ));
        let revision_effects = registry.update(cx, |registry, _| {
            registry.sync.upsert(
                identity.clone(),
                test_pane("terminal-failed", "pane-failed-2", 2, "C:/root"),
            )
        });
        assert!(matches!(
            revision_effects.as_slice(),
            [AgentSyncEffect::Open(record)] if record.key == key
        ));
        let replay = registry.update(cx, |registry, _| registry.sync.resync(&identity));
        assert_eq!(
            replay
                .iter()
                .filter(
                    |effect| matches!(effect, AgentSyncEffect::Open(record) if record.key == key)
                )
                .count(),
            1,
            "an explicit resync retries the failed live agent exactly once"
        );
    }
    #[test]
    fn path_change_emits_open_effect_for_unchanged_pane() {
        let root = if cfg!(windows) { "C:/root" } else { "/root" };
        let other = if cfg!(windows) {
            "C:/other-root"
        } else {
            "/other-root"
        };
        let identity = session("main");
        let mut sync = AgentSyncState::default();

        assert_eq!(
            sync.upsert(identity.clone(), test_pane("terminal-1", "pane-1", 1, root))
                .len(),
            1
        );
        let effects = sync.upsert(
            identity,
            test_pane("terminal-1", "pane-1", 2, other),
        );
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Open(record)] if record.revision == 2
        ));
    }

    #[gpui::test]
    fn stale_mirror_failure_after_pane_exit_is_suppressed(cx: &mut TestAppContext) {
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-1");

        registry.update(cx, |registry, cx| {
            assert_eq!(
                registry
                    .sync
                    .upsert(
                        identity.clone(),
                        test_pane("terminal-1", "pane-1", 1, "C:/root")
                    )
                    .len(),
                1
            );
            assert_eq!(registry.sync.exit(&identity, "pane-1").len(), 1);
            registry.report_mirror_failure(
                &key,
                1,
                "stale failure".into(),
                None,
                cx,
            );
        });

        assert!(registry.update(cx, |registry, _| {
            registry.record_mirror_failure(&key, 1)
        }));
    }

    fn agent_snapshot() -> SessionSnapshot {
        let mut snapshot = empty_snapshot();
        snapshot.focused_workspace_id = Some("workspace-1".to_owned());
        snapshot.workspaces.push(herdr::WorkspaceInfo {
            workspace_id: "workspace-1".to_owned(),
            number: 1,
            label: "test".to_owned(),
            focused: true,
            pane_count: 1,
            tab_count: 1,
            active_tab_id: Some("tab-1".to_owned()),
            agent_status: "working".to_owned(),
            worktree: None,
        });
        let mut agent = herdr::PaneInfo::default();
        agent.workspace_id = "workspace-1".to_owned();
        agent.tab_id = "tab-1".to_owned();
        agent.pane_id = "pane-1".to_owned();
        agent.terminal_id = "terminal-1".to_owned();
        agent.focused = true;
        agent.revision = 1;
        agent.agent = Some("test-agent".to_owned());
        snapshot.agents.push(agent);
        snapshot
    }

    fn init_app(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = settings::SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            project::DisableAiSettings::register(cx);
            terminal_view::init(cx);
        });
    }
    fn init_agent_app(cx: &mut TestAppContext) {
        agent_ui::test_support::init_test(cx);
        cx.update(|cx| {
            ThreadStore::init_global(cx);
            agent_ui::thread_metadata_store::ThreadMetadataStore::init_global(cx);
            language_model::init(cx);
            project::DisableAiSettings::register(cx);
        });
    }
    #[gpui::test]
    async fn startup_prompt_opens_picker_once_and_explicit_selection_cancels(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        cx.update(|cx| editor::init(cx));
        let project = test_project(cx).await;
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| {
                async { Err(anyhow::anyhow!("startup picker test never connects")) }.boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        registry.update(cx, |registry, _| {
            registry.set_picker_sink(crate::zed::herdr_session_picker::sink());
        });
        cx.update(|cx| HerdrSessionRegistry::install_as_global(registry.clone(), cx));

        let (_multi_workspace, first_window, first_workspace) = {
            let (multi_workspace, window_cx) = cx.add_window_view(|window, cx| {
                MultiWorkspace::test_new(project.clone(), window, cx)
            });
            let handle = window_cx
                .update(|window, _| window.window_handle().downcast::<MultiWorkspace>().unwrap());
            let workspace = multi_workspace.read_with(window_cx, |multi_workspace, _| {
                multi_workspace.workspace().clone()
            });
            (multi_workspace, handle, workspace)
        };
        let first_window_id = registry.update(cx, |registry, cx| {
            registry.handle_new_window(first_window, cx);
            first_window.window_id()
        });
        assert!(
            registry.read_with(cx, |registry, _| registry
                .prompt_pending
                .contains(&first_window_id)),
            "a newly observed unselected window must have a pending startup prompt"
        );

        cx.background_executor.advance_clock(PROMPT_SETTLE_DELAY);
        cx.run_until_parked();
        let first_picker = first_workspace
            .update(cx, |workspace, cx| {
                workspace.active_modal::<crate::zed::herdr_session_picker::SessionPicker>(cx)
            })
            .expect("startup settle must open the session picker");
        let first_picker_id = first_picker.entity_id();
        assert!(
            registry.read_with(cx, |registry, _| !registry
                .prompt_pending
                .contains(&first_window_id)),
            "showing the startup picker must consume its pending prompt"
        );

        cx.background_executor.advance_clock(PROMPT_SETTLE_DELAY);
        cx.run_until_parked();
        let second_picker_id = first_workspace
            .update(cx, |workspace, cx| {
                workspace
                    .active_modal::<crate::zed::herdr_session_picker::SessionPicker>(cx)
                    .map(|picker| picker.entity_id())
            })
            .expect("the startup picker must remain open after its one-shot task");
        assert_eq!(
            second_picker_id, first_picker_id,
            "the startup prompt must show exactly once"
        );
        let second_project = test_project(cx).await;

        let (_second_multi_workspace, second_window, second_workspace) = {
            let (multi_workspace, window_cx) = cx
                .add_window_view(|window, cx| MultiWorkspace::test_new(second_project, window, cx));
            let handle = window_cx
                .update(|window, _| window.window_handle().downcast::<MultiWorkspace>().unwrap());
            let workspace = multi_workspace.read_with(window_cx, |multi_workspace, _| {
                multi_workspace.workspace().clone()
            });
            (multi_workspace, handle, workspace)
        };
        let second_window_id = registry.update(cx, |registry, cx| {
            registry.handle_new_window(second_window, cx);
            second_window.window_id()
        });
        assert!(
            registry.read_with(cx, |registry, _| registry
                .prompt_pending
                .contains(&second_window_id)),
            "the second unselected window must also defer its startup prompt"
        );

        cx.update(|cx| {
            HerdrSessionRegistry::select_or_create_for_window(second_window_id, cx);
        });
        cx.run_until_parked();
        let explicit_picker = second_workspace
            .update(cx, |workspace, cx| {
                workspace.active_modal::<crate::zed::herdr_session_picker::SessionPicker>(cx)
            })
            .expect("explicit selection must open the picker immediately");
        let explicit_picker_id = explicit_picker.entity_id();
        assert!(
            !registry.read_with(cx, |registry, _| registry
                .prompt_pending
                .contains(&second_window_id)),
            "explicit selection must cancel the delayed startup prompt"
        );

        cx.background_executor.advance_clock(PROMPT_SETTLE_DELAY);
        cx.run_until_parked();
        let after_cancel_picker_id = second_workspace
            .update(cx, |workspace, cx| {
                workspace
                    .active_modal::<crate::zed::herdr_session_picker::SessionPicker>(cx)
                    .map(|picker| picker.entity_id())
            })
            .expect("the canceled startup task must not toggle off the explicit picker");
        assert_eq!(after_cancel_picker_id, explicit_picker_id);
    }

    /// A real `MultiWorkspace` window: `run_connection` refuses to bind
    /// through a dead window handle, so lifecycle tests need live ones.
    async fn add_real_window(
        cx: &mut TestAppContext,
        project: &Entity<project::Project>,
    ) -> WindowHandle<MultiWorkspace> {
        let (_multi_workspace, vcx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        vcx.update(|window, _| window.window_handle().downcast::<MultiWorkspace>().unwrap())
    }


    async fn add_agent_window(
        cx: &mut TestAppContext,
        project: &Entity<project::Project>,
    ) -> (
        Entity<MultiWorkspace>,
        WindowHandle<MultiWorkspace>,
        Entity<AgentPanel>,
        TerminalId,
    ) {
        let (multi_workspace, vcx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        let workspace = multi_workspace.read_with(vcx, |multi_workspace, _| {
            multi_workspace.workspace().clone()
        });
        let panel = workspace.update_in(vcx, |workspace, window, cx| {
            let panel = cx.new(|cx| AgentPanel::test_new(workspace, window, cx));
            workspace.add_panel(panel.clone(), window, cx);
            panel
        });
        let terminal_id = panel
            .update_in(vcx, |panel, window, cx| {
                panel.insert_test_terminal("herdr test", true, window, cx)
            })
            .expect("test terminal should be inserted");
        let handle =
            vcx.update(|window, _| window.window_handle().downcast::<MultiWorkspace>().unwrap());
        (multi_workspace, handle, panel, terminal_id)
    }
    async fn test_project(cx: &mut TestAppContext) -> Entity<project::Project> {
        let fs = fs::FakeFs::new(cx.executor());
        fs.insert_tree("/root", serde_json::json!({ "file.txt": "" }))
            .await;
        project::Project::test(fs, [std::path::Path::new("/root")], cx).await
    }

    fn roots_in_window(
        window: WindowHandle<MultiWorkspace>,
        cx: &mut TestAppContext,
    ) -> Vec<herdr::CanonicalPath> {
        window
            .read_with(cx, |multi_workspace, cx| {
                multi_workspace
                    .workspaces()
                    .flat_map(|workspace| {
                        workspace
                            .read(cx)
                            .project()
                            .read(cx)
                            .worktrees(cx)
                            .filter_map(|worktree| worktree.read(cx).root_dir())
                            .filter_map(|root| canonical_checkout_path(root.as_ref()).ok())
                            .collect::<Vec<_>>()
                    })
                    .collect()
            })
            .expect("test window remains open")
    }


    async fn import_test_app(cx: &mut TestAppContext) -> Arc<AppState> {
        let app_state = cx.update(|cx| {
            let app_state = AppState::test(cx);
            AppState::set_global(app_state.clone(), cx);
            <dyn Fs>::set_global(app_state.fs.clone(), cx);
            app_state
        });
        app_state
            .fs
            .as_fake()
            .insert_tree("C:/existing", serde_json::json!({ "file.txt": "" }))
            .await;
        app_state
            .fs
            .as_fake()
            .insert_tree("C:/space-a", serde_json::json!({ "file.txt": "" }))
            .await;
        app_state
            .fs
            .as_fake()
            .insert_tree("C:/space-b", serde_json::json!({ "file.txt": "" }))
            .await;
        app_state
            .fs
            .as_fake()
            .insert_tree("C:/agent-root", serde_json::json!({ "file.txt": "" }))
            .await;
        app_state
    }

    #[gpui::test]
    async fn initial_snapshot_imports_all_spaces_into_invoking_window(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        let app_state = import_test_app(cx).await;
        let project = project::Project::test(
            app_state.fs.clone(),
            [std::path::Path::new("C:/existing")],
            cx,
        )
        .await;
        let window = add_real_window(cx, &project).await;
        let mut snapshot = empty_snapshot();
        snapshot.workspaces = vec![
            workspace_with_checkout("C:/space-a"),
            workspace_with_checkout("C:/space-b"),
        ];
        let gateway = HerdrGateway::fake(
            || async { Ok(vec![session_info("main", true)]) }.boxed_local(),
            move |_info| {
                let snapshot = snapshot.clone();
                async move {
                    Ok(Rc::new(FakeConnection {
                        snapshot,
                        dropped: Rc::new(Cell::new(0)),
                        subscribe_calls: Rc::new(Cell::new(0)),
                        idle_stream: true,
                        event: None,
                    }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(window, BindingState::Unselected, Vec::new())
        });
        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(window_id, Arc::from("main"), cx);
        });
        cx.run_until_parked();

        assert_eq!(cx.windows().len(), 1);
        assert_eq!(
            roots_in_window(window, cx),
            vec![
                checkout("C:/existing"),
                checkout("C:/space-a"),
                checkout("C:/space-b")
            ]
        );
        assert!(matches!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Connected(_)
        ));
    }

    #[gpui::test]
    async fn unmatched_agent_root_stays_in_invoking_window(cx: &mut TestAppContext) {
        init_app(cx);
        init_agent_app(cx);
        let app_state = import_test_app(cx).await;
        let agent_root = PathBuf::from("C:/space-a");
        app_state
            .fs
            .as_fake()
            .insert_tree(&agent_root, serde_json::json!({ "file.txt": "" }))
            .await;
        let project = project::Project::test(
            app_state.fs.clone(),
            [std::path::Path::new("C:/existing")],
            cx,
        )
        .await;
        let (_multi_workspace, window, _panel, _terminal_id) =
            add_agent_window(cx, &project).await;
        let mut snapshot = empty_snapshot();
        snapshot.workspaces = vec![workspace_without_checkout()];
        let mut agent = herdr::PaneInfo::default();
        agent.workspace_id = "workspace".to_owned();
        agent.tab_id = "tab-1".to_owned();
        agent.pane_id = "pane-1".to_owned();
        agent.terminal_id = "terminal-1".to_owned();
        agent.focused = true;
        agent.revision = 1;
        agent.agent = Some("test-agent".to_owned());
        agent.cwd = Some(agent_root.to_string_lossy().into_owned());
        snapshot.agents.push(agent);
        let gateway = HerdrGateway::fake(
            || async { Ok(vec![session_info("main", true)]) }.boxed_local(),
            move |_info| {
                let snapshot = snapshot.clone();
                async move {
                    Ok(Rc::new(FakeConnection {
                        snapshot,
                        dropped: Rc::new(Cell::new(0)),
                        subscribe_calls: Rc::new(Cell::new(0)),
                        idle_stream: true,
                        event: None,
                    }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(window, BindingState::Unselected, Vec::new())
        });
        let mirror_targets = Rc::new(RefCell::new(Vec::new()));
        registry.update(cx, |registry, cx| {
            registry.set_mirror_target_sink_for_test(mirror_targets.clone());
            registry.start_binding_for_test(window_id, Arc::from("main"), cx);
        });
        cx.run_until_parked();

        assert_eq!(cx.windows().len(), 1);
        let expected_root =
            canonical_checkout_path(&agent_root).expect("temporary agent root canonicalizes");
        assert!(
            roots_in_window(window, cx).contains(&expected_root),
            "the unmatched agent checkout should be added to the invoking window"
        );
        let expected_key = AgentKey::new(session("main"), "terminal-1");
        assert_eq!(
            mirror_targets.borrow().as_slice(),
            &[(expected_key, window.window_id())],
            "the unmatched agent mirror should target the invoking window"
        );
    }

    #[gpui::test]
    async fn connected_selection_reuses_invoking_window(cx: &mut TestAppContext) {
        init_app(cx);
        let project = test_project(cx).await;
        let invoking = add_real_window(cx, &project).await;
        let old_identity = session("old");
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            let window_id = registry.register_window_for_test(
                invoking,
                BindingState::Connected(old_identity.clone()),
                Vec::new(),
            );
            registry.install_connection_for_test(old_identity.clone(), Rc::new(FakeHandle), 1);
            registry.attach_connection_window(&old_identity, window_id);
            window_id
        });

        registry.update(cx, |registry, cx| {
            registry.confirm_selection_for_test(
                invoking,
                SessionSelection::New {
                    name: "new".to_owned(),
                },
                cx,
            );
            assert_eq!(
                registry.binding_state(window_id),
                BindingState::Starting {
                    session_name: Arc::from("new"),
                }
            );
            assert_eq!(registry.bound_window_count(&old_identity), 0);
            assert!(!registry.has_connection(&old_identity));
        });
        assert_eq!(cx.windows().len(), 1);
    }

    #[gpui::test]
    async fn stream_loss_refresh_classifies_missing_session_without_reconnect(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        let project = test_project(cx).await;
        let handle = add_real_window(cx, &project).await;
        let list_calls = Rc::new(Cell::new(0usize));
        let calls = list_calls.clone();
        let gateway = HerdrGateway::fake(
            move || {
                let calls = calls.clone();
                let count = calls.get();
                calls.set(count + 1);
                let sessions = if count == 0 {
                    vec![session_info("main", true)]
                } else {
                    Vec::new()
                };
                async move { Ok(sessions) }.boxed_local()
            },
            |_info| {
                async move {
                    Ok(Rc::new(FakeConnection {
                        snapshot: empty_snapshot(),
                        dropped: Rc::new(Cell::new(0)),
                        subscribe_calls: Rc::new(Cell::new(0)),
                        idle_stream: false,
                        event: None,
                    }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(handle, BindingState::Unselected, Vec::new())
        });

        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_id,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();

        // The real seam ran: one connection task, and the stream ended, so
        // the catalog was refreshed exactly once after the initial list.
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.connection_tasks_started()),
            1
        );
        assert_eq!(list_calls.get(), 2);
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Unselected,
            "a missing session after stream loss is a normal exit"
        );
        assert!(!registry.read_with(cx, |registry, _| registry.has_connection(&session("main"))));
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.connection_tasks_started()),
            1,
            "stream loss must not spawn a reconnect task"
        );
    }

    #[gpui::test]
    async fn owner_bootstrap_failure_notifies_before_connected(cx: &mut TestAppContext) {
        init_app(cx);
        let project = test_project(cx).await;
        let window = add_real_window(cx, &project).await;
        let workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .expect("test window remains open");
        let gateway = HerdrGateway::fake(
            || async { Ok(vec![session_info("main", true)]) }.boxed_local(),
            |_info| {
                async {
                    Ok(Rc::new(FakeConnection {
                        snapshot: agent_snapshot(),
                        dropped: Rc::new(Cell::new(0)),
                        subscribe_calls: Rc::new(Cell::new(0)),
                        idle_stream: true,
                        event: None,
                    }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(window, BindingState::Unselected, Vec::new())
        });
        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_id,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(
            workspace.read_with(cx, |workspace, _| workspace.notification_ids().len()),
            1,
            "an owner-bootstrap failure must produce one actionable notification"
        );
    }

    #[gpui::test]
    async fn connected_session_failure_never_targets_newer_same_name_starting_window(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        let project_a = test_project(cx).await;
        let window_a = add_real_window(cx, &project_a).await;
        let project_b = test_project(cx).await;
        let window_b = add_real_window(cx, &project_b).await;
        let workspace_a = window_a
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .expect("window A remains open");
        let workspace_b = window_b
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .expect("window B remains open");
        let identity = session("main");
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let (window_a_id, _window_b_id, connected_id) = registry.update(cx, |registry, _| {
            let window_a_id =
                registry.register_window_for_test(window_a, BindingState::Unselected, Vec::new());
            let window_b_id =
                registry.register_window_for_test(window_b, BindingState::Unselected, Vec::new());
            let first = *registry
                .windows
                .keys()
                .next()
                .expect("two windows are registered");
            let starting_id = WindowId::from(first);
            let connected_id = if starting_id == window_a_id {
                window_b_id
            } else {
                window_a_id
            };
            registry
                .windows
                .get_mut(&starting_id.as_u64())
                .expect("starting window remains registered")
                .state = BindingState::Starting {
                session_name: Arc::from("main"),
            };
            registry.attempts.insert(starting_id.as_u64(), 2);
            registry
                .windows
                .get_mut(&connected_id.as_u64())
                .expect("connected window remains registered")
                .state = BindingState::Connected(identity.clone());
            registry.attempts.insert(connected_id.as_u64(), 1);
            registry.install_connection_for_test(identity.clone(), Rc::new(FakeHandle), 1);
            registry.attach_connection_window(&identity, connected_id);
            (window_a_id, window_b_id, connected_id)
        });
        let (connected_workspace, starting_workspace) = if connected_id == window_a_id {
            (workspace_a, workspace_b)
        } else {
            (workspace_b, workspace_a)
        };
        let key = AgentKey::new(identity.clone(), "terminal-1");
        let record = registry.update(cx, |registry, _| {
            let mut pane = test_pane("terminal-1", "pane-1", 1, "relative-agent-path");
            pane.workspace_id = "agent-without-checkout".to_owned();
            registry.sync.upsert(identity.clone(), pane);
            registry.sync.record(&key).expect("agent record is live")
        });
        registry.update(cx, |registry, cx| {
            registry.dispatch_effects(&identity, vec![AgentSyncEffect::Open(record)], None, cx);
        });

        assert_eq!(
            connected_workspace.read_with(cx, |workspace, _| workspace.notification_ids().len()),
            1,
            "a connected session's failure must target its bound window"
        );
        assert_eq!(
            starting_workspace.read_with(cx, |workspace, _| workspace.notification_ids().len()),
            0,
            "a newer same-name Starting attempt must never receive the failure"
        );
    }

    #[gpui::test]
    async fn shared_connection_is_kept_until_last_window_and_then_can_be_recreated(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        let project_one = test_project(cx).await;
        let handle_one = add_real_window(cx, &project_one).await;
        let project_two = test_project(cx).await;
        let handle_two = add_real_window(cx, &project_two).await;
        let identity = session("main");
        let dropped = Rc::new(Cell::new(0usize));
        let connect_calls = Rc::new(Cell::new(0usize));
        let dropped_for_gateway = dropped.clone();
        let connect_calls_for_gateway = connect_calls.clone();
        let subscribe_calls = Rc::new(Cell::new(0usize));
        let subscribe_calls_for_gateway = subscribe_calls.clone();
        let gateway = HerdrGateway::fake(
            move || async { Ok(vec![session_info("main", true)]) }.boxed_local(),
            move |_info| {
                connect_calls_for_gateway.set(connect_calls_for_gateway.get() + 1);
                let dropped = dropped_for_gateway.clone();
                let subscribe_calls = subscribe_calls_for_gateway.clone();
                async move {
                    Ok(Rc::new(FakeConnection {
                        snapshot: agent_snapshot(),
                        dropped,
                        subscribe_calls,
                        idle_stream: true,
                        event: None,
                    }) as Rc<dyn HerdrSessionHandle>)
                }
                .boxed_local()
            },
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let recorded_effects = Rc::new(RefCell::new(Vec::new()));
        registry.update(cx, |registry, _| {
            registry.set_effect_sink(Rc::new(RecordingEffects {
                effects: recorded_effects.clone(),
            }));
        });
        let window_one = registry.update(cx, |registry, _| {
            registry.register_window_for_test(handle_one, BindingState::Unselected, Vec::new())
        });
        let window_two = registry.update(cx, |registry, _| {
            registry.register_window_for_test(handle_two, BindingState::Unselected, Vec::new())
        });

        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_one,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_one)),
            BindingState::Connected(identity.clone())
        );
        assert_eq!(
            subscribe_calls.get(),
            1,
            "the owner installs one event stream"
        );
        assert_eq!(
            recorded_effects
                .borrow()
                .iter()
                .filter(|effect| matches!(effect, WorkspaceEffect::Open(_)))
                .count(),
            1,
            "the initial agent revision must emit one Open effect"
        );

        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_two,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(
            connect_calls.get(),
            1,
            "the second window shares the client"
        );
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_two)),
            BindingState::Connected(identity.clone())
        );
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.bound_window_count(&identity)),
            2
        );

        // First detach keeps the shared client and the other window bound.
        registry.update(cx, |registry, cx| {
            registry.disconnect_session(window_one, cx)
        });
        assert!(registry.read_with(cx, |registry, _| registry.has_connection(&identity)));
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.bound_window_count(&identity)),
            1
        );
        assert_eq!(dropped.get(), 0);

        // Last detach releases the connection; the owner loop notices via its
        // ownership poll and drops its client clone.
        registry.update(cx, |registry, cx| {
            registry.disconnect_session(window_two, cx)
        });
        assert!(!registry.read_with(cx, |registry, _| registry.has_connection(&identity)));
        cx.executor()
            .advance_clock(OWNERSHIP_POLL + Duration::from_millis(10));
        cx.run_until_parked();
        assert_eq!(
            dropped.get(),
            1,
            "the client is released with the last window"
        );

        // Re-selection builds a fresh client.
        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_one,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();
        assert_eq!(connect_calls.get(), 2);
        assert_eq!(
            subscribe_calls.get(),
            2,
            "recreation subscribes a fresh stream"
        );
        assert_eq!(
            recorded_effects
                .borrow()
                .iter()
                .filter(|effect| matches!(effect, WorkspaceEffect::Open(_)))
                .count(),
            2,
            "the same revision must reopen after the last owner forgets it"
        );
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_one)),
            BindingState::Connected(identity)
        );
    }

    #[gpui::test]
    async fn stale_attempt_deadline_cannot_fail_a_reselected_binding(cx: &mut TestAppContext) {
        init_app(cx);
        let project = test_project(cx).await;
        let handle = add_real_window(cx, &project).await;
        // Every catalog call hangs far past the connection deadline, so both
        // attempts are still parked in their first await when the fake clock
        // reaches the first attempt's 15-second deadline.
        let executor = cx.executor();
        let gateway = HerdrGateway::fake(
            move || {
                let executor = executor.clone();
                async move {
                    executor.timer(Duration::from_secs(60)).await;
                    Ok(Vec::new())
                }
                .boxed_local()
            },
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(handle, BindingState::Unselected, Vec::new())
        });
        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_id,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();
        assert!(matches!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Starting { .. }
        ));

        // Disconnect and re-select mid-deadline: the second attempt becomes
        // the only current one.
        cx.executor().advance_clock(Duration::from_secs(5));
        registry.update(cx, |registry, cx| {
            registry.disconnect_session(window_id, cx)
        });
        registry.update(cx, |registry, cx| {
            registry.start_binding_for_test(
                window_id,
                Arc::from("main"),
                cx,
            );
        });
        cx.run_until_parked();

        // The first attempt's deadline fires now; its guarded failure must
        // leave the newer Starting binding alone.
        cx.executor().advance_clock(Duration::from_secs(10));
        cx.run_until_parked();
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Starting {
                session_name: Arc::from("main")
            },
            "a stale attempt's deadline must not fail the reselected binding"
        );

        // The current attempt still fails on its own deadline.
        cx.executor().advance_clock(Duration::from_secs(10));
        cx.run_until_parked();
        assert!(matches!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Failed { .. }
        ));
    }



    #[gpui::test]
    fn finish_connected_rejects_a_stale_target_binding(cx: &mut TestAppContext) {
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let identity = session("main");
        let target = WindowHandle::<MultiWorkspace>::new(WindowId::from(31));
        let target_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                target,
                BindingState::Failed {
                    session_name: Arc::from("main"),
                    message: "gone".into(),
                },
                Vec::new(),
            )
        });
        registry.update(cx, |registry, _| {
            registry.install_connection(identity.clone(), Rc::new(FakeHandle), Task::ready(()), 1);
            registry.attach_connection_window(&identity, target_id);
            registry.finalize_tokens.insert(target_id.as_u64(), 1);
        });

        registry.update(cx, |registry, cx| {
            registry.finish_connected(target_id, &identity, 1, cx);
        });
        assert!(
            matches!(
                registry.read_with(cx, |registry, _| registry.binding_state(target_id)),
                BindingState::Failed { .. }
            ),
            "a reuse target that changed state must not be published Connected"
        );
    }
    #[gpui::test]
    fn stale_reuse_completion_preserves_a_newer_same_session_slot(cx: &mut TestAppContext) {
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.update(|cx| cx.new(|cx| HerdrSessionRegistry::test(cx, gateway)));
        let identity = session("main");
        let target_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                WindowHandle::new(WindowId::from(32)),
                BindingState::Starting {
                    session_name: Arc::from("main"),
                },
                Vec::new(),
            )
        });
        registry.update(cx, |registry, _| {
            registry.install_connection(identity.clone(), Rc::new(FakeHandle), Task::ready(()), 1);
            registry.attach_connection_window(&identity, target_id);
            registry.finalize_tokens.insert(target_id.as_u64(), 2);
            registry
                .slot_generations
                .insert((identity.clone(), target_id.as_u64()), 2);
        });

        registry.update(cx, |registry, cx| {
            registry.finish_connected(target_id, &identity, 1, cx);
        });

        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(target_id)),
            BindingState::Starting {
                session_name: Arc::from("main"),
            }
        );
        assert_eq!(
            registry.read_with(cx, |registry, _| {
                registry.finalize_tokens.get(&target_id.as_u64()).copied()
            }),
            Some(2),
            "a stale completion must not remove the newer finalize token"
        );
        registry.update(cx, |registry, cx| {
            registry.finish_connected(target_id, &identity, 2, cx);
        });
        assert_eq!(
            registry.read_with(cx, |registry, _| registry.binding_state(target_id)),
            BindingState::Connected(identity.clone()),
            "the valid newer completion must still publish Connected"
        );
    }

    #[gpui::test]
    async fn discover_agent_root_preserves_linked_worktree_checkout(cx: &mut TestAppContext) {
        let fs = fs::FakeFs::new(cx.executor());
        fs.insert_tree(
            "C:/linked",
            serde_json::json!({
                ".git": "gitdir: C:/main/.git/worktrees/linked",
                "src": {"agent.txt": ""},
            }),
        )
        .await;
        let root =
            discover_agent_checkout_root(std::path::Path::new("C:/linked/src"), fs.as_ref()).await;
        assert_eq!(root, std::path::PathBuf::from("C:/linked"));
    }
    #[gpui::test]
    fn process_stream_event_reduces_exit_through_registry_path(cx: &mut TestAppContext) {
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-exit");
        let registry = cx.update(|cx| {
            cx.new(|cx| {
                HerdrSessionRegistry::test(
                    cx,
                    HerdrGateway::fake(
                        || async { Ok(Vec::new()) }.boxed_local(),
                        |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
                        |_name| async { Ok(()) }.boxed_local(),
                    ),
                )
            })
        });
        registry.update(cx, |registry, _| {
            registry.install_connection_for_test(identity.clone(), Rc::new(FakeHandle), 1);
            registry.sync.upsert(
                identity.clone(),
                test_pane("terminal-exit", "pane-exit", 1, "C:/root"),
            );
        });
        let event = HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Exited {
                pane_id: "pane-exit".to_owned(),
                workspace_id: "workspace-1".to_owned(),
            },
        });
        let task = registry.update(cx, |_, cx| {
            let registry = cx.entity();
            let identity = identity.clone();
            cx.spawn(async move |_this, mut cx| {
                process_stream_event(&registry, &identity, 1, event, &mut cx).await;
            })
        });
        task.detach();
        cx.run_until_parked();
        assert!(
            registry.read_with(cx, |registry, _| registry.sync.record(&key).is_none()),
            "exit stream event should remove the live agent through process_stream_event"
        );
    }

}
