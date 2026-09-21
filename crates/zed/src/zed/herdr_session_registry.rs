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
    FocusTarget, SessionIdentity,
};
use fs::Fs;
use futures::channel::mpsc;
use futures::future::{AbortHandle, Abortable, LocalBoxFuture};
use futures::lock::Mutex;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, Global, SharedString, Subscription, Task,
    WindowHandle, WindowId,
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
/// the target to the connection that owns the in-flight follow effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MirrorTarget {
    window_id: WindowId,
    generation: u64,
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

/// Select the installed Herdr binary before a possibly stale `PATH` binary.
fn select_herdr_program(
    installed_program: Option<PathBuf>,
    path_program: Option<PathBuf>,
) -> Option<PathBuf> {
    installed_program.or(path_program)
}

/// Locate the herdr CLI through its managed installation or `PATH` without
/// extra dependencies. The returned path is only a launch candidate: session
/// endpoints always come from `herdr session list --json`.
pub(crate) fn herdr_program() -> Option<PathBuf> {
    let executable_name = if cfg!(windows) { "herdr.exe" } else { "herdr" };
    #[cfg(windows)]
    let installed_program = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .map(|local_app_data| {
            local_app_data
                .join("Programs")
                .join("Herdr")
                .join("bin")
                .join(executable_name)
        })
        .filter(|candidate| candidate.is_file());
    #[cfg(not(windows))]
    let installed_program = None;
    let path_program = std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path).find_map(|directory| {
            let candidate = directory.join(executable_name);
            candidate.is_file().then_some(candidate)
        })
    });
    select_herdr_program(installed_program, path_program)
}

/// The transport seam the registry drives. Production wraps the typed
/// `herdr` client; tests inject a fake catalog/connect harness so the full
/// lifecycle runs without a real herdr process.
pub(crate) trait HerdrSessionHandle {
    fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>>;
    fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>>;
    fn pane(&self, pane_id: String) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>>;
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
    /// Selections reserved for windows while their `MultiWorkspace` root is
    /// being constructed. The window id is known before the new root is
    /// observed, so unrelated windows cannot consume the reservation.
    pending_new_window_selections: HashMap<WindowId, Arc<str>>,
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
    /// Focus echoes are keyed by session and target so workspace and agent
    /// transitions never overwrite one another.
    focus_echoes: HashMap<SessionIdentity, FocusEcho<FocusTarget>>,
    #[cfg(test)]
    sink_effects: Option<Rc<dyn AgentEffectSink>>,
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
            pending_new_window_selections: HashMap::default(),
            attempts: HashMap::default(),
            in_flight: HashMap::default(),
            snapshot_bootstrapped: HashSet::default(),
            slot_generations: HashMap::default(),
            finalize_tokens: HashMap::default(),
            snapshot_import_abort_handles: HashMap::default(),
            workspace_root_locks: HashMap::default(),
            next_generation: 0,
            sync: AgentSyncState::default(),
            focus_echoes: HashMap::default(),
            #[cfg(test)]
            sink_effects: None,
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
        let window_id = self.register_window(handle);
        // Read the new window's roots once construction finishes. The
        // entity is still being built while this observer runs, so reading
        // it here would panic; the spawned update lands after the flush.
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            let _ = registry.update(cx, |registry, cx| registry.refresh_window_roots(cx));
        })
        .detach();

        if let Some(session_name) = self.pending_new_window_selections.remove(&window_id) {
            self.prompt_completed(window_id);
            self.start_binding_for_window(window_id, session_name, cx);
        } else {
            self.schedule_startup_prompt(handle, cx);
        }
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
        let state = self.binding_state(window_id);
        let name: Arc<str> = Arc::from(selection.name());

        match state {
            BindingState::Unselected => {
                self.prompt_completed(window_id);
                self.start_binding_for_window(window_id, name, cx);
            }
            BindingState::Starting { session_name } if session_name == name => {}
            BindingState::Failed { session_name, .. } if session_name == name => {
                self.prompt_completed(window_id);
                self.start_binding_for_window(window_id, name, cx);
            }
            BindingState::Connected(identity) => {
                if let SessionSelection::Existing(info) = &selection
                    && SessionIdentity::from_info(info) == identity
                {
                    return;
                }
                self.open_selection_in_new_window(invoking, name, cx);
            }
            BindingState::Starting { .. } | BindingState::Failed { .. } => {
                self.open_selection_in_new_window(invoking, name, cx);
            }
        }
    }

    fn open_selection_in_new_window(
        &mut self,
        invoking: WindowHandle<MultiWorkspace>,
        session_name: Arc<str>,
        cx: &mut Context<Self>,
    ) {
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            let result = async {
                let (paths, app_state) = invoking
                    .read_with(cx, |multi_workspace, cx| {
                        let workspace = multi_workspace.workspace();
                        let workspace = workspace.read(cx);
                        (
                            workspace
                                .root_paths(cx)
                                .into_iter()
                                .map(|path| path.to_path_buf())
                                .collect::<Vec<_>>(),
                            workspace.app_state().clone(),
                        )
                    })
                    .map_err(|error| {
                        anyhow::anyhow!("could not inspect invoking workspace: {error:#}")
                    })?;
                let registry_for_init = registry.clone();
                let session_for_init = session_name.clone();
                let open = cx.update(|cx| {
                    Workspace::new_local(
                        paths,
                        app_state,
                        None,
                        None,
                        Some(Box::new(move |_workspace, window, cx| {
                            let window_id = window.window_handle().window_id();
                            registry_for_init.update(cx, |registry, _| {
                                registry
                                    .pending_new_window_selections
                                    .insert(window_id, session_for_init);
                            });
                        })),
                        OpenMode::NewWindow,
                        cx,
                    )
                });
                let opened = open.await?;
                Ok::<WindowHandle<MultiWorkspace>, anyhow::Error>(opened.window)
            }
            .await;

            match result {
                Ok(window) => {
                    registry.update(cx, |registry, cx| {
                        registry.claim_new_window_selection(window, cx);
                    });
                }
                Err(error) => {
                    registry.update(cx, |registry, _| {
                        registry
                            .pending_new_window_selections
                            .retain(|_, name| name != &session_name);
                    });
                    log::error!("could not open herdr session window: {error:#}");
                }
            }
        })
        .detach();
    }

    fn claim_new_window_selection(
        &mut self,
        window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<Self>,
    ) {
        let window_id = window.window_id();
        let Some(session_name) = self.pending_new_window_selections.remove(&window_id) else {
            return;
        };
        self.prompt_completed(window_id);
        self.start_binding_for_window(window_id, session_name, cx);
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
        // A fresh snapshot through the existing shared connection refreshes
        // checkout routing; reducer revision guards deduplicate the replay.
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
            self.follow_agent_workspace(record, target, cx);
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

    /// Resolve the window a follow should target when no explicit finalization
    /// slot applies. Exact connection ownership wins over any same-name
    /// in-progress selection. A `Starting` fallback is valid only for this
    /// connection generation, so a stale attempt cannot steal a follow.
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





    /// Forward a local worktree activation to herdr. `workspace_id` must be
    /// the server's `WorkspaceInfo.workspace_id` for the window's checkout;
    /// an unmapped workspace never addresses a herdr session. The checkout
    /// is re-read from the window's live worktree set first so a workspace
    /// switch or worktree add is reflected immediately.
    ///
    /// Only the reflected event consumes a token: a repeated local target
    /// supersedes its pending request and still issues a fresh RPC, so a
    /// rapid A->B->A never loses A's final switch.
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
        let Some((client, generation)) = self.shared_connection(&identity) else {
            return;
        };
        let target = FocusTarget::Workspace {
            session: identity.clone(),
            workspace_id: workspace_id.clone(),
        };
        let echo = self.focus_echoes.entry(identity.clone()).or_default();
        let token = echo.request(target);
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            if let Err(error) = client.focus_workspace(workspace_id.to_string()).await {
                log::debug!("herdr workspace focus failed: {error:#}");
                // A failed RPC cannot produce a reflected event, so release
                // only this request's token. Successful requests remain
                // pending until their reflected workspace event arrives.
                let _ = registry.update(cx, |registry, _| {
                    // A detached generation must not resolve a replacement's
                    // same-numbered token after reconnect.
                    if registry.connection_matches(&identity, generation) {
                        if let Some(echo) = registry.focus_echoes.get_mut(&identity) {
                            echo.resolve(token);
                        }
                    }
                });
            }
        })
        .detach();
    }

    /// A remote workspace focus activates the matching inner workspace of a
    /// bound window exactly once. The mapping is the workspace's recorded
    /// checkout resolved against live window roots: a connected window with
    /// *no* detected agents is still focusable. Activation changes the window,
    /// so the local observer sends exactly one focus RPC back; the token that
    /// RPC registers absorbs its own reflection, and the loop stops there.
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
            .find(|binding| {
                binding
                    .roots
                    .iter()
                    .any(|candidate| workspace_root_matches_agent_root(candidate, &root))
            })
            .map(|binding| binding.window);
        if let Some(window) = window {
            let _ = window.update(cx, |_, window, _| window.activate_window());
            self.activate_workspace_for_root(&window, &root, cx);
        } else {
            log::debug!("no bound Zed window holds herdr workspace {workspace_id} yet");
        }
    }

    /// Route one effect through the workspace-follow consumer. Follow is
    /// stateless: a forget releases no Zed surface (the workspace remains the
    /// user's project), and open/move/focus all funnel into the same
    /// activate-or-add path.
    fn apply_workspace_effect(
        &mut self,
        _identity: &SessionIdentity,
        effect: WorkspaceEffect,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        match effect {
            WorkspaceEffect::Open(record) => self.follow_agent_workspace(record, target, cx),
            WorkspaceEffect::PaneMoved { key, .. } | WorkspaceEffect::Focus(key) => {
                if let Some(record) = self.sync.record(&key) {
                    self.follow_agent_workspace(record, target, cx);
                }
            }
            WorkspaceEffect::Forget(_) => {}
        }
    }


    /// Mid-session space creation/update: add the new checkout root to every
    /// window bound to this connection generation without activating it, so
    /// the space appears as a project immediately. A failed add is reported
    /// through the shared import notification; other windows keep trying.
    fn import_workspace_event_root(
        &mut self,
        identity: &SessionIdentity,
        workspace: &herdr::WorkspaceInfo,
        generation: u64,
        cx: &mut Context<Self>,
    ) {
        let Some(checkout) = workspace.checkout_path() else {
            return;
        };
        let Ok(root) = canonical_checkout_path(checkout) else {
            return;
        };
        self.refresh_window_roots(cx);
        let Some(connection) = self
            .connections
            .get(identity)
            .filter(|connection| connection.generation == generation)
        else {
            return;
        };
        let windows: Vec<WindowHandle<MultiWorkspace>> = connection
            .bound_windows
            .iter()
            .filter_map(|window_id| self.windows.get(window_id))
            .filter(|binding| {
                matches!(&binding.state, BindingState::Connected(bound) if bound == identity)
            })
            .filter(|binding| {
                !binding
                    .roots
                    .iter()
                    .any(|candidate| workspace_root_matches_agent_root(candidate, &root))
            })
            .map(|binding| binding.window)
            .collect();
        for window in windows {
            let root_lock = self.workspace_root_lock(window.window_id(), &root);
            let registry = cx.entity();
            let identity = identity.clone();
            let root = root.clone();
            cx.spawn(async move |_this, mut cx| {
                let _root_lock = root_lock.lock().await;
                // Double-check under the lock: a concurrent add (a follow or
                // a sibling window's import) may have won while this waiter
                // queued.
                let present = registry.read_with(cx, |registry, _| {
                    registry.connection_matches(&identity, generation)
                        && registry
                            .windows
                            .get(&window.window_id().as_u64())
                            .is_some_and(|binding| {
                                binding.roots.iter().any(|candidate| {
                                    workspace_root_matches_agent_root(candidate, &root)
                                })
                            })
                });
                if present {
                    return;
                }
                if let Err(error) = add_workspace_root(window, root.clone(), &mut cx).await {
                    report_workspace_import_failure(window, &root, &error, &mut cx);
                    return;
                }
                let _ = registry.update(cx, |registry, cx| {
                    if registry.connection_matches(&identity, generation) {
                        registry.refresh_window_roots(cx);
                    }
                });
            })
            .detach();
        }
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

    /// Follow one live herdr agent inside its bound window: ensure the
    /// agent's checkout root is present as an inner workspace (adding it with
    /// `OpenMode::Add` when absent) and activate it. Only the focused agent
    /// drives activation, so a snapshot import never steals the center from
    /// the user. Path problems are best-effort logs; only a failed workspace
    /// import surfaces, through the shared snapshot-import notification. The
    /// captured connection generation invalidates the whole attempt once its
    /// session connection is superseded or released.
    fn follow_agent_workspace(
        &mut self,
        record: AgentRecord,
        target: Option<MirrorTarget>,
        cx: &mut Context<Self>,
    ) {
        if !record.focused {
            return;
        }
        let Some(generation) = self
            .connections
            .get(&record.key.session)
            .map(|connection| connection.generation)
        else {
            log::debug!(
                "herdr agent {} lost its connection before workspace follow",
                record.key.terminal_id
            );
            return;
        };
        let target = target.filter(|target| target.generation == generation);
        let Some(candidate) = record
            .checkout_path
            .clone()
            .or_else(|| record.effective_cwd.clone())
        else {
            log::debug!(
                "herdr agent {} has no checkout path to follow",
                record.key.terminal_id
            );
            return;
        };
        if !candidate.is_absolute() {
            log::debug!(
                "herdr agent path is not absolute: {}",
                candidate.display()
            );
            return;
        }
        let fs = <dyn Fs>::global(cx).clone();
        let registry = cx.entity();
        cx.spawn(async move |_this, mut cx| {
            let owned = registry.read_with(cx, |registry, _| {
                registry.connection_matches(&record.key.session, generation)
            });
            if !owned {
                return;
            }
            let metadata = fs.metadata(&candidate).await;
            let valid_directory = metadata
                .as_ref()
                .is_ok_and(|metadata| metadata.as_ref().is_some_and(|m| m.is_dir));
            if !valid_directory {
                log::debug!(
                    "herdr agent worktree is unavailable: {}",
                    candidate.display()
                );
                return;
            }
            // Resolve the nearest checkout root rather than routing through
            // Git's common `.git` directory: linked worktrees keep their own
            // identity and nested agent cwds reuse their checkout workspace.
            let root = discover_agent_checkout_root(&candidate, fs.as_ref()).await;
            let Ok(root) = canonical_checkout_path(&root) else {
                log::debug!("herdr agent worktree could not be canonicalized");
                return;
            };
            let Some((window, root_lock)) = registry.update(cx, |registry, cx| {
                if !registry.connection_matches(&record.key.session, generation) {
                    return None;
                }
                registry.refresh_window_roots(cx);
                let window = target
                    .and_then(|target| registry.mirror_target_window(&record.key, target))
                    .or_else(|| {
                        registry.source_window_for_session(&record.key.session, generation)
                    })?;
                let needs_add = registry
                    .windows
                    .get(&window.window_id().as_u64())
                    .map(|binding| {
                        !binding.roots.iter().any(|candidate| {
                            workspace_root_matches_agent_root(candidate, &root)
                        })
                    })
                    .unwrap_or(true);
                let root_lock =
                    needs_add.then(|| registry.workspace_root_lock(window.window_id(), &root));
                Some((window, root_lock))
            }) else {
                return;
            };
            if let Some(root_lock) = root_lock {
                let _root_lock = root_lock.lock().await;
                if let Err(error) = add_workspace_root(window, root.clone(), &mut cx).await {
                    report_workspace_import_failure(window, &root, &error, &mut cx);
                    return;
                }
                let _ = registry.update(cx, |registry, cx| {
                    if registry.connection_matches(&record.key.session, generation) {
                        registry.refresh_window_roots(cx);
                    }
                });
            }
            let _ = registry.update(cx, |registry, cx| {
                if !registry.connection_matches(&record.key.session, generation) {
                    return;
                }
                registry.activate_workspace_for_root(&window, &root, cx);
            });
        })
        .detach();
    }

    /// Activate the inner workspace in `window` whose checkout matches `root`
    /// (exact root preferred, containing ancestor workspace as fallback) and
    /// pin it so a settings-disabled multi-workspace mode never detaches it
    /// on the next switch. Returns whether a match became active.
    fn activate_workspace_for_root(
        &self,
        window: &WindowHandle<MultiWorkspace>,
        root: &herdr::CanonicalPath,
        cx: &mut App,
    ) -> bool {
        window
            .update(cx, |multi_workspace, window, cx| {
                let mut candidate: Option<Entity<Workspace>> = None;
                for workspace in multi_workspace.workspaces() {
                    let mut exact = false;
                    let mut contains = false;
                    for worktree_root in workspace
                        .read(cx)
                        .project()
                        .read(cx)
                        .worktrees(cx)
                        .filter_map(|worktree| worktree.read(cx).root_dir())
                    {
                        let Ok(worktree_root) = canonical_checkout_path(worktree_root.as_ref())
                        else {
                            continue;
                        };
                        if &worktree_root == root {
                            exact = true;
                            break;
                        }
                        if workspace_root_matches_agent_root(&worktree_root, root) {
                            contains = true;
                        }
                    }
                    if exact {
                        candidate = Some(workspace.clone());
                        break;
                    }
                    if contains && candidate.is_none() {
                        candidate = Some(workspace.clone());
                    }
                }
                match candidate {
                    Some(workspace) => {
                        multi_workspace.activate(workspace, None, window, cx);
                        multi_workspace.retain_active_workspace(cx);
                        true
                    }
                    None => false,
                }
            })
            .unwrap_or(false)
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
            pending_new_window_selections: HashMap::default(),
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
            focus_echoes: HashMap::default(),
            #[cfg(test)]
            sink_effects: None,
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


/// True when `workspace_root` is the agent checkout itself or an ancestor
/// directory containing it: an open `/repo` workspace legitimately hosts an
/// agent running under `/repo/worktree`.
fn workspace_root_matches_agent_root(
    workspace_root: &herdr::CanonicalPath,
    agent_root: &herdr::CanonicalPath,
) -> bool {
    workspace_root == agent_root
        || Path::new(agent_root.as_str()).starts_with(Path::new(workspace_root.as_str()))
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
    log::error!("herdr connection failed for session {session_name}: {message}");
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
    if let HerdrEvent::Workspace(WorkspaceEvent::Created(workspace))
    | HerdrEvent::Workspace(WorkspaceEvent::Updated(workspace)) = &event {
        let owned = registry.read_with(cx, |registry, _| {
            registry.connection_matches(identity, generation)
        });
        if owned {
            registry.update(cx, |registry, cx| {
                if !registry.connection_matches(identity, generation) {
                    return;
                }
                registry.import_workspace_event_root(identity, workspace, generation, cx);
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
    use futures::channel::oneshot;
    use std::cell::{Cell, RefCell};

    use gpui::{TestAppContext, VisualTestContext};
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
    fn installed_herdr_program_precedes_path_program() {
        let installed_program = PathBuf::from("/installed/herdr");
        let path_program = PathBuf::from("/path/herdr");

        assert_eq!(
            select_herdr_program(Some(installed_program.clone()), Some(path_program.clone())),
            Some(installed_program),
        );
        assert_eq!(
            select_herdr_program(None, Some(path_program.clone())),
            Some(path_program),
        );
        assert_eq!(select_herdr_program(None, None), None);
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

        fn focus_workspace(
            &self,
            _workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            async { Ok(()) }.boxed_local()
        }
    }

    struct FocusResultHandle {
        succeeds: bool,
        calls: Rc<Cell<usize>>,
    }

    impl HerdrSessionHandle for FocusResultHandle {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            async { Err::<Box<dyn HerdrEventStream>, _>(anyhow::anyhow!("unused focus stream")) }
                .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            async { Err::<SessionSnapshot, _>(anyhow::anyhow!("unused focus snapshot")) }
                .boxed_local()
        }

        fn pane(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
            async { Err::<herdr::PaneInfo, _>(anyhow::anyhow!("unused focus pane")) }.boxed_local()
        }

        fn focus_workspace(
            &self,
            _workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            self.calls.set(self.calls.get().saturating_add(1));
            let succeeds = self.succeeds;
            async move {
                if succeeds {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("focus failed"))
                }
            }
            .boxed_local()
        }
    }


    struct DelayedFailureHandle {
        sender: Rc<RefCell<Option<oneshot::Sender<anyhow::Result<()>>>>>,
    }

    impl HerdrSessionHandle for DelayedFailureHandle {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            async {
                Err::<Box<dyn HerdrEventStream>, _>(anyhow::anyhow!(
                    "unused delayed focus stream"
                ))
            }
            .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            async { Err::<SessionSnapshot, _>(anyhow::anyhow!("unused delayed focus snapshot")) }
                .boxed_local()
        }

        fn pane(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
            async { Err::<herdr::PaneInfo, _>(anyhow::anyhow!("unused delayed focus pane")) }
                .boxed_local()
        }

        fn focus_workspace(
            &self,
            _workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            let (sender, receiver) = oneshot::channel();
            *self.sender.borrow_mut() = Some(sender);
            async move {
                receiver
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("focus request cancelled")))
            }
            .boxed_local()
        }
    }

    struct RecordingFocusHandle {
        workspace_ids: Rc<RefCell<Vec<String>>>,
    }

    impl HerdrSessionHandle for RecordingFocusHandle {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            async {
                Err::<Box<dyn HerdrEventStream>, _>(anyhow::anyhow!(
                    "unused recording focus stream"
                ))
            }
            .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            async { Err::<SessionSnapshot, _>(anyhow::anyhow!("unused recording focus snapshot")) }
                .boxed_local()
        }

        fn pane(
            &self,
            _pane_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<herdr::PaneInfo>> {
            async { Err::<herdr::PaneInfo, _>(anyhow::anyhow!("unused recording focus pane")) }
                .boxed_local()
        }

        fn focus_workspace(
            &self,
            workspace_id: String,
        ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
            self.workspace_ids.borrow_mut().push(workspace_id);
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
        let (target_id, _owner_id) = registry.update(cx, |registry, _| {
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

    async fn focus_registry_fixture(
        cx: &mut TestAppContext,
        handle: Option<Rc<dyn HerdrSessionHandle>>,
    ) -> (
        Entity<HerdrSessionRegistry>,
        VisualTestContext,
        WindowId,
        SessionIdentity,
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
        let roots = roots_in_window(window, cx);
        let identity = session("main");
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
        let window_id = registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                window,
                BindingState::Connected(identity.clone()),
                roots,
            )
        });
        registry.update(cx, |registry, _| {
            registry
                .windows
                .get_mut(&window_id.as_u64())
                .expect("focus fixture window is registered")
                .checkout_path = Some(checkout("C:/existing"));
            let _ = registry
                .sync
                .apply_workspace(&identity, &workspace_with_checkout("C:/existing"));
            if let Some(handle) = handle {
                registry.install_connection_for_test(identity.clone(), handle, 1);
            }
        });
        let visual = VisualTestContext::from_window(window.into(), cx);
        (registry, visual, window_id, identity)
    }

    fn process_workspace_focus_event(
        cx: &mut VisualTestContext,
        registry: &Entity<HerdrSessionRegistry>,
        identity: &SessionIdentity,
        generation: u64,
        workspace_id: &str,
    ) {
        let identity = identity.clone();
        let event = HerdrEvent::Workspace(WorkspaceEvent::Focused {
            workspace_id: workspace_id.to_owned(),
        });
        let task = registry.update(cx, |_, cx| {
            let registry = cx.entity();
            cx.spawn(async move |_this, mut cx| {
                process_stream_event(&registry, &identity, generation, event, &mut cx).await;
            })
        });
        task.detach();
        cx.cx.run_until_parked();
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
        let window = add_real_window(cx, &project).await;
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
        registry.update(cx, |registry, cx| {
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
    }

    #[gpui::test]
    async fn connected_selection_opens_new_window_and_preserves_invoking_window(
        cx: &mut TestAppContext,
    ) {
        init_app(cx);
        let project = test_project(cx).await;
        let invoking = add_real_window(cx, &project).await;
        let original_roots = roots_in_window(invoking, cx);
        let old_identity = session("old");
        let gateway = HerdrGateway::fake(
            || async { Ok(vec![session_info("new", true)]) }.boxed_local(),
            |_info| {
                async {
                    Ok(Rc::new(FakeConnection {
                        snapshot: empty_snapshot(),
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
        let registry = cx.update(|cx| cx.new(HerdrSessionRegistry::new));
        registry.update(cx, |registry, _| registry.gateway = Some(gateway));
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

        // Re-selecting the currently bound session is a no-op.
        registry.update(cx, |registry, cx| {
            registry.confirm_selection_for_test(
                invoking,
                SessionSelection::Existing(session_info("old", true)),
                cx,
            );
            assert_eq!(
                registry.binding_state(window_id),
                BindingState::Connected(old_identity.clone())
            );
        });
        assert_eq!(cx.windows().len(), 1);

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
                BindingState::Connected(old_identity.clone())
            );
            assert_eq!(registry.bound_window_count(&old_identity), 1);
            assert!(registry.has_connection(&old_identity));
        });
        cx.run_until_parked();

        assert_eq!(cx.windows().len(), 2);
        assert_eq!(roots_in_window(invoking, cx), original_roots);
        let new_identity = session("new");
        let states = registry.read_with(cx, |registry, _| {
            registry
                .windows
                .iter()
                .map(|(window_id, binding)| (*window_id, binding.state.clone()))
                .collect::<Vec<_>>()
        });
        assert!(states.iter().any(|(_, state)| {
            *state == BindingState::Connected(old_identity.clone())
        }));
        let new_window_id = states
            .iter()
            .find_map(|(window_id, state)| {
                (*state == BindingState::Connected(new_identity.clone())).then_some(*window_id)
            })
            .expect("the selected session must bind the new window");
        assert!(
            !registry.read_with(cx, |registry, _| registry
                .prompt_pending
                .contains(&WindowId::from(new_window_id))),
            "the selected new window must not open the startup picker"
        );
        assert_eq!(registry.read_with(cx, |registry, _| {
            registry.bound_window_count(&old_identity)
        }), 1);
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

    /// A bootstrapped agent whose checkout path cannot be resolved is a
    /// best-effort follow miss: it must not toast and must not fail the
    /// session binding.
    #[gpui::test]
    async fn owner_bootstrap_agent_without_path_stays_quiet(cx: &mut TestAppContext) {
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
            0,
            "an unresolvable agent path is a best-effort follow miss, not a toast"
        );
        assert!(matches!(
            registry.read_with(cx, |registry, _| registry.binding_state(window_id)),
            BindingState::Connected(_)
        ));
        assert_eq!(cx.windows().len(), 1);
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

    #[gpui::test]
    async fn successful_local_focus_waits_for_reflected_echo_before_activation(
        cx: &mut TestAppContext,
    ) {
        let calls = Rc::new(Cell::new(0));
        let (registry, mut visual, window_id, identity) = focus_registry_fixture(
            cx,
            Some(Rc::new(FocusResultHandle {
                succeeds: true,
                calls: calls.clone(),
            }) as Rc<dyn HerdrSessionHandle>),
        )
        .await;

        visual.deactivate_window();
        registry.update(&mut visual, |registry, cx| {
            registry.focus_herdr_workspace(window_id, cx);
        });
        visual.cx.run_until_parked();
        assert_eq!(calls.get(), 1, "local focus must issue one RPC");
        visual.update(|window, _| assert!(!window.is_window_active()));

        process_workspace_focus_event(&mut visual, &registry, &identity, 1, "workspace");
        visual.update(|window, _| {
            assert!(
                !window.is_window_active(),
                "a reflected local focus must be consumed as an echo"
            );
        });

        visual.deactivate_window();
        process_workspace_focus_event(&mut visual, &registry, &identity, 1, "workspace");
        visual.update(|window, _| {
            assert!(
                window.is_window_active(),
                "a later genuinely external focus must activate the window"
            );
        });
    }

    #[gpui::test]
    async fn failed_local_focus_clears_echo_for_later_external_focus(
        cx: &mut TestAppContext,
    ) {
        let calls = Rc::new(Cell::new(0));
        let (registry, mut visual, window_id, identity) = focus_registry_fixture(
            cx,
            Some(Rc::new(FocusResultHandle {
                succeeds: false,
                calls: calls.clone(),
            }) as Rc<dyn HerdrSessionHandle>),
        )
        .await;
        visual.deactivate_window();
        registry.update(&mut visual, |registry, cx| {
            registry.focus_herdr_workspace(window_id, cx);
        });
        visual.cx.run_until_parked();
        assert_eq!(calls.get(), 1, "failed local focus must issue one RPC");

        process_workspace_focus_event(&mut visual, &registry, &identity, 1, "workspace");
        visual.update(|window, _| {
            assert!(
                window.is_window_active(),
                "an RPC failure must clear its echo so the reflected focus is external"
            );
        });
    }

    #[gpui::test]
    async fn local_focus_without_connection_does_not_create_echo(
        cx: &mut TestAppContext,
    ) {
        let calls = Rc::new(Cell::new(0));
        let (registry, mut visual, window_id, identity) = focus_registry_fixture(cx, None).await;

        visual.deactivate_window();
        registry.update(&mut visual, |registry, cx| {
            registry.focus_herdr_workspace(window_id, cx);
            registry.install_connection_for_test(
                identity.clone(),
                Rc::new(FocusResultHandle {
                    succeeds: true,
                    calls: calls.clone(),
                }),
                1,
            );
        });
        visual.cx.run_until_parked();

        process_workspace_focus_event(&mut visual, &registry, &identity, 1, "workspace");
        visual.update(|window, _| {
            assert!(
                window.is_window_active(),
                "a missing client must not leave an orphan echo"
            );
        });
        assert_eq!(
            calls.get(),
            0,
            "neither the unconnected local focus nor the external activation may issue an RPC"
        );
    }

    #[gpui::test]
    async fn stale_focus_failure_cannot_clear_new_generation_echo(
        cx: &mut TestAppContext,
    ) {
        let old_failure_sender = Rc::new(RefCell::new(None));
        let new_focus_calls = Rc::new(Cell::new(0));
        let (registry, mut visual, window_id, identity) = focus_registry_fixture(
            cx,
            Some(Rc::new(DelayedFailureHandle {
                sender: old_failure_sender.clone(),
            }) as Rc<dyn HerdrSessionHandle>),
        )
        .await;

        visual.deactivate_window();
        registry.update(&mut visual, |registry, cx| {
            registry.focus_herdr_workspace(window_id, cx);
        });
        visual.cx.run_until_parked();
        assert!(
            old_failure_sender.borrow().is_some(),
            "the old generation focus RPC must be in flight"
        );

        registry.update(&mut visual, |registry, cx| {
            registry.detach_window(&identity, window_id, cx);
            registry.install_connection_for_test(
                identity.clone(),
                Rc::new(FocusResultHandle {
                    succeeds: true,
                    calls: new_focus_calls.clone(),
                }),
                2,
            );
            registry.focus_herdr_workspace(window_id, cx);
        });
        visual.cx.run_until_parked();
        assert_eq!(new_focus_calls.get(), 1, "new generation focus must issue one RPC");

        old_failure_sender
            .borrow_mut()
            .take()
            .expect("old focus RPC sender must remain available")
            .send(Err(anyhow::anyhow!("old generation failed")))
            .expect("old focus RPC receiver is still pending");
        visual.cx.run_until_parked();

        visual.deactivate_window();
        process_workspace_focus_event(&mut visual, &registry, &identity, 2, "workspace");
        visual.update(|window, _| {
            assert!(
                !window.is_window_active(),
                "a stale failure must not clear a newer generation's echo"
            );
        });
    }
    #[gpui::test]
    fn rapid_local_focus_reissues_superseded_workspace_rpc(cx: &mut TestAppContext) {
        let identity = session("main");
        let workspace_ids = Rc::new(RefCell::new(Vec::new()));
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
        let window_id = registry.update(cx, |registry, _| {
            let window_id = registry.register_window_for_test(
                WindowHandle::new(WindowId::from(77)),
                BindingState::Connected(identity.clone()),
                Vec::new(),
            );
            registry
                .windows
                .get_mut(&window_id.as_u64())
                .expect("rapid-focus fixture window is registered")
                .checkout_path = Some(checkout("C:/existing"));
            let _ = registry
                .sync
                .apply_workspace(&identity, &workspace_with_checkout("C:/existing"));
            let mut second_workspace = workspace_with_checkout("C:/space-b");
            second_workspace.workspace_id = "workspace-b".to_owned();
            let _ = registry
                .sync
                .apply_workspace(&identity, &second_workspace);
            registry.install_connection_for_test(
                identity.clone(),
                Rc::new(RecordingFocusHandle {
                    workspace_ids: workspace_ids.clone(),
                }),
                1,
            );
            window_id
        });

        // One park at the end: the pending A token can only be superseded by
        // a third RPC if a repeated local request never consumes it.
        registry.update(cx, |registry, cx| {
            registry.focus_herdr_workspace(window_id, cx);
            registry
                .windows
                .get_mut(&window_id.as_u64())
                .expect("rapid-focus fixture window is registered")
                .checkout_path = Some(checkout("C:/space-b"));
            registry.focus_herdr_workspace(window_id, cx);
            registry
                .windows
                .get_mut(&window_id.as_u64())
                .expect("rapid-focus fixture window is registered")
                .checkout_path = Some(checkout("C:/existing"));
            registry.focus_herdr_workspace(window_id, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            *workspace_ids.borrow(),
            vec![
                "workspace".to_owned(),
                "workspace-b".to_owned(),
                "workspace".to_owned(),
            ],
            "a repeated local target must issue a new superseding RPC"
        );
    }

}
