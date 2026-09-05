//! Application-global herdr session registry.
//!
//! Owns at most one client connection and event stream per herdr session,
//! shared by every Zed window bound to that session. Windows bind only after
//! an explicit picker confirmation or causal inheritance from a previously
//! confirmed selection; nothing here connects to a session on installation.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc;
use futures::future::LocalBoxFuture;
use futures::{FutureExt as _, StreamExt as _};
use gpui::{
    App, AppContext as _, AsyncApp, Context, Entity, EntityId, Global, SharedString, Subscription,
    Task, Window, WindowHandle, WindowId,
};
use herdr::{
    ClientConfig, HerdRClient, HerdrEvent, PaneEvent, PaneEventKind, SessionInfo, SessionSnapshot,
    WorkspaceEvent, canonical_checkout_path,
};
use workspace::{AppState, MultiWorkspace, OpenMode, Workspace};

use super::herdr_agent_sync::{
    AgentKey, AgentRecord, AgentSyncEffect, AgentSyncState, SessionIdentity,
};

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
    fn detach(
        &self,
        window: WindowHandle<MultiWorkspace>,
        cx: &mut Context<HerdrSessionRegistry>,
    );
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

/// Adapter seam for the modal session picker. The picker module supplies the
/// implementation in the explicit-session cutover; cancellation does
/// nothing, so the registry never receives a binding.
pub(crate) trait SessionPickerSink {
    fn show(
        &self,
        window: WindowHandle<MultiWorkspace>,
        registry: Entity<HerdrSessionRegistry>,
        cx: &mut Context<HerdrSessionRegistry>,
    );
}

#[derive(Default)]
struct NoopSessionPickerSink;

impl SessionPickerSink for NoopSessionPickerSink {
    fn show(
        &self,
        _window: WindowHandle<MultiWorkspace>,
        _registry: Entity<HerdrSessionRegistry>,
        _cx: &mut Context<HerdrSessionRegistry>,
    ) {
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BindingState {
    Unselected,
    Starting { session_name: Arc<str> },
    Connected(SessionIdentity),
    Failed {
        session_name: Arc<str>,
        message: SharedString,
    },
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

impl SessionSelection {
    pub(crate) fn name(&self) -> &str {
        match self {
            SessionSelection::Existing(info) => &info.name,
            SessionSelection::New { name } => name,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectionTarget {
    InvokingWindow,
    NewWindow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PendingBinding {
    ConfirmedSelection(SessionSelection),
    Inherited(SessionIdentity),
}

struct WindowBinding {
    window: WindowHandle<MultiWorkspace>,
    state: BindingState,
    workspace_id: Option<Arc<str>>,
    checkout_path: Option<herdr::CanonicalPath>,
    /// Canonical roots of every worktree this window currently holds.
    roots: Vec<herdr::CanonicalPath>,
    /// Target policy captured when the selection started; survives into
    /// `Failed` so `Retry` reruns the same route.
    selection_target: Option<SelectionTarget>,
}

/// One shared connection per session identity. Dropping this entry releases
/// the client and cancels the event task; the session socket belongs to the
/// independent herdr server, so closing every local client never terminates
/// it.
struct SessionConnection {
    info: SessionInfo,
    client: Rc<dyn HerdrSessionHandle>,
    bound_windows: HashSet<u64>,
    event_task: Task<()>,
}

/// Pure binding-state machine. `SessionNotRunning` is a normal exit: it
/// returns to `Unselected` and starts no reconnect task.
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

/// A selection made from a window that already has a binding always lands in
/// a freshly created window; an unselected window binds itself.
fn selection_target(state: &BindingState) -> SelectionTarget {
    match state {
        BindingState::Unselected => SelectionTarget::InvokingWindow,
        BindingState::Starting { .. }
        | BindingState::Connected(_)
        | BindingState::Failed { .. } => SelectionTarget::NewWindow,
    }
}

/// One candidate window for focused-worktree routing.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RoutingCandidate {
    window_id: WindowId,
    state: BindingState,
    checkout_paths: Vec<herdr::CanonicalPath>,
}

/// Where the confirmed binding actually lands once the snapshot's focused
/// worktree is known.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Route {
    /// Bind the bootstrap window as-is (exact root present, or no focused
    /// worktree to guess).
    BindBootstrap,
    /// Move the binding to an existing eligible window with the exact root.
    ReuseExact(WindowId),
    /// Open a new window for the focused worktree with an inherited binding.
    OpenInherited,
}

/// Pure focused-worktree routing policy:
/// - a `NewWindow` selection keeps its already-created target window even
///   when another window already has the same root;
/// - an `InvokingWindow` selection reuses an exact eligible window (same
///   root, `Unselected` or already bound to the same session);
/// - a mismatched invoking window remains `Unselected` and the session moves
///   to a new focused-worktree window with an inherited binding;
/// - with no focused worktree, bind the bootstrap window without guessing.
fn route_focused_worktree(
    target: SelectionTarget,
    bootstrap_id: WindowId,
    bootstrap_paths: &[herdr::CanonicalPath],
    focused: Option<&herdr::CanonicalPath>,
    identity: Option<&SessionIdentity>,
    candidates: &[RoutingCandidate],
) -> Route {
    let Some(focused) = focused else {
        return Route::BindBootstrap;
    };
    match target {
        SelectionTarget::NewWindow => Route::BindBootstrap,
        SelectionTarget::InvokingWindow => {
            if bootstrap_paths.iter().any(|path| path == focused) {
                return Route::BindBootstrap;
            }
            let exact = candidates
                .iter()
                .filter(|candidate| candidate.window_id != bootstrap_id)
                .filter(|candidate| {
                    candidate
                        .checkout_paths
                        .iter()
                        .any(|path| path == focused)
                })
                .find(|candidate| match &candidate.state {
                    BindingState::Unselected => true,
                    BindingState::Connected(bound) => {
                        identity.is_some_and(|identity| bound == identity)
                    }
                    _ => false,
                });
            match exact {
                Some(candidate) => Route::ReuseExact(candidate.window_id),
                None => Route::OpenInherited,
            }
        }
    }
}

/// The Agent Panel work the reducer asked for. `Forget` deliberately has no
/// variant: it only drops synchronization ownership, which the reducer has
/// already done when it emitted the effect, and never closes a terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WorkspaceEffect {
    Open(AgentRecord),
    PaneMoved { key: AgentKey, pane_id: Arc<str> },
    Focus(AgentKey),
}

/// Pure effect classification for the Agent Panel seam.
fn plan_effects(effects: Vec<AgentSyncEffect>) -> Vec<WorkspaceEffect> {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            AgentSyncEffect::Open(record) => Some(WorkspaceEffect::Open(record)),
            AgentSyncEffect::PaneMoved { key, pane_id } => {
                Some(WorkspaceEffect::PaneMoved { key, pane_id })
            }
            AgentSyncEffect::Focus(key) => Some(WorkspaceEffect::Focus(key)),
            AgentSyncEffect::Forget(_) => None,
        })
        .collect()
}

/// The designated Agent Panel effect channel. Task 5 drains this queue into
/// `AgentPanel::open_external_terminal_thread`, the mirror rename/move calls,
/// and the focus echo. Until then the queue is the only consumer, and
/// `Forget` never reaches it: dropping synchronization ownership must not
/// close mirrored terminals.
pub(crate) trait AgentEffectSink {
    fn push(&self, identity: &SessionIdentity, effect: WorkspaceEffect);
}

/// Production sink until Task 5 replaces its consumption with live Agent
/// Panel wiring. TODO(Task 5): drain this queue in the Agent Panel effect
/// application path (`herdr_session_registry` synchronization step).
#[derive(Default)]
pub(crate) struct QueuedAgentEffectSink {
    queue: Rc<std::cell::RefCell<VecDeque<(SessionIdentity, WorkspaceEffect)>>>,
}

impl QueuedAgentEffectSink {
    pub(crate) fn drain(&self) -> Vec<(SessionIdentity, WorkspaceEffect)> {
        self.queue
            .borrow_mut()
            .drain(..)
            .collect::<Vec<_>>()
    }
}

impl AgentEffectSink for QueuedAgentEffectSink {
    fn push(&self, identity: &SessionIdentity, effect: WorkspaceEffect) {
        self.queue
            .borrow_mut()
            .push_back((identity.clone(), effect));
    }
}

/// Pure initial-snapshot import: workspaces first (their checkout paths feed
/// later records), then every running agent through the reducer's upsert so
/// repeated imports stay deduplicated by revision.
fn import_snapshot(
    sync: &mut AgentSyncState,
    identity: &SessionIdentity,
    snapshot: &SessionSnapshot,
) -> Vec<AgentSyncEffect> {
    let mut effects = Vec::new();
    for workspace in &snapshot.workspaces {
        effects.extend(sync.apply_workspace(identity, workspace));
    }
    for pane in &snapshot.agents {
        effects.extend(sync.upsert(identity.clone(), pane.clone()));
    }
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
}

pub(crate) trait HerdrEventStream {
    fn next(&mut self) -> LocalBoxFuture<'_, anyhow::Result<Option<HerdrEvent>>>;
}

#[derive(Clone)]
pub(crate) struct HerdrGateway {
    list_sessions: Rc<dyn Fn() -> LocalBoxFuture<'static, anyhow::Result<Vec<SessionInfo>>>>,
    connect: Rc<
        dyn Fn(
            &SessionInfo,
        ) -> LocalBoxFuture<'static, anyhow::Result<Rc<dyn HerdrSessionHandle>>>,
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
                async move { herdr::list_sessions(program).await.map_err(Into::into) }
                    .boxed_local()
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
    fn fake(
        list_sessions: impl Fn() -> LocalBoxFuture<'static, anyhow::Result<Vec<SessionInfo>>>
        + 'static,
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
    pending: HashMap<EntityId, PendingBinding>,
    connections: HashMap<SessionIdentity, SessionConnection>,
    sync: AgentSyncState,
    effects: Rc<dyn AgentEffectSink>,
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
        let observer = cx.observe_new(
            move |multi_workspace: &mut MultiWorkspace, window, cx| {
                let Some(window) = window else {
                    return;
                };
                let Some(handle) = window.window_handle().downcast::<MultiWorkspace>() else {
                    return;
                };
                let workspace_entity_id = multi_workspace.workspace().entity_id();
                registry.update(cx, |registry, cx| {
                    registry.handle_new_window(handle, workspace_entity_id, cx)
                });
            },
        );
        Self {
            windows: HashMap::default(),
            prompted: HashSet::default(),
            pending: HashMap::default(),
            connections: HashMap::default(),
            sync: AgentSyncState::default(),
            effects: Rc::new(QueuedAgentEffectSink::default()),
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

    /// `Retry`/`Choose Session` availability for a window's binding.
    pub(crate) fn can_retry(&self, window_id: WindowId) -> bool {
        matches!(self.binding_state(window_id), BindingState::Failed { .. })
    }

    /// Reserve the binding a window is created with, keyed by the new
    /// workspace entity id so the `MultiWorkspace` observer consumes it
    /// before it can schedule a startup picker.
    pub(crate) fn reserve_pending(
        &mut self,
        workspace_entity_id: EntityId,
        pending: PendingBinding,
    ) {
        self.pending.insert(workspace_entity_id, pending);
    }

    /// Consume a reserved binding; returns its session exactly once.
    pub(crate) fn consume_pending(
        &mut self,
        workspace_entity_id: EntityId,
    ) -> Option<PendingBinding> {
        self.pending.remove(&workspace_entity_id)
    }

    fn handle_new_window(
        &mut self,
        handle: WindowHandle<MultiWorkspace>,
        workspace_entity_id: EntityId,
        cx: &mut Context<Self>,
    ) {
        let window_id = self.register_window(handle);
        match self.consume_pending(workspace_entity_id) {
            Some(PendingBinding::ConfirmedSelection(selection)) => {
                // The bootstrap target exists *because of* a confirmed
                // selection: it keeps its own window (NewWindow route).
                self.start_binding_for_window(
                    window_id,
                    selection.name().into(),
                    SelectionTarget::NewWindow,
                    cx,
                );
            }
            Some(PendingBinding::Inherited(identity)) => {
                // Causal inheritance: bind silently, no picker.
                self.prompt_completed(window_id);
                self.start_binding_for_window(
                    window_id,
                    identity.name.clone(),
                    SelectionTarget::NewWindow,
                    cx,
                );
            }
            None => {
                self.schedule_startup_prompt(handle, cx);
            }
        }
    }

    fn register_window(&mut self, handle: WindowHandle<MultiWorkspace>) -> WindowId {
        let window_id = handle.window_id();
        self.windows
            .entry(window_id.as_u64())
            .or_insert_with(|| WindowBinding {
                window: handle,
                state: BindingState::Unselected,
                workspace_id: None,
                checkout_path: None,
                roots: Vec::new(),
                selection_target: None,
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
        let registry = cx.entity();
        cx.spawn(async move |_this, cx: &mut AsyncApp| {
            cx.background_executor().timer(PROMPT_SETTLE_DELAY).await;
            let state = registry.read_with(cx, |registry, _| {
                registry
                    .windows
                    .get(&handle.window_id().as_u64())
                    .map(|binding| binding.state.clone())
            });
            match state {
                // Still unselected (or the window is gone): prompt once.
                Some(BindingState::Unselected) | None => {}
                // A binding landed while the window was settling: nothing
                // to prompt for.
                Some(_) => return,
            }
            let _ = registry.update(cx, |registry, cx| {
                let picker_sink = registry.picker_sink.clone();
                picker_sink.show(handle, cx.entity(), cx);
            });
        })
        .detach();

    }
    fn prompt_completed(&mut self, window_id: WindowId) {
        self.prompted.insert(window_id);
    }

    fn apply_binding_event(
        &mut self,
        window_id: WindowId,
        event: BindingEvent,
        cx: &mut Context<Self>,
    ) {
        let Some(binding) = self.windows.get_mut(&window_id.as_u64()) else {
            return;
        };
        binding.state = transition(binding.state.clone(), event);
        if matches!(binding.state, BindingState::Connected(_)) {
            binding.selection_target = None;
        }
        cx.notify();
    }

    /// Show the selected central host in a window. The host reads binding
    /// state from the registry and owns no connection itself.
    fn install_host(
        &self,
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
        registry.update(cx, |registry, cx| {
            // Any explicit selection attempt is this window's once-prompted
            // interaction; the startup observer will not ask again.
            registry.prompt_completed(window_id);
            let picker_sink = registry.picker_sink.clone();
            picker_sink.show(handle, cx.entity(), cx);
        });
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
        registry.update(cx, |registry, cx| registry.disconnect_session(window_id, cx));
    }

    /// `Retry` from a failed host surface: re-run the bounded connect loop.
    pub(crate) fn retry_for_window(window_id: WindowId, cx: &mut App) {
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        registry.update(cx, |registry, cx| {
            if let Err(error) = registry.retry(window_id, cx) {
                log::error!("herdr retry failed: {error:#}");
            }
        });
    }

    /// The `herdr --session` central client terminal of a window exited
    /// while the connection attempt was still starting.
    pub(crate) fn note_host_exit_for_window(window_id: WindowId, cx: &mut App) {
        let Some(registry) = Self::try_global(cx) else {
            return;
        };
        registry.update(cx, |registry, cx| {
            if !matches!(registry.binding_state(window_id), BindingState::Starting { .. }) {
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
        let target = selection_target(&self.binding_state(window_id));
        self.prompt_completed(window_id);
        match target {
            SelectionTarget::InvokingWindow => {
                let name: Arc<str> = Arc::from(selection.name());
                self.start_binding_for_window(window_id, name, target, cx);
            }
            SelectionTarget::NewWindow => {
                self.create_bootstrap_window(selection, cx);
            }
        }
    }

    fn start_binding_for_window(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
        target: SelectionTarget,
        cx: &mut Context<Self>,
    ) {
        if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
            binding.selection_target = Some(target);
        }
        let name = session_name.clone();
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
                registry.begin_connection(window_id, name, gateway, cx);
            });
        });
    }

    /// `SelectionTarget::NewWindow` creates one blank target with
    /// `Workspace::new_local`. Its init closure obtains the workspace entity
    /// id and reserves the pending binding before the `MultiWorkspace`
    /// observer can schedule a startup picker. The picker stays in the
    /// invoking window throughout.
    fn create_bootstrap_window(
        &mut self,
        selection: SessionSelection,
        cx: &mut Context<Self>,
    ) {
        let Some(app_state) = AppState::try_global(cx) else {
            return;
        };
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            let pending = PendingBinding::ConfirmedSelection(selection);
            let init = move |_workspace: &mut Workspace,
                             _window: &mut Window,
                             cx: &mut Context<Workspace>| {
                let entity_id = cx.entity().entity_id();
                let registry = registry.clone();
                registry.update(cx, |registry, _| {
                    registry.reserve_pending(entity_id, pending);
                });
            };
            let open_task = cx.update(|cx| {
                Workspace::new_local(
                    Vec::new(),
                    app_state,
                    None,
                    None,
                    Some(Box::new(init)),
                    OpenMode::NewWindow,
                    cx,
                )
            });
            if open_task.await.is_err() {
                log::error!("failed to open the herdr target window");
            }
        })
        .detach();
    }

    // ---------------------------------------------------------- connection

    /// Start (or restart) the bounded connection loop for one window. This
    /// is the only place a connection task is spawned; stream loss never
    /// calls it again.
    fn begin_connection(
        &mut self,
        window_id: WindowId,
        session_name: Arc<str>,
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
        let registry = cx.entity();
        cx.spawn(async move |_this, cx| {
            run_connection(
                registry,
                gateway,
                handle,
                window_id,
                session_name,
                cx.clone(),
            )
            .await;
        })
        .detach();
    }

    /// `Retry` re-runs the bounded connect loop for the failed session with
    /// the same target policy the selection started with.
    pub(crate) fn retry(
        &mut self,
        window_id: WindowId,
        cx: &mut Context<Self>,
    ) -> anyhow::Result<()> {
        let BindingState::Failed { session_name, .. } = self.binding_state(window_id) else {
            return Ok(());
        };
        let target = self
            .windows
            .get(&window_id.as_u64())
            .and_then(|binding| binding.selection_target)
            .unwrap_or(SelectionTarget::NewWindow);
        let _ = self.gateway()?;
        self.start_binding_for_window(window_id, session_name, target, cx);
        Ok(())
    }

    /// `DisconnectSession` removes only the invoking window binding, closes
    /// that binding's local client terminals, and detaches host
    /// presentation to the unselected surface. While another window stays
    /// bound, the shared client and event stream remain open; the last
    /// disconnect drops them. No agent-stop RPC is sent and the session
    /// server is untouched.
    pub(crate) fn disconnect_session(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        if !self.windows.contains_key(&window_id.as_u64()) {
            return;
        }
        let identities: Vec<SessionIdentity> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.bound_windows.contains(&window_id.as_u64()))
            .map(|(identity, _)| identity.clone())
            .collect();
        if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
            binding.state = BindingState::Unselected;
            binding.checkout_path = None;
            binding.selection_target = None;
        }
        let handle = self
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window);
        if let Some(handle) = handle {
            self.host_sink.detach(handle, cx);
        }
        for identity in identities {
            self.detach_window(&identity, window_id);
        }
        cx.notify();
    }

    /// Remove one window's binding from a session; when it was the last
    /// bound window, dropping the connection releases the client and
    /// cancels the event task.
    fn detach_window(&mut self, identity: &SessionIdentity, window_id: WindowId) {
        let released = self
            .connections
            .get_mut(identity)
            .is_some_and(|connection| {
                connection.bound_windows.remove(&window_id.as_u64());
                connection.bound_windows.is_empty()
            });
        if released {
            self.connections.remove(identity);
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
        self.dispatch_effects(&identity, effects);
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
                registry.dispatch_effects(&identity, effects);
                cx.notify();
            });
        })
        .detach();
    }

    fn dispatch_effects(&mut self, identity: &SessionIdentity, effects: Vec<AgentSyncEffect>) {
        for effect in plan_effects(effects) {
            self.effects.push(identity, effect);
        }
    }

    // --------------------------------------------------------- routing core

    /// Resolve the snapshot's focused worktree into the final binding and
    /// install the host there before detaching the local bootstrap client.
    /// Returns the window the binding landed on.
    fn finalize_binding(
        &mut self,
        bootstrap: WindowId,
        target: SelectionTarget,
        identity: &SessionIdentity,
        snapshot: &SessionSnapshot,
        cx: &mut Context<Self>,
    ) -> Option<WindowId> {
        self.refresh_window_roots(cx);
        let focused = focused_checkout_path(snapshot);
        let bootstrap_paths = self
            .windows
            .get(&bootstrap.as_u64())
            .map(|binding| binding.roots.clone())
            .unwrap_or_default();
        let route = route_focused_worktree(
            target,
            bootstrap,
            &bootstrap_paths,
            focused.as_ref(),
            Some(identity),
            &self.candidates(),
        );
        match route {
            Route::BindBootstrap => {
                if let Some(binding) = self.windows.get_mut(&bootstrap.as_u64()) {
                    binding.checkout_path = focused.clone();
                }
                self.bind_connected(bootstrap, identity, cx);
                if target == SelectionTarget::NewWindow
                    && let Some(path) = focused
                {
                    let Some(handle) = self
                        .windows
                        .get(&bootstrap.as_u64())
                        .map(|binding| binding.window)
                    else {
                        return Some(bootstrap);
                    };
                    let Some(app_state) = AppState::try_global(cx) else {
                        return Some(bootstrap);
                    };
                    // Add/activate the focused worktree inside that same
                    // target window; never reuse another window.
                    cx.spawn(async move |_this, cx| {
                        let open = cx.update(|cx| {
                            Workspace::new_local(
                                vec![PathBuf::from(path.as_str())],
                                app_state,
                                Some(handle),
                                None,
                                None,
                                OpenMode::Activate,
                                cx,
                            )
                        });
                        let _ = open.await;
                    })
                    .detach();
                }
                Some(bootstrap)
            }
            Route::ReuseExact(other) => {
                // Install the selected host in the final window before
                // detaching the local bootstrap client; detaching must not
                // terminate the herdr session.
                self.bind_connected(other, identity, cx);
                if other != bootstrap {
                    self.detach_bootstrap(bootstrap, cx);
                }
                Some(other)
            }
            Route::OpenInherited => {
                // Leave the mismatched invoking window `Unselected`; a new
                // focused-worktree window inherits the session.
                let path = focused?;
                self.detach_bootstrap(bootstrap, cx);
                let Some(app_state) = AppState::try_global(cx) else {
                    return None;
                };
                let registry = cx.entity();
                let identity = identity.clone();
                cx.spawn(async move |_this, cx| {
                    let init = move |_workspace: &mut Workspace,
                                     _window: &mut Window,
                                     cx: &mut Context<Workspace>| {
                        let entity_id = cx.entity().entity_id();
                        let registry = registry.clone();
                        let identity = identity.clone();
                        registry.update(cx, |registry, _| {
                            registry.reserve_pending(
                                entity_id,
                                PendingBinding::Inherited(identity),
                            );
                        });
                    };
                    let open = cx.update(|cx| {
                        Workspace::new_local(
                            vec![PathBuf::from(path.as_str())],
                            app_state,
                            None,
                            None,
                            Some(Box::new(init)),
                            OpenMode::NewWindow,
                            cx,
                        )
                    });
                    let _ = open.await;
                })
                .detach();
                None
            }
        }
    }

    /// Mark `window_id` `Connected` to `identity`, add it to the shared
    /// connection, and point its host presentation at the session.
    fn bind_connected(
        &mut self,
        window_id: WindowId,
        identity: &SessionIdentity,
        cx: &mut Context<Self>,
    ) {
        let already = self.windows.get(&window_id.as_u64()).is_some_and(|binding| {
            matches!(&binding.state, BindingState::Connected(bound) if bound == identity)
        });
        if !already {
            if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
                binding.state = BindingState::Connected(identity.clone());
                binding.selection_target = None;
            }
            self.install_host(window_id, identity.name.clone(), cx);
        }
        if !self.attach_connection_window(identity, window_id) {
            log::error!(
                "herdr connection for session {} was released during binding",
                identity.name
            );
        }
        cx.notify();
    }

    fn attach_connection_window(
        &mut self,
        identity: &SessionIdentity,
        window_id: WindowId,
    ) -> bool {
        self.connections
            .get_mut(identity)
            .is_some_and(|connection| connection.bound_windows.insert(window_id.as_u64()))
    }

    fn detach_bootstrap(&mut self, window_id: WindowId, cx: &mut Context<Self>) {
        let handle = self
            .windows
            .get(&window_id.as_u64())
            .map(|binding| binding.window);
        if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
            binding.state = BindingState::Unselected;
            binding.checkout_path = None;
            binding.selection_target = None;
        }
        if let Some(handle) = handle {
            self.host_sink.detach(handle, cx);
        }
    }

    fn candidates(&self) -> Vec<RoutingCandidate> {
        self.windows
            .iter()
            .map(|(window_id, binding)| RoutingCandidate {
                window_id: WindowId::from(*window_id),
                state: binding.state.clone(),
                checkout_paths: binding.roots.clone(),
            })
            .collect()
    }

    fn refresh_window_roots(&mut self, cx: &App) {
        let handles: Vec<(WindowId, WindowHandle<MultiWorkspace>)> = self
            .windows
            .iter()
            .map(|(window_id, binding)| (WindowId::from(*window_id), binding.window))
            .collect();
        for (window_id, handle) in handles {
            let Ok((workspace_id, roots)) = handle.read_with(cx, |multi_workspace, cx| {
                let workspace = multi_workspace.workspace();
                let workspace_id: Arc<str> = Arc::from(workspace.entity_id().to_string());
                let roots = workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .worktrees(cx)
                    .filter_map(|worktree| {
                        let root = worktree.read(cx).root_dir()?;
                        canonical_checkout_path(root.as_ref()).ok()
                    })
                    .collect::<Vec<_>>();
                (workspace_id, roots)
            }) else {
                continue;
            };
            let Some(binding) = self.windows.get_mut(&window_id.as_u64()) else {
                continue;
            };
            binding.workspace_id = Some(workspace_id);
            binding.checkout_path = roots.first().cloned();
            binding.roots = roots;
        }
    }

    // ------------------------------------------------------------- liveness

    /// Stream loss: refresh the catalog exactly once, inline — no new timer
    /// and no new connection task. Missing/not-running is a normal exit
    /// (`Unselected`, ownership dropped, mirrors untouched); still-running
    /// is `Failed`. Either way the registry releases the dead connection so
    /// a later `Retry` or selection creates a fresh client.
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
            self.dispatch_effects(identity, effects);
        }
        self.mark_state(identity, event, cx);
        self.connections.remove(identity);
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
        for window_id in window_ids {
            if let Some(binding) = self.windows.get_mut(&window_id.as_u64()) {
                binding.state = transition(binding.state.clone(), event.clone());
            }
        }
        cx.notify();
    }

    fn note_window_released(&mut self, window_id: WindowId, _cx: &mut Context<Self>) {
        self.windows.remove(&window_id.as_u64());
        self.prompted.remove(&window_id);
        let identities: Vec<SessionIdentity> = self
            .connections
            .iter()
            .filter(|(_, connection)| connection.bound_windows.contains(&window_id.as_u64()))
            .map(|(identity, _)| identity.clone())
            .collect();
        for identity in identities {
            self.detach_window(&identity, window_id);
        }
        // A connection that never gained a bound window (routing was
        // abandoned with its window) is released too.
        self.connections
            .retain(|_, connection| !connection.bound_windows.is_empty());
    }

    /// Register the established client for a session exactly once. Returns
    /// `false` when another connection task won the race; the caller then
    /// drops its own client and stream so the registry never holds two.
    fn install_connection(
        &mut self,
        identity: SessionIdentity,
        info: SessionInfo,
        client: Rc<dyn HerdrSessionHandle>,
        event_task: Task<()>,
    ) -> bool {
        if self.connections.contains_key(&identity) {
            return false;
        }
        self.connections.insert(
            identity,
            SessionConnection {
                info,
                client,
                bound_windows: HashSet::default(),
                event_task,
            },
        );
        true
    }

    fn shared_client(&self, identity: &SessionIdentity) -> Option<Rc<dyn HerdrSessionHandle>> {
        self.connections
            .get(identity)
            .map(|connection| connection.client.clone())
    }

    fn connection_exists(&self, identity: &SessionIdentity) -> bool {
        self.connections.contains_key(identity)
    }

    // ------------------------------------------------------------ test seams

    #[cfg(test)]
    pub(crate) fn test(_cx: &mut Context<Self>, gateway: HerdrGateway) -> Self {
        Self {
            windows: HashMap::default(),
            prompted: HashSet::default(),
            pending: HashMap::default(),
            connections: HashMap::default(),
            sync: AgentSyncState::default(),
            effects: Rc::new(QueuedAgentEffectSink::default()),
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
    pub(crate) fn effect_sink(&self) -> Rc<dyn AgentEffectSink> {
        self.effects.clone()
    }

    #[cfg(test)]
    pub(crate) fn set_effect_sink(&mut self, sink: Rc<dyn AgentEffectSink>) {
        self.effects = sink;
    }

    #[cfg(test)]
    pub(crate) fn pending_count(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    pub(crate) fn is_window_prompted(&self, window_id: WindowId) -> bool {
        self.prompted.contains(&window_id)
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
        target: SelectionTarget,
        cx: &mut Context<Self>,
    ) {
        self.start_binding_for_window(window_id, session_name, target, cx);
    }

    #[cfg(test)]
    pub(crate) fn finalize_binding_for_test(
        &mut self,
        bootstrap: WindowId,
        target: SelectionTarget,
        identity: &SessionIdentity,
        snapshot: &SessionSnapshot,
        cx: &mut Context<Self>,
    ) -> Option<WindowId> {
        self.finalize_binding(bootstrap, target, identity, snapshot, cx)
    }

    #[cfg(test)]
    pub(crate) fn classify_loss_for_test(
        &mut self,
        identity: &SessionIdentity,
        message: SharedString,
        sessions: Vec<SessionInfo>,
        cx: &mut Context<Self>,
    ) {
        self.classify_loss(identity, message, sessions, cx);
    }

    #[cfg(test)]
    pub(crate) fn bind_connected_for_test(
        &mut self,
        window_id: WindowId,
        identity: &SessionIdentity,
        cx: &mut Context<Self>,
    ) {
        self.bind_connected(window_id, identity, cx);
    }
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

fn stream_loss_event(
    identity: &SessionIdentity,
    message: SharedString,
    sessions: &[SessionInfo],
) -> BindingEvent {
    if sessions.iter().any(|session| {
        session.running && SessionIdentity::from_info(session) == *identity
    }) {
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
async fn run_connection(
    registry: Entity<HerdrSessionRegistry>,
    gateway: HerdrGateway,
    bootstrap: WindowHandle<MultiWorkspace>,
    window_id: WindowId,
    session_name: Arc<str>,
    mut cx: AsyncApp,
) {
    let deadline = cx.background_executor().now() + CONNECTION_DEADLINE;
    let start = registry.read_with(&mut cx, |registry, _| {
        let starting = matches!(
            registry.windows.get(&window_id.as_u64()).map(|binding| &binding.state),
            Some(BindingState::Starting { session_name: current })
                if current == &session_name
        );
        let target = registry
            .windows
            .get(&window_id.as_u64())
            .and_then(|binding| binding.selection_target);
        (starting, target)
    });
    let (true, Some(target)) = start else {
        return;
    };

    // 1. Refresh the session list until the selected entry and its reported
    //    socket appear — inside the single 15-second deadline.
    let info = loop {
        let sessions = gateway.list_sessions().await.unwrap_or_default();
        if let Some(info) = sessions
            .into_iter()
            .find(|session| session.running && session.name == session_name.as_ref())
        {
            break info;
        }
        let expired = cx.background_executor().now() >= deadline;
        let released = bootstrap.read_with(&mut cx, |_, _| ()).is_err();
        let cancelled = registry.read_with(&mut cx, |registry, _| {
            !matches!(
                registry.windows.get(&window_id.as_u64()).map(|binding| &binding.state),
                Some(BindingState::Starting { session_name: current })
                    if current == &session_name
            )
        });
        if expired || released || cancelled {
            if !released && !cancelled {
                registry.update(&mut cx, |registry, cx| {
                    registry.apply_binding_event(
                        window_id,
                        BindingEvent::SessionStillRunning(
                            "session did not appear within 15 seconds".into(),
                        ),
                        cx,
                    );
                    cx.notify();
                });
            } else if !released && cancelled {
                // The binding already moved on (disconnect or host exit);
                // nothing left to report.
            }
            return;
        }
        cx.background_executor().timer(ATTEMPT_INTERVAL).await;
    };
    let identity = SessionIdentity::from_info(&info);

    // 2. If the session already has a registry connection, reuse its client
    //    and event stream, fetch a fresh snapshot for this binding, and do
    //    not create a second subscription.
    let shared = registry.read_with(&mut cx, |registry, _| {
        registry.shared_client(&identity)
    });
    if let Some(client) = shared {
        match client.snapshot().await {
            Ok(snapshot) => {
                let _ = registry.update(&mut cx, |registry, cx| {
                    let final_window =
                        registry.finalize_binding(window_id, target, &identity, &snapshot, cx);
                    if final_window.is_some() {
                        let effects = import_snapshot(&mut registry.sync, &identity, &snapshot);
                        registry.dispatch_effects(&identity, effects);
                    }
                    cx.notify();
                });
            }
            Err(error) => fail(registry, window_id, format!("{error:#}"), &mut cx),
        }
        return;
    }

    let client = match gateway.connect(&info).await {
        Ok(client) => client,
        Err(error) => {
            log::error!("herdr connect failed: {error:#}");
            fail(registry, window_id, format!("{error:#}"), &mut cx);
            return;
        }
    };

    // 3. Open the event stream before the snapshot; the pump forwards every
    //    frame into a bounded channel while the single snapshot is fetched.
    let mut stream = match client.subscribe().await {
        Ok(stream) => stream,
        Err(error) => {
            log::error!("herdr subscribe failed: {error:#}");
            fail(registry, window_id, format!("{error:#}"), &mut cx);
            return;
        }
    };
    let (mut tx, mut rx) = mpsc::channel::<HerdrEvent>(EVENT_CHANNEL_CAPACITY);
    let pump = cx.spawn(async move |_cx| loop {
        match stream.next().await {
            Ok(Some(event)) => {
                if tx.try_send(event).is_err() {
                    break;
                }
            }
            Ok(None) => break,
            Err(error) => {
                log::debug!("herdr event stream error: {error:#}");
                break;
            }
        }
    });

    let snapshot = match client.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            drop(pump);
            log::error!("herdr snapshot failed: {error:#}");
            fail(registry, window_id, format!("{error:#}"), &mut cx);
            return;
        }
    };

    // Stop conditions: target-window release, user disconnect, host process
    // exit (recorded by `note_host_exit_for_window` moving the binding out
    // of `Starting`), or the single deadline.
    let released = bootstrap.read_with(&mut cx, |_, _| ()).is_err();
    let deadline_reached = cx.background_executor().now() >= deadline;
    let state = registry.read_with(&mut cx, |registry, _| {
        registry.windows.get(&window_id.as_u64()).map(|binding| binding.state.clone())
    });
    let still_starting = matches!(
        state,
        Some(BindingState::Starting { session_name: current })
            if current == session_name
    );
    if released || deadline_reached || !still_starting {
        drop(pump);
        if released || !still_starting {
            return;
        }
        registry.update(&mut cx, |registry, cx| {
            registry.apply_binding_event(
                window_id,
                BindingEvent::SessionStillRunning("connection deadline reached".into()),
                cx,
            );
            cx.notify();
        });
        return;
    }

    // Drain the buffered frames in order before importing.
    let mut buffered = Vec::new();
    while let Ok(event) = rx.try_recv() {
        buffered.push(event);
    }

    // Exactly one client and one event stream per session: install first,
    // and if another window's task won the race, drop ours and reuse.
    let installed = registry.update(&mut cx, |registry, _| {
        registry.install_connection(identity.clone(), info.clone(), client.clone(), pump)
    });
    if !installed {
        drop(client);
        let shared = registry.read_with(&mut cx, |registry, _| {
            registry.shared_client(&identity)
        });
        if let Some(client) = shared {
            if let Ok(snapshot) = client.snapshot().await {
                let _ = registry.update(&mut cx, |registry, cx| {
                    let final_window =
                        registry.finalize_binding(window_id, target, &identity, &snapshot, cx);
                    if final_window.is_some() {
                        let effects = import_snapshot(&mut registry.sync, &identity, &snapshot);
                        registry.dispatch_effects(&identity, effects);
                    }
                    cx.notify();
                });
            }
        }
        return;
    }

    // 4. Resolve the final target binding, then schedule every running agent
    //    from the snapshot through the deduplicated synchronization path.
    let _final_window = registry.update(&mut cx, |registry, cx| {
        let final_window = registry.finalize_binding(window_id, target, &identity, &snapshot, cx);
        if final_window.is_some() {
            let effects = import_snapshot(&mut registry.sync, &identity, &snapshot);
            registry.dispatch_effects(&identity, effects);
        }
        cx.notify();
        final_window
    });

    // 5. Buffered frames are applied after the binding settled, in order.
    for event in buffered {
        let effects = registry.update(&mut cx, |registry, _| {
            apply_event_effects(&mut registry.sync, &identity, &event)
        });
        registry.update(&mut cx, |registry, cx| {
            registry.dispatch_effects(&identity, effects);
            cx.notify();
        });
    }

    // 6. Steady state: further frames continue through the same channel.
    //    The ownership poll lets a last-window disconnect (which drops the
    //    connection entry, its client, and the pump task) end this loop.
    loop {
        futures::select_biased! {
            event = rx.next().fuse() => {
                match event {
                    Some(event) => {
                        let effects = registry.update(&mut cx, |registry, _| {
                            apply_event_effects(&mut registry.sync, &identity, &event)
                        });
                        registry.update(&mut cx, |registry, cx| {
                            registry.dispatch_effects(&identity, effects);
                            cx.notify();
                        });
                    }
                    None => break,
                }
            }
            _ = cx
                .background_executor()
                .timer(OWNERSHIP_POLL)
                .fuse() =>
            {
                let owned = registry
                    .read_with(&mut cx, |registry, _| registry.connection_exists(&identity));
                if !owned {
                    return;
                }
            }
        }
    }

    // 7. Stream loss: exactly one catalog refresh, then `Unselected` (normal
    //    exit) or `Failed`. No new timer and no new connection task.
    let sessions = gateway.list_sessions().await.unwrap_or_default();
    let message: SharedString = "herdr event stream ended".into();
    registry.update(&mut cx, move |registry, cx| {
        if !registry.connection_exists(&identity) {
            return;
        }
        registry.classify_loss(&identity, message, sessions, cx);
    });
}

fn fail(
    registry: Entity<HerdrSessionRegistry>,
    window_id: WindowId,
    message: String,
    cx: &mut AsyncApp,
) {
    let _ = registry.update(cx, |registry, cx| {
        registry.apply_binding_event(
            window_id,
            BindingEvent::SessionStillRunning(message.into()),
            cx,
        );
        cx.notify();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn empty_registry() -> HerdrSessionRegistry {
        HerdrSessionRegistry {
            windows: HashMap::default(),
            prompted: HashSet::default(),
            pending: HashMap::default(),
            connections: HashMap::default(),
            sync: AgentSyncState::default(),
            effects: Rc::new(QueuedAgentEffectSink::default()),
            host_sink: Rc::new(NoopHerdrHostSink),
            picker_sink: Rc::new(NoopSessionPickerSink),
            gateway: None,
            connection_tasks_started: 0,
            _window_observer: None,
            _window_close_subscription: None,
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
        let identity = session("main");
        let sessions = [session_info("main", true)];
        let event = stream_loss_event(&identity, "socket closed".into(), &sessions);
        let state = transition(BindingState::Connected(identity), event);
        assert!(matches!(state, BindingState::Failed { .. }));
    }

    #[test]
    fn selection_from_connected_window_always_targets_new_window() {
        assert_eq!(
            selection_target(&BindingState::Connected(session("main"))),
            SelectionTarget::NewWindow
        );
        assert_eq!(
            selection_target(&BindingState::Unselected),
            SelectionTarget::InvokingWindow
        );
    }

    #[test]
    fn invoking_selection_reuses_exact_eligible_window() {
        let focused = checkout("C:/repo/worktree");
        let route = route_focused_worktree(
            SelectionTarget::InvokingWindow,
            WindowId::from(1),
            &[],
            Some(&focused),
            Some(&session("main")),
            &[RoutingCandidate {
                window_id: WindowId::from(2),
                state: BindingState::Unselected,
                checkout_paths: vec![focused.clone()],
            }],
        );
        assert_eq!(route, Route::ReuseExact(WindowId::from(2)));
    }

    #[test]
    fn mismatched_invoking_window_inherits_into_new_focused_window() {
        let focused = checkout("C:/repo/worktree");
        let route = route_focused_worktree(
            SelectionTarget::InvokingWindow,
            WindowId::from(1),
            &[checkout("C:/repo/other")],
            Some(&focused),
            Some(&session("main")),
            &[],
        );
        assert_eq!(route, Route::OpenInherited);
    }

    #[test]
    fn connected_selection_keeps_its_new_target_window() {
        let focused = checkout("C:/repo/worktree");
        let route = route_focused_worktree(
            SelectionTarget::NewWindow,
            WindowId::from(1),
            &[],
            Some(&focused),
            Some(&session("main")),
            &[RoutingCandidate {
                window_id: WindowId::from(2),
                state: BindingState::Connected(session("main")),
                checkout_paths: vec![focused.clone()],
            }],
        );
        assert_eq!(route, Route::BindBootstrap);
    }

    #[test]
    fn pending_binding_is_consumed_once_and_inherited_is_silent() {
        let mut registry = empty_registry();
        let workspace_id = EntityId::from(7);
        let identity = session("main");
        registry.reserve_pending(workspace_id, PendingBinding::Inherited(identity.clone()));

        assert_eq!(registry.pending_count(), 1);
        assert_eq!(
            registry.consume_pending(workspace_id),
            Some(PendingBinding::Inherited(identity))
        );
        assert_eq!(registry.consume_pending(workspace_id), None);
        assert_eq!(registry.pending_count(), 0);
    }

    struct FakeHandle;

    impl HerdrSessionHandle for FakeHandle {
        fn subscribe(&self) -> LocalBoxFuture<'static, anyhow::Result<Box<dyn HerdrEventStream>>> {
            async {
                Err::<Box<dyn HerdrEventStream>, _>(anyhow::anyhow!("unused fake stream"))
            }
            .boxed_local()
        }

        fn snapshot(&self) -> LocalBoxFuture<'static, anyhow::Result<SessionSnapshot>> {
            async { Err::<SessionSnapshot, _>(anyhow::anyhow!("unused fake snapshot")) }
                .boxed_local()
        }
    }

    #[test]
    fn shared_connection_is_kept_until_last_window_and_then_can_be_recreated() {
        let mut registry = empty_registry();
        let identity = session("main");
        let info = session_info("main", true);
        assert!(registry.install_connection(
            identity.clone(),
            info.clone(),
            Rc::new(FakeHandle),
            Task::ready(()),
        ));
        assert!(!registry.install_connection(
            identity.clone(),
            info.clone(),
            Rc::new(FakeHandle),
            Task::ready(()),
        ));

        assert!(registry.attach_connection_window(&identity, WindowId::from(1)));
        assert!(registry.attach_connection_window(&identity, WindowId::from(2)));
        assert_eq!(registry.bound_window_count(&identity), 2);
        registry.detach_window(&identity, WindowId::from(1));
        assert!(registry.has_connection(&identity));
        assert_eq!(registry.bound_window_count(&identity), 1);
        registry.detach_window(&identity, WindowId::from(2));
        assert!(!registry.has_connection(&identity));

        assert!(registry.install_connection(
            identity.clone(),
            info,
            Rc::new(FakeHandle),
            Task::ready(()),
        ));
    }

    #[test]
    fn stream_loss_refresh_classifies_missing_session_without_reconnect() {
        let identity = session("main");
        let event = stream_loss_event(&identity, "socket closed".into(), &[session_info("main", false)]);
        assert_eq!(event, BindingEvent::SessionNotRunning);
        assert_eq!(transition(BindingState::Connected(identity), event), BindingState::Unselected);
        assert_eq!(empty_registry().connection_tasks_started(), 0);
    }
}
