use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use gpui::{
    App, AppContext as _, Context, DragMoveEvent, Entity, FocusHandle, Focusable, IntoElement,
    MouseButton, MouseDownEvent, ParentElement, Render, Styled, Subscription, Task, TaskExt,
    WeakEntity, Window, div, px,
};
use herdr::{
    CanonicalPath, ClientConfig, Endpoint, FocusEvent, Generation, HerdRClient, SessionSnapshot,
    canonical_checkout_path,
};
use paths::home_dir;
use project::{Event as ProjectEvent, ProjectPath};
use task::{RevealStrategy, RevealTarget, Shell, SpawnInTerminal, TaskId};
use terminal_view::TerminalView;
use ui::prelude::*;
use util::ResultExt as _;
use workspace::{MultiWorkspace, MultiWorkspaceEvent, OpenMode, Workspace};

static SESSION_OWNERS: LazyLock<Mutex<HashMap<Endpoint, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::default()));

fn claim_session(endpoint: &Endpoint, owner: u64) -> bool {
    match SESSION_OWNERS.lock() {
        Ok(mut owners) => match owners.get(endpoint) {
            Some(existing_owner) => *existing_owner == owner,
            None => {
                owners.insert(endpoint.clone(), owner);
                true
            }
        },
        Err(_) => {
            log::error!("HerdR session ownership registry was poisoned");
            false
        }
    }
}

fn release_session(endpoint: &Endpoint, owner: u64) {
    match SESSION_OWNERS.lock() {
        Ok(mut owners) => {
            if owners
                .get(endpoint)
                .is_some_and(|existing_owner| *existing_owner == owner)
            {
                owners.remove(endpoint);
            }
        }
        Err(_) => log::error!("HerdR session ownership registry was poisoned"),
    }
}
const DEFAULT_HOST_HEIGHT: gpui::Pixels = px(320.0);
const MIN_HOST_HEIGHT: gpui::Pixels = px(140.0);
const HOST_HEADER_HEIGHT: gpui::Pixels = px(32.0);

#[derive(Clone)]
struct DraggedHerdRHost;

impl Render for DraggedHerdRHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        gpui::Empty
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Unmapped,
}

impl ConnectionState {
    fn label(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Unmapped => "unmapped",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FocusOrigin {
    Zed,
    HerdR,
}

#[derive(Clone, Debug)]
struct PendingFocus {
    path: CanonicalPath,
    generation: Generation,
    origin: FocusOrigin,
}

pub struct HerdRHost {
    multi_workspace: WeakEntity<MultiWorkspace>,
    backing_workspace: Entity<Workspace>,
    fixed_worktree: PathBuf,
    session_endpoint: Endpoint,
    session_owner: u64,
    session_claimed: bool,
    terminal_view: Option<Entity<TerminalView>>,
    terminal_setup: Option<Task<anyhow::Result<()>>>,
    sync_task: Option<Task<()>>,
    shutdown: Arc<AtomicBool>,
    focus_handle: FocusHandle,
    _focus_subscription: Subscription,
    _subscriptions: Vec<Subscription>,
    subscribed_projects: HashSet<gpui::EntityId>,
    multi_workspace_subscribed: bool,
    dock_height: gpui::Pixels,
    collapsed: bool,
    maximized: bool,
    client: Option<HerdRClient>,
    snapshot: Option<SessionSnapshot>,
    connection_state: ConnectionState,
    mapped_path: Option<CanonicalPath>,
    pending_focus: Option<PendingFocus>,
    generation: Generation,
    last_error: Option<String>,
}

impl HerdRHost {
    fn new(
        multi_workspace: WeakEntity<MultiWorkspace>,
        backing_workspace: Entity<Workspace>,
        fixed_worktree: PathBuf,
        session_endpoint: Endpoint,
        session_owner: u64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let focus_handle = cx.focus_handle();
        let focus_subscription =
            cx.on_focus(&focus_handle, window, |host: &mut HerdRHost, window, cx| {
                if let Some(terminal_view) = host.terminal_view.as_ref() {
                    terminal_view.focus_handle(cx).focus(window, cx);
                }
            });
        Self {
            multi_workspace,
            backing_workspace,
            fixed_worktree,
            session_endpoint,
            session_owner,
            session_claimed: true,
            terminal_view: None,
            terminal_setup: None,
            sync_task: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            focus_handle,
            _focus_subscription: focus_subscription,
            _subscriptions: Vec::new(),
            subscribed_projects: HashSet::default(),
            multi_workspace_subscribed: false,
            dock_height: DEFAULT_HOST_HEIGHT,
            collapsed: false,
            maximized: false,
            client: None,
            snapshot: None,
            connection_state: ConnectionState::Disconnected,
            mapped_path: None,
            pending_focus: None,
            generation: Generation::default(),
            last_error: None,
        }
    }

    fn start(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.subscribe_to_window_projects(cx);
        self.start_terminal(window, cx);
        self.start_sync(window, cx);
    }

    fn start_terminal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.terminal_setup.is_some() || self.terminal_view.is_some() {
            return;
        }

        let project = self.backing_workspace.read(cx).project().clone();
        let fixed_worktree = self.fixed_worktree.clone();
        let terminal_task = project.update(cx, |project, cx| {
            project.create_terminal_task(
                SpawnInTerminal {
                    id: TaskId("HerdR".to_owned()),
                    full_label: "HerdR".to_owned(),
                    label: "HerdR".to_owned(),
                    command: Some("herdr".to_owned()),
                    command_label: "herdr".to_owned(),
                    cwd: Some(fixed_worktree),
                    use_new_terminal: true,
                    reveal: RevealStrategy::Never,
                    reveal_target: RevealTarget::Dock,
                    shell: Shell::System,
                    ..Default::default()
                },
                cx,
            )
        });
        let weak_workspace = self.backing_workspace.downgrade();
        let weak_project = project.downgrade();
        let workspace_id = self.backing_workspace.read(cx).database_id();
        let terminal_setup = cx.spawn_in(window, async move |host, cx| {
            match terminal_task.await {
                Ok(terminal) => {
                    let terminal_view = cx.new_window_entity(|window, cx| {
                        let mut view = TerminalView::new(
                            terminal,
                            weak_workspace,
                            workspace_id,
                            weak_project,
                            window,
                            cx,
                        );
                        view.set_show_workspace_actions(false, cx);
                        view
                    })?;
                    host.update(cx, |host, cx| {
                        host.terminal_view = Some(terminal_view);
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    host.update(cx, |host, cx| {
                        host.last_error = Some(format!("failed to start HerdR: {error}"));
                        host.connection_state = ConnectionState::Disconnected;
                        cx.notify();
                    })?;
                }
            }
            anyhow::Ok(())
        });
        self.terminal_setup = Some(terminal_setup);
    }

    fn start_sync(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.sync_task.is_some() {
            return;
        }
        let shutdown = Arc::clone(&self.shutdown);
        let configuration = ClientConfig::new(self.session_endpoint.clone());
        let sync_task = cx.spawn_in(window, async move |host, cx| {
            let reconnect_delay = Duration::from_secs(1);
            loop {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }

                if let Err(error) = host.update(cx, |host, cx| {
                    host.connection_state = ConnectionState::Connecting;
                    host.last_error = None;
                    cx.notify();
                }) {
                    log::debug!("HerdR host was released while connecting: {error}");
                    break;
                }

                let client = match HerdRClient::connect(configuration.clone()).await {
                    Ok(client) => client,
                    Err(error) => {
                        if let Err(update_error) = host.update(cx, |host, cx| {
                            host.connection_state = ConnectionState::Disconnected;
                            host.last_error = Some(error.to_string());
                            cx.notify();
                        }) {
                            log::debug!("failed to update HerdR connection error: {update_error}");
                            break;
                        }
                        cx.background_executor().timer(reconnect_delay).await;
                        continue;
                    }
                };

                let mut subscription = match client.subscribe().await {
                    Ok(subscription) => subscription,
                    Err(error) => {
                        if let Err(update_error) = host.update(cx, |host, cx| {
                            host.connection_state = ConnectionState::Disconnected;
                            host.last_error = Some(error.to_string());
                            cx.notify();
                        }) {
                            log::debug!(
                                "failed to update HerdR subscription error: {update_error}"
                            );
                            break;
                        }
                        cx.background_executor().timer(reconnect_delay).await;
                        continue;
                    }
                };

                let snapshot = match client.snapshot().await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        if let Err(update_error) = host.update(cx, |host, cx| {
                            host.connection_state = ConnectionState::Disconnected;
                            host.last_error = Some(error.to_string());
                            cx.notify();
                        }) {
                            log::debug!("failed to update HerdR snapshot error: {update_error}");
                            break;
                        }
                        cx.background_executor().timer(reconnect_delay).await;
                        continue;
                    }
                };

                if let Err(error) = host.update_in(cx, |host, window, cx| {
                    host.apply_snapshot(client.clone(), snapshot, window, cx);
                }) {
                    log::debug!("failed to apply HerdR snapshot: {error}");
                    break;
                }

                loop {
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    match subscription.next().await {
                        Ok(Some(event)) => {
                            let snapshot = match client.snapshot().await {
                                Ok(snapshot) => snapshot,
                                Err(error) => {
                                    if let Err(update_error) = host.update(cx, |host, cx| {
                                        host.connection_state = ConnectionState::Disconnected;
                                        host.last_error = Some(error.to_string());
                                        cx.notify();
                                    }) {
                                        log::debug!(
                                            "failed to update HerdR event error: {update_error}"
                                        );
                                    }
                                    break;
                                }
                            };
                            if let Err(error) = host.update_in(cx, |host, window, cx| {
                                host.apply_snapshot(client.clone(), snapshot, window, cx);
                                host.apply_herdr_focus(event, window, cx);
                            }) {
                                log::debug!("failed to apply HerdR focus event: {error}");
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            if let Err(update_error) = host.update(cx, |host, cx| {
                                host.connection_state = ConnectionState::Disconnected;
                                host.last_error = Some(error.to_string());
                                cx.notify();
                            }) {
                                log::debug!(
                                    "failed to update HerdR event stream error: {update_error}"
                                );
                            }
                            break;
                        }
                    }
                }

                if !shutdown.load(Ordering::Acquire) {
                    if let Err(error) = host.update(cx, |host, cx| {
                        host.client = None;
                        host.connection_state = ConnectionState::Disconnected;
                        cx.notify();
                    }) {
                        log::debug!("failed to update HerdR reconnect state: {error}");
                        break;
                    }
                    cx.background_executor().timer(reconnect_delay).await;
                }
            }
        });
        self.sync_task = Some(sync_task);
    }

    fn apply_snapshot(
        &mut self,
        client: HerdRClient,
        snapshot: SessionSnapshot,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.client = Some(client);
        self.snapshot = Some(snapshot.clone());
        self.connection_state = ConnectionState::Connected;
        self.last_error = None;
        if let Some(workspace_id) = snapshot.focused_workspace_id {
            self.apply_herdr_focus(FocusEvent { workspace_id }, window, cx);
        } else {
            self.mapped_path = None;
            cx.notify();
        }
    }

    fn apply_herdr_focus(
        &mut self,
        event: FocusEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some((workspace_id, checkout_path)) = self.snapshot.as_ref().and_then(|snapshot| {
            let workspace = snapshot
                .workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == event.workspace_id)?;
            Some((
                workspace.workspace_id.clone(),
                workspace.worktree.as_ref()?.checkout_path.clone(),
            ))
        }) else {
            self.set_unmapped(
                format!(
                    "HerdR workspace {} is not in the snapshot",
                    event.workspace_id
                ),
                cx,
            );
            return;
        };
        let Ok(path) = canonical_checkout_path(std::path::Path::new(&checkout_path)) else {
            self.set_unmapped(
                format!("HerdR workspace {workspace_id} has an unsupported checkout path"),
                cx,
            );
            return;
        };
        if self.pending_focus.as_ref().is_some_and(|pending| {
            pending.origin == FocusOrigin::HerdR
                && pending.generation.matches(self.generation)
                && pending.path == path
        }) {
            return;
        }

        if self.pending_focus.as_ref().is_some_and(|pending| {
            pending.origin == FocusOrigin::Zed
                && pending.generation.matches(self.generation)
                && pending.path == path
        }) {
            self.pending_focus = None;
            self.mapped_path = Some(path);
            self.connection_state = ConnectionState::Connected;
            cx.notify();
            return;
        }

        if self
            .current_zed_path(cx)
            .and_then(|current| canonical_checkout_path(&current).ok())
            .is_some_and(|current| current == path)
        {
            self.pending_focus = None;
            self.mapped_path = Some(path);
            self.connection_state = ConnectionState::Connected;
            cx.notify();
            return;
        }

        let generation = self.next_generation();
        self.pending_focus = Some(PendingFocus {
            path: path.clone(),
            generation,
            origin: FocusOrigin::HerdR,
        });
        self.mapped_path = Some(path.clone());
        self.connection_state = ConnectionState::Connected;
        self.focus_zed_path(path, window, cx);
        cx.notify();
    }

    fn focus_zed_path(&mut self, path: CanonicalPath, window: &mut Window, cx: &mut Context<Self>) {
        let Some(window_handle) = window.window_handle().downcast::<MultiWorkspace>() else {
            self.set_unmapped("HerdR host is not attached to a Zed window".to_owned(), cx);
            return;
        };
        let target_path = PathBuf::from(path.as_str());
        let generation = self.generation;
        let target = window_handle.update(cx, |multi_workspace, window, cx| {
            let matching_workspace = find_workspace_for_path(multi_workspace, &path, cx);
            let workspace = matching_workspace
                .clone()
                .unwrap_or_else(|| multi_workspace.workspace().clone());
            if let Some(workspace) = matching_workspace {
                multi_workspace.activate(workspace, None, window, cx);
            }
            let project = workspace.read(cx).project().clone();
            let worktree_task = project.update(cx, |project, cx| {
                project.find_or_create_worktree(&target_path, false, cx)
            });
            (workspace, worktree_task)
        });
        let (workspace, worktree_task) = match target {
            Ok(target) => target,
            Err(error) => {
                self.set_unmapped(format!("could not focus Zed worktree: {error}"), cx);
                return;
            }
        };
        let host = cx.weak_entity();
        cx.spawn(async move |_, cx| match worktree_task.await {
            Ok((worktree, relative_path)) => {
                if let Err(error) = host.update(cx, |host, cx| {
                    if host.shutdown.load(Ordering::Acquire) || !host.generation.matches(generation)
                    {
                        return;
                    }
                    let worktree_id = worktree.read(cx).id();
                    workspace.update(cx, |workspace, cx| {
                        workspace.project().update(cx, |project, cx| {
                            project.set_active_path(
                                Some(ProjectPath {
                                    worktree_id,
                                    path: relative_path,
                                }),
                                cx,
                            );
                        });
                    });
                }) {
                    log::debug!("failed to apply HerdR worktree focus: {error}");
                }
            }
            Err(error) => {
                if let Err(update_error) = host.update(cx, |host, cx| {
                    if host.shutdown.load(Ordering::Acquire) || !host.generation.matches(generation)
                    {
                        return;
                    }
                    host.set_unmapped(format!("could not open Zed worktree: {error}"), cx);
                }) {
                    log::debug!("failed to update HerdR worktree error: {update_error}");
                }
            }
        })
        .detach();
    }

    fn sync_zed_focus(&mut self, cx: &mut Context<Self>) {
        let Some(path) = self.current_zed_path(cx) else {
            return;
        };
        let Ok(path) = canonical_checkout_path(path.as_ref()) else {
            self.set_unmapped("active Zed worktree path is unsupported".to_owned(), cx);
            return;
        };

        if self.pending_focus.as_ref().is_some_and(|pending| {
            pending.origin == FocusOrigin::HerdR && pending.generation.matches(self.generation)
        }) {
            if self
                .pending_focus
                .as_ref()
                .is_some_and(|pending| pending.path == path)
            {
                self.pending_focus = None;
                self.mapped_path = Some(path);
                self.connection_state = ConnectionState::Connected;
                cx.notify();
            }
            return;
        }

        if self.connection_state == ConnectionState::Connected
            && self.pending_focus.is_none()
            && self.mapped_path.as_ref() == Some(&path)
        {
            return;
        }

        let Some(client) = self.client.clone() else {
            self.pending_focus = None;
            self.generation.advance();
            self.connection_state = ConnectionState::Disconnected;
            self.mapped_path = Some(path);
            cx.notify();
            return;
        };
        let Some(workspace_id) = self.workspace_for_path(&path) else {
            self.set_unmapped(
                format!("Zed worktree {} is not mapped in HerdR", path.as_str()),
                cx,
            );
            return;
        };

        let generation = self.next_generation();
        self.pending_focus = Some(PendingFocus {
            path: path.clone(),
            generation,
            origin: FocusOrigin::Zed,
        });
        self.mapped_path = Some(path);
        self.connection_state = ConnectionState::Connected;
        let host = cx.weak_entity();
        cx.spawn(async move |_, cx| {
            if let Err(error) = client.focus_workspace(workspace_id).await {
                if let Err(update_error) = host.update(cx, |host, cx| {
                    if host.shutdown.load(Ordering::Acquire) || !host.generation.matches(generation)
                    {
                        return;
                    }
                    host.pending_focus = None;
                    host.generation.advance();
                    host.connection_state = ConnectionState::Disconnected;
                    host.last_error = Some(error.to_string());
                    cx.notify();
                }) {
                    log::debug!("failed to update HerdR focus error: {update_error}");
                }
            }
        })
        .detach();
        cx.notify();
    }

    fn workspace_for_path(&self, path: &CanonicalPath) -> Option<String> {
        self.snapshot
            .as_ref()?
            .workspaces
            .iter()
            .find_map(|workspace| {
                let checkout_path = workspace.checkout_path()?;
                let checkout_path = canonical_checkout_path(checkout_path).ok()?;
                (checkout_path == *path).then(|| workspace.workspace_id.clone())
            })
    }

    fn current_zed_path(&self, cx: &App) -> Option<PathBuf> {
        let multi_workspace = self.multi_workspace.upgrade()?;
        multi_workspace.read_with(cx, |multi_workspace, cx| {
            multi_workspace
                .workspace()
                .read(cx)
                .project()
                .read(cx)
                .active_project_directory(cx)
                .map(|path| path.to_path_buf())
        })
    }

    fn subscribe_to_window_projects(&mut self, cx: &mut Context<Self>) {
        let Some(multi_workspace) = self.multi_workspace.upgrade() else {
            return;
        };
        let workspaces = multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.workspaces().cloned().collect::<Vec<_>>()
        });
        for workspace in workspaces {
            self.subscribe_to_project(&workspace, cx);
        }
        if !self.multi_workspace_subscribed {
            self.multi_workspace_subscribed = true;
            let subscription = cx.subscribe(&multi_workspace, |host, _, event, cx| match event {
                MultiWorkspaceEvent::ActiveWorkspaceChanged { .. } => {
                    host.subscribe_to_window_projects(cx);
                    host.sync_zed_focus(cx);
                }
                MultiWorkspaceEvent::WorkspaceAdded(_) => host.subscribe_to_window_projects(cx),
                MultiWorkspaceEvent::WorkspaceRemoved(_)
                | MultiWorkspaceEvent::ProjectGroupsChanged => {}
            });
            self._subscriptions.push(subscription);
        }
    }

    fn subscribe_to_project(&mut self, workspace: &Entity<Workspace>, cx: &mut Context<Self>) {
        let project = workspace.read(cx).project().clone();
        if !self.subscribed_projects.insert(project.entity_id()) {
            return;
        }
        self._subscriptions
            .push(cx.subscribe(&project, |host, _, event: &ProjectEvent, cx| {
                if matches!(event, ProjectEvent::ActiveEntryChanged(_)) {
                    host.sync_zed_focus(cx);
                }
            }));
    }

    fn next_generation(&mut self) -> Generation {
        self.generation.advance()
    }

    fn set_unmapped(&mut self, message: String, cx: &mut Context<Self>) {
        self.connection_state = ConnectionState::Unmapped;
        self.pending_focus = None;
        self.generation.advance();
        self.last_error = Some(message);
        cx.notify();
    }

    fn toggle_maximize(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.maximized = !self.maximized;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    fn toggle_collapse(&mut self, cx: &mut Context<Self>) {
        self.collapsed = !self.collapsed;
        cx.notify();
    }

    fn terminate(&mut self, cx: &mut Context<Self>) {
        self.shutdown.store(true, Ordering::Release);
        self.sync_task.take();
        self.terminal_setup.take();
        self.client = None;
        if self.session_claimed {
            release_session(&self.session_endpoint, self.session_owner);
            self.session_claimed = false;
        }
        if let Some(terminal_view) = self.terminal_view.take() {
            terminal_view.update(cx, |terminal_view, cx| {
                terminal_view
                    .terminal()
                    .update(cx, |terminal, _| terminal.kill_active_task());
            });
        }
    }

    fn close(&mut self, cx: &mut Context<Self>) {
        self.terminate(cx);
        let host_id = cx.entity().entity_id();
        if let Some(multi_workspace) = self.multi_workspace.upgrade() {
            multi_workspace.update(cx, |multi_workspace, cx| {
                let is_this_host = multi_workspace
                    .window_root_host()
                    .cloned()
                    .and_then(|host| host.downcast::<HerdRHost>().ok())
                    .is_some_and(|host| host.entity_id() == host_id);
                if is_this_host {
                    multi_workspace.set_window_root_host(None, cx);
                }
            });
        }
        cx.notify();
    }

    fn status_text(&self) -> String {
        let session = match &self.session_endpoint {
            Endpoint::Filesystem(path) => path.display().to_string(),
            Endpoint::Namespaced(name) => name.clone(),
        };
        let mut status = format!("{} · session={session}", self.connection_state.label());
        if let Some(path) = self.mapped_path.as_ref() {
            status.push_str(" · ");
            status.push_str(path.as_str());
        }
        if let Some(error) = self.last_error.as_ref() {
            status.push_str(" · ");
            status.push_str(error);
        }
        status
    }
}

impl Drop for HerdRHost {
    fn drop(&mut self) {
        if self.session_claimed {
            release_session(&self.session_endpoint, self.session_owner);
            self.session_claimed = false;
        }
    }
}

impl Focusable for HerdRHost {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for HerdRHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let collapsed = self.collapsed;
        let maximized = self.maximized;
        let host_height = if collapsed {
            HOST_HEADER_HEIGHT
        } else {
            self.dock_height
        };
        let terminal = self.terminal_view.clone();
        let status = self.status_text();
        let fixed_worktree = self.fixed_worktree.display().to_string();
        let host = div()
            .id("herdr-host")
            .track_focus(&self.focus_handle)
            .relative()
            .flex_col()
            .flex_shrink_0()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().colors().panel_background)
            .border_t_1()
            .border_color(cx.theme().colors().border)
            .when(maximized, |this| this.absolute().inset_0().h_full())
            .when(!maximized, |this| this.h(host_height))
            .child(
                h_flex()
                    .h(HOST_HEADER_HEIGHT)
                    .flex_shrink_0()
                    .px_2()
                    .gap_2()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().colors().border)
                    .child("HerdR")
                    .child(div().flex_1().truncate().child(status))
                    .child(format!("cwd: {fixed_worktree}"))
                    .child(
                        Button::new(
                            "herdr-collapse",
                            if collapsed { "Expand" } else { "Collapse" },
                        )
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .on_click(cx.listener(|host, _, _, cx| host.toggle_collapse(cx))),
                    )
                    .child(
                        Button::new(
                            "herdr-maximize",
                            if maximized { "Restore" } else { "Maximize" },
                        )
                        .label_size(LabelSize::Small)
                        .style(ButtonStyle::Subtle)
                        .on_click(
                            cx.listener(|host, _, window, cx| host.toggle_maximize(window, cx)),
                        ),
                    )
                    .child(
                        Button::new("herdr-close", "Close")
                            .label_size(LabelSize::Small)
                            .style(ButtonStyle::Subtle)
                            .on_click(cx.listener(|host, _, _, cx| host.close(cx))),
                    ),
            )
            .when(!collapsed, |this| {
                this.child(
                    div()
                        .id("herdr-terminal")
                        .flex_1()
                        .min_h_0()
                        .size_full()
                        .children(terminal)
                        .when(self.terminal_view.is_none(), |this| {
                            this.child("Starting HerdR…")
                        }),
                )
                .child(
                    div()
                        .id("herdr-resize-handle")
                        .absolute()
                        .top(-px(3.0))
                        .left_0()
                        .w_full()
                        .h(px(6.0))
                        .cursor_row_resize()
                        .on_drag(DraggedHerdRHost, |_, _, _, cx| cx.new(|_| DraggedHerdRHost))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|_, _: &MouseDownEvent, _, cx| cx.stop_propagation()),
                        ),
                )
            });

        host.on_drag_move(cx.listener(
            |host, event: &DragMoveEvent<DraggedHerdRHost>, window, cx| {
                let viewport_height = window.viewport_size().height;
                let height = (viewport_height - event.event.position.y)
                    .max(MIN_HOST_HEIGHT)
                    .min(viewport_height - HOST_HEADER_HEIGHT);
                host.dock_height = height;
                cx.notify();
            },
        ))
    }
}

fn find_workspace_for_path(
    multi_workspace: &MultiWorkspace,
    path: &CanonicalPath,
    cx: &App,
) -> Option<Entity<Workspace>> {
    multi_workspace.workspaces().find_map(|workspace| {
        let project = workspace.read(cx).project().read(cx);
        project.worktrees(cx).find_map(|worktree| {
            let root = worktree.read(cx).root_dir()?;
            let root = canonical_checkout_path(root.as_ref()).ok()?;
            (root == *path).then(|| workspace.clone())
        })
    })
}

fn fixed_worktree_for(workspace: &Workspace, cx: &App) -> PathBuf {
    workspace
        .project()
        .read(cx)
        .active_project_directory(cx)
        .map(|path| path.to_path_buf())
        .or_else(|| workspace.project().read(cx).first_project_directory(cx))
        .unwrap_or_else(|| home_dir().clone())
}

fn host_from_multi_workspace(multi_workspace: &MultiWorkspace) -> Option<Entity<HerdRHost>> {
    multi_workspace
        .window_root_host()
        .cloned()
        .and_then(|host| host.downcast::<HerdRHost>().ok())
}

fn install_host(
    multi_workspace: &mut MultiWorkspace,
    workspace: Entity<Workspace>,
    fixed_worktree: PathBuf,
    window: &mut Window,
    cx: &mut Context<MultiWorkspace>,
) {
    if let Some(host) = host_from_multi_workspace(multi_workspace) {
        host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
        return;
    }

    let session_endpoint = ClientConfig::default().endpoint;
    let session_owner = window.window_handle().window_id().as_u64();
    if !claim_session(&session_endpoint, session_owner) {
        log::info!("HerdR session is already hosted by another Zed window");
        return;
    }
    let weak_multi_workspace = cx.weak_entity();
    let host = cx.new(|cx| {
        HerdRHost::new(
            weak_multi_workspace,
            workspace,
            fixed_worktree,
            session_endpoint,
            session_owner,
            window,
            cx,
        )
    });
    multi_workspace.set_window_root_host(Some(host.clone().into()), cx);
    host.update(cx, |host, cx| host.start(window, cx));
}

pub fn open_current(workspace: &mut Workspace, window: &mut Window, cx: &mut Context<Workspace>) {
    let fixed_worktree = fixed_worktree_for(workspace, cx);
    let Some(window_handle) = window.window_handle().downcast::<MultiWorkspace>() else {
        return;
    };
    let workspace_entity = cx.entity();
    window_handle
        .update(cx, |multi_workspace, window, cx| {
            install_host(
                multi_workspace,
                workspace_entity,
                fixed_worktree,
                window,
                cx,
            );
        })
        .log_err();
}

pub fn open_current_from_app(cx: &mut App) {
    workspace::with_active_or_new_workspace(cx, |workspace, window, cx| {
        open_current(workspace, window, cx);
    });
}

pub fn open_new_window(
    workspace: &mut Workspace,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let fixed_worktree = fixed_worktree_for(workspace, cx);
    let app_state = workspace.app_state().clone();
    let open_task = Workspace::new_local(
        vec![fixed_worktree.clone()],
        app_state,
        None,
        None,
        None,
        OpenMode::NewWindow,
        cx,
    );
    cx.spawn_in(window, async move |_, cx| {
        let result = open_task.await?;
        result.window.update(cx, |multi_workspace, window, cx| {
            install_host(
                multi_workspace,
                result.workspace,
                fixed_worktree,
                window,
                cx,
            );
        })?;
        anyhow::Ok(())
    })
    .detach_and_log_err(cx);
}
pub fn open_new_window_from_app(cx: &mut App) {
    workspace::with_active_or_new_workspace(cx, |workspace, window, cx| {
        open_new_window(workspace, window, cx);
    });
}

fn with_active_host(
    cx: &mut App,
    action: impl FnOnce(&mut HerdRHost, &mut Window, &mut Context<HerdRHost>),
) {
    let Some(window_handle) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    window_handle
        .update(cx, |multi_workspace, window, cx| {
            if let Some(host) = host_from_multi_workspace(multi_workspace) {
                host.update(cx, |host, cx| action(host, window, cx));
            }
        })
        .log_err();
}

pub fn toggle_maximize_from_app(cx: &mut App) {
    with_active_host(cx, |host, window, cx| host.toggle_maximize(window, cx));
}

pub fn toggle_collapse_from_app(cx: &mut App) {
    with_active_host(cx, |host, _, cx| host.toggle_collapse(cx));
}

pub fn close_from_app(cx: &mut App) {
    let Some(window_handle) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    window_handle
        .update(cx, |multi_workspace, _window, cx| {
            if let Some(host) = host_from_multi_workspace(multi_workspace) {
                host.update(cx, |host, cx| host.terminate(cx));
                multi_workspace.set_window_root_host(None, cx);
            }
        })
        .log_err();
}

pub fn status_from_app(cx: &mut App) {
    with_active_host(cx, |host, window, cx| host.focus_handle.focus(window, cx));
}
