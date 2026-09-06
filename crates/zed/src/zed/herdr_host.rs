use std::path::PathBuf;
use std::rc::Rc;

use gpui::{
    App, AppContext as _, Context, Entity, FocusHandle, Focusable, IntoElement, ParentElement,
    Render, Subscription, Task, Window, WindowHandle, div, px,
};
use paths::home_dir;
use task::{RevealStrategy, RevealTarget, Shell, SpawnInTerminal, TaskId};
use terminal_view::TerminalView;
use ui::{TintColor, Tooltip, prelude::*};
use workspace::{MultiWorkspace, Workspace};

use super::herdr_session_registry::{
    BindingState, HerdrHostSink, HerdrLaunch, HerdrSessionRegistry,
};

const HOST_HEADER_HEIGHT: gpui::Pixels = px(32.0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HerdRVisibilityTransition {
    ShowAndFocus,
    FocusOnly,
    HideAndRestoreEditor,
}

pub(crate) fn toggle_visibility(visible: bool, herdr_focused: bool) -> HerdRVisibilityTransition {
    match (visible, herdr_focused) {
        (false, _) => HerdRVisibilityTransition::ShowAndFocus,
        (true, false) => HerdRVisibilityTransition::FocusOnly,
        (true, true) => HerdRVisibilityTransition::HideAndRestoreEditor,
    }
}

/// Host adapter installed into the application-global session registry.
#[derive(Default)]
pub(crate) struct HostSink;

impl HerdrHostSink for HostSink {
    fn install(
        &self,
        window: WindowHandle<MultiWorkspace>,
        launch: HerdrLaunch,
        cx: &mut Context<HerdrSessionRegistry>,
    ) {
        let _ = window.update(cx, |multi_workspace, window, cx| {
            let workspace = multi_workspace.workspace().clone();
            install_host(multi_workspace, workspace, launch, window, cx);
        });
    }

    fn detach(&self, window: WindowHandle<MultiWorkspace>, cx: &mut Context<HerdrSessionRegistry>) {
        let _ = window.update(cx, |multi_workspace, _window, cx| {
            if let Some(host) = host_from_multi_workspace(multi_workspace) {
                host.update(cx, |host, cx| host.detach(cx));
            }
            multi_workspace.set_herdr_visible(true, cx);
        });
    }
}

pub(crate) fn sink() -> Rc<dyn HerdrHostSink> {
    Rc::new(HostSink)
}

pub struct HerdRStatusButton {
    multi_workspace: Option<gpui::WeakEntity<MultiWorkspace>>,
    window_id: Option<gpui::WindowId>,
    _multi_workspace_subscription: Option<Subscription>,
    _registry_subscription: Option<Subscription>,
}

impl HerdRStatusButton {
    pub fn new(
        multi_workspace: Option<gpui::WeakEntity<MultiWorkspace>>,
        window_id: Option<gpui::WindowId>,
        cx: &mut Context<Self>,
    ) -> Self {
        let subscription = multi_workspace.as_ref().and_then(|multi_workspace| {
            multi_workspace
                .upgrade()
                .map(|multi_workspace| cx.observe(&multi_workspace, |_, _, cx| cx.notify()))
        });
        let registry_subscription = HerdrSessionRegistry::try_global(cx)
            .map(|registry| cx.observe(&registry, |_, _, cx| cx.notify()));
        Self {
            multi_workspace,
            window_id,
            _multi_workspace_subscription: subscription,
            _registry_subscription: registry_subscription,
        }
    }
}

impl Render for HerdRStatusButton {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let selected = self
            .multi_workspace
            .as_ref()
            .and_then(|multi_workspace| multi_workspace.upgrade())
            .is_some_and(|multi_workspace| multi_workspace.read(cx).herdr_visible());
        let label = self
            .window_id
            .and_then(|window_id| {
                HerdrSessionRegistry::try_global(cx)
                    .map(|registry| registry.read(cx).binding_state(window_id))
                    .map(|state| binding_label(&state))
            })
            .unwrap_or_else(|| "herdr: Select a session".to_owned());

        h_flex()
            .gap_1()
            .child(
                IconButton::new("herdr-status-button", IconName::Terminal)
                    .tab_index(0isize)
                    .aria_label(label.clone())
                    .icon_size(IconSize::Small)
                    .toggle_state(selected)
                    .selected_style(ButtonStyle::Tinted(TintColor::Accent))
                    .tooltip(|_window, cx| {
                        Tooltip::for_action(
                            "Show herdr Status",
                            &zed_actions::herdr::ShowHerdrStatus,
                            cx,
                        )
                    })
                    .on_click(|_, window, cx| {
                        window.dispatch_action(Box::new(zed_actions::herdr::ShowHerdrStatus), cx);
                    }),
            )
            .child(Label::new(label).size(LabelSize::Small))
    }
}

impl workspace::StatusItemView for HerdRStatusButton {
    fn set_active_pane_item(
        &mut self,
        _active_pane_item: Option<&dyn workspace::ItemHandle>,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
    }

    fn hide_setting(&self, _cx: &App) -> Option<workspace::HideStatusItem> {
        None
    }
}

pub struct HerdRHost {
    registry: Entity<HerdrSessionRegistry>,
    window_id: gpui::WindowId,
    backing_workspace: Entity<Workspace>,
    fixed_worktree: PathBuf,
    launch: Option<HerdrLaunch>,
    terminal_view: Option<Entity<TerminalView>>,
    terminal_setup: Option<Task<anyhow::Result<()>>>,
    terminal_completion: Option<Task<()>>,
    focus_handle: FocusHandle,
    _focus_subscription: Subscription,
    _registry_subscription: Subscription,
    collapsed: bool,
    maximized: bool,
}

impl HerdRHost {
    fn new(
        registry: Entity<HerdrSessionRegistry>,
        window_id: gpui::WindowId,
        backing_workspace: Entity<Workspace>,
        fixed_worktree: PathBuf,
        launch: Option<HerdrLaunch>,
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
        let registry_subscription = cx.observe(&registry, |_, _, cx| cx.notify());
        Self {
            registry,
            window_id,
            backing_workspace,
            fixed_worktree,
            launch,
            terminal_view: None,
            terminal_setup: None,
            terminal_completion: None,
            focus_handle,
            _focus_subscription: focus_subscription,
            _registry_subscription: registry_subscription,
            collapsed: false,
            maximized: false,
        }
    }

    fn start_terminal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.launch.is_none() || self.terminal_setup.is_some() || self.terminal_view.is_some() {
            return;
        }
        let launch = self.launch.clone().expect("selected host has a launch");
        let project = self.backing_workspace.read(cx).project().clone();
        let fixed_worktree = self.fixed_worktree.clone();
        let terminal_task = project.update(cx, |project, cx| {
            project.create_terminal_task(
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
                },
                cx,
            )
        });
        let weak_workspace = self.backing_workspace.downgrade();
        let weak_project = project.downgrade();
        let workspace_id = self.backing_workspace.read(cx).database_id();
        self.terminal_setup = Some(cx.spawn_in(window, async move |host, cx| {
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
                        let completion = terminal_view
                            .read(cx)
                            .terminal()
                            .read(cx)
                            .wait_for_completed_task(cx);
                        let completion_host = cx.weak_entity();
                        let window_id = host.window_id;
                        host.terminal_view = Some(terminal_view);
                        host.terminal_setup = None;
                        host.terminal_completion = Some(cx.spawn(async move |_, cx| {
                            let _ = completion.await;
                            let _ = completion_host.update(cx, |host, cx| {
                                host.terminal_completion = None;
                                host.terminal_view = None;
                                host.launch = None;
                                cx.notify();
                            });
                            let _ = cx.update(|cx| {
                                HerdrSessionRegistry::note_host_exit_for_window(window_id, cx);
                            });
                        }));
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    log::error!("failed to start herdr session host: {error:#}");
                    host.update(cx, |host, cx| {
                        host.terminal_setup = None;
                        cx.notify();
                    })?;
                }
            }
            anyhow::Ok(())
        }));
    }

    fn update_launch(&mut self, launch: HerdrLaunch, window: &mut Window, cx: &mut Context<Self>) {
        let start = self.launch.is_none();
        self.launch = Some(launch);
        if start {
            self.start_terminal(window, cx);
        }
        cx.notify();
    }

    fn detach(&mut self, cx: &mut Context<Self>) {
        self.launch = None;
        self.terminal_setup.take();
        self.terminal_completion.take();
        if let Some(terminal_view) = self.terminal_view.take() {
            terminal_view.update(cx, |terminal_view, cx| {
                terminal_view
                    .terminal()
                    .update(cx, |terminal, _| terminal.kill_active_task());
            });
        }
        cx.notify();
    }
    fn binding_state(&self, cx: &App) -> BindingState {
        self.registry.read(cx).binding_state(self.window_id)
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

    fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(multi_workspace) = window.window_handle().downcast::<MultiWorkspace>() {
            let _ = multi_workspace.update(cx, |multi_workspace, _window, cx| {
                multi_workspace.set_herdr_visible(false, cx);
                multi_workspace.focus_active_workspace(window, cx);
            });
        }
    }

    fn choose_session(&self, window: &mut Window, cx: &mut Context<Self>) {
        window.dispatch_action(Box::new(zed_actions::herdr::SelectOrCreateSession), cx);
    }

    fn retry(&self, cx: &mut Context<Self>) {
        if let Some(registry) = HerdrSessionRegistry::try_global(cx) {
            let window_id = self.window_id;
            registry.update(cx, |registry, cx| {
                if let Err(error) = registry.retry(window_id, cx) {
                    log::error!("herdr retry failed: {error:#}");
                }
            });
        }
    }

    fn is_focused(&self, window: &Window, cx: &App) -> bool {
        self.focus_handle.contains_focused(window, cx)
    }
}

impl Drop for HerdRHost {
    fn drop(&mut self) {
        // Dropping the TerminalView closes only this local herdr client
        // process. The registry owns the session connection and server.
    }
}

impl Focusable for HerdRHost {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for HerdRHost {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let state = self.binding_state(cx);
        let label = binding_label(&state);
        let collapsed = self.collapsed;
        let maximized = self.maximized;
        let terminal = self.terminal_view.clone();
        let controls = match state {
            BindingState::Unselected => h_flex().gap_2().child(
                div()
                    .debug_selector(|| "herdr-action-choose-session".to_owned())
                    .child(
                        Button::new("herdr-choose-session", "Choose Session")
                            .label_size(LabelSize::Small)
                            .on_click(
                                cx.listener(|host, _, window, cx| host.choose_session(window, cx)),
                            ),
                    ),
            ),
            BindingState::Starting { .. } | BindingState::Connected(_) => h_flex(),
            BindingState::Failed { .. } => h_flex()
                .gap_2()
                .child(
                    div()
                        .debug_selector(|| "herdr-action-retry".to_owned())
                        .child(
                            Button::new("herdr-retry", "Retry")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|host, _, _, cx| host.retry(cx))),
                        ),
                )
                .child(
                    div()
                        .debug_selector(|| "herdr-action-choose-session".to_owned())
                        .child(
                            Button::new("herdr-choose-session", "Choose Session")
                                .label_size(LabelSize::Small)
                                .on_click(cx.listener(|host, _, window, cx| {
                                    host.choose_session(window, cx)
                                })),
                        ),
                ),
        };
        let host = div()
            .id("herdr-host")
            .track_focus(&self.focus_handle)
            .relative()
            .flex()
            .flex_col()
            .when(!collapsed, |this| this.flex_1())
            .min_h_0()
            .w_full()
            .overflow_hidden()
            .bg(cx.theme().colors().panel_background)
            .border_t_1()
            .border_color(cx.theme().colors().border)
            .when(maximized, |this| this.absolute().inset_0().h_full())
            .when(!maximized && collapsed, |this| this.h(HOST_HEADER_HEIGHT))
            .child(
                h_flex()
                    .h(HOST_HEADER_HEIGHT)
                    .flex_shrink_0()
                    .px_2()
                    .gap_2()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().colors().border)
                    .child(Label::new(label))
                    .child(div().flex_1())
                    .child(controls)
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
                            .on_click(cx.listener(|host, _, window, cx| host.close(window, cx))),
                    ),
            )
            .when(!collapsed && self.launch.is_some(), |this| {
                this.child(
                    div()
                        .id("herdr-terminal")
                        .flex_1()
                        .min_h_0()
                        .w_full()
                        .overflow_hidden()
                        .children(terminal)
                        .when(self.terminal_view.is_none(), |this| {
                            this.child("Starting herdr…")
                        }),
                )
            })
            .into_any_element();
        host
    }
}

pub(crate) fn binding_label(state: &BindingState) -> String {
    match state {
        BindingState::Unselected => "herdr: Select a session".to_owned(),
        BindingState::Starting { session_name } => {
            format!("herdr: Starting {session_name}…")
        }
        BindingState::Connected(session) => format!("herdr: Connected to {}", session.name),
        BindingState::Failed { session_name, .. } => {
            format!("herdr: Could not connect to {session_name}")
        }
    }
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

/// Restores only the view requested by persisted state. It deliberately does
/// not choose a session, spawn a process, or reconnect anything.
pub fn restore_view_only_if_visible(
    window_handle: WindowHandle<MultiWorkspace>,
    cx: &mut gpui::AsyncApp,
) {
    let _ = window_handle.update(cx, |multi_workspace, window, cx| {
        if !multi_workspace.herdr_visible() || host_from_multi_workspace(multi_workspace).is_some()
        {
            return;
        }
        install_unselected_host(multi_workspace, window, cx);
    });
}

fn install_unselected_host(
    multi_workspace: &mut MultiWorkspace,
    window: &mut Window,
    cx: &mut Context<MultiWorkspace>,
) {
    if host_from_multi_workspace(multi_workspace).is_some() {
        return;
    }
    let Some(registry) = HerdrSessionRegistry::try_global(cx) else {
        return;
    };
    let workspace = multi_workspace.workspace().clone();
    let fixed_worktree = fixed_worktree_for(workspace.read(cx), cx);
    let host = cx.new(|cx| {
        HerdRHost::new(
            registry,
            window.window_handle().window_id(),
            workspace,
            fixed_worktree,
            None,
            window,
            cx,
        )
    });
    multi_workspace.set_window_root_host(Some(host.into()), cx);
}

pub(crate) fn install_host(
    multi_workspace: &mut MultiWorkspace,
    workspace: Entity<Workspace>,
    launch: HerdrLaunch,
    window: &mut Window,
    cx: &mut Context<MultiWorkspace>,
) {
    if let Some(host) = host_from_multi_workspace(multi_workspace) {
        let fixed_worktree = fixed_worktree_for(workspace.read(cx), cx);
        multi_workspace.set_herdr_visible(true, cx);
        host.update(cx, |host, cx| {
            // A restored/view-only host may retain the old workspace while
            // the active workspace changed before the next selection.
            host.backing_workspace = workspace.clone();
            host.fixed_worktree = fixed_worktree;
            host.update_launch(launch, window, cx);
        });
        host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
        return;
    }
    let Some(registry) = HerdrSessionRegistry::try_global(cx) else {
        return;
    };
    let fixed_worktree = fixed_worktree_for(workspace.read(cx), cx);
    let host = cx.new(|cx| {
        HerdRHost::new(
            registry,
            window.window_handle().window_id(),
            workspace,
            fixed_worktree,
            Some(launch),
            window,
            cx,
        )
    });
    multi_workspace.set_window_root_host(Some(host.clone().into()), cx);
    multi_workspace.set_herdr_visible(true, cx);
    host.update(cx, |host, cx| host.start_terminal(window, cx));
    host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
}

pub fn toggle_from_app(cx: &mut App) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    let _ = window.update(cx, |multi_workspace, window, cx| {
        let window_id = window.window_handle().window_id();
        let state = HerdrSessionRegistry::try_global(cx)
            .map(|registry| registry.read(cx).binding_state(window_id))
            .unwrap_or(BindingState::Unselected);
        if matches!(state, BindingState::Unselected) {
            HerdrSessionRegistry::select_or_create_for_window(window_id, cx);
            return;
        }
        let Some(host) = host_from_multi_workspace(multi_workspace) else {
            return;
        };
        match toggle_visibility(
            multi_workspace.herdr_visible(),
            host.read(cx).is_focused(window, cx),
        ) {
            HerdRVisibilityTransition::ShowAndFocus | HerdRVisibilityTransition::FocusOnly => {
                multi_workspace.set_herdr_visible(true, cx);
                host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
            }
            HerdRVisibilityTransition::HideAndRestoreEditor => {
                multi_workspace.set_herdr_visible(false, cx);
                multi_workspace.focus_active_workspace(window, cx);
            }
        }
    });
}

pub fn focus_from_app(cx: &mut App) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    let _ = window.update(cx, |multi_workspace, window, cx| {
        let window_id = window.window_handle().window_id();
        let state = HerdrSessionRegistry::try_global(cx)
            .map(|registry| registry.read(cx).binding_state(window_id))
            .unwrap_or(BindingState::Unselected);
        if matches!(state, BindingState::Unselected) {
            HerdrSessionRegistry::select_or_create_for_window(window_id, cx);
        } else if let Some(host) = host_from_multi_workspace(multi_workspace) {
            multi_workspace.set_herdr_visible(true, cx);
            host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
        }
    });
}

fn with_active_host(
    cx: &mut App,
    action: impl FnOnce(&mut HerdRHost, &mut Window, &mut Context<HerdRHost>) + 'static,
) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    let _ = window.update(cx, move |multi_workspace, window, cx| {
        if let Some(host) = host_from_multi_workspace(multi_workspace) {
            host.update(cx, |host, cx| action(host, window, cx));
        }
    });
}

pub fn toggle_maximize_from_app(cx: &mut App) {
    with_active_host(cx, |host, window, cx| host.toggle_maximize(window, cx));
}

pub fn toggle_collapse_from_app(cx: &mut App) {
    with_active_host(cx, |host, _, cx| host.toggle_collapse(cx));
}

pub fn close_from_app(cx: &mut App) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    let _ = window.update(cx, |multi_workspace, window, cx| {
        multi_workspace.set_herdr_visible(false, cx);
        multi_workspace.focus_active_workspace(window, cx);
    });
}

pub fn status_from_app(cx: &mut App) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    let _ = window.update(cx, |multi_workspace, window, cx| {
        let window_id = window.window_handle().window_id();
        let state = HerdrSessionRegistry::try_global(cx)
            .map(|registry| registry.read(cx).binding_state(window_id))
            .unwrap_or(BindingState::Unselected);
        if matches!(state, BindingState::Unselected) {
            HerdrSessionRegistry::select_or_create_for_window(window_id, cx);
        } else if let Some(host) = host_from_multi_workspace(multi_workspace) {
            multi_workspace.set_herdr_visible(true, cx);
            host.update(cx, |host, cx| host.focus_handle.focus(window, cx));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{HerdRHost, HerdRVisibilityTransition, binding_label, toggle_visibility};
    use crate::zed::herdr_agent_sync::SessionIdentity;
    use crate::zed::herdr_session_registry::{BindingState, HerdrSessionRegistry};
    use gpui::TestAppContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn herdr_toggle_visibility() {
        assert_eq!(
            toggle_visibility(false, false),
            HerdRVisibilityTransition::ShowAndFocus
        );
        assert_eq!(
            toggle_visibility(true, false),
            HerdRVisibilityTransition::FocusOnly
        );
        assert_eq!(
            toggle_visibility(true, true),
            HerdRVisibilityTransition::HideAndRestoreEditor
        );
    }

    #[test]
    fn surfaces_use_approved_labels() {
        assert_eq!(
            binding_label(&BindingState::Unselected),
            "herdr: Select a session"
        );
        assert_eq!(
            binding_label(&BindingState::Starting {
                session_name: Arc::from("main")
            }),
            "herdr: Starting main…"
        );
        assert_eq!(
            binding_label(&BindingState::Connected(SessionIdentity {
                name: Arc::from("main"),
                session_dir: Arc::from(PathBuf::from("/sessions/main")),
            })),
            "herdr: Connected to main"
        );
        assert_eq!(
            binding_label(&BindingState::Failed {
                session_name: Arc::from("main"),
                message: "socket closed".into(),
            }),
            "herdr: Could not connect to main"
        );
    }

    #[gpui::test]
    async fn rendered_failed_surface_has_retry_and_choose_session(cx: &mut TestAppContext) {
        use crate::zed::herdr_session_registry::HerdrGateway;
        use futures::FutureExt as _;
        use gpui::{AppContext as _, WindowHandle, px, size};
        use project::DisableAiSettings;
        use settings::{Settings as _, SettingsStore};
        use std::path::Path;
        use workspace::MultiWorkspace;

        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            DisableAiSettings::register(cx);
        });
        let fs = fs::FakeFs::new(cx.executor());
        let project = project::Project::test(fs, [Path::new("/root")], cx).await;
        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
        let gateway = HerdrGateway::fake(
            || async { Ok(Vec::new()) }.boxed_local(),
            |_info| async { Err(anyhow::anyhow!("unused")) }.boxed_local(),
            |_name| async { Ok(()) }.boxed_local(),
        );
        let registry = cx.new(|cx| HerdrSessionRegistry::test(cx, gateway));
        let window_id =
            multi_workspace.update_in(cx, |_, window, _| window.window_handle().window_id());
        let host = multi_workspace.update_in(cx, |multi_workspace, window, cx| {
            let workspace = multi_workspace.workspace().clone();
            cx.new(|cx| {
                HerdRHost::new(
                    registry.clone(),
                    window_id,
                    workspace,
                    PathBuf::from("/root"),
                    None,
                    window,
                    cx,
                )
            })
        });
        let handle = WindowHandle::<MultiWorkspace>::new(window_id);
        registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                handle,
                BindingState::Failed {
                    session_name: Arc::from("main"),
                    message: "socket closed".into(),
                },
                Vec::new(),
            );
        });
        multi_workspace.update_in(cx, |multi_workspace, _, cx| {
            multi_workspace.set_window_root_host(Some(host.into()), cx);
            multi_workspace.set_herdr_visible(true, cx);
        });
        cx.simulate_resize(size(px(900.0), px(700.0)));
        cx.update(|window, cx| {
            window.refresh();
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();

        assert!(
            cx.debug_bounds("herdr-action-retry").is_some(),
            "Failed surface must render the Retry action"
        );
        assert!(
            cx.debug_bounds("herdr-action-choose-session").is_some(),
            "Failed surface must render the Choose Session action"
        );

        registry.update(cx, |registry, _| {
            registry.register_window_for_test(
                handle,
                BindingState::Starting {
                    session_name: Arc::from("main"),
                },
                Vec::new(),
            );
        });
        cx.run_until_parked();
        cx.update(|window, cx| {
            window.refresh();
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        assert!(
            cx.debug_bounds("herdr-action-retry").is_none(),
            "Starting surface must not render a Retry action"
        );
    }
}
