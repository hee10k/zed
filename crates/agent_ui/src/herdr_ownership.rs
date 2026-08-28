use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
};

use gpui::{
    App, AppContext as _, BorrowAppContext as _, Context, Entity, EntityId, Global, Subscription,
    WeakEntity, Window, WindowId,
};
use project::Event as ProjectEvent;
use terminal::{Event as TerminalEvent, ForegroundProcess, Terminal};
use workspace::{MultiWorkspace, MultiWorkspaceEvent, Workspace};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HerdrLaunch {
    pub session_name: String,
    pub server_mode: bool,
}

pub(crate) fn parse_server_launch(
    argv: &[String],
    expected_session: &str,
) -> Option<HerdrLaunch> {
    argv.first()?
        .rsplit(['/', '\\'])
        .next()
        .filter(|executable| matches!(*executable, "herdr" | "herdr.exe"))?;

    let launch = match argv {
        [_] => HerdrLaunch {
            session_name: expected_session.to_string(),
            server_mode: false,
        },
        [_, command] if command == "server" => HerdrLaunch {
            session_name: expected_session.to_string(),
            server_mode: true,
        },
        [_, flag, session] if flag == "--session" && session == expected_session => HerdrLaunch {
            session_name: session.clone(),
            server_mode: false,
        },
        [_, command, flag, session]
            if command == "server" && flag == "--session" && session == expected_session =>
        {
            HerdrLaunch {
                session_name: session.clone(),
                server_mode: true,
            }
        }
        _ => return None,
    };

    Some(launch)
}

type LaunchHandler =
    Rc<dyn Fn(WindowId, EntityId, ForegroundProcess, HerdrLaunch, &mut App) -> bool>;
type ProcessExitHandler = Rc<dyn Fn(WindowId, EntityId, Option<u32>, &mut App)>;
type WindowReleaseHandler = Rc<dyn Fn(WindowId, &mut App)>;

#[derive(Clone)]
struct HerdrOwnershipCallbacks {
    on_launch: Rc<RefCell<LaunchHandler>>,
    on_process_exit: Rc<RefCell<ProcessExitHandler>>,
    on_window_release: Rc<RefCell<WindowReleaseHandler>>,
}

impl Default for HerdrOwnershipCallbacks {
    fn default() -> Self {
        Self {
            on_launch: Rc::new(RefCell::new(Rc::new(|_, _, _, _, _| true))),
            on_process_exit: Rc::new(RefCell::new(Rc::new(|_, _, _, _| {}))),
            on_window_release: Rc::new(RefCell::new(Rc::new(|_, _| {}))),
        }
    }
}

pub(crate) struct HerdrOwnershipRegistry {
    observers: HashMap<WindowId, Entity<HerdrOwnershipObserver>>,
    callbacks: HerdrOwnershipCallbacks,
}

impl Default for HerdrOwnershipRegistry {
    fn default() -> Self {
        Self {
            observers: HashMap::default(),
            callbacks: HerdrOwnershipCallbacks::default(),
        }
    }
}

impl Global for HerdrOwnershipRegistry {}

impl HerdrOwnershipRegistry {
    pub(crate) fn init(cx: &mut App) {
        if !cx.has_global::<Self>() {
            cx.set_global(Self::default());
        }
    }

    /// Installs the Task 6 bridge integration without coupling this observer
    /// to bridge construction or registry internals.
    pub(crate) fn set_handlers(
        &mut self,
        on_launch: impl Fn(WindowId, EntityId, ForegroundProcess, HerdrLaunch, &mut App) -> bool
            + 'static,
        on_process_exit: impl Fn(WindowId, EntityId, Option<u32>, &mut App) + 'static,
        on_window_release: impl Fn(WindowId, &mut App) + 'static,
    ) {
        *self.callbacks.on_launch.borrow_mut() = Rc::new(on_launch);
        *self.callbacks.on_process_exit.borrow_mut() = Rc::new(on_process_exit);
        *self.callbacks.on_window_release.borrow_mut() = Rc::new(on_window_release);
    }
}

struct HerdrOwner {
    terminal_id: EntityId,
    process_id: Option<u32>,
    session_name: String,
}

struct TerminalSubscriptions {
    _events: Subscription,
    _release: Subscription,
}

struct HerdrOwnershipObserver {
    window_id: WindowId,
    session_name: String,
    callbacks: HerdrOwnershipCallbacks,
    workspace_projects: HashMap<EntityId, EntityId>,
    project_subscriptions: HashMap<EntityId, Subscription>,
    terminal_projects: HashMap<EntityId, EntityId>,
    terminal_entities: HashMap<EntityId, WeakEntity<Terminal>>,
    terminal_subscriptions: HashMap<EntityId, TerminalSubscriptions>,
    owner: Option<HerdrOwner>,
    /// Terminal IDs already notified about process teardown. This keeps
    /// foreground-clear, process-exit, and release callbacks idempotent while
    /// allowing a later accepted launch on the same terminal to re-arm it.
    process_exit_notifications: HashSet<EntityId>,
    _multi_workspace_subscription: Option<Subscription>,
}

impl HerdrOwnershipObserver {
    fn new(
        window_id: WindowId,
        session_name: String,
        callbacks: HerdrOwnershipCallbacks,
    ) -> Self {
        Self {
            window_id,
            session_name,
            callbacks,
            workspace_projects: HashMap::default(),
            project_subscriptions: HashMap::default(),
            terminal_projects: HashMap::default(),
            terminal_entities: HashMap::default(),
            terminal_subscriptions: HashMap::default(),
            owner: None,
            process_exit_notifications: HashSet::default(),
            _multi_workspace_subscription: None,
        }
    }

    fn start(
        &mut self,
        multi_workspace: Entity<MultiWorkspace>,
        workspaces: Vec<Entity<Workspace>>,
        window: &Window,
        cx: &mut Context<Self>,
    ) {
        self._multi_workspace_subscription = Some(cx.subscribe_in(
            &multi_workspace,
            window,
            |observer, multi_workspace, event: &MultiWorkspaceEvent, window, cx| match event {
                MultiWorkspaceEvent::WorkspaceAdded(workspace) => {
                    observer.register_workspace(workspace, window, cx);
                }
                MultiWorkspaceEvent::WorkspaceRemoved(workspace_id) => {
                    observer.remove_workspace(*workspace_id, cx);
                }
                MultiWorkspaceEvent::ActiveWorkspaceChanged { .. } => {
                    let workspaces = multi_workspace
                        .read(cx)
                        .workspaces()
                        .filter(|workspace| {
                            !observer.workspace_projects.contains_key(&workspace.entity_id())
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    for workspace in workspaces {
                        observer.register_workspace(&workspace, window, cx);
                    }
                }
                _ => {}
            },
        ));

        for workspace in workspaces {
            self.register_workspace(&workspace, window, cx);
        }
    }

    fn register_workspace(
        &mut self,
        workspace: &Entity<Workspace>,
        window: &Window,
        cx: &mut Context<Self>,
    ) {
        let workspace_id = workspace.entity_id();
        let project = workspace.read(cx).project().clone();
        let project_id = project.entity_id();
        self.workspace_projects.insert(workspace_id, project_id);

        if let std::collections::hash_map::Entry::Vacant(entry) =
            self.project_subscriptions.entry(project_id)
        {
            let project_for_callback = project.clone();
            let subscription = cx.subscribe_in(
                &project,
                window,
                move |observer, _project, event: &ProjectEvent, window, cx| {
                    if matches!(event, ProjectEvent::TerminalAdded) {
                        observer.register_project_terminals(&project_for_callback, window, cx);
                    }
                },
            );
            entry.insert(subscription);
        }

        self.register_project_terminals(&project, window, cx);
    }

    fn register_project_terminals(
        &mut self,
        project: &Entity<project::Project>,
        window: &Window,
        cx: &mut Context<Self>,
    ) {
        let terminals = project
            .read(cx)
            .local_terminal_handles()
            .iter()
            .filter_map(|terminal| terminal.upgrade())
            .collect::<Vec<_>>();

        for terminal in terminals {
            self.register_terminal(terminal, project.entity_id(), window, cx);
        }
    }

    fn register_terminal(
        &mut self,
        terminal: Entity<Terminal>,
        project_id: EntityId,
        window: &Window,
        cx: &mut Context<Self>,
    ) {
        let terminal_id = terminal.entity_id();
        if self.terminal_subscriptions.contains_key(&terminal_id) {
            return;
        }

        let should_observe = {
            let terminal = terminal.read(cx);
            should_observe_terminal(terminal.is_remote(), terminal.is_pty())
        };
        if !should_observe {
            return;
        }
        self.terminal_entities
            .insert(terminal_id, terminal.downgrade());
        let foreground_process = terminal.read(cx).foreground_process();

        let events = cx.subscribe_in(
            &terminal,
            window,
            move |observer, terminal, event: &TerminalEvent, _window, cx| {
                observer.handle_terminal_event(terminal, event, cx);
            },
        );
        let release = cx.observe_release_in(
            &terminal,
            window,
            move |observer, _terminal, _window, cx| {
                if observer.release_owner_or_notify_terminal(terminal_id, cx) {
                    observer.rescan_foreground_processes(Some(terminal_id), cx);
                }
                observer.terminal_subscriptions.remove(&terminal_id);
                observer.terminal_projects.remove(&terminal_id);
                observer.terminal_entities.remove(&terminal_id);
            },
        );

        self.terminal_projects.insert(terminal_id, project_id);
        self.terminal_subscriptions.insert(
            terminal_id,
            TerminalSubscriptions {
                _events: events,
                _release: release,
            },
        );
        if let Some(process) = foreground_process.as_ref() {
            self.handle_foreground_process(terminal_id, process, cx);
        }
    }

    fn remove_workspace(&mut self, workspace_id: EntityId, cx: &mut App) {
        let Some(project_id) = self.workspace_projects.remove(&workspace_id) else {
            return;
        };
        if self.workspace_projects.values().any(|id| *id == project_id) {
            return;
        }

        self.project_subscriptions.remove(&project_id);
        let terminal_ids = self
            .terminal_projects
            .iter()
            .filter_map(|(terminal_id, id)| (*id == project_id).then_some(*terminal_id))
            .collect::<Vec<_>>();
        let mut process_exit_notified = false;
        for terminal_id in terminal_ids {
            process_exit_notified |= self.release_owner_or_notify_terminal(terminal_id, cx);
            self.terminal_entities.remove(&terminal_id);
            self.terminal_projects.remove(&terminal_id);
            self.terminal_subscriptions.remove(&terminal_id);
        }
        if process_exit_notified {
            self.rescan_foreground_processes(None, cx);
        }
    }

    fn handle_terminal_event(
        &mut self,
        terminal: &Entity<Terminal>,
        event: &TerminalEvent,
        cx: &mut App,
    ) {
        match event {
            TerminalEvent::ForegroundProcessChanged(Some(process)) => {
                self.handle_foreground_process(terminal.entity_id(), process, cx);
            }
            TerminalEvent::ForegroundProcessChanged(None)
            | TerminalEvent::ProcessExited => {
                let terminal_id = terminal.entity_id();
                if self.release_owner_or_notify_terminal(terminal_id, cx) {
                    self.rescan_foreground_processes(Some(terminal_id), cx);
                }
            }
            _ => {}
        }
    }

    fn handle_foreground_process(
        &mut self,
        terminal_id: EntityId,
        process: &ForegroundProcess,
        cx: &mut App,
    ) {
        let owner_process_changed = self.owner.as_ref().is_some_and(|owner| {
            owner.terminal_id == terminal_id
                && owner
                    .process_id
                    .map_or(true, |process_id| process.pid != Some(process_id))
        });
        if owner_process_changed {
            self.release_owner_and_rescan(terminal_id, cx);
        }

        let Some(launch) = parse_server_launch(&process.argv, &self.session_name) else {
            return;
        };
        let owner = HerdrOwner {
            terminal_id,
            process_id: process.pid,
            session_name: launch.session_name.clone(),
        };
        let is_duplicate = self.owner.as_ref().is_some_and(|current| {
            current.terminal_id == owner.terminal_id
                && current.process_id == owner.process_id
                && current.session_name == owner.session_name
        });
        if is_duplicate {
            return;
        }

        let on_launch = self.callbacks.on_launch.borrow().clone();
        let accepted = on_launch(
            self.window_id,
            terminal_id,
            process.clone(),
            launch,
            cx,
        );
        if accepted {
            self.owner = Some(owner);
            self.process_exit_notifications.remove(&terminal_id);
        }
    }

    fn release_owner_for_terminal(&mut self, terminal_id: EntityId, cx: &mut App) -> bool {
        let Some(owner) = self.owner.take() else {
            return false;
        };
        if owner.terminal_id != terminal_id {
            self.owner = Some(owner);
            return false;
        }
        self.process_exit_notifications.insert(terminal_id);

        let on_process_exit = self.callbacks.on_process_exit.borrow().clone();
        on_process_exit(
            self.window_id,
            owner.terminal_id,
            owner.process_id,
            cx,
        );
        true
    }
    fn release_owner_or_notify_terminal(
        &mut self,
        terminal_id: EntityId,
        cx: &mut App,
    ) -> bool {
        if !self.process_exit_notifications.insert(terminal_id) {
            return false;
        }
        let process_id = self
            .owner
            .as_ref()
            .filter(|owner| owner.terminal_id == terminal_id)
            .map(|owner| owner.process_id)
            .unwrap_or(None);
        let released = self.release_owner_for_terminal(terminal_id, cx);
        if !released {
            let on_process_exit = self.callbacks.on_process_exit.borrow().clone();
            on_process_exit(self.window_id, terminal_id, process_id, cx);
        }
        true
    }

    fn release_owner_and_rescan(&mut self, terminal_id: EntityId, cx: &mut App) {
        if self.release_owner_or_notify_terminal(terminal_id, cx) {
            self.rescan_foreground_processes(Some(terminal_id), cx);
        }
    }

    fn rescan_foreground_processes(
        &mut self,
        excluded_terminal_id: Option<EntityId>,
        cx: &mut App,
    ) {
        let terminals = self
            .terminal_entities
            .iter()
            .filter(|(terminal_id, _)| Some(*terminal_id) != excluded_terminal_id)
            .filter_map(|(terminal_id, terminal)| {
                terminal.upgrade().map(|terminal| (*terminal_id, terminal))
            })
            .collect::<Vec<_>>();

        for (terminal_id, terminal) in terminals {
            let process = terminal.read(cx).foreground_process();
            if let Some(process) = process.as_ref() {
                self.handle_foreground_process(terminal_id, process, cx);
            }
        }
    }
}

fn should_observe_terminal(is_remote: bool, is_pty: bool) -> bool {
    !is_remote && is_pty
}

pub(crate) fn init(cx: &mut App) {
    HerdrOwnershipRegistry::init(cx);
    cx.observe_new(|multi_workspace: &mut MultiWorkspace, window, cx| {
        let Some(window) = window else {
            return;
        };

        let window_id = window.window_handle().window_id();
        let session_name = multi_workspace.reserve_herdr_session_name(cx);
        terminal::set_herdr_session_for_window(window_id.as_u64(), session_name.clone(), cx);
        let workspaces = multi_workspace.workspaces().cloned().collect::<Vec<_>>();
        let multi_workspace = cx.entity();
        let callbacks = cx.global::<HerdrOwnershipRegistry>().callbacks.clone();
        let observer = cx.new(|_| {
            HerdrOwnershipObserver::new(window_id, session_name, callbacks.clone())
        });

        cx.update_global::<HerdrOwnershipRegistry, _>(|registry, _cx| {
            registry.observers.insert(window_id, observer.clone());
        });

        observer.update(cx, |observer, observer_cx| {
            observer.start(multi_workspace.clone(), workspaces, window, observer_cx);
        });

        cx.on_release(move |_multi_workspace, cx| {
            let on_window_release = callbacks.on_window_release.borrow().clone();
            on_window_release(window_id, cx);
            terminal::clear_herdr_session_for_window(window_id.as_u64(), cx);
            if cx.try_global::<HerdrOwnershipRegistry>().is_some() {
                cx.update_global::<HerdrOwnershipRegistry, _>(|registry, _cx| {
                    registry.observers.remove(&window_id);
                });
            }
        })
        .detach();
    })
    .detach();
}


#[cfg(test)]
mod tests {
    use gpui::TestAppContext;
    use std::{cell::{Cell, RefCell}, path::PathBuf, rc::Rc};

    fn process(argv: &[&str], pid: u32) -> ForegroundProcess {
        ForegroundProcess {
            name: "herdr".to_string(),
            cwd: PathBuf::from("/project"),
            argv: argv.iter().map(|arg| (*arg).to_string()).collect(),
            pid: Some(pid),
        }
    }

    fn terminal_entity(cx: &mut TestAppContext) -> gpui::Entity<Terminal> {
        cx.new(|cx| {
            terminal::TerminalBuilder::new_display_only(
                terminal::terminal_settings::CursorShape::default(),
                terminal::terminal_settings::AlternateScroll::On,
                None,
                0,
                cx.background_executor(),
                util::paths::PathStyle::local(),
            )
            .subscribe(cx)
        })
    }

    fn recording_observer(
    ) -> (
        HerdrOwnershipObserver,
        Rc<RefCell<Vec<(EntityId, Vec<String>, HerdrLaunch)>>>,
        Rc<RefCell<Vec<(EntityId, Option<u32>)>>>,
    ) {
        let launches = Rc::new(RefCell::new(Vec::new()));
        let exits = Rc::new(RefCell::new(Vec::new()));
        let callbacks = HerdrOwnershipCallbacks::default();

        let launches_ref = launches.clone();
        *callbacks.on_launch.borrow_mut() = Rc::new(
            move |_window_id, terminal_id, process, launch, _cx| {
                launches_ref
                    .borrow_mut()
                    .push((terminal_id, process.argv, launch));
                true
            },
        );
        let exits_ref = exits.clone();
        *callbacks.on_process_exit.borrow_mut() =
            Rc::new(move |_window_id, terminal_id, process_id, _cx| {
                exits_ref.borrow_mut().push((terminal_id, process_id));
            });

        (
            HerdrOwnershipObserver::new(WindowId::from(1), "zed-x".to_string(), callbacks),
            launches,
            exits,
        )
    }

    #[gpui::test]
    fn forwards_local_terminal_panel_and_agent_panel_launches(cx: &mut TestAppContext) {
        let (mut observer, launches, _exits) = recording_observer();
        let terminal_panel_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let agent_panel_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(
                terminal_panel_terminal,
                &process(&["herdr"], 11),
                cx,
            );
            observer.handle_foreground_process(
                agent_panel_terminal,
                &process(&["herdr", "server"], 12),
                cx,
            );
        });

        let launches = launches.borrow();
        assert_eq!(launches.len(), 2);
        assert_eq!(launches[0].0, terminal_panel_terminal);
        assert_eq!(launches[0].1, vec!["herdr".to_string()]);
        assert!(!launches[0].2.server_mode);
        assert_eq!(launches[1].0, agent_panel_terminal);
        assert_eq!(
            launches[1].1,
            vec!["herdr".to_string(), "server".to_string()]
        );
        assert!(launches[1].2.server_mode);
    }

    #[test]
    fn ignores_remote_and_display_only_terminals() {
        assert!(should_observe_terminal(false, true));
        assert!(!should_observe_terminal(true, true));
        assert!(!should_observe_terminal(false, false));
        assert!(!should_observe_terminal(true, false));
    }

    #[gpui::test]
    fn ignores_client_commands_and_mismatched_sessions(cx: &mut TestAppContext) {
        let (mut observer, launches, _exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();
        let rejected = [
            vec!["herdr", "status"],
            vec!["herdr", "--session", "other"],
            vec!["herdr", "server", "--session", "other"],
        ];

        cx.update(|cx| {
            for argv in &rejected {
                observer.handle_foreground_process(terminal_id, &process(argv, 11), cx);
            }
        });

        assert!(launches.borrow().is_empty());
    }

    #[gpui::test]
    fn releases_only_the_current_owner_once_after_terminal_replacement(
        cx: &mut TestAppContext,
    ) {
        let (mut observer, _launches, exits) = recording_observer();
        let old_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let new_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(old_terminal, &process(&["herdr"], 11), cx);
            observer.handle_foreground_process(new_terminal, &process(&["herdr"], 12), cx);
            observer.release_owner_for_terminal(old_terminal, cx);
            observer.release_owner_for_terminal(new_terminal, cx);
            observer.release_owner_for_terminal(new_terminal, cx);
        });

        assert_eq!(&*exits.borrow(), &[(new_terminal, Some(12))]);
    }

    #[gpui::test]
    fn foreground_process_clear_releases_owner_once(cx: &mut TestAppContext) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(terminal_id, &process(&["herdr"], 11), cx);
            observer.release_owner_for_terminal(terminal_id, cx);
            observer.release_owner_for_terminal(terminal_id, cx);
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, Some(11))]);
    }

    #[gpui::test]
    fn foreground_change_away_from_owner_releases_owner_once(cx: &mut TestAppContext) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(terminal_id, &process(&["herdr"], 11), cx);
            observer.handle_foreground_process(terminal_id, &process(&["shell"], 12), cx);
            observer.release_owner_for_terminal(terminal_id, cx);
            observer.release_owner_for_terminal(terminal_id, cx);
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, Some(11))]);
    }

    #[gpui::test]
    fn rejected_launch_preserves_existing_owner(cx: &mut TestAppContext) {
        let (mut observer, _launches, exits) = recording_observer();
        let rejection_enabled = Rc::new(Cell::new(false));
        let rejection_enabled_ref = rejection_enabled.clone();
        *observer.callbacks.on_launch.borrow_mut() =
            Rc::new(move |_window_id, _terminal_id, _process, _launch, _cx| {
                !rejection_enabled_ref.get()
            });
        let first_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let second_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(first_terminal, &process(&["herdr"], 11), cx);
            rejection_enabled.set(true);
            observer.handle_foreground_process(second_terminal, &process(&["herdr"], 12), cx);
            observer.release_owner_for_terminal(second_terminal, cx);
            observer.release_owner_for_terminal(first_terminal, cx);
        });

        assert_eq!(&*exits.borrow(), &[(first_terminal, Some(11))]);
    }


    #[gpui::test]
    fn terminal_release_releases_owner_once(cx: &mut TestAppContext) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(terminal_id, &process(&["herdr"], 11), cx);
            observer.release_owner_for_terminal(terminal_id, cx);
            observer.release_owner_for_terminal(terminal_id, cx);
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, Some(11))]);
    }
    #[gpui::test]
    fn terminal_release_notifies_pending_owner_without_observer_owner(
        cx: &mut TestAppContext,
    ) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            assert!(observer.release_owner_or_notify_terminal(terminal_id, cx));
            assert!(!observer.release_owner_or_notify_terminal(terminal_id, cx));
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, None)]);
    }
    #[gpui::test]
    fn foreground_clear_notifies_pending_owner_while_terminal_lives(
        cx: &mut TestAppContext,
    ) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal = terminal_entity(cx);
        let terminal_id = terminal.entity_id();

        cx.update(|cx| {
            observer.handle_terminal_event(
                &terminal,
                &TerminalEvent::ForegroundProcessChanged(None),
                cx,
            );
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, None)]);
    }

    #[gpui::test]
    fn process_exit_notifies_pending_owner_without_double_release(
        cx: &mut TestAppContext,
    ) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal = terminal_entity(cx);
        let terminal_id = terminal.entity_id();

        cx.update(|cx| {
            observer.handle_terminal_event(&terminal, &TerminalEvent::ProcessExited, cx);
            observer.handle_terminal_event(&terminal, &TerminalEvent::ProcessExited, cx);
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, None)]);
    }



    #[gpui::test]
    fn workspace_removal_releases_owned_terminal_once(cx: &mut TestAppContext) {
        let (mut observer, _launches, exits) = recording_observer();
        let terminal_id = cx.update(|cx| cx.new(|_| ())).entity_id();
        let workspace_id = cx.update(|cx| cx.new(|_| ())).entity_id();
        let project_id = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(terminal_id, &process(&["herdr"], 11), cx);
            observer.workspace_projects.insert(workspace_id, project_id);
            observer.terminal_projects.insert(terminal_id, project_id);
            observer.remove_workspace(workspace_id, cx);
            observer.release_owner_for_terminal(terminal_id, cx);
        });

        assert_eq!(&*exits.borrow(), &[(terminal_id, Some(11))]);
    }

    #[gpui::test]
    fn replacement_retries_rejected_launch_after_owner_release(cx: &mut TestAppContext) {
        let (mut observer, launches, exits) = recording_observer();
        let rejection_enabled = Rc::new(Cell::new(false));
        let rejection_enabled_ref = rejection_enabled.clone();
        let launches_ref = launches.clone();
        *observer.callbacks.on_launch.borrow_mut() =
            Rc::new(move |_window_id, terminal_id, process, launch, _cx| {
                launches_ref
                    .borrow_mut()
                    .push((terminal_id, process.argv, launch));
                !rejection_enabled_ref.get()
            });
        let first_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let second_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();

        cx.update(|cx| {
            observer.handle_foreground_process(first_terminal, &process(&["herdr"], 11), cx);
            rejection_enabled.set(true);
            observer.handle_foreground_process(second_terminal, &process(&["herdr"], 12), cx);
            observer.release_owner_for_terminal(first_terminal, cx);
            rejection_enabled.set(false);
            observer.handle_foreground_process(second_terminal, &process(&["herdr"], 12), cx);
            observer.release_owner_for_terminal(second_terminal, cx);
        });

        let launches = launches.borrow();
        assert_eq!(
            launches.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            vec![first_terminal, second_terminal, second_terminal]
        );
        assert_eq!(
            &*exits.borrow(),
            &[(first_terminal, Some(11)), (second_terminal, Some(12))]
        );
    }

    #[test]
    fn accepts_bare_herdr_for_reserved_session() {
        assert_eq!(
            parse_server_launch(&["herdr".into()], "zed-x"),
            Some(HerdrLaunch {
                session_name: "zed-x".into(),
                server_mode: false,
            })
        );
    }

    #[test]
    fn accepts_expected_named_server_and_rejects_other_commands() {
        assert!(parse_server_launch(&["herdr".into(), "server".into()], "zed-x").is_some());
        assert!(parse_server_launch(
            &["herdr".into(), "--session".into(), "zed-x".into()],
            "zed-x"
        )
        .is_some());
        assert!(parse_server_launch(
            &[
                "herdr".into(),
                "server".into(),
                "--session".into(),
                "zed-x".into(),
            ],
            "zed-x"
        )
        .is_some());
        assert!(parse_server_launch(
            &["herdr".into(), "--session".into(), "other".into()],
            "zed-x"
        )
        .is_none());
        assert!(parse_server_launch(&["herdr".into(), "status".into()], "zed-x").is_none());
    }

    #[test]
    fn rejects_malformed_or_extra_session_arguments() {
        let expected = "zed-x";
        for argv in [
            vec!["herdr".into(), "--session".into()],
            vec!["herdr".into(), "server".into(), "--session".into()],
            vec![
                "herdr".into(),
                "--session".into(),
                expected.into(),
                "extra".into(),
            ],
            vec!["herdr".into(), "server".into(), "extra".into()],
            vec![
                "herdr".into(),
                "server".into(),
                "--session".into(),
                "other".into(),
            ],
        ] {
            assert!(parse_server_launch(&argv, expected).is_none(), "unexpected acceptance: {argv:?}");
        }
    }

    #[test]
    fn accepts_executable_basename_after_path_normalization() {
        assert!(parse_server_launch(
            &["/usr/local/bin/herdr".into()],
            "zed-x"
        )
        .is_some());
        assert!(parse_server_launch(&[r"C:\tools\herdr.exe".into()], "zed-x").is_some());
    }

    #[test]
    fn rejects_empty_unknown_and_wrapped_commands() {
        let expected = "zed-x";
        assert!(parse_server_launch(&[], expected).is_none());
        assert!(parse_server_launch(&["other".into()], expected).is_none());
        assert!(parse_server_launch(&["herdr".into(), "unknown".into()], expected).is_none());
        assert!(parse_server_launch(
            &["env".into(), "herdr".into(), "server".into()],
            expected
        )
        .is_none());
        assert!(parse_server_launch(
            &["sh".into(), "-c".into(), "herdr server".into()],
            expected
        )
        .is_none());
    }

    #[test]
    fn rejects_client_subcommands() {
        for command in [
            "status",
            "workspace",
            "tab",
            "pane",
            "agent",
            "notification",
            "session",
            "api",
            "update",
            "completion",
        ] {
            assert!(
                parse_server_launch(&["herdr".into(), command.into()], "zed-x").is_none(),
                "{command} must not be treated as a server launch"
            );
        }
    }

    #[gpui::test]
    async fn in_window_launches_share_one_bridge_and_client_paths_do_not_activate(
        cx: &mut TestAppContext,
    ) {
        use herdr_bridge::{HerdrBridgeRegistry, HerdrSessionSelection};

        cx.update(|cx| {
            HerdrBridgeRegistry::init(cx);
            HerdrOwnershipRegistry::init(cx);
            cx.update_global::<HerdrOwnershipRegistry, _>(|registry, cx| {
                registry.set_handlers(
                    |window_id, terminal_id, process, launch, cx| {
                        let session_name = launch.session_name.clone();
                        let owner = crate::herdr_bridge::HerdrOwnerProcess {
                            terminal_id,
                            process_id: process.pid,
                            session_name: session_name.clone(),
                        };
                        cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                            registry.activate_window(
                                window_id,
                                HerdrSessionSelection::Named(session_name),
                                owner,
                                cx,
                            )
                        })
                        .is_ok()
                    },
                    |_window_id, _terminal_id, _process_id, _cx| {},
                    |_window_id, _cx| {},
                );
            });
        });

        let window_id = WindowId::from(6);
        let callbacks = cx.update(|cx| {
            cx.global::<HerdrOwnershipRegistry>().callbacks.clone()
        });
        let mut observer =
            HerdrOwnershipObserver::new(window_id, "zed-x".to_string(), callbacks);
        let standard_local_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let agent_panel_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let status_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let mismatch_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();
        let plain_terminal = cx.update(|cx| cx.new(|_| ())).entity_id();

        let bridge = |cx: &mut TestAppContext| {
            cx.update(|_window, cx| {
                cx.global::<HerdrBridgeRegistry>()
                    .bridge_for_window(window_id, cx)
            })
        };

        // A standard local terminal launch activates the per-window bridge.
        cx.update(|cx| {
            observer.handle_foreground_process(
                standard_local_terminal,
                &process(&["herdr"], 10),
                cx,
            );
        });
        let bridge_after_standard = bridge(cx).expect("a standard terminal should activate it");
        assert_eq!(
            bridge_after_standard.read_with(cx, |bridge, _| {
                bridge.owner().map(|owner| owner.terminal_id)
            }),
            Some(standard_local_terminal)
        );

        // An AgentPanel terminal launching the same reserved session cannot
        // replace the owner: both terminals share the SAME per-window bridge
        // and the first process remains its owner.
        cx.update(|cx| {
            observer.handle_foreground_process(
                agent_panel_terminal,
                &process(&["herdr", "--session", "zed-x"], 11),
                cx,
            );
        });
        let bridge_after_panel = bridge(cx).expect("the window bridge must persist");
        assert_eq!(
            bridge_after_panel.entity_id(),
            bridge_after_standard.entity_id(),
            "both in-window terminals must activate the same per-window bridge"
        );
        assert_eq!(
            bridge_after_panel.read_with(cx, |bridge, _| {
                bridge.owner().map(|owner| owner.terminal_id)
            }),
            Some(standard_local_terminal),
            "the first terminal must remain the owner after a second launch"
        );

        // Client commands, mismatched sessions, and non-Herdr processes must
        // never activate or replace the window bridge.
        cx.update(|cx| {
            observer.handle_foreground_process(status_terminal, &process(&["herdr", "status"], 12), cx);
            observer.handle_foreground_process(
                mismatch_terminal,
                &process(&["herdr", "--session", "other"], 13),
                cx,
            );
            observer.handle_foreground_process(plain_terminal, &process(&["/bin/sh"], 14), cx);
        });
        let bridge_after_rejects = bridge(cx).expect("rejected launches must keep the bridge");
        assert_eq!(
            bridge_after_rejects.entity_id(),
            bridge_after_standard.entity_id(),
            "client/mismatched/external processes must not replace the window bridge"
        );
        assert_eq!(
            bridge_after_rejects.read_with(cx, |bridge, _| {
                bridge.owner().map(|owner| owner.terminal_id)
            }),
            Some(standard_local_terminal)
        );

        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_window(window_id, cx);
            });
        });
    }
}
