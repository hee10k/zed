use std::path::PathBuf;

use super::*;
use crate::item::test::TestItem;
use agent_settings::AgentSettings;
use client::proto;
use fs::{FakeFs, Fs};
use gpui::{
    App, AppContext, Context, EventEmitter, FocusHandle, Focusable, IntoElement, Pixels, Render,
    TestAppContext, VisualTestContext, Window, div, px,
    size,
};
use project::DisableAiSettings;
use serde_json::json;
use settings::{Settings, SettingsStore};
use util::path;

fn init_test(cx: &mut TestAppContext) {
    cx.update(|cx| {
        let settings_store = SettingsStore::test(cx);
        cx.set_global(settings_store);
        theme_settings::init(theme::LoadThemes::JustBase, cx);
        DisableAiSettings::register(cx);
    });
}

struct TestWindowRootHost;

impl Render for TestWindowRootHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("test-window-root-host")
            .debug_selector(|| "test-window-root-host".to_owned())
            .h(px(100.0))
    }
}

struct TestHerdrCentralHost {
    collapsed: bool,
}

impl Render for TestHerdrCentralHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .debug_selector(|| "herdr-central-host".to_owned())
            .flex()
            .flex_col()
            .when(!self.collapsed, |this| this.flex_1())
            .when(self.collapsed, |this| this.h(px(32.0)))
            .min_h_0()
            .w_full()
    }
}

struct TestSidebar {
    focus_handle: FocusHandle,
}

impl TestSidebar {
    fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus_handle: cx.focus_handle(),
        }
    }
}

impl EventEmitter<SidebarEvent> for TestSidebar {}

impl Focusable for TestSidebar {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Sidebar for TestSidebar {
    fn width(&self, _cx: &gpui::App) -> Pixels {
        px(300.0)
    }

    fn set_width(&mut self, _width: Option<Pixels>, _cx: &mut Context<Self>) {}

    fn has_notifications(&self, _cx: &gpui::App) -> bool {
        false
    }

    fn side(&self, cx: &gpui::App) -> SidebarSide {
        AgentSettings::get_global(cx).sidebar_side()
    }
}

impl Render for TestSidebar {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div().size_full().debug_selector(|| "test-sidebar".to_owned())
    }
}

struct TestWideHerdrCentralHost;

impl Render for TestWideHerdrCentralHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .debug_selector(|| "wide-herdr-central-host".to_owned())
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .min_w(px(800.0))
            .w(px(800.0))
            .child(div().w(px(800.0)).h(px(32.0)))
    }
}

struct ProbeDockPanel {
    position: DockPosition,
    focus_handle: FocusHandle,
}

actions!(test_only, [ToggleProbeDockPanel]);

impl ProbeDockPanel {
    fn new(position: DockPosition, cx: &mut App) -> Self {
        Self {
            position,
            focus_handle: cx.focus_handle(),
        }
    }
}

impl EventEmitter<crate::dock::PanelEvent> for ProbeDockPanel {}

impl Focusable for ProbeDockPanel {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for ProbeDockPanel {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let selector = match self.position {
            DockPosition::Left => "probe-left-dock-panel",
            DockPosition::Right => "probe-right-dock-panel",
            DockPosition::Bottom => "probe-bottom-dock-panel",
        };
        div()
            .id(selector)
            .debug_selector(|| selector.to_owned())
            .track_focus(&self.focus_handle(cx))
            .size_full()
    }
}

impl Panel for ProbeDockPanel {
    fn persistent_name() -> &'static str {
        "ProbeDockPanel"
    }

    fn panel_key() -> &'static str {
        "ProbeDockPanel"
    }

    fn position(&self, _window: &Window, _cx: &App) -> DockPosition {
        self.position
    }

    fn position_is_valid(&self, _position: DockPosition) -> bool {
        true
    }

    fn set_position(&mut self, position: DockPosition, _window: &mut Window, _cx: &mut Context<Self>) {
        self.position = position;
    }

    fn default_size(&self, _window: &Window, _cx: &App) -> Pixels {
        px(100.0)
    }

    fn icon(&self, _window: &Window, _cx: &App) -> Option<ui::IconName> {
        None
    }

    fn icon_tooltip(&self, _window: &Window, _cx: &App) -> Option<&'static str> {
        None
    }

    fn toggle_action(&self) -> Box<dyn gpui::Action> {
        ToggleProbeDockPanel.boxed_clone()
    }
    fn activation_priority(&self) -> u32 {
        match self.position {
            DockPosition::Left => 10,
            DockPosition::Right => 20,
            DockPosition::Bottom => 30,
        }
    }
}

#[gpui::test]
async fn test_herdr_central_view_preserves_workspace_docks(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;
    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    let (left, right, bottom, host) = cx.update(|_, cx| {
        (
            cx.new(|cx| ProbeDockPanel::new(DockPosition::Left, cx)),
            cx.new(|cx| ProbeDockPanel::new(DockPosition::Right, cx)),
            cx.new(|cx| ProbeDockPanel::new(DockPosition::Bottom, cx)),
            cx.new(|_| TestHerdrCentralHost { collapsed: false }),
        )
    });
    let editor_id = multi_workspace.read_with(cx, |multi_workspace, _| {
        multi_workspace.workspace().entity_id()
    });
    let editor = multi_workspace.read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone());
    editor.update_in(cx, |workspace, window, cx| {
        workspace.add_panel(left.clone(), window, cx);
        workspace.add_panel(right.clone(), window, cx);
        workspace.add_panel(bottom.clone(), window, cx);
        for position in [DockPosition::Left, DockPosition::Right, DockPosition::Bottom] {
            let dock = workspace.dock_at_position(position).clone();
            dock.update(cx, |dock, cx| {
                dock.activate_panel(0, window, cx);
                dock.set_open(true, window, cx);
            });
        }
    });
    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_window_root_host(Some(host.clone().into()), cx);
    });
    cx.run_until_parked();
    let ids = (left.entity_id(), right.entity_id(), bottom.entity_id(), host.entity_id());
    cx.simulate_resize(size(px(900.0), px(700.0)));
    for visible in [false, true] {
        multi_workspace.update(cx, |multi_workspace, cx| {
            multi_workspace.set_herdr_visible(visible, cx)
        });
        let window = cx.windows().into_iter().next().expect("test window");
        cx.update_window(window, |_, window, cx| {
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        for selector in [
            "probe-left-dock-panel",
            "probe-right-dock-panel",
            "probe-bottom-dock-panel",
        ] {
            let bounds = cx.debug_bounds(selector).expect("dock probe should render");
            assert!(bounds.size.width > px(0.0) && bounds.size.height > px(0.0));
        }
        if visible {
            let host_bounds = cx.debug_bounds("herdr-central-host").expect("HerdR host");
            let central_bounds = cx.debug_bounds("herdr-central-content").expect("HerdR center");
            assert_eq!(host_bounds.origin, central_bounds.origin);
            assert_eq!(host_bounds.size, central_bounds.size);
        }
    }
    assert_eq!(editor_id, multi_workspace.read_with(cx, |multi_workspace, _| multi_workspace.workspace().entity_id()));
    assert_eq!(ids, (left.entity_id(), right.entity_id(), bottom.entity_id(), host.entity_id()));
}


#[gpui::test]
async fn test_right_sidebar_stays_inside_narrow_viewport(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let window = cx.open_window(size(px(400.0), px(700.0)), |window, cx| {
        let mut multi_workspace = MultiWorkspace::test_new(project, window, cx);
        let sidebar = cx.new(TestSidebar::new);
        multi_workspace.register_sidebar(sidebar, cx);
        multi_workspace
    });
    cx.run_until_parked();
    let (multi_workspace, cx) = {
        let view = window.root(cx).unwrap();
        (
            view,
            VisualTestContext::from_window(window.into(), cx).into_mut(),
        )
    };
    let host = cx.update(|_, cx| cx.new(|_| TestWideHerdrCentralHost));
    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_window_root_host(Some(host.into()), cx);
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.update(|_, cx| {
        let mut settings = AgentSettings::get_global(cx).clone();
        settings.enabled = true;
        settings.sidebar_side = settings::SidebarDockPosition::Right;
        AgentSettings::override_global(settings, cx);
    });
    cx.run_until_parked();
    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.open_sidebar(cx);
    });
    cx.run_until_parked();
    let viewport = size(px(400.0), px(700.0));
    cx.simulate_resize(viewport);
    cx.update_window(window.into(), |_, window, cx| {
        window.draw(cx).clear(cx);
    });
    cx.run_until_parked();
    let central_bounds = cx
        .debug_bounds("herdr-central-content")
        .expect("the HerdR central content should be rendered");
    let sidebar_bounds = cx
        .debug_bounds("sidebar-container")
        .expect("the right sidebar should be rendered");
    let content_bounds = cx
        .debug_bounds("test-sidebar")
        .expect("the right sidebar content should be rendered");
    assert!(content_bounds.size.width > px(0.0) && content_bounds.size.height > px(0.0));
    assert!(
        content_bounds.origin.x >= sidebar_bounds.origin.x
            && content_bounds.bottom_right().x <= sidebar_bounds.bottom_right().x
            && content_bounds.origin.y >= sidebar_bounds.origin.y
            && content_bounds.bottom_right().y <= sidebar_bounds.bottom_right().y
            && content_bounds.bottom_right().x <= viewport.width,
        "the sidebar content should stay inside its container and viewport: {content_bounds:?} vs {sidebar_bounds:?} / {viewport:?}"
    );
    assert!(sidebar_bounds.size.width > px(0.0));
    assert!(
        central_bounds.size.width <= viewport.width - sidebar_bounds.size.width,
        "central content should fit beside the right sidebar: {central_bounds:?} vs {sidebar_bounds:?}"
    );
}

struct StackedWindowRootHost;

impl Render for StackedWindowRootHost {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("stacked-window-root-host")
            .debug_selector(|| "stacked-window-root-host".to_owned())
            .flex()
            .flex_col()
            .h(px(320.0))
            .child(
                div()
                    .id("stacked-window-root-host-header")
                    .debug_selector(|| "stacked-window-root-host-header".to_owned())
                    .h(px(32.0))
                    .flex_shrink_0(),
            )
            .child(
                div()
                    .id("stacked-window-root-host-terminal")
                    .debug_selector(|| "stacked-window-root-host-terminal".to_owned())
                    .flex_1()
                    .min_h_0()
                    .w_full(),
            )
    }
}

#[gpui::test]
async fn test_stacked_window_root_host_allocates_terminal_space(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    multi_workspace.update(cx, |multi_workspace, cx| {
        let host = cx.new(|_| StackedWindowRootHost);
        multi_workspace.set_window_root_host(Some(host.into()), cx);
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.simulate_resize(size(px(900.0), px(700.0)));
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let header_bounds = cx
        .debug_bounds("stacked-window-root-host-header")
        .expect("stacked host header should be rendered");
    let terminal_bounds = cx
        .debug_bounds("stacked-window-root-host-terminal")
        .expect("stacked host terminal should be rendered");
    assert_eq!(header_bounds.size.height, px(32.0));
    assert_eq!(terminal_bounds.size.height, px(288.0));
}

#[gpui::test]
async fn test_window_root_host_is_laid_out_inside_window(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    multi_workspace.update(cx, |multi_workspace, cx| {
        let host = cx.new(|_| TestWindowRootHost);
        multi_workspace.set_window_root_host(Some(host.into()), cx);
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.simulate_resize(size(px(900.0), px(700.0)));
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let host_bounds = cx.debug_bounds("test-window-root-host");
    let workspace_bounds = cx.debug_bounds("workspace");
    assert!(
        host_bounds.is_some(),
        "window root host should be rendered; workspace={workspace_bounds:?}"
    );
    let Some(host_bounds) = host_bounds else {
        return;
    };
    let viewport_size = cx.update(|window, _| window.viewport_size());
    assert_eq!(host_bounds.size.height, px(100.0));
    assert!(
        host_bounds.bottom_left().y <= viewport_size.height,
        "window root host should remain inside the viewport: {host_bounds:?} vs {viewport_size:?}"
    );
}

#[gpui::test]
async fn test_herdr_central_view_visibility(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    let (editor_id, host) = multi_workspace.update(cx, |multi_workspace, cx| {
        let editor_id = multi_workspace.workspace().entity_id();
        let host = cx.new(|_| TestHerdrCentralHost { collapsed: false });
        multi_workspace.set_window_root_host(Some(host.clone().into()), cx);
        assert!(!multi_workspace.herdr_visible());
        (editor_id, host)
    });

    cx.simulate_resize(size(px(900.0), px(700.0)));
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let workspace_central_bounds = cx
        .debug_bounds("workspace-central-content")
        .expect("the normal Workspace branch should be rendered while HerdR is hidden");
    assert!(
        workspace_central_bounds.size.width > px(0.0)
            && workspace_central_bounds.size.height > px(0.0),
        "the normal Workspace branch should have central content bounds: {workspace_central_bounds:?}"
    );
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _cx| {
            multi_workspace.workspace().entity_id()
        }),
        editor_id,
        "the normal workspace Entity should remain installed while HerdR is hidden"
    );
    assert!(
        cx.debug_bounds("herdr-central-host").is_none(),
        "the HerdR host should be hidden while HerdR is hidden"
    );

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let herdr_central_bounds = cx
        .debug_bounds("herdr-central-content")
        .expect("the HerdR central branch should be rendered while HerdR is visible");
    let host_bounds = cx
        .debug_bounds("herdr-central-host")
        .expect("the visible HerdR host should be rendered");
    assert_eq!(
        host_bounds.origin, herdr_central_bounds.origin,
        "the visible HerdR host should start at the central content origin"
    );
    assert_eq!(
        host_bounds.size, herdr_central_bounds.size,
        "the visible HerdR host should fill the central content bounds"
    );
    assert!(
        cx.debug_bounds("workspace-central-content").is_none(),
        "the normal Workspace branch should be hidden while HerdR is visible"
    );
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _cx| {
            multi_workspace.workspace().entity_id()
        }),
        editor_id,
        "the normal workspace Entity should remain installed while HerdR is visible"
    );

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_window_root_host(None, cx);
    });
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let fallback_bounds = cx
        .debug_bounds("workspace-central-content")
        .expect("the normal Workspace branch should fall back when the host is unavailable");
    assert!(
        fallback_bounds.size.width > px(0.0) && fallback_bounds.size.height > px(0.0),
        "the Workspace fallback should have central content bounds: {fallback_bounds:?}"
    );
    assert!(
        cx.debug_bounds("herdr-central-host").is_none(),
        "the HerdR host should be absent when no host is installed"
    );
    assert!(
        cx.debug_bounds("herdr-central-content").is_none(),
        "the HerdR central branch should be absent when no host is installed"
    );
    assert!(
        multi_workspace.read_with(cx, |multi_workspace, _cx| {
            multi_workspace.herdr_visible()
        }),
        "visibility remains selected while the Workspace fallback is shown"
    );
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _cx| {
            multi_workspace.workspace().entity_id()
        }),
        editor_id,
        "the normal workspace Entity should remain installed for the fallback"
    );
}

#[gpui::test]
async fn test_herdr_collapsed_host_is_header_sized(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    multi_workspace.update(cx, |multi_workspace, cx| {
        let host = cx.new(|_| TestHerdrCentralHost { collapsed: true });
        multi_workspace.set_window_root_host(Some(host.into()), cx);
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.simulate_resize(size(px(900.0), px(700.0)));
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    let host_bounds = cx
        .debug_bounds("herdr-central-host")
        .expect("the collapsed HerdR host should be rendered");
    assert_eq!(
        host_bounds.size.height,
        px(32.0),
        "a collapsed HerdR host should remain header-sized instead of flex-filling the central slot"
    );
}

#[gpui::test]
async fn test_herdr_central_view_keeps_status_bar_visible(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    let status_bar = multi_workspace.read_with(cx, |multi_workspace, cx| {
        multi_workspace
            .workspace()
            .read(cx)
            .status_bar()
            .clone()
    });
    let status_bar_id = status_bar.entity_id();

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_herdr_visible(true, cx);
    });
    cx.simulate_resize(size(px(900.0), px(700.0)));
    cx.draw(
        gpui::Point::default(),
        size(px(900.0), px(700.0)),
        |_, _| multi_workspace.clone().into_any_element(),
    );
    cx.run_until_parked();

    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, cx| {
            multi_workspace
                .workspace()
                .read(cx)
                .status_bar()
                .entity_id()
        }),
        status_bar_id,
        "the active Workspace status bar should remain installed while HerdR is visible"
    );
    let status_bounds = cx
        .debug_bounds("status-bar")
        .expect("the active Workspace status bar should be rendered outside the central switch");
    assert!(
        status_bounds.size.width > px(0.0) && status_bounds.size.height > px(0.0),
        "the status bar should have visible bounds while HerdR is selected: {status_bounds:?}"
    );
}

#[gpui::test]
async fn test_herdr_visibility_preserves_entities(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));
    let (editor, host) = multi_workspace.update(cx, |multi_workspace, cx| {
        let editor = multi_workspace.workspace().clone();
        let host = cx.new(|_| TestHerdrCentralHost { collapsed: false });
        multi_workspace.set_window_root_host(Some(host.clone().into()), cx);
        (editor, host)
    });
    let editor_id = editor.entity_id();
    let host_id = host.entity_id();

    cx.simulate_resize(size(px(900.0), px(700.0)));
    for visible in [true, false, true, false] {
        multi_workspace.update(cx, |multi_workspace, cx| {
            multi_workspace.set_herdr_visible(visible, cx);
        });
        cx.draw(
            gpui::Point::default(),
            size(px(900.0), px(700.0)),
            |_, _| multi_workspace.clone().into_any_element(),
        );
        cx.run_until_parked();
    }

    multi_workspace.read_with(cx, |multi_workspace, _cx| {
        assert_eq!(
            multi_workspace.workspace().entity_id(),
            editor_id,
            "the workspace Entity should survive both visibility toggles"
        );
        let stored_host = multi_workspace
            .window_root_host()
            .expect("the HerdR host should remain installed")
            .clone()
            .downcast::<TestHerdrCentralHost>()
            .expect("the installed host should retain its test type");
        assert_eq!(
            stored_host.entity_id(),
            host_id,
            "the HerdR host Entity should survive both visibility toggles"
        );
    });
}


#[gpui::test]
async fn test_herdr_state_restores(cx: &mut TestAppContext) {
    init_test(cx);

    let older_state: MultiWorkspaceState = serde_json::from_value(json!({
        "active_workspace_id": null,
        "sidebar_open": false,
        "project_groups": [],
    }))
    .expect("older MultiWorkspaceState JSON should remain readable");
    assert!(!older_state.herdr_visible);

    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root".as_ref()], cx).await;
    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    let visible_state = MultiWorkspaceState {
        herdr_visible: true,
        ..Default::default()
    };
    let serialized = serde_json::to_string(&visible_state).expect("state should serialize");
    let restored_state: MultiWorkspaceState =
        serde_json::from_str(&serialized).expect("state should deserialize");

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_herdr_visible(restored_state.herdr_visible, cx);
    });
    assert!(multi_workspace.read_with(cx, |multi_workspace, _| {
        multi_workspace.herdr_visible()
    }));
}

#[gpui::test]
async fn test_herdr_state_survives_workspace_switch(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_b".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));
    let workspace_a = multi_workspace.read_with(cx, |multi_workspace, _| {
        multi_workspace.workspace().clone()
    });
    let workspace_b = multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.test_add_workspace(project_b, window, cx)
    });
    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.set_herdr_visible(true, cx);
    });

    multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.activate(workspace_a.clone(), None, window, cx);
    });
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.workspace().entity_id()
        }),
        workspace_a.entity_id()
    );
    assert_eq!(
        workspace_b.entity_id(),
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace
                .workspaces()
                .find(|workspace| workspace.entity_id() == workspace_b.entity_id())
                .expect("workspace B should remain held")
                .entity_id()
        })
    );
    assert!(multi_workspace.read_with(cx, |multi_workspace, _| {
        multi_workspace.herdr_visible()
    }));
}
fn setup_multi_workspace<'a>(
    projects: &[Entity<Project>],
    cx: &'a mut TestAppContext,
) -> (Entity<MultiWorkspace>, &'a mut VisualTestContext) {
    let mut iterator = projects.iter();
    let project = iterator
        .next()
        .expect("At least one project should be provided")
        .clone();

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    for project in iterator {
        multi_workspace.update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.test_add_workspace(project.clone(), window, cx);
        })
    }

    // Opening the sidebar retains the workspaces and establishes their project groups.
    multi_workspace.update(cx, |multi_workspace, cx| multi_workspace.open_sidebar(cx));
    cx.run_until_parked();

    (multi_workspace, cx)
}

#[gpui::test]
async fn test_sidebar_disabled_when_disable_ai_is_enabled(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    let project = Project::test(fs, [], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    multi_workspace.read_with(cx, |mw, cx| {
        assert!(mw.multi_workspace_enabled(cx));
    });

    multi_workspace.update_in(cx, |mw, _window, cx| {
        mw.open_sidebar(cx);
        assert!(mw.sidebar_open());
    });

    cx.update(|_window, cx| {
        DisableAiSettings::override_global(DisableAiSettings { disable_ai: true }, cx);
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, cx| {
        assert!(
            !mw.sidebar_open(),
            "Sidebar should be closed when disable_ai is true"
        );
        assert!(
            !mw.multi_workspace_enabled(cx),
            "Multi-workspace should be disabled when disable_ai is true"
        );
    });

    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.toggle_sidebar(window, cx);
    });
    multi_workspace.read_with(cx, |mw, _cx| {
        assert!(
            !mw.sidebar_open(),
            "Sidebar should remain closed when toggled with disable_ai true"
        );
    });

    cx.update(|_window, cx| {
        DisableAiSettings::override_global(DisableAiSettings { disable_ai: false }, cx);
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, cx| {
        assert!(
            mw.multi_workspace_enabled(cx),
            "Multi-workspace should be enabled after re-enabling AI"
        );
        assert!(
            !mw.sidebar_open(),
            "Sidebar should still be closed after re-enabling AI (not auto-opened)"
        );
    });

    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.toggle_sidebar(window, cx);
    });
    multi_workspace.read_with(cx, |mw, _cx| {
        assert!(
            mw.sidebar_open(),
            "Sidebar should open when toggled after re-enabling AI"
        );
    });
}

#[gpui::test]
async fn test_multi_workspace_collapses_when_agent_is_disabled(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_b".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.test_add_workspace(project_b, window, cx);
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert!(multi_workspace.multi_workspace_enabled(cx));
        assert_eq!(multi_workspace.workspaces().count(), 2);
    });

    cx.update(|_window, cx| {
        let mut settings = AgentSettings::get_global(cx).clone();
        settings.enabled = false;
        AgentSettings::override_global(settings, cx);
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert!(!multi_workspace.multi_workspace_enabled(cx));
        assert!(!multi_workspace.sidebar_open());
        assert_eq!(multi_workspace.workspaces().count(), 1);
        assert!(multi_workspace.project_group_keys().is_empty());
    });
}

#[gpui::test]
async fn test_project_group_keys_initial(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let expected_key = project.read_with(cx, |project, cx| project.project_group_key(cx));

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys: Vec<ProjectGroupKey> = mw.project_group_keys();
        assert_eq!(keys.len(), 1, "should have exactly one key on creation");
        assert_eq!(keys[0], expected_key);
    });
}

#[gpui::test]
async fn test_project_group_keys_add_workspace(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs.clone(), ["/root_b".as_ref()], cx).await;

    let key_a = project_a.read_with(cx, |p, cx| p.project_group_key(cx));
    let key_b = project_b.read_with(cx, |p, cx| p.project_group_key(cx));
    assert_ne!(
        key_a, key_b,
        "different roots should produce different keys"
    );

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });

    multi_workspace.read_with(cx, |mw, _cx| {
        assert_eq!(mw.project_group_keys().len(), 1);
    });

    // Adding a workspace with a different project root adds a new key.
    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.test_add_workspace(project_b, window, cx);
    });

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys: Vec<ProjectGroupKey> = mw.project_group_keys();
        assert_eq!(
            keys.len(),
            2,
            "should have two keys after adding a second workspace"
        );
        assert_eq!(keys[0], key_b);
        assert_eq!(keys[1], key_a);
    });
}

#[gpui::test]
async fn test_move_active_project_group_actions(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_b".as_ref()], cx).await;

    let key_a = project_a.read_with(cx, |project, cx| project.project_group_key(cx));
    let key_b = project_b.read_with(cx, |project, cx| project.project_group_key(cx));

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));
    multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.test_add_workspace(project_b, window, cx);
    });

    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.project_group_keys()
        }),
        vec![key_b.clone(), key_a.clone()]
    );

    cx.dispatch_action(MoveProjectDown);
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.project_group_keys()
        }),
        vec![key_a.clone(), key_b.clone()]
    );

    cx.dispatch_action(MoveProjectDown);
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.project_group_keys()
        }),
        vec![key_a.clone(), key_b.clone()]
    );

    cx.dispatch_action(MoveProjectUp);
    assert_eq!(
        multi_workspace.read_with(cx, |multi_workspace, _| {
            multi_workspace.project_group_keys()
        }),
        vec![key_b, key_a]
    );
}

#[gpui::test]
async fn test_open_new_window_does_not_open_sidebar_on_existing_window(cx: &mut TestAppContext) {
    init_test(cx);

    let app_state = cx.update(AppState::test);
    let fs = app_state.fs.as_fake();
    fs.insert_tree(path!("/project_a"), json!({ "file.txt": "" }))
        .await;
    fs.insert_tree(path!("/project_b"), json!({ "file.txt": "" }))
        .await;

    let project = Project::test(app_state.fs.clone(), [path!("/project_a").as_ref()], cx).await;

    let window = cx.add_window(|window, cx| MultiWorkspace::test_new(project, window, cx));

    window
        .read_with(cx, |mw, _cx| {
            assert!(!mw.sidebar_open(), "sidebar should start closed",);
        })
        .unwrap();

    cx.update(|cx| {
        open_paths(
            &[PathBuf::from(path!("/project_b"))],
            app_state,
            OpenOptions {
                open_mode: OpenMode::NewWindow,
                ..OpenOptions::default()
            },
            cx,
        )
    })
    .await
    .unwrap();

    window
        .read_with(cx, |mw, _cx| {
            assert!(
                !mw.sidebar_open(),
                "opening a project in a new window must not open the sidebar on the original window",
            );
        })
        .unwrap();
}

#[gpui::test]
async fn test_open_directory_in_empty_workspace_does_not_open_sidebar(cx: &mut TestAppContext) {
    init_test(cx);

    let app_state = cx.update(AppState::test);
    let fs = app_state.fs.as_fake();
    fs.insert_tree(path!("/project"), json!({ "file.txt": "" }))
        .await;

    let project = Project::test(app_state.fs.clone(), [], cx).await;
    let window = cx.add_window(|window, cx| {
        let mw = MultiWorkspace::test_new(project, window, cx);
        // Simulate a blank project that has an untitled editor tab,
        // so that workspace_windows_for_location finds this window.
        mw.workspace().update(cx, |workspace, cx| {
            workspace.active_pane().update(cx, |pane, cx| {
                let item = cx.new(|cx| item::test::TestItem::new(cx));
                pane.add_item(Box::new(item), false, false, None, window, cx);
            });
        });
        mw
    });

    window
        .read_with(cx, |mw, _cx| {
            assert!(!mw.sidebar_open(), "sidebar should start closed");
        })
        .unwrap();

    // Simulate what open_workspace_for_paths does for an empty workspace:
    // it downgrades OpenMode::NewWindow to Activate and sets requesting_window.
    cx.update(|cx| {
        open_paths(
            &[PathBuf::from(path!("/project"))],
            app_state,
            OpenOptions {
                requesting_window: Some(window),
                open_mode: OpenMode::Activate,
                ..OpenOptions::default()
            },
            cx,
        )
    })
    .await
    .unwrap();

    window
        .read_with(cx, |mw, _cx| {
            assert!(
                !mw.sidebar_open(),
                "opening a directory in a blank project via the file picker must not open the sidebar",
            );
        })
        .unwrap();
}

#[gpui::test]
async fn test_project_group_keys_duplicate_not_added(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    // A second project entity pointing at the same path produces the same key.
    let project_a2 = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;

    let key_a = project_a.read_with(cx, |p, cx| p.project_group_key(cx));
    let key_a2 = project_a2.read_with(cx, |p, cx| p.project_group_key(cx));
    assert_eq!(key_a, key_a2, "same root path should produce the same key");

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });

    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.test_add_workspace(project_a2, window, cx);
    });

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys: Vec<ProjectGroupKey> = mw.project_group_keys();
        assert_eq!(
            keys.len(),
            1,
            "duplicate key should not be added when a workspace with the same root is inserted"
        );
    });
}

#[gpui::test]
async fn test_adding_worktree_updates_project_group_key(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "other.txt": "" })).await;
    let project = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;

    let initial_key = project.read_with(cx, |p, cx| p.project_group_key(cx));

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));

    // Open sidebar to retain the workspace and create the initial group.
    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys = mw.project_group_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], initial_key);
    });

    // Add a second worktree to the project. This triggers WorktreeAdded →
    // handle_workspace_key_change, which should update the group key.
    project
        .update(cx, |project, cx| {
            project.find_or_create_worktree("/root_b", true, cx)
        })
        .await
        .expect("adding worktree should succeed");
    cx.run_until_parked();

    let updated_key = project.read_with(cx, |p, cx| p.project_group_key(cx));
    assert_ne!(
        initial_key, updated_key,
        "adding a worktree should change the project group key"
    );

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys = mw.project_group_keys();
        assert!(
            keys.contains(&updated_key),
            "should contain the updated key; got {keys:?}"
        );
    });
}

#[gpui::test]
async fn test_find_or_create_local_workspace_reuses_active_workspace_when_sidebar_closed(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    let active_workspace = multi_workspace.read_with(cx, |mw, cx| {
        assert!(
            mw.project_groups(cx).is_empty(),
            "sidebar-closed setup should start with no retained project groups"
        );
        mw.workspace().clone()
    });
    let active_workspace_id = active_workspace.entity_id();

    let workspace = multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.find_or_create_local_workspace(
                PathList::new(&[PathBuf::from("/root_a")]),
                None,
                None,
                OpenMode::Activate,
                None,
                window,
                cx,
            )
        })
        .await
        .expect("reopening the same local workspace should succeed");

    assert_eq!(
        workspace.entity_id(),
        active_workspace_id,
        "should reuse the current active workspace when the sidebar is closed"
    );

    multi_workspace.read_with(cx, |mw, _cx| {
        assert_eq!(
            mw.workspace().entity_id(),
            active_workspace_id,
            "active workspace should remain unchanged after reopening the same path"
        );
        assert_eq!(
            mw.workspaces().count(),
            1,
            "reusing the active workspace should not create a second open workspace"
        );
    });
}

#[gpui::test]
async fn test_find_or_create_workspace_uses_project_group_key_when_paths_are_missing(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree(
        "/project",
        json!({
            ".git": {},
            "src": {},
        }),
    )
    .await;
    cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
    let project = Project::test(fs.clone(), ["/project".as_ref()], cx).await;
    project
        .update(cx, |project, cx| project.git_scans_complete(cx))
        .await;

    let project_group_key = project.read_with(cx, |project, cx| project.project_group_key(cx));

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    let main_workspace = multi_workspace.read_with(cx, |mw, _cx| mw.workspace().clone());
    let main_workspace_id = main_workspace.entity_id();

    let workspace = multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.find_or_create_workspace(
                PathList::new(&[PathBuf::from("/wt-feature-a")]),
                None,
                Some(project_group_key.clone()),
                |_options, _window, _cx| Task::ready(Ok(None)),
                None,
                OpenMode::Activate,
                None,
                window,
                cx,
            )
        })
        .await
        .expect("opening a missing linked-worktree path should fall back to the project group key workspace");

    assert_eq!(
        workspace.entity_id(),
        main_workspace_id,
        "missing linked-worktree paths should reuse the main worktree workspace from the project group key"
    );

    multi_workspace.read_with(cx, |mw, cx| {
        assert_eq!(
            mw.workspace().entity_id(),
            main_workspace_id,
            "the active workspace should remain the main worktree workspace"
        );
        assert_eq!(
            PathList::new(&mw.workspace().read(cx).root_paths(cx)),
            project_group_key.path_list().clone(),
            "the activated workspace should use the project group key path list rather than the missing linked-worktree path"
        );
        assert_eq!(
            mw.workspaces().count(),
            1,
            "falling back to the project group key should not create a second workspace"
        );
    });
}

#[gpui::test]
async fn test_remove_fallback_via_find_or_create_skips_removed_workspaces(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    let workspace_a = multi_workspace.read_with(cx, |mw, _cx| mw.workspace().clone());
    let workspace_b = multi_workspace.update_in(cx, |mw, window, cx| {
        mw.test_add_workspace(project_b, window, cx)
    });
    cx.run_until_parked();

    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.activate(workspace_a.clone(), None, window, cx);
    });

    let removed = multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.remove(
                vec![workspace_a.clone()],
                RemovalIntent::CloseProject,
                window,
                cx,
            )
        })
        .await
        .expect("removing the active workspace should succeed");
    assert!(removed, "the workspace should have been removed");

    multi_workspace.read_with(cx, |mw, _cx| {
        assert_eq!(
            mw.workspace().entity_id(),
            workspace_b.entity_id(),
            "the non-excluded workspace should become active"
        );
        assert!(
            mw.workspaces()
                .all(|workspace| workspace.entity_id() != workspace_a.entity_id()),
            "the removed workspace should be gone"
        );
    });
}

#[gpui::test]
async fn test_remove_keeping_the_project_does_not_switch_projects(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file.txt": "" })).await;
    cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_b".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    let workspace_a = multi_workspace.read_with(cx, |mw, _cx| mw.workspace().clone());
    let _workspace_b = multi_workspace.update_in(cx, |mw, window, cx| {
        mw.test_add_workspace(project_b, window, cx)
    });
    cx.run_until_parked();

    multi_workspace.update_in(cx, |mw, window, cx| {
        mw.activate(workspace_a.clone(), None, window, cx);
    });
    cx.run_until_parked();

    multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.remove(
                vec![workspace_a.clone()],
                RemovalIntent::KeepProject,
                window,
                cx,
            )
        })
        .await
        .expect("removing the active workspace should succeed");
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, cx| {
        assert_eq!(
            PathList::new(&mw.workspace().read(cx).root_paths(cx)),
            PathList::new(&[PathBuf::from("/root_a")]),
            "the replacement workspace should be in the removed workspace's project"
        );
    });
}

#[gpui::test]
async fn test_find_or_create_local_workspace_reuses_active_workspace_after_sidebar_open(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });
    cx.run_until_parked();

    let active_workspace = multi_workspace.read_with(cx, |mw, cx| {
        assert_eq!(
            mw.project_groups(cx).len(),
            1,
            "opening the sidebar should retain the active workspace in a project group"
        );
        mw.workspace().clone()
    });
    let active_workspace_id = active_workspace.entity_id();

    let workspace = multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.find_or_create_local_workspace(
                PathList::new(&[PathBuf::from("/root_a")]),
                None,
                None,
                OpenMode::Activate,
                None,
                window,
                cx,
            )
        })
        .await
        .expect("reopening the same retained local workspace should succeed");

    assert_eq!(
        workspace.entity_id(),
        active_workspace_id,
        "should reuse the retained active workspace after the sidebar is opened"
    );

    multi_workspace.read_with(cx, |mw, _cx| {
        assert_eq!(
            mw.workspaces().count(),
            1,
            "reopening the same retained workspace should not create another workspace"
        );
    });
}

#[gpui::test]
async fn test_close_workspace_prefers_already_loaded_neighboring_workspace(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file_a.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file_b.txt": "" })).await;
    fs.insert_tree("/root_c", json!({ "file_c.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs.clone(), ["/root_b".as_ref()], cx).await;
    let project_b_key = project_b.read_with(cx, |project, cx| project.project_group_key(cx));
    let project_c = Project::test(fs, ["/root_c".as_ref()], cx).await;
    let project_c_key = project_c.read_with(cx, |project, cx| project.project_group_key(cx));

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.open_sidebar(cx);
    });
    cx.run_until_parked();

    let workspace_a = multi_workspace.read_with(cx, |multi_workspace, _cx| {
        multi_workspace.workspace().clone()
    });
    let workspace_b = multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.test_add_workspace(project_b, window, cx)
    });

    multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.activate(workspace_a.clone(), None, window, cx);
        multi_workspace.test_add_project_group(ProjectGroup {
            key: project_c_key.clone(),
            workspaces: Vec::new(),
            expanded: true,
        });
    });

    multi_workspace.read_with(cx, |multi_workspace, _cx| {
        let keys = multi_workspace.project_group_keys();
        assert_eq!(
            keys.len(),
            3,
            "expected three project groups in the test setup"
        );
        assert_eq!(keys[0], project_b_key);
        assert_eq!(
            keys[1],
            workspace_a.read_with(cx, |workspace, cx| { workspace.project_group_key(cx) })
        );
        assert_eq!(keys[2], project_c_key);
        assert_eq!(
            multi_workspace.workspace().entity_id(),
            workspace_a.entity_id(),
            "workspace A should be active before closing"
        );
    });

    let closed = multi_workspace
        .update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.remove(
                [workspace_a.clone()],
                RemovalIntent::CloseProject,
                window,
                cx,
            )
        })
        .await
        .expect("closing the active workspace should succeed");

    assert!(
        closed,
        "close_workspace should report that it removed a workspace"
    );

    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert_eq!(
            multi_workspace.workspace().entity_id(),
            workspace_b.entity_id(),
            "closing workspace A should activate the already-loaded workspace B instead of opening group C"
        );
        assert_eq!(
            multi_workspace.workspaces().count(),
            1,
            "only workspace B should remain loaded after closing workspace A"
        );
        assert!(
            multi_workspace
                .workspaces_for_project_group(&project_c_key, cx)
                .is_empty(),
            "the unloaded neighboring group C should remain unopened"
        );
    });
}

#[gpui::test]
async fn test_close_workspace_prefers_workspace_in_same_project_group(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;
    fs.insert_tree("/project-b", json!({})).await;

    let project_a_1 = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_a_2 = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/project-b".as_ref()], cx).await;
    let key_a = project_a_1.read_with(cx, |project, cx| project.project_group_key(cx));
    let (multi_workspace, cx) = setup_multi_workspace(&[project_a_1, project_a_2, project_b], cx);

    let (workspace_a_1, workspace_a_2) = multi_workspace.read_with(cx, |multi_workspace, cx| {
        let mut workspaces = multi_workspace
            .workspaces_for_project_group(&key_a, cx)
            .into_iter();
        let first = workspaces
            .next()
            .expect("project group A should have a first workspace");
        let second = workspaces
            .next()
            .expect("project group A should have a second workspace");

        assert!(
            workspaces.next().is_none(),
            "project group A should have exactly two workspaces"
        );

        (first, second)
    });

    multi_workspace.update_in(cx, |multi_workspace, window, cx| {
        multi_workspace.activate(workspace_a_1.clone(), None, window, cx);
    });

    let closed = multi_workspace
        .update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.remove(
                [workspace_a_1.clone()],
                RemovalIntent::CloseProject,
                window,
                cx,
            )
        })
        .await
        .expect("closing the active workspace should succeed");

    assert!(closed, "close_workspace should remove the active workspace");
    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert_eq!(
            multi_workspace.workspace(),
            &workspace_a_2,
            "the second workspace for project group a should be preferred"
        );

        assert_eq!(
            multi_workspace.workspaces_for_project_group(&key_a, cx),
            vec![workspace_a_2],
            "only the fallback workspace should remain in project group A"
        );
    });
}

#[gpui::test]
async fn test_close_workspace_opens_unloaded_local_neighbor(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;
    fs.insert_tree("/project-b", json!({})).await;
    cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));

    let project_a = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/project-b".as_ref()], cx).await;
    let key_b = project_b.read_with(cx, |project, cx| project.project_group_key(cx));
    let (multi_workspace, cx) = setup_multi_workspace(&[project_a], cx);
    let workspace_a = multi_workspace.read_with(cx, |multi_workspace, _cx| {
        multi_workspace.workspace().clone()
    });

    multi_workspace.update(cx, |multi_workspace, _cx| {
        multi_workspace.test_add_project_group(ProjectGroup {
            key: key_b.clone(),
            workspaces: Vec::new(),
            expanded: true,
        });
    });

    let closed = multi_workspace
        .update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.remove(
                [workspace_a.clone()],
                RemovalIntent::CloseProject,
                window,
                cx,
            )
        })
        .await
        .expect("closing the active workspace should succeed");

    assert!(closed, "close_workspace should remove the active workspace");
    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert_eq!(
            multi_workspace.workspace().read(cx).project_group_key(cx),
            key_b,
            "the unloaded local neighboring group should be opened"
        );
    });
}

#[gpui::test]
async fn test_remove_project_group_opens_unloaded_local_neighbor(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;
    fs.insert_tree("/project-b", json!({})).await;
    cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));

    let project_a = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/project-b".as_ref()], cx).await;
    let key_a = project_a.read_with(cx, |project, cx| project.project_group_key(cx));
    let key_b = project_b.read_with(cx, |project, cx| project.project_group_key(cx));
    let (multi_workspace, cx) = setup_multi_workspace(&[project_a], cx);

    multi_workspace.update(cx, |multi_workspace, _cx| {
        multi_workspace.test_add_project_group(ProjectGroup {
            key: key_b.clone(),
            workspaces: Vec::new(),
            expanded: true,
        });
    });

    let removed = multi_workspace
        .update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.remove_project_group(&key_a, window, cx)
        })
        .await
        .expect("removing the active project group should succeed");

    assert!(
        removed,
        "remove_project_group should remove the active group"
    );

    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert_eq!(
            multi_workspace.workspace().read(cx).project_group_key(cx),
            key_b,
            "the unloaded local neighboring group should be opened"
        );
    });
}

#[gpui::test]
async fn test_remove_project_group_replaces_unretained_active_workspace(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;

    let project_a = Project::test(fs, ["/project-a".as_ref()], cx).await;
    let key_a = project_a.read_with(cx, |project, cx| project.project_group_key(cx));
    let remote_key = ProjectGroupKey::new(
        Some(RemoteConnectionOptions::Mock(
            remote::MockConnectionOptions { id: 1 },
        )),
        PathList::new(&[PathBuf::from("/remote/project")]),
    );
    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));
    let workspace_a = multi_workspace.read_with(cx, |multi_workspace, _cx| {
        multi_workspace.workspace().clone()
    });

    multi_workspace.update(cx, |multi_workspace, cx| {
        multi_workspace.restore_project_groups(
            vec![
                SerializedProjectGroupState {
                    key: key_a.clone(),
                    expanded: true,
                },
                SerializedProjectGroupState {
                    key: remote_key.clone(),
                    expanded: true,
                },
            ],
            cx,
        );

        assert!(
            !multi_workspace.active_workspace_is_retained(),
            "the active workspace should remain provisional"
        );
        assert_eq!(
            multi_workspace.project_group_keys(),
            vec![key_a.clone(), remote_key.clone()],
            "the remote project group should immediately follow the active local group"
        );
    });

    multi_workspace
        .update_in(cx, |multi_workspace, window, cx| {
            multi_workspace.remove_project_group(&key_a, window, cx)
        })
        .await
        .expect("removing the active project group should succeed");

    multi_workspace.read_with(cx, |multi_workspace, cx| {
        assert_ne!(
            multi_workspace.workspace(),
            &workspace_a,
            "removing the active project group should replace its provisional workspace"
        );
        assert!(
            multi_workspace
                .workspace()
                .read(cx)
                .root_paths(cx)
                .is_empty(),
            "an unloaded remote neighbor should fall back to an empty workspace"
        );
        assert_eq!(
            multi_workspace.project_group_keys(),
            vec![remote_key],
            "only the remote project group should remain"
        );
    });
}

#[gpui::test]
async fn test_switching_projects_with_sidebar_closed_retains_old_active_workspace(
    cx: &mut TestAppContext,
) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file_a.txt": "" })).await;
    fs.insert_tree("/root_b", json!({ "file_b.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/root_b".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    let workspace_a = multi_workspace.read_with(cx, |mw, cx| {
        assert!(
            mw.project_groups(cx).is_empty(),
            "sidebar-closed setup should start with no retained project groups"
        );
        mw.workspace().clone()
    });
    assert!(
        workspace_a.read_with(cx, |workspace, _cx| workspace.session_id().is_some()),
        "initial active workspace should start attached to the session"
    );

    let workspace_b = multi_workspace.update_in(cx, |mw, window, cx| {
        mw.test_add_workspace(project_b, window, cx)
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, cx| {
        assert_eq!(
            mw.workspace().entity_id(),
            workspace_b.entity_id(),
            "the new workspace should become active"
        );
        assert_eq!(
            mw.workspaces().count(),
            2,
            "the previous active workspace should remain open after switching with the sidebar closed"
        );
        assert_eq!(mw.project_groups(cx).len(), 2);
    });

    assert!(
        workspace_a.read_with(cx, |workspace, _cx| workspace.session_id().is_some()),
        "the previous active workspace should remain attached when switching away with the sidebar closed"
    );
}

#[gpui::test]
async fn test_remote_project_root_dir_changes_update_groups(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    fs.insert_tree("/local_b", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs.clone(), ["/root_a".as_ref()], cx).await;
    let project_b = Project::test(fs.clone(), ["/local_b".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });
    cx.run_until_parked();

    let workspace_b = multi_workspace.update_in(cx, |mw, window, cx| {
        let workspace = cx.new(|cx| Workspace::test_new(project_b.clone(), window, cx));
        let key = workspace.read(cx).project_group_key(cx);
        mw.activate_provisional_workspace(workspace.clone(), key, window, cx);
        workspace
    });
    cx.run_until_parked();

    multi_workspace.read_with(cx, |mw, _cx| {
        assert_eq!(
            mw.workspace().entity_id(),
            workspace_b.entity_id(),
            "registered workspace should become active"
        );
    });

    let initial_key = project_b.read_with(cx, |p, cx| p.project_group_key(cx));
    multi_workspace.read_with(cx, |mw, _cx| {
        let keys = mw.project_group_keys();
        assert!(
            keys.contains(&initial_key),
            "project groups should contain the initial key for the registered workspace"
        );
    });

    let remote_worktree = project_b.update(cx, |project, cx| {
        project.add_test_remote_worktree("/remote/project", cx)
    });
    cx.run_until_parked();

    let worktree_id = remote_worktree.read_with(cx, |wt, _| wt.id().to_proto());
    remote_worktree.update(cx, |worktree, _cx| {
        worktree
            .as_remote()
            .unwrap()
            .update_from_remote(proto::UpdateWorktree {
                project_id: 0,
                worktree_id,
                abs_path: "/remote/project".to_string(),
                root_name: "project".to_string(),
                updated_entries: vec![proto::Entry {
                    id: 1,
                    is_dir: true,
                    path: "".to_string(),
                    inode: 1,
                    mtime: Some(proto::Timestamp {
                        seconds: 0,
                        nanos: 0,
                    }),
                    is_ignored: false,
                    is_hidden: false,
                    is_external: false,
                    is_fifo: false,
                    size: None,
                    canonical_path: None,
                    is_unloaded: false,
                }],
                removed_entries: vec![],
                scan_id: 1,
                is_last_update: true,
                updated_repositories: vec![],
                removed_repositories: vec![],
                root_repo_common_dir: None,
                root_repo_is_linked_worktree: false,
            });
    });
    cx.run_until_parked();

    let updated_key = project_b.read_with(cx, |p, cx| p.project_group_key(cx));
    assert_ne!(
        initial_key, updated_key,
        "remote worktree update should change the project group key"
    );

    multi_workspace.read_with(cx, |mw, _cx| {
        let keys = mw.project_group_keys();
        assert!(
            keys.contains(&updated_key),
            "project groups should contain the updated key after remote change; got {keys:?}"
        );
        assert!(
            !keys.contains(&initial_key),
            "project groups should no longer contain the stale initial key; got {keys:?}"
        );
    });
}

#[gpui::test]
async fn test_open_project_closes_empty_workspace_but_not_non_empty_ones(cx: &mut TestAppContext) {
    init_test(cx);
    let app_state = cx.update(AppState::test);
    let fs = app_state.fs.as_fake();
    fs.insert_tree(path!("/project_a"), json!({ "file_a.txt": "" }))
        .await;
    fs.insert_tree(path!("/project_b"), json!({ "file_b.txt": "" }))
        .await;

    // Start with an empty (no-worktrees) workspace.
    let project = Project::test(app_state.fs.clone(), [], cx).await;
    let window = cx.add_window(|window, cx| MultiWorkspace::test_new(project, window, cx));
    cx.run_until_parked();

    window
        .update(cx, |mw, _window, cx| mw.open_sidebar(cx))
        .unwrap();
    cx.run_until_parked();

    let empty_workspace = window
        .read_with(cx, |mw, _| mw.workspace().clone())
        .unwrap();
    let cx = &mut VisualTestContext::from_window(window.into(), cx);

    // Add a dirty untitled item to the empty workspace.
    let dirty_item = cx.new(|cx| TestItem::new(cx).with_dirty(true));
    empty_workspace.update_in(cx, |workspace, window, cx| {
        workspace.add_item_to_active_pane(Box::new(dirty_item.clone()), None, true, window, cx);
    });

    // Opening a project while the lone empty workspace has unsaved
    // changes prompts the user.
    let open_task = window
        .update(cx, |mw, window, cx| {
            mw.open_project(
                vec![PathBuf::from(path!("/project_a"))],
                OpenMode::Activate,
                window,
                cx,
            )
        })
        .unwrap();
    cx.run_until_parked();

    // Cancelling keeps the empty workspace.
    assert!(cx.has_pending_prompt(),);
    cx.simulate_prompt_answer("Cancel");
    cx.run_until_parked();
    assert_eq!(open_task.await.unwrap(), empty_workspace);
    window
        .read_with(cx, |mw, _cx| {
            assert_eq!(mw.workspaces().count(), 1);
            assert_eq!(mw.workspace(), &empty_workspace);
            assert_eq!(mw.project_group_keys(), vec![]);
        })
        .unwrap();

    // Discarding the unsaved changes closes the empty workspace
    // and opens the new project in its place.
    let open_task = window
        .update(cx, |mw, window, cx| {
            mw.open_project(
                vec![PathBuf::from(path!("/project_a"))],
                OpenMode::Activate,
                window,
                cx,
            )
        })
        .unwrap();
    cx.run_until_parked();

    assert!(cx.has_pending_prompt(),);
    cx.simulate_prompt_answer("Don't Save");
    cx.run_until_parked();

    let workspace_a = open_task.await.unwrap();
    assert_ne!(workspace_a, empty_workspace);

    window
        .read_with(cx, |mw, _cx| {
            assert_eq!(mw.workspaces().count(), 1);
            assert_eq!(mw.workspace(), &workspace_a);
            assert_eq!(
                mw.project_group_keys(),
                vec![ProjectGroupKey::new(
                    None,
                    PathList::new(&[path!("/project_a")])
                )]
            );
        })
        .unwrap();
    assert!(
        empty_workspace.read_with(cx, |workspace, _cx| workspace.session_id().is_none()),
        "the detached empty workspace should no longer be attached to the session",
    );

    let dirty_item = cx.new(|cx| TestItem::new(cx).with_dirty(true));
    workspace_a.update_in(cx, |workspace, window, cx| {
        workspace.add_item_to_active_pane(Box::new(dirty_item.clone()), None, true, window, cx);
    });

    // Opening another project does not close the existing project or prompt.
    let workspace_b = window
        .update(cx, |mw, window, cx| {
            mw.open_project(
                vec![PathBuf::from(path!("/project_b"))],
                OpenMode::Activate,
                window,
                cx,
            )
        })
        .unwrap()
        .await
        .unwrap();
    cx.run_until_parked();

    assert!(!cx.has_pending_prompt());
    assert_ne!(workspace_b, workspace_a);
    window
        .read_with(cx, |mw, _cx| {
            assert_eq!(mw.workspaces().count(), 2);
            assert_eq!(mw.workspace(), &workspace_b);
            assert_eq!(
                mw.project_group_keys(),
                vec![
                    ProjectGroupKey::new(None, PathList::new(&[path!("/project_b")])),
                    ProjectGroupKey::new(None, PathList::new(&[path!("/project_a")]))
                ]
            );
        })
        .unwrap();
    assert!(workspace_a.read_with(cx, |workspace, _cx| workspace.session_id().is_some()),);
}

#[gpui::test]
async fn test_close_workspace_with_remote_neighbor_does_not_create_local_workspace(
    cx: &mut TestAppContext,
) {
    // Regression test: closing a workspace whose neighboring group is
    // remote with no existing workspace should not create a local
    // workspace with the remote paths.
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a, window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });
    cx.run_until_parked();

    // Add a mock-remote group with no workspace as the second group.
    let remote_key = ProjectGroupKey::new(
        Some(RemoteConnectionOptions::Mock(
            remote::MockConnectionOptions { id: 1 },
        )),
        PathList::new(&[PathBuf::from("/remote/project")]),
    );
    multi_workspace.update(cx, |mw, _cx| {
        mw.test_add_project_group(ProjectGroup {
            key: remote_key.clone(),
            workspaces: Vec::new(),
            expanded: true,
        });
    });

    let workspace_a = multi_workspace.read_with(cx, |mw, _cx| mw.workspace().clone());

    // Close workspace A. The neighbor is the remote group with no workspace.
    // The fix should skip find_or_create_local_workspace and fall through
    // to creating an empty workspace instead.
    multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.remove(
                [workspace_a.clone()],
                RemovalIntent::CloseProject,
                window,
                cx,
            )
        })
        .await
        .expect("close_workspace should succeed");

    cx.run_until_parked();

    multi_workspace.update(cx, |mw, cx| {
        // The active workspace should NOT be a local workspace with the
        // remote paths. It should be an empty workspace (no worktrees).
        let workspaces: Vec<_> = mw.workspaces().cloned().collect();
        for ws in &workspaces {
            let key = ws.read(cx).project_group_key(cx);
            assert!(
                key.host().is_some()
                    || key.path_list().paths() != [PathBuf::from("/remote/project")],
                "remote neighbor should not have created a local workspace"
            );
        }
    });
}

#[gpui::test]
async fn test_remove_project_group_with_remote_neighbor_does_not_create_local_workspace(
    cx: &mut TestAppContext,
) {
    // Regression test: removing a project group whose neighboring group is
    // remote with no workspace should not create a local workspace with
    // the remote paths.
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/root_a", json!({ "file.txt": "" })).await;
    let project_a = Project::test(fs, ["/root_a".as_ref()], cx).await;

    let (multi_workspace, cx) =
        cx.add_window_view(|window, cx| MultiWorkspace::test_new(project_a.clone(), window, cx));

    multi_workspace.update(cx, |mw, cx| {
        mw.open_sidebar(cx);
    });
    cx.run_until_parked();

    let key_a = project_a.read_with(cx, |p, cx| p.project_group_key(cx));

    // Add a mock-remote group with no workspace.
    let remote_key = ProjectGroupKey::new(
        Some(RemoteConnectionOptions::Mock(
            remote::MockConnectionOptions { id: 1 },
        )),
        PathList::new(&[PathBuf::from("/remote/project")]),
    );
    multi_workspace.update(cx, |mw, _cx| {
        mw.test_add_project_group(ProjectGroup {
            key: remote_key.clone(),
            workspaces: Vec::new(),
            expanded: true,
        });
    });

    // Remove the local group A. The neighbor is the remote group with no
    // workspace. The fix should skip find_or_create_local_workspace and
    // fall through to creating an empty workspace.
    multi_workspace
        .update_in(cx, |mw, window, cx| {
            mw.remove_project_group(&key_a, window, cx)
        })
        .await
        .expect("remove_project_group should succeed");

    cx.run_until_parked();

    multi_workspace.update(cx, |mw, cx| {
        let workspaces: Vec<_> = mw.workspaces().cloned().collect();
        for ws in &workspaces {
            let key = ws.read(cx).project_group_key(cx);
            assert!(
                key.host().is_some() || key.path_list().paths() != [PathBuf::from("/remote/project")],
                "remote neighbor should not have created a local workspace after remove_project_group"
            );
        }
    });
}

#[gpui::test]
async fn test_nearest_retained_workspace(cx: &mut TestAppContext) {
    init_test(cx);

    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;
    fs.insert_tree("/project-b", json!({})).await;
    fs.insert_tree("/project-c", json!({})).await;
    fs.insert_tree("/project-d", json!({})).await;

    // These two projects create separate workspaces in the same project group. The second
    // workspace is activated after the first, making it the group's last active workspace.
    let project_a_1 = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_a_2 = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_b = Project::test(fs.clone(), ["/project-b".as_ref()], cx).await;
    let project_c = Project::test(fs.clone(), ["/project-c".as_ref()], cx).await;
    let project_d = Project::test(fs, ["/project-d".as_ref()], cx).await;
    let key_a = project_a_1.read_with(cx, |project, cx| project.project_group_key(cx));
    let key_b = project_b.read_with(cx, |project, cx| project.project_group_key(cx));
    let key_c = project_c.read_with(cx, |project, cx| project.project_group_key(cx));
    let key_d = project_d.read_with(cx, |project, cx| project.project_group_key(cx));

    let (multi_workspace, cx) = setup_multi_workspace(
        &[project_a_1, project_a_2, project_b, project_c, project_d],
        cx,
    );

    multi_workspace.update(cx, |multi_workspace, cx| {
        assert_eq!(
            multi_workspace.project_group_keys(),
            vec![key_d.clone(), key_c.clone(), key_b.clone(), key_a.clone()],
            "new project groups should be inserted before existing groups"
        );

        let group_c_index = multi_workspace
            .project_groups(cx)
            .iter()
            .position(|project_group| project_group.key == key_c)
            .expect("project group for project-c should exist");
        let workspace_b = multi_workspace
            .workspaces_for_project_group(&key_b, cx)
            .into_iter()
            .next()
            .expect("workspace for project-b should exist");
        let workspace_d = multi_workspace
            .workspaces_for_project_group(&key_d, cx)
            .into_iter()
            .next()
            .expect("workspace for project-d should exist");
        let retained_workspaces_a = multi_workspace
            .workspaces_for_project_group(&key_a, cx);
        assert_eq!(
            retained_workspaces_a.len(),
            2,
            "project group A should retain both workspaces"
        );
        let workspace_a_1 = retained_workspaces_a
            .first()
            .expect("project group A should have a retained workspace")
            .clone();
        let workspace_a_2 = multi_workspace
            .last_active_workspace_for_group(&key_a, cx)
            .expect("project group A should have a last active workspace");
        assert_ne!(
            workspace_a_1, workspace_a_2,
            "project group A's last active workspace should differ from its first retained workspace"
        );

        // Since Project Group B is the one after C, it is preferred over
        // Project Group D, even if they're at the same distance.
        assert_eq!(
            multi_workspace.nearest_retained_workspace(group_c_index, &[], cx),
            Some(workspace_b.clone()),
            "the following project group should be preferred at equal distance"
        );

        // With Project Group B being excluded, Project Group D is picked as it
        // is the one with the smallest distance.
        assert_eq!(
            multi_workspace.nearest_retained_workspace(
                group_c_index,
                std::slice::from_ref(&workspace_b),
                cx,
            ),
            Some(workspace_d.clone()),
            "the preceding project group should be used when the following workspace is excluded"
        );

        // With both adjacent Project Groups excluded, the search expands and
        // reaches Project A at distance 2 and prefers its last active workspace (A2)
        // over its first retained workspace (A1).
        assert_eq!(
            multi_workspace.nearest_retained_workspace(
                group_c_index,
                &[workspace_b.clone(), workspace_d.clone()],
                cx
            ),
            Some(workspace_a_2.clone()),
            "the farther group's last active workspace should be preferred"
        );

        // With the group's most recently activated workspace excluded, the
        // search falls back to the member activated before it.
        assert_eq!(
            multi_workspace.nearest_retained_workspace(
                group_c_index,
                &[
                    workspace_b.clone(),
                    workspace_d.clone(),
                    workspace_a_2.clone()
                ],
                cx
            ),
            Some(workspace_a_1.clone()),
            "the previously activated workspace should be used when the last active one is excluded"
        );

        // Excluding every neighboring workspace exhausts the search.
        assert_eq!(
            multi_workspace.nearest_retained_workspace(
                group_c_index,
                &[
                    workspace_b,
                    workspace_d,
                    workspace_a_1,
                    workspace_a_2,
                ],
                cx
            ),
            None,
            "no workspace should be returned when every candidate is excluded"
        );
    });
}

#[gpui::test]
async fn test_nearest_retained_workspace_skips_disconnected_workspace(cx: &mut TestAppContext) {
    init_test(cx);
    let fs = FakeFs::new(cx.executor());
    fs.insert_tree("/project-a", json!({})).await;
    fs.insert_tree("/project-b", json!({})).await;

    let project_a = Project::test(fs.clone(), ["/project-a".as_ref()], cx).await;
    let project_b = Project::test(fs, ["/project-b".as_ref()], cx).await;
    let key_a = project_a.read_with(cx, |project, cx| project.project_group_key(cx));
    let (multi_workspace, cx) = setup_multi_workspace(&[project_a.clone(), project_b.clone()], cx);

    project_b.update(cx, |project, cx| {
        project.mark_as_collab_for_testing();
        project.disconnected_from_host(cx);
    });
    cx.run_until_parked();

    multi_workspace.update(cx, |multi_workspace, cx| {
        let group_a_index = multi_workspace
            .project_groups(cx)
            .iter()
            .position(|group| group.key == key_a)
            .expect("project group A should exist");

        assert_eq!(
            multi_workspace.nearest_retained_workspace(group_a_index, &[], cx),
            None,
            "a disconnected workspace should not be selected as a fallback"
        );
    });
}
