use collections::{HashMap, IndexMap};
use gpui::{
    App, Context, Entity, FocusHandle, Focusable, Render, SharedString, Task, WeakEntity, Window,
    div, prelude::*,
};
use crate::herdr_bridge::{HerdrBridgeEvent, HerdrConnectionStatus, HerdrThreadBridge};
use crate::herdr_client::{HerdrAgentSessionIdentity, HerdrAgentStatus, HerdrClientError};
use ui::{Button, ButtonStyle, Label, Tooltip, prelude::*};
use crate::herdr_thread_view::HerdrThreadView;
use crate::thread_metadata_store::ThreadId;

/// Events translated from the bridge for the Herdr root view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HerdrConversationEvent {
    AgentDetected {
        pane_id: String,
        session: Option<HerdrAgentSessionIdentity>,
        title: Option<String>,
        status: HerdrAgentStatus,
    },
    StatusChanged {
        pane_id: String,
        status: HerdrAgentStatus,
    },
    Output {
        pane_id: String,
        revision: u64,
        output: String,
    },
    PaneFocused {
        pane_id: String,
    },
    Renamed {
        pane_id: String,
        title: String,
    },
    PaneClosed {
        pane_id: String,
    },
}

/// Data-only state used by the GPUI root and by deterministic tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HerdrSubthreadState {
    pub(crate) pane_id: String,
    pub(crate) session: HerdrAgentSessionIdentity,
    pub(crate) title: SharedString,
    pub(crate) status: HerdrAgentStatus,
    pub(crate) output: String,
    pub(crate) output_revision: u64,
}

impl HerdrSubthreadState {
    fn new(
        pane_id: String,
        session: HerdrAgentSessionIdentity,
        title: Option<String>,
        status: HerdrAgentStatus,
    ) -> Self {
        Self {
            title: title
                .filter(|title| !title.trim().is_empty())
                .unwrap_or_else(|| pane_id.clone())
                .into(),
            pane_id,
            session,
            status,
            output: String::new(),
            output_revision: 0,
        }
    }

    pub(crate) fn apply_output(&mut self, revision: u64, output: String) {
        if revision > self.output_revision {
            self.output_revision = revision;
            self.output = output;
        }
    }
}

/// Pure root state. A pane without an agent session remains in `status_only`
/// and is deliberately not exposed as a selectable subthread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HerdrConversationState {
    pub(crate) workspace_id: String,
    pub(crate) thread_id: ThreadId,
    subthreads: IndexMap<String, HerdrSubthreadState>,
    status_only: HashMap<String, HerdrAgentStatus>,
    active_pane_id: Option<String>,
}

impl HerdrConversationState {
    pub(crate) fn new(workspace_id: impl Into<String>, thread_id: ThreadId) -> Self {
        Self {
            workspace_id: workspace_id.into(),
            thread_id,
            subthreads: IndexMap::default(),
            status_only: HashMap::default(),
            active_pane_id: None,
        }
    }

    pub(crate) fn apply(&mut self, event: HerdrConversationEvent) {
        match event {
            HerdrConversationEvent::AgentDetected {
                pane_id,
                session,
                title,
                status,
            } => {
                if let Some(session) = session {
                    let state = self.subthreads.entry(pane_id.clone()).or_insert_with(|| {
                        HerdrSubthreadState::new(pane_id.clone(), session.clone(), title.clone(), status.clone())
                    });
                    state.session = session;
                    if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
                        state.title = title.into();
                    }
                    state.status = status;
                    self.status_only.remove(&pane_id);
                } else {
                    // Reverse identity transition: a previously selectable
                    // pane falls back to status-only so only one record
                    // remains.
                    self.subthreads.shift_remove(&pane_id);
                    if self.active_pane_id.as_deref() == Some(pane_id.as_str()) {
                        self.active_pane_id = None;
                    }
                    self.status_only.insert(pane_id, status);
                }
            }
            HerdrConversationEvent::StatusChanged { pane_id, status } => {
                if let Some(state) = self.subthreads.get_mut(&pane_id) {
                    state.status = status;
                } else {
                    self.status_only.insert(pane_id, status);
                }
            }
            HerdrConversationEvent::Output {
                pane_id,
                revision,
                output,
            } => {
                if let Some(state) = self.subthreads.get_mut(&pane_id) {
                    state.apply_output(revision, output);
                }
            }
            HerdrConversationEvent::PaneFocused { pane_id } => {
                if self.subthreads.contains_key(&pane_id) {
                    self.active_pane_id = Some(pane_id);
                }
            }
            HerdrConversationEvent::Renamed { pane_id, title } => {
                if let Some(state) = self.subthreads.get_mut(&pane_id) {
                    state.title = title.into();
                }
            }
            HerdrConversationEvent::PaneClosed { pane_id } => {
                self.subthreads.shift_remove(&pane_id);
                self.status_only.remove(&pane_id);
                if self.active_pane_id.as_deref() == Some(pane_id.as_str()) {
                    self.active_pane_id = None;
                }
            }
        }
    }

#[allow(dead_code)]
    pub(crate) fn subthreads(&self) -> &IndexMap<String, HerdrSubthreadState> {
        &self.subthreads
    }

#[allow(dead_code)]
    pub(crate) fn status_only(&self, pane_id: &str) -> Option<&HerdrAgentStatus> {
        self.status_only.get(pane_id)
    }

    pub(crate) fn is_selectable(&self, pane_id: &str) -> bool {
        self.subthreads.contains_key(pane_id)
    }

    pub(crate) fn active_pane_id(&self) -> Option<&str> {
        self.active_pane_id.as_deref()
    }

#[allow(dead_code)]
    pub(crate) fn output(&self, pane_id: &str) -> Option<&str> {
        self.subthreads.get(pane_id).map(|state| state.output.as_str())
    }

#[allow(dead_code)]
    pub(crate) fn status(&self, pane_id: &str) -> Option<&HerdrAgentStatus> {
        self.subthreads
            .get(pane_id)
            .map(|state| &state.status)
            .or_else(|| self.status_only(pane_id))
    }
}

/// A Herdr-backed root thread. It owns explicit pane child entities instead of
/// constructing ACP sessions for Herdr agents.
pub(crate) struct HerdrConversationView {
    pub(crate) thread_id: ThreadId,
    pub(crate) workspace_id: String,
    pub(crate) subthreads: IndexMap<String, Entity<HerdrThreadView>>,
    pub(crate) active_pane_id: Option<String>,
    pub(crate) focus_handle: FocusHandle,
    bridge: WeakEntity<HerdrThreadBridge>,
    state: HerdrConversationState,
    title: SharedString,
    title_override: Option<SharedString>,
    connection_status: HerdrConnectionStatus,
}

#[allow(dead_code)]
impl HerdrConversationView {
    pub(crate) fn new(
        thread_id: ThreadId,
        workspace_id: impl Into<String>,
        title: impl Into<SharedString>,
        bridge: Entity<HerdrThreadBridge>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let workspace_id = workspace_id.into();
        let bridge_ref = bridge.read(cx);
        let title_override = bridge_ref
            .root_metadata(&workspace_id)
            .and_then(|metadata| metadata.title_override.clone());
        let connection_status = bridge_ref.status();
        Self {
            thread_id,
            state: HerdrConversationState::new(workspace_id.clone(), thread_id),
            workspace_id,
            subthreads: IndexMap::default(),
            active_pane_id: None,
            focus_handle: cx.focus_handle(),
            bridge: bridge.downgrade(),
            title: title.into(),
            title_override,
            connection_status,
        }
    }

    pub(crate) fn title(&self) -> SharedString {
        self.title.clone()
    }

#[allow(dead_code)]
    pub(crate) fn connection_status(&self) -> HerdrConnectionStatus {
        self.connection_status
    }
    pub(crate) fn controls_enabled(&self) -> bool {
        self.connection_status.allows_actions()
    }

    pub(crate) fn controls_disabled_reason(&self) -> Option<&'static str> {
        self.connection_status.disabled_reason()
    }


    pub(crate) fn state(&self) -> &HerdrConversationState {
        &self.state
    }

    pub(crate) fn apply_connection_status(&mut self, status: HerdrConnectionStatus, cx: &mut Context<Self>) {
        if self.connection_status != status {
            self.connection_status = status;
            cx.notify();
        }
    }

    pub(crate) fn apply_bridge_event(
        &mut self,
        event: &HerdrBridgeEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match event {
            HerdrBridgeEvent::StatusChanged(status) => {
                self.apply_connection_status(*status, cx);
            }
            HerdrBridgeEvent::SubthreadStatusOnly {
                workspace_id,
                pane_id,
                status,
            } if workspace_id == &self.workspace_id => {
                self.apply_event(
                    HerdrConversationEvent::AgentDetected {
                        pane_id: pane_id.clone(),
                        session: None,
                        title: None,
                        status: status.clone(),
                    },
                    window,
                    cx,
                );
            }
            HerdrBridgeEvent::RootRenamed {
                workspace_id,
                title,
                ..
            } if workspace_id == &self.workspace_id => {
                if self.title_override.is_none() {
                    self.title_override = self.bridge.upgrade().and_then(|bridge| {
                        bridge
                            .read(cx)
                            .root_metadata(workspace_id)
                            .and_then(|metadata| metadata.title_override.clone())
                    });
                }
                if self.title_override.is_none() {
                    self.title = title.clone().into();
                    cx.notify();
                }
            }
            HerdrBridgeEvent::SubthreadCreated {
                key,
                pane_id,
                session,
                title,
                status,
                ..
            } if key.workspace_id == self.workspace_id => {
                self.apply_event(
                    HerdrConversationEvent::AgentDetected {
                        pane_id: pane_id.clone(),
                        session: Some(session.clone()),
                        title: Some(title.clone()),
                        status: status.clone(),
                    },
                    window,
                    cx,
                );
            }
            HerdrBridgeEvent::SubthreadUpdated {
                key,
                pane_id,
                title,
                status,
                ..
            } if key.workspace_id == self.workspace_id => {
                if let Some(title) = title {
                    self.apply_event(
                        HerdrConversationEvent::Renamed {
                            pane_id: pane_id.clone(),
                            title: title.clone(),
                        },
                        window,
                        cx,
                    );
                }
                if let Some(status) = status {
                    self.apply_event(
                        HerdrConversationEvent::StatusChanged {
                            pane_id: pane_id.clone(),
                            status: status.clone(),
                        },
                        window,
                        cx,
                    );
                }
            }
            HerdrBridgeEvent::SubthreadOutput {
                key,
                pane_id,
                revision,
                output,
                ..
            } if key.workspace_id == self.workspace_id => {
                self.apply_event(
                    HerdrConversationEvent::Output {
                        pane_id: pane_id.clone(),
                        revision: *revision,
                        output: output.clone(),
                    },
                    window,
                    cx,
                );
            }
            HerdrBridgeEvent::SubthreadFocused { key, .. } if key.workspace_id == self.workspace_id => {
                if let Some(pane_id) = key.pane_id.as_ref() {
                    self.apply_event(
                        HerdrConversationEvent::PaneFocused {
                            pane_id: pane_id.clone(),
                        },
                        window,
                        cx,
                    );
                }
            }
            HerdrBridgeEvent::SubthreadClosed { key, .. } if key.workspace_id == self.workspace_id => {
                if let Some(pane_id) = key.pane_id.as_ref() {
                    self.apply_event(
                        HerdrConversationEvent::PaneClosed {
                            pane_id: pane_id.clone(),
                        },
                        window,
                        cx,
                    );
                }
            }
            _ => {}
        }
    }

    pub(crate) fn refresh_from_bridge(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let snapshots = self
            .bridge
            .upgrade()
            .map(|bridge| bridge.read(cx).subthread_snapshots(&self.workspace_id))
            .unwrap_or_default();
        for snapshot in snapshots {
            self.apply_event(
                HerdrConversationEvent::AgentDetected {
                    pane_id: snapshot.pane_id,
                    session: snapshot.session_identity,
                    title: snapshot.title,
                    status: snapshot.status,
                },
                window,
                cx,
            );
        }
    }

    fn apply_event(
        &mut self,
        event: HerdrConversationEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let pane_id = match &event {
            HerdrConversationEvent::AgentDetected { pane_id, .. }
            | HerdrConversationEvent::StatusChanged { pane_id, .. }
            | HerdrConversationEvent::Output { pane_id, .. }
            | HerdrConversationEvent::PaneFocused { pane_id }
            | HerdrConversationEvent::Renamed { pane_id, .. }
            | HerdrConversationEvent::PaneClosed { pane_id } => pane_id.clone(),
        };
        let was_selectable = self.state.is_selectable(&pane_id);
        self.state.apply(event.clone());
        match event {
            HerdrConversationEvent::AgentDetected {
                session: Some(session),
                title,
                status,
                ..
            } => {
                if !was_selectable {
                    let child = cx.new(|cx| {
                        HerdrThreadView::new(
                            self.workspace_id.clone(),
                            pane_id.clone(),
                            session,
                            title.unwrap_or_else(|| pane_id.clone()),
                            status,
                            self.bridge.clone(),
                            window,
                            cx,
                        )
                    });
                    self.subthreads.insert(pane_id.clone(), child.clone());
                    child.update(cx, |child, cx| child.hydrate_output(window, cx));
                } else if let Some(child) = self.subthreads.get(&pane_id) {
                    child.update(cx, |child, cx| {
                        child.apply_metadata(session, title, status, cx);
                    });
                }
            }
            HerdrConversationEvent::AgentDetected { session: None, .. } => {
                // The state demoted this pane; release its child view too.
                self.subthreads.shift_remove(&pane_id);
            }
            HerdrConversationEvent::StatusChanged { status, .. } => {
                if let Some(child) = self.subthreads.get(&pane_id) {
                    child.update(cx, |child, cx| child.apply_status(status, cx));
                }
            }
            HerdrConversationEvent::Output {
                revision, output, ..
            } => {
                if let Some(child) = self.subthreads.get(&pane_id) {
                    child.update(cx, |child, cx| child.apply_output(revision, output, cx));
                }
            }
            HerdrConversationEvent::PaneFocused { .. } => {
                self.active_pane_id = self.state.active_pane_id().map(str::to_string);
                if let Some(child) = self.subthreads.get(&pane_id) {
                    child.read(cx).activation_focus_handle(cx).focus(window, cx);
                }
            }
            HerdrConversationEvent::Renamed { title, .. } => {
                if let Some(child) = self.subthreads.get(&pane_id) {
                    child.update(cx, |child, cx| child.apply_title(title, cx));
                }
            }
            HerdrConversationEvent::PaneClosed { .. } => {
                self.subthreads.shift_remove(&pane_id);
                self.active_pane_id = self.state.active_pane_id().map(str::to_string);
            }
        }
        cx.notify();
    }

    /// User selection requests focus in Herdr. The active pane changes only
    /// after the bridge publishes the authoritative `pane_focused` event.
    pub(crate) fn select_subthread(
        &self,
        pane_id: &str,
        cx: &mut Context<Self>,
    ) -> Option<Task<Result<(), HerdrClientError>>> {
        if !self.controls_enabled() {
            return None;
        }
        let child = self.subthreads.get(pane_id)?;
        Some(child.update(cx, |child, cx| child.request_focus(cx)))
    }

    pub(crate) fn request_rename(
        &self,
        title: &str,
        cx: &mut Context<Self>,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.request_rename_workspace(&self.workspace_id, title, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn request_close(
        &self,
        cx: &mut Context<Self>,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.request_close_workspace(&self.workspace_id, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn activation_focus_handle(&self, cx: &App) -> FocusHandle {
        self.active_pane_id
            .as_ref()
            .and_then(|pane_id| self.subthreads.get(pane_id))
            .map(|child| child.read(cx).activation_focus_handle(cx))
            .unwrap_or_else(|| self.focus_handle.clone())
    }
}

impl Focusable for HerdrConversationView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for HerdrConversationView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let active_child = self
            .active_pane_id
            .as_ref()
            .and_then(|pane_id| self.subthreads.get(pane_id))
            .cloned();
        let controls_enabled = self.controls_enabled();
        let controls_reason = self.controls_disabled_reason();
        let root = cx.entity().downgrade();
        let cards = self.subthreads.iter().map(|(pane_id, child)| {
            let pane_id_for_click = pane_id.clone();
            let root_for_click = root.clone();
            let active = self.active_pane_id.as_deref() == Some(pane_id.as_str());
            let child_ref = child.read(cx);
            Button::new(
                format!("herdr-subthread-{pane_id}"),
                format!("{} · {:?}", child_ref.title, child_ref.status),
            )
            .style(if active {
                ButtonStyle::Tinted(ui::TintColor::Accent)
            } else {
                ButtonStyle::Outlined
            })
            .disabled(!controls_enabled)
            .when_some(controls_reason, |button, reason| {
                button.tooltip(Tooltip::text(reason))
            })
            .on_click(move |_, _window, cx| {
                if let Some(root) = root_for_click.upgrade() {
                    let _ = root.update(cx, |root, cx| root.select_subthread(&pane_id_for_click, cx));
                }
            })
        });
        let status_only = self.state.status_only.iter().map(|(pane_id, status)| {
            Label::new(format!("{pane_id} · {:?}", status))
                .color(Color::Muted)
                .into_any_element()
        });

        v_flex()
            .size_full()
            .track_focus(&self.focus_handle)
            .gap_2()
            .p_2()
            .child(
                h_flex()
                    .justify_between()
                    .child(Label::new(self.title.clone()).size(LabelSize::Large))
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                Label::new(format!("Herdr: {:?}", self.connection_status))
                                    .color(Color::Muted),
                            )
                            .when_some(controls_reason, |this, reason| {
                                this.child(Label::new(reason).color(Color::Muted))
                            }),
                    ),
            )
            .child(v_flex().gap_1().children(cards).children(status_only))
            .child(
                active_child.map_or_else(
                    || {
                        div()
                            .child(Label::new("Select an agent pane to continue").color(Color::Muted))
                            .into_any_element()
                    },
                    |child| child.into_any_element(),
                ),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{TestAppContext, VisualTestContext};
    use crate::herdr_mapping_store::HerdrMappingKey;

    fn agent_detected(pane_id: &str, session: &str) -> HerdrConversationEvent {
        HerdrConversationEvent::AgentDetected {
            pane_id: pane_id.to_string(),
            session: Some(HerdrAgentSessionIdentity::id(session)),
            title: Some("omp".to_string()),
            status: HerdrAgentStatus::Idle,
        }
    }

    fn output(pane_id: &str, revision: u64, text: &str) -> HerdrConversationEvent {
        HerdrConversationEvent::Output {
            pane_id: pane_id.to_string(),
            revision,
            output: text.to_string(),
        }
    }

    #[test]
    fn panes_without_identity_upgrade_when_identity_arrives() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(HerdrConversationEvent::AgentDetected {
            pane_id: "p1".to_string(),
            session: None,
            title: None,
            status: HerdrAgentStatus::Working,
        });
        assert_eq!(view.status_only("p1"), Some(&HerdrAgentStatus::Working));

        view.apply(agent_detected("p1", "session-1"));
        assert!(view.is_selectable("p1"));
        assert_eq!(view.status_only("p1"), None);
    }

    #[test]
    fn identity_loss_demotes_a_selectable_pane_to_status_only() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(agent_detected("p1", "session-1"));
        view.apply(HerdrConversationEvent::PaneFocused {
            pane_id: "p1".to_string(),
        });
        assert!(view.is_selectable("p1"));
        assert_eq!(view.active_pane_id(), Some("p1"));

        // The pane loses its identity; exactly one record (status-only)
        // must remain and the pane must no longer be selectable.
        view.apply(HerdrConversationEvent::AgentDetected {
            pane_id: "p1".to_string(),
            session: None,
            title: None,
            status: HerdrAgentStatus::Blocked,
        });
        assert!(!view.is_selectable("p1"));
        assert_eq!(view.subthreads().len(), 0);
        assert_eq!(view.status_only("p1"), Some(&HerdrAgentStatus::Blocked));
        assert_eq!(view.active_pane_id(), None);
    }

    #[test]
    fn agent_session_identity_creates_a_selectable_subthread() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(agent_detected("p1", "session-1"));
        assert_eq!(view.subthreads().len(), 1);
        assert!(view.is_selectable("p1"));
    }

    #[test]
    fn panes_without_identity_remain_status_only() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(HerdrConversationEvent::AgentDetected {
            pane_id: "p1".to_string(),
            session: None,
            title: None,
            status: HerdrAgentStatus::Working,
        });
        assert!(view.subthreads().is_empty());
        assert!(!view.is_selectable("p1"));
        assert_eq!(view.status_only("p1"), Some(&HerdrAgentStatus::Working));
    }

    #[test]
    fn older_output_revision_cannot_replace_newer_output() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(agent_detected("p1", "session-1"));
        view.apply(output("p1", 4, "new"));
        view.apply(output("p1", 3, "old"));
        assert_eq!(view.output("p1"), Some("new"));
    }

    #[test]
    fn focus_confirmation_activates_child_in_both_directions() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(agent_detected("p1", "session-1"));
        view.apply(agent_detected("p2", "session-2"));
        assert_eq!(view.active_pane_id(), None);
        view.apply(HerdrConversationEvent::PaneFocused {
            pane_id: "p2".to_string(),
        });
        assert_eq!(view.active_pane_id(), Some("p2"));
        view.apply(HerdrConversationEvent::PaneFocused {
            pane_id: "p1".to_string(),
        });
        assert_eq!(view.active_pane_id(), Some("p1"));
    }

    #[test]
    fn status_updates_follow_the_mapped_child() {
        let mut view = HerdrConversationState::new("w1", ThreadId::new());
        view.apply(agent_detected("p1", "session-1"));
        view.apply(HerdrConversationEvent::StatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Blocked,
        });
        assert_eq!(view.status("p1"), Some(&HerdrAgentStatus::Blocked));
    }

    #[gpui::test]
    fn duplicate_agent_detection_refreshes_existing_child_metadata(
        cx: &mut TestAppContext,
    ) {
        crate::test_support::init_test(cx);
        let conversation = cx.add_window(|window, cx| {
            let bridge = cx.new(|_| HerdrThreadBridge::for_test("alpha"));
            HerdrConversationView::new(
                ThreadId::new(),
                "w1",
                "Root",
                bridge,
                window,
                cx,
            )
        });
        let mut cx = VisualTestContext::from_window(conversation.into(), cx);

        let initial_child_id = conversation.update(&mut cx, |view, window, cx| {
            view.apply_event(
                HerdrConversationEvent::AgentDetected {
                    pane_id: "p1".to_string(),
                    session: Some(HerdrAgentSessionIdentity::id("session-old")),
                    title: Some("Old title".to_string()),
                    status: HerdrAgentStatus::Idle,
                },
                window,
                cx,
            );
            let child = view.subthreads.get("p1").expect("initial child").clone();
            child.update(cx, |child, cx| {
                child.apply_output(4, "preserve this output".to_string(), cx);
            });
            child.entity_id()
        }).expect("initial child setup");

        conversation.update(&mut cx, |view, window, cx| {
            view.apply_bridge_event(
                &HerdrBridgeEvent::SubthreadCreated {
                    key: HerdrMappingKey::subthread(
                        "alpha",
                        "w1",
                        "p1",
                        HerdrAgentSessionIdentity::id("session-new"),
                    ),
                    thread_id: ThreadId::new(),
                    pane_id: "p1".to_string(),
                    session: HerdrAgentSessionIdentity::id("session-new"),
                    title: "New title".to_string(),
                    status: HerdrAgentStatus::Blocked,
                },
                window,
                cx,
            );
        }).expect("duplicate child refresh");

        conversation.read_with(&cx, |view, cx| {
            assert_eq!(view.subthreads.len(), 1);
            let child = view.subthreads.get("p1").expect("refreshed child");
            assert_eq!(child.entity_id(), initial_child_id);
            let child = child.read(cx);
            assert_eq!(child.session, HerdrAgentSessionIdentity::id("session-new"));
            assert_eq!(child.title, "New title");
            assert_eq!(child.status, HerdrAgentStatus::Blocked);
            assert_eq!(child.output_revision, 4);
            assert_eq!(child.output, "preserve this output");
        }).expect("child metadata assertions");
    }
    #[test]
    fn herdr_controls_are_enabled_only_when_ready() {
        assert!(!HerdrConnectionStatus::Unavailable.allows_actions());
        assert!(!HerdrConnectionStatus::Reconnecting.allows_actions());
        assert!(!HerdrConnectionStatus::Synchronizing.allows_actions());
        assert!(HerdrConnectionStatus::Ready.allows_actions());
        assert!(HerdrConnectionStatus::Unavailable
            .disabled_reason()
            .is_some());
    }

    #[gpui::test]
    fn root_rename_does_not_clobber_an_explicit_title_override(
        cx: &mut TestAppContext,
    ) {
        crate::test_support::init_test(cx);
        let conversation = cx.add_window(|window, cx| {
            let bridge = cx.new(|_| HerdrThreadBridge::for_test("alpha"));
            HerdrConversationView::new(
                ThreadId::new(),
                "w1",
                "Generated title",
                bridge,
                window,
                cx,
            )
        });
        let mut cx = VisualTestContext::from_window(conversation.into(), cx);
        conversation.update(&mut cx, |view, window, cx| {
            view.title = "User title".into();
            view.title_override = Some("User title".into());
            view.apply_bridge_event(
                &HerdrBridgeEvent::RootRenamed {
                    workspace_id: "w1".to_string(),
                    thread_id: view.thread_id,
                    title: "Generated rename".to_string(),
                },
                window,
                cx,
            );
        })
        .expect("rename should apply");
        conversation
            .read_with(&cx, |view, _| {
                assert_eq!(view.title(), "User title");
            })
            .expect("conversation should still be alive");
    }
}
