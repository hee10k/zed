//! The explicit herdr session picker.
//!
//! The picker owns only transient UI state. A confirmed selection is emitted
//! after the herdr CLI validates a new name; cancellation never reaches the
//! session registry.

use std::sync::Arc;

use futures::{StreamExt as _, channel::mpsc};
use gpui::{
    App, AppContext as _, AsyncApp, Context, DismissEvent, Entity, EventEmitter, FocusHandle,
    Focusable, Subscription, Task, Window, WindowHandle,
};
use picker::{Picker, PickerDelegate};
use ui::{ListItem, prelude::*};
use workspace::{ModalView, MultiWorkspace, Workspace};

use super::herdr_session_registry::{
    HerdrGateway, HerdrSessionRegistry, SessionPickerSink, SessionSelection,
};

/// What the picker hands the registry after the CLI accepted it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SessionPickerEvent {
    Confirmed(SessionSelection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PickerEntry {
    Session(herdr::SessionInfo),
    NewSession,
}

enum PickerMode {
    Sessions,
    NewName { suggested_name: String },
}

/// A small adapter installed into the registry by `zed::init`.
#[derive(Default)]
pub(crate) struct PickerSink;

impl SessionPickerSink for PickerSink {
    fn show(
        &self,
        window: WindowHandle<MultiWorkspace>,
        registry: Entity<HerdrSessionRegistry>,
        cx: &mut Context<HerdrSessionRegistry>,
    ) {
        let _ = window.update(cx, |multi_workspace, window, cx| {
            let workspace = multi_workspace.workspace().clone();
            workspace.update(cx, |workspace, cx| {
                SessionPicker::show(workspace, registry, window, cx);
            });
        });
    }
}

pub(crate) fn sink() -> std::rc::Rc<dyn SessionPickerSink> {
    std::rc::Rc::new(PickerSink)
}

/// The visible picker wrapper. The inner `Picker` registers its focus handle
/// with the reopenable-picker registry; forwarding that handle here preserves
/// the normal modal-layer behavior.
pub(crate) struct SessionPicker {
    picker: Entity<Picker<SessionPickerDelegate>>,
    _dismiss_subscription: Subscription,
    _confirmations: Task<()>,
}

impl SessionPicker {
    pub(crate) fn show(
        workspace: &mut Workspace,
        registry: Entity<HerdrSessionRegistry>,
        window: &mut Window,
        cx: &mut Context<Workspace>,
    ) {
        let Some(invoking) = window.window_handle().downcast::<MultiWorkspace>() else {
            return;
        };
        let suggested_name = suggested_session_name(workspace, cx);
        workspace.toggle_modal(window, cx, |window, cx| {
            SessionPicker::new(registry, invoking, suggested_name, window, cx)
        });
    }

    fn new(
        registry: Entity<HerdrSessionRegistry>,
        invoking: WindowHandle<MultiWorkspace>,
        suggested_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let gateway = registry.update(cx, |registry, _| registry.gateway()).ok();
        let (selection_tx, mut selection_rx) = mpsc::channel::<SessionSelection>(8);
        let delegate = SessionPickerDelegate::new(gateway, suggested_name, selection_tx);
        let picker = cx.new(|cx| Picker::uniform_list(delegate, window, cx));
        let dismiss_subscription = cx.subscribe(&picker, |_, _, _: &DismissEvent, cx| {
            // No selection event is emitted here. Dismissal therefore leaves
            // the registry untouched, including click-outside and Escape.
            cx.emit(DismissEvent);
        });
        let registry_for_confirm = registry.clone();
        let this = cx.weak_entity();
        let confirmations = cx.spawn(async move |_, cx: &mut AsyncApp| {
            // A modal confirmation is one-shot. The picker may still have
            // queued key events while dismissal is asynchronous, but none
            // may create a second binding/window.
            if let Some(selection) = selection_rx.next().await {
                let _ = registry_for_confirm.update(cx, |registry, cx| {
                    registry.apply_selection_for_window(invoking, selection.clone(), cx);
                });
                let _ = this.update(cx, |_, cx| {
                    cx.emit(SessionPickerEvent::Confirmed(selection));
                    cx.emit(DismissEvent);
                });
            }
        });
        Self {
            picker,
            _dismiss_subscription: dismiss_subscription,
            _confirmations: confirmations,
        }
    }
}

impl Focusable for SessionPicker {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.picker.focus_handle(cx)
    }
}

impl EventEmitter<SessionPickerEvent> for SessionPicker {}
impl EventEmitter<DismissEvent> for SessionPicker {}
impl ModalView for SessionPicker {}

impl Render for SessionPicker {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        self.picker.clone()
    }
}

/// The delegate's entries deliberately contain no synthetic default session.
/// A list failure is represented by the one retry row rendered by
/// `render_match`, not by pretending that a session exists.
pub(crate) struct SessionPickerDelegate {
    gateway: Option<HerdrGateway>,
    mode: PickerMode,
    suggested_name: String,
    sessions: Vec<herdr::SessionInfo>,
    entries: Vec<PickerEntry>,
    list_error: Option<SharedString>,
    validation_error: Option<SharedString>,
    query: String,
    selected_index: usize,
    loading: bool,
    loaded: bool,
    selection_tx: mpsc::Sender<SessionSelection>,
    confirmation_sent: bool,
    _refresh_task: Option<Task<()>>,
    _validation_task: Option<Task<()>>,
}

impl SessionPickerDelegate {
    fn new(
        gateway: Option<HerdrGateway>,
        suggested_name: String,
        selection_tx: mpsc::Sender<SessionSelection>,
    ) -> Self {
        Self {
            gateway,
            mode: PickerMode::Sessions,
            suggested_name,
            sessions: Vec::new(),
            entries: Vec::new(),
            list_error: None,
            validation_error: None,
            query: String::new(),
            selected_index: 0,
            loading: false,
            loaded: false,
            selection_tx,
            confirmation_sent: false,
            _refresh_task: None,
            _validation_task: None,
        }
    }
    fn rebuild_entries(&mut self) {
        if self.list_error.is_some() && matches!(self.mode, PickerMode::Sessions) {
            self.entries.clear();
            self.selected_index = 0;
            return;
        }
        match self.mode {
            PickerMode::Sessions => {
                let query = self.query.to_lowercase();
                self.entries = self
                    .sessions
                    .iter()
                    .filter(|session| query.is_empty() || session.name.to_lowercase().contains(&query))
                    .cloned()
                    .map(PickerEntry::Session)
                    .collect();
                self.entries.push(PickerEntry::NewSession);
            }
            PickerMode::NewName { .. } => {
                self.entries = vec![PickerEntry::NewSession];
            }
        }
        self.selected_index = self
            .selected_index
            .min(self.entries.len().saturating_sub(1));
    }

    fn start_refresh(&mut self, cx: &mut Context<Picker<Self>>) -> Task<()> {
        self.loading = true;
        self.loaded = false;
        self.list_error = None;
        let Some(gateway) = self.gateway.clone() else {
            self.loading = false;
            self.loaded = true;
            self.list_error = Some("herdr executable was not found in PATH".into());
            self.rebuild_entries();
            cx.notify();
            return Task::ready(());
        };
        cx.spawn(async move |picker, cx| {
            let result = gateway.list_sessions().await;
            let _ = picker.update(cx, |picker, cx| {
                picker.delegate.loading = false;
                picker.delegate.loaded = true;
                match result {
                    Ok(sessions) => {
                        picker.delegate.sessions = sessions;
                        picker.delegate.list_error = None;
                    }
                    Err(error) => {
                        picker.delegate.sessions.clear();
                        picker.delegate.list_error = Some(error.to_string().trim().to_owned().into());
                    }
                }
                picker.delegate.rebuild_entries();
                cx.notify();
            });
        })
    }

    fn enter_new_name(&mut self) -> String {
        let suggested = self.suggested_name.clone();
        self.mode = PickerMode::NewName {
            suggested_name: suggested.clone(),
        };
        self.list_error = None;
        self.validation_error = None;
        self.rebuild_entries();
        suggested
    }

    fn selected_entry(&self) -> Option<&PickerEntry> {
        self.entries.get(self.selected_index)
    }

    fn validate_name(&mut self, name: String, cx: &mut Context<Picker<Self>>) {
        if self.confirmation_sent {
            return;
        }
        let trimmed = name.trim().to_owned();
        if trimmed.is_empty() {
            self.validation_error = Some("Session name cannot be empty".into());
            cx.notify();
            return;
        }
        let Some(gateway) = self.gateway.clone() else {
            self.validation_error = Some("herdr executable was not found in PATH".into());
            cx.notify();
            return;
        };
        self.confirmation_sent = true;
        self._validation_task = Some(cx.spawn(async move |picker, cx| {
            let result = gateway.validate_session_name(trimmed.clone()).await;
            let _ = picker.update(cx, |picker, cx| match result {
                Ok(()) => {
                    picker.delegate.validation_error = None;
                    if picker
                        .delegate
                        .selection_tx
                        .try_send(SessionSelection::New { name: trimmed })
                        .is_err()
                    {
                        picker.delegate.confirmation_sent = false;
                    }
                }
                Err(error) => {
                    picker.delegate.confirmation_sent = false;
                    // The CLI's stderr is the source of truth for grammar;
                    // preserve its trimmed message in the open modal.
                    picker.delegate.validation_error =
                        Some(error.to_string().trim().to_owned().into());
                    cx.notify();
                }
            });
        }));
    }
}

impl PickerDelegate for SessionPickerDelegate {
    type ListItem = ui::ListItem;

    fn name() -> &'static str {
        "herdr-session"
    }

    fn match_count(&self) -> usize {
        if self.list_error.is_some() && matches!(self.mode, PickerMode::Sessions) {
            1
        } else {
            self.entries.len()
        }
    }

    fn selected_index(&self) -> usize {
        self.selected_index
    }

    fn set_selected_index(
        &mut self,
        ix: usize,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) {
        self.selected_index = ix.min(self.match_count().saturating_sub(1));
    }

    fn can_select(
        &self,
        ix: usize,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) -> bool {
        ix < self.match_count()
    }

    fn placeholder_text(&self, _window: &mut Window, _cx: &mut App) -> Arc<str> {
        match self.mode {
            PickerMode::Sessions => "Select a herdr session".into(),
            PickerMode::NewName { .. } => "New herdr session name".into(),
        }
    }

    fn no_matches_text(&self, _window: &mut Window, _cx: &mut App) -> Option<SharedString> {
        if self.list_error.is_some() {
            None
        } else {
            Some("No sessions found".into())
        }
    }

    fn update_matches(
        &mut self,
        query: String,
        _window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Task<()> {
        self.query = query;
        self.rebuild_entries();
        if matches!(self.mode, PickerMode::Sessions) && !self.loaded && !self.loading {
            return self.start_refresh(cx);
        }
        cx.notify();
        Task::ready(())
    }

    fn confirm_update_query(
        &mut self,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) -> Option<String> {
        if matches!(self.mode, PickerMode::Sessions)
            && matches!(self.selected_entry(), Some(PickerEntry::NewSession))
        {
            Some(self.enter_new_name())
        } else {
            None
        }
    }

    fn confirm(
        &mut self,
        _secondary: bool,
        _window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        if self.confirmation_sent {
            return;
        }
        if self.list_error.is_some() && matches!(self.mode, PickerMode::Sessions) {
            self._refresh_task = Some(self.start_refresh(cx));
            return;
        }
        match (&self.mode, self.selected_entry()) {
            (PickerMode::Sessions, Some(PickerEntry::Session(session))) => {
                if self
                    .selection_tx
                    .try_send(SessionSelection::Existing(session.clone()))
                    .is_ok()
                {
                    self.confirmation_sent = true;
                }
            }
            (PickerMode::NewName { .. }, Some(PickerEntry::NewSession)) => {
                self.validate_name(self.query.clone(), cx);
            }
            _ => {}
        }
    }

    fn dismissed(&mut self, _window: &mut Window, _cx: &mut Context<Picker<Self>>) {}

    fn render_header(
        &self,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) -> Option<AnyElement> {
        self.validation_error
            .as_ref()
            .or(self.list_error.as_ref())
            .map(|message| {
                div()
                    .px_2()
                    .py_1()
                    .child(Label::new(message.clone()).color(Color::Error))
                    .into_any_element()
            })
    }

    fn render_match(
        &self,
        ix: usize,
        selected: bool,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) -> Option<Self::ListItem> {
        let label = if self.list_error.is_some() && matches!(self.mode, PickerMode::Sessions) {
            "Retry".to_owned()
        } else {
            match self.entries.get(ix)? {
                PickerEntry::Session(session) => picker_entry_label(&session.name, session.running),
                PickerEntry::NewSession => "New session…".to_owned(),
            }
        };
        Some(
            ListItem::new(ix)
                .inset(true)
                .toggle_state(selected)
                .child(Label::new(label)),
        )
    }
}
pub(crate) fn picker_entry_label(name: &str, running: bool) -> String {
    if running {
        name.to_owned()
    } else {
        format!("{name} · stopped")
    }
}

fn suggested_session_name(workspace: &Workspace, cx: &App) -> String {
    workspace
        .project()
        .read(cx)
        .first_project_directory(cx)
        .and_then(|path| path.file_name().and_then(|name| name.to_str()).map(str::to_owned))
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "session".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{PickerEntry, SessionPickerDelegate, picker_entry_label};
    use futures::channel::mpsc;
    use picker::PickerDelegate;

    fn session(name: &str, running: bool) -> herdr::SessionInfo {
        herdr::SessionInfo {
            name: name.to_owned(),
            is_default: false,
            running,
            session_dir: std::path::PathBuf::from(format!("/sessions/{name}")),
            socket_path: std::path::PathBuf::from(format!("/sessions/{name}/herdr.sock")),
        }
    }

    #[test]
    fn sessions_mode_keeps_running_stopped_and_new_entries() {
        let (tx, _rx) = mpsc::channel(1);
        let mut delegate = SessionPickerDelegate::new(None, "suggested".to_owned(), tx);
        delegate.sessions = vec![session("running", true), session("stopped", false)];
        delegate.rebuild_entries();
        assert_eq!(delegate.match_count(), 3);
        assert!(matches!(delegate.entries[0], PickerEntry::Session(_)));
        assert!(matches!(delegate.entries[1], PickerEntry::Session(_)));
        assert!(matches!(delegate.entries[2], PickerEntry::NewSession));
    }

    #[test]
    fn list_error_exposes_one_retry_match() {
        let (tx, _rx) = mpsc::channel(1);
        let mut delegate = SessionPickerDelegate::new(None, "suggested".to_owned(), tx);
        delegate.list_error = Some("list failed".into());
        delegate.rebuild_entries();
        assert_eq!(delegate.match_count(), 1);
        assert_eq!(picker_entry_label("stopped", false), "stopped · stopped");
    }

    #[test]
    fn stopped_sessions_have_a_visible_indicator() {
        assert_eq!(picker_entry_label("main", false), "main · stopped");
        assert_eq!(picker_entry_label("main", true), "main");
    }
}
