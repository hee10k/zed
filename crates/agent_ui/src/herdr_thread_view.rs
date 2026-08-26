use editor::{Editor, EditorMode, MultiBuffer};
use gpui::{
    App, Context, Entity, FocusHandle, Focusable, InteractiveElement, Render, SharedString, Task,
    WeakEntity, Window, prelude::*,
};
use language::Buffer;
use ui::{Button, ButtonStyle, Label, Tooltip, prelude::*};


use crate::herdr_bridge::{HerdrConnectionStatus, HerdrThreadBridge};
use crate::herdr_client::{
    HerdrAgentSessionIdentity, HerdrAgentSnapshot, HerdrAgentStatus, HerdrClientError,
};

/// A selectable Herdr agent pane. Its identity is a Herdr identity, never an
/// ACP session id, and all side effects go through `HerdrThreadBridge`.
pub(crate) struct HerdrThreadView {
    pub(crate) pane_id: String,
    pub(crate) session: HerdrAgentSessionIdentity,
    pub(crate) title: SharedString,
    pub(crate) status: HerdrAgentStatus,
    pub(crate) output: String,
    pub(crate) output_revision: u64,
    pub(crate) focus_handle: FocusHandle,
    workspace_id: String,
    bridge: WeakEntity<HerdrThreadBridge>,
    prompt_editor: Entity<Editor>,
    error: Option<SharedString>,
}

impl HerdrThreadView {
    pub(crate) fn new(
        workspace_id: String,
        pane_id: String,
        session: HerdrAgentSessionIdentity,
        title: String,
        status: HerdrAgentStatus,
        bridge: WeakEntity<HerdrThreadBridge>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let buffer = cx.new(|cx| {
            MultiBuffer::singleton(cx.new(|cx| Buffer::local(String::new(), cx)), cx)
        });
        let prompt_editor = cx.new(|cx| {
            let mut editor = Editor::new(
                EditorMode::AutoHeight {
                    min_lines: 1,
                    max_lines: Some(6),
                },
                buffer,
                None,
                window,
                cx,
            );
            editor.set_placeholder_text("Prompt this Herdr agent…", window, cx);
            editor.set_soft_wrap();
            editor
        });
        Self {
            pane_id,
            session,
            title: title.into(),
            status,
            output: String::new(),
            output_revision: 0,
            focus_handle: cx.focus_handle(),
            workspace_id,
            bridge,
            prompt_editor,
            error: None,
        }
    }
    fn connection_status(&self, cx: &App) -> HerdrConnectionStatus {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.read(cx).status())
            .unwrap_or(HerdrConnectionStatus::Unavailable)
    }

    pub(crate) fn controls_enabled(&self, cx: &App) -> bool {
        self.connection_status(cx).allows_actions()
    }

    pub(crate) fn controls_disabled_reason(&self, cx: &App) -> Option<&'static str> {
        self.connection_status(cx).disabled_reason()
    }


    pub(crate) fn activation_focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }

    pub(crate) fn apply_status(&mut self, status: HerdrAgentStatus, cx: &mut Context<Self>) {
        self.status = status;
        cx.notify();
    }

    pub(crate) fn apply_title(&mut self, title: String, cx: &mut Context<Self>) {
        if !title.trim().is_empty() {
            self.title = title.into();
            cx.notify();
        }
    }

    pub(crate) fn apply_metadata(
        &mut self,
        session: HerdrAgentSessionIdentity,
        title: Option<String>,
        status: HerdrAgentStatus,
        cx: &mut Context<Self>,
    ) {
        self.session = session;
        if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
            self.title = title.into();
        }
        self.status = status;
        cx.notify();
    }

    pub(crate) fn apply_output(&mut self, revision: u64, output: String, cx: &mut Context<Self>) {
        if revision > self.output_revision {
            self.output_revision = revision;
            self.output = output;
            cx.notify();
        }
    }

    pub(crate) fn apply_snapshot(&mut self, snapshot: HerdrAgentSnapshot, cx: &mut Context<Self>) {
        if let Some(identity) = snapshot.session_identity {
            self.session = identity;
        }
        if let Some(title) = snapshot.title {
            self.apply_title(title, cx);
        }
        self.status = snapshot.status;
        cx.notify();
    }

    pub(crate) fn request_focus(
        &self,
        cx: &mut App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .and_then(|bridge| {
                bridge.update(cx, |bridge, cx| {
                    bridge.focus_pane_in_context(&self.workspace_id, &self.pane_id, cx)
                })
            })
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn request_prompt(
        &self,
        prompt: &str,
        cx: &mut App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.prompt_agent(&self.pane_id, prompt, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn request_cancel(
        &self,
        cx: &mut App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.cancel_agent(&self.pane_id, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn request_rename(
        &self,
        title: Option<&str>,
        cx: &mut App,
    ) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.rename_agent(&self.pane_id, title, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn request_close(
        &self,
        cx: &mut App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.bridge
            .upgrade()
            .map(|bridge| bridge.update(cx, |bridge, cx| bridge.close_pane(&self.pane_id, cx)))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn hydrate_output(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(bridge) = self.bridge.upgrade() else {
            return;
        };
        let pane_id = self.pane_id.clone();
        let since_revision = self.output_revision;
        let task = bridge.update(cx, |bridge, cx| {
            bridge.read_pane_output(&pane_id, Some(since_revision), cx)
        });
        let _ = cx.spawn_in(window, async move |this, cx| {
            match task.await {
                Ok((revision, output)) => {
                    let _ = this.update(cx, |view, cx| view.apply_output(revision, output, cx));
                }
                Err(error) => {
                    let _ = this.update(cx, |view, cx| {
                        view.error = Some(format!("Unable to read pane output: {error}").into());
                        cx.notify();
                    });
                }
            }
        });
    }
    fn send_prompt(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(reason) = self.controls_disabled_reason(cx) {
            self.error = Some(reason.into());
            cx.notify();
            return;
        }
        let prompt = self.prompt_editor.read(cx).text(cx);
        if prompt.trim().is_empty() {
            return;
        }
        let task = self.request_prompt(&prompt, cx);
        let editor = self.prompt_editor.clone();
        cx.spawn_in(window, async move |this, cx| {
            match task.await {
                Ok(()) => {
                    this.update_in(cx, |view, window, cx| {
                        editor.update(cx, |editor, cx| editor.set_text("", window, cx));
                        view.error = None;
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    this.update_in(cx, |view, _window, cx| {
                        view.error = Some(format!("Prompt failed: {error}").into());
                        cx.notify();
                    })?;
                }
            }
            anyhow::Ok(())
        })
        .detach();
    }

    fn send_cancel(&mut self, cx: &mut Context<Self>) {
        if let Some(reason) = self.controls_disabled_reason(cx) {
            self.error = Some(reason.into());
            cx.notify();
            return;
        }
        let task = self.request_cancel(cx);
        cx.spawn(async move |this, cx| {
            if let Err(error) = task.await {
                this.update(cx, |view, cx| {
                    view.error = Some(format!("Cancel failed: {error}").into());
                    cx.notify();
                })?;
            }
            anyhow::Ok(())
        })
        .detach();
    }
    fn send_close(&mut self, cx: &mut Context<Self>) {
        if let Some(reason) = self.controls_disabled_reason(cx) {
            self.error = Some(reason.into());
            cx.notify();
            return;
        }
        let task = self.request_close(cx);
        cx.spawn(async move |this, cx| {
            if let Err(error) = task.await {
                this.update(cx, |view, cx| {
                    view.error = Some(format!("Close failed: {error}").into());
                    cx.notify();
                })?;
            }
            anyhow::Ok(())
        })
        .detach();
    }
}

impl Focusable for HerdrThreadView {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for HerdrThreadView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let pane_id = self.pane_id.clone();
        let session = format!("{}:{}", self.session.kind, self.session.value);
        let connection_status = self.connection_status(cx);
        let controls_enabled = connection_status.allows_actions();
        let controls_reason = connection_status.disabled_reason();
        v_flex()
            .id(format!("herdr-thread-{pane_id}"))
            .flex_1()
            .min_h_0()
            .track_focus(&self.focus_handle)
            .gap_2()
            .p_2()
            .border_1()
            .border_color(cx.theme().colors().border_variant)
            .child(
                h_flex()
                    .justify_between()
                    .child(Label::new(self.title.clone()))
                    .child(Label::new(format!("{:?}", self.status)).color(Color::Muted)),
            )
            .child(
                h_flex()
                    .gap_1()
                    .child(Label::new(format!("Herdr: {:?}", connection_status)).color(Color::Muted))
                    .when_some(controls_reason, |this, reason| {
                        this.child(Label::new(reason).color(Color::Muted))
                    }),
            )
            .child(Label::new(format!("Pane {pane_id} · {session}")).color(Color::Muted))
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .child(Label::new(self.output.clone())),
            )
            .child(
                Button::new("herdr-close", "Close")
                    .style(ButtonStyle::Outlined)
                    .disabled(!controls_enabled)
                    .when_some(controls_reason, |button, reason| {
                        button.tooltip(Tooltip::text(reason))
                    })
                    .on_click(cx.listener(|this, _, _, cx| this.send_close(cx))),
            )
            .child(self.prompt_editor.clone())
            .child(
                h_flex()
                    .gap_1()
                    .justify_end()
                    .child(
                        Button::new("herdr-cancel", "Cancel")
                            .style(ButtonStyle::Outlined)
                            .disabled(!controls_enabled)
                            .when_some(controls_reason, |button, reason| {
                                button.tooltip(Tooltip::text(reason))
                            })
                            .on_click(cx.listener(|this, _, _, cx| this.send_cancel(cx))),
                    )
                    .child(
                        Button::new("herdr-send", "Send")
                            .style(ButtonStyle::Tinted(ui::TintColor::Accent))
                            .disabled(!controls_enabled)
                            .when_some(controls_reason, |button, reason| {
                                button.tooltip(Tooltip::text(reason))
                            })
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.send_prompt(window, cx)
                            })),
                    ),
            )
            .when_some(self.error.clone(), |this, error| {
                this.child(Label::new(error).color(Color::Error))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> crate::herdr_conversation_view::HerdrSubthreadState {
        crate::herdr_conversation_view::HerdrSubthreadState {
            pane_id: "p1".to_string(),
            session: HerdrAgentSessionIdentity::id("s1"),
            title: "omp".into(),
            status: HerdrAgentStatus::Idle,
            output: String::new(),
            output_revision: 0,
        }
    }

    #[test]
    fn output_revision_is_monotonic() {
        let mut state = state();
        state.apply_output(4, "new".to_string());
        state.apply_output(3, "old".to_string());
        assert_eq!(state.output_revision, 4);
        assert_eq!(state.output, "new");
    }

    #[test]
    fn status_and_identity_are_kept_separate_from_acp() {
        let state = state();
        assert_eq!(state.session.kind, "id");
        assert_eq!(state.status, HerdrAgentStatus::Idle);
    }
}
