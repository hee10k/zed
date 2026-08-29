use editor::Editor;
use futures::channel::oneshot;
use gpui::{
    App, Context, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, Render,
    SharedString, Task, WeakEntity, Window,
};
use menu::{Cancel, Confirm};
use ui::prelude::*;
use util::ResultExt as _;
use workspace::{ModalView, Workspace};

/// A focused modal that requires a non-empty new message for a stash rename.
///
/// Confirming returns the trimmed message (or `None` when the user cancels,
/// presses Escape, or dismisses the window). The concrete "required" error is
/// shown inline; the backend re-validates the returned message because the UI
/// is not a trust boundary.
pub struct StashRenameModal {
    /// Human-readable target (e.g. "stash@{2}: WIP on main: …") so the user
    /// knows exactly which stash is being renamed.
    context_label: Option<SharedString>,
    editor: Entity<Editor>,
    error: Option<SharedString>,
    result: Option<oneshot::Sender<Option<String>>>,
}

/// Require a non-empty, trimmed stash message. Returning `Ok` means the message
/// is safe to dispatch; the backend re-validates it again because the UI is not
/// a trust boundary.
pub(crate) fn validate_stash_rename_message(text: &str) -> anyhow::Result<String> {
    let trimmed = text.trim();
    anyhow::ensure!(!trimmed.is_empty(), "A stash message is required.");
    Ok(trimmed.to_string())
}

impl StashRenameModal {
    /// Opens the required-message rename modal on `workspace` and resolves to
    /// the new stash message, or `None` on cancel/dismissal or if the workspace
    /// expired before the modal could be shown.
    pub fn open(
        workspace: WeakEntity<Workspace>,
        initial_message: Option<String>,
        context_label: Option<SharedString>,
        window: &mut Window,
        cx: &mut App,
    ) -> Task<Option<String>> {
        let (sender, receiver) = oneshot::channel();
        window.spawn(cx, async move |cx| {
            workspace
                .update_in(cx, |workspace, window, cx| {
                    workspace.toggle_modal(window, cx, |window, cx| {
                        StashRenameModal::new(initial_message, context_label, sender, window, cx)
                    })
                })
                .log_err();
            receiver.await.ok().flatten()
        })
    }

    fn new(
        initial_message: Option<String>,
        context_label: Option<SharedString>,
        result: oneshot::Sender<Option<String>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Stash message", window, cx);
            if let Some(message) = initial_message {
                editor.set_text(message, window, cx);
            }
            editor
        });
        Self {
            context_label,
            editor,
            error: None,
            result: Some(result),
        }
    }

    fn cancel(&mut self, _: &Cancel, _window: &mut Window, cx: &mut Context<Self>) {
        self.result.take();
        cx.emit(DismissEvent);
    }

    fn confirm(&mut self, _: &Confirm, _window: &mut Window, cx: &mut Context<Self>) {
        let text = self.editor.read(cx).text(cx);
        match validate_stash_rename_message(&text) {
            Ok(message) => {
                self.result.take().map(|sender| sender.send(Some(message)));
                cx.emit(DismissEvent);
            }
            Err(error) => {
                self.error = Some(format!("{error:#}").into());
                cx.notify();
            }
        }
    }
}

impl EventEmitter<DismissEvent> for StashRenameModal {}
impl ModalView for StashRenameModal {}

#[cfg(any(test, feature = "test-support"))]
impl StashRenameModal {
        #[allow(dead_code)]
    pub(crate) fn test_set_editor_text(
        &mut self,
        message: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.editor
            .update(cx, |editor, cx| editor.set_text(message.to_owned(), window, cx));
        cx.notify();
    }
}

impl Focusable for StashRenameModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.editor.focus_handle(cx)
    }
}

impl Render for StashRenameModal {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .key_context("StashRenameModal")
            .on_action(cx.listener(Self::cancel))
            .on_action(cx.listener(Self::confirm))
            .elevation_2(cx)
            .w(rems(34.))
            .child(
                h_flex()
                    .px_3()
                    .pt_2()
                    .pb_1()
                    .gap_1p5()
                    .child(Icon::new(IconName::BoxOpen).size(IconSize::XSmall))
                    .child(
                        h_flex()
                            .gap_1()
                            .overflow_x_hidden()
                            .child(
                                Headline::new("Rename Stash")
                                    .size(HeadlineSize::XSmall),
                            ),
                    ),
            )
            .when_some(self.context_label.clone(), |this, label| {
                this.child(
                    div()
                        .px_3()
                        .pb_1()
                        .child(Label::new(label).size(LabelSize::Small).color(Color::Muted)),
                )
            })
            .child(div().px_3().pb_2().child(self.editor.clone()))
            .when_some(self.error.clone(), |this, error| {
                this.child(
                    div()
                        .px_3()
                        .pb_2()
                        .child(Label::new(error).color(Color::Error)),
                )
            })
            .child(
                h_flex()
                    .justify_end()
                    .gap_2()
                    .px_3()
                    .pb_3()
                    .child(
                        Button::new("cancel", "Cancel")
                            .style(ButtonStyle::OutlinedGhost)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.cancel(&Cancel, window, cx);
                            })),
                    )
                    .child(
                        Button::new("confirm", "Rename")
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.confirm(&Confirm, window, cx);
                            })),
                    ),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_stash_rename_message_requires_nonempty() {
        // Empty / whitespace-only messages are rejected before dispatch.
        for invalid in ["", "   ", "\t\n "] {
            assert!(
                validate_stash_rename_message(invalid).is_err(),
                "{invalid:?} must be rejected as required"
            );
        }
        // A real message is trimmed and returned for dispatch.
        assert_eq!(validate_stash_rename_message("  renamed stash  ").unwrap(), "renamed stash");
        assert_eq!(validate_stash_rename_message("WIP on main: keep").unwrap(), "WIP on main: keep");
    }
}