use editor::Editor;
use editor::actions::SelectAll;
use futures::channel::oneshot;
use gpui::{
    App, Context, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, IntoElement, Render,
    SharedString, Task, WeakEntity, Window,
};
use menu::{Cancel, Confirm};
use ui::prelude::*;
use util::ResultExt as _;
use workspace::{ModalView, Workspace};

use crate::worktree_names;

/// A focused modal that requires a single portable worktree name.
///
/// Confirming returns the normalized name (or `None` when the user cancels,
/// presses Escape, or dismisses the window). The concrete validation error is
/// shown inline and creation is only dispatched once a valid name survives
/// [`worktree_names::normalize_worktree_name`]; the service re-validates the
/// returned name before path calculation because the UI is not a trust
/// boundary.
pub struct WorktreeNameModal {
    /// Human-readable "create from \<SHA\> in \<repo\>" context shown in the
    /// header so the user knows exactly which commit the worktree will be
    /// based on.
    context_label: Option<SharedString>,
    editor: Entity<Editor>,
    error: Option<SharedString>,
    result: Option<oneshot::Sender<Option<String>>>,
}

impl WorktreeNameModal {
    /// Opens the required-name modal on `workspace` and resolves to the
    /// normalized worktree name, or `None` on cancel/dismissal or if the
    /// workspace expired before the modal could be shown.
    pub fn open(
        workspace: WeakEntity<Workspace>,
        initial_name: Option<String>,
        context_label: Option<SharedString>,
        window: &mut Window,
        cx: &mut App,
    ) -> Task<Option<String>> {
        let (sender, receiver) = oneshot::channel();
        window.spawn(cx, async move |cx| {
            workspace
                .update_in(cx, |workspace, window, cx| {
                    workspace.toggle_modal(window, cx, |window, cx| {
                        WorktreeNameModal::new(initial_name, context_label, sender, window, cx)
                    })
                })
                .log_err();
            receiver.await.ok().flatten()
        })
    }

    fn new(
        initial_name: Option<String>,
        context_label: Option<SharedString>,
        result: oneshot::Sender<Option<String>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Worktree name", window, cx);
            if let Some(name) = initial_name {
                editor.set_text(name, window, cx);
                editor.select_all(&SelectAll, window, cx);
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
        let name = self.editor.read(cx).text(cx);
        match worktree_names::normalize_worktree_name(&name) {
            Ok(normalized_name) => {
                self.result.take().map(|sender| sender.send(Some(normalized_name)));
                cx.emit(DismissEvent);
            }
            Err(error) => {
                self.error = Some(format!("{error:#}").into());
                cx.notify();
            }
        }
    }
}

impl EventEmitter<DismissEvent> for WorktreeNameModal {}
impl ModalView for WorktreeNameModal {}

impl Focusable for WorktreeNameModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.editor.focus_handle(cx)
    }
}

impl Render for WorktreeNameModal {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .key_context("WorktreeNameModal")
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
                    .child(Icon::new(IconName::GitBranch).size(IconSize::XSmall))
                    .child(
                        h_flex()
                            .gap_1()
                            .overflow_x_hidden()
                            .child(Headline::new("Create Detached Worktree").size(HeadlineSize::XSmall)),
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
                        Button::new("confirm", "Create")
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
    use crate::worktree_names;

    #[test]
    fn test_worktree_name_modal_validation_reuses_shared_boundary() {
        // The modal reuses the exact same validator as the service's path
        // calculation, so a name accepted here is always re-validated there.
        for valid in ["feature", "work-🦀", "práce", "分支工作"] {
            assert!(worktree_names::normalize_worktree_name(valid).is_ok());
        }
        for invalid in ["", "   ", "..", "a/b", "CON", "bad\u{0}name", "x?y", "trail."] {
            assert!(
                worktree_names::normalize_worktree_name(invalid).is_err(),
                "{invalid:?} should be rejected before dispatch"
            );
        }
    }

    #[test]
    fn test_worktree_name_modal_confirm_normalizes_before_returning() {
        // Whitespace-collapsed names are what the graph dispatches; the
        // normalized value matches what the modal returns on confirm.
        assert_eq!(
            worktree_names::normalize_worktree_name("  feature   work  ").unwrap(),
            "feature-work"
        );
    }
}