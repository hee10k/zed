use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use fs::Fs;
use git::repository::delete_branch_flag;
use gpui::{
    App, Context, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, InteractiveElement,
    IntoElement, ParentElement, PromptLevel, Render, SharedString, Task, WeakEntity,
    Window,
};
use project::git_store::Repository;
use ui::{Button, Checkbox, Headline, HeadlineSize, Label, prelude::*};
use workspace::Workspace;

use crate::notifications::show_error_toast;
use crate::worktree_service::{
    HostScopedRepositoryIdentity, app_workspaces_with_active_window,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DeleteOutcome {
    Deleted,
    Cancelled,
}

#[derive(Debug)]
pub enum WorktreeRemovalOutcome {
    Removed { branch_error: Option<anyhow::Error> },
    BlockedOpen,
    Cancelled,
}

impl PartialEq for WorktreeRemovalOutcome {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Removed { branch_error: a }, Self::Removed { branch_error: b }) => {
                a.as_ref().map(|e| e.to_string()) == b.as_ref().map(|e| e.to_string())
            }
            (Self::BlockedOpen, Self::BlockedOpen) => true,
            (Self::Cancelled, Self::Cancelled) => true,
            _ => false,
        }
    }
}

fn delete_branch_command(is_remote: bool, branch_name: &str, force: bool) -> String {
    format!(
        "branch {} {branch_name}",
        delete_branch_flag(is_remote, force)
    )
}

fn remove_worktree_command(path: &Path, force: bool) -> String {
    if force {
        format!("worktree remove --force {}", path.display())
    } else {
        format!("worktree remove {}", path.display())
    }
}

struct BranchDeleteForceDeletePrompt {
    required_error_substrings: &'static [&'static str],
    message: fn(&str) -> String,
}

impl BranchDeleteForceDeletePrompt {
    fn matches(&self, normalized_error_message: &str) -> bool {
        self.required_error_substrings
            .iter()
            .all(|substring| normalized_error_message.contains(substring))
    }
}

const BRANCH_DELETE_FORCE_DELETE_PROMPTS: &[BranchDeleteForceDeletePrompt] =
    &[BranchDeleteForceDeletePrompt {
        required_error_substrings: &["not fully merged"],
        message: unmerged_branch_force_delete_prompt,
    }];

fn unmerged_branch_force_delete_prompt(branch_name: &str) -> String {
    format!("Branch \"{branch_name}\" is not fully merged. Force delete it?")
}

fn force_delete_prompt_for_branch_delete_error(
    error: &anyhow::Error,
    branch_name: &str,
) -> Option<String> {
    let normalized_error_message = error.to_string().to_lowercase();
    BRANCH_DELETE_FORCE_DELETE_PROMPTS
        .iter()
        .find(|prompt| prompt.matches(&normalized_error_message))
        .map(|prompt| (prompt.message)(branch_name))
}

struct WorktreeRemoveForceDeletePrompt {
    required_error_substrings: &'static [&'static str],
    message: fn(&str) -> String,
}

impl WorktreeRemoveForceDeletePrompt {
    fn matches(&self, normalized_error_message: &str) -> bool {
        self.required_error_substrings
            .iter()
            .all(|substring| normalized_error_message.contains(substring))
    }
}

const WORKTREE_REMOVE_FORCE_DELETE_PROMPTS: &[WorktreeRemoveForceDeletePrompt] =
    &[WorktreeRemoveForceDeletePrompt {
        required_error_substrings: &[
            "contains modified or untracked files",
            "use --force to delete it",
        ],
        message: dirty_worktree_force_delete_prompt,
    }];

fn dirty_worktree_force_delete_prompt(display_name: &str) -> String {
    format!("Worktree \"{display_name}\" contains modified or untracked files. Force delete it?")
}

fn force_delete_prompt_for_worktree_remove_error(
    error: &anyhow::Error,
    display_name: &str,
) -> Option<String> {
    let normalized_error_message = error.to_string().to_lowercase();
    WORKTREE_REMOVE_FORCE_DELETE_PROMPTS
        .iter()
        .find(|prompt| prompt.matches(&normalized_error_message))
        .map(|prompt| (prompt.message)(display_name))
}

pub fn delete_branch(
    repository: Entity<Repository>,
    is_remote_tracking_ref: bool,
    branch_name: String,
    display_name: SharedString,
    force: bool,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Task<Result<DeleteOutcome>> {
    window.spawn(cx, async move |cx| {
        let initial_result = repository
            .update(cx, |repo, _| {
                repo.delete_branch(is_remote_tracking_ref, branch_name.clone(), force)
            })
            .await?;

        let (result, attempted_force) = match initial_result {
            Ok(()) => (Ok(()), force),
            Err(error) => {
                let force_delete_prompt = (!force)
                    .then(|| force_delete_prompt_for_branch_delete_error(&error, &display_name))
                    .flatten();

                if let Some(prompt_message) = force_delete_prompt {
                    let answer = cx.update(|window, cx| {
                        window.prompt(
                            PromptLevel::Warning,
                            &prompt_message,
                            None,
                            &["Force Delete", "Cancel"],
                            cx,
                        )
                    })?;

                    if answer.await != Ok(0) {
                        return Ok(DeleteOutcome::Cancelled);
                    }

                    let retry = repository
                        .update(cx, |repo, _| {
                            repo.delete_branch(is_remote_tracking_ref, branch_name.clone(), true)
                        })
                        .await?;

                    if let Err(error) = &retry {
                        log::error!("Failed to force delete branch: {error}");
                    }
                    (retry, true)
                } else {
                    (Err(error), force)
                }
            }
        };

        if let Err(error) = result {
            if let Some(workspace) = workspace.upgrade() {
                let error_msg = error.to_string();
                cx.update(|_window, cx| {
                    show_error_toast(
                        workspace,
                        delete_branch_command(
                            is_remote_tracking_ref,
                            &display_name,
                            attempted_force,
                        ),
                        anyhow!("{error_msg}"),
                        cx,
                    )
                })?;
            }
            return Err(error);
        }

        Ok(DeleteOutcome::Deleted)
    })
}

pub fn worktree_is_open(
    identity: HostScopedRepositoryIdentity,
    path: PathBuf,
    fs: Arc<dyn Fs>,
    cx: &App,
) -> Task<Result<bool>> {
    worktree_is_open_in_window(None, identity, path, fs, cx)
}

pub fn worktree_is_open_in_window(
    active_window: Option<&Window>,
    identity: HostScopedRepositoryIdentity,
    path: PathBuf,
    fs: Arc<dyn Fs>,
    cx: &App,
) -> Task<Result<bool>> {
    let workspaces = match app_workspaces_with_active_window(active_window, cx) {
        Ok(workspaces) => workspaces,
        Err(err) => return Task::ready(Err(err)),
    };

    let is_local = identity.host_key == "local";
    let mut open_paths_matching_repo: Vec<(PathBuf, util::paths::PathStyle)> = Vec::new();

    for workspace in workspaces {
        let (repos, visible_worktrees, remote_options, path_style) =
            workspace.read_with(cx, |ws, cx| {
                let project = ws.project().clone();
                let remote_options = project.read(cx).remote_connection_options(cx);
                let path_style = project.read(cx).path_style(cx);
                let repos = project.read(cx).repositories(cx).clone();
                let visible_worktrees = project
                    .read(cx)
                    .visible_worktrees(cx)
                    .map(|wt| wt.read(cx).abs_path().to_path_buf())
                    .collect::<Vec<_>>();
                (repos, visible_worktrees, remote_options, path_style)
            });

        for open_wt_path in visible_worktrees {
            let matching_repo = repos.iter().find_map(|(_id, repo)| {
                let repo_ref = repo.read(cx);
                let work_dir = &repo_ref.work_directory_abs_path;
                if open_wt_path.starts_with(work_dir.as_ref()) {
                    Some(repo_ref.common_repository_identity())
                } else {
                    None
                }
            });

            if let Some(common_identity) = matching_repo {
                let repo_host_identity =
                    HostScopedRepositoryIdentity::new(common_identity, remote_options.as_ref());
                if repo_host_identity == identity {
                    open_paths_matching_repo.push((open_wt_path, path_style));
                }
            }
        }
    }

    if open_paths_matching_repo.is_empty() {
        return Task::ready(Ok(false));
    }

    if is_local {
        let background_executor = cx.background_executor().clone();
        background_executor.spawn(async move {
            let Ok(canonical_target) = fs.canonicalize(&path).await else {
                return Ok(false);
            };

            for (open_wt_path, _) in open_paths_matching_repo {
                let Ok(canonical_open) = fs.canonicalize(&open_wt_path).await else {
                    continue;
                };
                if canonical_open == canonical_target {
                    return Ok(true);
                }
            }

            Ok(false)
        })
    } else {
        for (open_wt_path, path_style) in open_paths_matching_repo {
            let open_str = open_wt_path.to_str().unwrap_or("");
            let target_str = path.to_str().unwrap_or("");
            if open_wt_path == path
                || path_style.normalize(open_str) == path_style.normalize(target_str)
            {
                return Task::ready(Ok(true));
            }
        }
        Task::ready(Ok(false))
    }
}

pub struct WorktreeRemovalConfirmModal {
    display_name: SharedString,
    path: PathBuf,
    is_blocked: bool,
    blocked_reason: Option<SharedString>,
    linked_branch: Option<String>,
    delete_linked_branch: bool,
    focus_handle: FocusHandle,
    tx: Option<futures::channel::oneshot::Sender<Result<ModalResult>>>,
}

#[derive(Debug)]
enum ModalResult {
    Confirmed { delete_linked_branch: bool },
    Cancelled,
    BlockedAcknowledged,
}

impl EventEmitter<DismissEvent> for WorktreeRemovalConfirmModal {}

impl Focusable for WorktreeRemovalConfirmModal {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl workspace::ModalView for WorktreeRemovalConfirmModal {}

impl Render for WorktreeRemovalConfirmModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let is_blocked = self.is_blocked;
        let display_name = self.display_name.clone();
        let path = self.path.clone();
        let delete_linked_branch = self.delete_linked_branch;
        let linked_branch = self.linked_branch.clone();

        v_flex()
            .key_context("WorktreeRemovalConfirmModal")
            .elevation_3(cx)
            .p_4()
            .gap_4()
            .w(rems(28.))
            .track_focus(&self.focus_handle)
            .on_action(cx.listener(|this, _: &menu::Cancel, window, cx| {
                this.cancel(window, cx);
            }))
            .on_action(cx.listener(|this, _: &menu::Confirm, window, cx| {
                this.confirm(window, cx);
            }))
            .child(
                v_flex()
                    .gap_2()
                    .child(
                        Headline::new(if is_blocked {
                            "Cannot Remove Worktree"
                        } else {
                            "Remove Worktree"
                        })
                        .size(HeadlineSize::Small),
                    )
                    .child(if is_blocked {
                        Label::new(self.blocked_reason.clone().unwrap_or_else(|| {
                            format!(
                                "Worktree \"{display_name}\" is currently open in a Zed window or is the main worktree and cannot be removed. Please close or switch away from this worktree first."
                            )
                            .into()
                        }))
                        .color(Color::Muted)
                    } else {
                        Label::new(format!(
                            "Are you sure you want to remove the worktree \"{display_name}\" at {}?",
                            path.display()
                        ))
                        .color(Color::Default)
                    }),
            )
            .when(!is_blocked && linked_branch.is_some(), |this| {
                let branch_name = linked_branch.unwrap();
                this.child(
                    Checkbox::new(
                        "also-delete-linked-branch",
                        if delete_linked_branch {
                            ToggleState::Selected
                        } else {
                            ToggleState::Unselected
                        },
                    )
                    .label(format!("Also delete linked branch \"{branch_name}\""))
                    .on_click(cx.listener(|this, _selection, _window, cx| {
                        this.delete_linked_branch = !this.delete_linked_branch;
                        cx.notify();
                    })),
                )
            })
            .child(
                h_flex()
                    .justify_end()
                    .gap_2()
                    .when(is_blocked, |this| {
                        this.child(
                            Button::new("ok", "OK")
                                .style(ButtonStyle::Filled)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.confirm(window, cx);
                                })),
                        )
                    })
                    .when(!is_blocked, |this| {
                        this.child(Button::new("cancel", "Cancel").on_click(cx.listener(
                            |this, _, window, cx| {
                                this.cancel(window, cx);
                            },
                        )))
                        .child(
                            Button::new("remove", "Remove Worktree")
                                .style(ButtonStyle::Filled)
                                .color(Color::Error)
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.confirm(window, cx);
                                })),
                        )
                    }),
            )
    }
}

impl WorktreeRemovalConfirmModal {
    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(tx) = self.tx.take() {
            if self.is_blocked {
                let _ = tx.send(Ok(ModalResult::BlockedAcknowledged));
            } else {
                let _ = tx.send(Ok(ModalResult::Confirmed {
                    delete_linked_branch: self.delete_linked_branch,
                }));
            }
        }
        cx.emit(DismissEvent);
    }

    fn cancel(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Ok(ModalResult::Cancelled));
        }
        cx.emit(DismissEvent);
    }
}

pub fn confirm_remove_worktree(
    repository: Entity<Repository>,
    identity: HostScopedRepositoryIdentity,
    worktree: git::repository::Worktree,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Task<Result<WorktreeRemovalOutcome>> {
    let fs = <dyn Fs>::global(cx);
    let path = worktree.path.clone();
    let is_main = worktree.is_main;
    let display_name: SharedString = worktree
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("worktree")
        .to_string()
        .into();

    let linked_branch = worktree.ref_name.as_ref().and_then(|r| {
        let name = r.strip_prefix("refs/heads/").unwrap_or(r.as_ref());
        if !r.starts_with("refs/remotes/") && r.as_ref() != "HEAD" {
            Some(name.to_string())
        } else {
            None
        }
    });

    window.spawn(cx, async move |cx| {
        let is_open = cx.update(|window, cx| worktree_is_open_in_window(Some(window), identity.clone(), path.clone(), fs.clone(), cx))?.await?;

        let is_blocked = is_main || is_open;
        let blocked_reason = if is_main {
            Some("Main worktree cannot be removed.".into())
        } else if is_open {
            Some(
                format!(
                    "Worktree \"{display_name}\" is currently open in a Zed window and cannot be removed. Please close or switch away from this worktree first."
                )
                .into(),
            )
        } else {
            None
        };

        let (tx, rx) = futures::channel::oneshot::channel::<Result<ModalResult>>();

        if let Some(workspace_entity) = workspace.upgrade() {
            workspace_entity.update_in(cx, |workspace, window, cx| {
                workspace.toggle_modal(window, cx, |_window, cx| {
                    let focus_handle = cx.focus_handle();
                    WorktreeRemovalConfirmModal {
                        display_name: display_name.clone(),
                        path: path.clone(),
                        is_blocked,
                        blocked_reason,
                        linked_branch: linked_branch.clone(),
                        delete_linked_branch: false,
                        focus_handle,
                        tx: Some(tx),
                    }
                })
            })?;
        } else {
            return Ok(WorktreeRemovalOutcome::Cancelled);
        }

        let modal_result = rx.await.unwrap_or(Ok(ModalResult::Cancelled))?;

        let delete_linked_branch = match modal_result {
            ModalResult::BlockedAcknowledged => return Ok(WorktreeRemovalOutcome::BlockedOpen),
            ModalResult::Cancelled => return Ok(WorktreeRemovalOutcome::Cancelled),
            ModalResult::Confirmed {
                delete_linked_branch,
            } => delete_linked_branch,
        };

        let is_open_now =
            cx.update(|window, cx| worktree_is_open_in_window(Some(window), identity.clone(), path.clone(), fs.clone(), cx))?.await?;

        let refreshed_worktrees = repository
            .update(cx, |repo, _| repo.worktrees())
            .await??;

        let matching_refreshed = refreshed_worktrees.iter().find(|wt| wt.path == path);

        let Some(matching_wt) = matching_refreshed else {
            return Ok(WorktreeRemovalOutcome::BlockedOpen);
        };

        if matching_wt.is_main || is_open_now {
            return Ok(WorktreeRemovalOutcome::BlockedOpen);
        }

        let refreshed_linked_branch = matching_wt.ref_name.as_ref().and_then(|r| {
            let name = r.strip_prefix("refs/heads/").unwrap_or(r.as_ref());
            if !r.starts_with("refs/remotes/") && r.as_ref() != "HEAD" {
                Some(name.to_string())
            } else {
                None
            }
        });

        let initial_remove_result = repository
            .update(cx, |repo, _| repo.remove_worktree(path.clone(), false))
            .await?;

        let (remove_result, attempted_force) = match initial_remove_result {
            Ok(()) => (Ok(()), false),
            Err(error) => {
                let force_delete_prompt =
                    force_delete_prompt_for_worktree_remove_error(&error, &display_name);

                if let Some(prompt_message) = force_delete_prompt {
                    let answer = cx.update(|window, cx| {
                        window.prompt(
                            PromptLevel::Warning,
                            &prompt_message,
                            None,
                            &["Force Remove", "Cancel"],
                            cx,
                        )
                    })?;

                    if answer.await != Ok(0) {
                        return Ok(WorktreeRemovalOutcome::Cancelled);
                    }

                    let is_open_before_force =
                        cx.update(|window, cx| worktree_is_open_in_window(Some(window), identity.clone(), path.clone(), fs.clone(), cx))?.await?;
                    if is_open_before_force {
                        return Ok(WorktreeRemovalOutcome::BlockedOpen);
                    }

                    let retry = repository
                        .update(cx, |repo, _| repo.remove_worktree(path.clone(), true))
                        .await?;

                    if let Err(error) = &retry {
                        log::error!("Failed to force remove worktree: {error}");
                    }

                    (retry, true)
                } else {
                    (Err(error), false)
                }
            }
        };

        if let Err(error) = remove_result {
            if let Some(workspace_entity) = workspace.upgrade() {
                let error_msg = error.to_string();
                cx.update(|_window, cx| {
                    show_error_toast(
                        workspace_entity,
                        remove_worktree_command(&path, attempted_force),
                        anyhow!("{error_msg}"),
                        cx,
                    )
                })?;
            }
            return Err(error);
        }

        let mut branch_error = None;

        if delete_linked_branch && let Some(branch_name) = refreshed_linked_branch {
            let branch_display: SharedString = branch_name.clone().into();
            let branch_res = cx
                .update(|window, cx| {
                    delete_branch(
                        repository.clone(),
                        false,
                        branch_name,
                        branch_display,
                        false,
                        workspace.clone(),
                        window,
                        cx,
                    )
                })?
                .await;

            match branch_res {
                Ok(DeleteOutcome::Deleted) => {}
                Ok(DeleteOutcome::Cancelled) => {
                    branch_error = Some(anyhow!("Linked branch deletion was cancelled"));
                }
                Err(err) => {
                    branch_error = Some(err);
                }
            }
        }

        Ok(WorktreeRemovalOutcome::Removed { branch_error })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::FakeFs;
    use gpui::{TestAppContext, VisualTestContext};
    use project::Project;
    use serde_json::json;
    use settings::Settings;
    use settings::SettingsStore;
    use util::path;
    use workspace::MultiWorkspace;

    fn init_test(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            editor::init(cx);
            project::project_settings::ProjectSettings::register(cx);
            project::WorktreeSettings::register(cx);
        });
    }

    async fn init_test_repo(
        cx: &mut TestAppContext,
    ) -> (
        Arc<FakeFs>,
        Entity<Project>,
        Entity<Repository>,
        git::repository::Worktree,
        Entity<Workspace>,
        VisualTestContext,
    ) {
        init_test(cx);

        let fs = FakeFs::new(cx.executor());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            path!("/root"),
            json!({
                "project": {
                    ".git": {},
                    "file.txt": "buffer_text",
                },
                "worktrees": {},
            }),
        )
        .await;
        fs.set_head_for_repo(
            path!("/root/project/.git").as_ref(),
            &[("file.txt", "buffer_text".to_string())],
            "deadbeef",
        );

        let project = Project::test(fs.clone(), [path!("/root/project").as_ref()], cx).await;
        cx.executor().run_until_parked();

        let repository = project.read_with(cx, |project, cx| {
            project.repositories(cx).values().next().unwrap().clone()
        });
        let worktree_path = PathBuf::from(path!("/root/worktrees/linked-wt"));

        cx.update(|cx| {
            repository.update(cx, |repository, _| {
                repository.create_worktree(
                    git::repository::CreateWorktreeTarget::NewBranch {
                        branch_name: "linked-wt".to_string(),
                        base_sha: Some("deadbeef".to_string()),
                    },
                    worktree_path.clone(),
                )
            })
        })
        .await
        .unwrap()
        .unwrap();

        let worktrees = repository
            .update(cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap();
        let linked_wt = worktrees
            .into_iter()
            .find(|wt| wt.path == worktree_path)
            .unwrap();

        let window_handle =
            cx.add_window(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        cx.executor().run_until_parked();
        let workspace = window_handle
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();

        let cx = VisualTestContext::from_window(window_handle.into(), cx);
        (fs, project, repository, linked_wt, workspace, cx)
    }

    #[gpui::test]
    async fn test_worktree_removal_outcome_partial_eq() {
        assert_eq!(
            WorktreeRemovalOutcome::Removed { branch_error: None },
            WorktreeRemovalOutcome::Removed { branch_error: None }
        );
        assert_eq!(
            WorktreeRemovalOutcome::Removed {
                branch_error: Some(anyhow!("branch error"))
            },
            WorktreeRemovalOutcome::Removed {
                branch_error: Some(anyhow!("branch error"))
            }
        );
        assert_ne!(
            WorktreeRemovalOutcome::Removed { branch_error: None },
            WorktreeRemovalOutcome::Removed {
                branch_error: Some(anyhow!("branch error"))
            }
        );
        assert_eq!(
            WorktreeRemovalOutcome::BlockedOpen,
            WorktreeRemovalOutcome::BlockedOpen
        );
        assert_eq!(
            WorktreeRemovalOutcome::Cancelled,
            WorktreeRemovalOutcome::Cancelled
        );
    }

    #[gpui::test]
    async fn test_delete_branch_success(cx: &mut TestAppContext) {
        let (_fs, _project, repository, _wt, workspace, mut cx) = init_test_repo(cx).await;

        repository
            .update(&mut cx, |repo, _| {
                repo.create_branch("feature".to_string(), None)
            })
            .await
            .unwrap()
            .unwrap();

        let task = cx.update(|window, cx| {
            delete_branch(
                repository.clone(),
                false,
                "feature".to_string(),
                "feature".into(),
                false,
                workspace.downgrade(),
                window,
                cx,
            )
        });

        let result = task.await.unwrap();
        assert_eq!(result, DeleteOutcome::Deleted);

        let branches = repository.update(&mut cx, |repo, _| repo.branches()).await.unwrap().unwrap();
        assert!(!branches.branches.iter().any(|b| b.name() == "feature"));
    }

    #[gpui::test]
    async fn test_delete_branch_unmerged_prompts_for_force_delete(cx: &mut TestAppContext) {
        let (fs, _project, repository, _wt, workspace, mut cx) = init_test_repo(cx).await;

        repository
            .update(&mut cx, |repo, _| {
                repo.create_branch("unmerged".to_string(), None)
            })
            .await
            .unwrap()
            .unwrap();

        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.branches_requiring_force_delete.insert("unmerged".to_string());
        })
        .unwrap();

        let task = cx.update(|window, cx| {
            delete_branch(
                repository.clone(),
                false,
                "unmerged".to_string(),
                "unmerged".into(),
                false,
                workspace.downgrade(),
                window,
                cx,
            )
        });

        cx.run_until_parked();
        assert!(cx.has_pending_prompt());

        cx.simulate_prompt_answer("Force Delete");
        cx.run_until_parked();

        let result = task.await.unwrap();
        assert_eq!(result, DeleteOutcome::Deleted);

        let branches = repository.update(&mut cx, |repo, _| repo.branches()).await.unwrap().unwrap();
        assert!(!branches.branches.iter().any(|b| b.name() == "unmerged"));
    }

    #[gpui::test]
    async fn test_delete_branch_unmerged_cancel_keeps_branch(cx: &mut TestAppContext) {
        let (fs, _project, repository, _wt, workspace, mut cx) = init_test_repo(cx).await;

        repository
            .update(&mut cx, |repo, _| {
                repo.create_branch("unmerged".to_string(), None)
            })
            .await
            .unwrap()
            .unwrap();

        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.branches_requiring_force_delete.insert("unmerged".to_string());
        })
        .unwrap();

        let task = cx.update(|window, cx| {
            delete_branch(
                repository.clone(),
                false,
                "unmerged".to_string(),
                "unmerged".into(),
                false,
                workspace.downgrade(),
                window,
                cx,
            )
        });

        cx.run_until_parked();
        assert!(cx.has_pending_prompt());

        cx.simulate_prompt_answer("Cancel");
        cx.run_until_parked();

        let result = task.await.unwrap();
        assert_eq!(result, DeleteOutcome::Cancelled);

        let branches = repository.update(&mut cx, |repo, _| repo.branches()).await.unwrap().unwrap();
        assert!(branches.branches.iter().any(|b| b.name() == "unmerged"));
    }

    #[gpui::test]
    async fn test_worktree_is_open_and_main_blocked(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, _workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let main_wt = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|wt| wt.is_main)
            .unwrap();

        // Main worktree open in workspace 1
        let is_main_open = cx
            .update(|window, cx| worktree_is_open_in_window(Some(window), identity.clone(), main_wt.path.clone(), fs.clone(), cx))
            .await
            .unwrap();
        assert!(is_main_open);

        // Linked worktree not open in any workspace yet
        let is_linked_open = cx
            .update(|window, cx| worktree_is_open_in_window(Some(window), identity.clone(), linked_wt.path.clone(), fs.clone(), cx))
            .await
            .unwrap();
        assert!(!is_linked_open);
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_main_blocked(cx: &mut TestAppContext) {
        let (_fs, _project, repository, _linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let main_wt = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|wt| wt.is_main)
            .unwrap();

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, main_wt, workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        // Modal opens in blocked mode; confirm it
        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::BlockedOpen);
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_open_in_another_window_blocked(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        // Open linked_wt in a second window
        let second_project = Project::test(fs.clone(), [linked_wt.path.as_path()], &mut cx).await;
        cx.executor().run_until_parked();

        let window_handle2 =
            cx.add_window(|window, cx| MultiWorkspace::test_new(second_project.clone(), window, cx));
        cx.executor().run_until_parked();

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        // Modal opens in blocked mode; confirm it
        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::BlockedOpen);

        let _ = window_handle2;
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_dirty_force_remove(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state
                .worktrees_requiring_force_delete
                .insert(linked_wt.path.clone());
        })
        .unwrap();

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        // Confirm removal modal
        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        // Dirty prompt should appear
        assert!(cx.has_pending_prompt());
        cx.simulate_prompt_answer("Force Remove");
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Removed { branch_error: None });

        let worktrees = repository.update(&mut cx, |repo, _| repo.worktrees()).await.unwrap().unwrap();
        assert!(!worktrees.iter().any(|wt| wt.path == linked_wt.path));
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_cancellation(cx: &mut TestAppContext) {
        let (_fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        cx.dispatch_action(menu::Cancel);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Cancelled);
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_opened_during_confirmation_blocks(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        let second_project = Project::test(fs.clone(), [linked_wt.path.as_path()], &mut cx).await;
        cx.executor().run_until_parked();

        let window_handle2 =
            cx.add_window(|window, cx| MultiWorkspace::test_new(second_project.clone(), window, cx));
        cx.executor().run_until_parked();

        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::BlockedOpen);

        let _ = window_handle2;
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_partial_success_branch_failure(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.branches_requiring_force_delete.insert("linked-wt".to_string());
        })
        .unwrap();

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        let modal = workspace.read_with(&cx, |ws, cx| {
            ws.active_modal::<WorktreeRemovalConfirmModal>(cx).unwrap()
        });

        modal.update(&mut cx, |modal, cx| {
            modal.delete_linked_branch = true;
            cx.notify();
        });

        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        assert!(cx.has_pending_prompt());
        cx.simulate_prompt_answer("Cancel");
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert!(matches!(outcome, WorktreeRemovalOutcome::Removed { branch_error: Some(_) }));

        let worktrees = repository.update(&mut cx, |repo, _| repo.worktrees()).await.unwrap().unwrap();
        assert!(!worktrees.iter().any(|wt| wt.path == linked_wt.path));
    }

    #[gpui::test]
    async fn test_worktree_is_open_same_path_another_remote_host(cx: &mut TestAppContext) {
        let (_fs, _project, repository, linked_wt, _workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        
        let identity_host_a = HostScopedRepositoryIdentity {
            common_identity: common_identity.clone(),
            host_key: "host_a".to_string(),
        };
        let identity_host_b = HostScopedRepositoryIdentity {
            common_identity,
            host_key: "host_b".to_string(),
        };

        let is_open_host_b = cx
            .update(|window, cx| worktree_is_open_in_window(Some(window), identity_host_b, linked_wt.path.clone(), <dyn Fs>::global(cx), cx))
            .await
            .unwrap();
        assert!(!is_open_host_b);

        let is_open_host_a = cx
            .update(|window, cx| worktree_is_open_in_window(Some(window), identity_host_a, linked_wt.path.clone(), <dyn Fs>::global(cx), cx))
            .await
            .unwrap();
        assert!(!is_open_host_a);
    }

    #[gpui::test]
    async fn test_worktree_is_open_symlink_alias(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, _workspace, mut cx) = init_test_repo(cx).await;

        let symlink_path = PathBuf::from(path!("/root/worktrees/linked-wt-symlink"));
        fs.insert_symlink(&symlink_path, linked_wt.path.clone()).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let second_project = Project::test(fs.clone(), [linked_wt.path.as_path()], &mut cx).await;
        cx.executor().run_until_parked();

        let window_handle2 =
            cx.add_window(|window, cx| MultiWorkspace::test_new(second_project.clone(), window, cx));
        cx.executor().run_until_parked();

        let is_open = cx
            .update(|window, cx| worktree_is_open_in_window(Some(window), identity, symlink_path, fs.clone(), cx))
            .await
            .unwrap();
        assert!(is_open, "symlink alias of open worktree path should be recognized as open");

        let _ = window_handle2;
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_stale_entry(cx: &mut TestAppContext) {
        let (_fs, _project, repository, mut linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        linked_wt.path = PathBuf::from(path!("/root/worktrees/stale-nonexistent-wt"));

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt, workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(
            outcome,
            WorktreeRemovalOutcome::BlockedOpen,
            "stale entry missing from refreshed repository worktrees should be blocked"
        );
    }

    #[gpui::test]
    async fn test_confirm_remove_worktree_unknown_failure(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;

        fs.save(
            &linked_wt.path.join(".git"),
            &"corrupted-invalid-gitdir-content".into(),
            Default::default(),
        )
        .await
        .unwrap();

        let common_identity = repository.read_with(&cx, |repo, _| repo.common_repository_identity());
        let identity = HostScopedRepositoryIdentity::new(common_identity, None);

        let task = cx.update(|window, cx| {
            confirm_remove_worktree(repository.clone(), identity, linked_wt.clone(), workspace.downgrade(), window, cx)
        });

        cx.run_until_parked();

        cx.dispatch_action(menu::Confirm);
        cx.run_until_parked();

        assert!(
            !cx.has_pending_prompt(),
            "unknown git failure should not prompt for force delete"
        );

        let result = task.await;
        assert!(
            result.is_err(),
            "unknown git failure should return an error without force delete prompt"
        );
    }
}

