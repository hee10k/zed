use crate::{
    commit_view::CommitView,
    git_graph::GitGraph,
    git_graph_actions::GraphMutation,
    git_ref_modal::{GitRefModal, GitRefModalKind, GitRefModalResult},
};
use anyhow::anyhow;
use git::Oid;
use git::repository::{CreateTagOptions, MergeMode, ResetMode};
use git_ui_core::delete_service::{self as delete_service, WorktreeRemovalOutcome};
use git_ui_core::notifications::show_error_toast;
use git_ui_core::worktree_name_modal::WorktreeNameModal;
use git_ui_core::worktree_service::{
    HostScopedRepositoryIdentity, handle_create_worktree, linked_worktree_label, switch_to_worktree,
};
use gpui::{
    Action, App, ClipboardItem, Entity, FocusHandle, SharedString, Task, WeakEntity, Window,
    actions,
};
use project::{GIT_COMMAND_TASK_TAG, git_store::Repository};
use task::{TaskContext, TaskVariables, VariableName};
use ui::{Color, ContextMenu, ContextMenuEntry, IconName, IconPosition, prelude::*};
use workspace::{
    OpenMode, Workspace,
    notifications::DetachAndPromptErr,
};
use zed_actions::{NewWorktreeBranchTarget, OpenWorktreeInNewWindow};

actions!(
    git_graph,
    [
        /// Copies the SHA of the selected commit to the clipboard.
        CopyCommitSha,
        /// Copies a tag from the selected commit to the clipboard.
        CopyCommitTag,
        /// Opens the commit view for the selected commit.
        OpenCommitView,
    ]
);

const COMMIT_TAG_LIST_WIDTH_IN_REMS: Rems = rems(10.);
const CUSTOM_GIT_COMMANDS_DOCS_SLUG: &str = "tasks#custom-git-commands";

pub(crate) struct CommitContextMenuData {
    pub(crate) sha: Oid,
    pub(crate) tag_names: Vec<SharedString>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitContextMenuSource {
    GitGraph,
    GitPanel,
}

pub(crate) fn commit_context_menu(
    commit: CommitContextMenuData,
    selected_commits: Vec<Oid>,
    source: CommitContextMenuSource,
    ref_name: Option<SharedString>,
    focus_handle: FocusHandle,
    repository: Option<WeakEntity<Repository>>,
    graph: Option<WeakEntity<GitGraph>>,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<ContextMenu> {
    let sha = commit.sha;
    let cherry_pick_commits = if selected_commits.is_empty() {
        vec![sha]
    } else {
        selected_commits
    };
    let sha_short = sha.display_short();
    // Linked worktrees checked out at this exact commit, captured from live
    // repository state so the Worktree submenu can offer one-entry-per-worktree
    // navigation. Only surfaced in the Git Graph; the Git Panel history menu
    // stays commit-only.
    let matching_worktrees: Vec<git::repository::Worktree> = if source
        == CommitContextMenuSource::GitGraph
    {
        repository
            .as_ref()
            .and_then(|repository| repository.upgrade())
            .map(|repository| {
                repository
                    .read(cx)
                    .linked_worktrees
                    .iter()
                    .filter(|worktree| worktree.sha.as_ref() == sha.to_string())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    // Per-worktree build-time "potentially eligible" flag for the Git Graph
    // Worktree submenu's `Remove Worktree…` actions. A worktree is potentially
    // eligible when it is not the main worktree and is not open in any Zed
    // window (including the current workspace). The optimistic sync open-check
    // never canonicalizes symlinks; the authoritative async re-check in the
    // shared remove service re-validates immediately before mutation, and
    // disappeared / stale-replaced targets are blocked there. This is computed
    // at the function top (Window + App are available here) and captured into
    // the build closure below, whose `.when(...GitGraph...)` builder only
    // threads `Window, App` to entry handlers.
    let matching_worktrees_eligible: Vec<(git::repository::Worktree, bool)> = if source
        == CommitContextMenuSource::GitGraph
    {
        // Reborrow the function's `&mut` handles as shared references so they
        // can be captured by the eligibility closure below without moving them
        // (the build closure still needs the mutable handles afterward).
        let window = &*window;
        let cx = &*cx;
        let workspace_entity = workspace.upgrade();
        let identity = repository
            .as_ref()
            .and_then(|repository| repository.upgrade())
            .map(|repository| {
                let common_identity = repository.read(cx).common_repository_identity();
                let remote_options = workspace_entity
                    .as_ref()
                    .and_then(|workspace| {
                        workspace
                            .read(cx)
                            .project()
                            .read(cx)
                            .remote_connection_options(cx)
                    });
                HostScopedRepositoryIdentity::new(common_identity, remote_options.as_ref())
            });
        matching_worktrees
            .iter()
            .map(|worktree| {
                let eligible = worktree_removal_eligible(worktree, identity.as_ref(), window, cx);
                (worktree.clone(), eligible)
            })
            .collect()
    } else {
        Vec::new()
    };
    let git_tasks = git_context_menu_tasks(
        git_task_context(&repository, sha, ref_name.as_deref(), cx),
        &workspace,
        cx,
    );
    let header = match &ref_name {
        Some(ref_name) => format!("Ref {ref_name}"),
        None => format!("Commit {sha_short}"),
    };

    ContextMenu::build(window, cx, move |context_menu, _, _| {
        context_menu
            .context(focus_handle)
            .header(header)
            .entry("View Diff", Some(OpenCommitView.boxed_clone()), {
                let repository = repository.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    let Some(repository) = repository.clone() else {
                        return;
                    };
                    CommitView::open(
                        sha.to_string(),
                        repository,
                        workspace.clone(),
                        None,
                        None,
                        window,
                        cx,
                    );
                }
            })
            .entry(
                "Copy SHA",
                Some(CopyCommitSha.boxed_clone()),
                move |_window, cx| {
                    cx.write_to_clipboard(ClipboardItem::new_string(sha.to_string()));
                },
            )
            .when_some(ref_name.clone(), |menu, ref_name| {
                menu.entry("Copy Ref Name", None, move |_window, cx| {
                    cx.write_to_clipboard(ClipboardItem::new_string(ref_name.to_string()));
                })
            })
            .when(ref_name.is_none(), |menu| {
                menu.map(|menu| {
                    let tag_names = commit.tag_names.clone();
                    let copy_tag_label = "Copy Tag";

                    match tag_names.as_slice() {
                        [] => menu.item(
                            ContextMenuEntry::new(copy_tag_label)
                                .action(CopyCommitTag.boxed_clone())
                                .disabled(true),
                        ),
                        [tag_name] => {
                            let tag_name = tag_name.clone();
                            let label = format!("{copy_tag_label}: {tag_name}");
                            menu.entry(
                                label,
                                Some(CopyCommitTag.boxed_clone()),
                                move |_window, cx| {
                                    cx.write_to_clipboard(ClipboardItem::new_string(
                                        tag_name.to_string(),
                                    ));
                                },
                            )
                        }
                        _ => menu.submenu(copy_tag_label, move |menu, _window, _cx| {
                            let mut menu = menu.fixed_width(COMMIT_TAG_LIST_WIDTH_IN_REMS.into());

                            for tag_name in tag_names.clone() {
                                let tag_name_to_copy = tag_name.clone();
                                menu = menu.entry(tag_name, None, move |_window, cx| {
                                    cx.write_to_clipboard(ClipboardItem::new_string(
                                        tag_name_to_copy.to_string(),
                                    ));
                                });
                            }
                            menu
                        }),
                    }
                })
            })
            .when_some(graph.clone(), |menu, graph| {
                let repository = repository.clone();
                let workspace = workspace.clone();
                let reset_graph = graph.clone();
                menu.separator()
                    .header("Git Actions")
                    .entry("Checkout Commit", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            schedule_graph_mutation(
                                graph.clone(),
                                GraphMutation::Checkout { commit: sha },
                                Some(sha),
                                window,
                                cx,
                            );
                        }
                    })
                    .entry("Create Branch…", None, {
                        let graph = graph.clone();
                        let repository = repository.clone();
                        let workspace = workspace.clone();
                        move |window, cx| {
                            open_ref_action(
                                GitRefModalKind::Branch,
                                graph.clone(),
                                repository.clone(),
                                workspace.clone(),
                                sha,
                                window,
                                cx,
                            );
                        }
                    })
                    .entry("Create Tag…", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            open_ref_action(
                                GitRefModalKind::Tag,
                                graph.clone(),
                                repository.clone(),
                                workspace.clone(),
                                sha,
                                window,
                                cx,
                            );
                        }
                    })
                    .entry("Cherry-pick", None, {
                        let graph = graph.clone();
                        let cherry_pick_commits = cherry_pick_commits.clone();
                        move |window, cx| {
                            schedule_graph_mutation(
                                graph.clone(),
                                GraphMutation::CherryPick {
                                    commits: cherry_pick_commits.clone(),
                                    no_commit: false,
                                },
                                Some(sha),
                                window,
                                cx,
                            );
                        }
                    })
                    .entry("Revert", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            schedule_graph_mutation(
                                graph.clone(),
                                GraphMutation::Revert {
                                    commit: sha,
                                    no_commit: false,
                                },
                                Some(sha),
                                window,
                                cx,
                            );
                        }
                    })
                    .submenu("Reset", move |menu, _window, _cx| {
                        let graph = reset_graph.clone();
                        menu.entry("Soft", None, {
                            let graph = graph.clone();
                            move |window, cx| {
                                schedule_graph_mutation(
                                    graph.clone(),
                                    GraphMutation::Reset {
                                        commit: sha,
                                        mode: ResetMode::Soft,
                                    },
                                    Some(sha),
                                    window,
                                    cx,
                                );
                            }
                        })
                        .entry("Mixed", None, {
                            let graph = graph.clone();
                            move |window, cx| {
                                schedule_graph_mutation(
                                    graph.clone(),
                                    GraphMutation::Reset {
                                        commit: sha,
                                        mode: ResetMode::Mixed,
                                    },
                                    Some(sha),
                                    window,
                                    cx,
                                );
                            }
                        })
                        .entry("Hard", None, move |window, cx| {
                            schedule_graph_mutation(
                                graph.clone(),
                                GraphMutation::Reset {
                                    commit: sha,
                                    mode: ResetMode::Hard,
                                },
                                Some(sha),
                                window,
                                cx,
                            );
                        })
                    })
                    .entry("Compare with HEAD", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            if let Some(graph) = graph.upgrade() {
                                graph.update(cx, |graph, cx| {
                                    graph.compare_with_head(sha, window, cx);
                                });
                            }
                        }
                    })
                    .entry("Compare with Working Tree", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            if let Some(graph) = graph.upgrade() {
                                graph.update(cx, |graph, cx| {
                                    graph.compare_with_working_tree(sha, window, cx);
                                });
                            }
                        }
                    })
                    .entry("Select for Compare", None, {
                        let graph = graph.clone();
                        move |_window, cx| {
                            if let Some(graph) = graph.upgrade() {
                                graph.update(cx, |graph, cx| graph.select_for_compare(sha, cx));
                            }
                        }
                    })
                    .entry("Compare with Selected Base", None, {
                        let graph = graph.clone();
                        move |window, cx| {
                            if let Some(graph) = graph.upgrade() {
                                graph.update(cx, |graph, cx| {
                                    graph.compare_with_selected_base(sha, window, cx);
                                });
                            }
                        }
                    })
                    .entry("Merge", None, move |window, cx| {
                        schedule_graph_mutation(
                            graph.clone(),
                            GraphMutation::Merge {
                                commit: sha,
                                mode: MergeMode::Default,
                            },
                            Some(sha),
                            window,
                            cx,
                        );
                    })
            })
            .when(source == CommitContextMenuSource::GitGraph, |menu| {
                let workspace = workspace.clone();
                #[allow(clippy::redundant_clone)]
                let workspace_for_entry = workspace.clone();
                let worktrees = matching_worktrees_eligible.clone();
                let mut menu = menu.separator().header("Worktree");
                menu = menu.submenu(
                    "Create Detached Worktree",
                    {
                        let repository = repository.clone();
                        let workspace = workspace_for_entry.clone();
                        move |menu, _window, _cx| {
                            #[allow(clippy::redundant_clone)]
                            let repository = repository.clone();
                            #[allow(clippy::redundant_clone)]
                            let workspace = workspace.clone();
                            menu.entry("Create…", None, move |window, cx| {
                                create_detached_worktree_for_commit(
                                    repository.clone(),
                                    workspace.clone(),
                                    sha,
                                    window,
                                    cx,
                                );
                            })
                        }
                    },
                );
                if !worktrees.is_empty() {
                    for (worktree, eligible) in &worktrees {
                        let path = worktree.path.clone();
                        // Each entry is distinguishable by its checked-out
                        // branch and a portable short path, so worktrees that
                        // share the clicked commit stay separately addressable.
                        let display_name = linked_worktree_label(worktree).to_string();
                        let switch_workspace = workspace_for_entry.clone();
                        let switch_path = path.clone();
                        let switch_display_name = display_name.clone();
                        let switch_offer_sha = sha.to_string();
                        menu = menu.entry(
                            format!("Switch to {display_name}"),
                            None,
                            move |window, cx| {
                                // This is also the shared worktree service switch
                                // (same OS window, never a terminal `cd`), catching
                                // current-target, disappeared-target, and
                                // stale-snapshot as explicit no-ops via toasts.
                                let Some(workspace) = switch_workspace.upgrade() else {
                                    // Missing-window-handle: explicit error state,
                                    // never a silent `if let Some` fallback.
                                    log::error!(
                                        "worktree switch: source window handle for {} is no \
                                         longer available",
                                        switch_path.display()
                                    );
                                    return;
                                };
                                workspace.update(cx, |workspace, cx| {
                                    switch_to_worktree(
                                        workspace,
                                        switch_path.clone(),
                                        switch_display_name.clone().into(),
                                        Some(switch_offer_sha.clone().into()),
                                        window,
                                        None,
                                        OpenMode::Activate,
                                        cx,
                                    );
                                });
                            },
                        );
                        let new_window_path = path.clone();
                        menu = menu.entry(
                            format!("Open {display_name} in New Window"),
                            None,
                            move |window, cx| {
                                // Routes through the shared open-in-new-window seam:
                                // a distinct OS window, no file/dock transfer.
                                window.dispatch_action(
                                    Box::new(OpenWorktreeInNewWindow {
                                        path: new_window_path.clone(),
                                    }),
                                    cx,
                                );
                            },
                        );
                        if *eligible {
                            // Per-worktree removal reuses the shared guarded service
                            // (confirmation, open-guard, safe/force removal, optional
                            // linked-branch cleanup, partial result). Only this entry's
                            // path is ever targeted, so duplicate-SHA worktrees stay
                            // independently addressable.
                            let remove_repository = repository.clone();
                            let remove_graph = graph.clone();
                            let remove_workspace = workspace_for_entry.clone();
                            let remove_worktree = worktree.clone();
                            menu = menu.entry(
                                "Remove Worktree…",
                                None,
                                move |window, cx| {
                                    remove_worktree_from_graph(
                                        remove_repository.clone(),
                                        remove_graph.clone(),
                                        remove_workspace.clone(),
                                        remove_worktree.clone(),
                                        window,
                                        cx,
                                    );
                                },
                            );
                        }
                    }
                }
                menu
            })
            .when(source == CommitContextMenuSource::GitPanel, |menu| {
                menu.entry("Show in Git Graph", None, move |window, cx| {
                    window.dispatch_action(
                        Box::new(crate::git_graph::OpenAtCommit {
                            sha: sha.to_string(),
                        }),
                        cx,
                    );
                })
            })
            .map(|mut menu| {
                menu = menu.separator().header("Custom Commands");

                if git_tasks.is_empty() {
                    return menu.item(
                        ContextMenuEntry::new("Learn More")
                            .icon(IconName::ArrowUpRight)
                            .icon_color(Color::Muted)
                            .icon_position(IconPosition::End)
                            .handler(|_window, cx| {
                                let docs_url =
                                    release_channel::docs_url(CUSTOM_GIT_COMMANDS_DOCS_SLUG, cx);
                                cx.open_url(&docs_url);
                            }),
                    );
                }

                for (task_source_kind, resolved_task) in git_tasks {
                    let label = resolved_task.display_label().to_string();
                    let workspace = workspace.clone();
                    menu = menu.entry(label, None, move |window, cx| {
                        workspace
                            .update(cx, |workspace, cx| {
                                workspace.schedule_resolved_task(
                                    task_source_kind.clone(),
                                    resolved_task.clone(),
                                    false,
                                    window,
                                    cx,
                                );
                            })
                            .ok();
                    });
                }

                menu
            })
    })
}

fn create_detached_worktree_for_commit(
    repository: Option<WeakEntity<Repository>>,
    workspace: WeakEntity<Workspace>,
    sha: git::Oid,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(repository_id) = repository
        .as_ref()
        .and_then(|repository| repository.upgrade())
        .map(|repository| repository.read(cx).id.to_proto())
    else {
        if let Some(workspace) = workspace.upgrade() {
            show_error_toast(
                workspace,
                "worktree create",
                anyhow!("The repository is no longer available"),
                cx,
            );
        }
        return;
    };
    let repository_name = repository
        .as_ref()
        .and_then(|repository| repository.upgrade())
        .and_then(|repository| {
            repository
                .read(cx)
                .work_directory_abs_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(ToString::to_string)
        });
    let context_label = match repository_name {
        Some(repository_name) => {
            Some(format!("from {} in {repository_name}", sha.display_short()).into())
        }
        None => Some(format!("from {}", sha.display_short()).into()),
    };

    let modal = WorktreeNameModal::open(workspace.clone(), None, context_label, window, cx);
    window
        .spawn(cx, async move |cx| {
            let Some(name) = modal.await else {
                return Ok(());
            };
            let action = zed_actions::CreateWorktree {
                worktree_name: Some(name),
                branch_target: NewWorktreeBranchTarget::Commit {
                    repository_id,
                    sha: sha.to_string(),
                },
            };
            workspace
                .update_in(cx, |workspace, window, cx| {
                    handle_create_worktree(workspace, &action, window, None, OpenMode::Activate, cx);
                })
                .map_err(|error| anyhow!(error))?;
            Ok(())
        })
        .detach_and_prompt_err("Git graph worktree action failed", window, cx, |error, _, _| {
            Some(error.to_string())
        });
}

fn worktree_removal_eligible(
    worktree: &git::repository::Worktree,
    identity: Option<&HostScopedRepositoryIdentity>,
    window: &Window,
    cx: &App,
) -> bool {
    let Some(identity) = identity else {
        return false;
    };
    if worktree.is_main {
        return false;
    }
    // Conservative: if the sync open-check errors, do not offer removal here;
    // the shared service revalidates on a confirm from the Worktree Picker.
    delete_service::worktree_is_open_in_window_sync(Some(window), identity.clone(), &worktree.path, cx)
        .map(|is_open| !is_open)
        .unwrap_or(false)
}

fn remove_worktree_from_graph(
    repository: Option<WeakEntity<Repository>>,
    graph: Option<WeakEntity<GitGraph>>,
    workspace: WeakEntity<Workspace>,
    worktree: git::repository::Worktree,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(repository_entity) = repository.as_ref().and_then(|repository| repository.upgrade())
    else {
        if let Some(workspace) = workspace.upgrade() {
            show_error_toast(
                workspace,
                "worktree remove",
                anyhow!("The repository is no longer available"),
                cx,
            );
        }
        return;
    };
    let Some(workspace_entity) = workspace.upgrade() else {
        // No workspace handle means no valid UI surface for a toast; report the
        // error explicitly rather than falling through silently.
        log::error!(
            "worktree remove: source window handle for {} is no longer available",
            worktree.path.display()
        );
        return;
    };
    let identity = HostScopedRepositoryIdentity::new(
        repository_entity.read(cx).common_repository_identity(),
        workspace_entity
            .read(cx)
            .project()
            .read(cx)
            .remote_connection_options(cx)
            .as_ref(),
    );
    let display_name = worktree
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("worktree")
        .to_string();

    window
        .spawn(cx, async move |cx| {
            // The shared guarded removal service owns the confirmation modal,
            // open-guard (async canonicalize), main/current re-check, safe/force
            // removal, optional linked-branch cleanup, and partial result. We
            // never reimplement any of the picker's removal logic here.
            let outcome = cx
                .update(|window, cx| {
                    delete_service::confirm_remove_worktree(
                        repository_entity,
                        identity,
                        worktree,
                        workspace.clone(),
                        window,
                        cx,
                    )
                })?
                .await;

            match outcome {
                Ok(WorktreeRemovalOutcome::Removed { branch_error }) => {
                    // Success (or partial success). Worktree removal updates the
                    // repository's linked-worktree snapshot event-driven, so the
                    // next open of this submenu re-captures `linked_worktrees`
                    // fresh and the removed path is gone. Nudge the graph and the
                    // workspace to re-render so any surfaces derived from
                    // worktree state stay consistent; the commit graph itself is
                    // not reloaded (removal does not change history).
                    if let Some(graph) = graph.as_ref().and_then(|graph| graph.upgrade()) {
                        graph.update(cx, |_graph, cx| cx.notify());
                    }
                    if let Some(workspace) = workspace.upgrade() {
                        workspace.update(cx, |_workspace, cx| cx.notify());
                    }
                    if let Some(branch_error) = branch_error {
                        // Partial result: the worktree was removed but the linked
                        // branch could not be deleted. Surface the concrete error.
                        if let Some(workspace) = workspace.upgrade() {
                            cx.update(|_window, cx| {
                                show_error_toast(workspace, "delete branch", branch_error, cx)
                            })?;
                        }
                    }
                }
                Ok(WorktreeRemovalOutcome::BlockedOpen) => {
                    // Selection is preserved; report the concrete outcome instead
                    // of silently ignoring the request.
                    if let Some(workspace) = workspace.upgrade() {
                        let message = anyhow!(
                            "Worktree \"{display_name}\" is currently open in a Zed window \
                             (or is the main worktree) and cannot be removed. Please close or \
                             switch away from it first."
                        );
                        cx.update(|_window, cx| {
                            show_error_toast(workspace, "worktree remove", message, cx)
                        })?;
                    }
                }
                Ok(WorktreeRemovalOutcome::Cancelled) => {
                    // User cancelled the confirmation (or the force-delete prompt);
                    // nothing mutated. Selection is preserved and we stay silent.
                }
                Err(error) => return Err(error),
            }

            anyhow::Ok(())
        })
        .detach_and_prompt_err("Git graph worktree removal failed", window, cx, |error, _, _| {
            Some(error.to_string())
        });
}

fn schedule_graph_mutation(
    graph: WeakEntity<GitGraph>,
    mutation: GraphMutation,
    primary_selection: Option<Oid>,
    window: &mut Window,
    cx: &mut App,
) {
    let task = match graph.upgrade() {
        Some(graph) => match graph.update(cx, |graph, cx| {
            graph.schedule_mutation(mutation, primary_selection, cx)
        }) {
            Ok(task) => task,
            Err(error) => Task::ready(Err(anyhow!(error))),
        },
        None => Task::ready(Err(anyhow!("Git graph is no longer available"))),
    };
    task.detach_and_prompt_err("Git graph action failed", window, cx, |error, _, _| {
        Some(error.to_string())
    });
}

fn open_ref_action(
    kind: GitRefModalKind,
    graph: WeakEntity<GitGraph>,
    repository: Option<WeakEntity<Repository>>,
    workspace: WeakEntity<Workspace>,
    sha: Oid,
    window: &mut Window,
    cx: &mut App,
) {
    let modal = GitRefModal::open(kind, workspace, window, cx);
    window
        .spawn(cx, async move |cx| {
            let Some(result) = modal.await else {
                return Ok(());
            };

            match result {
                GitRefModalResult::Branch { name } => {
                    let repository = repository
                        .and_then(|repository| repository.upgrade())
                        .ok_or_else(|| anyhow!("Repository is no longer available"))?;
                    let receiver = repository.update(cx, |repository, _| {
                        repository.create_branch(name, Some(sha.to_string()))
                    });
                    receiver.await??;
                }
                GitRefModalResult::Tag { name, message } => {
                    let graph = graph
                        .upgrade()
                        .ok_or_else(|| anyhow!("Git graph is no longer available"))?;
                    let task = graph.update(cx, |graph, cx| {
                        graph.schedule_mutation(
                            GraphMutation::CreateTag(CreateTagOptions {
                                name,
                                target: sha.to_string(),
                                message,
                            }),
                            Some(sha),
                            cx,
                        )
                    })?;
                    task.await?;
                }
            }
            Ok(())
        })
        .detach_and_prompt_err("Git graph action failed", window, cx, |error, _, _| {
            Some(error.to_string())
        });
}

fn git_task_context(
    repository: &Option<WeakEntity<Repository>>,
    commit_sha: git::Oid,
    ref_name: Option<&str>,
    cx: &App,
) -> Option<TaskContext> {
    let repository_path = repository
        .as_ref()?
        .upgrade()?
        .read(cx)
        .work_directory_abs_path
        .to_path_buf();
    let repository_name = repository_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToString::to_string);
    let mut task_variables = TaskVariables::from_iter([
        (VariableName::GitSha, commit_sha.to_string()),
        (VariableName::GitShaShort, commit_sha.display_short()),
        (
            VariableName::GitRepositoryPath,
            repository_path.to_string_lossy().into_owned(),
        ),
    ]);

    if let Some(repository_name) = repository_name {
        task_variables.insert(VariableName::GitRepositoryName, repository_name);
    }
    if let Some(ref_name) = ref_name {
        task_variables.insert(VariableName::GitRef, ref_name.to_string());
    }

    Some(TaskContext {
        cwd: Some(repository_path),
        task_variables,
        ..TaskContext::default()
    })
}

fn git_context_menu_tasks(
    task_context: Option<TaskContext>,
    workspace: &WeakEntity<Workspace>,
    cx: &App,
) -> Vec<(project::TaskSourceKind, task::ResolvedTask)> {
    let Some(task_context) = task_context else {
        return Vec::new();
    };
    let Some(workspace) = workspace.upgrade() else {
        return Vec::new();
    };
    let project = workspace.read(cx).project().clone();
    let task_inventory = project.read_with(cx, |project, cx| {
        project.task_store().read(cx).task_inventory().cloned()
    });
    let Some(task_inventory) = task_inventory else {
        return Vec::new();
    };

    task_inventory
        .read(cx)
        .resolve_global_tasks_with_tag(GIT_COMMAND_TASK_TAG, &task_context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::FakeFs;
    use fs::Fs;
    use git_ui_core::delete_service::{
        WorktreeRemovalConfirmModal, WorktreeRemovalOutcome,
    };
    use gpui::{TestAppContext, VisualTestContext};
    use menu::Confirm;
    use project::Project;
    use serde_json::json;
    use settings::Settings;
    use settings::SettingsStore;
    use std::path::PathBuf;
    use std::sync::Arc;
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

    fn repo_identity(
        repository: &Entity<Repository>,
        cx: &VisualTestContext,
    ) -> HostScopedRepositoryIdentity {
        HostScopedRepositoryIdentity::new(
            repository.read_with(cx, |repo, _| repo.common_repository_identity()),
            None,
        )
    }

    fn eligible_in_menu(
        worktree: &git::repository::Worktree,
        identity: &HostScopedRepositoryIdentity,
        window: &mut Window,
        cx: &mut App,
    ) -> bool {
        worktree_removal_eligible(worktree, Some(identity), window, cx)
    }

    #[gpui::test]
    async fn test_menu_eligible_linked_worktree_shown(cx: &mut TestAppContext) {
        let (_fs, _project, repository, linked_wt, _workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        // A linked worktree checked out at the graph commit that is not open in
        // any window (only the main repo's root workspace is open) gets a
        // "Remove Worktree…" entry: build-time eligible.
        let eligible =
            cx.update(|window, cx| eligible_in_menu(&linked_wt, &identity, window, cx));
        assert!(eligible, "unopened linked worktree should be removable from the menu");
    }

    #[gpui::test]
    async fn test_menu_main_worktree_blocked(cx: &mut TestAppContext) {
        let (_fs, _project, repository, _linked_wt, _workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        let main_wt = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|wt| wt.is_main)
            .unwrap();

        // The main worktree never shows a "Remove Worktree…" entry.
        let eligible = cx.update(|window, cx| eligible_in_menu(&main_wt, &identity, window, cx));
        assert!(!eligible, "main worktree must never be removable from the menu");
    }

    #[gpui::test]
    async fn test_menu_current_workspace_blocked(cx: &mut TestAppContext) {
        let (_fs, _project, repository, _linked_wt, _workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        let main_wt = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .find(|wt| wt.is_main)
            .unwrap();

        // The main worktree is open in the *current* workspace; the sync seam
        // (which enumerates the active window's workspaces too) must report it
        // open so the menu hides the entry.
        let is_open = cx.update(|window, cx| {
            delete_service::worktree_is_open_in_window_sync(
                Some(window),
                identity.clone(),
                &main_wt.path,
                cx,
            )
        });
        assert!(is_open.unwrap(), "open-in-current-workspace must be detected");
        let eligible = cx.update(|window, cx| eligible_in_menu(&main_wt, &identity, window, cx));
        assert!(!eligible);
    }

    #[gpui::test]
    async fn test_menu_open_elsewhere_blocked(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, _workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        // Open the linked worktree in a second window -> no longer eligible.
        let second_project = Project::test(fs.clone(), [linked_wt.path.as_path()], &mut cx).await;
        cx.executor().run_until_parked();
        let window_handle2 =
            cx.add_window(|window, cx| MultiWorkspace::test_new(second_project.clone(), window, cx));
        cx.executor().run_until_parked();

        let eligible =
            cx.update(|window, cx| eligible_in_menu(&linked_wt, &identity, window, cx));
        assert!(!eligible, "worktree open in another window must not be removable");

        let _ = window_handle2;
    }

    #[gpui::test]
    async fn test_menu_duplicate_sha_entries_independently_removable(cx: &mut TestAppContext) {
        let (_fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        // A second worktree checked out at the *same* SHA as the first.
        let second_path = PathBuf::from(path!("/root/worktrees/linked-wt2"));
        cx.update(|_window, cx| {
            repository.update(cx, |repository, _| {
                repository.create_worktree(
                    git::repository::CreateWorktreeTarget::NewBranch {
                        branch_name: "linked-wt2".to_string(),
                        base_sha: Some("deadbeef".to_string()),
                    },
                    second_path.clone(),
                )
            })
        })
        .await
        .unwrap()
        .unwrap();

        let worktrees = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap();
        let wt1 = worktrees.iter().find(|wt| wt.path == linked_wt.path).unwrap().clone();
        let wt2 = worktrees.iter().find(|wt| wt.path == second_path).unwrap().clone();
        assert_eq!(wt1.sha.to_string(), "deadbeef");
        assert_eq!(wt2.sha.to_string(), "deadbeef");

        // The menu captures one entry per matching worktree and each is
        // independently eligible (none open).
        let both_eligible = cx.update(|window, cx| {
            eligible_in_menu(&wt1, &identity, window, cx)
                && eligible_in_menu(&wt2, &identity, window, cx)
        });
        assert!(both_eligible);

        // Removing the first worktree targets only its path: the second
        // duplicate-SHA worktree must survive.
        let task = cx.update(|window, cx| {
            delete_service::confirm_remove_worktree(
                repository.clone(),
                identity,
                wt1.clone(),
                workspace.downgrade(),
                window,
                cx,
            )
        });
        cx.run_until_parked();
        cx.dispatch_action(Confirm);
        cx.run_until_parked();
        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Removed { branch_error: None });

        let linked = repository.read_with(&cx, |repo, _| repo.linked_worktrees.clone());
        assert!(
            !linked.iter().any(|wt| wt.path == wt1.path),
            "removed entry's path must be gone"
        );
        assert!(
            linked.iter().any(|wt| wt.path == wt2.path),
            "duplicate-SHA sibling must remain independently addressable"
        );
    }

    #[gpui::test]
    async fn test_menu_remove_worktree_deletes_linked_branch(cx: &mut TestAppContext) {
        let (_fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        let task = cx.update(|window, cx| {
            delete_service::confirm_remove_worktree(
                repository.clone(),
                identity,
                linked_wt,
                workspace.downgrade(),
                window,
                cx,
            )
        });
        cx.run_until_parked();

        // Request optional linked-branch deletion through the shared modal.
        let modal = workspace
            .read_with(&cx, |ws, cx| ws.active_modal::<WorktreeRemovalConfirmModal>(cx).unwrap());
        modal.update(&mut cx, |modal, cx| {
            modal.test_set_delete_linked_branch(true);
            cx.notify();
        });
        cx.dispatch_action(Confirm);
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Removed { branch_error: None });

        let branches = repository
            .update(&mut cx, |repo, _| repo.branches())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !branches.branches.iter().any(|b| b.name() == "linked-wt"),
            "linked branch should be removed alongside the worktree"
        );
    }

    #[gpui::test]
    async fn test_menu_force_remove_cancel_keeps_worktree(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        // Dirty worktree requires a force-delete confirmation.
        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.worktrees_requiring_force_delete.insert(linked_wt.path.clone());
        })
        .unwrap();

        let task = cx.update(|window, cx| {
            delete_service::confirm_remove_worktree(
                repository.clone(),
                identity,
                linked_wt.clone(),
                workspace.downgrade(),
                window,
                cx,
            )
        });
        cx.run_until_parked();

        cx.dispatch_action(Confirm);
        cx.run_until_parked();
        assert!(cx.has_pending_prompt());
        cx.simulate_prompt_answer("Cancel");
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Cancelled);

        let worktrees = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap();
        assert!(worktrees.iter().any(|wt| wt.path == linked_wt.path));
    }

    #[gpui::test]
    async fn test_menu_force_remove_accept_removes_worktree(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.worktrees_requiring_force_delete.insert(linked_wt.path.clone());
        })
        .unwrap();

        let task = cx.update(|window, cx| {
            delete_service::confirm_remove_worktree(
                repository.clone(),
                identity,
                linked_wt.clone(),
                workspace.downgrade(),
                window,
                cx,
            )
        });
        cx.run_until_parked();

        cx.dispatch_action(Confirm);
        cx.run_until_parked();
        assert!(cx.has_pending_prompt());
        cx.simulate_prompt_answer("Force Remove");
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert_eq!(outcome, WorktreeRemovalOutcome::Removed { branch_error: None });

        let worktrees = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap();
        assert!(!worktrees.iter().any(|wt| wt.path == linked_wt.path));
    }

    #[gpui::test]
    async fn test_menu_partial_success_branch_failure(cx: &mut TestAppContext) {
        let (fs, _project, repository, linked_wt, workspace, mut cx) = init_test_repo(cx).await;
        let identity = repo_identity(&repository, &cx);

        // Force the linked branch delete to fail (requires force that we deny),
        // producing the partial result: worktree removed, branch_error set.
        fs.with_git_state(path!("/root/project/.git").as_ref(), true, |state| {
            state.branches_requiring_force_delete.insert("linked-wt".to_string());
        })
        .unwrap();

        let task = cx.update(|window, cx| {
            delete_service::confirm_remove_worktree(
                repository.clone(),
                identity,
                linked_wt.clone(),
                workspace.downgrade(),
                window,
                cx,
            )
        });
        cx.run_until_parked();

        let modal = workspace
            .read_with(&cx, |ws, cx| ws.active_modal::<WorktreeRemovalConfirmModal>(cx).unwrap());
        modal.update(&mut cx, |modal, cx| {
            modal.test_set_delete_linked_branch(true);
            cx.notify();
        });
        cx.dispatch_action(Confirm);
        cx.run_until_parked();

        // Branch requires force; cancel it -> partial success.
        assert!(cx.has_pending_prompt());
        cx.simulate_prompt_answer("Cancel");
        cx.run_until_parked();

        let outcome = task.await.unwrap();
        assert!(
            matches!(outcome, WorktreeRemovalOutcome::Removed { branch_error: Some(_) }),
            "worktree removed but linked-branch deletion failed should be a partial success"
        );

        let worktrees = repository
            .update(&mut cx, |repo, _| repo.worktrees())
            .await
            .unwrap()
            .unwrap();
        assert!(!worktrees.iter().any(|wt| wt.path == linked_wt.path));
    }
}
