use crate::{
    commit_view::CommitView,
    git_graph::{GitGraph, RefKind, ResolvedRef},
    git_graph_actions::{GraphMutation, GraphMutationError},
    git_ref_modal::{GitRefModal, GitRefModalKind, GitRefModalResult},
    open_branch_rename_modal,
};
use anyhow::anyhow;
use futures::channel::oneshot;
use git::Oid;
use git::repository::{AskPassDelegate, CreateTagOptions, MergeMode, Remote, ResetMode};
use git_ui_core::askpass_modal::AskPassModal;
use git_ui_core::delete_service::{self as delete_service, WorktreeRemovalOutcome};
use git_ui_core::notifications::show_error_toast;
use git_ui_core::worktree_name_modal::WorktreeNameModal;
use git_ui_core::worktree_service::{
    HostScopedRepositoryIdentity, handle_create_worktree, linked_worktree_label, switch_to_worktree,
};
use gpui::{
    Action, AnyWindowHandle, App, AsyncWindowContext, ClipboardItem, Entity, FocusHandle,
    SharedString, Task, WeakEntity, Window, actions,
};
use gpui::{DismissEvent, EventEmitter, Focusable, InteractiveElement, ParentElement};
use menu;
use notifications::status_toast::StatusToast;
use project::{GIT_COMMAND_TASK_TAG, git_store::Repository};
use task::{TaskContext, TaskVariables, VariableName};
use ui::{
    Button, ButtonStyle, Color, ContextMenu, ContextMenuEntry, Headline, HeadlineSize, IconName,
    IconPosition, Label, prelude::*,
};
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

/// Schedules a per-ref operation through the graph with a duplicate-dispatch
/// guard. While one ref operation is in flight, further dispatches are
/// suppressed; the guard is cleared when the operation settles (success or
/// error) or the graph is no longer available. Errors are propagated to the UI
/// through `detach_and_prompt_err` — never silently dropped.
fn schedule_ref_operation(
    graph: Option<WeakEntity<GitGraph>>,
    make_task: impl FnOnce(
        &mut GitGraph,
        &mut gpui::Context<GitGraph>,
    ) -> Result<Task<anyhow::Result<()>>, GraphMutationError>,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(graph) = graph.and_then(|graph| graph.upgrade()) else {
        log::error!("ref action: git graph is no longer available");
        return;
    };
    let (task, weak) = graph.update(cx, |graph, graph_cx| {
        let weak = graph_cx.entity().downgrade();
        if !graph.begin_ref_operation() {
            // A ref operation is already in flight; suppress the duplicate
            // dispatch rather than queueing another mutation.
            return (None, weak);
        }
        let result = match make_task(graph, graph_cx) {
            Ok(task) => task,
            Err(error) => {
                graph.end_ref_operation();
                Task::ready(Err(anyhow!(error.to_string())))
            }
        };
        (Some(result), weak)
    });
    let Some(task) = task else {
        // Duplicate dispatch suppressed while a ref operation is in flight.
        return;
    };
    window
        .spawn(cx, async move |cx| {
            let result = task.await;
            weak.update(cx, |graph, _| graph.end_ref_operation()).ok();
            result
        })
        .detach_and_prompt_err("Git graph action failed", window, cx, |error, _, _| {
            Some(error.to_string())
        });
}

/// Builds the typed, per-ref context menu deployed from a Git Graph ref-chip
/// right-click. The menu is keyed to one fully-qualified canonical ref
/// (`ResolvedRef`) and the clicked commit's SHA; every action targets that exact
/// ref — never a fallback to the current/first branch.
///
/// Local branches get Checkout, Merge into Current, Create Detached Worktree,
/// Rename, Delete, and Copy Name. The current branch omits Checkout and Delete.
/// Remote refs and tags get the commit-target actions (Merge into Current,
/// Create Detached Worktree) and Copy Name; their chips are still typed to the
/// exact ref so Copy Name never mixes local/remote same-name refs.
pub(crate) fn ref_chip_context_menu(
    resolved_ref: ResolvedRef,
    commit_sha: git::Oid,
    is_current_branch: bool,
    repository: Option<WeakEntity<Repository>>,
    graph: Option<WeakEntity<GitGraph>>,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<ContextMenu> {
    let is_local_branch = resolved_ref.kind == RefKind::LocalBranch;
    let is_remote_branch = resolved_ref.kind == RefKind::RemoteBranch;
    let display_name = resolved_ref.display_name.clone();
    let header = format!("Ref {display_name}");

    // A local branch checked out in another (non-main) linked worktree is a
    // worktree destination, not an ordinary checkout target. Resolved from the
    // live repository snapshot against the worktree's exact fully-qualified
    // ref (`refs/heads/<name>`), never by the shortened display text — so a
    // local branch literally named `origin/main` is never confused with the
    // unrelated `refs/remotes/origin/main` remote-tracking ref. The snapshot
    // already excludes the main worktree, and the `!is_main` guard keeps this a
    // linked destination even if a snapshot ever carried the main worktree.
    let linked_worktree: Option<git::repository::Worktree> = if is_local_branch {
        let full_ref = resolved_ref.ref_name.clone();
        repository
            .as_ref()
            .and_then(|repository| repository.upgrade())
            .and_then(|repository| {
                repository
                    .read(cx)
                    .linked_worktrees
                    .iter()
                    .find(|worktree| {
                        !worktree.is_main && worktree.ref_name.as_deref() == Some(full_ref.as_ref())
                    })
                    .cloned()
            })
    } else {
        None
    };
    // Build-time "potentially eligible" flag for the linked-branch `Remove
    // Worktree…` entry, mirroring the commit menu's linked-worktree eligibility
    // logic (not the main worktree, and not open in any Zed window). The
    // authoritative open/disappearance re-check happens in the shared remove
    // service immediately before mutation, so a stale menu never mutates
    // against a worktree that vanished or was replaced while the menu was open.
    let linked_worktree_eligible = linked_worktree.as_ref().map(|worktree| {
        let window = &*window;
        let cx = &*cx;
        let workspace_entity = workspace.upgrade();
        let identity = repository
            .as_ref()
            .and_then(|repository| repository.upgrade())
            .map(|repository| {
                let common_identity = repository.read(cx).common_repository_identity();
                let remote_options = workspace_entity.as_ref().and_then(|workspace| {
                    workspace
                        .read(cx)
                        .project()
                        .read(cx)
                        .remote_connection_options(cx)
                });
                HostScopedRepositoryIdentity::new(common_identity, remote_options.as_ref())
            });
        worktree_removal_eligible(worktree, identity.as_ref(), window, cx)
    });
    let linked_worktree_label = linked_worktree.as_ref().map(linked_worktree_label);

    ContextMenu::build(window, cx, move |menu, _window, _cx| {
        let mut menu = menu.header(header);

        if let Some(worktree) = &linked_worktree {
            // Linked-worktree branch: route to the exact worktree instead of
            // offering ordinary Checkout / Rename / direct branch Delete. The
            // `Switch Here` path reuses the shared worktree service switch,
            // which fails safely (toast) when the target is already current,
            // has disappeared, or the snapshot is stale.
            let switch_workspace = workspace.clone();
            let switch_path = worktree.path.clone();
            let switch_label = linked_worktree_label
                .clone()
                .unwrap_or_else(|| display_name.clone().into());
            let switch_offer_sha = commit_sha.to_string();
            menu = menu.entry("Switch Here", None, move |window, cx| {
                let Some(workspace) = switch_workspace.upgrade() else {
                    log::error!(
                        "linked branch switch: source window handle for {} is no longer \
                         available",
                        switch_path.display()
                    );
                    return;
                };
                workspace.update(cx, |workspace, cx| {
                    switch_to_worktree(
                        workspace,
                        switch_path.clone(),
                        switch_label.clone(),
                        Some(switch_offer_sha.clone().into()),
                        window,
                        None,
                        OpenMode::Activate,
                        cx,
                    );
                });
            });
            menu = menu.entry(
                format!("Open {display_name} in New Window"),
                None,
                {
                    let new_window_path = worktree.path.clone();
                    move |window, cx| {
                        window.dispatch_action(
                            Box::new(OpenWorktreeInNewWindow {
                                path: new_window_path.clone(),
                            }),
                            cx,
                        );
                    }
                },
            );
            if linked_worktree_eligible == Some(true) {
                let remove_repository = repository.clone();
                let remove_graph = graph.clone();
                let remove_workspace = workspace.clone();
                let remove_worktree = worktree.clone();
                menu = menu.entry("Remove Worktree…", None, move |window, cx| {
                    remove_worktree_from_graph(
                        remove_repository.clone(),
                        remove_graph.clone(),
                        remove_workspace.clone(),
                        remove_worktree.clone(),
                        window,
                        cx,
                    );
                });
            }
        } else if is_local_branch {
            if !is_current_branch {
                menu = menu.entry("Checkout", None, {
                    let graph = graph.clone();
                    let display_name = display_name.clone();
                    move |window, cx| {
                        schedule_ref_operation(
                            graph.clone(),
                            |graph, graph_cx| {
                                graph.schedule_branch_checkout(
                                    display_name.clone().into(),
                                    commit_sha,
                                    graph_cx,
                                )
                            },
                            window,
                            cx,
                        );
                    }
                });
            }
            menu = menu.entry("Merge into Current", None, {
                let graph = graph.clone();
                move |window, cx| schedule_ref_merge(graph.clone(), commit_sha, window, cx)
            });
            menu = menu.entry("Create Detached Worktree", None, {
                let repository = repository.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    create_detached_worktree_for_commit(
                        repository.clone(),
                        workspace.clone(),
                        commit_sha,
                        window,
                        cx,
                    );
                }
            });
            menu = menu.entry("Rename…", None, {
                let repository = repository.clone();
                let workspace = workspace.clone();
                let display_name = display_name.clone();
                move |window, cx| {
                    rename_ref_branch(repository.clone(), workspace.clone(), display_name.as_ref(), window, cx);
                }
            });
            if !is_current_branch {
                menu = menu.entry("Delete…", None, {
                    let repository = repository.clone();
                    let workspace = workspace.clone();
                    let display_name = display_name.clone();
                    move |window, cx| {
                        delete_ref_branch(repository.clone(), workspace.clone(), display_name.as_ref(), window, cx);
                    }
                });
            }
        } else if is_remote_branch {
            // Remote-tracking branch: check out the exact clicked upstream (with
            // exact-tracker reuse, else a distinct local name that never repoints
            // an existing branch) and delete it on its server remote. Local-only
            // Rename/Delete are never offered for a remote chip.
            menu = menu.entry("Checkout", None, {
                let resolved_ref = resolved_ref.clone();
                let repository = repository.clone();
                let graph = graph.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    schedule_remote_branch_checkout(
                        &resolved_ref,
                        repository.clone(),
                        graph.clone(),
                        workspace.clone(),
                        window,
                        cx,
                    );
                }
            });
            menu = menu.entry("Merge into Current", None, {
                let graph = graph.clone();
                move |window, cx| schedule_ref_merge(graph.clone(), commit_sha, window, cx)
            });
            menu = menu.entry("Create Detached Worktree", None, {
                let repository = repository.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    create_detached_worktree_for_commit(
                        repository.clone(),
                        workspace.clone(),
                        commit_sha,
                        window,
                        cx,
                    );
                }
            });
            menu = menu.entry("Delete on Server…", None, {
                let resolved_ref = resolved_ref.clone();
                let repository = repository.clone();
                let graph = graph.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    schedule_remote_branch_delete_on_server(
                        &resolved_ref,
                        repository.clone(),
                        graph.clone(),
                        workspace.clone(),
                        window,
                        cx,
                    );
                }
            });
        } else {
            // Tags and other non-branch refs keep the commit-target actions;
            // the remote branch actions above are remote-chip-specific.
            menu = menu.entry("Merge into Current", None, {
                let graph = graph.clone();
                move |window, cx| schedule_ref_merge(graph.clone(), commit_sha, window, cx)
            });
            menu = menu.entry("Create Detached Worktree", None, {
                let repository = repository.clone();
                let workspace = workspace.clone();
                move |window, cx| {
                    create_detached_worktree_for_commit(
                        repository.clone(),
                        workspace.clone(),
                        commit_sha,
                        window,
                        cx,
                    );
                }
            });
        }

        menu.entry("Copy Name", None, move |_window, cx| {
            cx.write_to_clipboard(ClipboardItem::new_string(display_name.to_string()));
        })
    })
}

/// Merges the clicked ref's commit into the current branch, reusing the graph's
/// `Merge` mutation (clicked SHA) with the duplicate-dispatch guard.
fn schedule_ref_merge(
    graph: Option<WeakEntity<GitGraph>>,
    commit_sha: git::Oid,
    window: &mut Window,
    cx: &mut App,
) {
    schedule_ref_operation(
        graph.clone(),
        |graph, graph_cx| {
            graph.schedule_mutation(
                GraphMutation::Merge {
                    commit: commit_sha,
                    mode: MergeMode::Default,
                },
                Some(commit_sha),
                graph_cx,
            )
        },
        window,
        cx,
    );
}

/// Splits a canonical `refs/remotes/<remote>/<branch>` ref into its remote and
/// branch names. Returns `None` for malformed/empty refs (the caller then fails
/// visibly rather than guessing at a remote).
fn remote_ref_parts(ref_name: &str) -> Option<(String, String)> {
    let stripped = ref_name.strip_prefix("refs/remotes/")?;
    let (remote, branch) = stripped.split_once('/')?;
    if remote.is_empty() || branch.is_empty() {
        return None;
    }
    Some((remote.to_string(), branch.to_string()))
}

/// Acquires the ref-operation duplicate-dispatch guard from the graph. Returns
/// the graph's weak handle when the guard was acquired (the caller MUST call
/// `end_ref_operation` once the operation settles), or `None` when a ref
/// operation is already in flight (the dispatch is suppressed) or the graph is
/// gone.
fn begin_graph_ref_operation(
    graph: &Option<WeakEntity<GitGraph>>,
    cx: &mut App,
) -> Option<WeakEntity<GitGraph>> {
    let graph = graph.as_ref().and_then(|graph| graph.upgrade())?;
    if !graph.update(cx, |graph, _| graph.begin_ref_operation()) {
        return None;
    }
    Some(graph.downgrade())
}

/// A compact destructive-confirmation modal naming exactly what will be
/// destroyed. Distinct from the worktree-removal modal and reused for
/// branch-server-delete and tag-delete confirmations, which must be worded
/// differently and must never be implied by a generic "Delete".
pub(crate) struct RefDestroyConfirmModal {
    title: SharedString,
    body: SharedString,
    confirm_label: SharedString,
    focus_handle: FocusHandle,
    tx: Option<oneshot::Sender<bool>>,
}

impl EventEmitter<DismissEvent> for RefDestroyConfirmModal {}

impl Focusable for RefDestroyConfirmModal {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl workspace::ModalView for RefDestroyConfirmModal {}

impl Render for RefDestroyConfirmModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let title = self.title.clone();
        let body = self.body.clone();
        let confirm_label = self.confirm_label.clone();

        v_flex()
            .key_context("RefDestroyConfirmModal")
            .elevation_3(cx)
            .p_4()
            .gap_4()
            .w(rems(30.))
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
                    .child(Headline::new(title).size(HeadlineSize::Small))
                    .child(Label::new(body).color(Color::Muted)),
            )
            .child(
                h_flex()
                    .justify_end()
                    .gap_2()
                    .child(Button::new("cancel", "Cancel").on_click(cx.listener(
                        |this, _, window, cx| {
                            this.cancel(window, cx);
                        },
                    )))
                    .child(
                        Button::new("destroy", confirm_label.clone())
                            .style(ButtonStyle::Filled)
                            .color(Color::Error)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.confirm(window, cx);
                            })),
                    ),
            )
    }
}

impl RefDestroyConfirmModal {
    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(true);
        }
        cx.emit(DismissEvent);
    }

    fn cancel(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(false);
        }
        cx.emit(DismissEvent);
    }
}

/// Opens the destructive-confirmation modal from inside an async flow and
/// awaits the user's decision (or `false` if the workspace/graph is gone).
/// Opens the destructive-confirmation modal from inside an async flow and
/// returns a oneshot that yields the user's decision (`true` = confirmed,
/// `false` = cancelled or the workspace/graph disappeared).
/// Opens the destructive-confirmation modal from inside an async flow and
/// returns a oneshot that yields the user's decision (`true` = confirmed,
/// `false` = cancelled or the workspace/graph disappeared).
fn open_destructive_confirmation(
    workspace: &WeakEntity<Workspace>,
    cx: &mut AsyncWindowContext,
    title: SharedString,
    body: SharedString,
    confirm_label: SharedString,
) -> oneshot::Receiver<bool> {
    let (tx, rx) = oneshot::channel();
    if let Some(workspace_entity) = workspace.upgrade() {
        let _ = workspace_entity.update_in(cx, |workspace, window, cx| {
            workspace.toggle_modal(window, cx, |_window, cx| {
                let focus_handle = cx.focus_handle();
                RefDestroyConfirmModal {
                    title: title.clone(),
                    body: body.clone(),
                    confirm_label: confirm_label.clone(),
                    focus_handle,
                    tx: Some(tx),
                }
            })
        });
    }
    rx
}

/// Surfaces an actionable error toast from inside an async flow (e.g. expired
/// repository, auth/network failure, or an unknown error).
fn emit_error_async(
    workspace: &WeakEntity<Workspace>,
    cx: &mut AsyncWindowContext,
    action: &str,
    e: anyhow::Error,
) {
    if let Some(workspace_entity) = workspace.upgrade() {
        let action = action.to_string();
        let for_toast = workspace_entity.clone();
        let _ = workspace_entity.update(cx, |_, app_cx| {
            show_error_toast(for_toast, action, e, app_cx)
        });
    }
}

/// Surfaces a plain informational/success toast from inside an async flow.
fn emit_toast(
    workspace: &WeakEntity<Workspace>,
    cx: &mut AsyncWindowContext,
    message: impl Into<SharedString>,
) {
    let message = message.into();
    if let Some(workspace_entity) = workspace.upgrade() {
        let for_toast = workspace_entity.clone();
        let _ = for_toast.update(cx, |workspace, app_cx| {
            let status_toast =
                StatusToast::new(message.clone(), app_cx, |this, _| this.dismiss_button(true));
            workspace.toggle_status_toast(status_toast, app_cx);
        });
    }
}

/// Checkout of a clicked remote-tracking branch (`refs/remotes/<remote>/<b>`),
/// locked to the exact clicked ref. If a local branch already tracks that exact
/// upstream, switches to it; otherwise Zed proposes a local name and creates a
/// tracking branch, never repointing an existing branch whose upstream differs
/// or is absent (a same-name collision yields a distinct proposed name).
fn schedule_remote_branch_checkout(
    resolved_ref: &ResolvedRef,
    repository: Option<WeakEntity<Repository>>,
    graph: Option<WeakEntity<GitGraph>>,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) {
    let ref_name = resolved_ref.ref_name.as_ref().to_string();
    let Some((remote, branch)) = remote_ref_parts(&ref_name) else {
        emit_error_async(
            &workspace,
            &mut window.to_async(cx),
            "checkout remote branch",
            anyhow!("Malformed remote branch ref {ref_name}"),
        );
        return;
    };
    let Some(repository_entity) = repository.and_then(|r| r.upgrade()) else {
        emit_error_async(
            &workspace,
            &mut window.to_async(cx),
            "checkout remote branch",
            anyhow!("The repository is no longer available"),
        );
        return;
    };
    let Some(graph_weak) = begin_graph_ref_operation(&graph, cx) else {
        return;
    };
    window
        .spawn(cx, async move |cx| {
            let result = async {
                let scan = repository_entity
                    .update(cx, |repository, _| repository.branches())
                    .await
                    .map_err(|e| anyhow!("branch scan cancelled: {e}"))??;

                // 1. Exact tracker: a local branch whose upstream is this very
                //    remote ref. Switching to it preserves existing tracking.
                let target_upstream = format!("refs/remotes/{remote}/{branch}");
                let exact_tracker = scan.branches.iter().find(|b| {
                    !b.is_remote()
                        && b.upstream
                            .as_ref()
                            .map(|u| u.ref_name.as_ref())
                            == Some(target_upstream.as_str())
                });
                if let Some(tracker) = exact_tracker {
                    let local_name = tracker.name().to_string();
                    repository_entity
                        .update(cx, |repository, _| repository.change_branch(local_name.clone()))
                        .await
                        .map_err(|e| anyhow!("checkout cancelled: {e}"))??;
                    emit_toast(&workspace, cx, format!("Switched to \"{local_name}\""));
                    return Ok(());
                }

                // 2. No exact tracker. Propose `<branch>` unless a local branch
                //    of that name already exists (tracking something else or an
                //    absent upstream) — then use a distinct collision-free
                //    name. Never repoint the existing branch.
                let local_names: std::collections::HashSet<String> = scan
                    .branches
                    .iter()
                    .filter(|b| !b.is_remote())
                    .map(|b| b.name().to_string())
                    .collect();
                let mut proposed = branch.clone();
                let mut suffix = 2;
                while local_names.contains(&proposed) {
                    proposed = format!("{branch}-{suffix}");
                    suffix += 1;
                }
                // Creating a local branch from the remote-tracking ref sets up
                // exact upstream tracking and checks it out.
                repository_entity
                    .update(cx, |repository, _| {
                        repository.create_branch(proposed.clone(), Some(target_upstream.clone()))
                    })
                    .await
                    .map_err(|e| anyhow!("create branch cancelled: {e}"))??;
                emit_toast(
                    &workspace,
                    cx,
                    format!("Checked out \"{proposed}\" tracking {remote}/{branch}"),
                );
                Ok(())
            }
            .await;
            graph_weak.update(cx, |graph, _| graph.end_ref_operation()).ok();
            result
        })
        .detach_and_prompt_err(
            "Git graph remote branch checkout failed",
            window,
            cx,
            |error, _, _| Some(error.to_string()),
        );
}

/// Collects the configured remote names for the repository.
async fn configured_remote_names(
    repository_entity: &Entity<Repository>,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<Vec<SharedString>> {
    let remotes = repository_entity
        .update(cx, |repository, _| repository.get_remotes(None, true))
        .await
        .map_err(|e| anyhow!("get remotes cancelled: {e}"))??;
    Ok(remotes.into_iter().map(|r| r.name).collect())
}

/// Explicitly prompts the user to pick a configured remote, even when only one
/// exists. Returns `None` when there are no remotes or the selection is
/// cancelled.
async fn select_remote_explicit(
    repository_entity: &Entity<Repository>,
    workspace: WeakEntity<Workspace>,
    window_handle: AnyWindowHandle,
    cx: &mut AsyncWindowContext,
    prompt: &str,
) -> anyhow::Result<Option<Remote>> {
    let names = configured_remote_names(repository_entity, cx).await?;
    if names.is_empty() {
        return Ok(None);
    }
    let selection = window_handle
        .update(cx, |_, window, app_cx| {
            crate::picker_prompt::prompt_explicit(prompt, names.clone(), workspace, window, app_cx)
        })
        .map_err(|e| anyhow!("{e}"))?;
    Ok(selection
        .await
        .map(|index| Remote {
            name: names[index].clone(),
        }))
}

fn remote_delete_summary(remote: &str, stderr: String) -> String {
    let detail = if stderr.trim().is_empty() {
        "deleted".to_string()
    } else {
        stderr.trim().to_string()
    };
    format!("{remote}: {detail}")
}

/// Builds an [`AskPassDelegate`] wired to the `AskPassModal`, reusing Zed's
/// existing AskPass seam. Constructed in the synchronous flow (where a live
/// `&mut App` is available) and moved into the async operation.
fn askpass_delegate(
    workspace: &WeakEntity<Workspace>,
    window_handle: AnyWindowHandle,
    cx: &mut App,
    operation: &str,
) -> AskPassDelegate {
    let workspace = workspace.clone();
    let operation: SharedString = operation.to_string().into();
    AskPassDelegate::new(&mut cx.to_async(), move |prompt, tx, cx| {
        let workspace = workspace.clone();
        let operation = operation.clone();
        window_handle
            .update(cx, |_, window, cx| {
                workspace.update(cx, |workspace, cx| {
                    workspace.toggle_modal(window, cx, |window, cx| {
                        AskPassModal::new(operation.clone(), prompt.into(), tx, window, cx)
                    });
                })
            })
            .ok();
    })
}

/// Deletes the clicked remote-tracking branch on its server remote via an
/// explicit `git push <remote> --delete refs/heads/<branch>` — never a local-only
/// remote-tracking deletion. Requires a destructive confirmation naming the
/// remote and branch; clears the pending guard on success, cancellation,
/// expired repository, and any error.
fn schedule_remote_branch_delete_on_server(
    resolved_ref: &ResolvedRef,
    repository: Option<WeakEntity<Repository>>,
    graph: Option<WeakEntity<GitGraph>>,
    workspace: WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) {
    let ref_name = resolved_ref.ref_name.as_ref().to_string();
    let Some((remote, branch)) = remote_ref_parts(&ref_name) else {
        emit_error_async(
            &workspace,
            &mut window.to_async(cx),
            "delete remote branch",
            anyhow!("Malformed remote branch ref {ref_name}"),
        );
        return;
    };
    let Some(repository_entity) = repository.and_then(|r| r.upgrade()) else {
        emit_error_async(
            &workspace,
            &mut window.to_async(cx),
            "delete remote branch",
            anyhow!("The repository is no longer available"),
        );
        return;
    };
    let Some(graph_weak) = begin_graph_ref_operation(&graph, cx) else {
        return;
    };
    let window_handle = window.window_handle();
    let askpass = askpass_delegate(&workspace, window_handle, cx, "git push");
    window
        .spawn(cx, async move |cx| {
            let result = async {
                let confirmed = open_destructive_confirmation(
                    &workspace,
                    cx,
                    "Delete Branch on Remote".into(),
                    format!(
                        "Delete branch \"{branch}\" on remote \"{remote}\"? This removes it on the server and cannot be undone."
                    )
                    .into(),
                    format!("Delete {remote}/{branch}").into(),
                )
                .await
                .unwrap_or(false);
                if !confirmed {
                    // Cancellation clears pending state without dispatch.
                    return Ok(());
                }
                let remote_name =
                    match select_remote_explicit(
                        &repository_entity,
                        workspace.clone(),
                        window_handle,
                        cx,
                        "Pick which remote to delete the branch from",
                    )
                    .await?
                    {
                        Some(remote) => remote.name,
                        None => {
                            // Zero remotes: informational, no dispatch.
                            emit_toast(
                                &workspace,
                                cx,
                                format!("No configured remote to delete {branch} from."),
                            );
                            return Ok(());
                        }
                    };
                let output = repository_entity
                    .update(cx, |repository, cx| {
                        repository.delete_refs_on_remote(
                            remote_name.clone(),
                            vec![format!("refs/heads/{branch}")],
                            askpass,
                            cx,
                        )
                    })
                    .await
                    .map_err(|e| anyhow!("remote delete cancelled: {e}"))?;
                let output = output?;
                emit_toast(&workspace, cx, remote_delete_summary(&remote_name, output.stderr));
                Ok(())
            }
            .await;
            graph_weak.update(cx, |graph, _| graph.end_ref_operation()).ok();
            result
        })
        .detach_and_prompt_err(
            "Git graph remote branch delete failed",
            window,
            cx,
            |error, _, _| Some(error.to_string()),
        );
}


/// Opens the single shared prefill renaming modal for the exact canonical
/// branch captured by the chip. Cancellation dispatches nothing and retains
/// state; Git errors are surfaced by the modal's own error handling.
fn rename_ref_branch(
    repository: Option<WeakEntity<Repository>>,
    workspace: WeakEntity<Workspace>,
    branch_name: &str,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(repository) = repository.and_then(|repository| repository.upgrade()) else {
        if let Some(workspace) = workspace.upgrade() {
            show_error_toast(
                workspace,
                "rename branch",
                anyhow!("The repository is no longer available"),
                cx,
            );
        }
        return;
    };
    let Some(workspace) = workspace.upgrade() else {
        log::error!("rename branch: workspace is no longer available");
        return;
    };
    let branch_name = branch_name.to_string();
    // Spawn into the window so we get a visual context through which we can
    // open the modal on the workspace entity.
    window
        .spawn(cx, async move |cx| {
            let _ = workspace.update_in(cx, |workspace, window, cx| {
                open_branch_rename_modal(
                    workspace,
                    branch_name.clone(),
                    repository.clone(),
                    window,
                    cx,
                )
            });
        })
        .detach();
}

/// Deletes the exact branch captured by the chip via the shared store-backed
/// delete + force-confirm service. The service surfaces git errors (and the
/// unmerged-branch force-delete prompt) itself.
fn delete_ref_branch(
    repository: Option<WeakEntity<Repository>>,
    workspace: WeakEntity<Workspace>,
    branch_name: &str,
    window: &mut Window,
    cx: &mut App,
) {
    let Some(repository) = repository.and_then(|repository| repository.upgrade()) else {
        if let Some(workspace) = workspace.upgrade() {
            show_error_toast(
                workspace,
                "delete branch",
                anyhow!("The repository is no longer available"),
                cx,
            );
        }
        return;
    };
    let display_name = SharedString::from(branch_name.to_string());
    let task = delete_service::delete_branch(
        repository,
        false,
        branch_name.to_string(),
        display_name,
        false,
        workspace,
        window,
        cx,
    );
    task.detach();
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
