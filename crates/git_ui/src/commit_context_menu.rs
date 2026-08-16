use crate::{
    commit_view::CommitView,
    git_graph::GitGraph,
    git_graph_actions::GraphMutation,
    git_ref_modal::{GitRefModal, GitRefModalKind, GitRefModalResult},
};
use anyhow::anyhow;
use git::Oid;
use git::repository::{CreateTagOptions, MergeMode, ResetMode};
use git_ui_core::notifications::show_error_toast;
use git_ui_core::worktree_name_modal::WorktreeNameModal;
use git_ui_core::worktree_service::{handle_create_worktree, handle_switch_worktree};
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
use zed_actions::{NewWorktreeBranchTarget, OpenWorktreeInNewWindow, SwitchWorktree};

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
                let worktrees = matching_worktrees.clone();
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
                    for worktree in &worktrees {
                        let path = worktree.path.clone();
                        let display_name = worktree
                            .path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("worktree")
                            .to_string();
                        let switch_workspace = workspace_for_entry.clone();
                        let switch_path = path.clone();
                        let switch_display_name = display_name.clone();
                        menu = menu.entry(
                            format!("Switch to {display_name}"),
                            None,
                            move |window, cx| {
                                if let Some(workspace) = switch_workspace.upgrade() {
                                    workspace.update(cx, |workspace, cx| {
                                        handle_switch_worktree(
                                            workspace,
                                            &SwitchWorktree {
                                                path: switch_path.clone(),
                                                display_name: switch_display_name.clone(),
                                            },
                                            window,
                                            None,
                                            OpenMode::Activate,
                                            cx,
                                        );
                                    });
                                }
                            },
                        );
                        let new_window_path = path.clone();
                        menu = menu.entry(
                            format!("Open {display_name} in New Window"),
                            None,
                            move |window, cx| {
                                window.dispatch_action(
                                    Box::new(OpenWorktreeInNewWindow {
                                        path: new_window_path.clone(),
                                    }),
                                    cx,
                                );
                            },
                        );
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
