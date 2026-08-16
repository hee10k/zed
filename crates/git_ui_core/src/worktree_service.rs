use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use askpass::AskPassDelegate;
use collections::HashSet;
use fs::Fs;
use gpui::{
    AppContext, AsyncWindowContext, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable,
    SharedString, Task, TaskExt, WeakEntity,
};
use project::Project;
use project::git_store::{CommonRepositoryIdentity, Repository};
use project::project_settings::ProjectSettings;
use project::trusted_worktrees::{PathTrust, TrustedWorktrees};
use remote::RemoteConnectionOptions;
use settings::Settings;
use ui::prelude::*;
use workspace::{
    MultiWorkspace, OpenMode, PreviousWorkspaceState, ToastView, Workspace, dock::DockPosition,
};
use zed_actions::NewWorktreeBranchTarget;

use git::repository::{Branch, CreateWorktreeTarget, FetchOptions, Remote};

use util::ResultExt as _;

use crate::askpass_modal::AskPassModal;
use crate::notifications::{open_output, show_error_toast};
use crate::worktree_names;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HostScopedRepositoryIdentity {
    pub common_identity: CommonRepositoryIdentity,
    pub host_key: String,
}

impl HostScopedRepositoryIdentity {
    pub fn new(
        common_identity: CommonRepositoryIdentity,
        options: Option<&RemoteConnectionOptions>,
    ) -> Self {
        let host_key = match options {
            None => "local".to_string(),
            Some(RemoteConnectionOptions::Ssh(opts)) => {
                let mut key = opts.host.to_string();
                if let Some(port) = opts.port {
                    key.push_str(&format!(":{}", port));
                }
                key
            }
            Some(RemoteConnectionOptions::Wsl(opts)) => opts.distro_name.clone(),
            Some(RemoteConnectionOptions::Docker(opts)) => {
                let kind = if opts.use_podman { "podman" } else { "docker" };
                format!("{}:{}", kind, opts.container_id)
            }
            #[cfg(any(test, feature = "test-support"))]
            Some(RemoteConnectionOptions::Mock(opts)) => format!("mock-{}", opts.id),
            #[allow(unreachable_patterns)]
            _ => "remote".to_string(),
        };
        Self {
            common_identity,
            host_key,
        }
    }
}

pub fn app_workspaces(cx: &gpui::App) -> anyhow::Result<Vec<gpui::Entity<workspace::Workspace>>> {
    app_workspaces_with_active_window(None, cx)
}

pub fn app_workspaces_with_active_window(
    active_window: Option<&gpui::Window>,
    cx: &gpui::App,
) -> anyhow::Result<Vec<gpui::Entity<workspace::Workspace>>> {
    let mut workspaces = Vec::new();
    let active_window_id = active_window.map(|w| w.window_handle().window_id());
    if let Some(active_window) = active_window {
        if let Some(multi_workspace) = active_window.root::<workspace::MultiWorkspace>().flatten() {
            workspaces.extend(multi_workspace.read(cx).workspaces().cloned());
        }
    }
    for window in cx.windows() {
        if Some(window.window_id()) == active_window_id {
            continue;
        }
        if let Some(multi_workspace) = window.downcast::<workspace::MultiWorkspace>() {
            let res = multi_workspace.read_with(cx, |mw, _| mw.workspaces().cloned().collect::<Vec<_>>());
            if let Ok(ws) = res {
                for w in ws {
                    if !workspaces.contains(&w) {
                        workspaces.push(w);
                    }
                }
            }
        }
    }
    Ok(workspaces)
}

/// A remote-tracking branch reference parsed into its remote and branch parts,
/// e.g. `origin/main` -> remote `origin`, branch `main`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteBranchName {
    pub remote_name: String,
    pub branch_name: String,
}

impl RemoteBranchName {
    pub fn parse(name: &str) -> Option<Self> {
        let name = name.strip_prefix("refs/remotes/").unwrap_or(name);
        let (remote_name, branch_name) = name.split_once('/')?;
        if remote_name.is_empty() || branch_name.is_empty() {
            return None;
        }
        Some(Self {
            remote_name: remote_name.to_string(),
            branch_name: branch_name.to_string(),
        })
    }

    pub fn display_name(&self) -> String {
        format!("{}/{}", self.remote_name, self.branch_name)
    }
}

/// A "create new worktree" option offered to the user. The set of targets is
/// derived from repository state by [`worktree_create_targets`] so that the
/// worktree picker and the sidebar's new-thread menu stay in sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorktreeCreateTarget {
    CurrentBranch,
    DefaultBranch(RemoteBranchName),
}

impl WorktreeCreateTarget {
    pub fn branch_target(&self) -> NewWorktreeBranchTarget {
        match self {
            WorktreeCreateTarget::CurrentBranch => NewWorktreeBranchTarget::CurrentBranch,
            WorktreeCreateTarget::DefaultBranch(default_branch) => {
                NewWorktreeBranchTarget::RemoteBranch {
                    remote_name: default_branch.remote_name.clone(),
                    branch_name: default_branch.branch_name.clone(),
                }
            }
        }
    }

    pub fn branch_label(
        &self,
        has_multiple_repositories: bool,
        current_branch_name: Option<&str>,
    ) -> String {
        match self {
            WorktreeCreateTarget::DefaultBranch(default_branch) => default_branch.display_name(),
            WorktreeCreateTarget::CurrentBranch => {
                if has_multiple_repositories {
                    "current branches".to_string()
                } else {
                    current_branch_name.unwrap_or("HEAD").to_string()
                }
            }
        }
    }
}

pub fn worktree_branch_target(branch: &Branch) -> NewWorktreeBranchTarget {
    if let Some(remote_name) = branch.remote_name() {
        let branch_name = branch
            .name()
            .strip_prefix(remote_name)
            .and_then(|name| name.strip_prefix('/'))
            .unwrap_or(branch.name());
        NewWorktreeBranchTarget::RemoteBranch {
            remote_name: remote_name.to_string(),
            branch_name: branch_name.to_string(),
        }
    } else {
        NewWorktreeBranchTarget::ExistingBranch {
            name: branch.name().to_string(),
        }
    }
}

/// Determines which "create new worktree" options to surface for the given
/// repository state: prefer the remote default branch when it differs from the
/// current branch, and otherwise offer the current branch.
pub fn worktree_create_targets(
    has_multiple_repositories: bool,
    default_branch: Option<RemoteBranchName>,
    current_branch_name: Option<&str>,
) -> Vec<WorktreeCreateTarget> {
    if has_multiple_repositories {
        return vec![WorktreeCreateTarget::CurrentBranch];
    }
    let Some(default_branch) = default_branch else {
        return vec![WorktreeCreateTarget::CurrentBranch];
    };
    let is_different =
        current_branch_name.is_none_or(|current| current != default_branch.branch_name);
    let mut targets = vec![WorktreeCreateTarget::DefaultBranch(default_branch)];
    if is_different {
        targets.push(WorktreeCreateTarget::CurrentBranch);
    }
    targets
}

/// Whether a worktree operation is creating a new one or switching to an
/// existing one. Controls whether the source workspace's state (dock layout,
/// open files, agent panel draft) is inherited by the destination.
enum WorktreeOperation {
    Create,
    Switch,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemoteBranchFetchMode {
    Fetch,
    UseLocal,
}

impl RemoteBranchFetchMode {
    fn should_fetch(self) -> bool {
        matches!(self, Self::Fetch)
    }
}

#[derive(Debug)]
struct WorktreeFetchError {
    remote_name: String,
    branch_name: String,
    source: anyhow::Error,
}

impl WorktreeFetchError {
    fn remote_branch_name(&self) -> String {
        format!("{}/{}", self.remote_name, self.branch_name)
    }

    fn output(&self) -> String {
        format!("git fetch {} failed:\n{:#}", self.remote_name, self.source)
    }
}

impl fmt::Display for WorktreeFetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "git fetch {} failed while creating worktree from {}: {}",
            self.remote_name,
            self.remote_branch_name(),
            self.source
        )
    }
}

impl Error for WorktreeFetchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

struct WorktreeFetchFailedToast {
    workspace: WeakEntity<Workspace>,
    worktree_name: Option<String>,
    branch_target: NewWorktreeBranchTarget,
    focused_dock: Option<DockPosition>,
    remote_branch_name: String,
    operation: SharedString,
    output: String,
    focus_handle: FocusHandle,
}

impl WorktreeFetchFailedToast {
    fn new(
        workspace: WeakEntity<Workspace>,
        worktree_name: Option<String>,
        branch_target: NewWorktreeBranchTarget,
        focused_dock: Option<DockPosition>,
        fetch_error: &WorktreeFetchError,
        cx: &mut gpui::Context<Self>,
    ) -> Self {
        Self {
            workspace,
            worktree_name,
            branch_target,
            focused_dock,
            remote_branch_name: fetch_error.remote_branch_name(),
            operation: format!("fetch {}", fetch_error.remote_name).into(),
            output: fetch_error.output(),
            focus_handle: cx.focus_handle(),
        }
    }
}

impl Focusable for WorktreeFetchFailedToast {
    fn focus_handle(&self, _cx: &gpui::App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl EventEmitter<DismissEvent> for WorktreeFetchFailedToast {}

impl ToastView for WorktreeFetchFailedToast {
    fn action(&self) -> Option<workspace::ToastAction> {
        None
    }

    fn auto_dismiss(&self) -> bool {
        false
    }
}

impl Render for WorktreeFetchFailedToast {
    fn render(&mut self, _window: &mut Window, cx: &mut gpui::Context<Self>) -> impl IntoElement {
        let workspace_for_retry = self.workspace.clone();
        let worktree_name = self.worktree_name.clone();
        let branch_target = self.branch_target.clone();
        let focused_dock = self.focused_dock;

        let workspace_for_log = self.workspace.clone();
        let operation = self.operation.clone();
        let output = self.output.clone();

        h_flex()
            .id("worktree-fetch-failed-toast")
            .elevation_3(cx)
            .gap_2()
            .py_1p5()
            .pl_2p5()
            .pr_1p5()
            .flex_none()
            .bg(cx.theme().colors().surface_background)
            .shadow_lg()
            .child(
                Icon::new(IconName::XCircle)
                    .size(IconSize::Small)
                    .color(Color::Error),
            )
            .child(Label::new(format!(
                "git fetch failed for {}",
                self.remote_branch_name
            )))
            .child(
                Button::new(
                    "use-local-worktree-base",
                    format!("Use local {}", self.remote_branch_name),
                )
                .color(Color::Muted)
                .on_click(cx.listener(move |_, _event, window, cx| {
                    cx.emit(DismissEvent);
                    if let Some(workspace) = workspace_for_retry.upgrade() {
                        workspace.update(cx, |workspace, cx| {
                            let task = create_worktree_workspace_inner(
                                workspace,
                                &zed_actions::CreateWorktree {
                                    worktree_name: worktree_name.clone(),
                                    branch_target: branch_target.clone(),
                                },
                                window,
                                focused_dock,
                                RemoteBranchFetchMode::UseLocal,
                                // User-initiated retry of a foreground create.
                                OpenMode::Activate,
                                cx,
                            );
                            task.detach_and_log_err(cx);
                        });
                    }
                })),
            )
            .child(
                Button::new("view-worktree-fetch-log", "Show Error Logs")
                    .color(Color::Muted)
                    .on_click(cx.listener(move |_, _event, window, cx| {
                        cx.emit(DismissEvent);
                        let output = output.clone();
                        let operation = operation.clone();
                        workspace_for_log
                            .update(cx, move |workspace, cx| {
                                open_output(operation, workspace, &output, window, cx)
                            })
                            .ok();
                    })),
            )
            .child(
                IconButton::new("dismiss-worktree-fetch-failed-toast", IconName::Close)
                    .shape(ui::IconButtonShape::Square)
                    .icon_size(IconSize::Small)
                    .icon_color(Color::Muted)
                    .on_click(cx.listener(|_, _event, _window, cx| {
                        cx.emit(DismissEvent);
                    })),
            )
    }
}

/// Classifies the project's visible worktrees into git-managed repositories
/// and non-git paths. Each unique repository is returned only once.
pub fn classify_worktrees(
    project: &Project,
    cx: &gpui::App,
) -> (Vec<Entity<Repository>>, Vec<PathBuf>) {
    let repositories = project.repositories(cx).clone();
    let mut git_repos: Vec<Entity<Repository>> = Vec::new();
    let mut non_git_paths: Vec<PathBuf> = Vec::new();
    let mut seen_repo_ids = HashSet::default();

    for worktree in project.visible_worktrees(cx) {
        let wt_path = worktree.read(cx).abs_path();

        let matching_repo = repositories
            .iter()
            .filter_map(|(id, repo)| {
                let work_dir = repo.read(cx).work_directory_abs_path.clone();
                if wt_path.starts_with(work_dir.as_ref()) {
                    Some((*id, repo.clone(), work_dir.as_ref().components().count()))
                } else {
                    None
                }
            })
            .max_by(
                |(left_id, _left_repo, left_depth), (right_id, _right_repo, right_depth)| {
                    left_depth
                        .cmp(right_depth)
                        .then_with(|| left_id.cmp(right_id))
                },
            );

        if let Some((id, repo, _)) = matching_repo {
            if seen_repo_ids.insert(id) {
                git_repos.push(repo);
            }
        } else {
            non_git_paths.push(wt_path.to_path_buf());
        }
    }

    (git_repos, non_git_paths)
}

/// Resolves a branch target into the ref the new worktree should be based on.
/// Returns `None` for `CurrentBranch`, meaning "use the current HEAD".
/// A `Commit` target does not name a single ref observable outside the
/// worktree service, so it can't be resolved here; the repository-aware
/// resolver in [`start_worktree_creations`] picks the per-repository base.
pub fn resolve_worktree_branch_target(branch_target: &NewWorktreeBranchTarget) -> Option<String> {
    match branch_target {
        NewWorktreeBranchTarget::CurrentBranch
        | NewWorktreeBranchTarget::Commit { .. } => None,
        NewWorktreeBranchTarget::ExistingBranch { name } => Some(name.clone()),
        NewWorktreeBranchTarget::RemoteBranch {
            remote_name,
            branch_name,
        } => Some(format!("refs/remotes/{remote_name}/{branch_name}")),
    }
}

fn remote_branch_to_fetch(branch_target: &NewWorktreeBranchTarget) -> Option<(&str, &str)> {
    match branch_target {
        NewWorktreeBranchTarget::RemoteBranch {
            remote_name,
            branch_name,
        } => Some((remote_name, branch_name)),
        NewWorktreeBranchTarget::CurrentBranch
        | NewWorktreeBranchTarget::ExistingBranch { .. }
        | NewWorktreeBranchTarget::Commit { .. } => None,
    }
}

fn create_worktree_askpass_delegate(
    workspace: WeakEntity<Workspace>,
    operation: impl Into<SharedString>,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) -> AskPassDelegate {
    let operation = operation.into();
    let window = window.window_handle();
    AskPassDelegate::new_with_cancellation(
        &mut cx.to_async(),
        move |prompt, tx, cancellation, cx| {
            window
                .update(cx, |_, window, cx| {
                    workspace.update(cx, |workspace, cx| {
                        workspace.toggle_modal(window, cx, |window, cx| {
                            AskPassModal::new(
                                operation.clone(),
                                prompt.into(),
                                tx,
                                cancellation,
                                window,
                                cx,
                            )
                        });
                    })
                })
                .ok();
        },
    )
}

async fn fetch_remote_for_worktree_base(
    git_repos: &[Entity<Repository>],
    remote_name: String,
    askpass_delegates: Vec<AskPassDelegate>,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<()> {
    if askpass_delegates.len() != git_repos.len() {
        return Err(anyhow!(
            "Unable to fetch {remote_name}: missing credential prompt delegate"
        ));
    }

    let fetches = cx.update(|_, cx| {
        git_repos
            .iter()
            .cloned()
            .zip(askpass_delegates)
            .map(|(repo, askpass)| {
                repo.update(cx, |repo, cx| {
                    repo.fetch(
                        FetchOptions::Remote(Remote {
                            name: remote_name.clone().into(),
                        }),
                        askpass,
                        cx,
                    )
                })
            })
            .collect::<Vec<_>>()
    })?;

    for fetch in futures::future::join_all(fetches).await {
        fetch??;
    }

    Ok(())
}

/// Resolves the Git creation target for one repository given the requested
/// branch target.
///
/// For a [`NewWorktreeBranchTarget::Commit`] target, every repository
/// sharing the clicked repository's common identity uses the selected SHA
/// and every other underlying repository uses the current HEAD. All other
/// targets resolve to a single base shared by every repository.
fn detached_target_for_repository(
    branch_target: &NewWorktreeBranchTarget,
    clicked_common_identity: Option<&CommonRepositoryIdentity>,
    repository: &Repository,
) -> anyhow::Result<CreateWorktreeTarget> {
    let repository_common_identity = repository.common_repository_identity();
    let base_sha = resolve_target_base_sha(
        branch_target,
        clicked_common_identity,
        &repository_common_identity,
    )?;
    Ok(CreateWorktreeTarget::Detached { base_sha })
}

/// Pure base-ref decision for one repository against a branch target. Kept
/// separate from [`detached_target_for_repository`] so the commit-versus-HEAD
/// grouping is directly unit-testable without a live [`Repository`]. For a
/// `Commit` target only repositories sharing the clicked common identity are
/// based on the selected SHA; every other repository uses the current HEAD
/// (`None`).
fn resolve_target_base_sha(
    branch_target: &NewWorktreeBranchTarget,
    clicked_common_identity: Option<&CommonRepositoryIdentity>,
    repository_common_identity: &CommonRepositoryIdentity,
) -> anyhow::Result<Option<String>> {
    let base_sha = match branch_target {
        NewWorktreeBranchTarget::CurrentBranch => None,
        NewWorktreeBranchTarget::ExistingBranch { name } => Some(name.clone()),
        NewWorktreeBranchTarget::RemoteBranch {
            remote_name,
            branch_name,
        } => Some(format!("refs/remotes/{remote_name}/{branch_name}")),
        NewWorktreeBranchTarget::Commit { sha, .. } => clicked_common_identity
            .filter(|clicked_identity| *clicked_identity == repository_common_identity)
            .map(|_| sha.clone()),
    };
    Ok(base_sha)
}

/// Kicks off an async git-worktree creation for each repository. Returns:
///
/// - `creation_infos`: a vec of `(repo, new_path, receiver)` tuples.
/// - `path_remapping`: `(old_work_dir, new_worktree_path)` pairs for remapping editor tabs.
///
/// Multiple entries in `git_repos` can be linked worktrees of the *same*
/// underlying repository (e.g. a project that has both the main checkout and
/// one of its linked worktrees open as separate Zed worktrees, or two open
/// sub-directories of one checkout). Those entries resolve to the same target
/// path via [`Repository::path_for_new_linked_worktree`] and receive the same
/// base from [`detached_target_for_repository`], so we create the new worktree
/// only once and remap every contributing work directory onto it. Without this
/// dedup, the second `git worktree add` fails with "already exists".
///
/// `clicked_common_identity` is `Some` only for a `Commit` target and is the
/// common identity of the repository the user clicked in the graph; it
/// determines which repositories are based on the selected SHA.
fn start_worktree_creations(
    git_repos: &[Entity<Repository>],
    worktree_name: Option<String>,
    existing_worktree_names: &[String],
    existing_worktree_paths: &HashSet<PathBuf>,
    branch_target: &NewWorktreeBranchTarget,
    clicked_common_identity: Option<CommonRepositoryIdentity>,
    worktree_directory_setting: &str,
    rng: &mut impl rand::Rng,
    cx: &mut gpui::App,
) -> anyhow::Result<(
    Vec<(
        Entity<Repository>,
        PathBuf,
        futures::channel::oneshot::Receiver<anyhow::Result<()>>,
    )>,
    Vec<(PathBuf, PathBuf)>,
)> {
    let mut creation_infos = Vec::new();
    let mut path_remapping = Vec::new();
    let mut scheduled_paths: HashSet<PathBuf> = HashSet::default();

    let worktree_name = worktree_name.unwrap_or_else(|| {
        let existing_refs: Vec<&str> = existing_worktree_names.iter().map(|s| s.as_str()).collect();
        worktree_names::generate_worktree_name(&existing_refs, rng)
            .unwrap_or_else(|| "worktree".to_string())
    });

    for repo in git_repos {
        let (work_dir, new_path, receiver) = repo.update(cx, |repo, _cx| {
            let new_path =
                repo.path_for_new_linked_worktree(&worktree_name, worktree_directory_setting)?;
            if existing_worktree_paths.contains(&new_path) {
                anyhow::bail!("A worktree already exists at {}", new_path.display());
            }
            let work_dir = repo.work_directory_abs_path.clone();
            let target = detached_target_for_repository(
                branch_target,
                clicked_common_identity.as_ref(),
                repo,
            )?;
            // Only the first repo that resolves to a given target path
            // actually creates the worktree; subsequent linked worktrees of
            // the same repository just contribute a path remapping.
            let receiver = if scheduled_paths.contains(&new_path) {
                None
            } else {
                Some(repo.create_worktree(target, new_path.clone()))
            };
            anyhow::Ok((work_dir, new_path, receiver))
        })?;
        path_remapping.push((work_dir.to_path_buf(), new_path.clone()));
        if let Some(receiver) = receiver {
            scheduled_paths.insert(new_path.clone());
            creation_infos.push((repo.clone(), new_path, receiver));
        }
    }

    Ok((creation_infos, path_remapping))
}

/// Waits for every in-flight worktree creation to complete. If any
/// creation fails, all successfully-created worktrees are rolled back
/// (removed) so the project isn't left in a half-migrated state.
pub async fn await_and_rollback_on_failure(
    creation_infos: Vec<(
        Entity<Repository>,
        PathBuf,
        futures::channel::oneshot::Receiver<anyhow::Result<()>>,
    )>,
    fs: Arc<dyn Fs>,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut created_paths: Vec<PathBuf> = Vec::new();
    let mut repos_and_paths: Vec<(Entity<Repository>, PathBuf)> = Vec::new();
    let mut first_error: Option<anyhow::Error> = None;

    for (repo, new_path, receiver) in creation_infos {
        repos_and_paths.push((repo.clone(), new_path.clone()));
        match receiver.await {
            Ok(Ok(())) => {
                created_paths.push(new_path);
            }
            Ok(Err(err)) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
            Err(_canceled) => {
                if first_error.is_none() {
                    first_error = Some(anyhow!("Worktree creation was canceled"));
                }
            }
        }
    }

    let Some(err) = first_error else {
        return Ok(created_paths);
    };

    // Rollback all attempted worktrees
    let mut rollback_futures = Vec::new();
    for (rollback_repo, rollback_path) in &repos_and_paths {
        let receiver = cx
            .update(|_, cx| {
                rollback_repo.update(cx, |repo, _cx| {
                    repo.remove_worktree(rollback_path.clone(), true)
                })
            })
            .ok();

        rollback_futures.push((rollback_path.clone(), receiver));
    }

    let mut rollback_failures: Vec<String> = Vec::new();
    for (path, receiver_opt) in rollback_futures {
        let mut git_remove_failed = false;

        if let Some(receiver) = receiver_opt {
            match receiver.await {
                Ok(Ok(())) => {}
                Ok(Err(rollback_err)) => {
                    log::error!(
                        "git worktree remove failed for {}: {rollback_err}",
                        path.display()
                    );
                    git_remove_failed = true;
                }
                Err(canceled) => {
                    log::error!(
                        "git worktree remove failed for {}: {canceled}",
                        path.display()
                    );
                    git_remove_failed = true;
                }
            }
        } else {
            log::error!(
                "failed to dispatch git worktree remove for {}",
                path.display()
            );
            git_remove_failed = true;
        }

        if git_remove_failed {
            if let Err(fs_err) = fs
                .remove_dir(
                    &path,
                    fs::RemoveOptions {
                        recursive: true,
                        ignore_if_not_exists: true,
                    },
                )
                .await
            {
                let msg = format!("{}: failed to remove directory: {fs_err}", path.display());
                log::error!("{}", msg);
                rollback_failures.push(msg);
            }
        }
    }
    let mut error_message = format!("Failed to create worktree: {err}");
    if !rollback_failures.is_empty() {
        error_message.push_str("\n\nFailed to clean up: ");
        error_message.push_str(&rollback_failures.join(", "));
    }
    Err(anyhow!(error_message))
}

/// Propagates worktree trust from the source workspace to the new workspace.
/// If the source project's worktrees are all trusted, the new worktree paths
/// will also be trusted automatically.
fn maybe_propagate_worktree_trust(
    source_workspace: &WeakEntity<Workspace>,
    new_workspace: &Entity<Workspace>,
    paths: &[PathBuf],
    cx: &mut AsyncWindowContext,
) {
    cx.update(|_, cx| {
        if ProjectSettings::get_global(cx).session.trust_all_worktrees {
            return;
        }
        let source_is_trusted = source_workspace
            .upgrade()
            .map(|workspace| {
                let source_worktree_store = workspace.read(cx).project().read(cx).worktree_store();
                !TrustedWorktrees::has_restricted_worktrees(&source_worktree_store, cx)
            })
            .unwrap_or(false);

        if !source_is_trusted {
            return;
        }

        let worktree_store = new_workspace.read(cx).project().read(cx).worktree_store();
        let paths_to_trust: HashSet<_> = paths
            .iter()
            .filter_map(|path| {
                let (worktree, _) = worktree_store.read(cx).find_worktree(path, cx)?;
                Some(PathTrust::Worktree(worktree.read(cx).id()))
            })
            .collect();

        if !paths_to_trust.is_empty() {
            if let Some(trusted_store) = TrustedWorktrees::try_get_global(cx) {
                trusted_store.update(cx, |store, cx| {
                    store.trust(&worktree_store, paths_to_trust, cx);
                });
            }
        }
    })
    .ok();

    // After trust propagation, refresh the security modal on the new workspace
    // so it dismisses itself if there are no more restricted worktrees.
    cx.update(|window, cx| {
        new_workspace.update(cx, |workspace, cx| {
            workspace.show_worktree_trust_security_modal(false, window, cx);
        });
    })
    .ok();
}

/// Handles the `CreateWorktree` action generically, without any agent panel involvement.
/// Creates a new git worktree, opens the workspace, restores layout and files.
/// Errors are surfaced to the user via toasts; the new workspace handle is
/// discarded. Use [`create_worktree_workspace`] when you need the resulting
/// workspace (e.g., the `create_thread` agent tool spawns a thread in it).
pub fn handle_create_worktree(
    workspace: &mut Workspace,
    action: &zed_actions::CreateWorktree,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) {
    let task = create_worktree_workspace_inner(
        workspace,
        action,
        window,
        fallback_focused_dock,
        RemoteBranchFetchMode::Fetch,
        open_mode,
        cx,
    );
    task.detach_and_log_err(cx);
}

/// Outcome of [`create_worktree_workspace`].
pub struct CreatedWorktreeWorkspace {
    /// The newly opened workspace.
    pub workspace: Entity<Workspace>,
    /// True when the project contained more than one Zed worktree backed by
    /// the same underlying git repository, so they were consolidated into a
    /// single new worktree (they resolve to the same target path). Callers
    /// that care — like the `create_thread` agent tool — can use this to warn
    /// that the result may not reflect every source worktree's state.
    pub consolidated_worktrees: bool,
}

/// Same as [`handle_create_worktree`], but returns a `Task` that resolves to
/// the new workspace once worktree creation and post-open setup are
/// complete. The caller receives errors as `Result`s and is expected to
/// handle them. Note that a small set of early failures (no git repositories,
/// disconnected remote, mid-creation `git fetch` failure) still surface a
/// toast on the source workspace so the user understands why the action
/// didn't take effect; the same error is also returned to the caller.
///
/// Used by the `create_thread` agent tool to spawn a sibling thread inside
/// the newly-opened workspace.
///
/// The new workspace is opened in the **background** (added as a retained
/// tab without switching to it or moving focus), and it's a clean checkout
/// rather than inheriting the source workspace's open files and dock layout.
/// This mirrors how the agent's non-worktree threads are created in the
/// background rather than yanking the user away from what they're doing.
pub fn create_worktree_workspace(
    workspace: &mut Workspace,
    action: &zed_actions::CreateWorktree,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) -> Task<anyhow::Result<CreatedWorktreeWorkspace>> {
    create_worktree_workspace_inner(
        workspace,
        action,
        window,
        fallback_focused_dock,
        RemoteBranchFetchMode::Fetch,
        open_mode,
        cx,
    )
}

fn create_worktree_workspace_inner(
    workspace: &mut Workspace,
    action: &zed_actions::CreateWorktree,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    remote_branch_fetch_mode: RemoteBranchFetchMode,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) -> Task<anyhow::Result<CreatedWorktreeWorkspace>> {
    let project = workspace.project().clone();

    if project.read(cx).repositories(cx).is_empty() {
        return Task::ready(Err(anyhow!(
            "create_worktree: no git repository in the project"
        )));
    }
    if project.read(cx).is_via_collab() {
        return Task::ready(Err(anyhow!(
            "create_worktree: not supported in collab projects"
        )));
    }

    // Guard against concurrent creation. We treat a concurrent creation as
    // a hard error here so the caller can surface it; the user-facing
    // wrapper [`handle_create_worktree`] swallows the error via
    // `detach_and_log_err`, matching the pre-existing silent return.
    if workspace.active_worktree_creation().label.is_some() {
        return Task::ready(Err(anyhow!("A worktree creation is already in progress")));
    }

    let previous_state =
        workspace.capture_state_for_worktree_switch(window, fallback_focused_dock, cx);
    let workspace_handle = workspace.weak_handle();
    let window_handle = window.window_handle().downcast::<MultiWorkspace>();
    let remote_connection_options = project.read(cx).remote_connection_options(cx);

    let (git_repos, non_git_paths) = classify_worktrees(project.read(cx), cx);

    if git_repos.is_empty() {
        let toast_workspace = cx.entity();
        show_error_toast(
            toast_workspace,
            "worktree create",
            anyhow!("No git repositories found in the project"),
            cx,
        );
        return Task::ready(Err(anyhow!("No git repositories found in the project")));
    }

    if remote_connection_options.is_some() {
        let is_disconnected = project
            .read(cx)
            .remote_client()
            .is_some_and(|client| client.read(cx).is_disconnected());
        if is_disconnected {
            let toast_workspace = cx.entity();
            show_error_toast(
                toast_workspace,
                "worktree create",
                anyhow!("Cannot create worktree: remote connection is not active"),
                cx,
            );
            return Task::ready(Err(anyhow!(
                "Cannot create worktree: remote connection is not active"
            )));
        }
    }

    let worktree_name = action.worktree_name.clone();
    // Re-validate a caller-supplied name at the service boundary before any
    // path calculation or Git job: the UI is not a trust boundary.
    if let Some(name) = &worktree_name
        && let Err(error) = worktree_names::normalize_worktree_name(name)
    {
        return Task::ready(Err(error.context("Invalid worktree name")));
    }
    let branch_target = action.branch_target.clone();
    let fetch_askpass_delegates = if remote_branch_fetch_mode.should_fetch() {
        remote_branch_to_fetch(&branch_target)
            .map(|(remote_name, _branch_name)| {
                git_repos
                    .iter()
                    .map(|_| {
                        create_worktree_askpass_delegate(
                            workspace_handle.clone(),
                            format!("git fetch {remote_name}"),
                            window,
                            cx,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let display_name: SharedString = worktree_name
        .as_deref()
        .unwrap_or("worktree")
        .to_string()
        .into();

    workspace.set_active_worktree_creation(Some(display_name), false, cx);

    cx.spawn_in(window, async move |_workspace_entity, mut cx| {
        let result = do_create_worktree(
            git_repos,
            non_git_paths,
            worktree_name.clone(),
            branch_target.clone(),
            fetch_askpass_delegates,
            remote_branch_fetch_mode,
            previous_state,
            workspace_handle.clone(),
            window_handle,
            remote_connection_options,
            open_mode,
            &mut cx,
        )
        .await;

        if let Err(err) = &result {
            log::error!("Failed to create worktree: {err}");
            workspace_handle
                .update(cx, |workspace, cx| {
                    workspace.set_active_worktree_creation(None, false, cx);
                    if let Some(fetch_error) = err.downcast_ref::<WorktreeFetchError>() {
                        let toast = cx.new(|cx| {
                            WorktreeFetchFailedToast::new(
                                workspace.weak_handle(),
                                worktree_name,
                                branch_target,
                                fallback_focused_dock,
                                fetch_error,
                                cx,
                            )
                        });
                        workspace.toggle_status_toast(toast, cx);
                    } else {
                        show_error_toast(cx.entity(), "worktree create", anyhow!("{err:#}"), cx);
                    }
                })
                .ok();
        }

        result
    })
}

pub fn handle_switch_worktree(
    workspace: &mut Workspace,
    action: &zed_actions::SwitchWorktree,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) {
    let project = workspace.project().clone();

    if project.read(cx).repositories(cx).is_empty() {
        log::error!("switch_to_worktree: no git repository in the project");
        return;
    }
    if project.read(cx).is_via_collab() {
        log::error!("switch_to_worktree: not supported in collab projects");
        return;
    }

    // Guard against concurrent creation
    if workspace.active_worktree_creation().label.is_some() {
        return;
    }

    let previous_state =
        workspace.capture_state_for_worktree_switch(window, fallback_focused_dock, cx);
    let workspace_handle = workspace.weak_handle();
    let window_handle = window.window_handle().downcast::<MultiWorkspace>();
    let remote_connection_options = project.read(cx).remote_connection_options(cx);

    let (git_repos, non_git_paths) = classify_worktrees(project.read(cx), cx);

    let git_repo_work_dirs: Vec<PathBuf> = git_repos
        .iter()
        .map(|repo| repo.read(cx).work_directory_abs_path.to_path_buf())
        .collect();

    let display_name: SharedString = action.display_name.clone().into();

    workspace.set_active_worktree_creation(Some(display_name), true, cx);

    let worktree_path = action.path.clone();

    cx.spawn_in(window, async move |_workspace_entity, mut cx| {
        let result = do_switch_worktree(
            worktree_path,
            git_repo_work_dirs,
            non_git_paths,
            previous_state,
            workspace_handle.clone(),
            window_handle,
            remote_connection_options,
            open_mode,
            &mut cx,
        )
        .await;

        if let Err(err) = &result {
            log::error!("Failed to switch worktree: {err}");
            workspace_handle
                .update(cx, |workspace, cx| {
                    workspace.set_active_worktree_creation(None, false, cx);
                    show_error_toast(cx.entity(), "worktree switch", anyhow!("{err:#}"), cx);
                })
                .ok();
        }

        result
    })
    .detach_and_log_err(cx);
}

async fn do_create_worktree(
    git_repos: Vec<Entity<Repository>>,
    non_git_paths: Vec<PathBuf>,
    worktree_name: Option<String>,
    branch_target: NewWorktreeBranchTarget,
    fetch_askpass_delegates: Vec<AskPassDelegate>,
    remote_branch_fetch_mode: RemoteBranchFetchMode,
    previous_state: PreviousWorkspaceState,
    workspace: WeakEntity<Workspace>,
    window_handle: Option<gpui::WindowHandle<MultiWorkspace>>,
    remote_connection_options: Option<RemoteConnectionOptions>,
    open_mode: OpenMode,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<CreatedWorktreeWorkspace> {
    // List existing worktrees from all repos to detect name collisions
    let worktree_receivers: Vec<_> = cx.update(|_, cx| {
        git_repos
            .iter()
            .map(|repo| repo.update(cx, |repo, _cx| repo.worktrees()))
            .collect()
    })?;
    let worktree_directory_setting = cx.update(|_, cx| {
        ProjectSettings::get_global(cx)
            .git
            .worktree_directory
            .clone()
    })?;

    let mut existing_worktree_names = Vec::new();
    let mut existing_worktree_paths = HashSet::default();
    for result in futures::future::join_all(worktree_receivers).await {
        match result {
            Ok(Ok(worktrees)) => {
                for worktree in worktrees {
                    if let Some(name) = worktree
                        .path
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                    {
                        existing_worktree_names.push(name.to_string());
                    }
                    existing_worktree_paths.insert(worktree.path.clone());
                }
            }
            Ok(Err(err)) => {
                Err::<(), _>(err).log_err();
            }
            Err(_) => {}
        }
    }

    if remote_branch_fetch_mode.should_fetch()
        && let Some((remote_name, branch_name)) = remote_branch_to_fetch(&branch_target)
    {
        let remote_name = remote_name.to_string();
        let branch_name = branch_name.to_string();
        if let Err(error) = fetch_remote_for_worktree_base(
            &git_repos,
            remote_name.clone(),
            fetch_askpass_delegates,
            cx,
        )
        .await
        {
            return Err(WorktreeFetchError {
                remote_name,
                branch_name,
                source: error,
            }
            .into());
        }
    }

    let mut rng = rand::rng();

    let clicked_common_identity = match &branch_target {
        NewWorktreeBranchTarget::Commit { repository_id, .. } => {
            let clicked_repository = cx.update(|_, cx| {
                git_repos
                    .iter()
                    .find(|repo| repo.read(cx).id.to_proto() == *repository_id)
                    .map(|repo| repo.read(cx).common_repository_identity())
            })?;
            Some(clicked_repository.ok_or_else(|| {
                anyhow!(
                    "Unable to create worktree: no repository with id {repository_id} \
                     was found in the current project"
                )
            })?)
        }
        NewWorktreeBranchTarget::CurrentBranch
        | NewWorktreeBranchTarget::ExistingBranch { .. }
        | NewWorktreeBranchTarget::RemoteBranch { .. } => None,
    };

    let (creation_infos, path_remapping) = cx.update(|_, cx| {
        start_worktree_creations(
            &git_repos,
            worktree_name,
            &existing_worktree_names,
            &existing_worktree_paths,
            &branch_target,
            clicked_common_identity,
            &worktree_directory_setting,
            &mut rng,
            cx,
        )
    })??;

    let fs = cx.update(|_, cx| <dyn Fs>::global(cx))?;

    let creation_pairs: Vec<(Entity<Repository>, PathBuf)> = creation_infos
        .iter()
        .map(|(repo, path, _)| (repo.clone(), path.clone()))
        .collect();

    let created_paths = await_and_rollback_on_failure(creation_infos, fs, cx).await?;

    // Record each created worktree so thread archival can later verify that
    // Zed created it before deleting it from disk. Failures are non-fatal:
    // the worktree just won't be eligible for automatic archival.
    for (repo, path) in creation_pairs {
        crate::created_worktrees::record_created_worktree_for_repo(
            &repo,
            &path,
            remote_connection_options.as_ref(),
            cx,
        )
        .await;
    }

    // `path_remapping` has one entry per source git repo, while `created_paths`
    // has one per *unique* target worktree. When the former is larger, two or
    // more source repos were linked worktrees of the same underlying
    // repository and `start_worktree_creations` consolidated them.
    let consolidated_worktrees = path_remapping.len() > created_paths.len();

    let mut all_paths = created_paths;
    let has_non_git = !non_git_paths.is_empty();
    all_paths.extend(non_git_paths.iter().cloned());

    let workspace = open_worktree_workspace(
        all_paths,
        path_remapping,
        non_git_paths,
        has_non_git,
        previous_state,
        workspace,
        window_handle,
        remote_connection_options,
        WorktreeOperation::Create,
        open_mode,
        cx,
    )
    .await?;

    Ok(CreatedWorktreeWorkspace {
        workspace,
        consolidated_worktrees,
    })
}

async fn do_switch_worktree(
    worktree_path: PathBuf,
    git_repo_work_dirs: Vec<PathBuf>,
    non_git_paths: Vec<PathBuf>,
    previous_state: PreviousWorkspaceState,
    workspace: WeakEntity<Workspace>,
    window_handle: Option<gpui::WindowHandle<MultiWorkspace>>,
    remote_connection_options: Option<RemoteConnectionOptions>,
    open_mode: OpenMode,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<Entity<Workspace>> {
    let path_remapping: Vec<(PathBuf, PathBuf)> = git_repo_work_dirs
        .iter()
        .map(|work_dir| (work_dir.clone(), worktree_path.clone()))
        .collect();

    let mut all_paths = vec![worktree_path];
    let has_non_git = !non_git_paths.is_empty();
    all_paths.extend(non_git_paths.iter().cloned());

    open_worktree_workspace(
        all_paths,
        path_remapping,
        non_git_paths,
        has_non_git,
        previous_state,
        workspace,
        window_handle,
        remote_connection_options,
        WorktreeOperation::Switch,
        open_mode,
        cx,
    )
    .await
}

/// Core workspace opening logic shared by both create and switch flows.
/// Returns the newly opened workspace entity so callers can do post-open
/// work (e.g., the `create_thread` agent tool spawns a thread inside it).
async fn open_worktree_workspace(
    all_paths: Vec<PathBuf>,
    path_remapping: Vec<(PathBuf, PathBuf)>,
    non_git_paths: Vec<PathBuf>,
    has_non_git: bool,
    previous_state: PreviousWorkspaceState,
    workspace: WeakEntity<Workspace>,
    window_handle: Option<gpui::WindowHandle<MultiWorkspace>>,
    remote_connection_options: Option<RemoteConnectionOptions>,
    operation: WorktreeOperation,
    open_mode: OpenMode,
    cx: &mut AsyncWindowContext,
) -> anyhow::Result<Entity<Workspace>> {
    let is_creating_new_worktree = matches!(operation, WorktreeOperation::Create);

    let window_handle = window_handle
        .ok_or_else(|| anyhow!("No window handle available for workspace creation"))?;

    let focused_dock = previous_state.focused_dock;

    // When `open_mode` is `Add` (e.g. the agent's `create_thread` tool) the
    // new workspace is opened in the background, so it should be a clean
    // checkout rather than inheriting the source workspace's open files and
    // dock layout. The state transfer only applies when we're foregrounding a
    // freshly-created worktree for the user.
    let transfer_state =
        is_creating_new_worktree && matches!(open_mode, OpenMode::Activate);

    let source_for_transfer = if transfer_state {
        Some(workspace.clone())
    } else {
        None
    };

    let (workspace_task, modal_workspace) =
        window_handle.update(cx, |multi_workspace, window, cx| {
            let path_list = util::path_list::PathList::new(&all_paths);
            let active_workspace = multi_workspace.workspace().clone();
            let modal_workspace = active_workspace.clone();

            let init: Option<
                Box<
                    dyn FnOnce(&mut Workspace, &mut gpui::Window, &mut gpui::Context<Workspace>)
                        + Send,
                >,
            > = if transfer_state {
                let dock_structure = previous_state.dock_structure;
                Some(Box::new(
                    move |workspace: &mut Workspace,
                          window: &mut gpui::Window,
                          cx: &mut gpui::Context<Workspace>| {
                        workspace.set_dock_structure(dock_structure, window, cx);
                    },
                ))
            } else {
                None
            };

            let task = multi_workspace.find_or_create_workspace(
                path_list,
                remote_connection_options,
                None,
                move |connection_options, window, cx| {
                    remote_connection::connect_with_modal(
                        &active_workspace,
                        connection_options,
                        window,
                        cx,
                    )
                },
                init,
                OpenMode::Add,
                source_for_transfer.clone(),
                window,
                cx,
            );
            (task, modal_workspace)
        })?;

    let result = workspace_task.await;
    remote_connection::dismiss_connection_modal(&modal_workspace, cx);
    let new_workspace = result?;

    let panels_task = new_workspace.update(cx, |workspace, _cx| workspace.take_panels_task());

    if let Some(task) = panels_task {
        task.await.log_err();
    }

    new_workspace
        .update(cx, |workspace, cx| {
            workspace.project().read(cx).wait_for_initial_scan(cx)
        })
        .await;

    new_workspace
        .update(cx, |workspace, cx| {
            let repos = workspace
                .project()
                .read(cx)
                .repositories(cx)
                .values()
                .cloned()
                .collect::<Vec<_>>();

            let tasks = repos
                .into_iter()
                .map(|repo| repo.update(cx, |repo, _| repo.barrier()));
            futures::future::join_all(tasks)
        })
        .await;

    maybe_propagate_worktree_trust(&workspace, &new_workspace, &all_paths, cx);

    if transfer_state {
        window_handle.update(cx, |_multi_workspace, window, cx| {
            new_workspace.update(cx, |workspace, cx| {
                if has_non_git {
                    struct WorktreeCreationToast;
                    let toast_id =
                        workspace::notifications::NotificationId::unique::<WorktreeCreationToast>();
                    workspace.show_toast(
                        workspace::Toast::new(
                            toast_id,
                            "Some project folders are not git repositories. \
                             They were included as-is without creating a worktree.",
                        ),
                        cx,
                    );
                }

                // Remap every previously-open file path into the new worktree.
                let remap_path = |original_path: PathBuf| -> Option<PathBuf> {
                    let best_match = path_remapping
                        .iter()
                        .filter_map(|(old_root, new_root)| {
                            original_path.strip_prefix(old_root).ok().map(|relative| {
                                (old_root.components().count(), new_root.join(relative))
                            })
                        })
                        .max_by_key(|(depth, _)| *depth);

                    if let Some((_, remapped_path)) = best_match {
                        return Some(remapped_path);
                    }

                    for non_git in &non_git_paths {
                        if original_path.starts_with(non_git) {
                            return Some(original_path);
                        }
                    }
                    None
                };

                let remapped_active_path =
                    previous_state.active_file_path.and_then(|p| remap_path(p));

                let mut paths_to_open: Vec<PathBuf> = Vec::new();
                let mut seen = HashSet::default();
                for path in previous_state.open_file_paths {
                    if let Some(remapped) = remap_path(path) {
                        if remapped_active_path.as_ref() != Some(&remapped)
                            && seen.insert(remapped.clone())
                        {
                            paths_to_open.push(remapped);
                        }
                    }
                }

                if let Some(active) = &remapped_active_path {
                    if seen.insert(active.clone()) {
                        paths_to_open.push(active.clone());
                    }
                }

                if !paths_to_open.is_empty() {
                    let should_focus_center = focused_dock.is_none();
                    let open_task = workspace.open_paths(
                        paths_to_open,
                        workspace::OpenOptions {
                            focus: Some(false),
                            ..Default::default()
                        },
                        None,
                        window,
                        cx,
                    );
                    cx.spawn_in(window, async move |workspace, cx| {
                        for item in open_task.await.into_iter().flatten() {
                            item.log_err();
                        }
                        if should_focus_center {
                            workspace.update_in(cx, |workspace, window, cx| {
                                workspace.focus_center_pane(window, cx);
                            })?;
                        }
                        anyhow::Ok(())
                    })
                    .detach_and_log_err(cx);
                }
            });
        })?;
    }

    // Clear the creation status on the SOURCE workspace so its title bar
    // stops showing the loading indicator immediately.
    workspace
        .update(cx, |ws, cx| {
            ws.set_active_worktree_creation(None, false, cx);
        })
        .ok();

    window_handle.update(cx, |multi_workspace, window, cx| {
        if open_mode == OpenMode::Activate {
            multi_workspace.activate(new_workspace.clone(), source_for_transfer, window, cx);
        } else {
            // Background open: register the new workspace as a retained tab
            // but leave the user where they are.
            multi_workspace.add(new_workspace.clone(), window, cx);
        }

        if is_creating_new_worktree {
            new_workspace.update(cx, |workspace, cx| {
                // Run create-worktree setup hooks regardless of foreground vs
                // background — the worktree was created either way.
                workspace.run_create_worktree_tasks(window, cx);

                if open_mode == OpenMode::Activate && let Some(dock_position) = focused_dock {
                    let dock = workspace.dock_at_position(dock_position);
                    if let Some(panel) = dock.read(cx).active_panel() {
                        panel.activation_focus_handle(cx).focus(window, cx);
                    }
                }
            });
        }
    })?;

    Ok(new_workspace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::Fs;
    use gpui::{App, Task, TestAppContext};
    use language::language_settings::AllLanguageSettings;
    use project::project_settings::ProjectSettings;
    use project::task_store::{TaskSettingsLocation, TaskStore};
    use project::{FakeFs, WorktreeSettings};
    use serde_json::json;
    use settings::{SettingsLocation, SettingsStore};
    use std::path::{Path, PathBuf};
    use std::process::ExitStatus;
    use std::sync::Mutex;
    use task::SpawnInTerminal;
    use theme::LoadThemes;
    use util::path;
    use util::rel_path::rel_path;
    use workspace::{TerminalProvider, WorkspaceSettings};

    struct CountingTerminalProvider {
        spawned_task_labels: Arc<Mutex<Vec<String>>>,
    }

    impl TerminalProvider for CountingTerminalProvider {
        fn spawn(
            &self,
            task: SpawnInTerminal,
            _window: &mut ui::Window,
            _cx: &mut App,
        ) -> Task<Option<anyhow::Result<ExitStatus>>> {
            self.spawned_task_labels
                .lock()
                .expect("terminal spawn mutex should not be poisoned")
                .push(task.label);
            Task::ready(Some(Ok(ExitStatus::default())))
        }
    }

    fn init_test(cx: &mut TestAppContext) {
        zlog::init_test();
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(LoadThemes::JustBase, cx);
            AllLanguageSettings::register(cx);
            editor::init(cx);
            ProjectSettings::register(cx);
            WorktreeSettings::register(cx);
            WorkspaceSettings::register(cx);
            TaskStore::init(None);
        });
    }

    fn install_counting_provider_and_worktree_hook(
        workspace: &Entity<Workspace>,
        spawned_task_labels: &Arc<Mutex<Vec<String>>>,
        main_project_root: &Path,
        hook_tasks_json: &str,
        cx: &mut App,
    ) {
        workspace.update(cx, |workspace, cx| {
            workspace.set_terminal_provider(CountingTerminalProvider {
                spawned_task_labels: spawned_task_labels.clone(),
            });

            let project = workspace.project().clone();
            let Some(worktree) = project.read(cx).worktrees(cx).next() else {
                return;
            };
            let worktree = worktree.read(cx);
            let worktree_id = worktree.id();
            let worktree_root = worktree.abs_path().to_path_buf();
            if worktree_root == main_project_root {
                return;
            }

            let Some(task_inventory) = project
                .read(cx)
                .task_store()
                .read(cx)
                .task_inventory()
                .cloned()
            else {
                return;
            };
            task_inventory.update(cx, |inventory, _| {
                inventory
                    .update_file_based_tasks(
                        TaskSettingsLocation::Worktree(SettingsLocation {
                            worktree_id,
                            path: rel_path(".zed"),
                        }),
                        Some(hook_tasks_json),
                    )
                    .expect("should inject create_worktree hook tasks for linked worktree");
            });
        });
    }

    #[gpui::test]
    async fn test_create_worktree_hook_does_not_run_when_switching_back_to_main_worktree(
        cx: &mut TestAppContext,
    ) {
        init_test(cx);

        let hook_tasks_json = r#"[{"label":"setup worktree","command":"echo","hide":"never","hooks":["create_worktree"]}]"#;
        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project": {
                    ".git": {},
                    ".zed": {
                        "tasks.json": hook_tasks_json,
                    },
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                },
            }),
        )
        .await;

        let main_project_root = PathBuf::from(path!("/root/project"));
        let project = Project::test(fs.clone(), [main_project_root.as_path()], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));

        let spawned_task_labels = Arc::new(Mutex::new(Vec::new()));
        multi_workspace.update(cx, |multi_workspace, cx| {
            multi_workspace.retain_active_workspace(cx);
            let active_workspace = multi_workspace.workspace().clone();
            install_counting_provider_and_worktree_hook(
                &active_workspace,
                &spawned_task_labels,
                &main_project_root,
                hook_tasks_json,
                cx,
            );
        });

        let main_workspace =
            multi_workspace.read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone());
        main_workspace.update_in(cx, |workspace, window, cx| {
            handle_create_worktree(
                workspace,
                &zed_actions::CreateWorktree {
                    worktree_name: Some("feature".to_string()),
                    branch_target: NewWorktreeBranchTarget::CurrentBranch,
                },
                window,
                None,
                OpenMode::Activate,
                cx,
            );
        });
        cx.run_until_parked();

        let active_workspace =
            multi_workspace.read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone());
        cx.update(|_, cx| {
            install_counting_provider_and_worktree_hook(
                &active_workspace,
                &spawned_task_labels,
                &main_project_root,
                hook_tasks_json,
                cx,
            );
        });
        active_workspace.update_in(cx, |workspace, window, cx| {
            workspace.run_create_worktree_tasks(window, cx);
        });
        cx.run_until_parked();

        assert_eq!(
            spawned_task_labels
                .lock()
                .expect("terminal spawn mutex should not be poisoned")
                .as_slice(),
            ["setup worktree"],
            "create_worktree hook should run once for the created linked worktree"
        );

        active_workspace.update_in(cx, |workspace, window, cx| {
            handle_switch_worktree(
                workspace,
                &zed_actions::SwitchWorktree {
                    path: main_project_root.clone(),
                    display_name: "project".to_string(),
                },
                window,
                None,
                OpenMode::Activate,
                cx,
            );
        });
        cx.run_until_parked();

        assert_eq!(
            spawned_task_labels
                .lock()
                .expect("terminal spawn mutex should not be poisoned")
                .as_slice(),
            ["setup worktree"],
            "switching back to the main worktree should not rerun create_worktree hooks"
        );
    }

    #[gpui::test]
    async fn test_linked_worktree_inherits_trust_from_main_worktree(cx: &mut TestAppContext) {
        init_test(cx);
        cx.update(|cx| {
            project::trusted_worktrees::init(collections::HashMap::default(), cx);
        });

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project": {
                    ".git": {},
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                },
            }),
        )
        .await;

        let main_project_root = PathBuf::from(path!("/root/project"));
        let project =
            Project::test_with_worktree_trust(fs.clone(), [main_project_root.as_path()], cx).await;
        project
            .update(cx, |project, cx| project.git_scans_complete(cx))
            .await;

        // The main worktree starts restricted; trust it explicitly
        let worktree_store = project.read_with(cx, |project, _| project.worktree_store());
        let main_worktree_id = worktree_store.read_with(cx, |store, cx| {
            store
                .worktrees()
                .next()
                .map(|wt| wt.read(cx).id())
                .expect("should have a worktree")
        });
        let trusted_store = cx
            .read(|cx| project::trusted_worktrees::TrustedWorktrees::try_get_global(cx))
            .expect("trust store should exist");
        trusted_store.update(cx, |store, cx| {
            store.trust(
                &worktree_store,
                collections::HashSet::from_iter([project::trusted_worktrees::PathTrust::Worktree(
                    main_worktree_id,
                )]),
                cx,
            );
        });

        // Verify main worktree is now trusted
        let has_restricted = cx.read(|cx| {
            project::trusted_worktrees::TrustedWorktrees::has_restricted_worktrees(
                &worktree_store,
                cx,
            )
        });
        assert!(
            !has_restricted,
            "main worktree should be trusted after explicit trust"
        );

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |multi_workspace, cx| {
            multi_workspace.retain_active_workspace(cx);
        });

        // Create a linked worktree from the trusted main worktree
        let main_workspace = multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
        main_workspace.update_in(cx, |workspace, window, cx| {
            handle_create_worktree(
                workspace,
                &zed_actions::CreateWorktree {
                    worktree_name: Some("feature".to_string()),
                    branch_target: NewWorktreeBranchTarget::CurrentBranch,
                },
                window,
                None,
                OpenMode::Activate,
                cx,
            );
        });
        cx.run_until_parked();

        // The new workspace (linked worktree) should inherit trust
        let new_workspace = multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
        let new_worktree_store =
            new_workspace.read_with(cx, |ws, cx| ws.project().read(cx).worktree_store());
        let new_has_restricted = cx.read(|cx| {
            project::trusted_worktrees::TrustedWorktrees::has_restricted_worktrees(
                &new_worktree_store,
                cx,
            )
        });
        assert!(
            !new_has_restricted,
            "linked worktree should inherit trust from the main worktree"
        );

        // The security modal should not be showing
        let has_modal = new_workspace.read_with(cx, |ws, cx| {
            ws.active_modal::<workspace::security_modal::SecurityModal>(cx)
                .is_some()
        });
        assert!(
            !has_modal,
            "security modal should not show for a linked worktree created from a trusted main worktree"
        );
    }

    #[test]
    fn test_remote_branch_name_parse() {
        assert_eq!(
            RemoteBranchName::parse("refs/remotes/origin/main"),
            Some(RemoteBranchName {
                remote_name: "origin".to_string(),
                branch_name: "main".to_string(),
            })
        );
        assert_eq!(
            RemoteBranchName::parse("upstream/feature/foo"),
            Some(RemoteBranchName {
                remote_name: "upstream".to_string(),
                branch_name: "feature/foo".to_string(),
            })
        );
        assert_eq!(RemoteBranchName::parse("main"), None);
        assert_eq!(RemoteBranchName::parse("origin/"), None);
    }

    #[test]
    fn test_worktree_create_targets() {
        let origin_main = RemoteBranchName {
            remote_name: "origin".to_string(),
            branch_name: "main".to_string(),
        };

        // Multiple repositories: only the current branch, regardless of default.
        assert_eq!(
            worktree_create_targets(true, Some(origin_main.clone()), Some("feature")),
            vec![WorktreeCreateTarget::CurrentBranch]
        );

        // Default branch differs from current: offer both, default first.
        assert_eq!(
            worktree_create_targets(false, Some(origin_main.clone()), Some("feature")),
            vec![
                WorktreeCreateTarget::DefaultBranch(origin_main.clone()),
                WorktreeCreateTarget::CurrentBranch,
            ]
        );

        // Current branch matches the default: only the default branch entry.
        assert_eq!(
            worktree_create_targets(false, Some(origin_main.clone()), Some("main")),
            vec![WorktreeCreateTarget::DefaultBranch(origin_main)]
        );

        // No default branch resolved: fall back to the current branch.
        assert_eq!(
            worktree_create_targets(false, None, Some("feature")),
            vec![WorktreeCreateTarget::CurrentBranch]
        );
    }

    #[test]
    fn test_worktree_create_target_branch_label() {
        let origin_main = RemoteBranchName {
            remote_name: "origin".to_string(),
            branch_name: "main".to_string(),
        };
        assert_eq!(
            WorktreeCreateTarget::DefaultBranch(origin_main).branch_label(false, Some("feature")),
            "origin/main"
        );
        assert_eq!(
            WorktreeCreateTarget::CurrentBranch.branch_label(false, Some("feature")),
            "feature"
        );
        // Detached HEAD falls back to "HEAD".
        assert_eq!(
            WorktreeCreateTarget::CurrentBranch.branch_label(false, None),
            "HEAD"
        );
        // Multiple repositories pluralize the current branch.
        assert_eq!(
            WorktreeCreateTarget::CurrentBranch.branch_label(true, Some("feature")),
            "current branches"
        );
    }

    fn fake_common_identity(path: &str) -> CommonRepositoryIdentity {
        CommonRepositoryIdentity::from_path_for_tests(std::sync::Arc::from(Path::new(path)))
    }

    #[test]
    fn test_resolve_target_base_sha_commit_grouping_is_order_independent() {
        let clicked = fake_common_identity("/root/a/.git");
        let sibling = fake_common_identity("/root/a/.git");
        let other_repo = fake_common_identity("/root/b/.git");

        let commit_target = NewWorktreeBranchTarget::Commit {
            repository_id: 1,
            sha: "abc123".to_string(),
        };

        // The clicked repository and any sibling sharing its common identity
        // are based on the selected SHA; a distinct repository uses HEAD.
        let clicked_base =
            resolve_target_base_sha(&commit_target, Some(&clicked), &clicked).unwrap();
        let sibling_base =
            resolve_target_base_sha(&commit_target, Some(&clicked), &sibling).unwrap();
        let other_base =
            resolve_target_base_sha(&commit_target, Some(&clicked), &other_repo).unwrap();

        assert_eq!(clicked_base.as_deref(), Some("abc123"));
        assert_eq!(sibling_base.as_deref(), Some("abc123"));
        assert_eq!(other_base, None);

        // Reversing which repository position is inspected first does not
        // change any allocation: equality is symmetric.
        assert_eq!(
            resolve_target_base_sha(&commit_target, Some(&sibling), &clicked).unwrap(),
            Some("abc123".to_string())
        );
    }

    #[test]
    fn test_resolve_target_base_sha_non_commit_targets_share_one_base() {
        let identity = fake_common_identity("/root/a/.git");
        let ta = |target: &NewWorktreeBranchTarget, repo: &CommonRepositoryIdentity| {
            resolve_target_base_sha(target, None, repo).unwrap()
        };

        // For non-commit targets the clicked identity is irrelevant and every
        // repository resolves to the same single base.
        let current = NewWorktreeBranchTarget::CurrentBranch;
        assert_eq!(ta(&current, &identity), None);

        let existing = NewWorktreeBranchTarget::ExistingBranch {
            name: "feature".to_string(),
        };
        assert_eq!(ta(&existing, &identity).as_deref(), Some("feature"));

        let remote = NewWorktreeBranchTarget::RemoteBranch {
            remote_name: "origin".to_string(),
            branch_name: "main".to_string(),
        };
        assert_eq!(
            ta(&remote, &identity).as_deref(),
            Some("refs/remotes/origin/main")
        );

        // A Commit target never resolves when no clicked identity was resolved
        // (defensive; `do_create_worktree` always resolves it or errors first).
        let commit = NewWorktreeBranchTarget::Commit {
            repository_id: 1,
            sha: "abc123".to_string(),
        };
        assert_eq!(ta(&commit, &identity), None);
    }

    /// Returns the repository entity whose work directory is `work_dir`,
    /// panicking if the project does not contain one.
    fn repo_with_work_dir(
        project: &Entity<Project>,
        work_dir: &Path,
        cx: &mut gpui::VisualTestContext,
    ) -> Entity<Repository> {
        project.read_with(cx, |project, cx| {
            project
                .repositories(cx)
                .values()
                .find(|repo| repo.read(cx).work_directory_abs_path.as_ref() == work_dir)
                .cloned()
                .unwrap_or_else(|| panic!("expected a repository for {work_dir:?}"))
        })
    }

    /// Runs a full commit-target worktree creation for `worktree_name` at
    /// `sha` on the multi-root project in `multi_workspace`.
    async fn create_commit_worktree(
        multi_workspace: &Entity<MultiWorkspace>,
        repository_id: u64,
        sha: &str,
        worktree_name: &str,
        cx: &mut gpui::VisualTestContext,
    ) -> anyhow::Result<CreatedWorktreeWorkspace> {
        let main_workspace = multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
        let action = zed_actions::CreateWorktree {
            worktree_name: Some(worktree_name.to_string()),
            branch_target: NewWorktreeBranchTarget::Commit {
                repository_id,
                sha: sha.to_string(),
            },
        };
        let task = main_workspace.update_in(cx, |workspace, window, cx| {
            create_worktree_workspace(workspace, &action, window, None, OpenMode::Activate, cx)
        });
        task.await
    }

    /// Returns the checked-out SHA recorded in each newly created linked
    /// worktree under `<root>/.git/worktrees/*/HEAD`, keyed by the repo root.
    fn created_worktree_head_shas(
        fs: &FakeFs,
        repo_roots: &[&Path],
    ) -> std::collections::BTreeMap<PathBuf, Vec<String>> {
        let mut result: std::collections::BTreeMap<PathBuf, Vec<String>> = Default::default();
        for (path, content) in fs.files_with_contents(std::path::Path::new("/")) {
            if !path.ends_with("HEAD") {
                continue;
            }
            let has_worktrees_component = path
                .components()
                .any(|component| component == std::path::Component::Normal("worktrees".as_ref()));
            if !has_worktrees_component {
                continue;
            }
            let Some(root) = repo_roots
                .iter()
                .find(|root| path.starts_with(root))
                .map(|root| (*root).to_path_buf())
            else {
                continue;
            };
            result
                .entry(root)
                .or_default()
                .push(String::from_utf8_lossy(content.as_slice()).trim().to_string());
        }
        result
    }

    #[gpui::test]
    async fn test_commit_target_applies_only_to_matching_common_repository(
        cx: &mut TestAppContext,
    ) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn fs::Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "repo-a": { ".git": {}, "src": { "a.rs": "fn a() {}" } },
                "repo-b": { ".git": {}, "src": { "b.rs": "fn b() {}" } },
                "notes": { "notes.txt": "note" },
            }),
        )
        .await;

        let root_a = PathBuf::from(path!("/root/repo-a"));
        let root_b = PathBuf::from(path!("/root/repo-b"));
        let non_git_root = PathBuf::from(path!("/root/notes"));

        let project =
            Project::test(fs.clone(), [root_a.as_path(), root_b.as_path(), non_git_root.as_path()], cx)
                .await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));

        let repo_a = repo_with_work_dir(&project, &root_a, cx);
        let repo_b = repo_with_work_dir(&project, &root_b, cx);
        assert_ne!(repo_a.read_with(cx, |r, _| r.id), repo_b.read_with(cx, |r, _| r.id));

        let created = create_commit_worktree(
            &multi_workspace,
            repo_a.read_with(cx, |r, _| r.id.to_proto()),
            "abc123",
            "feature",
            cx,
        )
        .await
        .expect("commit-target creation should succeed");

        let shas = created_worktree_head_shas(&fs, &[&root_a, &root_b]);
        assert_eq!(
            shas.get(&root_a).map(|v| v.as_slice()),
            Some(&["abc123".to_string()][..]),
            "the clicked common repository should be based on the selected SHA"
        );
        assert_eq!(
            shas.get(&root_b).map(|v| v.as_slice()),
            Some(&["fake-sha".to_string()][..]),
            "the other underlying git repository should use current HEAD"
        );
        assert_eq!(shas.len(), 2, "exactly one worktree per common repository");

        // The non-git root must be retained in the created workspace.
        let has_non_git_root = created.workspace.read_with(cx, |ws, cx| {
            ws.project()
                .read(cx)
                .visible_worktrees(cx)
                .any(|wt| wt.read(cx).abs_path().as_ref() == non_git_root.as_path())
        });
        assert!(has_non_git_root, "non-git roots must remain in the new workspace");
    }

    #[gpui::test]
    async fn test_commit_target_is_independent_of_root_order(cx: &mut TestAppContext) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn fs::Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "repo-a": { ".git": {}, "src": { "a.rs": "fn a() {}" } },
                "repo-b": { ".git": {}, "src": { "b.rs": "fn b() {}" } },
                "notes": { "notes.txt": "note" },
            }),
        )
        .await;

        // Register the roots in the reverse order from the grouping test.
        let root_a = PathBuf::from(path!("/root/repo-a"));
        let root_b = PathBuf::from(path!("/root/repo-b"));
        let non_git_root = PathBuf::from(path!("/root/notes"));

        let project =
            Project::test(fs.clone(), [non_git_root.as_path(), root_b.as_path(), root_a.as_path()], cx)
                .await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));

        let repo_a = repo_with_work_dir(&project, &root_a, cx);

        let created = create_commit_worktree(
            &multi_workspace,
            repo_a.read_with(cx, |r, _| r.id.to_proto()),
            "abc123",
            "feature-order",
            cx,
        )
        .await
        .expect("commit-target creation should succeed");

        let shas = created_worktree_head_shas(&fs, &[&root_a, &root_b]);
        assert_eq!(
            shas.get(&root_a).map(|v| v.as_slice()),
            Some(&["abc123".to_string()][..]),
        );
        assert_eq!(
            shas.get(&root_b).map(|v| v.as_slice()),
            Some(&["fake-sha".to_string()][..]),
        );
        assert_eq!(shas.len(), 2);

        let has_non_git_root = created.workspace.read_with(cx, |ws, cx| {
            ws.project()
                .read(cx)
                .visible_worktrees(cx)
                .any(|wt| wt.read(cx).abs_path().as_ref() == non_git_root.as_path())
        });
        assert!(has_non_git_root);
    }

    #[gpui::test]
    async fn test_missing_commit_target_repository_fails_before_creation(cx: &mut TestAppContext) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn fs::Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project": { ".git": {}, "src": { "main.rs": "fn main() {}" } },
            }),
        )
        .await;

        let project_root = PathBuf::from(path!("/root/project"));
        let project = Project::test(fs.clone(), [project_root.as_path()], cx).await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));

        // Use a repository id that is not present in the project.
        let missing_id = u64::MAX;
        let result = create_commit_worktree(
            &multi_workspace,
            missing_id,
            "abc123",
            "feature",
            cx,
        )
        .await;
        let err = match result {
            Ok(_) => panic!("missing repository must fail creation"),
            Err(err) => err,
        };

        let message = format!("{err:#}");
        assert!(
            message.contains("no repository with id"),
            "unexpected error message: {message}"
        );

        // No worktree may have been created anywhere.
        let shas = created_worktree_head_shas(&fs, &[&project_root]);
        assert!(
            shas.values().all(|heads| heads.is_empty()),
            "no worktree should be created when the target repository is missing"
        );
    }
}
