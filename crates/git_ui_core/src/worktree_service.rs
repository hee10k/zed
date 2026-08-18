use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Context as _;
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
    notifications::DetachAndPromptErr,
};
use zed_actions::NewWorktreeBranchTarget;

use git::repository::{Branch, CreateWorktreeTarget, FetchOptions, Remote, Worktree};

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

/// Explicit no-op/error reasons a worktree navigation is prevented. The Git
/// Graph navigation surfaces these to the user (via a toast) instead of
/// silently falling back, covering the five failure categories: current-target,
/// disappeared-target, stale-snapshot, missing-window-handle, and open-failure
/// (the last is produced by [`open_worktree_workspace`] and surfaced by the
/// switch/open wrappers rather than this enum).
#[derive(Clone, Debug)]
pub enum WorktreeNavigationBlocker {
    /// The requested target is the workspace the user is already in.
    AlreadyCurrent { display_name: SharedString },
    /// The target worktree directory no longer exists on disk.
    TargetDisappeared { path: PathBuf },
    /// The offer came from a stale snapshot: the path/sha is no longer a live
    /// linked worktree in repository state.
    StaleSnapshot { path: PathBuf },
    /// The source workspace's window handle is no longer available.
    NoWindowHandle,
}

impl WorktreeNavigationBlocker {
    /// User-facing explanation of why the navigation cannot proceed. Callers
    /// surface this via a toast so the block is never silent.
    pub fn message(&self) -> SharedString {
        match self {
            WorktreeNavigationBlocker::AlreadyCurrent { display_name } => {
                format!("Already working in {display_name}").into()
            }
            WorktreeNavigationBlocker::TargetDisappeared { path } => {
                format!("Cannot open worktree: {} no longer exists", path.display()).into()
            }
            WorktreeNavigationBlocker::StaleSnapshot { path } => {
                format!(
                    "This worktree is no longer linked at {}. Refresh the Git Graph and try again.",
                    path.display()
                )
                .into()
            }
            WorktreeNavigationBlocker::NoWindowHandle => {
                "Worktree window is no longer available; open the folder from the project picker."
                    .into()
            }
        }
    }
}

/// The last up-to-two components of `path`, joined portably with `/`. Used to
/// build stable Git Graph worktree labels: sibling worktrees that share a final
/// folder name stay distinguishable because their parent component differs.
fn portable_short_path(path: &Path) -> String {
    let mut components = Vec::new();
    for component in path.components().rev().take(2) {
        components.push(component.as_os_str().to_string_lossy().into_owned());
    }
    components.reverse();
    components.join("/")
}

/// A stable, distinguishable label for a linked worktree shown in the Git Graph
/// commit submenu. Combines the checked-out branch with a portable short path so
/// that entries at the same commit remain distinguishable by both — even when
/// their final folder names collide (e.g. two worktrees both named "worktree"
/// under different parent directories, or a detached-HEAD worktree).
pub fn linked_worktree_label(worktree: &Worktree) -> SharedString {
    let branch = worktree.branch_name().unwrap_or("detached HEAD");
    format!("{branch} · {}", portable_short_path(&worktree.path)).into()
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

/// Shared "open this worktree in a new OS window" seam. Every open-in-new-window
/// control — the Worktree Picker's row/icon buttons, the Git Graph commit
/// submenu, and the `OpenWorktreeInNewWindow` action handler — routes through
/// this so the local and remote implementations live behind one interface. The
/// picker-owned remote implementation ([`open_remote_worktree`]) was moved here
/// during the cutover, so there is no longer a duplicate high-level path.
///
/// Opening in a new window neither transfers the source window's open files nor
/// its dock layout: the destination is a clean checkout. Errors are surfaced as
/// a toast on the source workspace (the explicit open-failure state) and
/// returned so callers can respond.
pub fn open_worktree_in_new_window(
    workspace: &mut Workspace,
    path: PathBuf,
    window: &mut gpui::Window,
    cx: &mut gpui::Context<Workspace>,
) -> Task<anyhow::Result<()>> {
    let workspace_handle = workspace.weak_handle();

    if workspace.project().read(cx).is_local() {
        // A distinct OS window opens via `open_workspace_for_paths` with
        // `WorkspaceMatching::None` and no state transfer: the destination is a
        // clean checkout with neither the source window's files nor its docks.
        let open_task =
            workspace.open_workspace_for_paths(OpenMode::NewWindow, vec![path], window, cx);
        cx.spawn_in(window, async move |_, cx| {
            let result = open_task.await.map(|_workspace| ());
            surface_new_window_open_error(&workspace_handle, &result, cx);
            result
        })
    } else {
        let connection_options = workspace.project().read(cx).remote_connection_options(cx);
        let app_state = workspace.app_state().clone();
        cx.spawn_in(window, async move |_, cx| {
            let result = match connection_options {
                Some(connection_options) => open_remote_worktree(
                    connection_options,
                    vec![path],
                    app_state,
                    workspace_handle.clone(),
                    cx,
                )
                .await,
                None => anyhow::Ok(()),
            };
            surface_new_window_open_error(&workspace_handle, &result, cx);
            result
        })
    }
}

/// Surfaces an open-in-new-window failure to the user (explicit open-failure
/// state) rather than silently dropping the error. Kept as a helper so the
/// local and remote branches of [`open_worktree_in_new_window`] share it.
fn surface_new_window_open_error(
    workspace_handle: &WeakEntity<Workspace>,
    result: &anyhow::Result<()>,
    cx: &mut AsyncWindowContext,
) {
    if let Err(err) = result {
        log::error!("Failed to open worktree in new window: {err}");
        workspace_handle
            .update(cx, |_workspace, cx| {
                show_error_toast(
                    cx.entity(),
                    "open worktree in new window",
                    anyhow!("{err:#}"),
                    cx,
                );
            })
            .ok();
    }
}

/// Opens a linked-worktree folder in a new OS window for a remote project,
/// reusing the existing connection (showing a connection modal if disconnected)
/// and restoring the workspace position stored for that folder. Shared by the
/// Worktree Picker, the Git Graph submenu, and the `OpenWorktreeInNewWindow`
/// action handler through [`open_worktree_in_new_window`].
pub async fn open_remote_worktree(
    connection_options: remote::RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<workspace::AppState>,
    workspace: gpui::WeakEntity<Workspace>,
    cx: &mut gpui::AsyncWindowContext,
) -> anyhow::Result<()> {
    let connect_task = workspace.update_in(cx, |workspace, window, cx| {
        workspace.toggle_modal(window, cx, |window, cx| {
            remote_connection::RemoteConnectionModal::new(
                &connection_options,
                Vec::new(),
                window,
                cx,
            )
        });

        let prompt = workspace
            .active_modal::<remote_connection::RemoteConnectionModal>(cx)
            .expect("Modal just created")
            .read(cx)
            .prompt
            .clone();

        remote_connection::connect(
            remote::remote_client::ConnectionIdentifier::setup(),
            connection_options.clone(),
            prompt,
            window,
            cx,
        )
        .prompt_err("Failed to connect", window, cx, |_, _, _| None)
    })?;

    let session = connect_task.await;

    workspace
        .update_in(cx, |workspace, _window, cx| {
            if let Some(prompt) =
                workspace.active_modal::<remote_connection::RemoteConnectionModal>(cx)
            {
                prompt.update(cx, |prompt, cx| prompt.finished(cx))
            }
        })
        .ok();

    let Some(Some(session)) = session else {
        return Ok(());
    };

    let new_project = cx.update(|_, cx| {
        project::Project::remote(
            session,
            app_state.client.clone(),
            app_state.node_runtime.clone(),
            app_state.user_store.clone(),
            app_state.languages.clone(),
            app_state.fs.clone(),
            true,
            cx,
        )
    })?;

    let workspace_position = cx
        .update(|_, cx| {
            workspace::remote_workspace_position_from_db(connection_options.clone(), &paths, cx)
        })?
        .await
        .context("fetching workspace position from db")?;

    let mut options =
        cx.update(|_, cx| (app_state.build_window_options)(workspace_position.display, cx))?;
    options.window_bounds = workspace_position.window_bounds;

    let new_window = cx.open_window(options, |window, cx| {
        let workspace = cx.new(|cx| {
            let mut workspace =
                Workspace::new(None, new_project.clone(), app_state.clone(), window, cx);
            workspace.centered_layout = workspace_position.centered_layout;
            workspace
        });
        cx.new(|cx| MultiWorkspace::new(workspace, window, cx))
    })?;

    workspace::open_remote_project_with_existing_connection(
        connection_options,
        new_project,
        paths,
        app_state,
        new_window,
        None,
        None,
        cx,
    )
    .await?;

    Ok(())
}

/// Validates a switch to `target_path` against live repository state and the
/// current workspace, returning the explicit [`WorktreeNavigationBlocker`] that
/// should stop the switch (to be shown to the user) or `None` if it may
/// proceed. This is the single shared decision point for the current-target,
/// disappeared-target, and stale-snapshot states, so every switch caller (the
/// Git Graph submenu, the Worktree Picker row, and the `SwitchWorktree` action
/// handler) gets explicit no-op handling rather than a silent fallback.
///
/// `offer_sha` is the commit SHA captured when the offering control built its
/// entry. Only the Git Graph supplies it (its entries are always linked
/// worktrees); when present it enables the stale-snapshot check. Switches to
/// the main checkout or a plain folder (Sidebar, Agent Panel) never supply it,
/// so they are never misclassified as stale.
pub async fn switch_worktree_blocker(
    source_workspace: &Entity<Workspace>,
    target_path: &Path,
    display_name: SharedString,
    offer_sha: Option<&str>,
    cx: &mut AsyncWindowContext,
) -> Option<WorktreeNavigationBlocker> {
    // Current-target: the target is already the active workspace's root. This
    // is the common "switch to the worktree I'm already in" no-op.
    let is_current = cx
        .update(|_, cx| {
            source_workspace
                .read(cx)
                .project()
                .read(cx)
                .visible_worktrees(cx)
                .any(|worktree| worktree.read(cx).abs_path().as_ref() == target_path)
        })
        .unwrap_or(false);
    if is_current {
        return Some(WorktreeNavigationBlocker::AlreadyCurrent { display_name });
    }

    // Stale-snapshot: the Git Graph offered a linked worktree at this path/sha,
    // but it is no longer a live linked worktree in repository state. Only
    // checked when the offer carries a SHA, so non-graph switches are unaffected.
    if let Some(offer_sha) = offer_sha {
        let still_linked = cx
            .update(|_, cx| {
                source_workspace
                    .read(cx)
                    .project()
                    .read(cx)
                    .repositories(cx)
                    .values()
                    .any(|repository| {
                        repository.read(cx).linked_worktrees().iter().any(|worktree| {
                            worktree.path == target_path && worktree.sha.as_ref() == offer_sha
                        })
                    })
            })
            .unwrap_or(false);
        if !still_linked {
            return Some(WorktreeNavigationBlocker::StaleSnapshot {
                path: target_path.to_path_buf(),
            });
        }
    }

    // Disappeared-target: the worktree folder no longer exists on disk.
    let fs = cx.update(|_, cx| <dyn Fs>::global(cx)).ok()?;
    if !fs.is_dir(target_path).await {
        return Some(WorktreeNavigationBlocker::TargetDisappeared {
            path: target_path.to_path_buf(),
        });
    }

    None
}

/// Handles the `SwitchWorktree` action generically (Git UI action handler,
/// Worktree Picker row, Sidebar, Agent Panel). Same-window workspace activation
/// via the shared switch implementation; explicit no-op states are toasted.
pub fn handle_switch_worktree(
    workspace: &mut Workspace,
    action: &zed_actions::SwitchWorktree,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) {
    switch_worktree_impl(
        workspace,
        action.path.clone(),
        action.display_name.clone().into(),
        None,
        window,
        fallback_focused_dock,
        open_mode,
        cx,
    );
}

/// Shared switch seam used by the Git Graph submenu. Identical to
/// [`handle_switch_worktree`] but carries the commit SHA the menu entry was
/// built from, enabling the stale-snapshot explicit state (a linked worktree
/// removed or moved since the menu was captured surfaces as a toast instead of
/// silently proceeding).
pub fn switch_to_worktree(
    workspace: &mut Workspace,
    target_path: PathBuf,
    display_name: SharedString,
    offer_sha: Option<SharedString>,
    window: &mut gpui::Window,
    fallback_focused_dock: Option<DockPosition>,
    open_mode: OpenMode,
    cx: &mut gpui::Context<Workspace>,
) {
    switch_worktree_impl(
        workspace,
        target_path,
        display_name,
        offer_sha,
        window,
        fallback_focused_dock,
        open_mode,
        cx,
    );
}

fn switch_worktree_impl(
    workspace: &mut Workspace,
    worktree_path: PathBuf,
    display_name: SharedString,
    offer_sha: Option<SharedString>,
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
    let source_workspace = cx.entity();
    let window_handle = window.window_handle().downcast::<MultiWorkspace>();
    let remote_connection_options = project.read(cx).remote_connection_options(cx);

    let (git_repos, non_git_paths) = classify_worktrees(project.read(cx), cx);

    let git_repo_work_dirs: Vec<PathBuf> = git_repos
        .iter()
        .map(|repo| repo.read(cx).work_directory_abs_path.to_path_buf())
        .collect();

    workspace.set_active_worktree_creation(Some(display_name.clone()), true, cx);

    let blocker_path = worktree_path.clone();

    cx.spawn_in(window, async move |_workspace_entity, mut cx| {
        // Surface explicit no-op states (already-current, disappeared target,
        // stale snapshot) instead of silently proceeding or doing nothing.
        if let Some(blocker) = switch_worktree_blocker(
            &source_workspace,
            &blocker_path,
            display_name,
            offer_sha.as_deref(),
            &mut cx,
        )
        .await
        {
            workspace_handle
                .update(cx, |workspace, cx| {
                    workspace.set_active_worktree_creation(None, false, cx);
                    show_error_toast(
                        cx.entity(),
                        "worktree switch",
                        anyhow!(blocker.message()),
                        cx,
                    );
                })
                .ok();
            return anyhow::Ok(());
        }

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

        result.map(|_workspace| ())
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

    #[test]
    fn test_linked_worktree_label_distinguishes_same_sha_worktrees() {
        let make = |path: &str, ref_name: Option<&str>| Worktree {
            path: PathBuf::from(path),
            ref_name: ref_name.map(Into::into),
            sha: "abc123".into(),
            is_main: false,
            is_bare: false,
        };

        // Same commit, same final folder name, different parents: entries must
        // stay distinguishable via their portable path.
        let a = make("/root/worktrees/wt-a/feature", Some("refs/heads/feature"));
        let b = make("/root/worktrees/wt-b/feature", Some("refs/heads/feature"));
        let label_a = linked_worktree_label(&a);
        let label_b = linked_worktree_label(&b);
        assert_ne!(
            label_a, label_b,
            "same-SHA entries must be distinguishable by parent path"
        );
        assert!(
            label_a.contains("feature"),
            "label should carry the checked-out branch"
        );
        assert!(
            label_a.contains("wt-a/feature"),
            "label should carry a portable path"
        );

        // Different branches on the same path also differ.
        let c = make("/root/worktrees/wt-a/feature", Some("refs/heads/other"));
        assert_ne!(linked_worktree_label(&c), label_a);

        // A detached-HEAD worktree has no branch; the portable path remains.
        let detached = make("/root/worktrees/wt-a/feature", None);
        let detached_label = linked_worktree_label(&detached);
        assert!(
            detached_label.contains("detached HEAD"),
            "detached worktree label should still surface a branch-or-fallback"
        );
        assert!(detached_label.contains("wt-a/feature"));
    }

    #[test]
    fn test_worktree_navigation_blocker_messages_are_non_empty_and_distinct() {
        let blockers = [
            WorktreeNavigationBlocker::AlreadyCurrent {
                display_name: "feature · wt-a/feature".into(),
            },
            WorktreeNavigationBlocker::TargetDisappeared {
                path: PathBuf::from("/root/gone"),
            },
            WorktreeNavigationBlocker::StaleSnapshot {
                path: PathBuf::from("/root/gone"),
            },
            WorktreeNavigationBlocker::NoWindowHandle,
        ];
        let mut seen = std::collections::HashSet::new();
        for blocker in blockers {
            let message = blocker.message();
            assert!(!message.is_empty(), "blocker message must never be empty");
            assert!(
                seen.insert(message),
                "blocker messages should be distinguishable"
            );
        }
    }

    #[gpui::test]
    async fn test_switch_worktree_blocker_states(cx: &mut TestAppContext) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project": { ".git": {}, "src": { "main.rs": "fn main() {}" } },
                "other": { "file.txt": "x" },
            }),
        )
        .await;

        let main_root = PathBuf::from(path!("/root/project"));
        let other_root = PathBuf::from(path!("/root/other"));
        let gone_root = PathBuf::from(path!("/root/gone"));
        let project = Project::test(fs.clone(), [main_root.as_path()], cx).await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));
        let workspace_entity = multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());

        // Run `switch_worktree_blocker` on the foreground executor.
        let mut run_blocker = |target: PathBuf, offer_sha: Option<&str>| {
            let workspace_entity = workspace_entity.clone();
            let offer_sha = offer_sha.map(ToOwned::to_owned);
            multi_workspace.update_in(cx, |_mw, window, cx| {
                cx.spawn_in(window, async move |_, cx| {
                    switch_worktree_blocker(
                        &workspace_entity,
                        &target,
                        "wt".into(),
                        offer_sha.as_deref(),
                        cx,
                    )
                    .await
                })
            })
        };

        // Current-target: switching to the already-active workspace is an
        // explicit no-op, not a silent fallback.
        assert!(
            matches!(
                run_blocker(main_root.clone(), None).await,
                Some(WorktreeNavigationBlocker::AlreadyCurrent { .. })
            ),
            "switching to the already-active workspace must be an explicit no-op"
        );

        // A switch to an existing sibling folder (no stale offer) proceeds.
        assert!(
            run_blocker(other_root.clone(), None).await.is_none(),
            "a plain switch to an existing sibling folder must proceed"
        );

        // Stale-snapshot: a Git Graph offer for a path that is no longer a
        // live linked worktree is surfaced even though the folder exists.
        assert!(
            matches!(
                run_blocker(other_root.clone(), Some("abc123")).await,
                Some(WorktreeNavigationBlocker::StaleSnapshot { .. })
            ),
            "a stale Git Graph offer at a non-linked path must be surfaced"
        );

        // Disappeared-target: the folder no longer exists on disk.
        assert!(
            matches!(
                run_blocker(gone_root.clone(), None).await,
                Some(WorktreeNavigationBlocker::TargetDisappeared { .. })
            ),
            "a vanished worktree folder must be surfaced as disappeared"
        );
    }

    #[gpui::test]
    async fn test_switch_to_worktree_activates_sibling_in_same_window_without_terminal_cd(
        cx: &mut TestAppContext,
    ) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project": { ".git": {}, "src": { "main.rs": "fn main() {}" } },
            }),
        )
        .await;

        let main_root = PathBuf::from(path!("/root/project"));
        let project = Project::test(fs.clone(), [main_root.as_path()], cx).await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

        let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));

        let spawned_task_labels = Arc::new(Mutex::new(Vec::new()));
        multi_workspace.update(cx, |mw, cx| {
            mw.workspace().update(cx, |workspace, _cx| {
                workspace.set_terminal_provider(CountingTerminalProvider {
                    spawned_task_labels: spawned_task_labels.clone(),
                });
            });
        });

        let main_workspace =
            multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
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

        let active_root_after_create = multi_workspace
            .read_with(cx, |mw, cx| {
                mw.workspace()
                    .read(cx)
                    .project()
                    .read(cx)
                    .visible_worktrees(cx)
                    .next()
                    .map(|wt| wt.read(cx).abs_path().to_path_buf())
            })
            .expect("active workspace should have a root");
        assert_ne!(
            active_root_after_create.as_path(),
            main_root.as_path(),
            "creating a worktree should foreground the new linked worktree"
        );
        assert_eq!(cx.windows().len(), 1, "the same-window flow must not open a new OS window");

        let spawns_after_create = spawned_task_labels
            .lock()
            .expect("terminal spawn mutex should not be poisoned")
            .len();

        // Switch back to the main worktree through the shared service switch.
        let active_before =
            multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
        active_before.update_in(cx, |workspace, window, cx| {
            switch_to_worktree(
                workspace,
                main_root.clone(),
                "project".into(),
                None,
                window,
                None,
                OpenMode::Activate,
                cx,
            );
        });
        cx.run_until_parked();

        let active_root_after_switch = multi_workspace
            .read_with(cx, |mw, cx| {
                mw.workspace()
                    .read(cx)
                    .project()
                    .read(cx)
                    .visible_worktrees(cx)
                    .next()
                    .map(|wt| wt.read(cx).abs_path().to_path_buf())
            })
            .expect("a workspace should be active after switch");
        assert_eq!(
            active_root_after_switch.as_path(),
            main_root.as_path(),
            "switch must activate the sibling workspace in the current window"
        );

        // A worktree switch is Workspace activation, never a terminal `cd`:
        // no terminal command (e.g. a `cd`) may have been spawned for it.
        let spawns_after_switch = spawned_task_labels
            .lock()
            .expect("terminal spawn mutex should not be poisoned")
            .len();
        assert_eq!(
            spawns_after_switch, spawns_after_create,
            "a worktree switch must not spawn a terminal or issue a `cd`"
        );
    }

    #[gpui::test]
    async fn test_open_worktree_in_new_window_opens_second_os_window(cx: &mut TestAppContext) {
        init_test(cx);

        let fs = FakeFs::new(cx.background_executor.clone());
        cx.update(|cx| <dyn Fs>::set_global(fs.clone(), cx));
        fs.insert_tree(
            "/root",
            json!({
                "project-old": { ".git": {}, "src": { "main.rs": "fn main() {}" } },
                "project-new": { "file.txt": "hi" },
            }),
        )
        .await;

        let old_root = PathBuf::from(path!("/root/project-old"));
        let new_root = PathBuf::from(path!("/root/project-new"));
        let project = Project::test(fs.clone(), [old_root.as_path()], cx).await;
        project.update(cx, |project, cx| project.git_scans_complete(cx)).await;

let (multi_workspace, cx) =
            cx.add_window_view(|window, cx| MultiWorkspace::test_new(project.clone(), window, cx));
        multi_workspace.update(cx, |mw, cx| mw.retain_active_workspace(cx));
        let source_workspace =
            multi_workspace.read_with(cx, |mw, _| mw.workspace().clone());
        let open_task = source_workspace.update_in(cx, |workspace, window, cx| {
            open_worktree_in_new_window(workspace, new_root.clone(), window, cx)
        });
        open_task
            .await
            .expect("opening a worktree in a new window should succeed");
        cx.run_until_parked();

        // A distinct, second OS window opened.
        assert_eq!(cx.windows().len(), 2, "new-window open must create a second OS window");

        // The new window's workspace is rooted at the target and carried over
        // no open files or docks from the source window.
        let new_item_count = cx
            .windows()
            .iter()
            .find_map(|handle| {
                let mw = handle.downcast::<MultiWorkspace>()?;
                mw.read_with(cx, |mw, cx| {
                    let ws = mw.workspace();
                    let root = ws
                        .read(cx)
                        .project()
                        .read(cx)
                        .visible_worktrees(cx)
                        .next()
                        .map(|wt| wt.read(cx).abs_path().to_path_buf());
                    if root.as_deref() == Some(new_root.as_path()) {
                        Some(ws.read(cx).items(cx).count())
                    } else {
                        None
                    }
                })
                .ok()
                .flatten()
            })
            .expect("a new-window workspace rooted at the target should exist");
        assert_eq!(
            new_item_count, 0,
            "open in new window must not transfer files from the source window"
        );
    }
}
