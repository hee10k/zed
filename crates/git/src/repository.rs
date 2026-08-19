use crate::commit::{CommitDiffObject, CommitDiffObjectKind, parse_git_diff_raw};
use crate::stash::{
    GitStash, STASH_REF, STASH_RENAME_MANIFEST_VERSION, STASH_RENAME_RECOVERY_PREFIX, StashEntry,
    StashIdentity, StashMutationResult, StashRenameRecovery, StashRenameResult,
    parse_stash_index, parse_stash_message, renamed_stash_subject, resolve_stash_identity,
};
use crate::status::{
    DiffTreeType, FileStatus, GitStatus, StatusCode, TrackedStatus, TreeDiff, TreeDiffStatus,
};
use crate::{Oid, RunHook, SHORT_SHA_LENGTH};
use anyhow::{Context as _, Result, anyhow, bail};
use async_channel::Sender;
use collections::HashMap;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::io::BufWriter;
use futures::{AsyncWriteExt, FutureExt as _, select_biased};
use gpui::{AppContext as _, AsyncApp, BackgroundExecutor, SharedString, Task};
use parking_lot::Mutex;
use rope::Rope;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use text::LineEnding;

use std::borrow::Cow;
use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::AtomicBool;

use std::process::{ExitStatus, Output};
use std::str::FromStr;
use std::time::SystemTime;
use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
    sync::Arc,
};
use sum_tree::MapSeekTarget;
use thiserror::Error;
use util::command::{Stdio, new_command};
use util::paths::PathStyle;
use util::rel_path::RelPath;
use util::{ResultExt, paths};
use uuid::Uuid;

pub use askpass::{AskPassDelegate, AskPassResult, AskPassSession};

pub const REMOTE_CANCELLED_BY_USER: &str = "Operation cancelled by user";

/// Format string used in graph log to get initial data for the git graph
/// %H - Full commit hash
/// %P - Parent hashes
/// %D - Ref names
/// %x00 - Null byte separator, used to split up commit data
///
/// NOTE: we combine this with `--decorate=full` when building the log command so
/// that `%D` emits fully-qualified ref names (`refs/heads/main`,
/// `refs/remotes/origin/main`, `tag: refs/tags/v1.0`) rather than git's
/// display-shortened form. `%D` by itself shortens remote-tracking and local
/// refs the same way (e.g. both `refs/heads/origin/main` and
/// `refs/remotes/origin/main` render as `origin/main`), which makes a local
/// branch literally named `origin/main` indistinguishable from the
/// remote-tracking `origin/main`. Consumers classify against the full ref name
/// and only shorten for display after classification (see git_graph.rs).
static GRAPH_COMMIT_FORMAT: &str = "--format=%H%x00%P%x00%D";

/// Used to get commits that match with a search
/// %H - Full commit hash
static SEARCH_COMMIT_FORMAT: &str = "--format=%H";

/// Number of commits to load per chunk for the git graph.
pub const GRAPH_CHUNK_SIZE: usize = 1000;

/// Default value for the `git.worktree_directory` setting.
pub const DEFAULT_WORKTREE_DIRECTORY: &str = "../worktrees";

/// Given the git common directory (from `commondir()`), derive the original
/// repository's working directory.
///
/// For a standard checkout, `common_dir` is `<work_dir>/.git`, so the parent
/// is the working directory. For a git worktree, `common_dir` is the **main**
/// repo's `.git` directory, so the parent is the original repo's working directory.
///
/// Returns `None` if `common_dir` doesn't end with `.git` (e.g. bare repos),
/// because there is no working-tree root to resolve to in that case.
pub fn original_repo_path_from_common_dir(common_dir: &Path) -> Option<PathBuf> {
    if common_dir.file_name() == Some(OsStr::new(".git")) {
        common_dir.parent().map(|p| p.to_path_buf())
    } else {
        None
    }
}

fn linked_worktree_git_dir(worktree_path: &Path) -> Result<PathBuf> {
    let dot_git_path = worktree_path.join(".git");
    let git_file = std::fs::read_to_string(&dot_git_path)
        .with_context(|| format!("failed to read {}", dot_git_path.display()))?;
    let git_dir = git_file
        .strip_prefix("gitdir:")
        .context("worktree .git file missing gitdir pointer")?
        .trim();
    Ok(worktree_path.join(git_dir))
}

fn normalize_git_metadata_path(path: PathBuf) -> Result<PathBuf> {
    paths::normalize_lexically(&path)
        .map_err(|_| anyhow!("git metadata path escapes its filesystem root: {path:?}"))
}

/// Commit data needed for the git graph visualization.
#[derive(Debug, Clone)]
pub struct CommitData {
    pub sha: Oid,
    /// Most commits have a single parent, so we use a SmallVec to avoid allocations.
    pub parents: SmallVec<[Oid; 1]>,
    pub author_name: SharedString,
    pub author_email: SharedString,
    pub commit_timestamp: i64,
    pub subject: SharedString,
    pub message: SharedString,
}

#[derive(Debug)]
pub struct InitialGraphCommitData {
    pub sha: Oid,
    pub parents: SmallVec<[Oid; 1]>,
    pub ref_names: Vec<SharedString>,
}

impl InitialGraphCommitData {
    /// If this row is a stash-reflog row, returns its reflog selector
    /// (`refs/stash@{N}`), which is the row's distinct identity. Stash rows
    /// carry the selector as their only ref name; regular commit rows return
    /// `None`.
    pub fn stash_selector(&self) -> Option<&SharedString> {
        self.ref_names
            .iter()
            .find(|ref_name| ref_name.starts_with(&format!("{STASH_REF}@{{")))
    }

    pub fn tag_names(&self) -> Vec<&str> {
        self.ref_names
            .iter()
            .filter_map(|ref_name| {
                // With `--decorate=full` a tag decoration is `tag: refs/tags/<name>`.
                // Fall back to the legacy shortened `tag: <name>` form for graph
                // data produced before the full-decoration change.
                let tag_name = ref_name
                    .strip_prefix("tag: refs/tags/")
                    .or_else(|| ref_name.strip_prefix("tag: "))?;

                if tag_name.is_empty() {
                    return None;
                }
                Some(tag_name)
            })
            .collect()
    }
}

struct CommitDataRequest {
    sha: Oid,
    response_tx: oneshot::Sender<Result<CommitData>>,
}

pub struct CommitDataReader {
    request_tx: async_channel::Sender<CommitDataRequest>,
    _task: Task<()>,
}

impl CommitDataReader {
    pub async fn read(&self, sha: Oid) -> Result<CommitData> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx
            .send(CommitDataRequest { sha, response_tx })
            .await
            .map_err(|_| anyhow!("commit data reader task closed"))?;
        response_rx
            .await
            .map_err(|_| anyhow!("commit data reader task dropped response"))?
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(
        executor: BackgroundExecutor,
        resolve: impl 'static + Send + Sync + Fn(Oid) -> Result<CommitData>,
    ) -> Self {
        let (request_tx, request_rx) = smol::channel::bounded::<CommitDataRequest>(64);
        let resolve = Arc::new(resolve);
        let delay_executor = executor.clone();
        let task = executor.spawn(async move {
            while let Ok(CommitDataRequest { sha, response_tx }) = request_rx.recv().await {
                delay_executor.simulate_random_delay().await;
                response_tx.send(resolve(sha)).ok();
            }
        });

        Self {
            request_tx,
            _task: task,
        }
    }
}

fn parse_cat_file_commit(sha: Oid, content: &str) -> Option<CommitData> {
    let mut parents = SmallVec::new();
    let mut author_name = SharedString::default();
    let mut author_email = SharedString::default();
    let mut commit_timestamp = 0i64;
    let mut in_headers = true;
    let mut subject = None;
    let mut message_lines = Vec::new();

    for line in content.lines() {
        if in_headers {
            if line.is_empty() {
                in_headers = false;
                continue;
            }

            if let Some(parent_sha) = line.strip_prefix("parent ") {
                if let Ok(oid) = Oid::from_str(parent_sha.trim()) {
                    parents.push(oid);
                }
            } else if let Some(author_line) = line.strip_prefix("author ") {
                if let Some((name_email, _timestamp_tz)) = author_line.rsplit_once(' ') {
                    if let Some((name_email, timestamp_str)) = name_email.rsplit_once(' ') {
                        if let Ok(ts) = timestamp_str.parse::<i64>() {
                            commit_timestamp = ts;
                        }
                        if let Some((name, email)) = name_email.rsplit_once(" <") {
                            author_name = SharedString::from(name.to_string());
                            author_email =
                                SharedString::from(email.trim_end_matches('>').to_string());
                        }
                    }
                }
            }
        } else {
            if subject.is_none() {
                subject = Some(SharedString::from(line.to_string()));
            }
            message_lines.push(line);
        }
    }

    Some(CommitData {
        sha,
        parents,
        author_name,
        author_email,
        commit_timestamp,
        subject: subject.unwrap_or_default(),
        message: SharedString::from(message_lines.join("\n")),
    })
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Branch {
    pub is_head: bool,
    pub ref_name: SharedString,
    pub upstream: Option<Upstream>,
    pub most_recent_commit: Option<CommitSummary>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BranchesScanResult {
    pub branches: Vec<Branch>,
    pub error: Option<SharedString>,
}

impl From<Vec<Branch>> for BranchesScanResult {
    fn from(branches: Vec<Branch>) -> Self {
        Self {
            branches,
            error: None,
        }
    }
}

impl Branch {
    pub fn name(&self) -> &str {
        self.ref_name
            .as_ref()
            .strip_prefix("refs/heads/")
            .or_else(|| self.ref_name.as_ref().strip_prefix("refs/remotes/"))
            .unwrap_or(self.ref_name.as_ref())
    }

    pub fn is_remote(&self) -> bool {
        self.ref_name.starts_with("refs/remotes/")
    }

    pub fn remote_name(&self) -> Option<&str> {
        self.ref_name
            .strip_prefix("refs/remotes/")
            .and_then(|stripped| stripped.split("/").next())
    }

    pub fn tracking_status(&self) -> Option<UpstreamTrackingStatus> {
        self.upstream
            .as_ref()
            .and_then(|upstream| upstream.tracking.status())
    }

    pub fn priority_key(&self) -> (bool, Option<i64>) {
        (
            self.is_head,
            self.most_recent_commit
                .as_ref()
                .map(|commit| commit.commit_timestamp),
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Worktree {
    pub path: PathBuf,
    pub ref_name: Option<SharedString>,
    // todo(git_worktree) This type should be a Oid
    pub sha: SharedString,
    pub is_main: bool,
    pub is_bare: bool,
}

/// Describes how a new worktree should choose or create its checked-out HEAD.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum CreateWorktreeTarget {
    /// Check out an existing local branch in the new worktree.
    ExistingBranch {
        /// The existing local branch to check out.
        branch_name: String,
    },
    /// Create a new local branch for the new worktree.
    NewBranch {
        /// The new local branch to create and check out.
        branch_name: String,
        /// The commit or ref to create the branch from. Uses `HEAD` when `None`.
        base_sha: Option<String>,
    },
    /// Check out a commit or ref in detached HEAD state.
    Detached {
        /// The commit or ref to check out. Uses `HEAD` when `None`.
        base_sha: Option<String>,
    },
}

impl CreateWorktreeTarget {
    pub fn branch_name(&self) -> Option<&str> {
        match self {
            Self::ExistingBranch { branch_name } | Self::NewBranch { branch_name, .. } => {
                Some(branch_name)
            }
            Self::Detached { .. } => None,
        }
    }
}

impl Worktree {
    /// Returns the branch name if the worktree is attached to a branch.
    pub fn branch_name(&self) -> Option<&str> {
        self.ref_name.as_ref().map(|ref_name| {
            ref_name
                .strip_prefix("refs/heads/")
                .or_else(|| ref_name.strip_prefix("refs/remotes/"))
                .unwrap_or(ref_name)
        })
    }

    /// Returns a display name for the worktree, suitable for use in the UI.
    ///
    /// If the worktree is attached to a branch, returns the branch name.
    /// Otherwise, returns the short SHA of the worktree's HEAD commit.
    pub fn display_name(&self) -> &str {
        self.branch_name()
            .unwrap_or(&self.sha[..self.sha.len().min(SHORT_SHA_LENGTH)])
    }

    pub fn directory_name(&self, name_anchor_path: Option<&Path>) -> String {
        if self.is_main {
            return "main worktree".to_string();
        }

        let dir_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(self.display_name());

        if let Some(name_anchor_path) = name_anchor_path {
            let name_anchor_dir = name_anchor_path.file_name().and_then(|name| name.to_str());
            if name_anchor_dir == Some(dir_name) {
                if let Some(parent_name) = self
                    .path
                    .parent()
                    .and_then(|parent| parent.file_name())
                    .and_then(|name| name.to_str())
                {
                    return parent_name.to_string();
                }
            }
        }

        dir_name.to_string()
    }
}

pub fn parse_worktrees_from_str<T: AsRef<str>>(
    raw_worktrees: T,
    main_worktree_path: Option<&Path>,
) -> Vec<Worktree> {
    let mut worktrees = Vec::new();
    let normalized = raw_worktrees.as_ref().replace("\r\n", "\n");
    let entries = normalized.split("\n\n");
    for entry in entries {
        let mut path = None;
        let mut sha = None;
        let mut ref_name = None;

        let mut is_bare = false;

        for line in entry.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("worktree ") {
                path = Some(rest.to_string());
            } else if let Some(rest) = line.strip_prefix("HEAD ") {
                sha = Some(rest.to_string());
            } else if let Some(rest) = line.strip_prefix("branch ") {
                ref_name = Some(rest.to_string());
            } else if line == "bare" {
                is_bare = true;
            }
            // Ignore other lines: detached, locked, prunable, etc.
        }

        if let (Some(path), Some(sha)) = (path, sha) {
            let path = PathBuf::from(path);
            let is_main =
                main_worktree_path.is_some_and(|main_worktree_path| path == main_worktree_path);
            worktrees.push(Worktree {
                path,
                ref_name: ref_name.map(Into::into),
                sha: sha.into(),
                is_main,
                is_bare,
            });
        }
    }

    worktrees
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Upstream {
    pub ref_name: SharedString,
    pub tracking: UpstreamTracking,
}

impl Upstream {
    pub fn is_remote(&self) -> bool {
        self.remote_name().is_some()
    }

    pub fn remote_name(&self) -> Option<&str> {
        self.ref_name
            .strip_prefix("refs/remotes/")
            .and_then(|stripped| stripped.split("/").next())
    }

    pub fn stripped_ref_name(&self) -> Option<&str> {
        self.ref_name.strip_prefix("refs/remotes/")
    }

    pub fn branch_name(&self) -> Option<&str> {
        self.ref_name
            .strip_prefix("refs/remotes/")
            .and_then(|stripped| stripped.split_once('/').map(|(_, name)| name))
    }
}

#[derive(Clone, Copy, Default)]
pub struct CommitOptions {
    pub amend: bool,
    pub signoff: bool,
    pub allow_empty: bool,
    pub no_verify: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum UpstreamTracking {
    /// Remote ref not present in local repository.
    Gone,
    /// Remote ref present in local repository (fetched from remote).
    Tracked(UpstreamTrackingStatus),
}

impl From<UpstreamTrackingStatus> for UpstreamTracking {
    fn from(status: UpstreamTrackingStatus) -> Self {
        UpstreamTracking::Tracked(status)
    }
}

impl UpstreamTracking {
    pub fn is_gone(&self) -> bool {
        matches!(self, UpstreamTracking::Gone)
    }

    pub fn status(&self) -> Option<UpstreamTrackingStatus> {
        match self {
            UpstreamTracking::Gone => None,
            UpstreamTracking::Tracked(status) => Some(*status),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteCommandOutput {
    pub stdout: String,
    pub stderr: String,
}

impl RemoteCommandOutput {
    pub fn is_empty(&self) -> bool {
        self.stdout.is_empty() && self.stderr.is_empty()
    }
}

/// The object type a Git tag ultimately points at (after peeling a chain of
/// annotated tag objects). Used by tag details to distinguish a lightweight tag
/// (points directly at a commit/tree/blob) from an annotated tag (a `tag`
/// object carrying tagger metadata and a message).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TagObjectType {
    Commit,
    Tag,
    Tree,
    Blob,
}

/// Structured metadata about a single Git tag, resolved from its canonical
/// `refs/tags/<name>` ref. Individual fields are parsed from Git's own output
/// (via `git cat-file`), not by splitting on a custom delimiter, so an
/// annotated-tag message containing arbitrary bytes is returned verbatim.
#[derive(Clone, Debug)]
pub struct TagDetails {
    /// The canonical fully-qualified tag ref (`refs/tags/<name>`).
    pub ref_name: SharedString,
    /// The display-shortened tag name.
    pub name: SharedString,
    /// The OID the tag points at — the peeled target object (`^{}`) for an
    /// annotated tag, or the direct target for a lightweight tag.
    pub target_oid: Oid,
    /// The type of the ultimate target object. `TagObjectType::Tag` is only
    /// returned when the tag points at another tag (a nested annotated tag);
    /// ordinary annotated tags peel to `Commit`/`Tree`/`Blob`.
    pub object_type: TagObjectType,
    /// Tagger metadata, present only for annotated tags.
    pub tagger: Option<TagTagger>,
    /// The tag message, present only for annotated tags.
    pub message: Option<String>,
}

/// Author/tagger identity plus signature timestamp from an annotated tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagTagger {
    pub name: SharedString,
    pub email: SharedString,
    pub time: i64,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct UpstreamTrackingStatus {
    pub ahead: u32,
    pub behind: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CommitSummary {
    pub sha: SharedString,
    pub subject: SharedString,
    /// This is a unix timestamp
    pub commit_timestamp: i64,
    pub author_name: SharedString,
    pub has_parent: bool,
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct CommitDetails {
    pub sha: SharedString,
    pub message: SharedString,
    pub commit_timestamp: i64,
    pub author_email: SharedString,
    pub author_name: SharedString,
}

#[derive(Debug)]
pub struct CommitDiff {
    pub files: Vec<CommitFile>,
    pub is_shallow_boundary: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileHistoryChangedFileSets {
    pub file_sets: Vec<Vec<RepoPath>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitFileStatus {
    Added,
    Modified,
    Deleted,
}

#[derive(Debug)]
pub struct CommitFile {
    pub path: RepoPath,
    pub old_content: Option<Vec<u8>>,
    pub new_content: Option<Vec<u8>>,
    pub is_binary: bool,
}

impl CommitFile {
    pub fn status(&self) -> CommitFileStatus {
        match (&self.old_content, &self.new_content) {
            (None, Some(_)) => CommitFileStatus::Added,
            (Some(_), None) => CommitFileStatus::Deleted,
            _ => CommitFileStatus::Modified,
        }
    }
}

impl CommitDetails {
    pub fn short_sha(&self) -> SharedString {
        self.sha[..SHORT_SHA_LENGTH].to_string().into()
    }
}

/// Detects if content is binary by checking for NUL bytes in the first 8000 bytes.
/// This matches git's binary detection heuristic.
pub fn is_binary_content(content: &[u8]) -> bool {
    let check_len = content.len().min(8000);
    content[..check_len].contains(&0)
}

struct LoadedCommitObject {
    content: Vec<u8>,
    is_binary: bool,
}

async fn read_commit_blob<R: smol::io::AsyncBufRead + Unpin>(
    stdout: &mut R,
    info_line: &mut String,
    newline: &mut [u8; 1],
) -> Result<LoadedCommitObject> {
    info_line.clear();
    stdout.read_line(info_line).await?;

    let len = info_line
        .trim_end()
        .parse()
        .with_context(|| format!("invalid object size output from cat-file {info_line}"))?;

    let mut bytes = vec![0; len];
    stdout.read_exact(&mut bytes).await?;
    stdout.read_exact(newline).await?;

    let is_binary = is_binary_content(&bytes);
    Ok(LoadedCommitObject {
        content: bytes,
        is_binary,
    })
}

async fn load_commit_object<R: smol::io::AsyncBufRead + Unpin>(
    object: Option<CommitDiffObject<'_>>,
    stdout: &mut R,
    info_line: &mut String,
    newline: &mut [u8; 1],
) -> Result<Option<LoadedCommitObject>> {
    match object {
        Some(object) if object.kind == CommitDiffObjectKind::Gitlink => {
            Ok(Some(LoadedCommitObject {
                content: format!("Subproject commit {}\n", object.oid).into_bytes(),
                is_binary: false,
            }))
        }
        Some(_) => Ok(Some(read_commit_blob(stdout, info_line, newline).await?)),
        None => Ok(None),
    }
}

async fn read_shallow_file(shallow_file_path: &Path) -> Result<Option<String>> {
    match smol::fs::read_to_string(shallow_file_path).await {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("reading shallow file"),
    }
}

async fn is_shallow_boundary_commit(
    git: &GitBinary,
    shallow_file_path: &Path,
    commit: &str,
) -> Result<bool> {
    let Some(shallow_contents) = read_shallow_file(shallow_file_path).await? else {
        return Ok(false);
    };

    let oid = git
        .run(&["rev-parse", "--verify", &format!("{commit}^{{commit}}")])
        .await
        .context("resolving commit for shallow boundary check")?;
    Ok(shallow_contents
        .lines()
        .any(|line| line.trim() == oid.trim()))
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Remote {
    pub name: SharedString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetMode {
    /// Reset the branch pointer, leave index and worktree unchanged (this will make it look like things that were
    /// committed are now staged).
    Soft,
    /// Reset the branch pointer and index, leave worktree unchanged (this makes it look as though things that were
    /// committed are now unstaged).
    Mixed,
    /// Reset the branch pointer, index, and worktree.
    Hard,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MergeMode {
    #[default]
    Default,
    FastForwardOnly,
    NoFastForward,
    Squash,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTagOptions {
    pub name: String,
    pub target: String,
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GitOperationKind {
    Merge,
    Rebase,
    CherryPick,
    Revert,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GitOperationAction {
    Continue,
    Skip,
    Abort,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum FetchOptions {
    All,
    Unshallow,
    Remote(Remote),
}

impl FetchOptions {
    pub fn to_proto(&self) -> Option<String> {
        match self {
            FetchOptions::All | FetchOptions::Unshallow => None,
            FetchOptions::Remote(remote) => Some(remote.clone().name.into()),
        }
    }

    pub fn from_proto(remote_name: Option<String>, unshallow: bool) -> Self {
        if unshallow {
            return FetchOptions::Unshallow;
        }
        match remote_name {
            Some(name) => FetchOptions::Remote(Remote { name: name.into() }),
            None => FetchOptions::All,
        }
    }

    pub fn name(&self) -> SharedString {
        match self {
            Self::All => "Fetch all remotes".into(),
            Self::Unshallow => "Fetch missing history".into(),
            Self::Remote(remote) => remote.name.clone(),
        }
    }
}

impl std::fmt::Display for FetchOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchOptions::All => write!(f, "--all"),
            FetchOptions::Unshallow => write!(f, "--unshallow"),
            FetchOptions::Remote(remote) => write!(f, "{}", remote.name),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Copy)]
pub enum LogOrder {
    #[default]
    DateOrder,
    TopoOrder,
    AuthorDateOrder,
    ReverseChronological,
}

impl LogOrder {
    pub fn as_arg(&self) -> &'static str {
        match self {
            LogOrder::DateOrder => "--date-order",
            LogOrder::TopoOrder => "--topo-order",
            LogOrder::AuthorDateOrder => "--author-date-order",
            LogOrder::ReverseChronological => "--reverse",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum LogSource {
    #[default]
    All,
    Branch(SharedString),
    Sha(Oid),
    Path(RepoPath),
}

impl LogSource {
    fn get_args(&self) -> Vec<Cow<'_, str>> {
        match self {
            LogSource::All => vec![
                Cow::Borrowed("--ignore-missing"), // needed in case of unborn HEAD
                Cow::Borrowed("--branches"),
                Cow::Borrowed("--remotes"),
                Cow::Borrowed("--tags"),
                Cow::Borrowed("HEAD"),
            ],
            LogSource::Branch(branch) => vec![Cow::Borrowed(branch.as_str())],
            LogSource::Sha(oid) => vec![Cow::Owned(oid.to_string())],
            LogSource::Path(path) => vec![
                Cow::Borrowed("--follow"),
                Cow::Borrowed("--"),
                Cow::Borrowed(path.as_unix_str()),
            ],
        }
    }
}

pub struct SearchCommitArgs {
    pub query: SharedString,
    pub case_sensitive: bool,
}

pub fn commit_hash_search_query(query: &str) -> Option<&str> {
    let query = query.trim();
    (7..=40)
        .contains(&query.len())
        .then_some(query)
        .filter(|query| query.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

pub fn delete_branch_flag(is_remote_tracking_ref: bool, force: bool) -> &'static str {
    match (is_remote_tracking_ref, force) {
        (true, true) => "-Dr",
        (true, false) => "-dr",
        (false, true) => "-D",
        (false, false) => "-d",
    }
}

pub trait GitRepository: Send + Sync {
    /// Returns the contents of an entry in the repository's index, or None if there is no entry for the given path.
    ///
    /// Also returns `None` for symlinks.
    fn load_index_text(&self, path: RepoPath) -> BoxFuture<'_, Option<Vec<u8>>> {
        let future = self.load_revisions(vec![format!(":{}", path.as_unix_str())]);
        async move { future.await.ok()?.pop()? }.boxed()
    }

    /// Returns the contents of an entry in the repository's HEAD, or None if HEAD does not exist or has no entry for the given path.
    ///
    /// Also returns `None` for symlinks.
    fn load_committed_text(&self, path: RepoPath) -> BoxFuture<'_, Option<Vec<u8>>> {
        let future = self.load_revisions(vec![format!("HEAD:{}", path.as_unix_str())]);
        async move { future.await.ok()?.pop()? }.boxed()
    }
    fn load_blob_content(&self, oid: Oid) -> BoxFuture<'_, Result<Vec<u8>>>;

    fn set_index_text(
        &self,
        path: RepoPath,
        content: Option<Vec<u8>>,
        env: Arc<HashMap<String, String>>,
        is_executable: bool,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Returns the URL of the remote with the given name.
    fn remote_url(&self, name: &str) -> BoxFuture<'_, Option<String>> {
        let name = name.to_string();
        let fut = self.remote_urls();
        async move { fut.await.remove(&name) }.boxed()
    }

    /// Returns the URL of all remotes.
    fn remote_urls(&self) -> BoxFuture<'_, HashMap<String, String>>;

    /// Resolve a list of refs to SHAs.
    fn revparse_batch(&self, revs: Vec<String>) -> BoxFuture<'_, Result<Vec<Option<String>>>>;

    fn load_revisions(&self, revisions: Vec<String>)
    -> BoxFuture<'_, Result<Vec<Option<Vec<u8>>>>>;

    fn head_sha(&self) -> BoxFuture<'_, Option<String>> {
        async move {
            self.revparse_batch(vec!["HEAD".into()])
                .await
                .unwrap_or_default()
                .into_iter()
                .next()
                .flatten()
        }
        .boxed()
    }

    fn merge_message(&self) -> BoxFuture<'_, Option<String>>;

    fn status(&self, path_prefixes: &[RepoPath]) -> Task<Result<GitStatus>>;
    fn diff_tree(&self, request: DiffTreeType) -> BoxFuture<'_, Result<TreeDiff>>;

    fn stash_entries(&self) -> BoxFuture<'static, Result<GitStash>>;

    fn check_access(&self) -> BoxFuture<'_, Result<()>> {
        async move { Ok(()) }.boxed()
    }

    fn branches(&self) -> BoxFuture<'_, Result<BranchesScanResult>>;

    fn change_branch(&self, name: String) -> BoxFuture<'_, Result<()>>;
    fn create_branch(&self, name: String, base_branch: Option<String>)
    -> BoxFuture<'_, Result<()>>;
    fn rename_branch(&self, branch: String, new_name: String) -> BoxFuture<'_, Result<()>>;

    fn delete_branch(
        &self,
        is_remote: bool,
        name: String,
        force: bool,
    ) -> BoxFuture<'_, Result<()>>;

    fn worktrees(&self) -> BoxFuture<'_, Result<Vec<Worktree>>>;

    /// Returns the creation time of a linked worktree's git metadata
    /// directory (`.git/worktrees/<name>/`), resolved via the worktree's
    /// `.git` file.
    ///
    /// The metadata directory is created by `git worktree add` and removed
    /// by `git worktree remove`, so its creation time identifies a
    /// particular incarnation of the worktree: if the worktree is removed
    /// and recreated at the same path, the creation time changes.
    ///
    /// Returns `Ok(None)` when the worktree directory does not exist at
    /// all, and an error when the directory exists but the time cannot be
    /// determined (e.g. on filesystems without birthtime support); callers
    /// should fail safe in the error case.
    fn worktree_created_at(
        &self,
        worktree_path: PathBuf,
    ) -> BoxFuture<'_, Result<Option<SystemTime>>>;

    fn create_worktree(
        &self,
        target: CreateWorktreeTarget,
        path: PathBuf,
    ) -> BoxFuture<'_, Result<()>>;

    fn checkout_branch_in_worktree(
        &self,
        branch_name: String,
        worktree_path: PathBuf,
        create: bool,
    ) -> BoxFuture<'_, Result<()>>;

    fn remove_worktree(&self, path: PathBuf, force: bool) -> BoxFuture<'_, Result<()>>;

    fn rename_worktree(&self, old_path: PathBuf, new_path: PathBuf) -> BoxFuture<'_, Result<()>>;

    fn reset(
        &self,
        commit: String,
        mode: ResetMode,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn checkout_commit(
        &self,
        commit: String,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn create_tag(
        &self,
        options: CreateTagOptions,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn cherry_pick(
        &self,
        commits: Vec<String>,
        no_commit: bool,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn revert(
        &self,
        commit: String,
        no_commit: bool,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn merge(
        &self,
        commit: String,
        mode: MergeMode,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn operation_state(&self) -> BoxFuture<'_, Result<Option<GitOperationKind>>>;

    fn run_operation_action(
        &self,
        operation: GitOperationKind,
        action: GitOperationAction,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn checkout_files(
        &self,
        commit: String,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn show(&self, commit: String) -> BoxFuture<'_, Result<CommitDetails>>;

    fn load_commit(
        &self,
        commit: String,
        ignore_shallow_boundary: bool,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<CommitDiff>>;

    fn load_commit_range(
        &self,
        base: String,
        target: String,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<CommitDiff>>;
    fn blame(
        &self,
        path: RepoPath,
        content: Rope,
        line_ending: LineEnding,
    ) -> BoxFuture<'_, Result<crate::blame::Blame>>;

    fn blame_at_revision(
        &self,
        path: RepoPath,
        revision: Oid,
    ) -> BoxFuture<'_, Result<crate::blame::Blame>>;

    /// Returns the absolute path to the repository. For worktrees, this will be the path to the
    /// worktree's gitdir within the main repository (typically `.git/worktrees/<name>`).
    fn path(&self) -> PathBuf;

    fn main_repository_path(&self) -> PathBuf;

    /// Updates the index to match the worktree at the given paths.
    ///
    /// If any of the paths have been deleted from the worktree, they will be removed from the index if found there.
    fn stage_paths(
        &self,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;
    /// Updates the index to match HEAD at the given paths.
    ///
    /// If any of the paths were previously staged but do not exist in HEAD, they will be removed from the index.
    fn unstage_paths(
        &self,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    /// Only used to serve `proto::RunGitHook` requests from older remote clients;
    /// new code lets `git commit` run hooks itself.
    ///
    /// TODO: remove together with `proto::RunGitHook` (see the deprecation note in git.proto).
    fn run_hook(
        &self,
        hook: RunHook,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn commit(
        &self,
        message: SharedString,
        name_and_email: Option<(SharedString, SharedString)>,
        options: CommitOptions,
        askpass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn stash_paths(
        &self,
        paths: Vec<RepoPath>,
        message: Option<String>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn stash_staged(
        &self,
        message: Option<String>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn stash_pop(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<StashMutationResult>>;

    fn stash_apply(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    fn stash_drop(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>>;

    /// Rename only the selected stash entry's display message without moving it
    /// to the top, preserving every other stash OID and non-target observable
    /// reflog field. Crash-recoverable: before the destructive replay this
    /// writes a versioned recovery manifest blob plus stable recovery refs for
    /// every involved OID in one atomic ref transaction, rebuilds the stash
    /// reflog oldest-to-newest through Git's ref backend, verifies the complete
    /// observable result, and only then deletes the recovery refs. Any
    /// destructive-boundary failure retains the manifest + recovery refs and
    /// reports them plus the observed stack via `StashRenameResult`.
    fn stash_rename(
        &self,
        identity: Option<StashIdentity>,
        message: String,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<StashRenameResult>>;

    /// Discover unfinished crash-recoverable stash renames left by a previous
    /// run (any recovery ref under `STASH_RENAME_RECOVERY_PREFIX`), reading the
    /// versioned manifest blob and the current observable stack so a caller can
    /// offer retry/recover/cleanup guidance after a restart.
    fn pending_stash_rename_recovers(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StashRenameRecovery>>>;

    fn push(
        &self,
        branch_name: String,
        remote_branch_name: String,
        upstream_name: String,
        options: Option<PushOptions>,
        askpass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        // This method takes an AsyncApp to ensure it's invoked on the main thread,
        // otherwise git-credentials-manager won't work.
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>>;

    fn pull(
        &self,
        branch_name: Option<String>,
        upstream_name: String,
        rebase: bool,
        askpass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        // This method takes an AsyncApp to ensure it's invoked on the main thread,
        // otherwise git-credentials-manager won't work.
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>>;

    fn fetch(
        &self,
        fetch_options: FetchOptions,
        askpass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        // This method takes an AsyncApp to ensure it's invoked on the main thread,
        // otherwise git-credentials-manager won't work.
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>>;

    fn get_push_remote(&self, branch: String) -> BoxFuture<'_, Result<Option<Remote>>>;

    fn get_branch_remote(&self, branch: String) -> BoxFuture<'_, Result<Option<Remote>>>;

    fn get_all_remotes(&self) -> BoxFuture<'_, Result<Vec<Remote>>>;

    fn remove_remote(&self, name: String) -> BoxFuture<'_, Result<()>>;

    fn create_remote(&self, name: String, url: String) -> BoxFuture<'_, Result<()>>;

    /// returns a list of remote branches that contain HEAD
    fn check_for_pushed_commit(&self) -> BoxFuture<'_, Result<Vec<SharedString>>>;

    /// Run git diff
    fn diff(&self, diff: DiffType) -> BoxFuture<'_, Result<String>>;

    /// Unified-text diff between `base` and `target` using three-dot semantics
    /// (`git diff base...target`): the cumulative changes reachable from
    /// `target` but not `base`, i.e. everything the target branch introduced
    /// since it diverged from `base`. Powers the git graph's "branch combined
    /// diff" view.
    fn diff_commits(
        &self,
        base: String,
        target: String,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>>;

    /// Unified-text diff of a single tracked working-tree path against the
    /// given HEAD oid (`git diff <head_oid> -- <path>`), covering both staged
    /// and unstaged changes for that path. Untracked paths yield an empty diff
    /// (they live only on disk, not in the index), so callers fall back to
    /// [`Self::load_worktree_path`] for those.
    fn diff_worktree_path(
        &self,
        head_oid: String,
        path: RepoPath,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>>;

    /// Raw text of an untracked worktree file at `path`, read from disk. Used
    /// to synthesize a new-file diff for paths git does not track.
    fn load_worktree_path(
        &self,
        path: RepoPath,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>>;

    fn diff_stat(
        &self,
        diff: DiffStatType,
        path_prefixes: &[RepoPath],
    ) -> BoxFuture<'static, Result<crate::status::GitDiffStat>>;

    /// Creates a checkpoint for the repository.
    fn checkpoint(&self) -> BoxFuture<'static, Result<GitRepositoryCheckpoint>>;

    /// Resets to a previously-created checkpoint.
    fn restore_checkpoint(&self, checkpoint: GitRepositoryCheckpoint) -> BoxFuture<'_, Result<()>>;

    /// Creates two detached commits capturing the current staged and unstaged
    /// state without moving any branch. Returns (staged_sha, unstaged_sha).
    fn create_archive_checkpoint(&self) -> BoxFuture<'_, Result<(String, String)>>;

    /// Restores the working directory and index from archive checkpoint SHAs.
    /// Assumes HEAD is already at the correct commit (original_commit_hash).
    /// Restores the index to match staged_sha's tree, and the working
    /// directory to match unstaged_sha's tree.
    fn restore_archive_checkpoint(
        &self,
        staged_sha: String,
        unstaged_sha: String,
    ) -> BoxFuture<'_, Result<()>>;

    /// Compares two checkpoints, returning true if they are equal
    fn compare_checkpoints(
        &self,
        left: GitRepositoryCheckpoint,
        right: GitRepositoryCheckpoint,
    ) -> BoxFuture<'_, Result<bool>>;

    /// Computes a diff between two checkpoints.
    fn diff_checkpoints(
        &self,
        base_checkpoint: GitRepositoryCheckpoint,
        target_checkpoint: GitRepositoryCheckpoint,
    ) -> BoxFuture<'_, Result<String>>;

    fn load_commit_template(&self) -> BoxFuture<'_, Result<Option<GitCommitTemplate>>>;

    fn default_branch(
        &self,
        include_remote_name: bool,
    ) -> BoxFuture<'_, Result<Option<SharedString>>>;

    /// Runs `git rev-list --parents` to get the commit graph structure.
    /// Returns commit SHAs and their parent SHAs for building the graph visualization.
    fn initial_graph_data(
        &self,
        log_source: LogSource,
        log_order: LogOrder,
        request_tx: Sender<Vec<Arc<InitialGraphCommitData>>>,
    ) -> BoxFuture<'_, Result<()>>;

    /// Enumerates the current `refs/stash` reflog as graph rows, one per stash
    /// entry, using an explicit revision set (`refs/stash`) and reflog selectors
    /// (`%gD`) that exclude normal refs and are correct for both SHA-1 and
    /// SHA-256. Each row keeps only the stash commit's first parent (its base)
    /// so the graph connects the stash row to the base without pulling
    /// unrelated stash-only ancestors, and carries `refs/stash@{N}` as its
    /// distinct row identity.
    fn stash_graph_data(
        &self,
    ) -> BoxFuture<'_, Result<Vec<Arc<InitialGraphCommitData>>>>;

    /// Fetches a single commit as one graph row (parents + decorations). Used to
    /// surface an unreachable stash base as exactly one supplemental row rather
    /// than pulling unrelated stash-only ancestors. Returns `None` when the
    /// commit does not exist or is not a commit.
    fn graph_commit_for_base(
        &self,
        sha: Oid,
    ) -> BoxFuture<'_, Result<Option<Arc<InitialGraphCommitData>>>>;

    fn search_commits(
        &self,
        log_source: LogSource,
        search_args: SearchCommitArgs,
        request_tx: Sender<Oid>,
    ) -> BoxFuture<'_, Result<()>>;

    fn file_history_changed_files(
        &self,
        paths: Vec<RepoPath>,
        commit_limit: usize,
    ) -> BoxFuture<'_, Result<Vec<FileHistoryChangedFileSets>>>;

    fn commit_data_reader(&self) -> Result<CommitDataReader>;

    fn update_ref(&self, ref_name: String, commit: String) -> BoxFuture<'_, Result<()>>;

    fn delete_ref(&self, ref_name: String) -> BoxFuture<'_, Result<()>>;

    fn repair_worktrees(&self) -> BoxFuture<'_, Result<()>>;

    /// Deletes the given fully-qualified refs on the given remote by issuing an
    /// explicit remote push deletion (`git push <remote> --delete <ref>…`).
    /// This mutates the remote's refs on the server — it never touches the
    /// local remote-tracking namespace by itself. Each ref is passed as its own
    /// argument so bridges/tag names containing unusual characters are never
    /// interpreted by a shell.
    fn delete_refs_on_remote(
        &self,
        remote_name: String,
        refs: Vec<String>,
        askpass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>>;

    /// Deletes the local tag `name` (`git tag -d <name>`). Only deletes a tag
    /// ref in `refs/tags/`; never a branch.
    fn delete_tag(&self, name: String) -> BoxFuture<'_, Result<()>>;

    /// Reads structured metadata about the tag at the canonical
    /// `refs/tags/<name>` ref, distinguishing lightweight from annotated tags
    /// via `git cat-file` (not a custom delimiter).
    fn tag_details(&self, ref_name: String) -> BoxFuture<'_, Result<TagDetails>>;

    fn set_trusted(&self, trusted: bool);
    fn is_trusted(&self) -> bool;
}

pub enum DiffType {
    HeadToIndex,
    HeadToWorktree,
    MergeBase { base_ref: SharedString },
}

#[derive(Clone, Copy)]
pub enum DiffStatType {
    HeadToIndex,
    HeadToWorktree,
    IndexToWorktree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
pub enum PushOptions {
    SetUpstream,
    Force,
}

impl std::fmt::Debug for dyn GitRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("dyn GitRepository<...>").finish()
    }
}

pub struct RealGitRepository {
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    /// `None` only for bare repositories, which do not have a working directory.
    pub working_directory: Option<PathBuf>,
    pub system_git_binary_path: Option<PathBuf>,
    pub any_git_binary_path: PathBuf,
    any_git_binary_help_output: Arc<Mutex<Option<SharedString>>>,
    executor: BackgroundExecutor,
    is_trusted: Arc<AtomicBool>,
    /// Test-only fault-injection hook for crash-recoverable stash rename
    /// boundaries. Production paths never install one (`None`), so this is a
    /// zero-cost no-op there. Field lives here (not a global) so parallel tests
    /// cannot interfere with one another.
    stash_rename_fault: Arc<
        Mutex<Option<Box<dyn Fn(StashRenameBoundary) -> Result<()> + Send>>>,
    >,
}

#[derive(Debug)]
pub enum RefEdit {
    Update { ref_name: String, commit: String },
    Delete { ref_name: String },
}

impl RefEdit {
    fn into_args(self) -> Vec<OsString> {
        match self {
            Self::Update { ref_name, commit } => {
                vec!["update-ref".into(), ref_name.into(), commit.into()]
            }
            Self::Delete { ref_name } => {
                vec!["update-ref".into(), "-d".into(), ref_name.into()]
            }
        }
    }
}

impl RealGitRepository {
    pub fn new(
        dotgit_path: &Path,
        bundled_git_binary_path: Option<PathBuf>,
        system_git_binary_path: Option<PathBuf>,
        executor: BackgroundExecutor,
    ) -> Result<Self> {
        let any_git_binary_path = system_git_binary_path
            .clone()
            .or(bundled_git_binary_path)
            .context("no git binary available")?;
        log::info!(
            "opening git repository at {dotgit_path:?} using git binary {any_git_binary_path:?}"
        );
        let dotgit_parent = dotgit_path.parent().context(".git has no parent")?;
        let has_working_directory =
            dotgit_path.is_file() || dotgit_path.file_name() == Some(OsStr::new(".git"));
        let working_directory = if has_working_directory {
            Some(normalize_git_metadata_path(dotgit_parent.to_path_buf())?)
        } else {
            None
        };

        let git_dir = if dotgit_path.is_file() {
            let content =
                std::fs::read_to_string(dotgit_path).context("reading .git worktree file")?;
            let path_str = content
                .strip_prefix("gitdir: ")
                .context("expected .git file to start with 'gitdir: '")?
                .trim();
            let resolved = PathBuf::from(path_str);
            let resolved = if resolved.is_absolute() {
                resolved
            } else {
                dotgit_parent.join(resolved)
            };
            normalize_git_metadata_path(resolved)?
        } else {
            normalize_git_metadata_path(dotgit_path.to_path_buf())?
        };

        let common_dir = {
            let commondir_file = git_dir.join("commondir");
            if commondir_file.is_file() {
                let content =
                    std::fs::read_to_string(&commondir_file).context("reading commondir file")?;
                let path_str = content.trim();
                let resolved = PathBuf::from(path_str);
                let resolved = if resolved.is_absolute() {
                    resolved
                } else {
                    git_dir.join(resolved)
                };
                normalize_git_metadata_path(resolved)?
            } else {
                git_dir.clone()
            }
        };

        Ok(Self {
            git_dir,
            common_dir,
            working_directory,
            system_git_binary_path,
            any_git_binary_path,
            executor,
            any_git_binary_help_output: Arc::new(Mutex::new(None)),
            is_trusted: Arc::new(AtomicBool::new(false)),
            stash_rename_fault: Arc::new(Mutex::new(None)),
        })
    }

    fn working_directory(&self) -> Result<PathBuf> {
        self.working_directory
            .clone()
            .context("bare repositories do not have a working directory")
    }

    fn command_directory(&self) -> PathBuf {
        self.working_directory
            .clone()
            .unwrap_or_else(|| self.git_dir.clone())
    }

    fn git_binary_in_worktree(&self) -> Result<GitBinary> {
        Ok(GitBinary::new(
            self.any_git_binary_path.clone(),
            self.working_directory()?,
            self.path(),
            self.executor.clone(),
            self.is_trusted(),
        ))
    }

    fn git_binary(&self) -> GitBinary {
        GitBinary::new(
            self.any_git_binary_path.clone(),
            self.command_directory(),
            self.path(),
            self.executor.clone(),
            self.is_trusted(),
        )
    }

    fn edit_ref(&self, edit: RefEdit) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary();
        self.executor
            .spawn(async move {
                let git = git_binary;
                let args = edit.into_args();
                git.run(&args).await?;
                Ok(())
            })
            .boxed()
    }

    async fn any_git_binary_help_output(&self) -> SharedString {
        if let Some(output) = self.any_git_binary_help_output.lock().clone() {
            return output;
        }
        let git = self.git_binary();
        let output: SharedString = self
            .executor
            .spawn(async move { git.run(&["help", "-a"]).await })
            .await
            .unwrap_or_default()
            .into();
        *self.any_git_binary_help_output.lock() = Some(output.clone());
        output
    }
}

#[derive(Clone, Debug)]
pub struct GitRepositoryCheckpoint {
    pub commit_sha: Oid,
}

#[derive(Debug)]
pub struct GitCommitter {
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Clone, Debug)]
pub struct GitCommitTemplate {
    pub template: String,
}

pub async fn get_git_committer(cx: &AsyncApp) -> GitCommitter {
    if cfg!(any(feature = "test-support", test)) {
        return GitCommitter {
            name: None,
            email: None,
        };
    }

    let git_binary_path =
        if cfg!(target_os = "macos") && option_env!("ZED_BUNDLE").as_deref() == Some("true") {
            cx.update(|cx| {
                cx.path_for_auxiliary_executable("git")
                    .context("could not find git binary path")
                    .log_err()
            })
        } else {
            None
        };

    let git = GitBinary::new(
        git_binary_path.unwrap_or(PathBuf::from("git")),
        paths::home_dir().clone(),
        paths::home_dir().join(".git"),
        cx.background_executor().clone(),
        true,
    );

    cx.background_spawn(async move {
        let name = git
            .run(&["config", "--global", "user.name"])
            .await
            .log_err();
        let email = git
            .run(&["config", "--global", "user.email"])
            .await
            .log_err();
        GitCommitter { name, email }
    })
    .await
}

fn parse_remote_urls(stdout: &str) -> HashMap<String, String> {
    let mut urls = HashMap::default();
    for line in stdout.lines() {
        if let Some((line, suffix)) = line.rsplit_once(" (fetch)")
            && (suffix.is_empty() || suffix.starts_with(" [") && suffix.ends_with(']'))
            && let Some((name, url)) = line.split_once(char::is_whitespace)
        {
            urls.insert(name.to_string(), url.trim_start().to_string());
        }
    }
    urls
async fn resolve_commit_oid(git: &GitBinary, commit: &str) -> Result<String> {
    let commit = format!("{commit}^{{commit}}");
    git.run(&["rev-parse", "--verify", "--end-of-options", &commit])
        .await
}

async fn run_git_mutation<S>(
    git: &GitBinary,
    args: &[S],
    env: &HashMap<String, String>,
) -> Result<()>
where
    S: AsRef<OsStr>,
{
    let output = git.build_command(args).envs(env.iter()).output().await?;
    anyhow::ensure!(
        output.status.success(),
        GitBinaryCommandError {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            status: output.status,
        }
    );
    Ok(())
}

/// Build the explicit, fully-qualified reflog selector for a stash index.
fn stash_selector_for_index(index: usize) -> String {
    format!("{STASH_REF}@{{{index}}}")
}

/// Run `git stash list` against the current reflog (`refs/stash`).
async fn stash_entries_for(git: &GitBinary) -> Result<Vec<StashEntry>> {
    let output = git
        .build_command(&["stash", "list", "--pretty=format:%gd%x00%H%x00%ct%x00%s"])
        .output()
        .await?;
    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stash: GitStash = stdout.parse()?;
        Ok(stash.entries.to_vec())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git stash list failed: {stderr}");
    }
}

/// Refresh `refs/stash` and resolve exactly one target entry for a mutation.
/// `None` selects the current top of stack (`stash@{0}`), resolved fresh.
async fn resolve_stash_target(
    git: &GitBinary,
    identity: Option<&StashIdentity>,
) -> Result<StashEntry> {
    let entries = stash_entries_for(git).await?;
    match identity {
        Some(identity) => resolve_stash_identity(&entries, identity)
            .cloned()
            .map_err(|err| anyhow!("{err}")),
        None => entries
            .into_iter()
            .next()
            .with_context(|| "Expected a stash entry to operate on"),
    }
}

/// A stash entry as captured for a rename, carrying the raw subject and reflog
/// subject lines so a rebuild can reproduce every non-target observable reflog
/// field byte-for-byte.
#[derive(Clone, Debug)]
struct CapturedStash {
    entry: StashEntry,
    /// The commit subject (`%s`) — the primary display message.
    raw_subject: String,
    /// The reflog subject (`%gs`) — `git stash list` shows this field too.
    reflog_message: String,
}

impl CapturedStash {
    /// Deterministic recovery-ref key for this entry: its position in the
    /// newest-first captured stack.
    fn ref_key(&self) -> String {
        self.entry.index.to_string()
    }

    /// Audit representation written into the recovery manifest.
    fn manifest_line(&self) -> String {
        format!(
            "{}\0{}\0{}\0{}\0{}",
            self.entry.index,
            self.entry.oid,
            self.entry.timestamp,
            self.entry.message,
            self.entry.branch.as_deref().unwrap_or("")
        )
    }

    fn from_manifest_line(line: &str) -> Result<Self> {
        let parts: Vec<&str> = line.splitn(5, '\0').collect();
        if parts.len() != 5 {
            anyhow::bail!("invalid captured stash manifest line: {line:?}");
        }
        let index = parts[0].parse::<usize>()?;
        let oid = Oid::from_str(parts[1])?;
        let timestamp = parts[2].parse::<i64>()?;
        let message = parts[3].to_string();
        let branch = (!parts[4].is_empty()).then(|| parts[4].to_string());
        let raw_subject = match (&branch, message.as_str()) {
            (Some(branch), _) => format!("On {branch}: {message}"),
            (None, message) => message.to_string(),
        };
        Ok(Self {
            entry: StashEntry {
                index,
                oid,
                message,
                branch,
                timestamp,
            },
            raw_subject: raw_subject.clone(),
            reflog_message: raw_subject,
        })
    }
}

/// Versioned manifest a stash rename writes before its destructive replay.
#[derive(Clone, Serialize, Deserialize)]
struct StashRenameManifest {
    version: u32,
    manifest_id: String,
    new_message: String,
    /// Hex OID of the rewritten target commit.
    new_oid: String,
    /// Reflog selector of the exact selected target (e.g. `refs/stash@{2}`).
    target_selector: String,
    /// Captured stack newest-first as audit lines (see `CapturedStash`).
    captured_lines: Vec<String>,
}

/// Deterministic manifest ref for a rename id.
fn stash_rename_manifest_ref(manifest_id: &str) -> String {
    format!("{STASH_RENAME_RECOVERY_PREFIX}/{manifest_id}/manifest")
}

/// Deterministic recovery ref protecting one involved OID.
fn stash_rename_entry_ref(manifest_id: &str, key: &str) -> String {
    format!("{STASH_RENAME_RECOVERY_PREFIX}/{manifest_id}/entry/{key}")
}

/// The recovery refs protecting every involved OID (each captured entry,
/// newest-first, plus the rewritten target OID), deterministically named so a
/// later failure and a post-restart discovery report the same refs.
fn stash_rename_recovery_refs(manifest_id: &str, captured: &[CapturedStash]) -> Vec<String> {
    let mut refs = Vec::with_capacity(captured.len() + 1);
    for entry in captured {
        refs.push(stash_rename_entry_ref(manifest_id, &entry.ref_key()));
    }
    refs.push(stash_rename_entry_ref(manifest_id, "target"));
    refs
}

/// Capture the current stash stack newest-first, keeping the raw subject and
/// reflog subject lines so a rebuild can reproduce non-target fields exactly.
async fn capture_stash_stack(git: &GitBinary) -> Result<Vec<CapturedStash>> {
    let output = git
        .build_command(&[
            "stash",
            "list",
            "--pretty=format:%gd%x00%H%x00%ct%x00%s%x00%gs",
        ])
        .output()
        .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git stash list failed: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut captured = Vec::new();
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(5, '\0').collect();
        if parts.len() != 5 {
            anyhow::bail!("unexpected stash list line: {line:?}");
        }
        let index = parse_stash_index(parts[0])?;
        let oid = Oid::from_str(parts[1])?;
        let timestamp = parts[2].parse::<i64>()?;
        let raw_subject = parts[3].to_string();
        let reflog_message = parts[4].to_string();
        let (branch, message) = parse_stash_message(&raw_subject);
        captured.push(CapturedStash {
            entry: StashEntry {
                index,
                oid,
                message: message.to_string(),
                branch: branch.map(Into::into),
                timestamp,
            },
            raw_subject,
            reflog_message,
        });
    }
    Ok(captured)
}

/// Write bytes as a repository blob (`git hash-object -w`) and return its OID.
async fn write_git_blob(
    git: &GitBinary,
    content: &str,
    env: &Arc<HashMap<String, String>>,
) -> Result<Oid> {
    let mut child = git
        .build_command(&["hash-object", "-w", "--stdin"])
        .envs(env.iter())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning git hash-object")?;
    let mut stdin = child.stdin.take().context("hash-object has no stdin")?;
    stdin.write_all(content.as_bytes()).await?;
    stdin.flush().await?;
    drop(stdin);
    let output = child.output().await?;
    anyhow::ensure!(
        output.status.success(),
        "writing recovery manifest blob failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sha = str::from_utf8(&output.stdout)?.trim();
    Oid::from_str(sha).context("invalid manifest blob OID")
}

/// Run an atomic `git update-ref --stdin` transaction: `start`, the given
/// directives, `prepare`, then `commit`. Any failing directive or `prepare`
/// failure aborts the whole transaction (nothing partially applied).
async fn run_ref_transaction(git: &GitBinary, lines: &[String]) -> Result<()> {
    let mut input = String::from("start\n");
    for line in lines {
        input.push_str(line);
        input.push('\n');
    }
    input.push_str("prepare\n");
    input.push_str("commit\n");

    let mut child = git
        .build_command(&["update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning git update-ref --stdin")?;
    let mut stdin = child.stdin.take().context("git update-ref has no stdin")?;
    stdin.write_all(input.as_bytes()).await?;
    stdin.flush().await?;
    drop(stdin);
    let output = child.output().await?;
    anyhow::ensure!(
        output.status.success(),
        "git ref transaction failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// Probe that Git supports atomic multi-ref transactions (required to write the
/// recovery refs + manifest in one all-or-nothing transaction before any
/// destructive work). Runs a create+delete of a scratch ref in one transaction
/// and cleans it up; unsupported Git stops the feature here.
async fn ensure_ref_transaction_supported(git: &GitBinary) -> Result<()> {
    let tip = git
        .run(&["rev-parse", STASH_REF])
        .await
        .context("resolving refs/stash for capability probe")?;
    let probe_a = format!(
        "{STASH_RENAME_RECOVERY_PREFIX}/probe/{}",
        Uuid::new_v4().simple()
    );
    let probe_b = format!(
        "{STASH_RENAME_RECOVERY_PREFIX}/probe/{}",
        Uuid::new_v4().simple()
    );
    // Verify that Git can apply multiple ref updates atomically (the rename
    // writes the manifest ref + one recovery ref per OID in a single
    // transaction). Two distinct creates then two distinct deletes; unsupported
    // Git stops the feature here, before any destructive work.
    run_ref_transaction(git, &[format!("create {probe_a} {tip}"), format!("create {probe_b} {tip}")])
        .await
        .with_context(|| {
            "this git does not support atomic ref transactions, required for crash-recoverable stash rename"
        })?;
    run_ref_transaction(git, &[format!("delete {probe_a}"), format!("delete {probe_b}")])
        .await
        .context("cleaning up capability probe refs")
}

/// Rewrite a stash commit to carry a new subject while preserving its tree,
/// parents, and author/committer identity and timestamps, returning the new OID.
async fn rewrite_stash_commit(
    git: &GitBinary,
    original: Oid,
    new_subject: &str,
    env: &Arc<HashMap<String, String>>,
) -> Result<Oid> {
    let raw = git
        .run(&["cat-file", "commit", &original.to_string()])
        .await
        .with_context(|| format!("reading stash commit {original}"))?;
    let headers = raw.split_once("\n\n").map(|(h, _)| h).unwrap_or(&raw);

    let mut tree: Option<&str> = None;
    let mut parents: Vec<String> = Vec::new();
    let mut author: Option<&str> = None;
    let mut committer: Option<&str> = None;
    for line in headers.lines() {
        if let Some(value) = line.strip_prefix("tree ") {
            tree = Some(value);
        } else if let Some(value) = line.strip_prefix("parent ") {
            parents.push(value.to_string());
        } else if let Some(value) = line.strip_prefix("author ") {
            author = Some(value);
        } else if let Some(value) = line.strip_prefix("committer ") {
            committer = Some(value);
        }
    }

    let tree = tree.context("stash commit missing tree header")?;
    let (author_name, author_email, author_date) =
        parse_identity_line(author.context("stash commit missing author")?)?;
    let (committer_name, committer_email, committer_date) =
        parse_identity_line(committer.context("stash commit missing committer")?)?;

    let mut args = vec!["commit-tree".to_string(), tree.to_string()];
    for parent in &parents {
        args.push("-p".to_string());
        args.push(parent.clone());
    }
    args.push("-m".to_string());
    args.push(new_subject.to_string());

    let output = git
        .build_command(&args)
        .envs(env.iter())
        .env("GIT_AUTHOR_NAME", &author_name)
        .env("GIT_AUTHOR_EMAIL", &author_email)
        .env("GIT_AUTHOR_DATE", &author_date)
        .env("GIT_COMMITTER_NAME", &committer_name)
        .env("GIT_COMMITTER_EMAIL", &committer_email)
        .env("GIT_COMMITTER_DATE", &committer_date)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    anyhow::ensure!(
        output.status.success(),
        "rewriting stash commit failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let sha = str::from_utf8(&output.stdout)?.trim();
    Oid::from_str(sha).context("invalid rewritten stash commit OID")
}

/// Parse a `name <email> timestamp tz` identity line into its name, email, and
/// raw date (preserved verbatim so git reproduces the original commit time).
fn parse_identity_line(line: &str) -> Result<(String, String, String)> {
    let lt = line.find('<').context("identity line missing '<'")?;
    let gt = line.find('>').context("identity line missing '>'")?;
    let name = line[..lt].trim().to_string();
    let email = line[lt + 1..gt].to_string();
    let date = line[gt + 1..].trim().to_string();
    Ok((name, email, date))
}

/// A planned rebuilt stash entry: the observable `StashEntry` plus the raw
/// subject used to reproduce its reflog message.
#[derive(Clone, Debug)]
struct PlannedStash {
    entry: StashEntry,
    /// Reflog subject reproduced when this entry is rewritten into the rebuilt
    /// reflog, so every non-target observable reflog field is preserved
    /// byte-for-byte and the target carries its new subject.
    reflog_message: String,
}

/// Compare two observable stacks field-by-field (oid, message, branch,
/// timestamp) for the fields `git stash list` surfaces.
fn observable_stacks_match(actual: &[StashEntry], expected: &[StashEntry]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| {
                actual.oid == expected.oid
                    && actual.message == expected.message
                    && actual.branch == expected.branch
                    && actual.timestamp == expected.timestamp
            })
}

/// Build recovery metadata for a failed rename by reading the current stack.
async fn stash_recovery_for(
    git: &GitBinary,
    manifest_ref: &str,
    recovery_refs: &[String],
    rename_applied: bool,
) -> Result<StashRenameRecovery> {
    let observed_entries = stash_entries_for(git).await?;
    Ok(StashRenameRecovery {
        manifest_ref: manifest_ref.to_string(),
        recovery_refs: recovery_refs.to_vec(),
        observed_entries,
        rename_applied,
    })
}

/// Read a recovery manifest blob referenced by `manifest_ref`.
async fn read_rename_manifest(git: &GitBinary, manifest_ref: &str) -> Result<StashRenameManifest> {
    let content = git.run(&["cat-file", "blob", manifest_ref]).await?;
    serde_json::from_str(&content)
        .with_context(|| format!("invalid stash rename manifest at {manifest_ref}"))
}

/// Whether the observed stack reflects an applied rename described by the
/// manifest: it matches the captured stack except the selected target, whose
/// entry now carries the rewritten OID and the new message.
fn observed_matches_manifest(observed: &[StashEntry], manifest: &StashRenameManifest) -> bool {
    let mut captured: Vec<CapturedStash> = manifest
        .captured_lines
        .iter()
        .filter_map(|line| CapturedStash::from_manifest_line(line).ok())
        .collect();
    if captured.len() != observed.len() {
        return false;
    }
    let target_index = manifest
        .target_selector
        .strip_prefix(&format!("{STASH_REF}@{{"))
        .and_then(|s| s.strip_suffix('}'))
        .and_then(|index| index.parse::<usize>().ok());
    let new_oid = Oid::from_str(&manifest.new_oid).ok();
    let Some(target_index) = target_index else {
        return false;
    };
    let Some(new_oid) = new_oid else {
        return false;
    };
    if let Some(target) = captured.get_mut(target_index) {
        target.entry.oid = new_oid;
        target.entry.message = manifest.new_message.clone();
        target.raw_subject = renamed_stash_subject(&target.raw_subject, &manifest.new_message);
    } else {
        return false;
    }
    let expected: Vec<StashEntry> = captured.into_iter().map(|c| c.entry).collect();
    observable_stacks_match(observed, &expected)
}

/// List unfinished crash-recoverable stash renames: group every recovery ref
/// under `STASH_RENAME_RECOVERY_PREFIX` by manifest id and read each manifest.
async fn pending_stash_rename_recovers_impl(
    git: &GitBinary,
) -> Result<Vec<StashRenameRecovery>> {
    let refs_output = git
        .run(&[
            "for-each-ref",
            "--format=%(refname)",
            STASH_RENAME_RECOVERY_PREFIX,
        ])
        .await?;

    let mut grouped: HashMap<String, Vec<String>> = HashMap::default();
    for line in refs_output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix(&format!("{STASH_RENAME_RECOVERY_PREFIX}/")) else {
            continue;
        };
        let manifest_id = rest
            .split('/')
            .next()
            .map(ToString::to_string)
            .unwrap_or_default();
        grouped.entry(manifest_id).or_default().push(line.to_string());
    }

    let observed = stash_entries_for(git).await.unwrap_or_default();
    let mut recovers = Vec::new();
    for (manifest_id, refs) in grouped {
        if manifest_id.is_empty() {
            continue;
        }
        let manifest_ref = stash_rename_manifest_ref(&manifest_id);
        let recovery_refs: Vec<String> = refs
            .into_iter()
            .filter(|r| *r != manifest_ref)
            .collect();
        // Read the manifest best-effort to decide whether the rename applied.
        let rename_applied = match read_rename_manifest(git, &manifest_ref).await {
            Ok(manifest) => observed_matches_manifest(&observed, &manifest),
            Err(_) => false,
        };
        recovers.push(StashRenameRecovery {
            manifest_ref: manifest_ref.clone(),
            recovery_refs,
            observed_entries: observed.clone(),
            rename_applied,
        });
    }
    recovers.sort_by(|a, b| a.manifest_ref.cmp(&b.manifest_ref));
    Ok(recovers)
}

/// The destructive boundaries of a stash rename at which a test may inject a
/// failure (or a concurrent mutation) to verify crash-recovery semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StashRenameBoundary {
    /// Immediately after the recovery manifest + refs are written.
    AfterRecoveryWrite,
    /// After pre-replay revalidation, before the stash tip is deleted.
    BeforeRebuild,
    /// After the stash tip is deleted, before the first rebuild step (a
    /// partially replayed stack).
    MidRebuild,
    /// After the rebuild, before verification.
    BeforeVerify,
    /// After verification, before recovery refs are cleaned up.
    Cleanup,
}

/// Fire the repository's fault-injection hook at a destructive boundary, if one
/// is installed (only tests install one; production always stores `None`).
fn stash_rename_test_hook(
    fault: &Arc<
        Mutex<Option<Box<dyn Fn(StashRenameBoundary) -> Result<()> + Send>>>,
    >,
    boundary: StashRenameBoundary,
) -> Result<()> {
    if let Some(hook) = fault.lock().as_ref() {
        hook(boundary)?;
    }
    Ok(())
}

/// Perform a crash-recoverable stash rename. See `GitRepository::stash_rename`
/// for the contract; every destructive step is preceded by revalidation that
/// no external mutation moved the stack, and any failure after the recovery
/// manifest + refs are written retains them and reports their names plus the
/// observed stack.
async fn stash_rename_impl(
    git: &GitBinary,
    identity: Option<StashIdentity>,
    message: String,
    env: Arc<HashMap<String, String>>,
    fault: &Arc<
        Mutex<Option<Box<dyn Fn(StashRenameBoundary) -> Result<()> + Send>>>,
    >,
) -> Result<StashRenameResult> {
    let message = message.trim();
    anyhow::ensure!(
        !message.is_empty(),
        "stash rename requires a non-empty message"
    );

    // Capability gate: atomic ref transactions must be supported before any
    // destructive work (the recovery refs + manifest are written in one).
    ensure_ref_transaction_supported(git)
        .await
        .context("unsupported git capabilities for stash rename")?;

    // Capture the stack and uniquely resolve the exact target.
    let captured = capture_stash_stack(git).await?;
    let entries: Vec<StashEntry> = captured.iter().map(|c| c.entry.clone()).collect();
    let target = match identity {
        Some(identity) => resolve_stash_identity(&entries, &identity)
            .cloned()
            .map_err(|err| anyhow!("{err}"))?,
        None => entries
            .first()
            .cloned()
            .context("no stash entry to rename")?,
    };
    let target_captured = captured
        .iter()
        .find(|c| c.entry.index == target.index)
        .context("target stash vanished during planning")?;

    // Rewrite only the target commit's subject into a new commit (new OID),
    // preserving every other entry's OID and the stack order.
    let new_subject = renamed_stash_subject(&target_captured.raw_subject, message);
    let new_oid = rewrite_stash_commit(git, target.oid, &new_subject, &env).await?;

    // Planned observable outcome (newest-first): the target swapped for the
    // rewritten commit, everything else byte-for-byte identical and in place.
    let planned: Vec<PlannedStash> = captured
        .iter()
        .map(|captured| {
            if captured.entry.index == target.index {
                PlannedStash {
                    entry: StashEntry {
                        index: captured.entry.index,
                        oid: new_oid,
                        message: message.to_string(),
                        branch: captured.entry.branch.clone(),
                        timestamp: captured.entry.timestamp,
                    },
                    reflog_message: new_subject.clone(),
                }
            } else {
                PlannedStash {
                    entry: captured.entry.clone(),
                    reflog_message: captured.reflog_message.clone(),
                }
            }
        })
        .collect();

    // Phase 1: write the versioned recovery manifest blob and stable recovery
    // refs for every involved OID in ONE atomic ref transaction. After this
    // succeeds, any later failure must retain them and report them.
    let manifest_id = Uuid::new_v4().simple().to_string();
    let manifest_ref = stash_rename_manifest_ref(&manifest_id);
    let recovery_refs = stash_rename_recovery_refs(&manifest_id, &captured);
    let manifest = StashRenameManifest {
        version: STASH_RENAME_MANIFEST_VERSION,
        manifest_id: manifest_id.clone(),
        new_message: message.to_string(),
        new_oid: new_oid.to_string(),
        target_selector: stash_selector_for_index(target.index),
        captured_lines: captured.iter().map(|c| c.manifest_line()).collect(),
    };
    let manifest_blob_oid = write_git_blob(git, &serde_json::to_string(&manifest)?, &env).await?;
    let mut transaction_lines = vec![format!("create {manifest_ref} {}", manifest_blob_oid)];
    for (i, entry) in captured.iter().enumerate() {
        transaction_lines.push(format!(
            "create {} {}",
            recovery_refs[i],
            entry.entry.oid
        ));
    }
    transaction_lines.push(format!(
        "create {} {new_oid}",
        recovery_refs.last().context("missing target recovery ref")?
    ));
    run_ref_transaction(git, &transaction_lines).await?;

    // Phase 2: rebuild the stash reflog oldest-to-newest through Git's ref
    // backend, revalidating the captured prefix before each destructive step.
    let renamed = async {
        // After the recovery manifest + refs are written, failures must retain
        // and report them.
        stash_rename_test_hook(fault, StashRenameBoundary::AfterRecoveryWrite)?;

        // Pre-replay revalidation: nothing external may have moved the stack
        // while we were planning.
        let observed_now = stash_entries_for(git).await?;
        if !observable_stacks_match(&observed_now, &entries) {
            anyhow::bail!(
                "the stash changed while renaming; aborting before any destructive step"
            );
        }

        stash_rename_test_hook(fault, StashRenameBoundary::BeforeRebuild)?;

        // Delete the stash reflog tip, CAS-checked against the captured newest
        // entry so an external drop/insert is detected, not clobbered.
        let newest_hex = captured
            .first()
            .context("captured stack is empty")?
            .entry
            .oid
            .to_string();
        let delete_output = git
            .build_command(&["update-ref", "-d", STASH_REF])
            .arg(&newest_hex)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;
        anyhow::ensure!(
            delete_output.status.success(),
            "stash changed concurrently while renaming (could not take the tip):\n{}",
            String::from_utf8_lossy(&delete_output.stderr)
        );

        stash_rename_test_hook(fault, StashRenameBoundary::MidRebuild)?;

        // Rebuild oldest-to-newest. Each update-ref records a reflog entry and,
        // from the second entry on, is CAS-checked against the previous OID we
        // wrote: an external insert/drop mid-replay fails the step, not us.
        // The first entry must create the reflog explicitly (`stash list` reads
        // the reflog, so a bare update would leave a silent empty stash).
        let mut prev: Option<Oid> = None;
        for planned in planned.iter().rev() {
            let new_hex = planned.entry.oid.to_string();
            let mut command = git.build_command(&["update-ref"]);
            if prev.is_none() {
                command.arg("--create-reflog");
            }
            command
                .arg("-m")
                .arg(&planned.reflog_message)
                .arg(STASH_REF)
                .arg(&new_hex);
            if let Some(prev_oid) = prev {
                command.arg(prev_oid.to_string());
            }
            let update_output = command.stdout(Stdio::piped()).stderr(Stdio::piped()).output().await?;
            anyhow::ensure!(
                update_output.status.success(),
                "rebuilding stash reflog failed at entry {}:\n{}",
                planned.entry.index,
                String::from_utf8_lossy(&update_output.stderr)
            );
            prev = Some(planned.entry.oid);
        }
        stash_rename_test_hook(fault, StashRenameBoundary::BeforeVerify)?;
        Ok::<bool, anyhow::Error>(true)
    }
    .await;

    let applied = match renamed {
        Ok(applied) => applied,
        Err(err) => {
            log::warn!("stash rename failed mid-replay, retaining recovery: {err:#}");
            return Ok(StashRenameResult::FailedWithRecovery(
                stash_recovery_for(git, &manifest_ref, &recovery_refs, false).await?,
            ));
        }
    };
    if !applied {
        return Ok(StashRenameResult::FailedWithRecovery(
            stash_recovery_for(git, &manifest_ref, &recovery_refs, false).await?,
        ));
    }

    // Phase 3: verify the complete observable result matches the plan.
    let observed = stash_entries_for(git).await?;
    let expected: Vec<StashEntry> = planned.into_iter().map(|p| p.entry).collect();
    if !observable_stacks_match(&observed, &expected) {
        log::warn!("stash rename verification failed; retaining recovery");
        return Ok(StashRenameResult::FailedWithRecovery(
            stash_recovery_for(git, &manifest_ref, &recovery_refs, false).await?,
        ));
    }

    // Phase 4: delete the recovery refs only after verification passed.
    let mut cleanup_lines: Vec<String> = recovery_refs
        .iter()
        .map(|ref_name| format!("delete {ref_name}"))
        .collect();
    cleanup_lines.push(format!("delete {manifest_ref}"));
    if let Err(err) = async {
        stash_rename_test_hook(fault, StashRenameBoundary::Cleanup)?;
        run_ref_transaction(git, &cleanup_lines).await
    }
    .await
    {
        // The rename is applied and verified; only cleanup failed. Retain and
        // report the recovery refs so they are not left silently behind.
        log::warn!("stash rename cleanup of recovery refs failed: {err:#}");
        return Ok(StashRenameResult::SuccessWithRecoveryRefs(
            stash_recovery_for(git, &manifest_ref, &recovery_refs, true).await?,
        ));
    }

    Ok(StashRenameResult::Success)
}

impl GitRepository for RealGitRepository {
    fn path(&self) -> PathBuf {
        self.git_dir.clone()
    }

    fn main_repository_path(&self) -> PathBuf {
        self.common_dir.clone()
    }

    fn show(&self, commit: String) -> BoxFuture<'_, Result<CommitDetails>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let output = git
                    .build_command(&[
                        "show",
                        "--no-patch",
                        "--format=%H%x00%B%x00%at%x00%ae%x00%an%x00",
                        &commit,
                    ])
                    .output()
                    .await?;
                let output = std::str::from_utf8(&output.stdout)?;
                let fields = output.split('\0').collect::<Vec<_>>();
                if fields.len() != 6 {
                    bail!("unexpected git-show output for {commit:?}: {output:?}")
                }
                let sha = fields[0].to_string().into();
                let message = fields[1].to_string().into();
                let commit_timestamp = fields[2].parse()?;
                let author_email = fields[3].to_string().into();
                let author_name = fields[4].to_string().into();
                Ok(CommitDetails {
                    sha,
                    message,
                    commit_timestamp,
                    author_email,
                    author_name,
                })
            })
            .boxed()
    }

    fn load_commit(
        &self,
        commit: String,
        ignore_shallow_boundary: bool,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<CommitDiff>> {
        let git = self.git_binary();
        let shallow_file_path = self.common_dir.join("shallow");
        cx.background_spawn(async move {
            if !ignore_shallow_boundary
                && is_shallow_boundary_commit(&git, &shallow_file_path, &commit).await?
            {
                return Ok(CommitDiff {
                    files: Vec::new(),
                    is_shallow_boundary: true,
                });
            }

            let show_output = git
                .build_command(&[
                    "show",
                    "--format=",
                    "-z",
                    "--no-renames",
                    "--raw",
                    "--no-abbrev",
                    "--first-parent",
                ])
                .arg(&commit)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("starting git show process")?;
            anyhow::ensure!(
                show_output.status.success(),
                "git show failed: {}",
                String::from_utf8_lossy(&show_output.stderr)
            );

            let show_stdout = String::from_utf8_lossy(&show_output.stdout);
            let changes = parse_git_diff_raw(&show_stdout);

            let mut cat_file_process = git
                .build_command(&["cat-file", "--batch=%(objectsize)"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .context("starting git cat-file process")?;

            let mut files = Vec::<CommitFile>::new();
            let stdin = cat_file_process
                .stdin
                .take()
                .context("git cat-file process has no stdin")?;
            let stdout = cat_file_process
                .stdout
                .take()
                .context("git cat-file process has no stdout")?;
            let mut stdin = BufWriter::with_capacity(512, stdin);
            let mut stdout = BufReader::new(stdout);
            let mut info_line = String::new();
            let mut newline = [b'\0'];
            for change in changes {
                let change = change?;
                let path = change.path;
                // git-show outputs `/`-delimited paths even on Windows.
                let Some(rel_path) = RelPath::from_unix_str(path).log_err() else {
                    continue;
                };

                let objects = [change.new_object, change.old_object];
                let mut has_blobs = false;
                for object in objects.iter().flatten() {
                    if object.kind == CommitDiffObjectKind::Blob {
                        stdin.write_all(object.oid.as_bytes()).await?;
                        stdin.write_all(b"\n").await?;
                        has_blobs = true;
                    }
                }
                if has_blobs {
                    stdin.flush().await?;
                }

                let [new_object, old_object] = objects;
                let new_object =
                    load_commit_object(new_object, &mut stdout, &mut info_line, &mut newline)
                        .await?;
                let old_object =
                    load_commit_object(old_object, &mut stdout, &mut info_line, &mut newline)
                        .await?;
                let is_binary = new_object.as_ref().is_some_and(|object| object.is_binary)
                    || old_object.as_ref().is_some_and(|object| object.is_binary);
                let new_content = new_object.map(|object| object.content);
                let old_content = old_object.map(|object| object.content);

                files.push(CommitFile {
                    path: RepoPath(Arc::from(rel_path)),
                    old_content,
                    new_content,
                    is_binary,
                })
            }

            Ok(CommitDiff {
                files,
                is_shallow_boundary: false,
            })
        })
        .boxed()
    }

    fn load_commit_range(
        &self,
        base: String,
        target: String,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<CommitDiff>> {
        let git = self.git_binary();
        cx.background_spawn(async move {
            let diff_output = git
                .build_command(&["diff", "-z", "--no-renames", "--raw", "--no-abbrev"])
                .arg(&base)
                .arg(&target)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .context("starting git diff process")?;
            anyhow::ensure!(
                diff_output.status.success(),
                "git diff failed: {}",
                String::from_utf8_lossy(&diff_output.stderr)
            );

            let diff_stdout = String::from_utf8_lossy(&diff_output.stdout);
            let changes = parse_git_diff_raw(&diff_stdout);

            let mut cat_file_process = git
                .build_command(&["cat-file", "--batch=%(objectsize)"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .context("starting git cat-file process")?;

            let mut files = Vec::<CommitFile>::new();
            let stdin = cat_file_process
                .stdin
                .take()
                .context("git cat-file process has no stdin")?;
            let stdout = cat_file_process
                .stdout
                .take()
                .context("git cat-file process has no stdout")?;
            let mut stdin = BufWriter::with_capacity(512, stdin);
            let mut stdout = BufReader::new(stdout);
            let mut info_line = String::new();
            let mut newline = [b'\0'];
            for change in changes {
                let change = change?;
                let path = change.path;
                // git-show outputs `/`-delimited paths even on Windows.
                let Some(rel_path) = RelPath::from_unix_str(path).log_err() else {
                    continue;
                };

                let objects = [change.new_object, change.old_object];
                let mut has_blobs = false;
                for object in objects.iter().flatten() {
                    if object.kind == CommitDiffObjectKind::Blob {
                        stdin.write_all(object.oid.as_bytes()).await?;
                        stdin.write_all(b"\n").await?;
                        has_blobs = true;
                    }
                }
                if has_blobs {
                    stdin.flush().await?;
                }

                let [new_object, old_object] = objects;
                let new_object =
                    load_commit_object(new_object, &mut stdout, &mut info_line, &mut newline)
                        .await?;
                let old_object =
                    load_commit_object(old_object, &mut stdout, &mut info_line, &mut newline)
                        .await?;
                let is_binary = new_object.as_ref().is_some_and(|object| object.is_binary)
                    || old_object.as_ref().is_some_and(|object| object.is_binary);
                let new_content = new_object.map(|object| object.content);
                let old_content = old_object.map(|object| object.content);

                files.push(CommitFile {
                    path: RepoPath(Arc::from(rel_path)),
                    old_content,
                    new_content,
                    is_binary,
                })
            }

            Ok(CommitDiff {
                files,
                is_shallow_boundary: false,
            })
        })
        .boxed()
    }

    fn reset(
        &self,
        commit: String,
        mode: ResetMode,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            let commit = resolve_commit_oid(&git, &commit).await?;
            let mode_flag = match mode {
                ResetMode::Mixed => "--mixed",
                ResetMode::Soft => "--soft",
                ResetMode::Hard => "--hard",
            };
            run_git_mutation(&git, &["reset", mode_flag, &commit], &env).await
        }
        .boxed()
    }

    fn checkout_commit(
        &self,
        commit: String,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            let commit = resolve_commit_oid(&git, &commit).await?;
            run_git_mutation(&git, &["checkout", "--detach", &commit], &env).await
        }
        .boxed()
    }

    fn create_tag(
        &self,
        options: CreateTagOptions,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            anyhow::ensure!(
                !options.name.starts_with('-'),
                "tag name cannot start with '-'"
            );
            let target = resolve_commit_oid(&git, &options.target).await?;
            let tag_ref = format!("refs/tags/{}", options.name);
            git.run(&["check-ref-format", &tag_ref]).await?;

            let mut args = vec!["tag".to_string()];
            if let Some(message) = options.message {
                args.extend(["-a".into(), "-m".into(), message]);
            }
            args.push("--".into());
            args.extend([options.name, target]);
            run_git_mutation(&git, &args, &env).await
        }
        .boxed()
    }

    fn cherry_pick(
        &self,
        commits: Vec<String>,
        no_commit: bool,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            anyhow::ensure!(
                !commits.is_empty(),
                "cherry-pick requires at least one commit"
            );
            let git = git?;
            let mut args = vec!["cherry-pick".to_string()];
            if no_commit {
                args.push("--no-commit".into());
            }
            for commit in commits {
                args.push(resolve_commit_oid(&git, &commit).await?);
            }
            run_git_mutation(&git, &args, &env).await
        }
        .boxed()
    }

    fn revert(
        &self,
        commit: String,
        no_commit: bool,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            let commit = resolve_commit_oid(&git, &commit).await?;
            let mut args = vec!["revert".to_string(), "--no-edit".into()];
            if no_commit {
                args.push("--no-commit".into());
            }
            args.push(commit);
            run_git_mutation(&git, &args, &env).await
        }
        .boxed()
    }

    fn merge(
        &self,
        commit: String,
        mode: MergeMode,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            let commit = resolve_commit_oid(&git, &commit).await?;
            let mut args = vec!["merge".to_string(), "--no-edit".into()];
            match mode {
                MergeMode::Default => {}
                MergeMode::FastForwardOnly => args.push("--ff-only".into()),
                MergeMode::NoFastForward => args.push("--no-ff".into()),
                MergeMode::Squash => args.push("--squash".into()),
            }
            args.push(commit);
            run_git_mutation(&git, &args, &env).await
        }
        .boxed()
    }

    fn operation_state(&self) -> BoxFuture<'_, Result<Option<GitOperationKind>>> {
        let git_dir = self.git_dir.clone();
        async move {
            if git_dir.join("rebase-merge").exists() || git_dir.join("rebase-apply").exists() {
                Ok(Some(GitOperationKind::Rebase))
            } else if git_dir.join("CHERRY_PICK_HEAD").exists() {
                Ok(Some(GitOperationKind::CherryPick))
            } else if git_dir.join("REVERT_HEAD").exists() {
                Ok(Some(GitOperationKind::Revert))
            } else if git_dir.join("MERGE_HEAD").exists() {
                Ok(Some(GitOperationKind::Merge))
            } else {
                Ok(None)
            }
        }
        .boxed()
    }

    fn run_operation_action(
        &self,
        operation: GitOperationKind,
        action: GitOperationAction,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        let current_op_fut = self.operation_state();
        async move {
            let current_op = current_op_fut.await?;
            anyhow::ensure!(
                current_op == Some(operation),
                "operation state mismatch: expected {:?}, found {:?}",
                operation,
                current_op
            );

            if operation == GitOperationKind::Merge && action == GitOperationAction::Skip {
                anyhow::bail!("cannot skip a merge operation");
            }

            let git = git?;
            let op_str = match operation {
                GitOperationKind::Merge => "merge",
                GitOperationKind::Rebase => "rebase",
                GitOperationKind::CherryPick => "cherry-pick",
                GitOperationKind::Revert => "revert",
            };

            let action_str = match action {
                GitOperationAction::Continue => "--continue",
                GitOperationAction::Skip => "--skip",
                GitOperationAction::Abort => "--abort",
            };

            let mut merged_env = (*env).clone();
            if action == GitOperationAction::Continue {
                merged_env.insert("GIT_EDITOR".to_string(), "true".to_string());
            }

            let args = [op_str, action_str];
            run_git_mutation(&git, &args, &merged_env).await
        }
        .boxed()
    }

    fn checkout_files(
        &self,
        commit: String,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        async move {
            let git = git?;
            if paths.is_empty() {
                return Ok(());
            }

            let output = git
                .build_command(&["checkout", &commit, "--"])
                .envs(env.iter())
                .args(paths.iter().map(|path| path.as_unix_str()))
                .output()
                .await?;
            anyhow::ensure!(
                output.status.success(),
                "Failed to checkout files:\n{}",
                String::from_utf8_lossy(&output.stderr),
            );
            Ok(())
        }
        .boxed()
    }

    fn load_blob_content(&self, oid: Oid) -> BoxFuture<'_, Result<Vec<u8>>> {
        let git_binary = self.git_binary();
        let oid_str = oid.to_string();
        self.executor
            .spawn(async move {
                let mut command = git_binary.build_command(&["cat-file", "blob", &oid_str]);
                let output = command.output().await?;
                anyhow::ensure!(
                    output.status.success(),
                    GitBinaryCommandError {
                        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                        status: output.status,
                    }
                );
                Ok(output.stdout)
            })
            .boxed()
    }

    fn load_commit_template(&self) -> BoxFuture<'_, Result<Option<GitCommitTemplate>>> {
        let working_directory = self.working_directory();
        let git_binary = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let working_directory = working_directory?;
                let git_binary = git_binary?;
                let output = git_binary
                    .build_command(&["config", "--get", "commit.template"])
                    .output()
                    .await
                    .context("failed to run git config --get commit.template")?;

                let raw_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !output.status.success() || raw_path.is_empty() {
                    return Ok(None);
                }

                let path = PathBuf::from(&raw_path);
                let path = if let Some(path) = raw_path.strip_prefix("~/") {
                    paths::home_dir().join(path)
                } else if path.is_relative() {
                    working_directory.join(path)
                } else {
                    path
                };

                let template = match std::fs::read_to_string(&path) {
                    Ok(s) if !s.trim().is_empty() => Some(s),
                    Err(err) => {
                        log::warn!("failed to read commit template {}: {}", path.display(), err);
                        None
                    }
                    _ => None,
                };

                Ok(template.map(|template| GitCommitTemplate { template }))
            })
            .boxed()
    }

    fn set_index_text(
        &self,
        path: RepoPath,
        content: Option<Vec<u8>>,
        env: Arc<HashMap<String, String>>,
        is_executable: bool,
    ) -> BoxFuture<'_, anyhow::Result<()>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let mode = if is_executable { "100755" } else { "100644" };

                if let Some(content) = content {
                    let mut child = git
                        .build_command(&[
                            "hash-object",
                            "-w",
                            "--stdin",
                            "--path",
                            path.as_unix_str(),
                        ])
                        .envs(env.iter())
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()?;
                    let mut stdin = child.stdin.take().context("hash-object has no stdin")?;
                    stdin.write_all(&content).await?;
                    stdin.flush().await?;
                    drop(stdin);
                    let output = child.output().await?;
                    anyhow::ensure!(
                        output.status.success(),
                        GitBinaryCommandError {
                            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                            status: output.status,
                        }
                    );
                    let sha = str::from_utf8(&output.stdout)?.trim();

                    log::debug!("indexing SHA: {sha}, path {path:?}");

                    let output = git
                        .build_command(&["update-index", "--add", "--cacheinfo", mode, sha])
                        .envs(env.iter())
                        .arg(path.as_unix_str())
                        .output()
                        .await?;

                    anyhow::ensure!(
                        output.status.success(),
                        "Failed to stage:\n{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                } else {
                    log::debug!("removing path {path:?} from the index");
                    let output = git
                        .build_command(&["update-index", "--force-remove", "--"])
                        .envs(env.iter())
                        .arg(path.as_unix_str())
                        .output()
                        .await?;
                    anyhow::ensure!(
                        output.status.success(),
                        "Failed to unstage:\n{}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }

                Ok(())
            })
            .boxed()
    }

    fn remote_urls(&self) -> BoxFuture<'_, HashMap<String, String>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                if let Ok(stdout) = git.run(&["remote", "-v"]).await {
                    parse_remote_urls(&stdout)
                } else {
                    HashMap::default()
                }
            })
            .boxed()
    }

    fn revparse_batch(&self, revs: Vec<String>) -> BoxFuture<'_, Result<Vec<Option<String>>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let mut process = git
                    .build_command(&["cat-file", "--batch-check=%(objectname)"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?;

                let stdin = process
                    .stdin
                    .take()
                    .context("no stdin for git cat-file subprocess")?;
                let mut stdin = BufWriter::new(stdin);
                for rev in &revs {
                    stdin.write_all(rev.as_bytes()).await?;
                    stdin.write_all(b"\n").await?;
                }
                stdin.flush().await?;
                drop(stdin);

                let output = process.output().await?;
                let output = std::str::from_utf8(&output.stdout)?;
                let shas = output
                    .lines()
                    .map(|line| {
                        if line.ends_with("missing") {
                            None
                        } else {
                            Some(line.to_string())
                        }
                    })
                    .collect::<Vec<_>>();

                if shas.len() != revs.len() {
                    // In an octopus merge, git cat-file still only outputs the first sha from MERGE_HEAD.
                    bail!("unexpected number of shas")
                }

                Ok(shas)
            })
            .boxed()
    }

    fn load_revisions(
        &self,
        revisions: Vec<String>,
    ) -> BoxFuture<'_, Result<Vec<Option<Vec<u8>>>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                if revisions.is_empty() {
                    return Ok(Vec::new());
                }
                if let Some(revision) = revisions.iter().find(|revision| revision.contains('\n')) {
                    anyhow::bail!(
                        "revision spec {revision:?} contains a newline and cannot be passed to git cat-file --batch"
                    );
                }

                let mut process = git
                    .build_command(&["cat-file", "--batch"])
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()?;

                let mut stdin = BufWriter::new(process.stdin.take().context("no stdin")?);
                let mut stdout = BufReader::new(process.stdout.take().context("no stdout")?);
                let mut newline = [0u8; 1];

                let mut header_bytes = Vec::new();
                let mut results = Vec::with_capacity(revisions.len());
                for rev in &revisions {
                    stdin.write_all(rev.as_bytes()).await?;
                    stdin.write_all(b"\n").await?;
                    stdin.flush().await?;

                    header_bytes.clear();
                    stdout.read_until(b'\n', &mut header_bytes).await?;
                    let header_line = String::from_utf8_lossy(&header_bytes);

                    let parts: Vec<&str> = header_line.trim().split(' ').collect();
                    match parts[..] {
                        [.., "missing"] => {
                            results.push(None);
                        }
                        [_, object_type, size_str] => {
                            let size: usize = size_str
                                .parse()
                                .with_context(|| format!("invalid object size: {size_str}"))?;

                            let mut content = vec![0u8; size];
                            stdout.read_exact(&mut content).await?;
                            stdout.read_exact(&mut newline).await?;

                            if object_type == "blob" {
                                results.push(Some(content));
                            } else {
                                results.push(None);
                            }
                        }
                        _ => bail!("invalid cat-file header: {header_line}"),
                    }
                }

                drop(stdin);
                process.output().await?;
                Ok(results)
            })
            .boxed()
    }

    fn merge_message(&self) -> BoxFuture<'_, Option<String>> {
        let path = self.path().join("MERGE_MSG");
        self.executor
            .spawn(async move { std::fs::read_to_string(&path).ok() })
            .boxed()
    }

    fn status(&self, path_prefixes: &[RepoPath]) -> Task<Result<GitStatus>> {
        let git = self.git_binary_in_worktree();
        let args = git_status_args(path_prefixes);
        log::debug!("Checking for git status in {path_prefixes:?}");
        self.executor.spawn(async move {
            let git = git?;
            let output = git.build_command(&args).output().await?;
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                stdout.parse()
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("git status failed: {stderr}");
            }
        })
    }

    fn check_access(&self) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                git?.run(&["rev-parse"]).await?;
                Ok(())
            })
            .boxed()
    }

    fn diff_tree(&self, request: DiffTreeType) -> BoxFuture<'_, Result<TreeDiff>> {
        let git = self.git_binary_in_worktree();
        let working_directory = self.working_directory.clone();
        let merge_base_ref = match &request {
            DiffTreeType::MergeBaseWithWorktree { base } => Some(base.clone()),
            DiffTreeType::MergeBase { .. } | DiffTreeType::Since { .. } => None,
        };

        let args = match request {
            DiffTreeType::MergeBase { base, head } => [
                "diff-tree",
                "-r",
                "-z",
                "--abbrev=64",
                "--no-renames",
                "--merge-base",
                base.as_str(),
                head.as_str(),
            ]
            .map(OsString::from)
            .to_vec(),
            DiffTreeType::MergeBaseWithWorktree { base } => [
                "diff",
                "--raw",
                "-z",
                "--abbrev=64",
                "--no-renames",
                "--merge-base",
                base.as_str(),
            ]
            .map(OsString::from)
            .to_vec(),
            DiffTreeType::Since { base, head } => [
                "diff-tree",
                "-r",
                "-z",
                "--abbrev=64",
                "--no-renames",
                base.as_str(),
                head.as_str(),
            ]
            .map(OsString::from)
            .to_vec(),
        };

        self.executor
            .spawn(async move {
                let git = git?;
                let output = git.build_command(&args).output().await?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("git diff-tree failed: {stderr}");
                }

                let stdout = String::from_utf8_lossy(&output.stdout);
                let mut tree_diff = stdout.parse::<TreeDiff>()?;
                let Some(merge_base_ref) = merge_base_ref else {
                    return Ok(tree_diff);
                };
                let Some(working_directory) = working_directory else {
                    return Ok(tree_diff);
                };
                if !tree_diff
                    .entries
                    .values()
                    .any(|status| matches!(status, TreeDiffStatus::Deleted { .. }))
                {
                    return Ok(tree_diff);
                }

                let status_output = git.build_command(&git_status_args(&[])).output().await?;
                if !status_output.status.success() {
                    let stderr = String::from_utf8_lossy(&status_output.stderr);
                    anyhow::bail!("git status failed: {stderr}");
                }
                let status = String::from_utf8_lossy(&status_output.stdout).parse::<GitStatus>()?;
                // Files the diff reports as deleted but that exist on disk
                // (deleted from the index or from a commit, then recreated).
                // `git diff` compares them against the index, so compare their
                // disk contents against the merge base ourselves.
                let recreated: Vec<(RepoPath, Oid)> = status
                    .entries
                    .iter()
                    .filter(|(_, status)| {
                        matches!(
                            *status,
                            FileStatus::Untracked
                                | FileStatus::Tracked(TrackedStatus {
                                    index_status: StatusCode::Deleted,
                                    worktree_status: StatusCode::Added,
                                })
                        )
                    })
                    .filter_map(|(path, _)| match tree_diff.entries.get(path) {
                        Some(TreeDiffStatus::Deleted { old }) => Some((path.clone(), *old)),
                        _ => None,
                    })
                    .collect();
                if recreated.is_empty() {
                    return Ok(tree_diff);
                }

                let merge_base_output = git
                    .build_command(&["merge-base", merge_base_ref.as_ref(), "HEAD"])
                    .output()
                    .await?;
                if !merge_base_output.status.success() {
                    let stderr = String::from_utf8_lossy(&merge_base_output.stderr);
                    anyhow::bail!("git merge-base failed: {stderr}");
                }
                let merge_base = String::from_utf8_lossy(&merge_base_output.stdout);
                let merge_base = merge_base.trim();

                for (path, old) in recreated {
                    let full_path = working_directory.join(path.as_std_path());
                    let metadata = match smol::fs::symlink_metadata(&full_path).await {
                        Ok(metadata) => metadata,
                        Err(_) => continue,
                    };
                    let base_entry = git
                        .build_command(
                            &["ls-tree", merge_base, "--", path.as_unix_str()].map(OsString::from),
                        )
                        .output()
                        .await?;
                    if !base_entry.status.success() {
                        continue;
                    }
                    let base_mode = String::from_utf8_lossy(&base_entry.stdout);
                    let Some(base_mode) = base_mode.split_ascii_whitespace().next() else {
                        continue;
                    };
                    let current_mode = if metadata.file_type().is_symlink() {
                        "120000"
                    } else {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::PermissionsExt as _;
                            if metadata.permissions().mode() & 0o111 == 0 {
                                "100644"
                            } else {
                                "100755"
                            }
                        }
                        #[cfg(not(unix))]
                        {
                            "100644"
                        }
                    };
                    if current_mode != base_mode {
                        tree_diff
                            .entries
                            .insert(path.clone(), TreeDiffStatus::Modified { old });
                        continue;
                    }

                    let hash_output = if metadata.file_type().is_symlink() {
                        let target = smol::fs::read_link(&full_path).await?;
                        let mut child = git
                            .build_command(&["hash-object", "--stdin"])
                            .stdin(Stdio::piped())
                            .stdout(Stdio::piped())
                            .spawn()?;
                        let mut stdin = child.stdin.take().context("hash-object has no stdin")?;
                        stdin
                            .write_all(target.as_os_str().as_encoded_bytes())
                            .await?;
                        stdin.flush().await?;
                        drop(stdin);
                        child.output().await?
                    } else {
                        git.build_command(&[
                            OsString::from("hash-object"),
                            OsString::from(format!("--path={}", path.as_unix_str())),
                            OsString::from("--"),
                            full_path.into_os_string(),
                        ])
                        .output()
                        .await?
                    };
                    if !hash_output.status.success() {
                        continue;
                    }
                    let worktree_oid = String::from_utf8_lossy(&hash_output.stdout);
                    if worktree_oid.trim() == old.to_string() {
                        tree_diff.entries.remove(&path);
                    } else {
                        tree_diff
                            .entries
                            .insert(path.clone(), TreeDiffStatus::Modified { old });
                    }
                }
                Ok(tree_diff)
            })
            .boxed()
    }

    fn stash_entries(&self) -> BoxFuture<'static, Result<GitStash>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let output = git
                    .build_command(&["stash", "list", "--pretty=format:%gd%x00%H%x00%ct%x00%s"])
                    .output()
                    .await?;
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    stdout.parse()
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("git status failed: {stderr}");
                }
            })
            .boxed()
    }

    fn branches(&self) -> BoxFuture<'_, Result<BranchesScanResult>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let fields = [
                    "%(HEAD)",
                    "%(objectname)",
                    "%(parent)",
                    "%(refname)",
                    "%(upstream)",
                    "%(upstream:track)",
                    "%(committerdate:unix)",
                    "%(authorname)",
                    "%(contents:subject)",
                ]
                .join("%00");
                let args = vec![
                    "for-each-ref",
                    "refs/heads/**/*",
                    "refs/remotes/**/*",
                    "--format",
                    &fields,
                ];
                let output = git.build_command(&args).output().await?;

                let error = if output.status.success() {
                    None
                } else {
                    let error = format_branch_scan_error(&output);
                    log::warn!("failed to get git branches with commit metadata: {error}");
                    Some(error.into())
                };

                let input = String::from_utf8_lossy(&output.stdout);
                let mut branches = parse_branch_input(&input)?;
                if branches.is_empty() {
                    let args = vec!["symbolic-ref", "--quiet", "HEAD"];

                    let output = git.build_command(&args).output().await?;

                    // git symbolic-ref returns a non-0 exit code if HEAD points
                    // to something other than a branch
                    if output.status.success() {
                        let name = String::from_utf8_lossy(&output.stdout).trim().to_string();

                        branches.push(Branch {
                            ref_name: name.into(),
                            is_head: true,
                            upstream: None,
                            most_recent_commit: None,
                        });
                    }
                }

                Ok(BranchesScanResult { branches, error })
            })
            .boxed()
    }

    fn worktrees(&self) -> BoxFuture<'_, Result<Vec<Worktree>>> {
        let git = self.git_binary();
        let main_worktree_path = original_repo_path_from_common_dir(&self.common_dir);
        self.executor
            .spawn(async move {
                let output = git
                    .build_command(&["worktree", "list", "--porcelain"])
                    .output()
                    .await?;
                if output.status.success() {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    Ok(parse_worktrees_from_str(
                        &stdout,
                        main_worktree_path.as_deref(),
                    ))
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("git worktree list failed: {stderr}");
                }
            })
            .boxed()
    }

    fn worktree_created_at(
        &self,
        worktree_path: PathBuf,
    ) -> BoxFuture<'_, Result<Option<SystemTime>>> {
        self.executor
            .spawn(async move {
                match std::fs::metadata(&worktree_path) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("failed to stat {}", worktree_path.display())
                        });
                    }
                    Ok(_) => {}
                }
                let git_dir = linked_worktree_git_dir(&worktree_path)?;
                let metadata = std::fs::metadata(&git_dir)
                    .with_context(|| format!("failed to stat {}", git_dir.display()))?;
                let created_at = metadata.created().with_context(|| {
                    format!("creation time unavailable for {}", git_dir.display())
                })?;
                Ok(Some(created_at))
            })
            .boxed()
    }

    fn create_worktree(
        &self,
        target: CreateWorktreeTarget,
        path: PathBuf,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();
        let mut args = vec![OsString::from("worktree"), OsString::from("add")];

        match &target {
            CreateWorktreeTarget::ExistingBranch { branch_name } => {
                args.push(OsString::from("--"));
                args.push(OsString::from(path.as_os_str()));
                args.push(OsString::from(branch_name));
            }
            CreateWorktreeTarget::NewBranch {
                branch_name,
                base_sha: start_point,
            } => {
                args.push(OsString::from("-b"));
                args.push(OsString::from(branch_name));
                args.push(OsString::from("--"));
                args.push(OsString::from(path.as_os_str()));
                args.push(OsString::from(start_point.as_deref().unwrap_or("HEAD")));
            }
            CreateWorktreeTarget::Detached {
                base_sha: start_point,
            } => {
                args.push(OsString::from("--detach"));
                args.push(OsString::from("--"));
                args.push(OsString::from(path.as_os_str()));
                args.push(OsString::from(start_point.as_deref().unwrap_or("HEAD")));
            }
        }

        self.executor
            .spawn(async move {
                std::fs::create_dir_all(path.parent().unwrap_or(&path))?;
                let output = git.build_command(&args).output().await?;
                if output.status.success() {
                    Ok(())
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("git worktree add failed: {stderr}");
                }
            })
            .boxed()
    }

    fn remove_worktree(&self, path: PathBuf, force: bool) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();

        self.executor
            .spawn(async move {
                let mut args: Vec<OsString> = vec!["worktree".into(), "remove".into()];
                if force {
                    args.push("--force".into());
                }
                args.push("--".into());
                args.push(path.as_os_str().into());
                git.run(&args).await?;
                anyhow::Ok(())
            })
            .boxed()
    }

    fn rename_worktree(&self, old_path: PathBuf, new_path: PathBuf) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();

        self.executor
            .spawn(async move {
                let args: Vec<OsString> = vec![
                    "worktree".into(),
                    "move".into(),
                    "--".into(),
                    old_path.as_os_str().into(),
                    new_path.as_os_str().into(),
                ];
                git.run(&args).await?;
                anyhow::Ok(())
            })
            .boxed()
    }

    fn checkout_branch_in_worktree(
        &self,
        branch_name: String,
        worktree_path: PathBuf,
        create: bool,
    ) -> BoxFuture<'_, Result<()>> {
        let git_binary = GitBinary::new(
            self.any_git_binary_path.clone(),
            worktree_path,
            self.path(),
            self.executor.clone(),
            self.is_trusted(),
        );

        self.executor
            .spawn(async move {
                if create {
                    git_binary.run(&["checkout", "-b", &branch_name]).await?;
                } else {
                    git_binary.run(&["checkout", &branch_name]).await?;
                }
                anyhow::Ok(())
            })
            .boxed()
    }

    fn change_branch(&self, name: String) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git_binary = git_binary?;
                let local_ref = format!("refs/heads/{name}");
                if git_binary
                    .run(&["show-ref", "--verify", "--quiet", &local_ref])
                    .await
                    .is_ok()
                {
                    git_binary.run(&["checkout", &name]).await?;
                    return anyhow::Ok(());
                }

                let remote_ref = format!("refs/remotes/{name}");
                if git_binary
                    .run(&["show-ref", "--verify", "--quiet", &remote_ref])
                    .await
                    .is_ok()
                {
                    let name = match git_binary.run(&["symbolic-ref", &remote_ref]).await {
                        Ok(resolved) => resolved
                            .strip_prefix("refs/remotes/")
                            .map(str::to_owned)
                            .unwrap_or(name),
                        Err(_) => name,
                    };
                    let (_, branch_name) =
                        name.split_once('/').context("Unexpected branch format")?;
                    let local_branch_ref = format!("refs/heads/{branch_name}");
                    if git_binary
                        .run(&["show-ref", "--verify", "--quiet", &local_branch_ref])
                        .await
                        .is_ok()
                    {
                        git_binary
                            .run(&["branch", "--set-upstream-to", &name, branch_name])
                            .await?;
                    } else {
                        git_binary
                            .run(&["branch", "--track", branch_name, &name])
                            .await?;
                    }

                    git_binary.run(&["checkout", branch_name]).await?;
                    return anyhow::Ok(());
                }

                anyhow::bail!("Branch '{}' not found", name);
            })
            .boxed()
    }

    fn create_branch(
        &self,
        name: String,
        base_branch: Option<String>,
    ) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git_binary = git_binary?;
                let mut args = vec!["switch", "-c", &name];
                let base_branch_str;
                if let Some(ref base) = base_branch {
                    base_branch_str = base.clone();
                    args.push(&base_branch_str);
                }

                git_binary.run(&args).await?;
                anyhow::Ok(())
            })
            .boxed()
    }

    fn rename_branch(&self, branch: String, new_name: String) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git_binary = git_binary?;
                git_binary
                    .run(&["branch", "-m", &branch, &new_name])
                    .await?;
                anyhow::Ok(())
            })
            .boxed()
    }

    fn delete_branch(
        &self,
        is_remote: bool,
        name: String,
        force: bool,
    ) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git_binary = git_binary?;
                let flag = delete_branch_flag(is_remote, force);
                git_binary.run(&["branch", flag, &name]).await?;
                anyhow::Ok(())
            })
            .boxed()
    }

    fn blame(
        &self,
        path: RepoPath,
        content: Rope,
        line_ending: LineEnding,
    ) -> BoxFuture<'_, Result<crate::blame::Blame>> {
        let git = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git = git?;
                crate::blame::Blame::for_path(&git, &path, &content, line_ending).await
            })
            .boxed()
    }

    fn blame_at_revision(
        &self,
        path: RepoPath,
        revision: Oid,
    ) -> BoxFuture<'_, Result<crate::blame::Blame>> {
        let git = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git = git?;
                crate::blame::Blame::for_path_at_revision(&git, &path, revision).await
            })
            .boxed()
    }

    fn diff(&self, diff: DiffType) -> BoxFuture<'_, Result<String>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let output = match diff {
                    DiffType::HeadToIndex => {
                        git.build_command(&["diff", "--staged"]).output().await?
                    }
                    DiffType::HeadToWorktree => git.build_command(&["diff"]).output().await?,
                    DiffType::MergeBase { base_ref } => {
                        git.build_command(&["diff", "--merge-base", base_ref.as_ref()])
                            .output()
                            .await?
                    }
                };

                anyhow::ensure!(
                    output.status.success(),
                    "Failed to run git diff:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            })
            .boxed()
    }

    fn diff_stat(
        &self,
        diff: DiffStatType,
        path_prefixes: &[RepoPath],
    ) -> BoxFuture<'static, Result<crate::status::GitDiffStat>> {
        let path_prefixes = path_prefixes.to_vec();
        let git_binary = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git_binary = git_binary?;
                let mut args: Vec<String> =
                    vec!["diff".into(), "--numstat".into(), "--no-renames".into()];
                match diff {
                    DiffStatType::HeadToIndex => args.extend(["--cached".into(), "HEAD".into()]),
                    DiffStatType::HeadToWorktree => args.push("HEAD".into()),
                    DiffStatType::IndexToWorktree => {}
                }
                if !path_prefixes.is_empty() {
                    args.push("--".into());
                    args.extend(
                        path_prefixes
                            .iter()
                            .map(|p| p.as_std_path().to_string_lossy().into_owned()),
                    );
                }
                let output = git_binary.run(&args).await?;
                Ok(crate::status::parse_numstat(&output))
            })
            .boxed()
    }

    fn diff_commits(
        &self,
        base: String,
        target: String,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>> {
        let git = self.git_binary();
        let diff_arg = format!("{base}...{target}");
        let mut command = git.build_command(&["diff"]);
        command.arg(diff_arg);
        cx.background_spawn(async move {
            let output = command.output().await?;
            anyhow::ensure!(
                output.status.success(),
                "git diff failed:\n{}",
                String::from_utf8_lossy(&output.stderr),
            );
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        })
        .boxed()
    }

    fn diff_worktree_path(
        &self,
        head_oid: String,
        path: RepoPath,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>> {
        let git = self.git_binary_in_worktree();
        let path = path.as_unix_str().to_owned();
        cx.background_spawn(async move {
            let git = git?;
            let output = git
                .build_command(&["diff", &head_oid, "--"])
                .arg(&path)
                .output()
                .await?;
            anyhow::ensure!(
                output.status.success(),
                "git diff failed:\n{}",
                String::from_utf8_lossy(&output.stderr),
            );
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        })
        .boxed()
    }

    fn load_worktree_path(
        &self,
        path: RepoPath,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<String>> {
        let working_directory = self.working_directory();
        let rel_path = path.as_std_path().to_path_buf();
        cx.background_spawn(async move {
            let working_directory = working_directory.context("reading worktree file")?;
            let abs_path = working_directory.join(rel_path);
            smol::fs::read_to_string(abs_path)
                .await
                .context("reading untracked worktree file")
        })
        .boxed()
    }

    fn stage_paths(
        &self,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                if !paths.is_empty() {
                    let output = git
                        .build_command(&["update-index", "--add", "--remove", "--"])
                        .envs(env.iter())
                        .args(paths.iter().map(|p| p.as_unix_str()))
                        .output()
                        .await?;
                    anyhow::ensure!(
                        output.status.success(),
                        "Failed to stage paths:\n{}",
                        String::from_utf8_lossy(&output.stderr),
                    );
                }
                Ok(())
            })
            .boxed()
    }

    fn unstage_paths(
        &self,
        paths: Vec<RepoPath>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();

        self.executor
            .spawn(async move {
                let git = git?;
                if !paths.is_empty() {
                    let output = git
                        .build_command(&["reset", "--quiet", "--"])
                        .envs(env.iter())
                        .args(paths.iter().map(|p| p.as_std_path()))
                        .output()
                        .await?;

                    anyhow::ensure!(
                        output.status.success(),
                        "Failed to unstage:\n{}",
                        String::from_utf8_lossy(&output.stderr),
                    );
                }
                Ok(())
            })
            .boxed()
    }

    fn stash_paths(
        &self,
        paths: Vec<RepoPath>,
        message: Option<String>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let mut args = vec!["stash", "push", "--quiet", "--include-untracked"];
                if let Some(message) = message.as_deref() {
                    args.extend_from_slice(&["--message", message]);
                }
                args.push("--");
                let output = git
                    .build_command(&args)
                    .envs(env.iter())
                    .args(paths.iter().map(|p| p.as_unix_str()))
                    .output()
                    .await?;

                anyhow::ensure!(
                    output.status.success(),
                    "Failed to stash:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok(())
            })
            .boxed()
    }

    fn stash_staged(
        &self,
        message: Option<String>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                // `--staged` cannot be expressed as a pathspec: a partially staged
                // file would otherwise have its unstaged hunks stashed too.
                let mut args = vec!["stash", "push", "--quiet", "--staged"];
                if let Some(message) = message.as_deref() {
                    args.extend_from_slice(&["--message", message]);
                }
                let output = git.build_command(&args).envs(env.iter()).output().await?;

                anyhow::ensure!(
                    output.status.success(),
                    "Failed to stash staged changes (requires git 2.35 or newer):\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok(())
            })
            .boxed()
    }

    fn stash_pop(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<StashMutationResult>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let target = resolve_stash_target(&git, identity.as_ref()).await?;
                // Apply uses the exact selected OID.
                let oid_hex = target.oid.to_string();
                let output = git
                    .build_command(&["stash", "apply", &oid_hex])
                    .envs(env.iter())
                    .output()
                    .await?;
                anyhow::ensure!(
                    output.status.success(),
                    "Failed to stash apply during pop:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );

                // Re-resolve the same entry after applying, then drop it by its
                // freshly resolved reflog selector.
                let retained = match resolve_stash_target(&git, identity.as_ref()).await {
                    Ok(target) => {
                        let selector = stash_selector_for_index(target.index);
                        let output = git
                            .build_command(&["stash", "drop", &selector])
                            .envs(env.iter())
                            .output()
                            .await?;
                        if output.status.success() {
                            false
                        } else {
                            log::warn!(
                                "stash pop applied but drop failed: {}",
                                String::from_utf8_lossy(&output.stderr)
                            );
                            true
                        }
                    }
                    Err(err) => {
                        // The entry vanished between apply and drop on the next
                        // step; treat it as still applied (it was), so we do not
                        // hide the partial success.
                        log::warn!("stash pop apply succeeded but drop could not be re-resolved: {err}");
                        true
                    }
                };

                Ok(if retained {
                    StashMutationResult::AppliedButRetained
                } else {
                    StashMutationResult::Success
                })
            })
            .boxed()
    }

    fn stash_apply(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let target = resolve_stash_target(&git, identity.as_ref()).await?;
                // Apply uses the exact selected OID.
                let oid_hex = target.oid.to_string();
                let output = git
                    .build_command(&["stash", "apply", &oid_hex])
                    .envs(env.iter())
                    .output()
                    .await?;
                anyhow::ensure!(
                    output.status.success(),
                    "Failed to apply stash:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok(())
            })
            .boxed()
    }

    fn stash_drop(
        &self,
        identity: Option<StashIdentity>,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let target = resolve_stash_target(&git, identity.as_ref()).await?;
                // Drop uses only the freshly resolved reflog selector.
                let selector = stash_selector_for_index(target.index);
                let output = git
                    .build_command(&["stash", "drop", &selector])
                    .envs(env.iter())
                    .output()
                    .await?;
                anyhow::ensure!(
                    output.status.success(),
                    "Failed to drop stash:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                Ok(())
            })
            .boxed()
    }

    fn stash_rename(
        &self,
        identity: Option<StashIdentity>,
        message: String,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<StashRenameResult>> {
        let git = self.git_binary_in_worktree();
        let fault = self.stash_rename_fault.clone();
        self.executor
            .spawn(async move {
                let git = git?;
                stash_rename_impl(&git, identity, message, env, &fault).await
            })
            .boxed()
    }

    fn pending_stash_rename_recovers(&self) -> BoxFuture<'_, Result<Vec<StashRenameRecovery>>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                pending_stash_rename_recovers_impl(&git).await
            })
            .boxed()
    }

    fn commit(
        &self,
        message: SharedString,
        name_and_email: Option<(SharedString, SharedString)>,
        options: CommitOptions,
        ask_pass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        let executor = self.executor.clone();
        // Note: Do not spawn this command on the background thread, it might pop open the credential helper
        // which we want to block on.
        async move {
            let git = git?;
            let mut cmd = git.build_command(&["commit", "--quiet", "-m"]);
            cmd.envs(env.iter())
                .arg(&message.to_string())
                .arg("--cleanup=strip")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            if options.amend {
                cmd.arg("--amend");
            }

            if options.signoff {
                cmd.arg("--signoff");
            }

            if options.allow_empty {
                cmd.arg("--allow-empty");
            }

            if options.no_verify {
                cmd.arg("--no-verify");
            }

            if let Some((name, email)) = name_and_email {
                cmd.arg("--author").arg(&format!("{name} <{email}>"));
            }

            run_git_command(env, ask_pass, cmd, executor).await?;

            Ok(())
        }
        .boxed()
    }

    fn update_ref(&self, ref_name: String, commit: String) -> BoxFuture<'_, Result<()>> {
        self.edit_ref(RefEdit::Update { ref_name, commit })
    }

    fn delete_ref(&self, ref_name: String) -> BoxFuture<'_, Result<()>> {
        self.edit_ref(RefEdit::Delete { ref_name })
    }

    fn repair_worktrees(&self) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let args: Vec<OsString> = vec!["worktree".into(), "repair".into()];
                git.run(&args).await?;
                Ok(())
            })
            .boxed()
    }

    fn delete_refs_on_remote(
        &self,
        remote_name: String,
        refs: Vec<String>,
        ask_pass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>> {
        let working_directory = self.command_directory();
        let git_directory = self.path();
        let executor = cx.background_executor().clone();
        let git_binary_path = self.system_git_binary_path.clone();
        let is_trusted = self.is_trusted();
        // Note: Do not spawn this command on the background thread, it might pop open the credential helper
        // which we want to block on.
        async move {
            let git_binary_path = git_binary_path.context("git not found on $PATH, can't delete refs on remote")?;
            if refs.is_empty() {
                anyhow::bail!("no refs provided to delete on remote {remote_name}");
            }
            let git = GitBinary::new(
                git_binary_path,
                working_directory,
                git_directory,
                executor.clone(),
                is_trusted,
            );
            // `git push <remote> --delete <ref>…` — an explicit deletion on the
            // server. Each ref is its own argument so unusual ref names are
            // never shell-interpreted.
            let mut command = git.build_command(&["push"]);
            command.envs(env.iter()).arg(&remote_name).arg("--delete");
            for ref_name in &refs {
                command.arg(ref_name);
            }
            command
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            run_git_command(env, ask_pass, command, executor).await
        }
        .boxed()
    }

    fn delete_tag(&self, name: String) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                // `git tag -d <name>` refuses to delete a non-tag ref. The name
                // is passed as its own argument so a tag containing an unusual
                // but valid character (e.g. a slash) is never split.
                if name.is_empty() || name.contains('/') || name == "." || name == ".." {
                    anyhow::bail!("invalid tag name {name:?}");
                }
                git.run(&["tag", "-d", &name]).await?;
                Ok(())
            })
            .boxed()
    }

    fn tag_details(&self, ref_name: String) -> BoxFuture<'_, Result<TagDetails>> {
        let git = self.git_binary();
        self.executor.spawn(async move {
            if ref_name.is_empty() {
                anyhow::bail!("empty tag ref");
            }
            // The display-shortened name is the bit after `refs/tags/`.
            let name = ref_name
                .strip_prefix("refs/tags/")
                .context("tag details require a refs/tags/ ref")?
                .to_string();
            if name.is_empty() || name == "." || name == ".." {
                anyhow::bail!("invalid tag name {name:?}");
            }

            // `git cat-file -t` reports whether the ref points at a `tag`
            // object (annotated) or directly at a commit/tree/blob
            // (lightweight). This is Git's own classification, not a custom
            // delimiter parse, so it cannot be fooled by tag message content.
            let raw_type = git.run(&["cat-file", "-t", ref_name.as_str()]).await?.trim().to_string();
            let is_annotated = raw_type == "tag";

            // The ultimate non-tag target the tag points at (works for both
            // lightweight and annotated tags since `^{}` peels and a
            // commit/tree/blob peels to itself).
            let peeled_arg = format!("{ref_name}^{{}}");
            let target_oid = Oid::try_from(
                git.run(&["rev-parse", "--verify", peeled_arg.as_str()])
                    .await?
                    .trim(),
            )
            .context("failed to parse tag target oid")?;

            if !is_annotated {
                return Ok(TagDetails {
                    ref_name: ref_name.clone().into(),
                    name: name.into(),
                    target_oid,
                    object_type: tag_object_type(&raw_type)?,
                    tagger: None,
                    message: None,
                });
            }

            // Annotated: the tag object body carries tagger metadata and a
            // message, both parsed unambiguously off a single blank-line
            // delimiter that separates the fixed header block from the message.
            let tag_body = git.run(&["cat-file", "-p", ref_name.as_str()]).await?;
            let (tagger, message) = parse_tag_body(&tag_body);
            let target_oid_str = target_oid.to_string();
            let object_type = tag_object_type(
                &git.run(&["cat-file", "-t", target_oid_str.as_str()])
                    .await?
                    .trim()
                    .to_string(),
            )?;
            Ok(TagDetails {
                ref_name: ref_name.clone().into(),
                name: name.into(),
                target_oid,
                object_type,
                tagger,
                message,
            })
        })
        .boxed()
    }

    fn push(
        &self,
        branch_name: String,
        remote_branch_name: String,
        remote_name: String,
        options: Option<PushOptions>,
        ask_pass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>> {
        let working_directory = self.command_directory();
        let git_directory = self.path();
        let executor = cx.background_executor().clone();
        let git_binary_path = self.system_git_binary_path.clone();
        let is_trusted = self.is_trusted();
        // Note: Do not spawn this command on the background thread, it might pop open the credential helper
        // which we want to block on.
        async move {
            let git_binary_path = git_binary_path.context("git not found on $PATH, can't push")?;
            let git = GitBinary::new(
                git_binary_path,
                working_directory,
                git_directory,
                executor.clone(),
                is_trusted,
            );
            let mut command = git.build_command(&["push"]);
            command
                .envs(env.iter())
                .args(options.map(|option| match option {
                    PushOptions::SetUpstream => "--set-upstream",
                    PushOptions::Force => "--force-with-lease",
                }))
                .arg(remote_name)
                .arg(format!("{}:{}", branch_name, remote_branch_name))
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            run_git_command(env, ask_pass, command, executor).await
        }
        .boxed()
    }

    fn pull(
        &self,
        branch_name: Option<String>,
        remote_name: String,
        rebase: bool,
        ask_pass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>> {
        let working_directory = self.command_directory();
        let git_directory = self.path();
        let executor = cx.background_executor().clone();
        let git_binary_path = self.system_git_binary_path.clone();
        let is_trusted = self.is_trusted();
        // Note: Do not spawn this command on the background thread, it might pop open the credential helper
        // which we want to block on.
        async move {
            let git_binary_path = git_binary_path.context("git not found on $PATH, can't pull")?;
            let git = GitBinary::new(
                git_binary_path,
                working_directory,
                git_directory,
                executor.clone(),
                is_trusted,
            );
            let mut command = git.build_command(&["pull"]);
            command.envs(env.iter());

            if rebase {
                command.arg("--rebase");
            }

            command
                .arg(remote_name)
                .args(branch_name)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            run_git_command(env, ask_pass, command, executor).await
        }
        .boxed()
    }

    fn fetch(
        &self,
        fetch_options: FetchOptions,
        ask_pass: AskPassDelegate,
        env: Arc<HashMap<String, String>>,
        cx: AsyncApp,
    ) -> BoxFuture<'_, Result<RemoteCommandOutput>> {
        let working_directory = self.command_directory();
        let git_directory = self.path();
        let remote_name = format!("{}", fetch_options);
        let git_binary_path = self.system_git_binary_path.clone();
        let executor = cx.background_executor().clone();
        let is_trusted = self.is_trusted();
        // Note: Do not spawn this command on the background thread, it might pop open the credential helper
        // which we want to block on.
        async move {
            let git_binary_path = git_binary_path.context("git not found on $PATH, can't fetch")?;
            let git = GitBinary::new(
                git_binary_path,
                working_directory,
                git_directory,
                executor.clone(),
                is_trusted,
            );
            let mut command = git.build_command(&["fetch", &remote_name]);
            command
                .envs(env.iter())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());

            run_git_command(env, ask_pass, command, executor).await
        }
        .boxed()
    }

    fn get_push_remote(&self, branch: String) -> BoxFuture<'_, Result<Option<Remote>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let output = git
                    .build_command(&["rev-parse", "--abbrev-ref"])
                    .arg(format!("{branch}@{{push}}"))
                    .output()
                    .await?;
                if !output.status.success() {
                    return Ok(None);
                }
                let remote_name = String::from_utf8_lossy(&output.stdout)
                    .split('/')
                    .next()
                    .map(|name| Remote {
                        name: name.trim().to_string().into(),
                    });

                Ok(remote_name)
            })
            .boxed()
    }

    fn get_branch_remote(&self, branch: String) -> BoxFuture<'_, Result<Option<Remote>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let output = git
                    .build_command(&["config", "--get"])
                    .arg(format!("branch.{branch}.remote"))
                    .output()
                    .await?;
                if !output.status.success() {
                    return Ok(None);
                }

                let remote_name = String::from_utf8_lossy(&output.stdout);
                return Ok(Some(Remote {
                    name: remote_name.trim().to_string().into(),
                }));
            })
            .boxed()
    }

    fn get_all_remotes(&self) -> BoxFuture<'_, Result<Vec<Remote>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let output = git.build_command(&["remote", "-v"]).output().await?;

                anyhow::ensure!(
                    output.status.success(),
                    "Failed to get all remotes:\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                let remote_names: HashSet<Remote> = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .filter(|line| !line.is_empty())
                    .filter_map(|line| {
                        let mut split_line = line.split_whitespace();
                        let remote_name = split_line.next()?;

                        Some(Remote {
                            name: remote_name.trim().to_string().into(),
                        })
                    })
                    .collect();

                Ok(remote_names.into_iter().collect())
            })
            .boxed()
    }

    fn remove_remote(&self, name: String) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary();
        self.executor
            .spawn(async move {
                git_binary.run(&["remote", "remove", &name]).await?;
                Ok(())
            })
            .boxed()
    }

    fn create_remote(&self, name: String, url: String) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary();
        self.executor
            .spawn(async move {
                git_binary.run(&["remote", "add", &name, &url]).await?;
                Ok(())
            })
            .boxed()
    }

    fn check_for_pushed_commit(&self) -> BoxFuture<'_, Result<Vec<SharedString>>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                // This command outputs a list of remote tracking refs, e.g.:
                // refs/remotes/origin/HEAD
                // refs/remotes/origin/main
                let Ok(output) = git?
                    .run(&[
                        "for-each-ref",
                        "--format=%(refname)",
                        "--contains",
                        "HEAD",
                        "refs/remotes/",
                    ])
                    .await
                else {
                    return Ok(Vec::new());
                };

                Ok(output
                    .lines()
                    .map(|line| line.trim())
                    .filter(|line| !line.ends_with("/HEAD"))
                    .filter_map(|line| line.strip_prefix("refs/remotes/"))
                    .map(SharedString::from)
                    .collect())
            })
            .boxed()
    }

    fn checkpoint(&self) -> BoxFuture<'static, Result<GitRepositoryCheckpoint>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let mut git = git?.envs(checkpoint_author_envs());
                git.with_temp_index(async |git| {
                    let head_sha = git.run(&["rev-parse", "HEAD"]).await.ok();

                    git.run(&["add", "--update"]).await?;
                    let untracked_files = untracked_files_for_checkpoint(git).await?;
                    add_files_to_index(git, &untracked_files).await?;
                    let tree = git.run(&["write-tree"]).await?;
                    let checkpoint_sha = if let Some(head_sha) = head_sha.as_deref() {
                        git.run(&["commit-tree", &tree, "-p", head_sha, "-m", "Checkpoint"])
                            .await?
                    } else {
                        git.run(&["commit-tree", &tree, "-m", "Checkpoint"]).await?
                    };

                    Ok(GitRepositoryCheckpoint {
                        commit_sha: checkpoint_sha.parse()?,
                    })
                })
                .await
            })
            .boxed()
    }

    fn restore_checkpoint(&self, checkpoint: GitRepositoryCheckpoint) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                git.run(&[
                    "restore",
                    "--source",
                    &checkpoint.commit_sha.to_string(),
                    "--worktree",
                    ".",
                ])
                .await?;

                // TODO: We don't track binary and large files anymore,
                //       so the following call would delete them.
                //       Implement an alternative way to track files added by agent.
                //
                // git.with_temp_index(async move |git| {
                //     git.run(&["read-tree", &checkpoint.commit_sha.to_string()])
                //         .await?;
                //     git.run(&["clean", "-d", "--force"]).await
                // })
                // .await?;

                Ok(())
            })
            .boxed()
    }

    fn create_archive_checkpoint(&self) -> BoxFuture<'_, Result<(String, String)>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let mut git = git?.envs(checkpoint_author_envs());
                let head_sha = git
                    .run(&["rev-parse", "HEAD"])
                    .await
                    .context("failed to read HEAD")?;

                // Capture the staged state: write-tree reads the current index
                let staged_tree = git
                    .run(&["write-tree"])
                    .await
                    .context("failed to write staged tree")?;
                let staged_sha = git
                    .run(&[
                        "commit-tree",
                        &staged_tree,
                        "-p",
                        &head_sha,
                        "-m",
                        "WIP staged",
                    ])
                    .await
                    .context("failed to create staged commit")?;

                // Capture the full state (staged + unstaged + untracked) using
                // a temporary index so we don't disturb the real one.
                let unstaged_sha = git
                    .with_temp_index(async |git| {
                        git.run(&["add", "--all"]).await?;
                        let full_tree = git.run(&["write-tree"]).await?;
                        let sha = git
                            .run(&[
                                "commit-tree",
                                &full_tree,
                                "-p",
                                &staged_sha,
                                "-m",
                                "WIP unstaged",
                            ])
                            .await?;
                        Ok(sha)
                    })
                    .await
                    .context("failed to create unstaged commit")?;

                Ok((staged_sha, unstaged_sha))
            })
            .boxed()
    }

    fn restore_archive_checkpoint(
        &self,
        staged_sha: String,
        unstaged_sha: String,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                // First, set the index AND working tree to match the unstaged
                // tree. --reset -u computes a tree-level diff between the
                // current index and unstaged_sha's tree and applies additions,
                // modifications, and deletions to the working directory.
                git.run(&["read-tree", "--reset", "-u", &unstaged_sha])
                    .await
                    .context("failed to restore working directory from unstaged commit")?;

                // Then replace just the index with the staged tree. Without -u
                // this doesn't touch the working directory, so the result is:
                // working tree = unstaged state, index = staged state.
                git.run(&["read-tree", &staged_sha])
                    .await
                    .context("failed to restore index from staged commit")?;

                Ok(())
            })
            .boxed()
    }

    fn compare_checkpoints(
        &self,
        left: GitRepositoryCheckpoint,
        right: GitRepositoryCheckpoint,
    ) -> BoxFuture<'_, Result<bool>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                let result = git
                    .run(&[
                        "diff-tree",
                        "--quiet",
                        &left.commit_sha.to_string(),
                        &right.commit_sha.to_string(),
                    ])
                    .await;
                match result {
                    Ok(_) => Ok(true),
                    Err(error) => {
                        if let Some(GitBinaryCommandError { status, .. }) =
                            error.downcast_ref::<GitBinaryCommandError>()
                            && status.code() == Some(1)
                        {
                            return Ok(false);
                        }

                        Err(error)
                    }
                }
            })
            .boxed()
    }

    fn diff_checkpoints(
        &self,
        base_checkpoint: GitRepositoryCheckpoint,
        target_checkpoint: GitRepositoryCheckpoint,
    ) -> BoxFuture<'_, Result<String>> {
        let git = self.git_binary_in_worktree();
        self.executor
            .spawn(async move {
                let git = git?;
                git.run(&[
                    "diff",
                    "--find-renames",
                    "--patch",
                    &base_checkpoint.commit_sha.to_string(),
                    &target_checkpoint.commit_sha.to_string(),
                ])
                .await
            })
            .boxed()
    }

    fn default_branch(
        &self,
        include_remote_name: bool,
    ) -> BoxFuture<'_, Result<Option<SharedString>>> {
        let git = self.git_binary();
        self.executor
            .spawn(async move {
                let output = git
                    .run(&[
                        "for-each-ref",
                        "--format=%(refname)\t%(symref)",
                        "refs/remotes/upstream/HEAD",
                        "refs/remotes/origin/HEAD",
                        "refs/heads/",
                    ])
                    .await
                    .unwrap_or_default();
                let refs: HashMap<&str, &str> = output
                    .lines()
                    .filter_map(|line| line.split_once('\t'))
                    .collect();

                if let Some(target) = refs.get("refs/remotes/upstream/HEAD") {
                    let strip_prefix = if include_remote_name {
                        "refs/remotes/"
                    } else {
                        "refs/remotes/upstream/"
                    };
                    if let Some(branch) = target.strip_prefix(strip_prefix) {
                        return Ok(Some(branch.into()));
                    }
                }

                if let Some(target) = refs.get("refs/remotes/origin/HEAD") {
                    let strip_prefix = if include_remote_name {
                        "refs/remotes/"
                    } else {
                        "refs/remotes/origin/"
                    };
                    if let Some(branch) = target.strip_prefix(strip_prefix) {
                        return Ok(Some(branch.into()));
                    }
                }

                let local_branch_exists =
                    |branch: &str| refs.contains_key(format!("refs/heads/{branch}").as_str());

                if let Ok(default_branch) = git.run(&["config", "init.defaultBranch"]).await {
                    if local_branch_exists(&default_branch) {
                        return Ok(Some(default_branch.into()));
                    }
                }

                if local_branch_exists("main") {
                    return Ok(Some("main".into()));
                }

                if local_branch_exists("master") {
                    return Ok(Some("master".into()));
                }

                Ok(None)
            })
            .boxed()
    }

    fn run_hook(
        &self,
        hook: RunHook,
        env: Arc<HashMap<String, String>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git_binary = self.git_binary_in_worktree();
        let git_dir = self.git_dir.clone();
        let help_output = self.any_git_binary_help_output();

        // Note: Do not spawn these commands on the background thread, as this causes some git hooks to hang.
        async move {
            let git_binary = git_binary?;
            let working_directory = git_binary.working_directory.clone();
            if !help_output
                .await
                .lines()
                .any(|line| line.trim().starts_with("hook "))
            {
                let hook_abs_path = git_dir.join("hooks").join(hook.as_str());
                if hook_abs_path.is_file() && git_binary.is_trusted {
                    #[allow(clippy::disallowed_methods)]
                    let output = new_command(&hook_abs_path)
                        .envs(env.iter())
                        .current_dir(&working_directory)
                        .output()
                        .await?;

                    if !output.status.success() {
                        return Err(GitBinaryCommandError {
                            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                            status: output.status,
                        }
                        .into());
                    }
                }

                return Ok(());
            }

            if git_binary.is_trusted {
                let git_binary = git_binary.envs(HashMap::clone(&env));
                git_binary
                    .run(&["hook", "run", "--ignore-missing", hook.as_str()])
                    .await?;
            }
            Ok(())
        }
        .boxed()
    }

    fn initial_graph_data(
        &self,
        log_source: LogSource,
        log_order: LogOrder,
        request_tx: Sender<Vec<Arc<InitialGraphCommitData>>>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();

        async move {
            let log_source_args = log_source.get_args();
            let mut git_log_command = vec![
                "log",
                GRAPH_COMMIT_FORMAT,
                "--decorate=full",
                log_order.as_arg(),
            ];
            git_log_command.extend(log_source_args.iter().map(|arg| arg.as_ref()));
            let mut command = git.build_command(&git_log_command);
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());

            let mut child = command.spawn()?;
            let stdout = child.stdout.take().context("failed to get stdout")?;
            let stderr = child.stderr.take().context("failed to get stderr")?;
            let mut reader = BufReader::new(stdout);

            let mut line_buffer = String::new();
            let mut lines: Vec<String> = Vec::with_capacity(GRAPH_CHUNK_SIZE);

            // `LogSource::All` additionally renders the current stash reflog as
            // connected stash rows. Stash rows must appear above their base so
            // the child→parent lane connects downward, so we buffer the regular
            // stream and emit stash rows first. Every other log source keeps the
            // existing byte-for-byte streaming path below.
            let mut regular_commits: Option<Vec<Arc<InitialGraphCommitData>>> = if log_source
                == LogSource::All
            {
                Some(Vec::new())
            } else {
                None
            };

            loop {
                line_buffer.clear();
                let bytes_read = reader.read_line(&mut line_buffer).await?;

                if bytes_read == 0 {
                    if !lines.is_empty() {
                        let commits = parse_initial_graph_output(lines.iter().map(|s| s.as_str()));
                        if let Some(buffered) = regular_commits.as_mut() {
                            buffered.extend(commits);
                        } else if request_tx.send(commits).await.is_err() {
                            log::warn!(
                                "initial_graph_data: receiver dropped while sending commits"
                            );
                        }
                    }
                    break;
                }

                let line = line_buffer.trim_end_matches('\n').to_string();
                lines.push(line);

                if lines.len() >= GRAPH_CHUNK_SIZE {
                    let commits = parse_initial_graph_output(lines.iter().map(|s| s.as_str()));
                    if let Some(buffered) = regular_commits.as_mut() {
                        buffered.extend(commits);
                    } else if request_tx.send(commits).await.is_err() {
                        log::warn!("initial_graph_data: receiver dropped while streaming commits");
                        break;
                    }
                    lines.clear();
                }
            }

            let status = child.status().await?;
            if !status.success() {
                let mut stderr_output = String::new();
                BufReader::new(stderr)
                    .read_to_string(&mut stderr_output)
                    .await
                    .log_err();

                if stderr_output.is_empty() {
                    anyhow::bail!("git log command failed with {}", status);
                } else {
                    anyhow::bail!("git log command failed with {}: {}", status, stderr_output);
                }
            }

            if let Some(regular_commits) = regular_commits {
                // Stash rows carry `refs/stash@{N}` as their identity. An exact
                // base that is not reachable from the loaded graph is fetched as
                // one supplemental row placed directly below its stash row so the
                // child→parent lane resolves; unrelated stash-only ancestors are
                // never pulled in.
                let stash_rows = self.stash_graph_data().await?;
                let regular_oids: std::collections::HashSet<_> = regular_commits
                    .iter()
                    .map(|commit| commit.sha)
                    .collect();

                // Fetch each unreachable exact base once so adjacent stash rows
                // referencing the same base never duplicate a supplemental row.
                let bases_to_fetch: std::collections::HashSet<Oid> = stash_rows
                    .iter()
                    .filter_map(|stash| stash.parents.first().copied())
                    .filter(|base| !regular_oids.contains(base))
                    .collect();
                let mut base_rows: std::collections::HashMap<Oid, Arc<InitialGraphCommitData>> =
                    std::collections::HashMap::default();
                for base in &bases_to_fetch {
                    if let Some(row) = self.graph_commit_for_base(*base).await? {
                        base_rows.insert(*base, row);
                    }
                }

                let mut ordered = Vec::with_capacity(stash_rows.len() * 2 + regular_commits.len());
                for stash in &stash_rows {
                    ordered.push(stash.clone());
                    if let Some(base) = stash.parents.first().copied()
                        && !regular_oids.contains(&base)
                        && let Some(base_row) = base_rows.remove(&base)
                    {
                        ordered.push(base_row);
                    }
                }
                ordered.extend(regular_commits);

                for chunk in ordered.chunks(GRAPH_CHUNK_SIZE) {
                    if request_tx.send(chunk.to_vec()).await.is_err() {
                        log::warn!(
                            "initial_graph_data: receiver dropped while streaming commits"
                        );
                        break;
                    }
                }
            }

            Ok(())
        }
        .boxed()
    }

    fn stash_graph_data(
        &self,
    ) -> BoxFuture<'_, Result<Vec<Arc<InitialGraphCommitData>>>> {
        let git = self.git_binary_in_worktree();
        self.executor.spawn(async move {
            let git = git?;
            // The stash reflog may be absent (no stash entries); that is a valid
            // empty result, not an error.
            if git
                .build_command(&["rev-parse", "-q", "--verify", STASH_REF])
                .output()
                .await?
                .status
                .success()
            {
                let output = git
                    .run(&["log", "-g", STASH_REF, "--format=%H%x00%P%x00%gD"])
                    .await?;
                // Reflog rows carry the reflog selector (`refs/stash@{N}`) as a
                // decoration; the existing graph parser understands that shape.
                // Only the first parent (the base) is kept so the stash row
                // connects to the base without pulling stash-only ancestors.
                Ok(output
                    .lines()
                    .filter_map(|line| {
                        let mut parts = line.split('\x00');
                        let sha = Oid::from_str(parts.next()?).ok()?;
                        let parents = parts.next()?;
                        let first_parent = parents
                            .split_whitespace()
                            .filter_map(|p| Oid::from_str(p).ok())
                            .next();
                        let ref_names = parts.next().unwrap_or("");
                        let ref_names = if ref_names.is_empty() {
                            Vec::new()
                        } else {
                            ref_names
                                .split(", ")
                                .map(|s| SharedString::from(s.to_string()))
                                .collect()
                        };
                        Some(Arc::new(InitialGraphCommitData {
                            sha,
                            parents: first_parent.into_iter().collect(),
                            ref_names,
                        }))
                    })
                    .collect())
            } else {
                Ok(Vec::new())
            }
        })
        .boxed()
    }

    fn graph_commit_for_base(
        &self,
        sha: Oid,
    ) -> BoxFuture<'_, Result<Option<Arc<InitialGraphCommitData>>>> {
        let git = self.git_binary_in_worktree();
        self.executor.spawn(async move {
            let git = git?;
            let output = git
                .build_command(&[
                    "log",
                    "-n",
                    "1",
                    "--format=%H%x00%P%x00%D",
                    "--decorate=full",
                    &sha.to_string(),
                ])
                .output()
                .await?;
            let output = String::from_utf8_lossy(&output.stdout).to_string();
            Ok(output.lines().filter_map(|line| {
                let mut parts = line.split('\x00');
                let sha = Oid::from_str(parts.next()?).ok()?;
                let parents = parts.next()?;
                let parents = parents
                    .split_whitespace()
                    .filter_map(|p| Oid::from_str(p).ok())
                    .collect();
                let ref_names = parts.next().unwrap_or("");
                let ref_names = if ref_names.is_empty() {
                    Vec::new()
                } else {
                    ref_names
                        .split(", ")
                        .map(|s| SharedString::from(s.to_string()))
                        .collect()
                };
                Some(Arc::new(InitialGraphCommitData {
                    sha,
                    parents,
                    ref_names,
                }))
            }).next())
        })
        .boxed()
    }

    fn search_commits(
        &self,
        log_source: LogSource,
        search_args: SearchCommitArgs,
        request_tx: Sender<Oid>,
    ) -> BoxFuture<'_, Result<()>> {
        let git = self.git_binary();

        async move {
            let log_source_args = log_source.get_args();
            let mut args = vec!["log", SEARCH_COMMIT_FORMAT];
            let hash_query = commit_hash_search_query(search_args.query.as_str())
                .map(|query| query.to_ascii_lowercase());

            if hash_query.is_none() {
                args.push("--fixed-strings");

                if !search_args.case_sensitive {
                    args.push("--regexp-ignore-case");
                }

                args.push("--grep");
                args.push(search_args.query.as_str());
            }

            args.extend(log_source_args.iter().map(|arg| arg.as_ref()));
            let mut command = git.build_command(&args);
            command.stdout(Stdio::piped());
            command.stderr(Stdio::null());

            let mut child = command.spawn()?;
            let stdout = child.stdout.take().context("failed to get stdout")?;
            let mut reader = BufReader::new(stdout);

            let mut line_buffer = String::new();

            loop {
                line_buffer.clear();
                let bytes_read = reader.read_line(&mut line_buffer).await?;

                if bytes_read == 0 {
                    break;
                }

                let sha = line_buffer.trim_end_matches('\n');
                if let Some(hash_query) = hash_query.as_ref()
                    && !sha.to_ascii_lowercase().starts_with(hash_query)
                {
                    continue;
                }

                if let Ok(oid) = Oid::from_str(sha)
                    && request_tx.send(oid).await.is_err()
                {
                    break;
                }
            }

            child.status().await?;
            Ok(())
        }
        .boxed()
    }

    fn file_history_changed_files(
        &self,
        paths: Vec<RepoPath>,
        commit_limit: usize,
    ) -> BoxFuture<'_, Result<Vec<FileHistoryChangedFileSets>>> {
        let git = self.git_binary();
        let shallow_file_path = self.common_dir.join("shallow");

        async move {
            if paths.is_empty() {
                return Ok(Vec::new());
            }

            if commit_limit == 0 {
                return Ok(vec![FileHistoryChangedFileSets::default(); paths.len()]);
            }

            let max_count_arg = format!("--max-count={commit_limit}");
            let mut args = [
                "log",
                max_count_arg.as_str(),
                "--full-diff",
                "--no-renames",
                "--name-only",
                "-z",
                "--format=%x1e%H",
                "--",
            ]
            .map(OsString::from)
            .to_vec();
            args.extend(paths.iter().map(|path| OsString::from(path.as_unix_str())));

            let output = git.build_command(&args).output().await?;
            anyhow::ensure!(
                output.status.success(),
                "git log failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );

            let shallow_boundary_oids = read_shallow_file(&shallow_file_path)
                .await?
                .map(|contents| {
                    contents
                        .lines()
                        .map(|line| line.trim().to_string())
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();

            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(parse_file_history_changed_files_output(
                &stdout,
                &paths,
                &shallow_boundary_oids,
            ))
        }
        .boxed()
    }

    fn commit_data_reader(&self) -> Result<CommitDataReader> {
        let git_binary = self.git_binary();

        let (request_tx, request_rx) = async_channel::bounded::<CommitDataRequest>(64);

        let task = self.executor.spawn(async move {
            if let Err(error) = run_commit_data_reader(git_binary, request_rx).await {
                log::error!("commit data reader failed: {error:?}");
            }
        });

        Ok(CommitDataReader {
            request_tx,
            _task: task,
        })
    }

    fn set_trusted(&self, trusted: bool) {
        self.is_trusted
            .store(trusted, std::sync::atomic::Ordering::Release);
    }

    fn is_trusted(&self) -> bool {
        self.is_trusted.load(std::sync::atomic::Ordering::Acquire)
    }
}

async fn run_commit_data_reader(
    git: GitBinary,
    request_rx: async_channel::Receiver<CommitDataRequest>,
) -> Result<()> {
    let mut process = git
        .build_command(&["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("starting git cat-file --batch process")?;

    let mut stdin = BufWriter::new(process.stdin.take().context("no stdin")?);
    let mut stdout = BufReader::new(process.stdout.take().context("no stdout")?);

    const MAX_BATCH_SIZE: usize = 64;

    while let Ok(first_request) = request_rx.recv().await {
        let mut pending_requests = vec![first_request];

        while pending_requests.len() < MAX_BATCH_SIZE {
            match request_rx.try_recv() {
                Ok(request) => pending_requests.push(request),
                Err(_) => break,
            }
        }

        for request in &pending_requests {
            stdin.write_all(request.sha.to_string().as_bytes()).await?;
            stdin.write_all(b"\n").await?;
        }
        stdin.flush().await?;

        for request in pending_requests {
            let result = read_single_commit_response(&mut stdout, &request.sha).await;
            request.response_tx.send(result).ok();
        }
    }

    drop(stdin);
    process.kill().ok();

    Ok(())
}

async fn read_single_commit_response<R: smol::io::AsyncBufRead + Unpin>(
    stdout: &mut R,
    sha: &Oid,
) -> Result<CommitData> {
    let mut header_bytes = Vec::new();
    stdout.read_until(b'\n', &mut header_bytes).await?;
    let header_line = String::from_utf8_lossy(&header_bytes);

    let parts: Vec<&str> = header_line.trim().split(' ').collect();
    if parts.len() < 3 {
        bail!("invalid cat-file header: {header_line}");
    }

    let object_type = parts[1];
    if object_type == "missing" {
        bail!("object not found: {}", sha);
    }

    if object_type != "commit" {
        bail!("expected commit object, got {object_type}");
    }

    let size: usize = parts[2]
        .parse()
        .with_context(|| format!("invalid object size: {}", parts[2]))?;

    let mut content = vec![0u8; size];
    stdout.read_exact(&mut content).await?;

    let mut newline = [0u8; 1];
    stdout.read_exact(&mut newline).await?;

    let content_str = String::from_utf8_lossy(&content);
    parse_cat_file_commit(*sha, &content_str)
        .ok_or_else(|| anyhow!("failed to parse commit {}", sha))
}

fn parse_file_history_changed_files_output(
    output: &str,
    queried_paths: &[RepoPath],
    shallow_boundary_oids: &HashSet<String>,
) -> Vec<FileHistoryChangedFileSets> {
    let mut histories = vec![FileHistoryChangedFileSets::default(); queried_paths.len()];

    for record in output.split('\x1e') {
        let mut fields = record.split('\0');
        let sha = fields.next().unwrap_or_default().trim();
        if shallow_boundary_oids.contains(sha) {
            continue;
        }
        let changed_files = fields
            .filter_map(|field| {
                let path = field.trim_start_matches('\n');
                if path.is_empty() {
                    return None;
                }
                RepoPath::new(path).ok()
            })
            .collect::<std::collections::BTreeSet<_>>();

        if changed_files.is_empty() {
            continue;
        }

        let file_set = changed_files.iter().cloned().collect::<Vec<_>>();
        for (index, queried_path) in queried_paths.iter().enumerate() {
            if changed_files.contains(queried_path) {
                histories[index].file_sets.push(file_set.clone());
            }
        }
    }

    histories
}

fn parse_initial_graph_output<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> Vec<Arc<InitialGraphCommitData>> {
    lines
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            // Format: "SHA\x00PARENT1 PARENT2...\x00REF1, REF2, ..."
            let mut parts = line.split('\x00');

            let sha = Oid::from_str(parts.next()?).ok()?;
            let parents_str = parts.next()?;
            let parents = parents_str
                .split_whitespace()
                .filter_map(|p| Oid::from_str(p).ok())
                .collect();

            let ref_names_str = parts.next().unwrap_or("");
            let ref_names = if ref_names_str.is_empty() {
                Vec::new()
            } else {
                ref_names_str
                    .split(", ")
                    .filter(|decoration| *decoration != "grafted" && *decoration != "replaced")
                    .map(|s| SharedString::from(s.to_string()))
                    .collect()
            };

            Some(Arc::new(InitialGraphCommitData {
                sha,
                parents,
                ref_names,
            }))
        })
        .collect()
}

fn git_status_args(path_prefixes: &[RepoPath]) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("status"),
        OsString::from("--porcelain=v1"),
        OsString::from("--untracked-files=all"),
        OsString::from("--no-renames"),
        OsString::from("-z"),
        OsString::from("--"),
    ];
    args.extend(path_prefixes.iter().map(|path_prefix| {
        if path_prefix.is_empty() {
            Path::new(".").into()
        } else {
            path_prefix.as_std_path().into()
        }
    }));
    args
}

/// Lists untracked files that should be included in a checkpoint, skipping
/// commonly ignored file types and files over 2MB.
async fn untracked_files_for_checkpoint(git: &GitBinary) -> Result<Vec<String>> {
    const MAX_SIZE: u64 = 2 * 1024 * 1024; // 2 MB

    // The extra checkpoint excludes are passed ad hoc via --exclude-from
    // rather than by mutating .git/info/exclude, whose writes would trigger a
    // rescan of the repository. The scratch file is placed directly in the
    // .git directory with a .tmp extension so that the worktree scanner
    // filters out the events it generates.
    let excludes_file_path = git
        .git_directory
        .join(format!("checkpoint-excludes-{}.tmp", Uuid::new_v4()));

    let delete_excludes_file = util::defer({
        let excludes_file_path = excludes_file_path.clone();
        let executor = git.executor.clone();
        move || {
            executor
                .spawn(async move {
                    smol::fs::remove_file(excludes_file_path).await.log_err();
                })
                .detach();
        }
    });

    smol::fs::write(&excludes_file_path, include_str!("./checkpoint.gitignore")).await?;

    let mut exclude_from_arg = OsString::from("--exclude-from=");
    exclude_from_arg.push(&excludes_file_path);
    let output = git
        .run(&[
            OsStr::new("ls-files"),
            OsStr::new("--others"),
            OsStr::new("--exclude-standard"),
            OsStr::new("-z"),
            exclude_from_arg.as_os_str(),
        ])
        .await;

    smol::fs::remove_file(&excludes_file_path).await.ok();
    delete_excludes_file.abort();
    let output = output?;

    let working_directory = git.working_directory.clone();
    let size_checks = output
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(|path| {
            let full_path = working_directory.join(path);
            let path = path.to_string();
            smol::spawn(async move {
                match smol::fs::metadata(&full_path).await {
                    Ok(metadata) if metadata.is_file() && metadata.len() >= MAX_SIZE => None,
                    _ => Some(path),
                }
            })
        })
        .collect::<Vec<_>>();

    let untracked_files = futures::future::join_all(size_checks)
        .await
        .into_iter()
        .flatten()
        .collect();
    Ok(untracked_files)
}

async fn add_files_to_index(git: &GitBinary, files: &[String]) -> Result<()> {
    if files.is_empty() {
        return Ok(());
    }

    let mut process = git
        .build_command(&["update-index", "--add", "-z", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    let mut stdin = BufWriter::new(
        process
            .stdin
            .take()
            .context("no stdin for git update-index subprocess")?,
    );
    for file in files {
        stdin.write_all(file.as_bytes()).await?;
        stdin.write_all(b"\0").await?;
    }
    stdin.flush().await?;
    drop(stdin);

    let output = process.output().await?;
    anyhow::ensure!(
        output.status.success(),
        GitBinaryCommandError {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            status: output.status,
        }
    );
    Ok(())
}

pub(crate) struct GitBinary {
    git_binary_path: PathBuf,
    working_directory: PathBuf,
    git_directory: PathBuf,
    executor: BackgroundExecutor,
    index_file_path: Option<PathBuf>,
    envs: HashMap<String, String>,
    is_trusted: bool,
}

impl GitBinary {
    pub(crate) fn new(
        git_binary_path: PathBuf,
        working_directory: PathBuf,
        git_directory: PathBuf,
        executor: BackgroundExecutor,
        is_trusted: bool,
    ) -> Self {
        Self {
            git_binary_path,
            working_directory,
            git_directory,
            executor,
            index_file_path: None,
            envs: HashMap::default(),
            is_trusted,
        }
    }

    fn envs(mut self, envs: HashMap<String, String>) -> Self {
        self.envs = envs;
        self
    }

    pub async fn with_temp_index<R>(
        &mut self,
        f: impl AsyncFnOnce(&Self) -> Result<R>,
    ) -> Result<R> {
        let index_file_path = self.path_for_index_id(Uuid::new_v4());

        let delete_temp_index = util::defer({
            let index_file_path = index_file_path.clone();
            let executor = self.executor.clone();
            move || {
                executor
                    .spawn(async move {
                        smol::fs::remove_file(index_file_path).await.log_err();
                    })
                    .detach();
            }
        });

        // Copy the default index file so that Git doesn't have to rebuild the
        // whole index from scratch. This might fail if this is an empty repository.
        smol::fs::copy(self.git_directory.join("index"), &index_file_path)
            .await
            .ok();

        self.index_file_path = Some(index_file_path.clone());
        let result = f(self).await;
        self.index_file_path = None;
        let result = result?;

        smol::fs::remove_file(index_file_path).await.ok();
        delete_temp_index.abort();

        Ok(result)
    }

    fn path_for_index_id(&self, id: Uuid) -> PathBuf {
        self.git_directory.join(format!("index-{}.tmp", id))
    }

    pub async fn run<S>(&self, args: &[S]) -> Result<String>
    where
        S: AsRef<OsStr>,
    {
        let mut stdout = self.run_raw(args).await?;
        if stdout.chars().last() == Some('\n') {
            stdout.pop();
        }
        Ok(stdout)
    }

    /// Returns the result of the command without trimming the trailing newline.
    pub async fn run_raw<S>(&self, args: &[S]) -> Result<String>
    where
        S: AsRef<OsStr>,
    {
        let mut command = self.build_command(args);
        let output = command.output().await?;
        anyhow::ensure!(
            output.status.success(),
            GitBinaryCommandError {
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                status: output.status,
            }
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    #[allow(clippy::disallowed_methods)]
    pub(crate) fn build_command<S>(&self, args: &[S]) -> util::command::Command
    where
        S: AsRef<OsStr>,
    {
        let mut command = new_command(&self.git_binary_path);
        command.current_dir(&self.working_directory);
        // Disabled to stop malicious actors from running arbitrary commands via fsmonitor hooks
        command.args(["-c", "core.fsmonitor=false"]);
        // Prepended signature lines would corrupt our --format parsers.
        command.args(["-c", "log.showSignature=false"]);
        command.arg("--no-optional-locks");
        // Internal commands must be non-interactive so background tasks never block on user input.
        command.arg("--no-pager");

        if !self.is_trusted {
            command.args(["-c", "core.hooksPath=/dev/null"]);
            command.args(["-c", "core.sshCommand=ssh"]);
            command.args(["-c", "credential.helper="]);
            command.args(["-c", "protocol.ext.allow=never"]);
            command.args(["-c", "diff.external="]);
        }
        command.args(args);

        // If the `diff` command is being used, we'll want to add the
        // `--no-ext-diff` flag when working on an untrusted repository,
        // preventing any external diff programs from being invoked.
        if !self.is_trusted && args.iter().any(|arg| arg.as_ref() == "diff") {
            command.arg("--no-ext-diff");
        }

        if let Some(index_file_path) = self.index_file_path.as_ref() {
            command.env("GIT_INDEX_FILE", index_file_path);
        }
        command.envs(&self.envs);
        command
    }
}

#[derive(Error, Debug)]
#[error("Git command failed:\n{stdout}{stderr}\n")]
struct GitBinaryCommandError {
    stdout: String,
    stderr: String,
    status: ExitStatus,
}

/// Maps a `git cat-file -t` object-type string to its typed equivalent.
fn tag_object_type(raw: &str) -> Result<TagObjectType> {
    match raw {
        "commit" => Ok(TagObjectType::Commit),
        "tag" => Ok(TagObjectType::Tag),
        "tree" => Ok(TagObjectType::Tree),
        "blob" => Ok(TagObjectType::Blob),
        other => anyhow::bail!("unexpected tag object type {other:?}"),
    }
}

/// Parses the tagger metadata and message out of an annotated tag object body
/// (`git cat-file -p` output). An annotated tag body has a fixed, ordered
/// header block (`object`, `type`, `tag`, `tagger`) terminated by a single
/// blank line, after which the message runs to the end of the body. The blank
/// line is the only delimiter relied on, so a message containing arbitrary
/// lines or any other text is preserved verbatim (no scanning for keywords or
/// custom delimiters inside the message).
///
/// Returns `(tagger, message)`; both are `None` when the corresponding part is
/// absent.
fn parse_tag_body(body: &str) -> (Option<TagTagger>, Option<String>) {
    let (header_block, message) = match body.split_once("\n\n") {
        Some((header, message)) => (header, Some(message.trim_end().to_string())),
        None => (body, None),
    };

    let tagger = header_block
        .lines()
        .find_map(|line| line.strip_prefix("tagger "))
        .map(parse_tagger_line);

    (tagger, message)
}

/// Parses a `tagger Name <email> <unixtime> <tz>` header line. The tagger line
/// format is fixed by Git, so the name is everything before the first ` <` and
/// the rest is `<email> <time> [<tz>]`.
fn parse_tagger_line(line: &str) -> TagTagger {
    let (name, rest) = match line.find(" <") {
        Some(idx) => (&line[..idx], &line[idx + 1..]),
        None => (line, ""),
    };
    let email = rest
        .trim_start_matches('<')
        .split('>')
        .next()
        .unwrap_or("")
        .to_string();
    // After `<email> ` comes the unix timestamp (optionally followed by a
    // timezone offset that Git accepts but does not require).
    let time = rest
        .split('>')
        .nth(1)
        .and_then(|after| after.trim().split_whitespace().next())
        .and_then(|ts| ts.parse::<i64>().ok())
        .unwrap_or(0);
    TagTagger {
        name: name.trim().into(),
        email: email.into(),
        time,
    }
}

async fn run_git_command(
    env: Arc<HashMap<String, String>>,
    ask_pass: AskPassDelegate,
    mut command: util::command::Command,
    executor: BackgroundExecutor,
) -> Result<RemoteCommandOutput> {
    if env.contains_key("GIT_ASKPASS") {
        let git_process = command.spawn()?;
        let output = git_process.output().await?;
        anyhow::ensure!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(RemoteCommandOutput {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    } else {
        let ask_pass = AskPassSession::new(executor, ask_pass).await?;
        command
            .env("GIT_ASKPASS", ask_pass.script_path())
            .env("SSH_ASKPASS", ask_pass.script_path())
            .env("SSH_ASKPASS_REQUIRE", "force");

        if !env.contains_key("GIT_CONFIG_COUNT")
            && let Some(gpg_wrapper) = ask_pass.gpg_wrapper_path()
        {
            command
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "gpg.program")
                .env("GIT_CONFIG_VALUE_0", gpg_wrapper);
        }

        #[cfg(target_os = "windows")]
        command.env("ZED_ASKPASS_SOCKET", ask_pass.socket_path());
        let git_process = command.spawn()?;

        run_askpass_command(ask_pass, git_process).await
    }
}

async fn run_askpass_command(
    mut ask_pass: AskPassSession,
    git_process: util::command::Child,
) -> anyhow::Result<RemoteCommandOutput> {
    select_biased! {
        // Git can legitimately run long without prompting (e.g. large fetches,
        // hooks), so completion is determined by the process itself.
        result = ask_pass.run(None).fuse() => {
            match result {
                AskPassResult::CancelledByUser => {
                    Err(anyhow!(REMOTE_CANCELLED_BY_USER))?
                }
                AskPassResult::Timedout => {
                    // Unreachable since no timeout is passed to run()
                    Err(anyhow!("Connecting to host timed out"))?
                }
            }
        }
        output = git_process.output().fuse() => {
            let output = output?;
            anyhow::ensure!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            Ok(RemoteCommandOutput {
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            })
        }
    }
}

#[derive(Clone, Ord, Hash, PartialOrd, Eq, PartialEq)]
pub struct RepoPath(Arc<RelPath>);

impl std::fmt::Debug for RepoPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl RepoPath {
    pub fn new<S: AsRef<str> + ?Sized>(s: &S) -> Result<Self> {
        let rel_path = RelPath::from_unix_str(s.as_ref())?;
        Ok(Self::from_rel_path(rel_path))
    }

    pub fn from_std_path(path: &Path, path_style: PathStyle) -> Result<Self> {
        let rel_path = RelPath::new(path, path_style)?;
        Ok(Self::from_rel_path(&rel_path))
    }

    pub fn from_proto(proto: &str) -> Result<Self> {
        let rel_path = RelPath::from_unix_str(proto)?.into();
        Ok(Self(rel_path))
    }

    pub fn from_rel_path(path: &RelPath) -> RepoPath {
        Self(Arc::from(path))
    }

    pub fn as_std_path(&self) -> &Path {
        if self.is_empty() {
            Path::new(".")
        } else {
            self.0.as_std_path()
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn repo_path<S: AsRef<str> + ?Sized>(s: &S) -> RepoPath {
    RepoPath(RelPath::from_unix_str(s.as_ref()).unwrap().into())
}

impl AsRef<Arc<RelPath>> for RepoPath {
    fn as_ref(&self) -> &Arc<RelPath> {
        &self.0
    }
}

impl std::ops::Deref for RepoPath {
    type Target = RelPath;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct RepoPathDescendants<'a>(pub &'a RepoPath);

impl MapSeekTarget<RepoPath> for RepoPathDescendants<'_> {
    fn cmp_cursor(&self, key: &RepoPath) -> Ordering {
        if key.starts_with(self.0) {
            Ordering::Greater
        } else {
            self.0.cmp(key)
        }
    }
}

fn parse_branch_input(input: &str) -> Result<Vec<Branch>> {
    let mut branches = Vec::new();
    for line in input.split('\n') {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split('\x00');
        let Some(head) = fields.next() else {
            continue;
        };
        let Some(head_sha) = fields.next().map(|f| f.to_string().into()) else {
            continue;
        };
        let Some(parent_sha) = fields.next().map(|f| f.to_string()) else {
            continue;
        };
        let Some(ref_name) = fields.next().map(|f| f.to_string().into()) else {
            continue;
        };
        let Some(upstream_name) = fields.next().map(|f| f.to_string()) else {
            continue;
        };
        let Some(upstream_tracking) = fields.next().and_then(|f| parse_upstream_track(f).ok())
        else {
            continue;
        };
        let Some(commiterdate) = fields.next().and_then(|f| f.parse::<i64>().ok()) else {
            continue;
        };
        let Some(author_name) = fields.next().map(|f| f.to_string().into()) else {
            continue;
        };
        let Some(subject) = fields.next().map(|f| f.to_string().into()) else {
            continue;
        };

        branches.push(Branch {
            is_head: head == "*",
            ref_name,
            most_recent_commit: Some(CommitSummary {
                sha: head_sha,
                subject,
                commit_timestamp: commiterdate,
                author_name: author_name,
                has_parent: !parent_sha.is_empty(),
            }),
            upstream: if upstream_name.is_empty() {
                None
            } else {
                Some(Upstream {
                    ref_name: upstream_name.into(),
                    tracking: upstream_tracking,
                })
            },
        })
    }

    Ok(branches)
}

fn format_branch_scan_error(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr)
        .trim()
        .replace('\n', " ");
    if stderr.is_empty() {
        format!("git for-each-ref exited with {}", output.status)
    } else {
        stderr
    }
}

fn parse_upstream_track(upstream_track: &str) -> Result<UpstreamTracking> {
    if upstream_track.is_empty() {
        return Ok(UpstreamTracking::Tracked(UpstreamTrackingStatus {
            ahead: 0,
            behind: 0,
        }));
    }

    let upstream_track = upstream_track.strip_prefix("[").context("missing [")?;
    let upstream_track = upstream_track.strip_suffix("]").context("missing [")?;
    let mut ahead: u32 = 0;
    let mut behind: u32 = 0;
    for component in upstream_track.split(", ") {
        if component == "gone" {
            return Ok(UpstreamTracking::Gone);
        }
        if let Some(ahead_num) = component.strip_prefix("ahead ") {
            ahead = ahead_num.parse::<u32>()?;
        }
        if let Some(behind_num) = component.strip_prefix("behind ") {
            behind = behind_num.parse::<u32>()?;
        }
    }
    Ok(UpstreamTracking::Tracked(UpstreamTrackingStatus {
        ahead,
        behind,
    }))
}

fn checkpoint_author_envs() -> HashMap<String, String> {
    HashMap::from_iter([
        ("GIT_AUTHOR_NAME".to_string(), "Zed".to_string()),
        ("GIT_AUTHOR_EMAIL".to_string(), "hi@zed.dev".to_string()),
        ("GIT_COMMITTER_NAME".to_string(), "Zed".to_string()),
        ("GIT_COMMITTER_EMAIL".to_string(), "hi@zed.dev".to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{OsStr, OsString},
        fs,
    };

    use super::*;
    use gpui::TestAppContext;

    fn disable_git_global_config() {
        unsafe {
            std::env::set_var("GIT_CONFIG_GLOBAL", "");
            std::env::set_var("GIT_CONFIG_SYSTEM", "");
        }
    }

    #[allow(clippy::disallowed_methods)]
    #[track_caller]
    fn git_command_output<I, S>(working_directory: &Path, arguments: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(working_directory)
            .env("GIT_CONFIG_GLOBAL", "")
            .env("GIT_CONFIG_SYSTEM", "")
            .env("GIT_AUTHOR_NAME", "test")
            .env("GIT_AUTHOR_EMAIL", "test@zed.dev")
            .env("GIT_COMMITTER_NAME", "test")
            .env("GIT_COMMITTER_EMAIL", "test@zed.dev")
            .output()
            .expect("failed to run git command");
        assert!(
            output.status.success(),
            "git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("git command output was not valid UTF-8")
            .trim()
            .to_string()
    }

    #[track_caller]
    fn git_command<I, S>(working_directory: &Path, arguments: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        git_command_output(working_directory, arguments);
    }

    #[allow(clippy::disallowed_methods)]
    #[track_caller]
    fn git_command_failure<I, S>(working_directory: &Path, arguments: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(working_directory)
            .env("GIT_CONFIG_GLOBAL", "")
            .env("GIT_CONFIG_SYSTEM", "")
            .env("GIT_AUTHOR_NAME", "test")
            .env("GIT_AUTHOR_EMAIL", "test@zed.dev")
            .env("GIT_COMMITTER_NAME", "test")
            .env("GIT_COMMITTER_EMAIL", "test@zed.dev")
            .output()
            .expect("failed to run git command");
        assert!(
            !output.status.success(),
            "git command unexpectedly succeeded"
        );
    }

    fn git_init_repo(path: &Path) {
        fs::create_dir_all(path).expect("failed to create repo directory");
        git_command(path, ["init", "-b", "main"]);
    }

    fn clone_remote_repository_with_main_and_feature(temp_dir: &Path) -> (PathBuf, PathBuf) {
        let remote_directory = temp_dir.join("remote.git");
        let seed_directory = temp_dir.join("seed");
        let clone_directory = temp_dir.join("clone");

        git_command(
            temp_dir,
            [
                OsString::from("init"),
                OsString::from("--bare"),
                OsString::from("-b"),
                OsString::from("main"),
                remote_directory.as_os_str().into(),
            ],
        );
        git_init_repo(&seed_directory);
        fs::write(seed_directory.join("file.txt"), "main").unwrap();
        git_command(&seed_directory, ["add", "file.txt"]);
        git_command(&seed_directory, ["commit", "-m", "initial"]);
        git_command(&seed_directory, ["switch", "-c", "feature"]);
        fs::write(seed_directory.join("feature.txt"), "feature").unwrap();
        git_command(&seed_directory, ["add", "feature.txt"]);
        git_command(&seed_directory, ["commit", "-m", "feature"]);
        git_command(
            &seed_directory,
            [
                OsString::from("remote"),
                OsString::from("add"),
                OsString::from("origin"),
                remote_directory.as_os_str().into(),
            ],
        );
        git_command(&seed_directory, ["push", "-u", "origin", "main"]);
        git_command(&seed_directory, ["push", "-u", "origin", "feature"]);
        git_command(
            temp_dir,
            [
                OsString::from("clone"),
                remote_directory.as_os_str().into(),
                clone_directory.as_os_str().into(),
            ],
        );

        (remote_directory, clone_directory)
    }

    fn test_commit_envs() -> HashMap<String, String> {
        let mut env = checkpoint_author_envs();
        env.insert("GIT_ASKPASS".to_string(), "false".to_string());
        env
    }

    #[track_caller]
    fn assert_same_path(left: impl AsRef<Path>, right: impl AsRef<Path>) {
        assert_eq!(
            fs::canonicalize(left.as_ref()).unwrap(),
            fs::canonicalize(right.as_ref()).unwrap()
        );
    }

    #[gpui::test]
    async fn test_real_git_repository_new_resolves_normal_repository_paths(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        assert_same_path(&repository.git_dir, repo_dir.path().join(".git"));
        assert_same_path(&repository.common_dir, repo_dir.path().join(".git"));
        assert_same_path(
            repository.working_directory.as_ref().unwrap(),
            repo_dir.path(),
        );
        assert_same_path(
            original_repo_path_from_common_dir(&repository.common_dir).unwrap(),
            repo_dir.path(),
        );
    }

    #[gpui::test]
    async fn test_merge_base_worktree_diff_handles_recreated_index_deletion(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let file_path = repo_dir.path().join("file.txt");
        fs::write(&file_path, "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let base_oid = git_command_output(repo_dir.path(), ["rev-parse", "HEAD:file.txt"])
            .parse()
            .unwrap();

        fs::write(&file_path, "head\n").unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "head"]);
        git_command(repo_dir.path(), ["rm", "--cached", "file.txt"]);
        fs::write(&file_path, "base\n").unwrap();

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::default(),
            }
        );

        fs::write(&file_path, "worktree\n").unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::from_iter([(
                    RepoPath::new("file.txt").unwrap(),
                    TreeDiffStatus::Modified { old: base_oid },
                )]),
            }
        );
    }

    #[gpui::test]
    async fn test_merge_base_worktree_diff_handles_committed_deletion_recreated_on_disk(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let file_path = repo_dir.path().join("file.txt");
        fs::write(&file_path, "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let base_oid = git_command_output(repo_dir.path(), ["rev-parse", "HEAD:file.txt"])
            .parse()
            .unwrap();

        git_command(repo_dir.path(), ["rm", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "delete"]);
        fs::write(&file_path, "base\n").unwrap();

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::default(),
            }
        );

        fs::write(&file_path, "worktree\n").unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::from_iter([(
                    RepoPath::new("file.txt").unwrap(),
                    TreeDiffStatus::Modified { old: base_oid },
                )]),
            }
        );
    }

    #[cfg(unix)]
    #[gpui::test]
    async fn test_merge_base_worktree_diff_handles_recreated_symlink(cx: &mut TestAppContext) {
        use std::os::unix::fs::symlink;

        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let file_path = repo_dir.path().join("file.txt");
        symlink("base-target", &file_path).unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let base_oid = git_command_output(repo_dir.path(), ["rev-parse", "HEAD:file.txt"])
            .parse()
            .unwrap();

        fs::remove_file(&file_path).unwrap();
        symlink("head-target", &file_path).unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "head"]);
        git_command(repo_dir.path(), ["rm", "--cached", "file.txt"]);
        fs::remove_file(&file_path).unwrap();
        symlink("base-target", &file_path).unwrap();

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::default(),
            }
        );

        fs::remove_file(&file_path).unwrap();
        fs::write(&file_path, "base-target").unwrap();
        assert_eq!(
            repository
                .diff_tree(DiffTreeType::MergeBaseWithWorktree {
                    base: "HEAD^".into(),
                })
                .await
                .unwrap(),
            TreeDiff {
                entries: HashMap::from_iter([(
                    RepoPath::new("file.txt").unwrap(),
                    TreeDiffStatus::Modified { old: base_oid },
                )]),
            }
        );
    }

    #[gpui::test]
    async fn test_load_commit_with_type_changed_file(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().expect("failed to create temporary repository");
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("file.txt"), "regular contents\n")
            .expect("failed to write regular file");
        git_command(repo_dir.path(), ["add", "file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "initial"]);

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .expect("failed to open repository");
        fs::write(repo_dir.path().join("file.txt"), "target")
            .expect("failed to write symlink target");

        let symlink_blob = repository
            .git_binary()
            .run(&["hash-object", "-w", "file.txt"])
            .await
            .expect("failed to write symlink blob");
        git_command(
            repo_dir.path(),
            [
                OsString::from("update-index"),
                OsString::from("--cacheinfo"),
                OsString::from("120000"),
                OsString::from(symlink_blob),
                OsString::from("file.txt"),
            ],
        );
        git_command(repo_dir.path(), ["commit", "-m", "type change"]);

        let commit_diff = repository
            .load_commit("HEAD".to_string(), false, cx.to_async())
            .await
            .expect("failed to load type-changed commit");
        assert_eq!(commit_diff.files.len(), 1);

        let file = commit_diff
            .files
            .first()
            .expect("type-changed file should be present");
        assert_eq!(file.path.as_unix_str(), "file.txt");
        assert_eq!(
            file.old_content.as_deref(),
            Some(b"regular contents\n".as_slice())
        );
        assert_eq!(file.new_content.as_deref(), Some(b"target".as_slice()));
        assert_eq!(file.status(), CommitFileStatus::Modified);
    }

    #[gpui::test]
    async fn test_load_commit_with_gitlink_changes(cx: &mut TestAppContext) {
        const FIRST_SUBMODULE_COMMIT: &str = "1111111111111111111111111111111111111111";
        const SECOND_SUBMODULE_COMMIT: &str = "2222222222222222222222222222222222222222";

        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().expect("failed to create temporary repository");
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("README.md"), "parent repository\n")
            .expect("failed to write regular file");
        git_command(repo_dir.path(), ["add", "README.md"]);
        git_command(
            repo_dir.path(),
            [
                "update-index",
                "--add",
                "--cacheinfo",
                crate::commit::GITLINK_MODE,
                FIRST_SUBMODULE_COMMIT,
                "modules/example",
            ],
        );
        git_command(repo_dir.path(), ["commit", "-m", "add submodule"]);

        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .expect("failed to open repository");

        let commit_diff = repository
            .load_commit("HEAD".to_string(), false, cx.to_async())
            .await
            .expect("failed to load commit that adds a gitlink");
        assert_eq!(commit_diff.files.len(), 2);
        let gitlink = commit_diff
            .files
            .iter()
            .find(|file| file.path.as_unix_str() == "modules/example")
            .expect("gitlink should be present alongside the regular file");
        assert_eq!(gitlink.status(), CommitFileStatus::Added);
        assert_eq!(gitlink.old_content, None);
        assert_eq!(
            gitlink.new_content.as_deref(),
            Some(b"Subproject commit 1111111111111111111111111111111111111111\n".as_slice())
        );
        assert!(!gitlink.is_binary);

        git_command(
            repo_dir.path(),
            [
                "update-index",
                "--cacheinfo",
                crate::commit::GITLINK_MODE,
                SECOND_SUBMODULE_COMMIT,
                "modules/example",
            ],
        );
        git_command(repo_dir.path(), ["commit", "-m", "update submodule"]);

        let commit_diff = repository
            .load_commit("HEAD".to_string(), false, cx.to_async())
            .await
            .expect("failed to load commit that updates a gitlink");
        let [gitlink] = commit_diff.files.as_slice() else {
            panic!("expected one updated gitlink");
        };
        assert_eq!(gitlink.status(), CommitFileStatus::Modified);
        assert_eq!(
            gitlink.old_content.as_deref(),
            Some(b"Subproject commit 1111111111111111111111111111111111111111\n".as_slice())
        );
        assert_eq!(
            gitlink.new_content.as_deref(),
            Some(b"Subproject commit 2222222222222222222222222222222222222222\n".as_slice())
        );
        assert!(!gitlink.is_binary);

        git_command(repo_dir.path(), ["rm", "--cached", "modules/example"]);
        git_command(repo_dir.path(), ["commit", "-m", "remove submodule"]);

        let commit_diff = repository
            .load_commit("HEAD".to_string(), false, cx.to_async())
            .await
            .expect("failed to load commit that deletes a gitlink");
        let [gitlink] = commit_diff.files.as_slice() else {
            panic!("expected one deleted gitlink");
        };
        assert_eq!(gitlink.status(), CommitFileStatus::Deleted);
        assert_eq!(
            gitlink.old_content.as_deref(),
            Some(b"Subproject commit 2222222222222222222222222222222222222222\n".as_slice())
        );
        assert_eq!(gitlink.new_content, None);
        assert!(!gitlink.is_binary);
    }

    #[gpui::test]
    async fn test_load_commit_shallow_boundary(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let source_dir = tempfile::tempdir().expect("failed to create source repository");
        git_init_repo(source_dir.path());
        fs::write(source_dir.path().join("a.txt"), "one\n").expect("failed to write a.txt");
        git_command(source_dir.path(), ["add", "a.txt"]);
        git_command(source_dir.path(), ["commit", "-m", "first"]);
        fs::write(source_dir.path().join("a.txt"), "two\n").expect("failed to update a.txt");
        fs::write(source_dir.path().join("b.txt"), "new\n").expect("failed to write b.txt");
        git_command(source_dir.path(), ["add", "a.txt", "b.txt"]);
        git_command(source_dir.path(), ["commit", "-m", "second"]);

        let clone_dir = tempfile::tempdir().expect("failed to create clone directory");
        git_command(
            clone_dir.path(),
            [
                "clone".to_string(),
                "--depth=1".to_string(),
                format!("file://{}", source_dir.path().display()),
                "shallow".to_string(),
            ],
        );

        let repository = RealGitRepository::new(
            &clone_dir.path().join("shallow").join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .expect("failed to open shallow repository");

        let commit_diff = repository
            .load_commit("HEAD".to_string(), false, cx.to_async())
            .await
            .expect("failed to load boundary commit");
        assert!(commit_diff.is_shallow_boundary);
        assert_eq!(commit_diff.files.len(), 0);

        let commit_diff = repository
            .load_commit("HEAD".to_string(), true, cx.to_async())
            .await
            .expect("failed to load boundary commit snapshot");
        assert!(!commit_diff.is_shallow_boundary);
        let files = commit_diff
            .files
            .iter()
            .map(|file| {
                (
                    file.path.as_unix_str().to_owned(),
                    file.old_content.clone(),
                    file.status(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            files,
            vec![
                ("a.txt".to_string(), None, CommitFileStatus::Added),
                ("b.txt".to_string(), None, CommitFileStatus::Added),
            ]
        );

        let source_repository = RealGitRepository::new(
            &source_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .expect("failed to open source repository");
        let commit_diff = source_repository
            .load_commit("HEAD~1".to_string(), false, cx.to_async())
            .await
            .expect("failed to load root commit");
        assert!(!commit_diff.is_shallow_boundary);
        assert_eq!(commit_diff.files.len(), 1);
    }

    #[gpui::test]
    async fn test_check_access(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        let repository = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        assert!(repository.check_access().await.is_err());
        git_init_repo(repo_dir.path());
        assert!(repository.check_access().await.is_ok());
    }

    #[gpui::test]
    async fn test_real_git_repository_new_resolves_linked_worktree_paths(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        let worktree_dir = temp_dir.path().join("worktree");
        git_init_repo(&repo_dir);
        fs::write(repo_dir.join("file.txt"), "initial").unwrap();
        git_command(&repo_dir, ["add", "file.txt"]);
        git_command(&repo_dir, ["commit", "-m", "initial"]);
        git_command(
            &repo_dir,
            [
                OsString::from("worktree"),
                OsString::from("add"),
                OsString::from("-b"),
                OsString::from("feature"),
                worktree_dir.as_os_str().into(),
            ],
        );

        let repository = RealGitRepository::new(
            &worktree_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        assert_same_path(
            repository.working_directory.as_ref().unwrap(),
            &worktree_dir,
        );
        assert_same_path(&repository.common_dir, repo_dir.join(".git"));
        assert_same_path(
            original_repo_path_from_common_dir(&repository.common_dir).unwrap(),
            repo_dir,
        );
    }

    #[gpui::test]
    async fn test_real_git_repository_new_supports_bare_repositories(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo.git");
        git_command(
            temp_dir.path(),
            [
                OsString::from("init"),
                OsString::from("--bare"),
                repo_dir.as_os_str().into(),
            ],
        );

        let repository =
            RealGitRepository::new(&repo_dir, None, Some("git".into()), cx.executor()).unwrap();

        assert_same_path(&repository.git_dir, &repo_dir);
        assert_same_path(&repository.common_dir, &repo_dir);
        assert_eq!(repository.working_directory, None);
        assert_same_path(repository.main_repository_path(), &repo_dir);
        assert_eq!(
            repository
                .git_binary()
                .run(&["rev-parse", "--is-bare-repository"])
                .await
                .unwrap(),
            "true"
        );
    }

    #[gpui::test]
    async fn test_change_branch_creates_local_tracking_branch_from_remote(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let (_remote_directory, clone_directory) =
            clone_remote_repository_with_main_and_feature(temp_dir.path());

        let repository = RealGitRepository::new(
            &clone_directory.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        let git = repository.git_binary_in_worktree().unwrap();
        assert!(
            git.run(&[
                "show-ref",
                "--verify",
                "--quiet",
                "refs/remotes/origin/feature"
            ])
            .await
            .is_ok()
        );
        assert!(
            git.run(&["show-ref", "--verify", "--quiet", "refs/heads/feature"])
                .await
                .is_err()
        );

        repository
            .change_branch("origin/feature".to_string())
            .await
            .unwrap();

        let git = repository.git_binary_in_worktree().unwrap();
        assert_eq!(
            git.run(&["branch", "--show-current"]).await.unwrap(),
            "feature"
        );
        assert_eq!(
            git.run(&["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}",])
                .await
                .unwrap(),
            "origin/feature"
        );

        git.run(&["checkout", "main"]).await.unwrap();
        git.run(&["branch", "--unset-upstream", "feature"])
            .await
            .unwrap();

        repository
            .change_branch("origin/feature".to_string())
            .await
            .unwrap();

        let git = repository.git_binary_in_worktree().unwrap();
        assert_eq!(
            git.run(&["branch", "--show-current"]).await.unwrap(),
            "feature"
        );
        assert_eq!(
            git.run(&["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}",])
                .await
                .unwrap(),
            "origin/feature"
        );
    }

    #[gpui::test]
    async fn test_change_branch_resolves_remote_head_to_tracking_branch(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let (_remote_directory, clone_directory) =
            clone_remote_repository_with_main_and_feature(temp_dir.path());

        let repository = RealGitRepository::new(
            &clone_directory.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        let git = repository.git_binary_in_worktree().unwrap();
        git.run(&[
            "symbolic-ref",
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/feature",
        ])
        .await
        .unwrap();
        assert_eq!(
            git.run(&["symbolic-ref", "refs/remotes/origin/HEAD"])
                .await
                .unwrap(),
            "refs/remotes/origin/feature"
        );
        assert!(
            git.run(&["show-ref", "--verify", "--quiet", "refs/heads/feature"])
                .await
                .is_err()
        );

        repository
            .change_branch("origin/HEAD".to_string())
            .await
            .unwrap();

        let git = repository.git_binary_in_worktree().unwrap();
        assert_eq!(
            git.run(&["branch", "--show-current"]).await.unwrap(),
            "feature"
        );
        assert_eq!(
            git.run(&["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}",])
                .await
                .unwrap(),
            "origin/feature"
        );
    }

    #[gpui::test]
    async fn test_change_branch_resolves_non_origin_remote_head(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let (remote_directory, clone_directory) =
            clone_remote_repository_with_main_and_feature(temp_dir.path());

        git_command(
            &clone_directory,
            [
                OsString::from("remote"),
                OsString::from("add"),
                OsString::from("upstream"),
                remote_directory.as_os_str().into(),
            ],
        );
        git_command(&clone_directory, ["fetch", "upstream"]);

        let repository = RealGitRepository::new(
            &clone_directory.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        let git = repository.git_binary_in_worktree().unwrap();
        git.run(&[
            "symbolic-ref",
            "refs/remotes/upstream/HEAD",
            "refs/remotes/upstream/main",
        ])
        .await
        .unwrap();
        git.run(&["checkout", "-b", "scratch"]).await.unwrap();

        repository
            .change_branch("upstream/HEAD".to_string())
            .await
            .unwrap();

        let git = repository.git_binary_in_worktree().unwrap();
        assert_eq!(
            git.run(&["branch", "--show-current"]).await.unwrap(),
            "main"
        );
        assert_eq!(
            git.run(&["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}",])
                .await
                .unwrap(),
            "upstream/main"
        );
    }

    #[gpui::test]
    fn test_real_git_repository_new_rejects_malformed_git_file(cx: &mut TestAppContext) {
        disable_git_global_config();

        let temp_dir = tempfile::tempdir().unwrap();
        let worktree_dir = temp_dir.path().join("worktree");
        fs::create_dir_all(&worktree_dir).unwrap();
        fs::write(worktree_dir.join(".git"), "not a gitdir file\n").unwrap();

        let error = match RealGitRepository::new(
            &worktree_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        ) {
            Ok(_) => panic!("malformed .git file should be rejected"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("expected .git file to start with 'gitdir: '"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn test_initial_graph_commit_data_tag_names() {
        let commit = InitialGraphCommitData {
            sha: Oid::from_bytes(&[0; 20]).unwrap(),
            parents: SmallVec::new(),
            ref_names: vec![
                SharedString::from("HEAD -> refs/heads/main"),
                SharedString::from("refs/remotes/origin/main"),
                SharedString::from("tag: refs/tags/v1.0.0"),
                SharedString::from("tag: refs/tags/v1.1.0"),
                SharedString::from("tag: "),
                SharedString::from("refs/heads/feature"),
            ],
        };

        assert_eq!(commit.tag_names(), ["v1.0.0", "v1.1.0"]);
    }

    #[test]
    fn test_initial_graph_commit_data_tag_names_legacy_short_form() {
        // Graph data produced before the `--decorate=full` change (or directly
        // constructed) may carry the shortened `tag: <name>` decoration; the
        // accessor must keep stripping it so legacy consumers are unaffected.
        let commit = InitialGraphCommitData {
            sha: Oid::from_bytes(&[0; 20]).unwrap(),
            parents: SmallVec::new(),
            ref_names: vec![SharedString::from("tag: legacy-tag"), SharedString::from("main")],
        };

        assert_eq!(commit.tag_names(), ["legacy-tag"]);
    }

    #[test]
    fn test_parse_file_history_changed_files_output() {
        let queried_paths = vec![
            RepoPath::new("src/a.rs").unwrap(),
            RepoPath::new("src/b.rs").unwrap(),
        ];
        let output = concat!(
            "\x1e1111111111111111111111111111111111111111\0\nsrc/a.rs\0src/shared.rs\0",
            "\x1e2222222222222222222222222222222222222222\0\nsrc/b.rs\0src/shared.rs\0",
            "\x1e3333333333333333333333333333333333333333\0\nsrc/a.rs\0src/b.rs\0src/shared.rs\0",
        );

        let histories =
            parse_file_history_changed_files_output(output, &queried_paths, &HashSet::default());

        assert_eq!(histories.len(), 2);
        assert_eq!(
            histories[0].file_sets,
            vec![
                vec![
                    RepoPath::new("src/a.rs").unwrap(),
                    RepoPath::new("src/shared.rs").unwrap(),
                ],
                vec![
                    RepoPath::new("src/a.rs").unwrap(),
                    RepoPath::new("src/b.rs").unwrap(),
                    RepoPath::new("src/shared.rs").unwrap(),
                ],
            ]
        );
        assert_eq!(
            histories[1].file_sets,
            vec![
                vec![
                    RepoPath::new("src/b.rs").unwrap(),
                    RepoPath::new("src/shared.rs").unwrap(),
                ],
                vec![
                    RepoPath::new("src/a.rs").unwrap(),
                    RepoPath::new("src/b.rs").unwrap(),
                    RepoPath::new("src/shared.rs").unwrap(),
                ],
            ]
        );
    }

    #[test]
    fn test_parse_file_history_changed_files_output_skips_shallow_boundary() {
        let queried_paths = vec![RepoPath::new("src/a.rs").unwrap()];
        let output = concat!(
            "\x1e1111111111111111111111111111111111111111\0\nsrc/a.rs\0src/shared.rs\0",
            "\x1e2222222222222222222222222222222222222222\0\nsrc/a.rs\0src/b.rs\0src/shared.rs\0",
        );
        let shallow_boundary_oids =
            HashSet::from_iter(["2222222222222222222222222222222222222222".to_string()]);

        let histories =
            parse_file_history_changed_files_output(output, &queried_paths, &shallow_boundary_oids);

        assert_eq!(histories.len(), 1);
        assert_eq!(
            histories[0].file_sets,
            vec![vec![
                RepoPath::new("src/a.rs").unwrap(),
                RepoPath::new("src/shared.rs").unwrap(),
            ]]
        );
    }

    #[test]
    fn test_parse_initial_graph_output_filters_graft_decorations() {
        let line = "0f36a166633a057bf7dd660508d237cad2606cab\x00\x00grafted, HEAD -> refs/heads/main, refs/remotes/origin/main";
        let commits = parse_initial_graph_output([line].into_iter());
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].parents.len(), 0);
        assert_eq!(
            commits[0].ref_names,
            vec![
                SharedString::from("HEAD -> refs/heads/main"),
                SharedString::from("refs/remotes/origin/main"),
            ]
        );
    }

    #[gpui::test]
    async fn test_initial_graph_data_accepts_sha_log_source(cx: &mut TestAppContext) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();

        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("file"), "initial").unwrap();
        git_command(repo_dir.path(), ["add", "file"]);
        git_command(repo_dir.path(), ["commit", "-m", "Initial commit"]);

        let commit_sha: Oid = git_command_output(repo_dir.path(), ["rev-parse", "HEAD"])
            .parse()
            .unwrap();

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        let (request_tx, request_rx) = async_channel::unbounded();

        repo.initial_graph_data(LogSource::Sha(commit_sha), LogOrder::DateOrder, request_tx)
            .await
            .unwrap();

        let graph_data = request_rx.recv().await.unwrap();
        assert_eq!(graph_data.len(), 1);
        assert_eq!(graph_data[0].sha, commit_sha);
    }

    #[gpui::test]
    async fn test_build_command_untrusted_includes_both_safety_args(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let dir = tempfile::tempdir().unwrap();
        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            false,
        );
        let output = git
            .build_command(&["version"])
            .output()
            .await
            .expect("git version should succeed");
        assert!(output.status.success());

        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            false,
        );
        let output = git
            .build_command(&["config", "--get", "core.fsmonitor"])
            .output()
            .await
            .expect("git config should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            "false",
            "fsmonitor should be disabled for untrusted repos"
        );

        git_init_repo(dir.path());
        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            false,
        );
        let output = git
            .build_command(&["config", "--get", "core.hooksPath"])
            .output()
            .await
            .expect("git config should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            "/dev/null",
            "hooksPath should be /dev/null for untrusted repos"
        );
    }

    #[gpui::test]
    async fn test_build_command_trusted_only_disables_fsmonitor(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let dir = tempfile::tempdir().unwrap();
        git_init_repo(dir.path());

        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            true,
        );
        let output = git
            .build_command(&["config", "--get", "core.fsmonitor"])
            .output()
            .await
            .expect("git config should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            "false",
            "fsmonitor should be disabled even for trusted repos"
        );

        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            true,
        );
        let output = git
            .build_command(&["config", "--get", "core.hooksPath"])
            .output()
            .await
            .expect("git config should run");
        assert!(
            !output.status.success(),
            "hooksPath should NOT be overridden for trusted repos"
        );
    }

    #[gpui::test]
    async fn test_build_command_disables_log_show_signature(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let dir = tempfile::tempdir().unwrap();
        git_init_repo(dir.path());

        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            true,
        );
        let output = git
            .build_command(&["config", "--get", "log.showSignature"])
            .output()
            .await
            .expect("git config should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            "false",
            "log.showSignature should be disabled for trusted repos"
        );

        let git = GitBinary::new(
            PathBuf::from("git"),
            dir.path().to_path_buf(),
            dir.path().join(".git"),
            cx.executor(),
            false,
        );
        let output = git
            .build_command(&["config", "--get", "log.showSignature"])
            .output()
            .await
            .expect("git config should run");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert_eq!(
            stdout.trim(),
            "false",
            "log.showSignature should be disabled for untrusted repos"
        );
    }

    #[gpui::test]
    async fn test_path_for_index_id_uses_real_git_directory(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let working_directory = PathBuf::from("/code/worktree");
        let git_directory = PathBuf::from("/code/repo/.git/modules/worktree");
        let git = GitBinary::new(
            PathBuf::from("git"),
            working_directory,
            git_directory.clone(),
            cx.executor(),
            false,
        );

        let path = git.path_for_index_id(Uuid::nil());

        assert_eq!(
            path,
            git_directory.join(format!("index-{}.tmp", Uuid::nil()))
        );
    }

    #[gpui::test]
    async fn test_checkpoint_basic(cx: &mut TestAppContext) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();

        git_init_repo(repo_dir.path());
        let file_path = repo_dir.path().join("file");
        smol::fs::write(&file_path, "initial").await.unwrap();

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        repo.stage_paths(vec![repo_path("file")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        smol::fs::write(&file_path, "modified before checkpoint")
            .await
            .unwrap();
        smol::fs::write(repo_dir.path().join("new_file_before_checkpoint"), "1")
            .await
            .unwrap();
        let checkpoint = repo.checkpoint().await.unwrap();

        // Ensure the user can't see any branches after creating a checkpoint.
        assert_eq!(repo.branches().await.unwrap().branches.len(), 1);

        smol::fs::write(&file_path, "modified after checkpoint")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Commit after checkpoint".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        smol::fs::remove_file(repo_dir.path().join("new_file_before_checkpoint"))
            .await
            .unwrap();
        smol::fs::write(repo_dir.path().join("new_file_after_checkpoint"), "2")
            .await
            .unwrap();

        // Ensure checkpoint stays alive even after a Git GC.
        repo.gc().await.unwrap();
        repo.restore_checkpoint(checkpoint.clone()).await.unwrap();

        assert_eq!(
            smol::fs::read_to_string(&file_path).await.unwrap(),
            "modified before checkpoint"
        );
        assert_eq!(
            smol::fs::read_to_string(repo_dir.path().join("new_file_before_checkpoint"))
                .await
                .unwrap(),
            "1"
        );
        // See TODO above
        // assert_eq!(
        //     smol::fs::read_to_string(repo_dir.path().join("new_file_after_checkpoint"))
        //         .await
        //         .ok(),
        //     None
        // );
    }

    #[cfg(unix)]
    #[gpui::test]
    async fn test_commit_runs_git_hooks(cx: &mut TestAppContext) {
        use std::os::unix::fs::PermissionsExt as _;

        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        let hooks_dir = repo_dir.path().join(".git").join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();
        let write_hook = |name: &str, contents: &str| {
            let path = hooks_dir.join(name);
            fs::write(&path, contents).unwrap();
            fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        };

        write_hook("pre-commit", "#!/bin/sh\nexit 1\n");

        fs::write(repo_dir.path().join("file"), "one").unwrap();
        repo.stage_paths(vec![repo_path("file")], Arc::new(HashMap::default()))
            .await
            .unwrap();

        // Hooks must not run for untrusted repositories.
        repo.commit(
            "Commit in untrusted repo".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .expect("failing pre-commit hook should be skipped in untrusted repos");

        repo.set_trusted(true);

        fs::write(repo_dir.path().join("file"), "two").unwrap();
        repo.stage_paths(vec![repo_path("file")], Arc::new(HashMap::default()))
            .await
            .unwrap();

        repo.commit(
            "Commit blocked by hook".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .expect_err("failing pre-commit hook should abort the commit");

        write_hook("pre-commit", "#!/bin/sh\nexit 0\n");
        write_hook(
            "commit-msg",
            "#!/bin/sh\necho 'rewritten by commit-msg hook' > \"$1\"\n",
        );

        repo.commit(
            "Original message".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        let message = git_command_output(repo_dir.path(), ["log", "-1", "--pretty=%B"]);
        assert_eq!(message, "rewritten by commit-msg hook");

        write_hook("pre-commit", "#!/bin/sh\nexit 1\n");
        fs::write(repo_dir.path().join("file"), "three").unwrap();
        repo.stage_paths(vec![repo_path("file")], Arc::new(HashMap::default()))
            .await
            .unwrap();

        repo.commit(
            "Commit without verification".into(),
            None,
            CommitOptions {
                no_verify: true,
                ..Default::default()
            },
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .expect("--no-verify should skip pre-commit and commit-msg hooks");

        let message = git_command_output(repo_dir.path(), ["log", "-1", "--pretty=%B"]);
        assert_eq!(message, "Commit without verification");
    }

    #[gpui::test]
    async fn test_load_revisions(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());

        let file1_path = repo_dir.path().join("file1");
        let file2_path = repo_dir.path().join("file2");
        let space_file_path = repo_dir.path().join("file with spaces");

        smol::fs::write(&file1_path, "file1 committed contents")
            .await
            .unwrap();
        smol::fs::write(&file2_path, "file2 committed contents")
            .await
            .unwrap();
        smol::fs::write(&space_file_path, "space file committed contents")
            .await
            .unwrap();

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // Stage files and commit
        repo.stage_paths(
            vec![
                repo_path("file1"),
                repo_path("file2"),
                repo_path("file with spaces"),
            ],
            Arc::new(HashMap::default()),
        )
        .await
        .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        // Now modify files in index but not yet committed
        smol::fs::write(&file1_path, "file1 index contents")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file1")], Arc::new(HashMap::default()))
            .await
            .unwrap();

        // Write working tree contents (not indexed, not committed)
        smol::fs::write(&file1_path, "file1 worktree contents")
            .await
            .unwrap();

        // Now test load_revisions
        let results = repo
            .load_revisions(
                [
                    "HEAD:file1",
                    ":file1",
                    "HEAD:file2",
                    ":file2",
                    "HEAD:nonexistent",
                    "HEAD:file with spaces",
                    "HEAD:nonexistent file with spaces",
                ]
                .into_iter()
                .map(String::from)
                .collect(),
            )
            .await
            .unwrap();

        assert_eq!(
            results,
            vec![
                Some(b"file1 committed contents".to_vec()),
                Some(b"file1 index contents".to_vec()),
                Some(b"file2 committed contents".to_vec()),
                Some(b"file2 committed contents".to_vec()), // untouched in index, should match HEAD
                None,
                Some(b"space file committed contents".to_vec()),
                None,
            ]
        );
    }

    #[gpui::test]
    async fn test_blame_at_revision(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        let file_name = "ürlich file1";
        fs::write(repo_dir.path().join(file_name), "line one\n").unwrap();
        git_command(repo_dir.path(), ["add", "-A"]);
        git_command(repo_dir.path(), ["commit", "-m", "First commit"]);
        let first_sha = git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]);

        fs::write(repo_dir.path().join(file_name), "line one\nline two\n").unwrap();
        git_command(repo_dir.path(), ["add", "-A"]);
        git_command(repo_dir.path(), ["commit", "-m", "Second commit"]);
        let second_sha = git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]);

        let blame_at_head = repo
            .blame_at_revision(repo_path(file_name), second_sha.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            blame_at_head
                .entries
                .iter()
                .map(|entry| {
                    (
                        entry.sha.to_string(),
                        entry.range.clone(),
                        entry.filename.clone(),
                        entry.previous.clone(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (first_sha.clone(), 0..1, file_name.to_owned(), None),
                (
                    second_sha.clone(),
                    1..2,
                    file_name.to_owned(),
                    Some(format!("{first_sha} {file_name}"))
                ),
            ]
        );

        let blame_at_first = repo
            .blame_at_revision(repo_path(file_name), first_sha.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            blame_at_first
                .entries
                .iter()
                .map(|entry| (entry.sha.to_string(), entry.range.clone()))
                .collect::<Vec<_>>(),
            vec![(first_sha.clone(), 0..1)]
        );
    }

    #[gpui::test]
    async fn test_checkpoint_empty_repo(cx: &mut TestAppContext) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        smol::fs::write(repo_dir.path().join("foo"), "foo")
            .await
            .unwrap();
        let checkpoint_sha = repo.checkpoint().await.unwrap();

        // Ensure the user can't see any branches after creating a checkpoint.
        assert_eq!(repo.branches().await.unwrap().branches.len(), 1);

        smol::fs::write(repo_dir.path().join("foo"), "bar")
            .await
            .unwrap();
        smol::fs::write(repo_dir.path().join("baz"), "qux")
            .await
            .unwrap();
        repo.restore_checkpoint(checkpoint_sha).await.unwrap();
        assert_eq!(
            smol::fs::read_to_string(repo_dir.path().join("foo"))
                .await
                .unwrap(),
            "foo"
        );
        // See TODOs above
        // assert_eq!(
        //     smol::fs::read_to_string(repo_dir.path().join("baz"))
        //         .await
        //         .ok(),
        //     None
        // );
    }

    #[gpui::test]
    async fn test_branches_return_head_when_commit_metadata_cannot_be_read(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        smol::fs::write(repo_dir.path().join("file.txt"), "content")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file.txt")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        smol::fs::write(
            repo_dir.path().join(".git").join("refs/heads/broken"),
            "0a103ede22f159c792dc6405e0c8304d9bd4dc29\n",
        )
        .await
        .unwrap();

        let branches_scan = repo.branches().await.unwrap();
        assert!(branches_scan.error.is_some());
        let head_branch = branches_scan
            .branches
            .iter()
            .find(|branch| branch.is_head)
            .expect("branch list should include HEAD");
        assert!(head_branch.ref_name.starts_with("refs/heads/"));

        assert!(
            branches_scan
                .branches
                .iter()
                .all(|branch| branch.ref_name.as_ref() != "refs/heads/broken")
        );
    }

    #[gpui::test]
    async fn test_compare_checkpoints(cx: &mut TestAppContext) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        smol::fs::write(repo_dir.path().join("file1"), "content1")
            .await
            .unwrap();
        let checkpoint1 = repo.checkpoint().await.unwrap();

        smol::fs::write(repo_dir.path().join("file2"), "content2")
            .await
            .unwrap();
        let checkpoint2 = repo.checkpoint().await.unwrap();

        assert!(
            !repo
                .compare_checkpoints(checkpoint1, checkpoint2.clone())
                .await
                .unwrap()
        );

        let checkpoint3 = repo.checkpoint().await.unwrap();
        assert!(
            repo.compare_checkpoints(checkpoint2, checkpoint3)
                .await
                .unwrap()
        );
    }

    #[gpui::test]
    async fn test_checkpoint_exclude_binary_files(cx: &mut TestAppContext) {
        disable_git_global_config();

        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        let text_path = repo_dir.path().join("main.rs");
        let bin_path = repo_dir.path().join("binary.o");

        git_init_repo(repo_dir.path());

        smol::fs::write(&text_path, "fn main() {}").await.unwrap();

        smol::fs::write(&bin_path, "some binary file here")
            .await
            .unwrap();

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // initial commit
        repo.stage_paths(vec![repo_path("main.rs")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        let checkpoint = repo.checkpoint().await.unwrap();

        smol::fs::write(&text_path, "fn main() { println!(\"Modified\"); }")
            .await
            .unwrap();
        smol::fs::write(&bin_path, "Modified binary file")
            .await
            .unwrap();

        repo.restore_checkpoint(checkpoint).await.unwrap();

        // Text files should be restored to checkpoint state,
        // but binaries should not (they aren't tracked)
        assert_eq!(
            smol::fs::read_to_string(&text_path).await.unwrap(),
            "fn main() {}"
        );

        assert_eq!(
            smol::fs::read_to_string(&bin_path).await.unwrap(),
            "Modified binary file"
        );
    }

    #[test]
    fn test_branches_parsing() {
        // suppress "help: octal escapes are not supported, `\0` is always null"
        #[allow(clippy::octal_escapes)]
        let input = "*\0060964da10574cd9bf06463a53bf6e0769c5c45e\0\0refs/heads/zed-patches\0refs/remotes/origin/zed-patches\0\01733187470\0John Doe\0generated protobuf\n";
        assert_eq!(
            parse_branch_input(input).unwrap(),
            vec![Branch {
                is_head: true,
                ref_name: "refs/heads/zed-patches".into(),
                upstream: Some(Upstream {
                    ref_name: "refs/remotes/origin/zed-patches".into(),
                    tracking: UpstreamTracking::Tracked(UpstreamTrackingStatus {
                        ahead: 0,
                        behind: 0
                    })
                }),
                most_recent_commit: Some(CommitSummary {
                    sha: "060964da10574cd9bf06463a53bf6e0769c5c45e".into(),
                    subject: "generated protobuf".into(),
                    commit_timestamp: 1733187470,
                    author_name: SharedString::new_static("John Doe"),
                    has_parent: false,
                })
            }]
        )
    }

    #[test]
    fn test_branches_parsing_containing_refs_with_missing_fields() {
        #[allow(clippy::octal_escapes)]
        let input = " \090012116c03db04344ab10d50348553aa94f1ea0\0refs/heads/broken\n \0eb0cae33272689bd11030822939dd2701c52f81e\0895951d681e5561478c0acdd6905e8aacdfd2249\0refs/heads/dev\0\0\01762948725\0Zed\0Add feature\n*\0895951d681e5561478c0acdd6905e8aacdfd2249\0\0refs/heads/main\0\0\01762948695\0Zed\0Initial commit\n";

        let branches = parse_branch_input(input).unwrap();
        assert_eq!(branches.len(), 2);
        assert_eq!(
            branches,
            vec![
                Branch {
                    is_head: false,
                    ref_name: "refs/heads/dev".into(),
                    upstream: None,
                    most_recent_commit: Some(CommitSummary {
                        sha: "eb0cae33272689bd11030822939dd2701c52f81e".into(),
                        subject: "Add feature".into(),
                        commit_timestamp: 1762948725,
                        author_name: SharedString::new_static("Zed"),
                        has_parent: true,
                    })
                },
                Branch {
                    is_head: true,
                    ref_name: "refs/heads/main".into(),
                    upstream: None,
                    most_recent_commit: Some(CommitSummary {
                        sha: "895951d681e5561478c0acdd6905e8aacdfd2249".into(),
                        subject: "Initial commit".into(),
                        commit_timestamp: 1762948695,
                        author_name: SharedString::new_static("Zed"),
                        has_parent: false,
                    })
                }
            ]
        )
    }

    #[test]
    fn test_upstream_branch_name() {
        let upstream = Upstream {
            ref_name: "refs/remotes/origin/feature/branch".into(),
            tracking: UpstreamTracking::Tracked(UpstreamTrackingStatus {
                ahead: 0,
                behind: 0,
            }),
        };
        assert_eq!(upstream.branch_name(), Some("feature/branch"));

        let upstream = Upstream {
            ref_name: "refs/remotes/upstream/main".into(),
            tracking: UpstreamTracking::Tracked(UpstreamTrackingStatus {
                ahead: 0,
                behind: 0,
            }),
        };
        assert_eq!(upstream.branch_name(), Some("main"));

        let upstream = Upstream {
            ref_name: "refs/heads/local".into(),
            tracking: UpstreamTracking::Tracked(UpstreamTrackingStatus {
                ahead: 0,
                behind: 0,
            }),
        };
        assert_eq!(upstream.branch_name(), None);

        // Test case where upstream branch name differs from what might be the local branch name
        let upstream = Upstream {
            ref_name: "refs/remotes/origin/feature/git-pull-request".into(),
            tracking: UpstreamTracking::Tracked(UpstreamTrackingStatus {
                ahead: 0,
                behind: 0,
            }),
        };
        assert_eq!(upstream.branch_name(), Some("feature/git-pull-request"));
    }

    #[test]
    fn test_parse_worktrees_from_str() {
        // Empty input
        let result = parse_worktrees_from_str("", None);
        assert!(result.is_empty());

        // Single worktree (main)
        let input = "worktree /home/user/project\nHEAD abc123def\nbranch refs/heads/main\n\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[0].sha.as_ref(), "abc123def");
        assert_eq!(result[0].ref_name, Some("refs/heads/main".into()));
        assert!(result[0].is_main);
        assert!(!result[0].is_bare);

        // Multiple worktrees
        let input = "worktree /home/user/project-wt\nHEAD def456\nbranch refs/heads/feature\n\n\
                      worktree /home/user/project\nHEAD abc123\nbranch refs/heads/main\n\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project-wt"));
        assert_eq!(result[0].ref_name, Some("refs/heads/feature".into()));
        assert!(!result[0].is_main);
        assert!(!result[0].is_bare);
        assert_eq!(result[1].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[1].ref_name, Some("refs/heads/main".into()));
        assert!(result[1].is_main);
        assert!(!result[1].is_bare);

        // Detached HEAD entry (included with ref_name: None)
        let input = "worktree /home/user/project\nHEAD abc123\nbranch refs/heads/main\n\n\
                      worktree /home/user/detached\nHEAD def456\ndetached\n\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[0].ref_name, Some("refs/heads/main".into()));
        assert!(result[0].is_main);
        assert_eq!(result[1].path, PathBuf::from("/home/user/detached"));
        assert_eq!(result[1].ref_name, None);
        assert_eq!(result[1].sha.as_ref(), "def456");
        assert!(!result[1].is_main);
        assert!(!result[1].is_bare);

        // Bare repo entry with no main worktree.
        let input = "worktree /home/user/bare.git\nHEAD abc123\nbare\n\n\
                      worktree /home/user/project\nHEAD def456\nbranch refs/heads/main\n\n";
        let result = parse_worktrees_from_str(input, None);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].path, PathBuf::from("/home/user/bare.git"));
        assert_eq!(result[0].ref_name, None);
        assert!(!result[0].is_main);
        assert!(result[0].is_bare);
        assert_eq!(result[1].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[1].ref_name, Some("refs/heads/main".into()));
        assert!(!result[1].is_main);
        assert!(!result[1].is_bare);

        // Extra porcelain lines (locked, prunable) should be ignored
        let input = "worktree /home/user/project\nHEAD abc123\nbranch refs/heads/main\n\n\
                      worktree /home/user/locked-wt\nHEAD def456\nbranch refs/heads/locked-branch\nlocked\n\n\
                      worktree /home/user/prunable-wt\nHEAD 789aaa\nbranch refs/heads/prunable-branch\nprunable\n\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[0].ref_name, Some("refs/heads/main".into()));
        assert!(result[0].is_main);
        assert_eq!(result[1].path, PathBuf::from("/home/user/locked-wt"));
        assert_eq!(result[1].ref_name, Some("refs/heads/locked-branch".into()));
        assert!(!result[1].is_main);
        assert_eq!(result[2].path, PathBuf::from("/home/user/prunable-wt"));
        assert_eq!(
            result[2].ref_name,
            Some("refs/heads/prunable-branch".into())
        );
        assert!(!result[2].is_main);

        // Leading/trailing whitespace on lines should be tolerated
        let input =
            "  worktree /home/user/project  \n  HEAD abc123  \n  branch refs/heads/main  \n\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[0].sha.as_ref(), "abc123");
        assert_eq!(result[0].ref_name, Some("refs/heads/main".into()));
        assert!(result[0].is_main);

        // Windows-style line endings should be handled
        let input = "worktree /home/user/project\r\nHEAD abc123\r\nbranch refs/heads/main\r\n\r\n";
        let result = parse_worktrees_from_str(input, Some(Path::new("/home/user/project")));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].path, PathBuf::from("/home/user/project"));
        assert_eq!(result[0].sha.as_ref(), "abc123");
        assert_eq!(result[0].ref_name, Some("refs/heads/main".into()));
        assert!(result[0].is_main);
    }

    #[gpui::test]
    async fn test_create_and_list_worktrees(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        let worktrees_dir = temp_dir.path().join("worktrees");

        fs::create_dir_all(&repo_dir).unwrap();
        fs::create_dir_all(&worktrees_dir).unwrap();

        git_init_repo(&repo_dir);

        let repo = RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // Create an initial commit (required for worktrees)
        smol::fs::write(repo_dir.join("file.txt"), "content")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file.txt")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        // List worktrees — should have just the main one
        let worktrees = repo.worktrees().await.unwrap();
        assert_eq!(worktrees.len(), 1);
        assert_eq!(
            worktrees[0].path.canonicalize().unwrap(),
            repo_dir.canonicalize().unwrap()
        );

        let worktree_path = worktrees_dir.join("some-worktree");

        // Create a new worktree
        repo.create_worktree(
            CreateWorktreeTarget::NewBranch {
                branch_name: "test-branch".to_string(),
                base_sha: Some("HEAD".to_string()),
            },
            worktree_path.clone(),
        )
        .await
        .unwrap();

        // List worktrees — should have two
        let worktrees = repo.worktrees().await.unwrap();
        assert_eq!(worktrees.len(), 2);

        let new_worktree = worktrees
            .iter()
            .find(|w| w.display_name() == "test-branch")
            .expect("should find worktree with test-branch");
        assert_eq!(
            new_worktree.path.canonicalize().unwrap(),
            worktree_path.canonicalize().unwrap(),
        );

        // The new worktree's git metadata directory should report a creation
        // time, resolved via the worktree's `.git` file.
        let created_at = repo
            .worktree_created_at(worktree_path.clone())
            .await
            .unwrap();
        assert!(
            created_at.is_some(),
            "creation time should be available for a freshly created worktree"
        );

        // A path with no worktree at all reports `None`.
        let missing = repo
            .worktree_created_at(worktrees_dir.join("does-not-exist"))
            .await
            .unwrap();
        assert_eq!(missing, None);
    }

    #[gpui::test]
    async fn test_remove_worktree(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        let worktrees_dir = temp_dir.path().join("worktrees");
        git_init_repo(&repo_dir);

        let repo = RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // Create an initial commit
        smol::fs::write(repo_dir.join("file.txt"), "content")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file.txt")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        // Create a worktree
        let worktree_path = worktrees_dir.join("worktree-to-remove");
        repo.create_worktree(
            CreateWorktreeTarget::NewBranch {
                branch_name: "to-remove".to_string(),
                base_sha: Some("HEAD".to_string()),
            },
            worktree_path.clone(),
        )
        .await
        .unwrap();

        // Remove the worktree
        repo.remove_worktree(worktree_path.clone(), false)
            .await
            .unwrap();

        // Verify the directory is removed
        let worktrees = repo.worktrees().await.unwrap();
        assert_eq!(worktrees.len(), 1);
        assert!(
            worktrees.iter().all(|w| w.display_name() != "to-remove"),
            "removed worktree should not appear in list"
        );
        assert!(!worktree_path.exists());

        // Create a worktree
        let worktree_path = worktrees_dir.join("dirty-wt");
        repo.create_worktree(
            CreateWorktreeTarget::NewBranch {
                branch_name: "dirty-wt".to_string(),
                base_sha: Some("HEAD".to_string()),
            },
            worktree_path.clone(),
        )
        .await
        .unwrap();

        assert!(worktree_path.exists());

        // Add uncommitted changes in the worktree
        smol::fs::write(worktree_path.join("dirty-file.txt"), "uncommitted")
            .await
            .unwrap();

        // Non-force removal should fail with dirty worktree
        let result = repo.remove_worktree(worktree_path.clone(), false).await;
        assert!(
            result.is_err(),
            "non-force removal of dirty worktree should fail"
        );

        // Force removal should succeed
        repo.remove_worktree(worktree_path.clone(), true)
            .await
            .unwrap();

        let worktrees = repo.worktrees().await.unwrap();
        assert_eq!(worktrees.len(), 1);
        assert!(!worktree_path.exists());
    }

    #[gpui::test]
    async fn test_rename_worktree(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        let worktrees_dir = temp_dir.path().join("worktrees");

        git_init_repo(&repo_dir);

        let repo = RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // Create an initial commit
        smol::fs::write(repo_dir.join("file.txt"), "content")
            .await
            .unwrap();
        repo.stage_paths(vec![repo_path("file.txt")], Arc::new(HashMap::default()))
            .await
            .unwrap();
        repo.commit(
            "Initial commit".into(),
            None,
            CommitOptions::default(),
            AskPassDelegate::new(&mut cx.to_async(), |_, _, _| {}),
            Arc::new(test_commit_envs()),
        )
        .await
        .unwrap();

        // Create a worktree
        let old_path = worktrees_dir.join("old-worktree-name");
        repo.create_worktree(
            CreateWorktreeTarget::NewBranch {
                branch_name: "old-name".to_string(),
                base_sha: Some("HEAD".to_string()),
            },
            old_path.clone(),
        )
        .await
        .unwrap();

        assert!(old_path.exists());

        // Move the worktree to a new path
        let new_path = worktrees_dir.join("new-worktree-name");
        repo.rename_worktree(old_path.clone(), new_path.clone())
            .await
            .unwrap();

        // Verify the old path is gone and new path exists
        assert!(!old_path.exists());
        assert!(new_path.exists());

        // Verify it shows up in worktree list at the new path
        let worktrees = repo.worktrees().await.unwrap();
        assert_eq!(worktrees.len(), 2);
        let moved_worktree = worktrees
            .iter()
            .find(|w| w.display_name() == "old-name")
            .expect("should find worktree by branch name");
        assert_eq!(
            moved_worktree.path.canonicalize().unwrap(),
            new_path.canonicalize().unwrap()
        );
    }

    #[gpui::test]
    async fn test_initial_graph_data_ref_set(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();
        let git = repo.git_binary();

        let graph_commits = async || {
            let (tx, rx) = smol::channel::unbounded();
            repo.initial_graph_data(LogSource::All, LogOrder::DateOrder, tx)
                .await
                .unwrap();
            let mut commits = std::collections::HashSet::new();
            while let Ok(chunk) = rx.try_recv() {
                for commit in chunk {
                    commits.insert(commit.sha);
                }
            }
            commits
        };

        smol::fs::write(repo_dir.path().join("file1"), "1")
            .await
            .unwrap();
        let branch_sha = repo.checkpoint().await.unwrap().commit_sha;
        repo.update_ref("refs/heads/main".into(), branch_sha.to_string())
            .await
            .unwrap();

        smol::fs::write(repo_dir.path().join("file2"), "2")
            .await
            .unwrap();
        let hidden_sha = repo.checkpoint().await.unwrap().commit_sha;
        repo.update_ref("refs/custom/hidden".into(), hidden_sha.to_string())
            .await
            .unwrap();

        let graph = graph_commits().await;
        assert!(graph.contains(&branch_sha));
        assert!(!graph.contains(&hidden_sha));

        git.build_command(&["update-ref", "--no-deref", "HEAD", &hidden_sha.to_string()])
            .output()
            .await
            .unwrap();

        let graph = graph_commits().await;
        assert!(graph.contains(&branch_sha));
        assert!(graph.contains(&hidden_sha));
    }

    #[gpui::test]
    async fn test_check_for_pushed_commit(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        git_init_repo(&repo_dir);

        let repo = RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        // New repo doesn't have any commits yet
        assert!(repo.check_for_pushed_commit().await.unwrap().is_empty());

        git_command(
            &repo_dir,
            ["commit", "--allow-empty", "-m", "Initial commit"],
        );

        // No remote branches exist yet
        assert!(repo.check_for_pushed_commit().await.unwrap().is_empty());

        // Create simulated remote branches
        git_command(
            &repo_dir,
            ["update-ref", "refs/remotes/origin/main", "HEAD"],
        );
        git_command(
            &repo_dir,
            ["update-ref", "refs/remotes/origin/other-branch", "HEAD"],
        );
        assert_eq!(
            repo.check_for_pushed_commit().await.unwrap(),
            vec![
                SharedString::from("origin/main"),
                SharedString::from("origin/other-branch")
            ]
        );

        // Switch to a new branch, commit but do not push
        git_command(&repo_dir, ["switch", "-c", "local-feature"]);
        git_command(&repo_dir, ["commit", "--allow-empty", "-m", "Local commit"]);

        // New commit has not been pushed
        assert!(repo.check_for_pushed_commit().await.unwrap().is_empty());
    }

    #[test]
    fn test_original_repo_path_from_common_dir() {
        // Normal repo: common_dir is <work_dir>/.git
        assert_eq!(
            original_repo_path_from_common_dir(Path::new("/code/zed5/.git")),
            Some(PathBuf::from("/code/zed5"))
        );

        // Worktree: common_dir is the main repo's .git
        // (same result — that's the point, it always traces back to the original)
        assert_eq!(
            original_repo_path_from_common_dir(Path::new("/code/zed5/.git")),
            Some(PathBuf::from("/code/zed5"))
        );

        // Bare repo: no .git suffix, returns None (no working-tree root)
        assert_eq!(
            original_repo_path_from_common_dir(Path::new("/code/zed5.git")),
            None
        );

        // Root-level .git directory
        assert_eq!(
            original_repo_path_from_common_dir(Path::new("/.git")),
            Some(PathBuf::from("/"))
        );
    }

    #[gpui::test]
    async fn test_default_branch(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());

        let repo = RealGitRepository::new(
            &repo_dir.path().join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        assert_eq!(repo.default_branch(false).await.unwrap(), None);

        git_command(
            repo_dir.path(),
            ["commit", "--allow-empty", "-m", "Initial commit"],
        );

        assert_eq!(
            repo.default_branch(false).await.unwrap(),
            Some("main".into())
        );

        git_command(
            repo_dir.path(),
            ["update-ref", "refs/remotes/origin/main", "HEAD"],
        );
        git_command(
            repo_dir.path(),
            [
                "symbolic-ref",
                "refs/remotes/origin/HEAD",
                "refs/remotes/origin/main",
            ],
        );

        assert_eq!(
            repo.default_branch(false).await.unwrap(),
            Some("main".into())
        );
        assert_eq!(
            repo.default_branch(true).await.unwrap(),
            Some("origin/main".into())
        );
    }

    fn graph_mutation_repository(path: &Path, cx: &TestAppContext) -> RealGitRepository {
        RealGitRepository::new(&path.join(".git"), None, Some("git".into()), cx.executor()).unwrap()
    }

    fn graph_mutation_commit(path: &Path, file: &str, contents: &str, message: &str) -> String {
        fs::write(path.join(file), contents).unwrap();
        git_command(path, ["add", file]);
        git_command(path, ["commit", "-m", message]);
        git_command_output(path, ["rev-parse", "HEAD"])
    }

    fn graph_mutation_env() -> Arc<HashMap<String, String>> {
        Arc::new(test_commit_envs())
    }

    #[gpui::test]
    async fn test_graph_mutation_checkout_commit_detaches_head(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let initial = graph_mutation_commit(repo_dir.path(), "file.txt", "one", "one");
        graph_mutation_commit(repo_dir.path(), "file.txt", "two", "two");
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.checkout_commit(initial.clone(), graph_mutation_env())
            .await
            .unwrap();

        assert_eq!(
            git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]),
            initial
        );
        assert_eq!(
            git_command_output(repo_dir.path(), ["branch", "--show-current"]),
            ""
        );
    }

    #[gpui::test]
    async fn test_graph_mutation_create_lightweight_and_annotated_tags(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let target = graph_mutation_commit(repo_dir.path(), "file.txt", "one", "one");
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.create_tag(
            CreateTagOptions {
                name: "lightweight".into(),
                target: target.clone(),
                message: None,
            },
            graph_mutation_env(),
        )
        .await
        .unwrap();
        repo.create_tag(
            CreateTagOptions {
                name: "annotated".into(),
                target: target.clone(),
                message: Some("release notes".into()),
            },
            graph_mutation_env(),
        )
        .await
        .unwrap();
        let error = repo
            .create_tag(
                CreateTagOptions {
                    name: "-leading-hyphen".into(),
                    target: target.clone(),
                    message: None,
                },
                graph_mutation_env(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cannot start with"));

        assert_eq!(
            git_command_output(repo_dir.path(), ["rev-parse", "lightweight"]),
            target
        );
        assert_eq!(
            git_command_output(repo_dir.path(), ["rev-parse", "annotated^{}"]),
            target
        );
        assert_eq!(
            git_command_output(repo_dir.path(), ["cat-file", "-t", "annotated"]),
            "tag"
        );
    }

    #[gpui::test]
    async fn test_tag_details_distinguishes_lightweight_from_annotated(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let target = graph_mutation_commit(repo_dir.path(), "file.txt", "one", "one");
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.create_tag(
            CreateTagOptions {
                name: "annotated".into(),
                target: target.clone(),
                message: Some("release notes\nsecond line".into()),
            },
            graph_mutation_env(),
        )
        .await
        .unwrap();
        repo.create_tag(
            CreateTagOptions {
                name: "lightweight".into(),
                target: target.clone(),
                message: None,
            },
            graph_mutation_env(),
        )
        .await
        .unwrap();

        // Annotated tag carries tagger metadata + full message; the target
        // OID peels to the tagged commit.
        let annotated = repo
            .tag_details("refs/tags/annotated".into())
            .await
            .unwrap();
        assert_eq!(annotated.name.as_ref(), "annotated");
        assert_eq!(annotated.ref_name.as_ref(), "refs/tags/annotated");
        assert_eq!(annotated.target_oid.to_string(), target);
        assert_eq!(annotated.object_type, TagObjectType::Commit);
        assert!(annotated.tagger.is_some(), "annotated tag should have a tagger");
        assert_eq!(
            annotated.message.as_deref(),
            Some("release notes\nsecond line")
        );

        // Lightweight tag: no tagger, no message, same target.
        let lightweight = repo
            .tag_details("refs/tags/lightweight".into())
            .await
            .unwrap();
        assert_eq!(lightweight.object_type, TagObjectType::Commit);
        assert_eq!(lightweight.target_oid.to_string(), target);
        assert_eq!(lightweight.tagger, None);
        assert_eq!(lightweight.message, None);
    }

    #[gpui::test]
    async fn test_delete_local_tag(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let target = graph_mutation_commit(repo_dir.path(), "file.txt", "one", "one");
        let repo = graph_mutation_repository(repo_dir.path(), cx);
        repo.create_tag(
            CreateTagOptions {
                name: "v1".into(),
                target: target.clone(),
                message: None,
            },
            graph_mutation_env(),
        )
        .await
        .unwrap();

        repo.delete_tag("v1".into()).await.unwrap();

        // The tag ref no longer resolves (git_command_output panics on a
        // failing rev-parse, so use the git crate path through the repo).
        let gone = repo.delete_tag("v1".into()).await.unwrap_err();
        assert!(gone.to_string().contains("error"), "{gone:?}");
    }

    #[gpui::test]
    async fn test_delete_refs_on_remote_performs_server_deletion(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let temp_dir = tempfile::tempdir().unwrap();
        let remote_dir = temp_dir.path().join("remote.git");

        // Bare remote so we can verify the server-side ref table.
        git_command(
            temp_dir.path(),
            [
                OsString::from("init"),
                OsString::from("--bare"),
                OsString::from("-b"),
                OsString::from("main"),
                remote_dir.as_os_str().into(),
            ],
        );

        git_init_repo(&temp_dir.path().join("seed"));
        let seed = temp_dir.path().join("seed");
        fs::write(seed.join("file.txt"), "main").unwrap();
        git_command(&seed, ["add", "file.txt"]);
        git_command(&seed, ["commit", "-m", "initial"]);
        git_command(
            &seed,
            [
                OsString::from("remote"),
                OsString::from("add"),
                OsString::from("origin"),
                remote_dir.as_os_str().into(),
            ],
        );
        git_command(&seed, ["push", "-u", "origin", "main"]);

        // Bare remotes refuse to delete their checked-out (HEAD) branch by
        // default, so relax that guard to exercise the actual server deletion.
        git_command(&remote_dir, ["config", "receive.denyDeleteCurrent", "ignore"]);

        // Confirm the branch exists on the server before deletion.
        assert!(
            git_command_output(
                &remote_dir,
                ["for-each-ref", "--format=%(refname)", "refs/heads"],
            )
            .contains("refs/heads/main"),
        );

        let repo = graph_mutation_repository(&seed, cx);
        let mut async_cx = cx.to_async();
        let askpass = AskPassDelegate::new(&mut async_cx, |_prompt, _tx, _cx| {});

        // Server deletion explicitly pushes a deletion of refs/heads/main on
        // origin — it must not leave the local remote-tracking ref behind.
        repo.delete_refs_on_remote(
            "origin".into(),
            vec!["refs/heads/main".into()],
            askpass,
            graph_mutation_env(),
            cx.to_async(),
        )
        .await
        .unwrap();

        let remaining =
            git_command_output(&remote_dir, ["for-each-ref", "--format=%(refname)", "refs/heads"]);
        assert!(
            !remaining.contains("refs/heads/main"),
            "refs/heads/main should be deleted on the server, got {remaining:?}"
        );
    }

    #[gpui::test]
    async fn test_graph_mutation_cherry_pick_preserves_supplied_order(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
        git_command(repo_dir.path(), ["switch", "-c", "source"]);
        let first = graph_mutation_commit(repo_dir.path(), "first.txt", "first", "first");
        let second = graph_mutation_commit(repo_dir.path(), "second.txt", "second", "second");
        git_command(repo_dir.path(), ["switch", "main"]);
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.cherry_pick(vec![first, second], false, graph_mutation_env())
            .await
            .unwrap();

        assert_eq!(
            git_command_output(repo_dir.path(), ["log", "-2", "--format=%s"]),
            "second\nfirst"
        );
    }

    #[gpui::test]
    async fn test_graph_mutation_cherry_pick_rejects_empty_input(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        let error = repo
            .cherry_pick(Vec::new(), false, graph_mutation_env())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("at least one commit"));
    }

    #[gpui::test]
    async fn test_graph_mutation_revert_restores_previous_tree(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        graph_mutation_commit(repo_dir.path(), "file.txt", "before", "before");
        let change = graph_mutation_commit(repo_dir.path(), "file.txt", "after", "after");
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.revert(change, false, graph_mutation_env())
            .await
            .unwrap();

        assert_eq!(
            fs::read_to_string(repo_dir.path().join("file.txt")).unwrap(),
            "before"
        );
    }

    #[gpui::test]
    async fn test_graph_mutation_merge_modes(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        {
            let repo_dir = tempfile::tempdir().unwrap();
            git_init_repo(repo_dir.path());
            graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
            git_command(repo_dir.path(), ["switch", "-c", "feature"]);
            let feature =
                graph_mutation_commit(repo_dir.path(), "feature.txt", "feature", "feature");
            git_command(repo_dir.path(), ["switch", "main"]);
            let repo = graph_mutation_repository(repo_dir.path(), cx);

            repo.merge(feature.clone(), MergeMode::Default, graph_mutation_env())
                .await
                .unwrap();

            assert_eq!(
                git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]),
                feature
            );
        }

        {
            let repo_dir = tempfile::tempdir().unwrap();
            git_init_repo(repo_dir.path());
            graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
            git_command(repo_dir.path(), ["switch", "-c", "feature"]);
            let feature =
                graph_mutation_commit(repo_dir.path(), "feature.txt", "feature", "feature");
            git_command(repo_dir.path(), ["switch", "main"]);
            let repo = graph_mutation_repository(repo_dir.path(), cx);

            repo.merge(feature, MergeMode::NoFastForward, graph_mutation_env())
                .await
                .unwrap();

            assert_eq!(
                git_command_output(
                    repo_dir.path(),
                    ["rev-list", "--parents", "-n", "1", "HEAD"]
                )
                .split_whitespace()
                .count(),
                3
            );
        }

        {
            let repo_dir = tempfile::tempdir().unwrap();
            git_init_repo(repo_dir.path());
            graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
            git_command(repo_dir.path(), ["switch", "-c", "feature"]);
            let feature =
                graph_mutation_commit(repo_dir.path(), "feature.txt", "feature", "feature");
            git_command(repo_dir.path(), ["switch", "main"]);
            graph_mutation_commit(repo_dir.path(), "main.txt", "main", "main");
            let repo = graph_mutation_repository(repo_dir.path(), cx);

            assert!(
                repo.merge(feature, MergeMode::FastForwardOnly, graph_mutation_env(),)
                    .await
                    .is_err()
            );
        }

        {
            let repo_dir = tempfile::tempdir().unwrap();
            git_init_repo(repo_dir.path());
            graph_mutation_commit(repo_dir.path(), "base.txt", "base", "base");
            git_command(repo_dir.path(), ["switch", "-c", "feature"]);
            let feature =
                graph_mutation_commit(repo_dir.path(), "feature.txt", "feature", "feature");
            git_command(repo_dir.path(), ["switch", "main"]);
            let head_before = git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]);
            let repo = graph_mutation_repository(repo_dir.path(), cx);

            repo.merge(feature, MergeMode::Squash, graph_mutation_env())
                .await
                .unwrap();

            assert_eq!(
                git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]),
                head_before
            );
            assert_eq!(
                git_command_output(repo_dir.path(), ["diff", "--cached", "--name-only"]),
                "feature.txt"
            );
        }
    }

    #[gpui::test]
    async fn test_graph_mutation_hard_reset_updates_head_index_and_worktree(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();
        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        let target = graph_mutation_commit(repo_dir.path(), "file.txt", "before", "before");
        graph_mutation_commit(repo_dir.path(), "file.txt", "after", "after");
        fs::write(repo_dir.path().join("file.txt"), "dirty").unwrap();
        git_command(repo_dir.path(), ["add", "file.txt"]);
        let repo = graph_mutation_repository(repo_dir.path(), cx);

        repo.reset(target.clone(), ResetMode::Hard, graph_mutation_env())
            .await
            .unwrap();

        assert_eq!(
            git_command_output(repo_dir.path(), ["rev-parse", "HEAD"]),
            target
        );
        assert_eq!(
            fs::read_to_string(repo_dir.path().join("file.txt")).unwrap(),
            "before"
        );
        assert_eq!(
            git_command_output(repo_dir.path(), ["diff", "--cached", "--name-only"]),
            ""
        );
        assert_eq!(
            git_command_output(repo_dir.path(), ["diff", "--name-only"]),
            ""
        );
    }

    impl RealGitRepository {
        /// Force a Git garbage collection on the repository.
        fn gc(&self) -> BoxFuture<'_, Result<()>> {
            let working_directory = self.command_directory();
            let git_directory = self.path();
            let git_binary_path = self.any_git_binary_path.clone();
            let executor = self.executor.clone();
            self.executor
                .spawn(async move {
                    let git_binary_path = git_binary_path.clone();
                    let git = GitBinary::new(
                        git_binary_path,
                        working_directory,
                        git_directory,
                        executor,
                        true,
                    );
                    git.run(&["gc", "--prune"]).await?;
                    Ok(())
                })
                .boxed()
        }
    }

    #[test]
    fn test_parse_remote_urls() {
        let stdout = concat!(
            "origin\thttps://github.com/zed-industries/zed.git (fetch) [blob:none]\n",
            "origin\thttps://github.com/zed-industries/zed.git (push)\n",
            "upstream\t/Users/user/My Projects/upstream.git (fetch)\n",
            "upstream\t/Users/user/My Projects/upstream.git (push)\n",
            "a\t/x (fetch) dir (fetch)\n",
            "a\t/x (fetch) dir (push)\n",
            "archive\t/tmp/remote [archive].git (fetch)\n",
            "archive\t/tmp/remote [archive].git (push)\n",
        );

        let remote_urls = parse_remote_urls(stdout);
        assert_eq!(remote_urls.len(), 4);
        assert_eq!(
            remote_urls.get("origin").map(String::as_str),
            Some("https://github.com/zed-industries/zed.git")
        );
        assert_eq!(
            remote_urls.get("upstream").map(String::as_str),
            Some("/Users/user/My Projects/upstream.git")
        );
        assert_eq!(
            remote_urls.get("a").map(String::as_str),
            Some("/x (fetch) dir")
        );
        assert_eq!(
            remote_urls.get("archive").map(String::as_str),
            Some("/tmp/remote [archive].git")
        );
    }

    #[gpui::test]
    async fn test_remote_urls(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let temp_dir = tempfile::tempdir().unwrap();
        let repo_dir = temp_dir.path().join("repo");
        std::fs::create_dir_all(&repo_dir).unwrap();

        git_init_repo(&repo_dir);

        let repo = RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap();

        let git = repo.git_binary();
        git.run(&[
            "remote",
            "add",
            "origin",
            "https://github.com/zed-industries/zed.git",
        ])
        .await
        .unwrap();
        git.run(&[
            "remote",
            "add",
            "upstream",
            "/Users/user/My Projects/upstream.git",
        ])
        .await
        .unwrap();
        git.run(&["config", "remote.origin.promisor", "true"])
            .await
            .unwrap();
        git.run(&["config", "remote.origin.partialclonefilter", "blob:none"])
            .await
            .unwrap();

        let remote_urls = repo.remote_urls().await;
        assert_eq!(remote_urls.len(), 2);
        assert_eq!(
            remote_urls.get("origin").unwrap(),
            "https://github.com/zed-industries/zed.git"
        );
        assert_eq!(
            remote_urls.get("upstream").unwrap(),
            "/Users/user/My Projects/upstream.git"
        );
    }

    #[gpui::test]
    async fn test_git_operation_lifecycle_detection_and_actions(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        graph_mutation_commit(repo_dir.path(), "file.txt", "one", "one");
        git_command(repo_dir.path(), ["switch", "-c", "feature"]);
        graph_mutation_commit(repo_dir.path(), "file.txt", "two", "two");
        git_command(repo_dir.path(), ["switch", "main"]);
        graph_mutation_commit(repo_dir.path(), "file.txt", "three", "three");

        let repo = graph_mutation_repository(repo_dir.path(), cx);

        git_command_failure(repo_dir.path(), ["merge", "--no-ff", "feature"]);

        assert_eq!(
            repo.operation_state().await.unwrap(),
            Some(GitOperationKind::Merge)
        );

        assert!(
            repo.run_operation_action(
                GitOperationKind::Merge,
                GitOperationAction::Skip,
                graph_mutation_env()
            )
            .await
            .is_err()
        );

        repo.run_operation_action(
            GitOperationKind::Merge,
            GitOperationAction::Abort,
            graph_mutation_env(),
        )
        .await
        .unwrap();

        assert_eq!(repo.operation_state().await.unwrap(), None);
    }

    /// Set up a tracked base file, then push a stash whose working-tree change is
    /// the given marker line. Returns the resulting stash commit OID.
    fn stash_with_marker(repo_dir: &Path, marker: &str) -> Oid {
        fs::write(
            repo_dir.join("stash_file.txt"),
            format!("base\n{marker}\n"),
        )
        .unwrap();
        git_command(repo_dir, ["add", "stash_file.txt"]);
        git_command(
            repo_dir,
            ["stash", "push", "-m", &format!("stash {marker}")],
        );
        git_command_output(repo_dir, ["rev-parse", "refs/stash"])
            .parse()
            .unwrap()
    }

    fn stash_oids(repo_dir: &Path) -> Vec<Oid> {
        let output = git_command_output(repo_dir, ["stash", "list", "--format=%H"]);
        if output.trim().is_empty() {
            Vec::new()
        } else {
            output
                .lines()
                .map(|line| line.parse().unwrap())
                .collect::<Vec<_>>()
        }
    }

    fn new_real_repo(repo_dir: &Path, cx: &TestAppContext) -> RealGitRepository {
        RealGitRepository::new(
            &repo_dir.join(".git"),
            None,
            Some("git".into()),
            cx.executor(),
        )
        .unwrap()
    }

    /// List the recovery refs an unfinished stash rename left behind.
    fn stash_rename_recovery_refs(repo_dir: &Path) -> Vec<String> {
        git_command_output(
            repo_dir,
            ["for-each-ref", "--format=%(refname)", "refs/zed-git/stash-rename"],
        )
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect()
    }

    /// Read the stash stack as `(oid, message)` newest-first, for asserting
    /// message/order preservation.
    fn stash_entries_newest_first(repo_dir: &Path) -> Vec<(Oid, String)> {
        let output =
            git_command_output(repo_dir, ["stash", "list", "--format=%H%x00%s"]);
        if output.trim().is_empty() {
            return Vec::new();
        }
        output
            .lines()
            .filter_map(|line| {
                let (oid, message) = line.split_once('\0')?;
                Some((oid.trim().parse().ok()?, message.to_string()))
            })
            .collect()
    }

    /// Assert a stash rename cleaned up its recovery refs + manifest.
    fn assert_no_rename_recovery(repo_dir: &Path) {
        let refs = stash_rename_recovery_refs(repo_dir);
        assert!(
            refs.is_empty(),
            "expected no lingering recovery refs, found: {refs:?}"
        );
    }

    fn set_stash_rename_fault(
        repository: &RealGitRepository,
        f: impl Fn(StashRenameBoundary) -> Result<()> + Send + 'static,
    ) {
        *repository.stash_rename_fault.lock() = Some(Box::new(f));
    }

    fn clear_stash_rename_fault(repository: &RealGitRepository) {
        *repository.stash_rename_fault.lock() = None;
    }

    #[gpui::test]
    async fn test_stash_apply_uses_exact_oid_and_retains_entry(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);

        let oid = stash_with_marker(repo_dir.path(), "applied");
        let repository = new_real_repo(repo_dir.path(), cx);
        let identity = StashIdentity::for_entry(&StashEntry {
            index: 0,
            oid,
            message: "stash applied".into(),
            branch: None,
            timestamp: 0,
        });

        repository
            .stash_apply(Some(identity), graph_mutation_env())
            .await
            .unwrap();

        // apply restores the change but keeps the stash.
        let contents = fs::read_to_string(repo_dir.path().join("stash_file.txt")).unwrap();
        assert!(contents.contains("applied"), "apply did not restore: {contents}");
        assert!(
            stash_oids(repo_dir.path()).contains(&oid),
            "apply must retain the stash entry"
        );
    }

    #[gpui::test]
    async fn test_stash_pop_removes_fresh_selector_and_follows_reorder(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);

        // push two stashes, then an external one so "first" moves off its hint index.
        let first = stash_with_marker(repo_dir.path(), "first");
        stash_with_marker(repo_dir.path(), "second");
        stash_with_marker(repo_dir.path(), "third");

        // Capture the identity of "first" as it was selected (at index 2 before the reorder
        // by the externally-inserted "third" at the top). The backend must still resolve it
        // by OID and drop the freshly resolved selector.
        let identity = StashIdentity {
            oid: first,
            ref_name: STASH_REF.to_string(),
            selector: format!("{}@{{2}}", STASH_REF),
        };
        let repository = new_real_repo(repo_dir.path(), cx);
        let outcome = repository
            .stash_pop(Some(identity), graph_mutation_env())
            .await
            .unwrap();
        assert_eq!(
            outcome,
            StashMutationResult::Success,
            "pop should fully drop the matched entry"
        );

        let remaining = stash_oids(repo_dir.path());
        assert!(!remaining.contains(&first), "popped entry must be dropped");
        assert_eq!(remaining.len(), 2, "only the other two stashes remain");

        let contents = fs::read_to_string(repo_dir.path().join("stash_file.txt")).unwrap();
        assert!(
            contents.contains("first"),
            "pop must apply the exactly-selected entry: {contents}"
        );
    }

    #[gpui::test]
    async fn test_stash_pop_missing_identity_errors(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        stash_with_marker(repo_dir.path(), "only");

        let dangling_oid = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
            .parse()
            .unwrap();
        let identity = StashIdentity {
            oid: dangling_oid,
            ref_name: STASH_REF.to_string(),
            selector: format!("{}@{{0}}", STASH_REF),
        };
        let repository = new_real_repo(repo_dir.path(), cx);
        let err = repository
            .stash_pop(Some(identity), graph_mutation_env())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("stash not found"),
            "expected a missing-stash error, got: {err:#}"
        );
        // Holding on to the repo so the drop does not remove the whole temp dir early.
        drop(repository);
    }

    #[gpui::test]
    async fn test_stash_rename_mid_entry_preserves_order_oids_and_metadata(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);

        let first = stash_with_marker(repo_dir.path(), "first");
        let second = stash_with_marker(repo_dir.path(), "second");
        let third = stash_with_marker(repo_dir.path(), "third");

        let before = stash_entries_newest_first(repo_dir.path());
        // newest-first: [third, second, first]
        assert_eq!(before.len(), 3);
        assert_eq!(before[0].0, third);
        assert!(before[0].1.contains("third"));
        assert_eq!(before[1].0, second);
        assert!(before[1].1.contains("second"));
        assert_eq!(before[2].0, first);
        assert!(before[2].1.contains("first"));

        // Rename the middle entry (stash@{1} = second).
        let identity = StashIdentity {
            oid: second,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{1}".to_string(),
        };
        let repository = new_real_repo(repo_dir.path(), cx);
        let result = repository
            .stash_rename(Some(identity), "renamed middle".to_string(), graph_mutation_env())
            .await
            .unwrap();
        assert_eq!(result, StashRenameResult::Success);

        let after = stash_entries_newest_first(repo_dir.path());
        assert_eq!(after.len(), 3, "order and count preserved");
        // Order preserved (no move to top): third still 0, middle still 1, first still 2.
        assert_eq!(after[0].0, third, "top entry OID untouched");
        assert!(after[0].1.contains("third"), "top subject untouched");
        assert_eq!(after[2].0, first, "bottom entry OID untouched");
        assert!(after[2].1.contains("first"), "bottom subject untouched");
        // Only the middle entry's message changed; its OID must differ (rewritten).
        assert_ne!(after[1].0, second, "renamed entry gets a rewritten OID");
        assert_eq!(after[1].1, "On main: renamed middle");
        // Other non-target OIDs preserved exactly.
        let before_oids: Vec<Oid> = before.iter().map(|(o, _)| *o).collect();
        let after_oids: Vec<Oid> = after.iter().map(|(o, _)| *o).collect();
        let renamed_oid = after_oids[1];
        assert!(before_oids.contains(&after_oids[0]));
        assert!(before_oids.contains(&after_oids[2]));
        assert!(!before_oids.contains(&renamed_oid));

        // Recovery refs must be cleaned up after a successful rename.
        assert_no_rename_recovery(repo_dir.path());

        // The rewritten commit preserves the original commit timestamp.
        let before_ct = git_command_output(
            repo_dir.path(),
            ["log", "-1", "--format=%ct", &second.to_string()],
        );
        let after_ct = git_command_output(
            repo_dir.path(),
            ["log", "-1", "--format=%ct", &renamed_oid.to_string()],
        );
        assert_eq!(after_ct, before_ct, "commit timestamp preserved");
    }

    #[gpui::test]
    async fn test_stash_rename_duplicate_oids_renames_only_selector_target(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);

        // Push one stash, then move the ref away and back so two reflog
        // entries point at the SAME commit OID, with a non-duplicate entry
        // between them.
        let oid = stash_with_marker(repo_dir.path(), "shared");
        let head = git_command_output(repo_dir.path(), ["rev-parse", "HEAD"])
            .trim()
            .to_string();
        // Move the tip to HEAD (drops the stash tip), then back to `oid`.
        git_command(
            repo_dir.path(),
            ["update-ref", "-m", "interim", STASH_REF, &head, &oid.to_string()],
        );
        git_command(
            repo_dir.path(),
            ["update-ref", "-m", "On main: dup", STASH_REF, &oid.to_string(), &head],
        );

        let before = stash_entries_newest_first(repo_dir.path());
        assert_eq!(before.len(), 3);
        assert_eq!(before[0].0, oid, "top duplicate shares the OID");
        assert_eq!(before[1].0.to_string(), head, "interim entry is the base");
        assert_eq!(before[2].0, oid, "bottom duplicate shares the OID");
        // Both duplicate entries surface the SAME commit subject (the reflog
        // message differs but `%s` is the commit subject), so only the exact
        // OID + selector identity can tell them apart.
        assert!(before[0].1.contains("stash shared"));
        assert!(before[2].1.contains("stash shared"));

        // Rename the entry captured at selector @2 (the "shared" duplicate).
        let identity = StashIdentity {
            oid,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{2}".to_string(),
        };
        let repository = new_real_repo(repo_dir.path(), cx);
        let result = repository
            .stash_rename(Some(identity), "only this one".to_string(), graph_mutation_env())
            .await
            .unwrap();
        assert_eq!(result, StashRenameResult::Success);

        let after = stash_entries_newest_first(repo_dir.path());
        assert_eq!(after.len(), 3);
        // Only the target (bottom) duplicate changed; the upper duplicate keeps
        // the original OID and message, and the interim entry is untouched.
        assert_eq!(after[0].0, oid, "untouched duplicate OID preserved");
        assert!(after[0].1.contains("stash shared"), "untouched subject kept");
        assert_eq!(after[1].0.to_string(), head, "interim entry untouched");
        assert_ne!(after[2].0, oid, "renamed duplicate gets a rewritten OID");
        assert_eq!(after[2].1, "On main: only this one");
        assert_no_rename_recovery(repo_dir.path());
    }

    #[gpui::test]
    async fn test_stash_rename_empty_message_and_missing_identity_are_safe(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let oid = stash_with_marker(repo_dir.path(), "only");

        let repository = new_real_repo(repo_dir.path(), cx);

        // Empty message is rejected before any destructive work.
        let identity = StashIdentity {
            oid,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{0}".to_string(),
        };
        let err = repository
            .stash_rename(Some(identity.clone()), "   ".to_string(), graph_mutation_env())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("non-empty"), "{err:#}");
        assert_eq!(stash_oids(repo_dir.path()), vec![oid]);
        assert_no_rename_recovery(repo_dir.path());

        // A missing identity is rejected uniquely (no recovery, nothing changed).
        let dangling = Oid::from_str("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap();
        let missing = StashIdentity {
            oid: dangling,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{0}".to_string(),
        };
        let err = repository
            .stash_rename(Some(missing), "new".to_string(), graph_mutation_env())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("stash not found"), "{err:#}");
        assert_eq!(stash_oids(repo_dir.path()), vec![oid]);
        assert!(stash_entries_newest_first(repo_dir.path())[0].1.contains("only"));
        assert_no_rename_recovery(repo_dir.path());

        drop(repository);
    }

    #[gpui::test]
    async fn test_stash_rename_every_boundary_failure_retains_recovery(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        // A failure at every destructive boundary must retain the manifest +
        // recovery refs and report them plus the observed stack.
        let boundaries = [
            ("after-write", StashRenameBoundary::AfterRecoveryWrite, false),
            ("before-rebuild", StashRenameBoundary::BeforeRebuild, false),
            ("mid-rebuild", StashRenameBoundary::MidRebuild, false),
            ("before-verify", StashRenameBoundary::BeforeVerify, false),
            ("cleanup", StashRenameBoundary::Cleanup, true),
        ];
        for (label, boundary, cleanup_only) in boundaries {
            let repo_dir = tempfile::tempdir().unwrap();
            git_init_repo(repo_dir.path());
            fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
            git_command(repo_dir.path(), ["add", "stash_file.txt"]);
            git_command(repo_dir.path(), ["commit", "-m", "base"]);
            let first = stash_with_marker(repo_dir.path(), "first");
            let second = stash_with_marker(repo_dir.path(), "second");
            stash_with_marker(repo_dir.path(), "third");
            let identity = StashIdentity {
                oid: second,
                ref_name: STASH_REF.to_string(),
                selector: "refs/stash@{1}".to_string(),
            };

            let repository = new_real_repo(repo_dir.path(), cx);
            set_stash_rename_fault(&repository, {
                let boundary = boundary;
                move |step| {
                    if step == boundary {
                        anyhow::bail!("injected {label} failure");
                    }
                    Ok(())
                }
            });
            let result = repository
                .stash_rename(Some(identity), "renamed".to_string(), graph_mutation_env())
                .await
                .unwrap();
            let recovery = if cleanup_only {
                assert!(
                    matches!(result, StashRenameResult::SuccessWithRecoveryRefs(_)),
                    "{label}: expected SuccessWithRecoveryRefs, got {result:?}"
                );
                match result {
                    StashRenameResult::SuccessWithRecoveryRefs(r) => r,
                    _ => unreachable!(),
                }
            } else {
                assert!(
                    matches!(result, StashRenameResult::FailedWithRecovery(_)),
                    "{label}: expected FailedWithRecovery, got {result:?}"
                );
                match result {
                    StashRenameResult::FailedWithRecovery(r) => r,
                    _ => unreachable!(),
                }
            };

            // Manifest + recovery refs retained and reported.
            let listed = stash_rename_recovery_refs(repo_dir.path());
            assert!(!listed.is_empty(), "{label}: no recovery refs retained");
            assert!(
                listed.iter().any(|r| *r == recovery.manifest_ref),
                "{label}: manifest ref {0} not in retained refs {listed:?}",
                recovery.manifest_ref
            );
            for ref_name in &recovery.recovery_refs {
                assert!(
                    listed.contains(ref_name),
                    "{label}: recovery ref {ref_name} not retained"
                );
            }
            assert!(
                recovery.manifest_ref.starts_with("refs/zed-git/stash-rename/"),
                "{label}: unexpected manifest ref"
            );
            assert!(
                recovery.recovery_refs.iter().all(|r| r.contains("/entry/")),
                "{label}: unexpected recovery ref"
            );

            // The commit OID protection refs prevent GC of the involved OIDs.
            for affected in [first, second] {
                assert!(
                    listed.iter().any(|r| {
                        git_command_output(
                            repo_dir.path(),
                            ["for-each-ref", "--format=%(objectname)", r],
                        )
                        .trim()
                            == affected.to_string()
                    }),
                    "{label}: involved OID {affected} not protected"
                );
            }

            clear_stash_rename_fault(&repository);
        }
    }

    #[gpui::test]
    async fn test_stash_rename_external_drop_between_steps_is_safe(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let first = stash_with_marker(repo_dir.path(), "first");
        let second = stash_with_marker(repo_dir.path(), "second");
        stash_with_marker(repo_dir.path(), "third");

        let identity = StashIdentity {
            oid: second,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{1}".to_string(),
        };

        let repo_dir_path = repo_dir.path().to_path_buf();
        let repository = new_real_repo(repo_dir.path(), cx);
        // An external drop of the top of stack happens at the revalidation
        // boundary (right before the destructive delete). The CAS delete must
        // detect it and refuse, retaining the recovery instead of clobbering.
        set_stash_rename_fault(&repository, move |step| {
            if step == StashRenameBoundary::BeforeRebuild {
                git_command(&repo_dir_path, ["stash", "drop", "stash@{0}"]);
            }
            Ok(())
        });
        let result = repository
            .stash_rename(Some(identity), "renamed".to_string(), graph_mutation_env())
            .await
            .unwrap();
        assert!(
            matches!(result, StashRenameResult::FailedWithRecovery(_)),
            "external drop mid-rename must fail safe, got {result:?}"
        );
        let StashRenameResult::FailedWithRecovery(recovery) = result else {
            unreachable!()
        };

        // Recovery retained + observed stack reflects the external drop (2 left).
        let listed = stash_rename_recovery_refs(repo_dir.path());
        assert!(!listed.is_empty(), "recovery must be retained");
        assert!(listed.contains(&recovery.manifest_ref));
        assert_eq!(recovery.observed_entries.len(), 2, "external drop observed");
        let observed_oids: Vec<Oid> =
            recovery.observed_entries.iter().map(|e| e.oid).collect();
        assert_eq!(observed_oids, vec![second, first], "post-drop observed stack");
        clear_stash_rename_fault(&repository);
    }

    #[gpui::test]
    async fn test_stash_rename_partial_replay_is_discovered_after_restart(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let _first = stash_with_marker(repo_dir.path(), "first");
        let second = stash_with_marker(repo_dir.path(), "second");
        stash_with_marker(repo_dir.path(), "third");

        let identity = StashIdentity {
            oid: second,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{1}".to_string(),
        };

        // Fail right after the stash tip is deleted (mid-rebuild): the stack is
        // partially replayed and the recovery refs must persist for discovery.
        let repository = new_real_repo(repo_dir.path(), cx);
        set_stash_rename_fault(&repository, |step| {
            if step == StashRenameBoundary::MidRebuild {
                anyhow::bail!("injected mid-rebuild failure");
            }
            Ok(())
        });
        let result = repository
            .stash_rename(Some(identity), "renamed".to_string(), graph_mutation_env())
            .await
            .unwrap();
        assert!(matches!(result, StashRenameResult::FailedWithRecovery(_)));
        clear_stash_rename_fault(&repository);

        // A fresh repository (simulating a Zed restart) must discover the
        // unfinished recovery: manifest ref + recovery refs + observed stack.
        let listed = stash_rename_recovery_refs(repo_dir.path());
        assert!(!listed.is_empty(), "recovery refs must survive restart");
        let pending = new_real_repo(repo_dir.path(), cx)
            .pending_stash_rename_recovers()
            .await
            .unwrap();
        assert_eq!(pending.len(), 1, "exactly one unfinished recovery");
        let recovery = &pending[0];
        assert!(
            listed.contains(&recovery.manifest_ref),
            "discovered manifest ref not in retained refs"
        );
        for ref_name in &recovery.recovery_refs {
            assert!(
                listed.contains(ref_name),
                "discovered recovery ref {ref_name} missing"
            );
        }
        assert!(
            recovery.manifest_ref.starts_with("refs/zed-git/stash-rename/"),
            "unexpected manifest ref: {}",
            recovery.manifest_ref
        );
        // A MidRebuild failure deletes the stash tip but never rebuilds it, so
        // the observed stack is empty at discovery — exactly why the manifest
        // retains the captured stack for recovery.
        assert_eq!(
            recovery.observed_entries.len(),
            0,
            "mid-rebuild leaves the stack empty until recovery"
        );
    }

    #[gpui::test]
    async fn test_stash_rename_cleanup_failure_reports_recovery_and_is_discoverable(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        stash_with_marker(repo_dir.path(), "first");
        let second = stash_with_marker(repo_dir.path(), "second");

        let identity = StashIdentity {
            oid: second,
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{0}".to_string(),
        };

        let repository = new_real_repo(repo_dir.path(), cx);
        set_stash_rename_fault(&repository, |step| {
            if step == StashRenameBoundary::Cleanup {
                anyhow::bail!("injected cleanup failure");
            }
            Ok(())
        });
        let result = repository
            .stash_rename(Some(identity), "renamed top".to_string(), graph_mutation_env())
            .await
            .unwrap();
        let StashRenameResult::SuccessWithRecoveryRefs(recovery) = result else {
            panic!("cleanup-only failure should report applied rename, got {result:?}")
        };
        assert!(recovery.rename_applied, "rename applied and verified");
        // The rename visibly applied.
        let entries = stash_entries_newest_first(repo_dir.path());
        assert_eq!(entries[0].1, "On main: renamed top");
        assert!(stash_rename_recovery_refs(repo_dir.path()).contains(&recovery.manifest_ref));
        clear_stash_rename_fault(&repository);

        // Discovery sees the applied rename (rename_applied == true) since the
        // observed stack already carries the rewritten commit.
        let pending = new_real_repo(repo_dir.path(), cx)
            .pending_stash_rename_recovers()
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert!(
            pending[0].rename_applied,
            "discovered an applied-but-uncleaned rename"
        );
    }

    #[gpui::test]
    async fn test_stash_graph_data_enumerates_reflog_rows(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);

        let first = stash_with_marker(repo_dir.path(), "first");
        stash_with_marker(repo_dir.path(), "second");

        let repository = new_real_repo(repo_dir.path(), cx);
        let rows = repository.stash_graph_data().await.unwrap();

        // Newest stash first, each carrying its reflog selector identity and
        // only its first parent (the base).
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].sha, second_stash_oid(repo_dir.path()));
        assert_eq!(rows[1].sha, first);
        assert_eq!(rows[0].ref_names.first().map(|n| n.as_str()), Some("refs/stash@{0}"));
        assert_eq!(rows[1].ref_names.first().map(|n| n.as_str()), Some("refs/stash@{1}"));
        assert!(
            rows.iter().all(|row| row.parents.len() <= 1),
            "stash rows must keep only the first parent"
        );
    }

    #[gpui::test]
    async fn test_graph_commit_for_base_fetches_exact_commit(cx: &mut TestAppContext) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        stash_with_marker(repo_dir.path(), "only");

        let repository = new_real_repo(repo_dir.path(), cx);
        let base_oid = Oid::from_str(&git_command_output(
            repo_dir.path(),
            ["rev-parse", "HEAD"],
        ))
        .unwrap();
        // The base is a reachable regular commit: it exists as a commit.
        let row = repository
            .graph_commit_for_base(base_oid)
            .await
            .unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().sha, base_oid);

        let missing = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
            .parse()
            .unwrap();
        assert!(
            repository
                .graph_commit_for_base(missing)
                .await
                .unwrap()
                .is_none(),
            "a non-existent base must yield no row"
        );
    }

    #[gpui::test]
    async fn test_log_source_all_includes_stash_rows_but_others_do_not(
        cx: &mut TestAppContext,
    ) {
        disable_git_global_config();
        cx.executor().allow_parking();

        let repo_dir = tempfile::tempdir().unwrap();
        git_init_repo(repo_dir.path());
        fs::write(repo_dir.path().join("stash_file.txt"), "base\n").unwrap();
        git_command(repo_dir.path(), ["add", "stash_file.txt"]);
        git_command(repo_dir.path(), ["commit", "-m", "base"]);
        let stash_oid = stash_with_marker(repo_dir.path(), "only");

        let repository = new_real_repo(repo_dir.path(), cx);
        let (tx, rx) = smol::channel::unbounded();
        repository
            .initial_graph_data(LogSource::All, LogOrder::DateOrder, tx.clone())
            .await
            .unwrap();
        let mut all_shas = Vec::new();
        while let Ok(chunk) = rx.try_recv() {
            all_shas.extend(chunk.iter().map(|c| c.sha));
        }
        // The stash row appears ahead of the regular commits, carrying its
        // reflog-selector identity.
        assert!(
            all_shas.contains(&stash_oid),
            "LogSource::All must render the stash row"
        );

        // A branch/path/sha load must not pull the stash row in.
        let (tx, rx) = smol::channel::unbounded();
        repository
            .initial_graph_data(LogSource::Branch("main".into()), LogOrder::DateOrder, tx)
            .await
            .unwrap();
        let mut branch_shas = Vec::new();
        while let Ok(chunk) = rx.try_recv() {
            branch_shas.extend(chunk.iter().map(|c| c.sha));
        }
        assert!(
            !branch_shas.contains(&stash_oid),
            "branch history must not include the stash row"
        );
    }

    fn second_stash_oid(repo_dir: &Path) -> Oid {
        git_command_output(repo_dir, ["rev-parse", "refs/stash"])
            .trim()
            .parse()
            .unwrap()
    }
}
