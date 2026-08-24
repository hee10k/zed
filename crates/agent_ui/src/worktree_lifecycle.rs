use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};

use anyhow::{Context as _, Result, anyhow};
use async_lock::Mutex;
use fs::{Fs, RemoveOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use workspace::WorkspaceId;

const WORKTREE_STATE_DIRECTORY: &str = "worktree-state";
static GLOBAL_LIFECYCLE_OPERATION_LOCK: LazyLock<Arc<Mutex<()>>> =
    LazyLock::new(|| Arc::new(Mutex::new(())));

fn global_lifecycle_operation_lock() -> Arc<Mutex<()>> {
    GLOBAL_LIFECYCLE_OPERATION_LOCK.clone()
}
const LIFECYCLE_FILE_NAME: &str = "state.json";

/// Stable identity for one worktree incarnation.
///
/// Callers should provide canonical paths. [`WorktreeLifecycleCoordinator`] also
/// exposes path-based reconciliation and canonicalizes paths before constructing
/// this key, while retaining a lexical path when the worktree is temporarily
/// unavailable.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WorktreeLifecycleKey {
    pub repository_path: PathBuf,
    pub worktree_path: PathBuf,
    pub remote_identity: String,
}

impl WorktreeLifecycleKey {
    pub fn new(
        repository_path: impl Into<PathBuf>,
        worktree_path: impl Into<PathBuf>,
        remote_identity: impl Into<String>,
    ) -> Self {
        Self {
            repository_path: repository_path.into(),
            worktree_path: worktree_path.into(),
            remote_identity: remote_identity.into(),
        }
    }

    /// Constructs a filesystem-safe, deterministic directory name.
    pub fn stable_name(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.repository_path.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(self.worktree_path.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(self.remote_identity.as_bytes());
        let digest = hasher.finalize();
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    pub fn stable_key(&self) -> gpui::SharedString {
        gpui::SharedString::from(self.stable_name())
    }
}

/// Derives a restart-stable worktree identity string suitable for `ThreadMetadata.worktree_id`.
pub fn stable_worktree_identity(
    repository_path: impl AsRef<Path>,
    worktree_path: impl AsRef<Path>,
    remote_identity: impl AsRef<str>,
) -> gpui::SharedString {
    WorktreeLifecycleKey::new(
        repository_path.as_ref(),
        worktree_path.as_ref(),
        remote_identity.as_ref(),
    )
    .stable_key()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeLifecycleState {
    Active,
    Closing,
    Unavailable,
    Removed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorktreeTerminalLocator {
    pub harness: String,
    pub locator: String,
}

impl WorktreeTerminalLocator {
    pub fn new(harness: impl Into<String>, locator: impl Into<String>) -> Self {
        Self {
            harness: harness.into(),
            locator: locator.into(),
        }
    }
}

/// Operational state owned by a worktree. Durable conversation history remains
/// in SQLite and is intentionally not represented here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorktreeLifecycleRecord {
    pub key: WorktreeLifecycleKey,
    pub workspace_id: Option<WorkspaceId>,
    pub last_seen_workspace_id: Option<WorkspaceId>,
    #[serde(default)]
    pub workspace_ids: Vec<WorkspaceId>,
    pub root_group_id: Option<String>,
    pub derived_worktree_ids: Vec<String>,
    pub derived_thread_ids: Vec<String>,
    pub terminal_locators: Vec<WorktreeTerminalLocator>,
    pub state: WorktreeLifecycleState,
    pub checkpoint: Option<String>,
}

impl WorktreeLifecycleRecord {
    pub fn new(key: WorktreeLifecycleKey, workspace_id: WorkspaceId) -> Self {
        Self {
            key,
            workspace_id: Some(workspace_id),
            last_seen_workspace_id: Some(workspace_id),
            workspace_ids: vec![workspace_id],
            root_group_id: None,
            derived_worktree_ids: Vec::new(),
            derived_thread_ids: Vec::new(),
            terminal_locators: Vec::new(),
            state: WorktreeLifecycleState::Active,
            checkpoint: None,
        }
    }

    fn reconcile(&mut self, workspace_id: WorkspaceId, path_exists: Option<bool>) -> bool {
        let old_state = self.state.clone();
        let workspace_changed = self.workspace_id != Some(workspace_id)
            || self.last_seen_workspace_id != Some(workspace_id);
        let owner_added = if self.workspace_ids.contains(&workspace_id) {
            false
        } else {
            self.workspace_ids.push(workspace_id);
            true
        };
        self.workspace_id = Some(workspace_id);
        self.last_seen_workspace_id = Some(workspace_id);
        if !matches!(&self.state, WorktreeLifecycleState::Removed)
            && (owner_added && self.state == WorktreeLifecycleState::Closing
                || !matches!(&self.state, WorktreeLifecycleState::Closing)
                    && path_exists.is_some())
        {
            self.state = if owner_added && path_exists.is_none() {
                WorktreeLifecycleState::Active
            } else if path_exists.unwrap_or(true) {
                WorktreeLifecycleState::Active
            } else {
                WorktreeLifecycleState::Unavailable
            };
        }
        workspace_changed || owner_added || old_state != self.state
    }
}

/// A worktree observed in a restored project. The coordinator resolves its
/// canonical identity and checks whether its root is still present.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreeLifecycleWorktree {
    pub repository_path: PathBuf,
    pub worktree_path: PathBuf,
    pub remote_identity: String,
}

impl WorktreeLifecycleWorktree {
    pub fn new(
        repository_path: impl Into<PathBuf>,
        worktree_path: impl Into<PathBuf>,
        remote_identity: impl Into<String>,
    ) -> Self {
        Self {
            repository_path: repository_path.into(),
            worktree_path: worktree_path.into(),
            remote_identity: remote_identity.into(),
        }
    }
}

#[derive(Clone)]
pub struct WorktreeLifecycleStore {
    fs: Arc<dyn Fs>,
    root: PathBuf,
}

impl WorktreeLifecycleStore {
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self::with_root(fs, paths::data_dir())
    }

    /// Creates a store rooted at `root`, useful for isolated tests and callers
    /// that provide an alternate Zed data directory.
    pub fn with_root(fs: Arc<dyn Fs>, root: impl Into<PathBuf>) -> Self {
        Self {
            fs,
            root: root.into().join(WORKTREE_STATE_DIRECTORY),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path_for(&self, key: &WorktreeLifecycleKey) -> PathBuf {
        self.root.join(key.stable_name()).join(LIFECYCLE_FILE_NAME)
    }

    pub async fn load(
        &self,
        key: &WorktreeLifecycleKey,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        let path = self.path_for(key);
        if !self.fs.is_file(&path).await {
            return Ok(None);
        }
        let contents = self
            .fs
            .load(&path)
            .await
            .with_context(|| format!("loading worktree lifecycle file {}", path.display()))?;
        let record = match serde_json::from_str::<WorktreeLifecycleRecord>(&contents) {
            Ok(record) => record,
            Err(error) => {
                log::error!(
                    "skipping malformed worktree lifecycle file {}: {error:#}",
                    path.display()
                );
                return Ok(None);
            }
        };
        let matches_directory = path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|directory| directory.to_string_lossy() == record.key.stable_name());
        if record.key != *key || !matches_directory {
            log::error!(
                "skipping worktree lifecycle file with mismatched key {}",
                path.display()
            );
            return Ok(None);
        }
        Ok(Some(record))
    }

    pub async fn save(&self, record: &WorktreeLifecycleRecord) -> Result<()> {
        let path = self.path_for(&record.key);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("lifecycle path has no parent: {}", path.display()))?;
        self.fs
            .create_dir(parent)
            .await
            .with_context(|| format!("creating worktree lifecycle directory {}", parent.display()))?;
        let contents = serde_json::to_string_pretty(record).context("encoding worktree lifecycle record")?;
        // Fs::atomic_write writes a sibling temporary file and renames it into
        // place, so readers never observe a partially written JSON document.
        self.fs
            .atomic_write(path.clone(), contents)
            .await
            .with_context(|| format!("saving worktree lifecycle file {}", path.display()))
    }

    pub async fn mark_closing(
        &self,
        key: &WorktreeLifecycleKey,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        self.mutate_state(key, WorktreeLifecycleState::Closing).await
    }

    pub async fn mark_unavailable(
        &self,
        key: &WorktreeLifecycleKey,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        self.mutate_state(key, WorktreeLifecycleState::Unavailable).await
    }

    pub async fn mark_active(
        &self,
        key: &WorktreeLifecycleKey,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        self.mutate_state(key, WorktreeLifecycleState::Active).await
    }

    pub async fn remove(&self, key: &WorktreeLifecycleKey) -> Result<()> {
        let path = self.path_for(key);
        let directory = path
            .parent()
            .ok_or_else(|| anyhow!("lifecycle path has no parent"))?;
        self.fs
            .remove_dir(
                directory,
                RemoveOptions {
                    recursive: true,
                    ignore_if_not_exists: true,
                },
            )
            .await
            .with_context(|| format!("removing worktree lifecycle directory {}", directory.display()))
    }

    pub async fn list(&self) -> Result<Vec<WorktreeLifecycleRecord>> {
        if !self.fs.is_dir(&self.root).await {
            return Ok(Vec::new());
        }
        let mut entries = self
            .fs
            .read_dir(&self.root)
            .await
            .with_context(|| format!("reading worktree lifecycle directory {}", self.root.display()))?;
        let mut records = Vec::new();
        while let Some(entry) = futures::StreamExt::next(&mut entries).await {
            let entry = entry?;
            let path = entry.join(LIFECYCLE_FILE_NAME);
            if !self.fs.is_file(&path).await {
                continue;
            }
            let contents = self.fs.load(&path).await.with_context(|| {
                format!("loading worktree lifecycle file {}", path.display())
            })?;
            match serde_json::from_str::<WorktreeLifecycleRecord>(&contents) {
                Ok(record) => {
                    let matches_directory = path
                        .parent()
                        .and_then(Path::file_name)
                        .is_some_and(|directory| {
                            directory.to_string_lossy() == record.key.stable_name()
                        });
                    if matches_directory {
                        records.push(record);
                    } else {
                        log::error!(
                            "skipping worktree lifecycle file with mismatched key {}",
                            path.display()
                        );
                    }
                }
                Err(error) => {
                    log::error!(
                        "skipping malformed worktree lifecycle file {}: {error:#}",
                        path.display()
                    );
                }
            }
        }
        Ok(records)
    }

    async fn mutate_state(
        &self,
        key: &WorktreeLifecycleKey,
        state: WorktreeLifecycleState,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        let Some(mut record) = self.load(key).await? else {
            return Ok(None);
        };
        record.state = state;
        self.save(&record).await?;
        Ok(Some(record))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreeLifecycleDescendant {
    pub id: String,
    pub path: Option<PathBuf>,
    pub active: bool,
    pub dirty: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreeDeletionConfirmation {
    pub key: WorktreeLifecycleKey,
    pub active_descendants: Vec<String>,
    pub dirty_descendants: Vec<String>,
    pub lifecycle_files: Vec<PathBuf>,
    pub linked_sessions: Vec<WorktreeTerminalLocator>,
}

#[derive(Clone)]
pub struct WorktreeLifecycleCoordinator {
    store: WorktreeLifecycleStore,
    operation_lock: Arc<Mutex<()>>,
}

impl WorktreeLifecycleCoordinator {
    pub fn new(store: WorktreeLifecycleStore) -> Self {
        Self {
            store,
            operation_lock: global_lifecycle_operation_lock(),
        }
    }

    pub fn store(&self) -> &WorktreeLifecycleStore {
        &self.store
    }

    pub async fn reconcile_workspace(
        &self,
        workspace_id: WorkspaceId,
        worktrees: &[WorktreeLifecycleWorktree],
    ) -> Result<Vec<WorktreeLifecycleRecord>> {
        let _operation_guard = self.operation_lock.lock().await;
        let mut seen_keys = HashSet::with_capacity(worktrees.len());
        let mut reconciled = Vec::with_capacity(worktrees.len());

        for worktree in worktrees {
            let key = self.key_for(worktree).await?;
            seen_keys.insert(key.clone());
            let path_exists = if worktree.remote_identity == "local" {
                Some(self.store.fs.is_dir(&key.worktree_path).await)
            } else {
                None
            };
            let existing = self.store.load(&key).await?;
            let was_missing = existing.is_none();
            let mut record = match existing {
                Some(record) => record,
                None => WorktreeLifecycleRecord::new(key.clone(), workspace_id),
            };
            let record_changed = record.reconcile(workspace_id, path_exists);
            if was_missing || record_changed {
                self.store.save(&record).await?;
            }
            reconciled.push(record);
        }

        // A restored project may have moved or lost a root path. Keep those
        // records visible as unavailable; never retarget them to a new path.
        for mut record in self.store.list().await? {
            if record.workspace_id == Some(workspace_id)
                && !seen_keys.contains(&record.key)
                && !matches!(
                    &record.state,
                    WorktreeLifecycleState::Closing | WorktreeLifecycleState::Removed
                )
            {
                record.state = WorktreeLifecycleState::Unavailable;
                self.store.save(&record).await?;
            }
        }

        Ok(reconciled)
    }

    pub async fn mark_worktree_closing(
        &self,
        key: &WorktreeLifecycleKey,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        let _operation_guard = self.operation_lock.lock().await;
        self.store.mark_closing(key).await
    }

    pub async fn release_worktree_owner(
        &self,
        key: &WorktreeLifecycleKey,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorktreeLifecycleRecord>> {
        let _operation_guard = self.operation_lock.lock().await;
        let Some(mut record) = self.store.load(key).await? else {
            return Ok(None);
        };
        let old_state = record.state.clone();
        let old_owners = record.workspace_ids.clone();
        record.workspace_ids.retain(|owner| *owner != workspace_id);
        if record.workspace_ids.is_empty() {
            record.state = WorktreeLifecycleState::Closing;
        } else {
            record.state = WorktreeLifecycleState::Active;
            record.workspace_id = record.workspace_ids.first().copied();
        }
        record.last_seen_workspace_id = Some(workspace_id);
        if old_state != record.state || old_owners != record.workspace_ids {
            self.store.save(&record).await?;
        }
        Ok(Some(record))
    }

    pub async fn release_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<WorktreeLifecycleRecord>> {
        let _operation_guard = self.operation_lock.lock().await;
        let mut changed = Vec::new();
        for mut record in self.store.list().await? {
            let owns_record = record.workspace_ids.contains(&workspace_id)
                || (record.workspace_ids.is_empty()
                    && record.workspace_id == Some(workspace_id));
            if !owns_record {
                continue;
            }
            let old_state = record.state.clone();
            record.workspace_ids.retain(|owner| *owner != workspace_id);
            if record.workspace_ids.is_empty() {
                record.state = WorktreeLifecycleState::Closing;
            } else {
                record.state = WorktreeLifecycleState::Active;
                record.workspace_id = record.workspace_ids.first().copied();
            }
            record.last_seen_workspace_id = Some(workspace_id);
            if old_state != record.state {
                changed.push(record.clone());
            } else {
                changed.push(record.clone());
            }
            self.store.save(&record).await?;
        }
        Ok(changed)
    }
    pub async fn flush_workspace(&self, workspace_id: WorkspaceId) -> Result<()> {
        let _operation_guard = self.operation_lock.lock().await;
        for record in self.store.list().await? {
            if record.workspace_ids.contains(&workspace_id)
                || record.workspace_id == Some(workspace_id)
            {
                self.store.save(&record).await?;
            }
        }
        Ok(())
    }


    pub async fn mark_workspace_closing(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Vec<WorktreeLifecycleRecord>> {
        let _operation_guard = self.operation_lock.lock().await;
        let mut records = self.store.list().await?;
        let mut changed = Vec::new();
        for record in &mut records {
            if record.workspace_id == Some(workspace_id)
                && record.state != WorktreeLifecycleState::Removed
                && record.state != WorktreeLifecycleState::Closing
            {
                record.state = WorktreeLifecycleState::Closing;
                changed.push(record.clone());
            }
        }
        for record in &changed {
            self.store.save(record).await?;
        }
        Ok(changed)
    }

    pub async fn prepare_deletion(
        &self,
        key: &WorktreeLifecycleKey,
        descendants: &[WorktreeLifecycleDescendant],
    ) -> Result<WorktreeDeletionConfirmation> {
        let _operation_guard = self.operation_lock.lock().await;
        self.store.mark_closing(key).await?;
        let record = self
            .store
            .load(key)
            .await?
            .ok_or_else(|| anyhow!("no lifecycle record for {}", key.stable_name()))?;
        Ok(WorktreeDeletionConfirmation {
            key: key.clone(),
            active_descendants: descendants
                .iter()
                .filter(|descendant| descendant.active)
                .map(|descendant| descendant.id.clone())
                .collect(),
            dirty_descendants: descendants
                .iter()
                .filter(|descendant| descendant.dirty)
                .map(|descendant| descendant.id.clone())
                .collect(),
            lifecycle_files: vec![self.store.path_for(key)],
            linked_sessions: record.terminal_locators,
        })
    }

    pub async fn confirm_deletion(&self, key: &WorktreeLifecycleKey) -> Result<()> {
        let _operation_guard = self.operation_lock.lock().await;
        self.store.remove(key).await
    }

    pub async fn remove_worktree(&self, key: &WorktreeLifecycleKey) -> Result<()> {
        let _operation_guard = self.operation_lock.lock().await;
        self.store.remove(key).await
    }

    pub async fn flush(&self, records: &[WorktreeLifecycleRecord]) -> Result<()> {
        let _operation_guard = self.operation_lock.lock().await;
        for record in records {
            self.store.save(record).await?;
        }
        Ok(())
    }

    pub async fn key_for(
        &self,
        worktree: &WorktreeLifecycleWorktree,
    ) -> Result<WorktreeLifecycleKey> {
        let (repository_path, worktree_path) = if worktree.remote_identity == "local" {
            (
                canonical_path(&*self.store.fs, &worktree.repository_path).await?,
                canonical_path(&*self.store.fs, &worktree.worktree_path).await?,
            )
        } else {
            (
                worktree.repository_path.clone(),
                worktree.worktree_path.clone(),
            )
        };
        Ok(WorktreeLifecycleKey::new(
            repository_path,
            worktree_path,
            worktree.remote_identity.clone(),
        ))
    }
}

async fn canonical_path(fs: &dyn Fs, path: &Path) -> Result<PathBuf> {
    if let Ok(canonical) = fs.canonicalize(path).await {
        return Ok(canonical);
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("resolving relative worktree lifecycle path")?
        .join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs::RealFs;
    use gpui::TestAppContext;
    use tempfile::TempDir;

    fn key(repository: &Path, worktree: &Path) -> WorktreeLifecycleKey {
        WorktreeLifecycleKey::new(repository, worktree, "local")
    }

    fn record(key: WorktreeLifecycleKey) -> WorktreeLifecycleRecord {
        let mut record = WorktreeLifecycleRecord::new(key, WorkspaceId::from_i64(7));
        record.root_group_id = Some("group-root".to_string());
        record.derived_worktree_ids = vec!["worktree-derived".to_string()];
        record.derived_thread_ids = vec!["thread-derived".to_string()];
        record.terminal_locators = vec![WorktreeTerminalLocator::new("omp", "/tmp/session")];
        record.checkpoint = Some("saved".to_string());
        record
    }

    #[test]
    fn lifecycle_key_is_deterministic_and_scoped() {
        let first = WorktreeLifecycleKey::new("/repo/.git", "/repo", "local");
        let second = WorktreeLifecycleKey::new("/repo/.git", "/repo", "local");
        let remote = WorktreeLifecycleKey::new("/repo/.git", "/repo", "ssh:zed");
        assert_eq!(first, second);
        assert_eq!(first.stable_name(), second.stable_name());
        assert_ne!(first.stable_name(), remote.stable_name());
        assert_eq!(first.stable_name().len(), 64);
    }

    #[test]
    fn lifecycle_record_json_round_trip() {
        let original = record(key(Path::new("/repo/.git"), Path::new("/repo")));
        let encoded = serde_json::to_string(&original).expect("serialize lifecycle record");
        let decoded: WorktreeLifecycleRecord =
            serde_json::from_str(&encoded).expect("deserialize lifecycle record");
        assert_eq!(decoded, original);
    }

    #[gpui::test]
    async fn lifecycle_store_recovers_missing_file(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs, temp.path());
        let key = key(Path::new("/repo/.git"), Path::new("/repo"));
        assert_eq!(store.load(&key).await.expect("load missing record"), None);
        let saved = record(key.clone());
        store.save(&saved).await.expect("save lifecycle record");
        assert_eq!(store.load(&key).await.expect("load saved record"), Some(saved));
    }

    #[gpui::test]
    async fn lifecycle_store_removal_cleans_directory(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs.clone(), temp.path());
        let key = key(Path::new("/repo/.git"), Path::new("/repo"));
        store.save(&record(key.clone())).await.expect("save lifecycle record");
        assert!(fs.is_file(&store.path_for(&key)).await);
        store.remove(&key).await.expect("remove lifecycle record");
        assert!(!fs.is_file(&store.path_for(&key)).await);
    }

    #[gpui::test]
    async fn lifecycle_coordinator_preserves_explicit_terminal_states(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs.clone(), temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let workspace_id = WorkspaceId::from_i64(8);
        let repository_path = temp.path().join("repository");
        let worktree_path = temp.path().join("worktree");
        fs.create_dir(&worktree_path)
            .await
            .expect("create worktree path");
        let worktree = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            worktree_path.clone(),
            "local",
        );
        let key = WorktreeLifecycleKey::new(repository_path, worktree_path, "local");

        let mut closing = WorktreeLifecycleRecord::new(key.clone(), workspace_id);
        closing.state = WorktreeLifecycleState::Closing;
        store.save(&closing).await.expect("save closing record");
        let reconciled = coordinator
            .reconcile_workspace(workspace_id, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile closing record");
        assert_eq!(reconciled[0].state, WorktreeLifecycleState::Closing);
        coordinator
            .reconcile_workspace(workspace_id, &[])
            .await
            .expect("reconcile unseen closing record");
        assert_eq!(
            store
                .load(&key)
                .await
                .expect("load unseen closing record")
                .expect("closing record should remain persisted")
                .state,
            WorktreeLifecycleState::Closing
        );

        let mut removed = closing;
        removed.state = WorktreeLifecycleState::Removed;
        store.save(&removed).await.expect("save removed record");
        let reconciled = coordinator
            .reconcile_workspace(workspace_id, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile removed record");
        assert_eq!(reconciled[0].state, WorktreeLifecycleState::Removed);
    }

    #[gpui::test]
    async fn lifecycle_coordinator_marks_unavailable_and_not_seen_records(
        cx: &mut TestAppContext,
    ) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs, temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let workspace_id = WorkspaceId::from_i64(9);
        let repository_path = temp.path().join("repository");
        let worktree_path = temp.path().join("missing-worktree");
        let worktree = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            worktree_path.clone(),
            "local",
        );
        let key = WorktreeLifecycleKey::new(repository_path, worktree_path, "local");
        store
            .save(&WorktreeLifecycleRecord::new(key.clone(), workspace_id))
            .await
            .expect("save active record");

        let reconciled = coordinator
            .reconcile_workspace(workspace_id, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile unavailable record");
        assert_eq!(reconciled[0].state, WorktreeLifecycleState::Unavailable);

        coordinator
            .reconcile_workspace(workspace_id, &[])
            .await
            .expect("reconcile not-seen record");
        let persisted = store
            .load(&key)
            .await
            .expect("load unavailable record")
            .expect("unavailable record should remain persisted");
        assert_eq!(persisted.state, WorktreeLifecycleState::Unavailable);
    }

    #[gpui::test]
    async fn lifecycle_coordinator_recovers_from_corrupt_file(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs.clone(), temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let workspace_id = WorkspaceId::from_i64(10);
        let repository_path = temp.path().join("repository");
        let worktree_path = temp.path().join("missing-worktree");
        let worktree = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            worktree_path.clone(),
            "local",
        );
        let key = WorktreeLifecycleKey::new(repository_path, worktree_path, "local");
        let path = store.path_for(&key);
        fs.create_dir(path.parent().expect("lifecycle file parent"))
            .await
            .expect("create lifecycle file parent");
        fs.atomic_write(path, "{not valid json".to_string())
            .await
            .expect("write corrupt lifecycle file");

        assert_eq!(store.load(&key).await.expect("load corrupt record"), None);
        assert!(
            store
                .list()
                .await
                .expect("list corrupt records")
                .is_empty()
        );

        let reconciled = coordinator
            .reconcile_workspace(workspace_id, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile around corrupt record");
        assert_eq!(reconciled[0].state, WorktreeLifecycleState::Unavailable);
        assert_eq!(
            store
                .load(&key)
                .await
                .expect("load recovered record")
                .expect("reconciliation should replace corrupt record")
                .state,
            WorktreeLifecycleState::Unavailable
        );
    }
    #[gpui::test]
    async fn lifecycle_coordinator_removes_only_requested_worktree(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs.clone(), temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let workspace_id = WorkspaceId::from_i64(11);
        let repository_path = temp.path().join("repository");
        let first_path = temp.path().join("worktree-first");
        let second_path = temp.path().join("worktree-second");
        fs.create_dir(&first_path)
            .await
            .expect("create first worktree path");
        fs.create_dir(&second_path)
            .await
            .expect("create second worktree path");
        let first = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            first_path.clone(),
            "local",
        );
        let second = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            second_path.clone(),
            "local",
        );
        coordinator
            .reconcile_workspace(workspace_id, &[first, second])
            .await
            .expect("reconcile multi-worktree workspace");

        let first_key = WorktreeLifecycleKey::new(repository_path.clone(), first_path, "local");
        let second_key = WorktreeLifecycleKey::new(repository_path, second_path, "local");
        coordinator
            .remove_worktree(&first_key)
            .await
            .expect("remove one worktree lifecycle record");
        assert!(
            store
                .load(&first_key)
                .await
                .expect("load removed record")
                .is_none()
        );
        assert_eq!(
            store
                .load(&second_key)
                .await
                .expect("load retained record")
                .expect("second worktree should remain")
                .state,
            WorktreeLifecycleState::Active
        );
    }

    #[gpui::test]
    async fn lifecycle_reconcile_persists_workspace_id_reassignment(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs, temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let first_workspace = WorkspaceId::from_i64(12);
        let second_workspace = WorkspaceId::from_i64(13);
        let repository_path = temp.path().join("repository");
        let worktree_path = temp.path().join("worktree");
        let worktree = WorktreeLifecycleWorktree::new(
            repository_path.clone(),
            worktree_path.clone(),
            "local",
        );
        coordinator
            .reconcile_workspace(first_workspace, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile first workspace");
        coordinator
            .reconcile_workspace(second_workspace, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile reassigned workspace");

        let key = WorktreeLifecycleKey::new(repository_path, worktree_path, "local");
        let persisted = store
            .load(&key)
            .await
            .expect("load reassigned record")
            .expect("record should persist");
        assert_eq!(persisted.workspace_id, Some(second_workspace));
        assert_eq!(persisted.last_seen_workspace_id, Some(second_workspace));
    }

    #[gpui::test]
    async fn lifecycle_store_skips_key_file_mismatch(cx: &mut TestAppContext) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs.clone(), temp.path());
        let expected_key = key(Path::new("/repo/.git"), Path::new("/repo"));
        let embedded_key = key(Path::new("/other/.git"), Path::new("/other"));
        let path = store.path_for(&expected_key);
        fs.create_dir(path.parent().expect("lifecycle file parent"))
            .await
            .expect("create lifecycle file parent");
        fs.atomic_write(
            path,
            serde_json::to_string(&record(embedded_key)).expect("encode mismatched record"),
        )
        .await
        .expect("write mismatched record");

        assert!(
            store
                .load(&expected_key)
                .await
                .expect("load mismatched record")
                .is_none()
        );
        assert!(
            store
                .list()
                .await
                .expect("list mismatched record")
                .is_empty()
        );
    }
    #[gpui::test]
    async fn lifecycle_reconcile_preserves_remote_state_without_local_fs(
        cx: &mut TestAppContext,
    ) {
        let temp = TempDir::new().expect("create temporary lifecycle root");
        let fs = Arc::new(RealFs::new(None, cx.executor()));
        let store = WorktreeLifecycleStore::with_root(fs, temp.path());
        let coordinator = WorktreeLifecycleCoordinator::new(store.clone());
        let workspace_id = WorkspaceId::from_i64(14);
        let repository_path = PathBuf::from("/remote/repository/.git");
        let worktree_path = PathBuf::from("/remote/worktree");
        let key = WorktreeLifecycleKey::new(
            repository_path.clone(),
            worktree_path.clone(),
            "remote-v1:ssh-host",
        );
        let mut existing = WorktreeLifecycleRecord::new(key, WorkspaceId::from_i64(13));
        existing.state = WorktreeLifecycleState::Unavailable;
        store.save(&existing).await.expect("save remote record");

        let worktree = WorktreeLifecycleWorktree::new(
            repository_path,
            worktree_path,
            "remote-v1:ssh-host",
        );
        let reconciled = coordinator
            .reconcile_workspace(workspace_id, std::slice::from_ref(&worktree))
            .await
            .expect("reconcile remote worktree");
        assert_eq!(reconciled[0].state, WorktreeLifecycleState::Unavailable);
        assert_eq!(reconciled[0].workspace_id, Some(workspace_id));
    }
}
