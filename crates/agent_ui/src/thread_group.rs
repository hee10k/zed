use std::path::{Path, PathBuf};

use db::sqlez::{
    bindable::{Bind, Column, StaticColumnCount},
    statement::Statement,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ThreadGroupId(uuid::Uuid);

impl StaticColumnCount for ThreadGroupId {}

impl ThreadGroupId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }

    pub fn to_key_string(&self) -> String {
        self.0.hyphenated().to_string()
    }
}
/// Returns the restart-stable worktree identity used by lifecycle JSON records.
pub fn stable_worktree_id(
    repository_path: &Path,
    worktree_path: &Path,
    remote_identity: &str,
) -> SharedString {
    WorktreeLifecycleKey::new(repository_path, worktree_path, remote_identity)
        .stable_key()
        .into()
}


impl Bind for ThreadGroupId {
    fn bind(&self, statement: &Statement, start_index: i32) -> anyhow::Result<i32> {
        self.0.bind(statement, start_index)
    }
}

impl Column for ThreadGroupId {
    fn column(statement: &mut Statement, start_index: i32) -> anyhow::Result<(Self, i32)> {
        let (uuid, next) = Column::column(statement, start_index)?;
        Ok((ThreadGroupId(uuid), next))
    }
}

use chrono::Utc;
use project::WorktreePaths;
use ui::{Context, SharedString};

use crate::thread_metadata_store::{ThreadId, ThreadMetadata, ThreadMetadataStore};

use crate::worktree_lifecycle::WorktreeLifecycleKey;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadGroupTransfer {
    Move,
    Clone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoveOrCloneThread {
    Move,
    Clone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebaseResult {
    Success,
    Conflict { details: String },
    Cancelled,
    Error { details: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveOrCloneResult {
    Moved {
        thread_id: ThreadId,
        group_id: ThreadGroupId,
    },
    Cloned {
        new_thread_id: ThreadId,
        new_group_id: ThreadGroupId,
        parent_thread_id: ThreadId,
    },
    MoveFailed {
        reason: String,
        rebase_result: Option<RebaseResult>,
    },
    CloneFailed {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadGroupTransferPreview {
    pub operation: ThreadGroupTransfer,
    pub source_group: ThreadGroupId,
    pub target_group: ThreadGroupId,
    pub source_root_worktree: PathBuf,
    pub target_root_worktree: PathBuf,
    pub requires_rebase_confirmation: bool,
    pub preserves_source_identity: bool,
    pub creates_child_identity: bool,
}

pub fn validate_transfer(
    operation: ThreadGroupTransfer,
    source_group: ThreadGroupId,
    target_group: ThreadGroupId,
    source_root_worktree: Option<PathBuf>,
    target_root_worktree: Option<PathBuf>,
    source_is_dirty: bool,
    source_has_active_session: bool,
) -> anyhow::Result<ThreadGroupTransferPreview> {
    let source_root_worktree = source_root_worktree
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("source root worktree path is missing"))?;
    let target_root_worktree = target_root_worktree
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("target root worktree path is missing"))?;

    anyhow::ensure!(
        source_group != target_group,
        "source and target thread groups are the same; no cross-group transfer is needed"
    );

    let (requires_rebase_confirmation, preserves_source_identity, creates_child_identity) =
        match operation {
            ThreadGroupTransfer::Move => (
                source_group != target_group || source_is_dirty || source_has_active_session,
                true,
                false,
            ),
            ThreadGroupTransfer::Clone => (false, false, true),
        };

    Ok(ThreadGroupTransferPreview {
        operation,
        source_group,
        target_group,
        source_root_worktree,
        target_root_worktree,
        requires_rebase_confirmation,
        preserves_source_identity,
        creates_child_identity,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveOrClonePayload {
    pub operation: MoveOrCloneThread,
    pub source_thread_id: ThreadId,
    pub target_group_id: ThreadGroupId,
    pub target_root_thread_id: ThreadId,
    pub source_is_dirty: bool,
    pub source_has_active_session: bool,
    pub confirmed: bool,
}

pub fn unsupported_rebase_executor() -> RebaseResult {
    RebaseResult::Error {
        details: "Git rebase API is unsupported in this environment".to_string(),
    }
}

pub fn execute_move_or_clone(
    operation: MoveOrCloneThread,
    source_thread_id: ThreadId,
    target_group_id: ThreadGroupId,
    target_root_thread_id: Option<ThreadId>,
    target_worktree_id: Option<SharedString>,
    source_root_worktree: Option<PathBuf>,
    target_root_worktree: Option<PathBuf>,
    source_is_dirty: bool,
    source_has_active_session: bool,
    rebase_executor: impl FnOnce() -> RebaseResult,
    worktree_factory: impl FnOnce() -> Result<(WorktreePaths, SharedString), String>,
    store: &mut ThreadMetadataStore,
    cx: &mut Context<ThreadMetadataStore>,
) -> MoveOrCloneResult {
    let transfer_op = match operation {
        MoveOrCloneThread::Move => ThreadGroupTransfer::Move,
        MoveOrCloneThread::Clone => ThreadGroupTransfer::Clone,
    };

    let Some(source_thread) = store.entry(source_thread_id).cloned() else {
        return match operation {
            MoveOrCloneThread::Move => MoveOrCloneResult::MoveFailed {
                reason: "source thread not found".to_string(),
                rebase_result: None,
            },
            MoveOrCloneThread::Clone => MoveOrCloneResult::CloneFailed {
                reason: "source thread not found".to_string(),
            },
        };
    };

    let source_group_id = source_thread.group_id.unwrap_or_else(ThreadGroupId::new);

    let preview = match validate_transfer(
        transfer_op,
        source_group_id,
        target_group_id,
        source_root_worktree,
        target_root_worktree,
        source_is_dirty,
        source_has_active_session,
    ) {
        Ok(p) => p,
        Err(err) => {
            return match operation {
                MoveOrCloneThread::Move => MoveOrCloneResult::MoveFailed {
                    reason: err.to_string(),
                    rebase_result: None,
                },
                MoveOrCloneThread::Clone => MoveOrCloneResult::CloneFailed {
                    reason: err.to_string(),
                },
            };
        }
    };

    // Target root invariant: target root thread must exist in store and belong to target_group_id.
    let target_root_id = target_root_thread_id.or_else(|| {
        store
            .entries()
            .find(|entry| {
                entry.group_id == Some(target_group_id)
                    && (entry.parent_thread_id.is_none()
                        || entry.root_thread_id.is_none()
                        || entry.root_thread_id == Some(entry.thread_id))
            })
            .map(|entry| entry.thread_id)
    });

    let target_root = match target_root_id.and_then(|id| store.entry(id).cloned()) {
        Some(root) if root.group_id == Some(target_group_id) => root,
        _ => {
            return match operation {
                MoveOrCloneThread::Move => MoveOrCloneResult::MoveFailed {
                    reason: "target root thread missing or does not belong to target group".to_string(),
                    rebase_result: None,
                },
                MoveOrCloneThread::Clone => MoveOrCloneResult::CloneFailed {
                    reason: "target root thread missing or does not belong to target group".to_string(),
                },
            };
        }
    };

    // Reject dirty Move before calling rebase_executor.
    if operation == MoveOrCloneThread::Move && source_is_dirty {
        return MoveOrCloneResult::MoveFailed {
            reason: "cannot move dirty thread; uncommitted changes present".to_string(),
            rebase_result: None,
        };
    }

    match operation {
        MoveOrCloneThread::Move => {
            if preview.requires_rebase_confirmation {
                let rebase_res = rebase_executor();
                if rebase_res != RebaseResult::Success {
                    return MoveOrCloneResult::MoveFailed {
                        reason: "rebase operation did not succeed".to_string(),
                        rebase_result: Some(rebase_res),
                    };
                }
            }

            let mut updated_metadata = source_thread.clone();
            updated_metadata.group_id = Some(target_group_id);
            updated_metadata.parent_thread_id = Some(target_root.thread_id);
            updated_metadata.root_thread_id = target_root
                .root_thread_id
                .or(Some(target_root.thread_id));
            updated_metadata.worktree_id = target_root
                .worktree_id
                .clone()
                .or(target_worktree_id);
            updated_metadata.worktree_paths = target_root.worktree_paths.clone();
            store.save(updated_metadata, cx);

            MoveOrCloneResult::Moved {
                thread_id: source_thread_id,
                group_id: target_group_id,
            }
        }
        MoveOrCloneThread::Clone => {
            let (cloned_paths, cloned_worktree_id) = match worktree_factory() {
                Ok((paths, wt_id)) if !paths.folder_path_list().is_empty() => (paths, wt_id),
                Ok(_) => {
                    return MoveOrCloneResult::CloneFailed {
                        reason: "derived-worktree creation returned empty worktree paths".to_string(),
                    };
                }
                Err(err) => {
                    return MoveOrCloneResult::CloneFailed { reason: err };
                }
            };

            let new_thread_id = ThreadId::new();
            let cloned_metadata = ThreadMetadata { thread_id: new_thread_id,
            session_id: None,
            agent_id: source_thread.agent_id.clone(),
            title: source_thread.title.clone(),
            title_override: source_thread.title_override.clone(),
            updated_at: Utc::now(),
            created_at: Some(Utc::now()),
            interacted_at: None,
            worktree_paths: cloned_paths,
            remote_connection: source_thread.remote_connection.clone(),
            archived: false,
            user_order: None,
            group_id: Some(target_group_id),
            parent_thread_id: Some(target_root.thread_id),
            worktree_id: Some(cloned_worktree_id),
            root_thread_id: target_root
                .root_thread_id
                .or(Some(target_root.thread_id)), last_activity_at: None, activity_status: Default::default() };

            store.save(cloned_metadata, cx);

            MoveOrCloneResult::Cloned {
                new_thread_id,
                new_group_id: target_group_id,
                parent_thread_id: source_thread_id,
            }
        }
    }
}

pub fn execute_move_or_clone_payload(
    payload: MoveOrClonePayload,
    target_worktree_id: Option<SharedString>,
    rebase_executor: impl FnOnce() -> RebaseResult,
    worktree_factory: impl FnOnce() -> Result<(WorktreePaths, SharedString), String>,
    store: &mut ThreadMetadataStore,
    cx: &mut Context<ThreadMetadataStore>,
) -> MoveOrCloneResult {
    let source_root_worktree = store
        .entry(payload.source_thread_id)
        .and_then(|t| t.worktree_paths.folder_path_list().paths().first().cloned());
    let target_root_worktree = store
        .entry(payload.target_root_thread_id)
        .and_then(|t| t.worktree_paths.folder_path_list().paths().first().cloned());

    execute_move_or_clone(
        payload.operation,
        payload.source_thread_id,
        payload.target_group_id,
        Some(payload.target_root_thread_id),
        target_worktree_id,
        source_root_worktree,
        target_root_worktree,
        payload.source_is_dirty,
        payload.source_has_active_session,
        rebase_executor,
        worktree_factory,
        store,
        cx,
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        MoveOrCloneResult, MoveOrCloneThread, RebaseResult, ThreadGroupId, ThreadGroupTransfer,
        execute_move_or_clone, validate_transfer,
    };
    use crate::thread_metadata_store::ThreadMetadataStore;

    fn group(number: u128) -> ThreadGroupId {
        ThreadGroupId(uuid::Uuid::from_u128(number))
    }

    fn roots() -> (PathBuf, PathBuf) {
        (PathBuf::from("/worktrees/source"), PathBuf::from("/worktrees/target"))
    }

    #[test]
    fn move_preview_preserves_source_identity_and_requires_rebase_confirmation() {
        let (source_root, target_root) = roots();

        let preview = validate_transfer(
            ThreadGroupTransfer::Move,
            group(1),
            group(2),
            Some(source_root.clone()),
            Some(target_root.clone()),
            false,
            false,
        )
        .expect("different groups with worktree paths should produce a move preview");

        assert_eq!(preview.operation, ThreadGroupTransfer::Move);
        assert_eq!(preview.source_group, group(1));
        assert_eq!(preview.target_group, group(2));
        assert_eq!(preview.source_root_worktree, source_root);
        assert_eq!(preview.target_root_worktree, target_root);
        assert!(preview.requires_rebase_confirmation);
        assert!(preview.preserves_source_identity);
        assert!(!preview.creates_child_identity);
    }

    #[test]
    fn clone_preview_creates_child_identity_without_source_preservation() {
        let (source_root, target_root) = roots();

        let preview = validate_transfer(
            ThreadGroupTransfer::Clone,
            group(1),
            group(2),
            Some(source_root),
            Some(target_root),
            false,
            false,
        )
        .expect("different groups with worktree paths should produce a clone preview");

        assert_eq!(preview.operation, ThreadGroupTransfer::Clone);
        assert!(!preview.requires_rebase_confirmation);
        assert!(!preview.preserves_source_identity);
        assert!(preview.creates_child_identity);
    }

    #[test]
    fn same_group_transfer_returns_an_error_for_both_operations() {
        let (source_root, target_root) = roots();

        for operation in [ThreadGroupTransfer::Move, ThreadGroupTransfer::Clone] {
            let result = validate_transfer(
                operation,
                group(1),
                group(1),
                Some(source_root.clone()),
                Some(target_root.clone()),
                false,
                false,
            );

            assert!(result.is_err(), "same-group {operation:?} must be rejected");
            let error = result.expect_err("same-group transfer should fail").to_string();
            assert!(error.contains("same"), "error should identify same-group transfer: {error}");
        }
    }

    #[test]
    fn missing_source_or_target_worktree_returns_an_error() {
        let source_root = PathBuf::from("/worktrees/source");
        let target_root = PathBuf::from("/worktrees/target");

        let missing_source = validate_transfer(
            ThreadGroupTransfer::Move,
            group(1),
            group(2),
            None,
            Some(target_root.clone()),
            false,
            false,
        )
        .expect_err("missing source path should be rejected");
        assert!(missing_source.to_string().contains("source"));

        let missing_target = validate_transfer(
            ThreadGroupTransfer::Clone,
            group(1),
            group(2),
            Some(source_root),
            Some(PathBuf::new()),
            false,
            false,
        )
        .expect_err("empty target path should be rejected");
        assert!(missing_target.to_string().contains("target"));
    }

    #[test]
    fn dirty_or_active_source_still_produces_move_preview_with_rebase_confirmation() {
        let (source_root, target_root) = roots();

        for (source_is_dirty, source_has_active_session) in [(true, false), (false, true), (true, true)] {
            let preview = validate_transfer(
                ThreadGroupTransfer::Move,
                group(1),
                group(2),
                Some(source_root.clone()),
                Some(target_root.clone()),
                source_is_dirty,
                source_has_active_session,
            )
            .expect("dirty or active source state should be represented by a move preview");

            assert!(preview.requires_rebase_confirmation);
            assert!(preview.preserves_source_identity);
        }
    }

    #[gpui::test]
    async fn test_execute_move_success_and_failure(cx: &mut gpui::TestAppContext) {
        let (source_root, target_root) = roots();
        let source_group = group(1);
        let target_group = group(2);
        let source_id = crate::thread_metadata_store::ThreadId::new();
        let target_root_id = crate::thread_metadata_store::ThreadId::new();

        let source_metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: source_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-1")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Source Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[source_root.clone()])),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(source_group),
            parent_thread_id: None,
            worktree_id: Some("source-wt-id".into()),
            root_thread_id: Some(source_id),
            last_activity_at: None,
            activity_status: Default::default(),
        };

        let target_root_metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: target_root_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-target-root")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Target Root Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(target_group),
            parent_thread_id: None,
            worktree_id: Some("target-wt-id".into()),
            root_thread_id: Some(target_root_id),
            last_activity_at: None,
            activity_status: Default::default(),
        };

        cx.update(|cx| {
            ThreadMetadataStore::init_global(cx);
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                store.save(source_metadata.clone(), cx);
                store.save(target_root_metadata.clone(), cx);

                // Case 1: Rebase failure
                let res = execute_move_or_clone(
                    MoveOrCloneThread::Move,
                    source_id,
                    target_group,
                    Some(target_root_id),
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Conflict { details: "conflict".to_string() },
                    || Ok((project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])), "target-wt-id".into())),
                    store,
                    cx,
                );

                assert_eq!(
                    res,
                    MoveOrCloneResult::MoveFailed {
                        reason: "rebase operation did not succeed".to_string(),
                        rebase_result: Some(RebaseResult::Conflict { details: "conflict".to_string() }),
                    }
                );

                let retrieved = store.entry(source_id).unwrap();
                assert_eq!(retrieved.group_id, Some(source_group));

                // Case 2: Reject dirty Move BEFORE calling rebase
                let mut rebase_called = false;
                let res_dirty = execute_move_or_clone(
                    MoveOrCloneThread::Move,
                    source_id,
                    target_group,
                    Some(target_root_id),
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    true, // dirty Move!
                    false,
                    || {
                        rebase_called = true;
                        RebaseResult::Success
                    },
                    || Ok((project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])), "target-wt-id".into())),
                    store,
                    cx,
                );

                assert!(!rebase_called, "dirty Move MUST be rejected before rebase_executor is called!");
                assert_eq!(
                    res_dirty,
                    MoveOrCloneResult::MoveFailed {
                        reason: "cannot move dirty thread; uncommitted changes present".to_string(),
                        rebase_result: None,
                    }
                );
            });
        });

        cx.update(|cx| {
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                // Case 3: Successful Move updates group_id, parent_thread_id, root_thread_id, worktree_id, worktree_paths
                let res = execute_move_or_clone(
                    MoveOrCloneThread::Move,
                    source_id,
                    target_group,
                    Some(target_root_id),
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Success,
                    || Ok((project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])), "target-wt-id".into())),
                    store,
                    cx,
                );

                assert_eq!(
                    res,
                    MoveOrCloneResult::Moved {
                        thread_id: source_id,
                        group_id: target_group,
                    }
                );

                let retrieved = store.entry(source_id).unwrap();
                assert_eq!(retrieved.group_id, Some(target_group));
                assert_eq!(retrieved.parent_thread_id, Some(target_root_id));
                assert_eq!(retrieved.root_thread_id, Some(target_root_id));
                assert_eq!(retrieved.worktree_id.as_deref(), Some("target-wt-id"));
                assert_eq!(retrieved.worktree_paths, target_root_metadata.worktree_paths);
            });
        });
    }

    #[gpui::test]
    async fn test_execute_clone_creates_child_without_transcript(cx: &mut gpui::TestAppContext) {
        let (source_root, target_root) = roots();
        let source_group = group(1);
        let target_group = group(2);
        let source_id = crate::thread_metadata_store::ThreadId::new();
        let target_root_id = crate::thread_metadata_store::ThreadId::new();

        let source_metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: source_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-source")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Source Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[source_root.clone()])),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(source_group),
            parent_thread_id: None,
            worktree_id: Some("source-wt-id".into()),
            root_thread_id: Some(source_id),
            last_activity_at: None,
            activity_status: Default::default(),
        };

        let target_root_metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: target_root_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-target-root")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Target Root Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(target_group),
            parent_thread_id: None,
            worktree_id: Some("target-wt-id".into()),
            root_thread_id: Some(target_root_id),
            last_activity_at: None,
            activity_status: Default::default(),
        };

        cx.update(|cx| {
            ThreadMetadataStore::init_global(cx);
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                store.save(source_metadata, cx);
                store.save(target_root_metadata, cx);

                // Case 1: Worktree creation failure returns CloneFailed without mutating metadata
                let store_entries_before = store.entries().count();
                let res_fail = execute_move_or_clone(
                    MoveOrCloneThread::Clone,
                    source_id,
                    target_group,
                    Some(target_root_id),
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Success,
                    || Err("failed to spawn derived worktree".to_string()),
                    store,
                    cx,
                );

                assert_eq!(
                    res_fail,
                    MoveOrCloneResult::CloneFailed {
                        reason: "failed to spawn derived worktree".to_string(),
                    }
                );
                assert_eq!(store.entries().count(), store_entries_before, "Clone failure MUST NOT insert metadata into store");

                // Case 2: Successful Clone
                let res = execute_move_or_clone(
                    MoveOrCloneThread::Clone,
                    source_id,
                    target_group,
                    Some(target_root_id),
                    None,
                    Some(source_root),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Success,
                    || Ok((project::WorktreePaths::from_folder_paths(&workspace::PathList::new(&[target_root.clone()])), "cloned-wt-id".into())),
                    store,
                    cx,
                );

                let MoveOrCloneResult::Cloned { new_thread_id, new_group_id, parent_thread_id } = res else {
                    panic!("expected Cloned result");
                };

                assert_eq!(new_group_id, target_group);
                assert_eq!(parent_thread_id, source_id);

                let cloned = store.entry(new_thread_id).expect("cloned thread should exist in store");
                assert_eq!(cloned.group_id, Some(target_group));
                assert_eq!(cloned.parent_thread_id, Some(target_root_id));
                assert_eq!(cloned.root_thread_id, Some(target_root_id));
                assert_eq!(cloned.worktree_id.as_deref(), Some("cloned-wt-id"));
                assert_eq!(cloned.session_id, None, "clone MUST NOT copy transcript / session_id");
                assert_eq!(cloned.title, Some("Source Title".into()));
            });
        });
    }
}
