use std::path::PathBuf;

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
            updated_metadata.worktree_id = target_worktree_id;
            updated_metadata.root_thread_id = target_root_thread_id.or(Some(source_thread_id));
            store.save(updated_metadata, cx);

            MoveOrCloneResult::Moved {
                thread_id: source_thread_id,
                group_id: target_group_id,
            }
        }
        MoveOrCloneThread::Clone => {
            let new_thread_id = ThreadId::new();
            let cloned_metadata = ThreadMetadata {
                thread_id: new_thread_id,
                session_id: None,
                agent_id: source_thread.agent_id.clone(),
                title: source_thread.title.clone(),
                title_override: source_thread.title_override.clone(),
                updated_at: Utc::now(),
                created_at: Some(Utc::now()),
                interacted_at: None,
                worktree_paths: WorktreePaths::default(),
                remote_connection: source_thread.remote_connection.clone(),
                archived: false,
                user_order: None,
                group_id: Some(target_group_id),
                parent_thread_id: Some(source_thread_id),
                worktree_id: target_worktree_id,
                root_thread_id: target_root_thread_id.or(Some(source_thread_id)),
            };

            store.save(cloned_metadata, cx);

            MoveOrCloneResult::Cloned {
                new_thread_id,
                new_group_id: target_group_id,
                parent_thread_id: source_thread_id,
            }
        }
    }
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

        let metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: source_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-1")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Thread Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::default(),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(source_group),
            parent_thread_id: None,
            worktree_id: None,
            root_thread_id: None,
        };

        cx.update(|cx| {
            ThreadMetadataStore::init_global(cx);
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                store.save(metadata.clone(), cx);

                let res = execute_move_or_clone(
                    MoveOrCloneThread::Move,
                    source_id,
                    target_group,
                    None,
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Conflict { details: "conflict".to_string() },
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
            });
        });

        cx.update(|cx| {
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                let res = execute_move_or_clone(
                    MoveOrCloneThread::Move,
                    source_id,
                    target_group,
                    None,
                    None,
                    Some(source_root.clone()),
                    Some(target_root.clone()),
                    false,
                    false,
                    || RebaseResult::Success,
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
            });
        });
    }

    #[gpui::test]
    async fn test_execute_clone_creates_child_without_transcript(cx: &mut gpui::TestAppContext) {
        let (source_root, target_root) = roots();
        let source_group = group(1);
        let target_group = group(2);
        let source_id = crate::thread_metadata_store::ThreadId::new();

        let metadata = crate::thread_metadata_store::ThreadMetadata {
            thread_id: source_id,
            session_id: Some(agent_client_protocol::schema::v1::SessionId::new("sess-source")),
            agent_id: agent::ZED_AGENT_ID.clone(),
            title: Some("Source Title".into()),
            title_override: None,
            updated_at: chrono::Utc::now(),
            created_at: Some(chrono::Utc::now()),
            interacted_at: None,
            worktree_paths: project::WorktreePaths::default(),
            remote_connection: None,
            archived: false,
            user_order: None,
            group_id: Some(source_group),
            parent_thread_id: None,
            worktree_id: None,
            root_thread_id: None,
        };

        cx.update(|cx| {
            ThreadMetadataStore::init_global(cx);
            let store = ThreadMetadataStore::global(cx);
            store.update(cx, |store, cx| {
                store.save(metadata, cx);

                let res = execute_move_or_clone(
                    MoveOrCloneThread::Clone,
                    source_id,
                    target_group,
                    None,
                    None,
                    Some(source_root),
                    Some(target_root),
                    false,
                    false,
                    || RebaseResult::Success,
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
                assert_eq!(cloned.parent_thread_id, Some(source_id));
                assert_eq!(cloned.session_id, None, "clone MUST NOT copy transcript / session_id");
                assert_eq!(cloned.title, Some("Source Title".into()));
            });
        });
    }
}
