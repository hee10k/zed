use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ThreadGroupId(uuid::Uuid);

impl ThreadGroupId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThreadGroupTransfer {
    Move,
    Clone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ThreadGroupTransferPreview {
    pub(crate) operation: ThreadGroupTransfer,
    pub(crate) source_group: ThreadGroupId,
    pub(crate) target_group: ThreadGroupId,
    pub(crate) source_root_worktree: PathBuf,
    pub(crate) target_root_worktree: PathBuf,
    pub(crate) requires_rebase_confirmation: bool,
    pub(crate) preserves_source_identity: bool,
    pub(crate) creates_child_identity: bool,
}

pub(crate) fn validate_transfer(
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{ThreadGroupId, ThreadGroupTransfer, validate_transfer};

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
}
