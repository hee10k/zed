# Workspace Lifecycle, Thread Groups, and Sidebar State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make restored Zed workspaces reconnect worktree ownership, grouped sidebar state, activity/status metadata, and safe drag operations without changing Native thread history or restoring terminal sessions.

**Architecture:** Workspace/worktree identity is restored through existing `WorkspaceDb` and canonical project paths. A new agent-side lifecycle coordinator stores worktree-owned operational state as JSON files under Zed's data directory and keeps durable sidebar/group/activity metadata in SQLite. Thread-group operations explicitly choose Move or Clone; Move requires confirmation and successful rebase, while Clone creates a new child/derived worktree without copying transcript or live process state.

**Tech Stack:** Rust, GPUI, SQLite domains, serde JSON lifecycle files, existing Git worktree/rebase APIs, ACP/native status events, terminal status events, agent/sidebar UI.

## Global Constraints

- Current code target: `crates/zed` package `1.18.0` on main branch.
- Worktree lifecycle files live under Zed's data directory, not inside project worktrees.
- SQLite stores durable group metadata, activity timestamps, status disposition, order, and archive state.
- Existing Native thread database schema, serialization, transcript persistence, and history loading remain unchanged.
- Do not add Native thread history recovery or migration behavior.
- Do not restore terminal processes, PTYs, shell sessions, OMP sessions, session locators, or resume commands.
- Terminal rows expose live status only; a restart must not claim a process is `Running` without new process evidence.
- Move changes existing relations only after confirmed successful rebase.
- Clone creates a new child thread and derived worktree without copying transcript, ACP session, terminal process, queued work, or locator.
- Rebase conflict, missing worktree, or delete cancellation preserves recoverable metadata.
- `agy` is an implementation worker only; do not add an agy runtime adapter.
- Use GPUI executor timers in asynchronous tests; do not use `smol::Timer` for `run_until_parked()` flows.
- Follow Zed Rust rules: propagate errors, avoid `unwrap()`, use `./script/clippy`, and do not run project-wide suites mid-task.

## Existing foundation

- The selected ThreadGroup model is already present from the A/B comparison and remains the source of truth for group transfer semantics.
- The worktree lifecycle store/coordinator is already present and owns canonical identity, atomic JSON persistence, unavailable/closing transitions, and confirmed deletion cleanup.
- Prior terminal session/recovery work is outside this plan. Do not extend it or make it a prerequisite for the tasks below.

---

### Task 1: Finalize the bounded ThreadGroup Move/Clone model

**Files:**
- Modify: `crates/agent_ui/src/thread_group.rs`
- Test: `crates/agent_ui/src/thread_group.rs` existing `#[cfg(test)]` module
- Update: `docs/superpowers/specs/2026-08-24-workspace-terminal-thread-recovery-design.md`

**Interfaces:**
- `ThreadGroupId`
- `ThreadGroupTransfer::{Move, Clone}`
- `ThreadGroupTransferPreview` containing source group, target group, source/target root worktree, and rebase requirement
- `validate_transfer(...) -> Result<ThreadGroupTransferPreview>`

- [ ] Confirm the selected implementation’s model and tests match the approved Move/Clone contract.
- [ ] Verify Move preserves thread/worktree IDs and requires explicit rebase confirmation.
- [ ] Verify Clone creates only a new child/derived-worktree plan and copies no transcript or live process state.
- [ ] Record the A/B matrix, winner, and rejected-branch reason in the implementation report.

Acceptance: Move and Clone are distinct, independently testable operations with non-destructive failure behavior.

---

### Task 2: Complete worktree lifecycle file storage and coordinator

**Files:**
- Create/modify: `crates/agent_ui/src/worktree_lifecycle.rs`
- Modify: `crates/agent_ui/src/agent_ui.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/agent_ui/src/thread_worktree_archive.rs`
- Test: `crates/agent_ui/src/worktree_lifecycle.rs`
- Test: existing workspace/project lifecycle tests

**Interfaces:**
- `WorktreeLifecycleKey` from canonical repository path, canonical worktree path, and remote identity.
- `WorktreeLifecycleState::{Active, Closing, Unavailable, Removed}`.
- `WorktreeLifecycleRecord` containing identity, workspace ID, root group ID, derived IDs, state, and checkpoint.
- `WorktreeLifecycleStore::load/save/mark_closing/mark_unavailable/remove` using `<zed data dir>/worktree-state/<key>/state.json`.
- `WorktreeLifecycleCoordinator::reconcile_workspace(...)`.

- [ ] Keep deterministic-key, JSON round-trip, missing-file, corrupt-file, and removal-cleanup tests.
- [ ] Keep atomic JSON writes using a temporary file plus rename; never expose a partial lifecycle file.
- [ ] Load records after `Workspace::new_local` restores worktrees; mark path mismatches unavailable instead of retargeting them.
- [ ] Observe worktree removal/release and move records to `Closing` before deletion.
- [ ] Expose a confirmation payload listing active/dirty descendants, lifecycle files, and linked sidebar/group metadata.
- [ ] On confirmed deletion, remove the lifecycle directory and clear only worktree-owned sidebar/group links.
- [ ] Flush lifecycle and sidebar metadata at normal shutdown and reconcile `Closing` records on startup.

Acceptance: workspace restart restores lifecycle records by canonical identity; confirmed worktree deletion removes lifecycle files; crash/partial transition leaves a record that can be reconciled.

---

### Task 3: Persist ThreadGroup metadata and implement Move/Clone operations

**Files:**
- Modify: `crates/agent_ui/src/thread_metadata_store.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/sidebar/src/sidebar.rs`
- Modify: existing Git worktree/rebase integration files at the current worktree-manager seam
- Test: `crates/agent_ui/src/thread_metadata_store.rs`
- Test: `crates/sidebar/src/sidebar_tests.rs`
- Test: Git/worktree integration seam

**Interfaces:**
- `ThreadMetadata` fields: `group_id`, `parent_thread_id`, `worktree_id`, `root_thread_id`, `last_activity_at`.
- `ThreadGroupMetadata` root/child relation queries.
- `MoveOrCloneThread::{Move, Clone}` action result.
- `MovePreview` and `RebaseResult` with explicit conflict/error states.

- [ ] Add SQLite migrations and round-trip tests for group/parent/worktree IDs.
- [ ] Implement root-group creation and child-thread creation from the root worktree.
- [ ] For Clone, create a new derived worktree and child thread; do not copy transcript, ACP session, queued messages, process state, or locator.
- [ ] For Move, build a preview, require confirmation, run rebase against the target root branch, and update group metadata only after success.
- [ ] Preserve the source group on conflict, cancellation, dirty state, or failed worktree creation.
- [ ] Ensure archive/delete operations include descendants without dangling group links.
- [ ] Add sidebar tests for root/child grouping, Move success/conflict, and Clone independence.

Acceptance: group hierarchy survives restart; Move and Clone are visibly distinct; failed Move is non-destructive; Clone has a new identity and derived worktree.

---

### Task 4: Unify last activity and live status

**Files:**
- Modify: `crates/agent_ui/src/thread_metadata_store.rs`
- Modify: `crates/agent_ui/src/terminal_thread_metadata_store.rs`
- Modify: `crates/agent_ui/src/conversation_view.rs`
- Modify: `crates/agent_ui/src/agent_panel.rs`
- Modify: `crates/sidebar/src/sidebar.rs`
- Modify: `crates/ui/src/components/ai/thread_item.rs` only for shared status rendering
- Test: metadata store tests, conversation/sidebar tests, terminal status tests

**Interfaces:**
- `ActivityStatus::{Idle, Running, WaitingForUser, Completed, Error}`.
- `last_activity_at` persisted for thread and terminal metadata.
- `ActivityEvent` conversion from ACP status, terminal process/title/wakeup/exit, tool completion, and user interaction.

- [ ] Add timestamp/status migrations and round-trip tests with legacy defaults.
- [ ] Update meaningful activity events and throttle repeated status-only writes.
- [ ] Map Native/ACP status to the unified display model without changing Native history loading or restoring `Running` blindly.
- [ ] Map terminal foreground process, wakeup, `ProcessExited`, and waiting inference into the same display model.
- [ ] Render relative last-activity time, preferring `last_activity_at` over `created_at`.
- [ ] Render `R/I/W/D/!` status marks for thread and terminal rows.
- [ ] Add tests for status transitions and restart re-evaluation.

Acceptance: sidebar shows last behavior time and status; idle/running/waiting transitions are observable; persisted status does not falsely claim a process is running after restart.

---

### Task 5: Add insertion guide and cross-group drag UX

**Files:**
- Modify: `crates/sidebar/src/sidebar.rs`
- Modify: `crates/sidebar/src/sidebar_tests.rs`
- Modify: `crates/ui/src/components/ai/thread_item.rs` if the guide belongs in the shared row component

**Interfaces:**
- `DropPosition::{Before, After}`.
- Drag state carries source entry, target group, target row, and target position.
- Cross-group drop opens Move/Clone preview; same-group drop persists `user_order`.

- [ ] Add drag-state tests for before/after hit testing and invalid header/end drops.
- [ ] Render a visible insertion line at the actual target boundary.
- [ ] Keep same-group reorder behavior and fractional/manual order persistence.
- [ ] Highlight the cross-group target and expose Move/Clone choices.
- [ ] Do not mutate metadata until reorder, rebase, or clone succeeds.
- [ ] Add tests for cancellation, conflict, and successful Move/Clone.

Acceptance: users can see exactly where a drop lands; cross-group operations are explicit; failed operations leave the original order/group unchanged.

---

### Task 6: Final integration and verification

**Files:**
- Modify: approved source files only
- Update: `docs/superpowers/specs/2026-08-24-workspace-terminal-thread-recovery-design.md`
- Update: this plan with completed commits and A/B result
- Test: focused package tests plus workspace/group/status/drag smoke scenarios

- [ ] Run the selected group-model and lifecycle focused tests.
- [ ] Integrate only the selected A/B implementation; preserve the rejected branch and reason in the report.
- [ ] Run workspace restore, group Move/Clone, activity/status, drag guide, and failure-isolation smoke scenarios.
- [ ] Run `cargo test -p agent_ui -p workspace -p sidebar` after disk/build resources permit.
- [ ] Run `./script/clippy -p agent_ui -p workspace -p sidebar` once at the end.
- [ ] Run final whole-branch code review before declaring completion.

Acceptance: all in-scope workspace, lifecycle, group, status, and drag contracts are verified; Native history and terminal/session resume behavior remain outside the change.

