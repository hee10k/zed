# Workspace Lifecycle, Thread Groups, and Sidebar State

## Status

Updated integrated design for the current Zed main codebase (`crates/zed` package `1.18.0`). Approved scope:

- Lifecycle-coupled state is stored in files under Zed's data directory.
- Durable sidebar, group, activity, and status metadata remains in SQLite.
- Cross-group worktree moves require confirmation before rebase.
- Move preserves an existing thread/worktree; Clone creates a new child thread and derived worktree without copying transcript or live processes.
- `agy` is an implementation worker for A/B comparison, not a Zed runtime adapter.
- Native thread history/serialization changes and terminal session/OMP resume are explicitly excluded.

## Problem

Workspace restoration, worktree removal, sidebar metadata, thread groups, and terminal status currently operate as separate lifecycles. A restored workspace can therefore lose worktree ownership metadata, leave durable sidebar records pointing at a removed worktree, or show ambiguous group and drag state.

The sidebar also lacks an explicit root-thread group model, a reliable last-activity field for all entry types, a unified live status model, and an insertion guide that makes drag destinations clear.

## Goals

1. Restore workspace, worktree, and sidebar metadata in workspace-first order.
2. Store worktree-lifetime state in Zed data files and durable user-facing metadata in SQLite.
3. Delete lifecycle files and worktree-owned metadata links when worktree deletion is confirmed.
4. Group root threads and derived child threads/worktrees explicitly.
5. Support confirmed Move and Clone operations between groups.
6. Show last activity time and live status for thread and terminal rows.
7. Show a precise before/after insertion guide during drag.
8. Compare the same bounded implementation task between task workers and agy workers in isolated branches.

## Non-goals

- Changing Native thread database serialization, transcript persistence, or history loading.
- Adding recovery or migration behavior for Native thread conversation history.
- Restoring or resuming terminal processes, PTYs, shell sessions, OMP sessions, or session locators.
- Automatically executing any terminal/session command during workspace restoration.
- Preserving an OS process, PID, PTY byte stream, file descriptors, SSH connection, or REPL memory.
- Treating agy as a runtime adapter or inventing an agy resume API.
- Copying transcript, ACP session state, tool state, or live processes during Clone.
- Silently rebasing or deleting a dirty/active worktree.
- Starting every saved terminal or thread process at startup.

## Persistence architecture

### Worktree lifecycle files

Use a Zed data-directory lifecycle store:

```text
<zed data dir>/worktree-state/<stable-worktree-key>/state.json
```

The key is derived from the canonical repository identity, canonical worktree path, and remote identity. The JSON record contains:

- canonical identity fields;
- lifecycle state: `active`, `closing`, `unavailable`, or `removed`;
- root `ThreadGroupId`;
- derived worktree/thread IDs;
- last checkpoint and last-seen workspace ID;
- local/remote scope.

These files are operational state, not source files. They are deleted after confirmed worktree removal. They do not contain terminal session locators or executable recovery commands. A missing file never deletes unrelated durable SQLite rows.

### SQLite durable metadata

Keep SQLite for data that remains useful beyond one worktree incarnation:

- existing Native thread data, without changing its schema, serialization, or load behavior;
- thread/group titles and activity timestamps;
- sidebar order and archive state;
- group and parent relationships;
- terminal display metadata, activity timestamps, and last disposition;
- archived worktree links.

When a worktree is deleted, remove lifecycle-owned links and mark affected sidebar metadata according to the existing archive policy. Do not add a recovery path capable of resurrecting a removed process, terminal session, or Native transcript.

## Workspace/worktree lifecycle

`Workspace::new_local` and `restore_multiworkspace` remain responsible for restoring `WorkspaceId`, project paths, panes, and worktree groups. A `WorktreeLifecycleCoordinator` in `agent_ui` observes the restored project and worktree events after the workspace exists.

The coordinator:

1. resolves canonical worktree identity;
2. loads or creates the lifecycle JSON record;
3. reconnects group/sidebar metadata;
4. marks missing paths unavailable without deleting metadata;
5. handles worktree removal after entity release;
6. flushes lifecycle and durable sidebar metadata at normal shutdown.

It does not restore terminal sessions or alter Native thread history loading.

### Confirmed root/worktree deletion

Before removing a root worktree or root thread group, show all descendants and their state:

- dirty/index changes;
- running or waiting thread/terminal status;
- pending rebase conflicts;
- derived worktree paths;
- lifecycle files and group/sidebar links.

Only after user confirmation:

1. mark lifecycle records closing;
2. remove the worktree through the existing Git/worktree lifecycle;
3. delete the lifecycle file;
4. clear worktree-owned sidebar/group links;
5. leave existing Native thread persistence and unrelated durable rows unchanged.

## Thread groups and Move/Clone

### Data model

Add explicit group metadata:

```text
ThreadGroup
  group_id
  root_thread_id
  root_worktree_id
  root_branch

ThreadMetadata additions
  group_id
  parent_thread_id
  worktree_id
  root_thread_id
```

Existing Native `parent_id` remains the persistence/deletion relationship. The explicit group fields also support external ACP threads and terminal-backed sidebar entries; they do not change Native transcript storage.

### New child thread

A new child thread is created from the root thread's worktree:

```text
root worktree
  → create linked derived worktree from root branch
  → create child thread
  → persist parent/group/worktree relation
```

The child starts a new session. It does not clone a live process or transcript.

### Move

Move keeps the existing thread and worktree IDs. A cross-group move opens a preview containing:

- current group and target root branch;
- worktree dirty/index state;
- proposed rebase;
- files likely to conflict.

After explicit confirmation, run rebase. Only a successful rebase changes `group_id`, `root_thread_id`, and sidebar placement. A conflict leaves the original group relation intact and exposes a recoverable conflict state.

### Clone

Clone creates a new child thread and derived worktree from the source/root worktree. It copies only creation context and group relation. It does not copy conversation history, ACP session IDs, terminal process state, queued work, or terminal session locators.

The drag/drop UI must offer Move and Clone explicitly when the target is another group; it must not infer Clone from an ambiguous gesture.

## Last activity

Add `last_activity_at` to thread and terminal durable metadata. Update it for meaningful events:

- user message submission;
- assistant turn completion;
- tool completion/error;
- compaction completion;
- waiting-for-user entry;
- terminal foreground-process/status changes;
- process exit.

Status-only updates must be throttled before SQLite writes. Sidebar display uses the existing relative timestamp formatter, with `last_activity_at` preferred over `created_at`.

## Unified status

Use one display model:

```rust
enum ActivityStatus {
    Idle,
    Running,
    WaitingForUser,
    Completed,
    Error,
}
```

Native/ACP sources:

- ACP `Generating` → `Running`;
- permission/elicitation → `WaitingForUser`;
- completed turn → `Completed`;
- error → `Error`;
- otherwise → `Idle`.

These mappings affect sidebar status only; they do not restore Native history or session state.

Terminal sources:

- foreground process/title events;
- wakeup/bell;
- `ProcessExited`;
- existing waiting inference.

After restart, terminal status defaults to `Idle`/unavailable until a new process reports activity. Never restore `Running` blindly.

Rows use compact status marks:

```text
R = Running
I = Idle
W = WaitingForUser
D = Completed
! = Error
```

## Drag destination guide

Extend current whole-row drag state with `DropPosition::Before` or `DropPosition::After`. Render a visible insertion line at the exact target boundary. Keep same-group reorder behavior and `user_order` persistence.

A cross-group drop highlights the target group and opens the explicit Move/Clone preview. Invalid header/end drops remain no-ops. The guide is visual state only; metadata changes occur after the selected operation succeeds.

## Implementation worker A/B

`agy` is not added to product code. After this design is approved, choose one bounded implementation slice (recommended: group Move/Clone decision model), give the exact same brief and tests to:

- a task implementation worker;
- an agy implementation worker.

Each works in an isolated branch. Compare tests, spec coverage, diff size, review findings, and rework. Integrate only the better implementation. This A/B process does not compare runtime adapters and does not add terminal/session recovery behavior.

## Failure behavior

- Missing worktree: keep metadata visible as unavailable; do not retarget it.
- Missing lifecycle file: rebuild a safe record from canonical identity; keep durable sidebar rows.
- Missing or corrupt sidebar metadata: preserve other workspaces and show the existing load error.
- Rebase conflict: keep original group ownership and expose conflict state.
- Clone failure: leave source thread/worktree unchanged.
- Delete confirmation cancellation: leave all lifecycle and durable metadata unchanged.
- Crash during transition: recover `closing` records on next startup and require explicit reconciliation.

No failure path creates a terminal session, executes a resume command, changes Native thread serialization, or deletes unrelated durable metadata.

## Verification

- Workspace restart restores the same workspace/worktree identity.
- Worktree lifecycle files survive normal app updates and are removed with confirmed worktree deletion.
- Existing Native thread persistence and loading remain unchanged.
- Root/child group relationships survive restart.
- Move requires confirmation and changes metadata only after successful rebase.
- Clone creates a new child/derived worktree without copying transcript/process state.
- Last activity and status update correctly for Native, ACP, and terminal rows.
- Drag guide identifies before/after placement and cross-group operation.
- Crash/close/rebase failure paths leave recoverable metadata.
- The selected bounded slice is implemented independently by a task worker and agy, then compared before integration.


