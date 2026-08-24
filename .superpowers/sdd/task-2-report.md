# Task 2 Report: Worktree lifecycle file storage and coordinator

## Status

DONE (source, commit). Focused test run blocked by disk space — exact error recorded below, no pass claimed.

## Commit

- SHA: `ca9469a9ac`
- Subject: `Add worktree lifecycle store and coordinator`
- Files (5, +575/-1):
  - `crates/agent_ui/Cargo.toml` (+1): add `sha2.workspace = true` (deterministic key hashing)
  - `Cargo.lock` (+1): `"sha2"` added to agent_ui deps
  - `crates/agent_ui/src/agent_ui.rs` (+1): `pub mod worktree_lifecycle;`
  - `crates/agent_ui/src/agent_panel.rs` (+61/-1): narrow reconciliation seam
  - `crates/agent_ui/src/worktree_lifecycle.rs` (+512): new module

## What was implemented (per Task 2 brief)

### `crates/agent_ui/src/worktree_lifecycle.rs`

- **`WorktreeLifecycleKey`** — `{ repository_path, worktree_path, remote_identity }`. `stable_name()` = hex SHA-256 over the three canonical fields (NUL-separated), producing a deterministic, filesystem-safe, 64-char directory name. `stable_key()` alias.
- **`WorktreeLifecycleState`** — `Active | Closing | Unavailable | Removed` (snake_case serde).
- **`WorktreeLifecycleTerminalLocator`** — `{ harness, locator }` (opaque; e.g. OMP session dir).
- **`WorktreeLifecycleRecord`** — key, `workspace_id`, `last_seen_workspace_id`, `root_group_id`, `derived_worktree_ids`, `derived_thread_ids`, `terminal_locators`, `state`, `checkpoint`. `reconcile(...)` transitions state to `Active`/`Unavailable` from path existence.
- **`WorktreeLifecycleStore`** — paths under `<zed data dir>/worktree-state/<stable-key>/state.json` (override root for tests via `with_root`). API:
  - `load` → `Ok(None)` on missing file (missing-file recovery = rebuild an `Active` record at reconcile time);
  - `save` — `create_dir` then `Fs::atomic_write` (temp-file + rename), so a partially written JSON file is never observable;
  - `mark_closing`, `mark_unavailable`, `mark_active` (state mutation preserving the record);
  - `remove` — recursive `remove_dir` with `ignore_if_not_exists`;
  - `list` — stream all record dirs.
- **`WorktreeLifecycleCoordinator`** —
  - `reconcile_workspace(workspace_id, worktrees)` — canonicalizes each observed worktree, loads-or-creates the record, marks `Active` when the root exists and `Unavailable` when it does not; records for this workspace not present in the seen set are marked `Unavailable` (kept visible, **never retargeted**).
  - `mark_workspace_closing(workspace_id)` — shutdown flush seam.
  - `mark_worktree_closing(key)`.
  - `prepare_deletion(key, descendants)` — marks `Closing`, returns `WorktreeDeletionConfirmation { key, active_descendants, dirty_descendants, lifecycle_files, linked_sessions }`.
  - `confirm_deletion(key)` / `remove_worktree(key)` — deletes the lifecycle directory (leaves durable SQLite history untouched; that belongs to Task 3/5 seams).
  - `flush(records)` — normal-shutdown save of a set of records.
  - Path canonicalization in `key_for` via `Fs::canonicalize`, falling back to the lexical absolute path when the root is temporarily unavailable.

### `crates/agent_ui/src/agent_panel.rs` — narrow seam

- `WorktreeLifecycleCoordinator` + task field added to `AgentPanel`.
- Constructed from the workspace fs in `AgentPanel::new`.
- `reconcile_worktree_lifecycle(cx, mark_closing)` spawns a background task that (a) optionally marks the workspace's records `Closing` (used on worktree removal) and (b) calls `reconcile_workspace` with the project's current worktrees (canonical repo common-dir as repository identity, worktree abs path, remote options → "local"/remote identity). Errors are logged, never panic (`let ... else`, no unwraps).
- Invoked on the existing `WorktreeAdded`/`WorktreeOrderChanged`/`WorktreePathsChanged` and `WorktreeRemoved` project-event subscription branch, plus once after the panel is constructed (load-after-restore timing).

### Not changed (per brief / acceptance)

Sidebar grouping UI, activity/status rendering, drag guide, and final OMP resume behavior were **not** touched. The coordinator is emit-only and does not change workspace crate ownership or any Git/worktree lifecycle call.

## Focused test summary

Four unit tests were written in `worktree_lifecycle.rs`:

1. `lifecycle_key_is_deterministic_and_scoped` — identical identities hash to the same 64-char name; a differing remote identity changes it.
2. `lifecycle_record_json_round_trip` — serde round-trip preserves the full record.
3. `lifecycle_store_recovers_missing_file` — `load` returns `None` before save and the saved record afterward (RealFs + TempDir).
4. `lifecycle_store_removal_cleans_directory` — `remove` deletes the lifecycle directory and `state.json`.

### Test execution — BLOCKED BY DISK (not a failure)

`cargo test -p agent_ui --lib worktree_lifecycle::tests` could not build the agent_ui dev-dependency graph (gpui/editor/project/search/extension_host/terminal_view/agent/git_ui_core test-support trees). Exact error (os error 28):

```
error: failed to write query cache to `.../.worktrees/workspace-terminal-thread-recovery/target/debug/incremental/extension_host-1vnfk1hnx6dby/s-hln4by2u84-09sqd3r-working/query-cache.bin`: No space left on device (os error 28)
error: could not write output to .../target/debug/deps/*.rcgu.o: No space left on device (os error 28)
error: failed to write to `.../target/debug/deps/rmeta*/full.rmeta`: No space left on device (os error 28)
Caused by: rustc-LLVM ERROR: IO failure on output stream: No space left on device
```

Volume was at 100% (≈116 MiB free; an 11 GiB worktree `target` plus a 69 GiB parent `target`). I freed the worktree's 6.4 GiB incremental cache (reproducible, non-destructive) and retried; the dev-dep rebuild still exhausted the volume. **No test result is claimed for these four tests.**

### Source verification (PASSED)

`cargo check -p agent_ui --lib` passes on the final committed state (only pre-existing `thread_group.rs` dead-code warnings from the Task 1 A/B module). No unwraps in live code; errors propagate via `anyhow`/`?` or are logged.

## Concerns for integration review

1. **Tests unverified on this run** — the four lifecycle unit tests are written and the lib compiles, but disk space prevented executing them. They should run once the volume has headroom (Task 7 verification).
2. **Concurrent reconcile writes** — `AgentPanel::new` and project-event-driven reconciles can spawn overlapping background saves to the same key. Because reconcile is idempotent and `atomic_write` is last-writer-wins with valid JSON, this is harmless but redundant; a per-workspace serialization (e.g. a task chain) would tighten it if it shows up in profiling.
3. **Remote identity form** — `remote_identity` is derived via `format!("{options:?}")` of `RemoteConnectionOptions` on the agent-panel side ("local" otherwise). It is deterministic within a session/connection but is a Debug form, so a version change could in principle rotate the key; acceptable for the current seam, flagging for the Task 5/7 ownership cleanup.
4. **`reconcile_workspace` unavailable-marking** — any record matching the workspace id that isn't among currently-observed worktrees is marked `Unavailable`. This is correct post-restore (spec: keep visible, don't retarget), but relies on the coordinator being run after worktrees are fully restored; the constructor hook runs at panel `new`, which the existing subscription also covers on add events.
5. **Parent checkout hygiene** — an earlier file-path mix-up briefly edited the parent (`/Users/aigo/.cache/checkouts/github.com/zed-industries/zed`), including a `WorktreeLifecycleCoordinator` field, a `let worktree_lifecycle`, subscription calls, `sha2` dep, and the module declaration. All were reverted and the parent's `crates/agent_ui/Cargo.toml`, `agent_ui.rs`, and `agent_panel.rs` were verified **byte-identical to `HEAD`** via `diff` before committing. No residual change remains in the parent.

## Report location

`/Users/aigo/.cache/checkouts/github.com/zed-industries/zed/.worktrees/workspace-terminal-thread-recovery/.superpowers/sdd/task-2-report.md`

## Reviewer-fix follow-up

### Status

Source changes are complete for all six lifecycle reviewer findings. The follow-up commit will supersede the original Task 2 commit once it is created.

### Changes

1. **Durable terminal states remain authoritative.** `WorktreeLifecycleRecord::reconcile` now derives `Active`/`Unavailable` only for non-terminal records. Existing `Closing` and `Removed` states are preserved even when the path exists. The not-seen reconciliation pass applies the same guard, so it cannot downgrade an explicit transition.
2. **Reconciles are serialized at the AgentPanel seam.** Each lifecycle background task takes ownership of the previous task and awaits it before any read-modify-write. The AgentPanel release callback uses the same chain before flushing shutdown state, preventing a stale event task from overwriting `Closing`.
3. **Corrupt records are recoverable.** `WorktreeLifecycleStore::load` and `list` log malformed JSON and skip only that record. Filesystem/list I/O failures still propagate. A later reconciliation can replace a skipped corrupt record with a valid record.
4. **Remote identity is stable and versioned.** AgentPanel now uses the normalized `remote_connection_identity(...).persistence_key()` with a `remote-v1:` prefix instead of `RemoteConnectionOptions`'s Debug formatting. Local identity remains the existing `"local"` value to avoid an unnecessary local-key rotation.
5. **Shutdown flush has a production caller.** `mark_workspace_closing` now batches changed records through `flush`, and AgentPanel's existing `on_release` callback invokes it on the background executor, chained after any in-flight lifecycle event.
6. **Focused coordinator coverage.** Added tests for Closing/Removed preservation, missing/unavailable and not-seen transitions, and corrupt-file recovery (including store `list` skipping malformed JSON). Existing deterministic-key, JSON round-trip, missing-file, and removal tests remain.

### Verification

- `cargo metadata --no-deps --format-version 1` — passed.
- A targeted `cargo check -p agent_ui --lib` was started but canceled before completion at the parent agent's request to avoid another long build while siblings are active. No compile result is claimed for this follow-up.
- The focused GPUI tests were not executed; the prior Task 2 attempt exhausted the volume with `os error 28` while rebuilding the agent_ui test-support graph. No test pass is claimed here.

### Self-review and concerns

- Atomic writes and recursive lifecycle-directory cleanup are unchanged.
- Malformed JSON is the only load/list failure converted to a skipped record; permission, read, directory, and write failures still surface to the coordinator and are logged by AgentPanel.
- The task chain covers all AgentPanel lifecycle event and release callers. Direct coordinator API callers remain responsible for not invoking concurrent mutations outside that production seam.
- No sidebar/group UI, activity/status UI, drag guide, OMP restore behavior, workspace ownership, or SQLite history behavior was added.