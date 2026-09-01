# Task 2 Implementation Report

## Status

DONE_WITH_CONCERNS — Task 2 implementation and central integration fixes are present in the working tree.

## Implementation

- Added `ToggleHerdR` and `FocusHerdR` action definitions with the requested display names in `zed_actions::herdr`.
- Registered both actions in `zed::init` and routed them to deferred HerdR app helpers.
- Added a pure `toggle_visibility` transition helper and deterministic tests for show/focus, focus-only, and hide/restore-editor transitions.
- Added deferred app-level toggle/focus behavior. Missing hosts are installed through the existing lifecycle-safe `install_host` path; existing hosts are reused.
- Changed HerdR Close to hide the central view and restore Workspace focus without terminating the session, terminal, snapshot, or connection tasks.
- Added `HerdRStatusButton`, with Terminal icon, `HerdR` aria label, Toggle HerdR tooltip/action dispatch, selected visibility state, weak MultiWorkspace ownership, and notification observation. It opts out of hide settings.
- Registered the button in initialized Workspace status bars.
- Moved active Workspace status-bar rendering outside the mutually-exclusive MultiWorkspace central selector and suppressed nested Workspace status-bar rendering when parented by MultiWorkspace.
- Updated the production host layout to fill the selected central content area in its ordinary state while retaining Collapse, Maximize, and Close controls.
- Added deterministic workspace coverage that verifies the status bar remains rendered while HerdR is selected.

## TDD and focused validation

- Red: `cargo test -p zed --bin zed herdr_toggle_visibility` failed as expected because the helper and transition enum were absent.
- Green/final focused checks:
  - `cargo test -p zed --bin zed herdr_toggle_visibility` — 1 passed.
  - `cargo check -p zed_actions` — passed.
  - `cargo check -p zed --bin zed` — passed with existing warnings (`git_ui` dead code, linker compact-unwind, and future-incompatibility warning).
  - `cargo test -p workspace --lib multi_workspace_tests::test_herdr_central_view_visibility` — 1 passed.
  - `cargo test -p workspace --lib multi_workspace_tests::test_herdr_visibility_preserves_entities` — 1 passed.
  - `cargo test -p workspace --lib multi_workspace_tests::test_herdr_central_view_keeps_status_bar_visible` — 1 passed.

No formatter, linter, project-wide suite, real PTY, or HerdR server was run.

## Files

- `crates/zed_actions/src/lib.rs`
- `crates/zed/src/zed.rs`
- `crates/zed/src/zed/herdr_host.rs`
- `crates/workspace/src/multi_workspace.rs`
- `crates/workspace/src/multi_workspace_tests.rs`
- `crates/workspace/src/status_bar.rs`
- `crates/workspace/src/workspace.rs`

## Concerns

- The focused tests do not exercise real PTY/server startup or full app-level action dispatch; those remain runtime-only by design and should be covered by the later smoke/restoration tasks.
- The existing `terminate` method remains available for future lifecycle shutdown but is no longer used by the in-view Close path.
- The status-bar debug selector is test instrumentation shared by the status bar and is otherwise behavior-neutral.

## Commit

Prior commit: `f326513924f90c7e8fe84347b12ae750fccf5571 feat(herdr): add persistent central view toggle`

## Fix validation

- Restored `status_bar.add_right_item(image_info, window, cx)` so every initialized Workspace retains its ImageInfo status item alongside HerdR.
- Changed `herdr-central-content` to a flex-column parent and kept the real HerdR host flex-filling that slot with `flex_1`, `min_h_0`, and `w_full`; no fixed arbitrary height was added.
- Strengthened `TestHerdrCentralHost` to use the same flex-fill child contract instead of `.size_full()`, so the central layout tests exercise production-shaped parent/child sizing.
- `cargo test -p workspace --lib herdr_central_view` — 2 passed (visibility and status-bar coverage).
- `cargo test -p workspace --lib test_herdr_visibility_preserves_entities` — 1 passed.
- `cargo test -p zed --bin zed herdr_toggle_visibility` — 1 passed (82 filtered; 2 existing warnings).
- `cargo check -p zed_actions` — passed; future-incompatibility warning for `block v0.1.6`.
- `cargo check -p zed --bin zed` — passed; existing dead-code warning for `git_ui::WorktreeFileDiff::repo_path`, existing unused `HerdRHost::terminate`, and future-incompatibility warning for `block v0.1.6`.
- `cargo check -p workspace --lib --tests` — could not complete because the unrelated pre-existing `RemoteConnectionIdentity::Mock { .. }` exhaustiveness error remains in `crates/workspace/src/persistence.rs:1712`.
- No focused ImageInfo-preserving initialization test is available in the current test surface; the restored registration was verified by source/semantic diff against the prior initialization path.
- The initial multi-filter command `cargo test -p workspace --lib multi_workspace_tests::test_herdr_central_view_visibility multi_workspace_tests::test_herdr_central_view_keeps_status_bar_visible multi_workspace_tests::test_herdr_visibility_preserves_entities` was rejected by Cargo because `cargo test` accepts one filter; the equivalent valid filters above were run.
- No formatter, linter, project-wide suite, real PTY, or HerdR server was run.

## Fix concerns

- Focused coverage verifies production-shaped layout and status-bar preservation but does not initialize the real HerdR PTY/server, by design.
- Workspace test compilation remains blocked by the unrelated `RemoteConnectionIdentity::Mock { .. }` match error described above.
- Root cause: the production `HerdRHost` retained unconditional `.flex_1()` while collapsed and only added a height constraint, allowing the new column-flex central parent to allocate the full central slot.
- Fix: apply `.flex_1()` only when expanded; preserve the existing collapsed `.h(HOST_HEADER_HEIGHT)` branch and all header controls.
- Added deterministic GPUI coverage: `test_herdr_collapsed_host_is_header_sized` uses the pure test host fixture and asserts collapsed height is exactly 32px without starting PTY/server.
- Exact validation: `cargo test -p workspace --lib test_herdr_collapsed_host_is_header_sized` — 1 passed (268 filtered).
- Exact validation: `cargo test -p workspace --lib herdr_central_view` — 2 passed (267 filtered).
- Exact validation: `cargo test -p workspace --lib test_herdr_visibility_preserves_entities` — 1 passed (268 filtered).
- Exact validation: `cargo test -p zed --bin zed herdr_toggle_visibility` — 1 passed (82 filtered; 2 existing warnings).
- Exact validation: `cargo check -p zed --bin zed` — passed with existing warnings (`git_ui` dead code, unused `HerdRHost::terminate`, and future-incompatibility warning for `block v0.1.6`).
- Self-review: only `crates/zed/src/zed/herdr_host.rs`, `crates/workspace/src/multi_workspace_tests.rs`, and this report are bounded fix files; maximize, close-hide, session persistence, ImageInfo/status items, and deferred lifecycle paths are untouched.

## Report path

`.superpowers/sdd/task-2-report.md`
