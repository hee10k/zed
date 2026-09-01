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

Pending commit: `feat(herdr): add persistent central view toggle`

## Report path

`.superpowers/sdd/task-2-report.md`
