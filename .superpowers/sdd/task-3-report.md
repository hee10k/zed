# Task 3 Report: Explicit OMP Agent Terminal Sessions

## Status

Complete in commit `e49c53a0a7` (`feat(agent-ui): add explicit OMP agent terminal sessions`).

## Implementation

- Added `zed_actions::agent::NewOmpTerminal`.
- Registered the action beside `NewTerminalThread` at the workspace and AgentPanel action seams.
- Added `AgentPanel::new_omp_terminal(window, cx)`:
  - Allocates a Zed `TerminalId`.
  - Derives the opaque locator at `paths::data_dir()/agent_sessions/omp/<terminal-id>`.
  - Creates a shell in the current terminal working directory.
  - Registers `harness = "omp"`, the locator, `restore_on_workspace_open = true`, and `SessionBoundary::Live` before command delivery.
  - Sends only `omp --session-dir <locator>` through the existing post-startup handshake; configured plain-terminal init commands are not run for this path.
- Added `AgentPanel::register_terminal_agent_session(...) -> anyhow::Result<()>`, preserving live terminal metadata while writing the OMP recovery fields to `TerminalThreadMetadataStore`.
- Added `AgentPanel::resume_locator_for_terminal(...)` as the store-backed locator lookup seam for later restore work.
- OMP `TerminalEvent::ProcessExited` transitions metadata to `SessionBoundary::Sleeping`. Plain terminal rows do not receive OMP metadata or transitions.
- Explicit close of an OMP terminal uses the Task 1 `Cleared` transition (which deletes the row); plain terminal close continues using the existing direct delete path.
- Deliberately did not add `on_release`/application-shutdown hooks; the parent-task boundary assigns shutdown persistence to Task 5.
- Added GPUI tests for action-driven OMP creation/metadata/command persistence, ProcessExited sleeping transitions, plain-terminal isolation, and OMP close deletion.

## Verification

- `CARGO_BUILD_JOBS=1 cargo check -p zed_actions` — PASS; only the pre-existing `block v0.1.6` future-incompatibility warning.
- `CARGO_BUILD_JOBS=1 cargo check -p agent_ui --lib` — PASS; only the pre-existing `block v0.1.6` future-incompatibility warning.
- `git diff --check -- crates/zed_actions/src/lib.rs crates/agent_ui/src/agent_panel.rs` — PASS before commit.
- `cargo test -p agent_ui agent_panel::tests::new_omp_terminal -- --nocapture` — started as the focused proof but canceled before completion under the isolated worktree disk constraint; no test executable ran.
- `CARGO_BUILD_JOBS=1 cargo check -p agent_ui --tests` — started to compile the focused test target, then canceled before completion under the isolated worktree disk constraint; no test executable was linked or run.
- No formatter, clippy, project-wide build, or project-wide test suite was run.

## Concerns

The new GPUI tests need to be rerun by the parent agent when shared build artifacts or sufficient disk are available. Production library compilation passed, but this task does not claim a focused GPUI test PASS because the test target could not be completed within the constrained isolated worktree.

## Review Fixes

- OMP creation now quotes the locator with the terminal's actual `task::ShellKind::try_quote` implementation after shell creation, so paths such as macOS `Library/Application Support` remain one argument.
- OMP creation is rejected before allocating a terminal for non-local projects. The workspace receives an actionable error explaining that OMP sessions require a local project because the locator is stored in Zed's local data directory.
- The GPUI creation test now uses `/bin/sh -c` as an input-observing read/echo sink instead of an interactive shell that could execute `omp`; production terminal behavior is unchanged.
- Added focused unit assertions for whitespace-path quoting and remote-project rejection, and updated the GPUI assertion to compare the shell-quoted command with persisted metadata.

## Review-Fix Verification

- `CARGO_BUILD_JOBS=1 cargo check -p agent_ui --lib` — PASS; only the pre-existing `block v0.1.6` future-incompatibility warning.
- `CARGO_BUILD_JOBS=1 cargo test -p agent_ui omp_creation_command_quotes_whitespace_paths --no-run` — stopped after 30 seconds while compiling dependencies; no test executable ran.
- `CARGO_BUILD_JOBS=1 cargo test -p agent_ui omp_creation_command_quotes_whitespace_paths --lib -- --nocapture` — stopped after 60 seconds while compiling test dependencies; no test executable ran.
- No formatter, clippy, project-wide build, or project-wide test suite was run.

## Self-Review

- Production OMP commands remain `omp --session-dir <quoted locator>` and only local projects can create them.
- Plain terminal initialization still uses the existing configured command path and is not shell-quoted by this change.
- The focused GPUI tests could not be executed in this isolated worktree because the test target exceeded the constrained build window; the parent should rerun them with shared artifacts.
