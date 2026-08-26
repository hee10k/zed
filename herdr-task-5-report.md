# Herdr Task 5 Report — Sidebar / Session Navigation

## Scope executed
- `crates/sidebar/src/sidebar.rs`
  - `load_agent_thread_in_workspace` now inspects the backend marker (`HERDR_AGENT_ID`) before choosing a path: Herdr roots activate via `AgentPanel::load_herdr_thread`, which routes through the window bridge and issues exactly one outbound `workspace.focus` per activation; ACP-native threads keep the existing `load_agent_thread` path and never call Herdr. This single cutover covers local activation, cross-window activation, archive-restore activation, and thread-switcher preview/confirm (all funnel through this function).
  - `apply_thread_rename` routes Herdr-root renames through `bridge.request_rename_workspace` (falling back to a title override when disconnected); ACP renames unchanged.
  - Row close action is backend-aware: Herdr rows call the new `Sidebar::close_herdr_entry` → `bridge.request_close_workspace`; no `agent.cancel`/`agent.close` is emitted for Herdr roots. Tooltip reads "Close Herdr Root".
  - New "New Herdr Thread" entries in the project-header new-thread menu call `Sidebar::new_herdr_entry` → `AgentPanel::create_herdr_root`: `workspace.create` is sent, and the Zed row activates only after the returned identity is persisted as a root mapping (`RootCreated`), with focus suppressed to avoid a reflected second focus.
  - Rebuild persistence: Herdr rows already flow from `ThreadMetadataStore` via their stored worktree/cwd identity, are not drafts (`is_draft()` excludes them), and remain visible while disconnected. Added a session-label pass that appends ` · {session}` to bridge-mapped roots only when a project holds multiple Herdr rows (ambiguity with historical sessions).
  - New `#[cfg(test)] mod tests` with three behavior tests (below).
- `crates/sidebar/src/thread_switcher.rs`: no functional change required — switcher selection flows through `load_agent_thread_in_workspace`, so Herdr routing applies automatically; entries inherit non-draft classification.
- `crates/agent_ui/src/agent_panel.rs`
  - Public routing API: `rename_herdr_thread`, `close_herdr_thread`, `create_herdr_root`, `herdr_root_thread_id`, `herdr_mapped_root_thread_ids`, `herdr_session_name`.
  - Status surface: `herdr_status_label` returns `Ready` / `Synchronizing` / `Reconnecting` / `Unavailable` / `Conflict`; rendered next to the Herdr root title in the panel header bar. `Conflict` is raised by `HerdrBridgeEvent::Conflict` and cleared when a fresh synchronization run starts.
  - Explicit rebind UI: new `ConnectHerdrSession` action (declared in `agent_ui.rs`, registered on Workspace in `agent_panel::init` and on the panel). It opens a one-line session-name editor overlay; rebinding happens only on explicit confirm, going through the new `HerdrBridgeRegistry::rebind_window_session` (falls back to direct `bridge.rebind_selection` when unregistered), after which the stale Herdr surface resets to a draft.
  - Test-support helpers under `#[cfg(any(test, feature = "test-support"))]`: `install_test_herdr_root` (recording-API-backed bridge seeded with one workspace) and `take_test_herdr_api_calls`, so dependent-crate tests can assert outbound calls without a real server.
  - Two new tests in `agent_panel::tests::herdr`.
- `crates/agent_ui/src/herdr_bridge.rs` (minimal accessors only)
  - New `pub(crate) fn rebind_window_session(window_id, selection, cx)` on `HerdrBridgeRegistry` (required by the explicit-rebind flow).
  - New `pub(crate) fn root_thread_ids()` on `HerdrThreadBridge` (sidebar labeling + creation flow).
  - Widened existing test-only cfg gates (`RecordingHerdrApi`, `for_test_with_api`) from `cfg(test)` to `cfg(any(test, feature = "test-support"))` so the sidebar crate's dev-dependency feature can use them. No production code paths changed.
- `crates/agent_ui/src/agent_ui.rs`: declared the `ConnectHerdrSession` action.

## Verification commands and results

| Command | Result |
|---|---|
| `cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact` | ok, **0 matched** — see naming note below |
| `cargo test -p sidebar tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact` | ok, 1 passed |
| `cargo test -p sidebar sidebar::tests::activating_acp_root_does_not_request_herdr_focus -- --exact` | ok, **0 matched** — same naming note |
| `cargo test -p sidebar tests::activating_acp_root_does_not_request_herdr_focus -- --exact` | ok, 1 passed |
| `cargo test -p sidebar tests::` | ok, **148 passed; 0 failed** (full suite incl. 3 new) |
| `cargo test -p agent_ui agent_panel::tests::herdr` | ok, **5 passed; 0 failed** (3 pre-existing + 2 new) |
| `cargo test -p agent_ui herdr_bridge` | ok, **21 passed; 0 failed** |
| `cargo check -p agent_ui --lib`, `cargo check -p sidebar --lib` | clean |

**Naming note:** the brief's literal filter `sidebar::tests::…` matches nothing because unit tests in a lib target are named without the crate prefix; the module path here is `tests::*`. The identical assertions run green under `tests::<name> -- --exact`. If strict textual parity with the checklist is required, the module would need to move into an integration-test target (a new file outside Task 5's allowed file list).

**Pre-existing breakage fixed en route:** at parent commit `8150878310` a clean-worktree `cargo test -p sidebar --lib --no-run` failed with 6 errors: `sidebar_tests.rs` calls `AgentPanel::has_terminal`, which was `pub(crate)` (invisible cross-crate). Verified against HEAD in a temporary worktree. Fixed minimally by widening `has_terminal` to `pub` (agent_panel.rs is in Task 5 scope); without it none of the required sidebar commands could compile.

## Test coverage added
1. `activating_herdr_root_requests_herdr_workspace_focus` — activating a Herdr row records exactly `focus_workspace:w1` on the recording API.
2. `activating_acp_root_does_not_request_herdr_focus` — with a bound recording bridge, activating an ACP-native row records zero Herdr calls.
3. `herdr_rows_persist_through_rebuilds_while_disconnected` — a disconnected Herdr row survives rebuilds, stays visible, and is never draft-classified.
4. `herdr_rename_and_close_route_through_the_bridge` — rename/close of a Herdr root produce `rename_workspace:w1:*` / `close_workspace:w1`; unknown ids are not routed.
5. `herdr_status_label_tracks_conflict_and_connection_states` — Unavailable → Conflict on a conflict event → cleared on a new sync run; session name exposed.

## Concerns / notes for review
- `create_herdr_root` relies on the server broadcasting `WorkspaceCreated` (which reconciliation persists) and activates on `RootCreated`; a race window is covered by checking the mapping again after the create response resolves.
- The synthetic status test drives `StatusChanged` through the panel handler, so the label falls back to the bridge's real status rather than the synthetic one; asserted accordingly.
- Session labels appear only under multi-Herdr-row ambiguity, per the brief's "only when needed"; single historical rows render their stored titles untouched.
- Task 6 items (fake NDJSON server fixture, transport/socket coverage, real smoke scenarios) were intentionally not started.
