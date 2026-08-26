# Herdr Task 4 Report

## Scope

Implemented explicit `HerdrConversationView` and `HerdrThreadView` GPUI entities, revision-safe child state, identity-gated selectability, bridge-routed prompt/cancel/focus/rename/close/output hydration, AgentPanel root integration, focus confirmation routing, and Herdr-only serialization/restoration metadata.

Herdr-backed views do not construct ACP sessions or write directly to terminal processes. Child cancellation uses the supported `agent.send_keys` `CTRL_C` request.

## Verification

Commands run from the repository root:

- `cargo check -p agent_ui --lib`
  - PASS (warnings only; no compilation errors).
- `cargo test -p agent_ui herdr_conversation_view::tests`
  - PASS: 5 tests.
- `cargo test -p agent_ui herdr_thread_view::tests`
  - PASS: 2 tests.
- `cargo test -p agent_ui agent_panel::tests::herdr`
  - PASS: focused AgentPanel Herdr filter (1 suite; 544 tests filtered).
- `cargo test -p agent_ui herdr_bridge::tests`
  - PASS: 17 tests, including pane identity/status/output/focus/close routing.
- `cargo test -p agent_ui herdr_client::tests`
  - PASS: 37 tests.
- `cargo test -p agent_ui herdr_bridge::tests::pane_agent_events_publish_selectable_output_status_and_close_updates -- --exact`
  - PASS: 1 test.

No formatter, linter, or project-wide test suite was run per Task 4 instructions.

## Review 1 Fixes (this commit)

1. **Background subthread events no longer steal the panel.** `AgentPanel::handle_herdr_subthread_event` now activates the Herdr surface only for `SubthreadFocused`; `SubthreadCreated/Updated/Output/Closed` (including reconnect snapshot re-emissions) update the open view in place, and only when the active Herdr root owns the event's `thread_id`. ACP threads/drafts are never replaced by background activity.
2. **Live pane events follow Task 2 conflict reconciliation.** `HerdrThreadBridge::reconcile_state_event` routes `PaneUpdated` through the same pure state machine as `PaneAgentDetected` (synthesized identity event), and `emit_subthread_event` became translation-only: it emits against the persisted live record or not at all. A pane whose agent restarts with an unknown identity now surfaces a `Conflict` event instead of gaining a second live mapping; tombstoned keys are never emitted (`live_subthread_record_for_pane`); bootstrap snapshot agents likewise only re-emit persisted identities. `SubthreadCreated` vs `SubthreadUpdated` is decided by whether the pane was ever published to UI consumers.
3. **RootRenamed and StatusChanged reach the open view.** Both variants are forwarded into the active `HerdrConversationView::apply_bridge_event`, so title and connection label stay live; sidebar titles continue to come from the bridge's metadata persistence before the event.
4. **Herdr title editing is functional.** The pencil button and a click on the Herdr toolbar title open a panel-managed single-line editor (terminal-editor lifecycle): Confirm/blur commits via `HerdrConversationView::request_rename` → bridge `workspace.rename`, Cancel discards, empty/unchanged text is a no-op, failures toast; the durable title and explicit override still change only when Herdr reflects `RootRenamed`.

### New regressions

- `herdr_bridge::tests::pane_updated_updates_the_persisted_subthread_without_recreating_it`
- `herdr_bridge::tests::restarted_agent_identity_conflicts_instead_of_duplicating_the_live_mapping`
- `herdr_bridge::tests::pane_update_with_foreign_identity_surfaces_a_conflict_without_emitting_a_key`
- `herdr_bridge::tests::pane_updated_before_detection_creates_the_subthread_once`
- `agent_panel::tests::herdr::background_subthread_events_do_not_activate_or_focus_the_herdr_surface`
- `agent_panel::tests::herdr::root_renamed_and_status_changed_forward_to_the_open_view`
- `agent_panel::tests::herdr::herdr_title_editing_requests_rename_through_the_bridge`

### Verification evidence after fixes

- `cargo test -p agent_ui herdr_conversation_view::tests`
  - PASS: 5 passed; 0 failed.
- `cargo test -p agent_ui herdr_thread_view::tests`
  - PASS: 2 passed; 0 failed.
- `cargo test -p agent_ui agent_panel::tests::herdr`
  - PASS: 3 passed; 0 failed.
- `cargo test -p agent_ui herdr_bridge::tests`
  - PASS: 21 passed; 0 failed.
- `cargo test -p agent_ui herdr_client::tests`
  - PASS: 37 passed; 0 failed.
- `cargo check -p agent_ui --tests`
  - PASS (warnings only; all pre-existing dead-code/import warnings in untouched code paths).

No formatter, linter, or project-wide test suite was run per Task 4 instructions.

## Concerns

- The Herdr UI intentionally remains status-only for panes that have not supplied a session identity; those panes are not selectable subthreads.
- Root title editing is wired through the panel editor to `request_rename`; the durable title (and explicit override) changes only when Herdr confirms via the reflected rename event. Subthread-level rename (`HerdrThreadView::request_rename`) remains unused by panel chrome and is reserved for per-card actions.
- Output hydration is asynchronous and revision-fenced; a late `pane.read` result cannot overwrite newer pane output.
