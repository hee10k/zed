# Herdr Task 5 Report — Sidebar / Session Navigation

## Scope executed
- `crates/sidebar/src/sidebar.rs`
  - Herdr rows continue through `load_agent_thread_in_workspace` using their persisted root mapping and never enter ACP loading. Local activation, cross-window activation, archive restore, and thread-switcher selection preserve the existing ACP path while Herdr roots route through the window bridge.
  - `ArchiveSelectedThread` now recognizes Herdr metadata (`HERDR_AGENT_ID`) when `session_id` is absent and routes the row to `workspace.close` instead of silently returning. The row action uses the same Herdr close path.
  - `apply_thread_rename` returns immediately after the first panel successfully routes a Herdr rename, preventing duplicate `workspace.rename` requests when panels share one bridge.
  - `close_herdr_entry` now loads `AgentPanel` asynchronously for the matching workspace when no panel is present, then issues `workspace.close` through the loaded bridge. Loaded-panel behavior is unchanged.
  - `new_herdr_entry` now loads/adds the lazy `AgentPanel` before issuing `workspace.create`; the old missing-panel path was a silent no-op.
  - Thread-switcher preview still activates the Zed workspace and reveals the panel, but Herdr `workspace.focus` is sent only by committed activation (`focus=true`). Preview followed by confirm therefore emits one focus request, not two.
  - Herdr metadata rebuild behavior remains persisted by worktree/cwd identity, visible while disconnected, and excluded from ACP draft classification.
  - Added deterministic regressions for archive routing, shared-panel rename routing, lazy create-panel loading, lazy close-panel loading, and switcher preview focus suppression.
- `crates/sidebar/src/thread_switcher.rs`: no functional change required; both preview and confirm already funnel through the sidebar loading helper.
- `crates/agent_ui/src/agent_panel.rs`
  - Session-editor blur now dismisses with `commit=false`; only Confirm/Newline commits a selected session.
  - Successful `workspace.create` responses are reconciled directly into the bridge mapping and `ThreadMetadataStore` before the returned root is activated. Activation no longer depends on a separate `WorkspaceCreated` event.
  - Added `SessionRebound` bridge-event handling so every AgentPanel subscribed to a shared window bridge clears stale Herdr state and returns to a draft after explicit rebinding.
  - Herdr loading sends `workspace.focus` only when the caller requested focus; background/switcher preview loads do not focus Herdr.
  - `has_terminal` is crate-private again. Cross-crate sidebar tests use the existing public `terminals` accessor instead of widening the API.
  - Test support includes a configurable create response and a shared-bridge helper for deterministic dependent-crate regressions.
- `crates/agent_ui/src/herdr_bridge.rs`
  - Added the minimal create-response reconciliation seam, which persists the root mapping/metadata without requiring a pushed create event.
  - `rebind_selection` emits the explicit `SessionRebound` event to the bridge's subscribers before starting the new synchronization worker; shared-panel subscribers can reset together.
  - Existing test-only `RecordingHerdrApi` now accepts one controlled create response.
- `crates/agent_ui/src/agent_ui.rs`: existing `ConnectHerdrSession` action remains registered from `agent_panel::init`.

## Verification commands and results

| Command | Result |
|---|---|
| `cargo test -p agent_ui agent_panel::tests::herdr::herdr_session_editor_blur_dismisses_without_rebinding -- --exact` | **1 passed; 0 failed** |
| `cargo test -p agent_ui agent_panel::tests::herdr::herdr_create_response_persists_mapping_and_activates_without_event -- --exact` | **1 passed; 0 failed** |
| `cargo test -p agent_ui agent_panel::tests::herdr::session_rebind_resets_active_herdr_surface -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::activating_acp_root_does_not_request_herdr_focus -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::archiving_herdr_root_routes_to_workspace_close -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::switcher_preview_does_not_focus_herdr_root_until_confirm -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::new_herdr_thread_loads_lazy_panel_before_create -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::closing_herdr_root_loads_bridge_when_panel_is_lazy -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::herdr_rename_routes_once_when_multiple_panels_share_bridge -- --exact` | **1 passed; 0 failed** |
| `cargo test -p sidebar tests::` | **153 passed; 0 failed** |
| `cargo test -p agent_ui agent_panel::tests::herdr` | **8 passed; 0 failed** |
| `cargo test -p agent_ui herdr_bridge::tests` | **21 passed; 0 failed** |

The brief's literal `sidebar::tests::…` filter is not a matching unit-test path for this lib target; the same tests run under `tests::<name> -- --exact` as shown above.

## Regression coverage added
1. `herdr_session_editor_blur_dismisses_without_rebinding` — changing the session name then emitting editor blur dismisses the editor while retaining the original bridge session.
2. `herdr_create_response_persists_mapping_and_activates_without_event` — a controlled create response with no pushed create event persists mapping and metadata, then activates the returned Herdr root.
3. `session_rebind_resets_active_herdr_surface` — a rebound bridge event clears the active Herdr surface and leaves the panel on a draft.
4. `archiving_herdr_root_routes_to_workspace_close` — the selected Herdr row with no ACP session ID emits exactly `close_workspace:w1`.
5. `switcher_preview_does_not_focus_herdr_root_until_confirm` — preview emits no focus request; confirm emits exactly one.
6. `new_herdr_thread_loads_lazy_panel_before_create` — the New Herdr Thread path installs the lazy panel before attempting creation.
7. `closing_herdr_root_loads_bridge_when_panel_is_lazy` — close loads the missing AgentPanel/bridge for the owning workspace.
8. `herdr_rename_routes_once_when_multiple_panels_share_bridge` — two panels sharing one bridge still produce one rename request.
9. Existing ACP/sidebar terminal assertions now use `AgentPanel::terminals`, proving `has_terminal` remains crate-private while preserving coverage.

## Concerns / notes for review
- Herdr bridge mapping persistence remains asynchronous through the existing `HerdrMappingStore::save_session` background task; metadata is cached/persisted through `ThreadMetadataStore` before activation, matching the bridge's existing persistence model.
- A Herdr close requested for a row whose persisted paths do not match any currently open workspace is logged and not sent; there is no owning workspace/bridge to target in that state.
- The shared-panel reset is delivered by the explicit `SessionRebound` event emitted by `rebind_selection`; ordinary reconnect/bootstrap status events do not clear active Herdr surfaces.
- Task 6 items (fake NDJSON server fixture, transport/socket coverage, and real Herdr smoke scenarios) were intentionally not started.
