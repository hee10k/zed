# Herdr Task 2 Implementation Report: Durable Mappings and Pure Reconciliation

## Status

Complete.

## Commit

`feat(agent_ui): persist Herdr thread mappings`

## Changed Interfaces

- `crates/agent_ui/src/herdr_mapping_store.rs`
  - `HerdrMappingKey`: session-qualified workspace and agent-pane identities.
  - `HerdrMappingRecord` and `HerdrLifecycleState`: durable Zed root/subthread mapping metadata with retained closed tombstones.
  - `HerdrMappingStore`: one atomically replaced serialized map per Herdr session under the dedicated `herdr_thread_mappings` `ScopedKeyValueStore` namespace.
  - Session records are validated on load/save; cwd/worktree identity is persisted only for diagnostics and never used for matching.
- `crates/agent_ui/src/herdr_state.rs`
  - `BridgeState`, `HerdrOperationOrigin`, `FocusTarget`, `OutboundRequest`, and `ReconciliationAction`.
  - Pure snapshot/event reconciliation returning actions only; it does not mutate GPUI entities.
  - Exact session-qualified mapping wins, agent-session restoration handles pane/workspace rebinding, ambiguous matches become conflicts, and tombstones block stale resurrection.
  - Strict non-increasing sequence rejection plus operation-ID focus reflection suppression.
- `crates/agent_ui/src/agent_ui.rs`
  - Registers the two Task 2 modules.

## Verification

```text
$ cargo test -p agent_ui herdr_mapping_store::tests
running 9 tests
test herdr_mapping_store::tests::same_workspace_id_in_different_sessions_never_collides ... ok
test herdr_mapping_store::tests::subthread_keys_differ_by_pane_and_agent_session ... ok
test herdr_mapping_store::tests::upsert_never_implicitly_resurrects_a_tombstone ... ok
test herdr_mapping_store::tests::tombstoning_an_unknown_or_already_closed_record_is_a_no_op ... ok
test herdr_mapping_store::tests::tombstones_are_kept_not_deleted ... ok
test herdr_mapping_store::tests::session_map_rejects_records_from_another_session ... ok
test herdr_mapping_store::tests::missing_session_decodes_to_empty_map_and_bad_payload_is_rejected ... ok
test herdr_mapping_store::tests::session_map_round_trips_through_serialization ... ok
test herdr_mapping_store::tests::store_atomically_replaces_one_session_map ... ok
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 484 filtered out

$ cargo test -p agent_ui herdr_state::tests
running 25 tests
test herdr_state::tests::focus_with_different_operation_id_does_not_consume_pending_op ... ok
test herdr_state::tests::generated_operation_ids_are_unique ... ok
test herdr_state::tests::identical_cwd_never_merges_mappings ... ok
test herdr_state::tests::foreign_focus_event_activates_without_outbound_echo ... ok
test herdr_state::tests::agent_without_session_identity_creates_no_subthread ... ok
test herdr_state::tests::ambiguous_agent_session_becomes_a_conflict_not_a_duplicate ... ok
test herdr_state::tests::agent_detection_reconciles_like_the_snapshot_path ... ok
test herdr_state::tests::initiating_focus_produces_exactly_one_outbound_request ... ok
test herdr_state::tests::agent_session_restoration_rebinds_the_workspace_and_pane ... ok
test herdr_state::tests::initiating_pane_focus_registers_and_suppresses_its_reflection ... ok
test herdr_state::tests::pane_close_retains_a_tombstone ... ok
test herdr_state::tests::pane_exit_completes_the_subthread_with_a_tombstone ... ok
test herdr_state::tests::pane_focus_requires_matching_workspace_identity ... ok
test herdr_state::tests::reflected_focus_operation_is_not_emitted_again ... ok
test herdr_state::tests::reflected_focus_advances_the_sequence_fence ... ok
test herdr_state::tests::repeated_sequence_event_is_rejected ... ok
test herdr_state::tests::rename_and_status_events_update_existing_mappings_only ... ok
test herdr_state::tests::snapshot_restores_existing_workspace_mapping ... ok
test herdr_state::tests::snapshot_creates_roots_for_unknown_workspaces ... ok
test herdr_state::tests::snapshot_tombstoned_workspace_is_a_conflict_not_a_resurrection ... ok
test herdr_state::tests::snapshot_restores_subthread_by_agent_session_after_restart ... ok
test herdr_state::tests::stale_sequence_events_are_rejected ... ok
test herdr_state::tests::workspace_created_emits_root_creation_for_its_snapshot ... ok
test herdr_state::tests::workspace_close_tombstones_descendant_subthreads ... ok
test herdr_state::tests::workspace_close_tombstones_and_rejects_late_focus ... ok
test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 468 filtered out
```

The focused tests defend session-qualified uniqueness, serialized-map replacement through the real KVP store, session mismatch rejection, tombstone retention, exact and agent-session restoration, cwd non-matching, ambiguity conflicts, root/pane close handling, stale and repeated sequence rejection, workspace/pane focus suppression, and workspace/pane lifecycle reconciliation.

## Concerns

Task 1's existing `HerdrApi::focus_workspace` and `focus_pane` implementations accept `operation_id` but currently omit it from their RPC payloads. Task 2 maintains the complete pure operation-ID/origin fence; Task 3 must ensure focus RPC propagation preserves the operation ID so Herdr can reflect it for end-to-end loop suppression.

Unrelated untracked `.scratch`, `.superpowers`, and workspace recovery-plan files were left untouched.
