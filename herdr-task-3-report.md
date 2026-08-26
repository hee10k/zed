# Task 3 Herdr Bridge Report

## Review-2 repairs

- Added subscribe-before-discovery bootstrap ordering and cursor-partitioned replay so each event is applied once across bootstrap and the live stream.
- Retained and cancelled generation-owned subscriptions, woke stopped/rebound bridge workers, and retired/restarted pane output watchers on reconnect.
- Preserved fenced replay focus as authoritative over stale snapshot focus while emitting replay effects only once.
- Reloaded target-session mappings before rebind snapshot reconciliation, routed only through the owning MultiWorkspace workspace, and persisted newly archived root metadata by saving before archiving.
- Added focused lifecycle regressions for replay boundaries/effects/focus fencing, worker cancellation, and watcher retirement while preserving Task 1/2 transport and state behavior.

## Delivered

- Added generation-fenced, bounded reconnect with fresh bootstrap/subscription attempts, explicit lifecycle status transitions, and subscription termination detection.
- Reworked bootstrap replay around the client event boundary and arrival order, with replayed focus taking precedence over snapshot focus.
- Awaited metadata-store reload, merged stored overrides, and persisted every Herdr root before Ready; RootRenamed updates now persist while retaining overrides.
- Sent focus operation IDs and origins in the one Herdr API request envelope and retained reflection origin for fencing.
- Deferred bridge bootstrap until panel subscription registration, routed Herdr events through owning MultiWorkspace panels, surfaced conflicts/request failures as toasts, and restored the normal draft fallback after confirmed closure.
- Removed the unused competing `HerdrOperationRequest` envelope and added protocol payload/origin tests.

## Verification

Commands were run from the repository root without formatters, linters, or project-wide suites:

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 12 passed (1 suite, 514 filtered, 0.00s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 474 filtered, 0.62s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 99 passed (1 suite, 427 filtered, 0.15s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 526 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 31 passed (1 suite, 495 filtered, 0.15s)

cargo test -p agent_ui agent_panel::tests::test_non_native_thread_without_metadata_is_not_restored -- --exact
cargo test: 1 passed (1 suite, 525 filtered, 0.18s)
```

## Notes

Task 4's Herdr conversation/subthread rendering remains intentionally out of scope. Herdr roots are represented as durable metadata and routed without fabricating ACP sessions until that surface is added.

## Review-3 repairs

- Retained bootstrap bulk pane-filter kill switches in the generation-owned subscription list, fenced late handshakes, and cancelled every retained stream on failure, reconnect, rebind, or stop.
- Rejected a primary `subscription_ended` sentinel observed during bootstrap so the generation fails and the bridge reconnects instead of publishing Ready without a live primary stream.
- Stopped forwarding cursor-mode events to the unused legacy event channel; cursor delivery remains the sole live bridge stream and the legacy receiver no longer grows.
- Applied focus fencing only after accepting global/resource sequence order, so a delayed older fenced reflection cannot replace newer current focus.
- Added deterministic regressions for bulk filter teardown/late handshakes, primary bootstrap termination detection, legacy queue non-growth, and stale fenced focus ordering.

## Review-3 verification

Commands were run from the repository root without formatters, linters, or project-wide suites:

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 13 passed (1 suite, 516 filtered, 0.01s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 477 filtered, 0.66s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 102 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 529 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 33 passed (1 suite, 496 filtered, 0.15s)

cargo test -p agent_ui herdr_bridge::tests::stale_fenced_focus_does_not_replace_newer_current_focus -- --exact
cargo test: 1 passed (1 suite, 528 filtered, 0.01s)

cargo test -p agent_ui herdr_client::tests::bootstrap_bulk_filter_kill_is_retained_and_cancelled -- --exact
cargo test: 1 passed (1 suite, 528 filtered, 0.00s)
```

## Final focused rerun

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 13 passed (1 suite, 516 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 33 passed (1 suite, 496 filtered, 0.16s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 477 filtered, 0.65s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 102 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 529 filtered, 0.00s)
```

## Review-4 repairs

- Added a persistent generation/publish fence to request, primary/filter subscription, and pane-output watcher publishers. Established subscriptions also keep a cancellation watchdog alive so late Windows reads cannot publish after a stop, rebind, or retry boundary.
- Retained primary and bootstrap bulk-filter subscription IDs, reconnecting when either terminates while ignoring expected per-pane filter retirement. Bootstrap now rejects any terminated retained stream before exposing Ready.
- Tracked superseded local focus targets without treating every sequence-less external focus as stale. Sequence-less delayed local reflections are suppressed, while fenced operation IDs still pass through state reconciliation so they are consumed.
- Added deterministic regressions for established-pump cancellation, watcher output publication, bulk-filter termination, sequence-less focus ordering, and external sequence-less focus.
- Retried transient `events.wait` and `pane.read` watcher failures instead of leaving a Ready bridge without live output updates.

## Review-4 verification

Commands were run from the repository root without formatters, linters, or project-wide suites:

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 15 passed (1 suite, 519 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 36 passed (1 suite, 498 filtered, 0.16s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 482 filtered, 0.64s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 107 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 534 filtered, 0.00s)

cargo test -p agent_ui herdr_bridge::tests::stale_sequence_less_focus_does_not_replace_newer_focus -- --exact
cargo test: 1 passed (1 suite, 533 filtered, 0.01s)

cargo test -p agent_ui herdr_bridge::tests::bulk_filter_termination_is_reconnect_trigger -- --exact
cargo test: 1 passed (1 suite, 533 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests::established_subscription_pump_discards_events_after_generation_cancellation -- --exact
cargo test: 1 passed (1 suite, 533 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests::cancelled_generation_drops_watcher_output_publication -- --exact
cargo test: 1 passed (1 suite, 533 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests::bootstrap_rejects_bulk_filter_subscription_end_event -- --exact
cargo test: 1 passed (1 suite, 533 filtered, 0.00s)
```

## Final review-4 rerun

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 15 passed (1 suite, 519 filtered, 0.00s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 482 filtered, 0.61s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 107 passed (1 suite, 427 filtered, 0.15s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 534 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 36 passed (1 suite, 498 filtered, 0.15s)
```

## Final post-hardening rerun

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 15 passed (1 suite, 519 filtered, 0.00s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 482 filtered, 0.64s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 107 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 534 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 36 passed (1 suite, 498 filtered, 0.15s)
```

## Final error-path rerun

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 15 passed (1 suite, 519 filtered, 0.01s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 482 filtered, 0.66s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 107 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 534 filtered, 0.00s)

cargo test -p agent_ui herdr_client::tests
cargo test: 36 passed (1 suite, 498 filtered, 0.15s)
```
