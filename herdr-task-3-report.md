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
