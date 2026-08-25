# Task 3 Herdr Bridge Report

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
cargo test: 8 passed (1 suite, 512 filtered, 0.01s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 468 filtered, 0.69s)

cargo test -p agent_ui herdr_ --no-default-features
cargo test: 93 passed (1 suite, 427 filtered, 0.16s)

cargo test -p agent_ui agent_panel::tests -- --exact
cargo test: ok (1 suite, 520 filtered, 0.00s)

cargo test -p agent_ui agent_panel::tests::test_non_native_thread_without_metadata_is_not_restored -- --exact
cargo test: 1 passed (1 suite, 519 filtered, 0.19s)

cargo test -p agent_ui herdr_client::tests
cargo test: 29 passed (1 suite, 491 filtered, 0.16s)

cargo check -p agent_ui --tests
Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Notes

Task 4's Herdr conversation/subthread rendering remains intentionally out of scope. Herdr roots are represented as durable metadata and routed without fabricating ACP sessions until that surface is added.
