# Task 3 Herdr Bridge Report

## Delivered

- Added the per-window `HerdrBridgeRegistry` and `HerdrThreadBridge` lifecycle, session selection/rebinding, status transitions, snapshot/event reconciliation, root metadata persistence, focus operation fencing, and bridge event publication.
- Registered the bridge registry during `agent_ui::init`.
- Integrated `AgentPanel` with per-window bridge registration, Herdr root activation, ACP-load routing, Herdr-backed workspace close requests, and reflected root focus suppression.
- Reserved `HERDR_AGENT_ID` and kept Herdr roots with no ACP session out of draft cleanup while preserving ACP draft behavior.
- Added focused lifecycle, snapshot, focus-fencing, session-rebind, and metadata draft tests.

## Verification

Commands were run from the repository root without formatters, linters, or project-wide suites:

```text
cargo test -p agent_ui herdr_bridge::tests
cargo test: 8 passed (1 suite, 510 filtered, 0.00s)

cargo test -p agent_ui thread_metadata_store::tests
cargo test: 52 passed (1 suite, 466 filtered, 0.70s)

cargo test -p agent_ui agent_panel::tests::test_non_native_thread_without_metadata_is_not_restored -- --exact
cargo test: 1 passed (1 suite, 517 filtered, 0.19s)
```

## Notes

Task 4's Herdr conversation/subthread rendering remains intentionally out of scope. Herdr roots are represented as durable metadata and routed without fabricating ACP sessions until that surface is added.
