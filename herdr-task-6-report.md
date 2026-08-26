# Herdr–Zed Task 6 Verification Report

## Scope

Task 6 adds `crates/agent_ui/src/herdr_test_support.rs`, registered under the
`test-support`/test configuration in `agent_ui`. The fixture is a real local
NDJSON endpoint: Unix domain socket on macOS/Linux and a Windows named-pipe
server under `cfg(windows)`. It records request IDs, methods, and parameters;
serves deterministic snapshots and response sequences; returns revisioned pane
reads; buffers events before subscription acknowledgement; delivers pushed
events; and exposes disconnect/reconnect and connection-acceptance controls.

The verification tests cover:

- workspace-root and pane/subthread focus in both directions;
- one outbound focus request per user activation and no reflected-focus loop;
- workspace/agent creation, rename, close, pane exit, session identity arrival,
  stale-event rejection, session rebinding, and ambiguous identity conflicts;
- retaining a mapped root while Herdr is unavailable without synthesizing
  `agent.cancel` or `agent.close` requests;
- subscribe-before-snapshot bootstrap ordering and controlled replay;
- Unix newline framing, concurrent request matching, subscription delivery, EOF,
  and reconnect;
- Windows named-pipe framing/request/event/reconnect coverage (compile-gated).

The only production changes are crate-local visibility on existing client test
seams (`new_with_executor`, `request_on_executor`, `start_subscription`, and
request-parameter helpers) needed by the fixture, plus module registration.

## Host verification (macOS)

Commands were run from the repository root. Results are the complete test
summaries reported by Cargo:

```text
$ cargo test -p agent_ui herdr_client
cargo test: 37 passed (1 suite, 525 filtered, 0.15s)

$ cargo test -p agent_ui herdr_transport
cargo test: 6 passed (1 suite, 556 filtered, 0.16s)

$ cargo test -p agent_ui herdr_mapping_store
cargo test: 12 passed (1 suite, 550 filtered, 0.00s)

$ cargo test -p agent_ui herdr_state
cargo test: 37 passed (1 suite, 525 filtered, 0.00s)

$ cargo test -p agent_ui herdr_bridge
cargo test: 21 passed (1 suite, 541 filtered, 0.01s)

$ cargo test -p agent_ui herdr_conversation_view
cargo test: 5 passed (1 suite, 557 filtered, 0.01s)

$ cargo test -p agent_ui herdr_thread_view
cargo test: 2 passed (1 suite, 560 filtered, 0.00s)

$ cargo test -p agent_ui agent_panel::tests::herdr
cargo test: 8 passed (1 suite, 554 filtered, 0.96s)

$ cargo test -p sidebar sidebar::tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact
cargo test: ok (1 suite, 153 filtered, 0.00s)

$ cargo test -p agent_ui herdr_test_support -- --nocapture
cargo test: 6 passed (1 suite, 556 filtered, 0.03s)
```

`./script/clippy` was available and was run as required. It invokes:

```text
cargo clippy --workspace --release --all-targets --all-features -- --deny warnings
```

It is currently blocked by an unrelated existing warning-as-error in
`crates/git/src/repository.rs:4205`: Clippy `unnecessary_to_owned` reports the
`.trim().to_string()` expression. No Task 6 source error was reported before
that workspace failure.

An additional feature-only compile probe,
`cargo check -p agent_ui --features test-support`, was also blocked outside
this change by `crates/remote_connection/src/remote_connection.rs:245`, where
`RemoteConnectionOptions::Mock(_)` is not handled, plus a pre-existing unused
import warning in `crates/fs/src/fake_git_repo.rs`.

The installed Rust target was probed with:

```text
$ cargo check -p agent_ui --target x86_64-pc-windows-msvc --features test-support
failed before compiling agent_ui: psm could not find lib.exe and stacker
could not find windows.h while cross-compiling from macOS
```

After the verification commit, one parallel test run transiently failed two
fixture tests (`ping: Disconnected` and `first subscription: Io("Broken pipe")`)
while four tests passed. The failing tests passed when rerun individually, and
the full fixture suite passed on the subsequent run:

```text
$ cargo test -p agent_ui herdr_test_support -- --nocapture
cargo test: 6 passed (suite rerun; 556 filtered, 0.02s)

$ cargo test -p agent_ui herdr_test_support -- --test-threads=6
cargo test: 6 passed (1 suite, 556 filtered, 0.02s)
```



## Platform expectations and limitations

- **macOS:** Unix fixture and transport tests ran successfully on this host.
- **Linux:** Run the same Unix tests in Linux CI; no Linux host is available in
  this verification run.
- **Windows:** The fixture includes a named-pipe server and a reconnect test
  under `cfg(windows)`. Run the test and a Windows target compile in Windows CI;
  named pipes cannot be exercised on this macOS host.
- The fixture never reads a Windows marker file as a byte stream. Its endpoint
  is converted to the namespaced pipe name by the production transport.

## Real-Herdr manual smoke command

The repository does not provide a Herdr launcher or installation command. With
a real Herdr installation running in one terminal under a named session, build
and launch Zed from this checkout in another:

```bash
# Terminal 1: use the installed Herdr launcher for the local installation.
herdr --session alpha

# Terminal 2:
HERDR_SESSION=alpha cargo run -p zed --release -- "$PWD"
```

Then exercise the plan's manual flow: create two Herdr workspaces and two
recognized agent panes; focus workspaces and panes in both directions; prompt,
cancel, rename, and close; restart Herdr; and rebind the Zed window between two
named sessions. Repeat the launcher/Zed flow on macOS, Linux, and Windows.
The `herdr --session alpha` line is the expected installed-Herdr CLI shape; if a
packaged installation uses another launcher, substitute that launcher while
preserving the named session and local endpoint.

## Final whole-branch review repairs

All nineteen findings in `.superpowers/sdd/final-review-findings.md` were
reproduced against the active implementation and fixed:

1. Agent restoration now atomically removes the prior live mapping and its
   derived snapshot/publication state.
2. Snapshot reconciliation merges nested agents with protocol-level
   `snapshot.agents` and `snapshot.panes` without duplicate identity entries.
3. Unsequenced watcher reads replace the complete pane buffer; sequenced
   deltas retain append behavior and revision fencing.
4. Snapshot-reported session names no longer overwrite the stable
   selection/endpoint mapping key.
5. Status and output forwarding use live-only mapping lookup after tombstones.
6. Identity-less agent detection emits a status-only bridge/view entry and
   identity arrival upgrades it to a selectable subthread.
7. Context action batches emit lifecycle events once, after all actions are
   applied.
8. Added an end-to-end fake-server focus round trip asserting one encoded
   request for workspace focus and one for pane focus, with operation IDs and
   `zed` origins.
9. Windows named-pipe setup accepts the `ERROR_PIPE_CONNECTED` success race.
10. Fixture subscription IDs are allocated while holding the registration
    lock.
11. Unknown fixture methods return top-level protocol errors, surfaced by the
    client as `HerdrClientError::ProtocolError`.
12. Unix fixture accept retries `Interrupted` and `ConnectionAborted` errors.
13. Session disambiguation is stored as presentation-only row state, leaving
    durable Herdr titles unchanged for rename persistence.
14. Pending activation resolves through `AgentPanel::active_thread_id`, which
    includes Herdr roots.
15. Event routing activates and forwards to the owning panel, loading a lazy
    panel before replaying the event.
16. Leaving or replacing a Herdr surface clears its title editor without
    committing stale text.
17. Herdr roots use the title/editor toolbar path even when they have no ACP
    messages.
18. Create responses carry the selection session and bridge generation fence;
    stale responses are discarded after rebind.
19. Closed-workspace rows route close through the owning window bridge before
    attempting workspace loading.

## Final host verification

Commands were run from the repository root on macOS:

```text
$ cargo test -p agent_ui herdr_client
cargo test: 37 passed (1 suite, 534 filtered, 0.16s)

$ cargo test -p agent_ui herdr_transport
cargo test: 6 passed (1 suite, 565 filtered, 0.16s)

$ cargo test -p agent_ui herdr_mapping_store
cargo test: 12 passed (1 suite, 559 filtered, 0.01s)

$ cargo test -p agent_ui herdr_state
cargo test: 37 passed (1 suite, 534 filtered, 0.00s)

$ cargo test -p agent_ui herdr_bridge
cargo test: 26 passed (1 suite, 545 filtered, 0.00s)

$ cargo test -p agent_ui herdr_conversation_view
cargo test: 6 passed (1 suite, 565 filtered, 0.00s)

$ cargo test -p agent_ui herdr_thread_view
cargo test: 2 passed (1 suite, 569 filtered, 0.00s)

$ cargo test -p agent_ui agent_panel::tests::herdr
cargo test: 8 passed (1 suite, 563 filtered, 0.88s)

$ cargo test -p agent_ui herdr_test_support
cargo test: 9 passed (1 suite, 562 filtered, 0.02s)

$ cargo test -p sidebar tests::
cargo test: 153 passed (1 suite, 23.99s)
```

No formatter, linter, or project-wide suite was run. Windows named-pipe
behavior remains compile/CI-gated on this macOS host; the fixture-specific
`ERROR_PIPE_CONNECTED` branch is covered by the Windows implementation.

The exact activation regression was also rerun:

```text
$ cargo test -p sidebar tests::activating_herdr_root_requests_herdr_workspace_focus -- --exact
cargo test: 1 passed (1 suite, 152 filtered, 0.25s)
```

## Residual gate repairs

The final-gate residuals were repaired:

1. The Windows `ERROR_PIPE_CONNECTED` race now returns `Ok(true)` and returns
   other `ConnectNamedPipe` errors through the `Result` chain instead of
   constructing an `Option<bool>`. This code is `cfg(windows)` and remains
   compile-gated on the macOS host.
2. The fixture focus round trip now constructs a real
   `HerdrThreadBridge` entity backed by `HerdrClientHandle` and calls
   `focus_root_in_context` and `focus_pane_in_context`. It asserts one
   fixture request per direction, encoded operation IDs/origins, and no
   outbound request after each reflected focus event.
3. Identity-less pane snapshots are retained in bridge state, replayed by
   `refresh_from_bridge`, and removed when the pane receives an identity.
4. Multi-workspace event routing no longer forwards an event to an already
   subscribed owning panel. Lazy owner loading still forwards when no owner
   panel is loaded.

The new regressions cover status-only snapshot retention/upgrading and the
panel refresh path.

## Residual repair verification

Commands were run from the repository root on macOS:

```text
$ cargo test -p agent_ui herdr_test_support
cargo test: 9 passed (1 suite, 564 filtered, 0.02s)

$ cargo test -p agent_ui herdr_bridge
cargo test: 27 passed (1 suite, 546 filtered, 0.00s)

$ cargo test -p agent_ui herdr_conversation_view
cargo test: 6 passed (1 suite, 567 filtered, 0.00s)

$ cargo test -p agent_ui agent_panel::tests::herdr
cargo test: 9 passed (1 suite, 564 filtered, 1.11s)

$ cargo test -p sidebar tests::
cargo test: 153 passed (1 suite, 24.09s)
```

The Windows named-pipe branch was not executable on this macOS host; Windows
compilation and the named-pipe regression remain CI-gated.
