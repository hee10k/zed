# herdr Same-Window Session Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans (preferred) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep herdr session selection and agent synchronization inside the invoking Zed window while importing each herdr space as a Zed project/workspace.

**Architecture:** The application-global `HerdrSessionRegistry` remains the single session/agent owner. Confirmed selection always binds the invoking `MultiWorkspace`; the initial snapshot is imported into that `MultiWorkspace` with `OpenMode::Add`, and unmatched agent roots use the same-window add path instead of opening a top-level window. Herdr-specific bootstrap and inherited-window handoff code is removed because no herdr flow creates a new Zed window after this change.

**Tech Stack:** Rust, GPUI entities and `Task`, Zed `MultiWorkspace`/`Workspace::new_local`, `OpenMode::Add`, deterministic registry tests, Windows herdr smoke testing.

## Global Constraints

- Keep one herdr binding per Zed window and one shared registry connection per session.
- Use the existing `herdr::canonical_checkout_path` and `MultiWorkspace` root APIs; do not add a second path-normalization scheme.
- Add projects with `Workspace::new_local(..., OpenMode::Add)` and the invoking `WindowHandle<MultiWorkspace>`; never use `OpenMode::NewWindow` for herdr selection or agent mirroring.
- Existing session list, endpoint, stream, stable `(session, terminal_id)` identity, focus echo, dismissal, and normal-exit behavior remain unchanged.
- Picker cancellation leaves the current binding unchanged.
- Invalid roots are isolated to the root/agent notification; valid roots and the session connection continue.
- Do not add ACP import/resume, new herdr protocol fields, retries, or unrelated workspace/window refactors.
- Before changing a cross-file exported symbol, run LSP references and migrate every caller; the expected implementation changes are private to `herdr_session_registry.rs`.
- Before modifying any Rust source file, prepend the required two-line `> [!IMPORTANT]` marker to `README.md` if it is not already present, and leave it for the human reviewer.

---

### Task 1: Add a red regression seam for same-window routing

**Files:**
- Modify: `crates/zed/src/zed/herdr_session_registry.rs:250-457,4277-4475`
- Test: `crates/zed/src/zed/herdr_session_registry.rs` existing `#[cfg(test)]` registry module

**Interfaces:**
- Produce a pure helper `snapshot_checkout_paths(snapshot: &SessionSnapshot) -> Vec<herdr::CanonicalPath>` that preserves first-seen snapshot order and removes canonical duplicates.
- Replace the old selection-target expectation with the invariant that every confirmed selection uses the invoking window.
- Keep the test fixtures GPUI-free for path planning; use the existing fake snapshot and registry window helpers for lifecycle coverage.

- [ ] **Step 1: Write the failing pure tests**

Add tests with observable contracts, not implementation details:

```rust
#[test]
fn snapshot_checkout_paths_preserve_order_and_deduplicate() {
    let mut snapshot = empty_snapshot();
    snapshot.workspaces = vec![
        workspace_with_checkout("C:/repo/one"),
        workspace_with_checkout("C:/repo/two"),
        workspace_with_checkout("c:/repo/one"),
        workspace_without_checkout(),
    ];

    assert_eq!(
        snapshot_checkout_paths(&snapshot),
        vec![checkout("C:/repo/one"), checkout("C:/repo/two")]
    );
}

#[test]
fn every_binding_state_selects_the_invoking_window() {
    let states = [
        BindingState::Unselected,
        BindingState::Starting { session_name: Arc::from("main") },
        BindingState::Connected(session("main")),
        BindingState::Failed {
            session_name: Arc::from("main"),
            message: "failed".into(),
        },
    ];

    for state in states {
        assert_eq!(selection_target(&state), SelectionTarget::InvokingWindow);
    }
}
```

Use the existing `workspace_with_checkout`/`workspace_without_checkout` fixture helpers or add those exact small constructors beside `agent_snapshot()` if they do not exist.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry::tests::snapshot_checkout_paths_preserve_order_and_deduplicate -- --exact
cargo test -p zed --bin zed herdr_session_registry::tests::every_binding_state_selects_the_invoking_window -- --exact
```

Expected: the first test cannot resolve `snapshot_checkout_paths`; the second fails for `Starting`, `Connected`, and `Failed` because the current implementation returns `NewWindow`.

- [ ] **Step 3: Implement the minimal pure planner**

Implement `snapshot_checkout_paths` by iterating `snapshot.workspaces`, converting each `WorkspaceInfo::checkout_path()` through `canonical_checkout_path`, and retaining only the first occurrence. Use a `HashSet<CanonicalPath>` only for membership; push into a `Vec` to preserve herdr snapshot order. Invalid paths are skipped here so the asynchronous importer can report/continue at its existing boundary.

Change `selection_target` to return `SelectionTarget::InvokingWindow` for every `BindingState`. Do not change the test until the implementation makes the test green.

- [ ] **Step 4: Run the focused tests and verify they pass**

Run the two commands from Step 2. Expected: PASS.

- [ ] **Step 5: Commit the regression seam**

```text
git add crates/zed/src/zed/herdr_session_registry.rs
git commit -m "test(herdr): pin same-window session routing"
```

---

### Task 2: Remove herdr-specific top-level window selection and handoff

**Files:**
- Modify: `crates/zed/src/zed/herdr_session_registry.rs:94-457,700-940,1040-1245,1290-1405,1540-1580,2320-2695,2820-3362`
- Test: `crates/zed/src/zed/herdr_session_registry.rs` registry lifecycle and handoff tests

**Interfaces:**
- `apply_selection_for_window` confirms the current window as the only target.
- `start_binding_for_window` and `retry` no longer accept or restore a `SelectionTarget`; the binding target is implicitly the registered window.
- `FinalizeResult` has no inherited-window variant. Its population variant always names the invoking window.

- [ ] **Step 1: Write the failing replacement lifecycle test**

Add a GPUI test that registers one real `MultiWorkspace`, marks it `Connected`, confirms another `SessionSelection`, and asserts that:

```rust
assert_eq!(cx.windows().len(), 1);
assert!(matches!(registry.read_with(cx, |r, _| r.binding_state(window_id)), BindingState::Starting { .. }));
```

The test must also assert that the old binding is detached before the new attempt starts when the selected session differs. Keep the existing cancel test unchanged to pin no-op cancellation.

- [ ] **Step 2: Run the replacement test and related old tests to capture red output**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry::tests::connected_selection_reuses_invoking_window -- --exact
cargo test -p zed --bin zed herdr_session_registry::tests::selection_from_connected_window_always_targets_new_window -- --exact
```

Expected: the new test is absent/fails before the implementation, and the old test documents the obsolete behavior that must be renamed/replaced.

- [ ] **Step 3: Cut over selection confirmation**

In `apply_selection_for_window`, keep `prompt_completed`, call `disconnect_session(window_id, cx)` when the current state is not `Unselected`, and call `start_binding_for_window(window_id, Arc::from(selection.name()), cx)`. Delete `create_bootstrap_window`; no confirmed selection should call `Workspace::new_local(..., OpenMode::NewWindow)`.

Update `handle_new_window` so a newly observed ordinary window only registers itself and schedules its startup picker. Remove `PendingBinding`, `pending`, and the `ConfirmedSelection`/`Inherited` observer handoff branch because no herdr operation creates a window that needs causal inheritance.

- [ ] **Step 4: Delete unreachable herdr handoff machinery**

Remove `MirrorOpenRequest`, `MirrorWindowRouter`, `ProductionMirrorWindowRouter`, `SelectionTarget::NewWindow`, `Route`, `RoutingCandidate`, `InheritedHandoff`, `inherited_handoffs`, and their production/test-only helpers. Remove the inherited handoff cleanup from disconnect, stream-loss, failure, and `finish_connected` paths. Keep normal connection ownership (`connections`, `slot_generations`, `finalize_tokens`) intact.

Update all constructors and tests in the same file after field/type removal. Replace old tests asserting `OpenInherited`, inherited pending bindings, donor release, and inherited-open failure with same-window binding tests; delete tests whose only contract was creating a new herdr destination window.

- [ ] **Step 5: Update connection lifecycle call sites**

Change `run_connection` in all owner/shared branches so it reads only whether the attempt is current, calls the simplified `finalize_binding`, and handles the single invoking window result. Replace `FinalizeResult::Handoff` and `FinalizeResult::Activate` matches with the new same-window population variant. Keep deadline, connection-generation, stream-loss, and `finish_connected` gates unchanged.

- [ ] **Step 6: Run focused registry tests and verify the old path is gone**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry::tests -- --list
cargo test -p zed --bin zed herdr_session_registry::tests::connected_selection_reuses_invoking_window -- --exact
cargo test -p zed --bin zed herdr_session_registry::tests::startup_prompt_opens_picker_once_and_explicit_selection_cancels -- --exact
```

Use a repository search to confirm no production code in this file contains `OpenMode::NewWindow`, `create_bootstrap_window`, `OpenInherited`, or `MirrorWindowRouter`.

- [ ] **Step 7: Commit the selection cutover**

```text
git add crates/zed/src/zed/herdr_session_registry.rs
git commit -m "fix(herdr): keep session selection in invoking window"
```

---

### Task 3: Import all session spaces and unmatched agents into one MultiWorkspace

**Files:**
- Modify: `crates/zed/src/zed/herdr_session_registry.rs:1900-2255,2320-2660,3360-3410,3429-4075`
- Test: `crates/zed/src/zed/herdr_session_registry.rs` initial snapshot and mirror routing tests

**Interfaces:**
- Add an async helper with this contract:

```rust
async fn add_workspace_root(
    window: WindowHandle<MultiWorkspace>,
    root: herdr::CanonicalPath,
    cx: &mut AsyncApp,
) -> anyhow::Result<()>
```

It checks the invoking `MultiWorkspace` for an exact existing root, and only when absent calls `Workspace::new_local(vec![PathBuf::from(root.as_str())], app_state, Some(window), None, None, OpenMode::Add, cx)` and awaits the returned task.

- Add a sequential helper:

```rust
async fn import_snapshot_workspaces(
    window: WindowHandle<MultiWorkspace>,
    roots: Vec<herdr::CanonicalPath>,
    cx: &mut AsyncApp,
) -> anyhow::Result<()>
```

It calls `add_workspace_root` in input order and continues after an individual root error while reporting that root through the existing registry notification seam.

- `mirror_agent` uses the invoking bound window as the unmatched-root fallback; it never invokes a top-level-window router.

- [ ] **Step 1: Write the failing same-window import test**

Add a GPUI test using the existing fake gateway and real test windows:

1. Create one `MultiWorkspace` rooted at `C:/existing`.
2. Return a snapshot with `C:/space-a`, `C:/space-b`, and two agents rooted in those spaces.
3. Start one binding against the fake connection.
4. Pump GPUI until the connection is connected.
5. Assert exactly one top-level window remains and the invoking `MultiWorkspace` contains the existing root plus the two imported roots.

Add a second test with an agent root not present in `snapshot.workspaces`; assert the mirror remains associated with the invoking window and no new top-level window is created.

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry::tests::initial_snapshot_imports_all_spaces_into_invoking_window -- --exact
cargo test -p zed --bin zed herdr_session_registry::tests::unmatched_agent_root_stays_in_invoking_window -- --exact
```

Expected: the first test imports only the focused worktree/current behavior, and the second follows the removed/new-window mirror path before the implementation is complete.

- [ ] **Step 3: Implement same-window root addition**

Use the window's existing `workspace_for_paths`/live root inspection to skip exact duplicates. Obtain `AppState` from the invoking `MultiWorkspace` and call `Workspace::new_local` with `Some(window)` and `OpenMode::Add`. Serialize additions through the sequential snapshot helper so two spaces cannot race each other during initial import.

When an individual add task fails, report the path through the existing per-agent/worktree notification mechanism and continue with the next root. Do not transition the session binding to `Failed` for an individual project import.

- [ ] **Step 4: Populate all snapshot spaces before agent effects**

Change `finalize_binding` to:

1. Install the binding target on `bootstrap`.
2. Compute `snapshot_checkout_paths(snapshot)`.
3. Return a population task that calls `import_snapshot_workspaces` against the bootstrap window.
4. Return the bootstrap window as the only mirror target after the task completes.

Replace focused-only activation with this task. Do not call `OpenMode::Activate` or create a destination window. Refresh the registry's live roots after the task so subsequent agent routing sees every imported project.

- [ ] **Step 5: Route unmatched agents to the invoking window**

In `mirror_agent`, preserve exact/ancestor matching. If no root is found, resolve the target/source window from the connection and call `add_workspace_root` for the discovered root; then continue with `open_mirror_in_window` on that same handle. If the root cannot be added, report one revision-scoped failure and return. Remove the old router field and all top-level window opening code.

Keep `mirroring_in_flight` and `pending_mirror_replay` semantics unchanged so repeated agent revisions remain deduplicated.

- [ ] **Step 6: Run the import and mirror regression tests**

Run the two commands from Step 2 plus:

```text
cargo test -p zed --bin zed herdr_session_registry::tests::shared_connection_is_kept_until_last_window_and_then_can_be_recreated -- --exact
cargo test -p zed --bin zed herdr_session_registry::tests::mirror_spawn_failure_notifies_once_and_resyncs_live_agents -- --exact
```

Expected: all pass, with one top-level window for same-window session import and unchanged shared-connection/mirror failure behavior.

- [ ] **Step 7: Commit same-window import**

```text
git add crates/zed/src/zed/herdr_session_registry.rs
git commit -m "fix(herdr): import session workspaces into one window"
```

---

### Task 4: Run final verification and Windows smoke coverage

**Files:**
- Modify: `docs/superpowers/specs/2026-09-07-herdr-same-window-session-import-design.md` only if implementation evidence corrects a factual statement
- Modify: `docs/superpowers/plans/2026-09-07-herdr-same-window-session-import.md` only to record completed verification if repository convention requires it

- [ ] **Step 1: Run the focused registry suite**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry::tests
```

Expected: the registry unit and GPUI tests pass. If the environment cannot resolve `cargo`, record the exact toolchain failure and use the repository's configured Windows developer shell before falling back to a source-level verification.

- [ ] **Step 2: Run the affected crate checks**

Run:

```text
cargo check -p zed --bin zed
cargo test -p herdr
```

Expected: both complete successfully without changes to the herdr protocol crate.

- [ ] **Step 3: Verify the exact source contract**

Search the changed registry for `OpenMode::NewWindow`, `open_window`, `create_bootstrap_window`, `OpenInherited`, and `MirrorWindowRouter`. Expected: no herdr session/agent path contains those operations. Confirm `OpenMode::Add` is used by both snapshot import and unmatched-agent fallback.

- [ ] **Step 4: Run the Windows smoke scenario**

With the installed `herdr.exe` and Zed development binary:

1. Start one ordinary Zed window.
2. Select a session containing at least two spaces and two agents.
3. Confirm the top-level Zed window count remains one.
4. Confirm the current window's project/worktree entries match the valid herdr space checkout roots without duplicates.
5. Confirm mirrored agent terminals appear in that same window.
6. Select another session from the connected window and confirm replacement occurs in the same window.
7. Cancel a subsequent picker and confirm the current binding and projects remain unchanged.

- [ ] **Step 5: Remove temporary test-only artifacts and close the task**

Delete only obsolete tests/helpers removed by the new same-window contract. Do not remove the required `README.md` review marker or the committed design/plan documents. Confirm the unrelated `untitled.md` user file remains untouched.

- [ ] **Step 6: Commit final verification/documentation updates if any**

```text
git add docs/superpowers/specs/2026-09-07-herdr-same-window-session-import-design.md docs/superpowers/plans/2026-09-07-herdr-same-window-session-import.md
 git commit -m "docs(herdr): record same-window verification"
```

Only create this final commit when the documents changed; otherwise leave the source commits as the complete implementation.
