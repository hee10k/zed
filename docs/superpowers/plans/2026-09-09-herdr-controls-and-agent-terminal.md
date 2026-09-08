# Herdr Controls and Agent Terminal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Herdr controls operate reliably, remove the meaningless maximize control, and open/reuse regular Zed terminals for Herdr agents without creating Zed Agent Panel threads.

**Architecture:** Keep Herdr session selection, registry binding, and the `herdr --session` host unchanged. Move all app-level Herdr window mutations behind a deferred `WindowHandle<MultiWorkspace>` update, and make both the status button and header Close use the same toggle action. Add a narrow TerminalPanel API that activates an existing shell terminal by working directory or creates a new shell. Replace only the Herdr agent-to-Zed Agent Panel effect with project activation plus that regular-terminal API.

**Tech Stack:** Rust, GPUI entities/windows/actions, Zed `Workspace`/`MultiWorkspace`, `terminal_view::TerminalPanel`, `cargo test`, Windows debug binaries.

## Global Constraints

- Herdr session selection and `herdr --session <name>` host attachment remain unchanged.
- Herdr agents remain managed by Herdr; Zed must not create `AgentPanel::open_external_terminal_thread` entries or run `herdr agent attach` for agent mirrors.
- Agent checkout paths are canonicalized and validated before workspace or terminal operations.
- Existing `OpenTerminal` action semantics remain unchanged; path-based reuse is a Herdr-only TerminalPanel API.
- GPUI window mutations dispatched from an action callback must occur after the current window lease is released.
- Preserve existing session identity, generation, workspace-root locking, and failure-reporting behavior.

---

### Task 1: Make Herdr controls use a safe, shared toggle path

**Files:**
- Modify: `crates/zed/src/zed/herdr_host.rs:20-143,305-343,586-699`
- Modify: `crates/zed/src/zed.rs:218-232`
- Modify: `crates/zed_actions/src/lib.rs:1023-1031`
- Test: `crates/zed/src/zed/herdr_host.rs` test module

**Interfaces:**
- Produces `with_active_multi_workspace(cx, action)`, which captures the active `WindowHandle<MultiWorkspace>` before `cx.defer` and runs the root mutation after the current action callback returns.
- Keeps `ToggleHerdr`, `FocusHerdr`, `ToggleHerdrCollapse`, `CloseHerdr`, and `ShowHerdrStatus`; removes `ToggleHerdrMaximize` and its handler.

- [ ] **Step 1: Write the failing control tests**

Add GPUI coverage that dispatches the Herdr toggle path while a window callback is active, then drains deferred work and asserts `MultiWorkspace::herdr_visible()` changes from true to false and back. Add a render test that draws `HerdRStatusButton`, asserts the `herdr-status-button` selector exists, and asserts no visible state-label element is emitted. Add a host render test that asserts `herdr-maximize` is absent and `herdr-close` remains present.

- [ ] **Step 2: Run the control tests and verify the old behavior fails**

Run:

```text
cargo test -p zed --bin zed herdr_host -- --nocapture
```

Expected before the implementation: the action-driven toggle test observes no visibility transition because the nested same-window update returns an error, and the maximize selector test finds the old control.

- [ ] **Step 3: Implement deferred action handling and control cleanup**

In `herdr_host.rs`:

```rust
fn with_active_multi_workspace(
    cx: &mut App,
    action: impl FnOnce(WindowHandle<MultiWorkspace>, &mut App) + 'static,
) {
    let Some(window) = cx
        .active_window()
        .and_then(|window| window.downcast::<MultiWorkspace>())
    else {
        return;
    };
    cx.defer(move |cx| action(window, cx));
}
```

Use this helper for `toggle_from_app`, `focus_from_app`, `close_from_app`, `status_from_app`, and `with_active_host`. Capture the window before deferring; do not call `active_window()` from inside the deferred callback.

Make the visible selected toggle contract strictly show/hide: hidden becomes visible and focused; visible becomes hidden and returns editor focus. Retain `FocusHerdr` for focus-only behavior. Remove `maximized`, `toggle_maximize`, `toggle_maximize_from_app`, the maximize button, its action registration, and the `ToggleHerdrMaximize` declaration. Remove only the visible `Label::new(label)` child from `HerdRStatusButton`; preserve its aria label and tooltip. Change the header Close click to dispatch `ToggleHerdr`.

- [ ] **Step 4: Run the focused control tests**

Run:

```text
cargo test -p zed --bin zed herdr_host -- --nocapture
```

Expected: all Herdr host tests pass; visibility transitions occur and the maximize selector is absent.

- [ ] **Step 5: Commit the control change**

```text
git add crates/zed/src/zed/herdr_host.rs crates/zed/src/zed.rs crates/zed_actions/src/lib.rs
git commit -m "fix(herdr): make controls toggle reliably"
```

---

### Task 2: Add path-based regular terminal reuse

**Files:**
- Modify: `crates/terminal_view/src/terminal_panel.rs:77-1000`
- Test: `crates/terminal_view/src/terminal_panel.rs` test module

**Interfaces:**
- Produces `TerminalPanel::open_or_activate_terminal(working_directory: PathBuf, window: &mut Window, cx: &mut Context<TerminalPanel>) -> Task<Result<WeakEntity<Terminal>>>`.
- Existing `TerminalPanel::open_terminal` and the global `workspace::OpenTerminal` action remain unchanged.

- [ ] **Step 1: Write the failing terminal reuse test**

Create a test workspace with a fake project and a terminal panel. Open a shell at `/repo`, call the new API again for `/repo`, and assert the terminal pane still contains one terminal and the existing terminal is active. Call it for `/other` and assert a second shell is created with `/other` as its working directory.

- [ ] **Step 2: Run the test to verify the API is missing**

Run:

```text
cargo test -p terminal_view terminal_panel -- --nocapture
```

Expected before the implementation: compilation fails because `open_or_activate_terminal` is not defined.

- [ ] **Step 3: Implement the dedicated reuse API**

Scan `TerminalPanel::center.panes()` for `TerminalView` items whose terminal `working_directory()` equals the requested path. For a match, set `active_pane`, activate the matching item with the existing `activate_terminal_view` helper, and return its terminal handle without spawning another shell. If no match exists, delegate to `add_terminal_shell(false, Some(working_directory), RevealStrategy::Always, window, cx)`.

Keep path matching scoped to the exact terminal working directory. Do not alter `OpenTerminal`, task terminals, Agent Panel terminals, or serialization behavior.

- [ ] **Step 4: Run the terminal tests**

Run:

```text
cargo test -p terminal_view terminal_panel -- --nocapture
```

Expected: the reuse and new-terminal cases pass.

- [ ] **Step 5: Commit the terminal API**

```text
git add crates/terminal_view/src/terminal_panel.rs
git commit -m "feat(terminal): reuse shells by working directory"
```

---

### Task 3: Route Herdr agent opens to an active workspace and regular terminal

**Files:**
- Modify: `crates/zed/src/zed/herdr_session_registry.rs:1574-1947,2696-2735`
- Modify: `crates/zed/src/zed/herdr_agent_sync.rs:142-463` only where effect comments/types describe the Zed Agent Panel seam
- Test: `crates/zed/src/zed/herdr_session_registry.rs` test module

**Interfaces:**
- Replaces the Agent Panel-specific `open_mirror_in_window` consumer with `open_terminal_in_window`.
- `add_workspace_root` becomes open-or-activate semantics while retaining its existing `WindowHandle<MultiWorkspace>` and `AsyncApp` inputs.
- `AgentSyncState` identity and reducer effects remain unchanged; `Open` and path-changing `PaneMoved` effects can request a regular terminal, while `Focus` and `Forget` do not touch Zed Agent Panel state.

- [ ] **Step 1: Write the failing launch-flow regression test**

Use the existing fake filesystem, test Herdr gateway, and test workspace setup to feed an agent Open effect with a checkout path. Assert the invoking window contains and activates that checkout workspace. Assert the regular terminal panel receives one shell terminal for that checkout path. Feed the same Open effect again and assert the terminal count does not grow. Assert no Agent Panel external-terminal entry is created and no command containing `agent attach` is spawned.

- [ ] **Step 2: Run the regression test before changing the consumer**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry -- --nocapture
```

Expected before the implementation: the existing consumer creates an Agent Panel external terminal using `herdr --session ... agent attach ...`, so the regular-terminal and no-Agent-Panel assertions fail.

- [ ] **Step 3: Implement active workspace routing**

In the workspace import helper, inspect all workspaces in the invoking window. If the canonical root already exists, call `MultiWorkspace::activate` on that workspace. If it does not exist, call `Workspace::new_local` with `OpenMode::Activate` rather than `OpenMode::Add`. Keep the existing root lock and canonical path validation around this operation.

- [ ] **Step 4: Replace Agent Panel thread creation**

Replace the `ensure_agent_panel_for_workspace` and `AgentPanel::open_external_terminal_thread` path in `open_mirror_in_window` with:

```rust
window.update(cx, |multi_workspace, window, cx| {
    let workspace = multi_workspace.workspace().clone();
    workspace.update(cx, |workspace, cx| {
        let Some(terminal_panel) = workspace.panel::<terminal_view::TerminalPanel>(cx) else {
            return Task::ready(Err(anyhow::anyhow!("terminal panel is unavailable")));
        };
        workspace.focus_panel::<terminal_view::TerminalPanel>(window, cx);
        terminal_panel.update(cx, |panel, cx| {
            panel.open_or_activate_terminal(checkout_path.clone(), window, cx)
        })
    })
})
```

Await the returned task through the existing generation guard and error reporting. Keep in-flight replay protection so repeated Herdr events do not spawn duplicate shells. Remove the Herdr-specific `agent attach` terminal specification from the call path. `Focus` and `Forget` effects must not open, close, or focus Zed Agent Panel threads; closing a Herdr session still only detaches the Herdr session host.

- [ ] **Step 5: Run launch-flow and Herdr tests**

Run:

```text
cargo test -p zed --bin zed herdr_session_registry -- --nocapture
cargo test -p zed --bin zed herdr_session_picker -- --nocapture
```

Expected: workspace import/activation, path-based terminal reuse, no-Agent-Panel behavior, session selection, and host attach tests pass.

- [ ] **Step 6: Commit the launch-flow change**

```text
git add crates/zed/src/zed/herdr_session_registry.rs crates/zed/src/zed/herdr_agent_sync.rs
git commit -m "fix(herdr): open regular terminals for agents"
```

---

### Task 4: Final verification and native smoke test

**Files:**
- Modify: none unless tests expose a contract break

- [ ] **Step 1: Run focused final tests**

Run:

```text
cargo test -p terminal_view terminal_panel -- --nocapture
cargo test -p zed --bin zed herdr_host -- --nocapture
cargo test -p zed --bin zed herdr_session_registry -- --nocapture
cargo test -p zed --bin zed herdr_session_picker -- --nocapture
```

Expected: all focused suites pass.

- [ ] **Step 2: Check formatting and compile Windows binaries**

Run:

```text
cargo fmt --all -- --check
cargo build -p zed -p cli -j 1
```

If the workspace formatter reports unrelated pre-existing drift, verify the changed files with `rustfmt --check --edition 2024 --config skip_children=true` and report the unrelated files without reformatting them.

- [ ] **Step 3: Launch and smoke-test the actual binary**

Launch:

```text
C:/Users/aigo90/projects/zed/target/debug/cli.exe --foreground --zed=C:/Users/aigo90/projects/zed/target/debug/zed.exe
```

Exercise: select/attach a Herdr session, create a Herdr space, navigate to a project path, launch an agent from Herdr, confirm Zed opens/activates the project and reuses or opens a normal shell terminal at that path, and confirm no Zed Agent Panel thread is created.

- [ ] **Step 4: Review status and commit any final test-only changes**

Run:

```text
git diff --check
git status --short --branch
```

Leave unrelated user changes untouched and report any native UI limitation explicitly.
