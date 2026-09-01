# HerdR Persistent Central View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans (preferred) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make HerdR a persistent, keymap-configurable central view that replaces only the editor content area while preserving the editor Entity, HerdR session, terminal, sidebars, and visibility state across worktree/thread switches and Zed restarts.

**Architecture:** Keep HerdR owned by `MultiWorkspace`, because that entity survives active Workspace, worktree, and thread changes. Add a persisted central-view visibility flag to `MultiWorkspace`; render either the existing Workspace view or the existing HerdR host in the central slot, without dropping either Entity. Add a `StatusItemView` button to each Workspace status bar that dispatches global HerdR actions, while all action-to-window mutations remain deferred through the existing safe lifecycle path.

**Tech Stack:** Rust, GPUI entities and actions, Zed `StatusItemView`, SQLite/KVP-backed `MultiWorkspaceState`, GPUI tests, macOS development-app smoke testing.

## Global Constraints

- Keep the left and right sidebars outside the HerdR/editor central-view switch.
- Do not close or recreate the editor Entity when HerdR is shown or hidden.
- Keep one HerdR session and terminal owned by `MultiWorkspace`; never claim a second endpoint during worktree/thread changes.
- Persist HerdR visibility with `MultiWorkspaceState`; default to hidden when older state has no field.
- When persisted visibility is true during startup, restore the runtime host before exposing HerdR as the visible central view; never render an empty central area.
- Use deferred window updates for global actions and host startup; never synchronously update a root window while it is temporarily leased.
- Do not start a real PTY or HerdR server in deterministic GPUI unit tests.
- Preserve the existing pending lifecycle-safety changes in `crates/zed/src/zed/herdr_host.rs`.

---

### Task 1: Add persistent central-view state

**Files:**
- Modify: `crates/workspace/src/persistence/model.rs:108-117`
- Modify: `crates/workspace/src/multi_workspace.rs:305-419,1454-1484,2006-2220`
- Modify: `crates/workspace/src/workspace.rs:9996-10053`
- Test: `crates/workspace/src/multi_workspace_tests.rs`

**Interfaces:**
- `MultiWorkspaceState` gains `pub herdr_visible: bool` with `#[serde(default)]`.
- `MultiWorkspace` gains `herdr_visible: bool`, initialized to `false`.
- Add `pub fn herdr_visible(&self) -> bool`.
- Add `pub fn set_herdr_visible(&mut self, visible: bool, cx: &mut Context<Self>)` that updates only on value changes, calls `cx.notify()`, and triggers the existing serialization path.
- `serialize_now` writes `herdr_visible`.
- `apply_restored_multiworkspace_state` applies the desired visibility after the MultiWorkspace window exists.

- [ ] **Step 1: Write failing state and layout tests**

Add tests in `crates/workspace/src/multi_workspace_tests.rs` that construct a `MultiWorkspace`, assert the default is hidden, set visibility through `set_herdr_visible`, draw the view, and assert the normal workspace selector is present when hidden and a dedicated HerdR central selector is present when visible. Add a second test that uses a cloned editor Entity and a pure test host Entity, toggles visibility twice, and asserts both Entity IDs remain unchanged.

Use the existing `init_test`, `MultiWorkspace::test_new`, `cx.draw`, `cx.run_until_parked`, and `cx.debug_bounds` helpers already used in this file. The test host must not spawn a terminal.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
cargo test -p workspace --lib multi_workspace_tests::test_herdr_central_view_visibility
cargo test -p workspace --lib multi_workspace_tests::test_herdr_visibility_preserves_entities
```

Expected: the tests fail because `MultiWorkspaceState` has no HerdR visibility field and the central renderer has no visibility switch.

- [ ] **Step 3: Implement the state and persistence fields**

Extend `MultiWorkspaceState` with a defaulted `herdr_visible` field. Initialize `MultiWorkspace::herdr_visible` to `false`; include it in `serialize_now`; and apply it in `apply_restored_multiworkspace_state` through `set_herdr_visible`.

Older serialized rows must deserialize successfully because the new field uses `#[serde(default)]`. If state restoration fails, leave the default hidden state unchanged.

- [ ] **Step 4: Implement the central mutually-exclusive renderer**

In `MultiWorkspace::render`, retain the existing left and right sidebar siblings. Replace the current unconditional central workspace/`window_root_host` children with a central container whose visible child is selected by the visibility flag and host availability:

```rust
.child(
    div()
        .id("herdr-central-content")
        .relative()
        .flex_1()
        .min_h_0()
        .w_full()
        .overflow_hidden()
        .when(self.herdr_visible && self.window_root_host.is_some(), |this| {
            this.children(self.window_root_host().cloned())
        })
        .when(!self.herdr_visible || self.window_root_host.is_none(), |this| {
            this.child(self.workspace().clone())
        }),
)
```

Keep the Workspace Entity in `MultiWorkspace`; conditional rendering changes only the visible element tree and must not reconstruct the Entity. The fallback to Workspace while a persisted host is being restored prevents a blank central view.

- [ ] **Step 5: Run the focused tests and verify success**

Run:

```bash
cargo test -p workspace --lib multi_workspace_tests::test_herdr_central_view_visibility
cargo test -p workspace --lib multi_workspace_tests::test_herdr_visibility_preserves_entities
```

Expected: both tests pass.

- [ ] **Step 6: Commit the state/layout slice**

```bash
git add crates/workspace/src/persistence/model.rs crates/workspace/src/multi_workspace.rs crates/workspace/src/workspace.rs crates/workspace/src/multi_workspace_tests.rs
git commit -m "feat(workspace): persist HerdR central view state"
```

---

### Task 2: Add bottom status-bar toggle and keymap actions

**Files:**
- Modify: `crates/zed_actions/src/lib.rs:1008-1034`
- Modify: `crates/zed/src/zed.rs:193-218,447-681`
- Modify: `crates/zed/src/zed/herdr_host.rs:101-127,965-1096`
- Test: `crates/zed/src/zed/herdr_host.rs` pure transition tests and `crates/workspace/src/multi_workspace_tests.rs`

**Interfaces:**
- Add `ToggleHerdR` with action name `Toggle HerdR`.
- Add `FocusHerdR` with action name `Focus HerdR`.
- Add `pub fn toggle_from_app(cx: &mut App)` and `pub fn focus_from_app(cx: &mut App)`.
- Add a `HerdRStatusButton` implementing `workspace::StatusItemView` and rendering an `IconButton` that dispatches `ToggleHerdR`.

- [ ] **Step 1: Write failing pure transition tests**

Add a pure transition helper and tests for these states:

```rust
assert_eq!(toggle_visibility(false, false), ShowAndFocus);
assert_eq!(toggle_visibility(true, false), FocusOnly);
assert_eq!(toggle_visibility(true, true), HideAndRestoreEditor);
```

The two boolean inputs represent HerdR visibility and whether HerdR currently owns focus. The helper must not access GPUI, a Window, a PTY, or a HerdR server.

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cargo test -p zed --bin zed herdr_toggle_visibility
cargo check -p zed_actions
```

Expected: the helper and new actions are absent before implementation.

- [ ] **Step 3: Add keymap-configurable actions**

In the existing `zed_actions::herdr` action list, add:

```rust
/// Toggles the HerdR central view and its focus.
#[action(name = "Toggle HerdR")]
ToggleHerdR,
/// Shows HerdR and moves focus to it.
#[action(name = "Focus HerdR")]
FocusHerdR,
```

Register both actions in `zed::init`. `ToggleHerdR` routes to `herdr_host::toggle_from_app`; `FocusHerdR` routes to `herdr_host::focus_from_app`. Do not remove the existing maximize, collapse, close, status, or new-window actions.

- [ ] **Step 4: Implement deferred toggle/focus behavior**

Refactor the app-level helpers in `herdr_host.rs` to use the existing deferred `cx.spawn` plus GPUI timer path. The transition must be:

- hidden and no host: create/install the host, set `herdr_visible = true`, then focus it after the window update;
- hidden and existing host: set visible and focus the existing host;
- visible with HerdR focus: set `herdr_visible = false` and restore focus to the Workspace;
- visible with another focus target: focus HerdR without hiding;
- `FocusHerdR`: always set visible and focus the existing host or create it if absent.

Change the existing in-view Close behavior to hide the central view instead of terminating and dropping the host. Keep session ownership, terminal Entity, snapshot, and connection tasks alive while hidden. Retain the current pending lifecycle changes to `install_host`, `open_current`, `open_new_window`, and `with_active_host`.

Because `Workspace::render` owns the status bar but the central switch replaces the whole Workspace view, render the active Workspace's existing status bar outside the mutually-exclusive central content selector when MultiWorkspace is used, and prevent duplicate status-bar rendering inside the nested Workspace. The HerdR toggle must remain available while HerdR is visible. Make the real HerdR host fill the selected central content area in its non-maximized state; preserve the existing Collapse and Maximize controls and avoid restoring a bottom-dock split.

Expose a pure transition helper for deterministic tests rather than testing real terminal startup.

- [ ] **Step 5: Add the status-bar icon**

Implement `HerdRStatusButton` following `crates/search/src/search_status_button.rs`:

- implement `Render` and `StatusItemView`;
- render `IconButton` with the existing `IconName::Terminal` icon and aria label `HerdR`;
- use `Tooltip::for_action` or `Tooltip::for_action_in` for `ToggleHerdR`;
- dispatch `ToggleHerdR` from `on_click`;
- keep the item visible in every Workspace so it can open HerdR even when no host exists;
- hold a weak `MultiWorkspace` handle, query visibility for selected/active styling, and observe MultiWorkspace notifications so the icon updates after toggles;
- return `None` from `hide_setting` unless a dedicated user setting is added for hiding this feature button.

Create and add this item in `initialize_workspace` using `workspace.status_bar().add_right_item(...)`. Every Workspace initialized through this path receives the button, while the host itself remains owned by MultiWorkspace.

- [ ] **Step 6: Run focused checks**

Run:

```bash
cargo test -p zed --bin zed herdr_toggle_visibility
cargo check -p zed_actions
cargo check -p zed --bin zed
```

Expected: all commands exit successfully.

- [ ] **Step 7: Commit the action/icon slice**

```bash
git add crates/zed_actions/src/lib.rs crates/zed/src/zed.rs crates/zed/src/zed/herdr_host.rs
git commit -m "feat(herdr): add persistent central view toggle"
```

---

### Task 3: Restore the runtime host and cover workspace/thread persistence

**Files:**
- Modify: `crates/zed/src/main.rs:1418-1465`
- Modify: `crates/zed/src/zed/herdr_host.rs:917-1096`
- Modify: `crates/workspace/src/multi_workspace_tests.rs`
- Modify: `crates/workspace/src/persistence.rs` only if state fixtures require an explicit field
- Modify: `crates/workspace/src/workspace.rs:9996-10053` only for restoration plumbing

**Interfaces:**
- Add `herdr_host::restore_if_visible(window_handle, cx)` for the post-restore runtime hook.
- Use `MultiWorkspace::set_herdr_visible`, `herdr_visible`, and `window_root_host` APIs.
- Do not invoke HerdR startup or a real PTY in deterministic tests.

- [ ] **Step 1: Add failing restoration and lifecycle tests**

Add tests that:

1. deserialize an older `MultiWorkspaceState` JSON object without `herdr_visible` and assert the default is false;
2. serialize a visible HerdR state, restore it into a new MultiWorkspace, and assert the desired visibility is true;
3. add multiple Workspaces, set HerdR visible, activate another Workspace/thread using existing helpers, and assert visibility remains true;
4. toggle hidden and visible twice and assert the backing editor Entity ID remains unchanged;
5. install a pure test host, hide it, show it again, and assert the host Entity ID remains unchanged.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
cargo test -p workspace --lib multi_workspace_tests::test_herdr_state_restores
cargo test -p workspace --lib multi_workspace_tests::test_herdr_state_survives_workspace_switch
```

Expected: failures identify the missing restoration or lifecycle assertion.

- [ ] **Step 3: Restore a visible host after startup**

After local `restore_multiworkspace` and remote `apply_restored_multiworkspace_state` complete in `crates/zed/src/main.rs`, call `herdr_host::restore_if_visible` with the restored `WindowHandle<MultiWorkspace>`. The hook must inspect the persisted desired visibility, choose the active Workspace as the backing workspace, and invoke the existing deferred `install_host` path. Set the visible flag only after the host is installed; until then, the renderer must use the normal Workspace fallback.

Do not serialize the live HerdR host, terminal, endpoint, or snapshot. On restart, restore the visibility preference and recreate the runtime host lazily through the normal client/terminal startup path.

- [ ] **Step 4: Run workspace and restoration regressions**

Run:

```bash
cargo test -p workspace --lib multi_workspace_tests::test_herdr_state_restores
cargo test -p workspace --lib multi_workspace_tests::test_herdr_state_survives_workspace_switch
cargo test -p workspace --lib test_window_root_host_is_laid_out_inside_window
cargo check -p zed --bin zed
```

Expected: all selected tests and the check pass.

- [ ] **Step 5: Commit the restoration slice**

```bash
git add crates/zed/src/main.rs crates/zed/src/zed/herdr_host.rs crates/workspace/src/multi_workspace_tests.rs crates/workspace/src/persistence.rs crates/workspace/src/workspace.rs
git commit -m "test(herdr): preserve central view across restoration"
```

---

### Task 4: Run macOS smoke verification and finish integration

**Files:**
- Modify: `crates/zed/src/zed/herdr_host.rs` only for smoke-discovered integration defects
- Modify: `crates/workspace/src/multi_workspace.rs` only for smoke-discovered layout defects
- Modify: `crates/zed/src/zed.rs` only for smoke-discovered status-bar registration defects

- [ ] **Step 1: Run the complete affected Rust checks**

Run:

```bash
cargo test -p workspace --lib
cargo test -p herdr
cargo check -p zed --bin zed
cargo build -p zed
```

Expected: selected tests pass and the Zed binary builds successfully. Existing warnings about unused fields, linker compact-unwind size, or future incompatibilities are recorded but do not block this feature unless they become errors.

- [ ] **Step 2: Launch the rebuilt macOS development app**

Start the rebuilt `target/debug/zed` with `ZED_STATELESS=1` and a temporary `--user-data-dir`, leaving the installed `/Applications/Zed.app` untouched. Keep the HerdR server running at the default socket for the smoke run.

- [ ] **Step 3: Exercise the user scenario**

Verify visually and interactively:

1. both sidebars remain visible;
2. the bottom status area contains the HerdR icon;
3. clicking the icon replaces only the central content with HerdR;
4. clicking again hides HerdR and restores the editor without reopening it;
5. `Toggle HerdR` and `Focus HerdR` appear in Command Palette and can be assigned in keymap settings;
6. the toggle action focuses HerdR when another central view has focus and hides/restores the editor when HerdR already has focus;
7. switching worktrees and threads leaves the HerdR session, terminal, and visible central view unchanged;
8. restarting the development app restores the prior visible/hidden state and recreates the runtime host if it was visible;
9. HerdR Close hides the central view without terminating the session;
10. no window-not-found error or panic appears in the Zed log.

- [ ] **Step 4: Fix only integration defects found by the smoke run**

For each defect, add or adjust the smallest behavioral regression test that proves it, apply one root-cause fix, and rerun the affected focused test before continuing. Do not change the split-layout non-goal or add per-worktree sessions.

- [ ] **Step 5: Commit and push the final integration changes**

```bash
git add crates/zed/src/zed/herdr_host.rs crates/workspace/src/multi_workspace.rs crates/workspace/src/multi_workspace_tests.rs crates/zed/src/zed.rs
git commit -m "fix(herdr): finalize persistent central view"
git push origin main
```

Verify:

```bash
git status --short --branch
git log -1 --oneline
git ls-remote --heads origin main
```

Expected: clean `main`, remote `origin/main` points at the final commit, and no PR or upstream branch is created.
