# herdr Same-Window Session Import

## Status

Approved behavior change for the herdr session-sync implementation. This document supersedes the prior rule that selecting a session from a bound Zed window creates a new top-level Zed window.

## Problem

Selecting a herdr session currently routes a bound window through `SelectionTarget::NewWindow`. The registry creates a bootstrap Zed window for the selection. During the initial snapshot, only the focused herdr worktree is placed in that window. Agents whose checkout roots are not already present then use the mirror router, which opens another Zed window for each unmatched root. A session with multiple spaces or agents can therefore create multiple top-level Zed windows.

The intended behavior is one existing Zed window containing one Zed project/workspace entry per herdr space. Agent mirrors must remain in that same top-level window.

## Goals

- Keep the Zed window that opened the session picker.
- Add every valid herdr space checkout from the initial snapshot to that window's `MultiWorkspace`.
- Avoid creating any top-level Zed window from herdr session selection or agent synchronization.
- Route mirrored agent terminals into the project containing their current checkout root.
- Keep existing session sharing, stable agent identity, focus synchronization, and per-agent error isolation.
- Preserve cancellation semantics: cancelling the picker does not alter the current window or binding.

## Non-goals

- No change to herdr protocol types or endpoint discovery.
- No change to the number of herdr sessions a Zed window may own: one binding per window remains the invariant.
- No ACP import/resume or agent lifecycle propagation.
- No redesign of unrelated Zed project or window behavior.

## Behavior

### Selection

A confirmed selection always targets the invoking `MultiWorkspace` window. The selection target is no longer `NewWindow` for a window that is already `Starting`, `Connected`, or `Failed`.

If a connected window selects another session, the current binding is detached only after confirmation and the selected session replaces it in the same window. Cancelling leaves the current binding unchanged. A failed or stopped selection likewise never creates a replacement top-level window.

The registry no longer uses the herdr-specific bootstrap-window and inherited-handoff route. Normal Zed windows opened for unrelated actions remain unaffected.

### Initial session import

After the session connection has obtained a coherent snapshot:

1. Read `snapshot.workspaces` in snapshot order.
2. Keep entries with a checkout path, canonicalize them with the existing path helper, and remove duplicates.
3. Compare each canonical root with the live roots already held by the invoking `MultiWorkspace`.
4. Add missing roots one at a time with `Workspace::new_local(..., OpenMode::Add)` and the invoking window handle.
5. Leave the top-level window and its active project unchanged unless normal Zed project loading requires a focus update.
6. Import the snapshot agents only after workspace additions are complete.

Existing projects are reused rather than duplicated. A failed path addition is isolated to that path and does not discard the session connection or valid additions.

### Agent routing

An agent resolves its checkout root using the existing checkout discovery and canonicalization logic. Exact and ancestor root matches continue to reuse projects already held by the invoking window.

When no matching root exists, the registry adds the agent root to the invoking window with `OpenMode::Add` rather than calling the top-level-window mirror router. Concurrent agents for the same root share the existing in-flight and root deduplication gates. The mirrored terminal is opened in the same `MultiWorkspace` after the root is available.

The agent identity remains `(session identity, terminal_id)`; pane moves still update the existing terminal instead of creating a second one.

## Error handling

- Session list, connection, subscription, and snapshot failures retain the existing `Failed` binding behavior.
- Invalid or unavailable space/agent roots produce the existing actionable mirror/worktree notification and do not prevent other roots from being imported.
- No retry loop or fallback top-level window is introduced.
- A normal herdr session exit still leaves the Zed window and its imported projects intact and returns its binding to `Unselected`.

## Implementation seam

Primary source and test changes are confined to `crates/zed/src/zed/herdr_session_registry.rs`:

- make confirmed selection use the invoking target;
- replace focused-worktree-only finalization with a same-window snapshot import task;
- remove or bypass the herdr-specific new-window handoff path;
- add a same-window root-add helper used by initial space import and unmatched agent fallback;
- keep the existing `MultiWorkspace` and `Workspace::new_local` APIs as the project insertion boundary.

`crates/herdr/src/herdr.rs` remains unchanged because the snapshot already exposes the required workspace checkout paths.

## Verification

### Deterministic coverage

Add behavior-focused tests at the registry seam:

- confirmed selection from an unselected and connected window both target the invoking window;
- snapshot checkout roots are canonicalized, deduplicated, and imported in order;
- a session with multiple spaces remains in one registered Zed window while its `MultiWorkspace` gains one project per root;
- repeated roots and agents sharing a root do not duplicate projects;
- an unmatched agent root is added to the invoking window, never routed to a new top-level window;
- invalid roots do not prevent valid roots and agent mirrors from completing.

Assertions inspect window count, `MultiWorkspace::workspaces()`, canonical roots, binding state, and mirrored terminal identity. They do not assert implementation-only field forwarding or user-facing prose.

### Windows smoke scenario

With the installed herdr CLI on Windows:

1. Start Zed with one ordinary window.
2. Select a running herdr session containing multiple spaces and agents.
3. Confirm no additional top-level Zed window appears.
4. Confirm the current window contains one project/workspace entry per valid herdr space.
5. Confirm agents in those spaces open mirrored terminals in the current window.
6. Repeat selection from the connected window and confirm the selected session still replaces the binding in that same window.
7. Cancel selection and confirm the current binding remains unchanged.

## Compatibility note

The existing session-sync design remains authoritative for endpoint discovery, shared connections, stable agent identity, focus echo suppression, dismissal, and normal session exit. This document changes only the top-level Zed window and initial project-import policy described above.
