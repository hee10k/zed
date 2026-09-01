# HerdR Persistent Central View

## Goal

Make HerdR a persistent tool view in Zed. When opened, HerdR occupies the complete central content area while the left and right sidebars remain available. A dedicated bottom icon toggles the view.

## User-facing behavior

- The left and right sidebars remain unchanged.
- The bottom status area contains a dedicated HerdR toggle icon.
- Activating the icon opens HerdR in the complete central content area.
- Activating it again hides HerdR and restores the editor/thread view.
- HerdR is not a separate application window and does not cover the sidebars.
- Switching worktrees or threads does not close, hide, recreate, or reset HerdR.
- The same HerdR session, terminal, working directory, and interaction state remain active during those switches.
- Hiding HerdR preserves the editor/thread state; showing HerdR again restores the same HerdR state.
- The last open/closed state is persisted and restored after restarting Zed.
- Existing HerdR maximize, collapse, and close controls remain available. The in-view Close control performs the same transition as hiding HerdR with the bottom toggle.

## Central view model

The central content area has two mutually exclusive visible modes:

1. Normal editor/workspace content.
2. HerdR content.

Switching modes must not close or recreate the editor Entity. The editor remains owned by the workspace and retains its buffers, panes, thread state, selection, and scroll state. Only the visible central view changes.

The left and right sidebars remain outside this mode switch and keep their existing state and interactions.

## Ownership and lifecycle

`MultiWorkspace` owns HerdR view state because the host must survive changes to the active workspace, worktree, and thread. The existing HerdR session ownership guard remains the single-session authority for the default HerdR endpoint.

The HerdR host is created once per MultiWorkspace/session and reused while hidden or shown. Switching the active workspace or thread changes the normal editor content only; it does not replace the HerdR host or claim another session.

Opening, hiding, and focusing HerdR notify the owning `MultiWorkspace`. They must not synchronously update a window while the root window is temporarily leased; action handlers continue to use deferred window updates.

## State model

`MultiWorkspace` stores:

- whether the HerdR central view is visible;
- the normal editor/workspace Entity and its existing state;
- the HerdR host Entity and its existing session/terminal state;
- the persisted open/closed state used for restart restoration.

The visible mode is derived from the HerdR visibility flag. Hiding HerdR does not drop the host Entity, terminal, or session ownership.

## Toggle and focus actions

All entry points converge on the same state transition:

- bottom HerdR icon;
- command palette HerdR toggle action;
- keymap-configurable `Toggle HerdR` action;
- keymap-configurable `Focus HerdR` action;
- existing HerdR open/close actions;
- in-view Close control.

`Toggle HerdR` behaves as follows:

- HerdR hidden: show HerdR and focus it.
- HerdR visible while editor is focused: focus HerdR without changing visibility.
- HerdR visible while HerdR is focused: hide HerdR and restore editor focus.

`Focus HerdR` shows HerdR if hidden and focuses it if already visible. The action must be discoverable in the command palette and bindable through the normal keymap configuration.

The bottom icon reflects visibility and focus using existing status-bar conventions. It does not create a second dock or consume central space while HerdR is hidden.

## Rendering layout

The root MultiWorkspace layout keeps both sidebars as siblings around the central content column. The central column renders exactly one visible child: the normal workspace content or the HerdR host. HerdR must not be rendered as a child below the workspace content and must not be positioned between the sidebars as an additional column.

When HerdR is hidden, the normal workspace content receives the full central area. When HerdR is visible, the HerdR host receives the same central bounds and the normal workspace content is not visible, while its Entity remains alive for restoration.

## Error handling

- If HerdR cannot connect to its server, the central HerdR view still opens and displays the existing connection/error status.
- Showing HerdR again retries connection through the existing startup path without creating a second session owner.
- If the host entity or window disappears, actions log the update error and leave the visible state consistent; they must not panic.
- Switching worktrees or threads must not trigger a new HerdR server claim or terminal creation.
- If persisted state cannot be read, Zed defaults to the normal editor view without losing editor state.

## Verification

Add behavioral coverage for:

- the bottom toggle showing HerdR in the central bounds while preserving both sidebars;
- toggling again hiding HerdR and restoring the normal editor view;
- hiding/showing without recreating the editor or HerdR host Entities;
- worktree/thread changes preserving the same HerdR host, terminal, and session;
- persisted open state restoring after MultiWorkspace recreation;
- `Toggle HerdR` and `Focus HerdR` being registered and keymap-configurable;
- all toggle entry points converging on the same state transition;
- no second HerdR session being claimed when switching workspaces or reopening the view.

Use existing GPUI layout tests for central-content bounds and state-transition tests for lifecycle behavior. Do not start a real PTY or HerdR server in deterministic unit tests; keep those dependencies behind the existing runtime path and validate them with a macOS smoke run.

## Non-goals

- No new HerdR server protocol or endpoint behavior.
- No per-worktree HerdR sessions.
- No editor/HerdR split layout in this iteration.
- No independent HerdR window as the default presentation.
- No changes to unrelated sidebar or terminal-panel behavior.
