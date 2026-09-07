# herdr Persistent Central View

> **Superseded (2026-09-05):** herdr session selection, startup, and connection
> ownership are now specified in
> [herdr session selection design](2026-09-05-herdr-session-sync-design.md).
> Default-session ownership and automatic startup/retry described below are no
> longer how herdr works: windows bind to a session only after explicit picker
> selection or causal inheritance, endpoints come from `herdr session list
> --json`, and a lost connection never reconnects automatically. The central
> view layout, editor-entity preservation, and persistence history below remain
> valid.

## Goal

Make herdr a persistent tool view in Zed. When opened, herdr occupies the complete central content area while the left and right sidebars remain available. A dedicated bottom icon toggles the view.

## User-facing behavior

- The left and right sidebars remain unchanged.
- The bottom status area contains a dedicated herdr toggle icon.
- Activating the icon opens herdr in the complete central content area.
- Activating it again hides herdr and restores the editor/thread view.
- herdr is not a separate application window and does not cover the sidebars.
- Switching worktrees or threads does not close, hide, recreate, or reset herdr.
- The same herdr session, terminal, working directory, and interaction state remain active during those switches.
- Hiding herdr preserves the editor/thread state; showing herdr again restores the same herdr state.
- The last open/closed state is persisted and restored after restarting Zed.
- Existing herdr maximize, collapse, and close controls remain available. The in-view Close control performs the same transition as hiding herdr with the bottom toggle.

## Central view model

The central content area has two mutually exclusive visible modes:

1. Normal editor/workspace content.
2. herdr content.

Switching modes must not close or recreate the editor Entity. The editor remains owned by the workspace and retains its buffers, panes, thread state, selection, and scroll state. Only the visible central view changes.

The left and right sidebars remain outside this mode switch and keep their existing state and interactions.

## Ownership and lifecycle

`MultiWorkspace` owns herdr view state because the host must survive changes to the active workspace, worktree, and thread. The existing herdr session ownership guard remains the single-session authority for the default herdr endpoint.

The herdr host is created once per MultiWorkspace/session and reused while hidden or shown. Switching the active workspace or thread changes the normal editor content only; it does not replace the herdr host or claim another session.

Opening, hiding, and focusing herdr notify the owning `MultiWorkspace`. They must not synchronously update a window while the root window is temporarily leased; action handlers continue to use deferred window updates.

## State model

`MultiWorkspace` stores:

- whether the herdr central view is visible;
- the normal editor/workspace Entity and its existing state;
- the herdr host Entity and its existing session/terminal state;
- the persisted open/closed state used for restart restoration.

The visible mode is derived from the herdr visibility flag. Hiding herdr does not drop the host Entity, terminal, or session ownership.

## Toggle and focus actions

All entry points converge on the same state transition:

- bottom herdr icon;
- command palette herdr toggle action;
- keymap-configurable `Toggle herdr` action;
- keymap-configurable `Focus herdr` action;
- existing herdr open/close actions;
- in-view Close control.

`Toggle herdr` behaves as follows:

- herdr hidden: show herdr and focus it.
- herdr visible while editor is focused: focus herdr without changing visibility.
- herdr visible while herdr is focused: hide herdr and restore editor focus.

`Focus herdr` shows herdr if hidden and focuses it if already visible. The action must be discoverable in the command palette and bindable through the normal keymap configuration.

The bottom icon reflects visibility and focus using existing status-bar conventions. It does not create a second dock or consume central space while herdr is hidden.

## Rendering layout

The root MultiWorkspace layout keeps both sidebars as siblings around the central content column. The central column renders exactly one visible child: the normal workspace content or the herdr host. herdr must not be rendered as a child below the workspace content and must not be positioned between the sidebars as an additional column.

When herdr is hidden, the normal workspace content receives the full central area. When herdr is visible, the herdr host receives the same central bounds and the normal workspace content is not visible, while its Entity remains alive for restoration.

## Error handling

- If herdr cannot connect to its server, the central herdr view still opens and displays the existing connection/error status.
- Showing herdr again retries connection through the existing startup path without creating a second session owner.
- If the host entity or window disappears, actions log the update error and leave the visible state consistent; they must not panic.
- Switching worktrees or threads must not trigger a new herdr server claim or terminal creation.
- If persisted state cannot be read, Zed defaults to the normal editor view without losing editor state.

## Verification

Add behavioral coverage for:

- the bottom toggle showing herdr in the central bounds while preserving both sidebars;
- toggling again hiding herdr and restoring the normal editor view;
- hiding/showing without recreating the editor or herdr host Entities;
- worktree/thread changes preserving the same herdr host, terminal, and session;
- persisted open state restoring after MultiWorkspace recreation;
- `Toggle herdr` and `Focus herdr` being registered and keymap-configurable;
- all toggle entry points converging on the same state transition;
- no second herdr session being claimed when switching workspaces or reopening the view.

Use existing GPUI layout tests for central-content bounds and state-transition tests for lifecycle behavior. Do not start a real PTY or herdr server in deterministic unit tests; keep those dependencies behind the existing runtime path and validate them with a macOS smoke run.

## Non-goals

- No new herdr server protocol or endpoint behavior.
- No per-worktree herdr sessions.
- No editor/herdr split layout in this iteration.
- No independent herdr window as the default presentation.
- No changes to unrelated sidebar or terminal-panel behavior.
