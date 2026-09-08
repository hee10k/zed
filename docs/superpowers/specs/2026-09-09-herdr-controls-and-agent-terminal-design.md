# Herdr Controls and Agent Terminal Design

## Goal

Make the Herdr controls usable and change Herdr agent integration so Zed opens the agent project and a regular shell terminal, while Herdr remains the only agent-management surface.

## Preserved behavior

Selecting or creating a Herdr session from Zed remains unchanged. The registry still validates the session, attaches the `herdr --session <name>` host terminal, tracks the binding, and reports connection state. Herdr session host visibility remains controlled by the Zed toggle.

## Controls

`HerdRStatusButton` remains an icon-only status item. Its accessible label and tooltip retain the binding state, but the visible text label is removed.

All app-level Herdr window mutations use a deferred update. GPUI dispatches actions while the current window is temporarily leased; synchronously updating that same `WindowHandle` fails with `window not found` and is currently discarded. The helper captures the active `WindowHandle<MultiWorkspace>` before deferring and performs the root update after the callback returns.

The toggle contract is:

- no selected session: open the session picker;
- selected and hidden: show Herdr and focus its host;
- selected and visible: hide Herdr and focus the editor.

`FocusHerdr` remains the focus-only action. The header Close control dispatches `ToggleHerdr`, so it has exactly the same behavior as the status-bar toggle. The header Maximize/Restore capability is removed completely. Collapse/Expand remains because it changes the host header-only layout without competing with visibility.

## Agent launch flow

Herdr workspace and agent events continue to use the existing session identity, generation guards, checkout canonicalization, and same-window workspace routing.

When an agent Open effect is received:

1. Resolve `checkout_path`, falling back to `effective_cwd`.
2. Canonicalize and validate the path.
3. Find the invoking Zed window for the session.
4. If the checkout is not already present in that window, add it with `OpenMode::Activate`; if it is present, activate the matching workspace.
5. In the active workspace, open or activate a regular shell terminal whose working directory is the checkout path.
6. Do not create an Agent Panel external terminal thread and do not run `herdr agent attach` inside Zed.

Existing regular terminal reuse is path-based. A terminal with the same working directory is activated; otherwise a new shell terminal is created. The existing global `OpenTerminal` action keeps its current always-open-a-new-terminal semantics; the reuse behavior is a dedicated TerminalPanel API used only by the Herdr integration.

Subsequent Herdr agent focus and pane-move events do not create or focus Zed Agent Panel threads. Herdr owns those agent transitions. Session close/disconnect still detaches the Herdr session host and clears registry binding state; it does not stop Herdr agents.

## Boundaries and failure handling

The session registry remains responsible for Herdr connection state and workspace path resolution. `TerminalPanel` owns inspection, activation, and creation of regular Zed terminals. The registry does not duplicate terminal entities or terminal-pane state.

If workspace import, terminal inspection, or terminal creation fails, the registry records the existing Herdr failure notification/log path and leaves Zed running. A failed terminal operation must not create an Agent Panel thread as a fallback.

## Verification

Add focused GPUI coverage for:

- the status button rendering without visible state text;
- deferred toggle/close mutations changing `herdr_visible` and editor/Herdr focus;
- removal of the maximize control from the host surface;
- opening a new checkout workspace on an agent Open effect;
- reusing a regular terminal for a matching working directory;
- no Agent Panel external-thread creation for Herdr agent events;
- preserved Herdr session selection and `herdr --session` host attachment.

Run the focused Herdr host, registry, terminal-panel, and launch-flow tests, then build and launch the Windows Zed/CLI binaries for a native smoke test.
