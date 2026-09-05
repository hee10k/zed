# herdr Session Selection and Agent Synchronization

## Goal

Make herdr session ownership explicit and reliable on every platform, especially Windows, and synchronize selected herdr sessions with Zed workspaces and agent threads.

This design replaces automatic default-session selection, guessed socket paths, unbounded reconnects, and the existing single-endpoint ownership transfer between Zed windows.

## User-facing terminology

All user-visible references use the exact lowercase spelling `herdr`.

This includes:

- action names and command-palette entries;
- status text and central-view headers;
- picker titles and options;
- notifications and errors;
- settings descriptions and documentation.

Internal Rust crate, module, and type names may retain conventional Rust casing where required. User-visible variants such as `HerdR`, `herdR`, and `herd R` are removed.

## Root cause addressed

The current client derives a socket path from the user's home directory and assumes a Unix-style `.config/herdr` location. On Windows, herdr 0.8.2 reports session sockets under `%APPDATA%\herdr`. Zed therefore connects to a nonexistent path and displays the raw Windows “file not found” error while the view remains at `Starting`.

Zed must not construct a session socket path when the herdr CLI can report the authoritative path.

## Chosen architecture

### Application session registry

A Zed process owns one application-wide herdr session registry. The registry:

- discovers sessions with `herdr session list --json`;
- records each session's name, running state, session directory, and authoritative socket path;
- owns at most one API connection and event stream per herdr session;
- shares that connection across all Zed windows bound to the session;
- serializes snapshot and event handling so multiple windows cannot create duplicate mirrored threads;
- does not choose or connect to any session during registry installation.

The stable registry identity is the CLI-reported session identity, not a path synthesized by Zed. The CLI-reported `socket_path` is used verbatim for connection attempts after platform-native path conversion.

`HERDR_SESSION` and `HERDR_SOCKET_PATH` are not consulted by the Zed UI integration. Explicit low-level client configuration remains available for tests and non-UI callers, but a Zed window binding always comes from picker confirmation or causal inheritance.

### Window binding

Each Zed workspace window has a herdr binding in one of these states:

- `Unselected`;
- `Starting(session)`;
- `Connected(session)`;
- `Failed(session, cause)`.

A session may be shared by several windows when its agents belong to different worktrees. Each window remains associated with one selected session and one current worktree mapping. The application registry, rather than an individual window, owns the shared connection.

Windows opened because of a selected session or agent event inherit that known session. They do not show the startup picker again. This is causal inheritance from an explicit selection, not automatic session discovery.

## Session picker and commands

### Commands

The integration provides these command-palette and keymap-configurable actions:

- `herdr: Select or Create Session`;
- `herdr: Resync Agents`;
- `herdr: Disconnect Session`.

### Startup selection

An ordinary Zed workspace window that has no inherited herdr binding opens the session picker once during startup. The picker contains:

- all running sessions;
- all stopped sessions, visibly marked as stopped;
- `New Session`.

Cancelling leaves the window in `Unselected`. Zed remains fully usable, no herdr process is spawned, and no reconnect task runs. The status entry remains an explicit way to reopen the picker.

Selecting a running session connects to its CLI-reported endpoint. Selecting a stopped session starts or attaches it with structured process arguments equivalent to `herdr --session <name>`. It does not fall back to a default session.

Creating a session opens an editable name field prefilled with the current Zed workspace name. The user must confirm the name before Zed starts `herdr --session <name>`. CLI validation errors remain in the picker with actionable text and do not create a partial binding.

### Selection from an already connected window

When `herdr: Select or Create Session` is invoked from a window whose selected herdr session is still running:

1. the picker appears in the current window;
2. the current window and its existing connection remain unchanged while the picker is open;
3. after the user selects or creates a session, Zed always creates a separate workspace window for the selected result;
4. the new window receives the selected session binding and the selected session's focused worktree, when available.

This explicit operation does not reuse or focus an existing worktree window. Cancelling changes nothing. Zed does not silently replace a live window binding or run multiple selected sessions in one window.

If the selected session is the same session already used elsewhere, the application registry reuses its existing connection but still creates the requested new Zed window.

## Initial synchronization

The selected herdr session is authoritative during initial synchronization.

After connection, Zed fetches one coherent snapshot before processing later events. It reads the focused herdr space/workspace and all currently detected, running agents.

A confirmed selection has one target window:

- selection from an already connected window always uses the newly created window described above;
- selection from an unselected window may reuse that window or another existing window only when its worktree root exactly matches the normalized focused herdr worktree;
- if no exact worktree window exists, Zed opens a new window for the focused herdr worktree and leaves a mismatched invoking window unselected;
- if herdr has no focused worktree, the invoking unselected window or newly created target window binds to the session without guessing a path.

The target window receives the selected herdr session binding. All existing running agents in the initial snapshot are then synchronized immediately using the same deduplication and worktree-opening path as later agent events.

## Space and worktree behavior

Opening a herdr space, or changing a space path by itself, does not open or repurpose a Zed window. A space may change paths before an agent is launched, so a space-only event is not a stable opening trigger.

When an agent is detected, Zed resolves the worktree from the latest workspace checkout path associated with the agent. If that value is unavailable, it falls back to the agent's effective working directory. Zed then resolves the containing worktree root through its normal project/worktree APIs.

For a valid resolved worktree:

- reuse and focus an existing Zed window with the exact normalized worktree root;
- otherwise open a new Zed window for the worktree;
- bind the window to the originating herdr session;
- open or focus the mirrored terminal thread for the agent.

Windows path comparison accounts for drive-letter casing, case-insensitive components, and separator differences. Zed does not compare Windows worktrees using raw display strings.

If the path does not exist, is not a directory, or cannot be opened as a worktree, Zed does not create an empty window. It emits one actionable notification for that agent revision and continues synchronizing other agents.

## Agent thread representation

A herdr agent is an already running process in a herdr pane. Zed mirrors it as a terminal thread in the Agent Panel, attached to that exact live pane.

The terminal is launched from a structured executable-and-arguments specification equivalent to:

```text
herdr --session <session-name> agent attach <pane-id>
```

The implementation must not build a shell command string from a session name or pane ID. Structured arguments avoid Windows quoting errors and command injection.

The Agent Panel receives a focused API for opening a terminal thread with:

- executable and arguments;
- display title;
- working directory;
- stable external identity.

The stable mirrored-thread key is `(herdr session identity, pane_id)`. A pane update or repeated snapshot cannot create a second thread for the same key. Pane revisions merge snapshot and event data and reject stale updates.

ACP session import or resume is not used. It is not supported consistently by all detected agents and could create a second frontend or process for a session that is already live in herdr.

## Focus synchronization

For mapped spaces and agents, focus is bidirectional:

- focusing a herdr space focuses its mapped Zed worktree window when one exists;
- focusing a mapped Zed worktree window asks herdr to focus the corresponding space;
- focusing a herdr agent pane focuses its Zed window and mirrored terminal thread;
- focusing a mirrored Zed terminal thread sends `agent.focus` for its herdr pane.

Every focus transition records its origin and the latest `(session, workspace_id or pane_id, revision)`. The reflected event acknowledges the transition instead of initiating another one. This prevents Zed↔herdr feedback loops, repeated window activation, and focus flicker.

Unmapped space focus does not open a Zed window. Agent detection remains the trigger that opens a worktree and thread.

## Closing and resynchronizing mirrored threads

Closing a mirrored terminal thread in Zed detaches only the Zed terminal client. It does not stop the herdr agent or close its pane.

The registry records that `(session, pane_id)` was dismissed. Further updates for the same live pane do not reopen it automatically. The dismissal entry is removed when the pane exits.

`herdr: Resync Agents` clears dismissal and retry suppression for the selected session, fetches a fresh snapshot, and reopens all currently running agents using the normal deduplicated synchronization path.

## Disconnect and process exit

`herdr: Disconnect Session` removes the invoking window's binding and leaves the window, worktree, and editor state intact. It detaches mirrored terminal clients owned by that binding without terminating herdr agents. A shared session connection remains alive while another Zed window is bound to it; otherwise the registry closes its client connection without asking the herdr session to terminate.

If a selected herdr session exits while Zed is running:

- all affected window bindings transition to `Unselected`;
- automatic reconnect stops;
- mirrored terminal threads reach their normal exited state and retain their final output;
- Zed windows, worktrees, editors, and unrelated threads remain open;
- the user may invoke `herdr: Select or Create Session` to select or create a session again.

A normal session exit is not presented as an application error.

## Connection and error handling

### Starting and connecting

For a selected running, stopped, or newly created session, Zed waits for the CLI-reported endpoint for at most 15 seconds. Connection attempts stop immediately when the launched process exits or the user cancels/disconnects.

A successful connection must complete the initial snapshot before the binding becomes `Connected`.

There is no unbounded one-second reconnect loop. After an unexpected socket loss, Zed refreshes the session list once:

- if the session is no longer running, transition to `Unselected` as a normal exit;
- if the session is still reported running, transition to `Failed` and offer explicit retry or session selection.

### User-visible errors

The central view and status area show concise lowercase herdr states:

- `herdr: Select a session`;
- `herdr: Starting <session>…`;
- `herdr: Connected to <session>`;
- `herdr: Could not connect to <session>`.

Raw socket paths, process command lines, and operating-system error text are logged as diagnostics, not placed in the persistent window title. Failure UI offers `Retry` and `Choose Session` actions where applicable.

Session-list failure leaves the window unselected and offers `Retry`. Zed does not synthesize a default session as a fallback.

An individual worktree-open or agent-attach failure is isolated to that agent. Other agents and windows continue synchronizing. Repeated events with the same pane revision do not repeat the same notification.

## Compatibility with the persistent central view

The persistent central-view behavior remains: showing and hiding herdr must not destroy normal editor state. This design supersedes the default-session ownership, automatic retry, and user-visible `HerdR` naming described in `2026-09-02-herdr-persistent-central-view-design.md`.

A central herdr terminal is created only after explicit session selection or inherited binding. An unselected window may still show the central herdr surface, but it shows the session-selection state rather than starting a default process.

## Verification

### Deterministic behavioral coverage

Add focused coverage for observable state and synchronization behavior:

- parse running and stopped session-list entries and preserve their reported socket paths;
- use an `%APPDATA%\herdr` socket fixture on Windows without deriving `.config\herdr`;
- leave cancellation in `Unselected` with no process or reconnect task;
- start a stopped or new named session only after explicit confirmation;
- reuse one registry connection for the same session across multiple windows;
- keep a live current binding unchanged when selection opens another session in a new window;
- use herdr's focused worktree as the initial source of truth;
- synchronize every existing running agent from the initial snapshot;
- open one mirrored terminal thread for a newly detected agent;
- deduplicate repeated snapshots and pane revisions;
- during agent synchronization, reuse a window by normalized worktree identity and create one only when needed;
- synchronize worktree and agent focus in both directions without feedback loops;
- suppress automatic reopening after a user closes a mirrored thread;
- clear suppression and retry current agents through `Resync Agents`;
- isolate invalid worktree and attach failures from other agents;
- transition normal herdr exit to `Unselected` without closing Zed state;
- transition a lost connection for a still-running session to `Failed` without automatic retry.

Tests assert behavior and state transitions, not exact user-facing prose or internal field forwarding.

### Windows smoke scenario

On Windows with the installed herdr CLI:

1. verify session discovery returns and uses `%APPDATA%\herdr` endpoints;
2. cancel startup selection and confirm Zed remains usable and idle;
3. select one running session, one stopped session, and create one confirmed named session;
4. launch agents in two worktrees and confirm Zed reuses or opens the correct windows;
5. confirm each Agent Panel terminal thread is attached to the original live herdr pane;
6. focus agents and worktrees from both herdr and Zed and confirm one stable transition per action;
7. close one mirrored thread, confirm it stays closed while the agent runs, then restore it with `Resync Agents`;
8. invoke session selection from a connected window and confirm the selected result opens in a separate Zed window;
9. terminate herdr and confirm Zed remains usable, existing output remains visible, and no reconnect loop starts;
10. scan the actual UI for remaining `HerdR`, `herdR`, or `herd R` labels.

## Non-goals

- Zed does not create or close a herdr space solely because a Zed workspace opens or closes.
- Zed does not terminate a herdr agent when a mirrored terminal thread closes.
- Zed does not import herdr agents into native ACP conversation threads.
- Zed does not infer session selection from a default name, socket convention, workspace path, or environment variable.
- Zed does not synchronize more than one selected herdr session inside a single workspace window.
- This change does not redesign unrelated Agent Panel, terminal, sidebar, or editor behavior.
