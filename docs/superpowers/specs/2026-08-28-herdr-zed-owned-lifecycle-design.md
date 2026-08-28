# Zed-Owned Herdr Lifecycle and Session Binding

## Status

Revised design, pending user review.

## Problem

The current integration binds a default Herdr bridge when an `AgentPanel` is
constructed. That makes a Zed window probe and retry against Herdr even when the
user never opened Herdr from Zed. A Herdr process started in another terminal
can therefore appear eligible for synchronization, and a missing endpoint can
produce connection activity unrelated to the user's current Zed workspace.

The intended ownership rule is narrower:

- Herdr synchronization begins only when Herdr is launched from inside Zed.
- A Herdr process launched in another terminal is independent and is not
  synchronized with Zed.
- Zed and Herdr must reconnect to the same relationship after a later restart.
- Herdr remains running when its owning Zed window closes; Zed detaches and can
  reconnect after the user starts Herdr again inside Zed.

## Goals

1. Bind one Zed window to one Zed-owned, named Herdr session.
2. Support both explicit Zed launch and direct `herdr` execution in a Zed
   terminal.
3. Keep external Herdr processes out of the bridge unless the user explicitly
   starts the named session from the Zed window.
4. Persist the named session and root-thread mappings across Zed restarts.
5. Reconnect quietly when the owned Herdr endpoint appears after Zed starts.
6. Keep Herdr workspace-to-Zed workspace routing deterministic and reject
   ambiguous ownership instead of selecting the first path match.
7. Preserve existing Herdr-authoritative agent state, prompt, output, focus,
   and close behavior.

## Non-goals

- Connecting Zed to arbitrary Herdr processes by discovering a socket or pipe.
- Automatically launching Herdr when Zed starts.
- Sharing one Herdr session across multiple Zed windows.
- Killing or closing Herdr when the owning Zed window closes.
- Mapping ordinary shell or non-agent panes to Zed subthreads.
- Replacing Herdr's local Unix socket or Windows named-pipe transport.

## Ownership model

A `MultiWorkspace` window is the ownership boundary:

```text
Zed window
  <-> one Zed-owned named Herdr session
```

The bridge is lazy. Constructing an `AgentPanel` does not create a Herdr
client, start synchronization, or retry an endpoint. A bridge is created only
when one of the following ownership triggers occurs:

1. The user invokes the Zed `Open Herdr` action.
2. Zed observes a direct server-launch executable named `herdr` in any local
   terminal belonging to that window.

Remote terminals cannot trigger ownership. Running Herdr in a local Zed
terminal is an ownership trigger; running Herdr in a separate terminal outside
Zed is not.

The bridge uses `HerdrSessionSelection::Named` with the persisted session name.
It never uses the ambient default session for an owned window.

The named session is generated once per Zed window as a UUID-based value with a
stable prefix such as `zed-<uuid>`. The runtime `WindowId` is not used as the
session name because it can change across restarts.

## Persistent state

Extend the existing per-window `MultiWorkspaceState` with optional Herdr
ownership state:

- `herdr_session_name: Option<String>` — the stable named session reserved for
  this Zed window.
- `herdr_owned: bool` — whether this window has previously launched or accepted
  a Herdr process through an ownership trigger.

The fields use serde defaults so older workspace-state records remain readable.
The existing workspace restoration flow carries the serialized state from the
previous runtime window key to the newly created window; serialization then
writes the restored Herdr state under the new runtime key.

A new window reserves its name before any Herdr launch so Zed-created terminal
shells can receive the correct `HERDR_SESSION` environment value. Reservation
alone does not set `herdr_owned` and does not activate a bridge.

The live owner process is runtime state, not persisted state. The bridge must
track the terminal and process that satisfied the current launch trigger. A
bridge may retry while that process is still alive or while its initial launch
is pending; observing that Herdr process exit returns the bridge to `Dormant`
and stops retries. A Herdr server restarted outside Zed cannot reactivate the
bridge.

The persisted `herdr_owned` bit records prior ownership history only. The
runtime bridge also keeps an owner terminal identifier and process identity.
Those runtime values are cleared when the process exits or the window closes;
they are never reconstructed from endpoint availability alone.

Session reservation is flushed before the launch action returns and during
normal Zed window shutdown. A crash may lose an unflushed reservation, but it
must not overwrite an already persisted name or root mapping.

On Zed restart:

1. Restore the session name, ownership bit, and Herdr root mappings.
2. Keep the bridge inactive until a user launches Herdr from inside that Zed
   window.
3. Do not attach merely because a matching named session or pipe already
   exists.
4. After the in-window launch trigger, reconnect to the restored named session
   and reuse existing root `ThreadId` values.

## Launch triggers

### Explicit `Open Herdr` action

The action runs Herdr in a Zed-owned terminal with:

```text
herdr --session <persisted-session-name>
```

The terminal environment also contains:

```text
HERDR_SESSION=<persisted-session-name>
```

The action is available only for a local project/workspace with a usable
working directory. It creates a visible local terminal using the current
project's terminal construction path, overlays `HERDR_SESSION` without
overwriting unrelated user environment entries, and passes the session as
structured process arguments rather than interpolating it into shell text.
Failure to create the terminal clears the pending ownership claim and does not
start a bridge.

The action records ownership before waiting for the endpoint. The bridge may
remain `Unavailable` while Herdr starts, then retries until the named endpoint
accepts a bootstrap.

### Direct local-terminal execution

Zed observes all local terminals in the window, including the standard
workspace `TerminalPanel` and terminals managed by the `AgentPanel`. It
recognizes a server-launch invocation whose normalized executable basename is
exactly `herdr` (or `herdr.exe` on Windows). Accepted forms are a bare `herdr`
using the injected `HERDR_SESSION`, or a supported launch form whose explicit
session equals the reserved name. The `herdr server` form is also eligible
when its session is provided through the reserved environment.

Remote terminals and terminals in other Zed windows cannot trigger ownership.

CLI client commands are not ownership triggers. In particular, `status`,
`workspace`, `tab`, `pane`, `agent`, `notification`, `session`, `api`,
`update`, `completion`, and unknown subcommands are excluded. Zed must inspect
the foreground process arguments, not only the normalized executable name.

Zed does not infer ownership from a pipe, cwd, output text, agent name, shell
alias, wrapper, or unrelated external process.

Zed-created local terminal shells receive the window's `HERDR_SESSION` value,
so a bare `herdr` command launches the reserved named session. If process
arguments contain an explicit `--session` value, that value must equal the
reserved name; otherwise the process is not bound to this Zed window.

A shell alias or wrapper is outside the direct-executable contract. Users can
use the explicit Zed action or invoke `herdr --session <reserved-name>` when
an existing local terminal was created without the Zed session environment.

A process that exits before establishing a connection clears the pending
launch claim and leaves the bridge dormant.

An external terminal process cannot trigger ownership because it is not
observed by the Zed terminal process monitor. An existing socket or pipe is
never sufficient on its own.


### Process-observation seam

The ownership observer is window-scoped but terminal-surface agnostic. The
existing terminal process monitor must publish the normalized executable name,
full argv, local/remote flag, terminal identity, and process identity whenever
the foreground process changes, and publish process exit for the tracked
terminal. The current command-name-only accessor and name/cwd-only change event
are insufficient for session-argument validation and owner-process teardown.

Both the standard workspace `TerminalPanel` and `AgentPanel` terminals feed
this observer. The observer forwards only local terminals to the
`HerdrBridgeRegistry`; the registry applies the exact launch-command and
session checks before creating a bridge. No Herdr-specific logic is placed in
the standard terminal's UI.

The implementation seam is the existing terminal process-information path:
`PtyProcessInfo` must expose a safe snapshot containing normalized executable
name, argv, local/remote status, terminal identity, and a stable process
identity. The terminal emits a foreground-process-changed event whenever any
of those ownership-relevant values changes, not only when title or cwd changes.
The existing `TerminalBuilder::new` environment map receives the
window-scoped `HERDR_SESSION` overlay for local terminals.
## Bridge lifecycle

### Start

An ownership trigger creates or obtains the window's bridge, binds it to the
persisted named session, and starts synchronization. Bootstrap performs the
existing `ping`, subscription, snapshot, mapping reconciliation, and replay
sequence.

A missing endpoint after an ownership trigger is a supported startup state:

- retry with bounded exponential backoff;
- expose `Unavailable`/`Reconnecting` status;
- do not show a Toast for automatic bootstrap failures;
- retain one-time Toasts for explicit user actions such as focus, rename,
  create, or close when those actions fail.

### Herdr remains alive after Zed closes

Closing the Zed window releases the bridge and stops Zed-side event
subscriptions. It does not send `workspace.close`, stop the Herdr session, or
kill the Herdr process. Durable named-session and root-thread mappings remain.

The next Zed run restores the state but waits for the user to launch Herdr from
inside Zed before reconnecting. A Herdr process that was started only from
another terminal remains independent during this dormant period.

### Herdr disconnects or exits

A broken pipe/socket or server exit changes bridge status to `Unavailable` or
`Reconnecting`, preserves known roots/subthreads, and keeps retrying only while
the Zed-owned Herdr process is still alive or its initial in-Zed launch is
still pending. Once the owner process exits, the bridge stops retrying and
returns to `Dormant`; a later launch from another terminal does not reconnect
it.

The retry loop is gated by the runtime owner process. A connection loss while
the owner process is alive may retry automatically. If the owner process exits,
the bridge cancels subscriptions, clears its runtime owner, and becomes
`Dormant`. A Herdr background server that remains alive after its Zed client
exits is not enough to reactivate that bridge; a new in-Zed launch trigger is
required.

The bridge must not create repeated user notifications for automatic attempts.

When Herdr later accepts the named endpoint after an in-window launch, the next
bootstrap restores the persisted mapping and returns to `Ready`.

When `Open Herdr` starts a process, the action records the terminal and process
identity before the first endpoint attempt. If process creation fails or the
process exits before a successful bootstrap, the pending owner record is
cleared and the bridge returns to `Dormant`. A second launch in the same window
cannot claim the session while a different owner process is still active; it
must either reuse the existing owner or report a deterministic ownership
conflict.

## Deterministic workspace and thread mapping

The canonical durable mapping remains:

```text
Herdr session + Herdr workspace_id
  <-> Zed root ThreadId
```

`HerdrMappingRecord` additionally stores the owning Zed `WorkspaceId` when the
root is associated from a Zed launch context. The field is optional for
backward compatibility with existing records.

Routing rules:

1. An exact persisted `WorkspaceId` match wins when that Zed workspace is still
   present in the window.
2. A new root created by `Open Herdr` is associated with the current active Zed
   workspace before the Herdr response is published.
3. For an older record without a Zed workspace ID, compare canonical project
   group/path identity only when exactly one Zed workspace matches.
4. If no workspace matches, keep the root disconnected until its workspace is
   restored or the user explicitly selects an owner.
5. If multiple workspaces match, record a mapping conflict. Never select the
   first item in workspace order.

The path value remains diagnostic and a unique fallback only; it is not an
implicit authority when a persisted workspace owner exists.

Recognized agent panes use the existing session-qualified identity:


```text
Herdr session + workspace_id + pane_id + agent_session
  <-> Zed Herdr-backed subthread
```

Shell and non-agent panes remain outside the Zed subthread model.

The bridge keeps the Herdr process identity separate from the Herdr server
identity. The foreground `herdr` client is the ownership witness; the
background Herdr server may outlive it, but cannot wake a dormant bridge by
itself. This distinction is required to keep an external restart from
reconnecting automatically.

## State transitions

```text
Dormant
  -- Open Herdr action or direct `herdr` process -->
Owned / Connecting
  -- endpoint unavailable ----------------------->
Owned / Reconnecting
  -- bootstrap succeeds ------------------------->
Ready / Synchronized
  -- pipe or server exits ------------------------>
Owned / Reconnecting
  -- Zed window closes --------------------------->
Dormant (Herdr remains alive)
```

A fresh Zed window with no ownership trigger remains `Dormant`, even if an
external Herdr session is running.

## Compatibility and migration

- Existing `herdr_thread_mappings` records remain keyed by session and
  workspace ID.
- Existing mappings without an owning Zed workspace ID are not discarded.
- The first unambiguous canonical path match may populate the missing owner;
  ambiguous or missing matches remain conflicts/disconnected.
- Existing default-session mappings are not silently claimed by a new Zed
  window. The user must launch Herdr through an ownership trigger to establish
  the new Zed-owned named session.
- Moving a Zed workspace between windows does not transfer Herdr ownership or
  share a named session. The destination window must establish its own
  ownership; the old session mapping remains historical until explicitly
  re-associated.
- Herdr's public API, Unix socket, Windows named-pipe transport, and structured
  request payloads remain unchanged.

## Verification plan

### Unit tests

- Generate and persist a stable named session independent of runtime
  `WindowId`.
- Deserialize older `MultiWorkspaceState` records without Herdr fields.
- Restore Herdr state into a new runtime window ID.
- Flush a newly reserved session before launch and preserve it across restart.
- Accept only server-launch `herdr` argument forms; reject CLI subcommands,
  aliases/wrappers, mismatched `--session` values, remote terminals, and
  unrelated process names.

### GPUI and integration tests
- Constructing an AgentPanel without a launch trigger creates no bridge and no
  Herdr retry task.
- The explicit action launches the persisted named session and activates one
  bridge per window.
- A direct server-launch `herdr` process in any local Zed terminal activates
  the same bridge.
- A fake Herdr server started after an in-window launch reconnects
  automatically and reaches `Ready`.
- Missing endpoints produce status changes but no repeated Toast events.
- Herdr process exit stops retries; a later external Herdr process does not
  reactivate a dormant bridge.
- Restart restores root `ThreadId` mappings and requires a new in-window launch
  trigger before synchronization.
- Closing Zed leaves the Herdr process/session running and detaches the bridge.
- An externally started Herdr process with the same project paths does not
  activate synchronization in a fresh window.
- Existing Herdr root/agent focus, rename, create, close, reconnect, and
  subthread tests continue to pass.

### Platform checks

- Run Unix socket tests on macOS/Linux.
- Run Windows named-pipe tests and Windows target compilation in Windows CI.
- Run release checks for Zed's desktop binaries.
- Exercise the manual flow: start Zed, launch Herdr inside Zed, switch spaces
  and threads in both directions, close Zed while Herdr remains alive, restart
  Zed, launch Herdr again inside Zed, and verify the original mappings restore.
