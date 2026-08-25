# Herdr–Zed Thread Synchronization Design

## Status

Approved design for implementation planning.

## Problem

Herdr and Zed currently maintain independent navigation and lifecycle models:

- Herdr owns persistent workspaces, tabs, panes, recognized coding agents, agent state, and terminal input.
- Zed owns root agent threads and ACP subthreads.
- Neither product knows when the other product changes the active item.

This produces two independent notions of the current task. Selecting a Herdr workspace does not select the corresponding Zed thread, and selecting a Zed thread does not select the corresponding Herdr workspace. Agent panes and Zed subthreads also have no durable relationship.

## Goal

Provide a bidirectional, cross-platform bridge with these canonical mappings:

```text
Herdr session + workspace_id
    <-> Zed root ThreadId

Herdr session + pane_id + agent_session
    <-> Zed Herdr-backed subthread
```

The bridge must support:

- workspace/thread focus synchronization in both directions;
- recognized Herdr agent pane/subthread focus synchronization in both directions;
- creation, rename, close, reconnect, and restoration;
- multiple Herdr sessions without ID collisions;
- macOS, Linux, and Windows desktop builds;
- Herdr as the execution authority for prompts, cancellation, process state, and pane closure.

## Non-goals

- Remote Zed development where Zed and Herdr run on different hosts.
- Mapping ordinary shell, log, or non-agent panes to Zed subthreads.
- Replacing Herdr's terminal/process runtime with Zed process management.
- Replacing ACP-native Zed threads with Herdr-backed threads.
- Supporting arbitrary TCP endpoints for Herdr.
- Merging multiple Herdr sessions into one Zed window.

## Terminology

- **Herdr session**: an isolated Herdr runtime namespace with its own socket and persisted state.
- **Herdr workspace / space**: Herdr's top-level project container.
- **Herdr agent pane**: a pane containing a recognized coding-agent process and, when available, a session identity.
- **Zed root thread**: a persisted root entry in Zed's thread metadata store.
- **Zed Herdr-backed subthread**: a Zed subthread view whose prompt, state, output, and lifecycle are controlled through Herdr rather than ACP.
- **Bridge**: one Zed-window-scoped connection to one Herdr session.

## Session relationship

One Zed window binds to one Herdr session.

Multiple Herdr sessions are supported by multiple Zed windows or by rebinding a Zed window to a different session. A session switch disconnects the old bridge, preserves its thread metadata as disconnected, and loads the new session through a fresh snapshot. The bridge never merges focus events from multiple sessions into one Zed window.

A mapping key always includes the Herdr session identity. `workspace_id` and `pane_id` are not globally unique across named sessions.

The default session and endpoint resolution follow Herdr's documented order:

1. explicit user-selected session or endpoint;
2. `HERDR_SOCKET_PATH`;
3. `HERDR_SESSION`;
4. the default Herdr session endpoint.

## Architecture

### Platform-neutral client

Add a Zed-side Herdr client with these responsibilities:

- request ID generation and response matching;
- newline-delimited JSON encoding and decoding;
- `session.snapshot` bootstrap;
- `events.subscribe` registration;
- event dispatch;
- request timeout and reconnect state;
- protocol/version validation;
- operation origin and sequence propagation.

The client must not expose OS-specific stream types to thread, sidebar, or UI code.

### Platform transport boundary

Define a small transport boundary used by the client:

```rust
trait HerdrTransport {
    async fn connect(endpoint: HerdrEndpoint) -> Result<Self>;
    async fn send(&mut self, request: &[u8]) -> Result<()>;
    async fn receive(&mut self) -> Result<Vec<u8>>;
}
```

The implementation is platform-specific only inside the transport module:

- Unix targets use a Unix domain socket stream.
- Windows uses a named pipe client compatible with Herdr's pipe endpoint.
- The endpoint remains an opaque platform-specific value above this layer.
- Common NDJSON framing is shared across all targets.

The bridge uses GPUI-managed background work for socket reads, reconnects, and request completion, then applies state changes on the foreground app context.

### Thread bridge

`HerdrThreadBridge` owns:

- the current session binding;
- the persisted mapping index;
- Herdr snapshot and live event state;
- root-thread and subthread lifecycle reconciliation;
- focus synchronization;
- operation fencing and loop suppression;
- connection status exposed to the UI.

The bridge is installed per Zed window and publishes app/entity events rather than directly mutating unrelated UI entities.

### Herdr-backed subthread view

Reuse the existing Zed subthread presentation and navigation concepts, but add an explicit backend distinction:

- ACP-native subthreads continue using the existing ACP connection.
- Herdr-backed subthreads render Herdr-provided output/status and route actions to Herdr.

The Herdr-backed view must not fabricate an ACP session or pretend that Herdr owns an ACP connection. The backend type is persisted in the mapping record and determines which action router and reload path are used.

## Mapping model

Persist a mapping record containing at least:

```text
herdr_session
herdr_workspace_id
herdr_pane_id                optional
herdr_agent_session_kind    optional: id | path
herdr_agent_session_value   optional
zed_root_thread_id
zed_subthread_session_id    optional
worktree_or_cwd_identity
last_seen_sequence
lifecycle_state
```

Root records have workspace identity and a Zed root `ThreadId`. Subthread records additionally have pane and agent-session identity. The mapping store must be backed by Zed's existing cross-platform persistence layer.

### Reconciliation identity

1. Exact session + Herdr ID mapping wins.
2. A restarted pane may be matched by agent session identity.
3. If session identity is unavailable, worktree/cwd is only a diagnostic hint and must not silently reuse an unrelated subthread.
4. Ambiguous matches become a visible mapping conflict; the bridge does not create duplicate entities automatically.
5. A closed resource remains as a tombstone long enough to reject late events and prevent resurrection from stale data.

An agent pane without an agent session identity can report status but does not create a Zed subthread. When the identity arrives later, the bridge creates or restores the subthread.

## Initial synchronization

1. Resolve the selected Herdr session endpoint.
2. Connect through the platform transport.
3. Send `ping` and validate the Herdr protocol version/capabilities.
4. Register `events.subscribe` before requesting the snapshot; buffer pushed events received after subscription acceptance.
5. Request `session.snapshot`.
6. Load persisted mappings for the current session.
7. Reconcile every Herdr workspace, including workspaces with no agent panes.
8. Create or restore Zed root thread metadata for missing workspaces.
9. Reconcile recognized agent panes with session identities into Herdr-backed subthreads.
10. Apply Herdr's current workspace, tab, and pane focus to Zed.
11. Replay buffered events whose sequence is newer than the snapshot state.
12. Mark the bridge synchronized only after snapshot reconciliation, buffered-event replay, and subscription registration succeed.

Subscribing before the snapshot prevents a lifecycle event between bootstrap requests from being lost because Herdr subscriptions do not replay events emitted before subscription acceptance.


## Event and action flow

### Herdr to Zed

- `workspace.created` creates a root thread mapping.
- `workspace.renamed` updates the root title unless a newer explicit Zed title change exists.
- `workspace.focused` activates the corresponding Zed root thread.
- `workspace.closed` transitions the root thread to archived/closed state.
- `pane_agent_detected` with a session identity creates or restores a subthread.
- `pane_agent_status_changed` updates the Zed subthread status.
- `pane_focused` activates the corresponding Zed subthread.
- `pane_exited` marks the subthread complete and leaves a tombstone mapping.
- agent session changes reconcile the subthread identity before applying focus or status.

Herdr output is read through its pane/agent read API and reflected in the Herdr-backed subthread transcript. Subscribe to pane output/revision events where available, and use a revision-aware pane read to hydrate the initial transcript. Older reads must not overwrite newer content.

### Zed to Herdr

- Selecting a Herdr-backed root thread calls `workspace.focus`.
- Selecting a Herdr-backed subthread calls `pane.focus`.
- Prompt submission calls `agent.prompt` for a recognized agent target.
- Cancel/interrupt sends the supported Herdr key sequence through `agent.send_keys` or `pane.send_keys`; Herdr has no separate agent-cancel method.
- Title editing calls `workspace.rename` or the supported pane metadata/title API.
- Closing a Herdr-backed root thread requests Herdr workspace closure.
- Closing a Herdr-backed subthread requests `pane.close` after any required interruption and waits for Herdr confirmation.
- Creating a Herdr-backed root thread creates a Herdr workspace.
- Creating a Herdr-backed subthread creates or attaches a Herdr agent pane.

ACP-native Zed thread creation and actions keep their existing behavior and do not create or control Herdr resources.

Herdr remains the authority for process execution, prompt delivery, agent state, and closure. Zed actions are requests, not local state mutations that bypass Herdr.


## Lifecycle and conflict policy

### Focus

Focus operations carry an operation ID, origin, and monotonic sequence. A reflected event matching an already-applied operation is ignored. The most recent valid explicit user focus wins.

Zed focus changes are only sent for Herdr-backed entities. ACP-native Zed threads do not affect Herdr.

### Rename

Explicit user titles and automatically detected terminal titles are stored separately. Automatic terminal-title updates never overwrite an explicit title. A newer explicit user edit wins over an older rename event from either product.

### Create

Herdr workspace creation and Zed root-thread creation are reconciled by session and stable identity. If both sides independently create an entity and no deterministic match exists, the bridge records a conflict rather than silently merging unrelated work.

Agent subthreads require a Herdr agent session identity. A pane that is only a shell remains outside the Zed subthread model.

### Close

- Herdr workspace close archives/closes the root thread after the event is observed.
- Zed root-thread close requests Herdr workspace closure and waits for confirmation.
- Herdr pane exit marks the subthread complete.
- Zed subthread close requests Herdr interruption/closure and retains the tombstone until the final event or confirmed failure.

### Connection loss

A lost connection moves the bridge to `Unavailable` or `Reconnecting` while preserving the last known thread state. Herdr-backed input, focus, rename, and close controls are disabled until reconnection. ACP-native Zed threads remain functional.

Reconnect uses bounded exponential backoff. After reconnect:

1. send `ping` and validate protocol compatibility;
2. register a fresh subscription and begin buffering events;
3. request a fresh snapshot;
4. reconcile mappings;
5. discard stale events by sequence and replay buffered events newer than the snapshot;
6. reapply the authoritative current Herdr focus;
7. replace the old subscription and return to `Ready`.

## Error and security policy

- Request failures are returned to the initiating UI action and shown with an actionable error.
- Malformed frames, protocol mismatch, response timeout, and broken subscriptions fault the bridge and trigger reconnect.
- Missing or reversed sequence data marks only the affected resource stale and triggers a targeted or full snapshot refresh.
- Herdr unavailable at startup is a supported state; Zed continues normally with Herdr integration disabled.
- The bridge connects only to Herdr's local Unix socket or Windows named pipe. Arbitrary TCP endpoints are unsupported.
- Prompt and key input are sent as structured API payloads, never reconstructed as shell command strings.
- Titles, labels, and output from Herdr are treated as UI data, not executable content.
- The bridge does not alter Windows named-pipe permissions.
- Session rebinding requires explicit user action.

## User interface

- Add a visible Herdr connection/session status to the relevant Zed thread surface.
- Add a `Connect to Herdr Session` action for named-session selection.
- Group or label Herdr-backed root threads with their session name when disconnected or when multiple historical sessions are visible.
- Preserve existing Zed thread rows and subthread navigation where possible.
- Disable Herdr-backed controls while disconnected and retain the reason in the UI.
- Do not display ordinary Herdr shell/log panes as Zed subthreads.

## Testing strategy

### Platform-neutral tests

- NDJSON request/response encoding and decoding.
- Snapshot reconciliation and mapping persistence.
- Session-qualified ID collision prevention.
- Workspace and pane create/rename/close transitions.
- Agent session arrival, replacement, and disappearance.
- Focus reflection and loop suppression.
- Sequence ordering and stale-event rejection.
- Reconnect reconciliation.
- Ambiguous mapping conflict behavior.

### Platform transport tests

- Unix domain socket fixture on macOS/Linux.
- Windows named-pipe fixture on Windows.
- Disconnect, timeout, malformed frame, reconnect, and subscription replacement on each transport.
- Cross-target compilation in macOS, Linux, and Windows CI jobs.

### Runtime smoke scenarios

1. Start Herdr with two workspaces and two recognized agent panes.
2. Select each Herdr workspace and verify the corresponding Zed root thread activates.
3. Select each Zed root thread and verify Herdr workspace focus changes.
4. Select each Herdr agent pane and verify the corresponding Zed subthread activates.
5. Select each Zed subthread and verify Herdr pane focus changes.
6. Submit a prompt, cancel it, rename the workspace/agent, and close each resource from both products.
7. Stop and restart Herdr, then verify mapping restoration after snapshot reconciliation.
8. Rebind one Zed window from one named Herdr session to another and verify session-qualified mappings.
9. Run the same scenario on macOS, Linux, and Windows.

## Compatibility and rollout

- Existing ACP-native threads remain unchanged.
- Existing Zed thread metadata remains readable; Herdr mapping records are additive.
- If Herdr is absent or reports an unsupported protocol, Zed falls back to normal standalone behavior.
- The bridge must not block Zed startup or the normal agent panel while connecting.
- Herdr-backed behavior is enabled only after successful endpoint discovery and protocol validation.

## External protocol references

- Herdr concepts: https://herdr.dev/docs/concepts/
- Herdr agent automation: https://herdr.dev/docs/agent-automation/
- Herdr socket API: https://herdr.dev/docs/socket-api/
- Herdr API schema: https://github.com/herdrdev/herdr/blob/master/docs/next/api/herdr-api.schema.json
