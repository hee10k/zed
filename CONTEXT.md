# Agent Panel Terminal Revival

Terms around keeping agent-session terminals recoverable across Zed updates and restarts. Model follows Orca's sleeping-session + host-authority design.

## Language

**Agent session**:
A terminal running a TUI coding agent (OMP, Claude Code, Codex, etc.) that can be resumed through a provider locator even after its process exits.
_Avoid_: shell, terminal thread

**Sleeping session**:
An agent session whose process has ended but whose resume locator is retained so a new process can relaunch it.
_Avoid_: dead session, zombie

**Resume locator**:
The minimal provider-owned value that identifies the CLI resume target. For OMP this is a file path Zed assigns and controls; the session id is a fallback.
_Avoid_: command line, session metadata

**Session boundary**:
The lifecycle transition an agent session moves through: live (process running), sleeping (process ended, resumable), cleared (no longer resumable).
_Avoid_: restart, reconnect

**Claim key**:
The identity that fences resume so a given session is relaunched exactly once even if multiple windows or processes observe it.
_Avoid_: lock, idempotency token

**Automatic resume**:
Relaunching a sleeping session on activation without user action.
_Avoid_: auto-restore

**Restore-on-tab-open**:
A manually slept session that resumes only when its tab is opened, not on activation.
_Avoid_: lazy restore

# Sidebar entry ordering

**Explicit order**:
A user-assigned, persisted position for a sidebar thread or terminal entry within its project group. Entries without an explicit order fall through to recency sorting.
_Avoid_: pinned, manual order, custom order

**Recency sort**:
The default sidebar ordering by most-recently-touched (`thread_display_time`), used for any entry the user has not assigned an explicit order.

**Project group**:
The scope to which an explicit order belongs; different project groups keep independent arrangements.

# Terminal status

**Terminal status badge**:
A monotone badge on a terminal row indicating whether its terminal-backed agent is running, idle, waiting for user input, or completed. Meaning is carried by shape/fill/position, never hue alone.

**Deterministic status**:
A status derived from process and session signals (process alive + generating → running; `ProcessExited` → completed) without model inference.

**Inferred status**:
The idle-vs-waiting-for-user-input distinction resolved by debounced, cached model inference, cleared on any deterministic transition.

# Process lifecycle

**Orphan process**:
A live process tied to a closed thread or worktree: either a descendant of a thread/terminal whose worktree closed while it ran, or an agent/terminal process whose owning UI is gone but which is still alive.
_Avoid_: zombie, stray, leaked process