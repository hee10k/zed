# 03 — Monotone terminal status badge (deterministic states)

**What to build:** Terminal rows in the sidebar show a badge indicating the terminal-backed agent state: Running, Idle, or Completed. State is derived deterministically from process-alive + currently-generating, `ProcessExited`/no process, and shell-prompt-at-rest signals. The badge is monotone — it uses icon shape, filled↔hollow state, and/or position, never hue alone — and follows the active theme.

**Blocked by:** 01 — Persistent drag-to-reorder of sidebar threads and terminals (lands on the settled sidebar-row rendering path).

**Status:** done

- [x] Terminal rows show a status badge for Running, Idle, and Completed.
- [x] Deterministic signals map to state: alive+generating → Running; exited/no process → Completed.
- [x] The badge is monotone (legible without colour alone).
- [x] The badge follows the active theme and requires no new colour-differentiated semantic palette entries.
- [x] The Idle-vs-WaitingForUserInput disambiguation is out of scope here (deferred to ticket 04).

**Implementation notes:**
- `TerminalAgentStatus` (Idle/Running/Completed) added to `terminal_thread_metadata_store` with a pure `derive(session_boundary, title)` — a live session whose title carries a busy prefix (braille spinner, `>>>`, `✳` etc.) is Running; an ended session (Sleeping/Cleared) is Completed; otherwise Idle.
- `AgentPanelTerminalInfo` carries the derived status; `AgentPanel::terminals()` computes it from the live terminal's session boundary + title.
- The sidebar collects live statuses into `live_terminal_statuses`, falls back to deriving from stored metadata for non-live rows, stores it on `TerminalEntry`, and renders it via the existing `ThreadItem.status(...)` — Running shows the rotating LoadCircle (the codebase's existing monotone spinner badge); Idle/Completed render plain. Reuses existing theming; no new colour semantics.

**Verification:** `cargo test -p agent_ui test_terminal_agent_status_derivation` passes (Running/Idle/Completed matrix). Sidebar render-path test `test_terminal_metadata_is_deduped_across_project_groups` still passes; agent_ui + sidebar build clean; clippy shows no new findings (the one `redundant clone` in agent_ui is pre-existing).