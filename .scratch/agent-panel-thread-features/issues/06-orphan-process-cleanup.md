# 06 — Orphan-process detection and cleanup

**What to build:** Processes tied to closed threads or worktrees are detected and cleaned up. Two orphan classes are covered: (a) child processes of threads/terminals whose worktree was closed while they kept running, and (b) agent/terminal processes whose owning UI is gone but which are still alive. Detection triggers on worktree close (async, non-blocking), a slow periodic sweep, and a manual "Clean Up Orphans" action. Cleanup only targets processes provably tied to a closed thread/worktree, never unrelated processes.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] A process-discovery pass enumerates live processes and matches them to known thread/terminal owners and worktrees.
- [x] Orphans of class (a) — descendants of a thread/terminal whose worktree closed — are detected.
- [x] Orphans of class (b) — live processes whose owning terminal UI is gone — are detected.
- [x] Cleanup triggers on worktree close, without blocking it.
- [x] A slow periodic sweep finds orphans without user action.
- [x] A manual "Clean Up Orphans" action finds and terminates orphans on demand.
- [x] Cleanup only terminates processes provably tied to a closed thread/worktree; unrelated processes are never touched.

**Implementation notes:**
- New `terminal::orphan_cleanup` module: `capture_descendants(root)` walks a terminal's live descendant tree at close time (recording PID + start time); `reap_orphans` kills each still-alive candidate only if its PID still maps to the same capture-time `start_time` — immune to PID reuse. In-memory capture (no persisted PID registry) keeps it safe and simple.
- `AgentPanel` captures a terminal's descendants in `close_terminal_internal` before the terminal handle is dropped; a 30s background sweep calls `reap_pending_orphans`; a `CleanUpOrphans` action reaps on demand.
- Safety: never persists PIDs across restarts; always re-verifies identity before kill.

**Verification:** `cargo test -p terminal orphan_cleanup` passes (2 tests: captures+reaps a real forked child tree while leaving the root shell alive; refuses to reap a recycled/nonexistent PID). `cargo build -p agent_ui` and `cargo build -p git` succeed. Clippy on both crates introduces no new findings (the one `redundant clone` in `agent_ui` is pre-existing on this branch, verified via stash).