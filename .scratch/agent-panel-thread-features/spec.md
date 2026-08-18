# Spec: Agent-panel thread-management, terminal status badges, git fetch prune, orphan cleanup

## Problem Statement

From the user's perspective: the agent-panel sidebar gives no control over how threads and terminal entries are arranged (they reorder themselves silently by when they were last touched); thread titles can be renamed but there is no one-click way to ask the model to retitle a thread; terminal entries show only a notification marker, so a user cannot tell at a glance whether a terminal-backed agent is running, idle, waiting on their input, or done; `git fetch` never prunes deleted remote branches, so stale `origin/…` refs accumulate locally; and when a thread or worktree is closed, child processes spawned by the agent can linger as orphans with no way to find or clean them.

## Solution

Five coordinated improvements to the agent panel, its Git integration, and process lifecycle:

1. **Explicit, persistent thread ordering.** Users can reorder thread and terminal entries in the sidebar by drag-and-drop. A user-set order persists per project group across restarts; entries without a user-set order fall back to the existing recency sort. Both conversation threads and terminal entries participate in the same ordering key.
2. **Title refresh button.** A row-level hover affordance (next to the title) that re-runs the existing model-driven title regeneration for the thread or terminal.
3. **Monotone status badge on terminal entries.** A shaded, color-only-free badge on the terminal icon showing whether the terminal-backed agent is running, idle, waiting for user input, or completed. A deterministic baseline (process alive + generating; process exited; shell prompt state) covers the common cases; model inference is used only to disambiguate **idle vs waiting-for-user-input**, debounced.
4. **git fetch with prune.** The existing Fetch / Fetch-From actions prune deleted remote-tracking branches (`--prune`), default on.
5. **Orphan-process detection and cleanup.** Both (a) child processes of threads/terminals whose worktree was closed while they kept running, and (b) agent/terminal processes whose owning UI is gone but which are still alive, are detected and shown/cleaned. Detection triggers on worktree close, a slow periodic sweep, and a manual "clean orphans" action.

## User Stories

1. As a user, I want to drag thread and terminal rows in the sidebar into any order, so that the list matches how I actually work.
2. As a user, I want my ordering to survive closing and reopening the app, so that I am not re-arranging the sidebar every session.
3. As a user, I want entries without an explicit order to keep the current recency sort, so that default behaviour is unchanged for lists I have never reordered.
4. As a user, I want the saved order to be scoped to each project group, so that different projects keep different arrangements.
5. As a user, I want reordering to work whether the entry is a live conversation thread or a persistent terminal, so that one gesture covers the whole sidebar.
6. As a user, I want a refresh button on a thread row that asks the model to propose a better title, so that I can retitle without leaving my flow.
7. As a user, I want the refresh button to also appear on terminal entries, so that terminal-agent titles can be regenerated.
8. As a user, I want a terminal entry to show a badge indicating it is running, idling, waiting for my input, or completed, so that I can see agent state at a glance.
9. As a user, I want the badge to be understandable without colour alone, so that the status is legible even with colour-blindness or a custom theme.
10. As a user, I want the badge to be accurate without hammering the model on every keystroke, so that it is both useful and economical.
11. As a user, I want `git fetch` and `git fetch --from` to also prune deleted remote-tracking branches, so that stale `origin/*` refs do not pile up.
12. As a user, I want the prune behaviour enabled by default, so that fetch stays consistent with upstream git convention.
13. As a user, I want orphan processes from closed threads or worktrees to be detected and reported, so that stray agent subprocesses do not keep running unseen.
14. As a user, I want a manual "clean orphans" action that finds and terminates orphans on demand, so that I have an explicit escape hatch.
15. As a user, I want periodic and worktree-close detection so orphans are caught without me having to remember to clean up.
16. As a user, I want the cleanup to be safe: it must only target processes demonstrably tied to a closed thread/worktree, never unrelated processes.

## Implementation Decisions

- **Ordering model.** A single explicit, persisted integer/numbered `order` shared by threads and terminals within a project group. Entries carrying a user-assigned order sort by it; entries without one sort by their existing `thread_display_time` (recency), grouped after the explicitly-ordered ones. The order lives in the existing metadata stores (`ThreadMetadataStore`, `TerminalThreadMetadataStore`) as a new nullable column so unset = fall through to recency. Drag-and-drop is implemented in the sidebar `ThreadList` via GPUI's existing drag/drop element primitives; on drop the affected store rows are rewritten with the new ordering.
- **Title refresh.** Reuses the existing model-driven regeneration path (`regenerate_thread_title` / `ThreadTitleRegenerationResult`, plus the terminal-composition machinery). The sidebar row gains a hover-only refresh icon button beside the title; the existing inline rename affordance is unchanged.
- **Terminal status badge.** New status enum for sidebar display with four states — Running, Idle, WaitingForUserInput, Completed. Deterministic derivation: process alive + currently generating → Running; `ProcessExited`/no process → Completed; process alive and at a shell prompt with no pending agent work → Idle. The ambiguous Idle vs WaitingForUserInput split is resolved with a debounced model inference that inspects the terminal foreground/context; the inference result is cached and cleared on Deterministic transitions (Running/Completed). Monotone rendering: a single shade's use of icon shape, filled↔hollow state, and/or position, never hue alone — below `UiHexColors`-driven theming so the badge follows the active theme's accent/foreground without introducing new semantic palette entries that rely on colour.
- **git fetch prune.** `FetchOptions` gains a prune dimension; the fetch command builder appends `--prune`. Default: pruned. The existing Fetch and Fetch-From actions both route through it.
- **Orphan detection & cleanup.** A process-discovery pass enumerates live processes and matches them against known thread/terminal owners and worktrees. An orphan is either (a) a descendant of a thread/terminal whose worktree was closed or removed while the process ran, or (b) a process whose owning terminal UI is gone but which still runs. Cleanup triggers: on worktree close (async, non-blocking), a slow periodic sweep (configurable interval), and a manual "Clean Up Orphans" action. Cleanup consults the existing PTY process table and the terminal kill helpers; it only terminates processes whose lineage ties them to a closed thread/worktree. The workspace's well-known lifecycle hooks (`WorktreeRemoved`, thread close) feed the detection set.

## Testing Decisions

- A good test asserts external, observable behaviour — the sidebar order after a drag, the store row's persisted order, the badge state shown for a given deterministic signal, the fetch argv including `--prune`, an orphan being reported and then gone — not internal plumbing.
- **Primary seams (P):** `sidebar` GPUI tests — drag a row, rebuild/reopen, assert order persists; refresh button fires regeneration; badge renders for each deterministic state. Prior art: existing `sidebar.rs` GPUI tests using `TestAppContext` and the `register_test_sidebar` harness; existing `agent_panel.rs` tests.
- **Supporting seam (S1):** store tests in `ThreadMetadataStore` and `TerminalThreadMetadataStore` for order-persistence migration plus round-trip. Prior art: existing `mod tests` in `terminal_thread_metadata_store.rs`.
- **Supporting seam (S2):** pure decision tests for the badge state machine (deterministic signal → state) and the orphan predicate (process lineage → orphan or not). Prior art: `terminal/src/pty_info.rs` process tests and the pure shared-module pattern used for resume-argv in the OMP revival work.
- **Supporting seam (S3):** a git integration test asserting the fetch command shape includes `--prune`. Prior art: existing `git` repo integration tests.

## Out of Scope

- A new status vocabulary beyond the four sidebar states (no error/disconnected/etc. states).
- Colour-based status design; the badge is monotone by requirement.
- Making orphan cleanup automatic-without-safety — it remains gated to provably-tied processes and never nominates processes outside closed threads/worktrees.
- Reordering behaviour in the threads-archive/history view (locked to the primary ThreadList sidebar).
- Any change to how thread titles are *generated* by the model — only the affordance to trigger regeneration is new.
- Remote/WSL processes (consistent with the OMP-local-first ADR 0003).

## Further Notes

- The ordering, badge, and refresh features all live on the same sidebar `ThreadList` surface and share the two metadata stores, so their seams overlap; the tickets that touch them must sequence carefully to avoid editing the same `rebuild_contents` sort region concurrently.
- Reuses the deterministic/differentiate split philosophy already present in the codebase (deterministic first, model only for the ambiguous remainder).
- No changes to the existing OMP revival lifecycle; orphan cleanup complements rather than replaces it.