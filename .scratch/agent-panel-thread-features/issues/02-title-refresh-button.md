# 02 — Title refresh button on thread and terminal rows

**What to build:** A hover-only refresh icon button appears next to the title on both thread and terminal rows in the sidebar. Clicking it re-runs the existing model-driven title regeneration for that entry, without touching the existing inline rename affordance.

**Blocked by:** 01 — Persistent drag-to-reorder of sidebar threads and terminals (lands on the settled sidebar-row rendering path).

**Status:** done

- [x] Thread rows show a hover-only refresh button next to the title.
- [x] Terminal rows show the same hover-only refresh button.
- [x] Clicking the refresh button triggers the existing model-driven title regeneration.
- [x] The existing inline rename affordance is unchanged.
- [x] The button is hidden unless the row is hovered.

**Implementation notes:**
- Thread rows: a hover-only `RotateCw` icon button sits next to the rename button and calls the existing `Sidebar::regenerate_thread_title` (the LLM regeneration path), gated to native zed-agent threads — the only profile with a model-driven regeneration path today.
- Terminal rows: a hover-only `RotateCw` button sits next to the close button. It calls the new `AgentPanel::refresh_terminal_title`, which re-derives the live terminal title (dropping stale spinner prefixes) and persists it to the metadata store; the sidebar re-renders from the store. For entries whose workspace is closed, the row opens the workspace first (mirroring the close-terminal flow) before refreshing.
- The inline rename affordance (pencil) is untouched.

**Verification:** `cargo build -p agent_ui -p sidebar` clean; existing sidebar and agent_ui tests still pass (`test_terminal_metadata_is_deduped_across_project_groups`, `test_revival_fields_round_trip_through_db`); clippy on sidebar and agent_ui shows no new findings.