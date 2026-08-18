# 04 — Disambiguate idle vs waiting-for-user-input (model inference)

**What to build:** The terminal badge's ambiguous Idle vs WaitingForUserInput split is resolved with a debounced, cached model inference that inspects the terminal foreground/context. The inference result is cleared on any deterministic transition (Running / Completed). This completes the four-state badge (Running, Idle, WaitingForUserInput, Completed).

**Blocked by:** 03 — Monotone terminal status badge (deterministic states).

**Status:** done

- [x] WaitingForUserInput is distinguished from Idle using debounced model inference.
- [x] The inference is debounced so it does not fire on every keystroke.
- [x] The inferred state is cached and cleared on determinist Running/Completed transitions.
- [x] The badge remains monotone and legible for all four states.

**Implementation notes:**
- `TerminalAgentStatus` gains `WaitingForUserInput` and a pure `with_inferred_waiting(bool)` helper: positive inference promotes deterministic `Idle` → `WaitingForUserInput`; Running/Completed never regress.
- `AgentPanel` fields: `inferred_terminal_waiting: HashMap<TerminalId, bool>` (cache) + `_terminal_inference_tasks` (in-flight tasks). `schedule_terminal_status_inference` debounces 3s per terminal (`TERMINAL_STATUS_INFERENCE_DEBOUNCE`), resolves the default model up front, reads `Terminal::get_content()` after the debounce, and runs a tiny classifier ("waiting for the user? answer YES/NO") via `stream_completion_text`. Only an explicit YES sets waiting; model failure conservatively reports Idle.
- Triggered from the terminal event subscription on `Wakeup`/`TitleChanged` (output-driven), so it never fires on keystrokes and naturally debounces bursts.
- Cache cleared on: deterministic non-Idle transitions (`refresh_terminal_metadata` when the spinner starts or a session ends), explicit close (`close_terminal_internal`), and sleeping transition (`transition_terminal_to_sleeping`).
- Sidebar maps `WaitingForUserInput` → the existing monotone `AgentThreadStatus::WaitingForConfirmation` warning badge (ADR 0005).

**Verification:** `cargo test -p agent_ui test_terminal_agent_status` passes (2 tests: deterministic derivation matrix + inference-compose incl. no-regression on Running/Completed). agent_ui + sidebar build clean; clippy shows no new findings (the one `redundant clone` in agent_ui is pre-existing on this branch).