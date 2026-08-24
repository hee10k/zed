# Task 2 Report: Validated per-harness terminal resume commands

## Commit

- `12d3c0a35a517002aff91e82dd0cd7f4d3263c77` — `feat(agent-ui): add harness-specific terminal resume commands`

## Changed files

- `crates/agent_ui/src/terminal_resume.rs`
  - Added `ResumeCommandTemplate` with `SharedString` harness/template fields.
  - Added `validate_resume_locator`, enforcing non-empty locators, no Unicode control characters, no leading `-`, and a 512-byte maximum.
  - Added `build_resume_command`, which looks up a harness template, validates the template and locator before interpolation, requires `{locator}`, rejects empty/control-character templates, and never returns a partial command on failure.
  - Added `resume_comment`, which rejects empty/control-character commands and emits exactly `# {command}\r`.
  - Added focused pure tests for valid/invalid locators, newline/control-character injection, leading dashes, empty values, 512-byte boundaries, missing harnesses/placeholders, invalid templates, interpolation, and comment formatting.
- `crates/agent_ui/src/agent_ui.rs`
  - Exported the new `terminal_resume` module.
- `crates/settings_content/src/agent.rs`
  - Added optional object-valued `agent.terminal_resume_commands` settings with documented `{locator}` templates and the OMP default.
- `crates/agent_settings/src/agent_settings.rs`
  - Added normalized `AgentSettings::terminal_resume_commands`.
  - Absent settings normalize to an `IndexMap` containing only `omp -> omp --resume {locator}`.
  - User-provided maps are preserved, including explicit empty templates; the command builder remains the validation boundary and rejects those templates before rendering.
  - Added settings tests for default normalization, user overrides, and an explicitly empty template.

## TDD evidence

- Pure helper tests were written before production helper implementation and the named `agent_ui` test command was started as the RED phase.
- The initial compile was interrupted by interim implementation issues (the collections `IndexMap` API does not support `IndexMap::from`/`IndexMap::new`, and an existing `expand_edit_card` field was temporarily displaced during a surgical edit); those source issues were corrected before commit.

## Focused test summary

- `cargo test -p agent_ui terminal_resume -- --nocapture`
  - Attempted in the RED phase and again after implementation fixes.
  - The build could not complete because the isolated worktree target consumed approximately 15 GiB and the filesystem returned `errno=28` (`No space left on device`) while compiling/linking dependencies.
- `CARGO_BUILD_JOBS=1 cargo test -p agent_settings --lib test_terminal_resume -- --nocapture`
  - Attempted after removing the failed 15 GiB target.
  - Canceled before completion at the parent agent's direction when the partial target reached approximately 5 GiB and only about 10 GiB remained free; no test executable was linked or run.
- The isolated worktree `target/` directory was removed after the resource failures. No formatter, clippy, project-wide build, or project-wide test suite was run.

## Self-review

- Locators are validated before template lookup/interpolation, so invalid locators cannot produce a rendered command.
- Templates are validated before replacement, and all `{locator}` occurrences are replaced literally rather than using shell formatting or execution APIs.
- Generated comments reject all control characters before adding the comment prefix and one carriage return; the helper does not send commands to a terminal.
- Default and user-supplied settings maps retain insertion order through `collections::IndexMap`; absent settings do not merge additional harness defaults.
- Only the four Task 2 files were staged and committed. The pre-existing untracked planning/spec files remain unstaged.

## Concerns

- The focused tests did not reach a PASS result in this constrained worktree because of disk exhaustion and the required cancellation. The main agent should rerun the two named commands after the complete task set lands with shared build artifacts/resources.
- No post-fix Rust compilation completed after the final source-only cleanup; the main agent's focused/project validation should be treated as the compile proof.
## Review-fix follow-up

### Change

- Added `terminal_resume_commands: Default::default()` to the exhaustive `AgentSettings` test literals in `crates/agent_ui/src/agent_ui.rs` and `crates/agent/src/tool_permissions.rs`.
- No production behavior or additional Task 2-6 behavior was changed.

### Test / result

- Source-level verification confirmed both literals now initialize `terminal_resume_commands` with the repository's default settings shape.
- Started `CARGO_BUILD_JOBS=1 cargo check -p agent_ui`, but stopped it before completion at the parent agent's direction to avoid a long constrained-disk build. No compiler result is claimed.

### Self-review

- Both initializers are adjacent to `terminal_init_command` and preserve every existing field/value.
- The new field uses `Default::default()`, matching the normalized `IndexMap<String, String>` settings shape.
- No focused assertion was needed; this compile-fix is fully covered by the exhaustive literal initialization.
