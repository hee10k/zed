# Task 3 stale import cancellation report

Status: complete

Commit: `fix(herdr): cancel stale snapshot imports`

Changes:

- Added per-target snapshot-import abort handles and initialized the map in production and test registry constructors.
- Wrapped each guarded sequential snapshot workspace import in `Abortable`, treating cancellation as a harmless completion and retaining the existing freshness checks around every workspace add and root refresh.
- Removed a completed import handle only when its attempt and finalize token still match, preventing stale work from deleting a newer retry's handle.
- Abort and remove target imports during disconnect, window release, session-loss invalidation, retry/reselection, and finalized timeout cleanup; timeout cleanup now signals the import before detaching the binding.

Tests:

- `git diff --check` — passed; Git emitted only its normal LF-to-CRLF working-copy warning.
- Cargo/rustc are unavailable in this environment, so focused Rust tests were not run.
- Formatter, linter, and project-wide suites were skipped as required.
- `untitled.md` and `herdr_host.rs` were not modified.

Concerns: Rust compilation and runtime verification remain unavailable until a Rust toolchain is present.
