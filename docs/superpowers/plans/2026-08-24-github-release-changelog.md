# User-Friendly GitHub Release Changelog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make GitHub draft releases contain deterministic, grouped, user-friendly changelogs while preserving the existing PR `Release Notes` workflow.

**Architecture:** Keep Git range discovery and tag/channel handling in `script/draft-release-notes`. Split note parsing, fallback classification, link attachment, deduplication, and Markdown rendering into pure functions exported for Node tests. The release workflow continues invoking the script and creating a draft release; `.github/workflows/release.yml` remains generated from `tooling/xtask/src/tasks/workflows/release.rs`.

**Tech Stack:** Node.js built-in modules, Git CLI, GitHub Markdown, GitHub Actions, Node `node:test`.

## Global Constraints

- Preserve the existing CLI: `node --redirect-warnings=/dev/null ./script/draft-release-notes <version> <stable|preview>`.
- Preserve the existing previous-tag lookup, shallow clone, channel suffix, and no-public-changes behavior.
- Explicit PR `Release Notes` remain the primary source of truth.
- `- N/A` excludes an entry from the public changelog.
- Deterministic fallbacks are allowed only for `feat`, `fix`, and `perf` commit subjects.
- Exclude `docs`, `test`, `refactor`, `chore`, and untyped commit subjects from fallback generation.
- Output sections in fixed order: `New`, `Improvements`, `Fixes`.
- Omit empty sections and deduplicate identical normalized entries.
- Preserve PR links and commit comparison links.
- Do not add an LLM, network API, new npm dependency, release permission, or release-tag behavior.
- Regenerate `.github/workflows/release.yml` from `tooling/xtask/src/tasks/workflows/release.rs` when workflow-source changes occur.

---

### Task 1: Add pure release-note classification and normalization

**Files:**
+- Modify: `script/draft-release-notes`
+- Create: `script/test/draft-release-notes.test.js`
+
+**Interfaces:**
+- `parseReleaseNotes(body) -> { kind: "explicit" | "skip" | "missing", text: string }`
+- `classifyReleaseEntry(text) -> "New" | "Improvements" | "Fixes" | null`
+- `fallbackReleaseEntry(subject) -> { section: string, text: string } | null`
+
+- [ ] Refactor `script/draft-release-notes` so `main()` runs only when the file is the CLI entry point and the pure helpers are exportable from `require()`.
+- [ ] Implement `parseReleaseNotes` against the first case-insensitive `Release Notes:` section. Extract the first non-empty Markdown bullet, classify `- N/A` as `skip`, and treat absent/malformed content as `missing`.
+- [ ] Implement `classifyReleaseEntry` using the leading verbs `Added`, `Improved`, and `Fixed`. Return `New`, `Improvements`, and `Fixes`; return `null` for all other wording.
+- [ ] Implement `fallbackReleaseEntry` for subjects matching `feat`, `fix`, and `perf`, including optional scopes such as `fix(sidebar): ...`. Remove the prefix/scope and produce `Added ...`, `Fixed ...`, or `Improved ...`; return `null` for `docs`, `test`, `refactor`, `chore`, and untyped subjects.
+- [ ] Add Node tests for explicit notes, `N/A`, malformed sections, all three fallback types, and excluded commit prefixes.
+- [ ] Run:
+
+```bash
+node --test script/test/draft-release-notes.test.js
+```
+
+Expected: all pure classification tests pass.
+- [ ] Commit:
+
+```bash
+git add script/draft-release-notes script/test/draft-release-notes.test.js
+git commit -m "test(release): define changelog classification rules"
+```
+
+---
+
+### Task 2: Render grouped release Markdown with links and fallbacks
+
+**Files:**
+- Modify: `script/draft-release-notes`
+- Modify: `script/test/draft-release-notes.test.js`
+
+**Interfaces:**
+- `formatCommitEntry(commit) -> { section: string, text: string, key: string } | null`
+- `renderReleaseNotes(entries, compareUrl) -> string`
+
+- [ ] Extend commit parsing so explicit notes override fallbacks and `- N/A` never reaches rendered output.
+- [ ] Attach a pull-request link when `commit.pr` is present and a commit link otherwise. Preserve an existing issue/PR link already present in explicit note text without adding a duplicate link.
+- [ ] Normalize each accepted entry to one Markdown bullet whose text starts with `Added`, `Improved`, or `Fixed`.
+- [ ] Deduplicate by normalized text plus linked PR/commit identity while preserving first-seen release-range order.
+- [ ] Render non-empty sections in exactly this order:
+
+```markdown
+## New
+
+- Added ...
+
+## Improvements
+
+- Improved ...
+
+## Fixes
+
+- Fixed ...
+```
+
+- [ ] Render `No public-facing changes in this release. [View the commits](<compare-url>).` when no entries remain.
+- [ ] Keep the CLI output unchanged for invalid arguments, missing prior tags, clone failures, and malformed individual commits except for the improved Markdown body.
+- [ ] Add tests for section ordering, empty-section omission, duplicate removal, PR/commit links, and no-public-changes output.
+- [ ] Run:
+
+```bash
+node --test script/test/draft-release-notes.test.js
+```
+
+Expected: pure and renderer tests pass.
+- [ ] Commit:
+
+```bash
+git add script/draft-release-notes script/test/draft-release-notes.test.js
+git commit -m "feat(release): group user-facing changelog entries"
+```
+
+---
+
+### Task 3: Document and verify release workflow ownership
+
+**Files:**
+- Modify: `docs/src/development/release-notes.md`
+- Modify: `tooling/xtask/src/tasks/workflows/release.rs` only if the generated command or step name needs to reflect the new formatter
+- Generated: `.github/workflows/release.yml` only when Task 3 modifies the workflow source
+
+- [ ] Update the release-note guide with the generated GitHub Release format and explain that explicit PR notes are grouped into New/Improvements/Fixes, while only conventional `feat`, `fix`, and `perf` subjects receive fallbacks.
+- [ ] State that `N/A`, docs, tests, refactors, chores, and untyped commits are excluded from the user-facing release body.
+- [ ] Confirm the generated workflow still runs `script/draft-release-notes` before `script/create-draft-release`.
+- [ ] If `tooling/xtask/src/tasks/workflows/release.rs` changes, run:
+
+```bash
+cargo xtask workflows
+```
+
+Expected: `.github/workflows/release.yml` is regenerated without unrelated changes.
+- [ ] Run the documentation/script checks from Tasks 1–2 again after the workflow verification.
+- [ ] Commit:
+
+```bash
+git add docs/src/development/release-notes.md tooling/xtask/src/tasks/workflows/release.rs .github/workflows/release.yml
+git commit -m "docs(release): explain grouped changelog output"
+```
+
+---
+
+### Task 4: Run final release-note verification
+
+**Files:**
+- Modify: none unless verification exposes a defect
+
+- [ ] Run the script unit tests:
+
+```bash
+node --test script/test/draft-release-notes.test.js
+```
+
+- [ ] Run the repository script checks relevant to release workflow generation:
+
+```bash
+./script/shellcheck-scripts error
+cargo xtask workflows
+```
+
+Expected: exit code 0 and no generated workflow diff.
+- [ ] Run a local renderer smoke test with representative commits through the exported pure functions and verify the exact Markdown section order.
+- [ ] Inspect the final semantic diff and confirm no release permissions, tags, external APIs, or unrelated changelog files changed.
+- [ ] Commit any verification-only correction with an imperative message.
+- [ ] Run final code review before declaring the release changelog flow complete.
+
+Acceptance: a GitHub draft release generated by the existing tag workflow receives a deterministic, grouped, user-friendly Markdown changelog with safe fallbacks and preserved PR/commit links.
+