# User-Friendly GitHub Release Changelog

## Status

Approved design for the GitHub release workflow. The release process must produce a deterministic, user-facing changelog from the existing PR `Release Notes` convention without requiring an external LLM or runtime API.

## Context

GitHub releases are drafted by `.github/workflows/release.yml`. The generated workflow source is `tooling/xtask/src/tasks/workflows/release.rs`. The `create_draft_release` job runs `script/draft-release-notes`, then passes the generated Markdown to `script/create-draft-release`.

`script/draft-release-notes` currently extracts `Release Notes` blocks from commits between the previous and current release tags. It preserves the note text and adds pull-request links, but does not group entries, normalize user-facing wording, deduplicate repeated entries, or safely derive a fallback when a user-facing commit has no explicit note.

## Goals

1. Generate a readable GitHub Release body with predictable sections.
2. Preserve explicit PR-authored `Release Notes` as the primary source of truth.
3. Normalize common release-note verbs into user-facing sections:
   - `Added` → `New`
   - `Improved` → `Improvements`
   - `Fixed` → `Fixes`
4. Provide deterministic fallback wording for conventional user-facing commits when an explicit note is missing.
5. Exclude `N/A`, documentation-only, test-only, refactor-only, and internal maintenance changes.
6. Preserve PR links and commit comparison links.
7. Keep release generation offline-compatible apart from the existing shallow Git clone.
8. Test parsing and formatting without creating or modifying a GitHub release.

## Non-goals

- LLM-generated or probabilistic rewriting.
- Changing the PR template or release-note policy for contributors.
- Replacing the preview-channel release-note pipeline.
- Writing a repository `CHANGELOG.md` for every release.
- Inferring user-facing impact from arbitrary prose or implementation details.
- Changing release tags, draft/publish permissions, or artifact upload behavior.

## Design

### Source priority

For each commit in the tag range:

1. Parse the commit body for the first `Release Notes:` section.
2. Extract bullets until the next blank section boundary.
3. Treat a note beginning with `- N/A` as intentionally excluded.
4. If the note is empty, consider a deterministic conventional-commit fallback.
5. Ignore commits that do not produce a valid user-facing entry.

Explicit PR notes always override generated fallback text.

### Deterministic fallback

Only commit subjects with these prefixes are eligible:

- `feat` → `Added <subject description>`
- `fix` → `Fixed <subject description>`
- `perf` → `Improved <subject description>`

The fallback removes the conventional-commit prefix and optional scope, converts the first character to lowercase after the leading release verb, and retains the subject wording. `refactor`, `chore`, `test`, `docs`, and untyped subjects do not produce fallback entries.

The fallback is intentionally conservative. It must never turn an arbitrary commit title into a user-facing claim.

### Section formatting

Normalize each accepted entry into one of these sections:

```markdown
## New

- Added ... ([#123](https://github.com/zed-industries/zed/pull/123))

## Improvements

- Improved ... ([#456](https://github.com/zed-industries/zed/pull/456))

## Fixes

- Fixed ... ([#789](https://github.com/zed-industries/zed/pull/789))
```

Section order is fixed: New, Improvements, Fixes. Empty sections are omitted. Within a section, preserve release-range commit order after deduplicating identical normalized entries. PR links remain Markdown links; commit-only entries link to the commit URL.

If no accepted entries remain, emit:

```markdown
No public-facing changes in this release. [View the commits](<compare-url>).
```

### Parsing and formatting boundary

Keep Git range discovery and commit extraction in `script/draft-release-notes`. Separate pure functions should handle:

- commit-body parsing;
- fallback classification;
- section classification;
- PR/commit link attachment;
- duplicate removal;
- Markdown rendering.

The command-line entry point remains unchanged:

```bash
node --redirect-warnings=/dev/null ./script/draft-release-notes "$RELEASE_VERSION" "$RELEASE_CHANNEL"
```

### Workflow ownership

`tooling/xtask/src/tasks/workflows/release.rs` remains the source of truth for the release workflow. `.github/workflows/release.yml` must be regenerated with `cargo xtask workflows` after workflow-source changes. The release job continues to create a draft release; this design changes only the generated body.

### Error handling

- Invalid CLI arguments fail with the existing usage message and non-zero status.
- Missing previous tags retain the existing no-op behavior with an explanatory message.
- Malformed Release Notes sections are ignored rather than emitted as broken Markdown.
- Git clone or tag lookup failures remain fatal.
- One malformed commit must not prevent valid notes from other commits from rendering.

### Verification

Unit-level script tests must cover:

1. Explicit Added/Improved/Fixed notes.
2. `N/A` exclusion.
3. Conventional `feat`, `fix`, and `perf` fallbacks.
4. Exclusion of `docs`, `test`, `refactor`, and `chore` commits.
5. PR and commit link formatting.
6. Duplicate removal.
7. Fixed section ordering with empty-section omission.
8. No-public-changes output.
9. Malformed release-note block isolation.

Workflow verification must confirm:

- `cargo xtask workflows` leaves `.github/workflows` unchanged after regeneration.
- The release job still invokes `script/draft-release-notes` and `script/create-draft-release` in that order.
- The generated Markdown is accepted by `gh release create -F` semantics without opening a real release.
