const test = require("node:test");
const assert = require("node:assert/strict");

const {
  classifyReleaseEntry,
  fallbackReleaseEntry,
  formatCommitEntry,
  parseReleaseNotes,
  renderReleaseNotes,
} = require("../draft-release-notes");

test("parses an explicit release note", () => {
  assert.deepEqual(
    parseReleaseNotes("Implementation details\n\nRelease Notes:\n\n- Added a faster project search."),
    { kind: "explicit", text: "Added a faster project search." },
  );
});

test("skips N/A and malformed release notes", () => {
  assert.deepEqual(parseReleaseNotes("Release Notes:\n\n- N/A"), {
    kind: "skip",
    text: "",
  });
  assert.deepEqual(parseReleaseNotes("Release Notes:\n\nNo bullet"), {
    kind: "missing",
    text: "",
  });
});

test("classifies explicit release verbs", () => {
  assert.equal(classifyReleaseEntry("Added a project panel"), "New");
  assert.equal(classifyReleaseEntry("Improved startup time"), "Improvements");
  assert.equal(classifyReleaseEntry("Fixed a crash"), "Fixes");
  assert.equal(classifyReleaseEntry("Changed an internal helper"), null);
});

test("creates conservative conventional-commit fallbacks", () => {
  assert.deepEqual(fallbackReleaseEntry("feat(sidebar): Add grouped rows"), {
    section: "New",
    text: "Added add grouped rows",
  });
  assert.deepEqual(fallbackReleaseEntry("fix: Prevent duplicate tabs"), {
    section: "Fixes",
    text: "Fixed prevent duplicate tabs",
  });
  assert.deepEqual(fallbackReleaseEntry("perf: Reduce startup allocations"), {
    section: "Improvements",
    text: "Improved reduce startup allocations",
  });
  assert.equal(fallbackReleaseEntry("refactor: Split a module"), null);
  assert.equal(fallbackReleaseEntry("docs: Explain releases"), null);
  assert.equal(fallbackReleaseEntry("Update dependencies"), null);
});

test("explicit notes override fallbacks and preserve PR links", () => {
  assert.deepEqual(
    formatCommitEntry({
      hash: "abc123",
      pr: "42",
      firstLine: "feat: ignored fallback",
      body: "Release Notes:\n\n- Fixed the visible issue.",
    }),
    {
      section: "Fixes",
      text: "Fixed the visible issue. ([#42](https://github.com/zed-industries/zed/pull/42))",
      key: "Fixes\nFixed the visible issue. ([#42](https://github.com/zed-industries/zed/pull/42))",
    },
  );

  const linked = formatCommitEntry({
    hash: "abc123",
    pr: "42",
    firstLine: "fix: ignored",
    body: "Release Notes:\n\n- Fixed the issue ([#42](https://github.com/zed-industries/zed/pull/42)).",
  });
  assert.equal(linked.text, "Fixed the issue ([#42](https://github.com/zed-industries/zed/pull/42)).");
});

test("renders ordered, deduplicated sections", () => {
  const entries = [
    { section: "Fixes", text: "Fixed a crash", key: "fix" },
    { section: "New", text: "Added a panel", key: "new" },
    { section: "New", text: "Added a panel", key: "new" },
    { section: "Improvements", text: "Improved startup", key: "improvement" },
  ];

  assert.equal(
    renderReleaseNotes(entries, "https://example.test/compare"),
    "## New\n\n- Added a panel\n\n## Improvements\n\n- Improved startup\n\n## Fixes\n\n- Fixed a crash\n",
  );
});

test("renders a useful no-public-changes message", () => {
  assert.equal(
    renderReleaseNotes([], "https://example.test/compare"),
    "No public-facing changes in this release. [View the commits](https://example.test/compare).\n",
  );
});
