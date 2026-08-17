use crate::Oid;
use anyhow::{Context, Result, anyhow};
use std::{fmt, str::FromStr, sync::Arc};

/// The ref that backs the stash reflog.
pub const STASH_REF: &str = "refs/stash";

/// Namespace prefix under which a crash-recoverable stash rename keeps its
/// versioned manifest ref plus one stable recovery ref per involved OID. Any
/// ref under this prefix marks an unfinished rename that the next repository
/// refresh must discover and expose for retry/recover/cleanup.
pub const STASH_RENAME_RECOVERY_PREFIX: &str = "refs/zed-git/stash-rename";

/// Version of the manifest blob a stash rename writes before its destructive
/// replay, so recovery tooling can evolve the format without losing old runs.
pub const STASH_RENAME_MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StashEntry {
    pub index: usize,
    pub oid: Oid,
    pub message: String,
    pub branch: Option<String>,
    pub timestamp: i64,
}

/// A transparent reference to a single stash entry, captured when the user
/// selects it. Not authoritative at mutation time: backends refresh `refs/stash`
/// and re-resolve exactly one current entry from this composite, so mutations
/// keep following the entry even when indices move or entries share one OID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct StashIdentity {
    /// Exact commit OID of the selected entry.
    pub oid: Oid,
    /// Reflog ref name (always `refs/stash`).
    pub ref_name: String,
    /// Explicit, fully-qualified reflog selector captured at selection time
    /// (for example `refs/stash@{2}`). The backend uses it only as a hint: it
    /// refreshes `refs/stash` at mutation time and re-resolves a fresh selector,
    /// following the entry even when indices move. Destructive drops use only
    /// the freshly resolved selector, never this captured one.
    pub selector: String,
}

impl StashIdentity {
    pub fn for_entry(entry: &StashEntry) -> Self {
        Self {
            oid: entry.oid,
            ref_name: STASH_REF.to_string(),
            selector: format!("{STASH_REF}@{{{}}}", entry.index),
        }
    }
}

/// The result of a stash mutation that can succeed partially.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StashMutationResult {
    /// The mutation completed fully (pop applied and dropped the entry).
    Success,
    /// Pop applied the entry but the follow-up drop failed; the stash is retained.
    AppliedButRetained,
}

/// Why a composite stash identity could not be resolved against a fresh reflog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StashResolveError {
    /// No current entry matches; the stash was likely dropped or never existed.
    Missing(String),
    /// More than one current entry matches and the selector hint cannot disambiguate.
    Ambiguous(String),
}

impl fmt::Display for StashResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StashResolveError::Missing(detail) => write!(f, "stash not found: {detail}"),
            StashResolveError::Ambiguous(detail) => write!(f, "stash is ambiguous: {detail}"),
        }
    }
}

impl std::error::Error for StashResolveError {}

/// Manifests and stable recovery refs a stash rename retains if any destructive
/// step (replay, verification, or cleanup) fails or is interrupted. They stay
/// in the repository so the operation can be discovered after a restart and
/// retried, recovered, or cleaned up; never delete them blindly.
#[derive(Clone, Debug, PartialEq)]
pub struct StashRenameRecovery {
    /// Fully-qualified ref referencing the versioned manifest blob.
    pub manifest_ref: String,
    /// Stable recovery refs protecting every involved stash OID plus the new
    /// rewritten OID from garbage collection.
    pub recovery_refs: Vec<String>,
    /// The observable stash stack (newest first) after the failure, so a caller
    /// can judge whether the rename applied partial.
    pub observed_entries: Vec<StashEntry>,
    /// True when the rename applied and verified but recovery cleanup failed;
    /// false when the rename itself did not complete.
    pub rename_applied: bool,
}

/// The outcome of a crash-recoverable stash rename.
#[derive(Clone, Debug, PartialEq)]
pub enum StashRenameResult {
    /// The rename applied, verified, and the recovery refs were cleaned up.
    Success,
    /// The rename applied and verified, but deleting the recovery refs failed;
    /// they are retained and reported for cleanup.
    SuccessWithRecoveryRefs(StashRenameRecovery),
    /// The rename did not complete (a destructive replay, concurrent-mutation,
    /// verification, or cleanup failure). The manifest + recovery refs are
    /// retained and reported for retry/recover/cleanup.
    FailedWithRecovery(StashRenameRecovery),
}

/// Rebuild the full stash subject line for a renamed entry, preserving the
/// original branch prefix (`On <branch>: ...` / `WIP on <branch>: ...`) so the
/// entry keeps reading like the stash Git reports, and falling back to a bare
/// message when the original had no prefix.
pub fn renamed_stash_subject(original_subject: &str, new_message: &str) -> String {
    if let Some(rest) = original_subject.strip_prefix("WIP on ")
        && let Some(colon) = rest.find(": ")
    {
        let branch = &rest[..colon];
        format!("WIP on {branch}: {new_message}")
    } else if let Some(rest) = original_subject.strip_prefix("On ")
        && let Some(colon) = rest.find(": ")
    {
        let branch = &rest[..colon];
        format!("On {branch}: {new_message}")
    } else {
        new_message.to_string()
    }
}

/// Resolve exactly one current stash entry from a captured identity against a
/// fresh reflog snapshot (`entries`). Returns the current entry, or
/// `StashResolveError::Missing`/`Ambiguous` when zero or multiple entries match.
pub fn resolve_stash_identity<'a>(
    entries: &'a [StashEntry],
    identity: &StashIdentity,
) -> Result<&'a StashEntry, StashResolveError> {
    if identity.ref_name != STASH_REF {
        return Err(StashResolveError::Missing(format!(
            "unexpected stash ref '{}'",
            identity.ref_name
        )));
    }

    // Resolve by the captured reflog selector first: the entry currently at that
    // selector, provided its OID still matches the captured one. This is the
    // authoritative hint and pins the exact selected entry even when another
    // entry shares the same commit OID.
    if let Some(selector_index) = parse_stash_selector_index(&identity.selector) {
        if let Some(entry) = entries.get(selector_index)
            && entry.oid == identity.oid
        {
            return Ok(entry);
        }
    }

    // Fall back to matching purely by OID. This handles reordered stacks where
    // the selected entry moved to a different index.
    let mut matching = entries.iter().filter(move |entry| entry.oid == identity.oid);
    match (matching.next(), matching.next()) {
        // The entry is unique, so the stack has reordered since selection.
        (Some(first), None) => Ok(first),
        // Several current entries claim the same commit and the selector hint
        // cannot disambiguate: refuse the mutation rather than guess.
        (Some(_), Some(_)) => Err(StashResolveError::Ambiguous(format!(
            "multiple entries reference commit {}",
            identity.oid
        ))),
        (None, _) => Err(StashResolveError::Missing(format!(
            "no entry references commit {}",
            identity.oid
        ))),
    }
}

/// Parse the index out of an explicit fully-qualified reflog selector such as
/// `refs/stash@{2}`. Returns `None` for a malformed selector.
fn parse_stash_selector_index(selector: &str) -> Option<usize> {
    let selector = selector.strip_prefix(&format!("{STASH_REF}@{{"))?;
    let selector = selector.strip_suffix('}')?;
    selector.parse().ok()
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct GitStash {
    pub entries: Arc<[StashEntry]>,
}

impl GitStash {
    pub fn apply(&mut self, other: GitStash) {
        self.entries = other.entries;
    }
}

impl FromStr for GitStash {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.trim().is_empty() {
            return Ok(Self::default());
        }

        let mut entries = Vec::new();
        let mut errors = Vec::new();

        for (line_num, line) in s.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }

            match parse_stash_line(line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    errors.push(format!("Line {}: {}", line_num + 1, e));
                }
            }
        }

        // If we have some valid entries but also some errors, log the errors but continue
        if !errors.is_empty() && !entries.is_empty() {
            log::warn!("Failed to parse some stash entries: {}", errors.join(", "));
        } else if !errors.is_empty() {
            return Err(anyhow!(
                "Failed to parse stash entries: {}",
                errors.join(", ")
            ));
        }

        Ok(Self {
            entries: entries.into(),
        })
    }
}

/// Parse a single stash line in the format: "stash@{N}\0<oid>\0<timestamp>\0<message>"
fn parse_stash_line(line: &str) -> Result<StashEntry> {
    let parts: Vec<&str> = line.splitn(4, '\0').collect();

    if parts.len() != 4 {
        return Err(anyhow!(
            "Expected 4 null-separated parts, got {}",
            parts.len()
        ));
    }

    let index = parse_stash_index(parts[0])
        .with_context(|| format!("Failed to parse stash index from '{}'", parts[0]))?;

    let oid = Oid::from_str(parts[1])
        .with_context(|| format!("Failed to parse OID from '{}'", parts[1]))?;

    let timestamp = parts[2]
        .parse::<i64>()
        .with_context(|| format!("Failed to parse timestamp from '{}'", parts[2]))?;

    let (branch, message) = parse_stash_message(parts[3]);

    Ok(StashEntry {
        index,
        oid,
        message: message.to_string(),
        branch: branch.map(Into::into),
        timestamp,
    })
}

/// Parse stash index from format "stash@{N}" where N is the index
pub(crate) fn parse_stash_index(input: &str) -> Result<usize> {
    let trimmed = input.trim();

    if !trimmed.starts_with("stash@{") || !trimmed.ends_with('}') {
        return Err(anyhow!(
            "Invalid stash index format: expected 'stash@{{N}}'"
        ));
    }

    let index_str = trimmed
        .strip_prefix("stash@{")
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| anyhow!("Failed to extract index from stash reference"))?;

    index_str
        .parse::<usize>()
        .with_context(|| format!("Invalid stash index number: '{}'", index_str))
}

/// Parse stash message and extract branch information if present
///
/// Handles the following formats:
/// - "WIP on <branch>: <message>" -> (Some(branch), message)
/// - "On <branch>: <message>" -> (Some(branch), message)
/// - "<message>" -> (None, message)
pub(crate) fn parse_stash_message(input: &str) -> (Option<&str>, &str) {
    // Handle "WIP on <branch>: <message>" pattern
    if let Some(stripped) = input.strip_prefix("WIP on ")
        && let Some(colon_pos) = stripped.find(": ")
    {
        let branch = &stripped[..colon_pos];
        let message = &stripped[colon_pos + 2..];
        if !branch.is_empty() && !message.is_empty() {
            return (Some(branch), message);
        }
    }

    // Handle "On <branch>: <message>" pattern
    if let Some(stripped) = input.strip_prefix("On ")
        && let Some(colon_pos) = stripped.find(": ")
    {
        let branch = &stripped[..colon_pos];
        let message = &stripped[colon_pos + 2..];
        if !branch.is_empty() && !message.is_empty() {
            return (Some(branch), message);
        }
    }

    // Fallback: treat entire input as message with no branch
    (None, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stash_index() {
        assert_eq!(parse_stash_index("stash@{0}").unwrap(), 0);
        assert_eq!(parse_stash_index("stash@{42}").unwrap(), 42);
        assert_eq!(parse_stash_index("  stash@{5}  ").unwrap(), 5);

        assert!(parse_stash_index("invalid").is_err());
        assert!(parse_stash_index("stash@{not_a_number}").is_err());
        assert!(parse_stash_index("stash@{0").is_err());
    }

    #[test]
    fn test_parse_stash_message() {
        // WIP format
        let (branch, message) = parse_stash_message("WIP on main: working on feature");
        assert_eq!(branch, Some("main"));
        assert_eq!(message, "working on feature");

        // On format
        let (branch, message) = parse_stash_message("On feature-branch: some changes");
        assert_eq!(branch, Some("feature-branch"));
        assert_eq!(message, "some changes");

        // No branch format
        let (branch, message) = parse_stash_message("just a regular message");
        assert_eq!(branch, None);
        assert_eq!(message, "just a regular message");

        // Edge cases
        let (branch, message) = parse_stash_message("WIP on : empty message");
        assert_eq!(branch, None);
        assert_eq!(message, "WIP on : empty message");

        let (branch, message) = parse_stash_message("On branch-name:");
        assert_eq!(branch, None);
        assert_eq!(message, "On branch-name:");
    }

    #[test]
    fn test_parse_stash_line() {
        let line = "stash@{0}\u{0000}abc123\u{0000}1234567890\u{0000}WIP on main: test commit";
        let entry = parse_stash_line(line).unwrap();

        assert_eq!(entry.index, 0);
        assert_eq!(entry.message, "test commit");
        assert_eq!(entry.branch, Some("main".to_string()));
        assert_eq!(entry.timestamp, 1234567890);
    }

    #[test]
    fn test_git_stash_from_str() {
        let input = "stash@{0}\u{0000}abc123\u{0000}1234567890\u{0000}WIP on main: first stash\nstash@{1}\u{0000}def456\u{0000}1234567891\u{0000}On feature: second stash";
        let stash = GitStash::from_str(input).unwrap();

        assert_eq!(stash.entries.len(), 2);
        assert_eq!(stash.entries[0].index, 0);
        assert_eq!(stash.entries[0].branch, Some("main".to_string()));
        assert_eq!(stash.entries[1].index, 1);
        assert_eq!(stash.entries[1].branch, Some("feature".to_string()));
    }

    #[test]
    fn test_git_stash_empty_input() {
        let stash = GitStash::from_str("").unwrap();
        assert_eq!(stash.entries.len(), 0);

        let stash = GitStash::from_str("   \n  \n  ").unwrap();
        assert_eq!(stash.entries.len(), 0);
    }

    fn entry(index: usize, oid: Oid) -> StashEntry {
        StashEntry {
            index,
            oid,
            message: format!("stash #{index}"),
            branch: None,
            timestamp: 0,
        }
    }

    fn oid(n: u8) -> Oid {
        Oid::from_str(&format!("{:040x}", n)).unwrap()
    }

    #[test]
    fn test_stash_identity_for_entry() {
        let entry = entry(2, oid(7));
        let identity = StashIdentity::for_entry(&entry);
        assert_eq!(identity.oid, oid(7));
        assert_eq!(identity.ref_name, STASH_REF);
        assert_eq!(identity.selector, "refs/stash@{2}");
    }

    #[test]
    fn test_parse_stash_selector_index() {
        assert_eq!(parse_stash_selector_index("refs/stash@{0}"), Some(0));
        assert_eq!(parse_stash_selector_index("refs/stash@{42}"), Some(42));
        assert_eq!(parse_stash_selector_index("refs/heads/main@{1}"), None);
        assert_eq!(parse_stash_selector_index("refs/stash@{}"), None);
        assert_eq!(parse_stash_selector_index("refs/stash@{x}"), None);
        assert_eq!(parse_stash_selector_index("stash@{1}"), None);
    }

    #[test]
    fn test_renamed_stash_subject_preserves_branch_prefix() {
        assert_eq!(
            renamed_stash_subject("On main: old message", "new message"),
            "On main: new message"
        );
        assert_eq!(
            renamed_stash_subject("WIP on feature: old", "renamed"),
            "WIP on feature: renamed"
        );
        assert_eq!(
            renamed_stash_subject("plain message", "fresh"),
            "fresh"
        );
        // An empty-branch "On main:" has no recognized prefix
        // (parse_stash_message treats it as bare), so the rename is bare.
        assert_eq!(renamed_stash_subject("On main:", "bare"), "bare");
    }

    #[test]
    fn test_resolve_identity_exact_position_match() {
        let entries = vec![entry(0, oid(1)), entry(1, oid(2))];
        let identity = StashIdentity::for_entry(&entries[1]);
        let resolved = resolve_stash_identity(&entries, &identity).unwrap();
        assert_eq!(resolved.index, 1);
        assert_eq!(resolved.oid, oid(2));
    }

    #[test]
    fn test_resolve_identity_follows_reordering_by_oid() {
        // The entry originally at index 1 (commit 2) was pushed to index 2 by an
        // externally inserted stash; resolution must follow the OID.
        let entries = vec![entry(0, oid(9)), entry(1, oid(1)), entry(2, oid(2))];
        let identity = StashIdentity {
            oid: oid(2),
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{1}".to_string(),
        };
        let resolved = resolve_stash_identity(&entries, &identity).unwrap();
        assert_eq!(resolved.index, 2);
        assert_eq!(resolved.oid, oid(2));
    }

    #[test]
    fn test_resolve_identity_disambiguates_duplicate_oids_by_position() {
        // Two entries share commit 3; a hint index pins the exact selected one.
        let entries = vec![entry(0, oid(1)), entry(1, oid(3)), entry(2, oid(3))];
        let identity = StashIdentity {
            oid: oid(3),
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{2}".to_string(),
        };
        let resolved = resolve_stash_identity(&entries, &identity).unwrap();
        assert_eq!(resolved.index, 2);
    }

    #[test]
    fn test_resolve_identity_missing() {
        let entries = vec![entry(0, oid(1))];
        let identity = StashIdentity {
            oid: oid(99),
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{0}".to_string(),
        };
        let err = resolve_stash_identity(&entries, &identity).unwrap_err();
        assert_eq!(
            err,
            StashResolveError::Missing(format!(
                "no entry references commit {}",
                oid(99)
            ))
        );
    }

    #[test]
    fn test_resolve_identity_ambiguous_duplicates_after_reorder() {
        // Duplicate OIDs AND a reordered stack means the hint cannot disambiguate.
        let entries = vec![entry(0, oid(3)), entry(1, oid(3))];
        let identity = StashIdentity {
            oid: oid(3),
            ref_name: STASH_REF.to_string(),
            selector: "refs/stash@{4}".to_string(),
        };
        let err = resolve_stash_identity(&entries, &identity).unwrap_err();
        assert!(matches!(err, StashResolveError::Ambiguous(_)));
    }

    #[test]
    fn test_resolve_identity_rejects_wrong_ref() {
        let entries = vec![entry(0, oid(1))];
        let identity = StashIdentity {
            oid: oid(1),
            ref_name: "refs/heads/main".to_string(),
            selector: "refs/heads/main@{0}".to_string(),
        };
        assert!(resolve_stash_identity(&entries, &identity).is_err());
    }
}
