use std::collections::BTreeMap;

use anyhow::Context as _;
use db::kvp::KeyValueStore;
use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Serialize,
};

use crate::herdr_client::HerdrAgentSessionIdentity;
use crate::thread_metadata_store::ThreadId;

/// Dedicated `scoped_kv_store` namespace for Herdr thread mappings. One
/// serialized record map per Herdr session is stored under this namespace,
/// keyed by the session name.
pub(crate) const HERDR_MAPPING_NAMESPACE: &str = "herdr_thread_mappings";

/// Format version of the serialized per-session map. Bump when the record
/// shape changes in a way older readers must reject rather than misread.
const SESSION_MAP_FORMAT_VERSION: u32 = 1;

/// Lifecycle of a mapped Herdr resource. Closed records are retained as
/// tombstones: they keep rejecting late events instead of disappearing, so
/// stale data cannot resurrect a resource the user closed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HerdrLifecycleState {
    #[default]
    Active,
    Archived,
    Closed,
}

impl HerdrLifecycleState {
    pub(crate) fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
}

/// Session-qualified identity of one mapped Herdr resource. Every key carries
/// the Herdr session: `workspace_id` and `pane_id` are only unique within a
/// session, so keys from different sessions never collide.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub(crate) struct HerdrMappingKey {
    pub session: String,
    pub workspace_id: String,
    pub pane_id: Option<String>,
    pub agent_session: Option<HerdrAgentSessionIdentity>,
}

impl HerdrMappingKey {
    /// Key for a Herdr workspace mapped to a Zed root thread.
    pub(crate) fn workspace(session: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            workspace_id: workspace_id.into(),
            pane_id: None,
            agent_session: None,
        }
    }

    /// Key for a recognized agent pane mapped to a Zed Herdr-backed subthread.
    pub(crate) fn subthread(
        session: impl Into<String>,
        workspace_id: impl Into<String>,
        pane_id: impl Into<String>,
        agent_session: HerdrAgentSessionIdentity,
    ) -> Self {
        Self {
            session: session.into(),
            workspace_id: workspace_id.into(),
            pane_id: Some(pane_id.into()),
            agent_session: Some(agent_session),
        }
    }

    /// Canonical persisted form of the key. Length-prefixing is injective for
    /// arbitrary UTF-8 identifiers and cannot fail on this data-only type.
    pub(crate) fn to_key_string(&self) -> String {
        fn append_component(target: &mut String, value: &str) {
            target.push_str(&value.len().to_string());
            target.push(':');
            target.push_str(value);
        }

        let mut encoded = String::new();
        encoded.push_str("v1|");
        append_component(&mut encoded, &self.session);
        append_component(&mut encoded, &self.workspace_id);
        match &self.pane_id {
            Some(pane_id) => {
                encoded.push('1');
                append_component(&mut encoded, pane_id);
            }
            None => encoded.push('0'),
        }
        match &self.agent_session {
            Some(agent_session) => {
                encoded.push('1');
                append_component(&mut encoded, &agent_session.kind);
                append_component(&mut encoded, &agent_session.value);
            }
            None => encoded.push('0'),
        }
        encoded
    }
}

/// Durable relationship between one Herdr resource and its Zed counterpart.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct HerdrMappingRecord {
    pub key: HerdrMappingKey,
    pub zed_root_thread_id: ThreadId,
    #[serde(default)]
    pub zed_subthread_session_id: Option<String>,
    /// Stored for diagnostics only. Worktree/cwd identity NEVER participates
    /// in matching: an equal cwd must not silently merge two mappings.
    #[serde(default)]
    pub worktree_or_cwd_identity: Option<String>,
    #[serde(default)]
    pub last_seen_sequence: u64,
    #[serde(default)]
    pub lifecycle: HerdrLifecycleState,
}

impl HerdrMappingRecord {
    pub(crate) fn root(
        session: impl Into<String>,
        workspace_id: impl Into<String>,
        thread_id: ThreadId,
    ) -> Self {
        Self {
            key: HerdrMappingKey::workspace(session, workspace_id),
            zed_root_thread_id: thread_id,
            zed_subthread_session_id: None,
            worktree_or_cwd_identity: None,
            last_seen_sequence: 0,
            lifecycle: HerdrLifecycleState::Active,
        }
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.lifecycle.is_closed()
    }
}

/// All mapping records of one Herdr session, keyed by the canonical key string.
pub(crate) type SessionMappings = BTreeMap<String, HerdrMappingRecord>;

#[derive(Clone, Debug)]
struct StrictRecords(BTreeMap<String, HerdrMappingRecord>);

impl Serialize for StrictRecords {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StrictRecords {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RecordsVisitor;

        impl<'de> Visitor<'de> for RecordsVisitor {
            type Value = StrictRecords;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a map of canonical Herdr mapping keys to records")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut records = BTreeMap::new();
                while let Some(stored_key) = access.next_key::<String>()? {
                    let record = access.next_value::<HerdrMappingRecord>()?;
                    if records.insert(stored_key.clone(), record).is_some() {
                        return Err(de::Error::custom(format!(
                            "duplicate Herdr mapping index key {stored_key:?}"
                        )));
                    }
                }
                Ok(StrictRecords(records))
            }
        }

        deserializer.deserialize_map(RecordsVisitor)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SerializedSessionMap {
    version: u32,
    records: StrictRecords,
}


fn validate_key_shape(key: &HerdrMappingKey) -> anyhow::Result<()> {
    match (&key.pane_id, &key.agent_session) {
        (None, None) | (Some(_), Some(_)) => Ok(()),
        (None, Some(_)) => anyhow::bail!(
            "Herdr mapping key for workspace {:?} has an agent identity without a pane",
            key.workspace_id
        ),
        (Some(_), None) => anyhow::bail!(
            "Herdr mapping key for workspace {:?} has a pane without an agent identity",
            key.workspace_id
        ),
    }
}

fn validate_record_index(stored_key: &str, record: &HerdrMappingRecord) -> anyhow::Result<()> {
    validate_key_shape(&record.key)?;
    let canonical = record.key.to_key_string();
    if stored_key != canonical {
        anyhow::bail!(
            "Herdr mapping record is stored under noncanonical key {:?}; expected {:?}",
            stored_key,
            canonical
        );
    }
    Ok(())
}

fn validate_records_index(mappings: &SessionMappings) -> anyhow::Result<()> {
    for (stored_key, record) in mappings {
        validate_record_index(stored_key, record)?;
    }
    Ok(())
}

/// Encodes one session's map into the value stored under
/// `(HERDR_MAPPING_NAMESPACE, session)` in `scoped_kv_store`.
pub(crate) fn encode_session_map(mappings: &SessionMappings) -> anyhow::Result<String> {
    validate_records_index(mappings)?;
    let envelope = SerializedSessionMap {
        version: SESSION_MAP_FORMAT_VERSION,
        records: StrictRecords(mappings.clone()),
    };
    serde_json::to_string(&envelope).context("Failed to serialize Herdr session mappings")
}

/// Decodes a stored session map. A missing value decodes to an empty map;
/// an unknown format version or corrupt record index is rejected instead of
/// being silently ignored.
pub(crate) fn decode_session_map(stored: Option<&str>) -> anyhow::Result<SessionMappings> {
    let Some(stored) = stored else {
        return Ok(BTreeMap::new());
    };
    if stored.trim().is_empty() {
        anyhow::bail!("Present Herdr session mapping payload is blank");
    }
    let envelope: SerializedSessionMap =
        serde_json::from_str(stored).context("Malformed Herdr session mapping payload")?;
    if envelope.version != SESSION_MAP_FORMAT_VERSION {
        anyhow::bail!(
            "Unsupported Herdr mapping format version {} (expected {})",
            envelope.version,
            SESSION_MAP_FORMAT_VERSION
        );
    }

    let mut records = BTreeMap::new();
    for (stored_key, record) in envelope.records.0 {
        validate_record_index(&stored_key, &record)?;
        let canonical = record.key.to_key_string();
        if records.insert(canonical.clone(), record).is_some() {
            anyhow::bail!("Duplicate Herdr mapping record for canonical key {canonical:?}");
        }
    }
    Ok(records)
}

/// Inserts or replaces a record. An active record never implicitly overwrites
/// a closed tombstone (that would resurrect a closed resource); callers that
/// genuinely re-open it go through an explicit restoration. Records with an
/// older sequence are also rejected so persistence cannot move a fence
/// backwards. Returns whether the map changed.
pub(crate) fn upsert_record(mappings: &mut SessionMappings, record: HerdrMappingRecord) -> bool {
    if validate_key_shape(&record.key).is_err() {
        return false;
    }
    let key_string = record.key.to_key_string();
    match mappings.get(&key_string) {
        Some(existing) if existing.is_tombstone() && !record.lifecycle.is_closed() => false,
        Some(existing) if record.last_seen_sequence < existing.last_seen_sequence => false,
        Some(existing) if existing == &record => false,
        _ => {
            mappings.insert(key_string, record);
            true
        }
    }
}

/// Marks a record closed while keeping it in the map as a tombstone. Returns
/// the tombstoned record, or `None` when no live record exists for the key or
/// the sequence is missing/stale.
pub(crate) fn tombstone_record(
    mappings: &mut SessionMappings,
    key: &HerdrMappingKey,
    sequence: u64,
) -> Option<HerdrMappingRecord> {
    if sequence == 0 {
        return None;
    }
    let key_string = key.to_key_string();
    let record = mappings.get_mut(&key_string)?;
    if record.is_tombstone() || sequence <= record.last_seen_sequence {
        return None;
    }
    record.lifecycle = HerdrLifecycleState::Closed;
    record.last_seen_sequence = sequence;
    Some(record.clone())
}



/// Live (non-tombstoned) records in insertion-stable canonical order.
pub(crate) fn live_records(mappings: &SessionMappings) -> Vec<&HerdrMappingRecord> {
    mappings.values().filter(|r| !r.is_tombstone()).collect()
}

/// Persists and loads per-session mapping maps through Zed's existing
/// cross-platform key-value store. Each Herdr session owns exactly one
/// serialized map that is atomically replaced on write; no schema migration
/// is involved.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HerdrMappingStore;

impl HerdrMappingStore {
    pub(crate) fn load_session(
        store: &KeyValueStore,
        session: &str,
    ) -> anyhow::Result<SessionMappings> {
        let scoped = store.scoped(HERDR_MAPPING_NAMESPACE);
        let stored = scoped
            .read(session)
            .with_context(|| format!("Failed to load Herdr mappings for session {session:?}"))?;
        let mappings = decode_session_map(stored.as_deref())?;
        validate_session_records(session, &mappings)?;
        Ok(mappings)
    }

    /// Atomically replaces the stored map for `session` with `mappings` in
    /// this module's dedicated namespace.
    pub(crate) async fn save_session(
        store: &KeyValueStore,
        session: &str,
        mappings: &SessionMappings,
    ) -> anyhow::Result<()> {
        validate_session_records(session, mappings)?;
        let encoded = encode_session_map(mappings)?;
        let scoped = store.scoped(HERDR_MAPPING_NAMESPACE);
        scoped
            .write(session.to_string(), encoded)
            .await
            .with_context(|| format!("Failed to save Herdr mappings for session {session:?}"))
    }
}

fn validate_session_records(session: &str, mappings: &SessionMappings) -> anyhow::Result<()> {
    validate_records_index(mappings)?;
    for record in mappings.values() {
        if record.key.session != session {
            anyhow::bail!(
                "Herdr mapping session mismatch: map for {session:?} contains record for {:?}",
                record.key.session
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root_mapping(session: &str, workspace_id: &str) -> HerdrMappingRecord {
        HerdrMappingRecord::root(session, workspace_id, ThreadId::new())
    }

    fn subthread_mapping(
        session: &str,
        workspace_id: &str,
        pane_id: &str,
        agent_value: &str,
    ) -> HerdrMappingRecord {
        HerdrMappingRecord {
            key: HerdrMappingKey::subthread(
                session,
                workspace_id,
                pane_id,
                HerdrAgentSessionIdentity::id(agent_value),
            ),
            zed_root_thread_id: ThreadId::new(),
            zed_subthread_session_id: Some(format!("subthread-{agent_value}")),
            worktree_or_cwd_identity: None,
            last_seen_sequence: 0,
            lifecycle: HerdrLifecycleState::Active,
        }
    }

    #[test]
    fn same_workspace_id_in_different_sessions_never_collides() {
        let first = HerdrMappingKey::workspace("alpha", "w1");
        let second = HerdrMappingKey::workspace("beta", "w1");
        assert_ne!(first, second);
        assert_ne!(first.to_key_string(), second.to_key_string());
    }

    #[test]
    fn subthread_keys_differ_by_pane_and_agent_session() {
        let base = HerdrMappingKey::subthread("s", "w1", "p1", HerdrAgentSessionIdentity::id("a1"));
        let other_pane =
            HerdrMappingKey::subthread("s", "w1", "p2", HerdrAgentSessionIdentity::id("a1"));
        let other_agent =
            HerdrMappingKey::subthread("s", "w1", "p1", HerdrAgentSessionIdentity::id("a2"));
        let other_kind =
            HerdrMappingKey::subthread("s", "w1", "p1", HerdrAgentSessionIdentity::path("/a1"));
        assert_ne!(base, other_pane);
        assert_ne!(base, other_agent);
        assert_ne!(base, other_kind);
        assert_ne!(base, HerdrMappingKey::workspace("s", "w1"));
    }

    #[test]
    fn session_map_round_trips_through_serialization() {
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root_mapping("alpha", "w1"));
        upsert_record(
            &mut mappings,
            subthread_mapping("alpha", "w1", "p1", "agent-session-1"),
        );
        let encoded = encode_session_map(&mappings).unwrap();
        let decoded = decode_session_map(Some(&encoded)).unwrap();
        assert_eq!(decoded.len(), 2);
        for (key, record) in &mappings {
            assert_eq!(decoded.get(key), Some(record));
        }
    }

    #[test]
    fn missing_session_decodes_to_empty_map_and_bad_payload_is_rejected() {
        assert!(decode_session_map(None).unwrap().is_empty());
        assert!(decode_session_map(Some("")).is_err());
        assert!(decode_session_map(Some("not json")).is_err());
        let wrong_version = r#"{"version":9999,"records":{}}"#;
        assert!(decode_session_map(Some(wrong_version)).is_err());
    }

    #[test]
    fn tombstones_are_kept_not_deleted() {
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root_mapping("alpha", "w1"));
        let key = HerdrMappingKey::workspace("alpha", "w1");
        let tombstoned = tombstone_record(&mut mappings, &key, 42).unwrap();
        assert!(tombstoned.is_tombstone());
        assert_eq!(tombstoned.last_seen_sequence, 42);
        // The record survives with its thread id so late events can be
        // rejected and diagnostics remain possible.
        assert!(mappings.contains_key(&key.to_key_string()));
        assert_eq!(live_records(&mappings).len(), 0);
    }

    #[test]
    fn upsert_never_implicitly_resurrects_a_tombstone() {
        let mut mappings = SessionMappings::new();
        let original = root_mapping("alpha", "w1");
        upsert_record(&mut mappings, original.clone());
        tombstone_record(&mut mappings, &original.key, 7);

        let mut resurrection = original.clone();
        resurrection.last_seen_sequence = 8;
        assert!(!upsert_record(&mut mappings, resurrection));

        let key_string = original.key.to_key_string();
        let tombstone = mappings.get(&key_string).expect("tombstone remains mapped");
        assert!(tombstone.is_tombstone());
        assert_eq!(tombstone.last_seen_sequence, 7);
    }

    #[test]
    fn tombstoning_an_unknown_or_already_closed_record_is_a_no_op() {
        let mut mappings = SessionMappings::new();
        let key = HerdrMappingKey::workspace("alpha", "w1");
        assert!(tombstone_record(&mut mappings, &key, 1).is_none());
        upsert_record(&mut mappings, root_mapping("alpha", "w1"));
        assert!(tombstone_record(&mut mappings, &key, 2).is_some());
        assert!(tombstone_record(&mut mappings, &key, 3).is_none());
    }

    #[gpui::test]
    async fn store_atomically_replaces_one_session_map() {
        let store = KeyValueStore::open_test_db("herdr_mapping_store_round_trip").await;
        let mut initial = SessionMappings::new();
        upsert_record(&mut initial, root_mapping("alpha", "w1"));
        HerdrMappingStore::save_session(&store, "alpha", &initial)
            .await
            .expect("initial session map persists");

        let mut replacement = SessionMappings::new();
        upsert_record(&mut replacement, root_mapping("alpha", "w2"));
        HerdrMappingStore::save_session(&store, "alpha", &replacement)
            .await
            .expect("replacement session map persists");

        assert_eq!(
            HerdrMappingStore::load_session(&store, "alpha")
                .expect("session map loads"),
            replacement
        );
        assert!(HerdrMappingStore::load_session(&store, "beta")
            .expect("unseen session loads empty")
            .is_empty());
    }

    #[test]
    fn session_map_rejects_records_from_another_session() {
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root_mapping("beta", "w1"));
        assert!(validate_session_records("alpha", &mappings).is_err());
    }
    
    #[test]
    fn present_blank_session_payload_is_rejected() {
        assert!(decode_session_map(Some(" \n\t")).is_err());
    }

    #[test]
    fn noncanonical_duplicate_and_invalid_shape_records_are_rejected() {
        let record = root_mapping("alpha", "w1");
        let canonical = record.key.to_key_string();
        let noncanonical = serde_json::json!({
            "version": 1,
            "records": {
                "wrong-key": record,
            }
        });
        assert!(decode_session_map(Some(&noncanonical.to_string())).is_err());

        let duplicate = serde_json::json!({
            "version": 1,
            "records": {
                canonical.clone(): record.clone(),
                "another-key": record,
            }
        });
        assert!(decode_session_map(Some(&duplicate.to_string())).is_err());

        let invalid = HerdrMappingRecord {
            key: HerdrMappingKey {
                session: "alpha".into(),
                workspace_id: "w1".into(),
                pane_id: Some("p1".into()),
                agent_session: None,
            },
            ..root_mapping("alpha", "w1")
        };
        let invalid_map = serde_json::json!({
            "version": 1,
            "records": {
                invalid.key.to_key_string(): invalid,
            }
        });
        assert!(decode_session_map(Some(&invalid_map.to_string())).is_err());
        
        let invalid_root = HerdrMappingRecord {
            key: HerdrMappingKey {
                session: "alpha".into(),
                workspace_id: "w1".into(),
                pane_id: None,
                agent_session: Some(HerdrAgentSessionIdentity::id("agent-1")),
            },
            ..root_mapping("alpha", "w1")
        };
        let invalid_root_map = serde_json::json!({
            "version": 1,
            "records": {
                invalid_root.key.to_key_string(): invalid_root,
            }
        });
        assert!(decode_session_map(Some(&invalid_root_map.to_string())).is_err());

        let duplicate_record = root_mapping("alpha", "w1");
        let duplicate_key = serde_json::to_string(&duplicate_record.key.to_key_string()).unwrap();
        let duplicate_value = serde_json::to_string(&duplicate_record).unwrap();
        let duplicate_raw = format!(
            r#"{{"version":1,"records":{{{duplicate_key}:{duplicate_value},{duplicate_key}:{duplicate_value}}}}}"#
        );
        assert!(decode_session_map(Some(&duplicate_raw)).is_err());
    }

    #[test]
    fn upsert_rejects_a_older_sequence_without_losing_tombstone() {
        let mut mappings = SessionMappings::new();
        let mut current = root_mapping("alpha", "w1");
        current.last_seen_sequence = 7;
        assert!(upsert_record(&mut mappings, current.clone()));
        let mut older = current.clone();
        older.last_seen_sequence = 6;
        older.lifecycle = HerdrLifecycleState::Archived;
        assert!(!upsert_record(&mut mappings, older));
        assert_eq!(
            mappings[&current.key.to_key_string()].last_seen_sequence,
            7
        );
        assert!(!mappings[&current.key.to_key_string()].is_tombstone());
        assert!(tombstone_record(&mut mappings, &current.key, 8).is_some());
        let mut resurrection = current;
        resurrection.last_seen_sequence = 9;
        assert!(!upsert_record(&mut mappings, resurrection));
        assert!(mappings[&HerdrMappingKey::workspace("alpha", "w1").to_key_string()]
            .is_tombstone());
    }
}
