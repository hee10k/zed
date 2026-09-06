use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_ui::{ExternalTerminalThread, TerminalId};
use herdr::{PaneInfo, SessionInfo, WorkspaceInfo};

/// Stable identity of one herdr CLI session. The CLI session directory, not
/// the display name, is the discriminator: a session may be renamed while its
/// directory (and socket) stay the same.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SessionIdentity {
    pub(crate) name: Arc<str>,
    pub(crate) session_dir: Arc<Path>,
}

impl SessionIdentity {
    pub(crate) fn from_info(info: &SessionInfo) -> Self {
        Self {
            name: Arc::from(info.name.as_str()),
            session_dir: Arc::from(info.session_dir.clone()),
        }
    }
}

/// Stable identity of one mirrored herdr agent: the owning session plus the
/// terminal id. Pane ids change as the agent moves between panes; the terminal
/// id never does.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct AgentKey {
    pub(crate) session: SessionIdentity,
    pub(crate) terminal_id: Arc<str>,
}

impl AgentKey {
    pub(crate) fn new(session: SessionIdentity, terminal_id: impl Into<Arc<str>>) -> Self {
        Self {
            session,
            terminal_id: terminal_id.into(),
        }
    }
}

/// The latest mirrored facts about one agent, captured at upsert time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentRecord {
    pub(crate) key: AgentKey,
    pub(crate) workspace_id: Arc<str>,
    pub(crate) pane_id: Arc<str>,
    pub(crate) revision: u64,
    pub(crate) focused: bool,
    pub(crate) checkout_path: Option<PathBuf>,
    pub(crate) effective_cwd: Option<PathBuf>,
    pub(crate) agent_name: Arc<str>,
}

/// Build the structured external terminal command for one live agent.
///
/// The terminal id is the stable identity while the pane id is deliberately
/// read from the latest reducer record, so a moved agent keeps one Zed thread
/// while subsequent opens/focuses target its current pane.
pub(crate) fn external_terminal_spec(
    record: &AgentRecord,
    herdr_program: &Path,
) -> ExternalTerminalThread {
    ExternalTerminalThread {
        identity: format!(
            "herdr:{}:{}",
            record.key.session.session_dir.display(),
            record.key.terminal_id
        )
        .into(),
        executable: herdr_program.to_string_lossy().into_owned(),
        args: vec![
            "--session".into(),
            record.key.session.name.to_string(),
            "agent".into(),
            "attach".into(),
            record.pane_id.to_string(),
        ],
        title: format!("herdr · {}", record.agent_name).into(),
        working_directory: record
            .checkout_path
            .clone()
            .or_else(|| record.effective_cwd.clone())
            .unwrap_or_default(),
    }
}

/// Pure index of live herdr agent keys and their Agent Panel terminal ids.
///
/// The registry owns the window/panel handles around this index. Keeping the
/// identity-to-terminal mapping pure makes close reconciliation deterministic
/// and keeps it independently testable.
pub(crate) struct MirrorIndex<T = TerminalId> {
    entries: HashMap<AgentKey, T>,
}

impl<T> Default for MirrorIndex<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::default(),
        }
    }
}

impl<T: Copy> MirrorIndex<T> {
    pub(crate) fn insert(&mut self, key: AgentKey, terminal_id: T) {
        self.entries.insert(key, terminal_id);
    }

    pub(crate) fn remove(&mut self, key: &AgentKey) -> Option<T> {
        self.entries.remove(key)
    }

    pub(crate) fn get(&self, key: &AgentKey) -> Option<T> {
        self.entries.get(key).copied()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&AgentKey, T)> {
        self.entries.iter().map(|(key, terminal_id)| (key, *terminal_id))
    }

    pub(crate) fn key_for_terminal(&self, terminal_id: T) -> Option<AgentKey>
    where
        T: Eq,
    {
        self.entries
            .iter()
            .find_map(|(key, id)| (*id == terminal_id).then_some(key.clone()))
    }

    pub(crate) fn missing(
        &self,
        mut terminal_present: impl FnMut(T) -> bool,
    ) -> Vec<AgentKey> {
        self.entries
            .iter()
            .filter_map(|(key, terminal_id)| {
                (!terminal_present(*terminal_id)).then_some(key.clone())
            })
            .collect()
    }
}
/// Effects the driver applies after each reduction, in emitted order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AgentSyncEffect {
    Open(AgentRecord),
    PaneMoved { key: AgentKey, pane_id: Arc<str> },
    Focus(AgentKey),
    Forget(AgentKey),
}

/// The identity a focus transition targets. The current pane id is only the
/// outbound RPC address; it never participates in identity or echo bookkeeping.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FocusTarget {
    Workspace {
        session: SessionIdentity,
        workspace_id: Arc<str>,
    },
    Agent(AgentKey),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FocusObservation {
    Echo,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FocusToken(u64);

/// One pending focus transition per target so concurrent workspace and agent
/// requests never overwrite each other. The saturating global generation makes
/// every token unique, so a completion carrying a superseded token is rejected.
pub(crate) struct FocusEcho<T> {
    generation: u64,
    pending: HashMap<T, FocusToken>,
}

impl<T> Default for FocusEcho<T> {
    fn default() -> Self {
        Self {
            generation: 0,
            pending: HashMap::new(),
        }
    }
}

impl<T: Clone + Eq + Hash> FocusEcho<T> {
    pub(crate) fn request(&mut self, target: T) -> FocusToken {
        self.generation = self.generation.saturating_add(1);
        let token = FocusToken(self.generation);
        self.pending.insert(target, token);
        token
    }

    pub(crate) fn has_pending(&self, target: &T) -> bool {
        self.pending.contains_key(target)
    }

    pub(crate) fn is_current(&self, token: FocusToken) -> bool {
        self.pending.values().any(|current| *current == token)
    }

    pub(crate) fn observe(&mut self, target: T) -> FocusObservation {
        if self.pending.remove(&target).is_some() {
            FocusObservation::Echo
        } else {
            FocusObservation::External
        }
    }
}

/// Pure reducer for herdr agent synchronization. No GPUI, no windows, no
/// process handles, no filesystem I/O: everything is computed from the typed
/// `herdr` snapshots and events the driver feeds in.
#[derive(Default)]
pub(crate) struct AgentSyncState {
    /// Latest workspace checkout paths, keyed by (session identity, workspace id).
    workspace_checkouts: HashMap<(SessionIdentity, Arc<str>), Option<PathBuf>>,
    /// Live mirrored agents.
    records: HashMap<AgentKey, AgentRecord>,
    /// Maps (session identity, current pane id) to the owning agent key so
    /// exit events, which carry no terminal id, resolve to their agent.
    pane_lookup: HashMap<(SessionIdentity, Arc<str>), AgentKey>,
    /// Agents whose mirrored terminal was closed locally; reopening is
    /// suppressed until resync or pane exit.
    dismissed: HashSet<AgentKey>,
    /// Last failed mirror-open revision per agent.
    failed_revision: HashMap<AgentKey, u64>,
}

impl AgentSyncState {
    /// Applies a workspace create/update snapshot before panes. Records the
    /// latest checkout path for later upserts; never emits effects by itself.
    pub(crate) fn apply_workspace(
        &mut self,
        session: &SessionIdentity,
        workspace: &WorkspaceInfo,
    ) -> Vec<AgentSyncEffect> {
        self.workspace_checkouts.insert(
            (session.clone(), Arc::from(workspace.workspace_id.as_str())),
            workspace.checkout_path().map(PathBuf::from),
        );
        Vec::new()
    }

    /// Applies the latest pane snapshot for one agent, in revision order.
    ///
    /// Guard rules, in order: panes without an agent or with an empty terminal
    /// id are ignored; revisions at or below the stored revision are ignored;
    /// dismissed agents record the snapshot without emitting any effect.
    /// Otherwise a new key opens, a pane move emits `PaneMoved` (or one
    /// retrying `Open` when the previous open failed), and a false-to-true
    /// focus transition emits `Focus`.
    pub(crate) fn upsert(
        &mut self,
        session: SessionIdentity,
        pane: PaneInfo,
    ) -> Vec<AgentSyncEffect> {
        if pane.agent.is_none() || pane.terminal_id.is_empty() {
            return Vec::new();
        }
        let key = AgentKey::new(session.clone(), Arc::from(pane.terminal_id.as_str()));
        if self
            .records
            .get(&key)
            .is_some_and(|current| current.revision >= pane.revision)
        {
            return Vec::new();
        }
        let workspace_id: Arc<str> = Arc::from(pane.workspace_id.as_str());
        let checkout_path = self
            .workspace_checkouts
            .get(&(session.clone(), workspace_id.clone()))
            .cloned()
            .flatten();
        let record = AgentRecord {
            key: key.clone(),
            workspace_id,
            pane_id: Arc::from(pane.pane_id.as_str()),
            revision: pane.revision,
            focused: pane.focused,
            checkout_path,
            effective_cwd: pane.foreground_cwd.or(pane.cwd).map(PathBuf::from),
            agent_name: Arc::from(pane.agent.as_deref().expect("agent guard above")),
        };
        let previous = self
            .records
            .get(&key)
            .map(|current| (current.pane_id.clone(), current.focused));

        // The pane-to-key lookup always points at the record's current pane id,
        // replacing the previous entry on a pane move.
        if let Some((old_pane_id, _)) = &previous {
            self.pane_lookup
                .remove(&(session.clone(), old_pane_id.clone()));
        }
        self.pane_lookup
            .insert((session.clone(), record.pane_id.clone()), key.clone());

        if self.dismissed.contains(&key) {
            self.records.insert(key, record);
            return Vec::new();
        }

        let mut effects = Vec::new();
        if let Some((old_pane_id, was_focused)) = previous {
            let pane_moved = old_pane_id != record.pane_id;
            let retry_open = self.failed_revision.contains_key(&key);
            if pane_moved && !retry_open {
                effects.push(AgentSyncEffect::PaneMoved {
                    key: key.clone(),
                    pane_id: record.pane_id.clone(),
                });
            }
            if retry_open {
                effects.push(AgentSyncEffect::Open(record.clone()));
            }
            if !was_focused && record.focused {
                effects.push(AgentSyncEffect::Focus(key.clone()));
            }
        } else {
            effects.push(AgentSyncEffect::Open(record.clone()));
        }
        self.records.insert(key, record);
        effects
    }

    /// A herdr pane exited. The pane id resolves to its agent through the
    /// lookup, forgetting the agent even though exit events carry no terminal
    /// id, and clears its dismissal permanently.
    pub(crate) fn exit(
        &mut self,
        session: &SessionIdentity,
        pane_id: &str,
    ) -> Vec<AgentSyncEffect> {
        let Some(key) = self
            .pane_lookup
            .remove(&(session.clone(), Arc::from(pane_id)))
        else {
            return Vec::new();
        };
        vec![self.forget_one(&key)]
    }

    /// A session stopped: forget every live agent it owned.
    pub(crate) fn forget_session(&mut self, session: &SessionIdentity) -> Vec<AgentSyncEffect> {
        let keys: Vec<AgentKey> = self
            .records
            .keys()
            .filter(|key| &key.session == session)
            .cloned()
            .collect();
        keys.into_iter().map(|key| self.forget_one(&key)).collect()
    }

    /// Suppress automatic reopening of one agent until resync or pane exit.
    /// Used when the mirrored terminal is closed locally: the herdr agent
    /// keeps running; we just stop mirroring it.
    pub(crate) fn dismiss(&mut self, key: AgentKey) {
        self.dismissed.insert(key);
    }

    /// Records a failed mirror open. Returns `true` only for the first failure
    /// at this revision so the driver emits exactly one actionable
    /// notification; a later revision or resync permits one new attempt.
    pub(crate) fn record_failure(&mut self, key: &AgentKey, revision: u64) -> bool {
        if self.failed_revision.get(key) == Some(&revision) {
            false
        } else {
            self.failed_revision.insert(key.clone(), revision);
            true
        }
    }

    /// Clears the failed-open record after a successful mirror open.
    pub(crate) fn clear_failure(&mut self, key: &AgentKey) {
        self.failed_revision.remove(key);
    }

    /// Explicit resync: lift dismissal and failed-revision suppression for the
    /// session and replay an `Open` for every live record.
    pub(crate) fn resync(&mut self, session: &SessionIdentity) -> Vec<AgentSyncEffect> {
        self.dismissed.retain(|key| &key.session != session);
        self.failed_revision.retain(|key, _| &key.session != session);
        self.records
            .values()
            .filter(|record| &record.key.session == session)
            .map(|record| AgentSyncEffect::Open(record.clone()))
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    /// Removes every trace of `key` (record, pane lookup, dismissal, failed
    /// revision) and returns its `Forget` effect. `Forget` only drops

    pub(crate) fn record(&self, key: &AgentKey) -> Option<AgentRecord> {
        self.records.get(key).cloned()
    }

    pub(crate) fn record_for_workspace(
        &self,
        session: &SessionIdentity,
        workspace_id: &str,
    ) -> Option<AgentRecord> {
        self.records
            .values()
            .find(|record| {
                &record.key.session == session && record.workspace_id.as_ref() == workspace_id
            })
            .cloned()
    }
    /// synchronization ownership; it never closes the Agent Panel terminal.
    fn forget_one(&mut self, key: &AgentKey) -> AgentSyncEffect {
        if let Some(record) = self.records.remove(key) {
            self.pane_lookup
                .remove(&(record.key.session.clone(), record.pane_id.clone()));
        }
        self.dismissed.remove(key);
        self.failed_revision.remove(key);
        AgentSyncEffect::Forget(key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use herdr::WorkspaceWorktreeInfo;

    fn session(name: &str) -> SessionIdentity {
        SessionIdentity {
            name: Arc::from(name),
            session_dir: Arc::from(PathBuf::from(format!("/sessions/{name}"))),
        }
    }

    fn pane(terminal_id: &str, pane_id: &str, revision: u64) -> PaneInfo {
        PaneInfo {
            workspace_id: "workspace-1".into(),
            tab_id: "tab-1".into(),
            pane_id: pane_id.into(),
            terminal_id: terminal_id.into(),
            focused: false,
            revision,
            agent: Some("claude".into()),
            agent_status: Some("working".into()),
            cwd: Some("/repo/worktree".into()),
            foreground_cwd: None,
            agent_session: None,
        }
    }

    fn test_session() -> SessionIdentity {
        SessionIdentity {
            name: Arc::from("main"),
            session_dir: Arc::from(PathBuf::from(r"C:\sessions\main")),
        }
    }

    fn agent_record(terminal_id: &str, pane_id: &str, worktree: &str) -> AgentRecord {
        AgentRecord {
            key: AgentKey::new(test_session(), terminal_id),
            workspace_id: Arc::from("workspace-1"),
            pane_id: Arc::from(pane_id),
            revision: 4,
            focused: true,
            checkout_path: Some(PathBuf::from(worktree)),
            effective_cwd: Some(PathBuf::from(worktree)),
            agent_name: Arc::from("claude"),
        }
    }

    #[test]
    fn attach_spec_uses_latest_pane_and_structured_args() {
        let record = agent_record("terminal-1", "pane-new", r"C:\repo\worktree");
        let spec = external_terminal_spec(&record, Path::new("herdr.exe"));

        assert_eq!(spec.executable, "herdr.exe");
        assert_eq!(
            spec.args,
            vec!["--session", "main", "agent", "attach", "pane-new"]
        );
        assert_eq!(spec.working_directory, PathBuf::from(r"C:\repo\worktree"));
    }

    #[test]
    fn missing_panel_terminal_returns_the_live_agent_key() {
        let key = AgentKey::new(test_session(), "terminal-1");
        let terminal_id = 7_u64;
        let mut mirrors: MirrorIndex<u64> = MirrorIndex::default();
        mirrors.insert(key.clone(), terminal_id);
        assert_eq!(mirrors.missing(|_| false), vec![key]);
        assert!(mirrors.missing(|id| id == terminal_id).is_empty());
    }

    #[test]
    fn initial_open_creates_one_record() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let effects = state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Open(record)]
                if record.key.terminal_id.as_ref() == "terminal-1"
                    && record.key.session == identity
                    && record.workspace_id.as_ref() == "workspace-1"
                    && record.pane_id.as_ref() == "pane-a"
                    && record.agent_name.as_ref() == "claude"
                    && record.effective_cwd.as_deref() == Some(Path::new("/repo/worktree"))
        ));
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn stale_revision_is_rejected() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        state.upsert(identity.clone(), pane("terminal-1", "pane-a", 5));
        // Older revisions, and even a different pane id at an equal revision,
        // are ignored.
        assert!(state
            .upsert(identity.clone(), pane("terminal-1", "pane-a", 4))
            .is_empty());
        assert!(state
            .upsert(identity.clone(), pane("terminal-1", "pane-b", 5))
            .is_empty());
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn pane_move_keeps_one_agent_by_terminal_identity() {
        let mut state = AgentSyncState::default();
        let first = state.upsert(session("main"), pane("terminal-1", "pane-a", 1));
        assert!(matches!(first.as_slice(), [AgentSyncEffect::Open(_)]));

        let moved = state.upsert(session("main"), pane("terminal-1", "pane-b", 2));
        assert!(matches!(
            moved.as_slice(),
            [AgentSyncEffect::PaneMoved { pane_id, .. }] if pane_id.as_ref() == "pane-b"
        ));
        assert_eq!(state.len(), 1);
    }

    #[test]
    fn false_to_true_focus_transition_emits_focus() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-1");
        assert!(matches!(
            state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1)).as_slice(),
            [AgentSyncEffect::Open(_)]
        ));

        // A false→true transition at a newer revision emits exactly Focus.
        let mut focused = pane("terminal-1", "pane-a", 2);
        focused.focused = true;
        assert!(matches!(
            state.upsert(identity.clone(), focused).as_slice(),
            [AgentSyncEffect::Focus(emitted)] if emitted == &key
        ));

        // A remaining true→true re-update at a newer revision emits nothing.
        let mut still_focused = pane("terminal-1", "pane-a", 3);
        still_focused.focused = true;
        assert!(state.upsert(identity.clone(), still_focused).is_empty());

        // Returning to false at a newer revision also never emits Focus.
        let mut unfocused = pane("terminal-1", "pane-a", 4);
        unfocused.focused = false;
        assert!(state.upsert(identity, unfocused).is_empty());
    }

    #[test]
    fn dismissed_terminal_stays_closed_until_resync() {
        let mut state = AgentSyncState::default();
        let key = AgentKey::new(session("main"), "terminal-1");
        state.dismiss(key.clone());
        assert!(state
            .upsert(session("main"), pane("terminal-1", "pane-a", 3))
            .is_empty());
        assert_eq!(state.resync(&session("main")).len(), 1);
    }

    #[test]
    fn stopped_session_forgets_every_agent() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));

        let effects = state.forget_session(&identity);
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Forget(key)] if key.terminal_id.as_ref() == "terminal-1"
        ));
        assert_eq!(state.len(), 0);
    }

    #[test]
    fn failed_open_retries_once_on_a_new_revision() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-1");
        assert!(matches!(
            state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1)).as_slice(),
            [AgentSyncEffect::Open(_)]
        ));
        assert!(state.record_failure(&key, 1));
        assert!(!state.record_failure(&key, 1));
        assert!(state
            .upsert(identity.clone(), pane("terminal-1", "pane-a", 1))
            .is_empty());
        assert!(matches!(
            state.upsert(identity, pane("terminal-1", "pane-a", 2)).as_slice(),
            [AgentSyncEffect::Open(_)]
        ));
    }

    #[test]
    fn pane_exit_forgets_and_clears_dismissal() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-1");
        state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));
        state.dismiss(key.clone());

        let effects = state.exit(&identity, "pane-a");
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Forget(forgotten)] if forgotten == &key
        ));
        assert_eq!(state.len(), 0);

        // Pane exit clears dismissal permanently, so a later update reopens.
        assert!(matches!(
            state.upsert(identity.clone(), pane("terminal-1", "pane-b", 2)).as_slice(),
            [AgentSyncEffect::Open(_)]
        ));
        // The pane-to-key lookup for the exited pane id is gone.
        assert!(state.exit(&identity, "pane-a").is_empty());
    }

    #[test]
    fn resync_clears_failed_revision_suppression() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let key = AgentKey::new(identity.clone(), "terminal-1");
        state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));
        assert!(state.record_failure(&key, 1));

        // Resync replays an Open for every live record and lifts suppression:
        // the next revision advance is a plain pane move, not a retrying open.
        assert_eq!(state.resync(&identity).len(), 1);
        let moved = state.upsert(identity.clone(), pane("terminal-1", "pane-b", 2));
        assert!(matches!(
            moved.as_slice(),
            [AgentSyncEffect::PaneMoved { .. }]
        ));
    }

    #[test]
    fn upsert_ignores_non_agent_panes() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let mut no_agent = pane("terminal-1", "pane-a", 1);
        no_agent.agent = None;
        assert!(state.upsert(identity.clone(), no_agent).is_empty());
        assert!(state.upsert(identity.clone(), pane("", "pane-a", 1)).is_empty());
        assert_eq!(state.len(), 0);
    }

    #[test]
    fn effective_cwd_prefers_foreground_cwd() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let mut focused_pane = pane("terminal-1", "pane-a", 1);
        focused_pane.foreground_cwd = Some("/repo/foreground".into());
        let effects = state.upsert(identity.clone(), focused_pane);
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Open(record)]
                if record.effective_cwd.as_deref() == Some(Path::new("/repo/foreground"))
        ));
    }

    #[test]
    fn apply_workspace_feeds_checkout_path_into_opens() {
        let mut state = AgentSyncState::default();
        let identity = session("main");
        let workspace = WorkspaceInfo {
            workspace_id: "workspace-1".into(),
            number: 1,
            label: "work".into(),
            focused: true,
            pane_count: 1,
            tab_count: 1,
            active_tab_id: None,
            agent_status: "connected".into(),
            worktree: Some(WorkspaceWorktreeInfo {
                checkout_path: "/repo/worktree".into(),
                repo_root: None,
                repo_key: None,
                repo_name: None,
                is_linked_worktree: false,
            }),
        };
        // Workspace updates record the path but never emit effects by themselves.
        assert!(state.apply_workspace(&identity, &workspace).is_empty());

        let effects = state.upsert(identity.clone(), pane("terminal-1", "pane-a", 1));
        assert!(matches!(
            effects.as_slice(),
            [AgentSyncEffect::Open(record)]
                if record.workspace_id.as_ref() == "workspace-1"
                    && record.checkout_path.as_deref() == Some(Path::new("/repo/worktree"))
        ));

        let updated = WorkspaceInfo {
            worktree: Some(WorkspaceWorktreeInfo {
                checkout_path: "/repo/another".into(),
                repo_root: None,
                repo_key: None,
                repo_name: None,
                is_linked_worktree: false,
            }),
            ..workspace
        };
        assert!(state.apply_workspace(&identity, &updated).is_empty());
    }

    #[test]
    fn reflected_focus_is_acknowledged_once() {
        let mut focus = FocusEcho::default();
        let target = FocusTarget::Workspace {
            session: session("session-a"),
            workspace_id: Arc::from("space-1"),
        };
        let token = focus.request(target.clone());
        assert!(focus.is_current(token));
        assert_eq!(focus.observe(target.clone()), FocusObservation::Echo);
        assert_eq!(focus.observe(target.clone()), FocusObservation::External);
    }

    #[test]
    fn concurrent_focus_transitions_echo_independently() {
        let mut focus = FocusEcho::default();
        let workspace = FocusTarget::Workspace {
            session: session("session-a"),
            workspace_id: Arc::from("space-1"),
        };
        let agent = FocusTarget::Agent(AgentKey::new(session("session-a"), "terminal-1"));

        let workspace_token = focus.request(workspace.clone());
        let _agent_token = focus.request(agent.clone());

        // Each pending transition acknowledges its own target and never a peer.
        assert_eq!(focus.observe(workspace), FocusObservation::Echo);
        assert!(focus.is_current(workspace_token) == false);
        assert_eq!(focus.observe(agent), FocusObservation::Echo);

        // Requesting the same target again supersedes the earlier token, so a
        // stale completion carrying the old token is rejected by the
        // generation; an unrelated target stays External.
        let stale = focus.request(FocusTarget::Workspace {
            session: session("session-a"),
            workspace_id: Arc::from("space-2"),
        });
        let _supersede = focus.request(FocusTarget::Workspace {
            session: session("session-a"),
            workspace_id: Arc::from("space-2"),
        });
        assert_eq!(
            focus.observe(FocusTarget::Workspace {
                session: session("session-a"),
                workspace_id: Arc::from("space-3"),
            }),
            FocusObservation::External
        );
        assert!(!focus.is_current(stale));
    }

    #[test]
    fn identity_from_info_uses_session_dir_as_discriminator() {
        let info = SessionInfo {
            name: "work".to_string(),
            is_default: false,
            running: true,
            session_dir: PathBuf::from("/sessions/work"),
            socket_path: PathBuf::from("/sessions/work.sock"),
        };
        let identity = SessionIdentity::from_info(&info);
        assert_eq!(identity.name.as_ref(), "work");
        assert_eq!(identity.session_dir.as_ref(), Path::new("/sessions/work"));

        // The CLI session directory is the stable discriminator: two sessions
        // sharing a display name but different session dirs (cloned sessions)
        // are distinct identities, and the same catalog entry always maps to
        // the same identity.
        let same_name_other_dir = SessionInfo {
            session_dir: PathBuf::from("/sessions/work-copy"),
            ..info.clone()
        };
        assert_ne!(identity, SessionIdentity::from_info(&same_name_other_dir));
        assert_eq!(identity, SessionIdentity::from_info(&info));
    }
}