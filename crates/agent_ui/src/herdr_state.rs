use std::collections::BTreeMap;

use uuid::Uuid;

use crate::herdr_client::{
    HerdrAgentSessionIdentity, HerdrAgentSnapshot, HerdrAgentStatus, HerdrEvent,
    HerdrPaneSnapshot, HerdrWorkspaceSnapshot,
};
use crate::herdr_mapping_store::{
    HerdrMappingKey, HerdrMappingRecord, SessionMappings, tombstone_record, upsert_record,
};

/// Who initiated an operation that carries an operation ID. Zed-originated
/// operations are recorded as pending until Herdr reflects them back; a
/// reflected match is loop-suppressed instead of being acted on twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HerdrOperationOrigin {
    Zed,
#[allow(dead_code)]
    Herdr,
}

/// What a pending focus operation points at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FocusTarget {
    Workspace(String),
    Pane { workspace_id: String, pane_id: String },
}

/// An outstanding Zed-originated focus request awaiting reflection from Herdr.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingFocus {
    pub operation_id: String,
    pub target: FocusTarget,
    pub origin: HerdrOperationOrigin,
}

/// Pure, GPUI-free reconciliation state for one window's Herdr bridge.
///
/// Reconciliation functions take `&mut self` for bookkeeping (sequence fences,
/// tombstones, pending operations) and return explicit
/// [`ReconciliationAction`]s; they never touch GPUI entities. Task 3's bridge
/// translates actions into entity updates and outbound requests.
#[derive(Default)]
pub(crate) struct BridgeState {
    /// The Herdr session this bridge is bound to. Part of every mapping key.
    pub session: String,
    pub mappings: SessionMappings,
    /// Highest applied event sequence. Events older than this fence are stale
    /// and rejected.
    pub last_sequence: u64,
    pub pending_focus: Option<PendingFocus>,
    /// Every Zed-issued focus operation remains fenced until Herdr reflects
    /// that exact operation ID. Keeping superseded IDs prevents a delayed
    /// reflection from activating an older target after a newer focus wins.
    pub issued_focus: BTreeMap<String, PendingFocus>,
}

impl BridgeState {
    pub(crate) fn new(session: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            ..Self::default()
        }
    }

    /// Registers a Zed-originated pending focus operation.
#[allow(dead_code)]
    pub(crate) fn with_pending_focus(
        mut self,
        operation_id: impl Into<String>,
        target: FocusTarget,
    ) -> Self {
        let pending = PendingFocus {
            operation_id: operation_id.into(),
            target,
            origin: HerdrOperationOrigin::Zed,
        };
        self.issued_focus
            .insert(pending.operation_id.clone(), pending.clone());
        self.pending_focus = Some(pending);
        self
    }

    pub(crate) fn workspace_key(&self, workspace_id: &str) -> HerdrMappingKey {
        HerdrMappingKey::workspace(self.session.clone(), workspace_id)
    }

    pub(crate) fn subthread_key(
        &self,
        workspace_id: &str,
        pane_id: &str,
        agent_session: &HerdrAgentSessionIdentity,
    ) -> HerdrMappingKey {
        HerdrMappingKey::subthread(
            self.session.clone(),
            workspace_id,
            pane_id,
            agent_session.clone(),
        )
    }
}

/// Explicit work items the pure reconciliation layer hands to the bridge.
/// The bridge performs them against real entities; nothing here mutates GPUI
/// state.
#[derive(Clone, Debug)]
pub(crate) enum ReconciliationAction {
    CreateWorkspaceRoot(HerdrWorkspaceSnapshot),
    RestoreWorkspaceRoot(HerdrMappingRecord),
    CreateAgentSubthread(HerdrAgentSnapshot),
    RestoreAgentSubthread(HerdrMappingRecord),
    UpdateTitle(HerdrMappingKey, String),
    UpdateStatus(HerdrMappingKey, HerdrAgentStatus),
    Activate(HerdrMappingKey),
    Archive(HerdrMappingKey),
    RecordConflict(HerdrMappingKey, String),
}

impl PartialEq for ReconciliationAction {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CreateWorkspaceRoot(left), Self::CreateWorkspaceRoot(right)) => {
                workspace_snapshots_match(left, right)
            }
            (Self::RestoreWorkspaceRoot(left), Self::RestoreWorkspaceRoot(right))
            | (Self::RestoreAgentSubthread(left), Self::RestoreAgentSubthread(right)) => left == right,
            (Self::CreateAgentSubthread(left), Self::CreateAgentSubthread(right)) => {
                agent_snapshots_match(left, right)
            }
            (Self::UpdateTitle(left_key, left_title), Self::UpdateTitle(right_key, right_title))
            | (Self::RecordConflict(left_key, left_title), Self::RecordConflict(right_key, right_title)) => {
                left_key == right_key && left_title == right_title
            }
            (Self::UpdateStatus(left_key, left_status), Self::UpdateStatus(right_key, right_status)) => {
                left_key == right_key && left_status == right_status
            }
            (Self::Activate(left), Self::Activate(right)) | (Self::Archive(left), Self::Archive(right)) => {
                left == right
            }
            _ => false,
        }
    }
}

fn workspace_snapshots_match(left: &HerdrWorkspaceSnapshot, right: &HerdrWorkspaceSnapshot) -> bool {
    left.workspace_id == right.workspace_id
        && left.label == right.label
        && left.paths == right.paths
        && left.active_pane_id == right.active_pane_id
        && left.agents.len() == right.agents.len()
        && left
            .agents
            .iter()
            .zip(&right.agents)
            .all(|(left_agent, right_agent)| agent_snapshots_match(left_agent, right_agent))
        && left.focused == right.focused
        && left.number == right.number
        && left.pane_count == right.pane_count
        && left.tab_count == right.tab_count
        && left.active_tab_id == right.active_tab_id
        && left.agent_status == right.agent_status
}

fn agent_snapshots_match(left: &HerdrAgentSnapshot, right: &HerdrAgentSnapshot) -> bool {
    left.pane_id == right.pane_id
        && left.workspace_id == right.workspace_id
        && left.agent_type == right.agent_type
        && left.session_identity == right.session_identity
        && left.status == right.status
        && left.title == right.title
        && left.cwd == right.cwd
        && left.last_seen_sequence == right.last_seen_sequence
}
/// operation, never by a reflected one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutboundRequest {
    FocusWorkspace {
        workspace_id: String,
        operation_id: String,
        origin: HerdrOperationOrigin,
    },
    FocusPane {
        workspace_id: String,
        pane_id: String,
        operation_id: String,
        origin: HerdrOperationOrigin,
    },
}

/// Outcome of applying one event to [`BridgeState`].
#[derive(Debug, Default, PartialEq)]
pub(crate) struct AppliedEvent {
    pub actions: Vec<ReconciliationAction>,
    pub outbound: Vec<OutboundRequest>,
}

impl AppliedEvent {
    fn none() -> Self {
        Self::default()
    }

    fn single(action: ReconciliationAction) -> Self {
        Self {
            actions: vec![action],
            outbound: Vec::new(),
        }
    }
}

/// Reconciles a freshly received snapshot against persisted mappings.
///
/// Identity precedence per resource:
/// 1. exact session-qualified key match;
/// 2. agent-session restoration for subthreads whose pane was restarted;
/// 3. creation for genuinely unknown resources.
///
/// Worktree/cwd identity is never used to match. A snapshot entry whose only
/// stored counterpart is a closed tombstone becomes a visible conflict rather
/// than a silent resurrection.
pub(crate) fn reconcile_snapshot(
    session: &str,
    workspaces: &[HerdrWorkspaceSnapshot],
    mappings: &SessionMappings,
) -> Vec<ReconciliationAction> {
    let mut actions = Vec::new();
    for workspace in workspaces {
        let key = HerdrMappingKey::workspace(session, &workspace.workspace_id);
        let root_is_closed = match mappings.get(&key.to_key_string()) {
            Some(record) if record.is_tombstone() => {
                actions.push(ReconciliationAction::RecordConflict(
                    key,
                    format!(
                        "workspace {:?} has a closed mapping; refusing to resurrect it from snapshot data",
                        workspace.workspace_id
                    ),
                ));
                true
            }
            Some(record) => {
                actions.push(ReconciliationAction::RestoreWorkspaceRoot(record.clone()));
                false
            }
            None => {
                actions.push(ReconciliationAction::CreateWorkspaceRoot(workspace.clone()));
                false
            }
        };
        if root_is_closed {
            // A closed root fences every child below it. In particular, do
            // not create a fresh child merely because its pane identity
            // differs from a retained tombstone.
            continue;
        }

        for agent in &workspace.agents {
            let Some(identity) = agent.session_identity.as_ref() else {
                continue;
            };
            let workspace_id = if agent.workspace_id.is_empty() {
                &workspace.workspace_id
            } else {
                &agent.workspace_id
            };
            let target_key =
                HerdrMappingKey::subthread(session, workspace_id, &agent.pane_id, identity.clone());
            match resolve_agent_identity(mappings, session, workspace_id, &agent.pane_id, identity) {
                Ok(Some(record)) => match rebind_record_for_location(
                    mappings,
                    &record,
                    workspace_id,
                    &agent.pane_id,
                    agent.last_seen_sequence,
                ) {
                    Ok(rebound) => {
                        actions.push(ReconciliationAction::RestoreAgentSubthread(rebound))
                    }
                    Err(message) => {
                        actions.push(ReconciliationAction::RecordConflict(target_key, message))
                    }
                },
                Ok(None) => {
                    let same_pane = live_subthreads_at(mappings, session, workspace_id, &agent.pane_id);
                    if same_pane.len() == 1 {
                        actions.push(ReconciliationAction::RecordConflict(
                            target_key,
                            "pane already has a live mapping for a different agent session"
                                .to_string(),
                        ));
                    } else if same_pane.len() > 1 {
                        actions.push(ReconciliationAction::RecordConflict(
                            target_key,
                            "pane has multiple live mappings; refusing to create a duplicate"
                                .to_string(),
                        ));
                    } else {
                        actions.push(ReconciliationAction::CreateAgentSubthread(
                            HerdrAgentSnapshot {
                                pane_id: agent.pane_id.clone(),
                                workspace_id: workspace_id.clone(),
                                agent_type: agent.agent_type.clone(),
                                session_identity: Some(identity.clone()),
                                status: agent.status.clone(),
                                title: agent.title.clone(),
                                cwd: agent.cwd.clone(),
                                last_seen_sequence: agent.last_seen_sequence,
                            },
                        ));
                    }
                }
                Err(message) => actions.push(ReconciliationAction::RecordConflict(
                    target_key,
                    message,
                )),
            }
        }
    }
    actions
}

/// Resolves one (pane, agent-session) identity against persisted mappings.
/// Exact session-qualified matches win over agent-session restoration; cwd and
/// worktree identity never participate in matching. Tombstones are included
/// in fallback candidates so a closed identity is a conflict, never a
/// candidate for creation.
fn resolve_agent_identity(
    mappings: &SessionMappings,
    session: &str,
    workspace_id: &str,
    pane_id: &str,
    identity: &HerdrAgentSessionIdentity,
) -> Result<Option<HerdrMappingRecord>, String> {
    let exact_key = HerdrMappingKey::subthread(session, workspace_id, pane_id, identity.clone());
    match mappings.get(&exact_key.to_key_string()) {
        Some(record) if record.is_tombstone() => {
            return Err(format!(
                "pane {pane_id:?} maps to a closed subthread mapping; refusing to resurrect it"
            ));
        }
        Some(record) => return Ok(Some(record.clone())),
        None => {}
    }

    let candidates: Vec<&HerdrMappingRecord> = mappings
        .values()
        .filter(|record| {
            record.key.session == session
                && record.key.pane_id.is_some()
                && record.key.agent_session.as_ref() == Some(identity)
        })
        .collect();
    match candidates.as_slice() {
        [] => Ok(None),
        [only] if only.is_tombstone() => Err(format!(
            "agent session {} is retained as a closed mapping; refusing to resurrect it",
            identity.value
        )),
        [only] => Ok(Some((*only).clone())),
        _ => Err(format!(
            "ambiguous agent session {}: {} mappings claim this identity",
            identity.value,
            candidates.len()
        )),
    }
}

fn live_subthreads_at<'a>(
    mappings: &'a SessionMappings,
    session: &str,
    workspace_id: &str,
    pane_id: &str,
) -> Vec<&'a HerdrMappingRecord> {
    mappings
        .values()
        .filter(|record| {
            !record.is_tombstone()
                && record.key.session == session
                && record.key.workspace_id == workspace_id
                && record.key.pane_id.as_deref() == Some(pane_id)
        })
        .collect()
}

/// Builds the record that represents a restored resource at its current
/// location. This is deliberately transactional: callers validate the
/// destination and root before replacing the old key, and a failed validation
/// leaves the old record untouched.
fn rebind_record_for_location(
    mappings: &SessionMappings,
    record: &HerdrMappingRecord,
    workspace_id: &str,
    pane_id: &str,
    sequence: u64,
) -> Result<HerdrMappingRecord, String> {
    let Some(agent_session) = record.key.agent_session.clone() else {
        return Err("subthread mapping lacks an agent identity".to_string());
    };
    let target_key =
        HerdrMappingKey::subthread(record.key.session.clone(), workspace_id, pane_id, agent_session);
    if live_subthreads_at(mappings, &record.key.session, workspace_id, pane_id)
        .iter()
        .any(|candidate| candidate.key != record.key)
    {
        return Err("destination pane already has a live mapping".to_string());
    }
    if let Some(target) = mappings.get(&target_key.to_key_string()) {
        if target.key != record.key && target.is_tombstone() {
            return Err("destination subthread mapping is a retained tombstone".to_string());
        }
    }
    let root_key =
        HerdrMappingKey::workspace(record.key.session.clone(), workspace_id).to_key_string();
    let destination_root = mappings.get(&root_key);
    if let Some(root) = destination_root {
        if root.is_tombstone() {
            return Err("destination workspace mapping is a retained tombstone".to_string());
        }
    } else if record.key.workspace_id != workspace_id {
        return Err("destination workspace has no live root mapping".to_string());
    }

    let mut rebound = record.clone();
    rebound.key = target_key;
    if let Some(root) = destination_root {
        rebound.zed_root_thread_id = root.zed_root_thread_id;
    }
    rebound.last_seen_sequence = rebound.last_seen_sequence.max(sequence);
    Ok(rebound)
}


pub(crate) fn apply_event(state: &mut BridgeState, event: &HerdrEvent) -> AppliedEvent {
    let sequence = event.sequence();
    if sequence_guarded(event) {
        if sequence == 0 {
            if sequence_required(event) {
                return zero_sequence_conflict(state, event);
            }
        } else {
            // The bridge-wide fence handles ordinary in-memory ordering. The
            // resource fence below also protects a restarted bridge whose loaded
            // records are newer than its newly initialized global fence.
            if sequence <= state.last_sequence
                || event_is_stale_against_persisted_mapping(state, event, sequence)
            {
                return AppliedEvent::none();
            }
            state.last_sequence = sequence;
        }
    }

    match event {
        HerdrEvent::WorkspaceCreated { workspace, sequence } => {
            reconcile_workspace_created(state, workspace, *sequence)
        }
        HerdrEvent::WorkspaceUpdated { workspace, sequence } => {
            reconcile_workspace_updated(state, workspace, *sequence)
        }
        HerdrEvent::WorkspaceFocused {
            workspace_id,
            operation_id,
            ..
        } => apply_focus(
            state,
            FocusTarget::Workspace(workspace_id.clone()),
            operation_id.as_deref(),
            sequence,
        ),
        HerdrEvent::WorkspaceRenamed {
            workspace_id,
            label,
            ..
        } => {
            let key = state.workspace_key(workspace_id);
            if live_record(&state.mappings, &key).is_some() {
                touch_record_sequence(state, &key, sequence);
                AppliedEvent::single(ReconciliationAction::UpdateTitle(key, label.clone()))
            } else {
                AppliedEvent::none()
            }
        }
        HerdrEvent::WorkspaceClosed { workspace_id, .. } => {
            close_workspace_mappings(state, workspace_id, sequence)
        }
        HerdrEvent::PaneAgentDetected {
            pane_id,
            workspace_id,
            agent_type,
            session_identity,
            ..
        } => {
            let Some(identity) = session_identity else {
                return AppliedEvent::none();
            };
            restore_or_create_agent(
                state,
                workspace_id,
                pane_id,
                identity,
                agent_type.clone(),
                sequence,
            )
        }
        HerdrEvent::PaneMoved {
            pane,
            previous_pane_id,
            previous_workspace_id,
            ..
        } => apply_pane_moved(
            state,
            pane,
            previous_workspace_id.as_deref(),
            previous_pane_id.as_deref(),
            sequence,
        ),
        HerdrEvent::PaneAgentStatusChanged {
            pane_id, status, ..
        } => match live_subthread_by_id(state, pane_id) {
            Some(key) => {
                touch_record_sequence(state, &key, sequence);
                AppliedEvent::single(ReconciliationAction::UpdateStatus(key, status.clone()))
            }
            None => AppliedEvent::none(),
        },
        HerdrEvent::PaneFocused {
            pane_id,
            workspace_id,
            operation_id,
            ..
        } => apply_focus(
            state,
            FocusTarget::Pane {
                workspace_id: workspace_id.clone(),
                pane_id: pane_id.clone(),
            },
            operation_id.as_deref(),
            sequence,
        ),
        HerdrEvent::PaneClosed {
            pane_id,
            workspace_id,
            ..
        } => match live_subthread_by_pane(state, workspace_id, pane_id) {
            Some(key) => close_mapping(state, &key, sequence),
            None => AppliedEvent::none(),
        },
        HerdrEvent::PaneExited { pane_id, .. } => match live_subthread_by_id(state, pane_id) {
            Some(key) => close_mapping(state, &key, sequence),
            None => AppliedEvent::none(),
        },
        _ => AppliedEvent::none(),
    }
}

fn sequence_guarded(event: &HerdrEvent) -> bool {
    !matches!(
        event,
        HerdrEvent::SubscriptionStarted { .. } | HerdrEvent::Unknown { .. }
    )
}

fn sequence_required(event: &HerdrEvent) -> bool {
    !matches!(
        event,
        HerdrEvent::WorkspaceFocused { .. }
            | HerdrEvent::PaneFocused { .. }
            | HerdrEvent::PaneAgentStatusChanged { .. }
            | HerdrEvent::PaneOutput { .. }
            | HerdrEvent::PaneScrollChanged { .. }
            | HerdrEvent::SubscriptionStarted { .. }
            | HerdrEvent::Unknown { .. }
    )
}

fn event_is_stale_against_persisted_mapping(
    state: &BridgeState,
    event: &HerdrEvent,
    sequence: u64,
) -> bool {
    mapping_keys_for_event(state, event).into_iter().any(|key| {
        state
            .mappings
            .get(&key.to_key_string())
            .is_some_and(|record| sequence <= record.last_seen_sequence)
    })
}

fn add_workspace_mapping_keys(
    state: &BridgeState,
    keys: &mut Vec<HerdrMappingKey>,
    workspace_id: &str,
) {
    keys.push(state.workspace_key(workspace_id));
    keys.extend(
        state
            .mappings
            .values()
            .filter(|record| {
                record.key.session == state.session && record.key.workspace_id == workspace_id
            })
            .map(|record| record.key.clone()),
    );
}

fn add_pane_mapping_keys(
    state: &BridgeState,
    keys: &mut Vec<HerdrMappingKey>,
    workspace_id: Option<&str>,
    pane_id: &str,
) {
    keys.extend(
        state
            .mappings
            .values()
            .filter(|record| {
                record.key.pane_id.is_some()
                    && record.key.session == state.session
                    && record.key.pane_id.as_deref() == Some(pane_id)
                    && workspace_id.map_or(true, |workspace| record.key.workspace_id == workspace)
            })
            .map(|record| record.key.clone()),
    );
}
fn mapping_keys_for_event(state: &BridgeState, event: &HerdrEvent) -> Vec<HerdrMappingKey> {
    let mut keys = Vec::new();
    match event {
        HerdrEvent::WorkspaceCreated { workspace, .. }
        | HerdrEvent::WorkspaceUpdated { workspace, .. } => {
            add_workspace_mapping_keys(state, &mut keys, &workspace.workspace_id)
        }
        HerdrEvent::WorkspaceRenamed { workspace_id, .. }
        | HerdrEvent::WorkspaceFocused { workspace_id, .. }
        | HerdrEvent::WorkspaceClosed { workspace_id, .. }
        | HerdrEvent::WorkspaceMoved { workspace_id, .. } => {
            add_workspace_mapping_keys(state, &mut keys, workspace_id)
        }
        HerdrEvent::PaneCreated { pane, .. } | HerdrEvent::PaneUpdated { pane, .. } => {
            add_workspace_mapping_keys(state, &mut keys, &pane.workspace_id);
            add_pane_mapping_keys(state, &mut keys, Some(&pane.workspace_id), &pane.pane_id);
        }
        HerdrEvent::PaneMoved {
            pane,
            previous_workspace_id,
            previous_pane_id,
            ..
        } => {
            add_workspace_mapping_keys(state, &mut keys, &pane.workspace_id);
            add_pane_mapping_keys(state, &mut keys, Some(&pane.workspace_id), &pane.pane_id);
            if let Some(previous_workspace_id) = previous_workspace_id.as_deref() {
                add_workspace_mapping_keys(state, &mut keys, previous_workspace_id);
                if let Some(previous_pane_id) = previous_pane_id.as_deref() {
                    add_pane_mapping_keys(
                        state,
                        &mut keys,
                        Some(previous_workspace_id),
                        previous_pane_id,
                    );
                }
            }
        }
        HerdrEvent::PaneAgentDetected {
            pane_id,
            workspace_id,
            session_identity,
            ..
        } => {
            add_workspace_mapping_keys(state, &mut keys, workspace_id);
            add_pane_mapping_keys(state, &mut keys, Some(workspace_id), pane_id);
            if let Some(identity) = session_identity {
                keys.push(state.subthread_key(workspace_id, pane_id, identity));
                keys.extend(
                    state
                        .mappings
                        .values()
                        .filter(|record| {
                            record.key.session == state.session
                                && record.key.agent_session.as_ref() == Some(identity)
                        })
                        .map(|record| record.key.clone()),
                );
            }
        }
        HerdrEvent::PaneAgentStatusChanged { pane_id, .. }
        | HerdrEvent::PaneExited { pane_id, .. }
        | HerdrEvent::PaneOutput { pane_id, .. }
        | HerdrEvent::PaneScrollChanged { pane_id, .. } => {
            add_pane_mapping_keys(state, &mut keys, None, pane_id)
        }
        HerdrEvent::PaneFocused {
            pane_id,
            workspace_id,
            ..
        }
        | HerdrEvent::PaneClosed {
            pane_id,
            workspace_id,
            ..
        } => add_pane_mapping_keys(state, &mut keys, Some(workspace_id), pane_id),
        HerdrEvent::WorkspaceReordered { .. }
        | HerdrEvent::SubscriptionStarted { .. }
        | HerdrEvent::Unknown { .. } => {}
    }
    keys
}

fn zero_sequence_conflict(state: &BridgeState, event: &HerdrEvent) -> AppliedEvent {
    let key = match event {
        HerdrEvent::WorkspaceCreated { workspace, .. }
        | HerdrEvent::WorkspaceUpdated { workspace, .. } => {
            state.workspace_key(&workspace.workspace_id)
        }
        HerdrEvent::WorkspaceRenamed { workspace_id, .. }
        | HerdrEvent::WorkspaceFocused { workspace_id, .. }
        | HerdrEvent::WorkspaceClosed { workspace_id, .. }
        | HerdrEvent::WorkspaceMoved { workspace_id, .. } => state.workspace_key(workspace_id),
        HerdrEvent::PaneCreated { pane, .. }
        | HerdrEvent::PaneUpdated { pane, .. }
        | HerdrEvent::PaneMoved { pane, .. } => pane
            .session_identity
            .as_ref()
            .map(|identity| state.subthread_key(&pane.workspace_id, &pane.pane_id, identity))
            .or_else(|| {
                any_subthread_by_pane(state, &pane.workspace_id, &pane.pane_id)
            })
            .unwrap_or_else(|| state.workspace_key(&pane.workspace_id)),
        HerdrEvent::PaneAgentDetected {
            pane_id,
            workspace_id,
            session_identity,
            ..
        } => session_identity
            .as_ref()
            .map(|identity| state.subthread_key(workspace_id, pane_id, identity))
            .or_else(|| any_subthread_by_pane(state, workspace_id, pane_id))
            .unwrap_or_else(|| state.workspace_key(workspace_id)),
        HerdrEvent::PaneAgentStatusChanged { pane_id, .. }
        | HerdrEvent::PaneExited { pane_id, .. }
        | HerdrEvent::PaneOutput { pane_id, .. }
        | HerdrEvent::PaneScrollChanged { pane_id, .. } => any_subthread_by_pane(state, "", pane_id)
            .unwrap_or_else(|| state.workspace_key(&format!("pane:{pane_id}"))),
        HerdrEvent::PaneFocused {
            pane_id,
            workspace_id,
            ..
        }
        | HerdrEvent::PaneClosed {
            pane_id,
            workspace_id,
            ..
        } => any_subthread_by_pane(state, workspace_id, pane_id)
            .unwrap_or_else(|| state.workspace_key(workspace_id)),
        HerdrEvent::WorkspaceReordered { .. }
        | HerdrEvent::SubscriptionStarted { .. }
        | HerdrEvent::Unknown { .. } => state.workspace_key("unknown"),
    };
    AppliedEvent::single(ReconciliationAction::RecordConflict(
        key,
        "event has a missing or zero sequence; refresh is required".to_string(),
    ))
}

fn any_subthread_by_pane(
    state: &BridgeState,
    workspace_id: &str,
    pane_id: &str,
) -> Option<HerdrMappingKey> {
    state
        .mappings
        .values()
        .find(|record| {
            record.key.session == state.session
                && record.key.pane_id.as_deref() == Some(pane_id)
                && (workspace_id.is_empty() || record.key.workspace_id == workspace_id)
        })
        .map(|record| record.key.clone())
}

fn restore_or_create_agent(
    state: &mut BridgeState,
    workspace_id: &str,
    pane_id: &str,
    identity: &HerdrAgentSessionIdentity,
    agent_type: Option<String>,
    sequence: u64,
) -> AppliedEvent {
    let target_key = state.subthread_key(workspace_id, pane_id, identity);
    if state
        .mappings
        .get(&state.workspace_key(workspace_id).to_key_string())
        .is_some_and(|record| record.is_tombstone())
    {
        return AppliedEvent::single(ReconciliationAction::RecordConflict(
            state.workspace_key(workspace_id),
            "agent event belongs to a closed workspace mapping".to_string(),
        ));
    }

    match resolve_agent_identity(&state.mappings, &state.session, workspace_id, pane_id, identity) {
        Ok(Some(record)) => match rebind_subthread_location(
            state,
            &record,
            workspace_id,
            pane_id,
            sequence,
        ) {
            Ok(rebound) => {
                AppliedEvent::single(ReconciliationAction::RestoreAgentSubthread(rebound))
            }
            Err(message) => {
                AppliedEvent::single(ReconciliationAction::RecordConflict(target_key, message))
            }
        },
        Ok(None) => {
            let same_pane = live_subthreads_at(&state.mappings, &state.session, workspace_id, pane_id);
            if !same_pane.is_empty() {
                return AppliedEvent::single(ReconciliationAction::RecordConflict(
                    target_key,
                    if same_pane.len() == 1 {
                        "pane already has a live mapping for a different agent session"
                            .to_string()
                    } else {
                        "pane has multiple live mappings; refusing to create a duplicate"
                            .to_string()
                    },
                ));
            }
            AppliedEvent::single(ReconciliationAction::CreateAgentSubthread(
                HerdrAgentSnapshot {
                    pane_id: pane_id.to_string(),
                    workspace_id: workspace_id.to_string(),
                    agent_type,
                    session_identity: Some(identity.clone()),
                    last_seen_sequence: sequence,
                    ..Default::default()
                },
            ))
        }
        Err(message) => {
            AppliedEvent::single(ReconciliationAction::RecordConflict(target_key, message))
        }
    }
}

fn apply_pane_moved(
    state: &mut BridgeState,
    pane: &HerdrPaneSnapshot,
    previous_workspace_id: Option<&str>,
    previous_pane_id: Option<&str>,
    sequence: u64,
) -> AppliedEvent {
    if let Some(identity) = pane.session_identity.as_ref() {
        return restore_or_create_agent(
            state,
            &pane.workspace_id,
            &pane.pane_id,
            identity,
            pane.agent_type.clone(),
            sequence,
        );
    }

    let old_workspace = previous_workspace_id.unwrap_or(&pane.workspace_id);
    let old_pane = previous_pane_id.unwrap_or(&pane.pane_id);
    let candidates: Vec<HerdrMappingRecord> =
        live_subthreads_at(&state.mappings, &state.session, old_workspace, old_pane)
            .into_iter()
            .cloned()
            .collect();
    match candidates.as_slice() {
        [record] => match rebind_subthread_location(
            state,
            record,
            &pane.workspace_id,
            &pane.pane_id,
            sequence,
        ) {
            Ok(rebound) => {
                AppliedEvent::single(ReconciliationAction::RestoreAgentSubthread(rebound))
            }
            Err(message) => {
                let conflict_key = record
                    .key
                    .agent_session
                    .clone()
                    .map(|identity| {
                        HerdrMappingKey::subthread(
                            state.session.clone(),
                            &pane.workspace_id,
                            &pane.pane_id,
                            identity,
                        )
                    })
                    .unwrap_or_else(|| state.workspace_key(&pane.workspace_id));
                AppliedEvent::single(ReconciliationAction::RecordConflict(
                    conflict_key,
                    message,
                ))
            }
        },
        [] => AppliedEvent::none(),
        _ => AppliedEvent::single(ReconciliationAction::RecordConflict(
            state.workspace_key(&pane.workspace_id),
            "pane move has multiple source mappings".to_string(),
        )),
    }
}

fn apply_focus(
    state: &mut BridgeState,
    target: FocusTarget,
    operation_id: Option<&str>,
    sequence: u64,
) -> AppliedEvent {
    // A reflected operation is fenced by ID, even after a newer focus
    // supersedes it. This prevents delayed A from activating A after B.
    if let Some(operation_id) = operation_id {
        if state.issued_focus.remove(operation_id).is_some() {
            if state
                .pending_focus
                .as_ref()
                .is_some_and(|pending| pending.operation_id == operation_id)
            {
                state.pending_focus = None;
            }
            return AppliedEvent::none();
        }
        // Keep compatibility with callers that construct `pending_focus`
        // directly rather than through the registration helpers.
        if state
            .pending_focus
            .as_ref()
            .is_some_and(|pending| {
                pending.origin == HerdrOperationOrigin::Zed
                    && pending.operation_id == operation_id
                    && pending.target == target
            })
        {
            state.pending_focus = None;
            return AppliedEvent::none();
        }
    }

    let key = match &target {
        FocusTarget::Workspace(workspace_id) => state.workspace_key(workspace_id),
        FocusTarget::Pane {
            workspace_id,
            pane_id,
        } => match live_subthread_by_pane(state, workspace_id, pane_id) {
            Some(key) => key,
            None => return AppliedEvent::none(),
        },
    };
    if live_record(&state.mappings, &key).is_none() {
        return AppliedEvent::none();
    }
    touch_record_sequence(state, &key, sequence);
    AppliedEvent::single(ReconciliationAction::Activate(key))
}

pub(crate) fn initiate_workspace_focus(
    state: &mut BridgeState,
    workspace_id: &str,
) -> OutboundRequest {
    let operation_id = Uuid::new_v4().to_string();
    let pending = PendingFocus {
        operation_id: operation_id.clone(),
        target: FocusTarget::Workspace(workspace_id.to_string()),
        origin: HerdrOperationOrigin::Zed,
    };
    state
        .issued_focus
        .insert(operation_id.clone(), pending.clone());
    state.pending_focus = Some(pending);
    OutboundRequest::FocusWorkspace {
        workspace_id: workspace_id.to_string(),
        operation_id,
        origin: HerdrOperationOrigin::Zed,
    }
}

pub(crate) fn initiate_pane_focus(
    state: &mut BridgeState,
    workspace_id: &str,
    pane_id: &str,
) -> OutboundRequest {
    let operation_id = Uuid::new_v4().to_string();
    let pending = PendingFocus {
        operation_id: operation_id.clone(),
        target: FocusTarget::Pane {
            workspace_id: workspace_id.to_string(),
            pane_id: pane_id.to_string(),
        },
        origin: HerdrOperationOrigin::Zed,
    };
    state
        .issued_focus
        .insert(operation_id.clone(), pending.clone());
    state.pending_focus = Some(pending);
    OutboundRequest::FocusPane {
        workspace_id: workspace_id.to_string(),
        pane_id: pane_id.to_string(),
        operation_id,
        origin: HerdrOperationOrigin::Zed,
    }
}

fn live_record<'a>(
    mappings: &'a SessionMappings,
    key: &HerdrMappingKey,
) -> Option<&'a HerdrMappingRecord> {
    mappings
        .get(&key.to_key_string())
        .filter(|record| !record.is_tombstone())
}

fn live_subthread_by_pane(
    state: &BridgeState,
    workspace_id: &str,
    pane_id: &str,
) -> Option<HerdrMappingKey> {
    state
        .mappings
        .values()
        .find(|record| {
            !record.is_tombstone()
                && record.key.session == state.session
                && record.key.workspace_id == workspace_id
                && record.key.pane_id.as_deref() == Some(pane_id)
        })
        .map(|record| record.key.clone())
}

fn live_subthread_by_id(state: &BridgeState, pane_id: &str) -> Option<HerdrMappingKey> {
    let mut candidates = state.mappings.values().filter(|record| {
        !record.is_tombstone()
            && record.key.session == state.session
            && record.key.pane_id.as_deref() == Some(pane_id)
    });
    let candidate = candidates.next()?.key.clone();
    if candidates.next().is_some() {
        None
    } else {
        Some(candidate)
    }
}

fn close_mapping(state: &mut BridgeState, key: &HerdrMappingKey, sequence: u64) -> AppliedEvent {
    match tombstone_record(&mut state.mappings, key, sequence) {
        Some(_) => AppliedEvent::single(ReconciliationAction::Archive(key.clone())),
        None => AppliedEvent::none(),
    }
}

fn reconcile_workspace_created(
    state: &mut BridgeState,
    workspace: &HerdrWorkspaceSnapshot,
    sequence: u64,
) -> AppliedEvent {
    let key = state.workspace_key(&workspace.workspace_id);
    match state.mappings.get(&key.to_key_string()) {
        Some(record) if record.is_tombstone() => AppliedEvent::single(
            ReconciliationAction::RecordConflict(
                key,
                "workspace created while a closed mapping is retained".to_string(),
            ),
        ),
        Some(_) => {
            touch_record_sequence(state, &key, sequence);
            AppliedEvent::none()
        }
        None => AppliedEvent::single(ReconciliationAction::CreateWorkspaceRoot(workspace.clone())),
    }
}

fn reconcile_workspace_updated(
    state: &mut BridgeState,
    workspace: &HerdrWorkspaceSnapshot,
    sequence: u64,
) -> AppliedEvent {
    let key = state.workspace_key(&workspace.workspace_id);
    match state.mappings.get(&key.to_key_string()) {
        Some(record) if record.is_tombstone() => AppliedEvent::single(
            ReconciliationAction::RecordConflict(
                key,
                "workspace updated while a closed mapping is retained".to_string(),
            ),
        ),
        Some(_) => {
            touch_record_sequence(state, &key, sequence);
            AppliedEvent::single(ReconciliationAction::UpdateTitle(
                key,
                workspace.label.clone(),
            ))
        }
        None => AppliedEvent::single(ReconciliationAction::CreateWorkspaceRoot(workspace.clone())),
    }
}


fn close_workspace_mappings(
    state: &mut BridgeState,
    workspace_id: &str,
    sequence: u64,
) -> AppliedEvent {
    let keys: Vec<HerdrMappingKey> = state
        .mappings
        .values()
        .filter(|record| {
            !record.is_tombstone()
                && record.key.session == state.session
                && record.key.workspace_id == workspace_id
        })
        .map(|record| record.key.clone())
        .collect();
    let actions = keys
        .into_iter()
        .filter_map(|key| {
            tombstone_record(&mut state.mappings, &key, sequence)
                .map(|_| ReconciliationAction::Archive(key))
        })
        .collect();
    AppliedEvent {
        actions,
        outbound: Vec::new(),
    }
}

/// Rebinds a restored subthread to its current workspace and pane so
/// subsequent per-pane events resolve without another restoration round.
/// Destination and root checks happen before removing the old key, making a
/// failed rebind transactional.
fn rebind_subthread_location(
    state: &mut BridgeState,
    record: &HerdrMappingRecord,
    workspace_id: &str,
    pane_id: &str,
    sequence: u64,
) -> Result<HerdrMappingRecord, String> {
    let rebound =
        rebind_record_for_location(&state.mappings, record, workspace_id, pane_id, sequence)?;
    let old_key = record.key.to_key_string();
    let new_key = rebound.key.to_key_string();
    if old_key == new_key {
        touch_record_sequence(state, &rebound.key, sequence);
        return state
            .mappings
            .get(&new_key)
            .cloned()
            .ok_or_else(|| "mapped subthread disappeared during rebind".to_string());
    }

    let old_record = state.mappings.get(&old_key).cloned();
    state.mappings.remove(&old_key);
    if !upsert_record(&mut state.mappings, rebound.clone()) {
        if let Some(old_record) = old_record {
            state.mappings.insert(old_key, old_record);
        }
        return Err("destination mapping rejected during transactional rebind".to_string());
    }
    Ok(rebound)
}

fn touch_record_sequence(state: &mut BridgeState, key: &HerdrMappingKey, sequence: u64) {
    if let Some(record) = state.mappings.get_mut(&key.to_key_string()) {
        if sequence > record.last_seen_sequence {
            record.last_seen_sequence = sequence;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        herdr_mapping_store::HerdrLifecycleState,
        thread_metadata_store::ThreadId,
    };

    fn root_record(session: &str, workspace_id: &str) -> HerdrMappingRecord {
        HerdrMappingRecord::root(session, workspace_id, ThreadId::new())
    }

    fn subthread_record(
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
            zed_workspace_id: None,
            zed_subthread_session_id: Some(format!("sub-{agent_value}")),
            worktree_or_cwd_identity: None,
            last_seen_sequence: 0,
            lifecycle: HerdrLifecycleState::Active,
        }
    }

    fn workspace_snapshot(workspace_id: &str, label: &str) -> HerdrWorkspaceSnapshot {
        HerdrWorkspaceSnapshot {
            workspace_id: workspace_id.to_string(),
            label: label.to_string(),
            ..Default::default()
        }
    }

    fn focused_event(workspace_id: &str, operation_id: &str, sequence: u64) -> HerdrEvent {
        HerdrEvent::WorkspaceFocused {
            workspace_id: workspace_id.to_string(),
            operation_id: Some(operation_id.to_string()),
            sequence,
        }
    }

    #[test]
    fn snapshot_restores_existing_workspace_mapping() {
        let session = "alpha";
        let mapping = root_record(session, "w1");
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, mapping.clone());

        let actions = reconcile_snapshot(session, &[workspace_snapshot("w1", "Review")], &mappings);
        assert_eq!(actions, vec![ReconciliationAction::RestoreWorkspaceRoot(mapping)]);
    }

    #[test]
    fn snapshot_restores_root_owner_metadata() {
        let session = "alpha";
        let owner = workspace::WorkspaceId::from_i64(42);
        let mut mapping = root_record(session, "w1");
        mapping.zed_workspace_id = Some(owner);
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, mapping);

        let actions = reconcile_snapshot(session, &[workspace_snapshot("w1", "Review")], &mappings);
        assert!(matches!(
            &actions[..],
            [ReconciliationAction::RestoreWorkspaceRoot(record)]
                if record.zed_workspace_id == Some(owner)
        ));
    }


    #[test]
    fn snapshot_creates_roots_for_unknown_workspaces() {
        let session = "alpha";
        let actions = reconcile_snapshot(
            session,
            &[workspace_snapshot("w1", "New"), workspace_snapshot("w2", "Other")],
            &SessionMappings::new(),
        );
        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], ReconciliationAction::CreateWorkspaceRoot(_)));
        assert!(matches!(actions[1], ReconciliationAction::CreateWorkspaceRoot(_)));
    }

    #[test]
    fn snapshot_tombstoned_workspace_is_a_conflict_not_a_resurrection() {
        let session = "alpha";
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root_record(session, "w1"));
        tombstone_record(&mut mappings, &HerdrMappingKey::workspace(session, "w1"), 5);

        let actions = reconcile_snapshot(session, &[workspace_snapshot("w1", "Back?")], &mappings);
        assert!(
            matches!(&actions[..], [ReconciliationAction::RecordConflict(_, _)]),
            "expected a single RecordConflict, got {actions:?}"
        );
    }

    #[test]
    fn snapshot_restores_subthread_by_agent_session_after_restart() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let restored = subthread_record(session, "w1", "p-old", "agent-1");
        let mut expected = restored.clone();
        expected.key.pane_id = Some("p-new".into());
        expected.zed_root_thread_id = root.zed_root_thread_id;
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root.clone());
        upsert_record(&mut mappings, restored);

        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p-new".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            reconcile_snapshot(session, &[workspace], &mappings),
            vec![
                ReconciliationAction::RestoreWorkspaceRoot(root),
                ReconciliationAction::RestoreAgentSubthread(expected),
            ]
        );
    }

    #[test]
    fn ambiguous_agent_session_becomes_a_conflict_not_a_duplicate() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root.clone());
        upsert_record(&mut mappings, subthread_record(session, "w1", "p1", "agent-1"));
        upsert_record(&mut mappings, subthread_record(session, "w1", "p2", "agent-1"));

        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p3".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::RestoreWorkspaceRoot(restored_root),
                ReconciliationAction::RecordConflict(_, _)
            ] if restored_root == &root
        ), "expected root restoration and a conflict, got {actions:?}");
    }

    #[test]
    fn identical_cwd_never_merges_mappings() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let mut existing = subthread_record(session, "w1", "p1", "agent-1");
        existing.worktree_or_cwd_identity = Some("/repo".into());
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root.clone());
        upsert_record(&mut mappings, existing);

        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p2".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-2")),
                cwd: Some("/repo".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::RestoreWorkspaceRoot(restored_root),
                ReconciliationAction::CreateAgentSubthread(_)
            ] if restored_root == &root
        ), "expected root restoration and independent creation, got {actions:?}");
    }

    #[test]
    fn agent_without_session_identity_creates_no_subthread() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root.clone());
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                session_identity: None,
                status: HerdrAgentStatus::Working,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            reconcile_snapshot(session, &[workspace], &mappings),
            vec![ReconciliationAction::RestoreWorkspaceRoot(root)]
        );
    }

    #[test]
    fn reflected_focus_operation_is_not_emitted_again() {
        let mut state =
            BridgeState::new("alpha").with_pending_focus("op-1", FocusTarget::Workspace("w1".into()));
        let applied = apply_event(&mut state, &focused_event("w1", "op-1", 1));
        assert!(applied.outbound.is_empty());
        assert!(applied.actions.is_empty());
        assert!(state.pending_focus.is_none());
    }

    #[test]
    fn reflected_focus_advances_the_sequence_fence() {
        let mut state =
            BridgeState::new("alpha").with_pending_focus("op-1", FocusTarget::Workspace("w1".into()));
        apply_event(&mut state, &focused_event("w1", "op-1", 5));
        assert_eq!(state.last_sequence, 5);
        assert!(apply_event(&mut state, &focused_event("w1", "late", 4))
            .actions
            .is_empty());
    }

    #[test]
    fn foreign_focus_event_activates_without_outbound_echo() {
        let session = "alpha";
        let record = root_record(session, "w1");
        let key = record.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);

        let applied = apply_event(&mut state, &focused_event("w1", "herdr-op", 2));
        assert_eq!(applied.actions, vec![ReconciliationAction::Activate(key)]);
        assert!(applied.outbound.is_empty());
        assert!(state.pending_focus.is_none());
    }

    #[test]
    fn focus_with_different_operation_id_does_not_consume_pending_op() {
        let mut state =
            BridgeState::new("alpha").with_pending_focus("op-1", FocusTarget::Workspace("w1".into()));
        // No mapping exists yet, so a foreign focus yields no activation either.
        let applied = apply_event(&mut state, &focused_event("w1", "other-op", 3));
        assert!(applied.outbound.is_empty());
        assert!(state.pending_focus.is_some());
        assert_eq!(state.last_sequence, 3);
    }

    #[test]
    fn initiating_focus_produces_exactly_one_outbound_request() {
        let mut state = BridgeState::new("alpha");
        let outbound = initiate_workspace_focus(&mut state, "w1");
        let pending = state.pending_focus.clone().expect("pending focus registered");
        assert_eq!(outbound, OutboundRequest::FocusWorkspace {
            workspace_id: "w1".into(),
            operation_id: pending.operation_id.clone(),
            origin: HerdrOperationOrigin::Zed,
        });
        assert_eq!(pending.origin, HerdrOperationOrigin::Zed);

        let applied = apply_event(
            &mut state,
            &focused_event("w1", &pending.operation_id, 4),
        );
        assert!(applied.outbound.is_empty());
        assert!(applied.actions.is_empty());
    }

    #[test]
    fn initiating_pane_focus_registers_and_suppresses_its_reflection() {
        let mut state = BridgeState::new("alpha");
        let outbound = initiate_pane_focus(&mut state, "w1", "p1");
        let OutboundRequest::FocusPane {
            workspace_id,
            pane_id,
            operation_id,
            origin,
        } = outbound
        else {
            panic!("pane focus emits pane focus request");
        };
        assert_eq!(workspace_id, "w1");
        assert_eq!(pane_id, "p1");
        assert_eq!(origin, HerdrOperationOrigin::Zed);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneFocused {
                pane_id,
                workspace_id,
                operation_id: Some(operation_id),
                sequence: 1,
            },
        );
        assert!(applied.actions.is_empty());
        assert!(applied.outbound.is_empty());
        assert!(state.pending_focus.is_none());
    }

    #[test]
    fn stale_sequence_events_are_rejected() {
        let session = "alpha";
        let record = root_record(session, "w1");
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);
        state.last_sequence = 10;

        let applied = apply_event(&mut state, &focused_event("w1", "late-op", 9));
        assert!(applied.actions.is_empty());
        assert!(applied.outbound.is_empty());
        assert_eq!(state.last_sequence, 10, "fence must not move backwards");
        assert!(state.pending_focus.is_none());
    }

    #[test]
    fn workspace_close_tombstones_and_rejects_late_focus() {
        let session = "alpha";
        let record = root_record(session, "w1");
        let key = record.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceClosed {
                workspace_id: "w1".into(),
                sequence: 11,
            },
        );
        assert_eq!(applied.actions, vec![ReconciliationAction::Archive(key.clone())]);
        let stored = state
            .mappings
            .get(&key.to_key_string())
            .expect("tombstone retained");
        assert!(stored.is_tombstone());
        assert_eq!(stored.last_seen_sequence, 11);

        // A later focus for the closed workspace must not resurrect anything.
        let applied = apply_event(&mut state, &focused_event("w1", "late-focus", 12));
        assert!(applied.actions.is_empty());
        assert!(state
            .mappings
            .get(&key.to_key_string())
            .unwrap()
            .is_tombstone());
    }

    #[test]
    fn rename_and_status_events_update_existing_mappings_only() {
        let session = "alpha";
        let mut state = BridgeState::new(session);
        let root = root_record(session, "w1");
        let root_key = root.key.clone();
        upsert_record(&mut state.mappings, root);
        let sub = subthread_record(session, "w1", "p1", "agent-1");
        let sub_key = sub.key.clone();
        upsert_record(&mut state.mappings, sub);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceRenamed {
                workspace_id: "w1".into(),
                label: "Renamed".into(),
                sequence: 20,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::UpdateTitle(root_key, "Renamed".into())]
        );

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentStatusChanged {
                pane_id: "p1".into(),
                status: HerdrAgentStatus::Blocked,
                sequence: 21,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::UpdateStatus(sub_key, HerdrAgentStatus::Blocked)]
        );

        // Unknown resources produce no phantom updates.
        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceRenamed {
                workspace_id: "ghost".into(),
                label: "?".into(),
                sequence: 22,
            },
        );
        assert!(applied.actions.is_empty());
    }

    #[test]
    fn pane_exit_completes_the_subthread_with_a_tombstone() {
        let session = "alpha";
        let mut state = BridgeState::new(session);
        let sub = subthread_record(session, "w1", "p1", "agent-1");
        let key = sub.key.clone();
        upsert_record(&mut state.mappings, sub);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneExited {
                pane_id: "p1".into(),
                exit_code: Some(0),
                sequence: 30,
            },
        );
        assert_eq!(applied.actions, vec![ReconciliationAction::Archive(key.clone())]);
        assert!(state
            .mappings
            .get(&key.to_key_string())
            .unwrap()
            .is_tombstone());
    }

    #[test]
    fn pane_close_retains_a_tombstone() {
        let session = "alpha";
        let mut state = BridgeState::new(session);
        let sub = subthread_record(session, "w1", "p1", "agent-1");
        let key = sub.key.clone();
        upsert_record(&mut state.mappings, sub);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneClosed {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                sequence: 31,
            },
        );
        assert_eq!(applied.actions, vec![ReconciliationAction::Archive(key.clone())]);
        assert!(state.mappings[&key.to_key_string()].is_tombstone());
    }

    #[test]
    fn agent_detection_reconciles_like_the_snapshot_path() {
        let session = "alpha";
        let mut state = BridgeState::new(session);
        let restored = subthread_record(session, "w1", "p-old", "agent-1");
        let mut expected = restored.clone();
        expected.key.pane_id = Some("p-new".into());
        expected.last_seen_sequence = 40;
        upsert_record(&mut state.mappings, restored);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentDetected {
                pane_id: "p-new".into(),
                workspace_id: "w1".into(),
                agent_type: None,
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                sequence: 40,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::RestoreAgentSubthread(expected)]
        );

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentDetected {
                pane_id: "p4".into(),
                workspace_id: "w1".into(),
                agent_type: None,
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-9")),
                sequence: 41,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::CreateAgentSubthread(agent)]
                if agent.pane_id == "p4" && agent.workspace_id == "w1"
        ));
    }

    #[test]
    fn agent_session_restoration_rebinds_the_workspace_and_pane() {
        let session = "alpha";
        let old = subthread_record(session, "w-old", "p-old", "agent-1");
        let old_key = old.key.clone();
        let new_root = root_record(session, "w-new");
        let new_root_id = new_root.zed_root_thread_id;
        let new_key = HerdrMappingKey::subthread(
            session,
            "w-new",
            "p-new",
            HerdrAgentSessionIdentity::id("agent-1"),
        );
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, new_root);
        upsert_record(&mut state.mappings, old);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentDetected {
                pane_id: "p-new".into(),
                workspace_id: "w-new".into(),
                agent_type: None,
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                sequence: 1,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::RestoreAgentSubthread(record)]
                if record.key == new_key && record.zed_root_thread_id == new_root_id
        ));
        assert!(!state.mappings.contains_key(&old_key.to_key_string()));
        assert!(state.mappings.contains_key(&new_key.to_key_string()));
    }

    #[test]
    fn agent_session_restoration_preserves_source_on_live_destination_collision() {
        let session = "alpha";
        let old_root = root_record(session, "w-old");
        let new_root = root_record(session, "w-new");
        let restored = subthread_record(session, "w-old", "p-old", "agent-1");
        let destination = subthread_record(session, "w-new", "p-new", "agent-2");
        let old_key = restored.key.clone();
        let destination_key = destination.key.clone();
        let target_key = HerdrMappingKey::subthread(
            session,
            "w-new",
            "p-new",
            HerdrAgentSessionIdentity::id("agent-1"),
        );
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, old_root);
        upsert_record(&mut state.mappings, new_root);
        upsert_record(&mut state.mappings, restored);
        upsert_record(&mut state.mappings, destination);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentDetected {
                pane_id: "p-new".into(),
                workspace_id: "w-new".into(),
                agent_type: None,
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                sequence: 1,
            },
        );

        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::RecordConflict(key, message)]
                if key == &target_key && message.contains("destination pane")
        ));
        assert!(state.mappings.contains_key(&old_key.to_key_string()));
        assert!(state.mappings.contains_key(&destination_key.to_key_string()));
        assert!(!state.mappings.contains_key(&target_key.to_key_string()));
        assert_eq!(
            state
                .mappings
                .values()
                .filter(|record| !record.is_tombstone() && record.key.pane_id.is_some())
                .count(),
            2
        );
    }

    #[test]
    fn generated_operation_ids_are_unique() {
        let mut state = BridgeState::new("alpha");
        let first = initiate_workspace_focus(&mut state, "w1");
        // A second initiation replaces the pending operation.
        let second = initiate_workspace_focus(&mut state, "w2");
        assert_ne!(first, second);
    }

    #[test]
    fn workspace_created_emits_root_creation_for_its_snapshot() {
        let workspace = workspace_snapshot("w1", "Review");
        let mut state = BridgeState::new("alpha");
        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceCreated {
                workspace: workspace.clone(),
                sequence: 1,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::CreateWorkspaceRoot(workspace)]
        );
    }

    #[test]
    fn repeated_sequence_event_is_rejected() {
        let session = "alpha";
        let record = root_record(session, "w1");
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);
        state.last_sequence = 5;

        let applied = apply_event(&mut state, &focused_event("w1", "duplicate", 5));
        assert!(applied.actions.is_empty());
        assert_eq!(state.last_sequence, 5);
    }

    #[test]
    fn pane_focus_requires_matching_workspace_identity() {
        let session = "alpha";
        let record = subthread_record(session, "w1", "p1", "agent-1");
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneFocused {
                pane_id: "p1".into(),
                workspace_id: "other-workspace".into(),
                operation_id: Some("foreign".into()),
                sequence: 6,
            },
        );
        assert!(applied.actions.is_empty());
    }

    #[test]
    fn workspace_close_tombstones_descendant_subthreads() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let subthread = subthread_record(session, "w1", "p1", "agent-1");
        let subthread_key = subthread.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, root);
        upsert_record(&mut state.mappings, subthread);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceClosed {
                workspace_id: "w1".into(),
                sequence: 7,
            },
        );
        assert_eq!(applied.actions.len(), 2);
        assert!(state
            .mappings
            .get(&subthread_key.to_key_string())
            .expect("descendant tombstone retained")
            .is_tombstone());
    }
    
    #[test]
    fn snapshot_rebind_returns_current_identity_and_root_association() {
        let session = "alpha";
        let old_root = root_record(session, "w-old");
        let old = subthread_record(session, "w-old", "p-old", "agent-1");
        let current_root = root_record(session, "w-new");
        let current_root_id = current_root.zed_root_thread_id;
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, old_root);
        upsert_record(&mut mappings, old);
        upsert_record(&mut mappings, current_root);

        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w-new".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p-new".into(),
                workspace_id: "w-new".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::RestoreWorkspaceRoot(_),
                ReconciliationAction::RestoreAgentSubthread(record)
            ] if record.key == HerdrMappingKey::subthread(
                session,
                "w-new",
                "p-new",
                HerdrAgentSessionIdentity::id("agent-1"),
            ) && record.zed_root_thread_id == current_root_id
        ), "expected current rebind action, got {actions:?}");
    }

    #[test]
    fn snapshot_changed_identity_on_same_pane_is_a_conflict() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let existing = subthread_record(session, "w1", "p1", "agent-old");
        let old_key = existing.key.clone();
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root);
        upsert_record(&mut mappings, existing);
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-new")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::RestoreWorkspaceRoot(_),
                ReconciliationAction::RecordConflict(key, _)
            ] if key == &HerdrMappingKey::subthread(
                session,
                "w1",
                "p1",
                HerdrAgentSessionIdentity::id("agent-new"),
            )
        ));
        assert!(mappings.contains_key(&old_key.to_key_string()));
    }

    #[test]
    fn snapshot_skips_agents_below_tombstoned_workspace() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root);
        tombstone_record(&mut mappings, &HerdrMappingKey::workspace(session, "w1"), 4);
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [ReconciliationAction::RecordConflict(key, _)]
                if key == &HerdrMappingKey::workspace(session, "w1")
        ));
    }

    #[test]
    fn snapshot_tombstoned_agent_fallback_is_a_conflict() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let old = subthread_record(session, "w1", "p-old", "agent-1");
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, root);
        upsert_record(&mut mappings, old);
        tombstone_record(
            &mut mappings,
            &HerdrMappingKey::subthread(
                session,
                "w1",
                "p-old",
                HerdrAgentSessionIdentity::id("agent-1"),
            ),
            5,
        );
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p-new".into(),
                workspace_id: "w1".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::RestoreWorkspaceRoot(_),
                ReconciliationAction::RecordConflict(_, _)
            ]
        ));
    }

    #[test]
    fn pane_moved_rebinds_destination_before_later_events() {
        let session = "alpha";
        let root_old = root_record(session, "w-old");
        let root_new = root_record(session, "w-new");
        let old = subthread_record(session, "w-old", "p-old", "agent-1");
        let old_key = old.key.clone();
        let new_key = HerdrMappingKey::subthread(
            session,
            "w-new",
            "p-new",
            HerdrAgentSessionIdentity::id("agent-1"),
        );
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, root_old);
        upsert_record(&mut state.mappings, root_new);
        upsert_record(&mut state.mappings, old);
        let pane = HerdrPaneSnapshot {
            pane_id: "p-new".into(),
            workspace_id: "w-new".into(),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            ..Default::default()
        };
        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneMoved {
                pane,
                previous_pane_id: Some("p-old".into()),
                previous_workspace_id: Some("w-old".into()),
                previous_tab_id: None,
                sequence: 8,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::RestoreAgentSubthread(record)]
                if record.key == new_key
        ));
        assert!(!state.mappings.contains_key(&old_key.to_key_string()));
        assert!(state.mappings.contains_key(&new_key.to_key_string()));
        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentStatusChanged {
                pane_id: "p-new".into(),
                status: HerdrAgentStatus::Working,
                sequence: 9,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::UpdateStatus(key, _)] if key == &new_key
        ));
    }

    #[test]
    fn changed_identity_on_live_pane_conflicts_without_duplicate() {
        let session = "alpha";
        let existing = subthread_record(session, "w1", "p1", "agent-old");
        let existing_key = existing.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, existing);
        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentDetected {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                agent_type: None,
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-new")),
                sequence: 10,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::RecordConflict(key, _)]
                if key == &HerdrMappingKey::subthread(
                    session,
                    "w1",
                    "p1",
                    HerdrAgentSessionIdentity::id("agent-new"),
                )
        ));
        assert_eq!(
            state
                .mappings
                .values()
                .filter(|record| !record.is_tombstone())
                .count(),
            1
        );
        assert!(state.mappings.contains_key(&existing_key.to_key_string()));
    }

    #[test]
    fn delayed_reflection_of_superseded_focus_is_suppressed() {
        let session = "alpha";
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, root_record(session, "w1"));
        upsert_record(&mut state.mappings, root_record(session, "w2"));
        let first = initiate_workspace_focus(&mut state, "w1");
        let first_id = match first {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => unreachable!(),
        };
        let second = initiate_workspace_focus(&mut state, "w2");
        let second_id = match second {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => unreachable!(),
        };
        let applied = apply_event(&mut state, &focused_event("w1", &first_id, 1));
        assert!(applied.actions.is_empty());
        assert!(applied.outbound.is_empty());
        assert_eq!(
            state.pending_focus.as_ref().map(|pending| pending.operation_id.as_str()),
            Some(second_id.as_str())
        );
    }

    #[test]
    fn persisted_mapping_sequence_rejects_older_event_after_restart() {
        let session = "alpha";
        let mut record = root_record(session, "w1");
        record.last_seen_sequence = 10;
        let key = record.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);
        let applied = apply_event(&mut state, &focused_event("w1", "old", 9));
        assert!(applied.actions.is_empty());
        assert_eq!(state.last_sequence, 0);
        assert_eq!(
            state.mappings[&key.to_key_string()].last_seen_sequence,
            10
        );
    }

    #[test]
    fn zero_sequence_close_marks_resource_stale_without_tombstoning() {
        let session = "alpha";
        let record = root_record(session, "w1");
        let key = record.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, record);
        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceClosed {
                workspace_id: "w1".into(),
                sequence: 0,
            },
        );
        assert!(matches!(
            &applied.actions[..],
            [ReconciliationAction::RecordConflict(conflict_key, message)]
                if conflict_key == &key && message.contains("sequence")
        ));
        assert!(!state.mappings[&key.to_key_string()].is_tombstone());
        assert_eq!(state.last_sequence, 0);
    }

    #[test]
    fn sequence_less_buffered_non_lifecycle_events_apply_safe_effects() {
        let session = "alpha";
        let root = root_record(session, "w1");
        let root_key = root.key.clone();
        let subthread = subthread_record(session, "w1", "p1", "agent-1");
        let subthread_key = subthread.key.clone();
        let mut state = BridgeState::new(session);
        upsert_record(&mut state.mappings, root);
        upsert_record(&mut state.mappings, subthread);

        let applied = apply_event(
            &mut state,
            &HerdrEvent::WorkspaceFocused {
                workspace_id: "w1".into(),
                operation_id: None,
                sequence: 0,
            },
        );
        assert_eq!(applied.actions, vec![ReconciliationAction::Activate(root_key)]);
        assert!(applied.outbound.is_empty());

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneFocused {
                pane_id: "p1".into(),
                workspace_id: "w1".into(),
                operation_id: None,
                sequence: 0,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::Activate(subthread_key.clone())]
        );
        assert!(applied.outbound.is_empty());

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneAgentStatusChanged {
                pane_id: "p1".into(),
                status: HerdrAgentStatus::Working,
                sequence: 0,
            },
        );
        assert_eq!(
            applied.actions,
            vec![ReconciliationAction::UpdateStatus(
                subthread_key.clone(),
                HerdrAgentStatus::Working
            )]
        );
        assert!(applied.outbound.is_empty());

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneOutput {
                pane_id: "p1".into(),
                revision: 1,
                delta: "buffered".into(),
                sequence: 0,
            },
        );
        assert!(applied.actions.is_empty());
        assert!(applied.outbound.is_empty());

        let applied = apply_event(
            &mut state,
            &HerdrEvent::PaneScrollChanged {
                pane_id: "p1".into(),
                sequence: 0,
            },
        );
        assert!(applied.actions.is_empty());
        assert!(applied.outbound.is_empty());

        assert_eq!(state.last_sequence, 0);
        assert_eq!(state.mappings[&subthread_key.to_key_string()].last_seen_sequence, 0);
    }


    #[test]
    fn rebind_without_current_root_conflicts_and_preserves_old_mapping() {
        let session = "alpha";
        let old = subthread_record(session, "w-old", "p-old", "agent-1");
        let old_key = old.key.clone();
        let mut mappings = SessionMappings::new();
        upsert_record(&mut mappings, old);
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w-new".into(),
            agents: vec![HerdrAgentSnapshot {
                pane_id: "p-new".into(),
                workspace_id: "w-new".into(),
                session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let actions = reconcile_snapshot(session, &[workspace], &mappings);
        assert!(matches!(
            &actions[..],
            [
                ReconciliationAction::CreateWorkspaceRoot(_),
                ReconciliationAction::RecordConflict(key, _)
            ] if key == &HerdrMappingKey::subthread(
                session,
                "w-new",
                "p-new",
                HerdrAgentSessionIdentity::id("agent-1"),
            )
        ));
        assert!(mappings.contains_key(&old_key.to_key_string()));
    }

}
