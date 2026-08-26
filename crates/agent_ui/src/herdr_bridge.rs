use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use chrono::Utc;
use db::kvp::KeyValueStore;
use gpui::{App, AppContext as _, Context, Entity, Global, Task, TaskExt, WindowId};
use workspace::PathList;

use crate::{
    herdr_client::{
        HerdrAgentSessionIdentity, HerdrAgentSnapshot, HerdrAgentStatus, HerdrApi, HerdrBootstrap,
        HerdrClientError, HerdrClientHandle, HerdrEvent, HerdrEventCursor, HerdrSnapshot,
        HerdrWorkspaceSnapshot,
    },
    herdr_mapping_store::{
        HerdrLifecycleState, HerdrMappingKey, HerdrMappingRecord, HerdrMappingStore,
        SessionMappings, upsert_record,
    },
    herdr_state::{
        AppliedEvent, BridgeState, FocusTarget, HerdrOperationOrigin, OutboundRequest,
        ReconciliationAction, apply_event, initiate_pane_focus, initiate_workspace_focus,
        reconcile_snapshot,
    },
    herdr_transport::HerdrEndpoint,
    thread_metadata_store::{
        HERDR_AGENT_ID, ThreadId, ThreadMetadata, ThreadMetadataStore, WorktreePaths,
    },
};


/// The user-visible selection used to bind one Zed window to one Herdr session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HerdrSessionSelection {
    Default,
    Named(String),
    Explicit(String),
}

impl Default for HerdrSessionSelection {
    fn default() -> Self {
        Self::Default
    }
}

impl HerdrSessionSelection {
    fn endpoint(&self) -> HerdrEndpoint {
        match self {
            Self::Default => HerdrEndpoint::Default,
            Self::Named(name) => HerdrEndpoint::NamedSession(name.clone()),
            Self::Explicit(path) => HerdrEndpoint::Explicit(path.clone()),
        }
    }

    fn session_name(&self) -> String {
        match self {
            Self::Default => std::env::var("HERDR_SESSION")
                .ok()
                .filter(|session| !session.is_empty())
                .unwrap_or_else(|| "default".to_string()),
            Self::Named(name) => name.clone(),
            Self::Explicit(path) => path.clone(),
        }
    }
}

/// Connection state for a per-window Herdr bridge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HerdrConnectionStatus {
    Unavailable,
    Reconnecting,
    Synchronizing,
    Ready,
}

/// Events consumed by AgentPanel and other UI surfaces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HerdrBridgeEvent {
    StatusChanged(HerdrConnectionStatus),
    RootCreated {
        workspace_id: String,
        thread_id: ThreadId,
    },
    RootRenamed {
        workspace_id: String,
        thread_id: ThreadId,
        title: String,
    },
    RootFocused {
        workspace_id: String,
        thread_id: ThreadId,
    },
    RootClosed {
        workspace_id: String,
        thread_id: ThreadId,
    },
    SubthreadFocused {
        key: HerdrMappingKey,
        thread_id: ThreadId,
    },
    Conflict {
        key: HerdrMappingKey,
        message: String,
    },
    RequestFailed {
        workspace_id: Option<String>,
        operation: String,
        message: String,
    },
    SubthreadCreated {
        key: HerdrMappingKey,
        thread_id: ThreadId,
        pane_id: String,
        session: crate::herdr_client::HerdrAgentSessionIdentity,
        title: String,
        status: crate::herdr_client::HerdrAgentStatus,
    },
    SubthreadUpdated {
        key: HerdrMappingKey,
        thread_id: ThreadId,
        pane_id: String,
        title: Option<String>,
        status: Option<crate::herdr_client::HerdrAgentStatus>,
    },
    SubthreadOutput {
        key: HerdrMappingKey,
        thread_id: ThreadId,
        pane_id: String,
        revision: u64,
        output: String,
    },
    SubthreadClosed {
        key: HerdrMappingKey,
        thread_id: ThreadId,
        pane_id: String,
    },
}

impl gpui::EventEmitter<HerdrBridgeEvent> for HerdrThreadBridge {}

/// GPUI entity holding one Herdr session's mapping and lifecycle state.
///
/// The pure event/state transitions remain in [`crate::herdr_state`]. This
/// type translates those transitions into root metadata, persisted mappings,
/// and UI-facing events.
pub(crate) struct HerdrThreadBridge {
    window_id: Option<WindowId>,
    selection: HerdrSessionSelection,
    client: Option<Arc<dyn HerdrApi>>,
    event_receiver: Option<async_channel::Receiver<HerdrEventCursor>>,
    state: BridgeState,
    root_metadata: HashMap<String, ThreadMetadata>,
    agent_snapshots: HashMap<String, HerdrAgentSnapshot>,
    pane_outputs: HashMap<String, (u64, String)>,
    status: HerdrConnectionStatus,
    events: Vec<HerdrBridgeEvent>,
    outbound_requests: Vec<OutboundRequest>,
    pending_authoritative_focus: Option<String>,
    current_focus_workspace: Option<String>,
    metadata_dirty: HashSet<String>,
    active_subscription_id: Option<String>,
    active_subscription_ids: HashSet<String>,
    active: Arc<AtomicBool>,
    sync_generation: Arc<AtomicU64>,
    sync_cancel_tx: async_channel::Sender<()>,
    sync_cancel_rx: async_channel::Receiver<()>,
    sync_started: bool,
}
impl HerdrThreadBridge {
    fn new(
        window_id: Option<WindowId>,
        selection: HerdrSessionSelection,
        client: Option<Arc<dyn HerdrApi>>,
        event_receiver: Option<async_channel::Receiver<HerdrEventCursor>>,
        mappings: SessionMappings,
    ) -> Self {
        let session = selection.session_name();
        let (sync_cancel_tx, sync_cancel_rx) = async_channel::unbounded();
        Self {
            window_id,
            selection,
            client,
            event_receiver,
            state: BridgeState {
                session,
                mappings,
                ..BridgeState::default()
            },
            root_metadata: HashMap::default(),
            status: HerdrConnectionStatus::Unavailable,
            events: Vec::new(),
            outbound_requests: Vec::new(),
            pending_authoritative_focus: None,
            current_focus_workspace: None,
            metadata_dirty: HashSet::default(),
            active_subscription_id: None,
            agent_snapshots: HashMap::default(),
            pane_outputs: HashMap::default(),
            active_subscription_ids: HashSet::default(),
            active: Arc::new(AtomicBool::new(true)),
            sync_generation: Arc::new(AtomicU64::new(0)),
            sync_cancel_tx,
            sync_cancel_rx,
            sync_started: false,
        }
    }

    /// Constructor used by bridge tests and by callers that already have a
    /// session-qualified mapping state.
    #[cfg(test)]
    pub(crate) fn for_test(session: impl Into<String>) -> Self {
        let session = session.into();
        Self::new(
            None,
            HerdrSessionSelection::Named(session),
            None,
            None,
            SessionMappings::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn for_test_in_session(session: impl Into<String>) -> Self {
        Self::for_test(session)
    }

    pub(crate) fn window_id(&self) -> Option<WindowId> {
        self.window_id
    }

    pub(crate) fn session_name(&self) -> &str {
        &self.state.session
    }

    pub(crate) fn selection(&self) -> &HerdrSessionSelection {
        &self.selection
    }

    pub(crate) fn status(&self) -> HerdrConnectionStatus {
        self.status
    }

    pub(crate) fn root_mapping(&self, workspace_id: &str) -> Option<&HerdrMappingRecord> {
        let key = self.state.workspace_key(workspace_id).to_key_string();
        self.state.mappings.get(&key)
    }

    pub(crate) fn root_mapping_for_thread(
        &self,
        thread_id: ThreadId,
    ) -> Option<&HerdrMappingRecord> {
        self.state.mappings.values().find(|record| {
            record.key.pane_id.is_none()
                && record.zed_root_thread_id == thread_id
                && !record.is_tombstone()
        })
    }

    pub(crate) fn root_metadata(&self, workspace_id: &str) -> Option<&ThreadMetadata> {
        self.root_metadata.get(workspace_id)
    }

    pub(crate) fn subthread_snapshots(&self, workspace_id: &str) -> Vec<HerdrAgentSnapshot> {
        self.agent_snapshots
            .values()
            .filter(|snapshot| snapshot.workspace_id == workspace_id)
            .cloned()
            .collect()
    }

    pub(crate) fn root_thread_id(&self, workspace_id: &str) -> Option<ThreadId> {
        self.root_mapping(workspace_id)
            .filter(|record| !record.is_tombstone())
            .map(|record| record.zed_root_thread_id)
    }

    pub(crate) fn is_root_thread(&self, thread_id: ThreadId) -> bool {
        self.root_mapping_for_thread(thread_id)
            .is_some_and(|record| !record.is_tombstone())
    }

    pub(crate) fn root_title(&self, workspace_id: &str) -> Option<String> {
        self.root_metadata(workspace_id)
            .and_then(|metadata| metadata.title().map(|title| title.to_string()))
    }

    pub(crate) fn take_events(&mut self) -> Vec<HerdrBridgeEvent> {
        std::mem::take(&mut self.events)
    }

    pub(crate) fn take_outbound_requests(&mut self) -> Vec<OutboundRequest> {
        std::mem::take(&mut self.outbound_requests)
    }

    fn set_status(&mut self, status: HerdrConnectionStatus) {
        if self.status != status {
            self.status = status;
            self.events
                .push(HerdrBridgeEvent::StatusChanged(status));
        }
    }

    fn subscription_ended_requires_reconnect(&self, event: &HerdrEvent) -> bool {
        let HerdrEvent::Unknown {
            event: event_name,
            data,
        } = event
        else {
            return false;
        };
        if event_name != "subscription_ended" {
            return false;
        }
        let subscription_id = serde_json::from_str::<serde_json::Value>(data.get())
            .ok()
            .and_then(|value| {
                value
                    .get("subscription_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            });
        subscription_id.as_deref().map_or(true, |id| {
            self.active_subscription_ids.contains(id)
        })

    }
    fn emit_new_events(&self, start: usize, cx: &mut Context<Self>) {
        for event in self.events.iter().skip(start) {
            cx.emit(event.clone());
        }
    }


    fn event_for_state(&self, event: &HerdrEvent) -> HerdrEvent {
        let Some(origin) = event.operation_origin() else {
            return event.clone();
        };
        if origin.eq_ignore_ascii_case("zed") {
            return event.clone();
        }
        match event {
            HerdrEvent::WorkspaceFocused {
                workspace_id,
                sequence,
                ..
            } => HerdrEvent::WorkspaceFocused {
                workspace_id: workspace_id.clone(),
                operation_id: None,
                sequence: *sequence,
            },
            HerdrEvent::PaneFocused {
                pane_id,
                workspace_id,
                sequence,
                ..
            } => HerdrEvent::PaneFocused {
                pane_id: pane_id.clone(),
                workspace_id: workspace_id.clone(),
                operation_id: None,
                sequence: *sequence,
            },
            _ => event.clone(),
        }
    }
    fn focus_is_fenced(&self, event: &HerdrEvent) -> bool {
        let operation_id = match event {
            HerdrEvent::WorkspaceFocused { operation_id, .. }
            | HerdrEvent::PaneFocused { operation_id, .. } => operation_id.as_deref(),
            _ => None,
        };
        let Some(operation_id) = operation_id else {
            return false;
        };
        if self.state.issued_focus.contains_key(operation_id) {
            return true;
        }

        let (workspace_id, pane_id) = match event {
            HerdrEvent::WorkspaceFocused { workspace_id, .. } => {
                (workspace_id.as_str(), None)
            }
            HerdrEvent::PaneFocused {
                pane_id,
                workspace_id,
                ..
            } => (workspace_id.as_str(), Some(pane_id.as_str())),
            _ => return false,
        };
        self.state.pending_focus.as_ref().is_some_and(|pending| {
            pending.origin == HerdrOperationOrigin::Zed
                && pending.operation_id == operation_id
                && Self::focus_target_matches_event(&pending.target, workspace_id, pane_id)
        })
    }
    fn focus_target_matches_event(
        target: &FocusTarget,
        workspace_id: &str,
        pane_id: Option<&str>,
    ) -> bool {
        match (target, pane_id) {
            (FocusTarget::Workspace(target_workspace), None) => {
                target_workspace == workspace_id
            }
            (
                FocusTarget::Pane {
                    workspace_id: target_workspace,
                    pane_id: target_pane,
                },
                Some(pane),
            ) => target_workspace == workspace_id && target_pane == pane,
            _ => false,
        }
    }

    fn focus_event_is_stale(&self, event: &HerdrEvent) -> bool {
        let sequence = event.sequence();
        let (workspace_id, pane_id) = match event {
            HerdrEvent::WorkspaceFocused { workspace_id, .. } => {
                (workspace_id.as_str(), None)
            }
            HerdrEvent::PaneFocused {
                pane_id,
                workspace_id,
                ..
            } => (workspace_id.as_str(), Some(pane_id.as_str())),
            _ => return false,
        };
        if sequence == 0 {
            // Sequence-less focus has no ordering information. Only a
            // reflection tied to an issued/pending local operation is stale;
            // an external event may legitimately target a superseded local
            // focus target.
            return self.focus_is_fenced(event);
        }
        if sequence <= self.state.last_sequence {
            return true;
        }

        self.state
            .mappings
            .values()
            .filter(|record| {
                record.key.session == self.state.session
                    && record.key.workspace_id == workspace_id
                    && pane_id.map_or(true, |pane| {
                        record.key.pane_id.as_deref() == Some(pane)
                    })
            })
            .any(|record| sequence <= record.last_seen_sequence)
    }

    fn note_focus_event(
        &mut self,
        event: &HerdrEvent,
        applied: &AppliedEvent,
        fenced: bool,
        stale: bool,
    ) {
        if !stale
            && (fenced
                || applied
                    .actions
                    .iter()
                    .any(|action| matches!(action, ReconciliationAction::Activate(_))))
        {
            self.current_focus_workspace = event.workspace_id().map(ToOwned::to_owned);
        }
    }

    /// Apply one pushed Herdr event without requiring a GPUI context. This is
    /// intentionally also used by deterministic bridge tests.
    pub(crate) fn apply_event(&mut self, event: HerdrEvent) {
        let fenced = self.focus_is_fenced(&event);
        let stale = self.focus_event_is_stale(&event);
        if stale && !fenced {
            return;
        }
        let state_event = self.event_for_state(&event);
        let applied = apply_event(&mut self.state, &state_event);
        self.note_focus_event(&event, &applied, fenced, stale);
        self.apply_actions(applied);
        self.emit_subthread_event(&event);
    }
    fn apply_event_in_context(&mut self, event: HerdrEvent, cx: &mut Context<Self>) {
        let start = self.events.len();
        let fenced = self.focus_is_fenced(&event);
        let stale = self.focus_event_is_stale(&event);
        if stale && !fenced {
            return;
        }
        let state_event = self.event_for_state(&event);
        let applied = apply_event(&mut self.state, &state_event);
        self.note_focus_event(&event, &applied, fenced, stale);
        self.apply_actions_in_context(applied, cx);
        self.emit_subthread_event(&event);
        self.emit_new_events(start, cx);
        self.persist_mappings(cx);
    }


    fn apply_actions(&mut self, applied: AppliedEvent) {
        for outbound in applied.outbound {
            self.outbound_requests.push(outbound);
        }
        for action in applied.actions {
            self.apply_action(action);
        }
    }

    fn apply_actions_in_context(&mut self, applied: AppliedEvent, cx: &mut Context<Self>) {
        for outbound in applied.outbound {
            self.outbound_requests.push(outbound);
        }
        for action in applied.actions {
            self.apply_action_in_context(action, cx);
        }
    }

    fn apply_action(&mut self, action: ReconciliationAction) {
        match action {
            ReconciliationAction::CreateWorkspaceRoot(workspace) => {
                self.create_or_restore_root(&workspace, false);
            }
            ReconciliationAction::RestoreWorkspaceRoot(record) => {
                self.restore_root_mapping(record);
            }
            ReconciliationAction::CreateAgentSubthread(agent) => {
                self.create_agent_mapping(agent);
            }
            ReconciliationAction::RestoreAgentSubthread(record) => {
                self.restore_agent_mapping(record);
            }
            ReconciliationAction::UpdateTitle(key, title) => {
                self.update_title(&key, title);
            }
            ReconciliationAction::UpdateStatus(_, _) => {}
            ReconciliationAction::Activate(key) => self.activate_mapping(&key),
            ReconciliationAction::Archive(key) => self.archive_mapping(&key),
            ReconciliationAction::RecordConflict(key, message) => {
                self.events.push(HerdrBridgeEvent::Conflict { key, message });
            }
        }
    }

    fn apply_action_in_context(&mut self, action: ReconciliationAction, cx: &mut Context<Self>) {
        let event_before = self.events.len();
        let mapping_before = self.state.mappings.clone();
        self.apply_action(action);
        self.persist_metadata_changes(&mapping_before, cx);
        self.persist_dirty_metadata(cx);
        self.emit_new_events(event_before, cx);
    }

    fn create_or_restore_root(
        &mut self,
        workspace: &HerdrWorkspaceSnapshot,
        restored: bool,
    ) -> ThreadId {
        let key = self.state.workspace_key(&workspace.workspace_id);
        let key_string = key.to_key_string();
        let existing = self.state.mappings.get(&key_string).cloned();
        let record = match existing {
            Some(record) => record,
            None => {
                let record = HerdrMappingRecord::root(
                    self.state.session.clone(),
                    workspace.workspace_id.clone(),
                    ThreadId::new(),
                );
                if !upsert_record(&mut self.state.mappings, record.clone()) {
                    return record.zed_root_thread_id;
                }
                record
            }
        };
        let metadata = self.metadata_for_workspace(&record, workspace);
        self.root_metadata
            .insert(workspace.workspace_id.clone(), metadata);
        self.metadata_dirty.insert(workspace.workspace_id.clone());
        if !restored {
            self.events.push(HerdrBridgeEvent::RootCreated {
                workspace_id: workspace.workspace_id.clone(),
                thread_id: record.zed_root_thread_id,
            });
        }
        record.zed_root_thread_id
    }

    fn restore_root_mapping(&mut self, record: HerdrMappingRecord) {
        let workspace_id = record.key.workspace_id.clone();
        self.state
            .mappings
            .insert(record.key.to_key_string(), record);
        if let Some(metadata) = self.root_metadata.get_mut(&workspace_id) {
            metadata.archived = false;
            metadata.updated_at = Utc::now();
            self.metadata_dirty.insert(workspace_id);
        }
    }

    fn create_agent_mapping(&mut self, agent: HerdrAgentSnapshot) {
        let Some(identity) = agent.session_identity.clone() else {
            return;
        };
        let key = self
            .state
            .subthread_key(&agent.workspace_id, &agent.pane_id, &identity);
        let root_thread_id = self
            .root_mapping(&agent.workspace_id)
            .map(|record| record.zed_root_thread_id);
        let Some(root_thread_id) = root_thread_id else {
            return;
        };
        let record = HerdrMappingRecord {
            key: key.clone(),
            zed_root_thread_id: root_thread_id,
            zed_subthread_session_id: Some(identity.value.clone()),
            worktree_or_cwd_identity: agent.cwd.clone(),
            last_seen_sequence: agent.last_seen_sequence,
            lifecycle: HerdrLifecycleState::Active,
        };
        let _ = upsert_record(&mut self.state.mappings, record);
        self.agent_snapshots.insert(key.to_key_string(), agent);
    }
    fn subthread_record_for_pane(
        &self,
        workspace_id: Option<&str>,
        pane_id: &str,
    ) -> Option<HerdrMappingRecord> {
        self.state
            .mappings
            .values()
            .find(|record| {
                record.key.pane_id.as_deref() == Some(pane_id)
                    && workspace_id.is_none_or(|workspace| record.key.workspace_id == workspace)
            })
            .cloned()
    }

    fn emit_subthread_event(&mut self, event: &HerdrEvent) {
        match event {
            HerdrEvent::PaneAgentDetected {
                pane_id,
                workspace_id,
                agent_type,
                session_identity,
                ..
            } => {
                let Some(session) = session_identity.clone() else {
                    return;
                };
                let snapshot = HerdrAgentSnapshot {
                    pane_id: pane_id.clone(),
                    workspace_id: workspace_id.clone(),
                    agent_type: agent_type.clone(),
                    session_identity: Some(session.clone()),
                    status: HerdrAgentStatus::default(),
                    title: agent_type.clone(),
                    ..Default::default()
                };
                self.create_agent_mapping(snapshot);
                let Some(record) = self.subthread_record_for_pane(Some(workspace_id), pane_id)
                else {
                    return;
                };
                self.events.push(HerdrBridgeEvent::SubthreadCreated {
                    key: record.key,
                    thread_id: record.zed_root_thread_id,
                    pane_id: pane_id.clone(),
                    session,
                    title: agent_type.clone().unwrap_or_else(|| pane_id.clone()),
                    status: HerdrAgentStatus::default(),
                });
            }
            HerdrEvent::PaneUpdated { pane, .. } => {
                let Some(session) = pane.session_identity.clone() else {
                    return;
                };
                let snapshot = HerdrAgentSnapshot {
                    pane_id: pane.pane_id.clone(),
                    workspace_id: pane.workspace_id.clone(),
                    agent_type: pane.agent_type.clone(),
                    session_identity: Some(session),
                    status: pane.status.clone(),
                    title: pane.title.clone(),
                    cwd: pane.cwd.clone(),
                    ..Default::default()
                };
                let had_mapping = self
                    .subthread_record_for_pane(Some(&pane.workspace_id), &pane.pane_id)
                    .is_some();
                self.create_agent_mapping(snapshot);
                let Some(record) =
                    self.subthread_record_for_pane(Some(&pane.workspace_id), &pane.pane_id)
                else {
                    return;
                };
                if !had_mapping {
                    self.events.push(HerdrBridgeEvent::SubthreadCreated {
                        key: record.key,
                        thread_id: record.zed_root_thread_id,
                        pane_id: pane.pane_id.clone(),
                        session: pane.session_identity.clone().unwrap_or_else(|| {
                            HerdrAgentSessionIdentity::id(pane.pane_id.clone())
                        }),
                        title: pane
                            .title
                            .clone()
                            .unwrap_or_else(|| pane.pane_id.clone()),
                        status: pane.status.clone(),
                    });
                } else {
                    self.events.push(HerdrBridgeEvent::SubthreadUpdated {
                        key: record.key,
                        thread_id: record.zed_root_thread_id,
                        pane_id: pane.pane_id.clone(),
                        title: pane.title.clone(),
                        status: Some(pane.status.clone()),
                    });
                }
            }
            HerdrEvent::PaneAgentStatusChanged {
                pane_id,
                status,
                ..
            } => {
                let Some(record) = self.subthread_record_for_pane(None, pane_id) else {
                    return;
                };
                let key_string = record.key.to_key_string();
                if let Some(snapshot) = self.agent_snapshots.get_mut(&key_string) {
                    snapshot.status = status.clone();
                }
                self.events.push(HerdrBridgeEvent::SubthreadUpdated {
                    key: record.key,
                    thread_id: record.zed_root_thread_id,
                    pane_id: pane_id.clone(),
                    title: None,
                    status: Some(status.clone()),
                });
            }
            HerdrEvent::PaneOutput {
                pane_id,
                revision,
                delta,
                ..
            } => {
                let Some(record) = self.subthread_record_for_pane(None, pane_id) else {
                    return;
                };
                let output = self
                    .pane_outputs
                    .entry(pane_id.clone())
                    .or_insert_with(|| (0, String::new()));
                if *revision <= output.0 {
                    return;
                }
                if output.0 == 0 || *revision == output.0 + 1 {
                    output.1.push_str(delta);
                } else {
                    output.1 = delta.clone();
                }
                output.0 = *revision;
                self.events.push(HerdrBridgeEvent::SubthreadOutput {
                    key: record.key,
                    thread_id: record.zed_root_thread_id,
                    pane_id: pane_id.clone(),
                    revision: *revision,
                    output: output.1.clone(),
                });
            }
            HerdrEvent::PaneClosed {
                pane_id,
                workspace_id,
                ..
            } => {
                self.emit_subthread_closed(Some(workspace_id), pane_id);
            }
            HerdrEvent::PaneExited { pane_id, .. } => {
                self.emit_subthread_closed(None, pane_id);
            }
            _ => {}
        }
    }

    fn restore_agent_mapping(&mut self, record: HerdrMappingRecord) {
        self.state
            .mappings
            .insert(record.key.to_key_string(), record);
    }

    fn emit_subthread_closed(&mut self, workspace_id: Option<&String>, pane_id: &str) {
        let Some(record) = self.subthread_record_for_pane(workspace_id.map(String::as_str), pane_id)
        else {
            return;
        };
        self.agent_snapshots.remove(&record.key.to_key_string());
        self.pane_outputs.remove(pane_id);
        self.events.push(HerdrBridgeEvent::SubthreadClosed {
            key: record.key,
            thread_id: record.zed_root_thread_id,
            pane_id: pane_id.to_string(),
        });
    }
    fn update_title(&mut self, key: &HerdrMappingKey, title: String) {
        if key.pane_id.is_some() {
            return;
        }
        let Some(metadata) = self.root_metadata.get_mut(&key.workspace_id) else {
            return;
        };
        metadata.title = Some(title.clone().into());
        metadata.updated_at = Utc::now();
        self.metadata_dirty.insert(key.workspace_id.clone());
        self.events.push(HerdrBridgeEvent::RootRenamed {
            workspace_id: key.workspace_id.clone(),
            thread_id: metadata.thread_id,
            title,
        });
    }

    fn activate_mapping(&mut self, key: &HerdrMappingKey) {
        if key.pane_id.is_none() {
            if let Some(metadata) = self.root_metadata.get(&key.workspace_id) {
                self.events.push(HerdrBridgeEvent::RootFocused {
                    workspace_id: key.workspace_id.clone(),
                    thread_id: metadata.thread_id,
                });
            }
        } else if let Some(record) = self.state.mappings.get(&key.to_key_string()) {
            self.events.push(HerdrBridgeEvent::SubthreadFocused {
                key: key.clone(),
                thread_id: record.zed_root_thread_id,
            });
        }
    }

    fn archive_mapping(&mut self, key: &HerdrMappingKey) {
        if key.pane_id.is_some() {
            return;
        }
        if let Some(metadata) = self.root_metadata.get_mut(&key.workspace_id) {
            metadata.archived = true;
            metadata.updated_at = Utc::now();
            self.metadata_dirty.insert(key.workspace_id.clone());
            self.events.push(HerdrBridgeEvent::RootClosed {
                workspace_id: key.workspace_id.clone(),
                thread_id: metadata.thread_id,
            });
        }
    }

    fn metadata_for_workspace(
        &self,
        record: &HerdrMappingRecord,
        workspace: &HerdrWorkspaceSnapshot,
    ) -> ThreadMetadata {
        let paths = workspace
            .paths
            .iter()
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let path_list = PathList::new(&paths);
        let mut metadata = ThreadMetadata {
            thread_id: record.zed_root_thread_id,
            session_id: None,
            agent_id: HERDR_AGENT_ID.clone(),
            title: Some(workspace.label.clone().into()),
            title_override: None,
            updated_at: Utc::now(),
            created_at: Some(Utc::now()),
            interacted_at: None,
            worktree_paths: WorktreePaths::from_folder_paths(&path_list),
            remote_connection: None,
            archived: false,
            user_order: None,
        };
        if let Some(existing) = self.root_metadata.get(&workspace.workspace_id) {
            metadata.created_at = existing.created_at;
            metadata.title_override = existing.title_override.clone();
            if metadata.title_override.is_some() {
                metadata.title = existing.title.clone();
            }
        }
        metadata
    }

    fn persist_metadata_changes(
        &mut self,
        previous_mappings: &SessionMappings,
        cx: &mut Context<Self>,
    ) {
        let Some(store) = ThreadMetadataStore::try_global(cx) else {
            return;
        };
        let changed_roots = self
            .state
            .mappings
            .values()
            .filter(|record| record.key.pane_id.is_none())
            .filter(|record| {
                previous_mappings
                    .get(&record.key.to_key_string())
                    != Some(record)
            })
            .map(|record| record.key.workspace_id.clone())
            .collect::<Vec<_>>();
        for workspace_id in changed_roots {
            if let Some(metadata) = self.root_metadata.get(&workspace_id).cloned() {
                if metadata.archived {
                    // `archive` only updates an existing row. Save first so
                    // a root closed during its first bootstrap is durable.
                    store.update(cx, |store, cx| {
                        store.save(metadata.clone(), cx);
                        store.archive(metadata.thread_id, None, cx);
                    });
                } else {
                    store.update(cx, |store, cx| store.save(metadata, cx));
                }
            }
        }
        for metadata in self.root_metadata.values() {
            if !previous_mappings
                .values()
                .any(|record| record.zed_root_thread_id == metadata.thread_id)
            {
                let metadata = metadata.clone();
                store.update(cx, |store, cx| store.save(metadata, cx));
            }
        }
    }
    fn persist_dirty_metadata(&mut self, cx: &mut Context<Self>) {
        let Some(store) = ThreadMetadataStore::try_global(cx) else {
            return;
        };
        let dirty = std::mem::take(&mut self.metadata_dirty);
        for workspace_id in dirty {
            let Some(metadata) = self.root_metadata.get(&workspace_id).cloned() else {
                continue;
            };
            if metadata.archived {
                store.update(cx, |store, cx| {
                    store.save(metadata.clone(), cx);
                    store.archive(metadata.thread_id, None, cx);
                });
            } else {
                store.update(cx, |store, cx| store.save(metadata, cx));
            }
        }
    }

    fn persist_all_root_metadata(&mut self, cx: &mut Context<Self>) {
        let Some(store) = ThreadMetadataStore::try_global(cx) else {
            return;
        };
        let metadata = self.root_metadata.values().cloned().collect::<Vec<_>>();
        self.metadata_dirty.clear();
        for metadata in metadata {
            if metadata.archived {
                store.update(cx, |store, cx| {
                    store.save(metadata.clone(), cx);
                    store.archive(metadata.thread_id, None, cx);
                });
            } else {
                store.update(cx, |store, cx| store.save(metadata, cx));
            }
        }
    }

    fn persist_mappings(&self, cx: &mut Context<Self>) {
        let kvp = KeyValueStore::global(cx);
        let session = self.state.session.clone();
        let mappings = self.state.mappings.clone();
        cx.background_spawn(async move {
            HerdrMappingStore::save_session(&kvp, &session, &mappings).await
        })
        .detach_and_log_err(cx);
    }

    fn apply_snapshot(&mut self, snapshot: HerdrSnapshot) {
        if !snapshot.session.is_empty() && snapshot.session != self.state.session {
            self.state.session = snapshot.session;
        }
        self.current_focus_workspace = None;
        self.pending_authoritative_focus = snapshot.active_workspace_id;
        let actions = reconcile_snapshot(
            &self.state.session,
            &snapshot.workspaces,
            &self.state.mappings,
        );
        for action in actions {
            self.apply_action(action);
        }
        for workspace in &snapshot.workspaces {
            let key = self.state.workspace_key(&workspace.workspace_id);
            if self
                .state
                .mappings
                .get(&key.to_key_string())
                .is_some_and(|record| !record.is_tombstone())
            {
                let record = self
                    .state
                    .mappings
                    .get(&key.to_key_string())
                    .cloned();
                if let Some(record) = record {
                    self.create_or_restore_root(workspace, true);
                    if let Some(metadata) = self.root_metadata.get_mut(&workspace.workspace_id) {
                        metadata.thread_id = record.zed_root_thread_id;
                        self.metadata_dirty.insert(workspace.workspace_id.clone());
                    }
                }
            }
        }
        for workspace in &snapshot.workspaces {
            for agent in &workspace.agents {
                let Some(session) = agent.session_identity.clone() else {
                    continue;
                };
                self.create_agent_mapping(agent.clone());
                let Some(record) =
                    self.subthread_record_for_pane(Some(&workspace.workspace_id), &agent.pane_id)
                else {
                    continue;
                };
                self.events.push(HerdrBridgeEvent::SubthreadCreated {
                    key: record.key,
                    thread_id: record.zed_root_thread_id,
                    pane_id: agent.pane_id.clone(),
                    session,
                    title: agent
                        .title
                        .clone()
                        .or_else(|| agent.agent_type.clone())
                        .unwrap_or_else(|| agent.pane_id.clone()),
                    status: agent.status.clone(),
                });
            }
        }
        // Snapshot sequence is not a reliable replay cursor across Herdr
        // versions. The client-provided event boundary controls replay.
        self.state.last_sequence = 0;
    }

    fn merge_existing_metadata(
        &mut self,
        workspaces: &[HerdrWorkspaceSnapshot],
        cx: &mut Context<Self>,
    ) {
        let Some(store) = ThreadMetadataStore::try_global(cx) else {
            return;
        };
        for workspace in workspaces {
            let Some(record) = self.root_mapping(&workspace.workspace_id).cloned() else {
                continue;
            };
            let existing = store.read(cx).entry(record.zed_root_thread_id).cloned();
            let Some(existing) = existing else {
                continue;
            };
            if existing.agent_id.as_ref() != HERDR_AGENT_ID.as_ref()
                || existing.session_id.is_some()
            {
                continue;
            }
            if let Some(metadata) = self.root_metadata.get_mut(&workspace.workspace_id) {
                metadata.created_at = existing.created_at;
                metadata.title_override = existing.title_override;
                if metadata.title_override.is_some() {
                    metadata.title = existing.title;
                }
                metadata.archived = false;
                metadata.updated_at = Utc::now();
                self.metadata_dirty.insert(workspace.workspace_id.clone());
            }
        }
    }
    fn apply_replay_events(&mut self, events: impl IntoIterator<Item = HerdrEvent>) {
        for event in events {
            let fenced = self.focus_is_fenced(&event);
            let stale = self.focus_event_is_stale(&event);
            if stale && !fenced {
                continue;
            }
            let state_event = self.event_for_state(&event);
            let applied = apply_event(&mut self.state, &state_event);
            self.note_focus_event(&event, &applied, fenced, stale);
            self.apply_actions(applied);
            self.emit_subthread_event(&event);
        }
    }

    fn apply_bootstrap(&mut self, bootstrap: HerdrBootstrap, cx: &mut Context<Self>) {
        let start = self.events.len();
        self.set_status(HerdrConnectionStatus::Synchronizing);
        self.apply_snapshot(bootstrap.snapshot.clone());
        self.merge_existing_metadata(&bootstrap.snapshot.workspaces, cx);
        self.apply_replay_events(bootstrap.events);
        if self.current_focus_workspace.is_none() {
            if let Some(workspace_id) = self.pending_authoritative_focus.take() {
                let key = self.state.workspace_key(&workspace_id);
                self.activate_mapping(&key);
            }
        } else {
            self.pending_authoritative_focus = None;
        }
        // Persist roots before Ready is observable. This also writes rows
        // created by a snapshot even when no mapping changed.
        self.persist_all_root_metadata(cx);
        self.active_subscription_id = Some(bootstrap.subscription_id);
        self.active_subscription_ids = bootstrap.subscription_ids.into_iter().collect();
        self.set_status(HerdrConnectionStatus::Ready);
        self.emit_new_events(start, cx);
        self.persist_mappings(cx);
    }
    fn start_sync(&mut self, cx: &mut Context<Self>) {
        self.active.store(true, Ordering::SeqCst);
        let generation = self.sync_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let status_start = self.events.len();
        self.set_status(HerdrConnectionStatus::Reconnecting);
        self.emit_new_events(status_start, cx);
        let Some(client) = self.client.clone() else {
            let status_start = self.events.len();
            self.set_status(HerdrConnectionStatus::Unavailable);
            self.emit_new_events(status_start, cx);
            return;
        };
        let events = self.event_receiver.clone();
        let active = self.active.clone();
        let sync_generation = self.sync_generation.clone();
        let sync_cancel_rx = self.sync_cancel_rx.clone();
        cx.spawn(async move |this, cx| {
            let mut backoff = Duration::from_millis(100);
            loop {
                if !active.load(Ordering::SeqCst)
                    || sync_generation.load(Ordering::SeqCst) != generation
                {
                    return;
                }

                let bootstrap_task = match this.update(cx, |_bridge, cx| client.bootstrap(cx)) {
                    Ok(task) => task,
                    Err(_) => return,
                };
                let cancellation = sync_cancel_rx.clone();
                let bootstrap = match futures::future::select(
                    Box::pin(bootstrap_task),
                    Box::pin(cancellation.recv()),
                )
                .await
                {
                    futures::future::Either::Left((result, _)) => result,
                    futures::future::Either::Right((_result, _)) => return,
                };
                match bootstrap {
                    Ok(bootstrap) => {
                        let replay_until = bootstrap.replay_until;
                        let reload_task = this
                            .update(cx, |_bridge, cx| {
                                ThreadMetadataStore::try_global(cx)
                                    .map(|store| store.read(cx).reload_task())
                            })
                            .ok()
                            .flatten();
                        if let Some(reload_task) = reload_task {
                            let cancellation = sync_cancel_rx.clone();
                            match futures::future::select(
                                Box::pin(reload_task),
                                Box::pin(cancellation.recv()),
                            )
                            .await
                            {
                                futures::future::Either::Left((_result, _)) => {}
                                futures::future::Either::Right((_result, _)) => return,
                            }
                        }
                        let applied = this
                            .update(cx, |bridge, cx| {
                                if !active.load(Ordering::SeqCst)
                                    || sync_generation.load(Ordering::SeqCst) != generation
                                {
                                    return false;
                                }
                                bridge.apply_bootstrap(bootstrap, cx);
                                true
                            })
                            .unwrap_or(false);
                        if !applied {
                            return;
                        }
                        backoff = Duration::from_millis(100);

                        let Some(events) = events.clone() else {
                            return;
                        };
                        let mut next_event_index = replay_until;
                        let mut pending_events = BTreeMap::new();
                        let mut reconnect = false;
                        'event_loop: while active.load(Ordering::SeqCst)
                            && sync_generation.load(Ordering::SeqCst) == generation
                        {
                            let cancellation = sync_cancel_rx.clone();
                            let received = futures::future::select(
                                Box::pin(events.recv()),
                                Box::pin(cancellation.recv()),
                            )
                            .await;
                            let cursor = match received {
                                futures::future::Either::Left((Ok(cursor), _)) => cursor,
                                futures::future::Either::Left((Err(_), _)) => {
                                    reconnect = true;
                                    let _ = this.update(cx, |bridge, cx| {
                                        let start = bridge.events.len();
                                        bridge.set_status(HerdrConnectionStatus::Unavailable);
                                        bridge.emit_new_events(start, cx);
                                    });
                                    break;
                                }
                                futures::future::Either::Right((_result, _)) => return,
                            };
                            if cursor.index < next_event_index {
                                continue;
                            }
                            pending_events.insert(cursor.index, cursor.event);
                            while let Some(event) = pending_events.remove(&next_event_index) {
                                next_event_index += 1;
                                let ended_subscription = this
                                    .update(cx, |bridge, _cx| {
                                        bridge.subscription_ended_requires_reconnect(&event)
                                    })
                                    .unwrap_or(true);
                                if ended_subscription {
                                    reconnect = true;
                                    let _ = this.update(cx, |bridge, cx| {
                                        let start = bridge.events.len();
                                        bridge.active_subscription_id = None;
                                        bridge.active_subscription_ids.clear();
                                        bridge.set_status(HerdrConnectionStatus::Unavailable);
                                        bridge.emit_new_events(start, cx);
                                    });
                                    break 'event_loop;
                                }
                                if this
                                    .update(cx, |bridge, cx| {
                                        if !active.load(Ordering::SeqCst)
                                            || sync_generation.load(Ordering::SeqCst) != generation
                                        {
                                            return false;
                                        }
                                        bridge.apply_event_in_context(event, cx);
                                        true
                                    })
                                    .unwrap_or(false)
                                    == false
                                {
                                    return;
                                }
                            }
                        }

                        if !reconnect
                            || !active.load(Ordering::SeqCst)
                            || sync_generation.load(Ordering::SeqCst) != generation
                        {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = this.update(cx, |bridge, cx| {
                            if !active.load(Ordering::SeqCst)
                                || sync_generation.load(Ordering::SeqCst) != generation
                            {
                                return;
                            }
                            let start = bridge.events.len();
                            bridge.active_subscription_id = None;
                            bridge.active_subscription_ids.clear();
                            bridge.set_status(HerdrConnectionStatus::Unavailable);
                            bridge.events.push(HerdrBridgeEvent::RequestFailed {
                                workspace_id: None,
                                operation: "bootstrap".to_string(),
                                message: error.to_string(),
                            });
                            bridge.emit_new_events(start, cx);
                        });
                    }
                }

                if !active.load(Ordering::SeqCst)
                    || sync_generation.load(Ordering::SeqCst) != generation
                {
                    return;
                }
                let cancellation = sync_cancel_rx.clone();
                match futures::future::select(
                    Box::pin(cx.background_executor().timer(backoff)),
                    Box::pin(cancellation.recv()),
                )
                .await
                {
                    futures::future::Either::Left((_result, _)) => {}
                    futures::future::Either::Right((_result, _)) => return,
                }
                let _ = this.update(cx, |bridge, cx| {
                    if !active.load(Ordering::SeqCst)
                        || sync_generation.load(Ordering::SeqCst) != generation
                    {
                        return;
                    }
                    let start = bridge.events.len();
                    bridge.set_status(HerdrConnectionStatus::Reconnecting);
                    bridge.emit_new_events(start, cx);
                });
                backoff = std::cmp::min(Duration::from_secs(2), backoff.saturating_mul(2));
            }
        })
        .detach();
    }

    pub(crate) fn begin_sync(&mut self, cx: &mut Context<Self>) {
        if self.sync_started {
            return;
        }
        self.sync_started = true;
        self.start_sync(cx);
    }

    pub(crate) fn stop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        self.sync_generation.fetch_add(1, Ordering::SeqCst);
        let _ = self.sync_cancel_tx.try_send(());
        if let Some(client) = self.client.as_ref() {
            client.cancel_subscriptions();
        }
        self.sync_started = false;
        self.active_subscription_id = None;
        self.active_subscription_ids.clear();
        self.client = None;
        self.event_receiver = None;
        self.set_status(HerdrConnectionStatus::Unavailable);
    }

    /// Disconnect the current state before binding this bridge to a new
    /// session. The fresh snapshot is loaded by [`begin_sync`].
    pub(crate) fn rebind_session(&mut self, session: impl Into<String>) -> Result<()> {
        let session = session.into();
        if session.trim().is_empty() {
            return Err(anyhow!("Herdr session name cannot be empty"));
        }
        self.active.store(false, Ordering::SeqCst);
        self.sync_generation.fetch_add(1, Ordering::SeqCst);
        let _ = self.sync_cancel_tx.try_send(());
        if let Some(client) = self.client.as_ref() {
            client.cancel_subscriptions();
        }
        let (sync_cancel_tx, sync_cancel_rx) = async_channel::unbounded();
        self.sync_cancel_tx = sync_cancel_tx;
        self.sync_cancel_rx = sync_cancel_rx;
        self.sync_started = false;
        self.active_subscription_id = None;
        self.active_subscription_ids.clear();
        self.agent_snapshots.clear();
        self.pane_outputs.clear();
        self.client = None;
        self.event_receiver = None;
        self.state = BridgeState::new(session.clone());
        self.pending_authoritative_focus = None;
        self.current_focus_workspace = None;
        self.metadata_dirty.clear();
        self.root_metadata.clear();
        self.outbound_requests.clear();
        self.selection = HerdrSessionSelection::Named(session);
        self.set_status(HerdrConnectionStatus::Synchronizing);
        Ok(())
    }

    pub(crate) fn rebind_selection(
        &mut self,
        selection: HerdrSessionSelection,
        cx: &mut Context<Self>,
    ) -> Result<()> {
        let session_name = selection.session_name();
        self.rebind_session(session_name.clone())?;
        self.selection = selection.clone();
        self.state.mappings =
            match HerdrMappingStore::load_session(&KeyValueStore::global(cx), &session_name) {
                Ok(mappings) => mappings,
                Err(error) => {
                    log::warn!("Herdr bridge mapping load failed for {session_name:?}: {error}");
                    SessionMappings::default()
                }
            };
        let endpoint = selection.endpoint();
        self.client = match HerdrClientHandle::new(endpoint, cx) {
            Ok(client) => {
                self.event_receiver = Some(client.subscribe_with_cursor());
                Some(Arc::new(client))
            }
            Err(error) => {
                log::warn!("Herdr bridge client creation failed: {error}");
                self.event_receiver = None;
                None
            }
        };
        self.sync_started = true;
        self.start_sync(cx);
        Ok(())
    }
    /// Request Herdr to focus a mapped root and return the fenced operation.
    /// Reflected events with this operation ID are consumed by Task 2's state
    /// machine and never produce another outbound request.
    pub(crate) fn focus_root(&mut self, workspace_id: &str) -> Option<OutboundRequest> {
        if self.root_mapping(workspace_id)?.is_tombstone() {
            return None;
        }
        let request = initiate_workspace_focus(&mut self.state, workspace_id);
        self.outbound_requests.push(request.clone());
        Some(request)
    }

    pub(crate) fn focus_root_in_context(
        &mut self,
        workspace_id: &str,
        cx: &mut Context<Self>,
    ) -> Option<Task<Result<(), HerdrClientError>>> {
        let request = self.focus_root(workspace_id)?;
        let OutboundRequest::FocusWorkspace {
            workspace_id,
            operation_id,
            origin,
        } = request
        else {
            return None;
        };
        let client = self.client.clone()?;
        let origin = match origin {
            HerdrOperationOrigin::Zed => "zed",
            HerdrOperationOrigin::Herdr => "herdr",
        };
        let task = client.focus_workspace(&workspace_id, Some(&operation_id), Some(origin), cx);
        Some(task)
    }

    pub(crate) fn request_create_workspace(
        &self,
        label: &str,
        paths: Vec<String>,
        cx: &App,
    ) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>> {
        match self.client.as_ref() {
            Some(client) => client.create_workspace(label, paths, cx),
            None => Task::ready(Err(HerdrClientError::Disconnected)),
        }
    }

    pub(crate) fn request_rename_workspace(
        &self,
        workspace_id: &str,
        label: &str,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        match self.client.as_ref() {
            Some(client) => client.rename_workspace(workspace_id, label, cx),
            None => Task::ready(Err(HerdrClientError::Disconnected)),
        }
    }

    pub(crate) fn request_close_workspace(
        &self,
        workspace_id: &str,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        match self.client.as_ref() {
            Some(client) => client.close_workspace(workspace_id, cx),
            None => Task::ready(Err(HerdrClientError::Disconnected)),
        }
    }
    pub(crate) fn focus_pane(
        &mut self,
        workspace_id: &str,
        pane_id: &str,
    ) -> Option<OutboundRequest> {
        let record = self.subthread_record_for_pane(Some(workspace_id), pane_id)?;
        if record.is_tombstone() {
            return None;
        }
        let request = initiate_pane_focus(&mut self.state, workspace_id, pane_id);
        self.outbound_requests.push(request.clone());
        Some(request)
    }

    pub(crate) fn focus_pane_in_context(
        &mut self,
        workspace_id: &str,
        pane_id: &str,
        cx: &mut Context<Self>,
    ) -> Option<Task<Result<(), HerdrClientError>>> {
        let request = self.focus_pane(workspace_id, pane_id)?;
        let OutboundRequest::FocusPane {
            pane_id,
            operation_id,
            origin,
            ..
        } = request
        else {
            return None;
        };
        let client = self.client.clone()?;
        let origin = match origin {
            HerdrOperationOrigin::Zed => "zed",
            HerdrOperationOrigin::Herdr => "herdr",
        };
        Some(client.focus_pane(&pane_id, Some(&operation_id), Some(origin), cx))
    }

    pub(crate) fn prompt_agent(
        &self,
        pane_id: &str,
        prompt: &str,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.client
            .as_ref()
            .map(|client| client.prompt_agent(pane_id, prompt, cx))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn cancel_agent(
        &self,
        pane_id: &str,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.client
            .as_ref()
            .map(|client| client.send_agent_keys(pane_id, "CTRL_C", cx))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn rename_agent(
        &self,
        pane_id: &str,
        name: Option<&str>,
        cx: &App,
    ) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        self.client
            .as_ref()
            .map(|client| client.rename_agent(pane_id, name, cx))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn close_pane(
        &self,
        pane_id: &str,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.client
            .as_ref()
            .map(|client| client.close_pane(pane_id, cx))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }

    pub(crate) fn read_pane_output(
        &self,
        pane_id: &str,
        since_revision: Option<u64>,
        cx: &App,
    ) -> Task<Result<(u64, String), HerdrClientError>> {
        self.client
            .as_ref()
            .map(|client| client.read_pane_output(pane_id, since_revision, cx))
            .unwrap_or_else(|| Task::ready(Err(HerdrClientError::Disconnected)))
    }
}

/// Per-window registry. A window gets exactly one bridge entity, and repeated
/// panel construction in that window reuses that entity.
pub(crate) struct HerdrBridgeRegistry {
    bridges: HashMap<WindowId, Entity<HerdrThreadBridge>>,
    panel_counts: HashMap<WindowId, usize>,
}

impl Default for HerdrBridgeRegistry {
    fn default() -> Self {
        Self {
            bridges: HashMap::default(),
            panel_counts: HashMap::default(),
        }
    }
}

impl Global for HerdrBridgeRegistry {}

impl HerdrBridgeRegistry {
    pub(crate) fn init(cx: &mut App) {
        if !cx.has_global::<Self>() {
            cx.set_global(Self::default());
        }
    }

    pub(crate) fn for_window(
        &mut self,
        window_id: WindowId,
        session: HerdrSessionSelection,
        cx: &mut App,
    ) -> Entity<HerdrThreadBridge> {
        if let Some(bridge) = self.bridges.get(&window_id).cloned() {
            *self.panel_counts.entry(window_id).or_default() += 1;
            return bridge;
        }

        let endpoint = session.endpoint();
        let (client, event_receiver) = match HerdrClientHandle::new(endpoint, cx) {
            Ok(client) => {
                let event_receiver = client.subscribe_with_cursor();
                (
                    Some(Arc::new(client) as Arc<dyn HerdrApi>),
                    Some(event_receiver),
                )
            }
            Err(error) => {
                log::warn!("Herdr bridge client creation failed: {error}");
                (None, None)
            }
        };
        let mappings =
            match HerdrMappingStore::load_session(&KeyValueStore::global(cx), &session.session_name())
            {
                Ok(mappings) => mappings,
                Err(error) => {
                    log::warn!("Herdr bridge mapping load failed: {error}");
                    SessionMappings::default()
                }
            };
        let bridge = cx.new(|_| {
            HerdrThreadBridge::new(
                Some(window_id),
                session,
                client,
                event_receiver,
                mappings,
            )
        });
        self.bridges.insert(window_id, bridge.clone());
        self.panel_counts.insert(window_id, 1);
        bridge
    }

    pub(crate) fn bridge_for_window(
        &self,
        window_id: WindowId,
        _cx: &App,
    ) -> Option<Entity<HerdrThreadBridge>> {
        self.bridges.get(&window_id).cloned()
    }

    pub(crate) fn release_panel(&mut self, window_id: WindowId, cx: &mut App) {
        let Some(count) = self.panel_counts.get_mut(&window_id) else {
            return;
        };
        if *count > 1 {
            *count -= 1;
            return;
        }
        self.panel_counts.remove(&window_id);
        if let Some(bridge) = self.bridges.remove(&window_id) {
            bridge.update(cx, |bridge, _cx| bridge.stop());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::herdr_client::{HerdrEvent, HerdrWorkspaceSnapshot};
    use gpui::TestAppContext;
    fn workspace_created(workspace_id: &str, path: &str, label: &str) -> HerdrEvent {
        HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: workspace_id.to_string(),
                paths: vec![path.to_string()],
                label: label.to_string(),
                ..Default::default()
            },
            sequence: 1,
        }
    }

    fn test_bridge() -> HerdrThreadBridge {
        HerdrThreadBridge::for_test("alpha")
    }

    #[test]
    fn workspace_created_creates_a_herdr_root_mapping() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        assert!(bridge.root_mapping("w1").is_some());
    }

    #[test]
    fn workspace_renamed_updates_root_metadata_title() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceRenamed {
            workspace_id: "w1".to_string(),
            label: "Renamed".to_string(),
            sequence: 2,
        });
        assert_eq!(bridge.root_title("w1").as_deref(), Some("Renamed"));
    }

    #[test]
    fn workspace_focus_publishes_the_mapped_root_thread() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 2,
        });
        assert!(bridge
            .take_events()
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1")));
    }

    #[test]
    fn workspace_close_archives_root_and_keeps_tombstone() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceClosed {
            workspace_id: "w1".to_string(),
            sequence: 2,
        });
        assert!(bridge.root_mapping("w1").is_some_and(|record| record.is_tombstone()));
        assert!(bridge.root_metadata("w1").is_some_and(|metadata| metadata.archived));
    }

    #[test]
    fn session_rebind_disconnects_old_session_before_loading_new_snapshot() {
        let mut bridge = HerdrThreadBridge::for_test_in_session("alpha");
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.rebind_session("beta").expect("session rebind should succeed");
        assert_eq!(bridge.session_name(), "beta");
        assert_eq!(bridge.status(), HerdrConnectionStatus::Synchronizing);
        assert!(bridge.root_mapping("w1").is_none());
    }

    #[test]
    fn reflected_focus_operation_is_fenced_without_a_second_outbound_request() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        let operation = bridge.focus_root("w1").expect("root focus should be mapped");
        let operation_id = match operation {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: Some(operation_id),
            sequence: 2,
        });
        assert!(bridge.take_outbound_requests().len() == 1);
        assert!(bridge.take_events().iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::RootFocused { .. }
        )));
    }
    #[test]
    fn initial_snapshot_creates_non_draft_root_metadata() {
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            sequence: 7,
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        });
        let metadata = bridge.root_metadata("w1").expect("snapshot root metadata");
        assert_eq!(metadata.agent_id, HERDR_AGENT_ID.clone());
        assert!(!metadata.is_draft());
        assert_eq!(metadata.folder_paths().paths(), &[PathBuf::from("/repo")]);
    }

    #[test]
    fn stale_fenced_focus_does_not_replace_newer_current_focus() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w2".to_string(),
                paths: vec!["/repo-2".to_string()],
                label: "Other".to_string(),
                ..Default::default()
            },
            sequence: 2,
        });
        let first = bridge.focus_root("w1").expect("first root focus");
        let second = bridge.focus_root("w2").expect("second root focus");
        let first_operation_id = match first {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };
        let second_operation_id = match second {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };

        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: Some(second_operation_id),
            sequence: 4,
        });
        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w2"),
            "newer fenced focus should become current"
        );

        bridge.apply_replay_events([HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: Some(first_operation_id),
            sequence: 3,
        }]);
        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w2"),
            "a delayed older fenced focus must not replace newer focus"
        );
    }
    #[test]
    fn stale_sequence_less_focus_does_not_replace_newer_focus() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w2".to_string(),
                paths: vec!["/repo-2".to_string()],
                label: "Other".to_string(),
                ..Default::default()
            },
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w3".to_string(),
                paths: vec!["/repo-3".to_string()],
                label: "External".to_string(),
                ..Default::default()
            },
            sequence: 3,
        });
        let first_operation_id = match bridge.focus_root("w1").expect("first root focus") {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: None,
            sequence: 0,
        });
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w3".to_string(),
            operation_id: None,
            sequence: 0,
        });
        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w3"),
            "external sequence-less focus should become current"
        );
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: Some(first_operation_id.clone()),
            sequence: 0,
        });
        assert!(
            !bridge.state.issued_focus.contains_key(&first_operation_id),
            "a delayed local reflection must consume its issued operation"
        );
        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w3"),
            "a delayed local reflection must not activate its superseded target"
        );
        assert!(bridge.take_events().iter().all(|event| {
            !matches!(
                event,
                HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1"
            )
        }));
    }

    #[test]
    fn external_sequence_less_focus_on_superseded_target_applies() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w2".to_string(),
                paths: vec!["/repo-2".to_string()],
                label: "Other".to_string(),
                ..Default::default()
            },
            sequence: 2,
        });
        bridge.focus_root("w1").expect("first root focus");
        bridge.focus_root("w2").expect("second root focus");

        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: None,
            sequence: 0,
        });
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 0,
        });

        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w1"),
            "external sequence-less focus must not be suppressed by target history"
        );
        assert!(bridge.take_events().iter().any(|event| {
            matches!(
                event,
                HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1"
            )
        }));
    }

    #[test]
    fn bulk_filter_termination_is_reconnect_trigger() {
        let mut bridge = test_bridge();
        bridge
            .active_subscription_ids
            .insert("bulk-filter".to_string());
        let event = HerdrEvent::Unknown {
            event: "subscription_ended".to_string(),
            data: serde_json::from_str(
                r#"{"subscription_id":"bulk-filter","error":"closed"}"#,
            )
            .expect("subscription-ended data"),
        };
        assert!(bridge.subscription_ended_requires_reconnect(&event));

        let dynamic_filter = HerdrEvent::Unknown {
            event: "subscription_ended".to_string(),
            data: serde_json::from_str(
                r#"{"subscription_id":"pane-filter","error":"retired"}"#,
            )
            .expect("subscription-ended data"),
        };
        assert!(
            !bridge.subscription_ended_requires_reconnect(&dynamic_filter),
            "expected per-pane filter retirement must not fault the primary session"
        );
    }

    #[test]
    fn stale_reflected_focus_does_not_activate_a_superseded_root() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w2".to_string(),
                paths: vec!["/repo-2".to_string()],
                label: "Other".to_string(),
                ..Default::default()
            },
            sequence: 2,
        });
        let first = bridge.focus_root("w1").expect("first root focus");
        let second = bridge.focus_root("w2").expect("second root focus");
        let first_operation_id = match first {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };
        let second_operation_id = match second {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("root focus must produce a workspace request"),
        };
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: Some(first_operation_id),
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: Some(second_operation_id),
            sequence: 3,
        });
        assert!(bridge
            .take_events()
            .iter()
            .all(|event| !matches!(event, HerdrBridgeEvent::RootFocused { .. })));
    }
    #[test]
    fn replay_actions_publish_each_effect_once() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();
        bridge.apply_replay_events([HerdrEvent::WorkspaceRenamed {
            workspace_id: "w1".to_string(),
            label: "Renamed".to_string(),
            sequence: 2,
        }]);
        let events = bridge.take_events();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, HerdrBridgeEvent::RootRenamed { .. }))
                .count(),
            1,
            "replay effects must be published once"
        );
    }

    #[test]
    fn fenced_replay_focus_overrides_snapshot_focus_without_echo() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: HerdrWorkspaceSnapshot {
                workspace_id: "w2".to_string(),
                paths: vec!["/repo-2".to_string()],
                label: "Other".to_string(),
                ..Default::default()
            },
            sequence: 2,
        });
        let operation_id = match bridge.focus_root("w2").expect("focus operation") {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            _ => panic!("expected workspace focus"),
        };
        bridge.take_events();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            workspaces: vec![
                HerdrWorkspaceSnapshot {
                    workspace_id: "w1".to_string(),
                    paths: vec!["/repo".to_string()],
                    label: "Review".to_string(),
                    ..Default::default()
                },
                HerdrWorkspaceSnapshot {
                    workspace_id: "w2".to_string(),
                    paths: vec!["/repo-2".to_string()],
                    label: "Other".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        bridge.apply_replay_events([HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: Some(operation_id),
            sequence: 3,
        }]);
        assert_eq!(
            bridge.current_focus_workspace.as_deref(),
            Some("w2"),
            "a fenced focus reflection is authoritative"
        );
        assert!(bridge
            .take_events()
            .iter()
            .all(|event| !matches!(event, HerdrBridgeEvent::RootFocused { .. })));
    }

    #[test]
    fn stop_wakes_the_current_sync_worker() {
        let mut bridge = test_bridge();
        let cancellation = bridge.sync_cancel_rx.clone();
        bridge.stop();
        assert!(cancellation.try_recv().is_ok());
    }

    #[test]
    fn rebind_wakes_old_worker_and_replaces_cancellation_generation() {
        let mut bridge = test_bridge();
        let old_cancellation = bridge.sync_cancel_rx.clone();
        bridge.rebind_session("beta").expect("rebind");
        assert!(old_cancellation.try_recv().is_ok());
        assert!(bridge.sync_cancel_rx.try_recv().is_err());
    }
    #[test]
    fn pane_agent_events_publish_selectable_output_status_and_close_updates() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        let created = bridge.take_events();
        assert!(created.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated { pane_id, .. } if pane_id == "p1"
        )));

        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 3,
        });
        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 4,
            delta: "new".to_string(),
            sequence: 0,
        });
        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 3,
            delta: "old".to_string(),
            sequence: 0,
        });
        let updates = bridge.take_events();
        assert!(updates.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadUpdated {
                pane_id,
                status: Some(HerdrAgentStatus::Working),
                ..
            } if pane_id == "p1"
        )));
        assert!(updates.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadOutput {
                pane_id,
                revision: 4,
                output,
                ..
            } if pane_id == "p1" && output == "new"
        )));
        assert!(!updates.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadOutput { revision: 3, .. }
        )));

        let focus = bridge.focus_pane("w1", "p1").expect("mapped pane focus");
        assert!(matches!(focus, OutboundRequest::FocusPane { .. }));
        bridge.apply_event(HerdrEvent::PaneFocused {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 4,
        });
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { key, .. }
                if key.pane_id.as_deref() == Some("p1")
        )));

        bridge.apply_event(HerdrEvent::PaneClosed {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            sequence: 5,
        });
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadClosed { pane_id, .. } if pane_id == "p1"
        )));
    }
}
