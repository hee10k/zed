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
        HerdrClientError, HerdrClientHandle, HerdrEvent, HerdrEventCursor,
        HerdrSnapshot, HerdrWorkspaceSnapshot,
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
        ActivityStatus, HERDR_AGENT_ID, ThreadId, ThreadMetadata, ThreadMetadataStore,
        WorktreePaths,
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

/// Runtime identity of the local Herdr process that owns a window bridge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HerdrOwnerProcess {
    pub terminal_id: gpui::EntityId,
    pub process_id: Option<u32>,
    pub session_name: String,
}

/// Connection state for a per-window Herdr bridge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HerdrConnectionStatus {
    Dormant,
    Unavailable,
    Reconnecting,
    Synchronizing,
    Ready,
}
impl HerdrConnectionStatus {
    pub(crate) fn allows_actions(self) -> bool {
        matches!(self, Self::Ready)
    }

    pub(crate) fn disabled_reason(self) -> Option<&'static str> {
        match self {
            Self::Ready => None,
            Self::Dormant => Some("Herdr is dormant; launch Herdr from this window to connect."),
            Self::Unavailable => Some("Herdr is unavailable; reconnect to continue."),
            Self::Reconnecting => Some("Herdr is reconnecting; actions are temporarily disabled."),
            Self::Synchronizing => Some("Herdr is synchronizing; actions are temporarily disabled."),
        }
    }
}


/// Events consumed by AgentPanel and other UI surfaces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HerdrBridgeEvent {
    StatusChanged(HerdrConnectionStatus),
    /// A user explicitly rebound this bridge; all panels sharing it must drop
    /// surfaces that belong to the previous session.
    SessionRebound,
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
    SubthreadStatusOnly {
        workspace_id: String,
        pane_id: String,
        status: crate::herdr_client::HerdrAgentStatus,
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
    owner: Option<HerdrOwnerProcess>,
    client: Option<Arc<dyn HerdrApi>>,
    event_receiver: Option<async_channel::Receiver<HerdrEventCursor>>,
    state: BridgeState,
    root_metadata: HashMap<String, ThreadMetadata>,
    agent_snapshots: HashMap<String, HerdrAgentSnapshot>,
    status_only_snapshots: HashMap<(String, String), HerdrAgentSnapshot>,
    pane_outputs: HashMap<String, (u64, String)>,
    /// Subthreads already published to UI consumers, keyed by
    /// `(workspace_id, pane_id)`. This decides whether a live pane event
    /// emits `SubthreadCreated` or `SubthreadUpdated` and is independent of
    /// internal mapping bookkeeping, which reconciliation may change without
    /// the panel ever having seen the subthread.
    published_subthreads: HashSet<(String, String)>,
    status: HerdrConnectionStatus,
    events: Vec<HerdrBridgeEvent>,
    outbound_requests: Vec<OutboundRequest>,
    pending_authoritative_focus: Option<PendingAuthoritativeFocus>,
    current_focus_workspace: Option<String>,
    metadata_dirty: HashSet<String>,
    active_subscription_id: Option<String>,
    active_subscription_ids: HashSet<String>,
    active: Arc<AtomicBool>,
    sync_generation: Arc<AtomicU64>,
    sync_cancelled: Arc<AtomicBool>,
    sync_cancel_tx: async_channel::Sender<()>,
    sync_cancel_rx: async_channel::Receiver<()>,
    sync_started: bool,
}
#[derive(Clone)]
struct PaneMoveSource {
    record: HerdrMappingRecord,
    snapshot: Option<HerdrAgentSnapshot>,
    published: bool,
}

#[derive(Clone)]
struct PendingAuthoritativeFocus {
    target: FocusTarget,
    pane_identity: Option<PendingAuthoritativePaneIdentity>,
}

#[derive(Clone)]
enum PendingAuthoritativePaneIdentity {
    Exact(HerdrAgentSessionIdentity),
    AwaitReplay,
    RootFallback,
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
        let sync_cancelled = Arc::new(AtomicBool::new(false));
        Self {
            window_id,
            selection,
            owner: None,
            client,
            event_receiver,
            state: BridgeState {
                session,
                mappings,
                ..BridgeState::default()
            },
            status: HerdrConnectionStatus::Dormant,
            events: Vec::new(),
            outbound_requests: Vec::new(),
            pending_authoritative_focus: None,
            current_focus_workspace: None,
            metadata_dirty: HashSet::default(),
            active_subscription_id: None,
            agent_snapshots: HashMap::default(),
            status_only_snapshots: HashMap::default(),
            published_subthreads: HashSet::default(),
            pane_outputs: HashMap::default(),
            active_subscription_ids: HashSet::default(),
            active: Arc::new(AtomicBool::new(false)),
            sync_generation: Arc::new(AtomicU64::new(0)),
            sync_cancelled,
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

    /// Constructor used by UI tests that must observe outbound API calls.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn for_test_with_api(client: Arc<dyn HerdrApi>) -> Self {
        Self::new(
            None,
            HerdrSessionSelection::Named("alpha".to_string()),
            Some(client),
            None,
            SessionMappings::default(),
        )
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

    pub(crate) fn set_owner(&mut self, owner: HerdrOwnerProcess) {
        self.owner = Some(owner);
    }

    pub(crate) fn clear_owner(&mut self) -> Option<HerdrOwnerProcess> {
        self.owner.take()
    }

    pub(crate) fn owner(&self) -> Option<&HerdrOwnerProcess> {
        self.owner.as_ref()
    }

    pub(crate) fn status(&self) -> HerdrConnectionStatus {
        self.status
    }

    pub(crate) fn root_mapping(&self, workspace_id: &str) -> Option<&HerdrMappingRecord> {
        let key = self.state.workspace_key(workspace_id).to_key_string();
        self.state.mappings.get(&key)
    }

    pub(crate) fn root_zed_workspace_id(
        &self,
        herdr_workspace_id: &str,
    ) -> Option<workspace::WorkspaceId> {
        self.root_mapping(herdr_workspace_id)
            .filter(|record| !record.is_tombstone())
            .and_then(|record| record.zed_workspace_id)
    }

    pub(crate) fn set_root_zed_workspace_id(
        &mut self,
        herdr_workspace_id: &str,
        zed_workspace_id: workspace::WorkspaceId,
        cx: &mut Context<Self>,
    ) -> bool {
        let key = self.state.workspace_key(herdr_workspace_id);
        let key_string = key.to_key_string();
        let Some(existing_record) = self.state.mappings.get(&key_string) else {
            return false;
        };
        if existing_record.is_tombstone() {
            return false;
        }

        match existing_record.zed_workspace_id {
            Some(existing) if existing == zed_workspace_id => false,
            Some(existing) => {
                let event = HerdrBridgeEvent::Conflict {
                    key,
                    message: format!(
                        "Herdr root {herdr_workspace_id:?} is already owned by Zed workspace \
                         {existing:?}; refusing to overwrite it with {zed_workspace_id:?}"
                    ),
                };
                cx.emit(event.clone());
                self.events.push(event);
                false
            }
            None => {
                let Some(record) = self.state.mappings.get_mut(&key_string) else {
                    return false;
                };
                record.zed_workspace_id = Some(zed_workspace_id);
                self.persist_mappings(cx);
                true
            }
        }
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
        let mut snapshots = self
            .agent_snapshots
            .values()
            .filter(|snapshot| snapshot.workspace_id == workspace_id)
            .cloned()
            .collect::<Vec<_>>();
        snapshots.extend(
            self.status_only_snapshots
                .values()
                .filter(|snapshot| snapshot.workspace_id == workspace_id)
                .cloned(),
        );
        snapshots
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

    pub(crate) fn root_thread_ids(&self) -> Vec<ThreadId> {
        self.state
            .mappings
            .values()
            .filter(|record| record.key.pane_id.is_none() && !record.is_tombstone())
            .map(|record| record.zed_root_thread_id)
            .collect()
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

    /// Reconciles one transport event through Task 2's pure state machine.
    /// `PaneUpdated` carries the same identity payload as `PaneAgentDetected`,
    /// so both live identity events resolve through the approved
    /// restore-or-create outcome instead of bypassing reconciliation; a pane
    /// whose agent restarted with an unknown identity therefore surfaces a
    /// conflict rather than gaining a second live mapping.
    fn reconcile_state_event(&mut self, state_event: &HerdrEvent) -> AppliedEvent {
        match state_event {
            HerdrEvent::PaneUpdated { pane, sequence } if pane.session_identity.is_some() => {
                let detected = HerdrEvent::PaneAgentDetected {
                    pane_id: pane.pane_id.clone(),
                    workspace_id: pane.workspace_id.clone(),
                    agent_type: pane.agent_type.clone(),
                    session_identity: pane.session_identity.clone(),
                    sequence: *sequence,
                };
                apply_event(&mut self.state, &detected)
            }
            _ => apply_event(&mut self.state, state_event),
        }
    }

    fn accepted_subthread_reconciliation(
        event: &HerdrEvent,
        applied: &AppliedEvent,
        sequence_before: u64,
        sequence_after: u64,
    ) -> bool {
        if applied
            .actions
            .iter()
            .any(|action| matches!(action, ReconciliationAction::RecordConflict(_, _)))
        {
            return false;
        }
        if applied.actions.iter().any(|action| {
            matches!(
                action,
                ReconciliationAction::CreateAgentSubthread(_)
                    | ReconciliationAction::RestoreAgentSubthread(_)
                    | ReconciliationAction::CreateWorkspaceRoot(_)
                    | ReconciliationAction::RestoreWorkspaceRoot(_)
                    | ReconciliationAction::UpdateTitle(_, _)
                    | ReconciliationAction::UpdateStatus(_, _)
                    | ReconciliationAction::Activate(_)
                    | ReconciliationAction::Archive(_)
            )
        }) {
            return true;
        }

        // Task 2 intentionally leaves safe, non-destructive events as
        // no-ops. A sequence-less status/output/scroll notification may still
        // be published when it targets a live or status-only pane, while a
        // sequenced no-op is accepted only when it advanced the bridge fence.
        let sequence_advanced = event.sequence() > 0 && sequence_after > sequence_before;
        match event {
            HerdrEvent::PaneAgentStatusChanged { .. }
            | HerdrEvent::PaneOutput { .. }
            | HerdrEvent::PaneScrollChanged { .. } => {
                event.sequence() == 0 || sequence_advanced
            }
            HerdrEvent::PaneAgentDetected {
                session_identity: None,
                ..
            }
            | HerdrEvent::WorkspaceClosed { .. }
            | HerdrEvent::PaneClosed { .. }
            | HerdrEvent::PaneExited { .. } => sequence_advanced,
            _ => false,
        }
    }

    fn pane_move_source(&self, event: &HerdrEvent) -> Option<PaneMoveSource> {
        let HerdrEvent::PaneMoved {
            pane,
            previous_pane_id,
            previous_workspace_id,
            ..
        } = event
        else {
            return None;
        };
        let old_workspace_id = previous_workspace_id.as_deref().unwrap_or(&pane.workspace_id);
        let old_pane_id = previous_pane_id.as_deref().unwrap_or(&pane.pane_id);
        let record = pane
            .session_identity
            .as_ref()
            .and_then(|identity| {
                self.state
                    .mappings
                    .values()
                    .find(|record| {
                        !record.is_tombstone()
                            && record.key.session == self.state.session
                            && record.key.agent_session.as_ref() == Some(identity)
                    })
                    .cloned()
            })
            .or_else(|| {
                self.live_subthread_record_for_pane(Some(old_workspace_id), old_pane_id)
            })?;
        let source_workspace_id = record.key.workspace_id.clone();
        let source_pane_id = record.key.pane_id.clone().unwrap_or_default();
        Some(PaneMoveSource {
            published: self
                .published_subthreads
                .contains(&(source_workspace_id, source_pane_id)),
            snapshot: self.agent_snapshots.get(&record.key.to_key_string()).cloned(),
            record,
        })
    }

    /// Apply one pushed Herdr event without requiring a GPUI context. This is
    /// intentionally also used by deterministic bridge tests.
    pub(crate) fn apply_event(&mut self, event: HerdrEvent) {
        let sequence_before = self.state.last_sequence;
        let move_source = self.pane_move_source(&event);
        let fenced = self.focus_is_fenced(&event);
        let stale = self.focus_event_is_stale(&event);
        if stale && !fenced {
            return;
        }
        let retained_status = self.retained_status_for_pane_updated(&event);
        let state_event = self.event_for_state(&event);
        let applied = self.reconcile_state_event(&state_event);
        let accepted = Self::accepted_subthread_reconciliation(
            &event,
            &applied,
            sequence_before,
            self.state.last_sequence,
        );
        self.note_focus_event(&event, &applied, fenced, stale);
        self.apply_actions(applied);
        self.update_pending_authoritative_focus_from_replay(&event, accepted);
        if accepted {
            self.clear_status_only_for_workspace_close(&event);
            if matches!(event, HerdrEvent::PaneMoved { .. }) {
                self.emit_pane_moved_event(&event, move_source);
            } else {
                self.emit_subthread_event(&event, retained_status);
            }
        }
    }
    fn apply_event_in_context(&mut self, event: HerdrEvent, cx: &mut Context<Self>) {
        let start = self.events.len();
        let sequence_before = self.state.last_sequence;
        let move_source = self.pane_move_source(&event);
        let fenced = self.focus_is_fenced(&event);
        let stale = self.focus_event_is_stale(&event);
        if stale && !fenced {
            return;
        }
        let retained_status = self.retained_status_for_pane_updated(&event);
        let state_event = self.event_for_state(&event);
        let applied = self.reconcile_state_event(&state_event);
        let accepted = Self::accepted_subthread_reconciliation(
            &event,
            &applied,
            sequence_before,
            self.state.last_sequence,
        );
        self.note_focus_event(&event, &applied, fenced, stale);
        self.apply_actions_in_context(applied, cx);
        self.update_pending_authoritative_focus_from_replay(&event, accepted);
        if accepted {
            self.clear_status_only_for_workspace_close(&event);
            if matches!(event, HerdrEvent::PaneMoved { .. }) {
                self.emit_pane_moved_event(&event, move_source);
            } else {
                self.emit_subthread_event(&event, retained_status);
            }
        }
        self.emit_new_events(start, cx);
        self.persist_mappings(cx);
    }
    fn clear_status_only_for_workspace_close(&mut self, event: &HerdrEvent) {
        if let HerdrEvent::WorkspaceClosed { workspace_id, .. } = event {
            self.status_only_snapshots
                .retain(|(existing_workspace_id, _), _| existing_workspace_id != workspace_id);
        }
    }
    fn retained_status_for_pane_updated(&self, event: &HerdrEvent) -> Option<HerdrAgentStatus> {
        let HerdrEvent::PaneUpdated { pane, .. } = event else {
            return None;
        };
        pane.session_identity.as_ref()?;
        self.status_only_snapshots
            .get(&(pane.workspace_id.clone(), pane.pane_id.clone()))
            .map(|snapshot| snapshot.status.clone())
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
        let mapping_before = self.state.mappings.clone();
        self.apply_action(action);
        self.persist_metadata_changes(&mapping_before, cx);
        self.persist_dirty_metadata(cx);
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

    fn create_agent_mapping(&mut self, mut agent: HerdrAgentSnapshot) {
        let Some(identity) = agent.session_identity.clone() else {
            return;
        };
        // A status-only record for this pane is the visible status truth
        // until identity arrives; carry it into the agent snapshot so the
        // upgrade does not regress the status to the default.
        let retained_status = self
            .status_only_snapshots
            .remove(&(agent.workspace_id.clone(), agent.pane_id.clone()))
            .map(|existing| existing.status);
        if let Some(status) = retained_status {
            agent.status = status;
        }
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
            zed_workspace_id: None,
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

    /// Live (non-tombstone) mapping for a pane. Emission must never bind a
    /// bridge event to a retained tombstone key.
    fn live_subthread_record_for_pane(
        &self,
        workspace_id: Option<&str>,
        pane_id: &str,
    ) -> Option<HerdrMappingRecord> {
        self.state
            .mappings
            .values()
            .find(|record| {
                !record.is_tombstone()
                    && record.key.pane_id.as_deref() == Some(pane_id)
                    && workspace_id.is_none_or(|workspace| record.key.workspace_id == workspace)
            })
            .cloned()
    }
    fn emit_pane_moved_event(
        &mut self,
        event: &HerdrEvent,
        source: Option<PaneMoveSource>,
    ) {
        let HerdrEvent::PaneMoved { pane, .. } = event else {
            return;
        };
        let Some(record) =
            self.live_subthread_record_for_pane(Some(&pane.workspace_id), &pane.pane_id)
        else {
            return;
        };
        let Some(session) = record.key.agent_session.clone() else {
            return;
        };
        let inherited = source
            .as_ref()
            .and_then(|source| source.snapshot.clone())
            .or_else(|| self.agent_snapshots.get(&record.key.to_key_string()).cloned());
        let status = match (&pane.status, inherited.as_ref().map(|snapshot| &snapshot.status)) {
            (HerdrAgentStatus::Unknown(value), Some(status)) if value == "unknown" => {
                (*status).clone()
            }
            _ => pane.status.clone(),
        };
        let title = pane
            .title
            .clone()
            .or_else(|| inherited.as_ref().and_then(|snapshot| snapshot.title.clone()));
        let agent_type = pane
            .agent_type
            .clone()
            .or_else(|| inherited.as_ref().and_then(|snapshot| snapshot.agent_type.clone()));
        let cwd = pane
            .cwd
            .clone()
            .or_else(|| inherited.as_ref().and_then(|snapshot| snapshot.cwd.clone()));

        let source_changed = source
            .as_ref()
            .is_some_and(|source| source.record.key != record.key);
        if let Some(source) = source.as_ref().filter(|_| source_changed) {
            self.agent_snapshots.remove(&source.record.key.to_key_string());
            if let Some(old_pane_id) = source.record.key.pane_id.as_deref() {
                if old_pane_id != pane.pane_id
                    && let Some(output) = self.pane_outputs.remove(old_pane_id)
                {
                    self.pane_outputs.insert(pane.pane_id.clone(), output);
                }
            }
            self.published_subthreads.remove(&(
                source.record.key.workspace_id.clone(),
                source.record.key.pane_id.clone().unwrap_or_default(),
            ));
            if source.published {
                self.events.push(HerdrBridgeEvent::SubthreadClosed {
                    key: source.record.key.clone(),
                    thread_id: source.record.zed_root_thread_id,
                    pane_id: source.record.key.pane_id.clone().unwrap_or_default(),
                });
            }
        }

        let snapshot = HerdrAgentSnapshot {
            pane_id: pane.pane_id.clone(),
            workspace_id: pane.workspace_id.clone(),
            agent_type,
            session_identity: Some(session.clone()),
            status: status.clone(),
            title: title.clone(),
            cwd,
            last_seen_sequence: event.sequence(),
        };
        self.agent_snapshots
            .insert(record.key.to_key_string(), snapshot);
        let location = (pane.workspace_id.clone(), pane.pane_id.clone());
        let already_published = self.published_subthreads.contains(&location);
        self.published_subthreads.insert(location);
        if source_changed || !already_published {
            self.events.push(HerdrBridgeEvent::SubthreadCreated {
                key: record.key.clone(),
                thread_id: record.zed_root_thread_id,
                pane_id: pane.pane_id.clone(),
                session,
                title: title
                    .or_else(|| pane.agent_type.clone())
                    .unwrap_or_else(|| pane.pane_id.clone()),
                status,
            });
        } else {
            self.events.push(HerdrBridgeEvent::SubthreadUpdated {
                key: record.key.clone(),
                thread_id: record.zed_root_thread_id,
                pane_id: pane.pane_id.clone(),
                title,
                status: Some(status),
            });
        }
    }


    fn emit_subthread_event(
        &mut self,
        event: &HerdrEvent,
        retained_status: Option<HerdrAgentStatus>,
    ) {
        match event {
            HerdrEvent::PaneAgentDetected {
                pane_id,
                workspace_id,
                agent_type,
                session_identity,
                ..
            } => {
                let Some(session) = session_identity.clone() else {
                    if self
                        .root_mapping(workspace_id)
                        .is_some_and(|record| !record.is_tombstone())
                    {
                        // The pane lost a previously published identity: drop
                        // its stale agent snapshot/publication so only the
                        // status-only record remains. Preserve the last known
                        // status while the identity is unavailable.
                        let retained_snapshot = self
                            .agent_snapshots
                            .values()
                            .find(|existing| {
                                existing.workspace_id == *workspace_id
                                    && existing.pane_id == *pane_id
                            })
                            .cloned();
                        self.agent_snapshots.retain(|_, existing| {
                            existing.workspace_id != *workspace_id
                                || existing.pane_id != *pane_id
                        });
                        self.published_subthreads
                            .remove(&(workspace_id.clone(), pane_id.clone()));
                        let snapshot = self
                            .status_only_snapshots
                            .entry((workspace_id.clone(), pane_id.clone()))
                            .or_insert_with(|| HerdrAgentSnapshot {
                                pane_id: pane_id.clone(),
                                workspace_id: workspace_id.clone(),
                                status: retained_snapshot
                                    .map(|snapshot| snapshot.status)
                                    .unwrap_or_default(),
                                ..Default::default()
                            });
                        // Repeated identity-less detections carry the latest
                        // agent metadata instead of leaving the first value.
                        snapshot.agent_type = agent_type.clone();
                        let status = snapshot.status.clone();
                        self.events.push(HerdrBridgeEvent::SubthreadStatusOnly {
                            workspace_id: workspace_id.clone(),
                            pane_id: pane_id.clone(),
                            status,
                        });
                    }
                    return;
                };
                let Some(record) =
                    self.live_subthread_record_for_pane(Some(workspace_id), pane_id)
                else {
                    return;
                };
                // Translation only: reconciliation has already decided whether
                // this identity creates, restores, or conflicts, so a
                // conflicting or unpersisted key is never emitted here.
                if record.key.agent_session.as_ref() != Some(&session) {
                    return;
                }
                let retained_status = self
                    .status_only_snapshots
                    .get(&(workspace_id.clone(), pane_id.clone()))
                    .map(|snapshot| snapshot.status.clone())
                    // Reconciliation's create path already carried a retained
                    // status-only status into the mapping snapshot.
                    .or_else(|| {
                        self.agent_snapshots
                            .get(&record.key.to_key_string())
                            .map(|snapshot| snapshot.status.clone())
                    });
                self.status_only_snapshots
                    .remove(&(workspace_id.clone(), pane_id.clone()));
                self.agent_snapshots.insert(
                    record.key.to_key_string(),
                    HerdrAgentSnapshot {
                        pane_id: pane_id.clone(),
                        workspace_id: workspace_id.clone(),
                        agent_type: agent_type.clone(),
                        session_identity: Some(session.clone()),
                        status: retained_status
                            .clone()
                            .unwrap_or_else(HerdrAgentStatus::default),
                        title: agent_type.clone(),
                        ..Default::default()
                    },
                );
                self.published_subthreads
                    .insert((workspace_id.clone(), pane_id.clone()));
                self.events.push(HerdrBridgeEvent::SubthreadCreated {
                    key: record.key,
                    thread_id: record.zed_root_thread_id,
                    pane_id: pane_id.clone(),
                    session,
                    title: agent_type.clone().unwrap_or_else(|| pane_id.clone()),
                    status: retained_status.unwrap_or_else(HerdrAgentStatus::default),
                });
            }
            HerdrEvent::PaneUpdated { pane, .. } => {
                let Some(session) = pane.session_identity.clone() else {
                    return;
                };
                let Some(record) = self
                    .live_subthread_record_for_pane(Some(&pane.workspace_id), &pane.pane_id)
                else {
                    return;
                };
                if record.key.agent_session.as_ref() != Some(&session) {
                    return;
                }
                let status = retained_status.unwrap_or_else(|| pane.status.clone());
                self.status_only_snapshots
                    .remove(&(pane.workspace_id.clone(), pane.pane_id.clone()));
                self.agent_snapshots.insert(
                    record.key.to_key_string(),
                    HerdrAgentSnapshot {
                        pane_id: pane.pane_id.clone(),
                        workspace_id: pane.workspace_id.clone(),
                        agent_type: pane.agent_type.clone(),
                        session_identity: Some(session),
                        status: status.clone(),
                        title: pane.title.clone(),
                        cwd: pane.cwd.clone(),
                        ..Default::default()
                    },
                );
                let first_publication = self
                    .published_subthreads
                    .insert((pane.workspace_id.clone(), pane.pane_id.clone()));
                if first_publication {
                    self.events.push(HerdrBridgeEvent::SubthreadCreated {
                        key: record.key,
                        thread_id: record.zed_root_thread_id,
                        pane_id: pane.pane_id.clone(),
                        session: pane.session_identity.clone().unwrap_or_else(|| {
                            HerdrAgentSessionIdentity::id(pane.pane_id.clone())
                        }),
                        title: pane.title.clone().unwrap_or_else(|| pane.pane_id.clone()),
                        status: status.clone(),
                    });
                } else {
                    self.events.push(HerdrBridgeEvent::SubthreadUpdated {
                        key: record.key,
                        thread_id: record.zed_root_thread_id,
                        pane_id: pane.pane_id.clone(),
                        title: pane.title.clone(),
                        status: Some(status),
                    });
                }
            }
            HerdrEvent::PaneAgentStatusChanged {
                pane_id,
                status,
                ..
            } => {
                if let Some(snapshot) = self
                    .status_only_snapshots
                    .values_mut()
                    .find(|snapshot| snapshot.pane_id == *pane_id)
                {
                    snapshot.status = status.clone();
                    let workspace_id = snapshot.workspace_id.clone();
                    self.events.push(HerdrBridgeEvent::SubthreadStatusOnly {
                        workspace_id,
                        pane_id: pane_id.clone(),
                        status: status.clone(),
                    });
                    return;
                }
                let Some(record) = self.live_subthread_record_for_pane(None, pane_id) else {
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
                sequence,
                ..
            } => {
                let Some(record) = self.live_subthread_record_for_pane(None, pane_id) else {
                    return;
                };
                let output = self
                    .pane_outputs
                    .entry(pane_id.clone())
                    .or_insert_with(|| (0, String::new()));
                if *revision <= output.0 {
                    return;
                }
                // The output watcher fetches the complete pane.read buffer and
                // marks it as an unsequenced event. Unsequenced output is a
                // snapshot, not a delta; appending it duplicates prior screen
                // contents on every revision.
                if output.0 == 0 || *sequence == 0 || *revision != output.0 + 1 {
                    output.1 = delta.clone();
                } else {
                    output.1.push_str(delta);
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
                self.status_only_snapshots
                    .remove(&(workspace_id.clone(), pane_id.clone()));
                self.emit_subthread_closed(Some(workspace_id), pane_id);
            }
            HerdrEvent::PaneExited { pane_id, .. } => {
                self.status_only_snapshots
                    .retain(|(_, status_pane_id), _| status_pane_id != pane_id);
                self.emit_subthread_closed(None, pane_id);
            }
            _ => {}
        }
    }

    fn restore_agent_mapping(&mut self, record: HerdrMappingRecord) {
        // A restore can rebind an agent identity to a new pane/workspace.
        // Replace the old location atomically so two keys cannot claim the
        // same live Herdr agent after reconnect.
        let source = record
            .key
            .agent_session
            .as_ref()
            .and_then(|identity| {
                self.state.mappings.iter().find_map(|(_key, existing)| {
                    (existing.key != record.key
                        && !existing.is_tombstone()
                        && existing.key.session == record.key.session
                        && existing.key.agent_session.as_ref() == Some(identity))
                        .then(|| existing.key.clone())
                })
            });
        if let Some(source) = source {
            self.state.mappings.remove(&source.to_key_string());
            self.agent_snapshots.remove(&source.to_key_string());
            self.status_only_snapshots
                .remove(&(source.workspace_id.clone(), source.pane_id.clone().unwrap_or_default()));
            self.published_subthreads
                .remove(&(source.workspace_id, source.pane_id.unwrap_or_default()));
        }
        let pane_id = record.key.pane_id.clone().unwrap_or_default();
        let status_key = (record.key.workspace_id.clone(), pane_id.clone());
        if let Some(mut snapshot) = self.status_only_snapshots.remove(&status_key) {
            snapshot.pane_id = pane_id;
            snapshot.workspace_id = record.key.workspace_id.clone();
            snapshot.session_identity = record.key.agent_session.clone();
            self.agent_snapshots
                .insert(record.key.to_key_string(), snapshot);
        }
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
        self.published_subthreads
            .remove(&(record.key.workspace_id.clone(), pane_id.to_string()));
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
        // Closing the workspace retires its identity-less panes too; keep no
        // stale status-only rows for an archived workspace.
        self.status_only_snapshots
            .retain(|(workspace_id, _), _| workspace_id != &key.workspace_id);
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
            group_id: None,
            parent_thread_id: None,
            worktree_id: None,
            root_thread_id: None,
            last_activity_at: None,
            activity_status: ActivityStatus::Idle,
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
    /// Combines the protocol's nested and top-level agent collections. Herdr
    /// versions differ in which collection carries pane identity; merge
    /// duplicate locations without dropping distinct identities (which must
    /// remain visible to reconciliation as a conflict).
    fn snapshot_agents(
        snapshot: &HerdrSnapshot,
    ) -> (Vec<HerdrWorkspaceSnapshot>, Vec<HerdrAgentSnapshot>) {
        fn add_agent(agents: &mut Vec<HerdrAgentSnapshot>, agent: HerdrAgentSnapshot) {
            if agent.session_identity.is_none()
                && agents.iter().any(|existing| {
                    existing.workspace_id == agent.workspace_id
                        && existing.pane_id == agent.pane_id
                        && existing.session_identity.is_some()
                })
            {
                return;
            }
            if agent.workspace_id.is_empty() || agent.pane_id.is_empty() {
                return;
            }
            // A top-level identity-bearing record upgrades a nested
            // status-only record for the same pane.
            if agent.session_identity.is_some() {
                agents.retain(|existing| {
                    !(existing.workspace_id == agent.workspace_id
                        && existing.pane_id == agent.pane_id
                        && existing.session_identity.is_none())
                });
            }
            if let Some(existing) = agents.iter_mut().find(|existing| {
                existing.workspace_id == agent.workspace_id
                    && existing.pane_id == agent.pane_id
                    && existing.session_identity == agent.session_identity
            }) {
                *existing = agent;
            } else {
                agents.push(agent);
            }
        }

        let mut agents = Vec::new();
        for workspace in &snapshot.workspaces {
            for agent in &workspace.agents {
                let mut agent = agent.clone();
                if agent.workspace_id.is_empty() {
                    agent.workspace_id = workspace.workspace_id.clone();
                }
                add_agent(&mut agents, agent);
            }
        }
        for agent in &snapshot.agents {
            add_agent(&mut agents, agent.clone());
        }
        for pane in &snapshot.panes {
            add_agent(
                &mut agents,
                HerdrAgentSnapshot {
                    pane_id: pane.pane_id.clone(),
                    workspace_id: pane.workspace_id.clone(),
                    agent_type: pane.agent_type.clone(),
                    session_identity: pane.session_identity.clone(),
                    status: pane.status.clone(),
                    title: pane.title.clone(),
                    cwd: pane.cwd.clone(),
                    last_seen_sequence: snapshot.sequence,
                },
            );
        }

        let mut workspaces = snapshot.workspaces.clone();
        for workspace in &mut workspaces {
            workspace.agents = agents
                .iter()
                .filter(|agent| agent.workspace_id == workspace.workspace_id)
                .cloned()
                .collect();
        }
        (workspaces, agents)
    }


    fn pending_authoritative_focus_for_snapshot(
        snapshot: &HerdrSnapshot,
        workspaces: &[HerdrWorkspaceSnapshot],
        agents: &[HerdrAgentSnapshot],
    ) -> Option<PendingAuthoritativeFocus> {
        let workspace_id = snapshot.active_workspace_id.clone()?;
        let active_pane_id = snapshot.active_pane_id.clone().or_else(|| {
            workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == workspace_id)
                .and_then(|workspace| workspace.active_pane_id.clone())
        });
        let target = match active_pane_id {
            Some(pane_id) => FocusTarget::Pane {
                workspace_id: workspace_id.clone(),
                pane_id,
            },
            None => FocusTarget::Workspace(workspace_id),
        };
        let pane_identity = match &target {
            FocusTarget::Workspace(_) => None,
            FocusTarget::Pane {
                workspace_id,
                pane_id,
            } => {
                let matching_agents = agents
                    .iter()
                    .filter(|agent| {
                        agent.workspace_id == *workspace_id && agent.pane_id == *pane_id
                    })
                    .collect::<Vec<_>>();
                Some(match matching_agents.as_slice() {
                    [agent] => agent
                        .session_identity
                        .clone()
                        .map(PendingAuthoritativePaneIdentity::Exact)
                        .unwrap_or(PendingAuthoritativePaneIdentity::AwaitReplay),
                    [] => PendingAuthoritativePaneIdentity::AwaitReplay,
                    _ => PendingAuthoritativePaneIdentity::RootFallback,
                })
            }
        };
        Some(PendingAuthoritativeFocus {
            target,
            pane_identity,
        })
    }

    fn apply_snapshot(&mut self, snapshot: HerdrSnapshot) {
        // The server can expose pane identities either nested under each
        // workspace or in the protocol-level `agents`/`panes` collections.
        // Reconcile one de-duplicated view so either representation restores
        // the same mapping exactly once.
        let (workspaces, agents) = Self::snapshot_agents(&snapshot);
        self.current_focus_workspace = None;
        self.status_only_snapshots.retain(|(workspace_id, pane_id), _| {
            agents.iter().any(|agent| {
                agent.session_identity.is_none()
                    && agent.workspace_id == *workspace_id
                    && agent.pane_id == *pane_id
            })
        });
        self.pending_authoritative_focus =
            Self::pending_authoritative_focus_for_snapshot(&snapshot, &workspaces, &agents);
        let actions = reconcile_snapshot(
            &self.state.session,
            &workspaces,
            &self.state.mappings,
        );
        for action in actions {
            self.apply_action(action);
        }
        for workspace in &workspaces {
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
        for agent in &agents {
            let workspace_id = &agent.workspace_id;
            if agent.session_identity.is_none() {
                // Reverse identity transition: a pane that previously
                // published an identity must not keep its stale
                // identity-bearing record next to the status-only one.
                self.agent_snapshots.retain(|_, existing| {
                    existing.workspace_id != *workspace_id || existing.pane_id != agent.pane_id
                });
                self.published_subthreads
                    .remove(&(workspace_id.clone(), agent.pane_id.clone()));
                self.status_only_snapshots.insert(
                    (agent.workspace_id.clone(), agent.pane_id.clone()),
                    agent.clone(),
                );
                if self
                    .root_mapping(workspace_id)
                    .is_some_and(|record| !record.is_tombstone())
                {
                    self.events.push(HerdrBridgeEvent::SubthreadStatusOnly {
                        workspace_id: workspace_id.clone(),
                        pane_id: agent.pane_id.clone(),
                        status: agent.status.clone(),
                    });
                }
                continue;
            }
            let Some(session) = agent.session_identity.clone() else {
                continue;
            };
            // `reconcile_snapshot` already decided which identities may be
            // persisted; conflicting or unpersisted keys are never emitted.
            let Some(record) = self
                .live_subthread_record_for_pane(Some(workspace_id), &agent.pane_id)
            else {
                continue;
            };
            if record.key.agent_session.as_ref() != Some(&session) {
                continue;
            }
            self.status_only_snapshots
                .remove(&(workspace_id.clone(), agent.pane_id.clone()));
            self.agent_snapshots
                .insert(record.key.to_key_string(), agent.clone());
            self.published_subthreads
                .insert((workspace_id.clone(), agent.pane_id.clone()));
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
            let sequence_before = self.state.last_sequence;
            let move_source = self.pane_move_source(&event);
            let fenced = self.focus_is_fenced(&event);
            let stale = self.focus_event_is_stale(&event);
            if stale && !fenced {
                continue;
            }
            let retained_status = self.retained_status_for_pane_updated(&event);
            let state_event = self.event_for_state(&event);
            let applied = self.reconcile_state_event(&state_event);
            let accepted = Self::accepted_subthread_reconciliation(
                &event,
                &applied,
                sequence_before,
                self.state.last_sequence,
            );
            self.note_focus_event(&event, &applied, fenced, stale);
            self.apply_actions(applied);
            self.update_pending_authoritative_focus_from_replay(&event, accepted);
            if accepted {
                self.clear_status_only_for_workspace_close(&event);
                if matches!(event, HerdrEvent::PaneMoved { .. }) {
                    self.emit_pane_moved_event(&event, move_source);
                } else {
                    self.emit_subthread_event(&event, retained_status);
                }
            }
        }
    }

    fn unique_live_subthread_at(
        &self,
        workspace_id: &str,
        pane_id: &str,
    ) -> Option<HerdrMappingKey> {
        let mut candidates = self.state.mappings.values().filter(|record| {
            !record.is_tombstone()
                && record.key.session == self.state.session
                && record.key.workspace_id == workspace_id
                && record.key.pane_id.as_deref() == Some(pane_id)
        });
        let candidate = candidates.next()?.key.clone();
        if candidates.next().is_some() {
            None
        } else {
            Some(candidate)
        }
    }

    fn update_pending_authoritative_focus_from_replay(
        &mut self,
        event: &HerdrEvent,
        accepted: bool,
    ) {
        if !accepted {
            return;
        }
        let Some(PendingAuthoritativeFocus {
            target:
                FocusTarget::Pane {
                    workspace_id,
                    pane_id,
                },
            pane_identity: Some(PendingAuthoritativePaneIdentity::AwaitReplay),
        }) = self.pending_authoritative_focus.as_ref()
        else {
            return;
        };
        let (event_workspace_id, event_pane_id) = match event {
            HerdrEvent::PaneMoved { pane, .. } | HerdrEvent::PaneUpdated { pane, .. } => {
                (&pane.workspace_id, &pane.pane_id)
            }
            HerdrEvent::PaneAgentDetected {
                workspace_id,
                pane_id,
                ..
            } => (workspace_id, pane_id),
            _ => return,
        };
        if event_workspace_id != workspace_id || event_pane_id != pane_id {
            return;
        }
        let identity = self
            .unique_live_subthread_at(workspace_id, pane_id)
            .and_then(|key| key.agent_session);
        if let Some(PendingAuthoritativeFocus {
            pane_identity: Some(pane_identity),
            ..
        }) = self.pending_authoritative_focus.as_mut()
        {
            *pane_identity = identity
                .map(PendingAuthoritativePaneIdentity::Exact)
                .unwrap_or(PendingAuthoritativePaneIdentity::RootFallback);
        }
    }

    fn active_authoritative_subthread(
        &self,
        workspace_id: &str,
        pane_id: &str,
        agent_session: &HerdrAgentSessionIdentity,
    ) -> Option<HerdrMappingKey> {
        let key = self.unique_live_subthread_at(workspace_id, pane_id)?;
        if key.agent_session.as_ref() != Some(agent_session) {
            return None;
        }
        self.agent_snapshots
            .get(&key.to_key_string())
            .filter(|snapshot| {
                snapshot.workspace_id == workspace_id
                    && snapshot.pane_id == pane_id
                    && snapshot.session_identity.as_ref() == Some(agent_session)
            })?;
        Some(key)
    }

    fn activate_pending_authoritative_focus(&mut self) {
        let Some(focus) = self.pending_authoritative_focus.take() else {
            return;
        };
        let key = match focus.target {
            FocusTarget::Workspace(workspace_id) => self.state.workspace_key(&workspace_id),
            FocusTarget::Pane {
                workspace_id,
                pane_id,
            } => focus
                .pane_identity
                .and_then(|identity| match identity {
                    PendingAuthoritativePaneIdentity::Exact(agent_session) => self
                        .active_authoritative_subthread(&workspace_id, &pane_id, &agent_session),
                    PendingAuthoritativePaneIdentity::AwaitReplay
                    | PendingAuthoritativePaneIdentity::RootFallback => None,
                })
                .unwrap_or_else(|| self.state.workspace_key(&workspace_id)),
        };
        self.activate_mapping(&key);
    }

    fn apply_bootstrap(&mut self, bootstrap: HerdrBootstrap, cx: &mut Context<Self>) {
        let start = self.events.len();
        self.set_status(HerdrConnectionStatus::Synchronizing);
        self.apply_snapshot(bootstrap.snapshot.clone());
        self.merge_existing_metadata(&bootstrap.snapshot.workspaces, cx);
        self.apply_replay_events(bootstrap.events);
        if self.current_focus_workspace.is_none() {
            self.activate_pending_authoritative_focus();
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
        if self.owner.is_none() {
            return;
        }
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
        let sync_cancelled = self.sync_cancelled.clone();
        let sync_cancel_rx = self.sync_cancel_rx.clone();
        cx.spawn(async move |this, cx| {
            let mut backoff = Duration::from_millis(100);
            loop {
                let owner_active = this
                    .update(cx, |bridge, _cx| bridge.owner.is_some())
                    .unwrap_or(false);
                if !owner_active
                    || sync_cancelled.load(Ordering::SeqCst)
                    || !active.load(Ordering::SeqCst)
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
                                if bridge.owner.is_none()
                                    || !active.load(Ordering::SeqCst)
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
                                        if bridge.owner.is_none()
                                            || !active.load(Ordering::SeqCst)
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
                            if bridge.owner.is_none()
                                || !active.load(Ordering::SeqCst)
                                || sync_generation.load(Ordering::SeqCst) != generation
                            {
                                return;
                            }
                            let start = bridge.events.len();
                            bridge.active_subscription_id = None;
                            bridge.active_subscription_ids.clear();
                            bridge.set_status(HerdrConnectionStatus::Unavailable);
                            bridge.emit_new_events(start, cx);
                        });
                        log::debug!("Herdr bootstrap failed; retrying: {error}");
                    }
                }
                let owner_active = this
                    .update(cx, |bridge, _cx| bridge.owner.is_some())
                    .unwrap_or(false);
                if !owner_active
                    || !active.load(Ordering::SeqCst)
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
                    if bridge.owner.is_none()
                        || !active.load(Ordering::SeqCst)
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
        if self.owner.is_none() || self.sync_started {
            return;
        }
        self.sync_started = true;
        self.start_sync(cx);
    }

    pub(crate) fn stop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        self.sync_cancelled.store(true, Ordering::SeqCst);
        self.sync_generation.fetch_add(1, Ordering::SeqCst);
        let _ = self.sync_cancel_tx.try_send(());
        // Closing the generation channel wakes every old receiver, rather
        // than letting one cloned receiver consume the only cancellation.
        self.sync_cancel_tx.close();
        if let Some(client) = self.client.as_ref() {
            client.cancel_subscriptions();
        }
        self.sync_started = false;
        self.active_subscription_id = None;
        self.active_subscription_ids.clear();
        self.owner = None;
        self.client = None;
        self.event_receiver = None;
        self.set_status(HerdrConnectionStatus::Dormant);
    }

    fn activate(
        &mut self,
        selection: HerdrSessionSelection,
        owner: HerdrOwnerProcess,
        client: Arc<dyn HerdrApi>,
        event_receiver: async_channel::Receiver<HerdrEventCursor>,
        mappings: SessionMappings,
    ) {
        let session = selection.session_name();
        let (sync_cancel_tx, sync_cancel_rx) = async_channel::unbounded();
        self.selection = selection;
        self.owner = Some(owner);
        self.client = Some(client);
        self.event_receiver = Some(event_receiver);
        self.state.session = session;
        self.state.mappings = mappings;
        self.active.store(true, Ordering::SeqCst);
        self.sync_cancelled = Arc::new(AtomicBool::new(false));
        self.sync_cancel_tx = sync_cancel_tx;
        self.sync_cancel_rx = sync_cancel_rx;
        self.sync_started = false;
        self.active_subscription_id = None;
        self.active_subscription_ids.clear();
    }

    /// Disconnect the current state before binding this bridge to a new
    /// session. The fresh snapshot is loaded by [`begin_sync`].
    pub(crate) fn rebind_session(&mut self, session: impl Into<String>) -> Result<()> {
        let session = session.into();
        if session.trim().is_empty() {
            return Err(anyhow!("Herdr session name cannot be empty"));
        }
        let Some(owner) = self.owner.as_ref() else {
            return Err(anyhow!(
                "Herdr session rebind requires an active owned bridge"
            ));
        };
        if owner.session_name != session || self.state.session != session {
            return Err(anyhow!(
                "Herdr session rebind must use the active persisted session {:?}",
                owner.session_name
            ));
        }
        self.active.store(false, Ordering::SeqCst);
        self.sync_cancelled.store(true, Ordering::SeqCst);
        self.sync_generation.fetch_add(1, Ordering::SeqCst);
        let _ = self.sync_cancel_tx.try_send(());
        self.sync_cancel_tx.close();
        if let Some(client) = self.client.as_ref() {
            client.cancel_subscriptions();
        }
        let (sync_cancel_tx, sync_cancel_rx) = async_channel::unbounded();
        self.sync_cancelled = Arc::new(AtomicBool::new(false));
        self.sync_cancel_tx = sync_cancel_tx;
        self.sync_cancel_rx = sync_cancel_rx;
        self.sync_started = false;
        self.active_subscription_id = None;
        self.active_subscription_ids.clear();
        self.agent_snapshots.clear();
        self.status_only_snapshots.clear();
        self.pane_outputs.clear();
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
        let session_name = match &selection {
            HerdrSessionSelection::Named(session) if !session.trim().is_empty() => session.clone(),
            HerdrSessionSelection::Named(_) => {
                return Err(anyhow!("Herdr session name cannot be empty"));
            }
            HerdrSessionSelection::Default | HerdrSessionSelection::Explicit(_) => {
                return Err(anyhow!(
                    "Herdr session rebind requires the active named session"
                ));
            }
        };
        let Some(owner) = self.owner.as_ref() else {
            return Err(anyhow!(
                "Herdr session rebind requires an active owned bridge"
            ));
        };
        if owner.session_name != session_name || self.state.session != session_name {
            return Err(anyhow!(
                "Herdr session rebind must use the active persisted session {:?}",
                owner.session_name
            ));
        }
        let event_start = self.events.len();
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
        self.events.push(HerdrBridgeEvent::SessionRebound);
        self.emit_new_events(event_start, cx);
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

    pub(crate) fn create_request_fence(&self) -> (String, u64) {
        (
            self.state.session.clone(),
            self.sync_generation.load(Ordering::SeqCst),
        )
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

    /// Reconciles a successful `workspace.create` response directly into the
    pub(crate) fn apply_create_response(
        &mut self,
        workspace: HerdrWorkspaceSnapshot,
        expected_session: &str,
        expected_generation: u64,
        cx: &mut Context<Self>,
    ) -> Option<ThreadId> {
        if workspace.workspace_id.is_empty()
            || self.state.session != expected_session
            || self.sync_generation.load(Ordering::SeqCst) != expected_generation
        {
            return None;
        }
        let previous_mappings = self.state.mappings.clone();
        let thread_id = self.create_or_restore_root(&workspace, true);
        self.persist_metadata_changes(&previous_mappings, cx);
        self.persist_dirty_metadata(cx);
        self.persist_mappings(cx);
        Some(thread_id)
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
}

impl Default for HerdrBridgeRegistry {
    fn default() -> Self {
        Self {
            bridges: HashMap::default(),
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


    pub(crate) fn activate_window(
        &mut self,
        window_id: WindowId,
        selection: HerdrSessionSelection,
        owner: HerdrOwnerProcess,
        cx: &mut App,
    ) -> Result<Entity<HerdrThreadBridge>> {
        let session_name = match &selection {
            HerdrSessionSelection::Named(session) if !session.trim().is_empty() => session.clone(),
            HerdrSessionSelection::Named(_) => {
                return Err(anyhow!("Herdr session name cannot be empty"));
            }
            HerdrSessionSelection::Default | HerdrSessionSelection::Explicit(_) => {
                return Err(anyhow!(
                    "owned Herdr activation requires a named session"
                ));
            }
        };
        if owner.session_name != session_name {
            return Err(anyhow!(
                "Herdr owner session {:?} does not match activation session {:?}",
                owner.session_name,
                session_name
            ));
        }

        if let Some(bridge) = self.bridges.get(&window_id).cloned() {
            let current_owner = bridge.read(cx).owner().cloned();
            if let Some(current_owner) = current_owner {
                let same_process = match (current_owner.process_id, owner.process_id) {
                    (None, None) => true,
                    (None, Some(_)) => true,
                    (Some(existing), Some(candidate)) => existing == candidate,
                    (Some(_), None) => false,
                };
                if current_owner.terminal_id != owner.terminal_id
                    || current_owner.session_name != owner.session_name
                    || !same_process
                {
                    return Err(anyhow!(
                        "Herdr window {window_id:?} has a conflicting owner terminal {:?} session {:?} process {:?}",
                        current_owner.terminal_id,
                        current_owner.session_name,
                        current_owner.process_id
                    ));
                }
                if bridge.read(cx).session_name() != session_name {
                    return Err(anyhow!(
                        "Herdr window {window_id:?} is bound to persisted session {:?}",
                        bridge.read(cx).session_name()
                    ));
                }
                if current_owner.process_id.is_none() && owner.process_id.is_some() {
                    bridge.update(cx, |bridge, _cx| {
                        if let Some(current_owner) = bridge.owner.as_mut() {
                            current_owner.process_id = owner.process_id;
                        }
                    });
                }
                return Ok(bridge);
            }

            if bridge.read(cx).session_name() != session_name {
                return Err(anyhow!(
                    "Herdr window {window_id:?} is bound to persisted session {:?}",
                    bridge.read(cx).session_name()
                ));
            }

            let endpoint = selection.endpoint();
            let client = HerdrClientHandle::new(endpoint, cx)
                .map_err(|error| anyhow!("Herdr bridge client creation failed: {error}"))?;
            let event_receiver = client.subscribe_with_cursor();
            let client: Arc<dyn HerdrApi> = Arc::new(client);
            let mappings = match HerdrMappingStore::load_session(
                &KeyValueStore::global(cx),
                &session_name,
            ) {
                Ok(mappings) => mappings,
                Err(error) => {
                    log::warn!("Herdr bridge mapping load failed: {error}");
                    SessionMappings::default()
                }
            };
            bridge.update(cx, |bridge, _cx| {
                bridge.activate(
                    selection,
                    owner,
                    client,
                    event_receiver,
                    mappings,
                );
            });
            crate::agent_panel::attach_herdr_bridge_to_window(window_id, bridge.clone(), cx);
            bridge.update(cx, |bridge, cx| bridge.begin_sync(cx));
            return Ok(bridge);
        }

        let endpoint = selection.endpoint();
        let client = HerdrClientHandle::new(endpoint, cx)
            .map_err(|error| anyhow!("Herdr bridge client creation failed: {error}"))?;
        let event_receiver = client.subscribe_with_cursor();
        let client: Arc<dyn HerdrApi> = Arc::new(client);
        let mappings = match HerdrMappingStore::load_session(
            &KeyValueStore::global(cx),
            &session_name,
        ) {
            Ok(mappings) => mappings,
            Err(error) => {
                log::warn!("Herdr bridge mapping load failed: {error}");
                SessionMappings::default()
            }
        };
        let bridge = cx.new(|_| {
            HerdrThreadBridge::new(
                Some(window_id),
                selection,
                Some(client),
                Some(event_receiver),
                mappings,
            )
        });
        bridge.update(cx, |bridge, _cx| bridge.set_owner(owner));
        self.bridges.insert(window_id, bridge.clone());
        crate::agent_panel::attach_herdr_bridge_to_window(window_id, bridge.clone(), cx);
        bridge.update(cx, |bridge, cx| bridge.begin_sync(cx));
        Ok(bridge)
    }

    pub(crate) fn bridge_for_window(
        &self,
        window_id: WindowId,
        _cx: &App,
    ) -> Option<Entity<HerdrThreadBridge>> {
        self.bridges.get(&window_id).cloned()
    }

    /// Rebinds an already-owned bridge to its persisted named session.
    ///
    /// Arbitrary/default sessions are deliberately rejected by
    /// [`HerdrThreadBridge::rebind_selection`].
    pub(crate) fn rebind_window_session(
        &mut self,
        window_id: WindowId,
        selection: HerdrSessionSelection,
        cx: &mut App,
    ) -> Result<()> {
        let bridge = self
            .bridges
            .get(&window_id)
            .cloned()
            .ok_or_else(|| anyhow!("no Herdr bridge is bound to this window"))?;
        bridge.update(cx, |bridge, cx| bridge.rebind_selection(selection, cx))
    }

    pub(crate) fn release_owner_process(
        &mut self,
        window_id: WindowId,
        terminal_id: gpui::EntityId,
        process_id: Option<u32>,
        cx: &mut App,
    ) {
        let Some(bridge) = self.bridges.get(&window_id).cloned() else {
            return;
        };
        let owner_matches = bridge.read(cx).owner().is_some_and(|owner| {
            owner.terminal_id == terminal_id
                && (owner.process_id == process_id
                    || owner.process_id.is_none()
                    || process_id.is_none())
        });
        if owner_matches {
            let _ = bridge.update(cx, |bridge, cx| {
                let start = bridge.events.len();
                bridge.stop();
                bridge.emit_new_events(start, cx);
            });
        }
    }

    pub(crate) fn release_window(&mut self, window_id: WindowId, cx: &mut App) {
        if let Some(bridge) = self.bridges.remove(&window_id) {
            let _ = bridge.update(cx, |bridge, _cx| bridge.stop());
        }
    }

    /// A panel release only detaches that panel's subscription. The window
    /// observer owns bridge lifetime, so this method intentionally does not
    /// stop or remove the bridge.
    pub(crate) fn release_panel(&mut self, _window_id: WindowId, _cx: &mut App) {}
}

/// Records outbound API calls so UI tests can assert side effects without a
/// real Herdr server.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct RecordingHerdrApi {
    calls: parking_lot::Mutex<Vec<String>>,
    create_response: parking_lot::Mutex<Option<HerdrWorkspaceSnapshot>>,
}

#[cfg(any(test, feature = "test-support"))]
impl RecordingHerdrApi {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: parking_lot::Mutex::new(Vec::new()),
            create_response: parking_lot::Mutex::new(None),
        })
    }

    pub(crate) fn set_create_response(&self, snapshot: HerdrWorkspaceSnapshot) {
        *self.create_response.lock() = Some(snapshot);
    }
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }



    fn record(&self, call: String) {
        self.calls.lock().push(call);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl HerdrApi for RecordingHerdrApi {
    fn ping(&self, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        Task::ready(Ok(()))
    }
    fn subscribe_events(&self, _cx: &App) -> Task<Result<String, HerdrClientError>> {
        Task::ready(Ok(String::new()))
    }
    fn bootstrap(&self, _cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        self.record("bootstrap".to_string());
        Task::ready(Err(HerdrClientError::Disconnected))
    }
    fn get_snapshot(&self, _cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>> {
        Task::ready(Err(HerdrClientError::Disconnected))
    }
    fn focus_workspace(
        &self,
        workspace_id: &str,
        _operation_id: Option<&str>,
        _origin: Option<&str>,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("focus_workspace:{workspace_id}"));
        Task::ready(Ok(()))
    }
    fn create_workspace(
        &self,
        _label: &str,
        _paths: Vec<String>,
        _cx: &App,
    ) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>> {
        Task::ready(
            self.create_response
                .lock()
                .take()
                .ok_or(HerdrClientError::Disconnected),
        )
    }
    fn rename_workspace(
        &self,
        workspace_id: &str,
        label: &str,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("rename_workspace:{workspace_id}:{label}"));
        Task::ready(Ok(()))
    }
    fn close_workspace(&self, workspace_id: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("close_workspace:{workspace_id}"));
        Task::ready(Ok(()))
    }
    fn focus_pane(
        &self,
        pane_id: &str,
        _operation_id: Option<&str>,
        _origin: Option<&str>,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("focus_pane:{pane_id}"));
        Task::ready(Ok(()))
    }
    fn close_pane(&self, pane_id: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("close_pane:{pane_id}"));
        Task::ready(Ok(()))
    }
    fn prompt_agent(&self, pane_id: &str, prompt: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("prompt_agent:{pane_id}:{prompt}"));
        Task::ready(Ok(()))
    }
    fn send_agent_keys(&self, pane_id: &str, keys: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("send_agent_keys:{pane_id}:{keys}"));
        Task::ready(Ok(()))
    }
    fn send_pane_keys(&self, pane_id: &str, keys: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("send_pane_keys:{pane_id}:{keys}"));
        Task::ready(Ok(()))
    }
    fn send_pane_text(&self, pane_id: &str, text: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("send_pane_text:{pane_id}:{text}"));
        Task::ready(Ok(()))
    }
    fn send_pane_input(
        &self,
        pane_id: &str,
        text: Option<&str>,
        keys: Vec<String>,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        self.record(format!("send_pane_input:{pane_id}:{text:?}:{keys:?}"));
        Task::ready(Ok(()))
    }
    fn split_pane(&self, _pane_id: &str, _direction: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        Task::ready(Err(HerdrClientError::Disconnected))
    }
    fn rename_agent(
        &self,
        _pane_id: &str,
        _name: Option<&str>,
        _cx: &App,
    ) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        Task::ready(Err(HerdrClientError::Disconnected))
    }
    fn start_agent(
        &self,
        _pane_id: &str,
        _kind: &str,
        _name: &str,
        _args: Vec<String>,
        _cx: &App,
    ) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        Task::ready(Err(HerdrClientError::Disconnected))
    }
    fn read_pane_output(
        &self,
        _pane_id: &str,
        _since_revision: Option<u64>,
        _cx: &App,
    ) -> Task<Result<(u64, String), HerdrClientError>> {
        Task::ready(Err(HerdrClientError::Disconnected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::herdr_client::{HerdrEvent, HerdrPaneSnapshot, HerdrWorkspaceSnapshot};
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
    fn workspace_rename_updates_generated_title_without_clobbering_override() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        let metadata = bridge
            .root_metadata
            .get_mut("w1")
            .expect("workspace metadata");
        metadata.title = Some("Pinned".into());
        metadata.title_override = Some("Pinned".into());

        bridge.apply_event(HerdrEvent::WorkspaceRenamed {
            workspace_id: "w1".to_string(),
            label: "Generated rename".to_string(),
            sequence: 2,
        });

        let metadata = bridge.root_metadata("w1").expect("workspace metadata");
        assert_eq!(metadata.title_override.as_deref(), Some("Pinned"));
        assert_eq!(metadata.title.as_deref(), Some("Generated rename"));
        assert_eq!(bridge.root_title("w1").as_deref(), Some("Pinned"));
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::RootRenamed { title, .. } if title == "Generated rename"
        )));
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
    fn session_rebind_requires_the_active_persisted_session() {
        let mut bridge = HerdrThreadBridge::for_test_in_session("alpha");
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.set_owner(HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(3u64),
            process_id: Some(3),
            session_name: "alpha".to_string(),
        });
        assert!(bridge.rebind_session("beta").is_err());
        bridge
            .rebind_session("alpha")
            .expect("rebind to the active persisted session should succeed");
        assert_eq!(bridge.session_name(), "alpha");
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
    fn restoring_a_moved_agent_replaces_its_source_mapping() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 2,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p2".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 3,
        });

        assert!(bridge.root_mapping("w1").is_some());
        assert!(bridge
            .state
            .mappings
            .values()
            .any(|record| record.key.pane_id.as_deref() == Some("p2")));
        assert!(!bridge
            .state
            .mappings
            .values()
            .any(|record| record.key.pane_id.as_deref() == Some("p1")));
    }

    #[test]
    fn snapshot_reconciles_top_level_agents_and_panes_once() {
        let agent = HerdrAgentSnapshot {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            status: HerdrAgentStatus::Working,
            title: Some("Agent".to_string()),
            ..Default::default()
        };
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "server-session".to_string(),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            agents: vec![agent.clone()],
            panes: vec![HerdrPaneSnapshot {
                pane_id: agent.pane_id.clone(),
                workspace_id: agent.workspace_id.clone(),
                agent_type: agent.agent_type.clone(),
                session_identity: agent.session_identity.clone(),
                status: agent.status.clone(),
                title: agent.title.clone(),
                ..Default::default()
            }],
            ..Default::default()
        });

        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);
        assert_eq!(
            bridge
                .take_events()
                .iter()
                .filter(|event| matches!(event, HerdrBridgeEvent::SubthreadCreated { pane_id, .. } if pane_id == "p1"))
                .count(),
            1
        );
    }

    #[test]
    fn identityless_snapshot_pane_is_retained_until_identity_arrives() {
        let mut bridge = test_bridge();
        let status = HerdrAgentStatus::Working;
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            panes: vec![HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                status: status.clone(),
                ..Default::default()
            }],
            ..Default::default()
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].pane_id, "p1");
        assert_eq!(snapshots[0].session_identity, None);
        assert_eq!(snapshots[0].status, status);
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadStatusOnly {
                workspace_id,
                pane_id,
                ..
            } if workspace_id == "w1" && pane_id == "p1"
        )));

        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            panes: vec![HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
                status,
                ..Default::default()
            }],
            ..Default::default()
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].session_identity,
            Some(HerdrAgentSessionIdentity::id("session-1"))
        );
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated { pane_id, .. } if pane_id == "p1"
        )));
    }

    #[test]
    fn explicit_endpoint_keeps_selection_session_key_when_snapshot_differs() {
        let selection = HerdrSessionSelection::Explicit("/tmp/herdr.sock".to_string());
        let mut bridge = HerdrThreadBridge::new(
            None,
            selection,
            None,
            None,
            SessionMappings::default(),
        );
        bridge.apply_snapshot(HerdrSnapshot {
            session: "server-canonical-session".to_string(),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        });

        assert_eq!(bridge.session_name(), "/tmp/herdr.sock");
        assert_eq!(
            bridge.root_mapping("w1").map(|record| record.key.session.as_str()),
            Some("/tmp/herdr.sock")
        );
    }

    #[test]
    fn status_and_output_events_after_close_do_not_resurrect_a_subthread() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 2,
        });
        bridge.take_events();
        bridge.apply_event(HerdrEvent::PaneClosed {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 4,
        });
        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 1,
            delta: "stale".to_string(),
            sequence: 0,
        });

        assert!(bridge.take_events().iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::SubthreadUpdated { .. } | HerdrBridgeEvent::SubthreadOutput { .. }
        )));
    }

    #[test]
    fn identityless_agent_detection_is_forwarded_as_status_only() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });

        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadStatusOnly {
                workspace_id,
                pane_id,
                ..
            } if workspace_id == "w1" && pane_id == "p1"
        )));
    }

    #[test]
    fn snapshot_identity_loss_replaces_the_agent_snapshot_with_status_only() {
        let mut bridge = test_bridge();
        let workspace = HerdrWorkspaceSnapshot {
            workspace_id: "w1".to_string(),
            label: "Review".to_string(),
            paths: vec!["/repo".to_string()],
            ..Default::default()
        };
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            workspaces: vec![workspace.clone()],
            panes: vec![HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
                status: HerdrAgentStatus::Working,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);
        assert_eq!(
            bridge
                .take_events()
                .iter()
                .filter(|event| matches!(event, HerdrBridgeEvent::SubthreadCreated { .. }))
                .count(),
            1
        );

        // The pane is later reported identity-less; only the status-only
        // record must remain.
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            workspaces: vec![workspace],
            panes: vec![HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                status: HerdrAgentStatus::Blocked,
                ..Default::default()
            }],
            ..Default::default()
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].pane_id, "p1");
        assert_eq!(snapshots[0].session_identity, None);
        assert_eq!(snapshots[0].status, HerdrAgentStatus::Blocked);
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadStatusOnly { pane_id, .. } if pane_id == "p1"
        )));
    }

    #[test]
    fn live_identity_loss_replaces_the_agent_snapshot_with_status_only() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 3,
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].session_identity, None);
    }

    #[test]
    fn identity_upgrade_preserves_the_retained_status_only_status() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 4,
        });

        assert!(bridge.subthread_snapshots("w1").iter().all(
            |snapshot| snapshot.status == HerdrAgentStatus::Working
        ));
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated {
                pane_id,
                status: HerdrAgentStatus::Working,
                ..
            } if pane_id == "p1"
        )));
    }
    #[test]
    fn pane_updated_identity_upgrade_preserves_the_retained_status_only_status() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneUpdated {
            pane: crate::herdr_client::HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
                status: HerdrAgentStatus::default(),
                title: Some("live title".to_string()),
                ..Default::default()
            },
            sequence: 4,
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].status, HerdrAgentStatus::Working);
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated {
                pane_id,
                status: HerdrAgentStatus::Working,
                ..
            } if pane_id == "p1"
        )));
    }

    #[test]
    fn live_identity_transition_preserves_status_through_reupgrade() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 4,
        });
        assert_eq!(
            bridge
                .subthread_snapshots("w1")
                .first()
                .map(|snapshot| &snapshot.status),
            Some(&HerdrAgentStatus::Working)
        );
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 5,
        });

        assert_eq!(
            bridge
                .subthread_snapshots("w1")
                .first()
                .map(|snapshot| &snapshot.status),
            Some(&HerdrAgentStatus::Working)
        );
        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated {
                pane_id,
                status: HerdrAgentStatus::Working,
                ..
            } if pane_id == "p1"
        )));
    }

    #[test]
    fn repeated_identityless_detection_refreshes_agent_metadata() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("codex".to_string()),
            session_identity: None,
            sequence: 3,
        });

        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].agent_type.as_deref(), Some("codex"));
    }

    #[test]
    fn workspace_close_clears_status_only_cache_without_live_root() {
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            panes: vec![HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                agent_type: Some("omp".to_string()),
                status: HerdrAgentStatus::Blocked,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);
        assert!(bridge.root_mapping("w1").is_none());

        bridge.apply_event(HerdrEvent::WorkspaceClosed {
            workspace_id: "w1".to_string(),
            sequence: 1,
        });

        assert!(bridge.subthread_snapshots("w1").is_empty());
    }

    #[test]
    fn workspace_close_clears_the_status_only_cache() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);

        bridge.apply_event(HerdrEvent::WorkspaceClosed {
            workspace_id: "w1".to_string(),
            sequence: 3,
        });

        assert!(bridge.subthread_snapshots("w1").is_empty());
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
        bridge.set_owner(HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(1u64),
            process_id: Some(1),
            session_name: "alpha".to_string(),
        });
        let old_cancellation = bridge.sync_cancel_rx.clone();
        let old_cancellation_clone = bridge.sync_cancel_rx.clone();
        let old_cancelled = bridge.sync_cancelled.clone();
        bridge.rebind_session("alpha").expect("rebind");
        assert!(old_cancelled.load(Ordering::SeqCst));
        assert!(old_cancellation.try_recv().is_ok());
        assert!(
            old_cancellation_clone.is_closed(),
            "closing the old generation wakes every receiver"
        );
        assert!(!bridge.sync_cancelled.load(Ordering::SeqCst));
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
        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 5,
            delta: "full-screen".to_string(),
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
        assert!(updates.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadOutput {
                pane_id,
                revision: 5,
                output,
                ..
            } if pane_id == "p1" && output == "full-screen"
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

    fn pane_updated(
        pane_id: &str,
        workspace_id: &str,
        identity: &str,
        sequence: u64,
    ) -> HerdrEvent {
        HerdrEvent::PaneUpdated {
            pane: crate::herdr_client::HerdrPaneSnapshot {
                pane_id: pane_id.to_string(),
                workspace_id: workspace_id.to_string(),
                agent_type: Some("omp".to_string()),
                session_identity: Some(HerdrAgentSessionIdentity::id(identity)),
                status: HerdrAgentStatus::Working,
                title: Some("live title".to_string()),
                ..Default::default()
            },
            sequence,
        }
    }

    #[test]
    fn pane_updated_updates_the_persisted_subthread_without_recreating_it() {
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
        assert_eq!(bridge.take_events().len(), 1); // SubthreadCreated

        bridge.apply_event(pane_updated("p1", "w1", "session-1", 3));
        let events = bridge.take_events();
        assert!(events.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadUpdated {
                pane_id,
                title: Some(title),
                status: Some(HerdrAgentStatus::Working),
                ..
            } if pane_id == "p1" && title == "live title"
        )));
        assert!(!events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::SubthreadCreated { .. })));
        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].session_identity,
            Some(HerdrAgentSessionIdentity::id("session-1"))
        );
    }

    #[test]
    fn rejected_zero_sequence_detection_does_not_refresh_existing_subthread_metadata() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("original-agent".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();
        let before = bridge
            .subthread_snapshots("w1")
            .into_iter()
            .next()
            .expect("initial subthread snapshot");

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("stale-agent".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 0,
        });
        let events = bridge.take_events();
        assert!(events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::Conflict { .. })));
        assert!(events.iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated { .. } | HerdrBridgeEvent::SubthreadUpdated { .. }
        )));

        let after = bridge
            .subthread_snapshots("w1")
            .into_iter()
            .next()
            .expect("retained subthread snapshot");
        assert_eq!(after.agent_type, before.agent_type);
        assert_eq!(after.session_identity, before.session_identity);
        assert_eq!(after.status, before.status);
        assert_eq!(after.title, before.title);
    }

    #[test]
    fn restarted_agent_identity_conflicts_instead_of_duplicating_the_live_mapping() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-2")),
            sequence: 3,
        });
        let events = bridge.take_events();
        assert!(events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::Conflict { .. })));
        assert!(!events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::SubthreadCreated { .. })));
        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].session_identity,
            Some(HerdrAgentSessionIdentity::id("session-1"))
        );
    }

    #[test]
    fn pane_update_with_foreign_identity_surfaces_a_conflict_without_emitting_a_key() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();

        bridge.apply_event(pane_updated("p1", "w1", "session-2", 3));
        let events = bridge.take_events();
        assert!(events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::Conflict { .. })));
        assert!(events.iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated { .. } | HerdrBridgeEvent::SubthreadUpdated { .. }
        )));
        let snapshots = bridge.subthread_snapshots("w1");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(
            snapshots[0].session_identity,
            Some(HerdrAgentSessionIdentity::id("session-1"))
        );
    }

    #[test]
    fn pane_updated_before_detection_creates_the_subthread_once() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();

        // A PaneUpdated may be the first identity-bearing event for a pane;
        // it reconciles through the same restore-or-create outcome.
        bridge.apply_event(pane_updated("p1", "w1", "session-1", 2));
        let events = bridge.take_events();
        assert!(events.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadCreated { pane_id, .. } if pane_id == "p1"
        )));

        bridge.apply_event(pane_updated("p1", "w1", "session-1", 3));
        let events = bridge.take_events();
        assert!(!events
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::SubthreadCreated { .. })));
        assert!(events.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadUpdated { pane_id, .. } if pane_id == "p1"
        )));
    }
    #[test]
    fn rejected_stale_status_and_output_do_not_publish() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Working,
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentStatusChanged {
            pane_id: "p1".to_string(),
            status: HerdrAgentStatus::Blocked,
            sequence: 2,
        });
        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 4,
            delta: "stale".to_string(),
            sequence: 1,
        });

        let events = bridge.take_events();
        assert!(
            events.iter().all(|event| !matches!(
                event,
                HerdrBridgeEvent::SubthreadUpdated { .. }
                    | HerdrBridgeEvent::SubthreadOutput { .. }
                    | HerdrBridgeEvent::SubthreadClosed { .. }
            )),
            "rejected status/output events must not publish UI changes"
        );
        assert_eq!(
            bridge
                .subthread_snapshots("w1")
                .first()
                .map(|snapshot| &snapshot.status),
            Some(&HerdrAgentStatus::Working)
        );
    }

    #[test]
    fn rejected_zero_sequence_workspace_close_preserves_status_only_cache() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 2,
        });
        bridge.take_events();
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);

        bridge.apply_event(HerdrEvent::WorkspaceClosed {
            workspace_id: "w1".to_string(),
            sequence: 0,
        });

        assert!(bridge
            .take_events()
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::Conflict { .. })));
        assert_eq!(
            bridge
                .subthread_snapshots("w1")
                .first()
                .map(|snapshot| snapshot.session_identity.clone()),
            Some(None)
        );
    }

    #[test]
    fn identityless_zero_sequence_detection_does_not_mutate_status_only_cache() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: None,
            sequence: 0,
        });

        assert!(bridge.subthread_snapshots("w1").is_empty());
        assert!(bridge
            .take_events()
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::Conflict { .. })));
    }

    #[test]
    fn accepted_pane_move_publishes_close_and_create_for_current_mapping() {
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
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 3,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneOutput {
            pane_id: "p1".to_string(),
            revision: 1,
            delta: "screen".to_string(),
            sequence: 0,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::PaneMoved {
            pane: HerdrPaneSnapshot {
                pane_id: "p2".to_string(),
                workspace_id: "w2".to_string(),
                agent_type: Some("omp".to_string()),
                session_identity: None,
                status: HerdrAgentStatus::Working,
                title: Some("Moved agent".to_string()),
                ..Default::default()
            },
            previous_pane_id: Some("p1".to_string()),
            previous_workspace_id: Some("w1".to_string()),
            previous_tab_id: None,
            sequence: 4,
        });
        assert_eq!(
            bridge
                .pane_outputs
                .get("p2")
                .map(|(_, output)| output.as_str()),
            Some("screen")
        );

        let events = bridge.take_events();
        let close = events.iter().find_map(|event| match event {
            HerdrBridgeEvent::SubthreadClosed {
                key,
                pane_id,
                ..
            } => Some((key.clone(), pane_id.clone())),
            _ => None,
        });
        let create = events.iter().find_map(|event| match event {
            HerdrBridgeEvent::SubthreadCreated {
                key,
                pane_id,
                session,
                title,
                ..
            } => Some((key.clone(), pane_id.clone(), session.clone(), title.clone())),
            _ => None,
        });
        let (close_key, close_pane) = close.expect("move closes the source view");
        let (create_key, create_pane, session, title) =
            create.expect("move creates the destination view");
        assert_eq!(close_key.workspace_id, "w1");
        assert_eq!(close_pane, "p1");
        assert_eq!(create_key.workspace_id, "w2");
        assert_eq!(create_key.pane_id.as_deref(), Some("p2"));
        assert_eq!(create_pane, "p2");
        assert_eq!(session, HerdrAgentSessionIdentity::id("session-1"));
        assert_eq!(title, "Moved agent");
        assert_eq!(
            bridge
                .subthread_snapshots("w2")
                .first()
                .and_then(|snapshot| snapshot.session_identity.clone()),
            Some(HerdrAgentSessionIdentity::id("session-1"))
        );
    }
    #[test]
    fn snapshot_active_pane_focuses_the_matching_subthread() {
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            active_pane_id: Some("p1".to_string()),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                agents: vec![HerdrAgentSnapshot {
                    pane_id: "p1".to_string(),
                    workspace_id: "w1".to_string(),
                    agent_type: Some("omp".to_string()),
                    session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        bridge.take_events();

        bridge.activate_pending_authoritative_focus();

        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { key, .. }
                if key.workspace_id == "w1" && key.pane_id.as_deref() == Some("p1")
        )));
    }

    #[test]
    fn snapshot_active_pane_without_identity_falls_back_to_the_root() {
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            active_pane_id: Some("p1".to_string()),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                agents: vec![HerdrAgentSnapshot {
                    pane_id: "p1".to_string(),
                    workspace_id: "w1".to_string(),
                    agent_type: Some("omp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        bridge.take_events();

        bridge.activate_pending_authoritative_focus();

        let events = bridge.take_events();
        assert!(events.iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { .. }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1"
        )));
    }

    #[test]
    fn snapshot_active_duplicate_pane_mappings_fall_back_to_the_root() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        let root_thread_id = bridge.root_thread_id("w1").expect("root thread");
        for session in ["session-1", "session-2"] {
            let record = HerdrMappingRecord {
                key: HerdrMappingKey::subthread(
                    "alpha",
                    "w1",
                    "p1",
                    HerdrAgentSessionIdentity::id(session),
                ),
                zed_root_thread_id: root_thread_id,
                zed_workspace_id: None,
                zed_subthread_session_id: Some(session.to_string()),
                worktree_or_cwd_identity: None,
                last_seen_sequence: 2,
                lifecycle: HerdrLifecycleState::Active,
            };
            bridge.state.mappings.insert(record.key.to_key_string(), record);
        }
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            active_pane_id: Some("p1".to_string()),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                agents: vec![HerdrAgentSnapshot {
                    pane_id: "p1".to_string(),
                    workspace_id: "w1".to_string(),
                    agent_type: Some("omp".to_string()),
                    session_identity: Some(HerdrAgentSessionIdentity::id("session-2")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        bridge.take_events();

        bridge.activate_pending_authoritative_focus();

        let events = bridge.take_events();
        assert!(events.iter().all(|event| !matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { .. }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1"
        )));
    }

    #[test]
    fn replayed_identity_for_active_status_only_pane_focuses_the_subthread() {
        let mut bridge = test_bridge();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            active_pane_id: Some("p1".to_string()),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                agents: vec![HerdrAgentSnapshot {
                    pane_id: "p1".to_string(),
                    workspace_id: "w1".to_string(),
                    agent_type: Some("omp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        bridge.take_events();
        bridge.apply_replay_events([HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 1,
        }]);
        bridge.take_events();

        bridge.activate_pending_authoritative_focus();

        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { key, .. }
                if key.workspace_id == "w1" && key.pane_id.as_deref() == Some("p1")
        )));
    }

    #[test]
    fn replayed_identityless_pane_move_focuses_the_rebound_active_subthread() {
        let mut bridge = test_bridge();
        bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p0".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("session-1")),
            sequence: 2,
        });
        bridge.take_events();
        bridge.apply_snapshot(HerdrSnapshot {
            session: "alpha".to_string(),
            active_workspace_id: Some("w1".to_string()),
            active_pane_id: Some("p1".to_string()),
            workspaces: vec![HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                label: "Review".to_string(),
                paths: vec!["/repo".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        });
        bridge.take_events();
        bridge.apply_replay_events([HerdrEvent::PaneMoved {
            pane: HerdrPaneSnapshot {
                pane_id: "p1".to_string(),
                workspace_id: "w1".to_string(),
                ..Default::default()
            },
            previous_pane_id: Some("p0".to_string()),
            previous_workspace_id: Some("w1".to_string()),
            previous_tab_id: None,
            sequence: 3,
        }]);
        bridge.take_events();

        bridge.activate_pending_authoritative_focus();

        assert!(bridge.take_events().iter().any(|event| matches!(
            event,
            HerdrBridgeEvent::SubthreadFocused { key, .. }
                if key.workspace_id == "w1" && key.pane_id.as_deref() == Some("p1")
        )));
    }
    #[gpui::test]
    async fn root_workspace_owner_setter_getter_and_conflict_behavior(
        cx: &mut gpui::TestAppContext,
    ) {
        let bridge = cx.new(|_| test_bridge());
        bridge.update(cx, |bridge, _cx| {
            bridge.apply_event(workspace_created("w1", "/repo", "Review"));
        });
        bridge.update(cx, |bridge, _cx| {
            bridge.take_events();
        });
        let owner = workspace::WorkspaceId::from_i64(42);
        let other_owner = workspace::WorkspaceId::from_i64(43);

        assert_eq!(
            bridge.update(cx, |bridge, _cx| bridge.root_zed_workspace_id("w1")),
            None
        );
        assert!(bridge.update(cx, |bridge, cx| {
            bridge.set_root_zed_workspace_id("w1", owner, cx)
        }));
        assert_eq!(
            bridge.update(cx, |bridge, _cx| bridge.root_zed_workspace_id("w1")),
            Some(owner)
        );
        cx.run_until_parked();
        let kvp = cx.update(|cx| KeyValueStore::global(cx));
        let persisted = cx
            .background_spawn(async move {
                HerdrMappingStore::load_session(&kvp, "alpha")
            })
            .await
            .expect("owner mapping should persist");
        assert_eq!(
            persisted
                .get(&HerdrMappingKey::workspace("alpha", "w1").to_key_string())
                .and_then(|record| record.zed_workspace_id),
            Some(owner)
        );

        assert!(!bridge.update(cx, |bridge, cx| {
            bridge.set_root_zed_workspace_id("w1", owner, cx)
        }));
        assert!(bridge
            .update(cx, |bridge, _cx| bridge.take_events())
            .is_empty());

        assert!(!bridge.update(cx, |bridge, cx| {
            bridge.set_root_zed_workspace_id("w1", other_owner, cx)
        }));
        let events = bridge.update(cx, |bridge, _cx| bridge.take_events());
        assert!(matches!(
            events.as_slice(),
            [HerdrBridgeEvent::Conflict { key, message }]
                if *key == HerdrMappingKey::workspace("alpha", "w1")
                    && !message.is_empty()
        ));
        assert_eq!(
            bridge.update(cx, |bridge, _cx| bridge.root_zed_workspace_id("w1")),
            Some(owner)
        );

        assert!(!bridge.update(cx, |bridge, cx| {
            bridge.set_root_zed_workspace_id("missing", owner, cx)
        }));
    }
    #[gpui::test]
    async fn bootstrap_failures_do_not_emit_user_request_failure_events(
        cx: &mut gpui::TestAppContext,
    ) {
        let bridge = cx.new(|_| HerdrThreadBridge::for_test_with_api(RecordingHerdrApi::new()));
        bridge.update(cx, |bridge, _cx| {
            bridge.set_owner(HerdrOwnerProcess {
                terminal_id: gpui::EntityId::from(2u64),
                process_id: Some(2),
                session_name: "alpha".to_string(),
            });
        });
        bridge.update(cx, |bridge, cx| bridge.begin_sync(cx));
        cx.run_until_parked();

        let events = bridge.update(cx, |bridge, _| bridge.take_events());

        assert_eq!(
            events,
            vec![
                HerdrBridgeEvent::StatusChanged(HerdrConnectionStatus::Reconnecting),
                HerdrBridgeEvent::StatusChanged(HerdrConnectionStatus::Unavailable),
            ],
            "automatic bootstrap failures must only update connection state"
        );

        bridge.update(cx, |bridge, _| bridge.stop());
    }
    #[gpui::test]
    async fn owner_release_stops_future_bootstrap_retries(
        cx: &mut gpui::TestAppContext,
    ) {
        let api = RecordingHerdrApi::new();
        let bridge = cx.new(|_| HerdrThreadBridge::for_test_with_api(api.clone()));
        let owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(42u64),
            process_id: Some(42),
            session_name: "alpha".to_string(),
        };

        bridge.update(cx, |bridge, cx| {
            bridge.set_owner(owner.clone());
            bridge.begin_sync(cx);
        });
        cx.run_until_parked();
        let attempts_before_release = api
            .calls()
            .iter()
            .filter(|call| call.as_str() == "bootstrap")
            .count();
        assert!(attempts_before_release > 0);

        bridge.update(cx, |bridge, _cx| {
            assert_eq!(bridge.clear_owner(), Some(owner));
        });
        cx.run_until_parked();
        let attempts_after_release = api
            .calls()
            .iter()
            .filter(|call| call.as_str() == "bootstrap")
            .count();
        assert_eq!(
            attempts_after_release, attempts_before_release,
            "clearing the owner must stop the retry loop"
        );
    }

    #[gpui::test]
    async fn duplicate_owner_activation_is_rejected(
        cx: &mut gpui::TestAppContext,
    ) {
        cx.update(|cx| HerdrBridgeRegistry::init(cx));
        let window_id = WindowId::from(7u64);
        let first_owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(10u64),
            process_id: None,
            session_name: "zed-test".to_string(),
        };
        let upgraded_owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(10u64),
            process_id: Some(10),
            session_name: "zed-test".to_string(),
        };
        let different_process_owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(10u64),
            process_id: Some(99),
            session_name: "zed-test".to_string(),
        };
        let second_owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(11u64),
            process_id: Some(11),
            session_name: "zed-test".to_string(),
        };

        let first = cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.activate_window(
                    window_id,
                    HerdrSessionSelection::Named("zed-test".to_string()),
                    first_owner.clone(),
                    cx,
                )
            })
        })
        .expect("first owner should activate the bridge");
        let reused = cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.activate_window(
                    window_id,
                    HerdrSessionSelection::Named("zed-test".to_string()),
                    upgraded_owner,
                    cx,
                )
            })
        })
        .expect("same terminal/session should reuse and upgrade the bridge owner");
        let same_terminal_conflict = cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.activate_window(
                    window_id,
                    HerdrSessionSelection::Named("zed-test".to_string()),
                    different_process_owner,
                    cx,
                )
            })
        });
        assert!(
            same_terminal_conflict.is_err(),
            "a concrete PID change in the same terminal must be rejected"
        );
        assert_eq!(first, reused);
        assert_eq!(
            first.read_with(cx, |bridge, _cx| bridge.owner().and_then(|owner| owner.process_id)),
            Some(10)
        );
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_owner_process(
                    window_id,
                    gpui::EntityId::from(10u64),
                    Some(99),
                    cx,
                );
            })
        });
        assert_eq!(
            first.read_with(cx, |bridge, _cx| bridge.owner().and_then(|owner| owner.process_id)),
            Some(10),
            "a concrete PID mismatch must not release the active owner"
        );

        let conflict = cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.activate_window(
                    window_id,
                    HerdrSessionSelection::Named("zed-test".to_string()),
                    second_owner,
                    cx,
                )
            })
        });
        assert!(
            conflict.is_err(),
            "a different terminal/process must not replace the active owner"
        );
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_window(window_id, cx);
            })
        });
    }

    #[gpui::test]
    async fn rebind_requires_the_active_owned_named_session(
        cx: &mut gpui::TestAppContext,
    ) {
        let bridge = cx.new(|_| HerdrThreadBridge::for_test("alpha"));
        let default_selection = bridge.update(cx, |bridge, cx| {
            bridge.rebind_selection(HerdrSessionSelection::Default, cx)
        });
        assert!(
            default_selection.is_err(),
            "an unowned bridge must not attach the default session"
        );

        bridge.update(cx, |bridge, _cx| {
            bridge.set_owner(HerdrOwnerProcess {
                terminal_id: gpui::EntityId::from(12u64),
                process_id: Some(12),
                session_name: "alpha".to_string(),
            });
        });
        let different_named_selection = bridge.update(cx, |bridge, cx| {
            bridge.rebind_selection(
                HerdrSessionSelection::Named("external".to_string()),
                cx,
            )
        });
        assert!(
            different_named_selection.is_err(),
            "rebind must use the persisted session owned by this bridge"
        );
    }

    #[gpui::test]
    async fn releasing_a_panel_does_not_drop_an_owned_bridge(
        cx: &mut gpui::TestAppContext,
    ) {
        cx.update(|cx| HerdrBridgeRegistry::init(cx));
        let window_id = WindowId::from(8u64);
        let owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(13u64),
            process_id: Some(13),
            session_name: "zed-panel".to_string(),
        };
        let bridge = cx
            .update(|cx| {
                cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                    registry.activate_window(
                        window_id,
                        HerdrSessionSelection::Named("zed-panel".to_string()),
                        owner,
                        cx,
                    )
                })
            })
            .expect("owner should activate the bridge");
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_panel(window_id, cx);
                assert!(registry.bridge_for_window(window_id, cx).is_some());
            })
        });
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_window(window_id, cx);
            })
        });

    }
    #[gpui::test]
    async fn owner_process_release_marks_bridge_dormant(
        cx: &mut gpui::TestAppContext,
    ) {
        cx.update(|cx| HerdrBridgeRegistry::init(cx));
        let window_id = WindowId::from(9u64);
        let owner = HerdrOwnerProcess {
            terminal_id: gpui::EntityId::from(14u64),
            process_id: None,
            session_name: "zed-dormant".to_string(),
        };
        let bridge = cx
            .update(|cx| {
                cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                    registry.activate_window(
                        window_id,
                        HerdrSessionSelection::Named("zed-dormant".to_string()),
                        owner.clone(),
                        cx,
                    )
                })
            })
            .expect("owner should activate the bridge");
        bridge.update(cx, |bridge, _cx| {
            bridge.apply_event(workspace_created("w1", "/project", "Review"));
        });
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                registry.release_owner_process(
                    window_id,
                    owner.terminal_id,
                    owner.process_id,
                    cx,
                );
            })
        });
        bridge.read_with(cx, |bridge, _cx| {
            assert_eq!(bridge.status(), HerdrConnectionStatus::Dormant);
            assert!(bridge.owner().is_none());
            assert!(
                bridge.root_mapping("w1").is_some(),
                "owner release must preserve persisted root mappings"
            );
        });
        cx.update(|cx| {
            cx.update_global::<HerdrBridgeRegistry, _>(|registry, cx| {
                assert!(registry.bridge_for_window(window_id, cx).is_some());
            })
        });
    }

}
