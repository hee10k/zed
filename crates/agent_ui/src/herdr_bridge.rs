use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow};
use chrono::Utc;
use db::kvp::KeyValueStore;
use gpui::{App, AppContext as _, Context, Entity, EventEmitter, Global, Task, TaskExt, WindowId};
use workspace::PathList;

use crate::{
    herdr_client::{
        HerdrAgentSnapshot, HerdrApi, HerdrBootstrap, HerdrClientError, HerdrClientHandle,
        HerdrEvent, HerdrSnapshot, HerdrWorkspaceSnapshot,
    },
    herdr_mapping_store::{
        HerdrLifecycleState, HerdrMappingKey, HerdrMappingRecord, HerdrMappingStore,
        SessionMappings, upsert_record,
    },
    herdr_state::{
        AppliedEvent, BridgeState, FocusTarget, HerdrOperationOrigin, OutboundRequest,
        ReconciliationAction, apply_event, initiate_workspace_focus, reconcile_snapshot,
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
}

impl gpui::EventEmitter<HerdrBridgeEvent> for HerdrThreadBridge {}

/// A request sent to Herdr by a bridge action. The origin is retained even
/// though the current Herdr RPC surface carries the operation ID in params.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HerdrOperationRequest {
    pub operation_id: String,
    pub origin: HerdrOperationOrigin,
    pub target: FocusTarget,
}

/// GPUI entity holding one Herdr session's mapping and lifecycle state.
///
/// The pure event/state transitions remain in [`crate::herdr_state`]. This
/// type translates those transitions into root metadata, persisted mappings,
/// and UI-facing events.
pub(crate) struct HerdrThreadBridge {
    window_id: Option<WindowId>,
    selection: HerdrSessionSelection,
    client: Option<Arc<dyn HerdrApi>>,
    event_receiver: Option<async_channel::Receiver<HerdrEvent>>,
    state: BridgeState,
    root_metadata: HashMap<String, ThreadMetadata>,
    status: HerdrConnectionStatus,
    events: Vec<HerdrBridgeEvent>,
    outbound_requests: Vec<OutboundRequest>,
    pending_authoritative_focus: Option<String>,
    active: Arc<AtomicBool>,
}
impl HerdrThreadBridge {
    fn new(
        window_id: Option<WindowId>,
        selection: HerdrSessionSelection,
        client: Option<Arc<dyn HerdrApi>>,
        event_receiver: Option<async_channel::Receiver<HerdrEvent>>,
        mappings: SessionMappings,
    ) -> Self {
        let session = selection.session_name();
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
            active: Arc::new(AtomicBool::new(true)),
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

    fn emit_new_events(&self, start: usize, cx: &mut Context<Self>) {
        for event in self.events.iter().skip(start) {
            cx.emit(event.clone());
        }
    }

    /// Apply one pushed Herdr event without requiring a GPUI context. This is
    /// intentionally useful for deterministic lifecycle tests.
    pub(crate) fn apply_event(&mut self, event: HerdrEvent) {
        let applied = apply_event(&mut self.state, &event);
        self.apply_actions(applied);
    }

    fn apply_event_in_context(&mut self, event: HerdrEvent, cx: &mut Context<Self>) {
        let start = self.events.len();
        let applied = apply_event(&mut self.state, &event);
        self.apply_actions_in_context(applied, cx);
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
        }
    }

    fn create_agent_mapping(&mut self, agent: HerdrAgentSnapshot) {
        let Some(identity) = agent.session_identity else {
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
            zed_subthread_session_id: Some(identity.value),
            worktree_or_cwd_identity: agent.cwd,
            last_seen_sequence: agent.last_seen_sequence,
            lifecycle: HerdrLifecycleState::Active,
        };
        let _ = upsert_record(&mut self.state.mappings, record);
    }

    fn restore_agent_mapping(&mut self, record: HerdrMappingRecord) {
        self.state
            .mappings
            .insert(record.key.to_key_string(), record);
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
        if metadata.title_override.is_none() {
            self.events.push(HerdrBridgeEvent::RootRenamed {
                workspace_id: key.workspace_id.clone(),
                thread_id: metadata.thread_id,
                title,
            });
        }
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
                    store.update(cx, |store, cx| store.archive(metadata.thread_id, None, cx));
                } else {
                    store.update(cx, |store, cx| store.save(metadata.clone(), cx));
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
                    }
                }
            }
        }
        self.state.last_sequence = snapshot.sequence;
        self.pending_authoritative_focus = snapshot.active_workspace_id;
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
            }
        }
    }

    fn apply_bootstrap(&mut self, bootstrap: HerdrBootstrap, cx: &mut Context<Self>) {
        let start = self.events.len();
        self.set_status(HerdrConnectionStatus::Synchronizing);
        self.apply_snapshot(bootstrap.snapshot.clone());
        self.merge_existing_metadata(&bootstrap.snapshot.workspaces, cx);
        for event in bootstrap.events {
            if event.sequence() > bootstrap.snapshot.sequence {
                let applied = apply_event(&mut self.state, &event);
                self.apply_actions_in_context(applied, cx);
            }
        }
        if let Some(workspace_id) = self.pending_authoritative_focus.take() {
            let key = self.state.workspace_key(&workspace_id);
            self.activate_mapping(&key);
        }
        self.set_status(HerdrConnectionStatus::Ready);
        self.emit_new_events(start, cx);
        self.persist_mappings(cx);
    }
    fn start_sync(&mut self, cx: &mut Context<Self>) {
        self.active.store(true, Ordering::SeqCst);
        let status_start = self.events.len();
        self.set_status(HerdrConnectionStatus::Reconnecting);
        self.emit_new_events(status_start, cx);
        let Some(client) = self.client.clone() else {
            let status_start = self.events.len();
            self.set_status(HerdrConnectionStatus::Unavailable);
            self.emit_new_events(status_start, cx);
            return;
        };
        let bootstrap = client.bootstrap(cx);
        let events = self.event_receiver.clone();
        let active = self.active.clone();
        cx.spawn(async move |this, cx| {
            match bootstrap.await {
                Ok(bootstrap) => {
                    let started = this.update(cx, |bridge, cx| {
                        if !active.load(Ordering::SeqCst) {
                            return;
                        }
                        bridge.apply_bootstrap(bootstrap, cx);
                    });
                    if let Err(error) = started {
                        log::debug!("Herdr bridge bootstrap target was released: {error}");
                        return;
                    }
                }
                Err(error) => {
                    let updated = this.update(cx, |bridge, cx| {
                        let status_start = bridge.events.len();
                        bridge.set_status(HerdrConnectionStatus::Unavailable);
                        bridge.emit_new_events(status_start, cx);
                        log::warn!("Herdr bridge bootstrap failed: {error}");
                    });
                    if let Err(update_error) = updated {
                        log::debug!(
                            "Herdr bridge could not publish bootstrap failure: {update_error}"
                        );
                    }
                    return;
                }
            }

            let Some(events) = events else {
                return;
            };
            while active.load(Ordering::SeqCst) {
                let Ok(event) = events.recv().await else {
                    let updated = this.update(cx, |bridge, cx| {
                        let status_start = bridge.events.len();
                        bridge.set_status(HerdrConnectionStatus::Unavailable);
                        bridge.emit_new_events(status_start, cx);
                    });
                    if let Err(error) = updated {
                        log::debug!("Herdr bridge could not publish disconnect: {error}");
                    }
                    break;
                };
                let updated = this.update(cx, |bridge, cx| {
                    bridge.apply_event_in_context(event, cx);
                });
                if let Err(error) = updated {
                    log::debug!("Herdr bridge event target was released: {error}");
                    break;
                }
            }
        })
        .detach();
    }

    pub(crate) fn begin_sync(&mut self, cx: &mut Context<Self>) {
        self.start_sync(cx);
    }

    pub(crate) fn stop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        self.client = None;
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
        self.client = None;
        self.event_receiver = None;
        self.state = BridgeState::new(session.clone());
        self.pending_authoritative_focus = None;
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
        self.pending_authoritative_focus = None;
        self.rebind_session(selection.session_name())?;
        let endpoint = selection.endpoint();
        self.client = match HerdrClientHandle::new(endpoint, cx) {
            Ok(client) => {
                self.event_receiver = Some(client.subscribe());
                Some(Arc::new(client))
            }
            Err(error) => {
                log::warn!("Herdr bridge could not create a client: {error}");
                self.event_receiver = None;
                None
            }
        };
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
            origin: _,
        } = request
        else {
            return None;
        };
        let client = self.client.clone()?;
        let task = client.focus_workspace(&workspace_id, Some(&operation_id), cx);
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
            if bridge.read(cx).selection() != &session {
                let result = bridge.update(cx, |bridge, cx| {
                    bridge.rebind_selection(session.clone(), cx)
                });
                if let Err(error) = result {
                    log::warn!("Herdr bridge session rebind failed: {error}");
                }
            }
            return bridge;
        }

        let endpoint = session.endpoint();
        let (client, event_receiver) = match HerdrClientHandle::new(endpoint, cx) {
            Ok(client) => {
                let event_receiver = client.subscribe();
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
        let mappings = match KeyValueStore::global(cx)
            .scoped(crate::herdr_mapping_store::HERDR_MAPPING_NAMESPACE)
            .read(&session.session_name())
        {
            Ok(Some(payload)) => match crate::herdr_mapping_store::decode_session_map(Some(&payload)) {
                Ok(mappings) => mappings,
                Err(error) => {
                    log::warn!("Herdr bridge mapping payload is invalid: {error}");
                    SessionMappings::default()
                }
            },
            Ok(None) => SessionMappings::default(),
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
        bridge.update(cx, |bridge, cx| bridge.begin_sync(cx));
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
}
