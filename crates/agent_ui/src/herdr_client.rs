use std::collections::HashMap;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::channel::oneshot;
use gpui::{App, BackgroundExecutor, Task};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

use crate::herdr_transport::{
    ConnectionKillSwitch, HerdrEndpoint, HerdrLineReader, HerdrStream,
};

const HERDR_PROTOCOL: u64 = 20;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The replay log only needs to cover events waiting for the next bootstrap.
/// Once a bootstrap boundary is consumed, older entries are superseded by its
/// authoritative snapshot and can be discarded.
const MAX_EVENT_LOG: usize = 256;


/// Margin added on top of the server-side `events.wait` hold time so the
/// request deadline always exceeds the server wait by a bounded amount: a
/// wait cycle never abandons a live server waiter, yet never blocks past
/// the server's own timeout plus this margin.
const EVENTS_WAIT_DEADLINE_MARGIN_MS: u64 = 5_000;

#[derive(Clone)]
struct SubscriptionFence {
    generation: Arc<AtomicU64>,
    expected_generation: u64,
    cancellation: Arc<AtomicBool>,
    publish_lock: Arc<Mutex<()>>,
}

impl SubscriptionFence {
    fn new(
        generation: Arc<AtomicU64>,
        expected_generation: u64,
        publish_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self::with_cancellation(
            generation,
            expected_generation,
            publish_lock,
            Arc::new(AtomicBool::new(false)),
        )
    }

    fn with_cancellation(
        generation: Arc<AtomicU64>,
        expected_generation: u64,
        publish_lock: Arc<Mutex<()>>,
        cancellation: Arc<AtomicBool>,
    ) -> Self {
        Self {
            generation,
            expected_generation,
            cancellation,
            publish_lock,
        }
    }

    fn is_current(&self) -> bool {
        !self.cancellation.load(Ordering::SeqCst)
            && self.generation.load(Ordering::SeqCst) == self.expected_generation
    }
}

#[derive(Clone)]
struct HerdrLifecycleEvent {
    generation: u64,
    event: HerdrEvent,
}

/// A generation fence is checked while holding the publish lock, so a
/// cancellation boundary either precedes an event entirely or follows it.
fn publish_event(
    fence: &SubscriptionFence,
    event: HerdrEvent,
    event_cursor_tx: &Sender<HerdrEventCursor>,
    lifecycle_tx: &Sender<HerdrLifecycleEvent>,
    event_log: &SharedEventLog,
) -> Result<bool, HerdrClientError> {
    let _publish_guard = match fence.publish_lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if !fence.is_current() {
        return Ok(false);
    }
    let cursor = record_event(event_log, event.clone());
    forward_lifecycle(lifecycle_tx, fence.expected_generation, &event);
    futures::executor::block_on(event_cursor_tx.send(cursor))
        .map_err(|_| HerdrClientError::Disconnected)?;
    Ok(true)
}

type PendingResult = Result<Box<RawValue>, HerdrClientError>;
type PendingRequests = Arc<Mutex<HashMap<String, oneshot::Sender<PendingResult>>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HerdrClientError {
    Io(String),
    Codec(String),
    ProtocolError { code: String, message: String },
    ProtocolMismatch { expected: u64, actual: Option<u64> },
    Disconnected,
    Timeout,
    EndpointNotFound(String),
    Other(String),
}

impl std::fmt::Display for HerdrClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) => write!(f, "I/O error: {message}"),
            Self::Codec(message) => write!(f, "Codec error: {message}"),
            Self::ProtocolError { code, message } => {
                write!(f, "Protocol error ({code}): {message}")
            }
            Self::ProtocolMismatch { expected, actual } => {
                write!(f, "Unsupported Herdr protocol: expected {expected}, got {actual:?}")
            }
            Self::Disconnected => write!(f, "Disconnected"),
            Self::Timeout => write!(f, "Timeout"),
            Self::EndpointNotFound(endpoint) => write!(f, "Endpoint not found: {endpoint}"),
            Self::Other(message) => write!(f, "Other error: {message}"),
        }
    }
}

impl std::error::Error for HerdrClientError {}

impl From<anyhow::Error> for HerdrClientError {
    fn from(error: anyhow::Error) -> Self {
        Self::Other(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HerdrAgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    #[serde(untagged)]
    Unknown(String),
}

impl Default for HerdrAgentStatus {
    fn default() -> Self {
        Self::Unknown("unknown".to_string())
    }
}

fn unknown_status() -> HerdrAgentStatus {
    HerdrAgentStatus::Unknown("unknown".to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct HerdrAgentSessionIdentity {
    #[serde(alias = "type")]
    pub kind: String,
    pub value: String,
}

impl HerdrAgentSessionIdentity {
    pub(crate) fn id(value: impl Into<String>) -> Self {
        Self {
            kind: "id".to_string(),
            value: value.into(),
        }
    }

    pub(crate) fn path(value: impl Into<String>) -> Self {
        Self {
            kind: "path".to_string(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HerdrAgentSnapshot {
    #[serde(default)]
    pub pane_id: String,
    #[serde(default)]
    pub workspace_id: String,
    #[serde(default, alias = "agent")]
    pub agent_type: Option<String>,
    #[serde(default, alias = "agent_session")]
    pub session_identity: Option<HerdrAgentSessionIdentity>,
    #[serde(default = "unknown_status", alias = "agent_status")]
    pub status: HerdrAgentStatus,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub last_seen_sequence: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HerdrPaneSnapshot {
    #[serde(default)]
    pub pane_id: String,
    #[serde(default)]
    pub workspace_id: String,
    #[serde(default)]
    pub tab_id: String,
    #[serde(default)]
    pub focused: bool,
    #[serde(default, alias = "agent_status")]
    pub status: HerdrAgentStatus,
    #[serde(default)]
    pub revision: u64,
    #[serde(default, alias = "agent")]
    pub agent_type: Option<String>,
    #[serde(default, alias = "agent_session")]
    pub session_identity: Option<HerdrAgentSessionIdentity>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HerdrWorkspaceSnapshot {
    #[serde(default)]
    pub workspace_id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub active_pane_id: Option<String>,
    #[serde(default)]
    pub agents: Vec<HerdrAgentSnapshot>,
    #[serde(default)]
    pub focused: bool,
    #[serde(default)]
    pub number: u32,
    #[serde(default)]
    pub pane_count: u32,
    #[serde(default)]
    pub tab_count: u32,
    #[serde(default)]
    pub active_tab_id: Option<String>,
    #[serde(default, alias = "agent_status")]
    pub agent_status: HerdrAgentStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct HerdrSnapshot {
    #[serde(default)]
    pub session: String,
    #[serde(default)]
    pub sequence: u64,
    #[serde(default, alias = "focused_workspace_id")]
    pub active_workspace_id: Option<String>,
    #[serde(default, alias = "focused_tab_id")]
    pub active_tab_id: Option<String>,
    #[serde(default, alias = "focused_pane_id")]
    pub active_pane_id: Option<String>,
    #[serde(default)]
    pub workspaces: Vec<HerdrWorkspaceSnapshot>,
    #[serde(default)]
    pub tabs: Vec<Value>,
    #[serde(default)]
    pub panes: Vec<HerdrPaneSnapshot>,
    #[serde(default)]
    pub layouts: Vec<Value>,
    #[serde(default)]
    pub agents: Vec<HerdrAgentSnapshot>,
    #[serde(default)]
    pub protocol: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct HerdrRequest {
    pub id: String,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct HerdrResponse {
    pub id: String,
    #[serde(default)]
    pub result: Option<Box<RawValue>>,
    #[serde(default)]
    pub error: Option<HerdrErrorBody>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct HerdrErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HerdrEventEnvelope {
    pub event: String,
    pub data: Box<RawValue>,
}

#[derive(Debug, Clone)]
pub(crate) enum HerdrEvent {
    WorkspaceCreated { workspace: HerdrWorkspaceSnapshot, sequence: u64 },
    WorkspaceUpdated { workspace: HerdrWorkspaceSnapshot, sequence: u64 },
    WorkspaceRenamed { workspace_id: String, label: String, sequence: u64 },
    WorkspaceFocused {
        workspace_id: String,
        operation_id: Option<String>,
        sequence: u64,
    },
    WorkspaceClosed { workspace_id: String, sequence: u64 },
    WorkspaceMoved {
        workspace_id: String,
        insert_index: u64,
        workspaces: Vec<HerdrWorkspaceSnapshot>,
        sequence: u64,
    },
    WorkspaceReordered {
        workspace_ids: Vec<String>,
        before_workspace_id: Option<String>,
        workspaces: Vec<HerdrWorkspaceSnapshot>,
        sequence: u64,
    },
    PaneCreated { pane: HerdrPaneSnapshot, sequence: u64 },
    PaneUpdated { pane: HerdrPaneSnapshot, sequence: u64 },
    PaneMoved {
        pane: HerdrPaneSnapshot,
        previous_pane_id: Option<String>,
        previous_workspace_id: Option<String>,
        previous_tab_id: Option<String>,
        sequence: u64,
    },
    PaneAgentDetected {
        pane_id: String,
        workspace_id: String,
        agent_type: Option<String>,
        session_identity: Option<HerdrAgentSessionIdentity>,
        sequence: u64,
    },
    PaneAgentStatusChanged { pane_id: String, status: HerdrAgentStatus, sequence: u64 },
    PaneFocused {
        pane_id: String,
        workspace_id: String,
        operation_id: Option<String>,
        sequence: u64,
    },
    PaneClosed { pane_id: String, workspace_id: String, sequence: u64 },
    PaneExited { pane_id: String, exit_code: Option<i32>, sequence: u64 },
    PaneOutput { pane_id: String, revision: u64, delta: String, sequence: u64 },
    PaneScrollChanged { pane_id: String, sequence: u64 },
    SubscriptionStarted { subscription_id: String },
    Unknown { event: String, data: Box<RawValue> },
}

/// Event-log position paired with the event delivered to the bridge. The
/// cursor lets bootstrap replay partition the shared stream without applying
/// the same event again after discovery completes.
#[derive(Default)]
struct HerdrEventLog {
    base_index: usize,
    events: Vec<HerdrEvent>,
    replay_boundary: Option<usize>,
}
type SharedEventLog = Arc<Mutex<HerdrEventLog>>;


#[derive(Clone, Debug)]
pub(crate) struct HerdrEventCursor {
    pub(crate) index: usize,
    pub(crate) event: HerdrEvent,
}
static FOCUS_OPERATION_ORIGINS: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn focus_operation_origins() -> &'static Mutex<HashMap<String, String>> {
    &FOCUS_OPERATION_ORIGINS
}

fn remember_focus_origin(operation_id: Option<&str>, origin: Option<&str>) {
    let (Some(operation_id), Some(origin)) = (operation_id, origin) else {
        return;
    };
    if let Ok(mut origins) = focus_operation_origins().lock() {
        origins.insert(operation_id.to_string(), origin.to_string());
    }
}

impl HerdrEvent {
    pub(crate) fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::WorkspaceCreated { workspace, .. } | Self::WorkspaceUpdated { workspace, .. } => {
                Some(&workspace.workspace_id)
            }
            Self::WorkspaceRenamed { workspace_id, .. }
            | Self::WorkspaceFocused { workspace_id, .. }
            | Self::WorkspaceClosed { workspace_id, .. }
            | Self::WorkspaceMoved { workspace_id, .. }
            | Self::PaneAgentDetected { workspace_id, .. }
            | Self::PaneFocused { workspace_id, .. }
            | Self::PaneClosed { workspace_id, .. } => Some(workspace_id),
            Self::PaneCreated { pane, .. }
            | Self::PaneUpdated { pane, .. }
            | Self::PaneMoved { pane, .. } => Some(&pane.workspace_id),
            _ => None,
        }
    }
    pub(crate) fn operation_origin(&self) -> Option<String> {
        let operation_id = match self {
            Self::WorkspaceFocused { operation_id, .. }
            | Self::PaneFocused { operation_id, .. } => operation_id.as_deref(),
            _ => None,
        }?;
        focus_operation_origins()
            .lock()
            .ok()
            .and_then(|origins| origins.get(operation_id).cloned())
    }

    pub(crate) fn pane_id(&self) -> Option<&str> {
        match self {
            Self::PaneAgentDetected { pane_id, .. }
            | Self::PaneAgentStatusChanged { pane_id, .. }
            | Self::PaneFocused { pane_id, .. }
            | Self::PaneClosed { pane_id, .. }
            | Self::PaneExited { pane_id, .. }
            | Self::PaneOutput { pane_id, .. }
            | Self::PaneScrollChanged { pane_id, .. } => Some(pane_id),
            Self::PaneCreated { pane, .. }
            | Self::PaneUpdated { pane, .. }
            | Self::PaneMoved { pane, .. } => Some(&pane.pane_id),
            _ => None,
        }
    }

    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::WorkspaceCreated { sequence, .. }
            | Self::WorkspaceUpdated { sequence, .. }
            | Self::WorkspaceRenamed { sequence, .. }
            | Self::WorkspaceFocused { sequence, .. }
            | Self::WorkspaceClosed { sequence, .. }
            | Self::WorkspaceMoved { sequence, .. }
            | Self::WorkspaceReordered { sequence, .. }
            | Self::PaneCreated { sequence, .. }
            | Self::PaneUpdated { sequence, .. }
            | Self::PaneMoved { sequence, .. }
            | Self::PaneAgentDetected { sequence, .. }
            | Self::PaneAgentStatusChanged { sequence, .. }
            | Self::PaneFocused { sequence, .. }
            | Self::PaneClosed { sequence, .. }
            | Self::PaneExited { sequence, .. }
            | Self::PaneOutput { sequence, .. }
            | Self::PaneScrollChanged { sequence, .. } => *sequence,
            Self::SubscriptionStarted { .. } | Self::Unknown { .. } => 0,
        }
    }
}

fn value_from_raw(raw: &RawValue) -> Result<Value, HerdrClientError> {
    serde_json::from_str(raw.get()).map_err(|error| HerdrClientError::Codec(error.to_string()))
}

fn required_string(value: &Value, field: &str) -> Result<String, HerdrClientError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| HerdrClientError::Codec(format!("missing string field {field}")))
}

fn sequence_of(value: &Value) -> u64 {
    value
        .get("sequence")
        .or_else(|| value.get("state_change_seq"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn decode_workspace_list(
    data: &Value,
) -> Result<Vec<HerdrWorkspaceSnapshot>, HerdrClientError> {
    let empty = Vec::new();
    let workspaces = data
        .get("workspaces")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    workspaces
        .iter()
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| HerdrClientError::Codec(error.to_string()))
        })
        .collect()
}

pub(crate) fn decode_response(input: &str) -> Result<HerdrResponse, HerdrClientError> {
    serde_json::from_str(input).map_err(|error| HerdrClientError::Codec(error.to_string()))
}

pub(crate) fn decode_snapshot_result(input: &str) -> Result<HerdrSnapshot, HerdrClientError> {
    let value: Value = serde_json::from_str(input)
        .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
    let snapshot = match value.get("type").and_then(Value::as_str) {
        Some("session_snapshot") => value
            .get("snapshot")
            .cloned()
            .ok_or_else(|| HerdrClientError::Codec("session_snapshot missing snapshot".to_string()))?,
        Some(other) => {
            return Err(HerdrClientError::Codec(format!(
                "expected session_snapshot result, got {other}"
            )));
        }
        None => value,
    };
    serde_json::from_value(snapshot).map_err(|error| HerdrClientError::Codec(error.to_string()))
}
pub(crate) fn decode_workspace_result(input: &str) -> Result<HerdrWorkspaceSnapshot, HerdrClientError> {
    let value: Value = serde_json::from_str(input)
        .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
    let workspace = match value.get("type").and_then(Value::as_str) {
        Some("workspace_created") | Some("workspace_info") => value
            .get("workspace")
            .cloned()
            .ok_or_else(|| HerdrClientError::Codec("workspace result missing workspace".to_string()))?,
        Some(other) => {
            return Err(HerdrClientError::Codec(format!(
                "unexpected workspace result type {other}"
            )));
        }
        None => value,
    };
    serde_json::from_value(workspace).map_err(|error| HerdrClientError::Codec(error.to_string()))
}

pub(crate) fn decode_pane_read_result(input: &str) -> Result<(u64, String), HerdrClientError> {
    let value: Value = serde_json::from_str(input)
        .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
    let read = if value.get("type").and_then(Value::as_str) == Some("pane_read") {
        value
            .get("read")
            .cloned()
            .ok_or_else(|| HerdrClientError::Codec("pane_read result missing read".to_string()))?
    } else {
        value
    };
    let revision = read
        .get("revision")
        .or_else(|| read.get("read").and_then(|nested| nested.get("revision")))
        .and_then(Value::as_u64)
        .ok_or_else(|| HerdrClientError::Codec("pane_read result missing revision".to_string()))?;
    let text = read
        .get("text")
        .or_else(|| read.get("read").and_then(|nested| nested.get("text")))
        .and_then(Value::as_str)
        .ok_or_else(|| HerdrClientError::Codec("pane_read result missing text".to_string()))?;
    Ok((revision, text.to_string()))
}

pub(crate) fn validate_ping_result(input: &str) -> Result<(), HerdrClientError> {
    let value: Value = serde_json::from_str(input)
        .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
    let protocol = value.get("protocol").and_then(Value::as_u64);
    if value.get("type").and_then(Value::as_str) != Some("pong") || protocol != Some(HERDR_PROTOCOL) {
        return Err(HerdrClientError::ProtocolMismatch {
            expected: HERDR_PROTOCOL,
            actual: protocol,
        });
    }
    Ok(())
}

pub(crate) fn decode_event(input: &str) -> Result<HerdrEvent, HerdrClientError> {
    let envelope: HerdrEventEnvelope = serde_json::from_str(input)
        .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
    let data = value_from_raw(&envelope.data)?;
    let event_type = data.get("type").and_then(Value::as_str).unwrap_or(&envelope.event);
    let sequence = sequence_of(&data);

    match event_type {
        "workspace_created" | "workspace.created" => {
            let workspace = data
                .get("workspace")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("workspace_created missing workspace".to_string()))?;
            let workspace = serde_json::from_value(workspace)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::WorkspaceCreated { workspace, sequence })
        }
        "workspace_updated" | "workspace.updated" | "workspace_metadata_updated" => {
            let workspace = data
                .get("workspace")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("workspace_updated missing workspace".to_string()))?;
            let workspace = serde_json::from_value(workspace)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::WorkspaceUpdated { workspace, sequence })
        }
        "workspace_renamed" | "workspace.renamed" => Ok(HerdrEvent::WorkspaceRenamed {
            workspace_id: required_string(&data, "workspace_id")?,
            label: required_string(&data, "label")?,
            sequence,
        }),
        "workspace_focused" | "workspace.focused" => {
            let workspace_id = required_string(&data, "workspace_id")?;
            let operation_id = data
                .get("operation_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let origin = data
                .get("origin")
                .or_else(|| data.get("operation_origin"))
                .and_then(Value::as_str);
            remember_focus_origin(operation_id.as_deref(), origin);
            Ok(HerdrEvent::WorkspaceFocused {
                workspace_id,
                operation_id,
                sequence,
            })
        }
        "workspace_closed" | "workspace.closed" => Ok(HerdrEvent::WorkspaceClosed {
            workspace_id: required_string(&data, "workspace_id")?,
            sequence,
        }),
        "workspace_moved" | "workspace.moved" => Ok(HerdrEvent::WorkspaceMoved {
            workspace_id: required_string(&data, "workspace_id")?,
            insert_index: data
                .get("insert_index")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    HerdrClientError::Codec("workspace_moved missing insert_index".to_string())
                })?,
            workspaces: decode_workspace_list(&data)?,
            sequence,
        }),
        "workspace_reordered" | "workspace.reordered" => Ok(HerdrEvent::WorkspaceReordered {
            workspace_ids: data
                .get("workspace_ids")
                .and_then(Value::as_array)
                .map(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            before_workspace_id: data
                .get("before_workspace_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            workspaces: decode_workspace_list(&data)?,
            sequence,
        }),
        "pane_created" | "pane.created" => {
            let pane = data
                .get("pane")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("pane_created missing pane".to_string()))?;
            let pane = serde_json::from_value(pane)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::PaneCreated { pane, sequence })
        }
        "pane_updated" | "pane.updated" => {
            let pane = data
                .get("pane")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("pane_updated missing pane".to_string()))?;
            let pane = serde_json::from_value(pane)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::PaneUpdated { pane, sequence })
        }
        "pane_moved" | "pane.moved" => {
            let pane = data
                .get("pane")
                .cloned()
                .unwrap_or_else(|| data.clone());
            let pane = serde_json::from_value(pane)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::PaneMoved {
                pane,
                previous_pane_id: data
                    .get("previous_pane_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                previous_workspace_id: data
                    .get("previous_workspace_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                previous_tab_id: data
                    .get("previous_tab_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                sequence,
            })
        }
        "pane_focused" | "pane.focused" => {
            let pane_id = required_string(&data, "pane_id")?;
            let workspace_id = required_string(&data, "workspace_id")?;
            let operation_id = data
                .get("operation_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let origin = data
                .get("origin")
                .or_else(|| data.get("operation_origin"))
                .and_then(Value::as_str);
            remember_focus_origin(operation_id.as_deref(), origin);
            Ok(HerdrEvent::PaneFocused {
                pane_id,
                workspace_id,
                operation_id,
                sequence,
            })
        }
        "pane_closed" | "pane.closed" => Ok(HerdrEvent::PaneClosed {
            pane_id: required_string(&data, "pane_id")?,
            workspace_id: required_string(&data, "workspace_id")?,
            sequence,
        }),
        "pane_exited" | "pane.exited" => Ok(HerdrEvent::PaneExited {
            pane_id: required_string(&data, "pane_id")?,
            exit_code: data.get("exit_code").and_then(Value::as_i64).map(|code| code as i32),
            sequence,
        }),
        "pane_agent_detected" | "pane.agent_detected" | "agent.detected" => {
            let session_identity = data
                .get("agent_session")
                .or_else(|| data.get("session_identity"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
            Ok(HerdrEvent::PaneAgentDetected {
                pane_id: required_string(&data, "pane_id")?,
                workspace_id: required_string(&data, "workspace_id")?,
                agent_type: data
                    .get("agent")
                    .or_else(|| data.get("agent_type"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                session_identity,
                sequence,
            })
        }
        "pane_agent_status_changed" | "pane.agent_status_changed" => {
            let status_value = data
                .get("agent_status")
                .or_else(|| data.get("status"))
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("status event missing agent_status".to_string()))?;
            let status = serde_json::from_value(status_value)
                .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
            Ok(HerdrEvent::PaneAgentStatusChanged {
                pane_id: required_string(&data, "pane_id")?,
                status,
                sequence,
            })
        }
        "pane_output_changed" | "pane.output_changed" | "pane_output" | "pane.output"
        | "pane.output_matched" => Ok(HerdrEvent::PaneOutput {
            pane_id: required_string(&data, "pane_id")?,
            revision: data
                .get("revision")
                .or_else(|| data.get("read").and_then(|read| read.get("revision")))
                .and_then(Value::as_u64)
                .unwrap_or(0),
            delta: data
                .get("text")
                .or_else(|| data.get("delta"))
                .or_else(|| data.get("output"))
                .or_else(|| data.get("read").and_then(|read| read.get("text")))
                .or_else(|| data.get("matched_line"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            sequence,
        }),
        "pane_scroll_changed" | "pane.scroll_changed" => Ok(HerdrEvent::PaneScrollChanged {
            pane_id: required_string(&data, "pane_id")?,
            sequence,
        }),
        "subscription_started" | "subscription.started" => Ok(HerdrEvent::SubscriptionStarted {
            subscription_id: data
                .get("subscription_id")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string(),
        }),
        _ => Ok(HerdrEvent::Unknown {
            event: envelope.event,
            data: envelope.data,
        }),
    }
}

fn fail_pending(pending: &PendingRequests, error: HerdrClientError) {
    let mut requests = match pending.lock() {
        Ok(requests) => requests,
        Err(poisoned) => poisoned.into_inner(),
    };
    for (_, sender) in requests.drain() {
        if sender.send(Err(error.clone())).is_err() {
            log::debug!("Herdr request waiter was already dropped while failing connection");
        }
    }
}
fn response_result(response: HerdrResponse) -> PendingResult {
    match (response.result, response.error) {
        (_, Some(error)) => Err(HerdrClientError::ProtocolError {
            code: error.code,
            message: error.message,
        }),
        (Some(result), None) => Ok(result),
        (None, None) => Err(HerdrClientError::Codec(
            "success response missing result".to_string(),
        )),
    }
}

fn prune_event_log(log: &mut HerdrEventLog) {
    if log.replay_boundary.is_some() || log.events.len() <= MAX_EVENT_LOG {
        return;
    }
    let remove = log.events.len() - MAX_EVENT_LOG;
    log.events.drain(..remove);
    log.base_index += remove;
}

fn record_event(
    event_log: &SharedEventLog,
    event: HerdrEvent,
) -> HerdrEventCursor {
    match event_log.lock() {
        Ok(mut log) => {
            let index = log.base_index + log.events.len();
            log.events.push(event.clone());
            prune_event_log(&mut log);
            HerdrEventCursor { index, event }
        }
        Err(poisoned) => {
            let mut log = poisoned.into_inner();
            let index = log.base_index + log.events.len();
            log.events.push(event.clone());
            prune_event_log(&mut log);
            HerdrEventCursor { index, event }
        }
    }
}

fn event_log_len(event_log: &SharedEventLog) -> usize {
    match event_log.lock() {
        Ok(log) => log.events.len(),
        Err(poisoned) => poisoned.into_inner().events.len(),
    }
}

fn event_log_end(event_log: &SharedEventLog) -> usize {
    match event_log.lock() {
        Ok(log) => log.base_index + log.events.len(),
        Err(poisoned) => {
            let log = poisoned.into_inner();
            log.base_index + log.events.len()
        }
    }
}

fn mark_event_log_replay_boundary(event_log: &SharedEventLog, boundary: usize) {
    let mut log = match event_log.lock() {
        Ok(log) => log,
        Err(poisoned) => poisoned.into_inner(),
    };
    let remove = boundary.saturating_sub(log.base_index).min(log.events.len());
    if remove > 0 {
        log.events.drain(..remove);
        log.base_index += remove;
    }
    log.replay_boundary = Some(boundary.max(log.base_index));
}

fn finish_event_log_replay(event_log: &SharedEventLog, replay_until: usize) {
    let mut log = match event_log.lock() {
        Ok(log) => log,
        Err(poisoned) => poisoned.into_inner(),
    };
    let remove = replay_until
        .saturating_sub(log.base_index)
        .min(log.events.len());
    if remove > 0 {
        log.events.drain(..remove);
        log.base_index += remove;
    }
    log.replay_boundary = None;
    prune_event_log(&mut log);
}

fn events_since(
    event_log: &SharedEventLog,
    start: usize,
) -> Vec<HerdrEvent> {
    events_since_with_boundary(event_log, start).0
}

fn events_since_with_boundary(
    event_log: &SharedEventLog,
    start: usize,
) -> (Vec<HerdrEvent>, usize) {
    match event_log.lock() {
        Ok(log) => {
            let offset = start.saturating_sub(log.base_index).min(log.events.len());
            (
                log.events[offset..].to_vec(),
                log.base_index + log.events.len(),
            )
        }
        Err(poisoned) => {
            let log = poisoned.into_inner();
            let offset = start.saturating_sub(log.base_index).min(log.events.len());
            (
                log.events[offset..].to_vec(),
                log.base_index + log.events.len(),
            )
        }
    }
}
fn bootstrap_primary_subscription_ended(
    events: &[HerdrEvent],
    subscription_id: &str,
) -> bool {
    events.iter().any(|event| {
        let HerdrEvent::Unknown { event, data } = event else {
            return false;
        };
        if event != "subscription_ended" {
            return false;
        }
        serde_json::from_str::<Value>(data.get())
            .ok()
            .and_then(|value| {
                value
                    .get("subscription_id")
                    .and_then(Value::as_str)
                    .map(|id| id == subscription_id)
            })
            .unwrap_or(false)
    })
}

fn bootstrap_subscription_ended(events: &[HerdrEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            HerdrEvent::Unknown { event, .. } if event == "subscription_ended"
        )
    })
}

/// Keeps cancellation active while an established subscription is blocked in
/// a read. A one-shot Windows `CancelIoEx` can miss a read that starts just
/// after the cancellation call; repeated triggers close that race while the
/// generation fence rejects any late publisher.
struct SubscriptionKillGuard {
    stop_tx: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl SubscriptionKillGuard {
    fn new(kill: ConnectionKillSwitch, fence: SubscriptionFence) -> Self {
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("herdr-subscription-cancel".to_string())
            .spawn(move || {
                while fence.is_current() {
                    match stop_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            return;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    }
                }
                loop {
                    kill.trigger();
                    match stop_rx.recv_timeout(Duration::from_millis(25)) {
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    }
                }
            })
            .ok();
        Self {
            stop_tx: Some(stop_tx),
            worker,
        }
    }
}

impl Drop for SubscriptionKillGuard {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}


#[derive(Clone)]
pub(crate) struct HerdrClientHandle {
    endpoint: HerdrEndpoint,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    event_tx: Sender<HerdrEvent>,
    event_rx: Receiver<HerdrEvent>,
    event_cursor_tx: Sender<HerdrEventCursor>,
    event_cursor_rx: Receiver<HerdrEventCursor>,
    event_log: SharedEventLog,
    lifecycle_tx: Sender<HerdrLifecycleEvent>,
    lifecycle_rx: Receiver<HerdrLifecycleEvent>,
    watched_panes: WatchedPanes,
    subscription_kills: Arc<Mutex<Vec<ConnectionKillSwitch>>>,
    subscription_generation: Arc<AtomicU64>,
    publish_lock: Arc<Mutex<()>>,
    supervisor_started: Arc<AtomicBool>,
    executor: BackgroundExecutor,
}

impl HerdrClientHandle {
    pub(crate) fn new(endpoint: HerdrEndpoint, cx: &App) -> Result<Self, HerdrClientError> {
        Ok(Self::new_with_executor(
            endpoint,
            cx.background_executor().clone(),
        ))
    }

    pub(crate) fn new_with_executor(endpoint: HerdrEndpoint, executor: BackgroundExecutor) -> Self {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = async_channel::unbounded();
        let (event_cursor_tx, event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, lifecycle_rx) = async_channel::unbounded();
        Self {
            endpoint,
            next_id: Arc::new(AtomicU64::new(1)),
            pending,
            event_tx,
            event_rx,
            event_cursor_tx,
            event_cursor_rx,
            event_log: Arc::new(Mutex::new(HerdrEventLog::default())),
            lifecycle_tx,
            lifecycle_rx,
            watched_panes: Arc::new(Mutex::new(HashMap::new())),
            subscription_kills: Arc::new(Mutex::new(Vec::new())),
            subscription_generation: Arc::new(AtomicU64::new(0)),
            publish_lock: Arc::new(Mutex::new(())),
            supervisor_started: Arc::new(AtomicBool::new(false)),
            executor,
        }
    }

    pub(crate) fn connect(endpoint: &HerdrEndpoint, cx: &App) -> Task<Result<Self, HerdrClientError>> {
        let endpoint = endpoint.clone();
        let executor = cx.background_executor().clone();
        executor
            .clone()
            .spawn(async move { Ok(Self::new_with_executor(endpoint, executor)) })
    }

    pub(crate) fn request(
        &self,
        method: &str,
        params: Option<Value>,
        _cx: &App,
    ) -> Task<PendingResult> {
        self.request_on_executor(method, params.unwrap_or_else(|| serde_json::Map::new().into()))
    }

    /// Herdr accepts one initial request per connection. Open a fresh request
    /// connection for every RPC and keep the subscription connection separate.
    /// Blocking connect/send/read runs on a dedicated thread under a hard
    /// deadline (socket timeouts on Unix, an I/O-cancelling watchdog on
    /// Windows), so a server that accepts and never responds resolves the
    /// caller's task with `Timeout` instead of hanging forever.
    pub(crate) fn request_on_executor(&self, method: &str, params: Value) -> Task<PendingResult> {
        self.request_on_executor_with_deadline(method, params, REQUEST_TIMEOUT)
    }

    fn request_on_executor_with_deadline(
        &self,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Task<PendingResult> {
        let request_id = format!("req-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let (sender, receiver) = oneshot::channel();
        match self.pending.lock() {
            Ok(mut pending) => {
                pending.insert(request_id.clone(), sender);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(request_id.clone(), sender);
            }
        }

        let request = HerdrRequest {
            id: request_id.clone(),
            method: method.to_string(),
            params,
        };
        let endpoint = self.endpoint.clone();
        let pending = self.pending.clone();
        let event_tx = self.event_tx.clone();
        let event_cursor_tx = self.event_cursor_tx.clone();
        let lifecycle_tx = self.lifecycle_tx.clone();
        let event_log = self.event_log.clone();
        let generation = self.subscription_generation.load(Ordering::SeqCst);
        let fence = SubscriptionFence::new(
            self.subscription_generation.clone(),
            generation,
            self.publish_lock.clone(),
        );
        let spawned = std::thread::Builder::new()
            .name("herdr-request".to_string())
            .spawn(move || {
                let _ = run_request_once(
                    endpoint,
                    request,
                    deadline,
                    pending,
                    event_tx,
                    event_cursor_tx,
                    lifecycle_tx,
                    event_log,
                    fence,
                );
            });
        if spawned.is_err() {
            log::error!("Failed to spawn Herdr request thread");
            remove_pending(&self.pending, &request_id);
        }
        self.executor.clone().spawn(async move {
            receiver
                .await
                .unwrap_or_else(|_| Err(HerdrClientError::Disconnected))
        })
    }

    pub(crate) fn subscribe(&self) -> Receiver<HerdrEvent> {
        self.event_rx.clone()
    }

    pub(crate) fn subscribe_with_cursor(&self) -> Receiver<HerdrEventCursor> {
        self.event_cursor_rx.clone()
    }

    /// Start the long-lived subscription connection. Resolves once
    /// `subscription_started` is acknowledged; pushed events then flow through
    /// the shared cursor channel until the connection terminates.
    pub(crate) fn start_subscription(
        &self,
        params: Value,
        retain_kill_switch: bool,
    ) -> Task<Result<(String, usize, ConnectionKillSwitch, u64), HerdrClientError>> {
        self.start_subscription_with_cancellation(
            params,
            retain_kill_switch,
            Arc::new(AtomicBool::new(false)),
        )
    }

    fn start_subscription_with_cancellation(
        &self,
        params: Value,
        retain_kill_switch: bool,
        cancellation: Arc<AtomicBool>,
    ) -> Task<Result<(String, usize, ConnectionKillSwitch, u64), HerdrClientError>> {
        let generation = self.subscription_generation.load(Ordering::SeqCst);
        self.start_subscription_with_cancellation_at_generation(
            params,
            retain_kill_switch,
            cancellation,
            generation,
        )
    }

    fn start_subscription_with_cancellation_at_generation(
        &self,
        params: Value,
        retain_kill_switch: bool,
        cancellation: Arc<AtomicBool>,
        generation: u64,
    ) -> Task<Result<(String, usize, ConnectionKillSwitch, u64), HerdrClientError>> {
        let request_id = format!("req-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let request = HerdrRequest {
            id: request_id.clone(),
            method: "events.subscribe".to_string(),
            params,
        };
        let fence = SubscriptionFence::with_cancellation(
            self.subscription_generation.clone(),
            generation,
            self.publish_lock.clone(),
            cancellation,
        );
        let endpoint = self.endpoint.clone();
        let event_tx = self.event_tx.clone();
        let event_cursor_tx = self.event_cursor_tx.clone();
        let lifecycle_tx = self.lifecycle_tx.clone();
        let event_log = self.event_log.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<
            Result<(String, usize, ConnectionKillSwitch), HerdrClientError>,
        >();
        let spawned = std::thread::Builder::new()
            .name("herdr-subscription".to_string())
            .spawn(move || {
                run_subscription_connection(
                    endpoint,
                    request,
                    REQUEST_TIMEOUT,
                    ready_tx,
                    event_tx,
                    event_cursor_tx,
                    lifecycle_tx,
                    event_log,
                    fence,
                );
            });
        if let Err(error) = spawned {
            log::error!("Failed to spawn Herdr subscription thread: {error}");
        }
        let registrar = self.clone();
        self.executor.clone().spawn(async move {
            let result = ready_rx
                .await
                .unwrap_or_else(|_| Err(HerdrClientError::Disconnected));
            let (subscription_id, boundary, kill) = result?;
            registrar.accept_subscription_kill(generation, &kill, retain_kill_switch)?;
            Ok((subscription_id, boundary, kill, generation))
        })
    }
    fn cancel_subscription_generation(&self) {
        let generation_guard = match self.publish_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.subscription_generation.fetch_add(1, Ordering::SeqCst);
        let kills = match self.subscription_kills.lock() {
            Ok(mut kills) => std::mem::take(&mut *kills),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        let watches = {
            let mut watched = watched_lock(&self.watched_panes);
            std::mem::take(&mut *watched)
        };
        drop(generation_guard);

        for kill in kills {
            kill.trigger();
        }
        for (_, watch) in watches {
            watch.watcher_cancel.store(true, Ordering::SeqCst);
            if let Some(kill) = watch.filter_kill {
                kill.trigger();
            }
        }

        // Lifecycle events already queued belong to the retired generation;
        // the next snapshot is authoritative for pane discovery.
        while self.lifecycle_rx.try_recv().is_ok() {}
    }

    /// Accept a subscription handshake only for the generation that created
    /// it. The kill-list mutex is also the registration/cancellation fence:
    /// either registration wins and cancellation drains it, or cancellation
    /// wins and the late connection is killed immediately.
    fn accept_subscription_kill(
        &self,
        generation: u64,
        kill: &ConnectionKillSwitch,
        retain: bool,
    ) -> Result<(), HerdrClientError> {
        let mut kills = match self.subscription_kills.lock() {
            Ok(kills) => kills,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.subscription_generation.load(Ordering::SeqCst) != generation {
            drop(kills);
            kill.trigger();
            return Err(HerdrClientError::Disconnected);
        }
        if retain {
            kills.push(kill.clone());
        }
        Ok(())
    }
    /// Track a pane created mid-session: per-pane filters plus a continuous
    /// output watcher. No-op if the pane is already watched.
    fn watch_pane_at_generation(&self, pane_id: String, expected_generation: u64) {
        let Some((cancel, generation)) =
            self.ensure_watched_at_generation(&pane_id, Some(expected_generation))
        else {
            return;
        };

        let filter_cancel = cancel.clone();
        let filters = self.start_subscription_with_cancellation_at_generation(
            pane_filter_subscription_params(&[pane_id.clone()]),
            false,
            filter_cancel.clone(),
            expected_generation,
        );
        let registrar = self.clone();
        let filter_pane_id = pane_id.clone();
        self.executor
            .clone()
            .spawn(async move {
                match filters.await {
                    Ok((_, _, kill, filter_generation)) => {
                        registrar.store_filter_kill_switch_at_generation(
                            &filter_pane_id,
                            filter_generation,
                            filter_cancel,
                            kill,
                        );
                    }
                    Err(error) => {
                        log::error!(
                            "Herdr per-pane subscription failed for {filter_pane_id}: {error}"
                        );
                    }
                }
            })
            .detach();
        self.spawn_output_watcher(pane_id, cancel, generation);
    }

    /// Begin output watching for a pane whose per-pane filters were already
    /// registered by the bootstrap subscription batch.
    fn track_pane_output(&self, pane_id: String) {
        let Some((cancel, generation)) = self.ensure_watched_with_generation(&pane_id) else {
            return;
        };
        self.spawn_output_watcher(pane_id, cancel, generation);
    }

    fn ensure_watched(&self, pane_id: &str) -> Option<Arc<AtomicBool>> {
        self.ensure_watched_with_generation(pane_id)
            .map(|(cancel, _generation)| cancel)
    }

    fn ensure_watched_with_generation(
        &self,
        pane_id: &str,
    ) -> Option<(Arc<AtomicBool>, u64)> {
        self.ensure_watched_at_generation(pane_id, None)
    }

    fn ensure_watched_at_generation(
        &self,
        pane_id: &str,
        expected_generation: Option<u64>,
    ) -> Option<(Arc<AtomicBool>, u64)> {
        let generation_guard = match self.publish_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let generation = self.subscription_generation.load(Ordering::SeqCst);
        if expected_generation.is_some_and(|expected| expected != generation) {
            drop(generation_guard);
            return None;
        }
        let mut watched = watched_lock(&self.watched_panes);
        if watched.contains_key(pane_id) {
            drop(generation_guard);
            return None;
        }
        let cancel = Arc::new(AtomicBool::new(false));
        watched.insert(
            pane_id.to_string(),
            PaneWatch {
                generation,
                watcher_cancel: cancel.clone(),
                filter_kill: None,
            },
        );
        drop(generation_guard);
        Some((cancel, generation))
    }


    /// Record a pane filter connection's kill switch once its handshake
    /// completes. If the pane was retired before the handshake finished,
    /// tear the connection down immediately instead of registering an
    /// orphaned subscription loop. Pane ids can be reused: a late
    /// handshake from a previous generation must trigger the live switch
    /// it replaces, or that connection would leak and its pump would keep
    /// running against a pane id it no longer owns.
    fn store_filter_kill_switch(&self, pane_id: &str, kill: ConnectionKillSwitch) {
        let generation = self.subscription_generation.load(Ordering::SeqCst);
        let cancellation = watched_lock(&self.watched_panes)
            .get(pane_id)
            .map(|watch| watch.watcher_cancel.clone())
            .unwrap_or_else(|| Arc::new(AtomicBool::new(true)));
        self.store_filter_kill_switch_at_generation(pane_id, generation, cancellation, kill);
    }

    fn store_filter_kill_switch_at_generation(
        &self,
        pane_id: &str,
        generation: u64,
        cancellation: Arc<AtomicBool>,
        kill: ConnectionKillSwitch,
    ) {
        let generation_guard = match self.publish_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.subscription_generation.load(Ordering::SeqCst) != generation
            || cancellation.load(Ordering::SeqCst)
        {
            drop(generation_guard);
            kill.trigger();
            return;
        }
        let mut watched = watched_lock(&self.watched_panes);
        match watched.get_mut(pane_id) {
            Some(watch)
                if watch.generation == generation
                    && Arc::ptr_eq(&watch.watcher_cancel, &cancellation) =>
            {
                if let Some(previous) = watch.filter_kill.replace(kill) {
                    log::debug!("Replacing stale Herdr filter subscription for {pane_id}");
                    previous.trigger();
                }
            }
            Some(_) | None => drop(kill.trigger()),
        }
        drop(generation_guard);
    }
    /// Retire a pane only when the lifecycle event still belongs to the
    /// current generation. The generation check is fenced by the same lock
    /// used by cancellation, so a late old event cannot remove or replace a
    /// new-generation watch.
    fn retire_pane_at_generation(&self, pane_id: &str, expected_generation: u64) {
        let generation_guard = match self.publish_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.subscription_generation.load(Ordering::SeqCst) != expected_generation {
            drop(generation_guard);
            return;
        }
        self.retire_pane(pane_id);
        drop(generation_guard);
    }

    /// Retire a pane: cancel its output watcher and terminate its dedicated
    /// filter-subscription connection so neither loop outlives the pane.
    /// Covers closed panes, exited agents, and stale moved-away pane ids.
    fn retire_pane(&self, pane_id: &str) {
        let retired = watched_lock(&self.watched_panes).remove(pane_id);
        if let Some(PaneWatch {
            watcher_cancel,
            filter_kill,
            ..
        }) = retired
        {
            watcher_cancel.store(true, Ordering::SeqCst);
            if let Some(kill) = filter_kill {
                log::debug!("Tearing down Herdr filter subscription for {pane_id}");
                kill.trigger();
            }
        }
    }

    /// Follow a pane's output continuously. Herdr matcher subscriptions only
    /// fire on non-matching -> matching transitions, so instead repeatedly
    /// block in `events.wait` for `pane_output_changed` and fetch the changed
    /// buffer with a revision-aware `pane.read`.
    fn spawn_output_watcher(
        &self,
        pane_id: String,
        cancel: Arc<AtomicBool>,
        generation: u64,
    ) {
        let client = self.clone();
        let event_cursor_tx = self.event_cursor_tx.clone();
        let fence = SubscriptionFence::with_cancellation(
            self.subscription_generation.clone(),
            generation,
            self.publish_lock.clone(),
            cancel.clone(),
        );
        self.executor
            .clone()
            .spawn(async move {
                let mut last_revision: u64 = 0;
                let retry_delay = Duration::from_millis(100);
                loop {
                    if !fence.is_current() {
                        return;
                    }
                    let wait = client
                        .request_on_executor_with_deadline(
                            "events.wait",
                            events_wait_params(&pane_id, last_revision.saturating_add(1)),
                            events_wait_deadline(),
                        )
                        .await;
                    if !fence.is_current() {
                        return;
                    }
                    let notified_revision = match wait {
                        Err(HerdrClientError::Timeout) => continue,
                        Err(HerdrClientError::ProtocolError { code, message }) => {
                            if code.contains("timeout") || code.contains("timed_out") {
                                continue;
                            }
                            log::warn!(
                                "Herdr output watcher retrying for {pane_id}: protocol error ({code}): {message}"
                            );
                            if !fence.is_current() {
                                return;
                            }
                            client.executor.clone().timer(retry_delay).await;
                            continue;
                        }
                        Err(error) => {
                            log::warn!(
                                "Herdr output watcher retrying for {pane_id}: {error}"
                            );
                            if !fence.is_current() {
                                return;
                            }
                            client.executor.clone().timer(retry_delay).await;
                            continue;
                        }
                        Ok(result) => match wait_matched_revision(&result) {
                            Some(revision) => revision,
                            None => {
                                log::warn!(
                                    "Herdr output watcher retrying for {pane_id}: unexpected events.wait result"
                                );
                                if !fence.is_current() {
                                    return;
                                }
                                client.executor.clone().timer(retry_delay).await;
                                continue;
                            }
                        },
                    };
                    if notified_revision <= last_revision {
                        continue;
                    }
                    let read = client
                        .request_on_executor(
                            "pane.read",
                            serde_json::json!({
                                "pane_id": pane_id,
                                "source": "recent",
                                "format": "text",
                                "strip_ansi": true
                            }),
                        )
                        .await;
                    if !fence.is_current() {
                        return;
                    }
                    match read
                        .as_deref()
                        .map_err(Clone::clone)
                        .and_then(|result| decode_pane_read_result(result.get()))
                    {
                        Ok((revision, text)) => {
                            if revision > last_revision {
                                last_revision = revision;
                                let event = HerdrEvent::PaneOutput {
                                    pane_id: pane_id.clone(),
                                    revision,
                                    delta: text,
                                    sequence: 0,
                                };
                                if !publish_event(
                                    &fence,
                                    event,
                                    &event_cursor_tx,
                                    &client.lifecycle_tx,
                                    &client.event_log,
                                )
                                .unwrap_or(false)
                                {
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            log::warn!(
                                "Herdr output watcher retrying for {pane_id}: pane.read failed: {error}"
                            );
                            if !fence.is_current() {
                                return;
                            }
                            client.executor.clone().timer(retry_delay).await;
                        }
                    }
                }
            })
            .detach();
    }

    /// Apply a lifecycle event only when it belongs to the current
    /// subscription generation. The generation-aware watch/retire methods
    /// repeat the check under the publish lock to fence cancellation races
    /// between receive and action.
    fn handle_lifecycle_event(&self, lifecycle: HerdrLifecycleEvent) {
        if self.subscription_generation.load(Ordering::SeqCst) != lifecycle.generation {
            return;
        }
        match lifecycle.event {
            HerdrEvent::PaneCreated { pane, .. } => {
                self.watch_pane_at_generation(pane.pane_id, lifecycle.generation);
            }
            HerdrEvent::PaneMoved {
                pane,
                previous_pane_id,
                ..
            } => {
                if let Some(previous) = previous_pane_id {
                    if previous != pane.pane_id {
                        self.retire_pane_at_generation(&previous, lifecycle.generation);
                    }
                }
                self.watch_pane_at_generation(pane.pane_id, lifecycle.generation);
            }
            HerdrEvent::PaneClosed { pane_id, .. } | HerdrEvent::PaneExited { pane_id, .. } => {
                self.retire_pane_at_generation(&pane_id, lifecycle.generation);
            }
            _ => {}
        }
    }

    /// Maintain per-pane watches as lifecycle events create, move, or close
    /// panes so filtered subscriptions never cover only the bootstrap set.
    fn start_watch_supervisor(&self) {
        if self.supervisor_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let lifecycle_rx = self.lifecycle_rx.clone();
        let client = self.clone();
        self.executor
            .clone()
            .spawn(async move {
                while let Ok(event) = lifecycle_rx.recv().await {
                    client.handle_lifecycle_event(event);
                }
            })
            .detach();
    }

    pub(crate) fn bootstrap_on_executor(
        &self,
    ) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        let client = self.clone();
        let event_log = self.event_log.clone();
        let executor = self.executor.clone();
        executor.clone().spawn(async move {
            client.cancel_subscription_generation();
            let result = async {
                let ping = client.request_on_executor("ping", empty_params()).await?;
                validate_ping_result(ping.get())?;

                // Subscribe before pane discovery. A pane.created event that
                // arrives while either snapshot is in flight is then captured
                // by the primary stream instead of falling into a gap.
                let (subscription_id, boundary, _, _) = client
                    .start_subscription(subscription_params(), true)
                    .await?;
                // Events before this boundary are superseded by the snapshot.
                // Retain the new-generation window until replay is consumed.
                mark_event_log_replay_boundary(&event_log, boundary);
                let mut subscription_ids = vec![subscription_id.clone()];

                // First snapshot is only used to learn pane IDs so every
                // per-pane filter can register its baseline before the
                // authoritative state is captured.
                let initial = decode_snapshot_result(
                    client
                        .request_on_executor("session.snapshot", empty_params())
                        .await?
                        .get(),
                )?;
                let pane_ids: Vec<String> = initial
                    .panes
                    .iter()
                    .map(|pane| pane.pane_id.clone())
                    .filter(|pane_id| !pane_id.is_empty())
                    .collect();

                if !pane_ids.is_empty() {
                    let (filter_subscription_id, _, _, _) = client
                        .start_subscription(
                            pane_filter_subscription_params(&pane_ids),
                            true,
                        )
                        .await?;
                    subscription_ids.push(filter_subscription_id);
                    for pane_id in &pane_ids {
                        client.track_pane_output(pane_id.clone());
                    }
                }
                client.start_watch_supervisor();

                let snapshot = decode_snapshot_result(
                    client
                        .request_on_executor("session.snapshot", empty_params())
                        .await?
                        .get(),
                )?;
                let (events, replay_until) =
                    events_since_with_boundary(&event_log, boundary);
                if bootstrap_subscription_ended(&events) {
                    finish_event_log_replay(&event_log, replay_until);
                    return Err(HerdrClientError::Disconnected);
                }
                finish_event_log_replay(&event_log, replay_until);
                Ok(HerdrBootstrap {
                    snapshot,
                    subscription_id,
                    subscription_ids,
                    events,
                    replay_until,
                })
            }
            .await;
            if result.is_err() {
                finish_event_log_replay(&event_log, event_log_end(&event_log));
                client.cancel_subscription_generation();
            }
            result
        })
    }
}
type WatchedPanes = Arc<Mutex<HashMap<String, PaneWatch>>>;

/// Per-pane watch ownership: the output watcher's cancellation flag plus the
/// kill switch for the pane's dedicated filter-subscription connection, so
/// retiring the pane terminates both loops.
struct PaneWatch {
    generation: u64,
    watcher_cancel: Arc<AtomicBool>,
    filter_kill: Option<ConnectionKillSwitch>,
}

fn watched_lock(watched: &WatchedPanes) -> std::sync::MutexGuard<'_, HashMap<String, PaneWatch>> {
    match watched.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Map transport failures to concrete client errors; socket deadlines surface
/// as `Timeout` rather than a raw I/O wrapper.
fn read_error_to_client_error(error: anyhow::Error) -> HerdrClientError {
    for cause in error.chain() {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            if matches!(
                io_error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) {
                return HerdrClientError::Timeout;
            }
            // A Windows deadline watchdog cancelled the in-flight pipe read
            // (ERROR_OPERATION_ABORTED); surface it as the request timeout.
            if io_error.raw_os_error() == Some(995) {
                return HerdrClientError::Timeout;
            }
        }
    }
    HerdrClientError::Io(error.to_string())
}

fn forward_lifecycle(
    lifecycle_tx: &Sender<HerdrLifecycleEvent>,
    generation: u64,
    event: &HerdrEvent,
) {
    if matches!(
        event,
        HerdrEvent::PaneCreated { .. }
            | HerdrEvent::PaneMoved { .. }
            | HerdrEvent::PaneClosed { .. }
            | HerdrEvent::PaneExited { .. }
    ) {
        let _ = lifecycle_tx.try_send(HerdrLifecycleEvent {
            generation,
            event: event.clone(),
        });
    }
}

/// Extract the revision from an official `wait_matched` result:
/// `{type:"wait_matched", event:{event:"pane.output_changed", data:{revision}}}`.
fn wait_matched_revision(result: &RawValue) -> Option<u64> {
    let value: Value = serde_json::from_str(result.get()).ok()?;
    if value.get("type").and_then(Value::as_str) != Some("wait_matched") {
        return None;
    }
    let data = value.get("event")?.get("data")?;
    if data.get("type").and_then(Value::as_str) != Some("pane_output_changed") {
        return None;
    }
    data.get("revision").and_then(Value::as_u64)
}

#[allow(clippy::too_many_arguments)]
fn run_request_once(
    endpoint: HerdrEndpoint,
    request: HerdrRequest,
    deadline: Duration,
    pending: PendingRequests,
    _event_tx: Sender<HerdrEvent>,
    event_cursor_tx: Sender<HerdrEventCursor>,
    lifecycle_tx: Sender<HerdrLifecycleEvent>,
    event_log: SharedEventLog,
    fence: SubscriptionFence,
) -> PendingResult {
    let request_id = request.id.clone();
    let attempt = (|| -> PendingResult {
        let mut stream = HerdrStream::connect_with_deadline(&endpoint, deadline)
            .map_err(|error| HerdrClientError::EndpointNotFound(error.to_string()))?;
        // Bounded-request watchdog: on Windows a silent pipe peer would
        // otherwise block this worker forever; the watchdog cancels the
        // in-flight read at the deadline (a no-op on Unix, whose socket
        // timeouts already bound every operation). Dropped with `attempt`.
        let _io_deadline = stream.arm_io_deadline(deadline).ok();
        let encoded = serde_json::to_string(&request)
            .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
        stream
            .send_line(&encoded)
            .map_err(|error| HerdrClientError::Io(error.to_string()))?;

        let mut reader = HerdrLineReader::new(stream);
        loop {
            let line = match reader.read_line() {
                Ok(Some(line)) => line,
                Ok(None) => break Err(HerdrClientError::Disconnected),
                Err(error) => break Err(read_error_to_client_error(error)),
            };
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(error) => break Err(HerdrClientError::Codec(error.to_string())),
            };
            if value.get("id").is_some() {
                let response = match decode_response(&line) {
                    Ok(response) => response,
                    Err(error) => break Err(error),
                };
                if response.id != request_id {
                    continue;
                }
                break response_result(response);
            }
            if value.get("event").is_some() {
                let event = match decode_event(&line) {
                    Ok(event) => event,
                    Err(error) => break Err(error),
                };
                match publish_event(
                    &fence,
                    event,
                    &event_cursor_tx,
                    &lifecycle_tx,
                    &event_log,
                ) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => break Err(error),
                }
            } else {
                break Err(HerdrClientError::Codec(
                    "frame is neither response nor event".to_string(),
                ));
            }
        }
    })();

    if let Some(sender) = take_pending(&pending, &request_id) {
        if sender.send(attempt.clone()).is_err() {
            log::debug!("Herdr response waiter was already dropped");
        }
    }
    attempt
}

#[allow(clippy::too_many_arguments)]
fn run_subscription_connection(
    endpoint: HerdrEndpoint,
    request: HerdrRequest,
    handshake_deadline: Duration,
    ready_tx: oneshot::Sender<Result<(String, usize, ConnectionKillSwitch), HerdrClientError>>,
    _event_tx: Sender<HerdrEvent>,
    event_cursor_tx: Sender<HerdrEventCursor>,
    lifecycle_tx: Sender<HerdrLifecycleEvent>,
    event_log: SharedEventLog,
    fence: SubscriptionFence,
) {
    if !fence.is_current() {
        let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
        return;
    }
    let mut stream = match HerdrStream::connect_with_deadline(&endpoint, handshake_deadline) {
        Ok(stream) => stream,
        Err(error) => {
            let _ = ready_tx.send(Err(HerdrClientError::EndpointNotFound(error.to_string())));
            return;
        }
    };
    // The kill switch lets the pane owner terminate this connection later;
    // the handshake watchdog bounds a silent peer during setup. The
    // generation guard keeps cancellation active after a Windows read starts.
    let kill_switch = match stream.kill_switch() {
        Ok(kill) => kill,
        Err(error) => {
            let _ = ready_tx.send(Err(HerdrClientError::Io(error.to_string())));
            return;
        }
    };
    let _cancellation_guard = SubscriptionKillGuard::new(kill_switch.clone(), fence.clone());
    let handshake_watchdog = stream.arm_io_deadline(handshake_deadline).ok();
    if !fence.is_current() {
        let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
        return;
    }
    let encoded = match serde_json::to_string(&request) {
        Ok(encoded) => encoded,
        Err(error) => {
            let _ = ready_tx.send(Err(HerdrClientError::Codec(error.to_string())));
            return;
        }
    };
    if let Err(error) = stream.send_line(&encoded) {
        let _ = ready_tx.send(Err(HerdrClientError::Io(error.to_string())));
        return;
    }

    let mut reader = HerdrLineReader::new(stream);
    let subscription_id: String;
    loop {
        if !fence.is_current() {
            let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
            return;
        }
        let line = match reader.read_line() {
            Ok(Some(line)) => line,
            Ok(None) => {
                let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
                return;
            }
            Err(error) => {
                let _ = ready_tx.send(Err(read_error_to_client_error(error)));
                return;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                let _ = ready_tx.send(Err(HerdrClientError::Codec(error.to_string())));
                return;
            }
        };
        if value.get("id").is_none() {
            // Pushed events may arrive before the acknowledgement; buffer them.
            if value.get("event").is_some() {
                match decode_event(&line) {
                    Ok(event) => match publish_event(
                        &fence,
                        event,
                        &event_cursor_tx,
                        &lifecycle_tx,
                        &event_log,
                    ) {
                        Ok(true) => {}
                        Ok(false) => {
                            let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
                            return;
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return;
                        }
                    },
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                }
            }
            continue;
        }
        let response = match decode_response(&line) {
            Ok(response) => response,
            Err(error) => {
                let _ = ready_tx.send(Err(error));
                return;
            }
        };
        if response.id != request.id {
            continue;
        }
        let result = match response_result(response) {
            Ok(result) => result,
            Err(error) => {
                let _ = ready_tx.send(Err(error));
                return;
            }
        };
        let value = match value_from_raw(&result) {
            Ok(value) => value,
            Err(error) => {
                let _ = ready_tx.send(Err(error));
                return;
            }
        };
        if value.get("type").and_then(Value::as_str) != Some("subscription_started") {
            let _ = ready_tx.send(Err(HerdrClientError::Codec(
                "events.subscribe did not return subscription_started".to_string(),
            )));
            return;
        }
        subscription_id = value
            .get("subscription_id")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let boundary = event_log_end(&event_log);
        if !fence.is_current() {
            let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
            return;
        }
        if ready_tx.send(Ok((subscription_id.clone(), boundary, kill_switch))).is_err() {
            return;
        }
        break;
    }

    if let Some(watchdog) = handshake_watchdog {
        watchdog.disarm();
    }
    // Established: idle pushes must not be cut off by the handshake deadline.

    if let Err(error) = reader.set_read_timeout(None) {
        log::error!("Herdr subscription could not clear its read deadline: {error}");
        return;
    }
    if let Err(error) = pump_subscription_events(
        &mut reader,
        &event_cursor_tx,
        &lifecycle_tx,
        &event_log,
        &fence,
    ) {
        if fence.is_current() {
            log::error!("Herdr subscription terminated: {error}");
            let data = serde_json::from_str::<Box<RawValue>>(
                &serde_json::json!({
                    "subscription_id": subscription_id,
                    "error": error.to_string()
                })
                .to_string(),
            )
            .expect("subscription-ended payload is valid JSON");
            let event = HerdrEvent::Unknown {
                event: "subscription_ended".to_string(),
                data,
            };
            let _ = publish_event(
                &fence,
                event,
                &event_cursor_tx,
                &lifecycle_tx,
                &event_log,
            );
        }
    }
}

fn pump_subscription_events(
    reader: &mut HerdrLineReader,
    event_cursor_tx: &Sender<HerdrEventCursor>,
    lifecycle_tx: &Sender<HerdrLifecycleEvent>,
    event_log: &SharedEventLog,
    fence: &SubscriptionFence,
) -> Result<(), HerdrClientError> {
    loop {
        if !fence.is_current() {
            return Err(HerdrClientError::Disconnected);
        }
        let line = match reader.read_line() {
            Ok(Some(line)) => line,
            Ok(None) => return Err(HerdrClientError::Disconnected),
            Err(error) => return Err(read_error_to_client_error(error)),
        };
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .map_err(|error| HerdrClientError::Codec(error.to_string()))?;
        if value.get("event").is_none() {
            return Err(HerdrClientError::Codec(format!(
                "subscription frame has no event field: {value}"
            )));
        }
        let event = decode_event(&line)?;
        match publish_event(fence, event, event_cursor_tx, lifecycle_tx, event_log) {
            Ok(true) => {}
            Ok(false) => return Err(HerdrClientError::Disconnected),
            Err(error) => return Err(error),
        }
    }
}
fn take_pending(
    pending: &PendingRequests,
    request_id: &str,
) -> Option<oneshot::Sender<PendingResult>> {
    match pending.lock() {
        Ok(mut pending) => pending.remove(request_id),
        Err(poisoned) => poisoned.into_inner().remove(request_id),
    }
}
fn remove_pending(pending: &PendingRequests, request_id: &str) {
    match pending.lock() {
        Ok(mut pending) => {
            pending.remove(request_id);
        }
        Err(poisoned) => {
            poisoned.into_inner().remove(request_id);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HerdrBootstrap {
    pub snapshot: HerdrSnapshot,
    pub subscription_id: String,
    pub subscription_ids: Vec<String>,
    pub events: Vec<HerdrEvent>,
    /// Exclusive event-log index covered by `events`. Cursors below this
    /// boundary are either replayed here or superseded by the snapshot.
    pub replay_until: usize,
}
pub(crate) trait HerdrApi: Send + Sync {
    fn ping(&self, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn subscribe_events(&self, cx: &App) -> Task<Result<String, HerdrClientError>>;
    fn bootstrap(&self, cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>>;
    fn cancel_subscriptions(&self) {}
    fn get_snapshot(&self, cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>>;
    fn focus_workspace(
        &self,
        workspace_id: &str,
        operation_id: Option<&str>,
        origin: Option<&str>,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>>;
    fn create_workspace(&self, label: &str, paths: Vec<String>, cx: &App) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>>;
    fn rename_workspace(&self, workspace_id: &str, label: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn close_workspace(&self, workspace_id: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn focus_pane(
        &self,
        pane_id: &str,
        operation_id: Option<&str>,
        origin: Option<&str>,
        cx: &App,
    ) -> Task<Result<(), HerdrClientError>>;
    fn close_pane(&self, pane_id: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn prompt_agent(&self, pane_id: &str, prompt: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn send_agent_keys(&self, pane_id: &str, keys: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn send_pane_keys(&self, pane_id: &str, keys: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn send_pane_text(&self, pane_id: &str, text: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn send_pane_input(&self, pane_id: &str, text: Option<&str>, keys: Vec<String>, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn split_pane(&self, pane_id: &str, direction: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn rename_agent(&self, pane_id: &str, name: Option<&str>, cx: &App) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>>;
    fn start_agent(&self, pane_id: &str, kind: &str, name: &str, args: Vec<String>, cx: &App) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>>;
    fn read_pane_output(&self, pane_id: &str, since_revision: Option<u64>, cx: &App) -> Task<Result<(u64, String), HerdrClientError>>;
}

pub(crate) fn empty_params() -> Value {
    serde_json::Map::new().into()
}
pub(crate) fn subscription_params() -> Value {
    serde_json::json!({
        "subscriptions": [
            {"type": "workspace.created"},
            {"type": "workspace.updated"},
            {"type": "workspace.metadata_updated"},
            {"type": "workspace.renamed"},
            {"type": "workspace.moved"},
            {"type": "workspace.reordered"},
            {"type": "workspace.closed"},
            {"type": "workspace.focused"},
            {"type": "pane.created"},
            {"type": "pane.updated"},
            {"type": "pane.closed"},
            {"type": "pane.focused"},
            {"type": "pane.moved"},
            {"type": "pane.exited"},
            {"type": "pane.agent_detected"}
        ]
    })
}

fn pane_filter_subscription_params(pane_ids: &[String]) -> Value {
    // Herdr's pane.output_matched matcher only emits non-matching ->
    // matching transitions, so it cannot carry continuous output updates.
    // Status and scroll changes subscribe per pane here; output changes are
    // followed with repeated `events.wait` plus revision-aware `pane.read`.
    let mut subscriptions = Vec::with_capacity(pane_ids.len() * 2);
    for pane_id in pane_ids {
        subscriptions.push(serde_json::json!({
            "type": "pane.agent_status_changed",
            "pane_id": pane_id
        }));
        subscriptions.push(serde_json::json!({
            "type": "pane.scroll_changed",
            "pane_id": pane_id
        }));
    }
    serde_json::json!({"subscriptions": subscriptions})
}

/// Server-side hold time of a matched `events.wait` long poll.
const OUTPUT_WAIT_TIMEOUT_MS: u64 = 15_000;

/// Request deadline for `events.wait`: the server holds the request open for
/// `OUTPUT_WAIT_TIMEOUT_MS`, so the client deadline must exceed that wait by
/// this bounded margin. A shorter deadline would abandon live server waiters
/// every cycle; an unbounded one would leak the request worker.
fn events_wait_deadline() -> Duration {
    Duration::from_millis(OUTPUT_WAIT_TIMEOUT_MS + EVENTS_WAIT_DEADLINE_MARGIN_MS)
}

fn events_wait_params(pane_id: &str, min_revision: u64) -> Value {
    serde_json::json!({
        "match_event": {
            "event": "pane_output_changed",
            "pane_id": pane_id,
            "min_revision": min_revision
        },
        "timeout_ms": OUTPUT_WAIT_TIMEOUT_MS
    })
}

fn pane_input_params(pane_id: &str, text: Option<&str>, keys: &[String]) -> Value {
    let mut params = serde_json::json!({"pane_id": pane_id, "keys": keys});
    if let Some(text) = text {
        params["text"] = Value::String(text.to_string());
    }
    params
}
fn agent_prompt_params(target: &str, text: &str) -> Value {
    serde_json::json!({"target": target, "text": text})
}

fn agent_keys_params(target: &str, keys: &[String]) -> Value {
    serde_json::json!({"target": target, "keys": keys})
}
fn focus_workspace_params(
    workspace_id: &str,
    operation_id: Option<&str>,
    origin: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({"workspace_id": workspace_id});
    if let Some(operation_id) = operation_id {
        params["operation_id"] = Value::String(operation_id.to_string());
    }
    if let Some(origin) = origin {
        params["origin"] = Value::String(origin.to_string());
    }
    params
}

fn focus_pane_params(
    pane_id: &str,
    operation_id: Option<&str>,
    origin: Option<&str>,
) -> Value {
    let mut params = serde_json::json!({"pane_id": pane_id});
    if let Some(operation_id) = operation_id {
        params["operation_id"] = Value::String(operation_id.to_string());
    }
    if let Some(origin) = origin {
        params["origin"] = Value::String(origin.to_string());
    }
    params
}

fn pane_target_params(pane_id: &str) -> Value {
    serde_json::json!({"pane_id": pane_id})
}

fn workspace_create_params(label: &str, paths: &[String]) -> Value {
    serde_json::json!({
        "cwd": paths.first().cloned(),
        "label": label,
        "focus": false,
        "env": {}
    })
}

impl HerdrApi for HerdrClientHandle {
    fn ping(&self, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("ping", empty_params());
        let executor = self.executor.clone();
        executor.spawn(async move {
            let result = task.await?;
            validate_ping_result(result.get())
        })
    }

    fn subscribe_events(&self, _cx: &App) -> Task<Result<String, HerdrClientError>> {
        let task = self.start_subscription(subscription_params(), true);
        self.executor
            .clone()
            .spawn(async move { task.await.map(|(subscription_id, _, _, _)| subscription_id) })
    }

    fn bootstrap(&self, _cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        self.bootstrap_on_executor()
    }

    fn cancel_subscriptions(&self) {
        self.cancel_subscription_generation();
    }

    fn get_snapshot(&self, _cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>> {
        let task = self.request_on_executor("session.snapshot", empty_params());
        let executor = self.executor.clone();
        executor.spawn(async move {
            let result = task.await?;
            decode_snapshot_result(result.get())
        })
    }

    fn focus_workspace(
        &self,
        workspace_id: &str,
        operation_id: Option<&str>,
        origin: Option<&str>,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor(
            "workspace.focus",
            focus_workspace_params(workspace_id, operation_id, origin),
        );
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn create_workspace(&self, label: &str, paths: Vec<String>, _cx: &App) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>> {
        let task = self.request_on_executor("workspace.create", workspace_create_params(label, &paths));
        let executor = self.executor.clone();
        executor.spawn(async move { decode_workspace_result(task.await?.get()) })
    }

    fn rename_workspace(&self, workspace_id: &str, label: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("workspace.rename", serde_json::json!({"workspace_id": workspace_id, "label": label}));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn close_workspace(&self, workspace_id: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("workspace.close", serde_json::json!({"workspace_id": workspace_id}));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn focus_pane(
        &self,
        pane_id: &str,
        operation_id: Option<&str>,
        origin: Option<&str>,
        _cx: &App,
    ) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor(
            "pane.focus",
            focus_pane_params(pane_id, operation_id, origin),
        );
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn close_pane(&self, pane_id: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("pane.close", pane_target_params(pane_id));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn prompt_agent(&self, pane_id: &str, prompt: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("agent.prompt", agent_prompt_params(pane_id, prompt));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn send_agent_keys(&self, pane_id: &str, keys: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("agent.send_keys", agent_keys_params(pane_id, &[keys.to_string()]));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn send_pane_keys(&self, pane_id: &str, keys: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("pane.send_keys", serde_json::json!({"pane_id": pane_id, "keys": [keys]}));
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn send_pane_text(&self, pane_id: &str, text: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor(
            "pane.send_text",
            serde_json::json!({"pane_id": pane_id, "text": text}),
        );
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }
    fn send_pane_input(&self, pane_id: &str, text: Option<&str>, keys: Vec<String>, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor(
            "pane.send_input",
            pane_input_params(pane_id, text, &keys),
        );
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn split_pane(&self, pane_id: &str, direction: &str, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor(
            "pane.split",
            serde_json::json!({"target_pane_id": pane_id, "direction": direction}),
        );
        let executor = self.executor.clone();
        executor.spawn(async move { task.await.map(|_| ()) })
    }

    fn rename_agent(&self, pane_id: &str, name: Option<&str>, _cx: &App) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        let task = self.request_on_executor(
            "agent.rename",
            serde_json::json!({"target": pane_id, "name": name}),
        );
        let executor = self.executor.clone();
        executor.spawn(async move {
            let value = value_from_raw(&task.await?)?;
            let agent = value
                .get("agent")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("agent.rename missing agent".to_string()))?;
            serde_json::from_value(agent).map_err(|error| HerdrClientError::Codec(error.to_string()))
        })
    }

    fn start_agent(&self, pane_id: &str, kind: &str, name: &str, args: Vec<String>, _cx: &App) -> Task<Result<HerdrAgentSnapshot, HerdrClientError>> {
        let task = self.request_on_executor(
            "agent.start",
            serde_json::json!({"pane_id": pane_id, "kind": kind, "name": name, "args": args}),
        );
        let executor = self.executor.clone();
        executor.spawn(async move {
            let value = value_from_raw(&task.await?)?;
            let agent = value
                .get("agent")
                .cloned()
                .ok_or_else(|| HerdrClientError::Codec("agent.start missing agent".to_string()))?;
            serde_json::from_value(agent).map_err(|error| HerdrClientError::Codec(error.to_string()))
        })
    }

    fn read_pane_output(&self, pane_id: &str, _since_revision: Option<u64>, _cx: &App) -> Task<Result<(u64, String), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "source": "recent",
            "format": "text",
            "strip_ansi": true,
            "lines": Value::Null
        });
        let task = self.request_on_executor("pane.read", params);
        let executor = self.executor.clone();
        executor.spawn(async move { decode_pane_read_result(task.await?.get()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    fn test_fence() -> SubscriptionFence {
        let generation = Arc::new(AtomicU64::new(0));
        SubscriptionFence::new(generation, 0, Arc::new(Mutex::new(())))
    }
    #[test]
    fn decodes_success_response_by_request_id() {
        let response = decode_response(r#"{"id":"req-1","result":{"type":"pong"}}"#)
            .expect("valid response");
        assert_eq!(response.id, "req-1");
        assert!(response.error.is_none());
    }

    #[test]
    fn decodes_workspace_focused_subscription_event() {
        let event = decode_event(
            r#"{"event":"workspace.focused","data":{"type":"workspace_focused","workspace_id":"w1"}}"#,
        )
        .expect("valid event");
        assert_eq!(event.workspace_id(), Some("w1"));
    }

    #[test]
    fn rejects_malformed_json_frame() {
        assert!(decode_response("not-json").is_err());
    }

    #[test]
    fn encodes_official_subscription_payload() {
        let payload = subscription_params();
        assert_eq!(payload["subscriptions"][0], serde_json::json!({"type": "workspace.created"}));
        assert_eq!(payload["subscriptions"].as_array().map(Vec::len), Some(15));
        assert_eq!(payload["subscriptions"][12]["type"], "pane.moved");
        assert_eq!(payload["subscriptions"][13]["type"], "pane.exited");
        assert_eq!(payload["subscriptions"][14]["type"], "pane.agent_detected");

        let filters = pane_filter_subscription_params(&["w1:p1".to_string()]);
        let encoded = serde_json::to_string(&filters).expect("filters encode");
        // The empty-substring output_matched filter is banned: Herdr matcher
        // subscriptions only fire on non-matching -> matching transitions.
        assert!(!encoded.contains("output_matched"));
        assert_eq!(
            filters["subscriptions"].as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(
            filters["subscriptions"][0],
            serde_json::json!({"type": "pane.agent_status_changed", "pane_id": "w1:p1"})
        );
        assert_eq!(
            filters["subscriptions"][1],
            serde_json::json!({"type": "pane.scroll_changed", "pane_id": "w1:p1"})
        );
        assert!(payload.get("events").is_none());
    }
    #[test]
    fn encodes_focus_operation_id_and_origin_payload() {
        assert_eq!(
            focus_workspace_params("w1", Some("op-1"), Some("zed")),
            serde_json::json!({
                "workspace_id": "w1",
                "operation_id": "op-1",
                "origin": "zed"
            })
        );
        assert_eq!(
            focus_pane_params("p1", Some("op-2"), Some("zed")),
            serde_json::json!({
                "pane_id": "p1",
                "operation_id": "op-2",
                "origin": "zed"
            })
        );
    }

    #[test]
    fn decodes_focus_operation_origin_for_reflection_matching() {
        let event = decode_event(
            r#"{"event":"workspace.focused","data":{"type":"workspace_focused","workspace_id":"w1","operation_id":"op-origin","origin":"zed"}}"#,
        )
        .expect("focus event");
        assert_eq!(event.operation_origin().as_deref(), Some("zed"));
    }

    #[test]
    fn decodes_tagged_snapshot_and_pane_read_result() {
        let snapshot = decode_snapshot_result(
            r#"{"type":"session_snapshot","snapshot":{"protocol":20,"focused_workspace_id":"w1","workspaces":[{"workspace_id":"w1","number":0,"label":"Repo","focused":true,"pane_count":1,"tab_count":1,"active_tab_id":"t1","agent_status":"idle"}]}}"#,
        )
        .expect("snapshot");
        assert_eq!(snapshot.active_workspace_id.as_deref(), Some("w1"));
        assert_eq!(snapshot.workspaces[0].workspace_id, "w1");

        let pane = decode_pane_read_result(
            r#"{"type":"pane_read","read":{"pane_id":"p1","workspace_id":"w1","tab_id":"t1","source":"recent","format":"text","text":"output","revision":4,"truncated":false}}"#,
        )
        .expect("pane read");
        assert_eq!(pane, (4, "output".to_string()));
    }

    #[test]
    fn replays_buffered_events_in_arrival_order_without_sequences() {
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let start = event_log_len(&event_log);
        record_event(
            &event_log,
            HerdrEvent::WorkspaceFocused {
                workspace_id: "w1".to_string(),
                operation_id: None,
                sequence: 0,
            },
        );
        record_event(
            &event_log,
            HerdrEvent::PaneScrollChanged {
                pane_id: "w1:p1".to_string(),
                sequence: 0,
            },
        );
        let events = events_since(&event_log, start);
        assert!(matches!(events.as_slice(), [
            HerdrEvent::WorkspaceFocused { workspace_id, .. },
            HerdrEvent::PaneScrollChanged { pane_id, .. },
        ] if workspace_id == "w1" && pane_id == "w1:p1"));
    }

    #[test]
    fn bootstrap_event_boundary_excludes_pre_subscription_events() {
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let before = record_event(
            &event_log,
            HerdrEvent::WorkspaceFocused {
                workspace_id: "old".to_string(),
                operation_id: None,
                sequence: 1,
            },
        );
        let boundary = event_log_len(&event_log);
        let replay = record_event(
            &event_log,
            HerdrEvent::WorkspaceFocused {
                workspace_id: "new".to_string(),
                operation_id: None,
                sequence: 2,
            },
        );
        let (events, replay_until) = events_since_with_boundary(&event_log, boundary);
        assert_eq!(before.index, 0);
        assert_eq!(replay.index, boundary);
        assert_eq!(replay_until, 2);
        assert!(matches!(
            events.as_slice(),
            [HerdrEvent::WorkspaceFocused { workspace_id, .. }] if workspace_id == "new"
        ));
    }
    #[test]
    fn event_log_is_bounded_and_consumed_replay_keeps_absolute_cursors() {
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        for sequence in 0..(MAX_EVENT_LOG + 8) {
            record_event(
                &event_log,
                HerdrEvent::WorkspaceFocused {
                    workspace_id: format!("w{sequence}"),
                    operation_id: None,
                    sequence: sequence as u64,
                },
            );
        }
        assert_eq!(event_log_len(&event_log), MAX_EVENT_LOG);

        let boundary = event_log_end(&event_log);
        mark_event_log_replay_boundary(&event_log, boundary);
        let cursor = record_event(
            &event_log,
            HerdrEvent::PaneScrollChanged {
                pane_id: "w1:p1".to_string(),
                sequence: 0,
            },
        );
        let (events, replay_until) = events_since_with_boundary(&event_log, boundary);
        assert_eq!(cursor.index, boundary);
        assert_eq!(replay_until, boundary + 1);
        assert!(matches!(
            events.as_slice(),
            [HerdrEvent::PaneScrollChanged { pane_id, .. }] if pane_id == "w1:p1"
        ));

        finish_event_log_replay(&event_log, replay_until);
        assert_eq!(event_log_len(&event_log), 0);
        let next = record_event(
            &event_log,
            HerdrEvent::PaneScrollChanged {
                pane_id: "w1:p2".to_string(),
                sequence: 0,
            },
        );
        assert_eq!(
            next.index, replay_until,
            "pruning must not reset cursor indices for the live stream"
        );
    }


    #[test]
    fn cancelling_subscription_generation_retires_output_watchers() {
        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle = HerdrClientHandle::new_with_executor(
            HerdrEndpoint::Default,
            executor,
        );
        let cancel = handle
            .ensure_watched("w1:p1")
            .expect("watch should be registered");
        handle.cancel_subscription_generation();
        assert!(cancel.load(Ordering::SeqCst));
        assert!(watched_lock(&handle.watched_panes).is_empty());
    }
    /// Review 3 finding 1: a bootstrap bulk filter subscription keeps its
    /// kill switch in the generation owner and late handshakes are cancelled.
    #[cfg(unix)]
    #[test]
    fn bootstrap_bulk_filter_kill_is_retained_and_cancelled() {
        use std::io::{BufRead, BufReader};
        use std::os::unix::net::UnixStream;
        use std::thread;

        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle = HerdrClientHandle::new_with_executor(
            HerdrEndpoint::Default,
            executor,
        );
        let generation = handle.subscription_generation.load(Ordering::SeqCst);

        let (server, client) = UnixStream::pair().expect("socket pair");
        let kill = HerdrStream::Unix(client.try_clone().expect("clone socket"))
            .kill_switch()
            .expect("kill switch");
        let waiter = thread::spawn(move || {
            let mut reader = BufReader::new(server);
            let mut line = String::new();
            reader.read_line(&mut line)
        });
        assert!(
            handle
                .accept_subscription_kill(generation, &kill, true)
                .is_ok()
        );
        assert_eq!(
            handle
                .subscription_kills
                .lock()
                .expect("kill list")
                .len(),
            1
        );

        handle.cancel_subscription_generation();
        assert!(
            handle
                .subscription_kills
                .lock()
                .expect("kill list")
                .is_empty()
        );
        assert_eq!(
            waiter.join().expect("filter waiter").expect("filter read"),
            0,
            "generation cancellation must close the bulk filter stream"
        );
        drop(client);

        let (late_server, late_client) = UnixStream::pair().expect("late socket pair");
        let late_kill = HerdrStream::Unix(late_client.try_clone().expect("clone late socket"))
            .kill_switch()
            .expect("late kill switch");
        let late_waiter = thread::spawn(move || {
            let mut reader = BufReader::new(late_server);
            let mut line = String::new();
            reader.read_line(&mut line)
        });
        assert!(matches!(
            handle.accept_subscription_kill(generation, &late_kill, true),
            Err(HerdrClientError::Disconnected)
        ));
        assert_eq!(
            late_waiter
                .join()
                .expect("late filter waiter")
                .expect("late filter read"),
            0
        );
        drop(late_client);
    }


    #[test]
    fn encodes_target_based_control_payloads() {
        assert_eq!(agent_prompt_params("p1", "hello"), serde_json::json!({"target":"p1","text":"hello"}));
        assert_eq!(agent_keys_params("p1", &["enter".to_string(), "ctrl+c".to_string()]), serde_json::json!({"target":"p1","keys":["enter","ctrl+c"]}));
    }

    #[test]
    fn encodes_workspace_create_payload() {
        assert_eq!(workspace_create_params("Repo", &["/repo".to_string()]), serde_json::json!({"cwd":"/repo","label":"Repo","focus":false,"env":{}}));
    }

    #[test]
    fn validates_ping_protocol() {
        assert!(validate_ping_result(r#"{"type":"pong","version":"0.20.0","protocol":20}"#).is_ok());
        assert!(validate_ping_result(r#"{"type":"pong","version":"0.19.0","protocol":19}"#).is_err());
    }

    #[test]
    fn decodes_protocol_20_pane_status_and_output_events() {
        let status = decode_event(r#"{"event":"pane.agent_status_changed","data":{"type":"pane_agent_status_changed","pane_id":"p1","workspace_id":"w1","agent_status":"working"}}"#).expect("status event");
        assert!(matches!(status, HerdrEvent::PaneAgentStatusChanged { status: HerdrAgentStatus::Working, .. }));
        let output = decode_event(r#"{"event":"pane.output_matched","data":{"type":"pane.output_matched","pane_id":"p1","revision":9,"text":"hello"}}"#).expect("output event");
        assert!(matches!(output, HerdrEvent::PaneOutput { revision: 9, delta, .. } if delta == "hello"));
    }
    
    #[test]
    fn decodes_typed_pane_moved_event() {
        let event = decode_event(
            r#"{"event":"pane.moved","data":{"type":"pane_moved","previous_pane_id":"w1:p1","previous_workspace_id":"w1","previous_tab_id":"w1:t1","pane":{"pane_id":"w2:p2","workspace_id":"w2","tab_id":"w2:t1"}}}"#,
        )
        .expect("pane moved event");
        assert!(matches!(
            event,
            HerdrEvent::PaneMoved {
                pane,
                previous_pane_id: Some(previous),
                ..
            } if pane.pane_id == "w2:p2" && previous == "w1:p1"
        ));
    }

    #[test]
    fn omits_absent_pane_input_text() {
        assert_eq!(
            pane_input_params("w1:p1", None, &["enter".to_string()]),
            serde_json::json!({"pane_id":"w1:p1","keys":["enter"]})
        );
        assert_eq!(
            pane_input_params("w1:p1", Some("hello"), &[]),
            serde_json::json!({"pane_id":"w1:p1","text":"hello","keys":[]})
        );
    }
    #[test]
    fn decodes_string_protocol_error_code() {
        let response =
            decode_response(r#"{"id":"req-1","error":{"code":"invalid_params","message":"bad"}}"#)
                .expect("error response");
        let error = response.error.expect("error body");
        assert_eq!(error.code, "invalid_params");
    }

    #[test]
    fn decodes_nested_output_subscription_read() {
        let event = decode_event(
            r#"{"event":"pane.output_matched","data":{"pane_id":"p1","matched_line":"hello","read":{"revision":7,"text":"screen"}}}"#,
        )
        .expect("output event");
        assert!(matches!(
            event,
            HerdrEvent::PaneOutput {
                revision: 7,
                delta,
                ..
            } if delta == "screen"
        ));
    }

    #[test]
    fn failing_pending_requests_wakes_waiters() {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = oneshot::channel();
        pending
            .lock()
            .expect("pending lock")
            .insert("req-1".to_string(), sender);
        fail_pending(&pending, HerdrClientError::Disconnected);
        let result = futures::executor::block_on(receiver).expect("waiter result");
        assert!(matches!(result, Err(HerdrClientError::Disconnected)));
    }

    #[test]
    fn decodes_typed_workspace_moved_event() {
        let event = decode_event(
            r#"{"event":"workspace.moved","data":{"type":"workspace_moved","workspace_id":"w2","insert_index":0,"workspaces":[{"workspace_id":"w2","number":0,"label":"Moved","focused":true,"pane_count":1,"tab_count":1,"active_tab_id":"t","agent_status":"idle"},{"workspace_id":"w1","number":1,"label":"Other","focused":false,"pane_count":0,"tab_count":0,"active_tab_id":"","agent_status":"idle"}]}}"#,
        )
        .expect("workspace moved event");
        match event {
            HerdrEvent::WorkspaceMoved {
                workspace_id,
                insert_index,
                workspaces,
                ..
            } => {
                assert_eq!(workspace_id, "w2");
                assert_eq!(insert_index, 0);
                assert_eq!(workspaces.len(), 2);
                assert_eq!(workspaces[0].label, "Moved");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn decodes_typed_workspace_reordered_event() {
        let event = decode_event(
            r#"{"event":"workspace.reordered","data":{"type":"workspace_reordered","workspace_ids":["w2","w1"],"before_workspace_id":null,"workspaces":[{"workspace_id":"w2","number":0,"label":"A","focused":false,"pane_count":0,"tab_count":0,"active_tab_id":"","agent_status":"idle"},{"workspace_id":"w1","number":1,"label":"B","focused":false,"pane_count":0,"tab_count":0,"active_tab_id":"","agent_status":"idle"}]}}"#,
        )
        .expect("workspace reordered event");
        match event {
            HerdrEvent::WorkspaceReordered {
                workspace_ids,
                before_workspace_id,
                workspaces,
                ..
            } => {
                assert_eq!(workspace_ids, vec!["w2".to_string(), "w1".to_string()]);
                assert_eq!(before_workspace_id, None);
                assert_eq!(workspaces.len(), 2);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn extracts_revision_from_official_wait_matched_result() {
        let result: Box<RawValue> = serde_json::from_str(
            r#"{"type":"wait_matched","event":{"event":"pane.output_changed","data":{"type":"pane_output_changed","pane_id":"p1","workspace_id":"w1","revision":42}}}"#,
        )
        .expect("wait matched");
        assert_eq!(wait_matched_revision(&result), Some(42));
    }

    #[test]
    fn events_wait_params_target_pane_output_changes() {
        let params = events_wait_params("w1:p1", 7);
        assert_eq!(
            params,
            serde_json::json!({
                "match_event": {
                    "event": "pane_output_changed",
                    "pane_id": "w1:p1",
                    "min_revision": 7
                },
                "timeout_ms": OUTPUT_WAIT_TIMEOUT_MS
            })
        );
    }


    #[cfg(unix)]
    fn recorded_methods(log: &[Value]) -> Vec<String> {
        log.iter()
            .map(|value| value["method"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// Finding 1: the timeout must cover blocking connect/send/read, so a
    /// server that accepts and never answers resolves with `Timeout`.
    #[cfg(unix)]
    #[test]
    fn request_times_out_when_server_never_responds() {
        use std::io::{BufRead, BufReader};
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("silent.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture");
        let server = std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 {
                    line.clear();
                }
            }
        });

        let pending: PendingRequests = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, _event_rx) = async_channel::unbounded();
        let (event_cursor_tx, _event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, _lifecycle_rx) = async_channel::unbounded();
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let request = HerdrRequest {
            id: "req-timeout".to_string(),
            method: "ping".to_string(),
            params: serde_json::Map::new().into(),
        };

        let started = std::time::Instant::now();
        let result = run_request_once(
            HerdrEndpoint::Explicit(path.to_string_lossy().into_owned()),
            request,
            Duration::from_millis(150),
            pending,
            event_tx,
            event_cursor_tx,
            lifecycle_tx,
            event_log,
            test_fence(),
        );

        assert!(matches!(result, Err(HerdrClientError::Timeout)));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timeout must not wait on an unbounded blocking read"
        );
        server.join().expect("fixture thread");
    }

    #[test]
    fn bootstrap_rejects_primary_subscription_end_event() {
        let data: Box<RawValue> =
            serde_json::from_str(r#"{"subscription_id":"primary","error":"closed"}"#)
                .expect("subscription-ended data");
        let ended = HerdrEvent::Unknown {
            event: "subscription_ended".to_string(),
            data,
        };
        assert!(bootstrap_primary_subscription_ended(
            std::slice::from_ref(&ended),
            "primary"
        ));
        assert!(!bootstrap_primary_subscription_ended(
            std::slice::from_ref(&ended),
            "pane-filter"
        ));
    }

    #[test]
    fn bootstrap_rejects_bulk_filter_subscription_end_event() {
        let data: Box<RawValue> =
            serde_json::from_str(r#"{"subscription_id":"bulk-filter","error":"closed"}"#)
                .expect("subscription-ended data");
        let ended = HerdrEvent::Unknown {
            event: "subscription_ended".to_string(),
            data,
        };
        assert!(
            bootstrap_subscription_ended(std::slice::from_ref(&ended)),
            "any terminated bootstrap subscription must force a reconnect"
        );
    }

    /// Finding 6: a well-formed frame without an `event` field terminates the
    /// subscription visibly instead of being discarded.
    #[cfg(unix)]
    #[test]
    fn subscription_pump_terminates_without_event_field() {
        use std::io::Write;
        use std::os::unix::net::UnixStream;

        let (server_side, client_side) = UnixStream::pair().expect("socket pair");
        let mut writer = server_side.try_clone().expect("clone socket");
        writer
            .write_all(
                br#"{"event":"pane.focused","data":{"type":"pane_focused","pane_id":"p1","workspace_id":"w1"}}"#,
            )
            .expect("write first frame");
        writer.write_all(b"\n").expect("write frame newline");
        writer
            .write_all(b"{\"id\":\"stray\"}\n")
            .expect("write stray frame");
        // Keep `server_side` alive on purpose: any further read would block
        // forever, so the pump must terminate from the stray frame itself.
        let mut reader = HerdrLineReader::new(HerdrStream::Unix(client_side));
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let (_event_tx, event_rx) = async_channel::unbounded::<HerdrEvent>();
        let (event_cursor_tx, event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, _lifecycle_rx) = async_channel::unbounded();

        let result = pump_subscription_events(
            &mut reader,
            &event_cursor_tx,
            &lifecycle_tx,
            &event_log,
            &test_fence(),
        );

        match result {
            Err(HerdrClientError::Codec(message)) => {
                assert!(message.contains("no event field"), "got: {message}");
            }
            other => panic!("expected codec termination, got {other:?}"),
        }
        assert_eq!(event_log_len(&event_log), 1);
        assert!(
            event_rx.try_recv().is_err(),
            "cursor-mode events must not accumulate in the legacy queue"
        );
        let delivered = futures::executor::block_on(event_cursor_rx.recv())
            .expect("delivered cursor");
        assert_eq!(delivered.index, 0);
        assert!(matches!(delivered.event, HerdrEvent::PaneFocused { .. }));
    }

    /// Review 4 finding 1: an established pump must discard events after its
    /// subscription generation is cancelled, including events already queued
    /// by the socket.
    #[cfg(unix)]
    #[test]
    fn established_subscription_pump_discards_events_after_generation_cancellation() {
        use std::io::Write;
        use std::os::unix::net::UnixStream;

        let (mut server_side, client_side) = UnixStream::pair().expect("socket pair");
        server_side
            .write_all(
                br#"{"event":"pane.focused","data":{"type":"pane_focused","pane_id":"p1","workspace_id":"w1"}}"#,
            )
            .expect("write queued event");
        server_side.write_all(b"\n").expect("write event newline");
        drop(server_side);

        let generation = Arc::new(AtomicU64::new(0));
        generation.store(1, Ordering::SeqCst);
        let fence = SubscriptionFence::new(
            generation,
            0,
            Arc::new(Mutex::new(())),
        );
        let mut reader = HerdrLineReader::new(HerdrStream::Unix(client_side));
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let (event_cursor_tx, event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, lifecycle_rx) = async_channel::unbounded();
        let result = pump_subscription_events(
            &mut reader,
            &event_cursor_tx,
            &lifecycle_tx,
            &event_log,
            &fence,
        );

        assert!(matches!(result, Err(HerdrClientError::Disconnected)));
        assert_eq!(event_log_len(&event_log), 0);
        assert!(event_cursor_rx.try_recv().is_err());
        assert!(lifecycle_rx.try_recv().is_err());
    }

    #[test]
    fn cancelled_generation_drops_watcher_output_publication() {
        let generation = Arc::new(AtomicU64::new(0));
        let fence = SubscriptionFence::new(
            generation.clone(),
            0,
            Arc::new(Mutex::new(())),
        );
        generation.store(1, Ordering::SeqCst);
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        let (event_cursor_tx, event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, lifecycle_rx) = async_channel::unbounded();
        let published = publish_event(
            &fence,
            HerdrEvent::PaneOutput {
                pane_id: "w1:p1".to_string(),
                revision: 2,
                delta: "stale".to_string(),
                sequence: 0,
            },
            &event_cursor_tx,
            &lifecycle_tx,
            &event_log,
        )
        .expect("stale publication check");

        assert!(!published);
        assert_eq!(event_log_len(&event_log), 0);
        assert!(event_cursor_rx.try_recv().is_err());
        assert!(lifecycle_rx.try_recv().is_err());
    }

    /// Finding 4 (deterministic): pane lifecycle events are forwarded to the
    /// watch supervisor, and unrelated events are not.
    #[test]
    fn lifecycle_events_are_forwarded_for_watch_supervision() {
        let (lifecycle_tx, lifecycle_rx) = async_channel::unbounded();

        let created = HerdrEvent::PaneCreated {
            pane: HerdrPaneSnapshot {
                pane_id: "w1:p2".to_string(),
                workspace_id: "w1".to_string(),
                ..Default::default()
            },
            sequence: 0,
        };
        forward_lifecycle(&lifecycle_tx, 7, &created);
        let seen = lifecycle_rx.try_recv().expect("lifecycle event forwarded");
        assert_eq!(seen.generation, 7);
        assert_eq!(seen.event.pane_id(), Some("w1:p2"));

        let closed = HerdrEvent::PaneClosed {
            pane_id: "w1:p2".to_string(),
            workspace_id: "w1".to_string(),
            sequence: 0,
        };
        forward_lifecycle(&lifecycle_tx, 7, &closed);
        lifecycle_rx.try_recv().expect("closed forwarded");

        let focused = HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 0,
        };
        forward_lifecycle(&lifecycle_tx, 7, &focused);
        assert!(lifecycle_rx.try_recv().is_err(), "non-lifecycle event filtered");
    }

    #[test]
    fn watch_supervisor_discards_queued_lifecycle_events_from_retired_generation() {
        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle = HerdrClientHandle::new_with_executor(HerdrEndpoint::Default, executor);
        let retired_generation = handle.subscription_generation.load(Ordering::SeqCst);

        handle.cancel_subscription_generation();
        assert_eq!(
            handle.subscription_generation.load(Ordering::SeqCst),
            retired_generation + 1
        );

        let created = HerdrEvent::PaneCreated {
            pane: HerdrPaneSnapshot {
                pane_id: "w1:p2".to_string(),
                workspace_id: "w1".to_string(),
                ..Default::default()
            },
            sequence: 0,
        };
        let moved = HerdrEvent::PaneMoved {
            pane: HerdrPaneSnapshot {
                pane_id: "w1:p3".to_string(),
                workspace_id: "w1".to_string(),
                ..Default::default()
            },
            previous_pane_id: Some("w1:p2".to_string()),
            previous_workspace_id: None,
            previous_tab_id: None,
            sequence: 0,
        };
        for event in [created, moved] {
            handle
                .lifecycle_tx
                .try_send(HerdrLifecycleEvent {
                    generation: retired_generation,
                    event,
                })
                .expect("queue stale lifecycle event");
        }

        while let Ok(event) = handle.lifecycle_rx.try_recv() {
            handle.handle_lifecycle_event(event);
        }
        assert!(
            watched_lock(&handle.watched_panes).is_empty(),
            "queued events from a retired generation must not register new watches"
        );
    }

    /// Finding 4 (deterministic): watch state is added once per pane and
    /// retired with a cancellation signal when panes close or move away.
    #[test]
    fn pane_watch_state_tracks_ensure_and_retire() {
        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle = HerdrClientHandle::new_with_executor(
            HerdrEndpoint::Default,
            executor,
        );

        let cancel = handle.ensure_watched("w1:p1").expect("first watch");
        assert!(!cancel.load(Ordering::SeqCst));
        assert!(handle.ensure_watched("w1:p1").is_none(), "already watched");

        handle.retire_pane("w1:p1");
        assert!(cancel.load(Ordering::SeqCst), "retirement cancels watcher");
        assert!(!watched_lock(&handle.watched_panes).contains_key("w1:p1"));
        handle.retire_pane("w1:p1"); // retiring an unknown pane is a no-op
    }

    /// Review 4 finding 3: the `events.wait` request deadline must exceed
    /// the server-side wait by the bounded margin.
    #[test]
    fn events_wait_deadline_exceeds_server_wait_with_bounded_margin() {
        let deadline = events_wait_deadline();
        assert_eq!(
            deadline,
            Duration::from_millis(OUTPUT_WAIT_TIMEOUT_MS + EVENTS_WAIT_DEADLINE_MARGIN_MS)
        );
        assert!(
            deadline > Duration::from_millis(OUTPUT_WAIT_TIMEOUT_MS),
            "request deadline must outlive the server wait"
        );
    }

    /// Review 4 finding 4 (race): a filter handshake that completes after
    /// its pane was retired is torn down immediately instead of registering
    /// an orphaned subscription loop.
    #[cfg(unix)]
    #[test]
    fn storing_filter_switch_after_retire_triggers_teardown() {
        use std::os::unix::net::UnixStream;

        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle =
            HerdrClientHandle::new_with_executor(HerdrEndpoint::Default, executor);

        let cancel = handle.ensure_watched("w1:p9").expect("watch");
        handle.retire_pane("w1:p9");
        assert!(cancel.load(Ordering::SeqCst));

        let (server_side, client_side) = UnixStream::pair().expect("socket pair");
        let kill = HerdrStream::Unix(client_side)
            .kill_switch()
            .expect("kill switch");
        drop(server_side);
        handle.store_filter_kill_switch("w1:p9", kill);
        assert!(
            !watched_lock(&handle.watched_panes).contains_key("w1:p9"),
            "retired pane stays retired"
        );
    }

    /// Review 5 finding 2 (race): a reused pane id can hold a live filter
    /// subscription when a stale-generation handshake completes; storing it
    /// must trigger the kill switch it replaces so the superseded
    /// subscription connection is torn down instead of leaking.
    #[cfg(unix)]
    #[test]
    fn storing_filter_switch_over_a_live_one_triggers_the_previous() {
        use std::os::unix::net::UnixStream;
        use std::time::Instant;

        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle =
            HerdrClientHandle::new_with_executor(HerdrEndpoint::Default, executor);

        let make_pair = || {
            let (server_side, client_side) = UnixStream::pair().expect("socket pair");
            let kill = HerdrStream::Unix(client_side.try_clone().expect("clone socket"))
                .kill_switch()
                .expect("kill switch");
            (kill, client_side, server_side)
        };

        // Live watch whose filter handshake already completed: pane "w1:p1"
        // was retired and its id reused, so this switch belongs to the new
        // generation.
        handle.ensure_watched("w1:p1").expect("watch");
        let (live_kill, live_client, live_server) = make_pair();
        handle.store_filter_kill_switch("w1:p1", live_kill);

        // A pump blocked reading the live connection.
        let mut live_reader = HerdrLineReader::new(HerdrStream::Unix(live_client));
        let live_waiter = std::thread::spawn(move || live_reader.read_line());
        std::thread::sleep(Duration::from_millis(50));

        // Stale-generation handshake arrives late and must replace-and-
        // trigger the live switch instead of silently overwriting it.
        let (stale_kill, _, stale_server) = make_pair();
        let started = Instant::now();
        handle.store_filter_kill_switch("w1:p1", stale_kill);

        let unblocked = live_waiter.join().expect("reader thread exits");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "replacing the filter switch must tear down the previous connection"
        );
        assert!(
            unblocked.is_ok(),
            "the superseded connection's read completes after teardown"
        );
        assert!(
            watched_lock(&handle.watched_panes)["w1:p1"]
                .filter_kill
                .is_some(),
            "the newest switch stays registered"
        );
        drop(stale_server);
        drop(live_server);
    }

    /// Review 4 finding 4: retiring a pane (close, agent exit, or stale
    /// moved-away id) terminates its per-pane filter subscription connection,
    /// not only the output watcher.
    #[cfg(unix)]
    #[test]
    fn retire_pane_tears_down_filter_subscription_connection() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("filters.sock");
        let listener = UnixListener::bind(&path).expect("bind fixture");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept fixture");
            let mut subscribe_line = String::new();
            BufReader::new(stream.try_clone().expect("clone fixture"))
                .read_line(&mut subscribe_line)
                .expect("read subscribe line");
            let request: Value =
                serde_json::from_str(subscribe_line.trim()).expect("subscribe frame");
            // Acknowledge whatever id the client used, then hold the socket
            // open like a live Herdr server would.
            let ack = format!(
                "{{\"id\":{},\"result\":{{\"type\":\"subscription_started\",\"subscription_id\":\"s1\"}}}}\n",
                request["id"]
            );
            stream
                .write_all(ack.as_bytes())
                .expect("write acknowledgement");

            // Teardown must close the connection: this read returns EOF
            // instead of blocking forever.
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("server read timeout");
            let mut after = String::new();
            match BufReader::new(stream).read_line(&mut after) {
                Ok(0) => {}
                Ok(count) => panic!("filter connection stayed open; got {count} bytes"),
                Err(error) => panic!("filter read failed unexpectedly: {error}"),
            }
        });

        let dispatcher = Arc::new(gpui::TestDispatcher::new(0));
        let executor = gpui::BackgroundExecutor::new(dispatcher);
        let handle = HerdrClientHandle::new_with_executor(
            HerdrEndpoint::Explicit(path.to_string_lossy().into_owned()),
            executor,
        );

        let (event_tx, _event_rx) = async_channel::unbounded();
        let (event_cursor_tx, _event_cursor_rx) = async_channel::unbounded();
        let (lifecycle_tx, _lifecycle_rx) = async_channel::unbounded();
        let event_log = Arc::new(Mutex::new(HerdrEventLog::default()));
        handle.ensure_watched("w1:p1").expect("register watch");

        // Drive the connection thread directly: awaiting a GPUI-executor
        // task under block_on would deadlock the test dispatcher.
        let (ready_tx, ready_rx) = oneshot::channel::<
            Result<(String, usize, ConnectionKillSwitch), HerdrClientError>,
        >();
        std::thread::Builder::new()
            .name("herdr-subscription-fixture".to_string())
            .spawn({
                let endpoint = HerdrEndpoint::Explicit(path.to_string_lossy().into_owned());
                move || {
                    run_subscription_connection(
                        endpoint,
                        HerdrRequest {
                            id: "req-sub".to_string(),
                            method: "events.subscribe".to_string(),
                            params: pane_filter_subscription_params(&[
                                "w1:p1".to_string(),
                            ]),
                        },
                        Duration::from_secs(5),
                        ready_tx,
                        event_tx,
                        event_cursor_tx,
                        lifecycle_tx,
                        event_log,
                        test_fence(),
                    );
                }
            })
            .expect("spawn fixture subscription");
        let (_, boundary, kill) = futures::executor::block_on(ready_rx)
            .expect("handshake channel")
            .expect("handshake success");
        assert_eq!(boundary, 0);
        handle.store_filter_kill_switch("w1:p1", kill);
        assert!(
            watched_lock(&handle.watched_panes)["w1:p1"]
                .filter_kill
                .is_some(),
            "filter ownership recorded before retirement"
        );

        handle.retire_pane("w1:p1");
        server.join().expect("fixture observes teardown");
    }
}
