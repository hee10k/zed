use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::channel::oneshot;
use gpui::{App, BackgroundExecutor, Task};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};

use crate::herdr_transport::{HerdrEndpoint, HerdrLineReader, HerdrStream};

const HERDR_PROTOCOL: u64 = 20;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

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
    WorkspaceFocused { workspace_id: String, operation_id: Option<String>, sequence: u64 },
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
        "workspace_focused" | "workspace.focused" => Ok(HerdrEvent::WorkspaceFocused {
            workspace_id: required_string(&data, "workspace_id")?,
            operation_id: data.get("operation_id").and_then(Value::as_str).map(ToOwned::to_owned),
            sequence,
        }),
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
        "pane_focused" | "pane.focused" => Ok(HerdrEvent::PaneFocused {
            pane_id: required_string(&data, "pane_id")?,
            workspace_id: required_string(&data, "workspace_id")?,
            operation_id: data.get("operation_id").and_then(Value::as_str).map(ToOwned::to_owned),
            sequence,
        }),
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

fn record_event(event_log: &Arc<Mutex<Vec<HerdrEvent>>>, event: HerdrEvent) {
    match event_log.lock() {
        Ok(mut events) => events.push(event),
        Err(poisoned) => poisoned.into_inner().push(event),
    }
}
fn event_log_len(event_log: &Arc<Mutex<Vec<HerdrEvent>>>) -> usize {
    match event_log.lock() {
        Ok(events) => events.len(),
        Err(poisoned) => poisoned.into_inner().len(),
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
fn events_since(
    event_log: &Arc<Mutex<Vec<HerdrEvent>>>,
    start: usize,
) -> Vec<HerdrEvent> {
    match event_log.lock() {
        Ok(events) => events[start..].to_vec(),
        Err(poisoned) => poisoned.into_inner()[start..].to_vec(),
    }
}

#[derive(Clone)]
pub(crate) struct HerdrClientHandle {
    endpoint: HerdrEndpoint,
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    event_tx: Sender<HerdrEvent>,
    event_rx: Receiver<HerdrEvent>,
    event_log: Arc<Mutex<Vec<HerdrEvent>>>,
    lifecycle_tx: Sender<HerdrEvent>,
    lifecycle_rx: Receiver<HerdrEvent>,
    watched_panes: WatchedPanes,
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

    fn new_with_executor(endpoint: HerdrEndpoint, executor: BackgroundExecutor) -> Self {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = async_channel::unbounded();
        let (lifecycle_tx, lifecycle_rx) = async_channel::unbounded();
        Self {
            endpoint,
            next_id: Arc::new(AtomicU64::new(1)),
            pending,
            event_tx,
            event_rx,
            event_log: Arc::new(Mutex::new(Vec::new())),
            lifecycle_tx,
            lifecycle_rx,
            watched_panes: Arc::new(Mutex::new(HashMap::new())),
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
    /// Blocking connect/send/read runs on a dedicated thread under socket
    /// deadlines, so a server that accepts and never responds resolves the
    /// caller's task with `Timeout` instead of hanging forever.
    fn request_on_executor(&self, method: &str, params: Value) -> Task<PendingResult> {
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
        let lifecycle_tx = self.lifecycle_tx.clone();
        let event_log = self.event_log.clone();
        let spawned = std::thread::Builder::new()
            .name("herdr-request".to_string())
            .spawn(move || {
                let _ = run_request_once(
                    endpoint,
                    request,
                    REQUEST_TIMEOUT,
                    pending,
                    event_tx,
                    lifecycle_tx,
                    event_log,
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

    /// Start the long-lived subscription connection. Resolves once
    /// `subscription_started` is acknowledged; pushed events then flow through
    /// the shared event channel until the connection terminates.
    fn start_subscription(&self, params: Value) -> Task<Result<(String, usize), HerdrClientError>> {
        let request_id = format!("req-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let request = HerdrRequest {
            id: request_id.clone(),
            method: "events.subscribe".to_string(),
            params,
        };
        let endpoint = self.endpoint.clone();
        let event_tx = self.event_tx.clone();
        let lifecycle_tx = self.lifecycle_tx.clone();
        let event_log = self.event_log.clone();
        let (ready_tx, ready_rx) =
            oneshot::channel::<Result<(String, usize), HerdrClientError>>();
        let spawned = std::thread::Builder::new()
            .name("herdr-subscription".to_string())
            .spawn(move || {
                run_subscription_connection(
                    endpoint,
                    request,
                    REQUEST_TIMEOUT,
                    ready_tx,
                    event_tx,
                    lifecycle_tx,
                    event_log,
                );
            });
        if let Err(error) = spawned {
            log::error!("Failed to spawn Herdr subscription thread: {error}");
        }
        self.executor.clone().spawn(async move {
            ready_rx
                .await
                .unwrap_or_else(|_| Err(HerdrClientError::Disconnected))
        })
    }

    /// Track a pane created mid-session: per-pane filters plus a continuous
    /// output watcher. No-op if the pane is already watched.
    fn watch_pane(&self, pane_id: String) {
        let Some(cancel) = self.ensure_watched(&pane_id) else {
            return;
        };

        let filters = self.start_subscription(pane_filter_subscription_params(&[pane_id.clone()]));
        let filter_executor = self.executor.clone();
        let filter_pane_id = pane_id.clone();
        filter_executor
            .spawn(async move {
                if let Err(error) = filters.await {
                    log::error!("Herdr per-pane subscription failed for {filter_pane_id}: {error}");
                }
            })
            .detach();
        self.spawn_output_watcher(pane_id, cancel);
    }

    /// Begin output watching for a pane whose per-pane filters were already
    /// registered by the bootstrap subscription batch.
    fn track_pane_output(&self, pane_id: String) {
        let Some(cancel) = self.ensure_watched(&pane_id) else {
            return;
        };
        self.spawn_output_watcher(pane_id, cancel);
    }

    fn ensure_watched(&self, pane_id: &str) -> Option<Arc<AtomicBool>> {
        let mut watched = watched_lock(&self.watched_panes);
        if watched.contains_key(pane_id) {
            return None;
        }
        let cancel = Arc::new(AtomicBool::new(false));
        watched.insert(pane_id.to_string(), cancel.clone());
        Some(cancel)
    }

    fn retire_pane(&self, pane_id: &str) {
        let cancel = watched_lock(&self.watched_panes).remove(pane_id);
        if let Some(cancel) = cancel {
            cancel.store(true, Ordering::SeqCst);
        }
    }

    /// Follow a pane's output continuously. Herdr matcher subscriptions only
    /// fire on non-matching -> matching transitions, so instead repeatedly
    /// block in `events.wait` for `pane_output_changed` and fetch the changed
    /// buffer with a revision-aware `pane.read`.
    fn spawn_output_watcher(&self, pane_id: String, cancel: Arc<AtomicBool>) {
        let client = self.clone();
        self.executor
            .clone()
            .spawn(async move {
                let mut last_revision: u64 = 0;
                loop {
                    if cancel.load(Ordering::SeqCst) {
                        return;
                    }
                    let wait = client
                        .request_on_executor(
                            "events.wait",
                            events_wait_params(&pane_id, last_revision.saturating_add(1)),
                        )
                        .await;
                    let notified_revision = match wait {
                        Err(HerdrClientError::Timeout) => continue,
                        Err(HerdrClientError::ProtocolError { code, message }) => {
                            if code.contains("timeout") || code.contains("timed_out") {
                                continue;
                            }
                            log::warn!(
                                "Herdr output watcher stopped for {pane_id}: protocol error ({code}): {message}"
                            );
                            return;
                        }
                        Err(error) => {
                            log::warn!("Herdr output watcher stopped for {pane_id}: {error}");
                            return;
                        }
                        Ok(result) => match wait_matched_revision(&result) {
                            Some(revision) => revision,
                            None => {
                                log::warn!(
                                    "Herdr output watcher stopped for {pane_id}: unexpected events.wait result"
                                );
                                return;
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
                    match read.as_deref().map_err(Clone::clone).and_then(|result| decode_pane_read_result(result.get())) {
                        Ok((revision, text)) => {
                            if revision > last_revision {
                                last_revision = revision;
                                let event = HerdrEvent::PaneOutput {
                                    pane_id: pane_id.clone(),
                                    revision,
                                    delta: text,
                                    sequence: 0,
                                };
                                record_event(&client.event_log, event.clone());
                                if client.event_tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            log::warn!("Herdr pane.read failed for {pane_id}: {error}");
                        }
                    }
                }
            })
            .detach();
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
                    match event {
                        HerdrEvent::PaneCreated { ref pane, .. } => {
                            client.watch_pane(pane.pane_id.clone());
                        }
                        HerdrEvent::PaneMoved {
                            ref pane,
                            ref previous_pane_id,
                            ..
                        } => {
                            if let Some(previous) = previous_pane_id {
                                if previous != &pane.pane_id {
                                    client.retire_pane(previous);
                                }
                            }
                            client.watch_pane(pane.pane_id.clone());
                        }
                        HerdrEvent::PaneClosed { ref pane_id, .. }
                        | HerdrEvent::PaneExited { ref pane_id, .. } => {
                            client.retire_pane(pane_id);
                        }
                        _ => {}
                    }
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
            let ping = client.request_on_executor("ping", empty_params()).await?;
            validate_ping_result(ping.get())?;

            // First snapshot is only used to learn pane IDs so every per-pane
            // filter can register its baseline before authoritative state is
            // captured; changes between the two snapshots cannot be lost.
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

            let (subscription_id, _) = client.start_subscription(subscription_params()).await?;
            if !pane_ids.is_empty() {
                client
                    .start_subscription(pane_filter_subscription_params(&pane_ids))
                    .await?;
                for pane_id in &pane_ids {
                    client.track_pane_output(pane_id.clone());
                }
            }
            client.start_watch_supervisor();

            let start = event_log_len(&event_log);
            let snapshot = decode_snapshot_result(
                client
                    .request_on_executor("session.snapshot", empty_params())
                    .await?
                    .get(),
            )?;
            let events = events_since(&event_log, start);
            Ok(HerdrBootstrap {
                snapshot,
                subscription_id,
                events,
            })
        })
    }
}

type WatchedPanes = Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>;

fn watched_lock(watched: &WatchedPanes) -> std::sync::MutexGuard<'_, HashMap<String, Arc<AtomicBool>>> {
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
        }
    }
    HerdrClientError::Io(error.to_string())
}

fn forward_lifecycle(lifecycle_tx: &Sender<HerdrEvent>, event: &HerdrEvent) {
    if matches!(
        event,
        HerdrEvent::PaneCreated { .. }
            | HerdrEvent::PaneMoved { .. }
            | HerdrEvent::PaneClosed { .. }
            | HerdrEvent::PaneExited { .. }
    ) {
        let _ = lifecycle_tx.try_send(event.clone());
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
    event_tx: Sender<HerdrEvent>,
    lifecycle_tx: Sender<HerdrEvent>,
    event_log: Arc<Mutex<Vec<HerdrEvent>>>,
) -> PendingResult {
    let request_id = request.id.clone();
    let attempt = (|| -> PendingResult {
        let mut stream = HerdrStream::connect_with_deadline(&endpoint, deadline)
            .map_err(|error| HerdrClientError::EndpointNotFound(error.to_string()))?;
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
                record_event(&event_log, event.clone());
                forward_lifecycle(&lifecycle_tx, &event);
                if futures::executor::block_on(event_tx.send(event)).is_err() {
                    break Err(HerdrClientError::Disconnected);
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
    ready_tx: oneshot::Sender<Result<(String, usize), HerdrClientError>>,
    event_tx: Sender<HerdrEvent>,
    lifecycle_tx: Sender<HerdrEvent>,
    event_log: Arc<Mutex<Vec<HerdrEvent>>>,
) {
    let mut stream = match HerdrStream::connect_with_deadline(&endpoint, handshake_deadline) {
        Ok(stream) => stream,
        Err(error) => {
            let _ = ready_tx.send(Err(HerdrClientError::EndpointNotFound(error.to_string())));
            return;
        }
    };
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
    loop {
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
                    Ok(event) => {
                        record_event(&event_log, event.clone());
                        forward_lifecycle(&lifecycle_tx, &event);
                        if futures::executor::block_on(event_tx.send(event)).is_err() {
                            let _ = ready_tx.send(Err(HerdrClientError::Disconnected));
                            return;
                        }
                    }
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
        let subscription_id = value
            .get("subscription_id")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();
        let boundary = event_log_len(&event_log);
        if ready_tx.send(Ok((subscription_id, boundary))).is_err() {
            return;
        }
        break;
    }

    // Established: idle pushes must not be cut off by the handshake deadline.
    if let Err(error) = reader.set_read_timeout(None) {
        log::error!("Herdr subscription could not clear its read deadline: {error}");
        return;
    }
    if let Err(error) = pump_subscription_events(&mut reader, &event_tx, &lifecycle_tx, &event_log) {
        log::error!("Herdr subscription terminated: {error}");
    }
}

/// Drain pushed frames from an established subscription. A well-formed frame
/// without an `event` field is malformed subscription input: report it and
/// terminate instead of silently discarding it.
fn pump_subscription_events(
    reader: &mut HerdrLineReader,
    event_tx: &Sender<HerdrEvent>,
    lifecycle_tx: &Sender<HerdrEvent>,
    event_log: &Arc<Mutex<Vec<HerdrEvent>>>,
) -> Result<(), HerdrClientError> {
    loop {
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
        record_event(event_log, event.clone());
        forward_lifecycle(lifecycle_tx, &event);
        futures::executor::block_on(event_tx.send(event))
            .map_err(|_| HerdrClientError::Disconnected)?;
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
    pub events: Vec<HerdrEvent>,
}

pub(crate) trait HerdrApi: Send + Sync {
    fn ping(&self, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn subscribe_events(&self, cx: &App) -> Task<Result<String, HerdrClientError>>;
    fn bootstrap(&self, cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>>;
    fn get_snapshot(&self, cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>>;
    fn focus_workspace(&self, workspace_id: &str, operation_id: Option<&str>, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn create_workspace(&self, label: &str, paths: Vec<String>, cx: &App) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>>;
    fn rename_workspace(&self, workspace_id: &str, label: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn close_workspace(&self, workspace_id: &str, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn focus_pane(&self, pane_id: &str, operation_id: Option<&str>, cx: &App) -> Task<Result<(), HerdrClientError>>;
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

fn empty_params() -> Value {
    serde_json::Map::new().into()
}
fn subscription_params() -> Value {
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

const OUTPUT_WAIT_TIMEOUT_MS: u64 = 15_000;

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
        let task = self.start_subscription(subscription_params());
        self.executor
            .clone()
            .spawn(async move { task.await.map(|(subscription_id, _)| subscription_id) })
    }

    fn bootstrap(&self, _cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        self.bootstrap_on_executor()
    }

    fn get_snapshot(&self, _cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>> {
        let task = self.request_on_executor("session.snapshot", empty_params());
        let executor = self.executor.clone();
        executor.spawn(async move {
            let result = task.await?;
            decode_snapshot_result(result.get())
        })
    }

    fn focus_workspace(&self, workspace_id: &str, _operation_id: Option<&str>, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("workspace.focus", serde_json::json!({"workspace_id": workspace_id}));
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

    fn focus_pane(&self, pane_id: &str, _operation_id: Option<&str>, _cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request_on_executor("pane.focus", pane_target_params(pane_id));
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
        let event_log = Arc::new(Mutex::new(Vec::new()));
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
        let (lifecycle_tx, _lifecycle_rx) = async_channel::unbounded();
        let event_log = Arc::new(Mutex::new(Vec::new()));
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
            lifecycle_tx,
            event_log,
        );

        assert!(matches!(result, Err(HerdrClientError::Timeout)));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timeout must not wait on an unbounded blocking read"
        );
        server.join().expect("fixture thread");
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
        let event_log = Arc::new(Mutex::new(Vec::new()));
        let (event_tx, event_rx) = async_channel::unbounded();
        let (lifecycle_tx, _lifecycle_rx) = async_channel::unbounded();

        let result = pump_subscription_events(&mut reader, &event_tx, &lifecycle_tx, &event_log);
        match result {
            Err(HerdrClientError::Codec(message)) => {
                assert!(message.contains("no event field"), "got: {message}");
            }
            other => panic!("expected codec termination, got {other:?}"),
        }
        assert_eq!(event_log_len(&event_log), 1);
        let delivered = futures::executor::block_on(event_rx.recv()).expect("delivered event");
        assert!(matches!(delivered, HerdrEvent::PaneFocused { .. }));
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
        forward_lifecycle(&lifecycle_tx, &created);
        let seen = lifecycle_rx.try_recv().expect("lifecycle event forwarded");
        assert_eq!(seen.pane_id(), Some("w1:p2"));

        let closed = HerdrEvent::PaneClosed {
            pane_id: "w1:p2".to_string(),
            workspace_id: "w1".to_string(),
            sequence: 0,
        };
        forward_lifecycle(&lifecycle_tx, &closed);
        lifecycle_rx.try_recv().expect("closed forwarded");

        let focused = HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 0,
        };
        forward_lifecycle(&lifecycle_tx, &focused);
        assert!(lifecycle_rx.try_recv().is_err(), "non-lifecycle event filtered");
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
}
