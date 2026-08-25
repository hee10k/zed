use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use async_channel::{Receiver, Sender};
use futures::{channel::oneshot, future::Either};
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
    PaneCreated { pane: HerdrPaneSnapshot, sequence: u64 },
    PaneUpdated { pane: HerdrPaneSnapshot, sequence: u64 },
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
            | Self::PaneAgentDetected { workspace_id, .. }
            | Self::PaneFocused { workspace_id, .. }
            | Self::PaneClosed { workspace_id, .. } => Some(workspace_id),
            Self::PaneCreated { pane, .. } | Self::PaneUpdated { pane, .. } => {
                Some(&pane.workspace_id)
            }
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
            Self::PaneCreated { pane, .. } | Self::PaneUpdated { pane, .. } => Some(&pane.pane_id),
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
            | Self::PaneCreated { sequence, .. }
            | Self::PaneUpdated { sequence, .. }
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

#[derive(Clone)]
pub(crate) struct HerdrClientHandle {
    next_id: Arc<AtomicU64>,
    pending: PendingRequests,
    event_rx: Receiver<HerdrEvent>,
    event_log: Arc<Mutex<Vec<HerdrEvent>>>,
    writer_tx: Sender<String>,
    executor: BackgroundExecutor,
}

impl HerdrClientHandle {
    pub(crate) fn new(stream: HerdrStream, cx: &App) -> Result<Self, HerdrClientError> {
        Self::new_with_executor(stream, cx.background_executor().clone())
    }

    fn new_with_executor(
        stream: HerdrStream,
        executor: BackgroundExecutor,
    ) -> Result<Self, HerdrClientError> {
        let write_stream = stream
            .try_clone()
            .map_err(|error| HerdrClientError::Io(error.to_string()))?;
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = async_channel::unbounded();
        let (writer_tx, writer_rx) = async_channel::unbounded::<String>();
        let event_log = Arc::new(Mutex::new(Vec::new()));

        let pending_for_reader = pending.clone();
        let event_tx_for_reader = event_tx.clone();
        let event_log_for_reader = event_log.clone();
        executor
            .clone()
            .spawn(async move {
                let mut line_reader = HerdrLineReader::new(stream);
                loop {
                    let line = match line_reader.read_line() {
                        Ok(Some(line)) => line,
                        Ok(None) => {
                            fail_pending(&pending_for_reader, HerdrClientError::Disconnected);
                            break;
                        }
                        Err(error) => {
                            fail_pending(
                                &pending_for_reader,
                                HerdrClientError::Io(error.to_string()),
                            );
                            break;
                        }
                    };
                    if line.trim().is_empty() {
                        continue;
                    }
                    let value: Value = match serde_json::from_str(&line) {
                        Ok(value) => value,
                        Err(error) => {
                            fail_pending(
                                &pending_for_reader,
                                HerdrClientError::Codec(error.to_string()),
                            );
                            break;
                        }
                    };
                    if value.get("id").is_some() {
                        let response = match decode_response(&line) {
                            Ok(response) => response,
                            Err(error) => {
                                fail_pending(&pending_for_reader, error);
                                break;
                            }
                        };
                        let sender = match pending_for_reader.lock() {
                            Ok(mut pending) => pending.remove(&response.id),
                            Err(poisoned) => poisoned.into_inner().remove(&response.id),
                        };
                        if let Some(sender) = sender {
                            let result = match (response.result, response.error) {
                                (_, Some(error)) => Err(HerdrClientError::ProtocolError {
                                    code: error.code,
                                    message: error.message,
                                }),
                                (Some(result), None) => Ok(result),
                                (None, None) => Err(HerdrClientError::Codec(
                                    "success response missing result".to_string(),
                                )),
                            };
                            if sender.send(result).is_err() {
                                log::debug!("Herdr response waiter was already dropped");
                            }
                        }
                    } else if value.get("event").is_some() {
                        let event = match decode_event(&line) {
                            Ok(event) => event,
                            Err(error) => {
                                fail_pending(&pending_for_reader, error);
                                break;
                            }
                        };
                        record_event(&event_log_for_reader, event.clone());
                        if event_tx_for_reader.send(event).await.is_err() {
                            fail_pending(
                                &pending_for_reader,
                                HerdrClientError::Disconnected,
                            );
                            break;
                        }
                    } else {
                        fail_pending(
                            &pending_for_reader,
                            HerdrClientError::Codec("frame is neither response nor event".to_string()),
                        );
                        break;
                    }
                }
            })
            .detach();

        let pending_for_writer = pending.clone();
        executor
            .clone()
            .spawn(async move {
                let mut stream = write_stream;
                while let Ok(line) = writer_rx.recv().await {
                    if let Err(error) = stream.send_line(&line) {
                        fail_pending(
                            &pending_for_writer,
                            HerdrClientError::Io(error.to_string()),
                        );
                        break;
                    }
                }
                fail_pending(&pending_for_writer, HerdrClientError::Disconnected);
            })
            .detach();
        Ok(Self {
            next_id: Arc::new(AtomicU64::new(1)),
            pending,
            event_rx,
            event_log,
            writer_tx,
            executor,
        })
    }

    pub(crate) fn connect(endpoint: &HerdrEndpoint, cx: &App) -> Task<Result<Self, HerdrClientError>> {
        let endpoint = endpoint.clone();
        let executor = cx.background_executor().clone();
        executor.clone().spawn(async move {
            let stream = HerdrStream::connect(&endpoint)
                .map_err(|error| HerdrClientError::EndpointNotFound(error.to_string()))?;
            Self::new_with_executor(stream, executor)
        })
    }

    pub(crate) fn request(
        &self,
        method: &str,
        params: Option<Value>,
        _cx: &App,
    ) -> Task<PendingResult> {
        self.request_on_executor(method, params.unwrap_or_else(|| serde_json::Map::new().into()))
    }

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
        let writer_tx = self.writer_tx.clone();
        let pending = self.pending.clone();
        let executor = self.executor.clone();
        executor.clone().spawn(async move {
            let encoded = match serde_json::to_string(&request) {
                Ok(encoded) => encoded,
                Err(error) => {
                    remove_pending(&pending, &request_id);
                    return Err(HerdrClientError::Codec(error.to_string()));
                }
            };
            if let Err(error) = writer_tx.send(encoded).await {
                remove_pending(&pending, &request_id);
                return Err(HerdrClientError::Io(error.to_string()));
            }
            let timeout = executor.timer(REQUEST_TIMEOUT);
            futures::pin_mut!(receiver, timeout);
            match futures::future::select(receiver, timeout).await {
                Either::Left((result, _)) => result.unwrap_or(Err(HerdrClientError::Disconnected)),
                Either::Right((_, _)) => {
                    remove_pending(&pending, &request_id);
                    Err(HerdrClientError::Timeout)
                }
            }
        })
    }

    pub(crate) fn subscribe(&self) -> Receiver<HerdrEvent> {
        self.event_rx.clone()
    }

    pub(crate) fn bootstrap(&self, _cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        let client = self.clone();
        let event_log = self.event_log.clone();
        let executor = self.executor.clone();
        executor.clone().spawn(async move {
            let ping = client.request_on_executor("ping", empty_params()).await?;
            validate_ping_result(ping.get())?;

            let subscription = client
                .request_on_executor("events.subscribe", subscription_params())
                .await?;
            let subscription_value = value_from_raw(&subscription)?;
            if subscription_value.get("type").and_then(Value::as_str)
                != Some("subscription_started")
            {
                return Err(HerdrClientError::Codec(
                    "events.subscribe did not return subscription_started".to_string(),
                ));
            }
            let subscription_id = subscription_value
                .get("subscription_id")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string();
            let start = match event_log.lock() {
                Ok(events) => events.len(),
                Err(poisoned) => poisoned.into_inner().len(),
            };

            let snapshot = decode_snapshot_result(
                client
                    .request_on_executor("session.snapshot", empty_params())
                    .await?
                    .get(),
            )?;
            let events = match event_log.lock() {
                Ok(events) => events[start..]
                    .iter()
                    .filter(|event| event.sequence() > snapshot.sequence)
                    .cloned()
                    .collect(),
                Err(poisoned) => poisoned.into_inner()[start..]
                    .iter()
                    .filter(|event| event.sequence() > snapshot.sequence)
                    .cloned()
                    .collect(),
            };
            Ok(HerdrBootstrap {
                snapshot,
                subscription_id,
                events,
            })
        })
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
        let task = self.request_on_executor("events.subscribe", subscription_params());
        let executor = self.executor.clone();
        executor.spawn(async move {
            let result = task.await?;
            let value = value_from_raw(&result)?;
            if value.get("type").and_then(Value::as_str) != Some("subscription_started") {
                return Err(HerdrClientError::Codec(
                    "events.subscribe did not return subscription_started".to_string(),
                ));
            }
            Ok(value
                .get("subscription_id")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string())
        })
    }

    fn bootstrap(&self, cx: &App) -> Task<Result<HerdrBootstrap, HerdrClientError>> {
        HerdrClientHandle::bootstrap(self, cx)
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
            serde_json::json!({"pane_id": pane_id, "text": text, "keys": keys}),
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
        assert_eq!(payload["subscriptions"].as_array().map(Vec::len), Some(13));
        assert_eq!(payload["subscriptions"][12]["type"], "pane.agent_detected");
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
}
