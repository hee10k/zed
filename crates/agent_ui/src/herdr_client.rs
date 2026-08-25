use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use async_channel;
use futures::channel::oneshot;
use gpui::{App, Task};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::herdr_transport::{HerdrEndpoint, HerdrLineReader, HerdrStream};

#[derive(Debug, Clone)]
pub(crate) enum HerdrClientError {
    Io(String),
    Codec(String),
    ProtocolError { code: i64, message: String },
    Disconnected,
    Timeout,
    EndpointNotFound(String),
    Other(String),
}

impl std::fmt::Display for HerdrClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HerdrClientError::Io(msg) => write!(f, "I/O error: {msg}"),
            HerdrClientError::Codec(msg) => write!(f, "Codec error: {msg}"),
            HerdrClientError::ProtocolError { code, message } => {
                write!(f, "Protocol error ({code}): {message}")
            }
            HerdrClientError::Disconnected => write!(f, "Disconnected"),
            HerdrClientError::Timeout => write!(f, "Timeout"),
            HerdrClientError::EndpointNotFound(ep) => write!(f, "Endpoint not found: {ep}"),
            HerdrClientError::Other(msg) => write!(f, "Other error: {msg}"),
        }
    }
}

impl std::error::Error for HerdrClientError {}

impl From<anyhow::Error> for HerdrClientError {
    fn from(err: anyhow::Error) -> Self {
        HerdrClientError::Other(err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HerdrAgentStatus {
    Idle,
    Running,
    WaitingForInput,
    Errored,
    Stopped,
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct HerdrAgentSessionIdentity {
    pub kind: String,
    pub value: String,
}

impl HerdrAgentSessionIdentity {
    pub(crate) fn id(val: impl Into<String>) -> Self {
        Self {
            kind: "id".to_string(),
            value: val.into(),
        }
    }

    pub(crate) fn path(val: impl Into<String>) -> Self {
        Self {
            kind: "path".to_string(),
            value: val.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HerdrAgentSnapshot {
    pub pane_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub agent_type: Option<String>,
    #[serde(default)]
    pub session_identity: Option<HerdrAgentSessionIdentity>,
    pub status: HerdrAgentStatus,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub last_seen_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HerdrWorkspaceSnapshot {
    pub workspace_id: String,
    pub label: String,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub active_pane_id: Option<String>,
    #[serde(default)]
    pub agents: Vec<HerdrAgentSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HerdrSnapshot {
    pub session: String,
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub active_workspace_id: Option<String>,
    #[serde(default)]
    pub workspaces: Vec<HerdrWorkspaceSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct HerdrRequest {
    pub id: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HerdrResponse {
    pub id: String,
    #[serde(default)]
    pub result: Option<Box<RawValue>>,
    #[serde(default)]
    pub error: Option<HerdrErrorBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HerdrErrorBody {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct HerdrEventEnvelope {
    pub event: String,
    #[serde(default)]
    pub data: Option<Box<RawValue>>,
}

#[derive(Debug, Clone)]
pub(crate) enum HerdrEvent {
    WorkspaceCreated {
        workspace: HerdrWorkspaceSnapshot,
        sequence: u64,
    },
    WorkspaceRenamed {
        workspace_id: String,
        label: String,
        sequence: u64,
    },
    WorkspaceFocused {
        workspace_id: String,
        operation_id: Option<String>,
        sequence: u64,
    },
    WorkspaceClosed {
        workspace_id: String,
        sequence: u64,
    },
    PaneAgentDetected {
        pane_id: String,
        workspace_id: String,
        agent_type: Option<String>,
        session_identity: Option<HerdrAgentSessionIdentity>,
        sequence: u64,
    },
    PaneAgentStatusChanged {
        pane_id: String,
        status: HerdrAgentStatus,
        sequence: u64,
    },
    PaneFocused {
        pane_id: String,
        workspace_id: String,
        operation_id: Option<String>,
        sequence: u64,
    },
    PaneExited {
        pane_id: String,
        exit_code: Option<i32>,
        sequence: u64,
    },
    PaneOutput {
        pane_id: String,
        revision: u64,
        delta: String,
        sequence: u64,
    },
    SubscriptionStarted {
        subscription_id: String,
    },
    Unknown {
        event: String,
        data: Box<RawValue>,
    },
}

impl HerdrEvent {
    pub(crate) fn workspace_id(&self) -> Option<&str> {
        match self {
            HerdrEvent::WorkspaceCreated { workspace, .. } => Some(&workspace.workspace_id),
            HerdrEvent::WorkspaceRenamed { workspace_id, .. } => Some(workspace_id),
            HerdrEvent::WorkspaceFocused { workspace_id, .. } => Some(workspace_id),
            HerdrEvent::WorkspaceClosed { workspace_id, .. } => Some(workspace_id),
            HerdrEvent::PaneAgentDetected { workspace_id, .. } => Some(workspace_id),
            HerdrEvent::PaneFocused { workspace_id, .. } => Some(workspace_id),
            _ => None,
        }
    }

    pub(crate) fn pane_id(&self) -> Option<&str> {
        match self {
            HerdrEvent::PaneAgentDetected { pane_id, .. } => Some(pane_id),
            HerdrEvent::PaneAgentStatusChanged { pane_id, .. } => Some(pane_id),
            HerdrEvent::PaneFocused { pane_id, .. } => Some(pane_id),
            HerdrEvent::PaneExited { pane_id, .. } => Some(pane_id),
            HerdrEvent::PaneOutput { pane_id, .. } => Some(pane_id),
            _ => None,
        }
    }

    pub(crate) fn sequence(&self) -> u64 {
        match self {
            HerdrEvent::WorkspaceCreated { sequence, .. } => *sequence,
            HerdrEvent::WorkspaceRenamed { sequence, .. } => *sequence,
            HerdrEvent::WorkspaceFocused { sequence, .. } => *sequence,
            HerdrEvent::WorkspaceClosed { sequence, .. } => *sequence,
            HerdrEvent::PaneAgentDetected { sequence, .. } => *sequence,
            HerdrEvent::PaneAgentStatusChanged { sequence, .. } => *sequence,
            HerdrEvent::PaneFocused { sequence, .. } => *sequence,
            HerdrEvent::PaneExited { sequence, .. } => *sequence,
            HerdrEvent::PaneOutput { sequence, .. } => *sequence,
            _ => 0,
        }
    }
}

pub(crate) fn decode_response(input: &str) -> Result<HerdrResponse, HerdrClientError> {
    serde_json::from_str(input).map_err(|e| HerdrClientError::Codec(e.to_string()))
}

pub(crate) fn decode_event(input: &str) -> Result<HerdrEvent, HerdrClientError> {
    let envelope: HerdrEventEnvelope = serde_json::from_str(input)
        .map_err(|e| HerdrClientError::Codec(e.to_string()))?;

    let data_str = envelope.data.as_ref().map(|d| d.get()).unwrap_or("{}");

    match envelope.event.as_str() {
        "workspace.created" | "workspace_created" => {
            #[derive(Deserialize)]
            struct WorkspaceCreatedData {
                #[serde(default)]
                workspace: Option<HerdrWorkspaceSnapshot>,
                #[serde(default)]
                sequence: u64,
            }
            if let Ok(parsed) = serde_json::from_str::<WorkspaceCreatedData>(data_str) {
                if let Some(ws) = parsed.workspace {
                    return Ok(HerdrEvent::WorkspaceCreated {
                        workspace: ws,
                        sequence: parsed.sequence,
                    });
                }
            }
            if let Ok(ws) = serde_json::from_str::<HerdrWorkspaceSnapshot>(data_str) {
                return Ok(HerdrEvent::WorkspaceCreated {
                    sequence: ws.agents.first().map(|a| a.last_seen_sequence).unwrap_or(0),
                    workspace: ws,
                });
            }
            Err(HerdrClientError::Codec("Invalid workspace.created data".to_string()))
        }
        "workspace.renamed" | "workspace_renamed" => {
            #[derive(Deserialize)]
            struct WorkspaceRenamedData {
                workspace_id: String,
                label: String,
                #[serde(default)]
                sequence: u64,
            }
            let data: WorkspaceRenamedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::WorkspaceRenamed {
                workspace_id: data.workspace_id,
                label: data.label,
                sequence: data.sequence,
            })
        }
        "workspace.focused" | "workspace_focused" => {
            #[derive(Deserialize)]
            struct WorkspaceFocusedData {
                workspace_id: String,
                #[serde(default)]
                operation_id: Option<String>,
                #[serde(default)]
                sequence: u64,
            }
            let data: WorkspaceFocusedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::WorkspaceFocused {
                workspace_id: data.workspace_id,
                operation_id: data.operation_id,
                sequence: data.sequence,
            })
        }
        "workspace.closed" | "workspace_closed" => {
            #[derive(Deserialize)]
            struct WorkspaceClosedData {
                workspace_id: String,
                #[serde(default)]
                sequence: u64,
            }
            let data: WorkspaceClosedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::WorkspaceClosed {
                workspace_id: data.workspace_id,
                sequence: data.sequence,
            })
        }
        "pane_agent_detected" | "pane.agent_detected" | "agent.detected" | "agent_detected" => {
            #[derive(Deserialize)]
            struct PaneAgentDetectedData {
                pane_id: String,
                workspace_id: String,
                #[serde(default)]
                agent_type: Option<String>,
                #[serde(default)]
                session_identity: Option<HerdrAgentSessionIdentity>,
                #[serde(default)]
                sequence: u64,
            }
            let data: PaneAgentDetectedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::PaneAgentDetected {
                pane_id: data.pane_id,
                workspace_id: data.workspace_id,
                agent_type: data.agent_type,
                session_identity: data.session_identity,
                sequence: data.sequence,
            })
        }
        "pane_agent_status_changed" | "pane.agent_status_changed" | "agent.status_changed" | "agent_status_changed" => {
            #[derive(Deserialize)]
            struct PaneAgentStatusChangedData {
                pane_id: String,
                status: HerdrAgentStatus,
                #[serde(default)]
                sequence: u64,
            }
            let data: PaneAgentStatusChangedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::PaneAgentStatusChanged {
                pane_id: data.pane_id,
                status: data.status,
                sequence: data.sequence,
            })
        }
        "pane_focused" | "pane.focused" => {
            #[derive(Deserialize)]
            struct PaneFocusedData {
                pane_id: String,
                workspace_id: String,
                #[serde(default)]
                operation_id: Option<String>,
                #[serde(default)]
                sequence: u64,
            }
            let data: PaneFocusedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::PaneFocused {
                pane_id: data.pane_id,
                workspace_id: data.workspace_id,
                operation_id: data.operation_id,
                sequence: data.sequence,
            })
        }
        "pane_exited" | "pane.exited" => {
            #[derive(Deserialize)]
            struct PaneExitedData {
                pane_id: String,
                #[serde(default)]
                exit_code: Option<i32>,
                #[serde(default)]
                sequence: u64,
            }
            let data: PaneExitedData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::PaneExited {
                pane_id: data.pane_id,
                exit_code: data.exit_code,
                sequence: data.sequence,
            })
        }
        "pane_output" | "pane.output" => {
            #[derive(Deserialize)]
            struct PaneOutputData {
                pane_id: String,
                #[serde(default)]
                revision: u64,
                #[serde(default)]
                delta: String,
                #[serde(default)]
                sequence: u64,
            }
            let data: PaneOutputData = serde_json::from_str(data_str)
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(HerdrEvent::PaneOutput {
                pane_id: data.pane_id,
                revision: data.revision,
                delta: data.delta,
                sequence: data.sequence,
            })
        }
        "subscription_started" | "subscription.started" => {
            #[derive(Deserialize)]
            struct SubscriptionStartedData {
                #[serde(default)]
                subscription_id: Option<String>,
            }
            let data: SubscriptionStartedData = serde_json::from_str(data_str)
                .unwrap_or(SubscriptionStartedData { subscription_id: None });
            Ok(HerdrEvent::SubscriptionStarted {
                subscription_id: data.subscription_id.unwrap_or_else(|| "default".to_string()),
            })
        }
        other => Ok(HerdrEvent::Unknown {
            event: other.to_string(),
            data: envelope.data.unwrap_or_else(|| RawValue::from_string("{}".to_string()).unwrap()),
        }),
    }
}

pub(crate) struct HerdrClientHandle {
    next_id: AtomicU64,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Box<RawValue>, HerdrClientError>>>>>,
    event_tx: async_channel::Sender<HerdrEvent>,
    event_rx: async_channel::Receiver<HerdrEvent>,
    writer_tx: async_channel::Sender<String>,
}

impl HerdrClientHandle {
    pub(crate) fn new(stream: HerdrStream, cx: &App) -> Result<Self, HerdrClientError> {
        let write_stream = stream.try_clone().map_err(|e| HerdrClientError::Io(e.to_string()))?;

        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<Result<Box<RawValue>, HerdrClientError>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = async_channel::unbounded();
        let (writer_tx, writer_rx) = async_channel::unbounded::<String>();

        let pending_for_reader = pending.clone();
        let event_tx_for_reader = event_tx.clone();

        cx.background_executor().spawn(async move {
            let mut line_reader = HerdrLineReader::new(stream);
            loop {
                match line_reader.read_line() {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Ok(response) = decode_response(&line) {
                            if let Ok(mut map) = pending_for_reader.lock() {
                                if let Some(sender) = map.remove(&response.id) {
                                    if let Some(err) = response.error {
                                        let _ = sender.send(Err(HerdrClientError::ProtocolError {
                                            code: err.code,
                                            message: err.message,
                                        }));
                                    } else {
                                        let res = response
                                            .result
                                            .unwrap_or_else(|| RawValue::from_string("{}".to_string()).unwrap());
                                        let _ = sender.send(Ok(res));
                                    }
                                    continue;
                                }
                            }
                        }
                        if let Ok(event) = decode_event(&line) {
                            let _ = event_tx_for_reader.send(event).await;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            if let Ok(mut map) = pending_for_reader.lock() {
                for (_, sender) in map.drain() {
                    let _ = sender.send(Err(HerdrClientError::Disconnected));
                }
            }
        }).detach();

        cx.background_executor().spawn(async move {
            let mut stream = write_stream;
            while let Ok(line) = writer_rx.recv().await {
                if stream.send_line(&line).is_err() {
                    break;
                }
            }
        }).detach();

        Ok(Self {
            next_id: AtomicU64::new(1),
            pending,
            event_tx,
            event_rx,
            writer_tx,
        })
    }

    pub(crate) fn connect(endpoint: &HerdrEndpoint, cx: &App) -> Result<Self, HerdrClientError> {
        let stream = HerdrStream::connect(endpoint)
            .map_err(|e| HerdrClientError::EndpointNotFound(e.to_string()))?;
        Self::new(stream, cx)
    }

    pub(crate) fn request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
        cx: &App,
    ) -> Task<Result<Box<RawValue>, HerdrClientError>> {
        let req_id = format!("req-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let (tx, rx) = oneshot::channel();

        if let Ok(mut map) = self.pending.lock() {
            map.insert(req_id.clone(), tx);
        }

        let request = HerdrRequest {
            id: req_id,
            method: method.to_string(),
            params,
        };

        let writer_tx = self.writer_tx.clone();
        cx.background_executor().spawn(async move {
            if let Ok(encoded) = serde_json::to_string(&request) {
                let _ = writer_tx.send(encoded).await;
            }
            match rx.await {
                Ok(res) => res,
                Err(_) => Err(HerdrClientError::Disconnected),
            }
        })
    }

    pub(crate) fn subscribe(&self) -> async_channel::Receiver<HerdrEvent> {
        self.event_rx.clone()
    }
}

pub(crate) trait HerdrApi: Send + Sync {
    fn ping(&self, cx: &App) -> Task<Result<(), HerdrClientError>>;
    fn subscribe_events(&self, cx: &App) -> Task<Result<String, HerdrClientError>>;
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
    fn read_pane_output(&self, pane_id: &str, since_revision: Option<u64>, cx: &App) -> Task<Result<(u64, String), HerdrClientError>>;
}

impl HerdrApi for HerdrClientHandle {
    fn ping(&self, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let task = self.request("ping", None, cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn subscribe_events(&self, cx: &App) -> Task<Result<String, HerdrClientError>> {
        let params = serde_json::json!({
            "events": [
                "workspace.created",
                "workspace.renamed",
                "workspace.focused",
                "workspace.closed",
                "pane_agent_detected",
                "pane_agent_status_changed",
                "pane_focused",
                "pane_exited",
                "pane_output"
            ]
        });
        let task = self.request("events.subscribe", Some(params), cx);
        cx.background_executor().spawn(async move {
            let res = task.await?;
            let value: serde_json::Value = serde_json::from_str(res.get())
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            let sub_id = value
                .get("subscription_id")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            Ok(sub_id)
        })
    }

    fn get_snapshot(&self, cx: &App) -> Task<Result<HerdrSnapshot, HerdrClientError>> {
        let task = self.request("session.snapshot", None, cx);
        cx.background_executor().spawn(async move {
            let res = task.await?;
            let snapshot: HerdrSnapshot = serde_json::from_str(res.get())
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(snapshot)
        })
    }

    fn focus_workspace(&self, workspace_id: &str, operation_id: Option<&str>, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "workspace_id": workspace_id,
            "operation_id": operation_id,
        });
        let task = self.request("workspace.focus", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn create_workspace(&self, label: &str, paths: Vec<String>, cx: &App) -> Task<Result<HerdrWorkspaceSnapshot, HerdrClientError>> {
        let params = serde_json::json!({
            "label": label,
            "paths": paths,
        });
        let task = self.request("workspace.create", Some(params), cx);
        cx.background_executor().spawn(async move {
            let res = task.await?;
            let ws: HerdrWorkspaceSnapshot = serde_json::from_str(res.get())
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            Ok(ws)
        })
    }

    fn rename_workspace(&self, workspace_id: &str, label: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "workspace_id": workspace_id,
            "label": label,
        });
        let task = self.request("workspace.rename", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn close_workspace(&self, workspace_id: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "workspace_id": workspace_id,
        });
        let task = self.request("workspace.close", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn focus_pane(&self, pane_id: &str, operation_id: Option<&str>, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "operation_id": operation_id,
        });
        let task = self.request("pane.focus", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn close_pane(&self, pane_id: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
        });
        let task = self.request("pane.close", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn prompt_agent(&self, pane_id: &str, prompt: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "prompt": prompt,
        });
        let task = self.request("agent.prompt", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn send_agent_keys(&self, pane_id: &str, keys: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "keys": keys,
        });
        let task = self.request("agent.send_keys", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn send_pane_keys(&self, pane_id: &str, keys: &str, cx: &App) -> Task<Result<(), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "keys": keys,
        });
        let task = self.request("pane.send_keys", Some(params), cx);
        cx.background_executor().spawn(async move {
            task.await?;
            Ok(())
        })
    }

    fn read_pane_output(&self, pane_id: &str, since_revision: Option<u64>, cx: &App) -> Task<Result<(u64, String), HerdrClientError>> {
        let params = serde_json::json!({
            "pane_id": pane_id,
            "since_revision": since_revision,
        });
        let task = self.request("pane.read", Some(params), cx);
        cx.background_executor().spawn(async move {
            let res = task.await?;
            let val: serde_json::Value = serde_json::from_str(res.get())
                .map_err(|e| HerdrClientError::Codec(e.to_string()))?;
            let revision = val.get("revision").and_then(|v| v.as_u64()).unwrap_or(0);
            let output = val.get("output").and_then(|v| v.as_str()).unwrap_or("").to_string();
            Ok((revision, output))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_success_response_by_request_id() {
        let response = decode_response(
            r#"{"id":"req-1","result":{"type":"pong"}}"#,
        )
        .unwrap();

        assert_eq!(response.id, "req-1");
        assert!(response.error.is_none());
    }

    #[test]
    fn decodes_workspace_focused_subscription_event() {
        let event = decode_event(
            r#"{"event":"workspace.focused","data":{"event":"workspace_focused","workspace_id":"w1"}}"#,
        )
        .unwrap();

        assert_eq!(event.workspace_id(), Some("w1"));
    }

    #[test]
    fn rejects_malformed_json_frame() {
        assert!(decode_response("not-json").is_err());
    }
}
