//! Deterministic Herdr fixtures used by the synchronization verification suite.
//!
//! The fixture speaks the same one-request-per-connection NDJSON protocol as a
//! local Herdr process. Subscription connections stay open and receive events
//! through an explicit command channel, which keeps tests deterministic without
//! sleeping on a real Herdr installation.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};

#[cfg(test)]
use crate::herdr_bridge::{HerdrBridgeEvent, HerdrConnectionStatus, HerdrThreadBridge};
use crate::herdr_client::HerdrSnapshot;
#[cfg(test)]
use crate::herdr_client::{
    HerdrAgentSessionIdentity, HerdrAgentSnapshot, HerdrAgentStatus, HerdrApi, HerdrClientError,
    HerdrClientHandle, HerdrEvent, HerdrWorkspaceSnapshot, decode_pane_read_result, empty_params,
    subscription_params, validate_ping_result,
};
#[cfg(test)]
use crate::herdr_mapping_store::{HerdrMappingKey, HerdrMappingRecord, SessionMappings};
#[cfg(test)]
use crate::herdr_state::{OutboundRequest, ReconciliationAction, reconcile_snapshot};
use crate::herdr_transport::HerdrEndpoint;
#[cfg(test)]
use crate::herdr_transport::{HerdrLineReader, HerdrStream};

#[cfg(test)]
use gpui::TestAppContext;

#[cfg(windows)]
use std::os::windows::io::FromRawHandle;

#[cfg(unix)]
use std::os::unix::net::UnixListener;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RecordedHerdrRequest {
    pub id: String,
    pub method: String,
    pub params: Value,
}

#[derive(Debug)]
enum ServerCommand {
    Frame(String),
    Close,
}

struct Subscription {
    id: String,
    tx: std::sync::mpsc::Sender<ServerCommand>,
}

struct ServerState {
    requests: Mutex<Vec<RecordedHerdrRequest>>,
    responses: Mutex<HashMap<String, VecDeque<Value>>>,
    snapshots: Mutex<VecDeque<HerdrSnapshot>>,
    default_snapshot: Mutex<HerdrSnapshot>,
    pane_outputs: Mutex<HashMap<String, VecDeque<(u64, String)>>>,
    subscriptions: Mutex<Vec<Subscription>>,
    buffered_events: Mutex<VecDeque<String>>,
    next_subscription: AtomicU64,
    next_sequence: AtomicU64,
    accepting: AtomicBool,
}

impl ServerState {
    fn new(snapshot: HerdrSnapshot) -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(VecDeque::new()),
            default_snapshot: Mutex::new(snapshot),
            pane_outputs: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(Vec::new()),
            buffered_events: Mutex::new(VecDeque::new()),
            next_subscription: AtomicU64::new(1),
            next_sequence: AtomicU64::new(1),
            accepting: AtomicBool::new(true),
        }
    }

    fn record(&self, request: RecordedHerdrRequest) {
        self.requests.lock().expect("fixture request lock").push(request);
    }

    fn response_override(&self, method: &str) -> Option<Value> {
        self.responses
            .lock()
            .expect("fixture response lock")
            .get_mut(method)
            .and_then(VecDeque::pop_front)
    }

    fn snapshot(&self) -> HerdrSnapshot {
        self.default_snapshot
            .lock()
            .expect("fixture snapshot lock")
            .clone()
    }
    fn response_for(
        &self,
        request: &RecordedHerdrRequest,
        subscription_id: Option<&str>,
    ) -> Result<Value, (String, String)> {
        if let Some(result) = self.response_override(&request.method) {
            return Ok(result);
        }
        match request.method.as_str() {
            "ping" => Ok(json!({"type": "pong", "version": "0.20.0", "protocol": 20})),
            "events.subscribe" => {
                let Some(subscription_id) = subscription_id else {
                    return Err((
                        "internal_error".to_string(),
                        "fixture subscription response missing allocated ID".to_string(),
                    ));
                };
                Ok(json!({"type": "subscription_started", "subscription_id": subscription_id}))
            }
            "session.snapshot" => {
                let snapshot = self
                    .snapshots
                    .lock()
                    .expect("fixture snapshot sequence lock")
                    .pop_front()
                    .unwrap_or_else(|| self.snapshot());
                Ok(json!({"type": "session_snapshot", "snapshot": snapshot}))
            }
            "pane.read" => Ok(self.pane_read_response(request)),
            "workspace.create" => {
                let params = &request.params;
                let label = params
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or("Fixture Workspace");
                let path = params
                    .get("cwd")
                    .and_then(Value::as_str)
                    .unwrap_or("/fixture");
                Ok(json!({
                    "type": "workspace_created",
                    "workspace": {
                        "workspace_id": "fixture-workspace-created",
                        "label": label,
                        "paths": [path],
                        "focused": false,
                        "number": 0,
                        "pane_count": 0,
                        "tab_count": 0,
                        "active_tab_id": null,
                        "agent_status": "idle"
                    }
                }))
            }
            "workspace.focus"
            | "workspace.rename"
            | "workspace.close"
            | "pane.focus"
            | "pane.close"
            | "pane.send_keys"
            | "pane.send_text"
            | "pane.send_input"
            | "pane.split"
            | "agent.prompt"
            | "agent.send_keys" => Ok(json!({})),
            "agent.rename" | "agent.start" => {
                let pane_id = request
                    .params
                    .get("target")
                    .or_else(|| request.params.get("pane_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("fixture-pane");
                Ok(json!({
                    "agent": {
                        "pane_id": pane_id,
                        "workspace_id": "fixture-workspace",
                        "agent": "omp",
                        "agent_session": {"type": "id", "value": "fixture-agent"},
                        "agent_status": "idle",
                        "title": "Fixture Agent"
                    }
                }))
            }
            _ => Err((
                "method_not_found".to_string(),
                format!("fixture does not implement Herdr method {:?}", request.method),
            )),
        }
    }

    fn pane_read_response(&self, request: &RecordedHerdrRequest) -> Value {
        let pane_id = request
            .params
            .get("pane_id")
            .and_then(Value::as_str)
            .unwrap_or("fixture-pane");
        let (revision, text) = self
            .pane_outputs
            .lock()
            .expect("fixture pane output lock")
            .get_mut(pane_id)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| (0, String::new()));
        json!({
            "type": "pane_read",
            "read": {
                "pane_id": pane_id,
                "source": "recent",
                "format": "text",
                "text": text,
                "revision": revision,
                "truncated": false
            }
        })
    }

    fn remove_subscription(&self, id: &str) {
        self.subscriptions
            .lock()
            .expect("fixture subscription lock")
            .retain(|subscription| subscription.id != id);
    }

    fn send_to_subscriptions(&self, frame: String) {
        let mut subscriptions = self.subscriptions.lock().expect("fixture subscription lock");
        subscriptions.retain(|subscription| {
            subscription
                .tx
                .send(ServerCommand::Frame(frame.clone()))
                .is_ok()
        });
    }
}

struct ServerLifecycle {
    stop: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    #[cfg(windows)]
    endpoint_path: String,
}

/// A real local endpoint backed by a deterministic in-process Herdr protocol
/// server. Clones share server state; the last clone shuts down the listener.
#[derive(Clone)]
pub(crate) struct FakeHerdrServer {
    inner: Arc<ServerState>,
    lifecycle: Arc<ServerLifecycle>,
    endpoint: HerdrEndpoint,
    #[cfg(unix)]
    socket_path: Arc<std::path::PathBuf>,
}

impl FakeHerdrServer {
    pub(crate) fn new(snapshot: HerdrSnapshot) -> Result<Self> {
        let inner = Arc::new(ServerState::new(snapshot));
        let stop = Arc::new(AtomicBool::new(false));

        #[cfg(unix)]
        let (endpoint, socket_path, join) = {
            static NEXT_SOCKET: AtomicU64 = AtomicU64::new(1);
            let socket_path = std::env::temp_dir().join(format!(
                "zed-herdr-fixture-{}-{}.sock",
                std::process::id(),
                NEXT_SOCKET.fetch_add(1, Ordering::SeqCst)
            ));
            let listener = UnixListener::bind(&socket_path)?;
            listener.set_nonblocking(true)?;
            let thread_inner = Arc::clone(&inner);
            let thread_stop = Arc::clone(&stop);
            let join = thread::Builder::new()
                .name("herdr-fixture-listener".to_string())
                .spawn(move || unix_accept_loop(listener, thread_inner, thread_stop))?;
            (
                HerdrEndpoint::Explicit(socket_path.to_string_lossy().into_owned()),
                socket_path,
                join,
            )
        };

        #[cfg(windows)]
        let (endpoint, endpoint_path, join) = {
            static NEXT_PIPE: AtomicU64 = AtomicU64::new(1);
            let endpoint_path = format!(
                "zed-herdr-fixture-{}-{}",
                std::process::id(),
                NEXT_PIPE.fetch_add(1, Ordering::SeqCst)
            );
            let pipe_name = windows_pipe_name(&endpoint_path);
            let thread_inner = Arc::clone(&inner);
            let thread_stop = Arc::clone(&stop);
            let thread_pipe_name = pipe_name.clone();
            let join = thread::Builder::new()
                .name("herdr-fixture-listener".to_string())
                .spawn(move || windows_accept_loop(thread_pipe_name, thread_inner, thread_stop))?;
            (HerdrEndpoint::Explicit(endpoint_path.clone()), endpoint_path, join)
        };

        let lifecycle = Arc::new(ServerLifecycle {
            stop,
            join: Mutex::new(Some(join)),
            #[cfg(windows)]
            endpoint_path,
        });
        Ok(Self {
            inner,
            lifecycle,
            endpoint,
            #[cfg(unix)]
            socket_path: Arc::new(socket_path),
        })
    }

    pub(crate) fn endpoint(&self) -> HerdrEndpoint {
        self.endpoint.clone()
    }

    #[cfg(unix)]
    pub(crate) fn socket_path(&self) -> &std::path::Path {
        self.socket_path.as_path()
    }

    pub(crate) fn set_snapshot(&self, snapshot: HerdrSnapshot) {
        *self
            .inner
            .default_snapshot
            .lock()
            .expect("fixture snapshot lock") = snapshot;
    }

    pub(crate) fn enqueue_snapshot(&self, snapshot: HerdrSnapshot) {
        self.inner
            .snapshots
            .lock()
            .expect("fixture snapshot sequence lock")
            .push_back(snapshot);
    }

    /// Queue a response result for the next request of `method`.
    pub(crate) fn enqueue_response(&self, method: &str, result: Value) {
        self.inner
            .responses
            .lock()
            .expect("fixture response lock")
            .entry(method.to_string())
            .or_default()
            .push_back(result);
    }

    pub(crate) fn set_response(&self, method: &str, result: Value) {
        let mut responses = self.inner.responses.lock().expect("fixture response lock");
        responses.insert(method.to_string(), VecDeque::from([result]));
    }

    pub(crate) fn set_pane_output(&self, pane_id: &str, revision: u64, text: &str) {
        let mut outputs = self
            .inner
            .pane_outputs
            .lock()
            .expect("fixture pane output lock");
        outputs.insert(
            pane_id.to_string(),
            VecDeque::from([(revision, text.to_string())]),
        );
    }

    pub(crate) fn enqueue_pane_output(&self, pane_id: &str, revision: u64, text: &str) {
        self.inner
            .pane_outputs
            .lock()
            .expect("fixture pane output lock")
            .entry(pane_id.to_string())
            .or_default()
            .push_back((revision, text.to_string()));
    }

    pub(crate) fn requests(&self) -> Vec<RecordedHerdrRequest> {
        self.inner
            .requests
            .lock()
            .expect("fixture request lock")
            .clone()
    }

    pub(crate) fn methods(&self) -> Vec<String> {
        self.requests()
            .into_iter()
            .map(|request| request.method)
            .collect()
    }

    pub(crate) fn take_requests(&self) -> Vec<RecordedHerdrRequest> {
        std::mem::take(
            &mut *self
                .inner
                .requests
                .lock()
                .expect("fixture request lock"),
        )
    }

    pub(crate) fn wait_for_method(&self, method: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if self
                .inner
                .requests
                .lock()
                .expect("fixture request lock")
                .iter()
                .any(|request| request.method == method)
            {
                return true;
            }
            thread::yield_now();
        }
        false
    }

    pub(crate) fn next_sequence(&self) -> u64 {
        self.inner.next_sequence.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn set_sequence(&self, sequence: u64) {
        self.inner.next_sequence.store(sequence, Ordering::SeqCst);
    }

    pub(crate) fn event_with_sequence(
        &self,
        event: &str,
        mut data: Value,
        sequence: u64,
    ) -> Value {
        if let Value::Object(object) = &mut data {
            object.insert("sequence".to_string(), Value::from(sequence));
        }
        json!({"event": event, "data": data})
    }

    pub(crate) fn queue_event(&self, event: Value) {
        let frame = serde_json::to_string(&event).expect("fixture event is JSON");
        self.inner
            .buffered_events
            .lock()
            .expect("fixture buffered event lock")
            .push_back(frame);
    }

    pub(crate) fn emit_event(&self, event: Value) {
        let frame = serde_json::to_string(&event).expect("fixture event is JSON");
        let has_subscription = !self
            .inner
            .subscriptions
            .lock()
            .expect("fixture subscription lock")
            .is_empty();
        if has_subscription {
            self.inner.send_to_subscriptions(frame);
        } else {
            self.inner
                .buffered_events
                .lock()
                .expect("fixture buffered event lock")
                .push_back(frame);
        }
    }

    /// Close every established subscription while leaving the listener up.
    pub(crate) fn disconnect(&self) {
        let subscriptions = std::mem::take(
            &mut *self
                .inner
                .subscriptions
                .lock()
                .expect("fixture subscription lock"),
        );
        for subscription in subscriptions {
            let _ = subscription.tx.send(ServerCommand::Close);
        }
    }

    pub(crate) fn disconnect_subscriptions(&self) {
        self.disconnect();
    }

    pub(crate) fn stop_accepting(&self) {
        self.inner.accepting.store(false, Ordering::SeqCst);
    }

    pub(crate) fn reconnect(&self) {
        self.inner.accepting.store(true, Ordering::SeqCst);
    }
}

impl Drop for FakeHerdrServer {
    fn drop(&mut self) {
        if Arc::strong_count(&self.lifecycle) != 1 {
            return;
        }
        self.disconnect();
        self.lifecycle.stop.store(true, Ordering::SeqCst);
        #[cfg(windows)]
        wake_windows_pipe(&self.lifecycle.endpoint_path);
        if let Some(join) = self
            .lifecycle
            .join
            .lock()
            .expect("fixture listener lock")
            .take()
        {
            let _ = join.join();
        }
        #[cfg(unix)]
        {
            let _ = std::fs::remove_file(self.socket_path.as_path());
        }
    }
}

fn write_frame<S: Write>(stream: &mut S, frame: &str) -> Result<()> {
    stream.write_all(frame.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn response_frame(
    id: &str,
    result: Result<Value, (String, String)>,
) -> Value {
    match result {
        Ok(result) => json!({"id": id, "result": result}),
        Err((code, message)) => json!({
            "id": id,
            "error": {"code": code, "message": message}
        }),
    }
}

fn handle_connection<S: Read + Write + Send + 'static>(
    stream: S,
    inner: Arc<ServerState>,
) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let Ok(value) = serde_json::from_str::<Value>(&line) else {
        return;
    };
    let request = RecordedHerdrRequest {
        id: value
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        method: value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        params: value.get("params").cloned().unwrap_or_else(|| json!({})),
    };
    inner.record(request.clone());

    if request.method == "events.subscribe" {
        handle_subscription(reader, inner, request);
        return;
    }

    let response = response_frame(&request.id, inner.response_for(&request, None));
    let _ = write_frame(reader.get_mut(), &response.to_string());
}

fn handle_subscription<S: Read + Write + Send + 'static>(
    mut reader: BufReader<S>,
    inner: Arc<ServerState>,
    request: RecordedHerdrRequest,
) {
    let (subscription_id, rx) = {
        // Allocate the ID while holding the same lock that registers the
        // channel. The response cannot observe a different subscription's
        // counter value under concurrent handshakes.
        let mut subscriptions = inner
            .subscriptions
            .lock()
            .expect("fixture subscription lock");
        let subscription_number = inner.next_subscription.fetch_add(1, Ordering::SeqCst);
        let subscription_id = format!("fixture-sub-{subscription_number}");
        let (tx, rx) = std::sync::mpsc::channel();
        subscriptions.push(Subscription {
            id: subscription_id.clone(),
            tx,
        });
        (subscription_id, rx)
    };
    // Deliberately send buffered events before the acknowledgement. The real
    // client must retain them while completing the subscription handshake.
    let buffered = std::mem::take(
        &mut *inner
            .buffered_events
            .lock()
            .expect("fixture buffered event lock"),
    );
    for frame in buffered {
        if write_frame(reader.get_mut(), &frame).is_err() {
            inner.remove_subscription(&subscription_id);
            return;
        }
    }

    let response = response_frame(
        &request.id,
        inner.response_for(&request, Some(&subscription_id)),
    );
    if write_frame(reader.get_mut(), &response.to_string()).is_err() {
        inner.remove_subscription(&subscription_id);
        return;
    }

    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(ServerCommand::Frame(frame)) => {
                if write_frame(reader.get_mut(), &frame).is_err() {
                    break;
                }
            }
            Ok(ServerCommand::Close) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    inner.remove_subscription(&subscription_id);
}

#[cfg(unix)]
fn unix_accept_loop(
    listener: UnixListener,
    inner: Arc<ServerState>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                if !inner.accepting.load(Ordering::SeqCst) {
                    drop(stream);
                    continue;
                }
                let thread_inner = Arc::clone(&inner);
                let _ = thread::Builder::new()
                    .name("herdr-fixture-connection".to_string())
                    .spawn(move || handle_connection(stream, thread_inner));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::Interrupted
                        | std::io::ErrorKind::ConnectionAborted
                ) => {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        thread::sleep(Duration::from_millis(2));
                    }
                },
            Err(_) => break,
        }
    }
}

#[cfg(windows)]
fn windows_pipe_name(endpoint_path: &str) -> String {
    format!(r"\\.\pipe\{endpoint_path}")
}

#[cfg(windows)]
fn windows_accept_loop(
    pipe_name: String,
    inner: Arc<ServerState>,
    stop: Arc<AtomicBool>,
) {
    use windows::core::HSTRING;
    use windows::Win32::Foundation::{CloseHandle, ERROR_PIPE_CONNECTED};
    use windows::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX;
    use windows::Win32::System::Pipes::{
        ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_TYPE_BYTE, PIPE_WAIT,
    };

    while !stop.load(Ordering::SeqCst) {
        let pipe = unsafe {
            CreateNamedPipeW(
                &HSTRING::from(&pipe_name),
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                8,
                64 * 1024,
                64 * 1024,
                0,
                None,
            )
        };
        if pipe.is_invalid() {
            break;
        }
        let connected = unsafe { ConnectNamedPipe(pipe, None) }
            .map(|_| true)
            .or_else(|error| {
                (error.code() == ERROR_PIPE_CONNECTED.to_hresult()).then_some(true)
            })
            .unwrap_or(false);
        if !connected {
            let _ = unsafe { CloseHandle(pipe) };
            continue;
        }
        if !inner.accepting.load(Ordering::SeqCst) {
            let _ = unsafe { CloseHandle(pipe) };
            continue;
        }
        let thread_inner = Arc::clone(&inner);
        let _ = thread::Builder::new()
            .name("herdr-fixture-connection".to_string())
            .spawn(move || {
                let stream = unsafe { std::fs::File::from_raw_handle(pipe.0 as _) };
                handle_connection(stream, thread_inner);
            });
    }
}

#[cfg(windows)]
fn wake_windows_pipe(endpoint_path: &str) {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use windows::Win32::Foundation::{CloseHandle, GENERIC_READ, GENERIC_WRITE, HANDLE};
    use windows::Win32::Storage::FileSystem::{CreateFileW, FILE_SHARE_NONE, OPEN_EXISTING};

    let pipe_name = windows_pipe_name(endpoint_path);
    let wide: Vec<u16> = OsStr::new(&pipe_name)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    if let Ok(handle) = unsafe {
        CreateFileW(
            windows::core::PCWSTR(wide.as_ptr()),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_NONE,
            None,
            OPEN_EXISTING,
            Default::default(),
            Some(HANDLE::default()),
        )
    } {
        let _ = unsafe { CloseHandle(handle) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::join_all;

    fn snapshot(session: &str, workspaces: Vec<HerdrWorkspaceSnapshot>) -> HerdrSnapshot {
        HerdrSnapshot {
            session: session.to_string(),
            protocol: Some(20),
            workspaces,
            ..Default::default()
        }
    }

    fn workspace(id: &str, label: &str, path: &str) -> HerdrWorkspaceSnapshot {
        HerdrWorkspaceSnapshot {
            workspace_id: id.to_string(),
            label: label.to_string(),
            paths: vec![path.to_string()],
            ..Default::default()
        }
    }

    fn pane(id: &str, workspace_id: &str, identity: &str) -> HerdrAgentSnapshot {
        HerdrAgentSnapshot {
            pane_id: id.to_string(),
            workspace_id: workspace_id.to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id(identity)),
            status: HerdrAgentStatus::Idle,
            ..Default::default()
        }
    }

    #[gpui::test]
    async fn fake_server_records_control_requests_and_revisioned_reads(
        cx: &mut TestAppContext,
    ) {
        cx.executor().allow_parking();
        let server = FakeHerdrServer::new(snapshot("alpha", vec![workspace("w1", "Alpha", "/repo")]))
            .expect("fixture server");
        server.set_pane_output("w1:p1", 7, "screen output");
        server.set_sequence(40);
        assert_eq!(server.next_sequence(), 40);
        assert_eq!(server.next_sequence(), 41);
        server.enqueue_response("workspace.focus", json!({"accepted":true}));
        server.enqueue_pane_output("w1:p1", 8, "newer output");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());

        let ping = client.request_on_executor("ping", empty_params()).await.expect("ping");
        validate_ping_result(ping.get()).expect("ping protocol");

        let methods = [
            ("workspace.focus", json!({"workspace_id":"w1"})),
            ("workspace.create", json!({"label":"Created","cwd":"/created"})),
            ("workspace.rename", json!({"workspace_id":"w1","label":"Renamed"})),
            ("workspace.close", json!({"workspace_id":"w1"})),
            ("pane.focus", json!({"pane_id":"w1:p1"})),
            ("agent.prompt", json!({"target":"w1:p1","text":"hello"})),
            ("agent.send_keys", json!({"target":"w1:p1","keys":["ENTER"]})),
            ("pane.send_keys", json!({"pane_id":"w1:p1","keys":["ENTER"]})),
            ("pane.close", json!({"pane_id":"w1:p1"})),
            ("pane.read", json!({"pane_id":"w1:p1","source":"recent"})),
            ("agent.rename", json!({"target":"w1:p1","name":"Renamed Agent"})),
        ];
        let results = join_all(methods.into_iter().map(|(method, params)| {
            client.request_on_executor(method, params)
        }))
        .await;
        assert!(results.iter().all(Result::is_ok), "all fixture requests succeed");
        let read = results
            .iter()
            .find_map(|result| {
                result.as_ref().ok().filter(|raw| raw.get().contains("pane_read"))
            })
            .expect("pane read result");
        assert_eq!(decode_pane_read_result(read.get()).expect("pane read"), (7, "screen output".into()));
        assert!(
            results.iter().any(|result| {
                result
                    .as_ref()
                    .is_ok_and(|raw| raw.get().contains("\"accepted\":true"))
            }),
            "controlled response sequence must be returned"
        );
        let newer_read = client
            .request_on_executor("pane.read", json!({"pane_id":"w1:p1"}))
            .await
            .expect("newer pane read");
        assert_eq!(
            decode_pane_read_result(newer_read.get()).expect("newer pane read"),
            (8, "newer output".into())
        );

        let recorded_methods = server.methods();
        for method in [
            "ping",
            "workspace.focus",
            "workspace.create",
            "workspace.rename",
            "workspace.close",
            "pane.focus",
            "agent.prompt",
            "agent.send_keys",
            "pane.send_keys",
            "pane.close",
            "pane.read",
            "agent.rename",
        ] {
            assert!(recorded_methods.iter().any(|recorded| recorded == method), "fixture did not record {method}");
        }
    }

    #[gpui::test]
    async fn fake_server_surfaces_unknown_method_as_protocol_error(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let server =
            FakeHerdrServer::new(snapshot("alpha", vec![workspace("w1", "Alpha", "/repo")]))
                .expect("fixture server");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());
        let result = client
            .request_on_executor("unsupported.fixture.method", json!({}))
            .await;
        assert!(matches!(
            result,
            Err(HerdrClientError::ProtocolError { code, message })
                if code == "method_not_found" && message.contains("unsupported.fixture.method")
        ));
    }

    #[gpui::test]
    async fn concurrent_subscriptions_receive_their_atomically_allocated_ids(
        cx: &mut TestAppContext,
    ) {
        cx.executor().allow_parking();
        let server =
            FakeHerdrServer::new(snapshot("alpha", vec![workspace("w1", "Alpha", "/repo")]))
                .expect("fixture server");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());
        let results = join_all((0..8).map(|_| {
            client.start_subscription(subscription_params(), false)
        }))
        .await;
        let mut ids = results
            .into_iter()
            .map(|result| result.expect("subscription").0)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            (1..=8)
                .map(|id| format!("fixture-sub-{id}"))
                .collect::<Vec<_>>()
        );
        client.cancel_subscriptions();
        server.disconnect_subscriptions();
    }

    #[gpui::test]
    async fn fake_server_buffers_events_delivers_eof_and_reconnects(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let server = FakeHerdrServer::new(snapshot("alpha", vec![workspace("w1", "Alpha", "/repo")]))
            .expect("fixture server");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());
        let events = client.subscribe_with_cursor();
        server.queue_event(json!({
            "event": "workspace.focused",
            "data": {"type":"workspace_focused", "workspace_id":"w1", "sequence":1}
        }));

        let (subscription_id, _, _, _) = client
            .start_subscription(subscription_params(), false)
            .await
            .expect("first subscription");
        assert_eq!(subscription_id, "fixture-sub-1");
        let buffered = events.recv().await.expect("buffered event");
        assert!(matches!(buffered.event, HerdrEvent::WorkspaceFocused { workspace_id, .. } if workspace_id == "w1"));

        server.emit_event(json!({
            "event": "pane.scroll_changed",
            "data": {"type":"pane_scroll_changed", "pane_id":"w1:p1", "sequence":2}
        }));
        let pushed = events.recv().await.expect("pushed event");
        assert!(matches!(pushed.event, HerdrEvent::PaneScrollChanged { pane_id, .. } if pane_id == "w1:p1"));

        server.disconnect_subscriptions();
        let ended = events.recv().await.expect("subscription EOF event");
        assert!(matches!(ended.event, HerdrEvent::Unknown { event, .. } if event == "subscription_ended"));

        server.reconnect();
        let (second_id, _, _, _) = client
            .start_subscription(subscription_params(), false)
            .await
            .expect("reconnected subscription");
        assert_eq!(second_id, "fixture-sub-2");
        server.emit_event(json!({
            "event": "workspace.focused",
            "data": {"type":"workspace_focused", "workspace_id":"w1", "sequence":3}
        }));
        let after_reconnect = events.recv().await.expect("reconnected event");
        assert!(matches!(after_reconnect.event, HerdrEvent::WorkspaceFocused { sequence: 3, .. }));
    }

    #[gpui::test]
    async fn bootstrap_subscribes_before_snapshot_and_replays_controlled_sequence(
        cx: &mut TestAppContext,
    ) {
        cx.executor().allow_parking();
        let first = snapshot("alpha", vec![workspace("w1", "Initial", "/repo")]);
        let second = snapshot("alpha", vec![workspace("w1", "Authoritative", "/repo")]);
        let server = FakeHerdrServer::new(first.clone()).expect("fixture server");
        server.enqueue_snapshot(first);
        server.enqueue_snapshot(second);
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());

        let bootstrap = client.bootstrap_on_executor().await.expect("bootstrap");
        assert_eq!(bootstrap.snapshot.workspaces[0].label, "Authoritative");
        let methods = server.methods();
        let subscribe_index = methods
            .iter()
            .position(|method| method == "events.subscribe")
            .expect("primary subscription request");
        let snapshot_index = methods
            .iter()
            .position(|method| method == "session.snapshot")
            .expect("snapshot request");
        assert!(subscribe_index < snapshot_index, "subscription must precede discovery snapshot");
        client.cancel_subscriptions();
        server.disconnect_subscriptions();
    }

    #[test]
    fn focus_round_trip_has_one_request_per_user_action_and_no_reflection_loop() {
        let mut bridge = HerdrThreadBridge::for_test("alpha");
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: workspace("w1", "Alpha", "/repo"),
            sequence: 1,
        });
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: workspace("w2", "Beta", "/repo-b"),
            sequence: 2,
        });
        bridge.take_events();

        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 3,
        });
        assert_eq!(
            bridge
                .take_events()
                .iter()
                .filter(|event| matches!(event, HerdrBridgeEvent::RootFocused { workspace_id, .. } if workspace_id == "w1"))
                .count(),
            1
        );
        assert!(bridge.take_outbound_requests().is_empty());

        let root_request = bridge.focus_root("w2").expect("Zed root activation");
        let root_operation = match root_request {
            OutboundRequest::FocusWorkspace { operation_id, .. } => operation_id,
            other => panic!("unexpected root request: {other:?}"),
        };
        let root_outbound = bridge.take_outbound_requests();
        assert_eq!(root_outbound.len(), 1, "one root activation sends one focus");
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w2".to_string(),
            operation_id: Some(root_operation),
            sequence: 4,
        });
        assert!(bridge.take_outbound_requests().is_empty());
        assert!(bridge
            .take_events()
            .iter()
            .all(|event| !matches!(event, HerdrBridgeEvent::RootFocused { .. })));

        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "w1:p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 5,
        });
        bridge.take_events();
        bridge.apply_event(HerdrEvent::PaneFocused {
            pane_id: "w1:p1".to_string(),
            workspace_id: "w1".to_string(),
            operation_id: None,
            sequence: 6,
        });
        assert_eq!(
            bridge
                .take_events()
                .iter()
                .filter(|event| matches!(event, HerdrBridgeEvent::SubthreadFocused { key, .. } if key.pane_id.as_deref() == Some("w1:p1")))
                .count(),
            1
        );
        assert!(bridge.take_outbound_requests().is_empty());

        let pane_request = bridge.focus_pane("w1", "w1:p1").expect("Zed subthread activation");
        let pane_operation = match pane_request {
            OutboundRequest::FocusPane { operation_id, .. } => operation_id,
            other => panic!("unexpected pane request: {other:?}"),
        };
        assert_eq!(bridge.take_outbound_requests().len(), 1);
        bridge.apply_event(HerdrEvent::PaneFocused {
            pane_id: "w1:p1".to_string(),
            workspace_id: "w1".to_string(),
            operation_id: Some(pane_operation),
            sequence: 7,
        });
        assert!(bridge.take_outbound_requests().is_empty());
        assert!(bridge
            .take_events()
            .iter()
            .all(|event| !matches!(event, HerdrBridgeEvent::SubthreadFocused { .. })));
    }

    #[gpui::test]
    async fn focus_round_trip_reaches_fake_server_once_per_direction(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let server =
            FakeHerdrServer::new(snapshot("alpha", vec![workspace("w1", "Alpha", "/repo")]))
                .expect("fixture server");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());
        let mut bridge = HerdrThreadBridge::for_test("alpha");
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: workspace("w1", "Alpha", "/repo"),
            sequence: 1,
        });
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 2,
        });
        bridge.take_events();

        let root_operation = match bridge.focus_root("w1").expect("root focus") {
            OutboundRequest::FocusWorkspace { operation_id, origin, .. } => {
                assert_eq!(origin, crate::herdr_state::HerdrOperationOrigin::Zed);
                operation_id
            }
            other => panic!("unexpected root request: {other:?}"),
        };
        client
            .request_on_executor(
                "workspace.focus",
                json!({"workspace_id":"w1","operation_id":root_operation,"origin":"zed"}),
            )
            .await
            .expect("workspace focus request");
        bridge.apply_event(HerdrEvent::WorkspaceFocused {
            workspace_id: "w1".to_string(),
            operation_id: Some(root_operation.clone()),
            sequence: 3,
        });

        let pane_operation = match bridge.focus_pane("w1", "p1").expect("pane focus") {
            OutboundRequest::FocusPane { operation_id, origin, .. } => {
                assert_eq!(origin, crate::herdr_state::HerdrOperationOrigin::Zed);
                operation_id
            }
            other => panic!("unexpected pane request: {other:?}"),
        };
        client
            .request_on_executor(
                "pane.focus",
                json!({"pane_id":"p1","operation_id":pane_operation,"origin":"zed"}),
            )
            .await
            .expect("pane focus request");
        bridge.apply_event(HerdrEvent::PaneFocused {
            pane_id: "p1".to_string(),
            workspace_id: "w1".to_string(),
            operation_id: Some(pane_operation.clone()),
            sequence: 4,
        });
        assert!(bridge.take_outbound_requests().len() == 2);

        let requests = server.requests();
        let root_requests = requests
            .iter()
            .filter(|request| request.method == "workspace.focus")
            .collect::<Vec<_>>();
        let pane_requests = requests
            .iter()
            .filter(|request| request.method == "pane.focus")
            .collect::<Vec<_>>();
        assert_eq!(root_requests.len(), 1);
        assert_eq!(pane_requests.len(), 1);
        assert_eq!(root_requests[0].params["origin"], "zed");
        assert_eq!(pane_requests[0].params["origin"], "zed");
        assert_eq!(root_requests[0].params["operation_id"], root_operation);
        assert_eq!(pane_requests[0].params["operation_id"], pane_operation);
    }

    #[test]
    fn lifecycle_reconnect_preserves_threads_rejects_stale_events_and_conflicts_ambiguity() {
        let mut bridge = HerdrThreadBridge::for_test("alpha");
        bridge.apply_event(HerdrEvent::WorkspaceCreated {
            workspace: workspace("w1", "Alpha", "/repo"),
            sequence: 1,
        });
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "w1:p1".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-1")),
            sequence: 2,
        });
        bridge.take_events();
        bridge.apply_event(HerdrEvent::WorkspaceRenamed {
            workspace_id: "w1".to_string(),
            label: "Current".to_string(),
            sequence: 4,
        });
        bridge.apply_event(HerdrEvent::WorkspaceRenamed {
            workspace_id: "w1".to_string(),
            label: "Stale".to_string(),
            sequence: 3,
        });
        assert_eq!(bridge.root_title("w1").as_deref(), Some("Current"));

        // A pane exit archives only the child and does not synthesize any
        // agent cancellation/closure request. The root remains usable.
        bridge.apply_event(HerdrEvent::PaneExited {
            pane_id: "w1:p1".to_string(),
            exit_code: Some(0),
            sequence: 5,
        });
        assert!(bridge.root_thread_id("w1").is_some());
        let focus_while_unavailable = bridge
            .focus_root("w1")
            .expect("ACP root remains usable while Herdr is unavailable");
        assert!(matches!(focus_while_unavailable, OutboundRequest::FocusWorkspace { .. }));
        assert_eq!(
            bridge.take_outbound_requests().len(),
            1,
            "unavailable Herdr must not synthesize cancel/close requests"
        );
        assert!(bridge
            .take_events()
            .iter()
            .any(|event| matches!(event, HerdrBridgeEvent::SubthreadClosed { pane_id, .. } if pane_id == "w1:p1")));

        // Session ID arrival is identity-bearing and creates a selectable
        // subthread; a later close leaves a tombstone rather than resurrecting.
        bridge.apply_event(HerdrEvent::PaneAgentDetected {
            pane_id: "w1:p2".to_string(),
            workspace_id: "w1".to_string(),
            agent_type: Some("omp".to_string()),
            session_identity: Some(HerdrAgentSessionIdentity::id("agent-2")),
            sequence: 6,
        });
        assert_eq!(bridge.subthread_snapshots("w1").len(), 1);
        bridge.apply_event(HerdrEvent::WorkspaceClosed {
            workspace_id: "w1".to_string(),
            sequence: 7,
        });
        assert!(bridge.root_mapping("w1").is_some_and(HerdrMappingRecord::is_tombstone));
        bridge.rebind_session("beta").expect("session rebinding");
        assert_eq!(bridge.session_name(), "beta");
        assert_eq!(bridge.status(), HerdrConnectionStatus::Synchronizing);
        assert!(bridge.root_thread_id("w1").is_none());

        let root = HerdrMappingRecord::root("alpha", "w1", crate::thread_metadata_store::ThreadId::new());
        let mut mappings = SessionMappings::new();
        mappings.insert(root.key.to_key_string(), root);
        for pane_id in ["p1", "p2"] {
            let record = HerdrMappingRecord {
                key: HerdrMappingKey::subthread(
                    "alpha",
                    "w1",
                    pane_id,
                    HerdrAgentSessionIdentity::id("same-agent"),
                ),
                zed_root_thread_id: crate::thread_metadata_store::ThreadId::new(),
                zed_subthread_session_id: None,
                worktree_or_cwd_identity: None,
                last_seen_sequence: 1,
                lifecycle: Default::default(),
            };
            mappings.insert(record.key.to_key_string(), record);
        }
        let actions = reconcile_snapshot(
            "alpha",
            &[HerdrWorkspaceSnapshot {
                workspace_id: "w1".to_string(),
                agents: vec![pane("p3", "w1", "same-agent")],
                ..Default::default()
            }],
            &mappings,
        );
        assert!(actions.iter().any(|action| matches!(action, ReconciliationAction::RecordConflict(_, message) if message.contains("ambiguous"))));

        // No lifecycle path emits an agent.cancel/agent.close request while
        // Herdr is unavailable; only explicit focus actions are outbound.
        assert!(bridge
            .take_outbound_requests()
            .into_iter()
            .all(|request| matches!(request, OutboundRequest::FocusWorkspace { .. } | OutboundRequest::FocusPane { .. })));
    }

    #[cfg(unix)]
    #[test]
    fn unix_fixture_round_trips_ndjson_and_reports_eof() {
        let server = FakeHerdrServer::new(snapshot("alpha", vec![])).expect("fixture server");
        let mut stream = HerdrStream::connect(&server.endpoint()).expect("fixture connect");
        stream
            .send_line(r#"{"id":"fixture-1","method":"ping","params":{}}"#)
            .expect("fixture send");
        let mut reader = HerdrLineReader::new(stream);
        let line = reader.read_line().expect("fixture response").expect("response line");
        let response: Value = serde_json::from_str(&line).expect("response JSON");
        assert_eq!(response["id"], "fixture-1");
        assert_eq!(response["result"]["protocol"], 20);
        assert!(server.socket_path().exists());

        // Normal request connections close after the response, making EOF
        // observable to the real line reader.
        assert_eq!(reader.read_line().expect("fixture EOF"), None);
    }

    #[cfg(windows)]
    #[gpui::test]
    async fn windows_fixture_round_trips_named_pipe_and_reconnects(cx: &mut TestAppContext) {
        cx.executor().allow_parking();
        let server = FakeHerdrServer::new(snapshot("alpha", vec![])).expect("fixture server");
        let client = HerdrClientHandle::new_with_executor(server.endpoint(), cx.executor().clone());
        let ping = client.request_on_executor("ping", empty_params()).await.expect("pipe ping");
        validate_ping_result(ping.get()).expect("pipe protocol");
        let events = client.subscribe_with_cursor();
        let _ = client
            .start_subscription(subscription_params(), false)
            .await
            .expect("pipe subscription");
        server.emit_event(json!({
            "event": "workspace.focused",
            "data": {"type":"workspace_focused", "workspace_id":"w1", "sequence":1}
        }));
        assert!(matches!(events.recv().await.expect("pipe event").event, HerdrEvent::WorkspaceFocused { .. }));
        server.disconnect();
        assert!(matches!(events.recv().await.expect("pipe EOF").event, HerdrEvent::Unknown { event, .. } if event == "subscription_ended"));
        server.reconnect();
        let _ = client
            .start_subscription(subscription_params(), false)
            .await
            .expect("pipe reconnect");
    }
}
