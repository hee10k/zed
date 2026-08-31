use std::io::{self, BufReader, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(windows)]
use std::time::Instant;

#[cfg(unix)]
use interprocess::local_socket::GenericFilePath;
use interprocess::local_socket::prelude::*;
use interprocess::local_socket::{GenericNamespaced, Stream};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
pub const SUBSCRIPTION_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const DEFAULT_PROTOCOL: u32 = 21;
pub const MIN_SUPPORTED_PROTOCOL: u32 = 16;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Endpoint {
    Filesystem(PathBuf),
    Namespaced(String),
}

impl Endpoint {
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::Filesystem(path.into())
    }

    pub fn namespaced(name: impl Into<String>) -> Self {
        Self::Namespaced(name.into())
    }

    pub fn session(name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        #[cfg(windows)]
        {
            return Self::Filesystem(session_socket_path(name));
        }
        #[cfg(not(windows))]
        Self::Filesystem(session_socket_path(name))
    }

    pub fn from_environment() -> Self {
        if let Some(path) = std::env::var_os("HERDR_SOCKET_PATH") {
            if !path.is_empty() {
                return Self::Filesystem(PathBuf::from(path));
            }
        }
        if let Ok(name) = std::env::var("HERDR_SESSION") {
            let name = name.trim();
            if !name.is_empty() {
                return Self::session(name);
            }
        }
        Self::session("default")
    }
}

fn session_socket_path(name: &str) -> PathBuf {
    let config_directory = dirs::home_dir()
        .map(|path| path.join(".config"))
        .or_else(dirs::config_dir)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|path| path.join(".config"))
        })
        .unwrap_or_else(|| PathBuf::from("."));
    let herdr_directory = config_directory.join("herdr");
    if name == "default" {
        herdr_directory.join("herdr.sock")
    } else {
        herdr_directory
            .join("sessions")
            .join(name)
            .join("herdr.sock")
    }
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub endpoint: Endpoint,
    pub request_timeout: Duration,
    pub max_frame_bytes: usize,
}

impl ClientConfig {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new(Endpoint::from_environment())
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("local transport error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid HerdR JSON frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error("HerdR response was empty")]
    EmptyResponse,
    #[error("HerdR response exceeded the {limit}-byte frame limit")]
    FrameTooLarge { limit: usize },
    #[error("HerdR response frame ended before a newline")]
    UnterminatedFrame,
    #[error("HerdR response id {actual:?} did not match request id {expected}")]
    MismatchedResponseId {
        expected: String,
        actual: Option<String>,
    },
    #[error("HerdR request failed ({code}): {message}")]
    Remote {
        code: String,
        message: String,
        data: Option<Value>,
    },
    #[error("HerdR response did not contain a result")]
    MissingResult,
    #[error("HerdR snapshot response had an invalid shape")]
    InvalidSnapshot,
    #[error("HerdR protocol version {protocol} is unsupported; minimum is {minimum}")]
    UnsupportedProtocol { protocol: u32, minimum: u32 },
    #[error("HerdR event had no workspace id")]
    MissingWorkspaceId,
    #[error("invalid checkout path: {0}")]
    InvalidCheckoutPath(String),
}

impl Error {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Io(error) if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock))
    }
}

#[derive(Clone, Debug)]
pub struct HerdRClient {
    config: ClientConfig,
}

impl HerdRClient {
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let client = Self { config };
        let endpoint = client.config.endpoint.clone();
        smol::unblock(move || connect_stream(&endpoint).map(drop)).await?;
        Ok(client)
    }

    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    pub async fn snapshot(&self) -> Result<SessionSnapshot> {
        let client = self.clone();
        smol::unblock(move || client.snapshot_sync()).await
    }

    pub async fn focus_workspace(&self, workspace_id: impl Into<String>) -> Result<()> {
        let client = self.clone();
        let workspace_id = workspace_id.into();
        smol::unblock(move || client.focus_workspace_sync(&workspace_id)).await
    }

    pub async fn subscribe(&self) -> Result<SubscribeStream> {
        let client = self.clone();
        smol::unblock(move || client.subscribe_sync()).await
    }

    fn snapshot_sync(&self) -> Result<SessionSnapshot> {
        let result = self.request_sync("session.snapshot", serde_json::json!({}))?;
        let snapshot = result
            .get("snapshot")
            .cloned()
            .ok_or(Error::InvalidSnapshot)?;
        let snapshot: SessionSnapshot = serde_json::from_value(snapshot)?;
        if snapshot.protocol < MIN_SUPPORTED_PROTOCOL {
            return Err(Error::UnsupportedProtocol {
                protocol: snapshot.protocol,
                minimum: MIN_SUPPORTED_PROTOCOL,
            });
        }
        Ok(snapshot)
    }

    fn focus_workspace_sync(&self, workspace_id: &str) -> Result<()> {
        self.request_sync(
            "workspace.focus",
            serde_json::json!({"workspace_id": workspace_id}),
        )?;
        Ok(())
    }

    fn subscribe_sync(&self) -> Result<SubscribeStream> {
        let mut stream = connect_stream(&self.config.endpoint)?;
        set_request_timeouts(&stream, self.config.request_timeout)?;
        let request_id = next_request_id();
        write_request(
            &mut stream,
            &request_id,
            "events.subscribe",
            serde_json::json!({
                "subscriptions": [{"type": "workspace.focused"}]
            }),
        )?;
        let mut reader = BufReader::new(stream);
        let response = read_json_frame_with_timeout(
            &mut reader,
            self.config.max_frame_bytes,
            self.config.request_timeout,
        )?;
        parse_response(response, &request_id)?;
        configure_subscription_reader(reader.get_mut())?;
        Ok(SubscribeStream {
            reader: Arc::new(Mutex::new(reader)),
            cancelled: Arc::new(AtomicBool::new(false)),
            max_frame_bytes: self.config.max_frame_bytes,
        })
    }

    fn request_sync(&self, method: &str, params: Value) -> Result<Value> {
        let mut stream = connect_stream(&self.config.endpoint)?;
        set_request_timeouts(&stream, self.config.request_timeout)?;
        let request_id = next_request_id();
        write_request(&mut stream, &request_id, method, params)?;
        let mut reader = BufReader::new(stream);
        let response = read_json_frame_with_timeout(
            &mut reader,
            self.config.max_frame_bytes,
            self.config.request_timeout,
        )?;
        parse_response(response, &request_id)
    }
}

pub struct SubscribeStream {
    reader: Arc<Mutex<BufReader<Stream>>>,
    cancelled: Arc<AtomicBool>,
    max_frame_bytes: usize,
}

impl SubscribeStream {
    pub async fn next(&mut self) -> Result<Option<FocusEvent>> {
        let reader = Arc::clone(&self.reader);
        let cancelled = Arc::clone(&self.cancelled);
        let max_frame_bytes = self.max_frame_bytes;
        smol::unblock(move || {
            loop {
                if cancelled.load(Ordering::Acquire) {
                    return Ok(None);
                }
                let mut reader = reader
                    .lock()
                    .map_err(|_| io::Error::other("HerdR subscription reader was poisoned"))?;
                match read_subscription_frame(&mut *reader, max_frame_bytes, &cancelled) {
                    Ok(None) => return Ok(None),
                    Ok(Some(frame)) if frame.is_empty() => continue,
                    Ok(Some(frame)) => {
                        let value = serde_json::from_slice::<Value>(&frame)?;
                        if let Some(event) = parse_focus_event(value)? {
                            return Ok(Some(event));
                        }
                    }
                    Err(error) if error.is_timeout() => continue,
                    Err(error) => return Err(error),
                }
            }
        })
        .await
    }
}

impl Drop for SubscribeStream {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SessionSnapshot {
    pub version: String,
    pub protocol: u32,
    pub focused_workspace_id: Option<String>,
    #[serde(default)]
    pub focused_tab_id: Option<String>,
    #[serde(default)]
    pub focused_pane_id: Option<String>,
    #[serde(default)]
    pub workspaces: Vec<WorkspaceInfo>,
    #[serde(default)]
    pub tabs: Vec<Value>,
    #[serde(default)]
    pub panes: Vec<Value>,
    #[serde(default)]
    pub layouts: Vec<Value>,
    #[serde(default)]
    pub agents: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WorkspaceInfo {
    pub workspace_id: String,
    pub number: u64,
    pub label: String,
    pub focused: bool,
    pub pane_count: u64,
    pub tab_count: u64,
    pub active_tab_id: Option<String>,
    pub agent_status: String,
    #[serde(default)]
    pub worktree: Option<WorkspaceWorktreeInfo>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WorkspaceWorktreeInfo {
    pub checkout_path: String,
    #[serde(default)]
    pub repo_root: Option<String>,
    #[serde(default)]
    pub repo_key: Option<String>,
    #[serde(default)]
    pub repo_name: Option<String>,
    #[serde(default)]
    pub is_linked_worktree: bool,
}

impl WorkspaceInfo {
    pub fn checkout_path(&self) -> Option<&Path> {
        self.worktree
            .as_ref()
            .map(|worktree| Path::new(worktree.checkout_path.as_str()))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct FocusEvent {
    pub workspace_id: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CanonicalPath(String);

impl CanonicalPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CanonicalPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

pub fn canonical_checkout_path(path: &Path) -> Result<CanonicalPath, Error> {
    if !path.is_absolute() {
        return Err(Error::InvalidCheckoutPath(format!(
            "path is not absolute: {}",
            path.display()
        )));
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }

    let mut value = normalized.to_string_lossy().into_owned();
    #[cfg(windows)]
    {
        value = value.replace('/', "\\").to_ascii_lowercase();
        if value.starts_with(r"\\wsl$\") || value.starts_with(r"\\wsl.localhost\") {
            return Err(Error::InvalidCheckoutPath(
                "WSL checkout paths are unsupported".to_owned(),
            ));
        }
        while value.len() > 3 && value.ends_with('\\') {
            value.pop();
        }
    }
    #[cfg(not(windows))]
    {
        while value.len() > 1 && value.ends_with('/') {
            value.pop();
        }
    }
    Ok(CanonicalPath(value))
}

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Generation(u64);

impl Generation {
    pub fn current(self) -> u64 {
        self.0
    }

    pub fn advance(&mut self) -> Self {
        self.0 = self.0.saturating_add(1);
        *self
    }

    pub fn matches(self, other: Self) -> bool {
        self == other
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Deserialize)]
struct WireResponse {
    id: Option<String>,
    result: Option<Value>,
    error: Option<WireError>,
}

#[derive(Debug, Deserialize)]
struct WireError {
    #[serde(default)]
    code: Option<Value>,
    message: String,
    #[serde(default)]
    data: Option<Value>,
}

fn next_request_id() -> String {
    format!(
        "zed-herdr:{}",
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn connect_stream(endpoint: &Endpoint) -> Result<Stream> {
    match endpoint {
        Endpoint::Filesystem(path) => {
            #[cfg(unix)]
            {
                let name = path.as_os_str().to_fs_name::<GenericFilePath>()?;
                return Ok(Stream::connect(name)?);
            }
            #[cfg(windows)]
            {
                let value = path.to_string_lossy().into_owned();
                let name = value.to_ns_name::<GenericNamespaced>()?;
                return Ok(Stream::connect(name)?);
            }
            #[cfg(not(any(unix, windows)))]
            {
                let _ = path;
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "HerdR transport is unsupported on this platform",
                ))?
            }
        }
        Endpoint::Namespaced(value) => {
            let name = value.as_str().to_ns_name::<GenericNamespaced>()?;
            Ok(Stream::connect(name)?)
        }
    }
}

fn set_request_timeouts(stream: &Stream, timeout: Duration) -> Result<()> {
    tolerate_unsupported_timeout(stream.set_send_timeout(Some(timeout)))?;
    tolerate_unsupported_timeout(stream.set_recv_timeout(Some(timeout)))?;
    Ok(())
}

fn configure_subscription_reader(stream: &Stream) -> Result<()> {
    #[cfg(windows)]
    {
        let _ = stream;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        stream.set_recv_timeout(Some(SUBSCRIPTION_POLL_INTERVAL))?;
        Ok(())
    }
}

fn tolerate_unsupported_timeout(result: io::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if cfg!(windows) && error.kind() == io::ErrorKind::Unsupported => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn read_subscription_frame(
    reader: &mut BufReader<Stream>,
    max_frame_bytes: usize,
    cancelled: &AtomicBool,
) -> Result<Option<Vec<u8>>> {
    #[cfg(windows)]
    {
        return read_named_pipe_frame(reader, max_frame_bytes, cancelled);
    }
    #[cfg(not(windows))]
    {
        read_unix_subscription_frame(reader, max_frame_bytes, cancelled)
    }
}

#[cfg(not(windows))]
fn read_unix_subscription_frame(
    reader: &mut BufReader<Stream>,
    max_frame_bytes: usize,
    cancelled: &AtomicBool,
) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::with_capacity(max_frame_bytes.min(4096));
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut byte = [0; 1];
        match reader.read(&mut byte) {
            Ok(0) => {
                if frame.is_empty() {
                    return Ok(None);
                }
                return Err(Error::UnterminatedFrame);
            }
            Ok(_) if byte[0] == b'\n' => break,
            Ok(_) => {
                frame.push(byte[0]);
                if frame.len() > max_frame_bytes {
                    return Err(Error::FrameTooLarge {
                        limit: max_frame_bytes,
                    });
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                continue;
            }
            Err(error) => return Err(error.into()),
        }
    }
    if frame.last() == Some(&b'\r') {
        frame.pop();
    }
    if frame.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(frame))
}

#[cfg(windows)]
fn read_named_pipe_frame(
    reader: &mut BufReader<Stream>,
    max_frame_bytes: usize,
    cancelled: &AtomicBool,
) -> Result<Option<Vec<u8>>> {
    read_named_pipe_frame_until(reader, max_frame_bytes, cancelled, None)
}

#[cfg(windows)]
fn read_named_pipe_frame_until(
    reader: &mut BufReader<Stream>,
    max_frame_bytes: usize,
    cancelled: &AtomicBool,
    deadline: Option<Instant>,
) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::with_capacity(max_frame_bytes.min(4096));
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(None);
        }
        if reader.buffer().is_empty() && !named_pipe_has_data(reader.get_mut())? {
            if let Some(deadline) = deadline {
                let now = Instant::now();
                if now >= deadline {
                    return Err(Error::Io(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for HerdR response",
                    )));
                }
                std::thread::sleep((deadline - now).min(SUBSCRIPTION_POLL_INTERVAL));
            } else {
                std::thread::sleep(SUBSCRIPTION_POLL_INTERVAL);
            }
            continue;
        }

        let mut byte = [0; 1];
        let read = reader.read(&mut byte)?;
        if read == 0 {
            if frame.is_empty() {
                return Ok(None);
            }
            return Err(Error::UnterminatedFrame);
        }
        if byte[0] == b'\n' {
            break;
        }
        frame.push(byte[0]);
        if frame.len() > max_frame_bytes {
            return Err(Error::FrameTooLarge {
                limit: max_frame_bytes,
            });
        }
    }
    if frame.last() == Some(&b'\r') {
        frame.pop();
    }
    if frame.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(frame))
}

#[cfg(windows)]
fn named_pipe_has_data(stream: &mut Stream) -> Result<bool> {
    use std::os::windows::io::{AsHandle, AsRawHandle};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::Pipes::PeekNamedPipe;

    let Stream::NamedPipe(pipe) = stream;
    let handle = HANDLE(pipe.as_handle().as_raw_handle() as *mut std::ffi::c_void);
    let mut available = 0;
    unsafe {
        PeekNamedPipe(handle, None, 0, None, Some(&mut available), None)
            .map(|()| available > 0)
            .map_err(|error| io::Error::from_raw_os_error(error.code().0).into())
    }
}

fn write_request(stream: &mut Stream, id: &str, method: &str, params: Value) -> Result<()> {
    let request = serde_json::json!({
        "id": id,
        "method": method,
        "params": params,
    });
    let frame = serde_json::to_vec(&request)?;
    stream.write_all(&frame)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    Ok(())
}

fn read_json_frame_with_timeout(
    reader: &mut BufReader<Stream>,
    max_frame_bytes: usize,
    timeout: Duration,
) -> Result<Value> {
    #[cfg(windows)]
    {
        let cancelled = AtomicBool::new(false);
        let frame = read_named_pipe_frame_until(
            reader,
            max_frame_bytes,
            &cancelled,
            Some(Instant::now() + timeout),
        )?
        .ok_or(Error::EmptyResponse)?;
        if frame.is_empty() {
            return Err(Error::EmptyResponse);
        }
        return Ok(serde_json::from_slice(&frame)?);
    }
    #[cfg(not(windows))]
    {
        let _ = timeout;
        read_json_frame(reader, max_frame_bytes)
    }
}

#[cfg(not(windows))]
fn read_json_frame<R: Read>(reader: &mut R, max_frame_bytes: usize) -> Result<Value> {
    let frame = read_frame(reader, max_frame_bytes)?.ok_or(Error::EmptyResponse)?;
    if frame.is_empty() {
        return Err(Error::EmptyResponse);
    }
    Ok(serde_json::from_slice(&frame)?)
}
#[cfg(any(not(windows), test))]

fn read_frame<R: Read>(reader: &mut R, max_frame_bytes: usize) -> Result<Option<Vec<u8>>> {
    let mut frame = Vec::with_capacity(max_frame_bytes.min(4096));
    loop {
        let mut byte = [0; 1];
        let read = reader.read(&mut byte)?;
        if read == 0 {
            if frame.is_empty() {
                return Ok(None);
            }
            return Err(Error::UnterminatedFrame);
        }
        if byte[0] == b'\n' {
            break;
        }
        frame.push(byte[0]);
        if frame.len() > max_frame_bytes {
            return Err(Error::FrameTooLarge {
                limit: max_frame_bytes,
            });
        }
    }
    if frame.last() == Some(&b'\r') {
        frame.pop();
    }
    if frame.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(frame))
}

fn parse_response(value: Value, expected_id: &str) -> Result<Value> {
    let response: WireResponse = serde_json::from_value(value)?;
    if response.id.as_deref() != Some(expected_id) {
        return Err(Error::MismatchedResponseId {
            expected: expected_id.to_owned(),
            actual: response.id,
        });
    }
    if let Some(error) = response.error {
        return Err(Error::Remote {
            code: error
                .code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            message: error.message,
            data: error.data,
        });
    }
    response.result.ok_or(Error::MissingResult)
}

fn parse_focus_event(value: Value) -> Result<Option<FocusEvent>> {
    let event_name = value.get("event").and_then(Value::as_str);
    if event_name != Some("workspace_focused") {
        return Ok(None);
    }
    let workspace_id = value
        .get("data")
        .and_then(|data| data.get("workspace_id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("workspace_id").and_then(Value::as_str))
        .ok_or(Error::MissingWorkspaceId)?;
    Ok(Some(FocusEvent {
        workspace_id: workspace_id.to_owned(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_session_snapshot_result() {
        let response = serde_json::json!({
            "id": "request-1",
            "result": {
                "type": "session_snapshot",
                "snapshot": {
                    "version": "1.0.0",
                    "protocol": DEFAULT_PROTOCOL,
                    "focused_workspace_id": "workspace-1",
                    "workspaces": [{
                        "workspace_id": "workspace-1",
                        "number": 1,
                        "label": "main",
                        "focused": true,
                        "pane_count": 1,
                        "tab_count": 1,
                        "active_tab_id": "tab-1",
                        "agent_status": "idle",
                        "worktree": {
                            "checkout_path": "/worktree",
                            "repo_root": "/repo",
                            "repo_key": "repo",
                            "repo_name": "repo",
                            "is_linked_worktree": false
                        }
                    }]
                }
            }
        });
        let result = parse_response(response, "request-1").expect("response result");
        let snapshot: SessionSnapshot =
            serde_json::from_value(result.get("snapshot").cloned().expect("snapshot result"))
                .expect("snapshot");
        assert_eq!(
            snapshot.focused_workspace_id.as_deref(),
            Some("workspace-1")
        );
        let worktree = snapshot.workspaces[0].worktree.as_ref().expect("worktree");
        assert_eq!(worktree.checkout_path, "/worktree");
    }

    #[test]
    fn parses_workspace_focus_event_and_ignores_other_events() {
        let event = parse_focus_event(serde_json::json!({
            "event": "workspace_focused",
            "data": {"workspace_id": "workspace-2"}
        }))
        .expect("focus event")
        .expect("workspace focus");
        assert_eq!(event.workspace_id, "workspace-2");
        assert_eq!(
            parse_focus_event(serde_json::json!({"event": "pane_focused", "data": {}}))
                .expect("event"),
            None
        );
    }

    #[test]
    fn rejects_frames_over_limit() {
        let mut input = &b"12345\n"[..];
        assert!(matches!(
            read_frame(&mut input, 4),
            Err(Error::FrameTooLarge { limit: 4 })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn canonicalizes_absolute_checkout_paths_lexically() {
        let path =
            canonical_checkout_path(Path::new("/repo/./worktree/../main/")).expect("absolute path");
        assert_eq!(path.as_str(), "/repo/main");
    }
    #[test]
    fn resolves_named_session_socket_in_herdr_config_layout() {
        let endpoint = Endpoint::session("named");
        if let Endpoint::Filesystem(path) = endpoint {
            assert!(path.ends_with(Path::new(".config/herdr/sessions/named/herdr.sock")));
        } else {
            assert!(false, "session endpoints must use filesystem paths");
        }
    }

    #[test]
    fn generation_advances_without_losing_equality() {
        let mut generation = Generation::default();
        let first = generation.advance();
        let second = generation.advance();
        assert!(second.current() > first.current());
        assert!(second.matches(generation));
        assert!(!first.matches(second));
    }
}
