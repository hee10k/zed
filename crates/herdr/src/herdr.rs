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

/// `herdr session list --json` result: the authoritative session catalog.
/// Endpoint paths come from here; the UI never guesses the config layout.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionList {
    #[serde(default)]
    pub sessions: Vec<SessionInfo>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionInfo {
    pub name: String,
    #[serde(rename = "default")]
    pub is_default: bool,
    pub running: bool,
    pub session_dir: PathBuf,
    pub socket_path: PathBuf,
}

impl SessionInfo {
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::Filesystem(self.socket_path.clone())
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
    #[error("herdr session list exited unsuccessfully: {message}")]
    SessionListCommand { message: String },
    #[error("herdr rejected the session name: {message}")]
    SessionNameRejected { message: String },
}

impl Error {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Io(error) if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock))
    }
}

/// Lists herdr sessions by running the CLI's own `session list --json`
/// subcommand. The catalog is authoritative; endpoint paths come from here.
pub async fn list_sessions(program: PathBuf) -> Result<Vec<SessionInfo>> {
    smol::unblock(move || {
        let output = std::process::Command::new(program)
            .args(["session", "list", "--json"])
            .output()?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(Error::SessionListCommand {
                message: if message.is_empty() {
                    "herdr session list exited unsuccessfully".to_owned()
                } else {
                    message
                },
            });
        }
        let list: SessionList = serde_json::from_slice(&output.stdout)?;
        Ok(list.sessions)
    })
    .await
}

/// Validates a new session name with the CLI's own parser without starting a
/// session: `herdr` checks `--session` in `configure_from_args` before the
/// side-effect-free `session list` subcommand runs, so a rejected name yields
/// one session-specific stderr line with a non-zero exit and creates no
/// session directory. Zed therefore neither copies a version-sensitive name
/// grammar nor starts a partial session.
pub async fn validate_session_name(program: PathBuf, name: String) -> Result<()> {
    smol::unblock(move || {
        let output = std::process::Command::new(program)
            .arg("--session")
            .arg(&name)
            .args(["session", "list", "--json"])
            .output()?;
        if !output.status.success() {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            return Err(Error::SessionNameRejected {
                message: if message.is_empty() {
                    "herdr rejected the session name".to_owned()
                } else {
                    message
                },
            });
        }
        Ok(())
    })
    .await
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

    /// Fetches the current full pane info for a pane id.
    pub async fn pane(&self, pane_id: impl Into<String>) -> Result<PaneInfo> {
        let client = self.clone();
        let pane_id = pane_id.into();
        smol::unblock(move || client.pane_sync(&pane_id)).await
    }

    /// Focuses the agent attached to a pane.
    pub async fn focus_agent(&self, pane_id: impl Into<String>) -> Result<()> {
        let client = self.clone();
        let pane_id = pane_id.into();
        smol::unblock(move || client.focus_agent_sync(&pane_id)).await
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

    fn pane_sync(&self, pane_id: &str) -> Result<PaneInfo> {
        let result = self.request_sync("pane.get", serde_json::json!({"pane_id": pane_id}))?;
        parse_pane_result(result)
    }

    fn focus_agent_sync(&self, pane_id: &str) -> Result<()> {
        self.request_sync("agent.focus", serde_json::json!({"target": pane_id}))?;
        Ok(())
    }

    fn subscribe_sync(&self) -> Result<SubscribeStream> {
        let mut stream = connect_stream(&self.config.endpoint)?;
        set_request_timeouts(&stream, self.config.request_timeout)?;
        let request_id = next_request_id();
        let subscriptions: Vec<Value> = EVENT_TYPES
            .iter()
            .map(|event_type| serde_json::json!({"type": event_type}))
            .collect();
        write_request(
            &mut stream,
            &request_id,
            "events.subscribe",
            serde_json::json!({ "subscriptions": subscriptions }),
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
    pub async fn next(&mut self) -> Result<Option<HerdrEvent>> {
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
                        if let Some(event) = parse_event(value)? {
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
    pub panes: Vec<PaneInfo>,
    #[serde(default)]
    pub layouts: Vec<Value>,
    #[serde(default)]
    pub agents: Vec<AgentInfo>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AgentSessionKind {
    Id,
    Path,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AgentSessionInfo {
    pub agent: String,
    pub kind: AgentSessionKind,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct PaneInfo {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub terminal_id: String,
    pub focused: bool,
    pub revision: u64,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub agent_status: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub foreground_cwd: Option<String>,
    #[serde(default)]
    pub agent_session: Option<AgentSessionInfo>,
}

/// A mirrored herdr agent is identified by its stable terminal id; the pane
/// id is the current attach/focus target.
pub type AgentInfo = PaneInfo;

/// Typed event stream envelope. Mirrors herdr's `EventEnvelope` + `EventData`:
/// `event` is the snake_case kind, `data` is the tagged payload.
#[derive(Clone, Debug, PartialEq)]
pub enum HerdrEvent {
    Workspace(WorkspaceEvent),
    Pane(PaneEvent),
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkspaceEvent {
    Created(WorkspaceInfo),
    Updated(WorkspaceInfo),
    Closed { workspace_id: String },
    Focused { workspace_id: String },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PaneEvent {
    pub kind: PaneEventKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PaneEventKind {
    Created(PaneInfo),
    Updated(PaneInfo),
    Closed { pane_id: String, workspace_id: String },
    Focused { pane_id: String, workspace_id: String },
    Moved { previous_pane_id: String, pane: PaneInfo },
    Exited { pane_id: String, workspace_id: String },
    AgentDetected { pane_id: String, workspace_id: String },
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

/// The fixed multi-event subscription for a herdr session stream.
///
/// `pane.agent_status_changed` is deliberately absent: in herdr 0.8.2 that
/// subscription requires a `pane_id`, so an unscoped entry fails the whole
/// `events.subscribe` request and the event stream never starts. Status is
/// not consumed by any mirror decision.
const EVENT_TYPES: &[&str] = &[
    "workspace.created",
    "workspace.updated",
    "workspace.closed",
    "workspace.focused",
    "pane.created",
    "pane.updated",
    "pane.closed",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
];

fn parse_pane_result(result: Value) -> Result<PaneInfo> {
    let pane = result.get("pane").cloned().ok_or(Error::MissingResult)?;
    Ok(serde_json::from_value(pane)?)
}

fn event_id(data: &Value, field: &str) -> Result<String> {
    data.get(field)
        .and_then(Value::as_str)
        .map(String::from)
        .ok_or(Error::MissingWorkspaceId)
}

/// Parses one subscription frame: the envelope `{"event": "<snake_case>",
/// "data": …}`. Unknown event kinds are skipped so the stream can carry
/// future event types without breaking the consumer.
fn parse_event(value: Value) -> Result<Option<HerdrEvent>> {
    let event_name = match value.get("event").and_then(Value::as_str) {
        Some(name) => name,
        None => return Ok(None),
    };
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let event = match event_name {
        "workspace_created" => {
            HerdrEvent::Workspace(WorkspaceEvent::Created(serde_json::from_value(data)?))
        }
        "workspace_updated" => {
            HerdrEvent::Workspace(WorkspaceEvent::Updated(serde_json::from_value(data)?))
        }
        "workspace_closed" => HerdrEvent::Workspace(WorkspaceEvent::Closed {
            workspace_id: event_id(&data, "workspace_id")?,
        }),
        "workspace_focused" => HerdrEvent::Workspace(WorkspaceEvent::Focused {
            workspace_id: event_id(&data, "workspace_id")?,
        }),
        "pane_created" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Created(serde_json::from_value(data)?),
        }),
        "pane_updated" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Updated(serde_json::from_value(data)?),
        }),
        "pane_closed" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Closed {
                pane_id: event_id(&data, "pane_id")?,
                workspace_id: event_id(&data, "workspace_id")?,
            },
        }),
        "pane_focused" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Focused {
                pane_id: event_id(&data, "pane_id")?,
                workspace_id: event_id(&data, "workspace_id")?,
            },
        }),
        "pane_moved" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Moved {
                previous_pane_id: event_id(&data, "previous_pane_id")?,
                pane: serde_json::from_value(
                    data.get("pane").cloned().unwrap_or(Value::Null),
                )?,
            },
        }),
        "pane_exited" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Exited {
                pane_id: event_id(&data, "pane_id")?,
                workspace_id: event_id(&data, "workspace_id")?,
            },
        }),
        "pane_agent_detected" => HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::AgentDetected {
                pane_id: event_id(&data, "pane_id")?,
                workspace_id: event_id(&data, "workspace_id")?,
            },
        }),
        _ => return Ok(None),
    };
    Ok(Some(event))
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
        let event = parse_event(serde_json::json!({
            "event": "workspace_focused",
            "data": {"workspace_id": "workspace-2"}
        }))
        .expect("focus event")
        .expect("workspace focus");
        assert_eq!(
            event,
            HerdrEvent::Workspace(WorkspaceEvent::Focused {
                workspace_id: "workspace-2".to_owned()
            })
        );
        assert_eq!(
            parse_event(serde_json::json!({"event": "space_created", "data": {}})).expect("event"),
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

    #[test]
    fn parses_cli_session_list_with_reported_windows_socket() {
        let list: SessionList = serde_json::from_str(
            r#"{"sessions":[{"name":"main","default":false,"running":true,"session_dir":"C:\\Users\\me\\AppData\\Roaming\\herdr\\sessions\\main","socket_path":"C:\\Users\\me\\AppData\\Roaming\\herdr\\sessions\\main\\herdr.sock"}]}"#,
        )
        .expect("session list");

        let session = &list.sessions[0];
        assert_eq!(session.name, "main");
        assert!(session.running);
        assert_eq!(
            session.endpoint(),
            Endpoint::Filesystem(PathBuf::from(
                r"C:\Users\me\AppData\Roaming\herdr\sessions\main\herdr.sock"
            ))
        );
    }

    #[test]
    fn parses_pane_identity_and_revision() {
        let pane: PaneInfo = serde_json::from_value(serde_json::json!({
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "pane_id": "pane-7",
            "terminal_id": "terminal-stable",
            "focused": true,
            "revision": 9,
            "agent": "claude",
            "agent_status": "working",
            "cwd": "/repo/worktree",
            "foreground_cwd": "/repo/worktree/src",
            "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
        }))
        .expect("pane");

        assert_eq!(pane.terminal_id, "terminal-stable");
        assert_eq!(pane.pane_id, "pane-7");
        assert_eq!(pane.revision, 9);
    }

    fn pane_payload(
        workspace_id: &str,
        tab_id: &str,
        pane_id: &str,
        revision: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "workspace_id": workspace_id,
            "tab_id": tab_id,
            "pane_id": pane_id,
            "terminal_id": "terminal-stable",
            "focused": true,
            "revision": revision,
            "agent": "claude",
            "agent_status": "working",
            "cwd": "/repo/worktree",
            "foreground_cwd": "/repo/worktree/src",
            "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
        })
    }

    #[test]
    fn parses_herdr_event_kinds_from_subscription_frames() {
        let Some(HerdrEvent::Workspace(WorkspaceEvent::Created(workspace))) =
            parse_event(serde_json::json!({
                "event": "workspace_created",
                "data": {
                    "workspace_id": "workspace-1",
                    "number": 1,
                    "label": "main",
                    "focused": true,
                    "pane_count": 1,
                    "tab_count": 1,
                    "active_tab_id": "tab-1",
                    "agent_status": "idle"
                }
            }))
            .expect("workspace created event")
        else {
            panic!("expected a created workspace event");
        };
        assert_eq!(workspace.workspace_id, "workspace-1");

        let Some(HerdrEvent::Workspace(WorkspaceEvent::Updated(workspace))) =
            parse_event(serde_json::json!({
                "event": "workspace_updated",
                "data": {
                    "workspace_id": "workspace-1",
                    "number": 1,
                    "label": "main",
                    "focused": false,
                    "pane_count": 2,
                    "tab_count": 3,
                    "active_tab_id": "tab-2",
                    "agent_status": "working"
                }
            }))
            .expect("workspace updated event")
        else {
            panic!("expected an updated workspace event");
        };
        assert_eq!(workspace.workspace_id, "workspace-1");

        let Some(HerdrEvent::Workspace(WorkspaceEvent::Closed { workspace_id })) =
            parse_event(serde_json::json!({
                "event": "workspace_closed",
                "data": {"workspace_id": "workspace-1"}
            }))
            .expect("workspace closed event")
        else {
            panic!("expected a closed workspace event");
        };
        assert_eq!(workspace_id, "workspace-1");

        let Some(HerdrEvent::Workspace(WorkspaceEvent::Focused { workspace_id })) =
            parse_event(serde_json::json!({
                "event": "workspace_focused",
                "data": {"workspace_id": "workspace-2"}
            }))
            .expect("workspace focused event")
        else {
            panic!("expected a focused workspace event");
        };
        assert_eq!(workspace_id, "workspace-2");

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Created(pane),
        })) = parse_event(serde_json::json!({
            "event": "pane_created",
            "data": pane_payload("workspace-1", "tab-1", "pane-7", 9)
        }))
        .expect("pane created event")
        else {
            panic!("expected a created pane event");
        };
        assert_eq!(pane.pane_id, "pane-7");
        assert_eq!(pane.revision, 9);

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Updated(pane),
        })) = parse_event(serde_json::json!({
            "event": "pane_updated",
            "data": pane_payload("workspace-1", "tab-1", "pane-7", 10)
        }))
        .expect("pane updated event")
        else {
            panic!("expected an updated pane event");
        };
        assert_eq!(pane.pane_id, "pane-7");
        assert_eq!(pane.revision, 10);

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Closed {
                pane_id,
                workspace_id,
            },
        })) = parse_event(serde_json::json!({
            "event": "pane_closed",
            "data": {"pane_id": "pane-7", "workspace_id": "workspace-1"}
        }))
        .expect("pane closed event")
        else {
            panic!("expected a closed pane event");
        };
        assert_eq!(pane_id, "pane-7");
        assert_eq!(workspace_id, "workspace-1");

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Focused {
                pane_id,
                workspace_id,
            },
        })) = parse_event(serde_json::json!({
            "event": "pane_focused",
            "data": {"pane_id": "pane-7", "workspace_id": "workspace-1"}
        }))
        .expect("pane focused event")
        else {
            panic!("expected a focused pane event");
        };
        assert_eq!(pane_id, "pane-7");
        assert_eq!(workspace_id, "workspace-1");

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Moved {
                previous_pane_id,
                pane,
            },
        })) = parse_event(serde_json::json!({
            "event": "pane_moved",
            "data": {
                "previous_pane_id": "pane-old",
                "previous_workspace_id": "workspace-1",
                "previous_tab_id": "tab-1",
                "pane": {
                    "workspace_id": "workspace-2",
                    "tab_id": "tab-2",
                    "pane_id": "pane-new",
                    "terminal_id": "terminal-stable",
                    "focused": true,
                    "revision": 10,
                    "agent": "claude",
                    "agent_status": "working",
                    "cwd": "/repo/worktree",
                    "foreground_cwd": "/repo/worktree/src",
                    "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
                }
            }
        }))
        .expect("pane moved event")
        else {
            panic!("expected a moved pane event");
        };
        assert_eq!(previous_pane_id, "pane-old");
        assert_eq!(pane.pane_id, "pane-new");
        assert_eq!(pane.terminal_id, "terminal-stable");
        assert_eq!(pane.revision, 10);
        assert_eq!(pane.workspace_id, "workspace-2");

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::Exited {
                pane_id,
                workspace_id,
            },
        })) = parse_event(serde_json::json!({
            "event": "pane_exited",
            "data": {"pane_id": "pane-7", "workspace_id": "workspace-1"}
        }))
        .expect("pane exited event")
        else {
            panic!("expected an exited pane event");
        };
        assert_eq!(pane_id, "pane-7");
        assert_eq!(workspace_id, "workspace-1");

        let Some(HerdrEvent::Pane(PaneEvent {
            kind: PaneEventKind::AgentDetected {
                pane_id,
                workspace_id,
            },
        })) = parse_event(serde_json::json!({
            "event": "pane_agent_detected",
            "data": {"pane_id": "pane-7", "workspace_id": "workspace-1"}
        }))
        .expect("pane agent detected event")
        else {
            panic!("expected an agent detected pane event");
        };
        assert_eq!(pane_id, "pane-7");
        assert_eq!(workspace_id, "workspace-1");
    }

    #[test]
    fn parses_pane_get_response() {
        let response = serde_json::json!({
            "id": "request-1",
            "result": {
                "type": "pane_info",
                "pane": {
                    "workspace_id": "workspace-1",
                    "tab_id": "tab-1",
                    "pane_id": "pane-7",
                    "terminal_id": "terminal-stable",
                    "focused": true,
                    "revision": 9,
                    "agent": "claude",
                    "agent_status": "working",
                    "cwd": "/repo/worktree",
                    "foreground_cwd": "/repo/worktree/src",
                    "agent_session": {"agent": "claude", "kind": "id", "value": "session-42"}
                }
            }
        });
        let result = parse_response(response, "request-1").expect("response result");
        let pane = parse_pane_result(result).expect("pane result");
        assert_eq!(pane.pane_id, "pane-7");
        assert_eq!(pane.terminal_id, "terminal-stable");
        assert_eq!(pane.revision, 9);
    }
}
