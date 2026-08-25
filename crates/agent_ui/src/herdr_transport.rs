use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};

/// Default connect/read/write window for endpoint connections when no
/// explicit request deadline is supplied.
pub(crate) const DEFAULT_CONNECT_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HerdrEndpoint {
    Default,
    NamedSession(String),
    Explicit(String),
}

fn herdr_config_dir() -> PathBuf {
    if let Ok(config_dir) = std::env::var("HERDR_CONFIG_DIR") {
        if !config_dir.is_empty() {
            return PathBuf::from(config_dir);
        }
    }

    #[cfg(windows)]
    {
        if let Ok(app_data) = std::env::var("APPDATA") {
            if !app_data.is_empty() {
                return PathBuf::from(app_data).join("herdr");
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = std::env::var("HOME") {
            if !home.is_empty() {
                return PathBuf::from(home)
                    .join("Library")
                    .join("Application Support")
                    .join("herdr");
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        if let Ok(xdg_config_home) = std::env::var("XDG_CONFIG_HOME") {
            if !xdg_config_home.is_empty() {
                return PathBuf::from(xdg_config_home).join("herdr");
            }
        }
        if let Ok(home) = std::env::var("HOME") {
            if !home.is_empty() {
                return PathBuf::from(home).join(".config").join("herdr");
            }
        }
    }

    PathBuf::from("/tmp").join("herdr")
}

fn named_session_path(name: &str) -> PathBuf {
    herdr_config_dir().join("sessions").join(name).join("herdr.sock")
}

impl HerdrEndpoint {
    pub(crate) fn resolve(&self) -> String {
        match self {
            Self::Explicit(path) => path.clone(),
            Self::NamedSession(name) => named_session_path(name).to_string_lossy().into_owned(),
            Self::Default => {
                if let Ok(socket_path) = std::env::var("HERDR_SOCKET_PATH") {
                    if !socket_path.is_empty() {
                        return socket_path;
                    }
                }
                if let Ok(session) = std::env::var("HERDR_SESSION") {
                    if !session.is_empty() {
                        return named_session_path(&session).to_string_lossy().into_owned();
                    }
                }
                herdr_config_dir()
                    .join("herdr.sock")
                    .to_string_lossy()
                    .into_owned()
            }
        }
    }
}

#[cfg(windows)]
fn windows_pipe_endpoint(endpoint_path: &str) -> String {
    const PIPE_PREFIX: &str = r"\\.\pipe\";
    if endpoint_path.starts_with(PIPE_PREFIX) {
        endpoint_path.to_string()
    } else {
        // Herdr writes pid:nonce marker contents to endpoint_path. The
        // namespaced pipe is derived from the configured path itself; marker
        // contents are never a pipe name.
        format!("{PIPE_PREFIX}{endpoint_path}")
    }
}

pub(crate) enum HerdrStream {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    /// The pipe file is shared with the connection's kill switch so a
    /// deadline watchdog or pane teardown can cancel an in-flight blocking
    /// read from another thread.
    #[cfg(windows)]
    NamedPipe(Arc<std::fs::File>),
}

impl HerdrStream {
    /// Connect with a hard deadline covering DNS-less local connect plus the
    /// initial read/write window, so a server that accepts and never answers
    /// cannot block the caller indefinitely.
    pub(crate) fn connect_with_deadline(endpoint: &HerdrEndpoint, deadline: Duration) -> Result<Self> {
        let endpoint_path = endpoint.resolve();
        #[cfg(unix)]
        {
            use std::os::unix::net::{SocketAddr, UnixStream};
            let address = SocketAddr::from_pathname(&endpoint_path)
                .map_err(|error| anyhow!("Invalid Herdr socket path {endpoint_path}: {error}"))?;
            // Local filesystem sockets connect immediately; the read/write
            // deadlines below carry the timeout guarantee.
            let stream = UnixStream::connect_addr(&address).map_err(|error| {
                anyhow!("Failed to connect to Herdr Unix socket at {endpoint_path}: {error}")
            })?;
            stream.set_read_timeout(Some(deadline))?;
            stream.set_write_timeout(Some(deadline))?;
            Ok(Self::Unix(stream))
        }
        #[cfg(windows)]
        {
            let _ = deadline;
            use std::ffi::OsStr;
            use std::os::windows::ffi::OsStrExt;
            use std::os::windows::io::FromRawHandle;
            use windows::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE, HANDLE};
            use windows::Win32::Storage::FileSystem::{
                CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_NONE, OPEN_EXISTING,
            };

            let pipe_path = windows_pipe_endpoint(&endpoint_path);
            let wide: Vec<u16> = OsStr::new(&pipe_path)
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();
            let handle = unsafe {
                CreateFileW(
                    windows::core::PCWSTR(wide.as_ptr()),
                    (GENERIC_READ | GENERIC_WRITE).0,
                    FILE_SHARE_NONE,
                    None,
                    OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL,
                    Some(HANDLE::default()),
                )
            }
            .map_err(|error| anyhow!("Failed to connect to Herdr named pipe at {pipe_path}: {error}"))?;
            let file = Arc::new(unsafe { std::fs::File::from_raw_handle(handle.0 as _) });
            Ok(Self::NamedPipe(file))
        }
    }

    pub(crate) fn connect(endpoint: &HerdrEndpoint) -> Result<Self> {
        Self::connect_with_deadline(endpoint, DEFAULT_CONNECT_DEADLINE)
    }
    pub(crate) fn try_clone(&self) -> Result<Self> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => Ok(Self::Unix(stream.try_clone()?)),
            #[cfg(windows)]
            Self::NamedPipe(file) => Ok(Self::NamedPipe(Arc::new((**file).try_clone()?))),
        }
    }

    pub(crate) fn send_line(&mut self, line: &str) -> Result<()> {
        let line_with_newline = if line.ends_with('\n') {
            line.to_string()
        } else {
            format!("{line}\n")
        };
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => {
                stream.write_all(line_with_newline.as_bytes())?;
                stream.flush()?;
            }
            #[cfg(windows)]
            Self::NamedPipe(file) => {
                (&**file).write_all(line_with_newline.as_bytes())?;
                (&**file).flush()?;
            }
        }
        Ok(())
    }

    /// Derive a switch that terminates this connection's pending reads when
    /// triggered. Create it before the stream moves into a reader.
    pub(crate) fn kill_switch(&self) -> Result<ConnectionKillSwitch> {
        let inner = match self {
            #[cfg(unix)]
            Self::Unix(stream) => KillSwitchInner::Unix(stream.try_clone()?),
            #[cfg(windows)]
            Self::NamedPipe(file) => KillSwitchInner::NamedPipe(file.clone()),
        };
        Ok(ConnectionKillSwitch {
            inner: Arc::new(inner),
        })
    }

    /// Arm a watchdog that cancels I/O on this connection once `deadline`
    /// elapses and *keeps* cancelling until disarmed, so every blocking
    /// operation started after expiry also fails: a peer that accepts but
    /// never answers cannot block the request thread forever.
    #[cfg(windows)]
    pub(crate) fn arm_io_deadline(&self, deadline: Duration) -> Result<IoDeadline> {
        let kill = self.kill_switch()?;
        IoDeadline::spawn(deadline, kill)
    }

    /// Unix connections bound every blocking operation with socket
    /// read/write timeouts, so no cancellation watchdog is needed.
    #[cfg(not(windows))]
    pub(crate) fn arm_io_deadline(&self, deadline: Duration) -> Result<IoDeadline> {
        let _ = deadline;
        Ok(IoDeadline::noop())
    }
}

/// Forcibly unblocks reads pending on the connection this switch was derived
/// from: Unix sockets are shut down, Windows pipe reads are cancelled with
/// `CancelIoEx`. Used to enforce request deadlines on Windows and to tear
/// down retired per-pane subscription connections on every platform.
#[derive(Clone)]
pub(crate) struct ConnectionKillSwitch {
    inner: Arc<KillSwitchInner>,
}

enum KillSwitchInner {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    NamedPipe(Arc<std::fs::File>),
}

impl ConnectionKillSwitch {
    /// Terminate any read currently blocked on the connection. Safe to call
    /// repeatedly; triggering with nothing pending is a no-op.
    pub(crate) fn trigger(&self) {
        match &*self.inner {
            #[cfg(unix)]
            KillSwitchInner::Unix(stream) => {
                use std::net::Shutdown;
                let _ = stream.shutdown(Shutdown::Both);
            }
            #[cfg(windows)]
            KillSwitchInner::NamedPipe(file) => {
                use std::os::windows::io::AsRawHandle;
                use windows::Win32::Foundation::HANDLE;
                use windows::Win32::System::IO::CancelIoEx;
                let handle = HANDLE(file.as_raw_handle() as _);
                // ERROR_NOT_FOUND simply means nothing was in flight.
                let _ = unsafe { CancelIoEx(handle, None) };
            }
        }
    }
}

/// Interval between repeated cancellations once a deadline has expired.
const DEADLINE_REARM_INTERVAL: Duration = Duration::from_millis(25);

/// Deadline watchdog loop, kept platform-neutral so its expiry semantics
/// are unit-testable. Waits out `deadline`, then keeps invoking
/// `cancel_io` on `DEADLINE_REARM_INTERVAL` until the cancel channel
/// closes (disarm or drop). One cancellation pass only reaches I/O already
/// in flight at that instant — Windows `CancelIoEx` cancels nothing for a
/// read that starts later — so cancellation must stay active after expiry.
fn run_deadline_watchdog(
    cancel_rx: std::sync::mpsc::Receiver<()>,
    deadline: Duration,
    mut cancel_io: impl FnMut(),
) {
    use std::sync::mpsc::RecvTimeoutError;
    match cancel_rx.recv_timeout(deadline) {
        Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
        Err(RecvTimeoutError::Timeout) => {}
    }
    loop {
        cancel_io();
        match cancel_rx.recv_timeout(DEADLINE_REARM_INTERVAL) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => continue,
        }
    }
}

/// Watchdog armed around a bounded request. Once its deadline elapses it
/// keeps cancelling the connection's I/O until disarmed or dropped, so no
/// blocked request worker — including one whose read starts after expiry —
/// can survive the deadline; each cancelled read completes with an abort
/// error and the worker exits.
pub(crate) struct IoDeadline {
    cancel_tx: Option<std::sync::mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl IoDeadline {
    #[cfg(windows)]
    fn spawn(deadline: Duration, kill: ConnectionKillSwitch) -> Result<Self> {
        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel::<()>();
        let worker = std::thread::Builder::new()
            .name("herdr-io-deadline".to_string())
            .spawn(move || run_deadline_watchdog(cancel_rx, deadline, || kill.trigger()))?;
        Ok(Self {
            cancel_tx: Some(cancel_tx),
            worker: Some(worker),
        })
    }

    fn noop() -> Self {
        Self {
            cancel_tx: None,
            worker: None,
        }
    }

    /// Cancel the pending deadline without firing the kill switch.
    pub(crate) fn disarm(mut self) {
        self.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }

    fn cancel(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(());
        }
    }
}

impl Drop for IoDeadline {
    fn drop(&mut self) {
        self.cancel();
    }
}

pub(crate) struct HerdrLineReader {
    reader: BufReader<HerdrStreamReadHandle>,
}

pub(crate) enum HerdrStreamReadHandle {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    /// Shared with the connection kill switch so teardown can cancel an
    /// in-flight blocking read.
    #[cfg(windows)]
    NamedPipe(Arc<std::fs::File>),
}

impl Read for HerdrStreamReadHandle {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
            #[cfg(windows)]
            Self::NamedPipe(file) => (&**file).read(buffer),
        }
    }
}

impl HerdrLineReader {
    pub(crate) fn new(stream: HerdrStream) -> Self {
        let handle = match stream {
            #[cfg(unix)]
            HerdrStream::Unix(stream) => HerdrStreamReadHandle::Unix(stream),
            #[cfg(windows)]
            HerdrStream::NamedPipe(file) => HerdrStreamReadHandle::NamedPipe(file),
        };
        Self {
            reader: BufReader::new(handle),
        }
    }

    pub(crate) fn read_line(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;
        if bytes_read == 0 {
            return Ok(None);
        }
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }
        Ok(Some(line))
    }

    /// Adjust the read deadline. Subscription connections clear it after the
    /// handshake so idle pushed events are not mistaken for a stall.
    pub(crate) fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        match self.reader.get_ref() {
            #[cfg(unix)]
            HerdrStreamReadHandle::Unix(stream) => stream.set_read_timeout(timeout)?,
            #[cfg(windows)]
            HerdrStreamReadHandle::NamedPipe(file) => {
                let _ = timeout;
                let _ = file;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Review 5 finding 3: a watchdog that fires exactly one cancellation
    /// leaves every read started after expiry unbounded (one CancelIoEx
    /// pass only reaches I/O already in flight). Cancellation must stay
    /// active after the deadline until the watchdog is disarmed.
    #[test]
    fn deadline_watchdog_keeps_cancelling_after_expiry() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel::<()>();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cancellations);
        let worker = std::thread::Builder::new()
            .name("herdr-watchdog-fixture".to_string())
            .spawn(move || {
                run_deadline_watchdog(cancel_rx, Duration::from_millis(50), || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            })
            .expect("spawn fixture watchdog");
        // Outlive the deadline plus several re-arm intervals: reads started
        // anywhere in this window must have been covered by a later pass.
        std::thread::sleep(Duration::from_millis(150));
        drop(cancel_tx);
        worker.join().expect("watchdog exits after expiry");
        assert!(
            cancellations.load(Ordering::SeqCst) >= 2,
            "cancellation must stay active after expiry"
        );
    }

    #[test]
    fn deadline_watchdog_stays_idle_until_expiry_and_stops_on_disarm() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel::<()>();
        let cancellations = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&cancellations);
        let worker = std::thread::Builder::new()
            .name("herdr-watchdog-fixture".to_string())
            .spawn(move || {
                run_deadline_watchdog(cancel_rx, Duration::from_secs(30), || {
                    counter.fetch_add(1, Ordering::SeqCst);
                })
            })
            .expect("spawn fixture watchdog");
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(
            cancellations.load(Ordering::SeqCst),
            0,
            "nothing is cancelled before the deadline elapses"
        );
        cancel_tx.send(()).expect("disarm watchdog");
        worker.join().expect("watchdog stops on disarm");
        assert_eq!(cancellations.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn resolves_explicit_endpoint() {
        let endpoint = HerdrEndpoint::Explicit("/tmp/custom.sock".to_string());
        assert_eq!(endpoint.resolve(), "/tmp/custom.sock");
    }

    #[test]
    fn resolves_named_session_under_session_directory() {
        let endpoint = HerdrEndpoint::NamedSession("test-session".to_string());
        let resolved = endpoint.resolve();
        assert!(resolved.ends_with("sessions/test-session/herdr.sock"));
    }

    #[cfg(unix)]
    #[test]
    fn unix_fixture_round_trips_ndjson() {
        use std::os::unix::net::UnixListener;
        use std::thread;

        let path = std::env::temp_dir().join(format!("herdr-test-{}", std::process::id()));
        let listener = UnixListener::bind(&path).expect("bind fixture");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept fixture");
            let mut line = String::new();
            BufReader::new(stream.try_clone().expect("clone fixture"))
                .read_line(&mut line)
                .expect("read fixture");
            assert_eq!(line, "{\"id\":\"1\"}\n");
            stream.write_all(b"{\"id\":\"1\",\"result\":{}}\n").expect("write fixture");
        });

        let mut stream = HerdrStream::connect(&HerdrEndpoint::Explicit(
            path.to_string_lossy().into_owned(),
        ))
        .expect("connect fixture");
        stream.send_line(r#"{"id":"1"}"#).expect("send fixture");
        let mut reader = HerdrLineReader::new(stream);
        assert_eq!(reader.read_line().expect("read response").as_deref(), Some(r#"{"id":"1","result":{}}"#));
        server.join().expect("fixture thread");
        std::fs::remove_file(path).expect("remove fixture");
    }

    /// Kill switch terminates a reader blocked on the connection: this is
    /// what bounds a silent-peer pipe request on Windows and tears down
    /// retired per-pane subscription connections everywhere.
    #[cfg(unix)]
    #[test]
    fn kill_switch_unblocks_a_blocked_reader() {
        use std::os::unix::net::UnixStream;
        use std::time::Instant;

        let (server_side, client_side) = UnixStream::pair().expect("socket pair");
        let switch = HerdrStream::Unix(client_side.try_clone().expect("clone socket"))
            .kill_switch()
            .expect("kill switch");
        let mut reader = HerdrLineReader::new(HerdrStream::Unix(client_side));

        let waiter = std::thread::spawn(move || {
            let _ = reader.read_line();
        });
        // Give the reader time to block inside read_line.
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        switch.trigger();
        waiter.join().expect("reader thread exits");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "trigger must unblock a pending read promptly"
        );
        drop(server_side);
    }

    #[cfg(windows)]
    #[test]
    fn windows_marker_endpoint_is_namespaced() {
        assert!(windows_pipe_endpoint(r#"\\.\pipe\herdr-test"#).starts_with(r#"\\.\pipe\"#));
    }
    #[cfg(windows)]
    #[test]
    fn windows_marker_contents_are_not_used_as_pipe_name() {
        let marker_path = r"C:\Users\test\AppData\Roaming\herdr\herdr.sock";
        assert_eq!(
            windows_pipe_endpoint(marker_path),
            r"\\.\pipe\C:\Users\test\AppData\Roaming\herdr\herdr.sock"
        );
    }
}
