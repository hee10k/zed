use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};

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
fn windows_pipe_endpoint(marker_path: &str) -> String {
    const PIPE_PREFIX: &str = r"\\.\pipe\";
    if marker_path.starts_with(PIPE_PREFIX) {
        return marker_path.to_string();
    }
    if let Ok(contents) = std::fs::read_to_string(marker_path) {
        let endpoint = contents.trim();
        if !endpoint.is_empty() {
            if endpoint.starts_with(PIPE_PREFIX) {
                return endpoint.to_string();
            }
            return format!("{PIPE_PREFIX}{endpoint}");
        }
    }
    let path = Path::new(marker_path);
    let name = match path
        .parent()
        .and_then(Path::file_name)
        .and_then(|component| component.to_str())
    {
        Some(session)
            if path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .and_then(|component| component.to_str())
                == Some("sessions") =>
        {
            format!("herdr-{session}")
        }
        _ => "herdr".to_string(),
    };
    format!("{PIPE_PREFIX}{name}")
}

pub(crate) enum HerdrStream {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    NamedPipe(std::fs::File),
}

impl HerdrStream {
    pub(crate) fn connect(endpoint: &HerdrEndpoint) -> Result<Self> {
        let endpoint_path = endpoint.resolve();
        #[cfg(unix)]
        {
            let stream = std::os::unix::net::UnixStream::connect(&endpoint_path).map_err(|error| {
                anyhow!("Failed to connect to Herdr Unix socket at {endpoint_path}: {error}")
            })?;
            Ok(Self::Unix(stream))
        }
        #[cfg(windows)]
        {
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
                    HANDLE::default(),
                )
            }
            .map_err(|error| anyhow!("Failed to connect to Herdr named pipe at {pipe_path}: {error}"))?;
            let file = unsafe { std::fs::File::from_raw_handle(handle.0 as _) };
            Ok(Self::NamedPipe(file))
        }
    }

    pub(crate) fn try_clone(&self) -> Result<Self> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => Ok(Self::Unix(stream.try_clone()?)),
            #[cfg(windows)]
            Self::NamedPipe(file) => Ok(Self::NamedPipe(file.try_clone()?)),
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
                file.write_all(line_with_newline.as_bytes())?;
                file.flush()?;
            }
        }
        Ok(())
    }
}

pub(crate) struct HerdrLineReader {
    reader: BufReader<HerdrStreamReadHandle>,
}

pub(crate) enum HerdrStreamReadHandle {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    NamedPipe(std::fs::File),
}

impl Read for HerdrStreamReadHandle {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
            #[cfg(windows)]
            Self::NamedPipe(file) => file.read(buffer),
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[cfg(windows)]
    #[test]
    fn windows_marker_endpoint_is_namespaced() {
        assert!(windows_pipe_endpoint(r#"\\.\pipe\herdr-test"#).starts_with(r#"\\.\pipe\"#));
    }
}
