use std::io::{BufRead, BufReader, Write};
use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HerdrEndpoint {
    Default,
    NamedSession(String),
    Explicit(String),
}

impl HerdrEndpoint {
    pub(crate) fn resolve(&self) -> String {
        match self {
            HerdrEndpoint::Explicit(path) => path.clone(),
            HerdrEndpoint::NamedSession(name) => {
                if cfg!(windows) {
                    format!(r"\\.\pipe\herdr-{name}")
                } else {
                    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
                    format!("{home}/.config/herdr/herdr-{name}.sock")
                }
            }
            HerdrEndpoint::Default => {
                if let Ok(socket_path) = std::env::var("HERDR_SOCKET_PATH") {
                    if !socket_path.is_empty() {
                        return socket_path;
                    }
                }
                if let Ok(session) = std::env::var("HERDR_SESSION") {
                    if !session.is_empty() {
                        return HerdrEndpoint::NamedSession(session).resolve();
                    }
                }
                if cfg!(windows) {
                    r"\\.\pipe\herdr".to_string()
                } else {
                    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
                    format!("{home}/.config/herdr/herdr.sock")
                }
            }
        }
    }
}

pub(crate) enum HerdrStream {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    NamedPipe(std::fs::File),
}

impl HerdrStream {
    pub(crate) fn connect(endpoint: &HerdrEndpoint) -> Result<Self> {
        let path_str = endpoint.resolve();
        #[cfg(unix)]
        {
            let stream = std::os::unix::net::UnixStream::connect(&path_str)
                .map_err(|e| anyhow!("Failed to connect to Herdr Unix socket at {path_str}: {e}"))?;
            Ok(HerdrStream::Unix(stream))
        }
        #[cfg(windows)]
        {
            use std::ffi::OsStr;
            use std::os::windows::ffi::OsStrExt;
            use windows::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE};
            use windows::Win32::Storage::FileSystem::{
                CreateFileW, FILE_SHARE_NONE, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL,
            };

            let wide: Vec<u16> = OsStr::new(&path_str)
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
            };
            if handle.is_err() || handle == Ok(INVALID_HANDLE_VALUE) {
                return Err(anyhow!("Failed to connect to Herdr named pipe at {path_str}"));
            }
            let handle = handle.unwrap();
            use std::os::windows::io::FromRawHandle;
            let file = unsafe { std::fs::File::from_raw_handle(handle.0 as _) };
            Ok(HerdrStream::NamedPipe(file))
        }
    }

    pub(crate) fn try_clone(&self) -> Result<Self> {
        match self {
            #[cfg(unix)]
            HerdrStream::Unix(s) => Ok(HerdrStream::Unix(s.try_clone()?)),
            #[cfg(windows)]
            HerdrStream::NamedPipe(f) => Ok(HerdrStream::NamedPipe(f.try_clone()?)),
        }
    }

    pub(crate) fn send_line(&mut self, line: &str) -> Result<()> {
        let line_with_newline = if line.ends_with('\n') {
            line.to_string()
        } else {
            format!("{line}\n")
        };
        let bytes = line_with_newline.as_bytes();
        match self {
            #[cfg(unix)]
            HerdrStream::Unix(s) => {
                s.write_all(bytes)?;
                s.flush()?;
            }
            #[cfg(windows)]
            HerdrStream::NamedPipe(f) => {
                f.write_all(bytes)?;
                f.flush()?;
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

impl std::io::Read for HerdrStreamReadHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            #[cfg(unix)]
            HerdrStreamReadHandle::Unix(s) => s.read(buf),
            #[cfg(windows)]
            HerdrStreamReadHandle::NamedPipe(f) => f.read(buf),
        }
    }
}

impl HerdrLineReader {
    pub(crate) fn new(stream: HerdrStream) -> Self {
        let handle = match stream {
            #[cfg(unix)]
            HerdrStream::Unix(s) => HerdrStreamReadHandle::Unix(s),
            #[cfg(windows)]
            HerdrStream::NamedPipe(f) => HerdrStreamReadHandle::NamedPipe(f),
        };
        Self {
            reader: BufReader::new(handle),
        }
    }

    pub(crate) fn read_line(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;
        if bytes_read == 0 {
            Ok(None)
        } else {
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }
            Ok(Some(line))
        }
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
    fn resolves_named_session() {
        let endpoint = HerdrEndpoint::NamedSession("test-session".to_string());
        let resolved = endpoint.resolve();
        assert!(resolved.contains("test-session"));
    }
}
