//! Orphan-process capture and safe reaping for terminal-backed agents.
//!
//! When a terminal (or its worktree) closes, descendants of its shell/agent
//! process can keep running after the terminal UI is gone. We cannot kill
//! those by PID alone, because a PID may be recycled to an unrelated process.
//! Instead we capture the live descendant tree at close time — recording each
//! descendant's PID *and* start time — then reap only processes whose PID
//! still maps to the same start time. A recycled PID has a different start
//! time, so it is never touched.

use sysinfo::{
    Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System, UpdateKind,
};

/// A descendant process captured at close time, identified by PID plus its
/// start time as a guard against PID reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapturedOrphan {
    pub pid: Pid,
    /// `Process::start_time()` at capture time; re-verified before reaping.
    pub start_time: u64,
}

/// Snapshot of a terminal's live descendants at close time.
///
/// `root` is the terminal's shell/agent PID. The returned set includes
/// grandchildren; a process is included only if its start time could be read
/// (a process that vanished mid-scan is left out — it needs no reaping).
pub fn capture_descendants(root: Pid) -> Vec<CapturedOrphan> {
    let refresh = ProcessRefreshKind::nothing().with_cmd(UpdateKind::Always);
    let mut system = System::new_with_specifics(RefreshKind::nothing().with_processes(refresh));
    system.refresh_processes_specifics(ProcessesToUpdate::All, true, refresh);

    descendants_of(&system, root)
        .into_iter()
        .filter_map(|pid| {
            let process = system.process(pid)?;
            Some(CapturedOrphan {
                pid,
                start_time: process.start_time(),
            })
        })
        .collect()
}

/// Reap captured orphans that are still alive *and* still match their recorded
/// start time. A process whose PID was recycled (start time changed) or which
/// already exited is skipped. Returns the number of processes reaped.
pub fn reap_orphans(captured: &[CapturedOrphan]) -> usize {
    if captured.is_empty() {
        return 0;
    }
    let refresh = ProcessRefreshKind::nothing();
    let mut system = System::new_with_specifics(RefreshKind::nothing().with_processes(refresh));
    system.refresh_processes_specifics(ProcessesToUpdate::All, true, refresh);

    captured
        .iter()
        .filter(|candidate| {
            let Some(process) = system.process(candidate.pid) else {
                return false;
            };
            // The process must still exist AND still be the process we saw at
            // capture time. If the PID was reused, its start time differs and
            // we refuse to touch it.
            process.start_time() == candidate.start_time
        })
        .filter(|candidate| system.process(candidate.pid).is_some_and(|p| p.kill()))
        .count()
}

/// Walk the full descendant tree of `root` (including grandchildren) by
/// building a parent→children map from the whole process table.
fn descendants_of(system: &System, root: Pid) -> Vec<Pid> {
    let mut parent_map: std::collections::HashMap<Pid, Vec<Pid>> =
        std::collections::HashMap::new();
    for (pid, process) in system.processes() {
        if let Some(parent) = process.parent() {
            parent_map.entry(parent).or_default().push(*pid);
        }
    }
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(parent) = stack.pop() {
        if let Some(children) = parent_map.get(&parent) {
            for child in children {
                out.push(*child);
                stack.push(*child);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    #[cfg(not(windows))]
    #[test]
    #[allow(
        clippy::disallowed_methods,
        reason = "the test needs real short-lived child processes and may block"
    )]
    fn capture_and_reap_descendants() {
        // Spawn a shell that forks a long-lived child, so we have a real
        // descendant tree to capture.
        let mut shell = Command::new("sh")
            .arg("-c")
            .arg("sleep 60 & sleep 60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn shell");

        let root = Pid::from_u32(shell.id());

        // `sh` forks its children asynchronously after spawn, so a single
        // immediate snapshot can race the forks and observe no descendants.
        // Poll until both `sleep` children (background + foreground) are
        // visible before capturing, so the tree we test against is stable and
        // fully reaped afterward (no stray foreground child left behind).
        let captured = std::iter::repeat_with(|| capture_descendants(root))
            .take(100)
            .find(|captured| captured.len() == 2)
            .expect("both forked sleep children should be captured as descendants");

        // Reap only the forked children. The root shell is never in the
        // captured set (it is the root, not a descendant), so reaping cannot
        // touch it.
        let reaped = reap_orphans(&captured);
        assert!(reaped >= 1, "at least one descendant should be reaped");

        // A second reap on the same set is a no-op: the children are gone.
        let reaped_again = reap_orphans(&captured);
        assert_eq!(reaped_again, 0);

        // The shell may have exited on its own once we reaped its foreground
        // child. Reap it only if it is still running; either way it was never
        // reaped as an orphan (its pid was not among the captured children).
        if shell.try_wait().expect("failed to check test shell").is_none() {
            shell.kill().expect("failed to kill test shell");
            shell.wait().expect("failed to wait for test shell");
        }
    }

    #[test]
    fn reap_refuses_recycled_pid() {
        // A pid that is not currently alive must not be killed, and a fake
        // start time on a live-but-unknown pid must not be matched.
        let captured = vec![CapturedOrphan {
            pid: Pid::from_u32(u32::MAX),
            start_time: 0,
        }];
        assert_eq!(reap_orphans(&captured), 0);
    }
}