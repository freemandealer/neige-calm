//! Process identity observation shared by child cleanup and server supervision.
//!
//! Start identifiers are platform-specific: Linux uses clock ticks since boot;
//! macOS uses the kernel's process start timeval as microseconds since the Unix
//! epoch. Treat them as opaque equality tokens, paired with a boot ID when
//! persisted, not as durations or timestamps comparable across platforms.

/// Read Linux `starttime` (clock ticks since boot) from `/proc/<pid>/stat`.
/// Returns `None` if the process is gone or its stat cannot be read or parsed.
#[cfg(target_os = "linux")]
pub fn read_proc_start_time(pid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_starttime_from_stat(&stat)
}

/// Read macOS process start time as nonzero microseconds since the Unix epoch.
/// An unreadable, incomplete or invalid kernel response cannot prove identity.
#[cfg(target_os = "macos")]
pub fn read_proc_start_time(pid: i32) -> Option<u64> {
    if pid <= 0 {
        return None;
    }
    let size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
    // SAFETY: proc_bsdinfo contains only integer fields and arrays; zero is valid.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    // SAFETY: info is writable for exactly size bytes. arg=0 requests a live
    // process, without searching the zombie list. No pointers escape the call.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if written != size || info.pbi_pid != pid as u32 || info.pbi_start_tvusec >= 1_000_000 {
        return None;
    }
    let start = info
        .pbi_start_tvsec
        .checked_mul(1_000_000)?
        .checked_add(info.pbi_start_tvusec)?;
    (start != 0).then_some(start)
}

/// Pure parser for `/proc/<pid>/stat` field 22 (`starttime`); cfg-gate-free so unit tests run on every host.
pub fn parse_starttime_from_stat(content: &str) -> Option<u64> {
    parse_proc_stat_fields(content).map(|fields| fields.start_time)
}

/// The identity-relevant subset of `/proc/<pid>/stat`: `state` (field 3), `pgrp` (field 5) and `starttime` (field 22).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcStatFields {
    /// Single-char process state (`R`/`S`/`Z`/…) — field 3.
    pub state: char,
    /// Process-group id — field 5.
    pub pgrp: i32,
    /// `starttime` in clock-ticks since boot — field 22; the same value
    /// [`read_proc_start_time`] returns on Linux and owned-process verification checks.
    pub start_time: u64,
}

/// Pure parser for the [`ProcStatFields`] subset of `/proc/<pid>/stat`.
pub fn parse_proc_stat_fields(content: &str) -> Option<ProcStatFields> {
    // `comm` may contain `)` — split on the LAST `)`; the remainder starts with `state`.
    let after = content.rsplit_once(')')?.1;
    let mut fields = after.split_whitespace();
    // Post-comm indices: state(0) ppid(1) pgrp(2) … starttime is index 19.
    let state = fields.next()?.chars().next()?;
    let _ppid = fields.next()?;
    let pgrp = fields.next()?.parse::<i32>().ok()?;
    // `nth(16)` consumes 3..=19 and yields index 19 (`starttime`).
    let start_time = fields.nth(16)?.parse::<u64>().ok()?;
    Some(ProcStatFields {
        state,
        pgrp,
        start_time,
    })
}

/// Unsupported platforms cannot prove process identity.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read_proc_start_time(_pid: i32) -> Option<u64> {
    None
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::read_proc_start_time;

    struct ChildGuard(std::process::Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let status = self.0.wait().expect("reap identity test child");
            eprintln!("identity test child {} reaped: {status}", self.0.id());
        }
    }

    #[test]
    fn proc_identity_live_child_is_stable_and_reaped_child_is_absent() {
        let mut child = ChildGuard(
            std::process::Command::new("sleep")
                .arg("60")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("spawn identity test child"),
        );
        let pid = i32::try_from(child.0.id()).expect("pid fits i32");
        let start = read_proc_start_time(pid).expect("live child start time");
        assert_ne!(start, 0);
        assert_eq!(read_proc_start_time(pid), Some(start));
        child.0.kill().expect("kill owned test child");
        child.0.wait().expect("reap owned test child");
        assert_eq!(read_proc_start_time(pid), None);
    }

    #[test]
    fn proc_identity_invalid_pids_are_absent() {
        for pid in [i32::MIN, -1, 0, i32::MAX] {
            assert_eq!(read_proc_start_time(pid), None, "pid {pid}");
        }
    }
}
