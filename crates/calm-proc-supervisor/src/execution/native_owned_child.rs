//! The sole Darwin reaper. All methods run under Process::ownership (or before publication).
use std::{
    io,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(super) enum Pin {
    Owned,
    Reaped,
    Lost,
}
pub(super) struct Child {
    pub pid: i32,
    pub pin: Pin,
    pub group: bool,
    last_wait: Option<(i32, i32, i32, i32, i32, i32)>,
    #[cfg(test)]
    pub signals: usize,
}
impl Child {
    pub fn new(pid: i32) -> Self {
        Self {
            pid,
            pin: Pin::Owned,
            group: false,
            last_wait: None,
            #[cfg(test)]
            signals: 0,
        }
    }
    pub fn record(&self, event: &str, detail: &str) {
        tracing::info!(pid = self.pid, event, detail, "native child ledger");
        #[cfg(test)]
        {
            use std::io::Write;
            let mut stderr = io::stderr().lock();
            let _ = writeln!(
                stderr,
                "native-child pid={} event={event} {detail}",
                self.pid
            );
            let _ = stderr.flush();
        }
    }
    pub fn signal(&mut self, group: bool, signal: i32) -> anyhow::Result<()> {
        self.wait_info()?;
        #[cfg(test)]
        {
            self.signals += 1;
        }
        let target = if group { -self.pid } else { self.pid };
        let rc = unsafe { libc::kill(target, signal) };
        let errno = if rc == 0 {
            0
        } else {
            io::Error::last_os_error().raw_os_error().unwrap_or(0)
        };
        self.record(
            "signal",
            &format!("target={target} signal={signal} rc={rc} errno={errno}"),
        );
        anyhow::ensure!(
            rc == 0,
            "native signal failed: {}",
            io::Error::from_raw_os_error(errno)
        );
        Ok(())
    }
    pub fn wait_info(&mut self) -> anyhow::Result<libc::siginfo_t> {
        anyhow::ensure!(
            matches!(self.pin, Pin::Owned),
            "original native child pin unavailable"
        );
        loop {
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            unsafe {
                *libc::__error() = 0;
            }
            let rc = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.pid as u32,
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            let errno = io::Error::last_os_error();
            let fields = (
                rc,
                errno.raw_os_error().unwrap_or(0),
                info.si_signo,
                info.si_pid,
                info.si_code,
                info.si_status,
            );
            if self.last_wait != Some(fields) {
                self.record(
                    "waitid",
                    &format!(
                        "rc={rc} errno={} si_signo={} si_pid={} si_code={} si_status={}",
                        fields.1, fields.2, fields.3, fields.4, fields.5
                    ),
                );
                self.last_wait = Some(fields);
            }
            if rc == 0 {
                return Ok(info);
            }
            if errno.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if errno.raw_os_error() == Some(libc::ECHILD) {
                self.pin = Pin::Lost;
            }
            return Err(errno.into());
        }
    }
    pub fn reap_once(&mut self) -> anyhow::Result<bool> {
        anyhow::ensure!(
            matches!(self.pin, Pin::Owned),
            "original native child pin unavailable"
        );
        loop {
            let mut status = 0;
            unsafe {
                *libc::__error() = 0;
            }
            let rc = unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) };
            let error = io::Error::last_os_error();
            if rc == self.pid {
                self.pin = Pin::Reaped;
                self.record("reaped", &format!("waitpid={rc} errno=0 status={status}"));
                return Ok(true);
            }
            if rc == 0 {
                return Ok(false);
            }
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.raw_os_error() == Some(libc::ECHILD) {
                self.pin = Pin::Lost;
            }
            self.record("reap-error", &format!("waitpid={rc} errno={error}"));
            return Err(error.into());
        }
    }
    pub fn cleanup(&mut self) -> anyhow::Result<()> {
        if matches!(self.pin, Pin::Reaped) {
            return Ok(());
        }
        // Success establishes the existing child relationship; interpreting its
        // event or obtaining a group snapshot is unnecessary for failure cleanup.
        self.wait_info()?;
        if self.group {
            // No live member is also possible; root cleanup/reap still must run.
            let _ = self.signal(true, libc::SIGKILL);
        }
        // The root might have changed groups. Still address only our unreaped child.
        let _ = self.signal(false, libc::SIGKILL);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if self.reap_once()? {
                return Ok(());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "native child cleanup timed out: {}",
                self.pid
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            self.record("cleanup-unresolved", &error.to_string());
        }
    }
}
