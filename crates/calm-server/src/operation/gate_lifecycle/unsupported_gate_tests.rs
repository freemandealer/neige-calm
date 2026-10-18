//! macOS must refuse both gate adapters before creating even a held wrapper.
use super::*;
use std::sync::Arc;

use crate::db::RouteRepo;
use crate::db::sqlite::SqlxRepo;
use crate::event::EventBus;
use crate::operation::task_gate_run::{FrozenRun, TaskGateRunAdapter, finalize};
use crate::operation::task_verify_adapter::target::{FrozenTarget, FrozenUnbound};
use crate::operation::task_verify_adapter::{FrozenVerify, TaskVerifyAdapter};
use crate::operation::{
    Operation, OperationCompletionBus, OperationOutcome, Phase, PhaseTag, ProviderAdapter,
    SpawnCtx, SqlxOperationRepo, TxOutput,
};
use crate::state::DaemonClient;
use crate::terminal_renderer::TerminalRendererRegistry;

// Each declaration supplies its own exact libtest name; there is no second test registry.
macro_rules! isolated_test {
    ($name:ident, |$case:ident| $body:block) => {
        isolated_test!($name, |$case| $body, run_parent);
    };
    ($name:ident, |$case:ident| $body:block, $parent:expr) => {
        #[test]
        fn $name() {
            let qualified = concat!(module_path!(), "::", stringify!($name));
            let name = qualified.split_once("::").unwrap().1;
            run_test(name, |$case| Box::pin(async move $body), $parent);
        }
    };
}

const ENTRY: &str = "NEIGE_UNSUPPORTED_GATE_ENTRY";
const DIRECTORY: &str = "NEIGE_UNSUPPORTED_GATE_DIRECTORY";
const MODE: &str = "NEIGE_UNSUPPORTED_GATE_MODE";
const CASE_TIMEOUT: Duration = Duration::from_secs(5);

struct OwnedChild(std::process::Child);

impl OwnedChild {
    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = self.0.try_wait().expect("wait owned child") {
                eprintln!(
                    "unsupported_gate owner reaped pid={} status={status}",
                    self.0.id()
                );
                return status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "owned child timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Never signal after ECHILD or after reaping. No cached PID is cleanup authority.
        match self.0.try_wait() {
            Ok(None) => {
                let killed = self.0.kill();
                let waited = self.0.wait();
                eprintln!(
                    "unsupported_gate owner cleanup pid={} kill={killed:?} wait={waited:?}",
                    self.0.id()
                );
                assert!(waited.is_ok(), "owned child cleanup failed");
            }
            Ok(Some(_)) => {}
            Err(error) => {
                eprintln!("owned child lost wait authority: {error}");
                assert!(std::thread::panicking(), "owned child lost wait authority");
            }
        }
    }
}

fn publish(dir: &Path, name: &str, bytes: &[u8]) {
    let temporary = dir.join(format!(".{name}"));
    std::fs::write(&temporary, bytes).unwrap();
    std::fs::rename(temporary, dir.join(name)).unwrap();
}

struct CaseProcess {
    child: OwnedChild,
    dir: tempfile::TempDir,
}

impl CaseProcess {
    fn spawn(name: &str, mode: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", name, "--test-threads=1", "--nocapture"])
            .env_clear()
            .env(ENTRY, name)
            .env(DIRECTORY, dir.path())
            .env(MODE, mode);
        for key in [
            "PATH",
            "HOME",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "RUST_BACKTRACE",
            "DYLD_FALLBACK_LIBRARY_PATH",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let child = OwnedChild(command.spawn().unwrap());
        eprintln!(
            "unsupported_gate case spawned pid={} name={name} mode={mode}",
            child.0.id()
        );
        let mut case = Self { child, dir };
        let deadline = std::time::Instant::now() + CASE_TIMEOUT;
        while !case.dir.path().join("entered").exists() {
            assert!(
                case.child.0.try_wait().unwrap().is_none(),
                "exact child entry did not execute"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "child entry handshake timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            std::fs::read_to_string(case.dir.path().join("entered")).unwrap(),
            format!("{name}:{}", case.child.0.id())
        );
        case
    }

    fn release(&self) {
        publish(self.dir.path(), "release", b"run");
    }

    fn finish(&mut self) -> (std::process::ExitStatus, Value) {
        let status = self.child.wait();
        let report = std::fs::read(self.dir.path().join("finished"))
            .expect("child must execute and finish cleanup, even on panic/timeout");
        (status, serde_json::from_slice(&report).unwrap())
    }
}

impl Drop for CaseProcess {
    fn drop(&mut self) {
        // Parent assertion failures cancel an unreleased case. A released async case has its
        // own timeout and cleanup; wait for that before dropping our explicit Child handle.
        if matches!(self.child.0.try_wait(), Ok(None)) {
            publish(self.dir.path(), "release", b"cancel");
            self.child.wait();
        }
    }
}

struct Case {
    mode: String,
    dir: PathBuf,
    // Keep the deliberate regression wrapper held, including across panic and timeout.
    held: Vec<tokio::process::Child>,
}

fn run_test(
    name: &str,
    body: impl for<'a> FnOnce(
        &'a mut Case,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
    parent: impl FnOnce(&str),
) {
    let Some(entry) = std::env::var_os(ENTRY) else {
        parent(name);
        return;
    };
    assert_eq!(entry, name, "wrong isolated entry; refusing recursion");
    assert_eq!(
        std::env::args().skip(1).collect::<Vec<_>>(),
        ["--exact", name, "--test-threads=1", "--nocapture"]
    );
    static ENTERED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    assert!(
        !ENTERED.swap(true, std::sync::atomic::Ordering::SeqCst),
        "isolated entry ran twice"
    );
    let mut case = Case {
        mode: std::env::var(MODE).unwrap(),
        dir: PathBuf::from(std::env::var_os(DIRECTORY).unwrap()),
        held: Vec::new(),
    };
    // This is the only process allowed to enumerate children: exact singleton entry, before
    // its runtime, with no other libtest case or child. There is no shared-process snapshot.
    assert!(
        children().is_empty(),
        "singleton started with unexpected children"
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    publish(
        &case.dir,
        "entered",
        format!("{name}:{}", std::process::id()).as_bytes(),
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.block_on(async {
            tokio::time::timeout(CASE_TIMEOUT, async {
                while !case.dir.join("release").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                assert_eq!(
                    std::fs::read(case.dir.join("release")).unwrap(),
                    b"run",
                    "case cancelled"
                );
                body(&mut case).await;
            })
            .await
        })
    }));
    // Stop Tokio's orphan reaper before taking sole ownership of any residual children.
    runtime.shutdown_timeout(CASE_TIMEOUT);
    let reaped = cleanup_children();
    drop(case.held);
    let outcome = match &result {
        Ok(Ok(())) => "passed",
        Ok(Err(_)) => "timeout",
        Err(_) => "panic",
    };
    let panic_message = result
        .as_ref()
        .err()
        .and_then(|p| p.downcast_ref::<&str>())
        .copied();
    std::fs::write(
        case.dir.join("finished"),
        serde_json::to_vec(&json!({
            "outcome": outcome, "panic": panic_message, "reaped": reaped,
        }))
        .unwrap(),
    )
    .unwrap();
    eprintln!(
        "unsupported_gate case finished pid={} name={name} outcome={outcome} reaped={reaped:?}",
        std::process::id()
    );
    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("isolated case timed out"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn run_parent(name: &str) {
    let mut case = CaseProcess::spawn(name, "case");
    case.release();
    let (status, report) = case.finish();
    assert!(
        status.success(),
        "isolated scenario failed: {status} {report}"
    );
    assert_eq!(report["outcome"], "passed");
}

fn children() -> Vec<i32> {
    let mut pids = [0i32; 1024];
    // SAFETY: valid output buffer. Only called in the verified singleton process.
    let count = unsafe {
        libc::proc_listchildpids(
            std::process::id() as i32,
            pids.as_mut_ptr().cast(),
            std::mem::size_of_val(&pids) as i32,
        )
    };
    assert!(count >= 0, "cannot enumerate singleton children");
    assert_eq!(pids[pids.len() - 1], 0, "child buffer must not fill");
    pids.into_iter().filter(|pid| *pid > 0).collect()
}

fn group_descendants(pgid: i32) -> Vec<i32> {
    let mut pids = [0i32; 1024];
    // The unreaped child anchors this group until all descendants disappear. Our fixtures
    // never daemonize or escape their wrapper group; this is not a general host sweeper.
    let count = unsafe {
        libc::proc_listpgrppids(
            pgid,
            pids.as_mut_ptr().cast(),
            std::mem::size_of_val(&pids) as i32,
        )
    };
    assert!(count >= 0, "cannot enumerate owned singleton group");
    assert_eq!(pids[pids.len() - 1], 0, "group buffer must not fill");
    pids.into_iter()
        .filter(|pid| *pid > 0 && *pid != pgid)
        .collect()
}

fn cleanup_children() -> Vec<i32> {
    let mut reaped = Vec::new();
    for pid in children() {
        // WNOWAIT retains even a zombie leader as the group identity anchor. The runtime has
        // stopped and this singleton has no other wait owner. Never act on ECHILD/stale PIDs.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        assert_eq!(
            rc,
            0,
            "singleton child wait authority: {}",
            std::io::Error::last_os_error()
        );
        let pgid = unsafe { libc::getpgid(pid) };
        let target = if pgid == pid { -pid } else { pid };
        // Production held wrappers (and checkpoint commands) establish their own groups.
        // Signalling the anchored group also stops descendants before reaping its leader.
        let rc = unsafe { libc::kill(target, libc::SIGKILL) };
        assert!(rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH));
        let deadline = std::time::Instant::now() + CASE_TIMEOUT;
        let mut remaining = Vec::new();
        if target < 0 {
            loop {
                remaining = group_descendants(pid);
                if remaining.is_empty() || std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        let mut status = 0;
        loop {
            let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
            if rc == pid {
                break;
            }
            assert_eq!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EINTR)
            );
        }
        eprintln!("unsupported_gate singleton reaped pid={pid} pgid={pgid} status={status}");
        reaped.push(pid);
        assert!(
            remaining.is_empty(),
            "singleton descendants not reaped: {remaining:?}"
        );
    }
    assert!(children().is_empty(), "singleton child residue");
    eprintln!("unsupported_gate singleton remaining children: []");
    reaped
}

async fn context() -> SpawnCtx {
    let repo = Arc::new(SqlxRepo::open("sqlite::memory:").await.unwrap());
    for sql in [
        "INSERT INTO areas(id,name,color,sort,kind,created_at,updated_at) VALUES('area','a','#000',0,'user',0,0)",
        "INSERT INTO tracks(id,area_id,title,sort,created_at,updated_at) VALUES('track','area','t',0,0,0)",
        "INSERT INTO tasks (
            id, track_id, key, kind, goal, context_json,
            status, gate_attempt, created_at_ms, updated_at_ms
        ) VALUES (
            'track:gate', 'track', 'gate', 'terminal', 'g', '{}',
            'verifying', 1, 0, 0
        )",
    ] {
        sqlx::query(sql).execute(repo.pool()).await.unwrap();
    }
    let route_repo: Arc<dyn RouteRepo> = repo.clone();
    SpawnCtx::new(
        route_repo.clone(),
        Arc::new(SqlxOperationRepo::new(repo.pool().clone())),
        Arc::new(DaemonClient::new_stub()),
        TerminalRendererRegistry::new_with_repo(route_repo),
        EventBus::new(),
        OperationCompletionBus::new(),
    )
}

fn operation(output: &TxOutput, kind: &str) -> Operation {
    Operation {
        id: "unsupported-gate-op".into(),
        operation_key: "unsupported-gate-key".into(),
        kind: kind.into(),
        idempotency_key: None,
        payload_hash: String::new(),
        target_type: "task".into(),
        target_id: Some("track:gate".into()),
        target: Value::Null,
        payload: Value::Null,
        tx_output: Some(output.clone()),
        phase: Phase::SpawnStarted,
        phase_detail: None,
        attempt: 0,
        last_error: None,
        compensation_state: None,
        lease_owner: None,
        lease_until_ms: None,
        spawn_artifacts: None,
        parked_at_ms: None,
        parked_deadline_ms: None,
    }
}

fn gate() -> GateSpec {
    GateSpec {
        cwd: None,
        timeout_secs: Some(1),
        steps: vec![GateStep {
            name: "sentinel".into(),
            cmd: "printf executed > step-executed".into(),
        }],
    }
}

fn assert_pre_spawn_refusal<T>(result: Result<T>, dir: &Path, paths: &GatePaths) -> String {
    let error = match result {
        Err(error) => error.to_string(),
        Ok(_) => panic!("unsupported gate was allowed to spawn"),
    };
    for path in [
        &paths.script,
        &paths.log,
        &paths.step,
        &paths.exit,
        &dir.join("step-executed"),
    ] {
        assert!(
            !path.exists(),
            "gate created {} before refusal",
            path.display()
        );
    }
    assert!(
        error.contains("gate-infra") && error.contains("unsupported"),
        "{error}"
    );
    error
}

isolated_test!(
    unsupported_gate_shared_entry_refuses_before_wrapper,
    |_case| {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context().await;
        let paths = GatePaths::new(dir.path(), "direct");
        let result = spawn_held_identified(
            ctx.repo.as_ref(),
            dir.path(),
            &gate().steps,
            &paths,
            "direct",
        )
        .await;
        // Inspect the error without consuming a returned Child before the assertion.
        let result = result
            .as_ref()
            .map(|_| ())
            .map_err(|e| CalmError::Internal(e.to_string()));
        assert_pre_spawn_refusal(result, dir.path(), &paths);
    }
);

async fn verify_unbound(reason: Option<FrozenUnbound>) {
    let dir = tempfile::tempdir().unwrap();
    let ctx = context().await;
    let adapter = TaskVerifyAdapter::new(dir.path().to_path_buf());
    let mut output = TxOutput::new("task", Some("track:gate".into()), json!({}));
    output.data = serde_json::to_value(FrozenVerify {
        task_id: "track:gate".into(),
        track_id: "track".into(),
        area_id: "area".into(),
        key: "gate".into(),
        attempt: 1,
        cwd: dir.path().display().to_string(),
        gate: gate(),
        target: FrozenTarget::Unbound {
            reason: reason.unwrap_or(FrozenUnbound::LegacyFrozen),
        },
    })
    .unwrap();
    if reason.is_none() {
        output.data.as_object_mut().unwrap().remove("target");
    }
    let op = operation(&output, "task-verify");
    let paths = GatePaths::new(dir.path(), "track:gate-g1");
    let result = adapter.spawn_side_effect(&output, &op, &ctx).await;
    let error = assert_pre_spawn_refusal(result, dir.path(), &paths);
    let plan = adapter
        .plan_compensation(PhaseTag::SpawnStarted, &error, &output, &op)
        .await
        .unwrap();
    let fail = plan
        .steps
        .iter()
        .find(|step| step.op == "fail_task_gate_infra")
        .unwrap();
    adapter
        .compensate_step(fail, &output, &op, &ctx)
        .await
        .unwrap();
    let (status, verdict): (String, String) =
        sqlx::query_as("SELECT status, gate_result_json FROM tasks WHERE id='track:gate'")
            .fetch_one(&ctx.operation_repo.sqlite_pool())
            .await
            .unwrap();
    let verdict: Value = serde_json::from_str(&verdict).unwrap();
    assert_eq!(status, "failed");
    assert_eq!(verdict["passed"], false);
    assert_eq!(verdict["status_detail"], "gate-infra");
}

isolated_test!(
    unsupported_gate_terminal_unbound_refuses_before_wrapper,
    |_case| {
        verify_unbound(Some(FrozenUnbound::Terminal)).await;
    }
);

isolated_test!(
    unsupported_gate_legacy_lease_unbound_refuses_before_wrapper,
    |_case| {
        verify_unbound(Some(FrozenUnbound::LegacyLease)).await;
    }
);

isolated_test!(
    unsupported_gate_legacy_frozen_unbound_refuses_before_wrapper,
    |_case| {
        verify_unbound(None).await;
    }
);

isolated_test!(
    unsupported_gate_task_gate_run_refuses_before_wrapper,
    |_case| {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context().await;
        let adapter = TaskGateRunAdapter::new(dir.path().to_path_buf());
        let mut output = TxOutput::new("task", Some("track:gate".into()), json!({}));
        output.data = serde_json::to_value(FrozenRun {
            task_id: "track:gate".into(),
            track_id: "track".into(),
            card_id: "worker".into(),
            run: 1,
            cwd: dir.path().display().to_string(),
            branch: "test-branch".into(),
            base_sha: "unused-before-release".into(),
            canonical_path: dir.path().display().to_string(),
            git_common_dir: dir.path().join(".git").display().to_string(),
            ref_name: "refs/neige/gate-runs/track/worker/r1".into(),
            message: "must not checkpoint".into(),
            gate: gate(),
        })
        .unwrap();
        let op = operation(&output, "task-gate-run");
        let paths = GatePaths::new(dir.path(), "track:gate-r1");
        let result = adapter.spawn_side_effect(&output, &op, &ctx).await;
        let error = assert_pre_spawn_refusal(result, dir.path(), &paths);
        let result = finalize::terminal_result(
            1,
            &paths.log,
            OperationOutcome::Failed {
                last_error: error,
                from_phase: PhaseTag::SpawnStarted,
                last_error_class: None,
            },
        );
        assert!(!result.verdict.passed);
        assert_eq!(result.verdict.status_detail.as_deref(), Some("gate-infra"));
        assert!(result.commit.is_none());
    }
);

isolated_test!(
    unsupported_gate_cleanup_preserves_neighbor,
    |case| {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context().await;
        let paths = GatePaths::new(dir.path(), "cleanup-fixture");
        // The real lower-level wrapper creates the residue deliberately, without changing the
        // platform refusal or copying its validation. Never release it or poll an observer.
        let child = super::super::gate_process::spawn_held(
            ctx.repo.as_ref(),
            dir.path(),
            &gate().steps,
            paths.files(),
            "cleanup-fixture",
        )
        .await
        .unwrap();
        let pid = child.id().unwrap();
        case.held.push(child);
        std::fs::write(case.dir.join("wrapper"), pid.to_string()).unwrap();
        if case.mode == "timeout" {
            std::future::pending::<()>().await;
        }
        assert_eq!(case.mode, "panic");
        panic!("deliberate wrapper cleanup regression");
    },
    |name| {
        for mode in ["panic", "timeout"] {
            let mut case = CaseProcess::spawn(name, mode);
            // entered is the handshake: cleanup scope is established BEFORE the neighbor exists,
            // and release ensures cleanup cannot finish until AFTER its owner has a live Child.
            let mut neighbor = OwnedChild(
                std::process::Command::new("/bin/sh")
                    .args(["-c", "read -r done"])
                    .stdin(std::process::Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
            eprintln!(
                "unsupported_gate protected neighbor pid={} mode={mode}",
                neighbor.0.id()
            );
            case.release();
            let (status, report) = case.finish();
            // Observe both liveness and wait ownership before cleaning up, but assert only after
            // explicit wait so assertion failures cannot leak the protected fixture.
            let alive = neighbor.0.try_wait();
            drop(neighbor.0.stdin.take());
            let waited = neighbor.wait();
            eprintln!(
                "unsupported_gate neighbor preserved mode={mode} alive={alive:?} owner_wait={waited}"
            );
            assert!(
                matches!(alive, Ok(None)),
                "neighbor lost liveness/wait ownership: {alive:?}"
            );
            assert_eq!(
                waited.code(),
                Some(1),
                "neighbor must exit from stdin EOF, not a signal"
            );
            assert_eq!(status.code(), Some(101));
            assert_eq!(report["outcome"], mode);
            if mode == "panic" {
                assert_eq!(report["panic"], "deliberate wrapper cleanup regression");
            }
            let wrapper: i32 = std::fs::read_to_string(case.dir.path().join("wrapper"))
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                report["reaped"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(wrapper)),
                "wrapper must be reaped by singleton cleanup: {report}"
            );
        }
    }
);
