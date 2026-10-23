# Native macOS execution

Newly built binaries support registered **Private ExternalDir + Pipe** execution
for `task-verify`, `task-gate-run`, forge commands and their verdict/output probes,
and the provider rewind checks already connected to the registered executor.
These consumers use its durable journal, authorization, release, stop, and
retirement lifecycle. Following the registered Claude migration, ordinary turns
and compaction also use this executor. Their completion owner accepts either
Linux drain or Mac original-group settlement, while unconfirmed execution still
cannot certify a successful provider result. This does not establish full native
Claude feature parity or upgrade an already running Neige installation.

`neige-app` creates a private `execution-client-key.json` with mode `0600` in its
configured data directory and passes the same key path to the kernel and process
supervisor. The supervisor also receives its execution journal directory.
These paired arguments are required for registered execution. Launches use the
registered directory handle and the caller's explicit environment; they do not
inherit every variable or descriptor from the supervisor.

## Completion and recovery

The macOS supervisor owns the payload child directly. While that root remains
unreaped, its PID pins the original process group. Completion records
`OriginalGroupEmpty` only after observing root exit, observing no other member of
that original group, reaping the owned root, and completing bounded output
collection. A root that exits before same-group children triggers the existing
stop path. Stop uses TERM, then KILL after the grace period, through the same
owned handle. Nonzero exit and timeout do not become successful gate results.
Task cancellation preserves the canceled task outcome and keeps the checkout
busy until the running gate and delivery end; it does not release resources early.
Gate target sampling still runs before and after the steps.

This is an observation of the original group, **not a full descendant proof**.
Programs can deliberately leave it with `setsid`, `setpgid`, job control, or
daemonization. Such background processes can escape observation and cleanup;
their departure may be undetectable, and an original-group completion can still
be recorded. Run commands that remain in the foreground. Do not use this path
when safe handoff requires proving that every possible descendant has ended.

Lost live handles, failed identity/group observations, and incomplete output
cannot certify successful completion. After a supervisor restart, a formerly
active execution on the same OS boot remains `Unknown`; its journal, alias, and
resources remain reserved. The replacement does not signal a PID from stored
metadata, guess a new owner, or automatically rerun the command. A genuine OS
boot change is separate evidence that the old child no longer exists. Failure
to read the real boot UUID refuses startup; no synthetic boot identity is used.

Linux retains its helper/subreaper lifecycle and full descendant meaning for
`Drained` / `NoChildrenAfterForkEntryClosed`. Mac original-group evidence must
never be presented as that Linux proof. The native implementation writes journal
version 5 and retains receipt contract 4. Valid Linux version 4/5 journals remain
readable without filling missing facts; version 4 is normalized to version 5 on
open. Registered wire envelope 10 requires a matching peer. Legacy terminal
negotiation remains separate.

## Limits and macOS permissions

Registered strong Worktree isolation and registered PTY are unsupported on macOS.
The legacy terminal remains available. Secure file operations requiring Linux
`openat2` keep their existing refusal behavior. There is no container backend or
OS application automation in this support scope.

Commands run with the launching host's OS identity. Access to protected folders,
desktop data, and other TCC-controlled resources depends on how that host was
started and the permissions macOS grants it. Native execution does not bypass
TCC, grant Full Disk Access, or change a provider's sandbox permissions. No desktop
capture or permission prompt is needed to verify this implementation.

## Isolated verification

Use a disposable checkout and private temporary data, journal, key, database,
socket, and workspace directories. Do not point tests at a running installation's
resources. Tests must retain ownership of every child, arm bounded cleanup before
fallible assertions, and finish cleanup before starting more children. Unknown
PIDs are diagnostic information only.

From the repository root, with the pinned Rust toolchain:

```sh
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-proc-supervisor --lib native_macos \
  -- --test-threads=1 --nocapture
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-session --lib execution_ancillary \
  -- --test-threads=1 --nocapture
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-proc-supervisor --test integration_suite native_macos \
  -- --test-threads=1 --nocapture
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-server --test mcp_integration_suite native_macos \
  -- --test-threads=1 --nocapture
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-server --test planner_harness_suite native_macos \
  -- --test-threads=1 --nocapture
env -u NEIGE_CODEX_BIN RUSTC_WRAPPER= CARGO_BUILD_JOBS=6 \
  cargo test --locked -p calm-server --test forge_action_adapter native_macos \
  -- --test-threads=1 --nocapture
```

Require a nonzero selected test count for each target/filter and preserve complete logs,
exit codes, and child/resource cleanup evidence. Follow the repository's text,
contract, and quick Rust gates as well; use real cargo-nextest for contract gates.
On macOS those shell gates require GNU grep and coreutils on the command's PATH.
The full contract suite currently requires a case-sensitive temporary filesystem
for plugin-name fixtures; see [#3110](https://github.com/keanji-x/neige-calm/issues/3110).
Set `TMPDIR` for that command to a private directory on a case-sensitive volume,
retain the full suite and its assertions, and clean up only resources owned by
that verification run. The native gate tests also cover a failed task followed
by a successful task on the same isolated instance, track, and checkout through
worker report, candidate delivery, and `task-verify`.
An execution sandbox may deny `kern.bootsessionuuid` even when a native terminal
can read it. Record that denial and rerun the exact check in an authorized native
terminal; it is neither a pass nor a reason to weaken the identity check. A running
older kernel may also refuse a Mac gate before executing any step. Verify the
fixed saved revision independently without replacing the running service.
