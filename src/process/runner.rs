//! The attempt runner: a small blocking supervisor that outlives the daemon.
//!
//! The daemon spawns one runner per attempt in a fresh session. The runner
//! waits for durable launch authorization, forks the command into its own
//! process group, supervises it, delivers cancellation signals from outside
//! that group, and publishes an atomic terminal result only once the whole
//! command process group is provably empty.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::domain::{self, now_ms};
use crate::process::artifacts::{
    self, CANCEL_FILE, COMMAND_FILE, CancelFile, CommandFile, ExecFile, IDENTITY_FILE,
    IdentityFile, LIMIT_FILE, LimitFile, RESULT_FILE, RUNNER_LOCK_FILE, ResultFile, ResultOutcome,
    START_FILE, STDERR_LOG_FILE, STDOUT_LOG_FILE, StartFile, TIMEOUT_FILE, TimeoutFile,
};
use crate::process::identity;

/// Entry point for `mlqd __runner --attempt-dir DIR`. Returns the process
/// exit code; failures before `command.json` is readable cannot publish a
/// result and surface only through runner death.
pub fn runner_main(attempt_dir: &Path) -> i32 {
    match run(attempt_dir) {
        Ok(()) => 0,
        Err(err) => {
            log_line(attempt_dir, &format!("runner failed: {err:#}"));
            1
        }
    }
}

fn log_line(dir: &Path, message: &str) {
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join(artifacts::RUNNER_LOG_FILE))
    {
        let _ = writeln!(file, "[{}] {message}", now_ms());
    }
}

fn run(dir: &Path) -> anyhow::Result<()> {
    // The daemon ignores SIGCHLD to auto-reap detached runners, and ignored
    // dispositions survive exec: restore the default so waitpid on our own
    // command child works (and so the command inherits a clean disposition).
    unsafe {
        libc::signal(libc::SIGCHLD, libc::SIG_DFL);
    }

    // A per-attempt exclusive lock prevents duplicate runners for one
    // attempt directory; held for the runner's whole life.
    let lock = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .open(dir.join(RUNNER_LOCK_FILE))?;
    let rc = unsafe {
        libc::flock(
            std::os::unix::io::AsRawFd::as_raw_fd(&lock),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    };
    if rc != 0 {
        anyhow::bail!("another runner already owns this attempt directory");
    }

    let command: CommandFile = artifacts::read_artifact(&dir.join(COMMAND_FILE))?
        .ok_or_else(|| anyhow::anyhow!("missing {COMMAND_FILE}"))?;
    let poll = Duration::from_millis(command.poll_ms.max(10));

    // Publish verified runner identity for the daemon to attach.
    let self_pid = std::process::id() as i32;
    let self_stat = identity::proc_stat(self_pid)
        .ok_or_else(|| anyhow::anyhow!("cannot read own /proc stat"))?;
    let identity_record = IdentityFile {
        token: command.token.clone(),
        runner_pid: self_pid,
        runner_start_time: self_stat.start_time,
        boot_id: identity::boot_id()?,
    };
    // Non-exclusive: a recovery runner replacing a dead predecessor (the
    // flock serializes them) must be able to re-announce itself over the
    // predecessor's stale identity.
    artifacts::write_artifact(dir, IDENTITY_FILE, &identity_record, false)?;
    log_line(dir, &format!("runner {self_pid} waiting for authorization"));

    // Wait for durable launch authorization or committed cancellation.
    let deadline = Instant::now() + Duration::from_millis(command.start_wait_ms);
    loop {
        if let Some(cancel) = read_cancel(dir, &command.token) {
            let _ = cancel;
            log_line(dir, "cancelled before launch authorization");
            return publish_result(
                dir,
                &command,
                ResultOutcome::Cancelled,
                None,
                None,
                Some("cancelled before launch".to_string()),
            );
        }
        if command.window_end_at.is_some_and(|end| now_ms() >= end) {
            log_line(
                dir,
                "run window closed while waiting for launch authorization",
            );
            return publish_result(
                dir,
                &command,
                ResultOutcome::WindowClosed,
                None,
                None,
                Some("run window closed before command launch".into()),
            );
        }
        match artifacts::read_artifact::<StartFile>(&dir.join(START_FILE)) {
            Ok(Some(start)) if start.token == command.token => break,
            Ok(Some(_)) => {
                return publish_result(
                    dir,
                    &command,
                    ResultOutcome::LaunchFailed,
                    None,
                    None,
                    Some("authorization token mismatch".to_string()),
                );
            }
            Ok(None) => {}
            // A partially visible artifact is unexpected (writes are atomic);
            // treat as fatal rather than guessing.
            Err(err) => {
                return publish_result(
                    dir,
                    &command,
                    ResultOutcome::LaunchFailed,
                    None,
                    None,
                    Some(format!("corrupt start artifact: {err}")),
                );
            }
        }
        if Instant::now() >= deadline {
            log_line(dir, "timed out waiting for launch authorization");
            return publish_result(
                dir,
                &command,
                ResultOutcome::AuthorizationTimeout,
                None,
                None,
                Some(format!(
                    "no authorization within {} ms",
                    command.start_wait_ms
                )),
            );
        }
        std::thread::sleep(poll);
    }

    // Launch the command in its own process group. std::process reports
    // execve/chdir failure through its internal pipe, so a spawn error means
    // the command never ran.
    if command.window_end_at.is_some_and(|end| now_ms() >= end) {
        log_line(dir, "run window closed before command launch");
        return publish_result(
            dir,
            &command,
            ResultOutcome::WindowClosed,
            None,
            None,
            Some("run window closed before command launch".into()),
        );
    }
    let stdout = log_file(dir, STDOUT_LOG_FILE)?;
    let stderr = log_file(dir, STDERR_LOG_FILE)?;
    let mut cmd = Command::new(&command.argv[0]);
    cmd.args(&command.argv[1..])
        .current_dir(&command.cwd)
        .env_clear()
        .envs(&command.env)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            log_line(dir, &format!("launch failed: {err}"));
            return publish_result(
                dir,
                &command,
                ResultOutcome::LaunchFailed,
                None,
                None,
                Some(format!("launch failed: {err}")),
            );
        }
    };
    // The limit starts when the command is successfully spawned. This
    // intentionally includes any setup or compilation performed by the
    // submitted argv, while excluding time spent waiting in the queue.
    let command_started_at = now_ms();
    let command_started = Instant::now();
    // Adopt a limit published while this runner was waiting for authorization.
    // Later edits are read each poll until timeout termination begins.
    let mut live_limit = observed_time_limit(dir, &command);
    if live_limit.ms != command.time_limit_ms {
        log_time_limit(dir, live_limit.ms);
    }
    let child_pid = child.id() as i32;
    let pgid = child_pid;
    let child_start_time = identity::proc_stat(child_pid)
        .map(|stat| stat.start_time)
        .unwrap_or_default();
    artifacts::write_artifact(
        dir,
        artifacts::EXEC_FILE,
        &ExecFile {
            token: command.token.clone(),
            pid: child_pid,
            pgid,
            start_time: child_start_time,
            boot_id: identity::boot_id()?,
            started_at: Some(command_started_at),
        },
        true,
    )?;
    log_line(
        dir,
        &format!("command running: pid {child_pid} pgid {pgid}"),
    );

    // Supervision loop. Linearization of cancellation against natural
    // completion: a signal delivered before the exit was observed makes the
    // terminal result `cancelled`; a cancel that arrives after natural exit
    // is recorded as too late.
    let mut exit_status: Option<std::process::ExitStatus> = None;
    let mut term_sent = false;
    let mut cancelled_before_exit = false;
    let mut cancel_too_late = false;
    let mut kill_at: Option<Instant> = None;
    let mut kill_sent = false;
    let mut timed_out = false;
    let mut window_closed = false;
    let mut committed_deadline = None;
    let mut empty_scans = 0u32;
    let grace = Duration::from_millis(command.cancel_grace_ms);
    loop {
        // Observe a natural exit before processing cancellation, so a command
        // that already finished in the last poll window wins the race and a
        // simultaneous cancel is recorded as too late rather than rewriting a
        // genuine completion.
        if exit_status.is_none()
            && let Some(status) = peek_exit(child_pid)?
        {
            // WNOWAIT peek: the leader stays a zombie, pinning its PID so the
            // numeric pgid cannot be recycled by an unrelated new group while
            // we scan for surviving members.
            log_line(dir, &format!("command leader exited: {status}"));
            exit_status = Some(status);
        }
        // Prove group completion before considering control requests or the
        // deadline. If the first empty scan lands at the deadline, wait for
        // the confirming scan instead of misreporting already-finished work.
        let group_empty = exit_status.is_some() && identity::group_empty_except(pgid, child_pid);
        if group_empty {
            empty_scans += 1;
            if empty_scans >= 2 {
                break;
            }
        } else {
            empty_scans = 0;
        }
        let cancel = read_cancel(dir, &command.token);
        // Once SIGTERM has been chosen because the deadline passed, the bound
        // that fired stays in force. A later extension cannot revive a process
        // group that has already been signaled.
        if !timed_out {
            let observed = observed_time_limit(dir, &command);
            if observed.ms != live_limit.ms {
                log_time_limit(dir, observed.ms);
            }
            live_limit = observed;
        }
        let time_limit = live_limit.ms.map(Duration::from_millis);
        let time_expired = time_limit.is_some_and(|limit| command_started.elapsed() >= limit);
        let limit_deadline = live_limit.ms.and_then(|limit| {
            i64::try_from(limit)
                .ok()
                .and_then(|limit| command_started_at.checked_add(limit))
        });
        let (expired, window_first) = expired_execution_deadline(
            limit_deadline,
            time_expired,
            command.window_end_at,
            now_ms(),
        );
        let timeout_deadline = committed_deadline.or(expired);
        let limit_set_at = if window_closed || window_first {
            None
        } else {
            live_limit.set_at
        };
        let deadline_expired = timed_out || expired.is_some();
        let timeout_classifies = deadline_expired
            && cancel.as_ref().is_none_or(|cancel| {
                match (cancel.requested_at, timeout_deadline) {
                    (Some(cancelled_at), Some(deadline)) => {
                        domain::timeout_wins_over_cancel(cancelled_at, deadline, limit_set_at)
                    }
                    // Old cancellation artifacts predate time limits and
                    // retain cancellation precedence for compatibility.
                    _ => false,
                }
            });
        if deadline_expired && !group_empty {
            if timeout_classifies && !timed_out {
                timed_out = true;
                window_closed = window_first;
                committed_deadline = timeout_deadline;
                if let Some(deadline) = timeout_deadline {
                    // Preserve the first deadline if the runner dies during grace.
                    let record = TimeoutFile {
                        token: command.token.clone(),
                        deadline,
                        observed_at: now_ms(),
                    };
                    if let Err(err) = artifacts::write_artifact(dir, TIMEOUT_FILE, &record, true)
                        && err.kind() != std::io::ErrorKind::AlreadyExists
                    {
                        log_line(dir, &format!("recording timeout commitment: {err}"));
                    }
                }
                let reason = if window_closed {
                    "run window closed".to_string()
                } else {
                    format!(
                        "time limit of {} ms exceeded",
                        live_limit.ms.expect("time deadline requires a limit")
                    )
                };
                log_line(
                    dir,
                    &format!("{reason}; delivering SIGTERM to group {pgid}"),
                );
            }
            if !term_sent {
                signal_group(pgid, libc::SIGTERM);
                term_sent = true;
            }
            // Unlike a normal cancellation, a time limit is a hard bound:
            // always escalate if the process group ignores SIGTERM.
            if kill_at.is_none() && !kill_sent {
                kill_at = Some(Instant::now() + grace);
            }
        }
        if let Some(cancel) = cancel {
            if !timed_out {
                if exit_status.is_none() {
                    cancelled_before_exit = true;
                } else {
                    cancel_too_late = true;
                }
            }
            if !term_sent {
                log_line(dir, &format!("delivering SIGTERM to group {pgid}"));
                signal_group(pgid, libc::SIGTERM);
                term_sent = true;
            }
            if cancel.force && kill_at.is_none() && !kill_sent {
                kill_at = Some(Instant::now() + grace);
            }
        }
        if let Some(at) = kill_at
            && Instant::now() >= at
        {
            log_line(
                dir,
                &format!("grace expired; delivering SIGKILL to group {pgid}"),
            );
            signal_group(pgid, libc::SIGKILL);
            kill_sent = true;
            kill_at = None;
        }
        std::thread::sleep(poll);
    }

    // Only now reap the zombie leader; the group is provably empty.
    unsafe {
        let mut status: libc::c_int = 0;
        libc::waitpid(child_pid, &mut status, 0);
    }
    drop(child);
    let status = exit_status.expect("loop exits only after leader exit");
    let (outcome, exit_code, term_signal, message) = if timed_out {
        (
            if window_closed {
                ResultOutcome::WindowClosed
            } else {
                ResultOutcome::TimedOut
            },
            status.code(),
            status.signal(),
            Some(if window_closed {
                "run window closed".into()
            } else {
                format!(
                    "time limit of {} ms exceeded",
                    live_limit.ms.expect("timed out requires a time limit")
                )
            }),
        )
    } else if cancelled_before_exit {
        (
            ResultOutcome::Cancelled,
            status.code(),
            status.signal(),
            Some("cancelled by request".to_string()),
        )
    } else {
        let message = cancel_too_late.then(|| "cancellation_too_late".to_string());
        (
            ResultOutcome::Exited,
            status.code(),
            status.signal(),
            message,
        )
    };
    publish_result(dir, &command, outcome, exit_code, term_signal, message)
}

/// Non-destructive exit-status check: `WNOWAIT` reports the status while
/// leaving the child as a reapable zombie, so its PID (and therefore the
/// command's pgid) stays pinned until we have finished scanning the group.
fn peek_exit(pid: i32) -> std::io::Result<Option<std::process::ExitStatus>> {
    // WNOWAIT is a waitid-only flag (waitpid rejects it with EINVAL).
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // With WNOHANG, "no state change yet" is rc == 0 with si_pid left zero.
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    let status = unsafe { info.si_status() };
    let raw = match info.si_code {
        libc::CLD_EXITED => (status & 0xff) << 8,
        libc::CLD_DUMPED => status | 0x80,
        _ => status, // CLD_KILLED: the terminating signal number
    };
    Ok(Some(std::process::ExitStatus::from_raw(raw)))
}

/// Return the earliest expired deadline and whether it came from the window.
fn expired_execution_deadline(
    limit: Option<i64>,
    limit_expired: bool,
    window: Option<i64>,
    now: i64,
) -> (Option<i64>, bool) {
    let window_expired = window.filter(|&end| now >= end);
    let limit_expired = limit.filter(|_| limit_expired);
    match (limit_expired, window_expired) {
        (Some(limit), Some(window)) if limit < window => (Some(limit), false),
        (_, Some(window)) => (Some(window), true),
        (limit, None) => (limit, false),
    }
}

fn log_file(dir: &Path, name: &str) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(dir.join(name))
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct LiveLimit {
    ms: Option<u64>,
    /// When the override was committed. None for the launch snapshot.
    set_at: Option<i64>,
}

/// The limit the runner should enforce right now. `limit.json` wins over the
/// launch snapshot in `command.json`, including an explicit clear. A missing,
/// mismatched, or unreadable override keeps that snapshot.
fn observed_time_limit(dir: &Path, command: &CommandFile) -> LiveLimit {
    match artifacts::read_artifact::<LimitFile>(&dir.join(LIMIT_FILE)) {
        Ok(Some(limit)) if limit.token == command.token => LiveLimit {
            ms: limit.time_limit_ms,
            set_at: limit.updated_at,
        },
        Ok(Some(_)) | Ok(None) | Err(_) => LiveLimit {
            ms: command.time_limit_ms,
            set_at: None,
        },
    }
}

fn log_time_limit(dir: &Path, limit: Option<u64>) {
    match limit {
        Some(ms) => log_line(dir, &format!("time limit is now {ms} ms")),
        None => log_line(dir, "time limit cleared"),
    }
}

fn read_cancel(dir: &Path, token: &str) -> Option<CancelFile> {
    match artifacts::read_artifact::<CancelFile>(&dir.join(CANCEL_FILE)) {
        Ok(Some(cancel)) if cancel.token == token => Some(cancel),
        _ => None,
    }
}

fn signal_group(pgid: i32, signal: i32) {
    // ESRCH (already empty) is fine; anything else is unexpected but the
    // /proc scan remains the source of truth for emptiness.
    unsafe {
        libc::killpg(pgid, signal);
    }
}

fn publish_result(
    dir: &Path,
    command: &CommandFile,
    outcome: ResultOutcome,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    message: Option<String>,
) -> anyhow::Result<()> {
    let result = ResultFile {
        token: command.token.clone(),
        outcome,
        exit_code,
        term_signal,
        message,
        finished_at: now_ms(),
    };
    artifacts::write_artifact(dir, RESULT_FILE, &result, true)?;
    log_line(
        dir,
        &format!("result published: {outcome:?} code={exit_code:?} signal={term_signal:?}"),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn run_window_deadline_survives_a_cleared_or_extended_time_limit() {
        assert_eq!(
            expired_execution_deadline(None, false, Some(100), 99),
            (None, false)
        );
        assert_eq!(
            expired_execution_deadline(None, false, Some(100), 100),
            (Some(100), true)
        );
        assert_eq!(
            expired_execution_deadline(Some(200), false, Some(100), 100),
            (Some(100), true)
        );
        assert_eq!(
            expired_execution_deadline(Some(50), true, Some(100), 120),
            (Some(50), false)
        );
        assert_eq!(
            expired_execution_deadline(Some(150), true, Some(100), 200),
            (Some(100), true)
        );
        assert_eq!(
            expired_execution_deadline(Some(100), true, Some(100), 100),
            (Some(100), true)
        );
    }

    #[test]
    fn legacy_commands_have_no_window_deadline() {
        let mut json = serde_json::to_value(command(None)).unwrap();
        json.as_object_mut().unwrap().remove("window_end_at");
        let old: CommandFile = serde_json::from_value(json).unwrap();
        assert!(old.window_end_at.is_none());
    }

    fn command(limit: Option<u64>) -> CommandFile {
        CommandFile {
            attempt_id: 1,
            job_id: 1,
            token: "token".into(),
            argv: vec!["true".into()],
            cwd: "/tmp".into(),
            env: BTreeMap::new(),
            start_wait_ms: 1,
            cancel_grace_ms: 1,
            poll_ms: 10,
            time_limit_ms: limit,
            window_end_at: None,
        }
    }

    #[test]
    fn observed_time_limit_prefers_a_matching_override_including_clear() {
        let dir = tempfile::tempdir().unwrap();
        let command = command(Some(1_000));
        assert_eq!(observed_time_limit(dir.path(), &command).ms, Some(1_000));

        artifacts::write_artifact(
            dir.path(),
            LIMIT_FILE,
            &LimitFile {
                token: "token".into(),
                time_limit_ms: Some(50),
                updated_at: Some(9),
            },
            false,
        )
        .unwrap();
        let observed = observed_time_limit(dir.path(), &command);
        assert_eq!(observed.ms, Some(50));
        assert_eq!(observed.set_at, Some(9));

        artifacts::write_artifact(
            dir.path(),
            LIMIT_FILE,
            &LimitFile {
                token: "token".into(),
                time_limit_ms: None,
                updated_at: Some(10),
            },
            false,
        )
        .unwrap();
        assert_eq!(observed_time_limit(dir.path(), &command).ms, None);

        artifacts::write_artifact(
            dir.path(),
            LIMIT_FILE,
            &LimitFile {
                token: "other".into(),
                time_limit_ms: Some(1),
                updated_at: Some(11),
            },
            false,
        )
        .unwrap();
        assert_eq!(observed_time_limit(dir.path(), &command).ms, Some(1_000));

        std::fs::write(dir.path().join(LIMIT_FILE), b"{ not json").unwrap();
        assert_eq!(observed_time_limit(dir.path(), &command).ms, Some(1_000));
    }
}
