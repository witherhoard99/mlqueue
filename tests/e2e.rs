//! End-to-end tests: a real daemon, real runners, and the real CLI, all
//! isolated in a per-test state directory.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn wait_for(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    loop {
        if check() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

struct TestQueue {
    dir: tempfile::TempDir,
    daemon: Option<Child>,
}

struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        self.stop();
    }
}

impl TestQueue {
    fn new() -> Self {
        Self::with_runner_poll_ms(20)
    }

    fn with_runner_poll_ms(runner_poll_ms: u64) -> Self {
        Self::with_config(runner_poll_ms, "")
    }

    /// `extra` appends config keys for one test; TOML rejects duplicates, so
    /// they must not already be set below.
    fn with_config(runner_poll_ms: u64, extra: &str) -> Self {
        // Keep the tempdir shallow: the Unix socket path must stay under the
        // 108-byte sockaddr_un limit regardless of TMPDIR.
        let dir = tempfile::Builder::new()
            .prefix("mlq-e2e")
            .tempdir_in("/tmp")
            .unwrap();
        std::fs::write(
            dir.path().join("config.toml"),
            format!(
                "version = 1\n\
                 tick_interval_ms = 40\n\
                 runner_poll_ms = {runner_poll_ms}\n\
                 cancel_grace_ms = 800\n\
                 runner_identity_grace_ms = 5000\n\
                 idle_timeout_ms = 200\n\
                 {extra}"
            ),
        )
        .unwrap();
        let mut queue = Self { dir, daemon: None };
        queue.start_daemon();
        queue
    }

    fn socket(&self) -> PathBuf {
        self.dir.path().join("runtime/mlqd.sock")
    }

    fn state_dir(&self) -> PathBuf {
        self.dir.path().join("state")
    }

    fn apply_env(&self, cmd: &mut Command) {
        cmd.env("MLQUEUE_STATE_DIR", self.state_dir())
            .env("MLQUEUE_RUNTIME_DIR", self.dir.path().join("runtime"))
            .env("MLQUEUE_CONFIG_FILE", self.dir.path().join("config.toml"));
    }

    fn spawn_daemon(&self) -> Child {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlqd"));
        self.apply_env(&mut cmd);
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        cmd.spawn().unwrap()
    }

    fn start_daemon(&mut self) {
        assert!(self.daemon.is_none());
        self.daemon = Some(self.spawn_daemon());
        wait_for(
            "daemon to accept connections",
            Duration::from_secs(10),
            || self.try_cli(&["daemon", "status"]).status.success(),
        );
    }

    /// Simulate a crash: SIGKILL, so nothing shuts down cleanly.
    fn kill_daemon(&mut self) {
        let mut child = self.daemon.take().expect("daemon running");
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Graceful shutdown: SIGTERM, then wait for the daemon to exit.
    fn stop_daemon(&mut self) {
        let mut child = self.daemon.take().expect("daemon running");
        assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
        child.wait().unwrap();
    }

    fn try_cli(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlq"));
        self.apply_env(&mut cmd);
        cmd.args(args);
        cmd.output().unwrap()
    }

    fn cli(&self, args: &[&str]) -> String {
        let output = self.try_cli(args);
        assert!(
            output.status.success(),
            "mlq {args:?} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn cli_json(&self, args: &[&str]) -> serde_json::Value {
        serde_json::from_str(&self.cli(args)).expect("CLI produced valid JSON")
    }

    /// Submit a command; extra flags first, command after `--`.
    fn submit(&self, flags: &[&str], command: &[&str]) -> i64 {
        let mut args = vec!["submit", "--json"];
        args.extend_from_slice(flags);
        args.push("--cwd");
        let cwd = self.dir.path().to_str().unwrap();
        args.push(cwd);
        args.push("--");
        args.extend_from_slice(command);
        self.cli_json(&args)["id"]
            .as_i64()
            .expect("submit returns a job id")
    }

    fn job(&self, id: i64) -> serde_json::Value {
        self.cli_json(&["show", &id.to_string(), "--json"])
    }

    fn job_state(&self, id: i64) -> String {
        self.job(id)["state"].as_str().unwrap().to_string()
    }

    fn wait_state(&self, id: i64, state: &str, timeout: Duration) {
        wait_for(&format!("job {id} to be {state}"), timeout, || {
            self.job_state(id) == state
        });
    }

    fn status(&self) -> serde_json::Value {
        self.cli_json(&["status", "--json"])
    }

    /// Start `mlq subscribe` and read its NDJSON stream on a helper thread, so
    /// a test can wait for the next snapshot with a deadline.
    fn subscribe(&self) -> Subscriber {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_mlq"));
        self.apply_env(&mut cmd);
        cmd.arg("subscribe")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = cmd.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stdout).lines() {
                if tx.send(line.unwrap()).is_err() {
                    return;
                }
            }
        });
        Subscriber {
            child: KillOnDrop(Some(child)),
            lines,
        }
    }
}

impl Drop for TestQueue {
    fn drop(&mut self) {
        if let Some(mut child) = self.daemon.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A live `mlq subscribe` process plus its decoded snapshot stream.
struct Subscriber {
    child: KillOnDrop,
    lines: std::sync::mpsc::Receiver<String>,
}

impl Subscriber {
    fn next_snapshot(&self, timeout: Duration) -> serde_json::Value {
        let line = self
            .lines
            .recv_timeout(timeout)
            .expect("a snapshot before the deadline");
        serde_json::from_str(&line).expect("every snapshot line is valid JSON")
    }

    /// Read frames until one satisfies `matches`; the frames skipped on the way
    /// are reported if the deadline passes first.
    fn wait_snapshot(
        &self,
        what: &str,
        timeout: Duration,
        mut matches: impl FnMut(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let deadline = Instant::now() + timeout;
        let mut skipped = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.lines.recv_timeout(remaining).unwrap_or_else(|err| {
                panic!("no snapshot showed {what} ({err}); saw {skipped:#?}")
            });
            let snapshot: serde_json::Value =
                serde_json::from_str(&line).expect("every snapshot line is valid JSON");
            if matches(&snapshot) {
                return snapshot;
            }
            skipped.push(snapshot);
        }
    }

    /// An unchanged queue must produce no frames while keeping the stream open.
    fn expect_silence(&self, within: Duration) {
        use std::sync::mpsc::RecvTimeoutError;
        match self.lines.recv_timeout(within) {
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => panic!("the subscription was closed while idle"),
            Ok(line) => panic!("unexpected snapshot on an unchanged queue: {line}"),
        }
    }

    /// The daemon shutting down must end the stream and exit `mlq` cleanly.
    fn expect_clean_end(&mut self, timeout: Duration) {
        use std::sync::mpsc::RecvTimeoutError;
        let deadline = Instant::now() + timeout;
        // Drain any final snapshot still in flight; `Disconnected` is EOF.
        while let Ok(_) | Err(RecvTimeoutError::Timeout) =
            self.lines.recv_timeout(Duration::from_millis(50))
        {
            assert!(Instant::now() < deadline, "the subscription did not end");
        }
        let child = self.child.0.as_mut().expect("subscriber running");
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "mlq subscribe did not exit");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(
            status.code(),
            Some(0),
            "a clean daemon shutdown is not a client error"
        );
    }
}

#[test]
fn subscribe_streams_snapshots_outlives_idle_and_ends_on_shutdown() {
    let mut q = TestQueue::new();
    let mut subscriber = q.subscribe();

    // One complete snapshot arrives immediately, before anything changes.
    let initial = subscriber.next_snapshot(Duration::from_secs(10));
    assert!(initial["jobs"].as_array().unwrap().is_empty(), "{initial}");
    assert_eq!(initial["activeLeases"], 0);
    assert_eq!(initial["admissionBlocked"], false);

    // Idle for longer than the configured idle timeout (200ms, floored to 1s by
    // the daemon): an unchanged queue must stay silent and stay connected.
    subscriber.expect_silence(Duration::from_millis(1_500));

    // Work submitted on another connection is pushed to this one, and a
    // runner-driven transition arrives without any polling.
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "30"]);
    let running =
        subscriber.wait_snapshot("the blocker running", Duration::from_secs(10), |snapshot| {
            job_in(snapshot, blocker)["state"] == "running"
        });
    assert_eq!(running["activeLeases"], 1, "{running}");
    assert_eq!(running["effectiveLimit"], 1, "{running}");

    // Client-driven transitions arrive in order, each as a whole snapshot.
    let waiting = q.submit(&["--name", "waiting"], &["sh", "-c", "true"]);
    subscriber.wait_snapshot(
        "the second job queued",
        Duration::from_secs(10),
        |snapshot| job_in(snapshot, waiting)["state"] == "queued",
    );
    q.cli(&["hold", &waiting.to_string()]);
    subscriber.wait_snapshot("the job held", Duration::from_secs(10), |snapshot| {
        job_in(snapshot, waiting)["state"] == "held"
    });
    q.cli(&["release", &waiting.to_string()]);
    subscriber.wait_snapshot("the job released", Duration::from_secs(10), |snapshot| {
        job_in(snapshot, waiting)["state"] == "queued"
    });

    // Freeing the lease lets the queued job run through to its outcome.
    q.cli(&["cancel", &blocker.to_string()]);
    let done =
        subscriber.wait_snapshot("the job succeeding", Duration::from_secs(20), |snapshot| {
            job_in(snapshot, waiting)["state"] == "succeeded"
        });
    assert_eq!(job_in(&done, blocker)["state"], "cancelled", "{done}");

    // Shutting the daemon down closes the stream instead of hanging.
    q.stop_daemon();
    subscriber.expect_clean_end(Duration::from_secs(10));
}

#[test]
fn subscribe_puts_running_jobs_before_finished_history() {
    let q = TestQueue::new();
    let finished = q.submit(&["--name", "finished"], &["sh", "-c", "true"]);
    q.wait_state(finished, "succeeded", Duration::from_secs(10));

    let subscriber = q.subscribe();
    let initial = subscriber.next_snapshot(Duration::from_secs(10));
    assert_eq!(
        job_in(&initial, finished)["state"],
        "succeeded",
        "{initial}"
    );

    let running = q.submit(&["--name", "active"], &["sleep", "30"]);
    let snapshot =
        subscriber.wait_snapshot("the active job", Duration::from_secs(10), |snapshot| {
            job_in(snapshot, running)["state"] == "running"
        });
    assert_eq!(snapshot["jobs"][0]["id"], running, "{snapshot}");
    assert_eq!(snapshot["jobs"][0]["state"], "running", "{snapshot}");

    q.cli(&["cancel", &running.to_string()]);
    q.wait_state(running, "cancelled", Duration::from_secs(10));
}

/// The job with `id` in a status snapshot, or `null` when it is absent.
fn job_in(snapshot: &serde_json::Value, id: i64) -> &serde_json::Value {
    snapshot["jobs"]
        .as_array()
        .expect("snapshots carry a jobs array")
        .iter()
        .find(|job| job["id"] == id)
        .unwrap_or(&serde_json::Value::Null)
}

/// The exact bytes a non-Rust consumer sends to open a subscription.
const RAW_SUBSCRIBE: &[u8] =
    br#"{"protocol_version":9,"request_id":"raw-1","op":{"type":"subscribe"}}"#;

fn raw_subscribe(socket: &std::path::Path) -> (std::os::unix::net::UnixStream, serde_json::Value) {
    try_raw_subscribe(socket).expect("the daemon answered the subscribe request")
}

/// `None` when the daemon closed the connection without answering, which is what
/// being over the *connection* budget looks like.
fn try_raw_subscribe(
    socket: &std::path::Path,
) -> Option<(std::os::unix::net::UnixStream, serde_json::Value)> {
    use mlqueue::protocol::{DEFAULT_MAX_FRAME_BYTES, read_frame_sync, write_frame_sync};

    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write_frame_sync(&mut stream, RAW_SUBSCRIBE, DEFAULT_MAX_FRAME_BYTES).ok()?;
    let frame = read_frame_sync(&mut stream, DEFAULT_MAX_FRAME_BYTES).ok()??;
    let response = serde_json::from_slice(&frame).expect("frames carry JSON responses");
    Some((stream, response))
}

fn read_response(stream: &mut std::os::unix::net::UnixStream) -> serde_json::Value {
    use mlqueue::protocol::{DEFAULT_MAX_FRAME_BYTES, read_frame_sync};

    let frame = read_frame_sync(stream, DEFAULT_MAX_FRAME_BYTES)
        .expect("a readable frame")
        .expect("the daemon kept the stream open");
    serde_json::from_slice(&frame).expect("frames carry JSON responses")
}

#[test]
fn raw_subscribe_echoes_the_request_id_and_budgets_its_slot() {
    // One stream slot (a quarter of the connection budget, at least one), so the
    // refusal and the release of a slot are both deterministic.
    let q = TestQueue::with_config(20, "max_connections = 4\n");

    let (mut first_stream, initial) = raw_subscribe(&q.socket());
    assert_eq!(
        initial["request_id"], "raw-1",
        "the subscribe request id is echoed"
    );
    assert_eq!(initial["reply"]["type"], "status", "{initial}");
    assert!(initial["error"].is_null(), "{initial}");

    let job = q.submit(&["--name", "raw"], &["sh", "-c", "true"]);
    let pushed = read_response(&mut first_stream);
    assert_eq!(
        pushed["request_id"], "raw-1",
        "every frame echoes the same id"
    );
    assert_eq!(job_in(&pushed["reply"], job)["id"], job, "{pushed}");

    // A second stream does not fit, and is told so instead of being dropped.
    let (_refused_stream, refused) = raw_subscribe(&q.socket());
    assert_eq!(
        refused["error"]["code"], "too_many_subscriptions",
        "{refused}"
    );
    assert!(refused["reply"].is_null(), "{refused}");
    // Ordinary request/response clients still have headroom.
    assert!(q.try_cli(&["daemon", "status"]).status.success());

    // Disconnecting frees the slot again.
    drop(first_stream);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some((_stream, response)) = try_raw_subscribe(&q.socket())
            && response["error"].is_null()
        {
            assert_eq!(response["reply"]["type"], "status", "{response}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the subscription slot was never released"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn submit_runs_captures_logs_and_reports() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "hello"],
        &["sh", "-c", "echo out-marker; echo err-marker >&2"],
    );
    q.wait_state(job, "succeeded", Duration::from_secs(10));

    let detail = q.job(job);
    assert_eq!(
        detail["maxParallelRuns"], 1,
        "default limit is the conservative 1"
    );
    assert_eq!(detail["name"], "hello");
    let attempt = &detail["attempts"][0];
    assert_eq!(attempt["state"], "succeeded");
    assert_eq!(attempt["exitCode"], 0);

    let stdout = q.cli(&["logs", &job.to_string()]);
    assert!(stdout.contains("out-marker"), "stdout log: {stdout:?}");
    let stderr = q.cli(&["logs", &job.to_string(), "--stderr"]);
    assert!(stderr.contains("err-marker"), "stderr log: {stderr:?}");
}

#[test]
fn time_limit_excludes_queue_wait_but_bounds_the_whole_attempt() {
    let q = TestQueue::new();

    // This job waits behind the blocker longer than its own time limit, but
    // should still succeed because the clock begins only when its command is
    // spawned.
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "0.6"]);
    let queued = q.submit(
        &["--name", "queued-limit", "--time-limit", "250ms"],
        &["sleep", "0.05"],
    );
    q.wait_state(blocker, "succeeded", Duration::from_secs(10));
    q.wait_state(queued, "succeeded", Duration::from_secs(10));
    assert_eq!(q.job(queued)["timeLimitMs"], 250);

    // The limit covers the complete submitted command and is hard: a group
    // that ignores SIGTERM is escalated after the configured grace period.
    let bounded = q.submit(
        &["--name", "bounded", "--time-limit", "200ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(bounded, "failed", Duration::from_secs(10));
    let detail = q.job(bounded);
    let attempt = &detail["attempts"][0];
    assert_eq!(attempt["state"], "failed");
    assert_eq!(attempt["termSignal"], libc::SIGKILL);
    assert!(
        attempt["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "attempt detail: {attempt}"
    );
}

#[test]
fn detached_runner_keeps_the_time_limit_across_daemon_restart() {
    let mut q = TestQueue::new();
    let job = q.submit(
        &["--name", "restart-limit", "--time-limit", "300ms"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    q.kill_daemon();
    std::thread::sleep(Duration::from_millis(600));
    q.start_daemon();

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 300 ms exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn an_expired_time_limit_wins_over_a_later_cancellation() {
    // A long runner poll makes the ordering deterministic: the deadline and
    // cancellation are both first observed by the same supervision pass.
    let q = TestQueue::with_runner_poll_ms(500);
    let job = q.submit(
        &["--name", "timeout-race", "--time-limit", "200ms"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(300));
    q.cli(&["cancel", &job.to_string()]);

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn daemon_enforces_the_time_limit_after_the_runner_dies() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-limit", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    q.wait_state(job, "failed", Duration::from_secs(10));
    let detail = q.job(job);
    assert!(
        detail["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit exceeded")),
        "job detail: {detail}"
    );
}

#[test]
fn an_early_cancel_does_not_disable_runner_timeout_escalation() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "early-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    // Cancellation remains the terminal classification, but the later hard
    // deadline must still escalate the TERM-ignoring process group.
    q.wait_state(job, "cancelled", Duration::from_secs(10));
    assert_eq!(q.job(job)["attempts"][0]["termSignal"], libc::SIGKILL);
}

#[test]
fn an_early_cancel_does_not_disable_orphan_timeout_escalation() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-early-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    q.wait_state(job, "cancelled", Duration::from_secs(10));
}

#[test]
fn repeated_cancel_preserves_the_first_request_timestamp() {
    let q = TestQueue::with_runner_poll_ms(500);
    let job = q.submit(
        &["--name", "repeat-cancel", "--time-limit", "300ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);

    let attempt_dir = q.job(job)["attempts"][0]["logDir"]
        .as_str()
        .unwrap()
        .to_string();
    let cancel_path = PathBuf::from(&attempt_dir).join("cancel.json");
    let first: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cancel_path).unwrap()).unwrap();
    std::thread::sleep(Duration::from_millis(350));
    q.cli(&["cancel", &job.to_string(), "--force"]);
    let second: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&cancel_path).unwrap()).unwrap();
    assert_eq!(first["requested_at"], second["requested_at"]);
    assert_eq!(second["force"], true);

    q.wait_state(job, "cancelled", Duration::from_secs(10));
}

#[test]
fn set_time_limit_updates_queued_held_and_live_attempts() {
    let q = TestQueue::new();

    let blocker = q.submit(&["--name", "blocker"], &["sleep", "0.4"]);
    let queued = q.submit(&["--name", "queued-retune"], &["sleep", "30"]);
    let message = q.cli(&["set-time-limit", &queued.to_string(), "200ms"]);
    assert!(message.contains("time limit is now"), "message: {message}");
    assert_eq!(q.job(queued)["timeLimitMs"], 200);
    // Reapplying the same value is a no-op and must not disturb the job.
    q.cli(&["set-time-limit", &queued.to_string(), "200ms"]);
    assert_eq!(q.job(queued)["timeLimitMs"], 200);
    q.wait_state(blocker, "succeeded", Duration::from_secs(10));
    q.wait_state(queued, "failed", Duration::from_secs(10));
    assert!(
        q.job(queued)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "queued detail: {}",
        q.job(queued)
    );

    let held_blocker = q.submit(&["--name", "held-blocker"], &["sleep", "30"]);
    q.wait_state(held_blocker, "running", Duration::from_secs(10));
    let held = q.submit(&["--name", "held-retune"], &["sleep", "30"]);
    q.cli(&["hold", &held.to_string()]);
    q.cli(&["set-time-limit", &held.to_string(), "200ms"]);
    assert_eq!(q.job(held)["timeLimitMs"], 200);
    assert_eq!(q.job_state(held), "held");
    q.cli(&["release", &held.to_string()]);
    q.cli(&["cancel", &held_blocker.to_string()]);
    q.wait_state(held_blocker, "cancelled", Duration::from_secs(10));
    q.wait_state(held, "failed", Duration::from_secs(10));
    assert!(
        q.job(held)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "held detail: {}",
        q.job(held)
    );

    // An unlimited live attempt adopts a bound shorter than its elapsed time.
    let shorten = q.submit(&["--name", "live-shorten"], &["sleep", "30"]);
    q.wait_state(shorten, "running", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(300));
    let message = q.cli(&["set-time-limit", &shorten.to_string(), "200ms"]);
    assert!(
        message.contains("time limit is now 200ms"),
        "message: {message}"
    );
    q.wait_state(shorten, "failed", Duration::from_secs(10));
    assert!(
        q.job(shorten)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "shorten detail: {}",
        q.job(shorten)
    );

    // Extending before the original deadline lets the command finish.
    let extend = q.submit(
        &["--name", "live-extend", "--time-limit", "2s"],
        &["sleep", "3"],
    );
    q.wait_state(extend, "running", Duration::from_secs(10));
    q.cli(&["set-time-limit", &extend.to_string(), "30s"]);
    q.wait_state(extend, "succeeded", Duration::from_secs(15));

    // Clearing does too. The original bound would have fired during the sleep.
    let cleared = q.submit(
        &["--name", "live-clear", "--time-limit", "2s"],
        &["sleep", "3"],
    );
    q.wait_state(cleared, "running", Duration::from_secs(10));
    let message = q.cli(&["set-time-limit", &cleared.to_string(), "--clear"]);
    assert!(message.contains("time limit cleared"), "message: {message}");
    assert!(q.job(cleared)["timeLimitMs"].is_null());
    q.wait_state(cleared, "succeeded", Duration::from_secs(15));

    let finished = q.try_cli(&["set-time-limit", &cleared.to_string(), "1s"]);
    assert!(!finished.status.success());
    let stderr = String::from_utf8_lossy(&finished.stderr);
    assert!(stderr.contains("cannot be changed"), "stderr: {stderr}");

    let zero = q.try_cli(&["set-time-limit", "1", "0s"]);
    assert!(!zero.status.success());
    let stderr = String::from_utf8_lossy(&zero.stderr);
    assert!(stderr.contains("at least 1ms"), "stderr: {stderr}");
}

#[test]
fn updated_time_limit_survives_daemon_restart() {
    let mut q = TestQueue::new();
    let job = q.submit(
        &["--name", "restart-retune", "--time-limit", "2s"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["set-time-limit", &job.to_string(), "30s"]);

    q.kill_daemon();
    // Past the launch snapshot. The detached runner has to be enforcing the
    // updated limit, which no longer depends on the daemon staying up.
    std::thread::sleep(Duration::from_millis(2_500));
    q.start_daemon();
    assert_eq!(q.job_state(job), "running", "detail: {}", q.job(job));
    q.cli(&["cancel", &job.to_string()]);
    q.wait_state(job, "cancelled", Duration::from_secs(10));
}

#[test]
fn startup_reconciliation_publishes_a_time_limit_committed_while_down() {
    let mut q = TestQueue::new();
    let job = q.submit(
        &["--name", "republish-limit", "--time-limit", "30s"],
        &["sleep", "30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    let attempt_dir = q.job(job)["attempts"][0]["logDir"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(!PathBuf::from(&attempt_dir).join("limit.json").exists());

    q.kill_daemon();
    // The commit lands with nobody awake to publish limit.json. The launch
    // snapshot is still 30s, so the runner must not time out until reconciliation
    // publishes the shorter bound.
    let conn = mlqueue::db::open(&q.state_dir().join("mlqueue.db")).unwrap();
    mlqueue::db::set_job_time_limit(&conn, job, Some(200), mlqueue::domain::now_ms()).unwrap();
    drop(conn);

    q.start_daemon();
    q.wait_state(job, "failed", Duration::from_secs(10));
    let limit: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(&attempt_dir).join("limit.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(limit["time_limit_ms"], 200, "republished limit: {limit}");
    assert!(
        q.job(job)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit of 200 ms exceeded")),
        "detail: {}",
        q.job(job)
    );
}

#[test]
fn set_time_limit_bounds_an_orphaned_attempt() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-retune", "--time-limit", "30s"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    let attempt_dir = q.job(job)["attempts"][0]["logDir"]
        .as_str()
        .unwrap()
        .to_string();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(&attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);
    q.wait_state(job, "needs_attention", Duration::from_secs(10));
    q.cli(&["set-time-limit", &job.to_string(), "200ms"]);
    q.wait_state(job, "failed", Duration::from_secs(10));
    assert!(
        q.job(job)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit exceeded")),
        "orphan detail: {}",
        q.job(job)
    );
}

#[test]
fn shortening_a_limit_does_not_reclassify_an_earlier_cancel() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "cancel-then-shorten", "--time-limit", "30s"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    q.cli(&["cancel", &job.to_string()]);
    q.cli(&["set-time-limit", &job.to_string(), "200ms"]);
    q.wait_state(job, "cancelled", Duration::from_secs(10));
    assert_eq!(q.job(job)["attempts"][0]["state"], "cancelled");
}

#[test]
fn a_backdated_orphan_limit_waits_out_the_grace_period() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "orphan-grace", "--time-limit", "30s"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    let attempt_dir = q.job(job)["attempts"][0]["logDir"]
        .as_str()
        .unwrap()
        .to_string();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(&attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);
    q.wait_state(job, "needs_attention", Duration::from_secs(10));

    let exec: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(&attempt_dir).join("exec.json")).unwrap(),
    )
    .unwrap();
    let cmd_pid = exec["pid"].as_i64().unwrap() as i32;
    q.cli(&["set-time-limit", &job.to_string(), "1ms"]);
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        unsafe { libc::kill(cmd_pid, 0) },
        0,
        "SIGKILL must wait out the grace period"
    );
    q.wait_state(job, "failed", Duration::from_secs(10));
}

#[test]
fn a_started_timeout_continues_after_the_runner_dies_and_the_limit_is_cleared() {
    let q = TestQueue::new();
    let job = q.submit(
        &["--name", "committed-timeout", "--time-limit", "200ms"],
        &["sh", "-c", "trap '' TERM; exec sleep 30"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));
    let attempt_dir = q.job(job)["attempts"][0]["logDir"]
        .as_str()
        .unwrap()
        .to_string();
    let timeout_path = PathBuf::from(&attempt_dir).join("timeout.json");
    wait_for(
        "the runner to commit the timeout",
        Duration::from_secs(10),
        || timeout_path.exists(),
    );
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(&attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);
    q.cli(&["set-time-limit", &job.to_string(), "--clear"]);
    q.wait_state(job, "failed", Duration::from_secs(10));
    assert!(
        q.job(job)["attempts"][0]["message"]
            .as_str()
            .is_some_and(|message| message.contains("time limit exceeded")),
        "detail: {}",
        q.job(job)
    );
}

#[test]
fn late_observation_of_a_drained_orphan_is_lost_not_timed_out() {
    let mut q = TestQueue::new();
    let marker = q.dir.path().join("orphan-drained-marker");
    let marker_arg = format!("echo run >> {}; sleep 0.1", marker.display());
    let job = q.submit(
        &[
            "--name",
            "drained-orphan",
            "--time-limit",
            "1s",
            "--max-attempts",
            "2",
        ],
        &["sh", "-c", &marker_arg],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    let detail = q.job(job);
    let attempt_dir = detail["attempts"][0]["logDir"].as_str().unwrap();
    let identity: serde_json::Value = serde_json::from_slice(
        &std::fs::read(PathBuf::from(attempt_dir).join("identity.json")).unwrap(),
    )
    .unwrap();
    let runner_pid = identity["runner_pid"].as_i64().unwrap() as i32;
    q.kill_daemon();
    assert_eq!(unsafe { libc::kill(runner_pid, libc::SIGKILL) }, 0);

    // The command drains before its limit, but recovery does not observe the
    // empty group until after the nominal deadline.
    std::thread::sleep(Duration::from_millis(1_200));
    q.start_daemon();
    q.wait_state(job, "lost", Duration::from_secs(10));
    assert_eq!(
        q.job(job)["attemptCount"],
        1,
        "lost work must not auto-retry"
    );
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap().lines().count(),
        1,
        "the unknown command must not be duplicated"
    );
}

#[test]
fn submit_resolves_relative_executables_against_the_job_cwd() {
    let q = TestQueue::new();
    // Only the job's --cwd contains the script; the daemon's own working
    // directory (the crate root under `cargo test`) does not. A relative
    // command launches from the job cwd, so validation must look there.
    let bin = q.dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let script = bin.join("hello.sh");
    std::fs::write(&script, "#!/bin/sh\necho cwd-relative-marker\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let job = q.submit(&["--name", "relative-exe"], &["bin/hello.sh"]);
    q.wait_state(job, "succeeded", Duration::from_secs(10));
    let stdout = q.cli(&["logs", &job.to_string()]);
    assert!(
        stdout.contains("cwd-relative-marker"),
        "stdout log: {stdout:?}"
    );

    // A genuinely missing relative executable is still rejected, and the
    // message names the resolved path so the mistake is visible.
    let cwd = q.dir.path().to_str().unwrap();
    let rejected = q.try_cli(&[
        "submit",
        "--name",
        "missing-exe",
        "--cwd",
        cwd,
        "--",
        "bin/nope.sh",
    ]);
    assert!(
        !rejected.status.success(),
        "missing executable must be rejected"
    );
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("bin/nope.sh") && stderr.contains(cwd),
        "rejection should name the resolved path: {stderr}"
    );
}

#[test]
fn status_displays_long_job_names() {
    let q = TestQueue::new();
    let name = "long-job-name-now-fits-in-status";
    q.submit(&["--name", name], &["sleep", "1"]);

    let status = q.cli(&["status"]);
    assert!(
        status.contains(name),
        "long job name was truncated: {status}"
    );
    let header = status
        .lines()
        .find(|line| line.starts_with("JOB "))
        .unwrap();
    assert!(
        header.chars().count() <= 80,
        "status header is wider than 80 columns: {header}"
    );
}

#[test]
fn follow_tts_announces_completion_and_the_next_running_job() {
    let mut q = TestQueue::new();
    let fake_bin = q.dir.path().join("fake-bin");
    std::fs::create_dir(&fake_bin).unwrap();
    let fake_tts = fake_bin.join("tts");
    std::fs::write(
        &fake_tts,
        "#!/bin/sh\nprintf 'ARGS: %s\\n' \"$*\" >> \"$MLQ_TTS_LOG\"\ncat >> \"$MLQ_TTS_LOG\"\nprintf '\\n' >> \"$MLQ_TTS_LOG\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_tts, std::fs::Permissions::from_mode(0o755)).unwrap();
    let tts_log = q.dir.path().join("tts.log");

    let mut command = Command::new(env!("CARGO_BIN_EXE_mlq"));
    q.apply_env(&mut command);
    command
        .arg("follow-tts")
        .env("MLQ_TTS_LOG", &tts_log)
        .env(
            "PATH",
            format!(
                "{}:{}",
                fake_bin.display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut follower = KillOnDrop(Some(command.spawn().unwrap()));
    wait_for("initial TTS announcement", Duration::from_secs(5), || {
        std::fs::read_to_string(&tts_log).is_ok_and(|text| text.contains("Queue monitor started"))
    });
    let duplicate = q.try_cli(&["follow-tts"]);
    assert!(!duplicate.status.success());
    assert!(
        String::from_utf8_lossy(&duplicate.stderr).contains("already running"),
        "{}",
        String::from_utf8_lossy(&duplicate.stderr)
    );

    let first = q.submit(&["--name", "tts-first"], &["sleep", "0.4"]);
    let next = q.submit(&["--name", "tts-next"], &["sleep", "2"]);
    q.wait_state(next, "running", Duration::from_secs(10));
    let expected_finished = format!("job {first}, tts first finished successfully");
    let expected_running = format!("Now running: job {next}, tts next");
    let deadline = Instant::now() + Duration::from_secs(10);
    let spoken = loop {
        let spoken = std::fs::read_to_string(&tts_log).unwrap_or_default();
        if spoken.contains(&expected_finished) && spoken.contains(&expected_running) {
            break spoken;
        }
        if Instant::now() >= deadline {
            break spoken;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    // The durable event cursor survives a daemon restart while the follower
    // and managed command remain alive.
    q.kill_daemon();
    q.start_daemon();
    q.wait_state(next, "succeeded", Duration::from_secs(10));
    let expected_next_finished = format!("job {next}, tts next finished successfully");
    wait_for(
        "post-restart TTS announcement",
        Duration::from_secs(10),
        || {
            std::fs::read_to_string(&tts_log)
                .is_ok_and(|text| text.contains(&expected_next_finished))
        },
    );

    follower.stop();
    assert!(
        spoken.contains(&expected_finished),
        "missing finish announcement:\n{spoken}"
    );
    assert!(
        spoken.contains(&expected_running),
        "missing running announcement:\n{spoken}"
    );
    assert!(
        spoken.contains("ARGS: speak --daemon --level status --text-stdin"),
        "{spoken}"
    );
    assert!(
        !spoken.contains("--backend"),
        "default TTS backend was overridden: {spoken}"
    );
}

#[test]
fn default_limit_serializes_jobs() {
    let q = TestQueue::new();
    let slow = q.submit(&["--name", "slow"], &["sleep", "1.5"]);
    let fast = q.submit(&["--name", "fast"], &["sh", "-c", "true"]);
    q.wait_state(slow, "running", Duration::from_secs(10));

    // While the exclusive job runs, the second must wait. As the blocked
    // queue head it immediately becomes the protected job.
    let fast_view = q.job(fast);
    assert_eq!(
        fast_view["state"], "queued",
        "second maxParallelRuns=1 job must wait"
    );
    assert!(
        fast_view["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("protected_drain"),
        "unexpected eligibility: {fast_view}"
    );

    q.wait_state(slow, "succeeded", Duration::from_secs(10));
    q.wait_state(fast, "succeeded", Duration::from_secs(10));
}

#[test]
fn three_wide_jobs_share_and_a_fourth_waits() {
    let q = TestQueue::new();
    let jobs: Vec<i64> = (0..4)
        .map(|i| {
            q.submit(
                &[
                    "--max-parallel-runs",
                    "3",
                    "--name",
                    &format!("cleanrl-{i}"),
                ],
                &["sleep", "1.2"],
            )
        })
        .collect();

    // Exactly three run together; the fourth queues behind the formula.
    wait_for("three concurrent leases", Duration::from_secs(10), || {
        q.status()["activeLeases"].as_u64() == Some(3)
    });
    let running: Vec<String> = jobs.iter().map(|&j| q.job_state(j)).collect();
    assert_eq!(
        running
            .iter()
            .filter(|s| *s == "running" || *s == "starting")
            .count(),
        3
    );
    assert_eq!(running.iter().filter(|s| *s == "queued").count(), 1);

    // The invariant holds throughout: never more than three leases.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leases = q.status()["activeLeases"].as_u64().unwrap();
        assert!(leases <= 3, "over-admission: {leases} active leases");
        if jobs.iter().all(|&j| q.job_state(j) == "succeeded") {
            break;
        }
        assert!(Instant::now() < deadline, "jobs did not finish");
        std::thread::sleep(Duration::from_millis(60));
    }
}

#[test]
fn restrictive_job_is_protected_and_the_backfill_window_stays_open() {
    let q = TestQueue::new();
    // Three permissive jobs fill the 3-wide set; `gate` finishes first.
    let a = q.submit(
        &["--max-parallel-runs", "3", "--name", "a"],
        &["sleep", "4"],
    );
    let b = q.submit(
        &["--max-parallel-runs", "3", "--name", "b"],
        &["sleep", "4"],
    );
    let gate = q.submit(
        &["--max-parallel-runs", "3", "--name", "gate"],
        &["sleep", "1"],
    );
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }

    // The exclusive job is the eligible head and cannot start beside the
    // 3-wide set: it becomes the protected job.
    let exclusive = q.submit(
        &["--max-parallel-runs", "1", "--name", "exclusive"],
        &["sh", "-c", "true"],
    );
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Submitted after protection, but the original blockers are still
    // running: the open window lets these permissive jobs use slots in FIFO
    // order rather than idling them.
    let backfill = q.submit(
        &["--max-parallel-runs", "3", "--name", "backfill"],
        &["sleep", "1"],
    );
    let late = q.submit(
        &["--max-parallel-runs", "3", "--name", "late"],
        &["sleep", "1"],
    );
    let status = q.status();
    assert_eq!(
        status["reservation"]["backfillWindowOpen"], true,
        "window must be open: {status}"
    );
    assert!(
        status["reservation"]["backfillCutoff"].is_null(),
        "no cutoff while open: {status}"
    );
    let late_view = q.job(late);
    assert!(
        late_view["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("backfill_window_open"),
        "late job must be bypass-eligible in the open window: {late_view}"
    );

    // The earlier backfill job takes the first open slot; the late job follows
    // when another slot opens, all while the protected job keeps waiting.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");
    q.wait_state(late, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");
    let status = q.status();
    for consumed in [backfill, late] {
        assert!(
            status["reservation"]["consumedBypasses"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(consumed)),
            "bypass of job {consumed} must be recorded: {status}"
        );
    }

    // Full drain: the protected job starts alone and succeeds.
    q.wait_state(exclusive, "succeeded", Duration::from_secs(20));
    assert!(q.job(exclusive)["finishedAt"].as_i64() >= q.job(late)["finishedAt"].as_i64());
}

#[test]
fn frontier_freezes_once_the_original_blockers_drain() {
    let q = TestQueue::new();
    // Long enough that both blockers are still running through the submits
    // below even on a loaded machine; the freeze is then deterministic.
    let a = q.submit(
        &["--max-parallel-runs", "3", "--name", "a"],
        &["sleep", "4"],
    );
    let b = q.submit(
        &["--max-parallel-runs", "3", "--name", "b"],
        &["sleep", "4"],
    );
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive = q.submit(
        &["--max-parallel-runs", "1", "--name", "exclusive"],
        &["sh", "-c", "true"],
    );
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Open window: the post-protection job takes the third slot immediately.
    let occupant = q.submit(
        &["--max-parallel-runs", "3", "--name", "occupant"],
        &["sleep", "20"],
    );
    q.wait_state(occupant, "running", Duration::from_secs(10));

    // The original blockers drain; the occupant (a mere backfill) does not
    // hold the window open, so the frontier freezes.
    q.wait_state(a, "succeeded", Duration::from_secs(10));
    q.wait_state(b, "succeeded", Duration::from_secs(10));
    wait_for("frontier to freeze", Duration::from_secs(5), || {
        q.status()["reservation"]["backfillWindowOpen"] == serde_json::json!(false)
    });
    let cutoff = q.status()["reservation"]["backfillCutoff"]
        .as_i64()
        .unwrap();
    assert!(
        cutoff >= occupant,
        "cutoff must cover everything submitted before the freeze"
    );

    // Submitted after the freeze: may no longer pass the protected job, even
    // though two slots are open beside the occupant.
    let frozen_out = q.submit(
        &["--max-parallel-runs", "3", "--name", "frozen-out"],
        &["sh", "-c", "true"],
    );
    let frozen_view = q.job(frozen_out);
    assert!(
        frozen_view["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("behind_backfill_cutoff"),
        "post-freeze submission must wait: {frozen_view}"
    );
    assert_eq!(q.job_state(exclusive), "queued");

    // Drain the occupant: the protected job runs alone, then the frozen-out
    // job follows.
    q.cli(&["cancel", &occupant.to_string()]);
    q.wait_state(exclusive, "succeeded", Duration::from_secs(20));
    q.wait_state(frozen_out, "succeeded", Duration::from_secs(20));
    assert!(q.job(frozen_out)["finishedAt"].as_i64() >= q.job(exclusive)["finishedAt"].as_i64());
}

#[test]
fn reservation_and_consumed_bypasses_survive_daemon_restart() {
    let mut q = TestQueue::new();
    // Same shape as the open-window test, but long enough to straddle a
    // daemon crash while the reservation and one consumed bypass exist.
    let a = q.submit(
        &["--max-parallel-runs", "3", "--name", "a"],
        &["sleep", "8"],
    );
    let b = q.submit(
        &["--max-parallel-runs", "3", "--name", "b"],
        &["sleep", "8"],
    );
    let gate = q.submit(
        &["--max-parallel-runs", "3", "--name", "gate"],
        &["sleep", "1"],
    );
    for job in [a, b, gate] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive = q.submit(
        &["--max-parallel-runs", "1", "--name", "exclusive"],
        &["sh", "-c", "true"],
    );
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });
    let backfill = q.submit(
        &["--max-parallel-runs", "3", "--name", "backfill"],
        &["sleep", "2"],
    );
    let late = q.submit(
        &["--max-parallel-runs", "3", "--name", "late"],
        &["sh", "-c", "true"],
    );

    // The bypass is consumed, then the daemon crashes.
    q.wait_state(backfill, "running", Duration::from_secs(10));
    q.kill_daemon();
    q.start_daemon();

    // The restarted daemon restores the reservation and the consumed set
    // from the database, and re-derives the open window from the still-live
    // initial blockers rather than recomputing fairness from events.
    let status = q.status();
    assert_eq!(
        status["reservation"]["protectedJob"].as_i64(),
        Some(exclusive)
    );
    assert_eq!(
        status["reservation"]["backfillWindowOpen"], true,
        "blockers still run: {status}"
    );
    assert!(
        status["reservation"]["consumedBypasses"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(backfill)),
        "consumed bypass must survive the restart: {status}"
    );
    assert_eq!(q.job_state(exclusive), "queued");
    assert!(
        q.job(late)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("backfill_window_open"),
        "the window stays open across the restart"
    );

    // The late job takes a slot once the backfill finishes, while the
    // original blockers still run and the protected job keeps waiting.
    q.wait_state(late, "succeeded", Duration::from_secs(30));
    assert_eq!(q.job_state(a), "running");
    assert_eq!(q.job_state(exclusive), "queued");

    // Drain everything: the protected job still starts.
    q.cli(&["cancel", &a.to_string()]);
    q.cli(&["cancel", &b.to_string()]);
    q.wait_state(exclusive, "succeeded", Duration::from_secs(30));
}

#[test]
fn cancelling_the_protected_job_invalidates_the_reservation() {
    let q = TestQueue::new();
    let a = q.submit(
        &["--max-parallel-runs", "3", "--name", "a"],
        &["sleep", "6"],
    );
    let b = q.submit(
        &["--max-parallel-runs", "3", "--name", "b"],
        &["sleep", "6"],
    );
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive = q.submit(
        &["--max-parallel-runs", "1", "--name", "exclusive"],
        &["sleep", "5"],
    );
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // The open window admits the late job into the third slot; its bypass is
    // consumed under the standing reservation.
    let late = q.submit(
        &["--max-parallel-runs", "3", "--name", "late"],
        &["sleep", "6"],
    );
    q.wait_state(late, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(exclusive), "queued");

    // Cancelling the protected job invalidates the reservation entirely:
    // later submissions no longer answer to any frontier.
    q.cli(&["cancel", &exclusive.to_string()]);
    wait_for(
        "reservation to be invalidated",
        Duration::from_secs(5),
        || q.status()["reservation"].is_null(),
    );
    let free = q.submit(
        &["--max-parallel-runs", "4", "--name", "free"],
        &["sh", "-c", "true"],
    );
    let free_view = q.job(free);
    assert!(
        free_view["eligibility"]
            .as_str()
            .is_none_or(|reason| !reason.contains("backfill")),
        "no reservation may constrain the queue after invalidation: {free_view}"
    );
    assert_eq!(q.job_state(a), "running");
    assert_eq!(q.job_state(b), "running");
}

#[test]
fn unsupported_reservation_blocks_admission_until_resolved() {
    let mut q = TestQueue::new();
    let a = q.submit(
        &["--max-parallel-runs", "3", "--name", "a"],
        &["sleep", "10"],
    );
    let b = q.submit(
        &["--max-parallel-runs", "3", "--name", "b"],
        &["sleep", "10"],
    );
    for job in [a, b] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }
    let exclusive = q.submit(
        &["--max-parallel-runs", "1", "--name", "exclusive"],
        &["sh", "-c", "true"],
    );
    wait_for("reservation to exist", Duration::from_secs(5), || {
        q.status()["reservation"]["protectedJob"].as_i64() == Some(exclusive)
    });

    // Rewrite the active reservation to a semantics version this daemon does
    // not implement, as a bad upgrade or downgrade would leave behind.
    q.kill_daemon();
    {
        let conn = mlqueue::db::open(&q.state_dir().join("mlqueue.db")).unwrap();
        conn.execute(
            "UPDATE scheduler_reservation SET semantics_version = 99 WHERE status = 'active'",
            [],
        )
        .unwrap();
    }
    q.start_daemon();

    // The daemon must refuse ordinary admission rather than guess: a new
    // compatible job queues even though a slot is open beside a and b.
    wait_for("admission to block", Duration::from_secs(5), || {
        q.status()["admissionBlocked"] == serde_json::json!(true)
    });
    let stuck = q.submit(
        &["--max-parallel-runs", "3", "--name", "stuck"],
        &["sh", "-c", "true"],
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(stuck), "queued");

    // Cancelling the protected job resolves the uninterpretable reservation;
    // admission must heal without a daemon restart.
    q.cli(&["cancel", &exclusive.to_string()]);
    q.wait_state(stuck, "succeeded", Duration::from_secs(10));
    assert_eq!(q.status()["admissionBlocked"], serde_json::json!(false));
}

#[test]
fn cancel_terminates_and_frees_the_lease() {
    let q = TestQueue::new();
    let job = q.submit(&["--name", "long"], &["sleep", "60"]);
    q.wait_state(job, "running", Duration::from_secs(10));

    // Cancel of live work is async: report intent, not a false "is now running".
    let out = q.cli(&["cancel", &job.to_string()]);
    assert!(
        out.contains("cancel requested") && !out.contains("is now running"),
        "cancel of a live job should report async cancel intent, got: {out:?}"
    );
    q.wait_state(job, "cancelled", Duration::from_secs(10));

    // The lease is gone: an exclusive job starts immediately.
    let next = q.submit(&["--name", "next"], &["sh", "-c", "true"]);
    q.wait_state(next, "succeeded", Duration::from_secs(10));
}

#[test]
fn force_cancel_escalates_to_sigkill_after_grace() {
    let q = TestQueue::new();
    // Ignores SIGTERM; only SIGKILL can end the shell loop.
    let job = q.submit(
        &["--name", "stubborn"],
        &["sh", "-c", "trap '' TERM; while true; do sleep 0.2; done"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    q.cli(&["cancel", &job.to_string(), "--force"]);
    q.wait_state(job, "cancelled", Duration::from_secs(15));
}

#[test]
fn idempotency_key_replays_and_conflicts() {
    let q = TestQueue::new();
    let args = |name: &str| {
        vec![
            "--idempotency-key".to_string(),
            "test-key-1".to_string(),
            "submit".to_string(),
            "--json".to_string(),
            "--name".to_string(),
            name.to_string(),
            "--cwd".to_string(),
            q.dir.path().to_str().unwrap().to_string(),
            "--".to_string(),
            "sh".to_string(),
            "-c".to_string(),
            "true".to_string(),
        ]
    };
    let run = |name: &str| -> Output {
        let owned = args(name);
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        q.try_cli(&refs)
    };
    let parse = |output: &Output| -> serde_json::Value {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    };
    let first = parse(&run("same"));
    let second = parse(&run("same"));
    assert_eq!(
        first["id"], second["id"],
        "identical retry must not duplicate the job"
    );

    // Same key, different payload: stable conflict, no third job.
    let output = run("different");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("idempotency_conflict"), "stderr: {stderr}");
}

#[test]
fn retry_policy_reattempts_then_fails() {
    let q = TestQueue::new();
    let job = q.submit(
        &[
            "--name",
            "flaky",
            "--max-attempts",
            "2",
            "--retry-delay",
            "100ms",
        ],
        &["sh", "-c", "exit 1"],
    );
    q.wait_state(job, "failed", Duration::from_secs(15));
    let detail = q.job(job);
    assert_eq!(
        detail["attemptCount"], 2,
        "retry policy must run a second attempt"
    );
    assert_eq!(detail["attempts"][1]["state"], "failed");

    // Manual retry grants one more attempt.
    q.cli(&["retry", &job.to_string()]);
    q.wait_state(job, "failed", Duration::from_secs(15));
    assert_eq!(q.job(job)["attemptCount"], 3);
}

#[test]
fn failed_dependencies_cascade_skips_and_run_terminal_cleanup() {
    let q = TestQueue::new();
    let parent = q.submit(
        &[
            "--name",
            "parent",
            "--max-attempts",
            "2",
            "--retry-delay",
            "800ms",
        ],
        &["sh", "-c", "exit 3"],
    );
    let child = q.submit(
        &["--name", "child", "--after-success", &parent.to_string()],
        &["sh", "-c", "true"],
    );
    let grandchild = q.submit(
        &[
            "--name",
            "grandchild",
            "--after-success",
            &child.to_string(),
        ],
        &["sh", "-c", "true"],
    );
    let cleanup = q.submit(
        &["--name", "cleanup", "--after-terminal", &parent.to_string()],
        &["sh", "-c", "true"],
    );

    // The parent is not terminal between automatic attempts, so neither
    // dependency condition resolves during the retry window.
    wait_for(
        "parent retry to be scheduled",
        Duration::from_secs(10),
        || {
            let view = q.job(parent);
            view["state"] == "queued" && view["attemptCount"] == 1
        },
    );
    assert_eq!(q.job_state(child), "queued");
    assert!(
        q.job(child)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_dependencies")
    );
    assert_eq!(q.job_state(cleanup), "queued");

    q.wait_state(parent, "failed", Duration::from_secs(15));
    q.wait_state(child, "skipped", Duration::from_secs(10));
    q.wait_state(grandchild, "skipped", Duration::from_secs(10));
    q.wait_state(cleanup, "succeeded", Duration::from_secs(10));
    assert_eq!(
        q.try_cli(&["follow", &child.to_string()]).status.code(),
        Some(1),
        "waiting on a skipped job returns a stable generic failure"
    );

    let child_view = q.job(child);
    assert_eq!(child_view["dependencies"][0]["condition"], "succeeded");
    assert_eq!(child_view["dependencies"][0]["resolution"], "violated");
    assert_eq!(child_view["dependencies"][0]["parentOutcome"], "failed");
    let cleanup_view = q.job(cleanup);
    assert_eq!(cleanup_view["dependencies"][0]["condition"], "terminal");
    assert_eq!(cleanup_view["dependencies"][0]["resolution"], "satisfied");

    // Retrying the parent cannot change latched descendant outcomes.
    q.cli(&["retry", &parent.to_string()]);
    q.wait_state(parent, "failed", Duration::from_secs(15));
    assert_eq!(q.job_state(child), "skipped");
    assert_eq!(q.job_state(cleanup), "succeeded");
}

#[test]
fn successful_dependencies_persist_across_daemon_restart() {
    let mut q = TestQueue::new();
    let parent = q.submit(&["--name", "parent"], &["sh", "-c", "sleep 1; true"]);
    let child = q.submit(
        &["--name", "child", "--after-success", &parent.to_string()],
        &["sh", "-c", "true"],
    );
    assert!(
        q.job(child)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_dependencies")
    );

    q.kill_daemon();
    q.start_daemon();
    q.wait_state(parent, "succeeded", Duration::from_secs(15));
    q.wait_state(child, "succeeded", Duration::from_secs(10));
}

#[test]
fn dependency_submission_rejects_a_missing_parent_atomically() {
    let q = TestQueue::new();
    let before = q.status()["jobs"].as_array().unwrap().len();
    let output = q.try_cli(&[
        "submit",
        "--after-success",
        "999999",
        "--cwd",
        q.dir.path().to_str().unwrap(),
        "--",
        "sh",
        "-c",
        "true",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not_found"));
    assert_eq!(q.status()["jobs"].as_array().unwrap().len(), before);
}

#[test]
fn daemon_crash_neither_kills_nor_duplicates_running_work() {
    let q = TestQueue::new();
    let marker = q.dir.path().join("ran-count");
    let script = format!("sleep 1.5; echo done >> {}", marker.display());
    let job = q.submit(&["--name", "survivor"], &["sh", "-c", &script]);
    q.wait_state(job, "running", Duration::from_secs(10));

    let mut q = q;
    q.kill_daemon();
    std::thread::sleep(Duration::from_millis(300));
    q.start_daemon();

    // The worker kept running through the crash; the restarted daemon adopts
    // it and finalizes the durable result exactly once.
    q.wait_state(job, "succeeded", Duration::from_secs(15));
    wait_for("marker file", Duration::from_secs(5), || marker.exists());
    let content = std::fs::read_to_string(&marker).unwrap();
    assert_eq!(content, "done\n", "command must have run exactly once");

    // Queue still fully operational after recovery.
    let next = q.submit(&["--name", "post-restart"], &["sh", "-c", "true"]);
    q.wait_state(next, "succeeded", Duration::from_secs(10));
}

#[test]
fn hold_excludes_from_scheduling_until_release() {
    let q = TestQueue::new();
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "1"]);
    let held = q.submit(&["--name", "held"], &["sh", "-c", "true"]);
    q.cli(&["hold", &held.to_string()]);
    q.wait_state(blocker, "succeeded", Duration::from_secs(10));

    // Slot is free but the held job must not start.
    std::thread::sleep(Duration::from_millis(400));
    let view = q.job(held);
    assert_eq!(view["state"], "held");
    assert_eq!(view["eligibility"], "held");

    q.cli(&["release", &held.to_string()]);
    q.wait_state(held, "succeeded", Duration::from_secs(10));
}

#[test]
fn pause_blocks_new_starts_until_unpause() {
    let mut q = TestQueue::new();

    let running = q.submit(&["--name", "running"], &["sleep", "2"]);
    q.wait_state(running, "running", Duration::from_secs(5));

    assert_eq!(q.cli(&["pause"]).trim(), "queue paused");
    assert_eq!(q.status()["paused"], serde_json::json!(true));
    assert_eq!(q.cli_json(&["daemon", "status", "--json"])["paused"], true);
    assert_eq!(q.cli_json(&["pause", "--json"])["paused"], true);

    let queued = q.submit(&["--name", "queued"], &["sh", "-c", "true"]);
    q.wait_state(running, "succeeded", Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(400));
    let view = q.job(queued);
    assert_eq!(view["state"], "queued");
    assert_eq!(view["eligibility"], "paused");

    q.kill_daemon();
    q.start_daemon();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(queued), "queued");
    assert_eq!(q.status()["paused"], serde_json::json!(true));

    assert_eq!(q.cli(&["unpause"]).trim(), "queue unpaused");
    q.wait_state(queued, "succeeded", Duration::from_secs(10));
    assert_eq!(q.status()["paused"], serde_json::json!(false));
}

#[test]
fn second_daemon_instance_is_refused() {
    let q = TestQueue::new();
    let mut second = q.spawn_daemon();
    let status = second.wait().unwrap();
    assert!(
        !status.success(),
        "second daemon must refuse to start while the lock is held"
    );
}

#[test]
fn set_max_parallel_runs_updates_queued_and_live_jobs_safely() {
    let q = TestQueue::new();
    let first = q.submit(
        &["--max-parallel-runs", "2", "--name", "first"],
        &["sleep", "30"],
    );
    let second = q.submit(
        &["--max-parallel-runs", "2", "--name", "second"],
        &["sleep", "30"],
    );
    for job in [first, second] {
        q.wait_state(job, "running", Duration::from_secs(10));
    }

    let queued = q.submit(
        &["--max-parallel-runs", "2", "--name", "tune-me"],
        &["sleep", "30"],
    );

    // Reapplying the same value is a true no-op, including for this protected
    // queue head; it must not perturb the active reservation.
    q.cli(&["set-max-parallel-runs", &queued.to_string(), "2"]);
    assert_eq!(q.status()["reservation"]["protectedJob"], queued);

    q.cli(&["set-max-parallel-runs", &queued.to_string(), "3"]);
    assert_eq!(q.job(queued)["maxParallelRuns"], 3);
    // Both active declarations still cap the machine at two, so changing only
    // the queued candidate cannot admit it.
    assert_eq!(q.job_state(queued), "queued");

    // Raising one live declaration is not enough; raising both opens the third
    // slot and the queued job starts on the mutation-triggered scheduler pass.
    let message = q.cli(&["set-max-parallel-runs", &first.to_string(), "3"]);
    assert!(
        message.contains("maxParallelRuns is now 3"),
        "message: {message}"
    );
    assert_eq!(q.job_state(queued), "queued");
    q.cli(&["set-max-parallel-runs", &second.to_string(), "3"]);
    q.wait_state(queued, "running", Duration::from_secs(10));

    // Job, attempt, and lease views all reflect the live change.
    for job in [first, second] {
        let detail = q.job(job);
        assert_eq!(detail["maxParallelRuns"], 3);
        assert_eq!(detail["attempts"][0]["maxParallelRuns"], 3);
    }
    assert_eq!(q.status()["effectiveLimit"], 3);

    // Three leases are active. Lowering one declaration to 2 makes the set
    // temporarily oversubscribed (no preemption); the limit still applies to
    // future admissions, and the job/attempt/lease copies all update now.
    q.cli(&["set-max-parallel-runs", &first.to_string(), "2"]);
    let detail = q.job(first);
    assert_eq!(detail["maxParallelRuns"], 2);
    assert_eq!(detail["attempts"][0]["maxParallelRuns"], 2);
    assert_eq!(q.status()["effectiveLimit"], 2);
    assert_eq!(q.status()["activeLeases"], 3);

    // A fourth job must stay queued while three leases remain and the
    // effective declaration is only 2.
    let fourth = q.submit(
        &["--max-parallel-runs", "3", "--name", "must-wait"],
        &["sleep", "30"],
    );
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(fourth), "queued");

    // After the third lease drains, two remain at effective limit 2; still no
    // room for a third start until someone else finishes or limits rise.
    q.cli(&["cancel", &queued.to_string()]);
    q.wait_state(queued, "cancelled", Duration::from_secs(10));
    assert_eq!(q.status()["activeLeases"], 2);
    assert_eq!(q.status()["effectiveLimit"], 2);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(q.job_state(fourth), "queued");

    // Raise the tight declaration so three can coexist again; the waiter
    // admits on the mutation-triggered scheduler pass.
    q.cli(&["set-max-parallel-runs", &first.to_string(), "3"]);
    q.wait_state(fourth, "running", Duration::from_secs(10));
    assert_eq!(q.status()["activeLeases"], 3);
    assert_eq!(q.status()["effectiveLimit"], 3);

    for job in [first, second, fourth] {
        q.cli(&["cancel", &job.to_string()]);
        q.wait_state(job, "cancelled", Duration::from_secs(10));
    }
}

#[test]
fn set_priority_updates_queued_jobs_and_rejects_running() {
    let q = TestQueue::new();
    let blocker = q.submit(&["--name", "blocker"], &["sleep", "30"]);
    q.wait_state(blocker, "running", Duration::from_secs(10));

    let first = q.submit(&["--name", "first", "--priority", "0"], &["sleep", "30"]);
    let second = q.submit(&["--name", "second", "--priority", "0"], &["sleep", "30"]);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(first),
        "FIFO protection while priorities are equal"
    );

    // Same value is a no-op that leaves the protected reservation alone.
    let message = q.cli(&["set-priority", &first.to_string(), "0"]);
    assert!(message.contains("priority is now 0"), "message: {message}");
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(first)
    );

    // Raising a later job above the protected head supersedes the reservation.
    let message = q.cli(&["set-priority", &second.to_string(), "2"]);
    assert!(message.contains("priority is now 2"), "message: {message}");
    assert_eq!(q.job(second)["priority"], 2);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(second)
    );

    // Held jobs may also be retuned before release.
    q.cli(&["hold", &first.to_string()]);
    q.cli(&["set-priority", &first.to_string(), "-1"]);
    assert_eq!(q.job(first)["priority"], -1);
    assert_eq!(q.job_state(first), "held");

    // Running work cannot change priority (no preemption).
    let output = q.try_cli(&["set-priority", &blocker.to_string(), "9"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("queued or held"), "stderr: {stderr}");
    assert_eq!(q.job(blocker)["priority"], 0);

    // Negative priorities parse as a positional value.
    q.cli(&["release", &first.to_string()]);
    q.cli(&["set-priority", &first.to_string(), "-4"]);
    assert_eq!(q.job(first)["priority"], -4);

    q.cli(&["cancel", &blocker.to_string()]);
    q.wait_state(blocker, "cancelled", Duration::from_secs(10));
    q.wait_state(second, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(first), "queued");

    q.cli(&["cancel", &second.to_string()]);
    q.wait_state(second, "cancelled", Duration::from_secs(10));
    q.wait_state(first, "running", Duration::from_secs(10));
    q.cli(&["cancel", &first.to_string()]);
    q.wait_state(first, "cancelled", Duration::from_secs(10));
}

#[test]
fn default_end_and_start_reuse_the_command_and_preserve_attempt_history() {
    let q = TestQueue::new();
    let idle = q.submit(
        &["--default", "--name", "idle-work"],
        &["sh", "-c", "exec sleep 30"],
    );
    q.wait_state(idle, "running", Duration::from_secs(10));
    let first = q.job(idle);
    assert_eq!(first["attemptCount"], 1);

    q.cli(&["default", "end"]);
    q.wait_state(idle, "cancelled", Duration::from_secs(10));
    assert_eq!(q.status()["activeLeases"], 0);

    // Ordinary work still takes precedence when the saved default is started.
    let ordinary = q.submit(&[], &["sleep", "30"]);
    q.wait_state(ordinary, "running", Duration::from_secs(10));
    q.cli(&["default", "start"]);
    assert_eq!(q.job_state(idle), "queued");
    assert_eq!(q.job(idle)["attemptCount"], 1);
    q.cli(&["cancel", &ordinary.to_string()]);
    q.wait_state(ordinary, "cancelled", Duration::from_secs(10));
    q.wait_state(idle, "running", Duration::from_secs(10));

    let restarted = q.job(idle);
    assert_eq!(restarted["args"], first["args"]);
    assert_eq!(restarted["cwd"], first["cwd"]);
    assert_eq!(restarted["attemptCount"], 2);
    assert_eq!(restarted["attempts"][0]["state"], "cancelled");

    // Starting an already active default must not launch another copy.
    q.cli(&["default", "start"]);
    assert_eq!(q.job(idle)["attemptCount"], 2);
    assert_eq!(q.status()["activeLeases"], 1);
    q.cli(&["default", "end"]);
    q.wait_state(idle, "cancelled", Duration::from_secs(10));
}

#[test]
fn default_start_finds_an_ended_command_after_daemon_restart_and_newer_jobs() {
    let mut q = TestQueue::new();
    q.cli(&["pause"]);
    let idle = q.submit(&["--default"], &["sleep", "30"]);
    q.cli(&["default", "end"]);
    assert_eq!(q.job_state(idle), "cancelled");
    assert_eq!(q.job(idle)["attemptCount"], 0);

    // The saved default is outside the status page and has multiple argv entries.
    for _ in 0..30 {
        let ordinary = q.submit(&[], &["true"]);
        q.cli(&["cancel", &ordinary.to_string()]);
    }
    q.stop_daemon();
    q.start_daemon();
    assert_eq!(q.job_state(idle), "cancelled");
    q.cli(&["default", "start"]);
    assert_eq!(q.job_state(idle), "queued");
    q.cli(&["unpause"]);
    q.wait_state(idle, "running", Duration::from_secs(10));
    assert_eq!(q.job(idle)["attemptCount"], 1);
    q.cli(&["default", "end"]);
    q.wait_state(idle, "cancelled", Duration::from_secs(10));
}

#[test]
fn signed_priorities_strictly_order_queued_jobs_without_preempting_running_work() {
    let q = TestQueue::new();
    let blocker = q.submit(
        &["--priority", "-5", "--name", "running-low-priority"],
        &["sleep", "30"],
    );
    q.wait_state(blocker, "running", Duration::from_secs(10));

    let low = q.submit(&["--priority", "-1", "--name", "low"], &["sleep", "30"]);
    let default = q.submit(&["--name", "default"], &["sleep", "30"]);
    let high = q.submit(&["--priority", "1", "--name", "high"], &["sleep", "30"]);

    // Higher-priority arrivals supersede queued reservations, not active
    // leases. The already-running negative-priority job remains untouched.
    assert_eq!(q.job_state(blocker), "running");
    assert_eq!(q.job(high)["priority"], 1);
    assert_eq!(q.job(default)["priority"], 0);
    assert_eq!(q.job(low)["priority"], -1);
    assert_eq!(
        q.status()["reservation"]["protectedJob"].as_i64(),
        Some(high)
    );
    assert!(
        q.job(default)["eligibility"]
            .as_str()
            .unwrap()
            .starts_with("waiting_for_higher_priority"),
        "default-priority work should wait behind priority 1"
    );
    let status = q.cli(&["status"]);
    assert!(status.contains("PRI"), "status header: {status}");

    q.cli(&["cancel", &blocker.to_string()]);
    q.wait_state(blocker, "cancelled", Duration::from_secs(10));
    q.wait_state(high, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(default), "queued");
    assert_eq!(q.job_state(low), "queued");

    q.cli(&["cancel", &high.to_string()]);
    q.wait_state(high, "cancelled", Duration::from_secs(10));
    q.wait_state(default, "running", Duration::from_secs(10));
    assert_eq!(q.job_state(low), "queued");

    q.cli(&["cancel", &default.to_string()]);
    q.wait_state(default, "cancelled", Duration::from_secs(10));
    q.wait_state(low, "running", Duration::from_secs(10));
    q.cli(&["cancel", &low.to_string()]);
    q.wait_state(low, "cancelled", Duration::from_secs(10));
}

#[test]
fn follow_blocks_until_terminal_and_exits_with_the_outcome() {
    let q = TestQueue::new();

    let ok = q.submit(&["--name", "ok"], &["sh", "-c", "sleep 0.3; true"]);
    let output = q.try_cli(&["follow", &ok.to_string()]);
    assert!(
        output.status.success(),
        "follow on a succeeding job must exit 0"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("succeeded"));

    let bad = q.submit(&["--name", "bad"], &["sh", "-c", "exit 3"]);
    let output = q.try_cli(&["follow", &bad.to_string()]);
    assert_eq!(
        output.status.code(),
        Some(3),
        "follow must propagate the command's exit code"
    );

    let hurt = q.submit(&["--name", "hurt"], &["sh", "-c", "kill -SEGV $$"]);
    let output = q.try_cli(&["follow", &hurt.to_string()]);
    assert_eq!(
        output.status.code(),
        Some(128 + 11),
        "signal deaths map to 128+N"
    );

    let slow = q.submit(&["--name", "slow"], &["sleep", "5"]);
    let output = q.try_cli(&["follow", &slow.to_string(), "--timeout", "300ms"]);
    assert_eq!(
        output.status.code(),
        Some(124),
        "follow --timeout expiry must exit 124"
    );
    // Cancelling before launch would finalize without a signal; the 128+15
    // mapping is only defined for a delivered SIGTERM.
    q.wait_state(slow, "running", Duration::from_secs(10));
    q.cli(&["cancel", &slow.to_string()]);
    let output = q.try_cli(&["follow", &slow.to_string()]);
    assert_eq!(
        output.status.code(),
        Some(128 + 15),
        "cancellation by SIGTERM maps to 128+15"
    );
}

#[test]
fn logs_follow_reports_the_terminal_outcome() {
    let q = TestQueue::new();
    // Tail a live job so the follow loop crosses the running→terminal edge.
    let job = q.submit(
        &["--name", "tail-me"],
        &["sh", "-c", "echo body-marker; sleep 0.8; exit 7"],
    );
    q.wait_state(job, "running", Duration::from_secs(10));

    let output = q.try_cli(&["logs", &job.to_string(), "--follow"]);
    assert_eq!(
        output.status.code(),
        Some(7),
        "follow must reflect the attempt outcome"
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("body-marker"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("attempt 1 failed"),
        "missing terminal report: {stderr}"
    );

    // Without --follow the command only prints current logs; a failed job
    // must not turn that read into a failure.
    let output = q.try_cli(&["logs", &job.to_string()]);
    assert!(output.status.success());

    // Following a succeeding job drains, reports, and exits 0.
    let fine = q.submit(
        &["--name", "tail-fine"],
        &["sh", "-c", "echo fine-marker; sleep 0.5"],
    );
    q.wait_state(fine, "running", Duration::from_secs(10));
    let output = q.try_cli(&["logs", &fine.to_string(), "--follow"]);
    assert!(output.status.success(), "follow on success must exit 0");
    assert!(String::from_utf8_lossy(&output.stdout).contains("fine-marker"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("attempt 1 succeeded"));
}

#[test]
fn see_prints_detail_and_latest_stdout() {
    let q = TestQueue::new();
    let empty = q.try_cli(&["see"]);
    assert!(!empty.status.success());
    assert!(
        String::from_utf8_lossy(&empty.stderr).contains("no jobs"),
        "empty queue: {}",
        String::from_utf8_lossy(&empty.stderr)
    );

    let older = q.submit(
        &["--name", "older", "--env", "MLQ_SEE_MARK=older-stdout-only"],
        &["sh", "-c", "echo \"$MLQ_SEE_MARK\""],
    );
    q.wait_state(older, "succeeded", Duration::from_secs(10));
    let job = q.submit(
        &["--name", "see-me", "--env", "MLQ_SEE_MARK=see-stdout-only"],
        &["sh", "-c", "echo \"$MLQ_SEE_MARK\""],
    );
    q.wait_state(job, "succeeded", Duration::from_secs(10));

    let shown = q.cli(&["see", &job.to_string()]);
    assert!(shown.contains(&format!("job {job} [see-me]")), "{shown}");
    assert!(shown.contains("see-stdout-only"), "{shown}");
    assert!(!shown.contains("older-stdout-only"), "{shown}");

    let latest = q.cli(&["see"]);
    assert!(latest.contains(&format!("job {job} [see-me]")), "{latest}");
    assert!(latest.contains("see-stdout-only"), "{latest}");
    assert!(!latest.contains("older-stdout-only"), "{latest}");

    let json = q.cli_json(&["see", "--json"]);
    assert_eq!(json["id"], job);
    assert_eq!(json["name"], "see-me");

    let missing = q.try_cli(&["see", "999"]);
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("not found"),
        "missing job: {}",
        String::from_utf8_lossy(&missing.stderr)
    );
}

fn spawn_follower(q: &TestQueue, args: &[&str]) -> KillOnDrop {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mlq"));
    q.apply_env(&mut command);
    KillOnDrop(Some(
        command
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ))
}

fn follower_output(mut follower: KillOnDrop) -> Output {
    follower.0.take().unwrap().wait_with_output().unwrap()
}

fn follow_events(output: &Output) -> Vec<serde_json::Value> {
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("compact NDJSON event"))
        .collect()
}

// An explicitly enabled healthy check proves the CLI consumed a running
// snapshot. Probing subscription quotas races the follower for its own slot.
fn follow_barrier(q: &TestQueue, name: &str) -> (PathBuf, PathBuf) {
    let marker = q.dir.path().join(format!("{name}-ready"));
    let config = q.dir.path().join(format!("{name}.toml"));
    std::fs::write(
        &config,
        format!(
            r#"
events = ["warning"]
[[checks]]
id = "ready"
command = ["sh", "-c", "touch '{}'; echo '{{\"findings\":[]}}'"]
"#,
            marker.display()
        ),
    )
    .unwrap();
    (config, marker)
}

#[test]
fn follow_is_final_only_unless_selected_and_subscribes_through_retries() {
    let q = TestQueue::new();
    let job = q.submit(&["--max-attempts", "2", "--retry-delay", "100ms"],
        &["sh", "-c", "while ! test -e proceed; do sleep 0.05; done; if test -e retried; then exit 7; else touch retried; exit 8; fi"]);
    q.wait_state(job, "running", Duration::from_secs(5));
    let marker = q.dir.path().join("must-not-execute");
    let config = q.dir.path().join("follow.toml");
    std::fs::write(
        &config,
        format!(
            r#"
events = ["started", "warning", "retry", "attention"]
[[checks]]
id = "forbidden"
command = ["touch", "{}"]
every = "1s"
[[hooks]]
events = ["started", "warning", "retry", "attention"]
command = ["touch", "{}"]
"#,
            marker.display(),
            marker.display()
        ),
    )
    .unwrap();
    let id = job.to_string();
    let (selected_config, selected_ready) = follow_barrier(&q, "selected");
    let quiet = spawn_follower(&q, &["follow", &id, "--json", "--timeout", "10s"]);
    let selected = spawn_follower(
        &q,
        &[
            "follow",
            &id,
            "--config",
            selected_config.to_str().unwrap(),
            "--events",
            "started,retry,warning",
            "--json",
            "--timeout",
            "10s",
        ],
    );
    let overridden = spawn_follower(
        &q,
        &[
            "follow",
            &id,
            "--config",
            config.to_str().unwrap(),
            "--only-finish",
            "--json",
            "--timeout",
            "10s",
        ],
    );
    wait_for("selected follower attached", Duration::from_secs(5), || {
        selected_ready.exists()
    });
    std::fs::write(q.dir.path().join("proceed"), "").unwrap();
    let quiet = follower_output(quiet);
    let selected = follower_output(selected);
    let overridden = follower_output(overridden);
    for result in [&quiet, &selected, &overridden] {
        assert_eq!(result.status.code(), Some(7), "{result:?}");
    }
    for result in [&quiet, &overridden] {
        let events = follow_events(result);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["event"], "finished");
        assert_eq!(events[0]["state"], "failed");
        assert_eq!(events[0]["exitCode"], 7);
    }
    let events = follow_events(&selected);
    let kinds: Vec<_> = events
        .iter()
        .map(|event| event["event"].as_str().unwrap())
        .collect();
    // Attachment does not invent/replay attempt 1's start; attempt 2 is live.
    assert_eq!(kinds, ["retry", "started", "finished"]);
    assert_eq!(events[0]["attempt"], 1);
    assert_eq!(events[1]["attempt"], 2);
    assert!(
        !marker.exists(),
        "only-finish must prevent intermediate script execution"
    );
}

#[test]
fn follow_checks_deduplicate_before_invoking_notification_hooks() {
    let q = TestQueue::new();
    let job = q.submit(
        &[],
        &["sh", "-c", "while ! test -e finished; do sleep 0.1; done"],
    );
    let marker = q.dir.path().join("checked");
    let notified = q.dir.path().join("notified");
    let checker = q.dir.path().join("check");
    std::fs::write(
        &checker,
        format!(
            r#"#!/bin/sh
cat >/dev/null
echo checked >> '{}'
echo '{{"findings":[{{"key":"ema-declining","message":"EMA declined"}}]}}'
"#,
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&checker, std::fs::Permissions::from_mode(0o700)).unwrap();
    let hook = q.dir.path().join("notify");
    std::fs::write(
        &hook,
        format!(
            r#"#!/bin/sh
cat >> '{}'
echo >> '{}'
echo never-forward-hook-output
"#,
            notified.display(),
            notified.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = q.dir.path().join("follow.toml");
    std::fs::write(
        &config,
        r#"
events = ["warning"]
[[checks]]
id = "training"
command = ["./check"]
every = "1s"
[[hooks]]
events = ["warning"]
command = ["./notify"]
"#,
    )
    .unwrap();
    let follower = spawn_follower(
        &q,
        &[
            "follow",
            &job.to_string(),
            "--config",
            config.to_str().unwrap(),
            "--json",
            "--timeout",
            "15s",
        ],
    );
    wait_for("three local checks", Duration::from_secs(10), || {
        std::fs::read_to_string(&marker).is_ok_and(|value| value.lines().count() >= 3)
    });
    std::fs::write(q.dir.path().join("finished"), "").unwrap();
    let result = follower_output(follower);
    assert!(result.status.success(), "{result:?}");
    let events = follow_events(&result);
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0]["event"], "warning");
    assert_eq!(events[0]["findings"][0]["key"], "ema-declining");
    assert_eq!(events[1]["event"], "finished");
    let notifications: Vec<serde_json::Value> = std::fs::read_to_string(notified)
        .unwrap()
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(notifications.len(), 1, "{notifications:?}");
    assert_eq!(notifications[0]["event"], "warning");
}

#[test]
fn quiet_follow_timeout_and_disconnect_never_claim_completion_or_cancel_job() {
    let mut q = TestQueue::new();
    q.cli(&["pause"]);
    let job = q.submit(&[], &["sleep", "30"]);
    let result = q.try_cli(&["follow", &job.to_string(), "--timeout", "50ms", "--json"]);
    assert_eq!(result.status.code(), Some(124), "{result:?}");
    assert!(result.stdout.is_empty());
    assert_eq!(q.job_state(job), "queued");
    q.cli(&["unpause"]);
    let (config, ready) = follow_barrier(&q, "disconnect");
    let follower = spawn_follower(
        &q,
        &[
            "follow",
            &job.to_string(),
            "--config",
            config.to_str().unwrap(),
            "--timeout",
            "10s",
            "--json",
        ],
    );
    wait_for("attached quiet follower", Duration::from_secs(5), || {
        ready.exists()
    });
    q.stop_daemon();
    let result = follower_output(follower);
    q.start_daemon();
    q.cli(&["cancel", &job.to_string()]);
    q.wait_state(job, "cancelled", Duration::from_secs(5));
    assert_eq!(result.status.code(), Some(1));
    assert!(result.stdout.is_empty(), "{result:?}");
}

#[test]
fn follower_hook_cannot_replace_an_observed_terminal_outcome_with_timeout() {
    let q = TestQueue::new();
    let job = q.submit(&[], &["true"]);
    q.wait_state(job, "succeeded", Duration::from_secs(5));
    let config = q.dir.path().join("follow.toml");
    std::fs::write(
        &config,
        r#"
[[hooks]]
events = ["finished"]
command = ["sleep", "0.3"]
timeout = "1s"
"#,
    )
    .unwrap();
    let result = q.try_cli(&[
        "follow",
        &job.to_string(),
        "--config",
        config.to_str().unwrap(),
        "--json",
        "--timeout",
        "100ms",
    ]);
    assert_eq!(result.status.code(), Some(0), "{result:?}");
    let events = follow_events(&result);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["exitCode"], 0);
}

#[test]
fn follower_signals_clean_up_checker_and_hook_process_groups() {
    let q = TestQueue::new();
    for (kind, signal) in [("check", libc::SIGINT), ("hook", libc::SIGTERM)] {
        let job = if kind == "check" {
            q.submit(&[], &["sleep", "30"])
        } else {
            let job = q.submit(&[], &["true"]);
            q.wait_state(job, "succeeded", Duration::from_secs(5));
            job
        };
        let parent_path = q.dir.path().join(format!("{kind}-parent"));
        let child_path = q.dir.path().join(format!("{kind}-child"));
        let config = q.dir.path().join(format!("{kind}.toml"));
        let section = if kind == "check" {
            "events = [\"warning\"]\n[[checks]]\nid = \"long\""
        } else {
            "[[hooks]]\nevents = [\"finished\"]"
        };
        std::fs::write(
            &config,
            format!(
                r#"{section}
command = ["sh", "-c", "echo $$ > \"$1\"; sleep 30 & echo $! > \"$2\"; wait", "sh", "{}", "{}"]
timeout = "30s"
"#,
                parent_path.display(),
                child_path.display()
            ),
        )
        .unwrap();
        let follower = spawn_follower(
            &q,
            &[
                "follow",
                &job.to_string(),
                "--config",
                config.to_str().unwrap(),
                "--json",
                "--timeout",
                "10s",
            ],
        );
        wait_for(
            "monitor subprocess and descendant",
            Duration::from_secs(5),
            || {
                std::fs::read_to_string(&child_path)
                    .is_ok_and(|text| text.trim().parse::<i32>().is_ok())
            },
        );
        let parent: i32 = std::fs::read_to_string(parent_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let child: i32 = std::fs::read_to_string(child_path)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::kill(follower.0.as_ref().unwrap().id() as i32, signal) },
            0
        );
        let result = follower_output(follower);
        // Clean up the actual queued workload independently of the monitor.
        if kind == "check" {
            assert_eq!(
                q.job_state(job),
                "running",
                "stopping a follower must not cancel its job"
            );
            q.cli(&["cancel", &job.to_string()]);
            q.wait_state(job, "cancelled", Duration::from_secs(5));
        }
        let alive = |pid| {
            std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                !matches!(
                    stat.rsplit_once(')').unwrap().1.split_whitespace().next(),
                    Some("Z" | "X")
                )
            })
        };
        // Avoid leaking the reproduction processes if this regression returns.
        let deadline = Instant::now() + Duration::from_secs(3);
        while (alive(parent) || alive(child)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let leaked = alive(parent) || alive(child);
        if leaked {
            unsafe {
                libc::killpg(parent, libc::SIGKILL);
            }
        }
        assert!(
            !leaked,
            "{kind} subprocess group survived follower termination"
        );
        assert_eq!(result.status.code(), Some(128 + signal), "{result:?}");
    }
}

#[test]
fn follower_preempts_slow_intermediate_hooks_and_cleans_up_on_broken_pipe() {
    use std::io::{BufRead, Read};
    let q = TestQueue::new();
    for close_pipe in [false, true] {
        let marker = q.dir.path().join(format!("hook-{close_pipe}"));
        let finished = q.dir.path().join(format!("finish-{close_pipe}"));
        let job = q.submit(
            &[],
            &[
                "sh",
                "-c",
                &format!(
                    "while ! test -e '{}'; do sleep 0.05; done",
                    finished.display()
                ),
            ],
        );
        let config = q.dir.path().join(format!("pipe-{close_pipe}.toml"));
        std::fs::write(&config, format!(r#"
events = ["warning"]
[[checks]]
id = "check"
command = ["sh", "-c", "echo '{{\"findings\":[{{\"key\":\"warning\",\"message\":\"attention\"}}]}}'"]
[[hooks]]
events = ["warning"]
command = ["sh", "-c", "echo $$ > '{}'; sleep 30 & wait"]
timeout = "30s"
"#, marker.display())).unwrap();
        let mut follower = spawn_follower(
            &q,
            &[
                "follow",
                &job.to_string(),
                "--config",
                config.to_str().unwrap(),
                "--json",
                "--timeout",
                "5s",
            ],
        );
        let mut reader =
            std::io::BufReader::new(follower.0.as_mut().unwrap().stdout.take().unwrap());
        let mut first = String::new();
        reader.read_line(&mut first).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&first).unwrap()["event"],
            "warning"
        );
        wait_for(
            "slow intermediate hook started",
            Duration::from_secs(3),
            || {
                std::fs::read_to_string(&marker)
                    .is_ok_and(|text| text.trim().parse::<i32>().is_ok())
            },
        );
        let pid: i32 = std::fs::read_to_string(&marker)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let mut remaining = if close_pipe {
            drop(reader);
            None
        } else {
            Some(reader)
        };
        std::fs::write(&finished, "").unwrap();
        let result = follower_output(follower);
        let alive = || {
            std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                !matches!(
                    stat.rsplit_once(')').unwrap().1.split_whitespace().next(),
                    Some("Z" | "X")
                )
            })
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        while alive() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let leaked = alive();
        if leaked {
            unsafe {
                libc::killpg(pid, libc::SIGKILL);
            }
        }
        assert!(
            !leaked,
            "intermediate hook survived completion/pipe closure"
        );
        assert_eq!(
            result.status.code(),
            Some(if close_pipe { 141 } else { 0 }),
            "{result:?}"
        );
        assert!(result.stderr.is_empty(), "{result:?}");
        if let Some(reader) = &mut remaining {
            let mut final_output = String::new();
            reader.read_to_string(&mut final_output).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(final_output.trim()).unwrap()["event"],
                "finished"
            );
        }
    }
}
