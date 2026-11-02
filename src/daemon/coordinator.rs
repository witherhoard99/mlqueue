//! The single coordinator task. All scheduling decisions and database writes
//! are serialized here; socket handlers validate input and send commands, and
//! never launch work independently. No transaction or lock is held across
//! socket I/O, filesystem waits, or process spawning.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::process::CommandExt;
use std::process::Stdio;
use std::time::Instant;

use anyhow::{Context, Result};
use rusqlite::{Connection, Transaction};
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};

use crate::config::Config;
use crate::db::{self, AttemptRow, JobRow};
use crate::domain::{
    self, AttemptId, AttemptState, JobId, JobState, SCHEDULER_SEMANTICS_VERSION, now_ms,
};
use crate::paths::Paths;
use crate::process::artifacts::{
    self, CANCEL_FILE, COMMAND_FILE, CancelFile, CommandFile, EXEC_FILE, ExecFile, IDENTITY_FILE,
    IdentityFile, LIMIT_FILE, LimitFile, RESULT_FILE, RUNNER_LOG_FILE, ResultFile, ResultOutcome,
    START_FILE, StartFile, TIMEOUT_FILE, TimeoutFile,
};
use crate::process::identity;
use crate::protocol::{
    self, DaemonStatusView, ErrorBody, JobUpdateView, LogPathsView, Op, Reply, Request, ResolveAs,
    Response, SubmitParams, error_codes,
};
use crate::scheduler::{self, ReservationSnapshot};

use super::views;

pub enum Msg {
    Request {
        request: Box<Request>,
        reply: oneshot::Sender<Response>,
    },
    Tick,
    Shutdown,
}

#[derive(Debug)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::new(error_codes::INTERNAL, format!("{err:#}"))
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(err: rusqlite::Error) -> Self {
        Self::new(error_codes::INTERNAL, err.to_string())
    }
}

type ApiResult<T> = Result<T, ApiError>;

/// Side effects that must happen after (never inside) the mutation
/// transaction. They are idempotently re-derived from committed state by
/// reconcile, so a crash between commit and action loses nothing.
enum PostAction {
    PublishCancel {
        attempt_id: AttemptId,
        token: String,
        force: bool,
        requested_at: i64,
    },
    PublishTimeLimit {
        attempt_id: AttemptId,
        token: String,
        time_limit_ms: Option<u64>,
        updated_at: i64,
    },
    SignalGroup {
        pgid: i32,
        signal: i32,
    },
}

pub struct Coordinator {
    conn: Connection,
    config: Config,
    paths: Paths,
    started_at: i64,
    boot_id: String,
    /// Set when persisted scheduler semantics cannot be interpreted; blocks
    /// all ordinary admission until an operator intervenes.
    admission_blocked: Option<String>,
    /// Attempts whose runner this daemon process has spawned, with the spawn
    /// instant (for the identity grace period).
    spawned: HashMap<AttemptId, Instant>,
    /// Orphaned attempts that already received a daemon-delivered SIGTERM.
    orphan_term_sent: HashSet<AttemptId>,
    /// Latest committed event id, republished after every handled message so
    /// streaming subscribers can refetch a snapshot instead of polling.
    revisions: watch::Sender<i64>,
    published_revision: i64,
    /// Mirrors `admission_blocked` at the last publication: the flag is
    /// derived in memory, so it is the one snapshot field the event log does
    /// not already cover.
    published_admission_blocked: bool,
}

impl Coordinator {
    pub fn new(conn: Connection, config: Config, paths: Paths) -> Result<Self> {
        // Fail fast if /proc identity primitives are unavailable; every
        // recovery decision depends on them.
        let boot_id = identity::boot_id().context("reading boot id")?;
        let published_revision = db::latest_event_id(&conn)?;
        // Receivers are handed out by `revisions()`; the initial one is
        // dropped because a daemon with no subscribers is the normal case.
        let (revisions, _) = watch::channel(published_revision);
        Ok(Self {
            conn,
            config,
            paths,
            started_at: now_ms(),
            boot_id,
            admission_blocked: None,
            spawned: HashMap::new(),
            orphan_term_sent: HashSet::new(),
            revisions,
            published_revision,
            published_admission_blocked: false,
        })
    }

    /// A receiver that wakes whenever committed queue state changes. Handed
    /// to streaming connections before the coordinator moves to its thread.
    pub fn revisions(&self) -> watch::Receiver<i64> {
        self.revisions.subscribe()
    }

    pub fn run(mut self, mut rx: mpsc::Receiver<Msg>) {
        // Startup recovery is the same reconcile/schedule cycle as a tick:
        // adopt live runners, finalize durable results, respawn unlaunched
        // prepared attempts, orphan/quarantine the rest, restore the
        // reservation before ordinary admission.
        self.tick();
        self.publish_revision();
        while let Some(msg) = rx.blocking_recv() {
            // Every committed change appends an event, so publishing the event
            // cursor once per handled message — after the transaction closed,
            // before the reply leaves for the socket — is the single choke
            // point that covers mutations, ticks and scheduling passes alike.
            let pending = match msg {
                Msg::Request { request, reply } => Some((self.handle_request(*request), reply)),
                Msg::Tick => {
                    self.tick();
                    None
                }
                Msg::Shutdown => break,
            };
            self.publish_revision();
            if let Some((response, reply)) = pending {
                let _ = reply.send(response);
            }
        }
        // Dropping `self` drops the watch sender, which ends every stream.
    }

    /// Notify streaming subscribers that the snapshot they hold may be stale.
    /// Runs on the coordinator thread with nothing locked and never blocks on
    /// a subscriber: `watch` keeps only the newest revision, so a slow reader
    /// coalesces instead of queueing.
    fn publish_revision(&mut self) {
        // `admission_blocked` is derived in memory and appends no event, so it
        // is compared as well as the cursor; every other snapshot field moves
        // only through a committed event.
        let blocked = self.admission_blocked.is_some();
        match db::latest_event_id(&self.conn) {
            Ok(revision) => {
                if revision != self.published_revision
                    || blocked != self.published_admission_blocked
                {
                    self.published_revision = revision;
                    self.published_admission_blocked = blocked;
                    let _ = self.revisions.send(revision);
                }
            }
            Err(err) => tracing::warn!("reading the event cursor for subscribers: {err:#}"),
        }
    }

    fn tick(&mut self) {
        if let Err(err) = self.reconcile_all() {
            tracing::error!("reconcile failed: {err:#}");
        }
        if let Err(err) = self.advance_dependency_graph() {
            tracing::error!("dependency reconciliation failed: {err:#}");
        }
        if let Err(err) = self.schedule() {
            tracing::error!("scheduling pass failed: {err:#}");
        }
    }

    fn advance_dependency_graph(&mut self) -> Result<()> {
        let tx = self.conn.transaction()?;
        db::advance_dependency_graph(&tx, now_ms())?;
        tx.commit()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Request handling
    // -----------------------------------------------------------------------

    fn handle_request(&mut self, request: Request) -> Response {
        let request_id = request.request_id.clone();
        let result = if request.protocol_version != protocol::PROTOCOL_VERSION {
            Err(ApiError::new(
                error_codes::UNSUPPORTED_PROTOCOL,
                format!(
                    "protocol version {} is not supported (daemon speaks {})",
                    request.protocol_version,
                    protocol::PROTOCOL_VERSION
                ),
            ))
        } else if request.op.is_mutation() {
            match &request.idempotency_key {
                Some(key) if !key.is_empty() => self.handle_mutation(&request.op, key),
                _ => Err(ApiError::new(
                    error_codes::MISSING_IDEMPOTENCY_KEY,
                    "mutating operations require a durable idempotency key",
                )),
            }
        } else {
            self.handle_read(&request.op)
        };
        match result {
            Ok(reply) => Response {
                request_id,
                reply: Some(reply),
                error: None,
            },
            Err(err) => Response {
                request_id,
                reply: None,
                error: Some(ErrorBody {
                    code: err.code.to_string(),
                    message: err.message,
                }),
            },
        }
    }

    fn handle_read(&mut self, op: &Op) -> ApiResult<Reply> {
        match op {
            // A subscription's every frame — the initial one and each refresh
            // the connection requests after a revision bump — is a `Status`
            // reply built here, so streaming never queries the database from a
            // connection task.
            Op::Status | Op::Subscribe => Ok(Reply::Status(views::status_view(
                &self.conn,
                &self.paths,
                self.admission_blocked.is_some(),
            )?)),
            Op::SubscribeJob { job, after } => {
                if after.is_some_and(|cursor| cursor < 0) {
                    return Err(ApiError::new(
                        error_codes::INVALID_ARGUMENT,
                        "subscribe_job requires after >= 0",
                    ));
                }
                // The snapshot and cursor/page share a read transaction.
                // No connection can insert a lifecycle event between them.
                let tx = self.conn.transaction()?;
                let row = db::job_row(&tx, *job)?.ok_or_else(|| api_not_found(*job))?;
                let mut snapshot = views::job_view(&tx, &self.paths, &row, false)?;
                if let Some(attempt) = db::attempt_by_number(&tx, *job, row.attempt_count)? {
                    snapshot
                        .attempts
                        .push(views::attempt_view(&self.paths, &attempt));
                }
                const PAGE_SIZE: u32 = 256;
                let (cursor, events, has_more) = match after {
                    None => (db::latest_event_id(&tx)?, Vec::new(), false),
                    Some(after) => {
                        let mut rows = db::job_events_after(&tx, *job, *after, PAGE_SIZE + 1)?;
                        let has_more = rows.len() > PAGE_SIZE as usize;
                        if has_more {
                            rows.pop();
                        }
                        let cursor = rows.last().map_or(*after, |event| event.id);
                        let events = rows.iter().map(views::event_view).collect();
                        (cursor, events, has_more)
                    }
                };
                tx.commit()?;
                Ok(Reply::JobUpdate(JobUpdateView {
                    job: snapshot,
                    cursor,
                    events,
                    has_more,
                }))
            }
            Op::FollowTtsSnapshot => Ok(Reply::FollowTtsSnapshot(views::follow_tts_snapshot(
                &self.conn,
            )?)),
            Op::Events { after, limit } => {
                if *after < 0 || !(1..=1_000).contains(limit) {
                    return Err(ApiError::new(
                        error_codes::INVALID_ARGUMENT,
                        "events requires after >= 0 and limit between 1 and 1000",
                    ));
                }
                let events = db::events_after(&self.conn, *after, *limit)?
                    .iter()
                    .map(views::event_view)
                    .collect();
                Ok(Reply::Events { events })
            }
            Op::Classes => Ok(Reply::Classes {
                classes: db::classes(&self.conn)?,
            }),
            Op::DefaultJob { command } => Ok(Reply::DefaultJob {
                job: db::default_for_command(&self.conn, command)?
                    .map(|job| views::job_view(&self.conn, &self.paths, &job, false))
                    .transpose()?,
            }),
            Op::Attempts { job, before, limit } => {
                if before.is_some_and(|number| number <= 0) || !(1..=100).contains(limit) {
                    return Err(ApiError::new(
                        error_codes::INVALID_ARGUMENT,
                        "attempts requires before > 0 and limit between 1 and 100",
                    ));
                }
                db::job_row(&self.conn, *job)?.ok_or_else(|| api_not_found(*job))?;
                let mut rows = db::attempts_page(&self.conn, *job, *before, limit + 1)?;
                let has_more = rows.len() > *limit as usize;
                if has_more {
                    rows.pop();
                }
                let next_before = if has_more {
                    rows.last().map(|row| row.number)
                } else {
                    None
                };
                Ok(Reply::Attempts {
                    attempts: rows
                        .iter()
                        .map(|row| views::attempt_view(&self.paths, row))
                        .collect(),
                    next_before,
                })
            }
            Op::ClassShow { name } => {
                let class = require_class(&self.conn, name)?;
                let jobs = db::class_jobs(&self.conn, name)?
                    .iter()
                    .map(|job| views::job_view(&self.conn, &self.paths, job, false))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Reply::ClassJobs { class, jobs })
            }
            Op::Show { job } => {
                let row = db::job_row(&self.conn, *job)?.ok_or_else(|| {
                    ApiError::new(error_codes::NOT_FOUND, format!("job {job} not found"))
                })?;
                Ok(Reply::Job {
                    job: views::job_view(&self.conn, &self.paths, &row, true)?,
                })
            }
            Op::See { job } => {
                let job = match *job {
                    Some(job) => job,
                    None => db::latest_job_id(&self.conn)?
                        .ok_or_else(|| ApiError::new(error_codes::NOT_FOUND, "no jobs"))?,
                };
                let row = db::job_row(&self.conn, job)?.ok_or_else(|| api_not_found(job))?;
                Ok(Reply::Job {
                    job: views::job_view(&self.conn, &self.paths, &row, true)?,
                })
            }
            Op::Logs { job, attempt } => {
                let attempt_row = match attempt {
                    Some(number) => db::attempt_by_number(&self.conn, *job, *number)?,
                    None => db::attempts_page(&self.conn, *job, None, 1)?
                        .into_iter()
                        .next(),
                }
                .ok_or_else(|| {
                    ApiError::new(
                        error_codes::NOT_FOUND,
                        format!("job {job} has no matching attempt (has it started?)"),
                    )
                })?;
                let dir = self.paths.attempt_dir(attempt_row.id);
                Ok(Reply::LogPaths(LogPathsView {
                    job: *job,
                    attempt: attempt_row.id,
                    attempt_number: attempt_row.number,
                    attempt_state: attempt_row.state.as_str().to_string(),
                    exit_code: attempt_row.exit_code,
                    term_signal: attempt_row.term_signal,
                    message: attempt_row.message.clone(),
                    stdout: dir.join(artifacts::STDOUT_LOG_FILE).display().to_string(),
                    stderr: dir.join(artifacts::STDERR_LOG_FILE).display().to_string(),
                }))
            }
            Op::DaemonStatus => Ok(Reply::DaemonStatus(DaemonStatusView {
                pid: std::process::id(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                protocol_version: protocol::PROTOCOL_VERSION,
                scheduler_semantics_version: SCHEDULER_SEMANTICS_VERSION,
                started_at: self.started_at,
                db_path: self.paths.db().display().to_string(),
                socket_path: self.paths.socket().display().to_string(),
                active_leases: db::active_leases(&self.conn)?.len() as u32,
                queued_jobs: db::count_jobs_in_state(&self.conn, JobState::Queued)?,
                admission_blocked: self.admission_blocked.is_some(),
                paused: db::is_paused(&self.conn)?,
            })),
            Op::RecoverList => {
                let attempts = db::non_terminal_attempts(&self.conn)?
                    .into_iter()
                    .filter(|a| {
                        matches!(a.state, AttemptState::Orphaned | AttemptState::Quarantined)
                    })
                    .map(|a| views::attempt_view(&self.paths, &a))
                    .collect();
                Ok(Reply::RecoverList { attempts })
            }
            _ => Err(ApiError::new(
                error_codes::INTERNAL,
                "mutation routed to read handler",
            )),
        }
    }

    /// Insert the operation record and the mutation in one transaction. An
    /// identical retry returns the original result; a reused key with a
    /// different payload conflicts.
    fn handle_mutation(&mut self, op: &Op, key: &str) -> ApiResult<Reply> {
        let hash = op.request_hash();
        if let Some((stored_hash, stored_response)) = db::lookup_operation(&self.conn, key)? {
            let matches_legacy_v5_submit =
                op.legacy_v5_request_hash().as_deref() == Some(stored_hash.as_str());
            if stored_hash != hash && !matches_legacy_v5_submit {
                return Err(ApiError::new(
                    error_codes::IDEMPOTENCY_CONFLICT,
                    format!("idempotency key {key:?} was already used with a different request"),
                ));
            }
            let mut reply = serde_json::from_str(&stored_response).map_err(|err| {
                ApiError::new(
                    error_codes::INTERNAL,
                    format!("stored response corrupt: {err}"),
                )
            })?;
            if views::compact_mutation_history(&mut reply) {
                db::compact_operation_response(
                    &self.conn,
                    key,
                    &serde_json::to_string(&reply)
                        .map_err(|err| ApiError::new(error_codes::INTERNAL, err.to_string()))?,
                )?;
            }
            return Ok(reply);
        }

        // Argument and filesystem validation runs before the write
        // transaction opens: a slow or hung cwd (NFS, FUSE) must never stall
        // the transaction and every mutation queued behind it.
        if let Op::Submit(params) = op {
            validate_submission_fs(params)?;
        }

        let now = now_ms();
        let (reply, actions) = {
            let tx = self.conn.transaction().map_err(ApiError::from)?;
            let (mut reply, actions) = apply_mutation(&tx, &self.paths, op, now)?;
            views::compact_mutation_history(&mut reply);
            let encoded = serde_json::to_string(&reply)
                .map_err(|err| ApiError::new(error_codes::INTERNAL, err.to_string()))?;
            db::insert_operation(&tx, key, op.kind(), &hash, &encoded, now)?;
            tx.commit().map_err(ApiError::from)?;
            (reply, actions)
        };
        for action in actions {
            self.perform(action);
        }
        // Mutations change eligibility; run the same cycle as a tick.
        self.tick();
        Ok(reply)
    }

    fn perform(&mut self, action: PostAction) {
        match action {
            PostAction::PublishCancel {
                attempt_id,
                token,
                force,
                requested_at,
            } => {
                let dir = self.paths.attempt_dir(attempt_id);
                if let Err(err) = fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&dir)
                {
                    tracing::warn!("creating {} for cancel intent: {err}", dir.display());
                }
                if let Err(err) = artifacts::write_artifact(
                    &dir,
                    CANCEL_FILE,
                    &CancelFile {
                        token,
                        force,
                        requested_at: Some(requested_at),
                    },
                    false,
                ) {
                    // Reconcile republishes committed cancellation intent.
                    tracing::warn!("publishing cancel for attempt {attempt_id}: {err}");
                }
            }
            PostAction::PublishTimeLimit {
                attempt_id,
                token,
                time_limit_ms,
                updated_at,
            } => {
                publish_time_limit(&self.paths, attempt_id, &token, time_limit_ms, updated_at);
            }
            PostAction::SignalGroup { pgid, signal } => unsafe {
                libc::killpg(pgid, signal);
            },
        }
    }

    // -----------------------------------------------------------------------
    // Reconciliation: artifacts + /proc -> database state
    // -----------------------------------------------------------------------

    fn reconcile_all(&mut self) -> Result<()> {
        let attempts = db::non_terminal_attempts(&self.conn)?;
        // Attempts finalized by mutation paths (free functions that cannot
        // reach these maps) are pruned here instead.
        let live: std::collections::HashSet<AttemptId> =
            attempts.iter().map(|attempt| attempt.id).collect();
        self.spawned.retain(|id, _| live.contains(id));
        self.orphan_term_sent.retain(|id| live.contains(id));
        for attempt in attempts {
            if let Err(err) = self.reconcile_attempt(&attempt) {
                tracing::error!("reconciling attempt {}: {err:#}", attempt.id);
            }
        }
        Ok(())
    }

    fn reconcile_attempt(&mut self, attempt: &AttemptRow) -> Result<()> {
        let dir = self.paths.attempt_dir(attempt.id);

        // A valid terminal result always wins, whatever the current state:
        // the runner only publishes it after the command group is provably
        // gone (or was never created).
        match artifacts::read_artifact::<ResultFile>(&dir.join(RESULT_FILE)) {
            Ok(Some(result)) if result.token == attempt.launch_token => {
                return self.finalize_from_result(attempt, &result);
            }
            Ok(Some(_)) => return self.quarantine(attempt, "result artifact token mismatch"),
            Err(err) => {
                return self.quarantine(attempt, &format!("corrupt result artifact: {err}"));
            }
            Ok(None) => {}
        }

        if self.recover_rebooted_default(attempt, &dir)? {
            return Ok(());
        }

        // Republish committed cancellation intent (crash between commit and
        // publication, or daemon restart).
        if attempt.cancel_requested
            && !attempt.state.is_terminal()
            && !matches!(
                artifacts::read_artifact::<CancelFile>(&dir.join(CANCEL_FILE)),
                Ok(Some(_))
            )
        {
            let _ = artifacts::write_artifact(
                &dir,
                CANCEL_FILE,
                &CancelFile {
                    token: attempt.launch_token.clone(),
                    force: attempt.cancel_force,
                    requested_at: attempt.cancel_requested_at,
                },
                false,
            );
        }

        // A crash between the limit commit and its publication, or a daemon
        // restart, leaves the runner holding the launch snapshot. Republish
        // the job's current limit so the surviving runner adopts it.
        if matches!(
            attempt.state,
            AttemptState::Prepared | AttemptState::Authorized | AttemptState::Running
        ) {
            self.republish_time_limit(attempt, &dir);
        }

        match attempt.state {
            AttemptState::Prepared => self.reconcile_prepared(attempt, &dir),
            AttemptState::Authorized => self.reconcile_authorized(attempt, &dir),
            AttemptState::Running => self.reconcile_running(attempt),
            AttemptState::Orphaned => self.reconcile_orphaned(attempt, &dir),
            // Quarantine holds its lease until an operator resolves it (or a
            // valid result appears above).
            AttemptState::Quarantined => Ok(()),
            _ => Ok(()),
        }
    }

    fn recover_rebooted_default(
        &mut self,
        attempt: &AttemptRow,
        dir: &std::path::Path,
    ) -> Result<bool> {
        if !matches!(
            attempt.state,
            AttemptState::Authorized | AttemptState::Running | AttemptState::Orphaned
        ) || !attempt
            .runner_boot_id
            .as_deref()
            .is_some_and(|boot| boot != self.boot_id)
        {
            return Ok(false);
        }
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of rebooted attempt")?;
        if !job.is_default || job.state.is_terminal() || job.state == JobState::Held {
            return Ok(false);
        }
        // A changed boot ID proves the old group is gone. Keep committed
        // timeouts and user cancellation instead of treating them as reboot loss.
        let timeout = match artifacts::read_artifact::<TimeoutFile>(&dir.join(TIMEOUT_FILE)) {
            Ok(Some(file)) if file.token == attempt.launch_token => true,
            Ok(None) => attempt.timed_out_at.is_some(),
            Ok(Some(_)) => {
                self.quarantine(attempt, "timeout artifact token mismatch")?;
                return Ok(true);
            }
            Err(err) => {
                self.quarantine(attempt, &format!("corrupt timeout artifact: {err}"))?;
                return Ok(true);
            }
        };
        let state = if timeout {
            AttemptState::Failed
        } else if attempt.cancel_requested {
            AttemptState::Cancelled
        } else {
            AttemptState::Lost
        };
        self.finalize_attempt_and_job(
            attempt,
            state,
            None,
            None,
            Some("previous boot ended; process group is gone"),
            "daemon",
        )?;
        Ok(true)
    }

    fn republish_time_limit(&self, attempt: &AttemptRow, dir: &std::path::Path) {
        let (desired, set_at) = match db::job_row(&self.conn, attempt.job_id) {
            Ok(Some(job)) => (
                job.time_limit_ms.map(|value| value as u64),
                job.time_limit_set_at,
            ),
            Ok(None) => return,
            Err(err) => {
                tracing::warn!(
                    "reading job {} to republish its time limit: {err:#}",
                    attempt.job_id
                );
                return;
            }
        };
        if published_time_limit_is_current(dir, &attempt.launch_token, desired, set_at) {
            return;
        }
        let Some(set_at) = set_at else {
            return;
        };
        publish_time_limit(
            &self.paths,
            attempt.id,
            &attempt.launch_token,
            desired,
            set_at,
        );
    }

    fn reconcile_prepared(&mut self, attempt: &AttemptRow, dir: &std::path::Path) -> Result<()> {
        // Not spawned by this daemon process: crash-recovery. The per-attempt
        // runner flock makes a duplicate spawn race safe.
        let Some(spawned_at) = self.spawned.get(&attempt.id).copied() else {
            self.launch(attempt.id);
            return Ok(());
        };

        match artifacts::read_artifact::<IdentityFile>(&dir.join(IDENTITY_FILE)) {
            Ok(Some(identity_file)) => {
                if identity_file.token != attempt.launch_token {
                    return self.quarantine(attempt, "identity artifact token mismatch");
                }
                if attempt.cancel_requested {
                    // Cancellation of a prepared attempt finalizes it in the
                    // cancel mutation; reaching here means only the flag
                    // raced, and the runner will exit on cancel.json.
                    return Ok(());
                }
                // A stale identity left by a runner that died before this
                // daemon spawned its replacement must not be authorized: the
                // replacement republishes its own identity (the write is
                // non-exclusive under the per-attempt flock). Since the
                // attempt is still `prepared`, no `start` was ever published
                // and failing after the grace period is safe.
                if !identity::identity_alive(
                    identity_file.runner_pid,
                    identity_file.runner_start_time,
                    &identity_file.boot_id,
                ) {
                    if spawned_at.elapsed().as_millis() as u64
                        > self.config.runner_identity_grace_ms
                    {
                        self.finalize_attempt_and_job(
                            attempt,
                            AttemptState::Failed,
                            None,
                            None,
                            Some("runner died before launch authorization"),
                            "daemon",
                        )?;
                    }
                    return Ok(());
                }
                self.authorize(attempt, &identity_file)
            }
            Ok(None) => {
                // Never authorized and no runner identity: if the runner does
                // not appear within the grace period it is dead, and without
                // `start` no command can ever have run — safe to fail.
                if spawned_at.elapsed().as_millis() as u64 > self.config.runner_identity_grace_ms {
                    self.finalize_attempt_and_job(
                        attempt,
                        AttemptState::Failed,
                        None,
                        None,
                        Some("runner never published its identity"),
                        "daemon",
                    )?;
                }
                Ok(())
            }
            Err(err) => self.quarantine(attempt, &format!("corrupt identity artifact: {err}")),
        }
    }

    /// Commit the launch authorization, then publish the durable `start`
    /// marker. Cancellation is serialized against this because both run on
    /// the coordinator.
    fn authorize(&mut self, attempt: &AttemptRow, identity_file: &IdentityFile) -> Result<()> {
        let now = now_ms();
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of live attempt")?;
        if job.state != JobState::Starting {
            // Revalidation failed (e.g. cancel raced); leave the runner
            // waiting — cancellation paths stop it.
            return Ok(());
        }
        {
            let tx = self.conn.transaction()?;
            db::set_attempt_runner_identity(
                &tx,
                attempt.id,
                identity_file.runner_pid,
                identity_file.runner_start_time,
                &identity_file.boot_id,
            )?;
            db::set_attempt_authorized(&tx, attempt.id, now)?;
            db::append_event(
                &tx,
                Some(attempt.job_id),
                Some(attempt.id),
                "attempt_authorized",
                "daemon",
                Some(&json!({ "runnerPid": identity_file.runner_pid }).to_string()),
            )?;
            tx.commit()?;
        }
        let dir = self.paths.attempt_dir(attempt.id);
        match artifacts::write_artifact(
            &dir,
            START_FILE,
            &StartFile {
                token: attempt.launch_token.clone(),
            },
            true,
        ) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            // Committed but unpublished: the authorized reconcile branch
            // republishes on the next tick.
            Err(err) => {
                tracing::warn!("publishing start for attempt {}: {err}", attempt.id);
                Ok(())
            }
        }
    }

    fn reconcile_authorized(&mut self, attempt: &AttemptRow, dir: &std::path::Path) -> Result<()> {
        let runner_dead = match (
            attempt.runner_pid,
            attempt.runner_start_time,
            &attempt.runner_boot_id,
        ) {
            (Some(pid), Some(start_time), Some(boot)) => {
                !identity::identity_alive(pid, start_time, boot)
            }
            _ => false,
        };
        let start_exists = dir.join(START_FILE).exists();

        // Dead runner that never saw a published authorization: the command
        // provably never ran, so the lease is safe to release.
        if runner_dead && !start_exists {
            return self.finalize_attempt_and_job(
                attempt,
                AttemptState::Failed,
                None,
                None,
                Some("runner died before launch authorization was published"),
                "daemon",
            );
        }

        // Republish a missing authorization marker; the committed state
        // still allows it.
        if !start_exists {
            match artifacts::write_artifact(
                dir,
                START_FILE,
                &StartFile {
                    token: attempt.launch_token.clone(),
                },
                true,
            ) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => tracing::warn!("republishing start for attempt {}: {err}", attempt.id),
            }
        }

        match artifacts::read_artifact::<ExecFile>(&dir.join(EXEC_FILE)) {
            Ok(Some(exec)) if exec.token == attempt.launch_token => {
                let now = now_ms();
                let tx = self.conn.transaction()?;
                db::set_attempt_running(
                    &tx,
                    attempt.id,
                    exec.pid,
                    exec.pgid,
                    exec.start_time,
                    exec.started_at.unwrap_or(now),
                    now,
                )?;
                db::update_job_state(&tx, attempt.job_id, JobState::Running, None, now)?;
                db::append_event(
                    &tx,
                    Some(attempt.job_id),
                    Some(attempt.id),
                    "attempt_running",
                    "runner",
                    Some(&json!({ "pid": exec.pid, "pgid": exec.pgid }).to_string()),
                )?;
                tx.commit()?;
                Ok(())
            }
            Ok(Some(_)) => self.quarantine(attempt, "exec artifact token mismatch"),
            Err(err) => self.quarantine(attempt, &format!("corrupt exec artifact: {err}")),
            Ok(None) if runner_dead => {
                // Authorization was visible but the runner died before the
                // exec handshake: a command process group may or may not
                // exist, and its pgid is unknown. Never guess.
                self.quarantine(
                    attempt,
                    "runner died between authorization and exec handshake; command state unknown",
                )
            }
            Ok(None) => Ok(()),
        }
    }

    fn reconcile_running(&mut self, attempt: &AttemptRow) -> Result<()> {
        let runner_alive = match (
            attempt.runner_pid,
            attempt.runner_start_time,
            &attempt.runner_boot_id,
        ) {
            (Some(pid), Some(start_time), Some(boot)) => {
                identity::identity_alive(pid, start_time, boot)
            }
            _ => false,
        };
        if runner_alive {
            return Ok(());
        }
        // Runner died mid-supervision. The command group may still be live;
        // the lease is retained until it is proven empty.
        let now = now_ms();
        let tx = self.conn.transaction()?;
        db::set_attempt_state(&tx, attempt.id, AttemptState::Orphaned, Some("runner died"))?;
        db::update_job_state(
            &tx,
            attempt.job_id,
            JobState::NeedsAttention,
            Some("attempt orphaned: runner died while the command may still run"),
            now,
        )?;
        db::append_event(
            &tx,
            Some(attempt.job_id),
            Some(attempt.id),
            "attempt_orphaned",
            "daemon",
            None,
        )?;
        tx.commit()?;
        Ok(())
    }

    fn reconcile_orphaned(&mut self, attempt: &AttemptRow, dir: &std::path::Path) -> Result<()> {
        let (Some(pgid), Some(boot)) = (attempt.cmd_pgid, attempt.runner_boot_id.as_deref()) else {
            // Orphaned is only entered from running, which has exec identity.
            return self.quarantine(attempt, "orphaned attempt lacks command identity");
        };
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of orphaned attempt")?;
        let limit_deadline = match (attempt.command_started_at, job.time_limit_ms) {
            (Some(started_at), Some(limit)) => started_at.checked_add(limit),
            _ => None,
        };
        let policy_deadline = match (limit_deadline, attempt.window_end_at) {
            (Some(limit), Some(window)) => Some(limit.min(window)),
            (limit, window) => limit.or(window),
        };
        let limit_set_at = if policy_deadline.is_some() && policy_deadline == attempt.window_end_at
        {
            None
        } else {
            job.time_limit_set_at
        };
        // The runner records a timeout it has already started. That commitment
        // outlives both the runner and a later edit of the job's limit.
        let committed = match artifacts::read_artifact::<TimeoutFile>(&dir.join(TIMEOUT_FILE)) {
            Ok(Some(file)) if file.token == attempt.launch_token => Some(file),
            Ok(Some(_)) => {
                return self.quarantine(attempt, "timeout artifact token mismatch");
            }
            Err(err) => {
                return self.quarantine(attempt, &format!("corrupt timeout artifact: {err}"));
            }
            Ok(None) => None,
        };
        let now = now_ms();
        let deadline = committed
            .as_ref()
            .map(|file| file.deadline)
            .or(attempt.timed_out_at)
            .or(policy_deadline);
        let deadline_expired =
            committed.is_some() || deadline.is_some_and(|deadline| now >= deadline);
        let timeout_classifies = if committed.is_some() || attempt.timed_out_at.is_some() {
            true
        } else if let Some(deadline) = deadline.filter(|_| deadline_expired) {
            if attempt.cancel_requested {
                attempt.cancel_requested_at.is_some_and(|cancelled_at| {
                    domain::timeout_wins_over_cancel(cancelled_at, deadline, limit_set_at)
                })
            } else {
                true
            }
        } else {
            false
        };
        // A deadline noticed only after the group has already drained is not a
        // timeout: nobody committed to termination while the command was alive.
        // `timeout.json` and `timed_out_at` are that commitment.
        let timed_out = committed.is_some() || attempt.timed_out_at.is_some();
        if !identity::group_possibly_alive(pgid, boot, attempt.cmd_start_time) {
            let (state, message) = if timed_out {
                (
                    AttemptState::Failed,
                    if deadline == attempt.window_end_at && attempt.window_end_at.is_some() {
                        "run window closed; orphaned process group drained"
                    } else {
                        "time limit exceeded; orphaned process group drained"
                    },
                )
            } else if attempt.cancel_requested {
                (
                    AttemptState::Cancelled,
                    "cancelled; orphaned process group drained",
                )
            } else {
                (
                    AttemptState::Lost,
                    "runner died; command exit status unknown",
                )
            };
            return self.finalize_attempt_and_job(
                attempt,
                state,
                None,
                None,
                Some(message),
                "daemon",
            );
        }
        // The group is still live and there is no runner: the daemon itself
        // delivers committed cancellation signals from outside the group.
        if deadline_expired && timeout_classifies {
            let deadline = deadline.expect("timeout precedence requires a deadline");
            if attempt.timed_out_at.is_none() {
                db::set_attempt_timed_out(&self.conn, attempt.id, deadline)?;
            }
            // Freeze the grace anchor in timeout.json. A later edit of the job
            // limit must not move a kill that has already been armed.
            let anchor = if let Some(file) = &committed {
                file.observed_at
            } else {
                let anchor = domain::timeout_kill_anchor(deadline, limit_set_at, None);
                let record = TimeoutFile {
                    token: attempt.launch_token.clone(),
                    deadline,
                    observed_at: anchor,
                };
                if let Err(err) = artifacts::write_artifact(dir, TIMEOUT_FILE, &record, true)
                    && err.kind() != std::io::ErrorKind::AlreadyExists
                {
                    tracing::warn!("recording timeout for attempt {}: {err}", attempt.id);
                }
                anchor
            };
            if self.orphan_term_sent.insert(attempt.id) {
                unsafe {
                    libc::killpg(pgid, libc::SIGTERM);
                }
            }
            if now.saturating_sub(anchor) >= self.config.cancel_grace_ms as i64 {
                unsafe {
                    libc::killpg(pgid, libc::SIGKILL);
                }
            }
        } else if deadline_expired {
            // The deadline fired, but an earlier cancel keeps the classification.
            // Escalation still uses the deadline that was already in force, or
            // the installation time of a limit moved into the past.
            let deadline = deadline.expect("expired deadline");
            let anchor = domain::timeout_kill_anchor(deadline, limit_set_at, None);
            if self.orphan_term_sent.insert(attempt.id) {
                unsafe {
                    libc::killpg(pgid, libc::SIGTERM);
                }
            }
            if now.saturating_sub(anchor) >= self.config.cancel_grace_ms as i64 {
                unsafe {
                    libc::killpg(pgid, libc::SIGKILL);
                }
            }
        } else if attempt.cancel_requested {
            if self.orphan_term_sent.insert(attempt.id) {
                unsafe {
                    libc::killpg(pgid, libc::SIGTERM);
                }
            }
            let grace_elapsed = attempt
                .cancel_requested_at
                .is_some_and(|at| now_ms() - at >= self.config.cancel_grace_ms as i64);
            if attempt.cancel_force && grace_elapsed {
                unsafe {
                    libc::killpg(pgid, libc::SIGKILL);
                }
            }
        }
        Ok(())
    }

    fn quarantine(&mut self, attempt: &AttemptRow, reason: &str) -> Result<()> {
        if attempt.state == AttemptState::Quarantined {
            return Ok(());
        }
        tracing::error!("quarantining attempt {}: {reason}", attempt.id);
        let now = now_ms();
        let tx = self.conn.transaction()?;
        db::set_attempt_state(&tx, attempt.id, AttemptState::Quarantined, Some(reason))?;
        db::update_job_state(
            &tx,
            attempt.job_id,
            JobState::NeedsAttention,
            Some(&format!("attempt quarantined: {reason}")),
            now,
        )?;
        db::append_event(
            &tx,
            Some(attempt.job_id),
            Some(attempt.id),
            "attempt_quarantined",
            "daemon",
            Some(&json!({ "reason": reason }).to_string()),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn finalize_from_result(&mut self, attempt: &AttemptRow, result: &ResultFile) -> Result<()> {
        let (state, message) = match result.outcome {
            ResultOutcome::Exited => {
                if result.exit_code == Some(0) {
                    (AttemptState::Succeeded, result.message.clone())
                } else {
                    let describe = match (result.exit_code, result.term_signal) {
                        (Some(code), _) => format!("command exited with code {code}"),
                        (None, Some(signal)) => format!("command terminated by signal {signal}"),
                        (None, None) => "command exited abnormally".to_string(),
                    };
                    (
                        AttemptState::Failed,
                        Some(match &result.message {
                            Some(extra) => format!("{describe} ({extra})"),
                            None => describe,
                        }),
                    )
                }
            }
            ResultOutcome::LaunchFailed | ResultOutcome::AuthorizationTimeout => {
                (AttemptState::Failed, result.message.clone())
            }
            ResultOutcome::TimedOut | ResultOutcome::WindowClosed => {
                (AttemptState::Failed, result.message.clone())
            }
            ResultOutcome::Cancelled => (AttemptState::Cancelled, result.message.clone()),
        };
        self.finalize_attempt_and_job(
            attempt,
            state,
            result.exit_code,
            result.term_signal,
            message.as_deref(),
            "runner",
        )
    }

    /// One transaction: terminal attempt state, lease release, job
    /// transition (including automatic retry policy), and events.
    fn finalize_attempt_and_job(
        &mut self,
        attempt: &AttemptRow,
        state: AttemptState,
        exit_code: Option<i32>,
        term_signal: Option<i32>,
        message: Option<&str>,
        actor: &str,
    ) -> Result<()> {
        let now = now_ms();
        {
            let tx = self.conn.transaction()?;
            db::finalize_attempt(&tx, attempt.id, state, exit_code, term_signal, message, now)?;
            db::release_lease(&tx, attempt.id, now)?;
            db::append_event(
                &tx,
                Some(attempt.job_id),
                Some(attempt.id),
                &format!("attempt_{}", state.as_str()),
                actor,
                message
                    .map(|m| json!({ "message": m }).to_string())
                    .as_deref(),
            )?;
            let job = db::job_row(&tx, attempt.job_id)?.context("job of finalized attempt")?;
            if !job.state.is_terminal() {
                let attempts_used = if job.is_default {
                    db::default_failure_count(&tx, job.id)?
                } else {
                    job.attempt_count
                };
                match state {
                    _ if should_restart_default(
                        &job,
                        attempt,
                        state,
                        &self.boot_id,
                        attempts_used,
                    ) =>
                    {
                        requeue_default(&tx, &job, now, "default job ready for another idle run")?;
                    }
                    AttemptState::Succeeded => {
                        db::update_job_state(&tx, job.id, JobState::Succeeded, None, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_succeeded", actor, None)?;
                    }
                    AttemptState::Cancelled => {
                        db::update_job_state(&tx, job.id, JobState::Cancelled, message, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_cancelled", actor, None)?;
                    }
                    AttemptState::Lost => {
                        db::update_job_state(&tx, job.id, JobState::Lost, message, now)?;
                        db::append_event(&tx, Some(job.id), None, "job_lost", actor, None)?;
                    }
                    AttemptState::Failed => {
                        if attempts_used < job.max_attempts && !attempt.cancel_requested {
                            let retry_delay = if job.is_default {
                                job.retry_delay_ms.max(1_000)
                            } else {
                                job.retry_delay_ms
                            };
                            let not_before =
                                (retry_delay > 0).then(|| now.saturating_add(retry_delay));
                            db::update_job_state(&tx, job.id, JobState::Queued, message, now)?;
                            db::set_job_retry_not_before(&tx, job.id, not_before, now)?;
                            db::clear_job_readiness(&tx, job.id, now)?;
                            db::append_event(
                                &tx,
                                Some(job.id),
                                Some(attempt.id),
                                "retry_scheduled",
                                "daemon",
                                Some(
                                    &json!({
                                        "attemptsUsed": job.attempt_count,
                                        "maxAttempts": job.max_attempts,
                                        "notBefore": not_before,
                                    })
                                    .to_string(),
                                ),
                            )?;
                        } else {
                            db::update_job_state(&tx, job.id, JobState::Failed, message, now)?;
                            db::append_event(&tx, Some(job.id), None, "job_failed", actor, None)?;
                        }
                    }
                    _ => unreachable!("finalize called with non-terminal state"),
                }
            }
            db::advance_dependency_graph(&tx, now)?;
            tx.commit()?;
        }
        self.spawned.remove(&attempt.id);
        self.orphan_term_sent.remove(&attempt.id);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Scheduling
    // -----------------------------------------------------------------------

    fn schedule(&mut self) -> Result<()> {
        if self.admission_blocked.is_some() {
            // Blocked admission heals once the offending reservation is gone
            // or interpretable again (the operator cancelled or held the
            // protected job — an explicit, audited resolution); requiring a
            // daemon restart on top of that would strand the queue.
            let still_blocked = match db::active_reservation(&self.conn)? {
                Some(res) => matches!(
                    db::backfill_frontier(&self.conn, &res)?,
                    db::BackfillFrontier::Unsupported(_)
                ),
                None => false,
            };
            if still_blocked {
                return Ok(());
            }
            tracing::info!("admission unblocked: the unsupported reservation was resolved");
            self.admission_blocked = None;
        }
        if db::is_paused(&self.conn)? {
            return Ok(());
        }
        let now = now_ms();
        let leases = db::active_leases(&self.conn)?;
        let reservation = db::active_reservation(&self.conn)?;

        // Interpret the persisted reservation under its recorded semantics
        // version. `pending_freeze` carries the cutoff to pin if the backfill
        // window closed this pass and the reservation survives it.
        let mut pending_freeze: Option<i64> = None;
        let snapshot = match &reservation {
            None => None,
            Some(res) => {
                let cutoff_seq = match db::backfill_frontier(&self.conn, res)? {
                    db::BackfillFrontier::OpenWindow => None,
                    db::BackfillFrontier::Frozen(cutoff) => Some(cutoff),
                    // The window just closed: this pass already plans against
                    // the frontier it is about to pin.
                    db::BackfillFrontier::FreezePending(cutoff) => {
                        pending_freeze = Some(cutoff);
                        Some(cutoff)
                    }
                    db::BackfillFrontier::Unsupported(version) => {
                        let reason = format!(
                            "active reservation {} uses scheduler semantics version {version} \
                             (daemon implements {}); admission blocked until an operator \
                             migrates or resolves it",
                            res.id, SCHEDULER_SEMANTICS_VERSION
                        );
                        tracing::error!("{reason}");
                        self.admission_blocked = Some(reason);
                        return Ok(());
                    }
                };
                Some(ReservationSnapshot {
                    job_id: res.job_id,
                    cutoff_seq,
                    consumed: res.consumed.clone(),
                })
            }
        };

        let eligible = db::eligible_candidates(&self.conn, now)?;
        if eligible.iter().any(|candidate| !candidate.is_default) {
            // Commit the yield before signalling. Keep the lease until the
            // default exits; cancellation recovery handles daemon restarts.
            if let Some(default) = db::default_job(&self.conn)?
                && let Some(attempt) = db::live_attempt_for_job(&self.conn, default.id)?
            {
                let tx = self.conn.transaction()?;
                let actions = request_default_yield(&tx, &default, &attempt, now)?;
                tx.commit()?;
                for action in actions {
                    self.perform(action);
                }
                return Ok(());
            }
        }
        let active: Vec<_> = leases.iter().map(|(_, lease)| lease.clone()).collect();
        let outcome = scheduler::plan_pass(&active, &eligible, snapshot.as_ref());
        // A freeze is only persisted when the reservation outlives the pass;
        // a satisfied or invalidated reservation is resolved instead.
        let commit_freeze = pending_freeze.is_some()
            && !outcome.satisfy_reservation
            && outcome.invalidate_reservation.is_none();
        if outcome.starts.is_empty()
            && !outcome.satisfy_reservation
            && outcome.invalidate_reservation.is_none()
            && outcome.create_reservation.is_none()
            && !commit_freeze
        {
            return Ok(());
        }

        let lease_attempt_by_job: BTreeMap<JobId, AttemptId> = leases
            .iter()
            .map(|(attempt, lease)| (lease.job_id, *attempt))
            .collect();
        let mut launched: Vec<AttemptId> = Vec::new();
        {
            let tx = self.conn.transaction()?;
            // Revalidate and create every attempt + lease first so backfill
            // and seeding rows can reference attempt IDs.
            let mut batch: BTreeMap<JobId, AttemptId> = BTreeMap::new();
            for start in &outcome.starts {
                let job = db::job_row(&tx, start.job_id)?.context("scheduled job exists")?;
                anyhow::ensure!(
                    job.state == JobState::Queued,
                    "revalidation failed: job {} is {}",
                    job.id,
                    job.state
                );
                let token = uuid::Uuid::new_v4().to_string();
                let attempt = db::create_attempt_with_lease(&tx, &job, &token, now)?;
                db::append_event(
                    &tx,
                    Some(job.id),
                    Some(attempt.id),
                    "attempt_prepared",
                    "daemon",
                    Some(
                        &json!({
                            "number": attempt.number,
                            "maxParallelRuns": job.max_parallel_runs,
                            "backfill": start.consumes_bypass(),
                        })
                        .to_string(),
                    ),
                )?;
                batch.insert(job.id, attempt.id);
                launched.push(attempt.id);
            }

            if commit_freeze {
                let (id, job, cutoff) = reservation
                    .as_ref()
                    .zip(pending_freeze)
                    .map(|(res, cutoff)| (res.id, res.job_id, cutoff))
                    .context("freeze without reservation")?;
                db::freeze_reservation_cutoff(&tx, id, cutoff)?;
                db::append_event(
                    &tx,
                    Some(job),
                    None,
                    "backfill_cutoff_frozen",
                    "daemon",
                    Some(&json!({ "backfillCutoff": cutoff }).to_string()),
                )?;
            }

            let mut current_reservation = reservation.as_ref().map(|res| (res.id, res.job_id));
            if outcome.satisfy_reservation {
                let (id, job) = current_reservation
                    .take()
                    .context("satisfy without reservation")?;
                db::resolve_reservation(&tx, id, "satisfied", "protected job started", now)?;
                db::append_event(
                    &tx,
                    Some(job),
                    None,
                    "reservation_satisfied",
                    "daemon",
                    None,
                )?;
            }
            if let Some(reason) = outcome.invalidate_reservation {
                let (id, job) = current_reservation
                    .take()
                    .context("invalidate without reservation")?;
                db::resolve_reservation(&tx, id, "invalidated", reason, now)?;
                db::append_event(
                    &tx,
                    Some(job),
                    None,
                    "reservation_invalidated",
                    "daemon",
                    Some(&json!({ "reason": reason }).to_string()),
                )?;
            }
            if let Some(new_res) = &outcome.create_reservation {
                // Blockers: the shadow set when protection was created —
                // active leases plus this batch's pre-protection admissions.
                let blockers: Vec<AttemptId> = leases
                    .iter()
                    .map(|(attempt, _)| *attempt)
                    .chain(outcome.starts.iter().filter_map(|start| {
                        (start.bypasses != Some(new_res.job_id))
                            .then(|| batch.get(&start.job_id).copied())
                            .flatten()
                    }))
                    .collect();
                let rid = db::create_reservation(
                    &tx,
                    new_res.job_id,
                    SCHEDULER_SEMANTICS_VERSION,
                    &blockers,
                    now,
                )?;
                for job in &new_res.initial_consumed {
                    let attempt = lease_attempt_by_job
                        .get(job)
                        .or_else(|| batch.get(job))
                        .copied();
                    db::record_backfill(&tx, rid, *job, "initial_active", attempt, now)?;
                }
                db::append_event(
                    &tx,
                    Some(new_res.job_id),
                    None,
                    "reservation_created",
                    "daemon",
                    Some(
                        &json!({
                            "initialBlockers": blockers,
                            "initialConsumed": new_res.initial_consumed,
                        })
                        .to_string(),
                    ),
                )?;
                current_reservation = Some((rid, new_res.job_id));
            }
            for start in &outcome.starts {
                let Some(bypassed) = start.bypasses else {
                    continue;
                };
                // A pass can consume bypasses of the old reservation (before
                // it was satisfied) and of a newly created one; attribute by
                // protected job.
                let old_reservation = reservation.as_ref().map(|res| (res.id, res.job_id));
                let (rid, _) = [current_reservation, old_reservation]
                    .into_iter()
                    .flatten()
                    .find(|(_, job)| *job == bypassed)
                    .context("backfill without matching reservation")?;
                db::record_backfill(
                    &tx,
                    rid,
                    start.job_id,
                    "admitted",
                    batch.get(&start.job_id).copied(),
                    now,
                )?;
                db::append_event(
                    &tx,
                    Some(start.job_id),
                    batch.get(&start.job_id).copied(),
                    "backfill_admitted",
                    "daemon",
                    Some(&json!({ "bypassedProtectedJob": bypassed }).to_string()),
                )?;
            }
            tx.commit()?;
        }

        // Spawning happens strictly after the commit; a crash in between is
        // recovered by the prepared-attempt respawn path.
        for attempt in launched {
            self.launch(attempt);
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Runner launch
    // -----------------------------------------------------------------------

    fn launch(&mut self, attempt_id: AttemptId) {
        self.spawned.insert(attempt_id, Instant::now());
        let result = self.try_launch(attempt_id);
        if let Err(err) = result {
            tracing::error!("launching attempt {attempt_id}: {err:#}");
            if let Ok(Some(attempt)) = db::attempt_row(&self.conn, attempt_id)
                && !attempt.state.is_terminal()
                && let Err(finalize_err) = self.finalize_attempt_and_job(
                    &attempt,
                    AttemptState::Failed,
                    None,
                    None,
                    Some(&format!("runner spawn failed: {err:#}")),
                    "daemon",
                )
            {
                tracing::error!("finalizing failed launch {attempt_id}: {finalize_err:#}");
            }
        }
    }

    fn try_launch(&mut self, attempt_id: AttemptId) -> Result<()> {
        let attempt =
            db::attempt_row(&self.conn, attempt_id)?.context("attempt to launch exists")?;
        let job = db::job_row(&self.conn, attempt.job_id)?.context("job of attempt exists")?;
        let dir = self.paths.attempt_dir(attempt.id);
        if !dir.exists() {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&dir)?;
        }

        match artifacts::read_artifact::<CommandFile>(&dir.join(COMMAND_FILE)) {
            // Crash-recovery respawn of the same attempt: reuse the identical
            // command record.
            Ok(Some(existing)) if existing.token == attempt.launch_token => {}
            Ok(Some(_)) => anyhow::bail!("attempt directory reused with a different token"),
            Err(err) => anyhow::bail!("corrupt command artifact: {err}"),
            Ok(None) => {
                // A fresh attempt directory must not contain protocol
                // artifacts from a previous life.
                for stale in [IDENTITY_FILE, START_FILE, EXEC_FILE, RESULT_FILE] {
                    anyhow::ensure!(
                        !dir.join(stale).exists(),
                        "attempt directory contains stale {stale} without a command record"
                    );
                }
                artifacts::write_artifact(
                    &dir,
                    COMMAND_FILE,
                    &CommandFile {
                        attempt_id: attempt.id,
                        job_id: job.id,
                        token: attempt.launch_token.clone(),
                        argv: job.args.clone(),
                        cwd: job.cwd.clone(),
                        env: job.env.clone(),
                        start_wait_ms: self.config.runner_start_wait_ms,
                        cancel_grace_ms: self.config.cancel_grace_ms,
                        poll_ms: self.config.runner_poll_ms,
                        time_limit_ms: job.time_limit_ms.map(|value| value as u64),
                        window_end_at: attempt.window_end_at,
                    },
                    true,
                )?;
            }
        }

        let runner_log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(RUNNER_LOG_FILE))?;
        let runner_log_err = runner_log.try_clone()?;
        let exe = std::env::current_exe().context("resolving mlqd executable")?;
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("__runner")
            .arg("--attempt-dir")
            .arg(&dir)
            .stdin(Stdio::null())
            .stdout(runner_log)
            .stderr(runner_log_err);
        unsafe {
            // A fresh session detaches the runner from the daemon's lifetime
            // so daemon restart/upgrade never kills workers.
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        // SIGCHLD is SIG_IGN in the daemon, so the runner is auto-reaped;
        // dropping the child handle does not kill it.
        cmd.spawn().context("spawning attempt runner")?;
        db::append_event(
            &self.conn,
            Some(job.id),
            Some(attempt.id),
            "runner_spawned",
            "daemon",
            None,
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Mutations (free functions so they can run inside the coordinator's
// transaction without borrowing the whole coordinator)
// ---------------------------------------------------------------------------

fn publish_time_limit(
    paths: &Paths,
    attempt_id: AttemptId,
    token: &str,
    time_limit_ms: Option<u64>,
    updated_at: i64,
) {
    let dir = paths.attempt_dir(attempt_id);
    if let Err(err) = fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
    {
        tracing::warn!("creating {} for time limit: {err}", dir.display());
    }
    if let Err(err) = artifacts::write_artifact(
        &dir,
        LIMIT_FILE,
        &LimitFile {
            token: token.to_string(),
            time_limit_ms,
            updated_at: Some(updated_at),
        },
        false,
    ) {
        // Reconcile republishes the committed job limit on a later tick.
        tracing::warn!("publishing time limit for attempt {attempt_id}: {err}");
    }
}

/// True when a live runner will already observe `desired`, either from an
/// up-to-date `limit.json` or from the launch snapshot when no override exists.
fn published_time_limit_is_current(
    dir: &std::path::Path,
    token: &str,
    desired: Option<u64>,
    set_at: Option<i64>,
) -> bool {
    match artifacts::read_artifact::<LimitFile>(&dir.join(LIMIT_FILE)) {
        Ok(Some(limit)) => {
            limit.token == token && limit.time_limit_ms == desired && limit.updated_at == set_at
        }
        Ok(None) => match artifacts::read_artifact::<CommandFile>(&dir.join(COMMAND_FILE)) {
            // No override is current only while the launch snapshot is still the policy.
            Ok(Some(command)) if command.token == token => {
                command.time_limit_ms == desired && set_at.is_none()
            }
            // Nothing launched yet, or the command record is not ours to repair.
            _ => true,
        },
        Err(_) => false,
    }
}

fn api_not_found(job: JobId) -> ApiError {
    ApiError::new(error_codes::NOT_FOUND, format!("job {job} not found"))
}

fn invalidate_reservation_if_protected(
    tx: &Transaction<'_>,
    job: JobId,
    reason: &str,
    now: i64,
) -> ApiResult<()> {
    if let Some(res) = db::active_reservation(tx)?
        && res.job_id == job
    {
        db::resolve_reservation(tx, res.id, "invalidated", reason, now)?;
        db::append_event(
            tx,
            Some(job),
            None,
            "reservation_invalidated",
            "client",
            Some(&json!({ "reason": reason }).to_string()),
        )?;
    }
    Ok(())
}

fn job_reply(tx: &Transaction<'_>, paths: &Paths, job: JobId) -> ApiResult<Reply> {
    let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
    Ok(Reply::Job {
        job: views::job_view(tx, paths, &row, false)?,
    })
}

fn class_reply(conn: &Connection, name: &str) -> ApiResult<(Reply, Vec<PostAction>)> {
    Ok((
        Reply::Class {
            class: require_class(conn, name)?,
        },
        Vec::new(),
    ))
}

fn append_class_event(tx: &Transaction<'_>, kind: &str, details: serde_json::Value) -> Result<()> {
    db::append_event(tx, None, None, kind, "client", Some(&details.to_string()))?;
    Ok(())
}

fn validate_class_name(name: &str) -> ApiResult<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    {
        return Err(ApiError::new(
            error_codes::INVALID_ARGUMENT,
            "class name must contain 1-128 ASCII letters, digits, dots, underscores, or hyphens",
        ));
    }
    Ok(())
}

fn require_class(conn: &Connection, name: &str) -> ApiResult<protocol::ClassView> {
    validate_class_name(name)?;
    db::class_row(conn, name)?
        .ok_or_else(|| ApiError::new(error_codes::NOT_FOUND, format!("class {name:?} not found")))
}

fn require_editable_policy(tx: &Transaction<'_>, job: JobId) -> ApiResult<JobRow> {
    let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
    if !matches!(row.state, JobState::Queued | JobState::Held) {
        return Err(ApiError::new(
            error_codes::INVALID_STATE,
            "class membership and run windows can only be changed for queued or held jobs",
        ));
    }
    Ok(row)
}

fn apply_mutation(
    tx: &Transaction<'_>,
    paths: &Paths,
    op: &Op,
    now: i64,
) -> ApiResult<(Reply, Vec<PostAction>)> {
    match op {
        Op::DefaultEnd { force } => {
            let row = db::default_for_control(tx)?.ok_or_else(|| {
                ApiError::new(error_codes::NOT_FOUND, "no default job configured")
            })?;
            let actions = if row.state.is_terminal() {
                Vec::new()
            } else {
                cancel_job(tx, row.id, *force, now)?
            };
            Ok((job_reply(tx, paths, row.id)?, actions))
        }
        Op::DefaultStart => {
            let row = db::default_for_control(tx)?.ok_or_else(|| {
                ApiError::new(error_codes::NOT_FOUND, "no default job configured")
            })?;
            let live = db::live_attempt_for_job(tx, row.id)?;
            if row.state == JobState::NeedsAttention
                || live.as_ref().is_some_and(|attempt| {
                    attempt.cancel_requested
                        || matches!(
                            attempt.state,
                            AttemptState::Orphaned | AttemptState::Quarantined
                        )
                })
            {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    "default is still stopping or needs recovery; wait for it to stop or resolve recovery before starting",
                ));
            }
            if matches!(
                row.state,
                JobState::Queued | JobState::Starting | JobState::Running
            ) {
                return Ok((job_reply(tx, paths, row.id)?, Vec::new()));
            }
            if live.is_some()
                || db::active_leases(tx)?
                    .iter()
                    .any(|(_, lease)| lease.job_id == row.id)
                || row.state == JobState::Skipped
            {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    "default has unresolved work and cannot be started",
                ));
            }
            db::update_job_state(tx, row.id, JobState::Queued, None, now)?;
            db::set_job_retry_not_before(tx, row.id, None, now)?;
            db::clear_job_readiness(tx, row.id, now)?;
            db::advance_dependency_graph(tx, now)?;
            db::append_event(tx, Some(row.id), None, "default_started", "client", None)?;
            Ok((job_reply(tx, paths, row.id)?, Vec::new()))
        }
        Op::ClassCreate { name, run_window } => {
            validate_class_name(name)?;
            if db::class_row(tx, name)?.is_some() {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("class {name:?} already exists"),
                ));
            }
            tx.execute(
                "INSERT INTO job_classes (name, run_window, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?3)",
                rusqlite::params![name, run_window.as_ref().map(ToString::to_string), now],
            )?;
            append_class_event(
                tx,
                "class_created",
                json!({ "class": name, "runWindow": run_window }),
            )?;
            class_reply(tx, name)
        }
        Op::ClassPause { name, paused } => {
            let class = require_class(tx, name)?;
            if class.paused != *paused {
                tx.execute(
                    "UPDATE job_classes SET paused = ?2, updated_at = ?3 WHERE name = ?1",
                    rusqlite::params![name, paused, now],
                )?;
                if *paused {
                    for job in db::class_jobs(tx, name)? {
                        invalidate_reservation_if_protected(
                            tx,
                            job.id,
                            "protected job's class was paused",
                            now,
                        )?;
                    }
                }
                append_class_event(
                    tx,
                    if *paused {
                        "class_paused"
                    } else {
                        "class_unpaused"
                    },
                    json!({ "class": name }),
                )?;
            }
            class_reply(tx, name)
        }
        Op::ClassSetWindow { name, run_window } => {
            let class = require_class(tx, name)?;
            if class.run_window != *run_window {
                let jobs = db::class_jobs(tx, name)?;
                for job in &jobs {
                    if db::live_attempt_for_job(tx, job.id)?.is_some() {
                        return Err(ApiError::new(
                            error_codes::INVALID_STATE,
                            "cannot edit a class window while it has live attempts; pause and drain the class first",
                        ));
                    }
                }
                tx.execute(
                    "UPDATE job_classes SET run_window = ?2, updated_at = ?3 WHERE name = ?1",
                    rusqlite::params![name, run_window.as_ref().map(ToString::to_string), now],
                )?;
                for job in jobs {
                    invalidate_reservation_if_protected(
                        tx,
                        job.id,
                        "protected job's class window changed",
                        now,
                    )?;
                }
                append_class_event(
                    tx,
                    "class_window_changed",
                    json!({ "class": name, "runWindow": run_window }),
                )?;
            }
            class_reply(tx, name)
        }
        Op::ClassCancel { name, force } => {
            let class = require_class(tx, name)?;
            let mut actions = Vec::new();
            // Unsafe members roll back the request before any side effects.
            for job in db::class_jobs(tx, name)? {
                // A parent's cancellation may have skipped this member.
                let current = db::job_row(tx, job.id)?.ok_or_else(|| api_not_found(job.id))?;
                if !current.state.is_terminal() {
                    actions.extend(cancel_job(tx, job.id, *force, now)?);
                }
            }
            append_class_event(
                tx,
                "class_cancel_requested",
                json!({ "class": name, "force": force }),
            )?;
            Ok((Reply::Class { class }, actions))
        }
        Op::SetClass { job, class } => {
            let row = require_editable_policy(tx, *job)?;
            if let Some(name) = class {
                require_class(tx, name)?;
            }
            if row.class != *class {
                invalidate_reservation_if_protected(
                    tx,
                    *job,
                    "protected job's class changed",
                    now,
                )?;
                tx.execute(
                    "UPDATE jobs SET class = ?2, updated_at = ?3 WHERE id = ?1",
                    rusqlite::params![job, class, now],
                )?;
                db::append_event(
                    tx,
                    Some(*job),
                    None,
                    "job_class_changed",
                    "client",
                    Some(&json!({ "class": class }).to_string()),
                )?;
            }
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::SetRunWindow { job, run_window } => {
            let row = require_editable_policy(tx, *job)?;
            if row.run_window != *run_window {
                invalidate_reservation_if_protected(
                    tx,
                    *job,
                    "protected job's run window changed",
                    now,
                )?;
                tx.execute(
                    "UPDATE jobs SET run_window = ?2, updated_at = ?3 WHERE id = ?1",
                    rusqlite::params![job, run_window.as_ref().map(ToString::to_string), now],
                )?;
                db::append_event(
                    tx,
                    Some(*job),
                    None,
                    "job_window_changed",
                    "client",
                    Some(&json!({ "runWindow": run_window }).to_string()),
                )?;
            }
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Submit(params) => {
            validate_default_submission(tx, params)?;
            if let Some(name) = &params.class {
                require_class(tx, name)?;
            }
            validate_submission_parents(tx, params)?;
            let job = db::insert_job(tx, params, now)?;
            db::append_event(
                tx,
                Some(job),
                None,
                "job_submitted",
                "client",
                Some(
                    &json!({
                        "name": params.name,
                        "maxParallelRuns": params.max_parallel_runs,
                        "priority": params.priority,
                        "timeLimitMs": params.time_limit_ms,
                        "class": params.class,
                        "runWindow": params.run_window,
                        "isDefault": params.is_default,
                    })
                    .to_string(),
                ),
            )?;
            db::advance_dependency_graph(tx, now)?;
            let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
            Ok((
                Reply::Submitted {
                    job: views::job_view(tx, paths, &row, false)?,
                },
                Vec::new(),
            ))
        }
        Op::Cancel { job, force } => {
            let actions = cancel_job(tx, *job, *force, now)?;
            Ok((job_reply(tx, paths, *job)?, actions))
        }
        Op::Hold { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.state != JobState::Queued {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "only queued jobs can be held; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            invalidate_reservation_if_protected(tx, row.id, "protected job was held", now)?;
            db::update_job_state(tx, row.id, JobState::Held, None, now)?;
            db::append_event(tx, Some(row.id), None, "job_held", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Release { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.state != JobState::Held {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "only held jobs can be released; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            db::update_job_state(tx, row.id, JobState::Queued, None, now)?;
            db::append_event(tx, Some(row.id), None, "job_released", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Retry { job } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.is_default && db::default_job(tx)?.is_some_and(|default| default.id != row.id) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    "another default job is already configured",
                ));
            }
            if !row.state.is_retryable() {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "only failed or lost jobs can be retried; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            db::update_job_state(tx, row.id, JobState::Queued, None, now)?;
            db::set_job_retry_not_before(tx, row.id, None, now)?;
            db::clear_job_readiness(tx, row.id, now)?;
            db::advance_dependency_graph(tx, now)?;
            db::append_event(tx, Some(row.id), None, "job_retried", "client", None)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::SetMaxParallelRuns {
            job,
            max_parallel_runs,
        } => {
            if *max_parallel_runs == 0 {
                return Err(ApiError::new(
                    error_codes::INVALID_ARGUMENT,
                    "maxParallelRuns must be a positive integer",
                ));
            }
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if row.is_default && *max_parallel_runs != 1 {
                return Err(ApiError::new(
                    error_codes::INVALID_ARGUMENT,
                    "default jobs must have maxParallelRuns=1",
                ));
            }
            if !matches!(
                row.state,
                JobState::Queued
                    | JobState::Held
                    | JobState::Starting
                    | JobState::Running
                    | JobState::NeedsAttention
            ) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "maxParallelRuns cannot be changed after a job finishes; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            // In particular, do not invalidate and recreate a protected
            // job's reservation for an exact-value no-op.
            if row.max_parallel_runs == *max_parallel_runs {
                return Ok((job_reply(tx, paths, *job)?, Vec::new()));
            }
            let active_attempt = match row.state {
                JobState::Queued | JobState::Held => {
                    invalidate_reservation_if_protected(
                        tx,
                        row.id,
                        "protected job concurrency limit was changed",
                        now,
                    )?;
                    None
                }
                JobState::Starting | JobState::Running | JobState::NeedsAttention => {
                    let attempt = db::live_attempt_for_job(tx, row.id)?.ok_or_else(|| {
                        ApiError::new(
                            error_codes::INVALID_STATE,
                            format!("job {} has no live attempt to update", row.id),
                        )
                    })?;
                    let active_leases = db::active_leases(tx)?;
                    let active_count = active_leases.len() as u64;
                    // Lowering below the current active count is allowed: we
                    // do not preempt healthy work, so excess leases keep
                    // running and drain naturally. Admission (`can_admit`)
                    // refuses new starts until the set is compatible again.
                    if !active_leases.iter().any(|(id, _)| *id == attempt.id) {
                        return Err(ApiError::new(
                            error_codes::INVALID_STATE,
                            format!("job {} has no active run lease to update", row.id),
                        ));
                    }
                    db::set_active_attempt_max_parallel_runs(tx, attempt.id, *max_parallel_runs)?;
                    Some((attempt.id, active_count))
                }
                _ => unreachable!("state eligibility checked above"),
            };
            db::set_job_max_parallel_runs(tx, row.id, *max_parallel_runs, now)?;
            db::append_event(
                tx,
                Some(row.id),
                active_attempt.map(|(attempt, _)| attempt),
                "max_parallel_runs_changed",
                "client",
                Some(
                    &json!({
                        "from": row.max_parallel_runs,
                        "to": max_parallel_runs,
                        "activeLeases": active_attempt.map(|(_, count)| count),
                    })
                    .to_string(),
                ),
            )?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::SetPriority { job, priority } => {
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            // Priority only affects scheduling order among non-running work.
            // Live jobs keep their lease; changing their priority would imply
            // preemption semantics we deliberately do not implement.
            if !matches!(row.state, JobState::Queued | JobState::Held) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "priority can only be changed for queued or held jobs; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            if row.priority == *priority {
                return Ok((job_reply(tx, paths, *job)?, Vec::new()));
            }
            if row.state == JobState::Queued {
                invalidate_reservation_if_protected(
                    tx,
                    row.id,
                    "protected job priority was changed",
                    now,
                )?;
            }
            db::set_job_priority(tx, row.id, *priority, now)?;
            db::append_event(
                tx,
                Some(row.id),
                None,
                "priority_changed",
                "client",
                Some(
                    &json!({
                        "from": row.priority,
                        "to": priority,
                    })
                    .to_string(),
                ),
            )?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::SetTimeLimit { job, time_limit_ms } => {
            let stored = validate_time_limit(*time_limit_ms)?;
            let row = db::job_row(tx, *job)?.ok_or_else(|| api_not_found(*job))?;
            if !matches!(
                row.state,
                JobState::Queued
                    | JobState::Held
                    | JobState::Starting
                    | JobState::Running
                    | JobState::NeedsAttention
            ) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "time limit cannot be changed after a job finishes; job {} is {}",
                        row.id, row.state
                    ),
                ));
            }
            let current = row
                .time_limit_ms
                .and_then(|value| u64::try_from(value).ok());
            if current == *time_limit_ms {
                return Ok((job_reply(tx, paths, *job)?, Vec::new()));
            }
            let publish = match row.state {
                JobState::Queued | JobState::Held => None,
                JobState::Starting | JobState::Running | JobState::NeedsAttention => {
                    let attempt = db::live_attempt_for_job(tx, row.id)?.ok_or_else(|| {
                        ApiError::new(
                            error_codes::INVALID_STATE,
                            format!("job {} has no live attempt to update", row.id),
                        )
                    })?;
                    Some((attempt.id, attempt.launch_token))
                }
                _ => unreachable!("state eligibility checked above"),
            };
            db::set_job_time_limit(tx, row.id, stored, now)?;
            db::append_event(
                tx,
                Some(row.id),
                publish.as_ref().map(|(attempt, _)| *attempt),
                "time_limit_changed",
                "client",
                Some(
                    &json!({
                        "from": row.time_limit_ms,
                        "to": stored,
                    })
                    .to_string(),
                ),
            )?;
            let actions = publish
                .map(|(attempt_id, token)| PostAction::PublishTimeLimit {
                    attempt_id,
                    token,
                    time_limit_ms: *time_limit_ms,
                    updated_at: now,
                })
                .into_iter()
                .collect();
            Ok((job_reply(tx, paths, *job)?, actions))
        }
        Op::RecoverResolve {
            job,
            attempt,
            resolve_as,
        } => {
            let attempt_row = db::attempt_by_number(tx, *job, *attempt)?.ok_or_else(|| {
                ApiError::new(
                    error_codes::NOT_FOUND,
                    format!("job {job} has no attempt number {attempt}"),
                )
            })?;
            if !matches!(
                attempt_row.state,
                AttemptState::Orphaned | AttemptState::Quarantined
            ) {
                return Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!(
                        "attempt {} is {}, not awaiting recovery",
                        attempt_row.id, attempt_row.state
                    ),
                ));
            }
            // Refuse to release the lease while known containment is live.
            if let (Some(pgid), Some(boot)) =
                (attempt_row.cmd_pgid, attempt_row.runner_boot_id.as_deref())
                && identity::group_possibly_alive(pgid, boot, attempt_row.cmd_start_time)
            {
                return Err(ApiError::new(
                    error_codes::UNSAFE_RESOLUTION,
                    format!(
                        "command process group {pgid} still has live members {:?}; \
                         resolve after they exit or are killed",
                        identity::group_members(pgid)
                    ),
                ));
            }
            let (attempt_state, job_state) = match resolve_as {
                ResolveAs::Lost => (AttemptState::Lost, JobState::Lost),
                ResolveAs::Cancelled => (AttemptState::Cancelled, JobState::Cancelled),
            };
            // With no exec handshake the command's pgid was never learned, so
            // the safety gate above could not check anything: the release is
            // on the operator's judgement, and the record says so.
            let message = if attempt_row.cmd_pgid.is_none() {
                "resolved by operator via recover resolve (containment unknown: \
                 no exec handshake was recorded; verify no stray processes remain)"
            } else {
                "resolved by operator via recover resolve"
            };
            db::finalize_attempt(
                tx,
                attempt_row.id,
                attempt_state,
                None,
                None,
                Some(message),
                now,
            )?;
            db::release_lease(tx, attempt_row.id, now)?;
            db::update_job_state(tx, *job, job_state, Some(message), now)?;
            db::append_event(
                tx,
                Some(*job),
                Some(attempt_row.id),
                "recover_resolved",
                "client",
                Some(&json!({ "as": attempt_state.as_str() }).to_string()),
            )?;
            db::append_event(
                tx,
                Some(*job),
                None,
                match job_state {
                    JobState::Lost => "job_lost",
                    JobState::Cancelled => "job_cancelled",
                    _ => unreachable!("recovery resolves only to a terminal outcome"),
                },
                "client",
                None,
            )?;
            db::advance_dependency_graph(tx, now)?;
            Ok((job_reply(tx, paths, *job)?, Vec::new()))
        }
        Op::Pause | Op::Unpause => {
            let paused = matches!(op, Op::Pause);
            if db::set_paused(tx, paused, now)? {
                db::append_event(
                    tx,
                    None,
                    None,
                    if paused {
                        "queue_paused"
                    } else {
                        "queue_unpaused"
                    },
                    "client",
                    None,
                )?;
            }
            Ok((Reply::Paused { paused }, Vec::new()))
        }
        _ => Err(ApiError::new(
            error_codes::INTERNAL,
            "read op routed to mutation handler",
        )),
    }
}

fn should_restart_default(
    job: &JobRow,
    attempt: &AttemptRow,
    state: AttemptState,
    boot_id: &str,
    failures: i64,
) -> bool {
    if !job.is_default || job.state.is_terminal() || job.state == JobState::Held {
        return false;
    }
    attempt.yield_requested
        || (!attempt.cancel_requested
            && (state == AttemptState::Succeeded
                || (state == AttemptState::Lost
                    && failures < job.max_attempts
                    && attempt
                        .runner_boot_id
                        .as_deref()
                        .is_some_and(|boot| boot != boot_id))))
}

fn requeue_default(tx: &Transaction<'_>, job: &JobRow, now: i64, reason: &str) -> Result<()> {
    db::update_job_state(tx, job.id, JobState::Queued, Some(reason), now)?;
    // Delay short-lived commands to avoid a restart loop.
    db::set_job_retry_not_before(
        tx,
        job.id,
        Some(now.saturating_add(job.retry_delay_ms.max(1_000))),
        now,
    )?;
    db::clear_job_readiness(tx, job.id, now)?;
    db::append_event(tx, Some(job.id), None, "default_requeued", "daemon", None)?;
    Ok(())
}

fn request_default_yield(
    tx: &Transaction<'_>,
    job: &JobRow,
    attempt: &AttemptRow,
    now: i64,
) -> Result<Vec<PostAction>> {
    anyhow::ensure!(
        job.is_default,
        "only the default job may yield automatically"
    );
    // Preserve user cancellation and leave quarantined attempts for recovery.
    if attempt.yield_requested
        || attempt.cancel_requested
        || attempt.state == AttemptState::Quarantined
    {
        return Ok(Vec::new());
    }
    tx.execute(
        "UPDATE attempts SET yield_requested = 1 WHERE id = ?1",
        [attempt.id],
    )?;
    db::set_attempt_cancel_requested(tx, attempt.id, true, now)?;
    db::append_event(
        tx,
        Some(job.id),
        Some(attempt.id),
        "default_yield_requested",
        "daemon",
        None,
    )?;
    if attempt.state == AttemptState::Prepared {
        // An unauthorized command cannot run, so its lease can be released.
        db::finalize_attempt(
            tx,
            attempt.id,
            AttemptState::Cancelled,
            None,
            None,
            Some("default yielded before launch"),
            now,
        )?;
        db::release_lease(tx, attempt.id, now)?;
        db::append_event(
            tx,
            Some(job.id),
            Some(attempt.id),
            "attempt_cancelled",
            "daemon",
            Some("{\"reason\":\"default yielded before launch\"}"),
        )?;
        requeue_default(tx, job, now, "default yielded to ordinary work")?;
    }
    Ok(vec![PostAction::PublishCancel {
        attempt_id: attempt.id,
        token: attempt.launch_token.clone(),
        force: true,
        requested_at: now,
    }])
}

fn validate_default_submission(tx: &Transaction<'_>, params: &SubmitParams) -> ApiResult<()> {
    if !params.is_default {
        return Ok(());
    }
    if params.max_parallel_runs != 1 {
        return Err(ApiError::new(
            error_codes::INVALID_ARGUMENT,
            "default jobs must have maxParallelRuns=1",
        ));
    }
    if !params.dependencies.is_empty() {
        return Err(ApiError::new(
            error_codes::INVALID_ARGUMENT,
            "default jobs cannot have prerequisites",
        ));
    }
    if db::default_job(tx)?.is_some() {
        return Err(ApiError::new(
            error_codes::INVALID_STATE,
            "a default job is already configured; cancel it before submitting a replacement",
        ));
    }
    Ok(())
}

fn cancel_job(
    tx: &Transaction<'_>,
    job: JobId,
    force: bool,
    now: i64,
) -> ApiResult<Vec<PostAction>> {
    let row = db::job_row(tx, job)?.ok_or_else(|| api_not_found(job))?;
    match row.state {
        JobState::Queued | JobState::Held => {
            invalidate_reservation_if_protected(tx, row.id, "protected job was cancelled", now)?;
            db::update_job_state(
                tx,
                row.id,
                JobState::Cancelled,
                Some("cancelled before start"),
                now,
            )?;
            db::append_event(tx, Some(row.id), None, "job_cancelled", "client", None)?;
            db::advance_dependency_graph(tx, now)?;
            Ok(Vec::new())
        }
        JobState::Starting | JobState::Running | JobState::NeedsAttention => {
            let attempt = db::live_attempt_for_job(tx, row.id)?.ok_or_else(|| {
                ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("job {} has no live attempt to cancel", row.id),
                )
            })?;
            // User cancellation disables restart after a scheduler yield.
            if attempt.yield_requested {
                tx.execute(
                    "UPDATE attempts SET yield_requested = 0 WHERE id = ?1",
                    [attempt.id],
                )?;
            }
            let requested_at = attempt.cancel_requested_at.unwrap_or(now);
            let publish_force = attempt.cancel_force || force;
            db::set_attempt_cancel_requested(tx, attempt.id, force, now)?;
            db::append_event(
                tx,
                Some(row.id),
                Some(attempt.id),
                "cancel_requested",
                "client",
                Some(&json!({ "force": force }).to_string()),
            )?;
            match attempt.state {
                // Never authorized: `start` will never be published, so the
                // command can never run. Finalize immediately and release the
                // lease; the waiting runner exits on cancel.json.
                AttemptState::Prepared => {
                    db::finalize_attempt(
                        tx,
                        attempt.id,
                        AttemptState::Cancelled,
                        None,
                        None,
                        Some("cancelled before launch authorization"),
                        now,
                    )?;
                    db::release_lease(tx, attempt.id, now)?;
                    db::update_job_state(
                        tx,
                        row.id,
                        JobState::Cancelled,
                        Some("cancelled before launch"),
                        now,
                    )?;
                    db::append_event(tx, Some(row.id), None, "job_cancelled", "client", None)?;
                    db::advance_dependency_graph(tx, now)?;
                    Ok(vec![PostAction::PublishCancel {
                        attempt_id: attempt.id,
                        token: attempt.launch_token.clone(),
                        force: publish_force,
                        requested_at,
                    }])
                }
                AttemptState::Authorized | AttemptState::Running => {
                    Ok(vec![PostAction::PublishCancel {
                        attempt_id: attempt.id,
                        token: attempt.launch_token.clone(),
                        force: publish_force,
                        requested_at,
                    }])
                }
                // No runner remains; the daemon delivers the signal itself
                // and reconcile finalizes once the group drains.
                AttemptState::Orphaned => Ok(attempt
                    .cmd_pgid
                    .map(|pgid| PostAction::SignalGroup {
                        pgid,
                        signal: libc::SIGTERM,
                    })
                    .into_iter()
                    .collect()),
                AttemptState::Quarantined => Err(ApiError::new(
                    error_codes::UNSAFE_RESOLUTION,
                    format!(
                        "attempt {} is quarantined with unknown process containment; \
                         inspect and use `mlq recover resolve`",
                        attempt.id
                    ),
                )),
                _ => Err(ApiError::new(
                    error_codes::INVALID_STATE,
                    format!("attempt {} is already {}", attempt.id, attempt.state),
                )),
            }
        }
        state if state.is_terminal() => Err(ApiError::new(
            error_codes::INVALID_STATE,
            format!("job {} is already {state}", row.id),
        )),
        state => Err(ApiError::new(
            error_codes::INVALID_STATE,
            format!("job {} is {state} and cannot be cancelled", row.id),
        )),
    }
}

fn validate_time_limit(limit: Option<u64>) -> ApiResult<Option<i64>> {
    match limit {
        None => Ok(None),
        Some(0) => Err(ApiError::new(
            error_codes::INVALID_ARGUMENT,
            "timeLimitMs must be a positive integer",
        )),
        Some(limit) => i64::try_from(limit)
            .map(Some)
            .map_err(|_| ApiError::new(error_codes::INVALID_ARGUMENT, "timeLimitMs is too large")),
    }
}

/// Argument and filesystem validation with no database dependency; runs on
/// the coordinator before the write transaction opens.
fn validate_submission_fs(params: &SubmitParams) -> ApiResult<()> {
    let invalid = |message: String| ApiError::new(error_codes::INVALID_ARGUMENT, message);
    if params.name.is_empty() || params.name.contains('\0') {
        return Err(invalid(
            "job name must be non-empty UTF-8 without NUL".into(),
        ));
    }
    if params.max_parallel_runs == 0 {
        return Err(invalid("maxParallelRuns must be a positive integer".into()));
    }
    if params.max_attempts == 0 {
        return Err(invalid("maxAttempts must be a positive integer".into()));
    }
    if params.time_limit_ms == Some(0) {
        return Err(invalid("timeLimitMs must be a positive integer".into()));
    }
    if params
        .time_limit_ms
        .is_some_and(|limit| i64::try_from(limit).is_err())
    {
        return Err(invalid("timeLimitMs is too large".into()));
    }
    if params.args.is_empty() {
        return Err(invalid("a command argument vector is required".into()));
    }
    for arg in &params.args {
        if arg.contains('\0') {
            return Err(invalid("command arguments must not contain NUL".into()));
        }
    }
    for (key, value) in &params.env {
        if key.is_empty() || key.contains('\0') || key.contains('=') || value.contains('\0') {
            return Err(invalid(format!("invalid environment entry {key:?}")));
        }
    }
    let cwd = std::path::Path::new(&params.cwd);
    if !cwd.is_absolute() || params.cwd.contains('\0') {
        return Err(invalid("working directory must be an absolute path".into()));
    }
    // Fast feedback only; the launch path revalidates because queued
    // filesystem state can change.
    if !cwd.is_dir() {
        return Err(invalid(format!(
            "working directory {} is not a directory",
            params.cwd
        )));
    }
    let executable = &params.args[0];
    if executable.contains('/') {
        // A relative path launches relative to the job's cwd (the runner
        // chdirs before exec), so it must be validated there — never against
        // the daemon's own working directory.
        let resolved = cwd.join(executable);
        if !resolved.is_file() {
            return Err(invalid(format!(
                "executable {} does not exist",
                resolved.display()
            )));
        }
    } else {
        let path = params.env.get("PATH").map(String::as_str).unwrap_or("");
        let found = std::env::split_paths(path)
            .any(|dir| !dir.as_os_str().is_empty() && dir.join(executable).is_file());
        if !found {
            return Err(invalid(format!(
                "executable {executable} not found on the persisted PATH"
            )));
        }
    }
    Ok(())
}

/// Parents are necessarily older jobs because edges are attached atomically
/// to a brand-new child. That makes cycles unrepresentable without a graph
/// traversal while still rejecting typos before insertion.
fn validate_submission_parents(tx: &Transaction<'_>, params: &SubmitParams) -> ApiResult<()> {
    for parent in params.dependencies.iter().map(|dependency| dependency.job) {
        let row = db::job_row(tx, parent)?;
        if parent <= 0 || row.is_none() {
            return Err(ApiError::new(
                error_codes::NOT_FOUND,
                format!("dependency parent job {parent} does not exist"),
            ));
        }
        if row.is_some_and(|row| row.is_default) {
            return Err(ApiError::new(
                error_codes::INVALID_ARGUMENT,
                "a recurring default job cannot be a prerequisite",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn policy_paths() -> Paths {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/policy-tests");
        Paths {
            runtime_dir: root.join("runtime"),
            state_dir: root.join("state"),
            config_file: root.join("config.toml"),
        }
    }

    fn policy_op(conn: &mut Connection, op: Op) -> ApiResult<Reply> {
        let tx = conn.transaction()?;
        let (reply, _) = apply_mutation(&tx, &policy_paths(), &op, 0)?;
        tx.commit()?;
        Ok(reply)
    }

    fn test_params(name: &str) -> SubmitParams {
        SubmitParams {
            name: name.into(),
            is_default: false,
            class: None,
            run_window: None,
            cwd: env!("CARGO_MANIFEST_DIR").into(),
            args: vec!["never-executed".into()],
            env: BTreeMap::new(),
            max_parallel_runs: 1,
            priority: 0,
            max_attempts: 1,
            retry_delay_ms: 0,
            time_limit_ms: None,
            dependencies: Vec::new(),
        }
    }

    fn submit_job(conn: &mut Connection, params: SubmitParams) -> JobId {
        let Reply::Submitted { job } = policy_op(conn, Op::Submit(params)).unwrap() else {
            panic!("expected submitted")
        };
        job.id
    }

    fn policy_job(conn: &mut Connection, name: &str, parent: Option<JobId>) -> JobId {
        let params = SubmitParams {
            class: Some("ablations".into()),
            max_parallel_runs: 4,
            dependencies: parent
                .into_iter()
                .map(|job| protocol::DependencySpec {
                    job,
                    condition: domain::DependencyCondition::Succeeded,
                })
                .collect(),
            ..test_params(name)
        };
        submit_job(conn, params)
    }

    fn idle_params() -> SubmitParams {
        SubmitParams {
            is_default: true,
            ..test_params("idle")
        }
    }

    fn idle_job(conn: &mut Connection) -> JobId {
        submit_job(conn, idle_params())
    }

    // Simulate runner results in memory without process checks or a daemon loop.
    fn bookkeeping_coordinator(conn: Connection) -> Coordinator {
        let (revisions, _) = watch::channel(0);
        Coordinator {
            conn,
            config: Config::default(),
            paths: policy_paths(),
            started_at: 0,
            boot_id: "test-boot".into(),
            admission_blocked: None,
            spawned: HashMap::new(),
            orphan_term_sent: HashSet::new(),
            revisions,
            published_revision: 0,
            published_admission_blocked: false,
        }
    }

    #[test]
    fn default_controls_require_a_saved_default_and_requeue_terminal_or_held_jobs() {
        let mut conn = db::open_in_memory().unwrap();
        for op in [Op::DefaultStart, Op::DefaultEnd { force: false }] {
            assert_eq!(
                policy_op(&mut conn, op).unwrap_err().code,
                error_codes::NOT_FOUND
            );
        }
        let job = idle_job(&mut conn);
        for state in [
            JobState::Cancelled,
            JobState::Failed,
            JobState::Lost,
            JobState::Succeeded,
            JobState::Held,
        ] {
            db::update_job_state(&conn, job, state, Some("previous outcome"), 10).unwrap();
            db::set_job_retry_not_before(&conn, job, Some(5000), 10).unwrap();
            let Reply::Job { job: started } = policy_op(&mut conn, Op::DefaultStart).unwrap()
            else {
                panic!("expected job")
            };
            assert_eq!(started.id, job);
            assert_eq!(started.state, "queued");
            assert!(started.finished_at.is_none());
            assert!(started.retry_not_before.is_none());
            assert!(started.state_reason.is_none());
        }
        policy_op(&mut conn, Op::DefaultEnd { force: false }).unwrap();
        policy_op(&mut conn, Op::DefaultEnd { force: true }).unwrap();
        assert_eq!(
            db::job_row(&conn, job).unwrap().unwrap().state,
            JobState::Cancelled
        );

        // Ordinary retry keeps its existing restriction on cancelled jobs.
        assert_eq!(
            policy_op(&mut conn, Op::Retry { job }).unwrap_err().code,
            error_codes::INVALID_STATE
        );
    }

    #[test]
    fn default_start_cannot_bypass_pending_cancellation_or_recovery() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "idle-run", 1).unwrap();
        db::set_attempt_authorized(&conn, attempt.id, 2).unwrap();
        policy_op(&mut conn, Op::DefaultStart).unwrap();
        assert_eq!(db::job_row(&conn, job).unwrap().unwrap().attempt_count, 1);
        policy_op(&mut conn, Op::DefaultEnd { force: true }).unwrap();
        assert_eq!(
            policy_op(&mut conn, Op::DefaultStart).unwrap_err().code,
            error_codes::INVALID_STATE
        );
        assert_eq!(db::active_leases(&conn).unwrap().len(), 1);
        let cancel = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
        assert!(cancel.cancel_requested && cancel.cancel_force);
        conn.execute(
            "UPDATE attempts SET cancel_requested=0, state='quarantined' WHERE id=?1",
            [attempt.id],
        )
        .unwrap();
        db::update_job_state(&conn, job, JobState::NeedsAttention, None, 3).unwrap();
        assert_eq!(
            policy_op(&mut conn, Op::DefaultStart).unwrap_err().code,
            error_codes::INVALID_STATE
        );
        assert_eq!(db::active_leases(&conn).unwrap().len(), 1);
    }

    #[test]
    fn default_controls_select_active_then_latest_and_replay_idempotently() {
        let mut conn = db::open_in_memory().unwrap();
        let old = idle_job(&mut conn);
        policy_op(&mut conn, Op::DefaultEnd { force: false }).unwrap();
        let latest = idle_job(&mut conn);
        policy_op(&mut conn, Op::DefaultEnd { force: false }).unwrap();
        assert_eq!(db::default_for_control(&conn).unwrap().unwrap().id, latest);
        db::update_job_state(&conn, old, JobState::Queued, None, 1).unwrap();
        assert_eq!(db::default_for_control(&conn).unwrap().unwrap().id, old);
        policy_op(&mut conn, Op::DefaultEnd { force: false }).unwrap();
        let mut coordinator = bookkeeping_coordinator(conn);
        coordinator
            .handle_mutation(&Op::Pause, "pause-tests")
            .unwrap();
        coordinator
            .handle_mutation(&Op::DefaultStart, "start-once")
            .unwrap();
        let op = Op::DefaultEnd { force: false };
        coordinator.handle_mutation(&op, "end-once").unwrap();
        coordinator
            .handle_mutation(&Op::DefaultStart, "start-again")
            .unwrap();
        coordinator.handle_mutation(&op, "end-once").unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, latest)
                .unwrap()
                .unwrap()
                .state,
            JobState::Queued
        );
    }

    #[test]
    fn default_lookup_keeps_terminal_history_and_prefers_an_active_default() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        policy_op(&mut conn, Op::Cancel { job, force: false }).unwrap();
        for n in 0..30 {
            let now = now_ms() + n + 1;
            let ordinary = db::insert_job(&conn, &test_params("ordinary"), now).unwrap();
            db::update_job_state(&conn, ordinary, JobState::Succeeded, None, now).unwrap();
        }
        assert!(
            db::status_jobs(&conn, 25)
                .unwrap()
                .iter()
                .all(|row| row.id != job)
        );
        let found = db::default_for_command(&conn, "never-executed")
            .unwrap()
            .unwrap();
        assert_eq!((found.id, found.state), (job, JobState::Cancelled));
        assert!(db::default_for_command(&conn, "missing").unwrap().is_none());
        let mut other = idle_params();
        other.args = vec!["other-idle-command".into()];
        let active = submit_job(&mut conn, other);
        assert_eq!(
            db::default_for_command(&conn, "never-executed")
                .unwrap()
                .unwrap()
                .id,
            active
        );
    }

    #[test]
    fn reboot_recovery_preserves_cancellation_timeouts_and_quarantine() {
        for (boot, state, cancel, yielded, timeout, expected) in [
            (
                Some("old-boot"),
                AttemptState::Running,
                false,
                false,
                false,
                Some(JobState::Queued),
            ),
            (
                Some("old-boot"),
                AttemptState::Authorized,
                false,
                false,
                false,
                Some(JobState::Queued),
            ),
            (
                Some("old-boot"),
                AttemptState::Orphaned,
                false,
                false,
                false,
                Some(JobState::Queued),
            ),
            (
                Some("old-boot"),
                AttemptState::Running,
                true,
                false,
                false,
                Some(JobState::Cancelled),
            ),
            (
                Some("old-boot"),
                AttemptState::Running,
                true,
                true,
                false,
                Some(JobState::Queued),
            ),
            (
                Some("old-boot"),
                AttemptState::Running,
                false,
                false,
                true,
                Some(JobState::Failed),
            ),
            (
                Some("old-boot"),
                AttemptState::Quarantined,
                false,
                false,
                false,
                None,
            ),
            (
                Some("test-boot"),
                AttemptState::Running,
                false,
                false,
                false,
                None,
            ),
            (None, AttemptState::Running, false, false, false, None),
        ] {
            let mut conn = db::open_in_memory().unwrap();
            let job = idle_job(&mut conn);
            let row = db::job_row(&conn, job).unwrap().unwrap();
            let attempt = db::create_attempt_with_lease(&conn, &row, "never-launched", 0).unwrap();
            conn.execute(
                "UPDATE attempts SET state=?2, runner_boot_id=?3, cancel_requested=?4,
                 yield_requested=?5, timed_out_at=?6 WHERE id=?1",
                rusqlite::params![
                    attempt.id,
                    state.as_str(),
                    boot,
                    cancel,
                    yielded,
                    timeout.then_some(1)
                ],
            )
            .unwrap();
            let attempt = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
            let mut coordinator = bookkeeping_coordinator(conn);
            let dir = coordinator.paths.attempt_dir(attempt.id);
            assert_eq!(
                coordinator
                    .recover_rebooted_default(&attempt, &dir)
                    .unwrap(),
                expected.is_some()
            );
            if let Some(expected) = expected {
                assert_eq!(
                    db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
                    expected
                );
                assert!(db::active_leases(&coordinator.conn).unwrap().is_empty());
            } else {
                assert_eq!(db::active_leases(&coordinator.conn).unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn reboot_does_not_revive_held_jobs_or_exhausted_failure_budgets() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        policy_op(&mut conn, Op::Hold { job }).unwrap();
        let mut coordinator = bookkeeping_coordinator(conn);
        coordinator.reconcile_all().unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
            JobState::Held
        );

        policy_op(&mut coordinator.conn, Op::Release { job }).unwrap();
        let row = db::job_row(&coordinator.conn, job).unwrap().unwrap();
        let failed = db::create_attempt_with_lease(&coordinator.conn, &row, "failure", 0).unwrap();
        db::finalize_attempt(
            &coordinator.conn,
            failed.id,
            AttemptState::Failed,
            Some(1),
            None,
            None,
            1,
        )
        .unwrap();
        db::release_lease(&coordinator.conn, failed.id, 1).unwrap();
        let row = db::job_row(&coordinator.conn, job).unwrap().unwrap();
        let lost = db::create_attempt_with_lease(&coordinator.conn, &row, "reboot", 2).unwrap();
        coordinator
            .conn
            .execute(
                "UPDATE attempts SET runner_boot_id='old-boot' WHERE id=?1",
                [lost.id],
            )
            .unwrap();
        let lost = db::attempt_row(&coordinator.conn, lost.id)
            .unwrap()
            .unwrap();
        coordinator
            .finalize_attempt_and_job(&lost, AttemptState::Lost, None, None, None, "daemon")
            .unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
            JobState::Lost
        );
    }

    #[test]
    fn recurring_history_is_paged_and_mutation_acknowledgements_stay_small() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        conn.execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<6000)
             INSERT INTO attempts(job_id,number,state,launch_token,max_parallel_runs,created_at)
             SELECT ?1,x,'succeeded','history-'||x,1,x FROM n",
            [job],
        )
        .unwrap();
        conn.execute("UPDATE jobs SET attempt_count=6000 WHERE id=?1", [job])
            .unwrap();
        let mut coordinator = bookkeeping_coordinator(conn);
        let shown = coordinator.handle_read(&Op::Show { job }).unwrap();
        assert!(
            serde_json::to_vec(&shown).unwrap().len() < protocol::DEFAULT_MAX_FRAME_BYTES as usize
        );
        let Reply::Job { job: shown } = shown else {
            panic!("expected job")
        };
        assert_eq!(shown.attempts.len(), views::JOB_ATTEMPT_LIMIT as usize);
        assert!(shown.attempts_truncated);
        assert_eq!(shown.attempts.last().unwrap().number, 6000);
        let Reply::Attempts {
            attempts,
            next_before,
        } = coordinator
            .handle_read(&Op::Attempts {
                job,
                before: None,
                limit: 100,
            })
            .unwrap()
        else {
            panic!("expected attempts")
        };
        assert_eq!(attempts.len(), 100);
        assert_eq!(next_before, Some(5901));
        let Reply::Attempts { attempts, .. } = coordinator
            .handle_read(&Op::Attempts {
                job,
                before: next_before,
                limit: 100,
            })
            .unwrap()
        else {
            panic!("expected attempts")
        };
        assert_eq!(attempts.first().unwrap().number, 5900);
        let reply = coordinator
            .handle_mutation(&Op::Cancel { job, force: false }, "cancel-history")
            .unwrap();
        let Reply::Job { job: reply } = reply else {
            panic!("expected job")
        };
        assert!(reply.attempts.is_empty());
        let (_, stored) = db::lookup_operation(&coordinator.conn, "cancel-history")
            .unwrap()
            .unwrap();
        assert!(stored.len() < protocol::DEFAULT_MAX_FRAME_BYTES as usize);
        assert!(matches!(
            serde_json::from_str::<Reply>(&stored).unwrap(),
            Reply::Job { .. }
        ));
        let mut old_reply = reply;
        old_reply.attempts = vec![shown.attempts[0].clone(); 6000];
        let encoded = serde_json::to_string(&Reply::Job { job: old_reply }).unwrap();
        assert!(encoded.len() > protocol::DEFAULT_MAX_FRAME_BYTES as usize);
        let op = Op::Cancel { job, force: false };
        db::insert_operation(
            &coordinator.conn,
            "old-cancel",
            op.kind(),
            &op.request_hash(),
            &encoded,
            0,
        )
        .unwrap();
        let Reply::Job { job: replayed } = coordinator.handle_mutation(&op, "old-cancel").unwrap()
        else {
            panic!("expected job")
        };
        assert!(replayed.attempts.is_empty());
        let (hash, compact) = db::lookup_operation(&coordinator.conn, "old-cancel")
            .unwrap()
            .unwrap();
        assert_eq!(hash, op.request_hash());
        assert!(compact.len() < protocol::DEFAULT_MAX_FRAME_BYTES as usize);
    }

    #[test]
    fn only_one_default_can_be_enabled_and_it_cannot_share_or_gate_dependencies() {
        let mut conn = db::open_in_memory().unwrap();
        let mut invalid = idle_params();
        invalid.max_parallel_runs = 2;
        assert_eq!(
            policy_op(&mut conn, Op::Submit(invalid)).unwrap_err().code,
            error_codes::INVALID_ARGUMENT
        );
        let job = idle_job(&mut conn);
        assert!(db::job_row(&conn, job).unwrap().unwrap().is_default);
        assert_eq!(
            policy_op(&mut conn, Op::Submit(idle_params()))
                .unwrap_err()
                .code,
            error_codes::INVALID_STATE
        );
        let mut child = idle_params();
        child.is_default = false;
        child.dependencies.push(protocol::DependencySpec {
            job,
            condition: domain::DependencyCondition::Terminal,
        });
        assert_eq!(
            policy_op(&mut conn, Op::Submit(child)).unwrap_err().code,
            error_codes::INVALID_ARGUMENT
        );
        assert!(
            policy_op(
                &mut conn,
                Op::SetMaxParallelRuns {
                    job,
                    max_parallel_runs: 2
                }
            )
            .is_err()
        );
        policy_op(&mut conn, Op::Cancel { job, force: false }).unwrap();
        assert_ne!(idle_job(&mut conn), job);
    }

    #[test]
    fn default_yield_is_durable_and_keeps_the_lease_until_completion() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "not-spawned", 0).unwrap();
        db::set_attempt_state(&conn, attempt.id, AttemptState::Authorized, None).unwrap();
        let attempt = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
        let tx = conn.transaction().unwrap();
        let actions = request_default_yield(&tx, &row, &attempt, 10).unwrap();
        assert_eq!(actions.len(), 1);
        tx.commit().unwrap();
        let committed = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
        assert!(committed.yield_requested && committed.cancel_requested && committed.cancel_force);
        assert_eq!(db::active_leases(&conn).unwrap().len(), 1);
        // Simulate restart by constructing bookkeeping from persisted rows.
        let mut coordinator = bookkeeping_coordinator(conn);
        coordinator
            .finalize_attempt_and_job(
                &committed,
                AttemptState::Cancelled,
                None,
                Some(libc::SIGTERM),
                Some("yielded"),
                "runner",
            )
            .unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
            JobState::Queued
        );
        assert!(db::active_leases(&coordinator.conn).unwrap().is_empty());
    }

    #[test]
    fn operator_cancel_overrides_default_restart_and_prepared_yield_is_safe() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let prepared = db::create_attempt_with_lease(&conn, &row, "never-authorized", 0).unwrap();
        let tx = conn.transaction().unwrap();
        request_default_yield(&tx, &row, &prepared, 10).unwrap();
        tx.commit().unwrap();
        assert!(db::active_leases(&conn).unwrap().is_empty());
        assert_eq!(
            db::job_row(&conn, job).unwrap().unwrap().state,
            JobState::Queued
        );
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "next-run", 2_000).unwrap();
        db::set_attempt_state(&conn, attempt.id, AttemptState::Authorized, None).unwrap();
        let attempt = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
        let tx = conn.transaction().unwrap();
        request_default_yield(&tx, &row, &attempt, 2_010).unwrap();
        tx.commit().unwrap();
        policy_op(&mut conn, Op::Cancel { job, force: true }).unwrap();
        let committed = db::attempt_row(&conn, attempt.id).unwrap().unwrap();
        assert!(!committed.yield_requested);
        let mut coordinator = bookkeeping_coordinator(conn);
        coordinator
            .finalize_attempt_and_job(
                &committed,
                AttemptState::Cancelled,
                None,
                Some(libc::SIGTERM),
                None,
                "runner",
            )
            .unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
            JobState::Cancelled
        );
    }

    #[test]
    fn successful_default_repeats_but_failed_default_obeys_its_retry_budget() {
        let mut conn = db::open_in_memory().unwrap();
        let job = idle_job(&mut conn);
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "success", 0).unwrap();
        let mut coordinator = bookkeeping_coordinator(conn);
        coordinator
            .finalize_attempt_and_job(
                &attempt,
                AttemptState::Succeeded,
                Some(0),
                None,
                None,
                "runner",
            )
            .unwrap();
        let row = db::job_row(&coordinator.conn, job).unwrap().unwrap();
        assert_eq!(row.state, JobState::Queued);
        assert!(row.retry_not_before.is_some());
        let failed =
            db::create_attempt_with_lease(&coordinator.conn, &row, "failure", 2_000).unwrap();
        coordinator
            .finalize_attempt_and_job(&failed, AttemptState::Failed, Some(1), None, None, "runner")
            .unwrap();
        assert_eq!(
            db::job_row(&coordinator.conn, job).unwrap().unwrap().state,
            JobState::Failed
        );
    }

    #[test]
    fn class_pause_and_membership_changes_are_scoped() {
        let mut conn = db::open_in_memory().unwrap();
        policy_op(
            &mut conn,
            Op::ClassCreate {
                name: "ablations".into(),
                run_window: None,
            },
        )
        .unwrap();
        let a = policy_job(&mut conn, "a", None);
        let b = policy_job(&mut conn, "b", None);
        policy_op(
            &mut conn,
            Op::SetClass {
                job: b,
                class: None,
            },
        )
        .unwrap();
        policy_op(
            &mut conn,
            Op::ClassPause {
                name: "ablations".into(),
                paused: true,
            },
        )
        .unwrap();
        assert_eq!(
            db::eligible_candidates(&conn, 0)
                .unwrap()
                .iter()
                .map(|c| c.job_id)
                .collect::<Vec<_>>(),
            vec![b]
        );
        policy_op(
            &mut conn,
            Op::ClassPause {
                name: "ablations".into(),
                paused: false,
            },
        )
        .unwrap();
        assert_eq!(db::eligible_candidates(&conn, 0).unwrap().len(), 2);
        let err = policy_op(
            &mut conn,
            Op::SetClass {
                job: a,
                class: Some("missing".into()),
            },
        )
        .unwrap_err();
        assert_eq!(err.code, error_codes::NOT_FOUND);
        assert_eq!(
            db::job_row(&conn, a).unwrap().unwrap().class.as_deref(),
            Some("ablations")
        );
        assert!(
            policy_op(
                &mut conn,
                Op::ClassCreate {
                    name: "bad name".into(),
                    run_window: None
                }
            )
            .is_err()
        );
    }

    #[test]
    fn group_cancel_handles_dependency_cascades_and_leaves_other_jobs() {
        let mut conn = db::open_in_memory().unwrap();
        policy_op(
            &mut conn,
            Op::ClassCreate {
                name: "ablations".into(),
                run_window: None,
            },
        )
        .unwrap();
        let parent = policy_job(&mut conn, "parent", None);
        let child = policy_job(&mut conn, "child", Some(parent));
        let other = policy_job(&mut conn, "other", None);
        policy_op(
            &mut conn,
            Op::SetClass {
                job: other,
                class: None,
            },
        )
        .unwrap();
        policy_op(
            &mut conn,
            Op::ClassCancel {
                name: "ablations".into(),
                force: false,
            },
        )
        .unwrap();
        assert_eq!(
            db::job_row(&conn, parent).unwrap().unwrap().state,
            JobState::Cancelled
        );
        assert_eq!(
            db::job_row(&conn, child).unwrap().unwrap().state,
            JobState::Skipped
        );
        assert_eq!(
            db::job_row(&conn, other).unwrap().unwrap().state,
            JobState::Queued
        );
        // Repeating a class cancellation with only terminal members succeeds.
        policy_op(
            &mut conn,
            Op::ClassCancel {
                name: "ablations".into(),
                force: false,
            },
        )
        .unwrap();
    }

    #[test]
    fn live_class_windows_cannot_be_rewritten_but_pauses_are_allowed() {
        let mut conn = db::open_in_memory().unwrap();
        policy_op(
            &mut conn,
            Op::ClassCreate {
                name: "ablations".into(),
                run_window: None,
            },
        )
        .unwrap();
        let job = policy_job(&mut conn, "a", None);
        let row = db::job_row(&conn, job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "not-spawned", 0).unwrap();
        let window = Some("23:00-07:00".parse().unwrap());
        let err = policy_op(
            &mut conn,
            Op::ClassSetWindow {
                name: "ablations".into(),
                run_window: window.clone(),
            },
        )
        .unwrap_err();
        assert_eq!(err.code, error_codes::INVALID_STATE);
        assert!(
            policy_op(
                &mut conn,
                Op::SetRunWindow {
                    job,
                    run_window: window
                }
            )
            .is_err()
        );
        assert!(policy_op(&mut conn, Op::SetClass { job, class: None }).is_err());
        policy_op(
            &mut conn,
            Op::ClassPause {
                name: "ablations".into(),
                paused: true,
            },
        )
        .unwrap();
        assert_eq!(
            db::attempt_row(&conn, attempt.id).unwrap().unwrap().state,
            AttemptState::Prepared
        );
        assert_eq!(db::active_leases(&conn).unwrap().len(), 1);
    }

    #[test]
    fn unsafe_class_cancel_rolls_back_all_members() {
        let mut conn = db::open_in_memory().unwrap();
        policy_op(
            &mut conn,
            Op::ClassCreate {
                name: "ablations".into(),
                run_window: None,
            },
        )
        .unwrap();
        let queued = policy_job(&mut conn, "queued", None);
        let unsafe_job = policy_job(&mut conn, "quarantined", None);
        let row = db::job_row(&conn, unsafe_job).unwrap().unwrap();
        let attempt = db::create_attempt_with_lease(&conn, &row, "not-spawned", 0).unwrap();
        db::set_attempt_state(&conn, attempt.id, AttemptState::Quarantined, None).unwrap();
        db::update_job_state(&conn, unsafe_job, JobState::NeedsAttention, None, 0).unwrap();
        let err = policy_op(
            &mut conn,
            Op::ClassCancel {
                name: "ablations".into(),
                force: true,
            },
        )
        .unwrap_err();
        assert_eq!(err.code, error_codes::UNSAFE_RESOLUTION);
        assert_eq!(
            db::job_row(&conn, queued).unwrap().unwrap().state,
            JobState::Queued
        );
        assert!(
            !db::attempt_row(&conn, attempt.id)
                .unwrap()
                .unwrap()
                .cancel_requested
        );
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
    fn published_time_limit_follows_the_override_or_the_launch_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        assert!(published_time_limit_is_current(
            dir.path(),
            "token",
            None,
            None
        ));

        artifacts::write_artifact(dir.path(), COMMAND_FILE, &command(Some(1_000)), true).unwrap();
        assert!(published_time_limit_is_current(
            dir.path(),
            "token",
            Some(1_000),
            None
        ));
        assert!(!published_time_limit_is_current(
            dir.path(),
            "token",
            Some(50),
            None
        ));
        assert!(!published_time_limit_is_current(
            dir.path(),
            "token",
            None,
            None
        ));
        assert!(!published_time_limit_is_current(
            dir.path(),
            "token",
            Some(1_000),
            Some(5)
        ));

        artifacts::write_artifact(
            dir.path(),
            LIMIT_FILE,
            &LimitFile {
                token: "token".into(),
                time_limit_ms: None,
                updated_at: Some(5),
            },
            false,
        )
        .unwrap();
        assert!(published_time_limit_is_current(
            dir.path(),
            "token",
            None,
            Some(5)
        ));
        assert!(!published_time_limit_is_current(
            dir.path(),
            "token",
            Some(1_000),
            Some(5)
        ));

        std::fs::write(dir.path().join(LIMIT_FILE), b"{ not json").unwrap();
        assert!(!published_time_limit_is_current(
            dir.path(),
            "token",
            Some(1_000),
            None
        ));
    }

    #[test]
    fn job_subscription_starts_at_head_and_replays_every_backlog_page() {
        let dir = tempfile::tempdir().unwrap();
        let paths = Paths {
            runtime_dir: dir.path().join("runtime"),
            state_dir: dir.path().join("state"),
            config_file: dir.path().join("config.toml"),
        };
        let conn = db::open_in_memory().unwrap();
        let params = SubmitParams {
            name: "subscribed".into(),
            is_default: false,
            class: None,
            run_window: None,
            cwd: "/tmp".into(),
            args: vec!["true".into()],
            env: BTreeMap::new(),
            max_parallel_runs: 1,
            priority: 0,
            max_attempts: 1,
            retry_delay_ms: 0,
            time_limit_ms: None,
            dependencies: Vec::new(),
        };
        let job = db::insert_job(&conn, &params, 1000).unwrap();
        let other = db::insert_job(&conn, &params, 1000).unwrap();
        db::append_event(&conn, Some(job), None, "job_submitted", "client", None).unwrap();
        let mut coordinator = Coordinator::new(conn, Config::default(), paths).unwrap();
        let Reply::JobUpdate(initial) = coordinator
            .handle_read(&Op::SubscribeJob { job, after: None })
            .unwrap()
        else {
            panic!("expected job update")
        };
        assert_eq!(initial.job.id, job);
        assert_eq!(
            initial.cursor,
            db::latest_event_id(&coordinator.conn).unwrap()
        );
        assert!(
            initial.events.is_empty(),
            "initial subscribe does not replay history"
        );
        assert!(!initial.has_more);

        // All transitions commit before any reader gets another turn, just as
        // with coalesced watch wakeups and rapid starts/retries/completion.
        let mut expected = Vec::new();
        for _ in 0..200 {
            for event in ["attempt_running", "attempt_failed", "job_retry_scheduled"] {
                db::append_event(&coordinator.conn, Some(other), None, event, "daemon", None)
                    .unwrap();
                db::append_event(&coordinator.conn, Some(job), None, event, "daemon", None)
                    .unwrap();
                expected.push((
                    db::latest_event_id(&coordinator.conn).unwrap(),
                    event.to_string(),
                ));
            }
        }
        db::update_job_state(&coordinator.conn, job, JobState::Succeeded, None, 2000).unwrap();
        db::append_event(
            &coordinator.conn,
            Some(job),
            None,
            "job_succeeded",
            "daemon",
            None,
        )
        .unwrap();
        expected.push((
            db::latest_event_id(&coordinator.conn).unwrap(),
            "job_succeeded".into(),
        ));
        let mut cursor = initial.cursor;
        let mut actual = Vec::new();
        loop {
            let Reply::JobUpdate(page) = coordinator
                .handle_read(&Op::SubscribeJob {
                    job,
                    after: Some(cursor),
                })
                .unwrap()
            else {
                panic!("expected job update")
            };
            assert_eq!(page.job.state, "succeeded");
            assert!(page.events.iter().all(|event| event.job == Some(job)));
            if actual.is_empty() {
                assert!(page.has_more, "terminal snapshot must not hide a backlog");
            }
            assert!(page.events.len() <= 256);
            cursor = page.cursor;
            actual.extend(
                page.events
                    .into_iter()
                    .map(|event| (event.id, event.event_type)),
            );
            if !page.has_more {
                break;
            }
        }
        assert_eq!(actual, expected);
        let missing = coordinator
            .handle_read(&Op::SubscribeJob {
                job: other + 1,
                after: None,
            })
            .unwrap_err();
        assert_eq!(missing.code, error_codes::NOT_FOUND);
    }
}
