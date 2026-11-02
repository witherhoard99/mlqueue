//! SQLite persistence: pragmas, embedded monotonic migrations, and the
//! transactional queries the coordinator composes. Every state transition and
//! its event are committed together by the caller.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};

use crate::domain::{
    AttemptId, AttemptState, DependencyCondition, DependencyResolution, JobId, JobState,
    SCHEDULER_SEMANTICS_VERSION, now_ms,
};
use crate::protocol::{ClassView, SubmitParams};
use crate::run_window::RunWindow;
use crate::scheduler::{ActiveLease, Candidate};

const MIGRATIONS: &[&str] = &[
    // v1: initial schema.
    r#"
    CREATE TABLE jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        state TEXT NOT NULL,
        cwd TEXT NOT NULL,
        args TEXT NOT NULL,
        env TEXT NOT NULL,
        max_parallel_runs INTEGER NOT NULL CHECK (max_parallel_runs >= 1),
        max_attempts INTEGER NOT NULL,
        retry_delay_ms INTEGER NOT NULL,
        retry_not_before INTEGER,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        state_reason TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        finished_at INTEGER
    );
    CREATE INDEX jobs_state ON jobs(state);

    CREATE TABLE dependencies (
        parent_id INTEGER NOT NULL REFERENCES jobs(id),
        child_id INTEGER NOT NULL REFERENCES jobs(id),
        requirement TEXT NOT NULL,
        PRIMARY KEY (parent_id, child_id)
    ) WITHOUT ROWID;
    CREATE INDEX dependencies_child ON dependencies(child_id);

    CREATE TABLE attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        number INTEGER NOT NULL,
        state TEXT NOT NULL,
        launch_token TEXT NOT NULL,
        max_parallel_runs INTEGER NOT NULL,
        runner_pid INTEGER,
        runner_start_time INTEGER,
        runner_boot_id TEXT,
        cmd_pid INTEGER,
        cmd_pgid INTEGER,
        cmd_start_time INTEGER,
        exit_code INTEGER,
        term_signal INTEGER,
        cancel_requested INTEGER NOT NULL DEFAULT 0,
        cancel_force INTEGER NOT NULL DEFAULT 0,
        cancel_requested_at INTEGER,
        message TEXT,
        created_at INTEGER NOT NULL,
        authorized_at INTEGER,
        running_at INTEGER,
        finished_at INTEGER,
        UNIQUE (job_id, number)
    );
    CREATE INDEX attempts_job ON attempts(job_id);
    CREATE INDEX attempts_state ON attempts(state);

    CREATE TABLE run_leases (
        attempt_id INTEGER PRIMARY KEY REFERENCES attempts(id),
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        max_parallel_runs INTEGER NOT NULL,
        acquired_at INTEGER NOT NULL,
        released_at INTEGER
    );
    CREATE INDEX run_leases_open ON run_leases(attempt_id) WHERE released_at IS NULL;

    CREATE TABLE scheduler_reservation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        cutoff_seq INTEGER NOT NULL,
        semantics_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        reason TEXT,
        initial_blockers TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        resolved_at INTEGER
    );
    CREATE UNIQUE INDEX reservation_single_active
        ON scheduler_reservation(status) WHERE status = 'active';

    CREATE TABLE scheduler_reservation_backfills (
        reservation_id INTEGER NOT NULL REFERENCES scheduler_reservation(id),
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        reason TEXT NOT NULL,
        attempt_id INTEGER REFERENCES attempts(id),
        created_at INTEGER NOT NULL,
        PRIMARY KEY (reservation_id, job_id)
    ) WITHOUT ROWID;

    CREATE TABLE operations (
        key TEXT PRIMARY KEY,
        op_type TEXT NOT NULL,
        request_hash TEXT NOT NULL,
        response TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );

    CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        job_id INTEGER,
        attempt_id INTEGER,
        type TEXT NOT NULL,
        actor TEXT NOT NULL,
        details TEXT
    );
    CREATE INDEX events_job ON events(job_id);
    "#,
    // v2: signed scheduling priority; higher values rank before lower values,
    // with the existing job ID remaining the FIFO tiebreaker.
    r#"
    ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
    CREATE INDEX jobs_state_priority_sequence ON jobs(state, priority DESC, id ASC);
    UPDATE scheduler_reservation
       SET semantics_version = 3
     WHERE status = 'active' AND semantics_version IN (1, 2);
    "#,
    // v3: stop creating job dependency edges. Jobs whose prerequisites were
    // unresolved at upgrade time must never become runnable merely because
    // the feature disappeared; terminalize that snapshot before dropping the
    // edges. Historical skipped jobs remain truthful legacy records.
    r#"
    CREATE TEMP TABLE _mlqueue_v3_blocked_jobs (
        job_id INTEGER PRIMARY KEY,
        migrated_at INTEGER NOT NULL
    );
    INSERT INTO _mlqueue_v3_blocked_jobs (job_id, migrated_at)
    SELECT DISTINCT child.id,
           CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
      FROM jobs child
      JOIN dependencies d ON d.child_id = child.id
      JOIN jobs parent ON parent.id = d.parent_id
     WHERE child.state IN ('queued', 'held')
       AND NOT (
           (d.requirement = 'success' AND parent.state = 'succeeded')
           OR (d.requirement = 'completion'
               AND parent.state IN ('succeeded','failed','cancelled','lost','skipped'))
       );
    UPDATE jobs
       SET state = 'cancelled',
           state_reason = 'dependency feature removed before prerequisites completed',
           updated_at = (SELECT migrated_at FROM _mlqueue_v3_blocked_jobs WHERE job_id = jobs.id),
           finished_at = (SELECT migrated_at FROM _mlqueue_v3_blocked_jobs WHERE job_id = jobs.id)
     WHERE id IN (SELECT job_id FROM _mlqueue_v3_blocked_jobs);
    INSERT INTO events (ts, job_id, attempt_id, type, actor, details)
    SELECT migrated_at, job_id, NULL, 'job_cancelled', 'migration',
           '{"reason":"dependency feature removed before prerequisites completed"}'
      FROM _mlqueue_v3_blocked_jobs;
    DROP TABLE _mlqueue_v3_blocked_jobs;
    DROP TABLE IF EXISTS dependencies;
    "#,
    // v4: optional per-attempt wall-clock limit, measured from command spawn.
    r#"
    ALTER TABLE jobs ADD COLUMN time_limit_ms INTEGER
        CHECK (time_limit_ms IS NULL OR time_limit_ms > 0);
    ALTER TABLE attempts ADD COLUMN command_started_at INTEGER;
    ALTER TABLE attempts ADD COLUMN timed_out_at INTEGER;
    "#,
    // v5: restore immutable dependency edges with latched outcomes. Readiness
    // gets its own monotonic sequence so a dependency-delayed job joins FIFO
    // when it becomes runnable instead of reserving its submission position.
    r#"
    ALTER TABLE jobs ADD COLUMN ready_seq INTEGER;
    CREATE TABLE scheduler_ready_sequence (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        value INTEGER NOT NULL CHECK (value >= 0)
    );
    INSERT INTO scheduler_ready_sequence (singleton, value)
    SELECT 1, COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'jobs'), 0);
    UPDATE jobs
       SET ready_seq = id
     WHERE state NOT IN ('queued', 'held')
        OR retry_not_before IS NULL
        OR retry_not_before <= CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER);
    CREATE UNIQUE INDEX jobs_ready_sequence ON jobs(ready_seq) WHERE ready_seq IS NOT NULL;
    DROP INDEX IF EXISTS jobs_state_priority_sequence;
    CREATE INDEX jobs_state_priority_ready
        ON jobs(state, priority DESC, ready_seq ASC);

    CREATE TABLE dependencies (
        parent_id INTEGER NOT NULL REFERENCES jobs(id),
        child_id INTEGER NOT NULL REFERENCES jobs(id),
        condition TEXT NOT NULL CHECK (condition IN ('succeeded', 'terminal')),
        resolution TEXT CHECK (resolution IN ('satisfied', 'violated')),
        parent_outcome TEXT CHECK (
            parent_outcome IS NULL OR parent_outcome IN
                ('succeeded','failed','cancelled','lost','skipped')
        ),
        resolved_at INTEGER,
        PRIMARY KEY (parent_id, child_id),
        CHECK (parent_id < child_id),
        CHECK (
            (resolution IS NULL AND parent_outcome IS NULL AND resolved_at IS NULL)
            OR
            (resolution IS NOT NULL AND parent_outcome IS NOT NULL AND resolved_at IS NOT NULL)
        )
    ) WITHOUT ROWID;
    CREATE INDEX dependencies_child ON dependencies(child_id);
    CREATE INDEX dependencies_unresolved_parent
        ON dependencies(parent_id) WHERE resolution IS NULL;

    UPDATE scheduler_reservation
       SET semantics_version = 4
     WHERE status = 'active' AND semantics_version = 3;
    "#,
    // v6: durable operator pause. Queued work stays queued; the scheduler
    // issues no new run leases until unpause. Running leases are untouched.
    r#"
    CREATE TABLE scheduler_pause (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
        updated_at INTEGER NOT NULL
    );
    INSERT INTO scheduler_pause (singleton, paused, updated_at) VALUES (1, 0, 0);
    "#,
    // v7: when the current time limit was installed. Null means the attempt
    // launched with it. Used so a later edit cannot reclassify an earlier cancel.
    r#"
    ALTER TABLE jobs ADD COLUMN time_limit_set_at INTEGER;
    "#,
    // v8: existing jobs remain unclassified and unrestricted.
    r#"
    CREATE TABLE job_classes (
        name TEXT PRIMARY KEY,
        paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
        run_window TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    ALTER TABLE jobs ADD COLUMN class TEXT REFERENCES job_classes(name);
    ALTER TABLE jobs ADD COLUMN run_window TEXT;
    ALTER TABLE attempts ADD COLUMN window_end_at INTEGER;
    CREATE INDEX jobs_class ON jobs(class, id);
    "#,
    // v9: one recurring, preemptible idle job; yielding is distinct from
    // an operator cancellation so it can restart after ordinary work drains.
    r#"
    ALTER TABLE jobs ADD COLUMN is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1));
    ALTER TABLE attempts ADD COLUMN yield_requested INTEGER NOT NULL DEFAULT 0 CHECK (yield_requested IN (0, 1));
    CREATE UNIQUE INDEX jobs_one_default ON jobs(is_default)
        WHERE is_default = 1 AND state NOT IN ('succeeded','failed','cancelled','lost','skipped');
    "#,
];

pub fn open(path: &Path) -> Result<Connection> {
    let conn =
        Connection::open(path).with_context(|| format!("opening database {}", path.display()))?;
    configure(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

pub fn open_in_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    configure(&conn)?;
    migrate(&conn)?;
    Ok(conn)
}

fn configure(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(())
}

pub fn migrate(conn: &Connection) -> Result<()> {
    let current: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    ensure!(
        current <= MIGRATIONS.len() as i64,
        "database schema {current} is newer than this binary supports"
    );
    for (idx, migration) in MIGRATIONS.iter().enumerate() {
        let version = idx as i64 + 1;
        if version <= current {
            continue;
        }
        conn.execute_batch(&format!(
            "BEGIN IMMEDIATE;\n{migration}\nPRAGMA user_version = {version};\nCOMMIT;"
        ))
        .with_context(|| format!("applying migration {version}"))?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: JobId,
    pub name: String,
    pub is_default: bool,
    pub class: Option<String>,
    pub run_window: Option<RunWindow>,
    pub state: JobState,
    pub cwd: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub max_parallel_runs: u32,
    pub priority: i64,
    pub max_attempts: i64,
    pub retry_delay_ms: i64,
    pub time_limit_ms: Option<i64>,
    /// When `time_limit_ms` was last changed after submission. Null means the
    /// submitted value is still in force.
    pub time_limit_set_at: Option<i64>,
    pub retry_not_before: Option<i64>,
    pub ready_seq: Option<i64>,
    pub attempt_count: i64,
    pub state_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DependencyRow {
    pub parent_id: JobId,
    pub child_id: JobId,
    pub condition: DependencyCondition,
    pub parent_state: JobState,
    pub resolution: Option<DependencyResolution>,
    pub parent_outcome: Option<JobState>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct AttemptRow {
    pub id: AttemptId,
    pub job_id: JobId,
    pub number: i64,
    pub state: AttemptState,
    pub launch_token: String,
    pub max_parallel_runs: u32,
    pub runner_pid: Option<i32>,
    pub runner_start_time: Option<i64>,
    pub runner_boot_id: Option<String>,
    pub cmd_pid: Option<i32>,
    pub cmd_pgid: Option<i32>,
    pub cmd_start_time: Option<i64>,
    pub command_started_at: Option<i64>,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub cancel_requested: bool,
    pub cancel_force: bool,
    pub cancel_requested_at: Option<i64>,
    pub message: Option<String>,
    pub created_at: i64,
    pub authorized_at: Option<i64>,
    pub running_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub timed_out_at: Option<i64>,
    pub window_end_at: Option<i64>,
    pub yield_requested: bool,
}

#[derive(Debug, Clone)]
pub struct ReservationRow {
    pub id: i64,
    pub job_id: JobId,
    pub cutoff_seq: i64,
    pub semantics_version: i64,
    pub created_at: i64,
    pub initial_blockers: Vec<AttemptId>,
    pub consumed: BTreeSet<JobId>,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub id: i64,
    pub timestamp: i64,
    pub job_id: Option<JobId>,
    pub job_name: Option<String>,
    pub attempt_id: Option<AttemptId>,
    pub attempt_number: Option<i64>,
    pub event_type: String,
}

const JOB_COLUMNS: &str = "id, name, state, cwd, args, env, max_parallel_runs, priority, \
     max_attempts, retry_delay_ms, time_limit_ms, time_limit_set_at, retry_not_before, ready_seq, \
     attempt_count, state_reason, created_at, updated_at, finished_at, class, run_window, is_default";

fn job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRow> {
    let state: String = row.get(2)?;
    let args: String = row.get(4)?;
    let env: String = row.get(5)?;
    Ok(JobRow {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state.parse().expect("valid job state in database"),
        cwd: row.get(3)?,
        args: serde_json::from_str(&args).expect("valid args JSON in database"),
        env: serde_json::from_str(&env).expect("valid env JSON in database"),
        max_parallel_runs: row.get(6)?,
        priority: row.get(7)?,
        max_attempts: row.get(8)?,
        retry_delay_ms: row.get(9)?,
        time_limit_ms: row.get(10)?,
        time_limit_set_at: row.get(11)?,
        retry_not_before: row.get(12)?,
        ready_seq: row.get(13)?,
        attempt_count: row.get(14)?,
        state_reason: row.get(15)?,
        created_at: row.get(16)?,
        updated_at: row.get(17)?,
        finished_at: row.get(18)?,
        class: row.get(19)?,
        run_window: window_from_row(row, 20)?,
        is_default: row.get(21)?,
    })
}

const ATTEMPT_COLUMNS: &str = "id, job_id, number, state, launch_token, max_parallel_runs, \
     runner_pid, runner_start_time, runner_boot_id, cmd_pid, cmd_pgid, cmd_start_time, \
     command_started_at, exit_code, term_signal, cancel_requested, cancel_force, \
     cancel_requested_at, message, created_at, authorized_at, running_at, finished_at, timed_out_at, window_end_at, yield_requested";

fn attempt_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AttemptRow> {
    let state: String = row.get(3)?;
    Ok(AttemptRow {
        id: row.get(0)?,
        job_id: row.get(1)?,
        number: row.get(2)?,
        state: state.parse().expect("valid attempt state in database"),
        launch_token: row.get(4)?,
        max_parallel_runs: row.get(5)?,
        runner_pid: row.get(6)?,
        runner_start_time: row.get(7)?,
        runner_boot_id: row.get(8)?,
        cmd_pid: row.get(9)?,
        cmd_pgid: row.get(10)?,
        cmd_start_time: row.get(11)?,
        command_started_at: row.get(12)?,
        exit_code: row.get(13)?,
        term_signal: row.get(14)?,
        cancel_requested: row.get::<_, i64>(15)? != 0,
        cancel_force: row.get::<_, i64>(16)? != 0,
        cancel_requested_at: row.get(17)?,
        message: row.get(18)?,
        created_at: row.get(19)?,
        authorized_at: row.get(20)?,
        running_at: row.get(21)?,
        finished_at: row.get(22)?,
        timed_out_at: row.get(23)?,
        window_end_at: row.get(24)?,
        yield_requested: row.get(25)?,
    })
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

fn window_from_row(row: &rusqlite::Row<'_>, idx: usize) -> rusqlite::Result<Option<RunWindow>> {
    let value: Option<String> = row.get(idx)?;
    value
        .map(|value| {
            value.parse().map_err(|err: String| {
                rusqlite::Error::FromSqlConversionFailure(
                    idx,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
                )
            })
        })
        .transpose()
}

pub fn classes(conn: &Connection) -> Result<Vec<ClassView>> {
    let mut stmt =
        conn.prepare("SELECT name, paused, run_window FROM job_classes ORDER BY name")?;
    let rows = stmt.query_map([], class_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn class_row(conn: &Connection, name: &str) -> Result<Option<ClassView>> {
    Ok(conn
        .query_row(
            "SELECT name, paused, run_window FROM job_classes WHERE name = ?1",
            [name],
            class_from_row,
        )
        .optional()?)
}

fn class_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ClassView> {
    Ok(ClassView {
        name: row.get(0)?,
        paused: row.get(1)?,
        run_window: window_from_row(row, 2)?,
    })
}

pub fn class_jobs(conn: &Connection, name: &str) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs WHERE class = ?1 ORDER BY id"
    ))?;
    let rows = stmt.query_map([name], job_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn default_job(conn: &Connection) -> Result<Option<JobRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs
                  WHERE is_default = 1
                    AND state NOT IN ('succeeded','failed','cancelled','lost','skipped')"
            ),
            [],
            job_from_row,
        )
        .optional()?)
}

/// Select the active default, or the most recently submitted default from history.
pub fn default_for_control(conn: &Connection) -> Result<Option<JobRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE is_default = 1
                 ORDER BY (finished_at IS NULL) DESC, id DESC LIMIT 1"
            ),
            [],
            job_from_row,
        )
        .optional()?)
}

/// Prefer the active default, otherwise find this command's latest default.
pub fn default_for_command(conn: &Connection, command: &str) -> Result<Option<JobRow>> {
    let args = serde_json::to_string(&[command])?;
    Ok(conn
        .query_row(
            &format!(
                "SELECT {JOB_COLUMNS} FROM jobs
                 WHERE is_default = 1 AND (finished_at IS NULL OR args = ?1)
                 ORDER BY (finished_at IS NULL) DESC, id DESC LIMIT 1"
            ),
            [args],
            job_from_row,
        )
        .optional()?)
}

/// Count failures since the default last succeeded or yielded.
pub fn default_failure_count(conn: &Connection, job: JobId) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM attempts
         WHERE job_id = ?1 AND state = 'failed'
           AND id > COALESCE((
               SELECT MAX(id) FROM attempts
               WHERE job_id = ?1 AND (state = 'succeeded' OR yield_requested = 1)
           ), 0)",
        [job],
        |row| row.get(0),
    )?)
}

/// Explain a class pause or closed window. Running attempts are unaffected.
pub fn scheduling_reason(
    conn: &Connection,
    job: &JobRow,
    now: i64,
) -> Result<Option<&'static str>> {
    if let Some(name) = &job.class {
        let class = class_row(conn, name)?.context("job class missing")?;
        if class.paused {
            return Ok(Some("class_paused"));
        }
        if class.run_window.as_ref().is_some_and(|w| !w.contains(now)) {
            return Ok(Some("waiting_for_class_window"));
        }
    }
    if job.run_window.as_ref().is_some_and(|w| !w.contains(now)) {
        return Ok(Some("waiting_for_run_window"));
    }
    Ok(None)
}

/// Find the earliest window close to store with a new attempt.
pub fn window_end_at(conn: &Connection, job: &JobRow, now: i64) -> Result<Option<i64>> {
    let class_window = job
        .class
        .as_deref()
        .map(|name| class_row(conn, name))
        .transpose()?
        .flatten()
        .and_then(|class| class.run_window);
    let mut end: Option<i64> = None;
    for window in [job.run_window.as_ref(), class_window.as_ref()]
        .into_iter()
        .flatten()
    {
        let close = window
            .closing_at(now)
            .context("attempt cannot start outside its run window")?;
        end = Some(end.map_or(close, |current| current.min(close)));
    }
    Ok(end)
}

pub fn insert_job(conn: &Connection, params: &SubmitParams, now: i64) -> Result<JobId> {
    conn.execute(
        "INSERT INTO jobs (name, state, cwd, args, env, max_parallel_runs, priority, \
         max_attempts, retry_delay_ms, time_limit_ms, attempt_count, created_at, updated_at, class, run_window, is_default) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11, ?11, ?12, ?13, ?14)",
        params![
            params.name,
            JobState::Queued.as_str(),
            params.cwd,
            serde_json::to_string(&params.args)?,
            serde_json::to_string(&params.env)?,
            params.max_parallel_runs,
            params.priority,
            params.max_attempts,
            params.retry_delay_ms as i64,
            params.time_limit_ms.map(i64::try_from).transpose()?,
            now,
            params.class,
            params.run_window.as_ref().map(ToString::to_string),
            params.is_default,
        ],
    )?;
    let job_id = conn.last_insert_rowid();
    // A success condition is stricter than terminal, so duplicate declarations
    // collapse deterministically without weakening the prerequisite.
    let mut dependencies = BTreeMap::new();
    for dependency in &params.dependencies {
        dependencies
            .entry(dependency.job)
            .and_modify(|condition| {
                if dependency.condition == DependencyCondition::Succeeded {
                    *condition = DependencyCondition::Succeeded;
                }
            })
            .or_insert(dependency.condition);
    }
    let dependency_free = dependencies.is_empty();
    for (parent, condition) in dependencies {
        conn.execute(
            "INSERT INTO dependencies (parent_id, child_id, condition) VALUES (?1, ?2, ?3)",
            params![parent, job_id, condition.as_str()],
        )?;
    }
    if dependency_free {
        assign_next_ready_seq(conn, job_id, now)?;
    }
    Ok(job_id)
}

pub fn job_row(conn: &Connection, id: JobId) -> Result<Option<JobRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1"),
            [id],
            job_from_row,
        )
        .optional()?)
}

/// Most recently submitted job (`MAX(id)`), if the queue is not empty.
pub fn latest_job_id(conn: &Connection) -> Result<Option<JobId>> {
    Ok(conn.query_row("SELECT MAX(id) FROM jobs", [], |row| row.get(0))?)
}

/// Jobs shown by `status`: all non-terminal plus the most recent terminal.
pub fn status_jobs(conn: &Connection, recent_terminal: u32) -> Result<Vec<JobRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {JOB_COLUMNS} FROM jobs WHERE id IN (
             SELECT id FROM jobs WHERE finished_at IS NULL
             UNION
             SELECT id FROM (
                 SELECT id FROM jobs WHERE finished_at IS NOT NULL
                 ORDER BY finished_at DESC, id DESC LIMIT ?1
             )
         ) ORDER BY id"
    ))?;
    let rows = stmt.query_map([recent_terminal], job_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn update_job_state(
    conn: &Connection,
    id: JobId,
    state: JobState,
    reason: Option<&str>,
    now: i64,
) -> Result<()> {
    // Entering a terminal state stamps finished_at; leaving one (retry)
    // clears it.
    let finished_at = state.is_terminal().then_some(now);
    conn.execute(
        "UPDATE jobs SET state = ?2, state_reason = ?3, updated_at = ?4, finished_at = ?5 \
         WHERE id = ?1",
        params![id, state.as_str(), reason, now, finished_at],
    )?;
    Ok(())
}

pub fn set_job_retry_not_before(
    conn: &Connection,
    id: JobId,
    at: Option<i64>,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET retry_not_before = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, at, now],
    )?;
    Ok(())
}

pub fn clear_job_readiness(conn: &Connection, id: JobId, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET ready_seq = NULL, updated_at = ?2 WHERE id = ?1",
        params![id, now],
    )?;
    Ok(())
}

pub fn set_job_max_parallel_runs(conn: &Connection, id: JobId, value: u32, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET max_parallel_runs = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, value, now],
    )?;
    Ok(())
}

pub fn set_job_priority(conn: &Connection, id: JobId, value: i64, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE jobs SET priority = ?2, updated_at = ?3 WHERE id = ?1",
        params![id, value, now],
    )?;
    Ok(())
}

/// Replace the job's per-attempt wall-clock limit. `None` clears it. The
/// caller applies the same value to a live runner outside this transaction.
pub fn set_job_time_limit(
    conn: &Connection,
    id: JobId,
    value: Option<i64>,
    now: i64,
) -> Result<()> {
    if let Some(value) = value {
        ensure!(value > 0, "timeLimitMs must be a positive integer");
    }
    conn.execute(
        "UPDATE jobs SET time_limit_ms = ?2, time_limit_set_at = ?3, updated_at = ?3 WHERE id = ?1",
        params![id, value, now],
    )?;
    Ok(())
}

/// Change the declaration copied into a live attempt and its active lease.
/// The caller must run this inside the same transaction as the corresponding
/// job-row update. Lowering a limit may leave the active set temporarily
/// oversubscribed relative to the new declaration; that is intentional
/// (no preemption)—admission will refuse new starts until the set drains.
pub fn set_active_attempt_max_parallel_runs(
    conn: &Connection,
    attempt: AttemptId,
    value: u32,
) -> Result<()> {
    ensure!(value > 0, "maxParallelRuns must be positive");
    let mutable: bool = conn.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM attempts a
             JOIN run_leases l ON l.attempt_id = a.id
             WHERE a.id = ?1
               AND a.state IN ('prepared','authorized','running','orphaned','quarantined')
               AND l.released_at IS NULL
         )",
        [attempt],
        |row| row.get(0),
    )?;
    ensure!(
        mutable,
        "attempt {attempt} is not live with an active run lease"
    );

    let updated_attempts = conn.execute(
        "UPDATE attempts SET max_parallel_runs = ?2 WHERE id = ?1 \
         AND state IN ('prepared','authorized','running','orphaned','quarantined')",
        params![attempt, value],
    )?;
    debug_assert_eq!(updated_attempts, 1);

    let updated_leases = conn.execute(
        "UPDATE run_leases SET max_parallel_runs = ?2 \
         WHERE attempt_id = ?1 AND released_at IS NULL",
        params![attempt, value],
    )?;
    debug_assert_eq!(updated_leases, 1);
    Ok(())
}

pub fn max_job_seq(conn: &Connection) -> Result<i64> {
    // sqlite_sequence records the highest id ever allocated, so a future
    // retention cleanup deleting the newest rows can never lower the frozen
    // backfill cutoff. The row is absent until the first job is inserted.
    Ok(conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'jobs'",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

pub fn max_ready_seq(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT value FROM scheduler_ready_sequence WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?)
}

fn assign_next_ready_seq(conn: &Connection, job: JobId, now: i64) -> Result<i64> {
    conn.execute(
        "UPDATE scheduler_ready_sequence SET value = value + 1 WHERE singleton = 1",
        [],
    )?;
    let seq = max_ready_seq(conn)?;
    conn.execute(
        "UPDATE jobs SET ready_seq = ?2, updated_at = ?3 WHERE id = ?1 AND ready_seq IS NULL",
        params![job, seq, now],
    )?;
    Ok(seq)
}

pub fn count_jobs_in_state(conn: &Connection, state: JobState) -> Result<u32> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM jobs WHERE state = ?1",
        [state.as_str()],
        |row| row.get(0),
    )?)
}

pub fn bounded_job_names_in_state(
    conn: &Connection,
    state: JobState,
    name_chars: u32,
    limit: u32,
) -> Result<Vec<(JobId, String)>> {
    let mut stmt = conn.prepare(
        "SELECT id, substr(name, 1, ?2) FROM jobs WHERE state = ?1 ORDER BY id LIMIT ?3",
    )?;
    let rows = stmt.query_map(params![state.as_str(), name_chars, limit], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

// ---------------------------------------------------------------------------
// Dependencies and readiness
// ---------------------------------------------------------------------------

pub fn dependencies_of(conn: &Connection, child: JobId) -> Result<Vec<DependencyRow>> {
    let mut stmt = conn.prepare(
        "SELECT d.parent_id, d.child_id, d.condition, p.state, d.resolution,
                d.parent_outcome, d.resolved_at
           FROM dependencies d JOIN jobs p ON p.id = d.parent_id
          WHERE d.child_id = ?1 ORDER BY d.parent_id",
    )?;
    let rows = stmt.query_map([child], |row| {
        let condition: String = row.get(2)?;
        let parent_state: String = row.get(3)?;
        let resolution: Option<String> = row.get(4)?;
        let parent_outcome: Option<String> = row.get(5)?;
        Ok(DependencyRow {
            parent_id: row.get(0)?,
            child_id: row.get(1)?,
            condition: condition
                .parse()
                .expect("valid dependency condition in database"),
            parent_state: parent_state
                .parse()
                .expect("valid parent state in database"),
            resolution: resolution.map(|value| {
                value
                    .parse()
                    .expect("valid dependency resolution in database")
            }),
            parent_outcome: parent_outcome
                .map(|value| value.parse().expect("valid parent outcome in database")),
            resolved_at: row.get(6)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn unresolved_dependency_parents(conn: &Connection, child: JobId) -> Result<Vec<JobId>> {
    let mut stmt = conn.prepare(
        "SELECT parent_id FROM dependencies
          WHERE child_id = ?1 AND resolution IS NULL ORDER BY parent_id",
    )?;
    let rows = stmt.query_map([child], |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Latch terminal parent outcomes, cascade impossible success requirements,
/// and assign FIFO positions to jobs that have become runnable. Resolved
/// edges never reopen, including when a parent is manually retried later.
pub fn advance_dependency_graph(conn: &Connection, now: i64) -> Result<()> {
    loop {
        let unresolved = {
            let mut stmt = conn.prepare(
                "SELECT d.parent_id, d.child_id, d.condition, p.state
                   FROM dependencies d
                   JOIN jobs p ON p.id = d.parent_id
                  WHERE d.resolution IS NULL
                    AND p.state IN ('succeeded','failed','cancelled','lost','skipped')
                  ORDER BY d.parent_id, d.child_id",
            )?;
            let rows = stmt.query_map([], |row| {
                let condition: String = row.get(2)?;
                let outcome: String = row.get(3)?;
                Ok((
                    row.get::<_, JobId>(0)?,
                    row.get::<_, JobId>(1)?,
                    condition
                        .parse::<DependencyCondition>()
                        .expect("valid dependency condition in database"),
                    outcome
                        .parse::<JobState>()
                        .expect("valid job state in database"),
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (parent, child, condition, outcome) in unresolved {
            let resolution =
                if condition == DependencyCondition::Terminal || outcome == JobState::Succeeded {
                    DependencyResolution::Satisfied
                } else {
                    DependencyResolution::Violated
                };
            conn.execute(
                "UPDATE dependencies
                    SET resolution = ?3, parent_outcome = ?4, resolved_at = ?5
                  WHERE parent_id = ?1 AND child_id = ?2 AND resolution IS NULL",
                params![parent, child, resolution.as_str(), outcome.as_str(), now],
            )?;
        }

        let violated = {
            let mut stmt = conn.prepare(
                "SELECT d.child_id, d.parent_id
                   FROM dependencies d
                   JOIN jobs child ON child.id = d.child_id
                  WHERE d.resolution = 'violated'
                    AND child.state IN ('queued','held')
                  ORDER BY d.child_id, d.parent_id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, JobId>(0)?, row.get::<_, JobId>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        if violated.is_empty() {
            break;
        }
        let mut by_child: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
        for (child, parent) in violated {
            by_child.entry(child).or_default().push(parent);
        }
        for (child, parents) in by_child {
            let reason = format!("prerequisite jobs {parents:?} did not satisfy required outcomes");
            update_job_state(conn, child, JobState::Skipped, Some(&reason), now)?;
            clear_job_readiness(conn, child, now)?;
            append_event(
                conn,
                Some(child),
                None,
                "job_skipped",
                "daemon",
                Some(&serde_json::json!({ "failedPrerequisites": parents }).to_string()),
            )?;
        }
    }

    let ready = {
        let mut stmt = conn.prepare(
            "SELECT j.id FROM jobs j
              WHERE j.state IN ('queued','held')
                AND j.ready_seq IS NULL
                AND (j.retry_not_before IS NULL OR j.retry_not_before <= ?1)
                AND NOT EXISTS (
                    SELECT 1 FROM dependencies d
                     WHERE d.child_id = j.id
                       AND (d.resolution IS NULL OR d.resolution = 'violated')
                )
              ORDER BY j.id",
        )?;
        let rows = stmt.query_map([now], |row| row.get::<_, JobId>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for job in ready {
        let ready_seq = assign_next_ready_seq(conn, job, now)?;
        append_event(
            conn,
            Some(job),
            None,
            "job_ready",
            "daemon",
            Some(&serde_json::json!({ "readySequence": ready_seq }).to_string()),
        )?;
    }
    Ok(())
}

/// Priority/FIFO-ordered jobs eligible for admission right now.
pub fn eligible_candidates(conn: &Connection, now: i64) -> Result<Vec<Candidate>> {
    let mut stmt = conn.prepare(
        "SELECT j.id, j.ready_seq, j.max_parallel_runs, j.priority, j.run_window, c.run_window, j.is_default
         FROM jobs j LEFT JOIN job_classes c ON c.name = j.class
         WHERE j.state = 'queued'
           AND j.ready_seq IS NOT NULL
           AND (j.retry_not_before IS NULL OR j.retry_not_before <= ?1)
           AND COALESCE(c.paused, 0) = 0
         ORDER BY j.is_default ASC, j.priority DESC, j.ready_seq ASC",
    )?;
    let rows = stmt.query_map([now], |row| {
        let job_window = window_from_row(row, 4)?;
        let class_window = window_from_row(row, 5)?;
        if [job_window, class_window]
            .iter()
            .flatten()
            .any(|window| !window.contains(now))
        {
            return Ok(None);
        }
        Ok(Some(Candidate {
            job_id: row.get(0)?,
            ready_seq: row.get(1)?,
            limit: row.get(2)?,
            priority: row.get(3)?,
            is_default: row.get(6)?,
        }))
    })?;
    let mut candidates = Vec::new();
    for candidate in rows {
        if let Some(candidate) = candidate? {
            candidates.push(candidate);
        }
    }
    Ok(candidates)
}

// ---------------------------------------------------------------------------
// Attempts and leases
// ---------------------------------------------------------------------------

/// Create a `prepared` attempt and its run lease atomically and move the job
/// to `starting`. The caller wraps this in the pass-commit transaction.
pub fn create_attempt_with_lease(
    conn: &Connection,
    job: &JobRow,
    launch_token: &str,
    now: i64,
) -> Result<AttemptRow> {
    ensure!(
        scheduling_reason(conn, job, now)?.is_none(),
        "job cannot start under current class/window policy"
    );
    let window_end_at = window_end_at(conn, job, now)?;
    let number = job.attempt_count + 1;
    conn.execute(
        "INSERT INTO attempts (job_id, number, state, launch_token, max_parallel_runs, created_at, window_end_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            job.id,
            number,
            AttemptState::Prepared.as_str(),
            launch_token,
            job.max_parallel_runs,
            now,
            window_end_at
        ],
    )?;
    let attempt_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO run_leases (attempt_id, job_id, max_parallel_runs, acquired_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![attempt_id, job.id, job.max_parallel_runs, now],
    )?;
    conn.execute(
        "UPDATE jobs SET state = ?2, attempt_count = ?3, updated_at = ?4, state_reason = NULL \
         WHERE id = ?1",
        params![job.id, JobState::Starting.as_str(), number, now],
    )?;
    attempt_row(conn, attempt_id)?.context("attempt row just inserted")
}

pub fn attempt_row(conn: &Connection, id: AttemptId) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE id = ?1"),
            [id],
            attempt_from_row,
        )
        .optional()?)
}

pub fn attempt_by_number(conn: &Connection, job: JobId, number: i64) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 AND number = ?2"),
            params![job, number],
            attempt_from_row,
        )
        .optional()?)
}

pub fn attempts_for_job(conn: &Connection, job: JobId) -> Result<Vec<AttemptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 ORDER BY number"
    ))?;
    let rows = stmt.query_map([job], attempt_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Descending attempt numbers keep paging stable while new attempts arrive.
pub fn attempts_page(
    conn: &Connection,
    job: JobId,
    before: Option<i64>,
    limit: u32,
) -> Result<Vec<AttemptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM attempts
         WHERE job_id = ?1 AND (?2 IS NULL OR number < ?2)
         ORDER BY number DESC LIMIT ?3"
    ))?;
    let rows = stmt.query_map(params![job, before, limit], attempt_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// The single non-terminal attempt of a job, if any. The serialized
/// coordinator plus the unique lease per attempt guarantee at most one.
pub fn live_attempt_for_job(conn: &Connection, job: JobId) -> Result<Option<AttemptRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {ATTEMPT_COLUMNS} FROM attempts WHERE job_id = ?1 \
                 AND state IN ('prepared','authorized','running','orphaned','quarantined') \
                 ORDER BY number DESC LIMIT 1"
            ),
            [job],
            attempt_from_row,
        )
        .optional()?)
}

pub fn non_terminal_attempts(conn: &Connection) -> Result<Vec<AttemptRow>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ATTEMPT_COLUMNS} FROM attempts \
         WHERE state IN ('prepared','authorized','running','orphaned','quarantined') ORDER BY id"
    ))?;
    let rows = stmt.query_map([], attempt_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn active_leases(conn: &Connection) -> Result<Vec<(AttemptId, ActiveLease)>> {
    let mut stmt = conn.prepare(
        "SELECT attempt_id, job_id, max_parallel_runs FROM run_leases \
         WHERE released_at IS NULL ORDER BY attempt_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, AttemptId>(0)?,
            ActiveLease {
                job_id: row.get(1)?,
                limit: row.get(2)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn release_lease(conn: &Connection, attempt: AttemptId, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE run_leases SET released_at = ?2 WHERE attempt_id = ?1 AND released_at IS NULL",
        params![attempt, now],
    )?;
    Ok(())
}

pub fn set_attempt_runner_identity(
    conn: &Connection,
    attempt: AttemptId,
    pid: i32,
    start_time: i64,
    boot_id: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET runner_pid = ?2, runner_start_time = ?3, runner_boot_id = ?4 \
         WHERE id = ?1",
        params![attempt, pid, start_time, boot_id],
    )?;
    Ok(())
}

pub fn set_attempt_authorized(conn: &Connection, attempt: AttemptId, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, authorized_at = ?3 WHERE id = ?1",
        params![attempt, AttemptState::Authorized.as_str(), now],
    )?;
    Ok(())
}

pub fn set_attempt_running(
    conn: &Connection,
    attempt: AttemptId,
    cmd_pid: i32,
    cmd_pgid: i32,
    cmd_start_time: i64,
    command_started_at: i64,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, cmd_pid = ?3, cmd_pgid = ?4, cmd_start_time = ?5, \
         command_started_at = ?6, running_at = ?7 WHERE id = ?1",
        params![
            attempt,
            AttemptState::Running.as_str(),
            cmd_pid,
            cmd_pgid,
            cmd_start_time,
            command_started_at,
            now
        ],
    )?;
    Ok(())
}

pub fn set_attempt_timed_out(conn: &Connection, attempt: AttemptId, deadline: i64) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET timed_out_at = COALESCE(timed_out_at, ?2) WHERE id = ?1",
        params![attempt, deadline],
    )?;
    Ok(())
}

pub fn set_attempt_state(
    conn: &Connection,
    attempt: AttemptId,
    state: AttemptState,
    message: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET state = ?2, message = COALESCE(?3, message) WHERE id = ?1",
        params![attempt, state.as_str(), message],
    )?;
    Ok(())
}

pub fn finalize_attempt(
    conn: &Connection,
    attempt: AttemptId,
    state: AttemptState,
    exit_code: Option<i32>,
    term_signal: Option<i32>,
    message: Option<&str>,
    now: i64,
) -> Result<()> {
    debug_assert!(state.is_terminal());
    conn.execute(
        "UPDATE attempts SET state = ?2, exit_code = ?3, term_signal = ?4, \
         message = COALESCE(?5, message), finished_at = ?6 WHERE id = ?1",
        params![
            attempt,
            state.as_str(),
            exit_code,
            term_signal,
            message,
            now
        ],
    )?;
    Ok(())
}

pub fn set_attempt_cancel_requested(
    conn: &Connection,
    attempt: AttemptId,
    force: bool,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE attempts SET cancel_requested = 1, cancel_force = MAX(cancel_force, ?2), \
         cancel_requested_at = COALESCE(cancel_requested_at, ?3) WHERE id = ?1",
        params![attempt, force as i64, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Reservation
// ---------------------------------------------------------------------------

/// Sentinel `cutoff_seq` for a reservation whose backfill window is still
/// open. Job sequences start at 1, so 0 is unambiguous, and it keeps the
/// column NOT NULL: the freeze can then be made idempotent (pin the real
/// cutoff exactly once, when the window closes) without a schema change.
pub const RESERVATION_CUTOFF_UNFROZEN: i64 = 0;

pub fn active_reservation(conn: &Connection) -> Result<Option<ReservationRow>> {
    let row = conn
        .query_row(
            "SELECT id, job_id, cutoff_seq, semantics_version, created_at, initial_blockers \
             FROM scheduler_reservation WHERE status = 'active'",
            [],
            |row| {
                let blockers: String = row.get(5)?;
                Ok(ReservationRow {
                    id: row.get(0)?,
                    job_id: row.get(1)?,
                    cutoff_seq: row.get(2)?,
                    semantics_version: row.get(3)?,
                    created_at: row.get(4)?,
                    initial_blockers: serde_json::from_str(&blockers)
                        .expect("valid blockers JSON in database"),
                    consumed: BTreeSet::new(),
                })
            },
        )
        .optional()?;
    let Some(mut reservation) = row else {
        return Ok(None);
    };
    let mut stmt = conn
        .prepare("SELECT job_id FROM scheduler_reservation_backfills WHERE reservation_id = ?1")?;
    let consumed = stmt.query_map([reservation.id], |row| row.get::<_, JobId>(0))?;
    reservation.consumed = consumed.collect::<rusqlite::Result<BTreeSet<_>>>()?;
    Ok(Some(reservation))
}

/// Create an active reservation with an open backfill window
/// ([`RESERVATION_CUTOFF_UNFROZEN`]); the cutoff is pinned later by
/// [`freeze_reservation_cutoff`] when the initial blockers stop advancing.
pub fn create_reservation(
    conn: &Connection,
    job: JobId,
    semantics_version: i64,
    initial_blockers: &[AttemptId],
    now: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO scheduler_reservation \
         (job_id, cutoff_seq, semantics_version, status, initial_blockers, created_at) \
         VALUES (?1, ?2, ?3, 'active', ?4, ?5)",
        params![
            job,
            RESERVATION_CUTOFF_UNFROZEN,
            semantics_version,
            serde_json::to_string(initial_blockers)?,
            now
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Pin the frozen backfill cutoff. Guarded on the sentinel so a repeated
/// call (crash between commit and observation, or a racing recomputation)
/// can never advance an already-frozen frontier.
pub fn freeze_reservation_cutoff(conn: &Connection, id: i64, cutoff_seq: i64) -> Result<()> {
    debug_assert!(cutoff_seq != RESERVATION_CUTOFF_UNFROZEN);
    conn.execute(
        "UPDATE scheduler_reservation SET cutoff_seq = ?2 \
         WHERE id = ?1 AND cutoff_seq = ?3",
        params![id, cutoff_seq, RESERVATION_CUTOFF_UNFROZEN],
    )?;
    Ok(())
}

/// The backfill frontier of an active reservation, as scheduling interprets
/// it right now. Single source of truth for the coordinator and the views.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillFrontier {
    /// An initial blocker is still advancing: any equal-priority eligible job
    /// whose bypass is unconsumed may pass the protected job once.
    OpenWindow,
    /// The window closed but the sentinel is still stored; the next pass
    /// plans against this cutoff and pins it.
    FreezePending(i64),
    /// Pinned: jobs submitted after the cutoff may not pass.
    Frozen(i64),
    /// Unknown semantics version; ordinary admission must stay blocked.
    Unsupported(i64),
}

pub fn backfill_frontier(conn: &Connection, res: &ReservationRow) -> Result<BackfillFrontier> {
    match res.semantics_version {
        // v1 froze the cutoff at creation; a permanently frozen frontier at
        // the stored cutoff remains its exact meaning.
        1 => Ok(BackfillFrontier::Frozen(res.cutoff_seq)),
        2 | 3 | SCHEDULER_SEMANTICS_VERSION => {
            if res.cutoff_seq != RESERVATION_CUTOFF_UNFROZEN {
                Ok(BackfillFrontier::Frozen(res.cutoff_seq))
            } else if any_blockers_advancing(conn, &res.initial_blockers)? {
                Ok(BackfillFrontier::OpenWindow)
            } else {
                let cutoff = if res.semantics_version == SCHEDULER_SEMANTICS_VERSION {
                    max_ready_seq(conn)?
                } else {
                    max_job_seq(conn)?
                };
                Ok(BackfillFrontier::FreezePending(cutoff))
            }
        }
        version => Ok(BackfillFrontier::Unsupported(version)),
    }
}

/// True while any of the given attempts still holds an unreleased run lease
/// in a live-advancing state. Orphaned and quarantined attempts retain their
/// leases until resolved, but no longer advance: counting them would hold a
/// protected job's backfill window open indefinitely, admitting an unbounded
/// stream of later arrivals past it.
pub fn any_blockers_advancing(conn: &Connection, attempts: &[AttemptId]) -> Result<bool> {
    if attempts.is_empty() {
        return Ok(false);
    }
    let placeholders = vec!["?"; attempts.len()].join(",");
    let mut stmt = conn.prepare(&format!(
        "SELECT EXISTS (
             SELECT 1 FROM run_leases l
             JOIN attempts a ON a.id = l.attempt_id
             WHERE l.released_at IS NULL
               AND a.state IN ('prepared','authorized','running')
               AND l.attempt_id IN ({placeholders})
         )"
    ))?;
    Ok(
        stmt.query_row(rusqlite::params_from_iter(attempts.iter()), |row| {
            row.get(0)
        })?,
    )
}

pub fn resolve_reservation(
    conn: &Connection,
    id: i64,
    status: &str,
    reason: &str,
    now: i64,
) -> Result<()> {
    debug_assert!(status == "satisfied" || status == "invalidated");
    conn.execute(
        "UPDATE scheduler_reservation SET status = ?2, reason = ?3, resolved_at = ?4 \
         WHERE id = ?1",
        params![id, status, reason, now],
    )?;
    Ok(())
}

/// Record a consumed bypass. The unique reservation/job key enforces one
/// bypass per job across retries and restarts.
pub fn record_backfill(
    conn: &Connection,
    reservation: i64,
    job: JobId,
    reason: &str,
    attempt: Option<AttemptId>,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO scheduler_reservation_backfills \
         (reservation_id, job_id, reason, attempt_id, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![reservation, job, reason, attempt, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Operator pause
// ---------------------------------------------------------------------------

pub fn is_paused(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT paused FROM scheduler_pause WHERE singleton = 1",
        [],
        |row| Ok(row.get::<_, i64>(0)? != 0),
    )?)
}

/// Set the operator pause flag. Returns whether the stored value changed.
pub fn set_paused(conn: &Connection, paused: bool, now: i64) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE scheduler_pause
            SET paused = ?1, updated_at = ?2
          WHERE singleton = 1 AND paused != ?1",
        rusqlite::params![i64::from(paused), now],
    )? > 0;
    Ok(changed)
}

// ---------------------------------------------------------------------------
// Operations (idempotency)
// ---------------------------------------------------------------------------

pub fn lookup_operation(conn: &Connection, key: &str) -> Result<Option<(String, String)>> {
    Ok(conn
        .query_row(
            "SELECT request_hash, response FROM operations WHERE key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?)
}

pub fn compact_operation_response(conn: &Connection, key: &str, response: &str) -> Result<()> {
    conn.execute(
        "UPDATE operations SET response = ?2 WHERE key = ?1",
        params![key, response],
    )?;
    Ok(())
}

pub fn insert_operation(
    conn: &Connection,
    key: &str,
    op_type: &str,
    request_hash: &str,
    response: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO operations (key, op_type, request_hash, response, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![key, op_type, request_hash, response, now],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

pub fn latest_event_id(conn: &Connection) -> Result<i64> {
    Ok(
        conn.query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |row| {
            row.get(0)
        })?,
    )
}

pub fn events_after(conn: &Connection, after: i64, limit: u32) -> Result<Vec<EventRow>> {
    let mut stmt = conn.prepare(
        "SELECT e.id, e.ts, e.job_id, substr(j.name, 1, 80), e.attempt_id, a.number, e.type \
         FROM events e \
         LEFT JOIN jobs j ON j.id = e.job_id \
         LEFT JOIN attempts a ON a.id = e.attempt_id \
         WHERE e.id > ?1 ORDER BY e.id LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![after, limit], event_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// The existing events_job index includes the INTEGER PRIMARY KEY (rowid) as
/// its suffix, so both the job filter and cursor seek are indexed. Never scan
/// the global event stream to find a particular job's next page.
pub fn job_events_after(
    conn: &Connection,
    job: JobId,
    after: i64,
    limit: u32,
) -> Result<Vec<EventRow>> {
    let mut stmt = conn.prepare(
        "SELECT e.id, e.ts, e.job_id, substr(j.name, 1, 80), e.attempt_id, a.number, e.type \
         FROM events e \
         LEFT JOIN jobs j ON j.id = e.job_id \
         LEFT JOIN attempts a ON a.id = e.attempt_id \
         WHERE e.job_id = ?1 AND e.id > ?2 ORDER BY e.id LIMIT ?3",
    )?;
    let rows = stmt.query_map(params![job, after, limit], event_from_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        id: row.get(0)?,
        timestamp: row.get(1)?,
        job_id: row.get(2)?,
        job_name: row.get(3)?,
        attempt_id: row.get(4)?,
        attempt_number: row.get(5)?,
        event_type: row.get(6)?,
    })
}

pub fn append_event(
    conn: &Connection,
    job: Option<JobId>,
    attempt: Option<AttemptId>,
    event_type: &str,
    actor: &str,
    details: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO events (ts, job_id, attempt_id, type, actor, details) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![now_ms(), job, attempt, event_type, actor, details],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::DependencySpec;
    use std::collections::BTreeMap;

    fn submit_params(name: &str, limit: u32) -> SubmitParams {
        SubmitParams {
            name: name.into(),
            is_default: false,
            class: None,
            run_window: None,
            cwd: "/tmp".into(),
            args: vec!["true".into()],
            env: BTreeMap::new(),
            max_parallel_runs: limit,
            priority: 0,
            max_attempts: 1,
            retry_delay_ms: 0,
            time_limit_ms: None,
            dependencies: vec![],
        }
    }

    #[test]
    fn empty_database_migrates_to_current() {
        let conn = open_in_memory().unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, MIGRATIONS.len() as i64);
        // Idempotent.
        migrate(&conn).unwrap();
    }

    fn add_class(conn: &Connection, name: &str, window: Option<&str>) {
        conn.execute("INSERT INTO job_classes (name, run_window, created_at, updated_at) VALUES (?1, ?2, 0, 0)",
            params![name, window]).unwrap();
    }

    #[test]
    fn classes_and_both_windows_gate_admission() {
        const HOUR: i64 = 3_600_000;
        let conn = open_in_memory().unwrap();
        add_class(&conn, "ablations", Some("23:00-07:00@+00:00"));
        let mut p = submit_params("night", 4);
        p.class = Some("ablations".into());
        p.run_window = Some("01:00-05:00".parse().unwrap());
        let id = insert_job(&conn, &p, 0).unwrap();
        let row = job_row(&conn, id).unwrap().unwrap();
        assert_eq!(row.class.as_deref(), Some("ablations"));
        assert_eq!(row.run_window, p.run_window);
        assert_eq!(class_jobs(&conn, "ablations").unwrap().len(), 1);
        assert!(eligible_candidates(&conn, 23 * HOUR).unwrap().is_empty());
        assert_eq!(eligible_candidates(&conn, HOUR).unwrap()[0].job_id, id);
        assert_eq!(window_end_at(&conn, &row, HOUR).unwrap(), Some(5 * HOUR));
        assert!(eligible_candidates(&conn, 5 * HOUR).unwrap().is_empty());
        assert!(eligible_candidates(&conn, 12 * HOUR).unwrap().is_empty());
        conn.execute(
            "UPDATE job_classes SET paused = 1 WHERE name = 'ablations'",
            [],
        )
        .unwrap();
        assert_eq!(
            scheduling_reason(&conn, &row, HOUR).unwrap(),
            Some("class_paused")
        );
        assert!(eligible_candidates(&conn, HOUR).unwrap().is_empty());
    }

    #[test]
    fn closed_high_priority_window_does_not_block_other_classes() {
        let conn = open_in_memory().unwrap();
        add_class(&conn, "night", Some("23:00-07:00@+00:00"));
        let mut night = submit_params("night", 1);
        night.priority = 100;
        night.class = Some("night".into());
        let closed = insert_job(&conn, &night, 0).unwrap();
        let open = insert_job(&conn, &submit_params("open", 4), 1).unwrap();
        let candidates = eligible_candidates(&conn, 12 * 3_600_000).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].job_id, open);
        let reservation = crate::scheduler::ReservationSnapshot {
            job_id: closed,
            cutoff_seq: None,
            consumed: BTreeSet::new(),
        };
        let plan = crate::scheduler::plan_pass(&[], &candidates, Some(&reservation));
        assert!(plan.invalidate_reservation.is_some());
        assert_eq!(plan.starts[0].job_id, open);
    }

    #[test]
    fn attempt_snapshots_the_earliest_close_and_preserves_it() {
        const HOUR: i64 = 3_600_000;
        let conn = open_in_memory().unwrap();
        add_class(&conn, "night", Some("23:00-07:00@+00:00"));
        let mut p = submit_params("night", 2);
        p.class = Some("night".into());
        p.run_window = Some("22:00-04:00".parse().unwrap());
        let id = insert_job(&conn, &p, 0).unwrap();
        let row = job_row(&conn, id).unwrap().unwrap();
        assert!(create_attempt_with_lease(&conn, &row, "outside", 12 * HOUR).is_err());
        let attempt = create_attempt_with_lease(&conn, &row, "token", 23 * HOUR).unwrap();
        assert_eq!(attempt.window_end_at, Some(28 * HOUR));
        assert_eq!(
            attempt_row(&conn, attempt.id)
                .unwrap()
                .unwrap()
                .window_end_at,
            Some(28 * HOUR)
        );
        // A live time-limit edit, including clearing it, leaves the window bound.
        set_job_time_limit(&conn, id, None, 24 * HOUR).unwrap();
        assert_eq!(
            attempt_row(&conn, attempt.id)
                .unwrap()
                .unwrap()
                .window_end_at,
            Some(28 * HOUR)
        );
    }

    #[test]
    fn migration_preserves_existing_jobs_and_active_attempts() {
        let conn = Connection::open_in_memory().unwrap();
        configure(&conn).unwrap();
        for migration in &MIGRATIONS[..7] {
            conn.execute_batch(migration).unwrap();
        }
        conn.pragma_update(None, "user_version", 7).unwrap();
        conn.execute("INSERT INTO jobs (name, state, cwd, args, env, max_parallel_runs, max_attempts, retry_delay_ms, created_at, updated_at) VALUES ('old', 'running', '/unused', '[]', '{}', 1, 1, 0, 0, 0)", []).unwrap();
        conn.execute("INSERT INTO attempts (job_id, number, state, launch_token, max_parallel_runs, created_at) VALUES (1, 1, 'running', 'old-token', 1, 0)", []).unwrap();
        migrate(&conn).unwrap();
        let row = job_row(&conn, 1).unwrap().unwrap();
        assert_eq!(row.state, JobState::Running);
        assert!(row.class.is_none());
        assert!(row.run_window.is_none());
        assert!(
            attempt_row(&conn, 1)
                .unwrap()
                .unwrap()
                .window_end_at
                .is_none()
        );
    }

    #[test]
    fn file_database_migrates_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mlqueue.db");
        let id;
        {
            let conn = open(&path).unwrap();
            let mut params = submit_params("a", 1);
            params.priority = -2;
            id = insert_job(&conn, &params, 1000).unwrap();
        }
        let conn = open(&path).unwrap();
        let job = job_row(&conn, id).unwrap().unwrap();
        assert_eq!(job.name, "a");
        assert_eq!(job.state, JobState::Queued);
        assert_eq!(job.priority, -2);
        assert_eq!(job.time_limit_ms, None);
    }

    #[test]
    fn time_limit_is_persisted() {
        let conn = open_in_memory().unwrap();
        let mut params = submit_params("limited", 1);
        params.time_limit_ms = Some(12_345);
        let id = insert_job(&conn, &params, 1000).unwrap();
        assert_eq!(
            job_row(&conn, id).unwrap().unwrap().time_limit_ms,
            Some(12_345)
        );
    }

    #[test]
    fn set_job_time_limit_replaces_and_clears() {
        let conn = open_in_memory().unwrap();
        let id = insert_job(&conn, &submit_params("limited", 1), 1000).unwrap();
        set_job_time_limit(&conn, id, Some(5_000), 1001).unwrap();
        let job = job_row(&conn, id).unwrap().unwrap();
        assert_eq!(job.time_limit_ms, Some(5_000));
        assert_eq!(job.time_limit_set_at, Some(1001));
        assert_eq!(job.updated_at, 1001);
        set_job_time_limit(&conn, id, None, 1002).unwrap();
        let cleared = job_row(&conn, id).unwrap().unwrap();
        assert_eq!(cleared.time_limit_ms, None);
        assert_eq!(cleared.time_limit_set_at, Some(1002));
        assert!(set_job_time_limit(&conn, id, Some(0), 1003).is_err());
    }

    #[test]
    fn v3_database_migrates_existing_jobs_as_unlimited() {
        let conn = Connection::open_in_memory().unwrap();
        configure(&conn).unwrap();
        for migration in &MIGRATIONS[..3] {
            conn.execute_batch(migration).unwrap();
        }
        conn.pragma_update(None, "user_version", 3).unwrap();
        conn.execute_batch(
            r#"
            INSERT INTO jobs (
                name, state, cwd, args, env, max_parallel_runs, priority,
                max_attempts, retry_delay_ms, attempt_count, created_at, updated_at
            ) VALUES ('old', 'queued', '/tmp', '["true"]', '{}', 1, 0, 1, 0, 0, 1, 1);
            "#,
        )
        .unwrap();

        migrate(&conn).unwrap();
        assert_eq!(job_row(&conn, 1).unwrap().unwrap().time_limit_ms, None);
    }

    #[test]
    fn v4_migration_does_not_put_delayed_retries_before_a_frozen_frontier() {
        let conn = Connection::open_in_memory().unwrap();
        configure(&conn).unwrap();
        for migration in &MIGRATIONS[..4] {
            conn.execute_batch(migration).unwrap();
        }
        conn.pragma_update(None, "user_version", 4).unwrap();
        let retry_at = now_ms() + 60_000;
        conn.execute(
            "INSERT INTO jobs (
                id, name, state, cwd, args, env, max_parallel_runs, priority,
                max_attempts, retry_delay_ms, retry_not_before, attempt_count,
                created_at, updated_at
             ) VALUES (1, 'protected', 'queued', '/tmp', '[\"true\"]', '{}', 1, 0,
                       1, 0, NULL, 0, 1, 1),
                      (2, 'delayed-retry', 'queued', '/tmp', '[\"true\"]', '{}', 3, 0,
                       2, 60000, ?1, 1, 2, 2)",
            [retry_at],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO scheduler_reservation (
                job_id, cutoff_seq, semantics_version, status, initial_blockers, created_at
             ) VALUES (1, 2, 3, 'active', '[]', 3)",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();
        assert_eq!(job_row(&conn, 1).unwrap().unwrap().ready_seq, Some(1));
        assert_eq!(job_row(&conn, 2).unwrap().unwrap().ready_seq, None);
        let reservation = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(reservation.semantics_version, SCHEDULER_SEMANTICS_VERSION);
        assert_eq!(reservation.cutoff_seq, 2);

        advance_dependency_graph(&conn, retry_at - 1).unwrap();
        assert_eq!(job_row(&conn, 2).unwrap().unwrap().ready_seq, None);
        advance_dependency_graph(&conn, retry_at).unwrap();
        assert!(
            job_row(&conn, 2).unwrap().unwrap().ready_seq.unwrap() > reservation.cutoff_seq,
            "a retry becoming ready after migration must not bypass the frozen frontier"
        );
    }

    #[test]
    fn set_job_priority_updates_row() {
        let conn = open_in_memory().unwrap();
        let id = insert_job(&conn, &submit_params("p", 1), 1000).unwrap();
        set_job_priority(&conn, id, 4, 1001).unwrap();
        assert_eq!(job_row(&conn, id).unwrap().unwrap().priority, 4);
        set_job_priority(&conn, id, -3, 1002).unwrap();
        assert_eq!(job_row(&conn, id).unwrap().unwrap().priority, -3);
    }

    #[test]
    fn pause_flag_defaults_off_and_is_idempotent() {
        let conn = open_in_memory().unwrap();
        assert!(!is_paused(&conn).unwrap());
        assert!(set_paused(&conn, true, 1).unwrap());
        assert!(is_paused(&conn).unwrap());
        assert!(!set_paused(&conn, true, 2).unwrap());
        assert!(is_paused(&conn).unwrap());
        assert!(set_paused(&conn, false, 3).unwrap());
        assert!(!is_paused(&conn).unwrap());
        assert!(!set_paused(&conn, false, 4).unwrap());
    }

    #[test]
    fn latest_job_id_tracks_max_rowid() {
        let conn = open_in_memory().unwrap();
        assert_eq!(latest_job_id(&conn).unwrap(), None);
        let first = insert_job(&conn, &submit_params("a", 1), 1).unwrap();
        let second = insert_job(&conn, &submit_params("b", 1), 2).unwrap();
        assert_eq!(latest_job_id(&conn).unwrap(), Some(second));
        assert!(second > first);
    }

    #[test]
    fn v1_database_migrates_priority_and_active_reservation_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mlqueue-v1.db");
        {
            let conn = Connection::open(&path).unwrap();
            configure(&conn).unwrap();
            conn.execute_batch(MIGRATIONS[0]).unwrap();
            conn.pragma_update(None, "user_version", 1).unwrap();
            conn.execute_batch(
                r#"
                INSERT INTO jobs (
                    name, state, cwd, args, env, max_parallel_runs, max_attempts,
                    retry_delay_ms, attempt_count, created_at, updated_at
                ) VALUES ('old', 'queued', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 1, 1);
                INSERT INTO scheduler_reservation (
                    job_id, cutoff_seq, semantics_version, status, initial_blockers, created_at
                ) VALUES (1, 0, 2, 'active', '[]', 1);
                "#,
            )
            .unwrap();
        }

        let conn = open(&path).unwrap();
        assert_eq!(job_row(&conn, 1).unwrap().unwrap().priority, 0);
        assert_eq!(
            active_reservation(&conn)
                .unwrap()
                .unwrap()
                .semantics_version,
            SCHEDULER_SEMANTICS_VERSION
        );
    }

    #[test]
    fn v2_migration_never_releases_unresolved_dependency_jobs() {
        let conn = Connection::open_in_memory().unwrap();
        configure(&conn).unwrap();
        conn.execute_batch(MIGRATIONS[0]).unwrap();
        conn.execute_batch(MIGRATIONS[1]).unwrap();
        conn.pragma_update(None, "user_version", 2).unwrap();
        conn.execute_batch(
            r#"
            INSERT INTO jobs (
                id, name, state, cwd, args, env, max_parallel_runs, max_attempts,
                retry_delay_ms, attempt_count, created_at, updated_at
            ) VALUES
                (1, 'running-parent', 'running', '/tmp', '["true"]', '{}', 1, 1, 0, 1, 1, 10),
                (2, 'waiting-success', 'queued', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 2, 20),
                (3, 'waiting-held', 'held', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 3, 30),
                (4, 'succeeded-parent', 'succeeded', '/tmp', '["true"]', '{}', 1, 1, 0, 1, 4, 40),
                (5, 'ready-success', 'queued', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 5, 50),
                (6, 'failed-parent', 'failed', '/tmp', '["true"]', '{}', 1, 1, 0, 1, 6, 60),
                (7, 'ready-completion', 'queued', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 7, 70),
                (8, 'impossible-success', 'queued', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 8, 80),
                (9, 'historical-skip', 'skipped', '/tmp', '["true"]', '{}', 1, 1, 0, 0, 9, 90);
            INSERT INTO dependencies (parent_id, child_id, requirement) VALUES
                (1, 2, 'success'),
                (1, 3, 'completion'),
                (4, 5, 'success'),
                (6, 7, 'completion'),
                (6, 8, 'success');
            "#,
        )
        .unwrap();

        migrate(&conn).unwrap();

        for id in [2, 3, 8] {
            let job = job_row(&conn, id).unwrap().unwrap();
            assert_eq!(
                job.state,
                JobState::Cancelled,
                "job {id} must not become runnable"
            );
            assert_eq!(
                job.state_reason.as_deref(),
                Some("dependency feature removed before prerequisites completed")
            );
            assert_eq!(job.finished_at, Some(job.updated_at));
            assert!(
                job.updated_at > 80,
                "job {id} needs an upgrade-time timestamp"
            );
        }
        assert_eq!(job_row(&conn, 5).unwrap().unwrap().state, JobState::Queued);
        assert_eq!(job_row(&conn, 7).unwrap().unwrap().state, JobState::Queued);
        assert_eq!(job_row(&conn, 9).unwrap().unwrap().state, JobState::Skipped);
        let dependencies_exist: bool = conn
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'dependencies')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            dependencies_exist,
            "the restored schema recreates an empty edge table"
        );
        let restored_edges: i64 = conn
            .query_row("SELECT COUNT(*) FROM dependencies", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            restored_edges, 0,
            "removed legacy edges cannot be reconstructed"
        );
        let migration_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE type = 'job_cancelled' AND actor = 'migration'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migration_events, 3);
    }

    #[test]
    fn eligible_candidates_order_by_priority_then_fifo() {
        let conn = open_in_memory().unwrap();
        let mut low = submit_params("low", 1);
        low.priority = -1;
        let low = insert_job(&conn, &low, 1000).unwrap();
        let default_first = insert_job(&conn, &submit_params("default-first", 1), 1001).unwrap();
        let default_second = insert_job(&conn, &submit_params("default-second", 1), 1002).unwrap();
        let mut high = submit_params("high", 1);
        high.priority = 1;
        let high = insert_job(&conn, &high, 1003).unwrap();

        let candidates = eligible_candidates(&conn, 2000).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.job_id)
                .collect::<Vec<_>>(),
            vec![high, default_first, default_second, low]
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.priority)
                .collect::<Vec<_>>(),
            vec![1, 0, 0, -1]
        );
    }

    #[test]
    fn dependencies_latch_outcomes_cascade_skips_and_assign_readiness_order() {
        let conn = open_in_memory().unwrap();
        let parent = insert_job(&conn, &submit_params("parent", 1), 1000).unwrap();

        let mut child_params = submit_params("child", 1);
        child_params.dependencies = vec![DependencySpec {
            job: parent,
            condition: DependencyCondition::Succeeded,
        }];
        let child = insert_job(&conn, &child_params, 1001).unwrap();

        let mut grandchild_params = submit_params("grandchild", 1);
        grandchild_params.dependencies = vec![DependencySpec {
            job: child,
            condition: DependencyCondition::Succeeded,
        }];
        let grandchild = insert_job(&conn, &grandchild_params, 1002).unwrap();

        let mut cleanup_params = submit_params("cleanup", 1);
        cleanup_params.dependencies = vec![DependencySpec {
            job: parent,
            condition: DependencyCondition::Terminal,
        }];
        let cleanup = insert_job(&conn, &cleanup_params, 1003).unwrap();
        let independent = insert_job(&conn, &submit_params("independent", 1), 1004).unwrap();

        assert!(job_row(&conn, child).unwrap().unwrap().ready_seq.is_none());
        assert!(
            job_row(&conn, cleanup)
                .unwrap()
                .unwrap()
                .ready_seq
                .is_none()
        );
        update_job_state(&conn, parent, JobState::Failed, Some("test failure"), 2000).unwrap();
        advance_dependency_graph(&conn, 2000).unwrap();

        assert_eq!(
            job_row(&conn, child).unwrap().unwrap().state,
            JobState::Skipped
        );
        assert_eq!(
            job_row(&conn, grandchild).unwrap().unwrap().state,
            JobState::Skipped
        );
        let cleanup_row = job_row(&conn, cleanup).unwrap().unwrap();
        assert_eq!(cleanup_row.state, JobState::Queued);
        assert!(
            cleanup_row.ready_seq.unwrap()
                > job_row(&conn, independent)
                    .unwrap()
                    .unwrap()
                    .ready_seq
                    .unwrap(),
            "dependency-delayed work joins FIFO when it becomes ready"
        );

        let child_edge = dependencies_of(&conn, child).unwrap().pop().unwrap();
        assert_eq!(child_edge.resolution, Some(DependencyResolution::Violated));
        assert_eq!(child_edge.parent_outcome, Some(JobState::Failed));
        let cleanup_edge = dependencies_of(&conn, cleanup).unwrap().pop().unwrap();
        assert_eq!(
            cleanup_edge.resolution,
            Some(DependencyResolution::Satisfied)
        );

        // Manual retry is non-monotonic job state, but observed dependency
        // outcomes are immutable: descendants neither revive nor retract.
        update_job_state(&conn, parent, JobState::Queued, None, 3000).unwrap();
        advance_dependency_graph(&conn, 3000).unwrap();
        assert_eq!(
            job_row(&conn, child).unwrap().unwrap().state,
            JobState::Skipped
        );
        assert_eq!(
            dependencies_of(&conn, cleanup).unwrap()[0].resolution,
            Some(DependencyResolution::Satisfied)
        );
    }

    #[test]
    fn duplicate_dependency_declarations_keep_succeeded_condition() {
        let conn = open_in_memory().unwrap();
        let parent = insert_job(&conn, &submit_params("parent", 1), 1000).unwrap();
        let mut child = submit_params("child", 1);
        child.dependencies = vec![
            DependencySpec {
                job: parent,
                condition: DependencyCondition::Terminal,
            },
            DependencySpec {
                job: parent,
                condition: DependencyCondition::Succeeded,
            },
            DependencySpec {
                job: parent,
                condition: DependencyCondition::Terminal,
            },
        ];
        let child = insert_job(&conn, &child, 1001).unwrap();
        let dependencies = dependencies_of(&conn, child).unwrap();
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0].condition, DependencyCondition::Succeeded);
    }

    #[test]
    fn attempt_and_lease_lifecycle() {
        let conn = open_in_memory().unwrap();
        let id = insert_job(&conn, &submit_params("a", 3), 1000).unwrap();
        let job = job_row(&conn, id).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &job, "token", 1001).unwrap();
        assert_eq!(attempt.number, 1);
        assert_eq!(attempt.max_parallel_runs, 3);
        assert_eq!(active_leases(&conn).unwrap().len(), 1);
        assert_eq!(
            job_row(&conn, id).unwrap().unwrap().state,
            JobState::Starting
        );

        set_active_attempt_max_parallel_runs(&conn, attempt.id, 4).unwrap();
        set_job_max_parallel_runs(&conn, id, 4, 1002).unwrap();
        assert_eq!(
            attempt_row(&conn, attempt.id)
                .unwrap()
                .unwrap()
                .max_parallel_runs,
            4
        );
        assert_eq!(active_leases(&conn).unwrap()[0].1.limit, 4);
        assert_eq!(job_row(&conn, id).unwrap().unwrap().max_parallel_runs, 4);

        release_lease(&conn, attempt.id, 1003).unwrap();
        assert!(active_leases(&conn).unwrap().is_empty());
        assert!(set_active_attempt_max_parallel_runs(&conn, attempt.id, 5).is_err());
        assert_eq!(
            attempt_row(&conn, attempt.id)
                .unwrap()
                .unwrap()
                .max_parallel_runs,
            4
        );
    }

    #[test]
    fn eligible_candidates_respect_retry_delay() {
        let conn = open_in_memory().unwrap();
        let job = insert_job(&conn, &submit_params("retry", 1), 1000).unwrap();
        set_job_retry_not_before(&conn, job, Some(9000), 1001).unwrap();
        assert!(eligible_candidates(&conn, 3000).unwrap().is_empty());
        assert_eq!(eligible_candidates(&conn, 9000).unwrap().len(), 1);
    }

    #[test]
    fn single_active_reservation_enforced_and_restored() {
        let conn = open_in_memory().unwrap();
        let a = insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let b = insert_job(&conn, &submit_params("b", 1), 1001).unwrap();
        let res = create_reservation(&conn, a, SCHEDULER_SEMANTICS_VERSION, &[7], 1002).unwrap();
        assert!(create_reservation(&conn, b, SCHEDULER_SEMANTICS_VERSION, &[], 1003).is_err());

        record_backfill(&conn, res, b, "admitted", None, 1004).unwrap();
        // Second consumption attempt is ignored, not duplicated.
        record_backfill(&conn, res, b, "admitted", None, 1005).unwrap();

        let restored = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(restored.job_id, a);
        assert_eq!(restored.cutoff_seq, RESERVATION_CUTOFF_UNFROZEN);
        assert_eq!(restored.initial_blockers, vec![7]);
        assert_eq!(restored.consumed, BTreeSet::from([b]));

        resolve_reservation(&conn, res, "satisfied", "protected job started", 2000).unwrap();
        assert!(active_reservation(&conn).unwrap().is_none());
    }

    #[test]
    fn freezing_the_cutoff_is_idempotent() {
        let conn = open_in_memory().unwrap();
        let a = insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let res = create_reservation(&conn, a, SCHEDULER_SEMANTICS_VERSION, &[7], 1001).unwrap();

        freeze_reservation_cutoff(&conn, res, 9).unwrap();
        assert_eq!(active_reservation(&conn).unwrap().unwrap().cutoff_seq, 9);
        // A later attempt (crash replay, racing recomputation) must never
        // advance an already-frozen frontier.
        freeze_reservation_cutoff(&conn, res, 42).unwrap();
        assert_eq!(active_reservation(&conn).unwrap().unwrap().cutoff_seq, 9);
    }

    #[test]
    fn backfill_frontier_interprets_versions_and_window_state() {
        let conn = open_in_memory().unwrap();
        let blocker_job = insert_job(&conn, &submit_params("blocker", 2), 1000).unwrap();
        let row = job_row(&conn, blocker_job).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &row, "token", 1001).unwrap();
        let protected = insert_job(&conn, &submit_params("protected", 1), 1002).unwrap();

        // Native version with a live blocker: the window is open.
        let rid = create_reservation(
            &conn,
            protected,
            SCHEDULER_SEMANTICS_VERSION,
            &[attempt.id],
            1003,
        )
        .unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::OpenWindow
        );

        // Blocker drained: a freeze at the current maximum sequence is
        // pending, then pinning makes it durable.
        release_lease(&conn, attempt.id, 1004).unwrap();
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::FreezePending(protected)
        );
        freeze_reservation_cutoff(&conn, rid, protected).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::Frozen(protected)
        );
        resolve_reservation(&conn, rid, "invalidated", "test", 1005).unwrap();

        // A v1 reservation stays frozen at its stored cutoff even while a
        // blocker still runs: it never gains an open window retroactively.
        let row = job_row(&conn, blocker_job).unwrap().unwrap();
        let live = create_attempt_with_lease(&conn, &row, "token-2", 1006).unwrap();
        let rid = create_reservation(&conn, protected, 1, &[live.id], 1007).unwrap();
        freeze_reservation_cutoff(&conn, rid, 1).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert!(any_blockers_advancing(&conn, &res.initial_blockers).unwrap());
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::Frozen(1)
        );
        resolve_reservation(&conn, rid, "invalidated", "test", 1008).unwrap();

        // Unknown versions must surface as unsupported, never as a guess.
        create_reservation(&conn, protected, 99, &[], 1009).unwrap();
        let res = active_reservation(&conn).unwrap().unwrap();
        assert_eq!(
            backfill_frontier(&conn, &res).unwrap(),
            BackfillFrontier::Unsupported(99)
        );
    }

    #[test]
    fn blockers_advance_only_while_leased_in_a_live_state() {
        let conn = open_in_memory().unwrap();
        let job = insert_job(&conn, &submit_params("blocker", 2), 1000).unwrap();
        let row = job_row(&conn, job).unwrap().unwrap();
        let attempt = create_attempt_with_lease(&conn, &row, "token", 1001).unwrap();

        assert!(!any_blockers_advancing(&conn, &[]).unwrap());
        assert!(any_blockers_advancing(&conn, &[attempt.id]).unwrap());

        // Orphaned/quarantined attempts keep their lease but stop advancing:
        // the backfill window must not stay open on their account.
        set_attempt_state(&conn, attempt.id, AttemptState::Orphaned, None).unwrap();
        assert!(!any_blockers_advancing(&conn, &[attempt.id]).unwrap());

        set_attempt_state(&conn, attempt.id, AttemptState::Running, None).unwrap();
        assert!(any_blockers_advancing(&conn, &[attempt.id]).unwrap());
        release_lease(&conn, attempt.id, 2000).unwrap();
        assert!(!any_blockers_advancing(&conn, &[attempt.id]).unwrap());
    }

    #[test]
    fn job_sequence_never_decreases_after_deleting_the_newest_job() {
        let conn = open_in_memory().unwrap();
        insert_job(&conn, &submit_params("a", 1), 1000).unwrap();
        let b = insert_job(&conn, &submit_params("b", 1), 1001).unwrap();
        assert_eq!(max_job_seq(&conn).unwrap(), b);
        // Retention-style deletion of the highest row must not lower the
        // frozen backfill cutoff source.
        conn.execute("DELETE FROM jobs WHERE id = ?1", [b]).unwrap();
        assert_eq!(max_job_seq(&conn).unwrap(), b);
        let c = insert_job(&conn, &submit_params("c", 1), 1002).unwrap();
        assert!(c > b, "ids are never reused");
    }

    #[test]
    fn operations_are_idempotent_and_conflict_on_reuse() {
        let conn = open_in_memory().unwrap();
        insert_operation(&conn, "k1", "submit", "hash-a", "{\"jobId\":1}", 1000).unwrap();
        let (hash, response) = lookup_operation(&conn, "k1").unwrap().unwrap();
        assert_eq!(hash, "hash-a");
        assert_eq!(response, "{\"jobId\":1}");
        // Reusing the key is a constraint violation; the coordinator turns a
        // hash mismatch into a stable conflict error before inserting.
        assert!(insert_operation(&conn, "k1", "submit", "hash-b", "{}", 1001).is_err());
        assert!(lookup_operation(&conn, "missing").unwrap().is_none());
    }

    #[test]
    fn events_are_read_from_a_durable_monotonic_cursor() {
        let conn = open_in_memory().unwrap();
        let job = insert_job(&conn, &submit_params("event-job", 1), 1000).unwrap();
        append_event(&conn, Some(job), None, "job_submitted", "client", None).unwrap();
        append_event(&conn, Some(job), None, "job_succeeded", "daemon", None).unwrap();

        assert_eq!(latest_event_id(&conn).unwrap(), 2);
        let events = events_after(&conn, 0, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 1);
        assert_eq!(events[0].job_name.as_deref(), Some("event-job"));
        let events = events_after(&conn, events[0].id, 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, 2);
        assert_eq!(events[0].event_type, "job_succeeded");
    }

    #[test]
    fn job_event_pages_preserve_transitions_across_unrelated_events() {
        let conn = open_in_memory().unwrap();
        let job = insert_job(&conn, &submit_params("watched", 1), 1000).unwrap();
        let other = insert_job(&conn, &submit_params("other", 1), 1000).unwrap();
        let transitions = [
            "attempt_starting",
            "attempt_running",
            "attempt_failed",
            "job_retry_scheduled",
            "attempt_starting",
            "attempt_running",
            "job_succeeded",
        ];
        let mut expected_ids = Vec::new();
        for event in transitions {
            append_event(&conn, Some(other), None, "job_submitted", "client", None).unwrap();
            append_event(&conn, Some(job), None, event, "daemon", None).unwrap();
            expected_ids.push(latest_event_id(&conn).unwrap());
            append_event(&conn, None, None, "queue_paused", "client", None).unwrap();
        }
        let mut cursor = 0;
        let mut actual = Vec::new();
        loop {
            let page = job_events_after(&conn, job, cursor, 2).unwrap();
            assert!(page.len() <= 2);
            if page.is_empty() {
                break;
            }
            for event in page {
                assert_eq!(event.job_id, Some(job));
                cursor = event.id;
                actual.push((event.id, event.event_type));
            }
        }
        assert_eq!(
            actual,
            expected_ids
                .into_iter()
                .zip(transitions.map(str::to_string))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn notification_queries_bound_job_names() {
        let conn = open_in_memory().unwrap();
        let long_name = "x".repeat(2_000);
        let job = insert_job(&conn, &submit_params(&long_name, 1), 1000).unwrap();
        update_job_state(&conn, job, JobState::Running, None, 1001).unwrap();
        append_event(&conn, Some(job), None, "attempt_running", "runner", None).unwrap();

        let names = bounded_job_names_in_state(&conn, JobState::Running, 80, 5).unwrap();
        assert_eq!(names[0].1.chars().count(), 80);
        let events = events_after(&conn, 0, 256).unwrap();
        assert_eq!(events[0].job_name.as_ref().unwrap().chars().count(), 80);
    }
}
