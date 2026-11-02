//! Builders for the stable public JSON views. Status must explain the
//! declared limits, the effective limit, protection, and why each queued
//! job is not running.

use std::cmp::Ordering;

use anyhow::Result;
use rusqlite::Connection;

use crate::db::{self, AttemptRow, EventRow, JobRow, ReservationRow};
use crate::domain::{JobState, now_ms};
use crate::paths::Paths;
use crate::protocol::{
    AttemptView, DependencyView, EventView, FollowTtsJobView, FollowTtsSnapshotView, JobView,
    Reply, ReservationView, StatusView,
};

const TTS_NAME_LIMIT: usize = 80;
pub const JOB_ATTEMPT_LIMIT: u32 = 25;

/// Keep mutation results independent of a recurring job's history length.
pub fn compact_mutation_history(reply: &mut Reply) -> bool {
    if let Reply::Job { job } | Reply::Submitted { job } = reply
        && (!job.attempts.is_empty() || job.attempts_truncated)
    {
        job.attempts.clear();
        job.attempts_truncated = false;
        return true;
    }
    false
}

fn status_job_order(a: &JobView, b: &JobView) -> Ordering {
    fn group(job: &JobView) -> u8 {
        if job.finished_at.is_some() {
            4
        } else {
            match job.state.as_str() {
                "running" => 0,
                "starting" => 1,
                "needs_attention" => 2,
                _ => 3,
            }
        }
    }

    group(a).cmp(&group(b)).then_with(|| a.id.cmp(&b.id))
}

fn tts_name(name: &str) -> String {
    if name.chars().count() <= TTS_NAME_LIMIT {
        name.to_string()
    } else {
        format!(
            "{}…",
            name.chars().take(TTS_NAME_LIMIT - 1).collect::<String>()
        )
    }
}

pub fn event_view(event: &EventRow) -> EventView {
    EventView {
        id: event.id,
        timestamp: event.timestamp,
        job: event.job_id,
        job_name: event.job_name.as_deref().map(tts_name),
        attempt: event.attempt_id,
        attempt_number: event.attempt_number,
        event_type: event.event_type.clone(),
    }
}

pub fn attempt_view(paths: &Paths, attempt: &AttemptRow) -> AttemptView {
    AttemptView {
        id: attempt.id,
        job_id: attempt.job_id,
        number: attempt.number,
        state: attempt.state.as_str().to_string(),
        max_parallel_runs: attempt.max_parallel_runs,
        window_end_at: attempt.window_end_at,
        exit_code: attempt.exit_code,
        term_signal: attempt.term_signal,
        message: attempt.message.clone(),
        created_at: attempt.created_at,
        finished_at: attempt.finished_at,
        log_dir: paths.attempt_dir(attempt.id).display().to_string(),
    }
}

/// Derived (never persisted) explanation for why a non-terminal job is not
/// running right now.
fn eligibility_reason(
    conn: &Connection,
    job: &JobRow,
    reservation: Option<&ReservationRow>,
    now: i64,
) -> Result<Option<String>> {
    let reason = match job.state {
        JobState::Held => Some("held".to_string()),
        JobState::Starting => Some("launching".to_string()),
        JobState::NeedsAttention => Some("needs_recovery".to_string()),
        JobState::Queued => {
            if job.retry_not_before.is_some_and(|at| at > now) {
                Some("waiting_for_retry_delay".to_string())
            } else if let parents = db::unresolved_dependency_parents(conn, job.id)?
                && !parents.is_empty()
            {
                Some(format!("waiting_for_dependencies: jobs {parents:?}"))
            } else if db::is_paused(conn)? {
                Some("paused".to_string())
            } else if let Some(reason) = db::scheduling_reason(conn, job, now)? {
                Some(reason.to_string())
            } else if job.is_default
                && (!db::active_leases(conn)?.is_empty()
                    || db::eligible_candidates(conn, now)?
                        .iter()
                        .any(|candidate| !candidate.is_default))
            {
                Some("default_waiting_for_idle".to_string())
            } else if let Some(res) = reservation {
                let frontier = db::backfill_frontier(conn, res)?;
                let protected = db::job_row(conn, res.job_id)?;
                if matches!(frontier, db::BackfillFrontier::Unsupported(_)) {
                    Some("admission_blocked".to_string())
                } else if res.job_id == job.id {
                    Some("protected_drain".to_string())
                } else if let Some(protected) = &protected
                    && job.priority > protected.priority
                {
                    Some(format!(
                        "supersedes_lower_priority: job {} (pri {})",
                        protected.id, protected.priority
                    ))
                } else if let Some(protected) = &protected
                    && job.priority < protected.priority
                {
                    Some(format!(
                        "waiting_for_higher_priority: job {} (pri {})",
                        protected.id, protected.priority
                    ))
                } else if res.consumed.contains(&job.id) {
                    Some(format!("backfill_bypass_consumed: job {}", res.job_id))
                } else {
                    match frontier {
                        db::BackfillFrontier::OpenWindow => {
                            Some(format!("backfill_window_open: job {}", res.job_id))
                        }
                        db::BackfillFrontier::Frozen(cutoff)
                        | db::BackfillFrontier::FreezePending(cutoff) => {
                            if job.ready_seq.is_none_or(|ready_seq| ready_seq > cutoff) {
                                Some(format!("behind_backfill_cutoff: job {}", res.job_id))
                            } else {
                                Some(format!("backfill_eligible: job {}", res.job_id))
                            }
                        }
                        db::BackfillFrontier::Unsupported(_) => unreachable!(
                            "unsupported reservation handled before priority explanations"
                        ),
                    }
                }
            } else {
                Some("waiting_for_slot".to_string())
            }
        }
        _ => None,
    };
    Ok(reason)
}

pub fn job_view(
    conn: &Connection,
    paths: &Paths,
    job: &JobRow,
    include_attempts: bool,
) -> Result<JobView> {
    let reservation = db::active_reservation(conn)?;
    let attempts: Vec<_> = if include_attempts {
        db::attempts_page(conn, job.id, None, JOB_ATTEMPT_LIMIT)?
            .iter()
            .rev()
            .map(|attempt| attempt_view(paths, attempt))
            .collect()
    } else {
        Vec::new()
    };
    let cancel_requested = db::live_attempt_for_job(conn, job.id)?
        .map(|attempt| attempt.cancel_requested)
        .filter(|&requested| requested);
    let dependencies = if include_attempts {
        db::dependencies_of(conn, job.id)?
            .into_iter()
            .map(|dependency| DependencyView {
                parent: dependency.parent_id,
                condition: dependency.condition,
                parent_state: dependency.parent_state.as_str().to_string(),
                resolution: dependency.resolution,
                parent_outcome: dependency
                    .parent_outcome
                    .map(|outcome| outcome.as_str().to_string()),
                resolved_at: dependency.resolved_at,
            })
            .collect()
    } else {
        Vec::new()
    };
    Ok(JobView {
        id: job.id,
        name: job.name.clone(),
        is_default: job.is_default,
        class: job.class.clone(),
        run_window: job.run_window.clone(),
        class_run_window: job
            .class
            .as_deref()
            .map(|name| db::class_row(conn, name))
            .transpose()?
            .flatten()
            .and_then(|class| class.run_window),
        state: job.state.as_str().to_string(),
        eligibility: eligibility_reason(conn, job, reservation.as_ref(), now_ms())?,
        state_reason: job.state_reason.clone(),
        max_parallel_runs: job.max_parallel_runs,
        priority: job.priority,
        cwd: job.cwd.clone(),
        args: job.args.clone(),
        max_attempts: job.max_attempts as u32,
        retry_delay_ms: job.retry_delay_ms as u64,
        time_limit_ms: job.time_limit_ms.map(|value| value as u64),
        retry_not_before: job.retry_not_before,
        ready_sequence: job.ready_seq,
        attempt_count: job.attempt_count,
        created_at: job.created_at,
        updated_at: job.updated_at,
        finished_at: job.finished_at,
        dependencies,
        attempts_truncated: include_attempts && job.attempt_count > attempts.len() as i64,
        attempts,
        cancel_requested,
    })
}

pub fn status_view(
    conn: &Connection,
    paths: &Paths,
    admission_blocked: bool,
) -> Result<StatusView> {
    let leases = db::active_leases(conn)?;
    let reservation = db::active_reservation(conn)?;
    let mut jobs = db::status_jobs(conn, 25)?
        .iter()
        .map(|job| job_view(conn, paths, job, false))
        .collect::<Result<Vec<_>>>()?;
    // Keep operational work ahead of recent terminal history. The machine
    // stream includes finished jobs for context, but a consumer inspecting the
    // first entry should not mistake that history for the active run.
    jobs.sort_by(status_job_order);
    Ok(StatusView {
        jobs,
        active_leases: leases.len() as u32,
        effective_limit: leases.iter().map(|(_, lease)| lease.limit).min(),
        reservation: match reservation {
            Some(res) => {
                let (window_open, cutoff) = match db::backfill_frontier(conn, &res)? {
                    db::BackfillFrontier::OpenWindow => (true, None),
                    db::BackfillFrontier::Frozen(cutoff)
                    | db::BackfillFrontier::FreezePending(cutoff) => (false, Some(cutoff)),
                    db::BackfillFrontier::Unsupported(_) => (false, None),
                };
                Some(ReservationView {
                    protected_job: res.job_id,
                    backfill_window_open: window_open,
                    backfill_cutoff: cutoff,
                    created_at: res.created_at,
                    blocking_attempts: leases.iter().map(|(attempt, _)| *attempt).collect(),
                    consumed_bypasses: res.consumed.iter().copied().collect(),
                })
            }
            None => None,
        },
        admission_blocked,
        paused: db::is_paused(conn)?,
    })
}

pub fn follow_tts_snapshot(conn: &Connection) -> Result<FollowTtsSnapshotView> {
    const MAX_INITIAL_RUNNING: usize = 5;

    let running_count = db::count_jobs_in_state(conn, JobState::Running)?;
    let additional_running_jobs = running_count.saturating_sub(MAX_INITIAL_RUNNING as u32);
    let running_jobs = db::bounded_job_names_in_state(
        conn,
        JobState::Running,
        TTS_NAME_LIMIT as u32,
        MAX_INITIAL_RUNNING as u32,
    )?
    .into_iter()
    .map(|(id, name)| FollowTtsJobView {
        id,
        name: tts_name(&name),
    })
    .collect();
    Ok(FollowTtsSnapshotView {
        running_jobs,
        additional_running_jobs,
        latest_event_id: db::latest_event_id(conn)?,
    })
}
