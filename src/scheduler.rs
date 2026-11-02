//! Pure scheduling policy: the symmetric `maxParallelRuns` compatibility
//! formula plus protected-job backfill with a blocker-scoped window. While
//! any attempt that originally blocked the protected job is still advancing,
//! any eligible job may bypass it once; when the last original blocker
//! drains, the frontier freezes and later submissions can no longer pass.
//!
//! This module is deliberately independent of Tokio, SQLite, and process
//! code. The coordinator feeds it a snapshot and commits its decisions in one
//! transaction.

use std::collections::BTreeSet;

use crate::domain::JobId;

/// An attempt that currently holds a run lease.
#[derive(Debug, Clone)]
pub struct ActiveLease {
    pub job_id: JobId,
    pub limit: u32,
}

/// A job eligible to start: prerequisites satisfied, queued, not held, and
/// retry delay elapsed, class not paused, and all daily run windows open.
/// Must be supplied in priority/readiness-FIFO order.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub job_id: JobId,
    pub ready_seq: i64,
    pub limit: u32,
    /// Higher values rank first. Readiness sequence is the FIFO tiebreaker.
    pub priority: i64,
    pub is_default: bool,
}

/// The persisted protected-job reservation, if one is active.
#[derive(Debug, Clone)]
pub struct ReservationSnapshot {
    pub job_id: JobId,
    /// `None` while the backfill window is open: an initial blocker of the
    /// protected job is still advancing, so any equal-priority eligible job
    /// may consume its single bypass regardless of readiness order. `Some`
    /// once the frontier is frozen; jobs becoming ready after the cutoff may
    /// never pass. The coordinator derives this from blocker liveness and the
    /// persisted cutoff.
    pub cutoff_seq: Option<i64>,
    /// Jobs whose single backfill bypass is already consumed (seeded with the
    /// jobs active at creation plus every backfill admitted since).
    pub consumed: BTreeSet<JobId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Start {
    pub job_id: JobId,
    /// When this start is a backfill that bypasses a protected job, the
    /// protected job's ID. One pass can satisfy an old reservation and create
    /// a new one, so the flag alone would not say which reservation consumed
    /// the bypass.
    pub bypasses: Option<JobId>,
}

impl Start {
    pub fn consumes_bypass(&self) -> bool {
        self.bypasses.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReservation {
    pub job_id: JobId,
    /// Jobs seeded as already-consumed: everything holding a (shadow) lease
    /// when protection was created, so their retries cannot become fresh
    /// backfills.
    pub initial_consumed: BTreeSet<JobId>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassOutcome {
    /// Jobs to start, in admission order.
    pub starts: Vec<Start>,
    /// The pre-existing reservation's protected job started this pass.
    pub satisfy_reservation: bool,
    /// The pre-existing reservation is no longer meaningful.
    pub invalidate_reservation: Option<&'static str>,
    /// A new reservation to persist (the head job could not start).
    pub create_reservation: Option<NewReservation>,
}

/// The admission formula: candidate `j` may acquire a lease exactly when
/// `|R| + 1 <= min(j.limit, r.limit for r in R)`.
pub fn can_admit(active_limits: &[u32], candidate_limit: u32) -> bool {
    let resulting = active_limits.len() as u64 + 1;
    resulting <= u64::from(candidate_limit)
        && active_limits.iter().all(|&l| resulting <= u64::from(l))
}

/// One greedy scheduling pass over a consistent snapshot.
///
/// `eligible` must be sorted by descending priority, then ascending `seq`.
/// A reservation created in this pass starts with an open backfill window
/// (its blockers are the shadow set, which is active by construction); the
/// coordinator freezes the cutoff on the pass where those blockers stop
/// advancing. Lower-priority jobs never backfill past a protected job.
pub fn plan_pass(
    active: &[ActiveLease],
    eligible: &[Candidate],
    reservation: Option<&ReservationSnapshot>,
) -> PassOutcome {
    // The default follows all ordinary candidates, regardless of priority.
    let ordinary_end = eligible
        .iter()
        .position(|candidate| candidate.is_default)
        .unwrap_or(eligible.len());
    debug_assert!(
        eligible[ordinary_end..]
            .iter()
            .all(|candidate| candidate.is_default)
    );
    debug_assert!(eligible.len() - ordinary_end <= 1);
    let mut outcome = plan_regular_pass(active, &eligible[..ordinary_end], reservation);
    if active.is_empty()
        && ordinary_end == 0
        && let Some(default) = eligible.first()
    {
        outcome.starts.push(Start {
            job_id: default.job_id,
            bypasses: None,
        });
    }
    outcome
}

fn plan_regular_pass(
    active: &[ActiveLease],
    eligible: &[Candidate],
    reservation: Option<&ReservationSnapshot>,
) -> PassOutcome {
    debug_assert!(eligible.windows(2).all(|w| {
        w[0].priority > w[1].priority
            || (w[0].priority == w[1].priority && w[0].ready_seq < w[1].ready_seq)
    }));

    let mut outcome = PassOutcome::default();
    let mut shadow_limits: Vec<u32> = active.iter().map(|l| l.limit).collect();
    let mut shadow_jobs: BTreeSet<JobId> = active.iter().map(|l| l.job_id).collect();
    let mut started: BTreeSet<JobId> = BTreeSet::new();

    // Local mutable view of the reservation for this pass. `existing` tracks
    // whether it is the persisted one (whose satisfaction/invalidation must be
    // reported) or one created earlier in this same pass.
    struct Res {
        job_id: JobId,
        cutoff_seq: Option<i64>,
        consumed: BTreeSet<JobId>,
        existing: bool,
    }

    let mut res: Option<Res> = reservation.map(|r| Res {
        job_id: r.job_id,
        cutoff_seq: r.cutoff_seq,
        consumed: r.consumed.clone(),
        existing: true,
    });

    // A persisted reservation whose protected job is no longer eligible
    // (cancelled or held) is defensively invalidated; mutations normally do
    // this with a precise reason.
    if let Some(r) = &res
        && !eligible.iter().any(|c| c.job_id == r.job_id)
    {
        outcome.invalidate_reservation = Some("protected job is no longer eligible");
        res = None;
    }

    // A newly eligible higher-priority job supersedes a lower-priority
    // protected reservation. Running leases remain untouched; the highest
    // ranked queued job will either start or become the new protected job.
    if let Some(r) = &res {
        let protected = eligible
            .iter()
            .find(|c| c.job_id == r.job_id)
            .expect("protected job verified eligible");
        if eligible
            .first()
            .is_some_and(|head| head.priority > protected.priority)
        {
            outcome.invalidate_reservation = Some("higher-priority job superseded protected job");
            res = None;
        }
    }

    loop {
        match &mut res {
            Some(r) => {
                // Step 2: the protected job starts the moment it fits,
                // before any backfill is reconsidered.
                let protected = eligible
                    .iter()
                    .find(|c| c.job_id == r.job_id)
                    .expect("protected job verified eligible");
                if can_admit(&shadow_limits, protected.limit) {
                    outcome.starts.push(Start {
                        job_id: protected.job_id,
                        bypasses: None,
                    });
                    started.insert(protected.job_id);
                    shadow_limits.push(protected.limit);
                    shadow_jobs.insert(protected.job_id);
                    if r.existing {
                        outcome.satisfy_reservation = true;
                    } else {
                        // A reservation created this pass can never be
                        // satisfied in the same pass: the shadow set only
                        // grows, so a job that did not fit cannot fit later.
                        unreachable!("reservation created and satisfied in one pass");
                    }
                    res = None;
                    continue;
                }

                // Step 3: restrict candidates to the backfill frontier —
                // any equal-priority eligible job while the window is open,
                // only pre-cutoff jobs once frozen — whose single bypass is
                // unconsumed.
                let backfill = eligible.iter().find(|c| {
                    r.cutoff_seq.is_none_or(|cutoff| c.ready_seq <= cutoff)
                        && c.job_id != r.job_id
                        && c.priority == protected.priority
                        && !r.consumed.contains(&c.job_id)
                        && !started.contains(&c.job_id)
                        && can_admit(&shadow_limits, c.limit)
                });
                match backfill {
                    Some(c) => {
                        outcome.starts.push(Start {
                            job_id: c.job_id,
                            bypasses: Some(r.job_id),
                        });
                        started.insert(c.job_id);
                        shadow_limits.push(c.limit);
                        shadow_jobs.insert(c.job_id);
                        r.consumed.insert(c.job_id);
                    }
                    // Step 7: nothing fits right now. "Currently full" is not
                    // an exhausted frontier; the next pass rechecks.
                    None => break,
                }
            }
            None => {
                // Step 4: without a reservation, consider priority/FIFO
                // order; the first job either starts or becomes protected.
                let Some(head) = eligible.iter().find(|c| !started.contains(&c.job_id)) else {
                    break;
                };
                if can_admit(&shadow_limits, head.limit) {
                    outcome.starts.push(Start {
                        job_id: head.job_id,
                        bypasses: None,
                    });
                    started.insert(head.job_id);
                    shadow_limits.push(head.limit);
                    shadow_jobs.insert(head.job_id);
                    continue;
                }
                // Incompatible only because of active concurrency limits:
                // protect it. Seed the consumed set with every current shadow
                // lease holder (initially active attempts plus jobs admitted
                // earlier in this batch). The new reservation's backfill
                // window is open: its blockers are the shadow set, which is
                // active right now by construction.
                let initial_consumed = shadow_jobs.clone();
                outcome.create_reservation = Some(NewReservation {
                    job_id: head.job_id,
                    initial_consumed: initial_consumed.clone(),
                });
                res = Some(Res {
                    job_id: head.job_id,
                    cutoff_seq: None,
                    consumed: initial_consumed,
                    existing: false,
                });
            }
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leases(limits: &[u32]) -> Vec<ActiveLease> {
        limits
            .iter()
            .enumerate()
            .map(|(i, &limit)| ActiveLease {
                job_id: -(i as i64 + 1),
                limit,
            })
            .collect()
    }

    fn cand(job_id: JobId, limit: u32) -> Candidate {
        Candidate {
            job_id,
            ready_seq: job_id,
            limit,
            priority: 0,
            is_default: false,
        }
    }

    fn prioritized(job_id: JobId, limit: u32, priority: i64) -> Candidate {
        Candidate {
            job_id,
            ready_seq: job_id,
            limit,
            priority,
            is_default: false,
        }
    }

    fn start_ids(outcome: &PassOutcome) -> Vec<JobId> {
        outcome.starts.iter().map(|s| s.job_id).collect()
    }

    #[test]
    fn formula_matches_documented_table() {
        // none + LLM(1) -> starts alone
        assert!(can_admit(&[], 1));
        // two CleanRL(3) + CleanRL(3) -> starts as third
        assert!(can_admit(&[3, 3], 3));
        // two CleanRL(3) + LLM(1) -> waits
        assert!(!can_admit(&[3, 3], 1));
        // LLM(1) running + CleanRL(3) -> waits
        assert!(!can_admit(&[1], 3));
        // one job(2) + one job(4) -> starts; lower limit of 2 wins
        assert!(can_admit(&[2], 4));
        // three at 3-wide is full for everyone
        assert!(!can_admit(&[3, 3, 3], 3));
    }

    #[test]
    fn empty_queue_does_nothing() {
        let outcome = plan_pass(&leases(&[3]), &[], None);
        assert_eq!(outcome, PassOutcome::default());
    }

    #[test]
    fn default_starts_only_when_idle_and_never_reserves_or_backfills() {
        let mut idle = prioritized(99, 1, i64::MAX);
        idle.is_default = true;
        let plan = plan_pass(&[], &[idle.clone()], None);
        assert_eq!(start_ids(&plan), vec![99]);
        assert!(plan.create_reservation.is_none());
        assert!(plan.starts[0].bypasses.is_none());
        let busy = plan_pass(&leases(&[4]), &[idle.clone()], None);
        assert!(busy.starts.is_empty());
        assert!(busy.create_reservation.is_none());
        // Even the lowest ordinary priority wins over the highest default.
        let ordinary = prioritized(1, 4, i64::MIN);
        let ready = plan_pass(&[], &[ordinary, idle.clone()], None);
        assert_eq!(start_ids(&ready), vec![1]);
        // A default is not spare-capacity backfill for an exclusive head.
        let blocked = plan_pass(&leases(&[4]), &[cand(1, 1), idle], None);
        assert!(blocked.starts.is_empty());
        assert_eq!(blocked.create_reservation.unwrap().job_id, 1);
    }

    #[test]
    fn default_clears_obsolete_protection_and_waits_when_absent() {
        let reservation = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: None,
            consumed: BTreeSet::new(),
        };
        let mut idle = cand(2, 1);
        idle.is_default = true;
        let ready = plan_pass(&[], &[idle], Some(&reservation));
        assert_eq!(start_ids(&ready), vec![2]);
        assert!(ready.invalidate_reservation.is_some());
        assert!(plan_pass(&[], &[], None).starts.is_empty());
    }

    #[test]
    fn batch_admission_is_cumulative() {
        // Four 3-wide candidates on an idle machine: exactly three start and
        // the fourth becomes protected (not silently over-admitted).
        let eligible = vec![cand(1, 3), cand(2, 3), cand(3, 3), cand(4, 3)];
        let outcome = plan_pass(&[], &eligible, None);
        assert_eq!(start_ids(&outcome), vec![1, 2, 3]);
        let res = outcome.create_reservation.expect("fourth job is protected");
        assert_eq!(res.job_id, 4);
        assert_eq!(res.initial_consumed, BTreeSet::from([1, 2, 3]));
    }

    #[test]
    fn higher_priority_runs_first_and_equal_priority_stays_fifo() {
        let eligible = vec![
            prioritized(3, 4, 1),
            prioritized(1, 4, 0),
            prioritized(4, 4, 0),
            prioritized(2, 4, -1),
        ];
        let outcome = plan_pass(&[], &eligible, None);
        assert_eq!(start_ids(&outcome), vec![3, 1, 4, 2]);
    }

    #[test]
    fn lower_priority_cannot_backfill_a_protected_job() {
        let active = vec![
            ActiveLease {
                job_id: 100,
                limit: 3,
            },
            ActiveLease {
                job_id: 101,
                limit: 3,
            },
        ];
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: None,
            consumed: BTreeSet::from([100, 101]),
        };
        let eligible = vec![prioritized(1, 1, 1), prioritized(2, 3, 0)];
        let outcome = plan_pass(&active, &eligible, Some(&res));
        assert!(outcome.starts.is_empty());
        assert!(outcome.invalidate_reservation.is_none());
    }

    #[test]
    fn higher_priority_supersedes_a_lower_priority_reservation() {
        let active = vec![
            ActiveLease {
                job_id: 100,
                limit: 3,
            },
            ActiveLease {
                job_id: 101,
                limit: 3,
            },
        ];
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(3),
            consumed: BTreeSet::from([100, 101]),
        };
        let eligible = vec![
            prioritized(4, 1, 1),
            prioritized(1, 1, 0),
            prioritized(2, 3, 0),
        ];
        let outcome = plan_pass(&active, &eligible, Some(&res));
        assert!(outcome.starts.is_empty());
        assert_eq!(
            outcome.invalidate_reservation,
            Some("higher-priority job superseded protected job")
        );
        assert_eq!(outcome.create_reservation.unwrap().job_id, 4);
    }

    #[test]
    fn restrictive_head_is_protected_and_backfill_takes_the_slot() {
        // Motivating example: two CleanRL(3) active, LLM(1) first in line,
        // five CleanRL(3) behind it.
        let active = vec![
            ActiveLease {
                job_id: 100,
                limit: 3,
            },
            ActiveLease {
                job_id: 101,
                limit: 3,
            },
        ];
        let eligible = vec![
            cand(1, 1),
            cand(2, 3),
            cand(3, 3),
            cand(4, 3),
            cand(5, 3),
            cand(6, 3),
        ];
        let outcome = plan_pass(&active, &eligible, None);

        // LLM protected; exactly one CleanRL backfills the empty third slot.
        assert_eq!(
            outcome.starts,
            vec![Start {
                job_id: 2,
                bypasses: Some(1)
            }]
        );
        let res = outcome.create_reservation.unwrap();
        assert_eq!(res.job_id, 1);
        assert_eq!(res.initial_consumed, BTreeSet::from([100, 101]));
    }

    #[test]
    fn open_window_admits_post_protection_arrivals() {
        // The reported head-of-line bug: LLM(1) protected while two
        // CleanRL(3) run; CleanRL(4) was submitted after protection. While an
        // initial blocker still runs the window is open and job 4 takes the
        // empty third slot.
        let active = vec![
            ActiveLease {
                job_id: 100,
                limit: 3,
            },
            ActiveLease {
                job_id: 101,
                limit: 3,
            },
        ];
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: None,
            consumed: BTreeSet::from([100, 101]),
        };
        let eligible = vec![cand(1, 1), cand(4, 3)];
        let outcome = plan_pass(&active, &eligible, Some(&res));
        assert_eq!(
            outcome.starts,
            vec![Start {
                job_id: 4,
                bypasses: Some(1)
            }]
        );

        // A consumed bypass stays consumed in the open window: a retry of
        // job 4 may not pass again.
        let res2 = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: None,
            consumed: BTreeSet::from([100, 101, 4]),
        };
        let outcome2 = plan_pass(&active, &eligible, Some(&res2));
        assert!(outcome2.starts.is_empty());
    }

    #[test]
    fn post_cutoff_jobs_never_pass_a_frozen_frontier() {
        // Frontier frozen at cutoff 6; job 7 arrived later. A slot is open,
        // but job 7 must not take it and pre-cutoff job 3 may.
        let active = vec![
            ActiveLease {
                job_id: 100,
                limit: 3,
            },
            ActiveLease {
                job_id: 101,
                limit: 3,
            },
        ];
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(6),
            consumed: BTreeSet::from([100, 101, 2]),
        };
        let eligible = vec![cand(1, 1), cand(3, 3), cand(7, 3)];
        let outcome = plan_pass(&active, &eligible, Some(&res));
        assert_eq!(
            outcome.starts,
            vec![Start {
                job_id: 3,
                bypasses: Some(1)
            }]
        );

        // Same but job 3's bypass is already consumed: nothing starts even
        // though a slot is open — post-cutoff job 7 still may not pass.
        let res2 = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(6),
            consumed: BTreeSet::from([100, 101, 2, 3]),
        };
        let outcome2 = plan_pass(&active, &eligible, Some(&res2));
        assert!(outcome2.starts.is_empty());
        assert!(!outcome2.satisfy_reservation);
        assert!(outcome2.create_reservation.is_none());
    }

    #[test]
    fn frozen_frontier_uses_readiness_order_not_job_id() {
        let active = leases(&[3, 3]);
        let eligible = vec![
            Candidate {
                job_id: 100,
                ready_seq: 1,
                limit: 1,
                priority: 0,
                is_default: false,
            },
            Candidate {
                job_id: 200,
                ready_seq: 5,
                limit: 3,
                priority: 0,
                is_default: false,
            },
            Candidate {
                job_id: 1,
                ready_seq: 11,
                limit: 3,
                priority: 0,
                is_default: false,
            },
        ];
        let reservation = ReservationSnapshot {
            job_id: 100,
            cutoff_seq: Some(10),
            consumed: BTreeSet::new(),
        };
        let outcome = plan_pass(&active, &eligible, Some(&reservation));
        assert_eq!(start_ids(&outcome), vec![200]);
        assert!(
            !outcome.starts.iter().any(|start| start.job_id == 1),
            "an old job id that became ready after the freeze cannot bypass"
        );
    }

    #[test]
    fn protected_job_starts_before_backfills_when_drained() {
        // Everything drained: the protected LLM(1) starts alone; eligible
        // CleanRL jobs must not start beside it.
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(6),
            consumed: BTreeSet::new(),
        };
        let eligible = vec![cand(1, 1), cand(3, 3), cand(4, 3)];
        let outcome = plan_pass(&[], &eligible, Some(&res));
        assert_eq!(
            outcome.starts,
            vec![Start {
                job_id: 1,
                bypasses: None
            }]
        );
        assert!(outcome.satisfy_reservation);
        // After the exclusive job is admitted, the rest wait (head job 3
        // becomes the next protected job).
        assert_eq!(outcome.create_reservation.unwrap().job_id, 3);
    }

    #[test]
    fn satisfying_one_reservation_can_create_the_next() {
        // Protected job (2-wide) starts; one companion fits; the next head is
        // 1-wide and becomes the new protected job.
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(5),
            consumed: BTreeSet::new(),
        };
        let eligible = vec![cand(1, 2), cand(2, 2), cand(3, 1)];
        let outcome = plan_pass(&[], &eligible, Some(&res));
        assert_eq!(start_ids(&outcome), vec![1, 2]);
        assert!(outcome.satisfy_reservation);
        let new_res = outcome.create_reservation.unwrap();
        assert_eq!(new_res.job_id, 3);
        assert_eq!(new_res.initial_consumed, BTreeSet::from([1, 2]));
    }

    #[test]
    fn ineligible_protected_job_invalidates_reservation() {
        let res = ReservationSnapshot {
            job_id: 1,
            cutoff_seq: Some(5),
            consumed: BTreeSet::new(),
        };
        let eligible = vec![cand(2, 3), cand(3, 3)];
        let outcome = plan_pass(&[], &eligible, Some(&res));
        assert!(outcome.invalidate_reservation.is_some());
        // Ordinary FIFO admission resumes.
        assert_eq!(start_ids(&outcome), vec![2, 3]);
    }

    #[test]
    fn lower_limit_always_wins_in_mixed_sets() {
        // active {4}: a 2-wide candidate fits (resulting 2 <= min(4, 2)).
        let outcome = plan_pass(&leases(&[4]), &[cand(1, 2)], None);
        assert_eq!(start_ids(&outcome), vec![1]);
        // active {4, 2}: a 4-wide candidate would make 3 > 2 — protected.
        let outcome = plan_pass(&leases(&[4, 2]), &[cand(1, 4)], None);
        assert!(outcome.starts.is_empty());
        assert_eq!(outcome.create_reservation.unwrap().job_id, 1);
    }

    /// Simulation oracle: replay plan_pass decisions against randomized job
    /// streams and completions, asserting the invariants after every step.
    #[test]
    fn randomized_streams_never_violate_invariants() {
        // Small deterministic xorshift so the test needs no rand dependency.
        let mut rng_state: u64 = 0x9E3779B97F4A7C15;
        let mut rng = move |bound: u64| {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state % bound
        };

        for _round in 0..200 {
            let mut next_seq: i64 = 0;
            let mut queued: Vec<Candidate> = Vec::new();
            let mut active: Vec<ActiveLease> = Vec::new();
            let mut reservation: Option<ReservationSnapshot> = None;
            // The reservation's initial blockers (job IDs standing in for
            // attempt IDs; the oracle has no retries, so they coincide).
            let mut blockers: BTreeSet<JobId> = BTreeSet::new();
            // Sequence at which each job was protected, to check precedence.
            let mut protected_since: Option<(JobId, i64)> = None;

            for _step in 0..60 {
                match rng(3) {
                    // Submit 1-3 jobs.
                    0 => {
                        for _ in 0..=rng(2) {
                            next_seq += 1;
                            let limit = [1, 1, 2, 3, 3, 4][rng(6) as usize];
                            let priority = rng(3) as i64 - 1;
                            queued.push(Candidate {
                                job_id: next_seq,
                                ready_seq: next_seq,
                                limit,
                                priority,
                                is_default: false,
                            });
                        }
                    }
                    // Finish a random active job.
                    1 if !active.is_empty() => {
                        let idx = rng(active.len() as u64) as usize;
                        active.remove(idx);
                    }
                    _ => {}
                }

                // The coordinator's freeze rule: the first pass after every
                // initial blocker stopped advancing pins the cutoff exactly
                // once, at the maximum sequence submitted so far.
                if let Some(res) = &mut reservation
                    && res.cutoff_seq.is_none()
                    && !active.iter().any(|l| blockers.contains(&l.job_id))
                {
                    res.cutoff_seq = Some(next_seq);
                }

                queued.sort_by_key(|candidate| {
                    (std::cmp::Reverse(candidate.priority), candidate.ready_seq)
                });
                let outcome = plan_pass(&active, &queued, reservation.as_ref());

                // Apply decisions exactly as the coordinator would.
                if outcome.satisfy_reservation || outcome.invalidate_reservation.is_some() {
                    reservation = None;
                    blockers.clear();
                    protected_since = None;
                }
                for start in &outcome.starts {
                    let pos = queued
                        .iter()
                        .position(|c| c.job_id == start.job_id)
                        .unwrap();
                    let c = queued.remove(pos);

                    // Invariant: admission never violates any member's limit.
                    let limits: Vec<u32> = active.iter().map(|l| l.limit).collect();
                    assert!(
                        can_admit(&limits, c.limit),
                        "over-admission: active {limits:?} candidate {}",
                        c.limit
                    );

                    // Invariant: once the frontier is frozen, nothing
                    // submitted after the cutoff starts while that
                    // reservation is active.
                    if let Some(r) = &reservation {
                        assert!(start.consumes_bypass() || c.job_id == r.job_id);
                        if let Some(cutoff) = r.cutoff_seq {
                            assert!(
                                c.ready_seq <= cutoff || c.job_id == r.job_id,
                                "post-cutoff job {} bypassed protected job {}",
                                c.job_id,
                                r.job_id
                            );
                        }
                        if let Some((pjob, _)) = protected_since {
                            assert_ne!(c.job_id, pjob, "protected job started via backfill path");
                        }
                    }

                    active.push(ActiveLease {
                        job_id: c.job_id,
                        limit: c.limit,
                    });
                    if let Some(r) = &mut reservation
                        && start.consumes_bypass()
                    {
                        assert!(
                            r.consumed.insert(c.job_id),
                            "job {} consumed its bypass twice",
                            c.job_id
                        );
                    }
                }
                if let Some(new_res) = outcome.create_reservation {
                    assert!(reservation.is_none());
                    protected_since = Some((new_res.job_id, next_seq));
                    // As in the coordinator: the initial blockers are the
                    // shadow set at protection creation, which is exactly the
                    // seeded consumed set.
                    blockers = new_res.initial_consumed.clone();
                    reservation = Some(ReservationSnapshot {
                        job_id: new_res.job_id,
                        cutoff_seq: None,
                        consumed: new_res.initial_consumed,
                    });
                }

                // Global invariant: current active set satisfies everyone.
                let count = active.len() as u32;
                for lease in &active {
                    assert!(
                        count <= lease.limit,
                        "active set violates a member declaration"
                    );
                }
            }
        }
    }
}
