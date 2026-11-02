# Architecture

mlqueue is a **coordination service** for local ML commands on a single Linux
user account. One durable daemon (`mlqd`) is the only admission authority;
clients (`mlq`) talk to it over a Unix socket. It is not a cluster scheduler,
GPU allocator, or security boundary.

Operator-facing usage lives in [README.md](README.md). This document describes
how the system is built and the invariants it maintains.

## Goals

- Independently acting agents and shells share one machine-wide queue.
- Concurrency safety is expressed by the submitter as a single integer per
  job, not inferred from hardware.
- Daemon restart and upgrade never kill workers.
- Mutations are durable and safely retryable.

## Non-goals

The daemon does **not**:

- Discover GPUs or parse NVIDIA/ROCm telemetry
- Meter or reserve VRAM, RAM, CPU, or wall time
- Infer workload type from commands or repositories
- Preempt ordinary work, or suspend or checkpoint commands. Only an explicitly
  submitted default idle job yields automatically to ordinary work.
- Detect unmanaged processes or foreign GPU use
- Accept remote submission or multi-user isolation

Bypassing `mlq` (`python train.py`, `nohup`, detached terminals, repo-local
workers) is invisible to the model. Submitted workloads and their descendants
must stay in the runner’s process group (no `setsid` / daemonizing).

## Components

One Rust package, two public binaries, one internal runner role:

| Binary / role | Responsibility |
|---|---|
| `mlq` | CLI, socket client, local log tailing |
| `mlqd` | Singleton daemon, protocol, scheduling, recovery |
| `mlqd __runner` | Detached per-attempt process supervisor |

### Source layout

```text
src/bin/mlq.rs          CLI entry
src/bin/mlqd.rs         daemon / runner entry
src/lib.rs
src/cli.rs              clap surface and human/JSON formatting
src/client.rs           framed client
src/config.rs           TOML config
src/paths.rs            XDG paths (overridable in tests)
src/domain.rs           IDs, states, scheduler semantics version
src/protocol.rs         wire ops, public camelCase views, framing
src/run_window.rs       validated daily windows with fixed UTC offsets
src/db.rs               migrations, repositories, operations, events
src/scheduler.rs        pure admission + backfill policy (no I/O)
src/daemon/
  mod.rs                listen loop, singleton lock, runner adopt
  coordinator.rs        serialized mutations and schedule passes
  views.rs              status/job eligibility explanations
src/process/
  runner.rs             spawn, handshake, cancel, result
  identity.rs           PID/pgid + boot/start-time identity
  artifacts.rs          exclusive/fsync artifact publication
```

Dependencies of note: Tokio (socket/orchestration), Clap, Serde/JSON,
`rusqlite` (bundled SQLite), `libc` for `setsid`/`setpgid`/`killpg`/`flock`,
`tracing`.

**Serialization rule:** all scheduling and DB writes go through one
coordinator path. Handlers never launch work themselves. Transactions and
mutexes are never held across socket I/O, filesystem waits, or process spawn.

## Admission model

Each job declares:

```text
maxParallelRuns  positive integer, default 1   CLI: --max-parallel-runs
priority         signed i64, default 0         CLI: --priority
timeLimitMs      optional positive integer     CLI: --time-limit DURATION
```

JSON uses camelCase (`maxParallelRuns`). Rust/SQLite use snake_case.

### Compatibility formula

Let `R` be attempts that currently hold run leases. Candidate `j` may acquire
a lease iff:

```text
|R| + 1  <=  min(j.maxParallelRuns,  every r.maxParallelRuns for r in R)
```

The limit is **global and symmetric**: every running declaration must remain
true after admission. It does not count “only copies of the same command.”
Default `1` serializes unknown work.

One integer cannot express pair-specific compatibility. Agents must pick the
value safe next to *arbitrary* other managed jobs (`1` when type-dependent).

### Queue rank

Eligible jobs (dependencies satisfied, not held, retry delay elapsed) are
ordered by:

1. **priority** descending  
2. **readiness sequence** ascending (FIFO from when the job became runnable)

Priority never preempts an active lease. Strict priority can starve lower
priorities under sustained higher-priority arrivals.

### Dependency readiness

Submission may attach immutable backward edges to existing jobs. Repeated
conditions are ANDed:

- `succeeded`: the parent must reach `succeeded`; any other terminal outcome
  permanently skips the child.
- `terminal`: any terminal parent outcome satisfies the edge, for cleanup and
  unconditional follow-up work.

Every edge records its first observed terminal parent outcome. Resolution is
monotonic: manually retrying a failed or lost parent does not revive skipped
children or retract already-ready terminal follow-ups. Automatic attempts do
not terminalize the parent between retries, so children wait for the final
outcome. A fixed-point transaction cascades skips and assigns readiness order
before admission observes the new state.

### Protection and backfill (semantics v4)

When the highest-ranked eligible job cannot start only because of active
limits, it becomes the **protected** job. One global reservation is persisted.

1. **Open window** — while any *initial blocker* attempt still advances
   (`prepared` / `authorized` / `running`), equal-priority eligible jobs may
   backfill open slots **once each**. Lower priorities cannot.
2. **Freeze** — when the last initial blocker stops advancing, pin
   `max(readiness sequence)` as the backfill cutoff exactly once.
3. **Frozen frontier** — only equal-priority, unconsumed jobs at or before the
   cutoff may still bypass; later submissions wait.
4. **Protected first** — whenever the formula allows the protected job, it
   starts before further backfill.
5. **Supersede** — a newly eligible higher-priority job invalidates a lower-
   priority reservation (running leases untouched).

Orphaned/quarantined blockers keep their leases (containment) but count as
drained for the window so they cannot hold it open forever.

### Live mutations

| Mutation | Allowed when | Notes |
|---|---|---|
| `set-max-parallel-runs` | queued, held, starting, running, needs_attention | Live path updates job + attempt + lease atomically; lowering may leave the set temporarily oversubscribed until excess leases drain |
| `set-priority` | queued, held only | No preemption; invalidates reservation if the protected job’s priority changes |
| `set-time-limit` | queued, held, starting, running, needs_attention | Replaces the per-attempt wall-clock limit (`--clear` removes it). A live runner adopts the new deadline, still measured from command spawn, on its next poll. A limit already in the past begins termination; the runner records that commitment, and a later extension does not revive the attempt even if the runner then dies. An earlier cancellation stays a cancellation. On an orphan, a limit just moved into the past still gets the full SIGTERM grace; a limit that was already in force is enforced from its original deadline. Later attempts use the updated limit |
| `hold` | queued | Invalidates reservation if protected |
| `cancel` | most states | Before start → terminal; after start → signal process group |
| `pause` / `unpause` | always | Durable operator gate. Submissions still enqueue; running leases continue; the scheduler issues no new leases until unpause. Pause survives daemon restart. |

## Scheduling pass (outline)

On each pass the coordinator:

1. Reconciles leases and terminal attempts.
2. If the operator pause is set, stops there: no new attempts, no reservation
   mutations. Running work and dependency resolution continue on other paths.
3. Supersedes a protected job if a higher-priority eligible head exists.
4. Starts the protected job if now compatible.
5. If still reserved and blocked, considers only equal-priority unconsumed
   candidates (window or frozen cutoff rules).
6. Else walks eligible jobs in priority/FIFO; protects the first incompatible
   head; admits greedily against a **shadow** running set (never evaluate the
   whole batch against the same initial count).
7. Commits attempts, leases, reservation rows, and events in one transaction
   after revalidation.

Unknown reservation semantics versions block ordinary admission until an
operator migrates or invalidates the reservation (`admission_blocked`).


## Protocol

- Unix stream socket, big-endian `u32` length-prefixed JSON
- Hard frame limit checked before allocation
- Peer UID must match the daemon
- Protocol version is exact-match (currently **9**). Update the CLI and daemon together.
- Mutations require a durable **idempotency key**; identical retry returns the
  original result; same key + different payload → conflict

Log bytes are not streamed through the daemon. `mlq logs` asks for paths and
reads files directly (same user, same machine).

### Streaming subscriptions

`{"op":{"type":"subscribe"}}` is a read op that changes the *shape* of the
connection, not the framing: after one subscribe request, that connection stops
being request/response and becomes a one-way stream of `status` replies.

- Frame 1 is the current queue snapshot, echoing the subscribe `request_id`;
  every later frame is another complete snapshot with the same `request_id`.
  Consumers never replay events to reconstruct state.
- The coordinator publishes the committed event id on a `tokio::sync::watch`
  channel once per handled message — after the transaction closed, before the
  reply leaves for the socket. Every committed change appends an event, so that
  cursor is a single choke point covering mutations, reconciliation and
  scheduling passes, and no lock is ever held across a subscriber's socket I/O.
  The one snapshot field derived outside the event log is `admissionBlocked`
  (in-memory, set by a scheduling pass), so the coordinator compares a mirror of
  it alongside the cursor; any future field derived outside the log needs the
  same treatment.
- Coalescing is inherent: `watch` keeps only the newest revision, so a burst
  (or a slow reader) collapses into one refresh instead of a queue of frames.
  A refresh whose snapshot is byte-identical to the last one sent is dropped.
- The idle read timeout does not apply to a subscription — silence is the normal
  state of a quiet queue. Two bounds replace it: streams may occupy at most a
  quarter of `max_connections` (an excess subscriber is refused with
  `too_many_subscriptions` instead of starving request/response clients), and a
  frame write that stalls for the idle timeout ends the connection, so a peer
  that stops reading cannot hold its slot forever.
- The stream ends on client EOF (or any further bytes from the client, which
  would desynchronise it) and on daemon shutdown, which drops the coordinator's
  sender.
- Subscriptions use the same protocol version as other requests. An older
  daemon rejects an unrecognized operation as a malformed request.

### Job event subscriptions and local following

`{"op":{"type":"subscribe_job","job":42}}` opens a job-scoped stream of
`job_update` replies. Its initial read transaction captures the current compact
job snapshot (latest attempt only) and the global event head together, with no
historical event replay. Supplying `after` resumes after that durable cursor.
Job subscriptions use the current protocol version. Older daemons reject
unrecognized operations; update `mlqd` with `mlq`.

Each update contains `job`, `cursor`, `events`, and `hasMore`. Event pages use the
existing job index with a cursor seek, at most 256 records plus lookahead, and
shrink further when needed to fit a frame. `hasMore` drains immediately; consumers
must drain the backlog before treating the current snapshot as terminal. Unlike
queue snapshots, job lifecycle events are not discarded when watch wakeups
coalesce: even a brief running transition remains in the durable log.
Unrelated queue revisions do not emit job frames. Subscription quotas, peer UID
checks, framing, disconnect detection, and slow-reader timeouts are shared.

`mlq follow` uses this stream with no job-state polling. Observation has a
55-minute default deadline, overridable with `--timeout`; expiry exits `124`
without mutating the job. The deadline spans queueing, execution, and retries,
but not finish-hook delivery after a terminal outcome is known. The async
client keeps an in-progress frame read pinned across local timer ticks; cancelling
a partial `read_exact` would otherwise desynchronize framing. EOF before a terminal
snapshot is an error. Final output uses the compact terminal snapshot; full
attempt history is available separately through `show --json`.

The follower owns explicitly selected TOML checks and notification hooks, never
the daemon. Default output is only the final outcome. Checks are enabled only by
selecting warning events, run only while running, and commit deduplication state
after an async batch completes so an interrupted batch cannot consume unseen
warnings. Stable keys, fresh-step clearance, and a per-session warning budget
bound notifications before a hook or LLM sees them. A single-threaded Tokio runtime
handles subscription I/O, opt-in timers, and bounded subprocesses. Neither command
output nor queue snapshots are forwarded wholesale to an overseeing agent.

## Filesystem layout

XDG conventions (overridable in tests):

```text
$XDG_RUNTIME_DIR/mlqueue/mlqd.sock
$XDG_STATE_HOME/mlqueue/mlqueue.db
$XDG_STATE_HOME/mlqueue/daemon.lock          # stable singleton (not under runtime)
$XDG_STATE_HOME/mlqueue/attempts/<id>/
  stdout.log  stderr.log
  identity.json
  start                 # launch authorization marker
  exec.json             # command identity / running
  cancel.json
  limit.json            # live time-limit override; absent means command.json still applies
  timeout.json          # written once, when timeout termination starts
  result.json
$XDG_CONFIG_HOME/mlqueue/config.toml
```

Directories `0700`, artifacts `0600`. Singleton lock lives in **state**, not
runtime, so a recreated `$XDG_RUNTIME_DIR` cannot create a second lock.

## Persistence

SQLite: foreign keys, WAL, `synchronous=FULL`, busy timeout, embedded
migrations, one serialized writer.

The v3 migration removed the original dependency edges. A queued or held job
whose legacy prerequisites were unresolved was cancelled rather than allowed
to run out of order. Those deleted edges cannot be reconstructed. Migration v5
restores dependencies for new submissions with latched outcomes and a durable
readiness sequence; historical cancelled and skipped records remain truthful.

| Table | Role |
|---|---|
| `jobs` | Command, cwd, env, state, class, daily window, default role, concurrency/priority, readiness order, retries, optional time limit, timestamps |
| `job_classes` | Named groups, admission pause, shared daily window, timestamps |
| `dependencies` | Immutable parent conditions with monotonic observed-outcome resolution |
| `attempts` | Launch token, state, copied limit, absolute window close, durable default yield, runner/command identity, exit |
| `run_leases` | One active lease per attempt; source of truth for the formula |
| `scheduler_reservation` | Protected job, cutoff, initial blockers, semantics version |
| `scheduler_reservation_backfills` | One bypass consumption per job per reservation |
| `scheduler_pause` | Singleton operator pause flag (migration v6) |
| `operations` | Idempotency keys, request hash, stored response / tombstone |
| `events` | Append-only audit log |


Job IDs are `AUTOINCREMENT` rowids. Scheduling uses a separate durable
readiness sequence, so pre-enqueued dependency waiters do not reserve old FIFO
positions. Its monotonic counter also prevents retention from lowering a
frozen frontier.

## Job and attempt states

```text
Job:
  held ⇄ queued → starting → running → succeeded | failed | cancelled | lost
  queued | held → skipped (an observed success prerequisite became impossible)
  starting | running → needs_attention → lost | cancelled

Attempt (persisted):
  prepared → authorized → running → finalized (success/fail/cancel/lost)
  authorized | running → orphaned | quarantined → resolved when group empty
```

`prepared` holds a lease before authorization. Eligibility strings
(`protected_drain`, `waiting_for_higher_priority`, …) are **derived** for
status, not stored states.

## Crash-safe attempt lifecycle

Process spawn and SQLite are not atomic. Flow:

1. Transaction: create `prepared` attempt + lease, job → `starting`.
2. Spawn detached runner (new session); exclusive per-attempt flock.
3. Publish runner identity; coordinator attaches it.
4. Revalidate formula; commit `authorized`; publish `start`.
5. Runner forks command into its own process group; publishes `exec.json`;
   job/attempt → `running`.
6. Runner waits for process-group emptiness (not just leader exit), then
   publishes `result.json`.
7. Daemon finalizes job/attempt, releases lease, appends events.

After authorization, a missing ack means the command **may** have run—never
launch a replacement until the group is proven empty.

On daemon restart: adopt live runners, finalize complete results, respawn
prepared runners under the same flock, republish `start` only when still
valid, quarantine corrupt identity, retain leases for orphans.

### Cancellation

- Before authorization: never publish `start`; finalize cancelled.
- After: durable intent + `cancel.json`; runner signals the **command** group
  (SIGTERM, optional SIGKILL after grace).
- Lease released only when the group is empty.
- Natural exit before signal delivery can win (`cancellation_too_late`).

### Per-attempt wall-clock limits

- An optional limit starts at successful command spawn, not submission or
  authorization. Queue wait is excluded; everything executed by the command,
  including compilation and descendants, is included.
- Expiry is a failed attempt and therefore follows the ordinary retry policy.
- The runner sends SIGTERM to the command process group and always schedules
  SIGKILL after the cancellation grace period. It publishes the timed-out
  result only after the group is empty, so the run lease remains truthful.
- The detached runner owns the monotonic deadline, so daemon restart does not
  reset it. The command spawn time is also persisted; if the runner dies, the
  daemon takes over SIGTERM/SIGKILL enforcement while retaining the lease.

### Job classes and execution windows

Migration v8 adds `job_classes`, optional class membership and windows on jobs,
and `attempts.window_end_at`. Existing jobs remain unrestricted. Classes must
exist before use and keep the global concurrency rules.

SQLite and JSON store windows as `HH:MM-HH:MM@+HH:MM` or `HH:MM-HH:MM@-HH:MM`.
Offsets are fixed; omission means UTC. Windows include the start, exclude the
end, and can cross midnight. Both job and class windows must be open. Class
pauses and closed windows exclude jobs before protection/backfill planning.
The scheduler invalidates reservations for excluded jobs and preserves their
readiness order.

The admission transaction stores the earliest window close with the attempt
and lease. `command.json` copies it. The runner refuses late launches, sends
SIGTERM at close, and escalates to SIGKILL after grace. The lease stays active
until the process group exits. Shutdown may extend beyond the window during
grace. `runner.log` records closure and signals; `result.json` reports
`window_closed`. The attempt fails under the normal retry policy. Live
`limit.json` edits cannot change the window deadline. `timeout.json` lets the
daemon continue termination if the runner dies.

Only queued or held jobs can change class or window. Class windows require no
live attempts; class pauses affect new starts only. Class cancellation records
all members' cancellations before sending signals. An unsafe member rejects the
request. Mutations record idempotency keys and lifecycle events.

Protocol v9 adds class and window operations. Absent submit fields stay omitted
to preserve older request hashes. Job views add `class`, `runWindow`, and
`classRunWindow`; attempt views add the fixed `windowEndAt`.

### Default idle job

Migration v9 adds `jobs.is_default` and `attempts.yield_requested`, both false
on existing records. A partial unique index permits one unfinished default,
including held and recovery states. Ordinary candidates come first in
priority/readiness order. The default never enters protection or backfill;
it starts only when no ordinary candidate or active lease exists.

When ordinary work becomes ready, the coordinator commits the default's yield
and forced cancellation before publishing `cancel.json`. Existing runner and
orphan recovery code enforce the signals. The lease stays active until the
process group exits. Prepared attempts can release immediately because they
have no start authorization. Quarantine still requires operator recovery.

Success or scheduler yield requeues the default after at least one second.
Failures use its retry budget, counted since the last success or yield.
User cancellation clears restart intent in the same transaction. Defaults
obey class and window rules. They cannot depend on other jobs or serve as
prerequisites. Upgrading the binaries does not install a default command.

`default end` and `default start` select the active default or the latest saved
default in the same transaction as their mutation. Ending uses normal
cancellation and clears yield intent. Starting requeues a terminal or held
default without changing its command or attempt history. An already active
default stays active. Pending cancellation and unresolved recovery prevent a
start until containment is resolved. Both operations use durable idempotency keys.

Boot registration uses a dedicated database lookup for the active default or
the command's latest default. It does not depend on status's recent-history
limit. A changed boot ID proves an active default's old group is gone. Recovery
records the interrupted attempt as lost and requeues the default if its failure
budget remains. User cancellation, holds, quarantine, and committed timeouts
keep their normal outcomes. A valid runner result takes precedence.

Job detail includes the latest 25 attempts and reports `attemptsTruncated` when
older records exist. `attempts` pages history by descending attempt number with
an exclusive `before` cursor and a limit of 1 to 100. Single-job mutation replies
omit attempt history, including the reply stored for idempotency. Recurring
defaults therefore do not grow those replies on each restart. Replaying an
older cached reply compacts its attempt history without changing the request
hash or operation outcome.

### Group emptiness

- Runner: `waitid(WNOWAIT)` keeps the zombie leader pinning the pgid while
  scanning `/proc`; reaps after consecutive empty scans.
- Daemon (no zombie): conservative—same-boot pgid + start-time checks; false
  release is impossible; stuck D-state holds the lease visibly.

## Command and environment model

- Exact argv after `--` (UTF-8, no shell reconstruction).
- Explicit cwd; revalidated at launch.
- Env = small baseline + `--env` / `--inherit-env` only (not full parent env).
- Secrets in SQLite are plaintext; prefer credential files read by the job.
- No GPU env interpretation by the daemon.

## Configuration

Small versioned TOML under `$XDG_CONFIG_HOME/mlqueue/`: grace periods,
protocol limits, retention knobs. Unknown fields rejected. Reload requires
restart. Scheduling semantics are **code-versioned**, not config-toggled.

## Agent skill

[`skills/queue-ml-jobs/`](skills/queue-ml-jobs/) is versioned with the repo.
It requires agents to use `mlq`, keep work foregrounded, choose
`maxParallelRuns` explicitly (`1` for exclusive/unknown, higher only from
evidence), and leave priority at `0` unless ordering is intentional.

## Invariants

1. One daemon is authoritative for managed admission.
2. Every starting/running attempt owns exactly one durable run lease.
3. Leases are acquired transactionally before launch authorization.
4. Admission never creates an active lease set that violates any member’s
   `maxParallelRuns`. Operator lowers of a live limit may leave the set
   temporarily oversubscribed; excess leases drain without preemption and no
   new starts are admitted until the set is compatible again.
5. Leases release only when the attempt’s process group is empty.
6. A protected job is considered before ordinary admissions each pass.
7. At most one bypass per job per reservation; post-cutoff jobs cannot pass a
   frozen frontier; lower priority cannot backfill a higher protected job.
8. Commands are argv + cwd + explicit env—no shell strings.
9. Every mutation is idempotent under its key.
10. The daemon does not kill foreign work or preempt healthy managed work.
11. Events, results, and logs remain inspectable without a live daemon.

## Known limitations

- Pair-specific compatibility needs `1` or future lanes/groups (not built).
- Descendants that call `setsid` escape the lease model (unsupported).
- Uninterruptible D-state after SIGKILL can hold a lease indefinitely
  (visible; no silent release).
- Strict priority can starve lower priorities.
- No remote clients; no multi-tenant isolation.
