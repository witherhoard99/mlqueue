# mlqueue

**mlqueue** is a machine-wide queue for local ML work: one durable daemon
(`mlqd`) and one CLI (`mlq`). Agents and shells across repositories submit
commands through the same service so they cannot accidentally start an unsafe
number of runs at once.

It is a **coordination** tool for a single Linux user on one workstation—not a
cluster scheduler, not a GPU allocator, and not a security boundary.

Design rationale, components, and invariants live in
[ARCHITECTURE.md](ARCHITECTURE.md).

## Install

```bash
cargo install --path .    # installs mlq + mlqd into ~/.cargo/bin
mlq daemon install        # systemd user unit: enable + start
# without systemd:
mlq daemon run            # foreground daemon
```

Confirm:

```bash
mlq --version
mlq daemon status
```

### Stage daemon updates for downtime

`mlq` can be installed immediately. On a busy machine, stage `mlqd` instead of
replacing it with `cargo install` while runs are active. The optional updater
requires Python 3.11+ and a persistent systemd user daemon with `KillMode=process`.

```bash
./contrib/mlq-update install          # one-time: installs ~/.local/bin/mlq-update
cargo build --release --locked
mlq-update stage --mlqd target/release/mlqd
cargo install --path . --locked --force --bin mlq
mlq-update status
```

Stage before upgrading the CLI: staging saves a compatible observer alongside
the immutable daemon candidate. No Git fetching, automatic builds, or unfinished
source edits are installed. Staging another candidate replaces the pending one.

The enabled `mlqd-update.path` unit starts an updater only when an update is
pending. It subscribes quietly until there are **no unfinished jobs and no active
leases**, including held jobs, delayed retries, and recovery states. It does not
pause, cancel, or reorder work. At downtime it stops only the daemon, takes its
singleton lock, rechecks the database, atomically replaces the executable, and
starts the service again. A submission racing shutdown defers the replacement;
existing workers survive. Clients may briefly disconnect during activation.

Pending updates survive logout/reboot. Connection failures retry after 30 seconds;
there is no queue polling while subscribed. Interrupted activation records
restart intent so the updater can restore the service on its next start.
The previous executable is retained at
`~/.local/state/mlqueue-updates/previous-mlqd` (or under `$XDG_STATE_HOME`).
Failed readiness retains the pending update for inspection; there is no automatic
binary downgrade across potentially incompatible database migrations.

```bash
systemctl --user status mlqd-update.path mlqd-update.service
journalctl --user -u mlqd-update.service
# Disable automatic activation (the staged candidate remains available):
systemctl --user disable --now mlqd-update.path
systemctl --user stop mlqd-update.service
```

## Quick start

```bash
# exclusive smoke job (default maxParallelRuns = 1, priority = 0)
mlq submit --name smoke -- python train.py --smoke

# allow sharing with other cooperative jobs (only when known-safe)
mlq submit --name cleanrl --max-parallel-runs 3 -- python train.py

# prefer this job over default-priority queue work (does not preempt runners)
mlq submit --name urgent --priority 1 --max-parallel-runs 1 -- python eval.py

# durable smoke gate: if smoke submission returns job 12
mlq submit --name train --after-success 12 -- python train.py

# bound each attempt to two hours (queue wait is not counted)
mlq submit --name bounded --time-limit 2h -- cargo run --release --bin train

mlq status                # live queue: running, queued, held
mlq status -f             # recent finished runs only
mlq show 1
mlq see 1                 # detail plus latest stdout
mlq see                   # most recently submitted job
mlq logs 1 --follow       # exit code matches the attempt
mlq follow 1              # subscribe; print only the final outcome
mlq cancel 1              # SIGTERM; add --force for SIGKILL after grace
mlq pause                 # no new starts; queued work waits; running work continues
mlq unpause               # resume admission
```

## Submitting from different repositories

Each Linux user has a shared queue. Run `mlq` from any repository and set
`--cwd` to the workload's directory when needed. Use the same CLI and daemon
socket across projects; different `MLQUEUE_RUNTIME_DIR` values select different
sockets.

Submit the foreground workload. A queue script that submits and waits for
other jobs can block those jobs if it holds a lease itself. The CLI captures
`PATH`, including an activated virtual environment. Pass other needed variables
with `--env` or `--inherit-env`, or use an absolute interpreter path.

Default locations:

| Data | Default location |
|---|---|
| Daemon configuration | `$HOME/.config/mlqueue/config.toml` |
| Jobs, classes, events, and default job | `$HOME/.local/state/mlqueue/mlqueue.db` |
| Attempt logs and control records | `$HOME/.local/state/mlqueue/attempts/<attempt-id>/` |
| Daemon socket | `$XDG_RUNTIME_DIR/mlqueue/mlqd.sock`, or the state directory's `runtime/mlqd.sock` when `XDG_RUNTIME_DIR` is unset |

`XDG_CONFIG_HOME` and `XDG_STATE_HOME` replace the home defaults.
`MLQUEUE_CONFIG_FILE`, `MLQUEUE_STATE_DIR`, and `MLQUEUE_RUNTIME_DIR` override
paths directly. Use consistent overrides for the CLI and daemon.
`mlq daemon status --json` reports the database and socket paths.
Checkpoints and experiment outputs stay where the workload writes them,
often relative to `--cwd`.

## Default job for idle periods

Submit one recurring command as the default job:

```bash
mlq submit --default --name idle-work --cwd /absolute/project \
  -- /absolute/project/.venv/bin/python idle_work.py
```

The default runs alone while no ordinary job is eligible. Held jobs, unresolved
dependencies, retry delays, and closed windows leave room for idle work. The
default also obeys class pauses and run windows. It requires
`--max-parallel-runs 1`; ordinary jobs always take precedence over its priority.

When ordinary work becomes ready, the runner stops the default with SIGTERM
and, if needed, SIGKILL after the configured grace period. Ordinary jobs wait
until its process group exits. The request survives daemon restarts and appears
in the event log as `default_yield_requested`.

After yielding or succeeding, the default requeues. Each attempt restarts the
command and has separate logs; the workload must checkpoint to resume useful
work. Restarts wait at least one second, or longer if `--retry-delay` says so.
Failures, including time-limit and window expiry, use `--max-attempts`, counted
since the last success or yield. The default budget of one stops a failing job.

An active default interrupted by reboot restarts after the daemon proves its
stored boot ID differs from the current boot. Explicit cancellations, holds,
quarantine, and exhausted failure budgets stay in effect. A timeout committed
before reboot still counts as a failure.

Stop the default and restart its saved command later:

```bash
mlq default end
mlq default start
```

`end` cancels the current attempt and disables recurrence. Wait for it to finish
stopping before using `start`. Add `--force` to escalate to SIGKILL after the
cancellation grace period. `start` requeues the saved default with its original
arguments and settings, while keeping its attempt history. It uses the active
default, or the most recently submitted default when none is active. Ordinary
jobs still take precedence.

`mlq cancel JOB --force` also disables the default, even during a pending yield.
Cancel it before submitting a replacement or activating a staged daemon update,
since it counts as unfinished work. A queued default supports hold/release.
The default cannot depend on other jobs or serve as a prerequisite. `show` and
JSON status expose `isDefault`; human status labels the default.

## Job classes and daily run windows

Group ablations in a class with a daily window. This example runs from
11 PM to 7 AM at a fixed UTC offset of minus four hours:

```bash
mlq class create ablations --run-window '23:00-07:00@-04:00'
mlq submit --class ablations --name baseline --max-parallel-runs 4 -- python train.py --variant baseline
mlq submit --class ablations --name no-dropout --max-parallel-runs 4 -- python train.py --variant no-dropout
mlq class show ablations
mlq class list --json
```

A job can join one existing class. Classes keep the global `maxParallelRuns`
rule and leave GPU placement to the submitter. Here, each job declares that
sharing with up to four managed jobs is safe.

Use `submit --run-window '23:00-07:00@-04:00'` for an individual job.
Both the job and class windows must be open. Otherwise, the job stays queued
and `status` or `show` explains the wait. Other eligible jobs can run. Waiting
for a window preserves readiness order; priority and concurrency still apply.

Windows repeat daily and can cross midnight. They include the start and exclude
the end. Use `HH:MM-HH:MM@+HH:MM` or `HH:MM-HH:MM@-HH:MM`. No offset, or `@Z`,
means UTC. Offsets do not follow daylight saving time. Equal endpoints are
invalid; clear a window to allow any time.

Each attempt stores its closing time. The runner refuses late launches and
sends SIGTERM at close, followed by SIGKILL after `cancel_grace_ms` if needed.
Shutdown can extend past the window by the grace period and polling delay.
`runner.log` records closure and signals; `result.json` reports `window_closed`
after the process group exits.

Window closure fails the attempt. Retries follow `--max-attempts` and
`--retry-delay`, wait for an open window, and restart the command. The workload
must checkpoint to resume training. With one attempt, an interrupted job fails
without running again the next night.

The deadline survives daemon restarts and live `--time-limit` changes. An
earlier cancellation keeps its outcome and suppresses retries.

```bash
# Pause or resume new starts. Running attempts continue.
mlq class pause ablations
mlq class unpause ablations

# Cancel all unfinished members. --force adds SIGKILL after the grace period.
mlq class cancel ablations --force

# Change queued or held jobs.
mlq set-class 42 ablations
mlq set-class 42 --clear
mlq set-run-window 42 '23:00-07:00@-04:00'
mlq set-run-window 42 --clear

# Change the shared window once the class has no live attempts.
mlq class set-run-window ablations '22:00-06:00@-04:00'
mlq class set-run-window ablations --clear
```

Class cancellation either commits for all members or fails before sending
signals. A quarantined member rejects the whole request. Cancelling a parent
can leave dependent jobs `skipped`; finished jobs stay in history. Class
commands support `--json` and mutation idempotency keys.

## Admission model

Every job declares one concurrency number, `--max-parallel-runs N` (default
**1**):

> This job is safe to run only while the total number of concurrent managed
> jobs, including itself, is at most **N**.

Admission is **symmetric**. A candidate starts only when the resulting set of
run leases still satisfies every running job’s limit *and* the candidate’s:

```text
|running| + 1  <=  min(candidate N, every running N)
```

| Situation | Result |
|---|---|
| Machine idle, job with `1` | Starts alone |
| Two jobs with `3`, candidate with `3` | Starts as the third |
| Two jobs with `3`, candidate with `1` | Waits until the machine is empty |
| Job with `1` running, candidate with `3` | Waits |
| Job with `2` + candidate with `4` | Candidate may start; effective cap is `2` |

Default `1` means “unknown work is exclusive.” Raise `N` only when the
workload is safe next to *arbitrary* other managed jobs, not only next to
copies of itself.

The queue does **not** discover GPUs, meter VRAM/CPU, infer job types, or
preempt ordinary work. Only an explicit default job yields to other jobs.
Submit through `mlq`, and keep the command (and descendants) foregrounded in
the runner's process group
(no `setsid` / daemonizing).

### Protection and backfill

When a restrictive job reaches the head of the queue, it is **protected**.
While the attempts that originally blocked it are still advancing, equal-
priority eligible jobs may backfill open slots once each. When those blockers
drain, the frontier freezes so later arrivals cannot starve the protected job
indefinitely.

### Priority

Each job has a signed `--priority P` (default **0**). Eligible jobs are ordered
by **higher priority first**, then FIFO within the same priority.

- Priority never preempts an ordinary job that holds a run lease.
- A protected job only allows equal-priority backfill.
- A newly eligible higher-priority job can replace a lower-priority
  reservation.
- Sustained high-priority submissions can starve lower-priority work—use
  nonzero values deliberately.

```bash
mlq submit --priority 1  --max-parallel-runs 1 -- COMMAND...   # ahead of priority 0
mlq submit --priority -1 --max-parallel-runs 3 -- COMMAND...   # behind priority 0
mlq set-priority 12 2     # retune queued or held job only
```

## CLI

| Command | Purpose |
|---|---|
| `submit` | Enqueue a command (`--` then argv) |
| `status` | Live queue (leases, protection, reasons) |
| `status -f` | Most recent finished runs only |
| `subscribe` | Stream queue snapshots as NDJSON (no polling) |
| `default --command PATH` | Active default or the command's latest default, including older history |
| `default end` / `default start` | Stop the default or restart its saved command |
| `show JOB` | Job detail and the latest 25 attempts |
| `attempts JOB` | Attempt history, newest first; page with `--before NUMBER --limit N` |
| `see [JOB]` | Job detail and latest stdout; latest job if omitted |
| `logs JOB` | Stdout/stderr; `--follow` exits with attempt outcome |
| `follow JOB` | Subscribe; final outcome only by default, opt-in events/checks/hooks |
| `cancel JOB` | Cancel; `--force` escalates to SIGKILL after grace |
| `hold` / `release` | Park and restore a queued job |
| `pause` / `unpause` | Stop / resume new starts (running work continues) |
| `retry JOB` | Requeue a failed or lost job |
| `set-max-parallel-runs JOB N` | Change limit on queued **or live** jobs |
| `set-priority JOB P` | Change priority on **queued/held** jobs only |
| `set-time-limit JOB DURATION` | Change (`--clear` removes) the wall-clock limit on queued **or live** jobs |
| `class …` | Create, list, inspect, pause, unpause, cancel, or set the window of a job class |
| `set-class JOB CLASS` | Change a queued or held job's class; `--clear` removes membership |
| `set-run-window JOB WINDOW` | Change a queued or held job's daily window; `--clear` removes it |
| `follow-tts` | Speak completions and newly running work via local `tts` |
| `daemon …` | `status` / `run` / `install` / `uninstall` |
| `recover …` | List or resolve orphaned/quarantined attempts |


Common flags:

- `--json` on most commands for machine-readable output
- `--idempotency-key KEY` on mutations (auto-generated if omitted; safe to
  retry an identical request)
- Submit: `--name`, `--default`, `--class`, `--run-window`, `--cwd`, `--env KEY=VAL`, `--inherit-env VAR`,
  `--max-attempts`, `--retry-delay`, `--time-limit`, `--after-success JOB`,
  `--after-terminal JOB`

`submit --time-limit DURATION` is a per-attempt wall-clock limit. It starts
when the runner spawns the command, so queue wait is excluded, while command
startup, compilation, and all descendants are included. Expiry sends SIGTERM
to the command process group, escalates to SIGKILL after the configured grace
period, and records a failed attempt; normal retry policy still applies.
The detached runner enforces the limit across daemon restarts; if that runner
dies, the daemon resumes enforcement from the persisted command start time.
As with any failed attempt, a separate user cancellation suppresses retries.
`mlq set-time-limit JOB DURATION` changes that bound for a queued job or one
that is already running. The clock does not restart: shortening below the
elapsed time begins termination, lengthening extends it, and `--clear`
removes it. A termination that has already started is not reversed, including
when the runner dies during its grace period. An earlier cancellation stays a
cancellation. Automatic retries use the updated limit.

### `follow` and `logs --follow` exit codes

| Outcome | Exit |
|---|---|
| Success | `0` |
| Command failed | command’s exit code |
| Killed by signal | `128 + signal` |
| Skipped, lost, cancelled, or failed before launch | `1` |
| Observation timeout expired (`follow`: 55 minutes by default) | `124` |

### Quiet job following

```bash
mlq follow 42                         # only final outcome; no scripts
mlq follow 42 --json                  # one compact final NDJSON event
mlq follow 42 --events started,retry   # opt into starts and retries too
mlq follow 42 --config ./follow.toml   # explicitly selected configuration
mlq follow 42 --config ./follow.toml --only-finish
mlq follow 42 --config ./follow.toml --no-hooks --no-checks
```

`follow` replaces `wait`: it blocks on one job-scoped subscription until the job
is terminal or 55 minutes elapse, preserving outcome exit codes without polling.
Override the observation duration with `--timeout DURATION`. The default leaves
five minutes of headroom for agents with a 60-minute KV-cache TTL.
On expiry it prints only `follow timed out after 55 minutes` to stderr (using
the configured duration in minutes) and exits `124`, including in JSON mode.
The job stays queued or running. Reattach with `mlq follow JOB` if waiting is
still useful; otherwise record the job ID and stop following. Do not resubmit
or retry the workload just because observation timed out.
No heartbeats, progress chatter, log tails, repeated snapshots, or LLM calls.
Final output is always included. `--events` replaces the configuration's
intermediate-event selection; `--only-finish` wins over both. No configuration
is discovered automatically. Attaching to an already-running attempt does not
replay its start. Retries do not finish the follower; a final job outcome does.
`follow --json` emits compact NDJSON rather than the old `wait --json` full job
detail; use `show JOB --json` when attempt history is needed.
Daemon disconnection is an error, not success; neither a timeout nor
stopping the follower cancels the job. Restarting the CLI starts a new monitoring
session, without historical notification replay or persisted warning state.
`--timeout` bounds waiting for the job, not finish-hook delivery after its outcome
is known. Finish hooks retain their own configured timeouts and cannot change
the job's exit status.

Available opt-in events: `started` (actually running), `retry` (another attempt
scheduled), `attention` (operator recovery required), and `warning` (local check
finding). An existing recovery condition is reported once on attachment if
`attention` is selected.

Example explicit `follow.toml`:

```toml
events = ["warning", "attention"] # final outcome is always included

[[checks]]
id = "returns"
command = ["./check-returns"]
every = "60s"                    # default; runs only while actually running
timeout = "5s"                   # default
clear_after = 2                  # default: fresh healthy observations to rearm

[[hooks]]
events = ["warning", "finished"]
command = ["./notify-supervisor"]
timeout = "5s"
```

Checks run **only when `warning` is selected**. Merely listing a check does not
enable it. Hooks run only for events selected by both the follower and the hook.
`--only-finish` skips all checks and intermediate hooks; an explicitly configured
finish hook still runs. Add `--no-hooks` to disable that too. Invalid/unknown
configuration is rejected even when an override disables execution.
Only use configurations and scripts you trust: commands execute with your user
permissions, not in a security sandbox.

Commands are argv arrays, not shell snippets. Executable paths containing `/`
resolve relative to the configuration file; bare names use `PATH`. Arguments
and working directory are not rewritten: commands inherit the follower's cwd
and environment. Use JSON `cwd`/`logDir` to locate the job's artifacts. Commands
receive one JSON object on stdin:

- Checker context: `{"job":42,"attempt":1,"state":"running","cwd":"/project","logDir":"/state/attempts/7"}`.
- Hook context: the same compact event printed by `follow --json`.

A checker returns a JSON object on stdout; no findings means healthy:

```json
{"step":18400,"findings":[{"key":"returns-ema-declining","message":"Return EMA declined across 20 evaluations"}]}
```

Return `{"step":18500,"findings":[]}` for a healthy new observation. The optional
top-level `step` must advance before a result counts as a new observation;
re-reading the same step cannot trigger or clear a warning. A finding may also
carry `step` as evidence. Metric parsing, EMA windows, and “higher is better”
belong to the project checker, not the scheduler. Check only new metric records
where possible; don't reread whole training logs or call an LLM every minute.

Notification bounds:

- Findings deduplicate by check ID and stable key within an attempt, regardless
  of changed wording or numbers. Persistent conditions do not produce reminders.
- A key rearms only after `clear_after` successful fresh observations omit it.
  Failed checks do not clear active findings. A new attempt resets check state.
- Across all checks, at most one warning batch per minute, with at most eight
  findings. Excess findings are suppressed, not queued for a later burst; the
  final event includes `suppressedWarnings` when nonzero.
- Notification messages are capped at 240 characters. A broken checker reports
  one diagnostic until it recovers; hook errors produce one stderr diagnostic
  per follower session. Raw command stdout/stderr is never forwarded.
- Config, command input, and stdout are bounded to 64 KiB each. There are at most
  32 checks/hooks, 64 findings per check response, and 256 active keys per check.
  Check intervals range from 1 second to 7 days; command timeouts are positive
  and at most 5 minutes. Each check/hook stage runs serially; at most one check
  and one notification command can execute concurrently.
- Hooks have an eight-event mailbox and never block job observation. Overflow
  is discarded and counted as `suppressedHooks` in the final output. Intermediate
  hook delivery is best-effort: final output preempts queued/in-flight intermediate
  hooks, then runs explicitly selected finish hooks.
- A job transition interrupts in-flight checks. Timeouts, SIGINT, SIGTERM, and
  closed output pipes clean up command process groups. SIGKILL cannot run cleanup;
  scripts must not daemonize or escape their groups. Commands never run inside
  `mlqd`; findings and hook failures never trigger automatic job mutations.

Stdout updates only wake an LLM if its harness supports streaming notifications.
For other harnesses, explicitly configure a hook targeting its notification API.

### `subscribe` for programmatic consumers

```bash
mlq subscribe | while IFS= read -r snapshot; do ...; done
```

`subscribe` keeps one connection to `mlqd` open and prints **one compact JSON
queue snapshot per line** (NDJSON, flushed immediately): the current state right
away, then a new line whenever the queue changes — submission, state transition,
lease change, priority or limit change. Each line is the daemon's raw `status`
payload, which (unlike `mlq status --json`) still carries recently finished jobs.
Jobs are ordered with running work first, then other live work, then recent
finished history. Consumers should still select by `state` rather than relying
on a particular job ID or assuming there is only one running job.
Consumers never replay events; bursts are coalesced into the newest snapshot
instead of a backlog, and a refresh identical to the previous line is skipped.

No rendering, no colours, no polling. It exits `0` when the daemon shuts down
and non-zero with a message on stderr if the daemon cannot be reached, so a
supervisor can restart it. Streams are budgeted at a quarter of
`max_connections`, so an excess subscriber is refused with
`too_many_subscriptions` rather than starving `submit`/`status`.
`mlq status --watch` remains the human, redrawing view.

### Changing a live limit

`set-max-parallel-runs` updates the job, its live attempt, and its run lease
atomically. Lowering a limit below the current number of active leases is
allowed: existing work keeps running (no preemption), and the scheduler
refuses new admissions until the active set drains to a compatible size.

`set-time-limit` updates the job immediately and publishes the new bound to
the live runner, which adopts it on its next poll. The same value is what a
later attempt is launched with. Lowering a limit that has already expired
stops that attempt. Other leases are left running.

### Pausing the queue

`mlq pause` is a durable operator gate: new submissions still enqueue, running
jobs keep their leases, and nothing queued becomes `starting`/`running` until
`mlq unpause`. Typical use is to drain or kill current work before leaving the
machine, then resume later. The flag is stored in SQLite, so it survives
daemon restart. `mlq status` and `mlq daemon status` show when the queue is
paused.


## Submit recipe for agents

```bash
mlq submit \
  --name NAME \
  --max-parallel-runs N \
  --cwd /absolute/repo/path \
  -- COMMAND...
```

- Always set `--max-parallel-runs` explicitly (`1` for exclusive / unknown /
  multi-GPU / benchmarks).
- Omit `--priority` unless queue precedence is intentional.
- Use `--after-success JOB` for durable prerequisite gates and
  `--after-terminal JOB` for cleanup after any terminal outcome. Repeated
  dependency flags are ANDed. Metric-gated or output-derived pipelines still
  belong in the submitter.
- Secrets: prefer files over `--env` (env is stored in SQLite as plaintext).

A ready-made agent skill ships in [`skills/queue-ml-jobs/`](skills/queue-ml-jobs/).
Install or link it next to the CLI in agent environments.

## Durability

- **SQLite** (WAL, `synchronous=FULL`) under `$XDG_STATE_HOME/mlqueue`
- **Unix socket** under `$XDG_RUNTIME_DIR/mlqueue` with peer-UID checks
- Each attempt is supervised by a small detached **runner**; daemon restarts
  and upgrades do not kill workers—the new daemon adopts live runners and
  finalizes results exactly once
- A run lease is held until the attempt’s **entire process group** is gone
- Cancel is SIGTERM, then optional SIGKILL after a configured grace period
- A time-limited attempt always escalates to SIGKILL after the same grace
  period if its process group has not drained

## Development

```bash
cargo test        # unit + property tests and e2e (real daemons/runners/CLI)
cargo clippy
cargo install --path . --force && mlq daemon install   # rebuild + restart unit
```

The e2e suite (`tests/e2e.rs`) covers the concurrency formula, signed priority
ordering, windowed-then-frozen backfill, dependency gates and skip cascades,
live limit updates, cancellation, wall-clock limits, retries, idempotency
replay/conflict, and daemon-crash recovery.
