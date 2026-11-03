# mlqueue

mlqueue coordinates local ML jobs for one Linux user across repositories.
Submit commands with `mlq`; the durable `mlqd` daemon limits concurrent runs.
It does not allocate GPUs or sandbox commands.

See [ARCHITECTURE.md](ARCHITECTURE.md) for scheduling rules and internals.

## Install

```bash
cargo install --path .    # installs mlq + mlqd into ~/.cargo/bin
mlq daemon install        # systemd user unit: enable + start
mlq --version
mlq daemon status
```

Without systemd, run the daemon in the foreground:

```bash
mlq daemon run
```

### Stage daemon updates for downtime

Stage daemon updates while jobs are active. The optional updater requires
Python 3.11+ and a persistent systemd user daemon with `KillMode=process`.

```bash
./contrib/mlq-update install          # one-time: installs ~/.local/bin/mlq-update
cargo build --release --locked -j10
mlq-update stage --mlqd target/release/mlqd
cargo install --path . --locked --force --bin mlq
mlq-update status
```

Stage before upgrading the CLI so the updater saves a compatible observer.
Staging another candidate replaces the pending one.

The updater waits until all jobs finish and all leases are released. Held jobs,
delayed retries, and recovery states block activation. It then stops the daemon,
locks and rechecks the database, replaces the executable, and restarts the
service. A submission during shutdown defers activation. Clients may briefly
disconnect.

Pending updates survive logout and reboot. Connection failures retry after
30 seconds. Failed readiness retains the pending update for inspection without
automatically downgrading the binary. The previous executable stays at
`$XDG_STATE_HOME/mlqueue-updates/previous-mlqd`, defaulting to
`~/.local/state/mlqueue-updates/previous-mlqd`.

```bash
systemctl --user status mlqd-update.path mlqd-update.service
journalctl --user -u mlqd-update.service
# Disable automatic activation (the staged candidate remains available):
systemctl --user disable --now mlqd-update.path
systemctl --user stop mlqd-update.service
```

## Quick start

```bash
# Run an exclusive job. Default priority is 0.
mlq submit --name smoke -- python train.py --smoke

# Allow up to three concurrent managed jobs when safe.
mlq submit --name cleanrl --max-parallel-runs 3 -- python train.py

# Run ahead of default-priority jobs without preempting running work.
mlq submit --name urgent --priority 1 --max-parallel-runs 1 -- python eval.py

# Start only after job 12 succeeds.
mlq submit --name train --after-success 12 -- python train.py

# Limit each attempt to two hours, excluding queue wait.
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

Run `mlq` from any repository. Set `--cwd` when the workload needs another
directory. Use the same daemon socket across projects; `MLQUEUE_RUNTIME_DIR`
selects its location.

Submit the workload directly. A job that submits and waits for other jobs can
block them with its own lease. The CLI captures `PATH`, including an activated
virtual environment. Pass other variables with `--env` or `--inherit-env`, or
use an absolute interpreter path.

Default locations:

| Data | Default location |
|---|---|
| Daemon configuration | `$HOME/.config/mlqueue/config.toml` |
| Jobs, classes, events, and default job | `$HOME/.local/state/mlqueue/mlqueue.db` |
| Attempt logs and control records | `$HOME/.local/state/mlqueue/attempts/<attempt-id>/` |
| Daemon socket | `$XDG_RUNTIME_DIR/mlqueue/mlqd.sock`, or the state directory's `runtime/mlqd.sock` when `XDG_RUNTIME_DIR` is unset |

`XDG_CONFIG_HOME` and `XDG_STATE_HOME` replace the home directory defaults.
`MLQUEUE_CONFIG_FILE`, `MLQUEUE_STATE_DIR`, and `MLQUEUE_RUNTIME_DIR` override
paths directly. Use consistent overrides for the CLI and daemon.
`mlq daemon status --json` reports the database and socket paths.
Workloads choose their output paths, often relative to `--cwd`.

## Default job for idle periods

Submit a recurring command to run while the queue is idle:

```bash
mlq submit --default --name idle-work --cwd /absolute/project \
  -- /absolute/project/.venv/bin/python idle_work.py
```

The default requires `--max-parallel-runs 1` and runs alone when no ordinary job
is eligible. It obeys class pauses and run windows. When ordinary work becomes
ready, the runner sends SIGTERM, then SIGKILL after the grace period if needed.
Ordinary jobs wait for its process group to exit, regardless of priority.

Success or yielding requeues the default after the longer of one second and
`--retry-delay`. Each attempt restarts the command with separate
logs, so the workload must checkpoint. Failures use `--max-attempts`, counted
since the last success or yield. The default budget of one stops a failing job.
Reboot recovery restarts interrupted defaults while preserving cancellations,
holds, quarantine, and failure budgets.

Stop the default and restart its saved command later:

```bash
mlq default end
mlq default start
```

`end` cancels the attempt and disables recurrence. Add `--force` to escalate to
SIGKILL after the grace period. Wait for it to stop before using `start`, which
requeues the active or latest saved default with its settings and history.

`mlq cancel JOB` also disables recurrence. Cancel the default before replacing
it or activating a staged daemon update, since it counts as unfinished work.
A queued default supports hold/release. Defaults cannot have dependencies or
serve as prerequisites.

## Job classes and daily run windows

Group jobs with a daily window. This class runs from 11 PM to 7 AM at UTC-04:00:

```bash
mlq class create ablations --run-window '23:00-07:00@-04:00'
mlq submit --class ablations --name baseline --max-parallel-runs 4 -- python train.py --variant baseline
mlq submit --class ablations --name no-dropout --max-parallel-runs 4 -- python train.py --variant no-dropout
mlq class show ablations
mlq class list --json
```

A job can join one existing class and have its own `--run-window`. Both windows
must be open before it starts. Closed windows preserve readiness order while
other eligible jobs run. Classes still obey global concurrency limits; submitters
choose GPU placement.

Windows include the start, exclude the end, and can cross midnight. Use
`HH:MM-HH:MM@+HH:MM` or `HH:MM-HH:MM@-HH:MM`. Omit the offset or use `@Z` for
UTC. Offsets do not follow daylight saving time. Equal endpoints are invalid.

At close, the runner sends SIGTERM, then SIGKILL after `cancel_grace_ms` if
needed. Shutdown may extend past the window. Closure fails the attempt with
`window_closed`; retries obey `--max-attempts` and `--retry-delay` and wait for
an open window. Commands restart, so checkpoint to resume training. With one
attempt, interrupted jobs do not run again the next night. Window deadlines
survive daemon restarts and live time-limit changes; cancellation suppresses retries.

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

Class cancellation commits for all unfinished members or fails before sending
signals. Quarantine rejects the request; cancelling prerequisites can skip
dependent jobs. Finished jobs stay in history.

## Admission model

`--max-parallel-runs N` limits total concurrent managed jobs, including the job
itself. The default is `1`. A candidate starts only if every job's limit permits it:

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

Use `1` for unknown workloads. Raise `N` only if the workload can safely share
with arbitrary managed jobs. mlqueue does not measure GPU, VRAM, or CPU use.
Keep commands and descendants in the runner's foreground process group;
do not use `setsid` or daemonize them.

### Protection and backfill

The queue protects a restrictive job at its head. Equal-priority jobs may fill
open slots once each while its original blockers advance. Once those blockers
drain, later arrivals cannot indefinitely delay the protected job.

### Priority

`--priority P` is a signed integer, default `0`. Higher priorities run first,
then FIFO within each priority. Priority does not preempt ordinary running jobs.
A newly eligible higher-priority job can replace a lower-priority reservation.
Sustained high-priority submissions can starve lower-priority work.

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
| `set-max-parallel-runs JOB N` | Change concurrency limit on queued or live jobs |
| `set-priority JOB P` | Change priority on queued or held jobs |
| `set-time-limit JOB DURATION` | Change time limit on queued or live jobs; `--clear` removes it |
| `class …` | Create, list, inspect, pause, unpause, cancel, or set the window of a job class |
| `set-class JOB CLASS` | Change a queued or held job's class; `--clear` removes membership |
| `set-run-window JOB WINDOW` | Change a queued or held job's daily window; `--clear` removes it |
| `follow-tts` | Speak completions and newly running work via local `tts` |
| `daemon …` | `status` / `run` / `install` / `uninstall` |
| `recover …` | List or resolve orphaned/quarantined attempts |

Common flags:

- `--json` on most commands for machine-readable output
- `--idempotency-key KEY` on mutations, generated if omitted. Reuse the key to
  retry an identical request safely.
- Submit: `--name`, `--default`, `--class`, `--run-window`, `--cwd`, `--env KEY=VAL`, `--inherit-env VAR`,
  `--max-attempts`, `--retry-delay`, `--time-limit`, `--after-success JOB`,
  `--after-terminal JOB`

`--time-limit DURATION` counts from command spawn, including startup,
compilation, and descendants. Expiry sends SIGTERM, then SIGKILL after the grace
period, and fails the attempt. Normal retries apply unless cancelled. Enforcement
survives daemon restarts.

`set-time-limit` changes queued or live jobs without restarting the clock.
Shortening below elapsed time begins termination; extending or clearing the
limit cannot reverse termination already underway. Retries use the updated limit.

### `follow` and `logs --follow` exit codes

| Outcome | Exit |
|---|---|
| Success | `0` |
| Command failed | Command's exit code |
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

`follow` replaces `wait`. It subscribes without polling until the job finishes,
including retries, or 55 minutes elapse. Change the timeout with
`--timeout DURATION`. Expiry reports the timeout on stderr and exits `124`,
including in JSON mode. Timeout or stopping the follower leaves the job running
or queued. Reattach to the same job rather than resubmitting it. Daemon
disconnection is an error.

The final outcome always prints. Opt into `started`, `retry`, `attention` for
recovery conditions, or `warning` for local checks. `--events` overrides the
configuration; `--only-finish` suppresses intermediate events and checks.
History is not replayed. Each follower starts a new notification session.
Use `show JOB --json` for attempt history.

Configuration requires an explicit `--config` path. Example `follow.toml`:

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

Checks require `warning` events and run only while the job is running. Hooks
require the event to be selected by both follower and hook. `--only-finish`
still permits configured finish hooks; add `--no-hooks` to disable them.
Finish hooks have their own timeouts and cannot change the job's exit status.
Invalid configuration fails even when execution is disabled.

Commands use argument arrays and execute with your user permissions. Paths
containing `/` resolve relative to the configuration file; bare names use `PATH`.
Commands inherit the follower's working directory and environment. Each receives
JSON on stdin. Hooks receive the `follow --json` event; checkers receive:

```json
{"job":42,"attempt":1,"state":"running","cwd":"/project","logDir":"/state/attempts/7"}
```

A checker returns a JSON object on stdout; no findings means healthy:

```json
{"step":18400,"findings":[{"key":"returns-ema-declining","message":"Return EMA declined across 20 evaluations"}]}
```

Return `{"step":18500,"findings":[]}` for a healthy observation. If supplied,
`step` must advance to trigger or clear warnings. The project checker interprets
metrics; prefer reading new records over scanning full logs.

Findings deduplicate by check ID and key per attempt. A warning rearms after
`clear_after` fresh healthy observations; failed checks do not clear it. Warnings
are limited to one batch per minute and eight findings per batch. Excess warnings
and hook events are discarded and counted in the final output. Hooks never block
job observation; the final outcome takes precedence over intermediate hooks.
Check findings and hook failures do not trigger automatic job changes.

For agents without streaming notifications, configure a hook for their
notification API. See [following internals](ARCHITECTURE.md#job-event-subscriptions-and-local-following)
for protocol and notification handling.

### `subscribe` for programmatic consumers

```bash
mlq subscribe | while IFS= read -r snapshot; do ...; done
```

`subscribe` prints a compact NDJSON queue snapshot immediately and on changes,
using one connection without polling. Snapshots include recent finished jobs;
filter by `state`. Bursts coalesce into the newest snapshot, and identical
snapshots are skipped. It does not replay events.

Daemon shutdown exits `0`; connection failures exit nonzero. Subscribers are
limited to a quarter of `max_connections`; excess requests receive
`too_many_subscriptions`. Use `mlq status --watch` for an interactive display.

### Changing a live limit

`set-max-parallel-runs` updates the job, attempt, and lease atomically. Lowering
the limit below the active lease count keeps existing work running and blocks
new admissions until the limits permit them. The runner adopts live
`set-time-limit` changes on its next poll.

### Pausing the queue

`mlq pause` blocks new starts while submissions enqueue and running jobs
continue. It survives daemon restarts. Resume with `mlq unpause`; `mlq status`
and `mlq daemon status` report the pause state.

## Submit recipe for agents

```bash
mlq submit \
  --name NAME \
  --max-parallel-runs N \
  --cwd /absolute/repo/path \
  -- COMMAND...
```

- Set `--max-parallel-runs` explicitly. Use `1` for unknown workloads,
  multi-GPU jobs, and benchmarks.
- Omit `--priority` unless queue precedence is intentional.
- Use `--after-success JOB` for prerequisites and `--after-terminal JOB` for
  cleanup after any terminal outcome. All repeated dependencies must be met.
  The submitter handles conditions based on metrics or outputs.
- Pass secrets through files. `--env` values are stored as plaintext in SQLite.

Install or link the bundled [agent skill](skills/queue-ml-jobs/) in agent environments.

## Durability

SQLite uses WAL and `synchronous=FULL`. The Unix socket checks peer UIDs.
Detached runners survive daemon restarts and upgrades; the new daemon adopts
them and finalizes results once. Leases remain held until the entire process
group exits. Cancellation sends SIGTERM, with optional SIGKILL after the grace
period. Time limits always escalate if needed.

## Development

```bash
cargo test -j10   # unit, property, and e2e tests with real daemons/runners/CLI
cargo clippy -j10
cargo install --path . --force && mlq daemon install   # rebuild + restart unit
```

The [e2e suite](tests/e2e.rs) covers scheduling, dependencies, live limits,
cancellation, retries, idempotency, and crash recovery.
