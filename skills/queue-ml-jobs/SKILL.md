---
name: queue-ml-jobs
description: Submit, monitor, schedule, and control local ML workloads with mlq.
---

# Queue local ML work

## Submit work

Submit every workload through `mlq`. Keep its descendants in the runner's
foreground process group. Start `mlqd` if needed.

```bash
mlq submit --name NAME --max-parallel-runs N --cwd /absolute/path -- COMMAND...
```

- Set `--max-parallel-runs N` to the safe total number of concurrent managed
  jobs, including this one. Use `1` for unknown or exclusive work.
- Omit `--priority` unless queue order matters. Higher values go first; equal
  priorities use FIFO. Ordinary running jobs are not preempted.
- Set retries with `--max-attempts` and `--retry-delay`. `--time-limit` bounds
  each attempt, excluding queue wait.
- Pass environment with `--env KEY=VALUE` or `--inherit-env VAR`. Dependencies
  use `--after-success JOB` or `--after-terminal JOB`; repeated flags are ANDed.
- Add `--default`, `--class NAME`, or `--run-window WINDOW` for scheduling below.

## Command guide

Commands below follow `mlq`. Use `--help` for full syntax, `--json` where
supported, and the same `--idempotency-key KEY` when retrying an identical mutation.

| Command | Use |
| --- | --- |
| `status [--watch]` / `status --finished` | Inspect the live queue and admission reasons, or recent finished jobs. |
| `show JOB` / `see [JOB]` | Read job details and the latest 25 attempts, or details with stdout. `see` defaults to the latest job. |
| `attempts JOB [--before N] [--limit N]` | Page history newest first. The cursor is exclusive; the limit is 1 to 100. |
| `logs JOB [--attempt N] [--stderr] [--follow]` | Read or follow captured output; the latest attempt is the default. |
| `follow JOB [--timeout DURATION]` | Wait for the final outcome. The default timeout is 55m; expiry exits 124 and leaves the job running. |
| `follow JOB [--events EVENTS] [--config FILE]` | Enable selected events or TOML checks/hooks. Checks report metrics only. |
| `subscribe` / `follow-tts` | Stream queue snapshots as NDJSON, or speak transitions through local `tts`. |
| `cancel JOB [--force]` | Cancel a job with SIGTERM; `--force` adds SIGKILL after the grace period. |
| `hold JOB` / `release JOB` | Park a queued job or return a held job to the queue. |
| `retry JOB` | Requeue a failed or lost job for another attempt. |
| `set-max-parallel-runs JOB N` | Change a queued or live job's concurrency declaration. |
| `set-priority JOB P` | Change a queued or held job's priority. |
| `set-time-limit JOB DURATION` / `--clear` | Change or remove a queued or live job's limit, measured from its original start. |
| `default --command PATH` | Look up the active default or the command's latest default history. |
| `default end [--force]` / `default start` | Stop recurrence or restart the saved command after it has stopped. |
| `class create NAME [--run-window WINDOW]` | Create a class before submitting members with `--class NAME`. |
| `class list` / `class show NAME` | Inspect class pause states and windows. |
| `class pause NAME` / `class unpause NAME` | Stop or resume new starts for a class. |
| `class cancel NAME [--force]` | Cancel all unfinished members; quarantined members reject the operation. |
| `class set-run-window NAME WINDOW` / `--clear` | Change or remove the class window when it has no live attempts. |
| `set-class JOB CLASS` / `set-run-window JOB WINDOW` | Change a queued or held job's membership or window; `--clear` removes it. |
| `pause` / `unpause` | Stop or resume new queue admissions. Running jobs continue. Change this gate only when asked. |
| `daemon status` / `run` / `install` / `uninstall` | Inspect, run in the foreground, install, or remove the systemd user service. |
| `recover list` / `recover resolve JOB --attempt N --as lost` | Inspect orphaned/quarantined attempts; resolve only after their processes are gone. `--as cancelled` is also supported. |

## Scheduling rules

A default job runs alone during idle periods and yields to eligible ordinary
work. Only one unfinished default is allowed; it requires concurrency `1` and
cannot participate in dependencies. Success or yield restarts the command, so
checkpoint work. Failures consume the retry budget since the last success or
yield. Holding, cancelling, quarantine, or exhausted retries stops recurrence.

Windows repeat daily, for example `23:00-07:00@-04:00`. Offsets are fixed;
omitting one means UTC. Both job and class windows must be open. Window closure
terminates the attempt and applies normal retries. Classes share the global
concurrency rules. Queue and class pauses leave running attempts alone.

## Daemon updates

Stop the default job before updating. Do not replace or restart a busy daemon.
Build and stage the daemon before installing the CLI from the same build:

```bash
cargo build --release --locked -j10
mlq-update stage --mlqd target/release/mlqd
cargo install --path . --locked --force --bin mlq
mlq-update status
```

Staging never fetches Git. See [README.md](../../README.md) for detailed behavior
and configuration examples.
