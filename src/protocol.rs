//! Framed Unix-socket protocol: big-endian u32 length-prefixed JSON with a
//! hard frame limit checked before allocation, plus the stable public JSON
//! views (camelCase, per the architecture's `maxParallelRuns` convention).

use std::collections::BTreeMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::domain::{AttemptId, DependencyCondition, DependencyResolution, JobId};
use crate::run_window::RunWindow;

pub const PROTOCOL_VERSION: u32 = 9;
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 1 << 20;

fn is_zero(value: &i64) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

// Stable error codes.
pub mod error_codes {
    pub const UNSUPPORTED_PROTOCOL: &str = "unsupported_protocol";
    pub const MALFORMED_REQUEST: &str = "malformed_request";
    pub const MISSING_IDEMPOTENCY_KEY: &str = "missing_idempotency_key";
    pub const IDEMPOTENCY_CONFLICT: &str = "idempotency_conflict";
    pub const NOT_FOUND: &str = "not_found";
    pub const INVALID_STATE: &str = "invalid_state";
    pub const INVALID_ARGUMENT: &str = "invalid_argument";
    pub const UNSAFE_RESOLUTION: &str = "unsafe_resolution";
    pub const ADMISSION_BLOCKED: &str = "admission_blocked";
    /// The daemon's subscription slots are all in use; the stream was refused.
    pub const TOO_MANY_SUBSCRIPTIONS: &str = "too_many_subscriptions";
    pub const INTERNAL: &str = "internal";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub protocol_version: u32,
    pub request_id: String,
    /// Required for every mutating operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub op: Op,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Op {
    Submit(SubmitParams),
    Classes,
    ClassShow {
        name: String,
    },
    ClassCreate {
        name: String,
        run_window: Option<RunWindow>,
    },
    ClassPause {
        name: String,
        paused: bool,
    },
    ClassSetWindow {
        name: String,
        run_window: Option<RunWindow>,
    },
    ClassCancel {
        name: String,
        force: bool,
    },
    SetClass {
        job: JobId,
        class: Option<String>,
    },
    SetRunWindow {
        job: JobId,
        run_window: Option<RunWindow>,
    },
    Status,
    DefaultJob {
        command: String,
    },
    DefaultEnd {
        force: bool,
    },
    DefaultStart,
    Attempts {
        job: JobId,
        before: Option<i64>,
        limit: u32,
    },
    /// Turns the connection into a stream of `Status` snapshots (see
    /// ARCHITECTURE.md); the reply payload is identical to `Status`.
    Subscribe,
    /// Stream one job and lossless, cursor-paged lifecycle events.
    SubscribeJob {
        job: JobId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after: Option<i64>,
    },
    FollowTtsSnapshot,
    Events {
        after: i64,
        limit: u32,
    },
    Show {
        job: JobId,
    },
    /// Job detail; omit `job` to select the most recently submitted job.
    See {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        job: Option<JobId>,
    },
    Logs {
        job: JobId,
        attempt: Option<i64>,
    },
    Cancel {
        job: JobId,
        force: bool,
    },
    Hold {
        job: JobId,
    },
    Release {
        job: JobId,
    },
    /// Stop new admissions. Queued work stays queued; running leases continue.
    Pause,
    /// Resume admissions after [`Op::Pause`].
    Unpause,
    Retry {
        job: JobId,
    },
    SetMaxParallelRuns {
        job: JobId,
        max_parallel_runs: u32,
    },
    SetPriority {
        job: JobId,
        priority: i64,
    },
    /// Replace the per-attempt wall-clock limit. `None` clears it.
    SetTimeLimit {
        job: JobId,
        time_limit_ms: Option<u64>,
    },
    DaemonStatus,
    RecoverList,
    RecoverResolve {
        job: JobId,
        attempt: i64,
        resolve_as: ResolveAs,
    },
}

impl Op {
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            Op::Submit(_)
                | Op::ClassCreate { .. }
                | Op::ClassPause { .. }
                | Op::ClassSetWindow { .. }
                | Op::ClassCancel { .. }
                | Op::SetClass { .. }
                | Op::SetRunWindow { .. }
                | Op::DefaultEnd { .. }
                | Op::DefaultStart
                | Op::Cancel { .. }
                | Op::Hold { .. }
                | Op::Release { .. }
                | Op::Pause
                | Op::Unpause
                | Op::Retry { .. }
                | Op::SetMaxParallelRuns { .. }
                | Op::SetPriority { .. }
                | Op::SetTimeLimit { .. }
                | Op::RecoverResolve { .. }
        )
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Op::Submit(_) => "submit",
            Op::Classes => "classes",
            Op::ClassShow { .. } => "class_show",
            Op::ClassCreate { .. } => "class_create",
            Op::ClassPause { .. } => "class_pause",
            Op::ClassSetWindow { .. } => "class_set_window",
            Op::ClassCancel { .. } => "class_cancel",
            Op::SetClass { .. } => "set_class",
            Op::SetRunWindow { .. } => "set_run_window",
            Op::Status => "status",
            Op::DefaultJob { .. } => "default_job",
            Op::DefaultEnd { .. } => "default_end",
            Op::DefaultStart => "default_start",
            Op::Attempts { .. } => "attempts",
            Op::Subscribe => "subscribe",
            Op::SubscribeJob { .. } => "subscribe_job",
            Op::FollowTtsSnapshot => "follow_tts_snapshot",
            Op::Events { .. } => "events",
            Op::Show { .. } => "show",
            Op::See { .. } => "see",
            Op::Logs { .. } => "logs",
            Op::Cancel { .. } => "cancel",
            Op::Hold { .. } => "hold",
            Op::Release { .. } => "release",
            Op::Pause => "pause",
            Op::Unpause => "unpause",
            Op::Retry { .. } => "retry",
            Op::SetMaxParallelRuns { .. } => "set_max_parallel_runs",
            Op::SetPriority { .. } => "set_priority",
            Op::SetTimeLimit { .. } => "set_time_limit",
            Op::DaemonStatus => "daemon_status",
            Op::RecoverList => "recover_list",
            Op::RecoverResolve { .. } => "recover_resolve",
        }
    }

    /// Canonical request hash for idempotency-key conflict detection. Struct
    /// field order is deterministic and `env` is a BTreeMap, so equal payloads
    /// serialize identically.
    pub fn request_hash(&self) -> String {
        let mut canonical_op = self.clone();
        if let Self::Submit(params) = &mut canonical_op {
            params.dependencies.sort_by_key(|dependency| {
                (
                    dependency.job,
                    match dependency.condition {
                        DependencyCondition::Succeeded => 0,
                        DependencyCondition::Terminal => 1,
                    },
                )
            });
            params.dependencies.dedup();
            let succeeded: std::collections::BTreeSet<_> = params
                .dependencies
                .iter()
                .filter(|dependency| dependency.condition == DependencyCondition::Succeeded)
                .map(|dependency| dependency.job)
                .collect();
            params.dependencies.retain(|dependency| {
                dependency.condition == DependencyCondition::Succeeded
                    || !succeeded.contains(&dependency.job)
            });
        }
        let canonical = serde_json::to_vec(&canonical_op).expect("op serializes");
        hex::encode(Sha256::digest(&canonical))
    }

    /// Reconstruct the protocol-v5 submit hash for idempotent replay across
    /// the removal and restoration of dependencies. A replay only returns the
    /// original stored response; it never recreates migration-deleted edges.
    pub(crate) fn legacy_v5_request_hash(&self) -> Option<String> {
        let Self::Submit(params) = self else {
            return None;
        };
        if params.time_limit_ms.is_some()
            || params.class.is_some()
            || params.run_window.is_some()
            || params.is_default
        {
            return None;
        }
        let after_success: Vec<_> = params
            .dependencies
            .iter()
            .filter(|dependency| dependency.condition == DependencyCondition::Succeeded)
            .map(|dependency| dependency.job)
            .collect();
        let after_completion: Vec<_> = params
            .dependencies
            .iter()
            .filter(|dependency| dependency.condition == DependencyCondition::Terminal)
            .map(|dependency| dependency.job)
            .collect();
        let legacy = LegacyV5Op::Submit(LegacyV5SubmitParams {
            name: &params.name,
            cwd: &params.cwd,
            args: &params.args,
            env: &params.env,
            max_parallel_runs: params.max_parallel_runs,
            priority: params.priority,
            max_attempts: params.max_attempts,
            retry_delay_ms: params.retry_delay_ms,
            after_success: &after_success,
            after_completion: &after_completion,
        });
        let canonical = serde_json::to_vec(&legacy).expect("legacy op serializes");
        Some(hex::encode(Sha256::digest(&canonical)))
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum LegacyV5Op<'a> {
    Submit(LegacyV5SubmitParams<'a>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LegacyV5SubmitParams<'a> {
    name: &'a str,
    cwd: &'a str,
    args: &'a [String],
    env: &'a BTreeMap<String, String>,
    max_parallel_runs: u32,
    #[serde(skip_serializing_if = "is_zero")]
    priority: i64,
    max_attempts: u32,
    retry_delay_ms: u64,
    after_success: &'a [JobId],
    after_completion: &'a [JobId],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubmitParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_default: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_window: Option<RunWindow>,
    pub cwd: String,
    /// Exact argument vector; argv[0] is the executable.
    pub args: Vec<String>,
    /// Explicit persisted environment (baseline plus client-resolved values).
    pub env: BTreeMap<String, String>,
    pub max_parallel_runs: u32,
    // Omitting zero preserves request hashes for default-priority submissions
    // already recorded by protocol v3 daemons.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub priority: i64,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    /// Per-attempt wall-clock limit measured from command spawn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_limit_ms: Option<u64>,
    /// Immutable prerequisite edges. All conditions are ANDed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencySpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DependencySpec {
    pub job: JobId,
    pub condition: DependencyCondition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolveAs {
    Lost,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<Reply>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Reply {
    Classes {
        classes: Vec<ClassView>,
    },
    Class {
        class: ClassView,
    },
    ClassJobs {
        class: ClassView,
        jobs: Vec<JobView>,
    },
    Submitted {
        job: JobView,
    },
    Job {
        job: JobView,
    },
    Status(StatusView),
    DefaultJob {
        job: Option<JobView>,
    },
    Attempts {
        attempts: Vec<AttemptView>,
        next_before: Option<i64>,
    },
    FollowTtsSnapshot(FollowTtsSnapshotView),
    JobUpdate(JobUpdateView),
    Events {
        events: Vec<EventView>,
    },
    LogPaths(LogPathsView),
    DaemonStatus(DaemonStatusView),
    RecoverList {
        attempts: Vec<AttemptView>,
    },
    Paused {
        paused: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassView {
    pub name: String,
    pub paused: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_window: Option<RunWindow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobView {
    pub id: JobId,
    pub name: String,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_window: Option<RunWindow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_run_window: Option<RunWindow>,
    pub state: String,
    /// Why a non-terminal job is not running right now (derived, not stored).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eligibility: Option<String>,
    /// Extra detail for terminal or attention states.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    pub max_parallel_runs: u32,
    #[serde(default)]
    pub priority: i64,
    pub cwd: String,
    pub args: Vec<String>,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_limit_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_not_before: Option<i64>,
    /// Durable FIFO position assigned when all prerequisites and retry delay
    /// are satisfied. Absent while the job is not ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_sequence: Option<i64>,
    pub attempt_count: i64,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    /// Included by `show`; compact status responses rely on `eligibility`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<DependencyView>,
    pub attempts: Vec<AttemptView>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub attempts_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_requested: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobUpdateView {
    /// Compact current snapshot, including only the latest attempt.
    pub job: JobView,
    /// Resume after this durable event id. With no requested cursor, the
    /// initial snapshot starts at the current event-log head without replay.
    pub cursor: i64,
    pub events: Vec<EventView>,
    /// More matching events are ready; do not infer completion until drained.
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyView {
    pub parent: JobId,
    #[serde(
        alias = "requirement",
        deserialize_with = "deserialize_dependency_condition"
    )]
    pub condition: DependencyCondition,
    /// Empty only when replaying a protocol-v5 response stored before current
    /// parent-state visibility existed.
    #[serde(default)]
    pub parent_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution: Option<DependencyResolution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<i64>,
}

fn deserialize_dependency_condition<'de, D>(
    deserializer: D,
) -> Result<DependencyCondition, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "succeeded" | "success" => Ok(DependencyCondition::Succeeded),
        "terminal" | "completion" => Ok(DependencyCondition::Terminal),
        _ => Err(serde::de::Error::unknown_variant(
            &value,
            &["succeeded", "terminal"],
        )),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttemptView {
    pub id: AttemptId,
    pub job_id: JobId,
    pub number: i64,
    pub state: String,
    pub max_parallel_runs: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_end_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    pub log_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogPathsView {
    pub job: JobId,
    pub attempt: AttemptId,
    pub attempt_number: i64,
    pub attempt_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_signal: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusView {
    pub jobs: Vec<JobView>,
    pub active_leases: u32,
    /// `min` of active lease limits: the strictest declaration in force.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reservation: Option<ReservationView>,
    pub admission_blocked: bool,
    /// Operator gate: no new run leases until `mlq unpause`.
    #[serde(default)]
    pub paused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowTtsSnapshotView {
    pub running_jobs: Vec<FollowTtsJobView>,
    pub additional_running_jobs: u32,
    pub latest_event_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowTtsJobView {
    pub id: JobId,
    /// Bounded display name suitable for a framed notification response.
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventView {
    pub id: i64,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<JobId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Bounded display name; event details and command data are never exposed.
    pub job_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt: Option<AttemptId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_number: Option<i64>,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReservationView {
    pub protected_job: JobId,
    /// While true, an initial blocker of the protected job is still running
    /// and any eligible job may bypass it once.
    pub backfill_window_open: bool,
    /// The frozen frontier; absent while the backfill window is open.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backfill_cutoff: Option<i64>,
    pub created_at: i64,
    /// Attempts currently preventing the protected job from starting.
    pub blocking_attempts: Vec<AttemptId>,
    /// Jobs whose single backfill bypass is consumed.
    pub consumed_bypasses: Vec<JobId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DaemonStatusView {
    pub pid: u32,
    pub version: String,
    pub protocol_version: u32,
    pub scheduler_semantics_version: i64,
    pub started_at: i64,
    pub db_path: String,
    pub socket_path: String,
    pub active_leases: u32,
    pub queued_jobs: u32,
    pub admission_blocked: bool,
    #[serde(default)]
    pub paused: bool,
}

fn frame_len(bytes_len: usize, max: u32) -> io::Result<u32> {
    let len = u32::try_from(bytes_len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    Ok(len)
}

pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    max: u32,
) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    max: u32,
) -> io::Result<()> {
    let len = frame_len(bytes.len(), max)?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

pub fn read_frame_sync<R: Read>(reader: &mut R, max: u32) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds limit {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}

pub fn write_frame_sync<W: Write>(writer: &mut W, bytes: &[u8], max: u32) -> io::Result<()> {
    let len = frame_len(bytes.len(), max)?;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(bytes)?;
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_operations_require_idempotency_and_windows_are_validated() {
        let create: Op = serde_json::from_str(
            r#"{"type":"class_create","name":"night","run_window":"23:00-07:00@-04:00"}"#,
        )
        .unwrap();
        assert!(create.is_mutation());
        assert_eq!(create.kind(), "class_create");
        assert!(!Op::Classes.is_mutation());
        assert!(
            !Op::ClassShow {
                name: "night".into()
            }
            .is_mutation()
        );
        assert!(
            serde_json::from_str::<Op>(
                r#"{"type":"set_run_window","job":1,"run_window":"25:00-07:00"}"#
            )
            .is_err()
        );
        let alias: Op = serde_json::from_str(
            r#"{"type":"set_run_window","job":1,"run_window":"23:00-07:00@Z"}"#,
        )
        .unwrap();
        let canonical = Op::SetRunWindow {
            job: 1,
            run_window: Some("23:00-07:00@+00:00".parse().unwrap()),
        };
        assert_eq!(alias.request_hash(), canonical.request_hash());
        assert_ne!(
            alias.request_hash(),
            Op::SetRunWindow {
                job: 1,
                run_window: None
            }
            .request_hash()
        );
    }

    #[test]
    fn request_hash_is_stable_and_payload_sensitive() {
        let mk = |limit, priority| {
            Op::Submit(SubmitParams {
                name: "x".into(),
                is_default: false,
                class: None,
                run_window: None,
                cwd: "/tmp".into(),
                args: vec!["true".into()],
                env: BTreeMap::new(),
                max_parallel_runs: limit,
                priority,
                max_attempts: 1,
                retry_delay_ms: 0,
                time_limit_ms: None,
                dependencies: vec![],
            })
        };
        assert_eq!(mk(1, 0).request_hash(), mk(1, 0).request_hash());
        assert_ne!(mk(1, 0).request_hash(), mk(2, 0).request_hash());
        assert_ne!(mk(1, 0).request_hash(), mk(1, -1).request_hash());
        let Op::Submit(mut idle) = mk(1, 0) else {
            unreachable!()
        };
        idle.is_default = true;
        assert_ne!(
            Op::Submit(idle.clone()).request_hash(),
            mk(1, 0).request_hash()
        );
        assert!(Op::Submit(idle).legacy_v5_request_hash().is_none());
        let Op::Submit(mut classified) = mk(1, 0) else {
            unreachable!()
        };
        classified.class = Some("night".into());
        assert_ne!(
            Op::Submit(classified.clone()).request_hash(),
            mk(1, 0).request_hash()
        );
        assert!(
            Op::Submit(classified.clone())
                .legacy_v5_request_hash()
                .is_none()
        );
        classified.run_window = Some("23:00-07:00".parse().unwrap());
        assert_ne!(
            Op::Submit(classified.clone()).request_hash(),
            Op::Submit({
                let mut p = classified;
                p.run_window = None;
                p
            })
            .request_hash()
        );

        let mut dependency_order_a = match mk(1, 0) {
            Op::Submit(params) => params,
            _ => unreachable!(),
        };
        dependency_order_a.dependencies = vec![
            DependencySpec {
                job: 9,
                condition: DependencyCondition::Terminal,
            },
            DependencySpec {
                job: 4,
                condition: DependencyCondition::Succeeded,
            },
            DependencySpec {
                job: 9,
                condition: DependencyCondition::Succeeded,
            },
        ];
        let mut dependency_order_b = dependency_order_a.clone();
        dependency_order_b.dependencies = vec![
            DependencySpec {
                job: 9,
                condition: DependencyCondition::Succeeded,
            },
            DependencySpec {
                job: 4,
                condition: DependencyCondition::Succeeded,
            },
            DependencySpec {
                job: 9,
                condition: DependencyCondition::Succeeded,
            },
        ];
        assert_eq!(
            Op::Submit(dependency_order_a).request_hash(),
            Op::Submit(dependency_order_b).request_hash(),
            "flag order, duplicates, and weaker overlap are semantically identical"
        );

        let mut limited_params = match mk(1, 0) {
            Op::Submit(params) => params,
            _ => unreachable!(),
        };
        limited_params.time_limit_ms = Some(5_000);
        let limited = Op::Submit(limited_params);
        assert_ne!(mk(1, 0).request_hash(), limited.request_hash());
        assert!(limited.legacy_v5_request_hash().is_none());

        let default_json = serde_json::to_string(&mk(1, 0)).unwrap();
        assert!(!default_json.contains("priority"), "{default_json}");
        let prioritized_json = serde_json::to_string(&mk(1, 1)).unwrap();
        assert!(
            prioritized_json.contains("\"priority\":1"),
            "{prioritized_json}"
        );
        let limited_json = serde_json::to_string(&limited).unwrap();
        assert!(
            limited_json.contains("\"timeLimitMs\":5000"),
            "{limited_json}"
        );

        let current = mk(1, 0);
        let legacy_json = concat!(
            r#"{"type":"submit","name":"x","cwd":"/tmp","args":["true"],"env":{},"maxParallelRuns":1,"maxAttempts":1,"retryDelayMs":0,"#,
            r#""afterSuccess":[],"afterCompletion":[]}"#
        );
        let legacy_hash = hex::encode(Sha256::digest(legacy_json.as_bytes()));
        assert_eq!(
            current.legacy_v5_request_hash().as_deref(),
            Some(legacy_hash.as_str())
        );
        assert_ne!(current.request_hash(), legacy_hash);
        assert!(Op::Status.legacy_v5_request_hash().is_none());

        let mut legacy_dependency_params = match mk(1, 0) {
            Op::Submit(params) => params,
            _ => unreachable!(),
        };
        legacy_dependency_params.dependencies = vec![DependencySpec {
            job: 42,
            condition: DependencyCondition::Succeeded,
        }];
        let legacy_dependency_json = concat!(
            r#"{"type":"submit","name":"x","cwd":"/tmp","args":["true"],"env":{},"maxParallelRuns":1,"maxAttempts":1,"retryDelayMs":0,"#,
            r#""afterSuccess":[42],"afterCompletion":[]}"#
        );
        let legacy_dependency_hash = hex::encode(Sha256::digest(legacy_dependency_json.as_bytes()));
        assert_eq!(
            Op::Submit(legacy_dependency_params)
                .legacy_v5_request_hash()
                .as_deref(),
            Some(legacy_dependency_hash.as_str())
        );

        let removed_dependency = concat!(
            r#"{"type":"submit","name":"x","cwd":"/tmp","args":["true"],"env":{},"maxParallelRuns":1,"maxAttempts":1,"retryDelayMs":0,"#,
            r#""afterSuccess":[42]}"#
        );
        assert!(
            serde_json::from_str::<Op>(removed_dependency).is_err(),
            "removed dependency keys must not be silently discarded"
        );
    }

    #[test]
    fn subscribe_is_a_read_op_on_the_current_protocol_version() {
        // The exact bytes a streaming consumer sends; `subscribe` is additive,
        // and uses the current protocol version.
        let wire = r#"{"protocol_version":9,"request_id":"r1","op":{"type":"subscribe"}}"#;
        let request: Request = serde_json::from_str(wire).unwrap();
        assert_eq!(request.protocol_version, PROTOCOL_VERSION);
        assert!(matches!(request.op, Op::Subscribe));
        assert_eq!(request.op.kind(), "subscribe");
        assert!(!request.op.is_mutation(), "a subscription never writes");
        assert!(request.idempotency_key.is_none());
        assert_eq!(
            serde_json::to_string(&request.op).unwrap(),
            r#"{"type":"subscribe"}"#
        );
        // Nothing to canonicalize, but the hash must still be derivable and
        // distinct from the other read ops.
        assert_ne!(Op::Subscribe.request_hash(), Op::Status.request_hash());
        assert!(Op::Subscribe.legacy_v5_request_hash().is_none());
    }

    #[test]
    fn pause_and_unpause_are_idempotent_mutations() {
        assert!(Op::Pause.is_mutation());
        assert!(Op::Unpause.is_mutation());
        assert_eq!(Op::Pause.kind(), "pause");
        assert_eq!(Op::Unpause.kind(), "unpause");
        assert_eq!(
            serde_json::to_string(&Op::Pause).unwrap(),
            r#"{"type":"pause"}"#
        );
        assert_eq!(
            serde_json::to_string(&Op::Unpause).unwrap(),
            r#"{"type":"unpause"}"#
        );
        assert_ne!(Op::Pause.request_hash(), Op::Unpause.request_hash());
        assert!(Op::Pause.legacy_v5_request_hash().is_none());
    }

    #[test]
    fn set_time_limit_is_an_additive_mutation() {
        let set = Op::SetTimeLimit {
            job: 3,
            time_limit_ms: Some(1_500),
        };
        let clear = Op::SetTimeLimit {
            job: 3,
            time_limit_ms: None,
        };
        assert!(set.is_mutation());
        assert_eq!(set.kind(), "set_time_limit");
        assert_ne!(set.request_hash(), clear.request_hash());
        assert_eq!(
            serde_json::to_string(&set).unwrap(),
            r#"{"type":"set_time_limit","job":3,"time_limit_ms":1500}"#
        );
        assert_eq!(
            serde_json::to_string(&clear).unwrap(),
            r#"{"type":"set_time_limit","job":3,"time_limit_ms":null}"#
        );
        let wire = r#"{"protocol_version":9,"request_id":"r1","idempotency_key":"k","op":{"type":"set_time_limit","job":3,"time_limit_ms":1500}}"#;
        let request: Request = serde_json::from_str(wire).unwrap();
        assert!(matches!(
            request.op,
            Op::SetTimeLimit {
                job: 3,
                time_limit_ms: Some(1_500)
            }
        ));
    }

    #[test]
    fn see_is_a_read_op_with_optional_job() {
        let wire = r#"{"protocol_version":9,"request_id":"r1","op":{"type":"see"}}"#;
        let request: Request = serde_json::from_str(wire).unwrap();
        assert_eq!(request.protocol_version, PROTOCOL_VERSION);
        assert!(matches!(request.op, Op::See { job: None }));
        assert_eq!(request.op.kind(), "see");
        assert!(!request.op.is_mutation());
        assert_eq!(
            serde_json::to_string(&Op::See { job: None }).unwrap(),
            r#"{"type":"see"}"#
        );
        assert_eq!(
            serde_json::to_string(&Op::See { job: Some(12) }).unwrap(),
            r#"{"type":"see","job":12}"#
        );
        assert_ne!(
            Op::See { job: None }.request_hash(),
            Op::Show { job: 12 }.request_hash()
        );
        assert!(Op::See { job: None }.legacy_v5_request_hash().is_none());
    }

    #[test]
    fn legacy_dependency_views_in_stored_replies_remain_deserializable() {
        let encoded = r#"{
            "type":"job",
            "job":{
                "id":2,"name":"child","state":"queued","maxParallelRuns":1,
                "priority":0,"cwd":"/tmp","args":["true"],"maxAttempts":1,
                "retryDelayMs":0,"attemptCount":0,"createdAt":1,"updatedAt":1,
                "dependencies":[
                    {"parent":1,"requirement":"success","satisfied":true},
                    {"parent":3,"requirement":"completion","satisfied":false}
                ],
                "attempts":[]
            }
        }"#;
        let Reply::Job { job } = serde_json::from_str::<Reply>(encoded).unwrap() else {
            panic!("expected job reply")
        };
        assert_eq!(job.dependencies.len(), 2);
        assert_eq!(
            job.dependencies[0].condition,
            DependencyCondition::Succeeded
        );
        assert_eq!(job.dependencies[1].condition, DependencyCondition::Terminal);
        assert!(
            job.dependencies
                .iter()
                .all(|dependency| dependency.parent_state.is_empty())
        );
    }

    #[test]
    fn sync_framing_round_trips_and_enforces_limit() {
        let mut buf = Vec::new();
        write_frame_sync(&mut buf, b"hello", 1024).unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        assert_eq!(
            read_frame_sync(&mut cursor, 1024).unwrap().unwrap(),
            b"hello"
        );
        assert!(read_frame_sync(&mut cursor, 1024).unwrap().is_none());

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&100u32.to_be_bytes());
        oversized.extend_from_slice(&[0u8; 100]);
        let mut cursor = std::io::Cursor::new(oversized);
        assert!(read_frame_sync(&mut cursor, 10).is_err());
    }

    #[test]
    fn stable_json_uses_camel_case_max_parallel_runs() {
        let view = AttemptView {
            id: 1,
            job_id: 2,
            number: 1,
            state: "running".into(),
            max_parallel_runs: 3,
            window_end_at: None,
            exit_code: None,
            term_signal: None,
            message: None,
            created_at: 0,
            finished_at: None,
            log_dir: "/x".into(),
        };
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"maxParallelRuns\":3"), "{json}");
    }
}
