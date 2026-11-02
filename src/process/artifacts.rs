//! Durable attempt-directory artifacts shared by the daemon and the runner.
//!
//! Every artifact is written atomically: exclusive 0600 temp file, content
//! fsync, rename into place, directory fsync. Reads distinguish "absent" from
//! "corrupt" so recovery can quarantine rather than guess.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::domain::{AttemptId, JobId};

pub const COMMAND_FILE: &str = "command.json";
pub const IDENTITY_FILE: &str = "identity.json";
pub const START_FILE: &str = "start";
pub const EXEC_FILE: &str = "exec.json";
pub const CANCEL_FILE: &str = "cancel.json";
/// Live override of the launch-time wall-clock limit. Absent means the
/// runner keeps the limit snapshotted in `command.json`.
pub const LIMIT_FILE: &str = "limit.json";
/// Written once, when a supervisor commits to timeout termination.
pub const TIMEOUT_FILE: &str = "timeout.json";
pub const RESULT_FILE: &str = "result.json";
pub const RUNNER_LOCK_FILE: &str = "runner.lock";
pub const RUNNER_LOG_FILE: &str = "runner.log";
pub const STDOUT_LOG_FILE: &str = "stdout.log";
pub const STDERR_LOG_FILE: &str = "stderr.log";

/// Artifacts are small control records; anything larger is corrupt.
pub const MAX_ARTIFACT_BYTES: u64 = 1 << 20;

/// Written by the daemon before spawning the runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandFile {
    pub attempt_id: AttemptId,
    pub job_id: JobId,
    pub token: String,
    pub argv: Vec<String>,
    pub cwd: String,
    pub env: BTreeMap<String, String>,
    pub start_wait_ms: u64,
    pub cancel_grace_ms: u64,
    pub poll_ms: u64,
    /// Launch snapshot. A later `limit.json` overrides this for a live runner;
    /// absent on artifacts created before wall-clock limits were supported.
    #[serde(default)]
    pub time_limit_ms: Option<u64>,
    /// Window deadline fixed at admission.
    #[serde(default)]
    pub window_end_at: Option<i64>,
}

/// Upgradable wall-clock limit for an attempt that is already supervised.
/// `time_limit_ms: null` clears the bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitFile {
    pub token: String,
    pub time_limit_ms: Option<u64>,
    /// When this override was committed. Absent means the file predates the field.
    #[serde(default)]
    pub updated_at: Option<i64>,
}

/// Durable commitment that timeout termination has started. The daemon
/// continues it if the runner dies, even if the job's limit is edited later.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutFile {
    pub token: String,
    /// Wall-clock deadline (`command_started_at + limit`) that fired.
    pub deadline: i64,
    /// When SIGTERM was armed. SIGKILL waits one grace period after this.
    pub observed_at: i64,
}

/// Written by the runner immediately after starting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityFile {
    pub token: String,
    pub runner_pid: i32,
    pub runner_start_time: i64,
    pub boot_id: String,
}

/// Launch authorization, published by the daemon only after the `authorized`
/// state is committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartFile {
    pub token: String,
}

/// Durable command identity, published by the runner after a successful exec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecFile {
    pub token: String,
    pub pid: i32,
    pub pgid: i32,
    pub start_time: i64,
    pub boot_id: String,
    /// Wall-clock spawn timestamp for daemon-side orphan deadline recovery.
    #[serde(default)]
    pub started_at: Option<i64>,
}

/// Durable cancellation intent, published by the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelFile {
    pub token: String,
    pub force: bool,
    /// Commit timestamp used to order cancellation against a time limit.
    #[serde(default)]
    pub requested_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultOutcome {
    /// The command ran and exited on its own; see exit_code/term_signal.
    Exited,
    /// Setup, chdir, or execve failed; the command never ran.
    LaunchFailed,
    /// A delivered cancellation signal terminated the run.
    Cancelled,
    /// The per-attempt wall-clock limit expired.
    TimedOut,
    /// The job or class daily execution window closed.
    WindowClosed,
    /// The runner gave up waiting for launch authorization.
    AuthorizationTimeout,
}

/// Terminal record: published only after the command process group is proven
/// empty (or when no command ever ran).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultFile {
    pub token: String,
    pub outcome: ResultOutcome,
    pub exit_code: Option<i32>,
    pub term_signal: Option<i32>,
    pub message: Option<String>,
    pub finished_at: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact io: {0}")]
    Io(#[from] io::Error),
    #[error("artifact corrupt: {0}")]
    Corrupt(String),
}

/// Absent / present-and-valid / present-but-corrupt.
pub fn read_artifact<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, ArtifactError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let len = file.metadata()?.len();
    if len > MAX_ARTIFACT_BYTES {
        return Err(ArtifactError::Corrupt(format!(
            "{} is {len} bytes (limit {MAX_ARTIFACT_BYTES})",
            path.display()
        )));
    }
    let mut buf = Vec::with_capacity(len as usize);
    file.read_to_end(&mut buf)?;
    serde_json::from_slice(&buf)
        .map(Some)
        .map_err(|err| ArtifactError::Corrupt(format!("{}: {err}", path.display())))
}

/// Atomically publish an artifact. `exclusive` refuses to replace an existing
/// file (used for one-shot markers like `start` and `result.json`);
/// non-exclusive replacement is used for upgradable intent like `cancel.json`
/// and `limit.json`.
pub fn write_artifact<T: Serialize>(
    dir: &Path,
    name: &str,
    value: &T,
    exclusive: bool,
) -> io::Result<()> {
    let final_path = dir.join(name);
    if exclusive && final_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} already exists", final_path.display()),
        ));
    }
    let tmp_path = dir.join(format!(".tmp.{name}"));
    // A leftover temp file from a crashed writer is stale by construction.
    let _ = fs::remove_file(&tmp_path);
    let mut tmp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp_path)?;
    tmp.write_all(&serde_json::to_vec_pretty(value)?)?;
    tmp.sync_all()?;
    drop(tmp);
    if exclusive {
        publish_noreplace(&tmp_path, &final_path)?;
    } else {
        fs::rename(&tmp_path, &final_path)?;
    }
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Atomic publish that refuses to replace an existing file even against a
/// concurrent writer (`rename` silently clobbers; `renameat2` with
/// `RENAME_NOREPLACE` does not). Writers are serialized today (coordinator
/// thread, per-attempt flock), so this is defense in depth.
fn publish_noreplace(tmp_path: &Path, final_path: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let cstr = |path: &Path| {
        std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
    };
    let tmp_c = cstr(tmp_path)?;
    let final_c = cstr(final_path)?;
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            tmp_c.as_ptr(),
            libc::AT_FDCWD,
            final_c.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EEXIST) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} already exists", final_path.display()),
        )),
        // Filesystems without RENAME_NOREPLACE support: fall back to plain
        // rename; the pre-write existence check retains today's guarantee.
        Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::ENOTSUP) => {
            fs::rename(tmp_path, final_path)
        }
        _ => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_round_trip_and_exclusivity() {
        let dir = tempfile::tempdir().unwrap();
        let start = StartFile { token: "t".into() };
        write_artifact(dir.path(), START_FILE, &start, true).unwrap();
        let read: StartFile = read_artifact(&dir.path().join(START_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(read.token, "t");
        // One-shot markers cannot be replaced.
        assert!(write_artifact(dir.path(), START_FILE, &start, true).is_err());
        // Upgradable intent can.
        let cancel = CancelFile {
            token: "t".into(),
            force: false,
            requested_at: Some(1),
        };
        write_artifact(dir.path(), CANCEL_FILE, &cancel, false).unwrap();
        let cancel = CancelFile {
            token: "t".into(),
            force: true,
            requested_at: Some(2),
        };
        write_artifact(dir.path(), CANCEL_FILE, &cancel, false).unwrap();
        let read: CancelFile = read_artifact(&dir.path().join(CANCEL_FILE))
            .unwrap()
            .unwrap();
        assert!(read.force);
    }

    #[test]
    fn absent_and_corrupt_are_distinguished() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(RESULT_FILE);
        let absent: Result<Option<ResultFile>, _> = read_artifact(&path);
        assert!(absent.unwrap().is_none());

        fs::write(&path, b"{ not json").unwrap();
        let corrupt: Result<Option<ResultFile>, _> = read_artifact(&path);
        assert!(matches!(corrupt, Err(ArtifactError::Corrupt(_))));
    }
}
