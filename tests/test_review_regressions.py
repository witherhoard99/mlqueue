"""Run the repository's default-lookup and history SQL in isolated SQLite databases."""

import json
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / 'src/db.rs').read_text()


def sql_for(function):
    body = SOURCE.split(f'pub fn {function}(', 1)[1].split('\npub fn ', 1)[0]
    sql = re.search(r'"(SELECT .*?)"', body, re.S).group(1)
    return sql.replace('{JOB_COLUMNS}', '*').replace('{ATTEMPT_COLUMNS}', '*')


class ReviewRegressions(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        migrations = SOURCE.split('const MIGRATIONS: &[&str] = &[', 1)[1].split('\n];', 1)[0]
        for sql in re.findall(r'r#"(.*?)"#', migrations, re.S):
            self.db.executescript(sql)

    def job(self, command, state='queued', default=False, finished=None):
        return self.db.execute(
            "INSERT INTO jobs(name,state,cwd,args,env,max_parallel_runs,max_attempts,"
            "retry_delay_ms,created_at,updated_at,is_default,finished_at) "
            "VALUES('test',?,'unused',?,'{}',1,1,0,0,0,?,?)",
            (state, json.dumps([command], separators=(',', ':')), default, finished),
        ).lastrowid

    def lookup(self, command):
        return self.db.execute(sql_for('default_for_command'), (json.dumps([command]),)).fetchone()

    def test_cancelled_or_failed_default_survives_recent_history_limit(self):
        for state in ('cancelled', 'failed'):
            with self.subTest(state=state):
                command = f'/models/{state}.sh'
                job = self.job(command, state, default=True, finished=1)
                for n in range(30):
                    self.job('/ordinary', 'succeeded', finished=n + 2)
                recent = self.db.execute(sql_for('status_jobs'), (25,)).fetchall()
                self.assertNotIn(job, [row['id'] for row in recent])
                found = self.lookup(command)
                self.assertEqual((found['id'], found['state']), (job, state))

    def test_lookup_prefers_active_default_and_latest_matching_history(self):
        self.assertIsNone(self.lookup('/models/ninfer.sh'))
        self.job('/models/ninfer.sh', 'cancelled', default=True, finished=1)
        latest = self.job('/models/ninfer.sh', 'failed', default=True, finished=2)
        self.assertEqual(self.lookup('/models/ninfer.sh')['id'], latest)
        active = self.job('/different-command', default=True)
        self.assertEqual(self.lookup('/models/ninfer.sh')['id'], active)

    def test_pages_six_thousand_attempts_while_new_attempts_arrive(self):
        job = self.job('/models/ninfer.sh', default=True)
        self.db.executemany(
            "INSERT INTO attempts(job_id,number,state,launch_token,max_parallel_runs,created_at) "
            "VALUES(?,?,'succeeded',?,1,0)",
            ((job, n, f'attempt-{n}') for n in range(1, 6001)),
        )
        sql = sql_for('attempts_page')
        recent = self.db.execute(sql, (job, None, 25)).fetchall()
        self.assertEqual([row['number'] for row in recent], list(range(6000, 5975, -1)))
        cursor, seen = None, []
        while True:
            page = self.db.execute(sql, (job, cursor, 100)).fetchall()
            if not page:
                break
            seen.extend(row['number'] for row in page)
            cursor = page[-1]['number']
            if cursor == 5901:
                self.db.execute(
                    "INSERT INTO attempts(job_id,number,state,launch_token,max_parallel_runs,created_at) "
                    "VALUES(?,6001,'running','new-attempt',1,0)", (job,),
                )
        self.assertEqual(seen, list(range(6000, 0, -1)))

    def test_actual_restart_policy_with_simulated_boots_and_operator_actions(self):
        source = (ROOT / 'src/daemon/coordinator.rs').read_text()
        policy = 'fn should_restart_default(' + source.split(
            'fn should_restart_default(', 1,
        )[1].split('\nfn requeue_default(', 1)[0]
        views = (ROOT / 'src/daemon/views.rs').read_text()
        compact = 'fn compact_mutation_history(' + views.split(
            'pub fn compact_mutation_history(', 1,
        )[1].split('\nfn status_job_order(', 1)[0]
        harness = r'''
#[derive(Clone, Copy, PartialEq)]
enum JobState { Queued, Held, Cancelled, Failed }
impl JobState {
    fn is_terminal(self) -> bool { matches!(self, Self::Cancelled | Self::Failed) }
}
#[derive(Clone, Copy, PartialEq)]
enum AttemptState { Succeeded, Lost, Failed, Cancelled }
struct JobRow { is_default: bool, state: JobState, max_attempts: i64 }
struct AttemptRow { yield_requested: bool, cancel_requested: bool, runner_boot_id: Option<String> }
struct JobView { attempts: Vec<i64>, attempts_truncated: bool }
enum Reply { Job { job: JobView }, Submitted { job: JobView }, Other }
#[test]
fn old_cached_mutation_replies_discard_history() {
    for mut reply in [
        Reply::Job { job: JobView { attempts: vec![1; 6000], attempts_truncated: true } },
        Reply::Submitted { job: JobView { attempts: vec![1; 6000], attempts_truncated: false } },
    ] {
        assert!(compact_mutation_history(&mut reply));
        let (Reply::Job { job } | Reply::Submitted { job }) = &reply else { panic!() };
        assert!(job.attempts.is_empty());
        assert!(!job.attempts_truncated);
        assert!(!compact_mutation_history(&mut reply));
    }
    assert!(!compact_mutation_history(&mut Reply::Other));
}
#[test]
fn reboot_restart_respects_cancellation_holds_and_failure_budget() {
    let mut job = JobRow { is_default: true, state: JobState::Queued, max_attempts: 1 };
    let mut attempt = AttemptRow {
        yield_requested: false, cancel_requested: false, runner_boot_id: Some("old".into()),
    };
    assert!(should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "old", 0));
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 1));
    assert!(!should_restart_default(&job, &attempt, AttemptState::Failed, "new", 1));
    attempt.cancel_requested = true;
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    assert!(!should_restart_default(&job, &attempt, AttemptState::Succeeded, "new", 0));
    attempt.yield_requested = true;
    assert!(should_restart_default(&job, &attempt, AttemptState::Cancelled, "new", 0));
    job.state = JobState::Held;
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    for state in [JobState::Cancelled, JobState::Failed] {
        job.state = state;
        assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    }
    job.state = JobState::Queued;
    job.is_default = false;
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    job.is_default = true;
    attempt.yield_requested = false;
    attempt.cancel_requested = false;
    attempt.runner_boot_id = None;
    assert!(!should_restart_default(&job, &attempt, AttemptState::Lost, "new", 0));
    assert!(should_restart_default(&job, &attempt, AttemptState::Succeeded, "new", 0));
}
'''
        with tempfile.TemporaryDirectory(dir=ROOT / 'target') as directory:
            path = Path(directory)
            (path / 'policy.rs').write_text(policy + compact + harness)
            for command in (
                ['rustc', '--edition', '2024', '--test', str(path / 'policy.rs'), '-o', str(path / 'policy')],
                [str(path / 'policy')],
            ):
                result = subprocess.run(command, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
