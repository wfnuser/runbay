use crate::models::{AgentRecord, CapabilityTier, RunEvent, RunRecord, RunState};
use anyhow::{Context, Result};
use chrono::Utc;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

pub const MAX_STORED_EVENTS_PER_RUN: u64 = 512;

pub struct DaemonStore {
    conn: Mutex<Connection>,
}

impl DaemonStore {
    pub fn open(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)
            .with_context(|| format!("failed to open runbayd db at {}", db_path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;

             CREATE TABLE IF NOT EXISTS agents (
                 agent_id TEXT PRIMARY KEY,
                 runtime TEXT NOT NULL,
                 workspace_dir TEXT NOT NULL,
                 home_dir TEXT NOT NULL,
                 mvcc_dir TEXT NOT NULL,
                 config_json TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );

             CREATE TABLE IF NOT EXISTS runs (
                 run_id TEXT PRIMARY KEY,
                 agent_id TEXT NOT NULL,
                 runtime TEXT NOT NULL,
                 prompt TEXT NOT NULL,
                 state TEXT NOT NULL,
                 priority TEXT NOT NULL,
                 capability_tier TEXT NOT NULL,
                 workspace_rev INTEGER NOT NULL,
                 branch_workspace TEXT,
                 exit_code INTEGER,
                 error_code TEXT,
                 error_message TEXT,
                 sandbox_enforced INTEGER NOT NULL,
                 network_isolated INTEGER NOT NULL,
                 secretless_gateway INTEGER NOT NULL,
                 gateway_transport TEXT,
                 events_count INTEGER NOT NULL DEFAULT 0,
                 events_dropped INTEGER NOT NULL DEFAULT 0,
                 late_events_rejected INTEGER NOT NULL DEFAULT 0,
                 created_at TEXT NOT NULL,
                 started_at TEXT,
                 finished_at TEXT
             );

             CREATE INDEX IF NOT EXISTS idx_runs_agent_created
                 ON runs(agent_id, created_at DESC);

             CREATE TABLE IF NOT EXISTS run_events (
                 run_id TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 agent_id TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 timestamp TEXT NOT NULL,
                 data_json TEXT NOT NULL,
                 PRIMARY KEY (run_id, seq)
             );

             CREATE INDEX IF NOT EXISTS idx_run_events_agent
                 ON run_events(agent_id, timestamp, seq);",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Explicit restart recovery: processes do not survive daemon restarts, so any
    /// `queued` or `running` rows from a prior daemon process are transitioned cleanly
    /// to `failed` with `error_code = "daemon_restarted"`.
    pub fn recover_interrupted_runs(
        &self,
        cleanup: impl Fn(&str) -> Result<()>,
    ) -> Result<Vec<String>> {
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock();
        let mut stmt =
            conn.prepare("SELECT run_id, agent_id FROM runs WHERE state IN ('queued', 'running')")?;
        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut recovered = Vec::with_capacity(rows.len());
        for (run_id, agent_id) in rows {
            cleanup(&run_id)?;
            conn.execute(
                "UPDATE runs
                 SET state = 'failed',
                     error_code = 'daemon_restarted',
                     error_message = 'Daemon restarted before run completed; child processes do not survive daemon restart',
                     finished_at = COALESCE(finished_at, ?1)
                 WHERE run_id = ?2",
                params![now, run_id],
            )?;
            let next_seq: i64 = conn.query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM run_events WHERE run_id = ?1",
                params![run_id],
                |r| r.get(0),
            )?;
            let data = serde_json::json!({
                "error_code": "daemon_restarted",
                "error_message": "Daemon restarted before run completed; child processes do not survive daemon restart"
            });
            conn.execute(
                "INSERT INTO run_events (run_id, seq, agent_id, event_type, timestamp, data_json)
                 VALUES (?1, ?2, ?3, 'run.failed', ?4, ?5)",
                params![run_id, next_seq, agent_id, now, data.to_string()],
            )?;
            conn.execute(
                "UPDATE runs SET events_count = events_count + 1 WHERE run_id = ?1",
                params![run_id],
            )?;
            recovered.push(run_id);
        }
        Ok(recovered)
    }

    pub fn insert_agent(&self, agent: &AgentRecord) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO agents (agent_id, runtime, workspace_dir, home_dir, mvcc_dir, config_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                agent.agent_id,
                agent.runtime,
                agent.workspace_dir,
                agent.home_dir,
                agent.mvcc_dir,
                agent.config.to_string(),
                agent.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_agent(&self, agent_id: &str) -> Result<Option<AgentRecord>> {
        let conn = self.conn.lock();
        let row = conn
            .query_row(
                "SELECT agent_id, runtime, workspace_dir, home_dir, mvcc_dir, config_json, created_at
                 FROM agents WHERE agent_id = ?1",
                params![agent_id],
                |row| {
                    let cfg_raw: String = row.get(5)?;
                    let config = serde_json::from_str(&cfg_raw).unwrap_or(serde_json::Value::Null);
                    Ok(AgentRecord {
                        agent_id: row.get(0)?,
                        runtime: row.get(1)?,
                        workspace_dir: row.get(2)?,
                        home_dir: row.get(3)?,
                        mvcc_dir: row.get(4)?,
                        config,
                        created_at: row.get(6)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn list_agents(&self) -> Result<Vec<AgentRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT agent_id, runtime, workspace_dir, home_dir, mvcc_dir, config_json, created_at
             FROM agents ORDER BY created_at ASC, agent_id ASC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let cfg_raw: String = row.get(5)?;
                let config = serde_json::from_str(&cfg_raw).unwrap_or(serde_json::Value::Null);
                Ok(AgentRecord {
                    agent_id: row.get(0)?,
                    runtime: row.get(1)?,
                    workspace_dir: row.get(2)?,
                    home_dir: row.get(3)?,
                    mvcc_dir: row.get(4)?,
                    config,
                    created_at: row.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let changed = conn.execute("DELETE FROM agents WHERE agent_id = ?1", params![agent_id])?;
        Ok(changed > 0)
    }

    pub fn insert_run(&self, run: &RunRecord) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO runs (
                run_id, agent_id, runtime, prompt, state, priority, capability_tier,
                workspace_rev, branch_workspace, exit_code, error_code, error_message,
                sandbox_enforced, network_isolated, secretless_gateway, gateway_transport,
                events_count, events_dropped, late_events_rejected,
                created_at, started_at, finished_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15, ?16,
                ?17, ?18, ?19,
                ?20, ?21, ?22
             )",
            params![
                run.run_id,
                run.agent_id,
                run.runtime,
                run.prompt,
                run.state.as_str(),
                run.priority,
                "one_shot",
                run.workspace_rev as i64,
                run.branch_workspace,
                run.exit_code,
                run.error_code,
                run.error_message,
                i64::from(run.sandbox_enforced),
                i64::from(run.network_isolated),
                i64::from(run.secretless_gateway),
                run.gateway_transport,
                run.events_count as i64,
                run.events_dropped as i64,
                run.late_events_rejected as i64,
                run.created_at,
                run.started_at,
                run.finished_at,
            ],
        )?;
        Ok(())
    }

    pub fn update_run(&self, run: &RunRecord) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE runs
             SET state = ?1,
                 workspace_rev = ?2,
                 branch_workspace = ?3,
                 exit_code = ?4,
                 error_code = ?5,
                 error_message = ?6,
                 sandbox_enforced = ?7,
                 network_isolated = ?8,
                 secretless_gateway = ?9,
                 gateway_transport = ?10,
                 late_events_rejected = ?11,
                 started_at = ?12,
                 finished_at = ?13
             WHERE run_id = ?14",
            params![
                run.state.as_str(),
                run.workspace_rev as i64,
                run.branch_workspace,
                run.exit_code,
                run.error_code,
                run.error_message,
                i64::from(run.sandbox_enforced),
                i64::from(run.network_isolated),
                i64::from(run.secretless_gateway),
                run.gateway_transport,
                run.late_events_rejected as i64,
                run.started_at,
                run.finished_at,
                run.run_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let conn = self.conn.lock();
        let row = conn
            .query_row(
                "SELECT run_id, agent_id, runtime, prompt, state, priority,
                        workspace_rev, branch_workspace, exit_code, error_code, error_message,
                        sandbox_enforced, network_isolated, secretless_gateway, gateway_transport,
                        events_count, events_dropped, late_events_rejected,
                        created_at, started_at, finished_at
                 FROM runs WHERE run_id = ?1",
                params![run_id],
                row_to_run_record,
            )
            .optional()?;
        Ok(row)
    }

    pub fn list_runs(&self, agent_filter: Option<&str>) -> Result<Vec<RunRecord>> {
        let conn = self.conn.lock();
        if let Some(aid) = agent_filter {
            let mut stmt = conn.prepare(
                "SELECT run_id, agent_id, runtime, prompt, state, priority,
                        workspace_rev, branch_workspace, exit_code, error_code, error_message,
                        sandbox_enforced, network_isolated, secretless_gateway, gateway_transport,
                        events_count, events_dropped, late_events_rejected,
                        created_at, started_at, finished_at
                 FROM runs WHERE agent_id = ?1 ORDER BY created_at DESC LIMIT 100",
            )?;
            let rows = stmt
                .query_map(params![aid], row_to_run_record)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        } else {
            let mut stmt = conn.prepare(
                "SELECT run_id, agent_id, runtime, prompt, state, priority,
                        workspace_rev, branch_workspace, exit_code, error_code, error_message,
                        sandbox_enforced, network_isolated, secretless_gateway, gateway_transport,
                        events_count, events_dropped, late_events_rejected,
                        created_at, started_at, finished_at
                 FROM runs ORDER BY created_at DESC LIMIT 100",
            )?;
            let rows = stmt
                .query_map([], row_to_run_record)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        }
    }

    /// Persist a run event with bounded storage (`MAX_STORED_EVENTS_PER_RUN`).
    /// Lifecycle events (`run.*` excluding `run.output`/`run.stderr`) are always stored
    /// so SSE reconnects always observe start and terminal transitions.
    pub fn append_event(
        &self,
        run_id: &str,
        agent_id: &str,
        event_type: &str,
        data: serde_json::Value,
    ) -> Result<Option<RunEvent>> {
        let is_lifecycle = matches!(
            event_type,
            "run.queued" | "run.started" | "run.succeeded" | "run.failed" | "run.cancelled"
        );
        let now = Utc::now().to_rfc3339();
        let conn = self.conn.lock();

        let (stored_count, max_seq): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), COALESCE(MAX(seq), 0) FROM run_events WHERE run_id = ?1",
            params![run_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;

        if !is_lifecycle && (stored_count as u64) >= (MAX_STORED_EVENTS_PER_RUN - 4) {
            conn.execute(
                "UPDATE runs SET events_dropped = events_dropped + 1 WHERE run_id = ?1",
                params![run_id],
            )?;
            return Ok(None);
        }

        let seq = (max_seq + 1) as u64;
        conn.execute(
            "INSERT INTO run_events (run_id, seq, agent_id, event_type, timestamp, data_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                run_id,
                seq as i64,
                agent_id,
                event_type,
                now,
                data.to_string()
            ],
        )?;
        conn.execute(
            "UPDATE runs SET events_count = events_count + 1 WHERE run_id = ?1",
            params![run_id],
        )?;

        Ok(Some(RunEvent {
            seq,
            run_id: run_id.to_string(),
            agent_id: agent_id.to_string(),
            event_type: event_type.to_string(),
            timestamp: now,
            data,
        }))
    }

    pub fn list_events_after(&self, run_id: &str, after_seq: u64) -> Result<Vec<RunEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT seq, run_id, agent_id, event_type, timestamp, data_json
             FROM run_events
             WHERE run_id = ?1 AND seq > ?2
             ORDER BY seq ASC",
        )?;
        let rows = stmt
            .query_map(params![run_id, after_seq as i64], |row| {
                let raw: String = row.get(5)?;
                let data = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                Ok(RunEvent {
                    seq: row.get::<_, i64>(0)? as u64,
                    run_id: row.get(1)?,
                    agent_id: row.get(2)?,
                    event_type: row.get(3)?,
                    timestamp: row.get(4)?,
                    data,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn list_agent_events(&self, agent_id: &str, limit: usize) -> Result<Vec<RunEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT seq, run_id, agent_id, event_type, timestamp, data_json
             FROM run_events
             WHERE agent_id = ?1
             ORDER BY timestamp ASC, seq ASC
             LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(params![agent_id, limit as i64], |row| {
                let raw: String = row.get(5)?;
                let data = serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null);
                Ok(RunEvent {
                    seq: row.get::<_, i64>(0)? as u64,
                    run_id: row.get(1)?,
                    agent_id: row.get(2)?,
                    event_type: row.get(3)?,
                    timestamp: row.get(4)?,
                    data,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

fn row_to_run_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunRecord> {
    let state_raw: String = row.get(4)?;
    Ok(RunRecord {
        run_id: row.get(0)?,
        agent_id: row.get(1)?,
        runtime: row.get(2)?,
        prompt: row.get(3)?,
        state: RunState::from_db(&state_raw),
        priority: row.get(5)?,
        capability_tier: CapabilityTier::OneShot,
        workspace_rev: row.get::<_, i64>(6)? as u64,
        branch_workspace: row.get(7)?,
        exit_code: row.get(8)?,
        error_code: row.get(9)?,
        error_message: row.get(10)?,
        sandbox_enforced: row.get::<_, i64>(11)? != 0,
        network_isolated: row.get::<_, i64>(12)? != 0,
        secretless_gateway: row.get::<_, i64>(13)? != 0,
        gateway_transport: row.get(14)?,
        events_count: row.get::<_, i64>(15)? as u64,
        events_dropped: row.get::<_, i64>(16)? as u64,
        late_events_rejected: row.get::<_, i64>(17)? as u64,
        created_at: row.get(18)?,
        started_at: row.get(19)?,
        finished_at: row.get(20)?,
    })
}
