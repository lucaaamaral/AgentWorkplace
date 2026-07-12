//! SQLite store: the append-only log, channels, principals, subscriptions,
//! subscription overrides, and acknowledgment state (ADR-0005, ADR-0017).
//!
//! Single writer: only the broker process opens the database. Message and
//! system records are never updated or deleted (sole exception: channel
//! deletion, ADR-0018, which leaves a tombstone). Ack state lives in
//! separate mutable rows.
//!
//! Every fallible operation returns a `Result`: storage failures (disk full,
//! corruption, malformed persisted JSON) surface as RPC errors upstream —
//! they must never kill the daemon. State changes and the system records
//! that audit them are written in ONE transaction, so a mutation can never
//! outlive its log entry (and vice versa); policy checks (overrides) run in
//! the same transaction as the mutation they gate.

use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{AckState, AckStatus, Envelope, Recipients, Record, SystemEvent};
use rusqlite::{Connection, OptionalExtension, params};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("name taken")]
    NameTaken,
    #[error("database is from a newer workplace version (schema {found} > {supported})")]
    NewerSchema { found: i64, supported: i64 },
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error("corrupt stored json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type SResult<T> = Result<T, StoreError>;

pub struct ChannelRow {
    pub id: i64,
    pub name: String,
    pub archived: bool,
}

/// Human-set subscription precedence (ADR-0009): forced (agent cannot drop)
/// or cancelled (agent cannot rejoin).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverrideMode {
    Forced,
    Cancelled,
}

impl OverrideMode {
    fn as_str(self) -> &'static str {
        match self {
            OverrideMode::Forced => "forced",
            OverrideMode::Cancelled => "cancelled",
        }
    }
}

/// Outcome of a self-service subscription change.
pub enum SubOutcome {
    /// Membership changed; the system record (already persisted) to stream.
    Applied(Record),
    /// Already in the requested state — idempotent no-op.
    NoChange,
    /// Blocked by human-set precedence (ADR-0009).
    Denied(OverrideMode),
}

pub struct Store {
    conn: Mutex<Connection>,
}

const SCHEMA_VERSION: i64 = 2;

impl Store {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Store {
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Store {
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        // A poisoned lock means another thread panicked mid-query; the
        // connection itself is still usable (rusqlite statements are
        // transactional), so recover rather than cascade the panic.
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn migrate(&self) -> anyhow::Result<()> {
        let conn = self.conn();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )?;
        let found: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(found) = found {
            let found: i64 = found.parse().unwrap_or(0);
            if found > SCHEMA_VERSION {
                return Err(StoreError::NewerSchema {
                    found,
                    supported: SCHEMA_VERSION,
                }
                .into());
            }
        }
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS channels (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL UNIQUE,
                 archived INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS principals (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS subscriptions (
                 principal_id INTEGER NOT NULL REFERENCES principals(id),
                 channel_id INTEGER NOT NULL REFERENCES channels(id),
                 PRIMARY KEY (principal_id, channel_id)
             );
             CREATE TABLE IF NOT EXISTS subscription_overrides (
                 principal_id INTEGER NOT NULL REFERENCES principals(id),
                 channel_id INTEGER NOT NULL REFERENCES channels(id),
                 mode TEXT NOT NULL,
                 PRIMARY KEY (principal_id, channel_id)
             );
             CREATE TABLE IF NOT EXISTS records (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 kind TEXT NOT NULL,
                 timestamp INTEGER NOT NULL,
                 thread_id INTEGER,
                 sender TEXT,
                 body TEXT,
                 truncated INTEGER,
                 recipients_json TEXT,
                 event_json TEXT,
                 channel_ref INTEGER
             );
             CREATE TABLE IF NOT EXISTS record_channels (
                 record_id INTEGER NOT NULL REFERENCES records(id),
                 channel_id INTEGER NOT NULL REFERENCES channels(id),
                 PRIMARY KEY (record_id, channel_id)
             );
             CREATE TABLE IF NOT EXISTS record_participants (
                 record_id INTEGER NOT NULL REFERENCES records(id),
                 principal TEXT NOT NULL,
                 PRIMARY KEY (record_id, principal)
             );
             CREATE TABLE IF NOT EXISTS acks (
                 record_id INTEGER NOT NULL REFERENCES records(id),
                 recipient TEXT NOT NULL,
                 state TEXT NOT NULL,
                 reason TEXT,
                 held_at INTEGER,
                 relayed_at INTEGER,
                 processed_at INTEGER,
                 failed_at INTEGER,
                 PRIMARY KEY (record_id, recipient)
             );
             CREATE INDEX IF NOT EXISTS idx_acks_state ON acks(state);
             CREATE INDEX IF NOT EXISTS idx_record_channels_chan ON record_channels(channel_id, record_id);",
        )?;
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = ?1",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    // -- channels ----------------------------------------------------------

    /// Create a channel and its audit record in one transaction.
    pub fn create_channel_logged(&self, name: &str, by: &str) -> SResult<(i64, Record)> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        match tx.execute("INSERT INTO channels(name) VALUES (?1)", params![name]) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                return Err(StoreError::NameTaken);
            }
            Err(e) => return Err(e.into()),
        }
        let id = tx.last_insert_rowid();
        let event = SystemEvent::ChannelCreated {
            channel: name.to_string(),
            by: by.to_string(),
        };
        let record = append_system_in(&tx, &event, Some(id))?;
        tx.commit()?;
        Ok((id, record))
    }

    pub fn channel_by_name(&self, name: &str) -> SResult<Option<ChannelRow>> {
        let conn = self.conn();
        Ok(conn
            .query_row(
                "SELECT id, name, archived FROM channels WHERE name = ?1",
                params![name],
                |r| {
                    Ok(ChannelRow {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        archived: r.get::<_, i64>(2)? != 0,
                    })
                },
            )
            .optional()?)
    }

    /// Rename a channel and its audit record in one transaction.
    pub fn rename_channel_logged(
        &self,
        id: i64,
        old_name: &str,
        new_name: &str,
        by: &str,
    ) -> SResult<Record> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        match tx.execute(
            "UPDATE channels SET name = ?1 WHERE id = ?2",
            params![new_name, id],
        ) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                return Err(StoreError::NameTaken);
            }
            Err(e) => return Err(e.into()),
        }
        let event = SystemEvent::ChannelRenamed {
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
            by: by.to_string(),
        };
        let record = append_system_in(&tx, &event, Some(id))?;
        tx.commit()?;
        Ok(record)
    }

    /// Archive (or unarchive) a channel in one transaction: flips the flag,
    /// force-cancels every subscription and override on archive, and appends
    /// every audit record. Returns the records to stream, in order.
    pub fn archive_channel(
        &self,
        id: i64,
        name: &str,
        by: &str,
        archive: bool,
    ) -> SResult<Vec<Record>> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE channels SET archived = ?1 WHERE id = ?2",
            params![archive as i64, id],
        )?;
        let mut records = Vec::new();
        if archive {
            let names: Vec<String> = {
                let mut stmt = tx.prepare(
                    "SELECT p.name FROM subscriptions s JOIN principals p ON p.id = s.principal_id
                     WHERE s.channel_id = ?1 ORDER BY p.name",
                )?;
                stmt.query_map(params![id], |r| r.get(0))?
                    .collect::<Result<_, _>>()?
            };
            tx.execute(
                "DELETE FROM subscriptions WHERE channel_id = ?1",
                params![id],
            )?;
            tx.execute(
                "DELETE FROM subscription_overrides WHERE channel_id = ?1",
                params![id],
            )?;
            for principal in names {
                let event = SystemEvent::Unsubscribed {
                    principal,
                    channel: name.to_string(),
                    by_admin: true,
                };
                records.push(append_system_in(&tx, &event, Some(id))?);
            }
            let event = SystemEvent::ChannelArchived {
                channel: name.to_string(),
                by: by.to_string(),
            };
            records.push(append_system_in(&tx, &event, Some(id))?);
        } else {
            let event = SystemEvent::ChannelUnarchived {
                channel: name.to_string(),
                by: by.to_string(),
            };
            records.push(append_system_in(&tx, &event, Some(id))?);
        }
        tx.commit()?;
        Ok(records)
    }

    /// Permanent deletion (ADR-0018) plus its tombstone in one transaction:
    /// removes records that belong solely to this channel, detaches shared
    /// ones, drops subscriptions/overrides and the channel row. Returns the
    /// number of records fully removed and the tombstone record.
    pub fn delete_channel_logged(
        &self,
        channel_id: i64,
        name: &str,
        by: &str,
    ) -> SResult<(u64, Record)> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let sole: Vec<i64> = {
            let mut stmt = tx.prepare(
                "SELECT rc.record_id FROM record_channels rc
                 WHERE rc.channel_id = ?1
                   AND NOT EXISTS (SELECT 1 FROM record_channels o
                                   WHERE o.record_id = rc.record_id AND o.channel_id != ?1)",
            )?;
            stmt.query_map(params![channel_id], |r| r.get(0))?
                .collect::<Result<_, _>>()?
        };
        for rid in &sole {
            tx.execute("DELETE FROM acks WHERE record_id = ?1", params![rid])?;
            tx.execute(
                "DELETE FROM record_participants WHERE record_id = ?1",
                params![rid],
            )?;
        }
        tx.execute(
            "DELETE FROM record_channels WHERE channel_id = ?1",
            params![channel_id],
        )?;
        for rid in &sole {
            tx.execute("DELETE FROM records WHERE id = ?1", params![rid])?;
        }
        // System records referencing the channel are kept (they are the
        // audit trail of the channel's lifecycle), but the reference is
        // detached so the id can be vacuumed.
        tx.execute(
            "UPDATE records SET channel_ref = NULL WHERE channel_ref = ?1",
            params![channel_id],
        )?;
        tx.execute(
            "DELETE FROM subscriptions WHERE channel_id = ?1",
            params![channel_id],
        )?;
        tx.execute(
            "DELETE FROM subscription_overrides WHERE channel_id = ?1",
            params![channel_id],
        )?;
        tx.execute("DELETE FROM channels WHERE id = ?1", params![channel_id])?;
        let event = SystemEvent::ChannelDeleted {
            channel_id: channel_id as u64,
            name: name.to_string(),
            record_count: sole.len() as u64,
            by: by.to_string(),
        };
        let record = append_system_in(&tx, &event, None)?;
        tx.commit()?;
        Ok((sole.len() as u64, record))
    }

    pub fn live_channels(&self) -> SResult<Vec<ChannelRow>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare("SELECT id, name, archived FROM channels WHERE archived = 0 ORDER BY name")?;
        let rows = stmt
            .query_map([], |r| {
                Ok(ChannelRow {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    archived: r.get::<_, i64>(2)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn channel_count(&self) -> SResult<u64> {
        let conn = self.conn();
        Ok(conn.query_row("SELECT COUNT(*) FROM channels", [], |r| r.get::<_, i64>(0))? as u64)
    }

    // -- principals ---------------------------------------------------------

    /// Persist a registration: the principal row and its Registered audit
    /// record commit together — a failure after principal creation must not
    /// leave a phantom, never-registered name behind.
    pub fn register_principal(&self, name: &str, admin: bool) -> SResult<Record> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO principals(name) VALUES (?1) ON CONFLICT DO NOTHING",
            params![name],
        )?;
        let event = SystemEvent::Registered {
            principal: name.to_string(),
            admin,
        };
        let record = append_system_in(&tx, &event, None)?;
        tx.commit()?;
        Ok(record)
    }

    pub fn principal_id(&self, name: &str) -> SResult<Option<i64>> {
        let conn = self.conn();
        Ok(conn
            .query_row(
                "SELECT id FROM principals WHERE name = ?1",
                params![name],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn all_principals(&self) -> SResult<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT name FROM principals ORDER BY name")?;
        Ok(stmt
            .query_map([], |r| r.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn principal_count(&self) -> SResult<u64> {
        let conn = self.conn();
        Ok(conn.query_row("SELECT COUNT(*) FROM principals", [], |r| {
            r.get::<_, i64>(0)
        })? as u64)
    }

    // -- subscriptions (self-service + human overrides, ADR-0009) -----------

    /// Self-service subscribe/unsubscribe: the override check, the membership
    /// change, and the audit record are one transaction — an admin decision
    /// can never be raced past, and a change can never go unlogged.
    pub fn self_subscription(
        &self,
        principal_id: i64,
        channel_id: i64,
        subscribe: bool,
        principal: &str,
        channel: &str,
    ) -> SResult<SubOutcome> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mode = override_in(&tx, principal_id, channel_id)?;
        match (subscribe, mode) {
            (true, Some(OverrideMode::Cancelled)) => {
                return Ok(SubOutcome::Denied(OverrideMode::Cancelled));
            }
            (false, Some(OverrideMode::Forced)) => {
                return Ok(SubOutcome::Denied(OverrideMode::Forced));
            }
            _ => {}
        }
        let changed = apply_membership(&tx, principal_id, channel_id, subscribe)?;
        if !changed {
            tx.commit()?;
            return Ok(SubOutcome::NoChange);
        }
        let event = subscription_event(principal, channel, subscribe, false);
        let record = append_system_in(&tx, &event, Some(channel_id))?;
        tx.commit()?;
        Ok(SubOutcome::Applied(record))
    }

    /// Human override (ADR-0009): records the forced/cancelled policy and
    /// applies the membership change in one transaction. A policy transition
    /// is audited even when effective membership does not change (forcing an
    /// existing member still pins them). Returns the record to stream, None
    /// when both policy and membership were already as requested.
    pub fn admin_subscription(
        &self,
        principal_id: i64,
        channel_id: i64,
        subscribe: bool,
        principal: &str,
        channel: &str,
    ) -> SResult<Option<Record>> {
        let mode = if subscribe {
            OverrideMode::Forced
        } else {
            OverrideMode::Cancelled
        };
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let policy_changed = override_in(&tx, principal_id, channel_id)? != Some(mode);
        tx.execute(
            "INSERT INTO subscription_overrides(principal_id, channel_id, mode)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(principal_id, channel_id) DO UPDATE SET mode = ?3",
            params![principal_id, channel_id, mode.as_str()],
        )?;
        let membership_changed = apply_membership(&tx, principal_id, channel_id, subscribe)?;
        if !policy_changed && !membership_changed {
            tx.commit()?;
            return Ok(None);
        }
        let event = subscription_event(principal, channel, subscribe, true);
        let record = append_system_in(&tx, &event, Some(channel_id))?;
        tx.commit()?;
        Ok(Some(record))
    }

    pub fn subscribers_of(&self, channel_id: i64) -> SResult<Vec<String>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT p.name FROM subscriptions s JOIN principals p ON p.id = s.principal_id
                 WHERE s.channel_id = ?1 ORDER BY p.name",
        )?;
        Ok(stmt
            .query_map(params![channel_id], |r| r.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn is_subscribed(&self, principal_id: i64, channel_id: i64) -> SResult<bool> {
        let conn = self.conn();
        Ok(conn
            .query_row(
                "SELECT 1 FROM subscriptions WHERE principal_id = ?1 AND channel_id = ?2",
                params![principal_id, channel_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    // -- records ------------------------------------------------------------

    /// Append a message record together with every recipient's initial ack
    /// row (held for deliverable recipients, failed/disconnected for absent
    /// ones) in one transaction: a stored message always has its inspectable
    /// per-recipient ack state. `thread_id: None` starts a new thread (the
    /// thread id becomes the message's own id).
    #[allow(clippy::too_many_arguments)]
    pub fn append_message_with_acks(
        &self,
        sender: &str,
        recipients: &Recipients,
        body: &str,
        truncated: bool,
        thread_id: Option<u64>,
        channel_ids: &[i64],
        held: &[String],
        failed_disconnected: &[String],
    ) -> SResult<Envelope> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let ts = now_ms();
        let recipients_json = serde_json::to_string(recipients)?;
        tx.execute(
            "INSERT INTO records(kind, timestamp, thread_id, sender, body, truncated, recipients_json)
             VALUES ('message', ?1, ?2, ?3, ?4, ?5, ?6)",
            params![ts as i64, thread_id.map(|t| t as i64), sender, body, truncated as i64, recipients_json],
        )?;
        let id = tx.last_insert_rowid();
        let thread = thread_id.unwrap_or(id as u64);
        tx.execute(
            "UPDATE records SET thread_id = ?1 WHERE id = ?2",
            params![thread as i64, id],
        )?;
        for cid in channel_ids {
            tx.execute(
                "INSERT INTO record_channels(record_id, channel_id) VALUES (?1, ?2)",
                params![id, cid],
            )?;
        }
        if channel_ids.is_empty() {
            // DM: index participants (sender + addressed principals).
            tx.execute(
                "INSERT INTO record_participants(record_id, principal) VALUES (?1, ?2)
                 ON CONFLICT DO NOTHING",
                params![id, sender],
            )?;
            for p in &recipients.principals {
                tx.execute(
                    "INSERT INTO record_participants(record_id, principal) VALUES (?1, ?2)
                     ON CONFLICT DO NOTHING",
                    params![id, p],
                )?;
            }
        }
        for recipient in held {
            tx.execute(
                "INSERT INTO acks(record_id, recipient, state, held_at) VALUES (?1, ?2, 'held', ?3)",
                params![id, recipient, ts as i64],
            )?;
        }
        for recipient in failed_disconnected {
            tx.execute(
                "INSERT INTO acks(record_id, recipient, state, reason, failed_at)
                 VALUES (?1, ?2, 'failed', 'disconnected', ?3)",
                params![id, recipient, ts as i64],
            )?;
        }
        tx.commit()?;
        Ok(Envelope {
            message_id: id as u64,
            thread_id: thread,
            timestamp: ts,
            sender: sender.to_string(),
            recipients: recipients.clone(),
            body: body.to_string(),
            truncated,
        })
    }

    /// Append a system record not tied to another store mutation
    /// (registration lifecycle, disconnects).
    pub fn append_system(&self, event: &SystemEvent, channel_ref: Option<i64>) -> SResult<Record> {
        let conn = self.conn();
        append_system_in(&conn, event, channel_ref)
    }

    pub fn message_exists(&self, id: u64) -> SResult<bool> {
        let conn = self.conn();
        Ok(conn
            .query_row(
                "SELECT 1 FROM records WHERE id = ?1 AND kind = 'message'",
                params![id as i64],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn envelope_of(&self, id: u64) -> SResult<Option<Envelope>> {
        let conn = self.conn();
        Ok(conn
            .query_row(
                "SELECT id, thread_id, timestamp, sender, body, truncated, recipients_json
             FROM records WHERE id = ?1 AND kind = 'message'",
                params![id as i64],
                row_to_envelope,
            )
            .optional()?)
    }

    /// History for one channel: message records targeting it plus system
    /// records referencing it, newest-last, cursor-paged.
    pub fn history_channel(
        &self,
        channel_id: i64,
        before: Option<u64>,
        limit: u32,
    ) -> SResult<Vec<Record>> {
        let conn = self.conn();
        let before = before.map(|b| b as i64).unwrap_or(i64::MAX);
        let mut stmt = conn.prepare(
            "SELECT r.id, r.kind, r.timestamp, r.thread_id, r.sender, r.body, r.truncated,
                        r.recipients_json, r.event_json
                 FROM records r
                 WHERE r.id < ?1 AND (
                       r.id IN (SELECT record_id FROM record_channels WHERE channel_id = ?2)
                    OR (r.kind = 'system' AND r.channel_ref = ?2))
                 ORDER BY r.id DESC LIMIT ?3",
        )?;
        let mut rows: Vec<Record> = stmt
            .query_map(params![before, channel_id, limit], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.reverse();
        Ok(rows)
    }

    /// DM history between two principals, newest-last, cursor-paged.
    pub fn history_dm(
        &self,
        a: &str,
        b: &str,
        before: Option<u64>,
        limit: u32,
    ) -> SResult<Vec<Record>> {
        let conn = self.conn();
        let before = before.map(|v| v as i64).unwrap_or(i64::MAX);
        let mut stmt = conn.prepare(
            "SELECT r.id, r.kind, r.timestamp, r.thread_id, r.sender, r.body, r.truncated,
                        r.recipients_json, r.event_json
                 FROM records r
                 WHERE r.id < ?1 AND r.kind = 'message'
                   AND r.id IN (SELECT record_id FROM record_participants WHERE principal = ?2)
                   AND r.id IN (SELECT record_id FROM record_participants WHERE principal = ?3)
                 ORDER BY r.id DESC LIMIT ?4",
        )?;
        let mut rows: Vec<Record> = stmt
            .query_map(params![before, a, b, limit], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.reverse();
        Ok(rows)
    }

    // -- acknowledgments ----------------------------------------------------

    /// Advance an ack. The lifecycle is monotonic: a state only replaces a
    /// lower-ranked one (delivery tasks and processed notifications race —
    /// e.g. a late `relayed` must not regress `processed`). The per-state
    /// timestamp is recorded either way. Returns the timestamp when the
    /// state actually advanced, None when the update was a stale straggler.
    pub fn ack_set(
        &self,
        record_id: u64,
        recipient: &str,
        state: AckState,
        reason: Option<&str>,
    ) -> SResult<Option<u64>> {
        let conn = self.conn();
        let ts = now_ms();
        let (col, st) = ack_column(state);
        let current: Option<String> = conn
            .query_row(
                "SELECT state FROM acks WHERE record_id = ?1 AND recipient = ?2",
                params![record_id as i64, recipient],
                |r| r.get(0),
            )
            .optional()?;
        let Some(current) = current else {
            return Ok(None);
        };
        let advance = ack_rank(state) > ack_rank(parse_ack_state(&current));
        if advance {
            conn.execute(
                &format!(
                    "UPDATE acks SET state = ?3, reason = ?4, {col} = ?5
                     WHERE record_id = ?1 AND recipient = ?2"
                ),
                params![record_id as i64, recipient, st, reason, ts as i64],
            )?;
            Ok(Some(ts))
        } else {
            conn.execute(
                &format!("UPDATE acks SET {col} = ?3 WHERE record_id = ?1 AND recipient = ?2"),
                params![record_id as i64, recipient, ts as i64],
            )?;
            Ok(None)
        }
    }

    pub fn acks_for(&self, record_id: u64) -> SResult<Vec<AckStatus>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT recipient, state, reason, held_at, relayed_at, processed_at, failed_at
                 FROM acks WHERE record_id = ?1 ORDER BY recipient",
        )?;
        Ok(stmt
            .query_map(params![record_id as i64], |r| {
                Ok(AckStatus {
                    recipient: r.get(0)?,
                    state: parse_ack_state(&r.get::<_, String>(1)?),
                    reason: r.get(2)?,
                    held_at: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                    relayed_at: r.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                    processed_at: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    failed_at: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// All held deliveries for one recipient (drain-on-register and restart
    /// re-evaluation).
    pub fn held_for(&self, recipient: &str) -> SResult<Vec<u64>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT record_id FROM acks WHERE recipient = ?1 AND state = 'held' ORDER BY record_id",
        )?;
        Ok(stmt
            .query_map(params![recipient], |r| r.get::<_, i64>(0))?
            .map(|v| v.map(|v| v as u64))
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Every held (record, recipient) pair — the restart grace-window sweep.
    pub fn held_all(&self) -> SResult<Vec<(u64, String)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT record_id, recipient FROM acks WHERE state = 'held' ORDER BY record_id",
        )?;
        Ok(stmt
            .query_map([], |r| {
                Ok((r.get::<_, i64>(0)? as u64, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }
}

/// Append one system record on any connection-like handle (plain connection
/// or open transaction).
fn append_system_in(
    conn: &Connection,
    event: &SystemEvent,
    channel_ref: Option<i64>,
) -> SResult<Record> {
    let ts = now_ms();
    let event_json = serde_json::to_string(event)?;
    conn.execute(
        "INSERT INTO records(kind, timestamp, event_json, channel_ref)
         VALUES ('system', ?1, ?2, ?3)",
        params![ts as i64, event_json, channel_ref],
    )?;
    Ok(Record::System {
        id: conn.last_insert_rowid() as u64,
        timestamp: ts,
        event: event.clone(),
    })
}

fn override_in(
    conn: &Connection,
    principal_id: i64,
    channel_id: i64,
) -> SResult<Option<OverrideMode>> {
    let mode: Option<String> = conn
        .query_row(
            "SELECT mode FROM subscription_overrides
             WHERE principal_id = ?1 AND channel_id = ?2",
            params![principal_id, channel_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(mode.as_deref().and_then(|s| match s {
        "forced" => Some(OverrideMode::Forced),
        "cancelled" => Some(OverrideMode::Cancelled),
        _ => None,
    }))
}

fn apply_membership(
    conn: &Connection,
    principal_id: i64,
    channel_id: i64,
    subscribe: bool,
) -> SResult<bool> {
    let changed = if subscribe {
        conn.execute(
            "INSERT INTO subscriptions(principal_id, channel_id) VALUES (?1, ?2)
             ON CONFLICT DO NOTHING",
            params![principal_id, channel_id],
        )?
    } else {
        conn.execute(
            "DELETE FROM subscriptions WHERE principal_id = ?1 AND channel_id = ?2",
            params![principal_id, channel_id],
        )?
    };
    Ok(changed > 0)
}

fn subscription_event(
    principal: &str,
    channel: &str,
    subscribe: bool,
    by_admin: bool,
) -> SystemEvent {
    if subscribe {
        SystemEvent::Subscribed {
            principal: principal.to_string(),
            channel: channel.to_string(),
            by_admin,
        }
    } else {
        SystemEvent::Unsubscribed {
            principal: principal.to_string(),
            channel: channel.to_string(),
            by_admin,
        }
    }
}

/// Lifecycle ordering: held < relayed < (processed | failed). The two
/// terminal states tie — neither replaces the other.
fn ack_rank(state: AckState) -> u8 {
    match state {
        AckState::Held => 0,
        AckState::Relayed => 1,
        AckState::Processed => 2,
        AckState::Failed => 2,
    }
}

fn ack_column(state: AckState) -> (&'static str, &'static str) {
    match state {
        AckState::Held => ("held_at", "held"),
        AckState::Relayed => ("relayed_at", "relayed"),
        AckState::Processed => ("processed_at", "processed"),
        AckState::Failed => ("failed_at", "failed"),
    }
}

fn parse_ack_state(s: &str) -> AckState {
    match s {
        "held" => AckState::Held,
        "relayed" => AckState::Relayed,
        "processed" => AckState::Processed,
        _ => AckState::Failed,
    }
}

/// Corrupt persisted JSON surfaces as an SQL-layer conversion failure for
/// the row — never silently degraded (an empty audience is a lie).
fn json_column<T: serde::de::DeserializeOwned>(idx: usize, raw: &str) -> rusqlite::Result<T> {
    serde_json::from_str(raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(e))
    })
}

fn row_to_envelope(r: &rusqlite::Row<'_>) -> rusqlite::Result<Envelope> {
    let recipients_json: String = r.get(6)?;
    Ok(Envelope {
        message_id: r.get::<_, i64>(0)? as u64,
        thread_id: r.get::<_, i64>(1)? as u64,
        timestamp: r.get::<_, i64>(2)? as u64,
        sender: r.get(3)?,
        body: r.get(4)?,
        truncated: r.get::<_, i64>(5)? != 0,
        recipients: json_column(6, &recipients_json)?,
    })
}

fn row_to_record(r: &rusqlite::Row<'_>) -> rusqlite::Result<Record> {
    let kind: String = r.get(1)?;
    if kind == "message" {
        let recipients_json: String = r.get(7)?;
        Ok(Record::Message {
            envelope: Envelope {
                message_id: r.get::<_, i64>(0)? as u64,
                thread_id: r.get::<_, Option<i64>>(3)?.unwrap_or_default() as u64,
                timestamp: r.get::<_, i64>(2)? as u64,
                sender: r.get::<_, Option<String>>(4)?.unwrap_or_default(),
                body: r.get::<_, Option<String>>(5)?.unwrap_or_default(),
                truncated: r.get::<_, Option<i64>>(6)?.unwrap_or_default() != 0,
                recipients: json_column(7, &recipients_json)?,
            },
        })
    } else {
        let event_json: String = r.get(8)?;
        let event = json_column(8, &event_json)?;
        Ok(Record::System {
            id: r.get::<_, i64>(0)? as u64,
            timestamp: r.get::<_, i64>(2)? as u64,
            event,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Corrupt persisted JSON must surface as an error, never degrade into
    /// an empty audience or a skipped record.
    #[test]
    fn corrupt_json_surfaces_as_errors() {
        let store = Store::open_in_memory().unwrap();
        let channel_id = {
            let (id, _record) = store.create_channel_logged("#c", "@a").unwrap();
            let conn = store.conn();
            conn.execute(
                "INSERT INTO records(kind, timestamp, thread_id, sender, body, truncated, recipients_json)
                 VALUES ('message', 1, 0, '@a', 'x', 0, 'not-json')",
                [],
            )
            .unwrap();
            let msg_id = conn.last_insert_rowid();
            conn.execute(
                "UPDATE records SET thread_id = ?1 WHERE id = ?1",
                params![msg_id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO record_channels(record_id, channel_id) VALUES (?1, ?2)",
                params![msg_id, id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO records(kind, timestamp, event_json, channel_ref)
                 VALUES ('system', 2, 'not-json', ?1)",
                params![id],
            )
            .unwrap();
            id
        };
        // envelope_of parses recipients_json.
        let msg_id = 2; // record 1 is the channel-created system record
        assert!(
            store.envelope_of(msg_id).is_err(),
            "corrupt recipients must error"
        );
        // history readers hit both corrupt rows.
        assert!(
            store.history_channel(channel_id, None, 10).is_err(),
            "corrupt history rows must error"
        );
    }
}
