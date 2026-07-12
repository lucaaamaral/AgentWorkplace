//! SQLite store: the append-only log, channels, principals, subscriptions,
//! and acknowledgment state (ADR-0005, ADR-0017).
//!
//! Single writer: only the broker process opens the database. Message and
//! system records are never updated or deleted (sole exception: channel
//! deletion, ADR-0018, which leaves a tombstone). Ack state lives in
//! separate mutable rows.

use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use protocol::{AckState, AckStatus, Envelope, Record, Recipients, SystemEvent};
use rusqlite::{params, Connection, OptionalExtension};

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_millis() as u64
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("name taken")]
    NameTaken,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

pub struct ChannelRow {
    pub id: i64,
    pub name: String,
    pub archived: bool,
}

pub struct Store {
    conn: Mutex<Connection>,
}

const SCHEMA_VERSION: i64 = 1;

impl Store {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Store { conn: Mutex::new(conn) };
        store.migrate()?;
        Ok(store)
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Store { conn: Mutex::new(conn) };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS channels (
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
             ON CONFLICT(key) DO NOTHING",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    // -- channels ----------------------------------------------------------

    pub fn create_channel(&self, name: &str) -> Result<i64, StoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.execute("INSERT INTO channels(name) VALUES (?1)", params![name]) {
            Ok(_) => Ok(conn.last_insert_rowid()),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(StoreError::NameTaken)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn channel_by_name(&self, name: &str) -> Option<ChannelRow> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
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
        .optional()
        .expect("query channels")
    }

    pub fn rename_channel(&self, id: i64, new_name: &str) -> Result<(), StoreError> {
        let conn = self.conn.lock().unwrap();
        match conn.execute("UPDATE channels SET name = ?1 WHERE id = ?2", params![new_name, id]) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(StoreError::NameTaken)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn set_archived(&self, id: i64, archived: bool) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE channels SET archived = ?1 WHERE id = ?2",
            params![archived as i64, id],
        )?;
        Ok(())
    }

    /// Force-cancel every subscription of a channel; returns the principals
    /// that were subscribed.
    pub fn clear_subscriptions(&self, channel_id: i64) -> rusqlite::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT p.name FROM subscriptions s JOIN principals p ON p.id = s.principal_id
             WHERE s.channel_id = ?1",
        )?;
        let names: Vec<String> =
            stmt.query_map(params![channel_id], |r| r.get(0))?.collect::<Result<_, _>>()?;
        conn.execute("DELETE FROM subscriptions WHERE channel_id = ?1", params![channel_id])?;
        Ok(names)
    }

    /// Permanent deletion (ADR-0018): removes records that belong solely to
    /// this channel, detaches shared ones, drops subscriptions and the
    /// channel row. Returns the number of records fully removed.
    pub fn delete_channel(&self, channel_id: i64) -> rusqlite::Result<u64> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let sole: Vec<i64> = {
            let mut stmt = tx.prepare(
                "SELECT rc.record_id FROM record_channels rc
                 WHERE rc.channel_id = ?1
                   AND NOT EXISTS (SELECT 1 FROM record_channels o
                                   WHERE o.record_id = rc.record_id AND o.channel_id != ?1)",
            )?;
            stmt.query_map(params![channel_id], |r| r.get(0))?.collect::<Result<_, _>>()?
        };
        for rid in &sole {
            tx.execute("DELETE FROM acks WHERE record_id = ?1", params![rid])?;
            tx.execute("DELETE FROM record_participants WHERE record_id = ?1", params![rid])?;
        }
        tx.execute("DELETE FROM record_channels WHERE channel_id = ?1", params![channel_id])?;
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
        tx.execute("DELETE FROM subscriptions WHERE channel_id = ?1", params![channel_id])?;
        tx.execute("DELETE FROM channels WHERE id = ?1", params![channel_id])?;
        tx.commit()?;
        Ok(sole.len() as u64)
    }

    pub fn live_channels(&self) -> Vec<ChannelRow> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, archived FROM channels WHERE archived = 0 ORDER BY name")
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok(ChannelRow {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    archived: r.get::<_, i64>(2)? != 0,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows
    }

    pub fn channel_count(&self) -> u64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM channels", [], |r| r.get::<_, i64>(0)).unwrap() as u64
    }

    // -- principals ---------------------------------------------------------

    pub fn ensure_principal(&self, name: &str) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.execute("INSERT INTO principals(name) VALUES (?1) ON CONFLICT DO NOTHING", params![name])
            .expect("insert principal");
        conn.query_row("SELECT id FROM principals WHERE name = ?1", params![name], |r| r.get(0))
            .expect("principal id")
    }

    pub fn principal_id(&self, name: &str) -> Option<i64> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT id FROM principals WHERE name = ?1", params![name], |r| r.get(0))
            .optional()
            .expect("query principal")
    }

    pub fn all_principals(&self) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT name FROM principals ORDER BY name").unwrap();
        stmt.query_map([], |r| r.get(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    }

    pub fn principal_count(&self) -> u64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT COUNT(*) FROM principals", [], |r| r.get::<_, i64>(0)).unwrap()
            as u64
    }

    // -- subscriptions ------------------------------------------------------

    pub fn subscribe(&self, principal_id: i64, channel_id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO subscriptions(principal_id, channel_id) VALUES (?1, ?2)
             ON CONFLICT DO NOTHING",
            params![principal_id, channel_id],
        )
        .expect("subscribe")
            > 0
    }

    pub fn unsubscribe(&self, principal_id: i64, channel_id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM subscriptions WHERE principal_id = ?1 AND channel_id = ?2",
            params![principal_id, channel_id],
        )
        .expect("unsubscribe")
            > 0
    }

    pub fn subscribers_of(&self, channel_id: i64) -> Vec<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT p.name FROM subscriptions s JOIN principals p ON p.id = s.principal_id
                 WHERE s.channel_id = ?1 ORDER BY p.name",
            )
            .unwrap();
        stmt.query_map(params![channel_id], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    pub fn is_subscribed(&self, principal_id: i64, channel_id: i64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT 1 FROM subscriptions WHERE principal_id = ?1 AND channel_id = ?2",
            params![principal_id, channel_id],
            |_| Ok(()),
        )
        .optional()
        .expect("query subscription")
        .is_some()
    }

    // -- records ------------------------------------------------------------

    /// Append a message record. `thread_id: None` starts a new thread (the
    /// thread id becomes the message's own id).
    pub fn append_message(
        &self,
        sender: &str,
        recipients: &Recipients,
        body: &str,
        truncated: bool,
        thread_id: Option<u64>,
        channel_ids: &[i64],
    ) -> Envelope {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().expect("tx");
        let ts = now_ms();
        let recipients_json = serde_json::to_string(recipients).expect("recipients json");
        tx.execute(
            "INSERT INTO records(kind, timestamp, thread_id, sender, body, truncated, recipients_json)
             VALUES ('message', ?1, ?2, ?3, ?4, ?5, ?6)",
            params![ts as i64, thread_id.map(|t| t as i64), sender, body, truncated as i64, recipients_json],
        )
        .expect("insert record");
        let id = tx.last_insert_rowid();
        let thread = thread_id.unwrap_or(id as u64);
        tx.execute("UPDATE records SET thread_id = ?1 WHERE id = ?2", params![thread as i64, id])
            .expect("thread update");
        for cid in channel_ids {
            tx.execute(
                "INSERT INTO record_channels(record_id, channel_id) VALUES (?1, ?2)",
                params![id, cid],
            )
            .expect("record channel");
        }
        if channel_ids.is_empty() {
            // DM: index participants (sender + addressed principals).
            tx.execute(
                "INSERT INTO record_participants(record_id, principal) VALUES (?1, ?2)
                 ON CONFLICT DO NOTHING",
                params![id, sender],
            )
            .expect("participant");
            for p in &recipients.principals {
                tx.execute(
                    "INSERT INTO record_participants(record_id, principal) VALUES (?1, ?2)
                     ON CONFLICT DO NOTHING",
                    params![id, p],
                )
                .expect("participant");
            }
        }
        tx.commit().expect("commit");
        Envelope {
            message_id: id as u64,
            thread_id: thread,
            timestamp: ts,
            sender: sender.to_string(),
            recipients: recipients.clone(),
            body: body.to_string(),
            truncated,
        }
    }

    pub fn append_system(&self, event: &SystemEvent, channel_ref: Option<i64>) -> Record {
        let conn = self.conn.lock().unwrap();
        let ts = now_ms();
        let event_json = serde_json::to_string(event).expect("event json");
        conn.execute(
            "INSERT INTO records(kind, timestamp, event_json, channel_ref)
             VALUES ('system', ?1, ?2, ?3)",
            params![ts as i64, event_json, channel_ref],
        )
        .expect("insert system record");
        Record::System { id: conn.last_insert_rowid() as u64, timestamp: ts, event: event.clone() }
    }

    pub fn message_exists(&self, id: u64) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT 1 FROM records WHERE id = ?1 AND kind = 'message'",
            params![id as i64],
            |_| Ok(()),
        )
        .optional()
        .expect("query record")
        .is_some()
    }

    pub fn envelope_of(&self, id: u64) -> Option<Envelope> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, thread_id, timestamp, sender, body, truncated, recipients_json
             FROM records WHERE id = ?1 AND kind = 'message'",
            params![id as i64],
            row_to_envelope,
        )
        .optional()
        .expect("query envelope")
    }

    /// History for one channel: message records targeting it plus system
    /// records referencing it, newest-last, cursor-paged.
    pub fn history_channel(&self, channel_id: i64, before: Option<u64>, limit: u32) -> Vec<Record> {
        let conn = self.conn.lock().unwrap();
        let before = before.map(|b| b as i64).unwrap_or(i64::MAX);
        let mut stmt = conn
            .prepare(
                "SELECT r.id, r.kind, r.timestamp, r.thread_id, r.sender, r.body, r.truncated,
                        r.recipients_json, r.event_json
                 FROM records r
                 WHERE r.id < ?1 AND (
                       r.id IN (SELECT record_id FROM record_channels WHERE channel_id = ?2)
                    OR (r.kind = 'system' AND r.channel_ref = ?2))
                 ORDER BY r.id DESC LIMIT ?3",
            )
            .unwrap();
        let mut rows: Vec<Record> = stmt
            .query_map(params![before, channel_id, limit], row_to_record)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.reverse();
        rows
    }

    /// DM history between two principals, newest-last, cursor-paged.
    pub fn history_dm(&self, a: &str, b: &str, before: Option<u64>, limit: u32) -> Vec<Record> {
        let conn = self.conn.lock().unwrap();
        let before = before.map(|v| v as i64).unwrap_or(i64::MAX);
        let mut stmt = conn
            .prepare(
                "SELECT r.id, r.kind, r.timestamp, r.thread_id, r.sender, r.body, r.truncated,
                        r.recipients_json, r.event_json
                 FROM records r
                 WHERE r.id < ?1 AND r.kind = 'message'
                   AND r.id IN (SELECT record_id FROM record_participants WHERE principal = ?2)
                   AND r.id IN (SELECT record_id FROM record_participants WHERE principal = ?3)
                 ORDER BY r.id DESC LIMIT ?4",
            )
            .unwrap();
        let mut rows: Vec<Record> = stmt
            .query_map(params![before, a, b, limit], row_to_record)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.reverse();
        rows
    }

    // -- acknowledgments ----------------------------------------------------

    pub fn ack_init(&self, record_id: u64, recipient: &str, state: AckState, reason: Option<&str>) {
        let conn = self.conn.lock().unwrap();
        let ts = now_ms();
        let (col, st) = ack_column(state);
        conn.execute(
            &format!(
                "INSERT INTO acks(record_id, recipient, state, reason, {col})
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(record_id, recipient) DO UPDATE SET state = ?3, reason = ?4, {col} = ?5"
            ),
            params![record_id as i64, recipient, st, reason, ts as i64],
        )
        .expect("ack init");
    }

    /// Advance an ack. The lifecycle is monotonic: a state only replaces a
    /// lower-ranked one (delivery tasks and processed notifications race —
    /// e.g. a late `relayed` must not regress `processed`). The per-state
    /// timestamp is recorded either way. Returns the timestamp when the
    /// state actually advanced, None when the update was a stale straggler.
    pub fn ack_set(&self, record_id: u64, recipient: &str, state: AckState, reason: Option<&str>) -> Option<u64> {
        let conn = self.conn.lock().unwrap();
        let ts = now_ms();
        let (col, st) = ack_column(state);
        let current: Option<String> = conn
            .query_row(
                "SELECT state FROM acks WHERE record_id = ?1 AND recipient = ?2",
                params![record_id as i64, recipient],
                |r| r.get(0),
            )
            .optional()
            .expect("ack query");
        let Some(current) = current else { return None };
        let advance = ack_rank(state) > ack_rank(parse_ack_state(&current));
        if advance {
            conn.execute(
                &format!(
                    "UPDATE acks SET state = ?3, reason = ?4, {col} = ?5
                     WHERE record_id = ?1 AND recipient = ?2"
                ),
                params![record_id as i64, recipient, st, reason, ts as i64],
            )
            .expect("ack set");
            Some(ts)
        } else {
            conn.execute(
                &format!("UPDATE acks SET {col} = ?3 WHERE record_id = ?1 AND recipient = ?2"),
                params![record_id as i64, recipient, ts as i64],
            )
            .expect("ack timestamp");
            None
        }
    }

    pub fn acks_for(&self, record_id: u64) -> Vec<AckStatus> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT recipient, state, reason, held_at, relayed_at, processed_at, failed_at
                 FROM acks WHERE record_id = ?1 ORDER BY recipient",
            )
            .unwrap();
        stmt.query_map(params![record_id as i64], |r| {
            Ok(AckStatus {
                recipient: r.get(0)?,
                state: parse_ack_state(&r.get::<_, String>(1)?),
                reason: r.get(2)?,
                held_at: r.get::<_, Option<i64>>(3)?.map(|v| v as u64),
                relayed_at: r.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                processed_at: r.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                failed_at: r.get::<_, Option<i64>>(6)?.map(|v| v as u64),
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    /// All held deliveries for one recipient (drain-on-register and restart
    /// re-evaluation).
    pub fn held_for(&self, recipient: &str) -> Vec<u64> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT record_id FROM acks WHERE recipient = ?1 AND state = 'held' ORDER BY record_id")
            .unwrap();
        stmt.query_map(params![recipient], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|v| v.map(|v| v as u64))
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    /// Every held (record, recipient) pair — the restart grace-window sweep.
    pub fn held_all(&self) -> Vec<(u64, String)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT record_id, recipient FROM acks WHERE state = 'held' ORDER BY record_id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, String>(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
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

fn row_to_envelope(r: &rusqlite::Row<'_>) -> rusqlite::Result<Envelope> {
    let recipients_json: String = r.get(6)?;
    Ok(Envelope {
        message_id: r.get::<_, i64>(0)? as u64,
        thread_id: r.get::<_, i64>(1)? as u64,
        timestamp: r.get::<_, i64>(2)? as u64,
        sender: r.get(3)?,
        body: r.get(4)?,
        truncated: r.get::<_, i64>(5)? != 0,
        recipients: serde_json::from_str(&recipients_json).unwrap_or_default(),
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
                recipients: serde_json::from_str(&recipients_json).unwrap_or_default(),
            },
        })
    } else {
        let event_json: String = r.get(8)?;
        Ok(Record::System {
            id: r.get::<_, i64>(0)? as u64,
            timestamp: r.get::<_, i64>(2)? as u64,
            event: serde_json::from_str(&event_json).expect("system event json"),
        })
    }
}
