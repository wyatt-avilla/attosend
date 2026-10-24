use crate::{
    Error, Result,
    config::Config,
    model::{
        Counter, DeliveryOutcome, Event, FailedUnit, Message, MessageId, QueueEntry, QueueState,
        Sample, Status, SubmitResult,
    },
};
use anyhow::{Context, ensure};
use fs2::FileExt;
use serde_json::json;
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    path::Path,
};
use tokio::sync::{mpsc, oneshot};
use turso::{Connection, params};

#[must_use]
pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// All connections stay on the storage thread. The lock lives longer than the database.
pub struct Store {
    conn: Connection,
    _db: turso::Database,
    _lock: File,
    config: Config,
}

impl Store {
    /// # Errors
    /// Returns an error if ownership cannot be acquired, the schema is unsupported, or storage fails.
    pub async fn open(config: Config) -> anyhow::Result<Self> {
        let parent = config.database().parent().context("database directory")?;
        std::fs::create_dir_all(parent)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.database().with_extension("lock"))?;
        lock.try_lock_exclusive()
            .context("another daemon owns this database")?;
        let db = turso::Builder::new_local(
            config
                .database()
                .to_str()
                .context("database path is not UTF-8")?,
        )
        .build()
        .await?;
        let conn = db.connect()?;
        conn.execute("PRAGMA synchronous=FULL", ()).await?;
        let mut rows = conn.query("PRAGMA user_version", ()).await?;
        let version: i64 = rows.next().await?.context("schema version")?.get(0)?;
        drop(rows);
        ensure!(
            version != 1,
            "database schema 1 is unsupported; configure a fresh database for this attosend version"
        );
        ensure!(
            matches!(version, 0 | 2),
            "unsupported database schema version {version}"
        );
        if version == 0 {
            let mut tables = conn
                .query(
                    "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                    (),
                )
                .await?;
            ensure!(
                tables
                    .next()
                    .await?
                    .context("schema tables")?
                    .get::<i64>(0)?
                    == 0,
                "unversioned database is not empty; configure a fresh database"
            );
            drop(tables);
            conn.execute("BEGIN IMMEDIATE", ()).await?;
            if let Err(error) = conn
                .execute_batch(include_str!("../migrations/001.sql"))
                .await
            {
                conn.execute("ROLLBACK", ()).await?;
                return Err(error.into());
            }
            conn.execute("COMMIT", ()).await?;
        }
        Ok(Self {
            conn,
            _db: db,
            _lock: lock,
            config,
        })
    }

    async fn transaction<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.conn
            .execute("BEGIN IMMEDIATE", ())
            .await
            .map_err(storage)?;
        match operation.await {
            Ok(value) => {
                if let Err(error) = self.conn.execute("COMMIT", ()).await {
                    let _ = self.conn.execute("ROLLBACK", ()).await;
                    return Err(storage(error));
                }
                Ok(value)
            }
            Err(error) => {
                self.conn.execute("ROLLBACK", ()).await.map_err(storage)?;
                Err(error)
            }
        }
    }

    /// # Errors
    /// Returns a storage error; the entire batch is rolled back on failure.
    pub async fn submit(&self, events: Vec<Event>, now: i64) -> Result<SubmitResult> {
        self.transaction(async {
            let mut queued = 0;
            for event in events {
                queued += usize::from(self.submit_event(&event, now).await.map_err(storage)?);
            }
            Ok(SubmitResult { queued })
        })
        .await
    }

    /// # Errors
    /// Returns a storage error; observations and recoveries are applied atomically.
    pub async fn check(
        &self,
        samples: Vec<Sample>,
        failed_units: Option<Vec<FailedUnit>>,
        now: i64,
    ) -> Result<()> {
        self.transaction(async {
            for sample in samples {
                self.sample(&sample, now).await.map_err(storage)?;
            }
            if let Some(units) = failed_units {
                let keys: HashSet<_> = units
                    .iter()
                    .map(|unit| unit.event().key().as_str())
                    .collect();
                for unit in &units {
                    self.submit_event(unit.event(), now)
                        .await
                        .map_err(storage)?;
                }
                let mut rows = self
                    .conn
                    .query(
                        "SELECT key FROM incidents WHERE source='systemd' AND active=1",
                        (),
                    )
                    .await
                    .map_err(storage)?;
                let mut resolved = Vec::new();
                while let Some(row) = rows.next().await.map_err(storage)? {
                    let key: String = row.get(0).map_err(storage)?;
                    if !keys.contains(key.as_str()) {
                        resolved.push(key);
                    }
                }
                drop(rows);
                for key in resolved {
                    let unit = key.trim_start_matches("systemd:");
                    let summary = format!("Unit failed state cleared: {unit}");
                    let event = Event::new(
                        key,
                        "systemd".into(),
                        Status::Resolved,
                        summary,
                        "The unit no longer appears in systemd's failed-unit list.".into(),
                    )
                    .map_err(storage)?;
                    self.submit_event(&event, now).await.map_err(storage)?;
                }
            }
            Ok(())
        })
        .await
    }

    /// # Errors
    /// Returns an error if the message is absent, already sent, or storage fails.
    pub async fn retry(&self, id: MessageId) -> Result<()> {
        let changed = self
            .conn
            .execute(
                "UPDATE queue SET state=?,due=0 WHERE id=? AND state!=?",
                params![
                    QueueState::Pending.as_str(),
                    id.to_string(),
                    QueueState::Sent.as_str()
                ],
            )
            .await
            .map_err(storage)?;
        if changed == 0 {
            return Err(Error::Invalid(
                "no pending or failed message with that ID".into(),
            ));
        }
        Ok(())
    }

    async fn submit_event(&self, event: &Event, now: i64) -> anyhow::Result<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT active,last_notice FROM incidents WHERE key=?",
                [event.key().as_str()],
            )
            .await?;
        let old = rows.next().await?;
        let active = event.status() == Status::Firing;
        if let Some(row) = old {
            let was_active = match row.get::<i64>(0)? {
                0 => false,
                1 => true,
                _ => anyhow::bail!("invalid incident boolean"),
            };
            let last: i64 = row.get(1)?;
            if (!active && !was_active)
                || (active
                    && was_active
                    && now.saturating_sub(last) < self.config.reminder_seconds().get())
            {
                return Ok(false);
            }
        } else if !active {
            return Ok(false);
        }
        drop(rows);
        let timestamp = chrono::DateTime::from_timestamp(now, 0)
            .context("timestamp out of range")?
            .to_rfc3339();
        let status = event.status().as_str();
        let payload = json!({
            "from": self.config.sender(), "to": [self.config.recipient()],
            "subject": format!("[{}] {}: {}", self.config.host(), status.to_uppercase(), event.summary()),
            "text": format!("Host: {}\nSource: {}\nIncident: {}\nStatus: {status}\nTime: {timestamp}\n\n{}\n\n{}", self.config.host(), event.source(), event.key(), event.summary(), event.details()),
        }).to_string();
        self.conn
            .execute(
                "INSERT INTO queue(id,payload,created,due,state) VALUES(?,?,?,?,?)",
                params![
                    MessageId::new().to_string(),
                    payload,
                    now,
                    now,
                    QueueState::Pending.as_str()
                ],
            )
            .await?;
        self.conn.execute("INSERT OR REPLACE INTO incidents(key,source,active,last_notice,summary,details) VALUES(?,?,?,?,?,?)",
            params![event.key().as_str(), event.source(), i64::from(active), now, event.summary(), event.details()]).await?;
        Ok(true)
    }

    async fn sample(&self, sample: &Sample, now: i64) -> anyhow::Result<()> {
        let mut rows = self
            .conn
            .query(
                "SELECT since,last,successes FROM samples WHERE key=?",
                [sample.key().as_str()],
            )
            .await?;
        let old = rows.next().await?;
        let (mut since, mut successes) = (now, 0);
        if let Some(row) = old {
            let continuous = now.saturating_sub(row.get::<i64>(1)?) <= 90;
            let old_successes = Counter::try_from(row.get::<i64>(2)?)?.get();
            if continuous {
                if old_successes == 0 {
                    since = row.get(0)?;
                }
                successes = old_successes;
            }
        }
        drop(rows);
        successes = if sample.failed() {
            0
        } else {
            successes.saturating_add(1)
        };
        self.conn
            .execute(
                "INSERT OR REPLACE INTO samples(key,since,last,successes) VALUES(?,?,?,?)",
                params![sample.key().as_str(), since, now, successes],
            )
            .await?;
        if (sample.failed()
            && now.saturating_sub(since) >= self.config.checks().failure_seconds().get())
            || (!sample.failed() && successes >= 2)
        {
            let event = Event::new(
                sample.key().to_string(),
                "healthcheck".into(),
                if sample.failed() {
                    Status::Firing
                } else {
                    Status::Resolved
                },
                sample.summary().to_owned(),
                sample.details().to_owned(),
            )?;
            self.submit_event(&event, now).await?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if queued records cannot be read or decoded.
    pub async fn queue(&self) -> anyhow::Result<Vec<QueueEntry>> {
        let mut rows = self.conn.query("SELECT id,created,state,attempts,error FROM queue WHERE state!=? ORDER BY sequence", [QueueState::Sent.as_str()]).await?;
        let mut queue = Vec::new();
        while let Some(row) = rows.next().await? {
            queue.push(QueueEntry {
                id: decode_id(&row, 0)?,
                created: row.get(1)?,
                state: row.get::<String>(2)?.parse()?,
                attempts: decode_counter(&row, 3)?,
                error: row.get(4)?,
            });
        }
        Ok(queue)
    }

    /// # Errors
    /// Returns an error if the pending queue cannot be read or decoded.
    pub async fn next(&self, now: i64) -> anyhow::Result<Option<Message>> {
        let mut rows = self
            .conn
            .query(
                "SELECT id,payload,attempts,due FROM queue WHERE state=? ORDER BY sequence LIMIT 1",
                [QueueState::Pending.as_str()],
            )
            .await?;
        if let Some(row) = rows.next().await? {
            let message = Message {
                id: decode_id(&row, 0)?,
                payload: row.get(1)?,
                attempts: decode_counter(&row, 2)?,
            };
            if row.get::<i64>(3)? <= now {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    /// # Errors
    /// Returns an error if the delivery result cannot be persisted.
    pub async fn complete(
        &self,
        message: &Message,
        delivery: &DeliveryOutcome,
        now: i64,
    ) -> anyhow::Result<()> {
        let attempts = message.attempts.increment().get();
        let exponent = u32::try_from(message.attempts.get().min(5))?;
        let (state, error, due) = match delivery {
            DeliveryOutcome::Sent => (QueueState::Sent, None, now),
            DeliveryOutcome::Failed { error } => (QueueState::Failed, Some(error.as_str()), now),
            DeliveryOutcome::Retry { error, after } => {
                let delay = i64::try_from(after.as_secs())
                    .unwrap_or(i64::MAX)
                    .max((30_i64 * 2_i64.pow(exponent)).min(900));
                (
                    QueueState::Pending,
                    Some(error.as_str()),
                    now.saturating_add(delay),
                )
            }
        };
        self.conn
            .execute(
                "UPDATE queue SET state=?,error=?,attempts=?,due=? WHERE id=?",
                params![state.as_str(), error, attempts, due, message.id.to_string()],
            )
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns an error if expired records cannot be removed.
    pub async fn prune(&self, now: i64) -> anyhow::Result<()> {
        let cutoff = now.saturating_sub(7 * 86400);
        self.conn
            .execute(
                "DELETE FROM queue WHERE state=? AND created < ?",
                params![QueueState::Sent.as_str(), cutoff],
            )
            .await?;
        self.conn
            .execute(
                "DELETE FROM incidents WHERE active=0 AND last_notice < ?",
                [cutoff],
            )
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns an error if queue statistics cannot be read.
    pub async fn metrics(&self, now: i64) -> anyhow::Result<String> {
        let mut rows = self
            .conn
            .query(
                "SELECT count(*),min(created) FROM queue WHERE state=?",
                [QueueState::Pending.as_str()],
            )
            .await?;
        let row = rows.next().await?.context("pending metrics")?;
        let count: i64 = row.get(0)?;
        let created: Option<i64> = row.get(1)?;
        let age = created.map_or(0, |created| now.saturating_sub(created).max(0));
        drop(rows);
        let mut rows = self
            .conn
            .query(
                "SELECT count(*) FROM queue WHERE state=?",
                [QueueState::Failed.as_str()],
            )
            .await?;
        let failed: i64 = rows.next().await?.context("failed metrics")?.get(0)?;
        Ok(format!(
            "notifications_pending {count}\nnotifications_failed {failed}\nnotifications_oldest_pending_seconds {age}\n"
        ))
    }
}

fn decode_id(row: &turso::Row, index: usize) -> anyhow::Result<MessageId> {
    Ok(row.get::<String>(index)?.parse()?)
}
fn decode_counter(row: &turso::Row, index: usize) -> anyhow::Result<Counter> {
    Ok(Counter::try_from(row.get::<i64>(index)?)?)
}

fn storage(error: impl Into<anyhow::Error>) -> Error {
    Error::Storage(error.into())
}

enum Job {
    Submit {
        events: Vec<Event>,
        reply: oneshot::Sender<Result<SubmitResult>>,
    },
    Check {
        samples: Vec<Sample>,
        failed_units: Option<Vec<FailedUnit>>,
        reply: oneshot::Sender<Result<()>>,
    },
    Queue {
        reply: oneshot::Sender<Result<Vec<QueueEntry>>>,
    },
    Retry {
        id: MessageId,
        reply: oneshot::Sender<Result<()>>,
    },
    Next {
        reply: oneshot::Sender<Result<Option<Message>>>,
    },
    Complete {
        message: Message,
        delivery: DeliveryOutcome,
        reply: oneshot::Sender<Result<()>>,
    },
    Metrics {
        reply: oneshot::Sender<Result<String>>,
    },
    Prune {
        reply: oneshot::Sender<Result<()>>,
    },
}
fn reply_to<T>(reply: oneshot::Sender<Result<T>>, result: Result<T>) {
    if let Err(Error::Storage(error)) = &result {
        tracing::error!(%error, "storage operation failed");
    }
    let _ = reply.send(result);
}
#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Job>,
}
impl Handle {
    /// # Errors
    /// Returns an error if the storage thread or database cannot be initialized.
    pub async fn start(config: Config) -> anyhow::Result<(Self, oneshot::Receiver<()>)> {
        let (tx, mut rx) = mpsc::channel::<Job>(64);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("attosend-storage".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                let runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.into()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let store = match Store::open(config).await {
                        Ok(store) => store,
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    while let Some(job) = rx.recv().await {
                        match job {
                            Job::Submit { events, reply } => {
                                reply_to(reply, store.submit(events, now()).await);
                            }
                            Job::Check {
                                samples,
                                failed_units,
                                reply,
                            } => reply_to(reply, store.check(samples, failed_units, now()).await),
                            Job::Queue { reply } => {
                                reply_to(reply, store.queue().await.map_err(storage));
                            }
                            Job::Retry { id, reply } => reply_to(reply, store.retry(id).await),
                            Job::Next { reply } => {
                                reply_to(reply, store.next(now()).await.map_err(storage));
                            }
                            Job::Complete {
                                message,
                                delivery,
                                reply,
                            } => reply_to(
                                reply,
                                store
                                    .complete(&message, &delivery, now())
                                    .await
                                    .map_err(storage),
                            ),
                            Job::Metrics { reply } => {
                                reply_to(reply, store.metrics(now()).await.map_err(storage));
                            }
                            Job::Prune { reply } => {
                                reply_to(reply, store.prune(now()).await.map_err(storage));
                            }
                        }
                    }
                });
                let _ = exit_tx.send(());
            })?;
        ready_rx
            .await
            .context("storage thread stopped during initialization")??;
        Ok((Self { tx }, exit_rx))
    }

    async fn send<T>(&self, job: impl FnOnce(oneshot::Sender<Result<T>>) -> Job) -> Result<T> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(job(reply))
            .await
            .map_err(|_| Error::Unavailable)?;
        rx.await.map_err(|_| Error::Unavailable)?
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn submit(&self, events: Vec<Event>) -> Result<SubmitResult> {
        self.send(|reply| Job::Submit { events, reply }).await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn check(
        &self,
        samples: Vec<Sample>,
        failed_units: Option<Vec<FailedUnit>>,
    ) -> Result<()> {
        self.send(|reply| Job::Check {
            samples,
            failed_units,
            reply,
        })
        .await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn queue(&self) -> Result<Vec<QueueEntry>> {
        self.send(|reply| Job::Queue { reply }).await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn retry(&self, id: MessageId) -> Result<()> {
        self.send(|reply| Job::Retry { id, reply }).await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn next(&self) -> Result<Option<Message>> {
        self.send(|reply| Job::Next { reply }).await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn complete(&self, message: Message, delivery: DeliveryOutcome) -> Result<()> {
        self.send(|reply| Job::Complete {
            message,
            delivery,
            reply,
        })
        .await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn metrics(&self) -> Result<String> {
        self.send(|reply| Job::Metrics { reply }).await
    }
    /// # Errors
    /// Returns an error if the worker is unavailable or the operation fails.
    pub async fn prune(&self) -> Result<()> {
        self.send(|reply| Job::Prune { reply }).await
    }
}

/// Keep socket cleanup scoped to the daemon that holds the database lock.
/// # Errors
/// Returns an error if an existing socket path cannot be removed.
pub fn remove_socket(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn database_error_rolls_back_entire_batch() {
        let dir = tempfile::tempdir().unwrap();
        let config: Config = serde_json::from_value(json!({"sender":"a@example.org","recipient":"b@example.org","host":"test","database":dir.path().join("queue.db")})).unwrap();
        let store = Store::open(config).await.unwrap();
        // Force the second queue insert to fail after the first incident was written.
        store
            .conn
            .execute("CREATE UNIQUE INDEX force_error ON queue(created)", ())
            .await
            .unwrap();
        let events = ["one", "two"].map(|key| {
            Event::new(
                key.into(),
                "test".into(),
                Status::Firing,
                key.into(),
                String::new(),
            )
            .unwrap()
        });
        assert!(store.submit(events.to_vec(), 100).await.is_err());
        assert!(store.queue().await.unwrap().is_empty());
        let mut rows = store
            .conn
            .query("SELECT count(*) FROM incidents", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
    }
    fn test_config(dir: &Path) -> Config {
        serde_json::from_value(json!({"sender":"a@example.org", "recipient":"b@example.org", "host":"test", "database":dir.join("queue.db")})).unwrap()
    }

    async fn database_version(conn: &Connection) -> i64 {
        conn.query("PRAGMA user_version", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .get(0)
            .unwrap()
    }

    #[tokio::test]
    async fn schema_version_requires_a_fresh_database() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(config.clone()).await.unwrap();
        assert_eq!(database_version(&store.conn).await, 2);
        drop(store);
        let store = Store::open(config.clone()).await.unwrap();
        store
            .conn
            .execute("CREATE TABLE sentinel(value TEXT)", ())
            .await
            .unwrap();
        store
            .conn
            .execute("INSERT INTO sentinel VALUES('preserve me')", ())
            .await
            .unwrap();
        store
            .conn
            .execute("PRAGMA user_version=1", ())
            .await
            .unwrap();
        drop(store);
        let error = Store::open(config.clone()).await.err().unwrap();
        assert!(error.to_string().contains("fresh database"));
        let db = turso::Builder::new_local(config.database().to_str().unwrap())
            .build()
            .await
            .unwrap();
        let conn = db.connect().unwrap();
        assert_eq!(database_version(&conn).await, 1);
        let mut rows = conn.query("SELECT value FROM sentinel", ()).await.unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "preserve me"
        );
        drop(rows);
        for version in [0, 3] {
            conn.execute(&format!("PRAGMA user_version={version}"), ())
                .await
                .unwrap();
            assert!(Store::open(config.clone()).await.is_err());
            assert_eq!(database_version(&conn).await, version);
        }
    }

    #[tokio::test]
    async fn database_constraints_reject_invalid_inserts_and_updates() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).await.unwrap();
        store
            .conn
            .execute(
                "INSERT INTO queue(id,payload,created,due) VALUES('id','payload',0,0)",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO incidents VALUES('key','source',1,0,'summary','')",
                (),
            )
            .await
            .unwrap();
        store
            .conn
            .execute("INSERT INTO samples VALUES('key',0,0,0)", ())
            .await
            .unwrap();
        for sql in [
            "INSERT INTO queue(id,payload,created,due,state) VALUES('bad','',0,0,'unknown')",
            "INSERT INTO queue(id,payload,created,due,attempts) VALUES('bad','',0,0,-1)",
            "INSERT INTO incidents VALUES('bad','source',2,0,'summary','')",
            "INSERT INTO incidents VALUES(NULL,'source',1,0,'summary','')",
            "INSERT INTO samples VALUES('bad',0,0,-1)",
            "INSERT INTO samples VALUES(NULL,0,0,0)",
            "UPDATE queue SET state='unknown'",
            "UPDATE queue SET attempts=-1",
            "UPDATE incidents SET active=2",
            "UPDATE incidents SET key=NULL",
            "UPDATE samples SET successes=-1",
            "UPDATE samples SET key=NULL",
        ] {
            assert!(
                store.conn.execute(sql, ()).await.is_err(),
                "accepted: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn database_decoding_rejects_corrupt_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).await.unwrap();
        store
            .conn
            .execute(
                "INSERT INTO queue(id,payload,created,due) VALUES('not-a-uuid','payload',0,0)",
                (),
            )
            .await
            .unwrap();
        assert!(store.queue().await.is_err());
        assert!(store.next(0).await.is_err());
        store
            .conn
            .execute("UPDATE queue SET id=?", [MessageId::new().to_string()])
            .await
            .unwrap();
        store
            .conn
            .execute("PRAGMA ignore_check_constraints=ON", ())
            .await
            .unwrap();
        store
            .conn
            .execute("UPDATE queue SET state='corrupt'", ())
            .await
            .unwrap();
        assert!(store.queue().await.is_err());
        store
            .conn
            .execute(
                "UPDATE queue SET state=?,attempts=-1",
                [QueueState::Pending.as_str()],
            )
            .await
            .unwrap();
        assert!(store.queue().await.is_err());
        assert!(store.next(0).await.is_err());
    }

    #[tokio::test]
    async fn delivery_persistence_and_arithmetic_boundaries() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path())).await.unwrap();
        let event = Event::new(
            "key".into(),
            "manual".into(),
            Status::Firing,
            "summary".into(),
            String::new(),
        )
        .unwrap();
        store.submit(vec![event], 100).await.unwrap();
        let mut message = store.next(100).await.unwrap().unwrap();
        for attempts in [0, 1, 4, 5, i64::MAX] {
            message.attempts = Counter::try_from(attempts).unwrap();
            store
                .complete(
                    &message,
                    &DeliveryOutcome::Retry {
                        error: "network".into(),
                        after: Duration::ZERO,
                    },
                    100,
                )
                .await
                .unwrap();
            let exponent = u32::try_from(attempts.min(5)).unwrap();
            let due = 100 + (30 * 2_i64.pow(exponent)).min(900);
            assert!(store.next(due - 1).await.unwrap().is_none());
            let retry = store.next(due).await.unwrap().unwrap();
            assert_eq!(retry.attempts.get(), attempts.saturating_add(1));
        }
        store
            .complete(
                &message,
                &DeliveryOutcome::Retry {
                    error: "rate limited".into(),
                    after: Duration::MAX,
                },
                100,
            )
            .await
            .unwrap();
        assert!(store.next(i64::MAX - 1).await.unwrap().is_none());
        assert!(store.next(i64::MAX).await.unwrap().is_some());
        let row = store.queue().await.unwrap().remove(0);
        assert_eq!(row.state, QueueState::Pending);
        assert_eq!(row.error.as_deref(), Some("rate limited"));
        store
            .complete(
                &message,
                &DeliveryOutcome::Failed {
                    error: "rejected".into(),
                },
                101,
            )
            .await
            .unwrap();
        let row = store.queue().await.unwrap().remove(0);
        assert_eq!(row.state, QueueState::Failed);
        assert_eq!(row.error.as_deref(), Some("rejected"));
        store
            .complete(&message, &DeliveryOutcome::Sent, 102)
            .await
            .unwrap();
        assert!(store.queue().await.unwrap().is_empty());
        let mut rows = store
            .conn
            .query("SELECT state,error FROM queue", ())
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "sent");
        assert_eq!(row.get::<Option<String>>(1).unwrap(), None);
    }
}
