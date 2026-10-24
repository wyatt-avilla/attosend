use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

#[derive(Clone, Copy, Debug, Deserialize, Serialize, clap::ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Firing,
    Resolved,
}
impl Status {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Firing => "firing",
            Self::Resolved => "resolved",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MessageId(uuid::Uuid);
impl MessageId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}
impl FromStr for MessageId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        uuid::Uuid::parse_str(value)
            .map(Self)
            .map_err(|_| Error::Invalid("invalid message ID".into()))
    }
}
impl TryFrom<String> for MessageId {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}
impl From<MessageId> for String {
    fn from(value: MessageId) -> Self {
        value.to_string()
    }
}
impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IncidentKey(String);
impl TryFrom<String> for IncidentKey {
    type Error = Error;
    fn try_from(value: String) -> Result<Self> {
        bounded(&value, 1024, true)?;
        Ok(Self(value))
    }
}
impl From<IncidentKey> for String {
    fn from(value: IncidentKey) -> Self {
        value.0
    }
}
impl IncidentKey {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for IncidentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
fn bounded(value: &str, limit: usize, required: bool) -> Result<()> {
    if (required && value.is_empty()) || value.chars().count() > limit {
        return Err(Error::Invalid(
            "missing or oversized notification fields".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "RawEvent")]
pub struct Event {
    key: IncidentKey,
    source: String,
    status: Status,
    summary: String,
    details: String,
}
#[derive(Deserialize)]
struct RawEvent {
    key: String,
    source: String,
    status: Status,
    summary: String,
    #[serde(default)]
    details: String,
}
impl TryFrom<RawEvent> for Event {
    type Error = Error;
    fn try_from(raw: RawEvent) -> Result<Self> {
        Self::new(raw.key, raw.source, raw.status, raw.summary, raw.details)
    }
}
impl Event {
    /// # Errors
    /// Returns an error for missing or oversized notification fields.
    pub fn new(
        key: String,
        source: String,
        status: Status,
        summary: String,
        details: String,
    ) -> Result<Self> {
        let key = IncidentKey::try_from(key)?;
        bounded(&source, 1024, false)?;
        bounded(&summary, 1024, true)?;
        bounded(&details, 32768, false)?;
        Ok(Self {
            key,
            source,
            status,
            summary,
            details,
        })
    }
    #[must_use]
    pub fn key(&self) -> &IncidentKey {
        &self.key
    }
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
    #[must_use]
    pub const fn status(&self) -> Status {
        self.status
    }
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }
    #[must_use]
    pub fn details(&self) -> &str {
        &self.details
    }
}

#[derive(Debug, Deserialize)]
pub struct Webhook {
    pub alerts: Vec<Alert>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alert {
    fingerprint: String,
    starts_at: String,
    status: Status,
    labels: BTreeMap<String, String>,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}
impl Webhook {
    /// # Errors
    /// Returns an error if any alert is incomplete or invalid; no partial batch is returned.
    pub fn events(self) -> Result<Vec<Event>> {
        if self.alerts.is_empty() {
            return Err(Error::Invalid("expected nonempty alerts".into()));
        }
        self.alerts.into_iter().map(|alert| {
            if alert.fingerprint.is_empty() || alert.starts_at.is_empty() {
                return Err(Error::Invalid("missing fingerprint or startsAt".into()));
            }
            let summary = alert.annotations.get("summary").or_else(|| alert.labels.get("alertname"))
                .ok_or_else(|| Error::Invalid("missing alert summary".into()))?.clone();
            let event = Event::try_from(RawEvent {
                key: format!("prometheus:{}:{}", alert.fingerprint, alert.starts_at),
                source: "prometheus".into(), status: alert.status, summary,
                details: serde_json::json!({"labels": alert.labels, "annotations": alert.annotations}).to_string(),
            })?;
            Ok(event)
        }).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "RawSample")]
pub struct Sample {
    key: IncidentKey,
    failed: bool,
    summary: String,
    details: String,
}
#[derive(Deserialize)]
struct RawSample {
    key: String,
    failed: bool,
    summary: String,
    details: String,
}
impl TryFrom<RawSample> for Sample {
    type Error = Error;
    fn try_from(raw: RawSample) -> Result<Self> {
        Self::new(raw.key, raw.failed, raw.summary, raw.details)
    }
}
impl Sample {
    /// # Errors
    /// Returns an error for missing or oversized observation fields.
    pub fn new(key: String, failed: bool, summary: String, details: String) -> Result<Self> {
        let key = IncidentKey::try_from(key)?;
        bounded(&summary, 1024, true)?;
        bounded(&details, 32768, false)?;
        Ok(Self {
            key,
            failed,
            summary,
            details,
        })
    }
    #[must_use]
    pub fn key(&self) -> &IncidentKey {
        &self.key
    }
    #[must_use]
    pub const fn failed(&self) -> bool {
        self.failed
    }
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }
    #[must_use]
    pub fn details(&self) -> &str {
        &self.details
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "Event", into = "Event")]
pub struct FailedUnit(Event);
impl TryFrom<Event> for FailedUnit {
    type Error = Error;
    fn try_from(event: Event) -> Result<Self> {
        if event.source() != "systemd"
            || !event.key().as_str().starts_with("systemd:")
            || event.status() != Status::Firing
        {
            return Err(Error::Invalid("invalid failed-unit observation".into()));
        }
        Ok(Self(event))
    }
}
impl From<FailedUnit> for Event {
    fn from(unit: FailedUnit) -> Self {
        unit.0
    }
}
impl FailedUnit {
    #[must_use]
    pub const fn event(&self) -> &Event {
        &self.0
    }
}

/// The private socket protocol; database operations are never exposed over TCP.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Request {
    Submit {
        events: Vec<Event>,
    },
    Check {
        samples: Vec<Sample>,
        failed_units: Option<Vec<FailedUnit>>,
    },
    Queue,
    Retry {
        id: MessageId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct Counter(i64);
impl TryFrom<i64> for Counter {
    type Error = Error;
    fn try_from(value: i64) -> Result<Self> {
        if value < 0 {
            return Err(Error::Invalid("negative counter".into()));
        }
        Ok(Self(value))
    }
}
impl From<Counter> for i64 {
    fn from(value: Counter) -> Self {
        value.0
    }
}
impl Counter {
    pub const ZERO: Self = Self(0);
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
    #[must_use]
    pub const fn increment(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum QueueState {
    Pending,
    Sent,
    Failed,
}
impl QueueState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sent => "sent",
            Self::Failed => "failed",
        }
    }
}
impl FromStr for QueueState {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "sent" => Ok(Self::Sent),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::Invalid("unknown queue state".into())),
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QueueEntry {
    pub id: MessageId,
    pub created: i64,
    pub state: QueueState,
    pub attempts: Counter,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub id: MessageId,
    pub payload: String,
    pub attempts: Counter,
}

#[derive(Debug)]
pub enum DeliveryOutcome {
    Sent,
    Retry { error: String, after: Duration },
    Failed { error: String },
}
impl DeliveryOutcome {
    #[must_use]
    pub const fn state(&self) -> QueueState {
        match self {
            Self::Sent => QueueState::Sent,
            Self::Retry { .. } => QueueState::Pending,
            Self::Failed { .. } => QueueState::Failed,
        }
    }
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Sent => None,
            Self::Retry { error, .. } | Self::Failed { error } => Some(error),
        }
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitResult {
    pub queued: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(try_from = "RawOk")]
pub struct OkResponse {
    ok: bool,
}
#[derive(Deserialize)]
struct RawOk {
    ok: bool,
}
impl TryFrom<RawOk> for OkResponse {
    type Error = Error;
    fn try_from(raw: RawOk) -> Result<Self> {
        if !raw.ok {
            return Err(Error::Invalid("invalid success response".into()));
        }
        Ok(Self::new())
    }
}
impl OkResponse {
    #[must_use]
    pub const fn new() -> Self {
        Self { ok: true }
    }
}
impl Default for OkResponse {
    fn default() -> Self {
        Self::new()
    }
}
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Response {
    Submit(SubmitResult),
    Ok(OkResponse),
    Queue(Vec<QueueEntry>),
}
