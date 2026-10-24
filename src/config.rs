use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawConfig {
    sender: String,
    recipient: String,
    host: String,
    #[serde(default = "database")]
    database: PathBuf,
    #[serde(default = "socket")]
    socket: PathBuf,
    #[serde(default = "port")]
    port: u16,
    #[serde(default = "reminders")]
    reminder_seconds: i64,
    #[serde(default = "resend_url")]
    resend_url: String,
    #[serde(default = "log_level")]
    log_level: String,
    #[serde(default)]
    checks: RawChecks,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawChecks {
    #[serde(default)]
    peer: Option<String>,
    #[serde(default)]
    readiness: BTreeMap<String, String>,
    #[serde(default = "failure_seconds")]
    failure_seconds: i64,
    #[serde(default = "yes")]
    systemd_failures: bool,
}

impl Default for RawChecks {
    fn default() -> Self {
        Self {
            peer: None,
            readiness: BTreeMap::new(),
            failure_seconds: failure_seconds(),
            systemd_failures: true,
        }
    }
}
fn database() -> PathBuf {
    "/var/lib/attosend/queue.db".into()
}
fn socket() -> PathBuf {
    "/run/attosend/control.sock".into()
}
fn port() -> u16 {
    9169
}
fn reminders() -> i64 {
    21600
}
fn failure_seconds() -> i64 {
    120
}
fn yes() -> bool {
    true
}
fn resend_url() -> String {
    "https://api.resend.com/emails".into()
}
fn log_level() -> String {
    "info".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HttpUrl {
    original: String,
    parsed: reqwest::Url,
}
impl TryFrom<String> for HttpUrl {
    type Error = anyhow::Error;
    fn try_from(original: String) -> Result<Self> {
        let parsed = reqwest::Url::parse(&original)?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some(),
            "expected HTTP(S) URL"
        );
        ensure!(
            parsed.username().is_empty() && parsed.password().is_none(),
            "URL credentials are not supported"
        );
        Ok(Self { original, parsed })
    }
}
impl From<HttpUrl> for String {
    fn from(url: HttpUrl) -> Self {
        url.original
    }
}
impl HttpUrl {
    #[must_use]
    pub fn as_url(&self) -> &reqwest::Url {
        &self.parsed
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.original
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct PositiveSeconds(std::num::NonZeroU64);
impl TryFrom<i64> for PositiveSeconds {
    type Error = anyhow::Error;
    fn try_from(value: i64) -> Result<Self> {
        let value = u64::try_from(value).context("durations must be positive")?;
        Ok(Self(
            std::num::NonZeroU64::new(value).context("durations must be positive")?,
        ))
    }
}
impl From<PositiveSeconds> for i64 {
    fn from(value: PositiveSeconds) -> Self {
        // Construction restricts values to positive i64 seconds.
        i64::try_from(value.0.get()).expect("validated seconds fit i64")
    }
}
impl PositiveSeconds {
    #[must_use]
    pub fn get(self) -> i64 {
        self.into()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawConfig")]
pub struct Config {
    sender: String,
    recipient: String,
    host: String,
    database: PathBuf,
    socket: PathBuf,
    port: std::num::NonZeroU16,
    reminder_seconds: PositiveSeconds,
    resend_url: HttpUrl,
    log_level: String,
    checks: Checks,
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Checks {
    peer: Option<String>,
    readiness: BTreeMap<String, HttpUrl>,
    failure_seconds: PositiveSeconds,
    systemd_failures: bool,
}
impl TryFrom<RawConfig> for Config {
    type Error = anyhow::Error;
    fn try_from(raw: RawConfig) -> Result<Self> {
        for value in [&raw.sender, &raw.recipient, &raw.host] {
            ensure!(
                !value.trim().is_empty() && value.len() <= 1024,
                "sender, recipient and host must be nonempty and at most 1024 bytes"
            );
        }
        ensure!(
            raw.database.is_absolute() && raw.socket.is_absolute(),
            "database and socket paths must be absolute"
        );
        if let Some(peer) = &raw.checks.peer {
            ensure!(
                !peer.is_empty()
                    && !peer.starts_with('-')
                    && !peer.chars().any(char::is_whitespace),
                "invalid peer"
            );
        }
        tracing_subscriber::EnvFilter::try_new(&raw.log_level).context("invalid log level")?;
        Ok(Self {
            sender: raw.sender,
            recipient: raw.recipient,
            host: raw.host,
            database: raw.database,
            socket: raw.socket,
            port: std::num::NonZeroU16::new(raw.port).context("port must be positive")?,
            reminder_seconds: raw.reminder_seconds.try_into()?,
            resend_url: raw.resend_url.try_into()?,
            log_level: raw.log_level,
            checks: Checks {
                peer: raw.checks.peer,
                readiness: raw
                    .checks
                    .readiness
                    .into_iter()
                    .map(|(name, url)| Ok((name, HttpUrl::try_from(url)?)))
                    .collect::<Result<_>>()?,
                failure_seconds: raw.checks.failure_seconds.try_into()?,
                systemd_failures: raw.checks.systemd_failures,
            },
        })
    }
}
impl Config {
    /// Read and validate the non-secret configuration.
    /// # Errors
    /// Returns an error for unreadable, malformed, or invalid configuration.
    pub fn read(path: &std::path::Path) -> Result<Self> {
        Ok(serde_json::from_slice(
            &std::fs::read(path).context("read config")?,
        )?)
    }
    #[must_use]
    pub fn sender(&self) -> &str {
        &self.sender
    }
    #[must_use]
    pub fn recipient(&self) -> &str {
        &self.recipient
    }
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }
    #[must_use]
    pub fn database(&self) -> &std::path::Path {
        &self.database
    }
    #[must_use]
    pub fn socket(&self) -> &std::path::Path {
        &self.socket
    }
    #[must_use]
    pub fn port(&self) -> std::num::NonZeroU16 {
        self.port
    }
    #[must_use]
    pub fn reminder_seconds(&self) -> PositiveSeconds {
        self.reminder_seconds
    }
    #[must_use]
    pub fn resend_url(&self) -> &HttpUrl {
        &self.resend_url
    }
    #[must_use]
    pub fn log_level(&self) -> &str {
        &self.log_level
    }
    #[must_use]
    pub fn checks(&self) -> &Checks {
        &self.checks
    }
}
impl Checks {
    #[must_use]
    pub fn peer(&self) -> Option<&str> {
        self.peer.as_deref()
    }
    #[must_use]
    pub fn readiness(&self) -> &BTreeMap<String, HttpUrl> {
        &self.readiness
    }
    #[must_use]
    pub fn failure_seconds(&self) -> PositiveSeconds {
        self.failure_seconds
    }
    #[must_use]
    pub fn systemd_failures(&self) -> bool {
        self.systemd_failures
    }
}
