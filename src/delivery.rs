use crate::{
    config::{Config, HttpUrl},
    model::{DeliveryOutcome, Message},
    store::Handle,
};
use std::time::{Duration, UNIX_EPOCH};

/// # Errors
/// Returns an error if the HTTP client cannot be initialized.
pub fn client(timeout: Duration) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

#[derive(serde::Deserialize)]
struct Accepted {
    id: String,
}

pub async fn deliver(
    client: &reqwest::Client,
    url: &HttpUrl,
    key: &str,
    message: &Message,
    now: i64,
) -> DeliveryOutcome {
    let response = client
        .post(url.as_url().clone())
        .bearer_auth(key)
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", message.id.to_string())
        .header("User-Agent", "attosend/0.1")
        .body(message.payload.clone())
        .send()
        .await;
    let Ok(response) = response else {
        return pending("network failure or invalid response");
    };
    let status = response.status().as_u16();
    if status == 200 {
        // Bound response allocation even if the remote endpoint misbehaves.
        let mut response = response;
        let mut body = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if body.len() + chunk.len() <= 65536 => {
                    body.extend_from_slice(&chunk);
                }
                Ok(None) => break,
                _ => return pending("network failure or invalid response"),
            }
        }
        if serde_json::from_slice::<Accepted>(&body).is_ok_and(|response| !response.id.is_empty()) {
            return DeliveryOutcome::Sent;
        }
        return pending("network failure or invalid response");
    }
    let retry_after = response
        .headers()
        .get("Retry-After")
        .and_then(|v| v.to_str().ok())
        .map_or(Duration::ZERO, |value| retry_after(value, now));
    let error = format!("HTTP {status}");
    if matches!(status, 408 | 429) || status >= 500 {
        DeliveryOutcome::Retry {
            error,
            after: retry_after,
        }
    } else {
        DeliveryOutcome::Failed { error }
    }
}
fn pending(error: &str) -> DeliveryOutcome {
    DeliveryOutcome::Retry {
        error: error.into(),
        after: Duration::ZERO,
    }
}

#[must_use]
pub fn retry_after(value: &str, now: i64) -> Duration {
    if let Ok(seconds) = value.parse::<u32>() {
        return Duration::from_secs(u64::from(seconds));
    }
    let seconds = httpdate::parse_http_date(value)
        .ok()
        .and_then(|date| date.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .map_or(0, |timestamp| timestamp.saturating_sub(now).max(0));
    Duration::from_secs(seconds.unsigned_abs())
}

/// # Errors
/// Returns an error if storage or HTTP client initialization fails.
pub async fn worker(
    handle: Handle,
    config: Config,
    key: String,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let client = client(Duration::from_secs(20))?;
    let mut ticks = 0_u32;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        if ticks == 0 {
            handle.prune().await?;
        }
        ticks = (ticks + 1) % 3600;
        let message = handle.next().await?;
        if let Some(message) = message {
            let result = deliver(
                &client,
                config.resend_url(),
                &key,
                &message,
                crate::store::now(),
            )
            .await;
            if let Some(error) = result.error() {
                tracing::warn!(id = %message.id, %error, state = result.state().as_str(), "delivery unsuccessful");
            }
            handle.complete(message, result).await?;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(1)) => {},
            _ = stop.changed() => return Ok(()),
        }
    }
}
