use crate::{
    config::Config,
    model::{Event, FailedUnit, Request, Sample, Status},
};
use anyhow::{Context, Result, bail};
use std::{collections::HashSet, process::Output, time::Duration};
use tokio::process::Command;

#[must_use]
pub fn is_handler(unit: &str) -> bool {
    unit.starts_with("attosend-failure@") || unit.starts_with("notification-failure@")
}

async fn command(program: &str, args: &[&str], seconds: u64) -> Result<Output> {
    tokio::time::timeout(
        Duration::from_secs(seconds),
        Command::new(program).args(args).kill_on_drop(true).output(),
    )
    .await
    .context("host command timed out")?
    .context("start host command")
}

/// # Errors
/// Returns an error if the resulting incident exceeds notification field limits.
pub async fn unit_failure(unit: &str) -> Result<Option<Event>> {
    if is_handler(unit) {
        return Ok(None);
    }
    let result = command(
        "systemctl",
        &[
            "show",
            "--property=Id,ActiveState,Result,ExecMainCode,ExecMainStatus",
            "--",
            unit,
        ],
        10,
    )
    .await;
    // Even if the unit disappears before inspection, preserve its failure notification.
    let details = match result {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).into_owned()
        }
        _ => "Unit details unavailable.".into(),
    };
    let event = Event::new(
        format!("systemd:{unit}"),
        "systemd".into(),
        Status::Firing,
        format!("Unit failed: {unit}"),
        format!("{details}\nInspect: systemctl status -- {unit}\nJournal: journalctl -u {unit}"),
    )?;
    Ok(Some(event))
}

/// # Errors
/// Returns an error if observations cannot be submitted or systemd cannot be queried.
pub async fn check(config: &Config) -> Result<()> {
    let mut samples = Vec::new();
    if let Some(peer) = config.checks().peer() {
        let result = command("ping", &["-n", "-c", "1", "-W", "3", "--", peer], 5).await;
        samples.push(Sample::new(
            format!("peer:{peer}"),
            !result.is_ok_and(|output| output.status.success()),
            "WireGuard peer unreachable".into(),
            format!("Peer: {peer}"),
        )?);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;
    let mut tasks = tokio::task::JoinSet::new();
    for (name, url) in config.checks().readiness() {
        let (name, url, client) = (name.clone(), url.clone(), client.clone());
        tasks.spawn(async move {
            let healthy = client
                .get(url.as_url().clone())
                .send()
                .await
                .is_ok_and(|response| response.status() == 200);
            Sample::new(
                format!("readiness:{name}"),
                !healthy,
                format!("{name} is not ready"),
                url.as_str().to_owned(),
            )
        });
    }
    while let Some(sample) = tasks.join_next().await {
        samples.push(sample??);
    }

    let mut listing_failed = false;
    let failed_units = if config.checks().systemd_failures() {
        match failed_units().await {
            Ok(events) => Some(events),
            Err(error) => {
                tracing::error!(%error, "cannot list failed units; preserving active incidents");
                listing_failed = true;
                None
            }
        }
    } else {
        None
    };
    crate::server::request(
        config,
        &Request::Check {
            samples,
            failed_units,
        },
    )
    .await?;
    if listing_failed {
        bail!("systemd failed-unit listing failed");
    }
    Ok(())
}

async fn failed_units() -> Result<Vec<FailedUnit>> {
    #[derive(serde::Deserialize)]
    struct Unit {
        unit: String,
    }

    let output = command(
        "systemctl",
        &[
            "list-units",
            "--all",
            "--state=failed",
            "--output=json",
            "--no-pager",
        ],
        15,
    )
    .await?;
    if !output.status.success() {
        bail!("systemctl list-units failed");
    }
    let units: Vec<Unit> = serde_json::from_slice(&output.stdout)?;
    let mut seen = HashSet::new();
    let mut events = Vec::new();
    for unit in units {
        if seen.insert(unit.unit.clone())
            && let Some(event) = unit_failure(&unit.unit).await?
        {
            events.push(FailedUnit::try_from(event)?);
        }
    }
    Ok(events)
}
