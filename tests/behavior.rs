use attosend::{
    config::Config,
    model::{DeliveryOutcome, Event, FailedUnit, QueueEntry, Request, Sample, Status, Webhook},
    store::{Handle, Store},
};
use serde_json::json;
use std::time::Duration;

fn config(dir: &std::path::Path) -> Config {
    serde_json::from_value(json!({"sender":"alerts@example.org", "recipient":"test@example.org", "host":"test-host", "database":dir.join("queue.db"), "socket":dir.join("control.sock")})).unwrap()
}
fn event(key: &str, status: Status) -> Event {
    Event::new(
        key.into(),
        "test".into(),
        status,
        "Example failure".into(),
        "details".into(),
    )
    .unwrap()
}
async fn submit(store: &Store, key: &str, status: Status, time: i64) -> usize {
    store
        .submit(vec![event(key, status)], time)
        .await
        .unwrap()
        .queued
}
async fn queue(store: &Store) -> Vec<QueueEntry> {
    store.queue().await.unwrap()
}
async fn sample(store: &Store, failed: bool, time: i64) {
    store
        .check(
            vec![Sample::new("peer".into(), failed, "Peer down".into(), "details".into()).unwrap()],
            None,
            time,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn transitions_reminders_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let store = Store::open(cfg.clone()).await.unwrap();
    assert_eq!(submit(&store, "test", Status::Resolved, 99).await, 0);
    assert_eq!(submit(&store, "test", Status::Firing, 100).await, 1);
    assert_eq!(submit(&store, "test", Status::Firing, 100).await, 0);
    drop(store);
    let store = Store::open(cfg).await.unwrap();
    assert_eq!(submit(&store, "test", Status::Firing, 21699).await, 0);
    assert_eq!(submit(&store, "test", Status::Firing, 21700).await, 1);
    assert_eq!(submit(&store, "test", Status::Resolved, 21701).await, 1);
    assert_eq!(submit(&store, "test", Status::Resolved, 21702).await, 0);
    assert_eq!(submit(&store, "test", Status::Firing, 21703).await, 1);
    assert_eq!(queue(&store).await.len(), 4);
}

#[tokio::test]
async fn debounce_and_sampling_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(config(dir.path())).await.unwrap();
    for now in [0, 30, 60, 90] {
        sample(&store, true, now).await;
    }
    assert!(queue(&store).await.is_empty());
    sample(&store, true, 120).await;
    assert_eq!(queue(&store).await.len(), 1);
    sample(&store, false, 150).await;
    assert_eq!(queue(&store).await.len(), 1);
    sample(&store, false, 180).await;
    assert_eq!(queue(&store).await.len(), 2);
    sample(&store, true, 200).await;
    sample(&store, true, 3600).await;
    assert_eq!(queue(&store).await.len(), 2);
}

#[tokio::test]
async fn concurrent_submission_and_exclusive_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let (handle, stopped) = Handle::start(cfg.clone()).await.unwrap();
    assert!(Store::open(cfg).await.is_err());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let handle = handle.clone();
        tasks.spawn(async move {
            handle
                .submit(vec![event("test", Status::Firing)])
                .await
                .unwrap()
                .queued
        });
    }
    let mut queued = 0;
    while let Some(result) = tasks.join_next().await {
        queued += result.unwrap();
    }
    assert_eq!(queued, 1);
    drop(handle);
    stopped.await.unwrap();
}

#[tokio::test]
async fn retries_keep_identity_and_fifo_and_permanent_failures_do_not_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(config(dir.path())).await.unwrap();
    submit(&store, "test", Status::Firing, 100).await;
    submit(&store, "test", Status::Resolved, 101).await;
    let first = store.next(102).await.unwrap().unwrap();
    store
        .complete(
            &first,
            &DeliveryOutcome::Retry {
                error: "network".into(),
                after: Duration::from_secs(0),
            },
            102,
        )
        .await
        .unwrap();
    assert!(store.next(103).await.unwrap().is_none());
    let retry = store.next(132).await.unwrap().unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(retry.payload, first.payload);
    assert_eq!(retry.attempts.get(), 1);
    store
        .complete(
            &retry,
            &DeliveryOutcome::Failed {
                error: "HTTP 403".into(),
            },
            132,
        )
        .await
        .unwrap();
    let second = store.next(133).await.unwrap().unwrap();
    assert_ne!(second.id, first.id);
    store
        .complete(&second, &DeliveryOutcome::Sent, 133)
        .await
        .unwrap();
    assert!(
        store
            .metrics(134)
            .await
            .unwrap()
            .contains("notifications_failed 1")
    );
    store.retry(first.id).await.unwrap();
    assert_eq!(
        store.next(134).await.unwrap().unwrap().payload,
        first.payload
    );
    assert!(store.retry(second.id).await.is_err());
}

#[tokio::test]
async fn retry_after_and_retention() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(config(dir.path())).await.unwrap();
    submit(&store, "test", Status::Firing, 100).await;
    let message = store.next(100).await.unwrap().unwrap();
    store
        .complete(
            &message,
            &DeliveryOutcome::Retry {
                error: "HTTP 429".into(),
                after: Duration::from_mins(20),
            },
            100,
        )
        .await
        .unwrap();
    assert!(store.next(1299).await.unwrap().is_none());
    let message = store.next(1300).await.unwrap().unwrap();
    store
        .complete(&message, &DeliveryOutcome::Sent, 1300)
        .await
        .unwrap();
    submit(&store, "test", Status::Resolved, 1301).await;
    let message = store.next(1301).await.unwrap().unwrap();
    store
        .complete(&message, &DeliveryOutcome::Sent, 1301)
        .await
        .unwrap();
    store.prune(700_000).await.unwrap();
    assert_eq!(submit(&store, "test", Status::Resolved, 700_000).await, 0);
    assert!(queue(&store).await.is_empty());
    assert_eq!(
        attosend::delivery::retry_after("1200", 100),
        Duration::from_mins(20)
    );
    assert_eq!(
        attosend::delivery::retry_after("Thu, 01 Jan 1970 00:21:40 GMT", 100),
        Duration::from_mins(20)
    );
    assert_eq!(
        attosend::delivery::retry_after("garbage", 100),
        Duration::ZERO
    );
}

#[tokio::test]
async fn malformed_batch_is_atomic_and_systemd_recovery_requires_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(config(dir.path())).await.unwrap();
    let mut bad = serde_json::to_value(event("bad", Status::Firing)).unwrap();
    bad["summary"] = json!("");
    assert!(
        serde_json::from_value::<Request>(
            json!({"command":"submit", "events":[event("valid", Status::Firing), bad]})
        )
        .is_err()
    );
    assert!(queue(&store).await.is_empty());
    let unit = FailedUnit::try_from(
        Event::new(
            "systemd:example.service".into(),
            "systemd".into(),
            Status::Firing,
            "Unit failed".into(),
            String::new(),
        )
        .unwrap(),
    )
    .unwrap();
    store.check(vec![], Some(vec![unit]), 100).await.unwrap();
    store.check(vec![], None, 101).await.unwrap();
    assert_eq!(queue(&store).await.len(), 1);
    store.check(vec![], Some(vec![]), 102).await.unwrap();
    assert_eq!(queue(&store).await.len(), 2);
    assert!(attosend::checks::is_handler(
        "attosend-failure@foo.service.service"
    ));
}

#[tokio::test]
async fn alertmanager_retries_and_new_occurrences() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(config(dir.path())).await.unwrap();
    let mut body = json!({"alerts":[{"fingerprint":"abc","startsAt":"2026-01-01T00:00:00Z","status":"firing","labels":{"alertname":"Test"},"annotations":{"summary":"test alert"}}]});
    for _ in 0..2 {
        let events = serde_json::from_value::<Webhook>(body.clone())
            .unwrap()
            .events()
            .unwrap();
        store.submit(events, 100).await.unwrap();
    }
    assert_eq!(queue(&store).await.len(), 1);
    body["alerts"][0]["status"] = json!("resolved");
    let events = serde_json::from_value::<Webhook>(body.clone())
        .unwrap()
        .events()
        .unwrap();
    store.submit(events, 101).await.unwrap();
    body["alerts"][0]["status"] = json!("firing");
    body["alerts"][0]["startsAt"] = json!("2026-01-02T00:00:00Z");
    let events = serde_json::from_value::<Webhook>(body)
        .unwrap()
        .events()
        .unwrap();
    store.submit(events, 102).await.unwrap();
    assert_eq!(queue(&store).await.len(), 3);
}
