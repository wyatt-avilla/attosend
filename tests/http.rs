use attosend::{
    config::{Config, HttpUrl},
    model::{Counter, DeliveryOutcome, Event, Message, MessageId, Request, Response, Status},
};
use axum::{
    Router,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpListener;

fn event() -> Event {
    Event::new(
        "test".into(),
        "manual".into(),
        Status::Firing,
        "test message".into(),
        String::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn delivery_protocol_and_error_classification() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let copy = captured.clone();
    let app = Router::new()
        .route(
            "/ok",
            post(move |headers: HeaderMap, body: String| {
                let copy = copy.clone();
                async move {
                    copy.lock().unwrap().push((headers, body));
                    axum::Json(json!({"id":"delivered"}))
                }
            }),
        )
        .route(
            "/rate",
            post(|| async { (StatusCode::TOO_MANY_REQUESTS, [("Retry-After", "1200")]) }),
        )
        .route("/reject", post(|| async { StatusCode::FORBIDDEN }))
        .route(
            "/server",
            post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        )
        .route("/invalid", post(|| async { "{}" }))
        .route(
            "/slow",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                "{}"
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = attosend::delivery::client(Duration::from_millis(100)).unwrap();
    let message = Message {
        id: MessageId::new(),
        payload: json!({"text":"immutable"}).to_string(),
        attempts: Counter::ZERO,
    };
    for _ in 0..2 {
        assert_eq!(
            attosend::delivery::deliver(
                &client,
                &HttpUrl::try_from(format!("http://{address}/ok")).unwrap(),
                "test-key",
                &message,
                100
            )
            .await
            .state()
            .as_str(),
            "sent"
        );
    }
    {
        let requests = captured.lock().unwrap();
        for (headers, body) in requests.iter() {
            assert_eq!(headers["authorization"], "Bearer test-key");
            assert_eq!(headers["idempotency-key"], message.id.to_string());
            assert_eq!(body, &message.payload);
        }
    }
    for (path, state) in [
        ("rate", "pending"),
        ("reject", "failed"),
        ("server", "pending"),
        ("invalid", "pending"),
        ("slow", "pending"),
    ] {
        let result = attosend::delivery::deliver(
            &client,
            &HttpUrl::try_from(format!("http://{address}/{path}")).unwrap(),
            "test-key",
            &message,
            100,
        )
        .await;
        assert_eq!(result.state().as_str(), state);
        if path == "rate" {
            assert_eq!(
                match result {
                    DeliveryOutcome::Retry { after, .. } => after,
                    _ => panic!("expected retry"),
                },
                Duration::from_mins(20)
            );
        }
    }
    task.abort();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end daemon lifecycle scenario"
)]
async fn daemon_http_socket_crash_recovery_and_missing_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reserved.local_addr().unwrap().port();
    let cfg: Config = serde_json::from_value(json!({"sender":"alerts@example.org","recipient":"test@example.org","host":"test","database":dir.path().join("queue.db"),"socket":dir.path().join("control.sock"),"port":port,"resendUrl":"http://127.0.0.1:1/emails"})).unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
    std::fs::write(dir.path().join("resend-api-key"), "test-only-key").unwrap();
    assert!(
        attosend::server::request(&cfg, &Request::Queue)
            .await
            .is_err()
    );
    drop(reserved);
    let spawn = || {
        tokio::process::Command::new(env!("CARGO_BIN_EXE_attosend"))
            .arg("--config")
            .arg(&path)
            .arg("serve")
            .env("CREDENTIALS_DIRECTORY", dir.path())
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    };
    let mut child = spawn();
    wait_ready(&cfg, &mut child).await;
    verify_control_validation(&cfg).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for body in [json!({}), json!({"alerts":[]}), json!({"alerts":[{}]})] {
        assert_eq!(
            client
                .post(format!("{base}/alertmanager"))
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(
        client
            .post(format!("{base}/alertmanager"))
            .header("Content-Type", "application/json")
            .body("x".repeat(1024 * 1024 + 1))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/commands"))
            .json(&Request::Queue)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let alert = json!({"fingerprint":"abc","startsAt":"2026-01-01T00:00:00Z","status":"firing","labels":{"alertname":"Test"}});
    let invalid = json!({"alerts":[alert.clone(),{}]});
    assert_eq!(
        client
            .post(format!("{base}/alertmanager"))
            .json(&invalid)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(
        matches!(attosend::server::request(&cfg, &Request::Queue).await.unwrap(), Response::Queue(rows) if rows.is_empty())
    );
    assert_eq!(
        client
            .post(format!("{base}/alertmanager"))
            .json(&json!({"alerts":[alert]}))
            .send()
            .await
            .unwrap()
            .status(),
        202
    );
    attosend::server::request(
        &cfg,
        &Request::Submit {
            events: vec![event()],
        },
    )
    .await
    .unwrap();
    let Response::Queue(rows) = attosend::server::request(&cfg, &Request::Queue)
        .await
        .unwrap()
    else {
        panic!("expected queue");
    };
    let ids: Vec<_> = rows.iter().map(|r| r.id).collect();
    assert_eq!(ids.len(), 2);
    let mut second = spawn();
    assert!(
        !tokio::time::timeout(Duration::from_secs(10), second.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(
        attosend::server::request(&cfg, &Request::Queue)
            .await
            .is_ok()
    );
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    let mut child = spawn();
    wait_ready(&cfg, &mut child).await;
    let Response::Queue(rows) = attosend::server::request(&cfg, &Request::Queue)
        .await
        .unwrap()
    else {
        panic!("expected queue");
    };
    assert_eq!(rows.iter().map(|r| r.id).collect::<Vec<_>>(), ids);
    let metrics = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("notifications_pending 2"));
    child.kill().await.unwrap();
    child.wait().await.unwrap();
}

async fn wait_ready(config: &Config, child: &mut tokio::process::Child) {
    for _ in 0..200 {
        assert!(
            child.try_wait().unwrap().is_none(),
            "daemon exited before ready"
        );
        if attosend::server::request(config, &Request::Queue)
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("daemon did not become ready");
}

async fn verify_control_validation(config: &Config) {
    let client = reqwest::Client::builder()
        .unix_socket(config.socket())
        .no_proxy()
        .build()
        .unwrap();
    let mut invalid = serde_json::to_value(event()).unwrap();
    invalid["summary"] = json!("");
    for body in [
        json!({"command":"submit", "events":[event(), invalid]}),
        json!({"command":"retry", "id":"invalid"}),
        json!({"command":"check", "samples":[], "failed_units":[event()]}),
    ] {
        assert_eq!(
            client
                .post("http://localhost/v1/commands")
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    let mut resolved = serde_json::to_value(event()).unwrap();
    resolved["status"] = json!("resolved");
    for (body, expected) in [
        (json!({"command":"queue"}), json!([])),
        (
            json!({"command":"submit", "events":[resolved]}),
            json!({"queued":0}),
        ),
        (
            json!({"command":"check", "samples":[], "failed_units":null}),
            json!({"ok":true}),
        ),
    ] {
        let response = client
            .post("http://localhost/v1/commands")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            expected
        );
    }
}
