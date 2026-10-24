use crate::{
    Error,
    config::Config,
    model::{OkResponse, Request, Response as CommandResponse, Webhook},
    store::Handle,
};
use anyhow::{Context, Result, bail, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::watch,
};

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::Storage(_) | Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, Json(json!({"error": self.to_string()}))).into_response()
    }
}
fn json_error(error: &JsonRejection) -> Response {
    let status = if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::BAD_REQUEST
    };
    (status, Json(json!({"error": "invalid JSON request"}))).into_response()
}

async fn webhook(
    State(handle): State<Handle>,
    body: std::result::Result<Json<Webhook>, JsonRejection>,
) -> Response {
    let webhook = match body {
        Ok(Json(body)) => body,
        Err(error) => return json_error(&error),
    };
    let events = match webhook.events() {
        Ok(events) => events,
        Err(error) => return error.into_response(),
    };
    match handle.submit(events).await {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(error) => error.into_response(),
    }
}
async fn metrics(State(handle): State<Handle>) -> Response {
    match handle.metrics().await {
        Ok(metrics) => (
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            metrics,
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}
async fn control(
    State(handle): State<Handle>,
    body: std::result::Result<Json<Request>, JsonRejection>,
) -> Response {
    let request = match body {
        Ok(Json(request)) => request,
        Err(error) => return json_error(&error),
    };
    let result = match request {
        Request::Submit { events } => handle.submit(events).await.map(CommandResponse::Submit),
        Request::Check {
            samples,
            failed_units,
        } => handle
            .check(samples, failed_units)
            .await
            .map(|()| CommandResponse::Ok(OkResponse::new())),
        Request::Queue => handle.queue().await.map(CommandResponse::Queue),
        Request::Retry { id } => handle
            .retry(id)
            .await
            .map(|()| CommandResponse::Ok(OkResponse::new())),
    };
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    }
}

/// # Errors
/// Returns an error if validation, socket communication, or the daemon operation fails.
pub async fn request(config: &Config, request: &Request) -> Result<CommandResponse> {
    let client = reqwest::Client::builder()
        .unix_socket(config.socket().to_path_buf())
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let response = client
        .post("http://localhost/v1/commands")
        .json(request)
        .send()
        .await
        .context(
            "attosend daemon unavailable; start attosend.service before submitting notifications",
        )?;
    if !response.status().is_success() {
        #[derive(serde::Deserialize)]
        struct Failure {
            error: String,
        }
        let failure: Failure = response
            .json()
            .await
            .context("invalid daemon error response")?;
        bail!("{}", failure.error);
    }
    match request {
        Request::Submit { .. } => Ok(CommandResponse::Submit(
            response.json().await.context("invalid submit response")?,
        )),
        Request::Check { .. } | Request::Retry { .. } => Ok(CommandResponse::Ok(
            response.json().await.context("invalid success response")?,
        )),
        Request::Queue => Ok(CommandResponse::Queue(
            response.json().await.context("invalid queue response")?,
        )),
    }
}

async fn stopping(mut stop: watch::Receiver<bool>) {
    if !*stop.borrow() {
        let _ = stop.changed().await;
    }
}

/// # Errors
/// Returns an error for invalid credentials, unavailable listeners or storage, or a failed worker.
pub async fn serve(config: Config) -> Result<()> {
    let directory = std::env::var_os("CREDENTIALS_DIRECTORY")
        .context("CREDENTIALS_DIRECTORY is required for serve")?;
    let key = std::fs::read_to_string(std::path::Path::new(&directory).join("resend-api-key"))
        .context("read Resend credential")?;
    let key = key.trim().to_owned();
    ensure!(!key.is_empty(), "empty Resend credential");
    let (handle, mut storage_exit) = Handle::start(config.clone()).await?;
    let tcp = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, config.port().get())).await?;
    std::fs::create_dir_all(config.socket().parent().context("socket directory")?)?;
    if let Ok(metadata) = std::fs::symlink_metadata(config.socket()) {
        ensure!(
            metadata.file_type().is_socket(),
            "refusing to replace non-socket control path"
        );
        ensure!(
            tokio::net::UnixStream::connect(config.socket())
                .await
                .is_err(),
            "control socket already in use"
        );
        crate::store::remove_socket(config.socket())?;
    }
    let unix = UnixListener::bind(config.socket())?;
    std::fs::set_permissions(config.socket(), std::fs::Permissions::from_mode(0o660))?;
    let _cleanup = SocketCleanup(config.socket().to_path_buf());
    let http_app = Router::new()
        .route("/alertmanager", post(webhook))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(tower_http::timeout::RequestBodyTimeoutLayer::new(
            Duration::from_secs(10),
        ))
        .with_state(handle.clone());
    let control_app = Router::new()
        .route("/v1/commands", post(control))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .layer(tower_http::timeout::RequestBodyTimeoutLayer::new(
            Duration::from_secs(10),
        ))
        .with_state(handle.clone());
    let (stop_tx, stop_rx) = watch::channel(false);
    let tcp_stop = stop_rx.clone();
    let unix_stop = stop_rx.clone();
    let mut tcp_task = tokio::spawn(async move {
        axum::serve(tcp, http_app)
            .with_graceful_shutdown(stopping(tcp_stop))
            .await
    });
    let mut unix_task = tokio::spawn(async move {
        axum::serve(unix, control_app)
            .with_graceful_shutdown(stopping(unix_stop))
            .await
    });
    let mut delivery_task = tokio::spawn(crate::delivery::worker(
        handle.clone(),
        config,
        key,
        stop_rx,
    ));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tracing::info!("attosend ready");
    let result: Result<()> = tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("signal handler"),
        _ = terminate.recv() => Ok(()),
        _ = &mut storage_exit => Err(anyhow::anyhow!("storage worker stopped")),
        result = &mut tcp_task => Err(anyhow::anyhow!("HTTP server stopped: {result:?}")),
        result = &mut unix_task => Err(anyhow::anyhow!("control server stopped: {result:?}")),
        result = &mut delivery_task => Err(anyhow::anyhow!("delivery worker stopped: {result:?}")),
    };
    let _ = stop_tx.send(true);
    // Finish an in-flight delivery and commit its result before closing the database.
    let drained = tokio::time::timeout(Duration::from_secs(25), async {
        if !tcp_task.is_finished() {
            let _ = (&mut tcp_task).await;
        }
        if !unix_task.is_finished() {
            let _ = (&mut unix_task).await;
        }
        if !delivery_task.is_finished() {
            let _ = (&mut delivery_task).await;
        }
    })
    .await;
    tcp_task.abort();
    unix_task.abort();
    delivery_task.abort();
    drop(handle);
    if drained.is_err() {
        bail!("shutdown timed out; pending deliveries will be retried");
    }
    if result.is_ok() {
        storage_exit.await.context("storage worker shutdown")?;
    }
    result
}
struct SocketCleanup(std::path::PathBuf);
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = crate::store::remove_socket(&self.0);
    }
}
