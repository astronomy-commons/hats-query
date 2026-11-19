mod logging;
mod reposnses;
mod routes;

use crate::routes::app_router;
use anyhow::Context as _;
use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse as _, Response};
use chrono::{DateTime, Utc};
use clap::Parser;
use hats_query_core::Engine;
use hats_query_core::configuration::Configuration;
use std::any::Any;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// TOML configuration file
    #[arg(short, long, env = "HATS_QUERY_CONFIG")]
    config: PathBuf,
}

/// Application-wide values shared by the request handlers.
#[derive(Clone, axum::extract::FromRef)]
struct AppState {
    /// Server configuration.
    config: Arc<Configuration>,
    /// Query engine.
    engine: Arc<Engine>,
    /// Server start time.
    up_since: DateTime<Utc>,
}

impl AppState {
    fn new(config: Configuration) -> anyhow::Result<Self> {
        let engine = Engine::new(config.mounts.clone(), config.tap.tables.clone())
            .context("cannot create the query engine")?;
        Ok(Self {
            config: Arc::new(config),
            engine: Arc::new(engine),
            up_since: Utc::now(),
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Configuration::load(&cli.config)
        .with_context(|| format!("cannot load config {}", cli.config.display()))?;
    logging::init(config.log.level);
    let address = SocketAddr::new(config.server.address, config.server.port);
    let state = AppState::new(config)?;
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("cannot listen on {address}"))?;
    tracing::info!("listening on {address}");
    axum::serve(listener, app(app_router().await, state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server failed")?;
    Ok(())
}

/// Attach `state` to `routes`, and wrap them with the middleware: request logging, the default
/// Server header, and panics turned into 500 responses.
fn app(routes: Router<AppState>, state: AppState) -> Router {
    let developer_mode = state.config.server.developer_mode;
    routes
        .with_state(state)
        .layer(CatchPanicLayer::custom(move |panic| {
            panic_response(panic, developer_mode)
        }))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::SERVER,
            HeaderValue::from_static("hats-query"),
        ))
        // The outermost layer, so it logs the responses of caught panics too. The span holds the
        // method and path, it must be enabled at the level of the response record to show them.
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
}

/// A 500 response to a panic, with the panic message in `developer_mode` only.
///
/// The panic itself is logged by the panic hook, see `logging::init`.
fn panic_response(panic: Box<dyn Any + Send>, developer_mode: bool) -> Response {
    let body = if developer_mode {
        if let Some(message) = panic.downcast_ref::<String>() {
            message.clone()
        } else if let Some(message) = panic.downcast_ref::<&str>() {
            (*message).to_owned()
        } else {
            "panic with a non-string payload".to_owned()
        }
    } else {
        "Internal Server Error".to_owned()
    };
    (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
}

/// Wait for Ctrl-C, or SIGTERM on Unix.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutting down");
}
