use anyhow::Context;
use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, get_service, patch, post},
};
use chrono::{Duration, Utc};
use clap::Parser;
use rand::Rng;
use reqwest::{Client, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use std::{
    collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration as StdDuration,
};
use tokio::net::TcpListener;
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

mod engine;
use engine::{EngineAccount, EngineConfig, EngineMessage, EngineModel, WindsurfEngine};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, env = "WINDSURF_RS_HOST", default_value = "127.0.0.1")]
    host: String,
    #[arg(long, env = "WINDSURF_RS_PORT", default_value_t = 3003)]
    port: u16,
    #[arg(long, env = "WINDSURF_RS_DATA_DIR", default_value = ".data")]
    data_dir: PathBuf,
    #[arg(long, env = "WINDSURF_RS_STATIC_DIR")]
    static_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
    engine: WindsurfEngine,
}

#[derive(Clone)]
struct AccountScheduler {
    db: SqlitePool,
    capacity: CapacitySettings,
}

#[derive(Debug, Clone)]
struct AccountLease {
    account_id: i64,
    email: String,
    api_key: String,
    reservation_id: String,
    sticky: bool,
    caller_key: Option<String>,
    model: String,
    released: bool,
}

#[derive(Debug)]
struct SchedulerAccount {
    id: i64,
    email: String,
    status: String,
    tier: String,
    max_concurrent: i64,
    current_concurrent: i64,
    last_used_at: Option<String>,
    rate_limited_until: Option<String>,
    rpm_limit: i64,
    credits_json: Option<String>,
    blocked_models_json: Option<String>,
    credentials_json: Option<String>,
}

#[derive(Debug)]
enum AcquireError {
    #[allow(dead_code)]
    NoAccount,
    TemporarilyUnavailable {
        retry_after_secs: i64,
        reason: String,
    },
    Db(anyhow::Error),
}

#[derive(Serialize)]
struct ApiResponse<T: Serialize> {
    success: bool,
    data: T,
}

#[derive(Serialize)]
struct ApiError {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    kind: String,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SetupInstallRequest {
    admin_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginRequest {
    admin_key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAccountRequest {
    email: Option<String>,
    password: Option<String>,
    token: Option<String>,
    api_key: Option<String>,
    label: Option<String>,
    priority: Option<i64>,
    max_concurrent: Option<i64>,
    proxy_id: Option<i64>,
    proxy_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateAccountRequest {
    label: Option<String>,
    status: Option<String>,
    tier: Option<String>,
    tier_manual: Option<bool>,
    priority: Option<i64>,
    max_concurrent: Option<i64>,
    proxy_id: Option<Option<i64>>,
    blocked_models: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProxyRequest {
    name: String,
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateLoginJobRequest {
    text: String,
    delay_min_secs: Option<u64>,
    delay_max_secs: Option<u64>,
    fail_delay_min_secs: Option<u64>,
    fail_delay_max_secs: Option<u64>,
}

#[derive(Debug, Clone)]
struct LoginEntry {
    email: String,
    password: String,
    proxy: Option<String>,
}

#[derive(Debug)]
struct WindsurfLoginSuccess {
    email: String,
    name: String,
    api_key: String,
    credentials: Value,
    auth_method: String,
    api_server_url: Option<String>,
}

#[derive(Debug)]
struct WindsurfLoginError {
    code: String,
    message: String,
    auth_fail: bool,
    retry_after_secs: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginMethodResponse {
    user_exists: Option<bool>,
    has_password: Option<bool>,
}

#[derive(Deserialize)]
struct ConnectionsResponse {
    connections: Option<Vec<ConnectionInfo>>,
    auth_method: Option<AuthMethodInfo>,
}

#[derive(Deserialize)]
struct ConnectionInfo {
    #[serde(rename = "type")]
    kind: Option<String>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
struct AuthMethodInfo {
    method: Option<String>,
    has_password: Option<bool>,
}

#[derive(Deserialize)]
struct MessagesRequest {
    model: Option<String>,
    stream: Option<bool>,
    #[serde(default)]
    messages: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountTestRequest {
    account_id: Option<i64>,
    model: String,
    message: String,
    stream: Option<bool>,
    save_defaults: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CapacitySettings {
    queue_capacity: i64,
    queue_timeout_secs: i64,
    global_concurrency: i64,
    model_concurrency: i64,
    account_concurrency: i64,
    max_retries: i64,
    fallback_delay_ms: i64,
    model_cooldown_secs: i64,
    suspicious_cooldown_secs: i64,
    sticky_session_minutes: i64,
}

impl Default for CapacitySettings {
    fn default() -> Self {
        Self {
            queue_capacity: 300,
            queue_timeout_secs: 120,
            global_concurrency: 12,
            model_concurrency: 8,
            account_concurrency: 1,
            max_retries: 3,
            fallback_delay_ms: 350,
            model_cooldown_secs: 180,
            suspicious_cooldown_secs: 900,
            sticky_session_minutes: 30,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).with_context(|| {
        format!(
            "failed to create data directory {}",
            args.data_dir.display()
        )
    })?;
    let db_path = args.data_dir.join("windsurf-rs.sqlite3");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .with_context(|| format!("failed to open sqlite database {}", db_path.display()))?;
    run_migrations(&db).await?;

    let settings = settings_map(&db).await.unwrap_or_default();
    let engine = WindsurfEngine::new(EngineConfig::from_settings(
        &settings,
        args.data_dir.clone(),
    ));
    let state = AppState { db, engine };
    let app = router(state, args.static_dir);
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("windsurf-rs listening on http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

fn router(state: AppState, static_dir: Option<PathBuf>) -> Router {
    let api = Router::new()
        .route("/health", get(health))
        .route("/setup/status", get(setup_status))
        .route("/setup/install", post(setup_install))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/v1/models", get(models))
        .route("/v1/messages", post(messages))
        .route("/v1/messages/count_tokens", post(count_tokens))
        .route("/admin/accounts", get(accounts_list).post(accounts_create))
        .route(
            "/admin/accounts/{id}",
            patch(accounts_update).delete(accounts_delete),
        )
        .route("/admin/accounts/probe-all", post(accounts_probe_all))
        .route(
            "/admin/accounts/refresh-credits",
            post(accounts_refresh_credits_all),
        )
        .route("/admin/accounts/{id}/probe", post(account_probe))
        .route(
            "/admin/accounts/{id}/refresh-credits",
            post(account_refresh_credits),
        )
        .route(
            "/admin/accounts/{id}/reset-errors",
            post(account_reset_errors),
        )
        .route("/admin/accounts/{id}/reveal-key", post(account_reveal_key))
        .route(
            "/admin/accounts/{id}/clear-rate-limit",
            post(account_clear_rate_limit),
        )
        .route(
            "/admin/accounts/{id}/clear-sticky",
            post(account_clear_sticky),
        )
        .route("/admin/proxies", get(proxies_list).post(proxies_create))
        .route(
            "/admin/login-jobs",
            get(login_jobs_list).post(login_jobs_create),
        )
        .route(
            "/admin/login-jobs/{id}/events",
            get(login_job_events_stream),
        )
        .route("/admin/login-jobs/{id}/cancel", post(login_job_cancel))
        .route("/admin/requests", get(requests_list))
        .route("/admin/requests/{id}", get(request_detail))
        .route("/admin/capacity", get(capacity_get).put(capacity_put))
        .route(
            "/admin/account-test",
            get(account_test_defaults).post(account_test),
        )
        .route("/admin/settings", get(settings_get).put(settings_put))
        .with_state(Arc::new(state));

    let app = match static_dir {
        Some(dir) => {
            let index = dir.join("index.html");
            api.nest_service("/assets", ServeDir::new(dir.join("assets")))
                .fallback_service(get_service(ServeFile::new(index)))
        }
        None => api.fallback(api_not_found),
    };

    app.layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn run_migrations(db: &SqlitePool) -> anyhow::Result<()> {
    let sql = include_str!("../../../migrations/0001_init.sql");
    for statement in sql.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(db).await?;
        }
    }
    ensure_account_columns(db).await?;
    cleanup_runtime_state(db).await?;
    Ok(())
}

async fn ensure_account_columns(db: &SqlitePool) -> anyhow::Result<()> {
    let columns = [
        ("tier", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("tier_manual", "INTEGER NOT NULL DEFAULT 0"),
        ("error_count", "INTEGER NOT NULL DEFAULT 0"),
        ("last_used_at", "TEXT"),
        ("last_probed_at", "TEXT"),
        ("rate_limited_until", "TEXT"),
        ("rpm_used", "INTEGER NOT NULL DEFAULT 0"),
        ("rpm_limit", "INTEGER NOT NULL DEFAULT 60"),
        ("credits_json", "TEXT"),
        ("user_status_json", "TEXT"),
        ("available_models_json", "TEXT"),
        ("tier_models_json", "TEXT"),
        ("blocked_models_json", "TEXT"),
    ];
    let existing_rows = sqlx::query("PRAGMA table_info(accounts)")
        .fetch_all(db)
        .await?;
    let existing: std::collections::HashSet<String> = existing_rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    for (name, definition) in columns {
        if !existing.contains(name) {
            let sql = format!("ALTER TABLE accounts ADD COLUMN {} {}", name, definition);
            sqlx::query(&sql).execute(db).await?;
        }
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS account_model_inflight (
          reservation_id TEXT PRIMARY KEY,
          account_id INTEGER NOT NULL,
          model TEXT NOT NULL,
          created_at TEXT NOT NULL
        )",
    )
    .execute(db)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_account_model_inflight_model
          ON account_model_inflight(model, created_at)",
    )
    .execute(db)
    .await?;
    Ok(())
}

async fn cleanup_runtime_state(db: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query("UPDATE accounts SET current_concurrent=0")
        .execute(db)
        .await?;
    let now_text = now();
    sqlx::query("DELETE FROM sticky_sessions WHERE expires_at <= ?")
        .bind(&now_text)
        .execute(db)
        .await?;
    sqlx::query("DELETE FROM account_model_rate_limits WHERE limited_until <= ?")
        .bind(&now_text)
        .execute(db)
        .await?;
    sqlx::query("DELETE FROM account_rpm_events WHERE created_at <= ?")
        .bind((Utc::now() - Duration::seconds(60)).to_rfc3339())
        .execute(db)
        .await?;
    sqlx::query("DELETE FROM account_model_inflight")
        .execute(db)
        .await?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let setup = admin_key_hash(&state.db).await.ok().flatten().is_some();
    Json(json!({
        "ok": true,
        "setup": setup,
        "service": "windsurf-rs",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn setup_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let needs_setup = admin_key_hash(&state.db).await.ok().flatten().is_none();
    Json(ApiResponse {
        success: true,
        data: json!({ "needsSetup": needs_setup, "step": "welcome" }),
    })
}

async fn setup_install(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SetupInstallRequest>,
) -> impl IntoResponse {
    if admin_key_hash(&state.db).await.ok().flatten().is_some() {
        return error(
            StatusCode::FORBIDDEN,
            "setup_not_allowed",
            "系统已完成初始化",
        );
    }
    let admin_key = payload.admin_key.trim();
    if admin_key.len() < 12 {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_admin_key",
            "管理 key 至少需要 12 个字符",
        );
    }
    let hash = sha256_hex(admin_key);
    let now = now();
    match sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES ('admin_key_hash', ?, ?)",
    )
    .bind(hash)
    .bind(now)
    .execute(&state.db)
    .await
    {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: json!({ "message": "初始化完成" }),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "setup_failed",
            &err.to_string(),
        ),
    }
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<LoginRequest>,
) -> impl IntoResponse {
    let expected = match admin_key_hash(&state.db).await.ok().flatten() {
        Some(value) => value,
        None => return error(StatusCode::FORBIDDEN, "setup_required", "请先完成初始化"),
    };
    if sha256_hex(payload.admin_key.trim()) != expected {
        return error(StatusCode::UNAUTHORIZED, "invalid_key", "管理 key 不正确");
    }
    let token = Uuid::new_v4().to_string();
    let token_hash = sha256_hex(&token);
    let created_at = now();
    let expires_at = (Utc::now() + Duration::hours(24)).to_rfc3339();
    if let Err(err) = sqlx::query(
        "INSERT INTO admin_sessions (token_hash, created_at, expires_at) VALUES (?, ?, ?)",
    )
    .bind(token_hash)
    .bind(created_at)
    .bind(expires_at)
    .execute(&state.db)
    .await
    {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "login_failed",
            &err.to_string(),
        );
    }
    Json(ApiResponse {
        success: true,
        data: json!({ "token": token }),
    })
    .into_response()
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(token) = bearer_token(&headers) {
        let _ = sqlx::query("DELETE FROM admin_sessions WHERE token_hash = ?")
            .bind(sha256_hex(&token))
            .execute(&state.db)
            .await;
    }
    Json(ApiResponse {
        success: true,
        data: json!({}),
    })
}

async fn models(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let models = model_catalog(&state.db).await;
    Json(json!({
        "object": "list",
        "data": models.into_iter().map(|model| json!({
            "id": model.get("id").and_then(Value::as_str).unwrap_or("unknown"),
            "object": "model",
            "created": 0,
            "owned_by": model.get("provider").and_then(Value::as_str).unwrap_or("windsurf"),
            "_windsurf": model
        })).collect::<Vec<_>>()
    }))
}

async fn messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<MessagesRequest>,
) -> impl IntoResponse {
    let trace_id = Uuid::new_v4().to_string();
    let model = payload
        .model
        .clone()
        .unwrap_or_else(|| "claude-opus-4-1".to_string());
    let stream_requested = payload.stream.unwrap_or(false);
    let _ = create_trace(&state.db, &trace_id, Some(&model), stream_requested).await;
    let _ = add_trace_chunk(
        &state.db,
        &trace_id,
        "client_request",
        &json!(payload.messages),
    )
    .await;
    let caller_key = extract_caller_key(&headers, &payload.messages);
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone());
    let mut lease = match scheduler.acquire(&model, caller_key.clone()).await {
        Ok(lease) => lease,
        Err(AcquireError::TemporarilyUnavailable {
            retry_after_secs,
            reason,
        }) => {
            let message = format!("账号池暂时不可用，请 {} 秒后重试", retry_after_secs);
            let _ = finish_trace(&state.db, &trace_id, "error", None, Some(&reason)).await;
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    header::RETRY_AFTER,
                    HeaderValue::from_str(&retry_after_secs.to_string())
                        .unwrap_or_else(|_| HeaderValue::from_static("60")),
                )],
                Json(json!({
                    "error": {
                        "type": "rate_limit_exceeded",
                        "message": message,
                        "retry_after": retry_after_secs,
                        "reason": reason
                    }
                })),
            )
                .into_response();
        }
        Err(AcquireError::NoAccount) => {
            let _ = finish_trace(&state.db, &trace_id, "error", None, Some("没有可用账号")).await;
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "pool_exhausted",
                "没有可用账号",
            );
        }
        Err(AcquireError::Db(err)) => {
            let _ = finish_trace(&state.db, &trace_id, "error", None, Some(&err.to_string())).await;
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "scheduler_error",
                &err.to_string(),
            );
        }
    };
    let _ = bind_trace_account(&state.db, &trace_id, lease.account_id).await;
    let _ = add_trace_chunk(
        &state.db,
        &trace_id,
        "account_acquired",
        &json!({
            "accountId": lease.account_id,
            "email": lease.email,
            "model": model,
            "sticky": lease.sticky,
            "reservationId": lease.reservation_id
        }),
    )
    .await;

    if stream_requested {
        let db = state.db.clone();
        let capacity_for_stream = capacity.clone();
        let engine = state.engine.clone();
        let engine_account = EngineAccount {
            api_key: lease.api_key.clone(),
            proxy_url: proxy_url_for_account(&state.db, lease.account_id).await,
        };
        let engine_model = resolve_engine_model(&model);
        let engine_messages = messages_from_anthropic(&payload.messages);
        let trace = trace_id.clone();
        let model_for_stream = model.clone();
        let s = stream! {
            let start = json!({
                "type": "message_start",
                "message": {
                    "id": format!("msg_{}", trace),
                    "type": "message",
                    "role": "assistant",
                    "model": model_for_stream,
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": { "input_tokens": 1, "output_tokens": 0 }
                }
            });
            yield Ok::<Event, std::convert::Infallible>(Event::default().data(start.to_string()));
            match engine.cascade_stream(engine_account, engine_model, engine_messages).await {
                Ok(upstream) => {
                    use futures_util::StreamExt;
                    futures_util::pin_mut!(upstream);
                    let mut output_tokens = 0_i64;
                    while let Some(item) = upstream.next().await {
                        match item {
                            Ok(chunk) => {
                                output_tokens += (chunk.text.chars().count() as i64 / 4).max(1);
                                let delta = json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": chunk.text}});
                                let _ = add_trace_chunk(&db, &trace, "downstream_event", &delta).await;
                                yield Ok(Event::default().data(delta.to_string()));
                            }
                            Err(err) => {
                                let _ = finish_trace(&db, &trace, "error", None, Some(&err.to_string())).await;
                                let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
                                let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                                yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":err.to_string()}}).to_string()));
                                return;
                            }
                        }
                    }
                    let stop = json!({"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null}, "usage": {"output_tokens": output_tokens}});
                    yield Ok(Event::default().data(stop.to_string()));
                    yield Ok(Event::default().data(json!({"type": "message_stop"}).to_string()));
                    let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
                    let _ = scheduler.mark_success(&mut lease).await;
                    let _ = finish_trace(&db, &trace, "ok", Some("end_turn"), None).await;
                }
                Err(err) => {
                    let _ = finish_trace(&db, &trace, "error", None, Some(&err.to_string())).await;
                    let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
                    let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                    yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":err.to_string()}}).to_string()));
                }
            }
        };
        Sse::new(s).into_response()
    } else {
        let engine = state.engine.clone();
        let engine_account = EngineAccount {
            api_key: lease.api_key.clone(),
            proxy_url: proxy_url_for_account(&state.db, lease.account_id).await,
        };
        let engine_model = resolve_engine_model(&model);
        let engine_messages = messages_from_anthropic(&payload.messages);
        let mut text = String::new();
        match engine
            .cascade_stream(engine_account, engine_model, engine_messages)
            .await
        {
            Ok(upstream) => {
                use futures_util::StreamExt;
                futures_util::pin_mut!(upstream);
                while let Some(item) = upstream.next().await {
                    match item {
                        Ok(chunk) => text.push_str(&chunk.text),
                        Err(err) => {
                            let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                            let _ = finish_trace(
                                &state.db,
                                &trace_id,
                                "error",
                                None,
                                Some(&err.to_string()),
                            )
                            .await;
                            return error(
                                StatusCode::BAD_GATEWAY,
                                "upstream_error",
                                &err.to_string(),
                            );
                        }
                    }
                }
            }
            Err(err) => {
                let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                let _ =
                    finish_trace(&state.db, &trace_id, "error", None, Some(&err.to_string())).await;
                return error(StatusCode::BAD_GATEWAY, "upstream_error", &err.to_string());
            }
        }
        let output_tokens = (text.chars().count() as f64 / 4.0).ceil() as u64;
        let response = json!({
            "id": format!("msg_{}", trace_id),
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{ "type": "text", "text": text }],
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": { "input_tokens": 1, "output_tokens": output_tokens }
        });
        let _ = add_trace_chunk(&state.db, &trace_id, "downstream_response", &response).await;
        let _ = scheduler.mark_success(&mut lease).await;
        let _ = finish_trace(&state.db, &trace_id, "ok", Some("end_turn"), None).await;
        Json(response).into_response()
    }
}

async fn count_tokens(Json(payload): Json<Value>) -> impl IntoResponse {
    let raw = payload.to_string();
    let tokens = (raw.chars().count() as f64 / 4.0).ceil() as u64;
    Json(json!({ "input_tokens": tokens.max(1) }))
}

async fn accounts_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone());
    let _ = scheduler.cleanup_expired().await;
    let _ = refresh_rpm_counters(&state.db).await;
    let rows = match sqlx::query("SELECT * FROM accounts ORDER BY id DESC")
        .fetch_all(&state.db)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let model_limits = account_model_rate_limits(&state.db)
        .await
        .unwrap_or_default();
    let sticky_counts = account_sticky_counts(&state.db).await.unwrap_or_default();
    let accounts: Vec<Value> = rows
        .into_iter()
        .map(|row| account_json(row, &model_limits, &sticky_counts))
        .collect();
    Json(ApiResponse {
        success: true,
        data: json!({ "accounts": accounts }),
    })
    .into_response()
}

async fn accounts_create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateAccountRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let proxy_id =
        match resolve_proxy_id(&state.db, payload.proxy_id, payload.proxy_url.as_deref()).await {
            Ok(value) => value,
            Err(resp) => return resp,
        };
    let result = if let Some(api_key) = payload
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        upsert_manual_account(
            &state.db,
            ManualAccountInput {
                email: payload
                    .email
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| payload.label.as_deref().unwrap_or("API Key 账号")),
                label: payload.label.as_deref(),
                api_key,
                auth_method: "api_key",
                api_server_url: None,
                proxy_id,
                priority: payload.priority.unwrap_or(0),
                max_concurrent: payload.max_concurrent.unwrap_or(1),
                extra_credentials: json!({ "source": "manual_api_key" }),
            },
        )
        .await
    } else if let Some(token) = payload
        .token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match register_with_firebase(
            &build_windsurf_client(None).unwrap_or_else(|_| Client::new()),
            &login_fingerprint(),
            token,
        )
        .await
        {
            Ok(reg) => {
                let api_key = reg
                    .get("api_key")
                    .or_else(|| reg.get("apiKey"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let name = reg
                    .get("name")
                    .and_then(Value::as_str)
                    .or(payload.label.as_deref())
                    .unwrap_or("Token 账号");
                let api_server_url = reg
                    .get("api_server_url")
                    .or_else(|| reg.get("apiServerUrl"))
                    .and_then(Value::as_str);
                upsert_manual_account(
                    &state.db,
                    ManualAccountInput {
                        email: payload
                            .email
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .unwrap_or(name),
                        label: payload.label.as_deref().or(Some(name)),
                        api_key,
                        auth_method: "token",
                        api_server_url,
                        proxy_id,
                        priority: payload.priority.unwrap_or(0),
                        max_concurrent: payload.max_concurrent.unwrap_or(1),
                        extra_credentials: json!({ "source": "manual_token", "token": token }),
                    },
                )
                .await
            }
            Err(err) => return error(StatusCode::BAD_REQUEST, &err.code, &err.message),
        }
    } else if let (Some(email), Some(password)) =
        (payload.email.as_deref(), payload.password.as_deref())
    {
        let entry = LoginEntry {
            email: email.trim().to_string(),
            password: password.to_string(),
            proxy: payload.proxy_url.clone(),
        };
        match windsurf_login(&entry).await {
            Ok(login) => upsert_logged_in_account_with_options(
                &state.db,
                &entry,
                login,
                payload.label.as_deref(),
                proxy_id,
                payload.priority.unwrap_or(0),
                payload.max_concurrent.unwrap_or(1),
            )
            .await
            .ok_or_else(|| "账号保存失败".to_string()),
            Err(err) => return error(StatusCode::BAD_REQUEST, &err.code, &err.message),
        }
    } else if let Some(email) = payload
        .email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        create_placeholder_account(
            &state.db,
            email,
            payload.label.as_deref(),
            proxy_id,
            payload.priority.unwrap_or(0),
            payload.max_concurrent.unwrap_or(1),
        )
        .await
    } else {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_account",
            "请填写可添加的账号信息",
        );
    };
    match result {
        Ok(id) => Json(ApiResponse {
            success: true,
            data: json!({ "id": id }),
        })
        .into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "db_error", &err),
    }
}

async fn accounts_update(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateAccountRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let row = match sqlx::query("SELECT * FROM accounts WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", "账号不存在"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let label = payload
        .label
        .or_else(|| row.get::<Option<String>, _>("label"));
    let status = payload
        .status
        .unwrap_or_else(|| row.get::<String, _>("status"));
    let priority = payload
        .priority
        .unwrap_or_else(|| row.get::<i64, _>("priority"));
    let max_concurrent = payload
        .max_concurrent
        .unwrap_or_else(|| row.get::<i64, _>("max_concurrent"));
    let proxy_id = payload
        .proxy_id
        .unwrap_or_else(|| row.get::<Option<i64>, _>("proxy_id"));
    let tier = payload.tier.unwrap_or_else(|| row.get::<String, _>("tier"));
    let tier_manual = payload
        .tier_manual
        .map(|value| if value { 1_i64 } else { 0_i64 })
        .unwrap_or_else(|| row.get::<i64, _>("tier_manual"));
    let blocked_models = payload
        .blocked_models
        .map(|value| json!(value).to_string())
        .or_else(|| row.get::<Option<String>, _>("blocked_models_json"));
    let now = now();
    match sqlx::query("UPDATE accounts SET label=?, status=?, tier=?, tier_manual=?, priority=?, max_concurrent=?, proxy_id=?, blocked_models_json=?, updated_at=? WHERE id=?")
        .bind(label)
        .bind(status)
        .bind(tier)
        .bind(tier_manual)
        .bind(priority)
        .bind(max_concurrent)
        .bind(proxy_id)
        .bind(blocked_models)
        .bind(now)
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => Json(ApiResponse { success: true, data: json!({}) }).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "db_error", &err.to_string()),
    }
}

async fn accounts_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    match sqlx::query("DELETE FROM accounts WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: json!({}),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
}

async fn account_reset_errors(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let now_text = now();
    match sqlx::query("UPDATE accounts SET error_count=0, last_error=NULL, status='ready', updated_at=? WHERE id=?")
        .bind(now_text)
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => Json(ApiResponse { success: true, data: json!({}) }).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "db_error", &err.to_string()),
    }
}

async fn account_reveal_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let row = match sqlx::query("SELECT * FROM accounts WHERE id=?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", "账号不存在"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let Some(api_key) = account_api_key(&row) else {
        return error(
            StatusCode::BAD_REQUEST,
            "credential_missing",
            "该账号没有可用凭据",
        );
    };
    Json(ApiResponse {
        success: true,
        data: json!({ "apiKey": api_key, "credentialMask": mask_secret(&api_key) }),
    })
    .into_response()
}

async fn account_clear_rate_limit(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone());
    match scheduler.clear_rate_limit(id).await {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: json!({}),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
}

async fn account_clear_sticky(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity);
    match scheduler.clear_sticky_for_account(id).await {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: json!({}),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
}

async fn account_refresh_credits(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    match refresh_account_status(&state.db, id, false).await {
        Ok(value) => Json(ApiResponse {
            success: true,
            data: value,
        })
        .into_response(),
        Err(err) => error(StatusCode::BAD_REQUEST, &err.code, &err.message),
    }
}

async fn accounts_refresh_credits_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let ids = match account_ids(&state.db).await {
        Ok(ids) => ids,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let mut results = Vec::new();
    for id in ids {
        let result = refresh_account_status(&state.db, id, false).await;
        results.push(match result {
            Ok(value) => json!({ "id": id, "success": true, "data": value }),
            Err(err) => json!({ "id": id, "success": false, "error": { "type": err.code, "message": err.message } }),
        });
    }
    Json(ApiResponse {
        success: true,
        data: json!({ "results": results }),
    })
    .into_response()
}

async fn account_probe(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    match refresh_account_status(&state.db, id, true).await {
        Ok(value) => Json(ApiResponse {
            success: true,
            data: value,
        })
        .into_response(),
        Err(err) => error(StatusCode::BAD_REQUEST, &err.code, &err.message),
    }
}

async fn accounts_probe_all(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let ids = match account_ids(&state.db).await {
        Ok(ids) => ids,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let mut results = Vec::new();
    for id in ids {
        let result = refresh_account_status(&state.db, id, true).await;
        results.push(match result {
            Ok(value) => json!({ "id": id, "success": true, "data": value }),
            Err(err) => json!({ "id": id, "success": false, "error": { "type": err.code, "message": err.message } }),
        });
    }
    Json(ApiResponse {
        success: true,
        data: json!({ "results": results }),
    })
    .into_response()
}

async fn proxies_list(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let rows = match sqlx::query("SELECT * FROM proxies ORDER BY id DESC")
        .fetch_all(&state.db)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let proxies: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "name": row.get::<String, _>("name"),
                "url": row.get::<String, _>("url"),
                "status": row.get::<String, _>("status"),
                "lastError": row.get::<Option<String>, _>("last_error"),
                "createdAt": row.get::<String, _>("created_at"),
                "updatedAt": row.get::<String, _>("updated_at")
            })
        })
        .collect();
    Json(ApiResponse {
        success: true,
        data: json!({ "proxies": proxies }),
    })
    .into_response()
}

async fn proxies_create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateProxyRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let now = now();
    match sqlx::query("INSERT INTO proxies (name, url, created_at, updated_at) VALUES (?, ?, ?, ?)")
        .bind(payload.name.trim())
        .bind(payload.url.trim())
        .bind(&now)
        .bind(&now)
        .execute(&state.db)
        .await
    {
        Ok(done) => Json(ApiResponse {
            success: true,
            data: json!({ "id": done.last_insert_rowid() }),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
}

async fn login_jobs_create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateLoginJobRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let lines: Vec<String> = payload
        .text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    if lines.is_empty() {
        return error(StatusCode::BAD_REQUEST, "empty_job", "没有可导入的账号");
    }
    let id = Uuid::new_v4().to_string();
    let now_text = now();
    if let Err(err) = sqlx::query("INSERT INTO login_jobs (id, status, total, created_at, updated_at) VALUES (?, 'running', ?, ?, ?)")
        .bind(&id)
        .bind(lines.len() as i64)
        .bind(&now_text)
        .bind(&now_text)
        .execute(&state.db)
        .await
    {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "db_error", &err.to_string());
    }
    let db = state.db.clone();
    let job_id = id.clone();
    tokio::spawn(async move {
        run_login_job(db, job_id, lines, payload).await;
    });
    Json(ApiResponse {
        success: true,
        data: json!({ "id": id }),
    })
    .into_response()
}

async fn login_jobs_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let rows = match sqlx::query("SELECT * FROM login_jobs ORDER BY created_at DESC LIMIT 100")
        .fetch_all(&state.db)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let jobs: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<String, _>("id"),
                "status": row.get::<String, _>("status"),
                "total": row.get::<i64, _>("total"),
                "successCount": row.get::<i64, _>("success_count"),
                "failedCount": row.get::<i64, _>("failed_count"),
                "cancelled": row.get::<i64, _>("cancelled") != 0,
                "createdAt": row.get::<String, _>("created_at"),
                "updatedAt": row.get::<String, _>("updated_at"),
                "completedAt": row.get::<Option<String>, _>("completed_at")
            })
        })
        .collect();
    Json(ApiResponse {
        success: true,
        data: json!({ "jobs": jobs }),
    })
    .into_response()
}

async fn login_job_events_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let db = state.db.clone();
    let s = stream! {
        let mut last_id = 0_i64;
        loop {
            let rows = sqlx::query("SELECT * FROM login_job_events WHERE job_id = ? AND id > ? ORDER BY id ASC")
                .bind(&id)
                .bind(last_id)
                .fetch_all(&db)
                .await
                .unwrap_or_default();
            for row in rows {
                last_id = row.get::<i64, _>("id");
                let event_type = row.get::<String, _>("event_type");
                let payload = row.get::<String, _>("payload");
                yield Ok::<Event, std::convert::Infallible>(Event::default().event(event_type).data(payload));
            }
            let done = sqlx::query("SELECT status FROM login_jobs WHERE id = ?")
                .bind(&id)
                .fetch_optional(&db)
                .await
                .ok()
                .flatten()
                .map(|row| row.get::<String, _>("status"))
                .map(|status| status == "completed" || status == "cancelled")
                .unwrap_or(true);
            if done {
                yield Ok(Event::default().event("close").data("{}"));
                break;
            }
            tokio::time::sleep(StdDuration::from_secs(1)).await;
        }
    };
    Sse::new(s).into_response()
}

async fn login_job_cancel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let now_text = now();
    let _ = sqlx::query("UPDATE login_jobs SET cancelled=1, status='cancelled', updated_at=?, completed_at=? WHERE id=?")
        .bind(&now_text)
        .bind(&now_text)
        .bind(&id)
        .execute(&state.db)
        .await;
    let _ = add_job_event(
        &state.db,
        &id,
        "cancelled",
        json!({"message": "任务已停止"}),
    )
    .await;
    Json(ApiResponse {
        success: true,
        data: json!({}),
    })
    .into_response()
}

async fn requests_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let rows = match sqlx::query("SELECT * FROM request_traces ORDER BY started_at DESC LIMIT 100")
        .fetch_all(&state.db)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let requests: Vec<Value> = rows.into_iter().map(trace_json).collect();
    Json(ApiResponse {
        success: true,
        data: json!({ "requests": requests }),
    })
    .into_response()
}

async fn request_detail(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let row = match sqlx::query("SELECT * FROM request_traces WHERE id=?")
        .bind(&id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", "请求记录不存在"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let chunks = sqlx::query("SELECT * FROM request_trace_chunks WHERE trace_id=? ORDER BY id ASC")
        .bind(&id)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "layer": row.get::<String, _>("layer"),
                "payload": row.get::<String, _>("payload"),
                "createdAt": row.get::<String, _>("created_at")
            })
        })
        .collect::<Vec<_>>();
    Json(ApiResponse {
        success: true,
        data: json!({ "request": trace_json(row), "chunks": chunks }),
    })
    .into_response()
}

async fn capacity_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let global_inflight = sqlx::query("SELECT COUNT(*) AS count FROM account_model_inflight")
        .fetch_one(&state.db)
        .await
        .map(|row| row.get::<i64, _>("count"))
        .unwrap_or(0);
    let model_rows = sqlx::query("SELECT model, COUNT(*) AS count FROM account_model_inflight GROUP BY model ORDER BY count DESC")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let models = model_rows
        .into_iter()
        .map(|row| json!({ "model": row.get::<String, _>("model"), "inflight": row.get::<i64, _>("count") }))
        .collect::<Vec<_>>();
    Json(ApiResponse {
        success: true,
        data: json!({ "settings": capacity, "runtime": { "globalInflight": global_inflight, "models": models } }),
    })
    .into_response()
}

async fn capacity_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CapacitySettings>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let settings = normalize_capacity(payload);
    match save_json_setting(&state.db, "capacity", &json!(settings)).await {
        Ok(_) => Json(ApiResponse {
            success: true,
            data: json!({ "settings": settings }),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
}

async fn account_test_defaults(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let default_model = get_setting_string(&state.db, "account_test_model")
        .await
        .unwrap_or_else(|| "claude-opus-4.7".to_string());
    let default_message = get_setting_string(&state.db, "account_test_message")
        .await
        .unwrap_or_else(|| "用一句话确认这个账号可以正常回复。".to_string());
    let models = model_catalog(&state.db).await;
    Json(ApiResponse {
        success: true,
        data: json!({ "model": default_model, "message": default_message, "models": models }),
    })
    .into_response()
}

async fn account_test(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<AccountTestRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let model = payload.model.trim().to_string();
    let message = payload.message.trim().to_string();
    if model.is_empty() || message.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_test",
            "请选择模型并填写测试内容",
        );
    }
    if payload.save_defaults.unwrap_or(true) {
        let _ = save_json_setting(&state.db, "account_test_model", &json!(model)).await;
        let _ = save_json_setting(&state.db, "account_test_message", &json!(message)).await;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone());
    let mut lease = match payload.account_id {
        Some(id) => match scheduler
            .try_reserve_account(id, &model, Some(format!("admin-test:{id}")), false)
            .await
        {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                return error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "account_unavailable",
                    "这个账号暂时不可用",
                );
            }
            Err(err) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "scheduler_error",
                    &err.to_string(),
                );
            }
        },
        None => match scheduler
            .acquire(&model, Some("admin-test:auto".to_string()))
            .await
        {
            Ok(lease) => lease,
            Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs,
                reason,
            }) => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(header::RETRY_AFTER, HeaderValue::from_str(&retry_after_secs.to_string()).unwrap_or_else(|_| HeaderValue::from_static("60")))],
                    Json(json!({ "error": { "type": "rate_limit_exceeded", "message": reason, "retry_after": retry_after_secs } })),
                ).into_response();
            }
            Err(AcquireError::NoAccount) => {
                return error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "pool_exhausted",
                    "没有可用账号",
                );
            }
            Err(AcquireError::Db(err)) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "scheduler_error",
                    &err.to_string(),
                );
            }
        },
    };
    let stream_requested = payload.stream.unwrap_or(true);
    if stream_requested {
        let db = state.db.clone();
        let capacity_for_stream = capacity.clone();
        let engine = state.engine.clone();
        let engine_account = EngineAccount {
            api_key: lease.api_key.clone(),
            proxy_url: proxy_url_for_account(&state.db, lease.account_id).await,
        };
        let engine_model = resolve_engine_model(&model);
        let engine_messages = vec![EngineMessage {
            role: "user".to_string(),
            content: message.clone(),
        }];
        let s = stream! {
            let start = json!({"type": "message_start", "accountId": lease.account_id, "model": model, "email": lease.email});
            yield Ok::<Event, std::convert::Infallible>(Event::default().data(start.to_string()));
            match engine.cascade_stream(engine_account, engine_model, engine_messages).await {
                Ok(upstream) => {
                    use futures_util::StreamExt;
                    futures_util::pin_mut!(upstream);
                    while let Some(item) = upstream.next().await {
                        match item {
                            Ok(chunk) => yield Ok(Event::default().data(json!({"type": "content_block_delta", "delta": {"type": "text_delta", "text": chunk.text}}).to_string())),
                            Err(err) => {
                                let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
                                let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                                yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":err.to_string()}}).to_string()));
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
                    let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                    yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":err.to_string()}}).to_string()));
                    return;
                }
            }
            yield Ok(Event::default().data(json!({"type": "message_stop"}).to_string()));
            let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone());
            let _ = scheduler.mark_success(&mut lease).await;
        };
        Sse::new(s).into_response()
    } else {
        let account_id = lease.account_id;
        let email = lease.email.clone();
        let engine = state.engine.clone();
        let engine_account = EngineAccount {
            api_key: lease.api_key.clone(),
            proxy_url: proxy_url_for_account(&state.db, lease.account_id).await,
        };
        let engine_model = resolve_engine_model(&model);
        let engine_messages = vec![EngineMessage {
            role: "user".to_string(),
            content: message.clone(),
        }];
        let mut content = String::new();
        match engine
            .cascade_stream(engine_account, engine_model, engine_messages)
            .await
        {
            Ok(upstream) => {
                use futures_util::StreamExt;
                futures_util::pin_mut!(upstream);
                while let Some(item) = upstream.next().await {
                    match item {
                        Ok(chunk) => content.push_str(&chunk.text),
                        Err(err) => {
                            let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                            return error(
                                StatusCode::BAD_GATEWAY,
                                "upstream_error",
                                &err.to_string(),
                            );
                        }
                    }
                }
            }
            Err(err) => {
                let _ = scheduler.mark_error(&mut lease, &err.to_string()).await;
                return error(StatusCode::BAD_GATEWAY, "upstream_error", &err.to_string());
            }
        }
        let _ = scheduler.mark_success(&mut lease).await;
        Json(ApiResponse {
            success: true,
            data: json!({
                "accountId": account_id,
                "email": email,
                "model": model,
                "content": content
            }),
        })
        .into_response()
    }
}

async fn settings_get(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let rows = sqlx::query("SELECT key, value FROM settings WHERE key != 'admin_key_hash'")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let mut map = serde_json::Map::new();
    for row in rows {
        map.insert(
            row.get::<String, _>("key"),
            json!(row.get::<String, _>("value")),
        );
    }
    Json(ApiResponse {
        success: true,
        data: Value::Object(map),
    })
    .into_response()
}

async fn settings_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let now_text = now();
    if let Some(obj) = payload.as_object() {
        for (key, value) in obj {
            if key == "admin_key_hash" {
                continue;
            }
            let _ = sqlx::query(
                "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            )
            .bind(key)
            .bind(value.to_string())
            .bind(&now_text)
            .execute(&state.db)
            .await;
        }
    }
    Json(ApiResponse {
        success: true,
        data: json!({}),
    })
    .into_response()
}

async fn get_setting_string(db: &SqlitePool, key: &str) -> Option<String> {
    let row = sqlx::query("SELECT value FROM settings WHERE key=?")
        .bind(key)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()?;
    let raw = row.get::<String, _>("value");
    serde_json::from_str::<String>(&raw).ok().or(Some(raw))
}

async fn save_json_setting(db: &SqlitePool, key: &str, value: &Value) -> anyhow::Result<()> {
    sqlx::query("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)")
        .bind(key)
        .bind(value.to_string())
        .bind(now())
        .execute(db)
        .await?;
    Ok(())
}

async fn capacity_settings(db: &SqlitePool) -> anyhow::Result<CapacitySettings> {
    let Some(raw) = get_setting_string(db, "capacity").await else {
        return Ok(CapacitySettings::default());
    };
    let parsed = serde_json::from_str::<CapacitySettings>(&raw).unwrap_or_default();
    Ok(normalize_capacity(parsed))
}

fn normalize_capacity(mut settings: CapacitySettings) -> CapacitySettings {
    settings.queue_capacity = settings.queue_capacity.clamp(1, 5000);
    settings.queue_timeout_secs = settings.queue_timeout_secs.clamp(1, 900);
    settings.global_concurrency = settings.global_concurrency.clamp(1, 500);
    settings.model_concurrency = settings.model_concurrency.clamp(1, 500);
    settings.account_concurrency = settings.account_concurrency.clamp(1, 20);
    settings.max_retries = settings.max_retries.clamp(0, 20);
    settings.fallback_delay_ms = settings.fallback_delay_ms.clamp(0, 30_000);
    settings.model_cooldown_secs = settings.model_cooldown_secs.clamp(1, 3600);
    settings.suspicious_cooldown_secs = settings.suspicious_cooldown_secs.clamp(1, 7200);
    settings.sticky_session_minutes = settings.sticky_session_minutes.clamp(1, 1440);
    settings
}

async fn model_catalog(db: &SqlitePool) -> Vec<Value> {
    let rows = sqlx::query("SELECT available_models_json FROM accounts WHERE available_models_json IS NOT NULL ORDER BY last_probed_at DESC")
        .fetch_all(db)
        .await
        .unwrap_or_default();
    let mut seen = std::collections::HashSet::new();
    let mut models = Vec::new();
    for row in rows {
        let Some(raw) = row.get::<Option<String>, _>("available_models_json") else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let Some(items) = value.as_array() else {
            continue;
        };
        for item in items {
            let Some(id) = item
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            else {
                continue;
            };
            if seen.insert(id.to_string()) {
                models.push(item.clone());
            }
        }
    }
    if models.is_empty() {
        models = default_model_catalog();
    }
    models.sort_by(|a, b| {
        a.get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .cmp(b.get("id").and_then(Value::as_str).unwrap_or(""))
    });
    models
}

fn default_model_catalog() -> Vec<Value> {
    [
        ("claude-opus-4.7", "Claude Opus 4.7", "anthropic", 8.0),
        (
            "claude-opus-4.7-high",
            "Claude Opus 4.7 High",
            "anthropic",
            10.0,
        ),
        ("claude-opus-4.6", "Claude Opus 4.6", "anthropic", 6.0),
        ("claude-sonnet-4.6", "Claude Sonnet 4.6", "anthropic", 4.0),
        ("claude-4.5-sonnet", "Claude Sonnet 4.5", "anthropic", 2.0),
        ("gemini-2.5-flash", "Gemini 2.5 Flash", "google", 0.5),
    ]
    .into_iter()
    .map(|(id, label, provider, credit)| {
        json!({
            "id": id,
            "label": label,
            "provider": provider,
            "creditMultiplier": credit,
            "supportsImages": false
        })
    })
    .collect()
}

async fn settings_map(db: &SqlitePool) -> anyhow::Result<HashMap<String, String>> {
    let rows = sqlx::query("SELECT key, value FROM settings WHERE key != 'admin_key_hash'")
        .fetch_all(db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get::<String, _>("key"), row.get::<String, _>("value")))
        .collect())
}

async fn proxy_url_for_account(db: &SqlitePool, account_id: i64) -> Option<String> {
    let row = sqlx::query(
        "SELECT proxies.url AS url FROM accounts LEFT JOIN proxies ON proxies.id=accounts.proxy_id WHERE accounts.id=?",
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()?;
    row.get::<Option<String>, _>("url")
}

fn messages_from_anthropic(value: &Value) -> Vec<EngineMessage> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|item| EngineMessage {
                    role: item
                        .get("role")
                        .and_then(Value::as_str)
                        .unwrap_or("user")
                        .to_string(),
                    content: anthropic_content_to_text(item.get("content").unwrap_or(&Value::Null)),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn anthropic_content_to_text(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(items) = value.as_array() {
        return items
            .iter()
            .filter_map(|item| {
                item.get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| item.as_str().map(str::to_string))
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    if value.is_null() {
        String::new()
    } else {
        value.to_string()
    }
}

fn resolve_engine_model(model: &str) -> EngineModel {
    let key = model_alias(model);
    let (enum_value, model_uid): (u64, Option<String>) = match key.as_str() {
        "claude-4.1-opus" => (328, Some("MODEL_CLAUDE_4_1_OPUS".to_string())),
        "claude-4.5-opus" => (391, Some("MODEL_CLAUDE_4_5_OPUS".to_string())),
        "claude-4.5-sonnet" => (353, Some("MODEL_PRIVATE_2".to_string())),
        "claude-sonnet-4.6" => (0, Some("claude-sonnet-4-6".to_string())),
        "claude-opus-4.6" => (0, Some("claude-opus-4-6".to_string())),
        "claude-opus-4-7-low" => (0, Some("claude-opus-4-7-low".to_string())),
        "claude-opus-4-7-high" => (0, Some("claude-opus-4-7-high".to_string())),
        "claude-opus-4-7-xhigh" => (0, Some("claude-opus-4-7-xhigh".to_string())),
        "claude-opus-4-7-max" => (0, Some("claude-opus-4-7-max".to_string())),
        "claude-opus-4-7-medium" => (0, Some("claude-opus-4-7-medium".to_string())),
        "gemini-2.5-flash" => (312, Some("MODEL_GOOGLE_GEMINI_2_5_FLASH".to_string())),
        other => (0, Some(other.to_string())),
    };
    EngineModel {
        id: key,
        enum_value,
        model_uid,
    }
}

fn model_alias(model: &str) -> String {
    match model {
        "claude-opus-4-1" | "claude-opus-4.1" | "claude-opus-4-1-20250805" => {
            "claude-4.1-opus".to_string()
        }
        "claude-opus-4-5" | "claude-opus-4.5" | "claude-opus-4-5-20251101" => {
            "claude-4.5-opus".to_string()
        }
        "claude-opus-4-6" | "claude-opus-4.6" => "claude-opus-4.6".to_string(),
        "claude-sonnet-4-6" | "claude-sonnet-4.6" => "claude-sonnet-4.6".to_string(),
        "claude-opus-4-7" | "claude-opus-4.7" | "claude-opus-4-7-latest" => {
            "claude-opus-4-7-medium".to_string()
        }
        "claude-opus-4.7-low" => "claude-opus-4-7-low".to_string(),
        "claude-opus-4.7-high" => "claude-opus-4-7-high".to_string(),
        "claude-opus-4.7-xhigh" => "claude-opus-4-7-xhigh".to_string(),
        "claude-opus-4.7-max" => "claude-opus-4-7-max".to_string(),
        other => other.to_string(),
    }
}

async fn api_not_found() -> impl IntoResponse {
    error(StatusCode::NOT_FOUND, "not_found", "接口不存在")
}

async fn run_login_job(
    db: SqlitePool,
    job_id: String,
    lines: Vec<String>,
    opts: CreateLoginJobRequest,
) {
    let delay_min = opts.delay_min_secs.unwrap_or(15);
    let delay_max = opts.delay_max_secs.unwrap_or(45).max(delay_min);
    let fail_min = opts.fail_delay_min_secs.unwrap_or(60);
    let fail_max = opts.fail_delay_max_secs.unwrap_or(180).max(fail_min);
    for (idx, line) in lines.iter().enumerate() {
        if job_cancelled(&db, &job_id).await {
            break;
        }
        let index = idx + 1;
        let entry = match parse_login_line(line) {
            Ok(entry) => entry,
            Err(err) => {
                let _ = sqlx::query(
                    "UPDATE login_jobs SET failed_count=failed_count+1, updated_at=? WHERE id=?",
                )
                .bind(now())
                .bind(&job_id)
                .execute(&db)
                .await;
                let _ = add_job_event(
                    &db,
                    &job_id,
                    "failed",
                    json!({
                        "index": index,
                        "emailMasked": mask_email(line),
                        "errorCode": "ERR_FORMAT_INVALID",
                        "message": err,
                        "authFail": false
                    }),
                )
                .await;
                continue;
            }
        };
        let email_masked = mask_email(&entry.email);
        let _ = add_job_event(
            &db,
            &job_id,
            "progress",
            json!({"index": index, "total": lines.len(), "emailMasked": email_masked, "status": "running"}),
        )
        .await;

        let result = match check_login_locked(&db, &entry.email).await {
            Some(seconds) => Err(WindsurfLoginError {
                code: "ERR_EMAIL_LOCKED".to_string(),
                message: format!("该账号已被本地暂停尝试，请 {} 秒后再试", seconds),
                auth_fail: false,
                retry_after_secs: Some(seconds),
            }),
            None => windsurf_login(&entry).await,
        };

        let success = result.is_ok();
        match result {
            Ok(login) => {
                let account_id = upsert_logged_in_account(&db, &entry, login).await;
                let _ = clear_login_failures(&db, &entry.email).await;
                let _ = sqlx::query(
                    "UPDATE login_jobs SET success_count=success_count+1, updated_at=? WHERE id=?",
                )
                .bind(now())
                .bind(&job_id)
                .execute(&db)
                .await;
                let _ = add_job_event(
                    &db,
                    &job_id,
                    "success",
                    json!({"index": index, "emailMasked": email_masked, "accountId": account_id}),
                )
                .await;
            }
            Err(err) => {
                if err.auth_fail {
                    let _ = record_login_failure(&db, &entry.email, &err.message).await;
                }
                let _ = sqlx::query(
                    "UPDATE login_jobs SET failed_count=failed_count+1, updated_at=? WHERE id=?",
                )
                .bind(now())
                .bind(&job_id)
                .execute(&db)
                .await;
                let _ = add_job_event(
                    &db,
                    &job_id,
                    "failed",
                    json!({
                        "index": index,
                        "emailMasked": email_masked,
                        "errorCode": err.code,
                        "message": err.message,
                        "authFail": err.auth_fail,
                        "retryAfterSecs": err.retry_after_secs
                    }),
                )
                .await;
            }
        }
        if idx + 1 < lines.len() {
            let wait = if success {
                rand::rng().random_range(delay_min..=delay_max)
            } else {
                rand::rng().random_range(fail_min..=fail_max)
            };
            let _ = add_job_event(
                &db,
                &job_id,
                "waiting",
                json!({"seconds": wait, "reason": if success { "normal" } else { "failed" }}),
            )
            .await;
            tokio::time::sleep(StdDuration::from_secs(wait)).await;
        }
    }
    if !job_cancelled(&db, &job_id).await {
        let now_text = now();
        let _ = sqlx::query(
            "UPDATE login_jobs SET status='completed', updated_at=?, completed_at=? WHERE id=?",
        )
        .bind(&now_text)
        .bind(&now_text)
        .bind(&job_id)
        .execute(&db)
        .await;
        let done = sqlx::query("SELECT success_count, failed_count FROM login_jobs WHERE id=?")
            .bind(&job_id)
            .fetch_optional(&db)
            .await
            .ok()
            .flatten();
        let success_count = done
            .as_ref()
            .map(|row| row.get::<i64, _>("success_count"))
            .unwrap_or(0);
        let failed_count = done
            .as_ref()
            .map(|row| row.get::<i64, _>("failed_count"))
            .unwrap_or(0);
        let _ = add_job_event(
            &db,
            &job_id,
            "done",
            json!({"successCount": success_count, "failedCount": failed_count}),
        )
        .await;
    }
}

async fn job_cancelled(db: &SqlitePool, job_id: &str) -> bool {
    sqlx::query("SELECT cancelled FROM login_jobs WHERE id=?")
        .bind(job_id)
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .map(|row| row.get::<i64, _>("cancelled") != 0)
        .unwrap_or(true)
}

async fn add_job_event(
    db: &SqlitePool,
    job_id: &str,
    event_type: &str,
    payload: Value,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO login_job_events (job_id, event_type, payload, created_at) VALUES (?, ?, ?, ?)")
        .bind(job_id)
        .bind(event_type)
        .bind(payload.to_string())
        .bind(now())
        .execute(db)
        .await?;
    Ok(())
}

fn parse_login_line(line: &str) -> Result<LoginEntry, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.as_slice() {
        [email, password] if email.contains('@') && !password.is_empty() => Ok(LoginEntry {
            email: (*email).to_string(),
            password: (*password).to_string(),
            proxy: None,
        }),
        [proxy, email, password, ..]
            if looks_like_proxy(proxy) && email.contains('@') && !password.is_empty() =>
        {
            Ok(LoginEntry {
                email: (*email).to_string(),
                password: (*password).to_string(),
                proxy: Some((*proxy).to_string()),
            })
        }
        _ => Err("每行需要填写邮箱和密码，代理可放在最前面".to_string()),
    }
}

fn looks_like_proxy(value: &str) -> bool {
    value.contains("://") || value.matches(':').count() >= 1
}

fn mask_email(value: &str) -> String {
    let email = value
        .split_whitespace()
        .find(|part| part.contains('@'))
        .unwrap_or(value);
    let Some((name, domain)) = email.split_once('@') else {
        return "***".to_string();
    };
    let prefix: String = name.chars().take(2).collect();
    format!("{}***@{}", prefix, domain)
}

fn mask_secret(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= 10 {
        return "***".to_string();
    }
    let head: String = chars.iter().take(8).collect();
    let tail: String = chars
        .iter()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{}...{}", head, tail)
}

async fn check_login_locked(db: &SqlitePool, email: &str) -> Option<u64> {
    let row = sqlx::query("SELECT locked_until FROM login_lockouts WHERE email=?")
        .bind(email.to_lowercase())
        .fetch_optional(db)
        .await
        .ok()
        .flatten()?;
    let locked_until = row.get::<Option<String>, _>("locked_until")?;
    let locked = chrono::DateTime::parse_from_rfc3339(&locked_until)
        .ok()?
        .with_timezone(&Utc);
    if locked > Utc::now() {
        Some((locked - Utc::now()).num_seconds().max(1) as u64)
    } else {
        None
    }
}

async fn record_login_failure(db: &SqlitePool, email: &str, reason: &str) -> anyhow::Result<()> {
    let key = email.to_lowercase();
    let now_text = now();
    let row = sqlx::query("SELECT failure_count FROM login_lockouts WHERE email=?")
        .bind(&key)
        .fetch_optional(db)
        .await?;
    let count = row
        .map(|row| row.get::<i64, _>("failure_count"))
        .unwrap_or(0)
        + 1;
    let locked_until = if count >= 3 {
        Some((Utc::now() + Duration::minutes(15)).to_rfc3339())
    } else {
        None
    };
    let stored_count = if locked_until.is_some() { 0 } else { count };
    sqlx::query("INSERT OR REPLACE INTO login_lockouts (email, failure_count, locked_until, last_reason, last_activity) VALUES (?, ?, ?, ?, ?)")
        .bind(key)
        .bind(stored_count)
        .bind(locked_until)
        .bind(reason.chars().take(120).collect::<String>())
        .bind(now_text)
        .execute(db)
        .await?;
    Ok(())
}

async fn clear_login_failures(db: &SqlitePool, email: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM login_lockouts WHERE email=?")
        .bind(email.to_lowercase())
        .execute(db)
        .await?;
    Ok(())
}

struct ManualAccountInput<'a> {
    email: &'a str,
    label: Option<&'a str>,
    api_key: &'a str,
    auth_method: &'a str,
    api_server_url: Option<&'a str>,
    proxy_id: Option<i64>,
    priority: i64,
    max_concurrent: i64,
    extra_credentials: Value,
}

async fn resolve_proxy_id(
    db: &SqlitePool,
    proxy_id: Option<i64>,
    proxy_url: Option<&str>,
) -> Result<Option<i64>, Response> {
    if let Some(id) = proxy_id {
        return Ok(Some(id));
    }
    let Some(proxy_url) = proxy_url.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    Ok(ensure_proxy(db, proxy_url).await)
}

async fn create_placeholder_account(
    db: &SqlitePool,
    email: &str,
    label: Option<&str>,
    proxy_id: Option<i64>,
    priority: i64,
    max_concurrent: i64,
) -> Result<i64, String> {
    let now_text = now();
    sqlx::query("INSERT INTO accounts (email, label, status, priority, max_concurrent, proxy_id, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)")
        .bind(email)
        .bind(label)
        .bind(priority)
        .bind(max_concurrent)
        .bind(proxy_id)
        .bind(&now_text)
        .bind(&now_text)
        .execute(db)
        .await
        .map(|done| done.last_insert_rowid())
        .map_err(|err| err.to_string())
}

async fn upsert_manual_account(
    db: &SqlitePool,
    input: ManualAccountInput<'_>,
) -> Result<i64, String> {
    if input.api_key.trim().is_empty() {
        return Err("凭据不能为空".to_string());
    }
    let now_text = now();
    let credential_mask = mask_secret(input.api_key);
    let credentials = json!({
        "kind": input.auth_method,
        "apiKey": input.api_key,
        "source": input.extra_credentials.get("source").and_then(Value::as_str).unwrap_or(input.auth_method),
        "extra": input.extra_credentials
    });
    let existing = sqlx::query("SELECT id FROM accounts WHERE credential_mask=? OR lower(email)=lower(?) ORDER BY id DESC LIMIT 1")
        .bind(&credential_mask)
        .bind(input.email)
        .fetch_optional(db)
        .await
        .map_err(|err| err.to_string())?;
    if let Some(row) = existing {
        let id = row.get::<i64, _>("id");
        sqlx::query("UPDATE accounts SET email=?, label=?, status='ready', priority=?, max_concurrent=?, proxy_id=?, credentials_json=?, credential_mask=?, auth_method=?, api_server_url=?, last_error=NULL, updated_at=? WHERE id=?")
            .bind(input.email)
            .bind(input.label)
            .bind(input.priority)
            .bind(input.max_concurrent)
            .bind(input.proxy_id)
            .bind(credentials.to_string())
            .bind(credential_mask)
            .bind(input.auth_method)
            .bind(input.api_server_url)
            .bind(&now_text)
            .bind(id)
            .execute(db)
            .await
            .map_err(|err| err.to_string())?;
        Ok(id)
    } else {
        sqlx::query("INSERT INTO accounts (email, label, status, priority, max_concurrent, proxy_id, credentials_json, credential_mask, auth_method, api_server_url, created_at, updated_at) VALUES (?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(input.email)
            .bind(input.label)
            .bind(input.priority)
            .bind(input.max_concurrent)
            .bind(input.proxy_id)
            .bind(credentials.to_string())
            .bind(credential_mask)
            .bind(input.auth_method)
            .bind(input.api_server_url)
            .bind(&now_text)
            .bind(&now_text)
            .execute(db)
            .await
            .map(|done| done.last_insert_rowid())
            .map_err(|err| err.to_string())
    }
}

async fn upsert_logged_in_account(
    db: &SqlitePool,
    entry: &LoginEntry,
    login: WindsurfLoginSuccess,
) -> Option<i64> {
    upsert_logged_in_account_with_options(db, entry, login, None, None, 0, 1).await
}

async fn upsert_logged_in_account_with_options(
    db: &SqlitePool,
    entry: &LoginEntry,
    login: WindsurfLoginSuccess,
    label: Option<&str>,
    fixed_proxy_id: Option<i64>,
    priority: i64,
    max_concurrent: i64,
) -> Option<i64> {
    let now_text = now();
    let credential_mask = mask_secret(&login.api_key);
    let row =
        sqlx::query("SELECT id FROM accounts WHERE lower(email)=lower(?) ORDER BY id DESC LIMIT 1")
            .bind(&entry.email)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    let proxy_id = match fixed_proxy_id {
        Some(id) => Some(id),
        None => match &entry.proxy {
            Some(proxy) => ensure_proxy(db, proxy).await,
            None => None,
        },
    };
    let label = label.unwrap_or(&login.name);
    if let Some(row) = row {
        let id = row.get::<i64, _>("id");
        let _ = sqlx::query("UPDATE accounts SET label=?, status='ready', priority=?, max_concurrent=?, proxy_id=?, credentials_json=?, credential_mask=?, auth_method=?, api_server_url=?, last_login_at=?, last_error=NULL, updated_at=? WHERE id=?")
            .bind(label)
            .bind(priority)
            .bind(max_concurrent)
            .bind(proxy_id)
            .bind(login.credentials.to_string())
            .bind(credential_mask)
            .bind(login.auth_method)
            .bind(login.api_server_url)
            .bind(&now_text)
            .bind(&now_text)
            .bind(id)
            .execute(db)
            .await;
        Some(id)
    } else {
        sqlx::query("INSERT INTO accounts (email, label, status, priority, max_concurrent, proxy_id, credentials_json, credential_mask, auth_method, api_server_url, last_login_at, created_at, updated_at) VALUES (?, ?, 'ready', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(login.email)
            .bind(label)
            .bind(priority)
            .bind(max_concurrent)
            .bind(proxy_id)
            .bind(login.credentials.to_string())
            .bind(credential_mask)
            .bind(login.auth_method)
            .bind(login.api_server_url)
            .bind(&now_text)
            .bind(&now_text)
            .bind(&now_text)
            .execute(db)
            .await
            .ok()
            .map(|done| done.last_insert_rowid())
    }
}

async fn ensure_proxy(db: &SqlitePool, proxy_url: &str) -> Option<i64> {
    let row = sqlx::query("SELECT id FROM proxies WHERE url=?")
        .bind(proxy_url)
        .fetch_optional(db)
        .await
        .ok()
        .flatten();
    if let Some(row) = row {
        return Some(row.get::<i64, _>("id"));
    }
    let now_text = now();
    sqlx::query("INSERT INTO proxies (name, url, status, created_at, updated_at) VALUES (?, ?, 'unknown', ?, ?)")
        .bind("批量登录代理")
        .bind(proxy_url)
        .bind(&now_text)
        .bind(&now_text)
        .execute(db)
        .await
        .ok()
        .map(|done| done.last_insert_rowid())
}

async fn account_ids(db: &SqlitePool) -> anyhow::Result<Vec<i64>> {
    let rows = sqlx::query("SELECT id FROM accounts ORDER BY id DESC")
        .fetch_all(db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<i64, _>("id"))
        .collect())
}

fn account_api_key(row: &sqlx::sqlite::SqliteRow) -> Option<String> {
    let raw = row.get::<Option<String>, _>("credentials_json");
    account_api_key_from_raw(raw.as_deref())
}

async fn refresh_account_status(
    db: &SqlitePool,
    id: i64,
    include_models: bool,
) -> Result<Value, WindsurfLoginError> {
    let row = sqlx::query("SELECT * FROM accounts WHERE id=?")
        .bind(id)
        .fetch_optional(db)
        .await
        .map_err(|err| upstream_error(500, &err.to_string()))?
        .ok_or_else(|| auth_error("ERR_ACCOUNT_NOT_FOUND", "账号不存在"))?;
    let api_key = account_api_key(&row)
        .ok_or_else(|| auth_error("ERR_CREDENTIAL_MISSING", "该账号没有可用凭据"))?;
    let client = build_windsurf_client(None)?;
    let user_status = match get_user_status(&client, &api_key).await {
        Ok(value) => value,
        Err(err) => {
            let _ = mark_account_error(db, id, &err.message).await;
            return Err(err);
        }
    };
    let rate_limit = check_message_rate_limit(&client, &api_key).await.unwrap_or_else(|_| {
        json!({ "hasCapacity": true, "messagesRemaining": -1, "maxMessages": -1, "retryAfterMs": null })
    });
    let model_configs = if include_models {
        get_model_configs(&client, &api_key)
            .await
            .unwrap_or_else(|_| json!({ "configs": [], "sorts": [], "defaultOverride": null }))
    } else {
        row.get::<Option<String>, _>("available_models_json")
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .map(|models| json!({ "configs": models }))
            .unwrap_or_else(|| json!({ "configs": [] }))
    };
    let credits = normalize_user_status(&user_status);
    let tier = infer_tier(&credits);
    let available_models = normalize_models(&model_configs);
    let tier_models = if available_models
        .as_array()
        .is_some_and(|models| !models.is_empty())
    {
        available_models.clone()
    } else {
        json!([])
    };
    let max_messages = rate_limit
        .get("maxMessages")
        .and_then(Value::as_i64)
        .unwrap_or(-1);
    let remaining = rate_limit
        .get("messagesRemaining")
        .and_then(Value::as_i64)
        .unwrap_or(-1);
    let rpm_limit = if max_messages > 0 { max_messages } else { 60 };
    let rpm_used = if max_messages > 0 && remaining >= 0 {
        (max_messages - remaining).max(0)
    } else {
        0
    };
    let now_text = now();
    sqlx::query("UPDATE accounts SET status='ready', tier=?, rpm_used=?, rpm_limit=?, credits_json=?, user_status_json=?, available_models_json=?, tier_models_json=?, last_probed_at=?, last_error=NULL, updated_at=? WHERE id=?")
        .bind(&tier)
        .bind(rpm_used)
        .bind(rpm_limit)
        .bind(credits.to_string())
        .bind(user_status.to_string())
        .bind(available_models.to_string())
        .bind(tier_models.to_string())
        .bind(&now_text)
        .bind(&now_text)
        .bind(id)
        .execute(db)
        .await
        .map_err(|err| upstream_error(500, &err.to_string()))?;
    Ok(json!({
        "tier": tier,
        "credits": credits,
        "rateLimit": rate_limit,
        "availableModels": available_models,
        "tierModels": tier_models,
        "lastProbedAt": now_text
    }))
}

async fn mark_account_error(db: &SqlitePool, id: i64, message: &str) -> anyhow::Result<()> {
    let now_text = now();
    sqlx::query("UPDATE accounts SET status='error', error_count=error_count+1, last_error=?, updated_at=? WHERE id=?")
        .bind(message.chars().take(200).collect::<String>())
        .bind(now_text)
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

impl AccountScheduler {
    fn new(db: SqlitePool, capacity: CapacitySettings) -> Self {
        Self { db, capacity }
    }

    async fn acquire(
        &self,
        model: &str,
        caller_key: Option<String>,
    ) -> Result<AccountLease, AcquireError> {
        self.cleanup_expired().await.map_err(AcquireError::Db)?;
        let global_used = self.global_inflight().await.map_err(AcquireError::Db)?;
        if self.capacity.global_concurrency > 0 && global_used >= self.capacity.global_concurrency {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: self.capacity.queue_timeout_secs.clamp(1, 300),
                reason: "全局执行槽已满".to_string(),
            });
        }
        let model_used = self.model_inflight(model).await.map_err(AcquireError::Db)?;
        if self.capacity.model_concurrency > 0 && model_used >= self.capacity.model_concurrency {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: 30,
                reason: "当前模型执行槽已满".to_string(),
            });
        }
        if let Some(caller) = caller_key.as_deref() {
            if let Some(account_id) = self
                .sticky_account(caller, model)
                .await
                .map_err(AcquireError::Db)?
            {
                match self
                    .try_reserve_account(account_id, model, caller_key.clone(), true)
                    .await
                {
                    Ok(Some(lease)) => return Ok(lease),
                    Ok(None) => {
                        self.clear_sticky(caller, model)
                            .await
                            .map_err(AcquireError::Db)?;
                    }
                    Err(err) => return Err(AcquireError::Db(err)),
                }
            }
        }

        let rows = sqlx::query("SELECT * FROM accounts WHERE status IN ('ready', 'active', 'ok') ORDER BY priority DESC, id ASC")
            .fetch_all(&self.db)
            .await
            .map_err(|err| AcquireError::Db(err.into()))?;
        let mut candidates = Vec::new();
        let mut retry_after_secs = 60_i64;
        for row in rows {
            let account = scheduler_account_from_row(row);
            let availability = self
                .availability(&account, model)
                .await
                .map_err(AcquireError::Db)?;
            if availability.available {
                candidates.push((account, availability.rpm_used));
            } else if availability.retry_after_secs > 0 {
                retry_after_secs = retry_after_secs.min(availability.retry_after_secs);
            }
        }

        if candidates.is_empty() {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: retry_after_secs.max(1),
                reason: "所有账号都不可用或已达到限制".to_string(),
            });
        }

        candidates.sort_by(|(a, a_used), (b, b_used)| {
            let a_quota = quota_score(a);
            let b_quota = quota_score(b);
            a.current_concurrent
                .cmp(&b.current_concurrent)
                .then_with(|| b_quota.cmp(&a_quota))
                .then_with(|| {
                    let a_remaining = rpm_remaining_ratio(*a_used, a.rpm_limit);
                    let b_remaining = rpm_remaining_ratio(*b_used, b.rpm_limit);
                    b_remaining.cmp(&a_remaining)
                })
                .then_with(|| a.last_used_at.cmp(&b.last_used_at))
        });

        for (account, _) in candidates {
            if let Some(lease) = self
                .try_reserve_loaded_account(account, model, caller_key.clone(), false)
                .await
                .map_err(AcquireError::Db)?
            {
                return Ok(lease);
            }
        }

        Err(AcquireError::TemporarilyUnavailable {
            retry_after_secs: 5,
            reason: "账号并发槽位已满".to_string(),
        })
    }

    async fn mark_success(&self, lease: &mut AccountLease) -> anyhow::Result<()> {
        if let Some(caller) = lease.caller_key.as_deref() {
            self.set_sticky(caller, &lease.model, lease.account_id, &lease.api_key)
                .await?;
        }
        let now_text = now();
        sqlx::query("UPDATE accounts SET error_count=0, last_error=NULL, last_used_at=?, updated_at=? WHERE id=?")
            .bind(&now_text)
            .bind(&now_text)
            .bind(lease.account_id)
            .execute(&self.db)
            .await?;
        self.release(lease).await
    }

    #[allow(dead_code)]
    async fn mark_error(&self, lease: &mut AccountLease, message: &str) -> anyhow::Result<()> {
        mark_account_error(&self.db, lease.account_id, message).await?;
        self.release(lease).await
    }

    #[allow(dead_code)]
    async fn mark_rate_limited(
        &self,
        lease: &mut AccountLease,
        model: Option<&str>,
        retry_after_secs: i64,
        reason: &str,
    ) -> anyhow::Result<()> {
        let limited_until = (Utc::now() + Duration::seconds(retry_after_secs.max(1))).to_rfc3339();
        let now_text = now();
        if let Some(model) = model {
            sqlx::query(
                "INSERT INTO account_model_rate_limits (account_id, model, limited_until, reason, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(account_id, model) DO UPDATE SET limited_until=excluded.limited_until, reason=excluded.reason, updated_at=excluded.updated_at",
            )
            .bind(lease.account_id)
            .bind(model)
            .bind(&limited_until)
            .bind(reason)
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        } else {
            sqlx::query(
                "UPDATE accounts SET rate_limited_until=?, last_error=?, updated_at=? WHERE id=?",
            )
            .bind(&limited_until)
            .bind(reason)
            .bind(&now_text)
            .bind(lease.account_id)
            .execute(&self.db)
            .await?;
        }
        if let Some(caller) = lease.caller_key.as_deref() {
            self.clear_sticky(caller, &lease.model).await?;
        }
        self.release(lease).await
    }

    async fn release(&self, lease: &mut AccountLease) -> anyhow::Result<()> {
        if lease.released {
            return Ok(());
        }
        sqlx::query("UPDATE accounts SET current_concurrent=MAX(current_concurrent - 1, 0), updated_at=? WHERE id=?")
            .bind(now())
            .bind(lease.account_id)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_inflight WHERE reservation_id=?")
            .bind(&lease.reservation_id)
            .execute(&self.db)
            .await?;
        lease.released = true;
        Ok(())
    }

    #[allow(dead_code)]
    async fn refund_reservation(&self, lease: &AccountLease) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM account_rpm_events WHERE reservation_id=?")
            .bind(&lease.reservation_id)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_inflight WHERE reservation_id=?")
            .bind(&lease.reservation_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn clear_rate_limit(&self, account_id: i64) -> anyhow::Result<()> {
        sqlx::query("UPDATE accounts SET rate_limited_until=NULL, updated_at=? WHERE id=?")
            .bind(now())
            .bind(account_id)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_rate_limits WHERE account_id=?")
            .bind(account_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn clear_sticky_for_account(&self, account_id: i64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sticky_sessions WHERE account_id=?")
            .bind(account_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn global_inflight(&self) -> anyhow::Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM account_model_inflight")
            .fetch_one(&self.db)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }

    async fn model_inflight(&self, model: &str) -> anyhow::Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) AS count FROM account_model_inflight WHERE model=?")
            .bind(model)
            .fetch_one(&self.db)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }

    async fn try_reserve_account(
        &self,
        account_id: i64,
        model: &str,
        caller_key: Option<String>,
        sticky: bool,
    ) -> anyhow::Result<Option<AccountLease>> {
        let Some(row) = sqlx::query("SELECT * FROM accounts WHERE id=?")
            .bind(account_id)
            .fetch_optional(&self.db)
            .await?
        else {
            return Ok(None);
        };
        self.try_reserve_loaded_account(scheduler_account_from_row(row), model, caller_key, sticky)
            .await
    }

    async fn try_reserve_loaded_account(
        &self,
        account: SchedulerAccount,
        model: &str,
        caller_key: Option<String>,
        sticky: bool,
    ) -> anyhow::Result<Option<AccountLease>> {
        if !self.availability(&account, model).await?.available {
            return Ok(None);
        }
        let account_limit = if account.max_concurrent > 0 {
            account
                .max_concurrent
                .min(self.capacity.account_concurrency.max(1))
        } else {
            self.capacity.account_concurrency.max(1)
        };
        let updated = sqlx::query(
            "UPDATE accounts
             SET current_concurrent=current_concurrent+1, last_used_at=?, rpm_used=rpm_used+1, updated_at=?
             WHERE id=? AND current_concurrent < ?",
        )
        .bind(now())
        .bind(now())
        .bind(account.id)
        .bind(account_limit)
        .execute(&self.db)
        .await?;
        if updated.rows_affected() == 0 {
            return Ok(None);
        }
        let reservation_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO account_rpm_events (account_id, model, reservation_id, created_at) VALUES (?, ?, ?, ?)")
            .bind(account.id)
            .bind(model)
            .bind(&reservation_id)
            .bind(now())
            .execute(&self.db)
            .await?;
        sqlx::query("INSERT INTO account_model_inflight (reservation_id, account_id, model, created_at) VALUES (?, ?, ?, ?)")
            .bind(&reservation_id)
            .bind(account.id)
            .bind(model)
            .bind(now())
            .execute(&self.db)
            .await?;
        let api_key = account_api_key_from_raw(account.credentials_json.as_deref())
            .ok_or_else(|| anyhow::anyhow!("账号没有可用凭据"))?;
        Ok(Some(AccountLease {
            account_id: account.id,
            email: account.email,
            api_key,
            reservation_id,
            sticky,
            caller_key,
            model: model.to_string(),
            released: false,
        }))
    }

    async fn availability(
        &self,
        account: &SchedulerAccount,
        model: &str,
    ) -> anyhow::Result<AccountAvailability> {
        let now_utc = Utc::now();
        if !["ready", "active", "ok"].contains(&account.status.as_str()) {
            return Ok(AccountAvailability::unavailable("status", 60));
        }
        let account_limit = if account.max_concurrent > 0 {
            account
                .max_concurrent
                .min(self.capacity.account_concurrency.max(1))
        } else {
            self.capacity.account_concurrency.max(1)
        };
        if account.current_concurrent >= account_limit {
            return Ok(AccountAvailability::unavailable("concurrency_full", 5));
        }
        if date_in_future(account.rate_limited_until.as_deref(), now_utc) {
            return Ok(AccountAvailability::unavailable(
                "rate_limited",
                retry_after(account.rate_limited_until.as_deref(), 60),
            ));
        }
        if self.model_limited(account.id, model).await? {
            return Ok(AccountAvailability::unavailable("model_rate_limited", 60));
        }
        if account.rpm_limit <= 0 || account.tier == "expired" {
            return Ok(AccountAvailability::unavailable("tier_expired", 60));
        }
        if model_blocked(account.blocked_models_json.as_deref(), model) {
            return Ok(AccountAvailability::unavailable("model_blocked", 60));
        }
        let used = self.rpm_used(account.id).await?;
        if used >= account.rpm_limit {
            return Ok(AccountAvailability::unavailable("rpm_full", 60));
        }
        if account_api_key_from_raw(account.credentials_json.as_deref()).is_none() {
            return Ok(AccountAvailability::unavailable("credential_missing", 60));
        }
        Ok(AccountAvailability {
            available: true,
            retry_after_secs: 0,
            rpm_used: used,
        })
    }

    async fn rpm_used(&self, account_id: i64) -> anyhow::Result<i64> {
        let cutoff = (Utc::now() - Duration::seconds(60)).to_rfc3339();
        sqlx::query("DELETE FROM account_rpm_events WHERE created_at <= ?")
            .bind(&cutoff)
            .execute(&self.db)
            .await?;
        let row = sqlx::query("SELECT COUNT(*) AS count FROM account_rpm_events WHERE account_id=? AND created_at > ?")
            .bind(account_id)
            .bind(cutoff)
            .fetch_one(&self.db)
            .await?;
        Ok(row.get::<i64, _>("count"))
    }

    async fn model_limited(&self, account_id: i64, model: &str) -> anyhow::Result<bool> {
        let now_text = now();
        sqlx::query("DELETE FROM account_model_rate_limits WHERE limited_until <= ?")
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        let row = sqlx::query(
            "SELECT limited_until FROM account_model_rate_limits WHERE account_id=? AND model=?",
        )
        .bind(account_id)
        .bind(model)
        .fetch_optional(&self.db)
        .await?;
        Ok(row
            .and_then(|row| row.get::<Option<String>, _>("limited_until"))
            .is_some_and(|value| date_in_future(Some(&value), Utc::now())))
    }

    async fn sticky_account(&self, caller_key: &str, model: &str) -> anyhow::Result<Option<i64>> {
        let now_text = now();
        sqlx::query("DELETE FROM sticky_sessions WHERE expires_at <= ?")
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        let row = sqlx::query("SELECT account_id FROM sticky_sessions WHERE caller_key=? AND model=? AND expires_at > ?")
            .bind(caller_key)
            .bind(model)
            .bind(&now_text)
            .fetch_optional(&self.db)
            .await?;
        if let Some(row) = row {
            sqlx::query("UPDATE sticky_sessions SET last_used_at=? WHERE caller_key=? AND model=?")
                .bind(&now_text)
                .bind(caller_key)
                .bind(model)
                .execute(&self.db)
                .await?;
            Ok(Some(row.get::<i64, _>("account_id")))
        } else {
            Ok(None)
        }
    }

    async fn set_sticky(
        &self,
        caller_key: &str,
        model: &str,
        account_id: i64,
        api_key: &str,
    ) -> anyhow::Result<()> {
        let now_text = now();
        let expires_at = (Utc::now()
            + Duration::minutes(self.capacity.sticky_session_minutes.max(1)))
        .to_rfc3339();
        sqlx::query(
            "INSERT INTO sticky_sessions (caller_key, model, account_id, api_key_hash, created_at, last_used_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(caller_key, model) DO UPDATE SET account_id=excluded.account_id, api_key_hash=excluded.api_key_hash, last_used_at=excluded.last_used_at, expires_at=excluded.expires_at",
        )
        .bind(caller_key)
        .bind(model)
        .bind(account_id)
        .bind(sha256_hex(api_key))
        .bind(&now_text)
        .bind(&now_text)
        .bind(expires_at)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn clear_sticky(&self, caller_key: &str, model: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sticky_sessions WHERE caller_key=? AND model=?")
            .bind(caller_key)
            .bind(model)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn cleanup_expired(&self) -> anyhow::Result<()> {
        let now_text = now();
        sqlx::query("DELETE FROM sticky_sessions WHERE expires_at <= ?")
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_rate_limits WHERE limited_until <= ?")
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct AccountAvailability {
    available: bool,
    retry_after_secs: i64,
    rpm_used: i64,
}

impl AccountAvailability {
    fn unavailable(_reason: &str, retry_after_secs: i64) -> Self {
        Self {
            available: false,
            retry_after_secs,
            rpm_used: 0,
        }
    }
}

fn scheduler_account_from_row(row: sqlx::sqlite::SqliteRow) -> SchedulerAccount {
    SchedulerAccount {
        id: row.get::<i64, _>("id"),
        email: row.get::<String, _>("email"),
        status: row.get::<String, _>("status"),
        tier: row.get::<String, _>("tier"),
        max_concurrent: row.get::<i64, _>("max_concurrent"),
        current_concurrent: row.get::<i64, _>("current_concurrent"),
        last_used_at: row.get::<Option<String>, _>("last_used_at"),
        rate_limited_until: row.get::<Option<String>, _>("rate_limited_until"),
        rpm_limit: row.get::<i64, _>("rpm_limit"),
        credits_json: row.get::<Option<String>, _>("credits_json"),
        blocked_models_json: row.get::<Option<String>, _>("blocked_models_json"),
        credentials_json: row.get::<Option<String>, _>("credentials_json"),
    }
}

fn account_api_key_from_raw(raw: Option<&str>) -> Option<String> {
    let value = serde_json::from_str::<Value>(raw?).ok()?;
    value
        .get("apiKey")
        .or_else(|| value.pointer("/extra/apiKey"))
        .or_else(|| value.get("sessionToken"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn date_in_future(value: Option<&str>, now_utc: chrono::DateTime<Utc>) -> bool {
    value
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|time| time.with_timezone(&Utc) > now_utc)
        .unwrap_or(false)
}

fn retry_after(value: Option<&str>, fallback: i64) -> i64 {
    value
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|time| (time.with_timezone(&Utc) - Utc::now()).num_seconds().max(1))
        .unwrap_or(fallback)
}

fn model_blocked(raw: Option<&str>, model: &str) -> bool {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .map(|models| models.iter().any(|item| item == model))
        .unwrap_or(false)
}

fn quota_score(account: &SchedulerAccount) -> i64 {
    let Some(raw) = account.credits_json.as_deref() else {
        return 100;
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return 100;
    };
    let daily = value
        .get("dailyPercent")
        .and_then(Value::as_f64)
        .unwrap_or(100.0);
    let weekly = value
        .get("weeklyPercent")
        .and_then(Value::as_f64)
        .unwrap_or(100.0);
    daily.min(weekly).clamp(0.0, 100.0).round() as i64
}

fn rpm_remaining_ratio(used: i64, limit: i64) -> i64 {
    if limit <= 0 {
        return 0;
    }
    (((limit - used).max(0) * 1000) / limit).max(0)
}

fn extract_caller_key(headers: &HeaderMap, payload: &Value) -> Option<String> {
    if let Some(user_id) = payload
        .pointer("/metadata/user_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        if let Some(session) = user_id
            .rsplit("_session_")
            .next()
            .filter(|value| *value != user_id && !value.is_empty())
        {
            return Some(format!("claude:{session}"));
        }
        if user_id.trim_start().starts_with('{') {
            if let Ok(value) = serde_json::from_str::<Value>(user_id) {
                if let Some(session) = value
                    .get("session_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                {
                    return Some(format!("claude:{session}"));
                }
            }
        }
        return Some(format!("metadata:{}", sha256_hex(user_id)));
    }
    for (name, prefix) in [
        ("x-session-id", "header"),
        ("session_id", "codex"),
        ("x-amp-thread-id", "amp"),
        ("x-client-request-id", "clientreq"),
    ] {
        if let Some(value) = headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
        {
            return Some(format!("{prefix}:{value}"));
        }
    }
    let raw = payload.to_string();
    if raw.len() > 8 {
        Some(format!(
            "body:{}",
            sha256_hex(&raw.chars().take(2048).collect::<String>())
        ))
    } else {
        None
    }
}

async fn bind_trace_account(
    db: &SqlitePool,
    trace_id: &str,
    account_id: i64,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE request_traces SET account_id=? WHERE id=?")
        .bind(account_id)
        .bind(trace_id)
        .execute(db)
        .await?;
    Ok(())
}

async fn refresh_rpm_counters(db: &SqlitePool) -> anyhow::Result<()> {
    let cutoff = (Utc::now() - Duration::seconds(60)).to_rfc3339();
    sqlx::query("DELETE FROM account_rpm_events WHERE created_at <= ?")
        .bind(&cutoff)
        .execute(db)
        .await?;
    sqlx::query(
        "UPDATE accounts
         SET rpm_used=(
           SELECT COUNT(*) FROM account_rpm_events
           WHERE account_rpm_events.account_id=accounts.id AND account_rpm_events.created_at > ?
         )",
    )
    .bind(cutoff)
    .execute(db)
    .await?;
    Ok(())
}

async fn account_model_rate_limits(db: &SqlitePool) -> anyhow::Result<HashMap<i64, Value>> {
    let now_text = now();
    let rows = sqlx::query("SELECT account_id, model, limited_until, reason FROM account_model_rate_limits WHERE limited_until > ?")
        .bind(now_text)
        .fetch_all(db)
        .await?;
    let mut grouped: HashMap<i64, serde_json::Map<String, Value>> = HashMap::new();
    for row in rows {
        let account_id = row.get::<i64, _>("account_id");
        let model = row.get::<String, _>("model");
        grouped.entry(account_id).or_default().insert(
            model,
            json!({
                "limitedUntil": row.get::<String, _>("limited_until"),
                "reason": row.get::<Option<String>, _>("reason")
            }),
        );
    }
    Ok(grouped
        .into_iter()
        .map(|(id, values)| (id, Value::Object(values)))
        .collect())
}

async fn account_sticky_counts(db: &SqlitePool) -> anyhow::Result<HashMap<i64, i64>> {
    let now_text = now();
    let rows = sqlx::query("SELECT account_id, COUNT(*) AS count FROM sticky_sessions WHERE expires_at > ? GROUP BY account_id")
        .bind(now_text)
        .fetch_all(db)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get::<i64, _>("account_id"), row.get::<i64, _>("count")))
        .collect())
}

async fn post_cloud_json(
    client: &Client,
    host: &str,
    path: &str,
    api_key: &str,
) -> Result<(u16, Value, String), WindsurfLoginError> {
    let body = json!({ "metadata": cloud_metadata(api_key) });
    let url = format!("https://{}{}", host, path);
    post_json(client, &url, &cloud_fingerprint(), body).await
}

async fn get_user_status(client: &Client, api_key: &str) -> Result<Value, WindsurfLoginError> {
    cloud_dual_post(
        client,
        "/exa.seat_management_pb.SeatManagementService/GetUserStatus",
        api_key,
    )
    .await
}

async fn get_model_configs(client: &Client, api_key: &str) -> Result<Value, WindsurfLoginError> {
    let data = cloud_dual_post(
        client,
        "/exa.api_server_pb.ApiServerService/GetCascadeModelConfigs",
        api_key,
    )
    .await?;
    Ok(json!({
        "configs": data.get("clientModelConfigs").cloned().unwrap_or_else(|| json!([])),
        "sorts": data.get("clientModelSorts").cloned().unwrap_or_else(|| json!([])),
        "defaultOverride": data.get("defaultOverrideModelConfig").cloned().unwrap_or(Value::Null)
    }))
}

async fn check_message_rate_limit(
    client: &Client,
    api_key: &str,
) -> Result<Value, WindsurfLoginError> {
    let data = cloud_dual_post(
        client,
        "/exa.api_server_pb.ApiServerService/CheckUserMessageRateLimit",
        api_key,
    )
    .await?;
    Ok(json!({
        "hasCapacity": data.get("hasCapacity").and_then(Value::as_bool).unwrap_or(true),
        "messagesRemaining": data.get("messagesRemaining").and_then(Value::as_i64).unwrap_or(-1),
        "maxMessages": data.get("maxMessages").and_then(Value::as_i64).unwrap_or(-1),
        "retryAfterMs": data.get("retryAfterMs").cloned().unwrap_or(Value::Null)
    }))
}

async fn cloud_dual_post(
    client: &Client,
    path: &str,
    api_key: &str,
) -> Result<Value, WindsurfLoginError> {
    let hosts = ["server.codeium.com", "server.self-serve.windsurf.com"];
    let mut last = None;
    for host in hosts {
        match post_cloud_json(client, host, path, api_key).await {
            Ok((status, body, _)) if status < 400 => return Ok(body),
            Ok((status, _, raw)) => last = Some(upstream_error(status, &raw)),
            Err(err) => last = Some(err),
        }
    }
    Err(last.unwrap_or_else(|| upstream_error(500, "cloud request failed")))
}

fn cloud_metadata(api_key: &str) -> Value {
    json!({
        "apiKey": api_key,
        "ideName": "windsurf",
        "ideVersion": "1.9600.41",
        "extensionName": "windsurf",
        "extensionVersion": "1.9600.41",
        "locale": "en"
    })
}

fn cloud_fingerprint() -> HashMap<&'static str, String> {
    let mut headers = HashMap::new();
    headers.insert("user-agent", "windsurf/1.9600.41".to_string());
    headers.insert("accept", "application/json".to_string());
    headers
}

fn normalize_user_status(data: &Value) -> Value {
    let plan_status = data
        .pointer("/userStatus/planStatus")
        .unwrap_or(&Value::Null);
    let plan = plan_status.get("planInfo").unwrap_or(&Value::Null);
    let legacy_div = |value: Option<f64>| value.map(|number| number / 100.0);
    let prompt_limit = legacy_div(plan.get("monthlyPromptCredits").and_then(Value::as_f64));
    let prompt_used = legacy_div(plan_status.get("usedPromptCredits").and_then(Value::as_f64));
    let prompt_remaining = legacy_div(
        plan_status
            .get("availablePromptCredits")
            .and_then(Value::as_f64),
    );
    let daily_percent = plan_status
        .get("dailyQuotaRemainingPercent")
        .and_then(Value::as_f64);
    let percent = daily_percent.or_else(|| match (prompt_remaining, prompt_limit) {
        (Some(remaining), Some(limit)) if limit > 0.0 => Some((remaining / limit) * 100.0),
        _ => None,
    });
    let trial_end_ms = data
        .pointer("/userStatus/windsurfProTrialEndTime/seconds")
        .and_then(Value::as_i64)
        .map(|seconds| seconds * 1000)
        .or_else(|| {
            data.pointer("/userStatus/windsurfProTrialEndTime")
                .and_then(Value::as_i64)
                .map(|value| {
                    if value > 1_000_000_000_000 {
                        value
                    } else {
                        value * 1000
                    }
                })
        });
    json!({
        "planName": plan.get("planName").and_then(Value::as_str).unwrap_or("Unknown"),
        "trialEndMs": trial_end_ms,
        "dailyPercent": daily_percent,
        "weeklyPercent": plan_status.get("weeklyQuotaRemainingPercent").and_then(Value::as_f64),
        "dailyResetAt": plan_status.get("dailyQuotaResetAtUnix").cloned().unwrap_or(Value::Null),
        "weeklyResetAt": plan_status.get("weeklyQuotaResetAtUnix").cloned().unwrap_or(Value::Null),
        "overageBalance": plan_status.get("overageBalanceMicros").and_then(Value::as_f64).map(|value| value / 1_000_000.0),
        "prompt": { "limit": prompt_limit, "used": prompt_used, "remaining": prompt_remaining },
        "flex": {
            "limit": legacy_div(plan.get("monthlyFlexCreditPurchaseAmount").and_then(Value::as_f64)),
            "used": legacy_div(plan_status.get("usedFlexCredits").and_then(Value::as_f64)),
            "remaining": legacy_div(plan_status.get("availableFlexCredits").and_then(Value::as_f64))
        },
        "percent": percent,
        "fetchedAt": Utc::now().timestamp_millis()
    })
}

fn infer_tier(credits: &Value) -> String {
    let plan_name = credits
        .get("planName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_lowercase();
    if plan_name.contains("pro") || plan_name.contains("trial") || plan_name.contains("team") {
        "pro".to_string()
    } else if plan_name.contains("free") {
        "free".to_string()
    } else if plan_name.contains("expired") {
        "expired".to_string()
    } else {
        "unknown".to_string()
    }
}

fn normalize_models(model_configs: &Value) -> Value {
    let configs = model_configs
        .get("configs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let models: Vec<Value> = configs
        .into_iter()
        .filter_map(|item| {
            let id = item
                .get("model")
                .or_else(|| item.get("modelName"))
                .or_else(|| item.get("modelUid"))
                .or_else(|| item.get("label"))
                .and_then(Value::as_str)?;
            let label = item.get("label").and_then(Value::as_str).unwrap_or(id);
            Some(json!({
                "id": id,
                "label": label,
                "provider": item.get("provider").and_then(Value::as_str).unwrap_or("windsurf"),
                "creditMultiplier": item.get("creditMultiplier").cloned().unwrap_or(Value::Null),
                "supportsImages": item.get("supportsImages").and_then(Value::as_bool).unwrap_or(false)
            }))
        })
        .collect();
    json!(models)
}

async fn windsurf_login(entry: &LoginEntry) -> Result<WindsurfLoginSuccess, WindsurfLoginError> {
    let client = build_windsurf_client(entry.proxy.as_deref())?;
    let fingerprint = login_fingerprint();
    let method = check_login_method(&client, &fingerprint, &entry.email).await?;
    if method == Some(false) {
        return Err(auth_error(
            "ERR_NO_PASSWORD_SET",
            "该账号没有可用密码，请换用可登录的账号",
        ));
    }
    match auth1_login(&client, &fingerprint, entry).await {
        Ok(success) => Ok(success),
        Err(err) if err.auth_fail => Err(err),
        Err(auth1_err) => match firebase_login(&client, &fingerprint, entry).await {
            Ok(success) => Ok(success),
            Err(firebase_err) if firebase_err.code == "ERR_FIREBASE_APP_CHECK" => Err(auth1_err),
            Err(firebase_err) => Err(firebase_err),
        },
    }
}

fn build_windsurf_client(proxy: Option<&str>) -> Result<Client, WindsurfLoginError> {
    let mut builder = Client::builder().timeout(StdDuration::from_secs(30));
    if let Some(proxy) = proxy.filter(|value| !value.trim().is_empty()) {
        let proxy = Proxy::all(proxy).map_err(|err| WindsurfLoginError {
            code: "ERR_PROXY_INVALID".to_string(),
            message: format!("代理不可用：{}", err),
            auth_fail: false,
            retry_after_secs: None,
        })?;
        builder = builder.proxy(proxy);
    }
    builder.build().map_err(|err| WindsurfLoginError {
        code: "ERR_HTTP_CLIENT".to_string(),
        message: err.to_string(),
        auth_fail: false,
        retry_after_secs: None,
    })
}

fn login_fingerprint() -> HashMap<&'static str, String> {
    let os_versions = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 14_2_1",
        "X11; Linux x86_64",
    ];
    let chrome_versions = ["128.0.0.0", "129.0.0.0", "130.0.0.0", "131.0.0.0"];
    let os = os_versions[rand::rng().random_range(0..os_versions.len())];
    let chrome = chrome_versions[rand::rng().random_range(0..chrome_versions.len())];
    let major = chrome.split('.').next().unwrap_or("130");
    let platform = if os.contains("Windows") {
        "\"Windows\""
    } else if os.contains("Mac") {
        "\"macOS\""
    } else {
        "\"Linux\""
    };
    let mut headers = HashMap::new();
    headers.insert(
        "user-agent",
        format!(
            "Mozilla/5.0 ({}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36",
            os, chrome
        ),
    );
    headers.insert("accept-language", "en-US,en;q=0.9".to_string());
    headers.insert("accept", "application/json, text/plain, */*".to_string());
    headers.insert(
        "sec-ch-ua",
        format!(
            "\"Chromium\";v=\"{}\", \"Google Chrome\";v=\"{}\", \"Not.A/Brand\";v=\"99\"",
            major, major
        ),
    );
    headers.insert("sec-ch-ua-mobile", "?0".to_string());
    headers.insert("sec-ch-ua-platform", platform.to_string());
    headers.insert("origin", "https://windsurf.com".to_string());
    headers.insert("referer", "https://windsurf.com/".to_string());
    headers
}

async fn post_json(
    client: &Client,
    url: &str,
    fingerprint: &HashMap<&'static str, String>,
    body: Value,
) -> Result<(u16, Value, String), WindsurfLoginError> {
    let mut req = client
        .post(url)
        .json(&body)
        .header("connect-protocol-version", "1");
    for (key, value) in fingerprint {
        req = req.header(*key, value);
    }
    let resp = req.send().await.map_err(network_error)?;
    let status = resp.status().as_u16();
    let raw = resp.text().await.map_err(network_error)?;
    let json = serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| json!({ "raw": raw }));
    Ok((status, json, raw))
}

async fn check_login_method(
    client: &Client,
    fingerprint: &HashMap<&'static str, String>,
    email: &str,
) -> Result<Option<bool>, WindsurfLoginError> {
    let primary = post_json(
        client,
        "https://windsurf.com/_backend/exa.seat_management_pb.SeatManagementService/CheckUserLoginMethod",
        fingerprint,
        json!({ "email": email }),
    )
    .await;
    if let Ok((200, body, _)) = primary {
        if let Ok(parsed) = serde_json::from_value::<LoginMethodResponse>(body) {
            if parsed.user_exists == Some(false) {
                return Err(auth_error("ERR_EMAIL_NOT_FOUND", "账号不存在"));
            }
            if parsed.user_exists.is_some() || parsed.has_password.is_some() {
                return Ok(parsed.has_password);
            }
        }
    }

    let (status, body, raw) = post_json(
        client,
        "https://windsurf.com/_devin-auth/connections",
        fingerprint,
        json!({ "product": "windsurf", "email": email }),
    )
    .await?;
    if status >= 500 {
        return Err(upstream_error(status, &raw));
    }
    if let Ok(parsed) = serde_json::from_value::<ConnectionsResponse>(body) {
        if let Some(method) = parsed.auth_method {
            if method.method.as_deref() == Some("auth1") {
                return Ok(method.has_password);
            }
        }
        if let Some(connections) = parsed.connections {
            let email_enabled = connections
                .iter()
                .find(|item| item.kind.as_deref() == Some("email"))
                .and_then(|item| item.enabled)
                .unwrap_or(false);
            return Ok(Some(email_enabled));
        }
    }
    Ok(None)
}

async fn auth1_login(
    client: &Client,
    fingerprint: &HashMap<&'static str, String>,
    entry: &LoginEntry,
) -> Result<WindsurfLoginSuccess, WindsurfLoginError> {
    let (status, body, raw) = post_json(
        client,
        "https://windsurf.com/_devin-auth/password/login",
        fingerprint,
        json!({ "email": entry.email, "password": entry.password }),
    )
    .await?;
    if status >= 500 {
        return Err(upstream_error(status, &raw));
    }
    if status >= 400 {
        return Err(map_auth_error(&body, &raw));
    }
    let auth1_token =
        body.get("token")
            .and_then(Value::as_str)
            .ok_or_else(|| WindsurfLoginError {
                code: "ERR_AUTH1_TOKEN_MISSING".to_string(),
                message: "登录响应缺少 token".to_string(),
                auth_fail: false,
                retry_after_secs: None,
            })?;
    let post_auth = post_auth_dual(client, fingerprint, auth1_token).await?;
    let session_token = post_auth
        .get("sessionToken")
        .and_then(Value::as_str)
        .ok_or_else(|| WindsurfLoginError {
            code: "ERR_POSTAUTH_FAILED".to_string(),
            message: "登录成功后没有拿到会话凭据".to_string(),
            auth_fail: false,
            retry_after_secs: None,
        })?;
    Ok(WindsurfLoginSuccess {
        email: entry.email.clone(),
        name: entry.email.clone(),
        api_key: session_token.to_string(),
        auth_method: "auth1".to_string(),
        api_server_url: None,
        credentials: json!({
            "kind": "session_token",
            "apiKey": session_token,
            "sessionToken": session_token,
            "auth1Token": auth1_token,
            "source": "auth1_postauth"
        }),
    })
}

async fn post_auth_dual(
    client: &Client,
    fingerprint: &HashMap<&'static str, String>,
    auth1_token: &str,
) -> Result<Value, WindsurfLoginError> {
    let urls = [
        "https://windsurf.com/_backend/exa.seat_management_pb.SeatManagementService/WindsurfPostAuth",
        "https://server.self-serve.windsurf.com/exa.seat_management_pb.SeatManagementService/WindsurfPostAuth",
    ];
    let mut last = None;
    for url in urls {
        let mut req = client
            .post(url)
            .header("content-type", "application/proto")
            .header("content-length", "0")
            .header("connect-protocol-version", "1")
            .header("x-devin-auth1-token", auth1_token)
            .header("referer", "https://windsurf.com/account/login")
            .body(Vec::new());
        for (key, value) in fingerprint {
            req = req.header(*key, value);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let raw = resp.text().await.unwrap_or_default();
                let body = serde_json::from_str::<Value>(&raw)
                    .unwrap_or_else(|_| parse_post_auth_raw(&raw));
                if status < 400 && body.get("sessionToken").is_some() {
                    return Ok(body);
                }
                last = Some(WindsurfLoginError {
                    code: "ERR_POSTAUTH_FAILED".to_string(),
                    message: format!(
                        "登录确认失败：HTTP {} {}",
                        status,
                        body.get("error")
                            .or_else(|| body.get("raw"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                    ),
                    auth_fail: status == 401 || status == 403,
                    retry_after_secs: None,
                });
            }
            Err(err) => last = Some(network_error(err)),
        }
    }
    Err(last.unwrap_or_else(|| upstream_error(500, "post auth failed")))
}

fn parse_post_auth_raw(raw: &str) -> Value {
    match find_devin_token(raw) {
        Some(token) => json!({ "sessionToken": token }),
        None => json!({ "raw": raw.chars().take(200).collect::<String>() }),
    }
}

fn find_devin_token(raw: &str) -> Option<String> {
    let marker = "devin-session-token$";
    let start = raw.find(marker)?;
    let token = raw[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '$' | '.' | '_' | '-'))
        .collect::<String>();
    if token.len() > marker.len() {
        Some(token)
    } else {
        None
    }
}

async fn firebase_login(
    client: &Client,
    fingerprint: &HashMap<&'static str, String>,
    entry: &LoginEntry,
) -> Result<WindsurfLoginSuccess, WindsurfLoginError> {
    let url = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=AIzaSyDsOl-1XpT5err0Tcnx8FFod1H8gVGIycY";
    let (status, body, raw) = post_json(
        client,
        url,
        fingerprint,
        json!({ "email": entry.email, "password": entry.password, "returnSecureToken": true }),
    )
    .await?;
    if status >= 400 || body.get("error").is_some() {
        return Err(map_auth_error(&body, &raw));
    }
    let id_token = body
        .get("idToken")
        .and_then(Value::as_str)
        .ok_or_else(|| auth_error("ERR_FIREBASE_TOKEN_MISSING", "登录响应缺少凭据"))?;
    let refresh_token = body
        .get("refreshToken")
        .and_then(Value::as_str)
        .unwrap_or("");
    let reg = register_with_firebase(client, fingerprint, id_token).await?;
    let api_key = reg
        .get("api_key")
        .or_else(|| reg.get("apiKey"))
        .and_then(Value::as_str)
        .ok_or_else(|| WindsurfLoginError {
            code: "ERR_CODEIUM_REGISTER_FAILED".to_string(),
            message: "登录成功后没有拿到可用 key".to_string(),
            auth_fail: false,
            retry_after_secs: None,
        })?;
    let name = reg
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(&entry.email)
        .to_string();
    let api_server_url = reg
        .get("api_server_url")
        .or_else(|| reg.get("apiServerUrl"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(WindsurfLoginSuccess {
        email: entry.email.clone(),
        name,
        api_key: api_key.to_string(),
        auth_method: "firebase".to_string(),
        api_server_url: api_server_url.clone(),
        credentials: json!({
            "kind": "firebase",
            "apiKey": api_key,
            "idToken": id_token,
            "refreshToken": refresh_token,
            "source": "firebase_register"
        }),
    })
}

async fn register_with_firebase(
    client: &Client,
    fingerprint: &HashMap<&'static str, String>,
    id_token: &str,
) -> Result<Value, WindsurfLoginError> {
    let urls = [
        "https://register.windsurf.com/exa.seat_management_pb.SeatManagementService/RegisterUser",
        "https://api.codeium.com/register_user/",
    ];
    let mut last = None;
    for url in urls {
        match post_json(
            client,
            url,
            fingerprint,
            json!({ "firebase_id_token": id_token }),
        )
        .await
        {
            Ok((status, body, _))
                if status < 400
                    && (body.get("api_key").is_some() || body.get("apiKey").is_some()) =>
            {
                return Ok(body);
            }
            Ok((status, _, raw)) => last = Some(upstream_error(status, &raw)),
            Err(err) => last = Some(err),
        }
    }
    Err(last.unwrap_or_else(|| upstream_error(500, "register failed")))
}

fn map_auth_error(body: &Value, raw: &str) -> WindsurfLoginError {
    let detail = body
        .pointer("/error/message")
        .or_else(|| body.get("detail"))
        .and_then(Value::as_str)
        .unwrap_or(raw)
        .chars()
        .take(200)
        .collect::<String>();
    let lower_detail = detail.to_lowercase();
    let code = if detail.contains("EMAIL_NOT_FOUND") {
        "ERR_EMAIL_NOT_FOUND"
    } else if detail.contains("INVALID_PASSWORD") {
        "ERR_INVALID_PASSWORD"
    } else if detail.contains("INVALID_LOGIN_CREDENTIALS")
        || lower_detail.contains("invalid email or password")
    {
        "ERR_INVALID_CREDENTIALS"
    } else if detail.contains("TOO_MANY_ATTEMPTS") {
        "ERR_TOO_MANY_ATTEMPTS"
    } else if detail.contains("INVALID_EMAIL") {
        "ERR_INVALID_EMAIL"
    } else if lower_detail.contains("app check") {
        "ERR_FIREBASE_APP_CHECK"
    } else {
        "ERR_LOGIN_FAILED"
    };
    auth_error(code, &detail)
}

fn auth_error(code: &str, message: &str) -> WindsurfLoginError {
    WindsurfLoginError {
        code: code.to_string(),
        message: message.to_string(),
        auth_fail: matches!(
            code,
            "ERR_EMAIL_NOT_FOUND"
                | "ERR_INVALID_PASSWORD"
                | "ERR_INVALID_CREDENTIALS"
                | "ERR_INVALID_EMAIL"
                | "ERR_LOGIN_FAILED"
                | "ERR_NO_PASSWORD_SET"
        ),
        retry_after_secs: if code == "ERR_TOO_MANY_ATTEMPTS" {
            Some(900)
        } else {
            None
        },
    }
}

fn upstream_error(status: u16, raw: &str) -> WindsurfLoginError {
    WindsurfLoginError {
        code: format!("ERR_UPSTREAM_{}", status),
        message: format!(
            "Windsurf 返回异常：HTTP {} {}",
            status,
            raw.chars().take(120).collect::<String>()
        ),
        auth_fail: false,
        retry_after_secs: None,
    }
}

fn network_error(err: reqwest::Error) -> WindsurfLoginError {
    WindsurfLoginError {
        code: "ERR_NETWORK".to_string(),
        message: err.to_string(),
        auth_fail: false,
        retry_after_secs: None,
    }
}

async fn admin_key_hash(db: &SqlitePool) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT value FROM settings WHERE key = 'admin_key_hash'")
        .fetch_optional(db)
        .await?;
    Ok(row.map(|row| row.get::<String, _>("value")))
}

async fn require_admin(db: &SqlitePool, headers: &HeaderMap) -> Result<(), Response> {
    if admin_key_hash(db).await.ok().flatten().is_none() {
        return Err(error(
            StatusCode::FORBIDDEN,
            "setup_required",
            "请先完成初始化",
        ));
    }
    if let Some(key) = headers
        .get("x-admin-key")
        .and_then(|value| value.to_str().ok())
    {
        if let Some(expected) = admin_key_hash(db).await.ok().flatten() {
            if sha256_hex(key.trim()) == expected {
                return Ok(());
            }
        }
    }
    let token = bearer_token(headers)
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "unauthorized", "请先登录"))?;
    let token_hash = sha256_hex(&token);
    let exists =
        sqlx::query("SELECT token_hash FROM admin_sessions WHERE token_hash=? AND expires_at > ?")
            .bind(token_hash)
            .bind(now())
            .fetch_optional(db)
            .await
            .map_err(|err| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "db_error",
                    &err.to_string(),
                )
            })?
            .is_some();
    if exists {
        Ok(())
    } else {
        Err(error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "登录已过期",
        ))
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string)
}

fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    (
        status,
        Json(ApiError {
            error: ErrorBody {
                kind: kind.to_string(),
                message: message.to_string(),
            },
        }),
    )
        .into_response()
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn is_legacy_static_tier_models(value: &Value) -> bool {
    let Some(models) = value.as_array() else {
        return false;
    };
    if models.is_empty() {
        return false;
    }
    models.iter().all(|model| {
        let id = model
            .as_str()
            .or_else(|| model.get("id").and_then(Value::as_str))
            .unwrap_or("");
        matches!(id, "claude-opus-4-1" | "claude-sonnet-4-5")
    })
}

fn account_json(
    row: sqlx::sqlite::SqliteRow,
    model_limits: &HashMap<i64, Value>,
    sticky_counts: &HashMap<i64, i64>,
) -> Value {
    let parse_json = |name: &str, fallback: Value| {
        row.get::<Option<String>, _>(name)
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .unwrap_or(fallback)
    };
    let id = row.get::<i64, _>("id");
    let full_key = account_api_key(&row);
    let available_models = parse_json("available_models_json", json!([]));
    let stored_tier_models = parse_json("tier_models_json", json!([]));
    let tier_models = if is_legacy_static_tier_models(&stored_tier_models)
        && available_models
            .as_array()
            .is_some_and(|models| !models.is_empty())
    {
        available_models.clone()
    } else {
        stored_tier_models
    };
    json!({
        "id": id,
        "email": row.get::<String, _>("email"),
        "label": row.get::<Option<String>, _>("label"),
        "status": row.get::<String, _>("status"),
        "tier": row.get::<String, _>("tier"),
        "tierManual": row.get::<i64, _>("tier_manual") != 0,
        "errorCount": row.get::<i64, _>("error_count"),
        "priority": row.get::<i64, _>("priority"),
        "maxConcurrent": row.get::<i64, _>("max_concurrent"),
        "currentConcurrent": row.get::<i64, _>("current_concurrent"),
        "proxyId": row.get::<Option<i64>, _>("proxy_id"),
        "cooldownUntil": row.get::<Option<String>, _>("cooldown_until"),
        "lastUsed": row.get::<Option<String>, _>("last_used_at"),
        "lastProbed": row.get::<Option<String>, _>("last_probed_at"),
        "rateLimitedUntil": row.get::<Option<String>, _>("rate_limited_until"),
        "rateLimited": row.get::<Option<String>, _>("rate_limited_until").is_some_and(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|time| time.with_timezone(&Utc) > Utc::now())
                .unwrap_or(false)
        }),
        "rpmUsed": row.get::<i64, _>("rpm_used"),
        "rpmLimit": row.get::<i64, _>("rpm_limit"),
        "credits": parse_json("credits_json", Value::Null),
        "userStatus": parse_json("user_status_json", Value::Null),
        "availableModels": available_models,
        "tierModels": tier_models,
        "blockedModels": parse_json("blocked_models_json", json!([])),
        "modelRateLimits": model_limits.get(&id).cloned().unwrap_or_else(|| json!({})),
        "stickyCount": sticky_counts.get(&id).copied().unwrap_or(0),
        "lastError": row.get::<Option<String>, _>("last_error"),
        "credentialMask": row.get::<Option<String>, _>("credential_mask"),
        "apiKey": full_key,
        "authMethod": row.get::<Option<String>, _>("auth_method"),
        "apiServerUrl": row.get::<Option<String>, _>("api_server_url"),
        "lastLoginAt": row.get::<Option<String>, _>("last_login_at"),
        "createdAt": row.get::<String, _>("created_at"),
        "updatedAt": row.get::<String, _>("updated_at")
    })
}

fn trace_json(row: sqlx::sqlite::SqliteRow) -> Value {
    json!({
        "id": row.get::<String, _>("id"),
        "model": row.get::<Option<String>, _>("model"),
        "stream": row.get::<i64, _>("stream") != 0,
        "accountId": row.get::<Option<i64>, _>("account_id"),
        "status": row.get::<String, _>("status"),
        "endReason": row.get::<Option<String>, _>("end_reason"),
        "errorSummary": row.get::<Option<String>, _>("error_summary"),
        "startedAt": row.get::<String, _>("started_at"),
        "endedAt": row.get::<Option<String>, _>("ended_at")
    })
}

async fn create_trace(
    db: &SqlitePool,
    id: &str,
    model: Option<&str>,
    stream: bool,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO request_traces (id, model, stream, status, started_at) VALUES (?, ?, ?, 'running', ?)")
        .bind(id)
        .bind(model)
        .bind(if stream { 1 } else { 0 })
        .bind(now())
        .execute(db)
        .await?;
    Ok(())
}

async fn add_trace_chunk(
    db: &SqlitePool,
    trace_id: &str,
    layer: &str,
    payload: &Value,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO request_trace_chunks (trace_id, layer, payload, created_at) VALUES (?, ?, ?, ?)")
        .bind(trace_id)
        .bind(layer)
        .bind(payload.to_string())
        .bind(now())
        .execute(db)
        .await?;
    Ok(())
}

async fn finish_trace(
    db: &SqlitePool,
    id: &str,
    status: &str,
    end_reason: Option<&str>,
    error_summary: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE request_traces SET status=?, end_reason=?, error_summary=?, ended_at=? WHERE id=?",
    )
    .bind(status)
    .bind(end_reason)
    .bind(error_summary)
    .bind(now())
    .bind(id)
    .execute(db)
    .await?;
    Ok(())
}
