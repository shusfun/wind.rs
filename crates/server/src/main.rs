use anyhow::Context;
use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, get_service, patch, post},
};
use chrono::{DateTime, Duration, FixedOffset, Utc};
use clap::Parser;
use rand::Rng;
use regex::Regex;
use reqwest::{Client, Proxy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Component, PathBuf},
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, broadcast};
use tower_http::{
    cors::CorsLayer,
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

mod engine;
use engine::{
    EngineAccount, EngineConfig, EngineMessage, EngineModel, EnginePreflightFailure,
    EngineSamplingParams, EngineTool, EngineToolChoice, RemoteApiEngine, SystemPromptMode,
};

const LOG_RETENTION_DAYS: u64 = 7;
const TRACE_PAYLOAD_PREVIEW_CHARS: usize = 1200;

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
    engine: RemoteApiEngine,
    events: broadcast::Sender<AdminEvent>,
    branch_gate: BranchGate,
    data_dir: PathBuf,
}

#[derive(Clone, Default)]
struct BranchGate {
    inner: Arc<Mutex<HashMap<String, BranchGateState>>>,
}

#[derive(Debug, Clone)]
struct BranchGateState {
    recent_tool_request_at: Instant,
    fingerprints: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchGateDecision {
    Allow,
    SuppressNoToolBranch,
}

#[derive(Clone)]
struct AccountScheduler {
    db: SqlitePool,
    capacity: CapacitySettings,
    events: broadcast::Sender<AdminEvent>,
}

#[derive(Debug, Clone)]
struct AccountLease {
    account_id: i64,
    email: String,
    api_key: String,
    jwt_token: Option<String>,
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
    tier_manual: bool,
    max_concurrent: i64,
    current_concurrent: i64,
    last_used_at: Option<String>,
    rate_limited_until: Option<String>,
    rate_limit_probe_after: Option<String>,
    rpm_limit: i64,
    credits_json: Option<String>,
    user_status_json: Option<String>,
    available_models_json: Option<String>,
    tier_models_json: Option<String>,
    blocked_models_json: Option<String>,
    credentials_json: Option<String>,
}

#[derive(Debug, Clone)]
struct AccountCredentials {
    api_key: String,
    jwt_token: Option<String>,
}

#[derive(Debug)]
enum AcquireError {
    #[allow(dead_code)]
    NoAccount,
    TemporarilyUnavailable {
        retry_after_secs: i64,
        reason: String,
        upstream_error: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminEvent {
    kind: String,
    payload: Value,
    created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AvailabilityKind {
    Available,
    Probing,
    AccountRateLimited,
    ModelRateLimited,
    RpmFull,
    TierExpired,
    ModelBlocked,
    CredentialMissing,
    ConcurrencyFull,
    StatusError,
    StatusDisabled,
    StatusBanned,
    StatusUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpstreamRateLimitScope {
    Model,
    Account,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UpstreamRateLimit {
    scope: UpstreamRateLimitScope,
    retry_after_secs: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccountFailureAction {
    RateLimit(UpstreamRateLimit),
    FatalAccountError,
    TransientRecordOnly,
    ReleaseOnly,
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateClientApiKeyRequest {
    name: Option<String>,
    key: Option<String>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateClientApiKeyRequest {
    name: Option<String>,
    key: Option<String>,
    enabled: Option<bool>,
}

#[derive(Debug, Clone)]
struct LoginEntry {
    email: String,
    password: String,
    proxy: Option<String>,
}

#[derive(Debug)]
struct ExistingLoginAccount {
    id: i64,
    normal: bool,
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
    max_tokens: Option<u64>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<u64>,
    frequency_penalty: Option<f64>,
    presence_penalty: Option<f64>,
    system: Option<Value>,
    tools: Option<Value>,
    tool_choice: Option<Value>,
    #[serde(default)]
    metadata: Value,
    #[serde(default)]
    messages: Value,
}

#[derive(Debug, Clone)]
struct MessageDebugSummary {
    message_count: usize,
    roles: Vec<String>,
    system_chars: usize,
    tool_count: usize,
    has_tool_choice: bool,
    metadata_keys: Vec<String>,
    input_chars: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountProbeRequest {
    model: String,
    message: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ModelControlSettings {
    default_model: Option<String>,
    disabled_models: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct StatsQuery {
    range: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveModelControlRequest {
    default_model: String,
    disabled_models: Vec<String>,
}

const RATE_LIMIT_PROBE_INTERVAL_SECS: i64 = 300;
const DEFAULT_SYSTEM_PROMPT_MODE_SETTING: &str = "strip-identity";
const MAX_DYNAMIC_ACCOUNT_RETRIES: i64 = 80;
const DEFAULT_CHAT_MODEL: &str = "claude-opus-4-1";
const DEFAULT_ADMIN_MODEL: &str = "claude-sonnet-4-6";

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

impl BranchGate {
    const WINDOW: StdDuration = StdDuration::from_millis(1500);
    const WAIT_FOR_TOOL_BRANCH: StdDuration = StdDuration::from_millis(350);

    async fn check(
        &self,
        key: Option<&str>,
        tool_count: usize,
        messages: &[EngineMessage],
    ) -> BranchGateDecision {
        let Some(key) = key.filter(|value| !value.is_empty()) else {
            return BranchGateDecision::Allow;
        };
        let now = Instant::now();
        let fingerprint = branch_message_fingerprint(messages);
        if tool_count > 0 {
            let mut guard = self.inner.lock().await;
            prune_branch_gate(&mut guard, now);
            let state = guard
                .entry(key.to_string())
                .or_insert_with(|| BranchGateState {
                    recent_tool_request_at: now,
                    fingerprints: HashSet::new(),
                });
            state.recent_tool_request_at = now;
            state.fingerprints.insert(fingerprint);
            return BranchGateDecision::Allow;
        }

        if self.has_recent_tool_branch(key, &fingerprint, now).await {
            return BranchGateDecision::SuppressNoToolBranch;
        }
        tokio::time::sleep(Self::WAIT_FOR_TOOL_BRANCH).await;
        if self
            .has_recent_tool_branch(key, &fingerprint, Instant::now())
            .await
        {
            BranchGateDecision::SuppressNoToolBranch
        } else {
            BranchGateDecision::Allow
        }
    }

    async fn has_recent_tool_branch(&self, key: &str, fingerprint: &str, now: Instant) -> bool {
        let mut guard = self.inner.lock().await;
        prune_branch_gate(&mut guard, now);
        guard.get(key).is_some_and(|state| {
            now.duration_since(state.recent_tool_request_at) <= Self::WINDOW
                && state.fingerprints.contains(fingerprint)
        })
    }
}

fn prune_branch_gate(items: &mut HashMap<String, BranchGateState>, now: Instant) {
    items.retain(|_, state| now.duration_since(state.recent_tool_request_at) <= BranchGate::WINDOW);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).with_context(|| {
        format!(
            "failed to create data directory {}",
            args.data_dir.display()
        )
    })?;
    init_logging(&args.data_dir)?;
    cleanup_old_logs(&args.data_dir, LOG_RETENTION_DAYS);
    let db_path = args.data_dir.join("windsurf-rs.sqlite3");
    let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .with_context(|| format!("failed to open sqlite database {}", db_path.display()))?;
    run_migrations(&db, &args.data_dir).await?;

    let settings = settings_map(&db).await.unwrap_or_default();
    let engine = RemoteApiEngine::new(EngineConfig::from_settings(
        &settings,
        args.data_dir.clone(),
    ));
    let (events, _) = broadcast::channel(512);
    let state = AppState {
        db,
        engine,
        events,
        branch_gate: BranchGate::default(),
        data_dir: args.data_dir.clone(),
    };
    let app = router(state, args.static_dir);
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("windsurf-rs listening on http://{}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_logging(data_dir: &PathBuf) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let log_dir = data_dir.join("logs");
    std::fs::create_dir_all(&log_dir).with_context(|| {
        format!(
            "failed to create log directory {}",
            log_dir.display()
        )
    })?;
    let file_appender = tracing_appender::rolling::daily(&log_dir, "windsurf-rs.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    Box::leak(Box::new(guard));
    fmt()
        .with_env_filter(env_filter)
        .with_writer(non_blocking)
        .with_ansi(false)
        .init();
    tracing::info!(
        log_dir = %log_dir.display(),
        retention_days = LOG_RETENTION_DAYS,
        "file logging initialized"
    );
    Ok(())
}

fn cleanup_old_logs(data_dir: &PathBuf, retention_days: u64) {
    let log_dir = data_dir.join("logs");
    let cutoff = StdDuration::from_secs(retention_days.saturating_mul(24 * 60 * 60));
    let Ok(entries) = std::fs::read_dir(&log_dir) else {
        return;
    };
    let now = std::time::SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !name.starts_with("windsurf-rs.log") {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        if now.duration_since(modified).is_ok_and(|age| age > cutoff) {
            let _ = std::fs::remove_file(&path);
        }
    }
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
        .route("/admin/events", get(admin_events_stream))
        .route(
            "/admin/accounts/{id}",
            patch(accounts_update).delete(accounts_delete),
        )
        .route(
            "/admin/accounts/refresh-status",
            post(accounts_refresh_status_all),
        )
        .route(
            "/admin/accounts/refresh-credits",
            post(accounts_refresh_credits_all),
        )
        .route("/admin/accounts/{id}/probe", post(account_probe))
        .route(
            "/admin/accounts/probe-defaults",
            get(account_probe_defaults),
        )
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
        .route(
            "/admin/client-api-keys",
            get(client_api_keys_list).post(client_api_keys_create),
        )
        .route(
            "/admin/client-api-keys/{id}",
            patch(client_api_keys_update).delete(client_api_keys_delete),
        )
        .route("/admin/requests", get(requests_list))
        .route("/admin/requests/{id}", get(request_detail))
        .route("/admin/stats", get(admin_stats))
        .route(
            "/admin/models/config",
            get(admin_models_config_get).put(admin_models_config_put),
        )
        .route("/admin/capacity", get(capacity_get).put(capacity_put))
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

async fn run_migrations(db: &SqlitePool, data_dir: &PathBuf) -> anyhow::Result<()> {
    let sql = include_str!("../../../migrations/0001_init.sql");
    for statement in sql.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            sqlx::query(statement).execute(db).await?;
        }
    }
    ensure_account_columns(db).await?;
    ensure_trace_chunk_columns(db).await?;
    ensure_client_api_keys(db).await?;
    ensure_admin_query_indexes(db).await?;
    migrate_trace_payloads_to_files(db, data_dir).await?;
    cleanup_runtime_state(db).await?;
    Ok(())
}

async fn ensure_client_api_keys(db: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS client_api_keys (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          key_value TEXT,
          key_hash TEXT NOT NULL UNIQUE,
          key_mask TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_used_at TEXT
        )",
    )
    .execute(db)
    .await?;
    ensure_client_api_key_columns(db).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_client_api_keys_enabled
          ON client_api_keys(enabled, key_hash)",
    )
    .execute(db)
    .await?;
    migrate_client_api_keys_setting(db).await?;
    Ok(())
}

async fn ensure_client_api_key_columns(db: &SqlitePool) -> anyhow::Result<()> {
    let existing_rows = sqlx::query("PRAGMA table_info(client_api_keys)")
        .fetch_all(db)
        .await?;
    let existing: std::collections::HashSet<String> = existing_rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    if !existing.contains("key_value") {
        sqlx::query("ALTER TABLE client_api_keys ADD COLUMN key_value TEXT")
            .execute(db)
            .await?;
    }
    Ok(())
}

async fn migrate_client_api_keys_setting(db: &SqlitePool) -> anyhow::Result<()> {
    let keys = client_api_keys_from_settings(db).await?;
    if keys.is_empty() {
        return Ok(());
    }
    let now_text = now();
    for (index, key) in keys.into_iter().enumerate() {
        let name = format!("调用密钥 {}", index + 1);
        let hash = sha256_hex(&key);
        let mask = mask_secret(&key);
        sqlx::query(
            "INSERT OR IGNORE INTO client_api_keys (name, key_value, key_hash, key_mask, enabled, created_at, updated_at)
             VALUES (?, ?, ?, ?, 1, ?, ?)",
        )
        .bind(name)
        .bind(&key)
        .bind(hash)
        .bind(mask)
        .bind(&now_text)
        .bind(&now_text)
        .execute(db)
        .await?;
    }
    sqlx::query("DELETE FROM settings WHERE key='client_api_keys'")
        .execute(db)
        .await?;
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
        ("rate_limit_probe_after", "TEXT"),
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
    ensure_account_model_rate_limit_columns(db).await?;
    Ok(())
}

async fn ensure_account_model_rate_limit_columns(db: &SqlitePool) -> anyhow::Result<()> {
    let existing_rows = sqlx::query("PRAGMA table_info(account_model_rate_limits)")
        .fetch_all(db)
        .await?;
    let existing: std::collections::HashSet<String> = existing_rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    if !existing.contains("probe_after") {
        sqlx::query("ALTER TABLE account_model_rate_limits ADD COLUMN probe_after TEXT")
            .execute(db)
            .await?;
    }
    Ok(())
}

async fn ensure_trace_chunk_columns(db: &SqlitePool) -> anyhow::Result<()> {
    let existing_rows = sqlx::query("PRAGMA table_info(request_trace_chunks)")
        .fetch_all(db)
        .await?;
    let existing: std::collections::HashSet<String> = existing_rows
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    if !existing.contains("payload_path") {
        sqlx::query("ALTER TABLE request_trace_chunks ADD COLUMN payload_path TEXT")
            .execute(db)
            .await?;
    }
    if !existing.contains("payload_size") {
        sqlx::query(
            "ALTER TABLE request_trace_chunks ADD COLUMN payload_size INTEGER NOT NULL DEFAULT 0",
        )
        .execute(db)
        .await?;
    }
    Ok(())
}

async fn ensure_admin_query_indexes(db: &SqlitePool) -> anyhow::Result<()> {
    for statement in [
        "CREATE INDEX IF NOT EXISTS idx_request_traces_started_at ON request_traces(started_at)",
        "CREATE INDEX IF NOT EXISTS idx_login_jobs_created_at ON login_jobs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_request_trace_chunks_trace_id ON request_trace_chunks(trace_id)",
        "CREATE INDEX IF NOT EXISTS idx_login_job_events_job_id_id ON login_job_events(job_id, id)",
    ] {
        sqlx::query(statement).execute(db).await?;
    }
    Ok(())
}

async fn migrate_trace_payloads_to_files(
    db: &SqlitePool,
    data_dir: &PathBuf,
) -> anyhow::Result<()> {
    let rows = sqlx::query(
        "SELECT id, trace_id, payload FROM request_trace_chunks
         WHERE payload_path IS NULL AND length(payload) > ?",
    )
    .bind(i64::try_from(TRACE_PAYLOAD_PREVIEW_CHARS).unwrap_or(1200))
    .fetch_all(db)
    .await?;
    let total = rows.len();
    for row in rows {
        let id = row.get::<i64, _>("id");
        let trace_id = row.get::<String, _>("trace_id");
        let payload = row.get::<String, _>("payload");
        let payload_size = i64::try_from(payload.len()).unwrap_or(i64::MAX);
        let payload_path = write_trace_payload(data_dir, &trace_id, &payload).await?;
        let payload_preview = trace_payload_preview(&payload);
        sqlx::query(
            "UPDATE request_trace_chunks
             SET payload=?, payload_path=?, payload_size=?
             WHERE id=?",
        )
        .bind(payload_preview)
        .bind(payload_path)
        .bind(payload_size)
        .bind(id)
        .execute(db)
        .await?;
    }
    if total > 0 {
        tracing::info!(count = total, "trace payloads migrated to files");
    }
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

async fn models(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_client_api_key(&state.db, &headers, Some(&query)).await {
        return resp;
    }
    let controls = model_control_settings(&state.db).await.unwrap_or_default();
    let disabled = disabled_model_set(&controls);
    let models = model_catalog(&state.db)
        .await
        .into_iter()
        .filter(|model| {
            let Some(id) = model.get("id").and_then(Value::as_str) else {
                return false;
            };
            !disabled.contains(&model_alias(id))
        })
        .collect::<Vec<_>>();
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
    .into_response()
}

async fn messages(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Json(payload): Json<MessagesRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_client_api_key(&state.db, &headers, Some(&query)).await {
        return resp;
    }
    let started_at = Instant::now();
    let trace_id = Uuid::new_v4().to_string();
    let controls = model_control_settings(&state.db).await.unwrap_or_default();
    let model = payload.model.clone().unwrap_or_else(|| {
        controls
            .default_model
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string())
    });
    let stream_requested = payload.stream.unwrap_or(false);
    let debug_summary = message_debug_summary(&payload);
    let request_snapshot = sanitized_message_request(&payload, &debug_summary);
    if let Err(err) = create_trace(&state.db, &trace_id, Some(&model), stream_requested).await {
        tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace create failed");
    }
    if disabled_model_set(&controls).contains(&model_alias(&model)) {
        let message = "这个模型已停用，请选择其他模型";
        if let Err(err) = finish_trace(&state.db, &trace_id, "error", None, Some(message)).await {
            tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace finish failed");
        }
        return error(StatusCode::BAD_REQUEST, "model_disabled", message);
    }
    if let Err(err) = add_trace_chunk(
        &state.db,
        &state.data_dir,
        &trace_id,
        "client_request_summary",
        &request_snapshot,
    )
    .await
    {
        tracing::debug!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace chunk write failed");
    }
    let caller_payload = json!({
        "metadata": payload.metadata,
        "messages": payload.messages,
    });
    let caller_key = extract_caller_key(&headers, &caller_payload);
    let caller_key_hash = caller_key.as_deref().map(short_hash);
    tracing::info!(
        trace_id = %trace_id,
        model = %model,
        stream = stream_requested,
        message_count = debug_summary.message_count,
        caller_key_hash = caller_key_hash.as_deref().unwrap_or("none"),
        "messages request start"
    );
    tracing::debug!(
        trace_id = %trace_id,
        max_tokens = payload.max_tokens.unwrap_or(0),
        roles = ?debug_summary.roles,
        system_chars = debug_summary.system_chars,
        tool_count = debug_summary.tool_count,
        has_tool_choice = debug_summary.has_tool_choice,
        metadata_keys = ?debug_summary.metadata_keys,
        input_chars = debug_summary.input_chars,
        "messages request summary"
    );
    if is_probe_request(&payload) {
        if let Err(err) = finish_trace(&state.db, &trace_id, "ok", Some("probe"), None).await {
            tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages probe trace finish failed");
        }
        tracing::info!(trace_id = %trace_id, model = %model, "messages probe intercepted");
        return send_probe_response(&model);
    }
    let mut engine_messages = messages_from_request(&payload);
    if engine_messages.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "messages is required and must be a non-empty array",
        );
    }
    let caller_environment = extract_caller_environment(&engine_messages);
    let (mut engine_tools, truncated_docs) = sanitize_tools(payload.tools.as_ref());
    let risk = detect_tool_description_risk_client(&headers, &engine_tools);
    let claude_code_client = is_claude_code_client(&headers);
    if claude_code_client || risk.risky {
        replace_system_prompt_for_tool_description_risk(&mut engine_messages);
        shorten_tool_descriptions_for_risk_client(&mut engine_tools);
        tracing::info!(
            trace_id = %trace_id,
            reason = %if risk.risky { risk.reason.as_str() } else { "header:claude-code-legacy" },
            tool_count = engine_tools.len(),
            "messages risk client normalized"
        );
    }
    if claude_code_client || risk.risky {
        let branch_key = branch_gate_key(caller_key.as_deref(), &model, &engine_messages);
        match state
            .branch_gate
            .check(
                branch_key.as_deref(),
                debug_summary.tool_count,
                &engine_messages,
            )
            .await
        {
            BranchGateDecision::Allow => {}
            BranchGateDecision::SuppressNoToolBranch => {
                tracing::info!(
                    trace_id = %trace_id,
                    caller_key_hash = caller_key_hash.as_deref().unwrap_or("none"),
                    tool_count = debug_summary.tool_count,
                    "messages no-tool side branch suppressed"
                );
                if let Err(err) = finish_trace(
                    &state.db,
                    &trace_id,
                    "ok",
                    Some("suppressed_no_tool_branch"),
                    None,
                )
                .await
                {
                    tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace finish failed");
                }
                if stream_requested {
                    return empty_anthropic_stream_response(trace_id, model);
                }
                return empty_anthropic_message_response(trace_id, model);
            }
        }
    }
    let tool_choice = sanitize_tool_choice(payload.tool_choice.as_ref());
    let isolation = match isolate_tool_names(
        engine_tools,
        engine_messages,
        tool_choice,
        truncated_docs,
        DEFAULT_ANONYMOUS_TOOL_NAME_PREFIX,
    ) {
        Ok(value) => value,
        Err(err) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_tool_name_prefix",
                &err.to_string(),
            );
        }
    };
    let tool_degrade = degrade_tool_choice_for_upstream(
        isolation.tool_choice,
        isolation.messages,
        &isolation.to_client_name,
    );
    let engine_messages = if claude_code_client || risk.risky {
        tool_degrade.messages
    } else {
        inject_tool_docs_into_system(tool_degrade.messages, &isolation.truncated_docs)
    };
    let engine_tools = isolation.tools;
    let tool_name_map = isolation.to_client_name;
    let tool_choice = tool_degrade.tool_choice;
    let sampling_params = sampling_params_from_request(&payload);
    let estimated_input_tokens = estimate_input_tokens_from_messages(&engine_messages);
    let engine_session_key = caller_key
        .clone()
        .unwrap_or_else(|| format!("trace:{trace_id}"));
    let system_prompt_mode = system_prompt_mode_setting(&state.db).await;
    tracing::debug!(
        trace_id = %trace_id,
        engine_message_count = engine_messages.len(),
        estimated_input_tokens,
        tool_count = engine_tools.len(),
        caller_environment = %caller_environment.as_deref().unwrap_or(""),
        system_prompt_mode = %system_prompt_mode.as_str(),
        "messages converted for upstream"
    );
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone(), state.events.clone());
    let engine_model = resolve_engine_model(&model);
    log_model_resolution(&trace_id, &model, &engine_model);
    let (mut lease, mut engine_account) = match scheduler
        .acquire_preflighted(
            &state.engine,
            &trace_id,
            &model,
            &engine_model,
            caller_key.clone(),
        )
        .await
    {
        Ok(value) => value,
        Err(AcquireError::TemporarilyUnavailable {
            retry_after_secs,
            reason,
            upstream_error,
        }) => {
            let client_message = upstream_error.as_deref().unwrap_or(&reason);
            tracing::warn!(
                trace_id = %trace_id,
                retry_after_secs,
                reason = %reason,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "messages scheduler unavailable"
            );
            if let Err(err) = finish_trace(&state.db, &trace_id, "error", None, Some(&reason)).await
            {
                tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace finish failed");
            }
            return rate_limited_response(client_message, retry_after_secs);
        }
        Err(AcquireError::NoAccount) => {
            tracing::warn!(
                trace_id = %trace_id,
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "messages scheduler no account"
            );
            if let Err(err) =
                finish_trace(&state.db, &trace_id, "error", None, Some("没有可用账号")).await
            {
                tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace finish failed");
            }
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "pool_exhausted",
                "没有可用账号",
            );
        }
        Err(AcquireError::Db(err)) => {
            tracing::error!(
                trace_id = %trace_id,
                error = %redact_log_text(&err.to_string()),
                elapsed_ms = started_at.elapsed().as_millis() as u64,
                "messages scheduler db error"
            );
            if let Err(trace_err) =
                finish_trace(&state.db, &trace_id, "error", None, Some(&err.to_string())).await
            {
                tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
            }
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "scheduler_error",
                &err.to_string(),
            );
        }
    };
    tracing::info!(
        trace_id = %trace_id,
        account_id = lease.account_id,
        model = %model,
        sticky = lease.sticky,
        reservation_id = %lease.reservation_id,
        "messages account acquired"
    );
    emit_admin_event(
        &state.events,
        "account_request_started",
        json!({
            "accountId": lease.account_id,
            "email": lease.email,
            "model": model,
            "traceId": trace_id,
            "sticky": lease.sticky
        }),
    );
    if let Err(err) = bind_trace_account(&state.db, &trace_id, lease.account_id).await {
        tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&err.to_string()), "messages trace account bind failed");
    }
    if let Err(err) = add_trace_chunk(
        &state.db,
        &state.data_dir,
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
    .await
    {
        tracing::debug!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace chunk write failed");
    }
    if stream_requested {
        let db = state.db.clone();
        let data_dir_for_stream = state.data_dir.clone();
        let capacity_for_stream = capacity.clone();
        let events_for_stream = state.events.clone();
        let engine = state.engine.clone();
        let trace = trace_id.clone();
        let model_for_stream = model.clone();
        let engine_tools_for_stream = engine_tools.clone();
        let tool_choice_for_stream = tool_choice.clone();
        let sampling_for_stream = sampling_params.clone();
        let tool_name_map_for_stream = tool_name_map.clone();
        let engine_model_for_stream = engine_model.clone();
        let engine_messages_for_stream = engine_messages.clone();
        let engine_session_key_for_stream = engine_session_key.clone();
        let caller_key_for_stream = caller_key.clone();
        let system_prompt_mode_for_stream = system_prompt_mode;
        let caller_environment_for_stream = caller_environment.clone();
        let s = stream! {
            let stream_started_at = Instant::now();
            let mut lease = lease;
            let mut engine_account = engine_account;
            let mut retry_on_account_failure = retry_budget_for_account_pool(
                &db,
                &capacity_for_stream,
                &trace,
                "stream",
            )
            .await;
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
                    "usage": { "input_tokens": estimated_input_tokens, "output_tokens": 0 }
                }
            });
            tracing::info!(trace_id = %trace, model = %model_for_stream, estimated_input_tokens, "messages stream start");
            yield Ok::<Event, std::convert::Infallible>(anthropic_sse_event("message_start", start));
            'stream_retry: loop {
                match engine
                    .messages_stream(
                        Some(trace.clone()),
                        Some(engine_session_key_for_stream.clone()),
                        engine_account.clone(),
                        engine_model_for_stream.clone(),
                        engine_messages_for_stream.clone(),
                        engine_tools_for_stream.clone(),
                        tool_choice_for_stream.clone(),
                        sampling_for_stream.clone(),
                        system_prompt_mode_for_stream,
                        caller_environment_for_stream.clone(),
                    )
                    .await
                {
                    Ok(upstream) => {
                        use futures_util::StreamExt;
                        futures_util::pin_mut!(upstream);
                        let mut input_tokens = estimated_input_tokens;
                        let mut cached_input_tokens = 0_u64;
                        let mut output_tokens = 0_u64;
                        let mut chunk_count = 0_u64;
                        let mut first_chunk_logged = false;
                        let mut active_block: Option<(String, u64)> = None;
                        let mut next_block_index = 0_u64;
                        let mut active_tool_call_id: Option<String> = None;
                        let mut has_tool_calls = false;
                        let mut stop_reason = "end_turn".to_string();
                        while let Some(item) = upstream.next().await {
                            match item {
                                Ok(chunk) => {
                                    chunk_count += 1;
                                    if !first_chunk_logged {
                                        first_chunk_logged = true;
                                        tracing::debug!(
                                            trace_id = %trace,
                                            first_chunk_ms = stream_started_at.elapsed().as_millis() as u64,
                                            "messages stream first chunk"
                                        );
                                    }
                                    if let Some(tokens) = chunk.prompt_tokens {
                                        input_tokens = tokens;
                                    }
                                    if let Some(tokens) = chunk.cached_input_tokens {
                                        cached_input_tokens = tokens;
                                    }
                                    if !chunk.text.is_empty() {
                                        output_tokens += estimate_local_tokens(&chunk.text);
                                    }
                                    if !chunk.reasoning.is_empty() {
                                        output_tokens += estimate_local_tokens(&chunk.reasoning);
                                    }
                                    if let Some(tokens) = chunk.completion_tokens {
                                        output_tokens = tokens;
                                    }
                                    if let Some(reason) = chunk.stop_reason.as_deref() {
                                        stop_reason = reason.to_string();
                                    }
                                    if !chunk.reasoning.is_empty() {
                                        let (index, events) = switch_anthropic_block(&mut active_block, &mut next_block_index, "text", json!({"type":"text","text":""}));
                                        for event in events {
                                            yield Ok(event);
                                        }
                                        let delta = anthropic_text_delta(index, chunk.reasoning);
                                        if let Err(err) = add_trace_chunk(
                                            &db,
                                            &data_dir_for_stream,
                                            &trace,
                                            "downstream_event",
                                            &delta,
                                        )
                                        .await
                                        {
                                            tracing::debug!(trace_id = %trace, error = %redact_log_text(&err.to_string()), "messages trace chunk write failed");
                                        }
                                        yield Ok(anthropic_sse_event("content_block_delta", delta));
                                    }
                                    if !chunk.text.is_empty() {
                                        let (index, events) = switch_anthropic_block(&mut active_block, &mut next_block_index, "text", json!({"type":"text","text":""}));
                                        for event in events {
                                            yield Ok(event);
                                        }
                                        let delta = anthropic_text_delta(index, chunk.text);
                                        if let Err(err) = add_trace_chunk(
                                            &db,
                                            &data_dir_for_stream,
                                            &trace,
                                            "downstream_event",
                                            &delta,
                                        )
                                        .await
                                        {
                                            tracing::debug!(trace_id = %trace, error = %redact_log_text(&err.to_string()), "messages trace chunk write failed");
                                        }
                                        yield Ok(anthropic_sse_event("content_block_delta", delta));
                                    }
                                    if chunk.tool_call_id.is_some() || chunk.tool_call_name.is_some() || chunk.tool_call_args.is_some() {
                                        has_tool_calls = true;
                                        let tool_call_id = chunk
                                            .tool_call_id
                                            .clone()
                                            .or_else(|| active_tool_call_id.clone())
                                            .unwrap_or_else(|| format!("toolu_{}", Uuid::new_v4().simple()));
                                        active_tool_call_id = Some(tool_call_id.clone());
                                        let upstream_name = chunk.tool_call_name.as_deref().unwrap_or("unknown");
                                        let client_name = restore_tool_name(upstream_name, &tool_name_map_for_stream);
                                        let (index, events) = switch_anthropic_block(
                                            &mut active_block,
                                            &mut next_block_index,
                                            "tool_use",
                                            json!({"type":"tool_use","id":tool_call_id,"name":client_name,"input":{}}),
                                        );
                                        for event in events {
                                            yield Ok(event);
                                        }
                                        if let Some(args) = chunk.tool_call_args.as_deref().filter(|value| !value.is_empty()) {
                                            yield Ok(anthropic_sse_event("content_block_delta", json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": { "type": "input_json_delta", "partial_json": args }
                                            })));
                                        }
                                    }
                                    tracing::debug!(
                                        trace_id = %trace,
                                        chunk_count,
                                        output_tokens,
                                        "messages stream downstream chunk"
                                    );
                                }
                                Err(err) => {
                                    let error_summary = redact_log_text(&err.to_string());
                                    tracing::error!(
                                        trace_id = %trace,
                                        error = %error_summary,
                                        elapsed_ms = stream_started_at.elapsed().as_millis() as u64,
                                        chunk_count,
                                        "messages stream upstream error"
                                    );
                                    let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone(), events_for_stream.clone());
                                    if chunk_count == 0
                                        && is_retryable_before_output_error(&err.to_string())
                                        && retry_on_account_failure > 0
                                    {
                                        retry_on_account_failure -= 1;
                                        if let Err(mark_err) = scheduler.mark_failure(&mut lease, &err.to_string()).await {
                                            tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                                            let error_kind = anthropic_error_type_for_upstream(&mark_err.to_string());
                                            yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":error_kind,"message":mark_err.to_string()}})));
                                            return;
                                        }
                                        match scheduler
                                            .acquire_preflighted(
                                                &engine,
                                                &trace,
                                                &model_for_stream,
                                                &engine_model_for_stream,
                                                caller_key_for_stream.clone(),
                                            )
                                            .await
                                        {
                                            Ok((retry_lease, retry_engine_account)) => {
                                                lease = retry_lease;
                                                engine_account = retry_engine_account;
                                                if let Err(bind_err) = bind_trace_account(&db, &trace, lease.account_id).await {
                                                    tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&bind_err.to_string()), "messages trace account bind failed");
                                                }
                                                if let Err(chunk_err) = add_trace_chunk(
                                                    &db,
                                                    &data_dir_for_stream,
                                                    &trace,
                                                    "account_acquired",
                                                    &json!({
                                                        "accountId": lease.account_id,
                                                        "email": lease.email,
                                                        "model": model_for_stream,
                                                        "sticky": lease.sticky,
                                                        "reservationId": lease.reservation_id,
                                                        "retryReason": "stream_rate_limit"
                                                    }),
                                                )
                                                .await
                                                {
                                                    tracing::debug!(trace_id = %trace, error = %redact_log_text(&chunk_err.to_string()), "messages trace chunk write failed");
                                                }
                                                tracing::info!(
                                                    trace_id = %trace,
                                                    account_id = lease.account_id,
                                                    remaining_retries = retry_on_account_failure,
                                                    "messages stream retryable error switched account"
                                                );
                                                continue 'stream_retry;
                                            }
                                            Err(acquire_err) => {
                                                let message = acquire_error_message(&acquire_err);
                                                if let Err(trace_err) = finish_trace(&db, &trace, "error", None, Some(&message)).await {
                                                    tracing::warn!(trace_id = %trace, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                                                }
                                                yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":"rate_limit_error","message":message}})));
                                                return;
                                            }
                                        }
                                    }
                                    if let Err(trace_err) = finish_trace(&db, &trace, "error", None, Some(&err.to_string())).await {
                                        tracing::warn!(trace_id = %trace, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                                    }
                                    if let Err(mark_err) = scheduler.mark_failure(&mut lease, &err.to_string()).await {
                                        tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                                    }
                                    let error_kind = anthropic_error_type_for_upstream(&err.to_string());
                                    yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":error_kind,"message":err.to_string()}})));
                                    return;
                                }
                            }
                        }
                        if let Some((_, index)) = active_block.take() {
                            yield Ok(anthropic_sse_event("content_block_stop", json!({"type": "content_block_stop", "index": index})));
                        }
                        let final_stop_reason = if has_tool_calls { "tool_use" } else { stop_reason.as_str() };
                        let stop = json!({
                            "type": "message_delta",
                            "delta": {"stop_reason": final_stop_reason, "stop_sequence": null},
                            "usage": {
                                "input_tokens": input_tokens + cached_input_tokens,
                                "output_tokens": output_tokens,
                                "cache_creation_input_tokens": 0,
                                "cache_read_input_tokens": cached_input_tokens,
                                "service_tier": "standard"
                            }
                        });
                        yield Ok(anthropic_sse_event("message_delta", stop));
                        yield Ok(anthropic_sse_event("message_stop", json!({"type": "message_stop"})));
                        let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone(), events_for_stream.clone());
                        if let Err(mark_err) = scheduler.mark_success(&mut lease).await {
                            tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark success failed");
                        }
                        if let Err(trace_err) = finish_trace(&db, &trace, "ok", Some(final_stop_reason), None).await {
                            tracing::warn!(trace_id = %trace, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                        }
                        tracing::info!(
                            trace_id = %trace,
                            elapsed_ms = stream_started_at.elapsed().as_millis() as u64,
                            chunk_count,
                            input_tokens,
                            output_tokens,
                            "messages stream complete"
                        );
                        return;
                    }
                    Err(err) => {
                        let error_summary = redact_log_text(&err.to_string());
                        tracing::error!(
                            trace_id = %trace,
                            error = %error_summary,
                            elapsed_ms = stream_started_at.elapsed().as_millis() as u64,
                            "messages stream open upstream failed"
                        );
                        let scheduler = AccountScheduler::new(db.clone(), capacity_for_stream.clone(), events_for_stream.clone());
                        if is_retryable_before_output_error(&err.to_string()) && retry_on_account_failure > 0 {
                            retry_on_account_failure -= 1;
                            if let Err(mark_err) = scheduler.mark_failure(&mut lease, &err.to_string()).await {
                                tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                                let error_kind = anthropic_error_type_for_upstream(&mark_err.to_string());
                                yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":error_kind,"message":mark_err.to_string()}})));
                                return;
                            }
                            match scheduler
                                .acquire_preflighted(
                                    &engine,
                                    &trace,
                                    &model_for_stream,
                                    &engine_model_for_stream,
                                    caller_key_for_stream.clone(),
                                )
                                .await
                            {
                                Ok((retry_lease, retry_engine_account)) => {
                                    lease = retry_lease;
                                    engine_account = retry_engine_account;
                                    if let Err(bind_err) = bind_trace_account(&db, &trace, lease.account_id).await {
                                        tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&bind_err.to_string()), "messages trace account bind failed");
                                    }
                                    tracing::info!(
                                        trace_id = %trace,
                                        account_id = lease.account_id,
                                        remaining_retries = retry_on_account_failure,
                                        "messages stream open retryable error switched account"
                                    );
                                    continue 'stream_retry;
                                }
                                Err(acquire_err) => {
                                    let message = acquire_error_message(&acquire_err);
                                    if let Err(trace_err) = finish_trace(&db, &trace, "error", None, Some(&message)).await {
                                        tracing::warn!(trace_id = %trace, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                                    }
                                    yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":"rate_limit_error","message":message}})));
                                    return;
                                }
                            }
                        }
                        if let Err(trace_err) = finish_trace(&db, &trace, "error", None, Some(&err.to_string())).await {
                            tracing::warn!(trace_id = %trace, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                        }
                        if let Err(mark_err) = scheduler.mark_failure(&mut lease, &err.to_string()).await {
                            tracing::warn!(trace_id = %trace, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                        }
                        let error_kind = anthropic_error_type_for_upstream(&err.to_string());
                        yield Ok(anthropic_sse_event("error", json!({"type":"error","error":{"type":error_kind,"message":err.to_string()}})));
                        return;
                    }
                }
            }
        };
        Sse::new(s).keep_alive(KeepAlive::default()).into_response()
    } else {
        let engine = state.engine.clone();
        let mut retry_on_account_failure =
            retry_budget_for_account_pool(&state.db, &capacity, &trace_id, "nonstream").await;
        let mut input_tokens;
        let mut cached_input_tokens;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut reasoning_signature = String::new();
        let mut tool_calls: HashMap<String, (String, String)> = HashMap::new();
        let mut generated_tool_index;
        let mut active_tool_call_id: Option<String>;
        let mut output_tokens;
        let mut stop_reason: String;
        let mut chunk_count;
        'request_retry: loop {
            input_tokens = estimated_input_tokens;
            cached_input_tokens = 0;
            text.clear();
            reasoning.clear();
            reasoning_signature.clear();
            tool_calls.clear();
            generated_tool_index = 0;
            active_tool_call_id = None;
            output_tokens = 0;
            stop_reason = "end_turn".to_string();
            chunk_count = 0;
            let stream_result = engine
                .messages_stream(
                    Some(trace_id.clone()),
                    Some(engine_session_key.clone()),
                    engine_account.clone(),
                    engine_model.clone(),
                    engine_messages.clone(),
                    engine_tools.clone(),
                    tool_choice.clone(),
                    sampling_params.clone(),
                    system_prompt_mode,
                    caller_environment.clone(),
                )
                .await;
            match stream_result {
                Ok(upstream) => {
                    use futures_util::StreamExt;
                    futures_util::pin_mut!(upstream);
                    while let Some(item) = upstream.next().await {
                        match item {
                            Ok(chunk) => {
                                chunk_count += 1;
                                if let Some(tokens) = chunk.prompt_tokens {
                                    input_tokens = tokens;
                                }
                                if let Some(tokens) = chunk.cached_input_tokens {
                                    cached_input_tokens = tokens;
                                }
                                if !chunk.text.is_empty() {
                                    output_tokens += estimate_local_tokens(&chunk.text);
                                    text.push_str(&chunk.text);
                                }
                                if !chunk.reasoning.is_empty() {
                                    output_tokens += estimate_local_tokens(&chunk.reasoning);
                                    reasoning.push_str(&chunk.reasoning);
                                }
                                if !chunk.reasoning_signature.is_empty() {
                                    reasoning_signature.push_str(&chunk.reasoning_signature);
                                }
                                if let Some(tokens) = chunk.completion_tokens {
                                    output_tokens = tokens;
                                }
                                if let Some(reason) = chunk.stop_reason {
                                    stop_reason = reason;
                                }
                                if chunk.tool_call_id.is_some()
                                    || chunk.tool_call_name.is_some()
                                    || chunk.tool_call_args.is_some()
                                {
                                    let id = chunk
                                        .tool_call_id
                                        .or_else(|| active_tool_call_id.clone())
                                        .unwrap_or_else(|| {
                                            generated_tool_index += 1;
                                            format!("toolu_{}", generated_tool_index)
                                        });
                                    active_tool_call_id = Some(id.clone());
                                    let entry = tool_calls.entry(id).or_insert_with(|| {
                                        (
                                            chunk
                                                .tool_call_name
                                                .unwrap_or_else(|| "unknown".to_string()),
                                            String::new(),
                                        )
                                    });
                                    if let Some(args) = chunk.tool_call_args.as_deref() {
                                        entry.1.push_str(args);
                                    }
                                }
                            }
                            Err(err) => {
                                let error_summary = redact_log_text(&err.to_string());
                                tracing::error!(
                                    trace_id = %trace_id,
                                    error = %error_summary,
                                    elapsed_ms = started_at.elapsed().as_millis() as u64,
                                    chunk_count,
                                    "messages nonstream upstream error"
                                );
                                if chunk_count == 0
                                    && is_retryable_before_output_error(&err.to_string())
                                    && retry_on_account_failure > 0
                                {
                                    retry_on_account_failure -= 1;
                                    if let Err(mark_err) =
                                        scheduler.mark_failure(&mut lease, &err.to_string()).await
                                    {
                                        tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                                        return error(
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            "scheduler_error",
                                            &mark_err.to_string(),
                                        );
                                    }
                                    match scheduler
                                        .acquire_preflighted(
                                            &engine,
                                            &trace_id,
                                            &model,
                                            &engine_model,
                                            caller_key.clone(),
                                        )
                                        .await
                                    {
                                        Ok((retry_lease, retry_engine_account)) => {
                                            lease = retry_lease;
                                            engine_account = retry_engine_account;
                                            if let Err(bind_err) = bind_trace_account(
                                                &state.db,
                                                &trace_id,
                                                lease.account_id,
                                            )
                                            .await
                                            {
                                                tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&bind_err.to_string()), "messages trace account bind failed");
                                            }
                                            tracing::info!(
                                                trace_id = %trace_id,
                                                account_id = lease.account_id,
                                                remaining_retries = retry_on_account_failure,
                                                "messages nonstream retryable error switched account"
                                            );
                                            continue 'request_retry;
                                        }
                                        Err(acquire_err) => {
                                            let message = acquire_error_message(&acquire_err);
                                            if let Err(trace_err) = finish_trace(
                                                &state.db,
                                                &trace_id,
                                                "error",
                                                None,
                                                Some(&message),
                                            )
                                            .await
                                            {
                                                tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                                            }
                                            return acquire_error_response(acquire_err);
                                        }
                                    }
                                }
                                if let Err(mark_err) =
                                    scheduler.mark_failure(&mut lease, &err.to_string()).await
                                {
                                    tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                                }
                                if let Err(trace_err) = finish_trace(
                                    &state.db,
                                    &trace_id,
                                    "error",
                                    None,
                                    Some(&err.to_string()),
                                )
                                .await
                                {
                                    tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                                }
                                return upstream_messages_error_response(&err.to_string());
                            }
                        }
                    }
                }
                Err(err) => {
                    let error_summary = redact_log_text(&err.to_string());
                    tracing::error!(
                        trace_id = %trace_id,
                        error = %error_summary,
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        "messages nonstream open upstream failed"
                    );
                    if let Err(mark_err) =
                        scheduler.mark_failure(&mut lease, &err.to_string()).await
                    {
                        tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&mark_err.to_string()), "messages account mark error failed");
                    }
                    if let Err(trace_err) =
                        finish_trace(&state.db, &trace_id, "error", None, Some(&err.to_string()))
                            .await
                    {
                        tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&trace_err.to_string()), "messages trace finish failed");
                    }
                    return upstream_messages_error_response(&err.to_string());
                }
            }
            break;
        }
        if output_tokens == 0 {
            output_tokens = estimate_local_tokens(&format!("{reasoning}{text}"));
        }
        let mut content = Vec::new();
        if !reasoning.is_empty() {
            text = format!("{reasoning}{text}");
        }
        if !text.is_empty() || tool_calls.is_empty() {
            content.push(json!({ "type": "text", "text": text }));
        }
        for (id, (name, args)) in tool_calls {
            let input = serde_json::from_str::<Value>(&args)
                .ok()
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            content.push(json!({
                "type": "tool_use",
                "id": id,
                "name": restore_tool_name(&name, &tool_name_map),
                "input": input
            }));
        }
        let final_stop_reason = if content
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        {
            "tool_use"
        } else {
            stop_reason.as_str()
        };
        let response = json!({
            "id": format!("msg_{}", trace_id),
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": content,
            "stop_reason": final_stop_reason,
            "stop_sequence": null,
            "usage": {
                "input_tokens": input_tokens + cached_input_tokens,
                "output_tokens": output_tokens,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": cached_input_tokens,
                "service_tier": "standard"
            }
        });
        if let Err(err) = add_trace_chunk(
            &state.db,
            &state.data_dir,
            &trace_id,
            "downstream_response",
            &response,
        )
        .await
        {
            tracing::debug!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace chunk write failed");
        }
        if let Err(err) = scheduler.mark_success(&mut lease).await {
            tracing::warn!(trace_id = %trace_id, account_id = lease.account_id, error = %redact_log_text(&err.to_string()), "messages account mark success failed");
        }
        if let Err(err) =
            finish_trace(&state.db, &trace_id, "ok", Some(final_stop_reason), None).await
        {
            tracing::warn!(trace_id = %trace_id, error = %redact_log_text(&err.to_string()), "messages trace finish failed");
        }
        tracing::info!(
            trace_id = %trace_id,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            chunk_count,
            input_tokens,
            output_tokens,
            "messages nonstream complete"
        );
        Json(response).into_response()
    }
}

async fn count_tokens(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    if let Err(resp) = require_client_api_key(&state.db, &headers, Some(&query)).await {
        return resp;
    }
    let raw = payload.to_string();
    let tokens = (raw.chars().count() as f64 / 4.0).ceil() as u64;
    Json(json!({ "input_tokens": tokens.max(1) })).into_response()
}

fn emit_admin_event(events: &broadcast::Sender<AdminEvent>, kind: &str, payload: Value) {
    let _ = events.send(AdminEvent {
        kind: kind.to_string(),
        payload,
        created_at: now(),
    });
}

fn emit_account_event(events: &broadcast::Sender<AdminEvent>, action: &str, account_id: i64) {
    emit_admin_event(
        events,
        "account_changed",
        json!({ "action": action, "accountId": account_id }),
    );
}

async fn admin_events_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let mut rx = state.events.subscribe();
    let s = stream! {
        yield Ok::<Event, std::convert::Infallible>(
            Event::default()
                .event("ready")
                .data(json!({ "kind": "ready", "createdAt": now() }).to_string()),
        );
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let data = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
                    yield Ok(Event::default().event(&event.kind).data(data));
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    yield Ok(Event::default().event("resync").data(json!({
                        "kind": "resync",
                        "payload": { "skipped": skipped },
                        "createdAt": now()
                    }).to_string()));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(s).keep_alive(KeepAlive::default()).into_response()
}

async fn accounts_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let capacity = capacity_settings(&state.db).await.unwrap_or_default();
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone(), state.events.clone());
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
    let default_model = get_setting_string(&state.db, "account_probe_model")
        .await
        .unwrap_or_else(|| DEFAULT_ADMIN_MODEL.to_string());
    let default_engine_model = resolve_engine_model(&default_model);
    let default_upstream_model = default_engine_model
        .model_uid
        .as_deref()
        .unwrap_or(&default_engine_model.id)
        .to_string();
    let mut accounts = Vec::new();
    for row in rows {
        let scheduler_account = scheduler_account_from_row(&row);
        let availability = scheduler
            .availability(
                &scheduler_account,
                &default_model,
                Some(&default_engine_model.id),
                Some(&default_upstream_model),
            )
            .await
            .unwrap_or_else(|_| {
                AccountAvailability::unavailable(AvailabilityKind::StatusUnavailable, 60)
            });
        accounts.push(account_json(
            row,
            &model_limits,
            &sticky_counts,
            Some(&availability),
        ));
    }
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
        Ok(id) => {
            emit_account_event(&state.events, "saved", id);
            Json(ApiResponse {
                success: true,
                data: json!({ "id": id }),
            })
            .into_response()
        }
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
        Ok(_) => {
            emit_account_event(&state.events, "updated", id);
            Json(ApiResponse { success: true, data: json!({}) }).into_response()
        }
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
        Ok(_) => {
            emit_account_event(&state.events, "deleted", id);
            Json(ApiResponse {
                success: true,
                data: json!({}),
            })
            .into_response()
        }
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
        Ok(_) => {
            emit_account_event(&state.events, "reset_errors", id);
            Json(ApiResponse { success: true, data: json!({}) }).into_response()
        }
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
    let scheduler = AccountScheduler::new(state.db.clone(), capacity.clone(), state.events.clone());
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
    let scheduler = AccountScheduler::new(state.db.clone(), capacity, state.events.clone());
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
    match refresh_account_status(&state.db, &state.data_dir, id, false).await {
        Ok(value) => {
            emit_account_event(&state.events, "status_refreshed", id);
            Json(ApiResponse {
                success: true,
                data: value,
            })
            .into_response()
        }
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
        let result = refresh_account_status(&state.db, &state.data_dir, id, false).await;
        if result.is_ok() {
            emit_account_event(&state.events, "status_refreshed", id);
        }
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
    Json(payload): Json<AccountProbeRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let model = payload.model.trim().to_string();
    let message = payload.message.trim().to_string();
    if model.is_empty() || message.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_probe",
            "请选择模型并填写探测内容",
        );
    }
    if payload.save_defaults.unwrap_or(true) {
        let _ = save_json_setting(&state.db, "account_probe_model", &json!(model)).await;
        let _ = save_json_setting(&state.db, "account_probe_message", &json!(message)).await;
    }
    let row = match sqlx::query("SELECT * FROM accounts WHERE id=?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "account_not_found", "账号不存在"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let Some(credentials) = account_credentials(&row) else {
        return error(
            StatusCode::BAD_REQUEST,
            "credential_missing",
            "这个账号还没有可用凭据",
        );
    };
    let email = row.get::<String, _>("email");
    let account_id = row.get::<i64, _>("id");
    let db = state.db.clone();
    let events = state.events.clone();
    let engine = state.engine.clone();
    let engine_account = EngineAccount {
        api_key: credentials.api_key,
        jwt_token: credentials.jwt_token,
        proxy_url: proxy_url_for_account(&state.db, account_id).await,
    };
    let engine_model = resolve_engine_model(&model);
    log_model_resolution(
        &format!("probe:{account_id}:{model}"),
        &model,
        &engine_model,
    );
    let system_prompt_mode = system_prompt_mode_setting(&state.db).await;
    let engine_messages = vec![EngineMessage {
        role: "user".to_string(),
        content: message,
        ..Default::default()
    }];
    let s = stream! {
        let start = json!({"type": "message_start", "accountId": account_id, "model": model, "email": email});
        emit_admin_event(&events, "account_probe_started", start.clone());
        yield Ok::<Event, std::convert::Infallible>(Event::default().data(start.to_string()));
        match engine
            .messages_stream(
                None,
                Some(format!("probe:{account_id}:{model}")),
                engine_account,
                engine_model,
                engine_messages,
                Vec::new(),
                EngineToolChoice::Auto,
                None,
                system_prompt_mode,
                None,
            )
            .await
        {
            Ok(upstream) => {
                use futures_util::StreamExt;
                futures_util::pin_mut!(upstream);
                while let Some(item) = upstream.next().await {
                    match item {
                        Ok(chunk) => yield Ok(Event::default().data(json!({"type": "content_block_delta", "delta": {"type": "text_delta", "text": chunk.text}}).to_string())),
                        Err(err) => {
                            let message = err.to_string();
                            if is_transient_probe_error(&message) {
                                let _ = mark_account_transient_error_with_events(&db, &events, account_id, &message).await;
                            } else {
                                let _ = mark_account_error_with_events(&db, &events, account_id, &message).await;
                            }
                            yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":message}}).to_string()));
                            return;
                        }
                    }
                }
            }
            Err(err) => {
                let message = err.to_string();
                if is_transient_probe_error(&message) {
                    let _ = mark_account_transient_error_with_events(&db, &events, account_id, &message).await;
                } else {
                    let _ = mark_account_error_with_events(&db, &events, account_id, &message).await;
                }
                yield Ok(Event::default().data(json!({"type":"error","error":{"type":"api_error","message":message}}).to_string()));
                return;
            }
        }
        yield Ok(Event::default().data(json!({"type": "message_stop"}).to_string()));
        let _ = mark_account_probe_success_with_events(&db, &events, account_id).await;
    };
    Sse::new(s).into_response()
}

async fn accounts_refresh_status_all(
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
        let result = refresh_account_status(&state.db, &state.data_dir, id, false).await;
        if result.is_ok() {
            emit_account_event(&state.events, "status_refreshed", id);
        }
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

async fn account_probe_defaults(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let default_model = get_setting_string(&state.db, "account_probe_model")
        .await
        .unwrap_or_else(|| "claude-opus-4.7".to_string());
    let default_message = get_setting_string(&state.db, "account_probe_message")
        .await
        .unwrap_or_else(|| "用一句话确认这个账号可以正常回复。".to_string());
    let models = model_catalog(&state.db).await;
    Json(ApiResponse {
        success: true,
        data: json!({ "model": default_model, "message": default_message, "models": models }),
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
    let events = state.events.clone();
    let job_id = id.clone();
    emit_admin_event(
        &events,
        "login_job_changed",
        json!({ "jobId": id, "action": "created" }),
    );
    tokio::spawn(async move {
        run_login_job(db, events, job_id, lines, payload).await;
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
        &state.events,
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

async fn client_api_keys_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let rows = match sqlx::query("SELECT * FROM client_api_keys ORDER BY created_at DESC")
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
    let keys: Vec<Value> = rows.into_iter().map(client_api_key_json).collect();
    Json(ApiResponse {
        success: true,
        data: json!({ "keys": keys }),
    })
    .into_response()
}

async fn client_api_keys_create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<CreateClientApiKeyRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let key = payload
        .key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("wsk-{}", Uuid::new_v4().simple()));
    let name = normalize_client_api_key_name(payload.name.as_deref());
    let enabled = payload.enabled.unwrap_or(true);
    let now_text = now();
    match sqlx::query(
        "INSERT INTO client_api_keys (name, key_value, key_hash, key_mask, enabled, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(name)
    .bind(&key)
    .bind(sha256_hex(&key))
    .bind(mask_secret(&key))
    .bind(if enabled { 1_i64 } else { 0_i64 })
    .bind(&now_text)
    .bind(&now_text)
    .execute(&state.db)
    .await
    {
        Ok(done) => Json(ApiResponse {
            success: true,
            data: json!({
                "id": done.last_insert_rowid(),
                "key": key
            }),
        })
        .into_response(),
        Err(err) => error(
            StatusCode::BAD_REQUEST,
            "client_api_key_exists",
            &client_api_key_db_error_message(&err.to_string()),
        ),
    }
}

async fn client_api_keys_update(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(payload): Json<UpdateClientApiKeyRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let row = match sqlx::query("SELECT * FROM client_api_keys WHERE id=?")
        .bind(id)
        .fetch_optional(&state.db)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return error(StatusCode::NOT_FOUND, "not_found", "调用密钥不存在"),
        Err(err) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                &err.to_string(),
            );
        }
    };
    let name = payload
        .name
        .as_deref()
        .map(|value| normalize_client_api_key_name(Some(value)))
        .unwrap_or_else(|| row.get::<String, _>("name"));
    let enabled = payload
        .enabled
        .map(|value| if value { 1_i64 } else { 0_i64 })
        .unwrap_or_else(|| row.get::<i64, _>("enabled"));
    let existing_hash = row.get::<String, _>("key_hash");
    let existing_mask = row.get::<String, _>("key_mask");
    let existing_value = row.get::<Option<String>, _>("key_value");
    let (key_value, key_hash, key_mask) = payload
        .key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|key| (Some(key.to_string()), sha256_hex(key), mask_secret(key)))
        .unwrap_or((existing_value, existing_hash, existing_mask));
    let now_text = now();
    match sqlx::query(
        "UPDATE client_api_keys SET name=?, key_value=?, key_hash=?, key_mask=?, enabled=?, updated_at=? WHERE id=?",
    )
    .bind(name)
    .bind(key_value)
    .bind(key_hash)
    .bind(key_mask)
    .bind(enabled)
    .bind(now_text)
    .bind(id)
    .execute(&state.db)
    .await
    {
        Ok(done) if done.rows_affected() > 0 => Json(ApiResponse {
            success: true,
            data: json!({}),
        })
        .into_response(),
        Ok(_) => error(StatusCode::NOT_FOUND, "not_found", "调用密钥不存在"),
        Err(err) => error(
            StatusCode::BAD_REQUEST,
            "client_api_key_exists",
            &client_api_key_db_error_message(&err.to_string()),
        ),
    }
}

async fn client_api_keys_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    match sqlx::query("DELETE FROM client_api_keys WHERE id=?")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(done) if done.rows_affected() > 0 => Json(ApiResponse {
            success: true,
            data: json!({}),
        })
        .into_response(),
        Ok(_) => error(StatusCode::NOT_FOUND, "not_found", "调用密钥不存在"),
        Err(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "db_error",
            &err.to_string(),
        ),
    }
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
            let fallback_payload = row.get::<String, _>("payload");
            let payload = row
                .get::<Option<String>, _>("payload_path")
                .and_then(|path| read_trace_payload(&state.data_dir, &path).ok())
                .unwrap_or(fallback_payload);
            json!({
                "id": row.get::<i64, _>("id"),
                "layer": row.get::<String, _>("layer"),
                "payload": payload,
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

async fn admin_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<StatsQuery>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let range = stats_range(query.range.as_deref());
    let since = stats_since_utc(&range).to_rfc3339();
    let requests = sqlx::query("SELECT * FROM request_traces WHERE started_at >= ? ORDER BY started_at DESC")
        .bind(&since)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let accounts = sqlx::query("SELECT * FROM accounts")
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let jobs = sqlx::query("SELECT * FROM login_jobs WHERE created_at >= ?")
        .bind(&since)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();

    let total_requests = requests.len() as i64;
    let ok_requests = requests
        .iter()
        .filter(|row| row.get::<String, _>("status") == "ok")
        .count() as i64;
    let failed_requests = requests
        .iter()
        .filter(|row| row.get::<String, _>("status") != "ok")
        .count() as i64;
    let running_requests = requests
        .iter()
        .filter(|row| row.get::<String, _>("status") == "running")
        .count() as i64;
    let avg_latency_ms = average_trace_latency_ms(&requests);

    let mut model_stats: HashMap<String, (i64, i64, i64)> = HashMap::new();
    let mut account_stats: HashMap<i64, (i64, i64, Option<String>)> = HashMap::new();
    let mut error_stats: HashMap<String, i64> = HashMap::new();
    for row in &requests {
        let status = row.get::<String, _>("status");
        let ok = status == "ok";
        let model = row
            .get::<Option<String>, _>("model")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "未指定".to_string());
        let entry = model_stats.entry(model).or_insert((0, 0, 0));
        entry.0 += 1;
        if ok {
            entry.1 += 1;
        } else {
            entry.2 += 1;
        }
        if let Some(account_id) = row.get::<Option<i64>, _>("account_id") {
            let entry = account_stats.entry(account_id).or_insert((0, 0, None));
            entry.0 += 1;
            if !ok {
                entry.1 += 1;
                entry.2 = row.get::<Option<String>, _>("error_summary");
            }
        }
        if !ok {
            let reason = row
                .get::<Option<String>, _>("error_summary")
                .or_else(|| row.get::<Option<String>, _>("end_reason"))
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| status.clone());
            *error_stats.entry(compact_error_reason(&reason)).or_insert(0) += 1;
        }
    }

    let account_names = accounts
        .iter()
        .map(|row| {
            let id = row.get::<i64, _>("id");
            let name = row
                .get::<Option<String>, _>("label")
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| row.get::<String, _>("email"));
            (id, name)
        })
        .collect::<HashMap<_, _>>();
    let account_rows = accounts
        .iter()
        .map(|row| {
            let status = row.get::<String, _>("status");
            let rate_limited = row.get::<Option<String>, _>("rate_limited_until").is_some_and(|value| {
                chrono::DateTime::parse_from_rfc3339(&value)
                    .map(|time| time.with_timezone(&Utc) > Utc::now())
                    .unwrap_or(false)
            });
            json!({
                "id": row.get::<i64, _>("id"),
                "name": account_names.get(&row.get::<i64, _>("id")).cloned().unwrap_or_else(|| row.get::<String, _>("email")),
                "status": status,
                "rateLimited": rate_limited,
                "errorCount": row.get::<i64, _>("error_count"),
                "lastError": row.get::<Option<String>, _>("last_error")
            })
        })
        .collect::<Vec<_>>();

    Json(ApiResponse {
        success: true,
        data: json!({
            "range": { "key": range.key, "label": range.label, "since": since },
            "overview": {
                "requests": total_requests,
                "succeeded": ok_requests,
                "failed": failed_requests,
                "running": running_requests,
                "successRate": percent(ok_requests, total_requests),
                "avgLatencyMs": avg_latency_ms,
                "accounts": accounts.len(),
                "availableAccounts": accounts.iter().filter(|row| ["ready", "active", "ok"].contains(&row.get::<String, _>("status").as_str())).count(),
                "issueAccounts": accounts.iter().filter(|row| account_has_issue(row)).count()
            },
            "models": sorted_stat_rows(model_stats, |model, (total, ok, failed)| json!({
                "model": model,
                "requests": total,
                "succeeded": ok,
                "failed": failed,
                "successRate": percent(ok, total)
            })),
            "accounts": sorted_stat_rows(account_stats, |id, (total, failed, last_error)| json!({
                "accountId": id,
                "name": account_names.get(&id).cloned().unwrap_or_else(|| format!("#{id}")),
                "requests": total,
                "failed": failed,
                "successRate": percent(total - failed, total),
                "lastError": last_error
            })),
            "errors": sorted_stat_rows(error_stats, |message, count| json!({ "message": message, "count": count })),
            "accountStates": account_rows,
            "loginJobs": {
                "total": jobs.len(),
                "running": jobs.iter().filter(|row| row.get::<String, _>("status") == "running").count(),
                "succeeded": jobs.iter().map(|row| row.get::<i64, _>("success_count")).sum::<i64>(),
                "failed": jobs.iter().map(|row| row.get::<i64, _>("failed_count")).sum::<i64>()
            },
            "timeline": stats_timeline(&requests, &range)
        }),
    })
    .into_response()
}

async fn admin_models_config_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    models_config_response(&state.db).await
}

async fn admin_models_config_put(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<SaveModelControlRequest>,
) -> impl IntoResponse {
    if let Err(resp) = require_admin(&state.db, &headers).await {
        return resp;
    }
    let all_models = model_catalog(&state.db).await;
    let known = all_models
        .iter()
        .filter_map(|model| model.get("id").and_then(Value::as_str).map(model_alias))
        .collect::<HashSet<_>>();
    let default_model = model_alias(&payload.default_model);
    if !known.contains(&default_model) {
        return error(StatusCode::BAD_REQUEST, "invalid_default_model", "请选择可用的默认模型");
    }
    let disabled_models = payload
        .disabled_models
        .into_iter()
        .map(|model| model_alias(&model))
        .filter(|model| known.contains(model))
        .collect::<HashSet<_>>();
    if disabled_models.contains(&default_model) {
        return error(StatusCode::BAD_REQUEST, "default_model_disabled", "默认模型需要保持可用");
    }
    let settings = ModelControlSettings {
        default_model: Some(default_model),
        disabled_models: disabled_models.into_iter().collect(),
    };
    match save_json_setting(&state.db, "modelControl", &json!(settings)).await {
        Ok(_) => models_config_response(&state.db).await,
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "model_config_save_failed", &err.to_string()),
    }
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
        let raw = row.get::<String, _>("value");
        let value = serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| json!(raw));
        map.insert(row.get::<String, _>("key"), value);
    }
    map.entry("systemPromptMode".to_string())
        .or_insert_with(|| json!(DEFAULT_SYSTEM_PROMPT_MODE_SETTING));
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
            if key == "client_api_keys" {
                return error(
                    StatusCode::BAD_REQUEST,
                    "use_client_api_key_page",
                    "请在调用密钥页面管理",
                );
            }
            if key == "systemPromptMode"
                && !value
                    .as_str()
                    .is_some_and(|mode| parse_system_prompt_mode(mode).is_some())
            {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_system_prompt_mode",
                    "请选择可用的提示词处理方式",
                );
            }
            let stored_value = value.to_string();
            if let Err(err) = sqlx::query(
                "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            )
            .bind(key)
            .bind(stored_value)
            .bind(&now_text)
            .execute(&state.db)
            .await
            {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "settings_save_failed",
                    &err.to_string(),
                );
            }
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

fn parse_system_prompt_mode(value: &str) -> Option<SystemPromptMode> {
    match value {
        "passthrough" => Some(SystemPromptMode::Passthrough),
        "strip-identity" => Some(SystemPromptMode::StripIdentity),
        "windsurf-wrap" => Some(SystemPromptMode::WindsurfWrap),
        _ => None,
    }
}

async fn system_prompt_mode_setting(db: &SqlitePool) -> SystemPromptMode {
    get_setting_string(db, "systemPromptMode")
        .await
        .as_deref()
        .and_then(parse_system_prompt_mode)
        .unwrap_or(SystemPromptMode::StripIdentity)
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

async fn model_control_settings(db: &SqlitePool) -> anyhow::Result<ModelControlSettings> {
    let Some(raw) = get_setting_string(db, "modelControl").await else {
        return Ok(ModelControlSettings::default());
    };
    let mut parsed = serde_json::from_str::<ModelControlSettings>(&raw).unwrap_or_default();
    parsed.default_model = parsed
        .default_model
        .map(|model| model_alias(&model))
        .filter(|model| !model.trim().is_empty());
    parsed.disabled_models = parsed
        .disabled_models
        .into_iter()
        .map(|model| model_alias(&model))
        .filter(|model| !model.trim().is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    Ok(parsed)
}

fn disabled_model_set(settings: &ModelControlSettings) -> HashSet<String> {
    settings
        .disabled_models
        .iter()
        .map(|model| model_alias(model))
        .collect()
}

struct StatsRange {
    key: &'static str,
    label: &'static str,
    hours: i64,
    bucket_hours: i64,
}

fn stats_range(value: Option<&str>) -> StatsRange {
    match value.unwrap_or("24h") {
        "7d" => StatsRange {
            key: "7d",
            label: "近 7 天",
            hours: 24 * 7,
            bucket_hours: 24,
        },
        "30d" => StatsRange {
            key: "30d",
            label: "近 30 天",
            hours: 24 * 30,
            bucket_hours: 24,
        },
        _ => StatsRange {
            key: "24h",
            label: "近 24 小时",
            hours: 24,
            bucket_hours: 1,
        },
    }
}

fn percent(part: i64, total: i64) -> i64 {
    if total <= 0 {
        0
    } else {
        ((part.max(0) as f64 / total as f64) * 100.0).round() as i64
    }
}

fn average_trace_latency_ms(rows: &[sqlx::sqlite::SqliteRow]) -> i64 {
    let mut total = 0_i64;
    let mut count = 0_i64;
    for row in rows {
        let started = row.get::<String, _>("started_at");
        let Some(ended) = row.get::<Option<String>, _>("ended_at") else {
            continue;
        };
        let Ok(started_at) = chrono::DateTime::parse_from_rfc3339(&started) else {
            continue;
        };
        let Ok(ended_at) = chrono::DateTime::parse_from_rfc3339(&ended) else {
            continue;
        };
        total += ended_at
            .signed_duration_since(started_at)
            .num_milliseconds()
            .max(0);
        count += 1;
    }
    if count == 0 { 0 } else { total / count }
}

fn compact_error_reason(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.chars().count() <= 120 {
        return trimmed.to_string();
    }
    let mut out = trimmed.chars().take(120).collect::<String>();
    out.push_str("...");
    out
}

fn sorted_stat_rows<K, V, F>(items: HashMap<K, V>, map: F) -> Vec<Value>
where
    K: Eq + std::hash::Hash,
    F: Fn(K, V) -> Value,
{
    let mut rows = items
        .into_iter()
        .map(|(key, value)| map(key, value))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let left = a
            .get("requests")
            .or_else(|| a.get("count"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let right = b
            .get("requests")
            .or_else(|| b.get("count"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        right.cmp(&left)
    });
    rows
}

fn account_has_issue(row: &sqlx::sqlite::SqliteRow) -> bool {
    let status = row.get::<String, _>("status");
    let rate_limited = row.get::<Option<String>, _>("rate_limited_until").is_some_and(|value| {
        chrono::DateTime::parse_from_rfc3339(&value)
            .map(|time| time.with_timezone(&Utc) > Utc::now())
            .unwrap_or(false)
    });
    !["ready", "active", "ok"].contains(&status.as_str())
        || row.get::<i64, _>("error_count") > 0
        || row.get::<Option<String>, _>("last_error").is_some()
        || row.get::<Option<String>, _>("credentials_json").is_none()
        || rate_limited
}

fn stats_timeline(rows: &[sqlx::sqlite::SqliteRow], range: &StatsRange) -> Vec<Value> {
    let start = shanghai_bucket_start(stats_since_utc(range), range.bucket_hours);
    let end = shanghai_bucket_start(Utc::now(), range.bucket_hours);
    let bucket_count = (end
        .signed_duration_since(start)
        .num_hours()
        .div_euclid(range.bucket_hours)
        + 1)
        .max(1);
    let mut buckets = (0..bucket_count)
        .map(|index| {
            let time = start + Duration::hours(index * range.bucket_hours);
            let label = if range.bucket_hours == 1 {
                time.format("%H:00").to_string()
            } else {
                time.format("%m-%d").to_string()
            };
            (label, 0_i64, 0_i64, 0_i64)
        })
        .collect::<Vec<_>>();
    for row in rows {
        let started = row.get::<String, _>("started_at");
        let Ok(started_at) = chrono::DateTime::parse_from_rfc3339(&started) else {
            continue;
        };
        let elapsed_hours = started_at
            .with_timezone(&shanghai_offset())
            .signed_duration_since(start)
            .num_hours();
        if elapsed_hours < 0 {
            continue;
        }
        let index = (elapsed_hours / range.bucket_hours) as usize;
        let Some(bucket) = buckets.get_mut(index) else {
            continue;
        };
        bucket.1 += 1;
        if row.get::<String, _>("status") == "ok" {
            bucket.2 += 1;
        } else {
            bucket.3 += 1;
        }
    }
    buckets
        .into_iter()
        .map(|(label, requests, succeeded, failed)| {
            json!({ "label": label, "requests": requests, "succeeded": succeeded, "failed": failed })
        })
        .collect()
}

fn shanghai_offset() -> FixedOffset {
    FixedOffset::east_opt(8 * 60 * 60).expect("valid Asia/Shanghai offset")
}

fn stats_since_utc(range: &StatsRange) -> DateTime<Utc> {
    if range.bucket_hours == 1 {
        return Utc::now() - Duration::hours(range.hours);
    }
    shanghai_bucket_start(
        Utc::now() - Duration::hours(range.hours - range.bucket_hours),
        range.bucket_hours,
    )
    .with_timezone(&Utc)
}

fn shanghai_bucket_start(time: DateTime<Utc>, bucket_hours: i64) -> DateTime<FixedOffset> {
    let bucket_secs = (bucket_hours.max(1) * 60 * 60).max(1);
    let offset_secs = i64::from(shanghai_offset().local_minus_utc());
    let local_ts = time.timestamp() + offset_secs;
    let bucket_local_ts = local_ts.div_euclid(bucket_secs) * bucket_secs;
    let bucket_utc_ts = bucket_local_ts - offset_secs;
    DateTime::<Utc>::from_timestamp(bucket_utc_ts, 0)
        .unwrap_or(time)
        .with_timezone(&shanghai_offset())
}

async fn models_config_response(db: &SqlitePool) -> Response {
    let settings = model_control_settings(db).await.unwrap_or_default();
    let disabled = disabled_model_set(&settings);
    let catalog = model_catalog(db).await;
    let rows = sqlx::query("SELECT id, available_models_json FROM accounts")
        .fetch_all(db)
        .await
        .unwrap_or_default();
    let model_limits = account_model_rate_limits(db).await.unwrap_or_default();
    let recent_since = (Utc::now() - Duration::hours(24)).to_rfc3339();
    let recent_failures = sqlx::query("SELECT model, COUNT(*) AS count FROM request_traces WHERE started_at >= ? AND status != 'ok' GROUP BY model")
        .bind(recent_since)
        .fetch_all(db)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| {
            let model = row.get::<Option<String>, _>("model")?;
            Some((model_alias(&model), row.get::<i64, _>("count")))
        })
        .collect::<HashMap<_, _>>();
    let coverage = model_account_coverage(&rows);
    let limited = model_limited_account_counts(&model_limits);
    let default_model = settings
        .default_model
        .clone()
        .filter(|model| catalog.iter().any(|item| item.get("id").and_then(Value::as_str).is_some_and(|id| model_alias(id) == *model)))
        .unwrap_or_else(|| {
            catalog
                .first()
                .and_then(|model| model.get("id").and_then(Value::as_str))
                .map(model_alias)
                .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string())
        });
    let models = catalog
        .into_iter()
        .filter_map(|model| {
            let id = model.get("id").and_then(Value::as_str)?;
            let key = model_alias(id);
            Some(json!({
                "id": id,
                "label": model.get("label").and_then(Value::as_str).unwrap_or(id),
                "provider": model.get("provider").and_then(Value::as_str).unwrap_or("windsurf"),
                "creditMultiplier": model.get("creditMultiplier").cloned().unwrap_or(Value::Null),
                "supportsImages": model.get("supportsImages").and_then(Value::as_bool).unwrap_or(false),
                "enabled": !disabled.contains(&key),
                "accountCount": coverage.get(&key).copied().unwrap_or(0),
                "limitedAccountCount": limited.get(&key).copied().unwrap_or(0),
                "recentFailures": recent_failures.get(&key).copied().unwrap_or(0)
            }))
        })
        .collect::<Vec<_>>();
    Json(ApiResponse {
        success: true,
        data: json!({
            "defaultModel": default_model,
            "disabledModels": disabled.into_iter().collect::<Vec<_>>(),
            "models": models
        }),
    })
    .into_response()
}

fn model_account_coverage(rows: &[sqlx::sqlite::SqliteRow]) -> HashMap<String, i64> {
    let mut coverage: HashMap<String, HashSet<i64>> = HashMap::new();
    for row in rows {
        let account_id = row.get::<i64, _>("id");
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
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                coverage.entry(model_alias(id)).or_default().insert(account_id);
            }
        }
    }
    coverage
        .into_iter()
        .map(|(model, accounts)| (model, accounts.len() as i64))
        .collect()
}

fn model_limited_account_counts(model_limits: &HashMap<i64, Value>) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    for limits in model_limits.values() {
        let Some(items) = limits.as_object() else {
            continue;
        };
        for model in items.keys() {
            *counts.entry(model_alias(model)).or_insert(0) += 1;
        }
    }
    counts
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
            "claude-opus-4.7-medium",
            "Claude Opus 4.7 Medium",
            "anthropic",
            8.0,
        ),
        (
            "claude-opus-4.7-high",
            "Claude Opus 4.7 High",
            "anthropic",
            10.0,
        ),
        (
            "claude-opus-4.7-max",
            "Claude Opus 4.7 Max",
            "anthropic",
            12.0,
        ),
        ("claude-opus-4.6", "Claude Opus 4.6", "anthropic", 6.0),
        (
            "claude-opus-4.6-thinking",
            "Claude Opus 4.6 Thinking",
            "anthropic",
            6.0,
        ),
        ("claude-sonnet-4.6", "Claude Sonnet 4.6", "anthropic", 4.0),
        (
            "claude-sonnet-4.6-thinking",
            "Claude Sonnet 4.6 Thinking",
            "anthropic",
            4.0,
        ),
        ("claude-4.5-sonnet", "Claude Sonnet 4.5", "anthropic", 2.0),
        ("gemini-2.5-flash", "Gemini 2.5 Flash", "google", 0.5),
        (
            "chat-gpt-4.1-mini-2025.04.14",
            "GPT-4.1 mini",
            "openai",
            0.5,
        ),
        ("gpt-5-nano", "GPT-5 nano", "openai", 0.5),
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

const MAX_TOOL_DESCRIPTION_LEN: usize = 4096;
const TOOL_DESC_TRUNCATE_SUFFIX: &str =
    " [...truncated; full reference in <tool_documentation> section of system prompt]";
const TOOL_DESCRIPTION_RISK_REPLACEMENT_SYSTEM: &str = "You are a helpful coding assistant. Help users with software engineering tasks. Use the provided tools when needed. Be concise and accurate.";
const DEFAULT_ANONYMOUS_TOOL_NAME_PREFIX: &str = "client_tool";

#[derive(Debug, Clone)]
struct ToolDoc {
    name: String,
    full_description: String,
}

#[derive(Debug, Clone)]
struct ToolRisk {
    risky: bool,
    reason: String,
}

fn messages_from_anthropic(value: &Value) -> Vec<EngineMessage> {
    let Some(items) = value.as_array() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for item in items {
        let Some(item) = item.as_object() else {
            continue;
        };
        let role = item
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user")
            .to_string();
        let content = item.get("content").unwrap_or(&Value::Null);
        if role == "tool" {
            let tool_call_id = item
                .get("tool_call_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            if tool_call_id.is_empty() {
                continue;
            }
            out.push(EngineMessage {
                role,
                content: flatten_tool_result_content(content),
                tool_call_id: Some(tool_call_id.to_string()),
                ..Default::default()
            });
            continue;
        }

        if let Some(text) = content.as_str() {
            out.push(EngineMessage {
                role,
                content: text.to_string(),
                ..Default::default()
            });
            continue;
        }

        if !content.is_array() {
            let text = anthropic_content_to_text(content);
            if !text.trim().is_empty() {
                out.push(EngineMessage {
                    role,
                    content: text,
                    ..Default::default()
                });
            }
            continue;
        }

        let mut text_parts = Vec::new();
        let mut thinking_parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut tool_results = Vec::new();
        for block in content.as_array().unwrap_or(&Vec::new()) {
            let Some(block) = block.as_object() else {
                continue;
            };
            match block.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" => {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        text_parts.push(text.to_string());
                    }
                }
                "thinking" => {
                    if let Some(thinking) = block.get("thinking").and_then(Value::as_str) {
                        thinking_parts.push(thinking.to_string());
                    }
                }
                "tool_use" => {
                    let Some(id) = block.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(name) = block.get("name").and_then(Value::as_str) else {
                        continue;
                    };
                    let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                    let arguments =
                        serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string());
                    tool_calls.push(engine::EngineToolCall {
                        id: id.to_string(),
                        name: name.to_string(),
                        arguments,
                    });
                }
                "tool_result" => {
                    if let Some(tool_call_id) = block.get("tool_use_id").and_then(Value::as_str) {
                        tool_results.push(EngineMessage {
                            role: "tool".to_string(),
                            content: flatten_tool_result_content(
                                block.get("content").unwrap_or(&Value::Null),
                            ),
                            tool_call_id: Some(tool_call_id.to_string()),
                            ..Default::default()
                        });
                    }
                }
                _ => {}
            }
        }

        for tool_result in tool_results {
            out.push(tool_result);
        }

        let content_text = text_parts.join("\n").trim().to_string();
        let reasoning_content = thinking_parts.join("\n").trim().to_string();
        if !content_text.is_empty() || !reasoning_content.is_empty() || !tool_calls.is_empty() {
            out.push(EngineMessage {
                role,
                content: content_text,
                tool_calls,
                reasoning_content: (!reasoning_content.is_empty()).then_some(reasoning_content),
                ..Default::default()
            });
        }
    }
    out
}

fn messages_from_request(payload: &MessagesRequest) -> Vec<EngineMessage> {
    let mut messages = messages_from_anthropic(&payload.messages);
    if let Some(system) = payload.system.as_ref() {
        let content = anthropic_content_to_text(system);
        if !content.trim().is_empty() {
            messages.insert(
                0,
                EngineMessage {
                    role: "system".to_string(),
                    content,
                    ..Default::default()
                },
            );
        }
    }
    messages
}

fn extract_caller_environment(messages: &[EngineMessage]) -> Option<String> {
    let cwd = extract_cwd_from_messages(messages)?;
    let mut lines = vec![format!("- Working directory: {cwd}")];
    if let Some(value) = extract_env_line(
        messages,
        r"(?im)(?:^|\n)\s*(?:[-*]\s*)?Is(?:\s+(?:directory\s+)?(?:a\s+)?)git\s+repo(?:sitory)?\s*[:=]\s*([^\n<]+)",
    ) {
        lines.push(format!("- Is the directory a git repo: {value}"));
    }
    if let Some(value) = extract_env_line(
        messages,
        r"(?im)(?:^|\n)\s*(?:[-*]\s*)?Platform\s*[:=]\s*([^\n<]+)",
    ) {
        lines.push(format!("- Platform: {value}"));
    }
    if let Some(value) = extract_env_line(
        messages,
        r"(?im)(?:^|\n)\s*(?:[-*]\s*)?OS\s+[Vv]ersion\s*[:=]\s*([^\n<]+)",
    ) {
        lines.push(format!("- OS version: {value}"));
    }
    Some(lines.join("\n"))
}

fn extract_cwd_from_messages(messages: &[EngineMessage]) -> Option<String> {
    let path_tail = r#"((?:[\/~]|[A-Za-z]:\\)[^\s`'"<>\n.,;)]+)"#;
    let cwd_patterns = [
        format!(
            r#"(?im)(?:^|\n)\s*(?:[-*]\s*)?(?:(?:Primary|Current|Initial|Default|Active|Project|My)\s+)?(?:Working\s+directory|cwd)\s*[:=]\s*`?{path_tail}`?"#
        ),
        format!(r#"(?im)(?:current\s+working\s+directory(?:\s+is)?)\s*[:=]?\s*`?{path_tail}`?"#),
        format!(r#"(?im)<cwd>\s*{path_tail}\s*</cwd>"#),
    ];
    for pattern in cwd_patterns {
        if let Some(value) = extract_env_line(messages, &pattern) {
            return Some(value);
        }
    }
    scan_user_message_for_bare_cwd(messages).or_else(|| scan_system_for_bullet_cwd(messages))
}

fn extract_env_line(messages: &[EngineMessage], pattern: &str) -> Option<String> {
    let re = Regex::new(pattern).ok()?;
    for message in messages {
        for captures in re.captures_iter(&message.content) {
            for index in 1..captures.len() {
                let Some(value) = captures.get(index).map(|m| m.as_str().trim()) else {
                    continue;
                };
                if valid_environment_value(value) {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

fn scan_user_message_for_bare_cwd(messages: &[EngineMessage]) -> Option<String> {
    let re = Regex::new(r#"(?m)^[\s,;:.，。、；：　"'`(\[]*((?:[A-Za-z]:[\\/]|/[A-Za-z]|~[\\/])[A-Za-z0-9._\\/-]+)"#).ok()?;
    for message in messages {
        if message.role != "user" {
            continue;
        }
        let head = message.content.chars().take(300).collect::<String>();
        let Some(captures) = re.captures(&head) else {
            continue;
        };
        let value = captures.get(1).map(|m| m.as_str().trim())?;
        if valid_cwd_candidate(value) {
            return Some(value.to_string());
        }
    }
    None
}

fn scan_system_for_bullet_cwd(messages: &[EngineMessage]) -> Option<String> {
    let re = Regex::new(
        r#"(?m)^[\s]*[-*•]\s+`?((?:[A-Za-z]:[\\/]|/[A-Za-z]|~[\\/])[^\s`'"<>\n]+)`?\s*$"#,
    )
    .ok()?;
    for message in messages {
        if message.role != "system" {
            continue;
        }
        for captures in re.captures_iter(&message.content) {
            let value = captures.get(1).map(|m| m.as_str().trim())?;
            if valid_cwd_candidate(value) {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn valid_environment_value(value: &str) -> bool {
    !value.is_empty() && value != "<workspace>" && !value.chars().any(|ch| ch.is_control())
}

fn valid_cwd_candidate(value: &str) -> bool {
    valid_environment_value(value)
        && value.len() >= 5
        && Regex::new(r"(?i)\.(?:js|mjs|cjs|ts|tsx|jsx|json|jsonc|md|mdx|py|pyc|go|rs|java|kt|swift|cpp|cc|cxx|c|h|hpp|html?|css|scss|sass|less|yaml|yml|toml|ini|cfg|conf|sh|bash|zsh|fish|ps1|bat|cmd|exe|dll|so|dylib|zip|tar|gz|bz2|xz|7z|rar|png|jpe?g|gif|webp|svg|ico|mp[34]|wav|flac|ogg|webm|mov|avi|mkv|pdf|docx?|xlsx?|pptx?|csv|tsv|sql|db|sqlite|log|lock|map|min\.js|min\.css)$")
            .map(|re| !re.is_match(value))
            .unwrap_or(true)
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
                    .or_else(|| {
                        item.get("thinking")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
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

fn flatten_tool_result_content(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(items) = value.as_array() {
        let text = items
            .iter()
            .filter_map(|item| {
                item.get("type")
                    .and_then(Value::as_str)
                    .filter(|kind| *kind == "text")
                    .and_then(|_| item.get("text").and_then(Value::as_str))
                    .map(str::to_string)
            })
            .collect::<Vec<_>>()
            .join("\n");
        if items.iter().any(|item| {
            item.get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        }) {
            return format!("Error: {}", text);
        }
        return text;
    }
    value.to_string()
}

fn estimate_input_tokens_from_messages(messages: &[EngineMessage]) -> u64 {
    let mut total = 0_u64;
    for message in messages {
        total += estimate_local_tokens(&message.role);
        total += estimate_local_tokens(&message.content);
        if let Some(reasoning) = message.reasoning_content.as_deref() {
            total += estimate_local_tokens(reasoning);
        }
        if let Some(tool_call_id) = message.tool_call_id.as_deref() {
            total += estimate_local_tokens(tool_call_id);
        }
        for tool_call in &message.tool_calls {
            total += estimate_local_tokens(&tool_call.id);
            total += estimate_local_tokens(&tool_call.name);
            total += estimate_local_tokens(&tool_call.arguments);
        }
    }
    total.max(1)
}

fn estimate_local_tokens(text: &str) -> u64 {
    ((text.chars().count() as f64 / 3.5).ceil() as u64).max(1)
}

fn message_debug_summary(payload: &MessagesRequest) -> MessageDebugSummary {
    let engine_messages = messages_from_request(payload);
    let roles = engine_messages
        .iter()
        .map(|message| message.role.clone())
        .collect::<Vec<_>>();
    let metadata_keys = payload
        .metadata
        .as_object()
        .map(|items| items.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    MessageDebugSummary {
        message_count: payload.messages.as_array().map(Vec::len).unwrap_or(0),
        roles,
        system_chars: payload
            .system
            .as_ref()
            .map(anthropic_content_to_text)
            .map(|text| text.chars().count())
            .unwrap_or(0),
        tool_count: payload
            .tools
            .as_ref()
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0),
        has_tool_choice: payload.tool_choice.is_some(),
        metadata_keys,
        input_chars: engine_messages
            .iter()
            .map(|message| message.content.chars().count())
            .sum(),
    }
}

fn sanitized_message_request(payload: &MessagesRequest, summary: &MessageDebugSummary) -> Value {
    json!({
        "model": payload.model,
        "stream": payload.stream.unwrap_or(false),
        "maxTokens": payload.max_tokens,
        "messageCount": summary.message_count,
        "roles": summary.roles,
        "systemChars": summary.system_chars,
        "toolCount": summary.tool_count,
        "hasToolChoice": summary.has_tool_choice,
        "metadataKeys": summary.metadata_keys,
        "inputChars": summary.input_chars
    })
}

const TOOL_DESCRIPTION_RISK_SIGNATURES: &[(&str, &str)] = &[
    (
        "Claude Code 67 工具",
        "Agent,AskUserQuestion,Bash,CronCreate,CronDelete,CronList,Edit,EnterPlanMode,EnterWorktree,ExitPlanMode,ExitWorktree,Glob",
    ),
    (
        "Shell/Edit 工具客户端",
        "Shell,Glob,Grep,AwaitShell,Read,Delete,Edit,Write",
    ),
    (
        "Process/Browser 工具客户端",
        "str-replace-editor,open-browser,diagnostics,read-terminal,git-commit-retrieval,launch-process,kill-process,read-process",
    ),
    (
        "OpenClaw 默认工具",
        "agents_list,browser,canvas,edit,exec,image,js-reverse__break_on_xhr,js-reverse__evaluate_script,js-reverse__get_paused_info,js-reverse__get_request_initiator,js-reverse__get_script_source,js-reverse__get_websocket_messages",
    ),
    (
        "OpenClaw 快照工具",
        "canvas,nodes,cron,message,tts,gateway,agents_list,sessions_list,sessions_history,sessions_send,sessions_spawn,sessions_yield,subagents,session_status,web_search,web_fetch",
    ),
];

#[derive(Debug, Clone)]
struct ToolIsolation {
    tools: Vec<EngineTool>,
    messages: Vec<EngineMessage>,
    tool_choice: EngineToolChoice,
    truncated_docs: Vec<ToolDoc>,
    to_client_name: HashMap<String, String>,
}

fn is_probe_request(payload: &MessagesRequest) -> bool {
    payload.max_tokens == Some(1) && payload.stream == Some(false)
}

fn send_probe_response(model: &str) -> Response {
    Json(json!({
        "id": format!("msg_probe_{}", Uuid::new_v4().simple()),
        "type": "message",
        "role": "assistant",
        "content": [{ "type": "text", "text": "" }],
        "model": if model.trim().is_empty() { "claude-haiku-4-5-20251001" } else { model },
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 1,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0
        }
    }))
    .into_response()
}

fn empty_anthropic_message_response(trace_id: String, model: String) -> Response {
    Json(json!({
        "id": format!("msg_{}", trace_id),
        "type": "message",
        "role": "assistant",
        "content": [{ "type": "text", "text": "" }],
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 1,
            "output_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "service_tier": "standard"
        }
    }))
    .into_response()
}

fn empty_anthropic_stream_response(trace_id: String, model: String) -> Response {
    let s = stream! {
        yield Ok::<Event, std::convert::Infallible>(anthropic_sse_event("message_start", json!({
            "type": "message_start",
            "message": {
                "id": format!("msg_{}", trace_id),
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": { "input_tokens": 1, "output_tokens": 0 }
            }
        })));
        yield Ok(anthropic_sse_event("content_block_start", json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "text", "text": "" }
        })));
        yield Ok(anthropic_sse_event("content_block_stop", json!({
            "type": "content_block_stop",
            "index": 0
        })));
        yield Ok(anthropic_sse_event("message_delta", json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {
                "input_tokens": 1,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "service_tier": "standard"
            }
        })));
        yield Ok(anthropic_sse_event("message_stop", json!({"type": "message_stop"})));
    };
    Sse::new(s).keep_alive(KeepAlive::default()).into_response()
}

fn is_claude_code_client(headers: &HeaderMap) -> bool {
    let user_agent = headers
        .get("user-agent")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    if user_agent
        .split_whitespace()
        .any(|value| value.contains("claude-cli") || value.contains("claude-code"))
    {
        return true;
    }
    headers
        .get("x-claude-code-session-id")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| !value.is_empty())
}

fn detect_tool_description_risk_client(headers: &HeaderMap, tools: &[EngineTool]) -> ToolRisk {
    let header_hit = headers.iter().find_map(|(name, value)| {
        let name_hit = name.as_str().contains("claude-cli")
            || name.as_str().contains("claude-code")
            || name.as_str().contains("augment")
            || name.as_str().contains("cursor");
        let value_hit = value
            .to_str()
            .ok()
            .map(|value| value.to_ascii_lowercase())
            .is_some_and(|v| {
                v.contains("claude-cli")
                    || v.contains("claude-code")
                    || v.contains("augment")
                    || v.contains("cursor")
            });
        if name_hit || value_hit {
            Some(format!("header:{}", name.as_str().to_lowercase()))
        } else {
            None
        }
    });
    if let Some(reason) = header_hit {
        return ToolRisk {
            risky: true,
            reason,
        };
    }
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    for (idx, (_, sequence)) in TOOL_DESCRIPTION_RISK_SIGNATURES.iter().enumerate() {
        let signature = sequence
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .collect::<Vec<_>>();
        if has_ordered_tool_prefix(&tool_names, &signature) {
            return ToolRisk {
                risky: true,
                reason: format!(
                    "tools:configured-sequence:{}:{}",
                    idx + 1,
                    TOOL_DESCRIPTION_RISK_SIGNATURES[idx].0
                ),
            };
        }
    }
    let known = [
        "Agent",
        "AskUserQuestion",
        "Bash",
        "Edit",
        "Glob",
        "Grep",
        "Read",
    ];
    let hits = known
        .iter()
        .filter(|name| tools.iter().any(|tool| tool.name == **name))
        .count();
    if tools.len() >= 20 && hits >= 5 {
        return ToolRisk {
            risky: true,
            reason: format!("tools:claude-code-signature:{}", known[..hits].join(",")),
        };
    }
    ToolRisk {
        risky: false,
        reason: "none".to_string(),
    }
}

fn has_ordered_tool_prefix(tool_names: &[&str], signature: &[&str]) -> bool {
    signature
        .iter()
        .enumerate()
        .all(|(idx, name)| tool_names.get(idx).is_some_and(|value| value == name))
}

fn sanitize_tools(raw: Option<&Value>) -> (Vec<EngineTool>, Vec<ToolDoc>) {
    let Some(items) = raw.and_then(Value::as_array) else {
        return (Vec::new(), Vec::new());
    };
    let mut tools = Vec::new();
    let mut truncated_docs = Vec::new();
    for item in items {
        let Some(tool_obj) = item.as_object() else {
            continue;
        };
        let (name, description_raw, parameters_raw) =
            if tool_obj.get("type").and_then(Value::as_str) == Some("function") {
                let Some(func) = tool_obj.get("function").and_then(Value::as_object) else {
                    continue;
                };
                (
                    func.get("name").and_then(Value::as_str),
                    func.get("description").and_then(Value::as_str),
                    func.get("parameters"),
                )
            } else {
                (
                    tool_obj.get("name").and_then(Value::as_str),
                    tool_obj.get("description").and_then(Value::as_str),
                    tool_obj.get("input_schema"),
                )
            };
        let Some(name) = name else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        let mut description = description_raw.map(str::to_string);
        if let Some(desc) = description.as_ref() {
            let bytes = desc.as_bytes();
            if bytes.len() > MAX_TOOL_DESCRIPTION_LEN {
                let keep = MAX_TOOL_DESCRIPTION_LEN.saturating_sub(TOOL_DESC_TRUNCATE_SUFFIX.len());
                let mut end = keep.min(bytes.len());
                while end > 0 && !desc.is_char_boundary(end) {
                    end -= 1;
                }
                let full_description = desc.clone();
                description = Some(format!("{}{}", &desc[..end], TOOL_DESC_TRUNCATE_SUFFIX));
                truncated_docs.push(ToolDoc {
                    name: name.to_string(),
                    full_description,
                });
            }
        }
        let parameters = parameters_raw
            .and_then(Value::as_object)
            .map(|_| strip_json_schema_meta(parameters_raw.unwrap()));
        tools.push(EngineTool {
            name: name.to_string(),
            description,
            parameters,
        });
    }
    (tools, truncated_docs)
}

fn strip_json_schema_meta(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(strip_json_schema_meta).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                if matches!(
                    key.as_str(),
                    "$schema" | "$id" | "$defs" | "$ref" | "definitions" | "additionalProperties"
                ) {
                    continue;
                }
                out.insert(key.clone(), strip_json_schema_meta(value));
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

fn strip_risky_tool_schema_fields(value: &Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(strip_risky_tool_schema_fields).collect())
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                if matches!(
                    key.as_str(),
                    "description"
                        | "title"
                        | "markdownDescription"
                        | "examples"
                        | "example"
                        | "default"
                        | "const"
                        | "enum"
                        | "$comment"
                        | "enumDescriptions"
                        | "markdownEnumDescriptions"
                        | "oneOf"
                        | "anyOf"
                        | "allOf"
                        | "not"
                        | "propertyNames"
                        | "patternProperties"
                        | "unevaluatedProperties"
                        | "unevaluatedItems"
                        | "dependencies"
                        | "dependentRequired"
                        | "dependentSchemas"
                        | "if"
                        | "then"
                        | "else"
                        | "minProperties"
                        | "maxProperties"
                        | "contains"
                        | "minContains"
                        | "maxContains"
                        | "contentMediaType"
                        | "contentEncoding"
                        | "contentSchema"
                        | "deprecated"
                        | "readOnly"
                        | "writeOnly"
                        | "$anchor"
                        | "$vocabulary"
                        | "$dynamicAnchor"
                        | "$dynamicRef"
                ) {
                    continue;
                }
                out.insert(key.clone(), strip_risky_tool_schema_fields(value));
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

fn shorten_tool_descriptions_for_risk_client(tools: &mut [EngineTool]) {
    for tool in tools {
        tool.description = Some("Available tool.".to_string());
        if let Some(parameters) = tool.parameters.as_mut() {
            *parameters = strip_risky_tool_schema_fields(parameters);
        }
    }
}

fn replace_system_prompt_for_tool_description_risk(messages: &mut Vec<EngineMessage>) {
    let mut replaced = false;
    for message in messages.iter_mut() {
        if message.role == "system" {
            message.content = TOOL_DESCRIPTION_RISK_REPLACEMENT_SYSTEM.to_string();
            replaced = true;
            break;
        }
    }
    if !replaced {
        messages.insert(
            0,
            EngineMessage {
                role: "system".to_string(),
                content: TOOL_DESCRIPTION_RISK_REPLACEMENT_SYSTEM.to_string(),
                ..Default::default()
            },
        );
    }
}

fn build_tool_doc_block(truncated_docs: &[ToolDoc]) -> String {
    if truncated_docs.is_empty() {
        return String::new();
    }
    let mut out = String::from(
        "\n\n<tool_documentation>\nThe following tool descriptions were truncated for transport size limits. Use this section as the authoritative reference for full tool usage, edge cases, and examples:\n",
    );
    for doc in truncated_docs {
        out.push_str("\n## ");
        out.push_str(&doc.name);
        out.push_str("\n\n");
        out.push_str(&doc.full_description);
        out.push_str("\n");
    }
    out.push_str("</tool_documentation>");
    out
}

fn inject_tool_docs_into_system(
    messages: Vec<EngineMessage>,
    truncated_docs: &[ToolDoc],
) -> Vec<EngineMessage> {
    let block = build_tool_doc_block(truncated_docs);
    if block.is_empty() {
        return messages;
    }
    let mut out = messages;
    for message in &mut out {
        if message.role == "system" {
            message.content.push_str(&block);
            return out;
        }
    }
    out.insert(
        0,
        EngineMessage {
            role: "system".to_string(),
            content: block.trim_start().to_string(),
            ..Default::default()
        },
    );
    out
}

fn isolate_tool_names(
    tools: Vec<EngineTool>,
    messages: Vec<EngineMessage>,
    tool_choice: EngineToolChoice,
    truncated_docs: Vec<ToolDoc>,
    anonymous_tool_name_prefix: &str,
) -> anyhow::Result<ToolIsolation> {
    if tools.is_empty() {
        return Ok(ToolIsolation {
            tools,
            messages,
            tool_choice,
            truncated_docs,
            to_client_name: HashMap::new(),
        });
    }
    let prefix = anonymous_tool_name_prefix.trim();
    let prefix = if prefix.is_empty() {
        DEFAULT_ANONYMOUS_TOOL_NAME_PREFIX
    } else {
        prefix
    };
    if !prefix
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
    {
        return Err(anyhow::anyhow!(
            "匿名工具名前缀只能包含英文字母、数字、下划线或连字符"
        ));
    }
    let mut to_upstream_name = HashMap::new();
    let mut to_client_name = HashMap::new();
    for (index, tool) in tools.iter().enumerate() {
        let client_name = tool.name.clone();
        let upstream_name = format!(
            "{prefix}_{index}_{}",
            short_hash(&client_name).chars().take(8).collect::<String>()
        );
        to_upstream_name.insert(client_name.clone(), upstream_name.clone());
        to_client_name.insert(upstream_name, client_name);
    }
    let map_name = |name: &str| {
        to_upstream_name
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    };
    let isolated_tools = tools
        .into_iter()
        .map(|mut tool| {
            tool.name = map_name(&tool.name);
            tool
        })
        .collect::<Vec<_>>();
    let isolated_messages = messages
        .into_iter()
        .map(|mut message| {
            for tool_call in &mut message.tool_calls {
                tool_call.name = map_name(&tool_call.name);
            }
            message
        })
        .collect::<Vec<_>>();
    let isolated_tool_choice = match tool_choice {
        EngineToolChoice::Function { name } => EngineToolChoice::Function {
            name: map_name(&name),
        },
        other => other,
    };
    let isolated_truncated_docs = truncated_docs
        .into_iter()
        .map(|mut doc| {
            doc.name = map_name(&doc.name);
            doc
        })
        .collect::<Vec<_>>();
    Ok(ToolIsolation {
        tools: isolated_tools,
        messages: isolated_messages,
        tool_choice: isolated_tool_choice,
        truncated_docs: isolated_truncated_docs,
        to_client_name,
    })
}

fn restore_tool_name(name: &str, to_client_name: &HashMap<String, String>) -> String {
    to_client_name
        .get(name)
        .cloned()
        .unwrap_or_else(|| name.to_string())
}

fn sanitize_tool_choice(value: Option<&Value>) -> EngineToolChoice {
    let Some(value) = value else {
        return EngineToolChoice::Auto;
    };
    if let Some(kind) = value.as_str() {
        return match kind {
            "none" => EngineToolChoice::None,
            "required" | "any" => EngineToolChoice::Required,
            _ => EngineToolChoice::Auto,
        };
    }
    let Some(obj) = value.as_object() else {
        return EngineToolChoice::Auto;
    };
    match obj.get("type").and_then(Value::as_str).unwrap_or("auto") {
        "none" => EngineToolChoice::None,
        "required" | "any" => EngineToolChoice::Required,
        "tool" | "function" => {
            let name = obj
                .get("name")
                .and_then(Value::as_str)
                .or_else(|| {
                    obj.get("function")
                        .and_then(|value| value.get("name"))
                        .and_then(Value::as_str)
                })
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                EngineToolChoice::Auto
            } else {
                EngineToolChoice::Function { name }
            }
        }
        _ => EngineToolChoice::Auto,
    }
}

#[derive(Debug, Clone)]
struct ToolChoiceDegrade {
    tool_choice: EngineToolChoice,
    messages: Vec<EngineMessage>,
}

fn degrade_tool_choice_for_upstream(
    tool_choice: EngineToolChoice,
    messages: Vec<EngineMessage>,
    to_client_name: &HashMap<String, String>,
) -> ToolChoiceDegrade {
    match tool_choice {
        EngineToolChoice::Auto | EngineToolChoice::None => ToolChoiceDegrade {
            tool_choice,
            messages,
        },
        EngineToolChoice::Required => ToolChoiceDegrade {
            tool_choice: EngineToolChoice::Auto,
            messages: append_system_hint(
                messages,
                "You MUST call one of the provided tools to answer this request. Do not respond with plain text - every response must include a tool call.",
            ),
        },
        EngineToolChoice::Function { name } => {
            let client_name = restore_tool_name(&name, to_client_name);
            ToolChoiceDegrade {
                tool_choice: EngineToolChoice::Auto,
                messages: append_system_hint(
                    messages,
                    &format!(
                        "You MUST call the tool named \"{}\" to answer this request. Do not call any other tool, and do not respond with plain text.",
                        client_name
                    ),
                ),
            }
        }
    }
}

fn append_system_hint(mut messages: Vec<EngineMessage>, hint: &str) -> Vec<EngineMessage> {
    for message in &mut messages {
        if message.role == "system" {
            if !message.content.is_empty() {
                message.content.push_str("\n\n");
            }
            message.content.push_str(hint);
            return messages;
        }
    }
    messages.insert(
        0,
        EngineMessage {
            role: "system".to_string(),
            content: hint.to_string(),
            ..Default::default()
        },
    );
    messages
}

fn sampling_params_from_request(payload: &MessagesRequest) -> Option<EngineSamplingParams> {
    let params = EngineSamplingParams {
        max_tokens: payload.max_tokens,
        max_tool_calls: None,
        temperature: payload.temperature,
        top_p: payload.top_p,
        top_k: payload.top_k,
        frequency_penalty: payload.frequency_penalty,
        presence_penalty: payload.presence_penalty,
    };
    if params.max_tokens.is_some()
        || params.temperature.is_some()
        || params.top_p.is_some()
        || params.top_k.is_some()
        || params.frequency_penalty.is_some()
        || params.presence_penalty.is_some()
    {
        Some(params)
    } else {
        None
    }
}

fn anthropic_sse_event(event: &'static str, data: Value) -> Event {
    Event::default().event(event).data(data.to_string())
}

fn anthropic_text_delta(index: u64, text: String) -> Value {
    json!({
        "type": "content_block_delta",
        "index": index,
        "delta": { "type": "text_delta", "text": text }
    })
}

fn switch_anthropic_block(
    active_block: &mut Option<(String, u64)>,
    next_block_index: &mut u64,
    kind: &str,
    content_block: Value,
) -> (u64, Vec<Event>) {
    if let Some((active_kind, index)) = active_block.as_ref() {
        if active_kind == kind {
            return (*index, Vec::new());
        }
    }
    let mut events = Vec::new();
    if let Some((_, index)) = active_block.take() {
        events.push(anthropic_sse_event(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        ));
    }
    let index = *next_block_index;
    *next_block_index += 1;
    events.push(anthropic_sse_event(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": index,
            "content_block": content_block
        }),
    ));
    *active_block = Some((kind.to_string(), index));
    (index, events)
}

fn resolve_engine_model(model: &str) -> EngineModel {
    let key = model_alias(model);
    let model_uid = match key.as_str() {
        "claude-4.1-opus" => Some("MODEL_CLAUDE_4_1_OPUS".to_string()),
        "claude-4.5-opus" => Some("MODEL_CLAUDE_4_5_OPUS".to_string()),
        "claude-4.5-sonnet" => Some("MODEL_PRIVATE_2".to_string()),
        "claude-sonnet-4.6" => Some("claude-sonnet-4-6".to_string()),
        "claude-sonnet-4.6-thinking" => Some("claude-sonnet-4-6-thinking".to_string()),
        "claude-opus-4.6" => Some("claude-opus-4-6".to_string()),
        "claude-opus-4.6-thinking" => Some("claude-opus-4-6-thinking".to_string()),
        "claude-opus-4-7-low" => Some("claude-opus-4-7-low".to_string()),
        "claude-opus-4-7-high" => Some("claude-opus-4-7-high".to_string()),
        "claude-opus-4-7-xhigh" => Some("claude-opus-4-7-xhigh".to_string()),
        "claude-opus-4-7-max" => Some("claude-opus-4-7-max".to_string()),
        "claude-opus-4-7-medium" => Some("claude-opus-4-7-medium".to_string()),
        "gpt-5-4-low" => Some("gpt-5-4-low".to_string()),
        "gpt-5-3-codex-medium" => Some("gpt-5-3-codex-medium".to_string()),
        "kimi-k2-5" => Some("kimi-k2-5".to_string()),
        "glm-5-1" => Some("glm-5-1".to_string()),
        "swe-1-6" => Some("swe-1-6".to_string()),
        "swe-1-6-fast" => Some("swe-1-6-fast".to_string()),
        "gemini-2.5-flash" => Some("MODEL_GOOGLE_GEMINI_2_5_FLASH".to_string()),
        "chat-gpt-4.1-mini-2025.04.14" => Some("MODEL_CHAT_GPT_4_1_MINI_2025_04_14".to_string()),
        "chat-gpt-4.1-2025.04.14" => Some("MODEL_CHAT_GPT_4_1_2025_04_14".to_string()),
        "gpt-5-nano" => Some("MODEL_GPT_5_NANO".to_string()),
        other => Some(other.to_string()),
    };
    EngineModel { id: key, model_uid }
}

fn model_alias(model: &str) -> String {
    match model.trim() {
        "claude-opus" => "claude-opus-4-7-medium".to_string(),
        "claude-opus-4-1" | "claude-opus-4.1" | "claude-opus-4-1-20250805" => {
            "claude-4.1-opus".to_string()
        }
        "claude-opus-4-5" | "claude-opus-4.5" | "claude-opus-4-5-20251101" => {
            "claude-4.5-opus".to_string()
        }
        "opus4.6" | "opus-4.6" | "opus-4-6" | "claude-opus-4-6" | "claude-opus-4.6" => {
            "claude-opus-4.6".to_string()
        }
        "claude-opus-4-6-thinking" | "claude-opus-4.6-thinking" => {
            "claude-opus-4.6-thinking".to_string()
        }
        "claude-sonnet" => "claude-sonnet-4.6".to_string(),
        "sonnet4.6" | "sonnet-4.6" | "sonnet-4-6" | "claude-sonnet-4-6" | "claude-sonnet-4.6" => {
            "claude-sonnet-4.6".to_string()
        }
        "claude-sonnet-4-6-thinking" | "claude-sonnet-4.6-thinking" => {
            "claude-sonnet-4.6-thinking".to_string()
        }
        "claude-opus-4-7" | "claude-opus-4.7" | "claude-opus-4-7-latest" => {
            "claude-opus-4-7-medium".to_string()
        }
        "claude-opus-4.7-medium" => "claude-opus-4-7-medium".to_string(),
        "claude-opus-4.7-low" => "claude-opus-4-7-low".to_string(),
        "claude-opus-4.7-high" => "claude-opus-4-7-high".to_string(),
        "claude-opus-4.7-xhigh" => "claude-opus-4-7-xhigh".to_string(),
        "claude-opus-4.7-max" => "claude-opus-4-7-max".to_string(),
        "gpt-5" | "gpt-5.4" | "gpt-5.4-low" => "gpt-5-4-low".to_string(),
        "gpt-5-codex" | "gpt-5.3-codex" => "gpt-5-3-codex-medium".to_string(),
        "kimi-k2" | "kimi-k2.5" => "kimi-k2-5".to_string(),
        "glm-5" | "glm-5.1" => "glm-5-1".to_string(),
        "swe-1" | "swe-1.6" => "swe-1-6".to_string(),
        "swe-1.6-fast" => "swe-1-6-fast".to_string(),
        other => other.trim().to_string(),
    }
}

fn log_model_resolution(trace_id: &str, requested_model: &str, engine_model: &EngineModel) {
    tracing::info!(
        trace_id = %trace_id,
        requested_model = %requested_model,
        resolved_model = %engine_model.id,
        upstream_model = %engine_model.model_uid.as_deref().unwrap_or(&engine_model.id),
        "model resolved for windsurf upstream"
    );
}

async fn api_not_found() -> impl IntoResponse {
    error(StatusCode::NOT_FOUND, "not_found", "接口不存在")
}

async fn run_login_job(
    db: SqlitePool,
    events: broadcast::Sender<AdminEvent>,
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
                    &events,
                    &job_id,
                    "failed",
                    json!({
                        "index": index,
                        "email": line,
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
        let email = entry.email.clone();
        let email_masked = mask_email(&entry.email);
        let _ = add_job_event(
            &db,
            &events,
            &job_id,
            "progress",
            json!({
                "index": index,
                "total": lines.len(),
                "email": email,
                "emailMasked": email_masked,
                "status": "running",
                "message": "开始处理"
            }),
        )
        .await;

        let existing_account = find_existing_login_account(&db, &entry.email).await;
        if existing_account
            .as_ref()
            .is_some_and(|account| account.normal)
        {
            let account_id = existing_account.as_ref().map(|account| account.id);
            let _ = sqlx::query(
                "UPDATE login_jobs SET success_count=success_count+1, updated_at=? WHERE id=?",
            )
            .bind(now())
            .bind(&job_id)
            .execute(&db)
            .await;
            let _ = add_job_event(
                &db,
                &events,
                &job_id,
                "skipped",
                json!({
                    "index": index,
                    "email": entry.email,
                    "emailMasked": email_masked,
                    "accountId": account_id,
                    "message": "号池中已有可用账号，已跳过"
                }),
            )
            .await;
            continue;
        }

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
                if let Some(id) = account_id {
                    emit_account_event(&events, "imported", id);
                }
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
                    &events,
                    &job_id,
                    "success",
                    json!({
                        "index": index,
                        "email": entry.email,
                        "emailMasked": email_masked,
                        "accountId": account_id,
                        "message": if existing_account.is_some() { "已更新号池中的账号" } else { "账号已添加" }
                    }),
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
                    &events,
                    &job_id,
                    "failed",
                    json!({
                        "index": index,
                        "email": entry.email,
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
                &events,
                &job_id,
                "waiting",
                json!({
                    "seconds": wait,
                    "reason": if success { "normal" } else { "failed" },
                    "message": if success { "等待后继续处理下一个账号" } else { "失败后等待，随后继续处理下一个账号" }
                }),
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
            &events,
            &job_id,
            "done",
            json!({
                "successCount": success_count,
                "failedCount": failed_count,
                "message": "批量导入已完成"
            }),
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
    events: &broadcast::Sender<AdminEvent>,
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
    emit_admin_event(
        events,
        "login_job_changed",
        json!({ "jobId": job_id, "eventType": event_type, "payload": payload }),
    );
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

async fn find_existing_login_account(db: &SqlitePool, email: &str) -> Option<ExistingLoginAccount> {
    let row = sqlx::query(
        "SELECT id, status, error_count, last_error, rate_limited_until FROM accounts WHERE lower(email)=lower(?) ORDER BY id DESC LIMIT 1",
    )
    .bind(email)
    .fetch_optional(db)
    .await
    .ok()
    .flatten()?;
    let rate_limited_until = row.get::<Option<String>, _>("rate_limited_until");
    let rate_limited = rate_limited_until.as_deref().is_some_and(|value| {
        chrono::DateTime::parse_from_rfc3339(value)
            .map(|time| time.with_timezone(&Utc) > Utc::now())
            .unwrap_or(false)
    });
    let status = row.get::<String, _>("status");
    let normal = matches!(status.as_str(), "ready" | "active" | "ok")
        && row.get::<i64, _>("error_count") == 0
        && row.get::<Option<String>, _>("last_error").is_none()
        && !rate_limited;
    Some(ExistingLoginAccount {
        id: row.get::<i64, _>("id"),
        normal,
    })
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
        let _ = sqlx::query("UPDATE accounts SET label=?, status='ready', priority=?, max_concurrent=?, proxy_id=?, credentials_json=?, credential_mask=?, auth_method=?, api_server_url=?, last_login_at=?, error_count=0, last_error=NULL, rate_limited_until=NULL, rate_limit_probe_after=NULL, updated_at=? WHERE id=?")
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
    account_credentials(row).map(|credentials| credentials.api_key)
}

fn account_credentials(row: &sqlx::sqlite::SqliteRow) -> Option<AccountCredentials> {
    let raw = row.get::<Option<String>, _>("credentials_json");
    account_credentials_from_raw(raw.as_deref())
}

async fn refresh_account_status(
    db: &SqlitePool,
    data_dir: &PathBuf,
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
    if let Err(err) = write_account_status_snapshot(data_dir, id, &user_status) {
        tracing::warn!(
            account_id = id,
            error = %redact_log_text(&err.to_string()),
            "account status snapshot write failed"
        );
    }
    let user_status_summary = account_user_status_summary(&credits);
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
        .bind(user_status_summary.to_string())
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
        "userStatus": user_status_summary,
        "rateLimit": rate_limit,
        "availableModels": available_models,
        "tierModels": tier_models,
        "lastProbedAt": now_text
    }))
}

fn write_account_status_snapshot(data_dir: &PathBuf, account_id: i64, value: &Value) -> anyhow::Result<()> {
    let dir = data_dir.join("account-status");
    std::fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create account status directory {}",
            dir.display()
        )
    })?;
    let path = dir.join(format!("account-{account_id}.json"));
    std::fs::write(&path, serde_json::to_vec(value)?)
        .with_context(|| format!("failed to write account status snapshot {}", path.display()))?;
    Ok(())
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

async fn mark_account_error_with_events(
    db: &SqlitePool,
    events: &broadcast::Sender<AdminEvent>,
    id: i64,
    message: &str,
) -> anyhow::Result<()> {
    mark_account_error(db, id, message).await?;
    emit_admin_event(
        events,
        "account_error",
        json!({ "accountId": id, "message": message }),
    );
    Ok(())
}

async fn mark_account_transient_error(
    db: &SqlitePool,
    id: i64,
    message: &str,
) -> anyhow::Result<()> {
    let now_text = now();
    sqlx::query("UPDATE accounts SET last_error=?, updated_at=? WHERE id=?")
        .bind(message.chars().take(200).collect::<String>())
        .bind(now_text)
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

async fn mark_account_transient_error_with_events(
    db: &SqlitePool,
    events: &broadcast::Sender<AdminEvent>,
    id: i64,
    message: &str,
) -> anyhow::Result<()> {
    mark_account_transient_error(db, id, message).await?;
    emit_admin_event(
        events,
        "account_transient_error",
        json!({ "accountId": id, "message": message }),
    );
    Ok(())
}

async fn mark_account_probe_success(db: &SqlitePool, id: i64) -> anyhow::Result<()> {
    let now_text = now();
    sqlx::query("UPDATE accounts SET status='ready', error_count=0, last_error=NULL, last_probed_at=?, updated_at=? WHERE id=?")
        .bind(&now_text)
        .bind(&now_text)
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

async fn mark_account_probe_success_with_events(
    db: &SqlitePool,
    events: &broadcast::Sender<AdminEvent>,
    id: i64,
) -> anyhow::Result<()> {
    mark_account_probe_success(db, id).await?;
    emit_account_event(events, "probe_success", id);
    Ok(())
}

fn is_transient_probe_error(message: &str) -> bool {
    is_transient_upstream_error(message)
}

fn is_transient_upstream_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("transport")
        || text.contains("econnreset")
        || text.contains("stream")
        || text.contains("timeout")
        || text.contains("timed out")
        || text.contains("connection")
        || text.contains("rate limit")
        || text.contains("rate_limit")
        || text.contains("global rate limit")
        || text.contains("provider")
        || text.contains("temporarily")
        || text.contains("超时")
}

async fn retry_budget_for_account_pool(
    db: &SqlitePool,
    capacity: &CapacitySettings,
    trace_id: &str,
    phase: &str,
) -> i64 {
    let configured = capacity.max_retries.max(0);
    let ready_accounts = count_ready_accounts(db).await.unwrap_or_else(|err| {
        tracing::warn!(
            trace_id = %trace_id,
            phase,
            error = %redact_log_text(&err.to_string()),
            "messages account retry budget count failed"
        );
        0
    });
    let budget = ready_accounts.saturating_sub(1).max(configured);
    budget.clamp(1, MAX_DYNAMIC_ACCOUNT_RETRIES)
}

async fn count_ready_accounts(db: &SqlitePool) -> anyhow::Result<i64> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS count FROM accounts WHERE status IN ('ready', 'active', 'ok')",
    )
    .fetch_one(db)
    .await?;
    Ok(row.get::<i64, _>("count"))
}

fn is_retryable_before_output_error(message: &str) -> bool {
    !is_fatal_account_error(message)
        && (is_upstream_rate_limit_error(message)
            || is_transient_upstream_error(message)
            || is_quota_exhausted_error(message))
}

fn is_quota_exhausted_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    (text.contains("quota") && (text.contains("exhausted") || text.contains("usage")))
        || text.contains("weekly usage quota")
        || text.contains("usage quota has been exhausted")
}

fn classify_upstream_rate_limit(
    message: &str,
    capacity: &CapacitySettings,
) -> Option<UpstreamRateLimit> {
    let text = message.to_ascii_lowercase();
    if is_quota_exhausted_error(message) {
        return Some(UpstreamRateLimit {
            scope: UpstreamRateLimitScope::Account,
            retry_after_secs: capacity.suspicious_cooldown_secs,
        });
    }
    let retry_after_secs = parse_retry_after_secs(message);
    if text.contains("reached message rate limit for this model")
        || text.contains("message rate limit for this model")
        || text.contains("resource_exhausted")
        || text.contains("third-party model provider is experiencing issues")
        || text.contains("model provider is experiencing issues")
    {
        return Some(UpstreamRateLimit {
            scope: UpstreamRateLimitScope::Model,
            retry_after_secs: retry_after_secs.unwrap_or(capacity.model_cooldown_secs),
        });
    }
    if text.contains("global rate limit")
        || text.contains("over their global rate limit")
        || text.contains("rate_limit")
        || text.contains("rate limit")
    {
        return Some(UpstreamRateLimit {
            scope: UpstreamRateLimitScope::Account,
            retry_after_secs: retry_after_secs.unwrap_or(capacity.suspicious_cooldown_secs),
        });
    }
    None
}

fn classify_account_failure(message: &str, capacity: &CapacitySettings) -> AccountFailureAction {
    if let Some(limit) = classify_upstream_rate_limit(message, capacity) {
        return AccountFailureAction::RateLimit(limit);
    }
    let text = message.to_ascii_lowercase();
    if text.contains("checkchatcapacity returned no capacity") {
        return AccountFailureAction::RateLimit(UpstreamRateLimit {
            scope: UpstreamRateLimitScope::Model,
            retry_after_secs: capacity.model_cooldown_secs,
        });
    }
    if text.contains("checkusermessageratelimit returned no capacity") {
        return AccountFailureAction::RateLimit(UpstreamRateLimit {
            scope: UpstreamRateLimitScope::Account,
            retry_after_secs: capacity.suspicious_cooldown_secs,
        });
    }
    if is_fatal_account_error(message) {
        return AccountFailureAction::FatalAccountError;
    }
    if is_transient_upstream_error(message) {
        return AccountFailureAction::TransientRecordOnly;
    }
    AccountFailureAction::ReleaseOnly
}

fn is_fatal_account_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    if text.contains("your windsurf version is out of date")
        || text.contains("invalid_argument")
        || text.contains("internal error")
        || text.contains("an internal error occurred")
        || text.contains("请求构建失败")
        || text.contains("protobuf")
        || text.contains("trailer 错误")
    {
        return false;
    }
    let auth_failure = text.contains("unauthenticated")
        || text.contains("invalid api key")
        || text.contains("invalid_api_key")
        || text.contains("invalid token")
        || text.contains("jwt expired")
        || text.contains("authorization failed");
    let account_failure = text.contains("account disabled")
        || text.contains("account banned")
        || text.contains("account suspended")
        || text.contains("subscription expired")
        || text.contains("plan expired")
        || text.contains("credential")
        || text.contains("凭据");
    let permission_account_failure = text.contains("permission_denied")
        && (text.contains("subscription")
            || text.contains("plan")
            || text.contains("account")
            || text.contains("credential")
            || text.contains("api key")
            || text.contains("unauthorized"));
    auth_failure || account_failure || permission_account_failure
}

fn is_upstream_rate_limit_error(message: &str) -> bool {
    let text = message.to_ascii_lowercase();
    text.contains("rate limit")
        || text.contains("rate_limit")
        || text.contains("global rate limit")
        || text.contains("resource_exhausted")
        || text.contains("third-party model provider is experiencing issues")
        || text.contains("model provider is experiencing issues")
        || is_quota_exhausted_error(message)
}

fn parse_retry_after_secs(text: &str) -> Option<i64> {
    let text = text.to_ascii_lowercase();
    let markers = ["resets in:", "reset in:", "retry after:", "retry-after:"];
    let source = if let Some(rest) = markers
        .iter()
        .find_map(|marker| text.split_once(marker).map(|(_, rest)| rest.trim()))
    {
        let token: String = rest
            .chars()
            .take_while(|ch| ch.is_ascii_alphanumeric())
            .collect();
        if token.is_empty() {
            return None;
        }
        token
    } else {
        text.to_string()
    };
    let mut total = 0_i64;
    let mut number = String::new();
    let mut saw_unit = false;
    for ch in source.chars().take(80) {
        if ch.is_ascii_digit() {
            number.push(ch);
            continue;
        }
        if number.is_empty() {
            if saw_unit && matches!(ch, '.' | ',' | ';' | '}' | ']') {
                break;
            }
            continue;
        }
        let value = number.parse::<i64>().ok()?;
        number.clear();
        match ch {
            'd' => {
                total += value * 24 * 60 * 60;
                saw_unit = true;
            }
            'h' => {
                total += value * 60 * 60;
                saw_unit = true;
            }
            'm' => {
                total += value * 60;
                saw_unit = true;
            }
            's' => {
                total += value;
                saw_unit = true;
            }
            _ if !saw_unit => return Some(value.max(1)),
            _ => {}
        }
    }
    if saw_unit {
        Some(total.max(1))
    } else if !number.is_empty() {
        number.parse::<i64>().ok().map(|value| value.max(1))
    } else {
        None
    }
}

fn anthropic_error_type_for_upstream(message: &str) -> &'static str {
    if is_upstream_rate_limit_error(message) {
        "rate_limit_error"
    } else {
        "api_error"
    }
}

fn upstream_retry_after_secs(message: &str) -> i64 {
    parse_retry_after_secs(message).unwrap_or(60)
}

fn rate_limit_probe_after(retry_after_secs: i64) -> String {
    let delay = retry_after_secs.min(RATE_LIMIT_PROBE_INTERVAL_SECS).max(1);
    (Utc::now() + Duration::seconds(delay)).to_rfc3339()
}

fn next_rate_limit_probe_after() -> String {
    (Utc::now() + Duration::seconds(RATE_LIMIT_PROBE_INTERVAL_SECS)).to_rfc3339()
}

fn rate_limited_response(message: &str, retry_after_secs: i64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(
            header::RETRY_AFTER,
            HeaderValue::from_str(&retry_after_secs.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("60")),
        )],
        Json(ApiError {
            error: ErrorBody {
                kind: "rate_limit_error".to_string(),
                message: message.to_string(),
            },
        }),
    )
        .into_response()
}

fn upstream_messages_error_response(message: &str) -> Response {
    if is_upstream_rate_limit_error(message) {
        rate_limited_response(message, upstream_retry_after_secs(message))
    } else {
        error(StatusCode::BAD_GATEWAY, "upstream_error", message)
    }
}

fn preflight_failure_message(failure: &EnginePreflightFailure) -> String {
    if let Some(retry_after_secs) = failure.retry_after_secs {
        format!(
            "Windsurf preflight {} failed: {}. retry after: {}s",
            failure.phase, failure.message, retry_after_secs
        )
    } else {
        format!(
            "Windsurf preflight {} failed: {}",
            failure.phase, failure.message
        )
    }
}

fn acquire_error_message(error: &AcquireError) -> String {
    match error {
        AcquireError::TemporarilyUnavailable {
            reason,
            upstream_error,
            ..
        } => upstream_error.clone().unwrap_or_else(|| reason.clone()),
        AcquireError::NoAccount => "没有可用账号".to_string(),
        AcquireError::Db(err) => err.to_string(),
    }
}

fn acquire_error_response(acquire_error: AcquireError) -> Response {
    match acquire_error {
        AcquireError::TemporarilyUnavailable {
            retry_after_secs,
            reason,
            upstream_error,
        } => rate_limited_response(
            upstream_error.as_deref().unwrap_or(&reason),
            retry_after_secs,
        ),
        AcquireError::NoAccount => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "pool_exhausted",
            "没有可用账号",
        ),
        AcquireError::Db(err) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "scheduler_error",
            &err.to_string(),
        ),
    }
}

impl AccountScheduler {
    fn new(
        db: SqlitePool,
        capacity: CapacitySettings,
        events: broadcast::Sender<AdminEvent>,
    ) -> Self {
        Self {
            db,
            capacity,
            events,
        }
    }

    async fn acquire(
        &self,
        model: &str,
        resolved_model: Option<&str>,
        upstream_model: Option<&str>,
        caller_key: Option<String>,
    ) -> Result<AccountLease, AcquireError> {
        self.cleanup_expired().await.map_err(AcquireError::Db)?;
        let global_used = self.global_inflight().await.map_err(AcquireError::Db)?;
        if self.capacity.global_concurrency > 0 && global_used >= self.capacity.global_concurrency {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: self.capacity.queue_timeout_secs.clamp(1, 300),
                reason: "全局执行槽已满".to_string(),
                upstream_error: None,
            });
        }
        let model_used = self.model_inflight(model).await.map_err(AcquireError::Db)?;
        if self.capacity.model_concurrency > 0 && model_used >= self.capacity.model_concurrency {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: 30,
                reason: "当前模型执行槽已满".to_string(),
                upstream_error: None,
            });
        }
        if let Some(caller) = caller_key.as_deref() {
            if let Some(account_id) = self
                .sticky_account(caller, model)
                .await
                .map_err(AcquireError::Db)?
            {
                match self
                    .try_reserve_account(
                        account_id,
                        model,
                        resolved_model,
                        upstream_model,
                        caller_key.clone(),
                        true,
                    )
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
        let mut retry_after_secs = i64::MAX;
        let mut upstream_error: Option<String> = None;
        for row in rows {
            let account = scheduler_account_from_row(&row);
            let availability = self
                .availability(&account, model, resolved_model, upstream_model)
                .await
                .map_err(AcquireError::Db)?;
            if availability.available {
                candidates.push((account, availability.rpm_used));
            } else if availability.retry_after_secs > 0 {
                retry_after_secs = retry_after_secs.min(availability.retry_after_secs);
                if upstream_error.is_none() {
                    upstream_error = availability.upstream_error;
                }
            }
        }

        if candidates.is_empty() {
            return Err(AcquireError::TemporarilyUnavailable {
                retry_after_secs: if retry_after_secs == i64::MAX {
                    60
                } else {
                    retry_after_secs.max(1)
                },
                reason: "所有账号都不可用或已达到限制".to_string(),
                upstream_error,
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
                .reserve_loaded_account(account, model, caller_key.clone(), false)
                .await
                .map_err(AcquireError::Db)?
            {
                return Ok(lease);
            }
        }

        Err(AcquireError::TemporarilyUnavailable {
            retry_after_secs: 5,
            reason: "账号并发槽位已满".to_string(),
            upstream_error: None,
        })
    }

    async fn acquire_preflighted(
        &self,
        engine: &RemoteApiEngine,
        trace_id: &str,
        model: &str,
        engine_model: &EngineModel,
        caller_key: Option<String>,
    ) -> Result<(AccountLease, EngineAccount), AcquireError> {
        let _ = (engine, trace_id);
        let upstream_model = engine_model
            .model_uid
            .as_deref()
            .unwrap_or(&engine_model.id);
        let lease = self
            .acquire(
                model,
                Some(&engine_model.id),
                Some(upstream_model),
                caller_key,
            )
            .await?;
        let engine_account = EngineAccount {
            api_key: lease.api_key.clone(),
            jwt_token: lease.jwt_token.clone(),
            proxy_url: proxy_url_for_account(&self.db, lease.account_id).await,
        };
        Ok((lease, engine_account))
    }

    async fn mark_success(&self, lease: &mut AccountLease) -> anyhow::Result<()> {
        if let Some(caller) = lease.caller_key.as_deref() {
            self.set_sticky(caller, &lease.model, lease.account_id, &lease.api_key)
                .await?;
        }
        let now_text = now();
        sqlx::query("UPDATE accounts SET status='ready', error_count=0, last_error=NULL, last_used_at=?, rate_limited_until=NULL, rate_limit_probe_after=NULL, updated_at=? WHERE id=?")
            .bind(&now_text)
            .bind(&now_text)
            .bind(lease.account_id)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_rate_limits WHERE account_id=? AND model=?")
            .bind(lease.account_id)
            .bind(&lease.model)
            .execute(&self.db)
            .await?;
        emit_admin_event(
            &self.events,
            "account_request_succeeded",
            json!({
                "accountId": lease.account_id,
                "email": lease.email,
                "model": lease.model
            }),
        );
        self.release(lease).await
    }

    #[allow(dead_code)]
    async fn mark_error(&self, lease: &mut AccountLease, message: &str) -> anyhow::Result<()> {
        mark_account_error_with_events(&self.db, &self.events, lease.account_id, message).await?;
        self.release(lease).await
    }

    async fn mark_failure(&self, lease: &mut AccountLease, message: &str) -> anyhow::Result<()> {
        match classify_account_failure(message, &self.capacity) {
            AccountFailureAction::RateLimit(limit) => {
                let model = match limit.scope {
                    UpstreamRateLimitScope::Model => Some(lease.model.clone()),
                    UpstreamRateLimitScope::Account => None,
                };
                self.mark_rate_limited(lease, model.as_deref(), limit.retry_after_secs, message)
                    .await
            }
            AccountFailureAction::FatalAccountError => self.mark_error(lease, message).await,
            AccountFailureAction::TransientRecordOnly => {
                mark_account_transient_error_with_events(
                    &self.db,
                    &self.events,
                    lease.account_id,
                    message,
                )
                .await?;
                self.release(lease).await
            }
            AccountFailureAction::ReleaseOnly => {
                emit_admin_event(
                    &self.events,
                    "account_request_released",
                    json!({
                        "accountId": lease.account_id,
                        "email": lease.email,
                        "model": lease.model,
                        "message": message
                    }),
                );
                self.release(lease).await
            }
        }
    }

    async fn mark_rate_limited(
        &self,
        lease: &mut AccountLease,
        model: Option<&str>,
        retry_after_secs: i64,
        reason: &str,
    ) -> anyhow::Result<()> {
        let limited_until = (Utc::now() + Duration::seconds(retry_after_secs.max(1))).to_rfc3339();
        let probe_after = rate_limit_probe_after(retry_after_secs.max(1));
        let now_text = now();
        if let Some(model) = model {
            sqlx::query(
                "INSERT INTO account_model_rate_limits (account_id, model, limited_until, reason, probe_after, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT(account_id, model) DO UPDATE SET limited_until=excluded.limited_until, reason=excluded.reason, probe_after=excluded.probe_after, updated_at=excluded.updated_at",
            )
            .bind(lease.account_id)
            .bind(model)
            .bind(&limited_until)
            .bind(reason)
            .bind(&probe_after)
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        } else {
            sqlx::query(
                "UPDATE accounts SET rate_limited_until=?, rate_limit_probe_after=?, last_error=?, updated_at=? WHERE id=?",
            )
            .bind(&limited_until)
            .bind(&probe_after)
            .bind(reason)
            .bind(&now_text)
            .bind(lease.account_id)
            .execute(&self.db)
            .await?;
        }
        if let Some(caller) = lease.caller_key.as_deref() {
            self.clear_sticky(caller, &lease.model).await?;
        }
        emit_admin_event(
            &self.events,
            "account_rate_limited",
            json!({
                "accountId": lease.account_id,
                "email": lease.email,
                "model": model,
                "requestedModel": lease.model,
                "limitedUntil": limited_until,
                "probeAfter": probe_after,
                "message": reason
            }),
        );
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
        emit_admin_event(
            &self.events,
            "account_request_finished",
            json!({
                "accountId": lease.account_id,
                "email": lease.email,
                "model": lease.model
            }),
        );
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
        sqlx::query("UPDATE accounts SET rate_limited_until=NULL, rate_limit_probe_after=NULL, updated_at=? WHERE id=?")
            .bind(now())
            .bind(account_id)
            .execute(&self.db)
            .await?;
        sqlx::query("DELETE FROM account_model_rate_limits WHERE account_id=?")
            .bind(account_id)
            .execute(&self.db)
            .await?;
        emit_account_event(&self.events, "clear_rate_limit", account_id);
        Ok(())
    }

    async fn clear_sticky_for_account(&self, account_id: i64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sticky_sessions WHERE account_id=?")
            .bind(account_id)
            .execute(&self.db)
            .await?;
        emit_account_event(&self.events, "clear_sticky", account_id);
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
        resolved_model: Option<&str>,
        upstream_model: Option<&str>,
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
        self.try_reserve_loaded_account(
            scheduler_account_from_row(&row),
            model,
            resolved_model,
            upstream_model,
            caller_key,
            sticky,
        )
        .await
    }

    async fn try_reserve_loaded_account(
        &self,
        account: SchedulerAccount,
        model: &str,
        resolved_model: Option<&str>,
        upstream_model: Option<&str>,
        caller_key: Option<String>,
        sticky: bool,
    ) -> anyhow::Result<Option<AccountLease>> {
        if !self
            .availability(&account, model, resolved_model, upstream_model)
            .await?
            .available
        {
            return Ok(None);
        }
        self.reserve_loaded_account(account, model, caller_key, sticky)
            .await
    }

    async fn reserve_loaded_account(
        &self,
        account: SchedulerAccount,
        model: &str,
        caller_key: Option<String>,
        sticky: bool,
    ) -> anyhow::Result<Option<AccountLease>> {
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
        let credentials = account_credentials_from_raw(account.credentials_json.as_deref())
            .ok_or_else(|| anyhow::anyhow!("账号没有可用凭据"))?;
        Ok(Some(AccountLease {
            account_id: account.id,
            email: account.email,
            api_key: credentials.api_key,
            jwt_token: credentials.jwt_token,
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
        resolved_model: Option<&str>,
        upstream_model: Option<&str>,
    ) -> anyhow::Result<AccountAvailability> {
        if !["ready", "active", "ok"].contains(&account.status.as_str()) {
            return Ok(AccountAvailability::unavailable(
                availability_kind_for_status(&account.status),
                60,
            ));
        }
        self.availability_without_status(account, model, resolved_model, upstream_model)
            .await
    }

    async fn availability_without_status(
        &self,
        account: &SchedulerAccount,
        model: &str,
        resolved_model: Option<&str>,
        upstream_model: Option<&str>,
    ) -> anyhow::Result<AccountAvailability> {
        let now_utc = Utc::now();
        let account_limit = if account.max_concurrent > 0 {
            account
                .max_concurrent
                .min(self.capacity.account_concurrency.max(1))
        } else {
            self.capacity.account_concurrency.max(1)
        };
        if account.current_concurrent >= account_limit {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::ConcurrencyFull,
                5,
            ));
        }
        if date_in_future(account.rate_limited_until.as_deref(), now_utc) {
            let retry_after_secs = retry_after(account.rate_limited_until.as_deref(), 60);
            if self
                .claim_account_rate_limit_probe(
                    account.id,
                    account.rate_limit_probe_after.as_deref(),
                )
                .await?
            {
                return self.available_for_probe(account.id).await;
            }
            return Ok(AccountAvailability::unavailable_with_upstream(
                AvailabilityKind::AccountRateLimited,
                retry_after_secs,
                self.account_last_error(account.id).await?,
            ));
        }
        if let Some(limit) = self.model_rate_limit(account.id, model).await? {
            if self
                .claim_model_rate_limit_probe(account.id, model, limit.probe_after.as_deref())
                .await?
            {
                return self.available_for_probe(account.id).await;
            }
            return Ok(AccountAvailability::unavailable_with_upstream(
                AvailabilityKind::ModelRateLimited,
                retry_after(Some(&limit.limited_until), 60),
                limit.reason,
            ));
        }
        let effective_tier = effective_account_tier(account);
        if account.rpm_limit <= 0 || effective_tier == "expired" {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::TierExpired,
                60,
            ));
        }
        if !account_supports_model(account, model, resolved_model, upstream_model) {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::ModelBlocked,
                60,
            ));
        }
        if model_blocked(account.blocked_models_json.as_deref(), model) {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::ModelBlocked,
                60,
            ));
        }
        let used = self.rpm_used(account.id).await?;
        if used >= account.rpm_limit {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::RpmFull,
                60,
            ));
        }
        if account_api_key_from_raw(account.credentials_json.as_deref()).is_none() {
            return Ok(AccountAvailability::unavailable(
                AvailabilityKind::CredentialMissing,
                60,
            ));
        }
        Ok(AccountAvailability::available(used))
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

    async fn model_rate_limit(
        &self,
        account_id: i64,
        model: &str,
    ) -> anyhow::Result<Option<ModelRateLimit>> {
        let now_text = now();
        sqlx::query("DELETE FROM account_model_rate_limits WHERE limited_until <= ?")
            .bind(&now_text)
            .execute(&self.db)
            .await?;
        let row = sqlx::query(
            "SELECT limited_until, reason, probe_after FROM account_model_rate_limits WHERE account_id=? AND model=?",
        )
        .bind(account_id)
        .bind(model)
        .fetch_optional(&self.db)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let limited_until = row.get::<String, _>("limited_until");
        if !date_in_future(Some(&limited_until), Utc::now()) {
            return Ok(None);
        }
        Ok(Some(ModelRateLimit {
            limited_until,
            reason: row.get::<Option<String>, _>("reason"),
            probe_after: row.get::<Option<String>, _>("probe_after"),
        }))
    }

    async fn account_last_error(&self, account_id: i64) -> anyhow::Result<Option<String>> {
        let row = sqlx::query("SELECT last_error FROM accounts WHERE id=?")
            .bind(account_id)
            .fetch_optional(&self.db)
            .await?;
        Ok(row.and_then(|row| row.get::<Option<String>, _>("last_error")))
    }

    async fn available_for_probe(&self, account_id: i64) -> anyhow::Result<AccountAvailability> {
        let used = self.rpm_used(account_id).await?;
        Ok(AccountAvailability::probing(used))
    }

    async fn claim_account_rate_limit_probe(
        &self,
        account_id: i64,
        probe_after: Option<&str>,
    ) -> anyhow::Result<bool> {
        if date_in_future(probe_after, Utc::now()) {
            return Ok(false);
        }
        let next_probe = next_rate_limit_probe_after();
        let updated = sqlx::query(
            "UPDATE accounts SET rate_limit_probe_after=?, updated_at=? WHERE id=? AND (rate_limit_probe_after IS NULL OR rate_limit_probe_after <= ?)",
        )
        .bind(&next_probe)
        .bind(now())
        .bind(account_id)
        .bind(now())
        .execute(&self.db)
        .await?;
        Ok(updated.rows_affected() > 0)
    }

    async fn claim_model_rate_limit_probe(
        &self,
        account_id: i64,
        model: &str,
        probe_after: Option<&str>,
    ) -> anyhow::Result<bool> {
        if date_in_future(probe_after, Utc::now()) {
            return Ok(false);
        }
        let next_probe = next_rate_limit_probe_after();
        let updated = sqlx::query(
            "UPDATE account_model_rate_limits SET probe_after=?, updated_at=? WHERE account_id=? AND model=? AND (probe_after IS NULL OR probe_after <= ?)",
        )
        .bind(&next_probe)
        .bind(now())
        .bind(account_id)
        .bind(model)
        .bind(now())
        .execute(&self.db)
        .await?;
        Ok(updated.rows_affected() > 0)
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
struct ModelRateLimit {
    limited_until: String,
    reason: Option<String>,
    probe_after: Option<String>,
}

#[derive(Debug)]
struct AccountAvailability {
    available: bool,
    kind: AvailabilityKind,
    retry_after_secs: i64,
    rpm_used: i64,
    upstream_error: Option<String>,
}

impl AccountAvailability {
    fn available(rpm_used: i64) -> Self {
        Self {
            available: true,
            kind: AvailabilityKind::Available,
            retry_after_secs: 0,
            rpm_used,
            upstream_error: None,
        }
    }

    fn probing(rpm_used: i64) -> Self {
        Self {
            available: true,
            kind: AvailabilityKind::Probing,
            retry_after_secs: 0,
            rpm_used,
            upstream_error: None,
        }
    }

    fn unavailable(kind: AvailabilityKind, retry_after_secs: i64) -> Self {
        Self {
            available: false,
            kind,
            retry_after_secs,
            rpm_used: 0,
            upstream_error: None,
        }
    }

    fn unavailable_with_upstream(
        kind: AvailabilityKind,
        retry_after_secs: i64,
        upstream_error: Option<String>,
    ) -> Self {
        Self {
            available: false,
            kind,
            retry_after_secs,
            rpm_used: 0,
            upstream_error,
        }
    }
}

fn availability_kind_for_status(status: &str) -> AvailabilityKind {
    match status {
        "error" => AvailabilityKind::StatusError,
        "disabled" => AvailabilityKind::StatusDisabled,
        "banned" => AvailabilityKind::StatusBanned,
        _ => AvailabilityKind::StatusUnavailable,
    }
}

fn scheduler_account_from_row(row: &sqlx::sqlite::SqliteRow) -> SchedulerAccount {
    SchedulerAccount {
        id: row.get::<i64, _>("id"),
        email: row.get::<String, _>("email"),
        status: row.get::<String, _>("status"),
        tier: row.get::<String, _>("tier"),
        tier_manual: row.get::<i64, _>("tier_manual") != 0,
        max_concurrent: row.get::<i64, _>("max_concurrent"),
        current_concurrent: row.get::<i64, _>("current_concurrent"),
        last_used_at: row.get::<Option<String>, _>("last_used_at"),
        rate_limited_until: row.get::<Option<String>, _>("rate_limited_until"),
        rate_limit_probe_after: row.get::<Option<String>, _>("rate_limit_probe_after"),
        rpm_limit: row.get::<i64, _>("rpm_limit"),
        credits_json: row.get::<Option<String>, _>("credits_json"),
        user_status_json: row.get::<Option<String>, _>("user_status_json"),
        available_models_json: row.get::<Option<String>, _>("available_models_json"),
        tier_models_json: row.get::<Option<String>, _>("tier_models_json"),
        blocked_models_json: row.get::<Option<String>, _>("blocked_models_json"),
        credentials_json: row.get::<Option<String>, _>("credentials_json"),
    }
}

fn account_api_key_from_raw(raw: Option<&str>) -> Option<String> {
    account_credentials_from_raw(raw).map(|credentials| credentials.api_key)
}

fn account_credentials_from_raw(raw: Option<&str>) -> Option<AccountCredentials> {
    let value = serde_json::from_str::<Value>(raw?).ok()?;
    let api_key = value
        .get("apiKey")
        .or_else(|| value.pointer("/extra/apiKey"))
        .or_else(|| value.get("sessionToken"))
        .and_then(Value::as_str)
        .map(str::to_string)?;
    let jwt_token = value
        .get("jwt")
        .or_else(|| value.get("jwtToken"))
        .or_else(|| value.pointer("/extra/jwt"))
        .or_else(|| value.pointer("/extra/jwtToken"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty());
    Some(AccountCredentials { api_key, jwt_token })
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

fn effective_account_tier(account: &SchedulerAccount) -> String {
    if account.tier_manual {
        return account.tier.clone();
    }
    infer_tier_from_raw(account.credits_json.as_deref())
        .or_else(|| infer_tier_from_raw(account.user_status_json.as_deref()))
        .unwrap_or_else(|| account.tier.clone())
}

fn effective_row_tier(row: &sqlx::sqlite::SqliteRow) -> String {
    if row.get::<i64, _>("tier_manual") != 0 {
        return row.get::<String, _>("tier");
    }
    let credits_json = row.get::<Option<String>, _>("credits_json");
    let user_status_json = row.get::<Option<String>, _>("user_status_json");
    infer_tier_from_raw(credits_json.as_deref())
        .or_else(|| infer_tier_from_raw(user_status_json.as_deref()))
        .unwrap_or_else(|| row.get::<String, _>("tier"))
}

fn infer_tier_from_raw(raw: Option<&str>) -> Option<String> {
    let value = serde_json::from_str::<Value>(raw?).ok()?;
    let tier = infer_tier(&value);
    (tier != "unknown").then_some(tier)
}

fn account_supports_model(
    account: &SchedulerAccount,
    requested_model: &str,
    resolved_model: Option<&str>,
    upstream_model: Option<&str>,
) -> bool {
    let Some(models) = account_model_snapshot(account) else {
        return true;
    };
    if models.is_empty() {
        return true;
    }
    let aliases = model_match_aliases(requested_model, resolved_model, upstream_model);
    models
        .iter()
        .any(|model| model_entry_matches_aliases(model, &aliases))
}

fn account_model_snapshot(account: &SchedulerAccount) -> Option<Vec<Value>> {
    parse_model_list(account.available_models_json.as_deref())
        .filter(|models| !models.is_empty())
        .or_else(|| parse_model_list(account.tier_models_json.as_deref()))
}

fn parse_model_list(raw: Option<&str>) -> Option<Vec<Value>> {
    let value = serde_json::from_str::<Value>(raw?).ok()?;
    let items = value.as_array()?;
    let models = items
        .iter()
        .filter(|item| model_entry_names(item).iter().any(|name| !name.is_empty()))
        .cloned()
        .collect::<Vec<_>>();
    Some(models)
}

fn model_match_aliases(
    requested_model: &str,
    resolved_model: Option<&str>,
    upstream_model: Option<&str>,
) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    for value in [
        requested_model,
        resolved_model.unwrap_or(""),
        upstream_model.unwrap_or(""),
    ] {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        aliases.insert(trimmed.to_ascii_lowercase());
        aliases.insert(model_alias(trimmed).to_ascii_lowercase());
        aliases.insert(trimmed.replace('.', "-").to_ascii_lowercase());
        aliases.insert(trimmed.replace('-', ".").to_ascii_lowercase());
    }
    aliases
}

fn model_entry_matches_aliases(model: &Value, aliases: &std::collections::HashSet<String>) -> bool {
    model_entry_names(model)
        .into_iter()
        .any(|name| aliases.contains(&name.to_ascii_lowercase()))
}

fn model_entry_names(model: &Value) -> Vec<String> {
    if let Some(value) = model.as_str() {
        return vec![value.to_string()];
    }
    [
        "id",
        "shortName",
        "short_name",
        "model",
        "modelName",
        "modelUid",
        "model_uid",
        "name",
        "label",
    ]
    .iter()
    .filter_map(|key| model.get(*key).and_then(Value::as_str))
    .filter(|value| !value.trim().is_empty())
    .map(str::to_string)
    .collect()
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
    if let Some(value) = headers
        .get("x-claude-code-session-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    {
        return Some(format!("claude:{value}"));
    }
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

fn branch_gate_key(
    caller_key: Option<&str>,
    model: &str,
    messages: &[EngineMessage],
) -> Option<String> {
    let caller_key = caller_key?;
    Some(format!(
        "{}:{}:{}",
        caller_key,
        model_alias(model),
        branch_message_fingerprint(messages)
    ))
}

fn branch_message_fingerprint(messages: &[EngineMessage]) -> String {
    let original_user = messages
        .iter()
        .find(|message| message.role == "user" && !is_tool_result_message(message))
        .map(|message| message.content.as_str())
        .unwrap_or("");
    short_hash(original_user)
}

fn is_tool_result_message(message: &EngineMessage) -> bool {
    message.role == "tool" || message.content.trim_start().starts_with("<tool_result")
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
    let rows = sqlx::query("SELECT account_id, model, limited_until, reason, probe_after FROM account_model_rate_limits WHERE limited_until > ?")
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
                "reason": row.get::<Option<String>, _>("reason"),
                "probeAfter": row.get::<Option<String>, _>("probe_after")
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
    if plan_name.contains("trial") {
        "trial".to_string()
    } else if plan_name.contains("pro") || plan_name.contains("team") {
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

async fn require_client_api_key(
    db: &SqlitePool,
    headers: &HeaderMap,
    query: Option<&HashMap<String, String>>,
) -> Result<(), Response> {
    if !client_api_keys_required(db).await.map_err(|err| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "api_key_config_error",
            &err.to_string(),
        )
    })? {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "client_api_key_required",
            "请先在管理台创建并启用调用密钥",
        ));
    }
    let candidates = client_api_key_candidates(headers, query);
    if candidates.is_empty() {
        return Err(error(
            StatusCode::UNAUTHORIZED,
            "no_credentials",
            "缺少调用密钥",
        ));
    }
    if let Some(key_id) = find_active_client_api_key(db, &candidates)
        .await
        .map_err(|err| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_key_check_error",
                &err.to_string(),
            )
        })?
    {
        let _ = touch_client_api_key(db, key_id).await;
        return Ok(());
    }
    Err(error(
        StatusCode::UNAUTHORIZED,
        "invalid_credential",
        "调用密钥不正确",
    ))
}

async fn client_api_keys_required(db: &SqlitePool) -> anyhow::Result<bool> {
    let row = sqlx::query("SELECT id FROM client_api_keys WHERE enabled=1 LIMIT 1")
        .fetch_optional(db)
        .await?;
    Ok(row.is_some())
}

async fn find_active_client_api_key(
    db: &SqlitePool,
    candidates: &[String],
) -> anyhow::Result<Option<i64>> {
    for candidate in candidates {
        let row =
            sqlx::query("SELECT id FROM client_api_keys WHERE enabled=1 AND key_hash=? LIMIT 1")
                .bind(sha256_hex(candidate))
                .fetch_optional(db)
                .await?;
        if let Some(row) = row {
            return Ok(Some(row.get::<i64, _>("id")));
        }
    }
    Ok(None)
}

async fn touch_client_api_key(db: &SqlitePool, id: i64) -> anyhow::Result<()> {
    sqlx::query("UPDATE client_api_keys SET last_used_at=? WHERE id=?")
        .bind(now())
        .bind(id)
        .execute(db)
        .await?;
    Ok(())
}

async fn client_api_keys_from_settings(db: &SqlitePool) -> anyhow::Result<Vec<String>> {
    let Some(row) = sqlx::query("SELECT value FROM settings WHERE key='client_api_keys'")
        .fetch_optional(db)
        .await?
    else {
        return Ok(Vec::new());
    };
    let raw = row.get::<String, _>("value");
    if let Ok(value) = serde_json::from_str::<Value>(&raw) {
        return Ok(normalize_api_keys_setting(&value));
    }
    Ok(normalize_api_keys_text(&raw))
}

fn client_api_key_candidates(
    headers: &HeaderMap,
    query: Option<&HashMap<String, String>>,
) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(token) = bearer_token(headers) {
        candidates.push(token);
    }
    for name in ["x-api-key", "x-goog-api-key"] {
        if let Some(value) = headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            candidates.push(value.to_string());
        }
    }
    if let Some(query) = query {
        for name in ["key", "auth_token"] {
            if let Some(value) = query
                .get(name)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
            {
                candidates.push(value.to_string());
            }
        }
    }
    candidates
}

fn normalize_api_keys_setting(value: &Value) -> Vec<String> {
    if let Some(items) = value.as_array() {
        let joined = items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        return normalize_api_keys_text(&joined);
    }
    value
        .as_str()
        .map(normalize_api_keys_text)
        .unwrap_or_default()
}

fn normalize_api_keys_text(value: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for item in value.split(|ch: char| ch == '\n' || ch == '\r' || ch == ',' || ch == ';') {
        let key = item.trim();
        if !key.is_empty() && !keys.iter().any(|existing| existing == key) {
            keys.push(key.to_string());
        }
    }
    keys
}

fn normalize_client_api_key_name(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("调用密钥")
        .chars()
        .take(80)
        .collect()
}

fn client_api_key_json(row: sqlx::sqlite::SqliteRow) -> Value {
    json!({
        "id": row.get::<i64, _>("id"),
        "name": row.get::<String, _>("name"),
        "key": row.get::<Option<String>, _>("key_value"),
        "keyMask": row.get::<String, _>("key_mask"),
        "enabled": row.get::<i64, _>("enabled") != 0,
        "createdAt": row.get::<String, _>("created_at"),
        "updatedAt": row.get::<String, _>("updated_at"),
        "lastUsedAt": row.get::<Option<String>, _>("last_used_at")
    })
}

fn client_api_key_db_error_message(value: &str) -> String {
    if value.to_ascii_lowercase().contains("unique") {
        "这个密钥已经存在".to_string()
    } else {
        value.to_string()
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)?;
    let (scheme, token) = value.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
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

fn short_hash(value: &str) -> String {
    sha256_hex(value).chars().take(12).collect()
}

fn redact_log_text(value: &str) -> String {
    let mut out = Vec::new();
    for part in value.split_whitespace().take(80) {
        let lower = part.to_ascii_lowercase();
        let redacted = if is_sensitive_log_part(part, &lower) {
            "[redacted]"
        } else {
            part
        };
        out.push(redacted);
    }
    let mut text = out.join(" ");
    if text.chars().count() > 300 {
        text = text.chars().take(300).collect();
    }
    text
}

fn is_sensitive_log_part(part: &str, lower: &str) -> bool {
    lower.contains("authorization")
        || lower.contains("cookie")
        || lower.contains("api_key")
        || lower.contains("apikey")
        || lower.contains("access_token")
        || lower.contains("refresh_token")
        || lower.contains("jwt")
        || lower.contains("token")
        || looks_like_jwt(part)
}

fn looks_like_jwt(value: &str) -> bool {
    let trimmed = value.trim_matches(|ch: char| {
        !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
    });
    let mut parts = trimmed.split('.');
    let Some(header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    let Some(signature) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && header.starts_with("eyJ")
        && !payload.is_empty()
        && !signature.is_empty()
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
    availability: Option<&AccountAvailability>,
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
    let credits = parse_json("credits_json", Value::Null);
    let user_status_summary = account_user_status_summary(&credits);
    let effective_tier = effective_row_tier(&row);
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
        "tier": effective_tier,
        "storedTier": row.get::<String, _>("tier"),
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
        "rateLimitProbeAfter": row.get::<Option<String>, _>("rate_limit_probe_after"),
        "rateLimited": row.get::<Option<String>, _>("rate_limited_until").is_some_and(|value| {
            chrono::DateTime::parse_from_rfc3339(&value)
                .map(|time| time.with_timezone(&Utc) > Utc::now())
                .unwrap_or(false)
        }),
        "rpmUsed": row.get::<i64, _>("rpm_used"),
        "rpmLimit": row.get::<i64, _>("rpm_limit"),
        "credits": credits,
        "userStatus": user_status_summary,
        "availableModels": available_models,
        "tierModels": tier_models,
        "blockedModels": parse_json("blocked_models_json", json!([])),
        "modelRateLimits": model_limits.get(&id).cloned().unwrap_or_else(|| json!({})),
        "availability": availability.map(account_availability_json).unwrap_or_else(|| json!({
            "available": false,
            "kind": AvailabilityKind::StatusUnavailable,
            "retryAfterSecs": 60,
            "upstreamError": null
        })),
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

fn account_user_status_summary(credits: &Value) -> Value {
    if credits.is_null() {
        return Value::Null;
    }
    json!({
        "planName": credits.get("planName").cloned().unwrap_or(Value::Null),
        "trialEndMs": credits.get("trialEndMs").cloned().unwrap_or(Value::Null)
    })
}

fn account_availability_json(availability: &AccountAvailability) -> Value {
    json!({
        "available": availability.available,
        "kind": availability.kind,
        "retryAfterSecs": availability.retry_after_secs,
        "upstreamError": availability.upstream_error
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
    data_dir: &PathBuf,
    trace_id: &str,
    layer: &str,
    payload: &Value,
) -> anyhow::Result<()> {
    let payload_text = payload.to_string();
    let payload_size = i64::try_from(payload_text.len()).unwrap_or(i64::MAX);
    let payload_preview = trace_payload_preview(&payload_text);
    let payload_path = match write_trace_payload(data_dir, trace_id, &payload_text).await {
        Ok(path) => Some(path),
        Err(err) => {
            tracing::warn!(
                trace_id,
                error = %redact_log_text(&err.to_string()),
                "trace payload file write failed"
            );
            None
        }
    };
    sqlx::query("INSERT INTO request_trace_chunks (trace_id, layer, payload, payload_path, payload_size, created_at) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(trace_id)
        .bind(layer)
        .bind(payload_preview)
        .bind(payload_path)
        .bind(payload_size)
        .bind(now())
        .execute(db)
        .await?;
    Ok(())
}

async fn write_trace_payload(
    data_dir: &PathBuf,
    trace_id: &str,
    payload: &str,
) -> anyhow::Result<String> {
    let relative_dir = PathBuf::from("traces").join(trace_id);
    let dir = data_dir.join(&relative_dir);
    tokio::fs::create_dir_all(&dir).await.with_context(|| {
        format!("failed to create trace payload directory {}", dir.display())
    })?;
    let file_name = format!("{}.json", Uuid::new_v4().simple());
    let relative_path = relative_dir.join(file_name);
    let path = data_dir.join(&relative_path);
    tokio::fs::write(&path, payload)
        .await
        .with_context(|| format!("failed to write trace payload {}", path.display()))?;
    Ok(relative_path.to_string_lossy().to_string())
}

fn read_trace_payload(data_dir: &PathBuf, relative_path: &str) -> anyhow::Result<String> {
    let path = safe_data_relative_path(data_dir, relative_path)?;
    std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read trace payload {}", path.display()))
}

fn safe_data_relative_path(data_dir: &PathBuf, relative_path: &str) -> anyhow::Result<PathBuf> {
    let relative = PathBuf::from(relative_path);
    if relative.is_absolute() {
        anyhow::bail!("trace payload path must be relative");
    }
    for component in relative.components() {
        match component {
            Component::Normal(_) => {}
            _ => anyhow::bail!("trace payload path contains invalid component"),
        }
    }
    Ok(data_dir.join(relative))
}

fn trace_payload_preview(payload: &str) -> String {
    let mut preview = String::new();
    let mut chars = payload.chars();
    for _ in 0..TRACE_PAYLOAD_PREVIEW_CHARS {
        let Some(ch) = chars.next() else {
            return preview;
        };
        preview.push(ch);
    }
    if chars.next().is_some() {
        preview.push_str("\n...");
    }
    preview
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_windsurf_reset_duration_without_trace_id() {
        let message = "Reached message rate limit for this model. Please try again later. Resets in: 1h22m21s (trace ID: d38c923fabce64a67ff96f15c6840975)";
        assert_eq!(parse_retry_after_secs(message), Some(4941));
    }

    #[test]
    fn classifies_model_rate_limit() {
        let settings = CapacitySettings::default();
        let message = "Windsurf 远端 trailer 错误: {\"error\":{\"code\":\"permission_denied\",\"message\":\"Reached message rate limit for this model. Please try again later. Resets in: 26m32s\"}}";
        assert_eq!(
            classify_upstream_rate_limit(message, &settings),
            Some(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Model,
                retry_after_secs: 1592
            })
        );
    }

    #[test]
    fn release_only_for_request_or_client_compat_errors() {
        let settings = CapacitySettings::default();
        let invalid_argument = "Windsurf 远端 trailer 错误: {\"error\":{\"code\":\"invalid_argument\",\"message\":\"an internal error occurred (trace ID: abc)\"}}";
        let out_of_date = "Windsurf 远端 trailer 错误: {\"error\":{\"code\":\"failed_precondition\",\"message\":\"Your Windsurf version is out of date. Please update to the latest version to continue.\"}}";
        assert_eq!(
            classify_account_failure(invalid_argument, &settings),
            AccountFailureAction::ReleaseOnly
        );
        assert_eq!(
            classify_account_failure(out_of_date, &settings),
            AccountFailureAction::ReleaseOnly
        );
    }

    #[test]
    fn preserves_rate_limit_classification_for_scheduler() {
        let settings = CapacitySettings::default();
        assert_eq!(
            classify_account_failure(
                "Reached message rate limit for this model. Resets in: 2m3s",
                &settings,
            ),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Model,
                retry_after_secs: 123,
            })
        );
        assert_eq!(
            classify_account_failure("global rate limit, retry after: 30s", &settings),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Account,
                retry_after_secs: 30,
            })
        );
    }

    #[test]
    fn preflight_failures_are_rate_limited_for_retry() {
        let settings = CapacitySettings::default();
        assert_eq!(
            classify_account_failure(
                "Windsurf preflight rate-limit failed: CheckUserMessageRateLimit returned no capacity for claude-opus-4-7-medium",
                &settings,
            ),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Account,
                retry_after_secs: settings.suspicious_cooldown_secs,
            })
        );
        assert_eq!(
            classify_account_failure(
                "Windsurf preflight capacity failed: CheckChatCapacity returned no capacity",
                &settings,
            ),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Model,
                retry_after_secs: settings.model_cooldown_secs,
            })
        );
    }

    #[test]
    fn resource_exhausted_provider_errors_are_model_cooldown() {
        let settings = CapacitySettings::default();
        let message = "Windsurf 远端 trailer 错误: {\"error\":{\"code\":\"resource_exhausted\",\"message\":\"The third-party model provider is experiencing issues and is currently not available. Please try this model again later.\"}}";
        assert_eq!(
            classify_account_failure(message, &settings),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Model,
                retry_after_secs: settings.model_cooldown_secs,
            })
        );
        assert!(is_upstream_rate_limit_error(message));
    }

    #[test]
    fn weekly_quota_errors_are_retryable_account_limits() {
        let settings = CapacitySettings::default();
        let message = "Cascade 错误: Your weekly usage quota has been exhausted. Please ensure Windsurf is up to date for the best experience.";
        assert!(is_retryable_before_output_error(message));
        assert!(is_upstream_rate_limit_error(message));
        assert_eq!(
            classify_account_failure(message, &settings),
            AccountFailureAction::RateLimit(UpstreamRateLimit {
                scope: UpstreamRateLimitScope::Account,
                retry_after_secs: settings.suspicious_cooldown_secs,
            })
        );
    }

    #[tokio::test]
    async fn retry_budget_scales_with_ready_account_pool() {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_migrations(&db, &PathBuf::from(".data")).await.unwrap();
        for id in 0..6 {
            sqlx::query("INSERT INTO accounts (email, status, credentials_json, created_at, updated_at) VALUES (?, 'ready', ?, ?, ?)")
                .bind(format!("a{id}@example.com"))
                .bind(json!({ "apiKey": format!("k{id}") }).to_string())
                .bind(now())
                .bind(now())
                .execute(&db)
                .await
                .unwrap();
        }
        let mut settings = CapacitySettings::default();
        settings.max_retries = 1;
        assert_eq!(
            retry_budget_for_account_pool(&db, &settings, "test", "test").await,
            5
        );
    }

    #[tokio::test]
    async fn branch_gate_suppresses_same_session_no_tool_side_branch() {
        let gate = BranchGate::default();
        let messages = vec![EngineMessage {
            role: "user".to_string(),
            content: "看看项目".to_string(),
            ..Default::default()
        }];
        assert_eq!(
            gate.check(Some("claude:s1:model:req"), 25, &messages).await,
            BranchGateDecision::Allow
        );
        assert_eq!(
            gate.check(Some("claude:s1:model:req"), 0, &messages).await,
            BranchGateDecision::SuppressNoToolBranch
        );
    }

    #[tokio::test]
    async fn branch_gate_keeps_other_sessions_isolated() {
        let gate = BranchGate::default();
        let messages = vec![EngineMessage {
            role: "user".to_string(),
            content: "看看项目".to_string(),
            ..Default::default()
        }];
        assert_eq!(
            gate.check(Some("claude:s1:model:req"), 25, &messages).await,
            BranchGateDecision::Allow
        );
        assert_eq!(
            gate.check(Some("claude:s2:model:req"), 0, &messages).await,
            BranchGateDecision::Allow
        );
    }

    #[test]
    fn model_snapshot_matches_resolved_and_upstream_aliases() {
        let account = SchedulerAccount {
            id: 1,
            email: "a@example.com".to_string(),
            status: "ready".to_string(),
            tier: "pro".to_string(),
            tier_manual: false,
            max_concurrent: 1,
            current_concurrent: 0,
            last_used_at: None,
            rate_limited_until: None,
            rate_limit_probe_after: None,
            rpm_limit: 60,
            credits_json: None,
            user_status_json: None,
            available_models_json: Some(
                json!([{ "id": "claude-opus-4-7-medium", "shortName": "opus47" }]).to_string(),
            ),
            tier_models_json: None,
            blocked_models_json: None,
            credentials_json: Some(json!({ "apiKey": "k" }).to_string()),
        };
        assert!(account_supports_model(
            &account,
            "claude-opus-4.7",
            Some("claude-opus-4-7-medium"),
            Some("claude-opus-4-7-medium"),
        ));
        assert!(!account_supports_model(
            &account,
            "claude-sonnet-4.6",
            Some("claude-sonnet-4.6"),
            Some("claude-sonnet-4-6"),
        ));
    }

    #[test]
    fn windsurf_thinking_is_sent_as_text_delta() {
        let delta = anthropic_text_delta(2, "正在分析项目结构".to_string());
        assert_eq!(delta["type"], "content_block_delta");
        assert_eq!(delta["index"], 2);
        assert_eq!(delta["delta"]["type"], "text_delta");
        assert_eq!(delta["delta"]["text"], "正在分析项目结构");
        assert!(delta["delta"].get("thinking").is_none());
        assert!(delta["delta"].get("signature").is_none());
    }

    #[test]
    fn effective_tier_uses_plan_snapshot_for_old_rows() {
        let account = SchedulerAccount {
            id: 1,
            email: "trial@example.com".to_string(),
            status: "ready".to_string(),
            tier: "pro".to_string(),
            tier_manual: false,
            max_concurrent: 1,
            current_concurrent: 0,
            last_used_at: None,
            rate_limited_until: None,
            rate_limit_probe_after: None,
            rpm_limit: 60,
            credits_json: Some(json!({ "planName": "Trial" }).to_string()),
            user_status_json: None,
            available_models_json: None,
            tier_models_json: None,
            blocked_models_json: None,
            credentials_json: Some(json!({ "apiKey": "k" }).to_string()),
        };
        assert_eq!(effective_account_tier(&account), "trial");
    }

    #[test]
    fn only_explicit_account_errors_are_fatal() {
        let settings = CapacitySettings::default();
        assert_eq!(
            classify_account_failure("unauthenticated: invalid api key", &settings),
            AccountFailureAction::FatalAccountError
        );
        assert_eq!(
            classify_account_failure(
                "permission_denied: subscription expired for this account",
                &settings
            ),
            AccountFailureAction::FatalAccountError
        );
        assert_eq!(
            classify_account_failure("permission_denied: request shape rejected", &settings),
            AccountFailureAction::ReleaseOnly
        );
    }

    #[test]
    fn detects_probe_request_like_zephyrsail() {
        let payload = MessagesRequest {
            model: Some("claude-sonnet-4.6".to_string()),
            stream: Some(false),
            max_tokens: Some(1),
            temperature: None,
            top_p: None,
            top_k: None,
            frequency_penalty: None,
            presence_penalty: None,
            system: None,
            tools: None,
            tool_choice: None,
            metadata: Value::Null,
            messages: json!([{ "role": "user", "content": "ping" }]),
        };
        assert!(is_probe_request(&payload));
    }

    #[test]
    fn converts_anthropic_tool_blocks_to_engine_messages() {
        let payload = MessagesRequest {
            model: None,
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            top_k: None,
            frequency_penalty: None,
            presence_penalty: None,
            system: Some(json!("system text")),
            tools: None,
            tool_choice: None,
            metadata: Value::Null,
            messages: json!([
                {
                    "role": "assistant",
                    "content": [
                        { "type": "thinking", "thinking": "plan" },
                        { "type": "tool_use", "id": "toolu_1", "name": "Read", "input": { "file_path": "a.rs" } }
                    ]
                },
                {
                    "role": "user",
                    "content": [
                        { "type": "tool_result", "tool_use_id": "toolu_1", "content": [{ "type": "text", "text": "ok" }] }
                    ]
                }
            ]),
        };
        let messages = messages_from_request(&payload);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[1].reasoning_content.as_deref(), Some("plan"));
        assert_eq!(messages[1].tool_calls[0].name, "Read");
        assert_eq!(messages[2].role, "tool");
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("toolu_1"));
    }

    #[test]
    fn anonymizes_tools_and_degrades_specific_tool_choice() {
        let tools = vec![EngineTool {
            name: "Read".to_string(),
            description: Some("read file".to_string()),
            parameters: Some(json!({ "type": "object", "additionalProperties": false })),
        }];
        let messages = vec![EngineMessage {
            role: "assistant".to_string(),
            tool_calls: vec![engine::EngineToolCall {
                id: "toolu_1".to_string(),
                name: "Read".to_string(),
                arguments: "{}".to_string(),
            }],
            ..Default::default()
        }];
        let isolation = isolate_tool_names(
            tools,
            messages,
            EngineToolChoice::Function {
                name: "Read".to_string(),
            },
            Vec::new(),
            "client_tool",
        )
        .unwrap();
        assert_ne!(isolation.tools[0].name, "Read");
        assert_eq!(
            restore_tool_name(&isolation.tools[0].name, &isolation.to_client_name),
            "Read"
        );
        let degraded = degrade_tool_choice_for_upstream(
            isolation.tool_choice,
            isolation.messages,
            &isolation.to_client_name,
        );
        assert!(matches!(degraded.tool_choice, EngineToolChoice::Auto));
        assert_eq!(degraded.messages[0].role, "system");
        assert!(degraded.messages[0].content.contains("Read"));
    }

    #[test]
    fn extracts_claude_code_primary_working_directory() {
        let messages = vec![
            EngineMessage {
                role: "system".to_string(),
                content: "You are Claude Code.\n<env>\n- Primary working directory: /Users/wangshangbin/My/OpenSource/kiro-rs\n- Is git repo: true\n- Platform: darwin\n</env>"
                    .to_string(),
                ..Default::default()
            },
            EngineMessage {
                role: "user".to_string(),
                content: "看看项目".to_string(),
                ..Default::default()
            },
        ];
        let env = extract_caller_environment(&messages).unwrap();
        assert!(env.contains("- Working directory: /Users/wangshangbin/My/OpenSource/kiro-rs"));
        assert!(env.contains("- Is the directory a git repo: true"));
        assert!(env.contains("- Platform: darwin"));
    }

    #[test]
    fn sanitizes_anthropic_native_tools() {
        let (tools, _) = sanitize_tools(Some(&json!([
            {
                "name": "Bash",
                "description": "Run shell",
                "input_schema": {
                    "type": "object",
                    "properties": { "command": { "type": "string", "description": "command" } },
                    "additionalProperties": false
                }
            }
        ])));
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "Bash");
        assert!(
            tools[0]
                .parameters
                .as_ref()
                .unwrap()
                .get("additionalProperties")
                .is_none()
        );
    }

    #[test]
    fn risk_clients_do_not_reinject_full_tool_documentation() {
        let payload = MessagesRequest {
            model: None,
            stream: None,
            max_tokens: None,
            temperature: None,
            top_p: None,
            top_k: None,
            frequency_penalty: None,
            presence_penalty: None,
            system: Some(json!("original system")),
            tools: Some(json!([
                {
                    "name": "Bash",
                    "description": "x".repeat(MAX_TOOL_DESCRIPTION_LEN + 100),
                    "input_schema": { "type": "object" }
                }
            ])),
            tool_choice: None,
            metadata: Value::Null,
            messages: json!([{ "role": "user", "content": "看看项目" }]),
        };
        let mut engine_messages = messages_from_request(&payload);
        let (mut engine_tools, truncated_docs) = sanitize_tools(payload.tools.as_ref());
        replace_system_prompt_for_tool_description_risk(&mut engine_messages);
        shorten_tool_descriptions_for_risk_client(&mut engine_tools);
        let isolation = isolate_tool_names(
            engine_tools,
            engine_messages,
            EngineToolChoice::Auto,
            truncated_docs,
            DEFAULT_ANONYMOUS_TOOL_NAME_PREFIX,
        )
        .unwrap();
        let degraded = degrade_tool_choice_for_upstream(
            isolation.tool_choice,
            isolation.messages,
            &HashMap::new(),
        );
        let engine_messages = degraded.messages;
        assert!(engine_messages.iter().any(|message| {
            message.role == "system" && message.content == TOOL_DESCRIPTION_RISK_REPLACEMENT_SYSTEM
        }));
        assert!(
            !engine_messages
                .iter()
                .any(|message| message.content.contains("<tool_documentation>"))
        );
    }

    #[tokio::test]
    async fn rejects_client_api_calls_when_no_enabled_key_exists() {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_migrations(&db, &PathBuf::from(".data")).await.unwrap();
        let headers = HeaderMap::new();
        let err = require_client_api_key(&db, &headers, None)
            .await
            .unwrap_err();
        assert_eq!(err.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(err.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value.pointer("/error/type").and_then(Value::as_str),
            Some("client_api_key_required")
        );
    }
}
