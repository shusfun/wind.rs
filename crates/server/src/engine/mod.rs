mod proto;

use anyhow::{Context, anyhow};
use async_stream::stream;
use bytes::Bytes;
use std::{
    collections::HashMap,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    sync::Mutex,
};
use uuid::Uuid;

const DEFAULT_CSRF: &str = "windsurf-api-csrf-fixed-token";
const DEFAULT_API_URL: &str = "https://server.self-serve.windsurf.com";
const LS_SERVICE: &str = "/exa.language_server_pb.LanguageServerService";

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub binary_path: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub max_instances: usize,
    pub cascade_poll_interval_ms: u64,
    pub cascade_max_wait_ms: u64,
    pub cascade_warm_stall_ms: u64,
}

impl EngineConfig {
    pub fn from_settings(settings: &HashMap<String, String>, data_dir: PathBuf) -> Self {
        let binary_path = settings
            .get("lsBinaryPath")
            .and_then(|value| {
                serde_json::from_str::<String>(value)
                    .ok()
                    .or(Some(value.clone()))
            })
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty());
        Self {
            binary_path,
            data_dir: settings
                .get("lsDataDir")
                .and_then(|value| {
                    serde_json::from_str::<String>(value)
                        .ok()
                        .or(Some(value.clone()))
                })
                .map(PathBuf::from)
                .unwrap_or_else(|| data_dir.join("ls")),
            max_instances: setting_usize(settings, "lsMaxInstances", 20),
            cascade_poll_interval_ms: setting_u64(settings, "cascadePollIntervalMs", 500),
            cascade_max_wait_ms: setting_u64(settings, "cascadeMaxWaitMs", 600_000),
            cascade_warm_stall_ms: setting_u64(settings, "cascadeWarmStallMs", 45_000),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EngineAccount {
    pub api_key: String,
    pub proxy_url: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EngineMessage {
    pub role: String,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct EngineModel {
    pub id: String,
    pub enum_value: u64,
    pub model_uid: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EngineChunk {
    pub text: String,
}

#[derive(Clone)]
pub struct WindsurfEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    pool: Mutex<HashMap<String, Arc<Mutex<LsEntry>>>>,
    next_port: Mutex<u16>,
}

struct LsEntry {
    key: String,
    port: u16,
    csrf: String,
    proxy_url: Option<String>,
    child: Child,
    ready: bool,
    session_id: Option<String>,
    workspace_ready: bool,
    started_at: Instant,
}

impl WindsurfEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                config,
                pool: Mutex::new(HashMap::new()),
                next_port: Mutex::new(42100),
            }),
        }
    }

    pub async fn cascade_stream(
        &self,
        account: EngineAccount,
        model: EngineModel,
        messages: Vec<EngineMessage>,
    ) -> anyhow::Result<impl futures_core::Stream<Item = anyhow::Result<EngineChunk>>> {
        let account_key = account_key(&account.api_key);
        let entry = self
            .ensure_ls(account.proxy_url.clone(), account_key)
            .await?;
        let config = self.inner.config.clone();
        Ok(stream! {
            let result = cascade_chat(entry, config, account.api_key, model, messages).await;
            match result {
                Ok(chunks) => {
                    for text in chunks {
                        yield Ok(EngineChunk { text });
                    }
                }
                Err(err) => yield Err(err),
            }
        })
    }

    async fn ensure_ls(
        &self,
        proxy_url: Option<String>,
        account_key: String,
    ) -> anyhow::Result<Arc<Mutex<LsEntry>>> {
        let key = ls_key(proxy_url.as_deref(), &account_key);
        if let Some(entry) = self.inner.pool.lock().await.get(&key).cloned() {
            if entry.lock().await.ready {
                return Ok(entry);
            }
        }
        let binary = self
            .inner
            .config
            .binary_path
            .clone()
            .ok_or_else(|| anyhow!("请先配置 LS 二进制路径"))?;
        if !binary.exists() {
            return Err(anyhow!("LS 二进制不存在：{}", binary.display()));
        }
        let mut pool = self.inner.pool.lock().await;
        if pool.len() >= self.inner.config.max_instances {
            evict_one(&mut pool).await;
        }
        let port = {
            let mut next = self.inner.next_port.lock().await;
            let port = *next;
            *next = next.saturating_add(1);
            port
        };
        let data_dir = self.inner.config.data_dir.join(&key);
        tokio::fs::create_dir_all(data_dir.join("db")).await?;
        let mut cmd = Command::new(binary);
        cmd.arg(format!("--api_server_url={DEFAULT_API_URL}"))
            .arg(format!("--server_port={port}"))
            .arg(format!("--csrf_token={DEFAULT_CSRF}"))
            .arg("--register_user_url=https://api.codeium.com/register_user/")
            .arg(format!("--codeium_dir={}", data_dir.display()))
            .arg(format!("--database_dir={}", data_dir.join("db").display()))
            .arg("--detect_proxy=false")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some(proxy) = proxy_url.as_deref() {
            cmd.env("HTTP_PROXY", proxy)
                .env("HTTPS_PROXY", proxy)
                .env("http_proxy", proxy)
                .env("https_proxy", proxy);
        }
        let child = cmd.spawn().context("启动 LS 失败")?;
        wait_port(port, Duration::from_secs(25)).await?;
        let entry = Arc::new(Mutex::new(LsEntry {
            key: key.clone(),
            port,
            csrf: DEFAULT_CSRF.to_string(),
            proxy_url,
            child,
            ready: true,
            session_id: None,
            workspace_ready: false,
            started_at: Instant::now(),
        }));
        pool.insert(key, entry.clone());
        Ok(entry)
    }
}

async fn cascade_chat(
    entry: Arc<Mutex<LsEntry>>,
    config: EngineConfig,
    api_key: String,
    model: EngineModel,
    messages: Vec<EngineMessage>,
) -> anyhow::Result<Vec<String>> {
    let (port, csrf, session_id) = {
        let mut entry = entry.lock().await;
        let session = entry
            .session_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        entry.session_id = Some(session.clone());
        (entry.port, entry.csrf.clone(), session)
    };
    tracing::debug!(model = %model.id, port, "开始 Cascade 请求");
    warmup_cascade(entry.clone(), &config, &api_key, &session_id).await?;
    let start = proto::build_start_cascade_request(&api_key, &session_id);
    let start_resp = grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/StartCascade"),
        start,
        Duration::from_secs(30),
    )
    .await?;
    let cascade_id = proto::parse_start_cascade_response(&start_resp)?;
    if cascade_id.is_empty() {
        return Err(anyhow!("StartCascade 没有返回会话 ID"));
    }
    let prompt = build_prompt(&messages);
    let send = proto::build_send_cascade_message_request(
        &api_key,
        &cascade_id,
        &prompt,
        model.enum_value,
        model.model_uid.as_deref(),
        &session_id,
    )?;
    grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/SendUserCascadeMessage"),
        send,
        Duration::from_secs(30),
    )
    .await?;
    let deadline = Instant::now() + Duration::from_millis(config.cascade_max_wait_ms);
    let mut chunks = Vec::new();
    let mut yielded_by_step = HashMap::<usize, usize>::new();
    let mut last_growth = Instant::now();
    let mut idle_count = 0_u32;
    let mut saw_active = false;
    let mut saw_text = false;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(config.cascade_poll_interval_ms)).await;
        let req = proto::build_get_cascade_trajectory_steps_request(&cascade_id, 0);
        let resp = grpc_unary(
            port,
            &csrf,
            &format!("{LS_SERVICE}/GetCascadeTrajectorySteps"),
            req,
            Duration::from_secs(30),
        )
        .await?;
        let steps = proto::parse_trajectory_steps(&resp)?;
        for (idx, step) in steps.iter().enumerate() {
            if step.kind == 17 && !step.error_text.is_empty() {
                return Err(anyhow!("Cascade 返回错误：{}", step.error_text));
            }
            if step.status != 1 {
                saw_active = true;
            }
            let live_text = if step.response_text.is_empty() {
                step.modified_text.as_str()
            } else {
                step.response_text.as_str()
            };
            let prev = *yielded_by_step.get(&idx).unwrap_or(&0);
            if live_text.len() > prev {
                let delta = live_text[prev..].to_string();
                yielded_by_step.insert(idx, live_text.len());
                last_growth = Instant::now();
                saw_text = true;
                chunks.push(delta);
            }
        }
        if saw_text && last_growth.elapsed() > Duration::from_millis(config.cascade_warm_stall_ms) {
            return final_sweep(port, &csrf, &cascade_id, &mut yielded_by_step, &mut chunks).await;
        }
        let status_req = proto::build_get_cascade_trajectory_request(&cascade_id);
        let status_resp = grpc_unary(
            port,
            &csrf,
            &format!("{LS_SERVICE}/GetCascadeTrajectory"),
            status_req,
            Duration::from_secs(30),
        )
        .await?;
        let status = proto::parse_trajectory_status(&status_resp)?;
        if status != 1 {
            saw_active = true;
            idle_count = 0;
            continue;
        }
        if !saw_active {
            continue;
        }
        idle_count += 1;
        let growth_settled = last_growth.elapsed()
            > Duration::from_millis(config.cascade_poll_interval_ms.saturating_mul(2));
        if (saw_text && idle_count >= 2 && growth_settled) || (!saw_text && idle_count >= 4) {
            return final_sweep(port, &csrf, &cascade_id, &mut yielded_by_step, &mut chunks).await;
        }
    }
    if chunks.is_empty() {
        Err(anyhow!("Cascade 等待超时"))
    } else {
        Ok(chunks)
    }
}

async fn warmup_cascade(
    entry: Arc<Mutex<LsEntry>>,
    config: &EngineConfig,
    api_key: &str,
    session_id: &str,
) -> anyhow::Result<()> {
    let (port, csrf, data_key, already_ready) = {
        let entry = entry.lock().await;
        (
            entry.port,
            entry.csrf.clone(),
            entry.key.clone(),
            entry.workspace_ready,
        )
    };
    if already_ready {
        return Ok(());
    }
    let workspace_id = &crate::sha256_hex(api_key)[..16];
    let data_workspace = {
        let entry = entry.lock().await;
        tracing::debug!(
            port = entry.port,
            proxy = entry.proxy_url.as_deref().unwrap_or(""),
            age_ms = entry.started_at.elapsed().as_millis(),
            "准备 Cascade workspace warmup"
        );
        data_key
    };
    let workspace_dir = config
        .data_dir
        .join("ls-workspaces")
        .join(data_workspace)
        .join(format!("workspace-{workspace_id}"));
    tokio::fs::create_dir_all(&workspace_dir).await?;
    let workspace_path = workspace_dir.to_string_lossy().to_string();
    let init = proto::build_initialize_panel_state_request(api_key, session_id);
    grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/InitializeCascadePanelState"),
        init,
        Duration::from_secs(5),
    )
    .await?;
    let add_workspace = proto::build_add_tracked_workspace_request(&workspace_path);
    grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/AddTrackedWorkspace"),
        add_workspace,
        Duration::from_secs(5),
    )
    .await?;
    let trust = proto::build_update_workspace_trust_request(api_key, true, session_id);
    grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/UpdateWorkspaceTrust"),
        trust,
        Duration::from_secs(5),
    )
    .await?;
    let heartbeat = proto::build_heartbeat_request(api_key, session_id);
    grpc_unary(
        port,
        &csrf,
        &format!("{LS_SERVICE}/Heartbeat"),
        heartbeat,
        Duration::from_secs(5),
    )
    .await?;
    entry.lock().await.workspace_ready = true;
    Ok(())
}

async fn final_sweep(
    port: u16,
    csrf: &str,
    cascade_id: &str,
    yielded_by_step: &mut HashMap<usize, usize>,
    chunks: &mut Vec<String>,
) -> anyhow::Result<Vec<String>> {
    let req = proto::build_get_cascade_trajectory_steps_request(cascade_id, 0);
    let resp = grpc_unary(
        port,
        csrf,
        &format!("{LS_SERVICE}/GetCascadeTrajectorySteps"),
        req,
        Duration::from_secs(30),
    )
    .await?;
    let steps = proto::parse_trajectory_steps(&resp)?;
    for (idx, step) in steps.iter().enumerate() {
        if step.kind == 17 && !step.error_text.is_empty() {
            return Err(anyhow!("Cascade 返回错误：{}", step.error_text));
        }
        let response_text = step.response_text.as_str();
        let prev = *yielded_by_step.get(&idx).unwrap_or(&0);
        if response_text.len() > prev {
            chunks.push(response_text[prev..].to_string());
            yielded_by_step.insert(idx, response_text.len());
        }
        let cursor = *yielded_by_step.get(&idx).unwrap_or(&0);
        if step.modified_text.len() > cursor && step.modified_text.starts_with(response_text) {
            chunks.push(step.modified_text[cursor..].to_string());
            yielded_by_step.insert(idx, step.modified_text.len());
        }
    }
    Ok(chunks.clone())
}

async fn grpc_unary(
    port: u16,
    csrf: &str,
    path: &str,
    payload: Vec<u8>,
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    let framed = proto::grpc_frame(&payload);
    let tcp = TcpStream::connect(("127.0.0.1", port)).await?;
    let (mut client, connection) = h2::client::handshake(tcp).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("user-agent", "grpc-rust/0.1")
        .header("x-codeium-csrf-token", csrf)
        .body(())
        .map_err(|err| anyhow!(err))?;
    let response = tokio::time::timeout(timeout, async {
        let (response, mut stream) = client.send_request(request, false)?;
        stream.send_data(Bytes::from(framed), true)?;
        let response = response.await?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok::<_, anyhow::Error>(bytes)
    })
    .await
    .map_err(|_| anyhow!("gRPC 请求超时"))??;
    proto::extract_grpc_payload(&response)
}

async fn wait_port(port: u16, timeout: Duration) -> anyhow::Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow!("LS 端口 {} 没有按时就绪", port))
}

async fn evict_one(pool: &mut HashMap<String, Arc<Mutex<LsEntry>>>) {
    let Some(key) = pool.keys().next().cloned() else {
        return;
    };
    if let Some(entry) = pool.remove(&key) {
        let mut entry = entry.lock().await;
        let _ = entry.child.kill().await;
    }
}

fn ls_key(proxy_url: Option<&str>, account_key: &str) -> String {
    let proxy_part = match proxy_url.filter(|value| !value.is_empty()) {
        Some(proxy) => {
            let digest = crate::sha256_hex(proxy);
            format!("px_{}", &digest[..16])
        }
        None => "default".to_string(),
    };
    format!("{proxy_part}_acct_{account_key}")
}

fn account_key(api_key: &str) -> String {
    crate::sha256_hex(api_key).chars().take(16).collect()
}

fn build_prompt(messages: &[EngineMessage]) -> String {
    if messages.is_empty() {
        return String::new();
    }
    let mut system = Vec::new();
    let mut convo = Vec::new();
    for msg in messages {
        if msg.role == "system" {
            system.push(msg.content.clone());
        } else {
            convo.push(msg);
        }
    }
    let mut out = String::new();
    if !system.is_empty() {
        out.push_str(&system.join("\n"));
        out.push_str("\n\n");
    }
    if convo.len() <= 1 {
        out.push_str(convo.last().map(|msg| msg.content.as_str()).unwrap_or(""));
        return out;
    }
    out.push_str("The following is a multi-turn conversation. Use the prior turns as context.\n\n");
    for msg in &convo[..convo.len() - 1] {
        let tag = if msg.role == "assistant" {
            "assistant"
        } else {
            "human"
        };
        out.push_str(&format!("<{tag}>\n{}\n</{tag}>\n\n", msg.content));
    }
    let latest = convo.last().unwrap();
    out.push_str(&format!("<human>\n{}\n</human>", latest.content));
    out
}

fn setting_u64(settings: &HashMap<String, String>, key: &str, fallback: u64) -> u64 {
    settings
        .get(key)
        .and_then(|value| {
            serde_json::from_str::<u64>(value)
                .ok()
                .or_else(|| value.parse().ok())
        })
        .unwrap_or(fallback)
}

fn setting_usize(settings: &HashMap<String, String>, key: &str, fallback: usize) -> usize {
    settings
        .get(key)
        .and_then(|value| {
            serde_json::from_str::<usize>(value)
                .ok()
                .or_else(|| value.parse().ok())
        })
        .unwrap_or(fallback)
}
