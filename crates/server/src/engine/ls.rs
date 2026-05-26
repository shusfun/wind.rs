use anyhow::{Context, anyhow, bail};
use reqwest::Client;
use sha2::Digest;
use std::{
    env,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{process::Command, sync::Mutex, time::sleep};
use uuid::Uuid;

const DEFAULT_CSRF_TOKEN: &str = "windsurf-rs-csrf-token";
const LS_SERVICE: &str = "/exa.language_server_pb.LanguageServerService";

#[derive(Clone, Debug)]
pub struct LsConfig {
    pub api_server_url: String,
    pub data_dir: PathBuf,
    pub binary_path: PathBuf,
    pub csrf_token: String,
}

impl LsConfig {
    pub fn new(api_server_url: String, data_dir: PathBuf, binary_path: PathBuf) -> Self {
        Self {
            api_server_url,
            data_dir,
            binary_path,
            csrf_token: DEFAULT_CSRF_TOKEN.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct LsPool {
    inner: Arc<Mutex<Option<LsEntry>>>,
    config: LsConfig,
}

#[derive(Debug)]
struct LsEntry {
    port: u16,
    csrf_token: String,
    session_id: String,
    _child: tokio::process::Child,
    warmed: bool,
}

#[derive(Clone, Debug)]
pub struct LsHandle {
    pub port: u16,
    pub csrf_token: String,
    pub session_id: String,
}

impl LsPool {
    pub fn new(config: LsConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            config,
        }
    }

    pub async fn ensure(&self) -> anyhow::Result<LsHandle> {
        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.as_mut() {
            if entry_is_running(entry) && port_accepts_tcp(entry.port).await {
                return Ok(entry.handle());
            }
        }

        let port = pick_free_port().context("查找 Windsurf LS 可用端口失败")?;
        let session_id = Uuid::new_v4().to_string();
        let codeium_dir = self.config.data_dir.join("default");
        let db_dir = codeium_dir.join("db");
        std::fs::create_dir_all(&db_dir)
            .with_context(|| format!("创建 LS 数据目录失败: {}", db_dir.display()))?;
        let binary = self.config.binary_path.clone();
        if !binary.exists() {
            bail!(
                "Windsurf LS 二进制不存在: {}，请设置 WINDSURF_RS_LS_BINARY 或放置参考项目的 LS 文件",
                binary.display()
            );
        }

        let mut cmd = Command::new(&binary);
        cmd.arg(format!("--api_server_url={}", self.config.api_server_url))
            .arg(format!("--server_port={port}"))
            .arg(format!("--csrf_token={}", self.config.csrf_token))
            .arg("--register_user_url=https://api.codeium.com/register_user/")
            .arg(format!("--codeium_dir={}", codeium_dir.display()))
            .arg(format!("--database_dir={}", db_dir.display()))
            .arg("--detect_proxy=false")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd
            .spawn()
            .with_context(|| format!("启动 Windsurf LS 失败: {}", binary.display()))?;
        let entry = LsEntry {
            port,
            csrf_token: self.config.csrf_token.clone(),
            session_id,
            _child: child,
            warmed: false,
        };
        *guard = Some(entry);
        drop(guard);

        wait_port_ready(port, Duration::from_secs(25)).await?;
        tracing::info!(port, binary = %binary.display(), "windsurf ls ready");
        let guard = self.inner.lock().await;
        guard
            .as_ref()
            .map(LsEntry::handle)
            .ok_or_else(|| anyhow!("Windsurf LS 启动后状态丢失"))
    }

    pub async fn warmup(&self, api_key: &str) -> anyhow::Result<LsHandle> {
        let handle = self.ensure().await?;
        {
            let guard = self.inner.lock().await;
            if guard.as_ref().is_some_and(|entry| entry.warmed) {
                return Ok(handle);
            }
        }

        let workspace_id = short_hash(api_key);
        let workspace_path = self
            .config
            .data_dir
            .join("workspaces")
            .join(format!("workspace-{workspace_id}"));
        std::fs::create_dir_all(&workspace_path)
            .with_context(|| format!("创建 LS 工作区目录失败: {}", workspace_path.display()))?;
        let workspace_str = workspace_path.to_string_lossy().to_string();
        let workspace_uri = format!("file://{workspace_str}");

        let init = crate::engine::proto::build_initialize_panel_state_request(
            api_key,
            &handle.session_id,
            true,
        );
        self.warmup_step(
            &handle,
            "InitializeCascadePanelState",
            &init,
            Duration::from_secs(5),
            false,
        )
        .await?;

        let add_ws = crate::engine::proto::build_add_tracked_workspace_request(&workspace_str);
        self.warmup_step(
            &handle,
            "AddTrackedWorkspace",
            &add_ws,
            Duration::from_secs(5),
            false,
        )
        .await?;

        let trust = crate::engine::proto::build_update_workspace_trust_request(
            api_key,
            &handle.session_id,
            true,
        );
        self.warmup_step(
            &handle,
            "UpdateWorkspaceTrust",
            &trust,
            Duration::from_secs(5),
            true,
        )
        .await
        .with_context(|| format!("信任 Cascade 工作区失败: {workspace_uri}"))?;

        let heartbeat = crate::engine::proto::build_heartbeat_request(api_key, &handle.session_id);
        self.warmup_step(
            &handle,
            "Heartbeat",
            &heartbeat,
            Duration::from_secs(5),
            false,
        )
        .await?;

        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.as_mut().filter(|entry| entry.port == handle.port) {
            entry.warmed = true;
        }
        Ok(handle)
    }

    async fn warmup_step(
        &self,
        handle: &LsHandle,
        method: &str,
        payload: &[u8],
        timeout: Duration,
        error_level_for_non_transport: bool,
    ) -> anyhow::Result<()> {
        match self.grpc_unary(handle, method, payload, timeout).await {
            Ok(_) => Ok(()),
            Err(err) if is_cascade_transport_error(&err) => {
                Err(err).with_context(|| format!("{method} 传输失败"))
            }
            Err(err) => {
                if error_level_for_non_transport {
                    tracing::error!(
                        port = handle.port,
                        method,
                        error = %err,
                        "windsurf ls warmup step failed; continuing like WindsurfAPI"
                    );
                } else {
                    tracing::warn!(
                        port = handle.port,
                        method,
                        error = %err,
                        "windsurf ls warmup step failed; continuing like WindsurfAPI"
                    );
                }
                Ok(())
            }
        }
    }

    pub async fn grpc_unary(
        &self,
        handle: &LsHandle,
        method: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> anyhow::Result<Vec<u8>> {
        let client = Client::builder()
            .http2_prior_knowledge()
            .timeout(timeout)
            .build()
            .context("创建 LS HTTP/2 客户端失败")?;
        let url = format!("http://127.0.0.1:{}{LS_SERVICE}/{method}", handle.port);
        let response = client
            .post(url)
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("user-agent", "grpc-rust/0.1")
            .header("x-codeium-csrf-token", &handle.csrf_token)
            .body(grpc_frame(payload))
            .send()
            .await
            .with_context(|| format!("请求 LS {method} 失败"))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("读取 LS {method} 响应失败"))?
            .to_vec();
        if !status.is_success() {
            bail!(
                "LS {method} 返回 HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(300)
                    .collect::<String>()
            );
        }
        Ok(extract_grpc_payload(&bytes))
    }
}

fn is_cascade_transport_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("pending stream has been canceled")
        || message.contains("econnreset")
        || message.contains("err_http2")
        || message.contains("session closed")
        || message.contains("stream closed")
        || message.contains("panel state")
}

impl LsEntry {
    fn handle(&self) -> LsHandle {
        LsHandle {
            port: self.port,
            csrf_token: self.csrf_token.clone(),
            session_id: self.session_id.clone(),
        }
    }
}

fn entry_is_running(entry: &mut LsEntry) -> bool {
    match entry._child.try_wait() {
        Ok(None) => true,
        Ok(Some(status)) => {
            tracing::warn!(port = entry.port, status = %status, "windsurf ls exited");
            false
        }
        Err(err) => {
            tracing::warn!(port = entry.port, error = %err, "windsurf ls status check failed");
            false
        }
    }
}

async fn port_accepts_tcp(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .is_ok()
}

fn pick_free_port() -> anyhow::Result<u16> {
    for port in 42100..42250 {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        if TcpListener::bind(addr).is_ok() {
            return Ok(port);
        }
    }
    bail!("42100-42249 没有可用 LS 端口")
}

async fn wait_port_ready(port: u16, timeout: Duration) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if port_accepts_tcp(port).await {
            return Ok(());
        }
        sleep(Duration::from_millis(300)).await;
    }
    bail!("LS 端口 {port} 在 {} 秒内没有就绪", timeout.as_secs())
}

fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn extract_grpc_payload(buf: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut offset = 0;
    while offset + 5 <= buf.len() {
        let compressed = buf[offset];
        let len = u32::from_be_bytes([
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
        ]) as usize;
        if compressed != 0 || offset + 5 + len > buf.len() {
            break;
        }
        out.extend_from_slice(&buf[offset + 5..offset + 5 + len]);
        offset += 5 + len;
    }
    if out.is_empty() { buf.to_vec() } else { out }
}

fn short_hash(value: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, value.as_bytes());
    hex::encode(sha2::Digest::finalize(hasher))
        .chars()
        .take(16)
        .collect()
}

pub fn default_binary_path(workspace_dir: &Path) -> PathBuf {
    if let Ok(path) = env::var("WINDSURF_RS_LS_BINARY") {
        if path.trim().is_empty() {
            return workspace_dir.join("WindsurfAPI/.local/bin/language_server_macos_x64");
        }
        return PathBuf::from(path);
    }
    workspace_dir.join("WindsurfAPI/.local/bin/language_server_macos_x64")
}
