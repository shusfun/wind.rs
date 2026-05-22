mod proto;

use anyhow::{Context, anyhow};
use async_stream::try_stream;
use bytes::{Buf, BytesMut};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use futures_core::Stream;
use futures_util::StreamExt;
use reqwest::{Client, Proxy};
use sha2::Digest;
use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use uuid::Uuid;

const DEFAULT_API_URL: &str = "https://server.self-serve.windsurf.com";
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const CHAT_PATH: &str = "/exa.api_server_pb.ApiServerService/GetChatMessage";
const USER_JWT_PATH: &str = "/exa.auth_pb.AuthService/GetUserJwt";
const FRAME_DATA: u8 = 0;
const FRAME_GZIP_DATA: u8 = 1;
const FRAME_TRAILER: u8 = 2;
const FRAME_GZIP_TRAILER: u8 = 3;
const JWT_REFRESH_LEEWAY_SECS: i64 = 120;

const BUILTIN_GET_CHAT_MESSAGE: &[u8] =
    include_bytes!("../../resources/templates/GetChatMessage_req.bin");

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub api_base_url: String,
    pub template_dir: Option<PathBuf>,
    pub request_timeout_ms: u64,
}

impl EngineConfig {
    pub fn from_settings(settings: &HashMap<String, String>, data_dir: PathBuf) -> Self {
        let template_dir = setting_string(settings, "remoteTemplateDir")
            .map(PathBuf::from)
            .filter(|path| !path.as_os_str().is_empty());
        let api_base_url = setting_string(settings, "remoteApiBaseUrl")
            .unwrap_or_else(|| DEFAULT_API_URL.to_string())
            .trim_end_matches('/')
            .to_string();
        Self {
            api_base_url,
            template_dir: template_dir.or_else(|| {
                let path = data_dir.join("remote-templates");
                path.exists().then_some(path)
            }),
            request_timeout_ms: setting_u64(settings, "remoteRequestTimeoutMs", DEFAULT_TIMEOUT_MS),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EngineAccount {
    pub api_key: String,
    pub jwt_token: Option<String>,
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
    pub model_uid: Option<String>,
}

#[derive(Clone, Debug)]
pub struct EngineChunk {
    pub text: String,
    #[allow(dead_code)]
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    #[allow(dead_code)]
    pub stop_reason: Option<String>,
}

#[derive(Clone)]
pub struct RemoteApiEngine {
    inner: Arc<EngineInner>,
}

struct EngineInner {
    config: EngineConfig,
    sessions: Arc<Mutex<SessionStore>>,
    jwt_cache: Arc<Mutex<HashMap<String, CachedJwt>>>,
}

#[derive(Clone, Debug)]
struct CachedJwt {
    token: String,
    exp_sec: Option<i64>,
    cached_at: Instant,
}

#[derive(Clone, Debug)]
struct SessionState {
    conversation_id: String,
    trajectory_run_id: String,
    session_id: String,
    step_number: u64,
    updated_at: Instant,
}

#[derive(Default)]
struct SessionStore {
    entries: HashMap<String, SessionState>,
    order: VecDeque<String>,
}

struct ChatFrame {
    payload: Vec<u8>,
    is_trailer: bool,
}

#[derive(Default)]
struct FrameReader {
    buf: BytesMut,
}

#[derive(Default)]
struct ParsedChatFrame {
    text: String,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    stop_reason: Option<String>,
    conversation_id: Option<String>,
}

impl RemoteApiEngine {
    pub fn new(config: EngineConfig) -> Self {
        Self {
            inner: Arc::new(EngineInner {
                config,
                sessions: Arc::new(Mutex::new(SessionStore::default())),
                jwt_cache: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    pub async fn messages_stream(
        &self,
        trace_id: Option<String>,
        caller_session_key: Option<String>,
        account: EngineAccount,
        model: EngineModel,
        messages: Vec<EngineMessage>,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<EngineChunk>>> {
        let config = self.inner.config.clone();
        let trace_id = trace_id.unwrap_or_else(|| "none".to_string());
        let session_key = session_key(
            &account.api_key,
            &model,
            caller_session_key.as_deref(),
            &messages,
        );
        let session = {
            let mut sessions = self.inner.sessions.lock().await;
            sessions.acquire(&session_key)
        };
        tracing::debug!(
            trace_id = %trace_id,
            model = %model.id,
            model_uid = model.model_uid.as_deref().unwrap_or("none"),
            message_count = messages.len(),
            proxy_configured = account.proxy_url.is_some(),
            step_number = session.step_number + 1,
            "windsurf upstream request prepare"
        );
        tracing::debug!(
            trace_id = %trace_id,
            conversation_hash = %short_hash(&session.conversation_id),
            trajectory_hash = %short_hash(&session.trajectory_run_id),
            session_hash = %short_hash(&session.session_id),
            "windsurf upstream session"
        );
        let client = build_client(account.proxy_url.as_deref(), config.request_timeout_ms)?;
        let jwt_token = self.resolve_jwt(&client, &account).await?;
        let template = load_template(&config, "GetChatMessage_req.bin")?;
        let payload = proto::build_chat_message_request(
            &template,
            proto::ChatRequestParts {
                api_key: &account.api_key,
                jwt_token: &jwt_token,
                model: model.model_uid.as_deref().unwrap_or(&model.id),
                messages: &messages,
                conversation_id: &session.conversation_id,
                trajectory_run_id: &session.trajectory_run_id,
                session_id: &session.session_id,
                step_number: session.step_number + 1,
            },
        )?;
        let url = format!("{}{}", config.api_base_url.trim_end_matches('/'), CHAT_PATH);
        let sessions = self.inner.sessions.clone();
        Ok(try_stream! {
            let upstream_started_at = Instant::now();
            tracing::debug!(
                trace_id = %trace_id,
                path = CHAT_PATH,
                payload_bytes = payload.len(),
                request_timeout_ms = config.request_timeout_ms,
                "windsurf upstream request send"
            );
            let response = client
                .post(&url)
                .header("user-agent", "connect-go/1.18.1 (go1.26.0)")
                .header("content-type", "application/connect+proto")
                .header("connect-protocol-version", "1")
                .header("connect-accept-encoding", "gzip")
                .header("connect-content-encoding", "gzip")
                .header("connect-timeout-ms", config.request_timeout_ms.to_string())
                .header("accept-encoding", "identity")
                .body(build_frame(&payload, true)?)
                .send()
                .await
                .map_err(|err| {
                    tracing::error!(
                        trace_id = %trace_id,
                        error = %redact_log_text(&err.to_string()),
                        elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                        "windsurf upstream send failed"
                    );
                    err
                })
                .context("请求 Windsurf 远端接口失败")?;
            let status = response.status();
            tracing::info!(
                trace_id = %trace_id,
                status = status.as_u16(),
                elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                "windsurf upstream response"
            );
            let response = if status.is_success() {
                response
            } else {
                let status_code = status.as_u16();
                let body = response.text().await.unwrap_or_default();
                tracing::error!(
                    trace_id = %trace_id,
                    status = status_code,
                    body = %redact_log_text(&body),
                    elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                    "windsurf upstream non-success"
                );
                Err::<reqwest::Response, anyhow::Error>(anyhow!(
                    "Windsurf 远端返回 HTTP {}: {}",
                    status_code,
                    redact_log_text(&body)
                ))?;
                unreachable!();
            };
            let mut frames = FrameReader::default();
            let mut saw_text = false;
            let mut latest_prompt_tokens = None;
            let mut latest_completion_tokens = None;
            let mut latest_stop_reason = None;
            let mut stream = response.bytes_stream();
            let mut byte_count = 0_u64;
            let mut frame_count = 0_u64;
            let mut text_chunk_count = 0_u64;
            let mut first_text_logged = false;
            while let Some(item) = stream.next().await {
                let chunk = item.map_err(|err| {
                    tracing::error!(
                        trace_id = %trace_id,
                        error = %redact_log_text(&err.to_string()),
                        elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                        "windsurf upstream stream read failed"
                    );
                    err
                }).context("读取 Windsurf 远端流失败")?;
                byte_count += chunk.len() as u64;
                frames.push(&chunk);
                for frame in frames.drain()? {
                    frame_count += 1;
                    if frame.is_trailer {
                        let trailer = String::from_utf8_lossy(&frame.payload);
                        if trailer.contains("\"error\"") {
                            tracing::error!(
                                trace_id = %trace_id,
                                trailer = %redact_log_text(&trailer),
                                elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                                "windsurf upstream trailer error"
                            );
                            Err::<(), anyhow::Error>(anyhow!(
                                "Windsurf 远端 trailer 错误: {}",
                                redact_log_text(&trailer)
                            ))?;
                        }
                        continue;
                    }
                    let parsed = proto::parse_chat_frame(&frame.payload).map_err(|err| {
                        tracing::error!(
                            trace_id = %trace_id,
                            error = %redact_log_text(&err.to_string()),
                            frame_count,
                            elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                            "windsurf upstream frame parse failed"
                        );
                        err
                    })?;
                    if let Some(conversation_id) = parsed.conversation_id.as_deref().filter(|value| !value.is_empty()) {
                        let mut store = sessions.lock().await;
                        store.update_conversation(&session_key, conversation_id);
                    }
                    if parsed.prompt_tokens.is_some() {
                        latest_prompt_tokens = parsed.prompt_tokens;
                    }
                    if parsed.completion_tokens.is_some() {
                        latest_completion_tokens = parsed.completion_tokens;
                    }
                    if parsed.stop_reason.is_some() {
                        latest_stop_reason = parsed.stop_reason.clone();
                    }
                    if !parsed.text.is_empty() {
                        saw_text = true;
                        text_chunk_count += 1;
                        if !first_text_logged {
                            first_text_logged = true;
                            tracing::debug!(
                                trace_id = %trace_id,
                                first_text_ms = upstream_started_at.elapsed().as_millis() as u64,
                                "windsurf upstream first text"
                            );
                        }
                        tracing::debug!(
                            trace_id = %trace_id,
                            frame_count,
                            text_chunk_count,
                            text_chars = parsed.text.chars().count(),
                            prompt_tokens = latest_prompt_tokens.unwrap_or(0),
                            completion_tokens = latest_completion_tokens.unwrap_or(0),
                            "windsurf upstream text chunk"
                        );
                        yield EngineChunk {
                            text: parsed.text,
                            prompt_tokens: latest_prompt_tokens,
                            completion_tokens: latest_completion_tokens,
                            stop_reason: None,
                        };
                    }
                }
            }
            {
                let mut store = sessions.lock().await;
                store.commit(&session_key);
            }
            tracing::info!(
                trace_id = %trace_id,
                elapsed_ms = upstream_started_at.elapsed().as_millis() as u64,
                byte_count,
                frame_count,
                text_chunk_count,
                prompt_tokens = latest_prompt_tokens.unwrap_or(0),
                completion_tokens = latest_completion_tokens.unwrap_or(0),
                stop_reason = latest_stop_reason.as_deref().unwrap_or("none"),
                "windsurf upstream stream complete"
            );
            if !saw_text {
                yield EngineChunk {
                    text: String::new(),
                    prompt_tokens: latest_prompt_tokens,
                    completion_tokens: latest_completion_tokens,
                    stop_reason: latest_stop_reason.or_else(|| Some("end_turn".to_string())),
                };
            }
        })
    }

    async fn resolve_jwt(
        &self,
        client: &Client,
        account: &EngineAccount,
    ) -> anyhow::Result<String> {
        if let Some(jwt) = account
            .jwt_token
            .as_deref()
            .map(str::trim)
            .filter(|value| is_jwt_like(value))
        {
            return Ok(jwt.to_string());
        }
        let cache_key = account.api_key.clone();
        {
            let cache = self.inner.jwt_cache.lock().await;
            if let Some(cached) = cache.get(&cache_key).filter(|item| item.is_valid()) {
                return Ok(cached.token.clone());
            }
        }
        let token = fetch_user_jwt(client, &self.inner.config, &account.api_key).await?;
        let cached = CachedJwt {
            exp_sec: jwt_exp_sec(&token),
            token: token.clone(),
            cached_at: Instant::now(),
        };
        let mut cache = self.inner.jwt_cache.lock().await;
        cache.insert(cache_key, cached);
        Ok(token)
    }
}

impl CachedJwt {
    fn is_valid(&self) -> bool {
        if !is_jwt_like(&self.token) {
            return false;
        }
        if let Some(exp_sec) = self.exp_sec {
            return chrono::Utc::now().timestamp() < exp_sec - JWT_REFRESH_LEEWAY_SECS;
        }
        self.cached_at.elapsed() < Duration::from_secs(10 * 60)
    }
}

impl SessionStore {
    fn acquire(&mut self, key: &str) -> SessionState {
        let now = Instant::now();
        self.sweep(now);
        if let Some(state) = self.entries.get_mut(key) {
            state.updated_at = now;
            return state.clone();
        }
        let state = SessionState {
            conversation_id: Uuid::new_v4().to_string(),
            trajectory_run_id: Uuid::new_v4().to_string(),
            session_id: Uuid::new_v4().to_string(),
            step_number: 0,
            updated_at: now,
        };
        self.entries.insert(key.to_string(), state.clone());
        self.order.push_back(key.to_string());
        while self.entries.len() > 512 {
            if let Some(old) = self.order.pop_front() {
                self.entries.remove(&old);
            } else {
                break;
            }
        }
        state
    }

    fn update_conversation(&mut self, key: &str, conversation_id: &str) {
        if let Some(state) = self.entries.get_mut(key) {
            state.conversation_id = conversation_id.to_string();
            state.updated_at = Instant::now();
        }
    }

    fn commit(&mut self, key: &str) {
        if let Some(state) = self.entries.get_mut(key) {
            state.step_number += 1;
            state.updated_at = Instant::now();
        }
    }

    fn sweep(&mut self, now: Instant) {
        let max_idle = Duration::from_secs(30 * 60);
        self.entries
            .retain(|_, state| now.duration_since(state.updated_at) <= max_idle);
        self.order.retain(|key| self.entries.contains_key(key));
    }
}

impl FrameReader {
    fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    fn drain(&mut self) -> anyhow::Result<Vec<ChatFrame>> {
        let mut out = Vec::new();
        loop {
            if self.buf.len() < 5 {
                break;
            }
            let flag = self.buf[0];
            let len =
                u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
            if self.buf.len() < 5 + len {
                break;
            }
            self.buf.advance(5);
            let raw = self.buf.split_to(len).to_vec();
            let payload = if flag == FRAME_GZIP_DATA || flag == FRAME_GZIP_TRAILER {
                gunzip(&raw).unwrap_or(raw)
            } else {
                raw
            };
            out.push(ChatFrame {
                payload,
                is_trailer: flag == FRAME_TRAILER || flag == FRAME_GZIP_TRAILER,
            });
        }
        Ok(out)
    }
}

fn build_client(proxy_url: Option<&str>, timeout_ms: u64) -> anyhow::Result<Client> {
    let mut builder = Client::builder().timeout(Duration::from_millis(timeout_ms.max(1_000)));
    if let Some(proxy_url) = proxy_url.filter(|value| !value.trim().is_empty()) {
        builder = builder.proxy(Proxy::all(proxy_url).context("账号代理不可用")?);
    }
    builder.build().context("创建远端请求客户端失败")
}

async fn fetch_user_jwt(
    client: &Client,
    config: &EngineConfig,
    api_key: &str,
) -> anyhow::Result<String> {
    let url = format!(
        "{}{}",
        config.api_base_url.trim_end_matches('/'),
        USER_JWT_PATH
    );
    let body = proto::build_user_jwt_request(api_key);
    let response = client
        .post(&url)
        .header("user-agent", "connect-go/1.18.1 (go1.25.5)")
        .header("content-type", "application/proto")
        .header("connect-protocol-version", "1")
        .header("accept-encoding", "gzip")
        .body(body)
        .send()
        .await
        .context("请求 Windsurf JWT 失败")?;
    let status = response.status();
    let mut body = response
        .bytes()
        .await
        .context("读取 Windsurf JWT 响应失败")?
        .to_vec();
    if body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b {
        body = gunzip(&body).context("Windsurf JWT 响应 gzip 解压失败")?;
    }
    if !status.is_success() {
        let text = String::from_utf8_lossy(&body);
        return Err(anyhow!(
            "Windsurf JWT 返回 HTTP {}: {}",
            status.as_u16(),
            text.chars().take(300).collect::<String>()
        ));
    }
    let text = String::from_utf8_lossy(&body);
    extract_jwt(&text).ok_or_else(|| anyhow!("Windsurf JWT 响应中没有找到 token"))
}

fn load_template(config: &EngineConfig, name: &str) -> anyhow::Result<Vec<u8>> {
    let raw = if let Some(dir) = config.template_dir.as_ref() {
        let path = dir.join(name);
        if path.exists() {
            std::fs::read(&path).with_context(|| format!("读取远端模板失败: {}", path.display()))?
        } else {
            builtin_template(name)?
        }
    } else {
        builtin_template(name)?
    };
    decode_template_payload(&raw)
}

fn builtin_template(name: &str) -> anyhow::Result<Vec<u8>> {
    match name {
        "GetChatMessage_req.bin" => Ok(BUILTIN_GET_CHAT_MESSAGE.to_vec()),
        _ => Err(anyhow!("缺少内置远端模板: {}", name)),
    }
}

fn decode_template_payload(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    if raw.len() >= 5 && raw[0] <= 3 {
        let len = u32::from_be_bytes([raw[1], raw[2], raw[3], raw[4]]) as usize;
        if 5 + len == raw.len() {
            let payload = &raw[5..];
            return if raw[0] == FRAME_GZIP_DATA || raw[0] == FRAME_GZIP_TRAILER {
                gunzip(payload).context("远端模板 gzip 解压失败")
            } else {
                Ok(payload.to_vec())
            };
        }
    }
    if raw.len() >= 2 && raw[0] == 0x1f && raw[1] == 0x8b {
        return gunzip(raw).context("远端模板 raw gzip 解压失败");
    }
    Ok(raw.to_vec())
}

fn build_frame(payload: &[u8], compress: bool) -> anyhow::Result<Vec<u8>> {
    let body = if compress {
        gzip(payload)?
    } else {
        payload.to_vec()
    };
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(if compress {
        FRAME_GZIP_DATA
    } else {
        FRAME_DATA
    });
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

fn gzip(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload)?;
    Ok(encoder.finish()?)
}

fn gunzip(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(payload);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

fn session_key(
    api_key: &str,
    model: &EngineModel,
    caller_session_key: Option<&str>,
    messages: &[EngineMessage],
) -> String {
    let mut seed = format!(
        "{}|model:{}",
        account_key(api_key),
        model.model_uid.as_deref().unwrap_or(&model.id)
    );
    if let Some(caller) = caller_session_key.filter(|value| !value.is_empty()) {
        seed.push_str("|caller:");
        seed.push_str(&short_hash(caller));
        return seed;
    }
    seed.push_str("|messages:");
    for message in messages.iter().take(8) {
        seed.push('|');
        seed.push_str(&message.role);
        seed.push(':');
        seed.push_str(&message.content.chars().take(200).collect::<String>());
    }
    seed
}

fn account_key(api_key: &str) -> String {
    let prefix: String = api_key.chars().take(12).collect();
    format!("remote:{prefix}:{}", api_key.len())
}

fn short_hash(value: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    sha2::Digest::update(&mut hasher, value.as_bytes());
    hex::encode(sha2::Digest::finalize(hasher))
        .chars()
        .take(12)
        .collect()
}

fn redact_log_text(value: &str) -> String {
    let mut out = Vec::new();
    for part in value.split_whitespace().take(80) {
        let lower = part.to_ascii_lowercase();
        let redacted = if lower.contains("authorization")
            || lower.contains("cookie")
            || lower.contains("api_key")
            || lower.contains("apikey")
            || lower.contains("access_token")
            || lower.contains("refresh_token")
            || lower.contains("jwt")
            || lower.contains("token")
            || is_jwt_like(part.trim_matches(|ch: char| {
                !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
            })) {
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

fn extract_jwt(text: &str) -> Option<String> {
    text.split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.'))
        .find(|part| is_jwt_like(part))
        .map(str::to_string)
}

fn is_jwt_like(value: &str) -> bool {
    let mut parts = value.split('.');
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

fn jwt_exp_sec(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64_url_decode(payload)?;
    let value = serde_json::from_slice::<serde_json::Value>(&bytes).ok()?;
    value.get("exp")?.as_i64()
}

fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut buffer = 0_u32;
    let mut bits = 0_u8;
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => break,
            _ => return None,
        } as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    Some(out)
}

fn setting_string(settings: &HashMap<String, String>, key: &str) -> Option<String> {
    settings.get(key).and_then(|value| {
        serde_json::from_str::<String>(value)
            .ok()
            .or_else(|| Some(value.clone()))
    })
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
