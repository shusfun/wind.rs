use super::{EngineMessage, ParsedChatFrame};
use anyhow::{Context, bail};
use uuid::Uuid;

const SYSTEM_FALLBACK: &str = "You are a helpful AI assistant. Respond clearly and concisely.";
const WINDSURF_CLIENT_NAME: &str = "windsurf-next";
const WINDSURF_IDE_VERSION: &str = "1.48.2";
const WINDSURF_LANGUAGE_SERVER_VERSION: &str = "2.2.1017";

pub struct ChatRequestParts<'a> {
    pub api_key: &'a str,
    pub jwt_token: &'a str,
    pub model: &'a str,
    pub messages: &'a [EngineMessage],
    pub conversation_id: &'a str,
    pub trajectory_run_id: &'a str,
    pub session_id: &'a str,
    pub step_number: u64,
}

#[derive(Debug, Clone)]
struct Field {
    number: u64,
    wire_type: u8,
    raw: Vec<u8>,
    value: Vec<u8>,
    varint: Option<u64>,
}

pub fn build_chat_message_request(
    template: &[u8],
    parts: ChatRequestParts<'_>,
) -> anyhow::Result<Vec<u8>> {
    if parts.conversation_id.trim().is_empty() {
        bail!("conversationId 不能为空");
    }
    if parts.trajectory_run_id.trim().is_empty() {
        bail!("trajectoryRunId 不能为空");
    }
    let top_fields = parse_fields(template)?;
    let mut out = Vec::new();
    let mut wrote_messages = false;
    let mut wrote_model = false;
    let mut wrote_conversation = false;
    let mut wrote_trajectory = false;
    let mut wrote_session = false;
    let (system, conversation) = split_messages(parts.messages);
    let last_user_idx = conversation
        .iter()
        .rposition(|message| message.role == "user" || message.role == "tool");

    for field in top_fields {
        match (field.number, field.wire_type) {
            (1, 2) => out.extend(write_message_field(
                1,
                &patch_metadata(&field.value, parts.api_key, parts.jwt_token)?,
            )),
            (2, 2) => out.extend(write_string_field(2, &system)),
            (3, 2) => {
                if !wrote_messages {
                    for (idx, message) in conversation.iter().enumerate() {
                        out.extend(build_message_block(
                            message,
                            last_user_idx.is_some_and(|last| last == idx),
                        ));
                    }
                    wrote_messages = true;
                }
            }
            (15, 2) => {
                out.extend(write_message_field(
                    15,
                    &[
                        write_string_field(1, parts.session_id),
                        write_varint_field(2, parts.step_number),
                        write_varint_field(3, 4),
                        write_varint_field(4, 14),
                    ]
                    .concat(),
                ));
                wrote_session = true;
            }
            (16, 2) => {
                out.extend(write_string_field(16, parts.conversation_id));
                wrote_conversation = true;
            }
            (21, 2) => {
                out.extend(write_string_field(21, parts.model));
                wrote_model = true;
            }
            (22, 2) => {
                out.extend(write_string_field(22, parts.trajectory_run_id));
                wrote_trajectory = true;
            }
            (26, 2) => {}
            _ => out.extend(field.raw),
        }
    }

    if !wrote_messages {
        for (idx, message) in conversation.iter().enumerate() {
            out.extend(build_message_block(
                message,
                last_user_idx.is_some_and(|last| last == idx),
            ));
        }
    }
    if !wrote_session {
        out.extend(write_message_field(
            15,
            &[
                write_string_field(1, parts.session_id),
                write_varint_field(2, parts.step_number),
                write_varint_field(3, 4),
                write_varint_field(4, 14),
            ]
            .concat(),
        ));
    }
    if !wrote_conversation {
        out.extend(write_string_field(16, parts.conversation_id));
    }
    if !wrote_model {
        out.extend(write_string_field(21, parts.model));
    }
    if !wrote_trajectory {
        out.extend(write_string_field(22, parts.trajectory_run_id));
    }
    Ok(out)
}

pub fn build_user_jwt_request(api_key: &str) -> Vec<u8> {
    write_message_field(
        1,
        &[
            write_string_field(1, WINDSURF_CLIENT_NAME),
            write_string_field(2, WINDSURF_IDE_VERSION),
            write_string_field(3, api_key),
            write_string_field(4, "zh-cn"),
            write_string_field(5, &os_metadata_json()),
            write_string_field(7, WINDSURF_LANGUAGE_SERVER_VERSION),
            write_string_field(8, &cpu_metadata_json()),
            write_string_field(12, WINDSURF_CLIENT_NAME),
        ]
        .concat(),
    )
}

pub fn parse_chat_frame(payload: &[u8]) -> anyhow::Result<ParsedChatFrame> {
    let fields = parse_fields(payload)?;
    let mut out = ParsedChatFrame::default();
    for field in fields {
        match (field.number, field.wire_type) {
            (3, 2) => out.text = String::from_utf8_lossy(&field.value).to_string(),
            (5, 0) => out.stop_reason = field.varint.map(stop_reason_name),
            (7, 2) => apply_usage_block(&mut out, &field.value)?,
            (17, 2) => {
                out.conversation_id = Some(String::from_utf8_lossy(&field.value).to_string())
            }
            _ => {}
        }
    }
    Ok(out)
}

fn split_messages(messages: &[EngineMessage]) -> (String, Vec<EngineMessage>) {
    let mut system = String::new();
    let mut conversation = Vec::new();
    for message in messages {
        if message.role == "system" && system.is_empty() && !message.content.trim().is_empty() {
            system = message.content.clone();
        } else {
            conversation.push(message.clone());
        }
    }
    if system.is_empty() {
        system = SYSTEM_FALLBACK.to_string();
    }
    if conversation.is_empty() {
        conversation.push(EngineMessage {
            role: "user".to_string(),
            content: "Hello".to_string(),
        });
    }
    (system, conversation)
}

fn patch_metadata(metadata: &[u8], api_key: &str, jwt_token: &str) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut wrote_api_key = false;
    let mut wrote_jwt = false;
    for field in parse_fields(metadata)? {
        match (field.number, field.wire_type) {
            (3, 2) => {
                out.extend(write_string_field(3, api_key));
                wrote_api_key = true;
            }
            (16 | 24 | 27 | 31, _) => {}
            (21, 2) => {
                out.extend(write_string_field(21, jwt_token));
                wrote_jwt = true;
            }
            _ => out.extend(field.raw),
        }
    }
    if !wrote_api_key {
        out.extend(write_string_field(3, api_key));
    }
    if !wrote_jwt {
        out.extend(write_string_field(21, jwt_token));
    }
    let now = chrono::Utc::now();
    out.extend(write_message_field(
        16,
        &[
            write_varint_field(1, now.timestamp().max(0) as u64),
            write_varint_field(2, now.timestamp_subsec_nanos() as u64),
        ]
        .concat(),
    ));
    Ok(out)
}

fn build_message_block(message: &EngineMessage, current_turn: bool) -> Vec<u8> {
    let role = match message.role.as_str() {
        "assistant" => 2,
        "tool" => 4,
        _ => 1,
    };
    let mut inner = Vec::new();
    if role == 2 {
        inner.extend(write_string_field(1, &format!("bot-{}", Uuid::new_v4())));
    }
    inner.extend(write_varint_field(2, role));
    inner.extend(write_string_field(3, &message.content));
    inner.extend(write_varint_field(4, estimate_tokens(&message.content)));
    if role == 1 {
        inner.extend(write_varint_field(5, 1));
    }
    if current_turn && (role == 1 || role == 4) {
        inner.extend(write_bytes_field(8, &[8, 1]));
    }
    write_message_field(3, &inner)
}

fn apply_usage_block(out: &mut ParsedChatFrame, payload: &[u8]) -> anyhow::Result<()> {
    for field in parse_fields(payload)? {
        match (field.number, field.wire_type) {
            (3, 0) => out.completion_tokens = field.varint,
            (4, 0) => out.prompt_tokens = field.varint,
            (5, 0) => {}
            (6, 0) if out.stop_reason.is_none() => {
                out.stop_reason = field.varint.map(stop_reason_name)
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_fields(buf: &[u8]) -> anyhow::Result<Vec<Field>> {
    let mut fields = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let start = pos;
        let (tag, used) = decode_varint(&buf[pos..]).context("protobuf tag 解析失败")?;
        pos += used;
        let number = tag >> 3;
        let wire_type = (tag & 0x07) as u8;
        let mut value = Vec::new();
        let mut varint = None;
        match wire_type {
            0 => {
                let (parsed, used) = decode_varint(&buf[pos..])?;
                pos += used;
                varint = Some(parsed);
            }
            1 => {
                if pos + 8 > buf.len() {
                    bail!("fixed64 字段截断");
                }
                value.extend_from_slice(&buf[pos..pos + 8]);
                pos += 8;
            }
            2 => {
                let (len, used) = decode_varint(&buf[pos..])?;
                pos += used;
                let len = len as usize;
                if pos + len > buf.len() {
                    bail!("length-delimited 字段截断");
                }
                value.extend_from_slice(&buf[pos..pos + len]);
                pos += len;
            }
            5 => {
                if pos + 4 > buf.len() {
                    bail!("fixed32 字段截断");
                }
                value.extend_from_slice(&buf[pos..pos + 4]);
                pos += 4;
            }
            other => bail!("未知 protobuf wire type {}", other),
        }
        fields.push(Field {
            number,
            wire_type,
            raw: buf[start..pos].to_vec(),
            value,
            varint,
        });
    }
    Ok(fields)
}

fn write_varint_field(field: u64, value: u64) -> Vec<u8> {
    [encode_varint((field << 3) | 0), encode_varint(value)].concat()
}

fn write_string_field(field: u64, value: &str) -> Vec<u8> {
    write_bytes_field(field, value.as_bytes())
}

fn write_bytes_field(field: u64, value: &[u8]) -> Vec<u8> {
    [
        encode_varint((field << 3) | 2),
        encode_varint(value.len() as u64),
        value.to_vec(),
    ]
    .concat()
}

fn write_message_field(field: u64, value: &[u8]) -> Vec<u8> {
    write_bytes_field(field, value)
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

fn decode_varint(buf: &[u8]) -> anyhow::Result<(u64, usize)> {
    let mut result = 0_u64;
    let mut shift = 0_u32;
    for (idx, byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, idx + 1));
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint 溢出");
        }
    }
    bail!("varint 截断")
}

fn estimate_tokens(text: &str) -> u64 {
    ((text.chars().count() as f64 / 3.5).ceil() as u64).max(1)
}

fn stop_reason_name(value: u64) -> String {
    match value {
        4 => "tool_use",
        _ => "end_turn",
    }
    .to_string()
}

fn os_metadata_json() -> String {
    let (os_name, arch) = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => ("darwin", "arm64"),
        ("macos", _) => ("darwin", "x64"),
        ("windows", "x86_64") => ("windows", "x64"),
        ("windows", _) => ("windows", std::env::consts::ARCH),
        ("linux", "x86_64") => ("linux", "x64"),
        ("linux", "aarch64") => ("linux", "arm64"),
        (os, arch) => (os, arch),
    };
    serde_json::json!({
        "Os": os_name,
        "Arch": arch,
        "Version": "15.0",
        "ProductName": std::env::consts::OS,
        "MajorVersionNumber": 15,
        "MinorVersionNumber": 0,
        "Build": "0"
    })
    .to_string()
}

fn cpu_metadata_json() -> String {
    let threads = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    serde_json::json!({
        "NumSockets": 1,
        "NumCores": (threads / 2).max(1),
        "NumThreads": threads,
        "VendorID": "",
        "Family": "",
        "Model": "",
        "ModelName": std::env::consts::ARCH,
        "Memory": 0
    })
    .to_string()
}
