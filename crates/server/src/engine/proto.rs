use anyhow::{anyhow, bail};
use uuid::Uuid;

#[derive(Debug)]
pub struct TrajectoryStep {
    pub kind: u64,
    pub status: u64,
    pub response_text: String,
    pub modified_text: String,
    pub error_text: String,
}

pub fn build_initialize_panel_state_request(api_key: &str, session_id: &str) -> Vec<u8> {
    [
        write_message_field(1, &build_metadata(api_key, session_id)),
        write_bool_field(3, true),
    ]
    .concat()
}

pub fn build_heartbeat_request(api_key: &str, session_id: &str) -> Vec<u8> {
    write_message_field(1, &build_metadata(api_key, session_id))
}

pub fn build_add_tracked_workspace_request(workspace_path: &str) -> Vec<u8> {
    write_string_field(1, workspace_path)
}

pub fn build_update_workspace_trust_request(
    api_key: &str,
    trusted: bool,
    session_id: &str,
) -> Vec<u8> {
    [
        write_message_field(1, &build_metadata(api_key, session_id)),
        write_bool_field(2, trusted),
    ]
    .concat()
}

pub fn build_start_cascade_request(api_key: &str, session_id: &str) -> Vec<u8> {
    [
        write_message_field(1, &build_metadata(api_key, session_id)),
        write_varint_field(4, 1),
        write_varint_field(5, 1),
    ]
    .concat()
}

pub fn build_send_cascade_message_request(
    api_key: &str,
    cascade_id: &str,
    text: &str,
    model_enum: u64,
    model_uid: Option<&str>,
    session_id: &str,
) -> anyhow::Result<Vec<u8>> {
    Ok([
        write_string_field(1, cascade_id),
        write_message_field(2, &write_string_field(1, text)),
        write_message_field(3, &build_metadata(api_key, session_id)),
        write_message_field(5, &build_cascade_config(model_enum, model_uid)?),
    ]
    .concat())
}

pub fn build_get_cascade_trajectory_steps_request(cascade_id: &str, step_offset: u64) -> Vec<u8> {
    let mut parts = vec![write_string_field(1, cascade_id)];
    if step_offset > 0 {
        parts.push(write_varint_field(2, step_offset));
    }
    parts.concat()
}

pub fn build_get_cascade_trajectory_request(cascade_id: &str) -> Vec<u8> {
    write_string_field(1, cascade_id)
}

pub fn parse_start_cascade_response(buf: &[u8]) -> anyhow::Result<String> {
    let fields = parse_fields(buf)?;
    Ok(fields
        .iter()
        .find(|field| field.number == 1 && field.wire_type == 2)
        .map(|field| String::from_utf8_lossy(&field.value).to_string())
        .unwrap_or_default())
}

pub fn parse_trajectory_status(buf: &[u8]) -> anyhow::Result<u64> {
    let fields = parse_fields(buf)?;
    Ok(get_varint(&fields, 2).unwrap_or(0))
}

pub fn parse_trajectory_steps(buf: &[u8]) -> anyhow::Result<Vec<TrajectoryStep>> {
    let fields = parse_fields(buf)?;
    let mut steps = Vec::new();
    for step in fields
        .iter()
        .filter(|field| field.number == 1 && field.wire_type == 2)
    {
        let step_fields = parse_fields(&step.value)?;
        let kind = get_varint(&step_fields, 1).unwrap_or(0);
        let status = get_varint(&step_fields, 4).unwrap_or(0);
        let mut response_text = String::new();
        let mut modified_text = String::new();
        let mut error_text = String::new();
        if let Some(planner) = get_len(&step_fields, 20) {
            let planner_fields = parse_fields(planner)?;
            if let Some(response) = get_len(&planner_fields, 1) {
                response_text = String::from_utf8_lossy(response).to_string();
            }
            if let Some(modified) = get_len(&planner_fields, 8) {
                modified_text = String::from_utf8_lossy(modified).to_string();
            }
        }
        if let Some(error) = get_len(&step_fields, 24) {
            if let Some(inner) = get_len(&parse_fields(error)?, 3) {
                error_text = read_error_details(inner)?;
            }
        }
        if error_text.is_empty() {
            if let Some(error) = get_len(&step_fields, 31) {
                error_text = read_error_details(error)?;
            }
        }
        steps.push(TrajectoryStep {
            kind,
            status,
            response_text,
            modified_text,
            error_text,
        });
    }
    Ok(steps)
}

pub fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 5);
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

pub fn extract_grpc_payload(buf: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0;
    while pos + 5 <= buf.len() {
        if buf[pos] != 0 {
            break;
        }
        let len =
            u32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]) as usize;
        if pos + 5 + len > buf.len() {
            break;
        }
        out.extend_from_slice(&buf[pos + 5..pos + 5 + len]);
        pos += 5 + len;
    }
    if out.is_empty() && !buf.is_empty() {
        Ok(buf.to_vec())
    } else if out.is_empty() {
        Err(anyhow!("gRPC 返回为空"))
    } else {
        Ok(out)
    }
}

fn build_metadata(api_key: &str, session_id: &str) -> Vec<u8> {
    [
        write_string_field(1, "windsurf"),
        write_string_field(2, "2.0.67"),
        write_string_field(3, api_key),
        write_string_field(4, "en"),
        write_string_field(5, os_name()),
        write_string_field(7, "2.0.67"),
        write_string_field(8, arch_name()),
        write_varint_field(9, rand_request_id()),
        write_string_field(10, session_id),
        write_string_field(12, "windsurf"),
    ]
    .concat()
}

fn build_cascade_config(model_enum: u64, model_uid: Option<&str>) -> anyhow::Result<Vec<u8>> {
    if model_enum == 0 && model_uid.is_none() {
        bail!("模型缺少 Windsurf enum/modelUid");
    }
    let no_tool_section = [
        write_varint_field(1, 1),
        write_string_field(2, "No tools are available."),
    ]
    .concat();
    let additional = [
        write_varint_field(1, 1),
        write_string_field(
            2,
            "Answer directly in the user's language. Do not claim file, shell, or IDE access.",
        ),
    ]
    .concat();
    let communication = [
        write_varint_field(1, 1),
        write_string_field(2, "Respond clearly and directly."),
    ]
    .concat();
    let conversational = [
        write_varint_field(4, 3),
        write_message_field(10, &no_tool_section),
        write_message_field(12, &additional),
        write_message_field(13, &communication),
    ]
    .concat();
    let mut planner_parts = vec![write_message_field(2, &conversational)];
    if let Some(uid) = model_uid {
        planner_parts.push(write_string_field(35, uid));
        planner_parts.push(write_string_field(34, uid));
    }
    if model_enum > 0 {
        planner_parts.push(write_message_field(15, &write_varint_field(1, model_enum)));
        planner_parts.push(write_varint_field(1, model_enum));
    }
    planner_parts.push(write_varint_field(6, 32768));
    let empty_section = [write_varint_field(1, 1), write_string_field(2, "")].concat();
    planner_parts.push(write_message_field(11, &empty_section));
    let planner = planner_parts.concat();
    let memory = write_varint_field(1, 0);
    let brain = [
        write_varint_field(1, 1),
        write_message_field(6, &write_message_field(6, &[])),
    ]
    .concat();
    Ok([
        write_message_field(1, &planner),
        write_message_field(5, &memory),
        write_message_field(7, &brain),
    ]
    .concat())
}

#[derive(Debug)]
struct Field {
    number: u64,
    wire_type: u8,
    value: Vec<u8>,
    varint: Option<u64>,
}

fn parse_fields(buf: &[u8]) -> anyhow::Result<Vec<Field>> {
    let mut fields = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let (tag, used) = decode_varint(&buf[pos..])?;
        pos += used;
        let number = tag >> 3;
        let wire_type = (tag & 0x07) as u8;
        match wire_type {
            0 => {
                let (value, used) = decode_varint(&buf[pos..])?;
                pos += used;
                fields.push(Field {
                    number,
                    wire_type,
                    value: Vec::new(),
                    varint: Some(value),
                });
            }
            1 => {
                if pos + 8 > buf.len() {
                    bail!("fixed64 截断");
                }
                fields.push(Field {
                    number,
                    wire_type,
                    value: buf[pos..pos + 8].to_vec(),
                    varint: None,
                });
                pos += 8;
            }
            2 => {
                let (len, used) = decode_varint(&buf[pos..])?;
                pos += used;
                let len = len as usize;
                if pos + len > buf.len() {
                    bail!("length-delimited 字段截断");
                }
                fields.push(Field {
                    number,
                    wire_type,
                    value: buf[pos..pos + len].to_vec(),
                    varint: None,
                });
                pos += len;
            }
            5 => {
                if pos + 4 > buf.len() {
                    bail!("fixed32 截断");
                }
                fields.push(Field {
                    number,
                    wire_type,
                    value: buf[pos..pos + 4].to_vec(),
                    varint: None,
                });
                pos += 4;
            }
            other => bail!("未知 protobuf wire type {}", other),
        }
    }
    Ok(fields)
}

fn get_varint(fields: &[Field], number: u64) -> Option<u64> {
    fields
        .iter()
        .find(|field| field.number == number && field.wire_type == 0)
        .and_then(|field| field.varint)
}

fn get_len(fields: &[Field], number: u64) -> Option<&[u8]> {
    fields
        .iter()
        .find(|field| field.number == number && field.wire_type == 2)
        .map(|field| field.value.as_slice())
}

fn read_error_details(buf: &[u8]) -> anyhow::Result<String> {
    let fields = parse_fields(buf)?;
    for number in [1_u64, 2, 3] {
        if let Some(value) = get_len(&fields, number) {
            let msg = String::from_utf8_lossy(value)
                .trim()
                .lines()
                .next()
                .unwrap_or("")
                .to_string();
            if !msg.is_empty() {
                return Ok(msg.chars().take(300).collect());
            }
        }
    }
    Ok(String::new())
}

fn write_varint_field(field: u64, value: u64) -> Vec<u8> {
    [encode_varint((field << 3) | 0), encode_varint(value)].concat()
}

fn write_bool_field(field: u64, value: bool) -> Vec<u8> {
    if value {
        write_varint_field(field, 1)
    } else {
        Vec::new()
    }
}

fn write_string_field(field: u64, value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    [
        encode_varint((field << 3) | 2),
        encode_varint(bytes.len() as u64),
        bytes.to_vec(),
    ]
    .concat()
}

fn write_message_field(field: u64, value: &[u8]) -> Vec<u8> {
    if value.is_empty() {
        return Vec::new();
    }
    [
        encode_varint((field << 3) | 2),
        encode_varint(value.len() as u64),
        value.to_vec(),
    ]
    .concat()
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

fn os_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

fn arch_name() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "x86_64"
    }
}

fn rand_request_id() -> u64 {
    let id = Uuid::new_v4();
    let bytes = id.as_bytes();
    u64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8])) & 0x0000_FFFF_FFFF_FFFF
}
