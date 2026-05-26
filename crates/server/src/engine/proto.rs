use super::{EngineMessage, EngineSamplingParams, EngineTool, EngineToolChoice, ParsedChatFrame};
use anyhow::{Context, bail};
use uuid::Uuid;

const SYSTEM_FALLBACK: &str = "You are a helpful AI assistant. Respond clearly and concisely.";
const WINDSURF_CLIENT_NAME: &str = "windsurf-next";
const WINDSURF_IDE_VERSION: &str = "1.48.2";
const WINDSURF_LANGUAGE_SERVER_VERSION: &str = "2.2.1017";
const F3_MSG_BOT_ID: u64 = 1;
const F3_MSG_ROLE: u64 = 2;
const F3_MSG_CONTENT: u64 = 3;
const F3_MSG_TOKEN_COUNT: u64 = 4;
const F3_MSG_USER_FLAG: u64 = 5;
const F3_MSG_TOOL_CALL: u64 = 6;
const F3_MSG_TOOL_CALL_ID: u64 = 7;
const F3_MSG_CURRENT_TURN: u64 = 8;
const F3_MSG_REASONING: u64 = 11;
const CURRENT_TURN_MARKER: &[u8] = &[8, 1];
const F10_TOOL_NAME: u64 = 1;
const F10_TOOL_DESC: u64 = 2;
const F10_TOOL_PARAMS_JSON: u64 = 3;
const F10_TOOL_STREAM_ARG_NAME: u64 = 5;
const F12_TOOL_CHOICE: u64 = 1;
const F8_MAX_TOKENS: u64 = 2;
const F8_MAX_TOOL_CALLS: u64 = 3;
const F8_TEMPERATURE: u64 = 5;
const F8_TOP_P: u64 = 6;
const F8_TOP_K: u64 = 7;
const F8_FREQUENCY_PENALTY: u64 = 8;
const F8_PRESENCE_PENALTY: u64 = 11;
const CASCADE_F7_VALUE: u64 = 5;
const LARGE_TEXT_TOOL_PARAM_NAMES: [&str; 3] = ["CodeContent", "Input", "new_source"];
const WINDSURF_WRAP_MEMORY_MESSAGE: &str =
    "No MEMORIES were retrieved. Continue your work without acknowledging this message.";
const TEMPLATE_PREFIX: &str = r#"You are a powerful agentic AI coding assistant acting as a senior pair programmer.
The USER is interacting with you through a chat panel in their IDE and will send you requests to solve a coding task by pair programming with you.
Your goal is to help the user complete their task effectively and safely.
Be mindful that you are not the only actor in the environment; avoid unnecessary or disruptive changes.
The task may require modifying or debugging existing code, answering a question about existing code, or creating new code.
Make only changes that are clearly justified by the task. Do not create files, modify code, or run commands unless they are relevant and necessary.
<communication_style>
Be concise and avoid unnecessary verbosity.
Prefer short paragraphs or bullet points over long blocks of text.
Target:
*50-300 tokens* for simple queries,
*300-800 tokens* for complex tasks.
*800+ tokens* for substantial code changes or providing detailed technical explanations that genuinely require it.
Refer to the USER in the second person and yourself in the first person.
Be precise and rigorous: do not invent APIs, functions, files, or parameters.
- When feeling uncertain, use tools to gather more information, and clearly state your uncertainty if there's no way to get unstuck.
- By default, implement changes rather than only suggesting them. If the user's intent is unclear, you can try to clarify the user's intent by asking specific questions about what they want to accomplish or by using tools to read files and explore the workspace.
- When seeing a new user request, do not repeat your initial response. It is okay if you keep working and update the user with more information later but your messages should not be repetitive.
- Direct responses: Always provide an initial commentary message immediately to acknowledge the request and explain your approach. Do not use thinking tokens or preambles before this initial commentary - communicate with the user right away.
<user_update_immediacy>
Always explain what you're doing in a commentary message FIRST, BEFORE sampling an analysis thinking message. This is critical in order to communicate immediately to the user.
</user_update_immediacy>
- If you require user assistance, you should communicate this.
- Code style: Do not add or delete ***ANY*** comments or documentation unless asked.
- Always end a conversation with a clear and concise summary of the task completion status.
<markdown>
- IMPORTANT: Format your messages with Markdown.
- Use single backtick inline code for variable or function names.
- Use fenced code blocks with language when referencing code snippets.
- Bold or italicize critical information, if any.
- Section responses properly with Markdown headings, e.g., '# Recommended Actions', '## Cause of bug', '# Findings'.
- Use short display lists delimited by endlines, not inline lists. Always bold the title of every list item, e.g., '- **[title]**'.
- Never use unicode bullet points. Use the markdown list syntax to format lists.
</markdown>
</communication_style>
<tool_calling>
You have tools at your disposal to solve the coding task.
Follow these rules:
- If the USER's task is general or you already know the answer, respond without calling tools, which finalizes the conversation.
- If you state that you will use a tool, immediately call that tool as your next action.
- Always follow the tool call schema EXACTLY as specified and provide all necessary parameters.
- The conversation may reference tools that are no longer available.
- Some tools run asynchronously, so you may not see their output immediately. If you need to see the output of previous tool calls before continuing, simply stop making new tool calls.
- When exploring a new or unfamiliar area of the codebase, focus first on mapping the main entry points, core services, and where the authoritative logic for the task lives.
- As you read, build a concise mental model of data flow and responsibilities (what calls what, where state is stored/updated, and how errors are handled).
- Surface any key invariants, assumptions, or high-risk areas you discover that should shape how you implement changes.
- Identify likely call sites or consumers that must be updated if you change a central abstraction, and note any open questions to resolve before making invasive edits.
<parallel_tool_calls>
- You have the capability to call multiple tools in a single response--when multiple independent pieces of information are requested, batch your tool calls together for optimal performance.
- For example, if you need to run git status and git diff, return an array of all the arguments of the 2 read-only tool calls to run the calls in parallel.
- Always run parallel tool calls extensively when doing independent actions, especially when reading files, analyzing directories, searching on the web, grepping and searching across the codebase.
- Never perform dependent terminal commands or writes in parallel.
</parallel_tool_calls>
</tool_calling>
<making_code_changes>
When making code changes, NEVER output code to the USER, unless requested. Instead use one of the code edit tools to implement the change.
EXTREMELY IMPORTANT: Your generated code must be immediately runnable. To guarantee this, follow these instructions carefully:
- Add all necessary import statements, dependencies, and endpoints required to run the code.
- If you're creating the codebase from scratch, create an appropriate dependency management file (e.g. requirements.txt) with package versions and a helpful README.
- If you're building a web app from scratch, give it a beautiful and modern UI, imbued with best UX practices and use modern UI frameworks and libraries (e.g React for the web framework, Lucide for icons, TailwindCSS for styling, shadcn/ui for components, etc.).
- If you're making a very large edit (>300 lines), break it up into multiple smaller edits. Your max output tokens is 64000 tokens per generation, so each of your edits must stay below this limit.
- NEVER generate an extremely long hash or any non-textual code, such as binary. These are not helpful to the USER and are very expensive.
- IMPORTANT: When using any code edit tool, ALWAYS generate the filename argument first before any other arguments.
- Imports must always be at the top of the file. If you are making an edit, do not import libraries in your code block if it is not at the top of the file. Instead, make a second separate edit to add the imports. This is crucial since imports in the middle of a file is extremely poor code style.
</making_code_changes>
<task_management>
You have access to plan/update tools to help manage multi-step work and give the user visibility into progress.
- For medium or larger tasks (e.g. multi-file changes, new features, or multi-step investigations), create a lightweight plan before starting implementation.
- Plans should contain 2-5 outcome-oriented items (milestones), not micro-steps (e.g. avoid items like "open file" or "run tests").
- Maintain plan state accurately: only one item should be in_progress at a time; mark items complete as you finish them.
- Keep the plan up to date, but do not over-update it. Update status when meaningful progress is made or before starting a new phase.
- Finish with no remaining in_progress or pending items. Any unfinished work should be explicitly deferred or canceled with a brief reason.
- For very small or straightforward tasks (e.g. a single small edit), you may skip using the plan tool entirely.
</task_management>"#;
const TEMPLATE_SUFFIX: &str = r#"<memory_system>
You have access to a persistent memory database with three types of memories:
1. Global rules: System-wide rules that always apply
2. User-provided memories: Context explicitly provided by the USER for this task
3. System-retrieved memories: Automatically retrieved from previous conversations that may or may not be relevant

System-retrieved memories should be disregarded if they are not relevant to the USER's actual request. Only use them if they clearly apply to the current task.

You have the ability to add memories to preserve important information and context.
As soon as you encounter important information or context, proactively use the create_memory tool to save it to the database.
You DO NOT need USER permission to create a memory.
Pay attention to relevant memories, as they can provide valuable context to guide your behavior to solve the task.
Note that memories can be outdated.
</memory_system>"#;

pub struct ChatRequestParts<'a> {
    pub api_key: &'a str,
    pub jwt_token: &'a str,
    pub model: &'a str,
    pub messages: &'a [EngineMessage],
    pub tools: &'a [EngineTool],
    pub tool_choice: &'a EngineToolChoice,
    pub sampling_params: Option<&'a EngineSamplingParams>,
    pub conversation_id: &'a str,
    pub trajectory_run_id: &'a str,
    pub session_id: &'a str,
    pub step_number: u64,
    pub turn_flag: u64,
    pub system_prompt_mode: SystemPromptMode,
}

pub struct ModelPreflightParts<'a> {
    pub api_key: &'a str,
    pub jwt_token: &'a str,
    pub model: &'a str,
    pub include_jwt: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CascadeStep {
    pub step_type: u64,
    pub status: u64,
    pub text: String,
    pub response_text: String,
    pub modified_text: String,
    pub thinking: String,
    pub error_text: String,
    pub prompt_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RateLimitPreflight {
    pub has_capacity: bool,
    pub message: String,
    pub messages_remaining: i64,
    pub max_messages: i64,
    pub resets_in_seconds: i64,
}

#[derive(Debug, Clone)]
pub struct ChatCapacityPreflight {
    pub has_capacity: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemPromptMode {
    Passthrough,
    StripIdentity,
    WindsurfWrap,
}

impl SystemPromptMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passthrough => "passthrough",
            Self::StripIdentity => "strip-identity",
            Self::WindsurfWrap => "windsurf-wrap",
        }
    }
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
    let tool_blocks = parts
        .tools
        .iter()
        .filter_map(encode_tool_to_f10_block)
        .collect::<Vec<_>>();
    let tool_choice_block = if tool_blocks.is_empty() {
        Vec::new()
    } else {
        encode_tool_choice_to_f12_block(parts.tool_choice)
    };
    let keep_tool_runtime_flags = !tool_blocks.is_empty()
        && !tool_choice_block.is_empty()
        && !matches!(parts.tool_choice, EngineToolChoice::None);
    let sampling_patches = parts
        .sampling_params
        .map(build_f8_patch_map)
        .unwrap_or_default();
    let mut out = Vec::new();
    let mut wrote_messages = false;
    let mut wrote_model = false;
    let mut wrote_conversation = false;
    let mut wrote_trajectory = false;
    let mut wrote_session = false;
    let mut wrote_tools = false;
    let mut wrote_tool_choice = false;
    let (system, conversation) = split_messages(parts.messages, parts.system_prompt_mode);
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
            (7, 0) => out.extend(write_varint_field(7, CASCADE_F7_VALUE)),
            (8, 2) => {
                if sampling_patches.is_empty() {
                    out.extend(field.raw);
                } else {
                    let inner = patch_fields(&field.value, &sampling_patches)?;
                    out.extend(write_message_field(8, &inner));
                }
            }
            (9, 2) => out.extend(write_message_field(
                9,
                &strip_cascade_feature_flags(&field.value)?,
            )),
            (10, 2) => {
                if !wrote_tools {
                    for block in &tool_blocks {
                        out.extend(block);
                    }
                    wrote_tools = true;
                }
            }
            (12, 2) => {
                if !wrote_tool_choice && !tool_choice_block.is_empty() {
                    out.extend(tool_choice_block.clone());
                    wrote_tool_choice = true;
                }
            }
            (13, _) if !keep_tool_runtime_flags => {}
            (15, 2) => {
                out.extend(write_message_field(
                    15,
                    &[
                        write_string_field(1, parts.session_id),
                        write_varint_field(2, parts.step_number),
                        write_varint_field(3, 4),
                        write_varint_field(4, parts.turn_flag),
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
    if !wrote_tools {
        for block in &tool_blocks {
            out.extend(block);
        }
    }
    if !wrote_tool_choice && !tool_choice_block.is_empty() {
        out.extend(tool_choice_block);
    }
    if !wrote_session {
        out.extend(write_message_field(
            15,
            &[
                write_string_field(1, parts.session_id),
                write_varint_field(2, parts.step_number),
                write_varint_field(3, 4),
                write_varint_field(4, parts.turn_flag),
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

pub fn build_initialize_panel_state_request(
    api_key: &str,
    session_id: &str,
    trusted: bool,
) -> Vec<u8> {
    [
        write_message_field(1, &build_ls_metadata(api_key, session_id)),
        write_varint_field(3, u64::from(trusted)),
    ]
    .concat()
}

pub fn build_heartbeat_request(api_key: &str, session_id: &str) -> Vec<u8> {
    write_message_field(1, &build_ls_metadata(api_key, session_id))
}

pub fn build_add_tracked_workspace_request(workspace_path: &str) -> Vec<u8> {
    write_string_field(1, workspace_path)
}

pub fn build_update_workspace_trust_request(
    api_key: &str,
    session_id: &str,
    trusted: bool,
) -> Vec<u8> {
    [
        write_message_field(1, &build_ls_metadata(api_key, session_id)),
        write_varint_field(2, u64::from(trusted)),
    ]
    .concat()
}

pub fn build_start_cascade_request(api_key: &str, session_id: &str) -> Vec<u8> {
    [
        write_message_field(1, &build_ls_metadata(api_key, session_id)),
        write_varint_field(4, 1),
        write_varint_field(5, 1),
    ]
    .concat()
}

pub fn build_send_cascade_message_request(
    api_key: &str,
    cascade_id: &str,
    text: &str,
    model_uid: &str,
    session_id: &str,
    tool_preamble: Option<&str>,
) -> Vec<u8> {
    [
        write_string_field(1, cascade_id),
        write_message_field(2, &write_string_field(1, text)),
        write_message_field(3, &build_ls_metadata(api_key, session_id)),
        write_message_field(5, &build_cascade_config(model_uid, tool_preamble)),
    ]
    .concat()
}

pub fn build_get_trajectory_steps_request(cascade_id: &str, step_offset: u64) -> Vec<u8> {
    let mut out = write_string_field(1, cascade_id);
    if step_offset > 0 {
        out.extend(write_varint_field(2, step_offset));
    }
    out
}

pub fn build_get_trajectory_request(cascade_id: &str) -> Vec<u8> {
    write_string_field(1, cascade_id)
}

pub fn parse_start_cascade_response(payload: &[u8]) -> anyhow::Result<String> {
    Ok(first_field(&parse_fields(payload)?, 1, 2)
        .map(|field| String::from_utf8_lossy(&field.value).to_string())
        .unwrap_or_default())
}

pub fn parse_trajectory_status(payload: &[u8]) -> anyhow::Result<u64> {
    Ok(first_field(&parse_fields(payload)?, 2, 0)
        .and_then(|field| field.varint)
        .unwrap_or_default())
}

pub fn parse_trajectory_steps(payload: &[u8]) -> anyhow::Result<Vec<CascadeStep>> {
    let mut out = Vec::new();
    for step_field in all_fields(&parse_fields(payload)?, 1)
        .into_iter()
        .filter(|field| field.wire_type == 2)
    {
        let fields = parse_fields(&step_field.value)?;
        let mut step = CascadeStep {
            step_type: first_field(&fields, 1, 0)
                .and_then(|field| field.varint)
                .unwrap_or_default(),
            status: first_field(&fields, 4, 0)
                .and_then(|field| field.varint)
                .unwrap_or_default(),
            ..Default::default()
        };

        if let Some(metadata) = first_field(&fields, 5, 2) {
            apply_cascade_usage(&mut step, &metadata.value)?;
        }
        if let Some(planner) = first_field(&fields, 20, 2) {
            let planner_fields = parse_fields(&planner.value)?;
            step.response_text = first_field(&planner_fields, 1, 2)
                .map(|field| String::from_utf8_lossy(&field.value).to_string())
                .unwrap_or_default();
            step.thinking = first_field(&planner_fields, 3, 2)
                .map(|field| String::from_utf8_lossy(&field.value).to_string())
                .unwrap_or_default();
            step.modified_text = first_field(&planner_fields, 8, 2)
                .map(|field| String::from_utf8_lossy(&field.value).to_string())
                .unwrap_or_default();
            step.text = if step.modified_text.is_empty() {
                step.response_text.clone()
            } else {
                step.modified_text.clone()
            };
        }
        if let Some(error) = first_field(&fields, 24, 2) {
            step.error_text = parse_cascade_error_text(&error.value)?;
        }
        if step.error_text.is_empty() {
            if let Some(error) = first_field(&fields, 31, 2) {
                step.error_text = parse_error_details(&error.value)?;
            }
        }
        out.push(step);
    }
    Ok(out)
}

pub fn build_model_preflight_request(
    template: &[u8],
    parts: ModelPreflightParts<'_>,
) -> anyhow::Result<Vec<u8>> {
    let metadata = parse_fields(template)?
        .into_iter()
        .find(|field| field.number == 1 && field.wire_type == 2)
        .ok_or_else(|| anyhow::anyhow!("远端模板缺少 metadata"))?;
    Ok([
        write_message_field(
            1,
            &patch_preflight_metadata(
                &metadata.value,
                parts.api_key,
                parts.jwt_token,
                parts.include_jwt,
            )?,
        ),
        write_string_field(3, parts.model),
    ]
    .concat())
}

pub fn parse_chat_capacity_preflight(payload: &[u8]) -> anyhow::Result<ChatCapacityPreflight> {
    let has_capacity = parse_fields(payload)?.into_iter().any(|field| {
        field.number == 1 && field.wire_type == 0 && field.varint.is_some_and(|value| value != 0)
    });
    Ok(ChatCapacityPreflight { has_capacity })
}

pub fn parse_rate_limit_preflight(payload: &[u8]) -> anyhow::Result<RateLimitPreflight> {
    let mut data = RateLimitPreflight {
        has_capacity: false,
        message: String::new(),
        messages_remaining: 0,
        max_messages: 0,
        resets_in_seconds: 0,
    };
    for field in parse_fields(payload)? {
        match (field.number, field.wire_type) {
            (1, 0) => data.has_capacity = field.varint.is_some_and(|value| value != 0),
            (2, 2) => data.message = String::from_utf8_lossy(&field.value).to_string(),
            (3, 0) => data.messages_remaining = preflight_count(field.varint),
            (4, 0) => data.max_messages = preflight_count(field.varint),
            (5, 0) => data.resets_in_seconds = preflight_reset_seconds(field.varint),
            _ => {}
        }
    }
    Ok(data)
}

fn build_ls_metadata(api_key: &str, session_id: &str) -> Vec<u8> {
    [
        write_string_field(1, "windsurf"),
        write_string_field(2, "2.0.67"),
        write_string_field(3, api_key),
        write_string_field(4, "en"),
        write_string_field(5, ls_os_name()),
        write_string_field(7, "2.0.67"),
        write_string_field(8, ls_arch_name()),
        write_varint_field(9, rand::random::<u64>() & 0x0000_ffff_ffff_ffff),
        write_string_field(10, session_id),
        write_string_field(12, "windsurf"),
    ]
    .concat()
}

fn build_cascade_config(model_uid: &str, tool_preamble: Option<&str>) -> Vec<u8> {
    let mut conversational = Vec::new();
    conversational.extend(write_varint_field(4, 3));
    if let Some(tool_preamble) = tool_preamble.filter(|value| !value.trim().is_empty()) {
        conversational.extend(write_message_field(
            12,
            &[
                write_varint_field(1, 1),
                write_string_field(2, &format!("{tool_preamble}\n\n{}", tool_reinforcement())),
            ]
            .concat(),
        ));
        conversational.extend(write_message_field(
            13,
            &[
                write_varint_field(1, 1),
                write_string_field(
                    2,
                    "When tools are useful, emit tool calls exactly in the requested format and stop.",
                ),
            ]
            .concat(),
        ));
    } else {
        conversational.extend(write_message_field(
            10,
            &[
                write_varint_field(1, 1),
                write_string_field(2, "No tools are available."),
            ]
            .concat(),
        ));
    }

    let planner = [
        write_message_field(2, &conversational),
        write_string_field(35, model_uid),
        write_string_field(34, model_uid),
        write_varint_field(6, 32768),
    ]
    .concat();
    let memory_config = write_varint_field(1, 0);
    let brain_config = [
        write_varint_field(1, 1),
        write_message_field(6, &write_message_field(6, &[])),
    ]
    .concat();
    [
        write_message_field(1, &planner),
        write_message_field(5, &memory_config),
        write_message_field(7, &brain_config),
    ]
    .concat()
}

fn apply_cascade_usage(step: &mut CascadeStep, payload: &[u8]) -> anyhow::Result<()> {
    let metadata = parse_fields(payload)?;
    let Some(usage) = first_field(&metadata, 9, 2) else {
        return Ok(());
    };
    let fields = parse_fields(&usage.value)?;
    step.prompt_tokens = first_field(&fields, 2, 0).and_then(|field| field.varint);
    step.completion_tokens = first_field(&fields, 3, 0).and_then(|field| field.varint);
    step.cached_input_tokens = first_field(&fields, 5, 0).and_then(|field| field.varint);
    Ok(())
}

fn parse_cascade_error_text(payload: &[u8]) -> anyhow::Result<String> {
    let fields = parse_fields(payload)?;
    if let Some(details) = first_field(&fields, 3, 2) {
        parse_error_details(&details.value)
    } else {
        Ok(String::new())
    }
}

fn parse_error_details(payload: &[u8]) -> anyhow::Result<String> {
    let fields = parse_fields(payload)?;
    for number in [1, 2, 3] {
        if let Some(field) = first_field(&fields, number, 2) {
            let value = String::from_utf8_lossy(&field.value).trim().to_string();
            if !value.is_empty() {
                return Ok(value
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .chars()
                    .take(300)
                    .collect());
            }
        }
    }
    Ok(String::new())
}

fn first_field<'a>(fields: &'a [Field], number: u64, wire_type: u8) -> Option<&'a Field> {
    fields
        .iter()
        .find(|field| field.number == number && field.wire_type == wire_type)
}

fn all_fields(fields: &[Field], number: u64) -> Vec<&Field> {
    fields
        .iter()
        .filter(|field| field.number == number)
        .collect()
}

fn tool_reinforcement() -> &'static str {
    "Tools are real. Never describe that you will call a tool. Emit the tool call block directly, then stop. Never fabricate tool results."
}

fn ls_os_name() -> &'static str {
    match std::env::consts::OS {
        "macos" => "macos",
        "windows" => "windows",
        _ => "linux",
    }
}

fn ls_arch_name() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        _ => "x86_64",
    }
}

pub fn parse_chat_frame(payload: &[u8]) -> anyhow::Result<ParsedChatFrame> {
    let fields = parse_fields(payload)?;
    let mut out = ParsedChatFrame::default();
    for field in fields {
        match (field.number, field.wire_type) {
            (3, 2) => out.text = String::from_utf8_lossy(&field.value).to_string(),
            (5, 0) => out.stop_reason = field.varint.map(stop_reason_name),
            (6, 2) => apply_tool_call_block(&mut out, &field.value)?,
            (7, 2) => apply_usage_block(&mut out, &field.value)?,
            (9, 2) => out.reasoning = String::from_utf8_lossy(&field.value).to_string(),
            (10, 2) => out.reasoning_signature = String::from_utf8_lossy(&field.value).to_string(),
            (17, 2) => {
                out.conversation_id = Some(String::from_utf8_lossy(&field.value).to_string())
            }
            _ => {}
        }
    }
    Ok(out)
}

fn encode_tool_to_f10_block(tool: &EngineTool) -> Option<Vec<u8>> {
    if tool.name.trim().is_empty() {
        return None;
    }
    let description = tool.description.clone().unwrap_or_default();
    let params_json = tool
        .parameters
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_else(|| "{}".to_string());
    let mut parts = vec![
        write_string_field(F10_TOOL_NAME, &tool.name),
        write_string_field(F10_TOOL_DESC, &description),
        write_string_field(F10_TOOL_PARAMS_JSON, &params_json),
    ];
    if let Some(stream_arg_name) = pick_large_text_tool_param_name(tool.parameters.as_ref()) {
        parts.push(write_string_field(
            F10_TOOL_STREAM_ARG_NAME,
            stream_arg_name,
        ));
    }
    let inner = parts.concat();
    Some(write_message_field(10, &inner))
}

fn encode_tool_choice_to_f12_block(tool_choice: &EngineToolChoice) -> Vec<u8> {
    let value = match tool_choice {
        EngineToolChoice::Auto => "auto".to_string(),
        EngineToolChoice::None => "none".to_string(),
        EngineToolChoice::Required => "required".to_string(),
        EngineToolChoice::Function { name } => name.clone(),
    };
    let inner = write_string_field(F12_TOOL_CHOICE, &value);
    write_message_field(12, &inner)
}

fn pick_large_text_tool_param_name(parameters: Option<&serde_json::Value>) -> Option<&'static str> {
    let properties = parameters
        .and_then(|value| value.get("properties"))
        .and_then(serde_json::Value::as_object)?;
    LARGE_TEXT_TOOL_PARAM_NAMES
        .iter()
        .copied()
        .find(|name| properties.contains_key(*name))
}

fn build_f8_patch_map(params: &EngineSamplingParams) -> std::collections::HashMap<u64, Vec<u8>> {
    let mut patches = std::collections::HashMap::new();
    if let Some(value) = params.max_tokens {
        patches.insert(F8_MAX_TOKENS, write_varint_field(F8_MAX_TOKENS, value));
    }
    if let Some(value) = params.max_tool_calls {
        patches.insert(
            F8_MAX_TOOL_CALLS,
            write_varint_field(F8_MAX_TOOL_CALLS, value),
        );
    }
    if let Some(value) = params.temperature {
        patches.insert(F8_TEMPERATURE, write_fixed64_field(F8_TEMPERATURE, value));
    }
    if let Some(value) = params.top_p {
        patches.insert(F8_TOP_P, write_fixed64_field(F8_TOP_P, value));
    }
    if let Some(value) = params.top_k {
        patches.insert(F8_TOP_K, write_varint_field(F8_TOP_K, value));
    }
    if let Some(value) = params.frequency_penalty {
        patches.insert(
            F8_FREQUENCY_PENALTY,
            write_fixed64_field(F8_FREQUENCY_PENALTY, value),
        );
    }
    if let Some(value) = params.presence_penalty {
        patches.insert(
            F8_PRESENCE_PENALTY,
            write_fixed64_field(F8_PRESENCE_PENALTY, value),
        );
    }
    patches
}

fn patch_fields(
    raw: &[u8],
    patches: &std::collections::HashMap<u64, Vec<u8>>,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut remaining = patches
        .keys()
        .copied()
        .collect::<std::collections::HashSet<_>>();
    for field in parse_fields(raw)? {
        if let Some(replacement) = patches.get(&field.number) {
            out.extend(replacement.clone());
            remaining.remove(&field.number);
        } else {
            out.extend(field.raw);
        }
    }
    let mut left = remaining.into_iter().collect::<Vec<_>>();
    left.sort_unstable();
    for field_number in left {
        if let Some(replacement) = patches.get(&field_number) {
            out.extend(replacement.clone());
        }
    }
    Ok(out)
}

fn strip_cascade_feature_flags(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    for field in parse_fields(raw)? {
        if field.number == 6 && field.wire_type == 2 {
            if cascade_feature_flag_name(&field.value)?
                .as_deref()
                .is_some_and(is_allowed_cascade_feature_flag)
            {
                out.extend(field.raw);
            }
            continue;
        }
        out.extend(field.raw);
    }
    Ok(out)
}

fn cascade_feature_flag_name(raw: &[u8]) -> anyhow::Result<Option<String>> {
    for field in parse_fields(raw)? {
        if field.number == 5 && field.wire_type == 2 {
            return Ok(Some(String::from_utf8_lossy(&field.value).to_string()));
        }
    }
    Ok(None)
}

fn is_allowed_cascade_feature_flag(name: &str) -> bool {
    matches!(
        name,
        "CASCADE_PLAN_BASED_CONFIG_OVERRIDE"
            | "COLLAPSE_ASSISTANT_MESSAGES"
            | "CASCADE_USER_MEMORIES_IN_SYS_PROMPT"
            | "cascade-brain-config"
            | "cascade-api-server-experiment-keys"
            | "CASCADE_VIEW_FILE_TOOL_CONFIG_OVERRIDE"
            | "SNAPSHOT_TO_STEP_OPTIONS_OVERRIDE"
            | "CASCADE_MEMORY_CONFIG_OVERRIDE"
            | "CASCADE_AUTO_FIX_LINTS"
            | "CASCADE_GLOBAL_CONFIG_OVERRIDE"
            | "CASCADE_USE_REPLACE_CONTENT_EDIT_TOOL"
            | "gemini-xml-tool-fixes"
            | "use-responses-api"
            | "ENABLE_SUGGESTED_RESPONSES"
            | "API_SERVER_CLIENT_USE_HTTP_2"
            | "cascade-command-status-tool-config-override"
            | "cascade-view-code-item-tool-config-override"
            | "cascade-communication-section-content"
            | "cascade-code-changes-section-content"
            | "cascade-code-research-section-content"
            | "cascade-additional-instructions-section-content"
            | "cascade-tool-calling-section-content"
    )
}

fn apply_tool_call_block(out: &mut ParsedChatFrame, payload: &[u8]) -> anyhow::Result<()> {
    for field in parse_fields(payload)? {
        match (field.number, field.wire_type) {
            (1, 2) => {
                let value = String::from_utf8_lossy(&field.value).to_string();
                if !value.is_empty() {
                    out.tool_call_id = Some(value);
                }
            }
            (2, 2) => {
                let value = String::from_utf8_lossy(&field.value).to_string();
                if !value.is_empty() {
                    out.tool_call_name = Some(value);
                }
            }
            (3, 2) => out.tool_call_args = Some(String::from_utf8_lossy(&field.value).to_string()),
            _ => {}
        }
    }
    Ok(())
}

fn apply_usage_block(out: &mut ParsedChatFrame, payload: &[u8]) -> anyhow::Result<()> {
    for field in parse_fields(payload)? {
        match (field.number, field.wire_type) {
            (3, 0) => out.completion_tokens = field.varint,
            (4, 0) => out.prompt_tokens = field.varint,
            (5, 0) => out.cached_input_tokens = field.varint,
            (6, 0) if out.stop_reason.is_none() => {
                out.stop_reason = field.varint.map(stop_reason_name)
            }
            _ => {}
        }
    }
    Ok(())
}

fn build_message_block(message: &EngineMessage, current_turn: bool) -> Vec<u8> {
    let role = match message.role.as_str() {
        "assistant" => 2,
        "tool" => 4,
        _ => 1,
    };
    let mut inner = Vec::new();
    if role == 2 {
        inner.extend(write_string_field(
            F3_MSG_BOT_ID,
            &format!("bot-{}", Uuid::new_v4()),
        ));
    }
    inner.extend(write_varint_field(F3_MSG_ROLE, role));
    inner.extend(write_string_field(F3_MSG_CONTENT, &message.content));
    inner.extend(write_varint_field(
        F3_MSG_TOKEN_COUNT,
        estimate_message_tokens(message),
    ));
    if role == 1 && !message.ephemeral {
        inner.extend(write_varint_field(F3_MSG_USER_FLAG, 1));
    }
    if role == 2 {
        for tool_call in &message.tool_calls {
            if tool_call.id.trim().is_empty() || tool_call.name.trim().is_empty() {
                continue;
            }
            let tool_inner = [
                write_string_field(F3_MSG_TOOL_CALL_ID, &tool_call.id),
                write_string_field(2, &tool_call.name),
                write_string_field(3, &tool_call.arguments),
            ]
            .concat();
            inner.extend(write_message_field(F3_MSG_TOOL_CALL, &tool_inner));
        }
    }
    if role == 4 {
        if let Some(tool_call_id) = message
            .tool_call_id
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            inner.extend(write_string_field(F3_MSG_TOOL_CALL_ID, tool_call_id));
        }
    }
    if current_turn && (role == 1 || role == 4) {
        inner.extend(write_bytes_field(F3_MSG_CURRENT_TURN, CURRENT_TURN_MARKER));
    }
    if role == 2 {
        if let Some(reasoning) = message
            .reasoning_content
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            inner.extend(write_string_field(F3_MSG_REASONING, reasoning));
        }
    }
    write_message_field(3, &inner)
}

fn estimate_message_tokens(message: &EngineMessage) -> u64 {
    let mut total = estimate_tokens(&message.content);
    if let Some(reasoning) = message.reasoning_content.as_deref() {
        total += estimate_tokens(reasoning);
    }
    for tool_call in &message.tool_calls {
        total += estimate_tokens(&tool_call.id);
        total += estimate_tokens(&tool_call.name);
        total += estimate_tokens(&tool_call.arguments);
    }
    if let Some(tool_call_id) = message.tool_call_id.as_deref() {
        total += estimate_tokens(tool_call_id);
    }
    total.max(1)
}

fn write_fixed64_field(field: u64, value: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.extend(encode_varint((field << 3) | 1));
    out.extend(value.to_le_bytes());
    out
}
fn split_messages(
    messages: &[EngineMessage],
    system_prompt_mode: SystemPromptMode,
) -> (String, Vec<EngineMessage>) {
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
    system = match system_prompt_mode {
        SystemPromptMode::Passthrough => system,
        SystemPromptMode::StripIdentity => strip_cascade_identity(&system),
        SystemPromptMode::WindsurfWrap => wrap_system_prompt(&system),
    };
    if conversation.is_empty() {
        conversation.push(EngineMessage {
            role: "user".to_string(),
            content: "Hello".to_string(),
            ..Default::default()
        });
    }
    if system_prompt_mode == SystemPromptMode::WindsurfWrap {
        conversation.insert(
            0,
            EngineMessage {
                role: "user".to_string(),
                content: WINDSURF_WRAP_MEMORY_MESSAGE.to_string(),
                ephemeral: true,
                ..Default::default()
            },
        );
        for message in &mut conversation {
            if message.role != "user" || message.ephemeral || message.content.is_empty() {
                continue;
            }
            if message.content.contains("<user_request>") {
                continue;
            }
            message.content = wrap_user_request(&message.content);
        }
    }
    (system, conversation)
}

fn wrap_system_prompt(client_content: &str) -> String {
    let cleaned = strip_cascade_identity(client_content);
    if is_already_wrapped(&cleaned) {
        return cleaned;
    }
    format!("{TEMPLATE_PREFIX}\n{TEMPLATE_SUFFIX}\n<user_rules>\n{cleaned}\n</user_rules>")
}

fn is_already_wrapped(content: &str) -> bool {
    content.contains("<communication_style>") || content.contains("<tool_calling>")
}

fn strip_cascade_identity(text: &str) -> String {
    text.lines()
        .filter(|line| {
            let lower = line.to_ascii_lowercase();
            !(lower.contains("respond with `cascade`")
                || lower.contains("respond with 'cascade'")
                || lower.contains("respond with cascade"))
        })
        .map(|line| {
            line.replace("You are Cascade,", "You are ")
                .replace("You are Cascade", "You are an AI coding assistant")
                .replace("you are Cascade", "you are an AI coding assistant")
                .replace("Cascade, a powerful", "a powerful")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn wrap_user_request(content: &str) -> String {
    let now = chrono::Local::now().format("%b %-d, %Y, %-I:%M %p %Z");
    format!(
        "<additional_metadata>\nNOTE: Open files and cursor position may not be related to the user's current request. Always verify relevance before assuming connection.\n\nThe USER presented this request to you on {now}.\n</additional_metadata>\n<user_request>\n{content}\n</user_request>"
    )
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

fn patch_preflight_metadata(
    metadata: &[u8],
    api_key: &str,
    jwt_token: &str,
    include_jwt: bool,
) -> anyhow::Result<Vec<u8>> {
    let jwt_payload = jwt_payload(jwt_token).unwrap_or(serde_json::Value::Null);
    let user_id = jwt_payload
        .get("api_key")
        .and_then(serde_json::Value::as_str)
        .and_then(extract_user_id)
        .or_else(|| {
            jwt_payload
                .get("auth_uid")
                .and_then(serde_json::Value::as_str)
                .and_then(extract_user_id)
        })
        .or_else(|| extract_user_id(api_key));
    let team_id = jwt_payload
        .get("team_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty());
    let mut out = Vec::new();
    let mut wrote_api_key = false;
    let mut wrote_jwt = false;
    let mut wrote_user_id = false;
    let mut wrote_team_id = false;
    for field in parse_fields(metadata)? {
        match (field.number, field.wire_type) {
            (3, 2) => {
                out.extend(write_string_field(3, api_key));
                wrote_api_key = true;
            }
            (20, 2) => {
                if let Some(user_id) = user_id {
                    out.extend(write_string_field(20, user_id));
                    wrote_user_id = true;
                }
            }
            (21, 2) => {
                if include_jwt {
                    out.extend(write_string_field(21, jwt_token));
                    wrote_jwt = true;
                }
            }
            (32, 2) => {
                if let Some(team_id) = team_id {
                    out.extend(write_string_field(32, team_id));
                    wrote_team_id = true;
                }
            }
            (9 | 10 | 16 | 17 | 24 | 25 | 26 | 27 | 30 | 31, _) => {}
            _ => out.extend(field.raw),
        }
    }
    if !wrote_api_key {
        out.extend(write_string_field(3, api_key));
    }
    if include_jwt && !wrote_jwt {
        out.extend(write_string_field(21, jwt_token));
    }
    if let Some(user_id) = user_id.filter(|_| !wrote_user_id) {
        out.extend(write_string_field(20, user_id));
    }
    if let Some(team_id) = team_id.filter(|_| !wrote_team_id) {
        out.extend(write_string_field(32, team_id));
    }
    out.extend(write_bytes_field(30, &[0, 1, 3]));
    Ok(out)
}

fn jwt_payload(jwt_token: &str) -> Option<serde_json::Value> {
    let payload = jwt_token.split('.').nth(1)?;
    let bytes = base64_url_decode(payload)?;
    serde_json::from_slice::<serde_json::Value>(&bytes).ok()
}

fn extract_user_id(value: &str) -> Option<&str> {
    value
        .split('$')
        .find(|part| part.starts_with("user-") && !part.is_empty())
}

fn preflight_count(value: Option<u64>) -> i64 {
    const UNLIMITED_SENTINEL: u64 = u64::MAX;
    match value {
        Some(value) if value >= UNLIMITED_SENTINEL => -1,
        Some(value) => value.min(i64::MAX as u64) as i64,
        None => 0,
    }
}

fn preflight_reset_seconds(value: Option<u64>) -> i64 {
    preflight_count(value).max(0)
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
