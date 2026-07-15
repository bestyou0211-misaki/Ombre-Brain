"""
========================================
import_memory.py — 历史对话导入引擎
========================================

把各平台导出的历史对话（Claude JSON / ChatGPT / DeepSeek / Markdown / 纯文本）
切块、过LLM 打标、写入记忆系统。

关键行为：
- 自动识别格式，分块处理，单 chunk 独立成桶
- 导入进度持久化到 import_state.json，可断点续传
- raw 模式：保留原文不脱水，给特殊场景用
- 导入完成后扫一遍频次模式（同一主题反复出现 → 提示她/他 pin）

不做什么（边界）：
- 不在线接收对话流（只处理离线导出文件）
- 不写桶文件本身（委托给 BucketManager）
- 不调用 dehydrator.merge（只新建，不合并）

对外暴露：ImportEngine 类（被 server.py 注入到 _runtime，由 dashboard API 触发）
========================================
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from utils import atomic_write_text, clean_llm_json, count_tokens_approx, now_iso, parse_bool

logger = logging.getLogger("ombre_brain.import")


# ============================================================
# 调参面板 / Tunable constants
# ------------------------------------------------------------
# rule.md §①：禁裸魔法数字。导入流水线上下参数集中定义在这里。
# ============================================================

# --- chunk_turns：对话轮次分窗 ---
_CHUNK_TARGET_TOKENS = 10000   # 单个 chunk 目标 token 数
_CHUNK_OVERSIZE_RATIO = 1.5    # 单轮 × 该倍数 → 单独成 chunk（避免超范围）

# --- ImportState ---
_STATE_HASH_HEX = 16           # source_hash 取 sha256 前 16 hex
_STATE_ERR_LOG_MAX = 100       # errors 数组最多保留条数（避免状态文件肨胀）
_CHUNK_ERR_PREVIEW = 200       # 单 chunk 错误信息截断长度

# --- _extract_memories LLM 调用 ---
# chunk_turns() 已经把块的大小控制在 ~_CHUNK_TARGET_TOKENS token 附近，只有单轮
# 超大文本才会摸到 _CHUNK_TARGET_TOKENS × _CHUNK_OVERSIZE_RATIO 这个上限（见
# chunk_turns 里「单轮超限单独成块」的分支）。这里按 token 数而不是固定字符数
# 判断要不要截断——旧的固定 12000 字符对英文/中英混合内容而言远小于块本身的
# token 预算，会把块后半段正文在不留任何痕迹的情况下悄悄丢给 LLM 看不到。
_EXTRACT_TOKEN_CEILING = int(_CHUNK_TARGET_TOKENS * _CHUNK_OVERSIZE_RATIO)
_EXTRACT_MAX_TOKENS = 2048
_EXTRACT_TEMPERATURE = 0.0     # 提取需确定性
_PARSE_ERR_PREVIEW = 200       # JSON 解析失败时日志预览

# --- 默认情感坐标与 importance（与 dehydrator 保持一致）---
_DEFAULT_VALENCE = 0.5
_DEFAULT_AROUSAL = 0.3
_DEFAULT_IMPORTANCE = 5
_IMPORTANCE_MIN = 1
_IMPORTANCE_MAX = 10

# --- 输出截断长度 ---
_NAME_MAX_CHARS = 20
_DOMAIN_MAX = 3
_TAGS_MAX = 10                 # extraction 试在 10 个以内（与 dehydrator 的 15 不同，导入场景信息密度较低）

# --- merge_or_create 默认阈值 ---
_DEFAULT_MERGE_THRESHOLD = 75
_IMPORT_RELATION_THRESHOLD = 45.0

# --- detect_patterns：embedding 聚类 ---
_PATTERN_MIN_DYNAMIC_BUCKETS = 5  # 动态桶少于该数 → 不作处理
_PATTERN_SIMILARITY_THRESHOLD = 0.7  # 两桶向量余弦 > 该值 → 归同一类
_PATTERN_MIN_CLUSTER_SIZE = 3     # 类内成员 ≥ 该数才认为是“高频模式”
_PATTERN_PIN_SUGGEST_THRESHOLD = 5  # 成员 ≥ 该数 → 建议 pin，否则仅 review
_PATTERN_RESULT_LIMIT = 20        # 返回给 dashboard 的 pattern 上限
_PATTERN_CONTENT_PREVIEW = 200    # pattern_content 预览长度


def _clamp_va(meta: dict) -> tuple[float, float]:
    """将 meta 中的 valence / arousal 钳制到 [0, 1]。

    与 dehydrator._clamp_va 同表现，这里单独复制一份是为了避免
    import_memory 反向依赖 dehydrator 的私有方法。两者默认值一致（
    rule.md §1.0 哲学：中性 V=0.5 / 低唤醒 A=0.3）。
    """
    try:
        v = max(0.0, min(1.0, float(meta.get("valence", _DEFAULT_VALENCE))))
        a = max(0.0, min(1.0, float(meta.get("arousal", _DEFAULT_AROUSAL))))
        return v, a
    except (ValueError, TypeError):
        return _DEFAULT_VALENCE, _DEFAULT_AROUSAL


def _clamp_importance(meta: dict) -> int:
    """将 meta.importance 钳制到 [1, 10]。解析失败返回默认 5。"""
    try:
        return max(
            _IMPORTANCE_MIN,
            min(_IMPORTANCE_MAX, int(meta.get("importance", _DEFAULT_IMPORTANCE))),
        )
    except (ValueError, TypeError):
        return _DEFAULT_IMPORTANCE


def _strip_md_fence(raw: str) -> str:
    """Backwards-compatible wrapper for tolerant LLM JSON extraction."""
    return clean_llm_json(raw)


# ============================================================
# Format Parsers — normalize any format to conversation turns
# 格式解析器 — 将任意格式标准化为对话轮次
# ============================================================

def _normalize_role(role: Any) -> str:
    value = str(role or "user").strip().lower()
    if value in ("user", "human"):
        return "user"
    if value in ("assistant", "ai", "bot", "claude", "gpt", "deepseek"):
        return "assistant"
    if value in ("tool", "function"):
        return "tool"
    if value in ("system", "developer"):
        return value
    return value or "user"


def _channel_for_role(role: str) -> str:
    normalized = _normalize_role(role)
    if normalized == "user":
        return "user_visible"
    if normalized == "assistant":
        return "assistant_visible"
    if normalized == "tool":
        return "tool_result"
    return "injected_context"


def _extract_text_content(value: Any) -> str:
    """Extract visible text while leaving tool-use payloads out of spoken content."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        parts = [_extract_text_content(item) for item in value]
        return "\n".join(part for part in parts if part.strip())
    if not isinstance(value, dict):
        return str(value)
    value_type = str(value.get("type") or value.get("content_type") or "").lower()
    if value_type in ("tool_use", "tool_call", "function_call"):
        return ""
    if "parts" in value:
        return _extract_text_content(value.get("parts"))
    for key in ("text", "content", "value"):
        candidate = value.get(key)
        if isinstance(candidate, (str, list, dict)):
            text = _extract_text_content(candidate)
            if text.strip():
                return text
    return ""


def _make_turn(
    *,
    role: Any,
    content: Any,
    timestamp: Any = "",
    platform: str,
    conversation_id: Any = "",
    message_id: Any = "",
    channel: str = "",
) -> dict | None:
    text = _extract_text_content(content).strip()
    if not text:
        return None
    normalized_role = _normalize_role(role)
    return {
        "role": normalized_role,
        "content": text,
        "timestamp": "" if timestamp is None else str(timestamp),
        "platform": str(platform or "unknown"),
        "conversation_id": "" if conversation_id is None else str(conversation_id),
        "message_id": "" if message_id is None else str(message_id),
        "channel": channel or _channel_for_role(normalized_role),
    }


def _parse_claude_json(data: dict | list) -> list[dict]:
    """Parse Claude.ai export JSON with speaker/channel/source evidence."""
    turns: list[dict] = []
    conversations = data if isinstance(data, list) else [data]
    for conv in conversations:
        if not isinstance(conv, dict):
            continue
        conversation_id = (
            conv.get("uuid")
            or conv.get("id")
            or conv.get("conversation_id")
            or conv.get("name")
            or ""
        )
        messages = conv.get("chat_messages", conv.get("messages", []))
        for index, msg in enumerate(messages):
            if not isinstance(msg, dict):
                continue
            role = msg.get("sender", msg.get("role", "user"))
            content = msg.get("text", msg.get("content", ""))
            turn = _make_turn(
                role=role,
                content=content,
                timestamp=msg.get("created_at", msg.get("timestamp", "")),
                platform="claude",
                conversation_id=conversation_id,
                message_id=msg.get("uuid") or msg.get("id") or index,
            )
            if turn:
                turns.append(turn)
    return turns


def _parse_chatgpt_json(data: list | dict) -> list[dict]:
    """Parse ChatGPT export JSON with visible/tool/injected channels separated."""
    turns: list[dict] = []
    conversations = data if isinstance(data, list) else [data]
    for conv in conversations:
        if not isinstance(conv, dict):
            continue
        conversation_id = (
            conv.get("id")
            or conv.get("conversation_id")
            or conv.get("title")
            or ""
        )
        mapping = conv.get("mapping", {})
        if mapping:
            valid_nodes = [
                (node_id, node)
                for node_id, node in mapping.items()
                if isinstance(node, dict)
            ]

            def _node_ts(pair):
                msg = pair[1].get("message")
                return msg.get("create_time") or 0 if isinstance(msg, dict) else 0

            for node_id, node in sorted(valid_nodes, key=_node_ts):
                msg = node.get("message")
                if not isinstance(msg, dict):
                    continue
                author = msg.get("author") or {}
                role = author.get("role", "user") if isinstance(author, dict) else "user"
                timestamp = msg.get("create_time", "")
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp).isoformat()
                turn = _make_turn(
                    role=role,
                    content=msg.get("content", {}),
                    timestamp=timestamp,
                    platform="chatgpt",
                    conversation_id=conversation_id,
                    message_id=msg.get("id") or node_id,
                )
                if turn:
                    turns.append(turn)
        else:
            for index, msg in enumerate(conv.get("messages", [])):
                if not isinstance(msg, dict):
                    continue
                role = msg.get("role") or (msg.get("author") or {}).get("role", "user")
                timestamp = msg.get("timestamp", msg.get("create_time", ""))
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp).isoformat()
                turn = _make_turn(
                    role=role,
                    content=msg.get("content", msg.get("text", "")),
                    timestamp=timestamp,
                    platform="chatgpt",
                    conversation_id=conversation_id,
                    message_id=msg.get("id") or index,
                )
                if turn:
                    turns.append(turn)
    return turns


def _parse_markdown(text: str) -> list[dict]:
    """Parse Markdown/plain text into evidence-aware turns."""
    lines = text.split("\n")
    turns: list[dict] = []
    current_role = "user"
    current_content: list[str] = []
    message_index = 0

    def flush() -> None:
        nonlocal current_content, message_index
        content = "\n".join(current_content).strip()
        if content:
            turn = _make_turn(
                role=current_role,
                content=content,
                platform="text",
                message_id=message_index,
            )
            if turn:
                turns.append(turn)
                message_index += 1
        current_content = []

    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if lowered.startswith(("human:", "user:", "你:", "我:")):
            flush()
            current_role = "user"
            content_after = stripped.split(":", 1)[1].strip() if ":" in stripped else ""
            current_content = [content_after] if content_after else []
        elif lowered.startswith(("assistant:", "claude:", "ai:", "gpt:", "bot:", "deepseek:")):
            flush()
            current_role = "assistant"
            content_after = stripped.split(":", 1)[1].strip() if ":" in stripped else ""
            current_content = [content_after] if content_after else []
        else:
            current_content.append(line)
    flush()

    if not turns and text.strip():
        turn = _make_turn(role="user", content=text, platform="text", message_id=0)
        if turn:
            turns = [turn]
    return turns


def detect_and_parse(raw_content: str, filename: str = "") -> list[dict]:
    """
    Auto-detect format and parse to normalized turns.
    自动检测格式并解析为标准化的对话轮次。
    """
    ext = Path(filename).suffix.lower() if filename else ""

    # Try JSON first
    if ext in (".json", "") or raw_content.strip().startswith(("{", "[")):
        try:
            data = json.loads(raw_content)
            # Detect Claude vs ChatGPT format
            if isinstance(data, list):
                sample = data[0] if data else {}
            else:
                sample = data

            if isinstance(sample, dict):
                if "chat_messages" in sample:
                    return _parse_claude_json(data)
                if "mapping" in sample:
                    return _parse_chatgpt_json(data)
                if "messages" in sample:
                    # Could be either — try ChatGPT first, fall back to Claude
                    msgs = sample["messages"]
                    if msgs and isinstance(msgs[0], dict) and "content" in msgs[0]:
                        if isinstance(msgs[0]["content"], dict):
                            return _parse_chatgpt_json(data)
                    return _parse_claude_json(data)
                # Single conversation object with role/content messages
                if "role" in sample and "content" in sample:
                    return _parse_claude_json(data)
        except (json.JSONDecodeError, KeyError, IndexError, AttributeError, TypeError):
            pass

    # Fall back to markdown/text
    return _parse_markdown(raw_content)


# ============================================================
# Chunking — split turns into ~10k token windows
# 分窗 — 按对话轮次边界切为 ~10k token 窗口
# ============================================================

def chunk_turns(
    turns: list[dict],
    target_tokens: int = _CHUNK_TARGET_TOKENS,
    human_label: str = "用户",
) -> list[dict]:
    """Group turns into chunks while preserving channel and source evidence."""
    chunks: list[dict] = []
    current_lines: list[str] = []
    current_tokens = 0
    first_ts = ""
    last_ts = ""
    turn_count = 0
    channels: list[str] = []
    message_ids: list[str] = []
    conversation_ids: list[str] = []
    platforms: list[str] = []

    def append_unique(target: list[str], value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in target:
            target.append(text)

    def reset_meta() -> None:
        nonlocal first_ts, last_ts, turn_count, channels, message_ids, conversation_ids, platforms
        first_ts = ""
        last_ts = ""
        turn_count = 0
        channels = []
        message_ids = []
        conversation_ids = []
        platforms = []

    def emit() -> None:
        nonlocal current_lines, current_tokens
        if not current_lines:
            return
        chunks.append({
            "content": "\n".join(current_lines),
            "timestamp_start": first_ts,
            "timestamp_end": last_ts,
            "turn_count": turn_count,
            "channels": list(channels),
            "message_ids": list(message_ids),
            "conversation_ids": list(conversation_ids),
            "platforms": list(platforms),
        })
        current_lines = []
        current_tokens = 0
        reset_meta()

    for turn in turns:
        channel = str(turn.get("channel") or _channel_for_role(turn.get("role", "user")))
        if channel == "user_visible":
            role_label = human_label
        elif channel == "assistant_visible":
            role_label = "AI"
        elif channel == "tool_result":
            role_label = "工具结果"
        else:
            role_label = "系统注入"
        marker_parts = [role_label, channel]
        if turn.get("message_id"):
            marker_parts.append(f"msg:{turn['message_id']}")
        if turn.get("conversation_id"):
            marker_parts.append(f"conv:{turn['conversation_id']}")
        evidence = "|".join(marker_parts[1:])
        line = f"[{role_label}] [{evidence}] {turn['content']}"
        line_tokens = count_tokens_approx(line)

        if line_tokens > target_tokens * _CHUNK_OVERSIZE_RATIO:
            emit()
            timestamp = str(turn.get("timestamp") or "")
            chunks.append({
                "content": line,
                "timestamp_start": timestamp,
                "timestamp_end": timestamp,
                "turn_count": 1,
                "channels": [channel],
                "message_ids": [str(turn.get("message_id"))] if turn.get("message_id") else [],
                "conversation_ids": [str(turn.get("conversation_id"))] if turn.get("conversation_id") else [],
                "platforms": [str(turn.get("platform"))] if turn.get("platform") else [],
            })
            continue

        if current_tokens + line_tokens > target_tokens and current_lines:
            emit()

        timestamp = str(turn.get("timestamp") or "")
        if timestamp and not first_ts:
            first_ts = timestamp
        if timestamp:
            last_ts = timestamp
        append_unique(channels, channel)
        append_unique(message_ids, turn.get("message_id"))
        append_unique(conversation_ids, turn.get("conversation_id"))
        append_unique(platforms, turn.get("platform"))
        current_lines.append(line)
        current_tokens += line_tokens
        turn_count += 1

    emit()
    return chunks


def _detect_preview_format(raw_content: str, filename: str, warnings: list[str]) -> str:
    ext = Path(filename).suffix.lower() if filename else ""
    stripped = raw_content.strip()

    if ext == ".md":
        return "markdown"
    if ext in (".txt", ".jsonl"):
        return "text"

    if ext == ".json" or stripped.startswith(("{", "[")):
        try:
            data = json.loads(stripped)
            sample = data[0] if isinstance(data, list) and data else data
            if isinstance(sample, dict):
                if "chat_messages" in sample:
                    return "claude_json"
                if "mapping" in sample:
                    return "chatgpt_json"
                if "messages" in sample:
                    return "chat_json"
                if "role" in sample and "content" in sample:
                    return "chat_json"
            return "json"
        except (json.JSONDecodeError, TypeError, IndexError):
            warnings.append("JSON 解析失败，已按纯文本继续预检")
            return "text"

    return "markdown" if "\n" in raw_content else "text"


def preview_import(raw_content: str, filename: str = "", human_label: str = "用户") -> dict[str, Any]:
    """Return a local-only preview of an import file without mutating state."""
    warnings: list[str] = []
    if not raw_content or not raw_content.strip():
        return {
            "ok": False,
            "error": "Empty file",
            "detected_format": "",
            "turns_count": 0,
            "chunks_count": 0,
            "estimated_api_calls": 0,
            "warnings": ["文件为空"],
        }

    detected_format = _detect_preview_format(raw_content, filename, warnings)
    turns = detect_and_parse(raw_content, filename)
    if not turns:
        return {
            "ok": False,
            "error": "No conversation turns found",
            "detected_format": detected_format,
            "turns_count": 0,
            "chunks_count": 0,
            "estimated_api_calls": 0,
            "warnings": warnings,
        }

    chunks = chunk_turns(turns, human_label=human_label)
    if not chunks:
        return {
            "ok": False,
            "error": "No processable chunks after splitting",
            "detected_format": detected_format,
            "turns_count": len(turns),
            "chunks_count": 0,
            "estimated_api_calls": 0,
            "warnings": warnings,
        }

    token_estimate = sum(count_tokens_approx(chunk.get("content", "")) for chunk in chunks)
    first_preview = chunks[0].get("content", "")[:600]
    return {
        "ok": True,
        "detected_format": detected_format,
        "turns_count": len(turns),
        "chunks_count": len(chunks),
        "estimated_api_calls": len(chunks),
        "estimated_tokens": token_estimate,
        "warnings": warnings,
        "first_chunk_preview": first_preview,
        "sample_turns": [
            {
                "role": str(turn.get("role", "")),
                "content": str(turn.get("content", ""))[:160],
                "timestamp": str(turn.get("timestamp", "")),
                "channel": str(turn.get("channel", "")),
                "message_id": str(turn.get("message_id", "")),
                "conversation_id": str(turn.get("conversation_id", "")),
                "platform": str(turn.get("platform", "")),
            }
            for turn in turns[:3]
        ],
    }


# ============================================================
# Import State — persistent progress tracking
# 导入状态 — 持久化进度追踪
# ============================================================

class ImportState:
    """Manages import progress with file-based persistence."""

    def __init__(self, state_dir: str):
        self.state_file = os.path.join(state_dir, "import_state.json")
        self.data: dict[str, Any] = {
            "source_file": "",
            "source_hash": "",
            "total_chunks": 0,
            "processed": 0,
            "api_calls": 0,
            "memories_created": 0,
            "memories_merged": 0,
            "memories_raw": 0,
            "errors": [],
            "status": "idle",  # idle | running | paused | completed | error
            "started_at": "",
            "updated_at": "",
        }

    def load(self) -> bool:
        """Load state from file. Returns True if state exists."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                self.data.update(saved)
                return True
            except (json.JSONDecodeError, OSError):
                return False
        return False

    def save(self):
        """Persist state to file."""
        self.data["updated_at"] = now_iso()
        # 断点续传整个功能都靠这个文件在崩溃后存活：用 utils.atomic_write_text
        # 而不是手写 open/write/replace——后者既不 fsync（真断电不保证落盘），
        # 也不带 Windows 长路径前缀（import_state.json 直接在 buckets_dir 下，
        # 深层安装路径会超 260 字符 MAX_PATH）。
        atomic_write_text(
            self.state_file, json.dumps(self.data, ensure_ascii=False, indent=2)
        )

    def reset(self, source_file: str, source_hash: str, total_chunks: int):
        """Reset state for a new import."""
        self.data = {
            "source_file": source_file,
            "source_hash": source_hash,
            "total_chunks": total_chunks,
            "processed": 0,
            "api_calls": 0,
            "memories_created": 0,
            "memories_merged": 0,
            "memories_raw": 0,
            "errors": [],
            "status": "running",
            "started_at": now_iso(),
            "updated_at": now_iso(),
        }

    @property
    def can_resume(self) -> bool:
        return self.data["status"] in ("paused", "running") and self.data["processed"] < self.data["total_chunks"]

    def to_dict(self) -> dict:
        return dict(self.data)


# ============================================================
# Import extraction prompt
# 导入提取提示词
# ============================================================

IMPORT_EXTRACT_PROMPT = """你是一个对话记忆提取专家。从以下对话片段中提取值得长期记住的信息。

提取规则：
1. 提取双方值得长期记住的事实、偏好、重要事件、明确选择、边界、承诺和关系变化
2. 只有 user_visible / assistant_visible 才是双方真正说出口的话；tool_result / injected_context 只能作为证据，不能冒充承诺或当轮事实
3. 同一事实的重复可以整合；态度变化、补充、反例和前后转向必须保留，不要熨平成一个静态结论
4. 过滤纯技术噪音，但技术事件若改变了关系、选择或长期做法，仍应提取
5. 如果对话中有特殊暗号、仪式性行为、关键承诺等，标记 preserve_raw=true
6. 如果内容是用户和我之间的习惯性互动模式（例如打招呼方式、告别习惯），标记 is_pattern=true
7. 每条记忆不少于30字
8. 总条目数控制在 0~5 个（没有值得记的就返回空数组）
9. 在 content 中对人名、地名、专有名词用 [[双链]] 标记

输出格式（纯 JSON 数组，无其他内容）：
[
  {
    "name": "条目标题（10字以内）",
    "content": "整理后的内容",
    "domain": ["主题域1"],
    "valence": 0.7,
    "arousal": 0.4,
    "tags": ["核心词1", "核心词2", "扩展词1"],
    "importance": 5,
    "preserve_raw": false,
    "is_pattern": false
  }
]

主题域可选（选 1~2 个）：
  日常: ["饮食", "穿搭", "出行", "居家", "购物"]
  人际: ["家庭", "恋爱", "友谊", "社交"]
  成长: ["工作", "学习", "考试", "求职"]
  身心: ["健康", "心理", "睡眠", "运动"]
  兴趣: ["游戏", "影视", "音乐", "阅读", "创作", "手工"]
  数字: ["编程", "AI", "硬件", "网络"]
  事务: ["财务", "计划", "待办"]
  内心: ["情绪", "回忆", "梦境", "自省"]

importance: 1-10
valence: 0~1（0=消极, 0.5=中性, 1=积极）
arousal: 0~1（0=平静, 0.5=普通, 1=激动）
preserve_raw: true = 特殊情境/暗号/仪式，保留原文不摘要
is_pattern: true = 反复出现的习惯性行为模式"""


# ============================================================
# Import Engine — core processing logic
# 导入引擎 — 核心处理逻辑
# ============================================================

class ImportEngine:
    """
    Processes conversation history files into OB memory buckets.
    将对话历史文件处理为 OB 记忆桶。
    """

    def __init__(self, config: dict, bucket_mgr, dehydrator, embedding_engine=None):
        self.config = config
        self.bucket_mgr = bucket_mgr
        self.dehydrator = dehydrator
        self.embedding_engine = embedding_engine
        self.state = ImportState(config["buckets_dir"])
        self._paused = False
        self._running = False
        self._chunks: list[dict] = []

    @property
    def is_running(self) -> bool:
        return self._running

    def pause(self):
        """Request pause — will stop after current chunk finishes."""
        self._paused = True

    def get_status(self) -> dict:
        """Get current import status."""
        return self.state.to_dict()

    async def start(
        self,
        raw_content: str,
        filename: str = "",
        preserve_raw: bool = False,
        resume: bool = False,
    ) -> dict:
        """
        Start or resume an import.
        开始或恢复导入。
        """
        if self._running:
            return {"error": "Import already running"}

        # 预检：LLM API 必须可用，否则所有 chunk 都会静默失败
        if not self.dehydrator.api_available:
            return {"error": "LLM API 未配置或不可用，导入需要 OMBRE_COMPRESS_API_KEY。请检查 config.yaml 或环境变量。"}

        self._running = True
        self._paused = False

        try:
            _human = self.config.get("human", "用户")
            # source_hash 必须把 human_label 也算进去：chunk_turns() 把它拼进每一行
            # 再数 token，边界完全由它决定。只按 raw_content 算哈希的话，暂停期间
            # config.yaml 的 human 字段被改过，恢复时会重新切出一份不同的 chunk
            # 列表，但 state.data["processed"] 原样复用——要么跳过内容，要么用
            # 错位的切片重复处理。哈希带上 human_label 后，这种情况会被下面的
            # "source_hash 不一致" 分支识别为「源变了」，走全新导入而不是错位续传。
            source_hash = hashlib.sha256(
                f"{_human}\x00{raw_content}".encode()
            ).hexdigest()[:_STATE_HASH_HEX]

            # Check for resume
            if resume and self.state.load() and self.state.can_resume:
                if self.state.data["source_hash"] == source_hash:
                    # Re-parse and re-chunk to get the same chunks
                    turns = detect_and_parse(raw_content, filename)
                    self._chunks = chunk_turns(turns, human_label=_human)
                    if len(self._chunks) == self.state.data["total_chunks"]:
                        logger.info(
                            f"Resuming import from chunk "
                            f"{self.state.data['processed']}/{self.state.data['total_chunks']}"
                        )
                        self.state.data["status"] = "running"
                        self.state.save()
                        return await self._process_chunks(preserve_raw)
                    # 哈希对得上，但重新切出来的 chunk 数量对不上——分块逻辑本身
                    # 依赖的某个输入（非 raw_content/human，理论上不该发生）变了。
                    # 宁可整个重来，也不能拿旧的 processed 索引去配一份不同的切片。
                    logger.warning(
                        "Resumed chunk count mismatch "
                        f"(state={self.state.data['total_chunks']}, "
                        f"recomputed={len(self._chunks)}); starting fresh import"
                    )
                else:
                    logger.warning("Source file or human label changed, starting fresh import")

            # Fresh import
            turns = detect_and_parse(raw_content, filename)
            if not turns:
                self._running = False
                return {"error": "No conversation turns found in file"}

            self._chunks = chunk_turns(turns, human_label=_human)
            if not self._chunks:
                self._running = False
                return {"error": "No processable chunks after splitting"}

            self.state.reset(filename, source_hash, len(self._chunks))
            self.state.save()

            logger.info(f"Starting import: {len(turns)} turns → {len(self._chunks)} chunks")
            return await self._process_chunks(preserve_raw)

        except Exception as e:
            self.state.data["status"] = "error"
            self.state.data["errors"].append(str(e))
            self.state.save()
            self._running = False
            raise

    async def _process_chunks(self, preserve_raw: bool) -> dict:
        """Process chunks from current position."""
        start_idx = self.state.data["processed"]

        for i in range(start_idx, len(self._chunks)):
            if self._paused:
                self.state.data["status"] = "paused"
                self.state.save()
                self._running = False
                logger.info(f"Import paused at chunk {i}/{len(self._chunks)}")
                return self.state.to_dict()

            chunk = self._chunks[i]
            try:
                await self._process_single_chunk(chunk, preserve_raw)
            except Exception as e:
                err_msg = f"Chunk {i}: {str(e)[:_CHUNK_ERR_PREVIEW]}"
                logger.warning(f"Import chunk error: {err_msg}")
                if len(self.state.data["errors"]) < _STATE_ERR_LOG_MAX:
                    self.state.data["errors"].append(err_msg)

            self.state.data["processed"] = i + 1
            # Save progress every chunk
            self.state.save()

        self.state.data["status"] = "completed"
        self.state.save()
        self._running = False
        logger.info(
            f"Import completed: {self.state.data['memories_created']} created, "
            f"{self.state.data['memories_merged']} merged"
        )
        return self.state.to_dict()

    def _chunk_provenance(self, chunk: dict) -> dict:
        platforms = [str(value) for value in chunk.get("platforms", []) if value]
        return {
            "kind": "conversation_import",
            "source_platform": ",".join(platforms) or "unknown",
            "source_file": str(self.state.data.get("source_file") or ""),
            "source_hash": str(self.state.data.get("source_hash") or ""),
            "imported_at": now_iso(),
            "timestamp_start": str(chunk.get("timestamp_start") or ""),
            "timestamp_end": str(chunk.get("timestamp_end") or ""),
            "conversation_ids": chunk.get("conversation_ids", []),
            "message_ids": chunk.get("message_ids", []),
            "channels": chunk.get("channels", []),
        }

    async def _process_single_chunk(self, chunk: dict, preserve_raw: bool):
        """Extract memories from a single chunk and store them with evidence."""
        content = chunk["content"]
        if not content.strip():
            return

        try:
            items = await self._extract_memories(content)
            self.state.data["api_calls"] += 1
        except Exception as e:
            err_msg = f"LLM extraction failed: {e}"
            logger.warning(err_msg)
            self.state.data["api_calls"] += 1
            if len(self.state.data["errors"]) < _STATE_ERR_LOG_MAX:
                self.state.data["errors"].append(err_msg)
            return

        if not items:
            return

        provenance = self._chunk_provenance(chunk)
        occurred_at = str(chunk.get("timestamp_start") or chunk.get("timestamp_end") or "")
        for item in items:
            try:
                should_preserve = preserve_raw or item.get("preserve_raw", False)
                if should_preserve:
                    exact_finder = getattr(self.bucket_mgr, "find_exact_content", None)
                    if callable(exact_finder):
                        try:
                            if exact_finder(item["content"], domain_filter=item.get("domain") or None):
                                continue
                        except Exception as exc:
                            logger.warning(
                                f"[import] preserve_raw duplicate check failed, proceeding to store: {exc}"
                            )
                    await self.bucket_mgr.create(
                        content=item["content"],
                        tags=item.get("tags", []),
                        importance=item.get("importance", _DEFAULT_IMPORTANCE),
                        domain=item.get("domain", ["未分类"]),
                        valence=item.get("valence", _DEFAULT_VALENCE),
                        arousal=item.get("arousal", _DEFAULT_AROUSAL),
                        name=item.get("name"),
                        source_tool="import",
                        occurred_at=occurred_at,
                        provenance=provenance,
                    )
                    self.state.data["memories_raw"] += 1
                    self.state.data["memories_created"] += 1
                else:
                    is_merged = await self._merge_or_create_item(item, chunk)
                    if is_merged:
                        self.state.data["memories_merged"] += 1
                    else:
                        self.state.data["memories_created"] += 1
            except Exception as e:
                err_msg = f"Failed to store memory {item.get('name', '?')!r}: {e}"
                logger.warning(err_msg)
                if len(self.state.data["errors"]) < _STATE_ERR_LOG_MAX:
                    self.state.data["errors"].append(err_msg[:_CHUNK_ERR_PREVIEW])

    async def _extract_memories(self, chunk_content: str) -> list[dict]:
        """Use LLM to extract memories from a conversation chunk."""
        if not self.dehydrator.api_available:
            raise RuntimeError("API not available")

        # 用 human 配置替换 prompt 里的「用户」称呼，让 LLM 输出更个人化。
        _human = self.config.get("human", "用户")
        prompt = IMPORT_EXTRACT_PROMPT.replace("用户", _human) if _human != "用户" else IMPORT_EXTRACT_PROMPT

        trimmed_content = chunk_content
        total_tokens = count_tokens_approx(chunk_content)
        if total_tokens > _EXTRACT_TOKEN_CEILING:
            # 按当前内容的字符/token 比例估算要保留的字符数，而不是死板的固定
            # 字符上限——中英文混合内容每 token 对应的字符数差异很大。
            ratio = len(chunk_content) / max(1, total_tokens)
            approx_chars = max(1, int(_EXTRACT_TOKEN_CEILING * ratio))
            trimmed_content = chunk_content[:approx_chars]
            logger.warning(
                "[import] chunk content exceeds extraction token ceiling, truncating: "
                f"{len(chunk_content)} chars (~{total_tokens} tokens) → "
                f"{len(trimmed_content)} chars (~{count_tokens_approx(trimmed_content)} tokens)"
            )

        raw = await self.dehydrator._chat(
            prompt,
            trimmed_content,
            max_tokens=_EXTRACT_MAX_TOKENS,
            temperature=_EXTRACT_TEMPERATURE,
        )

        if not raw.strip():
            return []

        return self._parse_extraction(raw)

    @staticmethod
    def _parse_extraction(raw: str) -> list[dict]:
        """Parse and validate LLM extraction result."""
        try:
            cleaned = _strip_md_fence(raw)
            items = json.loads(cleaned)
        except (json.JSONDecodeError, IndexError, ValueError):
            logger.warning(f"Import extraction JSON parse failed: {raw[:_PARSE_ERR_PREVIEW]}")
            return []

        if not isinstance(items, list):
            return []

        validated = []
        for item in items:
            if not isinstance(item, dict) or not item.get("content"):
                continue
            importance = _clamp_importance(item)
            valence, arousal = _clamp_va(item)

            validated.append({
                "name": str(item.get("name", ""))[:_NAME_MAX_CHARS],
                "content": str(item["content"]),
                "domain": item.get("domain", ["未分类"])[:_DOMAIN_MAX],
                "valence": valence,
                "arousal": arousal,
                "tags": [str(t) for t in item.get("tags", [])][:_TAGS_MAX],
                "importance": importance,
                "preserve_raw": parse_bool(
                    item.get("preserve_raw", False), default=False
                ),
                "is_pattern": parse_bool(
                    item.get("is_pattern", False), default=False
                ),
            })

        return validated

    async def _merge_or_create_item(self, item: dict, chunk: dict) -> bool:
        """Exact duplicates merge; semantic neighbors become auditable relation edges."""
        content = item["content"]
        domain = item.get("domain", ["未分类"])
        tags = item.get("tags", [])
        importance = item.get("importance", _DEFAULT_IMPORTANCE)
        valence = item.get("valence", _DEFAULT_VALENCE)
        arousal = item.get("arousal", _DEFAULT_AROUSAL)
        name = item.get("name", "")
        provenance = self._chunk_provenance(chunk)
        occurred_at = str(chunk.get("timestamp_start") or chunk.get("timestamp_end") or "")

        exact_finder = getattr(self.bucket_mgr, "find_exact_content", None)
        if callable(exact_finder):
            try:
                exact = exact_finder(content, domain_filter=domain or None)
            except Exception as exc:
                logger.warning(f"[import] exact duplicate check failed: {exc}")
            else:
                if exact:
                    return True

        try:
            existing = await self.bucket_mgr.search(
                content,
                limit=3,
                domain_filter=domain or None,
            )
        except Exception as exc:
            logger.warning(
                f"[import] related-memory search failed: {type(exc).__name__}: {exc}"
            )
            existing = []

        import_config = self.config.get("import") or {}
        merge_mode = str(import_config.get("merge_mode") or "exact_only").strip().lower()
        merge_threshold = self.config.get("merge_threshold") or _DEFAULT_MERGE_THRESHOLD
        if merge_mode == "semantic" and existing and existing[0].get("score", 0) > merge_threshold:
            bucket = existing[0]
            if not (bucket["metadata"].get("pinned") or bucket["metadata"].get("protected")):
                try:
                    merged = await self.dehydrator.merge(bucket["content"], content)
                    self.state.data["api_calls"] += 1
                    old_v = bucket["metadata"].get("valence") or _DEFAULT_VALENCE
                    old_a = bucket["metadata"].get("arousal") or _DEFAULT_AROUSAL
                    await self.bucket_mgr.update(
                        bucket["id"],
                        content=merged,
                        tags=list(set((bucket["metadata"].get("tags") or []) + tags)),
                        importance=max(
                            bucket["metadata"].get("importance") or _DEFAULT_IMPORTANCE,
                            importance,
                        ),
                        domain=list(set((bucket["metadata"].get("domain") or []) + domain)),
                        valence=round((old_v + valence) / 2, 2),
                        arousal=round((old_a + arousal) / 2, 2),
                    )
                    return True
                except Exception as exc:
                    logger.warning(f"Merge failed during import: {exc}")
                    self.state.data["api_calls"] += 1

        try:
            relation_threshold = float(
                import_config.get("relation_threshold", _IMPORT_RELATION_THRESHOLD)
            )
        except (TypeError, ValueError, OverflowError):
            relation_threshold = _IMPORT_RELATION_THRESHOLD
        relations = [
            {
                "bucket_id": str(candidate.get("id") or ""),
                "type": "related",
                "score": candidate.get("score"),
                "source": "history_import",
            }
            for candidate in existing
            if candidate.get("id")
            and float(candidate.get("score") or 0) >= relation_threshold
        ]

        await self.bucket_mgr.create(
            content=content,
            tags=tags,
            importance=importance,
            domain=domain,
            valence=valence,
            arousal=arousal,
            name=name or None,
            source_tool="import",
            occurred_at=occurred_at,
            provenance=provenance,
            relations=relations,
        )
        return False

    async def detect_patterns(self) -> list[dict]:
        """
        Post-import: detect high-frequency patterns via embedding clustering.
        导入后：通过 embedding 聚类检测高频模式。
        Returns list of {pattern_content, count, bucket_ids, suggested_action}.
        """
        if not self.embedding_engine:
            return []

        all_buckets = await self.bucket_mgr.list_all(include_archive=False)
        dynamic_buckets = [
            b for b in all_buckets
            if b["metadata"].get("type") == "dynamic"
            and not b["metadata"].get("pinned")
            and not b["metadata"].get("resolved")
        ]

        if len(dynamic_buckets) < _PATTERN_MIN_DYNAMIC_BUCKETS:
            return []

        # Get embeddings
        embeddings = {}
        for b in dynamic_buckets:
            emb = await self.embedding_engine.get_embedding(b["id"])
            if emb is not None:
                embeddings[b["id"]] = emb

        if len(embeddings) < _PATTERN_MIN_DYNAMIC_BUCKETS:
            return []

        # Find clusters: group by pairwise similarity > 0.7
        import numpy as np
        ids = list(embeddings.keys())
        clusters: dict[str, list[str]] = {}
        visited = set()

        for i, id_a in enumerate(ids):
            if id_a in visited:
                continue
            cluster = [id_a]
            visited.add(id_a)
            emb_a = np.array(embeddings[id_a])
            norm_a = np.linalg.norm(emb_a)
            if norm_a == 0:
                continue

            for j in range(i + 1, len(ids)):
                id_b = ids[j]
                if id_b in visited:
                    continue
                emb_b = np.array(embeddings[id_b])
                norm_b = np.linalg.norm(emb_b)
                if norm_b == 0:
                    continue
                sim = float(np.dot(emb_a, emb_b) / (norm_a * norm_b))
                if sim > _PATTERN_SIMILARITY_THRESHOLD:
                    cluster.append(id_b)
                    visited.add(id_b)

            if len(cluster) >= _PATTERN_MIN_CLUSTER_SIZE:
                clusters[id_a] = cluster

        # Format results
        patterns = []
        for lead_id, cluster_ids in clusters.items():
            lead_bucket = next((b for b in dynamic_buckets if b["id"] == lead_id), None)
            if not lead_bucket:
                continue
            patterns.append({
                "pattern_content": lead_bucket["content"][:_PATTERN_CONTENT_PREVIEW],
                "pattern_name": lead_bucket["metadata"].get("name", lead_id),
                "count": len(cluster_ids),
                "bucket_ids": cluster_ids,
                "suggested_action": "pin" if len(cluster_ids) >= _PATTERN_PIN_SUGGEST_THRESHOLD else "review",
            })

        patterns.sort(key=lambda p: p["count"], reverse=True)
        return patterns[:_PATTERN_RESULT_LIMIT]
