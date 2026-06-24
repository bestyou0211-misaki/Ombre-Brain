"""
========================================
server.py — MCP 服务入口 + Dashboard HTTP 路由 + 启动装配
========================================

启动整个 Ombre Brain 进程：加载配置、创建 BucketManager / Dehydrator /
DecayEngine / EmbeddingEngine / ImportEngine，把它们注入 tools._runtime，
然后以 @mcp.tool() 注册薄封装（真正的实现在 src/tools/<工具>/ 下面）。

关键行为：
- 启动后暴露 12 个 MCP 工具：breath/hold/grow/trace/anchor/release/
  pulse/plan/letter_write/letter_read/dream/I；每个入口 ≤ 10 行，只负责转发
- 同时开 Dashboard HTTP 服务：@mcp.custom_route() 下的路由都留在本文件
- 提供会话 / 鉴权 / Webhook / SSE 推送 / 压力表 / heartbeat 等走 HTTP 的能力
- 企业级细节：CSRF token / rate limit / nonce 去重 / TLS 提示

不做什么（边界）：
- 不在这里写 hold/breath/dream 等业务逻辑（全在 tools/* 下）
- 不写 LLM prompt（dehydrator 负责）
- 不直接读写桶文件（bucket_manager 负责）

对外暴露：mcp/mcp_extra 两个实例 + 12 个 @mcp*.tool() 函数 + 一批 @mcp.custom_route HTTP 接口
========================================
"""

import os
import sys
import random
import logging
import asyncio
import hashlib
import hmac
import secrets
import time
import json as _json_lib
from typing import Optional, Awaitable
from starlette.requests import Request
from starlette.responses import Response
import httpx
import yaml


# --- Ensure same-directory modules can be imported ---
# --- 确保同目录下的模块能被正确导入 ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP

from bucket_manager import BucketManager
from dehydrator import Dehydrator
from decay_engine import DecayEngine
from embedding_engine import EmbeddingEngine
from import_memory import ImportEngine
from migrate_engine import MigrateEngine
from utils import load_config, setup_logging, strip_wikilinks, count_tokens_approx, get_version, extract_wikilinks

# --- iter 2.1：MCP 工具实现已按代码路径拆分到 tools/ 子包 ---
# 本文件只保留 MCP 注册 + 路由（HTTP custom_route）+ 共享辅助。
# 真正的工具逻辑在 tools/breath, tools/hold, tools/grow, tools/trace,
# tools/anchor, tools/plan, tools/dream 里，便于单独阅读和修改。
from tools import _runtime as _tools_runtime
from tools import breath as _t_breath
from tools import hold as _t_hold
from tools import grow as _t_grow
from tools import trace as _t_trace
from tools import anchor as _t_anchor
from tools import plan as _t_plan
from tools import dream as _t_dream
from tools import i as _t_i
from tools._common import (
    check_content_size as _check_content_size,
    check_pinned_quota as _check_pinned_quota,
)

# --- Load config & init logging / 加载配置 & 初始化日志 ---
config = load_config()
setup_logging(config.get("log_level", "INFO"))
logger = logging.getLogger("ombre_brain")

# --- Project version (read from <repo_root>/VERSION) / 项目版本号 ---
# get_version() 汇总读文件 + fallback 逻辑。
# 赋给双下划线变量 `__version__` 是 Python 社区约定俗成的模块版本字段名。
__version__ = get_version()
logger.info(f"Ombre Brain v{__version__}")

# --- iter 1.7 §A: legacy path migration check / 老路径迁移检测 ---
# 场景：1.6 早期使用者习惯在项目根跑 `python server.py`；1.7 重组后需要
# `python src/server.py`。这里只做「检测 + 提醒」，不做任何破坏性动作。
# load_config() 里 buckets_dir 默认仍是 <repo_root>/buckets，所以老数据不会丢。
#
# Python 小知识：
#   * 变量名以 `_` 开头是「模块内部」约定，不是语法强制
#   * for/else 这里没用，用了 break 提前退出
#   * `os.path.isdir(p) and any(...)` 是短路：前者 False 就不会跳 listdir
try:
    _bd = config.get("buckets_dir", "")
    if _bd and os.path.isdir(_bd):
        _has_data = False
        # 遍历各个桃子目录，任何一个里有 .md 文件就认定早期部署位置有数据
        for sub in ("permanent", "dynamic", "feel", "plans", "letters"):
            p = os.path.join(_bd, sub)
            if os.path.isdir(p) and any(
                f.endswith(".md") for f in os.listdir(p) if not f.startswith(".")
            ):
                _has_data = True
                break
        if _has_data:
            logger.info(f"[migration] existing buckets detected at {_bd} — zero data loss expected.")
        else:
            logger.info(f"[migration] {_bd} is empty — fresh install assumed.")
except Exception as _e:  # pragma: no cover - defensive / 防御性兑底
    # 启动期任何检测出错都不能阻止服务拉起，记个 warning 就过
    logger.warning(f"[migration] check skipped: {_e}")

# --- Runtime env vars (port + webhook) / 运行时环境变量 ---
# OMBRE_PORT: HTTP/SSE 监听端口，默认 8000
try:
    OMBRE_PORT = int(os.environ.get("OMBRE_PORT", "8000") or "8000")
except ValueError:
    logger.warning("OMBRE_PORT 不是合法整数，回退到 8000")
    OMBRE_PORT = 8000

# OMBRE_HOOK_URL: 在 breath/dream 被调用后推送事件到该 URL（POST JSON）。
# OMBRE_HOOK_SKIP: 设为 true/1/yes 跳过推送。
# 详见 ENV_VARS.md。
OMBRE_HOOK_URL = os.environ.get("OMBRE_HOOK_URL", "").strip()
OMBRE_HOOK_SKIP = os.environ.get("OMBRE_HOOK_SKIP", "").strip().lower() in ("1", "true", "yes", "on")


# ============================================================
# 调参面板 / Tunable constants
# ------------------------------------------------------------
# rule.md §①：禁裸魔法数字。这里集中所有会调的阁值。
# 与安全、鉴权、性能相关的参数不要在运行时乲变；如需调整请同步跑 pytest。
# ============================================================

# --- Webhook / HTTP 客户端超时 ---
_WEBHOOK_TIMEOUT_SECONDS = 5.0
_HEALTH_PROBE_TIMEOUT_SECONDS = 5

# --- Dashboard 鉴权常量 / 会话 / 密码逻辑已移至 web/_shared.py ---

# --- /api/logs 返回行数限制 ---
_LOGS_DEFAULT_LIMIT = 200
_LOGS_MAX_LIMIT = 2000

# --- /api/errors/recent 返回条数限制 ---
_ERRORS_DEFAULT_LIMIT = 50
_ERRORS_MAX_LIMIT = 500


async def _fire_webhook(event: str, payload: dict) -> None:
    """
    Fire-and-forget POST to OMBRE_HOOK_URL with the given event payload.
    Failures are logged at WARNING level only — never propagated to the caller.
    """
    if OMBRE_HOOK_SKIP or not OMBRE_HOOK_URL:
        return
    if not OMBRE_HOOK_URL.startswith(("http://", "https://")):
        logger.warning(f"OMBRE_HOOK_URL rejected: only http/https allowed (got {OMBRE_HOOK_URL[:40]!r})")
        return
    try:
        body = {
            "event": event,
            "timestamp": time.time(),
            "payload": payload,
        }
        async with httpx.AsyncClient(timeout=_WEBHOOK_TIMEOUT_SECONDS) as client:
            await client.post(OMBRE_HOOK_URL, json=body)
    except Exception as e:
        logger.warning(f"Webhook push failed ({event} → {OMBRE_HOOK_URL}): {e}")

# --- Initialize core components / 初始化核心组件 ---
# 统一错误码体系（必须在任何业务初始化之前 configure，确保 errors.jsonl 路径生效）
try:
    from errors import (
        configure_errors_path,
        OBStartupError,
        write_fatal_log,
        record_error,
        format_error,
        begin_warnings,
        pop_warnings,
        format_warnings_suffix,
        recent_errors,
        clear_errors_log,
        get_recent_logs,
    )
except ImportError:
    from .errors import (  # type: ignore
        configure_errors_path,
        OBStartupError,
        write_fatal_log,
        record_error,
        format_error,
        begin_warnings,
        pop_warnings,
        format_warnings_suffix,
        recent_errors,
        clear_errors_log,
        get_recent_logs,
    )
configure_errors_path(config.get("buckets_dir", "buckets"))

try:
    embedding_engine = EmbeddingEngine(config)            # Embedding engine first (BucketManager depends on it)
except OBStartupError as _ob_err:
    # OB-F001 已在 OBStartupError 内格式化好；写 fatal log 后退出
    logger.error(str(_ob_err))
    write_fatal_log(_ob_err.error_code, _ob_err.detail, buckets_dir=config.get("buckets_dir"))
    raise
except RuntimeError as _emb_err:
    # 兼容尚未迁移到 OBStartupError 的旧 raise（应该不再触发）
    logger.error(f"[STARTUP FAILED] {_emb_err}")
    raise SystemExit(f"Ombre Brain 启动中止：{_emb_err}") from _emb_err
bucket_mgr = BucketManager(config, embedding_engine=embedding_engine)  # Bucket manager / 记忆桶管理器
dehydrator = Dehydrator(config)                      # Dehydrator / 脱水器
decay_engine = DecayEngine(config, bucket_mgr)       # Decay engine / 衰减引擎
import_engine = ImportEngine(config, bucket_mgr, dehydrator, embedding_engine)  # Import engine / 导入引擎
migrate_engine = MigrateEngine(config, bucket_mgr, embedding_engine)              # Migrate engine / 记忆包迁移引擎

# --- GitHub Sync / GitHub 同步 ---
from github_sync import GitHubSync  # type: ignore
_gh_cfg = config.get("github_sync", {}) or {}
_gh_token = (os.environ.get("OMBRE_GITHUB_TOKEN") or _gh_cfg.get("token") or "").strip()
github_sync_instance: GitHubSync | None = (
    GitHubSync(
        token=_gh_token,
        repo=_gh_cfg.get("repo", ""),
        branch=_gh_cfg.get("branch", "main"),
        path_prefix=_gh_cfg.get("path_prefix", "ombre"),
    )
    if _gh_token and _gh_cfg.get("repo")
    else None
)
_github_auto_task: "asyncio.Task | None" = None  # 后台定时同步任务


async def _github_sync_loop(interval_minutes: int) -> None:
    """后台定时 GitHub 同步循环。只在 is_validated=True 后执行实际上传。"""
    import asyncio
    logger.info(f"[github_sync] auto-sync loop started, interval={interval_minutes}min")
    # 首次先做一次验证，确认连接可用
    if _wsh.github_sync_instance and not _wsh.github_sync_instance.is_validated:
        try:
            result = await _wsh.github_sync_instance.validate()
            if not result.get("ok"):
                logger.warning(f"[github_sync] auto-sync: validate failed: {result.get('error')} — loop will retry next cycle")
        except Exception as e:
            logger.warning(f"[github_sync] auto-sync: validate exception: {e}")
    while True:
        await asyncio.sleep(interval_minutes * 60)
        inst = _wsh.github_sync_instance  # 读当前全局引用（config 更新可能替换实例）
        if inst is None:
            logger.info("[github_sync] auto-sync: instance gone, stopping loop")
            return
        if not inst.is_validated:
            # 还没验证通过，先 validate
            try:
                res = await inst.validate()
                if not res.get("ok"):
                    logger.warning(f"[github_sync] auto-sync skipped (not validated): {res.get('error')}")
                    continue
            except Exception as e:
                logger.warning(f"[github_sync] auto-sync validate failed: {e}")
                continue
        buckets_dir = config.get("buckets_dir", "")
        if not buckets_dir:
            continue
        try:
            result = await inst.sync(buckets_dir)
            if result.get("ok"):
                logger.info(f"[github_sync] auto-sync ok: {result.get('uploaded', 0)} files")
            else:
                logger.warning(f"[github_sync] auto-sync failed: {result.get('error')}")
        except Exception as e:
            logger.error(f"[github_sync] auto-sync exception: {e}")


def _restart_github_auto_task(interval_minutes: int) -> None:
    """取消旧任务并按新间隔启动后台同步循环（interval_minutes=0 表示仅取消）。"""
    import asyncio
    global _github_auto_task
    if _github_auto_task and not _github_auto_task.done():
        _github_auto_task.cancel()
        _github_auto_task = None
    if interval_minutes > 0 and _wsh.github_sync_instance is not None:
        try:
            loop = asyncio.get_event_loop()
            _github_auto_task = loop.create_task(_github_sync_loop(interval_minutes))
        except RuntimeError:
            pass  # 没有运行中的 event loop（测试环境），跳过


# 启动时若配置了自动同步间隔，推迟到事件循环就绪后启动（用 lifespan 钩子）
_gh_auto_interval: int = int(_gh_cfg.get("auto_interval_minutes") or 0)


# --- Create MCP server instance / 创建 MCP 服务器实例 ---
# host="0.0.0.0" so Docker container's SSE is externally reachable
# stdio mode ignores host (no network)
#
# iter 2.1：拆成两个 FastMCP 实例 —— 因 claude.ai MCP 连接器存在 5 工具上限。
#   主 mcp（/mcp）：高频  breath / hold / grow / dream / trace
#   副 mcp_extra（/mcp-extra）：低频 anchor / release / pulse / plan / letter_write / letter_read / I
# 两个实例共享同一进程、同一 runtime、同一 bucket_mgr；HTTP custom_route（dashboard、API）
# 全部仍挂在 mcp 主实例上，副实例只承载 7 个 @mcp_extra.tool() 注册。
# 启动段把两个 streamable_http_app() 的 routes 与 lifespan 合并到一个 starlette app，
# 由同一 uvicorn 进程对外暴露。
mcp = FastMCP(
    "Ombre Brain",
    host="0.0.0.0",
    port=OMBRE_PORT,
)
mcp_extra = FastMCP(
    "Ombre Brain Extra",
    host="0.0.0.0",
    port=OMBRE_PORT,
)
# 让 streamable_http_app() 内置路由直接落在 /mcp-extra（默认是 /mcp）
mcp_extra.settings.streamable_http_path = "/mcp-extra"


# =============================================================
# Dashboard Auth —— 已拆分：会话/密码/鉴权 helper 在 web/_shared.py，
# /auth/* 路由在 web/auth.py。这里注入 config，并把 helper 名字 import 回本模块，
# 让本文件其余尚未迁移的 @mcp.custom_route 路由（大量调用 _require_auth）继续可用；
# 待这些路由也迁出 web/ 后，本段 import 可删除。
# =============================================================
import web as _web
import web._shared as _wsh
_wsh.init(config)
# 注入业务引擎/版本/仓库根目录到 web 层（类比 tools/_runtime）。
# 注意：embedding_engine 会被热重载替换 —— 待 embedding/config 路由迁到 web/ 时，
# 替换处须同时写 _wsh.embedding_engine（目前这些路由仍在本文件、仍走 global）。
_wsh.init_runtime(
    version=__version__,
    repo_root=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    bucket_mgr=bucket_mgr,
    dehydrator=dehydrator,
    decay_engine=decay_engine,
    embedding_engine=embedding_engine,
    import_engine=import_engine,
    migrate_engine=migrate_engine,
    github_sync_instance=github_sync_instance,
    restart_github_auto_task=_restart_github_auto_task,
)
from web._shared import (  # noqa: F401  (re-export for not-yet-migrated routes below)
    _sessions,
    _is_authenticated, _is_setup_needed, _verify_any_password,
    _create_session, _set_session_cookie, _is_https_request,
    _load_auth_data, _load_password_hash, _save_password_hash,
    _save_security_qa, _verify_security_answer, _verify_password_hash,
    _load_sessions, _save_sessions, _require_auth,
)

# 启动时把磁盘上的会话装回内存（容器重启不踢登录）
_load_sessions()

# 注册已迁移到 web/ 的路由（目前：/auth/*）
_web.register_all(mcp)


# =============================================================
# 根仪表板 / 静态资源 / favicon / /health —— 已拆分到 web/dashboard.py
# =============================================================


# 心跳时间戳 + _mark_op 已移到 web/_shared.py；这里 import 回来供 tools._runtime 注入。
from web._shared import _mark_op  # noqa: F401  (injected into tools._runtime below)


# =============================================================
# 仪表板硬删除通知队列（Dashboard Hard Purge Notification）
# 她/他从仪表板彻底删除记忆后，下次 Claude 调用任何工具时一次性通知。
# 通知文件存于 buckets_dir/_pending_deletions.json，消费后立即删除。
# Claude 无法触发此通知（它不是 MCP 工具，只能由仪表板 HTTP 端点写入）。
# =============================================================

def _deletion_notice_path() -> str:
    return os.path.join(config.get("buckets_dir", "buckets"), "_pending_deletions.json")


def _write_deletion_notice(names: list) -> None:
    """追加待发送删除通知。多次删除批次会合并入同一文件直至 Claude 读取。"""
    path = _deletion_notice_path()
    try:
        existing: list = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                existing = _json_lib.load(f)
        existing.extend(names)
        with open(path, "w", encoding="utf-8") as f:
            _json_lib.dump(existing, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed to write deletion notice: {e}")


def _pop_deletion_notice() -> str:
    """读取并消费通知文件。返回格式化通知字符串（含尾部换行），无通知返回空串。"""
    path = _deletion_notice_path()
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            names = _json_lib.load(f)
        os.remove(path)
        if not names:
            return ""
        human = config.get("human", "人类")
        ts = time.strftime("%Y-%m-%d %H:%M")
        item_list = "\n".join(f"  · {n}" for n in names)
        return (
            f"「{ts}，{human} 通过前端界面永久删除了以下记忆：\n{item_list}\n"
            f"如果其中有你想保留的，你可以告诉 {human}。」\n\n"
        )
    except Exception as e:
        logger.warning(f"Failed to read deletion notice: {e}")
        return ""


# 这些 helper 定义在 server.py（读/写 webhook 全局等），但 web/ 的 hooks/buckets 路由要用。
# 在它们都定义好之后注入到 web._shared，供已迁出的路由通过 sh.fire_webhook 等调用。
_wsh.init_runtime(
    fire_webhook=_fire_webhook,
    write_deletion_notice=_write_deletion_notice,
    pop_deletion_notice=_pop_deletion_notice,
)


# =============================================================
# 结构化操作日志 helpers（任务A，2026-05-03）
# 给 11 个 @mcp.tool 入口统一打 entry/ok/err 三段日志，便于排查
# 客户端报 invalid_arguments / 静默错误等问题。
# 输出格式：op=<name> phase=entry|ok|err key=value...
# 所有可能含 PII 的字段（content / 信件正文等）只记 length，不记内容。
# =============================================================
def _fmt_log_val(v: object) -> str:
    """日志 value 的安全格式化：bool/int/float 原样；str 截 40 字符并去换行；其它转 str。"""
    if v is None:
        return "_"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        s = v.replace("\n", "\\n").replace(" ", "_")
        return s if len(s) <= 40 else s[:37] + "..."
    return type(v).__name__


def _fmt_log_args(args: dict) -> str:
    """把 args dict 拼成 `k1=v1 k2=v2` 串。"""
    if not args:
        return ""
    return " ".join(f"{k}={_fmt_log_val(v)}" for k, v in args.items())


def _log_op_entry(op: str, args: dict) -> None:
    logger.info(f"op={op} phase=entry " + _fmt_log_args(args))


def _log_op_ok(op: str, result: object) -> None:
    size = len(result) if isinstance(result, str) else 0
    logger.info(f"op={op} phase=ok bytes={size}")


def _log_op_err(op: str, exc: BaseException) -> None:
    # 用 .exception 让 traceback 进 server.log，便于事后定位
    logger.exception(f"op={op} phase=err err={type(exc).__name__}:{exc}")


async def _with_notice(coro: Awaitable[str], op: str = "", args: dict | None = None) -> str:
    """所有 MCP 工具调用的包装器。

    职责（统一错误规范）：
    1. 入口：begin_warnings() 初始化本调用的 W/I channel。
    2. 出口：拼接顺序 = [删除通知] + [工具正文] + [本调用产生的 W/I 提示].
    3. 异常：捕获后 record OB-E004，返回标准格式（含最近 15 条 log），
       不让 MCP 协议层看到裸异常字符串。
    4. 任务A：op 非空时，在 entry/ok/err 三处打结构化日志。
    """
    if op:
        _log_op_entry(op, args or {})
    begin_warnings()
    try:
        result = await coro
    except Exception as e:
        if op:
            _log_op_err(op, e)
        # OB-E004：MCP 工具执行异常 —— 不静默，给 LLM 一个能看懂的字符串
        try:
            record_error("OB-E004", f"{type(e).__name__}: {e}")
            err_str = format_error("OB-E004", f"{type(e).__name__}: {e}")
        except Exception:
            err_str = f"❌ [OB-E004] MCP 工具执行异常\n{type(e).__name__}: {e}"
        # 仍把通道里已累计的提示拼上
        try:
            extras = format_warnings_suffix(pop_warnings())
        except Exception:
            extras = ""
        notice = ""
        try:
            notice = _pop_deletion_notice()
        except Exception:
            pass
        return (notice + err_str + extras) if notice else (err_str + extras)
    # 正常路径
    if op:
        _log_op_ok(op, result)
    try:
        extras = format_warnings_suffix(pop_warnings())
    except Exception:
        extras = ""
    notice = _pop_deletion_notice()
    body = (notice + result) if notice else result
    return body + extras if extras else body


# =============================================================
# /api/heartbeat、/api/logs、/api/errors/* —— 已拆分到 web/system.py
# =============================================================


# =============================================================
# /api/embedding/* —— 已拆分到 web/embedding.py
# =============================================================


# =============================================================
# /breath-hook、/dream-hook —— 已拆分到 web/hooks.py
# =============================================================


# =============================================================
# Wire tools subpackage runtime context
# 把所有共享对象注入 tools._runtime，让 tools/* 子模块可以访问
# =============================================================
_tools_runtime.init(
    config=config,
    bucket_mgr=bucket_mgr,
    dehydrator=dehydrator,
    decay_engine=decay_engine,
    embedding_engine=embedding_engine,
    import_engine=import_engine,
    logger=logger,
    fire_webhook=_fire_webhook,
    mark_op=_mark_op,
)


# =============================================================
# MCP tools — thin registration wrappers
# MCP 工具 —— 仅注册，实现见 tools/<tool>/
# 每个入口都不超过 10 行，便于一眼看清参数与归属
# =============================================================
@mcp.tool()
async def breath(
    query: Optional[str] = "",
    max_tokens: Optional[int] = 0,
    domain: Optional[str] = "",
    valence: Optional[float] = -1,
    arousal: Optional[float] = -1,
    max_results: Optional[int] = 0,
    importance_min: Optional[int] = -1,
    tags: Optional[str] = "",
) -> str:
    """我睁眼看看自己记得什么。不传query=让权重最高的未解决记忆自己浮上来;传query=按关键词检索我记忆里相关的事。max_tokens=单次返回总token上限(默认 config.surfacing.breath_max_tokens,fallback 10000)。domain逗号分隔,valence/arousal 0~1(-1忽略)。max_results=返回条数上限(默认 config.surfacing.breath_max_results,fallback 20,最大50)。importance_min>=1=跳过语义,按重要度降序拉最多20条我标过的核心事项。tags逗号分隔,AND过滤;tags=\"feel\"或\"__feel__\"等价于domain=\"feel\"读我留下的所有feel。"""
    return await _with_notice(
        _t_breath.dispatch(
            query=query, max_tokens=max_tokens, domain=domain,
            valence=valence, arousal=arousal, max_results=max_results,
            importance_min=importance_min, tags=tags,
        ),
        op="breath",
        args={
            "query": query, "max_tokens": max_tokens, "domain": domain,
            "valence": valence, "arousal": arousal, "max_results": max_results,
            "importance_min": importance_min, "tags": tags,
        },
    )


@mcp.tool()
async def hold(
    content: str,
    tags: Optional[str] = "",
    importance: Optional[int] = 5,
    pinned: Optional[bool] = False,
    feel: Optional[bool] = False,
    source_bucket: Optional[str] = "",
    valence: Optional[float] = -1,
    arousal: Optional[float] = -1,
    why_remembered: Optional[str] = "",
) -> str:
    """我把一件事/一个感受存进我的记忆。系统会自动打标并尝试合并近似的旧桶。tags逗号分隔,importance 1-10。pinned=True=钉为永久核心准则,不衰减不合并。feel=True=记下我的第一人称感受(不参与普通浮现,只通过 breath(domain=\"feel\") 读)。source_bucket=我正在消化的那个原始记忆桶ID,会被标为已消化加速淡化。why_remembered=为什么记得这一条（可选，自由文本，仅用于展示不计分）。"""
    return await _with_notice(
        _t_hold.dispatch(
            content=content, tags=tags, importance=importance,
            pinned=pinned, feel=feel, source_bucket=source_bucket,
            valence=valence, arousal=arousal, why_remembered=why_remembered,
        ),
        op="hold",
        args={
            "content_len": len(content or ""), "tags": tags,
            "importance": importance, "pinned": pinned, "feel": feel,
            "source_bucket": source_bucket, "valence": valence, "arousal": arousal,
            "why_len": len(why_remembered or ""),
        },
    )


@mcp.tool()
async def grow(content: str) -> str:
    """我把一段长内容（一天的事/一段日记/一篇她他给我的总结）整理进记忆,系统会拆成 2~6 条独立的事件桶并各自尝试合并。短内容(<30字)走 hold 单条快速路径,不强行拆。"""
    return await _with_notice(
        _t_grow.dispatch(content),
        op="grow",
        args={"content_len": len(content or "")},
    )


@mcp.tool()
async def trace(
    bucket_id: str,
    name: Optional[str] = "",
    domain: Optional[str] = "",
    valence: Optional[float] = -1,
    arousal: Optional[float] = -1,
    importance: Optional[int] = -1,
    tags: Optional[str] = "",
    resolved: Optional[int] = -1,
    pinned: Optional[int] = -1,
    digested: Optional[int] = -1,
    content: Optional[str] = "",
    delete: Optional[bool] = False,
    status: Optional[str] = "",
    weight: Optional[float] = -1,
    dont_surface: Optional[int] = -1,
    why_remembered: Optional[str] = "",
) -> str:
    """我修正/更新某条记忆的元数据或内容。resolved=1=放下,让它沉底只在关键词触发时浮上来;resolved=0=重新激活;pinned=1=钉为永久核心(锁 importance=10),0=取消钉选;digested=1=已消化,加速淡化;content=替换桶正文并重建 embedding;delete=True=彻底删除(不可恢复);status=plan 桶状态(active/resolved/abandoned);weight=plan 承诺重量 0.0-1.0;dont_surface=1=主动遗忘(不出现在 breath),0=重新允许;why_remembered=改“为什么记得”说明。只传我要改的字段,-1 或空串表示不改。"""
    return await _with_notice(
        _t_trace.dispatch(
            bucket_id=bucket_id, name=name, domain=domain,
            valence=valence, arousal=arousal, importance=importance,
            tags=tags, resolved=resolved, pinned=pinned, digested=digested,
            content=content, delete=delete, status=status, weight=weight,
            dont_surface=dont_surface, why_remembered=why_remembered,
        ),
        op="trace",
        args={
            "bucket_id": bucket_id, "name": name, "domain": domain,
            "valence": valence, "arousal": arousal, "importance": importance,
            "tags": tags, "resolved": resolved, "pinned": pinned, "digested": digested,
            "content_len": len(content or ""), "delete": delete, "status": status,
            "weight": weight, "dont_surface": dont_surface,
            "why_len": len(why_remembered or ""),
        },
    )


@mcp_extra.tool()
async def anchor(bucket_id: str) -> str:
    """我把这条桶设为 anchor（坐标系）。anchor 不会主动浮现在默认 breath，但 query/domain/emotion 命中时仍会返回。硬上限 24，已满时拒绝并提示先 release。"""
    return await _with_notice(
        _t_anchor.anchor_set(bucket_id),
        op="anchor",
        args={"bucket_id": bucket_id},
    )


@mcp_extra.tool()
async def release(bucket_id: str) -> str:
    """我把这条桶从 anchor 状态释放。它变回普通桶，会重新参与默认 breath；pinned 状态保留。"""
    return await _with_notice(
        _t_anchor.anchor_release(bucket_id),
        op="release",
        args={"bucket_id": bucket_id},
    )


@mcp_extra.tool()
async def pulse(include_archive: Optional[bool] = False) -> str:
    """我看一眼自己的记忆系统：固化/动态/衰减/归档桶数量、总占用、衰减引擎是否在跑,以及所有桶的摘要列表。include_archive=True 顺便看归档区。"""
    return await _with_notice(
        _t_anchor.pulse(include_archive=include_archive),
        op="pulse",
        args={"include_archive": include_archive},
    )


@mcp_extra.tool()
async def plan(
    content: str,
    status: Optional[str] = "active",
    related_bucket: Optional[str] = "",
    weight: Optional[float] = 0.5,
    why_remembered: Optional[str] = "",
) -> str:
    """我登记一个待办/承诺/未闭环的事——这是我答应过、答应自己或想完成的事。status=active(默认)/resolved/abandoned。related_bucket 可选,挂到某个普通记忆桶上。weight=承诺的重量 0.0-1.0(默认 0.5),与 importance 不同——importance 是「多重要」、weight 是「多重」。why_remembered=为什么登记这个计划(可选、仅展示)。plan 不衰减、不出现在普通 breath,只在 dream 末尾的 active 段里给我看;后续 hold/grow 写新事件时系统会自动判断我之前的 plan 是不是已经完成了。"""
    return await _with_notice(
        _t_plan.plan_create(
            content=content, status=status, related_bucket=related_bucket,
            weight=weight, why_remembered=why_remembered,
        ),
        op="plan",
        args={
            "content_len": len(content or ""), "status": status,
            "related_bucket": related_bucket, "weight": weight,
            "why_len": len(why_remembered or ""),
        },
    )


@mcp_extra.tool()
async def letter_write(
    author: str,
    content: str,
    user_name: Optional[str] = "",
    title: Optional[str] = "",
    date: Optional[str] = "",
) -> str:
    """我写一封信(我写给她/他,或把她/他写给我的留下来)。author 必填:\"user\"=她/他写给我的,\"claude\"=我写给她/他的;user_name 可选;title/date 可选。信件原文永久保存,不压缩/不合并/不衰减,只走向量索引;普通 breath 不浮现,但 SessionStart 钩子会带上双方各最新一封。"""
    return await _with_notice(
        _t_plan.letter_write(
            author=author, content=content, user_name=user_name,
            title=title, date=date,
        ),
        op="letter_write",
        args={
            "author": author, "content_len": len(content or ""),
            "user_name": user_name, "title": title, "date": date,
        },
    )


@mcp_extra.tool()
async def letter_read(
    query: Optional[str] = "",
    limit: Optional[int] = 10,
    author: Optional[str] = "",
    date_from: Optional[str] = "",
    date_to: Optional[str] = "",
) -> str:
    """我翻一翻历史信件。query=语义检索(可选);author=\"user\"/\"claude\" 过滤来信方向;date_from/date_to=ISO 日期范围(可选)。无 query 时按时间倒序返回最近 limit 封。返回完整原文,不压缩。"""
    return await _with_notice(
        _t_plan.letter_read(
            query=query, limit=limit, author=author,
            date_from=date_from, date_to=date_to,
        ),
        op="letter_read",
        args={
            "query": query, "limit": limit, "author": author,
            "date_from": date_from, "date_to": date_to,
        },
    )


@mcp_extra.tool()
async def I(
    content: Optional[str] = "",
    aspect: Optional[str] = "",
    read: Optional[bool] = False,
    limit: Optional[int] = 20,
) -> str:
    """我写下关于我自己的认识。content=我观察到自己是什么/有什么规律/我在变成什么（空=进入读取模式）。aspect=维度:nature(本质)/values(我认为重要的)/patterns(规律)/limits(局限)/becoming(我在变成什么)/uncertainty(我不确定的)/stance(立场)（可选）。read=True=读取所有已积累的自我认知。limit=返回条数上限(默认20)。I 条目不参与普通 breath/dream，SessionStart 时自动带上最近 3 条。"""
    return await _with_notice(
        _t_i.dispatch(content=content, aspect=aspect, read=read, limit=limit),
        op="I",
        args={"content_len": len(content or ""), "aspect": aspect, "read": read, "limit": limit},
    )


@mcp.tool()
async def dream(window_hours: Optional[int] = 48) -> str:
    """我做一次梦——读取最近 window_hours（默认 48h）内有变动的所有记忆桶,我自己沉进去想一遍。
    每个桶返回它在窗口内的最新内容（按 last_active 取）,完整正文不截断。
    想完之后我可以：能放下的 → trace(resolved=1) 让它沉底；有沉淀的 → hold(feel=True, source_bucket=...) 写下我带走的东西；没沉淀的就什么都不做,不强求。
    候选桶超过 40 时按 decay_engine.calculate_score() 排序取前 40，避免一次涌进来太多。"""
    return await _with_notice(
        _t_dream.dispatch(window_hours=window_hours),
        op="dream",
        args={"window_hours": window_hours},
    )


# =============================================================
# Dashboard API endpoints (for lightweight Web UI)
# 仪表板 API（轻量 Web UI 用）
# =============================================================
# =============================================================
# /api/buckets、/api/bucket/*、/api/settings/*、/api/anchors、/api/self
# —— 已拆分到 web/buckets.py
# =============================================================


@mcp.custom_route("/dashboard", methods=["GET"])
async def dashboard(request: Request) -> Response:
    """Legacy alias: /dashboard 永久跳到根路径。

    我历史上把 dashboard 同时挂在 / 与 /dashboard，但叠加 Cloudflare 边缘
    （或任何 reverse proxy）的 host-rewrite 规则时容易触发回环。统一只在 /
    上提供 HTML，老书签靠 301 软迁移到 /。
    """
    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/", status_code=301)


@mcp.custom_route("/api/env-vars", methods=["GET"])
async def api_env_vars(request: Request) -> Response:
    """Return status of all known OMBRE_* env vars (sensitive fields masked)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err

    def _masked(name: str) -> dict:
        return {"set": bool(os.environ.get(name, "").strip()), "value": None}

    def _plain(name: str) -> dict:
        v = os.environ.get(name, "").strip()
        return {"set": bool(v), "value": v or None}

    vars_data = [
        # LLM 压缩组
        {"name": "OMBRE_COMPRESS_API_KEY", "group": "llm", "label": "压缩 LLM API Key", "sensitive": True, **_masked("OMBRE_COMPRESS_API_KEY")},
        {"name": "OMBRE_COMPRESS_BASE_URL", "group": "llm", "label": "压缩 LLM Base URL", "sensitive": False, **_plain("OMBRE_COMPRESS_BASE_URL")},
        {"name": "OMBRE_COMPRESS_MODEL", "group": "llm", "label": "压缩 LLM 模型", "sensitive": False, **_plain("OMBRE_COMPRESS_MODEL")},
        # Embedding 组
        {"name": "OMBRE_EMBED_API_KEY", "group": "embed", "label": "向量化 API Key", "sensitive": True, **_masked("OMBRE_EMBED_API_KEY")},
        {"name": "OMBRE_EMBED_BASE_URL", "group": "embed", "label": "向量化 Base URL", "sensitive": False, **_plain("OMBRE_EMBED_BASE_URL")},
        {"name": "OMBRE_EMBED_MODEL", "group": "embed", "label": "向量化模型", "sensitive": False, **_plain("OMBRE_EMBED_MODEL")},
        # 服务配置组
        {"name": "OMBRE_TRANSPORT", "group": "system", "label": "传输模式", "sensitive": False, **_plain("OMBRE_TRANSPORT")},
        {"name": "OMBRE_PORT", "group": "system", "label": "服务端口", "sensitive": False, **_plain("OMBRE_PORT")},
        {"name": "OMBRE_LOG_FILE", "group": "system", "label": "日志文件路径", "sensitive": False, **_plain("OMBRE_LOG_FILE")},
        {"name": "OMBRE_CONFIG_PATH", "group": "system", "label": "配置文件路径", "sensitive": False, **_plain("OMBRE_CONFIG_PATH")},
        # 路径组
        {"name": "OMBRE_VAULT_DIR", "group": "paths", "label": "Vault 目录 (推荐)", "sensitive": False, **_plain("OMBRE_VAULT_DIR")},
        {"name": "OMBRE_BUCKETS_DIR", "group": "paths", "label": "桶目录 (旧版兼容)", "sensitive": False, **_plain("OMBRE_BUCKETS_DIR")},
        {"name": "OMBRE_HOST_VAULT_DIR", "group": "paths", "label": "宿主机 Vault 目录 (Docker)", "sensitive": False, **_plain("OMBRE_HOST_VAULT_DIR")},
        # Webhook 组
        {"name": "OMBRE_HOOK_URL", "group": "webhook", "label": "Webhook URL", "sensitive": False, **_plain("OMBRE_HOOK_URL")},
        {"name": "OMBRE_HOOK_SKIP", "group": "webhook", "label": "跳过 Webhook", "sensitive": False,
         "set": bool(os.environ.get("OMBRE_HOOK_SKIP", "").strip()),
         "value": os.environ.get("OMBRE_HOOK_SKIP", "").strip() or None},
        # 鉴权组
        {"name": "OMBRE_DASHBOARD_PASSWORD", "group": "auth", "label": "Dashboard 密码", "sensitive": True, **_masked("OMBRE_DASHBOARD_PASSWORD")},
    ]

    return JSONResponse({"vars": vars_data})


@mcp.custom_route("/api/config", methods=["GET"])
async def api_config_get(request: Request) -> Response:
    """Get current runtime config (safe fields only, API key masked)."""
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    dehy = config.get("dehydration", {})
    emb = config.get("embedding", {})
    api_key = dehy.get("api_key", "")
    masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else ("***" if api_key else "")
    return JSONResponse({
        "dehydration": {
            "model": dehy.get("model", ""),
            "base_url": dehy.get("base_url", ""),
            "api_key_masked": masked_key,
            "max_tokens": dehy.get("max_tokens", 1024),
            "temperature": dehy.get("temperature", 0.1),
            "api_format": dehy.get("api_format", "openai_compat"),
        },
        "embedding": {
            "enabled": emb.get("enabled", False),
            "model": emb.get("model", ""),
            "api_format": emb.get("api_format", "openai_compat"),
            "backend": "api",
            "backend_options": [
                {"value": "api", "label": "Gemini API（云端）", "note": "需填 OMBRE_EMBED_API_KEY，3072 维质量最高，需联网；客户端几乎不占额外内存"},
            ],
        },
        "surfacing": {
            "breath_max_results": int(config.get("surfacing", {}).get("breath_max_results") or 20),
            "breath_max_tokens": int(config.get("surfacing", {}).get("breath_max_tokens") or 10000),
            "feel_max_tokens": int(config.get("surfacing", {}).get("feel_max_tokens") or 6000),
        },
        "merge_threshold": config.get("merge_threshold", 75),
        "transport": config.get("transport", "stdio"),
        "buckets_dir": config.get("buckets_dir", ""),
    })


@mcp.custom_route("/api/config", methods=["POST"])
async def api_config_update(request: Request) -> Response:
    """Hot-update runtime config. Optionally persist to config.yaml."""
    global embedding_engine
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON"}, status_code=400)

    updated = []

    # --- Dehydration config ---
    if "dehydration" in body:
        d = body["dehydration"]
        dehy = config.setdefault("dehydration", {})
        for key in ("model", "base_url", "max_tokens", "temperature", "api_format"):
            if key in d:
                dehy[key] = d[key]
                updated.append(f"dehydration.{key}")
        if "api_key" in d and d["api_key"]:
            dehy["api_key"] = d["api_key"]
            updated.append("dehydration.api_key")
        # Hot-reload dehydrator — sync ALL attributes so dashboard changes take effect immediately
        dehydrator.model = dehy.get("model", dehydrator.model)
        dehydrator.base_url = dehy.get("base_url", dehydrator.base_url)
        dehydrator.max_tokens = int(dehy.get("max_tokens") or dehydrator.max_tokens)
        dehydrator.temperature = float(dehy.get("temperature") or dehydrator.temperature)
        dehydrator.api_format = dehy.get("api_format", getattr(dehydrator, "api_format", "openai_compat"))
        if "api_key" in d and d["api_key"]:
            dehydrator.api_key = dehy["api_key"]
        dehydrator.api_available = bool(dehydrator.api_key)
        # Rebuild OpenAI-compat client whenever key or url changes
        if dehydrator.api_available and dehydrator.api_format == "openai_compat":
            from openai import AsyncOpenAI
            dehydrator.client = AsyncOpenAI(
                api_key=dehydrator.api_key,
                base_url=dehydrator.base_url,
                timeout=60.0,
            )
        else:
            dehydrator.client = None

    # --- Embedding config ---
    if "embedding" in body:
        e = body["embedding"]
        emb = config.setdefault("embedding", {})
        if "enabled" in e:
            emb["enabled"] = bool(e["enabled"])
            embedding_engine.enabled = emb["enabled"]
            updated.append("embedding.enabled")
        if "model" in e:
            emb["model"] = e["model"]
            embedding_engine.model = emb["model"]
            if embedding_engine._backend:
                embedding_engine._backend.model = emb["model"]  # type: ignore[attr-defined]
            updated.append("embedding.model")
        if "api_format" in e:
            emb["api_format"] = str(e["api_format"]).strip()
            # 重建后端以应用新格式
            from embedding_engine import EmbeddingEngine as _EE
            embedding_engine = _EE(config)
            updated.append("embedding.api_format")
        if "backend" in e:
            new_backend_raw = str(e["backend"]).strip().lower()
            # 只支持 api backend，其他值直接拒绝
            new_backend = "api" if new_backend_raw in ("api", "gemini") else new_backend_raw
            if new_backend == "api":
                emb["backend"] = new_backend
                # 注意：这里仅热替换运行时引擎实例，不做 embeddings.db 迁移。
                # 如需重算所有向量，请显式调用 POST /api/embedding/migrate。
                from embedding_engine import EmbeddingEngine
                embedding_engine = EmbeddingEngine(config)
                updated.append("embedding.backend")

    # --- Merge threshold ---
    if "merge_threshold" in body:
        try:
            config["merge_threshold"] = int(body["merge_threshold"])
            updated.append("merge_threshold")
        except (TypeError, ValueError):
            pass

    # --- Surfacing defaults (breath/feel token & result caps) ---
    if "surfacing" in body and isinstance(body["surfacing"], dict):
        sf = config.setdefault("surfacing", {})
        for key, lo, hi in (
            ("breath_max_results", 1, 50),
            ("breath_max_tokens", 500, 20000),
            ("feel_max_tokens", 500, 20000),
        ):
            if key in body["surfacing"]:
                try:
                    val = int(body["surfacing"][key])
                    sf[key] = max(lo, min(hi, val))
                    updated.append(f"surfacing.{key}")
                except (TypeError, ValueError):
                    pass

    # --- Persist to config.yaml if requested ---
    if body.get("persist", False):
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
        try:
            save_config: dict[str, object] = {}
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    save_config = yaml.safe_load(f) or {}

            if "dehydration" in body:
                sc_dehy = save_config.setdefault("dehydration", {})
                if not isinstance(sc_dehy, dict):
                    sc_dehy = {}
                    save_config["dehydration"] = sc_dehy
                for key in ("model", "base_url", "max_tokens", "temperature", "api_format"):
                    if key in body["dehydration"]:
                        sc_dehy[key] = body["dehydration"][key]
                # Never persist api_key to yaml (use env var)

            if "embedding" in body:
                sc_emb = save_config.setdefault("embedding", {})
                if not isinstance(sc_emb, dict):
                    sc_emb = {}
                    save_config["embedding"] = sc_emb
                for key in ("enabled", "model", "api_format"):
                    if key in body["embedding"]:
                        sc_emb[key] = body["embedding"][key]

            if "merge_threshold" in body:
                try:
                    save_config["merge_threshold"] = int(body["merge_threshold"])
                except (TypeError, ValueError):
                    pass

            if "surfacing" in body and isinstance(body["surfacing"], dict):
                sc_sf = save_config.setdefault("surfacing", {})
                if not isinstance(sc_sf, dict):
                    sc_sf = {}
                    save_config["surfacing"] = sc_sf
                for key in ("breath_max_results", "breath_max_tokens", "feel_max_tokens"):
                    if key in body["surfacing"]:
                        try:
                            sc_sf[key] = int(body["surfacing"][key])
                        except (TypeError, ValueError):
                            pass
                if "sampling" in body["surfacing"] and isinstance(body["surfacing"]["sampling"], dict):
                    sc_samp = sc_sf.setdefault("sampling", {})
                    if not isinstance(sc_samp, dict):
                        sc_samp = {}
                        sc_sf["sampling"] = sc_samp
                    src_samp = body["surfacing"]["sampling"]
                    if "enabled" in src_samp:
                        sc_samp["enabled"] = bool(src_samp["enabled"])
                    for key in ("top_k", "sample_k"):
                        if key in src_samp:
                            try:
                                sc_samp[key] = int(src_samp[key])
                            except (TypeError, ValueError):
                                pass
                    if "temperature" in src_samp:
                        try:
                            sc_samp["temperature"] = float(src_samp["temperature"])
                        except (TypeError, ValueError):
                            pass

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(save_config, f, default_flow_style=False, allow_unicode=True)
            updated.append("persisted_to_yaml")
        except Exception as e:
            return JSONResponse({"error": f"persist failed: {e}", "updated": updated}, status_code=500)

    return JSONResponse({"updated": updated, "ok": True})


# =============================================================
# /api/test/dehydration — 测试脱水 LLM API Key 是否可用
# =============================================================
@mcp.custom_route("/api/test/dehydration", methods=["POST"])
async def api_test_dehydration(request: Request) -> Response:
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    # Use current runtime config (api_key may have been updated in-memory)
    dehyd = config.get("dehydration", {})
    model = dehyd.get("model", "")
    base_url = dehyd.get("base_url", "")
    api_key = dehyd.get("api_key", "")
    if not api_key:
        return JSONResponse({"ok": False, "error": "未设置 API Key"}, status_code=400)
    try:
        import httpx as _httpx
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
        async with _httpx.AsyncClient(timeout=15) as client:
            r = await client.post(f"{base_url.rstrip('/')}/chat/completions", json=payload, headers=headers)
        if r.status_code in (200, 201):
            return JSONResponse({"ok": True, "message": "API Key 有效 ✓"})
        else:
            try:
                detail = r.json().get("error", {})
                msg = detail.get("message", r.text[:200]) if isinstance(detail, dict) else str(detail)[:200]
            except Exception:
                msg = r.text[:200]
            return JSONResponse({"ok": False, "error": f"HTTP {r.status_code}: {msg}"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:300]})


# =============================================================
# /api/test/embedding — 测试向量化 Embedding 是否真的可用
# 之前只有脱水(compress)能测，向量化无从验证 → 用户「压缩正常但向量化静默失败」
# 时完全无感。这里实际发一次 embedding 请求，把成功/失败如实回给前端。(#2/#3)
# =============================================================
@mcp.custom_route("/api/test/embedding", methods=["POST"])
async def api_test_embedding(request: Request) -> Response:
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    eng = embedding_engine  # 读全局（Fix: env-config 保存后已正确重建）
    if not getattr(eng, "enabled", False) or getattr(eng, "_backend", None) is None:
        return JSONResponse({
            "ok": False,
            "error": "向量化未启用或缺 key（standby）。请填入 Embedding API Key 点「保存」后再测。",
        })
    try:
        vec = await eng._generate_async("connectivity probe / 连接性探针")
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"[:300]})
    if vec:
        model = getattr(eng, "model", "") or (
            eng._backend.model_name() if getattr(eng, "_backend", None) else "?"
        )
        return JSONResponse({
            "ok": True,
            "message": f"向量化连接成功 ✓（模型 {model}，维度 {len(vec)}）",
        })
    return JSONResponse({
        "ok": False,
        "error": "调用返回空向量：检查 model 名 / base_url / key 是否匹配该 provider"
                 "（如硅基流动 base_url=https://api.siliconflow.cn/v1、model=BAAI/bge-m3）。详见错误面板 OB-E001。",
    })


# =============================================================
# /api/models — 获取 LLM provider 可用模型列表（供 Dashboard 模型选择器使用）
# POST Body: {api_key, base_url, api_format}
# 支持 openai_compat / gemini / anthropic 三种格式
# =============================================================
@mcp.custom_route("/api/models", methods=["POST"])
async def api_list_models(request: Request) -> Response:
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)

    api_key = str(body.get("api_key", "")).strip()
    base_url = str(body.get("base_url", "")).strip()
    api_format = str(body.get("api_format", "openai_compat")).strip().lower()

    # Sentinel "__use_current__": use server-side key from dehydration config
    if api_key == "__use_current__":
        api_key = config.get("dehydration", {}).get("api_key", "")
        if not base_url:
            base_url = config.get("dehydration", {}).get("base_url", "")
        if not api_format or api_format == "openai_compat":
            api_format = config.get("dehydration", {}).get("api_format", "openai_compat")
    # Sentinel "__use_current_embed__": use server-side key from embedding config
    if api_key == "__use_current_embed__":
        api_key = config.get("embedding", {}).get("api_key", "")
        if not base_url:
            base_url = config.get("embedding", {}).get("base_url", "")

    if not api_key:
        return JSONResponse({"ok": False, "error": "需要 api_key（请先保存 API Key 或在输入框填入）"}, status_code=400)

    try:
        models: list[str] = []
        if api_format in ("gemini", "gemini_embed"):
            # gemini → generateContent models；gemini_embed → embedContent models
            method_filter = "embedContent" if api_format == "gemini_embed" else "generateContent"
            url = "https://generativelanguage.googleapis.com/v1beta/models"
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.get(url, params={"key": api_key, "pageSize": 200})
            r.raise_for_status()
            for m in r.json().get("models", []):
                if method_filter in m.get("supportedGenerationMethods", []):
                    models.append(m.get("name", "").replace("models/", ""))
        elif api_format == "anthropic":
            ant_base = base_url.rstrip("/") if base_url else "https://api.anthropic.com"
            headers = {"x-api-key": api_key, "anthropic-version": "2023-06-01"}
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.get(f"{ant_base}/v1/models", headers=headers)
            r.raise_for_status()
            models = [m.get("id", "") for m in r.json().get("data", []) if m.get("id")]
        else:  # openai_compat
            if not base_url:
                return JSONResponse({"ok": False, "error": "openai_compat 格式需要 base_url"}, status_code=400)
            headers_oai = {"Authorization": f"Bearer {api_key}"}
            async with httpx.AsyncClient(timeout=10.0) as c:
                r = await c.get(f"{base_url.rstrip('/')}/models", headers=headers_oai)
            r.raise_for_status()
            models = sorted(m.get("id", "") for m in r.json().get("data", []) if m.get("id"))
        return JSONResponse({"ok": True, "models": [m for m in models if m]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:300]})


# =============================================================
# /api/env-config — Dashboard 热更新环境变量（四块：Compress / Embed / Password / Webhook）
# GET  返回当前值（API key 脱敏）
# POST 批量更新：同时更新进程内 config + 写 .env 文件持久化
# =============================================================

# 哪些变量可以从 Dashboard 读写（不能出现在这里之外的变量）
_ENV_CONFIG_FIELDS: dict[str, dict] = {
    # Compress / 脱水压缩
    "OMBRE_COMPRESS_API_KEY":  {"group": "compress", "sensitive": True,  "in_memory": ("dehydration", "api_key")},
    "OMBRE_COMPRESS_BASE_URL": {"group": "compress", "sensitive": False, "in_memory": ("dehydration", "base_url")},
    "OMBRE_COMPRESS_MODEL":    {"group": "compress", "sensitive": False, "in_memory": ("dehydration", "model")},
    "OMBRE_COMPRESS_FORMAT":   {"group": "compress", "sensitive": False, "in_memory": ("dehydration", "api_format")},
    # Embed / 向量化（backend 切换走 /api/embedding/migrate）
    "OMBRE_EMBED_API_KEY":     {"group": "embed",    "sensitive": True,  "in_memory": ("embedding", "api_key")},
    "OMBRE_EMBED_BASE_URL":    {"group": "embed",    "sensitive": False, "in_memory": ("embedding", "base_url")},
    "OMBRE_EMBED_MODEL":       {"group": "embed",    "sensitive": False, "in_memory": ("embedding", "model")},
    "OMBRE_EMBED_FORMAT":      {"group": "embed",    "sensitive": False, "in_memory": ("embedding", "api_format")},
    # Webhook
    "OMBRE_HOOK_URL":          {"group": "webhook",  "sensitive": False, "in_memory": None},
    "OMBRE_HOOK_SKIP":         {"group": "webhook",  "sensitive": False, "in_memory": None},
}

_ENV_CONFIG_NOTE = {
    "compress": "改完即时生效（进程内 config 已更新），同时写 config.yaml 持久化（重启后仍有效）。",
    "embed": "API key / base_url / model 立即更新进程内 config；backend 切换请用「切换 / 重算所有 embedding…」按钮。",
    "webhook": "改完下次 breath/dream 触发时即生效，无需重启。",
}


def _mask(val: str) -> str:
    """对 API key 做脱敏，末 4 位保留供校验。"""
    if not val:
        return ""
    if len(val) > 8:
        return f"{val[:4]}...{val[-4:]}"
    return "***"


@mcp.custom_route("/api/env-config", methods=["GET"])
async def api_env_config_get(request: Request) -> Response:
    """
    返回四块配置的当前值（API key 脱敏显示）。
    优先读进程内 config / os.environ，其次读 .env 文件。
    """
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err

    result: dict[str, dict] = {}
    for var, meta in _ENV_CONFIG_FIELDS.items():
        # 优先从 config dict 读（进程内最新）
        raw = ""
        if meta["in_memory"]:
            section, key = meta["in_memory"]
            raw = str(config.get(section, {}).get(key, "")).strip()
        # 进程内为空，则读 os.environ
        if not raw:
            raw = os.environ.get(var, "").strip()
        # 再读 .env 文件
        if not raw:
            raw = _read_env_var(var)
        result[var] = {
            "group": meta["group"],
            "sensitive": meta["sensitive"],
            "value": _mask(raw) if meta["sensitive"] else raw,
            "is_set": bool(raw),
        }

    return JSONResponse({
        "ok": True,
        "fields": result,
        "notes": _ENV_CONFIG_NOTE,
    })


@mcp.custom_route("/api/env-config", methods=["POST"])
async def api_env_config_set(request: Request) -> Response:
    """
    热更新指定环境变量。

    Body (JSON): {"updates": {"OMBRE_COMPRESS_API_KEY": "sk-...", ...}}
    - 只写传入的字段，未传字段不动。
    - 空字符串 = 清除该变量（.env 里写成 NAME= ，进程内 config 设为 ""）。
    - API key 不支持 "***" 保持不变（应传实际值或空字符串）。

    成功返回 {ok, updated: [已写的变量名], .env 路径}。
    """
    # 必须声明 global：下面第 6 步会 `embedding_engine = EmbeddingEngine(config)` 重建实例。
    # 缺这行 → 该赋值把 embedding_engine 当函数局部变量，造成：
    #   1) 清 key 分支 `embedding_engine._backend = None` 触发 UnboundLocalError（被 except 吞掉 → 清 key 没真禁用）；
    #   2) 设新 key 时只更新局部，模块级全局仍指向旧引擎 → /api/embedding/info、search 等读全局处拿到旧/待机引擎，
    #      表现为「在 Dashboard 配了硅基流动等向量化却一直静默不生效」。
    global embedding_engine
    from starlette.responses import JSONResponse
    err = _require_auth(request)
    if err:
        return err

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)

    updates: dict = body.get("updates", {})
    if not isinstance(updates, dict) or not updates:
        return JSONResponse({"ok": False, "error": "updates 必须是非空对象"}, status_code=400)

    written: list[str] = []
    errors: list[str] = []

    for var, val in updates.items():
        if var not in _ENV_CONFIG_FIELDS:
            errors.append(f"{var}: 不在白名单里，跳过")
            continue
        if not isinstance(val, str):
            errors.append(f"{var}: 值必须是字符串，跳过")
            continue
        # 拒绝明显的注入字符
        if "\n" in val or "\r" in val:
            errors.append(f"{var}: 值不能含换行，跳过")
            continue

        value = val.strip()

        # OMBRE_HOOK_URL 只允许 http/https（防止意外配成 file:// 等非 HTTP scheme）
        if var == "OMBRE_HOOK_URL" and value and not value.startswith(("http://", "https://")):
            errors.append(f"{var}: 只允许 http:// 或 https:// 开头的 URL，跳过")
            continue

        # 1. 更新进程内 config dict（影响当次请求之后的业务逻辑）
        meta = _ENV_CONFIG_FIELDS[var]
        if meta["in_memory"]:
            section, key = meta["in_memory"]
            config.setdefault(section, {})[key] = value

        # 2. 更新 os.environ
        if value:
            os.environ[var] = value
        else:
            os.environ.pop(var, None)

        # 3. 持久化到 config.yaml（bind mount，重建不丢）
        try:
            _cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")
            _save: dict = {}
            if os.path.exists(_cfg_path):
                with open(_cfg_path, "r", encoding="utf-8") as _f:
                    _save = yaml.safe_load(_f) or {}
            if meta["in_memory"]:
                section, key = meta["in_memory"]
                _save.setdefault(section, {})[key] = value
            with open(_cfg_path, "w", encoding="utf-8") as _f:
                yaml.dump(_save, _f, allow_unicode=True, default_flow_style=False)
        except Exception as e:
            errors.append(f"{var}: 写 config.yaml 失败：{e}")
            continue

        # 4. Webhook 变量特殊处理：更新模块级全局
        # _fire_webhook 读的是模块级 OMBRE_HOOK_URL / OMBRE_HOOK_SKIP 常量（不是每次读 os.environ），
        # 必须在这里同步更新全局，否则 dashboard 改完要重启才生效。
        if var == "OMBRE_HOOK_URL":
            global OMBRE_HOOK_URL
            OMBRE_HOOK_URL = value
        if var == "OMBRE_HOOK_SKIP":
            global OMBRE_HOOK_SKIP
            OMBRE_HOOK_SKIP = value.lower() in ("1", "true", "yes", "on")

        # 5. Compress 配置变更 → 同步到 dehydrator 实例，重建 client
        if var in ("OMBRE_COMPRESS_API_KEY", "OMBRE_COMPRESS_BASE_URL", "OMBRE_COMPRESS_MODEL", "OMBRE_COMPRESS_FORMAT"):
            try:
                dehy_cfg = config.get("dehydration", {})
                dehydrator.api_key = dehy_cfg.get("api_key", dehydrator.api_key)  # type: ignore[attr-defined]
                dehydrator.base_url = dehy_cfg.get("base_url", dehydrator.base_url)  # type: ignore[attr-defined]
                dehydrator.model = dehy_cfg.get("model", dehydrator.model)  # type: ignore[attr-defined]
                dehydrator.api_format = dehy_cfg.get("api_format", getattr(dehydrator, "api_format", "openai_compat"))  # type: ignore[attr-defined]
                dehydrator.api_available = bool(dehydrator.api_key)  # type: ignore[attr-defined]
                if dehydrator.api_available and dehydrator.api_format == "openai_compat":  # type: ignore[attr-defined]
                    from openai import AsyncOpenAI as _OAI_DH
                    dehydrator.client = _OAI_DH(  # type: ignore[attr-defined]
                        api_key=dehydrator.api_key,
                        base_url=dehydrator.base_url,
                        timeout=60.0,
                    )
                else:
                    dehydrator.client = None  # type: ignore[attr-defined]
            except Exception:
                pass

        # 6. Embed 配置变更 → 完整重建 embedding_engine
        if var in ("OMBRE_EMBED_API_KEY", "OMBRE_EMBED_BASE_URL", "OMBRE_EMBED_MODEL", "OMBRE_EMBED_FORMAT"):
            try:
                config.setdefault("embedding", {})
                # key 被清空 → 禁用
                if var == "OMBRE_EMBED_API_KEY" and not value:
                    embedding_engine._backend = None  # type: ignore[attr-defined]
                    embedding_engine.enabled = False
                else:
                    from embedding_engine import EmbeddingEngine as _EE_hot
                    embedding_engine = _EE_hot(config)
                    # 更新 bucket_mgr / import_engine 持有的引用
                    try:
                        bucket_mgr.embedding_engine = embedding_engine  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    try:
                        import_engine.embedding_engine = embedding_engine  # type: ignore[attr-defined]
                    except Exception:
                        pass
            except Exception:
                pass

        written.append(var)

    response: dict = {
        "ok": True,
        "updated": written,
        "env_file": _project_env_path(),
        "note": "已同时更新进程内 config 和 config.yaml 文件。敏感字段（API key）重启后仍有效。",
    }
    if errors:
        response["warnings"] = errors
    return JSONResponse(response)


# =============================================================
# /api/host-vault — read/write the host-side OMBRE_HOST_VAULT_DIR
# 用于在 Dashboard 设置 docker-compose 挂载的宿主机记忆桶目录。
# 写入项目根目录的 .env 文件，需 docker compose down/up 才能生效。
# =============================================================

# .env 读写 helper 已移到 web/_shared.py（config/host-vault 共用）；import 回来供本文件仍未迁移的路由使用。
from web._shared import _project_env_path, _read_env_var, _write_env_var  # noqa: F401


# =============================================================
# /api/github/* —— 已拆分到 web/github.py
# =============================================================


# =============================================================
# /api/host-vault、/api/import/*、/api/bucket/{id}/edit、/api/export、/api/migrate/*
# —— 已拆分到 web/import_api.py
# =============================================================


# =============================================================
# /api/version、/api/update-info、/api/do-update、/api/author、
# /api/onboarding/status、/api/status —— 已拆分到 web/meta.py
# =============================================================


# ============================================================
# OAuth 2.0 — MCP Remote Auth —— 已拆分到 web/oauth.py（路由在其 register 内注册）。
# 这里仅把启动期 MCP 鉴权中间件要用的 _is_valid_mcp_token import 回来。
# ============================================================
from web.oauth import _is_valid_mcp_token  # noqa: F401  (used by _MCPAuthMiddleware below)


# ============================================================
# Cloudflare Tunnel 管理 —— 已拆分到 web/tunnel.py（路由在其 register 内注册）。
# 这里把启动/关停 lifespan 要用的 helper import 回来。
# ============================================================
from web.tunnel import _load_tunnel_config, _start_tunnel, _stop_tunnel  # noqa: F401


# --- Entry point / 启动入口 ---
if __name__ == "__main__":
    transport = config.get("transport", "stdio")
    logger.info(f"Ombre Brain starting | transport: {transport}")

    if transport in ("sse", "streamable-http"):
        import threading
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        # --- Application-level keepalive: ping /health every 60s ---
        # --- 应用层保活：每 60 秒 ping 一次 /health，防止 Cloudflare Tunnel 空闲断连 ---
        async def _keepalive_loop() -> None:
            await asyncio.sleep(10)  # Wait for server to fully start
            async with httpx.AsyncClient() as client:
                while True:
                    try:
                        await client.get(f"http://localhost:{OMBRE_PORT}/health", timeout=_HEALTH_PROBE_TIMEOUT_SECONDS)
                        logger.debug("Keepalive ping OK / 保活 ping 成功")
                    except Exception as e:
                        logger.warning(f"Keepalive ping failed / 保活 ping 失败: {e}")
                    await asyncio.sleep(60)

        def _start_keepalive() -> None:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_keepalive_loop())

        t = threading.Thread(target=_start_keepalive, daemon=True)
        t.start()

        # --- Add CORS middleware so remote clients (Cloudflare Tunnel / ngrok) can connect ---
        # --- 添加 CORS 中间件，让远程客户端（Cloudflare Tunnel / ngrok）能正常连接 ---
        if transport == "streamable-http":
            # iter 2.1：合并 mcp 主实例与 mcp_extra 副实例的 streamable_http_app。
            # 两个实例各自的 streamable_http_app() 会返回独立的 starlette app，
            # 内部分别只挂 /mcp 与 /mcp-extra 一条路由 + 各自的 SessionManager lifespan。
            # 这里把副实例的 routes 与 lifespan 合并进主实例，让一个 uvicorn 进程
            # 同时承载 /mcp、/mcp-extra 与所有 dashboard custom_route。
            import contextlib as _ctxlib
            _app = mcp.streamable_http_app()
            _extra_app = mcp_extra.streamable_http_app()
            _main_lifespan = _app.router.lifespan_context
            _extra_lifespan = _extra_app.router.lifespan_context

            @_ctxlib.asynccontextmanager
            async def _combined_lifespan(app):
                async with _main_lifespan(app):
                    async with _extra_lifespan(app):
                        # Auto-start tunnel if configured
                        _tcfg = _load_tunnel_config()
                        if _tcfg.get("auto_start") and _tcfg.get("token"):
                            _ok, _msg = _start_tunnel(_tcfg["token"])
                            logger.info(f"Tunnel auto-start: {_msg}")
                        # Auto-start GitHub sync loop if configured
                        if _gh_auto_interval > 0:
                            _restart_github_auto_task(_gh_auto_interval)
                        yield
                        _stop_tunnel()

            _app.router.lifespan_context = _combined_lifespan
            _app.routes.extend(_extra_app.routes)
            logger.info(
                "MCP split / MCP 拆分：主连接器 /mcp（5 高频工具）+ 副连接器 /mcp-extra（6 低频工具）"
            )
        else:
            _app = mcp.sse_app()
        _app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        logger.info("CORS middleware enabled for remote transport / 已启用 CORS 中间件")

        # MCP Bearer token auth — pure ASGI middleware (no response buffering)
        # BaseHTTPMiddleware buffers SSE streams and breaks MCP tool listing
        import json as _json_mw

        # config.yaml: mcp_require_auth: false → 完全跳过 OAuth 检查，
        # 任何客户端（GPT / GLM / 自定义前端）可免认证直连 /mcp。
        # 不填或 true → 保持默认：必须 OAuth Bearer token。
        _mcp_auth_required = bool(config.get("mcp_require_auth", True))

        class _MCPAuthMiddleware:
            def __init__(self, app):
                self.app = app

            async def __call__(self, scope, receive, send):
                if scope["type"] == "http" and _mcp_auth_required:
                    path = scope.get("path", "")
                    if path.startswith("/mcp"):
                        headers = {k.lower(): v for k, v in scope.get("headers", [])}
                        auth = headers.get(b"authorization", b"").decode("latin-1")
                        if not (auth.startswith("Bearer ") and _is_valid_mcp_token(auth[7:])):
                            # Build public base URL from ASGI scope headers
                            proto = headers.get(b"x-forwarded-proto", b"").decode() or scope.get("scheme", "http")
                            host = (headers.get(b"x-forwarded-host") or headers.get(b"host", b"")).decode()
                            base = f"{proto}://{host}"
                            ww_auth = (
                                f'Bearer realm="Ombre Brain",'
                                f' resource_metadata="{base}/.well-known/oauth-protected-resource"'
                            )
                            body = _json_mw.dumps({
                                "error": "Unauthorized",
                                "resource_metadata": f"{base}/.well-known/oauth-protected-resource",
                            }).encode()
                            await send({"type": "http.response.start", "status": 401, "headers": [
                                [b"content-type", b"application/json"],
                                [b"www-authenticate", ww_auth.encode()],
                                [b"content-length", str(len(body)).encode()],
                            ]})
                            await send({"type": "http.response.body", "body": body, "more_body": False})
                            return
                await self.app(scope, receive, send)

        _app.add_middleware(_MCPAuthMiddleware)
        if _mcp_auth_required:
            logger.info("MCP OAuth middleware enabled / MCP OAuth 中间件已启用")
        else:
            logger.info("MCP auth disabled (mcp_require_auth: false) — open access / MCP 认证已关闭，所有客户端可直连")
        uvicorn.run(_app, host="0.0.0.0", port=OMBRE_PORT)
    else:
        # stdio / sse：单连接器无 5 工具上限，把 mcp_extra 的工具回灌到 mcp
        # 让所有 11 个工具仍在同一连接器里暴露（兼容旧 Claude Desktop 配置）。
        # 依赖 FastMCP._tool_manager 私有结构；若未来版本变化，回退为只暴露主集 5 工具。
        try:
            mcp._tool_manager._tools.update(mcp_extra._tool_manager._tools)
            logger.info(
                f"stdio/sse 单连接器模式：已回灌 {len(mcp_extra._tool_manager._tools)} 个副集工具"
            )
        except AttributeError as e:
            logger.warning(
                f"FastMCP 内部结构变化，stdio 模式仅暴露主集 5 工具：{e}"
            )
        mcp.run(transport=transport)
