"""
========================================
web/embedding.py — 向量化后端摘要 / 迁移重算 / 本地 Ollama 模型管理
========================================
- /api/embedding/info、/api/embedding/migrate(+status)、/api/embedding/local/*
- 迁移成功后热替换 sh.embedding_engine + sh.bucket_mgr/sh.import_engine 引用，全局一致。
对外暴露：register(mcp)。
========================================
"""

import os
import httpx
import json as _json_lib
import yaml

from starlette.requests import Request
from starlette.responses import Response

from . import _shared as sh

logger = sh.logger

try:
    from errors import OBStartupError  # type: ignore
except ImportError:  # pragma: no cover
    from ..errors import OBStartupError  # type: ignore


def _persist_embedding_yaml(updates: dict) -> None:
    """把 embedding 配置写进 sh.config.yaml（bind mount，重启/重建不丢）。

    迁移完成后必须调用：否则切到本地/云端只改了进程内 sh.config，重启后 sh.config.yaml
    还是旧的 → 与 embeddings.db 里已重算的向量维度不一致 → OB-W005 / 检索失效。
    """
    try:
        _cfg_path = os.path.join(sh.repo_root, "config.yaml")
        _save: dict = {}
        if os.path.exists(_cfg_path):
            with open(_cfg_path, "r", encoding="utf-8") as _f:
                _save = yaml.safe_load(_f) or {}
        _sec = _save.setdefault("embedding", {})
        for k, v in updates.items():
            _sec[k] = v
        with open(_cfg_path, "w", encoding="utf-8") as _f:
            yaml.dump(_save, _f, allow_unicode=True, default_flow_style=False)
    except Exception as e:
        logger.error(f"[migration] persist embedding to sh.config.yaml failed: {e}")


_DEFAULT_OLLAMA_BASE = "http://ombre-ollama:11434"
# 模型下载镜像前缀（registry）。空 = ollama 官方。国内慢/不通时可换。
_OLLAMA_MIRRORS = {
    "official": "",
    "modelscope": "modelscope.cn/",   # 形如 modelscope.cn/<ns>/bge-m3，需该源确有此模型
}

_ollama_pull_state: dict = {"running": False, "model": "", "percent": 0, "status": "idle", "error": ""}
_ollama_pull_task: "asyncio.Task | None" = None  # 持有引用防止被 GC


def _ollama_base() -> str:
    """Ollama 管理 API 根地址（不带 /v1）。"""
    raw = (os.environ.get("OMBRE_OLLAMA_URL", "") or "").strip() or _DEFAULT_OLLAMA_BASE
    return raw.rstrip("/").removesuffix("/v1").rstrip("/")


async def _ollama_pull_run(ollama_url: str, name: str) -> None:
    """后台流式拉模型，进度写入 _ollama_pull_state。"""
    global _ollama_pull_state
    _ollama_pull_state = {"running": True, "model": name, "percent": 0, "status": "starting", "error": ""}
    try:
        async with httpx.AsyncClient(timeout=None) as c:
            async with c.stream("POST", f"{ollama_url}/api/pull", json={"name": name, "stream": True}) as r:
                if r.status_code != 200:
                    raw = await r.aread()
                    _ollama_pull_state.update(running=False, status="error",
                                              error=f"HTTP {r.status_code}: {raw[:200].decode('utf-8','replace')}")
                    return
                async for line in r.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        ev = _json_lib.loads(line)
                    except Exception:
                        continue
                    if ev.get("error"):
                        _ollama_pull_state.update(running=False, status="error", error=str(ev["error"])[:200])
                        return
                    st = str(ev.get("status", ""))
                    _ollama_pull_state["status"] = st
                    total, completed = ev.get("total"), ev.get("completed")
                    if total and completed:
                        try:
                            _ollama_pull_state["percent"] = round(completed / total * 100, 1)
                        except Exception:
                            pass
                    if st == "success":
                        _ollama_pull_state.update(running=False, status="success", percent=100)
                        return
        _ollama_pull_state["running"] = False
    except Exception as e:
        _ollama_pull_state.update(running=False, status="error", error=str(e)[:200])


def register(mcp) -> None:

    @mcp.custom_route("/api/embedding/info", methods=["GET"])
    async def api_embedding_info(request: Request) -> Response:
        """返回当前 embedding 后端的运行态摘要：backend / model / dim / enabled / db 状态。

        前端设置页用这个渲染「当前模型」面板。
        """
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err
        backend_obj = getattr(sh.embedding_engine, "_backend", None)
        info: dict[str, object] = {
            "ok": True,
            "backend": getattr(sh.embedding_engine, "backend", ""),
            "enabled": bool(getattr(sh.embedding_engine, "enabled", False)),
            "model": backend_obj.model_name() if backend_obj else "",
            "vector_dim": backend_obj.vector_dim() if backend_obj else 0,
            "db_path": getattr(sh.embedding_engine, "db_path", ""),
            "db_count": 0,
            "db_meta": {},
        }
        # 主表行数
        try:
            import sqlite3
            if info["db_path"] and os.path.exists(str(info["db_path"])):
                conn = sqlite3.connect(str(info["db_path"]))
                try:
                    info["db_count"] = conn.execute(
                        "SELECT COUNT(*) FROM embeddings"
                    ).fetchone()[0]
                    rows = conn.execute(
                        "SELECT key, value FROM embeddings_meta"
                    ).fetchall()
                    info["db_meta"] = {k: v for k, v in rows}
                finally:
                    conn.close()
        except Exception as e:
            info["db_error"] = str(e)
        return JSONResponse(info)

    @mcp.custom_route("/api/embedding/migrate", methods=["POST"])
    async def api_embedding_migrate(request: Request) -> Response:
        """启动后台迁移任务：用目标后端重算所有 bucket 的 embedding。

        Body (JSON):
            target_backend: 'api' | 'gemini' | 'local' | 'ollama'（底层都映射到 backend=api）
            api_format:     可选 'gemini' | 'openai_compat' | 'ollama'
            api_key:        云端必填；本地（ollama）可空，引擎会补占位符
            base_url:       可选
            model:          可选

        成功启动返回 202，body 含 {ok, status_path}；
        已有任务在跑返回 409。
        """
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err

        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid JSON"}, status_code=400)

        target_backend_raw = str(body.get("target_backend", "")).strip().lower()
        # local/ollama 底层也是 openai_compat（backend=api），用 api_format 区分云端/本地
        target_backend = "api" if target_backend_raw in ("api", "gemini", "local", "ollama", "") else target_backend_raw
        if target_backend != "api":
            return JSONResponse({
                "ok": False,
                "error": f"target_backend 不支持：{target_backend_raw!r}",
            }, status_code=400)

        # 解析目标 api_format：显式传入优先；否则按 target_backend 推断
        req_api_format = str(body.get("api_format", "")).strip().lower()
        if not req_api_format:
            if target_backend_raw in ("local", "ollama"):
                req_api_format = "ollama"
            elif target_backend_raw == "gemini":
                req_api_format = "gemini"

        try:
            from migration_engine import (  # type: ignore
                MigrationConfig, start_migration, is_running,
                status_path_for as _mig_status_path_for,
            )
        except ImportError:
            from .migration_engine import (  # type: ignore
                MigrationConfig, start_migration, is_running,
                status_path_for as _mig_status_path_for,
            )

        if is_running():
            return JSONResponse({
                "ok": False,
                "error": "另一个迁移任务正在进行；请稍后再试或等其完成",
            }, status_code=409)

        # 构造目标引擎（不替换 global，跑完才替）
        target_cfg = _json_lib.loads(_json_lib.dumps(sh.config))  # 深拷贝
        target_emb_cfg = target_cfg.setdefault("embedding", {})
        target_emb_cfg["enabled"] = True
        target_emb_cfg["backend"] = target_backend
        if req_api_format:
            target_emb_cfg["api_format"] = req_api_format
        if body.get("api_key"):
            target_emb_cfg["api_key"] = str(body["api_key"]).strip()
        if body.get("base_url"):
            target_emb_cfg["base_url"] = str(body["base_url"]).strip()
        if body.get("model"):
            target_emb_cfg["model"] = str(body["model"]).strip()

        try:
            from sh.embedding_engine import EmbeddingEngine  # type: ignore
        except ImportError:
            from .embedding_engine import EmbeddingEngine  # type: ignore
        try:
            target_engine = EmbeddingEngine(target_cfg)
        except OBStartupError as oe:
            return JSONResponse({
                "ok": False,
                "error": f"目标引擎构造失败：{oe.error_code} {oe.detail}",
            }, status_code=400)
        except Exception as e:
            return JSONResponse({
                "ok": False,
                "error": f"目标引擎构造失败：{type(e).__name__}: {e}",
            }, status_code=400)

        target_backend_obj = getattr(target_engine, "_backend", None)

        # 预检（fail-fast）：先用目标引擎试嵌入一小段，确认后端真的可用，
        # 再决定要不要启动全库重算。否则切到本地但 bge-m3 没下载 / ollama 没起，
        # 会让 392 个桶逐个失败几分钟才发现 —— 体验极差。
        if target_backend_obj is None or not getattr(target_engine, "enabled", False):
            return JSONResponse({
                "ok": False,
                "error": "目标 embedding 引擎不可用（可能缺 key / 本地模型未就绪）。本地模式请先在「本地向量模型」面板下载 bge-m3。",
            }, status_code=400)
        try:
            _probe = await target_engine._generate_async("connectivity probe / 连接性探针")
        except Exception as e:
            _probe = []
            _probe_err = f"{type(e).__name__}: {e}"
        else:
            _probe_err = ""
        if not _probe:
            _hint = "本地模式：确认 ollama 容器在跑且 bge-m3 已下载（设置页「本地向量模型」面板）。" \
                if req_api_format in ("ollama", "local") else "云端模式：确认 API key / base_url / 网络可用。"
            return JSONResponse({
                "ok": False,
                "error": f"目标后端嵌入测试失败，已取消重算（不会动现有向量）。{_hint}" + (f"（{_probe_err}）" if _probe_err else ""),
            }, status_code=400)

        # 准备桶内容供给函数
        async def _fetch_buckets() -> list[tuple[str, str]]:
            all_buckets = await sh.bucket_mgr.list_all(include_archive=True)
            return [(b["id"], b["content"]) for b in all_buckets]

        buckets_dir = sh.config.get("buckets_dir", "buckets")
        db_path = getattr(sh.embedding_engine, "db_path", "")

        mig_cfg = MigrationConfig(
            buckets_dir=buckets_dir,
            db_path=db_path,
            target_backend=target_backend,
            target_model=target_backend_obj.model_name() if target_backend_obj else "",
            target_dim=target_backend_obj.vector_dim() if target_backend_obj else 0,
            target_engine=target_engine,
            fetch_buckets=_fetch_buckets,
        )

        def _on_complete(success: bool) -> None:
            if not success:
                logger.warning("[migration] task finished with failures; sh.embedding_engine NOT swapped")
                return
            # 成功 → 把 global engine 切到目标
            try:
                sh.embedding_engine = target_engine
                # sh.bucket_mgr / sh.import_engine 持有的引用更新
                try:
                    sh.bucket_mgr.embedding_engine = target_engine
                except Exception:
                    pass
                try:
                    sh.import_engine.embedding_engine = target_engine
                except Exception:
                    pass
                # 持久化到 sh.config（进程内 + sh.config.yaml，重启/重建不丢）
                cfg_emb = sh.config.setdefault("embedding", {})
                cfg_emb["backend"] = target_backend
                cfg_emb["enabled"] = True
                _yaml_updates: dict = {"backend": target_backend, "enabled": True}
                if req_api_format:
                    cfg_emb["api_format"] = req_api_format
                    _yaml_updates["api_format"] = req_api_format
                if body.get("api_key"):
                    cfg_emb["api_key"] = str(body["api_key"]).strip()
                    _yaml_updates["api_key"] = str(body["api_key"]).strip()
                if body.get("base_url"):
                    cfg_emb["base_url"] = str(body["base_url"]).strip()
                    _yaml_updates["base_url"] = str(body["base_url"]).strip()
                if body.get("model"):
                    cfg_emb["model"] = str(body["model"]).strip()
                    _yaml_updates["model"] = str(body["model"]).strip()
                _persist_embedding_yaml(_yaml_updates)
                logger.info(f"[migration] sh.embedding_engine swapped to backend={target_backend} format={req_api_format or '(unchanged)'}; persisted to sh.config.yaml")
            except Exception as e:
                logger.error(f"[migration] post-swap failed: {e}")

        task = start_migration(mig_cfg, on_complete=_on_complete)
        if task is None:
            return JSONResponse({
                "ok": False,
                "error": "无法启动迁移任务（锁未获得）",
            }, status_code=409)

        return JSONResponse({
            "ok": True,
            "status_path": _mig_status_path_for(buckets_dir),
            "target_backend": target_backend,
        }, status_code=202)

    @mcp.custom_route("/api/embedding/migrate/status", methods=["GET"])
    async def api_embedding_migrate_status(request: Request) -> Response:
        """前端 3s 轮询：当前迁移任务状态。"""
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err
        try:
            from migration_engine import (  # type: ignore
                status_path_for as _mig_status_path_for,
                read_status as _mig_read_status,
                is_running,
            )
        except ImportError:
            from .migration_engine import (  # type: ignore
                status_path_for as _mig_status_path_for,
                read_status as _mig_read_status,
                is_running,
            )
        buckets_dir = sh.config.get("buckets_dir", "buckets")
        status = _mig_read_status(_mig_status_path_for(buckets_dir))
        return JSONResponse({"ok": True, "running": is_running(), "status": status})

    @mcp.custom_route("/api/embedding/local/status", methods=["GET"])
    async def api_embedding_local_status(request: Request) -> Response:
        """本地 ollama 是否可达 + 已有模型列表 + 目标模型是否就绪。"""
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err
        want = (request.query_params.get("model") or "bge-m3").strip()
        base = _ollama_base()
        out = {"ok": True, "ollama_url": base, "reachable": False, "models": [], "has_model": False, "mirrors": list(_OLLAMA_MIRRORS.keys())}
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                r = await c.get(f"{base}/api/tags")
                r.raise_for_status()
                names = [m.get("name", "") for m in r.json().get("models", [])]
                out["reachable"] = True
                out["models"] = names
                # ollama 模型名常带 :latest 后缀
                out["has_model"] = any(n == want or n.split(":")[0] == want for n in names)
        except Exception as e:
            out["error"] = str(e)[:160]
        out["pull"] = _ollama_pull_state
        return JSONResponse(out)

    @mcp.custom_route("/api/embedding/local/pull", methods=["POST"])
    async def api_embedding_local_pull(request: Request) -> Response:
        """触发后台拉模型。body: {model?: 'bge-m3', mirror?: 'official'|'modelscope'|<自定义前缀>}。"""
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err
        if _ollama_pull_state.get("running"):
            return JSONResponse({"ok": False, "error": "已有拉取任务在进行中"}, status_code=409)
        try:
            body = await request.json()
        except Exception:
            body = {}
        model = (str(body.get("model") or "bge-m3")).strip()
        mirror_raw = (str(body.get("mirror") or "official")).strip()
        prefix = _OLLAMA_MIRRORS.get(mirror_raw, mirror_raw if mirror_raw not in ("", "official") else "")
        name = f"{prefix}{model}" if prefix else model
        base = _ollama_base()
        # 可达性预检，避免后台任务静默失败
        try:
            async with httpx.AsyncClient(timeout=5.0) as c:
                vr = await c.get(f"{base}/api/version")
                vr.raise_for_status()
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"无法连接 ollama（{base}）：{str(e)[:120]}"}, status_code=502)
        import asyncio as _aio
        global _ollama_pull_task
        _ollama_pull_task = _aio.create_task(_ollama_pull_run(base, name))
        return JSONResponse({"ok": True, "started": True, "pulling": name})

    @mcp.custom_route("/api/embedding/local/pull/status", methods=["GET"])
    async def api_embedding_local_pull_status(request: Request) -> Response:
        from starlette.responses import JSONResponse
        err = sh._require_auth(request)
        if err:
            return err
        return JSONResponse({"ok": True, "pull": _ollama_pull_state})
