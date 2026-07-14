# Ombre Brain 全环境变量清单

本文是环境变量名称的唯一文档真源。代码、Docker、Zeabur 模板和 README 只能引用这里已经登记的名称；禁止静默改名。确需更名时，旧名必须保留兼容映射、弃用提示和迁移说明。

## 模型与向量

- `OMBRE_COMPRESS_API_KEY`：打标、脱水和合并模型密钥。
- `OMBRE_COMPRESS_BASE_URL`：OpenAI 兼容模型地址。
- `OMBRE_COMPRESS_MODEL`：打标模型名。
- `OMBRE_COMPRESS_FORMAT`：打标 API 格式。
- `OMBRE_COMPRESS_API_FORMAT`：`OMBRE_COMPRESS_FORMAT` 的旧兼容名。
- `OMBRE_COMPRESS_TIMEOUT_SECONDS`：打标请求超时秒数。
- `OMBRE_EMBED_API_KEY`：向量模型密钥。
- `OMBRE_EMBED_BASE_URL`：向量 API 地址。
- `OMBRE_EMBED_MODEL`：向量模型名。
- `OMBRE_EMBED_FORMAT`：向量 API 格式。
- `OMBRE_EMBED_TIMEOUT_SECONDS`：向量请求超时秒数。
- `OMBRE_EMBED_BACKEND`：向量后端，例如 `api` 或 `local`。
- `OMBRE_OLLAMA_URL`：本地 Ollama 地址。

## 存储、媒体与日志

- `OMBRE_VAULT_DIR`：推荐的数据根目录。
- `OMBRE_BUCKETS_DIR`：`OMBRE_VAULT_DIR` 的旧兼容名。
- `OMBRE_MEDIA_DIR`：永久媒体目录；默认 `<数据根目录>/_media`。
- `OMBRE_MEDIA_MAX_BYTES`：单个媒体文件最大字节数，默认 25 MiB。
- `OMBRE_CONFIG_PATH`：持久配置文件路径。
- `OMBRE_CODE_DIR`：容器中持久运行代码目录。
- `OMBRE_LOG_DIR`：日志目录。
- `OMBRE_LOG_FILE`：日志文件路径。
- `OMBRE_EXTERNAL_CHANGE_POLL_SECONDS`：外部 Markdown 变动轮询间隔。

## HTTP、MCP 与鉴权

- `OMBRE_TRANSPORT`：`stdio`、`sse` 或 `streamable-http`。
- `OMBRE_PORT`：容器或裸机监听端口。
- `OMBRE_BIND_HOST`：监听主机名。
- `OMBRE_MCP_REQUIRE_AUTH`：是否要求 MCP 鉴权。
- `OMBRE_MCP_AUTH_MODE`：`oauth`、`token` 或 `off`。
- `OMBRE_MCP_TOKEN`：静态 Token 模式密钥。
- `OMBRE_DASHBOARD_PASSWORD`：Dashboard 密码。
- `OMBRE_DASHBOARD_SESSION_DAYS`：Dashboard 登录会话天数。
- `OMBRE_TRUSTED_PROXY_CIDRS`：可信反向代理网段。

## Tunnel、Hook 与 GitHub

- `OMBRE_GITHUB_TOKEN`：GitHub 备份或更新访问令牌。
- `OMBRE_HOOK_URL`：外部 Hook 地址。
- `OMBRE_HOOK_TOKEN`：Hook 鉴权令牌。
- `OMBRE_HOOK_SKIP`：跳过 Hook。
- `OMBRE_HOOK_ALLOW_PUBLIC`：允许公网 Hook 地址。
- `TUNNEL_EDGE`：cloudflared 边缘节点覆盖值。
- `TUNNEL_TRANSPORT_PROTOCOL`：cloudflared 传输协议。

## 更新与容器维护

- `OMBRE_ALLOW_CUSTOM_UPDATE_REPO`：允许自定义更新仓库。
- `OMBRE_ALLOW_UNTRUSTED_MIRROR`：允许未受信任镜像源。
- `OMBRE_UPDATE_ALLOW_PIP`：允许热更新执行 pip。
- `OMBRE_FORCE_CODE_RESEED`：下次启动强制从镜像重播代码；使用后应移除。
- `OMBRE_IMAGE_ROOT`：镜像内置代码根目录。
- `OMBRE_BOOTSTRAP_ONLY`：仅执行启动引导和诊断。
- `OMBRE_DOCKER_INTEGRATION_URL`：Docker 集成服务地址。
- `OMBRE_DOCKER_WEB_BASE_URL`：Docker Web 基地址。

## 部署编排与多所有者

- `OMBRE_BIND_ADDRESS`：Compose 对外绑定地址。
- `OMBRE_HOST_PORT`：Compose 宿主机端口。
- `OMBRE_HOST_VAULT_DIR`：Compose 宿主机数据目录。
- `OMBRE_CONTAINER_NAME`：目标容器名。
- `OMBRE_OWNER_NAME`：当前所有者名。
- `OMBRE_OWNER_COUNT`：多所有者实例数。
- `OMBRE_MING_VAULT_DIR`、`OMBRE_HONG_VAULT_DIR`：示例多所有者数据目录。
- `OMBRE_MING_PASSWORD`、`OMBRE_HONG_PASSWORD`：示例多所有者密码。
- `AI_NAME`：AI 显示名称。

## v1.x 兼容变量

这些旧名仍然生效，但新部署应使用右侧正式名称：

- `OMBRE_API_KEY` → `OMBRE_COMPRESS_API_KEY`
- `OMBRE_BASE_URL` → `OMBRE_COMPRESS_BASE_URL`
- `PASSWORD` → `OMBRE_DASHBOARD_PASSWORD`

正式名称和旧名同时存在时，以正式名称为准。删除兼容名必须经过一次明确的主版本迁移，并在发布说明中写出截止版本，禁止在普通重构中删除。
