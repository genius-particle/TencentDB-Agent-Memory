# tdai-memory Helm chart

把合并前的 `deploy/k3s-single` 工作负载（Namespace 用 Helm `-n`、Secret、ConfigMap、PVC、三件套 Deployment、init-admin Job、Traefik Ingress）收成 chart，并接上本 fork 已合入的 PR：

| PR | 合入点 | Chart 里怎么用 |
|---|---|---|
| #3 | `5695d7b` | 可插拔存储；`store.mode` 选后端 |
| #4 | `aff03af` | `STORE_MODE=postgres`；对象存储默认不设 `STORAGE_BACKEND`（本地盘）；MinIO 需 `--set minio.deploy=true` |
| #5 | `1c78dc4` | `TDAI_METADATA_BACKEND=postgres`（与 memory 共用同一 Postgres，schema 分开） |
| #7 | `3b91d11` | 默认镜像 `ghcr.io/genius-particle/tencentdb-agent-memory/*:feat-server-team` |

`TDAI_DEPLOY_MODE` 固定 `standalone`。这不是官方 `deployMode=service`（TCVDB+COS+Mongo）路径；`DATABASE_URL` 单独存在也不会把 metadata 切到 Postgres，必须显式 `TDAI_METADATA_BACKEND=postgres`。

## 安装

默认走开源栈（Postgres+pgvector 记忆 + Postgres metadata + Redis 状态后端 + 本地盘对象存储 + GHCR；`STATE_BACKEND=redis`，不设 `STORAGE_BACKEND`）：

```bash
helm install tdai-memory deploy/helm/tdai-memory \
  -n tdai-memory --create-namespace \
  --set llm.baseUrl=https://api.deepseek.com/v1 \
  --set llm.apiKey=sk-... \
  --set llm.model=deepseek-chat \
  --set proxy.upstream.url=https://api.deepseek.com/v1 \
  --set proxy.upstream.apiKey=sk-... \
  --set proxy.upstream.model=deepseek-chat \
  --set embedding.apiKey=sk-... \
  --set ingress.host=tdai.192.168.11.170.nip.io \
  --set public.host=192.168.11.170
```

`values-eodev.yaml`：170 并列安装（`-f values-eodev.yaml`，namespace `tdai-open`）：

```bash
helm upgrade --install tdai-open deploy/helm/tdai-memory \
  -n tdai-open --create-namespace \
  -f deploy/helm/tdai-memory/values-eodev.yaml \
  --set llm.apiKey=... --set proxy.upstream.apiKey=... --set embedding.apiKey=...
```

Redis 镜像固定青岛仓；core/hub/proxy 默认 GHCR。拉取失败时可钉 tag 或改 registry：

```bash
--set image.tag=c020f22
# 或改回青岛 mirror（示例）:
# --set image.registry=registry.cn-qingdao.aliyuncs.com/eo \
# --set image.core.repository=ghcr.io.genius-particle.tencentdb-agent-memory.memory-core \
# ...
```

复现合并前 k3s-single 形态（SQLite、无 Postgres，仍用 GHCR 新镜像）：

```bash
helm install tdai-memory deploy/helm/tdai-memory \
  -n tdai-memory --create-namespace \
  -f deploy/helm/tdai-memory/values-sqlite.yaml \
  -f deploy/helm/tdai-memory/values-k3s-single.yaml \
  --set llm.baseUrl=... --set llm.apiKey=... --set llm.model=... \
  --set proxy.upstream.url=... --set proxy.upstream.apiKey=... --set proxy.upstream.model=... \
  --set embedding.apiKey=...
```

`values-eodev.yaml`：170 上与官方 `tdai-memory` 并列（namespace `tdai-open`）。**Redis** 走青岛仓 `registry.cn-qingdao.aliyuncs.com/eo/redis:7-alpine`；**三件套** 先试 GHCR `ghcr.io/genius-particle/tencentdb-agent-memory/*:feat-server-team`（`pullPolicy: Always`）。Postgres 用 ParadeDB 青岛 mirror；Panel `tdai-open.192.168.11.170.nip.io`。GHCR 401 时再改青岛 mirror 或配置 `imagePullSecrets`。`STORE_MODE=postgres` 下 Skill 模块可用（`PostgresSkillStore`，同 schema），无需 SQLite overlay。

不要用 `http://192.168.11.170/` 当管理端；Traefik 裸 IP 是 404。Panel 走 `http://tdai.192.168.11.170.nip.io/`。

## 校验

```bash
helm lint deploy/helm/tdai-memory
helm template tdai-memory deploy/helm/tdai-memory --debug >/tmp/tdai.yaml
kubectl -n tdai-memory get pods,svc,ingress
curl -sS http://127.0.0.1:30420/health          # NodePort 在节点上
```

登录 Panel 用 Secret 里的 `ADMIN_USER_KEY`（默认 `sk-mem-LocalTrialAdminKeyChangeMe000001`）。`init-admin` 对 HTTP 409（admin 已存在）视为成功，升级可重复跑。

Claude Code：

```bash
export ANTHROPIC_BASE_URL=http://192.168.11.170:30096/claude-code/default
export ANTHROPIC_AUTH_TOKEN='sk-mem-LocalTrialAdminKeyChangeMe000001'
```

## 镜像

默认 `pullPolicy: Always`，跟随 GHCR 移动 tag `feat-server-team`。包若是 private，先 `docker login ghcr.io` 并设 `imagePullSecrets`。

钉死某次构建：

```bash
--set image.tag=<short-sha>
```

仍可用青岛仓的 1.0.1（没有 PR #4/#5 开源后端）：

```bash
--set image.registry=registry.cn-qingdao.aliyuncs.com/eo \
--set image.core.repository=agentmemory.memory-core \
--set image.hub.repository=agentmemory.memory-hub \
--set image.proxy.repository=agentmemory.memory-proxy \
--set image.tag=1.0.1 \
--set image.pullPolicy=IfNotPresent \
-f deploy/helm/tdai-memory/values-sqlite.yaml
```

## 与 k3s-single 的差异

- 不再把 LLM / embedding Key 写进 ConfigMap；embedding 用 `${EMBEDDING_API_KEY}` 运行时展开。
- 默认部署 Redis（`redis.deploy=true`）并设 `STATE_BACKEND=redis`；实现位于公开 `MemoryCore/src/core/state/redis-backend.ts`。
- SQLite overlay（`values-sqlite.yaml`）关闭 Redis，使用 `stateBackend=local`。
- 不再挂 `sqlite-adapter-patch`。该补丁只存在于 1.0.1 现网 overlay；本 fork 源码的 `getUserByKey` 仍会每次 `touchUserKeyUsage`。HDD 上请用默认 postgres values（metadata 不走 SQLite）。
- Service 名带 release 前缀（`tdai-memory-core` 等），Hub/Proxy 的 core URL 由模板拼接，不再写死 `memory-core`。
- Gateway Bearer（`gatewayApiKey`）默认空，与 k3s-single 相同：非空时 Proxy `auth/verify` 会失败。
- 未把现网 LLM Key 打进 chart。安装时用 `--set` 或 `--set-file` / `existingSecret`。
- Proxy `redis.enabled` 仍为 `false`（会话/限流独立栈，与 Pipeline `STATE_BACKEND` 无关）。
- 默认不设 `STORAGE_BACKEND`，对象落 core PVC（`/data/tdai-memory`）。要 MinIO 时 `--set minio.deploy=true`（才会写入 `STORAGE_BACKEND=s3`）。

`existingSecret` 时 Secret 需含：`MEMORY_LLM_*`、`PROXY_UPSTREAM_*`、`ADMIN_*`、`EMBEDDING_API_KEY`。`store.mode=postgres` 时还需 `DATABASE_URL`、`TDAI_METADATA_POSTGRES_URL`、`POSTGRES_*`。`stateBackend=redis` 时还需 `REDIS_PASSWORD`（若设置了 `redis.auth.password`）。MinIO 开启时再加 `S3_*`、`MINIO_ROOT_*`。

## 卸载

```bash
helm uninstall tdai-memory -n tdai-memory
# PVC 默认保留；连数据一起删：
kubectl delete ns tdai-memory
```
