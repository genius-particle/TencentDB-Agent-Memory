# tdai-memory Helm chart

默认 **Postgres 开源栈**（`values.yaml` 即 sample）：

| 组件 | 默认 |
|---|---|
| 记忆向量 | Postgres + pgvector（`store.mode=postgres`） |
| 元数据 | Postgres（`TDAI_METADATA_BACKEND=postgres`） |
| Pipeline 状态 | Redis（`stateBackend=redis`） |
| 对象存储 | Core PVC 本地盘 |
| 镜像 | GHCR `ghcr.io/genius-particle/tencentdb-agent-memory/*:feat-server-team` |

`TDAI_DEPLOY_MODE` 固定 `standalone`（非官方 TCVDB+COS+Mongo 路径）。

## 安装

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
  --set ingress.host=tdai.<node-ip>.nip.io \
  --set public.host=<node-ip>
```

钉镜像：`--set image.tag=<short-sha>`。

环境差异（NodePort、registry mirror、第二 release）用**本机** `values-local.yaml`（已 gitignore，勿提交）：

```bash
helm upgrade --install <release> deploy/helm/tdai-memory \
  -n <namespace> --create-namespace \
  -f deploy/helm/tdai-memory/values-local.yaml \
  --set llm.apiKey=... --set proxy.upstream.apiKey=... --set embedding.apiKey=...
```

## 校验

```bash
helm lint deploy/helm/tdai-memory
kubectl -n tdai-memory get pods,svc,ingress
curl -sS http://127.0.0.1:30420/health
```

Panel 登录用 Secret 中 `ADMIN_USER_KEY`（默认 `sk-mem-LocalTrialAdminKeyChangeMe000001`）。

## 镜像

Private GHCR 需 `imagePullSecrets`。MinIO：`--set minio.deploy=true`。

## 卸载

```bash
helm uninstall tdai-memory -n tdai-memory
```
