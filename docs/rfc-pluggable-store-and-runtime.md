# RFC: 可插拔存储层（Pluggable Store）与分阶段数据面（Runtime）演进

| 字段 | 内容 |
|------|------|
| 状态 | Draft — 待评审 |
| 起始分支 | `feat/server_team` |
| 范围 | 仅架构决策与分阶段实施规格；**本 RFC 不包含任何运行时代码改动、数据迁移或后续实现 PR** |
| 涉及服务 | MemoryCore（主）、MemoryKnowledge（次）、MemoryProxy / MemoryPanel（仅边界说明） |
| 后续实施 | Phase 1 / Phase 2 由后续独立 PR 完成（可交由成本更低的模型执行，本文即其规格） |

---

## English Summary

TencentDB-Agent-Memory today has two hard-wired storage shapes: **Standalone** = SQLite (`node:sqlite` + `sqlite-vec` + FTS5) + local files + in-process state; **Service** = Tencent VectorDB (TCVDB) + COS + Redis + MongoDB. The Service path depends on vendor products *and* on a private `src/integrations/` submodule (COS + Redis backends) that is not in the open-source tree, so the multi-tenant path cannot be reproduced from source today.

The good news: the codebase already has the right seams. `IMemoryStore` (L0/L1 vectors + FTS + entities), `IStorageBackend` (L2/L3 blobs), `IStateBackend` (locks/queues/timers), `IMetadataStore` (Team/User/Agent/Task control plane) and `ISkillStore` are all interfaces with two implementations each. What is missing is (a) a registry instead of `"sqlite" | "tcvdb"` literal unions scattered across `StorePool`, config and skill code, (b) an **open** service-grade implementation set, and (c) a contract test suite so a third backend cannot silently diverge.

**Decisions**

1. Keep the existing four/five interfaces; do not invent a mega `IStore`. Introduce a `StoreBackendRegistry` and make `StorePool` backend-agnostic.
2. Open service defaults: **PostgreSQL + pgvector** for `IMemoryStore` / `ISkillStore` / `IMetadataStore` (later), **S3-compatible (MinIO default)** for `IStorageBackend`, **Redis** (open in-tree implementation) for `IStateBackend`. Qdrant is *not* a v1 target (it only covers the vector slice; L0/L1 need relational filters, pagination, counts and audit tables anyway) and is listed as a possible later vector-only sidecar.
3. TCVDB / COS / MongoDB become adapters registered like any other; they remain fully supported and remain the default for existing `deployMode=service` deployments (no silent default flip).
4. Sparse BM25 vectors are computed **client-side** (already true for TCVDB via `bm25-local.ts`) and stored as pgvector `sparsevec`; dense embeddings use the existing `EmbeddingService`. This makes hybrid search behave the same across SQLite/pgvector/TCVDB and avoids Chinese-tokenizer dependence in the database.
5. Language: MemoryProxy and MemoryPanel stay Node. MemoryCore stays TypeScript as the *control plane* (LLM pipeline, HTTP API, plugin hosts) through Phase 3. A Go data plane is **not** started in Phases 0–3; instead Phase 2 shapes the store contract so a `RemoteMemoryStore` adapter can later talk to a Go `store-service` over gRPC/HTTP without another interface redesign. Rust is not planned.
6. Explicit non-goals for v1: no WeKnora replacement (document ingest/OCR, human Q&A, KB governance), no `viking://` clone. A thin, honest read-only "memory tree" listing over `StoragePaths` is proposed as an *optional* Phase 3 item because the blob layout already is a filesystem.

Phases: (0) this RFC → (1) extract registry + capability flags + contract tests, SQLite/TCVDB unchanged in behaviour → (2) Postgres/pgvector + S3 + open Redis state skeleton, `deploy/compose/open-service.yml` → (3) polish: TCVDB/COS re-registered through the same registry, migration tool `sqlite → postgres`, optional Knowledge/Proxy alignment.

---

## 1. 背景与问题陈述

### 1.1 产品目标

Memory Hub 的长期目标是同时覆盖两类产品的能力：

- **OpenViking**（火山引擎）：Agent-native 上下文数据库，`viking://` 虚拟文件系统，L0/L1/L2 是**同一对象**在不同 token 预算下的深度（abstract / overview / full），热路径为 Rust/Go/C++。
- **WeKnora**（腾讯）：面向企业文档的 RAG / Wiki / ReAct，Go + Vue，可插拔 Postgres + 向量库 + 对象存储（本地 / S3 / OSS / COS 等），面向人的知识库治理。

本仓库的优势：MemoryProxy 拦截 Anthropic/OpenAI 流量注入记忆与 Skill；L0–L3 是**对话蒸馏管线**（原始轮次 → 原子记忆 → 场景块 → 画像），与 OpenViking 的 L0/L1/L2 语义不同；Team/Agent/Task 隔离；四类资产（Chat Memory、Skill、Wiki、CodeGraph）；四个 Node 22 / TS 服务。

### 1.2 已知架构缺口（视为既定前提）

1. **存储绑定**：Standalone = SQLite + 本地文件；Service = TCVDB + COS + Redis（+ MongoDB 存元数据）。云路径是厂商产品，不是可插拔的开放默认；SQLite 无法承担多租户扩展。
2. **语言**：四个服务全部 TypeScript。Proxy / Panel 用 Node 合理；Core / Knowledge 作为数据面，纯 TS 相对 WeKnora（Go）与 OpenViking（Rust/Go 热路径）偏弱。
3. **能力差距**：OpenViking 有可浏览的上下文文件系统（ls/tree/find，可观测的检索）；WeKnora 有完整的文档摄取（PDF/OCR/多模态）、人类问答、租户 RBAC。本 RFC **不**声称首日替代 WeKnora 摄取。

### 1.3 本仓库现状（代码事实，已逐一核对）

#### 1.3.1 双模式切换点

`README.deployment.md` 第 8–11 行定义了两种形态：

```8:11:README.deployment.md
| 形态 | 后端存储 | 状态后端 | 多租户 | 适用场景 |
|------|----------|----------|--------|----------|
| **Standalone（开源单机版）** | SQLite + 本地文件 | 进程内 Map / Timer | 单空间 | 本地开发、单 Agent sidecar、Docker 一体化、离线部署 |
| **Service（云服务化版）** | TCVDB + COS | Redis（分布式锁 + 任务队列） | 多空间 per-`service_id` | K8s 多副本、多租户 SaaS、多 Agent 共享记忆 |
```

Gateway 由 `deployMode` 直接推导 Store 模式，只有 `STORE_MODE` 一个逃生口：

```1788:1792:MemoryCore/src/gateway/server.ts
    const storeModeOverride = process.env.STORE_MODE === "sqlite" || process.env.STORE_MODE === "tcvdb"
      ? (process.env.STORE_MODE as "sqlite" | "tcvdb")
      : undefined;
    this.storePool = new StorePool({
      mode: storeModeOverride ?? (this.config.deployMode === "service" ? "tcvdb" : "sqlite"),
```

Service 模式还在同一处硬连 COS（`server.ts:1807–1809` 调用 `initSharedCosClient()`，定义在 `server.ts:2483`）。

#### 1.3.2 StorePool：模式是字面量联合，不是注册表

```59:59:MemoryCore/src/core/store/store-pool.ts
export type StoreMode = "sqlite" | "tcvdb";
```

```196:198:MemoryCore/src/core/store/store-pool.ts
    const pooledStore = this.mode === "tcvdb" && vdbConfig
      ? this.createTcvdbStore(vdbConfig)
      : this.createSqliteStore(instanceId);
```

`createTcvdbStore`（`store-pool.ts:340–366`）与 `createSqliteStore`（`store-pool.ts:372–401`）是私有方法；SQLite 路径布局在 `getSqlitePath`（`store-pool.ts:408–413`）：`default → {dataDir}/vectors.db`，其余 `→ {dataDir}/instances/{instanceId}/vectors.db`。StorePool 同时缓存 `TcvdbSkillStore`（仅 TCVDB，`store-pool.ts:26,334`）。

同样的 `"sqlite" | "tcvdb"` 联合还出现在：

- `MemoryCore/src/config.ts:183` — `export type StoreBackend = "sqlite" | "tcvdb";`
- `MemoryCore/src/config.ts:498–499` — YAML `memory.storeBackend` 解析，非 `tcvdb` 一律回落 `sqlite`
- `MemoryCore/src/core/store/factory.ts:54` — `createStoreBundle` 的 `switch (config.storeBackend)`（插件宿主路径，非 StorePool）
- `MemoryCore/src/core/skill/types.ts:20,103` 与 `skill-config.ts:76–89` — Skill 子系统独立解析并在缺凭证时降级到 sqlite

#### 1.3.3 已存在的四组抽象接口（这是本 RFC 的基础）

| 接口 | 文件 | 职责 | 现有实现 |
|------|------|------|----------|
| `IMemoryStore` | `MemoryCore/src/core/store/types.ts:551–708` | L0/L1 结构化行 + 向量 + FTS；L2/L3 `profiles` 行（仅 TCVDB）；Team/User/Agent/Task 实体；Knowledge 实体注册；审计；Prompt/GenerationRef | `VectorStore`（`sqlite.ts:367`，node:sqlite + sqlite-vec vec0 + FTS5）、`TcvdbMemoryStore`（`tcvdb.ts:238`） |
| `IStorageBackend` | `MemoryCore/src/core/storage/types.ts:97–158` | L0/L1 JSONL、L2 `scene_blocks/*.md`、L3 `persona.md`、checkpoint 等对象存储 | `LocalStorageBackend`（`storage/local-backend.ts`）、`CosStorageBackend`（**私有子模块** `src/integrations/cos/`，本树不存在） |
| `IStateBackend` | `MemoryCore/src/core/state/types.ts` | 会话状态、定时器、任务队列、分布式锁 | `LocalStateBackend`（`state/local-backend.ts`）、`RedisStateBackend`（**私有子模块** `src/integrations/redis/`） |
| `IMetadataStore` | `MemoryCore/src/metadata/store/interface.ts` | Team/User/Agent/Task/资产绑定控制面 | `SqliteMetadataStore`、`MongoMetadataStore`；**service 模式强制 Mongo**（`metadata/store/factory.ts:111–131`） |
| `ISkillStore` | `MemoryCore/src/core/skill/skill-store.interface.ts:57–123` | Skill 版本、FTS、向量 | `SqliteSkillStore`（`skill/skill-store.ts:155`）、`TcvdbSkillStore`（`store/tcvdb-skill-store.ts:115`） |

`IMemoryStore` 与 `IStorageBackend` 的关系在代码里已经写明：

```15:18:MemoryCore/src/core/storage/types.ts
 * Relationship to IMemoryStore (src/core/store/types.ts):
 *   - IMemoryStore  = database abstraction (L0/L1 structured data → VDB/SQLite)
 *   - IStorageBackend = file storage abstraction (L2/L3 Markdown files → COS/local-fs)
 *   Both are parallel, not replacements of each other.
```

#### 1.3.4 开源树里缺失的实现

对象存储与状态后端的工厂都是动态 `import()` 一个不在仓库里的目录，并在失败时抛出明确错误：

```34:45:MemoryCore/src/core/storage/factory.ts
      let CosStorageBackendCtor: typeof import("../../integrations/cos/cos-backend.js").CosStorageBackend;
      try {
        ({ CosStorageBackend: CosStorageBackendCtor } = await import(
          "../../integrations/cos/cos-backend.js"
        ));
      } catch (err) {
        throw new Error(
          `${TAG} COS backend is not available in this build; ` +
            `switch to storage type=local or provide a build that includes it. ` +
            `Original error: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
```

```54:64:MemoryCore/src/core/state/index.ts
    let RedisStateBackendCtor: typeof import("../../integrations/redis/index.js").RedisStateBackend;
    try {
      ({ RedisStateBackend: RedisStateBackendCtor } = await import("../../integrations/redis/index.js"));
    } catch (err) {
      throw new Error(
        "[state-backend] Redis integration is not available — install or initialize " +
        "src/integrations/redis/ (private submodule) to use state_backend=redis, " +
        "or switch to state_backend=local. " +
        `Original error: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
```

`MemoryCore/package.json:75` 的 `files` 里显式排除 `!src/integrations/`。**结论：今天从开源代码无法跑出一个多副本的 Service 模式**，无论是否使用腾讯云产品。这是比"厂商锁定"更根本的缺口，也是 Phase 2 要优先补的。

#### 1.3.5 两种后端的语义差异（迁移与契约测试必须覆盖）

| 维度 | SQLite `VectorStore` | `TcvdbMemoryStore` |
|------|----------------------|--------------------|
| 稠密向量 | 客户端 `EmbeddingService` 计算，维度来自 `memory.embedding.dimensions`；`dimensions=0` 时延迟建 vec0（`sqlite.ts:679–690`） | 服务端 embedding（默认 `bge-large-zh`，固定 `dimension: 1024`，`tcvdb.ts:316–322`）；`embeddingEnabled=false` 时用 dim-1 占位向量（`tcvdb.ts:354–362`） |
| 稀疏 / 关键词 | FTS5 | 客户端 `bm25-local.ts`（jieba）生成 `sparse_vector` + 服务端 inverted 索引（`tcvdb.ts:350`） |
| L2/L3 | **只落文件**，无 profiles 行（`sqlite.ts:2736–2737`） | `profiles` collection（type=l2/l3）+ 文件 |
| 集合 / 表 | `l1_records` / `l1_vec` / `l1_fts` / `l0_*` / `entity_*` / `memory_audit` / `memory_prompts*` | `{database}_l1_memories` / `_l0_conversations` / `_profiles` / `_memory_audit` / `_knowledge` / `_memory_prompts*` / `_memory_generation_refs`（`tcvdb.ts:89–99`） |
| 租户过滤 | `buildIsolationWhere`（`isolation.ts:120–151`）拼 `WHERE` | `buildIsolationConditions`（`tcvdb.ts:186–197`）拼 filter 表达式；批删前 `assertDeleteFilterSafe` 防清库（`tcvdb.ts:215–231`） |
| 每实例隔离 | 每 instanceId 一个 `.db` 文件 | 每 instanceId 一个 VDB `database` |

`l1_records` 的隔离列（`team_id / user_id / agent_id / session_id / task_id`）见 `sqlite.ts:626–645`，TCVDB 对应 filter 索引见 `tcvdb.ts:405–416`。这两个 schema 是 Postgres 表设计的直接来源。

#### 1.3.6 对象存储的路径布局本身就是一个"文件系统"

```238:248:MemoryCore/src/core/storage/types.ts
 * key directory structure:
 *   conversations/{YYYY-MM-DD}.jsonl   — L0 conversation records
 *   records/{YYYY-MM-DD}.jsonl         — L1 memory records
 *   scene_blocks/{name}.md             — L2 scene blocks
 *   persona.md                         — L3 user persona
 *   .metadata/scene_index.json         — scene index
 *   .metadata/checkpoint.json          — pipeline checkpoint
 *   .metadata/manifest.json            — metadata manifest
 *   .metadata/instance_id              — instance ID
 *   .backup/persona/                   — persona backups
 *   .backup/scene_blocks/              — scene block backups
```

COS 侧全局共用一个 bucket，按 `pathPrefix` + `/{instanceId}/` 隔离（`instance-config-provider.ts:5–6`）。这是 §6.4 "薄 memory tree" 的依据。

#### 1.3.7 其他服务的存储

- **MemoryKnowledge**：独立进程，`better-sqlite3 ^11.10.0` + `drizzle-orm ^0.44.0`，schema 在 `MemoryKnowledge/src/db/schema.ts:18–139`（`knowledge_code_graph` / `knowledge_wiki` / 审计 / `llm_binding`），每次读写按 `service_id` 再 `team_id` 过滤（`MemoryKnowledge/src/store/sqlite-store.ts:1–14`）。Wiki 正文与 CodeGraph 索引落 `KNOWLEDGE_DATA_DIR`。**不共享** MemoryCore 的 store 代码。
- **MemoryProxy**：自有 `ProxyStorage`（`MemoryProxy/src/storage/`：cos / sqlite / fs / memory），用于会话与注入缓存，不是 L0–L3 数据面；通过 HTTP 调 Core。
- **MemoryPanel**：纯 HTTP 适配 Core `/v3/meta/*`、`/v3/skill/*` 与 Knowledge，不直接碰数据库。
- **仓库布局**：无根 `package.json` / 根 workspace；四个服务各自独立 `package.json`。MemoryCore 要求 `node >= 22.16.0`（使用内建 `node:sqlite`）。

#### 1.3.8 已有的可复用先例

- **契约测试**：`MemoryCore/src/metadata/store/metadata-store.contract.ts:1–6` —— 同一套用例分别跑 SQLite / MongoDB。这正是 `IMemoryStore` 缺少的东西。
- **离线迁移工具**：`MemoryCore/scripts/migrate-sqlite-to-tcvdb/`（`sqlite-to-tcvdb.ts`、`manifest-write.ts`、`config-write.ts`，含 `--dry-run`）。Phase 3 的 `sqlite → postgres` 迁移应复用其骨架。
- **动态加载 + 明确报错**：storage / state 工厂的模式适合扩展为"注册表 + 可选 peer dependency"。

---

## 2. 目标与非目标

### 2.1 目标

- G1 任何后端都通过**注册表**接入，`StorePool`、Gateway、Skill 子系统不再出现 `"sqlite" | "tcvdb"` 字面量分支。
- G2 提供一套**完全开源、可从源码跑起来**的 Service 级后端组合：Postgres + pgvector、S3 兼容对象存储（默认 MinIO）、Redis（开源实现）。
- G3 TCVDB / COS / MongoDB 成为与其他后端平等的适配器；**现有部署行为不变**。
- G4 用**契约测试**约束所有 `IMemoryStore` / `IStorageBackend` / `IStateBackend` / `ISkillStore` 实现，防止第三个实现悄悄偏离。
- G5 为后续可能的 Go 数据面留好**边界**（接口形状可以 1:1 映射到 gRPC/HTTP），但不在本 RFC 各阶段启动重写。
- G6 明确 Standalone 与 Service 两种形态在新架构下的默认组合与迁移路径。

### 2.2 非目标（v1 不做）

- 不替代 WeKnora 的文档摄取（PDF/OCR/多模态解析）、人类问答 UI、知识库 RBAC 治理。
- 不实现 `viking://` 协议或 L0/L1/L2 "同一对象多深度"模型；本项目的 L0–L3 是蒸馏管线，语义不同，强行对齐会破坏现有 API。
- 不引入 Go / Rust 代码；不改四个 `package.json` 树的构建方式；不做大爆炸重写。
- 不改 `/v1`、`/v2`、`/v3` HTTP API 语义；不改 Proxy / Panel 的存储。
- 不在本 RFC 内做 MemoryKnowledge 的 Postgres 化（列为 Phase 3 可选项）。
- 不在 v1 提供 Qdrant 适配器（见 §3.2 决策理由）。

---

## 3. 决策

### 3.1 D1 —— 保留现有接口分层，引入注册表而非新的 `IStore`

**决策**：不合并成一个 `IStore`。保留 `IMemoryStore`（结构化+向量）、`IStorageBackend`（对象）、`IStateBackend`（协调）、`IMetadataStore`（控制面）、`ISkillStore` 五个契约，新增一个把它们按后端名组合起来的 `StoreBackendRegistry`，`StorePool` 只依赖注册表。

**理由**：五个接口对应五种完全不同的一致性/延迟/扩展模型（关系+向量、对象追加、锁与队列、事务性控制面、版本化文档）。一个 `IStore` 会把 pgvector 与 MinIO 与 Redis 强绑，反而让"pgvector + COS"或"TCVDB + MinIO"这类组合不可能。WeKnora 也是按 `RetrieveEngine / ObjectStorage / DB` 分开插拔的。

**注册表形状（规格，Phase 1 实现）**：

```ts
// MemoryCore/src/core/store/registry.ts  (new)
export type StoreBackendId = string;               // "sqlite" | "tcvdb" | "postgres" | ...

export interface MemoryStoreFactory {
  readonly id: StoreBackendId;
  /** 后端需要的实例级连接配置形状；由 InstanceConfigProvider / env 提供 */
  create(ctx: MemoryStoreCreateContext): Promise<PooledStore>;
  /** 用于 StorePool 判断"配置变了要重建"的指纹 */
  fingerprint(ctx: MemoryStoreCreateContext): string;
  /** 可选：与该后端配套的 ISkillStore 工厂 */
  createSkillStore?(ctx: MemoryStoreCreateContext): Promise<ISkillStore>;
}

export interface MemoryStoreCreateContext {
  instanceId: string;
  dataDir: string;                                  // sqlite 等本地后端使用
  memoryCfg: MemoryTdaiConfig;
  /** 后端专属连接配置；由 IConfigSource 按 instanceId 提供，形状由后端自定义 */
  connection: Record<string, unknown> | null;
  sharedBm25Encoder?: BM25LocalEncoder;
  logger: StoreLogger;
}

export interface StoreBackendRegistry {
  registerMemoryStore(f: MemoryStoreFactory): void;
  registerStorageBackend(id: string, f: StorageBackendFactory): void;
  registerStateBackend(id: string, f: StateBackendFactory): void;
  getMemoryStore(id: StoreBackendId): MemoryStoreFactory;   // 未注册 → 抛带安装提示的错误
  // ...
}
```

`VdbConfig`（`instance-config-provider.ts:27–32`）变成 `connection` 的一种形状（`{ kind: "tcvdb", url, user, apiKey, database }`）；Postgres 是 `{ kind: "postgres", url, schema }`。`IConfigSource` 的返回类型随之从 `InstanceConfig.vdb: VdbConfig` 泛化为 `InstanceConfig.store: StoreConnection`（保留 `vdb` 字段作为过渡别名一个 minor 版本）。

### 3.2 D2 —— 开源默认后端：Postgres + pgvector；Qdrant 不进 v1

**决策**：`IMemoryStore` / `ISkillStore` 的开源 Service 级默认实现是 **PostgreSQL ≥ 16 + pgvector ≥ 0.7**（需要 `sparsevec` 与 HNSW）。Qdrant 不作为 v1 目标。

**理由**：

1. `IMemoryStore` 有 60+ 方法，其中向量检索只占 6 个（`searchL0Vector` / `searchL1Vector` / `*Hybrid` 等）。其余是分页查询、计数、按 session 分组（`queryL0GroupedBySessionId`）、实体 CRUD、审计、Prompt 版本——全是关系型需求。Qdrant 只能覆盖向量切片，L0/L1 行仍要落一个关系库，结果是"Qdrant + Postgres"两个依赖，而 pgvector 一个就够。
2. SQLite 实现的形状（表 + `vec0` 虚表 + FTS5 虚表）与 Postgres（表 + `vector`/`sparsevec` 列 + `tsvector`）几乎同构，移植风险最低，且事务语义一致（`upsertL1` 同时写行与向量可以在一个事务里）。
3. WeKnora 的开源默认也是 Postgres（+ pgvector 选项），对外心智一致。
4. Qdrant 在未来仍可作为"向量侧车"接入：注册一个 `postgres+qdrant` 组合后端，行落 Postgres、向量落 Qdrant。这不需要改接口，只需要新增一个 factory，因此不必在 v1 决定。

**pgvector schema 规格（Phase 2）**——按 `sqlite.ts:626–690` 与 `tcvdb.ts:405–416` 对齐：

```sql
-- 每个 instanceId 一个 schema: tdai_{instance_id}（见 D6）
CREATE TABLE l1_records (
  record_id        TEXT PRIMARY KEY,
  content          TEXT NOT NULL,
  type             TEXT NOT NULL DEFAULT '',
  priority         INTEGER NOT NULL DEFAULT 50,
  scene_name       TEXT NOT NULL DEFAULT '',
  session_key      TEXT NOT NULL DEFAULT '',
  session_id       TEXT NOT NULL DEFAULT 'default',
  team_id          TEXT NOT NULL DEFAULT 'default',
  task_id          TEXT NOT NULL DEFAULT '',
  user_id          TEXT NOT NULL DEFAULT 'default',
  agent_id         TEXT NOT NULL DEFAULT 'default',
  version          INTEGER NOT NULL DEFAULT 0,
  timestamp_str    TEXT NOT NULL DEFAULT '',
  timestamp_start  TIMESTAMPTZ NULL,
  timestamp_end    TIMESTAMPTZ NULL,
  created_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_time     TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
  embedding        vector(:dims) NULL,          -- 维度由 memory.embedding.dimensions 决定
  sparse           sparsevec(:vocab) NULL,      -- bm25-local 产出的稀疏向量
  fts              tsvector GENERATED ALWAYS AS (to_tsvector('simple', content)) STORED
);
CREATE INDEX ON l1_records (team_id, agent_id, updated_time);
CREATE INDEX ON l1_records (user_id, agent_id, session_id);
CREATE INDEX ON l1_records (session_key, updated_time);
CREATE INDEX ON l1_records USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON l1_records USING hnsw (sparse sparsevec_ip_ops);
CREATE INDEX ON l1_records USING gin (fts);
-- l0_conversations / profiles / entity_* / memory_audit / memory_prompts* / memory_generation_refs 同理
```

要点：

- **L2/L3 `profiles` 表在 Postgres 里也建**（对齐 TCVDB，而不是 SQLite 的"只落文件"），这样 `pullProfiles` / `queryProfilesByIds` / `clearMemoryContent.profilesDeleted` 在开源 Service 路径上有一致语义。SQLite 保持现状不变。
- 稠密维度**不**硬编码 1024；沿用 `memory.embedding.dimensions`（SQLite 的做法）。TCVDB 的服务端 embedding 是它自己的适配器细节。
- 混合检索用 RRF 在应用层融合（`search-utils.ts` 已有融合逻辑可复用），不依赖数据库特定的 hybrid API。

### 3.3 D3 —— 稀疏向量客户端计算，落 `sparsevec`；不依赖数据库中文分词

**决策**：BM25 稀疏向量统一由现有 `bm25-local.ts`（jieba）在客户端生成，Postgres 落 `sparsevec`，与 TCVDB 路径一致；`tsvector` 仅作 `searchL*Fts` 的英文/简单分词兜底。

**理由**：Postgres 原生 `tsvector` 没有中文分词，`zhparser` / `pg_jieba` 是需要编译安装的扩展，会让"`docker run pgvector/pgvector`"这条最短路径失效。客户端已经有 jieba 词典与 BM25 编码器（`store-pool.ts:97,127` 显式全局共享以避免 OOM），复用它使三种后端的召回排序行为可比。

**代价**：`sparsevec` 维度等于 BM25 词表大小，需要 `bm25-local.ts` 暴露 `vocabSize()`；词表升级需要 `reindexAll`。这个约束记入 §8 风险。

### 3.4 D4 —— 对象存储：S3 兼容为开源默认（MinIO），`appendObject` 降为能力位

**决策**：新增 `S3StorageBackend`（`@aws-sdk/client-s3`，`forcePathStyle` 可配，MinIO 为 compose 默认）；`IStorageBackend.type` 从 `"local" | "cos"` 放宽为 `string`；`appendObject` 变为**可选能力**，由 `StorageAdapter` 在能力缺失时改写为"分片对象"布局。

**理由**：`IStorageBackend.appendObject`（`storage/types.ts:109–127`）依赖 COS 私有的 Append Object API（`?append&position=N`），S3 协议与 MinIO 没有等价原语。今天 `conversations/{date}.jsonl` 与 `records/{date}.jsonl` 靠 append 追加；在 S3 上只能 read-modify-write（并发不安全）或改布局。

**规格**：

```ts
export interface StorageCapabilities {
  append: boolean;          // local=true, cos=true, s3=false
  conditionalPut: boolean;  // If-Match / ETag（S3、COS 均支持；local 模拟）
}
export interface IStorageBackend {
  readonly type: string;
  getCapabilities(): StorageCapabilities;
  appendObject?(key: string, content: string | Buffer): Promise<void>;   // 变为可选
  // 其余签名不变
}
```

上层代码已经**不直接**调用 `appendObject`，而是统一经过 `StorageAdapter.appendFile(key, content)`（`storage/adapter.ts:127–128`）；调用点有 `core/conversation/l0-recorder.ts:298`、`core/record/l1-writer.ts:259`、`gateway/v2-router.ts:793`、`offload_server/ingest-handler.ts:145`、`offload_server/offload-task-executor.ts:222,531,622`。因此改动收敛在 `StorageAdapter.appendFile` 内部：

- `append=true` → 现行为不变：`backend.appendObject("records/2026-09-03.jsonl", line)`。
- `append=false` → 写 `records/2026-09-03/{epoch_ms}-{ulid}.jsonl` 单行小对象；读取侧（`readFile` / `listObjects` 的对应封装）对以 `.jsonl` 结尾的逻辑 key 做"目录存在则递归列出、按 key 排序拼接"的回退。`StoragePaths.record(date)` 保持返回逻辑 key，新增 `StoragePaths.recordShard(date, id)`。

读取 JSONL 的调用方需要逐一核对是否经过 `StorageAdapter`（`rg "readFile\(|listObjects\(" MemoryCore/src` 列出），这是 Phase 1.3 唯一可能触及上层代码的地方，单列为独立 PR，并保证 `append=true` 路径字节级不变。

### 3.5 D5 —— 状态后端：Redis 仍是开源 Service 默认，但实现必须开源在树内

**决策**：Phase 2 在 `MemoryCore/src/core/state/redis-backend.ts` 提供一个**开源** `RedisStateBackend`（`ioredis` 已是 optional dependency），实现 `IStateBackend` 全部方法，并跑与 `LocalStateBackend` 相同的契约测试。私有子模块可以继续存在并覆盖注册表中的同名 id（注册顺序：内建 → 私有），但开源树不再依赖它。

**备选（记录但不选）**：Postgres 状态后端（`SELECT ... FOR UPDATE SKIP LOCKED` 队列 + advisory lock）。优点是少一个依赖；缺点是 `TimerScanner`（`services/timer-scanner.ts:257–262`）以 500ms 轮询依赖 Redis 有序集合语义，迁到 Postgres 需要重新设计定时器。留作 Phase 3+ 可选项。

### 3.6 D6 —— 多租户：schema-per-instance；租户列与今天一致

**决策**：Postgres 上每个 `instanceId`（`x-tdai-service-id`）一个 schema `tdai_{instance_id}`，表结构在 schema 内部与 §3.2 一致；Team/User/Agent/Task/Session 隔离继续用列 + `buildIsolationWhere`（`isolation.ts:120–151`）；对象存储沿用单 bucket + `{prefix}/{instanceId}/`。

**理由**：与现状同构——TCVDB 是 per-instance `database`（`instance-config-provider.ts:14–17`），Mongo 元数据是 `tdai_metadata_{id}`（`metadata/store/factory.ts:17`），SQLite 是 per-instance 文件。schema 级隔离让 `purge instance` 是一条 `DROP SCHEMA ... CASCADE`，与 `MetadataStorePool` 的 `PurgeMetadataResult` 语义对齐；同时避免 database-per-instance 的连接池膨胀（`shark.maxInstances` 默认 1000）。

**不做**：Postgres RLS。团队级隔离已由应用层强制（`assertIsolation`，`isolation.ts:73`；`buildIsolationWhere`，`isolation.ts:120`；`rowMatchesIsolation`，`isolation.ts:159`），RLS 作为纵深防御列入 Phase 3+ 可选。

**破坏性删除护栏**：Postgres 适配器必须移植 `assertDeleteFilterSafe`（`tcvdb.ts:215–231`）的语义——任何 `DELETE` 缺少 `team_id`/`agent_id` 条件即抛错，契约测试覆盖。

### 3.7 D7 —— 元数据控制面：Phase 2 增加 Postgres `IMetadataStore`，解除 service ⇒ Mongo 的硬约束

**决策**：`validateMetadataStartupConfig`（`metadata/store/factory.ts:111–131`）当前在 `deployMode=service` 时强制 `TDAI_METADATA_MONGO_URI`。Phase 2 新增 `PostgresMetadataStore`（复用 `metadata-store.contract.ts`），校验规则改为"service 模式必须显式指定一个非 SQLite 的元数据后端"。Mongo 适配器保留。

**理由**：否则"开源 Service 组合"仍要多一个 MongoDB；而元数据表是纯关系模型，已有契约测试，是整个 RFC 里成本最低、确定性最高的一块。

### 3.8 D8 —— 语言与运行时：Proxy/Panel 留 Node；Core 留 TS 控制面；数据面先"可远程化"，不先"换语言"

**决策**：

| 组件 | Phase 0–3 | Phase 4+（本 RFC 不承诺，仅界定边界） |
|------|-----------|--------------------------------------|
| MemoryProxy | Node/TS，不变 | 不变。流量拦截是 I/O 密集、与 Anthropic/OpenAI SDK 生态强相关，Node 合适 |
| MemoryPanel | Node/TS + Web，不变 | 不变 |
| MemoryCore 控制面（HTTP `/v1 /v2 /v3`、L0→L3 LLM 管线、插件宿主 OpenClaw/Hermes/Pi、QuotaManager、Scanner/Worker 调度） | TS，不变 | TS |
| MemoryCore 数据面（`IMemoryStore` 实现、BM25 编码、hybrid 融合、`IStorageBackend`） | TS 适配器（sqlite / postgres / tcvdb） | **可选**：Go `store-service` 实现同一契约（gRPC/HTTP），TS 侧接一个 `RemoteMemoryStore` 适配器，通过注册表 `id="remote"` 接入 |
| MemoryKnowledge（Wiki/CodeGraph 摄取） | TS，不变 | **可选**：摄取重活（PDF/OCR/大仓库 CodeGraph）迁 Go，是最像 WeKnora 的那块 |
| Rust | 无 | 不规划。除非出现明确的 CPU 热点（embedding/rerank/分词）且 Node 侧无法用原生模块解决 |

**理由**：

1. 现在最贵的问题不是语言，而是"Service 模式不可从开源复现"和"后端硬编码"。先解决这两个，再谈数据面语言。
2. `IMemoryStore` 一旦经过 Phase 1 的 capability 化与契约测试，就是一份可以直译成 protobuf 的接口——`MaybePromise` 全部变为 RPC，`Float32Array` 变 `repeated float`，`IsolationFilter` 变 message。**Phase 2 的一个交付物是这份 `.proto` 草案**（只是文档，不生成代码），用来证明边界可行。
3. Node 侧真正的性能疑点只有一个：jieba BM25 编码器在进程内（`store-pool.ts:97` 注释提到 OOM 风险）。这恰好是最容易搬到 Go 侧车的纯函数。如果 Phase 3 后仍有压力，第一个 Go 组件应当是"BM25 编码 + hybrid rerank 服务"，而不是重写 store。
4. 四个独立 `package.json` 树没有根 workspace，任何"统一重写"都意味着四条构建链同时变动，不可接受。

### 3.9 D9 —— 配置面与默认值：不做静默翻转

**决策**：

- 新增 `TDAI_STORE_BACKEND=sqlite|postgres|tcvdb`（取代 `STORE_MODE`，后者保留为别名一个 minor 版本并打 deprecation 日志），`TDAI_BLOB_BACKEND=local|s3|cos`，`STATE_BACKEND=local|redis`（已存在）。
- `TDAI_DEPLOY_MODE=standalone` 的默认组合不变：`sqlite + local + local`。
- `TDAI_DEPLOY_MODE=service` **未显式指定后端时默认仍是 `tcvdb + cos + redis`**（现有部署零变化）；Phase 2 起在该情况下打一条 warning，文档把 `postgres + s3 + redis` 作为推荐组合。默认值的翻转留给下一次 major 版本。
- Postgres 连接：`TDAI_PG_URL`（或 `IConfigSource` 按实例返回）、`TDAI_PG_SCHEMA_PREFIX=tdai`；S3：`TDAI_S3_ENDPOINT / BUCKET / ACCESS_KEY / SECRET_KEY / REGION / PREFIX / FORCE_PATH_STYLE`。
- YAML `memory.storeBackend`（`config.ts:498`）与 Skill 的 `storeBackend`（`skill/types.ts:20`）改为字符串并走注册表校验；Skill 的"缺凭证降级 sqlite"逻辑（`skill-config.ts:76–89`）改为"缺凭证 → 报错"，仅当显式 `TDAI_STORE_BACKEND=sqlite` 时才用 sqlite。降级会掩盖配置错误，与多租户场景相悖。

---

## 4. 目标架构

```
                    ┌──────────────────────────────────────────────┐
                    │  MemoryCore Gateway (TS, 控制面)              │
                    │  /v1 /v2 /v3 · L0→L1→L2→L3 pipeline · plugins │
                    └───────────────┬──────────────────────────────┘
                                    │ StorePool.getStore(instanceId)
                    ┌───────────────▼──────────────────────────────┐
                    │  StoreBackendRegistry                          │
                    │  memory:  sqlite | postgres | tcvdb | remote*  │
                    │  blob:    local  | s3       | cos              │
                    │  state:   local  | redis                       │
                    │  meta:    sqlite | postgres | mongodb          │
                    └──┬──────────────┬──────────────┬─────────────┘
                       │              │              │
        ┌──────────────▼──┐  ┌────────▼───────┐  ┌───▼────────────┐
        │ IMemoryStore    │  │ IStorageBackend│  │ IStateBackend  │
        │ + ISkillStore   │  │ (L0/L1 jsonl,  │  │ (locks, queue, │
        │ (L0/L1 rows,    │  │  L2 md, L3 md, │  │  timers)       │
        │  vectors, FTS,  │  │  checkpoints)  │  │                │
        │  profiles,      │  └────────────────┘  └────────────────┘
        │  entities,audit)│
        └─────────────────┘
   Standalone:  sqlite      +  local           +  local     (+ sqlite meta)
   Open svc:    postgres    +  s3 (MinIO)      +  redis     (+ postgres meta)
   Tencent svc: tcvdb       +  cos             +  redis     (+ mongodb meta)
   * remote = Phase 4+ 可选的 Go store-service 适配器
```

`IConfigSource`（`core/abstractions/`）仍按 instanceId 返回连接配置，只是配置形状由后端定义。`InstanceConfigProvider` 的 VDB TTL 缓存与 COS 凭证刷新逻辑对 Postgres/S3 同样适用（连接串通常静态，但 STS 式临时凭证在 S3 上也存在）。

---

## 5. 分阶段计划

所有阶段遵循：**每个 PR 都能独立合并、`npm test` 通过、Standalone 行为字节级不变、`deployMode=service` 现有部署零配置变更**。

### Phase 0 —— 本 RFC

交付物：本文档。评审通过后进入 Phase 1。

### Phase 1 —— 提取注册表 + 能力位 + 契约测试（保留 SQLite / TCVDB 适配器，不引入新依赖）

目标：把"两个硬编码后端"变成"注册表里的两个条目"，行为不变。

| PR | 内容 | 涉及文件 | 验收 |
|----|------|----------|------|
| 1.1 | 新建 `src/core/store/registry.ts`：`StoreBackendRegistry`、`MemoryStoreFactory`、`MemoryStoreCreateContext`；新建 `src/core/store/backends/sqlite.factory.ts` 与 `backends/tcvdb.factory.ts`，把 `store-pool.ts:340–401` 的两个私有方法搬进去；`StorePool` 改为持有 registry，`StoreMode` 改为 `StoreBackendId: string`，`computeFingerprint` 委托给 factory | `store-pool.ts`, `registry.ts`(新), `backends/*.factory.ts`(新), `gateway/server.ts:1788–1802` | 现有 `store-pool` 单测通过；`STORE_MODE` 与 `deployMode` 推导行为不变；新增单测：未注册 id 抛出含安装提示的错误 |
| 1.2 | 放宽字面量联合：`config.ts:183` `StoreBackend` → `string`（解析时不再回落 sqlite，而是原样保留并在 StorePool 处校验）；`skill/types.ts:20,103`、`skill-config.ts:76–89` 同步；`createStoreBundle`（`store/factory.ts:54`）改走 registry | `config.ts`, `skill/types.ts`, `skill-config.ts`, `store/factory.ts` | 现有配置解析测试通过；`storeBackend: "tcvdb"` 缺凭证 → 报错而非静默降级（补测试） |
| 1.3 | `IStorageBackend`：`type: string`，新增 `getCapabilities()`，`appendObject` 改可选；`LocalStorageBackend` 返回 `{append:true}`；`StorageAdapter.appendFile` 内部按能力位选择直接 append 或分片写；对应读路径加分片拼接回退；`ScopedStorageBackend`（`adapter.ts:16`）透传能力位 | `storage/types.ts`, `storage/adapter.ts`, `storage/local-backend.ts`；只在确有绕过 `StorageAdapter` 直读 JSONL 的地方才改上层 | `append=true` 路径生成的文件与改动前逐字节一致（用现有 standalone e2e `npm run test:standalone:memory` 验证）；新增 `StorageAdapter` 单测覆盖 `append=false` 分片写读 |
| 1.4 | 契约测试骨架：`src/core/store/memory-store.contract.ts`（参照 `metadata/store/metadata-store.contract.ts`），先覆盖：`init/getCapabilities`、L0/L1 upsert+query+分页、隔离过滤、`clearMemoryContent` 护栏、`searchL1Fts`；SQLite 实现接入并通过；TCVDB 实现在 CI 无凭证时 `describe.skip` | `memory-store.contract.ts`(新), `sqlite.test.ts` | `npm test` 通过；契约用例数 ≥ 30 |
| 1.5 | `src/core/storage/storage-backend.contract.ts` 与 `src/core/state/state-backend.contract.ts`：`LocalStorageBackend` / `LocalStateBackend` 接入 | 同上 | `npm test` 通过 |
| 1.6 | 文档：`README.deployment.md` 增加"后端注册表"小节；`STORE_MODE` 标记 deprecated | 文档 | — |

**Phase 1 明确不做**：不新增任何 npm 依赖；不改 Postgres/S3；不碰 `IMetadataStore`；不碰 MemoryKnowledge。

### Phase 2 —— Postgres/pgvector + S3 兼容 + 开源 Redis 状态后端骨架

目标：从开源代码跑起来一个多副本 Service 模式。

| PR | 内容 | 涉及文件 | 验收 |
|----|------|----------|------|
| 2.1 | `backends/postgres/`：`PostgresMemoryStore implements IMemoryStore`，依赖 `pg`（optionalDependencies）；DDL 见 §3.2；`init()` 负责 `CREATE SCHEMA IF NOT EXISTS tdai_{instanceId}` + 建表建索引（幂等）；向量维度来自 `memory.embedding.dimensions`，`dims=0` 时不建 `embedding` 列/索引（与 sqlite 的延迟 vec0 同语义，`getCapabilities().vector=false`）；`sparse` 列来自 `bm25-local`；hybrid 走应用层 RRF | `backends/postgres/*.ts`(新), `registry` 注册 `"postgres"`, `package.json` | 契约测试在 `TDAI_TEST_PG_URL` 存在时全绿；无环境变量时 skip |
| 2.2 | `PostgresSkillStore implements ISkillStore`（对照 `skill-store-ddl.ts` 的 `skills / skill_fts / skill_vec`） | `backends/postgres/skill-store.ts` | Skill 契约测试（新建，参照 1.4）全绿 |
| 2.3 | `S3StorageBackend implements IStorageBackend`（`@aws-sdk/client-s3` optional）；`getCapabilities()={append:false, conditionalPut:true}`；`deleteByPrefix` 用分页 `ListObjectsV2` + `DeleteObjects`；`ICredentialProvider` 扩展为返回泛化 `BlobCredential`（`CosCredential` 变为其子类型） | `storage/s3-backend.ts`(新), `storage/types.ts`, `storage/factory.ts` | storage 契约测试在 `TDAI_TEST_S3_ENDPOINT`（MinIO）存在时全绿 |
| 2.4 | `RedisStateBackend`（开源）：`state/redis-backend.ts` 实现 `IStateBackend` 全部方法；`state/index.ts:49–94` 改为先查注册表内建实现，再尝试私有 `integrations/redis` 覆盖 | `state/redis-backend.ts`(新), `state/index.ts` | state 契约测试在 `TDAI_TEST_REDIS_URL` 存在时全绿；`TimerScanner` 单测通过 |
| 2.5 | `PostgresMetadataStore implements IMetadataStore`；`validateMetadataStartupConfig` 改为"service 必须非 sqlite"；`MetadataStorePool` 支持 `backend: "postgres"`（库名规则 `tdai_metadata_{id}` 改为 schema） | `metadata/store/postgres-adapter.ts`(新), `metadata/store/factory.ts` | `runMetadataStoreContract("postgres", ...)` 全绿 |
| 2.6 | Gateway 装配：`server.ts:1805–1809` 的 `initSharedCosClient` 泛化为 `initSharedBlobStorage()`，按 `TDAI_BLOB_BACKEND` 走 `createStorageBackend`；`InstanceConfigProvider` 的 `LocalConfigSource` 读取 `TDAI_PG_URL` / `TDAI_S3_*` | `gateway/server.ts`, `core/instance-config-provider.ts`, `utils/env-config.ts` | service 模式 + `postgres/s3/redis` 环境下 `__tests__/standalone/e2e.sh` 的 capture/recall 流程通过（新增 `scripts/e2e-open-service.sh`） |
| 2.7 | `deploy/compose/open-service.yml`：`pgvector/pgvector:pg16`、`minio/minio`、`redis:7`、MemoryCore（service 模式）、可选 Panel/Knowledge；`.env.example`；`README.deployment.md` 新增"开源 Service 模式"章节 | `deploy/compose/*`, 文档 | `docker compose up` 后 e2e 通过 |
| 2.8 | `docs/store-service.proto.md`：把 Phase 1 后的 `IMemoryStore` 直译为 protobuf 草案（仅文档），标注哪些方法是可选能力 | 文档 | 评审 |

**Phase 2 明确不做**：不翻转 service 默认；不删 TCVDB/COS/Mongo；不做数据迁移工具（Phase 3）；不改 MemoryKnowledge。

### Phase 3 —— 收口：TCVDB/COS 完全走注册表、迁移工具、可选对齐

| 项 | 内容 |
|----|------|
| 3.1 | TCVDB / COS / Mongo 适配器移到 `backends/tcvdb/`、`storage/cos-backend.ts`（若私有实现开源）或保持私有但通过注册表覆盖；删除所有对 `integrations/` 的直接 `import()` 路径字符串 |
| 3.2 | `scripts/store-migrate/`：泛化 `migrate-sqlite-to-tcvdb`，源/目标均为 `IMemoryStore` + `IStorageBackend`；支持 `--dry-run`、按 instanceId、断点续传（manifest）；向量策略：目标维度不同 → 用 `reindexAll(embedFn)` 重算，否则直拷 |
| 3.3 | Standalone 可选 `postgres`：允许 `TDAI_DEPLOY_MODE=standalone` + `TDAI_STORE_BACKEND=postgres`（单副本、本地状态），作为"SQLite 到多副本"的中间台阶 |
| 3.4 | **可选** 薄 memory tree：`GET /v3/memory/tree?team_id&agent_id[&path]` 与 `GET /v3/memory/cat?path`，只读，直接映射 `IStorageBackend.listObjects/getObject` 在 `StoragePaths` 上的结果；不引入 `viking://` scheme，不做写入 |
| 3.5 | **可选** MemoryKnowledge：Drizzle 从 `better-sqlite3` 驱动增加 `pg` 驱动，`IKnowledgeStore` 契约测试双跑；Wiki 正文/CodeGraph 索引继续落 `KNOWLEDGE_DATA_DIR`（或 `IStorageBackend`，另立 RFC） |
| 3.6 | **可选** MemoryProxy：`ProxyStorage` 增加 `s3` 实现（与 Core 的 `S3StorageBackend` 无代码共享，仅对齐配置命名） |
| 3.7 | 决定是否在下一 major 翻转 `service` 默认组合 |

### Phase 4+（本 RFC 不承诺）

- Go `store-service` 实现 `docs/store-service.proto.md`，TS 侧 `RemoteMemoryStore`。第一候选组件是 BM25 编码 + hybrid rerank，而非整个 store。
- Postgres RLS；Qdrant 向量侧车（`postgres+qdrant` 组合后端）；Postgres 状态后端。

---

## 6. 与现有形态的关系

### 6.1 Standalone（不变）

`sqlite + local + local`。Phase 1 后底层多一层注册表，但 `~/.memory-tencentdb/memory-tdai/vectors.db` 与 `StoragePaths` 文件布局逐字节不变。Phase 3.3 可选允许 `postgres` 作为单机后端。

### 6.2 Service — 腾讯云组合（不变）

`tcvdb + cos + redis + mongodb`。Phase 1–3 期间唯一变化是 Phase 2 起在未显式配置后端时打一条 warning。私有 `integrations/` 子模块继续通过注册表覆盖同名 id。

### 6.3 Service — 开源组合（新增）

`postgres + s3 + redis + postgres(meta)`，由 `deploy/compose/open-service.yml` 一键起。多副本水平扩展依赖与今天相同的机制（Redis 锁/队列 + `x-tdai-service-id` 路由 + StorePool LRU）。

### 6.4 关于 OpenViking 与 WeKnora

- 与 OpenViking：**不**对齐 L0/L1/L2 语义。可以诚实提供的子集是 §5 3.4 的只读 tree/cat，因为 `StoragePaths` 已经是一棵可 `ls` 的树；这解决"检索可观测"的一半（看到 L2/L3 产物），但不提供"同一对象多深度"能力。
- 与 WeKnora：本 RFC 只覆盖"可插拔 Postgres + 向量 + 对象存储"这一层的对齐。文档摄取、OCR、多模态、人类问答与 KB 治理不在范围内；MemoryKnowledge 的 Postgres 化（3.5）是为将来那条线铺路，但不承诺。

---

## 7. 迁移

### 7.1 SQLite → Postgres（Phase 3.2 工具）

1. 源：`VectorStore` 只读打开 `{dataDir}/instances/{id}/vectors.db`；`LocalStorageBackend` 只读打开数据目录。
2. 目标：`PostgresMemoryStore.init()` 建 schema；`S3StorageBackend` 目标 prefix `{prefix}/{instanceId}/`。
3. 行数据：`queryL1Records` / `queryL0Paginated` 分页拉取 → `upsertL1` / `upsertL0`。实体表、审计、Prompt 走对应 `IMemoryStore` 方法。
4. 向量：源维度 == 目标 `memory.embedding.dimensions` → 直拷；否则跳过向量，迁移结束后调用 `reindexAll(embedFn)`（接口已存在，`types.ts:635–638`）。
5. 稀疏向量：目标端重新用 `bm25-local` 编码（源 SQLite 没有稀疏向量）。
6. 文件：`listObjects` 递归拷贝；JSONL 在 `append=false` 目标上按 §3.4 分片布局落地（每行一个对象），或提供 `--jsonl-mode=whole` 保留整文件（只读迁移可接受）。
7. L2/L3：文件拷贝 + 解析 `scene_blocks/*.md` / `persona.md` 生成 `profiles` 行（TCVDB 路径今天如何生成 profiles 行，迁移工具复用同一函数）。
8. 幂等：manifest 记录已迁移 `record_id` 与对象 key；`--dry-run` 只统计。
9. 切换：Gateway 停写 → 迁移 → 改 `TDAI_STORE_BACKEND/TDAI_BLOB_BACKEND` → 起服务 → 对比 `countL0/countL1/countProfiles`。

### 7.2 TCVDB → Postgres

同一工具，源换 `TcvdbMemoryStore`。差异：TCVDB 服务端向量（1024 维）无法读出为客户端向量 → 一律 `reindexAll`；`profiles` 行直拷。

### 7.3 Redis key 前缀

`REDIS_KEY_PREFIX` 默认 `tdai_memory_v2`（`gateway/config.ts:571–575` 说明了上次前缀切换的原因）。开源 `RedisStateBackend` 必须使用**相同的 key 布局与 hash tag**（`{p:inst:tid:aid}`），否则与私有实现混跑会破坏锁互斥。Phase 2.4 需要私有实现的 key 布局文档作为输入；若不可得，开源实现使用新前缀 `tdai_memory_v3` 并在文档中声明不可与私有实现混跑。

---

## 8. 风险与缓解

| # | 风险 | 影响 | 缓解 |
|---|------|------|------|
| R1 | `IMemoryStore` 有 60+ 方法、大量 `?` 可选方法，第三个实现容易"部分实现"后在运行时才暴露 | 高 | Phase 1.4 契约测试是硬门禁；`getCapabilities()` 显式声明可选能力，调用方已有 `queryProfilesByIds → pullProfiles` 之类的回退模式（`types.ts:623–628`） |
| R2 | FTS 语义差异：FTS5 vs `tsvector('simple')` vs TCVDB inverted | 中 | 契约测试只断言"包含查询词的记录被召回且隔离过滤正确"，不断言排序；排序由应用层 RRF 统一 |
| R3 | `sparsevec` 维度 = BM25 词表大小；词表变更需全量重建 | 中 | `bm25-local` 暴露 `vocabVersion`，`init()` 把它写进 schema 元表；不匹配时 `isDegraded()=true` 并要求 `reindexAll` |
| R4 | S3 无 append → JSONL 分片对象数量大 | 中 | 分片按天目录；提供后台 compaction（Phase 3 可选）；Standalone/COS 路径不受影响 |
| R5 | 私有 `integrations/` 与开源实现同名冲突 | 中 | 注册表覆盖顺序明确（内建先注册，私有后注册覆盖）；Redis key 布局见 §7.3 |
| R6 | pgvector HNSW 在 schema-per-instance × 1000 实例下索引数量爆炸 | 中 | `dims=0`/低流量实例延迟建索引（`getCapabilities().vector=false` 已有语义）；监控 `pg_stat_user_indexes`；必要时 Phase 4 评估 partition-by-instance 的单 schema 方案 |
| R7 | Skill "缺凭证降级 sqlite" 行为被移除，可能让现有错误配置暴露 | 低 | Phase 1.2 在 CHANGELOG 明示；错误信息给出修复指引 |
| R8 | 后续被要求"顺手"把 Go 数据面塞进 Phase 2 | 高 | 本 RFC D8 明确 Phase 0–3 无 Go；唯一相关交付是 `.proto` 文档 |
| R9 | MemoryKnowledge 与 Core 存储分叉长期存在 | 低 | 明确为非目标；3.5 只做 Postgres 化，不做代码共享 |
| R10 | 引用的设计文档（`docs/design/*.md`、`docs/l0l3-tenant-isolation-design.md`）在仓库中不存在，代码注释指向的依据缺失 | 低 | 本 RFC 以代码为事实来源；建议后续把这些文档补进 `docs/`（不在本 RFC 范围） |

---

## 9. 待评审的开放问题

1. 私有 `integrations/redis` 的 key 布局是否可以公开（影响 §7.3 走 v2 还是 v3 前缀）。
2. 是否接受 Phase 2.5 把元数据 Postgres 化纳入 Phase 2（推荐纳入，否则开源 Service 组合仍需 Mongo）。
3. `sparsevec` 方案 vs `pg_jieba` 扩展：本 RFC 选前者，若团队更愿意维护自定义 Postgres 镜像可改。
4. Phase 3.4 只读 memory tree 是否值得做，或干脆从范围里删掉。

---

## 10. 术语

- **instanceId / service_id**：`x-tdai-service-id`，Service 模式的最外层租户边界。
- **Team / User / Agent / Task / Session**：`IsolationContext`（`isolation.ts:25–33`）定义的实例内隔离维度。
- **L0–L3**：对话原始记录 → 原子记忆 → 场景块 → 画像（`README.deployment.md:13–18`）。
- **Store（本 RFC）**：`IMemoryStore` + `IStorageBackend` + `IStateBackend` + `IMetadataStore` + `ISkillStore` 五个契约的统称，而非某个单一接口。
