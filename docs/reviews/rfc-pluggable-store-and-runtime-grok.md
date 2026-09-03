# 评审备忘：RFC「可插拔存储层与分阶段数据面」

| 字段 | 内容 |
|------|------|
| 评审对象 | `docs/rfc-pluggable-store-and-runtime.md`（PR #1, commit `30c165a`） |
| 评审人 | Grok 4.6（独立架构评审；未改 RFC、未改运行时） |
| 对照代码 | `feat/server_team` @ `2ee2239` + RFC 分支 |
| 结论 | **ship-with-fixes** — 决策方向可接受，Phase 1 规格必须先改再动手 |

本备忘约 200 行。不重写 RFC。§9 答案是**推荐**，不是已决。

---

## 1. Verdict

**带条件接受（ship-with-fixes）。** 不建议 rewrite：D1（五接口 + 注册表）、D9（不静默翻转现网 `service` 默认）、非目标（不做 WeKnora 摄取、不做 `viking://`）与 owner 约束对齐。代码引用大多能对上。

不能 as-is 开 Phase 1，因为：(a) Phase 1 相对「Store 接口 + SQLite adapter 搬家」过重，且 1.2/1.3 会碰到现网 Service；(b) 契约测试没有能力矩阵，挡不住第三个实现；(c) D8 用 Phase 2 一份 `.proto` 文档打发语言面投诉，等于没处理。

接受条件（Blocking，见 §3）写入 RFC 勘误即可，不必重写长文。

---

## 2. 声明核对

对代码逐条 spot-check。行号以当前树为准。RFC 自称「Fable extra」的条目，未核实前当猜测。

| RFC 声明 | 判定 | 证据 / 纠正 |
|----------|------|-------------|
| `StoreMode = "sqlite" \| "tcvdb"`；`getStore` 按 mode 分支 | **成立** | `store-pool.ts:59,196–198`。遗漏：`mode==="tcvdb" && !vdbConfig` 会静默落到 SQLite |
| `createTcvdbStore` 340–366 / `createSqliteStore` 372–401 / `getSqlitePath` 408–413 | **成立** | 行号准 |
| 共享 BM25 防 jieba OOM | **成立** | `store-pool.ts:97,126–127` |
| 字面量联合散落 config / factory / skill | **成立** | `config.ts:183,498–499`；`factory.ts:54`；`skill/types.ts:20,103`；`skill-config.ts:76–89` |
| Gateway `STORE_MODE` 逃生口；service 硬连 COS | **成立** | `server.ts:1788–1809,2483`。blob 跟 `deployMode` 走，不跟 store 走 |
| 五接口 + 双实现 | **成立** | 见下「接口」行 |
| `IMemoryStore` 60+ 方法，其中 6 个是向量检索 | **部分错** | 72 方法 + 1 可选属性；**required 24 / optional 49**。search* 共 6 个，向量/hybrid 只有 4（另 2 个是 FTS） |
| `getCapabilities` 已在 `IMemoryStore` | **成立**（RFC 当新能力写，过时） | `types.ts:250–259,567`。仅 4 个搜索位，**没有** profiles/entities/audit/prompts |
| SQLite 无 profiles 行；TCVDB 有 | **成立** | `sqlite.ts:2736–2737` vs `tcvdb.ts` `profiles` collection |
| TCVDB 服务端 embedding 默认 bge-large-zh / dim 1024 | **部分过述** | 模型名默认 `bge-large-zh`；**`embeddingEnabled` 默认 false**（`config.ts:624`）。关 embedding 时用 dim-1 占位（`tcvdb.ts:354–362`） |
| TCVDB `reindexAll` | **成立且危险** | **空操作**，返回 `{0,0}`（`tcvdb.ts:1796–1803`）。§7.2「目标端 reindexAll」必须跑在 **Postgres** 上 |
| `appendObject` 上层不直调，经 `StorageAdapter.appendFile` | **成立** | 7 处 call site 行号准。**但** `getBackend().listObjects/getObject` 有绕过：`pipeline-factory.ts:140`、`v2-router.ts:1774,1836`、`skill-resource-store.ts`、`memory-generation-log/store.ts` |
| 开源树无 `src/integrations/`，Service 不能从源码复现 | **成立** | `storage/factory.ts:34–45`、`state/index.ts:54–64`；`package.json:75` `!src/integrations/`；目录不存在 |
| service 强制 Mongo | **成立** | `metadata/store/factory.ts:111–131` |
| `metadata-store.contract.ts` 可当模板 | **成立** | |
| `search-utils.ts` RRF「已有、可复用」 | **过述** | `rrfMerge` 存在但**无人 import**；hybrid 工具各自复制了一份 |
| TimerScanner 500ms + Redis ZSET，引用 `timer-scanner.ts:257–262` | **行号错** | ZSET 分片属实。500ms 是 `gateway/config.ts:592`；scanner 文件默认 2000ms。standalone scanner 的 prefix 默认仍是 `tdai_memory`，与 Gateway `tdai_memory_v2` 不一致 |
| StorePool 还管 blob/state | **RFC 未声称；补充** | StorePool **只**管 `IMemoryStore` + TCVDB `ISkillStore`。COS cache、`createStateBackend`、`MetadataStorePool` 是另外三套工厂 |
| SQLite Skill 也走 StorePool | **错** | StorePool **只建** `TcvdbSkillStore`。SQLite Skill 在 `tdai-core.ts:881–886` 用 `VectorStore.getRawDb()` **同文件同连接**。注册表 `createSkillStore?` 没这个约束 |
| 插件路径 `createStoreBundle` ≠ StorePool | **成立但低估** | `factory.ts` 的 SQLite 永远写 `{dataDir}/vectors.db`，**没有** per-instance 布局 |
| `IMemoryStore` 可 1:1 译成 protobuf | **错** | 文件头原则 4：**Sync-first**（`types.ts:14–15`）。~49 处 `MaybePromise`；`close()`/`isFtsAvailable()`/`getCapabilities()` 同步；`reindexAll` 有 callback；`Float32Array` |
| LocalConfigSource 每 instance 一个 VDB database | **部分** | 注释图是对的；`LocalConfigSource.fetchVdb` **忽略** instanceId，读全局 env。多库只在远程 `IConfigSource` 返回不同 `database` 时成立 |
| Redis hash tag `{p:inst:tid:aid}` | **注释级成立，实现未核实** | `gateway/config.ts:571–575`、`state/types.ts:108`、`pipeline-worker.ts:703`。真正 key 拼接在私有 `integrations/redis` |
| COS Append 409 重试 | **未核实** | 接口注释有；实现不在树内 |
| Knowledge 不共享 Core store | **成立** | 独立 `better-sqlite3` + drizzle；Core 的 knowledge 方法只是实体注册 |
| 无根 workspace；Core `node>=22.16.0` | **成立** | |
| `docs/design/*.md` 缺失 | **成立** | 代码仍引用 |

---

## 3. Blocking（Phase 1 代码之前必须改 RFC）

**B1. 能力矩阵，不只是 4 个搜索 flag。**  
现有 `StoreCapabilities` 只有 `vectorSearch/ftsSearch/nativeHybridSearch/sparseVectors`。两个已实现后端已经在 **可选方法上互补**：SQLite 有 entity CRUD、无 profiles/hybrid；TCVDB 有 profiles/hybrid、无 entity CRUD，且 `reindexAll` 空操作。Phase 1.4「≥30 用例」若按并集写，SQLite 挂；按 SQLite 子集写，Postgres 可以合法缺 profiles。  
**修正：** 扩 `StoreCapabilities`（或另加 `StoreFeatureMatrix`）：`profiles`、`entities`、`audit`、`prompts`、`generationRefs`、`knowledge`、`pagination`、`clearMemoryContent`、`deferredEmbedding`。契约测试按 flag skip。Postgres 开源 Service 路径必须 `profiles=true`（对齐 TCVDB，RFC §3.2 已写，要落到测试门禁）。

**B2. 注册表必须覆盖三条装配路径，且 SQLite Skill 同库。**  
今天：Gateway `StorePool`、插件 `createStoreBundle`、`TdaiCore` 的 `SqliteSkillStore(getRawDb())`。只改 `store-pool.ts` 会留下第二套 `"sqlite"|"tcvdb"` 开关。`MemoryStoreCreateContext` 必须能交出 SQLite 的共享 `DatabaseSync`（或明确 Skill 仍由 TdaiCore 接线，不经过 registry）。§4 图把 meta 画进 registry，D1 类型草图却没有 `registerMetadataStore` — **Phase 1 不要合并 `MetadataStorePool`**。

**B3. 把 1.3（append 能力位 + JSONL 分片）移出 Phase 1。**  
Standalone/COS 都有 append。分片布局是 S3 的事，属于 Phase 2。现在改 `IStorageBackend.appendObject` 为可选，会碰到 COS JSONL 与 `getBackend()` 绕过点，违反 RFC 自己的「service 零配置变更 / Standalone 字节级不变」。  
**1.2 的 Skill「缺凭证 → 报错」同样是行为变化**（`skill-config.ts:76–89` 今天 warn 降级）。现网若靠降级活着会启动失败。降级改报错放到 CHANGELOG 明确的 minor，不要塞进「行为不变」的 Phase 1。

**B4. 语言面：Phase 0–3 可以不写 Go，但不可以只用一份 Phase 2 `.proto` 文档交差。** 见 §7。

**B5. Phase 1 范围必须收到 owner 的第一刀。** RFC 1.1–1.6 是 6 个 PR，含 TCVDB 搬家、Skill 行为变化、blob 分片、两套契约。第一刀应是「注册表 + SQLite factory 抽出 + 能力矩阵 + SQLite 契约」。TCVDB factory **原样搬家**可以附带，但 **禁止** 新 TCVDB 行为。

---

## 4. Non-blocking nits

- `computeFingerprint` 含 apiKey；Skill cache 无指纹，VDB 改配置不会踢 Skill。
- `StorePool.maxStores` 默认 100，Gateway 传入 `shark.maxInstances` 默认 1000。
- README.deployment / docker 仍写 `REDIS_KEY_PREFIX=tdai_memory`，Gateway 默认已是 `tdai_memory_v2`。
- `IMetadataStore` 已有未实现的 `mysql` 分支（`factory.ts:164–165`）；Postgres meta 应是新 backend id，别占 mysql。
- pgvector `sparsevec`：dim 上限极大，**NNZ ≤ 16000**。jieba 单文档通常远低于此，但要在 Phase 2 契约里断言截断策略。
- schema-per-instance × HNSW × 1000 实例（R6）真实；Phase 2 骨架可延迟建索引，不必先改 D6。
- English Summary「Open service defaults: Postgres…」和 D9「现网 service 默认仍是 tcvdb」容易被读成已经翻转。改一句措辞即可。

---

## 5. §9 推荐答案（推荐，不是已决）

**R9.1 Redis key 布局是否公开？**  
**推荐：公开一份 key 规格，开源实现继续用 `tdai_memory_v2` + `{p:inst:tid:aid}`。**  
依据：hash tag 升级原因已写在开源注释里（`gateway/config.ts:571–575`）。缺的是私有模块里的具体 key 名（sk/bk/timer ZSET/lock）。  
- 若 owner 能确认或抽出 `state/redis-keys.ts` 规格：开源 Redis 与私有实现可混跑。  
- 若不能：用 `tdai_memory_v3` 并写明不可混跑。  
**爆炸半径：** 混跑会破坏锁/队列互斥（历史已经因此做过一次 prefix 切割）。规格文档本身零运行时风险。不要在 Phase 1 实现 Redis。

**R9.2 Phase 2 要不要 Postgres 元数据？**  
**推荐：要，作为 2.5，但现网 service 默认仍是 Mongo。**  
否则 `deploy/compose/open-service.yml` 仍暗绑 Mongo，开源 Service 组合不成立。元数据已有契约测试，是本 RFC 里最便宜的一块。  
**爆炸半径：** 现网无感（D9）。漏做则「开源默认」名不副实。不要提前到 Phase 1。

**R9.3 `sparsevec` vs `pg_jieba`？**  
**推荐：维持 D3，客户端 jieba → `sparsevec`；`tsvector('simple')` 仅英文/兜底。**  
`pg_jieba` 要自定义镜像，打断 `pgvector/pgvector:pg16` 最短路径。三种后端排序可比。  
**爆炸半径：** 进程内 jieba OOM 仍在（要到 Go sidecar 才离开 Core）；词表变更要 `reindexAll`（源 SQLite 无稀疏向量，迁移本来就要重算）。不要为了中文 FTS 绑死 Postgres 扩展。

**R9.4 Phase 3.4 只读 memory tree？**  
**推荐：从本 RFC 范围删掉，另立产品 RFC。**  
blob 布局确实像文件系统，但 tree/cat 是 API + ACL + 可观测性，不是存储可插拔。Owner 目标是补 Cursor 记忆面、最终替代 OpenViking/WeKnora；把 tree 挂在存储 RFC 里会稀释第一刀。  
**爆炸半径：** 做了就要公开 `/v3/memory/tree|cat`，隔离/鉴权必须先定义；不做不影响 Phase 1–2。

---

## 6. Grok Phase 1 应做 / 禁止做

**应做（第一刀，接受 RFC 之后）：**

1. `StoreBackendRegistry` + `backends/sqlite.factory.ts`，从 `StorePool.createSqliteStore` / `getSqlitePath` 抽出；`createStoreBundle` 走同一 registry。  
2. 现有 TCVDB factory **原样搬家**（可选同 PR），行为零变化，含 `vdbConfig==null → sqlite` 这条隐患先**原样保留**并加测试，不要顺手改成抛错。  
3. 扩展能力矩阵（B1）；`memory-store.contract.ts` 先让 SQLite 全绿，TCVDB 无凭证 skip。  
4. 一页「RPC 可映射子集」说明（§7），不是 72 方法全译。  
5. 文档：注册表存在；`STORE_MODE` 仍是 sqlite/tcvdb；**不**宣布 Postgres。

**禁止：**

- 新 TCVDB 功能、新 COS/Redis 实现、Postgres/S3/MinIO、开源 Redis backend。  
- 改 `appendObject` 可选、JSONL 分片、`StoragePaths.recordShard`。  
- Skill 缺凭证改 fail-hard。  
- 翻转 `deployMode=service` 默认（仍是 tcvdb+cos+redis+mongo）。  
- 碰 MemoryKnowledge / Proxy / Panel 存储。  
- 写 Go/Rust，生成 gRPC stub，引入新 npm 依赖。  
- 把 `MetadataStorePool` 并进 Store registry。  
- 重写 RFC 成长文。

第二刀（人接受后，仍不是本评审的工作）：Postgres/pgvector + S3 兼容骨架 **或** `deploy/compose/open-service.yml` 文档化部署，不是生产迁移。

---

## 7. Phase 0–3 跳过 Go 是否可接受？

**跳过 Go/Rust 源码：可接受。** 现在最贵的是「开源树跑不起 Service」和「后端写死」，不是语言。Proxy/Panel 留 Node 正确。Knowledge 摄取重写是另一条 RFC。

**跳过数据面边界、把语言投诉推到 Phase 4 + 一份 Phase 2 `.proto` 文档：不可接受，是在糊弄。**  
`IMemoryStore` 明确 Sync-first，与 gRPC 不是 1:1。Phase 2.8 译 72 个方法（含 49 个 optional、callback、`Float32Array`）只会产出不能实现的草案，并让「Core 永远纯 TS」成为既成事实。这正是相对 OpenViking（Python + Rust/Go/C++）和 WeKnora（Go + Vue）的投诉。

**Phase 1 代码之前最小边界（仍不写 Go）：**

1. **冻结 in-process 契约与 remote 子集分离。** 进程内适配器可继续 `MaybePromise` + SQLite sync。Remote 子集 = 全部 `Promise`、无 callback、向量用 `number[]` / bytes、错误显式而非「吞掉变空数组」（与 `types.ts:12–13` 的 fault-tolerant 原则冲突，必须写明 remote 不吞错）。  
2. **Remote 子集 ⊆ required 24 方法 + profiles/pagination/clear/audit**（开源 Service 要的），**排除** entity CRUD（v3 已在 `IMetadataStore`）、`updateL0Embedding`、`reindexAll.onProgress`。`reindexAll` 若保留，做成无 callback + 返回计数。  
3. **一页 `docs/store-remote-subset.md`**（~40 行：方法表 + 类型映射 + 非目标）。这就是「边界存在」的证据。完整 `.proto` 可留 Phase 2，但必须译这份子集，不是整接口。  
4. **第一个未来 Go 组件仍应是 BM25 + hybrid rerank sidecar**（jieba OOM 已写在 `store-pool.ts`），不是重写 store。写进 D8，避免 Phase 2 被要求「顺便上 Go」。

没有以上 1–3，不要开 Phase 1 实现。有了之后，Phase 0–3 不写 Go 就是正确的排序，而不是拖延。
