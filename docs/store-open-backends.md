# Open-service backends (Phase 2–3)

Opt-in Postgres+pgvector memory store, Postgres metadata store, and S3-compatible blob store.
Does **not** change live `deployMode=service` defaults (`tcvdb+cos+redis+mongo`).
Closed RFC PR #1 is not reopened. Review PR #2 is not merged.

## Selecting backends

| Surface | How to select postgres / MinIO | Default when unset |
| --- | --- | --- |
| Gateway `StorePool` (memory) | `STORE_MODE=postgres` | `deployMode=service` → `tcvdb`; standalone → `sqlite` |
| Plugin `createStoreBundle` | `storeBackend: "postgres"` | `sqlite` |
| Metadata `IMetadataStore` | `TDAI_METADATA_BACKEND=postgres` and/or `TDAI_METADATA_POSTGRES_URL` | standalone → `sqlite`; `deployMode=service` → **requires Mongo** |
| Blobs (`IStorageBackend`) | `STORAGE_BACKEND=s3` (or `minio`) | service → COS; standalone → local |

`StoreBackendRegistry` registers `sqlite`, `tcvdb`, and `postgres` for **memory**.
Metadata backends are `sqlite` | `mongodb` | `postgres` (`mysql` remains unimplemented and is **not** the postgres id).

`DATABASE_URL` / `PG*` alone does **not** switch metadata off Mongo. Those variables are the memory-store (and optional metadata) connection string, selected only after metadata backend is explicit.

The documented silent fallback `mode===tcvdb && !vdbConfig → sqlite` is unchanged.

## Postgres + pgvector (`PostgresMemoryStore`)

- Dense embeddings: pgvector `vector`.
- Sparse / FTS: **client-side jieba + BM25** (`@tencentdb-agent-memory/tcvdb-text`) written to `sparsevec`. **No pg_jieba**.
- Schema per instance: `mem_{instanceId}` (override with `memory.postgres.schema`).
- Connection: `DATABASE_URL` or `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/`PGDATABASE`, or `memory.postgres.url`.

Honest capability flags:

| Flag | Value | Notes |
| --- | --- | --- |
| `profiles` | true | L2/L3 profile rows for the open service path |
| `pagination` | true | |
| `audit` | true | |
| `clearMemoryContent` | true | |
| `deferredEmbedding` | true | |
| `ftsSearch` / `sparseVectors` | true when BM25 encoder is enabled | |
| `vectorSearch` | true when `embedding.dimensions > 0` | |
| `entities` / `knowledge` / `prompts` / `generationRefs` | **false** | not implemented this phase |

No new TCVDB behavior. Skill still uses same-DB SQLite (`getRawDb()`) or TCVDB skill collections; skill-config fallbacks are unchanged.

## Postgres metadata (`PostgresMetadataStore`)

Implements the existing `IMetadataStore` contract (`src/metadata/store/metadata-store.contract.ts`).

| Env | Role |
| --- | --- |
| `TDAI_METADATA_BACKEND=postgres` | Explicit backend id (required unless `TDAI_METADATA_POSTGRES_URL` is set) |
| `TDAI_METADATA_POSTGRES_URL` | Dedicated metadata URL (wins over `DATABASE_URL`) |
| `DATABASE_URL` / `PG*` | Connection fallback **only after** backend is explicit |
| `TDAI_METADATA_MONGO_URI` | Live service default; still required when `deployMode=service` and postgres metadata is **not** explicit |

- Schema per instance: `tdai_metadata_{instanceId}` (same prefix as Mongo/SQLite logical db names; hyphens sanitized). Distinct from memory `mem_*` schemas, so both can share one Postgres database.
- Yaml: `metadata.store.backend: postgres` and optional `metadata.store.postgresUrl`.
- Do not set `TDAI_DEPLOY_MODE=service` on the open compose path — that default still expects Mongo.

Exclusive: only one of Mongo URI, SQLite base dir, or explicit postgres metadata env may be set.

## S3 / MinIO (`S3StorageBackend`)

Optional `IStorageBackend` (`type: "s3"`). COS `appendObject` / JSONL layout is **not** modified.

- JSONL keys stay `conversations/{YYYY-MM-DD}.jsonl` and `records/{YYYY-MM-DD}.jsonl` (`StoragePaths`).
- `appendObject` is read-modify-write (S3 has no COS Append Object API). Concurrent appends can race.
- Live COS still retries and logs; it is **not** fail-hard.

Env: `S3_ENDPOINT`, `S3_ACCESS_KEY` / `MINIO_ROOT_USER`, `S3_SECRET_KEY` / `MINIO_ROOT_PASSWORD`, `S3_BUCKET`, `S3_REGION`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`.

Requires optional dependency `@aws-sdk/client-s3`.

## Compose

See [`deploy/compose/open-service.yml`](../deploy/compose/open-service.yml): Postgres+pgvector, MinIO, Redis. Metadata uses the same Postgres instance (`tdai_metadata_*` schemas).

```bash
docker compose -f deploy/compose/open-service.yml up -d
export STORE_MODE=postgres STORAGE_BACKEND=s3
export DATABASE_URL=postgres://memory:memory@127.0.0.1:5432/memory
export TDAI_METADATA_BACKEND=postgres
# optional dedicated URL; otherwise DATABASE_URL is used after the explicit backend
# export TDAI_METADATA_POSTGRES_URL=postgres://memory:memory@127.0.0.1:5432/memory
export S3_ENDPOINT=http://127.0.0.1:9000 S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin S3_BUCKET=tdai-memory
# TDAI_DEPLOY_MODE remains standalone (default) — do not flip service defaults
```

## Tests

- Shared `runMemoryStoreContract` against Postgres when `DATABASE_URL` / `PG*` is set; otherwise skip (same as TCVDB-without-creds).
- Shared `runMetadataStoreContract` against Postgres when `DATABASE_URL` / `PG*` is set; otherwise skip.
- `npm test` in MemoryCore stays green with no Postgres / MinIO / cloud credentials.
