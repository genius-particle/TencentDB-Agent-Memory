# Open-service compose (Phase 2)

Optional stack: **Postgres+pgvector** (memory store) + **MinIO** (blobs) + **Redis** (state).

This directory is unused unless you start it. It does **not** change
`deployMode=service` production defaults (`tcvdb+cos+redis+mongo`).

```bash
docker compose -f deploy/compose/open-service.yml up -d
```

Then point a **standalone** Gateway at it (see comments in `open-service.yml`):

| Env | Value |
| --- | --- |
| `STORE_MODE` | `postgres` |
| `STORAGE_BACKEND` | `s3` |
| `DATABASE_URL` | `postgres://memory:memory@127.0.0.1:5432/memory` |
| `S3_ENDPOINT` | `http://127.0.0.1:9000` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` |
| `S3_BUCKET` | `tdai-memory` |
| `STATE_BACKEND` | `redis` |
| `TDAI_DEPLOY_MODE` | leave unset / `standalone` |

Metadata stays local SQLite (Phase 3). Do not set `TDAI_METADATA_MONGO_URI` unless you intend to use Mongo.

See [docs/store-open-backends.md](../../docs/store-open-backends.md).
