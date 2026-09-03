# Open-service compose (Phase 2–3)

Optional stack: **Postgres+pgvector** (memory store) + **Postgres metadata** + **MinIO** (blobs) + **Redis** (state).

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
| `TDAI_METADATA_BACKEND` | `postgres` |
| `TDAI_METADATA_POSTGRES_URL` | optional; defaults to `DATABASE_URL` once backend is explicit |
| `S3_ENDPOINT` | `http://127.0.0.1:9000` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` |
| `S3_BUCKET` | `tdai-memory` |
| `STATE_BACKEND` | `redis` |
| `TDAI_DEPLOY_MODE` | leave unset / `standalone` |

Metadata uses schemas `tdai_metadata_*` on the same Postgres instance as memory (`mem_*`).
`DATABASE_URL` alone does **not** switch live service metadata off Mongo.

Do not set `TDAI_METADATA_MONGO_URI` on this path unless you intend to use Mongo.

See [docs/store-open-backends.md](../../docs/store-open-backends.md).
