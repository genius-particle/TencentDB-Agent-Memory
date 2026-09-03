import { describe, expect, it } from "vitest";
import { S3StorageBackend, resolveS3Config } from "./s3-backend.js";
import { createStorageBackend } from "./factory.js";
import { StoragePaths } from "./types.js";

describe("S3 storage helpers (no MinIO required)", () => {
  it("resolveS3Config returns undefined without bucket/keys", () => {
    expect(resolveS3Config({})).toBeUndefined();
    expect(resolveS3Config({
      S3_BUCKET: "tdai-memory",
      S3_ACCESS_KEY: "minioadmin",
      S3_SECRET_KEY: "minioadmin",
      S3_ENDPOINT: "http://127.0.0.1:9000",
    })).toMatchObject({
      bucket: "tdai-memory",
      endpoint: "http://127.0.0.1:9000",
      forcePathStyle: true,
    });
  });

  it("rejects path-traversal keys", async () => {
    const backend = new S3StorageBackend({
      bucket: "test",
      accessKeyId: "x",
      secretAccessKey: "y",
      endpoint: "http://127.0.0.1:9000",
    });
    await expect(backend.putObject("../secret", "x")).rejects.toThrow(/Path traversal/);
  });

  it("keeps COS JSONL key layout constants", () => {
    expect(StoragePaths.conversation("2026-09-03")).toBe("conversations/2026-09-03.jsonl");
    expect(StoragePaths.record("2026-09-03")).toBe("records/2026-09-03.jsonl");
  });
});

const hasS3 = Boolean(
  process.env.S3_ENDPOINT?.trim() &&
  process.env.S3_BUCKET?.trim() &&
  (process.env.S3_ACCESS_KEY ?? process.env.MINIO_ROOT_USER)?.trim() &&
  (process.env.S3_SECRET_KEY ?? process.env.MINIO_ROOT_PASSWORD)?.trim(),
);

describe.skipIf(!hasS3)("S3StorageBackend live (MinIO)", () => {
  it("put / get / append / delete round-trip with JSONL keys", async () => {
    const cfg = resolveS3Config()!;
    const backend = await createStorageBackend({ type: "s3", s3: { ...cfg, prefix: `it-${Date.now()}` } });
    expect(backend.type).toBe("s3");
    const key = StoragePaths.conversation("2026-09-03");
    await backend.putObject(key, "{\"a\":1}\n", { contentType: "application/x-ndjson" });
    await backend.appendObject(key, "{\"a\":2}\n");
    const obj = await backend.getObject(key);
    expect(obj?.content.toString("utf-8")).toBe("{\"a\":1}\n{\"a\":2}\n");
    await backend.deleteObject(key);
    expect(await backend.exists(key)).toBe(false);
  });
});
