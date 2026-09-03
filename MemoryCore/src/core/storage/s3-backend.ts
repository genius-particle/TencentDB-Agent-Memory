/**
 * S3-compatible blob backend (MinIO / AWS S3) as an optional IStorageBackend.
 *
 * Does not change CosStorageBackend appendObject / JSONL key layout.
 * Live COS is unchanged when STORAGE_BACKEND is unset.
 *
 * appendObject is read-modify-write: S3 has no COS-style Append Object API.
 * JSONL keys stay StoragePaths (conversations/{date}.jsonl, records/{date}.jsonl).
 */

import type {
  IStorageBackend,
  StorageObject,
  PutObjectOptions,
  ListObjectsOptions,
  ListResult,
  ListEntry,
  StorageLogger,
} from "./types.js";

const TAG = "[storage][s3]";

export interface S3StorageBackendOptions {
  endpoint?: string;
  region?: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  /** Key prefix, e.g. "tenant/instance/". */
  prefix?: string;
  /** Required for MinIO. Default true when endpoint is set. */
  forcePathStyle?: boolean;
  logger?: StorageLogger;
}

type S3Module = typeof import("@aws-sdk/client-s3");

function joinKey(prefix: string, key: string): string {
  const p = prefix.replace(/^\/+/, "").replace(/\/+$/, "");
  const k = key.replace(/^\/+/, "");
  if (!p) return k;
  return k ? `${p}/${k}` : `${p}/`;
}

function stripPrefix(prefix: string, fullKey: string): string {
  const p = prefix.replace(/^\/+/, "").replace(/\/+$/, "");
  if (!p) return fullKey;
  const withSlash = `${p}/`;
  if (fullKey.startsWith(withSlash)) return fullKey.slice(withSlash.length);
  if (fullKey === p) return "";
  return fullKey;
}

export function resolveS3Config(env: NodeJS.ProcessEnv = process.env): S3StorageBackendOptions | undefined {
  const bucket = (env.S3_BUCKET ?? env.MINIO_BUCKET ?? "").trim();
  const accessKeyId = (env.S3_ACCESS_KEY ?? env.AWS_ACCESS_KEY_ID ?? env.MINIO_ROOT_USER ?? "").trim();
  const secretAccessKey = (env.S3_SECRET_KEY ?? env.AWS_SECRET_ACCESS_KEY ?? env.MINIO_ROOT_PASSWORD ?? "").trim();
  if (!bucket || !accessKeyId || !secretAccessKey) return undefined;
  const endpoint = (env.S3_ENDPOINT ?? env.MINIO_ENDPOINT ?? "").trim() || undefined;
  return {
    endpoint,
    region: (env.S3_REGION ?? env.AWS_REGION ?? "us-east-1").trim() || "us-east-1",
    bucket,
    accessKeyId,
    secretAccessKey,
    sessionToken: env.AWS_SESSION_TOKEN?.trim() || undefined,
    prefix: (env.S3_PREFIX ?? "").trim() || undefined,
    forcePathStyle: env.S3_FORCE_PATH_STYLE === "false" ? false : Boolean(endpoint) || env.S3_FORCE_PATH_STYLE === "true",
  };
}

export function resolveBlobBackendKind(
  deployMode: string,
  env: NodeJS.ProcessEnv = process.env,
): "s3" | "cos" | "local" {
  const override = (env.STORAGE_BACKEND ?? env.TDAI_STORAGE_BACKEND ?? "").trim().toLowerCase();
  if (override === "s3" || override === "minio") return "s3";
  if (override === "local") return "local";
  if (override === "cos") return "cos";
  return deployMode === "service" ? "cos" : "local";
}

export class S3StorageBackend implements IStorageBackend {
  readonly type = "s3" as const;
  private readonly opts: S3StorageBackendOptions;
  private readonly logger?: StorageLogger;
  private readonly prefix: string;
  private client: InstanceType<S3Module["S3Client"]> | null = null;
  private s3: S3Module | null = null;

  constructor(opts: S3StorageBackendOptions) {
    this.opts = opts;
    this.logger = opts.logger;
    const p = (opts.prefix ?? "").replace(/^\/+/, "").replace(/\/+$/, "");
    this.prefix = p ? `${p}/` : "";
  }

  private async sdk(): Promise<S3Module> {
    if (this.s3) return this.s3;
    try {
      this.s3 = await import("@aws-sdk/client-s3");
      return this.s3;
    } catch (err) {
      throw new Error(
        `${TAG} @aws-sdk/client-s3 is not installed; add it to use STORAGE_BACKEND=s3. ` +
        `Original error: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  private async getClient(): Promise<InstanceType<S3Module["S3Client"]>> {
    if (this.client) return this.client;
    const s3 = await this.sdk();
    this.client = new s3.S3Client({
      region: this.opts.region ?? "us-east-1",
      endpoint: this.opts.endpoint,
      forcePathStyle: this.opts.forcePathStyle ?? Boolean(this.opts.endpoint),
      credentials: {
        accessKeyId: this.opts.accessKeyId,
        secretAccessKey: this.opts.secretAccessKey,
        sessionToken: this.opts.sessionToken,
      },
    });
    return this.client;
  }

  private fullKey(key: string): string {
    if (key.includes("\0") || key.startsWith("/") || key.startsWith("\\")) {
      throw new Error(`Invalid storage key: ${JSON.stringify(key)}`);
    }
    if (key.split("/").some((part) => part === "..")) {
      throw new Error(`Path traversal rejected: key "${key}"`);
    }
    return joinKey(this.prefix, key);
  }

  async ensureBucket(): Promise<void> {
    const s3 = await this.sdk();
    const client = await this.getClient();
    try {
      await client.send(new s3.HeadBucketCommand({ Bucket: this.opts.bucket }));
    } catch {
      try {
        await client.send(new s3.CreateBucketCommand({ Bucket: this.opts.bucket }));
        this.logger?.info?.(`${TAG} created bucket ${this.opts.bucket}`);
      } catch (err) {
        this.logger?.warn?.(
          `${TAG} ensureBucket ${this.opts.bucket}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }

  async putObject(key: string, content: string | Buffer, opts?: PutObjectOptions): Promise<void> {
    if (!key) throw new Error(`Invalid storage key: ${JSON.stringify(key)}`);
    const s3 = await this.sdk();
    const client = await this.getClient();
    const body = typeof content === "string" ? Buffer.from(content, "utf-8") : content;
    await client.send(new s3.PutObjectCommand({
      Bucket: this.opts.bucket,
      Key: this.fullKey(key),
      Body: body,
      ContentType: opts?.contentType,
      Metadata: opts?.metadata,
    }));
    this.logger?.debug?.(`${TAG} putObject: ${key} (${body.length} bytes)`);
  }

  /**
   * S3 has no native append. Read-modify-write preserves JSONL key layout
   * used by COS (`conversations/{date}.jsonl`, `records/{date}.jsonl`).
   * Concurrent appends to the same key can race — unlike COS Append Object.
   */
  async appendObject(key: string, content: string | Buffer): Promise<void> {
    const existing = await this.getObject(key);
    const chunk = typeof content === "string" ? Buffer.from(content, "utf-8") : content;
    const combined = existing ? Buffer.concat([existing.content, chunk]) : chunk;
    await this.putObject(key, combined, existing?.contentType ? { contentType: existing.contentType } : undefined);
    this.logger?.debug?.(`${TAG} appendObject: ${key} (+${chunk.length} bytes, rmw)`);
  }

  async getObject(key: string): Promise<StorageObject | null> {
    const s3 = await this.sdk();
    const client = await this.getClient();
    try {
      const resp = await client.send(new s3.GetObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.fullKey(key),
      }));
      const bytes = resp.Body ? Buffer.from(await resp.Body.transformToByteArray()) : Buffer.alloc(0);
      return {
        key,
        content: bytes,
        contentType: resp.ContentType,
        metadata: resp.Metadata,
        lastModified: resp.LastModified,
        size: resp.ContentLength ?? bytes.length,
      };
    } catch (err) {
      const name = (err as { name?: string }).name ?? "";
      if (name === "NoSuchKey" || name === "NotFound") return null;
      const http = (err as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
      if (http === 404) return null;
      throw err;
    }
  }

  async exists(key: string): Promise<boolean> {
    const s3 = await this.sdk();
    const client = await this.getClient();
    try {
      await client.send(new s3.HeadObjectCommand({
        Bucket: this.opts.bucket,
        Key: this.fullKey(key),
      }));
      return true;
    } catch (err) {
      const name = (err as { name?: string }).name ?? "";
      if (name === "NotFound" || name === "NoSuchKey") return false;
      const http = (err as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
      if (http === 404) return false;
      throw err;
    }
  }

  async listObjects(prefix: string, opts?: ListObjectsOptions): Promise<ListResult> {
    const s3 = await this.sdk();
    const client = await this.getClient();
    const maxKeys = opts?.maxKeys ?? 100;
    const recursive = opts?.recursive ?? false;
    const fullPrefix = this.fullKey(prefix);
    const resp = await client.send(new s3.ListObjectsV2Command({
      Bucket: this.opts.bucket,
      Prefix: fullPrefix,
      MaxKeys: maxKeys,
      ContinuationToken: opts?.marker,
      Delimiter: recursive ? undefined : "/",
    }));

    const entries: ListEntry[] = [];
    for (const p of resp.CommonPrefixes ?? []) {
      if (!p.Prefix) continue;
      entries.push({
        key: stripPrefix(this.prefix, p.Prefix),
        size: 0,
        lastModified: new Date(0),
        isDirectory: true,
      });
    }
    for (const obj of resp.Contents ?? []) {
      if (!obj.Key) continue;
      entries.push({
        key: stripPrefix(this.prefix, obj.Key),
        size: obj.Size ?? 0,
        lastModified: obj.LastModified ?? new Date(0),
        isDirectory: false,
      });
    }
    return {
      entries,
      nextMarker: resp.IsTruncated ? resp.NextContinuationToken : undefined,
      total: resp.KeyCount,
    };
  }

  async deleteObject(key: string): Promise<void> {
    const s3 = await this.sdk();
    const client = await this.getClient();
    await client.send(new s3.DeleteObjectCommand({
      Bucket: this.opts.bucket,
      Key: this.fullKey(key),
    }));
    this.logger?.debug?.(`${TAG} deleteObject: ${key}`);
  }

  async deleteByPrefix(prefix: string): Promise<number> {
    let deleted = 0;
    let marker: string | undefined;
    for (;;) {
      const page = await this.listObjects(prefix, { maxKeys: 1000, recursive: true, marker });
      const files = page.entries.filter((e) => !e.isDirectory);
      for (const entry of files) {
        await this.deleteObject(entry.key);
        deleted++;
      }
      if (!page.nextMarker) break;
      marker = page.nextMarker;
    }
    this.logger?.debug?.(`${TAG} deleteByPrefix: ${prefix} (${deleted} files)`);
    return deleted;
  }
}
