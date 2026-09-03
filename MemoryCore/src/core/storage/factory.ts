/**
 * Storage Backend Factory — creates the appropriate IStorageBackend
 * based on configuration.
 *
 * The default `local` backend is bundled with core. Optional `cos` and `s3`
 * backends are loaded dynamically.
 */

import type { IStorageBackend, StorageBackendConfig, StorageLogger } from "./types.js";
import { LocalStorageBackend } from "./local-backend.js";

const TAG = "[storage][factory]";

/**
 * Create a storage backend instance based on configuration.
 *
 * Async because optional COS / S3 backends are dynamically imported only when needed.
 *
 * @param config Backend configuration (type + backend-specific options)
 * @param logger Optional logger
 * @returns IStorageBackend instance
 */
export async function createStorageBackend(
  config: StorageBackendConfig,
  logger?: StorageLogger,
): Promise<IStorageBackend> {
  switch (config.type) {
    case "s3": {
      const s3cfg = config.s3;
      if (!s3cfg?.bucket || !s3cfg.accessKeyId || !s3cfg.secretAccessKey) {
        throw new Error(`${TAG} S3 backend requires s3.bucket, s3.accessKeyId, and s3.secretAccessKey`);
      }
      const { S3StorageBackend } = await import("./s3-backend.js");
      logger?.info(`${TAG} Creating S3 storage backend: bucket=${s3cfg.bucket} endpoint=${s3cfg.endpoint ?? "(aws)"}`);
      const backend = new S3StorageBackend({ ...s3cfg, logger });
      await backend.ensureBucket();
      return backend;
    }

    case "cos": {
      if (!config.credentialProvider) {
        throw new Error(`${TAG} COS backend requires a credentialProvider`);
      }

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

      logger?.info(`${TAG} Creating COS storage backend`);
      return new CosStorageBackendCtor({
        credentialProvider: config.credentialProvider,
        logger,
      });
    }

    case "local":
    default: {
      const rootDir = config.localRootDir ?? "./data/storage";
      logger?.info(`${TAG} Creating local storage backend: rootDir=${rootDir}`);
      return new LocalStorageBackend({
        rootDir,
        logger,
      });
    }
  }
}

/**
 * Create a local storage backend for development.
 * Convenience helper for quick local setup.
 */
export function createLocalStorageBackend(
  rootDir: string,
  logger?: StorageLogger,
): IStorageBackend {
  return new LocalStorageBackend({ rootDir, logger });
}
