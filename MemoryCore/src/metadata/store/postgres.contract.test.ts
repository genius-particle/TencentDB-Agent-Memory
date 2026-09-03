import { describe, expect, it } from "vitest";
import { hasPostgresEnv, resolvePostgresConnection } from "../../core/store/postgres-env.js";
import { runMetadataStoreContract } from "./metadata-store.contract.js";
import { PostgresMetadataStore } from "./postgres-adapter.js";
import { MetadataStorePool } from "./factory.js";
import type { IMetadataStore } from "./interface.js";

/**
 * Postgres IMetadataStore contract. Skips unless DATABASE_URL or PG* is set
 * (same pattern as TCVDB / memory postgres). `npm test` stays green in CI
 * without a Postgres instance.
 */
const hasPostgres = hasPostgresEnv();
const connectionString = resolvePostgresConnection();

describe.skipIf(!hasPostgres)("IMetadataStore contract: postgres", () => {
  runMetadataStoreContract(
    "postgres",
    async (): Promise<IMetadataStore> => {
      const schema = `meta_c_${process.pid}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
      return new PostgresMetadataStore({
        connectionString: connectionString!,
        schema,
        ownsPool: true,
      });
    },
    async (store) => {
      if (store instanceof PostgresMetadataStore) {
        await store.dropSchema().catch(() => {});
      }
      await Promise.resolve(store.close()).catch(() => {});
    },
  );
});

describe.skipIf(!hasPostgres)("MetadataStorePool postgres", () => {
  it("getStore and purgeInstance isolate schemas on a shared pool", async () => {
    const instanceId = `p3_${process.pid}_${Date.now().toString(36)}`;
    const pool = new MetadataStorePool({
      backend: "postgres",
      postgresUrl: connectionString,
    });
    try {
      const store = await pool.getStore(instanceId);
      const user = await store.createUser({
        auth_provider: "local",
        external_id: `ext-${instanceId}`,
        username: `user-${instanceId}`,
      });
      expect(user.user_id).toMatch(/^usr-/);
      expect(await store.countUsers()).toBe(1);

      const purged = await pool.purgeInstance(instanceId);
      expect(purged.dropped).toBe(true);

      const store2 = await pool.getStore(instanceId);
      expect(await store2.countUsers()).toBe(0);
    } finally {
      await pool.purgeInstance(instanceId).catch(() => {});
      await pool.closeAll();
    }
  });
});
