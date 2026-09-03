import { describe, expect, it } from "vitest";
import {
  assertMetadataStoreConfigExclusive,
  createMetadataStore,
  hasExplicitPostgresMetadataEnv,
  loadStoreConfig,
  MetadataStartupValidationError,
  validateMetadataStartupConfig,
} from "./factory.js";
import { postgresMetadataSchemaForInstance } from "./db-name.js";

describe("metadata store factory (no database required)", () => {
  it("does not select postgres from DATABASE_URL / PG* alone", () => {
    expect(hasExplicitPostgresMetadataEnv({ DATABASE_URL: "postgres://memory/memory" })).toBe(false);
    expect(hasExplicitPostgresMetadataEnv({ PGHOST: "localhost", PGDATABASE: "memory" })).toBe(false);
    expect(loadStoreConfig({ DATABASE_URL: "postgres://memory/memory" }).backend).toBe("sqlite");
  });

  it("selects postgres only when backend or dedicated URL is explicit", () => {
    expect(hasExplicitPostgresMetadataEnv({ TDAI_METADATA_BACKEND: "postgres" })).toBe(true);
    expect(hasExplicitPostgresMetadataEnv({ TDAI_METADATA_POSTGRES_URL: "postgres://meta/meta" })).toBe(true);

    const fromBackend = loadStoreConfig({
      TDAI_METADATA_BACKEND: "postgres",
      DATABASE_URL: "postgres://memory/memory",
    });
    expect(fromBackend.backend).toBe("postgres");
    expect(fromBackend.postgresUrl).toBe("postgres://memory/memory");

    const fromUrl = loadStoreConfig({
      TDAI_METADATA_POSTGRES_URL: "postgres://meta/meta",
    });
    expect(fromUrl.backend).toBe("postgres");
    expect(fromUrl.postgresUrl).toBe("postgres://meta/meta");
  });

  it("prefers TDAI_METADATA_POSTGRES_URL over DATABASE_URL", () => {
    const cfg = loadStoreConfig({
      TDAI_METADATA_BACKEND: "postgres",
      TDAI_METADATA_POSTGRES_URL: "postgres://meta/meta",
      DATABASE_URL: "postgres://memory/memory",
    });
    expect(cfg.postgresUrl).toBe("postgres://meta/meta");
  });

  it("keeps mongodb inference when mongo URI is set (no postgres env)", () => {
    const cfg = loadStoreConfig({
      TDAI_METADATA_MONGO_URI: "mongodb://localhost:27017",
      DATABASE_URL: "postgres://memory/memory",
    });
    expect(cfg.backend).toBe("mongodb");
    expect(cfg.mongoUri).toBe("mongodb://localhost:27017");
  });

  it("service mode still requires Mongo without explicit postgres metadata env", () => {
    expect(() => validateMetadataStartupConfig("service", {})).toThrow(MetadataStartupValidationError);
    expect(() =>
      validateMetadataStartupConfig("service", { DATABASE_URL: "postgres://memory/memory" }),
    ).toThrow(/TDAI_METADATA_MONGO_URI is required when deployMode=service/);

    const mongo = validateMetadataStartupConfig("service", {
      TDAI_METADATA_MONGO_URI: "mongodb://localhost:27017",
    });
    expect(mongo.backend).toBe("mongodb");
  });

  it("service mode accepts explicit postgres without Mongo", () => {
    const cfg = validateMetadataStartupConfig("service", {
      TDAI_METADATA_BACKEND: "postgres",
      DATABASE_URL: "postgres://memory/memory",
    });
    expect(cfg.backend).toBe("postgres");
    expect(cfg.postgresUrl).toBe("postgres://memory/memory");
  });

  it("rejects mixing mongo / sqlite / postgres explicit env", () => {
    expect(() =>
      assertMetadataStoreConfigExclusive({
        TDAI_METADATA_MONGO_URI: "mongodb://localhost:27017",
        TDAI_METADATA_BACKEND: "postgres",
      }),
    ).toThrow(MetadataStartupValidationError);
    expect(() =>
      assertMetadataStoreConfigExclusive({
        TDAI_METADATA_SQLITE_BASE_DIR: "./data/metadata",
        TDAI_METADATA_POSTGRES_URL: "postgres://meta/meta",
      }),
    ).toThrow(MetadataStartupValidationError);
    expect(() =>
      assertMetadataStoreConfigExclusive({
        TDAI_METADATA_MONGO_URI: "mongodb://localhost:27017",
        TDAI_METADATA_SQLITE_BASE_DIR: "./data/metadata",
      }),
    ).toThrow(MetadataStartupValidationError);
  });

  it("does not reuse mysql as postgres; mysql stays unimplemented", async () => {
    await expect(createMetadataStore({ backend: "mysql" }, "default")).rejects.toThrow(
      "MySQL backend not yet implemented",
    );
    await expect(createMetadataStore({ backend: "postgres" }, "default")).rejects.toThrow(
      /TDAI_METADATA_POSTGRES_URL or DATABASE_URL/,
    );
  });

  it("standalone without any metadata env stays sqlite", () => {
    const cfg = validateMetadataStartupConfig("standalone", {});
    expect(cfg.backend).toBe("sqlite");
  });

  it("sanitizes postgres metadata schema names separately from mem_*", () => {
    expect(postgresMetadataSchemaForInstance("default")).toBe("tdai_metadata_default");
    expect(postgresMetadataSchemaForInstance("inst-9")).toBe("tdai_metadata_inst_9");
  });
});
