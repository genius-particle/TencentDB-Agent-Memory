import { describe, expect, it } from "vitest";
import { ftsQueryToText, sparseToPairs, toSparsevecLiteral, toVectorLiteral } from "./sparsevec.js";
import { hasPostgresEnv, resolvePostgresConnection } from "./postgres-env.js";
import { resolveBlobBackendKind } from "../storage/s3-backend.js";
import { buildFtsQuery } from "./sqlite.js";

describe("sparsevec literals (no database required)", () => {
  it("formats 0-based BM25 pairs as 1-based pgvector sparsevec", () => {
    expect(toSparsevecLiteral([[0, 0.5], [2, 0.25]], 10)).toBe("{1:0.5,3:0.25}/10");
  });

  it("accepts {indices, values} encoder shape", () => {
    expect(toSparsevecLiteral({ indices: [1, 4], values: [0.1, 0.2] }, 8)).toBe("{2:0.1,5:0.2}/8");
  });

  it("returns null for empty sparse vectors", () => {
    expect(toSparsevecLiteral([])).toBeNull();
    expect(sparseToPairs(null)).toEqual([]);
  });

  it("formats dense vector literals", () => {
    expect(toVectorLiteral([0.1, 0.2])).toBe("[0.1,0.2]");
  });

  it("strips FTS5 syntax from buildFtsQuery output", () => {
    const q = buildFtsQuery("unique-fts-token-alpha");
    expect(q).toBeTruthy();
    const text = ftsQueryToText(q!);
    expect(text.toLowerCase()).toContain("unique");
  });
});

describe("postgres env helpers", () => {
  it("hasPostgresEnv is false without DATABASE_URL/PG*", () => {
    expect(hasPostgresEnv({})).toBe(false);
    expect(hasPostgresEnv({ DATABASE_URL: "postgres://x" })).toBe(true);
    expect(hasPostgresEnv({ PGHOST: "localhost" })).toBe(true);
  });

  it("resolvePostgresConnection prefers DATABASE_URL", () => {
    expect(resolvePostgresConnection({ DATABASE_URL: "postgres://a/b" })).toBe("postgres://a/b");
    expect(resolvePostgresConnection({
      PGHOST: "db",
      PGPORT: "5433",
      PGUSER: "u",
      PGPASSWORD: "p",
      PGDATABASE: "mem",
    })).toBe("postgres://u:p@db:5433/mem");
  });
});

describe("blob backend kind (COS defaults unchanged)", () => {
  it("keeps service→cos and standalone→local when STORAGE_BACKEND is unset", () => {
    expect(resolveBlobBackendKind("service", {})).toBe("cos");
    expect(resolveBlobBackendKind("standalone", {})).toBe("local");
  });

  it("selects s3 only when STORAGE_BACKEND=s3", () => {
    expect(resolveBlobBackendKind("service", { STORAGE_BACKEND: "s3" })).toBe("s3");
    expect(resolveBlobBackendKind("standalone", { STORAGE_BACKEND: "minio" })).toBe("s3");
    expect(resolveBlobBackendKind("service", { STORAGE_BACKEND: "local" })).toBe("local");
  });
});
