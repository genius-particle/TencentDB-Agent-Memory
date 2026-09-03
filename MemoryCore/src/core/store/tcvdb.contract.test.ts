import { describe, it } from "vitest";

/**
 * TCVDB IMemoryStore contract. This PR does not require Tencent cloud
 * credentials — the suite skips unless VDB_* env vars are present.
 *
 * When credentials exist, a follow-up can construct TcvdbMemoryStore via
 * StoreBackendRegistry and call runMemoryStoreContract("tcvdb", ...).
 * Capability flags that TCVDB does not implement (entities) skip inside
 * the shared contract.
 */
const hasTcvdbCreds = Boolean(
  process.env.VDB_ENDPOINT?.trim() &&
  process.env.VDB_API_KEY?.trim() &&
  process.env.VDB_DATABASE?.trim(),
);

describe.skipIf(!hasTcvdbCreds)("IMemoryStore contract: tcvdb", () => {
  it.skip("runMemoryStoreContract against a live TcvdbMemoryStore (needs VDB credentials)", () => {
    // Placeholder: do not construct TCVDB without an explicit live-env follow-up.
  });
});
