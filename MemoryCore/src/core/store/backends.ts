/**
 * Built-in IMemoryStore backend registration.
 *
 * Import this module (or call ensureBuiltinStoreBackends) before resolving
 * stores so sqlite + tcvdb + postgres are first-class registry entries.
 */

import { defaultStoreBackendRegistry, type StoreBackendRegistry } from "./registry.js";
import { sqliteBackendFactory } from "./sqlite-backend.js";
import { tcvdbBackendFactory } from "./tcvdb-backend.js";
import { postgresBackendFactory } from "./postgres-backend.js";

export function ensureBuiltinStoreBackends(
  registry: StoreBackendRegistry = defaultStoreBackendRegistry,
): StoreBackendRegistry {
  if (!registry.has("sqlite")) {
    registry.register(sqliteBackendFactory);
  }
  if (!registry.has("tcvdb")) {
    registry.register(tcvdbBackendFactory);
  }
  if (!registry.has("postgres")) {
    registry.register(postgresBackendFactory);
  }
  return registry;
}

ensureBuiltinStoreBackends();
