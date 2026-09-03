/**
 * Built-in IMemoryStore backend registration.
 *
 * Import this module (or call ensureBuiltinStoreBackends) before resolving
 * stores so sqlite + tcvdb are first-class registry entries.
 */

import { defaultStoreBackendRegistry, type StoreBackendRegistry } from "./registry.js";
import { sqliteBackendFactory } from "./sqlite-backend.js";
import { tcvdbBackendFactory } from "./tcvdb-backend.js";

export function ensureBuiltinStoreBackends(
  registry: StoreBackendRegistry = defaultStoreBackendRegistry,
): StoreBackendRegistry {
  if (!registry.has("sqlite")) {
    registry.register(sqliteBackendFactory);
  }
  if (!registry.has("tcvdb")) {
    registry.register(tcvdbBackendFactory);
  }
  return registry;
}

ensureBuiltinStoreBackends();
