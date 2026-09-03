/**
 * Postgres connection helpers for the open MemoryStore backend.
 *
 * Selection is explicit (STORE_MODE / storeBackend=postgres). This module
 * does not change live deployMode=service defaults (tcvdb+cos+redis+mongo).
 */

export interface PostgresConnectionOptions {
  connectionString: string;
  schema?: string;
}

/**
 * True when the process has enough env to attempt a Postgres connection.
 * Mirrors the TCVDB contract skip: no creds → skip, do not fail CI.
 */
export function hasPostgresEnv(env: NodeJS.ProcessEnv = process.env): boolean {
  return Boolean(
    env.DATABASE_URL?.trim() ||
    env.PGHOST?.trim() ||
    env.PGDATABASE?.trim(),
  );
}

/**
 * Build a connection string from DATABASE_URL or PG* variables.
 * Returns undefined when nothing usable is set.
 */
export function resolvePostgresConnection(
  env: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const url = env.DATABASE_URL?.trim();
  if (url) return url;

  const host = env.PGHOST?.trim();
  const database = env.PGDATABASE?.trim();
  if (!host && !database) return undefined;

  const user = env.PGUSER?.trim() || "postgres";
  const password = env.PGPASSWORD ?? "";
  const port = env.PGPORT?.trim() || "5432";
  const db = database || "postgres";
  const auth = password
    ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}`
    : encodeURIComponent(user);
  return `postgres://${auth}@${host || "127.0.0.1"}:${port}/${db}`;
}

/**
 * Per-instance schema name. Postgres identifiers max 63 bytes.
 * "default" → mem_default; other ids are sanitized to [a-z0-9_].
 */
export function postgresSchemaForInstance(instanceId = "default"): string {
  const raw = instanceId === "default" ? "mem_default" : `mem_${instanceId}`;
  let cleaned = raw.toLowerCase().replace(/[^a-z0-9_]/g, "_").slice(0, 63);
  if (!/^[a-z]/.test(cleaned)) cleaned = `m_${cleaned}`.slice(0, 63);
  return cleaned;
}
