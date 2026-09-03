/**
 * Client-side jieba/BM25 → pgvector sparsevec literals.
 *
 * No pg_jieba: tokenization and BM25 encoding stay in-process
 * (@tencentdb-agent-memory/tcvdb-text / @node-rs/jieba), matching TCVDB.
 * pgvector stores the already-encoded sparse vector.
 *
 * pgvector sparsevec is 1-based: `{1:0.5,3:0.2}/250002`.
 */

/** Default BM25 vocab size used by tcvdb-text zh/en pretrained params. */
export const DEFAULT_SPARSE_DIMENSIONS = 250002;

export type SparsePair = [number, number];

/**
 * Normalize BM25 encoder output into [index, value] pairs.
 * Accepts Array<[i,v]>, number[][], or {indices, values}.
 */
export function sparseToPairs(sparse: unknown): SparsePair[] {
  if (!sparse) return [];
  if (Array.isArray(sparse)) {
    if (sparse.length === 0) return [];
    const first = sparse[0];
    if (Array.isArray(first) && first.length >= 2) {
      const out: SparsePair[] = [];
      for (const item of sparse) {
        if (!Array.isArray(item) || item.length < 2) continue;
        const idx = Number(item[0]);
        const val = Number(item[1]);
        if (!Number.isFinite(idx) || !Number.isFinite(val) || val === 0) continue;
        out.push([idx, val]);
      }
      return out;
    }
    return [];
  }
  if (typeof sparse === "object") {
    const obj = sparse as { indices?: unknown; values?: unknown };
    if (Array.isArray(obj.indices) && Array.isArray(obj.values)) {
      const out: SparsePair[] = [];
      for (let i = 0; i < obj.indices.length; i++) {
        const idx = Number(obj.indices[i]);
        const val = Number(obj.values[i] ?? 0);
        if (!Number.isFinite(idx) || !Number.isFinite(val) || val === 0) continue;
        out.push([idx, val]);
      }
      return out;
    }
  }
  return [];
}

/**
 * Format a sparse vector as a pgvector sparsevec literal.
 * Input indices are treated as 0-based (BM25 vocab); output is 1-based.
 */
export function toSparsevecLiteral(
  sparse: unknown,
  dimensions = DEFAULT_SPARSE_DIMENSIONS,
): string | null {
  const pairs = sparseToPairs(sparse);
  if (pairs.length === 0) return null;
  const parts: string[] = [];
  for (const [idx, val] of pairs) {
    const pgIdx = idx + 1; // BM25 vocab is 0-based; pgvector sparsevec is 1-based
    if (pgIdx < 1 || pgIdx > dimensions) continue;
    parts.push(`${pgIdx}:${val}`);
  }
  if (parts.length === 0) return null;
  return `{${parts.join(",")}}/${dimensions}`;
}

/** Dense embedding → pgvector `vector` literal `[0.1,0.2,...]`. */
export function toVectorLiteral(embedding: Float32Array | number[]): string {
  const arr = embedding instanceof Float32Array ? Array.from(embedding) : embedding;
  return `[${arr.join(",")}]`;
}

/**
 * Strip FTS5 syntax (`"term" OR "term2"`) produced by buildFtsQuery()
 * so the remaining text can be BM25-encoded for sparsevec search.
 */
export function ftsQueryToText(ftsQuery: string): string {
  return ftsQuery
    .replace(/"/g, " ")
    .replace(/\bOR\b/gi, " ")
    .replace(/\bAND\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}
