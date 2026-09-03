# Store remote subset (Phase 1 freeze)

Core is TypeScript this phase. This page freezes the process/language
boundary so Core is not locked into Sync-first forever. Not a `.proto`.
No gRPC stubs.

## In-process contract (today)

`IMemoryStore` returns `MaybePromise<T>`: SQLite may be sync (`DatabaseSync`),
TCVDB is async. Callers always `await`. Many methods swallow errors into
empty/`false`. `reindexAll(embedFn, onProgress?)` takes in-process callbacks.

## Remote subset

The remote subset is ⊆ the 24 required `IMemoryStore` methods
(`REQUIRED_MEMORY_STORE_METHODS` in `MemoryCore/src/core/store/types.ts`)
plus profiles, pagination, `clearMemoryContent`, and audit.

Rules:

- Promise-only. No `MaybePromise`, no sync returns.
- No callbacks. Progress/reindex is poll or stream, not an in-process fn.
- Vectors as `number[]` or bytes — not `Float32Array`.
- Remote does **not** swallow errors. In-process fault-tolerance stays in
  the local adapter.

Out of subset this phase: entities, prompts, generationRefs, knowledge
(optional in-process). Skill stays same-DB SQLite (`getRawDb()`); do not
fold `MetadataStorePool` into the memory-store registry.

## First future Go component (not built)

A BM25/jieba sidecar. Core stays TypeScript; the sidecar would implement
sparse encoding over this remote subset, not replace `IMemoryStore`.
