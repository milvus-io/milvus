# StreamingClient

In-process singleton library (accessed via `streaming.WAL()`) providing the client-side API for WAL operations: Append, Read, Broadcast, and Txn.

Append and Read route to the StreamingNode owning the target PChannel via [Channel Management](../coordination/channel_management.md) service discovery, and auto-recover from StreamingNode failures. When the PChannel is assigned to the local StreamingNode (same process), they bypass gRPC and operate directly on the local WAL instance. Broadcast delegates to StreamingCoord's [Broadcaster](../coordination/broadcaster.md) via gRPC.

## Key Packages

- `internal/distributed/streaming/` — `WALAccesser` singleton, producer, consumer
- `internal/streamingnode/client/` — `HandlerClient`, local/remote dispatch

## Analyzer

`streaming.WAL().AnalyzerClient()` exposes the handler client's native SN analyzer
API. It reuses streaming discovery and connections independently of QueryView.
See [Analyzer execution](analyzer.md) for routing, schema and lifetime rules.

## Collection schemas

`streaming.WAL().SchemaClient()` caches immutable `schemautil.SchemaInfo` values,
including Proxy field indices and a user-field projection. `FullSchema` retains
the complete schema, including system fields, for native node consumers.
Current-write lookups confirm write admission with SN on every call but reuse the
compiled object for the same cached collection/version. Exact-version reads,
including zero, use the cache directly.

Both request types accept `BypassCache`. A refresh uses a new collection epoch,
bypasses older flights and logically invalidates metadata in all cached versions
of that collection without scanning other entries. Older RPC completions cannot
publish over the new epoch. Cache bodies and epochs expire when idle.

Proxy MetaCache refreshes the current write schema before publishing a miss fill.
`CollectionInfo` stores routing metadata and the published read version, not a
schema pointer. Its `GetReadSchema`/`GetWriteSchema` methods return SchemaClient's
shared object without per-request metadata projection, shallow copies or index
reconstruction. Read and write versions remain independent. SN exact-version
responses combine that version's fields/TTL with current non-versioned metadata;
allocation-time WAL history remains unchanged. AlterCollection's consuming-side
ACK precedes coordinator callback/invalidation, so the subsequent refresh can
observe the update even when SchemaVersion did not change.
