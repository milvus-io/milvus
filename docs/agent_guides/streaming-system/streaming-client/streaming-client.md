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
including the complete schema and derived field indices. Native node consumers
and write-side Proxy validation use the complete schema, including RowID/Timestamp.
Write-side validation enforces the user-column constraints locally.
Current-write lookups cache both the selected version and its compiled schema,
so cache hits require no schema RPC. An append schema mismatch invalidates the
selector before the error reaches the caller; the next lookup refreshes from SN.
The failed mutation is not automatically rebuilt or retried by SchemaClient.
Exact-version reads, including zero, also use the cache directly.

Both request types accept `BypassCache`. A refresh uses a new collection epoch,
bypasses older flights and logically invalidates metadata in all cached versions
of that collection without scanning other entries. Older RPC completions cannot
publish over the new epoch. Refresh also clears the write selector: fetching an
exact read version does not establish the current write version. Cache bodies,
write selectors and epochs expire when idle.

On a miss, Proxy MetaCache fetches the published version from Coord, then obtains
that exact version through SchemaClient with `BypassCache`. It shallow-copies the
schema envelope and filters system fields, building a matching set of read indices
once before publishing `CollectionInfo.Schema`. Field nodes remain shared and
immutable. Read-side Proxy code uses this cached schema with its existing field
visibility; write-side code independently selects the current SN schema. Upsert
internal reads and Search requery retain their selected published read snapshot.

SN exact-version responses combine that version's fields/TTL with current
non-versioned metadata; allocation-time WAL history remains unchanged.
AlterCollection's consuming-side ACK precedes coordinator callback/invalidation,
so the subsequent refresh observes metadata updates even without a schema-version
change. MetaCache's fill lock orders the schema fetch and publication against
invalidation.
