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

Both request types accept `BypassCache`. Schema entries are keyed by collection
and SchemaVersion, with independent idle lifetimes and refresh tokens. Replacing
an entry separates its singleflight key and fences older completions; no separate
collection epoch map is needed. Refreshing one version leaves other versions
cached. Every bypass refresh clears the collection's write selector and fences
its pending write flights, regardless of the requested version. The next write
resolves the current version from SN, which may lead the published read version.
The write-version index is an ordinary map with no independent
TTL. Evicting a schema removes a selector pointing to that version, including
lazy expiry on access; other versions remain unaffected. Explicit invalidation
and client close also clear selectors. The index does not retain SchemaInfo objects.

Collection IDs route the schema cache and write index to 32 shards, each with one
mutex. Field-index construction runs outside that mutex and checks the entry's
identity again before publication. Schema entries form an idle queue: accesses
move an entry to the tail, and expiry removes only the expired prefix, at most 64
entries per lock acquisition. The shard releases its lock between batches. A
sweep with no expired entries checks only the head, not every cached version.
This bounds batch work, not wall-clock lock latency under arbitrary scheduling.

On a miss, Proxy MetaCache fetches the published version from Coord, then obtains
that exact version through SchemaClient with `BypassCache`. It shallow-copies the
schema envelope and filters system fields, building a matching set of read indices
once before publishing `CollectionInfo.Schema`. Field nodes remain shared and
immutable. Read-side Proxy code uses this cached schema with its existing field
visibility; write-side code independently selects the current SN schema. Upsert
internal reads and Search requery retain their selected published read snapshot.

SN exact-version lookups first try ShardManager: a matching current write version
is immediately available even before the recovery observer catches up. Other
versions fall back to RecoveryStorage; a different write version is never used
as a substitute.

SN exact-version responses return that version's own snapshot, without metadata
from other versions. For now, repeated snapshots with the same SchemaVersion
resolve to the last retained NORMAL entry. TODO(#40451): give distinct schema
snapshots unique versions; this change preserves existing WAL history.
AlterCollection's consuming-side ACK precedes coordinator callback/invalidation,
so the subsequent refresh observes metadata updates even without a schema-version
change. MetaCache's fill lock orders the schema fetch and publication against
invalidation.
