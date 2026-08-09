# Proxy request-scoped read snapshot

- Status: Implemented; verification is partially blocked by stale local C++/cgo artifacts
- Date: 2026-08-09
- Scope: Proxy Search, Query, HybridSearch, read-side RBAC, rate limiting, partition routing, and internal retries

## 1. Problem

Proxy currently resolves a request's collection name or alias multiple times during one Search or Query:

1. privilege checking may resolve the alias for RBAC;
2. rate limiting resolves the collection id and sometimes schema/partitions;
3. `CanSkipAllocTimestamp` resolves collection id and collection properties;
4. task `PreExecute` separately resolves collection id, schema, collection properties, and partitions;
5. Search retry, search-by-primary-key, and requery paths may start another read task.

Every individual MetaCache lookup is concurrency-safe, but the sequence is not a request-level transaction. If alias `A` is altered from `C1` to `C2` between lookups, one request can authorize or rate-limit `C1`, build a plan with `C2`'s schema, and send `C1`'s collection id to QueryNode.

The request must select either the old or new alias target. Mixing metadata from both targets is invalid.

### 1.1 Current branch and 2.6 baseline analysis

The comparison uses current-branch baseline `c552dfda0f` and 2.6 baseline `70148472bf`.

Both branches resolve the caller-supplied collection name or alias repeatedly in one logical read request. These are logical cache getter calls; a cache hit may avoid a RootCoord RPC, but it does not make the sequence atomic.

| Path | Current branch before this change | 2.6 before this change |
|---|---|---|
| Search timestamp decision | `GetCollectionID` + `GetCollectionInfo` | same |
| Search `PreExecute` | `GetCollectionID` + `GetCollectionSchema` + `GetCollectionInfo` + `isPartitionKeyMode`, followed by name-based partition routing when needed | same |
| Query timestamp decision | `GetCollectionID` + `GetCollectionInfo` | same |
| Query `PreExecute` | collection ID, collection info, schema, partition-key mode, partition routing, then a second collection-info lookup for consistency/TTL | same logical sequence, with schema lookup ordered later |
| Other consumers | RBAC alias resolution, rate-limit collection/partition lookup, Search-by-PK internal Query, requery, optimized retry, and recall evaluation can resolve again | same categories |
| Additional branch-specific lookup | none in this area | materialized-view planning performs another `GetCollectionInfo` |

Therefore both branches have a real TOCTOU window. An AlterAlias from `A -> C1` to `A -> C2` can produce combinations such as C1's ID with C2's schema, partitions, consistency level, or RBAC/rate-limit identity.

The current branch already protects cache fill/invalidation ordering with `fillMu`, and AlterAlias expiration carries the new and old target IDs. The 2.6 branch does not have the same complete ordering and by-ID partition behavior. A 2.6 backport must include those prerequisites; cherry-picking only the request object can still allow a pre-alter fill to republish stale alias metadata after invalidation.

## 2. Required semantics

Alias resolution is the request's metadata linearization point:

```text
Authentication
      |
Resolve alias -> immutable CollectionReadSnapshot
      |                       |
    RBAC                  RateLimit
      |                       |
 allocate read timestamp -> build plan -> route partitions -> execute/requery/retry
```

The guarantees are:

1. If snapshot resolution happens before AlterAlias, the whole request uses the old collection.
2. If snapshot resolution happens after AlterAlias completes, the whole request uses the new collection.
3. A request never combines one collection's id with another collection's schema, properties, or partitions.
4. RBAC, quota accounting, timestamp selection, plan construction, QueryNode routing, requery, and internal retry use the same collection binding.
5. A retry must never silently re-resolve an alias to another collection.
6. Iterator pages remain separate external requests and continue checking the client-carried `collection_id`.

## 3. Data model

Introduce an immutable request-scoped collection binding:

```go
type CollectionReadSnapshot struct {
    requestedDBName         string
    requestedCollectionName string
    databaseID              int64
    databaseName            string
    collectionID            int64
    canonicalName           string
    info                    *collectionInfo
}
```

`collectionInfo` is the only source of schema, consistency level, update timestamp, query mode, partition-key isolation, and collection properties. The snapshot must not separately acquire those values through name-based getters.

The request-level object additionally owns lazily initialized partition metadata and the pinned data visibility timestamp:

```go
type ReadRequestSnapshot struct {
    Collection *CollectionReadSnapshot

    partitionOnce sync.Once
    partitions    *partitionInfos
    partitionErr  error

    timestampMu     sync.RWMutex
    timestampPinned bool
    consistency     commonpb.ConsistencyLevel
    requestTS       Timestamp
    guaranteeTS     Timestamp
}
```

Cached `collectionInfo`, `schemaInfo`, and `partitionInfos` values are immutable after publication. Cache invalidation removes map ownership but does not invalidate pointers already held by in-flight requests.

## 4. Resolution and context propagation

Add one MetaCache-facing resolver that performs a single `GetCollectionInfo(database, nameOrAlias, 0)` and builds `CollectionReadSnapshot` from the returned object.
The resolver treats a missing schema or zero collection id as incomplete internal metadata and fails with `ServiceInternal`; it never builds a routable snapshot from a partial cache object.

Store `{snapshot, err}` in context and make resolution idempotent. The gRPC privilege interceptor pins the snapshot before alias-aware RBAC when that feature is enabled; the rate-limit interceptor ensures it otherwise; Proxy Search, Query, and HybridSearch remain the final fallback for direct/internal callers. A resolution failure is retained in context rather than returned before RBAC, preserving the existing resource-existence protection:

- privilege checking uses the canonical name when resolution succeeded;
- on resolution failure, privilege checking uses the literal request name;
- after authorization succeeds, the handler returns the saved resolution error;
- rate limiting and the handler consume the same context snapshot.

REST V1 pins the snapshot in its DQL interceptor and propagates the context returned by authorization. REST V2 first runs authorization, returns a saved snapshot-resolution error only after authorization succeeds, and pins the snapshot before schema/placeholder preprocessing; the common wrapper checks the same context idempotently. Schema conversion, limiter accounting, and the Proxy method therefore use the same binding.

Direct internal/test calls that do not pass through interceptors use an idempotent `EnsureReadRequestSnapshot` fallback.

## 5. Search and Query task changes

Resolve or obtain the request snapshot before task enqueue. Task construction stores the snapshot pointer.

`CanSkipAllocTimestamp` reads consistency from the snapshot and performs no MetaCache lookup.

`PreExecute` initializes all metadata from the snapshot:

```go
t.CollectionID = snapshot.CollectionID()
t.collectionName = snapshot.CanonicalName()
t.schema = snapshot.Schema()
collectionInfo := snapshot.Info()
```

Remove the task-local sequence of:

```text
GetCollectionID(name)
GetCollectionSchema(name)
GetCollectionInfo(name, id)
isPartitionKeyMode(name)
```

Schema-derived predicates such as partition-key mode are evaluated directly from `snapshot.Schema()`.

Search-by-primary-key, requery, optimized Search retry, recall evaluation, and Query retry receive the same request snapshot.

## 6. Partition routing

Partition lookup must not re-resolve the alias. Add collection-id-based helpers:

```go
GetPartitionInfosByID(ctx, database, collectionID)
```

Refactor `getPartitionIDs`, `assignPartitionKeys`, namespace partition routing, and rate-limit partition accounting to accept `ReadRequestSnapshot` rather than database/collection name strings.

The first partition consumer loads the immutable partition list by the pinned collection id. Later consumers reuse the same pointer.

For the AlterAlias problem, resolving partitions by collection id is sufficient to prevent cross-collection mixing. A future strict RootCoord-wide snapshot covering concurrent schema and partition DDL would require a combined/versioned RootCoord read API and is outside this change.

## 7. Data visibility timestamp

The collection metadata binding and data MVCC timestamp are separate but belong to the same external request.

The first task computes consistency, its allocated request timestamp, and guarantee timestamp from the pinned collection info, including the collection schema update timestamp barrier. The result is stored once in `ReadRequestSnapshot`.

All internal attempts reuse it:

- optimized Search retry;
- inconsistent-requery retry;
- recall evaluation;
- search requery;
- search-by-primary-key internal Query;
- Query internal retry.

Subsequent attempts may allocate a new task id, but `PreExecute` restores the pinned request timestamp and must not select a new collection or a new guarantee timestamp. Existing Search requery additionally reuses the per-channel MVCC timestamps returned by the first Search.

This change pins the Proxy-visible read contract. For normal non-iterator Eventually/Bounded reads, QueryNode may still choose its actual per-channel MVCC from the channel tSafe when the request carries `mvcc_timestamp=0`; a strict, externally visible, identical per-channel MVCC across every replica failover would require a separate QueryNode response/protocol extension. The implementation does not claim that stronger guarantee.

## 8. AlterAlias interaction

Search and Query do not acquire Streaming Broadcaster resource locks. Holding `SharedDBName` for a long DQL request would block AlterAlias on slow reads and introduce a distributed lock lifecycle into the read path.

Instead, concurrent operations use request binding:

- the request retains an immutable old-target snapshot;
- AlterAlias commits and invalidates Proxy caches;
- later requests resolve the new target;
- the in-flight old request remains valid and cannot switch targets.

The current branch already orders MetaCache fills against invalidations with `fillMu`, and AlterAlias expiration carries both old and new collection ids. Therefore no Streaming message change is required for the current-branch implementation.

For a 2.6 backport, request binding must be accompanied by fill/invalidation ordering and by-id partition fetching; otherwise a pre-alter fill can resurrect an old alias entry after invalidation returns.

## 9. Failure behavior

If the pinned collection is dropped after snapshot creation, the request must continue using the pinned id and fail through the existing by-id/query execution error path. It must not re-resolve the alias and execute against a replacement collection.

Alias changes between iterator pages continue to return the existing collection-id mismatch error.

An internal request-snapshot invariant violation is a system failure/TOCTOU bug, never an input error.

## 10. Implementation plan and status

1. [x] Add immutable collection/read snapshot types, context storage, and idempotent resolution helpers.
2. [x] Integrate snapshot resolution with privilege and rate-limit interceptors without exposing resolution errors before authorization.
3. [x] Resolve snapshot before Search/Query task enqueue and pass it into Search-by-PK, requery, optimized retry, and recall paths.
4. [x] Remove repeated collection ID/schema/info lookups from Search and Query `PreExecute`.
5. [x] Convert partition routing and rate-limit partition accounting to a pinned collection-ID API.
6. [x] Pin consistency, request timestamp, guarantee timestamp, schema-update barrier, and TTL time basis across internal attempts.
7. [x] Add a deterministic AlterAlias simulation covering pinned ID/schema, limiter identity, partitions, timestamp pinning, target mismatch, and a later request binding the new target.
8. [ ] Complete executable Proxy verification after repairing the local C++/cgo build artifacts; static formatting, diff, lookup, and mock-contract audits remain available.

## 11. Verification matrix

Use two collections with deliberately incompatible metadata:

```text
C1: vector field id 101, dim 128, Strong consistency, partition id 1001
C2: vector field id 201, dim 256, Bounded consistency, partition id 2001
```

Verify:

1. Snapshot before AlterAlias gives only C1 values.
2. Snapshot after AlterAlias gives only C2 values.
3. AlterAlias between the former ID/schema lookup points cannot produce a mixed plan/request.
4. RBAC canonical name, limiter collection id, task collection id, and QueryNode request collection id are identical.
5. Search retry and requery retain collection id and guarantee timestamp.
6. Search-by-primary-key internal Query retains the same snapshot.
7. Iterator page two rejects a changed alias target.
8. A request starting after AlterAlias returns binds the new target.
9. A parked cache fill cannot survive a completed invalidation in the 2.6 backport.

Current local verification limitation: the required Proxy test command is blocked before compiling this package because the workspace's generated C++/cgo artifacts are stale (`GetLoonReaderThreadPoolSize`, `InitIndexBuildReadWindow`, `InitLoonReaderThreadPool`, and the `C.Analyze` signature disagree with the Go wrappers). This is independent of the request-snapshot files. Do not treat the target test as executed successfully until those artifacts are rebuilt.
