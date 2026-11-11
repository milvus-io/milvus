# LoadCollection Warmup Hint Design (v6)

- **Scope:** Load admission, one-time prewarm requests, QN segment caches and SN lazy BM25 statistics.
- **Architecture:** Ordinary load and ephemeral prewarm use independent coordinator RPCs. There is no warmup epoch or View synchronization trigger.

## 1. Contract

`LoadCollection(load_params={"warmup_hint": "true"})` requests one background cache-warming pass on the local cluster. It creates no collection property or persistent policy.

- Proxy performs an ordinary load, then asynchronously submits a separate prewarm request after load admission succeeds. Each shard starts prewarming when its own serving View is Up; it does not wait for whole-collection loading progress. Queries and public loading progress do not wait for prewarming.
- Hinted loads retain ordinary load validation and configuration semantics, including field/index checks. The separate prewarm RPC never loads a collection or modifies its load configuration.
- Each explicit hint creates a new request. Existing cache entries or an already materialized BM25 aggregate can make the pass cheap.
- Balance, flush, compaction, new writes and restart neither inherit nor restore the request. Already borrowed resources continue warming across View replacement; the request never follows a new View or retries on another node.
- Release, load-config replacement, timeout and coordinator shutdown discard pending work and cancel running requests. A canceled warm must not abort a cache load shared by a live query.

This is best-effort work without a public completion guarantee. SDK load waits, `GetLoadingProgress` and `GetLoadState` describe ordinary loading. Queries may share an in-progress cache read or BM25 materialization and wait for that read; prewarming also consumes CPU, memory and storage bandwidth.

## 2. API and compatibility

The public parameter is `load_params={"warmup_hint": "true"}`; REST uses `warmupHint: true`. Existing proxy validation rejects malformed booleans, refresh combinations and unsupported external collections. `LoadPartitions` is unchanged.

`queryCoord.enableLoadCollectionWarmupHint` remains disabled by default. Enable it only after all participants support the new request. A worker without `PrewarmOnView` rejects that attempt; there is no compatibility fallback to persisted hints.

Existing collection and field cache warmup settings remain independent. Ordinary loads may perform work required by those settings, but cannot inherit an earlier hint.

## 3. Admission and dispatch

The public hint is consumed by Proxy. Proxy sends the existing `LoadCollectionRequest` without any warmup fields. Its load task retains ordinary validation and dispatch. After success, Proxy asynchronously submits `PrewarmCollection(collection_id)` once; admission errors are logged and cannot change the successful load result. The coordinator load callback, WAL broadcast and load completion criteria are unchanged.

The separate prewarm handler requires an existing load configuration. Under a short config-identity guard it subscribes to release/readiness notifications, then starts an in-memory task bounded by the existing load timeout. Admission returns without waiting for Up. Completion of the admission RPC does not cancel accepted background work. No collection DDL broadcast or allocator call is involved. The task captures replicas and channels at admission; release/reload, config replacement, timeout and coordinator shutdown cancel it.

For each captured replica/shard, the task independently waits for a serving Up View matching that load-config version. It snapshots that View version and QN placements, removes the shard from its pending set and sends `PrewarmOnView(shard_id, version)` once to each selected QN and the current SN owner. Ready shards need not wait for other shards. This request uses the existing View only to locate and borrow resources. It creates no View, changes no View metadata and participates in no View synchronization queue.

Dispatch concurrency is bounded. Both Proxy-to-QC admission and each worker target have one attempt; missing/retired Views, node loss, assignment races, timeout and storage errors end that attempt. SN resolves its owner once and pins the node ID and channel term. Neither application retry nor gRPC configured retry follows a replacement owner. Errors on one target do not cancel other targets.

## 4. Persistence, recovery and CDC

No epoch or hint is stored in `LoadConfig`, `CollectionLoadConfig`, `QueryViewMeta` or `AlterLoadConfig` messages. Legacy epoch field numbers and names are reserved. Recovery and configuration conversion ignore legacy unknown fields. Ordinary automatic/manual placement, View synchronization and recovery contain no prewarm producer.

Prewarming is local ephemeral cache work. It is not written to the WAL or replicated by CDC. Ordinary load requirements retain their existing WAL and replication path. Restart or promotion cannot recreate this request; another explicit hint is required.

## 5. QueryNode resource ownership

The handler acquires the requested ready View using the same lease as a query. The segment manager then borrows its sealed segment handles and immutable ReadStates. The lease and handles remain held until prewarming returns, so retiring the View cannot destroy those resources or redirect the pass to another generation.

Each borrowed ReadState is warmed once through the existing native load executor at low priority. The Go ReadState forwards its own native segment pointer, rather than the physical segment's subsequently adopted state. Segcore warms the pinned state's PK/timestamp resources, field chunks, JSON statistics and index readers. A failure ends this request without queuing retries.

A running warm holds an operation reference. Explicit cancellation reaches native/storage work once no live query or other warm request shares the base. Query release rechecks canceled work so it can unwind before destruction. Node shutdown also cancels unshared work. Cancellation is cooperative; slow storage may delay cleanup. Completion releases the pins and does not guarantee permanent cache residency.

## 6. StreamingNode lazy BM25

The SN handler acquires the exact serving View lease and calls the optional resource-module prewarmer with the request context. Ordinary preparation and Ready notifications never initiate warming.

BM25 prewarming joins or starts materialization for that View's exact data version. Queries and warm requests share flights and count their waiters. A warm-originated flight is canceled only after its last waiter leaves; canceling one warm therefore cannot abort a query or another warm using it.

If a new lazy target arrives during the flight, the old warm continues its borrowed reads while the new target can materialize independently. The old aggregate is not published as current, and the warm does not follow the replacement target. Node shutdown cancels and drains all flights, including detached old-target flights, before releasing oracle resources. Storage errors require another explicit request to start new prewarming.

## 7. Verification scope

Regression tests cover per-shard Up dispatch before whole-collection readiness, independent admission without load mutation, ordinary hinted-load field/index validation, release/reload/shutdown cancellation, one-attempt RPC behavior including real SN gRPC transport, immutable QN ReadState borrowing across View replacement, shared-query cancellation protection, and old-target SN materialization without following new data.

Source tracing covers proxy admission, ordinary load/WAL replay, load-config recovery, automatic placement, manual placement, native ReadState pinning, and QN/SN request routing. No deployment performance claim is made by this change.
