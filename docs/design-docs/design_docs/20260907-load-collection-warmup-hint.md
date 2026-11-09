# LoadCollection Warmup Hint Design

- **Status:** In progress; the feature gate is off by default.
- **History:** earlier drafts took `warmup="sync"` and made a load wait for its warmup. This design takes the hint `warmup_hint=true` instead: the server warms up in the background and does not guarantee completion.
- **Scope:** the Streaming DDL load path under the QueryView architecture, QueryView resource preparation on QueryNode, and the segcore warmup interface.

## 1. Requirements

1. Users can call `LoadCollection(load_params={"warmup_hint": "true"})` to hint the server to warm up ahead of time. They can also just query and let auto-load load the collection; auto-load does not warm up.
2. When a collection is already loaded (including by query-triggered auto-load) without warmup, calling `load(warmup_hint=true)` warms up the loaded data.
3. Every `load(warmup_hint=true)` call warms up again. When the cache still holds the data, the extra pass is cheap.
4. Warmup is a background operation. It does not affect search, QueryView availability or load progress. Search runs as usual while warmup is unfinished, and data not yet warmed up is loaded on demand.

## 2. Non-goals

- No external "warmup completed" signal. After `load(warmup_hint=true)`, `Await`, `GetLoadingProgress` and `GetLoadState` behave exactly as for a normal load; warmup progress is only observable in QueryNode logs and metrics.
- No guarantee that data stays in the cache; eviction works as usual.
- A warmup hint cannot be withdrawn; only Release ends it.
- `LoadPartitions` and external collections do not support the warmup hint.
- This design does not fix collection-level `warmup.*` properties taking only partial effect in QueryView mode.

## 3. Semantics

| Current state | Request | Result |
| --- | --- | --- |
| Not loaded | `load(warmup_hint=true)` | Searchable once a normal load completes; warmed up in the background afterwards |
| Loaded (normally, by auto-load or with the hint) | `load(warmup_hint=true)` naming no replica number, resource groups or load fields | The load configuration, partitions included, is kept and search is unaffected; loaded segments are warmed up (again) in place in the background |
| Loaded | `load(warmup_hint=true)` naming a replica number, resource groups or load fields | Restates the load configuration as a normal load does; warmed up in the background afterwards |
| Loaded with the warmup hint | `load()` without the hint | The warmup hint is kept; no new warmup |
| Any | Release (including auto-release) | Ends the warmup hint |
| Not loaded | Query-triggered auto-load | Normal load without warmup |
| Warmup hint present | New segments from flush, compaction or import | New segments are warmed up in the background after they load; segments already warmed up are not warmed up again |
| Any | `load(warmup_hint=true, refresh=true)`, a non-boolean `warmup_hint`, or the hint on an external collection | Invalid parameter |

## 4. Core model: the warmup epoch

**Definition:** a warmup epoch is a number QueryCoord allocates for each `load(warmup_hint=true)` call. It is persisted, monotonically increasing and globally unique; 0 means no warmup hint.

**Where it lives:** the load config stores the epoch of the latest `load(warmup_hint=true)`; each QueryView carries the epoch it was built with; QueryNode records, for each physical segment base, the latest epoch it finished warming up for (`warmedEpoch`).

**The QueryNode rule:** when the largest epoch among the views whose bindings are prepared on a segment base is greater than the base's `warmedEpoch`, the base is warmed up in the background. Warmup never blocks view readiness. The StreamingNode needs no epoch bookkeeping; see 5.8.

**Why an epoch:** QueryViews are rebuilt frequently because of flush, compaction, balance, node changes and so on. The epoch changes only when `load(warmup_hint=true)` is called, so ordinary view rebuilds do not warm up existing segments again, while every hinted load triggers one warmup. The same rule covers in-place warmup (a normally loaded segment records 0), repeated warmup (a new, larger epoch) and new segments.

## 5. End-to-end flow

### 5.1 Entry points

- Go SDK `NewLoadCollectionOption(name).WithWarmupHint()`, `"warmupHint": true` in the REST v2 load request body, and clients that pass `load_params` directly all end up writing `load_params["warmup_hint"]`.
- Proxy requires the value to be a boolean. When it is true, it cannot be combined with refresh or used on an external collection. Proxy then converts it to `warmup_hint` on the internal request.

### 5.2 QueryCoord admission

1. When the feature gate `queryCoord.enableLoadCollectionWarmupHint` is off, hinted loads are rejected. Node capabilities are not checked: warmup is only a hint, and an old QueryNode that does not support it ignores the epoch without affecting loading or queries.
2. Each hinted load allocates a new epoch; a load without the hint keeps the epoch already in the load config.
3. A hinted load on a loaded collection that names no replica number, resource groups or load fields (including `skip_load_dynamic_field`) keeps the current load configuration (partitions, fields, replicas and modes) and only advances the epoch; any other load restates the configuration as before.
4. An `AlterLoadConfig` broadcast is generated, also when only the epoch changes.

### 5.3 Persistence and config version

- The epoch is written to the `AlterLoadConfig` message header and to the versioned collection load config in etcd, and survives recovery.
- DDLs that carry no epoch (LoadPartitions, ReleasePartitions, TransferReplica, etc.) keep the existing epoch: merging takes the maximum. The epoch never decreases; Release deletes the load config, and the epoch with it.
- The collection's config version is allocated when the load requirements change. They exclude the epoch, so an epoch-only change keeps the version and rewrites its record. The data the views serve has not changed, so with auto-load enabled the search readiness gate (the Up view's config version equals the current version) does not wait for views to be rebuilt. The store's global version still increments, so the balancer still observes the change.

### 5.4 Balancer

- When the Up view's epoch is smaller than the config's, the balancer rebuilds the shard's view so QueryNodes receive the new epoch. Placement is sticky: most segments stay on their node and are warmed up in place, and a segment that moves is warmed up after it loads on its new node.
- Placement is not restricted by node warmup capability: warmup is background work that does not affect availability; an old QueryNode simply does not warm up.
- A plan computed before the epoch changed may still be applied; at worst the view is rebuilt one extra time.

### 5.5 Load progress and search admission

Same as a normal load and independent of the epoch: a shard counts as complete when its Up view's config version equals the current version, and `Await` returns when all shards are complete. With auto-load enabled, the search readiness gate uses the same check.

### 5.6 QueryNode

- **Load:** takes the normal path with no forced synchronous warmup. A segment reports ready to its views once it is loaded and has caught up with the TransformLog, and the views go Up as usual.
- **Background warmup:** each physical base of a segment resource (the sole owner of a segment's physical preparation and lifetime) records `warmedEpoch`. Reconciliation, the single decision point of the resource, starts a warm when the largest epoch of the bindings prepared on a base exceeds its `warmedEpoch`. The warm calls the physical loader's `Prewarm` on the base, which warms the read state the base currently adopts and the cells other read states share with it, and on success updates `warmedEpoch`. Warms run on the resource manager's own warm queue with at most one worker per CPU, the size of the load executor that runs `Prewarm`, not on the node scheduler, so they do not slow down reconciliation. A running warm is an operation of the resource, so closing the resource waits for it.
- **Triggers:** a binding is prepared on a base, that is, its view becomes ready; a binding with a larger epoch is prepared on an already loaded base; the previous warm of the base completes.
- **Serialization:** each base has at most one warm at a time. A warm that has not started adopts a larger epoch that arrives; a running warm started before it, so another warm follows when it completes.
- **Failure:** retried with jittered exponential backoff (from 1 second up to 1 minute). Each retry warms up for the current largest epoch, until it succeeds or no binding needs it any more. A failure only writes a rate-limited log; it does not affect views and is not reported.
- **Cancellation:** a warm is canceled only when its base retires, or when its segment resource is closing and no query uses the base. Both happen only after the last view and the last query using the base have released it, so canceling a warm never fails a query waiting on a cache load the warm started. A base retires only after its running warm stops, so its release does not wait for the warm on the node scheduler; a warm that has not started is dropped. Late results are discarded by task identity.

### 5.7 segcore

- `Prewarm` loads every resource of a loaded segment and waits for completion: field data (all chunks, including the materialization of lazy column groups), every index in the segment's index inventory (vector, scalar, ngram, JSON path and text indexes; each index's request reader is created and prefetched, which loads what that reader reads), JSON stats, and the segment-level PK index and timestamp index. Field data is loaded in runs of about one load batch, estimated from the storage v2 cell target, so a search waits for and fails with at most the run holding its chunk rather than the whole field. This does not cover the PK and timestamp indexes, whose builds load their whole columns, or storage v1, where a chunk is a whole binlog and a run may span the field.
- The C interface `AsyncSegmentPrewarm` runs `Prewarm` on the load executor at low priority, so warms yield to loads. It returns a future like `AsyncSegmentLoad`: errors keep their original code, and canceling the future stops the warm between runs.

### 5.8 StreamingNode

- The shard leader serves BM25 queries from the shard's IDF oracle. With `queryView.idfOracle.lazyLoadSealedStats=true` the oracle reads the sealed BM25 statistics on the first BM25 query, so a hinted load would still leave that read on the first query.
- Once a view with a warmup epoch is Ready, the StreamingNode queues a background warm of the oracles of the view's BM25 fields. The warm starts the shared materialization that the first BM25 query would otherwise start, and readiness does not wait for it.
- Materializing once is enough: the aggregate then stays in memory and is kept current by later preparations until the oracle closes, and a new oracle, for example for a BM25 field that a schema change adds, is warmed when its views become Ready. A materialized or eagerly built oracle needs no warm, so repeated hinted loads and view rebuilds cost nothing.
- Warms run on one node-wide queue, in arrival order, on at most a CPU count of workers, because every hinted view requests one when the StreamingNode recovers its views. A failed attempt, or one that a newer target canceled, is queued again after a jittered exponential backoff from 1 second up to 1 minute, until the warm succeeds or the oracle closes on Release. A query arriving meanwhile shares the running materialization, and at most one warm per oracle is queued, running or waiting to retry.

## 6. Protocol fields

| Location | Field | Notes |
| --- | --- | --- |
| `milvuspb.LoadCollectionRequest` | `load_params["warmup_hint"]` | Public parameter; the public proto is unchanged |
| `querypb.LoadCollectionRequest` | `bool warmup_hint` | Request flag from Proxy to QueryCoord |
| `querypb.LoadCollectionRequest` | `bool load_all_fields`, `bool skip_load_dynamic_field` | Whether the user named no load fields and whether it skipped the dynamic field; Proxy fills `load_fields` either way, so QueryCoord's in-place rule reads these |
| `messagespb.AlterLoadConfigMessageHeader` | `int64 warmup_epoch` | 0 means no warmup hint |
| `viewpb.CollectionLoadConfig` | `int64 warmup_epoch` | Persisted in etcd |
| `viewpb.QueryViewMeta` | `int64 warmup_epoch` | The epoch the view was built with |

## 7. Compatibility and gating

- The gate is off by default. Before it is turned on, the whole cluster (Proxy, QueryCoord, QueryNode and its native libraries) must be upgraded.
- During a rolling upgrade, old QueryNodes ignore the epoch and do not warm up, which does not affect loading or queries, so no separate node capability declaration is needed.
- After the gate is turned off, collections that already have a warmup hint stay as they are, loads without the hint work as usual, and new hinted loads are rejected.

## 8. Error handling

| Scenario | Classification | Result |
| --- | --- | --- |
| Non-boolean `warmup_hint`, combined with refresh, or on an external collection | Invalid parameter (Input) | Rejected before acceptance |
| Gate off | ServiceUnimplemented | Rejected without changing the config |
| Background warmup fails (storage throttling, corrupted files, out of memory, etc.) | Original error class kept and logged | QueryNode retries with backoff; views, search and load progress are unaffected |
| Release during warmup | Canceled | The warm and its pending retries are canceled once no view or query uses the segment; late results are discarded |

## 9. Implementation plan

**Phase 1** (this PR)

- QueryCoord: each hinted load allocates a new epoch, which keeps the config version; load progress and search admission ignore the epoch.
- Balancer: rebuilds views when the epoch changes; placement is not restricted by warmup capability.
- QueryNode: ready as soon as a normal load completes; background warmup of segment bases by epoch on a dedicated warm queue, with backoff retry, canceled only when a base retires, or its resource is closing and no query uses the base.
- segcore: `Prewarm` and the `AsyncSegmentPrewarm` C API.
- StreamingNode: a Ready hinted view materializes the lazy BM25 statistics of the shard's IDF oracle in the background, on a node-wide warm queue with backoff retry.

**Phase 2**

- QueryNode: reserve memory and disk for a full warmup before starting it; metrics for warmup progress and failures.
- segcore: warm up the downstream resources of JSON stats and the lazily loaded resources inside indexes.

## 10. Testing and acceptance

- **QueryCoord:** each hinted load increases the epoch; a load without the hint keeps it; other DDLs keep it; Release clears it; an epoch-only change keeps the config version; a hinted load naming no load options keeps the load configuration; hinted loads are rejected when the gate is off.
- **Balancer:** an epoch change triggers a rebuild that carries the epoch; placement does not depend on warmup capability.
- **QueryNode:** warmup does not block readiness and does not use the node scheduler; in-place warmup; a repeated hinted load queues another warmup; view rebuilds with the same epoch do not warm up again; a failed warmup retries with backoff without affecting views, and the retry warms up for the newest epoch; a warmup is not canceled while a query still uses the segment and stops when the segment closes; a replaced base retires after its warmup stops, and the new base warms up.
- **segcore:** after `Prewarm`, the fields of lazy column groups (every chunk, warmed in more than one run), the PK index and the timestamp index are loaded.
- **StreamingNode:** a Ready view with a warmup epoch materializes the lazy oracles of its BM25 fields without a query, and a view without one does not; a materialized or eager oracle is not warmed again; a failed attempt, or one a newer target canceled, retries after backoff; closing the oracle stops its warm; warms run on at most the node's worker count.
- **End to end:** a hinted load after a query-triggered auto-load, with search uninterrupted throughout and not waiting under auto-load; on an unloaded collection, a hinted load becomes searchable as fast as a normal load; two hinted loads in a row; a hinted load right after Release; continuous writes and compaction during warmup; warmup retries under injected faults such as storage throttling.

## 11. Known limitations

1. Resources added to a segment base after its warmup (for example, an index built later) load under the normal policy and are not warmed up automatically.
2. When bindings with different schemas or load scopes share a base, a warm covers the read state the base currently adopts; resources that only another read state uses are not warmed.
3. There is no external warmup-completion signal; see Non-goals.
4. For warmup coverage see 5.7; phase 2 fills the gaps.
