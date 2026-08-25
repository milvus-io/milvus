# Per-resource-group load percentage and shard-leader readiness

- Status: Implemented (PR #52716; 3.0 counterpart #52711)
- Date: 2026-08-24
- Scope: `internal/querycoordv2/utils/resource_group_load_percentage.go`, `internal/querycoordv2/utils/shard_leader_readiness.go`, `internal/querycoordv2/utils/util.go`, `internal/querycoordv2/services.go`, `pkg/proto/query_coord.proto`
- Related: issue #52713; split out of #52500

## 1. Problem

QueryCoord answers three questions about a loaded collection — *how far along is the load*, *can the shards be served*, and *which nodes should a query be routed to* — and all three are answered **collection-wide**. That is correct when a collection is loaded once, into one resource group. It is wrong as soon as the same collection is loaded into several resource groups that progress independently, which is what `LoadCollection(replica_number=N, resource_groups=[rg-a, rg-b])` and any resource-group-based rollout produce.

Concretely, with `rg-a` fully loaded and `rg-b` at 0%:

| Surface | Collection-wide answer | Why it is wrong for a per-group caller |
|---|---|---|
| `ShowLoadCollections` / `CalculateLoadPercentage` | ~50% | Reports the same misleading figure to both groups: `rg-a` looks half-loaded, `rg-b` looks half-loaded. Neither is true. |
| `GetShardLeaders` | one flat list per channel, `rg-a`'s and `rg-b`'s leaders merged | A caller that made a query ready on `rg-b` is free to route it onto `rg-a`'s query node. |
| `checkLoadStatus` (the strict `GetShardLeaders` gate) | refuses, `CollectionNotFullyLoaded` | Refuses `rg-a`, which is completely loaded and serving, until `rg-b` finishes. |

The last row is the sharpest: `shouldUpdateCurrentTarget` (`observers/target_observer.go`) unions ready delegators **across** replicas and requires only that every channel be covered by *some* replica, so `rg-a`'s delegators alone promote the current target. The collection is therefore routable through `rg-a` while the collection-wide percentage still says 50 — and the collection-wide gate refuses precisely the group that is ready.

The critical property is that **this cannot be fixed outside the coordinator**. `querypb.ShardLeadersList` carries only the channel name plus the node ids, addresses and serviceable flags of the leaders on it; the builder flattens every replica of the collection into that one list, so the replica each leader belongs to — and with it the resource group — is discarded before the answer leaves QueryCoord. Recovering the mapping by intersecting node ids with a resource group's node set does not work either: a replica may borrow nodes from another resource group (QueryCoord models exactly that as `num_outgoing_node` / `num_incoming_node`), so node-set membership is not replica membership — and the two diverge precisely during the rebalance windows a readiness check exists to catch. This is why the resource group has to be put on the wire by the coordinator (§2.3) rather than derived by whoever reads the answer.

## 2. Design

Three read-only surfaces, no new persisted state, no change to the replica model.

```
                         ┌──────────────────────────────────────────┐
                         │  querycoordv2/utils  (free functions)    │
   Server methods ──────►│                                          │
   (stable entry points) │  LoadPercentageByResourceGroup           │  progress
                         │  ShardLeaderReadinessByResourceGroup     │  can it serve?
                         └──────────────────────────────────────────┘
   GetShardLeaders RPC ───► response tags each leader with its
                            replica's resource group   │  route to whom?
                            reads: Meta, TargetManager,
                                   DistributionManager, NodeManager
```

The computations are **free functions in `querycoordv2/utils`, not methods on `Server`**, because `CollectionObserver` and the other observers hold exactly these read-only stores and cannot import the `querycoordv2` root package. `Server` methods exist alongside them so external callers keep a stable entry point.

### 2.1 `LoadPercentageByResourceGroup` — progress

Selects the replicas whose own resource group is `rgName`, computes each one's target coverage, and reports the **minimum**. A caller deciding whether a resource group can be trusted wants the laggard, not the average or the best replica: the group is only "ready" once every replica in it is.

Its outcome space is deliberately four-valued, because "the collection isn't ready here" hides four different states a caller must act on differently — and the two `-1`-with-an-error rows are the pair most easily conflated, one being retriable and the other terminal:

| Result | Meaning | What the caller should do |
|---|---|---|
| `-1`, no error | The group holds no replica of this collection at all | Terminal *once the load has been registered* — see the startup-window note below. Load it, or ask about a different group. |
| `0`, no error | A replica is there but carries none of the targets yet — **or** the target set itself is empty, which after an ungraceful restart lasts until the observer rebuilds it | Wait. To tell the two apart, ask readiness (`ShardLeadersReasonNoChannelTarget`) or `GetShardLeaders` (`ErrCollectionOnRecovering`). |
| `-1`, `ErrServiceNotReady` (1, retriable) | The coordinator's own read stores are not wired up yet | Retry. Nothing is known, as opposed to something being known to be absent. |
| `-1`, `ErrCollectionNotLoaded` (101, non-retriable) | The load failed terminally; `GlobalFailedLoadCache` holds the reason | Stop. Waiting will not help; the cause is in the message. |

The third row is reported by `Server`'s entry point, which gates on `merr.CheckHealthy(s.State())` like the rest of `Server`'s surface. That gate — not the nil checks inside the `utils` functions — is what orders these reads against `Init`: `s.meta`/`s.dist`/`s.targetMgr` are plain fields with no synchronization, so a nil check on them carries no happens-before edge, while the atomic status store `Start` performs after wiring everything does. The nil checks remain as defense in depth for direct `utils`-level callers (the observers, and the tests), not as a promise that a concurrently-initializing `Server` is safe to read. Answering this state with a bare `-1` would assert "this resource group holds no replica" — a claim neither layer is in a position to make.

The first row's "terminal" is qualified, because `job_load.go` spawns the replicas (`SpawnReplicasWithReplicaConfig`) in one meta commit and registers the collection (`PutCollection`) in a later one. The registration check runs before the replica scan — it has to, so that the terminal failed-load state `observeTimeout` leaves behind is not swallowed into "nothing here" — so a poll landing between those two commits reads `-1` for a group that already holds replicas, and the next poll answers correctly.

Special-casing it was considered and rejected: distinguishing the startup window from a *registered* collection with zero partitions (§2.1.1) — which must answer `-1`, so the figure agrees with what `GetShardLeaders` will do for the same state — would mean layering an `Exist` check back on top of the registration test these surfaces were just unified on, reintroducing the fork that section exists to close. The window is one etcd commit wide, so a caller treating `-1` as terminal should confirm it across two polls rather than act on a single one.

The last two rows are why the recorded load failure is **normalized** rather than returned verbatim. `FailedLoadCache` stores whatever error the failing load task recorded, and a load genuinely fails with retriable sentinels — `ErrServiceNotReady` is what `LoadSegments` returns when the target query node is restarting, and the scheduler's exclusion list does not filter it out before `recordSegmentTaskError` stores it. That is the *same* code the init window uses to mean "retry, this fixes itself". Returned as-is, a load that is never coming back would be indistinguishable from one that is, and a caller following this contract would retry until the cache entry expires 24h later. `ShowLoadCollections` and `ShowLoadPartitions` normalize the same cache the same way, which is the parity this surface claims. The readiness surface normalizes identically; its `Reason` field already disambiguates for a struct-reading caller, but the error code is what a caller triaging on `merr` sees.

### 2.1.1 One registration test across all three surfaces

All three surfaces — and the scoped `GetShardLeaders` gate — test "is this collection registered as loaded" with `CalculateLoadPercentage(...) >= 0`, never `m.Exist`. The two disagree on a collection record with **zero partitions**: `Exist` checks only the collection map and returns true, while `calculateLoadPercentage` additionally requires a non-empty partition set and otherwise falls through to `-1`.

That state is reachable and can persist. `job_load.go` calls `RemovePartition` — an independent etcd commit that does not touch the collection key — *before* `PutCollection`, and when the incoming partition set is disjoint from the loaded one, `toReleasePartitions` covers every current partition. The window is observable by a concurrent reader, and a crash inside it leaves etcd holding the collection key with zero partition keys, which `CollectionManager.Recover` restores as a `Loaded` record over an empty partition loop.

Under `m.Exist`, readiness would report `Ready=true` and the percentage `100` for a collection whose routing is refused with `ErrCollectionNotLoaded` (101, non-retriable, so the gRPC layer does not even resend) — a caller gating a switchover on the first two would cut traffic over and then have every route permanently refused. Using one test everywhere makes these surfaces structurally incapable of that disagreement, and matches what `ShowLoadCollections` has always used.

Per-replica coverage is measured against `meta.NextTargetFirst` — deliberately *not* the `meta.NextTarget` the `CollectionObserver` itself reads. `NextTarget` resolves to the next target alone, and `UpdateCollectionCurrentTarget` clears it on promotion until the observer re-pulls it ~10s later, so a `NextTarget` read in that window sees an empty target and reports 0 — a fully loaded, serving group would flap 100/0 on every promotion. `NextTargetFirst` falls back to the current target and closes that window.

Query-**invisible** replicas (load-config spawns replicas invisible until every one of them is serviceable) are deliberately **included** here. This is a progress figure, and those replicas are exactly the ones whose progress the load-config path is waiting on.

That inclusion is also where these surfaces deliberately disagree, so it carries a **pairing rule: 100 is not a servability verdict.** Readiness and the `GetShardLeaders` routing path both exclude query-invisible replicas — a leader the proxy can never be routed to cannot serve — so a group whose replicas are all still invisible reads `100` here while readiness says `Ready=false`, and the group does not appear in the `GetShardLeaders` answer at all: its leaders are dropped before the per-leader tag (§2.3) is applied. This is a normal product state: `UpdateLoadConfig` with `needWaitRGReady` spawns the new group's replicas `WithQueryInvisible`, and promotion is global and all-or-nothing, so the new group can finish carrying every target of its own while promotion stays blocked on an unrelated replica. **A caller gating a switchover must pair the percentage with the readiness verdict**; acting on `100` alone cuts traffic to a group that cannot answer, and keeps retrying it for as long as that unrelated replica stays unserviceable, instead of staying on the old one.

**This is a live target-coverage figure, and it is not the collection-wide number.** It agrees with `ShowLoadCollections` at the endpoints — `-1` when nothing is here, and both reach 100 exactly when every target is carried (which is what a gate on `== 100` needs, subject to the pairing rule above) — but diverges in between, in two ways that are properties of the design rather than defects:

- *It pools targets across partitions.* The observer computes each partition separately (its own segments plus the channel targets) and `CalculateLoadPercentage` averages the partitions, so a small partition counts as much as a large one. This figure counts each target once. On a multi-partition collection mid-load the two can differ widely.
- *It can fall back below 100 in steady state.* The observer's number is persisted per partition and never regresses once it reaches 100. This one is recomputed against the live target set, so when a freshly flushed segment or compaction output lands in the next target, the figure reports the not-yet-loaded remainder until the replica picks it up. That is the point — it answers "is this group carrying everything currently asked of it" — but a caller must expect the gate to re-arm whenever new work lands.

Making the figure monotone would require persisted per-group state, which is exactly what this surface avoids; both divergences are pinned by tests with concrete numbers rather than left to prose.

### 2.2 `ShardLeaderReadinessByResourceGroup` — can it serve?

Answers whether the replicas in `rgName` can serve **every** shard of the collection right now, returning a structured verdict rather than a bare bool:

```go
type ShardLeaderReadiness struct {
    Ready         bool
    Reason        string   // one of the exported constants; callers compare, never parse
    TotalShards   int
    UnreadyShards []string // sorted, so one state always prints one line
}
```

It reads the **current** target, not the next one: a shard is servable only once its leader is serving what the collection is currently expected to hold — the same target the native shard-leader path reads.

It does **not** reuse `checkLoadStatus`. That gate is collection-wide by construction: it reads `CalculateLoadPercentage(collectionID)` and then short-circuits to "ready" whenever the collection's own status is `LoadStatus_Loaded`. That status is set only once the collection-wide *average* reaches 100 — a group finishing first does not arm it — but nothing ever disarms it: `UpdateLoadConfigJob` spawns replicas for a newly added resource group without touching the collection's status. So once a collection has been `Loaded` even once, the short-circuit stays permanently armed, and the gate answers "ready" for every later resource group from the moment its replicas exist — the same admission bug from the opposite direction. The gate here is derived only from the leaders of the selected replicas: nothing about another group's progress, and nothing about the collection's aggregate status, can make this report ready.

Unlike the progress figure, only **query-visible** replicas count as able to serve, matching the `IsQueryVisible` filter both `GetShardLeaders` **routing** paths apply — a leader the proxy can never be routed to must not make its group look ready. Not every caller of the shard-leader machinery filters: `checkCollectionQueryable`, on the `CheckHealth` path, reaches `GetShardLeadersWithChannels` with a nil replica filter and does count leaders on query-invisible replicas. That asymmetry predates this PR and is untouched by it; readiness follows routing because routing is what its answer is about. A query-invisible replica still keeps the group out of the `NoReplicaInResourceGroup` bucket, whose meaning is "waiting will never help".

In both this surface and the progress figure, the **load-registration check runs before the replica scan**. The terminal failed-load state is the one `CollectionObserver.observeTimeout` leaves behind, with the collection registration *and* every replica record removed and only the `GlobalFailedLoadCache` entry remaining; scanning replicas first would turn that state into a bare "nothing is loading here" and swallow the recorded failure.

### 2.3 `ShardLeadersList.resource_groups` — route to whom?

`GetShardLeaders` gains **no request parameter**. Instead each leader in the response carries the resource group of the replica it leads:

```proto
message ShardLeadersList {
    string channel_name = 1;
    repeated int64 node_ids = 2;
    repeated string node_addrs = 3;
    repeated bool serviceable = 4;
    repeated string resource_groups = 5;   // parallel; resource_groups[i] describes node_ids[i]
}
```

The builder already holds the replica while walking each channel, so filling it is one `append` — placed inside the same `info != nil` branch as the other three arrays, because a leader dropped at the NodeManager check must not shift every tag after it.

**It is the replica's resource group, never the node's.** That is the whole reason the tag has to come from the coordinator: a replica may borrow nodes from another resource group (`num_outgoing_node` / `num_incoming_node` model exactly that), so intersecting node ids with a group's node set is not replica membership, and the two diverge precisely during the rebalance windows a caller asking this question cares about. The response flattens every replica into one list per channel, so the mapping exists nowhere else once the answer leaves QueryCoord.

#### Why the response side rather than a request filter

A request-side `resource_group` filter was implemented first and then replaced. Both answer "which leaders can this group serve from"; the difference is what it costs the caller this was built for.

| | request filter | response tag |
|---|---|---|
| proxy cache | needs a resource-group dimension: `map[collID]` → `map[collID]map[rg]`, N fetches and N entries per collection | **unchanged**, single copy per collection; the group is a per-leader attribute filtered in memory |
| `InvalidateShardLeaderCache([]int64)` | semantics must be rethought (evict all groups? carry the group on the wire?) | untouched |
| information | one group per call | every group in one call — a caller can compare them |
| old coordinator | silently ignores the field and answers **unfiltered**, in a response shape the caller cannot distinguish | leaves the field **empty**, which is detectable |

The proxy already consumes a per-leader boolean (`serviceable`) to build a two-tier candidate set; a per-leader string is the same shape of thing, and its cache needs no new key.

#### What the tag cannot answer

The tag says which group a leader belongs to, **not whether that group can serve the collection**. Both `GetShardLeaders` routing paths filter on `IsQueryVisible`, so a replica that is not query-visible yet — what `UpdateLoadConfig` spawns for a newly added resource group — is dropped before the list is built. Its group simply does not appear.

So **absence must not be read as "this group holds no replica"**, and the two states it conflates want opposite responses: a group still coming up is worth waiting for, a group nobody loaded into is not. §2.2's readiness surface is what separates them, reporting `ShardsWithoutLeader` for the first and `NoReplicaInResourceGroup` for the second. A caller gating a switchover should ask readiness; the tag is for routing once the decision is made.

The empty string is likewise **unknown, not "no resource group"** — an old coordinator fills the other three arrays and leaves this one empty, and proto3 gives the caller no other signal. The proxy indexes `resource_groups` defensively rather than assuming the arrays are parallel, so a rolling upgrade degrades to "unknown group" instead of panicking on every cache refresh.

## 3. Compatibility

- The proto change is additive: one optional field, one generated accessor. With the field unset, `GetShardLeaders` runs the identical replica filter and the identical gate as before.
- Compatibility runs **one way only**. An old caller is unaffected. A *new* caller must not assume the answer is scoped without establishing that the coordinator understands the field: proxy and coordinator deploy separately, so during a rolling upgrade a scoped request can reach a coordinator built before this field existed, which drops the unknown proto3 field silently and answers with every replica's leaders — in a response shape indistinguishable from a scoped one. There is no capability handshake on this path, so a caller that would be wrong to route on the unscoped answer must gate itself on the deployed coordinator version. This is stated on the field itself.
- Nothing in this PR sets the field; the only in-tree caller (`proxy/shardclient/manager.go`) sends `WithUnserviceableShards: true` and no resource group. Deployed traffic is unaffected, and this PR is where the field's contract is written.

## 4. Alternatives considered

- **Derive the group on the caller side from the existing response.** Impossible: the response discards replica identity, and node-set membership is not replica membership (§1). This is what forces a coordinator-side change of some kind.
- **Filter by resource group in the `GetShardLeaders` *request*.** Implemented first, then replaced by the response tag. It answers the same question but pushes a resource-group dimension into the proxy's shard-leader cache (N fetches and N entries per collection, and `InvalidateShardLeaderCache` has to be rethought), returns one group per call instead of all of them, and cannot be told apart from an old coordinator's unfiltered answer during a rolling upgrade. §2.3 has the comparison.
- **Filter in the request *and* keep the strict per-group refusal codes.** The refusal set that shape offered (`ErrReplicaNotFound` for a group holding nothing, `ErrCollectionNotFullyLoaded` for one still coming up) is genuinely useful for a caller gating a switchover — but it is the question `ShardLeaderReadinessByResourceGroup` already answers, in a structured verdict rather than an error code, and without inheriting the collection-wide gate. Keeping both would have meant two contracts for one question.
- **Make the progress figure monotone, matching `ShowLoadCollections`.** Requires persisted per-group state. The figure is deliberately a live coverage number; the divergences are documented and pinned by tests instead.
- **Reuse `checkLoadStatus` for the readiness surface.** It short-circuits to "ready" on the collection's aggregate `Loaded` status, which is the same admission bug from the other direction.

## 5. Testing

- `resource_group_load_percentage_test.go` — the `-1` / `0` / `ErrServiceNotReady` / `ErrCollectionNotLoaded` distinction, a failed load seeded with a *retriable* sentinel asserting the normalized result is neither retriable nor `ErrServiceNotReady`, partial progress, min-across-replicas, single-partition equivalence with the collection-wide figure, both deliberate divergences with concrete numbers (cross-partition pooling: 40 pooled vs 62 observer-style; re-arming: 100 → 66 after a new segment lands), stability across target promotion, failed-load surfacing after replica cleanup, and the partial-initialization guards including a nil `GlobalFailedLoadCache`.
- `shard_leader_readiness_test.go` — per-group readiness across serviceable/unserviceable leaders and unknown nodes, the reason strings, query-invisible replicas not counting until promoted, the collection-wide gate not being inherited, failed-load surfacing after replica cleanup, and byte-identical native `GetShardLeaders` output.
- `shard_leader_rg_tag_test.go` — each leader carrying the resource group of the replica it leads rather than anything node-derived; index alignment surviving a leader dropped at the NodeManager check (a leader on an unregistered node, which is how a misplaced `append` shifts every later tag); and the boundary of what the tag can answer — a query-invisible replica's group is absent from the response while readiness still reports `ShardsWithoutLeader` rather than `NoReplicaInResourceGroup`.
- `internal/proxy/shardclient/manager_test.go` — the tag reaching the proxy cache index-aligned, and an old coordinator's empty `resource_groups` reading as unknown instead of panicking on every cache refresh.
