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

The critical property is that **this cannot be fixed outside the coordinator**. `querypb.ShardLeadersList` carries only the channel name plus the node ids, addresses and serviceable flags of the leaders on it; the builder flattens every replica of the collection into that one list, so the replica each leader belongs to — and with it the resource group — is discarded before the answer leaves QueryCoord. Recovering the mapping by intersecting node ids with a resource group's node set does not work either: a replica may borrow nodes from another resource group (QueryCoord models exactly that as `num_outgoing_node` / `num_incoming_node`), so node-set membership is not replica membership — and the two diverge precisely during the rebalance windows a readiness check exists to catch.

## 2. Design

Three read-only surfaces, no new persisted state, no change to the replica model.

```
                         ┌──────────────────────────────────────────┐
                         │  querycoordv2/utils  (free functions)    │
   Server methods ──────►│                                          │
   (stable entry points) │  LoadPercentageByResourceGroup           │  progress
                         │  ShardLeaderReadinessByResourceGroup     │  can it serve?
   GetShardLeaders RPC ─►│  GetShardLeadersByResourceGroup          │  route to whom?
                         └──────────────────────────────────────────┘
                            reads: Meta, TargetManager,
                                   DistributionManager, NodeManager
```

The computations are **free functions in `querycoordv2/utils`, not methods on `Server`**, because `CollectionObserver` and the other observers hold exactly these read-only stores and cannot import the `querycoordv2` root package. `Server` methods exist alongside them so external callers keep a stable entry point.

### 2.1 `LoadPercentageByResourceGroup` — progress

Selects the replicas whose own resource group is `rgName`, computes each one's target coverage, and reports the **minimum**. A caller deciding whether a resource group can be trusted wants the laggard, not the average or the best replica: the group is only "ready" once every replica in it is.

Its outcome space is deliberately three-valued, because "the collection isn't ready here" hides three different states a caller must act on differently:

| Result | Meaning | What the caller should do |
|---|---|---|
| `-1`, no error | The group holds no replica of this collection at all | Terminal. Load it, or ask about a different group. |
| `0`, no error | A replica is there but carries none of the targets yet | Wait; loading is underway. |
| `-1`, `ErrServiceNotReady` (1, retriable) | The coordinator's own read stores are not wired up yet | Retry. Nothing is known, as opposed to something being known to be absent. |
| `-1`, `ErrCollectionNotLoaded` (101, non-retriable) | The load failed terminally; `GlobalFailedLoadCache` holds the reason | Stop. Waiting will not help; the cause is in the message. |

The third row exists because `initQueryCoord` wires the meta before the distribution and target managers, so a partially wired `Server` is genuinely reachable. Answering it with a bare `-1` would assert "this resource group holds no replica" — a claim the function is in no position to make.

The last two rows are why the recorded load failure is **normalized** rather than returned verbatim. `FailedLoadCache` stores whatever error the failing load task recorded, and a load genuinely fails with retriable sentinels — `ErrServiceNotReady` is what `LoadSegments` returns when the target query node is restarting, and the scheduler's exclusion list does not filter it out before `recordSegmentTaskError` stores it. That is the *same* code the init window uses to mean "retry, this fixes itself". Returned as-is, a load that is never coming back would be indistinguishable from one that is, and a caller following this contract would retry until the cache entry expires 24h later. `ShowLoadCollections` and `ShowLoadPartitions` normalize the same cache the same way, which is the parity this surface claims. The readiness surface normalizes identically; its `Reason` field already disambiguates for a struct-reading caller, but the error code is what a caller triaging on `merr` sees.

### 2.1.1 One registration test across all three surfaces

All three surfaces — and the scoped `GetShardLeaders` gate — test "is this collection registered as loaded" with `CalculateLoadPercentage(...) >= 0`, never `m.Exist`. The two disagree on a collection record with **zero partitions**: `Exist` checks only the collection map and returns true, while `calculateLoadPercentage` additionally requires a non-empty partition set and otherwise falls through to `-1`.

That state is reachable and can persist. `job_load.go` calls `RemovePartition` — an independent etcd commit that does not touch the collection key — *before* `PutCollection`, and when the incoming partition set is disjoint from the loaded one, `toReleasePartitions` covers every current partition. The window is observable by a concurrent reader, and a crash inside it leaves etcd holding the collection key with zero partition keys, which `CollectionManager.Recover` restores as a `Loaded` record over an empty partition loop.

Under `m.Exist`, readiness would report `Ready=true` and the percentage `100` for a collection whose scoped routing is refused with `ErrCollectionNotLoaded` (101, non-retriable, so the gRPC layer does not even resend) — a caller gating a switchover on the first two would cut traffic over and then have every route permanently refused. Using one test everywhere makes the three surfaces structurally incapable of that disagreement, and matches what `ShowLoadCollections` has always used.

Per-replica coverage is measured against `meta.NextTargetFirst`, the same target the `CollectionObserver` measures progress against. `NextTargetFirst`, not `NextTarget`: promotion clears the next target until the observer re-pulls it ~10s later, and a plain `NextTarget` read in that window sees an empty target and reports 0 — so a fully loaded, serving group would flap 100/0 on every promotion.

Query-**invisible** replicas (load-config spawns replicas invisible until every one of them is serviceable) are deliberately **included** here. This is a progress figure, and those replicas are exactly the ones whose progress the load-config path is waiting on.

That inclusion is also the one place the three surfaces deliberately disagree, so it carries a **pairing rule: 100 is not a servability verdict.** Readiness and scoped `GetShardLeaders` both exclude query-invisible replicas — a leader the proxy can never be routed to cannot serve — so a group whose replicas are all still invisible reads `100` here while readiness says `Ready=false` and scoped routing refuses every shard with the retriable `ErrCollectionNotFullyLoaded`. This is a normal product state: `UpdateLoadConfig` with `needWaitRGReady` spawns the new group's replicas `WithQueryInvisible`, and promotion is global and all-or-nothing, so the new group can finish carrying every target of its own while promotion stays blocked on an unrelated replica. **A caller gating a switchover must pair the percentage with the readiness verdict**; acting on `100` alone cuts traffic to a group that cannot answer, and keeps retrying it for as long as that unrelated replica stays unserviceable, instead of staying on the old one.

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

It does **not** reuse `checkLoadStatus`. That gate is collection-wide by construction: it reads `CalculateLoadPercentage(collectionID)` and then short-circuits to "ready" whenever the collection's own status is `LoadStatus_Loaded`, so under per-resource-group loading it passes as soon as *any* group finishes — the same admission bug from the opposite direction. The gate here is derived only from the leaders of the selected replicas: nothing about another group's progress, and nothing about the collection's aggregate status, can make this report ready.

Unlike the progress figure, only **query-visible** replicas count as able to serve, matching the `IsQueryVisible` filter every `GetShardLeaders` path applies — a leader the proxy can never be routed to must not make its group look ready. A query-invisible replica still keeps the group out of the `NoReplicaInResourceGroup` bucket, whose meaning is "waiting will never help".

In both this surface and the progress figure, the **load-registration check runs before the replica scan**. The terminal failed-load state is the one `CollectionObserver.observeTimeout` leaves behind, with the collection registration *and* every replica record removed and only the `GlobalFailedLoadCache` entry remaining; scanning replicas first would turn that state into a bare "nothing is loading here" and swallow the recorded failure.

### 2.3 `GetShardLeadersRequest.resource_group` — route to whom?

A new optional proto field. When set, `services.GetShardLeaders` takes `utils.GetShardLeadersByResourceGroup` instead of the native path.

This is a separate entry point rather than a `replicaFilter` over the existing one, because the scoped question needs a **different load gate**, not just a different filter:

- The **registered-at-all** half of `checkLoadStatus` runs **first**, before any resource-group reasoning, so a collection that is not loaded at all answers `ErrCollectionNotLoaded` for the scoped shape exactly as for the unscoped one. The proxy's retry policy branches on that code (`shardclient/lb_policy.go`), so the two shapes must not answer different families for the same state.
- The **full-load** half does not run at all — it is the collection-wide figure a lagging sibling group keeps below 100, i.e. the exact state the scope exists to see through. In its place the strict form runs the *scoped* equivalent: every current-target channel must have a serviceable, query-visible leader inside this group.

#### The refusal contract

`merr.Status` copies each sentinel's retriable bit onto the wire, and the generic gRPC client wrapper re-issues the call only when that bit is set — so the choice of sentinel *is* the caller's wait-or-give-up decision, not decoration. The strict scoped form has exactly three refusals:

| State | Sentinel | Retriable | Why |
|---|---|---|---|
| Collection not registered as loaded | `ErrCollectionNotLoaded` (101) | no | Same family and code as the unscoped shape for the same state. |
| Loaded, but this group holds no replica | `ErrReplicaNotFound` (400) | no | Terminal by construction: the answer cannot change until someone loads the collection into this group. Refused up front, **by name** — falling through to the channel walk would blame a channel that is fine (a sibling group may be serving it right now) and invite a retry that can never succeed. This is the shard-leader counterpart of the progress figure's `-1`. |
| Holds a replica, not every shard covered yet | `ErrCollectionNotFullyLoaded` (103) | **yes** | Ordinary load progress; waiting is the right response. |

The third row is why the scoped path must **not** reuse the per-channel `ErrChannelNotAvailable` (503, non-retriable) that the native path raises. On the native path that error only occurs *after* the full-load gate has already passed, so a missing leader there really is channel-level unavailability. On the scoped path the gate is gone, so the same code would be reached by a group that is simply still coming up — telling a caller "permanent failure" about a state that self-heals in seconds. Reserving 103 for it also keeps the two shapes on one story: both answer 103 while the collection is still coming up, differing only in *whose* progress they measure.

Flipping `ErrChannelNotAvailable`'s global retriable bit was rejected: it is raised from several delegator and DataCoord sites, and `shardclient/lb_policy.go` relies on `IsRetryableErr` to choose between excluded-node and blacklist handling.

#### Empty resource group

All three surfaces agree that `""` is **the absence of a filter, not a filter that matches nothing** — which is also what the proto field documents, since `""` is what an old caller sends. `GetShardLeadersByResourceGroup` implements this by handing an empty request back to the unscoped path whole, gate included: the scoped gate is justified only by a *named* group, and this keeps the unscoped answer byte-identical to what it was before the field existed.

## 3. Compatibility

- The proto change is additive: one optional field, one generated accessor. With the field unset, `GetShardLeaders` runs the identical replica filter and the identical gate as before.
- Compatibility runs **one way only**. An old caller is unaffected. A *new* caller must not assume the answer is scoped without establishing that the coordinator understands the field: proxy and coordinator deploy separately, so during a rolling upgrade a scoped request can reach a coordinator built before this field existed, which drops the unknown proto3 field silently and answers with every replica's leaders — in a response shape indistinguishable from a scoped one. There is no capability handshake on this path, so a caller that would be wrong to route on the unscoped answer must gate itself on the deployed coordinator version. This is stated on the field itself.
- Nothing in this PR sets the field; the only in-tree caller (`proxy/shardclient/manager.go`) sends `WithUnserviceableShards: true` and no resource group. Deployed traffic is unaffected, and this PR is where the field's contract is written.

## 4. Alternatives considered

- **Derive the scoped answer on the caller side from `GetShardLeaders`.** Impossible: the response discards replica identity, and node-set membership is not replica membership (§1).
- **Apply a `replicaFilter` to the existing `GetShardLeaders` path.** Filters the answer but not the gate, so the collection-wide full-load check would still refuse the very group the scope exists to serve.
- **Make the progress figure monotone, matching `ShowLoadCollections`.** Requires persisted per-group state. The figure is deliberately a live coverage number; the divergences are documented and pinned by tests instead.
- **Reuse `checkLoadStatus` for the readiness surface.** It short-circuits to "ready" on the collection's aggregate `Loaded` status, which is the same admission bug from the other direction.

## 5. Testing

- `resource_group_load_percentage_test.go` — the `-1` / `0` / `ErrServiceNotReady` / `ErrCollectionNotLoaded` distinction, a failed load seeded with a *retriable* sentinel asserting the normalized result is neither retriable nor `ErrServiceNotReady`, partial progress, min-across-replicas, single-partition equivalence with the collection-wide figure, both deliberate divergences with concrete numbers (cross-partition pooling: 40 pooled vs 62 observer-style; re-arming: 100 → 66 after a new segment lands), stability across target promotion, failed-load surfacing after replica cleanup, and the partial-initialization guards including a nil `GlobalFailedLoadCache`.
- `shard_leader_readiness_test.go` — per-group readiness across serviceable/unserviceable leaders and unknown nodes, the reason strings, query-invisible replicas not counting until promoted, the collection-wide gate not being inherited, failed-load surfacing after replica cleanup, and byte-identical native `GetShardLeaders` output.
- `shard_leader_scope_test.go` — scoped routing, the three refusals and their retriable bits (at both the fixture and the sentinel level), the scoped gate seeing through a lagging sibling group in the exact state of §1 (status `Loading`, collection-wide percentage 50, current target promoted by one group's delegators alone), a fully loaded collection not making a group ready whose own leader is unserviceable, the not-loaded error family matching across both request shapes, an empty scope reproducing the unscoped answer verbatim in both the strict and the loose form, and a zero-partition collection record reading as not-loaded on all three surfaces at once.
