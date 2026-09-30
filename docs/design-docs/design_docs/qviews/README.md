# Distributed Query View Design Document

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

## 1. Background and Motivation

StreamingNode needs to handle all incremental queries in Milvus while managing all data publish/subscribe operations. If the current delegator logic were placed directly on StreamingNode, it would cause the following problems:

1. All Segment Load/Release operations would need to be forwarded through StreamingNode (including Handoff caused by Compaction and other unrelated operations).
2. All Delete data would need to go through StreamingNode for Segment-level Apply.
3. All queries would need to be triggered through StreamingNode, and Shard-level Reduce would need to be executed by StreamingNode.
4. All QueryNode query result traffic would need to be forwarded through StreamingNode.

StreamingNode would become a compute-intensive and IO-intensive global bottleneck node, and scaling out and implementing multiple replicas would be extremely complex.

## 2. Core Architecture Changes

1. **StreamingNode is no longer responsible for Load/Release of SealedSegments**: QueryCoord directly manages all SealedSegment Load/Release operations. StreamingNode only accepts query view update requests from QueryCoord.
2. **QueryCoord is responsible for generating the globally complete distributed query view**.
3. **StreamingNode is no longer responsible for Search/Query logic forwarding**: Proxy uses a two-phase query approach — it generates a query plan on StreamingNode, then sends the query plan to designated QueryNodes to complete the query, and performs all distributed Reduce operations itself.
4. **StreamingNode no longer actively applies incremental delete data**: After LoadSegment, QueryNode proactively subscribes to the corresponding Delete data from StreamingNode and applies Delete data on its own.

## 3. Two-Phase Query Process

The following end-to-end flow describes the target architecture. The current
extraction implements the SN server side; it does not wire the new Proxy client,
complete Coord scheduling, or QN query execution/remote Delete subscriptions.

1. **Phase One**: Proxy generates a Shard-level query plan from StreamingNode using the highest version QueryView:
   - Includes MVCC
   - Query optimization (BM25, Segment filtering, etc.)
   - Query view version
2. **Phase Two**: Proxy sends queries to StreamingNode and QueryNode with the query plan:
   - StreamingNode and QueryNode execute query operations using Segments under the corresponding view version
   - Proxy reduces all results and returns them to the user
3. If a node failure or view invalidation occurs during the process, the query is canceled and retried directly.

### Current SN implementation

- **Phase 1:** `GetQueryPlan` acquires the highest available Up view, resolves
  query MVCC, performs request/BM25 optimization and selects the plan's workers.
  `GetMVCCTimestamp` exposes primary-WAL query frontiers. See the
  [consistency-level discussion](#13-consistency-implementation-consistency-level)
  for the current routing limits.
- **Phase 2:** `SearchOnView` and `QueryOnView` acquire the explicit Up version,
  wait for Growing/Transform MVCC, and pin segments selected by DataVersion and
  partition scope. The [serving lease](query_view_lease.md) protects the view
  during acquisition; handles retain resources during execution.
- **Execution dependency:** the new QueryView scheduler and resource path invoke
  legacy `querynodev2/tasks.SearchTask` / `QueryTask.ExecuteOnSegments` directly
  inside SN, with `querypb` request and collection/segment adapters. This reuses
  task execution and reduction, not the old QueryNode scheduler. TODO(#40451):
  replace these adapters with shared execution accepting plans and pinned
  segcore handles directly.
- **Scope:** `RequeryOnView` returns Unimplemented. The Proxy orchestration and
  complete SN/QN end-to-end flow above remain integration work.

Key source paths: `internal/streamingnode/server/queryplan/server.go`,
`internal/streamingnode/server/wal/adaptor/query_plan.go`,
`internal/streamingnode/server/wal/snview/query_task_provider.go`,
`internal/streamingnode/server/viewquery/{scheduler,executor,runner}.go`, and
`internal/views/viewquery/server.go`.

For hybrid search, Phase 1 builds independent mutable sub-requests for the
optimizer. Once optimization succeeds, it transfers each temporary request's
placeholder, serialized plan and partition slice back to its sub-request and
clears those fields in the temporary request. No optimizer retains these slices
after the transfer. Phase 2 expands independent sub-requests again for concurrent
execution; the ownership transfer changes no RPC fields or query semantics.

### Advantages

- StreamingNode logic is simplified; no need to migrate Load/Release and other QueryNode interfaces.
- Query processing load no longer converges on StreamingNode, mitigating the single-point bottleneck.
- The global single-point Delegator role is eliminated; Reduce and RPC bottlenecks can be resolved by scaling Proxy.
- Distributed query views facilitate query state persistence (requery, deletebyexpr, etc.).
- Strong consistency queries can eliminate the original tsafe wait time (100-200ms).
- Idle TimeTick can be completely removed from the system (MVCC).
- Recovery speed is improved; StreamingNode and QueryNode recovery do not interfere with each other.

## 4. Distributed Query View

### 4.1 Basic Requirements for Query Views

- **Completeness**: The query plan must contain a complete list of all segments.
- **No Duplication**: The same data must not be queried twice (the same segment may have both growing and sealed replicas simultaneously).
- **Leasable**: The query view should remain valid within a certain time window; queries should not be frequently interrupted due to view invalidation.
- **Swappable**: Query views can be switched quickly without causing unavailability.

### 4.2 Query View Data Composition

For a single Shard of a Collection, the complete distributed query view consists of:

- **Incremental portion** (maintained on StreamingNode):
  - **[A1]** Sealed and visible to Coord, but also loaded as Growing on StreamingNode.
  - **[A2]** Visible to StreamingNode but not to Coord (StreamingNode directly faces the stream and can see Growing Segments immediately; Coord must wait for Flusher to complete Flush before seeing them).
- **Historical portion**:
  - **[B1]** Maintained on QueryNode; Load operations are applied by Coord and are always visible to Coord.

## 5. Data Side — Storage View (DataView)

The complete contract, event triggers, delayed-visibility rules, persistence,
recovery, reference protection, and GC behavior are documented in
[DataView Design](data_view.md).

### 5.1 Overview

The storage view contains all complete, non-duplicate loadable Sealed Segment
data ([B1] and [A1]). A version number DataVersion is introduced:

- **streaming_version**: Incremented only by the flush atomic txn
  (PrepareFlush) for the growing-to-sealed query handoff.
- **compact_version**: Incremented for every other loadable-membership change,
  including import, copy completion, compaction, external refresh, L0 Manifest
  advance, partition drop, and truncate.

Version numbers are ordered lexicographically by
`(streaming_version, compact_version)`.

Completeness and logical non-duplication are projection contracts, not facts
derived by DataViewManager. The manager validates unique Segment-ID placement,
but the projection decides when a Segment is loadable and must not report
overlapping logical data under different Segment IDs.

Each partition also carries a packed Manifest-version array parallel to its
packed Segment-ID array. Version `0` keeps the Coordinator SegmentMeta watch
and resolves full SegmentInfo for loading. A positive version denotes a
canonical StorageV3 Manifest: QueryNode derives its object-storage path from
Collection/Partition/Segment IDs and the version, loads all data metadata from
the Manifest, and does not watch Coordinator SegmentMeta for that Segment.

### 5.2 Data Structures

`DataViewOfCollection`, `DataViewOfShard`, `DataViewOfPartition`, and
`DataVersion` are defined in [view.proto](../../../../pkg/proto/view.proto).

### 5.3 Storage View Version Evolution Example

The following timeline shows the current Collection-level snapshot behavior:

| Step | Event | DataView Version | Segments in the View |
|---|---|---|---|
| 1 | Create Collection | `(1,0)` | none; declared VChannels only |
| 2 | Flush Segments 1 and 2 | `(2,0)` | `1, 2` |
| 3 | Flush Segment 3 | `(3,0)` | `1, 2, 3` |
| 4 | L0 compact updates Segment 1 Manifest | `(3,1)` | `1, 2, 3` |
| 5 | Compact Segments 1 and 2 into Segments 4 and 5 | `(3,2)` | `3, 4, 5` |
| 6 | Cluster compaction or reshard | `(3,3)` | `6, 7, 8, 9` |
| 7 | Import Segment 10 | `(3,4)` | `6, 7, 8, 9, 10` |

Each row is a complete immutable snapshot. Every Segment entry also has a
Manifest version; the table omits the value for readability. The Collection
snapshot is identified and ordered by `DataVersion`.

Key observations:
- Only the flush atomic txn causes streaming_version to increment (for
  example, `(1,0) → (2,0) → (3,0)`).
- All other membership changes cause compact_version to increment (for
  example, `(3,1) → (3,2) → (3,3) → (3,4)`).
- L0 compaction changes no membership but advances Segment Manifest versions;
  this hard-triggers compact_version +1. Repeated L0 updates are collapsed by
  the recompute queue before the next drain.
- Compaction replaces its input membership with output membership in one
  snapshot. Clustering compaction keeps its output Segments invisible until
  they are published, so a snapshot rebuilt from the loadable projection never
  exposes them early.
- A Segment's Manifest version is monotonic across DataViews. Replaying the
  same version is a no-op, a higher version advances it, and a lower version is
  rejected (a projection reporting zero preserves the stored version).
- TODO: A future StreamingNode refactor will add the safe, monotonic shard
  `transform_start_after_timetick` protocol described in
  [Transform Start-After TimeTick](transform_start_after_timetick.md). The
  field is already present on the wire (`DataViewOfShard`, `QueryViewMeta`)
  for QueryView consumer compatibility, but the current branch does not
  advance or persist a meaningful frontier.

### 5.4 Constraints

- Membership changes create immutable DataVersions through `Recompute`
  (rebuilt from the SegmentMeta projection) or the flush atomic txn.
- DataViewManager does not understand compaction lineage. The projection must
  not report a superseded input Segment again after compaction removes it.
- SegmentMeta and Manifest updates do not automatically rewrite DataView: a
  mutation owner requests an asynchronous recompute after its SegmentMeta
  commit, and the manager-internal worker rebuilds the snapshot from the
  injected projection.
- The storage view version number is at the Collection level (laying the groundwork for future capabilities such as Shard splitting).
- DataView tracks loadable Segment membership and monotonically increasing
  Manifest versions.

## 6. Query Side — Query View (QueryView)

### 6.1 Version Number

Each query view version number is `(D, Q)`, where
`D = (streaming_version, compact_version)`. The full
ordering is therefore lexicographic by `(streaming_version, compact_version,
query_version)`:

- **StreamingVersion or CompactVersion increases**: immediately generate a
  QueryView because the growing/sealed handoff or loaded data changed.
- **Q increases**: Data undergoes load-level redistribution.

The query view version number is at the **ShardOnReplica level**, and its lifecycle is the same as the Load operation lifecycle of the corresponding replica.

### 6.2 Query View Version Evolution Example

The following timeline shows the version evolution process of the query view (QueryView), with each Segment labeled as `SegmentID @NodeID`:

| Step | Event | QueryView Version | Segment Placement |
|---|---|---|---|
| 1 | Place DataView `(2,0)` | `((2,0),1)` | `Segment 1 @Node1`, `Segment 2 @Node1` |
| 2 | Balance: move Segment 2 from Node1 to Node2 | `((2,0),2)` | `Segment 1 @Node1`, `Segment 2 @Node2` |
| 3 | DataView `(3,0)` adds Segment 3 | `((3,0),1)` | `Segment 1 @Node1`, `Segment 2 @Node2`, `Segment 3 @Node2` |
| 4 | Recovery balance after Node2 crashes | `((3,0),2)` | `Segment 1 @Node1`, `Segment 2 @Node1`, `Segment 3 @Node1` |
| 5 | DataView advances to `(3,3)` | `((3,3),1)` | `Segment 6 @Node1`, `Segment 7 @Node2`, `Segment 8 @Node3`, `Segment 9 @Node1`, `Segment 10 @Node2` |

Key observations:
- When composite D increases, Q is reset to 1 (new data at the storage level needs to be redistributed).
- An increase in Q represents pure load-level redistribution (Balance, Recovery); the data itself does not change.
- Node crashes are handled by generating a new QueryView, migrating crashed node's Segments to surviving nodes.

### 6.3 State Enumeration

See the definition of `QueryViewState` in [view.proto](../../../../pkg/proto/view.proto).

### 6.4 Data Structures

See the definitions of `QueryViewOfShard`, `QueryViewMeta`, `QueryViewVersion`,
`QueryViewOfQueryNode`, `QueryViewOfStreamingNode`, and
`QueryViewOfPartition` in [view.proto](../../../../pkg/proto/view.proto).

### 6.5 Constraints

- The version number `((S,C),Q)` of a QueryView in Up state may only increase
  non-strictly; rollback is not allowed.
- A Shard maintains a fixed upper limit of query views (typically 2–3, similar to a Double Buffer / Triple Buffer pipeline design).

## 7. Query View Lifecycle State Machine

The query view maintains consistency across Coord / QueryNode / StreamingNode, with Coord as the leader.

State transition flow:

```
Normal flow:   Preparing → Ready → Up → Down → Dropping → Dropped
Error flow:    Preparing → Unrecoverable → Dropping → Dropped
```

TODO(img/state_machine.png): add the global state machine transition diagram
when the QueryView documentation assets are picked.

For detailed per-node, per-state analysis (entry conditions, automatic behavior, transitions, peer state handling, persistence, and recovery), see [QueryView State Machine Per-Node Analysis](query_view_state_machine.md).

Key constraints:
- Workflows across multiple view versions are completely independent, but through Coord state machine constraints, each node has at most one view in Preparing state.
- QueryNode loss is handled only for active QN-targeted syncs: in Preparing it makes the view Unrecoverable, and in Dropping it counts that QN cleanup as complete. StreamingNode unavailability is handled by channel assignment, not by the QueryView per-view state machine.

The SN renewable serving lease delays local Up → Down during active use; see
[StreamingNode QueryView Serving Lease](query_view_lease.md).

## 8. Incremental Query Segment Lifecycle

In the target QueryView integration, incremental Segments generated from WAL on
StreamingNode follow Coord-driven lifecycle instructions:

```
Growing → Sealed [flush streaming_version S1] → Release
```

| State | State Transition Condition | Description | Query Behavior |
|---|---|---|---|
| **Growing** | Discovered from WAL | Segment is in Growing state; Coord has not yet managed it | Always queried |
| **Sealed [S1]** | Consumed Flush publication metadata | Segment was added by a DataView whose `streaming_version` is S1 | QueryView DataVersion with `streaming_version < S1`: still query it on SN; `streaming_version >= S1`: the sealed handoff may exclude it from growing-side queries |
| **Release** | SN required streaming-version watermark ≥ S1 and no retained view needs this Segment | Segment does not participate in any queries | Noop |

The Sealed state is retained on StreamingNode until the local required
streaming-version watermark reaches S1. This delayed GC is required for crash
recovery when a persisted Up view is older than the latest local SegmentModule
state: the old Up view still needs a flushed-at-S1 Segment as a growing-side
resource if its DataVersion has `streaming_version < S1`.

The recovery-storage implementation binds the first Flush DataVersion in
DataCoord SegmentInfo and StreamingNode SegmentAssignmentMeta as
`sealed_at_data_version`. Retried commits return this immutable version.
The current SN QueryRuntime integration uses this binding to filter query
membership, advances reclamation by the minimum retained DataVersion, and pins
physical resources for active queries. See the
[SN WAL input view](../wal/streamingnode_vchannel_wal_view.md) for preparation,
readiness and retention; the binding alone is not sufficient without this wiring.

## 9. Historical Query Segment Lifecycle

Sealed Segments on QueryNode:

```
Loaded → Release
```

| State | State Transition Condition | Query Behavior |
|---|---|---|
| **Loaded** | A new incoming view loads this Segment | Queried when the target view uses this segment |
| **Release** | No view on the current QN contains this Segment | Noop |

## 10. Resources and View Dependencies

- All resources are tied to view dependencies (except growing segments; see Section 8).
- Resource lifecycle ≥ the union of lifecycles of all query views that hold it.
- Resources are released when their associated query views are released.
- Multi-version view support enables atomic updates on nodes to ensure resource liveness, reducing the frequency of resource operations.

StreamingNode resources are prepared by QueryView state machines. The QueryView's
`load_info_version` resolves the required partitions, fields, and index metadata;
`AlterLoadConfig` no longer creates vchannel-local state in `VChannelMeta`.
When QueryView state enters the local Preparing/UpRecovering path, the state
machine calls the PChannel-local
`VChannelRecoveryModule` through `Acquire`; the module builds the query input
view from its StreamingNode-local WAL recovery data view, Segment state, and
TransformLog, then keeps
consuming DML so the recovered DataView only grows while the QueryView is live.
Long-term resource retention is driven by local QueryView references.
QueryNode sealed segment resources continue to follow QueryNode's segment/view resource
lifecycle.

TODO(qnview/querynode_queryview_resource_preparation.md): add the QueryNode-side
sealed segment resource preparation design when that resource module is picked.

Target coordination example: if a node cannot satisfy view A's resource
requirements, it reports A as Unrecoverable. Coord can prepare a replacement B
and arrange A's cleanup. Successful shared resources may be reused according to
their ownership rules; this does not require retaining a failed partial SN
bootstrap. The current SN builder closes failed partial modules and constructs
a fresh runtime on a locally retryable attempt. A process killed by OOM cannot
report a view failure; node failure detection must handle that case.

[SN WAL input view and QueryRuntime preparation](../wal/streamingnode_vchannel_wal_view.md)
describes the implemented resource manager, GrowingRuntime bootstrap, readiness
barriers and retention. Later load-info/schema changes and historical load-info
resolution remain explicitly deferred there.
[StreamingNode IDF Oracle Runtime](snview/idf_oracle_runtime.md) defines the
single locally prepared BM25 aggregate shared by all QueryView DataVersions.

## 11. Coord and Node Interactions

### 11.1 Design Principles

- **Coord**: Obtains global information, computes and generates QueryViews, and advances the state machine. No longer manages resource preparation workflows.
- **Node**: Responsible for preparing resources required by QueryViews and reporting resource preparation status.
- **Failure ownership**: Worknodes retry locally recoverable failures. Coord
  handles failures that cannot be resolved by the current node/view, including
  resource shortages requiring placement or allocation changes. See the
  [contract and current gaps](../wal/streamingnode_vchannel_wal_view.md#preparation-failure-ownership).

### 11.2 Component Modules

| Node | Module | Responsibility |
|---|---|---|
| Coord | Node Manager | Service discovery, maintaining the global available QueryNode list |
| Coord | Resource Group Manager | Resource Group partitioning, generating QueryNode-ResourceGroup grouping relationships |
| Coord | Replica Manager | Replica assignment, generating Replica-to-available-Node relationships |
| DataCoord (inside MixCoord) | DataView Manager | Maintaining immutable Collection snapshots and DataViewRefs |
| Coord | Sealed Segment Balancer | Gathering information from all Managers, generating and distributing QueryViews |
| Coord | QueryView Manager | View state machine transitions, syncing view information to all Nodes |
| Streaming Node | PChannel Query Resource Manager | Preparing vchannel resources from versioned load info, latest schema, SegmentModule views, TransformLog, and BM25 resource RPC |
| Streaming Node | QueryView Manager | Listening for view state machine changes, checking prepared view resources, and publishing the required DataVersion watermark for SN-only eviction |
| Streaming Node | TransformLog adaptor | Local Summary-backed SN bootstrap is wired; remote QN publication remains planned. See [TransformLog](../wal/transform_log.md). |
| Streaming Node | Growing Segment Manager | Incremental data management, maintaining Growing Segment lifecycle |
| Query Node | QueryView Manager | Listening for view state machine changes, applying to Sealed Segments |
| Query Node | Sealed Segment Manager | Historical data management, maintaining Sealed Segment lifecycle |
| Query Node | Pure Delete Stream Manager | Planned remote subscription client applying Delete data to each Segment; not wired by this SN extraction. |

### 11.3 SyncQueryView RPC

The sole RPC that unifies the synchronization layer behavior of StreamingNode
and QueryNode. See the definitions of `ViewSyncService`, `SyncRequest`,
`SyncResponse`, and related messages in
[view.proto](../../../../pkg/proto/view.proto).
See [Syncer](syncer.md) for the Coord-side transport design.

RPC rules:
- The QueryView list is atomically applied to the local QueryViewManager.
- The Node's async Scheduler parses Load/Release operations and applies them to other components.
- After a view reaches its target state, the updated result is pushed to Coord.
- **The Node's Response always carries the latest local state**. This ensures that at any point (including after Recovery), Coord can reconstruct its awareness of the node's true state through a single SyncQueryView interaction, without relying on intermediate states persisted in ETCD.
- State machine transitions strictly follow the rules; signals that break the rules are ignored.
- Can be implemented via polling or Stream RPC (Stream RPC avoids polling overhead).
- Fully idempotent.

## 12. Detailed Node Behavior

For detailed per-node state machine analysis (entry conditions, automatic behavior, transitions, peer state handling, and recovery), see [QueryView State Machine Per-Node Analysis](query_view_state_machine.md).

## 13. Consistency Implementation (Consistency Level)

### TODO: Consistency Levels — Pending Discussion

The table below records the proposed target behavior, not a completed capability
matrix or a finalized replica-routing policy. Replica-SN MVCC selection and the
mapping of consistency levels to primary/replica routing remain pending discussion.

| Level | MvccTimestamp Generation Logic |
|---|---|
| **Strong** | Proxy requests a query plan from the primary SN. SN obtains the maximum ts of messages written to the current WAL VChannel as MvccTimestamp (if ts has not yet triggered timeticksync, trigger it proactively) |
| **Bounded** | Proxy requests from any SN. Primary SN → same as Strong; Replica SN → obtains the maximum ts of the current WAL subscription stream VChannel |
| **Session** | Same as Strong |
| **Eventual** | Same as Bounded |

**Current implementation:** `wal/adaptor/query_plan.go` handles every
`ConsistencyLevel` request through the RW WAL's local query MVCC and rejects
non-RW WALs with NotPrimary; it does not branch on the level's enum value.
`GetMVCCTimestamp` also requires RW access. A request carrying an explicit
`QueryPlanMVCC` follows a separate path and does not demonstrate that replica
selection or the table's consistency-level routing has been implemented.

The follow-up discussion must settle which node supplies each level's MVCC,
what freshness/visibility it guarantees, and how the client routes or retries
when only a non-primary node is available. No routing or MVCC behavior changes
are part of this documentation update.

Target changes:
- GuaranteeTS assignment logic is moved down to StreamingNode, obtained from the WAL system.
- MvccTimestamp and GuaranteeTS are merged and always kept consistent.
- ts will trend toward LSN rather than system time in the future.

## 14. Pure Delete Stream

The current [TransformLog adaptor](../wal/transform_log.md) wraps WALSummary
reads. Local SN bootstrap uses bounded replay; subsequent SN events arrive via
the VChannel live path. The local unbounded mode has scoped notifications, but
remote QN subscriptions and their retention integration remain planned.

The following describes future pure-delete consumption options, not enabled
QN behavior in this extraction:

- During Recovery, pure delete stream subscriptions use batch processing for merging.
- L0 is used on StreamingNode.
- Bloom filter filtering + batch merge of delete data at the Node level.
- Remote Load L0 (conflicts with Bloom filter filtering; choose one of the two).
- Subscription catch-up merging.

## 15. TODO: DDL Query Visibility

[Truncate and Partition Drop Query Visibility](ddl_visibility.md) records the
agreed follow-up: typed TransformLog entries delivered to the affected SN/QN
consumers enable MVCC-based segment exclusion. Until then, DDL query visibility
uses a QueryView handoff fence (QueryCoord target update in the legacy model).
The MVCC extension and distributed delivery are outside the current SN query
extraction PR; a progress barrier alone does not implement DDL visibility.
