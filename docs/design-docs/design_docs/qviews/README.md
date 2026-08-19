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

TODO(query/query_client.md): add the detailed query-path flow, service boundary,
client orchestration, and shard discovery design when the query path is picked.
TODO(query/query_plan.md): add the node-side Phase 1 planning design when that
module is picked.
TODO(query/query_execution.md): add the node-side Phase 2 execution design when
that module is picked.

1. **Phase One**: Proxy generates a Shard-level query plan from StreamingNode using the highest version QueryView:
   - Includes MVCC
   - Query optimization (BM25, Segment filtering, etc.)
   - Query view version
2. **Phase Two**: Proxy sends queries to StreamingNode and QueryNode with the query plan:
   - StreamingNode and QueryNode execute query operations using Segments under the corresponding view version
   - Proxy reduces all results and returns them to the user
3. If a node failure or view invalidation occurs during the process, the query is canceled and retried directly.

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

- **streaming_version**: Incremented only when StreamingNode Flush publishes a
  Segment for the growing-to-sealed query handoff.
- **compact_version**: Incremented for every other loadable-membership change,
  including import, copy completion, compaction, external refresh, partition
  drop, and truncate.
- **transform_version**: Incremented when L0 compaction advances Segment
  Manifest versions without changing loadable membership.

Version numbers are ordered lexicographically by
`(streaming_version, compact_version, transform_version)`.

Completeness and logical non-duplication are publication contracts, not facts
derived by DataViewManager. The manager validates unique Segment-ID placement,
but callers decide when a Segment is loadable and must not publish overlapping
logical data under different Segment IDs.

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
| 1 | Create Collection | `(1,0,0)` | none; declared VChannels only |
| 2 | Flush Segments 1 and 2 | `(2,0,0)` | `1, 2` |
| 3 | Flush Segment 3 | `(3,0,0)` | `1, 2, 3` |
| 4 | L0 compact updates Segment 1 Manifest | `(3,0,1)` | `1, 2, 3` |
| 5 | Compact Segments 1 and 2 into Segments 4 and 5 | `(3,1,0)` | `3, 4, 5` |
| 6 | Cluster compaction or reshard | `(3,2,0)` | `6, 7, 8, 9` |
| 7 | Import Segment 10 | `(3,3,0)` | `6, 7, 8, 9, 10` |

Each row is a complete immutable snapshot. Every Segment entry also has a
Manifest version; the table omits the value for readability. Existing producers
publish `0` until they can guarantee a committed canonical StorageV3 Manifest.
The Collection snapshot is still identified and ordered by `DataVersion`.

Key observations:
- Only Flush operations cause streaming_version to increment (for example,
  `(1,0,0) → (2,0,0) → (3,0,0)`).
- All other membership changes cause compact_version to increment (for
  example, `(3,0,1) → (3,1,0) → (3,2,0) → (3,3,0)`).
- L0 compaction changes no membership and increments only transform_version;
  repeated L0 updates can be coalesced before a QueryView adopts them.
- Compaction replaces its input membership with output membership in one
  snapshot.
- Until StreamingNode performs Flush sorting directly, SortCompaction first
  publishes the immediately loadable flushed Segment and later replaces it
  with the final sorted Segment through a CompactVersion update. The flushed
  Segment is not hidden while sorting waits or runs.
- A Segment's Manifest version is monotonic across DataViews. Replaying the
  same version is a no-op, a higher version advances it, and a lower version is
  rejected.
- L0 compaction persists higher target Segment Manifest versions in a new
  immutable TransformVersion.
- TODO: A future StreamingNode refactor will add the safe, monotonic shard
  `transform_start_after_timetick` protocol described in
  [Transform Start-After TimeTick](transform_start_after_timetick.md). The
  current branch does not persist or publish this frontier.

### 5.4 Constraints

- Membership and L0 materialization changes create immutable DataVersions.
- DataViewManager does not understand compaction lineage. Event producers must
  not publish a superseded input Segment again after compaction removes it.
- SegmentMeta and Manifest updates do not automatically rewrite DataView.
  Version-0 Segments observe them through the Coordinator watch path. A higher
  version may be published through an existing membership event or the
  L0-specific event; there is no generic Manifest-update API.
- The storage view version number is at the Collection level (laying the groundwork for future capabilities such as Shard splitting).
- DataView tracks loadable Segment membership and monotonically increasing
  Manifest versions.

## 6. Query Side — Query View (QueryView)

### 6.1 Version Number

Each query view version number is `(D, Q)`, where
`D = (streaming_version, compact_version, transform_version)`. The full
ordering is therefore lexicographic by `(streaming_version, compact_version,
transform_version, query_version)`:

- **StreamingVersion or CompactVersion increases**: immediately generate a
  QueryView because the growing/sealed handoff or loaded data changed.
- **Only TransformVersion increases**: keep the current QueryView serving and
  coalesce the update. A later hard update, balance, retention threshold, or
  maintenance interval eventually generates a QueryView at the latest
  TransformVersion.
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

The current DataView PR returns the Flush DataVersion but does not durably bind
S1 to the Segment or deliver that binding to StreamingNode. That integration is
required before this release rule is enabled.

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

Example: If view A is unreasonable and causes OOM on a node, it is marked as Unrecoverable, but view A still exists on the node and already-loaded resources are not rolled back. After Coord detects this, the Balancer generates a new view B and pushes both views for atomic modification (A Dropped, B Preparing). Resources in (A diff B) are released, resources in (B diff A) are loaded, and resources in A∩B are retained.

TODO(snview/streamingnode_resource_manager.md): add the StreamingNode query
runtime manager design when that resource module is picked.
TODO(snview/growing_segment_runtime.md): add the StreamingNode growing segment
runtime design when that resource module is picked.
TODO(snview/idf_oracle_runtime.md): add the StreamingNode IDF oracle runtime
design when that resource module is picked.

## 11. Coord and Node Interactions

### 11.1 Design Principles

- **Coord**: Obtains global information, computes and generates QueryViews, and advances the state machine. No longer manages resource preparation workflows.
- **Node**: Responsible for preparing resources required by QueryViews and reporting resource preparation status.

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
| Streaming Node | Pure Delete Stream Manager | Acting as subscription server, publishing Delete data to QueryNodes. TODO(../wal/transform_log_view_module.md): add the TransformLog view module design. |
| Streaming Node | Growing Segment Manager | Incremental data management, maintaining Growing Segment lifecycle |
| Query Node | QueryView Manager | Listening for view state machine changes, applying to Sealed Segments |
| Query Node | Sealed Segment Manager | Historical data management, maintaining Sealed Segment lifecycle |
| Query Node | Pure Delete Stream Manager | Acting as subscription client, applying Delete data to each Segment. TODO(../wal/transform_log_view_module.md): add the TransformLog view module design. |

### 11.3 SyncQueryView RPC

The sole RPC that unifies the synchronization layer behavior of StreamingNode
and QueryNode. See the definitions of `ViewSyncService`, `SyncRequest`,
`SyncResponse`, and related messages in
[view.proto](../../../../pkg/proto/view.proto).
TODO(syncer.md): add the Coord-side transport design when the syncer is picked.

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

### Consistency Levels

| Level | MvccTimestamp Generation Logic |
|---|---|
| **Strong** | Proxy requests a query plan from the primary SN. SN obtains the maximum ts of messages written to the current WAL VChannel as MvccTimestamp (if ts has not yet triggered timeticksync, trigger it proactively) |
| **Bounded** | Proxy requests from any SN. Primary SN → same as Strong; Replica SN → obtains the maximum ts of the current WAL subscription stream VChannel |
| **Session** | Same as Strong |
| **Eventual** | Same as Bounded |

Key changes:
- GuaranteeTS assignment logic is moved down to StreamingNode, obtained from the WAL system.
- MvccTimestamp and GuaranteeTS are merged and always kept consistent.
- ts will trend toward LSN rather than system time in the future.

## 14. Pure Delete Stream

StreamingNode already implements Pub-Sub capability. PureDeleteStreamManager wraps and optimizes on top of it:

- During Recovery, pure delete stream subscriptions use batch processing for merging.
- L0 is used on StreamingNode.
- Bloom filter filtering + batch merge of delete data at the Node level.
- Remote Load L0 (conflicts with Bloom filter filtering; choose one of the two).
- Subscription catch-up merging.
