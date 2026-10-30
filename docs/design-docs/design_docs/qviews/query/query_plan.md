# QueryView plan, schema and read boundaries

## Phase 1 binding

The WAL adaptor selects the highest Up QueryView whose SchemaVersion exactly
matches the legacy request. Version zero is an exact schema identity too.
It never substitutes the latest schema. For an unknown replica ID, the handler
locates the local shard by vchannel and returns its real identity in the plan.
Matching UpRecovering state waits for local recovery; missing or unusable views
return the existing View errors.

Planning first acquires a serving View with the request's exact schema, then
fixes one MVCC and waits for the runtime's Growing and Transform data visibility.
It resolves the View's loaded partitions and optimizes against its exact read
runtime. The lease is renewed just before a successful plan return.

The selected QueryView determines read-schema identity and resource lifetime.
A later write-schema change does not independently revoke fields in a retained
serving View. Search, Query and requery share this rule; a missing or unavailable
View fails through the View lifecycle. MVCC determines data visibility and does
not grant access to a View. There is no field allowlist in the plan and no
Phase 1 declaration of future requery fields. Field errors are reported when
the corresponding request is planned/executed against the selected resources.

## Three distinct timestamp contracts

| Input | Meaning | Retry behavior |
| --- | --- | --- |
| Consistency level | Obtain paired frontiers from a complete published primary-WAL prefix | May select a new prefix when replanning |
| Explicit `QueryPlanMVCC` | Caller fixes Growing and Transform frontiers | Preserve both frontiers |
| `BarrierMVCC=B` | Establish WAL coverage for a scalar snapshot B | Preserve logical B; execution frontiers may be smaller where the covered prefix proves no intervening events |

`QueryMVCCManager` owns one executable frontier map, the complete published
prefix position, and pending freshness bounds. Successful append callbacks only
raise the pending bounds. Before appending the confirmation to WAL and WAB, the
serialized sync operator applies successful ACK messages in TimeTick order and
atomically publishes both frontiers with the complete ACK-prefix position. A pair
is never assembled from append-time maxima sampled at different stages.

Strong/Session fixes its minimum once from the completed query-relevant writes
of this VChannel and global publication operations. Later appends do not move
that attempt's target. Eventually may reuse the current completed snapshot;
Bounded also requires the prefix to cover the request's GuaranteeTimestamp.
An adequate snapshot returns without sync. Otherwise ordinary TimeTick sync is
triggered and the query waits under caller and WAL-owner cancellation. Pure
Growing writes do not increase TransformMVCC or QueryNode's wait target.

ACK publication can precede the deferred append callback. Callbacks at or below
the published prefix are ignored; newer callbacks retain only a pending bound.
Only ordered publication changes channel membership, so delayed Create/Drop
callbacks cannot register or revive channels. Recovery initializes live channels
at its complete opening fence before append admission opens.

Customized reads preserve the request's GuaranteeTimestamp as a lower bound
on the logical snapshot. Planning chooses B as the maximum of that guarantee and
the currently visible frontiers, then uses the same barrier resolution as an
explicit BarrierMVCC. This preserves already observed writes without requiring
both execution frontiers to reach the guarantee.

The manager logically owns (G,T,P): per-channel Growing/Transform frontiers and
the shared PChannel complete-prefix boundary P (`through`). All three are read
atomically; P is not a per-View version or a new wire field. `SnapshotAt(B)`
requires P >= B and returns (min(G,B), min(T,B)). A zero pair denotes a missing
channel and is rejected by planning. This reads at logical B without creating
Transform progress solely because Growing writes or idle confirmations advanced.

WAL initialization constructs the MVCCManager at the opening fence. After
bounded recovery, the opener registers recovered live channels before append
admission and live replay resume.

The live order is complete ACK prefix -> query MVCC publication -> confirmation
append to WAL -> WAB append. Successful data entries in that prefix are already
durable. Publication grants query targets, not consumer progress: planning and
execution retain their Growing/Transform visibility waits. Ordinary backend
errors retry within WAL.Append; terminal fencing/cancellation ends the writer
and recovery determines its durable state. An append-entry cancellation can
leave ACKs for the next sync, so publication skips already covered messages
without replaying Create/Drop or regressing previously granted scoped frontiers.
ACKs are retained until WAB delivery completes, and scoped sync waiters are
completed only after that delivery.

Both WAL and WAB consumers therefore receive the confirmation only after query
MVCC publication. Flush/DataView effects and View readiness cannot overtake the
query targets that enable them. RecoveryStorage needs no per-message publication
wait. The selected View is acquired before sampling; later publications cannot
replace it inside the attempt. Existing Views can reuse an adequate prefix
without waiting for an unrelated in-flight sync; a newly granted target waits
only for the runtime's existing data-visibility checks. Fixed snapshots retain
the same complete-prefix clipping rule.

For either frontier X, if X < B <= P, the complete prefix contains no relevant
event in (X,B], so reading at X is equivalent to reading at B. If X >= B, the
executor reads at B and filters out later row mutations. Delete advances both
frontiers; insert-only transactions advance Growing at Commit, while mixed
transactions advance both at Commit. Flush/import publication advances Transform,
so sealed membership cannot be used to treat newly published rows as unrelated
to that frontier. This argument assumes the acquired View's existing resource
and history guarantees; it does not introduce historical DDL or extend retention.

A clipped pair is an execution bound, not necessarily the exact maxima of
historical events at B. Subsequent prefixes or a recovered owner may produce a
different pair for the same B; both must remain logically equivalent to B.
Iterator continuation retains the original scalar B, never max of the optimized
pair. Explicit QueryPlanMVCC and retained requery still preserve their exact pair.

Explicit MVCC and BarrierMVCC are mutually exclusive. A barrier is useful for
Iterator snapshot continuation; an ACK published later than B does not change
the query's snapshot. Master supplies scoped barrier production and its ordered
TransformLog/query-resource delivery. If the complete prefix does not cover B,
planning first waits for `TSOAllocator.BarrierUntil` under the caller and WAL-owner
contexts, then calls `TimeTickSyncInspector.EnsureBarrier`. An unallocated future
timestamp cannot occupy shared TimeTick sync work. The inspector certifies
publication; planning separately waits for the selected runtime to apply the
required frontiers before executing. This slow path retains the conservative
(B,B) execution pair after establishing coverage.
Proxy Iterator uses this scalar barrier for snapshot continuation. Historical
retention is unchanged.

Internal partial-update CAS reads use the ordinary Strong snapshot. Their
channel ReadTs is max(GrowingMVCC, TransformMVCC): both frontiers come from one
complete prefix and there are no later query-visible channel changes inside
that prefix. No extra certificate or requirement that ReadTs reach BeginTs is
needed. The write path still checks conflicts and retained-history bounds;
old reads outside the history window fail instead of bypassing validation.
The client retains successful shard snapshots even for empty results.

DropCollection removes the channel when its complete prefix is published.
Pending bounds covered by that prefix are discarded. Scoped barriers update
only existing channels and cannot revive a dropped channel. A Strong waiter
covering Drop returns a channel-unavailable error; an unpublished Create waits
for its prefix. MVCC retirement does not replace View admission or resource
lifetime management.

## Exact read resources and shared physical resources

`queryresource.ReadRuntime` borrows the resources owned by its QueryView. Read-only
CCollections share `(CollectionID, SchemaVersion)` identity; the View retains its
load scope independently. LoadInfoVersion is an identity, not an ordered revision.
Existing View Acquire/Release controls schema, function and field-IDF guards.
Withdrawal cancels work and waits for in-flight preparation/planning before releasing
the guards. A serving lease protects the enclosing QueryRuntime; growing
handles also retain their selected schema through execution and result conversion.
The physical growing runtime and ordered WAL subscription remain shared.

The no-gap WAL snapshot exposes both `Schema` (the requested read version) and
`WriteSchema` (the WAL's write version). New physical growing segments capture
their encoding schema; subsequent schema changes must not reinterpret buffered
inserts under the current write schema. Shared per-field BM25 aggregates are described
in [IDF runtime](../snview/idf_oracle_runtime.md). Resources with no remaining View
owner are reclaimed even while other Views keep the QueryRuntime alive; native
handles independently retain their selected schema until execution finishes.

A field analyzer request may carry optional `read_schema_version`. Presence,
including zero, selects the exact schema and a call-owned function scope, released
when analysis finishes. Absence retains the existing WAL function-runner path.
The handler clones and forwards the optional field across assignment retries;
Proxy highlighting is not switched by this extraction.

## Payload ownership and wire compatibility

Phase 1 can omit unchanged serialized plans/placeholders using
`GetQueryPlanResponse.client_optimize_info.reused_payloads`.
`QueryPlanClientOptimizeInfo` also carries optional `SearchOptimizationInfo` for
client-side filter-first ANN tuning. Both are response metadata, outside the
reusable QueryPlan and never forwarded to workers. Payload restoration clears
only the reuse markers and preserves the search optimization inputs.
The client restores only explicitly marked fields from its original immutable
request. Optimized, replaced or deliberately cleared fields remain authoritative.
Hybrid sub-request count/index checks prevent accidental cross-request reuse.
Restoration occurs before per-node cloning and dispatch.

The added fields are `GetQueryPlanResponse.client_optimize_info` and
`StreamingFieldAnalyzer.read_schema_version`.
This master-based extraction also synchronizes the query-facing schema contracts:
`QueryViewMeta.schema_version`, Search/RetrieveRequest.schema_version,
required_fields in GetQueryViewLoadInfoResponse. Dynamic-field preloading hints
are already expressed by load_fields, without a separate skip boolean in this
response. It does not add their
Coordinator or QN/native implementations. The BarrierMVCC input and scoped WAL
barrier implementation are already provided by master. The unused, unimplemented
RequeryOnView RPC and its messages are removed; retained requery uses QueryOnView.
DataView fields are preserved.
Optional fields do not prove that an older peer enforces the new semantics:
activation and mixed-version acceptance require integration validation.

## Key packages

- `internal/streamingnode/server/wal/adaptor/{query_plan,analyzer}.go`
- `internal/streamingnode/server/wal/snview/{handler,query_lease,query_task_provider}.go`
- `internal/streamingnode/server/wal/vchannel/queryresource/read_schema.go`
- `internal/streamingnode/server/wal/vchannel/{schema,query_resource_module}.go`
- `internal/streamingnode/server/wal/interceptors/timetick/mvcc/query_mvcc.go`
- `internal/streamingnode/server/wal/interceptors/timetick/timetick_sync_operator.go`

## Physical statistics selection

A `statistics_request` selects the latest Up View independently of a logical
SchemaVersion. It reuses the outer `barrier_mvcc` and serving lease, counts
growing resources before renewing the lease, and returns QN workers in the plan.
The growing count is returned separately in
`GetQueryPlanResponse.growing_inline_response.statistics_response`, including
an explicit response for zero rows. QueryPlan itself contains no inline result.
Resolved partition scope is echoed in `statistics_request`. This operation does
not run query optimizers or use the barrier timestamp to filter historical rows. See [runtime statistics](query_client.md#runtime-row-statistics).

## Transaction and restore publication

Successful transaction bodies record whether Transform consumption is required
before releasing their in-flight slots. Recovery reconstructs the same
classification. At a complete ACK prefix, an insert-only commit advances only
GrowingMVCC; a commit containing Delete advances both frontiers. ACK details
remain available until WAB.Append and QueryMVCC publication finish. Empty
transactions retain their existing acceptance behavior and conservative dual
frontier projection.

A published RestoreSnapshot, like CreateCollection, initializes both frontiers
for its channel. Observing its append alone cannot register a query snapshot;
late append callbacks cannot overwrite newer published progress. The full
RestoreSnapshot creation workflow remains separate production integration.
