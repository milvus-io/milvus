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
| `BarrierMVCC=B` | Establish WAL coverage for a scalar snapshot B | Re-establish coverage if needed, execute Growing=B and Transform=B |

`QueryMVCCManager` owns one executable frontier map, the complete published
prefix position, and pending freshness bounds. Successful append callbacks only
raise the pending bounds. After WAB publication, the serialized sync operator
applies successful ACK messages in TimeTick order and atomically publishes both
frontiers with the prefix position. A pair is never assembled from append-time
maxima sampled at different stages.

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

Explicit MVCC and BarrierMVCC are mutually exclusive. A barrier is useful for
Iterator snapshot continuation; an ACK published later than B does not change
the query's snapshot. Master supplies scoped barrier production and its ordered
TransformLog/query-resource delivery. If existing frontiers do not cover B,
planning first waits for `TSOAllocator.BarrierUntil` under the caller and WAL-owner
contexts, then calls `TimeTickSyncInspector.EnsureBarrier`. An unallocated future
timestamp cannot occupy shared TimeTick sync work. The inspector certifies
publication; planning separately waits for the selected runtime to apply the
required frontiers before executing.
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

`queryresource.ReadRuntime` binds resources by
`ReadSchemaKey{SchemaVersion, LoadInfoVersion}`. LoadInfoVersion is an identity,
not an ordered revision. The read-schema module owns the immutable CCollection
and function scope. A serving lease protects the enclosing QueryRuntime; growing
handles also retain their selected schema through execution and result conversion.
The physical growing runtime and ordered WAL subscription remain shared.

The no-gap WAL snapshot exposes both `Schema` (the requested read version) and
`WriteSchema` (the WAL's write version). New physical growing segments capture
their encoding schema; subsequent schema changes must not reinterpret buffered
inserts under the current write schema. Per-schema BM25 aggregates are described
in [IDF runtime](../snview/idf_oracle_runtime.md). Read-schema resources currently
remain until runtime close; early schema-history reclamation is deferred.

A field analyzer request may carry optional `read_schema_version`. Presence,
including zero, selects the exact schema and a call-owned function scope, released
when analysis finishes. Absence retains the existing WAL function-runner path.
The handler clones and forwards the optional field across assignment retries;
Proxy highlighting is not switched by this extraction.

## Payload ownership and wire compatibility

Phase 1 can omit unchanged serialized plans/placeholders using `reused_payloads`.
The client restores only explicitly marked fields from its original immutable
request. Optimized, replaced or deliberately cleared fields remain authoritative.
Hybrid sub-request count/index checks prevent accidental cross-request reuse.
Restoration occurs before per-node cloning and dispatch.

The added fields are `QueryPlan.reused_payloads` and
`StreamingFieldAnalyzer.read_schema_version`.
This combined baseline synchronizes the query-facing schema contracts:
`QueryViewMeta.schema_version`, Search/RetrieveRequest.schema_version, and
explicit required_fields in GetQueryViewLoadInfoResponse. The coordinator and
QueryNode/native implementations come from their corresponding integration
commits; the current load-info response has no skip_load_dynamic_field field. The BarrierMVCC input and scoped WAL
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
