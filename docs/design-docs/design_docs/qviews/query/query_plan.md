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
| Consistency level | Sample query frontiers from the primary WAL | May sample again when replanning |
| Explicit `QueryPlanMVCC` | Caller fixes Growing and Transform frontiers | Preserve both frontiers |
| `BarrierMVCC=B` | Establish WAL coverage for a scalar snapshot B | Re-establish coverage if needed, execute Growing=B and Transform=B |

Explicit MVCC and BarrierMVCC are mutually exclusive. A barrier is useful for
Iterator snapshot continuation; an ACK published later than B does not change
the query's snapshot. Master supplies scoped barrier production and its ordered
TransformLog/query-resource delivery. If existing frontiers do not cover B,
planning first waits for `TSOAllocator.BarrierUntil` under the caller and WAL-owner
contexts, then calls `TimeTickSyncInspector.EnsureBarrier`. An unallocated future
timestamp cannot occupy shared TimeTick sync work. The inspector certifies
publication; planning separately waits for the selected runtime to apply the
required frontiers before executing.
This extraction does not enable the Proxy Iterator path or extend historical
retention.

`ReadProofAfter` is a separate, consistency-based CAS read contract. SN waits for
a complete published ACK prefix at least that new, then atomically samples that
prefix's Growing/Transform frontiers and `ReadProofTimetick`. The proof is not
`max(Growing, Transform)`, and query consumers do not wait to apply the proof
timestamp itself. Idle confirmations can refresh the proof while both executor
frontiers remain unchanged. Explicit MVCC/barrier requests cannot request this
certificate.

The sync operator publishes read-proof state after WAB publication. Successful
ACK messages are projected in order; transaction bodies do not advance visible
frontiers before commit. Append-time query frontiers and proof frontiers remain
separate, so a later acknowledged write cannot contaminate an older certified
prefix. Recovery initializes frontier state but cannot inherit the previous
WAL owner's volatile certificate. The client retains each successful shard's
proof, including empty shards; integration callers must validate those proofs
before using the read for CAS.

DropCollection removes the VChannel's query frontiers and pending-confirmation
marker. Only CreateCollection and recovery initialization register channels;
late scoped barriers and ordinary progress updates cannot recreate them.
The read-proof projection applies the same retirement when its published ACK
prefix reaches Drop, preserving certificates for earlier prefixes. Proof waits
fail when the channel is absent from both projections; a live channel whose
Create has not yet reached the proof projection still waits for publication.
This retires MVCC bookkeeping, while View admission and resource lifetimes
remain governed by their own lifecycle.

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

The added fields are `QueryPlanMVCC.read_proof_timetick`,
`GetQueryPlanRequest.read_proof_after`,
`QueryPlan.reused_payloads`, and `StreamingFieldAnalyzer.read_schema_version`.
This master-based extraction also synchronizes the query-facing schema contracts:
`QueryViewMeta.schema_version`, Search/RetrieveRequest.schema_version,
required_fields /
skip_load_dynamic_field in GetQueryViewLoadInfoResponse. It does not add their
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
- `internal/streamingnode/server/wal/interceptors/timetick/mvcc/read_proof.go`
- `internal/streamingnode/server/wal/interceptors/timetick/timetick_sync_operator.go`
