# QueryView plan, schema and read boundaries

## Phase 1 binding

The WAL adaptor selects the highest Up QueryView whose SchemaVersion exactly
matches the legacy request. Version zero is an exact schema identity too.
It never substitutes the latest schema. For an unknown replica ID, the handler
locates the local shard by vchannel and returns its real identity in the plan.
Matching UpRecovering state waits for local recovery; missing or unusable views
return the existing View errors.

Planning holds a serving lease, fixes one MVCC, and waits for the runtime to
observe its Growing and Transform frontiers before checking field-removal
history. It then resolves the View's loaded partitions, optimizes against the
exact read runtime, and validates all referenced fields, including fields needed
by later stages. `QuerySchemaBinding` ties schema version and field IDs to these
same two frontiers. Phase 2 requires this binding, including for schema version
zero and empty results, and validates it when acquiring tasks.
The lease is renewed just before a successful plan return.

## Three distinct timestamp contracts

| Input | Meaning | Retry behavior |
| --- | --- | --- |
| Consistency level | Sample query frontiers from the primary WAL | May sample again when replanning |
| Explicit `QueryPlanMVCC` | Caller fixes Growing and Transform frontiers | Clone and preserve both frontiers |
| `BarrierMVCC=B` | Establish WAL coverage for a scalar snapshot B | Re-establish coverage if needed, execute Growing=B and Transform=B |

Explicit MVCC and BarrierMVCC are mutually exclusive. A barrier is useful for
Iterator snapshot continuation; an ACK published later than B does not change
the query's snapshot. The client consumes the `QueryBarrierInspector` capability; scoped barrier
production and its TransformLog integration remain separate work. If the WAL
frontiers do not already cover B and the inspector lacks that capability,
planning returns ServiceNotReady. It never treats missing coverage as success.
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
`GetQueryPlanRequest.read_proof_after` and `required_field_ids`,
`QueryPlan.reused_payloads`, and `StreamingFieldAnalyzer.read_schema_version`.
This master-based extraction also synchronizes the query-facing schema contracts:
`QueryViewMeta.schema_version`, Search/RetrieveRequest.schema_version,
QuerySchemaBinding, the BarrierMVCC input, and required_fields /
skip_load_dynamic_field in GetQueryViewLoadInfoResponse. It does not add their
Coordinator or QN/native implementations. The unused, unimplemented
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
