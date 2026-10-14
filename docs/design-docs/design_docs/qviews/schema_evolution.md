# AlterCollection: Versioned Schema and QueryView Handoff

This document defines schema publication and QueryView handoff. QueryNode's
SealedSegment resource ownership follows the
[resource preparation design](qnview/querynode_queryview_resource_preparation.md).
StreamingNode/GrowingSegment ownership is outside that resource refactor.

## 1. Decisions and scope

1. WAL establishes the write schema before the read schema is published.
2. Proxy obtains write schemas from StreamingNode. For DQL, Proxy selects the
   published read version and obtains that exact schema from StreamingNode.
3. DataCoord and Coordinator QueryView planning obtain the target schema from
   Broadcaster's durable message/callback workflow, before public publication.
4. Each QueryView binds one immutable read SchemaVersion and one immutable
   LoadInfo snapshot. New views prepare field/index/function requirements while
   retaining the resources required by older serving views.
5. RootCoord publishes the new read schema only after the complete serving
   handoff obligation is satisfied. AlterCollection then completes its callback
   and API. Unloaded collections have no serving handoff obligation.
6. Dropped fields become invalid at each VChannel's WAL ALTER boundary. Keeping
   old resources does not permit post-boundary queries on removed fields.
7. Persist exact schema/load identities for recovery and keep runtime handles
   alive while views/queries use them. All durable-history and storage GC issues
   are deferred to the unified TODO in section 8.2, per the 2026-10-13 decision.

Keep one AlterCollection Broadcast and its existing idempotent callback. Do not
introduce a second schema-publication Broadcast, a separate DDL job state machine,
or recursive broadcasts under the same collection resource lock.

This refactor does not change DataView's W/A transform-frontier design, introduce
L0 query consumption, add arbitrary historical time-travel queries, or couple the
standalone Analyzer API to QueryView/replica discovery.

## 2. Schema roles and identities

| Role | Meaning | Authority / retrieval |
| --- | --- | --- |
| Segment encoding schema | Interpretation of a Segment's stored/logged fields and inserted rows | Segment metadata plus retained SN schema history |
| Current write schema | Schema accepted for subsequent writes on a VChannel | Its current primary SN, after WAL admission switches schema |
| Target read schema | Committed schema for which resources are being prepared | Broadcaster message/callback, projected into Coordinator memory |
| Published read schema | Schema exposed by DescribeCollection and selected for new DQL parsing | RootCoord's durable collection metadata |
| View read schema | Exact immutable schema under which this QueryView serves | QueryView SchemaVersion, resolved from SN history |

Write and target read schema normally refer to the same new schema. Public read
schema can lag while resources prepare. Segment encoding versions can be older
than either; loading a Segment must not reinterpret its stored rows as if they
were encoded by the latest schema.

SchemaVersion identifies an immutable query-relevant schema payload within a
CollectionID. It is not a DataVersion, Manifest version, index build version or
LoadInfoVersion. LoadInfoVersion is a content identity and supports equality,
not ordering. CollectionID distinguishes drop/recreate under the same name.

All query-affecting schema mutations, including TTL settings, must advance the
schema identity. Repeating an identical operation does not create a new version.
Legacy same-version mutations require an explicit compatibility mapping during
recovery; never silently give one exact-version lookup two different payloads.
Version zero can be a real legacy version and must not double as a latest selector.

Field and Function IDs retain their lifecycle identity. Drop/re-add, including
dynamic-field disable/re-enable, does not resurrect the old IDs or resources.

## 3. Classifying changes and readiness

Schema changes include additions, removals, and query-semantic changes with an
unchanged column set. A new SchemaVersion alone does not prove physical readiness.

| Change | Required read capability before publication |
| --- | --- |
| Ordinary default/nullable field | Correct value for every historical row; a supported virtual/default projection can satisfy this without physical rewrite |
| StructArray | Correct nullable/default projection and consistent child fields, offsets, validity and row alignment |
| Enable dynamic field | New `$meta` FieldID and the defined empty-object behavior for historical rows |
| BM25 / MinHash Function and outputs | Correct historical derived outputs, required statistics and the indexes required by LoadInfo; missing derived values are not ordinary NULL/default values |
| Drop field / StructArray / Function outputs / dynamic field | New logical schema/load requirements exclude them; retained views still own their old resources |
| TTL or other query-runtime semantic change | New immutable runtime semantics, even without physical field changes |
| Metadata-only change | Correct schema metadata; reuse resources if their requirements did not change |

Materialization continues deciding from the target schema versus **actual
physical fields**, not merely comparing Segment SchemaVersion:

- Missing columns without physical drops: reconcile on the same SegmentID and
  advance Manifest as required.
- Dropped physical columns: full rewrite to a new SegmentID; it can also apply
  deletes/TTL. Old views continue to reference their original bases; durable
  object protection against GC is part of the deferred work in section 8.2.
- No physical differences: update schema metadata without inventing a Manifest
  change or requiring data I/O.

Read publication does not universally wait for physical compaction. It waits
for the capabilities above. In particular, logical removal need not wait for
physical column deletion, whereas a new Function cannot publish before its
required historical outputs are usable.

The admission path checks whether required materialization is supported for the
collection. Schema-bump disabled, non-V3/no-Manifest data or unsupported external
storage must have a supported alternative or be rejected before committing an
unsatisfiable operation. Transient compaction protection, node loss or capacity
shortage can delay an otherwise supported operation; timeout is not rollback.

Completeness includes every relevant row represented by the selected DataView
and SN growing/retained-segment membership. Never omit an unprepared Segment to
make a view appear Ready. Flush/final commit, Import and Compaction racing with
preparation must preserve that membership and capability proof. If the captured
DataView cannot satisfy a requirement, replan against the updated DataView rather
than waiting forever or assigning it untrue resource readiness.

## 4. Schema acquisition and metadata ownership

### 4.1 Proxy and worker access through StreamingNode

Provide a generic streaming schema client, exposed through
`streaming.WAL().SchemaClient()`, following the existing AnalyzerClient accessor
style. Reuse HandlerClient's assignment discovery, connection pool and native
streaming error transport. Do not add a QueryCoord routing hop or an embedded
response error code.

The conceptual operations are:

```text
GetWriteSchema(VChannel) -> Schema, SchemaVersion, effective TimeTick
GetSchema(VChannel, ExactSchemaVersion) -> Schema, effective TimeTick
```

These are new contracts, not descriptions of existing public methods. Exact
proto/method names are implementation details. Exact lookup uses persisted
VChannel schema history; write lookup returns the active write schema. Read/write
selectors are explicit. Not-yet-observed versions retry; retired unsupported
versions are distinguishable. Neither case silently returns latest.

Proxy still resolves names/aliases, CollectionID and VChannels through Coord.
Schema reads route to the current primary owning the PChannel and do not require
the collection to be loaded. SN/QN preparing a QueryView request its exact version
and pin its lifetime, not a version inferred from DescribeCollection.

Schema bodies can be cached by `(CollectionID, SchemaVersion)`. Proxy maintains
separate write-version and published-read-version selections. Refreshing the
write schema must not replace the schema selected for DQL or public discovery.
Existing cache fill/invalidation ordering must continue preventing an older
in-flight fill from overwriting a newer publication.

One request pins its selected schema through validation, encoding or execution.
Write lookup is not atomic with WAL append: preserve exact write admission
checks, refresh and revalidate after mismatch, and retain existing idempotency /
partial-success semantics. Do not blindly replay already accepted writes.
Broadcast copies reach VChannels at different times; one primary's latest schema
does not prove that every write destination already accepts it.

### 4.2 DataCoord and QueryView access through Broadcaster

The full target schema is already business content of the durable ALTER message.
The existing ACK callback receives that message and per-VChannel append results
after all required ACKs. Extend this workflow to publish one immutable,
collection-scoped target snapshot for DataCoord and QueryView planning:

```text
CollectionID, BroadcastID, TargetSchemaVersion, TargetSchema,
business-VChannel -> ALTER TimeTick, field/index requirement delta
```

This is a shared in-process projection of Broadcaster-owned durable intent. It
is exposed through the Coordinator's package-level resource infrastructure, not
through a QueryCoord RPC or another collection-lock acquisition. The callback
installs it before refreshing DataCoord/index planning and triggering QueryView
planning. Readers obtain one immutable target, not independently sampled schema
and version. They do not fall back to an older public schema while a target is
active. Collection snapshots passed to compaction/index jobs remain immutable.

Recovery reconstructs this projection from eligible pending broadcast tasks in
the existing callback order. A merely persisted, partially appended task is not
yet a preparation/publication authorization. Affected planning waits until its
target can be reconstructed; absence during startup must not mark recovered
views obsolete. CDC/recovered tasks follow existing callback serialization too;
do not select the numerically largest pending task without checking that order.

After read publication, RootCoord's durable published snapshot supplies the
steady-state schema for Coordinator planning. Remove the pending projection only
after that snapshot durably covers the target. Thus completed Broadcast tasks can
still become tombstones and be GC'd normally. Broadcaster is **not** a permanent
historical-schema database; historical worker reads remain SN-owned.

RootCoord need not serve two Schema bodies publicly. Its existing metadata
publication must be split from the early internal side effects currently coupled
to `MetaTable.AlterCollection` and `BroadcastAlteredCollection`. The callback
must preserve rename/property/index side effects and idempotency while leaving
the public schema old until publication. Non-schema ALTER paths retain their
existing completion contract.

### 4.3 LoadConfig and retained LoadInfo

LoadConfig expresses the desired partition/field/index/replica set. Schema
identity remains separate; a semantic-only change can require handoff with an
unchanged LoadInfoVersion. New fields follow the collection's loading intent:
full-load and explicit partial-load behavior must be distinguished. Required
Function input/output dependencies are resolved without adding unrelated fields.

Each view pins the immutable LoadInfo field/partition scope it was created with.
Its IndexID is always zero, meaning the latest available index; index descriptors
are resolved from current metadata and are not historical View requirements. GetQueryViewLoadInfo must resolve a retained
exact identity, not require that it equal the latest configuration. Persist
versioned load snapshots with view-owned references; the latest desired config
remains the LoadConfigStore's responsibility. Schema is fetched separately and
is not added to the GetQueryViewLoadInfo response.

The preparation identity includes SchemaVersion, LoadInfoVersion, load lifecycle,
DataView membership, and required Segment content/Manifest revisions. Config
updates and release/reload are checked against that identity before publication;
a content hash alone is not an incarnation fence.

## 5. AlterCollection event flow

```mermaid
sequenceDiagram
    participant P as Proxy
    participant C as Coord callback / publication
    participant B as Broadcaster
    participant S as StreamingNode
    participant D as DataCoord
    participant V as QueryView planner
    participant Q as QueryNode
    P->>C: AlterCollection
    C->>B: One broadcast with stable target schema
    B->>S: ALTER on each VChannel
    S->>S: Fence old allocation; switch write schema; retain history
    P->>S: Fetch current write schema for DML
    B->>C: Ordered ACK callback with schema and channel boundaries
    C->>C: Install immutable target from Broadcaster
    C->>D: Expose target schema and bound index requirements
    C->>V: Apply desired load delta; plan target schema
    D-->>V: Publish materialized Segment / DataView revisions
    V->>S: Prepare target QueryView
    V->>Q: Prepare target QueryView
    Q->>S: Get exact schema and acquire retained version
    S-->>V: Resources ready; target Up protocol
    Q-->>V: Required resources Ready
    V-->>C: Complete expected-shard target Up coverage
    C->>C: Durably publish read schema
    C-->>P: Invalidate read cache; complete ALTER
    C->>V: Release superseded schema serving obligation
    V->>V: Drain old views / leases / resources
```

The sequence is a dependency graph; DataCoord work and SN/QN preparation can
overlap. Target Up uses the existing distributed Ready/Up protocol, including
its persistence and reports. WAL consumers must not wait for QueryView handoff.
Their ordered Flush, schema, data and visibility events are prerequisites for it.

The callback executes these idempotent stages:

1. Install the target schema from the ACKed broadcast and retain its dependencies.
2. Expose it to DataCoord before publishing bound index metadata; apply the load
   delta through the existing owner without recursively broadcasting.
3. Trigger preparation and wait for the complete target serving obligation.
4. Atomically persist the new public read metadata/version under the existing
   collection serialization; prevent stale callback replay from regressing it.
5. Invalidate read caches, notify view lifecycle publication, and finish. If
   publication succeeded before a crash, replay finishes these side effects
   without reverting metadata or starting another materialization operation.

The target obligation is all metadata VChannels crossed with the current desired
replicas, including shards not yet in the registry. Each must have an Up view
with the exact target schema and matching load lifecycle/LoadInfo, backed by the
capabilities in section 3. Resource shortages do not reduce the denominator or
produce successful publication. Recheck configuration identity around observation.

With no LoadConfig, publish the schema without implicitly loading the collection.
Release removes a serving obligation only when it actually commits; a release
queued behind the same broadcast resource lock cannot unblock the callback
holding that lock. Explicit administrative interruption needs its own existing
control path, not a recursive release from this callback. Concurrent reloads
cannot satisfy an earlier obligation using stale views.

## 6. Query routing, activation and removal boundaries

### 6.1 Exact logical schema and old serving coverage

Proxy binds a DQL attempt to the published read schema. Phase 1 selects the
latest eligible Up view **within that SchemaVersion**. A greater view schema is
not a substitute. Phase 2 checks the request/plan/view schema agreement and uses
the pinned runtime resources. Physical encoding schemas may differ safely.

New target Up views do not automatically Down the only Up coverage for the
still-published old schema. Retain published and target serving coverage while
the callback is pending. After durable publication, stop choosing the retired
schema for fresh plans and request ordinary Down/draining; in-flight plans use
existing leases and concrete task references. A stale-cache request can refresh
and replan, but never silently cross schema versions within one execution.

Reuse existing lifecycle states; change the manager's single-Up assumptions,
schema-aware selectors, statistics and retention decisions. Same-schema view
replacement keeps normal handoff. Recovery restores published coverage and target
coverage before resuming their retirement decisions. Preparing target work must
not starve recovery or necessary data advancement for the published schema.

Multiple schema generations may remain draining under existing lease renewal;
this design does not promise a fixed two-copy memory bound or introduce a hard
request-duration cap. Accounting distinguishes logical serving obligations from
physical sharing. User load-status APIs retain their complete expected-shard and
current desired-config checks, additionally qualifying the selected schema;
a pending load/schema change can remain Loading. The ALTER waiter checks its
target, while DQL selects the published read schema. Reuse a parameterized
readiness predicate for explicit load and AutoLoad; AutoLoad must not make DQL
parse an unpublished target. No check may use an unqualified latest-Up pointer.

One request keeps its schema through every shard, hybrid subquery, rerank,
requery and highlight. Same-schema view retries can remain shard-local. Schema
refresh requires a fresh whole-request plan/result accumulation. The current
legacy requery adapter must propagate this binding even though it issues another
Query operation. Iterators/cursors must retain their binding or fail explicitly
when it can no longer be served; this is not indefinite time-travel support.

### 6.2 Dropped-field validity

For a removed FieldID with ALTER TimeTick `T_v` on VChannel `v`, and a query that
references it with Transform MVCC `M_v`:

```text
M_v < T_v  -> may execute under a retained compatible old view
M_v >= T_v -> reject use of the removed field
```

Referenced fields include predicates, output, grouping, ranking, function inputs
and rendering dependencies. The rule uses each business channel's own boundary,
not the CChannel boundary or a maximum across channels. Removing an entire
StructArray or Function includes its affected child/output dependencies.

SN planning validates the field dependency set and fixed MVCC against ordered
schema history observed through the requested boundary. Sampling write MVCC
ahead of recovery's schema observation is not sufficient: wait for coverage or
retry before issuing a plan. Acquire a consistent boundary snapshot or recheck
its generation. The resulting plan binds the validated schema, MVCC and field
scope; workers cannot change them or append unvalidated field accesses during
execution. Phase 2 enforces these bindings, including supplied removal bounds.
Requery/rendering that needs additional fields must have them included in the
original validation scope or undergo schema-bound validation before execution.

A plan created before a drop with an earlier MVCC does not become invalid merely
because the drop is later observed. A stale plan cannot be reused with a newer
MVCC. This contract can use schema history and validated plans; it does not
assume that today's Delete-only TransformLog carries a schema payload.

Strong queries never lower MVCC to evade a removal boundary. During a pending
ALTER, the old public schema can therefore coexist with rejection of new Strong
queries referencing a field already dropped in WAL. This is intentional and
preserves the previously agreed drop semantics. Unaffected-field queries may
continue when their old-schema projection and resources are valid.

## 7. Column resources and physical lifetime

A view owns immutable read requirements and resource handles. Column-level
preparation is a logical unit; object I/O can still load column groups. Resource
identity includes SegmentID, FieldID, content/base revision and applicable index
or Function identity. SegmentID alone is insufficient for reuse.

For QN:

- CCollection owns immutable Schema by `(CollectionID, SchemaVersion)`. A View
  owns independent immutable LoadScope; load fields never mutate that Schema.
- Sealed Segment APIs borrow `const Schema*` from an operation that pins the
  owning CCollection. Segment and physical ReadState do not retain a current
  logical schema. The operation binds Schema, LoadScope, ReadState and MVCC.
- Preparation publishes immutable physical resource generations and independent
  field/group metadata. Reuse unchanged columns/readers; same-ID Manifest changes
  prepare affected resources before readiness. Queries do not perform lazy schema
  mutation. Physical compatibility is not inferred from schema version order.
- Old and new Views can read the same physical Segment with different logical
  schemas and scopes. Drop removes the new View's references; executing handles
  and older Views retain their own resources through actual completion, including
  asynchronous work, result reduction and field refill.
- Readers, Translators and cached preparation results own physical metadata and
  cannot implicitly borrow a retired Schema. Supported default projections are
  distinct from Function outputs requiring materialization.
- IndexID remains zero/latest-compatible, with execution-time resource pins.
  Durable object GC remains deferred to section 8.2. The complete QN ownership
  and execution contract is [QN resource preparation](qnview/querynode_queryview_resource_preparation.md).

For SN:

- Keep ordered WAL observation and shared data ownership. Decode/materialize
  inserts using their Segment write schema; expose an explicit view read schema
  over growing and retained flushed resources.
- Initialize each immutable native planning Collection with its logical schema
  version, including collections recreated after recovery. Parsing field metadata
  alone leaves the native version at zero and prevents `LazyCheckSchema` from
  filling added columns in older growing segments. Segment encoding Collections
  keep their own versions; queries do not replace their Go encoding schemas.
- New growing segments can use the new write schema while published-old-schema
  requests still run. Project compatible fields; apply the removal rule above.
- Function runners, analyzers, TTL settings and other query-runtime inputs bind
  to the selected read schema. Updating the WAL's latest runner key must not
  silently change old-schema query semantics.
- BM25 can retain compatible current aggregates by Function/field semantics;
  do not introduce historical statistics for every DataVersion or MVCC. A schema
  change must not destroy an aggregate still required by retained views. Each
  plan captures one complete compatible optimizer result.

Runtime handles also retain acquired analyzers and index resources. Durable
FileResource/index/object GC is deferred to section 8.2.
Standalone analyzer discovery remains the generic streaming path, independent
of collection load state; query/highlight can supply the selected schema version.

## 8. Recovery and deferred GC

### 8.1 Recovery in this refactor

SN already persists schema snapshots and their TimeTicks. This refactor adds
exact access, persists the schema/load identities required to rebuild views,
and restores available snapshots and runtime references. It does not implement
new GC policies, cross-node GC pins or storage-deletion fencing.

Coordinator recovery order:

1. Recover published collection metadata, Broadcaster tasks and desired config.
2. Reconstruct eligible target projections from ordered pending intent, without
   waiting synchronously for the handoff they will drive.
3. Recover QueryViews and resolve exact DataView/LoadInfo/schema dependencies;
   hold affected planning until those dependencies are installed.
4. Resume worker synchronization, materialization, planning and callback retries.
5. Reconcile publication and ordinary view/runtime releases.

SN failover restores available schema history and view requirements before
accepting historical-schema acquisition. Worker recovery
cannot replace an old view's load snapshot with latest configuration. Missing or
corrupt retained resources fail/retry through the appropriate owner rather than
fabricating readiness.

| Crash / race | Required result |
| --- | --- |
| Task persisted, some channels not appended | Continue the same broadcast; do not publish or treat it as globally ready |
| WAL changed, callback not yet applied | Writes use each SN's admitted schema; read publication stays old; callback restores target |
| Some target resources/Up views ready | Keep published-old coverage; resume missing work |
| Target Up everywhere, before read publication | Recover target and old obligation; safely retry publication |
| Read metadata persisted, callback not done | Keep published version; replay cache/lifecycle side effects idempotently |
| Completed task is no longer present | Published metadata supplies steady-state target; available SN/View metadata supplies history |
| Schema lookup races SN migration | Follow assignment/reacquire the exact supported version; never substitute latest |
| Client timeout after durable task creation | Operation may continue; no automatic WAL/schema rollback |
| Release/reload or config change | Re-evaluate obligation with lifecycle/config identity; old readiness cannot satisfy new load |

### 8.2 TODO: unified GC design (deferred)

All GC questions are deferred and are not implementation prerequisites of this
refactor. Do not introduce new schema/LoadInfo/Manifest/FileResource deletion or
retention algorithms in this work. Runtime reference counting and delayed
release of resources used by executing queries remain in scope: they are needed
for handoff even without persistent-storage GC.

The future unified design must cover:

- Schema history required by published read versions, pending ALTER operations,
  live/recovering views, and Segment encoding; the existing replay-floor and
  Segment-only schema GC does not describe all these consumers.
- The interval between WAL schema observation and Coordinator view registration,
  and between exact lookup and resource acquisition.
- Historical LoadInfo snapshots, index descriptors, old Manifest/data bases,
  Function/analyzer FileResources and their dependent objects.
- Consistent protection across Coord/SN/QN recovery, pchannel ownership changes,
  outstanding readers, delayed notifications and stale-owner deletion.
- When Broadcast task/tombstone cleanup and view/reference cleanup may release
  their durable dependencies, without turning Broadcaster into a permanent
  schema archive.
- Reclamation after the last owner, failure/retry behavior and GC race tests.

No particular distributed pin protocol, retention floor, grace period or GC
startup ordering is selected here. Historical recovery correctness below assumes
the referenced history still exists. Missing history must fail explicitly rather
than substitute latest schema or fabricate empty data; end-to-end correctness
under concurrent GC remains part of this TODO.

## 9. Correctness argument

**Write correctness:** WAL admission validates the schema used to encode the
request; Segment allocation is fenced at schema changes. A stale lookup can fail
and revalidate but cannot authorize decoding under an unrelated latest schema.

**Read schema consistency:** each request and every selected view bind the same
logical schema. Published-old coverage remains until target coverage exists.
Global read publication is one durable collection metadata decision; stale
Proxy caches retry rather than execute against an incompatible view. Temporary
node failures still use normal availability/retry behavior.

**Data completeness:** Ready proves actual field/Function/index capability over
complete membership, with Manifest and encoding requirements. A higher schema
number or omitted unprepared Segment cannot provide this proof.

**Removal correctness:** a validated field scope and fixed per-channel MVCC obey
the WAL removal boundary. Physical retention does not extend logical validity,
and delayed public metadata cannot resurrect a removed field past that boundary.

**Recovery correctness, with referenced history available:** pending intent
survives in Broadcaster until callback completion; published read metadata
survives independently afterward. Views record exact schema/load/base
requirements, so recovery does not infer them from latest configuration or need
expired broadcast tombstones. Protection against concurrent GC is deferred to
section 8.2 and is not claimed by this refactor.

## 10. Compatibility and rollout

Preserve existing RPCs/proto fields and new-Coord consumption of old WAL messages.
Add explicit selectors/identities where needed; do not reinterpret a legacy zero
as latest or silently promise exact-history behavior for an ambiguous old payload.
Recovery normalization must preserve existing segment decoding and effective
boundaries, and old pending callbacks must finish idempotently.

Existing QueryCoord compatibility RPCs remain available and delegate to shared
Coordinator QueryView infrastructure. Internal callback wiring does not use
those RPCs as a message bus.

Enable strict schema publication/routing only when participating Proxy, Coord,
SN and QN support its contracts. A mixed deployment must retain its supported
legacy behavior or reject unsupported evolution before broadcast; an old worker
must not acknowledge strict readiness by ignoring new requirements. Public
DescribeCollection continues returning the published read schema and does not
reveal a partially prepared target.

The coordinator checks the registered Proxy/SN/QN schema protocol capability
before admitting a schema-changing ALTER into the WAL. Missing capability in an
old session is unsupported; discovery failures remain retriable and do not
admit the broadcast. Metadata-only ALTER and replay of already persisted
messages do not run this new-admission check.

Each worker sync stream also attests the protocol in its initial gRPC metadata.
The coordinator checks that attestation before accepting the first view response
on every new stream, including reconnects and pchannel migrations. A response
from a worker lacking the protocol cannot satisfy a pending Ready/Up obligation.
This second check covers worker changes after the admission snapshot; it does
not turn session discovery into a cluster-wide upgrade transaction. Deployment
must retain capable Proxy/Coord participants while strict-schema serving is in
use; downgrading them during an admitted evolution is unsupported.

## 11. Implementation sequence and validation

Implement dependencies in this order without enabling the final protocol early:

1. Exact SN schema access, version/cache identities and snapshot recovery.
2. Broadcaster-derived target projection and separation of internal side effects
   from public RootCoord publication; recovery/bootstrap wiring.
3. Recoverable immutable LoadInfo plus QN/SN view-bound column/runtime resources.
4. Schema-aware view management, query plan/execution binding, Proxy read/write
   cache separation and retry behavior, including requery/highlight.
5. Materialization capability checks, complete publication fence and callback
   completion; enable only with the preceding dependencies in place.

Required tests include:

| Group | Scenarios and assertions |
| --- | --- |
| Added fields | Ordinary/default/nullable, StructArray and dynamic field on growing and sealed data; every historical row has correct values |
| Functions | BM25/MinHash outputs, statistics and configured indexes; no Ready with absent derived data; old functions still usable by retained valid plans |
| Removal | Ordinary/StructArray/function/dynamic removal and same-name re-add; correct pre/post-boundary behavior |
| Semantic changes | TTL set/switch/remove, Function runtime settings, metadata-only changes; no unnecessary data reload |
| Cross-shard handoff | Pause one shard/replica and between target Up and public publication; no early public schema, mixed-schema reduce or premature old-view retirement |
| Query stages | Phase-1/Phase-2 gap, retries, requery, hybrid search, rerank, highlight and iterators; fixed schema/MVCC and complete field dependency validation |
| Physical content | Same-ID Manifest advancement, new-ID full rewrite and no-diff bump; correct sharing, update and delayed release |
| Concurrent data | Inserts/upserts, automatic/manual flush, delayed final commits, Import, Compaction and snapshots during preparation; no omissions or duplicate handoff |
| Configuration | Partial fields/partitions, unloaded collection, release/reload and replica shortage; correct complete obligation |
| Recovery | Restart each node/Coord at every callback stage, SN migration and completed Broadcast task removal, with referenced schema/load/base history available |
| Failures | Unsupported materialization, transient I/O/capacity failure, timeout and callback replay; no false success, rollback or recursive broadcast deadlock |
| Compatibility | Old messages/data, version zero, legacy same-version updates and mixed participant capability; no silent schema substitution |

Use relevant unit/integration tests plus Python, REST and Go E2E for the affected
public APIs. Record effective schema-bump/storage configuration and failure
injection boundaries. These are future acceptance requirements, not test results
from writing this design.

GC safety/reclamation tests are deferred with section 8.2, not included in this
refactor's acceptance claim.

## 12. Current implementation map

All paths below are relative to the repository root:

| Area | Source / change |
| --- | --- |
| RC ALTER callback and public publication | `internal/rootcoord/ddl_callbacks_alter_collection_properties.go`, `ddl_callbacks_alter_collection_schema.go`, `meta_table.go` |
| Durable target source | `internal/streamingcoord/server/broadcaster/{broadcast_manager,broadcast_task,ack_callback_scheduler}.go` and `registry/` |
| Shared Coord access | `internal/views/coord/resource/resource.go`; add target projection wiring here, outside QueryCoord RPC routing |
| Materialization | `internal/datacoord/compaction_policy_bump_schema_version.go`, `internal/datanode/compactor/bump_schema_version_compactor.go` |
| View generation/publication | `internal/views/coord/{balancer,coordview,loadmgr,loadstatus}/`; replace single latest-Up / schema-lower-bound assumptions |
| Proxy schemas | `internal/proxy/metacache/meta_cache.go`, `internal/proxy/{dml,dql,ddl}/`; separate selection and retain public projection |
| Streaming client | `internal/distributed/streaming/`, `internal/streamingnode/client/handler/`; add SchemaClient using existing discovery |
| WAL write schema | `internal/streamingnode/server/wal/interceptors/shard/shards/shard_manager_collection.go` |
| Historical schema | `internal/streamingnode/server/wal/vchannel/{view,schema_gc}.go`, `internal/metastore/kv/streamingnode/` |
| Query routing/execution | `internal/streamingnode/server/wal/adaptor/query_plan.go`, `internal/views/queryclient/`, SN/QN task providers |
| SN resources | `internal/streamingnode/server/wal/vchannel/{queryresource,growingruntime}/`, `internal/util/function/` |
| QN resources | `internal/querynodev2/{qnview,qvresource,segments}/`; replace shared mutable schema acquisition with view-bound resources |
| Native sharing | `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`, `Collection.cpp`, `SegmentLoadInfo.cpp`; reuse staged publication without assuming existing Go bindings pin history |

Related contracts: [QueryView lifecycle](query_view_state_machine.md),
[shard management](shard_view_management.md), [serving lease](query_view_lease.md),
[QN preparation](qnview/querynode_queryview_resource_preparation.md),
[SN resources](snview/streamingnode_resource_manager.md),
[growing runtime](snview/growing_segment_runtime.md),
[IDF](snview/idf_oracle_runtime.md), [query planning](query/query_plan.md),
[execution](query/query_execution.md), [client](query/query_client.md),
[VChannel recovery](../wal/vchannel_view_module.md), and
[Broadcaster](../../../agent_guides/streaming-system/coordination/broadcaster.md).
