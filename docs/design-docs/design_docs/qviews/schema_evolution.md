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
6. Read-side field validity follows the selected QueryView's exact schema and
   required-field scope. WAL ALTER switches the write schema; it does not revoke
   fields from retained old read Views. Publish the new read schema before
   retiring old-schema serving coverage through normal View/lease draining.
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
| Target read schema | Durable preparation intent, visible before WAL append/ACK completion | Earliest unfinished persisted schema ALTER in Broadcaster |
| Published read schema | Schema exposed by DescribeCollection and selected for new DQL parsing | RootCoord's durable collection metadata |
| View read schema | Exact immutable schema under which this QueryView serves | QueryView SchemaVersion, resolved from SN history |

The target can be visible before any VChannel switches its write schema. Once
applied, write and target read schema refer to the same new schema; public read
schema can still lag while resources prepare. Segment encoding versions can be
older than either; loading a Segment must not reinterpret its stored rows as if
they were encoded by the latest schema.

SchemaVersion identifies an immutable query-relevant schema payload within a
CollectionID. It is not a DataVersion, Manifest version, index build version or
LoadInfoVersion. LoadInfoVersion is allocated durably and monotonically; compatibility still
requires exact requirements, not merely version ordering. CollectionID distinguishes drop/recreate under the same name.

All query-affecting schema mutations, including TTL settings, must advance the
schema identity. Repeating an identical operation does not create a new version.
Legacy same-version mutations require an explicit compatibility mapping during
recovery; never silently give one exact-version lookup two different payloads.
Version zero can be a real legacy version and must not double as a latest selector.

Field and Function IDs retain their lifecycle identity. Drop/re-add, including
dynamic-field disable/re-enable, does not resurrect the old IDs or resources.

## 3. DataView-owned read schema

Every immutable DataView carries its supported `SchemaVersion`. QueryView copies
that identity from its selected DataView; Broadcaster's pending schema is a target
for data preparation, never permission to bind an arbitrary physical snapshot to
that schema. Successive DataViews inherit the latest schema version and cannot
regress it. Only the schema DDL callback explicitly advances it, publishing one
new CompactVersion even when membership and Manifest versions are unchanged.

There are exactly two schema transitions:

1. **Direct**: deletion, metadata/runtime-only changes, and ordinary additions
   supported by existing NULL/default projection. The callback publishes a new
   DataView with unchanged membership/Manifest and the target SchemaVersion.
   Physical cleanup is independent and need not finish first.
2. **Materialized**: newly added Function outputs require historical computation.
   DataCoord waits until the current complete sealed projection has completed
   target schema-bump, then atomically publishes that projection and the target
   SchemaVersion. Replacements transfer the obligation to their successors; an
   obsolete SegmentID is never a permanent wait target. Recheck under the DataView
   mutation lock before publishing. Wait without holding metadata/mutation locks.

ALTER uses consuming-side `AckSyncUp`. WAL allocation fencing isolates old/new
write schemas; consuming-side Ack additionally waits for pre-ALTER L1 final
publication. Thus the callback cannot overlook old growing data still flushing.
Data preparation uses Broadcaster's pending target and must not await callback
completion or recursively broadcast.

The callback first installs bound index metadata and target load intent. While
DataView's schema trails the acknowledged target, ordinary target planning waits
and existing serving/recovery views remain retained. It then advances DataView
(directly or after materialization), triggers planning, waits for exact target
schema and current load configuration Up coverage, and publishes RootCoord's read
schema. Schema and load intent must both be ready before target planning.

Normal Flush/Compaction/Import publications inherit the committed DataView schema.
They must not introduce data incapable of satisfying that schema. Destructive
replacement results (sort, merge, clustering and schema-bump) cannot commit
before a direct logical schema publication;
old exact views retain their original bases. Schema-only publications survive
recovery and callback replay is idempotent. Version zero is a real schema.

No proto compatibility or legacy DataView-schema inference is provided: this
feature is unreleased. GC of retained physical objects remains TODO.

**TODO — Import/AlterSchema exclusion:** an Import frozen under an older schema
may already be Uncommitted when a Function-adding ALTER completes. The current
CommitImport callback does not revalidate that frozen projection and can still
publish segments missing the new output. The intended solution is mutual
exclusion across the Import and AlterSchema lifecycles, not merely serialization
of their final broadcast callbacks. This exclusion is pending; this change does
not add rejection or materialization stages to Import. The schema capability
proof therefore excludes that concurrent operation until exclusion is enforced.

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

The full target schema is business content of the durable ALTER message.
`GetPendingSchemaTarget(collectionID)` exposes the earliest unfinished persisted
schema ALTER as an immutable collection-scoped snapshot for DataCoord and
QueryView planning. The index is updated after successful task persistence,
before WAL append or ACK completion; it is not installed by the ACK callback:

```text
CollectionID, BroadcastID, Schema (including its Version),
TimeTicks: business-VChannel -> persisted, known ACK TimeTick
```

Missing TimeTicks are unknown; the initial map can be empty. The accessor shares
the immutable Schema and copies the TimeTicks map. Reading a target proves
durable intent, not channel application, completed data preparation or permission
to publish the read schema. Bound index requirements remain in the ALTER message
and are applied by the ordered callback after all required ACKs.

The shared in-process projection is consumed through Coordinator's package-level
`PreparationSchema` accessor, without a QueryCoord RPC or another collection-lock
acquisition. Readers obtain one immutable schema and version. They do not fall
back to an older public schema while a target is active. Collection snapshots
passed to compaction/index jobs remain immutable. Target-aware work may inspect
the pending intent early; DataView certification and read publication retain
their separate callback fences.

Recovery reconstructs the index from persisted unfinished tasks, including tasks
that are only partially appended or ACKed. For each collection the accessor
selects the lowest unfinished BroadcastID, preserving serialized ALTER order,
including replicated tasks. It does not select the newest pending schema or wait
for all ACKs to make the target visible. Recovery must install dependencies before
planning resumes; target visibility alone cannot certify a DataView or obsolete
retained published-schema Views.

After read publication, RootCoord's durable published snapshot supplies the
steady-state schema for Coordinator planning. The pending projection is retired
when callback completion is persisted, after durable read publication. Thus
completed Broadcast tasks can still become tombstones and be GC'd normally.
Broadcaster is **not** a permanent historical-schema database; historical worker
reads remain SN-owned.

RootCoord serves one published Schema body publicly. The implemented callback
applies bound index metadata and calls `Resources.PrepareSchema` before
`MetaTable.AlterCollection` publishes the new metadata. It then calls
`ShardViewRegistry.PublishCollectionSchema`; the outer callback retains other
ALTER side effects and cache invalidation. Replay detects already published
metadata and finishes lifecycle/cache notification without repeating preparation
for a retired schema. Non-schema ALTER paths retain their existing contract.

### 4.3 LoadConfig and retained LoadInfo

LoadConfig expresses the desired partition/field/index/replica set. Schema
identity remains separate; a semantic-only change can require handoff with an
unchanged LoadInfoVersion. `required_fields` is the explicit allowed query scope,
materialized from all target Schema user fields for public Load APIs. ALTER
adds new columns and removes dropped columns, including Function outputs and
StructArray children. Empty means no fields, with no worker-side default expansion.

The independently defined WAL `load_fields` and persisted `preload_fields` carry
the concrete preload subset. External load selection affects only preloads;
Coordinator resolves an omitted request against Schema `skip_load` defaults
before publishing. ALTER preserves existing fields' choices and adds each new
field to preloads according to its own Schema rules. Dropped fields leave both
sets. `skip_load_dynamic_field` affects only preloading. Both sets come from the
same Schema by construction. Workers consume the exact lists, including empty
lists. Required fields outside preloads retain on-demand access. Index selection
and query authorization follow required fields. Readiness establishes complete
query capabilities and preparation entries, and physical warmup policy applies
to the selected preloads. Both sets remain immutable in the selected LoadInfo.

Each View pins immutable field/partition requirements and exact index selections.
LoadFieldConfig carries repeated index_ids, supporting multiple JSON path indexes
per field. CollectionLoadConfig stores these references, not IndexMeta copies.
GetQueryViewLoadInfo returns the retained exact requirements and resolves their
selected IndexIDs in the same response. It does not append current indexes or
substitute another ID. AlterIndex is not a hot update; no parameter history is
needed. Schema is fetched separately and is not part of that response.

One LoadConfigStore owns current and retained versions. View references retain
historical versions until final durable View removal. Release withdraws planning,
drains Views, and then deletes all versions; reload gets a fresh allocated ID.
See [CollectionLoadInfo](collection_load_info.md) for recovery and local GC.

The preparation identity includes SchemaVersion, LoadInfoVersion, load lifecycle,
DataView membership, and required Segment content/Manifest revisions. Config
updates and release/reload are checked against that identity before publication;
a content hash alone is not an incarnation fence.

## 5. AlterCollection event flow

Four milestones must remain distinct:

| Milestone | What it establishes | What remains pending |
| --- | --- | --- |
| Target visible | Broadcast task persistence exposes durable target intent through `GetPendingSchemaTarget()` | WAL append/application and ACKs can still be incomplete; no readiness or public-read guarantee |
| Write schema effective | Each VChannel's WAL ALTER switches write admission; consuming-side ACK additionally fences old L1 final publication | DataView certification, target QueryView readiness and public read publication |
| Data preparation complete | `PrepareDataViewSchema` / `AdvanceSchema` publish a DataView certified for the target | Loaded collections additionally need exact-target Up coverage via `WaitAtSchema`; the public read schema remains old |
| Read schema public | `MetaTable.AlterCollection` durably publishes the schema after certification and any required Up coverage | Notify View managers and invalidate caches; drain superseded old-schema Views and in-flight references |

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
    B->>B: Persist task; index immutable target (target visible)
    Note over B,V: DataCoord and planner can read pending intent before append/ACK
    B->>S: ALTER on each VChannel
    S->>S: Fence old allocation; switch write schema per channel; retain history
    P->>S: Fetch current write schema for DML
    S-->>B: Consuming-side ACK after old L1 final publication
    B->>C: Ordered ACK callback with schema and channel boundaries
    C->>D: Apply bound index metadata from ACKed ALTER
    C->>V: Install target load intent (planning gated by DataView schema)
    C->>D: Advance DataView schema directly or await schema-bump
    D-->>V: Publish certified DataView (data preparation complete)
    V->>S: Prepare target QueryView
    V->>Q: Prepare target QueryView
    Q->>S: Get exact schema and acquire retained version
    S-->>V: Resources ready; target Up protocol
    Q-->>V: Required resources Ready
    V-->>C: WaitAtSchema: complete expected-shard target Up coverage
    C->>C: MetaTable.AlterCollection (read schema public)
    C->>V: PublishCollectionSchema: release old serving obligation
    C->>P: Invalidate read cache
    C-->>B: ACK callback complete
    B->>B: Persist completion; retire pending target
    C-->>P: Complete ALTER
    V->>V: Drain old views / leases / resources
```

The sequence is a dependency graph; DataCoord work and SN/QN preparation can
overlap. Target Up uses the existing distributed Ready/Up protocol, including
its persistence and reports. WAL consumers must not wait for QueryView handoff.
Their ordered Flush, schema, data and visibility events are prerequisites for it.

Target indexing precedes the callback. After all required ACKs, the callback
executes these idempotent stages:

1. Check whether durable public metadata already covers this ALTER. Otherwise,
   install its bound index metadata; the pending target is already visible.
2. Call `Resources.PrepareSchema`, which applies target load intent through
   `LoadConfigs.ApplySchema` without recursively broadcasting.
3. Within `PrepareSchema`, call DataCoord's `PrepareDataViewSchema` to advance
   DataView schema directly or await materialization of the current complete data
   set and atomically publish it with the target schema. For loaded collections,
   trigger QueryView planning and call `WaitAtSchema` for exact-target Up coverage.
4. Call `MetaTable.AlterCollection` to durably publish the new public read
   metadata/version under the existing collection serialization.
5. Call `ShardViewRegistry.PublishCollectionSchema`, finish the outer callback's
   ALTER side effects and invalidate read caches. Broadcaster then persists task
   completion and retires its pending target. If publication succeeded before a
   crash, replay finishes lifecycle/cache side effects without reverting metadata
   or starting another materialization operation.

The target obligation is all metadata VChannels crossed with the current desired
replicas, including shards not yet in the registry. Each must have an Up view
with the exact target schema and matching load lifecycle/LoadInfo, backed by the
capabilities in section 3. Resource shortages do not reduce the denominator or
produce successful publication. Recheck configuration identity around observation.

With no LoadConfig, still certify the DataView, then publish the read schema
without implicitly loading the collection or waiting for serving Up coverage.
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
replacement keeps normal handoff. Restart restores retained published and target
views before resuming their retirement decisions. Placement recovery, however,
may only replace the greatest QueryView and must preserve or advance DataVersion.
All schemas share one Preparing/Ready slot. While target data is not ready, the
latest published view can be re-placed with its exact base. Once a newer target
view exists, failures recover that target instead of creating another old-schema
view. The latest Unrecoverable stays retained until replacement or explicit
release, so failed preparation does not make an older base current again.

Existing old-schema Up views and executing queries keep their resource
references. If their placement fails after newer target preparation begins,
other eligible replicas may still serve the published schema; otherwise requests
wait/retry within their deadlines until target handoff and public publication
complete. Target publication does not wait for old-schema placement recovery.

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

The selected serving QueryView's exact read Schema and required-field scope
define which fields a request may access. WAL ALTER establishes the write-schema
boundary and flush fence; its TimeTick does not independently revoke fields in
an already retained read generation. During target preparation, the public read
schema remains the old one and matching Up Views keep serving it. After durable
read-schema publication, new requests resolve the new schema, where the removed
FieldID is absent. Coordinator requests Down for the old generation; existing
bound requests follow the normal lease and resource-handle draining rules.

SN planning and SN/QN execution require the request SchemaVersion to match the
acquired View. Expressions, outputs, grouping, ranking and rendering dependencies
are checked against that schema and its LoadScope. MVCC controls visible data; it
neither authorizes an unavailable View nor substitutes another schema. There is
no additional per-field `ALTER_T_v` proof or Phase 1 field allowlist in the plan.
Requery can request other fields supported by the same retained View at the same
MVCC; it must retain that schema binding rather than reparse against the latest
schema. It cannot regain fields absent from the selected View.

A stale-schema request whose View has become unavailable refreshes/replans or
fails through existing View/schema errors. Strong consistency does not lower its
MVCC to keep an unavailable schema alive. See the shared contracts in
[query planning](query/query_plan.md) and [worker execution](query/query_execution.md).

## 7. Column resources and physical lifetime

A view owns immutable read requirements and resource handles. Column-level
preparation is a logical unit; object I/O can still load column groups. Resource
identity includes SegmentID, FieldID, content/base revision and applicable index
or Function identity. SegmentID alone is insufficient for reuse.

For QN:

- CCollection owns immutable Schema by `(CollectionID, SchemaVersion)`. A View
  owns independent immutable LoadScope; load fields never mutate that Schema.
- Schema-dependent Sealed Segment operations borrow `const Schema*`. The Go View
  retains its CCollection until its last query or preparation operation finishes;
  no additional per-operation Collection.Pin is needed. Segment and physical
  ReadState do not retain a current logical schema. The operation binds Schema,
  LoadScope, ReadState and MVCC.
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
  distinct from Function outputs requiring materialization. PublishedSegmentState
  may retain SealedFieldLayout for the fields and column mappings required by its
  resources, including schema-less physical inspection, eviction and reload.
  SegmentLoadInfo stores field selection and load metadata without retaining
  another layout owner. Neither structure supplies a query's logical schema.
- IndexIDs are selected by LoadInfo; compatible physical artifacts are pinned
  through execution, independently of the lifetime of the metadata RPC response.
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
| Task persisted, some channels not appended | Reconstruct the visible pending target and continue the same broadcast; incomplete ACK TimeTicks do not prove application or readiness |
| WAL changed, callback not yet applied | Writes use each SN's admitted schema; the target is already visible; read publication stays old until callback preparation and handoff complete |
| Some target resources/Up views ready | Keep published-old coverage; resume missing work |
| Target Up everywhere, before read publication | Recover target and old obligation; safely retry publication |
| Read metadata persisted, callback not done | Keep published version; replay cache/lifecycle side effects idempotently |
| Completed task is no longer present | Published metadata supplies steady-state target; available SN/View metadata supplies history |
| Schema lookup races SN migration | Follow assignment/reacquire the exact supported version; never substitute latest |
| Client timeout after durable task creation | Operation may continue; no automatic WAL/schema rollback |
| Release/reload or config change | Re-evaluate obligation with lifecycle/config identity; old readiness cannot satisfy new load |

### 8.2 TODO: unified GC design (deferred)

Storage, schema and FileResource GC questions remain deferred. CollectionLoadInfo
version retention and final Release cleanup are defined separately in
[CollectionLoadInfo](collection_load_info.md); this does not establish a distributed
index-file or schema GC protocol. Runtime reference counting and delayed
release of resources used by executing queries remain in scope: they are needed
for handoff even without persistent-storage GC.

The future unified design must cover:

- Schema history required by published read versions, pending ALTER operations,
  live/recovering views, and Segment encoding; the existing replay-floor and
  Segment-only schema GC does not describe all these consumers.
- The interval between WAL schema observation and Coordinator view registration,
  and between exact lookup and resource acquisition.
- Index descriptors and their relationship to retained LoadInfo references, old Manifest/data bases,
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

**Removal correctness:** the request's bound View schema and required-field scope
determine field validity. WAL ALTER changes write admission, not the validity of
retained old read generations. Until read publication, matching old-schema Up
Views can still serve the published schema, including its removed-in-target
fields. After publication, fresh requests resolve the new schema without those
fields; old bound requests drain with their retained Views and resource handles.
Physical retention cannot authorize fields absent from the selected View, and
MVCC controls data visibility rather than overriding schema identity.

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

QueryView schema publication and routing assume participants implement the
same unreleased QueryView contracts. No session capability flag or stream
attestation is required. Public DescribeCollection continues returning the
published read schema and does not reveal a partially prepared target.

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
| Removal | Ordinary/StructArray/function/dynamic removal and same-name re-add; WAL write switching does not revoke old read Views; new published schema rejects removed fields; old bound requests drain normally |
| Target visibility | Persist task before append/ACK; expose immutable target with absent/partial TimeTicks without allowing early data certification or read publication; recover the earliest unfinished ALTER |
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
| Durable target source | `internal/streamingcoord/server/broadcaster/{schema,broadcast_task,ack_callback_scheduler}.go`; target index updated after task persistence, ordered callback after required ACKs |
| Shared Coord access | `internal/views/coord/resource/schema.go`; `PreparationSchema` reads pending intent, `PrepareSchema` applies load intent, certifies data and waits for exact-schema Up coverage |
| DataView certification and materialization | `internal/datacoord/dataview_schema.go`, `internal/dataview/schema.go`, `internal/datacoord/compaction_policy_bump_schema_version.go`, `internal/datanode/compactor/bump_schema_version_compactor.go` |
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
