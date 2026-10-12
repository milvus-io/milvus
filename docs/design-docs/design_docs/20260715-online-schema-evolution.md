# Design Document: Online Schema Evolution for Milvus

**Date**: July 2026
**Status**: Draft
**Scope**: Add field, drop field, add function field, drop function field, schema DDL admission gates
**Feature DRI**: @Congqi Xia
**Primary Approver**: @tedxu
**Independent Approver**: @chyezh
**Design Review**: 2026-07-16 & 2026-07-27
**Review Record**:

- 2026-07-16: https://zilliverse.feishu.cn/docx/J2TRdjO2xoxDHzxy7BAcbsj8nnh
- 2026-07-27: https://zilliverse.feishu.cn/docx/BDuOd29QSo4b1oxeZoMceObknoe

---

## 1. Overview

### 1.1 Motivation

Milvus already has several schema mutation paths:

- `AddCollectionField`
- `AddCollectionStructField`
- `AlterCollectionSchema(AddRequest)` for fields/functions
- `AlterCollectionSchema(DropRequest)` for fields/functions
- `AlterCollection` for collection properties that may also mutate schema

The current implementation treats most schema mutations as a single broadcasted
schema replacement. This is simple, but it exposes a correctness gap in a
distributed system:

- Proxy, StreamingNode, QueryNode, DataCoord, and QueryCoord do not switch
  schema at the exact same time.
- Additive schema changes need old-schema data and new-schema data to coexist
  across different segment/schema-version boundaries.
- Function output fields need historical backfill and index readiness before
  they are useful to readers.
- Destructive changes must not let stale reads corrupt results or mark healthy
  QueryNodes as bad.

This document proposes a Milvus-specific online schema evolution protocol
inspired by the F1 schema change model: schema changes are represented as
ordered states, and user visibility is published only after all required gates
are satisfied.

### 1.2 Goals

1. Support online add field, drop field, add function field, and drop function
   field under distributed asynchronous schema propagation.
2. Introduce an `invisible` field state: writable by the data path but not
   usable by user read/query APIs. `DescribeCollection` still exposes the field
   and its intermediate state so users can observe `adding`/`dropping` progress.
3. Add DDL admission and readiness gates so schema changes do not publish
   user-visible behavior before dependent data, indexes, and query-side state
   are ready.
4. Preserve field ID monotonicity and never reuse a dropped field ID.
5. Make stale destructive reads fail as input errors, not as system failures
   that trigger replica blacklisting.
6. Keep the implementation compatible with the existing
   `AlterCollectionMessage` broadcast path and existing DataCoord backfill/index
   infrastructure.
7. For the Milvus 3.0 rollout, first fix the known race between schema WAL
   consumption and query-side load/balance by forbidding load and balance while
   a schema change has not finished broadcasting to all required nodes.

### 1.3 Non-Goals

- Arbitrary multi-version schema compatibility. This design targets at most one
  in-flight schema evolution per collection.
- Segment-level multi-version serving. A segment is bound to one schema version;
  this design does not require one segment to serve rows under multiple schemas.
- Full global atomic-commit semantics for the first rollout. The complete
  schema/data-view atomic switch remains the long-term target.
- Transparent destructive changes. Drop operations are fail-safe, not invisible
  to every stale request.
- Synchronous waiting for long-running backfill inside the user DDL RPC. The DDL
  creates a pending schema evolution; publishing it is a separate gated step.
- Physical deletion of old field binlogs as part of the online DDL. Cleanup can
  be a later GC feature.

### 1.4 Current Code Facts

The following current implementation details shape the design:

- `model.Field` already has `State`, and `Available()` returns true only for
  `FieldCreated`. However, `MarshalFieldModel` and `UnmarshalFieldModel` do not
  currently preserve the state.
- Add/drop/function schema mutations are currently built as new collection
  schemas and broadcast through `AlterCollectionMessage`.
- StreamingNode flushes and fences growing segments for schema-changing
  `AlterCollection` messages before appending the message to WAL.
- `CreateSegmentMessageHeader` carries `SchemaVersion`, so DataCoord and Query
  components can reason about the schema version attached to each segment.
- RecoveryStorage persists VChannel schema history and can retrieve schema by
  TimeTick. The shard manager currently keeps only the latest schema for its
  write path, so historical schema use must be modeled explicitly where needed.
- QueryNode consumes `AlterCollection` and updates its collection schema directly.
  It has separate logical schema version and schema barrier timestamp domains,
  but it currently has no separate read-visible/write-visible schema view.
- Insert messages carry a schema version. StreamingNode rejects mismatched
  versions with a streaming schema-version-mismatch error.
- DataCoord already has bump-schema-version compaction for historical backfill.
- DataCoord index inspection already skips function-output index builds when the
  segment does not yet have the output binlog.
- QueryNode has an IDF oracle for BM25 function fields, but IDF readiness is
  currently driven through load/reopen/distribution side effects rather than a
  unified schema-evolution gate.
- External collection refresh already has task state and progress APIs; schema
  evolution must treat refresh completion as a readiness input when external
  schema changes are involved.
- RootCoord `AlterCollection` ack callback already performs metadata update,
  DataCoord schema refresh, bound index creation, load config update, cascade
  drop index, and cache expiration.

---

## 2. Design Principles

### 2.1 Separate Internal Availability From User Visibility

A field can be present in internal schemas before it is safe to expose to user
reads. The write path may need to materialize the field so future data is
complete, while old sealed segments still need backfill or index build.

The read/query contract is one `visible` bit: a visible field may appear in user
read APIs; an invisible field may not. `DescribeCollection` is the exception: it
must expose the field status so users can see that a field is `adding`,
`created`, `dropping`, or `dropped`. Other callers should consume explicit schema
views instead of reinterpreting the state machine locally.

Therefore this design derives four immutable projections from one versioned full
schema:

| View | Contains | Used by |
|------|----------|---------|
| Full schema view | Every non-metadata-removed field and its internal lifecycle state | Storage, recovery, backfill, function runtime, cleanup |
| Write schema view | Fields accepted by the target schema for DML; function-output and dropping restrictions still apply | Proxy insert/upsert validation, StreamingNode function materialization, DataNode backfill |
| Read schema view | Visible fields only | Search/query planning, output field validation, user-facing index/query operations |
| Describe schema view | User-visible schema plus field lifecycle status for pending/dropping fields | `DescribeCollection`, admin/debug visibility |

The projections preserve field IDs, struct topology, and function definitions.
They do not change generic schema helpers globally; view-specific helpers make
the intended visibility explicit at each boundary.

### 2.2 Additive Changes Preserve Segment Schema Boundaries

For additive changes, user reads continue to see the old read schema until the
publish gate passes, while the write path moves to the target write schema after
the phase-1 WAL barrier.

Milvus must not mix multiple schema versions inside one segment. The
schema-changing `AlterCollectionMessage` TimeTick is the write boundary:

- writes before the barrier are validated and flushed with the previous schema;
- StreamingNode flushes and fences old growing segments at the barrier;
- new growing segments created after the barrier are stamped with the target
  schema version;
- nullable/default fields and function outputs may be materialized only inside
  segments whose schema version includes those fields.

This is stricter than arbitrary old-write acceptance. Compatibility for stale
Proxy writes after phase 1 is allowed only when it can preserve the single-schema
segment invariant.

### 2.3 Destructive Changes Prefer Safe Failure

Drop operations do not need to keep every stale query working. They must ensure:

- no silent result corruption;
- no field ID reuse;
- no healthy QueryNode blacklist caused by a user query that references a
  dropped field;
- old field data remains loadable and skippable until physical cleanup.

### 2.4 Gates Publish User Semantics

The first schema broadcast installs internal state. A later gated publish step
switches user visibility and publishes a matching immutable data view.

For add field/function field:

```
create invisible field -> build snapshot data/index/query view -> publish visible field + data view
```

For drop field/function field:

```
mark invisible/dropping -> drain stale read/write windows -> publish dropped/final metadata
```

Atomic Switch is a durable RootCoord publish record binding the read-schema
version to a collection data-view identity. It is not a broadcaster fast ACK,
Proxy cache invalidation, or a globally comparable WAL TimeTick. A reader must
never observe the published read schema with a partially built data view.

The write-schema version sequence for one schema evolution is always
`N -> N+1 -> N+1`. Phase 1 installs the target write-schema epoch and advances
the schema version. Phase 2 publishes visibility or final drop metadata without
creating another write/segment schema epoch.

For the Milvus 3.0 rollout, this atomic switch is a target model rather than a
full semantic commitment. The first deliverable is a safe serialized path that
prevents load/balance from racing ahead of schema-change broadcast and keeps each
segment attached to exactly one schema version.

### 2.5 F1 Three-Stage Mapping

This design follows the F1 schema-change shape, but maps it onto Milvus
components and existing schema states.

| F1 stage | Milvus mapping | Add field/function field | Drop field/function field |
|----------|----------------|--------------------------|---------------------------|
| Write Only | Install a schema that write/internal paths can understand while the user read view keeps the field invisible | `FieldCreating`: write path and backfill can materialize the field; read view hides it | `FieldDropping`: the read view hides the field; stale requests fail safely |
| Data Build | Build or drain the data/index/query state needed before user semantics change | Bind the operation to a snapshot, backfill historical data, build indexes, load function runtime, and build a candidate data view | Expire Proxy schema caches, apply QueryNode barriers, drain in-flight plans, propagate distribution updates |
| Atomic Switch | Publish the user-visible schema and the matching immutable data-view boundary after gates pass | Promote `FieldCreating -> FieldCreated`, expose the field in the read view, and publish the bound data view together while keeping schema version `N+1` | Publish final dropped metadata or metadata removal, carry `DroppedFieldIds`, cascade index cleanup, and publish the removal data view while keeping schema version `N+1` |

The important invariant is that user-visible semantics change only at the
Atomic Switch step, where the read-schema version and data-view identity advance
together. A schema mutation that skips Write Only, Data Build, or Atomic Switch
can expose mixed data/schema state to readers or writers.

### 2.6 Accepted Boundaries

The protocol deliberately does not try to solve every distributed race in the
first design boundary. The following cases are accepted constraints, not hidden
requirements:

1. Cross-shard schema-version consistency is not guaranteed as a single global
   cutover. The design guarantees collection-level DDL ordering and safe
   per-shard behavior, but not a simultaneous schema switch across every shard.
2. Requests that were planned or issued before DDL completion may fail after the
   schema changes. This is acceptable when the failure is standardized as schema
   mismatch, field-not-visible, or field-not-found input error as appropriate.
3. Long-running Delete by Expression concurrent with schema DDL is not handled
   as a special atomic case. If the delete expression references a field whose
   visibility changes during execution, the operation may fail safely. A future
   field-version binding design can address this low-frequency edge case.
4. Primary/secondary clusters do not have aligned segment identities. Each
   cluster executes historical data processing for schema evolution independently;
   global data equivalence is guaranteed by the external computation/backfill
   contract, not by replaying one cluster's segment-level internal state.
5. External collection schema changes are gated by refresh completion and segment
   version alignment. Refresh remains a separate DataCoord job, but its terminal
   state participates in schema-evolution readiness.

---

## 3. Field State Model

### 3.1 State Definitions

This design reuses the existing `schemapb.FieldState` enum as its lifecycle
representation. User read/write APIs consume visibility projections, while
`DescribeCollection` exposes the state value so users can observe pending schema
changes.

| Internal state | Public visibility | User writable | System writable | Meaning |
|----------------|-------------------|---------------|-----------------|---------|
| `FieldCreating` | Invisible | Ordinary fields: Yes. Function outputs: No. | Yes | Field is installed for target-schema writes/backfill but not exposed to reads |
| `FieldCreated` | Visible | Ordinary fields: Yes. Function outputs: No unless existing special policy allows it | Yes | Field and its bound data view are fully published |
| `FieldDropping` | Invisible | TBD: first rollout rejects new user writes; stale-write compatibility follows the chosen version policy | Internal cleanup only | Field is draining; stale reads fail safely |
| `FieldDropped` | Invisible | No | No | Historical tombstone, optional if retained outside main schema |

The public names shown by describe should be stable API terms such as `adding`,
`created`, `dropping`, and `dropped`; they do not have to match the protobuf enum
names verbatim.

### 3.2 Compatibility Rules

1. A field without explicit state is treated as `FieldCreated` for backward
   compatibility with existing metadata.
2. An add with `backfill=true`, and every function output field, starts as
   `FieldCreating` and invisible. An add with `backfill=false` may be created
   visible once its target query data view is ready.
3. The publish operation changes only `FieldCreating -> FieldCreated` and binds
   the visible read schema to the completed data view.
4. Drop operations first change `FieldCreated -> FieldDropping`.
5. The `FieldDropping -> FieldDropped` transition or final metadata removal
   happens after the drop drain gate passes, but it does not wait for physical
   field-binlog deletion.
6. `max_field_id` remains monotonic across all states and after physical
   metadata removal.

### 3.3 Schema View Helpers

Add explicit helpers rather than changing all existing helpers to filter by
state:

```go
func FullSchemaView(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema
func WriteSchemaView(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema
func ReadSchemaView(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema
func IsFieldReadable(field *schemapb.FieldSchema) bool
func IsFieldWritableByUser(field *schemapb.FieldSchema) bool
func IsFieldWritableBySystem(field *schemapb.FieldSchema) bool
func DescribeSchemaView(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema
```

This avoids breaking internal paths that intentionally need the full schema,
such as storage column resolution, backfill, function materialization, and
schema recovery.

---

## 4. Architecture

### 4.1 High-Level Flow

```
+----------------------------+
| User schema DDL request    |
+-------------+--------------+
              |
              v
+----------------------------+
| Proxy validation           |
| - request constraints      |
| - read/write view checks   |
+-------------+--------------+
              |
              v
+----------------------------+
| RootCoord admission gate   |
| - one pending evolution    |
| - collection resource lock |
+-------------+--------------+
              |
              v
+----------------------------+
| RootCoord phase 1          |
| - assign IDs               |
| - set FieldCreating or     |
|   FieldDropping            |
| - broadcast AlterCollection|
+-------------+--------------+
              |
              v
+----------------------------+
| WAL / StreamingNode        |
| - flush/fence if schema    |
|   change                   |
| - install internal schema  |
| - stamp segment schema     |
+-------------+--------------+
              |
              v
+----------------------------+
| Background readiness gates |
| - backfill ready           |
| - index ready              |
| - IDF/refresh ready        |
| - query ready              |
| - load/balance drained     |
| - drop drain ready         |
+-------------+--------------+
              |
              v
+----------------------------+
| RootCoord phase 2          |
| - promote visible          |
| - or finalize drop metadata|
| - broadcast AlterCollection|
+----------------------------+
```

### 4.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| Proxy | Validate DDL and DML against the correct schema view; stamp insert schema versions |
| RootCoord | Own schema evolution state machine, ID assignment, admission gates, and publish broadcasts |
| StreamingCoord Broadcaster | Serialize collection DDL by resource key and provide WAL broadcast durability |
| StreamingNode | Install internal schema, flush/fence segments, and keep every growing segment bound to one schema version |
| DataCoord | Track backfill readiness, index readiness, and schema-dependent segment metadata |
| DataNode | Execute bump-schema-version/backfill compaction and function output materialization |
| QueryCoord | Track query-side schema/load/balance/index readiness and expose publish gates |
| QueryNode | Maintain full internal schema per Delegator/Segment where needed, expose only read-visible fields to user planning/execution, and expose field state through describe |
| Segcore | Reject absent/dropped field IDs as input errors and keep dropped-field load filtering |

---

## 5. Add Field Protocol

### 5.1 Supported Add Field Classes

| Field class | Phase 1 state | Segment/schema rule | Publish gate |
|-------------|---------------|---------------------|--------------|
| Nullable scalar/vector field without backfill | `FieldCreated` / visible | New post-barrier segments use target schema; old sealed segments rely on null synthesis | Query data view ready |
| Nullable scalar/vector field with backfill | `FieldCreating` / invisible | New post-barrier segments use target schema; eligible snapshot segments are backfilled | Snapshot backfill + query data view ready |
| Field with default value without backfill | `FieldCreated` / visible | New post-barrier segments use target schema; old sealed segments rely on default synthesis | Query data view ready |
| Field with default value with backfill | `FieldCreating` / invisible | New post-barrier segments use target schema; eligible snapshot segments are backfilled | Snapshot backfill + query data view ready |
| Required non-null ordinary field | Rejected for online add | Not supported | N/A |
| Function output field | `FieldCreating` / invisible | System generates output for target-schema segments; historical segments are materialized by backfill | Snapshot backfill + index + IDF/function runtime + query data view ready |

### 5.2 Phase 1: Install Invisible Field

`AddField` carries an explicit `backfill` option. `backfill=false` preserves
existing add-field behavior: historical rows use null/default semantics and the
field may be published once the target query data view is ready. `backfill=true`
creates one serialized schema evolution and takes the field through the
invisible path.

RootCoord for the invisible path:

1. Acquire the collection schema evolution admission lease.
2. Validate no other evolution or backfill publish is pending for the collection.
3. Assign a new field ID from `max_field_id + 1`.
4. Set field state to `FieldCreating` and record the field as invisible.
5. Capture and persist the operation ID, target schema version, snapshot/data-view
   identity, manifest version, and eligible segment set.
6. Increment schema version.
7. Broadcast `AlterCollectionMessage` with the full schema.
8. In ack callback, persist metadata and notify DataCoord of altered collection.

The snapshot bounds backfill: only its eligible historical segments are
backfilled. The phase-1 broadcast TimeTick bounds the write schema: old growing
segments are flushed and fenced at this boundary, and new writes allocate
segments stamped with the target schema version. New writes are not
retroactively included in the snapshot job.

StreamingNode:

1. Receives `AlterCollectionMessage`.
2. If schema changed, flushes and fences growing segment allocation.
3. Installs the full schema as internal write schema.
4. Keeps field invisible to user reads by relying on read schema view in
   QueryNode/Proxy.

### 5.3 Phase 1 Write Behavior

Proxy insert/upsert after observing the target schema:

- Uses write schema view.
- Allows user data for ordinary `FieldCreating` fields.
- Rejects user data for function output fields unless an existing explicit
  compatibility property allows non-BM25 function output insertion.
- Fills missing ordinary `FieldCreating` fields with null/default where allowed
  by the target write schema.
- Generates function output fields through the existing function materializer.

StreamingNode:

- Uses the schema-changing `AlterCollectionMessage` TimeTick as the write
  boundary.
- Flushes and fences pre-barrier growing segments before installing the target
  schema.
- Stamps `CreateSegment` for post-barrier growing segments with the target
  schema version.
- Rejects inserts that cannot be assigned to a segment whose schema matches the
  insert payload semantics.
- Does not place rows from different schema versions in the same segment.

Stale Proxy writes that arrive after phase 1 may be rejected with schema
mismatch. Supporting a bounded compatibility path is a later optimization and
must prove it preserves the single-schema segment invariant.

### 5.4 Phase 2: Publish Visible Field

A background RootCoord schema evolution checker promotes the field only when all
required gates pass:

1. Backfill gate:
   - required for every `backfill=true` add and function output field;
   - all eligible snapshot segments have the required field data committed;
   - the commit carries the operation, target schema, snapshot/data-view, and
     manifest-version fences; stale or conflicting commits are rejected.
2. Index gate:
   - required for vector fields that must be searchable;
   - required for bound function-output indexes.
3. Query gate:
   - loaded QueryNodes have applied the schema barrier;
   - required indexes are loadable/loaded for loaded collections;
   - load and balance tasks that could install stale schema payloads are blocked
     or drained;
   - the candidate data view is installed for the target schema.

Promotion:

1. RootCoord changes `FieldCreating -> FieldCreated`.
2. RootCoord keeps the target schema version from phase 1 (`N+1`); this publish
   does not create another write-schema epoch.
3. RootCoord persists one publish record binding the visible read-schema version
   and the completed data-view identity.
4. RootCoord broadcasts another cluster-local `AlterCollectionMessage`.
5. Proxy/QueryNode read schema views expose the field only with that bound data
   view.

---

## 6. Add Function Field Protocol

Add function field is an add field plus a function definition and often a bound
index. The protocol uses the same two phases but tightens the gates.

### 6.1 Function API Contract

Function operations must be bound to the lifecycle of their output fields. The
schema contract should not allow function metadata to change independently from
the columns that store its output.

API rules:

1. Reject standalone Add Function and Drop Function APIs that mutate only
   function metadata.
2. Add Function must be expressed as Add Function Field: the function definition,
   a newly allocated output field, and bound indexes are created in one schema
   evolution. It must reject attempts to attach a function to an existing
   ordinary, visible, or invisible field.
3. Drop Function must drop the corresponding output field in the same schema
   evolution. The output field must not remain as a normal field after the
   function metadata is removed.
4. Function output fields must not outlive an incompatible function definition.
   Model changes, hash-code changes, tokenizer changes, embedding dimension
   changes, or any other semantic rewrite that makes historical output
   incompatible must not be accepted as an in-place function modification.
5. Modify Function is allowed only for a whitelist of metadata-only changes that
   do not change the meaning of existing output field data. Examples may include
   comments, display metadata, or execution hints that are proven not to affect
   generated values. The exact whitelist is part of the API contract and must be
   reviewed before enabling the RPC path.

This is stricter than the current loose interface shape by design. It prevents a
schema state where a new function definition is attached to old output data that
was produced by an incompatible model or encoding rule.

### 6.2 Phase 1

RootCoord:

1. Validate function type, input/output arity, input field types, output field
   type, and index params.
2. Assign field ID and function ID.
3. Set output field `IsFunctionOutput=true`.
4. Set output field `State=FieldCreating`.
5. Serialize bound index metadata in the `AlterCollectionMessage` body.
6. Broadcast the full schema.

Ack callback:

1. Persist collection schema.
2. Broadcast altered collection to DataCoord before bound index is visible.
3. Apply bound index metadata inline using the existing callback pattern.

### 6.3 Write Path

Function outputs are writable by system code only:

- Proxy and StreamingNode may materialize them from function inputs.
- User-supplied BM25 output remains rejected.
- Non-BM25 function output follows the existing explicit property gate if
  Milvus already allows it.

### 6.4 Publish Gates

Function output fields require all of the following:

1. All healthy sealed segments at the target schema version have the function
   output column materialized, or are known to not require it.
2. All required segment indexes for the output field are finished.
3. QueryCoord has distributed the new schema and index info to all relevant
   QueryNodes.
4. QueryNodes have installed function runtime state for the new schema version.
5. BM25 function fields additionally require IDF oracle readiness: all target
   sealed segments that participate in the publish view have readable BM25 stats,
   the IDF target version has advanced to the candidate view, and any Reopen-only
   loading side effects have been converted into an observable readiness state.

Only after these pass does RootCoord promote the output field to `FieldCreated`.

---

## 7. Drop Field Protocol

### 7.1 Phase 1: Mark Dropping

RootCoord:

1. Wait for schema-drop readiness using the existing readiness barrier.
2. Acquire schema evolution admission.
3. Validate the field can be dropped:
   - not primary key;
   - not partition key;
   - not clustering key;
   - not dynamic field unless using the dynamic-field disable path;
   - not the last vector field;
   - not referenced by functions unless dropping the function too.
4. Change field state from `FieldCreated` to `FieldDropping`.
5. Increment schema version.
6. Broadcast `AlterCollectionMessage`.

Read/write behavior:

- Read schema view hides `FieldDropping`.
- Write schema view rejects user writes to `FieldDropping`.
- Stale requests that still carry the dropped field ID fail with an input error.

### 7.2 Drop Drain Gate

Before final dropped metadata is published, the system waits for:

1. Proxy cache expiration for the collection schema.
2. QueryNode schema barrier application for loaded collections.
3. No in-flight queries known to have been planned with the pre-drop schema, or
   expiration of a configured drain window.
4. Existing QueryCoord distribution updates have propagated.

### 7.3 Phase 2: Finalize Drop Metadata

RootCoord:

1. Builds a final schema where the dropped field is either represented as a
   `FieldDropped` tombstone or removed from the main field list according to the
   chosen metadata policy.
2. Keeps `max_field_id` unchanged or advanced, never decreased.
3. Carries `DroppedFieldIds` in the `AlterCollectionMessage` header.
4. Keeps the phase-1 target schema version (`N+1`); this broadcast does not
   create another write/segment schema epoch.
5. Broadcasts the final-drop schema as a cluster-local phase-2 message.

Ack callback:

1. Persists schema.
2. Broadcasts altered collection to DataCoord.
3. Cascades index drops inline for `DroppedFieldIds`.
4. Expires caches.

QueryNode/Segcore:

- Keep skipping binlogs/indexes whose fields no longer exist in schema.
- Return `FieldIDInvalid` for stale read plans that reference the removed field.
- The Go merr mapping classifies segcore code 2020 as input error.

Physical field data deletion is not part of the `FieldDropping -> FieldDropped`
state transition. Compaction or GC that removes old field binlogs runs
asynchronously after metadata has made the field unavailable to user reads and
writes.

---

## 8. Drop Function Field Protocol

Drop function field is a coordinated drop of:

- function metadata;
- output field metadata;
- indexes on output fields;
- function runtime state in QueryNode.

### 8.1 Mark Dropping

RootCoord marks output fields as `FieldDropping` and removes the function
reference in the same schema update. The function metadata and output fields
move through the same lifecycle. This proposal does not support preserving the
output field after function metadata is removed.

### 8.2 Final Drop Metadata

- phase 2 marks output fields dropped or removes them from the main schema
  metadata;
- `DroppedFieldIds` includes every removed output field;
- bound indexes are cascade-dropped inline;
- field IDs are not reused.

As with ordinary fields, physical function-output data cleanup is asynchronous
and is not a prerequisite for the dropped state.

Preserving the output field as a normal field after removing function metadata
is not part of this schema evolution contract.

---

## 9. DDL Admission Gates

### 9.1 Collection-Level Schema Evolution Gate

RootCoord must reject or queue a schema mutation when the same collection has a
pending evolution or a backfill job whose publish path is unfinished. The
admission lease lasts from phase 1 through backfill and phase 2; it is separate
from the broadcaster's short-lived collection resource lock.

Pending means:

- any field in `FieldCreating`;
- any field in `FieldDropping`;
- a persisted schema evolution task whose publish/cleanup phase is not done;
- a backfill operation whose snapshot/data-view commit or publish is unfinished.

The initial implementation should reject with a typed service-not-ready or
schema-evolution-in-progress error. Queuing can be added later. The meeting
requires serialized schema evolution and backfill publication; whether this
serialization remains per collection or is extended cluster-wide is an explicit
open question, not an implication of the broadcaster lock.

### 9.2 Resource Locking

Continue using broadcaster collection locks:

- `SharedDBName + ExclusiveCollectionName` for collection-level schema DDL.
- Do not call separate DDL RPCs from inside ack callback if they would acquire
  the same lock. Use inline ack callback application for bound index create and
  cascade drop index.

### 9.3 Broadcast ACK Semantics

The broadcaster can fast-ack by append results unless `AckSyncUp` is set.
Schema evolution must not interpret fast ACK as query-side readiness.

Therefore:

- phase 1 ACK means the schema change is durably appended and RootCoord metadata
  can be updated;
- publish gates separately observe DataCoord/QueryCoord/QueryNode readiness.

### 9.4 Proxy Consistency Simplification

The full target design uses readiness gates before publishing user-visible
semantics. For the near-term implementation, Proxy consistency can be simplified
without introducing a complex global distributed lock.

Near-term strategy:

1. RootCoord still serializes schema DDL per collection and broadcasts the schema
   mutation through the existing `AlterCollectionMessage` path.
2. After the schema mutation is durably acknowledged, RootCoord invalidates
   Proxy Describe/schema caches for the collection through the existing cache
   expiration mechanism.
3. Proxy fetches the latest schema version on cache miss and uses the read/write
   schema view rules locally.
4. DDL success means new-schema requests issued after the cache invalidation and
   refresh path should be accepted by the write path when they satisfy the target
   schema constraints.

Boundaries:

- This does not provide a hard global cutover instant across all Proxy nodes.
- Requests already planned before DDL completion may still fail with a
  standardized schema mismatch or field-not-visible error.
- This simplification is acceptable for the current rollout target as long as
  new-version writes after DDL completion are guaranteed to land, and stale
  destructive requests fail safely.
- The full query/data readiness gates remain the long-term publish mechanism for
  making newly added fields user-readable.

### 9.5 Load/Balance Admission Gate

Milvus 3.0 should first add a conservative gate around query-side load and
balance. While a schema change is in the phase-1 broadcast/update window, all
load, load-partition, reload, balance-segment, and balance-channel operations
for that collection must be rejected, delayed, or drained.

This gate fixes the known race where a QueryNode can load or balance data using a
schema snapshot whose version is inconsistent with the WAL order it later
consumes. It is intentionally simpler than the long-term publish gate:

- the gate is entered before RootCoord broadcasts the schema-changing
  `AlterCollectionMessage`;
- it is released only after required QueryCoord/QueryNode schema update callbacks
  are complete or the operation is rolled back;
- manual and automatic balance paths use the same predicate;
- the error must be retriable service-not-ready/schema-evolution-in-progress, not
  an input error.

### 9.6 Write Timeline and Segment Schema Gate

The first rollout should use WAL ordering and segment schema versions as the
compatibility boundary.

StreamingNode keeps enough schema metadata to establish:

- previous schema before the schema-change TimeTick;
- current target schema after the schema-change TimeTick;
- each segment's schema version from its `CreateSegment` message or recovered
  segment metadata.

Allowed cases:

| Incoming write position | Segment schema | Result |
|-------------------------|----------------|--------|
| Before schema-change barrier | Previous schema | accept if request is valid under previous schema |
| After schema-change barrier | Current target schema | accept if request is valid under write schema view |
| After schema-change barrier with stale payload | No matching segment can be allocated | reject with schema mismatch |
| Destructive change with stale field write | Field is `FieldDropping` or removed | reject with schema mismatch or field-not-visible input error |

The design no longer claims arbitrary multi-version write compatibility. A future
optimization may accept stale additive writes by transforming them to the target
write schema, but only if tests prove that omitted fields, partial upserts, and
function outputs still preserve the single-schema segment invariant.

---

## 10. Metadata and Recovery

### 10.1 Persist FieldState

`MarshalFieldModel` and `UnmarshalFieldModel` must preserve `FieldSchema.State`.
For backward compatibility, missing/zero state is interpreted as
`FieldCreated`.

### 10.2 Persist Schema Evolution Tasks

RootCoord needs persistent progress for phase-2 work. A minimal task record:

```protobuf
message SchemaEvolutionTask {
  int64 collection_id = 1;
  int32 from_schema_version = 2;
  int32 target_schema_version = 3;
  string operation_id = 4;
  SchemaEvolutionOp op = 5;
  repeated int64 field_ids = 6;
  SchemaEvolutionPhase phase = 7;
  uint64 phase1_timetick = 8;
  string snapshot_id = 9;
  string candidate_data_view_id = 10;
  int64 manifest_version = 11;
  repeated int64 eligible_segment_ids = 12;
}
```

This task can live in RootCoord catalog metadata. It allows RootCoord to resume
publishing after restart and makes external backfill commits fenceable. A commit
must match the operation ID, target schema version, snapshot/data-view identity,
and manifest version; stale or conflicting commits must be rejected or retried
rather than merged implicitly.

### 10.3 StreamingNode Recovery

RecoveryStorage already persists VChannel schema history. It should preserve
field states in the schema snapshots. On recovery, StreamingNode reconstructs
the current internal schema, the schema-change TimeTick history, and the schema
version attached to each active segment. This is required to keep the post-restart
write path from assigning rows to a segment with the wrong schema.

### 10.4 QueryNode Recovery

QueryNode loaded collection schema should keep full internal schema and derive
read view on demand. A restarted QueryNode receives load meta and schema barrier
from QueryCoord and must not expose `FieldCreating` or `FieldDropping` fields to
user plans.

The target model is that each Delegator and Segment has the schema snapshot it
needs for its own data. Shared schema objects may be deduplicated by schema
version for memory efficiency, but correctness cannot depend on a single mutable
collection-global schema being suitable for every segment.

---

## 11. Read Path Changes

### 11.1 Proxy Planning

Search/query/retrieve planning must use read schema view:

- output fields;
- filter expression fields;
- group-by/order-by fields;
- function-chain fields;
- index-related user APIs.

When a user references an invisible or dropping field by name, the error should
be an input error because the requested field is not user-visible.

### 11.2 QueryNode Execution

QueryNode may store the full schema internally for segment loading and function
runtime, but user-facing plans should not reference invisible/dropping fields.

Defense-in-depth:

- validate plan field IDs against read schema view before calling segcore;
- if segcore still sees a dropped/unknown field ID, preserve `FieldIDInvalid`
  mapping to input error.

### 11.3 Describe APIs

`DescribeCollection` should expose fields in intermediate states together with
their lifecycle status. This is the user-facing progress surface for online
schema evolution.

Describe visibility does not make a field readable or writable. A
`FieldCreating` field returned by describe is still rejected by search/query
planning until publish; a `FieldDropping` field is shown only so the user can
observe that the drop is draining.

---

## 12. Write Path Changes

### 12.1 Proxy Insert/Upsert

Insert/upsert validation uses write schema view:

- include `FieldCreating`;
- exclude `FieldDropping`;
- prevent user-supplied function output where policy disallows it;
- fill missing nullable/default fields;
- materialize function outputs.

The insert header continues to carry schema version.

### 12.2 StreamingNode Insert

StreamingNode validates inserts against the schema implied by the WAL timeline
and the target segment:

1. A DML before the schema-change barrier uses the previous schema and is flushed
   with pre-barrier growing segments.
2. A DML after the schema-change barrier must satisfy the target write schema.
3. Segment allocation/recovery must propagate `SchemaVersion` so every segment
   remains bound to one schema.
4. A stale write that cannot be transformed without violating the segment schema
   invariant is rejected.

The error for stale or incompatible writes remains a streaming schema version
mismatch. Proxy translates it to `ErrCollectionSchemaMismatch`.

### 12.3 Upsert Partial Update

Partial update must be audited separately. A stale partial update must not clear
new invisible fields by omission. For additive schema changes, merge logic must
treat omitted new fields as "unchanged/default fill", not as deletion.

This is a required verification item before claiming partial-upsert safety.

---

## 13. Readiness Gates

### 13.1 Backfill Gate

DataCoord exposes whether all relevant healthy segments have reached the target
schema version or contain required field binlogs.

For function output fields, the gate requires physical function output columns
for all target historical segments.

For nullable/default ordinary fields, the gate can be relaxed only if QueryNode
and segcore reliably synthesize missing values for all read surfaces.

### 13.2 Index Gate

For every required index on the new field:

- index metadata exists;
- every eligible sealed segment has a finished segment index;
- segments without function-output binlog are not counted as ready.

This reuses existing DataCoord index metadata and inspector behavior.

### 13.3 Query Gate

QueryCoord exposes per-collection schema readiness:

- loaded replicas have received the target schema barrier;
- loaded indexes required for the target field are available;
- QueryNode function runtime state is installed when needed.
- load/reload and balance operations that could install old schema have been
  blocked or drained for the collection.

Because TimeTick is only comparable within a PChannel, readiness must be tracked
per vchannel/pchannel and then aggregated at collection level.

### 13.4 IDF Gate

BM25 function fields require IDF readiness as an explicit gate, not an implicit
side effect of Reopen or distribution updates.

The gate passes only when:

- every target sealed segment has loaded readable BM25 stats for the function
  output field, or is known not to participate in the candidate view;
- QueryNode IDF oracle target version matches the candidate data view;
- missing stats, stale target versions, and Reopen-only inactive stats are
  observable as not-ready states.

### 13.5 External Collection Refresh Gate

For external collections, a schema evolution that depends on external metadata
or segment refresh waits for the corresponding refresh job to finish. The refresh
completion state becomes part of the schema-evolution gate, and promotion happens
only after all refreshed segments are aligned to the target schema/data view.

Primary/secondary clusters perform this historical-data processing independently
because their segment layouts are not guaranteed to match.

### 13.6 Drop Drain Gate

Drop drain readiness includes:

- proxy schema cache expiration;
- QueryNode schema update;
- configured in-flight query drain window;
- no known QueryCoord distribution update is pending for the old schema.

---

## 14. Error Handling

### 14.1 Input Errors

The following are user input errors:

- user references `FieldCreating`, `FieldDropping`, or removed fields by name;
- user writes a `FieldDropping` field;
- user writes a function output field that policy disallows;
- stale read plan reaches segcore and fails with `FieldIDInvalid` code 2020.

### 14.2 System Errors

The following are system errors:

- schema evolution task metadata is corrupt;
- RootCoord cannot resume a pending task due to internal inconsistency;
- QueryCoord/DataCoord readiness RPC fails;
- StreamingNode cannot resolve the schema version for a segment due to missing
  internal or recovery state.

### 14.3 Retriable Conditions

The following should remain retriable system conditions:

- publish gate not ready;
- backfill/index/query readiness temporarily unavailable;
- DataCoord/QueryCoord not ready while polling;
- load or balance blocked while schema evolution is in progress;
- external collection refresh not finished for a schema-dependent publish.

Do not mark these as input errors. InputError aborts `retry.Do` and disables
proxy failover.

---

## 15. Compatibility and Rollout

### 15.1 Feature Flag

Introduce a feature flag for the new two-phase schema evolution protocol.

When disabled:

- existing add/drop behavior remains unchanged;
- field states should still be persisted safely if present.

When enabled:

- add starts at `FieldCreating`;
- drop starts at `FieldDropping`;
- publish/cleanup is driven by gates;
- load and balance are gated while schema changes are not fully applied.

### 15.2 Rolling Upgrade

Rolling upgrade is a major risk. Old components do not understand state-based
visibility.

Minimum safe policy:

- reject stateful schema evolution while any component version is below the
  required minimum;
- allow only legacy schema DDL behavior when the feature flag is off;
- treat missing field state as `FieldCreated`.

### 15.3 API Compatibility

The user API can remain `AlterCollectionSchema`.

Function API compatibility must follow the stricter contract in section 6.1:

- standalone Add Function and Drop Function APIs should be deprecated;
- function creation/deletion should be bound to output field lifecycle;
- Modify Function should be guarded by an explicit whitelist of
  non-semantic-only changes.

Optional additions:

- `DescribeCollection` exposes field lifecycle status for fields in adding,
  created, dropping, and dropped states.
- A future admin API may expose detailed schema evolution task progress, gate
  state, and data-view identifiers.
- DDL response may include an operation ID if the user needs to wait for
  publish completion.

### 15.4 Primary/Secondary Clusters

Schema DDL replication must not assume that primary and secondary clusters have
the same segment IDs or segment boundaries. Each cluster runs its own historical
data processing, refresh, IDF, and index readiness checks. The cross-cluster
contract is data equivalence after the external computation/backfill completes,
not segment-level WAL replay of primary-side readiness state.

Phase 2 broadcasts are cluster-local. They must not be replicated from the
primary cluster to backup/secondary clusters, because the secondary's readiness
gates may complete at a different time and over a different segment layout. A
secondary cluster emits its own local Phase 2 broadcast only after its local
backfill/index/IDF/refresh/query gates pass.

### 15.5 Milvus 3.0 Rollout Boundary

The first production iteration should prioritize the safety fix agreed in the
2026-07-27 review:

1. Serialize schema change with query-side load and balance for the affected
   collection.
2. Ensure schema-change broadcast and QueryCoord/QueryNode update callbacks
   complete before the gate is released.
3. Keep each segment bound to a single schema version.

The 3.0 iteration does not claim full global atomic commit, transparent
multi-version segment serving, or complete Batch Update semantics. Batch Update
may reuse lower-level data-view machinery later, but it needs two physical data
versions and is a separate design.

---

## 16. Implementation Plan

### Phase 0: Milvus 3.0 Safety Gate

1. Add collection-level schema-evolution predicate shared by load, reload,
   manual balance, and automatic balance.
2. Enter the gate before schema-changing `AlterCollectionMessage` broadcast.
3. Release the gate only after required schema update callbacks finish or the
   schema change is rolled back.
4. Return retriable not-ready errors for blocked load/balance operations.

### Phase 1: Metadata Foundation

1. Preserve `FieldState` in model marshal/unmarshal.
2. Add schema view helpers and tests.
3. Treat missing state as `FieldCreated`.
4. Ensure `max_field_id` remains monotonic.

### Phase 2: Read/Write View Adoption

1. Proxy search/query/index paths use read schema view.
2. Proxy insert/upsert paths use write schema view.
3. Describe APIs use describe schema view and expose field status.
4. QueryNode planning/execution validates read-visible fields.
5. Keep full schema for internal storage/function paths.

### Phase 3: RootCoord State Machine

1. Add schema evolution admission gate.
2. Add phase-1 add/drop state transitions.
3. Persist schema evolution task progress.
4. Add phase-2 promote/remove broadcasts.

### Phase 4: StreamingNode Segment Schema Boundary

1. Preserve schema-change TimeTick history needed by recovery and validation.
2. Ensure `CreateSegment` and recovered segment metadata carry schema version.
3. Flush/fence pre-barrier growings and allocate post-barrier segments with the
   target schema.
4. Reject stale or incompatible writes that cannot preserve one schema per
   segment.

### Phase 5: Readiness Gates

1. Persist operation, snapshot/data-view, and manifest fences for backfill commits.
2. DataCoord backfill readiness API.
3. DataCoord index readiness API.
4. QueryCoord query/load/balance/data-view readiness API.
5. QueryNode IDF readiness signal for BM25 function fields.
6. External collection refresh readiness integration.
7. RootCoord background publisher that atomically binds visible schema and data view.

### Phase 6: Drop Cleanup

1. Implement `FieldDropping` drain gate.
2. Publish final dropped metadata or metadata removal with `DroppedFieldIds`
   while keeping schema version `N+1`.
3. Verify cascade index deletion remains idempotent.

---

## 17. Testing Strategy

### 17.1 Unit Tests

| Component | Test Scope |
|-----------|------------|
| metastore model | FieldState marshal/unmarshal; missing state compatibility |
| typeutil/schemautil | read/write/full/describe schema views; struct field handling |
| proxy | invisible fields hidden from read planning; describe exposes lifecycle status; accepted/rejected write cases |
| rootcoord | add phase 1, promote phase 2, drop phase 1, final drop metadata |
| streamingnode | schema-change TimeTick boundary; segment schema version stamping/recovery; destructive mismatch rejection |
| datacoord | snapshot-scoped backfill readiness; fenced commit; function output index readiness; external refresh readiness |
| querycoord | schema barrier/query/load/balance/data-view readiness aggregation |
| querynode | invisible/dropping field not user-plannable; schema/data-view publish is observed together; full schema still usable internally; IDF readiness |
| segcore/merr | FieldIDInvalid remains input error |

### 17.2 Integration Tests

1. Add nullable/default field without backfill with pre-barrier old-schema rows
   and post-barrier target-schema rows.
2. Add field with `backfill=true`, capture its snapshot, and verify only eligible
   historical segments are backfilled.
3. Verify schema-changing DDL flushes/fences old growings and post-barrier
   segments carry the target schema version.
4. Add function output field while collection is loaded.
5. Verify an invisible field is present with status in describe but not readable
   before publish.
6. Verify field becomes readable only after snapshot backfill/index/IDF/query/data-view
   readiness and the bound schema/data-view publish.
7. Inject stale operation/schema/snapshot/manifest identifiers into a backfill
   commit and verify it is rejected or retried without implicit merge.
8. Drop scalar field while collection is loaded.
9. Verify stale read fails as input error and does not blacklist QueryNode.
10. Drop indexed vector field when another vector field remains.
11. Drop function field cascades bound indexes and rejects a detach-only outcome.
12. Restart RootCoord during pending `FieldCreating` and ensure the admission lease
    and promotion resume.
13. Restart StreamingNode during pending evolution and ensure schema history and
    segment schema versions recover.
14. Restart QueryNode during pending evolution and ensure invisible fields are
    not exposed and schema/data-view binding is preserved.
15. Submit a second schema DDL while backfill publish is pending and verify it is
    rejected under the serialized-evolution policy.
16. Partial upsert during additive transition does not clear invisible fields.
17. Submit load and manual/automatic balance during phase-1 schema update and
    verify they are retried or blocked until the schema gate is released.
18. External collection schema evolution waits for refresh completion and segment
    version alignment before publish.
19. Primary and secondary clusters run independent historical processing and do
    not exchange segment-level readiness state.

### 17.3 Verification Gate

Before claiming behavioral safety:

1. Audit every place that constructs, rewrites, or drops field state.
2. Trace add field, add function field, drop field, and drop function field
   from RootCoord broadcast to QueryNode/segcore behavior.
3. Fault-inject backfill not ready, index not ready, IDF not ready, external
   refresh not ready, QueryCoord not ready, load/balance blocked, and stale field
   read.
4. Verify error classification end-to-end:
   - stale user field reference is input error;
   - readiness not ready is retriable system error;
   - load/balance blocked by schema evolution is retriable system error;
   - schema metadata corruption is system error.

---

## 18. Open Questions

1. Should `FieldDropped` be retained in collection schema, or represented only
   by `max_field_id` plus historical catalog versions?
2. Should the DDL RPC return immediately after phase 1 or block until phase 2
   publish for some operation modes?
3. What exact describe/API shape should expose pending schema evolution status?
4. Which ordinary field types require physical backfill before publish, versus
   query-side null/default synthesis?
5. What is the exact drain-window policy for drop field in high-QPS query
   workloads?
6. Should schema evolution admission reject concurrent DDL, or persist and queue
   them in RootCoord?
7. Is the serialized evolution/backfill admission lease per collection or
   cluster-wide, and which non-schema operations participate in it?
8. What is the canonical data-view identity and cut: internal snapshot, explicit
   per-vchannel cut, or another immutable collection view? How are growing
   segments represented and retained until publish?
9. Should a later rollout support bounded stale additive writes by transforming
   them to the target write schema, and what proof is required for partial upsert?
10. What is the exact unified readiness contract for BM25 IDF state, including
    Reopen-loaded but inactive stats?
11. How should external collection refresh failures be surfaced in schema
    evolution task status and DDL wait APIs?

---

## 19. Future Work

1. Multi-field atomic schema evolution.
2. Online rename as add-new-field plus drop-old-field with explicit migration.
3. Physical binlog GC for dropped fields.
4. Admin API for schema evolution progress.
5. More general safe-change rewriting inspired by F1, such as expanding a
   dangerous in-place mutation into add/backfill/swap/drop.
