# QueryNode QueryView Resource Preparation

This document defines QueryNode resource ownership, preparation, readiness and
release. Related contracts are [QueryView lifecycle](../query_view_state_machine.md),
[handler](../query_view_handler.md), [schema handoff](../schema_evolution.md),
[transform frontiers](../transform_start_after_timetick.md), and
[TransformLog delivery](../../wal/transform_log.md).

## 1. Principles

1. A QueryView declares immutable resource requirements. It does not prescribe
   a sequence of physical loading operations.
2. Resource references bind physical resources to their consumers. Resource
   lifetime covers every referencing View, executing query and unfinished
   operation that can access it.
3. Resource scheduling reconciles actual capabilities with the requirements of
   all live bindings. Preparation continues until those requirements are
   satisfied, withdrawn, or explicitly reported as unsatisfiable.

Each physical resource has one lifecycle owner. Loading, metadata transport,
transform application and native execution remain separate modules, but they
operate on references supplied by that owner. They do not independently decide
when the same physical resource can be destroyed.

## 2. Requirements and identity

A View requirement contains:

- CollectionID, VChannel and the complete QueryViewKey;
- exact SchemaVersion and immutable LoadInfoVersion;
- assigned partitions and Segments;
- each Segment's required Manifest/base and exclusive transform replay cursor;
- the shard's exclusive TransformLog retention start F.

Schema is resolved through `streaming.WAL().SchemaClient()` using the VChannel
and exact SchemaVersion. `GetQueryViewLoadInfo` resolves the exact retained
LoadInfo identity, including fields and partitions. LoadInfo IndexID is always
zero: QueryViews do not bind an index version. Physical preparation uses the latest
available compatible indexes from current Segment metadata. A
pending dependency is represented explicitly; preparation never substitutes
latest schema or load configuration for an unavailable required identity.

The requirements are copied or borrowed under an immutable ownership contract
when the View is bound. Later metadata resolution fills dependencies, not a new
logical requirement. Another QueryView carries any changed requirement.

Distinguish these identities:

| Identity | Meaning |
| --- | --- |
| QueryViewKey | The View that owns a binding; includes shard/replica and View version |
| CollectionID / SegmentID | Logical resource lookup key |
| Resource and physical-instance identity | The concrete owner and physical incarnation held by operations and handles |
| SchemaVersion / LoadInfoVersion | Exact query schema and load requirements |
| Manifest/base revision | Required stored data and materialization capability |
| Metadata revision | Equality token for a complete watched snapshot |
| Operation identity | A particular load, reopen, subscription or drain attempt |

A logical key is not an incarnation fence. LoadInfoVersion and metadata revision
are content identities, not ordered clocks. A larger SchemaVersion alone does
not prove compatibility with another View's field, Function or query semantics.

## 3. Ownership model

```text
QueryView
  -> ViewResources
       -> CollectionRuntime reference
       -> VChannel TransformBuffer range reference
       -> SegmentBinding[]
            -> SegmentResource
                 -> metadata subscription and accepted snapshot
                 -> physical Segment and load/reopen operations
                 -> prepared ReadStates
                 -> transform registration and application progress
                 -> query and operation references
```

| Component | Responsibility |
| --- | --- |
| QueryView handler/state machine | Receives desired Views, binds/unbinds their resources, aggregates reports and implements View lifecycle |
| ViewResources | Owns one View's immutable requirements, resource references and per-binding readiness results |
| SegmentResource | Sole owner of a Segment's physical preparation and lifetime; reconciles all bindings and owns subscriptions, operations and transform registration |
| CollectionRuntime | Reference-counted CollectionID + SchemaVersion planning resource with a separate immutable View load scope |
| TransformLogBuffer | Shares VChannel history and transport; applies transforms through instance-bound registrations |
| Metadata stream | Delivers complete snapshots and resumes live subscriptions after transport failure |
| Physical loader | Executes load/reopen/index/delta work using pinned inputs; returns results to the resource owner |
| NodeScheduler | Executes queued work and delayed retries; resource owners decide what work is necessary |

`ViewResources`, `SegmentBinding` and `SegmentResource` name ownership roles;
internal APIs and file organization follow these boundaries. No generic workflow
engine or additional cross-node protocol is required.

### 3.1 SegmentBinding

A binding combines the View's reference and its Segment requirement in one
record. It exists even while collection metadata or a Segment snapshot is
unavailable. It records resolved dependencies, its prepared ReadState, readiness
and failure result. A requirement cannot disappear merely because no physical
load can be submitted yet.

The resource's binding set is the authority for View ownership and required
capabilities. ViewResources holds the corresponding binding handles; it does
not maintain a second independent Segment lifecycle.

### 3.2 SegmentResource

The resource owns:

- the binding set and currently prepared capabilities;
- accepted metadata and its subscription;
- its physical instance and active operation identities;
- transform registration, replay coverage and live application;
- references held by queries and operations;
- reconciliation scheduling and closure state.

All creation, replacement and destruction of these objects belongs to this
owner. A resource failure is reported through its bindings; another manager does
not reset or destroy a Segment by looking up its ID.

A shared resource may satisfy multiple replicas and multiple View versions on
one node. References protect only compatible capabilities; a shared SegmentID
is not sufficient evidence of reuse.

## 4. Binding a View

The handler processes new Preparing bindings before old teardown in the same
batch. Under its normal lifecycle serialization, resource acquisition:

1. captures the immutable requirements;
2. registers collection, buffer-range and Segment bindings;
3. pins compatible existing resources before returning;
4. marks the affected resources for reconciliation.

This synchronous phase performs only bounded in-memory registration. It does
not wait for RPCs, subscriptions, object storage, native loading, catch-up or
resource destruction. All completion callbacks are asynchronous relative to
Acquire/Release and execute outside shard and resource locks.

Resolving schema/load info and establishing subscriptions are scheduled resource
work. The binding already exists while these dependencies prepare. Their
completion updates dependency state and wakes reconciliation of the same
binding; there is no second resource-acquisition phase.

An empty local Segment assignment still prepares its collection/load scope and
required View-level buffer dependencies before reporting an empty ready set.

## 5. Reconciliation and scheduling

`Reconcile` is the single decision point for advancing a Segment resource. It
compares every live binding against resolved dependencies and actual prepared
capabilities, then selects executable work or publishes completed bindings.

### 5.1 Inputs and wakeups

Every relevant change updates stored state and marks the resource dirty:

| Change | Required reconsideration |
| --- | --- |
| Bind or unbind | Required capabilities, reuse, remaining work and closure |
| Collection runtime becomes available | Requirements waiting for schema/load metadata |
| Metadata snapshot arrives | Initial load, materialization, Manifest and index work |
| Operation succeeds, fails or is canceled | Actual capability, retries and other executable bindings |
| Transform progress or registration changes | Catch-up completion and valid live attachment |
| Subscription recovery or dependency failure | Retained work and retry/failure decisions |
| Final query/operation reference releases | Deferred destruction or waiting reconstruction |

Notifications are coalesced wakeups, not the work itself. State and dirty work
remain stored until reconciled. An event arriving during a pass schedules a
successor pass; completing a pass cannot erase a concurrently published change.
Use local scheduling state independently of content-based metadata revisions.

A resource may be idle only if all required capabilities are satisfied, or if
its unsatisfied bindings have a registered dependency wakeup or scheduled retry.
Dependency observation and wakeup registration must have no missed-update gap.
Neither another metadata revision nor a repeated Coordinator push is required
to discover work already implied by local state.

### 5.2 Work selection

Reconciliation first publishes bindings already satisfied by retained resources,
then chooses work for unresolved bindings. It reuses complete compatible
ReadStates and prepares missing capabilities without invalidating older ones.

A binding waiting for unavailable materialization must not prevent another
compatible, executable binding from progressing. Preserve published-schema
availability and provide fair progress for pending bindings. LoadInfo hashes
cannot be used to rank requests.

Only one physical load/reopen mutation runs on a resource at a time. Metadata
acceptance, demand updates and cancellation remain available while I/O runs.
Transform application uses the native synchronization required to preserve its
ordering across physical updates; column preparation does not restart the
VChannel subscription.

### 5.3 Operation completion

Work executes outside resource locks using immutable snapshots and pinned
collection/native dependencies. Each operation holds its concrete owner and
attempt identity. Completion records the result only in that owner, releases
operation references and wakes reconciliation.

Successful work may be reused by another compatible binding even if the View
that first requested it has left. An obsolete result cannot acknowledge another
requirement or replace another incarnation. Unneeded results release their own
resources and accounting exactly once.

Cancellation is a request, not completion. Tasks canceled before execution and
native operations finishing after cancellation both require final accounting.
No scheduler worker may wait for work whose execution requires that same
exhausted worker pool. Close and external callbacks do not run under resource
or shard locks.

### 5.4 Retry and failure

Locally recoverable I/O, metadata and temporary admission failures retain the
requirements and retry with backoff. Every retry selects work from current
requirements and accepted metadata; it does not indefinitely execute an
obsolete captured target.

Failures requiring different placement, unavailable required history, or corrupt
required data fail the affected preparation bindings through OnUnrecoverable.
Unrelated successfully prepared resources remain referenced for reuse until
Release. Withdrawal and shutdown cancel their own work without reporting a new
preparation failure for a replacement View.

## 6. Physical preparation and sharing

Each operation consumes a complete immutable SegmentLoadInfo snapshot and the
exact collection runtime. Metadata lookup is outside native load execution.

Physical preparation includes:

1. preparing a private load-info copy with QueryNode-local index parameters;
2. resolving index metadata and reserving execution resources;
3. loading or reopening required fields, indexes and materialized Function data;
4. loading historical deltalogs and required PK/Bloom Filter resources;
5. validating required capabilities and publishing a pinned ReadState.

Reservations and temporary resources are released on every outcome. A reopened
Manifest is not complete until its associated historical deltas are applied.
External formats follow their storage-specific contracts; milvus-table source
deletes and real-PK filtering remain part of preparation when applicable.

Readiness is capability-based: ordinary missing nullable/default columns can
use supported projections; BM25/MinHash outputs require their actual derived
values and required statistics/indexes. A schema number is not proof of either.

### 6.1 Schema, LoadScope and physical ReadState

This resource model applies to QueryNode and SealedSegment. It does not change
StreamingNode/GrowingSegment ownership. Shared native entry points preserve
those callers through explicit compatibility adapters.

CCollection owns one immutable logical Schema per `(CollectionID, SchemaVersion)`.
LoadInfoVersion and loaded fields are not part of that identity. Published schema
content cannot change through another mutable alias. Equivalent derived caches
may be initialized with synchronization. Current compatible index metadata is
separate from immutable schema content; QueryView never binds an IndexID.

Segment operations borrow `const Schema*` from their operation context. A sealed
Segment does not store a current logical Schema, copy the complete collection
Schema, or replace its Schema during Reopen. A query context binds:

```text
borrowed Schema + immutable LoadScope + pinned physical ReadState + MVCC
```

The plan and View must agree on the logical schema. Queries never fall back to a
Segment's latest schema or invoke LazyCheckSchema to mutate shared resources.
Preparation establishes the required capabilities before Ready; execution can
use supported default/NULL projections but cannot replace missing Function
materialization with such projections.

LoadScope is independent of Schema. It distinguishes the logical field set a View
may access, supporting physical dependencies, and eager/lazy loading policy.
Having a cached physical column does not authorize a View to access it. An empty
explicit field set is empty, not an implicit request for all fields. Legacy
all-fields selection is normalized explicitly by its adapter. PK, TTL inputs,
StructArray offsets/validity and Function dependencies are included as required;
BM25 output availability does not imply eagerly loading its raw column.
Partition selection remains at the View/task layer.
Physical RowNum statistics count inserted rows minus Delete and do not interpret
View TTL. Sealed delete application deduplicates fixed row offsets; statistics
use the deletion container's atomic cardinality in O(1), without allocating a
query bitmap. Public `count(*)` uses the query's bound Schema, MVCC and TTL.
The physical segment keeps independent PK/delete-storage metadata and finalized
resource estimates; it releases the initial preparing Collection before becoming
available to Views. Subsequent operations pin their own exact Collection.


Physical ReadStates retain immutable resource generations: columns, indexes,
readers, row-domain metadata and independently owned descriptions of the required
physical fields/groups. Manifest discovery may keep lightweight storage-name
mappings for unselected columns; it does not copy their full field descriptions.
PublishedSegmentState and stored SegmentLoadInfo do not retain a borrowed logical
Schema, directly or through an alias. Reopen consumes the borrowed schema and
scope to prepare a successor and publishes only its physical resources and
necessary independently owned metadata. A retained successful result remains
valid after its preparing View and operation have disappeared.

For A requiring `{x, y}` and B requiring `{x, z}`, unchanged x can be shared,
A retains y, and B prepares z. B becoming Ready does not wait for y to be removed.
A later or earlier SchemaVersion is not itself evidence of physical compatibility.
Reuse checks content/base revision, field encoding, row domain and projection
semantics. Same SegmentID, equal row count or a greater Manifest number alone
is not a universal compatibility proof. Default projections retain their value
and type semantics; StructArray generation identity must survive name reuse.

ReadState preparation stages missing resources without mutating a published
state. Old queries retain their selected resources across Manifest or latest-index
refresh. Compatible latest indexes are pinned during execution, independently of
QueryView version. Physical mutation remains serialized by the resource owner.
Private column preparation preserves the physical row domain and never resizes
the continuously updated shared delete bitmap.

### 6.2 Borrowing and asynchronous lifetime

Each View and derived operation owns a reference to the exact CCollection and
physical resources it uses. The QueryView object itself need not remain alive
once independent operation handles have been acquired. Registry removal, Down,
Release and cancellation are not proof that native dependencies can be freed.

```text
acquire dependency handles
  -> enqueue / execute / complete asynchronous children
  -> reduce / refill fields / finish zero-copy result consumption
  -> destroy objects still borrowing dependencies
  -> release dependency handles
```

Pin acquisition precedes queueing. Queued cancellation, execution failure and
running cancellation release each reference exactly once after actual completion.
A task that continues independently acquires its own pin before detaching, or
owns all physical metadata it needs. Future completion certifies only work whose
completion is actually joined by that future.

Long-lived Reader/Translator/cache resources own necessary physical metadata;
they do not borrow FieldMeta references from a collection schema. Copying a field
or column-group descriptor is allowed and does not justify copying a complete
collection Schema. Cached preparation results follow the same ownership rule.
Any unavoidable schema-dependent derived object must explicitly retain its owner
for its full lifetime rather than rely on the original preparing operation.

The physical Segment does not pin its initial collection merely because that
collection created it. Its Delete/PK bookkeeping and physical preparation use
independent metadata. All existing physical dependencies must be detached before
removing that initial pin. Runtime destruction releases dependent objects before
their schema/physical owner handles, including exception and cancellation paths.

## 7. Metadata subscription ownership

A live SegmentResource owns its metadata subscription, independent of how many
Views bind it. The shared QueryNode stream delivers complete snapshots and
retains the last accepted revision for reconnection. Accepted metadata and
physically applied metadata are separate states.

Acknowledging a snapshot means it is retained by the resource and reconciliation
has been made responsible for it. It does not mean physical work has finished.
A snapshot arriving before schema/load dependencies is retained and becomes
usable when those dependencies resolve, without another delivery.

Subscribe installation, replacement and closure are serialized by the resource
owner. An old installation cannot replace a newer owner's subscription merely
because they have the same SegmentID. Handlers target the owning object, and
cleanup removes only the subscription it owns. Network work and Close execute
outside resource/shard locks.

On transport failure, live subscriptions resume from their accepted revisions.
On process restart, new resources subscribe for full snapshots. Coordinator
metadata publication must commit durable/in-memory state before notification;
replay and reconnection must observe committed state.

## 8. Transform coverage and readiness

### 8.1 Shared range and per-instance application

The View's shard cursor F protects the shared VChannel TransformLog range.
The trim boundary is the minimum retained View F, conservatively extended by
unfinished replay pins. Neither segment catch-up progress nor the set of segments
currently placed on this node authorizes advancing it: future balance reuses this
range. Trimming clears removed payload references in the backing-array tail as
well as the logical slice.
Each Segment's cursor C belongs to its selected physical base and identifies
its exclusive replay start. The buffer retains history required by live range
references and unfinished replay. Compatible Views reuse one buffer and its
subscription; moving a Segment within that covered range does not create an
independent remote subscription for the Segment.
The buffer owner also owns the shared PChannel stream. Canceling a subscription
attempt, including the last in-flight attempt, does not close that stream:
pending View bindings can still need it. The owner closes the stream after its
last buffer reference is released.

QueryView recovery uses TransformLog only, with no L0 loading fallback. Cursor
production and retention obligations follow the
[transform frontier contract](../transform_start_after_timetick.md).

Each registration binds a concrete physical instance. Removing or canceling a
registration affects only that object. Its history and live application form
one ordered stream; the owner keeps the instance alive until application and
cleanup have stopped.

### 8.2 Catch-up completion

Preparation chooses a finite catch-up frontier certified by TransformLog
SyncUp. It applies all required transforms through that frontier and joins the
ordered live stream without a gap. Concurrent arrivals are retained and applied
in order. Progress through empty intervals advances only after preceding
transforms have been applied.

Catch-up success proves both required historical coverage and live attachment
for this physical instance. Cancellation, replacement or absence from a pending
map is not a successful result. Shared transport progress alone cannot certify
a Segment that has not applied its required history.

A registration captures its catch-up frontier once SyncUp is available. Replay
can continue after this finite frontier is completed, using the same ordered
registration until it joins synchronous live application. Each registration
maintains an applied watermark, including certified empty intervals. A terminal
failure remains observable after the catch-up result and invalidates dependent
Views through reconciliation.

Ready is a preparation boundary, not a promise of zero lag after future writes.
Queries wait for their own Transform MVCC; that check must prove application
for their selected physical resources, not only remote receipt.

### 8.3 View readiness

A SegmentBinding becomes ready only when it owns:

- the exact schema/load runtime;
- a compatible pinned physical ReadState with required fields/indexes/base;
- completed transform coverage and valid live registration for that instance.

Publish this complete binding result before notifying ViewResources. The latter
reports incremental partition-to-Segment readiness and declares the local View
Ready only when every assigned binding and View-level dependency is ready.
An unresolved Segment cannot be omitted from the denominator.

Preparation failures are explicit results. Callbacks carry binding ownership
and cannot mark a removed or replacement binding Ready. Repeated notifications
are idempotent. Prepared resources remain protected until their owning references
are released.

## 9. Queries, release and reconstruction

A query acquires handles from the selected Ready View and waits for the required
transform watermark on those exact registrations before executing. Waiting on
a binding and then looking it up again would permit a concurrent base refresh
to substitute an instance whose application was never checked.

A query acquires handles from the selected Ready View. Each handle owns its
exact CollectionRuntime, ReadState and physical resource instance, together with
any application dependencies still needed by execution. Releasing a handle acts
on those objects; it does not look up the current Segment by ID.

View Release removes that View's bindings and range/runtime references, marks
affected resources dirty, and completes asynchronously. It cannot revoke
another View's ReadState or resources already held by executing queries.
Successful shared resources survive a failed Preparing View until its release,
and survive that release when replacement bindings already protect them.

When no View or query needs a resource, the owner enters Closing, prevents new
operations on that physical incarnation, cancels obsolete work, and drains all
operations/callbacks that can access it. It unregisters transform application,
closes its metadata subscription and releases native resources and dependency
pins in an order that preserves every outstanding reference.

Closing is not reversible for that physical incarnation. The registry retains
ownership of the closing resource until its operations and cleanup have finished.
New bindings arriving during closure are recorded for reconciliation after that
closure; destruction and reconstruction for the same logical Segment are
serialized. Detached old work cannot install a subscription or remove the newly
created instance.

`OnDropped` is invoked exactly once after the View's release obligations are
complete. Other Views or query handles may still own shared resources; their
lifetime is independent. If release starts exclusive resource destruction, its
required cleanup is completed before acknowledging that release. There are no
native waits or callback waits while holding the handler's shard mutex.

QueryNode is stateless. After restart, Coordinator-pushed requirements rebuild
bindings and resources using exact schema/load identities and full metadata
snapshots. No local scheduling flags or revision hashes become durable resource
truth. Existing ViewSync/RPC contracts remain unchanged.

## 10. Correctness and verification

### 10.1 Invariants

1. Every live View requirement has a binding, including unresolved dependencies.
2. Every physical instance has one lifecycle owner and explicit consumer/operation
   references.
3. Every unsatisfied binding has executable work, an effective dependency wakeup,
   a scheduled retry, or an explicit failure result.
4. Demand removal participates in reconciliation just as demand addition does.
5. Work and cleanup affect only their owning resource, registration and attempt.
6. Ready is backed by pinned capabilities and applied transform coverage.
7. Changes arriving during reconciliation remain scheduled afterward.
8. Callbacks, network work and destruction do not block View lifecycle locks.

With stable requirements, eventually available dependencies, sufficient capacity
and fair scheduling, these invariants ensure eventual preparation. Permanent
failures end the affected preparation obligation explicitly; they cannot produce
false Ready or silent indefinite waiting.

### 10.2 Required scenarios

| Scenario | Required assertion |
| --- | --- |
| Metadata precedes runtime resolution, or the reverse | Preparation completes without another metadata update |
| Replacement binds while earlier preparation releases | Compatible resources and complete requirements remain protected |
| Load/reopen completes after cancellation | No replacement state is modified; resources/accounting settle once |
| Subscription installation races closure/reconstruction | Current owner retains a usable metadata source |
| Removing a binding unblocks another requirement | Remaining work is scheduled without another external event |
| Old transform drain exits after a new registration | New registration is unaffected; no false catch-up success |
| Inputs change while a reconcile/task runs | A successor pass uses current requirements and metadata |
| One requirement lacks a Manifest, another is executable | Executable compatible work makes progress |
| Multiple schemas/scopes share a Segment | Correct fields/results per View, shared immutable CCollection, reuse of unchanged resources |
| Schema owner retires while physical caches remain | Cache eviction/reload and Delete use independent physical metadata |
| Original View exits before preparation completes | Reusable result has no dangling logical-schema borrow |
| Result outlives native execution | Reduce, refill and zero-copy consumption retain all dependencies |
| Same-ID Manifest update | Required content and deltas are applied before readiness |
| Query outlives View release and same-ID reconstruction | Its handles retain the exact resources and release their own owner |
| Queued cancellation, running cancellation and shutdown | No leaked operation references or unresolved release completion |
| Empty assignment, partial failure and reconnect | Complete readiness accounting and retained partial success |

Use deterministic interleavings and race tests around ownership and scheduling,
plus integration/E2E tests for returned data, schema isolation and Delete
semantics. Convergence tests stop injecting events once dependencies are available;
they must not rely on node restart or repeated View/metadata delivery. Relevant
acceptance includes non-TEI AlterCollection L0/L1 Python, REST and Go cases.

## 11. Scope and key packages

Runtime reference protection and closure are part of this design. Durable
schema/LoadInfo/Manifest/FileResource history GC remains the unified TODO in
[schema handoff](../schema_evolution.md#82-todo-unified-gc-design-deferred).
No new persistence or distributed GC protocol is introduced here.

| Package | Responsibility |
| --- | --- |
| `internal/querynodev2/qnview/` | View bindings, Segment resource ownership, reconciliation, query handles and reporting |
| `internal/querynodev2/qvresource/` | Exact collection runtime and physical loader adapters |
| `internal/querynodev2/transformlogbuffer/` | Shared history, registration identity and ordered application |
| `internal/querynodev2/segments/` | Native Segment integration and pinned ReadStates |
| `internal/core/src/segcore/` | Shared column/index resources and staged native publication |
| `internal/distributed/mixcoord/client/` | Versioned load metadata and Segment metadata stream |
| `pkg/util/nodescheduler/` | Execution and retry scheduling |
| `internal/views/worknode/handler/` | Shared ViewSync transport and state reports |
