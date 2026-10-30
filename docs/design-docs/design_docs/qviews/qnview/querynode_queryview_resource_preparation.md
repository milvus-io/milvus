# QueryNode QueryView Resource Preparation

This document defines QueryNode resource ownership, preparation, readiness and
release. Related contracts are [QueryView lifecycle](../query_view_state_machine.md),
[handler](../query_view_handler.md), schema handoff (see the source reference below),
[transform frontiers](../transform_start_after_timetick.md), and
[TransformLog delivery](../../wal/transform_log.md).

The schema handoff document is outside this extraction. Read it without changing
branches using `git show 257a76b6e71490e78e0c2730716e9a21fff30e41:docs/design-docs/design_docs/qviews/schema_evolution.md`.
Its former field-removal/prevalidation rule is superseded by the View-based
[query execution contract](../query/query_execution.md): retained serving Views
define read-schema validity; MVCC defines data visibility.

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
- each Segment's required Manifest/base; its exact replay cursor comes from the
  same fetched SegmentLoadInfo snapshot as the base that will actually be loaded;
- the shard's exclusive TransformLog retention start F.

Schema is resolved through `streaming.WAL().SchemaClient()` using the VChannel
and exact SchemaVersion. `GetQueryViewLoadInfo` resolves the exact retained
LoadInfo identity, including fields, partitions and exact IndexIDs. Physical
preparation filters Segment artifacts to this selection; it does not replace the
binding metadata with current collection indexes. See [load requirements](../collection_load_info.md). A
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
| Operation identity | A particular load, reopen, subscription or drain attempt |

A logical key is not an incarnation fence. The parent allocates durable LoadInfoVersion identities; their ordering does not
prove physical compatibility. A larger SchemaVersion alone does
not prove compatibility with another View's field, Function or query semantics.

## 3. Ownership model

```text
QueryView
  -> ViewResources
       -> CollectionRuntime reference
       -> VChannel TransformBuffer range reference
       -> SegmentBinding[]
            -> SegmentResource
                 -> on-demand metadata acquisition and retained snapshot
                 -> physical Segment and load/reopen operations
                 -> prepared ReadStates
                 -> transform registration and application progress
                 -> query and operation references
```

| Component | Responsibility |
| --- | --- |
| QueryView handler/state machine | Receives desired Views, binds/unbinds their resources, aggregates reports and implements View lifecycle |
| ViewResources | Owns one View's immutable requirements, resource references and per-binding readiness results |
| SegmentResource | Sole owner of a Segment's physical preparation and lifetime; reconciles all bindings and owns metadata acquisition, operations and transform registration |
| CollectionRuntime | Reference-counted CollectionID + SchemaVersion + LoadInfoVersion metadata binding with a separate immutable View load scope |
| TransformLogBuffer | Shares VChannel history and transport; applies transforms through instance-bound registrations |
| Metadata provider | Returns complete loading snapshots through GetQueryViewSegmentLoadInfo |
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
| Get completes | Initial load, materialization, Manifest and index work for unmet bindings |
| Operation succeeds, fails or is canceled | Actual capability, retries and other executable bindings |
| Transform progress or registration changes | Catch-up completion and valid live attachment |
| Metadata request or dependency fails | Retained work and retry/failure decisions |
| Final query/operation reference releases | Deferred destruction or waiting reconstruction |

Notifications are coalesced wakeups, not the work itself. State and dirty work
remain stored until reconciled. An event arriving during a pass is consumed by that pass or schedules an
immediate successor; completing a pass cannot erase a concurrently published change.
Use local scheduling state independently of metadata acquisition.

A resource may be idle only if all required capabilities are satisfied, or if
its unsatisfied bindings have a registered dependency wakeup or scheduled retry.
Dependency observation and wakeup registration must have no missed-update gap.

The resource's `scheduled` flag covers running or immediately queued work only.
Metadata acquisition and successful preparation immediately recheck local
state. Catch-up waits rely on their registered callbacks; metadata retries use
a resource-owned timer rather than the NodeScheduler's `ErrDelay` backoff. Each pass performs at most one
physical preparation, then yields further work to the queue tail. Failed
bindings retain independent exponential retry state, starting at 200 ms. Each
consecutive failed attempt doubles the base interval, with ±20% jitter and a
hard 5 s limit on the actual delay. A resource-owned timer wakes the earliest
pending retry without preventing other bindings or events from running.
Successful physical preparation or reuse of a ready compatible read clears that
binding's retry state. New bindings start fresh; unrelated wakeups do not bypass
or reset an existing failed binding's delay. Compatible reads are published
before checking retry deadlines, so newly satisfied requirements become Ready
immediately.
Release cancels the timer, and stale callbacks cannot wake a replacement owner.
Neither a metadata notification nor a repeated Coordinator push is required
to discover work already implied by local state.

### 5.2 Work selection

Reconciliation first publishes bindings already satisfied by retained resources,
then chooses work for unresolved bindings. It reuses complete compatible
ReadStates and prepares missing capabilities without invalidating older ones.

A binding waiting for unavailable materialization must not prevent another
compatible, executable binding from progressing. Preserve published-schema
availability and provide fair progress for pending bindings. LoadInfo version ordering cannot establish physical compatibility or justify
starving older retained Views.

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
requirements and retry with backoff. Collection/TransformBuffer dependency
preparation uses a separate exponential sequence per View, starting at 100 ms
and sharing the same doubling, jitter and hard 5 s limit. It spans the complete
dependency attempt: a successful collection acquisition followed by a failed
buffer preparation must not reset the delay. A new View starts a new sequence.
Neither retry path has an elapsed-time cutoff while its requirement is live;
Release and context cancellation interrupt waits promptly. Every retry selects work from current
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

The legacy creation adapter must not infer a QueryView scope from a nonempty
load-field list. Legacy preload choices retain schema-evolution default filling
and lazy-column preparation; only an explicit View selection restricts physical
field descriptions. Both initial loading and Reopen use the target operation's
field metadata while preparing columns and interim indexes. A newly added field
need not exist in the previously published ReadState, which remains unchanged
until preparation succeeds and the new state is published.

CCollection bindings share one immutable native Schema per
`(CollectionID, SchemaVersion)`. Each LoadInfoVersion has independent immutable
index metadata. Published schema content cannot change through another mutable
alias. Equivalent derived caches may be initialized with synchronization.

Schema-dependent Segment operations borrow `const Schema*` from their operation
context. Physical operations such as HasRawData and DropFieldData use the retained
layout. A sealed Segment does not deep-copy the collection Schema. Its layout
owns a reference to the immutable Schema needed for physical preparation/reload;
that Schema is not the authority for a query's logical schema.
A query context binds:

```text
borrowed Schema + immutable LoadScope + pinned physical ReadState + MVCC
```

Boost scoring and Arrow field refill borrow the same pinned ReadState as Search
and Retrieve. They never substitute the physical parent's latest segment. The
operation handle covers synchronous scoring, native asynchronous completion and
result conversion; these adapters introduce no separate resource owner. Boost
constructs its native query context with the plan's borrowed Schema and a captured
physical read snapshot, just like Search and Retrieve.

The plan and View must agree on the logical schema. Queries never fall back to a
Segment's latest schema or invoke LazyCheckSchema to mutate shared resources.
Preparation establishes the required capabilities before Ready; execution can
use supported default/NULL projections but cannot replace missing Function
materialization with such projections.

LoadScope is independent of Schema. It distinguishes the logical field set a View
may access, supporting physical dependencies, and eager/lazy loading policy.
Having a cached physical column does not authorize a View to access it. The
internal `required_fields` list is explicit and nonempty; legacy input is
normalized before publication. QN rejects snapshots missing this list rather
than deriving it from the bound schema. Snapshot partition IDs are authoritative,
including an empty list; QN does not reconstruct them from its local View.
User `load_fields` is only a preload hint, with
empty hints following the bound target schema. Fields outside a nonempty hint
remain accessible within required fields through prepared lazy-loading entries.
ALTER adds columns to required fields but not to explicit hints. Neither hints
nor internal physical dependencies expand logical access. PK, TTL inputs,
StructArray offsets/validity and Function dependencies are included as required;
BM25 output availability does not imply eagerly loading its raw column.
Partition selection remains at the View/task layer.
Physical RowNum statistics count inserted rows minus Delete and do not interpret
View TTL. Sealed delete application deduplicates fixed row offsets; statistics
use the deletion container's atomic cardinality in O(1), without allocating a
query bitmap. Public `count(*)` uses the query's bound Schema, MVCC and TTL.
The physical segment keeps independent PK/delete-storage metadata and finalized
resource estimates; it releases the initial preparing Collection before becoming
available to Views. The Go View owns its Collection until all operations finish.


Physical ReadStates retain immutable resource generations: columns, indexes,
readers and row-domain metadata. PublishedSegmentState owns a shared immutable
SealedFieldLayout. The layout owns a shared Schema reference, an IndexMeta
reference, a LoadScope value and derived field/column lookup data; it does not
copy FieldMeta. Collection caches layouts weakly by schema/index identity and
scope selection. SegmentLoadInfo does not own a duplicate layout.

Creation/reopen acquires the target layout before asynchronous execution. A
reopened private generation retains its target layout; failure leaves the old
state intact. Base adoption shares the adopted PublishedState and its layout.
Thus B reusing A's read remains safe after A releases its View and CCollection:
B and the physical state still retain the required metadata. A physical cache
can extend Schema lifetime beyond all Views of that Schema without retaining
those Views or their other Segment/buffer bindings.

Queries and output conversion use their operation's Plan Schema and pinned
ReadState. Arrow types and names also come from that Plan Schema. Native work,
result conversion and cancellation must finish before releasing their handles.
Background decoding resources own their descriptors or metadata references.
Delete owns stable PK identity/type. Final resource destruction releases the
states/layouts; the last shared metadata owner destroys Schema and IndexMeta.
A narrow layout can keep the full shared Schema alive, so memory savings are
not guaranteed for every field-selection pattern.

For A requiring `{x, y}` and B requiring `{x, z}`, unchanged x can be shared,
A retains y, and B prepares z. B becoming Ready does not wait for y to be removed.
A later or earlier SchemaVersion is not itself evidence of physical compatibility.
Reuse checks content/base revision, field encoding, row domain and projection
semantics. Same SegmentID, equal row count or a greater Manifest number alone
is not a universal compatibility proof. Default projections retain their value
and type semantics; StructArray generation identity must survive name reuse.

ReadState preparation stages missing resources without mutating a published
state. Old queries retain their selected resources while a new View prepares Manifest
or index changes. Compatible latest indexes are pinned during execution, independently of
QueryView version. Physical mutation remains serialized by the resource owner.
Private column preparation preserves the physical row domain and never resizes
the continuously updated shared delete bitmap.

### 6.2 Borrowing and asynchronous lifetime

The Go View owns its CCollection. Queries and synchronous preparation increment
that View's active-operation count before releasing the manager lock. Release
withdraws the View, but its Collection is released only after the last operation
finishes. No additional per-Segment/per-operation Collection.Pin is needed.
Physical ReadState references still protect column/index generations across
Reopen; those references are independent of Schema ownership.

```text
acquire View operation -> borrow CCollection Schema -> execute native operation
  -> join native children and output conversion -> release View operation
```

CSegment wrappers wait for their native futures; Future.Release also waits for
actual completion on cancellation. SealedSegment and helpers must not retain
borrowed Schema pointers beyond that synchronous boundary. Detached cache
warmup/reload owns the physical descriptors it needs, not a borrowed Schema.

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

## 7. On-demand loading metadata

This path assumes StorageV3 with index artifacts published into the Segment
Manifest. `GetQueryViewSegmentLoadInfo` is a unary batch request for Segment IDs
in one Collection. It returns one result per distinct requested ID, including a
per-Segment status and, on success, a complete current SegmentLoadInfo. Collection
index definitions are included once for the batch. The response-level status is
reserved for request-wide failures; a missing Segment or an uncertified replay
cursor must not discard successful siblings. An empty ID list succeeds with no
results. It does not promise historical SegmentInfo lookup. Manifest,
resource statistics and the certified Transform replay cursor must describe the
same base; callers cannot combine a Manifest with a separately fetched cursor.

A live SegmentResource shares the accepted snapshot across compatible View
bindings. Reconciliation first reuses a compatible prepared ReadState. If an
unmet binding needs a Manifest newer than the cached snapshot, or no snapshot
exists, it performs Get outside resource/shard locks. NodeScheduler bounds
concurrent resource tasks, and the RPC has a timeout. The QN metadata adapter
coalesces pending requests for the same Collection for up to 1 ms or 128 callers,
deduplicates Segment IDs, then distributes individual results back to their
resource owners. Different Collections never share a batch. A failed request retries
only while a live unmet requirement needs it; physical load retries retain their
accepted metadata rather than repeatedly calling the coordinator.

Successful preparation freezes the binding's ReadState. There is no metadata
polling or same-View index-refresh subscription. Manifest publication requests
DataView reconciliation; a new QueryView drives any subsequent resource update.
Old Views and in-flight queries retain their original reads while the new View
prepares. Schema and LoadInfoVersion remain exact; indexes are selected from
compatible available metadata at preparation and are not a new View identity.

Ordinary sealed bases retain their minimum-Manifest rule. External bases require
the exact requested Manifest because their row domain can change. A current
snapshot newer than an unavailable historical external requirement fails that
unprepared binding, rather than substituting the current base or polling forever.
Existing historical reads remain valid. Reconstructing historical external
SegmentInfo is outside this interface's current capability.

Get belongs to the resource owner, not its first View. Releasing one View cannot
cancel another binding's shared acquisition. Final demand removal cancels the
resource waiter; the RPC is canceled when its last waiter leaves (or the node
shuts down). Canceling one resource must not cancel siblings in the same batch.
The closing owner drains its waiter and ignores any late response before a
replacement owner can be installed. One Segment's request failure does not close
a shared transport session or invalidate unrelated ready resources.

Coordinator publication commits catalog and in-memory state before requesting
DataView reconciliation. Removing SegmentMeta Watch must preserve this trigger.
The existing recompute queue is best-effort; queue saturation/recovery behavior
belongs to the DataView producer and is not repaired by periodic QN polling.

## 8. Transform coverage and readiness

### 8.1 Shared range and per-instance application

The View's shard cursor F protects the shared VChannel TransformLog range.
The trim boundary is the minimum retained View F, conservatively extended by
unfinished replay pins. Neither segment catch-up progress nor the set of segments
currently placed on this node authorizes advancing it: future balance reuses this
range. Catch-up completion and registration removal immediately reconsider this
boundary, while preserving all remaining View and replay pins. Trimming clears
removed payload references in the backing-array tail as well as the logical slice.
Each Segment's cursor C belongs to its actual physical base and identifies
its exclusive replay start. It is fetched atomically with Manifest/binlogs in
SegmentLoadInfo, never inferred from DeltaPosition/StartPosition. QueryView does
not duplicate per-Segment cursors. A normal sealed View specifies a minimum
Manifest version; external bases require the exact Manifest. An existing base
keeps its original cursor/transform registration when compatible data resources
are reused; a different fetched cursor never rewrites that registration. The buffer retains history required by live range
references and unfinished replay. Compatible Views reuse one buffer and its
subscription; moving a Segment within that covered range does not create an
independent remote subscription for the Segment.
The buffer owner also owns the shared PChannel stream. Canceling a subscription
attempt, including the last in-flight attempt, does not close that stream:
pending View bindings can still need it. The owner closes the stream after its
last buffer reference is released. A shared subscription attempt belongs to the
buffer references, not to its first caller: canceling one caller ends only that
caller's wait. Final reference release cancels pending subscription work, and any
late successful subscription is closed without affecting a replacement buffer.

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
Views through reconciliation. Apply failure poisons only that concrete physical
registration: it stops applying entries and rejects subsequent query acquisition
at every MVCC, including historical timestamps. Other registrations on the same
VChannel continue applying the shared log. Replay and live application use the
same failure boundary; releasing an instance still drains any in-flight Apply.
A late failure cannot poison a replacement instance with the same SegmentID.
Physical subscription/history failures resume inside the transport, preserving
logical subscriptions and accepted cursors. A terminal logical-stream Done/Error
fails every retained VChannel buffer using that stream, including references
arriving after its termination. Handler events carry only Entry/SyncUp; closing
one subscription does not fail siblings.
This extraction does not preserve historical-query availability on a poisoned
instance or add a Coordinator Poison state or an in-place recovery protocol.

Ready is a preparation boundary, not a promise of zero lag after future writes.
Queries wait for their own Transform MVCC; that check must prove application
for their selected physical resources, not only remote receipt.

The QueryNode-wide catch-up worker count is
`max(1, floor(hardware.GetCPUNum() * ratio))`, configured by
`queryNode.queryView.transformLogDrainConcurrencyRatio` (default `0.25`).
Configuration changes resize the existing pool. Shrinking completes in-flight
pages and preserves queued registrations; growing starts more workers. The node
context cancellation unregisters the watcher and stops the pool. There is no
separate Segment catch-up worker pool.

Catch-up applies bounded pages on a shared worker pool, yielding after each
page. A registration awaiting transport coverage parks its state and releases
the worker. Arrivals schedule pending registrations under the same VChannel
lock used for the final empty check and attachment to live dispatch. A
queued/running bit coalesces work; parking clears it under that lock so an
arrival cannot lose its wakeup. Replay and live application both honor the
instance's exclusive cursor, after checking its terminal failure state.

After the finite readiness frontier, the resource installs a terminal-failure
callback and ends its temporary catch-up goroutine. Installation observes an
already recorded failure as well. The callback runs outside the error-state
lock and only schedules reconciliation; it never waits for application or
unregisters the instance inline. Release still joins in-flight application.

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
drains metadata acquisition and releases native resources and dependency
pins in an order that preserves every outstanding reference.

Closing is not reversible for that physical incarnation. The registry retains
ownership of the closing resource until its operations and cleanup have finished.
New bindings arriving during closure are recorded for reconciliation after that
closure; destruction and reconstruction for the same logical Segment are
serialized. Detached old work cannot install a metadata snapshot or remove the newly
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
| Multiple schemas/scopes share a Segment | Correct fields/results per View, shared parsed Schema with versioned index bindings, reuse of unchanged resources |
| Schema owner retires while physical caches remain | Retained layouts own Schema/index metadata; Delete keeps independent PK state |
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
schema handoff (`docs/design-docs/design_docs/qviews/schema_evolution.md` in source snapshot `257a76b6e71490e78e0c2730716e9a21fff30e41`).
No new persistence or distributed GC protocol is introduced here.

| Package | Responsibility |
| --- | --- |
| `internal/querynodev2/qnview/` | View bindings, Segment resource ownership, reconciliation, query handles and reporting |
| `internal/querynodev2/qvresource/` | Exact collection runtime and physical loader adapters |
| `internal/querynodev2/transformlogbuffer/` | Shared history, registration identity and ordered application |
| `internal/querynodev2/segments/` | Native Segment integration and pinned ReadStates |
| `internal/core/src/segcore/` | Shared column/index resources and staged native publication |
| `internal/distributed/mixcoord/client/` | Versioned collection metadata and unary Segment loading snapshots |
| `pkg/util/nodescheduler/` | Execution and retry scheduling |
| `internal/views/worknode/handler/` | Shared ViewSync transport and state reports |
