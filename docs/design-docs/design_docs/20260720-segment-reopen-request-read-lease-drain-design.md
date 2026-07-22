# Segment Reopen Request-Level ReadLease and Sealed Drain Design

## Document Information

- Date: 2026-07-20
- Status: Draft for final design review
- Language: English (canonical)
- Components: Segcore / QueryNode
- Primary scope:
  - `internal/core/src/segcore/ChunkedSegmentSealedImpl.{h,cpp}`
  - `internal/core/src/segcore/segment_c.cpp`
  - `internal/core/src/segcore/search_result_export_c.cpp`
  - `internal/core/src/common/QueryResult.h`
  - `internal/querynodev2/tasks/search_task.go`
- Related documents:
  - [Chinese version](./20260720-segment-reopen-request-read-lease-drain-design_zh.md)
  - [Segment Reopen Atomic Read & Update with Copy-on-Write](./20260627-segment-reopen-atomic-read-update-cow.md)
  - [Segment Reopen Review Page](./20260627-segment-reopen-generation-review/index.html)

---

## 1. Summary

This design retains the existing `PublishedSegmentState` and runtime
copy-on-write publication framework in `ChunkedSegmentSealedImpl`, but narrows
the serving consistency boundary for sealed segments to a
**request-level read lease**:

- A growing segment returns a no-op read lease.
- `AsyncSearch` on a sealed segment acquires a cross-thread-safe
  `SegmentReadLease`.
- The lease is transferred to `SearchResult` and remains alive until
  `DeleteSearchResult`.
- Reduce, export, and output-field fill reuse the lease already owned by the
  original `SearchResult`. They do not acquire a new lease.
- Before publication, Reopen marks a writer as pending and waits for all
  existing request leases to be released.
- New escaping Search lease acquisition fails fast while the publisher is
  pending or active. QueryNode releases every partial `SearchResult` from the
  rejected attempt, then retries the complete sealed segment batch with jitter
  under the request context.
- New call-scoped Retrieve lease acquisition fails fast while publication is
  pending or active. QueryNode retries only that Retrieve call with jitter under
  the request context. Growing reads remain no-op.
- After the drain completes, Reopen atomically publishes the complete metadata
  and runtime snapshot exactly once.

As a result, one search request observes one sealed snapshot from Segcore
search through reduce, export, and output-field fill.

This design explicitly stops supporting the following behavior:

> Different CGO phases of the same search request may observe different sealed
> snapshots.

The design accepts a longer Reopen publication wait in exchange for:

- a simpler consistency model;
- a much smaller reader-accessor migration surface;
- no whole-snapshot pin solely for Reopen lifetime;
- a direct proof that schema, readiness, and runtime do not change during a
  request.

---

## 2. Background and Problem Statement

The sealed segment implementation is already moving schema, load information,
readiness, and runtime resources into `PublishedSegmentState`. Writers clone
the current state, prepare the next state, and publish it with an atomic store.

With a call-scoped read guard, the following sequence is possible:

```text
AsyncSearch reads S0
        ↓
Search returns and releases the guard
        ↓
Reopen publishes S1
        ↓
PrepareSearchResults / FillOutputFields reads S1
```

This can be made logically valid with strict row-mapping invariants, but
`SearchResult` may also carry iterators, offset mappings, or cache-backed
resources created from S0. The ownership model then becomes part of every
escaped resource.

Because Reopen is infrequent and publication is allowed to temporarily reject
and retry new sealed searches, this design extends read protection through the
entire `SearchResult` lifetime.

---

## 3. Goals

### 3.1 Functional Goals

1. One sealed search request observes one `PublishedSegmentState`.
2. Reopen drains all old search requests before publishing.
3. Once a writer is pending, new sealed searches fail admission and cannot
   bypass it; QueryNode retries only after releasing the entire attempt.
4. Call-scoped Retrieve reads fail admission behind a pending or active
   publisher and retry only the current segment call without blocking a Segcore
   executor thread.
5. Growing segments do not receive a new operation gate.
6. Reopen preparation remains non-blocking for readers; only final publication
   performs the drain.
7. Reopen performs one externally visible publication.
8. `LazyCheckSchema` remains available as a transitional mechanism, runs
   before gate acquisition, and fails fast instead of waiting for either the
   publication gate or another reopen.
9. Cancellation, timeout, and error paths cannot leak a lease or leave the read
   gate closed.

### 3.2 Engineering Goals

1. Do not directly replace the existing `mutex_` with a writer-priority mutex.
2. Avoid adding SegmentInterface virtual methods and the associated CGO vtable
   layout risk.
3. Allow a lease to be acquired and released on different threads.
4. Encode the publisher contract in APIs instead of relying on comments.
5. Provide production visibility into long leases and long drains.

---

## 4. Non-Goals

This design does not attempt to:

- keep an old request running after a new snapshot has been published;
- apply request-level drain semantics to growing segments;
- immediately remove all historical sealed-segment mutexes;
- use the segment gate to solve independent cache-manager eviction ownership;
- immediately move all schema synchronization out of the search executor;
- support a Reopen that changes row count, row order, or offset-to-PK mapping.

---

## 5. Terminology

### 5.1 Published Snapshot

A complete reader-visible `PublishedSegmentState`:

```text
PublishedSegmentState
  ├─ schema
  ├─ load_info
  ├─ commit_ts
  ├─ readiness
  └─ runtime resource graph
```

### 5.2 SegmentReadLease

A request-scoped claim on the currently published sealed snapshot.

The lease does not select an arbitrary historical snapshot. Its contract is:

> While the lease is alive, the sealed segment cannot publish its next
> snapshot.

### 5.3 Writer Pending

A writer has completed, or nearly completed, preparation and is ready to
publish:

- new escaping Search leases fail admission;
- new call-scoped Retrieve leases fail admission without entering;
- existing leases may continue reduce and fill work;
- the writer waits for the active lease count to reach zero.

---

## 6. Consistency Model

### 6.1 Growing Segment

`AcquireSegmentReadLease` returns an empty lease for a growing segment.

Growing continues to rely on its existing synchronization:

- `mutex_`;
- `sch_mutex_`;
- `chunk_mutex_`;
- InsertRecord internal locks.

This design does not change the lock relationship among Insert, AddTexts,
Growing Reopen, and query execution.

### 6.2 Sealed Segment

Sealed search lifetime:

```text
LazyCheckSchema
        ↓
Acquire SegmentReadLease for S0
        ↓
Search
        ↓
Prepare / Reduce / Export / Fill
        ↓
DeleteSearchResult
        ↓
Release SegmentReadLease
```

Reopen lifetime:

```text
Prepare S1 while the gate is open
        ↓
writer_pending = true
        ↓
reject new sealed search leases
        ↓
wait for active_readers == 0
        ↓
publish S1 exactly once
        ↓
open the gate
```

### 6.3 Request-Level Guarantee

For one sealed segment:

- schema, load information, readiness, and runtime remain stable after lease
  acquisition;
- search offsets, PK fill, refine, and output fill all operate on the same
  snapshot;
- a new snapshot can only be published after the request releases its lease.

---

## 7. Invariants

Even though the request lease prevents cross-snapshot reads, Reopen must still
preserve:

1. segment ID;
2. row count;
3. row order;
4. segment offset to PK mapping;
5. insert-timestamp semantics;
6. the data type, vector dimension, and primary-key meaning of an existing
   FieldID;
7. readiness/runtime consistency;
8. prepare isolation from the currently published state.

Any operation that changes row layout or PK mapping must use full segment
replacement instead of this Reopen protocol.

---

## 8. Gate Data Structure

### 8.1 Why Not Store a Folly Shared Lock in SearchResult

`AsyncSearch` may acquire a lease on a Segcore search executor thread, while
`DeleteSearchResult` may execute on a different CGO thread.

Therefore, `SearchResult` must not own:

```cpp
std::shared_lock<folly::SharedMutexWritePriority>
```

and release it from another thread. Request-level protection should use a
cross-thread-safe reference-counted token.

### 8.2 GateState

Conceptual structure:

```cpp
struct GateState {
    std::mutex mutex;
    std::condition_variable cv;

    uint64_t active_readers = 0;
    bool writer_pending = false;
    bool writer_active = false;

    // Optional observability state.
    uint64_t blocked_readers = 0;
    std::chrono::steady_clock::time_point oldest_reader_start;
};
```

`ChunkedSegmentSealedImpl` and all outstanding leases share ownership of:

```cpp
std::shared_ptr<GateState> operation_gate_;
```

This separates the gate lifetime from the segment object lifetime and prevents
a lease destructor from dereferencing an already-destroyed segment.

### 8.3 SegmentReadLease

```cpp
class SegmentReadLease {
 public:
    SegmentReadLease() = default;  // Growing/no-op.
    SegmentReadLease(SegmentReadLease&&) = default;
    SegmentReadLease& operator=(SegmentReadLease&&) = default;
    ~SegmentReadLease();

    bool
    valid() const;

 private:
    explicit SegmentReadLease(std::shared_ptr<GateState> state);

    std::shared_ptr<GateState> state_;
};
```

Escaping Search read acquisition:

```text
lock gate mutex
if writer_pending || writer_active:
    increment rejection counter
    return the narrow transient gate-busy error
active_readers++
unlock gate mutex
```

Escaping read admission must not wait inside one segment while the same
QueryNode attempt retains leases from other segments. Otherwise two requests
that visit segments in opposite orders can retain partial leases and deadlock
two publishers. The Search retry boundary is therefore the complete sealed
segment batch, not one segment or one goroutine.

Call-scoped Retrieve acquisition uses the same fail-fast gate operation:

```text
lock gate mutex
if writer_pending || writer_active:
    increment rejection counter
    return the narrow transient gate-busy error
active_readers++
unlock gate mutex
```

Because Retrieve releases its lease before the CGO call returns, QueryNode can
retry only the rejected segment call. No reader waits inside the Search CPU
executor, so queued Retrieve work cannot prevent Search consume callbacks from
transferring results to Go for eventual lease release.

Read release:

```text
lock gate mutex
active_readers--
if active_readers == 0:
    notify writer
unlock gate mutex
```

### 8.4 PublishLease

Writer acquisition:

```text
lock gate mutex
check cancellation
writer_pending = true
wait, with cancellation polling and a bounded deadline, until
    active_readers == 0 && !writer_active
check cancellation again
writer_active = true
unlock gate mutex
```

The second cancellation check is the cancellation linearization point. A
cancellation observed by that check aborts publication and reopens the gate.
Cancellation arriving after the check may not prevent publication and does not
roll back a publication that has already been admitted.

The production drain deadline defaults to 30 seconds, including callers that
do not provide an `OpContext`. Cancellation or timeout clears
`writer_pending`, wakes waiters, discards the staged state, and leaves the old
snapshot active.

Writer release:

```text
lock gate mutex
writer_active = false
writer_pending = false
notify all readers
unlock gate mutex
```

All online publishers must first serialize on `reopen_mutex_` before entering
the writer protocol. The mutex may be renamed to `publication_mutex_` after
publisher convergence, but the single-publisher property is required: a
boolean `writer_pending` is not a correct queue for multiple concurrent
publishers.

---

## 9. Read Path

### 9.1 AsyncSearch

Recommended order:

```cpp
milvus::OpContext op_ctx(cancel_token);

// Reopen is allowed before entering the gate.
segment->LazyCheckSchema(plan->schema_, &op_ctx);

// The request snapshot is stable from this point.
auto lease = AcquireSegmentReadLease(segment);

// Only pure reads and validation are allowed inside the gate.
auto snapshot = CapturePublishedStateForValidation(segment);
ValidatePlanAgainstSnapshot(plan, snapshot);

CheckExternalFieldsInLoadedManifest(...);

std::unique_ptr<SearchResult> result;
if (!filter_only && !segment->FieldAccessible(target_field)) {
    result = BuildEmptySearchResult(...);
} else {
    result = segment->Search(...);
}

result->read_lease_ = std::move(lease);
return result;
```

Every successfully returned result must receive the lease:

- normal search results;
- inaccessible-target empty results;
- filter-only results;
- iterator and group-by results;
- zero-hit results.

If an exception occurs before result ownership is established, the local lease
releases through RAII.

### 9.2 LazyCheckSchema Before the Gate

`LazyCheckSchema` must execute before lease acquisition.

Executing it inside the gate would produce deterministic self-deadlock:

```text
request increments active_readers
        ↓
LazyCheckSchema calls Reopen
        ↓
Reopen waits for active_readers == 0
        ↓
the request waits for itself
```

The lazy path must also avoid blocking on older requests. Search result consume
callbacks run on the same Search CPU executor as AsyncSearch and Retrieve. If a
lazy reopen waits for an old lease while enough newer-schema requests occupy
the executor, those callbacks cannot run and the old lease cannot be released.

Sealed LazyCheckSchema therefore uses a fail-fast publication mode:

```text
compare schema version
        ↓
try_lock reopen_mutex; fail gate-busy if another reopen owns it
        ↓
recheck schema version
        ↓
if readers or a publisher are already present: fail gate-busy
        ↓
prepare the staged schema/runtime snapshot
        ↓
atomically try to acquire publication with active_readers == 0
        ↓
publish, or discard the staged snapshot and fail gate-busy
```

The early gate check avoids known-unnecessary preparation but is not the
linearization point: a reader may enter during preparation, so final
publication must repeat the check while holding the gate mutex. A failed lazy
publication does not set `writer_pending` and leaves the old snapshot readable.
QueryNode handles the narrow gate-busy signal using the existing whole-batch
Search retry or per-segment Retrieve retry.

Explicit Reopen continues to serialize with a blocking `reopen_mutex_` and use
bounded drain on the Load executor. Only the read-triggered lazy path is
fail-fast.

### 9.3 Race Between Lazy Check and Gate Acquisition

The following sequence is allowed:

```text
LazyCheckSchema publishes S1
        ↓
another writer publishes compatible S2
        ↓
the request acquires a lease on S2
```

Therefore, LazyCheckSchema only guarantees:

```text
acquired snapshot schema version >= plan schema version
```

It does not guarantee that the acquired snapshot is exactly the one published
by the request's own lazy reopen.

The race is valid only if:

- schema versions are monotonic;
- schema rollback is not allowed;
- a newer snapshot remains forward-compatible with an older plan;
- existing FieldID types and vector dimensions cannot change;
- dropped/default-field behavior has explicit compatibility semantics.

### 9.4 Post-Acquisition Compatibility Validation

After lease acquisition, validate the stable snapshot without mutation:

1. `snapshot.schema_version >= plan.schema_version`;
2. the target vector FieldID exists or follows the explicit inaccessible path;
3. vector type and dimension are compatible with the plan;
4. plan access entries are serviceable by the snapshot;
5. dropped/default fields follow a supported path;
6. external manifest state includes all required storage columns;
7. readiness and runtime resources describe the same visible state.

If the snapshot is unexpectedly older than the plan:

```text
release lease
        ↓
retry LazyCheckSchema
        ↓
reacquire lease
```

Reopen must never execute while a request lease is held.

If the snapshot is newer but incompatible:

- do not repair it inside the gate;
- return a well-defined error or trigger an upper-layer plan rebuild/retry;
- finalize the error and retry contract before implementation.

### 9.5 Later Search CGO Phases

The following entry points must not acquire another lease:

- `PrepareSearchResultsForExport`;
- `ExportSearchResultAsArrowRecordBatch`;
- `FillOutputFieldsOrdered`;
- worker tasks launched by reduce or fill.

They rely on the original `SearchResult` remaining alive and validate that a
sealed result already owns a valid lease.

Temporary `SearchResult` objects borrow protection from the original result;
they do not copy or release lease ownership.

Reacquisition would deadlock:

```text
old SearchResult holds a lease
writer_pending waits for the old lease
Fill tries to acquire a new lease and waits for the writer
Fill cannot finish
the old lease cannot be released
```

### 9.6 Retrieve and Direct Read APIs

`AsyncRetrieve`, `AsyncRetrieveByOffsets`, and direct read APIs without a
long-lived C++ result use a fail-fast call-scoped lease with per-call retry:

```text
LazyCheckSchema before the gate, when applicable
attempt fail-fast lease acquisition
if gate busy: return without entering and retry this segment call in QueryNode
validate
read and serialize
release before returning across CGO
```

The serialized protobuf no longer refers to the sealed snapshot, so Query
reduce and QueryStream send do not retain the lease. Separate Retrieve calls
may observe different compatible snapshots; the Reopen row/offset invariants
remain mandatory.

`GetRowCount`, `GetRealCount`, `HasRawData`, `HasFieldData`, and similar APIs
must be audited according to whether they consume published/runtime state.

---

## 10. SearchResult Ownership

### 10.1 Proposed Field

`QueryResult.h` can forward-declare the lease token and let `SearchResult` own
either:

```cpp
std::shared_ptr<segcore::SegmentReadLease> read_lease_;
```

or a move-only lease directly, depending on the final audit of
`SearchResult` move and copy behavior.

Requirements:

- a growing result may have an empty lease;
- a sealed result must have a valid lease;
- the lease is released exactly once;
- temporary results cannot release the original result's lease.

### 10.2 Release Point

`DeleteSearchResult` remains the ownership boundary:

```cpp
void
DeleteSearchResult(CSearchResult search_result) {
    delete static_cast<SearchResult*>(search_result);
}
```

Destroying `SearchResult` releases the lease.

No separate “release lease” C API should be added because it would create
double-release and missed-release risks across Go and C++.

---

## 11. Publish Path

### 11.1 Reopen Flow

```text
lock reopen_mutex_
capture current state
compute load/schema diff
prepare cloned runtime
load/build/drop resources in staged state
normalize final state
compact manifest load info in staged state
freeze next state and retired resource list
release committer/internal/cache/reader locks
set writer_pending
wait for active_readers == 0
atomic_store final state exactly once
clear writer_pending and wake readers
retire old resources / cancel warmup
unlock reopen_mutex_
```

### 11.2 Locks Forbidden During Drain

While waiting for active requests, a publisher must not hold:

- `mutex_`;
- `reader_mutex_`;
- the StagedStateCommitter mutex;
- cache accessor or cache slot internal locks;
- any other lock required by reduce/fill of an active request.

Otherwise:

```text
writer holds an internal mutex
        ↓
writer waits for active request lease
        ↓
active request waits for the internal mutex
```

and the drain cannot complete.

### 11.3 Single Publication

Current Reopen paths may call `CompactRuntimeLoadInfoForManifest()` after
`committer.Publish(...)`, producing a second publication.

This design requires:

- compacting manifest load information in the staged state;
- one atomic store after the drain;
- readers observing either the complete old state or the complete new state.

### 11.4 All Online Publishers

The gate must cover every serving-time publication, not only Reopen:

- field and index readiness;
- runtime resource publication;
- field and index drop;
- text, JSON, and interim index publication;
- `SetCommitTimestamp`;
- serving-time `SetLoadInfo`;
- `ClearData`;
- other `PublishState` helpers.

Initial load may use an explicit initialization publication only when a
lifecycle assertion proves that the segment is not yet visible to readers.

Recommended API split:

```cpp
PublishStateUnsafe(...);           // Constructor/initialization only.
PublishState(PublishLease&, ...);  // Online serving publication.
```

Ordinary helpers should not acquire the writer gate implicitly. Hidden gate
acquisition could begin a drain while the caller still owns an old mutex.

### 11.5 Cancellation

Writer wait must be cancellation-aware:

- cancellation is checked before setting writer pending, while draining, and
  once more after the final reader leaves;
- cancellation observed by the final pre-publication check clears writer
  pending, wakes readers, discards staged state, and keeps the old snapshot
  active;
- that final check is the cancellation linearization point; cancellation
  arriving later may not prevent publication and does not roll it back;
- a bounded drain timeout applies even when `OpContext` is null and follows the
  same rollback path;
- cancellation cleanup cannot hold locks required by active requests.

---

## 12. Relationship With Existing Mutexes

The first implementation keeps the existing shared `mutex_` acquisition in
`SegmentInternalInterface::Search/Retrieve`.

Sealed search therefore uses:

```text
Acquire request lease
        ↓
SegmentInternalInterface::Search
        ↓
mutex_ shared lock
```

These are different mechanisms:

- the request lease controls publication;
- `mutex_` continues to protect live state that has not completed snapshot
  migration.

Growing receives a no-op lease and continues to use only its existing locks.

Historical sealed mutexes may be removed incrementally after all
reader-visible live state and accessors are audited. That cleanup is not a
prerequisite for the first drain implementation.

---

## 13. Resource Lifetime

### 13.1 Snapshot Runtime

The request lease prevents Reopen publication, so the active
`PublishedSegmentState` and runtime graph are not replaced during the request.

Therefore, `SearchResult` does not need a separate whole-snapshot pin solely
for Reopen lifetime.

### 13.2 Cache Cell Exception

The cache manager may evict resources independently from Segment Reopen.

The current sealed index search uses a local cache accessor while a Knowhere
iterator may survive beyond the search CGO call.

The implementation must determine:

- whether the Knowhere iterator independently owns the underlying index;
- whether a cache cell can be evicted after the accessor is destroyed;
- whether `OffsetMapping*` points into an independently evictable object.

If the iterator is not self-owning:

- the iterator wrapper must retain the cache accessor; or
- `SearchResult` must retain the corresponding cache cell pin.

This is a cache-lifetime pin, not a snapshot-selection pin.

---

## 14. Go Lifetime

The current successful QueryNode search path effectively registers:

```go
defer Segment.Unpin(searchedSegments)
...
defer DeleteSearchResults(results)
```

Go defers execute in LIFO order, so successful requests release
`SearchResult` and its lease before unpinning the segment.

For a multi-segment sealed attempt, fail-fast admission may leave successful
partial results from other goroutines. The error path must therefore release
all non-null results only after all attempt goroutines have completed, then
retry the complete segment batch:

```go
for {
    results, err := searchSegmentsAttempt(...)
    if err == nil {
        return results, nil
    }
    DeleteSearchResults(nonNil(results))
    if !isSealedReadGateBusy(err) {
        return nil, err
    }
    waitJitteredBackoff(ctx)
}
```

The gate-busy classifier must require both the Segcore
`FollyOtherException` code and the stable `segment read gate busy` marker so
unrelated 2037 errors are not retried. Growing searches and other errors return
immediately.

The final contract must guarantee lease release for:

- partial-result errors;
- filter-only early return;
- cancellation;
- serialization failure.

---

## 15. File-Level Change List

### 15.1 New or Extracted Gate Types

Suggested files:

- `internal/core/src/segcore/SegmentReadLease.h`;
- `internal/core/src/segcore/SegmentReadLease.cpp`.

Responsibilities:

- GateState;
- SegmentReadLease;
- PublishLease;
- dynamic-cast-based acquire factory;
- metrics hooks.

### 15.2 ChunkedSegmentSealedImpl.h

- add `shared_ptr<GateState> operation_gate_`;
- add non-virtual fail-fast `AcquireReadLease`;
- add writer acquisition;
- require an explicit PublishLease token for online publication;
- add test hooks.

### 15.3 ChunkedSegmentSealedImpl.cpp

- separate Reopen prepare and publish;
- move manifest compaction before publication;
- route all online publishers through the gate;
- release internal locks before drain;
- implement cancellation-aware writer wait;
- add generation and wait metrics.

### 15.4 segment_c.cpp

- run `LazyCheckSchema` before gate acquisition;
- perform pure snapshot compatibility validation after acquisition;
- transfer the lease to every `AsyncSearch` result branch;
- use fail-fast scoped leases for retrieve;
- audit direct read C APIs;
- release the lease through `DeleteSearchResult` destruction.
- expose cancellable async index-drop entry points so the request context
  reaches online publication drain.

### 15.5 search_result_export_c.cpp

- do not acquire a new lease;
- validate that sealed results already own a lease;
- keep original results alive through worker completion;
- let temporary results borrow protection.

### 15.6 QueryResult.h

- add lease ownership;
- audit move, copy, and destruction behavior;
- add a cache cell pin only if required by the iterator audit.

### 15.7 search.go and services.go

- guarantee result deletion before segment unpin;
- release all partial results before whole-batch gate retry;
- use bounded jittered retry under the request context;
- propagate `DropIndex` publication cancellation/timeout instead of returning
  success.

---

## 16. Delivery Phases

### Phase 1: Gate and Request Lifetime

- implement GateState and SegmentReadLease;
- add growing no-op behavior;
- attach lease ownership to SearchResult;
- forbid reacquisition in later phases;
- add basic concurrency tests.

### Phase 2: Publisher Convergence

- make Reopen publish once;
- move manifest compaction before publication;
- audit all online `PublishState` call sites;
- add cancellation and lock-order tests.

### Phase 3: Schema and Cache Completeness

- implement pre-gate LazyCheckSchema and post-gate validation;
- add schema compatibility tests;
- audit iterator/cache ownership;
- harden Go partial-result cleanup;
- add metrics and stress tests.

### Phase 4: Historical Lock Cleanup

- audit the remaining responsibilities of sealed `mutex_`;
- remove only locks fully covered by immutable snapshots or request leases;
- do not change the growing lock model.

---

## 17. Observability

At minimum, expose:

- `segcore_segment_active_read_leases`;
- `segcore_segment_oldest_read_lease_seconds`;
- `segcore_segment_publish_drain_wait_seconds`;
- `segcore_segment_writer_pending_seconds`;
- `segcore_segment_blocked_read_requests_total`;
- `segcore_segment_published_generation`.

Recommended structured log fields:

- segment ID;
- current and target schema versions;
- published generation;
- active lease count;
- oldest lease age;
- drain wait duration;
- cancellation state.

Normal read-lease acquire/release must not emit INFO logs per query.

---

## 18. Verification Plan

### 18.1 Gate Behavior

1. Reopen remains blocked after Search returns.
2. Reopen completes after `DeleteSearchResult`.
3. New sealed Search fails fast and cannot bypass a pending writer.
4. A call-scoped Retrieve fails fast while a publisher is pending or active.
5. QueryNode retries only the rejected Retrieve call and obeys request cancellation.
6. Non-gate Folly errors are not retried.
7. Reduce/fill under an existing lease completes after writer pending.
8. Growing Search and Retrieve do not increment the active lease count.

### 18.2 Lazy Schema

1. LazyCheckSchema publishes before gate acquisition without self-deadlock.
2. Without an intervening publisher, the request acquires the snapshot
   produced by the lazy refresh.
3. A newer compatible snapshot published between lazy check and acquisition
   is accepted.
4. An unexpectedly older snapshot causes release-before-retry.
5. A newer incompatible snapshot returns an error without Reopen under lease.
6. Lazy cancellation or failure does not create a lease.
7. An active read lease makes lazy schema reopen return gate-busy without
   setting writer pending.
8. A lazy schema reopen does not wait behind an explicit reopen holding
   `reopen_mutex_`.

### 18.3 Publication

1. Reopen increments generation exactly once.
2. Published load information is already compacted.
3. Prepare failure or cancellation does not publish.
4. Drain cancellation clears writer pending.
5. The writer does not hold `mutex_` or `reader_mutex_` while waiting.
6. Every online publisher passes through the gate.

### 18.4 SearchResult Lifetime

1. Empty results hold and release a lease.
2. Filter-only results release their lease.
3. Group-by and iterator results release their lease.
4. Partial-result errors release all leases before retrying the complete sealed
   segment batch.
5. A multi-segment request owns one lease per sealed segment result.
6. Duplicate result release cannot decrement the gate twice.

### 18.5 Cache Lifetime

1. Iterators remain valid while publication is blocked.
2. Iterator consumption is safe during cache eviction.
3. Cache accessor pins release when the result is destroyed.

### 18.6 Stress and Fault Injection

1. Long search plus Reopen plus new searches.
2. Concurrent lazy-schema requests.
3. Search executor saturation.
4. Reopen prepare cancellation.
5. Reopen drain cancellation.
6. Intentionally delayed SearchResult release.
7. Segment release racing with result release.
8. Two multi-segment requests visiting segments in opposite orders while both
   segments publish.
9. Null-context publisher drain timeout.

---

## 19. Risks and Trade-Offs

### 19.1 Reopen Latency Amplification

Reopen waits for the slowest outstanding `SearchResult`.

This is accepted because:

- Reopen is infrequent;
- publication may temporarily reject and retry new sealed readers;
- request-level consistency materially simplifies correctness.

Mitigations:

- oldest-lease-age metrics;
- request timeout and cancellation;
- result leak detection;
- drain-wait alerts.

### 19.2 Localized Search Retry After Writer Pending

Once a writer is pending, new sealed searches fail admission. QueryNode cleans
up the full attempt and retries the complete segment batch with jitter. This
avoids retaining partial cross-segment leases, but can increase retry traffic
while a long-lived old request is draining.

The publisher has a bounded wait and returns cancellation/timeout instead of
forcing publication by ignoring a live lease, which would reintroduce
mixed-snapshot and resource-lifetime risks.

### 19.3 Fail-fast Scoped Retrieve Retry

Retrieve uses the same fail-fast admission signal as Search, but retries only
the rejected segment call because no lease escapes the CGO call. Retry uses
bounded jitter under the request context and never waits inside a Segcore
executor task.

A long drain can increase retry traffic and latency, but it cannot occupy every
Search CPU executor thread with gate waits or prevent Search consume callbacks
from progressing. Rejection counts and retry duration should be observable.

### 19.4 Lazy Reopen on the Search Executor

LazyCheckSchema may still perform I/O and loading on one Search CPU executor
thread, but it never waits for active readers or queues behind
`reopen_mutex_`. Competing requests fail fast so consume callbacks retain
executor capacity and can release old SearchResult leases.

Preparation can still increase latency, and a reader racing with the final
publication check can cause the staged work to be discarded and retried.
Long-term alternatives include:

- explicit QueryNode schema refresh;
- a dedicated reopen executor;
- stale-plan retry;
- removing mutation from the search path.

### 19.5 Cache Pins May Still Be Required

The request lease protects Segment publication but does not automatically
protect independent cache eviction.

If a Knowhere iterator is not self-owning, a resource-level cache pin remains
necessary.

---

## 20. Decisions

1. Search uses request-level consistency and cannot switch sealed snapshots
   across CGO phases.
2. Separate call-scoped Retrieve operations may observe different compatible
   snapshots.
3. Growing segment read leases are no-op.
4. Sealed segments use cross-thread counting leases instead of storing a Folly
   shared lock in SearchResult.
5. SearchResult owns the lease until `DeleteSearchResult`.
6. Search and call-scoped Retrieve acquisition both fail fast; Search retries
   the complete sealed batch after cleanup, while Retrieve retries only the
   rejected segment call.
7. Reduce, export, and fill do not reacquire a lease.
8. LazyCheckSchema runs before gate acquisition.
9. Only pure compatibility validation is allowed after gate acquisition.
10. Reopen performs one drain and one publication.
11. Existing `mutex_` use remains in the first implementation.
12. A whole-snapshot pin is unnecessary; cache cell pins are audited
    separately.

---

## 21. Open Questions

The following items must be resolved before the final design is approved:

1. **Gate type location**
   - standalone `SegmentReadLease.{h,cpp}`;
   - or private sealed-implementation types.

2. **SearchResult field type**
   - move-only `SegmentReadLease`;
   - or `shared_ptr<SegmentReadLease>`.

3. **Schema incompatibility error**
   - which Segcore error code to use;
   - whether Go retries or rebuilds the plan;
   - whether one transparent retry is allowed.

4. **Maximum lease duration**
   - metrics only;
   - alerting;
   - active request cancellation;
   - forced publication must not bypass a live lease.

5. **Writer-wait cancellation implementation**
   - timed condition-variable wait plus cancellation checks;
   - or a Folly cancellation callback.

6. **Cache iterator ownership**
   - whether Knowhere iterator already owns its index;
   - whether CacheCellAccessor must be retained.

7. **Initial versus online publication**
   - whether to introduce a lifecycle assertion;
   - which SetLoadInfo/Load paths can skip the gate before serving.

8. **Direct read C API scope**
   - which APIs read immutable or local counters only;
   - which APIs require a scoped lease.

---

## 22. Final Principle

The final serving principle is:

> A sealed segment fixes one published snapshot when a request starts. Reopen
> stops admitting new requests after preparation, waits for all old requests
> to finish, and publishes one complete new snapshot.

In addition:

> LazyCheckSchema may prepare or reopen schema before gate acquisition. After
> gate acquisition, only pure validation is allowed; every repair or retry
> must release the lease first.
