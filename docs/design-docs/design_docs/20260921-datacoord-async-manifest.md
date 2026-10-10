# MEP: DataCoord asynchronous manifest loading and commit

- **Created:** 2026-09-21
- **Author(s):** @somfornot
- **Status:** Under Review
- **Component:** Coordinator / Storage
- **Related Issues:** #53047

## Summary

Use milvus-storage PR #698's callback Open/Commit ABI in DataCoord startup
index recovery, restore/index and LOB GC reads, the segment-scoped commit
framework, and the legacy L0 delta-log adapter.
PR #698 merged on 2026-10-08. The dependency is pinned to `c40babbb83ff34e837ccf61345e589f16a1b21a1`.
The existing storage implementation, manifest format, worker protocol, resolver,
per-segment serialization and catalog publication rules are unchanged.

## Motivation

Startup recovery previously processed fixed batches, so one slow manifest delayed
replenishing the other read slots. Synchronous manifest Open/Commit also prevented
DataCoord from using the storage library's caller-owned async executor API.
This change integrates that API while keeping resource ownership and shutdown
explicit at the recovery and commit business entry points.

## Public interfaces and compatibility

No RPC, manifest format or catalog schema changes are introduced. The native
storage dependency and all synchronous transaction-open call sites move together
to the merged async ABI; the native libraries must be rebuilt with this revision.
`dataCoord.manifestCommitConcurrency` controls the shared commit executor, defaults
to 16, and is clamped to [1, 256]. It is read once during metadata initialization
and requires a DataCoord restart to change. The old
`dataCoord.compaction.levelzero.manifestUpdatePoolSize` key is no longer used and
is not a fallback for the global setting: remove that old override and configure
the new key explicitly if the default global capacity is unsuitable. The existing
manifest read concurrency setting continues to control read admission. Neither reader nor executor performs
configuration lookup.

## Design details: ownership and execution

Each recovery reader owns a `packed.ManifestIOContext` and closes it when the
startup scan finishes. Dropped-segment GC, unused-index GC and LOB scans each own
one read context for the whole sweep, passing it to per-segment helpers and closing
it on every exit. Commits use a separate, long-lived `manifestCommitExecutor` owned by
`meta`; each single/batch commit leases its context for the I/O phase and passes
it explicitly to nested operations. There is no executor shared between recovery
and commits. Callers wait for admission and completion in Go. A bounded Go worker
queue implements the caller executor
required by the ABI; buffered channels receive callbacks, including callbacks
that finish before submission returns. C retains only C allocations and integer
`runtime/cgo.Handle` tokens, never Go pointers. The callback owns and frees the
FFI result. Callback-style reads release the operation and destroy the loaded
transaction in the callback; the waiting commit caller retains those duties for
commits.

The bridge uses `LoonAsyncContextHandle` and the `loon_async_context_*` API.
Each open/commit supplies a zero-initialized, caller-owned `LoonAsyncHandle`
with `timeout_ms` set from the Go deadline (30 seconds by default, capped at
one day). The native API copies the timeout at submission. Open submission,
cancellation and callback release use the same handle address under a short
mutex; submission never waits for I/O or completion. `context.AfterFunc` requests
cancellation without a resident goroutine waiting per read. The callback stops
that cancellation hook and clears the native handle before delivering its result.
Commit handle access remains serialized by its waiting caller. No live handle is
copied or reused. The executor
descriptor contains only `context` and `submit`. Synchronous callers also use
the renamed `loon_transaction_open` entry point.

The existing read concurrency setting bounds startup/restore/GC index reads
through their shared admission budget. The recovery entry point reads that
setting, caps it by segment count, and passes the resulting concurrency to the
reader constructor. `newMeta` reads `ManifestCommitConcurrency` once and passes
it to `newManifestCommitExecutor`. Single and batch commits call `acquire(ctx)`
without supplying concurrency; batch submission uses the component's fixed
capacity. Workers start as accepted queued and running tasks exceed the existing
worker count, up to that capacity; idle workers are reused until shutdown. Neither
the reader nor the executor looks up configuration. Commit capacity remains fixed for the component's lifetime; this
setting is not refreshable and changes require a DataCoord restart.
Each sequential GC scan needs one executor worker. Commits derive index markers
from their already-loaded input and staged mutations, without a read-back. No
executor capacity is derived by combining read and commit settings. Waiting callers observe their
context cancellation. Native admission/queue rejection is returned without
waiting for a callback.

Each owner's close prevents new users, waits for active callers, drains native
callbacks, and finally joins executor workers and deletes the handle. Recovery
closes its executor on success and failure. The commit component remains alive
across batches and is closed on failed initialization or server shutdown, after
outstanding leases have returned. A single commit releases its lease before
catalog publication to avoid nested acquisition through metadata update paths.

This integration makes no new guarantee about the transport implementation or
network-wait thread usage. Those remain properties of the pinned storage library.

## Loading

Recovery consumes final read results from an internal `manifestIndexReader`
and installs metadata in the calling goroutine. The reader owns admission-window
accounting, delayed retries and callback draining. Its `next()` method drives
submission and returns a segment result after retries; `close()` cancels and
drains outstanding results on every exit, including metadata installation errors.
The reader calls `SubmitManifestIndexInfos` on its own `ManifestIOContext`
executor; there is no separate load-trigger worker group. Native load work, callback delivery and manifest
projection run on the same executor. Callbacks release admission themselves and
only enqueue completed results into a bounded channel. They never wait for
another executor task, re-enter submission or close their own context.

The coordinator consumes completions, installs metadata, and replenishes reads
up to the existing concurrency limit. A slow revision does not prevent other
slots from progressing. One timer schedules per-segment retries (at most three
attempts, with 200 ms then 400 ms backoff); delayed retries and accepted reads
share the bounded window. Failure cancels further submission and drains every
accepted result before returning. Metadata installation remains single-writer,
and empty index markers are persisted in bounded groups. Only failed segments
are retried. A failed read or invalid index fails startup;
partially populated metadata is never published by `newMeta`/`initMeta`.
Dropped manifests proven absent still retain their row for GC to finish.

## Commit and failures

`CommitSegmentManifest` and `CommitSegmentManifests` pass the request context and
owner to async Open/Commit. Mutations and C manifest projection remain in-memory
operations. Index-drop validation reuses the transaction's already-loaded exact
revision instead of opening a second transaction. A stale build ID still fails;
an already-absent index still avoids writing an empty revision.

The segment lock remains held through terminal callback and catalog publication.
The batch caller submits through `SubmitManifestUpdates`; native open, in-memory
mutation and native commit are chained on the same executor. Executor admission
is the only commit concurrency limit, with no additional batch worker pool or
per-segment waiting goroutines. The packed adapter derives index presence from
the already-loaded exact revision and the staged mutations: file appends invalidate
indexes on affected columns, explicit drops remove matching IDs, and additions
are applied last. OVERWRITE uses that same input even on version-allocation
retries. A successful callback carries the marker along with the committed path;
DataCoord does not reopen the final revision or run a second batch phase. Unchanged
index sections preserve the existing marker. Failures cancel peers and drain all
accepted callbacks before releasing locks, without catalog publication.

The legacy L0 batch follows the same direct-submission model. Updates for each
segment form an ordered chain. Terminal callbacks enqueue that segment's next
step; the batch caller submits it using the preceding committed revision. A
buffered completion slot per segment permits callbacks to return while the caller
waits for executor admission. Failure cancels peers, drains accepted commits, and
retains only confirmed paths in the retry cache.

Copy/restore source prefetch and copied-index ownership verification each own one
read executor for the batch. They use callback submissions and drain before request
assembly or segment publication; no additional pool waits on blocking async wrappers.
The existing shared read budget still bounds concurrent recovery, restore and GC.

Cancellation requests native cancellation and waits for the terminal result.
The native timeout is a queue deadline: it prevents work from starting after
expiry but cannot interrupt synchronous I/O already executing on a caller worker.
Shutdown may therefore wait for the storage backend's own timeout.
Cancellation cannot overwrite a confirmed commit or an uncertain outcome. A
`ManifestCommitError` retains NOT_COMMITTED or UNKNOWN through `errors.As` and
keeps the original error chain. The adapter never retries a consumed transaction.
A later coordinator task attempt may build a fresh transaction from the currently
published pointer under its segment lock; an uncertain unreferenced revision is
not adopted by selecting the latest object-storage version.

| Origin | Result at DataCoord |
| --- | --- |
| Go admission cancellation | context error, no native operation |
| Native initial enqueue rejection | transient storage error, no callback |
| Open/read failure | existing storage error chain; no publication |
| Commit cancelled before execution | NOT_COMMITTED |
| Commit succeeds despite cancellation | COMMITTED/version preserved |
| Native commit error after execution starts | UNKNOWN, original error preserved |
| Catalog failure after confirmed commit | pointer remains unpublished; existing recovery semantics |

## Test plan and validation

Validate real local-storage FFI read/commit/drop round trips; queued cancellation
and shutdown draining; executor rejection; commit outcome preservation; recovery
progress while one read is stalled; and existing segment lock/CAS, atomic catalog,
restart, dropped-manifest, GC and restore regressions. Source tracing covers
native submission rejection, callback ownership and commit outcome construction.

Verification on 2026-10-08 ran in the Milvus development container:

- Native `cmake --build cmake_build --target install -j 24` passed against storage
  revision `c40babbb83ff34e837ccf61345e589f16a1b21a1`, retaining the existing
  feature configuration. A pre-existing GCP compilation error required
  qualifying `milvus::ErrorCode`.
- Focused packed tests passed 20 top-level tests (28 including subtests).
  All 11 async tests passed three repetitions under `-race`; the extended
  admission-rejection test also passed three race-enabled repetitions.
- Focused DataCoord tests passed 183 top-level tests (726 including subtests)
  against isolated etcd. The five reader/executor tests passed three repetitions
  under `-race`, including independent reader ownership, shutdown draining and
  fixed commit capacity after configuration changes.

All Go tests used `-tags dynamic,test -gcflags='all=-N -l'`. The DataCoord
selection covers manifest recovery and commit, L0 compaction, LOB GC, copy-segment,
rejected stats cleanup, stats tasks, schema materialization and metadata updates.
These are focused local checks, not full-suite, remote CI, live cloud or E2E
coverage. Controlled production S3 latency/throughput measurements remain separate;
no numerical speedup is claimed from this implementation.

## Rejected alternatives

- Separate load-trigger and callback pools add a scheduling layer and lifecycle
  without being required by the caller-executor ABI. Recovery uses one executor.
- A shared read/commit singleton sized to the larger configuration couples
  independent workflows. Recovery owns a scan-scoped context; commits own a
  long-lived component with its own fixed capacity.
- Looking up configuration inside generic reader/executor helpers hides the
  business decision. Their constructors accept explicit capacity instead.

## PR breakdown and dependencies

One stack layer, `master <- coord-manifest/async-io`, contains the dependency pin,
bridge, DataCoord integration, tests and this document. Upstream storage PR #698
and its prerequisites are merged into the pinned revision. This follows the
manifest publication implementation in Milvus PR #53048; it does not change the
storage transport or remove existing metadata lock constraints.

## References

- [Manifest ownership issue #53047](https://github.com/milvus-io/milvus/issues/53047)
- [Manifest publication PR #53048](https://github.com/milvus-io/milvus/pull/53048)
- [Storage async manifest PR #698](https://github.com/milvus-io/milvus-storage/pull/698)
