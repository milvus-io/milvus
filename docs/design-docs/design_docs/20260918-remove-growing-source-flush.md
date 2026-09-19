# Remove growing-source flush and converge flush-path state

Date: 2026-09-18
Status: proposed

## Background

Growing-source flush lets a QueryNode persist a growing segment's rows from its
own in-memory copy through StorageV3, instead of from the DataNode write
buffer's copy. It is gated by `common.storage.enableGrowingSourceFlush`, which
defaults to `false`, so no production deployment runs it today.

The feature costs more than it returns:

- It adds a second sync-task type (`GrowingSourceSyncTask`) that duplicates
  roughly 70% of `SyncTask`: builders, error handling, metacache commit,
  column-group resolution, start-position gathering, and meta-writer path. Every
  write-buffer call site type-switches on the concrete task.
- It adds a seven-dimension progress state machine to `writeBufferBase`, which
  is already the largest type in the package.
- It adds a release protocol spanning QueryNode, StreamingNode client, and
  StreamingNode server, so releasing a growing segment must first fence
  admission and settle reference debt.
- Its failure modes can wedge the channel checkpoint silently. A non-retryable
  layout mismatch marks a segment permanently ineligible for sync while its
  checkpoint contribution stays pinned.

Removing it also removes the reason the flush path carries several parallel
mechanisms for the same job, which is what the second half of this document
converges.

## Scope

This is one change with two halves. The first half removes the feature and
changes no behavior at the default configuration. The second half converges
state and policy in the flush path that remains.

### Half 1: removal

| Category | Count |
| --- | --- |
| Files deleted outright | 9 |
| Symbols removed from shared files | ~120 |
| Call sites edited rather than deleted | ~24 |
| C++ test files needing a replacement fixture | 3 |
| Mockery mocks to regenerate | 7 |

Counts are from a symbol-level inventory and are approximate; the branch base
moved forward during design, so the implementation locates every symbol by name
rather than by line.

Files deleted outright:

- `internal/flushcommon/syncmgr/growing_source.go` and its test
- `internal/flushcommon/metacache/flush_source.go`
- `internal/querynodev2/delegator/growing_flush_source.go` and its test
- `internal/streamingnode/client/handler/registry/release_manual_flush_preparer.go`
- `internal/streamingnode/server/service/release_manual_flush_preparer.go` and
  `internal/streamingnode/server/service/handler_test.go`
- `internal/core/src/segcore/flush_growing_segment_test.cpp`

The config gate is removed end to end: the `configs/milvus.yaml` key, the
paramtable `ParamItem`, the cgo push in `internal/util/initcore`, and the
`SegcoreConfig` field with its accessors. No deprecated no-op key is left
behind, because an operator-visible switch that does nothing is worse than an
absent one. Unknown keys in `milvus.yaml` do not affect startup, so removal is
safe for existing deployments that set it.

No dormant abstraction seam is kept. Exported API with zero callers and zero
tests rots and misleads; the contract lives in this document and the
implementation lives in git history.

Three removals are not mechanical and are called out because a previous attempt
got each of them wrong:

**`internal/util/initcore` must keep its cgo push.** The function that pushes
the growing-source flag is also the repository's only caller of
`C.SegcoreSetStorageV3Enabled`. Deleting the function wholesale would leave
`SegcoreConfig::storage_v3_enabled_` at its default forever. The function is
renamed and keeps the StorageV3 half.

**Test setup lines are edited, never whole test functions.** The QueryNode
`ServiceSuite` runner contains one mock expectation for the removed release
protocol. Deleting the runner disables every QueryNode service test while the
package still reports success. The same shape appears in five delegator and
write-buffer `SetupTest` bodies, two integration-test `SetupSuite` bodies, and
one shard-manager assertion cluster. Each loses the named lines only.

**`sealSegments` changes behavior.** Today a segment absent from the metacache
is logged and skipped when the gate is on, and returns
`merr.ErrSegmentNotFound` when it is off. After removal the error path is
unconditional. Default-configuration behavior is unchanged, since the gate
defaults to off.

### Half 2: convergence

Three things in the remaining flush path are maintained in more than one place
or decided in more than one way. Each converges to a single owner.

**Row accounting.** `bufferRows` is written as an absolute value by the write
buffer and as a delta by metacache actions applied from the sync task. That
mixture is not observably wrong, because `wb.mut` serializes every writer, and
it is stated that way here after checking rather than assumed: `bufferInsert`
runs under `BufferData`'s lock, `getSyncTask` is reachable only through
`getSyncTasksLocked`, and `AbortSyncing`'s single caller carries a `Locked`
suffix. What the mixture costs is fragility. Correctness rests on that
serialization, on `yieldBuffer` discarding the whole `segmentBuffer` so a fresh
one restarts from zero, and on every terminal path remembering to undo
`StartSyncing`. None of the three is asserted anywhere.

The third one does fail. `AbortSyncing` has no call site on the plain
`SyncTask` path at all, so a failed sync leaks `syncingRows` and `syncingTasks`
permanently, and the two early returns in `Run` leak them too. Only the
panicking default error handler keeps that from being visible. The checkpoint
pin leaks with them: the success callback removes the candidate after an
`if err != nil { return err }` that runs first.

The zero clamp that looks like it hides this drift lives in
`updateGrowingSourceBufferedRows`, which is growing-source code and goes away
with the feature. The common path never clamped and never asserted.

Converged design: a sync reservation. One metacache transition moves rows from
buffered to syncing and returns a handle. The handle is settled exactly once,
guarded by `sync.Once`, on every terminal path. All counters become delta-only
and the absolute write path is removed. The invariant is asserted in the
metacache action rather than documented in a comment.

**Checkpoint pins.** Five independent mechanisms can hold back the channel
checkpoint today. Removing growing-source flush deletes two of them, the
uncommitted-batch ledger and the `processedTs` fence, leaving the last consumed
position as an upper bound, buffered start positions, and the in-flight sync
candidate set. The handoff from buffered position to candidate set is correct
only because two statements happen in the right order inside one critical
section, and on the failure path the candidate is never removed, so the
checkpoint is pinned forever.

Converged design: pin acquisition becomes part of the same reservation handle.
Yielding the payload and installing the pin are one operation, and pin release
follows settlement rather than call order.

**Terminal outcome policy.** The same condition is handled three different ways:
the meta writer swallows a missing segment and a missing channel as success,
`SyncTask.Run` returns nil when the segment is absent from the metacache, and
the growing-source meta writer propagates both. `HandleError` runs twice for one
failure on the plain path. Segment removal has three owners.

Converged design: one settlement table, below, and one exit point that applies
it.

## Replacing the StorageV3 test fixture

This is the largest non-obvious cost of the removal and the reason a previous
attempt deleted roughly 2500 lines of unrelated C++ tests.

Three C++ test files use `FlushGrowingSegmentData` as a way to materialize a
growing segment into an on-disk StorageV3 segment, then load it back as a sealed
segment. The flush call is scaffolding, not the subject under test:

- `internal/core/src/common/ArrayValueTest.cpp`, nested-array retrieval
- `internal/core/src/exec/expression/MatchExprTest.cpp`, match-family expressions
- `internal/core/unittest/test_storage.cpp`, five separate fixtures

Deleting the C API breaks their compilation. The tests themselves cover
nested arrays, match expressions, error classification, and Arrow reader
configuration, none of which relate to growing-source flush. They must keep
running.

Plan: add one shared test helper that writes a StorageV3 segment from generated
field data using `milvus_storage::SegmentWriter` and a transaction directly, and
point the three files at it. The pattern already exists in
`ChunkedSegmentSealedStorageV2Test.cpp`, `GroupChunkTranslatorTest.cpp`, and
`VectorArrayStorageV2Test.cpp`, and `test_utils/ManifestTestUtil.h` already
provides manifest helpers. The helper writes through the shared layout helpers
named in `docs/agent_guides/storage/path_contract.md`, so the sealed loader
resolves the fixture exactly as it resolves a production manifest.

Fixtures that only need an on-disk V3 segment take the generated dataset
directly and drop the growing-segment step, which is shorter than what they do
today.

## Settlement table

A reservation settles in exactly one of three ways. The distinction that matters
is whether the rows still exist somewhere a replay can find them.

| Outcome | Accounting | Checkpoint pin | Segment state | Channel |
| --- | --- | --- | --- | --- |
| Committed: DataCoord accepted | rows move syncing to flushed | released | flushed, or removed when the task is a flush or drop | unaffected |
| Discarded: the segment no longer exists | reservation dropped | released | segment removed | unaffected |
| Failed: rows are not persisted and not in memory | reservation dropped | **kept** | unchanged | escalated |

`Escalated` means the existing failure path is taken unchanged: the error
reaches the write buffer's configured error handler, which by default aborts the
process so that a restart replays the channel from its checkpoint. Replacing
that mechanism with something gentler is out of scope, listed below; this change
only stops the accounting and the pin from being wrong on the way there.

The `Failed` row is the one that is currently wrong in two directions. The
payload has already been yielded out of the buffer and released, so the rows
exist only in the WAL. Releasing the pin would lose them, and leaking the
accounting hides the failure. Keeping the pin and escalating is what makes a
restart replay the missing rows, which is the behavior the panicking default
error handler accidentally provides today.

`Failed` keeping the pin makes the escalation an obligation rather than an
implementation detail. The default write-buffer error handler aborts the
process, and `WithErrorHandler` has no production caller, so a failed sync
always ends in a restart that replays from the pinned position. A future
handler that swallowed the error instead would leave the pin in place and
freeze that channel's checkpoint with no further signal.

Policy for specific conditions:

- **Segment absent, from the metacache or from DataCoord**: `Discarded`. A
  segment id is never reissued, so the segment cannot come back, and there is no
  metadata left for the rows to belong to. Terminal, not an error.
- **Channel absent, from DataCoord**: retriable, which is a change. DataCoord
  returns this from its ownership check, and the comment at that check states
  the rejection can happen while the flusher is ready but the coordinator has
  not yet observed the assignment, so the caller should retry. Swallowing it as
  success reports persistence for rows DataCoord never recorded, releases the
  pin, and lets the checkpoint advance past them.
- **Cancelled**: `Failed`. Cancellation during shutdown must not release pins.
- **Channel removed without drop**: the buffer is discarded with its pins, which
  is correct because the channel's metacache goes away with it.

Retry classification follows `docs/dev/error_handling_guide.md`. Channel-absent
is a transient system error and stays retriable; segment-absent is terminal and
is not converted into an input error, so no `retry.Do` consumer changes meaning.

## Deviation from this design, recorded deliberately

The convergence section calls for metacache mutation from exactly one commit
point, and segment removal ended up with two rather than one. `SyncTask` removes
the segment when it drops one; the write buffer removes it when a flush
completes. Both are post-ack, so neither is the pre-ack mutation the design was
guarding against, and the third owner that made this a problem, the
growing-source task, is gone.

Consolidating the remaining two would mean moving drop removal out of the task
and into the write buffer's settlement path. That is safe today, because only
the write buffer ever sets the drop flag, but `SyncTask` is also constructed by
the import path in `internal/datanode/importv2`. Moving the cleanup would leave
a task that no longer cleans up after itself, so any future non-write-buffer
caller that set the drop flag would silently skip removal. Two owners, each
removing what it knows it finished, is the safer shape.

## Out of scope

Deliberately not attempted here, to keep the change reviewable:

- Splitting `writeBufferBase`, which is about 1800 lines carrying seven
  concerns, into separate collaborators.
- Splitting `bufferManager` into a registry and a memory governor.
- Separating persisted metadata from runtime accounting in
  `metacache.SegmentInfo`.
- Removing the panic-on-error behavior of the flow-graph nodes, which currently
  doubles as the backpressure and escalation mechanism.

Each is a larger change with its own blast radius. The convergence above is
chosen because it is what the removal already forces us to touch.

## Verification

Per the repository's verification gate, success-path tests alone do not
establish that a behavioral change works.

G1, verify inputs and not only the transform. Every construction site of the
row counters and of the checkpoint pins is audited, not only the new settlement
point. Specifically: every `UpdateSegments` call that touches
`bufferRows`, `syncingRows`, `syncingTasks`, or `flushedRows`, and every
`syncCheckpoint` add and remove.

G2, trace each failure mode end to end. For each row of the settlement table,
either fault-inject or hand-trace from origin to consumer and confirm the rows
land in the intended bucket: DataCoord rejecting with segment-absent, DataCoord
rejecting with channel-absent, a storage write failure, context cancellation
during shutdown, and channel drop with an in-flight sync.

G3, no over-claiming. The PR body asserts only what G1 and G2 establish.

G4, adversarial self-review before review, focused on which construction site
of the counters or pins was not read, and which failure mode was not traced.

Concrete test additions:

- A reservation is settled exactly once on each of the three outcomes, and a
  double settle is a no-op.
- A failed sync keeps its checkpoint pin, and the channel checkpoint does not
  advance past the failed batch.
- A missing segment settles as discarded and releases the pin.
- Channel-absent from DataCoord retries rather than reporting success.
- The row invariant holds across a buffer, seal, sync, and failure sequence.
- `sealSegments` returns segment-absent unconditionally.

Existing coverage that must keep running and is verified to still execute: the
QueryNode `ServiceSuite`, the write-buffer and L0 write-buffer suites, the
syncmgr suites, the three C++ fixture files, and the streaming WAL tests.

## Risks and rollback

The channel-absent retry change is the only intentional data-path semantic
change. It is localized to the meta writer's error classification and can be
reverted alone without touching the convergence work.

The C++ fixture helper is new test-only code. If it proves unable to reproduce a
specific fixture, the fallback is to keep that one fixture's segment built from
a checked-in manifest rather than to delete the test.

Removal of the config gate is not reversible in place, since the segcore field
goes with it. Re-adding the feature means re-adding the plumbing, which the
design above accepts as the cost of not maintaining a dormant seam.
