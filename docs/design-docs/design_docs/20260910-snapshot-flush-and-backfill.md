# Snapshot flush and backfill readiness

Issue: #52199. This document supersedes the creation semantics in the original
snapshot design. It does not change snapshot export, restore APIs, or storage formats.

## User behavior

CreateSnapshot actively flushes the collection. With
`compaction_protection_seconds > 0`, creation also waits for selected
stream-flushed segments to finish their growing-to-sealed handoff before pinning
their segment IDs. Spark can then backfill the captured serving segments for the
protection period. An existing compaction of a selected input must also finish before capture;
clustering can extend the wait through its index build and cleanup. External
collections have no sort lifecycle and bypass it.

A zero-protection snapshot still waits for the flush, but may capture unsorted
stream-flushed segments. It does not promise stable backfill targets.

The RPC acknowledges durable broadcast, as before. The snapshot becomes available
through the existing snapshot lookup after its callback saves it. A stalled flush,
sort, or storage operation can therefore delay snapshot availability and collection
DDL; this change does not introduce a new asynchronous job API or terminal failure
state. The configured wait timeout bounds one callback attempt, not the operation.

## Coverage contract

Each data-channel CreateSnapshot message seals growing segments and requests a
flush. Its WAL position is a **flush watermark**: DataCoord waits until every
channel checkpoint reaches that position before examining the selected inputs.

Selection retains the existing per-channel `segmentEffectiveTs < watermark`
rule. This identifies segments covering the requested flush, including their
current compaction replacements. A replacement can also contain later inserts.
All deletes and the immutable manifest version present when metadata is captured
are retained, including deletes after the watermark. Sort applies its normal
delete and TTL rules. Concurrent writes are allowed.

This is a capture of current persisted segment versions after a flush. It is not
an exact historical read at the watermark, a single cross-channel timestamp, or
a promise to preserve rows subsequently removed by deletes or TTL. `create_ts`
remains the minimum channel watermark for compatibility; `channel_seek_positions`
records flush provenance, not a restore-time read filter.

## Capture and protection

1. Recover or derive the broadcast's flush watermarks. For a completed replay,
   return the already saved snapshot.
2. Wait for channel persistence and, for protected snapshots, the selected
   stream-flushed inputs to become visible. Compaction continues during this wait.
3. Acquire the existing bounded capture slot.
4. Under `meta.segMu`, set the existing collection snapshot-pending flag. This
   drains a compaction that already passed its check; subsequent commits see the
   flag under the same lock. Compaction admission rechecks protection when setting
   compacting flags under this lock, so an earlier validated plan cannot begin
   computing after capture checked its inputs. Release `segMu` before
   metadata/storage work.
5. If a clustering task has published results but still has live inputs, release
   pending and retry later. The durable task records original inputs even when an
   intermediate sort segment has already been garbage collected. Its completed
   state also identifies the overlap after a result is replaced or garbage
   collected. A fully rolled-back task with dropped results no longer blocks.
6. For a protected snapshot, verify that selected inputs have no in-flight
   compaction, using both compacting flags and persistent task metadata (including
   before flags are restored on restart). Retry otherwise: precomputed outputs
   must not survive backfill and publish stale values when protection expires.
7. Generate and save the snapshot. Pending prevents segment replacement and GC
   during capture. Successful save registers the existing segment references and
   protection expiry before pending is cleared.
8. Clear pending on every exit. A failed attempt captures current versions again
   on retry. No capture-wide freeze has to survive callback backoff or restart.

The removed staging flag tried to preserve a historical segment boundary while
allowing sort and L0 updates. That was insufficient for historical row visibility
and is unnecessary under this contract.

## Clustering publication

Clustering creates invisible outputs, builds indexes, publishes outputs, advances
partition statistics, persists task completion, and finally retires inputs. These
writes remain in their existing recovery order.

Both output publication and input retirement now prepend a snapshot check to the
existing `UpdateSegmentsInfo` operator list. The check and update share `segMu`.
An already admitted task therefore respects both capture pending and committed
snapshot protection. A snapshot protection error pauses this state machine without
consuming its failure budget or sending partially published results to cleanup.
Worker-result validation likewise keeps a completed worker result pending when
snapshot protection temporarily prevents publishing it.

An already published clustering result cannot enter another replacement compaction
until the parent task finishes cleanup. Admission and result publication check the
existing durable task metadata under `segMu`; invisible intermediate outputs remain
eligible for sorting. This keeps failed-publication rollback from dropping only an
old result ID while a later replacement survives alongside the original inputs.

If publication wins before capture, the snapshot retries until input retirement
finishes. Protected capture also drains admitted compactions before pinning inputs. This
prevents a precomputed output from surviving the backfill until protection expires. L0 application remains allowed: it changes delete data,
not the identity of the backfill target.

## Upgrade and recovery

New broadcasts include data channels. StreamingNodes advertise `SnapshotFlush`
through the existing session capability pattern; DataCoord rejects creation when
any registered StreamingNode lacks that capability. Release version comparison is
insufficient because old and new development builds can report the same version.
DataNodes no longer consume the WAL, and QueryNodes filter out this message type.
This gate checks currently registered nodes; deployment must prevent older
StreamingNodes from joining or being restored after the feature is enabled.

A replayed old CChannel-only message has no flush watermark. It uses legacy
checkpoint coverage and does not claim a historical active flush. Protected old
requests also wait for the selected stream-flushed inputs to become visible.
Missing results on a declared data channel are errors, not a legacy fallback.

Completed snapshot metadata remains the idempotency marker. Existing snapshot
reference loading blocks compaction and GC until committed references are loaded.
The new capture flag is temporary and need not be reconstructed on restart: the
callback selects current versions again. A crash after durable snapshot save is
handled by the existing save/reload and callback replay paths.

If initial compaction persistence or queue admission fails, the unscheduled task
keeps its input ownership and enters the existing cleanup queue. Even an ambiguous
catalog response or failed cleanup write leaves an owner to retry until `cleaned`
is durable. With the compaction subsystem disabled, snapshot requests whose
required task drain cannot run are rejected before broadcast.

## Validation and limits

Regression coverage targets automatic protected-snapshot waiting, legacy replay,
checkpoint fallback, clustering publication/retirement protection, partially
published clustering tasks with missing intermediate/results, cleanup rollback,
ambiguous compaction persistence, protection error retry behavior, GC during
capture, capability advertisement, pre-broadcast rejection, and release of pending
on failure. Capture tests exercise real `CreateSnapshot -> GenSnapshot ->
SaveSnapshot` with shared protection state and local snapshot storage, including
catalog commit failure.

End-to-end acceptance should exercise concurrent insert/delete during the wait,
backfill of the captured segment IDs, restart before/after snapshot save, and
clustering publication failures. Unit tests alone do not establish those behaviors
in a running cluster. Persistent broadcast callback errors still retain collection
DDL locks; changing that lifecycle is outside this issue's scope.

### Local verification (2026-09-10)

- Rebased onto master `bbfd40c3aa802acee5d54637f09477dc7b3616a3`.
- Protobuf regenerated with `make generated-proto-without-cpp`.
- `pkg/streaming/util/message/...` passed with the required dynamic/test tags.
- The unchanged source of `snapshot_boundary.go` and its `TestNewSnapshotBoundary`
  were exercised separately from DataCoord; all five subtests passed. This checks
  watermark encoding and legacy recognition, not the callback/capture lifecycle.
- Go parsing/formatting and `git diff --check` passed.
- DataCoord focused regression passed: 317 top-level tests and 418 subtests,
  covering snapshot creation, storage, protection and the affected task lifecycle.
  The new session capability serialization test also passed. Tests used the
  required tags and disabled optimizations, with a matching existing native build
  selected through per-process library/pkg-config settings.
- A broader compaction run exposed a DISKANN expected-segment-size mismatch,
  reproduced independently with unchanged master code: the selected native build
  has `BUILD_DISK_ANN=OFF` / `WITH_DISKANN=OFF`, so DISKANN is absent from its
  advertised index features. This is a local build limitation.
- Proxy still cannot build with the installed native artifacts: they lack newer
  core symbols such as `SetStorageV2AsyncLoadEnabled`. No running-cluster
  fault-injection or Spark backfill end-to-end result is claimed.

### Rebase verification (2026-09-14)

- Rebased the single commit onto master
  `a876f471053edb2f68a06a9894afee9810ea7906`. The only conflict was generated
  `data_coord.pb.go`, resolved with `make generated-proto-without-cpp`.
- Range-diff showed no changes to the non-generated implementation patch.
  Source review checked the interaction with master's clustering worker-failure
  retries and session initialization retries; the new `fail_status` protobuf
  field and the snapshot fields are all retained.
- DataCoord and session regression attempts stop at native compilation on this
  base: the installed artifacts lack the newer column-group properties argument,
  analyzer status-return API, and packed-reader/writer signatures. The passing
  counts above describe the September 10 base, not this rebased head.
