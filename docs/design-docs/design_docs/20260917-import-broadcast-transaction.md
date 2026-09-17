# Retain import broadcast resource keys until Commit or Rollback

Status: draft implementation for review, 2026-09-17.
Issue: https://github.com/milvus-io/milvus/issues/52154
Base: trunk/master 2cbfc53b810377c0bbcc1f584252458a98385b74.
User scope: import/DDL mutual exclusion only; light mode, no new tests or compilation.
This document supersedes the earlier general broadcast transaction design.

## Problem and selected boundary

An import waits for collection indexes while a concurrent schema DDL can make its
segments too old to build indexes; schema reconciliation excludes importing segments,
so neither side progresses. Keep the collection's existing broadcast resource keys
owned by the Import broadcast until its CommitImport or RollbackImport completes.
Other DDL APIs keep their current locking code. No index snapshot, segment schema
migration, TryLock chain, public transaction protocol, or per-DDL job check is added.

## Mechanism

- Declare the existing Import/CommitImport/RollbackImport message family as a pair
  keyed by job ID. Begin and End remain ordinary WAL broadcast messages.
- Persist the resource owner BroadcastID on BroadcastTask: Begin owns itself and End
  references Begin. A separate released flag distinguishes ACKed Begin from closed
  ownership. Old records and secondary-created records have owner ID zero and retain
  their previous behavior.
- Begin ACK creates the job and returns to the caller while retaining its guards.
  Open Begin tombstones do not enter the ordinary GC queue.
- End uses the owner's keys without reacquiring them. Only one End is registered per
  owner; retries wait for that same broadcast. After End ACK is persisted, persist
  Begin's released flag, unlock once, and enqueue Begin and End for GC.
- Owned CommitImport disables FastAck so each data-channel flusher handles its
  commit fence before ACK; this does not assert the stronger checkpoint durability
  semantics that AckSyncUp has not yet implemented.
- Recovery first closes owners whose End is already durably ACKed, then restores one
  guard set per still-open owner. End records never acquire a second copy of the keys.
- An idempotency reservation before resource acquisition lets retries resolve an
  existing Begin without waiting for the entire import job. The reservation itself
  is not acceptance: only the persisted broadcast is accepted.
- Import admission rechecks the collection identity and request schema under its
  keys. Existing AutoID allocation stays before admission; no sizing retry loop.
- Empty auto-commit imports use the existing commit path. Failed and timed-out
  owned imports close through the existing checker GC loop, before job removal;
  this can wait until the next low-frequency checker tick. Physical cleanup stays
  asynchronous. The closer runs outside importMeta's mutex.

## Effects and limits

Retaining the current exclusive collection key serializes imports of one collection
and blocks its collection DDL; shared DB and cluster keys also block corresponding
exclusive database and cluster actions, including FlushAll. Ordinary DML and the
checked sort/index worker paths do not acquire these broadcaster keys.

This draft does not add CDC ordering anchors, passive replica ownership, seamless
legacy-job migration, or new cancellation/timeout policy. It preserves legacy and
secondary broadcast behavior but does not claim to fix their import/DDL race.
Existing active jobs must be drained before relying on the new guarantee; downgrade
with open owned imports is unsupported. An auto-commit job still cannot be manually
aborted through the current public API. Ordinary resource Lock remains non-cancellable.

## Review and validation

Review ownership transfer before/after registration, Begin ACK, End persistence,
callback completion, GC and restart. Confirm End never waits on its own keys,
failed jobs cannot be GC'd before closure, and duplicate requests reuse Begin/End.
No unit tests, compilation, end-to-end tests, CDC failover tests, or version upgrade
experiments are run in this light-mode draft. Formatting and source-level checks
are the only verification; generated descriptor inspection confirms only the two
ownership fields changed. Existing mock expectations and empty-import state tests
will need review when test work is authorized; this draft is not ready for merge.
