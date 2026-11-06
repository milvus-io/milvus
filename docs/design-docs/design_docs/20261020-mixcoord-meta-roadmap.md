# MixCoord Metadata Roadmap

Track: Memory Deduplication
Status: Draft
Issue: #54489 (RootCoord merge)
Predecessor: [Centralized Metacache for MixCoord](20260917-shared-segment-store.md) (#53577, #53579)

## Purpose

This is the umbrella document for MixCoord metadata work after #53579.
It lists the work items, their order and how they split into PRs. Each
substantial item gets its own design doc. This one only records scope,
order and how items depend on each other.

## Where #53579 Left Things

#53579 put segments, collections and channel checkpoints for DataCoord
(DC) and QueryCoord (QC) into one in-process `metacache.MetaStore`.
On master after #53579:

- Segment metadata exists once. QC targets hold ID sets and resolve
  details through `MetaView`.
- Collection metadata still exists twice. RootCoord's (RC) `MetaTable`
  is authoritative. metacache holds a converted `CollectionInfo` copy
  that DC fills from RC: a full walk at boot, a lazy backfill on a miss,
  and a push on the AlterCollection and Truncate acks.
- That copy goes stale in two known ways. Create and drop partition do
  not refresh it, and the lazy backfill can overwrite a newer entry with
  an older snapshot (`PutCollection` has no version guard).
- DC and QC still call RC through the in-process broker in many places
  (`DescribeCollectionInternal`, `ShowPartitions`, `HasCollection`,
  `DescribeDatabase`). QC also asks DC for segment details through the
  broker in a few places, and `GetRecoveryInfoV2` still runs on every
  next-target update.
- Multi-segment mutations (compaction, flush) are applied to MetaStore
  one `PutSegment` at a time, so QC can see a compaction's inputs as
  Dropped before its outputs exist.
- DataCoord's peripheral metas (compaction tasks, import jobs, stats
  tasks and others) load fully at boot and keep finished records in
  memory for hours.

## Work Items

### M1 — Single owner for collection metadata (milestone)

Design: [Single Owner for Collection Metadata](20261020-single-owner-collection-meta.md).
Issue: #54489.

metacache's collection reads delegate to RC's `MetaTable`, so collection,
partition and database metadata have one owner in MixCoord. Moving RC's
stores themselves into metacache is not part of M1. Bugs left over
from #53579 are fixed in the PRs of this milestone as they come up.
They are not tracked as separate issues.

| PR | Type | Content |
|----|------|---------|
| M1.1 | doc | Sync the #53579 design doc with the code (see "Doc corrections" below) |
| M1.2 | enhance | #53579 cleanup: remove dead `LoadChannelCheckpoints`, guard a nil MetaView in QC, run the segment normalizer in parallel after the catalog list |
| M1.3 | test | `GetRowCount` invariant regression test (promised in #53579 review) |
| M1.4 | fix | Batch segment writes in MetaStore, so compaction and flush outputs become visible to QC in one step |
| M1.5 | doc | Design doc for the merge (the linked doc) |
| M1.6 | enhance | `MetaTable`: route every collection mutation through one helper. No locking change |
| M1.7 | feat | `MetaTable` publishes a `CollectionInfo` view, read without `ddLock`; metacache collection reads delegate to it; delete the metacache collection copy and its boot walk, lazy backfill and push paths |
| M1.8 | enhance | Move DC and QC in-process broker calls for collection, partition and database metadata to direct reads |

M1.1–M1.4 do not depend on each other or on the merge. M1.6 must land
before M1.7. M1.8 can be split per caller group.

Doc corrections for M1.1 (the #53579 doc says things the code does not
do):

- "Batch operations for compaction and flush" are listed, but there are
  none. M1.4 adds them; the doc should match whatever lands.
- "A condition variable for WatchChannelCheckpoint": DC keeps per-channel
  key locks only.
- "All such lookups are O(1) under RLock": the ID-set accessors copy
  O(k) maps under the lock.
- "TargetObserver rebuilds on its next cycle": the rebuild is gated on
  `NextTargetSurviveTime`.
- Motivation says DC and QC can disagree on segment state, but QC's copy
  never carried State; only target membership could diverge.
- Open Question 2 says the store grows without bound, but GC's
  `DropSegment` already removes dropped segments.
- Status is still Draft.

### M2 — Load peripheral metadata on demand

Needs its own design doc before code. Most of these metas already load
in parallel with segments inside `newMeta`, so the payoff is mostly
memory, not boot time. Items, ranked by payoff then risk:

1. compaction tasks: keep only unfinished tasks in memory. Finished ones
   stay up to about 1.5h today, each with a full schema copy. Readers of
   finished tasks (state queries, cleanup, the clustering policy's
   latest-trigger lookup) page the catalog or use a small index.
2. import jobs: same pattern. Finished jobs stay 48h with schema and
   file lists, and boot runs 7 list calls in a row. Copy-segment,
   snapshot-export and external-refresh jobs follow the same pattern.
3. partition stats: boot reads the current version once per
   (channel, partition), one at a time. Replace with one prefix list.
   Small; can go first.
4. stats tasks: keep only unfinished tasks.
5. analyze tasks: fix the missing lifecycle (they are never marked
   dropped) before considering lazy loading.
6. index meta: biggest payoff, highest risk. Separate design (slim the
   resident record, e.g. keep `IndexFileKeys` out of memory, or move
   segment indexes under MetaStore).

The rule every M2 design must follow: GC, stats dedupe and import
idempotency act when an ID is absent from meta. With a partial cache, a
miss must fall back to a catalog read, or the scan must page the
catalog. Otherwise GC deletes live files.

Out of scope for M2: QC (its task scheduling and balance are not
persisted, and its other meta covers loaded collections only),
snapshot meta (already lazy where it matters), and RC credentials and
RBAC (not cached).

### M3 — Later

- QC reads segment details through `MetaView` instead of the broker,
  and the `GetRecoveryInfoV2` round trip on each next-target update
  goes away. This includes deciding where DmChannel info lives (#53579
  Open Question 1).
- MetaStore persists segment writes itself instead of DC calling
  `catalog.AlterSegments` separately (#53579 Open Question 3).
- Enforce proto immutability for callers of MetaView/MetaStore, which
  is convention-only today.
- Evict collections (and stop backfilling dropped ones), together with
  GC-gated segment eviction.

## Order

```
M1.1 ─┐
M1.2 ─┤ (independent, small)
M1.3 ─┤
M1.4 ─┘
M1.5 ── M1.6 ── M1.7 ── M1.8
M2 design ── M2.3 ── M2.1 ── M2.2 ── M2.4 ── M2.5   (parallel to M1)
M3 after M1.7
```

## Verification Approach

Each behavior change follows the repo verification gate: trace every
failure mode end to end (not only the success path), and claim only
what is verified. Specifically:

- M1.4: a test that interleaves QC reads with a compaction commit and
  never sees the inputs dropped without the outputs.
- M1.7: table-driven tests that run each DDL (create, drop, alter
  schema and properties, rename, create and drop partition, truncate,
  alias) and check the `MetaView` result against RC's model right after
  the ack; plus a test that the old lazy-backfill race cannot happen.
- M2: for each lazily loaded meta, a test that a cache miss on a
  finished record reaches the catalog, and that GC does not reclaim
  files of records not resident in memory.
