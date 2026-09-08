# Design Document: File-based Bulk Delete and Upsert (import delete / upsert)

**Date**: September 2026
**Status**: Proposed
**Feature DRI**: @bigsheeper

> Milvus can already bulk-write with files as input, but it cannot bulk-delete with files
> as input. This document adds that path, and lays out the principle and design for how
> deletes take effect on queries after they are written.

- **Visibility**: eventual
- **Delivery path**: reuse the existing load-L0 chain

---

## 1. Background

Milvus's write path already has import: the user stages parquet (or other) files on object
storage, submits one import job, and the data is parsed into sealed segments. But delete
and update exist only as streaming RPCs. In other words, **file-based bulk delete and
whole-row overwrite have no path today**.

### 1.1 Typical scenarios

- **Large-scale corpus deduplication**: vector clustering identifies duplicate samples and
  emits a list of primary keys to delete — one pass removes 20%~50% of the data. For
  typical industry deletion ratios see SemDeDup (LAION-440M, 50% removed with essentially
  unchanged training quality).
- **Training-set pruning**: a quality model scores rows; low scorers are deleted.
- **Compliance deletion**: bulk-remove all data for given business IDs, under a physical
  deletion deadline.
- **Batch rollback**: one imported batch is bad; delete it wholesale and re-import.
- **Knowledge-base document refresh**: document content changed; overwrite whole rows by
  primary key.

These scenarios share one shape: **the keys to delete are the output of an upstream batch
job and already sit on object storage as files**, at the 10^7–10^9 scale — impossible to
stuff into a single RPC.

### 1.2 Functional requirements

- **File-based delete**: given files containing the primary-key column, delete the
  corresponding data.
- **File-based upsert**: given files of complete rows, overwrite the whole row when the
  primary key exists, insert when it does not.
- **Delete-write atomicity**: an upsert's delete and write take effect in the same commit.
- **Observable progress**: reuse the import job's state and progress queries.

## 2. Design

### 2.1 Interface

Extend the existing `ImportRequest` with a new `write_mode` option; no new RPC:

```protobuf
ImportRequest {
  collection_name, partition_name
  files[]                                    // existing, shared by all three modes
  options[]                                  // existing; new key:
                                             //   write_mode: append | upsert | delete
                                             //   (default append)
}
```

Semantics of the three modes:

| `write_mode` | what `files` contains | semantics |
|---|---|---|
| `APPEND` | full-schema data | write only (existing behavior) |
| `UPSERT` | full-schema data | write, and delete old rows by these rows' primary keys |
| `DELETE` | at least the primary-key column | delete only |

`UPSERT` needs no extra delete keys from the user — the delete keys are the data file's
own primary-key column. All three modes therefore share one file list; no second file
field is needed.

Submit the same parquet three times changing only `write_mode`, and the semantics are
"write / overwrite / delete".

### 2.2 Delete-file specification

- **Format**: parquet first. It is the default output of upstream Spark / Ray / DuckDB
  jobs, import already has the corresponding reader, and its row-group statistics are
  usable for later optimizations. Other formats (JSON / CSV) come almost for free by
  sharing the reader, but the documentation recommends parquet.
- **Content**: must contain one column whose name and type match the collection's
  primary-key field. **All other columns are ignored** — a dedup job's output is typically
  `doc_id | cluster_id | similarity | kept_doc_id`, and users should not be required to
  project it down to a single column first.
- **Ordering**: not required. The preimport stage already reads files to count rows; it can
  also use parquet row-group min/max to detect whether the file is sorted by primary key,
  and choose the downstream dispatch algorithm accordingly.
- **File splitting**: multiple files parallelize naturally; the import job already fans out
  by file.

### 2.3 Other decisions

1. Does `DELETE` mode accept files with the full schema?
   1. Yes; only the primary-key column is read. Users may directly reuse existing data
      files.
2. Support "delete a batch A and simultaneously write an unrelated batch B"?
   1. No. That needs two independent file lists — added interface complexity for an
      uncommon combination. Split into two jobs when needed, at the cost of atomicity
      between them.

## 3. Architecture

```
upstream jobs                                Milvus
─────────────           ──────────────────────────────────────────────────
dedup / prune /   ──> parquet (PK column only) ─┐
compliance                                      │
data producers    ──> parquet (full rows) ──────┤ ImportRequest{files, write_mode}
                                                ▼
━━━━━━━━━━━━━━━━━━ Fence A · Import broadcast ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  one Import message lands per vchannel; the job starts only after all ack
  T_import := message timetick  ← the timestamp on every row and every
                                  delete record this job writes

  Pending → PreImporting → Importing → Sorting → IndexBuilding → Uncommitted
                               │          └───────┬───────┘
                               │            L1 only; L0 skips these
                               ▼
        APPEND → L1
        DELETE → L0 (delete keys)
        UPSERT → L1 (full rows) + L0 (companion delete keys)
                    ↑ all segments IsImporting=true: invisible to queries

━━━━━━━━━━━━━━━━ Fence B · CommitImport broadcast ━━━━━━━━━━━━━━━━━━━━━━━━
  → Committing (the decision is in the WAL; no going back)
  each vchannel's flusher applies it; the vchannel's L1 and L0 flip together:
      commit_timestamp := this message's timetick on this vchannel
      IsImporting = false          ← the visibility switch
  all vchannels done → Completed

  ── beyond this point is no longer 2PC ──
  Path A (implemented)   L0 compaction folds deletes into each target
                         segment's deltalog + advances its manifest
                         → QueryCoord compares manifests → Reopen
                         → deletes take effect on queries
  Path B (this document, "Visibility" chapter)
                         deliver the L0 directly into the delegator
                         → see "Condition 1 / Condition 2"
```

## 4. Visibility

### 4.1 Design principle: eventual visibility

**Deletes take effect asynchronously after commit; "queryable as soon as the commit
returns" is not promised.**

### 4.2 The existing load-L0 chain

Today the only action that loads L0 segments happens at channel subscription:

```
trigger: WatchDmChannels
   │
DataCoord   computes vchannel recovery info
   └─ L0 segment IDs go into VchannelInfo.LevelZeroSegmentIds
QueryCoord  fillSubChannelRequest
   └─ merges the L0 IDs into the batch metadata fetch
   └─ puts them into the request's segment-info table
QueryNode   loadL0Segments
   └─ assembles SegmentLoadInfo
delegator   LoadL0 → RegisterL0
   └─ per l0ForwardPolicy: read the data, or build a shell only
   └─ hangs it into the delete buffer
   │
applied to segments
```

For import-produced L0 segments to become visible on the read path, two conditions are
missing:

### 4.3 Condition 1: load new L0 while the collection is already loaded

**Approach: piggyback on `TargetObserver`'s periodic sync, carried by an optional field on
`SyncAction`.**

`TargetObserver` already iterates every collection's ready delegators periodically:
`genSyncAction` assembles a `SyncAction`, and `syncToDelegator` sends one
`SyncDistribution`. Attaching the L0 `SegmentLoadInfo` list as an **optional field** on
that `SyncAction` is sufficient — no new RPC, and no new `SyncType` enum value.

Fetching the metadata can mirror `fillSubChannelRequest` — it merges the vchannel's
`LevelZeroSegmentIds` into a batch metadata fetch. The two live in different packages and
build different request types, so the code cannot be shared directly; but
`syncNextTargetToDelegator` already calls `DescribeCollection` and `ListIndexes`, and one
more `broker.GetSegmentInfo` is the same class of operation.

```protobuf
message SyncAction {
    SyncType type = 1;
    int64 partitionID = 2;
    int64 segmentID = 3;
    int64 nodeID = 4;
    int64 version = 5;
    SegmentLoadInfo info = 6;
    repeated int64 growingInTarget = 7;
    repeated int64 sealedInTarget = 8;
    int64 TargetVersion = 9;
    repeated int64 droppedInTarget = 10;
    msg.MsgPosition checkpoint = 11;
    map<int64, int64> partition_stats_versions = 12;
    msg.MsgPosition deleteCP = 13;
    map<int64, int64> sealed_segment_row_count = 14;
+   // L0 segments to register into the delegator's delete buffer.
+   // Carried on UpdateVersion actions; delegator dedups by segment ID.
+   repeated SegmentLoadInfo level_zero_segments = 15;
}
```

### 4.4 Condition 2: apply new L0 to already-loaded segments

**Approach: reuse the streaming-delete `forwardStreamingByBF` machinery.**

That structure does exactly what is needed: `PinOnlineSegments` pins the current segment
distribution, `applyBFInParallel` sweeps the delete primary keys once and runs bloom-filter
tests against all segments in parallel, hits are aggregated per segment, then `applyDelete`
sends them to each segment's node — sealed via `DataScope_Historical`, growing via
`DataScope_Streaming` — and finally `Unpin` releases.

The inputs are all present: the distribution's `SegmentEntry` already records each
segment's `NodeID` and its bloom filter (growing entries hold the segment itself), so no
external information is needed. The only work is reading the L0's delete records out and
feeding them in.

### 4.5 Flow

```
QueryCoord (TargetObserver periodic sync)
  ├─ take the vchannel's LevelZeroSegmentIds from the target
  ├─ fetch metadata the way fillSubChannelRequest does
  └─ genSyncAction attaches the optional field → syncToDelegator

QueryNode (SyncDistribution handling — synchronous; failures are logged
           only and never fail the sync)
  ├─ LoadL0 → RegisterL0 (dedup by segment ID; count only on success)
  │                              → covers "segments loaded afterwards"
  └─ Forward Delete: PinOnlineSegments, then forward to existing segments
     per l0ForwardPolicy
           local filter: read the delete records, go through
                         forwardStreamingByBF
           remote load:  push deltalog paths for each node to read itself
                                 → covers "segments already loaded"
```
