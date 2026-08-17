# Design Document: File-based Bulk Delete and Upsert via Import (`write_mode`)

**Date**: August 2026
**Status**: Implemented (phase 1)
**Scope**: `ImportRequest.write_mode`, delete-key file reading, L0 segment emission from import, per-segment level allocation, admission gates
**Feature DRI**: @bigsheeper
**Tracking issue**: milvus-io/milvus#52567
**Source design discussion**: https://zilliverse.feishu.cn/docx/K2p5d27iXopSc3xMfXGcHhiznXc

---

## 1. Overview

### 1.1 Motivation

Milvus's write path already accepts files: a user stages parquet on object storage and submits one `Import` job, which parses it into sealed segments. Deletion and update have no such path — `Delete` and `Upsert` are streaming RPCs only.

The workloads that need file-based deletion are exactly the ones where import is already the natural entry point, because the keys to remove are the output of an upstream batch job and already sit on object storage:

- **Corpus deduplication** — clustering identifies near-duplicates and emits primary keys to drop. Published ratios are large (SemDeDup removes ~50% of LAION-440M with essentially unchanged training quality), so one pass may delete 20–50% of a collection, in the 10^7–10^9 key range.
- **Training-set pruning** — a quality model scores rows, low scorers are dropped in bulk.
- **Compliance deletion** — remove every row for a set of business IDs, often under a physical-deletion deadline.
- **Batch rollback** — one ingest batch was bad; drop it wholesale and re-import.
- **Knowledge-base refresh** — documents changed; overwrite whole rows by primary key.

Replaying these as millions of individual RPCs forces every caller to hand-roll batching, retry, backpressure and progress tracking that the import job already provides.

### 1.2 Non-goals

- Mixing "delete set A and write unrelated set B" in one job. That needs two independent file lists for an uncommon combination; two jobs achieve it at the cost of atomicity between them.
- Metadata-only predicate deletion (by partition key, or by conditions matching whole files).
- Deletion by non-primary-key columns, in the spirit of Iceberg equality deletes.

---

## 2. Interface

`write_mode` extends the existing `ImportRequest` rather than adding an RPC:

```protobuf
enum ImportWriteMode {
  Append = 0;
  Upsert = 1;
  Delete = 2;
}

message ImportRequest {
  string db_name = 1;
  string collection_name = 2;
  string partition_name = 3;
  repeated ImportFile files = 4;
  repeated common.KeyValuePair options = 5;
  ImportWriteMode write_mode = 6;
}
```

| `write_mode` | what `files` contains | semantics |
|-|-|-|
| `Append` | full-schema rows | write only (existing behaviour) |
| `Upsert` | full-schema rows | write, and delete pre-existing rows carrying those primary keys |
| `Delete` | at least the primary key column | delete only |

Three properties of this shape are deliberate:

- **`Upsert` needs no second file list.** The delete keys are the data file's own primary key column, so all three modes share one `files` field. The same parquet submitted three times with only `write_mode` changed means "write / overwrite / delete".
- **Delete-key files ignore extra columns.** A dedup job emits something like `doc_id | cluster_id | similarity | kept_doc_id`; requiring a projection to one column first is friction for no benefit.
- **Progress reuses the import job state machine**, so `GetImportProgress` works unchanged.

### 2.1 Why the value also rides in `options`

The typed field is what users set, but every component downstream reads the mode out of the request's `options` key-value list, and the proxy folds one into the other at `PreExecute`.

The reason is the WAL. An import is broadcast as `msgpb.ImportMsg`, which lives in the external `milvus-proto` repository and carries only `options map<string, string>` — a typed field cannot cross it. Folding into `options` means DataCoord, DataNode and CDC replicas all observe the same value with no external proto change, and it matches how `l0_import`, `backup` and `auto_commit` already travel.

The proxy rejects a request whose typed field and `options` value disagree, and rejects `write_mode` combined with `backup` or `l0_import`.

---

## 3. Data flow

```
user
  └─ ImportRequest{files, write_mode}
       │
   proxy PreExecute
       ├─ fold write_mode into options
       ├─ reject: conflicting field/options, backup, l0_import, autoID upsert
       └─ delete mode with no partition_name -> AllPartitionsID
       │
   DataCoord admission
       ├─ checkL0ImportAllowed        (legacy backup-restore L0 import only)
       └─ checkWriteModeSupported     (storage v3 required; see §6)
       │
   preimport
       └─ delete-key files are read through a primary-key projection,
          converted to delete records, and counted/hashed exactly like rows
       │
   AssignSegments
       ├─ Append  -> {L1}
       ├─ Delete  -> {L0}
       └─ Upsert  -> {L1, L0}  per (vchannel, partition)
       │
   DataNode ImportTask
       ├─ Delete  -> delete records into the L0 segment
       ├─ Upsert  -> rows into L1, companion deletes into L0
       └─ Append  -> unchanged
       │
   2PC commit fence (CommitImport WAL message)
       └─ per vchannel: set commit_timestamp, clear is_importing
       │
   L0 compaction -> deltalog + manifest bump -> SegmentChecker -> segment Reopen
       │
   visible to queries
```

---

## 4. The delete timestamp invariant

This is the mechanism that makes `Upsert` atomic without any extra machinery, and it is the single easiest thing to break by a well-intentioned change.

**Rule: delete records written by an import are stamped with the import request's own timetick (`ImportRequest.ts`, call it `T_import`) — never with the commit timestamp, never with a value derived in the write path.**

Why that is correct, traced end to end:

1. The persisted binlog row timestamp of an import segment is **also** `T_import`: `AppendSystemFieldsData` writes `req.GetTs()` into the timestamp column and no write path rewrites it.
2. The 2PC commit fence writes one **segment-level** field: `HandleCommitVchannel` sets `commit_timestamp` on every segment of the job, valued at the WAL timetick of the `CommitImport` message, which is later than `T_import`.
3. segcore applies `commit_timestamp` as a **load-time overlay**. When `commit_ts != 0`, the in-memory row-timestamp array is materialized as a constant `commit_ts` and every timestamp reader short-circuits to it. Decisively, the sealed segment's delete-filter callback is constructed with a null insert record and compares `delete_ts > commit_ts` directly — it never reads a row timestamp.
4. Therefore the comparison at query time is `T_import` vs `commit_ts`, not `T_import` vs `T_import`. A companion delete removes a pre-existing row (whose effective timestamp is its own, earlier) and can never remove the row this job just wrote.
5. After non-L0 compaction the overlay becomes a genuine persisted rewrite (row timestamps become `commit_ts`) and the field is cleared to 0. The compaction-side delete filter uses `max(row_ts, commit_ts) < delete_ts`, strict, so the margin survives compaction.
6. L0 segments are exempt from all of this. `L0Segment` on the query node is a plain Go structure holding primary keys and timestamps; nothing rewrites its delete timestamps. A second, independent layer of protection comes from `segmentEffectiveTs`, which uses `commit_ts` as the delete-buffer watermark, so a `T_import`-stamped buffered delete is not even forwarded to the job's own data segment.

**Correction worth recording**: the `delete_ts <= insert_ts` skip in `DeletedRecord.h` is *not* what protects this path. It reads the insert record, which is null for sealed segments, so that branch is inert there. Reasoning about this feature from that rule leads to a wrong model of how much margin exists.

**Residual**: the strictness of `commit_ts > T_import` is an ordering argument (the commit message is broadcast only after all import tasks complete), not an asserted invariant. The only code-level guard is `commit_ts >= max(binlog.TimestampTo)`, which admits equality. That is harmless today because both the segcore and compaction conditions are strict, so equality still skips — but changing either to a non-strict comparison would be unsafe at exactly that boundary, and the CDC path (where `DataTs` comes from a source-cluster TSO) is the case the guard exists to police.

---

## 5. Segment level becomes a per-segment property

Before this change an import job produced exactly one segment level, chosen job-wide from the `l0_import` option. `Upsert` breaks that: one job must produce both L1 data segments and L0 delete segments.

Consequences threaded through the implementation:

- `ImportRequestSegment` gains a `level` field, and DataNode gains `PickSegmentByLevel` so the row-data path and the delete path can each select their own target. The row-data path filters L0 out of its candidate set rather than requiring an exact L1 match — `SegmentLevel`'s zero value is `Legacy`, so an older DataCoord's level-less request keeps working under an exclusion filter but would hard-fail under an exact match.
- **Storage version is chosen per level, not per job.** The delete write path is always storage v2, so an L0 segment is recorded as v2 even on a cluster writing data segments as v3. This does not affect delete visibility: L0 compaction advances the manifest of the *target* data segment, keyed on that segment's manifest path, not the L0's own storage version.
- **L0 sizing is derived from row count, not byte size.** The delete payload is primary keys plus timestamps, not full rows, so sizing the L0 round by the row-data byte count over-allocates severely (100 GB of rows against a 16 MiB delete-buffer limit yields thousands of L0 segments where a handful suffice). `partition_rows` from the preimport stats is the correct basis.
- Stage gating moves from job level to segment level: an upsert job's L1 segments still need sort compaction and index building, its L0 segments need neither, and whether a segment's position comes from the delta or insert timestamp range depends on that segment's level.

---

## 6. Visibility, and the storage-v3 requirement

Deletes become visible asynchronously. The API does not promise read-after-commit.

An import-produced delete reaches an **already-loaded** data segment through exactly one path: L0 compaction folds the delete into the target segment's deltalog and advances that segment's manifest, and QueryCoord's `SegmentChecker.isSegmentUpdate` turns the manifest bump into a segment Reopen. There is no delegator fast path.

A segment with no manifest never matches that comparison. `DataVersion` is bumped only by column-group backfill, not by L0 compaction, so it does not provide an alternative signal. On such a collection the deletes would stay invisible **indefinitely** — not "eventually visible" but silently wrong results with no bound, until an operator happened to release and reload.

That is silent data corruption, so `write_mode=Delete/Upsert` **fails closed** on any collection still holding manifest-less data segments, in the same spirit as the existing `dataCoord.import.enableL0Import` gate. The check inspects the collection's actual segments rather than only the cluster config, so a collection whose history spans a config change is caught too.

The gate parses `write_mode` *before* consulting any predicate: `IsDeleteMode`/`ProducesDeletes` swallow a parse error and answer as if the mode were `Append`, so asking them first would let a malformed value through as a plain append. A CDC-replicated import message reaches `createImportJobFromAck` without passing the proxy, making this the first and only place the value is checked on that path.

### 6.1 The `enableL0Import` gate is narrowed, not reused

`dataCoord.import.enableL0Import` defaults to false and previously rejected every L0 import. Its purpose is to stop one specific corruption: a backup restore where L0 deltalogs carry the *source cluster's original* timestamps while data segments are stamped with `commit_ts`, producing `delete_ts < insert_ts` so deletes are silently dropped.

A `write_mode` job stamps its deletes with this transaction's own timetick and is not that combination, so the gate is scoped to `l0_import` and no longer blocks it.

---

## 7. Partition routing

`Delete` and `Upsert` route their deletes differently, and the asymmetry is intentional.

A delete-key file carries only primary keys — there is no partition key value to hash by. So the proxy confines a delete-mode job to a single partition (`AllPartitionsID` when no partition name is given, mirroring streaming upsert), and the delete data is hashed by vchannel only.

`Upsert` reads full rows and therefore has the real partition key. Its companion deletes are hashed by **both** vchannel and partition, using the same hash functions the row data uses, so a row's companion delete lands in the same `(vchannel, partition)` bucket as the row itself. `AssignSegments` allocates both levels per pair, so the target L0 segment always exists exactly where the L1 one does.

Routing all companion deletes to a single partition would be silently wrong on a partition-key collection: L0 deltas are partition-scoped, so deletes filed under the wrong partition never reach their rows and the upsert leaves duplicates behind.

---

## 8. `autoID` collections

`write_mode=Upsert` is rejected at admission when the primary key is auto-generated. The import file supplies no primary keys, so there is nothing to overwrite.

Without the gate the failure is silent rather than loud: `InsertData` pre-allocates a field entry for every schema field including the auto primary key, so a presence check on that column always passes even though the reader never populated it. Extraction then yields zero delete records and the import degrades to a plain append, with the user believing deduplication happened.

---

## 9. Verification

Unit coverage accompanies each component. Two end-to-end tests exercise the full chain against a real cluster:

- **Delete** — imports N rows, then a delete-key file holding half those primary keys *plus a column absent from the collection schema*. The extra column is the point: it proves the datanode reads through a primary-key projection that ignores unknown columns rather than failing on them. Row count settles at N/2.
- **Upsert** — imports N rows, then a full-row file for the same primary keys with a distinctive marker. Row count is deliberately invariant across an upsert, so the count cannot serve as the readiness signal; the test polls until every row carries the new marker and asserts the count afterwards, where 2N would mean the companion delete never applied and 0 would mean it removed the rows the job itself wrote.

Observed on a local cluster: the delete transition completed within one 3-second poll; the upsert marker distribution held at 0/N for roughly fifteen seconds and then flipped to N/0 in a single poll, with the total row count never leaving N and no sample ever showing a mixture of old and new markers.

---

## 10. Future work

1. **Metadata-only predicate deletion** — deleting by partition key or by conditions that match whole files, ideally without reading data. Useful for retention-window pruning.
2. **Deletion by non-primary-key columns**, in the spirit of Iceberg equality deletes. The file format can reserve a way to declare the match columns so this stays additive.
3. **Lower visibility latency** — L0 compaction advances the target segment's manifest, but QueryCoord only observes it when the next target refreshes, which is expiry-driven (`queryCoord.NextTargetSurviveTime`, 300s by default). Refreshing the target when an import-produced L0 is compacted would remove that wait. Import-produced L0 segments are identifiable by a non-zero `commit_timestamp`, which streaming-produced ones never carry, so the refresh can be restricted to them and add no load to the ordinary compaction path.
