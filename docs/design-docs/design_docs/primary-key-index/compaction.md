# Primary Key Index Compaction

- **Created:** 2026-10-16
- **Author:** bigsheeper
- **Status:** Draft
- **Component:** DataCoord, DataNode, `internal/datacoord/pkindex/compaction`, `internal/datanode/pkindex/compaction`
- **Related work:** Primary Key Index Manifest Service (`manifest.md`), Primary Key Index Core Library (`core-library.md`), issue #52305

## Summary

The primary key index keeps one LSM tree for each vchannel. The manifest of
the vchannel lists the SST files of the tree, newest first.

A **handover** is the step in which the streaming node uploads its frozen
in-memory data as SST files and commits them to the manifest as a flush (see
"Handover cycle" in `core-library.md`). Each flush adds tables to the front of
the list.

Compaction merges a contiguous range of these tables into new tables and
replaces the range in the manifest. DataCoord plans the work and commits the
result. A data node does the merge. The manifest service stays the only
writer of the manifest.

## Problem

Without compaction, the table list of a vchannel grows with each handover.
This has two costs:

- **Read cost.** A lookup probes every table whose key range covers the key.
  User primary keys are often random, so every flushed table covers almost
  the whole key space. The number of probes grows with the number of tables.
- **Space cost.** An upsert writes a new value for a key that an older table
  already holds. A delete writes a tombstone. The old values and the
  tombstones stay in object storage forever.

## Scope

In scope:

- The run structure of the manifest and the checks of the compaction edit.
- The trigger rules and the planning in DataCoord.
- The task orchestration in the generic task scheduler of DataCoord.
- The merge on the data node.
- The link with the GC of the manifest service.

Out of scope: see "Known Limits and Follow-ups".

## Runs in the Manifest

A **run** is a set of tables whose key ranges do not overlap. The proto field
`SSTRef.run_id` in `pkg/proto/pkindex.proto` records it.

```protobuf
message SSTRef {
  ...
  // 0: the SST came from a flush and is a run of its own (an L0 table).
  // Otherwise the SSTs of one compaction share one run id; their key
  // ranges do not overlap and they are listed in ascending key order.
  int64 run_id = 7;
}
```

Rules of the model:

- **Flushed table.** A table with run id 0 comes from a flush. This document
  also calls it an L0 table. Each flushed table is a run of its own. A flush
  always writes run id 0. `applyFlush` sets the field to 0 and ignores the
  value in the request.
- **Compacted run.** All outputs of one compaction share one nonzero run id.
  Their key ranges do not overlap. The list holds them in ascending key order.
  The tables of one run have the same age, so their order in the list does not
  tell new from old.
- **Flushed-table prefix.** The flushed tables are always a prefix of the
  newest-first list. A flush adds tables only at the front. A compaction
  never puts a flushed table behind a run.
- **Run id source.** DataCoord takes the run id from the global ID allocator
  when it plans the task. The allocator also gives the SST IDs, so a run id is
  unique.

The list stays flat. A reader that does not know runs still gets correct
results: it checks the min and max key of each table, and the order inside a
run does not matter.

### Checks of the compaction edit

`applyCompaction` in `internal/datacoord/pkindex/manifest/manager.go` applies
a compaction to the current manifest. It keeps the rules of the manifest
service: the inputs are a contiguous range of the list in the same order, and
the outputs take the position of the inputs. It adds these checks, in
`checkRunEdit` and `checkL0Prefix`:

1. The inputs do not split a run. The table before the first input and the
   table after the last input must not share a nonzero run id with that input.
2. The outputs form one new run. All outputs have the same nonzero run id,
   and the list does not contain this run id.
3. Each output has a min key that is not above its max key.
4. The key ranges of the outputs are in ascending order and do not overlap.
5. After the edit, the flushed tables are still a prefix of the list.

A failed check returns `ErrStaleEdit`. The same error covers a concurrent edit
and a malformed compaction.

Two more rules apply:

- **Empty outputs.** The manager accepts empty outputs. This occurs when all
  keys of the inputs are tombstones and the task drops tombstones. The task in
  DataCoord rejects an empty result when it does not drop tombstones, because
  then the inputs hold live entries.
- **Applied retry.** When no input is in the list and every output is in the
  list, the manager returns the current version and writes nothing. For
  non-empty outputs, this proves that the edit took effect, because the
  outputs have new IDs that no other edit can add. For empty outputs, the
  check reduces to "no input is in the list". Another compaction that replaced
  the same inputs also satisfies it.
- **Duplicate outputs.** An output ID that is already in the list, or that the
  outputs repeat, returns `ErrDuplicateSST`. The manager does this check before
  the run checks. It does not delete its cache entry for this error.

A flush and a compaction still commute. A flush adds tables at the front. A
compaction replaces a range that starts and ends at a run boundary. It does not
split a run, and the flushed tables stay a prefix.

## Trigger Rules

The planner splits the list into groups: one group per flushed table and one
group per compacted run (`groupSSTs` in
`internal/datacoord/pkindex/compaction/summary.go`). The size of a group is the
sum of the `size` of its tables. The rules are in `policy.go`.

| Rule | Condition | Inputs | Drops tombstones | Score |
|---|---|---|---|---|
| Flushed tables (`l0`) | The list has at least `l0Trigger` flushed tables | The oldest flushed tables, at least two, at most `maxInputTables`, and at most `maxInputBytes` once two are in | Only when no compacted run follows the flushed tables, so the inputs reach the end of the list | flushed tables / `l0Trigger` |
| Size-tiered merge (`merge`) | At least `minMergeRuns` consecutive compacted runs, with the largest at most `sizeRatio` times the smallest and the sum at most `maxInputBytes` | All tables of the best window. The best window has the most compacted runs, then the fewest bytes | Only when the window ends at the last compacted run | compacted runs in the window / `minMergeRuns` |
| Space amplification (`spaceamp`) | At least two compacted runs, an oldest run larger than 0 bytes, and bytes outside the oldest run at least `spaceAmpRatio` times the oldest run | Every table of the list | Always | (total - oldest run) / (oldest run × `spaceAmpRatio`) |

Notes:

- **Tombstones.** A tombstone hides older values of its key. It can drop only
  when no older table can hold the key. Thus a merge drops tombstones only
  when its inputs reach the end of the list.
- **Choice per vchannel.** `evaluate` takes the rule with the highest score.
  On equal scores the flushed-table rule wins, then the merge rule, then the
  space rule.
- **Caps.** `maxInputBytes` applies to the flushed-table rule and to the merge
  rule. `maxInputTables` applies to the flushed-table rule only. The space rule
  has no cap, because its purpose is to rewrite every table.
- **Why the space rule.** Upserts make the newer runs hold overwrites of keys
  in the older runs. The merge rule alone never merges a small new run into a
  large old run. Without the space rule, the space and read cost have no
  bound.

The inspector reads the configuration again on each inspection.
`Config.normalize` in `config.go` replaces a value that would make a rule
meaningless. `logCorrections` logs the corrections each time the set of
corrections changes:

| Field | Bad value | Value used |
|---|---|---|
| `l0Trigger`, `minMergeRuns`, `maxInputTables` | Below 2 | 2 |
| `sizeRatio` | Below 1 | 1 |
| `spaceAmpRatio` | 0 or below | 1 |
| `maxInputBytes` | 0 or below | No cap |
| `targetFileSize` | 0 or below | 128 MiB |
| `maxConcurrent` | Below 1 | 1 |
| `inspectInterval` | 0 or below | 10 s |
| `failureCooldown` | Below 0 | 0 |
| `dataCoord.slot.pkindexCompactionUsage` | Below 1 | 1 |

## Planning and Orchestration

The coordinator side is in `internal/datacoord/pkindex/compaction`. DataCoord
builds it in `initPKIndex` in `internal/datacoord/pkindex_service.go`. The
switch `dataCoord.enableCompaction` of segment compaction does not control it.

```
manifest.Manager ──ManifestChanged──▶ Inspector ◀──ManifestChanged── GC round
                                         │ score, plan, allocate IDs
                                         ▼
                                     task table (memory only)
                                         │
                                         ▼
                              generic task scheduler ──CreateTask──▶ data node
                                         │ QueryTask ◀───────────────┘
                                         ▼
                              CommitCompaction ──▶ manifest.Manager
task table ──InFlightOutputs──▶ GC
```

### Inspector

`Inspector` keeps a summary of each vchannel that it knows. It learns of a
manifest through the `manifest.Observer` interface from two sources:

- The manifest manager calls `ManifestChanged` after each commit.
- The GC calls `ManifestChanged` with each current manifest that a round
  reads. The GC does not report a vchannel whose layout check fails.
- With an invalid GC configuration, DataCoord disables the GC. The inspector
  then learns only from commits.

The inspector keeps the highest version per vchannel and marks the vchannel
as changed. Every `inspectInterval`, it does these steps:

1. It skips a changed vchannel that has a task or is in its failure cooldown.
2. It scores the other changed vchannels. A vchannel with no rule that applies
   loses its mark.
3. It sorts the candidates by score, then by the time of the first change that
   is not planned yet.
4. It launches candidates until `maxConcurrent` tasks are in flight.

To launch a task, the inspector reads the full manifest with
`Manager.Current`. This read uses the cache of the manager, so it can lag a
commit of another coordinator. A read error starts the failure cooldown. When
`Current` returns no manifest, the inspector forgets the vchannel. Otherwise
the inspector plans on this manifest and allocates two IDs: the task ID and
the run ID. When `AllocN` fails, the vchannel keeps its mark, and the next
inspection tries again. A vchannel has at most one task at a time.

### Task

`Task` in `task.go` implements `task.Task` of the generic task scheduler of
DataCoord. Its type is `taskcommon.PKIndexCompaction`. Its slot is
`dataCoord.slot.pkindexCompactionUsage`. It calls the node through three
wrappers in `internal/datacoord/session/cluster.go`:
`CreatePKIndexCompaction`, `QueryPKIndexCompaction` and
`DropPKIndexCompaction`. The contract of these wrappers:

- A non-nil error is a transport failure or a node that cannot be reached
  (`merr.ErrNodeNotFound`).
- Query state `taskcommon.None` with a nil error means that the node does not
  know the task.
- A failed task carries its class in `CompactionResult.retriable` and its text
  in the reason.

### Task table in memory only

The task table (`table.go`) holds each task from its creation until it
finishes or fails. It is not persisted. A coordinator restart forgets all tasks
in flight. This is safe for these reasons:

- A forgotten task is never committed, so the manifest does not change.
- Its outputs are in no manifest and not in the in-flight set. The GC collects
  them as orphans after `dataCoord.pkindex.gc.grace`.
- On the data node, the executor deletes a finished or failed task that no
  coordinator queries for `dataNode.pkindex.compaction.terminalTaskTTL`. See
  "Terminal TTL" below.

The cost is the work of the tasks that were in flight, at most
`maxConcurrent` merges.

### Output IDs and the GC

Each dispatch reserves a fresh output ID range of
`ceil(inputBytes / targetFileSize) × 2 + 4` IDs (`outputIDCount` in
`plan.go`). The task ID and the run ID stay the same across dispatches. A
retry never reuses an ID range, because the node of an earlier attempt can
still write objects under the old IDs.

`Inspector.InFlightOutputs` is the `gc.InFlightOutputs` source of the GC. It
returns the current ID range of the task of the vchannel, if any. The GC reads
this set before the pointer, so a commit between the two reads cannot lose its
outputs. A task records its range only after the dispatch succeeds. Thus a
node can upload for a short time before the range is in the set. The `grace`
of the GC covers this time. The range of an earlier attempt is not protected.
The GC collects it as orphans.

### Commit

`Task.QueryTaskOnWorker` commits a finished result. It runs on the check pool
of the scheduler, so each step has a bound:

1. It checks that the result belongs to the current attempt. Each output must
   have the run ID of the task and an ID inside the current range. A result of
   an earlier attempt puts the task back to dispatch.
2. It rejects an empty result when the task does not drop tombstones.
3. It calls `CommitCompaction` with a deadline of 10 s.
4. On success, or an applied retry, the task finishes.
5. On `pkindex.ErrUnavailable`, a deadline or a cancel, it waits with an
   exponential backoff from 1 s to 60 s, then commits again. The task stays in
   the table, so the GC keeps its outputs. Each scheduler round still queries
   the node first. If the node is lost during the backoff, the task goes back
   to dispatch, and the outputs of the first attempt become orphans.
6. On `ErrStaleEdit` or `ErrNoManifest`, the task fails with a warning. Other
   errors, for example `ErrDuplicateSST`, fail the task with an error log.

A query transport error, or a terminal state without a result payload, makes
the task query the node again at the next scheduler round. This has no limit.

When `applyCompaction` returns `ErrStaleEdit`, the manager deletes the cache
entry of the vchannel. The next load reads the pointer and the body from the
stores.

A finished or failed task leaves the table, and the inspector marks its
vchannel as changed. The inspector does not plan a failed vchannel again for
`failureCooldown`.

The inputs of a committed compaction stay in object storage for some time.
The GC deletes them only after no retained manifest version lists them and
they are older than `grace`.

## Execution on the Data Node

The data node side is in `internal/datanode/pkindex/compaction`. The shell
`internal/datanode/pkindex.go` builds the `Executor` and serves the three
task RPCs.

### Messages

```protobuf
message CompactionRequest {
  int64 task_id = 1;
  int64 collection_id = 2;
  string vchannel = 3;
  string sst_base_path = 4;    // Manifest.sst_base_path
  repeated SSTRef inputs = 5;  // newest first, a contiguous slice of the list
  int64 run_id = 6;            // the run id of every output
  int64 output_id_begin = 7;   // outputs take IDs from [begin, end)
  int64 output_id_end = 8;
  int64 target_file_size = 9;
  bool drop_tombstones = 10;
}

message CompactionResult {
  common.Status status = 1;
  repeated SSTRef outputs = 2;  // ascending key order, all with run_id
  int64 entries_processed = 3;
  bool retriable = 4;           // set with a failed task
}
```

### One run of a task

1. **Download.** The task downloads each input to `in/<id>.sst` and opens it
   with `sst.ExpectSize(ref.size)`. A size that differs, or a bad footer or
   block, is `pkindex.ErrCorrupted`. A missing object is also
   `ErrCorrupted`.
2. **Merge.** `Merge` in `merge.go` runs a k-way merge on a min-heap ordered
   by key, then by input position. For one key, the entry of the newest input
   wins (last writer wins). An **operator** is a function that can change or
   drop the winning entry (`Operator` in `merge.go`). The winner goes through
   the operators in order. This work has no operators yet. If the value is a
   tombstone and the task drops tombstones, the merge does not write the
   entry. The merge holds one iterator per
   input and one copied key in memory.
3. **Split.** `splitOutput` in `output.go` writes the entries to `out/`. It
   takes the IDs from the reserved range in order. It closes a table when the
   estimated on-disk size of the writer reaches `target_file_size`. The
   estimate counts the pending data block before compression and does not
   count the filter block. When the range has no more IDs, the task fails.
4. **Upload.** Each table goes to `pkindex.SSTKey(sst_base_path, id)` when it
   closes. The upload queue holds one table. Thus up to two closed tables, one
   in the queue and one in upload, and the open table can be on the local disk
   at the same time. The first upload failure stops the merge at the next
   table close.
5. **Finish.** The task reports `Finished` only after the last upload. The
   outputs are in ascending key order.

### Executor rules

- **Disk admission.** `Create` accepts a task only when the free disk under
  the local root is at least `2 × inputBytes + diskMargin`. Otherwise it
  returns a retriable `ServiceUnavailable`, and the scheduler tries later. A
  failure to read the free disk is `pkindex.ErrIO` (`IoFailed`). The check runs
  before the executor drops an earlier attempt of the same task ID.
- **Parallelism.** At most `dataNode.pkindex.compaction.parallelism` tasks
  run at once. A value below 1 becomes 1. A waiting task is `InProgress` and
  holds a slot.
- **Slots.** `Executor.Slots` returns the `InProgress` tasks times
  `dataCoord.slot.pkindexCompactionUsage`. A finished or failed task holds no
  slot. `QuerySlot` subtracts this from the free slots of the node. The data
  node and DataCoord both raise a slot value below 1 to 1.
- **Repeated task ID.** A request whose task ID is already on the node
  replaces the earlier attempt. The executor cancels it and deletes its
  directory.
- **Cancel.** A task that fails because a cancel stopped its context reports a
  retriable failure. A cancel is never the fault of the request.
- **Unknown task.** The node reports `taskcommon.None` for a task that it does
  not know. The coordinator then dispatches the task again.
- **Terminal TTL.** Each query refreshes the last query time of the task. The
  scheduler normally calls `DropTask` right after it reads a terminal state,
  so the TTL is only a fallback, for example after a coordinator restart. A
  finished or failed task that no one queries for `terminalTaskTTL` loses its
  task entry and its local directory. The reaper runs every TTL/4, so the
  deletion occurs between TTL and 1.25 × TTL. A running task does not expire.

### Local directory

```
<localStorage.path>/pkindex_compaction/
  <taskID>-<outputIdBegin>/
    in/    inputs, deleted when the run ends
    out/   outputs, each deleted after its upload
```

- The output ID begin in the name keeps two attempts of one task apart.
- The executor deletes the whole root at start. The coordinator dispatches
  the lost tasks again.
- The name has no node ID, because the node ID changes at each start. Thus
  two data nodes must not share one `localStorage.path`.
- `Drop` cancels the task and deletes its directory. Uploaded outputs stay in
  object storage for the GC.

## Failure Handling

| Failure | Detection | Result |
|---|---|---|
| Data node lost | Query returns `merr.ErrNodeNotFound` | Drop on the node (best effort), back to dispatch with a new ID range |
| Task unknown to the node (node restart, or TTL expired) | Query state `None` | Back to dispatch with a new ID range |
| Remote store failure during download or upload | `ErrUnavailable`, `retriable` is true | Back to dispatch with a new ID range |
| Local disk failure | `ErrIO`, `retriable` is true | Back to dispatch with a new ID range |
| Not enough free disk | `Create` returns `ServiceUnavailable` | The task stays in Init. The scheduler tries again later |
| Corrupt or missing input | `ErrCorrupted`, `retriable` is false | Task fails, vchannel cooldown. The manifest refers to a bad object, so an operator must check it |
| Output IDs exhausted | Error with no category | Task fails, vchannel cooldown |
| Commit unavailable or timed out | `ErrUnavailable`, deadline | Commit again after a backoff of 1 s to 60 s. Outputs stay protected |
| Stale edit | `ErrStaleEdit` | Task fails, vchannel cooldown, manager cache entry deleted. Outputs become orphans |
| Duplicate output ID | `ErrDuplicateSST` | Task fails, vchannel cooldown. The manager cache entry stays. Outputs become orphans |
| Query transport error, or terminal state without payload | Query returns an error that is not `ErrNodeNotFound` | Query again at the next round, with no limit |
| Node lost during the commit backoff | Query returns `merr.ErrNodeNotFound` | New attempt with a new ID range. The outputs of the first attempt become orphans |
| Coordinator restart | Task table is empty | Tasks in flight are forgotten and never committed. Outputs become orphans. The node deletes the tasks after `terminalTaskTTL` |
| Zombie node (the coordinator moved the task, the old node still runs) | The coordinator queries only the current node. `stale` rejects a result outside the current range | The old attempt is never committed. The GC collects its outputs |
| Data node without a storage address | `initPKIndexCompaction` builds no executor | The node does not run compaction. Its task RPCs answer not-ready |
| Flush during the task | Not a failure | The commit succeeds, because a flush and a compaction commute |

A failed task never deletes its outputs. The GC is the only delete path.

## Comparison with Other Systems

The facts below come from the sources named in each paragraph, read on
2026-10-15 for the design proposal. The values marked "not verified" were not
checked again for this document.

**Pebble and RocksDB leveled.** Both start an L0 compaction at 4 L0 files or
sublevels (not verified against the current source). The sources are
RocksDB `level0_file_num_compaction_trigger` in the RocksDB wiki "Leveled
Compaction", and `L0CompactionThreshold` in the Pebble v1.1.5 source code.
Below L0, leveled compaction partitions each level by key and picks only the
overlapping files. This design takes the trigger value 4. It does not take
the leveled shape. Random user primary keys make each flushed table cover the
whole key space, so leveled compaction would rewrite the whole next level each
time.

**SlateDB.** The manifest holds an L0 list and a list of sorted runs (RFC 0002,
`compactor.rs`, `size_tiered_compaction.rs`, `db_state.rs`, `config.rs`). The
size-tiered policy merges consecutive sources only. It drops tombstones only
when the destination is the last run. The writer polls the manifest every
`manifest_poll_interval`, 1 s by default (not verified against the current
source). Its size ratio `include_size_threshold` is 4.0 by default (not
verified against the current source). This design takes the run as part of the
manifest, the consecutive-input rule, the size ratio default of 4 and the
tombstone rule. It differs in two points. The list stays flat with a run id
per table. A data node merges, but only the coordinator writes the manifest.

**HBase exploring policy.** `ExploringCompactionPolicy.java` lists all
contiguous windows of store files and keeps the windows in ratio. It then
picks the window with the most files, then the fewest bytes. This design takes
this tie-break for the merge rule. Its ratio test is different: the largest run
must be at most `sizeRatio` times the smallest.

**RocksDB universal.** All data is in sorted runs in time order (RocksDB wiki
"Universal Compaction"). A space amplification rule
(`max_size_amplification_percent`) merges all runs when the newer runs are
large compared with the oldest run. The outputs take the place of the inputs.
This design takes the space amplification rule and the in-place output. It
gives the ratio as a factor (`spaceAmpRatio`), not as a percentage.

## Known Limits and Follow-ups

- **Streaming node refresh.** No code makes the streaming node poll for a
  compacted manifest and install it. This is follow-up work.
- **Reroute and purge operators.** The merge has an operator interface, but no
  operator exists.
- **Manual trigger.** No RPC starts a compaction.
- **Tombstone-ratio trigger.** `SSTRef` has no tombstone count, so no rule
  uses one.
- **Reads on demand from object storage.** The data node and the streaming
  node download whole tables. The GC has no input for tables that a streaming
  node reads on demand.
- **Retry cap.** When the task of a vchannel fails again and again, the
  inspector plans the vchannel again after each cooldown. A task that goes back to dispatch has no attempt limit.
- **Disk admission of waiting tasks.** The disk check counts only the free
  disk at `Create`. It does not count the tasks that are admitted but not
  started.
- **Inspector seed at start.** The inspector starts empty. It learns of a
  vchannel from the next commit or the next GC round. The first GC round
  starts after `dataCoord.pkindex.gc.interval` (1 hour by default).
- **Collection cleanup.** Nothing on this branch deletes a manifest pointer.
  A dropped collection keeps its manifest, and the inspector keeps planning
  for it. The cleanup of the primary key index after a collection drop is
  follow-up work.
- **Larger ID range after exhaustion.** A task that runs out of output IDs
  fails. The next plan uses the same range formula.

## Parameters

| Parameter | Default | Meaning |
|---|---|---|
| `dataCoord.pkindex.compaction.l0Trigger` | 4 | Flushed tables that trigger their merge |
| `dataCoord.pkindex.compaction.minMergeRuns` | 4 | Minimum consecutive runs in a merge |
| `dataCoord.pkindex.compaction.sizeRatio` | 4 | Largest run divided by smallest run, at most |
| `dataCoord.pkindex.compaction.spaceAmpRatio` | 1 | Bytes outside the oldest run, divided by the oldest run, that trigger a full merge |
| `dataCoord.pkindex.compaction.maxInputBytes` | 8 GiB | Input cap of the flushed-table and merge rules |
| `dataCoord.pkindex.compaction.maxInputTables` | 64 | Flushed tables in one compaction, at most |
| `dataCoord.pkindex.compaction.maxConcurrent` | 16 | Tasks in flight across all vchannels |
| `dataCoord.pkindex.compaction.inspectInterval` | 10 s | Time between two inspections. Read once at start |
| `dataCoord.pkindex.compaction.failureCooldown` | 600 s | Time a failed vchannel waits before the next plan |
| `dataCoord.pkindex.compaction.targetFileSize` | 128 MiB | Size at which an output table closes |
| `dataCoord.slot.pkindexCompactionUsage` | 4 | Slots of one task |
| `dataNode.pkindex.compaction.terminalTaskTTL` | 600 s | Time a finished or failed task waits for a query before the node deletes it. A value of 0 or below gives 1 minute |
| `dataNode.pkindex.compaction.parallelism` | 4 | Tasks that run at once on one node |
| `dataNode.pkindex.compaction.diskMargin` | 1 GiB | Free disk a task needs on top of twice its input bytes |
