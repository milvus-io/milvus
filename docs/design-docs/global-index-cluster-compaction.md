# Snapshot-owned global index contracts

Clustering compaction partitions vector rows with KMeans and writes contiguous
chunks that queries can prune. Centroid vectors and chunk locations belong to
one complete `PartitionStatsSnapshot`. A centroid's identity is its offset in
that snapshot, qualified by the snapshot version; no global centroid or group
ID allocator is required.

This first PR defines messages, configuration, JSON persistence and structural
validation. It does not activate sampling, planning, mapping, publication or
query pruning. Those integrations must satisfy the contracts below before the
feature gate can take effect.

## Snapshot ownership and format

The scope is `(collection, partition, vchannel)`. Preserve the existing JSON
snapshot and scalar `FieldStats`, with two optional additions:

| Owner | Addition | Meaning |
| --- | --- | --- |
| `PartitionStatsSnapshot` | `clusteringStats` | Format version, vector field ID/type/dimension and the complete centroid table |
| `SegmentStats` | `centroidRanges` | References into that table with physical `row_offset` and `row_count` |

`PartitionClusteringStats` and `ClusteringCentroidRange` are protobuf messages in
`pkg/proto/clustering.proto`. The enclosing JSON keeps `segmentStats`, `Version`,
`fieldStats` and `NumRows` unchanged. Only the new blocks use protojson, including
its oneof handling and string representation of int64 values. Legacy snapshots
without the new blocks remain readable; a missing legacy `Version` remains zero.

Format 1 requires a positive snapshot version and field ID, a positive vector
dimension and correctly typed, finite centroids. PR1 accepts FloatVector;
FP16/BF16 admission and typed execution are deferred to PR5. For every covered
nonempty segment:

- Offset zero is valid. Every offset indexes this snapshot's centroid table.
- Ranges are in physical row order, are positive and exactly cover `[0, NumRows)`
  without gaps or overlaps. Count checks must not overflow.
- Each centroid appears in at most one range per segment, but may span segments.
- Every table entry has physical rows. Centroids with no surviving output rows
  are removed. An empty complete snapshot has an empty table and segment map.

Numeric centroid offsets need not be sorted after a snapshot merge. A retained
segment may refer to a different offset in the next snapshot without changing
its files. Snapshot immutability is an ownership contract: Go map assignment and
slice/pointer copies are not deep copies. A merger must copy metadata before
changing offsets; it must never mutate a published snapshot.

## One replacement protocol

All L2 physical compactions that replace covered segments must use clustering
compaction and publish corresponding stats. Full construction and partial
replacement use the same operation. Given base `S`, compaction inputs `I` and
new outputs `O`:

```
R = I intersect keys(S.segmentStats)
Snew.segmentStats = (S.segmentStats minus R) union stats(O)
Snew.Version = planID
```

For example, replacing `{A, B} + E` in `{A, B, C, D}` with `{F, G}` yields
`{C, D, F, G}`. `E` may be fresh data outside the old coverage. The worker merges
retained centroids with this task's surviving centroids and remaps both sets of
ranges into the new table. Only the complete snapshot is persisted; the delta
is an in-memory merge input, not another file or directory.

The complete-centroid constraint requires selecting every segment sharing a
centroid with an input, transitively. An unavailable member defers the whole
selection; it cannot be removed from the group and leave the others scheduled.
This coverage closure does not preserve arbitrary historical planner groups.
If historical groups must also remain indivisible when they share no centroid,
explicit membership metadata would be needed before implementing PR3.

Mapping performs the final sort and produces ranges after deletion/TTL filtering.
There is no second sort-compaction task. The final snapshot is written at the
existing `part_stats/<collection>/<partition>/<vchannel>/<planID>` location using
the backend's existing complete-key rules. A published object is immutable.

## Task and recovery messages

| Message | New fields |
| --- | --- |
| `AnalyzeTask` | `train_rows`, `num_clusters`, `cluster_type`, `cluster_params` |
| `AnalyzeRequest` | `train_rows`, `cluster_type`, `cluster_params` |
| `AnalyzeInfo` | `cluster_type`, `cluster_params` |
| `CompactionTask` | `clustering_stats_format_version`, `base_partition_stats_version`, `clustering_plan_params`, `clustering_pending_result` |
| `CompactionPlan` | `clustering_stats_format_version`, `base_partition_stats_version` |
| `CompactionPlanResult` | `partition_stats_version` |
| `ChannelTarget` | `partition_stats_versions` |

Reuse existing task identities, analyze versions, input/output segment IDs and
assignment-file paths. Assignment `centroid_id_mapping` contains dense offsets
into the same Analyze generation, not final snapshot offsets. Its optional
`distance_to_centroid` has one value per input row with the Analyze metric's
semantics. Assignment counts precede deletion/TTL filtering. Filtering a row
still consumes its assignment, in the same input order.

For compaction, format zero is legacy and format one opts into these contracts.
Base version zero means initial construction; a missing nonzero base must fail,
not silently become an initial build. A successful new-format result records
`partition_stats_version == planID`, including an empty output. The complete
snapshot must exist before that result is returned.

Persist `clustering_pending_result` before adopting output metadata. Its presence
records success even when output segments are empty. Recovery replays that same
receipt idempotently instead of rerunning mapping or mixing output generations.
Validate worst-case result size against catalog value/transaction limits before
wiring this inline receipt in PR4; if it cannot fit, revise the receipt format
before activation. No runtime capacity claim is made in PR1.

`CompactionTask.resultSegments` describes only this task's outputs.
`PartitionStatsInfo.segmentIDs` describes the whole new snapshot, including
retained segments. Publication must validate the pinned base, current input
ownership and schema; serialize publication within the scope. Concurrent delta
rebasing is not introduced. Empty output is a valid removal of covered data.
Legacy layouts without complete ranges require an initial complete rebuild.

Tags and names used by the earlier experimental sidecar/ID/sort-task protocol
are reserved, not repurposed. That reservation prevents accidental wire aliasing;
it does not provide rolling compatibility with the experimental implementation.

## Query binding and consistency

For new layouts, `SyncAction.partition_stats_versions` on `UpdateVersion` is the
complete partition-to-stats binding activated with the segment view. Persist the
same binding in `ChannelTarget`. An absent binding disables this pruning for
that partition. Independent `UpdatePartitionStats` may prefetch objects but must
not replace a pinned view's active stats. These are follow-up integration rules;
PR1 does not change the legacy handlers.

DataCoord must expose a committed segment set with its matching stats versions.
A query pins both together. Retained segments can occur in several snapshots,
but their ranges must always be interpreted with the owning snapshot's table.
Validate the object's version against the requested version before adopting it;
do not overwrite its version and thereby hide a mismatch. Missing, unsupported
or corrupt matching stats disable this pruning, never select another version.
Only segments in the query's actual eligible view can consume candidate budget.

Several query targets may reference the same snapshot, forming an implicit
validity range. Target versions and plan IDs are different version domains;
never compare them numerically. Do not add mutable end-version fields to old
snapshots. In-flight queries retain their old binding. GC must protect versions
referenced by targets, queries and compaction tasks; retaining an arbitrary fixed
number of snapshots is insufficient.

## Parameters and activation

| Parameter | Contract/default |
| --- | --- |
| `dataCoord.compaction.clustering.globalIndex.enable` | `false`; reserved activation gate, no production consumer in PR1 |
| `trainSampleRatio` under the same clustering prefix | `0` means unset; explicitly configure `(0, 1]` before future activation |
| `chunkSizePerCentroid` under that prefix | Positive raw vector bytes, default `8MB`; a target, not a hard chunk limit |
| Existing `minCentroidsNum` / `maxCentroidsNum` | Keep defaults `16` / `10240`; sampling rows also bound K |
| Existing `maxTrainSizeRatio` | Worker memory budget; never reinterpret as input sampling fraction |
| `knowhere.cluster.type` | `KMEANS` |
| `knowhere.cluster.analyze.*` | Provider parameters frozen into AnalyzeTask |
| `knowhere.cluster.compactionPlan.*` | Planner parameters frozen into CompactionTask; YAML default `planner: ivf` |

For input physical vector row count `N`, sampling fraction `r`, vector width `D`
and bytes per dimension `B`, resolve once at scheduling time:

```
T = min(N, ceil(N * r))
Ktarget = ceil(N * D * B / chunkSizePerCentroid)
K = min(T, maxCentroidsNum, max(minCentroidsNum, Ktarget))
```

For nonempty input require `1 <= K <= T <= N`; skip native KMeans for empty input.
Use checked arithmetic. A worker converts `train_rows` into existing
`AnalyzeInfo.train_size` bytes without recomputing the fraction from local memory.
Memory admission may defer/reject the job, never silently reduce its sample.
Provider maps cannot override coordinator-owned shape, sample count, input
identity or format. Parameter names preserve Knowhere's case and underscores.

Before enabling writers, require worker/query capability support, complete
snapshot merging, recoverable publication and query binding. Old readers ignore
unknown JSON/protobuf fields, so protobuf compatibility alone is insufficient.
PR1 provides structural validation, not these end-to-end activation checks.

## Delivery sequence and verification

1. PR1: this contract, additive proto/config and snapshot codecs/validation.
2. PR2: existing Analyze lifecycle, fixed input sampling and Knowhere KMeans.
3. PR3: centroid-based planning replacing random grouping, closure checks and
   frozen planner parameters.
4. PR4: mapping plus sort, snapshot merge, recovery/publication, all L2 replacement
   routing and matching query-view binding.
5. PR5: FP16/BF16 throughout sampling, clustering, storage and query consumers.

PR1 tests cover legacy and new JSON, precise int64 round trips, independent
snapshot remapping, empty results, malformed vectors/ranges, unsupported formats
and preservation of nested error codes. Configuration tests cover namespace
isolation, exact provider key spelling and copies safe to freeze in tasks.
Later PRs need crash/retry, stale-base, empty replacement, concurrent query/target
switch and GC tests; those behaviors are not established by the PR1 unit tests.
