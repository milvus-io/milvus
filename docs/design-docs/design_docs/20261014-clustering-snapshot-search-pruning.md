# MEP: Clustering snapshots for vector search pruning

- **Created:** 2026-10-14
- **Author(s):** @foxspy
- **Status:** Draft
- **Component:** Coordinator, DataNode, QueryNode, Storage, Index
- **Related Issues:** #52175
- **Released:** Not released

## Summary

Extend centroid pruning with an immutable clustering layout and index owned by
`PartitionStatsSnapshot`. Analyze samples vectors within its training-memory
budget; compaction groups clusters and sorts their rows while mapping into final
segments. Queries use the snapshot index to select clusters, prune segments with
valid empty candidates for every query, and translate retained candidates into
optional physical-row hints for Knowhere.

The implementation covers dense FP32, FP16 and BF16 vectors. The initial hint path
supports L2 top-k search. Explicit metric propagation, COSINE semantics and the
new scalar mapping/index path remain follow-up work.

## Motivation

Clustering compaction already groups similar data. Keeping cluster locations
allows query execution to use that layout instead of searching every row without
cluster information. The layout must remain consistent across partial compaction,
retained segments, query pins and index loading.

A complete snapshot provides one publication and lookup boundary. Retained
segments can occur in multiple snapshots; their historical segment metadata
cannot determine which snapshot a current query should use.

## Public Interfaces

There is no new SDK search API or independent global-index activation setting.
Existing vector-clustering and segment-pruning settings govern the pipeline.

- Internal protobufs add clustering-field metadata, index artifacts, groups and
  physical ranges to partition stats, plus task/binding/search-hint metadata.
- `PartitionStatsSnapshot` retains its JSON envelope with optional
  `clusteringStats`; `SegmentStats` gains `clusteringRanges`.
- `knowhere.cluster.analyze`, `knowhere.cluster.compactionPlan` and
  `knowhere.cluster.index` carry phase-specific parameters. The training sample
  count derives from the existing memory budget, not a new sample-ratio setting.
- Query hints are optional metadata. The segment's scalar/MVCC mask and original
  NQ remain the query's filtering and batching boundaries.

No separate `milvus-proto` change is needed; the new messages are generated from
Milvus's internal proto definitions.

## Design Details

### Ownership and row layout

A snapshot is scoped by `(collection, partition, vchannel, version)`. It owns one
clustering field, the complete ordered cluster-description file, the built index,
the segment stats and the group directory. Index labels are offsets into this
snapshot's cluster table. Snapshot version, format version and index-engine
version describe different things and are not interchangeable.

`ClusteringGroup` stores only `segment_ids`. Every covered segment belongs to
exactly one group. Derive a cluster's group from its ranges and the segment-to-group
directory; all ranges for that cluster must have the same group. Do not persist a
second cluster-membership table or allocate global cluster/group IDs.

Each `ClusteringRange` maps a cluster offset to
`[row_offset, row_offset + row_count)` in a final segment. Ranges must be positive,
in bounds and cover that segment's physical rows without gaps or overlaps. A
cluster can span segments in its group, with at most one range per segment.

### Analyze, planning and mapping

Analyze freezes cluster count and provider parameters, samples across the input
rows within its training-memory budget, invokes Knowhere KMeans, then assigns all
rows. Its outputs contain aligned cluster offsets, per-cluster counts and finite
assignment distances. Generation-scoped artifacts isolate task attempts.

The compaction planner consumes centroids and counts. Selection closes over every
segment in a touched base-snapshot group; fresh uncovered segments may also join.
Unchanged groups remain in the next complete snapshot.

Mapping sorts by group, cluster, ascending assignment distance and source position
for deterministic ties. Bounded task-local spill runs are merged into final
segment writers. A segment cannot contain rows from different groups; a group may
rotate across several segments. Intermediate sort runs are not published as
segments and do not require a subsequent sort-compaction task.

FP16/BF16 input rows keep their original typed payload bytes through spilling and
merging. Empty clusters and groups disappear after filtering. The field-neutral
range format leaves room for scalar implementations, without enabling one here.

### Complete snapshot publication

Partial and full replacement share the same construction path. Copy retained
metadata, remove the selected complete groups, add final outputs and remap cluster
offsets into the new complete table. Never mutate a published snapshot's Go maps,
slices or nested protobuf values in place.

Build the snapshot's index after the final cluster order is known, using the
configured index parameters. A nonempty snapshot requires cluster descriptions,
index metadata and complete index files before publication. An empty snapshot has
none of those artifacts.

Publication checks the pinned base version and current input coverage. The
snapshot receipt and current-version update are committed through the catalog;
recovery uses that receipt to finish visibility updates. A conflicting base is a
publication conflict, not permission to overwrite a newer snapshot. This design
does not claim arbitrary concurrent delta merging.

The minimal worker protocol records a worker before dispatch and polls that
worker on recovery. It does not resubmit the same plan generation to another
worker. A replacement task uses a new plan identity. Unknown snapshot-reader
references prevent reclamation; cross-node leases and automatic cleanup are
separate work.

### Index loading and query binding

The snapshot index uses complete recorded file keys rather than a synthetic
segment ID. Snapshot context selects ownership/cache identity; shared index-loader
machinery handles parameters, resource estimation, scheduling and reader loading.
Load-time overrides apply to a copy of persisted index parameters.

Pin the query's segment view and its snapshot binding together. At load time,
derive `cluster offset -> (segment, row offset, row count)` from the complete
snapshot, then use only entries in the query's eligible segment view. Do not select
a snapshot using a retained segment's older `PartitionStatsVersion`.

### Optional hints and NQ batching

Probe the snapshot index with the query batch. Validate field/type/dimension,
metric, labels, distances, NQ and segment row domains before building owned hints.
An absent per-query hint means ordinary search; a present empty hint preserves a
valid no-matching-range entry. Both forms keep their original position in NQ.

Before dispatch, the delegator removes a sealed segment only when every query in
the original NQ has a valid, explicitly present empty hint list for that segment.
Missing hints, incomplete coverage, invalid metadata or any query with candidates
retain the segment. Uncovered segments and growing data remain in the search
scope. Pruning rebuilds the request's worker groups and row-count accounting
without changing the pinned view. Two-stage search, including its fallback path,
retains the original scope.

Forward hints through the existing Knowhere dataset API. A backend may use or
ignore them. A graph implementation can use hinted ranges as entry-point guidance
and traverse outside them; a brute-force implementation can scan selected ranges.
Hints are not a replacement for the scalar-expression/MVCC mask and are not a
guarantee of strict row filtering. Every retained segment receives the full NQ;
Milvus does not split the batch or remove individual queries with empty hints.

The current path requires dense-vector L2 top-k search. Unsupported modes, invalid
metadata, failed probes and excessive hint metadata fall back to ordinary search
for the affected scope. No additional capability-query API is required.

Compatible requests can merge their hints in the same order as their query
vectors and result partitions. Compatibility includes field, encoding,
visibility/filter scope, snapshot version and row domain. The normal segment
execution path continues sharing expression work across NQ.

## Compatibility, Deprecation, and Migration Plan

- Existing snapshots without clustering metadata remain readable and retain
  existing behavior. Unsupported or mismatched optional query metadata disables
  this optimization.
- Earlier unmerged experimental snapshot layouts are not compatibility targets.
- Scalar mapping and metric/COSINE generalization are not implicitly enabled by
  the new contracts. Existing scalar functionality remains separate.
- No rolling-upgrade, downgrade or automatic old-snapshot-reclamation guarantee is
  established by the current tests. Validate the desired version combinations and
  retention behavior before release.
- The final dependency pin must be built and tested without local dependency
  patches before integration is considered merge-ready.

## Test Plan

- Validate JSON/proto round trips, empty/legacy snapshots, large offsets, exact
  row coverage and one-group ownership.
- Exercise sampling budgets, binlog/manifest inputs, assignment row order and
  count validation, and typed FP32/FP16/BF16 artifacts.
- Exercise complete-group closure, stale base versions, retained-range remapping,
  sort spill/merge, filtering, task-local failure cleanup and publication recovery.
- Verify pinned segment/snapshot bindings, malformed/probe-error fallback,
  missing/empty/ordinary hints, ignored hints, and compatible/incompatible merges.
- Verify worker admission for all three float types and element-width overflow
  limits. Check actual delegator dispatch, empty-hint segment pruning, passthrough,
  two-stage fallback and request-local partial-result accounting.
- Verify per-query result ordering and the unchanged shared expression/MVCC mask.
- Repeat the final dependency build, upstream CI and large-dataset E2E. Measure
  search work, latency and recall against unpruned search and exact neighbors for
  each candidate budget. Approximate cluster selection does not guarantee
  unchanged recall.

The latest focused combined-stack run passed 5 native hint tests and 93 Go
tests/subtests using an existing dependency cache. The affected worker, Analyze
CGo and delegator packages passed 531 tests/subtests with one existing skip.
An earlier truncated LAION10M
run used a 1 GiB segment target and verified all persisted rows/ranges plus index
build/load and query comparisons. Those runs do not establish a fresh final-pin
E2E pass; validation details and limits belong in the integration PR.

## Rejected Alternatives

- Global centroid/group ID allocation and a separate directory duplicate
  identities already scoped by a complete snapshot.
- Persisting group-to-cluster membership duplicates the range-derived ownership
  relation and introduces another consistency obligation.
- A separate sort-compaction stage adds staging-segment publication and another
  scheduling/recovery boundary; mapping can sort before final output.
- Selecting stats through each segment's historical version fails when retained
  segments participate in a newer complete snapshot.
- Requiring a backend capability interface makes optional hints depend on a
  second protocol. Passing hints while allowing the backend to ignore them keeps
  the normal query shape.
- Per-query Milvus row bitmaps or dropping empty-hint queries would change the
  shared-filter or optional-hint semantics.

## References

- [Tracking issue #52175](https://github.com/milvus-io/milvus/issues/52175)
- [Review organization reference #53567](https://github.com/milvus-io/milvus/issues/53567)
- [Milvus contribution guidelines](https://github.com/milvus-io/milvus/blob/master/CONTRIBUTING.md)
