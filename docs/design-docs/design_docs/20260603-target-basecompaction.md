# MEP: Manual REWRITE compaction targets in Milvus 3.0

- **Created:** 2026-11-13
- **Author(s):** @XuanYang-cn
- **Status:** Draft
- **Component:** Coordinator, Proxy, SDK
- **Related Issues:** #49991
- **Released:** N/A

## Summary

A manual REWRITE request creates a durable collection-scoped target. DataCoord
reconciles it against current flushed data and publishes work through the
existing Sort and Mix task paths. Clients can read the target until it reaches
a durable terminal state or stop it by ID.

## Motivation

A manual compaction task describes one physical operation. A collection can
have matching segments that are currently busy, protected by a snapshot, or
waiting for an index. A durable target gives the caller a stable handle before
any of those segments are available to run. The target's outcome survives
coordinator restart and reflects the published data.

## Public Interfaces

The four gRPC methods are `CreateCompactionTarget`, `DescribeCompactionTarget`,
`ListCompactionTargets`, and `DropCompactionTarget`. The Go SDK and the
`/v2/vectordb/compaction_targets` REST routes expose the same operations. Create
takes a collection and optional segment IDs as lineage roots. It returns the
target ID and the initial number of matching rows. Describe returns the
durable state and may include a fresh remaining-row observation. List filters
by current database, collection, and state. Drop preserves the first
persisted terminal result.

The startup setting `dataCoord.compaction.enableTargetBasedCompaction` defaults
to false and also requires `dataCoord.enableCompaction`. The
`milvus_datacoord_compaction_target_duration_seconds` histogram measures the
time from activation to durable satisfaction by target family.

## Design Details

Proxy authorizes `PrivilegeCompaction` on the collection resolved by RootCoord.
DataCoord rechecks the current stable ID, names, and metadata revision before
persisting the target. This matters on 3.0 because DataCoord's collection
cache can lag renames or moves across databases. Target history for deleted
collections uses cluster administration privileges.

Create fixes a logical timestamp T and records the distinct roots and initial
row count. A segment is in scope when it is healthy, Flushed, outside L0 and
L2, and visible after import commit if applicable. Its effective data start
must be strictly before T. The observation uses the existing commit
timestamp, segment statistics, and start position in that order. An unknown
start timestamp is included conservatively. On 3.0 the import flusher submits
each channel's commit timestamp to DataCoord before its imported segments
become visible.

An in-scope segment needs a rewrite when its rewrite timestamp is zero or
strictly before T. A full Mix, Sort, Clustering, or Schema replacement gets
the producing task's admission timestamp. Schema updates that retain the
segment ID preserve its previous timestamp. Explicit roots follow retained
compaction lineage as the original segments are replaced. Each observation
deduplicates current matching segments.

Reconciliation first checks semantic progress and then applies execution
gates, including snapshot protection, occupancy, and index readiness. Shared
matches contribute one physical input per round. Before persisting
satisfaction, DataCoord repeats the full observation and checks relevant
in-flight inputs while serializing new task admission for the collection.

The target catalog uses an insert-if-absent transaction for Create and a
conditional update for terminal transitions. If a Create write may have
committed, DataCoord reads the same candidate ID to confirm it. An
unconfirmed result is reported as an unknown Create outcome. The Go SDK and
Proxy limit each Create call to one application dispatch.

## Compatibility, Deprecation, and Migration Plan

Older task and segment records read with a zero rewrite timestamp. The
release adds no required migration for existing records. With target-based
compaction disabled, target metadata is left in place and the legacy manual
path remains active. With it enabled, ordinary manual and target-size
requests direct callers to the Target API. L0, Clustering, and old task-state
queries continue to use the existing paths.

This slice implements manual REWRITE on standalone clusters without a CDC
topology. OPTIMIZE and Standing sources require their own implementation
before they can be enabled.

## Test Plan

- Check conditional creation and first-terminal persistence with etcd and
  TiKV, including an uncertain commit result.
- Exercise stable collection identity across rename, cross-database move,
  and deletion. Check that a fresh object at the old name cannot acquire the
  old target.
- Verify matching against committed import timestamps, lineage, and Flush
  publication between the first and final observations.
- Exercise all four replacement constructors and confirm that same-ID schema
  adoption preserves the previous rewrite timestamp.
- Check one-dispatch Create behavior and the distinction between absent and
  zero remaining rows in gRPC and REST.
- Exercise Go SDK and REST against a 3.0 deployment. Confirm that a published
  replacement retains the requested lineage, and that releasing snapshot
  protection lets a previously blocked REWRITE reach satisfaction.

## Rejected Alternatives

A task-state handle would lose a request that never enqueues work. Reusing the
legacy `compactionID` field would also conflate the target and task lifecycles.
The dedicated methods identify the durable target directly.

## References

- [Target-based compaction #49991](https://github.com/milvus-io/milvus/issues/49991)
- [Master API implementation #55502](https://github.com/milvus-io/milvus/pull/55502)
