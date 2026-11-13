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

Storage Version and Storage Format keep separate cluster-wide Targets.
Version compares the published segment storage version. Format reads the
insert-group formats of the published V3 manifest revision through the
3.0 storage library. Reads share a connection-bounded budget and reuse
successful observations of the same revision. A missing or unreadable
manifest leaves the format observation incomplete. Version work can still
proceed, subject to writer and QueryNode capability. TEXT collections retain
V3 storage when the desired version changes to V2.

Standing SORT uses one cluster-wide identity for periodic and post-flush work.
The shared admission bucket charges new physical tasks and preserves recovery
of existing tasks. Schema Version compares each V3 segment against the
confirmed pending ALTER when one exists, otherwise the published schema.
It captures that schema in the planned task. Version confirmation and
in-place materialization preserve segment identity and rewrite time.

Reconciliation first checks semantic progress and then applies execution
gates, including snapshot protection, occupancy, and index readiness. Shared
matches contribute one physical input per round. Before persisting
satisfaction, DataCoord repeats the full observation and checks relevant
in-flight inputs while serializing new task admission for the collection.
The final check also acquires the collection's existing DDL resource guards.
During DropCollection the control-channel acknowledgment saves the collection
tombstone and then closes its active Targets with `scope_dropped`. The DDL
guard covers this interval, including any early data channel cleanup. On
startup, a recovered Target whose scope has been deleted gets the same outcome.

The target catalog guards the current collection revision in the same
insert-if-absent transaction for Create and uses a
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

`dataCoord.compaction.target.retentionSeconds` controls inactive Target
retention independently of task history. The default is seven days. While a
record is retained, Describe and Drop preserve its durable lifecycle. After
collection of the record, both report TargetNotFound.

Manual REWRITE is available on clusters without a CDC topology. The automatic
sources described above keep their identities across restarts and configuration
changes.

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
- Delete a collection while its REWRITE is active. Verify `scope_dropped`
  after the DDL succeeds, and exercise recovery after deletion in legacy mode.
- Exercise Storage Version and Storage Format against published Mix results.
  Confirm the storage version and data-group format from the written data.
- Exercise Schema Version with real V3 workers and confirm which updates
  preserve the input and which publish a replacement.

## Rejected Alternatives

A task-state handle would lose a request that never enqueues work. Reusing the
legacy `compactionID` field would also conflate the target and task lifecycles.
The dedicated methods identify the durable target directly.

## References

- [Target-based compaction #49991](https://github.com/milvus-io/milvus/issues/49991)
- [Master API implementation #55502](https://github.com/milvus-io/milvus/pull/55502)
