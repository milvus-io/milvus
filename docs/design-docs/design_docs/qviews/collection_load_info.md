# Versioned CollectionLoadConfig

## Status and scope

This replaces the separate scope-only `coord/qv-load-info` history, hash-based LoadInfoVersion and
read-time current-index resolution. It also merges the QueryView load path's
partition membership into CollectionLoadInfo. The child PR scope is described in [the extraction boundary](querynode_extraction.md).

The design covers Coordinator metadata and its QN/SN consumers. It does not
change SchemaVersion, DataVersion or Manifest identity, introduce a metadata
watch stream, or implement general storage/schema history GC.

## One immutable record per version

Use `coord/load_info/<collectionID>/<version>` for both current
load configuration and retained historical requirements. One owner/catalog
provides `Latest(collectionID)` and `Get(collectionID, version)`; no duplicate
qv-load-info snapshot store is needed.

Each record contains:

- CollectionID, DBID and a persisted, monotonically increasing version;
- PartitionIDs, replacing separate `querycoord-partition-loadinfo` membership
  records for this path;
- explicit RequiredFields and LoadFields preload hints;
- exact field-to-index references: each LoadFieldConfig contains field_id and
  repeated index_ids, including all selected JSON path indexes for a field;
- replica IDs, resource groups and priorities; target node placement belongs
  to Balancer and actual lifecycle state belongs to ShardViewRegistry.

An empty index_ids list means no selected index; it never means "resolve whatever
index is current". Lists are sorted and deduplicated before publication. Both required fields and preload
hints use the same index selection. IndexID does not identify a Segment's build,
index files or Manifest. Those remain Segment-specific metadata.

PartitionLoadInfo currently contributes membership, with Loaded status in the
QueryView producer. Historical PartitionIDs must come from the selected record,
not from today's partition keys. This merge does not require removing legacy
partition APIs before their remaining consumers are adapted.

Allocate and persist a new version under the collection's serialized update
boundary before publishing it to planners. A committed version is immutable.
Recovery and callback retries must not reuse an identity for different contents;
version allocation must remain safe across complete Release and reload, even
though no old LoadInfo record is retained. Version zero remains a current-state
probe, never a resource-preparation identity. Exact historical lookup never
falls back to latest.

## Independent protocol and storage boundary

`viewpb.CollectionLoadConfig` is the complete immutable QueryView load record.
It owns its field/index and replica definitions in `view.proto`. WAL messages
retain their independent, original `messages.proto` definitions. The load owner
explicitly converts message intent; it resolves the complete exact index set
from committed metadata before publishing a version. The historical message's
single-index hint does not restrict the new record's multiple-index selection.

The catalog lives in `internal/metastore/kv/queryview/`. Each version is a single
key containing partitions, required fields, preload hints and replicas. Save
writes that key once, without separate partition or replica keys and without
legacy dual writes. Release removes only this collection's new-format prefix.
The legacy CollectionLoadInfo/PartitionLoadInfo catalogs and ReplicaManager
retain the master implementation and are not QueryView state owners.

Replica identities, resource groups and priorities come from this record.
Balancer owns target node placement; ShardViewRegistry owns actual View state.
ID allocation uses the Coordinator allocator directly. QueryView load admission,
configuration updates and management reads must not consult ReplicaManager.
ResourceManager continues to own resource groups and node membership.

TODO: legacy metadata migration and fault-recovery integration are deferred.
Reading retained new-format versions remains supported; this change adds no
legacy recovery converter, dual-write transaction, release tombstone or separate
pending-release scan. Recovery claims require a later dedicated review/test.

## Index DDL updates

CreateIndex and DropIndex update the corresponding active LoadInfo as part of
their durable DDL completion workflow. AlterIndex retains its existing admission
rule: index parameters cannot be hot-updated on a loaded collection. It does not
publish a live LoadInfo update; the next load obtains the updated metadata.

| DDL | New LoadInfo contents | Preparation effect |
| --- | --- | --- |
| CreateIndex | Add the exact IndexID for the affected field | New Views prepare the selected index when its artifacts are available |
| DropIndex | Remove the index reference | New Views stop selecting that index and prepare the remaining query capabilities |

Persist index metadata first, then publish the corresponding immutable LoadInfo
revision, and only then acknowledge the DDL workflow. A failure between these
steps is retried through the existing durable broadcast callback. Retry of the
same committed change is idempotent; it must not endlessly allocate versions or
publish a snapshot assembled from a later unrelated index change. Serialize
these updates with Load/Release and other changes for the same collection.
Do not depend on a best-effort in-memory notification for persistence.

For an unloaded collection there is no active LoadInfo to update. Index DDL
updates index metadata without implicitly loading the collection or creating a
retained load version. The next Load records the then-current index references. Existing API
admission rules remain in force: this design does not itself allow AlterIndex
on a loaded collection where the API currently rejects it.

The new LoadInfo version triggers QueryView reconciliation even when partitions,
SchemaVersion and DataVersion are unchanged. This is an asynchronous resource
update, not a new promise that the DDL API waits for every index to load.
CreateIndex committing a definition does not mean index files exist. StorageV3
index artifacts still arrive through Manifest publication and DataView updates;
Readiness checks actual capabilities. Existing supported fallback behavior may
serve while construction proceeds; the version alone cannot certify a loaded
index or force a ready View to refresh itself.

`dataCoord.index.writeSegmentIndexToManifest` is enabled by default. A successful
StorageV3 index build commits its artifact and the new Segment Manifest pointer
through `CommitSegmentManifest`. After persistence and in-memory publication,
the shared notification requests DataView reconciliation; the new ManifestVersion
advances `compact_version`, and the replacement QueryView prepares that revision.
This artifact update does not mint another LoadInfoVersion: the selected IndexIDs
are unchanged. The legacy etcd-only completion mode does not provide this physical
resource publication signal and is not the QueryView validation configuration.

## Index metadata resolution

CollectionLoadConfig persists only references. It contains no IndexInfo/IndexMeta
snapshot and no index-parameter history. GetQueryViewLoadInfo returns the exact
version's loading requirements together with metadata resolved for its recorded
IndexIDs in the same response. It must not append indexes created after that
version, or substitute another index on the same field. Missing exact metadata
is an error, not an implicit raw-data fallback or successful partial response.
Logically deleted definitions that still exist may be resolved by their exact IDs
for old Views; physical deletion/retention coordination remains a GC TODO.

QN/SN own the metadata they fetch for resource preparation. The immutable parsed
Schema may be shared by (CollectionID, SchemaVersion); a View's metadata binding
also distinguishes LoadInfoVersion so different index selections cannot mutate
one shared collection's metadata. GetQueryViewSegmentLoadInfo provides physical
artifacts, not permission to overwrite the selection obtained from LoadInfo.
CreateIndex does not imply files are already built. Manifest publication supplies
physical index artifacts separately. Old queries retain their owned resources
through completion even when a replacement View drops an index.

## Retention and Release

While the collection has active load intent, retain the greatest committed
version and every version referenced by a live QueryView. "Live" includes
Preparing, Ready, Up, Down, Unrecoverable and Dropping, plus Dropped Views whose
durable removal has not completed. Aggregate references across shards and
replicas. Follow the existing DataView reference pattern: acquire before View
publication, roll back failed creation, and release only after final durable
View removal. Lookup/acquisition and GC must serialize to cover in-progress
creation; ordinary queries do not need a new Coordinator ref RPC.

On recovery, rebuild all persisted View references before enabling GC. Merely
loading current configuration is insufficient to decide which historical
versions are unreferenced.

Release stops new View planning for the collection, drains reconciliation that
captured the old intent, and waits for all QueryViews to complete node cleanup
and durable removal. It then deletes **all** CollectionLoadConfig versions and
completes the Release workflow. There is no retained maximum, inactive version
or LoadInfo tombstone after Release. DropCollection likewise does not retain a
load version after its cleanup finishes.

The existing durable DropLoadConfig broadcast owns Release completion and
retries after failure. Startup recovers persisted requirements and View references,
then starts worker synchronization and callback replay. A retried Release withdraws
the recovered load intent and drains any reconciliation that already observed it.
It does not require a separate pending-release scan or an additional persisted
intent flag. Callback completion waits for final deletion; Broadcaster retains the
collection lock so a later Load cannot overtake cleanup. Startup must not wait for
Release callback completion before starting the worker synchronization it needs.
Cleanup remains idempotent, including a crash after deletion but before callback
completion.

For example, v10 belongs to A and v11 to B/current. A entering Dropping does not
permit deleting v10; its final durable removal does. Release waits for B and
any other Views, then removes v11 and every remaining version. A later Load
creates a fresh version from current partition/field/index metadata.

## Implementation and verification boundary

Coordinate changes to LoadConfigStore/catalog, View references and recovery,
CreateIndex/DropIndex DDL callbacks, exact LoadInfo lookup, QN/SN collection metadata,
and physical preparation/reuse. Replacing a storage key alone is insufficient.
Stopping planning is separate from final history deletion and does not add a
tombstone version.

Verify: multi-version partition and index-selection isolation; multiple indexes
on one JSON field; response mutation isolation; CreateIndex before artifacts exist and subsequent Manifest publication;
DropIndex with old Views/query handles; callback failure/replay between metadata
and LoadInfo persistence; View creation versus GC; restart before reference
reconstruction; Release/reload and crashes before/after final deletion. No
production implementation or test result is claimed by this design revision.

Key source packages: `internal/views/coord/loadmgr/`,
`internal/views/coord/coordview/`, `internal/metastore/kv/queryview/`, `internal/querycoordv2/ddl_callbacks_drop_load_info.go`,
`internal/datacoord/ddl_callbacks_{create,alter,drop}_index.go`,
`internal/querynodev2/qvresource/`, and the parent SN resource runtime.
