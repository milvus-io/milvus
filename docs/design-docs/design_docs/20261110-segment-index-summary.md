# Segment index summaries

Related issue: #53047.

## Goal and scope

DataCoord recovers index inventory from the catalog. Each completed index is
represented by an existing SegmentIndex row or a SegmentInfo summary. The file
list for a summary with artifacts lives in the associated manifest revision.
A segment may have both representations for different builds during migration.

`dataCoord.index.writeSegmentIndexToManifest` selects the publication layout
for new StorageV3 tasks and enables forward migration. It requires Loon FFI and
takes effect on startup. An in-flight task keeps its persisted layout.
Turning the switch off stops forward migration and sends new builds to etcd.
Existing summaries stay readable. StorageV1/V2 and L0 retain their catalog
paths.

During a mixed-binary rollout, set the switch to false and upgrade every
coordinator, DataNode and QueryNode before enabling migration. Disabling the
switch does not recreate catalog rows for completed summaries. Released
upgrade sources store their completed indexes in catalog rows.

## Forward migration

The existing DataCoord inspector selects eligible Finished catalog records and
groups them by segment. Each artifact is copied server-side into the segment's
`_index` directory. The new manifest pointer, summaries and deletion of the
corresponding records commit atomically. Artifact-free completions need only a
catalog transaction. Batches respect the existing copy and catalog limits. An
uncommitted batch retains its catalog records for retry. Restart scans the
remaining records.

Existing local artifact files that need relocation remain catalog-backed.
Readers always combine the catalog records and summaries. After publish, GC
protects the old and new layout for live builds and keeps snapshot pins
effective across placement changes.

## Metadata and publication

SegmentInfo.index_summaries identifies completed indexes by IndexID, FieldID and
BuildID, including completion without an artifact. It retains sizes and engine
versions needed by coordinator scheduling and accounting. It contains no file
list. Active task state remains keyed by globally allocated BuildID.

A successful build publishes its artifact in the manifest, replaces the summary,
and deletes the durable task record in the existing segment commit transaction.
An empty completion publishes only the summary and retires its task atomically.
In-memory completed records are removed after publication. Stale completion and
removal must not replace or erase a newer build. Data replacement invalidates
summaries under the same segment lock as the manifest change.

`GetAllSegmentIndexSummaries` returns a separate in-memory inventory type for
both catalog records and manifest-backed builds. It contains collection/partition/segment/index/build
identity, task state, deletion state, artifact presence, sizes and engine versions, including active and
pending-cleanup tasks. It has no file metadata and performs no manifest I/O.
Summary lookup, deletion staging and accounting consume this inventory directly.
A summary-only removal has no SegmentIndex task record or task-catalog action;
its inventory and counters are retired only after the segment update succeeds.
Consumers that need files resolve task records or the owning manifest explicitly;
a task record takes precedence when the same BuildID also has a persisted summary.

Readiness, build deduplication, progress and compaction decisions use summaries
plus catalog records. Load responses resolve artifact details from the exact manifest
revision paired with the segment snapshot. Load also reads live finished task
rows, such as a build that completes after its segment is dropped. Batch reads
share the manifest I/O budget and preserve empty results for missing segments.
Manifest read failures propagate.

Resource preflight requests an index projection alongside `GetSegmentInfo`.
The projection combines the returned segment's summaries with
live index definitions and finished tasks. Artifact presence, memory size and
build row count suffice for estimation. A response entry with zero indexes is
a complete projection. A missing entry uses `GetIndexInfos` for compatibility.
New summaries retain the build's row count. For summaries written before that
field existed, estimates use the segment's persisted row count. Full load
responses still resolve manifest-backed indexes from the manifest.

For an ordinary StorageV3 QueryView segment, QueryNode can
request local index resolution. DataCoord returns the summaries, retained
finished tasks, manifest pointer and matching replay cursor from the accepted
snapshot without opening the manifest. QueryNode opens that revision once per
preparation and uses it for index artifacts, column groups, delta paths and stats.
The manifest closes when preparation ends. Read states of the same revision
share the imported column groups. The View's index definitions supply its load
parameters. A finished catalog record for the same or a newer build supplies
the artifact metadata. Complete load metadata remains available for older
consumers, external collections and StorageV2. Segment diagnostics use the
selected segment's summaries and finished tasks directly. Full metadata batches
use the shared manifest I/O limit and return per-segment errors.

A QueryView load does not pin index files in DataCoord GC. If preparation
fails after a newer revision retracts an index, an ordinary StorageV3 binding
retries with current metadata and accepts a revision at or above its minimum.
Other bindings can still use their accepted metadata. Manifest reuse also
applies when the load response supplies explicit index files, as happens
for catalog-backed artifacts.

## Cleanup and restore

GC keeps active tasks, current manifest references, importing targets and
snapshot-pinned builds. Superseded artifacts are reclaimed by the build-aware
segment-rooted orphan scan after retention. Dropped index cleanup reads one
segment manifest at a time and retracts its matching summary atomically. Dropped
segments retire completed readiness in one catalog update before removing
files. Their manifests and task rows supply the retry list. Orphan scans reuse
one manifest revision at a time and check snapshot protection again at deletion.
A pending snapshot blocks new index deletions in its collection and waits for
already admitted deletions to finish before selecting revisions. The block lasts
until publication installs the build pins or snapshot creation fails.
The V0 orphan scan only knows the BuildID. Its prefix deletions wait until no
collection has a pending snapshot, and every snapshot drains already admitted
V0 orphan deletions before capture. The trailing slash confines each scan and
deletion to its own build directory.

Snapshot creation captures explicit Finished catalog records and the BuildIDs
of manifest-backed summaries with the snapshot's exact manifest pointers. The snapshot metadata
marks that manifest-backed file records are deferred. Summaries without files
remain explicit completion records. Restore and export use DataCoord's shared
manifest read limit to resolve deferred records from those versions and verify
that every captured build is accounted for. The self-contained export writes
the complete file list with rewritten paths.

To move a referenced snapshot to a version without manifest-backed snapshot
support, export it on the source version and restore the self-contained bundle.
The referenced snapshot's V6 metadata uses `index_files_from_manifest` to mark
the deferred files. Older readers ignore that field and can accept the snapshot
without copying its index artifacts, causing the target to rebuild them.

Copy/restore validates the worker result against the placement recorded at dispatch.
Targets publish their manifest and summaries or retain catalog records before
becoming visible. Legacy snapshots can supply explicit index files in either
catalog-backed layout. Workers that do not report a separate copied-index memory
estimate retain the serialized-size fallback.
L0 delete segments keep their existing format. When a snapshot captures no
index definitions, physical manifest entries left by dropped indexes are
discarded during copy.
An incompatible snapshot returns an explicit unsupported error; no format
conversion or legacy artifact publication is attempted.

The mode and physical index inventory are local cluster state. CDC continues to
replicate logical index DDL; each cluster constructs its own segments, build IDs
and artifacts. No new replicated side effect or WAL message is introduced.

## Validation

Exercise restart with manifest index reads disabled, mixed catalog and summary
reads and snapshots, a switch change with in-flight builds and copy tasks,
atomic catalog failure and retry, source artifact retention during and after
migration, and data replacement and index deletion races. Use the matching
Milvus development container for focused Go and native checks. A live upgrade
should also exercise coordinator failover and QueryNode load with real object
storage.
