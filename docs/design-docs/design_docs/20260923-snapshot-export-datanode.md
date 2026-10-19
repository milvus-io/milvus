# Distributed snapshot export execution on DataNode

- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** Pending
- **Status:** Segment-range execution implemented; release and scale validation pending.

## Scope and contract ownership

This document owns distributed Export execution, the shared Export/Restore copy
pipeline, and the Restore preparation/publication changes described below.
[External Snapshot Export and Restore](20260609-external-snapshot-export-restore.md)
owns public APIs, credentials, provider compatibility and bundle relocation.
The [storage path contract](../../agent_guides/storage/path_contract.md) owns
complete-key, path ownership and GC rules.

DataCoord owns source pinning, durable assignment/results, recovery and final
publication. DataNode owns segment inventory, path validation, provider copies
and V6 Avro OCF generation. Public APIs are unchanged. Snapshot writers emit format
6; readers retain raw binary decoding for formats 0–5 and OCF decoding for V6.
Each range must finish rewriting its V6 Avro OCF before reporting completion.

## Export lifecycle

```text
DataCoord: pin -> freeze source/configuration -> read top-level metadata
           -> persist generation and contiguous segment ranges
DataNode:  read assigned Avro -> prepare one segment -> copy physical files
           -> rewrite/write its V6 Avro OCF -> report scalar completion
DataCoord: persist each result -> Drop each writer -> metadata-only finalization
           -> write/read back staging -> persist Publishing
           -> publish/verify final metadata -> persist Completed
```

Submission persists a random `<target-prefix>/exports/<export-id>` namespace.
The frozen request carries snapshot/collection identity, exact metadata path,
source storage configuration, target root, external specification and
deadline. Workers never resolve snapshot names. The source pin protects the
immutable snapshot during copying; external objects must also remain immutable.

DataCoord reads only top-level metadata to split the manifest list into ranges.
The default for `dataCoord.import.maxSegmentsPerCopyTask` increases from 100 to
1000 and applies to TEXT as well as other schemas. Existing persisted groups are not repartitioned;
`snapshot_has_text` is ignored and no longer written. Empty snapshots need no
DataNode task. External Collection export is unsupported, including empty ones.

Every range in a generation shares:

```text
<job-root>/attempts/<generation-id>/
  files/...
  snapshots/<collection-id>/manifests/<segment-id>.avro
  _staging/metadata.json
  snapshots/<collection-id>/metadata/<snapshot-id>.json  # final publication
```

The common root preserves relative native-manifest references, including shared
partition LOBs. Confirmed worker loss or a missing task replaces only that range
with a fresh task ID, retaining the generation, output root and other results.
Transient query errors keep the assignment; explicit worker failures fail the job.
Replacement and retirement are persisted atomically. Coordinator restart resumes
these records, and retired IDs accept no results and only perform worker cleanup.
Drop cancels execution without deleting target objects; fresh IDs isolate retries
from delayed Drop requests.

Range replay relies on pinned immutable inputs and semantically equivalent target
content, including stable Avro index ordering. V6 OCF sync markers are random,
so retries need not produce identical Avro bytes. Each successful object write
must expose a complete object: either attempt decodes to the same segment, paths
and index metadata. Late provider writes may overlap a retry;
retired assignments do not block publication. This does not retry explicit cloud
copy failures: for example, Azure failed/aborted destination copies still fail the
job. Cleanup obligations remain durable until worker cleanup succeeds, the
worker is gone, or it explicitly rejects the Export task type as unsupported.
An unsupported worker still fails the Export job, but cannot block its cleanup;
transport failures retain the cleanup obligation.

## Segment preparation and inventory

Export reads top-level metadata once per task and loads only assigned Avro.
Each bounded segment worker reads, validates, inventories, copies and finishes
that segment before taking another. Restore receives source segment descriptions
from DataCoord and uses the same per-segment physical inventory/deduplication.
There is no task-wide inventory, destination map or preparation barrier.

`EnumerateSegmentFiles` shares V1/V2 metadata enumeration and reads the exact
pinned native manifest once. It supplies columns, deltas, stats, LOBs and the
pinned manifest object, supplemented by nonempty PB delta/stats/BM25 paths.
Pathless summaries remain metadata only. V3 insert and TEXT/JSON PB paths do not
create physical copies. Revision zero is empty and has no manifest object.
Neither directory objects nor other native revisions are discovered by LIST;
read failures never fall back to LIST.

Avro `IndexFiles` is the complete vector/scalar index inventory for Export and
Restore, including an empty list. `ManifestHasIndex` does not authorize native
index discovery. Legacy external snapshots with native-only index artifacts
receive no fallback. Native manifests may retain unselected historical index
records whose artifacts are absent from the bundle; Restore retracts them before
publishing selected target indexes. The bundle is a Restore input, not a native
manifest containing every historical index artifact.

External Collection Restore uses the same pinned inventory without directory
listing. Before normalization and planning, it excludes native manifest references
with a valid URI scheme, retaining those references in the copied manifest. This
includes external column, delta and stats references; bucket equality is irrelevant.
The pinned manifest object and snapshot-selected indexes are never filtered by
this rule. A nonempty segment without a pinned manifest object fails.

Export validates each segment's identity, URI/root ownership, source/destination
overlap and native relative-layout preservation before its own writes. Canonical
Avro basenames must be `<segment-id>.avro` and match the decoded segment ID.
Top-level native-manifest entries are authoritative: nonempty V3 segments lacking
an entry fail; DataCoord does not reconstruct entries from Avro/native reads.
For external native references, validate raw scheme/bucket/endpoint/root before
object-key deduplication can hide an invalid alias. Restore uses newly allocated
target IDs and its own path mapping to preserve native layout.

`DeduplicateFileCopies` collapses identical source/destination pairs per segment
and rejects different sources for one destination, including generated Avro
destinations. Source aliases normalize; destination keys remain literal. One
source may have several destinations. There is no cross-segment conflict scan.
Valid segment-owned destinations are disjoint; immutable shared LOBs may be
copied repeatedly within or across tasks. This reduces retained active inventory
at the cost of repeated requests and repeated accounting.

## V6 Avro OCF and relocation

After a segment's physical copies succeed, Export clones its already decoded
record, rewrites explicit paths into `<attempt-root>/files`, and encodes/writes
V6 Avro OCF through the shared copy permits. It never rereads source Avro or mutates
source records; encoded buffers exist only for active writes. V3 insert
placeholders are cleared, while manifest-owned TEXT/JSON references stay native.
Native manifest objects are copied byte-for-byte; DataCoord relocates
their top-level entry points.

Self-contained bundle relocation uses the shared `RebaseObjectPath` rule.
Restore and milvus-table rebase Avro entry points, native bases and L0 deltalogs
before current-filesystem resolution. Referenced snapshots are not rebased.
Re-export uses the current bundle's `files` root, avoiding repeated prefixes.
No additional public snapshot format version is introduced beyond upstream V6.
Format parsing compatibility does not establish support for native/storage
features missing from an older release.

## Scheduling, concurrency and provider copies

Export jobs have no job-count cap. Generic CreateTask/QueryTask/DropTask carry
`SnapshotExportCopy`; placement uses the available slots from QuerySlot, without
requiring an Export capability field from a shared-pool manager. Workers must
support this task type before Export is enabled. Each Export/Restore task
consumes `dataCoord.snapshot.taskSlotUsage` slots (default 1, omitted from the
default YAML). DataCoord persists the weight at creation and sends it to the
worker; retries and recovery retain it. Export freezes it in the job request,
so all shards of that job share the same weight. Older Export requests use one
slot. Older workers ignore the field and still report one slot during upgrades.
CreateTask properties carry the same effective slot weight as the copy request.
QueryTask consumes the outer state before decoding worker payloads: queued,
retrying or running tasks may have no payload; completion requires a valid result.
Slot accounting is a scheduling budget, not a hard node concurrency limit.
Export has its own registry, independent of Import scheduling.
Create is asynchronous and idempotent for identical inputs; conflicting requests
for one task ID fail. Requests contain no file lists and are limited to 1 MiB.

DataCoord sends `dataCoord.snapshot.copyConcurrency` (default 128) in each
Export/Restore task request. Export persists it in the frozen job request;
Restore reads it when assembling a dispatch. Running tasks retain their budget.
Older requests without a positive limit use 128. Older workers ignore the new
field during rolling upgrades. The parameter is configurable but omitted from
the default YAML; the legacy DataCoord Export parameters remain deprecated.

Each task owns its permit channel, shared by its segment workers. Export
Avro/native reads and Export/Restore file operations use that task's budget.
There is no node-wide snapshot copy limit or CPU multiplier; different tasks
execute independently. Task admission still uses the existing scheduler slots.
Import configuration does not control snapshot file-copy concurrency.

Export segment workers are bounded by the task limit. Avro reads release their
permit before inventory/copy processing. Top-level metadata reads are outside
this budget. Ordinary readers retain their existing behavior. Restore segment
workers run in the import pool; their orchestrator stays outside it and file
operations use the task budget, avoiding recursive pool submission.

`CopyFiles` acquires permits before launching callbacks. Failure cancels queued
work and joins active operations. Avro decode and native calls must return before
cancellation finishes. Other segments may already have written files or committed
manifests when a segment fails. A failed task cannot publish an Export snapshot.
Restore retains all provider-confirmed writes and new manifest revisions,
including successes racing cancellation; one TaskManager update records results
and cleanup files after workers drain. Drop cancels and joins an existing task;
an absent task succeeds without retaining a deletion marker. A later Create is
subject to normal admission and its execution deadline. Shutdown also joins tasks.
Draining tasks retain slots.
Task clients are owned and closed by the executor. Registration, query and Drop
identify tasks by `(ClusterID, TaskID)`, isolating instances on shared workers.

Physical data copies remain provider-side; data bytes do not stream through
DataNode. Restore retains `dataNode.import.copyObjectTimeout`. ObjectStore.Copy returns
the object size for Export, Restore and metadata copies. Backends without a
size-returning copy operation query the destination after copying; failure of
that query fails the operation even when the object was already written.
`CopySizeError` preserves the underlying error and write confirmation, so Restore
records the destination for cleanup without counting unverified bytes.
Export discovers sizes during copying, never during planning.
MinIO/S3 stats each source once and uses multipart copy at `minio.multipartCopyThreshold`
(default lowered from 1 GiB to 500 MiB, effective minimum 10 MiB), or above the
single-copy limit. This shared threshold also affects existing S3 copy callers.
Parts are `max(10 MiB, ceil(size / 10000))`, with up to ten concurrent parts per
file and the existing 5 GiB/5 TiB limits. Each part has at most ten SDK calls
with 200 ms–3 s backoff, in addition to transport retries; permanent errors stop.
Source ETags guard parts when available. Failure joins workers then aborts only
its upload with a detached 30-second/three-call bound, preserving the original
error. There is no whole-object retry or persistent multipart checkpoint.
File permits bound files, not HTTP requests: multipart can multiply requests by
ten. GCP/Azure retain their provider copy paths; other size accounting uses the
existing destination-size fallback.

Azure snapshot copies use destination `If-None-Match: *`; ordinary object copies
retain overwrite semantics. Condition/pending conflicts join an
existing copy only after validating source identity, CopyID and status; completed
same-source copies are reusable, failed/aborted copies fail. Parallel ranges
require upgraded copy workers and immutable sources/job-owned destinations.

## Completion, publication and recovery

Workers return state and scalar `CopiedFiles`, `TotalFiles`, `CopiedBytes`.
Results carry no physical object lists. Completed
file counts include successful Avro/native copies but exclude generated top-level
metadata. Bytes count successful data/index copies only. Shared objects count
again per segment that copies them; this is not unique bundle size. Failed
attempts contribute no accepted totals. Negative/overflow counts fail; zero
bytes is valid. Running progress is volatile, may reset, and is not a checkpoint.

DataCoord persists each accepted result before Drop. After every current writer
exits, finalization reads source top-level metadata only, validates frozen
identity, canonical paths, contiguous range coverage and counters, derives target
paths and writes/readback-validates `_staging/metadata.json`. It builds no global
file map and reads no Avro/native manifest. State, generation and writer-exit
guards are rechecked atomically before persisting `Publishing` with root, URI and
totals. Pre-Publishing storage/catalog failures can rebuild staging from persisted
results; finalization creates no worker assignment.

Publishing recovery uses target staging and persisted job state only, independent
of source availability and the copy deadline. Final metadata publication compares
bytes with staging and verifies ambiguous writes by readback. Matching output is
idempotent; conflicting bytes, missing/corrupt staging or permanent target-access
failure fail closed; transient/ambiguous storage failures retry. Completed is
persisted only after publication verification. Public state remains Executing/99
until then, and URI/bytes remain hidden. Terminal transition clears credentials;
pin cleanup retries precede terminal retention. Failed or abandoned generations
can leave unpublished objects; generic orphan GC does not reclaim these prefixes.

Existing `ExportSnapshotJob` wire numbers remain. `copy_request` freezes inputs
and generation; `copy_tasks` stores ranges, assignments and accepted responses;
`retired_copy_tasks` stores cleanup obligations. `copy_cursor` is final-only and
does not resume copying. Legacy plan/fingerprint fields retain wire numbers but
are not populated. Control-plane storage scales with ranges, not files.
Publishing jobs finish from staging. Workers must support the new Export task
type before distributed Export is enabled. Errors use the shared snapshot-storage
sanitizer; output objects contain no credentials.

## Restore admission and metadata publication

Named/external API preflight reads top-level metadata only. Job creation reads
and validates Avro with 16 shared DataCoord read/decode permits, then caches the
snapshot and segment lookup for first/subsequent task assembly. Target positions
are cloned before remapping. Cache loss/restart reloads; terminal updates release
it. Native reference validation belongs to DataNode's inventory read, and no
per-object existence probes are issued. Missing objects fail copying. These are
per-segment checks, not a job-wide write barrier.

New external Restore WAL messages carry `metadata-v1:` top-level fingerprints;
job creation verifies them and persists the full metadata/Avro fingerprint for
recovery. Legacy full/absent fingerprints retain replay behavior. Older
coordinators reject the prefixed form, so downgrade must drain pending Restore
broadcasts. Cached metadata does not freeze external object contents; they must
stay unchanged. Avro changes before the first job load have no API-time Avro
baseline.

Permanent read/parse/reference failures after admission create a queryable failed
job; transient reads and cancellation remain retryable. Named Restore persists
the failed record with its pin ID before unpinning; failed persistence retains
the pin and retries WAL. Unpin failure/crash uses the existing TTL safety net.
WAL replay of an existing job avoids another read/unpin. Target collections are
not automatically deleted.

Task assembly consumes only Avro index inventory. Fresh target BuildIDs come
from a task-local, lazy 1,000,000-ID `AllocN` range, refilled in equal batches;
unused IDs are discarded and index-free tasks allocate none. Deduplication is
per target segment; redispatch gets a fresh range. Cancellation is checked
before consumption and after allocation; in-flight AllocN retains its bounded
timeout. Cleanup prefixes derive from these same mappings before dispatch.
Target segments register individually with `meta.AddSegment`, importing=true;
registration/checkpoints finish before job publication. Partial registration
failure leaves earlier registrations and dispatches no copies.

Ordinary V3 preparation retains inherited native index IDs, including a known-empty
list. After byte-for-byte copying to an exclusively owned target, Restore removes
those entries and publishes selected target indexes without Go-side target
re-enumeration. Native transaction base reads/revision writes remain. Paths without
a prepared inventory retain enumeration; live-segment updates retain generic
absent-index/ExpectedBuildID safeguards.

DataNode acknowledges native rewrite only after commit and returns
`target_index_id` for published entries; source `index_id` keeps its meaning.
DataCoord retains baseline target-manifest readback for nonempty index
acknowledgements and older workers without an acknowledgement. It verifies index
identities, definitions, paths and artifacts before installing any result, then
checks dispatch-time manifest/etcd placement. Explicit failure is rejected;
explicitly acknowledged empty index sections need no readback. Installation
retains the verified index ID if a definition is replaced after validation.

Each segment then publishes through the ordinary metadata API:

1. Persist binlogs, target manifest and TEXT/JSON stats with `IsImporting=true`.
2. Install vector/scalar index metadata (manifest-backed in memory; legacy catalog).
3. Clear `IsImporting` and request DataView reconciliation.

GenSnapshot excludes importing segments. Failure stops publication, leaves the
current/later targets importing in memory, and retains cleanup protection;
earlier successful targets already reconcile. There is no task-wide atomic
catalog transaction or publication-batch setting. Task cleanup intent clears
only after every result succeeds. Restart retains durable manifest recovery.

## Cleanup and verification limits

Restore failure cleanup excludes only the actual target partition's shared
`lobs/` namespace, even for a task that first created a LOB. Durable coordinator
cleanup prefixes remain segment/index-owned. Reference-aware LOB GC owns orphan
reclamation under its safety window; disabling it can retain orphans. Export
Drop removes worker state, never copied objects. This does not establish safe
cleanup for arbitrary externally authored shared-file layouts.

The implementation has targeted local/mock and native-manifest regression tests;
this document does not certify a deployed release. Real cloud copies, rolling
upgrade/downgrade, cross-release restore/load/query and large multi-node
throughput/memory/failure measurements remain release work. Shared permits and
bounded inventories are resource bounds, not measured performance gains.

Key implementation entry points:

- `internal/datacoord/snapshot_export_{manager,meta,task,shards}.go`
- `internal/datanode/snapshot_export.go`
- `pkg/snapshotio/{snapshot_copy,snapshot_export_shards,snapshot_export_files,file_copy}.go`
- `internal/datanode/importv2/{task_copy_segment,copy_segment_utils}.go`
- `internal/datacoord/{copy_segment_task,copy_segment_job}.go`
- `internal/storagev2/packed/manifest_commit.go`

## Reusable package

[`pkg/snapshotio`](../../../pkg/snapshotio/README.md) provides the shared engine
for Export and backup callers. Milvus adapters retain client
construction, configuration, durable scheduling and publication fencing. Restore
retains business-specific ID/path conversion and visibility while sharing format,
inventory and object-copy execution. Native reads link milvus-storage;
backup callers do not import Milvus internal packages or construct native readers.
