# MEP: Snapshot-Sourced StorageV3 Backup Import

- **Created:** 2026-09-02
- **Updated:** 2026-09-09
- **Status:** Proposed
- **Feature DRI:** @weiliu1031
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Related Issue:** [milvus-io/milvus#52930](https://github.com/milvus-io/milvus/issues/52930)
- **Related PR:** [milvus-io/milvus#52932](https://github.com/milvus-io/milvus/pull/52932)

### Implementation Status

Commit `b6b908c4dd97b19eab5a6e81fc442a4ff3a846d3` implements the
snapshot-source baseline. The working-tree extension now implements source
timestamp capture/provenance, L0 selection and distribution, typed WAL/ACK and
ImportFile descriptors, bounded delete merging in the existing reader, and
cancelable memory admission in both PreImport and Import. Typed readers check
original row timestamps before filtering and fail closed on invalid nonzero
source commit times. Malformed durable descriptors create Failed jobs rather
than entering an ACK retry loop.

L0 folding activation remains job-wide. Typed readers are used for both jobs
with applicable L0 and jobs opting into `external_spec`, even without L0.
Only no-L0 jobs without `external_spec` keep the baseline one-manifest
representation, reader behavior, and resource admission; the legacy
`l0_import` path is not enabled or repurposed.

Local deterministic tests exercise real V3 data and V1/V2 delete files,
PreImport/Import execution, timestamp ordering, memory/cancellation failures,
WAL/CDC message transformation, ACK binding, protobuf persistence, regrouping,
and snapshot/export provenance. The local Go SDK run on 2026-09-09 passed both
layouts with and without L0, plus self-contained cross-bucket imports with and
without L0. A live CDC/backend
matrix, complete restore regression and all acceptance gates in section 18.4
are not yet verified. Changed-function coverage is also not yet at the 99%
repository gate. Do not equate the implemented runtime path with release-ready
end-to-end verification.

## 1. Decision Summary

Backup Import accepts a snapshot metadata object as its source instead of
requiring the caller to provide a StorageV3 partition prefix and
`storage_version=3`.

The feature remains an **Import** operation:

- the target collection and its target partitions already exist;
- rows are read from the source snapshot and rewritten into newly allocated
  target segments;
- target vchannel routing, target storage format, Import job state, and
  commit/abort behavior remain owned by the existing Import pipeline; and
- snapshot collection properties, partitions, indexes, and physical segment
  identities are not restored.

The implementation reuses `snapshotio` only to parse and validate the source.
It does not call `RestoreSnapshot`, `RestoreExternalSnapshot`,
`RestoreCollection`, `RestoreIndexes`, or `CopySegment`.

The first version supports both snapshot layouts:

- `SnapshotLayoutReferenced`, written by `CreateSnapshot`; and
- `SnapshotLayoutSelfContained`, written by `ExportSnapshot`.

The first version imports StorageV3 data segments only. Existing StorageV1 and
StorageV2 backup Import requests keep their current path-based contract.

Snapshot L0 segments become **read-time delete inputs** for those data segments.
DataNode merges applicable L0 deletes with each data segment's own deletes and
emits only surviving rows, before target routing or writing. It does not create
target L0 segments or delete rows already present in the target collection.
L0 inputs may use V1, V2, or V3 delete storage; this does not add V1/V2 data
segment support to snapshot-source Import.

### 1.1 Narrowed Activation Boundary

DataCoord decides once, during snapshot expansion, whether any selected data
segment has an applicable captured L0 segment. Applicability is defined by
source channel and partition scope in section 10.2, not by whether an L0 read
returns records or whether those records survive the requested timestamp range.

| Input | Execution contract |
|---|---|
| Ordinary file Import or legacy V1/V2 backup Import | Existing path, unchanged |
| Legacy `l0_import=true` outside snapshot-source Import | Existing delete-import path, unchanged |
| Snapshot with no applicable L0, without `external_spec` | Existing one-manifest-per-file path, no new descriptor |
| Snapshot with no applicable L0, with `external_spec` | Version 2 descriptors, source-storage routing and bounded typed readers; source commit timestamp remains zero |
| Snapshot with applicable L0 and reliable source timestamps | New descriptors and bounded read-time folding |
| Snapshot with applicable L0 but missing provenance or invalid input | Explicit failure, never fallback to the baseline path |

Activation is **job-wide**, not reevaluated per task. Every data file in an
activated job carries a descriptor, including files with no individually
applicable L0. This avoids mixed representations in one WAL message and keeps
dispatch stable when files are regrouped. A task containing only such files
still uses the descriptor path. A separate job with no applicable L0 remains
on the baseline path only when `external_spec` is absent, regardless of its
snapshot's age or timestamp marker.

External-storage opt-in independently requires version 2 descriptors to prevent
older workers from reading same-named objects in the target bucket. Descriptor
presence also selects the bounded delete map and cancelable memory admission
in both phases. Thus even an external job with no L0 reserves row-buffer plus
delete-map memory, and its segment-local deletes can exceed the configured
`dataNode.import.readDeleteBufferSizeInMB` budget (16 MiB by default). Such a
job fails explicitly; it does not fall back to the unbounded baseline reader.
This resource and mixed-version boundary is part of the `external_spec` opt-in,
not a change to existing no-option jobs.

This boundary reduces regression scope; it does not remove the need to prove
delete correctness, cross-stage input preservation, or bounded resource use.
The source timestamp capture fix remains a shared snapshot-producer change and
requires independent snapshot/export/restore regression coverage.

## 2. Problem

The existing backup Import contract makes the caller describe Milvus's
physical storage layout:

```text
backup=true
storage_version=3
files=[<StorageV3 partition prefix>]
```

That contract is not sufficient for StorageV3:

- segment contents are defined by an immutable manifest, not by directory
  enumeration;
- a partition also contains a shared `lobs` directory, which is not a segment;
- TEXT values may reference partition-level LOB files owned by individual
  segment manifests;
- readable deltalogs are recorded by the segment manifest;
- choosing `ManifestLatest` can read a version newer than the snapshot; and
- callers should not have to know the source storage version when the snapshot
  already records it per segment.

A snapshot metadata object already contains the source schema, partition
descriptions, segment inventory, exact segment manifest paths, segment storage
versions, and snapshot layout. It is therefore the correct source descriptor
for a backup Import.

## 3. Terminology

Three independent version dimensions must not be conflated:

| Dimension | Meaning | Source of truth |
|---|---|---|
| Snapshot layout | `Referenced` or `SelfContained` object placement | `SnapshotMetadata.layout` |
| Snapshot format version | Metadata and Avro schema compatibility | `SnapshotMetadata.format_version` |
| Segment storage version | V1, V2, or V3 physical segment representation | `SegmentDescription.storage_version` |

The public Import request does not specify any of these values. Milvus derives
them from the snapshot metadata and rejects unsupported combinations.

## 4. Goals

- Accept one snapshot metadata path as the source of a backup Import.
- Preserve existing Import semantics for an already-created target collection
  and partition.
- Support both referenced and self-contained snapshot layouts through the
  shared `SnapshotReader`.
- Use the exact StorageV3 manifest captured by the snapshot.
- Resolve inline and out-of-line TEXT values through the shared
  milvus-storage segment reader.
- Apply manifest deltalogs within the requested Import timestamp range.
- Fold captured L0 deletes from the same source channel and applicable source
  partition, including collection-wide `AllPartitionsID` L0 segments.
- Within L0-bearing snapshot jobs, use each source data segment's effective
  row timestamp when evaluating deletes, including segments created by an
  earlier Import or CDC transaction.
- Preserve the existing source representation, reader semantics, and resource
  admission for snapshot jobs without applicable L0 and all other Import kinds.
- Reuse the existing backup `ezk` contract to read CMEK-protected StorageV3
  snapshots whose schemas do not contain TEXT/LOB fields.
- Keep source and target storage formats independent.
- Preserve existing Import pre-import sizing, scheduling, row hashing,
  auto-commit, manual commit, abort, and CDC behavior.
- Fail closed for incompatible schemas, unsupported snapshot contents, unsafe
  paths, invalid source encryption metadata, and unsupported CMEK+TEXT/LOB
  sources.

## 5. Non-Goals

- Creating the target collection or target partition.
- Restoring source collection properties, consistency level, indexes, index
  files, shard count, segment IDs, or channel assignments.
- Directly adopting or copying source segments into the target collection.
- Preserving source partition placement. One Import request reads all data
  partitions represented in the snapshot and uses ordinary Import target routing:
  a named or default partition for normal collections, or partition-key hashing
  across the target collection's existing partitions.
- Creating target L0 segments, invoking `L0ImportTask`, or applying source
  deletes to pre-existing target rows.
- Running source compaction or generating intermediate compacted segments as
  part of Import.
- Importing external collections.
- Arbitrary cross-cloud/provider reads and snapshots spanning multiple source
  buckets. Cross-bucket snapshot import can opt into the existing snapshot
  external-storage provider/endpoint policy with `external_spec`.
- CMEK-protected StorageV3 snapshot sources containing TEXT/LOB fields. That
  combination remains a follow-up until the shared milvus-storage
  `SegmentReader` can receive Milvus's source key-retriever context.
- Supporting StorageV1, StorageV2, or mixed-storage-version **data segments**
  through the new snapshot-source contract in the first version.
- Unbounded in-memory L0 folding, external sorting/spill, or a shared delete
  cache in the first version.
- Migrating no-L0 snapshot jobs without `external_spec` to the new descriptor,
  changing legacy reader defaults, or refactoring all Import memory-allocation
  callers.

## 6. Why This Is Import, Not Restore

`RestoreSnapshot` and backup Import have different user-visible contracts.

| Behavior | Snapshot-sourced Import | RestoreSnapshot |
|---|---|---|
| Target collection | Must already exist | Must not exist |
| Schema authority | Target collection | Snapshot |
| Target partition | Existing partition selected by request | Partitions recreated from snapshot |
| Data path | Decode rows and rewrite new segments | Copy physical segment files |
| Index behavior | Target indexes build normally | Snapshot index definitions/files restored |
| Routing | Rehash rows to target vchannels | Preserve segment-level snapshot mapping |
| Job protocol | Import precheck, commit, abort, CDC | Restore and CopySegment jobs |

Routing an Import request to RestoreSnapshot would silently replace Import
semantics with collection restoration. Reusing CopySegment would also bypass
the row-level schema checks, partition routing, and Import commit protocol.

The selected design therefore reuses only snapshot parsing and validation.

## 7. Public API Contract

No public protobuf field is added. The existing Import request carries exactly
one complete snapshot metadata URI in `files` and uses options to identify the source:

```json
{
  "collectionName": "target_collection",
  "files": [
    ["s3://milvus-bucket/snapshots/100/metadata/123.json"]
  ],
  "options": {
    "backup": "true",
    "source_type": "snapshot",
    "start_ts": "0",
    "end_ts": "18446744073709551615"
  }
}
```

The exact REST/SDK representation of the existing `files` field remains
unchanged. The semantic requirements are:

1. `backup` must be `true`.
2. `source_type` must be `snapshot`.
3. The request contains exactly one non-empty file with exactly one complete
   metadata URI. Bare object keys are rejected, with or without `external_spec`.
4. Target partition selection follows ordinary Import. For a normal collection,
   an omitted `partition_name` selects `_default`; an explicit name must identify
   an existing partition. For a partition-key collection, `partition_name` must
   be absent and rows are hashed across its existing partitions. The `backup`
   flag preserves source PKs without imposing legacy backup partition selection.
5. All source data partitions in the snapshot are imported. Source partition
   IDs and channels are retained internally only for matching L0 deletes.
6. `storage_version` must be absent. Supplying it together with
   `source_type=snapshot` is rejected as ambiguous input.
7. `l0_import=true` is rejected. Snapshot L0 folding is automatic and has
   different semantics from importing deletes into the target collection.

`options.external_spec` optionally contains a JSON string with the same `extfs`
configuration used by external RestoreSnapshot, for example
`{"extfs":{"access_key_id":"...","access_key_value":"..."}}`.
It is accepted only with `source_type=snapshot`, must be nonempty and at most
64 KiB, and requires a complete metadata URI identifying the source bucket.
Without this option reads still use the instance storage, and the metadata URI
must match its provider, bucket, effective endpoint and transport. A URI naming
another endpoint is rejected even if that endpoint is permitted by the snapshot
cross-storage policy. URI validation occurs before metadata IO. This admission
rule only applies to new snapshot Import requests: shared snapshot readers,
bare object references inside snapshots and already-expanded persisted tasks
keep their existing representation. Callers that used bare metadata keys must
switch to complete URIs, such as the location returned by DescribeSnapshot.
The coordinator-owned `_snapshot_source_uri` option is rejected in public
requests; after validation DataCoord persists the original URI in that option
alongside the expanded immutable source inventory.

DataCoord resolves metadata, manifests and L0 using a source-only chunk manager
and storage config. The shared PreImport/Import reader resolves the same URI
and spec on each execution, without rereading snapshot metadata. The task's
target chunk manager/storage config is never replaced. Failed source access
never falls back to target storage. This does not change commit/abort semantics.

There is no source-partition selector. A new request carrying the earlier
`source_partition_name` option is rejected rather than silently broadening its
scope. Already-expanded persisted tasks keep their original file inventory and
do not pass through public request admission or snapshot expansion again.

There is no new public L0 toggle. After validation, DataCoord selects the
baseline or L0-bearing execution contract according to section 1.1. A caller
cannot opt out of applicable deletes to make an unsupported snapshot succeed.

Existing StorageV1 and StorageV2 path-based backup requests without
`source_type=snapshot` are unchanged. A path-based request that explicitly sets
`storage_version=3` is rejected; StorageV3 backup Import must use a snapshot
source so that Milvus receives an exact manifest version.

## 8. Source Layout Contract

### 8.1 Referenced Snapshot

A referenced snapshot points to the original Milvus segment objects. The
snapshot metadata, segment manifests, segment data, deltalogs, and LOB files
must remain readable and immutable until the Import job reaches a terminal
state. This includes every selected L0 manifest and its delete files, even
when that L0 belongs to `AllPartitionsID` rather than an individual partition.

Dropping the source snapshot or allowing source GC to remove referenced files
during Import can fail the job. The target cluster cannot pin resources owned
by another cluster.

### 8.2 Self-Contained Snapshot

A self-contained snapshot stores copied data under the bundle's `files`
subtree. `SnapshotReader` derives the bundle root from the metadata path and
rebases metadata and segment references when the complete bundle has moved.

Import consumes the rebased paths returned by `SnapshotReader`; Import-specific
code does not implement another root-rewrite algorithm. V3 L0 manifests and
their referenced deletes, as well as legacy L0 deltalog paths, must be closed
under the relocated bundle root. Export and validation must cover these
objects, not just data-segment manifests and LOBs.

### 8.3 Exact Manifest Requirement

Each generated segment source carries the exact `manifest_path` recorded in
the snapshot. The DataNode must not reconstruct the path with
`ManifestLatest`. This preserves the snapshot's point-in-time segment version
even if the source collection later publishes a newer manifest.

V3 L0 delete paths are resolved from their exact captured manifests during
expansion. V1/V2 L0 delete paths come from the snapshot's explicit deltalog
inventory. No L0 prefix walk or lookup of the live source collection is allowed.

## 9. Architecture

```text
Client Import request
  files = [snapshot metadata path]
  backup=true, source_type=snapshot
          |
          v
Proxy Import validation
  - resolve existing target collection/schema/vchannels
  - resolve default/named partition or ordered partition-key target partitions
          |
          v
DataCoord snapshot-source expansion
  - read metadata and manifests with SnapshotReader
  - validate layout, paths, schema, and segment types across all source partitions
  - select StorageV3 L1/L2 data segments
  - match captured L0 inputs by source channel and partition scope
          |
          v
Any applicable L0 in the selected input?
          |
    +-----+---------------------------+
    | No                              | Yes
    v                                 v
Baseline manifest files       Validate input + provenance
    |                                 |
    |                         WAL descriptors -> ACK
    |                         -> persist ImportFiles
    |                                 |
Existing PreImport            Bounded PreImport: fold L0,
    |                         count surviving rows, regroup
    |                                 |
Existing Import               Same-input Import: fold L0,
    |                         hash and write surviving rows
    |                                 |
    +---------------------------------+
                      |
                      v
          Existing commit/abort and index build
```

Any validation or read failure on the applicable-L0 branch fails that job;
there is no error arrow back to the baseline branch. Both branches preserve
the target writer and publication protocol.

## 10. Component Changes

### 10.1 Proxy

Proxy keeps the existing Import entry point and target collection validation.
It adds option validation for the snapshot-source contract:

- require backup mode;
- require one complete snapshot metadata URI;
- reject `storage_version`, `l0_import`, and unsupported option combinations;
- reuse ordinary Import target partition selection, including default partition
  selection and partition-key routing, without creating partitions; and
- forward the request through the normal Import broadcast path.

Proxy does not read snapshot objects. These validations belong to the existing
snapshot-source baseline; the L0 extension adds no new Proxy API or routing
branch.

### 10.2 DataCoord

DataCoord extends the existing snapshot expansion without introducing another
public source kind:

1. Resolve source storage using `external_spec` when supplied; otherwise reuse
   the instance ChunkManager. Open the metadata through that source reader.
2. Call `SnapshotReader.ReadSnapshot(..., true)`.
3. Let the reader validate snapshot format compatibility and normalize the
   referenced or self-contained layout.
4. Validate snapshot paths with the shared snapshot path validators.
5. Validate source collection type and schema compatibility.
6. Collect data segments from every source partition in the snapshot.
7. Exclude L0 from the data-file list. Reject any data segments
   whose storage version is not StorageV3 or whose manifests are missing or
   non-exact.
8. Independently scan the snapshot's L0 inventory. For each data segment `s`,
   select an L0 segment `d` only when:

   ```text
   d.channel_name == s.channel_name
   AND (d.partition_id == s.partition_id
        OR d.partition_id == AllPartitionsID)
   ```

   Preserve each data segment's original partition/channel while matching
   collection-wide and partition-local L0. Do not use target partition IDs or
   target vchannels for matching.
9. Decide `hasApplicableL0` for the entire selected input. If false, finish
   normal source validation and return the baseline exact-manifest files with
   no descriptors unless `external_spec` is present. External jobs instead
   receive version 2 descriptors with zero source commit timestamps, even
   without L0. Do not require the new timestamp provenance marker in this case.
10. If true, verify source timestamp provenance as specified in section 12.2.
    Resolve exact V3 L0 manifests or explicit V1/V2 deltalog inventories.
    Validate original URI bucket/endpoint and layout/root before normalizing
    delete references to object keys. Persist normalized keys and use them for
    deduplication and decoder-conflict checks; URI/key aliases identify one
    object. ChunkManager must never receive a complete URI as an object key.
11. Sort data sources by source segment ID, deduplicate each source's delete
    references, and generate a descriptor for every data file in the job.
    Conflicting metadata for the same delete object is an error, not a reason
    to choose one arbitrarily. Check incremental source-plan growth and the
    final encoded size before broadcast.
12. Broadcast the complete immutable input through the existing Import WAL
    message. The ACK callback attaches each descriptor to its internal
    `ImportFile` before persisting the job.

Determine applicability before skipping valid empty markers or filtering
delete timestamps. A missing identity needed to decide scope, a failed L0
read, or an unsupported applicable representation cannot mean
`hasApplicableL0=false`. Valid empty L0 markers may produce empty delete lists
in an activated job, but do not switch that job back to baseline execution.

Schema and source EZK validation must still precede opening any V3 manifest.
`snapshotImportValidationData` currently strips legacy binlog fields to keep
V3 manifests authoritative. The L0 extension must retain and validate explicit
V1/V2 **L0 deltalogs**, without restoring unused data/index fields to the Import
input. Missing channel identity or an unsupported applicable L0 representation
fails validation; unrelated partitions' L0 inputs are not read.

Retries reuse the persisted input representation: exact manifests for baseline
jobs, or complete descriptors for L0-bearing or external-storage jobs. Neither
task regrouping nor a DataCoord/DataNode restart repeats snapshot expansion or the activation
decision. A snapshot with no selected data segments remains invalid; L0-only
input is not an operation on target rows.

### 10.3 Import Source Representation

Add an internal, versioned source descriptor in `pkg/proto/internal.proto`.
L0-bearing jobs and external-storage snapshot jobs populate it. The fields are:

```protobuf
message SnapshotImportSource {
  uint32 version = 1; // 1: local source; 2: requires external source options.
  string manifest_path = 2;
  uint64 source_commit_timestamp = 3;
  // Explicit legacy inventory; use the existing V1/V2-compatible reader.
  repeated string legacy_l0_deltalogs = 4;
  // Exact V3 manifest inventory; use the existing packed delta reader.
  repeated string manifest_l0_deltalogs = 5;
}

// Added to internal.ImportFile; existing fields 1-3 remain unchanged.
SnapshotImportSource snapshot_source = 4;
```

Only concrete, validated delete object paths enter the descriptor; valid empty
markers are handled during expansion. Actual readability is established when
the tasks read the objects. Entry counts are validation hints at the manifest
boundary, not a substitute for reading the referenced delete records.
Source channel/partition IDs are needed for expansion, not by the row reader,
so they are not duplicated in every descriptor.

The two path lists describe the existing **delete reader contracts**, not the
source segment's storage version. In the current code, `BulkPackWriterV2`
inherits `BulkPackWriter.writeDelta`, which writes V1 deltalogs, while
`BulkPackWriterV3.writeDelta` writes packed V2 deltalogs recorded in a V3
manifest. Thus copying `SegmentDescription.storage_version` directly into
`NewDeltalogReader` is incorrect. Legacy inventories retain the existing
V1/V2-compatible `readDelete` behavior; V3 manifest inventories use
`readDeleteV3`. No new file-format detector is introduced.

The WAL Import body uses external `msgpb.ImportFile`, not internal
`ImportFile`. Adding the internal field alone would lose it before job
persistence. To avoid a public `milvus-proto` dependency change, add
`repeated internal.SnapshotImportSource snapshot_sources = 1` to the existing
`messagespb.ImportMessageHeader`. `messages.proto` already imports
`internal.proto`.

For an L0-bearing or external-storage job, the producer places one descriptor
in the header per body file, in identical order. The body still contains one file per selected
data segment, but its `paths` are **empty** for a typed source. File IDs are
allocated by the ACK callback, so pairing uses the original array index at this
boundary only. The callback validates cardinality, source kind, descriptor
version, and absence of legacy paths, then copies each descriptor into the
same internal file whose ID it allocates. All subsequent persistence,
PreImport statistics, regrouping, Import tasks, and retries carry that internal
file; they must never reconstruct files from paths alone or rejoin parallel
arrays. Broadcast and CDC must preserve header/body ordering until this binding.

For a snapshot job without applicable L0 or `external_spec`, the header has no
descriptors and each body file retains its exact manifest in `paths[0]`. The ACK callback keeps
the existing conversion for that representation. There is no migration of
baseline persisted jobs or automatic conversion of newly created no-L0 jobs
without `external_spec`.

Empty legacy `paths` are intentional. If an older consumer drops the new
header or protobuf field, the baseline snapshot reader rejects the resulting
zero-path file. Leaving a usable manifest in `paths[0]` would instead allow an
old DataNode to ignore L0 inputs and successfully import deleted rows. Keep
body-file cardinality nonzero so loss of the header cannot become an empty,
successful job. Compatibility tests must exercise both old ACK and old worker
behavior, not just unknown-field round trips.

Within an L0-bearing job, a file can have empty L0 lists and must still retain
its descriptor and source commit timestamp. This includes regrouped tasks
whose files all have empty lists. Reader and admission dispatch use descriptor
presence, not the local number of delete paths.

The reader accepts either a valid descriptor with no legacy paths, or the
baseline one-manifest representation for new no-L0 jobs without `external_spec`
and already-persisted baseline jobs. Both at once, an unknown version, or neither are errors. Reject
descriptors outside snapshot backup mode and mixed representations within an
typed snapshot job. A malformed replayed message must produce a failed job through
the existing ACK failure mechanism rather than retry forever or proceed
without its deletes. Input errors do not authorize fallback between contracts.

### 10.4 DataNode Reader

Extend the existing `NewStorageV3ManifestReader` and its binlog reader; do not
introduce another complete reader or route the request through `L0ImportTask`.
Keep its baseline call contract and behavior when no descriptor is present.
The new source context is supplied for L0-bearing and external-storage jobs;
legacy readers do not acquire a source commit timestamp from the target schema
or job options. With that context, the reader:

1. Accepts the exact manifest, source commit timestamp, and L0 references from
   `ImportFile.snapshot_source`.
2. Opens the manifest-backed milvus-storage reader with TEXT LOB resolution.
3. Reads LOB ownership and byte accounting from that manifest.
4. Resolves its own deltalog paths through the shared manifest helper.
5. Reads its own deltalogs and the attached L0 objects through the existing
   `readDelete`/`readDeleteV3` helpers and `storage.NewDeltalogReader`. Merge
   eligible deletes into one `PK -> maximum delete timestamp` map.
6. Applies that map using source effective timestamps as defined in section 12.
7. Returns logical rows to the unchanged Import row pipeline.

Reader-owned Arrow records remain borrowed until the next read or reader close.
Callers must not release them independently.

The existing early return when a data manifest has no deltalogs must not skip
attached L0 inputs. Install one delete filter after merging both sources.
Clone retained VarChar keys from borrowed Arrow buffers, check cancellation
while loading deletes, and close data/delete readers on every failure path.
Reuse the legacy V1/V2 fallback only for the legacy list; manifest-selected
packed files retain the existing direct V3 delta path. Refactor map insertion
into a shared, budgeted merge operation so the helpers do not retain a second
unbounded copy of all decoded deletes. Preserve terminal read errors; fallback
failure must not become an empty map. Broader changes to legacy decoder error
classification are not part of this extension. Shared helpers must retain
their baseline defaults when the new context is absent; do not enable the new
filter semantics or map limit for ordinary backup or no-L0 snapshot jobs
without `external_spec`. External no-L0 descriptors keep a zero source commit
timestamp, preserving raw timestamp comparisons while using the bounded map.

### 10.5 Target Writer

Source storage version does not choose the output format. DataCoord continues
to select the target segment storage version from the target cluster
configuration through the existing `importStorageVersion` logic.

Consequently, a StorageV3 snapshot source can be decoded and written using the
target's configured Import output path without exposing a target format option
to the caller.

### 10.6 Resource Admission

This section applies to all descriptor-bearing snapshot files: L0-bearing
jobs and jobs using `external_spec`, including those without L0. No-L0
snapshot jobs without `external_spec` and all other Import kinds retain their
existing reader and allocation behavior.

The first version deliberately uses per-data-source delete maps. It may read a
shared L0 object multiple times across segments and across the two phases.
There is no shared cache, Bloom-filter routing service, temporary manifest, or
compaction job in this design. For N data sources sharing D delete records,
the two phases can process approximately `2 * N * D` records before retries.
This cost is accepted only within bounded admission; the first version does
not claim to eliminate repeated I/O or support arbitrary source sizes.

Unbounded channel-wide maps are not acceptable. Reuse
`dataNode.import.readDeleteBufferSizeInMB` as the accounted delete-map budget
for the new typed snapshot reader, and document this additional meaning.
Account unique PK storage, owned VarChar bytes, timestamps, and a conservative
map overhead before admitting a new key. Replacing an existing timestamp does
not charge another key. Charge the combined segment/L0 map, not each input
file separately. Exceeding the budget fails explicitly; never drop deletes or
continue with a partial map. This is an accounting bound, not a strict RSS cap
for Arrow, decoder buffers, or the Go allocator.

Both PreImport and Import reserve the row buffer plus the delete-map budget
before opening a typed reader and release the reservation after closing it.
The baseline only reserves the Import row buffer and does not reserve this
memory during PreImport. Add a context-aware admission method to the existing
allocator for this path: cancellation must wake waiters, and a request larger
than the total allowance must fail instead of waiting forever. Do not acquire
the delete reservation incrementally while holding a row reservation. Close
readers and release reservations on success, initialization failure, read
failure, and cancellation. Existing allocation callers, including no-L0
snapshot jobs without `external_spec`, are not migrated in this change. A
runtime budget change may cause a later phase to fail admission; it must never change which rows a
successful reader emits.

`TotalMemorySize` and hashed row statistics continue to describe surviving
output data. Delete-map reservations are separate; adding them to output
statistics would distort segment sizing and disk quota checks. Input-size
accounting may include the exact delete objects read by each file, but must not
claim this repeated-read count is the snapshot's unique physical size.

The expanded message can grow with data-segment count times applicable L0
files. Keep the existing data-file count limit and account descriptor growth
incrementally while building an L0 source plan, so excessive expansion is
rejected before constructing an unbounded message. Validate the final encoded
WAL message, including header/properties and encoding overhead, before calling
Broadcast, not after durable broadcast-task creation. Use the active backend's
message-size admission where available; a portable explicit snapshot-plan
limit is required otherwise. Catalog persistence and task RPC envelopes also
need to fit within their supported limits.

The implementation caps the expanded source inventory at **256 KiB**, with
incremental per-reference accounting and a final protobuf-size check. The
complete message's encoded payload/properties are capped at **512 KiB** before
Broadcast. The shared WAL selector resolves `mq.type=default`; Pulsar and
Kafka additionally use half their configured message-size limit when that is
smaller. The factor leaves room for transport framing and later message
properties. Rocksmq and Woodpecker use the portable ceiling. These are explicit
first-version admission bounds, not advertised maximum sizes of the backends.
Do not use `pulsar.maxMessageSize` as a universal limit or silently split the
plan into independently visible Import jobs.

Oversized plans fail before broadcast; delete-map exhaustion fails the affected
task/job without accepting a partial map. Neither limit is a reason to omit
L0, downgrade to the baseline reader, or inflate output quota statistics. The
numeric bounds leave headroom under the default catalog and task RPC limits,
but have not been exercised against every deployed backend or customized
catalog/RPC limit. That transport matrix remains an acceptance gate, not a
claimed result of the local message-size unit tests.

## 11. Schema Compatibility

Snapshot-source Import uses the target collection schema as the output schema,
but the first version requires storage identity for fields read from the
StorageV3 manifest.

The following must match between the snapshot schema and target schema:

- field ID and data type;
- primary-key field ID, type, and AutoID setting;
- partition-key and clustering-key markers;
- vector dimensions and element types;
- array/StructArray nesting and element definitions;
- dynamic-field identity; and
- function output field IDs and types.

Collection name, database name, shard count, consistency level, collection
properties, and index definitions do not need to match because Import does not
restore them.

Target-only nullable or default-valued fields and source-only fields are not
supported in the first version. Supporting schema evolution requires an
explicit source-to-target field projection and is a follow-up.

## 12. Delete Semantics

### 12.1 Fold Before Target Routing

The following effective-timestamp rule applies to descriptor-bearing files in
L0-bearing snapshot jobs. No-L0 snapshot jobs keep the baseline raw-timestamp
delete comparison, including when new snapshot metadata contains a nonzero
source commit timestamp. Correcting that separate baseline limitation is not
silently included in this extension.

For each selected data segment in an activated job, merge eligible deletes
from its own manifest and all attached L0 objects. For each PK keep the largest
delete timestamp **after** applying the requested delete timestamp range. A
duplicate record or an overlap between segment-local and L0 deletes is
therefore idempotent.

`source_commit_timestamp` is segment-level. A nonzero value requires every
stored row in that segment to satisfy `raw_row_ts <= source_commit_timestamp`.
Zero means no commit-time override, not an upper bound of zero on row times.
Use the existing `tsoutil.EffectiveTimestamp` without changing its semantics:

```text
source_row_ts = EffectiveTimestamp(raw_row_ts, source_commit_timestamp)
             = max(raw_row_ts, source_commit_timestamp)

drop row iff latest_eligible_delete_ts[pk] > source_row_ts
```

For valid nonzero commit timestamps, this is equivalent to the commit-time
override used by segcore. It does not define a second supported interpretation
of invalid source timestamps. The activated reader checks each original row
as it is decoded, before timestamp-range or delete filtering:
`raw_row_ts > source_commit_timestamp` with a nonzero commit time
fails with `ErrDataIntegrity`. Do not clamp timestamps, drop the offending row,
or continue reading after the error. Both PreImport and Import use this same
reader path; no additional file scan is needed. Legacy/no-L0 readers retain
their baseline behavior.

The check intentionally does not rely only on catalog binlog timestamp bounds.
The existing V3 commit-fence gap (binlog arrays absent after metadata reload)
is a separate fix, not part of this L0 extension. Fixing that producer-side
check would not repair already-created invalid snapshots.

The comparison is strict. Equal timestamps must preserve the new row of an
upsert. Keeping only the maximum eligible delete is sufficient because the
predicate is monotonic in the delete timestamp; membership in a PK set is not
sufficient because it would also delete later reinserts.

| Raw row TS | Source commit TS | Delete TS | Result |
|---|---|---|---|
| 100 | 0 | 200 | Drop old row |
| 200 | 0 | 200 | Keep equal-timestamp upsert row |
| 300 | 0 | 200 | Keep later reinsert |
| 100 | 300 | 200 | Keep row made visible by source commit at 300 |
| 100 | 300 | 400 | Drop row deleted after source commit |
| 400 | 300 | Any | Reject invalid source before filtering |

Only surviving source rows reach target hashing/writing. The target Import's
new commit timestamp is used by the existing publication protocol, never as
an input to source delete filtering. Source deletes cannot affect pre-existing
target rows, even when those rows have the same PK.

### 12.2 Preserve Source Timestamp Provenance

There is a prerequisite in the current snapshot producer:
`SegmentDescription.commit_timestamp` and its Avro serialization already
exist, but `handler.GenSnapshot` does not copy
`SegmentInfo.GetCommitTimestamp()` into the description. This is a pre-existing
producer gap, not a reason to assume every zero means ordinary inserted data.

The minimal safe change is:

1. Copy source commit timestamps in `GenSnapshot` for every captured segment.
2. Add `bool segment_commit_timestamps_preserved = 14` to internal
   `SnapshotInfo`. A newly generated snapshot sets it only after copying all
   segment descriptions correctly.
3. Carry the marker through snapshot metadata writing/reading, export, and
   relocation. Exporting an old snapshot must preserve `false`; it must not
   fabricate provenance or upgrade the marker using the current catalog.
4. Require the marker when the selected data sources have applicable L0
   segments. Otherwise reject with an instruction to generate a new snapshot.
5. Persist each selected data segment's commit timestamp in its Import source
   descriptor; DataNode never looks up the live source segment.

The marker establishes that a zero was intentionally captured. It is not a
cryptographic integrity guarantee for caller-supplied metadata. The existing
Avro commit-timestamp field does not need another schema version; the new
metadata capability distinguishes reliable captures from older omissions.
An older tool that drops the marker produces an unsupported L0 source, not a
source that can be imported while ignoring the uncertainty.

Both old and new snapshots without applicable L0 retain the baseline Import
compatibility path and its timestamp-provenance limitation; this revision does
not claim to repair missing information in existing snapshots. Deliver the
producer correction as an independently testable prerequisite, with snapshot,
export, and ordinary restore regressions: other snapshot consumers will now
receive correctly populated source commit timestamps. Do not enable the
separate restore-L0 configuration as part of this work.

### 12.3 Timestamp Range and Snapshot Boundary

Keep the baseline backup Import range contract: insert rows are selected by
their raw stored timestamp in `[start_ts, end_ts]`, and deletes are selected by
their recorded timestamp in the same inclusive range. The effective timestamp
above is for delete ordering; this revision does not redefine row-range
selection as effective-timestamp selection. Changing that API meaning requires
a separate compatibility decision.

Do not invent a `delete_ts <= snapshot.create_ts` filter. `create_ts` is a
compatibility summary equal to the minimum captured channel seek timestamp,
not a global cross-channel row boundary. Channel seek positions determine
which segments are captured; they have not been established as a new universal
per-row cutoff for Import. This design consumes the snapshot's immutable file
inventory, matched by source channel/partition, under the existing Import
timestamp range. It does not add global as-of snapshot semantics.

### 12.4 Empty, Missing, and Fully Deleted Inputs

- A valid V3 zero-entry marker contributes no delete object. Keep the existing
  manifest helper's checks for inconsistent positive counts and missing paths.
- Do not generalize this rule to legacy L0 `EntriesNum == 0`: legacy metadata
  can omit a count while still referencing a nonempty delete file. Read every
  explicit legacy delete path regardless of an absent/zero count.
- A malformed manifest, missing required object, unsupported decoder, or
  corrupt delete record fails the job. I/O failures must retain their cause and
  existing retry classification; they are not empty input.
- No own-manifest deletes does not imply no deletes: L0 folding still runs.
- When L0 removes every source row, PreImport reports zero surviving rows. The
  existing zero-row auto-commit/manual-commit paths complete without creating
  a target L0 or writing a placeholder data row. Manual commit and abort retain
  their existing state transitions.

## 13. LOB Semantics

StorageV3 TEXT reference columns are not exposed to the Go Import pipeline.
The shared milvus-storage reader resolves them to logical Arrow UTF8 values.

LOB discovery and size accounting come from the exact segment manifest. Import
must not enumerate the partition-level `lobs` directory because multiple
segments share that directory and each manifest owns only a subset of its
files.

The target writer treats resolved TEXT as ordinary logical input and creates
new target-owned inline or LOB storage according to the target writer's
configuration.

## 14. Consistency and Retry Invariants

1. Snapshot expansion fixes the job's execution contract. Baseline jobs keep
   exact manifest paths; L0-bearing jobs keep per-file descriptors containing
   exact data manifests, L0 objects, and source commit timestamps.
2. PreImport and Import use the same persisted representation and filtering
   implementation; pre-import row counts describe exactly the surviving input
   to Import. Regrouping never changes execution contracts.
3. DataCoord never substitutes `ManifestLatest` during retry.
4. Referenced source files remain immutable for the job lifetime.
5. Self-contained paths stay under the validated bundle root.
6. Import retries may repeat reads but must not publish duplicate committed
   segments.
7. Existing manual commit and abort behavior remains unchanged.
8. A source error does not create or mutate collection, partition, or index
   definitions.
9. For L0-bearing jobs, WAL replay, CDC transport, file regrouping, and task
   retries must preserve the descriptor, including files with empty delete
   lists. No consumer may interpret a missing field as permission to omit L0.
10. Source timestamps and source file references are not rewritten to target
    IDs/timestamps by CDC. Existing target-routing rewrites remain separate.
11. The delete map is private to a source reader and is released with it. Neither
    source reads nor abort/retry deletes or rewrites snapshot objects.

## 15. Security and CMEK Boundary

Snapshot paths are validated through the existing snapshot path validators.
Manifest, data, delta, and LOB references must stay within the source root
defined by the snapshot layout. Logs and user-visible errors must redact
credentials and signed URL query strings.

Without `external_spec`, source reads use the configured target Milvus storage;
the complete metadata URI must match that storage identity before reading.
With it, source reads use the request-scoped foreign storage; target writes
always use the target instance configuration. Ordinary file and legacy backup
imports reject this option. External jobs use source descriptor version 2 and
empty legacy paths, so older workers fail closed rather than reading target
objects with identical keys. Provider/endpoint admission remains the existing
snapshot policy; no destination-side copier is constructed for Import.

Raw `external_spec` and `ezk` values must not appear in options logs. Credentials
are persisted with job options to support both phases and retries, and external
source options are removed from the retained job at Completed/Failed. Historical
WAL/broadcast records follow their normal retention: terminal cleanup is not
secure erasure of previously persisted credentials. Deployments must protect
these stores. Source IAM/static credentials must remain usable for the job's
lifetime; automatic renewal of caller-supplied credentials is not added.

For a CMEK-protected source, the caller supplies the source database's existing
backup `ezk`. DataCoord validates that the EZK's encryption-zone ID matches the
source schema before opening any segment manifest. PreImport and Import keep
the same option and read plugin context, while the target writer independently
uses the target collection's encryption context. This separation is the
required contract for plaintext/encrypted source and target combinations and
does not require the source and target databases to share an encryption zone.

For schemas without TEXT, snapshot Import reuses the existing StorageV3
`PackedReader`, which already accepts the source storage plugin context. A nil
source context is explicit and must not fall back to the target schema's CMEK
properties.

TEXT is physically stored as inline or out-of-line LOB references and must be
resolved through the shared milvus-storage `SegmentReader`. Its current C API
cannot receive Milvus's source key-retriever context. Therefore CMEK+TEXT/LOB
is rejected before the first manifest read. Supporting it later must extend the
shared reader API; it must not introduce a second Import-only reader.

L0 support does not expand the CMEK contract or reintroduce a positive CMEK
E2E case. Delete readers must use the existing supported physical format and
source context, never infer encryption from the target database. An encrypted
delete representation unsupported by the shared reader is rejected rather
than introducing a custom decryptor. Cross-database CMEK runtime behavior is
not claimed as verified by this design or its planned plaintext L0 tests.

## 16. Compatibility

- Ordinary JSON, CSV, Parquet, and Numpy Import are unchanged.
- Existing StorageV1 and StorageV2 path-based backup Import are unchanged.
- Legacy `l0_import=true` retains its own behavior and is not invoked by
  snapshot-source Import.
- New no-L0 snapshot jobs without `external_spec` and already-persisted baseline
  snapshot jobs retain the one-manifest representation, baseline reader semantics, and existing
  admission. No snapshot age or provenance marker forces their migration.
- Import does not invoke `RestoreSnapshot`, `RestoreExternalSnapshot`, or
  CopySegment. Snapshot capture/export additionally preserve the timestamp
  capability and L0 object inventory required above; restore-L0 remains a
  separate feature with its existing enablement rules.
- No public protobuf field or enum is added.
- `source_type=snapshot` is opt-in.
- Internal protobuf additions are append-only: `ImportFile.snapshot_source`,
  the Import WAL header descriptors, and the snapshot timestamp capability.
  Regenerate Go protobufs with `make generated-proto-without-cpp`; do not edit
  generated files manually.
- Upgrade DataCoord, DataNode, and participating CDC consumers before submitting
  L0-bearing or `external_spec` snapshot jobs. Unknown-field retention is not a
  capability negotiation. Mixed-version execution/downgrade of these jobs must fail
  closed, but uninterrupted completion on older nodes is not guaranteed.
  This extension does not impose the new descriptor requirement on no-L0
  snapshot jobs without `external_spec` or change their existing version
  compatibility. External jobs always require version 2 support, empty legacy
  paths and the typed-reader resource budget, even when no L0 is present.
- Because StorageV3 snapshot-source Import has not shipped, the existing
  `backup=true, storage_version=3` proposal has no compatibility guarantee and
  is replaced by this design.

## 17. Alternatives Considered

### 17.1 Continue Accepting a Partition Prefix and `storage_version=3`

Rejected. It makes the caller describe internal storage, enumerates sibling
directories, loses the exact snapshot manifest version, and duplicates
information already present in snapshot metadata.

### 17.2 Route Import to RestoreSnapshot

Rejected. RestoreSnapshot creates a collection, recreates partitions and
indexes, and copies physical segments. Those behaviors violate the existing
Import contract for an already-created target collection and partition.

### 17.3 Reuse CopySegment for an Existing Collection

Rejected for the first version. Direct segment copy requires strict physical
schema identity, new metadata-registration rules, partition/channel remapping,
and integration with Import commit/abort and CDC. It also prevents the target
writer from selecting its configured output format.

### 17.4 Add Snapshot Fields to the Public Import Protobuf

Rejected for the first version. The existing file carrier and key-value
options can express one metadata source without a public wire change. Internal
WAL/task descriptors are still required to preserve its expanded inputs. A
typed public source union should be considered only when additional source
kinds require stable SDK-level contracts.

### 17.5 Reuse Legacy `l0_import=true`

Rejected. That path writes target L0 deletes, whereas this operation filters
source rows. Source delete timestamps cannot be compared with newly assigned
target Import commit timestamps. Rewriting every delete to a later target
timestamp would also erase source reinserts and could affect pre-existing
target rows. Reuse `NewDeltalogReader`, not `L0ImportTask` or its timestamp and
publication semantics.

### 17.6 Compact the Source Before Import

Rejected as a prerequisite. It mutates or waits on the source collection and
does not work for an independent exported bundle. Import-side intermediate
compaction would require new temporary-object ownership, cleanup, retry, and
commit protocols. Read-time filtering provides the required logical result
inside both existing Import phases.

### 17.7 Encode L0 Paths in Options or a JSON-Shaped File Path

Rejected. L0 scope varies per data segment. Job-global options cannot identify
those associations after grouping without another protocol. JSON in a path
adds content-shaped dispatch and weak validation. A usable legacy data path
beside new optional metadata is particularly unsafe: an older reader can
ignore that metadata and succeed without applying L0 deletes.

## 18. Verification Plan

### 18.1 Unit Tests

- Parse and validate `source_type=snapshot` option combinations.
- Require exactly one complete metadata URI, reject bare metadata keys and
  `storage_version`, and reject URI/instance identity mismatches without extfs
  before metadata IO.
- Read both referenced and self-contained snapshot layouts.
- Rebase a relocated self-contained bundle before segment expansion.
- Expand all source partitions without an extra selector; verify partition-local
  L0 isolation and collection-wide L0 matching within the source channel.
- Produce exact per-segment manifest paths without using `ManifestLatest`.
- Reject schema mismatch, non-StorageV3 data segments, external
  collections, missing manifests, path escapes, invalid/mismatched EZKs, and
  CMEK sources containing TEXT/LOB fields.
- Accept matching source EZKs for non-TEXT schemas and keep plaintext-source
  reads from falling back to target CMEK properties.
- Resolve inline and out-of-line TEXT values.
- Handle absent, zero-entry, readable, corrupt, and missing deltalogs.
- Preserve borrowed Arrow record ownership.
- Keep target storage-version selection independent from source metadata.

Additional L0 tests must exercise the real functions, mocking only their
dependencies with mockey:

- Verify the routing boundary for ordinary Import, V1/V2 backup, legacy L0
  Import, old/new no-L0 snapshots, and L0-bearing snapshots. No-L0 jobs without
  `external_spec` retain one manifest per file, an empty descriptor header, baseline delete filtering,
  and existing resource admission even if metadata has a nonzero commit TS.
- External no-L0 jobs must retain version 2 descriptors, zero source commit
  timestamps, and bounded admission/map accounting. Cover segment-local deletes
  exceeding the budget and rejection by workers without version 2 support.
- Select same-channel/same-partition and same-channel `AllPartitionsID` L0
  segments; exclude other channels/partitions and reject missing identities.
- Include L0 only in an unrelated channel/partition: it must not activate
  folding for the selected sources. Conversely, malformed applicable L0 must
  fail rather than be misclassified as no-L0 input.
- Read V1/V2/V3 L0 storage without enabling V1/V2 snapshot data Import. Include
  V2 segment metadata whose deltalog was written by the legacy V1 writer.
- Capture regular zero and nonzero import/CDC source commit timestamps; carry
  the provenance marker through metadata and both layouts. Re-exporting an old
  snapshot must not add the marker. Applicable L0 without provenance fails.
- Serialize Import header/body, attach descriptors in ACK order, assign file
  IDs, persist/reload, regroup files, and construct both task types. Test
  mismatched cardinality, unknown versions, and both old ACK and old DataNode
  consumers dropping the new fields. Lost context must never succeed.
- Regroup an activated job so one task contains only files with empty L0
  lists. It must retain descriptors, source effective timestamps, and new-path
  admission. Test the same behavior with valid empty markers or all delete
  timestamps outside the requested range; none authorizes a baseline fallback.
- Verify CDC message handling preserves descriptors and source timestamps
  while remapping target routing. A protobuf round trip alone is insufficient.
- Cover the timestamp table in section 12.1 for Int64 and VarChar PKs, multiple
  deletes per PK, segment/L0 overlap, and time-range boundaries. Verify range
  filtering occurs before taking the maximum delete timestamp.
- Apply L0 when the data manifest has no deltalogs. Read legacy zero-count
  objects that contain records. Reject missing/corrupt objects and malformed
  positive-entry V3 metadata instead of treating them as empty.
- Check delete-map budget accounting, duplicate-key replacement, owned string
  lifetime, cancellation during decode and admission, impossible reservations,
  and reader/map/reservation cleanup after every error.
- Verify PreImport surviving-row and hashed statistics against actual Import
  rows, including zero-row auto/manual commit and abort. Delete-map accounting
  must not inflate output disk quota or row size.
- Exercise oversized expanded plans against the selected admission bound and
  supported WAL backends, catalog persistence, and task RPCs. Verify early
  growth rejection and final encoded-size rejection occur before Broadcast;
  no truncated or partially broadcast input is valid.

### 18.2 Integration Tests

Keep the baseline Go SDK E2E cases as no-L0 regression coverage; do not convert
them into L0 tests. They exercise the following scenarios through the real
Import lifecycle. This lists test intent, not a claim that the current working
tree reran the service-level tests:

| Layout | TEXT | Commit mode | Isolation check |
|---|---|---|---|
| Referenced | LOB | Manual commit | Exact snapshot metadata and manifest paths |
| Self-contained, relocated | LOB | Manual commit | Original export prefix removed before Import |

Both cases verify logical rows, TEXT payload equality, manual-commit visibility,
job progress, and cleanup. Unit tests cover manifest-selected deltalogs,
zero-entry markers, malformed deltalog metadata, and source/target storage
version independence. Auto-commit and timestamp-range failure scenarios remain
part of the follow-up failure-injection matrix below rather than being claimed
as first-version E2E coverage.

The Go SDK E2E follows the ordinary Import lifecycle: create source and target
collections, create a referenced snapshot, optionally export and relocate its
self-contained bundle, pass the resulting metadata URI to Import, and verify
manual-commit visibility and LOB equality. Collection, snapshot, export, and
query operations use the typed `milvusclient` APIs. Import job operations use
the Go SDK's existing `client/bulkwriter` wrapper, which is the SDK's public
REST-backed Import surface; this feature does not add a second typed Import
API only for testing. The test does not deploy or restart Milvus, change server
configuration, drop the source collection, or wait for asynchronous GC. It is
part of the standard `tests/go_client` E2E suite instead of relying on an
undocumented L3-only job.

For the self-contained case, the E2E copies the complete exported bundle to a
new prefix and removes the original export prefix before Import starts. This
exercises bundle-root rebasing without coupling the test to source collection
GC timing. Deterministic `snapshotio` and DataCoord tests additionally verify
that self-contained metadata, manifests, data files, and LOB references are
closed under the bundle root.

Regression coverage must confirm that legacy V1/V2 backup Import and ordinary
file Import remain unchanged.

Add L0-specific cases alongside those baseline cases, following the same Go SDK
lifecycle with insert, flush, delete, and same-PK reinsert before snapshot
creation. For **both layouts**, inspect the captured snapshot and require at
least one applicable L0 with readable delete records.
Compare exact PK/value sets after Import, not just row counts. Verify removed
rows stay absent, later reinserts survive, and existing target rows sharing
deleted source PKs remain unaffected. The relocated bundle check must include
L0 manifests and deltalogs after removal of the original export prefix.

`TestImportStorageV3SnapshotSourceL0` implements this public lifecycle beside
the baseline test. It reuses the Go-only `snapshotio` metadata/Avro parser via
a test-module dependency on the repository root. Normal Delete/Flush currently
creates V1 L0 even for V3 data collections, so the SDK fixture explicitly
requires V1 L0 with no storage manifest. A test-only framing inspector follows
the binlog descriptor/event lengths and passes only the embedded Parquet
payloads to Arrow; it handles both the default JSON-record payload and the
multi-field PK/timestamp payload. Golden bytes produced by the real V1 writer
cover both formats and truncated/corrupt framing without a running server.
V3 L0 manifest/path resolution remains covered by server tests; the SDK helper
fails explicitly if the normal L0 producer changes format, rather than reading
pathless PB summaries. It does not copy the snapshot Avro schema, add a public
SDK storage-inspection API, or link the C++ server into the client test. Both
the original and relocated captures must contain an applicable L0 record for
the expected deleted PK. A missing active-L0 fixture fails with an explicit
preparation diagnostic; a no-L0 snapshot cannot count as success.

Producing active L0 is a fixture requirement, not a timing assumption.
Background L0 compaction may remove it before capture; a successful run on such
a snapshot does not verify folding. In the current implementation, disabling
collection auto-compaction alone does not establish that L0 compaction is
disabled. Do not make that claim or change server configuration inside an
ordinary Import E2E test.

Use real delta/data writers and the shared snapshot writer to build
deterministic referenced and relocated self-contained fixtures for the reader
and two-phase integration tests. Keep the public Go SDK scenario's preparation
bounded and report failure to establish active L0 separately from product
failure. If reliable active-L0 capture requires scheduler isolation, provide a
suite-owned environment configured before test startup; do not hide service
restarts, config edits, raw source-object deletion, or GC waits inside the case.
The public lifecycle and the deterministic fixture tests are complementary;
neither substitutes for the other's assertions.

The deterministic two-phase cases must also include data from an earlier
Import with a nonzero source commit timestamp, and zero surviving rows in
both auto-commit and manual-commit modes. Compare PreImport statistics with
actual surviving rows, verify pre-commit invisibility, and exercise retry and
abort without rereading snapshot metadata or changing source associations.

`TestImportStorageV3SnapshotSourcePlaintextRejectsEZK` covers rejection of
plaintext snapshots with an unexpected EZK in both referenced and self-contained
layouts. It creates an explicitly unencrypted, test-owned database and cleans
up its collections, snapshots and exported objects. No cipher plugin, KMS key
or management endpoint is required.

CMEK runtime E2E coverage is outside this change. The plaintext rejection case
does not establish encrypted source reads, encrypted target writes or
cross-database CMEK support.

### 18.3 Failure Injection

- Snapshot metadata disappears before expansion.
- A referenced manifest or LOB file disappears between PreImport and Import.
- Snapshot metadata points outside the allowed root.
- The exact manifest exists during PreImport but is missing during Import.
- DataNode restarts after reading a batch and before syncing target data.
- Manual commit is issued after one task has failed.
- An L0 object disappears between PreImport and Import.
- The delete map exceeds its budget or cancellation arrives while admission
  is waiting for memory.
- A restart/replay/regroup loses or attaches the wrong source descriptor.
- An old consumer drops the header or the internal file extension.
- An activated job's regrouped task has no local L0 references and incorrectly
  attempts to switch to baseline execution.

For each case trace origin, error propagation, task/job state, cleanup, and
manual-commit visibility. Preserve existing error causes and retry behavior;
do not classify all read failures as permanent malformed input.

### 18.4 Acceptance Gates

Before claiming the narrowed extension is ready:

1. Baseline routing and behavior remain unchanged for the non-activated cases
   in section 1.1; shared reader and allocator defaults have regression tests.
2. Source timestamp provenance, channel/partition matching, and strict delete
   ordering pass deterministic tests, including imported source segments and
   reinserts. Missing/corrupt input never becomes an empty delete set.
3. WAL/ACK, persistence, regrouping, restart/retry, and CDC preserve the same
   complete source input. Unknown or lost descriptors fail closed.
4. Delete-map and source-plan limits are explicit, cancelable where waiting is
   involved, and validated against actual transport/storage limits. Repeated
   I/O remains an acknowledged bounded cost, not a claimed optimization.
5. Both snapshot layouts have active-L0 evidence and two-phase lifecycle
   validation; the source capture fix separately passes snapshot/export/
   restore regressions. Report any unexecuted scenario instead of claiming it
   passed based on compilation or baseline E2E success.

## 19. Rollout Plan

The snapshot-source baseline already exists. Implement the L0 extension in
the following order without changing the public request shape:

1. Establish routing regression coverage, then fix source commit-timestamp
   capture and provenance as an independently testable prerequisite. Run
   snapshot/export/restore regressions; do not enable restore-L0 or migrate
   no-L0 Import jobs without `external_spec` as part of that fix.
2. Add internal source descriptors and the WAL header carrier for
   L0-bearing and external-storage jobs; regenerate protobufs and test
   ACK/replay/CDC preservation and old-consumer failure. Leave baseline
   message/file representations alone.
3. Extend DataCoord expansion with the one-time job-wide applicability decision,
   complete scoped inputs, path/provenance validation, and no-error-fallback
   rule. Resolve incremental and encoded plan admission before accepting L0
   input; do not activate an incomplete producer/consumer path.
4. Extend the existing reader only through the new source context. Reuse delta
   decoding and apply source effective timestamps, bounded map accounting, and
   cancelable admission in both phases. Keep legacy defaults, no-option no-L0
   admission, output sizing, and target commit/abort behavior unchanged.
5. Add deterministic failure tests and separate both-layout active-L0 Go SDK
   cases while retaining the baseline E2E cases. Run targeted tests/coverage in
   the designated worktree and service-dependent validation only on an owned
   environment.
6. Advertise L0 support only after section 18.4 and the end-to-end input and
   failure-path audit are complete. Record unrun CDC/backend/plugin scenarios
   explicitly; neither compilation nor a no-L0 happy-path E2E proves L0 support.

Expected change areas are `handler.go`, `import_snapshot.go`,
`ddl_callbacks_import.go`, internal protobufs, `snapshotio`, the Import reader
dispatch and binlog reader/filter, and the two task entry points' memory
admission. There is no new public SDK API, target L0 task, source compaction
policy, or alternative TEXT/CMEK reader. Except for the explicitly isolated
snapshot-producer prerequisite, new behavior is gated by typed source context
(applicable L0 or `external_spec`) rather than changing shared defaults.

The working tree implements steps 1 through 4 and adds the step-5 regression
and Go SDK cases. Local service-level scenarios listed in Implementation Status
have passed; the full transport/restore matrix and the changed-function
coverage gate remain required before step 6.

## 20. Follow-Ups

- Extend the typed descriptor to StorageV1/V2 data segments and mixed snapshots.
- Shared per-channel delete caching or external spill for sources exceeding
  the first version's per-reader memory budget.
- Schema-evolution projection between source and target collections.
- Arbitrary cross-provider snapshot reads beyond the current endpoint policy.
- CMEK key-retriever propagation through the shared milvus-storage
  `SegmentReader` for TEXT/LOB resolution.
- A typed SDK option that replaces raw `source_type` key-value handling if the
  source contract expands beyond snapshots.
