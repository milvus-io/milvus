# MEP: Snapshot-Sourced StorageV3 Backup Import

- **Created:** 2026-09-02
- **Updated:** 2026-09-21
- **Status:** Proposed
- **Feature DRI:** @weiliu1031
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Related Issue:** [milvus-io/milvus#52930](https://github.com/milvus-io/milvus/issues/52930)
- **Related PR:** [milvus-io/milvus#52932](https://github.com/milvus-io/milvus/pull/52932)

### Implementation Status

The implementation follows backup Import's physical FieldID semantics and
existing target routing/default-filling rules (sections 10 and 11), not
name-based field mapping. Create captures root metadata; bounded Pending
preparation atomically publishes a complete plan, and DataNode performs the
physical data/L0/LOB checks.

Each import phase reuses source clients within a task and folds bounded L0
batches into per-segment row bitmaps before reading output rows. Sections 10.3-10.6
define the descriptor, cancellation and memory-admission contracts. Captured
source commit timestamps govern delete visibility; historical snapshots with
omitted timestamps retain the explicit limitation in section 12.2.

Local reader package tests and targeted DataCoord/DataNode regressions have
passed during development, including race-enabled checks of shared loading,
reader isolation, cancellation and cleanup. Real V3 data, out-of-line TEXT and
V1/V2 deletes exercise row alignment and single-/multi-batch equivalence.
These are scoped local results, not whole-feature or service E2E validation;
detailed execution history remains in the development records.

The current revision has not established complete live E2E coverage, live CDC/
backend coverage, full restore regression, whole-product builds, repository
verifiers or throughput benchmarks. Earlier service E2E runs do not validate
subsequent schema, partition-mapping or batched-L0 changes. Changed-function
coverage has not established the repository-wide 99% gate. CMEK runtime E2E
remains out of scope. Section 18 defines the remaining acceptance gates.

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

This section is the authoritative execution/descriptor selection rule for new
snapshot Import jobs. Both layouts accept old snapshots with omitted commit
timestamps, subject to the historical timestamp limitation in section 12.2.
After asynchronous source preparation, DataCoord selects typed files
when **any** of the following holds:

- a selected data segment has an applicable captured L0 segment;
- any selected data segment has a nonzero source commit timestamp;
- the request supplies `external_spec`; or
- the request supplies `partition_mapping`.

Only an admitted job with none of these conditions keeps one exact manifest
in each file's `paths[0]`, with no descriptor. L0 applicability is defined by
source channel and partition scope in section 10.2, not by whether an L0 read
returns records or whether those records survive the requested timestamp range.

| Input | Execution contract |
|---|---|
| Ordinary file Import or legacy V1/V2 backup Import | Existing path, unchanged |
| Legacy `l0_import=true` outside snapshot-source Import | Existing delete-import path, unchanged |
| Snapshot satisfying none of the four activation conditions | Existing one-manifest-per-file path |
| Snapshot with any nonzero source commit timestamp | Job-wide descriptors preserve commit-time delete semantics, including without L0 |
| Snapshot with `external_spec` | Job-wide descriptors select source storage, including without L0 |
| Snapshot with `partition_mapping` | Job-wide descriptors bind files to target partitions, including without L0 |
| Snapshot with applicable L0 | New descriptors and bounded read-time folding using captured timestamps |
| Snapshot with omitted commit timestamps | Accepted with the same activation rules; omitted commit times remain zero, with the historical limitation in section 12.2 |
| Snapshot with invalid applicable L0 input | Explicit failure, never fallback to the baseline path |

Every newly admitted snapshot job first persists a coordinator-only version-9
descriptor containing the captured top-level metadata. It has no runnable
files. Pending preparation atomically replaces it with the following expanded
representation before either worker phase is scheduled. Existing expanded jobs
do not repeat preparation.

For every expanded typed job, select the descriptor version from the source-storage,
partition-mapping options, and whether applicable L0 requires a shared inventory:

| Source storage | Partition mapping | Without applicable L0 | Shared L0 inventory |
|---|---|---|---|
| Instance storage | Absent | 1 | 5 |
| Request-scoped external storage | Absent | 2 | 6 |
| Instance storage | Present | 3 | 7 |
| Request-scoped external storage | Present | 4 | 8 |

Versions 3/4/7/8 require a positive `target_partition_id`; the others leave it
unset. Versions 5-8 require source channel/partition. All versions prohibit
inline L0 lists, including persisted tasks from development versions. All versions
preserve the source manifest, applicable deletes, and
source commit timestamp. Unsupported versions fail closed rather than losing
source-storage, delete, or target-routing semantics.

Activation is **job-wide**, not reevaluated per task. Every data file in an
activated job carries a descriptor, including files with no individually
applicable L0. This avoids mixed representations in one expanded job and keeps
dispatch stable when files are regrouped. A task containing only such files
still uses the descriptor path. Captured commit timestamps alone determine
commit-time activation; there is no separate completeness flag.

Descriptor presence also selects the bounded delete map and cancelable memory
admission in both phases. Thus even a job activated only by external storage
or partition mapping reserves row-buffer plus delete-map memory, and its
segment-local deletes can exceed the configured
`dataNode.import.readDeleteBufferSizeInMB` budget (128 MiB by default). Such a
job fails explicitly; it does not fall back to the unbounded baseline reader.
This resource and mixed-version boundary applies to every typed job; legacy
file Import is unchanged.

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
- Within snapshot jobs, use each source data segment's effective
  row timestamp when evaluating deletes, including segments created by an
  earlier Import or CDC transaction.
- Preserve the baseline source representation, delete semantics, and resource
  admission for admitted snapshot jobs requiring no source context and all
  other Import kinds.
- Reuse the existing backup `ezk` contract to read CMEK-protected StorageV3
  snapshots whose schemas do not contain TEXT/LOB fields.
- Keep source and target storage formats independent.
- Preserve existing Import pre-import sizing, scheduling, row hashing,
  auto-commit, manual commit, abort, and CDC behavior.
- Reject missing required physical fields and decoding failures through the
  existing backup reader, not a source/target schema-equality check. Fail closed
  for unsupported snapshot contents, unsafe paths, invalid source encryption
  metadata, and unsupported CMEK+TEXT/LOB sources.

## 5. Non-Goals

- Creating the target collection or target partition.
- Restoring source collection properties, consistency level, indexes, index
  files, shard count, segment IDs, or channel assignments.
- Directly adopting or copying source segments into the target collection.
- Implicitly preserving source partition placement. Without `partition_mapping`,
  one Import request reads all data partitions and uses ordinary Import target
  routing. Explicit complete name mappings are supported for ordinary targets;
  target partitions must already exist, and no mapping is inferred from equal
  partition counts or matching names.
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
- Migrating already-persisted baseline jobs, changing legacy reader defaults,
  or refactoring all Import memory-allocation callers.

## 6. Why This Is Import, Not Restore

`RestoreSnapshot` and backup Import have different user-visible contracts.

| Behavior | Snapshot-sourced Import | RestoreSnapshot |
|---|---|---|
| Target collection | Must already exist | Must not exist |
| Schema authority | Target collection | Snapshot |
| Target partition | Existing partitions selected by default/name, explicit mapping, or target partition-key hashing | Partitions recreated from snapshot |
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
   The explicit `partition_mapping` mode described below overrides only target
   partition selection for ordinary collections.
5. All source data partitions in the snapshot are imported. Source partition
   IDs identify files for explicit mapping and, together with source channels,
   define L0 matching scope; target placement never changes that source scope.
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
and storage config. Each PreImport or Import task execution lazily resolves the
same URI and spec once, without rereading snapshot metadata. Its reader factory
shares the resulting source client and immutable config between the shared L0
loader and all segment readers, including tasks without L0. The factory also
resolves the source EZK/plugin context once per execution for both same-bucket
and external snapshots. Record readers consume these explicit dependencies;
they do not parse EZK or construct source clients. An explicit nil plugin
context must not fall back to target CMEK properties. Source encryption is
tracked separately from that context, since a disabled plugin can return nil
even for an encrypted source. The projected-schema CMEK/TEXT restriction is
unchanged; ordinary backup retains its existing key-resolution path.
Concurrent initialization uses the task context, never a file's
context. Resolution errors are retained for that execution; a retry or another
phase creates a fresh factory. There is no cross-task or global client cache.
Each reader still owns its stream, decoder, read context and Close operation.
Original options and descriptor validation remain intact. The task's target
chunk manager/storage config is never replaced. Failed source access never
falls back to target storage. This does not change commit/abort semantics.

There is no source-partition selector. A new request carrying the earlier
`source_partition_name` option is rejected rather than silently broadening its
scope. Already-expanded persisted tasks keep their original file inventory and
do not pass through public request admission or snapshot expansion again.

### Explicit partition mapping

`options.partition_mapping` is an optional JSON **string** mapping source names
to existing target names, e.g. `"{\"A\":\"X\",\"B\":\"Y\",\"C\":\"Z\"}"`.
It is snapshot-only and mutually exclusive with `partition_name` and a
partition-key target. The mapping must cover every partition in the snapshot
metadata (including empty ones); unknown or omitted source names, duplicate JSON
keys, empty names, malformed/non-string values and nonexistent target partitions
fail admission. The encoded option is bounded to 64 KiB. Explicit many-to-one
mappings are allowed. Absence keeps the existing behavior unchanged.

Proxy resolves the unique target names in sorted order to the job's destination
IDs. DataCoord validates complete source coverage and binds each expanded data
file to one target ID. After snapshot I/O, DataCoord rechecks the name/ID pairs
under the collection broadcast lock: drop/recreate races fail retriably, never
redirecting prepared files to a replacement partition. Source partition/channel
IDs remain unchanged throughout L0 matching, including collection-wide deletes.

The WAL/ACK carries captured source partitions, mapping options and the ordered
target partition IDs. Pending preparation binds each file's destination in
`SnapshotImportSource.target_partition_id`, persisted with catalog files and
file statistics. Section 1.1
defines the mapped descriptor versions and required fields. Older consumers
reject the newer version instead of ignoring routing. Lost mapping options,
destination IDs or external storage context fail closed. CDC transport preserves
the descriptor along with the existing replicated target partition IDs; no new
name lookup occurs on replay.

Both task constructors split file groups by destination without increasing their
existing size/count limits. Each mapped task receives a single target partition,
and memory sizing uses that task's partition count. PreImport statistics and Import
segment allocation therefore refer to the same destination. DataNode validates
the file/task binding before opening any reader and reuses the existing single-
partition row-routing path. All tasks stay in one Import job and retain its
auto/manual commit, abort and retry behavior; no per-partition child jobs are added.

Tests cover parsing/admission, both snapshot layouts, L0 source scope, external
source descriptors, WAL/CDC descriptor preservation, catalog round trips, both
task constructors, destination segment allocation and worker mismatch rejection.
The Go SDK E2E `TestImportStorageV3SnapshotSourcePartitionMapping` imports three
source partitions into three permuted targets in one manually committed job and
checks exact PK sets per target, for referenced and self-contained snapshots.
Test presence is not a claim that the live E2E or CDC environment has been run.

There is no new public L0 toggle. After validation, DataCoord selects the
baseline or typed execution contract according to section 1.1. A caller
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

V3 L0 manifests are persisted as exact versioned references. Each DataNode task
resolves them once during bounded L0 batch processing, before opening output
readers. V1/V2 L0 delete paths come from the snapshot's explicit deltalog
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
  - resolve default/name, partition mapping, or partition-key destinations
          |
          v
DataCoord Create: capture top-level metadata only
  - validate URI, layout, schema, encryption and partition mapping
  - WAL -> ACK -> persist version-9 Pending descriptor
          |
          v
DataCoord bounded background Pending preparation
  - read segment metadata from captured references, never reread the root object
  - validate layout, paths, schema presence, source encryption, and segment types
  - select StorageV3 L1/L2 data segments
  - match captured L0 inputs by source channel and partition scope
  - preserve captured commit timestamps and bind explicit partition mappings
          |
          v
Typed activation under section 1.1?
  L0 OR nonzero commit TS OR external_spec OR partition_mapping
          |
    +-----+---------------------------+
    | No                              | Yes
    v                                 v
Baseline manifest files       Versioned source descriptors
    |                                 |
    +---------------------------------+
                      |
                      v
          Atomically persist complete ImportFiles and shared L0 inventory
          Split mapped file groups by target partition
                      |
    +---------------------------------+
    |                                 |
Baseline PreImport            Bounded typed PreImport
    |                         apply own/applicable L0 deletes
    |                         using source effective timestamps
    |                                 |
    +---------------------------------+
                      |
                      v
          Fill missing default partition key before hashing
          Count/hash surviving rows; regroup files
                      |
    +---------------------------------+
    |                                 |
Baseline Import               Same-input bounded typed Import
    |                                 |
    +---------------------------------+
                      |
                      v
          Fill target optional/dynamic fields; hash and write
          Existing commit/abort and index build
```

Both phases use the same persisted representation; regrouping does not choose
the baseline/typed contract again. Both readers select physical fields under
section 11. PreImport fills a missing default partition key only when routing
across multiple target partitions; single-destination tasks bypass that hash.
Admission errors reject the request; worker validation/read errors fail the
task/job. Neither permits fallback from typed to baseline execution. Both
branches preserve the target writer and publication protocol.

## 10. Component Changes

### 10.1 Proxy

Proxy keeps the existing Import entry point and target collection validation.
It adds option validation for the snapshot-source contract:

- require backup mode;
- require one file entry containing one snapshot path;
- reject `storage_version`, `l0_import`, and unsupported option combinations;
- reuse ordinary Import target partition selection, including default partition
  selection and partition-key routing, without creating partitions;
- for `partition_mapping`, resolve sorted unique target names to existing IDs,
  rejecting a simultaneous partition name or a partition-key target; and
- forward the request through the normal Import broadcast path.

Proxy does not read snapshot objects. DataCoord enforces complete URI and
storage-identity validation before metadata I/O. Explicit mapping adds a
target-resolution branch to the existing Import API; automatic L0 folding adds
no public toggle.

### 10.2 DataCoord

DataCoord extends the existing snapshot expansion without introducing another
public source kind:

1. Require a complete metadata URI and validate its storage identity. Resolve
   source storage using `external_spec` when supplied; otherwise reuse the
   instance ChunkManager. Open metadata through that source reader.
2. Call `SnapshotReader.ReadMetadata` once. Validate the metadata with
   `ReadSnapshotFromMetadata(..., false)` without reading segment metadata or
   physical data/L0 manifests, listing fragments/LOBs, or checking object existence.
3. Let the reader validate snapshot format compatibility and normalize the
   referenced or self-contained layout.
4. Validate snapshot metadata location. Accept omitted commit timestamps without
   probing whether data manifests contain deletes or inferring missing times.
   Retain the shared snapshot path validators.
5. Validate schema presence, source/target collection eligibility, and source
   encryption. Do not compare source and target schemas for equality; worker
   readers validate physical columns as described in section 11.
6. Validate complete mapping coverage when requested. Capture the original
   top-level metadata (excluding unused index/build metadata), source URI and
   layout in a coordinator-only version-9 descriptor and internal options.
   Recheck target name/ID bindings under the broadcast lock. Broadcast one
   empty-path body file paired with this descriptor; ACK persists the Pending
   job. The captured metadata is immutable input, not a request to reread the
   root object later. Source segment metadata must remain immutable/available under
   the existing snapshot storage contract.
7. The Pending checker starts bounded background preparation (at most four
   concurrent jobs, one attempt per job). The global checker loop performs no
   source I/O and continues scheduling ordinary jobs. Read segment metadata using
   `ReadSnapshotFromMetadata(..., true)` and collect data segments from every
   source partition in the captured snapshot.
8. Exclude L0 from the data-file list. Reject any data segments
   whose storage version is not StorageV3 or whose manifests are missing or
   non-exact.
9. Independently scan the snapshot's L0 inventory. For each data segment `s`,
   select an L0 segment `d` only when:

   ```text
   d.channel_name == s.channel_name
   AND (d.partition_id == s.partition_id
        OR d.partition_id == AllPartitionsID)
   ```

   Preserve each data segment's original partition/channel while matching
   collection-wide and partition-local L0. Do not use target partition IDs or
   target vchannels for matching.
10. Decide `hasApplicableL0` for the entire selected input.
   Select the job representation and descriptor version using section 1.1;
   L0 is only one of the activation conditions.
11. For applicable L0, retain exact V3 L0 manifest references or explicit V1/V2 deltalog inventories.
    Validate original URI bucket/endpoint and layout/root before normalizing
    legacy delete references to object keys. Do not open physical manifests in
    DataCoord. DataNode resolves the V3 L0 references once per task execution,
    validates every discovered path before normalizing it, and checks decoder
    conflicts. URI/key aliases identify one object; ChunkManager never receives
    a complete URI as an object key.
12. Sort data sources by source segment ID, deduplicate delete references per
    source channel/partition scope, and generate a descriptor for every file
    in a typed job. Store channel-wide L0 once under `AllPartitionsID`, not once
    per matching data segment or partition. Attach
    resolved target partition IDs after matching deletes in the source scope.
    Conflicting metadata for the same delete object is an error, not a reason
    to choose one arbitrarily. Check incremental source-plan growth and the
    final encoded size before publishing the expanded plan.
13. Allocate file IDs and atomically save files, normalized options and L0
    inventories together, replacing the Pending descriptor. Only a later
    checker tick creates PreImport tasks from that durable plan. Source errors
    fail the job; allocator/catalog failures retain the preparation input and
    retry. Cancellation, job timeout, terminal state and checker shutdown stop
    preparation; a late result cannot revive a failed job. Recovery restarts an
    unfinished preparation from captured input or reuses an already published
    plan. There is no partially expanded plan and no second WAL broadcast.

Determine applicability before skipping valid empty markers or filtering
delete timestamps. A missing identity needed to decide scope, a failed L0
read, or an unsupported applicable representation cannot mean
`hasApplicableL0=false`. Valid empty L0 markers may produce empty delete lists
in an activated job, but do not switch that job back to baseline execution.

Schema-presence, collection-eligibility and source-encryption validation must
precede opening any V3 manifest. Required-column checks and row decoding happen
in DataNode; a request can therefore be admitted and later fail its task for an
unreadable target field. This follows backup Import rather than promising
synchronous rejection of every schema mismatch.

DataNode validates manifest fragments, own deletes and projected TEXT/LOB paths
before constructing its row reader. LOB existence is checked even when the
manifest supplies a positive size. Unprojected TEXT/LOB and unused stats/index
files are not opened by Import. Missing channel identity or an unsupported
applicable L0 representation fails preparation; unrelated partitions' L0
inputs are not read.

Retries reuse the persisted input representation: exact manifests for baseline
jobs, or descriptors plus the shared L0 inventory for activated jobs. Neither
task regrouping nor a restart after plan publication repeats snapshot expansion
or the activation decision. A snapshot with no selected data segments remains invalid; L0-only
input is not an operation on target rows.

### 10.3 Import Source Representation

Add an internal, versioned source descriptor in `pkg/proto/internal.proto`.
Jobs with L0, nonzero source commit timestamps, external storage, or partition mapping populate it.
The fields are:

```protobuf
message SnapshotImportSource {
  uint32 version = 1; // 5-8 add shared L0 to the contracts of 1-4.
  string manifest_path = 2;
  uint64 source_commit_timestamp = 3;
  // Retained only to detect and reject development-version inline L0 tasks.
  // Both fields must be empty; L0 paths belong to SnapshotImportL0Source.
  repeated string legacy_l0_deltalogs = 4;
  repeated string manifest_l0_deltalogs = 5;
  int64 target_partition_id = 6; // Required for versions 3/4/7/8.
  string source_channel = 7; // Required for versions 5-8.
  int64 source_partition_id = 8;
  bytes snapshot_metadata = 9; // Version 9 only; never sent to DataNode.
}

message SnapshotImportL0Source {
  string source_channel = 1;
  int64 source_partition_id = 2;
  repeated string legacy_l0_deltalogs = 3;
  repeated string manifest_l0_paths = 4; // Exact versioned L0 manifests.
}

// Added to internal.ImportFile; existing fields 1-3 remain unchanged.
SnapshotImportSource snapshot_source = 4;
```

Only validated legacy delete paths and exact V3 L0 manifests enter the shared
inventory; valid empty markers are handled during expansion. Actual readability is established when
the tasks read the objects. Entry counts are validation hints at the manifest
boundary, not a substitute for reading the referenced delete records.
Versions 5-8 retain source channel/partition IDs in every file descriptor as
task-grouping and shared-inventory binding keys. Full L0 path lists are stored
once per source scope and merged per task, not duplicated in every descriptor.
The validated row-reader view clears these scope keys after task binding.

The two path lists describe the existing **delete reader contracts**, not the
source segment's storage version. In the current code, `BulkPackWriterV2`
inherits `BulkPackWriter.writeDelta`, which writes V1 deltalogs, while
`BulkPackWriterV3.writeDelta` writes packed V2 deltalogs recorded in a V3
manifest. Thus copying `SegmentDescription.storage_version` directly into
`NewDeltalogReader` is incorrect. The task-shared L0 loader uses
`DeleteMerger.Merge`: legacy inventories retain the existing V1/V2-compatible
decoding behavior, while manifest inventories use the packed V2 decoder.
Segment-local manifest deletes also use this helper via `readDeleteV3`.
No new file-format detector is introduced.

The WAL Import body uses external `msgpb.ImportFile`, not internal
`ImportFile`. Adding the internal field alone would lose it before job
persistence. To avoid a public `milvus-proto` dependency change, add
`repeated internal.SnapshotImportSource snapshot_sources = 1` to the existing
`messagespb.ImportMessageHeader`. L0 inventories are not carried by the WAL
header or ACK request: Pending preparation derives them from the captured
metadata and atomically persists them in `datapb.ImportJob.snapshot_l0_sources`,
one inventory per source scope. Explicit empty entries cover data scopes
without partition-local deletes. A missing
entry is an error, not permission to ignore L0. `messages.proto` already
imports `internal.proto`.

For a newly created snapshot job, the producer places one version-9 descriptor
in the header paired with one empty-path body file. The full metadata payload
is protobuf-encoded `SnapshotMetadata`; bytes avoid a proto dependency cycle.
Workers explicitly reject version 9. Existing versions 1-8 and expanded baseline
jobs remain executable, but cannot contain preparation bytes. File IDs are
allocated by the ACK callback, so pairing uses the original array index at this
boundary only; preparation allocates new IDs for the expanded files. The callback validates cardinality, source kind, descriptor
version, and absence of legacy paths, then copies each descriptor into the
same internal file whose ID it allocates. All subsequent persistence,
PreImport statistics, regrouping, Import tasks, and retries carry that internal
file together with the job inventory; they must never reconstruct files from paths alone or rejoin parallel
arrays. Broadcast and CDC must preserve header/body ordering until this binding.

For an expanded baseline job under section 1.1, each file retains its exact
manifest in `paths[0]`. Existing baseline WAL records and persisted jobs keep
their existing conversion/execution. New jobs select this representation only
after preparation. Descriptor presence in runnable files follows the complete
activation rule, not L0 presence alone.

Empty legacy `paths` are intentional. If an older consumer drops the new
header or protobuf field, the baseline snapshot reader rejects the resulting
zero-path file. Leaving a usable manifest in `paths[0]` would instead allow an
old DataNode to ignore L0 inputs and successfully import deleted rows. Keep
body-file cardinality nonzero so loss of the header cannot become an empty,
successful job. Compatibility tests must exercise both old ACK and old worker
behavior, not just unknown-field round trips.

For versions 5-8, both phases group files by source `(channel, partition)`
**before** applying their existing limits: PreImport uses file count, Import
uses estimated memory size. Target partition splitting still applies. A large
source scope may span multiple tasks, but a task never mixes source scopes.
DataCoord merges that scope's partition-local and channel-wide inventories into
one `snapshot_l0_source` on each PreImport/Import request, deduplicating paths.
Task persistence needs only the compact file descriptors and job ID; retries
reconstruct the common list from the persisted job, without reading snapshots
again. These are real source identities, not generated group IDs.

Within a typed job, even files with no applicable deletes retain their source
commit timestamp and typed execution contract. Reader and admission dispatch
use descriptor presence, not the number of delete paths. Versions 1-4 keep
their existing grouping only for tasks without inline L0 lists.

The reader accepts either a valid descriptor with no legacy paths, or the
baseline one-manifest representation for admitted jobs needing no source context
and already-persisted baseline jobs. Both at once, an unknown version, or neither are errors. Reject
descriptors outside snapshot backup mode and mixed representations within a
typed snapshot job. A malformed replayed message must produce a failed job through
the existing ACK failure mechanism rather than retry forever or proceed
without its deletes. Input errors do not authorize fallback between contracts.

### 10.4 DataNode Reader

Extend the existing `NewStorageV3ManifestReader` and its binlog reader; do not
introduce another complete reader or route the request through `L0ImportTask`.
Keep its baseline delete semantics and resource admission when
no descriptor is present. Field selection in section 11 applies to both
representations. The typed source context follows section 1.1; legacy readers
do not acquire a source commit timestamp from the target schema or job options.
Version 5-8 tasks first fold shared and segment-local deletes into task-owned
physical-row bitmaps as described in section 10.6. Their output readers borrow
the bitmap instead of retaining shared/private PK maps. The source commitTs is
part of the mask identity, and range filtering never renumbers physical rows.
The reader/merger behavior is:

1. Accepts the exact manifest and source commit timestamp from the file and
   L0 references from its task. The worker validates scope/version/inventory
   before reading, then prepares the task's immutable masks. Each reader
   borrows its source mask and a version 1-4 descriptor view
   without inline L0 paths. Persisted file stats stay compact. Old descriptors
   containing inline L0 paths are rejected before opening storage objects.
2. Projects physically present target fields, then selects the shared reader;
   enable TEXT LOB resolution only when the projected read schema contains TEXT.
3. Reads LOB ownership and byte accounting from that manifest.
4. Resolves its own deltalog paths through the shared manifest helper, during
   task preparation for versions 5-8 or reader initialization for versions 1-4.
5. Reuses the reader-independent `DeleteMerger` for both shared and own deletes.
   A synchronous batch consumer folds bounded maps into row masks for versions
   5-8; version 1-4 readers still retain an owned whole-segment delete map.
   Inputs remain source storage, PK type, retry attempts, budget, context,
   timestamp window and decoder contract. An own-manifest path already decoded
   from shared L0 is skipped only under the same decoder contract; conflicting
   contracts fail explicitly. Failed preparation publishes no masks. Task
   readers accept only prepared bitmaps; there is no shared whole-map API or
   per-reader overlay fallback. The scratch merger is reused across batches
   and local-delete scans, and is not retained by the published mask container.
6. Version 5-8 readers consult their physical-row mask before appending a row.
   Version 1-4 readers compare their map's maximum delete timestamp with this
   segment's source effective timestamp as defined in section 12. Every source
   keeps its own commitTs; never prefilter shared L0 using one source's commitTs.
7. Returns logical rows to the unchanged Import row pipeline.

Reader-owned Arrow records remain borrowed until the next read or reader close.
Callers must not release them independently.

The existing early return when a data manifest has no deltalogs must not skip
attached L0 inputs. Final mask consumption covers both shared and local deletes.
Clone retained VarChar keys from borrowed Arrow buffers, check cancellation
while loading deletes, and close data/delete readers on every failure path.
Reuse the legacy V1/V2 fallback only for the legacy list; manifest-selected
packed files retain the existing direct V3 delta path. Refactor map insertion
into a shared, budgeted merge operation so the helpers do not retain a second
unbounded copy of all decoded deletes. Preserve terminal read errors; fallback
failure must not become an empty map. Broader changes to legacy decoder error
classification are not part of this extension. Shared helpers must retain
their baseline defaults when the new context is absent; do not enable the new
filter semantics or map limit for ordinary backup or admitted snapshot jobs
needing no source context. External no-L0 descriptors preserve the actual
source commit timestamp rather than assuming it is zero.

### 10.5 Target Writer

Source storage version does not choose the output format. DataCoord continues
to select the target segment storage version from the target cluster
configuration through the existing `importStorageVersion` logic.

Consequently, a StorageV3 snapshot source can be decoded and written using the
target's configured Import output path without exposing a target format option
to the caller.

### 10.6 Resource Admission

This section applies to every descriptor-bearing snapshot file selected under
section 1.1, including mapping-only jobs. Baseline snapshot jobs and all other
Import kinds retain their existing resource admission behavior.

Version 5-8 tasks resolve each exact L0 manifest and consume each unique shared L0
path once per execution, in bounded PK-to-maximum-timestamp batches. Each batch
scans every task source's PK/timestamp projection and ORs deletions into its
physical-row bitmap. The decoder pauses inside a file/record while the batch is
applied, then continues at the next unconsumed entry without reopening the file.
After shared L0, each source's own deltas update only that source's mask, reusing
the same scratch map. Legacy V1/V2 format
probing and storage retries remain possible; this is not a promise of one
physical I/O attempt. PreImport and Import each build separate masks. Separate
tasks and retries reload it: there is no global/job cache, Bloom-filter routing
service, temporary manifest, or compaction job. Development-version tasks with
per-segment inline L0 inventories are unsupported and must be resubmitted.
The wire fields remain only so those tasks can be rejected explicitly, never
silently imported without their deletes. There is no inline-loading fallback.

Unbounded channel-wide maps are not acceptable. Reuse the single existing
`dataNode.import.readDeleteBufferSizeInMB` parameter as the accounted delete-map
budget for the new typed snapshot reader, increasing its default from 16 to
128 MiB. Do not add a snapshot-specific capacity parameter. The existing legacy
L0 slot estimate also consumes this parameter; legacy L0 readers continue to
use `readBufferSizeInMB` for their row/delete batches.
Account unique PK storage, owned VarChar bytes, timestamps, and a conservative
map overhead before admitting a new key. Replacing an existing timestamp does
not charge another key. Version 5-8 tasks flush a full batch before inserting
the next key; they do not fail merely because the total deletion set exceeds B.
A single oversized key or retained path inventory can still exceed B and fail.
Version 1-4 readers retain their explicit whole-map capacity limit. Errors never
publish partially prepared masks. This is an accounting bound, not a strict RSS
cap for Arrow, decoder buffers, or the Go allocator. In particular, the existing
V1 decoder can download/decode a complete individual file before yielding rows.

At 128 bytes per distinct Int64 PK, the default permits up to 1,048,576 union keys
per batch before charging shared path inventory;
VarChar keys also charge their owned string bytes. Increasing B reduces the
number of PK/timestamp scans, while also raising task admission requirements.

For version 5-8 tasks both phases atomically reserve
`B + R * (rowBuffer + B)`, preserving the existing reservation and DataCoord slot
estimate. B holds the current delete batch/path inventory; the previous R * B
private-map allowance is now the bitmap pool. Fixed-size bitmap blocks are
charged before allocation, including conservative block-pointer overhead.
The first raw scan determines physical row counts without relying on catalog
NumRows or filtered PreImport counts. All later scans and final readers check
those counts; row ordinals advance even for deleted/out-of-range rows.
Bound R by file count, execution-pool
capacity, and the total memory allowance. Clamp the preferred row buffer to
`allowance - 2 * B`; fail if even one positive row buffer cannot fit. Never
hold a delete-batch reservation while waiting for another memory allocation.
Typed version 1-4 readers without shared L0 reserve `rowBuffer + B` individually
for their own manifest deletes and source timestamp context.

PK/timestamp projection and full-field reads use the same sequential packed
column-group order. TEXT SegmentReader only resolves LOB columns over that
stream; it does not filter/reorder rows. No predicate pushdown is used. Exact
manifest versions and this reader contract, not matching row counts alone,
establish bitmap alignment. A real TEXT/column-group regression exercises both
projections and multiple delete/read batches.

The first runnable pool worker performs admission and sequential preparation;
other workers wait for its immutable masks and drain the same file queue. No
rows or statistics are emitted until shared and local deletion scans finish.
One worker can
finish the task without acquiring another execution slot, preventing pool
starvation from trapping a memory reservation. Cancel siblings on failure,
wait for all borrowers to close, then drop the masks and release the exact
reservation. Queued workers arriving after cleanup must not rebuild the masks.
The task's completion future waits for all workers, including on failure.
Cancellation wakes memory waiters. Capture budgets once, never reread them
during cleanup. Existing ordinary Import admission is unchanged. A runtime
budget change may cause a later phase to fail admission; it must never change
which rows a successful reader emits.

The I/O tradeoff is one PK/timestamp pass per shared delete batch (plus local
delete passes) and one final full-field pass. Even a single batch adds a
projection pass. This first version has no separate small-L0 fast path,
checkpoint, bitmap spill, new configuration, or wire-protocol change.

With the 128 MiB default, shared-L0 tasks require an Import allowance strictly
greater than 256 MiB even for one reader. An allowance of 256 MiB or less fails
explicitly rather than shrinking the delete budget or waiting forever. Small
nodes must configure a smaller delete budget or a larger Import allowance;
there is no automatic expansion, disk spill, or new memory allocator.

DataCoord includes these budgets in typed snapshot task slot estimation in
both phases: `N * (rowBuffer + B)`, plus one `B` for shared-L0 tasks. Before
worker selection the coordinator does not know the worker's execution-pool
capacity or memory allowance, so `N` is the task file count, a conservative
upper bound on concurrent readers. Memory slots round up; CPU slots retain
the existing calculation, and the larger cost is used. The worker allocator
still determines actual concurrency and admission. The existing scheduler can
dispatch an oversized task to an available worker on a best-effort basis, so
this estimate does not require all estimated slots to be simultaneously free.
Ordinary Import and legacy L0 slot formulas remain unchanged (the latter sees
the higher configured delete budget). No API or protobuf changes are needed.

`TotalMemorySize` and hashed row statistics continue to describe surviving
output data. Delete-map reservations are separate; adding them to output
statistics would distort segment sizing and disk quota checks. Input-size
accounting may include the exact delete objects read by each file, but must not
claim this repeated-read count is the snapshot's unique physical size.

Keep the existing data-file count limit and account descriptor growth
incrementally while building an L0 source plan. Bound both captured metadata
and the asynchronously expanded plan, so excessive expansion is rejected
before publishing it. Validate the final encoded
WAL message, including header/properties and encoding overhead, before calling
Broadcast, not after durable broadcast-task creation. Use the active backend's
message-size admission where available; a portable explicit snapshot-plan
limit is required otherwise. Catalog persistence and task RPC envelopes also
need to fit within their supported limits.

The implementation caps both the captured preparation descriptor and the
expanded source inventory at **256 KiB**, with
incremental accounting for descriptors and scope-level references, and a final
protobuf-size check. It does not charge the same L0 list once per segment.
Task RPCs carry one merged list regardless of their file count; different
tasks may repeat that list. V3 entries contain exact manifests, not expanded
delta path lists. Deferred path strings and per-entry map overhead are charged
to the same shared delete budget as PKs; they do not get a second allowance.
Resolve and consume one manifest at a time, rather than retaining all expanded
lists. Decoded
L0 is also shared within each task execution;
segment-local deletes remain private. The
complete message's encoded payload/properties are capped at **512 KiB** before
Broadcast. The shared WAL selector resolves `mq.type=default`; Pulsar and
Kafka additionally use half their configured message-size limit when that is
smaller. The factor leaves room for transport framing and later message
properties. Rocksmq and Woodpecker use the portable ceiling. These are explicit
first-version admission bounds, not advertised maximum sizes of the backends.
Do not use `pulsar.maxMessageSize` as a universal limit or silently split the
plan into independently visible Import jobs.

Oversized captured metadata fails before broadcast; oversized expanded plans
fail during Pending preparation. Deferred inventory/delete-map exhaustion fails
the affected task/job without accepting a partial map. Neither limit is a reason to omit
L0, downgrade to the baseline reader, or inflate output quota statistics. The
numeric bounds leave headroom under the default catalog and task RPC limits,
but have not been exercised against every deployed backend or customized
catalog/RPC limit. That transport matrix remains an acceptance gate, not a
claimed result of the local message-size unit tests.

## 11. Schema Compatibility

Snapshot-source Import follows ordinary backup Import: the target schema
interprets source columns by physical FieldID. The 2026-09-16 decision replaces
the earlier field-name alignment proposal. There is no source/target schema
equality check, name-based remapping, or generic Parquet-style type conversion.

Callers must preserve the intended physical field correspondence, including the
primary key used for delete matching. A renamed field still reads the same ID.
Conversely, creating same-named fields in a different order does not make their
IDs correspond; equal physical types do not detect a semantic column swap.
Snapshot Import deliberately does not add a name-mismatch rejection that legacy
backup Import lacks. Stored values must be readable using the target field types,
dimensions and element definitions.

The V1 binlog and V3 snapshot readers use the same field-selection function:

- Read target fields whose IDs are physically present in the source segment.
- Ignore source-only IDs, even when the target enables dynamic fields; do not
  promote their values into dynamic JSON.
- Omit missing nullable/default/dynamic target columns from the reader schema.
  The existing Import task fills target defaults, NULLs and empty dynamic JSON.
- Reject missing required columns, including primary keys and required stored
  function outputs. Keep the existing backup rules for StructArray presence.
- Keep the full target task schema unchanged when selecting reader columns.

Source and target AutoID settings need not match: both PreImport and Import use
the existing backup behavior that preserves source PKs. Stored function outputs
are imported without rerunning functions. Source nullable/default definitions,
partition/clustering markers, collection properties, shard counts and indexes
are not restored or compared for equality. Target routing and writing remain
the standard Import path. Present NULL values and decoding failures retain
existing backup reader semantics; this change does not reorder validation
relative to delete filtering.

Both phases select fields from the same exact manifest. No field mapping or
additional source schema is carried in the task protocol. Source type,
encryption, exact-manifest/path validation and snapshot delete semantics remain
unchanged. StorageV2's legacy packed-binlog path is not modified by this change.

### 11.1 Validation and Filling in Both Phases

DataCoord checks that source/target schemas exist and the collections are
eligible, but does not compare schema identity, names or types. The DataNode
reader checks required physical fields against each exact manifest and decodes
present columns with the target schema. Missing required columns or decoding
errors can therefore fail an admitted task/job; request acceptance is not a
promise that every segment can be decoded.

Both phases retain the full target task schema while projecting only the
reader schema. Filling occurs at the point the existing pipeline needs it:

- **PreImport:** before `GetRowsStats` hashes across multiple target partitions,
  fill an absent/empty default-valued partition-key column using
  `AppendNullableDefaultFieldsData` restricted to that field. Other absent
  optional columns are not materialized here. A single target partition,
  including every explicitly mapped task, takes the existing single-partition
  route without reading the partition key.
- **Import:** before `HashData`, fill all missing nullable/default fields with
  `AppendNullableDefaultFieldsData` and missing dynamic data with
  `FillDynamicData`. The missing partition key therefore gets the same default
  used by PreImport; an already populated column is not overwritten.

This guarantees the same routing value in both phases without eagerly filling
all optional columns during PreImport. PreImport memory statistics retain the
existing Import sizing behavior; they do not promise to include every optional
column that is materialized later.

### 11.2 Missing Columns and Stored NULL Values

For ordinary fields, whole-column absence and a stored NULL are separate paths:

| Source state | Target definition | Result |
|---|---|---|
| Physical column absent | Has a default | Omit from reader; fill target default later, with the PreImport routing exception above |
| Physical column absent | Nullable, no default | Omit from reader; Import fills NULL |
| Physical column absent | Required, no default | Reader initialization fails |
| Column present, row is NULL | Has a default | Shared deserializer substitutes the target default in both phases |
| Column present, row is NULL | Nullable, no default | Preserve NULL |
| Column present, row is NULL | Non-nullable, no default | Reject during row conversion |

`ValueDeserializerWithSchema` applies the present-NULL rule before timestamp
and delete filtering. A non-NULL stored value must decode as the target type;
it is not replaced merely because the target has a default. Missing dynamic
data is filled with `{}`; StructArray presence retains the shared backup rules
described above.

These are backup Import semantics, not an exact Restore contract: a target
default can replace an explicitly stored source NULL. No snapshot-only rule
is introduced to preserve NULL against that default.

## 12. Delete Semantics

### 12.1 Fold Before Target Routing

The following effective-timestamp rule applies to both segment-local and L0
deletes. A nonzero source commit timestamp activates descriptors even without
independent L0 segments: compaction may already have attached pre-commit
deletes to the data manifest. For example, raw row TS 100, commit TS 300, and
delete TS 200 must preserve the row regardless of where that delete is stored.

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
reader path; no additional row scan is needed. Legacy readers and admitted
no-context snapshot readers retain their baseline behavior.

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

### 12.2 Preserve Captured Source Commit Timestamps

Before this change, `SegmentDescription.commit_timestamp` and its Avro
serialization existed, but `handler.GenSnapshot` omitted
`SegmentInfo.GetCommitTimestamp()` when constructing the description. Old
snapshots retain that ambiguity; a zero is not proof of ordinary inserted data.

The current branch implements the producer fix and consumer contract:

1. `GenSnapshot` copies source commit timestamps for every captured segment.
2. Snapshot writing/reading, export, and relocation preserve the captured
   timestamps. Re-exporting an old snapshot must not fabricate omitted values
   using the current catalog.
3. Import accepts omitted commit timestamps in both layouts, including sources
   with L0 or segment-local deletes. Use every captured commit timestamp as-is;
   missing values default to zero. Do not infer missing times from live catalog
   state, the destination Import job, snapshot creation time, or manifest scans.
4. Typed jobs persist each selected data segment's commit timestamp in its
   Import source descriptor; DataNode never looks up the live source segment.

No separate completeness flag is stored or checked. The existing Avro
commit-timestamp field does not need another schema version.

Old snapshots are deliberately accepted with their historical capture defect.
For example, raw row timestamp 100, actual source commit timestamp 300 and
delete timestamp 200 should keep the row. If the snapshot omitted commitTs,
Import sees zero and compares the delete against raw timestamp 100, dropping
the row. The job can succeed with fewer rows than the original source snapshot.
This accepted limitation does not authorize skipping deletes or weakening
other validation. Neither re-export nor reading manifests recovers the missing
time. Create a fresh snapshot with a corrected producer when exact source
semantics are required; Import itself does not repair old metadata.
New snapshots with nonzero commit times always preserve them. The producer
correction remains an independently testable prerequisite requiring snapshot,
export, and ordinary restore regressions: other snapshot consumers now receive
correctly populated source commit timestamps. The separate restore-L0
configuration is not enabled as part of this work.

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
   exact manifest paths; typed jobs keep per-file descriptors containing
   exact data manifests and source commit timestamps, plus immutable L0
   inventories keyed by source scope for versions 5-8.
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
9. WAL replay and CDC transport preserve captured preparation input and source
   storage context. After expansion, file regrouping and task retries preserve
   the descriptors and inventories, including explicit empty scopes. No
   consumer may interpret a missing field as permission to omit L0.
10. Source timestamps and source file references are not rewritten to target
    IDs/timestamps by CDC. Existing target-routing rewrites remain separate.
11. Prepared row bitmaps belong to one task execution and outlive every borrowing
    reader. A reader closes its own stream and releases its bitmap reference;
    it never clears task-owned masks.
    Neither source reads nor abort/retry deletes or rewrites snapshot objects.

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
empty legacy paths, with version 4 required when partition mapping is also
present, as defined in section 1.1. Unsupported workers fail closed rather than
reading target objects with identical keys or losing the requested mapping.
Provider/endpoint admission remains the existing snapshot policy; no
destination-side copier is constructed for Import.

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

For a plaintext source, snapshot metadata is authoritative: a supplied `ezk`
is ignored, including a malformed value. DataCoord removes it from a new
options slice before broadcasting or persisting the Import job, without
mutating the caller's request. PreImport, Import and retries therefore all use
an explicit nil source plugin context, including for TEXT/LOB. The unused key
never reaches the reader or the new job's WAL/options. This normalization is
snapshot-specific; legacy backup Import keeps its existing EZK behavior.

For schemas without TEXT, snapshot Import reuses the existing StorageV3
`PackedReader`, which already accepts the source storage plugin context. A nil
source context is explicit and must not fall back to the target schema's CMEK
properties.

Worker reader selection uses the physically present, projected read schema, not
the complete target schema. A target-only nullable TEXT field requires no source
LOB read: both phases omit it from the reader, and Import fills it with NULL
using the unchanged target schema. It must neither reject an otherwise supported
CMEK source nor enable LOB resolution. This does not relax DataCoord's rejection
of a CMEK source whose snapshot schema contains TEXT/LOB.

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
- New snapshot jobs satisfying the baseline conditions in section 1.1 retain
  that representation and resource admission. Old snapshots with omitted commit
  timestamps are accepted using captured/default-zero values, with the
  historical delete-ordering limitation in section 12.2. Re-export does not
  repair omitted times.
  Already-persisted baseline jobs are not migrated; recreate affected jobs to
  obtain the corrected source timestamp context.
- Import does not invoke `RestoreSnapshot`, `RestoreExternalSnapshot`, or
  CopySegment. Snapshot capture/export additionally preserve the commit
  timestamps and L0 object inventory required above; restore-L0 remains a
  separate feature with its existing enablement rules.
- No public protobuf field or enum is added.
- Previously persisted development-version tasks containing per-segment inline
  L0 paths are rejected during WAL binding/plan validation and by both worker
  phases before storage IO. Recreate the Import job from a supported snapshot;
  recovery does not migrate those tasks. Versions 1-4 remain supported when
  inline L0 fields are empty, including commit-time, external-storage and
  partition-mapping contexts without shared L0. The task-shared L0 inventory
  still supports V1/V2 delete files: physical log format is independent of
  snapshot timestamp completeness and task protocol compatibility.
- `source_type=snapshot` is opt-in.
- Internal protobuf additions are append-only: `ImportFile.snapshot_source`,
  the Import WAL header descriptors, and `SnapshotImportSource.target_partition_id`.
  Regenerate Go protobufs with `make generated-proto-without-cpp`; do not edit
  generated files manually.
- Upgrade DataCoord, DataNode, and participating CDC consumers before submitting
  typed snapshot jobs. Consumers must support the descriptor version selected
  by section 1.1; support for external version 2 alone is insufficient for an
  external mapped job. Unknown-field retention is not capability negotiation.
  Mixed-version execution/downgrade must fail closed, but uninterrupted
  completion on older nodes is not guaranteed. Baseline jobs retain their
  existing representation compatibility. Every typed job requires empty legacy
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
WAL/task descriptors are still required to preserve captured and expanded inputs. A
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
- Reject missing required physical columns, non-StorageV3 data segments, external
  collections, missing manifests, path escapes, invalid/mismatched EZKs for
  encrypted sources, and CMEK sources containing TEXT/LOB fields.
- Accept matching source EZKs for non-TEXT schemas and keep plaintext-source
  reads from falling back to target CMEK properties. Strip unused plaintext
  EZKs before WAL persistence, even when malformed or used with TEXT/LOB;
  preserve the original request and unrelated options.
- Verify physical-ID selection, including renamed fields. Names must neither
  remap columns nor introduce a snapshot-only admission restriction.
- Verify target-only fields receive the target default or NULL as applicable,
  and missing required fields fail. Preserve the full target schema for filling.
- Verify a missing default partition key produces identical routing in both
  phases for multiple target partitions, while single-partition tasks bypass
  partition-key hashing. Keep stored-NULL/default behavior distinct from
  whole-column absence as described in section 11.2.
- Verify ordinary source-only fields are ignored, independently of target
  dynamic-field enablement, and absent target dynamic data is filled with `{}`.
- Exercise both layouts with differing AutoID settings and manual commit;
  verify source PK preservation, target default/NULL filling, and L0 folding
  while dropping source-only columns.
- Resolve inline and out-of-line TEXT values.
- Handle absent, zero-entry, readable, corrupt, and missing deltalogs.
- Preserve borrowed Arrow record ownership.
- Keep target storage-version selection independent from source metadata.

Additional L0 tests must exercise the real functions, mocking only their
dependencies with mockey:

- Verify the routing boundary for ordinary Import, V1/V2 backup, legacy L0
  Import, old/new no-L0 snapshots, and L0-bearing snapshots. Accept old snapshots
  in both layouts, including no-delete, nonzero-commit, external-source and
  partition-mapped inputs. No-context jobs retain baseline routing; nonzero
  commit TS activates descriptors even without L0. Do not probe data manifests
  to infer missing timestamps. Real-file tests must demonstrate both preserved
  commit-time filtering and the accepted row-loss case when commitTs was omitted.
- Cover all descriptor versions in section 1.1, including external-only,
  mapping-only, and external-plus-mapping jobs without L0. Preserve actual
  source commit timestamps and bounded admission/map accounting. Cover
  segment-local deletes exceeding the budget and rejection by workers that
  do not support the selected version.
- Select same-channel/same-partition and same-channel `AllPartitionsID` L0
  segments; exclude other channels/partitions and reject missing identities.
- Include L0 only in an unrelated channel/partition: it must not activate
  folding for the selected sources. Conversely, malformed applicable L0 must
  fail rather than be misclassified as no-L0 input.
- Read V1/V2/V3 L0 storage without enabling V1/V2 snapshot data Import. Include
  V2 segment metadata whose deltalog was written by the legacy V1 writer.
- Capture regular zero and nonzero import/CDC source commit timestamps; preserve
  them through both layouts and export. Re-exporting an old snapshot must not
  fabricate omitted values. Import uses the captured values, including the
  known missing-commitTs limitation.
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
- Count real delta-reader opens for multiple segments in both phases. Verify
  distinct source commitTs values, per-task reload, foreign source storage,
  single-worker progress, limited pool slots, parallel failures and cancellation
  without early reservation release or leaked workers.
- Exercise a real L0 log with 131,073 distinct Int64 PKs through both phases:
  the 128 MiB default and an explicit 16 MiB budget produce identical survivors;
  the latter requires multiple batches without reopening L0. Cover real TEXT,
  separate column groups, source commit timestamps, local deletes, exact
  capacity, single-key overflow, bitmap exhaustion and late batch failures.
  Cover default-budget admission on 2/4/8 GiB nodes,
  the no-row-room boundary at a 256 MiB Import allowance, and unchanged legacy
  L0 batch sizes. Check slots for descriptor versions 1-8, delete-batch charging
  once per task, partition mapping, ordinary Import, and configured legacy L0.
- Count foreign source resolutions under concurrent reader creation and shared
  L0 loading: once per task execution, with independent initialization in both
  phases and retries. Cover tasks with shared and no L0, rejection of inline-L0
  development-version tasks before IO, source access
  failure without target fallback, cancellation during initialization, isolated
  reader cancellation/Close, and unchanged target write configuration.
- Verify PreImport surviving-row and hashed statistics against actual Import
  rows, including zero-row auto/manual commit and abort. Delete-map accounting
  must not inflate output disk quota or row size.
- Verify Create performs one metadata read and no segment/physical object I/O
  with 400 and 1024 segments in both layouts. Preserve captured bytes through
  actual WAL/CDC message transformation; reject Pending descriptors in workers.
- Remove the root metadata object after capture and prepare from persisted
  input. Cover catalog-save and allocation failures, retries, superseded plans,
  cancellation/timeout, terminal-state races and shutdown. Four blocked source
  reads must not stop ordinary Import scheduling or start duplicate attempts.
- Count exact V3 L0 manifest resolution once per task, including URI/key aliases.
  Reject outside-root/foreign-bucket paths, missing LOBs, corrupt manifests and
  deferred inventory exhaustion before returning a usable reader/delete map.
- Exercise oversized capture/expanded plans against the selected admission
  bounds. Capture and encoded-message bounds reject before Broadcast; expanded
  inventory bounds reject before task creation. Catalog and task RPC envelopes
  also need backend validation; no partial plan is valid.

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

`TestImportStorageV3SnapshotSourcePlaintextIgnoresEZK` imports plaintext
snapshots with both valid-shaped and malformed unused EZKs in referenced and
self-contained layouts. It verifies inline TEXT and out-of-line LOB values
after import. It creates an explicitly unencrypted, test-owned database and
cleans up its collections, snapshots and exported objects. No cipher plugin,
KMS key or management endpoint is required.

CMEK runtime E2E coverage is outside this change. The plaintext import case
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

1. Baseline representation, delete semantics and resource admission remain
   unchanged for the non-activated cases in section 1.1. Shared field-selection
   and default-filling behavior follows section 11 and has regression tests.
2. Source timestamp capture, channel/partition matching, and delete ordering
   pass deterministic tests, including imported source segments, reinserts and
   the accepted missing-commitTs limitation. Missing/corrupt delete input never
   becomes an empty delete set.
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

The current branch implements the runtime and test additions in steps 1-5
below. This records their dependency order, not completion of every validation
gate. The public Import request reuses its existing options and file fields.

1. Source commit-timestamp capture is implemented as an
   independently testable producer fix. Snapshot/export/restore regression
   requirements remain; restore-L0 is not enabled and persisted baseline
   Import jobs are not migrated.
2. Internal source descriptors and the WAL header carrier preserve the typed
   contracts in section 1.1. Version 9 captures preparation input at Create;
   versions 1-8 and baseline files describe the expanded worker plan.
3. DataCoord expansion fixes the job-wide execution contract, validates scoped
   inputs, preserves captured timestamps, and bounds the plan before task creation. Explicit
   partition mappings are resolved and rechecked under the broadcast lock;
   neither error handling nor regrouping permits a baseline fallback.
4. The existing reader reuses delta decoding, source effective timestamps,
   bounded map accounting and cancelable admission for typed jobs. Shared
   physical-field selection and two-phase filling follow section 11; target
   writer and commit/abort behavior remain on the existing Import path.
5. Deterministic regressions and Go SDK cases have been added for the baseline,
   L0, schema and mapping paths. Section 18 also lists remaining validation
   requirements; it is not a claim of complete test coverage. Execute
   service-dependent validation only on an owned environment; test presence
   or compilation does not establish a live result.
6. Advertise L0 support only after section 18.4 and the end-to-end input and
   failure-path audit are complete. Record unrun CDC/backend/plugin scenarios
   explicitly; neither compilation nor a no-L0 happy-path E2E proves L0 support.

Implemented change areas include `handler.go`, `import_snapshot.go`,
`ddl_callbacks_import.go`, internal protobufs, `snapshotio`, the Import reader
dispatch and binlog reader/filter, and the two task entry points' memory
admission. There is no new public SDK API, target L0 task, source compaction
policy, or alternative TEXT/CMEK reader. Typed delete/timestamp semantics and
resource admission follow section 1.1. The snapshot-producer correction, field
selection, plaintext-EZK normalization and default filling have their separately
documented scope; they are not all conditional on a typed source descriptor.

Implementation Status separates historical service-level results from the
current revision's build and targeted-test results. The latest Go SDK additions
still need live execution, and the transport/restore matrix and changed-function
coverage gate remain required before step 6.

## 20. Follow-Ups

- Extend the typed descriptor to StorageV1/V2 data segments and mixed snapshots.
- Cross-task per-channel delete caching, bitmap spill, and fully streaming
  decoding of arbitrarily large individual legacy deltalog files.
- Name-based field remapping and broader type conversion beyond the shared
  physical-FieldID backup Import contract.
- Arbitrary cross-provider snapshot reads beyond the current endpoint policy.
- CMEK key-retriever propagation through the shared milvus-storage
  `SegmentReader` for TEXT/LOB resolution.
- A typed SDK option that replaces raw `source_type` key-value handling if the
  source contract expands beyond snapshots.
