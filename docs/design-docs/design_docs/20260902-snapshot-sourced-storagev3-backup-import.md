# MEP: Snapshot-Sourced StorageV3 Backup Import

- **Created:** 2026-09-02
- **Updated:** 2026-09-17
- **Status:** Proposed
- **Feature DRI:** @weiliu1031
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD
- **Related Issue:** [milvus-io/milvus#52930](https://github.com/milvus-io/milvus/issues/52930)
- **Related PR:** [milvus-io/milvus#52932](https://github.com/milvus-io/milvus/pull/52932)

### Implementation Status

The 2026-09-16 decision supersedes the earlier name-based projection plan:
snapshot Import directly follows backup Import's physical FieldID semantics.
It does not compare source and target schemas or field names for equality.
The V1 backup and snapshot readers share field selection; missing optional
target columns are left to the existing Import filling path. See section 11.
This is not name-based mapping or a general schema/type conversion feature.
The current implementation, including explicit partition mapping and the
two-phase default/nullable fixes, is recorded at commit `2865768271`.

Commit `b6b908c4dd97b19eab5a6e81fc442a4ff3a846d3` implements the
snapshot-source baseline. The current branch implements source
timestamp capture/provenance, L0 selection and distribution, typed WAL/ACK and
ImportFile descriptors, bounded delete merging in the existing reader, and
cancelable memory admission in both PreImport and Import. Typed readers check
original row timestamps before filtering and fail closed on invalid nonzero
source commit times. Malformed durable descriptors create Failed jobs rather
than entering an ACK retry loop.

Section 1.1 defines the job-wide typed-reader activation and descriptor versions,
including explicit partition mapping. A commit-time override is required even
when deletes have already moved into data manifests. Old snapshots without
timestamp provenance are rejected if any data manifest contains readable
deletes. The legacy `l0_import` path is not enabled or repurposed.

Local deterministic tests exercise real V3 data and V1/V2 delete files,
PreImport/Import execution, timestamp ordering, memory/cancellation failures,
WAL/CDC message transformation, ACK binding, protobuf persistence, regrouping,
and snapshot/export provenance. Execution results must be read by revision:

- **Historical service E2E, 2026-09-09:** the local Go SDK run passed both
  layouts with and without L0, plus self-contained cross-bucket imports with
  and without L0. This does not validate the later schema/mapping changes.
- **Current code at `2865768271`, 2026-09-17:** protobuf generation, C++ and Go
  builds, and targeted Go tests passed in the designated worktree. Reader and
  two-phase regressions were rerun after the final reader fix. The Go SDK
  testcases compiled; the new partition-mapping, backup-schema and plaintext
  EZK scenarios have not been executed against a running service.
- **Remaining gates:** live CDC/backend coverage, complete restore regression,
  full package suites, repository verifiers, and all acceptance gates in
  section 18.4 are not established for this revision. Changed-function coverage
  has not reached the 99% repository gate. CMEK runtime E2E remains out of scope.

Do not equate the implemented runtime path with release-ready end-to-end
verification.

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
snapshot Import jobs. After source validation, DataCoord selects typed files
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
| Snapshot satisfying none of the four activation conditions | Existing one-manifest-per-file path; old snapshots must have no readable manifest deletes |
| Snapshot with any nonzero source commit timestamp | Job-wide descriptors preserve commit-time delete semantics, including without L0 |
| Snapshot with `external_spec` | Job-wide descriptors select source storage, including without L0 |
| Snapshot with `partition_mapping` | Job-wide descriptors bind files to target partitions, including without L0 |
| Snapshot with applicable L0 and reliable source timestamps | New descriptors and bounded read-time folding |
| Snapshot with applicable L0 but missing provenance or invalid input | Explicit failure, never fallback to the baseline path |

For every typed job, select the descriptor version from the source-storage and
partition-mapping options. L0 and commit timestamps do not alter this matrix:

| Source storage | Partition mapping | Descriptor version |
|---|---|---|
| Instance storage | Absent | 1 |
| Request-scoped external storage | Absent | 2 |
| Instance storage | Present | 3 |
| Request-scoped external storage | Present | 4 |

Versions 3/4 require a positive `target_partition_id`; versions 1/2 leave it
unset. All versions preserve the source manifest, applicable deletes, and
source commit timestamp. Unsupported versions fail closed rather than losing
source-storage, delete, or target-routing semantics.

Activation is **job-wide**, not reevaluated per task. Every data file in an
activated job carries a descriptor, including files with no individually
applicable L0. This avoids mixed representations in one WAL message and keeps
dispatch stable when files are regrouped. A task containing only such files
still uses the descriptor path. Missing timestamp provenance is safe only
without deletes; external storage or partition mapping does not bypass this
source validation.

Descriptor presence also selects the bounded delete map and cancelable memory
admission in both phases. Thus even a job activated only by external storage
or partition mapping reserves row-buffer plus delete-map memory, and its
segment-local deletes can exceed the configured
`dataNode.import.readDeleteBufferSizeInMB` budget (16 MiB by default). Such a
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
and storage config. The shared PreImport/Import reader resolves the same URI
and spec on each execution, without rereading snapshot metadata. The task's
target chunk manager/storage config is never replaced. Failed source access
never falls back to target storage. This does not change commit/abort semantics.

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

The destination travels in `SnapshotImportSource.target_partition_id` through the
existing WAL header, ACK binding, catalog files and file statistics. Section 1.1
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
  - resolve default/name, partition mapping, or partition-key destinations
          |
          v
DataCoord snapshot-source expansion
  - read metadata and manifests with SnapshotReader
  - validate layout, paths, schema presence, source encryption, and segment types
  - select StorageV3 L1/L2 data segments
  - match captured L0 inputs by source channel and partition scope
  - validate timestamp provenance and bind explicit partition mappings
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
          WAL -> ACK -> persist complete ImportFiles
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
2. Call `SnapshotReader.ReadSnapshot(..., true)`.
3. Let the reader validate snapshot format compatibility and normalize the
   referenced or self-contained layout.
4. Validate snapshot paths with the shared snapshot path validators.
5. Validate schema presence, source/target collection eligibility, and source
   encryption. Do not compare source and target schemas for equality; worker
   readers validate physical columns as described in section 11.
6. Validate complete mapping coverage when requested, and collect data segments
   from every source partition in the snapshot.
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
9. Decide `hasApplicableL0` for the entire selected input. Reject missing
   timestamp provenance for applicable L0 or readable segment-local deletes.
   Select the job representation and descriptor version using section 1.1;
   L0 is only one of the activation conditions.
10. For applicable L0, verify timestamp provenance as specified in section 12.2.
    Resolve exact V3 L0 manifests or explicit V1/V2 deltalog inventories.
    Validate original URI bucket/endpoint and layout/root before normalizing
    delete references to object keys. Persist normalized keys and use them for
    deduplication and decoder-conflict checks; URI/key aliases identify one
    object. ChunkManager must never receive a complete URI as an object key.
11. Sort data sources by source segment ID, deduplicate each source's delete
    references, and generate a descriptor for every file in a typed job. Attach
    resolved target partition IDs after matching deletes in the source scope.
    Conflicting metadata for the same delete object is an error, not a reason
    to choose one arbitrarily. Check incremental source-plan growth and the
    final encoded size before broadcast.
12. Recheck mapped target name/ID bindings under the collection broadcast lock,
    then broadcast the complete immutable input through the existing Import WAL
    message. The ACK callback attaches each descriptor to its internal
    `ImportFile` before persisting the job.

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

`snapshotImportValidationData` keeps V3 manifests authoritative and retains
explicit V1/V2 **L0 deltalogs** for validation, without restoring unused
stats/index files to the Import input. Missing channel identity or an
unsupported applicable L0 representation fails validation; unrelated
partitions' L0 inputs are not read.

Retries reuse the persisted input representation: exact manifests for baseline
jobs, or complete descriptors for L0, commit-time override, external-storage, or partition-mapping jobs. Neither
task regrouping nor a DataCoord/DataNode restart repeats snapshot expansion or the activation
decision. A snapshot with no selected data segments remains invalid; L0-only
input is not an operation on target rows.

### 10.3 Import Source Representation

Add an internal, versioned source descriptor in `pkg/proto/internal.proto`.
Jobs with L0, nonzero source commit timestamps, external storage, or partition mapping populate it.
The fields are:

```protobuf
message SnapshotImportSource {
  uint32 version = 1; // 1/2: local/external; 3/4: mapped local/external.
  string manifest_path = 2;
  uint64 source_commit_timestamp = 3;
  // Explicit legacy inventory; use the existing V1/V2-compatible reader.
  repeated string legacy_l0_deltalogs = 4;
  // Exact V3 manifest inventory; use the existing packed delta reader.
  repeated string manifest_l0_deltalogs = 5;
  int64 target_partition_id = 6; // Required for versions 3/4; absent for 1/2.
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

For a job requiring source context, the producer places one descriptor
in the header per body file, in identical order. The body still contains one file per selected
data segment, but its `paths` are **empty** for a typed source. File IDs are
allocated by the ACK callback, so pairing uses the original array index at this
boundary only. The callback validates cardinality, source kind, descriptor
version, and absence of legacy paths, then copies each descriptor into the
same internal file whose ID it allocates. All subsequent persistence,
PreImport statistics, regrouping, Import tasks, and retries carry that internal
file; they must never reconstruct files from paths alone or rejoin parallel
arrays. Broadcast and CDC must preserve header/body ordering until this binding.

For a baseline job under section 1.1, the header has no descriptors and each
body file retains its exact manifest in `paths[0]`. The ACK callback keeps the
existing conversion for that representation. There is no migration of baseline
persisted jobs. Descriptor presence follows the complete activation rule, not
L0 presence alone.

Empty legacy `paths` are intentional. If an older consumer drops the new
header or protobuf field, the baseline snapshot reader rejects the resulting
zero-path file. Leaving a usable manifest in `paths[0]` would instead allow an
old DataNode to ignore L0 inputs and successfully import deleted rows. Keep
body-file cardinality nonzero so loss of the header cannot become an empty,
successful job. Compatibility tests must exercise both old ACK and old worker
behavior, not just unknown-field round trips.

Within a typed job, a file can have empty L0 lists and must still retain
its descriptor and source commit timestamp. This includes regrouped tasks
whose files all have empty lists. Reader and admission dispatch use descriptor
presence, not the local number of delete paths.

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
Keep its baseline call contract, delete semantics and resource admission when
no descriptor is present. Field selection in section 11 applies to both
representations. The typed source context follows section 1.1; legacy readers
do not acquire a source commit timestamp from the target schema or job options.
With that context, the reader:

1. Accepts the exact manifest, source commit timestamp, and L0 references from
   `ImportFile.snapshot_source`.
2. Projects physically present target fields, then selects the shared reader;
   enable TEXT LOB resolution only when the projected read schema contains TEXT.
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
allocator for this path: cancellation must wake waiters. Treat the existing
row-buffer calculation as a preference and clamp it to `allowance - deleteBudget`
before reserving the combined amount. Preserve the full delete budget; if it
leaves no room for a positive row buffer, fail instead of waiting forever.
Clamp against the total allowance, not currently free memory: concurrent
readers wait for their reservation without changing each other's batch sizes.
Both phases pass the admitted row buffer and delete budget to the reader, and
release exactly that combined reservation without rereading configuration.
Do not acquire the delete reservation incrementally while holding a row
reservation. Close readers and release reservations on success, initialization failure, read
failure, and cancellation. Existing allocation callers for ordinary Import and
admitted snapshot jobs needing no source context are not migrated. A
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

### 12.2 Preserve Source Timestamp Provenance

Before this change, `SegmentDescription.commit_timestamp` and its Avro
serialization existed, but `handler.GenSnapshot` omitted
`SegmentInfo.GetCommitTimestamp()` when constructing the description. Old
snapshots retain that ambiguity; a zero is not proof of ordinary inserted data.

The current branch implements the producer prerequisite and consumer contract:

1. `GenSnapshot` copies source commit timestamps for every captured segment.
2. Internal `SnapshotInfo` includes
   `bool segment_commit_timestamps_preserved = 14`. Newly generated snapshots
   set the marker after copying segment descriptions.
3. The marker is carried through snapshot metadata writing/reading, export, and
   relocation. Exporting an old snapshot must preserve `false`; it must not
   fabricate provenance or upgrade the marker using the current catalog.
4. Import requires the marker when selected data sources have applicable L0
   segments or readable segment-local deletes. Otherwise it rejects with an
   instruction to generate a new snapshot; zero in old metadata is ambiguous.
5. Typed jobs persist each selected data segment's commit timestamp in its
   Import source descriptor; DataNode never looks up the live source segment.

The marker establishes that a zero was intentionally captured. It is not a
cryptographic integrity guarantee for caller-supplied metadata. The existing
Avro commit-timestamp field does not need another schema version; the new
metadata capability distinguishes reliable captures from older omissions.
An older tool that drops the marker produces an unsupported L0 source, not a
source that can be imported while ignoring the uncertainty.

Old snapshots without applicable L0 and without readable manifest deletes can
still be admitted. Missing timestamp information is never guessed for deletes;
new snapshots with nonzero commit times always preserve them. The producer
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
9. For typed jobs, WAL replay, CDC transport, file regrouping, and task
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
  that representation and resource admission. Old
  snapshots with readable deletes but missing timestamp provenance are rejected.
  Already-persisted baseline jobs are not migrated; recreate affected jobs to
  obtain the corrected source timestamp context.
- Import does not invoke `RestoreSnapshot`, `RestoreExternalSnapshot`, or
  CopySegment. Snapshot capture/export additionally preserve the timestamp
  capability and L0 object inventory required above; restore-L0 remains a
  separate feature with its existing enablement rules.
- No public protobuf field or enum is added.
- `source_type=snapshot` is opt-in.
- Internal protobuf additions are append-only: `ImportFile.snapshot_source`,
  the Import WAL header descriptors, `SnapshotImportSource.target_partition_id`,
  and the snapshot timestamp capability.
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
  Import, old/new no-L0 snapshots, and L0-bearing snapshots. Admitted no-context
  jobs retain baseline routing; nonzero commit TS must activate descriptors
  even without L0. Reject readable deletes with missing timestamp provenance.
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

The current branch implements the runtime and test additions in steps 1-5
below. This records their dependency order, not completion of every validation
gate. The public Import request reuses its existing options and file fields.

1. Source commit-timestamp capture and provenance are implemented as an
   independently testable prerequisite. Snapshot/export/restore regression
   requirements remain; restore-L0 is not enabled and persisted baseline
   Import jobs are not migrated.
2. Internal source descriptors and the WAL header carrier preserve the typed
   contracts in section 1.1. Baseline message/file representations remain
   unchanged; generated protobufs include the target partition binding.
3. DataCoord expansion fixes the job-wide execution contract, validates scoped
   inputs and provenance, and bounds the plan before broadcast. Explicit
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
- Shared per-channel delete caching or external spill for sources exceeding
  the first version's per-reader memory budget.
- Name-based field remapping and broader type conversion beyond the shared
  physical-FieldID backup Import contract.
- Arbitrary cross-provider snapshot reads beyond the current endpoint policy.
- CMEK key-retriever propagation through the shared milvus-storage
  `SegmentReader` for TEXT/LOB resolution.
- A typed SDK option that replaces raw `source_type` key-value handling if the
  source contract expands beyond snapshots.
