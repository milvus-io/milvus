# Design Document: External Table Add-Column Refresh

**Branch**: `design/external-refresh-add-column`
**Date**: May 2026
**Related Issue**: [#45881](https://github.com/milvus-io/milvus/issues/45881)

---

## 1. Overview

### 1.1 Motivation

External tables keep segment data in storage-v2 manifests that point to
external files. Refresh normally preserves a current segment when its
external file fragments are still present, and only creates new segments for
new or rebalanced fragments.

That behavior is not enough after an additive schema change. If a user adds a
new Milvus field mapped to an existing Parquet column, an unchanged external
fragment still needs a new manifest column group and a new fake binlog entry.
Keeping the old segment unchanged would leave the segment behind the latest
collection schema, so the new field would not be covered by the segment
manifest or by the load-time memory estimate.

This design extends refresh so an unchanged external fragment can produce a
same-segment patch. The patch advances only the manifest path, fake binlog
coverage, and schema version while preserving the segment ID and existing
segment metadata.

### 1.2 Goals

1. Support additive external fields on existing external tables.
2. Preserve segment IDs for unchanged external fragments.
3. Append missing manifest column groups instead of rebuilding unchanged
   segments.
4. Return same-ID updated segments from DataNode to DataCoord.
5. Apply updated segments as upserts in DataCoord.
6. Reject stale refresh results if the collection schema changes during a
   refresh job.
7. Keep current refresh semantics for removed files, added files, and
   unchanged segments that already cover the latest schema.

### 1.3 Non-Goals

1. Dropping external fields.
2. Renaming fields or changing `external_field` mappings.
3. Changing field data types or vector dimensions.
4. Automatic refresh triggered by schema changes.
5. Rebuilding vector indexes as part of refresh.
6. Supporting additive changes for non-external collection data.

---

## 2. Terminology

| Term | Meaning |
|------|---------|
| External field | A Milvus field with `external_field` set, mapped to a column in the external source. |
| Fragment | A row range in an external file, identified by file path, start row, and end row. |
| Manifest column group | A storage-v2 manifest entry that maps one or more columns to a set of fragments. |
| Kept segment | An existing segment whose fragments and field coverage are still valid. |
| Patched segment | An existing segment whose fragments are still valid but whose manifest and fake binlogs must be advanced for newly added fields. |
| New segment | A segment created from orphan fragments after files are added, removed, or rebalanced. |
| Updated segment | DataCoord upsert payload. It can be either a patched existing segment or a new segment. |

---

## 3. High-Level Flow

```text
AlterCollectionSchema(AddField)
        |
        v
Collection schema version advances
        |
        v
RefreshExternalCollection
        |
        v
DataCoord records refresh job schema_version
        |
        v
DataCoord creates refresh tasks with the same schema_version
        |
        v
DataNode compares current segment fragments with external fragments
        |
        +-- Fragment removed or new fragment found
        |       |
        |       v
        |   Rebalance orphan fragments into new segments
        |
        +-- Same fragments and all external fields covered
        |       |
        |       v
        |   Return segment ID in kept_segments
        |
        +-- Same fragments but newly added external fields missing
                |
                v
            Append missing manifest column groups
                |
                v
            Rebuild fake binlogs and schema_version
                |
                v
            Return same segment ID in updated_segments
        |
        v
DataCoord validates schema_version and updated segment payload
        |
        v
DataCoord atomically drops obsolete segments, keeps valid segments,
and upserts patched or new segments
```

---

## 4. Metadata Model

### 4.1 Refresh Job Schema Version

`ExternalCollectionRefreshJob` stores the collection schema version that was
current when the refresh job was accepted:

```protobuf
message ExternalCollectionRefreshJob {
  ...
  int32 schema_version = 12;
}
```

This schema version is copied into every generated
`ExternalCollectionRefreshTask`:

```protobuf
message ExternalCollectionRefreshTask {
  ...
  int32 schema_version = 18;
}
```

The stored value is a refresh boundary. DataNode receives the schema snapshot
that DataCoord dispatches for the task, and DataCoord later rejects the result
if the collection schema has advanced beyond the job schema version.

For compatibility with older persisted refresh metadata, `schema_version == 0`
means "unknown" and skips the version gate.

### 4.2 Updated Segments As Upserts

The refresh response already carries `updated_segments`. This design makes the
field an upsert payload:

- If the segment ID does not exist in DataCoord meta, the payload is a new
  segment.
- If the segment ID already exists, the payload is a patch for that segment.
- A segment ID cannot appear in both `kept_segments` and `updated_segments`.
- Duplicate updated segment IDs are rejected.

This keeps the wire shape stable while extending the semantics from "new
segments only" to "new or patched segments".

---

## 5. DataCoord Design

### 5.1 Job Acceptance

When `RefreshExternalCollection` accepts a job, DataCoord reads the current
collection schema and persists `collection.Schema.Version` on the job. Task
creation copies that value into each task.

This captures the exact schema version the refresh is allowed to produce.

### 5.2 Dispatch Gate

Before dispatching a task to DataNode, DataCoord reloads the collection from
meta. If the current schema version differs from the task schema version, the
task fails before worker execution.

This prevents a task from using stale current segment metadata together with a
newer schema snapshot.

### 5.3 Apply Gate

Before applying a finished job, DataCoord validates that the collection schema
version still matches the job schema version. If the schema changed while the
job was running, the job result is rejected before any segment mutation.

### 5.4 Segment Update Validation

`applyExternalCollectionSegmentUpdate` now validates both kept and updated
segments before mutating meta:

- Kept segment IDs must exist.
- Kept segments must belong to the target collection.
- Dropped segments cannot be kept.
- Updated segments must have a manifest path.
- Updated segments must carry fake binlogs.
- Updated segment row counts must match their fake binlog row counts.
- A patch cannot change the row count of an existing segment.
- A patch cannot change the collection ID.
- A patch cannot target a dropped segment.
- A patch cannot roll back the segment schema version.
- A segment cannot be both kept and updated.
- Updated segment IDs must be unique.

### 5.5 Segment Mutation

The mutation remains atomic through DataCoord segment update operators:

1. Drop active segments that are neither kept nor upserted.
2. Add new updated segments.
3. Patch existing updated segments.

For an existing segment patch, only these fields are advanced:

- `ManifestPath`
- `SchemaVersion`
- `Binlogs`
- `StorageVersion`, when present in the incoming payload

Other segment metadata, including segment ID, collection ID, partition ID,
insert channel, row count, state, level, and index-adjacent metadata, is
preserved from the existing segment.

---

## 6. DataNode Design

### 6.1 Segment Classification

DataNode builds the current segment-to-fragments mapping from existing
manifest files, then compares it with the external fragments discovered by the
explore manifest.

For each current segment:

1. If any fragment is missing from the new external fragment set, the segment
   is invalidated and its surviving fragments are treated as orphan fragments.
2. If all fragments still exist and all current external fields are covered by
   fake binlogs, the segment is kept unchanged.
3. If all fragments still exist but some external fields are missing from fake
   binlog `ChildFields`, the segment is patched in place.

Orphan fragments are still balanced into new segments using the existing
refresh path.

### 6.2 Missing Field Detection

DataNode derives target external fields from the task schema:

- Only fields with `external_field` are considered.
- Existing coverage is read from segment fake binlog `ChildFields`.
- A field is missing if the schema requires it and no fake binlog child field
  covers it.

The missing set is expressed as external column names, not Milvus field IDs,
because manifest column groups reference external source column names.

### 6.3 Same-ID Segment Patch

When missing columns are detected for an otherwise unchanged segment, DataNode:

1. Appends the missing external columns to the existing segment manifest.
2. Samples external field sizes from the new manifest.
3. Computes the segment memory estimate.
4. Builds a fresh fake binlog payload for the latest schema.
5. Returns a cloned `SegmentInfo` with:
   - the same segment ID,
   - the new manifest path,
   - the task schema version,
   - storage version v3,
   - rebuilt fake binlogs.

The old segment is not mutated in DataNode. DataCoord performs the actual meta
patch after validation.

### 6.4 Worker Response

`GetUpdatedSegments()` returns only segments DataCoord should upsert:

- patched existing segments,
- newly created segments.

Unchanged kept segments are returned only by ID through `kept_segments`.

---

## 7. Manifest Design

### 7.1 Reading Column Groups

The packed manifest layer exposes `ReadColumnGroupsFromManifest`, which reads
both:

- column names in each manifest column group,
- fragment path and row range entries in each column group.

`ReadFragmentsFromManifest` is rebuilt on top of this function and deduplicates
fragments by `(file_path, start_row, end_row)`.

### 7.2 Appending Columns

`AppendSegmentManifestColumns` appends missing columns to an existing manifest:

1. Read existing column groups.
2. Filter out requested columns that already exist for the same fragment set.
3. Reject a column that already exists with a different fragment set.
4. Create new Loon column groups for the remaining columns.
5. Start a Loon transaction from the current manifest version.
6. Add the new column groups.
7. Commit and return the new manifest path.

If every requested column is already present for the same fragment set, the
operation returns the original manifest path. This makes retry behavior
idempotent.

### 7.3 Fragment Identity

Fragment identity is defined by:

```text
file_path + start_row + end_row
```

The fragment ID assigned inside Go is local to the current list and is not used
as persistent identity.

---

## 8. Failure Handling

### 8.1 Schema Changes During Refresh

Refresh results are rejected before mutation if the collection schema version
changed after job acceptance.

This avoids mixing an old task result with a newer schema. A user can retry
refresh after the newer schema is stable.

### 8.2 Invalid Worker Payload

DataCoord rejects invalid worker payloads before mutation. This includes row
count mismatches, dropped segment patches, collection mismatches, empty
manifests, empty fake binlogs, duplicate updated segment IDs, and
kept/updated overlap.

### 8.3 Manifest Append Failures

If manifest append fails, DataNode fails the task. DataCoord does not apply a
partial patch because the updated segment payload is not persisted as a
finished result.

### 8.4 Sampling Failures

If field-size sampling cannot produce a positive memory estimate, DataNode
fails the patch. This avoids persisting a segment that QueryNode cannot size
correctly during load.

---

## 9. Compatibility

### 9.1 Existing Refresh Jobs

Persisted jobs or tasks without a schema version continue to run with
`schema_version == 0`. The schema-version gate is skipped for those records.

### 9.2 Existing External Segments

Existing segments do not need data migration. They are patched lazily on the
next refresh when their fragment set is unchanged but their fake binlogs do
not cover the latest external fields.

### 9.3 Existing Query Load Path

QueryNode continues to load segments from manifest path and fake binlogs. The
patch updates those two inputs while preserving segment identity.

---

## 10. Test Coverage

The implementation should cover:

1. Refresh job records store schema version.
2. Generated refresh tasks copy job schema version.
3. Dispatch rejects a task if collection schema changed before dispatch.
4. Apply rejects a finished job if collection schema changed before apply.
5. DataCoord accepts same-ID updated segment patches.
6. DataCoord rejects invalid patches:
   - row count changes,
   - dropped segment patches,
   - wrong collection ID,
   - kept/updated overlap,
   - duplicate updated IDs,
   - empty manifest path,
   - empty fake binlogs,
   - fake binlog row-count mismatch,
   - schema version rollback.
7. DataNode keeps same-fragment segments when all external fields are covered.
8. DataNode patches same-fragment segments when newly added external fields
   are missing.
9. DataNode response returns unchanged segments through `kept_segments` and
   patched or new segments through `updated_segments`.
10. Manifest column group helpers deduplicate fragment reads and append only
    missing columns.
11. Manifest append is idempotent when requested columns already exist for the
    same fragment set.

---

## 11. Alternatives Considered

### 11.1 Rebuild Every Segment After AddField

Rebuilding every segment would be simple but expensive. It would also change
segment IDs for data whose external fragments did not change, causing
unnecessary reload and index-adjacent metadata churn.

### 11.2 Keep Same-Fragments Segments Unchanged

This preserves old behavior but leaves newly added external fields uncovered
by manifest column groups and fake binlogs. It does not satisfy AddField
semantics.

### 11.3 Patch Existing Segments Through A Dedicated RPC

A dedicated RPC would make the intent explicit but adds API surface and
duplicates refresh result handling. Reusing `updated_segments` as an upsert
payload keeps the refresh protocol compact and lets DataCoord enforce all
segment mutations in one path.

---

## 12. Open Constraints

1. This design supports additive external fields only.
2. A refresh retry is required after a schema-version race.
3. The manifest append path depends on storage-v2 Loon transaction support.
4. User-facing documentation should describe the supported AddField path only
   after the end-to-end API and SDK behavior are finalized.
