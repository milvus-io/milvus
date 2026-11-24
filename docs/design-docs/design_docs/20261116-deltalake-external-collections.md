# Delta Lake external collections

Use the Delta table root containing `_delta_log` as `external_source` and set
`external_spec.format` to `deltalake-table`. Delta uses the shared external
collection APIs, schema rules, virtual primary keys, storage configuration
(`extfs`), and manual refresh lifecycle. Refresh uses the primary broadcast path
and its messages stay local to that cluster, as with other external formats.
[External collection design](20260105-external_table.md)

```json
{"format":"deltalake-table","snapshot_id":"0"}
```

`snapshot_id` accepts an integer or decimal string in the int64 range.
Omission, null, and `-1` select the latest version during exploration.
Nonnegative values select an exact version. Queries continue reading the
imported view until refresh.

## Field types

The [Milvus external field allowlist](../../../pkg/util/typeutil/schema.go#L3889)
and the [Delta reader's Arrow types](https://github.com/milvus-io/milvus-storage/blob/f9138562ffa1c8567b7197518164374c9eb5e7fa/cpp/src/format/deltalake/README.md#schema-evolution)
both apply. Define Milvus fields with the corresponding source representation.

| Milvus field | Delta source representation |
| --- | --- |
| `Bool` | `boolean` |
| `Int8`, `Int16`, `Int32`, `Int64` | `byte`, `short`, `integer`, `long`, respectively |
| `Float`, `Double` | `float`, `double`, respectively |
| `VarChar`, `Text` | `string` or UTF-8 content in `binary` |
| `JSON` | Serialized JSON in `string` or `binary` |
| `Geometry` | WKT in `string` or WKB in `binary` |
| `Timestamptz` | `timestamp` or `long` containing epoch microseconds. `timestamp_ntz` is read as its epoch-microsecond value without timezone conversion. |
| `Array` | An `array` of a matching scalar type, with non-null elements |
| `FloatVector`, `Int8Vector` | `array<float>`, `array<byte>`, respectively, with exactly `dim` non-null elements per vector |
| `Float16Vector`, `BFloat16Vector`, `BinaryVector` | `binary` with the Milvus encoding, using `2 * dim` bytes for half-precision vectors or `dim / 8` bytes for packed bits |
| `SparseFloatVector` | A map from `integer` or `long` indices to `float` or `double` weights, an equivalent `indices` and `values` struct, or native Milvus sparse bytes in `binary` |

Dense vectors can also use correctly sized native bytes in a `binary` column.
The formats above apply to ordinary Delta arrays and binary columns. Delta has
no native half-float or unsigned byte type for the corresponding vector lists.

Milvus requires `ArrayOfVector` fields to be inside a struct array. Struct
array mappings are outside the Delta Lake coverage described here. Top-level
recursive scalar arrays and arrays with nullable elements are unsupported.
Delta `decimal`, `date`, `void`, and general maps and structs have no direct
Milvus field mapping. A map or struct used for a sparse vector must have the
specific shape above.

Delta `variant`, native geospatial types, file references, user-defined types,
and intervals are outside the pinned reader's supported schema. The reader
validates the full snapshot schema, so omitting such a field from the Milvus
projection still leaves the snapshot unsupported.

## Snapshot and fragment layout

milvus-storage resolves the snapshot and returns each file's path, size, and
live-row count, plus a descriptor containing `table_uri` and `snapshot_version`.
The explore manifest carries these properties to DataNode. The segment manifest
persists them for reopening. The resolved read version lives in each file
descriptor. The collection spec holds the selector supplied for exploration.

Each Delta file is one fragment covering its full live-row range. The reader
opens the pinned snapshot using the descriptor and the configured filesystem.
It uses the snapshot's column mapping, partition values, schema, and deletion
vector to expose logical rows. Files with zero live rows are omitted. A refresh
that finds no files fails through the shared refresh path and preserves the
previous segments. The source owner must retain the logs or checkpoints and
the data and deletion files needed by the versions Milvus still reads.
[milvus-storage Delta reader contract](https://github.com/milvus-io/milvus-storage/blob/f9138562ffa1c8567b7197518164374c9eb5e7fa/cpp/src/format/deltalake/README.md)

## Current fragment reuse

DataCoord assigns segments to refresh tasks by file path. DataNode compares
each segment's fragments with the newly explored fragments. A segment is
eligible for reuse when all its fragments match and it satisfies the shared
column and function-output checks. Surviving fragments from invalidated
segments are packed with newly added fragments into replacement segments.

Delta matches on the file path, live-row range, and a hash of all file
properties, including the snapshot pin. A refresh of the same snapshot can
retain a segment. A newer snapshot changes the pin on every explored file,
so even an append-only commit can rebuild existing segments and indexes and
change their virtual primary keys.

### Why file path and row range are insufficient

The Delta fragment range is `[0, live_row_count)` after deletion-vector
filtering. The path identifies the physical file and the range records how
many rows are visible. A new snapshot can change which rows are visible or
how their columns are interpreted while preserving both.

- **Deletion vectors.** One snapshot can hide physical row positions 0 and 1
  of a six-row file, and another can hide positions 2 and 3. Both have the
  same path and range `[0, 4)`. The first exposes rows 2, 3, 4, and 5. The
  second exposes rows 0, 1, 4, and 5. Delta records this change by removing
  the logical file with the old vector and adding the same path with the
  new vector. Reusing the old segment would preserve the wrong rows and
  row-offset mapping.
  [Delta file actions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file) ·
  [Milvus end-to-end regression](../../../tests/go_client/testcases/external_table_deltalake_e2e_test.go#L153)
- **Column mapping.** A mapped Delta table can drop a column and add a new
  nullable column with the same logical name. The new column has a fresh
  field ID and physical name. Older Parquet files can stay in the snapshot
  with the same live rows. The old snapshot reads the original column
  values from those files, while the new snapshot reads nulls because the
  new physical column is absent. A segment retained with its original
  snapshot pin would still return the old values, and any existing index
  on that field would describe those values.
  [Delta column mapping](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping)

Including the snapshot pin in fragment identity catches both cases.
Cross-snapshot reuse requires evidence that the same ordered rows and
logical values are visible to the Milvus collection.

## Proposed cross-snapshot reuse

Reuse matching fragments by advancing their snapshot pins in a new revision of
the segment manifest. The segment ID, row offsets, and existing indexes can
survive an append-only refresh. Reopening the refreshed segment then depends
on the newly explored snapshot.

### Per-file fingerprint

milvus-storage would emit a versioned fingerprint alongside each file's
existing read descriptor. Equal fingerprints must mean that the same file
yields the same ordered live rows and logical values under the same Milvus
field mappings. The fingerprint excludes the snapshot version so equivalent
files can match across snapshots. The descriptor still carries an exact
version for reads.

Compute the fingerprint in milvus-storage from the resolved file action and
the reader state that affects its interpretation. Include the table and
filesystem namespace, the immutable data-file identity, the partition values,
and the complete deletion-vector reference. For external vectors include the
location, offset, and byte length. For inline vectors include the bitmap
payload or its digest. Include the logical schema, physical column IDs and
names, partition types, relevant type changes, and protocol features that
affect reading. Start with a hash of the full logical schema and its mappings so
mapping changes and schema evolution invalidate reuse when their impact is
uncertain. Use deterministic encoding so property order does not affect the
fingerprint. This assumes immutable source files and deletion-vector ranges.
[Delta protocol for file actions](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file) ·
[Deletion vector descriptors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vector-descriptor-schema)

### Refresh and manifest update

1. **Explore the target snapshot.** Persist each file's fingerprint, live-row
   range, and read descriptor in the shared explore manifest. DataCoord keeps
   its path-based ownership plan. Task retries use the published plan and
   its resolved version.
2. **Match every fragment in a segment.** DataNode compares the files in the
   segment manifest with the explored files by path, live-row range, and
   fingerprint of the same supported version. The shared column and
   function-output checks still apply. An unproven match, including a missing
   fingerprint, falls back to the existing snapshot-sensitive comparison.
   If any fragment fails to match, rebuild the segment through the existing
   refresh path.
3. **Write a new manifest revision for a matching segment.** Replace the Delta
   file descriptors in every affected column group with the descriptors from
   the explore manifest. Preserve the manifest base, fragment order, row
   offsets, column groups, and references to compatible indexes, stats, and
   materialized function outputs. The manifest writer needs a file-property
   replacement operation that preserves other entries. If an index or stats
   writer advances the revision concurrently, revalidate the fragment match
   and apply the descriptor changes to the newer revision. DataNode returns the
   new manifest path in `updated_segments` with the original segment ID. A
   repeated refresh with the same descriptors can keep the existing revision.
4. **Publish through the existing refresh handoff.** DataCoord applies the
   same-ID manifest updates with the rest of the job's results and hands
   the new revisions to QueryView. Recovery resumes from persisted results.
   A failure before application leaves the old manifests published. Publication
   retries after application use the committed results.
   [Same-ID segment updates](20260526-external_table_add_column_refresh.md#42-updated-segments-as-upserts)

For example, a segment pinned to version 10 can keep its ID and indexes when
version 11 appends new files. Its new manifest points the original files at
version 11, and the added files go into new segments. Always use the exact
version selected by exploration, including when the user chooses an older
version. The Milvus manifest revision still advances.

### Reader and source retention

The Delta format reader keeps opening the exact snapshot in each manifest
descriptor. The fingerprint is used for refresh matching. Both the snapshot
context and file metadata caches continue to distinguish read versions. A
reader reopening the new manifest can resolve its files with the target
snapshot alone.

The source owner must keep the previous snapshot readable through handoff
and until readers using the old manifest have drained. Any other retained
views that still reference it extend that requirement. Once those references
are gone, the reused segments depend on the target snapshot and its referenced
objects.

## Validation

Milvus inherits the storage revision `f9138562ffa1c8567b7197518164374c9eb5e7fa`
and Rust 1.95.0 from upstream. That storage revision includes
[milvus-storage PR #801](https://github.com/milvus-io/milvus-storage/pull/801).
The initial Delta Lake unit and end-to-end runs passed on the original #801
revision `36c35e3cf0ef3d84953d02a4c9d3a52d5d83c8db`. The end-to-end suite
covers historical reads, same-count deletion-vector changes, column mapping,
partition values, and failed refreshes that preserve the previous view.

`TestExternalTableDeltaLakeFieldTypes` refreshes, indexes, loads, and queries
the supported Milvus types using generated Delta and Parquet data.

For cross-snapshot reuse, verify that append-only refresh retains segment IDs
and index artifacts, advances every retained descriptor, and cold reopens
after the previous snapshot becomes unreadable. Check that readers using the
old revision can finish during handoff. Cover manifests without fingerprints,
refresh failure and recovery around publication, and concurrent index or stats
updates. Deletion-vector, column-mapping, and partition-value changes must
invalidate the affected segments.
