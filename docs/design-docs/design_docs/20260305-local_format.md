# MEP: Local Format for Storage V3 Scalar Fields

- **Created:** 2026-03-05
- **Author(s):** @zhicheng
- **Status:** Under Review
- **Component:** QueryNode | DataNode | Storage
- **Related Issues:** milvus-io/milvus#50304
- **Released:** TBD

## Summary

Add a field-level `local_format` type parameter for sealed segment scalar data
loaded through Storage V3. The default value is `raw`, which keeps the existing
Milvus on-node raw chunk layout. The first alternate value is `vortex`, which
loads Vortex column group files through a cell-based local format path.

The proposal keeps the public field schema model small:

- `local_format=raw`: existing behavior.
- `local_format=vortex`: use Vortex local format when the Storage V3 manifest
  also points to a Vortex physical column group for that field.

Vortex local format is a read-path feature for sealed scalar fields. It does not
change growing segment execution, vector index execution, or the public query
language. It changes how QueryNode loads and scans sealed scalar column data.

## Motivation

Raw scalar chunks are simple and fast when fully resident, but they require
Milvus to materialize the field data in its raw on-node layout. This is costly
for large VARCHAR, JSON, ARRAY, and other scalar fields when a query only needs a
subset of rows or only needs a predicate result.

Vortex provides compressed, columnar files with row-group metadata and optional
zone maps. Local format support lets Milvus keep Vortex data in its native file
layout and materialize only the cells needed by scan or take operations.

The design goals are:

- Reduce sealed scalar field load memory for Storage V3 segments.
- Keep the existing raw path as the default and avoid adding copies to it.
- Let expression evaluation consume scalar data through a scan cursor instead
  of repeatedly materializing chunks.
- Pin Vortex data at a well-defined cell granularity in the Milvus cache layer.
- Keep the common `FormatReader` interface stable; Vortex-specific operations
  are exposed as Vortex extensions.
- Support normal filter, offset-input filter, and retrieve/requery output paths
  with clear and separate execution plans.

The non-goals for the initial implementation are:

- Vortex local format for vector fields.
- Vortex local format for primary-key fields.
- Changing the query expression language.
- Full predicate pushdown for every scalar expression.
- Bitmap/selection pushdown for offset-input execution.

## Public Interfaces

### Field Type Parameter

`local_format` is a field type parameter.

Valid values:

| Value | Meaning |
|-------|---------|
| `raw` | Default. Load sealed scalar data into the existing raw local format. |
| `vortex` | Prefer Vortex local format for this field when the physical Storage V3 column group is Vortex. |

Example schema intent:

```python
schema.add_field(
    field_name="description",
    datatype=DataType.VARCHAR,
    max_length=65535,
    type_params={"local_format": "vortex"},
)
```

SDKs may expose this as a direct field option, but the Milvus server stores and
validates it as a field type parameter.

Validation rules:

- Missing `local_format` means `raw`.
- `local_format=vortex` is accepted for non-primary-key, non-vector fields.
- Primary-key fields reject `local_format=vortex`.
- Vector fields reject `local_format=vortex`.
- Unknown values are rejected. The supported values are `raw` and `vortex`.

### Storage V3 Relationship

`local_format` is only effective for Storage V3 sealed segments.

For write-time column group planning, fields marked `local_format=vortex` are
partitioned away from fields using the raw local format. A column group whose
remaining fields all use Vortex local format is written with physical format
`vortex`; other column groups use the normal fallback storage format.

For read-time loading, Vortex local format is used only when both conditions are
true:

- all fields in the physical column group have `local_format=vortex`;
- the Storage V3 manifest says the physical column group file is Vortex.

If either condition is false, the segment uses the existing raw loading path for
that column group.

## Design Details

### High-Level Architecture

The design introduces `ColumnBasedInterface` as the column-oriented access
abstraction for sealed scalar fields. This is the main interface shift for
local format: Vortex data is not naturally owned as Milvus raw chunks, so the
new path moves callers from `ChunkedBase` chunk access to column-level
operations.

`Scan` is one operation under `ColumnBasedInterface`, used by expression
evaluation. Positional take/output operations are also part of the same
column-based abstraction and are used by retrieve and requery. The Vortex reader
consumes a sparse local file view behind these column-level operations.

Milvus is in a transition state where two access families coexist:

- `ChunkedBase` remains the raw chunk-oriented path for the existing raw local
  format and existing chunk consumers.
- `ColumnBasedInterface` is the local-format-aware path used by Vortex and by
  scan/take code that should not depend on physical chunk ownership.

Filter scan path:

```text
Expr
  -> ColumnBasedInterface::Scan(...)
  -> VortexColumn
  -> VortexPlanner
  -> VortexColumnGroup cache slot pin
  -> VortexFormatReader::read_with_plan / read_row_ids_with_plan
  -> Vortex scan builder
```

Retrieve/requery output path:

```text
Retrieve output / bulk_subscript
  -> ColumnBasedInterface positional take
  -> VortexColumn::Take...
  -> VortexPlanner::PlanForOffsets
  -> VortexColumnGroup cache slot pin
  -> VortexFormatReader::read_with_plan(row indices)
```

The key ownership split is:

- `milvus-storage` understands the Vortex file layout and maps row ranges,
  offsets, and predicates to Vortex read plans.
- Milvus QueryNode owns cache pinning, sparse-file lifecycle, expression cursor
  consumption, and output conversion.

### Column Group Splitting

Column group splitting partitions fields by `local_format` before subsequent
split policies finalize physical groups. This prevents raw and Vortex local
format fields from sharing one physical column group.

The split policy behavior is:

1. Partition pending fields by `local_format`.
2. Keep the partition's format metadata through later split policies.
3. Emit physical column groups with `Format=vortex` only for Vortex local format
   groups.
4. Leave raw groups with an empty format override so they use the configured
   fallback storage format.

System, vector, text, average-size, and remanent-short split policies still
apply after the local-format partitioning. They split within the current local
format partition instead of mixing formats.

### Cell Semantics

A cell is the cache and loading unit for Vortex local format. Cells are defined
by the Vortex file layout and exposed through `VortexPlanner`.

Vortex V1:

- There is no stable row-group boundary.
- A cell corresponds to a full flat physical unit.

Vortex V2:

- Row groups are available.
- A cell corresponds to a complete row group and its physical segments.
- Row-group boundaries must align for fields in the same physical column group.

General cell invariants:

- Cell ids are contiguous and start at zero within a file.
- Cell row ranges are contiguous and cover the file.
- Cells do not share physical segments.
- All fields in the same physical column group share a `VortexColumnGroup`; pinning
  a cell loads the underlying bytes once for all fields in that group.

### Storage-Side Vortex Interfaces

#### `VortexFooterReader`

`VortexFooterReader` reads Vortex file metadata. It is not responsible for data
scan, take, Milvus cache pinning, or cache lifetime.

Responsibilities:

- Open a Vortex file through a filesystem.
- Materialize the footer into the sparse local file.
- Optionally materialize V1/V2 zone-map segments.
- Expose schema, row count, footer size, field layout, row-group ranges, and
  physical byte ranges.
- Prune row groups using zone maps when they are loaded.

Lifecycle:

- `Open(fs, load_zonemap)` succeeds at most once per reader instance.
- `Open(false)` loads footer metadata only; pruning conservatively keeps all
  candidate row groups.
- `Open(true)` loads footer metadata, materializes zone-map bytes, and then
  reopens the final Vortex file view so Vortex's internal initial-read cache
  cannot retain sparse zero-filled zone-map bytes.

#### `VortexPlanner`

`VortexPlanner` converts logical Milvus access requests into two outputs:

```cpp
struct VortexPlan {
    std::vector<uint64_t> cell_ids;
    VortexReadPlan read_plan;
};
```

- `cell_ids` are used by Milvus to pin Vortex cells through the cache layer.
- `read_plan` is passed to `VortexFormatReader` for execution.

Supported planning modes:

- `PlanForRowRange(row_start, row_end, predicate)`
- `PlanForOffsets(offsets)`

For V2 row-group cells and supported predicates, the planner may use zone maps
to prune cells. For V1 files or unsupported predicates, it returns all candidate
cells conservatively.

#### `VortexFormatReader`

The common `FormatReader` interface remains compatible with existing callers.
Vortex local format uses Vortex-specific extensions:

- `read_with_plan(const VortexReadPlan&)`
- `read_row_ids_with_plan(const VortexReadPlan&)`

`read_with_plan` returns data as an Arrow stream. `read_row_ids_with_plan`
returns file-local row ids satisfying the predicate in the plan. Predicate state
is carried by `VortexReadPlan`, not by long-lived reader state. Existing
`set_predicate` behavior remains for compatibility but is not the local format
path.

### Milvus-Side Components

#### `FieldMeta`

`FieldMeta` parses `type_params["local_format"]` and defaults to `raw`. It also
serializes non-default local format back to the field schema.

#### `ColumnBasedInterface`

`ColumnBasedInterface` is the shared access contract for column-oriented scalar
data. It lets callers express the operation they need without assuming the data
is backed by raw Milvus chunks.

The interface covers two operation groups:

- scan operations for expression evaluation;
- positional take/output operations for retrieve, requery, and bulk_subscript.

`Scan` returns a cursor of `ScanBatch` values.

Scan outputs:

| Output | Payload |
|--------|---------|
| `ScanOutput::RowIds` | Sparse row ids that satisfy, or may satisfy, a pushed predicate. |
| `ScanOutput::Data` | Dense values over a row range, plus validity when needed. |

Data scan supports:

- row range;
- value kind (`FixedWidth`, `StringView`, `JsonView`, `ArrayView`);
- validity;
- validity-only projection.

Row-id scan supports:

- unary predicates;
- binary range predicates;
- sparse row-id batches.

If a column implementation cannot support a scan mode, it falls back to the
raw-compatible behavior through the existing chunked path.

#### `VortexColumnGroup`

`VortexColumnGroup` owns shared state for one physical Vortex column group.

Each file state contains:

- source filesystem and resolved source path;
- sparse filesystem and sparse path;
- `VortexFooterReader`;
- group-level `VortexPlanner`;
- cache slot and translator;
- row count and memory accounting.

All fields in the same physical group share the same `VortexColumnGroup`.

#### `VortexColumn`

`VortexColumn` is a field-level `ColumnBasedInterface` implementation over a
shared `VortexColumnGroup`.

Responsibilities:

- Resolve the Vortex field name. External fields use the external column name;
  internal fields use the field id string.
- Build a field-level projected Arrow schema.
- Create field-level planner/reader state.
- Implement `Scan`.
- Implement positional take helpers for retrieve output.

### Filter Scan

#### Predicate Pushdown

For supported unary and binary range expressions, expression execution requests
`ScanOutput::RowIds`.

Example:

```text
UnaryExpr / BinaryRangeExpr
  -> ColumnBasedInterface::Scan(ScanOutput::RowIds)
  -> VortexColumn::Scan
  -> VortexRowIdScanCursor
  -> VortexPlanner::PlanForRowRange(predicate)
  -> pin planned cells
  -> VortexFormatReader::read_row_ids_with_plan
  -> bitmap assembly in expression execution
```

The initial implementation supports a narrow set of predicate strings that can
be represented safely for the Vortex reader. Unsupported expressions fall back
to data scan. This keeps correctness independent of pushdown coverage.

#### Data Scan

Unsupported predicates, complex expressions, and expressions that need raw value
inspection use `ScanOutput::Data`.

Example:

```text
Expr data path
  -> ColumnBasedInterface::Scan(ScanOutput::Data)
  -> VortexDataScanCursor
  -> VortexPlanner::PlanForRowRange(no predicate)
  -> pin planned cells
  -> VortexFormatReader::read_with_plan
  -> expression layer evaluates predicate
```

This is the current path for examples such as `LIKE`, `IN`, JSON path
expressions, and array predicates when they cannot be represented as a Vortex
predicate.

### Offset Input Execution

Offset-input execution is used when expression evaluation is restricted to a
known set of segment offsets.

The initial Vortex local format implementation handles dense sorted offsets by
scanning one continuous range:

```text
ProcessDataByOffsets
  -> ProcessSortedDataByOffsetsByScan
  -> scan [min_offset, max_offset + 1)
  -> expression layer checks the offset bitmap
```

Bitmap or selection pushdown into `ColumnBasedInterface::Scan` is left as
future work. The current strategy avoids many small reads while keeping
semantics simple.

### Retrieve and Requery

Retrieve/requery output is not filter scan. It reads requested output fields at
selected row offsets.

```text
FillTargetEntry / Retrieve output
  -> bulk_subscript
  -> ColumnBasedInterface positional take
  -> VortexColumn::BulkPrimitiveValueAt / BulkRawStringAt / BulkArrayAt
  -> TakeOwn / TakeStringLikeViews
  -> VortexPlanner::PlanForOffsets
  -> pin planned cells
  -> VortexFormatReader::read_with_plan(row indices)
  -> restore requested output order
```

The planner disables predicate semantics for take because retrieve output is
positional. Random requery over long strings can still touch many cells; this is
tracked as a separate performance area from filter pushdown.

### Nullable and Validity

The `ColumnBasedInterface` scan API uses `ValidityView` to present nullability
uniformly.

Rules:

- Non-nullable fields may return all-valid validity.
- Nullable dense data scans must return validity aligned with the dense row
  range.
- Row-id scans may return validity aligned with sparse row ids.
- Validity-only projection is part of the scan model so callers that only need
  nullability do not need to materialize full values.

The raw path may adapt its existing `bool*` validity representation into this
model. Vortex uses Arrow bitmap/null-buffer semantics.

### Sparse Local File and Cache Loading

Vortex local format uses a sparse local file as the file view consumed by the
Vortex reader.

Flow:

```text
VortexFooterReader
  -> materialize footer and optional zone-map bytes into sparse file

VortexPlanner
  -> choose cell ids and read plan

Milvus cache layer
  -> pin cells
  -> Vortex translator loads cell byte ranges into sparse file

VortexFormatReader
  -> reads the sparse file as a normal file
```

Properties:

- Loaded byte ranges are written to the sparse file.
- Missing ranges remain sparse holes and read as zero-filled bytes if an
  over-wide read crosses them.
- Footer bytes are always materialized before planning.
- Zone-map bytes are materialized when pruning is enabled.
- Cell lifecycle remains controlled by Milvus cache pin/unpin.

### Warmup and Eviction

Vortex local format reuses Milvus cache warmup policy. Warmed scalar fields can
load their cells during segment load, reducing the first-query penalty. Manual
eviction and warmup cancellation are implemented at the `VortexColumnGroup`
level so all field proxies in the same physical group share the same state.

## Compatibility, Deprecation, and Migration Plan

- Backward compatible by default: fields without `local_format` behave as
  `raw`.
- Existing non-Vortex segments continue to load through the raw path.
- A schema can contain both raw and Vortex local format fields; column group
  splitting keeps them physically separate.
- During the transition, QueryNode keeps both access paths: raw fields continue
  to use the `ChunkedBase` chunk-oriented path, while Vortex fields use the
  `ColumnBasedInterface` column-oriented path.
- Vortex local format is only used for Storage V3 sealed segments.
- Rolling upgrades must ensure QueryNodes understand Vortex local format before
  new Vortex column groups are loaded. Older readers cannot load Vortex physical
  column groups.

## Test Plan

System and integration validation:

- Create collections with raw fields, Vortex local format fields, and mixed
  fields; verify insert, flush, load, search, query, and retrieve.
- Verify `local_format=vortex` is rejected for primary-key and vector fields.
- Verify Storage V3 manifests with Vortex physical column groups load as
  `VortexColumnGroup + VortexColumn`.
- Verify non-Vortex physical files continue to load through the raw path.
- Run scalar filter benchmark cases for primitive predicates, complex
  expressions, offset-input execution, and retrieve/requery output.

Unit and component validation:

- `FieldMeta` parse/serialize of `local_format`.
- Column group split policy keeps raw and Vortex fields separate.
- `ColumnBasedInterface` scan and positional take behavior.
- Raw `ChunkedBase` path and Vortex `ColumnBasedInterface` path coexist without
  changing raw field behavior.
- `VortexColumn` row-id scan, data scan, validity, and take paths.
- `VortexFooterReader` footer and optional zone-map lifecycle.
- `VortexPlanner` row range, offset, and predicate pruning plans.
- `VortexFormatReader::read_with_plan` and `read_row_ids_with_plan`.

Performance validation:

- Compare Vortex and raw local format for cold and hot retrieve.
- Compare Vortex and raw local format for primitive filter scan.
- Track complex data scan cases such as JSON, ARRAY, and `LIKE`.
- Track random retrieve/requery over long VARCHAR because it exercises take and
  output conversion rather than filter scan.

## Future Work

- Push offset bitmaps or row selections into `ColumnBasedInterface::Scan`.
- Expand Vortex predicate construction for more scalar types and expression
  forms.
- Optimize long string, JSON, and ARRAY retrieve/take conversion paths.
- Use validity-only scan in paths that only need nullability.

## References

- [Milvus PR: support vortex local format](https://github.com/milvus-io/milvus/pull/49908)
- [Milvus issue: vortex local format](https://github.com/milvus-io/milvus/issues/50304)
- [milvus-storage](https://github.com/milvus-io/milvus-storage)
- [Vortex project](https://github.com/vortex-data/vortex)
