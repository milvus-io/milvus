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

- Missing `local_format` uses the server default, which initially resolves to
  `raw`.
- `local_format=vortex` is accepted for non-primary-key, non-vector fields.
- Primary-key fields reject `local_format=vortex`.
- Vector fields reject `local_format=vortex`.
- Unknown values are rejected. The supported values are `raw` and `vortex`.

### Storage V3 Relationship

`local_format` is only effective for Storage V3 sealed segments.

For write-time column group planning, fields with a default (empty), `raw`, or
`vortex` local format are partitioned away from each other. The default value
remains a separate partition so a future server-level default can be changed
without mixing fields that explicitly requested `raw` or `vortex`.

Local format does not select the physical writer format. New column groups use
the writer's configured format, and existing column groups preserve the format
recorded in the Storage V3 manifest.

For read-time loading, Vortex local format is used only when both conditions are
true:

- all fields in the physical column group have `local_format=vortex`;
- the Storage V3 manifest says the physical column group file is Vortex.

If either condition is false, the segment uses the existing raw loading path for
that column group.

## Design Details

### High-Level Architecture

The design extends `ChunkedColumnInterface` with column-oriented scan and
positional access for sealed scalar fields. This is the main interface shift for
local format: Vortex data is not naturally owned as Milvus raw chunks, so the
new path moves callers from `ChunkedBase` chunk access to column-level
operations.

`Scan` is one operation under `ChunkedColumnInterface`, used by expression
evaluation. Positional take/output operations are also part of the same
column-based abstraction and are used by retrieve and requery. The Vortex reader
consumes a sparse local file view behind these column-level operations.

Milvus is in a transition state where two access families coexist:

- `ChunkedBase` remains the physical raw-chunk interface for existing consumers
  that explicitly need chunk ownership.
- `ChunkedColumnInterface` is the local-format-aware path used by scan/take
  code. Raw columns implement `Scan` as zero-copy views over their existing
  chunks, while Vortex columns implement it with reader-backed cursors.

Filter scan path:

```text
Expr
  -> ChunkedColumnInterface::Scan(...)
  -> VortexColumn
  -> VortexPlanner
  -> VortexColumnGroup cache slot pin
  -> VortexFormatReader::read_with_plan / read_row_ids_with_plan
  -> Vortex scan builder
```

Retrieve/requery output path:

```text
Retrieve output / bulk_subscript
  -> ChunkedColumnInterface positional take
  -> VortexColumn::Take...
  -> VortexPlanner::PlanForOffsets
  -> VortexColumnGroup cache slot pin
  -> VortexFormatReader::take(file-local offsets)
```

The key ownership split is:

- `milvus-storage` understands the Vortex file layout and maps row ranges,
  offsets, and predicates to Vortex read plans.
- Milvus QueryNode owns cache pinning, sparse-file lifecycle, expression cursor
  consumption, and output conversion.

### Column Group Splitting

Column group splitting partitions fields by the default (empty), `raw`, or
`vortex` `local_format` value before subsequent split policies finalize physical
groups. This gives each physical group an unambiguous local-format intent while
keeping physical writer-format selection independent.

The split policy behavior is:

1. Partition pending fields by the exact `local_format` value, keeping default
   (empty), `raw`, and `vortex` separate.
2. Keep the partition boundary through later split policies.
3. Leave the resulting column group's writer `Format` unset so normal writer
   configuration or existing manifest metadata determines the physical format.

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
- `take(const std::vector<int64_t>& offsets)`

`read_with_plan` returns data as an Arrow stream. `read_row_ids_with_plan`
returns file-local row ids satisfying the predicate in the plan. Predicate state
is carried by `VortexReadPlan`, not by long-lived reader state. Existing
`set_predicate` behavior remains for compatibility but is not the local format
path. For positional Take, the footer planner selects the Cells to pin, while
`take` receives file-local offsets and returns fully materialized Arrow data.

### Milvus-Side Components

#### `FieldMeta`

`FieldMeta` parses `type_params["local_format"]` and defaults to `raw`. It also
serializes non-default local format back to the field schema.

#### `ChunkedColumnInterface`

`ChunkedColumnInterface` is the shared access contract for column-oriented scalar
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
- value kind (`FixedWidth`, `StringView`, `JsonView`, `ArrayView`,
  `VectorArrayView`);
- validity;
- nullable-only validity projection.

`ScanBatch::validity` has one evaluator-facing representation: a batch-relative
`ValidityView`. An empty view means every row in the batch is valid. A non-empty
view may reference either one-byte-per-row expanded booleans or an LSB-first
packed bitmap with a bit offset; indexing remains relative to the returned dense
rows or sparse row ids in both cases. Raw scans reference the validity already
owned by the pinned Chunk or Span. Vortex scans reference the Arrow null bitmap
directly, including the Arrow array offset and the returned batch position,
instead of expanding it into `FixedVector<bool>`. `ScanBatch::owner` retains the
pin, Chunk, Arrow reader/array, and any derived values required for both the
validity view and value payload to remain alive for the batch lifetime.

Every Column generation owns exactly one immutable `ColumnPlanner`. It is the
only layer that translates expression ranges into Cell locations and provides
the default Raw mapping from segment offsets to Cell-local addresses:

- `PlanTake` preserves offset order and duplicates while producing the Cell id
  and Cell-local offset used by the default Raw Take implementation. Take does
  not apply SkipIndex filtering;
- `PlanScan` clips an expression range into ordered Cell ranges and evaluates
  each preloaded Cell decision once;
- execution supplies the expression-specific skip predicate for Scan and
  consumes the resulting plan, but does not call `GetChunkIDByOffset`,
  calculate Cell ends, or cache Cell decisions itself;
- Raw cursors use the same planner for their physical position, while the
  Vortex planner facade delegates physical read-plan construction to the
  footer-backed per-file planners owned by the ColumnGroup.

The statistics snapshot remains generation-stable execution input. It is not
mutated into a reused Column during reopen; the Column-owned planner invokes a
predicate bound to that immutable snapshot.

`Scan(ScanOptions)` creates one persistent logical cursor for the expression
leaf without pinning the complete remaining range. Each
`Next(position, length, mode, out)` request uses an absolute segment position
and an upper-bound length. The cursor locates the requested Cell or reader
range, pins only the resources needed for the returned batch, and may stop at a
Raw Cell or backend batch boundary. Validity-only mode is accepted only by
nullable data scans, omits values, and may avoid data parsing when the backend
can provide validity directly.

With `ScanPinPolicy::PerCall`, the cursor retains no Cell between calls; each
returned `ScanBatch::owner` keeps its own Cell pin, batch-local values, and
normalized validity alive until that batch is released. With
`ScanPinPolicy::UntilCellExhausted`, the cursor may additionally retain the
current file/Cell plan across calls. An identical next plan reuses that pin;
when the plan changes, the cursor releases the old plan before pinning the new
one. The expression layer does not separately pin Cells or reopen the cursor
between windows.

Row-id scan supports:

- unary predicates;
- binary range predicates;
- sparse row-id batches.

Every sealed scalar column used by expression evaluation must provide the
raw-compatible data scan. A missing sealed scan implementation is a column
contract violation rather than a per-batch fallback. Growing and non-chunked
segments continue to use their existing chunk access path.

#### `VortexColumnGroup`

`VortexColumnGroup` owns shared state for one physical Vortex column group.

Each file state contains:

- source path for diagnostics and its segment row range;
- sparse filesystem and sparse path;
- `VortexFooterReader`;
- per-field footer planners;
- cache slot, which owns the Cell translator;
- planner memory accounting.

The source filesystem, resolved source path, group planner, and validated file
row count are construction-local and are not retained in `FileState`.

All fields in the same physical group share the same `VortexColumnGroup`.

#### `VortexColumn`

`VortexColumn` is a field-level `ChunkedColumnInterface` implementation over a
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
  -> ChunkedColumnInterface::Scan(ScanOutput::RowIds)
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
  -> ChunkedColumnInterface::Scan(ScanOutput::Data)
  -> VortexDataScanCursor
  -> VortexPlanner::PlanForRowRange(no predicate)
  -> pin planned cells
  -> VortexFormatReader::read_with_plan
  -> expression layer evaluates predicate
```

This is the current path for examples such as `LIKE`, `IN`, JSON path
expressions, and array predicates when they cannot be represented as a Vortex
predicate.

Each expression leaf creates one persistent cursor with `Scan(ScanOptions)`.
For every expression window it calls
`ScanCursor::Next(position, length, mode, out)` with an absolute segment
position. `length` is an upper bound: a Raw Cell or reader boundary may produce
a shorter dense batch, and the expression layer continues from the returned
batch end. A greater `position` advances the same cursor without reading the
intervening rows.

The expression layer keeps both its segment-global execution position and the
existing chunk id/in-chunk offset cursor synchronized. Scan uses the global
position to describe its logical row window, while legacy Raw and Growing reads
continue from the chunk cursor. For Vortex, that cursor describes only the
logical file range and local row offset; maintaining it does not create a Raw
Chunk, pin a Cell, or open a reader. Normal evaluation and conjunction
short-circuit advance both representations. `Next` pins the Cell required by
the requested position. The returned batch and the configured scan pin policy
define how long that Cell stays pinned; the expression layer does not manage
Cell pins separately or reopen the cursor between windows.

### Offset Input Execution

Offset-input execution is used when expression evaluation is restricted to a
known set of segment offsets.

Offset input uses positional `Take`, not a dense range `Scan`:

```text
ProcessDataByOffsets
  -> ChunkedColumnInterface::Take(TakeOptions{segment_offsets})
  -> consume one ordered TakeResult
  -> evaluate exactly the requested rows in input order
```

Take keeps segment offsets as its public coordinate and does not apply either
preloaded or loaded-payload SkipIndex filtering. The default Raw implementation
uses `ColumnPlanner::PlanTake` to convert those offsets into ordered source Cell
ids and Cell-local offsets without pinning payload. Backends with a different
physical addressing model may override `Take(TakeOptions)` and consume the
segment offsets directly.

The public contract preserves plan order and duplicate offsets. `Take` is a
synchronous operation over one finite plan and returns one `TakeResult`. `Get(i)`
accesses the ith logical result, while `GetOwn()` exposes an ordered dense result
whose lifetime is independent of backend Cell pins.

Raw consumes the planned Cell locations without resolving offsets again.
Borrowed access pins only the Cell containing the requested row and reuses that
pin while later accesses remain in the same Cell. Switching Cells replaces the
pin; result destruction releases the last pin. Fixed-width `Get(i)` returns a
value copied from the pinned Chunk. A string/JSON/array view is valid until the
next access that switches Cells, `GetOwn()`, or result destruction. `GetOwn()`
groups positions by Cell, pins each Cell once while copying into the ordered
owned result, and then releases the borrowed pin.

Vortex sorts, groups, and deduplicates segment offsets by the immutable ordered
file table to reduce reader work. Its per-file footer planner is used only to
select the Cells that must be pinned; the file-local offset conversion remains
an internal reader detail rather than part of the public Take plan. Vortex then
restores the original input order in an already-owned decoded result. These
physical details do not cross the `TakeResult` boundary.

Generated columns implement the same logical interface without entering either
storage backend. In particular, `VirtualPKChunkedColumn` computes Scan batches
and planned Take values directly from `(segment id, row offset)` and returns
owned INT64 data. Its Scan/Take path does not pin a Raw Cell, open a Vortex
reader, or materialize the synthetic full-column Chunk retained for legacy
Chunk consumers.

### Retrieve and Requery

Retrieve/requery output is not filter scan. It reads requested output fields at
selected row offsets.

```text
FillTargetEntry / Retrieve output
  -> bulk_subscript
  -> ChunkedColumnInterface::Take(offsets)
  -> ordered TakeResult
  -> GetOwn(), copy, or serialize into the final owned result
```

For Vortex, `PlanForOffsets` selects and pins Cells only while the reader imports
and materializes the requested data. The returned result owns the materialized
Arrow/copied data rather than retaining Cell pins. Raw read-only expression Take
keeps at most its current Cell pinned, while retrieve/requery uses `GetOwn()` or
serializes each borrowed value before advancing. Random requery over long
strings can still cause frequent Cell transitions and remains a separate
performance area from filter pushdown.

### Nullable and Validity

The `ChunkedColumnInterface` scan API presents nullability uniformly through
`ScanBatch::validity`.

Rules:

- Non-nullable or all-valid batches return an empty `ValidityView`.
- Nullable dense data scans must return validity aligned with the dense row
  range.
- Row-id scans may return validity aligned with sparse row ids.
- Validity-only projection is restricted to nullable data scans so callers
  that need the stored nullability do not materialize full values.
  Non-nullable columns reject this mode; their all-valid representation is
  only returned alongside normal data batches.

Raw cursors expose their existing expanded or packed Chunk validity through
`ValidityView`. Vortex cursors expose the relevant Arrow null-bitmap slice as a
packed `ValidityView` without materializing a per-row boolean mask. Callers use
the common view contract and do not depend on the backing encoding; the batch
owner keeps the referenced storage alive through consumption.

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

- Backward compatible by default: fields without `local_format` use the server
  default, which initially behaves as `raw`.
- Existing non-Vortex segments continue to load through the raw path.
- A schema can contain default (empty), raw, and Vortex local format fields;
  column group splitting keeps the three intents physically separate.
- During the transition, existing raw storage and non-scan chunk consumers are
  unchanged. Sealed scalar expression evaluation uses the same
  `ChunkedColumnInterface::Scan` contract for both raw and Vortex columns.
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
- `ChunkedColumnInterface` scan and positional take behavior.
- Raw `ChunkedBase` path and Vortex `ChunkedColumnInterface` path coexist without
  changing raw field behavior.
- `VortexColumn` row-id scan, data scan, validity, and take paths.
- `VortexFooterReader` footer and optional zone-map lifecycle.
- `VortexPlanner` row range, offset, and predicate pruning plans.
- `VortexFormatReader::read_with_plan`, `read_row_ids_with_plan`, and `take`.

Performance validation:

- Compare Vortex and raw local format for cold and hot retrieve.
- Compare Vortex and raw local format for primitive filter scan.
- Track complex data scan cases such as JSON, ARRAY, and `LIKE`.
- Track random retrieve/requery over long VARCHAR because it exercises take and
  output conversion rather than filter scan.

## Future Work

- Push offset bitmaps or row selections into `ChunkedColumnInterface::Scan`.
- Expand Vortex predicate construction for more scalar types and expression
  forms.
- Optimize long string, JSON, and ARRAY retrieve/take conversion paths.
- Use validity-only scan in paths that only need nullability.

## References

- [Milvus PR: support vortex local format](https://github.com/milvus-io/milvus/pull/49908)
- [Milvus issue: vortex local format](https://github.com/milvus-io/milvus/issues/50304)
- [milvus-storage](https://github.com/milvus-io/milvus-storage)
- [Vortex project](https://github.com/vortex-data/vortex)
