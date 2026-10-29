# Array Element-Level Null

## Scope

This design extends row-level null support to elements of scalar `Array` and
`ArrayOfVector`, and to the inner Array and leaf of `Array<Array<T>>`.
`FieldSchema.nullable` controls the whole row, `element_nullable` controls the
first Array element, and the nested scalar leaf uses its `TypeSchema.nullable`.

The design targets Storage V2 and V3. Storage V1 cannot store the new
formats, and the check runs only when a V1 insert binlog is written: on
Storage V1, creating the collection and inserting succeed, and the flush
fails with ErrStorage. The check covers every field of the collection, so one
element-nullable or nested Array field fails the flush of the whole segment;
it also rejects a row-nullable `ArrayOfVector`, such as a vector sub-field of
a nullable struct.
Nested Arrays, vector Arrays, and element-nullable Arrays are allowed only as
`StructArrayField` sub-fields. Top-level `Array<nullable T>`,
`Array<Array<T>>`, and `Array<Vector>` are rejected. Nesting is limited to
`Array<Array<scalar leaf>>`; the leaf cannot be a vector, and a Struct object
cannot itself be null.

## Semantics

These semantics apply to row-level filters, element-level filters
(`MATCH_*`, `element_filter`) and search on new-format struct sub-fields.
[Query and Index](#query-and-index) lists the supported forms.

Row validity is shared by all sub-fields of a struct, and `s IS NULL` on the
struct field tests the whole row. An empty array and an array containing only
null elements are both non-null rows. `IS NULL` / `IS NOT NULL` on a
sub-field (`s[b]`, `s[i][b]`) is rejected; whether one element is null is
tested with `$[b] IS NULL` inside `MATCH_*` or `element_filter`. A subscript
`s[i][b]` evaluates to `NULL` when the row is null, the index is out of range,
or the selected element is null:

| State | `s[i][b] > 1` | `not (s[i][b] > 1)` |
| --- | --- | --- |
| Null row | `NULL` | `NULL` |
| Index out of range | `NULL` | `NULL` |
| Null element | `NULL` | `NULL` |
| Non-null element `x` | `x > 1` | `not (x > 1)` |

Here `NULL` means an unknown (UNKNOWN) predicate result, not a valid `false`;
`&&`, `||` and `not` follow three-valued logic, and a filter keeps only rows
whose result is `true`. Comparisons, term tests, arithmetic and string
predicates on an accessed element propagate that result. `array_contains*`
on a non-null Array value matches no null slot and is only `true` or
`false`; on an UNKNOWN operand (a null row, or a missing or null inner array
`s[i][tags]`) it is UNKNOWN. Null vectors do not participate in similarity
search. `array_length` and capacity limits count logical slots, including
null slots.

An empty `in` set is the exception to UNKNOWN propagation: `x in []` is
`false` for every `x`, including a null row, an out-of-range subscript and a
null element, so `not (x in [])` is `true`. This holds for `s[i][b] in []`,
`$[b] in []` and the subscript of a top-level Array, `arr[i] in []`. A
non-empty set follows the table above.

## Representation and Data Flow

The existing `ScalarField` and `VectorField` messages carry `valid_data` for
their immediate logical values; no nullable wrapper messages are needed.
Row validity is written to both `FieldData.valid_data` and the outer
`ScalarField.valid_data` or `VectorField.valid_data`, with identical bits.
Proxy rejects a request whose two copies differ; inside Milvus, readers take
`FieldData.valid_data` whenever it is set.
Only the corresponding inner `ScalarField.valid_data` or
`VectorField.valid_data` carries element or leaf validity. Go storage
containers keep row validity separate from child validity.

For each nullable inner level, the bitmap has one entry per logical value.
At the Proxy input boundary, payloads at that level are compact:

```text
len(child.valid_data)             = logical element count
count(child.valid_data == true)   = physical payload element count
```

Proxy validates this relationship before normalization, including at both
inner levels of a nested scalar Array, and rejects `valid_data` on a level
that is not nullable. Row payloads are also compact; Proxy
restores null row positions without changing child validity. Scalar levels
become dense (one payload or placeholder per logical position), while vector
payloads remain compact:

| Type | Logical value | Input payload | Payload after Proxy |
| --- | --- | --- | --- |
| Scalar Array | `[10, null, 30]` | `[10, 30]` | `[10, 0, 30]` |
| Nested scalar Array | `[[10, null], null, []]` | `[[10], []]` | `[[10, 0], [], []]` |
| ArrayOfVector | `[vec0, null, vec2]` | `[vec0, vec2]` | `[vec0, vec2]` |

The single-level scalar and vector examples retain child validity
`[true, false, true]`. In the nested example, the outer child validity is
`[true, false, true]` and the first inner Array's leaf validity is
`[true, false]`. Each null scalar leaf gets a zero value (`0`, `""`, or
`false`); a null inner Array gets an empty `ScalarField` of the leaf type.
The null and empty inner Arrays have the same empty payload and differ in the
parent validity. Non-nullable levels omit `valid_data`.

For example, after Proxy normalization the nested row above has this proto
shape (the `FieldData`/outer-message row validity is shown separately):

```text
FieldData.valid_data = [true, ...]
FieldData.scalars.valid_data = [true, ...]  // identical row bits
FieldData.scalars.array_data.data[row] = ScalarField{
  ArrayData: ArrayArray{ElementType: Int64, Data: [
    ScalarField{LongData: [10, 0], valid_data: [true, false]},
    ScalarField{LongData: []},  // null inner Array: empty Int64 payload
    ScalarField{LongData: []}   // non-null empty inner Array
  ]},
  valid_data: [true, false, true]  // inner Array validity for this row
}
```

The row-level `valid_data` above belongs to the outer field message; the
per-row `ScalarField` inside `ArrayArray.data` carries the inner Array
validity. For `ArrayOfVector`, each row's `VectorField.valid_data` counts
logical vector elements, but the vector payload contains only valid vectors.
A null row uses the existing `NewEmptyArrayOfVectorRow` placeholder.

The insert-to-query flow is:

```text
Insert request -> Proxy validation and normalization -> WAL
  -> flusher -> Storage V2 (Arrow -> Parquet / Vortex)
  -> QueryNode load -> runtime data -> queries and indexes
```

WAL preserves the normalized payload without interpreting element nulls.
Sorting, merging, flattening, and retrieval must preserve each element's
logical position and validity together with its value.

Storage V2 selects the physical format by the full field schema:

| Field | Arrow representation | Status |
| --- | --- | --- |
| Single-level scalar Array, non-nullable elements | One `Binary` per row containing a serialized `ScalarField` | Existing format |
| Vector Array, non-nullable elements | `list<item: fixed_size_binary[dim*width] nullable>` | Existing format |
| Single-level scalar Array, nullable elements | `list<item: T nullable>` | New format |
| Vector Array, nullable elements | `list<item: binary nullable>` | New format |
| Nested scalar `Array<Array<T>>`, whether or not its inner levels are nullable | `list<item: list<item: T> >` | New format |

For example, nested Int64 with non-nullable inner levels is
`list<item: list<item: int64 not null> not null>`; the top-level field has
its own row nullable flag.

The top-level Arrow field's nullable flag equals `FieldSchema.nullable`.
The first list's `item` nullable flag equals `FieldSchema.element_nullable`;
the nested leaf's flag equals the leaf `TypeSchema.nullable`. The vector
`list<Binary>` item is nullable. The existing `list<FixedSizeBinary>` item is
also declared nullable (Arrow's default list child, in both Go and C++), even
though its elements are never null. Every list child field is named `item`.

The scalar leaf type is one of Bool, Int8, Int16, Int32, Int64, Float, Double
and VarChar. Leaf `T` uses native widths in Go and C++: Bool→`bool`, Int8→`int8`,
Int16→`int16`, Int32→`int32`, Int64→`int64`, Float→`float32`,
Double→`float64`, VarChar→`utf8` (Go `arrow.BinaryTypes.String`, C++
`arrow::utf8()`). In particular, Int8 and Int16 are not widened to int32 as
they are in the proto representation.

`list<Binary>` preserves null vector positions without allocating a full
vector placeholder. Each non-null child must match the schema's vector width;
serialization maps compact vectors to logical child positions and reading
restores compact vectors and element validity.

The Parquet writer applies column properties to the converted leaf path
(for example, `vectors.list.element`; parquet-arrow renames the Arrow `item`
child to `element` in the Parquet schema): `FixedSizeBinary` leaves are
`UNCOMPRESSED` with statistics disabled, `Binary` leaves have statistics
disabled while retaining the caller's compression, and other leaves retain
their normal properties.

A null list row or null inner list must have a zero-length child segment
(repeated offsets). Parquet rejects a null list with a non-empty child
segment; Vortex does not check it. Arrow list copies, including compaction
builder copies and slices, must preserve this invariant.

## Schema and Runtime Routing

Only nested scalar Arrays carry a wire `TypeSchema`:
`data_type == Array` and `element_type == Array`; a single-level Array that
carries one is rejected. The server sets the root node's `nullable` to
`FieldSchema.nullable` without checking the value the request sent. The
second node's `nullable` must equal `FieldSchema.element_nullable`, or the
request is rejected. The leaf node's `nullable` is independent.
Single-level fields use `nullable`, `element_nullable`, `data_type`, and
`element_type` without a wire `TypeSchema`. Schema alter APIs cannot change
`nullable` or `element_nullable`.

In C++, new-format scalar fields (`list<T>` and `list<list<T>>`) load into
`ColumnarArrayChunk` directly from Arrow `ListArray`, with leaf validity and
native-width values; they do not pass through proto Binary. Existing scalar
proto Binary fields continue to use `ArrayChunk`. Both vector Array formats
load into `VectorArrayChunk`, which retains compact vectors and element
validity. For a single-level element-nullable scalar Array, `FieldMeta`
synthesizes an internal TypeSchema so `ColumnarArrayChunk` and `ArrayValue`
use the same representation for single-level and nested fields. Growing
segments store these scalar fields as one `ColumnarArrayChunk` per insert or
load batch written into a growing chunk and read rows as `ArrayValueView`;
vector Arrays are stored per
batch as well and read as `VectorArrayView` (see
[Recursive Array Columnar Layout](./20260724-recursive-array-struct-column-layout.md#growing-segments)).
The growing-source flush
path (`FlushGrowingSegmentData`/`AllowGrowingSourceFlush`) does not write
these formats. It is in use when `common.storage.enableGrowingSourceFlush`
is on and StorageV3 (`common.storage.useLoonFFI`) is enabled; then the proxy
rejects create collection, add struct field and add struct sub-field
requests that carry an element-nullable or nested Array field. An added
struct sub-field is always element-nullable, so adding one is always rejected
in that configuration. The check runs only on these DDL requests.

## Upsert

- An upsert that omits an element-nullable sub-field, or an insert that does,
  gets null elements in it (see
  [Adding and dropping sub-fields](./20260930-struct-subfield-evolution.md#producing-the-n-null-elements-value)).
- A partial upsert that omits a struct keeps the old struct value, null
  elements included: Proxy converts the queried value, which is dense, back
  to the compact input layout before the merged rows are validated.
- Replacing a whole struct in a partial upsert fills each omitted
  element-nullable sub-field with null elements. A non-nullable struct with
  no payload is rejected.
- `PATH_REPLACE` on a struct element or child accepts null elements and
  element-nullable nested Array children; see
  [Path Replacement](./20260901-array-positional-partial-update.md#element-nullable-struct-children).

## Query and Index

Ordinary filters produce row bits; `element_filter` and `MATCH_*` child
expressions produce element bits. Both apply row, bounds and element validity
as described in [Semantics](#semantics).

### Element ids

Outside the index and brute-force internals, element ids are logical: every
element of a non-null row takes an id, null elements included, and a null row
takes none. This is the numbering of `StructElementOffsets`, so
`element_filter` bits, element-level search results and the element indices
returned to the user refer to the same logical positions.

### Vector sub-fields

An element-nullable `ArrayOfVector` sub-field is indexed and searched at
element level with element-level metrics (L2, IP, COSINE, and others). Search
and hybrid search accept a plain vector placeholder on it. MAX_SIM-family
metrics, emb-list indexes and EmbeddingList placeholders are rejected;
autoindex does not map those fields to a MAX_SIM default. Like any vector
field, the sub-field needs an index before a load that includes it.
CreateIndex has no separate list of index types for these fields: under an
element-level metric the index type only has to support the element vector
type. HNSW and DiskANN builds are covered by tests.

- Index build: embedding-list mode is used only under an embedding-list
  metric. Under an element-level metric the memory and DiskANN materializers
  pass the flattened stored vectors without embedding offsets and, for an
  element-nullable field, an element-domain validity bitmap with one bit per
  logical element, so the knowhere IdMap returns logical element ids. A
  segment whose elements are all null has a valid element count of 0, below
  the index-build threshold, so DataCoord finishes its index task without a
  build and search on that segment uses brute force. Only when the threshold
  is skipped (a manifest-backed segment, below) or configured to 0 or less
  does the worker build an IdMap-only index for it. A segment without
  elements is skipped as empty. At load, the row-count and validity-length
  checks do not apply to element-level VECTOR_ARRAY indexes.
- Brute force: an element-nullable chunk stores only valid vectors. Sealed
  columns build and cache, per chunk, the logical element id of each stored
  vector; growing search computes the ids per searched range. The ids are
  attached to the bitset, so filtering and results use logical element ids.
  The cached search iterator does the same.
- Index-build threshold: DataCoord finishes a VectorArray index task without a
  build when the segment holds fewer vectors than
  `indexCoord.segment.minSegmentNumRowsToEnableIndex` (default 1024); an
  embedding-list index also needs that many rows. Binlog writers (flush and
  the packed StorageV2/V3 writers used by compaction, import and sort stats)
  record `Binlog.field_valid_element_counts` for every VectorArray field: the
  number of non-null elements in the non-null rows of the binlog, zero
  included. Only element-nullable fields use the count for the threshold.
  When `Statistics` is aggregated
  from binlogs, `Statistics.valid_element_counts` sums them per field and has
  an entry only when every binlog of the field recorded a count. Bump-schema
  additive reconciliation writes no per-binlog count; it sends the counts of
  the fields it materializes directly as a `Statistics` delta that DataCoord
  adds onto the segment. An element-nullable VectorArray with an entry uses it
  for the threshold. Without an entry (older data), DataCoord falls back to
  the estimate from binlog bytes, which counts the offset and validity bytes
  of null elements as vectors; a sub-field absent from an older-schema
  segment counts as zero vectors; a manifest-backed segment whose in-memory
  binlogs do not list the field skips the vector-count threshold and leaves
  the decision to the worker build.
- Compaction target size: a segment uses the DiskANN segment size
  (`DiskSegmentMaxSize`) only when every dense vector field of the
  collection, struct vector sub-fields included, has a disk index.

### Row-level filters

- Subscript predicates on `s[i][b]` (comparison, `in`, `like`, range,
  arithmetic) are UNKNOWN for a null row, an out-of-range index or a null
  element, and stay UNKNOWN under `not`. Subscripts are parsed with a range
  check, so an index that overflows is out of range rather than an error.
  These rules apply to top-level Array fields too: `arr[i] > 1` on a null
  row or a row shorter than `i + 1` is UNKNOWN.
- `in []` is `false` on every row, `not (... in [])` `true`; see
  [Semantics](#semantics).
- `array_contains`, `array_contains_any` and `array_contains_all` on `s[b]`:
  null elements match nothing, and the result of a non-null row is only
  `true` or `false`. For a null struct row the result is UNKNOWN, also when
  the target array is empty.
- `s[b] == [...]` / `!=`: an array holding a null element never equals a
  literal, since a literal cannot spell null.
- `array_length(s[b])` counts null elements, also on element-nullable vector
  sub-fields.
- The parser rejects `array_contains*` and `array_length` on a single array
  element (`arr[i]`, `s[i][scalar_sub]`), also on top-level Array fields, and
  `IS NULL` / `IS NOT NULL` on a sub-field (`s[b]`, `s[i][b]`).

Rules for nested `Array<Array<T>>` sub-fields (`s[i][tags]`, and the rejection
of `array_contains*` on the whole sub-field) are in
[Struct Data Type](./20260306-struct.md#nested-array-sub-fields-in-filters).

### Element-level filters

`MATCH_*` and `element_filter` predicates run on element-nullable sub-fields,
including sub-fields added to an existing struct.

- A null sub-field value makes the element comparison UNKNOWN; `&&`, `||` and
  `!` follow three-valued logic. Sub-fields of one element stay aligned by
  element id even when their null layouts differ.
- `element_filter` selects only `true` elements. `MATCH_*` counts an UNKNOWN
  element as a miss, so `MATCH_ALL` is `false` when any element is UNKNOWN; an
  empty array still satisfies `MATCH_ALL`. On a null struct row `MATCH_*` is
  UNKNOWN, also under `not`.
- `$[b] in []` is `false` for every element, a null one included. A
  non-empty set is UNKNOWN on a null element, also when all its literals are
  out of the element type's range (`$[i8] in [1000]`).
- `$[b] IS NULL` / `IS NOT NULL` is accepted inside `MATCH_*` and
  `element_filter` for scalar, vector and `Array<T>` sub-fields. It reads the
  element's own validity and is only `true` or `false`; on a sub-field that
  cannot hold null elements `IS NULL` is always `false`. `IS NULL` outside an
  element context (`clips[b] IS NULL`) stays rejected.
- On an `Array<T>` sub-field, `$[tags]` is an `Array<T>` value: a null leaf
  matches nothing in `array_contains*`, `array_length` counts null leaves, a
  null inner array is UNKNOWN, and `$[tags] IS NULL` is `true` only for a null
  inner array.

### Not supported

- Scalar indexes on element-nullable or nested scalar Array fields;
  CreateIndex rejects them.
- MAX_SIM-family metrics, emb-list indexes and EmbeddingList search on
  element-nullable vector Arrays; these are out of scope for this project.
- Struct fields in external collections; an external collection rejects
  every struct field, so element-nullable and nested Array sub-fields are not
  available there either.
