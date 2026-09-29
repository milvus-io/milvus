# Array Element-Level Null

## Scope

This design extends row-level null support to elements of scalar `Array` and
`ArrayOfVector`, and to the inner Array and leaf of `Array<Array<T>>`.
`FieldSchema.nullable` controls the whole row, `element_nullable` controls the
first Array element, and the nested scalar leaf uses its `TypeSchema.nullable`.

The design targets Storage V2. Storage V1 rejects all new-format fields.
Nested Arrays, vector Arrays, and element-nullable Arrays are allowed only as
`StructArrayField` sub-fields. Top-level `Array<nullable T>`,
`Array<Array<T>>`, and `Array<Vector>` are rejected. Nesting is limited to
`Array<Array<scalar leaf>>`; the leaf cannot be a vector, and a Struct object
cannot itself be null.

## Semantics

The following are the intended logical semantics. Expression evaluation on
new-format fields with null elements is not supported in this stage.

`array IS NULL` tests the whole row. An empty array and an array containing
only null elements are both non-null rows. `array[index]` evaluates to `NULL`
when the row is null, the index is out of range, or the selected element is
null:

| State | `array[index] > 1` | `array[index] IS NULL` | `array[index] IS NOT NULL` |
| --- | --- | --- | --- |
| Null row | `NULL` | `true` | `false` |
| Index out of range | `NULL` | `true` | `false` |
| Null element | `NULL` | `true` | `false` |
| Non-null element `x` | `x > 1` | `false` | `true` |

Here `NULL` means an unknown predicate result, not a valid `false`.
Comparisons, term tests, and string predicates on an accessed element
propagate that result. Array membership operations (`array_contains*`) skip
null slots. Null vectors do not participate in similarity search.
`array_length` and capacity limits count logical slots, including null slots.

## Representation and Data Flow

The existing `ScalarField` and `VectorField` messages carry `valid_data` for
their immediate logical values; no nullable wrapper messages are needed.
Row validity is written to both `FieldData.valid_data` and the outer
`ScalarField.valid_data` or `VectorField.valid_data`, with identical bits.
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
inner levels of a nested scalar Array. Row payloads are also compact; Proxy
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
| Vector Array, non-nullable elements | `list<item: fixed_size_binary[dim*width] not null>` | Existing format |
| Single-level scalar Array, nullable elements | `list<item: T nullable>` | New format |
| Vector Array, nullable elements | `list<item: binary nullable>` | New format |
| Nested scalar `Array<Array<T>>`, whether or not its inner levels are nullable | `list<item: list<item: T> >` | New format |

For example, nested Int64 with non-nullable inner levels is
`list<item: list<item: int64 not null> not null>`; the top-level field has
its own row nullable flag.

The top-level Arrow field's nullable flag equals `FieldSchema.nullable`.
The first list's `item` nullable flag equals `FieldSchema.element_nullable`;
the nested leaf's flag equals the leaf `TypeSchema.nullable`. The vector
`list<Binary>` item is nullable, while the existing
`list<FixedSizeBinary>` item is not. Every list child field is named `item`.

Scalar leaf `T` uses native widths in Go and C++: Bool→`bool`, Int8→`int8`,
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
`data_type == Array` and `element_type == Array`. The root node's `nullable`
must equal `FieldSchema.nullable`; the second node's `nullable` must equal
`FieldSchema.element_nullable`; the leaf node's `nullable` is independent.
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
segments use `ArrayValue` for these scalar fields as well. The growing-flush
path (`FlushGrowingSegmentData`/`AllowGrowingSourceFlush`) is being removed;
it receives no support or checks for these formats.

## Query and Index

In this stage, new-format fields with a nullable level other than the row
return a clear `not supported yet` error on Array expressions
(`element_filter`, `MATCH`, `array_contains*`, and others), scalar index
construction, element-level ANN, bulk import, and external tables.
Non-nullable nested scalar Arrays retain their existing expression and index
behavior. For element-nullable vector Arrays, MAX_SIM-family metrics and
emb-list indexes/placeholders are out of scope for this project and are
rejected; autoindex does not map those fields to a MAX_SIM default.
Element-level metrics (IP, L2, COSINE, and others), with their indexes and
search, are in scope for this project. Until query-layer support is
available, CreateIndex with those metrics and all other search placeholders
on those fields return `not supported yet`.

Later query support must apply row, bounds, and element validity in the right
bitmap space: ordinary filters produce row bits; `element_filter` and `MATCH`
child expressions produce element bits. Null elements contribute no value
postings and do not participate in vector similarity search. Element-level
ANN should use a Milvus-side bitset projection and physical-to-logical (p2l)
mapping so returned element indices refer to original logical positions;
knowhere does not need a change.
