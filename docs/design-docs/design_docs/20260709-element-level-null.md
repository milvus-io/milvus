# Array Element-Level Null

## Scope

This design extends row-level null support to individual elements of `Array`
and `ArrayOfVector`, such as `[1, null, 3]` and `[vec0, null, vec2]`.
`nullable` controls whether the whole row can be null; `element_nullable`
independently controls whether an element can be null.

The design targets Storage V2 and later; Storage V1 must reject
element-nullable fields. `ArrayOfVector` retains its restriction to
`StructArray` sub-fields. Element-level nullability for recursively nested
Arrays and nullable nodes in `TypeSchema` are out of scope.

## Semantics

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
On the outer field message it represents row validity. On a child message in
`ArrayArray.data[row]` or `VectorArray.data[row]` it represents element validity.
Go storage containers retain row validity separately from each child message's
element validity.

For an element-nullable array, the child bitmap has one entry per logical
element. At the Proxy input boundary, element payloads are compact:

```text
len(child.valid_data)             = logical element count
count(child.valid_data == true)   = physical payload element count
```

Proxy validates this relationship before normalization. Row payloads are also compact and 
restores null row positions without changing child validity. Element-level
normalization depends on the payload type:

| Type | Logical value | Input payload | Payload after Proxy |
| --- | --- | --- | --- |
| Scalar Array | `[10, null, 30]` | `[10, 30]` | `[10, 0, 30]` |
| ArrayOfVector | `[vec0, null, vec2]` | `[vec0, vec2]` | `[vec0, vec2]` |

Both examples retain child validity `[true, false, true]`. Scalar arrays use
dense placeholders to simplify downstream logical-index access.The placeholder has no 
semantic value and costs additional payload space. Vector arrays stay compact because 
a placeholder would occupy an entire vector. Empty and all-null arrays retain their 
element type even when their physical payload is empty.

The insert-to-query flow is:

```text
Insert request -> Proxy validation and normalization -> WAL
  -> flusher -> Storage V2 (Arrow -> Parquet / Vortex)
  -> QueryNode load -> runtime data -> queries and indexes
```

WAL preserves the normalized payload without interpreting element nulls.
Sorting, merging, flattening, and retrieval must preserve each element's
logical position and validity together with its value.

Storage V2 preserves the two validity levels as follows:

| Field | Arrow representation | Row validity | Element validity |
| --- | --- | --- | --- |
| Scalar Array | `Binary` containing a serialized `ScalarField` | Outer bitmap | Child proto `valid_data` |
| ArrayOfVector, non-element-nullable | `List<FixedSizeBinary>` | List bitmap | All elements valid |
| ArrayOfVector, element-nullable | `List<Binary>` | List bitmap | Child Binary bitmap |

`List<Binary>` preserves null vector positions without allocating a full
vector placeholder for each null. Its non-null children must still match the
schema's vector width. Serialization maps compact vectors to logical child
positions; deserialization restores compact vectors and child validity.
Parquet, Vortex, and runtime loading preserve these semantics.

## Query and Index

Element access accounts for row validity, bounds, and element validity before
reading a value. Raw-data and index-backed execution must produce the same
predicate results.

Element nullability does not determine result bitmap granularity. Ordinary
filters produce row bits; `element_filter` and `MATCH` child expressions
operate on element bits. Validity must be applied in the corresponding space.

Nested scalar indexes therefore preserve both row and element validity.
Term and range lookups return element document IDs; row-null queries use row
validity, and element-null queries use element validity. The execution layer
aggregates element matches into row results for operations such as
`array_contains`. Null elements contribute no value postings but retain their
logical positions for validity and result mapping.

Vector search excludes null elements. Any compact physical vector IDs and
filter positions must be mapped consistently to logical element positions,
so returned element indices still refer to the original array.
