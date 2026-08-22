# Array Element-Level Null

## Background

Milvus already supports row-level null. For `Array` and `ArrayOfVector`, this
means the whole value of a row can be null. This design adds null elements
inside a valid row:

```text
int_array    = [1, null, 3]
vector_array = [vec0, null, vec2]
```

The two schema flags are independent:

```text
nullable          controls whether the whole row can be null
element_nullable  controls whether an array element can be null
```

This design targets Storage V2 and later. Storage V1 does not encode element
validity and must reject element-nullable fields.

## Semantics

Row null and element null are different predicates:

```text
array is null       row-level null
array[0] is null    element-level null
```

For a null row or an out-of-range element index, element expressions produce
no match:

```text
array[0] is null     -> false
array[0] is not null -> false
array[0] > 1         -> false
```

An element expression reads a value only after checking:

1. the row is valid;
2. the index is in range;
3. the element is valid when `element_nullable=true`.

Value operators skip null elements. `array_length` counts logical slots,
including null slots.

## Data Flow

```text
SDK payload
  -> Proxy validation
  -> schemapb.FieldData
  -> WAL msgpb.InsertRequest
  -> storage.InsertData
  -> Storage V2 Arrow Record
  -> Parquet / Vortex
  -> QueryNode load
  -> segcore runtime data
  -> query / search expressions and indexes
```

The WAL stores the insert request and does not reinterpret array contents.
Element-null compatibility is determined by the payload producers and
consumers, not by a separate WAL format.

## Proto Representation

`valid_data` belongs to the message that carries the immediate logical values:

```protobuf
message ScalarField {
  oneof data {
    ...
    ArrayArray array_data = 8;
  }
  repeated bool valid_data = 17;
}

message VectorField {
  int64 dim = 1;
  oneof data {
    ...
    VectorArray vector_array = 8;
  }
  repeated bool valid_data = 9;
}
```

The same fields have different scopes at different nesting levels:

```text
FieldData.scalars.valid_data              row validity for a scalar field
FieldData.vectors.valid_data              row validity for a vector field
ArrayArray.data[row].valid_data            element validity in one scalar array
VectorArray.data[row].valid_data           element validity in one vector array
```

`FieldData.valid_data` remains a legacy row-validity source. Readers accept it
as a fallback, but a payload must not populate both legacy and field-specific
row validity. New writers store row validity on `ScalarField` or `VectorField`.

No nullable wrapper message or `nullable_data` field is needed. An array row is
still represented by its existing `ScalarField` or `VectorField`, with
`valid_data` attached directly to that row message.

### Scalar Array

`ArrayArray.data` always carries `ScalarField` rows:

```text
int_array = [10, null, 30]

SDK payload before Proxy normalization:
ScalarField {
  long_data.data = [10, 30]
  valid_data = [true, false, true]
}
```

Scalar array payload is compact at the Proxy input boundary. Proxy validates
the compact payload and expands it before the insert message enters the WAL:

```text
Normalized ScalarField {
  long_data.data = [10, 0, 30]
  valid_data = [true, false, true]
}
```

The normalized payload is dense in logical element space. A null element keeps
a typed placeholder, and the placeholder value has no semantic meaning.

The Proxy input invariant is:

```text
count(row.valid_data == true) = number of compact scalar payload values
```

### ArrayOfVector

`VectorArray.data` always carries `VectorField` rows. Vector payload is compact
when element validity is present:

```text
logical elements: [vec0, null, vec2]
row.valid_data:   [true, false, true]
physical payload: [vec0, vec2]
```

The required invariants are:

```text
len(row.valid_data) = logical vector count
count(row.valid_data == true) = physical vector count
```

## Proxy and Go Data

Proxy validates the request against the schema:

- child `valid_data` is allowed only when `element_nullable=true`;
- scalar array physical value count equals the number of valid elements;
- scalar array child payload is expanded to dense form after validation;
- ArrayOfVector physical vector count equals the number of valid elements;
- max capacity counts logical elements, including null elements;
- row validity has exactly one source;
- row-level null expansion does not overwrite child element validity.

The Go storage structures keep one row container for each type:

```go
type ArrayFieldData struct {
    ElementType     schemapb.DataType
    Data            []*schemapb.ScalarField
    ValidData       []bool
    Nullable        bool
    ElementNullable bool
}

type VectorArrayFieldData struct {
    Dim             int64
    ElementType     schemapb.DataType
    Data            []*schemapb.VectorField
    ValidData       []bool
    Nullable        bool
    ElementNullable bool
}
```

The struct-level `ValidData` is row validity. Element validity stays in each
`Data[row].ValidData` proto message. Sorting, merging, result slicing, struct
flattening, and conversion back to `InsertRecord` move the row message as a
unit, preserving its child validity.

## Storage V2

Scalar `Array` remains Arrow `Binary`. Each non-null Arrow value is the
serialized `ScalarField` row, so its child `valid_data` is preserved in the
protobuf bytes:

```text
Arrow Binary value = proto.Marshal(ArrayArray.data[row])
```

`ArrayOfVector` remains Arrow native list data:

```text
List<FixedSizeBinary(vector_bytes)>
```

Arrow list validity represents row null. Child `FixedSizeBinary` validity
represents vector-element null:

```text
ListArray
  offsets  = [0, 3, 3, 5]
  validity = [true, false, true]        # row validity

  values = FixedSizeBinaryArray
    validity = [true, false, true, ...] # element validity
```

The writer expands compact vector payload into Arrow child positions. The
reader compacts valid child vectors back into `VectorField` payload and writes
the child bitmap to `VectorField.valid_data`.

Parquet and Vortex consume the Arrow representation; neither changes the
Milvus-level null semantics.

## Load and Runtime

Load reconstructs both validity levels separately:

- row validity enters the existing row-level valid bitset;
- scalar array element validity is reconstructed from serialized
  `ScalarField.valid_data`;
- ArrayOfVector element validity is reconstructed from Arrow child validity.

Element null does not require a global element-level filter bitset. Expression
result granularity determines the result bitset: normal filters produce row
bits, while element-filter internals may temporarily produce element bits.

## Query and Index

The affected raw-data expressions include indexed access, comparisons, term
queries, string `like` / regex, `array_contains*`, and null predicates.

```text
array is null        uses row validity
array[index] is null uses the selected row's child validity
```

Nested indexes use element document IDs, so they need element validity in
addition to the existing row validity. Index primitives should return results
in their own document-ID space; the execution layer remains responsible for
converting nested element results to row results when required.

## Compatibility

The added schema and `valid_data` fields are protobuf wire-compatible, but old
nodes do not understand their semantics and will treat placeholders as real
values. Therefore element-nullable fields require a cluster-version gate and
must not be inserted, loaded, or queried by old nodes.

Within the supported version, readers accept legacy `FieldData.valid_data` for
row validity. New payloads use `ScalarField.valid_data` or
`VectorField.valid_data`, and dual row-validity sources are rejected.
