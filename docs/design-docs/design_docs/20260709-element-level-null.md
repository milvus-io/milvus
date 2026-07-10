# Array / ArrayOfVector Element-Level Null

## Background

Milvus already supports row-level nullable fields. For `Array` and
`ArrayOfVector`, row-level null means the whole array value of a row is null.

This design adds element-level null:

```text
int_array    = [1, null, 3]
vector_array = [vec0, null, vec2]
```

The new schema flag is `element_nullable`. It is independent from the existing
row-level `nullable` flag:

```text
nullable          controls whether the whole field row can be null
element_nullable  controls whether array elements can be null
```

The main storage decisions are:

- Scalar `Array` keeps the current Arrow `Binary` shape and stores element
  validity in a protobuf row wrapper.
- `ArrayOfVector` uses Arrow native `List<FixedSizeBinary>` child validity in
  Storage V2.
- Storage V1 rejects element-nullable `Array` / `ArrayOfVector`.

## Scope

Goals:

- support element-level null for scalar `Array`;
- support element-level null for `ArrayOfVector`;
- preserve existing row-level null semantics;
- define the behavior across insert, storage, load, query, and indexes.

Non-goals:

- no element-level null for non-array scalar or vector fields;
- no scalar `Array` migration to Arrow native `List<T>` in this change;
- no Storage V1 support;
- no global element-level valid bitset;
- no `array_contains(array, null)` support initially.

## Semantics

Row null and element null are different predicates:

```text
array is null       row-level null
array[0] is null    element-level null
```

For a row-level null array:

```text
array is null        -> true
array[0] is null     -> false
array[0] is not null -> false
array[0] > 1         -> false
```

For an out-of-range element index:

```text
array[10] is null     -> false
array[10] is not null -> false
array[10] > 1         -> false
```

Element expressions may read the element value only after checking:

1. the array row is valid;
2. the element index is in range;
3. if `element_nullable=true`, the element is valid.

Value-matching operators skip null elements. `array_length` counts logical
slots, including null slots.

## Data Flow

The write and read path is:

```text
SDK / client payload
  -> Proxy validation
  -> schemapb.FieldData
  -> WAL message body: msgpb.InsertRequest
  -> storage.InsertData
  -> Storage V2 Arrow Record
  -> Parquet / Vortex
  -> QueryNode load
  -> segcore field data / chunks
  -> query / search expression
```

WAL stores the `msgpb.InsertRequest` payload and does not parse array element
validity. The compatibility boundary is whether all payload producers and
consumers understand `element_nullable` and `nullable_data`, not the physical
WAL append format.

## Proto

`FieldSchema` adds `element_nullable`:

```protobuf
message FieldSchema {
  ...
  bool nullable = 15;
  bool element_nullable = 18; // New field
}
```

`element_nullable` is valid only for `DataType_Array` and
`DataType_ArrayOfVector`.

Scalar `Array` adds a nullable row wrapper:

```protobuf
// New message
message NullableScalarArrayValue {
  ScalarField data = 1; // New field
  repeated bool valid_data = 2; // New field
}

message ArrayArray {
  repeated ScalarField data = 1;
  DataType element_type = 2;
  repeated NullableScalarArrayValue nullable_data = 3; // New field
}
```

Representation:

```text
element_nullable=false -> use ArrayArray.data
element_nullable=true  -> use ArrayArray.nullable_data
```

Scalar `Array` is logical-shaped. A null element still occupies a typed scalar
slot, and `valid_data[i]` decides whether the slot is valid:

```text
int_array = [10, null, 30]

NullableScalarArrayValue {
  data.long_data.data = [10, 0, 30]
  valid_data = [true, false, true]
}
```

The placeholder value for a null element has no semantic meaning.

`ArrayOfVector` uses the same wrapper pattern:

```protobuf
// New message
message NullableVectorArrayValue {
  VectorField data = 1; // New field
  repeated bool valid_data = 2; // New field
}

message VectorArray {
  int64 dim = 1;
  repeated VectorField data = 2;
  DataType element_type = 3;
  repeated NullableVectorArrayValue nullable_data = 4; // New field
}
```

For `ArrayOfVector`, vector payload is compact:

```text
len(valid_data) = logical vector count
count(valid_data == true) = physical vector count in VectorField data
```

Null vector elements do not occupy `dim` floats or bytes in `VectorField`.

## Proxy and Go Data

Proxy owns request-level semantic validation:

- `element_nullable` is used only by `Array` / `ArrayOfVector`;
- `data` and `nullable_data` are not both set;
- `element_nullable=true` uses `nullable_data`;
- `element_nullable=false` does not use `nullable_data`;
- column-based `nullable_data` is expanded to `NumRows`;
- scalar `Array` `valid_data` length equals logical element count;
- `ArrayOfVector` physical vector count equals `count(valid_data == true)`.

`InsertData` keeps one active representation:

```go
type ArrayFieldData struct {
    ElementType     schemapb.DataType
    Data            []*schemapb.ScalarField
    NullableData    []*schemapb.NullableScalarArrayValue
    ValidData       []bool
    Nullable        bool
    ElementNullable bool
}

type VectorArrayFieldData struct {
    Dim             int64
    ElementType     schemapb.DataType
    Data            []*schemapb.VectorField
    NullableData    []*schemapb.NullableVectorArrayValue
    ValidData       []bool
    Nullable        bool
    ElementNullable bool
}
```

`ValidData` remains row-level validity. Element validity is stored in each
`NullableData[i].ValidData`.

Struct array sub-fields are flattened before normal validation. For
element-nullable sub-fields, flattening must preserve `NullableData`,
`ElementType`, and row-level `ValidData`.

## Storage V2

Storage V2 writes through Arrow:

```text
InsertData -> BuildRecord -> Arrow Record -> Parquet / Vortex
```

Scalar `Array` remains Arrow `Binary`:

```text
element_nullable=false: Arrow Binary value = proto.Marshal(ScalarField)
element_nullable=true:  Arrow Binary value = proto.Marshal(NullableScalarArrayValue)
```

The Arrow type is `Binary` in both cases. The reader must use
`FieldSchema.element_nullable` to choose the correct protobuf message type.

`ArrayOfVector` uses Arrow native list:

```text
Arrow type:
  List<FixedSizeBinary(vector_bytes)>
```

The column is a `ListArray`. Each row is a list value, and all vector elements
share one child `FixedSizeBinaryArray`:

```text
ListArray
  offsets  = [0, 3, 3, 5]
  validity = [true, false, true]        # row-level null

  values = FixedSizeBinaryArray
    validity = [true, false, true, ...] # element-level null
    values   = [vec0, null, vec2, ...]
```

Storage V1 must reject element-nullable `Array` / `ArrayOfVector` at both schema
writer and direct payload writer boundaries.

## Load and Runtime

For scalar `Array`:

- `element_nullable=false` deserializes `ScalarField`;
- `element_nullable=true` deserializes `NullableScalarArrayValue`;
- C++ `Array` / `ArrayView` carries element type, logical length, data buffer,
  optional offsets, `element_nullable`, and element validity bitmap.

Scalar array data remains logical-shaped. Invalid elements still have
placeholders in the data buffer. Callers should use:

```cpp
bool is_element_valid(int index) const;
T get_data_unchecked<T>(int index) const;
```

`get_data_unchecked` is valid only after bounds and element validity checks.

For `ArrayOfVector`, runtime data keeps logical element count while vector
payload remains compact:

```text
logical vector slots: [valid, null, valid]
physical vectors:     [vec0, vec2]
valid_data:           [true, false, true]
```

Logical vector access must map through element validity to a physical vector
offset.

## Query and Index

Raw data expressions must check row validity, bounds, and element validity
before reading element values. This affects:

- `array[index] == value`
- `array[index] > value`
- `array[index] in [...]`
- `array[index] is null`
- `array[index] is not null`
- string `like` / regex on `string_array[index]`
- `array_contains`, `array_contains_all`, `array_contains_any`
- `array_length`

`is null` uses different validity sources:

```text
array is null        -> row ValidData
array[index] is null -> per-row element valid_data
```

Array indexes can be used by both row-level and element-level expressions:

```text
array_contains(array, 3)  row-level result
array[0] in [1, 2]        row-level result over a fixed element slot
element_filter(...)       element-level intermediate result
```

Nested indexes need both row validity and element validity. Index primitive APIs
should not infer the caller's semantic level only from whether the index is
nested. The execution layer must preserve query context and request the correct
validity semantics.

## Compatibility

The new proto fields are wire-compatible. Old nodes ignore unknown fields and
would treat `element_nullable` as false.

That is not semantically safe. Old nodes cannot correctly process
element-nullable payloads because they do not understand:

- `FieldSchema.element_nullable`
- `ArrayArray.nullable_data`
- `VectorArray.nullable_data`
- Storage V2 read/write semantics
- segcore load/query/index semantics

Therefore, in a mixed-version cluster, element-nullable collections must not be
inserted, loaded, or queried by old nodes. Creating or using
`element_nullable=true` requires a cluster-version gate.
