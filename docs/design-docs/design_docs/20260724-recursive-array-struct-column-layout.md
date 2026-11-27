# Recursive Array Columnar Layout

## Background

This document covers the in-memory representation of nested scalar Array data
in segcore and its Storage V2 Arrow representation. In this stage, nesting is
limited to `Array<Array<scalar leaf>>` inside a `StructArrayField` sub-field.

The legacy `Array`, `ArrayView`, and `ArrayChunk` model places leaf data
immediately after the Array.
For example, an `ArrayChunk` in a sealed segment has the following in-memory
layout:

```text
[optional null bitmap]
[off0, len0, off1, len1, ..., offN-1, lenN-1, offN]
[row0 payload][row1 payload] ... [rowN-1 payload]
[padding]
```

Each Array row is stored as a payload and interpreted as leaf data such as Int
or String according to a single-level `element_type`. This format works for
`Array(Int)` and `Array(String)`: fixed-width types store their values
contiguously, while String additionally stores byte offsets and characters
inside each row payload.

However, this model has no independent logical offsets for the next level, so
it cannot naturally represent `Array(Array(Int))`.

## Design

For a native-list scalar Array, `ColumnarArrayChunk` represents each Array
level by logical offsets and a null bitset, and points to a child column:

```text
ArrayColumn
├── offsets        // parent row -> range of logical child rows
├── null_bitset    // when nullable, one bit per parent row; 0 means null
└── child          // another ArrayColumn or a leaf column
```

The offsets at each level describe only the logical boundaries of the Array at
that level:

```text
offsets.size == row_count + 1
offsets[0] == 0
offsets.back() == child.row_count
nullable => null_bitset.bit_count >= row_count
```

For a null list row or null inner list, `offsets[i] == offsets[i+1]` is
required: a null list cannot own a non-empty child segment. When the offsets
are equal, the row may be either empty or null. The two
cases must be distinguished using `null_bitset[i]`. A non-nullable Array can
omit this bitset; every nullable level of a nested Array has its own independent
null bitset.

For example, `Array(Array(Int32))` is represented as:

```text
ArrayColumn
├── outer_offsets
├── outer_null_bitset
└── ArrayColumn
    ├── inner_offsets
    ├── inner_null_bitset
    └── Int32Column
        ├── leaf_null_bitset  // when the leaf TypeSchema is nullable
        └── values
```

The outer bitset represents the whole field row, the inner bitset represents
the inner Array at each outer position, and the leaf bitset represents each
scalar value. A null leaf keeps a dense placeholder value; a null inner Array
has an empty child range. The leaf column uses native widths, including
`int8` and `int16` rather than proto's widened int32 values.

An Array does not interpret the physical format of its child. Variable-length
leaf types such as String continue to maintain their own byte offsets:

```text
StringColumn
├── leaf_null_bitset  // when nullable
├── byte_offsets
└── chars
```

Therefore, Array offsets are always expressed in logical child row numbers,
not byte positions. Null semantics are stored independently in the null bitset
at the same level, including the leaf level.

## StructArray Projection Example

A `StructArrayField` can project a nested scalar Array sub-field by FieldID.
For a Struct array with a scalar `label` sub-field and a
`nested_values: Array<Array<Int32>>` sub-field, the projected columns are:

```text
label: String                               <- FieldID 1
nested_values: Array<Array<Int32>>          <- FieldID 2
```

The outer offsets and row validity follow the parent Struct array. For the
nested sub-field, a second Array level supplies inner offsets and validity;
its scalar leaf has separate validity:

```text
FieldID: 1
└── ArrayColumn
    ├── outer offsets and row null bitset
    └── StringColumn

FieldID: 2
└── ArrayColumn
    ├── outer offsets and row null bitset
    └── ArrayColumn
        ├── inner offsets and inner null bitset
        └── Int32Column with leaf null bitset
```

## Storage Layer

Storage V2 writes nested scalar `Array<Array<T>>` as Arrow `list<list<T>>`,
regardless of which of its three levels are nullable. Each list child field is
named `item`. For example, a nullable row with nullable inner Arrays and
nullable Int32 leaves has this schema:

```text
field: list<item: list<item: int32 nullable> nullable> nullable
```

The top-level Arrow field nullable flag equals `FieldSchema.nullable`; the
first `item` nullable flag equals `FieldSchema.element_nullable`; the second
`item` nullable flag equals the leaf `TypeSchema.nullable`. The TypeSchema root
and second node must mirror the first two FieldSchema flags, respectively;
the leaf flag is independent. Only nested Arrays carry this wire TypeSchema.
For a single-level element-nullable scalar Array, `FieldMeta` synthesizes an
internal TypeSchema for `ColumnarArrayChunk`/`ArrayValue`; it is not added to
the field's wire schema.

Proxy passes dense nested proto data to storage. For example,
`[[1, null], null, []]` has this per-row shape:

```text
ScalarField{ArrayData: ArrayArray{ElementType: Int32, Data: [
  ScalarField{IntData: [1, 0], valid_data: [true, false]},
  ScalarField{IntData: []},  // null inner Array
  ScalarField{IntData: []}   // non-null empty inner Array
]}, valid_data: [true, false, true]}
```

The two empty payloads are distinguished by the parent validity. The row
validity is duplicated in `FieldData.valid_data` and the outer
`ScalarField.valid_data`; the per-row message above carries inner validity.
Go storage converts this shape to Arrow lists with native-width scalar leaves;
`ColumnarArrayChunk` builds directly from Arrow `ListArray` rather than
deserializing proto Binary. Null list rows and null inner lists have repeated
offsets and no child values. The legacy single-level scalar Array with
non-nullable elements remains one proto-serialized `Binary` per row and loads
through `ArrayChunk`.
