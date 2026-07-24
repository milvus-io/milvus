# Recursive Array Columnar Layout

## Background

This document covers only the in-memory representation of Array data in
segcore. It does not cover the persistence format.

Segcore's current `Array`, `ArrayView`, and `ArrayChunk` implementations are
all based on a model in which the leaf data immediately follows the Array.
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

An Array no longer interprets leaf data directly. It stores only its own
logical offsets and null bitset, and points to a child column:

```text
ArrayColumn
├── offsets        // parent row -> range of logical child rows
├── null_bitset    // when nullable, one bit per parent row; 1 means null
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

When `offsets[i] == offsets[i+1]`, the row may be either empty or null. The two
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
```

An Array does not interpret the physical format of its child. Variable-length
leaf types such as String continue to maintain their own byte offsets:

```text
StringColumn
├── byte_offsets
└── chars
```

Therefore, Array offsets are always expressed in logical child row numbers,
not byte positions. Null semantics are stored independently in the null bitset
at the same level.

## Complex StructArray Projection Example

A recursive ArrayColumn can also represent a multi-level StructArray after
FieldID projection. Given:

```text
StructArray(
    int,                                  <- FieldID 1
    nested StructArray(
        String,                           <- FieldID 2
        Double                            <- FieldID 3
    )
)
```

Let `S0` denote the logical offsets and null bitset of the outer StructArray,
and `S1` denote those of the inner StructArray. The column for each FieldID can
then be represented as:

```text
FieldID: 1
└── ArrayColumn
    ├── offsets -> S0.offsets
    ├── null_bitset -> S0.null_bitset
    └── IntColumn

FieldID: 2
└── ArrayColumn
    ├── offsets -> S0.offsets
    ├── null_bitset -> S0.null_bitset
    └── ArrayColumn
        ├── offsets -> S1.offsets
        ├── null_bitset -> S1.null_bitset
        └── StringColumn
            ├── byte_offsets
            └── chars

FieldID: 3
└── ArrayColumn
    ├── offsets -> S0.offsets
    ├── null_bitset -> S0.null_bitset
    └── ArrayColumn
        ├── offsets -> S1.offsets
        ├── null_bitset -> S1.null_bitset
        └── DoubleColumn
```

# Storage Layer Remains Unchanged

Currently, each Array row is represented by a `ScalarField` protobuf message.
In Arrow, an Array is represented as binary bytes, so each row is serialized
into bytes by protobuf and then written to Arrow.

The Array format conversion pipeline on the write path is:

```text
ScalarField -> serialized ScalarField protobuf bytes -> Arrow -> Parquet/Vortex
```

Because `ScalarField` is itself a nested representation, it can express Arrays
with arbitrary nesting depth. Serialization is handled by protobuf itself, so
the write path should not require any changes.
