# MEP: Native UUID Field Type (Usable as Primary Key)

- **Created:** 2026-07-03
- **Author(s):** @BlackPool25
- **Status:** Implementation Complete
- **Component:** Storage / Proxy / Client SDK
- **Related Issues:** [#50957](https://github.com/milvus-io/milvus/issues/50957)

## Summary

Add a native `UUID` scalar field type (`DataType_UUID = 28`) that can serve as a collection primary key alongside `Int64` and `VarChar`. The type stores UUIDs as fixed 16-byte binary internally, accepts/returns canonical UUID strings at the SDK boundary, and supports equality (`==`, `!=`) and `IN` filtering.

## Motivation

UUIDs are one of the most common entity-identifier formats and are frequently the natural primary key for records ingested into Milvus. Currently the only option is to store them in a `VarChar` primary key, which is inefficient and error-prone:

- A canonical UUID string is 36 chars (~36 bytes) vs. the 16 bytes of the underlying value — larger keys, larger indexes, more memory.
- Equality/`IN` lookups on a string PK are slower than on a fixed-width 16-byte key.
- No validation: any malformed string is accepted as an "id".
- Users must manually convert between UUID and string representations in application code.

## Public Interfaces

### Proto (`milvus-proto/go-api/schemapb`)

```protobuf
enum DataType {
  // ...
  UUID = 28;  // Already defined in milvus-proto
}
```

`DataType_UUID` was already added to the upstream `milvus-proto` repository before this implementation. UUID field data is transmitted over the wire as `StringData` (canonical UUID strings) and `BytesData` (16-byte binary), reusing existing ScalarField oneof variants. No new proto messages or oneof fields were added.

### Go Client SDK (`client/`)

```go
const FieldTypeUUID FieldType = 28
```

New types:
- `entity.FieldTypeUUID` constant with `Name()` → `"UUID"`, `String()` → `"string"`
- `column.ColumnUUID` backed by `[]string`, with `NewColumnUUID(name, values)`
- `column.NewNullableColumnUUID` for nullable UUID columns
- `columns.IDColumns()` support for UUID PK → returns `ColumnUUID`
- `columns.FieldDataColumn()` UUID case — reads `BytesData`, converts to strings

### Schema

Users create a UUID field like any other field type:

```python
schema.add_field("id", DataType.UUID, is_primary=True)
# or as a non-PK scalar field:
schema.add_field("uuid_field", DataType.UUID)
```

UUID fields accept filtering expressions:
```
id == "550e8400-e29b-41d4-a716-446655440000"
id in ["uuid-1", "uuid-2", "uuid-3"]
id != "550e8400-e29b-41d4-a716-446655440000"
```

Range expressions (`>`, `<`, `>=`, `<=`) are not supported for UUID fields.

## Design Details

### Storage Format

UUIDs are stored as fixed 16-byte binary values using Apache Arrow's `FixedSizeBinaryType{ByteWidth: 16}` type at the Parquet/storage layer. This is the same approach used by fixed-width vector types (BinaryVector, Float16Vector) and is the most space-efficient representation.

At the SDK and wire boundaries, UUIDs are represented as canonical 36-character strings (e.g., `"550e8400-e29b-41d4-a716-446655440000"`). The conversion between string and binary happens:

1. **Insert path**: Proxy validates UUID string format via `uuid.Parse()`, passes as `StringData` → storage layer converts to `[]byte` via `uuid.UUID.MarshalBinary()` → persisted as `FixedSizeBinary(16)` in Parquet.
2. **Read path**: Parquet reads `FixedSizeBinary(16)` → `array.FixedSizeBinary` → converts to `string` via `uuid.UUID.String()` → returned to client as string.
3. **Primary key path**: PK comparisons use `bytes.Compare()` on the raw 16-byte value (lexicographic byte order). PK values are transmited as strings via the existing `IDs_StrId` proto field, distinguished by the containing field's `DataType`.

### Primary Key Implementation

`UUIDPrimaryKey` implements the existing `PrimaryKey` interface:

| Method | Implementation |
|--------|----------------|
| `GT/GE/LT/LE` | `bytes.Compare(pk.Value[:], other[:])` |
| `EQ` | Direct `uuid.UUID` equality (`==`) |
| `MarshalJSON` | String form (e.g., `"550e8400-..."`) |
| `SetValue` | Accepts `string`, `uuid.UUID`, `[]byte`(16) |
| `Size()` | Fixed 16 bytes |

`UUIDPrimaryKeys` implements the batch `PrimaryKeys` interface with Append, MustMerge, Reset, and Get operations, backed by `[][16]byte` storage.

### Serialization (serde.go)

```go
m[schemapb.DataType_UUID] = serdeEntry{
    arrowType: func(...) arrow.DataType {
        return &arrow.FixedSizeBinaryType{ByteWidth: 16}
    },
    deserialize: func(a arrow.Array, i int, ...) (any, error) {
        // Read 16 bytes from FixedSizeBinary array
    },
    serialize: func(b array.Builder, v any, ...) error {
        // Accept []byte (must be 16), [16]byte, or string (parsed via uuid.Parse)
    },
}
```

### Index Support

UUID fields support the following index types:

| Index Type | Support | Note |
|------------|---------|------|
| TRIE | ✅ | Exact-match prefix tree, ideal for equality/IN |
| INVERTED | ✅ | Inverted index for equality lookups |
| STL_SORT | ✅ | Sorted index for ordered iteration |
| BITMAP | ✅ | Bitmap index for multi-value filtering |

### Type Classification

UUID is **not** grouped with string types (`IsStringType()` returns false). It has its own classification function `IsUUIDType()`. The `IsPrimaryFieldType()` function now returns true for `DataType_UUID` alongside `DataType_Int64` and `DataType_VarChar`.

### C++ Core

The C++ core engine treats UUID as a `std::string`-backed `FieldData` type (same storage class as VARCHAR/STRING/GEOMETRY). The `DataType::UUID = 28` enum value matches the proto definition. UUID data in the C++ layer uses `arrow::StringBuilder`/`arrow::StringScalar` for Arrow interop, consistent with other string-backed types.

### Dependency

The implementation uses `github.com/google/uuid v1.6.0` for UUID parsing, formatting, and binary conversion. This is a well-established, minimal-dependency library (2000+ GitHub stars, no transitive dependencies).

## Test Plan

### Unit Tests

- **Go storage**: UUIDPrimaryKey comparison, JSON marshal/unmarshal, factory functions; UUIDPrimaryKeys append/merge/reset; UUIDFieldData append ([]byte/string/nil/nullable); serde round-trip (valid UUID, null, negative)
- **Go proxy**: `checkUUIDFieldData` validation (valid/invalid UUID strings, type mismatch); `validatePrimaryKey` UUID acceptance
- **Go client SDK**: FieldTypeUUID constants, name/string/PbFieldType; ColumnUUID creation/slice; FieldDataColumn UUID deserialization from BytesData
- **Go typeutil**: IsUUIDType, IsPrimaryFieldType-UUID, GetPKSize-UUID; HashKey2Partitions UUID partitioning
- **Go parser**: UUID type comparison, range value casting, template value generation, plan parsing
- **Go index**: All four index checkers accept UUID
- **C++**: Enum value, IsStringDataType(false), ToProtoDataType mapping, InitScalarFieldData, Schema.AddDebugField

### Integration Tests (Python SDK)

- Create collection with UUID PK
- Auto-id UUID PK (requires auto-id generation support)
- Insert and query by exact UUID match
- Query with IN operator on UUID field
- Delete by UUID expression
- Delete and reinsert with same UUIDs
- Data consistency across multiple field types with UUID PK
- Batch insert and cross-batch UUID query
- UUID as non-PK scalar field
- Invalid UUID string rejection at insert

### Coverage Target

>= 90% for Go unit tests on modified packages; C++ unit test coverage for the new code paths.

## Compatibility

This feature is fully additive. No existing data types, APIs, or storage formats are modified. Collections using Int64 or VarChar PKs continue to work unchanged. UUID PK data is stored in a new format that does not affect existing segment readers.

Upgrade path: New collections can use UUID fields immediately after upgrade. Existing collections are unaffected.

## Rejected Alternatives

### VarChar primary key (current approach)
Storing UUIDs as VarChar strings wastes ~2.25x storage space (36 bytes vs. 16), has no format validation, and performs slower point lookups due to variable-length key comparison.

### Int64 PK with separate UUID column
Forces a synthetic integer key and a secondary lookup, defeating the purpose of a natural UUID identifier. Increases application complexity with no storage benefit.

### UUID auto-generation (v4/v7)
While a future enhancement could add automatic UUID generation for auto-id fields, this first implementation requires clients to provide UUID values. Auto-generation is tracked as a follow-up feature.

### New proto message for UUID
Rather than adding a new `UUIDArray` or `ScalarField_UuidData` oneof variant, this implementation reuses existing `StringData` (for SDK I/O) and `BytesData` (for PK operations) proto fields, distinguished by `DataType_UUID`. This avoids proto breaking changes and keeps the wire format simple.

## References

- [google/uuid Go library](https://github.com/google/uuid)
- PostgreSQL UUID type documentation
- Related issues: #50956 (Decimal), #50958 (Map) — part of broader scalar type expansion
- Prior art: TIMESTAMPTZ type addition (#44005, #44080)
- MEP template and contribution guidelines from CONTRIBUTING.md
