# MEP: Native UUID Field Type (Usable as Primary Key)

- **Created:** 2026-07-03
- **Author(s):** @BlackPool25
- **Status:** Implementation Complete (Phase 1)
- **Component:** Proxy / Client SDK / Type System
- **Related Issues:** [#50957](https://github.com/milvus-io/milvus/issues/50957)

## Summary

Add a native `UUID` scalar field type (`DataType_UUID = 31`) that can serve as a collection primary key alongside `Int64` and `VarChar`. In this initial implementation, UUIDs are stored as canonical strings (VarChar-backed) at the storage layer. Input validation and lowercase normalization happen at the proxy boundary. A follow-up phase will add 16-byte binary storage for space efficiency.

## Motivation

UUIDs are one of the most common entity-identifier formats and are frequently the natural primary key for records ingested into Milvus. Currently the only option is to store them in a `VarChar` primary key, which is inefficient and error-prone:

- No validation: any malformed string is accepted as an "id".
- Users must manually convert between UUID and string representations in application code.
- A canonical UUID string is 36 chars (~36 bytes) vs. the 16 bytes of the underlying value — larger keys, larger indexes, more memory. (This is addressed by a follow-up 16-byte storage phase.)

## Public Interfaces

### Proto (`milvus-io/milvus-proto`)

```protobuf
enum DataType {
  // ...
  UUID = 31;  // Added in milvus-proto PR #636
}
```

A `UUIDArray` message and `uuid_id = 3` field have also been added to the `IDs` oneof (milvus-proto PR #639) for future use in the 16-byte storage phase. This PR does not use `uuid_id` — UUID PKs travel on `str_id` as canonical strings.

### Go Client SDK (`client/`)

```go
const FieldTypeUUID FieldType = 31
```

New types:
- `entity.FieldTypeUUID` constant with `Name()` → `"UUID"`, `String()` → `"string"`
- `column.ColumnUUID` backed by `[]string`, with `NewColumnUUID(name, values)`
- `column.NewNullableColumnUUID` for nullable UUID columns
- `columns.IDColumns()` support for UUID PK → returns `ColumnUUID`
- `columns.FieldDataColumn()` UUID case — reads `BytesData` (from storage), converts to strings

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

Auto-id is not supported for UUID primary keys in this phase. Users must provide UUID values explicitly.

## Design Details

### Storage Format (Phase 1 — VarChar-backed)

UUIDs are stored as canonical strings (36 characters) at the Parquet/storage layer, reusing the existing string/VarChar storage path. This is the same approach used by VarChar fields.

The conversion between user input and stored value happens:

1. **Insert path**: Proxy validates UUID string format via `uuid.Parse()`, normalizes to lowercase via `uuid.UUID.String()`, passes as `StringData` → persisted as string in Parquet.
2. **Read path**: Parquet reads string → returned to client as canonical lowercase UUID string.
3. **Primary key path**: UUID PKs travel on the existing `IDs_StrId` proto field like VarChar PKs. `ParseIDs2PrimaryKeys` creates `VarCharPrimaryKey` for UUID just like VarChar.

### Phase 2 — 16-byte Binary Storage (Follow-up)

A future PR will switch storage to `FixedSizeBinary(16)` in Parquet, use the `IDs_UuidId` proto field for type-safe PK operations, and reduce storage from 36 bytes to 16 bytes per UUID. The `DataType.UUID = 31` user-facing type remains unchanged, so this is transparent to users.

Dependencies already in place for the follow-up:
- `milvus-proto` PR #639: `UUIDArray` message + `uuid_id` field in `IDs` oneof
- `UUIDPrimaryKey`, `UUIDFieldData`, and `FixedSizeBinary(16)` serde code in the repo history

### Index Support

UUID fields support the following index types:

| Index Type | Support | Note |
|------------|---------|------|
| TRIE | ✅ | Exact-match prefix tree, ideal for equality/IN |
| INVERTED | ✅ | Inverted index for equality lookups |
| STL_SORT | ✅ | Sorted index for ordered iteration |
| BITMAP | ✅ | Bitmap index for multi-value filtering |

Indexes work on the string representation (same as VarChar).

### Type Classification

UUID has its own classification function `IsUUIDType()`. The `IsPrimaryFieldType()` function returns true for `DataType_UUID` alongside `DataType_Int64` and `DataType_VarChar`.

For storage and comparison, UUID is treated like VarChar (string comparison).

### C++ Core

The C++ core engine treats UUID as a `std::string`-backed `FieldData` type (same storage class as VARCHAR/STRING). The `DataType::UUID = 31` enum value matches the proto definition. UUID data in the C++ layer uses `arrow::StringBuilder`/`arrow::StringScalar` for Arrow interop, consistent with other string-backed types.

### Dependency

The implementation uses `github.com/google/uuid v1.6.0` for UUID parsing, formatting, and normalization. This is a well-established, minimal-dependency library (2000+ GitHub stars, no transitive dependencies).

## Test Plan

### Unit Tests

- **Go proxy**: `checkUUIDFieldData` validation (valid/invalid UUID strings, type mismatch, lowercase normalization); `validatePrimaryKey` UUID acceptance
- **Go client SDK**: FieldTypeUUID constants, name/string/PbFieldType; ColumnUUID creation/slice; FieldDataColumn UUID deserialization from BytesData
- **Go typeutil**: IsUUIDType, IsPrimaryFieldType-UUID, GetPKSize-UUID; HashKey2Partitions UUID partitioning
- **Go parser**: UUID type comparison, range value casting, template value generation, plan parsing
- **Go index**: All four index checkers accept UUID
- **C++**: Enum value, IsStringDataType(false), ToProtoDataType mapping, InitScalarFieldData, Schema.AddDebugField

### Integration Tests (Python SDK)

- Create collection with UUID PK
- Insert and query by exact UUID match
- Query with IN operator on UUID field
- Delete by UUID expression
- Delete and reinsert with same UUIDs
- Data consistency across multiple field types with UUID PK
- Batch insert and cross-batch UUID query
- UUID as non-PK scalar field
- Invalid UUID string rejection at insert
- UUID normalization to lowercase

Auto-id UUID PK is deferred to the 16-byte storage follow-up.

### Coverage Target

>= 90% for Go unit tests on modified packages; C++ unit test coverage for the new code paths.

## Compatibility

This feature is fully additive. No existing data types, APIs, or storage formats are modified. Collections using Int64 or VarChar PKs continue to work unchanged.

Upgrade path: New collections can use UUID fields immediately after upgrade. Existing collections are unaffected.

## Rejected Alternatives

### 16-byte Binary Storage (Phase 2 — deferred)
The original design stored UUIDs as `FixedSizeBinary(16)` at the storage layer for space efficiency. This is deferred to a follow-up PR because:
- A UUID stored as a canonical string is semantically equivalent to VarChar for ordering, dedup, and delete
- The storage optimization is purely space/perf and can land later without a breaking schema change
- Scoping Phase 1 to VarChar-backed UUID reduces the PR surface and lets the type ship sooner

### Int64 PK with separate UUID column
Forces a synthetic integer key and a secondary lookup, defeating the purpose of a natural UUID identifier. Increases application complexity with no storage benefit.

### UUID auto-generation (v4/v7)
While a future enhancement could add automatic UUID generation for auto-id fields, this first implementation requires clients to provide UUID values. Auto-generation is tracked as a follow-up feature.

## References

- [google/uuid Go library](https://github.com/google/uuid)
- PostgreSQL UUID type documentation
- Related issues: #50956 (Decimal), #50958 (Map) — part of broader scalar type expansion
- Prior art: TIMESTAMPTZ type addition (#44005, #44080)
- milvus-proto PR #636: UUID=31 enum value
- milvus-proto PR #639: UUIDArray in IDs proto (for follow-up)
- MEP template and contribution guidelines from CONTRIBUTING.md
