# MEP: Native UUID Field Type (Usable as Primary Key)

- **Created:** 2026-07-03
- **Author(s):** @BlackPool25
- **Status:** Implemented (Native 16-Byte Binary Storage & Primary Key) — Phase 1: V2-only (V1 rejected via `ValidateStorageV1InsertWritableSchema`); Memory note: growing segment currently one heap allocation per UUID (16B > SSO 15) vs string 36B, disk win real
- **Component:** Proxy / Client SDK / Type System / Storage / Core Engine
- **Related Issues:** [#50957](https://github.com/milvus-io/milvus/issues/50957)

## Summary

Add a native `UUID` scalar field type (`DataType_UUID = 31`) that serves as a first-class collection primary key alongside `Int64` and `VarChar`. UUIDs are represented and stored as true 16-byte fixed-width binaries (`FixedSizeBinary(16)` in Parquet and deltalogs) and serialized over the wire via `IDs.uuid_id` (`UUIDArray`, repeated 16-byte binary entries). Input validation and canonical lowercase formatting happen at the user boundary, while the storage and execution engines operate directly on 16-byte RFC 4122 big-endian binary arrays.

## Motivation

UUIDs are one of the most common entity-identifier formats and are frequently the natural primary key for records ingested into Milvus. Previously, the only option was storing them in `VarChar` primary keys, which was suboptimal:

- No validation: malformed strings were accepted without verification.
- Inefficiency: canonical UUID strings require 36 characters (~36 bytes) vs. 16 bytes for native binary, bloating storage, memory, and index sizes by >2.2x.
- Type ambiguity: string operations (e.g. regex, LIKE, full-text tokenization) are semantically inappropriate for UUIDs.

## Public Interfaces

### Proto (`milvus-io/milvus-proto`)

```protobuf
enum DataType {
  // ...
  UUID = 31;  // Defined in milvus-proto PR #636
}

message IDs {
  oneof id_field {
    LongArray int_id = 1;
    StringArray str_id = 2;
    UUIDArray uuid_id = 3;  // Defined in milvus-proto PR #639
  }
}

message UUIDArray {
  repeated bytes data = 1;  // Repeated 16-byte binary entries
}
```

### Go Client SDK (`client/`)

```go
const FieldTypeUUID FieldType = 31
```

Types and Helpers:
- `entity.FieldTypeUUID`: Constant with `Name()` → `"UUID"`, `String()` → `"string"`
- `column.ColumnUUID`: Backed by `[]string` for user convenience, with `NewColumnUUID(name, values)`
- `column.NewNullableColumnUUID`: For nullable UUID columns
- `columns.IDField2Column()`: Unpacks `IDs.GetUuidId()` 16-byte binary slices into canonical UUID strings
- `columns.FieldDataColumn()`: Deserializes `DataType_UUID` `BytesData` into `ColumnUUID`
- `read_options.column2IDs()`: Packs `ColumnUUID` into `schemapb.IDs_UuidId` (`UUIDArray`)

### Schema & Queries

Users create UUID fields via standard schema definitions:

```python
schema.add_field("id", DataType.UUID, is_primary=True)
schema.add_field("device_uuid", DataType.UUID)
```

UUID fields accept filtering expressions:
```sql
id == "550e8400-e29b-41d4-a716-446655440000"
id in ["550e8400-e29b-41d4-a716-446655440000", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]
id != "550e8400-e29b-41d4-a716-446655440000"
id > "550e8400-e29b-41d4-a716-446655440000"
```

Range semantics:
- Single-sided ranges (`<`, `<=`, `>`, `>=`), `ORDER BY`, `MIN`, and `MAX` are supported because standard big-endian 16-byte binary ordering matches the lexicographical order of canonical lowercase hyphenated UUID strings. `BETWEEN` is rejected by `parser_visitor` (range not supported for UUID).

## Architecture & Implementation Details

### 1. Storage & Arrow Serde
- **Parquet Encoding**: Mapped to Arrow `FixedSizeBinaryType{ByteWidth: 16}`. Serialized with `*array.FixedSizeBinaryBuilder` and read with `*array.FixedSizeBinary`.
- **Primary Keys**: `UUIDPrimaryKey struct { Value [16]byte }` implements `PrimaryKey` (`GT, GE, LT, LE, EQ` via `bytes.Compare`), with a 24-byte in-memory footprint.
- **Batch Primary Keys**: `UUIDPrimaryKeys struct { values [][16]byte }` provides high-throughput batch operations without pointer overhead.
- **Field Data Container**: `UUIDFieldData struct { Data [][16]byte, ValidData []bool, DataType DataType_UUID, Nullable bool }` manages fixed 16-byte memory allocations.
- **Deltalog Tombstones**: Recorded as `FixedSizeBinary(16)` Arrow records for deleted primary keys.
- **Bloom Filters & Statistics**: Hash raw 16-byte binary payloads (`pk[:]`) into Bloom filter bitsets.

### 2. FastPB Codec
- Hot-path deserializer in `pkg/util/fastpb/searchresult.go` decodes `IDs_UuidId` (`case 3`) via `decodeRepeatedBytes` directly into byte slices, bypassing reflection overhead.

### 3. Proxy & REST Gateway
- Input strings in JSON/REST payloads (`query_by_id`, `delete_by_id`, `insert`, quick-create `idType: UUID`) are validated and normalized to canonical lowercase format via `typeutil.NormalizeUUID`.
- Routing (`HashPK2Channels` and `HashKey2Partitions`) uses `bytesRoutingHasher` over canonical 16-byte representations, ensuring partition/channel stability during rolling upgrades.

### 4. Index Support
| Index Type | Support | Note |
|------------|---------|------|
| INVERTED   | ✅      | Inverted index for exact match & IN lookups |
| BITMAP     | ✅      | Bitmap index for multi-value filtering |
| STL_SORT   | ✅      | Scalar sort index for fast range predicates |
| AUTOINDEX  | ✅      | Resolves to scalar index |
| TRIE       | ❌      | String-specific prefix tree rejected at validation |

### 5. C++ Segcore Engine
- `DataType::UUID = 31` is classified as Fixed-Width Scalar (`GetDataTypeSize = 16`, `GetArrowDataType = fixed_size_binary(16)`, `IsStringDataType = false`, `IsPrimitiveType = true`, `IsFixedSizeType = true` via `TypeTraits<UUID>::IsFixedWidth`).
- `GetArrowDataType(DataType::UUID)` returns `arrow::fixed_size_binary(16)`.
- `ConcurrentVector<UUID>`, `ChunkedColumn<UUID>` (`FixedWidthChunk 16`), `InsertRecord` store UUID as `milvus::UUID` (16B) in both growing and sealed segments — no `std::string` heap per value (inline 16B).
- `FieldData` accepts `FIXED_SIZE_BINARY(16)` primary and `STRING` fallback (canonical parse) for rolling upgrade; `ChunkWriter` routes UUID to `FixedWidthChunk`.

## Compatibility & Rolling Upgrade
- **Wire Compatibility**: Proxy accepts `IDs.uuid_id` (field 3) from updated clients while accepting `IDs.str_id` (field 2) from legacy clients as a fallback. FastPB decodes `IDs_UuidId` in-pass.
- **Memory & Storage Footprint**: On-disk Parquet uses 16B fixed binary (vs 36B string) — disk win real. In-memory `UUIDFieldData`/`UUIDPrimaryKeys` and C++ `ConcurrentVector<UUID>`/`ChunkedColumn<UUID>` use contiguous 16B inline storage (no per-value heap; `FixedWidthChunk` 16). Sealed and growing share the same 16B representation — `insert→flush→load→filter` returns identical results.
- **Safe Version Gating**: Creation of UUID collections is validated at RootCoord and Proxy to prevent mixed-cluster deserialization errors before all nodes are upgraded.
- **CVE Compliance**: Preserves all Go module constraints matching security policies (CVE-2026-39822).
