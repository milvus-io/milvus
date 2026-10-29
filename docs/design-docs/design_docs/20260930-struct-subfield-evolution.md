# Adding and dropping sub-fields of an existing struct array field

Status: v2 (2026-09-30). Depends on the native-list / element-null redesign (`20260709-element-level-null.md`, `20260724-recursive-array-struct-column-layout.md`).

## Goal

Let a user add a sub-field to, or drop a sub-field from, an existing `StructArrayField` of a collection that already holds data, without rewriting the collection and without breaking readers of old segments.

## Non-goals

- Changing a struct's `nullable`, renaming or reordering sub-fields, nested structs.
- Adding a sub-field with a default value or with `element_nullable=false`. Reason: Milvus has no array default values. The `schemapb.ValueField` oneof has only scalar variants, no array; the rootcoord create-collection check (`internal/rootcoord/util.go`, "type not support default_value") rejects default values on Array and vector types; the proxy rejects any default value on a struct sub-field (`internal/proxy/ddl/task.go`, "default value is not supported for struct field"). The only fill value available at the element level is null.
- Per-field garbage collection of dropped columns in live V3 manifests; the existing rule stays: a dropped column is reclaimed when the segment is rewritten by compaction or dropped.
- Emb-list (MAX_SIM) indexes on an added vector sub-field: an added sub-field is `element_nullable=true`, and MAX_SIM on `element_nullable=true` vector arrays is out of scope for the element-level null project.

## Why this is not "just another add field"

A struct array is stored as one physical Array column per sub-field. The invariant that makes it a struct is enforced everywhere: for every row, all sub-fields have the same row validity and the same element count. Segcore keeps one `StructElementOffsets` per struct, built from one representative sub-field and aliased to the others; `element_filter`, `MATCH_*`, element-level ANN and struct output all read through it. The proxy insert path, the import consistency check, the Go readers (`rw.go: "a struct array is physically all-or-nothing"`), the bump-schema compactor, the Go client and the REST accessor all check the same invariant.

Every existing "fill an absent field" path (proxy, write buffer, import `AppendNullableDefaultFieldsData`, `GenerateEmptyArrayFromSchema` in the absent-field record reader, bump-schema additive reconciliation, C++ `DefaultValueChunkTranslator` for sealed segments, `bulk_subscript_not_exist_field` / `fill_empty_field` for growing segments) produces a row-level null, i.e. zero elements. For a sub-field that violates the invariant: the shared offsets say N elements, the column has none, and element-level readers assert or skip rows. Schema evolution therefore rejects sub-field add/drop today.

For an ordinary scalar field with a default value, these paths backfill old rows with the default (`DefaultValueChunkTranslator` for sealed, `fill_empty_field` for growing, `GenerateEmptyArrayFromSchema` in Go); arrays have no default value, so a sub-field can only be filled with N null elements.

## Design

### Semantics

- **Added sub-field.** Must be `element_nullable=true`; `nullable` equals the parent struct's (as for all sub-fields): when the parent struct is `nullable=false`, the sub-field is `nullable=false` too, so adding a sub-field to a `nullable=false` struct is allowed; no default value. Allowed types: scalar `Array<T>`, `ArrayOfVector`, nested `Array<Array<T>>` (its `element_nullable` marks the inner arrays). It is appended at the end of the struct's sub-field list and gets a fresh field id.
- **Value on rows written before the DDL.** Two cases:
  - The row's struct is a null row: the new sub-field is a null row there too (validity=false, zero elements); there is no N.
  - The row's struct is non-null with N elements (N taken from any sibling): the new sub-field holds N null elements; for N=0 it is a non-null empty array.

  Row validity is shared by all sub-fields, so this is the same rule the siblings follow, the struct invariant holds exactly, and nothing downstream needs a special case.
- **Dropped sub-field.** Removed from the schema. Its indexes are dropped by the existing cascade: rootcoord puts the sub-field id into `DroppedFieldIds`, and the AlterCollection ack callback drops every index on it in DataCoord, as for a top-level field. A sealed segment releases the sub-field's column, loaded index and offsets alias like those of a dropped top-level field, when the segment is first reopened with the new schema: on the first search or query that carries it, or on a Reopen from QueryCoord. A growing segment keeps the alias for readers that still hold the previous schema and keeps the column and interim index until the segment is released (see [Struct offsets in segcore](#struct-offsets-in-segcore)). Dropping the struct's last sub-field is rejected, and so is a drop that would leave only sub-fields added after the struct was created (see "Original sub-fields" below); to remove the whole struct, drop the struct explicitly (`DropRequest{field_name:"s"}` or the struct's field id), and existing rootcoord logic puts the struct id and all sub-field ids into `droppedFieldIds`. Existing rules apply: cannot drop the collection's last vector field, cannot drop a sub-field referenced by a function.
- **Struct row validity and "first sub-field" semantics.** Row validity is shared by all sub-fields, so `struct IS NULL` and `MatchExpr`'s struct validity keep working whichever sub-field is first. New sub-fields are appended, so the first sub-field only changes when the first one is dropped; the offsets provider is then re-picked (below).

### API

- **Adding a sub-field takes the same path as a regular add field:** `AlterCollectionSchema` + `AddRequest.field_infos` with `field_schema.name` = `s[b]`; `FieldInfo` is the unit of `AddRequest` (a field plus an optional index binding). The Go SDK's `AddCollectionField` and REST `fields/add` take the sub-field path as the field name (see [Client compatibility](#client-compatibility)).
- **The name decides between a top-level add and a sub-field add:** a `field_schema.name` without `[` is a regular top-level add, unchanged; a name `s[b]` appends sub-field `b` to the existing struct `s`. No proto field is added: `DropRequest.field_name` already addresses a sub-field the same way.
- **Path grammar:** the stored-name grammar applied recursively. Segments are joined with `[]`; every segment but the last is a struct name and the last is the sub-field's raw name: `s[b]` is sub-field b of the top-level struct s, and `a[b][c]` would be sub-field c of the sub-struct b inside a. Arrays have no named members and never appear as a container in the path, so struct / array nesting in any combination does not change the grammar. The name given for the new sub-field is its stored name, and `DropRequest.field_name` and expressions use the same grammar. User field names allow only letters, digits and underscores (`validateFieldName`), so `[` and `]` never occur inside a segment and the path is unambiguous. Nested structs are out of scope this round: the name takes exactly one struct segment before the sub-field name, and more segments (`a[b][c]`) fail with "nested struct is not supported" instead of being truncated silently.
- **The sub-field is a `FieldSchema` named with its path `s[b]`**, which is also its stored name; the proxy and rootcoord split it into the parent struct `s` and the raw name `b`. A path whose segments are not valid field names is rejected, and so is a sub-field add combined with a function. `index_name` / `extra_params` are rejected on a sub-field add ("binding an index while adding a struct sub-field is not supported yet"). Element-nullable vector sub-fields can be indexed with element-level metrics, so the rejection is no longer forced by the data; binding is not implemented yet, and the index is created with a separate CreateIndex (see [Indexing an added vector sub-field](#indexing-an-added-vector-sub-field)).
- The proxy resolves the parent struct from the path, validates the sub-field with `ValidateFieldsInStruct` plus the sub-field role bans, and forwards. All sub-fields of a struct share one `max_capacity`: it may be omitted on the added sub-field, and proxy and rootcoord then fill it from the siblings; a given value that differs from the siblings' is rejected. Rootcoord appends the sub-field to the struct, bumps the schema version and broadcasts, as for a top-level add.
- The current `AddRequest` limit of one `field_info` per request is inherited.
- **Altering a struct or a sub-field:** `alter_collection_field` takes a struct field name or a stored sub-field name (`s[b]`). It accepts only `mmap.enabled`, `field.description` and `warmup` there; any other key, including `max_capacity`, `max_length` and the analyzer params, is rejected with the field name in the error, also when the value would not change.
- **Dropping a sub-field is unchanged:** `DropRequest{field_name:"s[b]"}` or `field_id`. A sub-field with a protected role (primary key, partition or clustering key, function output, and the like) cannot be dropped. Both already reach the server; only the proxy and schema-evolution rejections are lifted. `DroppedFieldIds = [sub_id]` so the index cascade works unchanged. Dropping the whole struct: `DropRequest{field_name:"s"}`.
- **Examples (pymilvus).** A sub-field describes one struct element, so its type is the element type (`VARCHAR`, `FLOAT_VECTOR`, or `ARRAY` for a nested `Array<Array<T>>` sub-field), not the stored column type.

Create a collection with a struct field:

```python
from pymilvus import DataType, MilvusClient

client = MilvusClient(uri="http://localhost:19530")

clips = client.create_struct_field_schema()
clips.add_field("score", DataType.INT64, nullable=True)
clips.add_field("emb", DataType.FLOAT_VECTOR, dim=8, nullable=True)
clips.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR,
                max_capacity=4, max_length=16, nullable=True, element_nullable=True)

schema = client.create_schema()
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("vec", DataType.FLOAT_VECTOR, dim=8)
schema.add_field("clips", DataType.ARRAY, element_type=DataType.STRUCT,
                 struct_schema=clips, max_capacity=100, nullable=True)
client.create_collection("c", schema=schema)
```

On the struct field, `nullable` is the row-level null shared by all sub-fields and `max_capacity` is the number of elements per row. On a sub-field, `nullable` lets one element be null and is sent as `element_nullable`; for `tags` the element is an inner array. On the `ARRAY` sub-field `tags`, `max_capacity` is the capacity of each inner array and `element_nullable` lets a leaf be null.

Add a whole struct (`AddCollectionStructField`):

```python
meta = client.create_struct_field_schema()
meta.add_field("lang", DataType.VARCHAR, max_length=16, nullable=True)
client.add_collection_struct_field("c", "meta", struct_schema=meta, max_capacity=100)
```

An added struct is always row-nullable, since rows written before it have no value; `nullable=False` is rejected by the client. `max_capacity` is the struct's elements per row.

Add a sub-field to an existing struct (`AlterCollectionSchema` with `FieldInfo.field_schema.name` set to the sub-field path, such as `clips[lang]`):

```python
client.add_collection_field("c", "clips[lang]", DataType.VARCHAR, max_length=16, nullable=True)
client.add_collection_field("c", "clips[emb2]", DataType.FLOAT_VECTOR, dim=8, nullable=True)
client.add_collection_field("c", "clips[more]", DataType.ARRAY, element_type=DataType.INT64,
                            max_capacity=4, nullable=True, element_nullable=True)
```

`nullable=True` is required: it is the added sub-field's `element_nullable`, and rows written before the DDL read N null elements. For the `ARRAY` sub-field `clips[more]`, `nullable` is the inner array, `element_nullable` is the leaf and `max_capacity` is the inner array's capacity. The struct's capacity and row nullability are not given; the server takes them from the struct.

Drop a sub-field, or the whole struct (`AlterCollectionSchema` with `DropRequest.field_name`):

```python
client.drop_collection_field("c", field_name="clips[lang]")
client.drop_collection_field("c", field_name="clips")
```

- **Field id:** rootcoord allocates `maxAssignedFieldIDFromSchema + 1` in the DDL callback, ignores any id sent by the client, and writes it back to the collection's max_field_id property; this is the same function add field / add struct use.
- **Alternatives considered and rejected:**
  1. A separate `FieldInfo.struct_path` field (`string struct_path = 4`) naming the parent struct, with the sub-field carrying its raw name `b`: it makes the parent explicit in the proto, but clients already pass the `s[b]` path as the field name (pymilvus `add_collection_field(c, "clips[lang]", ...)`, the Go SDK's `AddCollectionField`, REST `fields/add`), and the server already parses that form for drop (`DropRequest.field_name`) and for expressions. A separate field duplicated the information in the name and needed a proto change plus a rule for a name and a path that disagree.
  2. Give `AddCollectionStructField` merge semantics when the struct name already exists: it silently turns a duplicate-name error into a mutation.
  3. Put a `StructArrayFieldSchema` listing only the new sub-fields into `AddRequest`: matches the create-collection form, but its own `nullable`, `type_params` and `description` must be either ignored or checked for equality on merge, an extra layer of ambiguity; and there is no place for an index binding.
  4. ClickHouse-style restatement of the whole struct type (`MODIFY COLUMN s Tuple(...)`): a Milvus struct is a set of physical columns, each with its own id, not a type, so an incremental operation fits better, and users should not have to restate existing members.

  Precedents: Postgres composite types, `ALTER TYPE s ADD ATTRIBUTE b int` (explicit container); Delta Lake / BigQuery `ADD COLUMN s.b` (path name in SQL text). Milvus already addresses an existing sub-field by its `s[b]` path, so adding one takes the same path, as in Delta Lake and BigQuery.

### Schema evolution rules (`schemautil/schema_evolution.go`)

Replace the two blanket rejections for kept structs with:
- an added sub-field must be `element_nullable=true`, must not carry a default value or any protected role, must have a fresh id above `max_field_id`, and must have the struct's `nullable` (true or false);
- a dropped sub-field must not be the struct's last sub-field, the collection's last vector, or a function input/output;
- after a drop, the struct must keep at least one original sub-field (below);

### Original sub-fields

A segment written before a sub-field was added stores no column for it, and every reader (Go record readers, compaction, sealed load and Reopen) derives its per-row element counts and row validity from a sibling that the segment stores and the schema still has. An original sub-field is one that has been in the schema since the struct appeared: it was declared at collection creation or came with the whole struct through `AddCollectionStructField`. Every segment holding a row written after the struct appeared stores every original sub-field, so a remaining original sub-field is always a usable sibling.

- Rootcoord marks a sub-field added to an existing struct through `AlterCollectionSchema` with the internal type param `__struct_sub_field_added__`. A sub-field without the marker is original; existing collections are not migrated. Users cannot set, alter or delete the key (create, add field, add struct, add sub-field and alter field reject it), DescribeCollection strips it, and index creation does not copy it into index type params.
- Drop rule: a drop that leaves a struct with only added sub-fields is rejected with a parameter error, in the proxy and in rootcoord schema evolution. Dropping the whole struct is always allowed. The rule is conservative: once all original sub-fields are gone the struct can only be dropped as a whole, even after compaction has written the added sub-fields into every segment.
- Read-side check: when a segment stores no sub-field of a struct that is in the current schema, a reader decides by field ID. Field IDs only grow, and a struct's sub-fields get IDs above the struct's own ID. If the segment stores any field with an ID above the struct's ID, it was written after the struct appeared and all the sub-fields it stores were dropped; the read fails instead of producing whole-row nulls or empty arrays, with DataIntegrity in the Go readers and DataFormatBroken in the sealed loader. Otherwise the segment predates the struct and its rows are null struct rows, which is correct. The Go readers (V1, packed and manifest, and with them every compaction) and the sealed loader apply this check; with the drop rule in place it does not fire for collections whose DDL went through it.
- everything else about kept sub-fields stays immutable.

### Producing the N-null-elements value

The backfill contract is implemented once per layer and shared by all callers in that layer.

| Layer | Site | Change |
|---|---|---|
| Proxy insert | `checkAndFlattenStructFieldData` | No distinction between sub-fields added after collection creation and the rest; the origin marker (see [Original sub-fields](#original-sub-fields)) is not consulted here. Rule: any `element_nullable=true` sub-field may be omitted on write, and the proxy synthesizes a compact row with `valid_data = N × false` from the per-row element counts it already computed from the siblings; a missing `element_nullable=false` sub-field stays an error. A gRPC request that carries a sub-field column the struct does not have, such as a dropped sub-field from a stale client, is rejected with ParameterInvalid ("sub-field ... does not exist in struct field ..."), and so is a repeated sub-field; schema-version pinning already covers clients that opt in. REST ignores such keys (see [Client compatibility](#client-compatibility)). Insert and upsert (`checkAndFlattenStructFieldData` in PreExecute) accept a struct whose sub-fields are all present, or whose missing sub-fields are all `element_nullable=true`. Cost: a client that omits an `element_nullable=true` sub-field by mistake silently gets nulls, the same as omitting a `nullable=true` top-level field. |
| Import | `internal/datanode/importv2/util.go` (`AppendNullableDefaultFieldsData`, `CheckStructArrayConsistency`), parquet/json/csv/binlog readers | Not supported yet: `ValidateImportSchema` already rejects import into any collection with a native-list field, and an added sub-field is always one. When that gate is lifted, an `element_nullable=true` sub-field may be absent: same synthesis from siblings instead of "partial sub-field set" rejection; the vector element count must use `valid_data`; the struct readers accept a missing `element_nullable=true` sub-field. |
| Go readers and compaction | `filterSchemaToPresentFields`, `validateSchemaBumpIntegrity`, `absentFieldFillRecordReader` / `GenerateEmptyArrayFromSchema`, bump-schema additive reconciliation | Lift "all-or-nothing". The absent-field filler takes the record and, for a struct sub-field, builds `list<T>` / `list<Binary>` rows with N null children from a present sibling column's list offsets (nested: N null inner lists). Mix/sort/clustering then write the column physically; bump-schema compaction uses the same filler. |
| Sealed segments (C++) | `FillDefaultValueFields` → `DefaultValueChunkTranslator` | The translator receives the struct's `StructElementOffsets` and builds, per cell, an Arrow list array with N null children per row, then the normal chunk writer. Ordering already guarantees siblings are loaded before default fill. |
| Growing segments (C++) | `Reopen` → `fill_empty_field`, `Insert` backfill, `bulk_subscript_not_exist_field` | For a struct sub-field, produce N null elements per row from the struct's `StructElementOffsetsGrowing`; growing segments are fenced at DDL time, so this only serves segments still open on query nodes. |

**Packed binlogs without `ChildFields`.** StorageV2 segments flushed by 2.6.0/2.6.1, and binlogs rebuilt by import, carry no `FieldBinlog.ChildFields` and key `FieldID` by column group, so the binlog metadata does not say which fields a packed file stores. When `ChildFields` are missing, the Go binlog reader and mix, sort and clustering compaction read the `PARQUET:field_id` of every column from each file's footer (metadata only, one read per file) and take the union over all files of every column group; the reader then filters the schema to those fields and fills the rest with the absent-field filler above. An element-nullable sub-field stored in only some files of its group is rejected with DataIntegrity, because the packed engine would fill the other files with null rows. Bump-schema compaction only takes StorageV3 manifests; external sources keep using `ChildFields` only. In C++, `Load` and `Reopen` fill in the child fields of legacy StorageV2 groups (group id below `START_USER_FIELDID`) from the first file's footer before computing the load diff. A footer read failure keeps the error code the storage layer attached (for example, a transient object-storage error stays retriable), and a column without a field id is DataFormatBroken; neither is turned into an input error.

### Struct offsets in segcore

- `StructElementOffsets` must accept `element_nullable=true` sub-fields (logical lengths: `ColumnarArrayChunk` root offsets are already logical; `VectorArrayChunk` has logical lengths). This lifts the `NotImplemented` in `GetStructElementOffsets`, `StructElementOffsetsSealed::BuildFromColumn` and the skips in `prepare_struct_element_offsets` / `InitializeStructElementOffsets` / `EnsureStructElementOffsetsForField`.
- The offsets provider becomes deterministic. Sealed: a sub-field that has real data in the segment, a scalar `Array` before an `ArrayOfVector` (whose raw read is expensive) and then the lowest field id, the same rule segment loading uses to prepare the offsets before publishing; a backfilled sub-field is used only when no sub-field has real data and the segment predates the struct (see [Original sub-fields](#original-sub-fields)); a segment whose stored sub-fields were all dropped fails to load instead. The rule picks among the sources staged by the load operation that creates or rebuilds the offsets, after that operation's struct columns are loaded and before default fill. A sibling loaded later shares the offsets already published and does not re-pick the source. Growing: representative = first sub-field in proto order; on `Reopen` after a drop, re-pick it among the remaining sub-fields and keep the offsets object. The dropped sub-field's alias stays and keeps resolving to the struct's offsets, for readers that still hold the previous schema: all aliases of a struct point to the same offsets object, and field ids are never reused. Its column and interim index are not released: inserts after the drop no longer write or feed them, and both are freed with the growing segment.
- Backfilled columns are built from the provider's offsets, so aliasing them to the struct's offsets is exact. This also makes "drop a sub-field, re-add one with the same name" correct, which the current generation check (`InvalidateStaleStructElementOffsets`) cannot guarantee.

### Reads on the added sub-field

Retrieval returns N nulls per row for old rows. Row-level filters, element-level filters and `MATCH_*` see the null elements under three-valued logic, as for any element-nullable sub-field ([element-level null](./20260709-element-level-null.md#query-and-index)).

Output of a sub-field that a segment does not store uses the same dense layout as a stored sub-field column, so it has as many rows as its siblings: a null struct row is an empty placeholder with row validity false; a scalar row of N elements holds N zero values and N false element flags; an `Array<T>` row holds N empty inner arrays of the leaf type and N false element flags; a vector row holds no vectors and N false element flags. N and the row validity come from a stored sibling.

### Indexing an added vector sub-field

An added `ArrayOfVector` sub-field is element-nullable, so it takes element-level metrics only; MAX_SIM is rejected. Load requires an index on every vector field it loads, struct sub-fields included, so a later load that includes the new sub-field (the default load of all fields does) needs a CreateIndex on it first; a load with an explicit field list may leave it out. Adding it to a loaded collection does not release the collection, and retrieval of the new sub-field works before the index exists. DataCoord's indexed-segment filter waits on every vector field, struct vector sub-fields included, so until the index exists it counts no segment of the collection as indexed.

### Client compatibility

- Clients that omit an `element_nullable=true` sub-field on insert keep working (synthesis in the proxy). Clients that pin `schemaTimestamp` behave as before: they get `ErrCollectionSchemaMismatch` and refresh, as for top-level adds.
- A collection loaded with an explicit load-field list still serves the added sub-field before it is released and reloaded: partial load fields are a hint in segcore (`Schema::load_fields` returns every field), so the sub-field is backfilled on `Reopen` like any other added field. This is the existing behaviour for top-level added fields and is kept.
- REST: `fields/add` with `fieldName: "clips[lang]"` sends `AlterCollectionSchema` with `clips[lang]` as the field name. Malformed and multi-segment paths are rejected. An `Array<T>` sub-field is declared with `typeSchema`; the server fills its root `max_capacity` from the sibling sub-fields. `fields/drop` takes `clips[lang]`. The REST struct row parser ignores keys that name no sub-field, so a stale client writing a dropped sub-field succeeds and the dropped value is discarded, as in JSON and CSV import; it rejects duplicate keys, and a missing key of an element-nullable sub-field is a null element.
- Go SDK: `AddCollectionField` with a field named `clips[lang]` sends `AlterCollectionSchema` with that name as the field name; the field describes one struct element, as in `entity.StructSchema`, and the server fills its row-level `nullable` and `max_capacity` from the struct. `DropCollectionField("clips[lang]")` drops it. Struct columns are built from the schema, so an omitted element-nullable sub-field is filled with null elements; struct results accept the added sub-field.

## Risks

1. Memory of backfilled scalar sub-fields in segcore: `ColumnarArrayChunk` stores dense placeholders, so N null int64 elements cost 8N bytes per row until the segment is rewritten. This is the same cost as any all-null `element_nullable=true` array; if it matters, a payload-less "all leaves null" leaf variant can be added later.
2. Index builds on an added vector sub-field before old segments are backfilled physically: in DataCoord, `isMissingVectorArrayFieldOnStaleSchema` treats the absent column of an older-schema segment as zero vectors, so that segment's index task finishes without a build. This is right for an element-level index that skips nulls, and wrong for nothing else because MAX_SIM is excluded.
3. Bump-schema compaction is off by default and V3-only; the sibling-aware fillers must therefore stay in place permanently, they are not a transition measure.
