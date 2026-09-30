# Adding and dropping sub-fields of an existing struct array field

Status: v2 (2026-09-30), review decisions incorporated. Depends on the native-list / element-null redesign (`20260709-element-level-null.md`, `20260724-recursive-array-struct-column-layout.md`).

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

- **Added sub-field.** Must be `element_nullable=true`; `nullable` equals the parent struct's (as today for all sub-fields): when the parent struct is `nullable=false`, the sub-field is `nullable=false` too, so adding a sub-field to a `nullable=false` struct is allowed; no default value. Allowed types: scalar `Array<T>`, `ArrayOfVector`, nested `Array<Array<T>>` (its `element_nullable` marks the inner arrays). It is appended at the end of the struct's sub-field list and gets a fresh field id.
- **Value on rows written before the DDL.** Two cases:
  - The row's struct is a null row: the new sub-field is a null row there too (validity=false, zero elements); there is no N.
  - The row's struct is non-null with N elements (N taken from any sibling): the new sub-field holds N null elements; for N=0 it is a non-null empty array.

  Row validity is shared by all sub-fields, so this is the same rule the siblings follow, the struct invariant holds exactly, and nothing downstream needs a special case.
- **Dropped sub-field.** Removed from the schema; its column, index and offsets alias are released like a dropped top-level field. Dropping the struct's last sub-field is rejected; to remove the whole struct, drop the struct explicitly (`DropRequest{field_name:"s"}` or the struct's field id), and existing rootcoord logic puts the struct id and all sub-field ids into `droppedFieldIds`. Existing rules apply: cannot drop the collection's last vector field, cannot drop a sub-field referenced by a function.
- **Struct row validity and "first sub-field" semantics.** Row validity is shared by all sub-fields, so `struct IS NULL` and `MatchExpr`'s struct validity keep working whichever sub-field is first. New sub-fields are appended, so the first sub-field only changes when the first one is dropped; the offsets provider is then re-picked (below).

### API

- **Adding a sub-field takes the same path as a regular add field:** `AlterCollectionSchema` + `AddRequest.field_infos`. The Go SDK's `AddCollectionField` and REST `fields/add` already go through it today; `FieldInfo` is the unit of `AddRequest` (a field plus an optional index binding).
- **Proto change (milvus-proto):** `FieldInfo` gets `string struct_path = 4`. An empty `struct_path` means a regular top-level add, unchanged; a non-empty one appends a sub-field to the struct the path points at.
- **Path grammar:** the stored-name grammar applied recursively. Every segment is a struct name, joined with `[]`: `a` is the top-level struct a, `a[b]` is the sub-struct named b inside a. Arrays have no named members and never appear as a container in the path, so struct / array nesting in any combination does not change the grammar. The stored name of the new sub-field is `struct_path + "[" + name + "]"`, and `DropRequest.field_name` uses the same grammar (`a[b][c]`). User field names allow only letters, digits and underscores (`validateFieldName`), so `[` and `]` never occur inside a segment and the path is unambiguous. Nested structs are out of scope this round: `struct_path` accepts exactly one segment, and more segments fail with "nested struct is not supported" instead of being truncated silently.
- **The sub-field is a bare `FieldSchema`** named with its raw name `b`; rootcoord turns it into the stored name `s[b]` with the existing `normalizeStructSubFieldName` (which also accepts the `s[b]` form for the same struct). `index_name` / `extra_params` keep their meaning: they can bind an element-level index to a vector sub-field.
- The proxy resolves the parent struct by `struct_path`, validates the sub-field with `ValidateFieldsInStruct` plus the sub-field role bans and the "same `max_capacity` as siblings" rule, and forwards. Rootcoord appends the sub-field to the struct, bumps the schema version and broadcasts as today.
- The current `AddRequest` limit of one `field_info` per request is inherited.
- **Dropping a sub-field is unchanged:** `DropRequest{field_name:"s[b]"}` or `field_id`. Both already reach the server; only the proxy and schema-evolution rejections are lifted. `DroppedFieldIds = [sub_id]` so the index cascade works unchanged. Dropping the whole struct: `DropRequest{field_name:"s"}`.
- **Proto comparison (pseudocode):**

```
// create collection
CreateCollectionRequest.schema = CollectionSchema{
  struct_array_fields: [ StructArrayFieldSchema{
    name: "s", nullable: true,
    fields: [ FieldSchema{name:"a", data_type:Array, element_type:Int64, element_nullable:true} ] } ] }

// add a whole struct (existing RPC)
AddCollectionStructFieldRequest{ collection_name:"c",
  struct_array_field_schema: StructArrayFieldSchema{ name:"s", nullable:true, fields:[FieldSchema{...}] } }

// add a sub-field (this design)
AlterCollectionSchemaRequest{ collection_name:"c", action:{ add_request: AddRequest{
  field_infos: [ FieldInfo{
    struct_path: "s",
    field_schema: FieldSchema{ name:"b", data_type:Array, element_type:Float, element_nullable:true },
    index_name: "", extra_params: [] } ] } } }

// drop a sub-field / drop the whole struct
AlterCollectionSchemaRequest{ action:{ drop_request: DropRequest{ field_name:"s[b]" } } }   // or field_id
AlterCollectionSchemaRequest{ action:{ drop_request: DropRequest{ field_name:"s" } } }
```

- **Field id:** rootcoord allocates `maxAssignedFieldIDFromSchema + 1` in the DDL callback, ignores any id sent by the client, and writes it back to the collection's max_field_id property; this is the same function add field / add struct use.
- **Alternatives considered and rejected:**
  1. Encode the parent in the field name, `FieldSchema{name:"s[b]"}`: no proto change, but the parent-child relation is hidden in a string and the server has to parse the name to find the parent struct. The path name `s[b]` is reserved for addressing things that already exist (describe output, drop, expressions); creating something new uses an explicit parent name.
  2. Give `AddCollectionStructField` merge semantics when the struct name already exists: it silently turns a duplicate-name error into a mutation.
  3. Put a `StructArrayFieldSchema` listing only the new sub-fields into `AddRequest`: matches the create-collection form, but its own `nullable`, `type_params` and `description` must be either ignored or checked for equality on merge, an extra layer of ambiguity; and there is no place for an index binding.
  4. ClickHouse-style restatement of the whole struct type (`MODIFY COLUMN s Tuple(...)`): a Milvus struct is a set of physical columns, each with its own id, not a type, so an incremental operation fits better, and users should not have to restate existing members.

  Precedents: Postgres composite types, `ALTER TYPE s ADD ATTRIBUTE b int` (explicit container); Delta Lake / BigQuery `ADD COLUMN s.b` (path name in SQL text). The proto has structure available, so the explicit container is chosen.

### Schema evolution rules (`schemautil/schema_evolution.go`)

Replace the two blanket rejections for kept structs with:
- an added sub-field must be `element_nullable=true`, must not carry a default value or any protected role, must have a fresh id above `max_field_id`, and must have the struct's `nullable` (true or false);
- a dropped sub-field must not be the struct's last sub-field, the collection's last vector, or a function input/output;
- everything else about kept sub-fields stays immutable.

### Producing the N-null-elements value

The backfill contract is implemented once per layer and shared by all callers in that layer.

| Layer | Site | Change |
|---|---|---|
| Proxy insert | `checkAndFlattenStructFieldData` | No distinction between sub-fields added after collection creation and the rest; the schema has no such marker. Rule: any `element_nullable=true` sub-field may be omitted on write, and the proxy synthesizes a compact row with `valid_data = N × false` from the per-row element counts it already computed from the siblings; a missing `element_nullable=false` sub-field stays an error. A stale client whose request carries a dropped sub-field is rejected with a parameter error (schema-version pinning already covers clients that opt in). Today `checkAndFlattenStructFieldData` (called from insert / upsert PreExecute) requires the sub-field count to equal the schema's exactly and all-or-nothing presence; both are relaxed to "all present, or every missing one is `element_nullable=true`". Cost: a client that omits an `element_nullable=true` sub-field by mistake silently gets nulls, the same as omitting a `nullable=true` top-level field today. |
| Import | `AppendNullableDefaultFieldsData`, `CheckStructArrayConsistency` | An `element_nullable=true` sub-field may be absent: same synthesis from siblings instead of "partial sub-field set" rejection; the vector element count must use `valid_data`. Parquet/JSON/CSV struct readers accept a missing `element_nullable=true` sub-field. |
| Go readers and compaction | `filterSchemaToPresentFields`, `validateSchemaBumpIntegrity`, `absentFieldFillRecordReader` / `GenerateEmptyArrayFromSchema`, bump-schema additive reconciliation | Lift "all-or-nothing". The absent-field filler takes the record and, for a struct sub-field, builds `list<T>` / `list<Binary>` rows with N null children from a present sibling column's list offsets (nested: N null inner lists). Mix/sort/clustering then write the column physically; bump-schema compaction uses the same filler. |
| Sealed segments (C++) | `FillDefaultValueFields` → `DefaultValueChunkTranslator` | The translator receives the struct's `StructElementOffsets` and builds, per cell, an Arrow list array with N null children per row, then the normal chunk writer. Ordering already guarantees siblings are loaded before default fill. |
| Growing segments (C++) | `Reopen` → `fill_empty_field`, `Insert` backfill, `bulk_subscript_not_exist_field` | For a struct sub-field, produce N null elements per row from the struct's `StructElementOffsetsGrowing`; growing segments are fenced at DDL time, so this only serves segments still open on query nodes. |

### Struct offsets in segcore

- `StructElementOffsets` must accept `element_nullable=true` sub-fields (logical lengths: `ColumnarArrayChunk` root offsets are already logical; `VectorArrayChunk` has logical lengths). This lifts the `NotImplemented` in `GetStructElementOffsets`, `StructElementOffsetsSealed::BuildFromColumn` and the skips in `prepare_struct_element_offsets` / `InitializeStructElementOffsets` / `EnsureStructElementOffsetsForField`.
- The offsets provider becomes deterministic: the first sub-field in proto order that has real data in the segment. Sealed: build after all struct columns of the segment are loaded and before default fill. Growing: representative = first sub-field in proto order; on `Reopen` after a drop, re-pick and keep the offsets object; erase the dropped sub-field's alias and release its column and interim index.
- Backfilled columns are built from the provider's offsets, so aliasing them to the struct's offsets is exact. This also makes "drop a sub-field, re-add one with the same name" correct, which the current generation check (`InvalidateStaleStructElementOffsets`) cannot guarantee.

### Reads on the added sub-field

Retrieval returns N nulls per row for old rows. Element-level filters and `MATCH_*` see null elements under three-valued logic; this needs the element-level read path for `element_nullable=true` scalar arrays (`ArrayValueView` with validity) and the corresponding vector-array path from the query-layer plan. Until those land, expressions on the added sub-field return the existing "not supported yet" error, while insert, flush, load, compaction and retrieve work.

### Client compatibility

- Clients that omit an `element_nullable=true` sub-field on insert keep working (synthesis in the proxy). Clients that pin `schemaTimestamp` behave as before: they get `ErrCollectionSchemaMismatch` and refresh, as for top-level adds.
- Go client / REST: struct column parsing must accept the new sub-field; `nullable` equal to the parent struct's keeps `ValidateNullable` valid. The Go SDK gets `AddCollectionStructSubField(structName, field)`, and dropping uses `DropCollectionField("s[b]")`; REST `fields/add` passes `struct_path` through, and `fields/drop` passes `s[b]` through.

## Phases

- **B0 prerequisites** (shared with the query layer): `StructElementOffsets` for `element_nullable=true` sub-fields with logical lengths, deterministic offsets provider, sealed `EnsureStructElementOffsetsForField` aligned with the growing skip rules, element-level read of `element_nullable=true` scalar sub-fields.
- **B1 drop sub-field**: proxy + rootcoord + schema evolution, growing `Reopen` drop handling, index cascade, integration test (drop first / middle / last vector sub-field, query and element filter afterwards, compaction, restart; plus the test matrix below).
- **B2 add sub-field**: prerequisite: milvus-proto adds `FieldInfo.struct_path` and the go-api dependency is bumped. DDL path, all five backfill sites, proxy synthesis, import, Go reader / compaction filler, integration test (add scalar / vector / nested sub-field to a collection with growing, sealed, compacted and mmap-loaded segments; old client omitting the field; drop then re-add same name; plus the test matrix below).
- **B3 SDK and REST**.

### Test matrix

The B1/B2 integration tests must cover:

1. Create the collection without a struct → insert some rows → add struct S with `AddCollectionStructField` (these old rows are null rows) → insert more rows (with N elements) → add a sub-field to S. The new sub-field is a null row on the first batch and N null elements on the second batch; verify both.
2. On the later-added S: drop the first sub-field (offsets provider re-picked), drop down to one sub-field and then add, drop and then re-add with the same name.
3. Cover every state above on growing segments, sealed segments, compacted segments and mmap-loaded segments, and run one compaction and one restart.
4. Add a sub-field to a `nullable=false` struct; insert, query, compact.
5. A stale client writing a dropped sub-field must be rejected.

## Review decisions (2026-09-30)

1. Adding a sub-field to a `nullable=false` struct is allowed.
2. A stale client whose request carries a dropped sub-field is rejected.
3. The API uses `FieldInfo.struct_path` (the design above), not name encoding; the field is a path rather than a name so that nested structs can later use the `a[b]` form.
4. The proxy omission rule looks only at `element_nullable`; there is no "added after collection creation" check.

## Risks

1. Memory of backfilled scalar sub-fields in segcore: `ColumnarArrayChunk` stores dense placeholders, so N null int64 elements cost 8N bytes per row until the segment is rewritten. This is the same cost as any all-null `element_nullable=true` array; if it matters, a payload-less "all leaves null" leaf variant can be added later.
2. Index builds on an added vector sub-field before old segments are backfilled physically: `isMissingVectorArrayFieldOnStaleSchema` treats the absent column as zero vectors, which is right for an element-level index that skips nulls, and wrong for nothing else because MAX_SIM is excluded.
3. Bump-schema compaction is off by default and V3-only; the sibling-aware fillers must therefore stay in place permanently, they are not a transition measure.
