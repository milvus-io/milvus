# MEP: Evictable Support for Tiered Storage

- **Created:** 2026-07-01
- **Author(s):** @sparknack
- **Status:** Implemented
- **Component:** QueryNode | Proxy | Coordinator | Storage | Index | SDK
- **Related Issues:** #50984
- **Released:** N/A

---

## Summary

This design adds user-facing `evictable` controls for Milvus tiered storage
resources. Users can keep selected field data or indexes out of
policy-based eviction while preserving the existing default behavior for all
resources that are evictable today.

## Motivation

Milvus tiered storage already decides whether a cache slot can participate in
policy-based eviction through `cachinglayer::Meta.support_eviction`. Most
user-loadable resources currently default to evictable when tiered storage
eviction is enabled, but users cannot opt out per field or per index.

This is limiting for workloads where a small set of latency-sensitive fields or
indexes should stay resident while other resources can still be evicted.
Existing `warmup` controls when data is proactively loaded. It does not control
whether the loaded cache slot can later be evicted.

The goal is to expose that existing segcore capability through metadata,
QueryCoord load info, QueryNode resource estimation, and SDK helpers.

## Public Interfaces

Milvus adds the common property key:

```text
evictable
```

It can be used on:

- Field properties, where it controls raw field data and scalar stats that
  follow field-data semantics.
- Index properties, where it controls the index cache slot.

Collection properties provide default values:

```text
evictable.scalarField
evictable.scalarIndex
evictable.vectorField
evictable.vectorIndex
```

These metadata property keys follow the existing `warmup` convention: a common
field/index key plus camelCase collection-level resource categories. They are
separate from Milvus server configuration paths, which retain the existing
`queryNode.segcore.tieredStorage.evictable.*` namespace.

The default configuration is:

```yaml
queryNode:
  segcore:
    tieredStorage:
      evictable:
        scalarField: true
        scalarIndex: true
        vectorField: true
        vectorIndex: true
```

The Go client exposes these settings through its generic collection/field
property and index extra-parameter helpers.

PyMilvus currently accesses these settings through its generic metadata maps.
Python booleans are serialized as `True` or `False`, and Milvus accepts the
words `true` and `false` case-insensitively while rejecting other boolean-like
forms such as `1`, `t`, or values with surrounding whitespace. For example:

```python
client.create_collection(
    collection_name=name,
    schema=schema,
    properties={"evictable.scalarField": False},
)
client.alter_collection_field(
    collection_name=name,
    field_name="scalar",
    field_params={"evictable": False},
)
```

PyMilvus does not yet expose a typed create-time field helper for evictable
metadata; adding one requires a companion PyMilvus change. Until then, Python
users can set a field-specific override with `alter_collection_field` while the
collection is released. Collection defaults and index create/alter settings are
available through the existing generic `properties` and index `params` maps.

## Non-Goals

- Change manual release or unload behavior.
- Dynamically update existing cache slots after a collection is already loaded.
- Add separate controls for JSON stats or text stats in the first version.
- Configure eviction behavior for internal timestamp or primary-key indexes;
  this remains future work.
- Change the core cachinglayer eviction algorithm.

## Design Details

Priority is:

```text
field or index property > collection property > QueryNode global default
```

The value is parsed as a boolean string. SDK helpers emit `true` or `false`.

QueryNode defaults only have runtime eviction effect when
`queryNode.segcore.tieredStorage.evictionEnabled` is enabled. If eviction is
disabled globally, `evictable` is still persisted but no
policy-based eviction occurs.

The four QueryNode defaults are refreshable. Storage V3 field-data loading
captures the warmup and evictable defaults once when a load/reopen planning pass
creates its cache-slot tasks. Every task carries its resolved policy through
size estimation and translator construction. Existing cache slots and tasks
that are already planned or in flight keep that snapshot; a later planning pass
observes refreshed defaults. Field/index and collection metadata continue to
take precedence over these local defaults.

### Metadata Validation

Proxy validates collection and field properties:

- Collection alter accepts the four collection-level evictable keys.
- Collection alter rejects field-level `evictable`.
- Field alter accepts `evictable`.
- Loaded collections reject in-place changes to evictable field or collection
  properties, matching existing reload-style semantics.

DataCoord validates index properties:

- Index create and alter accept `evictable`.
- Non-boolean values are rejected.
- Runtime configurable index parameters are filtered out of index build
  parameter comparison and snapshot restore paths.

### Load Propagation

QueryCoord keeps collection properties and index user parameters as metadata on
the worker-bound request; it does not materialize collection defaults into each
field or index. The target QueryNode conditionally creates a private shallow
clone of that request and materializes only the representation required by the
loaders: inherited field warmup values are added to missing field type params,
and effective index warmup/evictable overrides are added to index params. The
large binlog, manifest, and index-file-path payloads remain shared. Field-data
evictable settings remain schema metadata for segcore to resolve.

QueryNode-local defaults are deliberately not written onto the request. Segcore
resolves them at its local planning boundary, so different workers can use
their own defaults without mutating or forwarding a worker-private request.

QueryNode and segcore then resolve the effective properties:

- Field data uses field `evictable` or scalar/vector field defaults.
- Index data uses index `evictable` or scalar/vector index defaults.
- JSON stats and text stats follow scalar field evictable settings.
- Resource estimates count non-evictable resources as resident even when
  tiered eviction is enabled.

Storage V2 column groups use a conservative aggregation rule: a group is marked
evictable only when every child field in that group is evictable.

Storage V3 keeps the existing load/reopen planner boundaries, then partitions
each planned entry by the fields' effective `(warmup, evictable)` values.
Fields with the same pair share one projected reader and cache slot; fields
with different pairs use independent projections and cache keys. For each
physical column group, the planner fetches one complete physical-column size
matrix and shares that immutable estimate across all projected tasks; each
translator selects the columns in its own projection. `mmap` does not
participate in this partitioning and keeps its existing entry-level behavior.

The planner also stores the resolved `(warmup, evictable)` policy in every
column-group load task. Translator construction consumes that stored policy
directly and never re-reads refreshable defaults after size I/O, preventing a
configuration refresh from changing the slot policy after the projection has
already been chosen.

#### StructArray Semantics

StructArray follows the same configuration-versus-storage model as `warmup`:
configuration is resolved through its nested fields. Storage V2 manages the
physical column group as one cache resource; Storage V3 may project nested
fields into separate cache entries when their effective warmup or evictable
values differ.

For raw field data, precedence within a StructArray is:

```text
nested field property > struct field property > collection property > QueryNode global default
```

- An explicit `evictable` value on a nested field affects that field.
- An explicit `evictable` value on the StructArray is propagated to nested
  fields that do not have their own override.
- Without a StructArray-level value, each nested field inherits the collection
  default for its own type: `Array` uses `evictable.scalarField`, while
  `ArrayOfVector` uses `evictable.vectorField`.
- QueryNode does not materialize `evictable.scalarField` onto the StructArray
  container itself.

Segcore flattens the nested fields into its field schema. For Storage V2, the
StructArray binlog/column group has one cache slot and therefore one
`support_eviction` value: if any nested field is non-evictable, the entire
group is non-evictable. For Storage V3, nested fields first inherit their
effective values and then follow the same `(warmup, evictable)` projection rule
as ordinary fields.

Indexes belong to individual nested fields and use their own scalar/vector
index evictable settings; they are not combined with the raw StructArray column
group setting.

### Segcore Integration

The CGo load messages now carry `support_eviction` for:

- Field binlogs.
- Vector/scalar index load info.
- Text index load info.
- JSON key index load info.

Segcore schema parsing reads field and collection-level evictable properties.
Load translators pass the computed bool into `cachinglayer::Meta` instead of
hard-coding user-loadable resources as evictable.

Vector lazy-load metadata keeps its existing non-evictable exception.
Internal timestamp and primary-key indexes are not covered by the user-facing
`evictable` properties in this version and keep their existing hard-coded
behavior. Making their eviction behavior configurable is a future TODO.

## Compatibility, Deprecation, and Migration Plan

The feature is backward compatible:

- All new defaults are `true`, preserving current eviction behavior.
- Existing collection, field, and index metadata without evictable properties
  continues to load through QueryNode global defaults.
- The new protobuf bool fields are only used by newer QueryNode/segcore load
  paths and do not require user data migration.
- There is no deprecation in this change.

## Test Plan

The implementation includes tests for:

- Common property helpers and QueryNode defaults.
- Python client E2E coverage for metadata round-trip, lifecycle, validation,
  and loaded-collection behavior.
- Proxy collection and field validation.
- DataCoord index validation, alter, idempotency, and snapshot filtering.
- QueryCoord load-info propagation and precedence.
- StructArray parent/child propagation, type-specific collection defaults, and
  conservative physical-group aggregation.
- QueryNode parsing, resource estimates, and CGo request construction.
- C++ load structs and translator propagation into `support_eviction`.
- Storage V3 full-group size-estimate sharing across projected tasks and
  propagation of the planner-resolved policy into translator metadata.
- Go client generic property and extra-parameter helpers.

Validation commands used by the implementation PR:

```bash
make SKIP_3RDPARTY=1 build-cpp-with-unittest
source ./scripts/setenv.sh && go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy ./internal/datacoord ./internal/querycoordv2/task ./internal/querynodev2/segments ./internal/util/segcore -run 'Test.*Evictable|TestAlterCollectionCheckLoaded|TestAlterCollectionFieldCheckLoaded|TestAlterCollectionField|TestHasPropInDeletekeys|TestValidateIndexParams|TestServer_AlterIndex|TestCheckParams|TestPackLoadSegmentRequest|TestEstimate.*ResourceUsage|TestLoadFieldData'
(cd pkg && go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./common ./util/paramtable ./util/indexparams)
(cd client && go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./milvusclient -run 'Test(Collection|Index)/Test.*Evictable')
source ./scripts/setenv.sh && LD_PRELOAD=$(gcc -print-file-name=libasan.so) ./internal/core/output/unittest/all_tests --gtest_filter='CApiTest.LoadInfoTest:SegmentLoadInfoTest.*:*Translator*:*LoadInfo*'
```

## Rejected Alternatives

- Use only global tiered storage eviction settings. This cannot express
  per-field or per-index residency requirements.
- Reuse `warmup` to imply non-evictable resources. Warmup controls proactive
  loading, while evictable controls later policy eviction. Keeping them
  independent preserves legal combinations such as lazy but non-evictable
  resources.
- Add separate JSON stats and text stats knobs in the first version. These
  stats follow scalar field semantics for now to avoid expanding the public
  configuration surface.

## References

- https://github.com/milvus-io/milvus/issues/50984
