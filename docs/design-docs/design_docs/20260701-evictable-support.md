# MEP: Evictable Support for Tiered Storage

- **Created:** 2026-07-01
- **Author(s):** @sparknack
- **Status:** Implemented
- **Component:** QueryNode | Proxy | Coordinator | Storage | Index | SDK
- **Related Issues:** #50984
- **Released:** N/A

---

## Summary

This design adds user-facing `evictable.enabled` controls for Milvus tiered
storage resources. Users can keep selected field data or indexes out of
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
evictable.enabled
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

The Go client adds typed helpers for collection defaults, field
`evictable.enabled`, and index `evictable.enabled`.

## Non-Goals

- Change manual release or unload behavior.
- Dynamically update existing cache slots after a collection is already loaded.
- Add separate controls for JSON stats or text stats in the first version.
- Change the core cachinglayer eviction algorithm.

## Design Details

Priority is:

```text
field or index property > collection property > QueryNode global default
```

The value is parsed as a boolean string. SDK helpers emit `true` or `false`.

QueryNode defaults only have runtime eviction effect when
`queryNode.segcore.tieredStorage.evictionEnabled` is enabled. If eviction is
disabled globally, `evictable.enabled` is still persisted but no policy-based
eviction occurs.

### Metadata Validation

Proxy validates collection and field properties:

- Collection alter accepts the four collection-level evictable keys.
- Collection alter rejects field-level `evictable.enabled`.
- Field alter accepts `evictable.enabled`.
- Loaded collections reject in-place changes to evictable field or collection
  properties, matching existing reload-style semantics.

DataCoord validates index properties:

- Index create and alter accept `evictable.enabled`.
- Non-boolean values are rejected.
- Runtime configurable index parameters are filtered out of index build
  parameter comparison and snapshot restore paths.

### Load Propagation

QueryCoord propagates collection-level defaults into load info before dispatch:

- Scalar/vector field defaults are applied to field type params when the field
  does not already have `evictable.enabled`.
- Scalar/vector index defaults are applied to index params when the index does
  not already have `evictable.enabled`.
- Struct-array nested fields inherit by `nested field > struct field >
  collection default`.

QueryNode parses the final properties:

- Field data uses field `evictable.enabled` or scalar/vector field defaults.
- Index data uses index `evictable.enabled` or scalar/vector index defaults.
- JSON stats and text stats follow scalar field evictable settings.
- Resource estimates count non-evictable resources as resident even when
  tiered eviction is enabled.

Storage V2 column groups use a conservative aggregation rule: a group is marked
evictable only when every child field in that group is evictable.

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
System indexes keep their existing default behavior.

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
- Proxy collection and field validation.
- DataCoord index validation, alter, idempotency, and snapshot filtering.
- QueryCoord load-info propagation and precedence.
- QueryNode parsing, resource estimates, and CGo request construction.
- C++ load structs and translator propagation into `support_eviction`.
- Go client typed helpers.

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
