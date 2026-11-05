# Index ownership and request readers

## Scope and documentation

This document records the motivation and architectural choices of the Index/request-reader split.
The maintained interface and implementation descriptions are:

| Topic | Detailed documentation |
|---|---|
| Index and Reader contracts, context lifetime, storage pins, resource accounting, and a query example | [Index contracts](../../../internal/core/src/index/contracts/README.md#index-and-request-reader-boundary) |
| Segment inventory, lazy creation, publication, replacement, and query call diagrams | [Segment index architecture](../../../internal/core/src/segcore/indexing/README.md) |
| Vector engines, native lazy loading, sparse vectors, VECTOR_ARRAY, and iterator lifetimes | [Vector indexes](../../../internal/core/src/index/vector/README.md) |
| Shared interface behavior and test coverage | [Index contract behavior tests](../../../internal/core/src/index/contracts/TESTING.md) |

Update those documents when an interface or implementation changes. This design note does not
maintain a second copy of the API reference or a per-family migration checklist.

## Motivation

Before this change, the segment inventory held cache slots whose cells were `IIndexReaderBase`
objects. Pinning a cell both loaded the index and obtained its query interface. This coupled query
object lifetime to cached engine lifetime. Adding operation contexts to every query method would
then spread storage concerns across otherwise ordinary scalar query contracts.

The new boundary separates a long-lived Index version from each request's Reader. A segment
snapshot retains the Index. A query creates a Reader with its context, and the concrete Reader
acquires the storage needed by its operations.

```text
Segment snapshot -> Index version -> resident Storage/Engine or internal cache
                         |
                         +-- CreateReader(op_ctx) -> request Reader
                                                        |
                                                        +-- In / Range / Search
                                                              -> pin storage
                                                              -> compute result
```

Persisted loading always requires an explicit `IndexCacheConfig` and creates an internal cache
slot. A resident Storage/Engine is used by direct in-memory construction, such as growing
publication or concrete Artifact storage/engine extraction, not as an alternative persisted-load mode.

## Decisions

- **Keep query interfaces on Reader.** `IIndex` owns metadata and resources and creates request
  readers; it does not gain `In`, `Range`, or `Search` merely to avoid existing interface casts.
  Binding the context once keeps query signatures independent of which family needs I/O.
- **Keep storage granularity inside each family.** Scalar implementations can own their existing
  Storage directly. A backend that uses an engine keeps that engine. The common interfaces do not
  expose cell IDs or impose a single-cell translator on every index.
- **Separate lazy Index creation from lazy storage loading.** The inventory may defer metadata
  resolution and Index construction. Once an Index exists, creating a request Reader does not
  open files or pin data. Initialization failure remains retryable.
- **Keep retirement distinct from destruction.** Replacing an Index cancels its warmup after the
  new segment state is published. Existing snapshots can still retain the old Index and query it;
  waiting for destruction would retain unnecessary background work.
- **Use known metadata without changing its meaning.** Unknown count is distinct from zero, and
  element count is distinct from parent row count. Missing legacy metadata can still require I/O.
  Splitting Index and Reader alone requires neither a scalar index version bump nor a new format.

The change moves the ownership and access boundary in Milvus. It does not change Knowhere's
internal loading algorithms, implement block loading, or make synchronous operations asynchronous.

## Verification boundaries

Validation must cover cold Reader creation, cancellation and retry, shared-resource lifetimes,
borrowed results and deferred iterators, three-valued null behavior, and old snapshots surviving
replacement. Consumer tests must exercise ARRAY/JSON coordinates, sparse and VECTOR_ARRAY search,
and the selected native lazy-loading backend. A skipped backend test is not evidence of runtime
support; report build, unit, and consumer verification separately.
