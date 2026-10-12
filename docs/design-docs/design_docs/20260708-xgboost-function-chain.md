# XGBoost FunctionChain Expression Design

## Overview

This document describes native `xgboost` FunctionChain expression support for Milvus L0 rerank.

The feature allows a search-time FunctionChain to reference an XGBoost model registered as a FileResource, load the corresponding local UBJ model file on the execution side, and use native model prediction as the rerank score.

Tracking issue: [#51192](https://github.com/milvus-io/milvus/issues/51192)

## Motivation

Recommendation and learning-to-rank workloads often use vector search as a recall stage, then apply a ranking model over scalar features to reorder the recalled candidates. The ranking model may use business features such as price, click-through-rate, freshness, user-specific scores, or other scalar columns stored with the entity.

Milvus already supports FunctionChain-based rerank pipelines. Adding a native XGBoost expression lets users keep vector recall and model-based reranking in the same search request, avoiding an external requery-and-rerank round trip for L0 rerank scenarios.

## Goals

- Add a native `xgboost` FunctionChain expression.
- Support L0 rerank execution.
- Load XGBoost models from Milvus FileResource metadata.
- Support UBJ model resources.
- Lazily load models on first use.
- Cache loaded model handles by FileResource identity.
- Merge concurrent first loads of the same model.
- Protect in-flight predictions from concurrent model close.
- Evict stale model handles after FileResource sync updates.
- Support `output=raw` and `output=default`.
- Validate model metadata, feature count, input column types, and expression parameters.
- Provide C++ unit tests, Go tests, and Python client end-to-end tests.

## Non-goals

- XGBoost training inside Milvus.
- JSON model format support.
- Legacy binary model format support.
- `model_format` user parameter.
- User-specified `feature_names` parameter.
- User-specified `objective` parameter.
- Sync-time eager loading of all FileResources.
- Function-level watched set or request-time temporary FileResource references.
- LRU or `maxCacheModels` cache capacity management.
- Proxy-side model validation.
- L2 rerank support in the first stage.

L2 rerank is not enabled in this stage because L2 rerank runs on Proxy, and Proxy does not yet have FileResource sync, local FileResource management, or FileResource resolving support. L2 support can be added later after Proxy-side FileResource support is available.

## User-facing API

### Function name

The FunctionChain expression name is `xgboost`.

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_resource` | Yes | FileResource name of the UBJ XGBoost model. |
| `output` | No | Prediction output mode. Defaults to `default`. |

Allowed `output` values:

| Value | Description |
|-------|-------------|
| `raw` | Return the raw tree margin without applying the model objective transform. |
| `default` | Return the model's default prediction output. For `binary:logistic`, this applies sigmoid to the raw margin. For `reg:squarederror`, this is the raw score. |

Client-side usage syntax is intentionally omitted from this design because the PyMilvus FunctionChain API may be updated separately.

### Arguments

Expression arguments are feature columns. The feature order passed to XGBoost is exactly the expression argument order.

The expression requires at least one feature column. Literal arguments are not supported.

## Architecture

```text
Search request with FunctionChain
        │
        ▼
FunctionChain execution side
        │
        ▼
`xgboost` FunctionExpr
  - validate parameters
  - validate input Arrow chunks
  - acquire model lease
        │
        ▼
XGBoost model cache
  - resolve FileResource name
  - lazy load by resource identity
  - singleflight concurrent loads
  - lease/refcount lifecycle protection
  - stale eviction on FileResource sync
        │
        ▼
CGO bridge
  - export Arrow arrays through Arrow C Data Interface
  - call native C API
        │
        ▼
C++ XGBoost UBJ runtime
  - parse UBJ model metadata and trees
  - validate objective / booster / feature count
  - batch predict over Arrow feature columns
  - apply optional objective transform
        │
        ▼
Arrow Float32 score column
```

The expression implementation is placed under `internal/util/function/chain/expr`. The native model runtime lives under `internal/core/src/rescores` and is called through a small C API.

## Component Design

### Function expression

Files:

- `internal/util/function/chain/expr/xgboost_expr.go`
- `internal/util/function/chain/expr/xgboost_expr_test.go`

Responsibilities:

- Register the `xgboost` FunctionChain expression.
- Parse and validate FunctionChain parameters.
- Reject unsupported parameters such as `feature_names` and `objective`.
- Validate that at least one feature column is provided.
- Declare a single Float32 output column.
- Validate input Arrow chunk layout before prediction.
- Acquire a model lease from the model cache by `model_resource`.
- Validate that the number of input columns matches the model's `num_feature` metadata.
- Call the native prediction bridge and return the output score column.

Parameter validation happens when building the expression. Runtime validation covers FileResource resolution, model metadata, input chunk layout, feature count, and native prediction failures.

### Model cache and FileResource listener

Files:

- `internal/util/function/chain/expr/xgboost_cache.go`
- `internal/util/function/chain/expr/xgboost_cache_test.go`

Responsibilities:

- Listen for FileResource sync events.
- Keep only `.ubj` resources in the XGBoost resource index.
- Resolve `model_resource` names to local FileResource paths.
- Lazy-load models only when a search request first needs them.
- Cache loaded models by FileResource identity and path.
- Use singleflight to merge concurrent first loads of the same model.
- Use lease/refcount protection so a cached model is not closed while prediction is in flight.
- Evict stale models when FileResource sync no longer contains the same resource identity.

The cache does not implement capacity-based eviction in this stage. LRU and `maxCacheModels` can be added later as an independent optimization.

### CGO bridge

Files:

- `internal/util/function/chain/expr/xgboost_model_cgo.go`
- `internal/util/function/chain/expr/xgboost_model_stub.go`
- `internal/util/function/chain/expr/xgboost_real_parity_local_test.go`

The CGO bridge is enabled by default for cgo builds. It is responsible for:

- Loading a UBJ model from a resolved local FileResource path.
- Converting native load results into Go model handles.
- Exporting Go Arrow arrays to Arrow C Data structures.
- Validating that input arrays are numeric before export.
- Calling native prediction for each Arrow chunk.
- Building a Float32 Arrow output chunk.
- Closing native model handles.
- Converting native `CStatus` failures into Milvus errors.

When cgo is disabled, the stub implementation keeps the package buildable and returns a clear error telling the operator to rebuild with `CGO_ENABLED=1`.

### Native C++ runtime

Files:

- `internal/core/src/rescores/xgboost_model_c.h`
- `internal/core/src/rescores/xgboost_model_c.cpp`
- `internal/core/src/rescores/xgboost_model_c_test.cpp`

Responsibilities:

- Expose a C ABI for model load, prediction, and deletion.
- Parse XGBoost UBJ model files.
- Validate required model metadata.
- Extract `num_feature`, objective, base score, tree parameters, split nodes, leaf values, and default directions.
- Reject unsupported objectives, boosters, multiclass models, and multi-target leaf vector models.
- Predict over Arrow feature columns using batch tree traversal.
- Treat null feature values according to the model's default direction.
- Apply the objective transform when `output=default`.
- Return structured `CStatus` errors for load and predict failures.

The model handle is immutable after load. Prediction must not mutate shared model state or retain Arrow input pointers after the call returns.

## Model Format and Runtime Semantics

### Supported model format

Only XGBoost UBJ model files are supported in this stage. FileResource sync filters resources by the `.ubj` extension for the XGBoost resource index.

Unsupported formats include:

- XGBoost JSON model files.
- XGBoost model dump JSON array format.
- Legacy XGBoost binary model files.

### Supported model types

Supported objectives:

- `reg:squarederror`
- `binary:logistic`

Supported booster path:

- Tree-based `gbtree` models compatible with the native parser.

Unsupported model capabilities include:

- `gblinear`
- `dart`
- multiclass models
- ranking objectives
- multi-target leaf vector models

A single-target `size_leaf_vector` value is accepted, while values greater than one are rejected because multi-target leaf vectors are not supported.

### Prediction output

For `output=raw`, Milvus returns the raw tree margin.

For `output=default`, Milvus applies the model's default objective transform:

- `binary:logistic`: sigmoid of raw margin.
- `reg:squarederror`: raw score.

The output column type is Float32.

## Validation and Error Handling

The implementation rejects invalid requests or unsupported runtime states with clear errors.

Expression and parameter validation:

- missing `model_resource`
- unsupported `output` value
- unsupported parameter names
- `feature_names` parameter
- `objective` parameter
- no feature columns
- literal arguments

FileResource validation:

- empty resource name
- unknown resource name
- non-UBJ resources not present in the XGBoost resource index
- resolved resource without a local path

Model validation:

- invalid UBJ content
- missing required UBJ objects or arrays
- invalid numeric metadata
- unsupported objective
- unsupported booster
- multiclass model
- multi-target leaf vector model
- malformed tree arrays

Runtime input validation:

- feature column count mismatch
- nil input columns or chunks
- mismatched chunk counts across feature columns
- mismatched row counts within the same chunk index
- unsupported input column type
- native runtime not enabled

The Go layer uses Milvus error helpers for request validation and internal failures. Native C++ failures are returned through `CStatus` and converted at the cgo boundary.

## Concurrency and Lifecycle

Model loading and prediction must be safe under concurrent search requests.

The cache lifecycle is:

1. FileResource sync builds the current `.ubj` resource index.
2. A search request resolves `model_resource` to a resource.
3. The cache key is derived from FileResource identity and path.
4. If the model is already cached, the request acquires a lease.
5. If the model is missing, singleflight loads it once for concurrent waiters.
6. Prediction runs while holding a lease.
7. FileResource sync eviction marks stale cached models closing.
8. A closing model is deleted only after active leases are released.

The native model handle is immutable. Concurrent predictions against the same handle are allowed. Per-request scratch data must stay local to the prediction call.

## Build and Deployment

The native runtime is required for XGBoost FunctionChain execution and is built by default when cgo is enabled. It requires:

- cgo enabled
- C++ runtime linked through the Milvus core build artifacts

When cgo is disabled, Milvus can still compile the expression package through the stub implementation, but executing an XGBoost expression returns an explicit runtime-disabled error.

## L0 and L2 Rerank Scope

This stage supports L0 rerank because L0 execution has access to the execution-side FileResource resolver and local model files.

L2 rerank is deferred. L2 runs on Proxy, and Proxy does not yet support FileResource sync and local FileResource resolution. Enabling L2 requires a follow-up design and implementation to provide Proxy-side FileResource local management, resource synchronization, resolver wiring, and L2 FunctionChain data-frame integration.

## Testing Plan

### C++ unit tests

C++ tests cover native UBJ parsing and prediction behavior:

- load valid UBJ model metadata
- predict raw scores
- predict default binary logistic output
- missing-value default direction
- reject non-UBJ content
- reject unsupported objective
- reject unsupported booster
- reject multiclass models
- accept single-target `size_leaf_vector=1`
- reject multi-target `size_leaf_vector>1`

### Go tests

Go tests cover expression validation, cache behavior, and native runtime integration:

- parameter validation
- unsupported parameter rejection
- output mode validation
- argument validation
- FileResource resolution
- `.ubj` FileResource filtering
- cache lazy load
- singleflight concurrent load behavior
- lease/refcount lifecycle behavior
- stale model eviction after FileResource sync
- feature count validation
- input chunk layout validation
- native UBJ parity against expected prediction output
- stub behavior when cgo is disabled

Native Go tests must be run with `CGO_ENABLED=1`.

### Python client end-to-end tests

Python client end-to-end tests cover real L0 execution through Milvus standalone:

- generate or load a UBJ XGBoost model
- upload the model to object storage
- register it as a FileResource
- create a collection with scalar feature columns and a vector field
- execute search with an L0 XGBoost FunctionChain
- verify result order matches local XGBoost prediction
- verify returned scores match expected model scores
- verify invalid cases return expected errors

Negative E2E coverage includes:

- missing FileResource
- invalid `output`
- feature count mismatch
- unsupported input type
- invalid model content
- unsupported objective
- unsupported booster

## Future Work

- Add Proxy-side FileResource sync and local resolving.
- Enable L2 rerank after Proxy FileResource support is available.
- Add Proxy-side model validation if needed.
- Add capacity-based cache management such as LRU and `maxCacheModels`.
- Evaluate support for additional XGBoost objectives and boosters.
- Evaluate support for additional model formats if required by users.
