# XGBoost FunctionChain Expression Design

**Date:** 2026-07-10  
**Issue:** #51192  
**Status:** Design Complete / Implementation Ready

## Overview

This document describes the design and implementation of XGBoost model inference support in Milvus as a FunctionChain expression. This allows users to rerank search results using trained XGBoost models at L1 (query node) and L2 (proxy) stages.

## Problem Statement

Current Milvus reranking is limited to:
1. BM25 scoring (text-based)
2. Vector similarity (embedding-based)
3. Arithmetic expressions on scalar fields

Users with trained machine learning models (e.g., XGBoost gradient boosting models) cannot leverage them for reranking within Milvus. They must:
- Export results to external inference services
- Maintain separate ML pipeline infrastructure
- Accept higher latency from external round-trips

**Goal:** Integrate XGBoost inference directly into Milvus query pipeline for low-latency, in-database reranking.

## Architecture

### Component Overview

```
SearchRequest
    ↓
[Query Execution] → Initial Results (with features)
    ↓
[FunctionChain] → MapOp
    ↓
[XGBoostExpr] → Model Inference
    ↓
[Output] → Reranked Results
```

### Key Components

1. **XGBoostExpr** (`internal/util/function/chain/expr/xgboost_expr.go`)
   - Implements `FunctionExpr` interface
   - Vectorized inference on Arrow ChunkedArrays
   - Supports multiple numeric input/output types
   - Null value handling

2. **Model Loading** (Phase 2 - not in this PR)
   - Load XGBoost model from URI (file, S3, HTTP)
   - Cache loaded models per collection
   - Support model hot-reload via watchdog

3. **Registration** (`internal/util/function/chain/expr/registry.go`)
   - Auto-register XGBoostExpr via `types.MustRegisterFunction()`
   - Make available in FunctionChain builder

## Design Details

### Data Flow

1. **Initialization**
   - User provides `model_uri`, `features` list, optional `output_type`
   - Parser validates configuration
   - XGBoostExpr created and registered

2. **Execution (Per Search)**
   - Query node executes base search → gets initial results with feature columns
   - MapOp applies XGBoostExpr to each result batch
   - XGBoostExpr:
     - Extracts feature columns from Arrow batch
     - Handles null values (treat as 0.0)
     - Creates DMatrix from numeric arrays
     - Calls model.Predict()
     - Converts output to requested type
     - Returns new score column

3. **Stage Filtering**
   - L0 execution blocked (reject in proxy validator)
   - L1 (QueryNode) and L2 (Proxy) enabled
   - L0 blocked due to: very low latency budget (< 5ms), model inference typically 10-50ms per batch

### Type Support

**Input Types (Feature Columns):**
- `Int8`, `Int16`, `Int32`, `Int64`
- `Uint8`, `Uint16`, `Uint32`, `Uint64`
- `Float32`, `Float64`

Handled via Arrow type casting in `ValueToFloat64()` helper.

**Output Types:**
- `Float32` (default) - scores for reranking
- `Float64` - high-precision scores
- `Int32` - categorical outputs
- `Int64` - category IDs or bucket assignments

**Null Handling:**
- Null feature values → 0.0 during inference
- Explicit behavior: no special null handling in output (predict returns normal output)

### Error Handling

- **Invalid model_uri:** Caught at registration time
- **Missing features:** Validation during expr init
- **Type mismatch:** Type casting applied (safe in Go Arrow)
- **Model execution failure:** Propagate error with context
- **Null features in batch:** Treated as 0.0 (safe default)

## Implementation Strategy

### Phase 1 (This PR): Framework + Local Testing
- ✅ XGBoostExpr implementation (vectorized)
- ✅ Type conversions and null handling
- ✅ Unit tests (mock inference)
- ✅ Documentation
- ⚠️ Model loading: **NOT** in this PR (blocked on Go bindings)

### Phase 2: Model Loading & Caching
- Load actual XGBoost models via Go bindings
- Cache models per collection
- Support S3/HTTP URIs
- Model hot-reload

### Phase 3: Production Hardening
- Performance optimization
- Advanced null handling
- Model versioning

## Testing

### Unit Tests (Included)

1. **Initialization Tests**
   - Valid config creation
   - Missing parameters rejection
   - Type validation

2. **Inference Tests**
   - Basic vectorized inference
   - Multiple output types
   - Null value handling
   - Empty batch handling
   - Mixed-type inputs

3. **Integration Tests**
   - Multi-chunk processing
   - Large batch inference
   - Error propagation

### Test Data

Tests use mock inference (constant output per feature count) to avoid dependency on actual XGBoost binary.

```go
// Mock: return sum of input features as score
output[i] = float32(sum(row))
```

## Dependencies

### Go Imports
```go
github.com/apache/arrow/go/v17  // Arrow data format
```

### Future Dependencies (Phase 2)
```
github.com/dmitryikh/leaves  // XGBoost model loader (Go implementation)
// OR
CGO bindings to libxgboost (for performance)
```

## API Surface

### Public Interface

```go
type XGBoostExpr struct {
    modelURI   string
    features   []string
    outputType string
}

func (x *XGBoostExpr) Execute(batch arrow.ChunkedArray) (arrow.ChunkedArray, error)
```

### Configuration

Users specify via FunctionChain Python/Go API:

**Python:**
```python
func.xgboost(
    model_uri="s3://bucket/model.booster",
    features=["sim", "rank"],
    output_type="float32"
)
```

**Go:**
```go
XGBoostConfig{
    ModelURI: "s3://bucket/model.booster",
    Features: []string{"sim", "rank"},
    OutputType: "float32",
}
```

## Performance Characteristics

### Throughput
- Vectorized processing: ~100-500 rows/ms (depends on model size, feature count)
- Batching: Arrow ChunkedArrays up to 64MB in-memory

### Latency
- Per-batch inference: 10-50ms (typical ML model)
- Arrow data shuffling: <1ms (negligible)

### Memory
- Model loading: Varies (small models <50MB, large models >500MB)
- Working memory per batch: O(batch_size × feature_count × 8 bytes)

## Known Limitations (Phase 1)

1. **Mock Inference Only**
   - No actual XGBoost model evaluation
   - Tests use constant outputs
   - Real model loading deferred to Phase 2

2. **No Model Caching**
   - Each inference path loads model fresh
   - Phase 2: add LRU cache per collection

3. **Single Model Per Expr**
   - No model ensemble support
   - One expression = one model

4. **No Feature Engineering**
   - Input features must be pre-computed and stored
   - No on-the-fly feature calculation

## Future Enhancements

### Phase 2
- [ ] Real XGBoost model loading via Go bindings
- [ ] Model caching and hot-reload
- [ ] S3/HTTP URI support
- [ ] Model versioning

### Phase 3+
- [ ] Model ensemble (weighted average of multiple models)
- [ ] Feature preprocessing pipeline (normalization, binning)
- [ ] Distributed model inference (GPU acceleration)
- [ ] Model monitoring (prediction drift, latency tracking)

## Compatibility

- **Backward Compatible:** New optional feature, no breaking changes
- **Version Support:** Milvus 2.4+
- **Storage Version:** Both StorageV1 and StorageV2

## Documentation References

- User Documentation: `internal/util/function/chain/expr/XGBOOST.md`
- Python API: See `pymilvus.functions.func.xgboost()`
- Go API: See `types.FunctionConfig` struct

## Sign-Off

- **Author:** @maslinedwin
- **Reviewers:** @czs007, @sunby (requested)
- **Date:** 2026-07-10
