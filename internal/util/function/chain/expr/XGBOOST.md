# XGBoost FunctionChain Expression

## Overview

The `xgboost` expression enables XGBoost model inference as part of the Milvus FunctionChain reranking pipeline. It allows users to rerank search results using a trained XGBoost model that takes numeric features as input.

## Features

- **Multi-feature support**: Accept 1 or more numeric feature columns
- **Flexible data types**: Support Int8/16/32/64, Uint8/16/32/64, Float32/64
- **Multiple output types**: Float32, Float64, Int32, Int64
- **Null handling**: Treat null values as 0.0 during inference
- **Batch inference**: Efficiently process multiple rows via DMatrix
- **Stage support**: L1_rerank and L2_rerank stages only (not L0 due to latency)

## Configuration

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `model_uri` | string | Yes | N/A | URI to XGBoost model file (file path or cloud URL) |
| `features` | string[] | Yes | N/A | List of feature column names matching model input |
| `output_type` | string | No | `float32` | Output prediction type: `float32`, `float64`, `int32`, or `int64` |

### Model URI

The `model_uri` parameter specifies where the XGBoost model is stored:

- **Local file**: `file:///path/to/model.booster`
- **S3 object**: `s3://bucket/path/model.booster`
- **HTTP URL**: `https://example.com/model.booster`

For cloud URIs, ensure appropriate credentials are configured via:
- AWS IAM roles (for S3)
- Environment variables
- Milvus credential system

## Usage Examples

### Python Client Example

```python
from pymilvus import MilvusClient
from pymilvus.functions import Function, func

client = MilvusClient("http://localhost:19530")

# Define XGBoost reranking function
xgboost_fn = Function(
    name="xgboost_rerank",
    operator="map",
    function=func.xgboost(
        model_uri="s3://my-bucket/models/ranking.booster",
        features=["user_embedding_similarity", "page_rank_score", "freshness_days"],
        output_type="float32"
    ),
    output_field_name="$score"
)

# Create function chain
chain = FunctionChain(
    functions=[xgboost_fn],
    stage="L2_rerank"
)

# Use in search
results = client.search(
    collection_name="documents",
    data=query_embedding,
    limit=100,
    output_fields=["text", "$score"],
    function_chain=chain
)
```

### Go API Example

```go
cfg := types.FunctionConfig{
    Name: "xgboost",
    Params: map[string]*schemapb.FunctionParamValue{
        "model_uri": &schemapb.FunctionParamValue{
            Val: &schemapb.FunctionParamValue_StringVal{
                StringVal: "file:///models/ranking.booster",
            },
        },
        "features": &schemapb.FunctionParamValue{
            Val: &schemapb.FunctionParamValue_StringListVal{
                StringListVal: &schemapb.StringList{
                    Values: []string{"score", "popularity", "recency"},
                },
            },
        },
        "output_type": &schemapb.FunctionParamValue{
            Val: &schemapb.FunctionParamValue_StringVal{
                StringVal: "float32",
            },
        },
    },
}

expr, err := NewXGBoostExprFromParams(ctx, cfg)
```

## Implementation Details

### Architecture

The XGBoost expression follows the established FunctionChain pattern:

1. **Factory Pattern**: `NewXGBoostExprFromParams()` creates expression instances from proto config
2. **Registration**: Auto-registered at package init via `types.MustRegisterFunction()`
3. **Lazy Loading**: Model is loaded once on first inference and cached
4. **Vectorized Execution**: Processes Arrow ChunkedArrays for batch efficiency

### Data Flow

```
Input Features (ChunkedArrays)
    ↓
Extract numeric values per column
    ↓
Transpose to row-major (feature vectors)
    ↓
Batch predict via XGBoost DMatrix
    ↓
Build output array (Float32/Int32/etc)
    ↓
Return as Arrow ChunkedArray
```

### Stage Support

- **L2 Rerank**: At proxy, on combined results from multiple partitions
- **L1 Rerank**: At query node, on segment results before merging
- **L0 Rerank**: NOT supported (model inference latency too high for segment-level execution)

## Testing

### Unit Tests

```bash
go test -v ./internal/util/function/chain/expr -run TestXGBoostExpr
```

Tests cover:
- Parameter validation (model_uri, features, output_type)
- Multiple output types (Float32, Int32, Int64, Float64)
- Multiple input columns with different numeric types
- Null value handling
- Empty chunks
- Feature count mismatch detection
- Stage filtering (L0 rejection)

### Integration Tests

```bash
go test -v ./internal/util/function/chain/expr -run TestXGBoostExprIntegration
```

Tests cover:
- Multi-feature inference on real numeric data
- Type conversions (Int32, Float32, Float64 mixing)
- Chunk structure preservation
- Multi-chunk processing

## Future Enhancements

### Phase 2: Production Model Loading

- [ ] Implement actual XGBoost model loading via egoai/go-xgboost
- [ ] Add model caching with TTL
- [ ] Support model parameter tuning (thread count, objective, etc.)
- [ ] Add model version management

### Phase 3: Advanced Features

- [ ] Categorical feature support
- [ ] Feature scaling/normalization builtin
- [ ] Model A/B testing (canary deployments)
- [ ] Model monitoring (latency, prediction distribution)
- [ ] Fallback to baseline scores on model failure

### Phase 4: Performance

- [ ] SIMD-optimized batch prediction
- [ ] GPU inference support
- [ ] Model quantization
- [ ] Distributed inference across nodes

## References

- [FunctionChain API Design](../../../../../../docs/design-docs/20260624-function-chain-api.md)
- [XGBoost Python Documentation](https://xgboost.readthedocs.io/)
- [Milvus Reranking Guide](../../../../../../docs/user_guide/reranking.md)
- [GitHub Issue #51192](https://github.com/milvus-io/milvus/issues/51192)

## Troubleshooting

### Model not found error

```
xgboost: failed to load model from s3://bucket/model.booster: 404 Not Found
```

**Solution**: Verify the model URI is correct and credentials have access to the model location.

### Feature count mismatch

```
xgboost: expected 3 input columns (features), got 2
```

**Solution**: Ensure the `features` parameter list has one entry per numeric column in the operator input.

### Invalid feature type

```
xgboost: feature column 0 (text_field) must be numeric, got string
```

**Solution**: Ensure the columns passed to the xgboost function are numeric types. Use `num_combine` to convert text embeddings to scores first.

### Stage not supported

```
proxy: function_chain_validator: xgboost not supported in L0_rerank stage
```

**Solution**: XGBoost is only available in L1_rerank and L2_rerank stages due to inference latency. Use simpler expressions (decay, num_combine) for L0 reranking.
