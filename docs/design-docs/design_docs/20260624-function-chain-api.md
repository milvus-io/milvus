# MEP: Function Chain API for Search Rerank

- **Created:** 2026-06-24
- **Author(s):** @junjie.jiang
- **Status:** Draft
- **Component:** SDK / Proxy / Function Chain
- **Related Issues:** TBD
- **Released:** N/A

## Summary

Function Chain introduces a typed, ordered, stage-aware pipeline for scoring and reranking search results. A chain is sent as structured protobuf, not as an opaque JSON string, so Milvus can validate dependencies, fetch required fields internally, execute built-in scoring functions, and project final search results back through the existing result schema.

The first release focuses on ordinary `SearchRequest` L2 rerank:

- Users build an L2 chain in SDKs and pass it through `function_chains`.
- Proxy validates the request, plans required rerank input fields, and reuses the existing rerank pipeline.
- The function-chain runtime executes `map`, `sort`, and `limit` operators on a DataFrame converted from reduced search results.
- Final `$score` is serialized through the existing search score/distance field.
- Intermediate variables and internally fetched fields are not returned unless requested by normal search output projection.

## Motivation

Milvus already has legacy rerank entry points such as `function_score` and ranker parameters. They are useful for predefined scoring formulas, but they do not provide a general ordered plan for composing multiple rerank steps.

Users need to express pipelines such as:

1. Compute a freshness score from a timestamp field.
2. Combine the original ANN score, freshness, and popularity.
3. Optionally call an external rerank model for text relevance.
4. Rewrite the final score.
5. Sort and optionally trim candidates.

Representing this as typed operations gives Milvus:

- deterministic execution order;
- typed nested parameters without JSON-in-string encoding;
- explicit field dependency analysis;
- consistent `$score` semantics;
- future room for additional stages and operators.

## Goals

- Add public protobuf messages for a function-chain logical plan.
- Add SDK builder APIs that compile to the protobuf plan.
- Support ordinary Search L2 rerank through `SearchRequest.function_chains`.
- Reuse the existing Proxy rerank pipeline rather than adding a separate search pipeline operator.
- Fetch function-chain-required schema fields internally even when users do not request them in `output_fields`.
- Keep final search response projection Search-owned.
- Support first-version built-in expressions:
  - `decay`
  - `num_combine`
  - `round_decimal`
  - `rerank_model`
- Preserve compatibility with existing search and legacy rerank behavior.

## Non-Goals

The first release does not include:

- `function_chains` support for hybrid search execution;
- insert/upsert/ingestion function chains;
- L0/L1 pushdown to QueryNode or Segcore;
- arbitrary user-defined expression language;
- returning intermediate chain variables as user-facing result fields;
- replacing `function_score` or legacy rank parameters;
- client-side execution of external model calls.

The public stage enum reserves room for future stages, but ordinary Search initially accepts only `L2_RERANK`.

## Public Interfaces

### PyMilvus DSL

A user builds a chain with `FunctionChain`, `FunctionChainStage`, `col`, and helper functions under `fn`:

```python
from pymilvus import FunctionChain, FunctionChainStage
from pymilvus.function_chain import col, fn

chain = (
    FunctionChain(FunctionChainStage.L2_RERANK, name="fresh_popular_rerank")
    .map(
        "freshness",
        fn.decay(
            col("published_at"),
            function="exp",
            origin=current_time,
            scale=86400,
            offset=0,
            decay=0.5,
        ),
    )
    .map(
        "$score",
        fn.num_combine(
            col("$score"),
            col("freshness"),
            col("popularity"),
            mode="weighted",
            weights=[0.7, 0.2, 0.1],
        ),
    )
    .map("$score", fn.round_decimal(col("$score"), decimal=4))
    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
    .limit(10)
)

client.search(
    collection_name="articles",
    data=[query_vector],
    anns_field="embedding",
    search_params={"metric_type": "IP"},
    limit=100,
    output_fields=["title"],
    function_chains=chain,
)
```

For model rerank:

```python
chain = (
    FunctionChain(FunctionChainStage.L2_RERANK, name="model_rerank")
    .map(
        "$score",
        fn.rerank_model(
            col("doc"),
            queries=["renewable energy developments"],
            provider="voyageai",
            model_name="rerank-2.5",
            truncation=True,
            max_client_batch_size=128,
        ),
    )
    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
)
```

External model credentials are resolved by Milvus server-side provider configuration. SDK requests should not carry API keys.

### Search API

Ordinary Search accepts `function_chains`:

```python
client.search(..., function_chains=chain)
client.search(..., function_chains=[chain])
```

SDK and server validation reject ambiguous combinations:

- `function_chains` with SDK `ranker` / proto `function_score`;
- non-L2 chains for ordinary Search;
- `function_chains` for hybrid search in the first release.

### Protobuf

The public protobuf models a chain as an ordered logical plan:

```proto
enum FunctionChainStage {
  FunctionChainStageUnspecified = 0;
  FunctionChainStageIngestion = 1;
  FunctionChainStagePreProcess = 2;
  FunctionChainStageL0Rerank = 3;
  FunctionChainStageL1Rerank = 4;
  FunctionChainStageL2Rerank = 5;
  FunctionChainStagePostProcess = 6;
}

message FunctionChain {
  string name = 1;
  FunctionChainStage stage = 2;
  repeated FunctionChainOp ops = 3;
}

message FunctionChainOp {
  string op = 1;
  FunctionChainExpr expr = 2;
  repeated string inputs = 3;
  repeated string outputs = 4;
  map<string, FunctionParamValue> params = 5;
}

message FunctionChainExpr {
  string name = 1;
  repeated FunctionChainExprArg args = 2;
  map<string, FunctionParamValue> params = 3;
}

message FunctionChainExprArg {
  oneof arg {
    FunctionChainColumnArg column = 1;
    FunctionParamValue literal = 2;
  }
}

message FunctionChainColumnArg {
  string name = 1;
}

message FunctionParamValue {
  oneof value {
    bool bool_value = 1;
    int64 int64_value = 2;
    double double_value = 3;
    string string_value = 4;
    FunctionParamArray array_value = 5;
    FunctionParamObject object_value = 6;
    bytes bytes_value = 7;
  }
}

message FunctionParamArray {
  repeated FunctionParamValue values = 1;
}

message FunctionParamObject {
  map<string, FunctionParamValue> fields = 1;
}
```

`SearchRequest` carries chains through:

```proto
repeated schema.FunctionChain function_chains = 24;
```

Hybrid request proto may reserve a field for future support, but first-version execution rejects it.

## Semantics

### `$score`

`$score` is a system virtual column, not a collection field.

Runtime behavior:

1. At rerank input construction, `$score` is initialized from the current search result score/distance.
2. Functions can read `$score` through `col("$score")`.
3. `map("$score", expr)` overwrites the current score register.
4. `sort(col("$score"), desc=True)` sorts candidates by the current rewritten score.
5. The final `$score` is serialized through existing result score/distance fields.
6. SDK users observe it as the normal hit distance/score value.

Representation:

| Layer | Representation |
|-------|----------------|
| Python DSL | `"$score"` |
| Proto | `FunctionChainColumnArg.name = "$score"` |
| Runtime | score register / DataFrame column |
| Search result | existing distance/score field |

`$id` is also available as a read-only system value for tie-breaking. Other `$xxx` system names are rejected in first-version L2 rerank.

### Operators

#### `map`

`map(output, expr)` evaluates an expression and writes the result to `output`.

- `output` may be a temporary variable such as `freshness`.
- `output` may be writable system value `$score`.
- First-version L2 rerank does not allow writing `$id` or unknown `$xxx` values.

#### `sort`

`sort(by, desc=True, tie_break_col=None)` sorts the current candidate chunk.

- `by` is encoded as an op input and parameter.
- `tie_break_col` is optional and is also encoded as an input.
- Sorting is explicit. Milvus does not infer ordering direction from vector metric type after a chain sort is present.

#### `limit`

`limit(limit, offset=0)` trims each query chunk after previous operators.

This is part of the user-provided plan. Search does not append implicit `limit` or `offset` operators to public function chains.

### Built-in expressions

#### `decay`

Computes a numeric decay score from one numeric input column.

Parameters:

- `function`: `gauss`, `exp`, or `linear`
- `origin`
- `scale`
- `offset`
- `decay`

#### `num_combine`

Combines two or more numeric inputs.

Modes:

- `multiply`
- `sum`
- `max`
- `min`
- `avg`
- `weighted`

`weighted` mode requires one numeric weight per input.

#### `round_decimal`

Rounds one Float32 score column to a fixed number of decimal places in `[0, 6]`.

#### `rerank_model`

Calls an external rerank model provider for a text column. It is only runnable at L2 rerank stage in the first release.

Required parameters:

- `queries`: one query per search query chunk.
- provider parameters such as `provider`, `model_name`, `max_client_batch_size`, and provider-specific options.

Provider credentials and endpoint defaults are resolved on the Milvus server using existing function provider configuration.

## Input, Write, and Projection Semantics

Function Chain separates chain execution names from final result projection.

```text
expr-based op read names = column references in FunctionChainExpr.args
non-expr op read names   = FunctionChainOp.inputs
op write names           = FunctionChainOp.outputs
final result projection  = Search-owned output projection
```

Example:

```python
FunctionChain(FunctionChainStage.L2_RERANK) \
    .map("freshness", fn.decay(col("published_at"), ...)) \
    .map("$score", fn.num_combine(col("$score"), col("freshness"), mode="sum")) \
    .sort(col("$score"), desc=True)
```

Dependency analysis sees:

- required input before previous writes: `published_at`, `$score`;
- written names: `freshness`, `$score`;
- `freshness` is not fetched from collection schema because a previous op writes it;
- `published_at` is fetched internally for rerank even if it is not in user `output_fields`;
- final response returns only `$id`, final `$score`, and user-requested output fields.

Intermediate variables such as `freshness` are not returned to the user.

## Design Details

### High-level Search flow

```text
SearchRequest.function_chains
  -> SDK serialization
  -> Proxy request validation
  -> chain.ProtoChainToRepr
  -> Proxy L2 input planning
  -> normal search/reduce/requery pipeline
  -> rerankOperator builds DataFrame
  -> chain.FuncChainFromRepr
  -> FuncChain.Execute
  -> Search-owned final projection
```

Function Chain L2 rerank is treated as a rerank source. It reuses the existing `rerankOperator` rather than adding a separate `functionChainOperator`.

### Internal representation

The chain package converts public proto to a caller-independent representation:

```go
type ChainRepr struct {
    Name      string
    Stage     string
    Operators []OperatorRepr
    Info      ChainReprInfo
}

type ChainReprInfo struct {
    RequiredInputs []string
    WrittenNames   []string
    Ops            []OperatorReprInfo
}

type OperatorReprInfo struct {
    Type       string
    ReadNames  []string
    WriteNames []string
}
```

`ChainRepr.Info.RequiredInputs` only means "the chain reads these names before any previous op writes them." It does not decide whether a name is a schema field, runtime system value, or invalid. That classification is caller-owned.

### Proxy L2 input planning

For ordinary Search L2 rerank, Proxy classifies each required input:

1. `$score` and `$id` are runtime system inputs.
2. Other names must resolve to supported collection schema fields.
3. Unknown non-system names are rejected.
4. Unsupported `$xxx` system inputs are rejected.
5. Temporary variables written by previous ops are not fetched from schema.

First-version supported schema input field types:

- Bool
- Int8 / Int16 / Int32 / Int64 / Timestamptz
- Float / Double
- String / VarChar / Text

Unsupported input field types include vector fields, JSON, Array, Geometry, and dynamic field subkeys.

### Request-level rules

#### `function_score` conflict

`function_score` and `function_chains` are mutually exclusive:

```text
function_score and function_chains cannot be used together
```

Both APIs define rerank score behavior. Combining them would make ordering ambiguous.

SDK `ranker` maps to legacy rerank/function-score behavior, so SDK rejects `ranker` plus `function_chains` before RPC.

#### Stage uniqueness

The same `FunctionChainStage` may appear at most once in one request.

The first release supports only one ordinary Search stage:

```text
FunctionChainStageL2Rerank
```

Users who need multiple rerank steps should put multiple ops in one L2 chain instead of sending multiple L2 chains.

#### Hybrid search

First-version hybrid search rejects `function_chains`:

```text
function_chains is not supported for hybrid search yet
```

Hybrid support needs a separate design for whether public chains apply to sub-searches, merged candidates, or both.

### Requery and field availability

No new fetch mechanism is required. Function-chain input fields flow through existing rerank metadata:

```text
rerankMeta.GetInputFieldNames()
rerankMeta.GetInputFieldIDs()
```

When requery is needed, the requery operator includes rerank-required field names so the DataFrame can be built before rerank execution. Final projection still uses user output fields and does not expose internally fetched rerank inputs.

### Rerank operator integration

`rerankOperator` follows the existing function-score flow:

```text
SearchResultData
  -> chain.FromSearchResultData(..., neededFields)
  -> build FuncChain from rerank metadata
  -> ExecuteWithContext
  -> chain.ToSearchResultDataWithOptions(...)
```

The chain builder dispatches by rerank metadata type:

- legacy function score -> existing function-score chain builder;
- legacy rank params -> existing legacy rank builder;
- public function chain -> `FuncChainFromRepr` / `FuncChainFromReprWithContext`.

### Tail behavior

A public `FunctionChain` is executed as sent.

Milvus does not implicitly append tail operators such as:

- sort;
- limit;
- group-by;
- round-decimal.

Users or SDK helpers must add explicit ops when they want those effects.

## Validation Rules

First-version validation includes:

1. Function chain proto must not be nil.
2. Stage must be supported by the request type.
3. Duplicate stages are rejected.
4. Operator names must be non-empty.
5. Expression names must be non-empty when an expression is present.
6. Column references and input/output names must be non-empty.
7. Expr args must be either column refs or supported literals.
8. Parameter values must be typed and convertible to runtime values.
9. L2 input system names are restricted to `$id` and `$score`.
10. L2 system outputs are restricted to `$score`.
11. Non-system required inputs must be supported collection fields.
12. Unknown operators and functions are rejected.
13. A function must be runnable at the chain stage.
14. Function-specific parameters must pass validation.
15. External rerank model query count must match query chunk count.

Additional ordering constraints such as "at most one sort" or "sort must be last" can be considered as future stricter validation. The first release executes the ordered plan as sent unless a runtime operator rejects it.

## Compatibility, Deprecation, and Migration Plan

### Compatibility

- Existing search requests without `function_chains` are unchanged.
- Existing `function_score` and legacy rank behavior remain supported.
- Public function chains are opt-in.
- Existing result schema is preserved; final score is exposed through current score/distance fields.

### Deprecation

No deprecation is introduced in this MEP.

### Migration

Users can migrate from `function_score` or ranker APIs to `function_chains` when they need explicit ordered composition. There is no automatic conversion in the first release.

## Security Considerations

External model rerank can call third-party services from Milvus server processes.

Security requirements:

1. API credentials are resolved server-side through existing provider configuration or credential stores.
2. SDK `function_chains` requests should not include raw API keys.
3. Provider credentials must be redacted in logs by existing credential handling paths.
4. Provider endpoints should use HTTPS unless explicitly configured for trusted local testing.
5. Requests to external providers may include user text fields. Deployments must treat this as data egress and configure providers accordingly.
6. Timeouts and batching limits must prevent unbounded external calls.

## Observability

Initial observability reuses existing search/function error paths and provider HTTP errors.

Useful follow-up metrics:

- function-chain execution latency by operator/function type;
- number of internally fetched fields for function-chain rerank;
- external rerank provider latency and error count by provider;
- rejected function-chain requests by validation category.

## Test Plan

### SDK tests

1. Builder serialization for `map`, `sort`, `limit`.
2. Typed parameter serialization for scalar, bytes, arrays, and nested objects.
3. `col(...)` validation.
4. Helper validation for `decay`, `num_combine`, `round_decimal`, and `rerank_model`.
5. Search request encoding with single chain and list of chains.
6. Reject `function_chains` plus `ranker`.
7. Reject non-L2 chains for ordinary Search.
8. Reject `function_chains` for hybrid search.

### Proxy and chain planning tests

1. `function_score` plus `function_chains` is rejected.
2. Duplicate L2 chains are rejected.
3. Non-L2 chain stages are rejected in ordinary Search.
4. Hybrid Search with `function_chains` is rejected.
5. `$score`-only chain succeeds with no schema input fields.
6. `field + $score` chain fetches only required schema fields.
7. A previous op output used by a later op is not fetched as a schema field.
8. Unknown non-system input is rejected.
9. Unsupported `$xxx` system input is rejected.
10. Unsupported system output is rejected.
11. Unsupported schema input field type is rejected.

### Rerank path tests

1. `buildChainFromMeta` builds a `FuncChain` from `functionChainRerankMeta`.
2. Existing `rerankOperator` executes a proto-derived chain through DataFrame.
3. A chain that maps and sorts `$score` changes result order and scores.
4. A chain with `limit` updates per-query TopKs.
5. Chain-required fields are available after requery but are not exposed in final response fields.
6. Existing `function_score` and legacy rank behavior remain unchanged.
7. Optional external provider test for `rerank_model`, such as VoyageAI, gated on server-side credentials.

### Regression checks

Run targeted Go tests with Milvus test flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/chain/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run 'FunctionChain|Rerank'
```

Run SDK tests from the PyMilvus repository or local checkout as appropriate.

## Rejected Alternatives

### 1. Encode params as JSON strings in `KeyValuePair`

Rejected because function-chain parameters may contain nested objects, arrays, booleans, integers, floating-point values, and bytes. JSON-in-string encoding would cause:

- late parsing failures;
- weak type information;
- inconsistent SDK behavior;
- weaker validation errors;
- ambiguity around numeric types.

`FunctionParamValue` keeps the public plan typed.

### 2. Reuse `function_score` for all chain behavior

Rejected because `function_score` is not an ordered operator pipeline and cannot naturally express multiple map/sort/limit/model steps with explicit dependencies.

### 3. Add a separate `functionChainOperator` to the search pipeline

Rejected for the first release because function chains are another rerank implementation. Reusing the existing `rerankOperator` keeps fetch/requery/final-projection behavior consistent with legacy rerank.

### 4. Classify required inputs in the generic chain package

Rejected because only the caller knows whether a name is a schema field, request payload field, runtime system value, or invalid. The chain package only reports structural dependencies.

## Open Questions

1. What is the best public API for hybrid search support: top-level post-merge chain, per-sub-search chains, or both?
2. Which functions should be allowed in future L0/L1 stages, and where should they execute?
3. Should strict operator ordering rules be enforced, such as one `sort` and only as the last ordering op?
4. Should users be able to return intermediate variables explicitly in future APIs?
5. Should provider-specific metrics be standardized across embedding and rerank model providers?
