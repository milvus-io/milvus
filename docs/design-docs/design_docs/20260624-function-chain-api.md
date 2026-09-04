# MEP: Function Chain API for Search Rerank

- **Created:** 2026-06-24
- **Author(s):** @junjie.jiang
- **Status:** Draft
- **Component:** SDK / Proxy / Function Chain
- **Related Issues:** TBD
- **Released:** N/A

## Summary

Function Chain introduces a typed, ordered, stage-aware pipeline for scoring and reranking search results. A chain is sent as structured protobuf, not as an opaque JSON string, so Milvus can validate dependencies, fetch required fields internally, execute built-in scoring functions, and project final search results back through the existing result schema.

Ordinary `SearchRequest` supports three rerank stages at distinct execution boundaries:

- L0 executes independently on each segment result in the worker QueryNode before cross-segment reduction.
- L1 executes on each worker QueryNode after its cross-segment reduction and before results are returned to the shard leader.
- L2 executes in Proxy after distributed/global reduction and reuses the existing rerank pipeline.
- L0 supports `map`; the first L1 release supports `map`, `sort`, and `limit`; L2 uses the generic function-chain runtime operators allowed by validation.
- Final `$score` is serialized through the existing search score/distance field.
- Intermediate variables, internally fetched fields, and internal provenance columns are not returned unless requested through normal search output projection.

The native XGBoost L0 expression and its execution constraints are described in [XGBoost FunctionChain Expression Design](20260708-xgboost-function-chain.md). The embedded Python L2 expression and its Production Runtime boundary are described in [PyUDF FunctionChain Expression](20260722-pyudf-function-chain.md).

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
- Support ordinary Search L0, L1, and L2 rerank through `SearchRequest.function_chains`.
- Execute L0 per segment, L1 after each worker QueryNode reduction, and L2 after Proxy global reduction.
- Reuse the existing Proxy rerank pipeline for L2 rather than adding a separate search pipeline operator.
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

- `function_chains` support for hybrid or advanced search execution;
- insert/upsert/ingestion function chains;
- L1 execution at the shard-leader reduction boundary or at multiple QueryNode reduction levels;
- L1 `filter`, `select`, or `group_by` operators;
- arbitrary user-defined expression language;
- returning intermediate chain variables as user-facing result fields;
- replacing `function_score` or legacy rank parameters;
- client-side execution of external model calls.

The public stage enum also reserves room for ingestion, preprocessing, and postprocessing. Ordinary Search accepts at most one chain at each of `L0_RERANK`, `L1_RERANK`, and `L2_RERANK`.

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

SDK and server validation reject ambiguous or unsupported combinations:

- `function_chains` with SDK `ranker` / proto `function_score`;
- stages other than L0, L1, and L2 for ordinary Search;
- `function_chains` for hybrid or advanced search;
- Function rerank with Search Iterator or `order_by`;
- L1 with search aggregation.

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

`$id` is also available as a read-only system value for tie-breaking. For public L0 and L1 chains, `$id` and `$score` are the only readable system columns and `$score` is the only writable system column. Internal columns used for segment offsets, grouping, element metadata, or L1 provenance are not part of the public chain namespace and must never be exposed in result fields.

### Stage execution semantics

The stages form one distributed rerank pipeline:

```text
segment ANN result
  -> L0 per-segment chain
  -> worker QueryNode cross-segment reduce / PK dedup / group-aware merge
  -> L1 per-worker merged-candidate chain
  -> shard-leader reduce
  -> Proxy global reduce
  -> L2 Proxy chain
  -> final projection
```

L1 runs exactly once for each worker QueryNode's merged result. It does not run again at the shard leader. This boundary lets L1 compare candidates across segments handled by one worker while preserving the existing distributed reducer and wire protocol.

| Stage | Execution boundary | First-version operators |
|-------|--------------------|-------------------------|
| L0 | Each segment result in worker QueryNode, before cross-segment reduce | `map` |
| L1 | Each worker QueryNode result, after cross-segment reduce | `map`, `sort`, `limit` |
| L2 | Proxy result, after distributed/global reduce | Generic runtime operators accepted by stage validation |

A function used by an operator must also declare that it is runnable at that stage. For example, an expression restricted to L0 or L2 is not made valid in L1 merely because it appears in an allowed `map` operator.

### Operators

#### `map`

`map(output, expr)` evaluates an expression and writes the result to `output`.

- L0, L1, and L2 may write a temporary variable such as `freshness` for use by later operators in the same chain.
- L0 and L1 may also overwrite ordinary collection columns within their stage-local DataFrames.
- `output` may be writable system value `$score`.
- First-version rerank does not allow writing `$id` or unknown `$xxx` values.

#### `sort`

`sort(by, desc=True, tie_break_col=None)` sorts the current candidate chunk.

- `by` is encoded as an op input and parameter.
- `tie_break_col` is optional and is also encoded as an input.
- Sorting is explicit. Milvus does not infer ordering direction from vector metric type after a chain sort is present.

#### `limit`

`limit(limit, offset=0)` trims each query chunk after previous operators.

This is part of the user-provided plan. Search does not append an implicit public `limit` or `offset` operator.

For L1, `limit` is an intermediate candidate budget applied independently for each worker and each query-vector chunk. It is not the final client Search limit. A candidate discarded by a worker cannot reappear at the shard leader or Proxy, so L1 `limit` can reduce recall. Users must size it for the expected worker fan-out and desired recall.

When an L1 chain contains both user `sort` and `limit`, the limit observes the user-defined ordering. After the complete user chain, QueryNode applies an internal normalization sort by `$score` descending with `$id` ascending as the tie-break. This internal step is not a public chain operator; it restores the ordering contract required by downstream shard and Proxy reducers without changing which candidates the user's L1 `limit` selected.

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
  -> Proxy request validation and stage split
  -> L0/L1 serialized into PlanNode.querynode_function_chains
  -> L2 retained as Proxy rerank metadata
  -> worker QueryNode exports per-segment Arrow DataFrames
  -> L0 executes on each segment DataFrame
  -> worker QueryNode cross-segment reduce
  -> L1 materializes required fields and executes on the merged DataFrame
  -> shard-leader and Proxy reductions
  -> L2 rerankOperator builds and executes a DataFrame chain
  -> Search-owned final projection
```

No new protobuf transport is required for L1. It uses the existing `PlanNode.querynode_function_chains` field together with L0 and is distinguished by `FunctionChain.stage`.

Function Chain L2 rerank is treated as a Proxy rerank source. It reuses the existing `rerankOperator` rather than adding a separate `functionChainOperator`. L1 is part of worker QueryNode Go reduction and does not create a new Proxy pipeline operator.

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

### QueryNode L0/L1 preparation

Proxy serializes both L0 and L1 chains into the physical plan. Worker QueryNode parses the `planpb.PlanNode` once, converts public chains to `ChainRepr`, validates each chain according to its stage, and plans L0 and L1 schema inputs separately. Legacy `function_score` is also normalized during this preparation pass into an L0 prepared configuration containing its scorers and resolved score-combine modes. The protobuf plan is not retained by L0 or L1 execution; segment-specific boost-score runners are bound only when L0 executes.

Only public L0 input fields are exported with each segment search result before reduction. A prepared legacy boost score needs only the existing segment offsets and does not add collection fields to this export. L1 input fields are materialized after cross-segment reduction for the surviving worker candidates.

L0 retains its segment-local behavior and accepts only `map`; ordinary temporary and collection columns may be written within its segment-local DataFrame. L1 accepts `map`, `sort`, and `limit`; ordinary temporary and collection columns may also be written within the L1 chain. Among public system columns, only `$score` is writable in either stage; `$id` and other `$xxx` system columns are read-only. Both stages use the same scalar input type set listed above.

### L1 input materialization

The worker cross-segment reducer produces a merged DataFrame containing ranking and reduction metadata plus a parallel source map:

```go
type segmentSource struct {
    InputIdx    int
    SegOffset   int64
    OriginalIdx int
}

type mergeResult struct {
    DF      *chain.DataFrame
    Sources [][]segmentSource
}
```

Ordinary scalar fields required only by L1 are not exported with all segment ANN candidates and are not copied during heap merge. Before L1 execution, QueryNode flattens the merged result's source map and reads only the surviving candidates from their source segments:

```text
Sources[query chunk][row].{InputIdx, SegOffset}
  -> ordered segment field read for L1 required field IDs
  -> Arrow RecordBatch in merged-row order
  -> L1 input DataFrame with mergedDF chunk sizes
```

The read API groups requested offsets by segment, performs field subscripts against each segment, and scatters the values back into the exact caller-provided order. `OriginalIdx` remains useful to the reducer for pre-reduce metadata columns but is not part of the L1 field-materialization contract.

Materialization must preserve Arrow type, field ID/type/nullability metadata, null values, chunk count, and row count. Missing fields, invalid source indexes or offsets, malformed Arrow metadata, or mismatched DataFrame/source shapes indicate an internal result-contract failure rather than invalid request content.

### L1 provenance and late materialization

`Sources` is later consumed by `FillOutputFieldsOrdered` to materialize requested output fields in final result order. Therefore, transforming only `mergeResult.DF` with L1 `sort` or `limit` would corrupt the row-to-segment mapping.

Before L1 execution, QueryNode attaches a hidden per-chunk source-index column. Each token is the row's index in that chunk's current `Sources` slice. All Function Chain row operators transform every column, so the token follows each row through user `map`, `sort`, and `limit`, as well as the internal normalization sort. After execution, QueryNode uses the transformed token to rebuild `Sources` and then removes the hidden column.

A PK must not be used as the provenance token. Element-level search may contain multiple rows with the same PK, and source reconstruction by PK would be ambiguous. The hidden token is internal-only: public chains cannot read it, and it must not appear in `SearchResultData.FieldsData` or final output projection.

The following invariants must hold before late materialization:

1. The DataFrame and `Sources` have the same number of query chunks.
2. Each DataFrame chunk has exactly one corresponding source entry per row.
3. Every provenance token is non-null, has the internal integer type, and is in range for the original source chunk.
4. Rebuilt `Sources` is in exactly the same order as the final L1 DataFrame.
5. The provenance token and L1-only temporary input columns are removed before wire serialization.

### Request-level rules

#### `function_score` conflict

`function_score` and `function_chains` are mutually exclusive:

```text
function_score and function_chains cannot be used together
```

Both APIs define rerank score behavior. Combining them would make ordering ambiguous.

SDK `ranker` maps to legacy rerank/function-score behavior, so SDK rejects `ranker` plus `function_chains` before RPC.

#### Stage uniqueness

The same `FunctionChainStage` may appear at most once in one request. One L0, one L1, and one L2 chain may coexist, and their execution order is fixed by stage rather than request-list order.

Users who need multiple steps at one stage should put multiple ordered ops in that stage's chain instead of sending duplicate chains.

#### L1 compatibility

The first L1 release has these request-level restrictions:

- L1 is not supported for hybrid or advanced search.
- L1 is not supported with Search Iterator (legacy or v2).
- L1 is not supported with `order_by`, because both define ordering behavior.
- L1 is not supported with search aggregation.
- Search-level group-by may be combined with L1 `map`, `sort`, or `limit`, matching L2 compatibility. Function Chain operators may reorder or trim grouped rows without rebuilding the original group count or `group_size` contract.

These validations occur before execution so an unsupported combination does not silently produce incomplete distributed results.

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

### QueryNode L0 execution and Arrow allocation

QueryNode executes an L0 chain independently for each segment before Go heap reduction. It builds a fresh `FuncChain` for each segment so mutable operator or expression state is not shared by concurrent segment execution.

Arrow buffers allocated by QueryNode L0 chain operators through `FuncContext.Pool()` use Arrow Go's libc-backed `mallocator`. This includes intermediate buffers produced by Go expressions before a later native expression exports them through the Arrow C Data Interface. The allocator itself has no `Close` operation; normal Arrow array, chunked-array, and DataFrame release chains return the underlying buffers to libc.

This allocator policy applies to public L0 chains and the operators in the internally generated boost-score chain. It does not replace allocators owned by native helpers or imported arrays; for example, a boost-score runner may return an array with its own allocator. Go-only heap reduction also retains its existing allocator, and imported segment DataFrames retain the ownership supplied by the C++ Arrow exporter.

### Tail behavior

A public `FunctionChain` is executed in its declared operator order. Milvus does not implicitly append public operators such as `limit`, group-by, or round-decimal.

L2 is executed as sent and does not receive an implicit sort. L0 and L1 are internal inputs to downstream score-merge reducers, so QueryNode appends a non-public normalization sort by `$score` descending and `$id` ascending after the user chain. For L1 this normalization occurs after every user operator, including `limit`, and therefore does not change which candidates the user plan selected.

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
9. L0/L1/L2 public input system names are restricted to `$id` and `$score`.
10. Public system outputs are restricted to `$score`.
11. Non-system required inputs must be supported collection fields.
12. L0 accepts only `map`; L1 accepts only `map`, `sort`, and `limit`.
13. Unknown operators and functions are rejected.
14. A function must be runnable at the chain stage.
15. Function-specific parameters must pass validation.
16. External rerank model query count must match query chunk count.
17. L1 is rejected with search aggregation.
18. Function rerank is rejected with Search Iterator (legacy or v2) and `order_by`.

Additional ordering constraints such as "at most one sort" or "sort must be last" can be considered as future stricter validation. The first release executes the user's ordered plan as sent unless an operator rejects it, then applies only the internal L0/L1 reducer normalization described above.

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

QueryNode reuses `milvus_querynode_function_chain_latency` for L0 and L1 execution. L1 records `chain_level="l1"` and the existing success/failure status label. The timed L1 phase includes required-field materialization, chain construction and execution, provenance reconstruction, and internal normalization. Metric labels must remain bounded; field names, expressions, collection names, and chain names are not metric labels.

Runtime errors use the existing typed error paths. Invalid user plans and unsupported combinations are input errors. Missing internal fields, malformed DataFrames, invalid source indexes, provenance corruption, and result-shape violations are system/internal failures. When adding context to an existing typed error, implementations use `merr.Wrap` or `merr.Wrapf` so the original code survives.

Useful follow-up metrics:

- function-chain execution latency by operator/function type;
- number of internally fetched fields for function-chain rerank;
- external rerank provider latency and error count by provider;
- rejected function-chain requests by validation category.

## Test Plan

### SDK tests

1. Builder serialization for `map`, `sort`, `limit`, and all supported stages.
2. Typed parameter serialization for scalar, bytes, arrays, and nested objects.
3. `col(...)` validation.
4. Helper validation for `decay`, `num_combine`, `round_decimal`, and `rerank_model`.
5. Search request encoding with one chain and with L0/L1/L2 chains.
6. Reject `function_chains` plus `ranker`.
7. Reject unsupported stages for ordinary Search.
8. Reject `function_chains` for hybrid search.

### Proxy and chain planning tests

1. `function_score` plus `function_chains` is rejected.
2. Duplicate chains at each stage are rejected.
3. L0, L1, and L2 route to the correct execution component and may coexist.
4. Unsupported stages and hybrid/advanced Search are rejected.
5. Iterator v2 and `order_by` conflicts are rejected.
6. L1 with search aggregation is rejected.
7. Search group-by accepts L1 `map`, `sort`, and `limit`, matching L2 request compatibility.
8. `$score`-only chains succeed with no schema input fields.
9. `field + $score` chains fetch only required schema fields.
10. L0 and L1 required field IDs are planned separately; pre-reduce Arrow export receives only L0 field IDs.
11. A previous op output used by a later L1 op is not fetched as a schema field; L0 allows temporary and collection-column outputs while restricting system outputs to `$score`.
12. Unknown inputs, unsupported `$xxx` names, invalid system outputs, and unsupported field types are rejected.

### QueryNode L1 tests

1. L1 accepts `map`, `sort`, and `limit`; it rejects `filter`, `select`, and `group_by`.
2. L1 functions must be runnable at `L1_RERANK`.
3. Scalar inputs, including null values, are materialized from the source segment rows.
4. Int64 and string PK results preserve source identity.
5. Element-level results with duplicate PKs preserve distinct source rows.
6. User sort determines the candidates selected by user limit.
7. Internal score-descending/ID-ascending normalization runs after the user chain.
8. Ragged TopKs and empty query chunks retain valid DataFrame/source shapes.
9. Missing fields, Arrow type mismatches, invalid source indexes, and malformed provenance tokens return typed internal errors.
10. The hidden provenance column and L1-only inputs are absent from serialized result fields.
11. Success and failure paths record the L1 latency metric exactly once and release Arrow resources.

### Reduction and late-materialization tests

1. Both worker Go-reduce paths execute L1 before late materialization.
2. L1 sort and limit reorder `mergeResult.Sources` together with IDs and scores.
3. Requested output fields remain attached to the correct hit after L1 reordering and trimming.
4. Merged SearchTasks with mixed NQ/topK preserve per-request slicing after L1.
5. Group-by and element metadata remain aligned.
6. Shard-leader reduction correctly merges score-normalized L1 outputs from multiple workers.
7. Worker-local limit behavior is explicit: discarded candidates do not reappear downstream.

### L2 and regression tests

1. `buildChainFromMeta` builds a `FuncChain` from `functionChainRerankMeta`.
2. Existing `rerankOperator` executes a proto-derived chain through DataFrame.
3. A chain that maps and sorts `$score` changes result order and scores.
4. A chain with `limit` updates per-query TopKs.
5. Chain-required fields are available after requery but are not exposed in final response fields.
6. L0 + L1 + L2 execute in stage order.
7. Existing `function_score` and legacy rank behavior remain unchanged.
8. Optional external provider test for `rerank_model`, gated on server-side credentials.

### End-to-end tests

Python and REST tests cover L1 score mapping, hidden scalar inputs, sort plus limit, L0/L1/L2 composition, incompatibility validation, output-field alignment, and worker-local candidate-budget semantics. Test data must distinguish segment-local L0, worker-level L1, and Proxy-global L2 so a passing result proves the selected execution boundary rather than only the final arithmetic.

### Regression checks

Run targeted Go tests with Milvus test flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/chain/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run 'FunctionChain|Rerank|L1'
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querynodev2/tasks/... -run 'FunctionChain|L1|GoReduce'
```

Because L1 changes distributed ordering, provenance, and late materialization, verification also includes the full Go test suite and end-to-end failure-mode tracing. A green success-path search alone is not evidence that source alignment or worker-local limit semantics are correct.

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
2. Which additional functions and operators should be allowed in future L0/L1 stages?
3. Should L1 eventually support a shard-leader execution mode in addition to worker post-reduce execution?
4. Should strict operator ordering rules be enforced, such as one `sort` and only as the last ordering op?
5. Should users be able to return intermediate variables explicitly in future APIs?
6. Should provider-specific metrics be standardized across embedding and rerank model providers?
