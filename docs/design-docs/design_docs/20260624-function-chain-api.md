# MEP: Function Chain API for Search Rerank

- **Created:** 2026-06-24
- **Author(s):** @junjie.jiang
- **Status:** Draft
- **Component:** SDK / Proxy / QueryNode / Function Chain
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

The server implementation also accepts typed paths into JSON fields and the dynamic `$meta` field at L0, L1, and L2. Paths become nullable scalar Arrow columns before chain execution. The proposed PyMilvus `col(..., data_type=...)` convenience API and mixed-version projection negotiation remain pending; see [Implementation and verification status](#implementation-and-verification-status).

The native XGBoost L0 expression and its execution constraints are described in [XGBoost FunctionChain Expression Design](20260708-xgboost-function-chain.md). The Python worker L2 expression and its gRPC runtime boundary are described in [PyUDF FunctionChain Expression](20260722-pyudf-function-chain.md).

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
- Support explicitly typed JSON and dynamic-field paths through a shared input plan across L0, L1, and L2.
- Support first-version built-in expressions:
  - `decay`
  - `num_combine`
  - `round_decimal`
  - `rerank_model`
- Preserve compatibility with existing search and legacy rerank behavior.

## Non-Goals

The first release does not include:

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

### JSON and dynamic-field column references

JSON paths use the existing identifier grammar, including nested object keys and array indexes. A path must declare one of `DataType.BOOL`, `DataType.INT64`, `DataType.DOUBLE`, or `DataType.VARCHAR`.

The following is the **proposed SDK syntax**. As of the implementation review on 2026-09-10, the inspected PyMilvus checkout still exposes `col(name)`; the `data_type` argument, its serialization, and SDK validation have not been implemented there.

```python
from pymilvus import DataType
from pymilvus.function_chain import col

price = col('metadata["price"]', data_type=DataType.DOUBLE)
category = col('metadata["categories"][0]', data_type=DataType.VARCHAR)
ctr = col('$meta["ctr"]', data_type=DataType.INT64)
enabled = col('$meta["enabled"]', data_type=DataType.BOOL)
```

Ordinary schema fields continue to use `col("price")` without a hint. Dynamic-field paths require explicit `$meta[...]` syntax and an actual dynamic JSON field in the collection schema. An unknown bare name such as `col("ctr")` does not fall back to `$meta["ctr"]`. Complete JSON roots, including `col("metadata")` and `col("$meta")`, are not supported as chain inputs. Nested paths on non-JSON fields are rejected.

### Search API

Ordinary Search accepts `function_chains`:

```python
client.search(..., function_chains=chain)
client.search(..., function_chains=[chain])
```

SDK and server validation reject ambiguous or unsupported combinations:

- `function_chains` with SDK `ranker` / proto `function_score`;
- stages other than L0, L1, and L2 for ordinary Search;
- top-level hybrid chains outside L2, or without exactly one leading `merge`;
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

Hybrid requests accept per-sub-search L0/L1 chains and one top-level L2 chain.

### JSON-path type hints on the wire

The server reads type hints from the reserved `FunctionChainOp.params["$input_data_types"]` entry. Its value is a `FunctionParamArray` whose elements are `int64` schema `DataType` enum values. `FunctionChainColumnArg` still contains only `name`; no public protobuf field is added for the path or hint.

The array aligns with **column occurrences**, not unique dependencies:

- For expression operators, use column arguments in `expr.args` order; literals occupy no slot.
- For other operators, use `op.inputs` order.
- Repeated columns retain separate slots. Each occurrence of a schema JSON path must carry the same non-`None` hint for that logical name.
- An absent array defaults to `None` for each occurrence. This supports existing scalar plans; JSON paths require explicit hints.
- Array length, enum values, and compatibility with the resolved input are validated. Runtime `$id` and `$score` inputs do not accept type hints.

For example:

```text
column occurrences: [metadata["price"], $score, metadata["price"]]
type hints:         [Double, None, Double]
physical fields:    [metadata.fieldID]
```

`ProtoOpToRepr` clones the params map, decodes the reserved entry into `OperatorRepr.InputDataTypes`, and removes it from the cloned map before operator-specific validation. The request protobuf is not mutated. Positional inputs and deduplicated fetch dependencies remain separate.

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

### JSON-path values and types

A path's declared type fixes its Arrow type before rows are read. The runtime does not infer types from values or reconcile types across segments.

| Declared JSON-path type | Accepted JSON value | Arrow type |
|---|---|---|
| Bool | Boolean | Boolean |
| Int64 | Integer representable as signed Int64 | Int64 |
| Double | Integer or floating-point number accepted by the Float64 converter | Float64 |
| VarChar | String | String |

Every JSON-path column is nullable. Missing paths, out-of-range array indexes, JSON null, nullable roots, incompatible value types, and numbers that cannot be converted to the declared type produce a null for that row. Objects and arrays cannot be path column values. No string-to-number, bool-to-number, or number-to-string coercion is performed.

Int64 overflow produces null. A failed Double conversion produces null; a successful conversion preserves its value, including underflow to zero and signed zero. The projector does not rescan the numeric token to override the converter's decision. Zero-row and all-null inputs retain the declared Arrow type and query chunk layout.

#### JSON reader limitations

L0/L1 use the C++ `milvus::Json::at<T>()` reader; L2 decodes each root once per row through the Go JSON decoder with `UseNumber`, then looks up its paths. C++ reads requested paths without an additional validation pass over unused content. The Go converter decodes one document and does not attempt to decode a second value. These paths do not promise identical validation of all malformed JSON input.

Duplicate object keys retain the current readers' behavior: C++ projection takes the first matching key and Go decoding takes the last. C++ also matches raw key text through the common JSON reader, without an extra unescaping pass for JSON key spellings; this can differ from Go's decoded map keys. Cross-stage equivalence is not guaranteed for these cases. Preserving path tokens across CGO does not remove these reader limitations.

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
- Complete JSON roots and JSON/dynamic paths cannot be `map` outputs; this also excludes `$meta` and `$meta[...]`.
- First-version rerank does not allow writing `$id` or unknown `$xxx` values.

#### `sort`

`sort(by, desc=True, tie_break_col=None)` sorts the current candidate chunk.

- `by` is encoded as an op input and parameter.
- `tie_break_col` is optional and is also encoded as an input.
- Sorting is explicit. Milvus does not infer ordering direction from vector metric type after a chain sort is present.
- Projected Int64, Double, and VarChar paths can be sort keys; projected Bool cannot. Nulls sort last in either direction. Existing stage restrictions still apply, so L0 does not gain a `sort` operator.

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

The server accepts an optional string parameter `null_policy`:

| Value | Behavior |
|---|---|
| `propagate` (default when omitted) | Return null if any input is null. |
| `as_zero` | Treat each null input as zero before applying the selected mode. |
| `skip` | Ignore null inputs and their weights; return null if all inputs are null. |

An explicitly empty, unknown, unset, or non-string `null_policy` is invalid.
This is server-side parameter support; SDK helper support is separate. `skip`
does not guarantee a non-null final score when all inputs are null.

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

For JSON inputs, the execution name is the complete logical path, such as `metadata["price"]`, while the fetched field is the physical root `metadata`. Multiple paths can share one root read. The chain DataFrame contains the requested path columns, without importing the complete JSON root as an ordinary input column.

`$meta` is a physical schema field, and `$meta[...]` is a schema path. Neither belongs to the function-chain system namespace merely because it starts with `$`; `KeepAllSystemColumns` must not retain those paths as system columns. Public runtime inputs remain `$id` and `$score`, while `$seg_offset` and `$l1_source_index` remain internal.

Scalar columns preserve schema FieldID, type, and nullability metadata. JSON-path columns carry their target Milvus type and nullable Arrow type, but **no root FieldID metadata**. Their `SourceFieldID` is used only to fetch stored data. This allows a path input and a search group-by value to share a physical root without suppressing either column. Internal path columns are removed through pruning/result projection; normal `output_fields` can still request the stored JSON root through the existing Search output path.

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

L1 uses the existing `PlanNode.querynode_function_chains` field together with L0 and is distinguished by `FunctionChain.stage`. Typed input materialization adds an in-process CGO protobuf payload. The current implementation does not add projection negotiation fields to component Search RPCs; those belong to the pending mixed-version design below.

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

type OperatorRepr struct {
    Type           string
    Params         map[string]*schemapb.FunctionParamValue
    Function       *FunctionRepr
    Inputs         []string
    InputDataTypes []schemapb.DataType
    Outputs        []string
}

type OperatorReprInfo struct {
    Type       string
    ReadNames  []string
    WriteNames []string
}
```

`OperatorRepr.Inputs` preserves column occurrences, while `ChainRepr.Info.RequiredInputs` deduplicates names read before any previous op writes them. Structural dependency analysis does not resolve schema fields. The caller supplies its collection schema to the shared schema-aware input planner; the execution engine consumes the resulting Arrow columns.

### Shared input plan and materialization

`CompileDataFrameInputPlan` (or `CompileDataFrameInputPlanWithSchemaHelper`) resolves the full chain into:

```go
type ResolvedChainInput struct {
    LogicalName   string
    SourceFieldID int64
    FieldName     string
    DataType      schemapb.DataType
    Nullable      bool
    NestedPath    []string
    DataTypeHint  schemapb.DataType
}

type DataFrameInputPlan struct {
    Inputs []ResolvedChainInput
}
```

`DataType` describes the physical field; for JSON, `DataTypeHint` describes the path's projected type. The plan preserves the first occurrence of each logical input, excludes names produced by earlier operators, validates each JSON-path hint, and rejects conflicting hints for the same logical name. `PhysicalFieldIDs()` and `PhysicalFieldNames()` return deduplicated roots. Consumers can group inputs by `SourceFieldID` without changing operator argument positions.

Proxy and QueryNode pass their schema into this planner. Exact field names are resolved before nested expressions, unknown bare dynamic names are rejected, and explicit `$meta[...]` must resolve to the dynamic JSON field. Scalar hints, when supplied, must be compatible with the schema's Arrow type; JSON-path hints must be one of the four supported types.

| Stage | Data source | Materialization entry point |
|---|---|---|
| L0 | Each segment's prepared search candidates | `ExportSearchResultAsArrowRecordBatchWithInputPlan` |
| L1 | Surviving rows after worker reduce, in source-map order | `FillFieldsOrderedAsArrowRecordBatchWithInputPlan` |
| L2 | Physical root fields in `SearchResultData` after fetch/requery | `FromSearchResultData(result, alloc, inputPlan)` |

For L0/L1, `MarshalFunctionChainInputPlan` serializes the plan to `cgo_msg.proto.FunctionChainInputPlan`. Each input carries source FieldID, logical name, target type, nested-path tokens, and an `is_json_path` flag. This protobuf is an in-process payload, not a client or component RPC message. Go lends the byte buffer to the synchronous C++ call; C++ does not retain Go pointers. L0 serializes once before per-segment parallel export, while L1 serializes before its ordered read. Each C++ call parses its own plan.

C++ reads each required physical root once per segment materialization and projects its logical paths before exporting Arrow. L2 locates roots by FieldID and decodes each root once per row for all its requested paths. `ValidateMaterializedInput` checks the resulting Arrow type and Milvus type metadata; it requires the schema FieldID for scalars and rejects FieldID metadata on JSON paths.

### Proxy L2 input planning

For ordinary Search L2 rerank, Proxy invokes the shared planner with the collection schema:

1. `$score` and `$id` are runtime system inputs.
2. Other names must resolve to supported scalar fields or explicitly typed JSON/dynamic paths.
3. Unknown non-system names are rejected.
4. Unsupported `$xxx` system inputs are rejected.
5. Temporary variables written by previous ops are not fetched from schema.

Supported ordinary scalar input field types:

- Bool
- Int8 / Int16 / Int32 / Int64 / Timestamptz
- Float / Double
- String / VarChar / Text

JSON and dynamic-field paths additionally support the four projected types described above. Vector, Array, Geometry, and complete JSON root inputs remain unsupported.

`functionChainRerankMeta` retains the compiled `DataFrameInputPlan` plus its deduplicated physical field IDs and names. Requery fetches those roots even when they are not requested in `output_fields`. `FromSearchResultData` resolves input `FieldData` by physical FieldID, validates source type and any supplied field name, and adds the planned scalar/path columns. For zero-hit stubs that omit `FieldData`, it constructs typed empty planned columns using the query chunks. Non-empty results with missing or inconsistent sources return an internal contract error.

### QueryNode L0/L1 preparation

Proxy serializes both L0 and L1 chains into the physical plan. Worker QueryNode parses the `planpb.PlanNode` once, converts public chains to `ChainRepr`, validates each chain according to its stage, and compiles separate L0 and L1 `DataFrameInputPlan` values. Legacy `function_score` is also normalized during this preparation pass into an L0 prepared configuration containing its scorers and resolved score-combine modes. The protobuf plan is not retained by L0 or L1 execution; segment-specific boost-score runners are bound only when L0 executes.

Only public L0 input fields are exported with each segment search result before reduction. A prepared legacy boost score needs only the existing segment offsets and does not add collection fields to this export. L1 input fields are materialized after cross-segment reduction for the surviving worker candidates.

L0 retains its segment-local behavior and accepts only `map`; ordinary temporary and collection columns may be written within its segment-local DataFrame. L1 accepts `map`, `sort`, and `limit`; ordinary temporary and collection columns may also be written within the L1 chain. Among public system columns, only `$score` is writable in either stage; `$id` and other `$xxx` system columns are read-only. Both stages use the ordinary scalar and explicitly typed JSON/dynamic-path inputs described above.

### L0 input materialization

After `PrepareSearchResultsForExport`, `exportSearchResultsAsArrow` calls the unified `ExportSearchResultAsArrowRecordBatchWithInputPlan` for each segment. It exports system columns, optional group-by/element columns, and any planned scalar/path inputs as one RecordBatch with per-NQ row counts. An empty input plan exports only system/reduction columns and skips projection; the former FieldID-based Go/C entry point has been removed.

Before returning the segment DataFrames, `exportSearchResultsAsArrow` validates each planned column and checks schema consistency across segments. Typed empty and all-null path columns follow the same schema, without runtime type promotion or cast/rebuild. Parallel export failure releases completed DataFrames; callers release successful results after use. L0 then executes per segment before cross-segment reduction.

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

Scalar and JSON/dynamic-path inputs required only by L1 are not exported with all segment ANN candidates and are not copied during heap merge. Before L1 execution, QueryNode flattens the merged result's source map and reads only the surviving candidates from their source segments:

```text
Sources[query chunk][row].{InputIdx, SegOffset}
  -> FillFieldsOrderedAsArrowRecordBatchWithInputPlan
  -> read unique physical roots and project logical inputs
  -> Arrow RecordBatch in merged-row order
  -> L1 input DataFrame with mergedDF chunk sizes
```

The read API groups requested offsets by segment, performs field subscripts against each segment, and scatters the values back into the exact caller-provided order. `OriginalIdx` remains useful to the reducer for pre-reduce metadata columns but is not part of the L1 field-materialization contract.

`buildL1InputDataFrame` calls the ordered exporter only when `inputPlan.Inputs` is non-empty. A chain using only `$id` and `$score` reuses the reduced columns without reading extra fields. The exporter returns only the planned scalar/path columns; the builder combines them with the reduced system columns and restores the reduced DataFrame's chunk sizes.

Materialization preserves row order, null values, chunk count, and row count. Scalar columns retain FieldID/type/nullability metadata; JSON-path columns are nullable, carry the target type, and omit the root FieldID. Missing fields, invalid source indexes or offsets, malformed Arrow metadata, or mismatched DataFrame/source shapes indicate an internal result-contract failure rather than invalid request content.

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

- In hybrid search, L1 belongs to an individual sub-search; top-level chains must use L2.
- L1 is not supported with Search Iterator (legacy or v2).
- L1 is not supported with `order_by`, because both define ordering behavior.
- L1 is not supported with search aggregation.
- Search-level group-by may be combined with L1 `map`, `sort`, or `limit`, matching L2 compatibility. Function Chain operators may reorder or trim grouped rows without rebuilding the original group count or `group_size` contract.

These validations occur before execution so an unsupported combination does not silently produce incomplete distributed results.

#### Hybrid search

The server implements the extension described in [Hybrid Search Function Chain Integration](20260818-hybrid-search-function-chain.md).

Hybrid search accepts L0/L1 chains on individual sub-searches and exactly one top-level L2 chain. The L2 chain must begin with its only `merge` operator; merge parameters are validated against the sub-search count. Ordinary Search rejects `merge`.

The top-level chain cannot be combined with `function_score` or explicit legacy rank strategy/params. A `function_score` also conflicts with sub-search chains. With no top-level chain, existing legacy rerank selection is preserved. Scalar and typed JSON/dynamic inputs use the shared input planner in both ordinary and hybrid search.

### Requery and field availability

No new fetch mechanism is required. Function-chain input fields flow through existing rerank metadata:

```text
rerankMeta.GetInputFieldNames()
rerankMeta.GetInputFieldIDs()
```

When requery is needed, the requery operator includes rerank-required physical field names, including unique JSON roots. The compiled input plan maps these roots to logical path columns before rerank execution. Final projection still uses user output fields and does not expose internally fetched rerank inputs.

### Rerank operator integration

`rerankOperator` follows the existing function-score flow:

```text
SearchResultData
  -> chain.FromSearchResultData(result, alloc, rerankMeta.GetInputPlan())
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

QueryNode L0 chains and internally generated boost-score chains use the existing `defaultAllocator`, which defaults to Arrow Go's `memory.DefaultAllocator`. Proxy rerank chains also use `memory.DefaultAllocator`. PyUDF exchanges serialized Arrow IPC streams over gRPC and does not require C-allocated buffers.

Imported segment DataFrames retain the ownership supplied by the C++ Arrow exporter. Normal Arrow array, chunked-array, and DataFrame release chains remain required. String values exported into search results are copied into Go-owned storage so they remain valid after the source DataFrame is released.

### Tail behavior

A public `FunctionChain` is executed in its declared operator order. Ordinary Search does not implicitly append public operators such as `limit`, group-by, or round-decimal. For Hybrid Search, an explicit L2 `limit` owns pagination; otherwise the Hybrid request's limit and offset apply after the chain, as specified by the Hybrid integration design.

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
11. Non-system required inputs must be supported scalar fields or explicitly typed JSON/dynamic paths.
12. L0 accepts only `map`; L1 accepts only `map`, `sort`, and `limit`.
13. Unknown operators and functions are rejected.
14. A function must be runnable at the chain stage.
15. Function-specific parameters must pass validation.
16. External rerank model query count must match query chunk count.
17. L1 is rejected with search aggregation.
18. Function rerank is rejected with Search Iterator (legacy or v2) and `order_by`.
19. JSON paths require supported, non-conflicting hints at every schema-input occurrence; `$input_data_types` must align with the column occurrences.
20. Complete JSON roots, implicit bare dynamic names, and nested paths on non-JSON fields are rejected as inputs.
21. Complete JSON roots and JSON/dynamic paths cannot be outputs. `$meta[...]` is a schema path, not a public system input.

Additional ordering constraints such as "at most one sort" or "sort must be last" can be considered as future stricter validation. The first release executes the user's ordered plan as sent unless an operator rejects it, then applies only the internal L0/L1 reducer normalization described above.

### Projection error semantics

| Condition | Behavior |
|---|---|
| Invalid path/hint, conflicting hints, malformed type-hint array, or prohibited root/path input/output | Request input error through planning/validation; projection contract checks use `ParameterInvalid` |
| Missing/null path, incompatible value, or numeric conversion failure | Append a typed Arrow null for that row |
| C++ path reader reports a recognized persisted-JSON format error | `DataFormatBroken`, translated to `DataIntegrity` at the Function Chain Go boundary |
| Go root-document decoding fails | `DataIntegrity` |
| C++ JSON reader reports allocation/capacity/depth failure | Preserve its resource error category; do not relabel it as data corruption |
| Malformed CGO input-plan protobuf or invalid internal plan | Internal error, not persisted-JSON corruption |
| CGO input-plan protobuf serialization fails | `SerializationFailed` |
| Missing/inconsistent materialized fields, metadata, chunk shape, or provenance | Internal result-contract error |
| Expression cannot operate on its runtime Arrow inputs | Existing expression/`FunctionFailed` error path |

C++ `NUMBER_ERROR` and numeric range failures produce null rather than being reclassified by rescanning number text. Exceptions thrown directly by the common JSON reader keep their existing codes. Existing typed errors are wrapped with `merr.Wrap/Wrapf` for context, except for the explicit corruption-to-`DataIntegrity` boundary mapping. New projection diagnostics do not add raw JSON values; this does not promise redaction of messages from existing shared parsers.

## Compatibility, Deprecation, and Migration Plan

### Compatibility

- Existing search requests without `function_chains` are unchanged.
- Existing `function_score` and legacy rank behavior remain supported.
- Public function chains are opt-in.
- Existing result schema is preserved; final score is exposed through current score/distance fields.
- The public Function Chain protobuf is unchanged for JSON paths. Existing requests without `$input_data_types` can still use ordinary scalar fields; schema JSON paths require explicit hints.
- The current server implementation does not negotiate JSON projection capability with older QueryNodes. Rolling-upgrade execution safety for L0/L1 JSON paths has not been established; the proposed gate below is pending.
- JSON inputs are not implicitly converted to whole roots, bare dynamic names, or inferred runtime types.

### Deprecation

No deprecation is introduced in this MEP.

### Migration

Users can migrate from `function_score` or ranker APIs to `function_chains` when they need explicit ordered composition. There is no automatic conversion in the first release.

### Future work: mixed-version projection negotiation

This protocol is **proposed, not implemented or verified**. It would protect L0/L1 JSON/dynamic paths that require the QueryNode projector. Scalar-only L0/L1 and L2-only JSON projection would not require this capability.

The proposal adds `required_function_chain_projection_version` to internal `SearchRequest` and `executed_function_chain_projection_version` to internal `SearchResults`; field numbers have not been assigned. Version 0 means no requirement/no acknowledgement, and v1 represents the occurrence, resolver, typed-null, metadata, and root-cleanup contract. These fields do not exist in the current internal Search messages.

The intended flow is:

1. Proxy validates paths and hints with the schema-aware planner, then marks L0/L1 JSON requests as requiring v1.
2. Every request copy and shard-leader fan-out preserves the requirement.
3. A worker rejects an unsupported version or a JSON plan without the required negotiation. Successful responses acknowledge execution, including zero-hit, empty-segment, and two-stage paths.
4. The leader verifies every participating worker acknowledgement before combining results. Its acknowledgement covers all participating workers; RPC fallback cannot bypass the check.
5. Proxy verifies the leader acknowledgement before accepting its result. Missing or insufficient acknowledgements fail the request.

Capability mismatch would use system-class `ErrServiceUnimplemented`. Existing worker failures retain their original errors, and invalid user plans must be rejected before capability checks. An old participant returning apparent success without an acknowledgement would not be accepted. This is an execution-tree check, not a cluster-wide minimum-version rule, and no silent fallback is proposed.

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
8. Validate per-sub-search L0/L1 and top-level L2 chains for hybrid search.
9. For the pending SDK extension, serialize typed JSON/dynamic references through `$input_data_types`; preserve duplicate column occurrences, skip literals, and reject invalid hints before sending the request.

### Proxy and chain planning tests

1. `function_score` plus `function_chains` is rejected.
2. Duplicate chains at each stage are rejected.
3. L0, L1, and L2 route to the correct execution component and may coexist.
4. Unsupported stages and invalid hybrid merge placement/count are rejected.
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
3. Scalar and JSON/dynamic-path inputs, including nullable roots and all-null paths, are materialized from the source segment rows.
4. Int64 and string PK results preserve source identity.
5. Element-level results with duplicate PKs preserve distinct source rows.
6. User sort determines the candidates selected by user limit.
7. Internal score-descending/ID-ascending normalization runs after the user chain.
8. Ragged TopKs and empty query chunks retain valid DataFrame/source shapes.
9. Missing fields, Arrow type mismatches, invalid source indexes, and malformed provenance tokens return typed internal errors.
10. The hidden provenance column and L1-only inputs are absent from serialized result fields.
11. Success and failure paths record the L1 latency metric exactly once and release Arrow resources.

### JSON/dynamic projection tests

The shared semantic matrix covers Bool, Int64, Double, and VarChar; object keys and array indexes; missing/null/incompatible values; signed integer bounds; Double overflow, underflow, and signed zero; zero rows and all-null columns; and repeated paths sharing one physical root. Reader-specific tests document the duplicate-key and escaped-key limitations rather than assuming equivalence for those cases.

Planning tests cover occurrence ordering, literals and repeated operands, params-map immutability, conflicting/missing hints, explicit `$meta[...]`, rejected whole roots and outputs, and dependencies produced by earlier operators.

Stage tests cover:

- L0 per-segment schema consistency, empty input-plan system columns, typed empty results, and release of completed exports on parallel failure.
- L1 interleaved segment reads in caller order, type/FieldID/nullability metadata, source-map reconstruction, and GroupBy with Sort/Limit.
- L2 physical-root requery, typed logical columns, GroupBy sharing a root FieldID, and removal of internal paths from exported results.
- Cancellation, malformed CGO plans, persisted-data format failures, and resource failures, with error categories and Arrow ownership checked at the consumer.

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

JSON/dynamic Python cases additionally target L0/L1 multi-segment searches, zero hits, all-null and mismatched values, nested paths, shared-root GroupBy, stored JSON output preservation, and combinations of L0/L1/L2. These are required end-to-end checks; their presence in the test source does not establish that they have passed.

### Future mixed-version tests

After the proposed gate is implemented, verify new Proxy/old leader, new leader/old worker, old Proxy/new worker, all-new participants, higher unsupported requirements, and scalar-only/L2-only requests. Include zero-hit, no-segment, local-worker, RPC fallback, request-copy, and response-aggregation paths. Check acknowledgements before result consumption, and preserve pre-existing worker failures. This matrix is currently a test plan, not verified behavior.

### Regression checks

Run targeted Go tests with Milvus test flags:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/chain/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/segcore/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run 'FunctionChain|Rerank|L1'
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querynodev2/tasks/... -run 'FunctionChain|L0|L1|GoReduce|ExportSearch'
```

Because L1 changes distributed ordering, provenance, and late materialization, verification also includes the full Go test suite and end-to-end failure-mode tracing. A green success-path search alone is not evidence that source alignment or worker-local limit semantics are correct.

Run SDK tests from the PyMilvus repository or local checkout as appropriate.

### Implementation and verification status

Snapshot from the 2026-09-10 implementation review; this is not a release declaration:

| Area | Status |
|---|---|
| Occurrence-aligned hints, shared input planning, L0/L1 C++ projection, and L2 Go projection | Implemented in the current server code |
| Unified L0 Go/C exporter, including empty input plans | Implemented; the former FieldID-based export entry point is removed |
| PyMilvus `ColumnRef.data_type` / `col(..., data_type=...)` and SDK preflight | Pending in the inspected SDK checkout |
| Internal Search projection-version handshake | Proposed; no wire fields or execution checks implemented |
| C++ `SearchResultExport.*` | 52 tests passed using freshly compiled export code and test objects with existing dependencies |
| Go function-chain, segcore, and merr suites; relevant QueryNode export/Go-reduce/L0/L1 regressions | Passed with the repository-required test flags |
| Full Go suite | Attempted with the rebuilt core library; Proxy telemetry reply authentication test failed (HTTP 500 instead of 200), and remaining execution was stopped |
| JSON/dynamic Python end-to-end coverage and mixed-version behavior | Not verified in this run; the mixed-version protocol is still pending |

The implementation entry points are [input planning](../../../internal/util/function/chain/input_plan.go), [proto representation](../../../internal/util/function/chain/repr.go), [L2 projection](../../../internal/util/function/chain/json_projector.go), [Go/CGO export](../../../internal/util/segcore/search_result_arrow.go), [C++ export](../../../internal/core/src/segcore/search_result_export_c.cpp), and [L1 materialization](../../../internal/querynodev2/tasks/l1_function_chain.go).

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

### 4. Resolve schema inputs inside the generic execution engine

Rejected because schema resolution requires the caller's collection schema and input namespace. Structural dependency analysis and generic operator execution remain schema-independent. The shared `CompileDataFrameInputPlan` helper in the chain package is explicitly schema-aware and is invoked by Proxy or QueryNode with that context; it centralizes resolution without making the execution engine infer field meanings.

## Open Questions

1. How should future hybrid operators extend the current per-sub-search L0/L1 and top-level L2 API?
2. Which additional functions and operators should be allowed in future L0/L1 stages?
3. Should L1 eventually support a shard-leader execution mode in addition to worker post-reduce execution?
4. Should strict operator ordering rules be enforced, such as one `sort` and only as the last ordering op?
5. Should users be able to return intermediate variables explicitly in future APIs?
6. Should provider-specific metrics be standardized across embedding and rerank model providers?
