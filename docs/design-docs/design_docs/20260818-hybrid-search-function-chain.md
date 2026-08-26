# Hybrid Search Function Chain Integration

## Status

Top-level Hybrid L2 and per-sub-search L0/L1 Milvus server integrations are
implemented in this change. In-repository Go, REST, and Python coverage has
been added. SDK delivery remains separate: top-level Hybrid exposure is in
progress, while public `AnnSearchRequest.function_chains`, dependency landing,
and full regression verification remain planned.

## Summary

This document extends the public Function Chain API to top-level Hybrid Search. A Hybrid Search request may continue to use the legacy ranker API, use the typed `function_score` rerank API, or provide a public L2 Function Chain that fully defines candidate fusion and subsequent reranking. A public Function Chain is mutually exclusive with both existing rerank sources.

Each Hybrid sub-search may additionally provide its own L0 and/or L1 Function
Chain through the existing `SearchRequest.function_chains` field. Those chains
execute independently inside that sub-search before its reduced result reaches
the top-level Hybrid reranker. A request may use per-sub-search L0/L1 with a
legacy ranker or a top-level L2 Function Chain, but not with top-level typed
`function_score`.

A Hybrid Search Function Chain starts with the declarative `merge` operator. `merge` consumes the reduced result DataFrames from all sub-searches, deduplicates candidates, computes the initial fused `$score`, and emits one DataFrame for the remaining operators. The first release supports the existing public L2 operators `map`, `sort`, and `limit` after `merge`.

This is an additive extension to [Function Chain API](20260624-function-chain-api.md). The existing document describes the first release, in which Hybrid Search rejects `function_chains`; this document supersedes that restriction. The Merge foundation, Milvus server integration, and core in-repository test coverage are implemented by this change. Public SDK delivery and complete regression coverage are tracked as follow-up work below.

## Related Designs

- [Function Chain API](20260624-function-chain-api.md)
- [Struct Hybrid Search](20260602-struct_hybrid_search.md)

## Motivation

Hybrid Search currently selects one of two server-built rerank paths:

- a legacy ranker serialized through `rank_params`; or
- a typed `function_score` reranker.

Both paths eventually build an internal Function Chain whose first operator is a `MergeOp`. The standalone Merge refactor has already provided:

- declarative `merge` construction through the operator registry;
- runtime metric injection through `FunctionBuildContext.Search`;
- native row identity through `$id`;
- native element identity through `($id, $element_indices)`; and
- one Merge implementation shared by legacy and declarative construction.

Before this change, Hybrid Function Chains were unavailable because:

1. Hybrid Search currently rejects top-level `function_chains`.
2. `convertHybridSearchToSearch` does not propagate the existing top-level `function_chains` field.
3. Hybrid-specific chain structure, rerank-source selection, and final-output ownership are not implemented.
4. SDK and REST Hybrid entry points rejected or could not serialize public Function Chains.

Per-sub-search L0/L1 has an additional transport gap. Public Hybrid sub-requests
are full `milvuspb.SearchRequest` messages and therefore already contain
`function_chains`, but `convertHybridSearchToSearch` converts them into the
compact `milvuspb.SubSearchRequest` representation without copying that field.
The compact public `SubSearchRequest` message did not have a Function Chain
field. As a result, a raw gRPC caller could send nested L0/L1 chains and have
them silently discarded before plan generation. This change adds the field to
the compact public request; the internal QueryNode request continues to carry
the validated chain only through its serialized `PlanNode`.

The top-level L2 API must replace the entire ranker path rather than execute
after an independently selected ranker. Otherwise two independent components
would own candidate fusion, score semantics, ordering, and truncation. Nested
L0/L1 is different: it belongs to one recall source and intentionally executes
before whichever top-level rerank source is selected.

## Goals

- Support top-level `HybridSearchRequest.function_chains` execution.
- Support independent L0/L1 execution from
  `HybridSearchRequest.requests[i].function_chains`.
- Preserve sub-search chains in the corresponding compact sub-request until
  each sub-search plan has been serialized.
- Preserve the existing ranker API and behavior.
- Make top-level `function_chains` mutually exclusive with legacy ranker configuration and typed `function_score` rerank.
- Require a declarative `merge` operator as the first operator of a Hybrid L2 chain.
- Support RRF, weighted, max, sum, and average fusion.
- Allow the current public L2 Function Chain operators and expressions after `merge`.
- Support both row-level and same-struct element-level Hybrid Search.
- Keep real primary keys visible through `$id` throughout chain execution.
- Derive metric information from executed sub-searches rather than trusting client input.
- Reuse one `MergeOp` implementation for legacy rankers and public Function Chains.
- Let the public Function Chain fully own final ordering, score transformation, offset, and result count.
- Preserve request-level `group_by` in every ANN sub-search while allowing the user chain to reshape the merged result.
- Validate each nested chain's stage structure in Proxy, preserve the accepted
  L0/L1 chains in its sub-search plan, and let QueryNode perform the existing
  semantic validation before executing that chain.

## Non-Goals

- Applying or inheriting the top-level chain independently to each sub-search
  before reduction.
- Adding arbitrary user-configurable candidate-key columns.
- Changing the ranking formulas of existing legacy RRF or weighted rankers.
- Pushing the top-level Hybrid merge into QueryNode or Segcore.
- Supporting L0 or L1 top-level Hybrid fusion in this change.
- Supporting L2 or `merge` inside a Hybrid sub-search.
- Sharing temporary columns produced by one sub-search chain with another
  sub-search or with the top-level L2 chain.
- Guaranteeing that the final Function Chain output still satisfies the request-level ANN `group_by` constraint.
- Appending a second Proxy-side GroupBy operation after a public Function Chain.
- Making `filter` or `group_by` new public Function Chain SDK operators in this change.
- Removing or changing the old ranker API.
- Changing the existing precedence of a request that already combines legacy rank parameters with `function_score`.
- Exposing internal metric metadata, candidate identity configuration, or score-direction controls in the public protobuf.

## API Contract

### Existing protobuf fields

`HybridSearchRequest` already contains:

```proto
// Function chains for top-level hybrid fusion/rerank.
repeated schema.FunctionChain function_chains = 15;
```

No new request field is required. Proxy must propagate this field when converting a Hybrid request into its internal `SearchRequest`.

Every element of `HybridSearchRequest.requests` is already a full
`milvuspb.SearchRequest`, which contains:

```proto
// Function chains for this individual search request.
repeated schema.FunctionChain function_chains = 24;
```

The compact request produced by Proxy gains the corresponding field:

```proto
repeated schema.FunctionChain function_chains = 8;
```

The nested field and the top-level field have different ownership:

```text
HybridSearchRequest.requests[i].function_chains  L0/L1 for sub-search i
HybridSearchRequest.function_chains              L2 for cross-sub-search fusion
```

Neither field is inherited, copied into the other scope, or used as a fallback
for the other scope.

### Per-sub-search API

SDKs should expose L0/L1 on `AnnSearchRequest`, while the top-level Hybrid call keeps
the L2 chain or legacy ranker:

```python
dense_req = AnnSearchRequest(
    data=[dense_vector],
    anns_field="dense",
    param={"metric_type": "COSINE", "params": {}},
    limit=100,
    function_chains=[dense_l0, dense_l1],
)

sparse_req = AnnSearchRequest(
    data=[sparse_vector],
    anns_field="sparse",
    param={"metric_type": "IP", "params": {}},
    limit=100,
    function_chains=[sparse_l1],
)

client.hybrid_search(
    collection_name="articles",
    reqs=[dense_req, sparse_req],
    ranker=None,
    function_chains=hybrid_l2_chain,
)
```

Sibling sub-searches may have different chains, or no chain. The same nested
shape is used by REST:

```json
{
  "collectionName": "articles",
  "search": [
    {
      "data": [[0.1, 0.2]],
      "annsField": "dense",
      "limit": 100,
      "functionChains": [
        {"stage": "FunctionChainStageL0Rerank", "ops": []},
        {"stage": "FunctionChainStageL1Rerank", "ops": []}
      ]
    }
  ],
  "functionChains": [
    {"stage": "FunctionChainStageL2Rerank", "ops": []}
  ]
}
```

The abbreviated empty `ops` arrays above show placement only; executable chains
must contain valid operators.

### PyMilvus API

PyMilvus adds a typed `merge` builder and makes the existing Hybrid ranker argument optional when `function_chains` is supplied:

```python
chain = (
    FunctionChain(FunctionChainStage.L2_RERANK, name="hybrid_rerank")
    .merge(
        strategy="weighted",
        weights=[0.7, 0.3],
        norm_score=True,
    )
    .map(
        "$score",
        fn.num_combine(
            col("$score"),
            col("freshness"),
            mode="weighted",
            weights=[0.8, 0.2],
        ),
    )
    .sort(col("$score"), desc=True, tie_break_col=col("$id"))
)

client.hybrid_search(
    collection_name="articles",
    reqs=[dense_req, sparse_req],
    ranker=None,
    function_chains=chain,
    limit=20,
    offset=0,
)
```

The SDK accepts either one `FunctionChain` or a one-element list for consistency with ordinary Search. More than one Hybrid chain is rejected.

### Merge representation

`merge` is serialized as a non-expression operator with implicit multi-DataFrame input:

```proto
FunctionChainOp {
  op: "merge"
  params: {
    "strategy": <"rrf" | "weighted" | "max" | "sum" | "avg">,
    // strategy-specific typed parameters
  }
}
```

The allowed parameters are:

| Strategy | Parameters |
|---|---|
| `rrf` | optional `k`, default `60` |
| `weighted` | required `weights`, optional `norm_score` |
| `max` / `sum` / `avg` | optional `norm_score` |

`merge` rejects an expression, explicit inputs, explicit outputs, unknown parameters, and a `weights` count that differs from the number of executed sub-searches. SDKs should serialize defaults explicitly where SDK compatibility requires them; for example, PyMilvus `WeightedRanker` currently defaults `norm_score` to `true`.

### Rerank source selection

Hybrid Search has three possible explicit rerank sources:

- legacy ranker configuration serialized as `strategy` and `params` in `rank_params`;
- typed rerank configuration serialized as `HybridSearchRequest.function_score`; and
- a declarative plan serialized as `HybridSearchRequest.function_chains`.

`function_chains` cannot be combined with either existing source. SDKs reject these combinations before RPC, and the server repeats the validation at the protobuf boundary. This feature does not change the historical handling of requests that already combine legacy rank parameters with `function_score`.

Top-level `HybridSearchRequest.function_chains` remains mutually exclusive with
the other top-level rerank sources. Nested L0/L1 chains may be combined with a
top-level legacy ranker or L2 Function Chain, but not with top-level
`function_score`. This is a request-level rule regardless of which functions it
contains. In particular, a typed `function_score` may add Boost scorers to each
sub-search plan, and QueryNode requires those scorers to be mutually exclusive
with `querynode_function_chains`.

This change defines transport and execution only for nested
`function_chains`. It does not add nested `function_score` to the compact
`SubSearchRequest`, change its historical handling, or add a new conflict check
between nested `function_score` and `function_chains`. SDKs should not combine
the two in one `AnnSearchRequest` until that contract is defined separately.

Conceptually, validation is:

```go
legacyRanker := hasRankParam("strategy") || hasRankParam("params")
typedRerank := request.GetFunctionScore() != nil
functionChain := len(request.GetFunctionChains()) > 0
nestedFunctionChain := hasNestedFunctionChain(request.GetSubReqs())

if functionChain && (legacyRanker || typedRerank) {
    return merr.WrapErrParameterInvalidMsg(
        "function_chains cannot be used with rank_params reranker or function_score",
    )
}
if typedRerank && nestedFunctionChain {
    return merr.WrapErrParameterInvalidMsg(
        "function_score cannot be used with nested function_chains",
    )
}
```

The server applies these rules:

| Request state | Result |
|---|---|
| no explicit rerank source | preserve compatibility by selecting the default legacy RRF ranker |
| explicit legacy `strategy`/`params` only | execute the legacy ranker |
| `function_score` only | execute the typed rerank path |
| `function_chains` only | execute the public chain |
| explicit legacy `strategy`/`params` and `function_score` | preserve existing behavior in this change |
| `function_chains` and `function_score` | reject as `ParameterInvalid` |
| `function_chains` and explicit legacy `strategy` or `params` | reject as `ParameterInvalid` |

Nested L0/L1 compatibility is narrower: legacy rankers and top-level L2
Function Chains accept nested chains, while top-level `function_score` rejects
them as `ParameterInvalid` before Proxy generates any sub-search plan.

`rank_params` as a whole is not mutually exclusive with `function_chains`. Existing clients and REST conversion may still send `limit`, `offset`, and `round_decimal`, but the public Function Chain path does not apply them to the final result. Users express final slicing and score rounding explicitly in the chain. Grouping keys remain valid ANN recall controls with the final-result limitation described below. Only meaningful legacy `strategy` and `params` values select a conflicting rerank source.

When `function_chains` is selected, Proxy must not construct the implicit default legacy RRF ranker. The REST handler preserves its existing `rerank` conversion and may serialize `strategy=""` and `params=null` for a chain-only request. Proxy treats only those empty placeholders as absent; meaningful legacy values combined with a chain remain invalid.

### Supported stage and cardinality

Top-level Hybrid Search accepts exactly one chain with stage `FunctionChainStageL2Rerank`.

The chain must:

- contain at least one operator;
- contain exactly one `merge` operator; and
- place `merge` at operator index zero.

L0 and L1 top-level Hybrid chains remain unsupported by this design. Multiple L2 chains remain invalid; callers compose multiple rerank steps as operators in one chain.

Ordinary Search rejects `merge`. Although the runtime can execute Merge with one DataFrame, the ordinary Search API defines a single recall source and must not accidentally expose a meaningless single-input fusion contract.

Each Hybrid sub-search accepts at most one L0 chain and at most one L1 chain.
It rejects L2, `merge`, duplicate stages, nil chains, and empty chains. Chain
order in the request is not execution order: L0 always executes before the
cross-segment reduction, and L1 always executes after that reduction. For
clarity, SDK builders should serialize L0 before L1.

The complete scope matrix is:

| Location | L0 | L1 | L2 | `merge` |
|---|---:|---:|---:|---:|
| `requests[i].function_chains` | at most one | at most one | reject | reject |
| top-level `function_chains` | reject | reject | exactly one | first and exactly one |

Per-sub-search L0/L1 may be combined with a top-level legacy ranker or L2
Function Chain. They cannot be combined with top-level `function_score` because
the latter can populate Boost scorers in the same QueryNode plan. Nested
`function_score` behavior is not changed by this extension; this change neither
implements new handling nor adds a new rejection for it.

The `xgboost` map expression is runnable at all three rerank stages. L0 and L1
execute it in QueryNode, while L2 executes it in Proxy. In a top-level Hybrid
chain, XGBoost appears after the required `merge` operator and consumes fields
planned by the L2 requery path:

| Expression | Nested L0 | Nested L1 | Top-level L2 |
|---|---:|---:|---:|
| `xgboost` | supported | supported | supported after `merge` |

Both Proxy and QueryNode receive FileResource synchronization and resolve the
configured XGBoost model in the process that executes the stage.

### Unsupported combinations in the first release

- Hybrid `function_chains` with `order_by`;
- Hybrid search iterator or search aggregation, which are already unsupported by Hybrid Search; and
- L0 or L1 top-level Hybrid Function Chains.

## Existing Merge Foundation

The standalone Merge refactor is complete. Hybrid integration relies on the following existing contract:

- declarative `op: "merge"` construction from typed parameters;
- `merge` is a non-expression operator with implicit multi-DataFrame input;
- real sub-search metric types are supplied through `FunctionBuildContext.Search`;
- row candidates use fixed `$id` identity;
- element candidates use fixed `($id, $element_indices)` identity;
- the real primary key remains visible as `$id` after fusion; and
- legacy programmatic Merge construction remains compatible;
- DataFrame conversion round-trips `$element_indices` through the dedicated `SearchResultData.element_indices` wire field.

This integration does not redefine Merge parameter parsing, score formulas, Arrow ownership, internal candidate keys, or defensive DataFrame validation. Those remain owned by the Merge implementation and its existing unit tests.

## End-to-End Proxy Flow

```text
HybridSearchRequest
  -> convertHybridSearchToSearch
       preserves top-level function_chains
       copies requests[i].function_chains to SubReqs[i].function_chains
  -> select and validate the top-level rerank source
       for public L2: ProtoChainToRepr
       exactly one chain with first/unique merge
       plan downstream required scalar fields
  -> for each sub-search i
       tryGeneratePlan(functionRerank = nestedChains[i] is not empty)
       attach nested chains to PlanNode[i].querynode_function_chains
       serialize PlanNode[i]
  -> QueryNode executes each sub-search independently
       ANN
       -> for non-empty results, validates PlanNode[i].querynode_function_chains
       -> L0 per segment
       -> cross-segment reduction
       -> L1 per worker
       -> encode sub-search result
  -> Proxy reduces every sub-search result
  -> infer row/element scope and collapse when required
  -> convert each reduced result to DataFrame
       imports $id, $score, optional $element_indices
  -> build FunctionBuildContext.Search from actual metrics
  -> build the selected top-level chain
       public L2 uses FuncChainFromReprWithContext
       legacy/typed paths retain their existing builders
  -> ExecuteWithOptions(dataframes...)
       merge -> remaining user operators
  -> assemble fields or requery
  -> apply Search-owned projection
  -> SearchResults
```

The Function Chain path replaces the ranker branch at metadata selection time. It is not appended after `BuildRerankChainWithLegacy` or `BuildRerankChain`.

### Preserving nested chains through conversion

`milvuspb.SubSearchRequest` gains a `function_chains` field. Conversion copies
`HybridSearchRequest.requests[i].function_chains` directly into
`SearchRequest.sub_reqs[i].function_chains`. The chain therefore remains owned
by the matching sub-request without a parallel index-aligned sidecar.

`internalpb.SubSearchRequest` does not gain another chain field. Proxy serializes
the compact request's structurally validated L0/L1 chains into the matching
`PlanNode.querynode_function_chains`; QueryNode receives and semantically
validates the execution contract through that plan.

### Per-sub-search plan generation

For each `SubReqs[i]`, Proxy first splits `SubReqs[i].function_chains` by stage
and rejects nil chains, duplicate stages, unknown stages, and nested L2. It then
assigns the accepted L0/L1 chains to `plan.QuerynodeFunctionChains` before
`marshalPlanWithMembershipFilterSizeLimit`. Proxy does not perform operator,
function, field, or Arrow-type semantic validation for nested chains.
No nested chain is assigned to `t.rerankMeta`, because `rerankMeta` owns only
Proxy-side L2 execution and its requery inputs.

`tryGeneratePlan` currently decides whether ANN-only optimization is safe by
calling `hasFunctionRerank(t.request)`, which observes only the converted
top-level request. It must instead receive an explicit per-plan
`hasFunctionRerank` value:

```text
ordinary Search plan  top-level function_score or any top-level chain
Hybrid sub-plan i     nested L0/L1 exists for sub-search i
```

Only affected sub-plans use `SearchType_DEFAULT`; sibling sub-searches without a
chain may retain their existing optimization classification. This avoids both
running an unsafe optimization for a chained sub-search and unnecessarily
disabling optimization for every sibling.

### Validation before chain execution

QueryNode validates each nested chain from the serialized plan before executing
that chain. The existing L0/L1 validation covers:

- protobuf-to-representation conversion;
- allowed operators and function/stage compatibility;
- system-column read/write rules; and
- required collection-field resolution and supported Arrow types.

QueryNode remains the authoritative semantic validation boundary. Its
preparation also turns validated field names into field IDs and runtime state.

Proxy validates nested chain stage structure before generating that sub-search's
plan and before dispatch, but does not semantically validate accepted L0/L1
chains before dispatch.
QueryNode normally prepares and validates their semantics after Segcore has
returned the per-segment result slice and before Arrow export or chain
execution. The existing no-result shortcut is intentionally earlier: when
`len(results) == 0`, QueryNode returns an encoded empty result without preparing,
semantically validating, or executing the nested chain. A stage-valid but
semantically invalid nested chain may therefore remain unobserved on that path.
If the result slice is present but contains zero candidate rows, QueryNode still
reaches chain preparation and semantic validation.

Top-level L2 validation is different: Proxy validates its stage, cardinality,
Merge placement, and representation before the final Hybrid result is known,
so the nested no-result shortcut does not waive top-level validation.

### Stage and candidate semantics

For sub-search `i`, the score consumed by the top-level Hybrid reranker is the
score after that sub-search's L0 and L1 stages:

```text
ANN score_i -> L0_i -> reduce_i -> L1_i -> Proxy sub-result_i -> top-level fusion
```

L0 is segment-local. L1 sees the QueryNode worker's cross-segment reduced
candidates; an L1 `limit` therefore changes that sub-search's candidate budget
before Proxy-level distributed reduction and top-level fusion. Existing
sub-search `topK` and maximum-result-window checks remain the upper resource
bound, while a chain may further shrink the candidate list.

Temporary columns are stage-local. L0-to-L1 follows the ordinary Search column
liveness contract, but sibling sub-searches cannot observe each other's columns,
and top-level Merge receives only the materialized candidate identity, final
sub-score, and fields required by the existing Hybrid pipeline. A nested chain
cannot create an arbitrary column for direct consumption by top-level L2.

## Function Chain Output Ownership

Public `function_chains` execute exactly as declared. Proxy does not append an implicit Sort, Limit, Offset, or RoundDecimal operation. The chain therefore fully owns final ordering, score transformation, and returned row count.

Hybrid `rank_params.limit`, `offset`, and `round_decimal` continue to be parsed for wire compatibility and are not treated as selection of the legacy ranker, but they do not alter public-chain output. A client that needs final pagination must use a `limit` operator with its `offset` parameter; score rounding is expressed with a `map` operator using the `round_decimal` expression. Without a chain-level Limit, the final response may contain all candidates emitted by Merge and the downstream operators.

Legacy rerank and typed `function_score` paths retain their existing server-built Sort/GroupBy, Limit, and RoundDecimal tail. The ownership change applies only when public `function_chains` selects the rerank path.

Request-level `group_by` remains an ANN recall control. Proxy copies its field and group-size settings into every Hybrid sub-search, so each ANN result is grouped before it reaches Merge. The public Function Chain then operates on those grouped candidate lists and may reorder, filter, or truncate them. Consequently, the final response is not guaranteed to continue satisfying the original group cardinality or ordering constraint.

The Function Chain path does not append a second Proxy-side GroupBy operation. This avoids overriding an explicit user sort or introducing two owners for final ordering. Typed `function_score` and legacy rerank paths retain their existing server-built GroupBy tail and therefore keep their existing final-grouping behavior.

Per-sub-search L0/L1 runs inside the existing ANN/group-aware QueryNode path.
L0 executes before group-aware cross-segment reduction, and L1 executes after
that worker-local reduction. An L1 chain may reorder or limit the grouped rows,
so—as with the public L2 chain—the final Hybrid response does not promise that
the original grouping invariant is preserved. The request remains valid; the
ANN stage still uses the configured group-by behavior.

### Struct element-level ordering

Per-sub-search chains do not change the candidate-scope inference from
[Struct Hybrid Search](20260602-struct_hybrid_search.md). The order is:

```text
1. Each sub-search executes ANN -> L0 -> QueryNode reduce -> L1.
2. Proxy completes distributed reduction for each sub-search.
3. Proxy infers common row/element scope.
4. When row scope is required, Proxy collapses each element-level sub-result.
5. The selected top-level reranker fuses the resulting candidate lists.
```

L0/L1 therefore operate on the native candidate scope of their own sub-search.
For element-level searches, the existing QueryNode provenance and element-index
alignment must survive any L1 reorder or limit. Collapse remains a Proxy-side
operation after the sub-search result is complete; this design does not move it
into the nested chain.

## Required-Field Planning

The first Merge operator contributes no schema dependencies to `ChainReprInfo.RequiredInputs`. Its fixed `$id` and `$score` inputs are runtime-owned system columns and are preserved without explicit dependency declarations.

Downstream expressions continue to determine required scalar fields. Proxy fetches only those fields into rerank DataFrames, plus fixed system columns required by execution:

- `$id`;
- `$score`; and
- `$element_indices` (Arrow Int32) when present in element-level results.

Fetched scalar fields are input materialization requirements, not final liveness roots. Execution-time column pruning derives their lifetime from operator inputs and removes them after their last use. System columns remain available automatically, so the chain does not need a final Select operator. Final response projection remains owned by Hybrid Search.

Nested L0/L1 field dependencies are planned independently for each sub-search
using the ordinary QueryNode Function Chain path. They must not be added to
`rerankMeta`, the Proxy requery field set, or final `output_fields` merely because
the nested chain reads them. QueryNode performs the existing L0 export and L1
late materialization for the corresponding plan only.

Top-level L2 required fields remain owned by Hybrid requery planning. A schema
field needed by both nested L1 and top-level L2 is therefore materialized once
in each scope that consumes it; the two planners must not assume that a
temporary QueryNode DataFrame column crosses the RPC boundary.

## Validation and Error Classification

### Request and representation errors

The following are input errors and return existing `merr` parameter errors:

- `function_chains` is combined with legacy `strategy`/`params` or typed
  `function_score`;
- top-level `function_score` is combined with a nested L0/L1 chain;
- unsupported chain stage or multiple L2 chains;
- missing, duplicated, or misplaced Merge;
- invalid declarative Merge parameters;
- `merge` is used in an ordinary Search Function Chain;
- a nested sub-search contains L2 or `merge`;
- a nested sub-search repeats L0 or L1, contains a nil/empty chain, uses an
  unknown stage, or uses an operator/function unsupported at that stage;
- a nested chain reads an unknown collection field or an unsupported field
  type.

Merge representation and parameter validation are owned by the existing Merge implementation. Hybrid validation adds only endpoint-specific stage, cardinality, placement, and rerank-source rules.

### Runtime contract errors

The following are system or function execution errors:

- Proxy fails to populate Search runtime metrics in sub-request order;
- reduced-result metrics no longer align with converted DataFrames;
- production conversion loses required system identity columns; and
- result assembly cannot resolve an identity emitted by the executed chain;
- a serialized sub-plan does not contain the already validated chain assigned
  to that sub-search index.

Context must be added with `merr.Wrap` or `merr.Wrapf` so errors returned by the Merge dependency preserve their original classification.

## Compatibility

### Old clients

Valid requests without `function_chains` follow the existing code path without behavior changes:

- legacy rank parameters still default to RRF where they do today;
- `function_score` still builds the existing internal rerank chain; and
- current SDK ranker APIs remain supported.

This integration deliberately does not change the current handling of requests that combine legacy `strategy`/`params` with `function_score`. Cleaning up that historical ambiguity requires a separate compatibility review.

### New clients against old servers

Old servers that contain the reserved protobuf field but retain current validation reject Hybrid `function_chains` as unsupported. SDKs should surface the server error and may gate the feature by server version.

That behavior applies to the top-level L2 field. For nested L0/L1, an older
server may accept the enclosing Hybrid request and silently discard
`requests[i].function_chains` during conversion. This is more dangerous than an
explicit unsupported-feature error. SDKs must therefore gate nested chains by a
known minimum server version or an explicit capability response before sending
them. Documentation must not describe a successful response from an older
server as evidence that nested rerank executed.

### New servers

A new server must never silently discard a nested rerank field. It either
validates and places the chain in the matching sub-plan, or returns a typed input
error. Requests from old clients contain no nested chains and remain unchanged.
Sub-searches without a chain continue to use their current plan generation and
execution paths.

## Development Plan and Status

The Merge factory, metric injection, native element identity, and DataFrame element-index conversion were implemented first. The endpoint, SDK, and end-to-end work are split into independently reviewable changes.

### PR 1: Milvus server integration — implemented

1. Copy `HybridSearchRequest.function_chains` in `convertHybridSearchToSearch`.
2. Introduce a rerank-source selector that:
   - selects the public chain without constructing an implicit legacy RRF ranker;
   - rejects a chain combined with `function_score`; and
   - rejects a chain combined with the legacy `strategy` or `params` keys while allowing response-control rank parameters.
3. Add Hybrid-specific chain validation:
   - exactly one L2 chain;
   - at least one operator;
   - first and unique `merge`.
4. Reuse `functionChainRerankMeta` to plan downstream scalar inputs and trigger pre-rerank requery only when required.
5. Reuse the existing metric list emitted by Hybrid reduction to construct `FunctionBuildContext.Search` in sub-request order.
6. Execute the public chain without appending a server-owned Sort, Limit, Offset, or RoundDecimal tail.
7. Preserve the existing empty-result shortcut. Request-stage structure and Merge validation still apply, while downstream operators are built only when there are rows to execute.
8. Reject declarative `merge` in ordinary Search endpoint validation.
9. Open the REST `functionChains` path while preserving the existing REST `rerank` conversion. Proxy ignores only the empty legacy placeholders emitted for a chain-only REST request and rejects meaningful legacy rerank configuration combined with a chain.
10. Preserve existing requery, non-requery, final projection, row collapse, and element-level assembly behavior.

### PR 1b: Hybrid sub-search L0/L1 integration — implemented

1. Add `function_chains` to `milvuspb.SubSearchRequest` and regenerate the Go
   protobuf binding.
2. Copy each public Hybrid sub-request chain into the corresponding compact
   `SubReqs[i]` during `convertHybridSearchToSearch`.
3. Reuse Proxy stage splitting to reject nil chains, duplicate or unknown
   stages, and nested L2 before generating the affected sub-search plan and
   before dispatch.
4. Copy accepted L0/L1 chains into their corresponding serialized plans without
   Proxy semantic validation. QueryNode rejects invalid operators, functions,
   system-column access, and schema inputs.
5. Change `tryGeneratePlan` to take explicit function-rerank presence for the
   plan being built. Ordinary Search passes its top-level state; Hybrid passes
   the state for the current sub-search.
6. Assign each sub-search's chains to that sub-search's
   `PlanNode.querynode_function_chains` before serialization.
7. Keep the QueryNode execution and authoritative validation implementation
   unchanged.
8. Reject top-level `function_score` combined with any nested L0/L1 chain in
   Proxy before generating sub-search plans.
9. Add REST nested `functionChains` parsing on `SubSearchReq` and attach the
   generated chains to the corresponding public `milvuspb.SearchRequest`.
10. Add Go unit, REST, mixed-stage, struct-element, validation, and old-client
   compatibility tests described below.

### PR 2: PyMilvus API — partial / separate SDK delivery

1. Add `FunctionChain.merge(strategy, ...)` with typed strategy-specific validation.
2. Make the Hybrid `ranker`/`rerank` argument optional when `function_chains` is supplied.
3. Add an explicit `function_chains` argument to synchronous, asynchronous, ORM, and `MilvusClient` Hybrid entry points instead of relying only on `kwargs`.
4. Accept one chain or a one-element list and reject unsupported stages, multiple chains, invalid Merge placement, and conflicting rerank sources before RPC.
5. Keep `limit`, `offset`, and `round_decimal` accepted for request compatibility, document that they do not alter public-chain output, and keep output fields, consistency, and partition controls available.
6. Add builder serialization, request construction, conflict, and backward-compatibility tests.
7. Add `function_chains` to `AnnSearchRequest`; serialize it only into that
   request's nested `milvuspb.SearchRequest`, never into the top-level Hybrid
   field or sibling requests.

Go, Java, Node, and other SDK builders may follow in separate SDK changes. They do not block the server contract because the protobuf field already exists.

### PR 3: End-to-end coverage and documentation — partially implemented

1. Add Python client E2E coverage for all supported Merge strategies and downstream public L2 operators.
2. Cover row-level and struct element-level candidate identities with both Int64 and String primary keys.
3. Verify hidden scalar inputs are fetched for rerank but never leaked into final projection.
4. Verify public-chain final-output ownership, REST parity, empty-result behavior, and old ranker compatibility.
5. Publish API examples and migration guidance from `RRFRanker` and `WeightedRanker` to declarative Merge.

## Hybrid Integration Test Plan

Existing Merge factory, strategy, identity, defensive-validation, and allocator tests remain the unit-test foundation. The integration tests below prove that the Hybrid endpoint supplies the correct inputs and preserves the result contract end to end.

### Proxy tests

- Hybrid request conversion preserves `function_chains`.
- Compact sub-request conversion preserves empty entries and exact sub-request
  ownership, including multiple requests over the same `anns_field`.
- A Hybrid sub-plan with L0, L1, or both contains exactly that sub-search's
  chains in `PlanNode.querynode_function_chains`; sibling plans do not inherit
  them.
- A chained sub-plan uses `SearchType_DEFAULT`, while an eligible sibling
  without a chain retains its optimized search type.
- Proxy rejects malformed nested stage structure before plan generation, while
  preserving stage-valid chains for QueryNode's existing semantic validation
  path before execution.
- Old ranker without a chain remains unchanged.
- `function_score` plus a top-level or nested chain is rejected.
- Explicit legacy `strategy`/`params` plus a chain is rejected.
- Response-only `rank_params` plus a chain is accepted.
- Exactly one L2 chain with first Merge is accepted.
- Missing, duplicate, or non-first Merge is rejected.
- Ordinary Search rejects Merge.
- Request-level Hybrid group-by is propagated to every ANN sub-search; tests do not require the final user-chain output to preserve grouping.
- Downstream required fields are fetched but not leaked into projection.
- RRF, weighted, max, sum, and average work through row-level Hybrid execution.
- Mixed metric directions and both `norm_score` values use actual sub-search metrics in request order.
- Element-level execution preserves Int64 and String PK without synthetic IDs.
- Supported L2 expressions, sort, and chain-level limit work after Merge.
- Request-level limit, offset, and score rounding do not modify public-chain output.
- Chain-level `limit` with `offset`, plus `map(round_decimal(...))`, determine final slicing and score representation.
- Empty-result requests return without building or executing downstream chain operators.
- Empty sub-search results retain the existing shortcut and do not prepare or
  validate nested chains because there is no chain input to execute.
- Requery and non-requery assembly preserve PK-to-field and element-index alignment.
- REST requests with a chain preserve existing rerank conversion, tolerate only empty legacy placeholders, and reject meaningful rerank conflicts in Proxy.
- REST `search[i].functionChains` populates only request `i`; nested L2, Merge,
  and duplicate L0/L1 return `ParameterInvalid`.

### Nested L0/L1 end-to-end tests

- One sub-search uses L0 only and another uses L1 only; use deterministic score
  transforms so the final fused order proves both stages executed.
- One sub-search uses L0 followed by L1 while a sibling has no chain.
- Two sub-searches over the same vector field use different chains without
  cross-assignment.
- Nested L0/L1 works with a legacy RRF/weighted ranker and declarative L2 Merge;
  top-level typed `function_score` is rejected before plan generation.
- L1 Sort and Limit change only that sub-search's candidate contribution before
  fusion; the top-level chain still exclusively owns the final returned count.
- Nested required scalar fields are available to L0/L1 but absent from the
  response unless requested in `output_fields`.
- A real XGBoost model produces the same scores at L1 and ordinary L2;
  Hybrid L2 accepts `merge -> xgboost` and plans the model feature fields in
  Proxy.
- Mixed row-level/element-level Hybrid preserves the configured collapse
  behavior after nested chains.
- Same-struct element-level Hybrid preserves `(primary_key, element_index)`
  alignment through L1 reorder and Limit.
- Int64 and String primary keys both preserve deterministic tie-breaking and
  final identity.
- PyMilvus and REST send equivalent nested protobuf structure.
- A server known not to support nested chains is rejected by SDK capability or
  version gating instead of accepting a silently ignored request.

### Failure-mode traces

The behavioral verification gate requires tracing the source of runtime data, not only testing the boundary transform. Cover each real failure mode from origin to final error class:

| Failure mode | Expected classification |
|---|---|
| invalid stage, Merge placement, strategy, parameter, or weight count | input / `ParameterInvalid` |
| nested chain lost between compact request and serialized plan | system error |
| missing or misordered runtime metric entry | system error |
| metric/DataFrame count mismatch | system error |
| missing `$id`, `$score`, or required `$element_indices` inside chain input | `FunctionFailed`; endpoint conversion loss remains a system error |
| element index outside Int32 range | system error |
| requery result cannot resolve a candidate identity | system error |
| downstream function execution/provider failure | preserve the originating typed error with `merr.Wrap`/`merr.Wrapf` |

For metrics, identity, and scalar fields, audit their construction and rewrite sites from QueryNode result reduction through DataFrame conversion, Merge execution, export, and final assembly. A passing success-path E2E alone is insufficient evidence for these behaviors.

### Compatibility tests

- Existing legacy Hybrid ranker suites pass unchanged.
- Existing Hybrid `function_score` suites pass unchanged, including historical mixed legacy-parameter requests.
- Existing ordinary Search Function Chain suites pass unchanged.
- Existing L0 Function Chain suites remain unchanged.
- Existing L1 Function Chain and mixed L0/L1 ordinary Search suites remain
  unchanged.
- Existing struct Hybrid row-collapse and element-level suites pass unchanged.
- Old Hybrid clients with no nested `function_chains` produce byte-compatible
  sub-plans apart from unrelated nondeterministic serialization details.

## Observability

Existing rerank tracing remains the outer execution span. Hybrid integration may add structured fields to existing contextual logs or traces for:

- selected rerank source;
- sub-search input count;
- row-level versus element-level execution; and
- per-stage candidate counts.

Merge-local error context and allocator behavior are covered by the existing implementation and tests. Integration logs must not include query vectors, model credentials, or full candidate payloads. New metrics use bounded labels; metric type lists and chain names do not become metric labels.

Nested execution reuses the existing bounded QueryNode L0/L1 latency metrics.
Proxy traces may record the sub-request index and the presence of L0/L1, but
chain names, field names, and expressions must not become metric labels.

## Alternatives Rejected

### Execute a public chain after the old ranker

Rejected because it leaves fusion owned by ranker and makes `function_chains` only a post-processing hook. It cannot fully replace ranker behavior and creates ambiguous scoring and ordering ownership.

### Retain synthetic element IDs

Rejected because downstream public expressions would observe an encoded internal string instead of the real primary key. It also changes Int64 PKs into String IDs during rerank and requires fragile encode/restore boundaries.

### Keep a Proxy-only sidecar

Rejected because the parallel slice duplicates the sub-request ordering
contract and requires separate length/index invariants. Adding the field to the
public compact `SubSearchRequest` keeps configuration attached to its owner.
The internal QueryNode request still avoids a second execution owner because
only the serialized `PlanNode` crosses that boundary.

### Associate nested chains by ANN field

Rejected because `anns_field` is not a request identity. Multiple sub-searches
may target the same field with different query vectors, filters, limits, and
chains. Stable request index is already the ordering contract used by Hybrid
metrics and Merge weights.

## Security and Resource Considerations

- Function Chain-required scalar fields remain subject to schema validation and supported-type restrictions.
- Model credentials continue to come from server-side provider configuration through `ModelExtraInfo`.
- Existing sub-search top-K and maximum-result-window limits remain the upstream candidate bound.
- XGBoost model artifacts are referenced through server-managed FileResources;
  Proxy resolves L2 models and QueryNode resolves L0/L1 models from their
  locally synchronized FileResource state.
- Nested L1 Limit may only reduce that bounded candidate set; it cannot increase
  ANN recall beyond the sub-search limit.
- Hybrid request validation rejects ambiguous rerank sources before execution.
- Nested chain semantic validation remains at QueryNode, immediately before that
  chain is prepared for execution.
- Merge parameter validation and Arrow ownership are covered by the existing Merge implementation and tests.
