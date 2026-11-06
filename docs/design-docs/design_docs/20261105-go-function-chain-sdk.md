# Go SDK Function Chain Support

## Scope and source baseline

The Go client previously exposed `FunctionScore` but could not construct or attach
public Function Chains. This change adds the typed plan builders and request
integration for the server capabilities in Milvus `46f3354858`.
The comparison SDK is PyMilvus `1dfdc1c6e0e3b8c81765d774f97e4c9e729b0cec`:
its `pymilvus/function_chain/{chain,fn}.py` provides Search, Hybrid, XGBoost and
QueryScan builders. Go additionally provides typed JSON-path column references
and a helper for the existing server-side Python UDF expression.

This is a client-only change. It does not change execution, storage, ingestion,
collection metadata, CDC, or protobuf definitions. The existing public protobuf
dependency already contains the necessary messages.

Related server designs:

- [Search Function Chain API](20260624-function-chain-api.md)
- [Hybrid Function Chains](20260818-hybrid-search-function-chain.md)
- [Grep / QueryScan](20261008-segcore-grep-ripgrep-integration.md)
- [Python UDF expression](20260722-pyudf-function-chain.md)

## Execution and ownership

| Request location | Accepted stages | Execution boundary |
| --- | --- | --- |
| Search | At most one each of L0, L1, L2 | Per segment, per worker after segment reduction, Proxy after global reduction |
| Hybrid ANN leg | At most one each of L0, L1 | Independently inside that recall leg |
| Hybrid top level | Exactly one L2 when chains are provided | Proxy fusion; first and unique operator must be `merge` |
| Query | At most one QueryScan | Per-segment Grep before final pagination and projection |

L0 accepts `map`. L1 accepts `map`, `sort`, and `limit`. L2 supports the server's
generic operators, with `merge` restricted to top-level Hybrid and `grep`
restricted to QueryScan. QueryScan currently requires exactly one `grep`.
Reserved Ingestion, PreProcess and PostProcess enum values are representable,
but are rejected by these request entry points.

The existing response contract is unchanged. Search scores are returned in
`ResultSet.Scores`; QueryScan computed columns are ordinary typed `FieldData`
and can be read with `ResultSet.GetColumn("matches")`. Typed empty JSON output
columns must survive a zero-hit query.

## Public Go API

The `entity` package owns `FunctionChain`, `FunctionChainExpr`, `FunctionChainOp`,
`ColumnRef`, and the stage aliases. These types use the existing client style:
constructors and fluent methods, with validation at `ProtoMessage` / `Request`.
`FunctionChainOp` and `AddOp` preserve access to the full public operator
representation without requiring a Go release for each new server operator.
For example, server-supported L2 `filter`, `select`, and `group_by` can use
`AddOp` with their typed inputs, outputs, expression and parameters.

```go
chain := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).
    WithName("freshness").
    Map("fresh", entity.NewDecayExpr(entity.Col("published_at"), "exp", 1700000000, 86400)).
    Map("$score", entity.NewNumCombineExpr("weighted",
        entity.Col("$score"), entity.Col("fresh")).
        WithParam("weights", []float64{0.7, 0.3})).
    Map("$score", entity.NewRoundDecimalExpr(entity.Col("$score"), 4)).
    Sort(entity.Col("$score"), true, entity.Col("$id")).
    Limit(10, 0)

option := milvusclient.NewSearchOption("articles", 100, vectors).
    WithANNSField("embedding").WithFunctionChains(chain)
```

`WithFunctionChains` is available on Search options, `AnnRequest`, HybridSearch
options, and Query options. It replaces the prior list; calling it without
arguments clears the list. Builders are mutable and read when constructing a
request. Each serialized chain is a deep snapshot, including bytes, nested
parameters, and operator slices. Concurrent builder mutation is unsupported.

### Expressions

| Helper | Parameters / constraints |
| --- | --- |
| `NewNumCombineExpr(mode, columns...)` | At least two columns; sum/multiply/min/max/avg/weighted; weighted requires matching weights |
| `NewDecayExpr(column, function, origin, scale)` | gauss/exp/linear; offset defaults to 0, decay to 0.5; override via `WithParam` |
| `NewRoundDecimalExpr(column, decimal)` | Decimal 0–6; current server requires Float32 input, normally `$score` |
| `NewRerankModelExpr(column, queries...)` | L2; nonempty queries; provider/model parameters via `WithParam` |
| `NewXGBoostExpr(modelResource, features...)` | Ordered features; UBJ FileResource; output defaults to `default`, accepts `raw`; L0/L1/L2 |
| `NewPyUDFExpr(resource, params, inputs...)` | L2; registered wheel and enabled server worker; `MapOutputs` supports multiple outputs |

`NewFunctionChainExpr(name, args...)` allows generic expressions and literal
arguments. A plain string is a literal; `entity.Col("field")` is a column.
Built-in helpers check their argument and parameter contracts. The generic
constructor defers expression semantics to the server, preserving server defaults
and access to new expressions. Current built-in expressions require columns.
Resource resolution, collection
schema validation, model loading, provider credentials and execution errors
remain server responsibilities. No client-side model execution is introduced.

The earlier XGBoost design described only its initial L0 delivery. The current
`NewXGBoostExpr` server implementation and newer Hybrid design support all three
rerank stages; this SDK follows that implementation.

### Typed JSON inputs

```go
price := entity.Col(`metadata["price"]`).WithDataType(entity.FieldTypeDouble)
ctr := entity.Col(`$meta["ctr"]`).WithDataType(entity.FieldTypeInt64)
expr := entity.NewNumCombineExpr("weighted", entity.Col("$score"), price).
    WithParam("weights", []float64{0, 1})
```

Hints use the existing reserved operator parameter `$input_data_types`.
Serialization preserves column occurrence order and duplicates, omits literal
arguments from the hint array, and uses None for untyped columns. It never
writes the generated parameter back into the caller's parameter map.
Supported explicit hints are Bool, Int64, Double and VarChar. System columns
cannot carry hints. Conflicting hints for a repeated column in one operator,
or explicit reserved parameters combined with generated hints, are rejected.
Schema-dependent checks and cross-operator hint consistency belong to the server.

### Hybrid fusion

```go
nested := entity.NewFunctionChain(entity.FunctionChainStageL1Rerank).
    Map("$score", entity.NewRoundDecimalExpr(entity.Col("$score"), 3))
leg := milvusclient.NewAnnRequest("dense", 100, queryVector).
    WithFunctionChains(nested)
fusion := entity.NewFunctionChain(entity.FunctionChainStageL2Rerank).
    Merge("weighted", map[string]any{"weights": []float64{0.7, 0.3}}).
    Sort(entity.Col("$score"), true, entity.Col("$id"))
option := milvusclient.NewHybridSearchOption("articles", 10, leg, otherLeg).
    WithFunctionChains(fusion)
```

Merge supports `rrf` (including optional weights), `weighted`, `max`, `sum`, and
`avg`. Defaults match PyMilvus: RRF k=60, otherwise norm_score=true. An explicit
chain Limit owns pagination; without one the server uses the Hybrid request's
limit/offset. The SDK preserves both independently and does not add a ranker.

Top-level chains conflict with both legacy rankers and FunctionScore. Nested
chains can coexist with a legacy top-level ranker or L2 chain, but cannot
coexist with top-level FunctionScore or same-leg FunctionScore.

### QueryScan

```go
scan := entity.NewFunctionChain(entity.FunctionChainStageQueryScan).
    WithName("scan").WithOutputColumns("matches").
    Grep(entity.Col("text"), "-i -e timeout", "matches")
option := milvusclient.NewQueryOption("logs").WithFilter("id > 0").
    WithOutputFields("id").WithLimit(20).WithFunctionChains(scan)
```

Without `WithOutputColumns`, Grep filters rows without returning matching details.
With it, decode each JSON byte entry in the `matches` column as an array of
matching lines. Line numbers are one-based; offsets and spans are UTF-8 byte
positions. Count, aggregation, order-by, and iterators are unsupported.
The raw rg argument string is forwarded unchanged for server parsing.

## Validation and compatibility

Typed parameters preserve bool, signed int64, double, string, bytes, arrays and
string-keyed objects. Unsigned values must fit int64. Unsupported values, nil,
empty object keys and nesting beyond 64 levels (including cycles) fail locally
with the client's typed ParameterInvalid error. There is no JSON round trip.

Request validation runs before collection lookup or Search/Query RPC. It checks
stage placement/cardinality, empty/nil plans, incompatible rerank sources,
merge position/weights/parameters, QueryScan shape, and known request conflicts.
Existing requests without chains retain their original behavior. Server errors
continue through the existing RPC status handling without reclassification.

This feature requires a matching server. Older protobuf servers can silently
ignore unknown request fields; the SDK does not negotiate chain capabilities.
No mixed-version execution guarantee is made.

## Verification plan and limits

Entity tests cover an independent PyMilvus-shaped protobuf fixture, wire round
trips, nested values, int64 bounds, snapshot ownership, JSON occurrence hints,
helpers and invalid input. Client tests cover stage placement, mutually exclusive
options, all merge strategies, RPC request transport, ragged Search results and
nonempty/empty computed Query columns.

Go E2E uses exact FLAT recall with four fixed rows to check scalar/JSON L0/L1/L2,
stage composition, merge arithmetic and pagination, hidden inputs, QueryScan
JSON contents, empty computed columns and server-side validation failures.
Related legacy weighted, decay and empty-result rerank cases are regressions.

This fixture does not establish multi-worker L1 candidate-budget behavior,
rolling-upgrade safety, successful external-provider calls, native XGBoost model
inference or enabled Python worker execution. Those require dedicated runtime
fixtures; protocol support must not be reported as proof of those executions.

## Implementation map and tradeoffs

The main data path is builder → typed public protobuf → server chain planner →
stage executor → existing result decoder. `FunctionChain` owns an ordered list
of operators; an operator owns an optional expression, typed column references,
outputs and parameters. A serialized request owns copies of that state.

The following diagram is generated with `beautiful-mermaid.renderMermaidASCII`:

```text
┌─────────────┐ ┌─────────────────┐ ┌───────────────┐ ┌───────────┐
│             │ │                 │ │               │ │           │
│ Go builders ├►│ Public protobuf ├►│ Server stages ├►│ ResultSet │
│             │ │                 │ │               │ │           │
└─────────────┘ └─────────────────┘ └───────────────┘ └───────────┘
```

Source entry points:

- `client/entity/function_chain.go`: plan representation, recursive values and hints.
- `client/entity/function_chain_expr.go`: built-in expression helpers.
- `client/milvusclient/function_chain.go` and `read_options.go`: request placement.
- `internal/proxy/dql/function_chain_validator.go`: Search and Hybrid planning.
- `internal/proxy/dql/query_function_chain.go`: QueryScan request routing.
- `internal/util/function/chain/repr.go` and `input_plan.go`: public-plan conversion
  and schema-aware input planning.
- `internal/util/function/chain/operator_merge.go`: fusion parameter semantics.
- `internal/util/function/chain/query_plan.go`: QueryScan subset and exports.
- `client/milvusclient/read.go`: existing score/column response decoding.

Storage retrieval, distributed reducers, native model inference and provider
clients are intentionally treated as existing server interfaces. The SDK sends
plans and consumes their results; it must not recreate those execution engines.

The main tradeoff is validation ownership. Strong Go types remove Python's
runtime type checks for stages, column references and builder arguments, but
nested `any` values still require checked conversion. Local checks catch
structural mistakes without requiring a collection lookup. Schema-dependent
semantics and resource state remain server-owned to avoid divergent client caches.
A future API could add immutable builders; this change preserves the existing
SDK's fluent mutable style and documents the snapshot boundary instead.
