# Function chains

`functionchain` mirrors PyMilvus's `pymilvus.function_chain` builders and wire
format. Go builders defer validation to `ProtoMessage()` or the SDK option's
`Request()` / `HybridRequest()` method; these return errors instead of panicking.

```go
import (
    fc "github.com/milvus-io/milvus/client/v3/functionchain"
    "github.com/milvus-io/milvus/client/v3/milvusclient"
)

chain := fc.NewFunctionChain(fc.L2Rerank).
    Map("freshness", fc.Decay(fc.Col("published_at"), "exp", 1700000000, 86400)).
    Map("$score", fc.NumCombine(fc.Col("$score"), fc.Col("freshness")).
        WithParam("mode", "weighted").WithParam("weights", []float64{0.7, 0.3})).
    SortBy(fc.Col("$score"), true, fc.Col("$id")).
    Limit(10)
option := milvusclient.NewSearchOption("docs", 100, vectors).WithFunctionChains(chain)
results, err := client.Search(ctx, option)
```

| PyMilvus | Go |
| --- | --- |
| `FunctionChain(stage, name, output_columns=...)` | `NewFunctionChain(stage).WithName(name).WithOutputColumns(...)` |
| `col(name)` | `Col(name)` |
| `FunctionChainExpr(name, args, params)` | `NewExpr(name, args...).WithParam(key, value)` |
| `FunctionChainOp(...)` | `Op{...}` and `chain.Append(op)` |
| `.map(output, expr)` | `.Map(output, expr)` |
| `.sort(by)` | `.Sort(by)` (descending) |
| `.sort(by, desc, tie_break_col)` | `.SortBy(by, desc, tieBreak)` (`nil` omits tie-break) |
| `.limit(n, offset=0)` | `.Limit(n)` or `.Limit(n, offset)` |
| `.merge(strategy, ...)` | `.Merge(strategy, MergeOptions{...})` |
| `.grep(field, arguments=..., output="matches")` | `.Grep(field, arguments)` or `.Grep(field, arguments, output)` |
| `fn.num_combine`, `fn.decay`, `fn.round_decimal` | `NumCombine`, `Decay`, `RoundDecimal` |
| `fn.xgboost`, `fn.rerank_model` | `XGBoost`, `RerankModel` |
| `function_chains=chain` or a list | `.WithFunctionChains(chain, ...)` |
| `client.grep(...)` | `client.Grep(ctx, NewGrepOption(...))` |

`WithParam` supplies helper options such as numeric combination mode/weights,
XGBoost output mode, and rerank provider parameters. `NewExpr` and `Op` allow
custom server functions and operations. Literal arguments and parameters support
booleans, signed integers, unsigned integers within int64 range, floats, strings,
`[]byte`, slices/arrays and string-keyed maps. Strings are literals; use `Col` for
field references. Nil literals and cyclic parameter structures are rejected.

The SDK serializes top-level hybrid search chains without enforcing stage or
chain-count rules; the server validates which combinations it supports. Current
server support is limited to a single top-level L2Rerank chain. Combinations such
as PreProcess + L2Rerank + PostProcess can be constructed and sent by the SDK,
but require a server that supports them. Request-construction tests verify the
wire representation, not server execution support.
Use `Merge("rrf")` (default k=60), `Merge("weighted", MergeOptions{Weights: ...})`,
or server-supported max/sum/avg merging. The latter strategies default
`norm_score` to true. `MergeOptions.K` and `NormScore` are pointers so explicit
zero/false overrides remain distinct from omitted options. ANN sub-requests
accept chains through `NewAnnRequest(...).WithFunctionChains(...)`. Sub-search
chains must be non-empty and have distinct stages. Legacy function scores
cannot be combined with chains on the same request; top-level hybrid chains
also exclude legacy merge rankers.

```go
option := milvusclient.NewGrepOption("docs", "content", "-i -e 'timeout'").
    WithFilter("id > 0").WithOutputFields("id").WithLimit(20).WithOffset(0)
results, err := client.Grep(ctx, option)
// results.GetColumn("matches") is a JSON column of matching-line details.
```

For filter-only grep, use a QueryScan chain without output columns:
`NewQueryOption("docs").WithFunctionChains(fc.NewFunctionChain(fc.QueryScan).Grep("content", "-e timeout"))`.
Exported columns require a named QueryScan chain and unique non-empty names;
`_original_idx` is reserved. Query accepts at most one non-empty QueryScan chain.
Grep arguments are forwarded unchanged. Matching line numbers are one-based;
line offsets and half-open match spans use UTF-8 bytes. Query limit/offset paginate
matching documents. Current server restrictions include count/aggregation,
order-by and query iterators.

**Server compatibility:** `Grep` and QueryScan chains passed directly to `Query`
require a server that supports QueryScan grep. Older servers may silently ignore
`function_chains` and execute an ordinary query, returning rows that were not
filtered by the grep pattern, without the requested match-details column and
without an error. This also affects filter-only chains. An empty result cannot
distinguish an unsupported server from a supported server with no matching rows.
The SDK does not perform a server version/capability check or validate returned
grep columns. Before using these APIs, verify QueryScan grep support in the
server deployment; a successful RPC alone does not establish support.

Builders retain argument/parameter references and must not be mutated concurrently.
Each serialization returns an independent protobuf snapshot. Calling
`WithFunctionChains()` with no arguments clears previously assigned chains.

`testdata/*.json` are actual PyMilvus protobuf outputs, compared with Go outputs
using protobuf equality. Regenerate with `testdata/generate.py` in the referenced
PyMilvus environment.
