# FunctionChain POST tables

- Status: implemented and locally exercised; repository-wide gate recorded below
- Related design: [transformation stages](20261010-function-chain-transform-stages.md)
- Related issues: #46565, #52319

## Contract

The [runtime schema extension](20261012-function-chain-runtime-schema.md)
unifies nested paths and validates actual Arrow schemas while preserving logical
types. Python runtime 0.4.0 adds schema-aware execution version 4.

An explicitly selected POST TABLE chain transforms finite query/search result
tables into a new table. Output fields, row count and grouping are independent
of collection fields, hit IDs/scores and request NQ. Existing POST HITS chains
continue to enrich candidates while preserving their identity and cardinality.
INGESTION and PRE contracts are unchanged.

`FunctionChain.post_process_options` selects `result_mode` (HITS or TABLE) and
`input` (FUSED or ANN_INPUTS). Options are only valid on POST. ANN_INPUTS
requires TABLE and HybridSearch. Unsupported combinations fail before execution.

SearchResults and QueryResults expose `post_process_result` for TABLE calls.
It contains an Arrow IPC stream, including its complete schema, and row_count.
It is mutually exclusive with the normal hit/field payload. Empty output still
contains a schema. SDKs check the dedicated capability and require the new
response field for every TABLE call; absence is an error, not empty success.

The optional PyMilvus Arrow dependency is checked before sending TABLE calls.
Ordinary search/query does not acquire an Arrow dependency or a new return type.

## Input scope and provenance

FUSED uses the normal search result after reduction, optional fusion/rerank and
the configured final candidate limit. Each NQ remains one logical input group.
`$query_index` identifies the original zero-based query; `$ann_index` is -1
because a fused candidate cannot be assigned to one ANN source.

ANN_INPUTS takes each ANN branch after distributed reduction, before top-level
fusion and the final Hybrid limit. Each branch retains its own candidate limit.
Top-level L2 chains and explicit rankers conflict with this mode; branch PRE,
L0 and L1 remain valid. Groups are ANN-major, then query-major, including empty
groups. `$ann_index` and `$query_index` identify their sources. Duplicate IDs
from different groups retain their individual scores and source rows until an
explicit operation combines them. Required stored fields are fetched for all
selected candidates, not only candidates surviving the ordinary fusion path.

Query accepts POST TABLE/FUSED over the normal reduced query result, with the
existing filter, limit and offset. Both source indices are -1. This does not
turn a bounded retrieval into an aggregation of every matching stored row.
Native Query aggregation, Search aggregation, iterators and search-by-IDs are
not silently routed through an incompatible adapter.

Lineage remains outside user-controlled fields. Many-to-one outputs retain the
union of their contributing source rows. Empty-input aggregate outputs may
have no source rows. Selecting a payload column never changes provenance.

## Operations

`concat` combines current logical groups into one group in stable input order,
allowing a subsequent aggregate to combine multiple NQ or ANN branches.
Grouping and aggregation are scoped to the current logical group. Explicit
group keys can retain query/branch identity after concatenation.

`group_by(keys).aggregate(...)` emits one row for each key group. Its semantics
are separate from the existing rerank group_by operator, which retains ranked
hits. Aggregate output includes keys and declared aggregate aliases, not
arbitrary ungrouped input columns. Group order follows first occurrence;
key nulls form a group. A keyed aggregate of no rows returns zero typed rows;
an unkeyed aggregate returns one row with COUNT zero and other empty values
following each aggregate's declared semantics.

Native aggregates are `count`, `sum`, `avg`, `min`, `max`, `first`, `last` and
`collect_list`. COUNT(*) counts rows; COUNT(column) excludes nulls. `first` and
`last` retain nulls at their chosen positions; `collect_list` retains nulls and
input order. Integer SUM checks overflow. Integer AVG uses a floating-point
accumulator. TABLE sort is stable and does not implicitly require an ID column.

TABLE also supports projection, map, filter, sort, limit and list expansion.
Native operators retain their column type checks. Every operator participates
in dependency analysis, provenance and cancellation. Output is serialized as
Arrow rather than coercing a new schema into hit-shaped SearchResultData.

## Python execution

Table execution retains the query snapshots and original NQ independently of
the current number of groups. Combining groups does not change query identity.
POST callable inputs remain selected Arrow columns; runtime context is
request-owned and separate from cached UDF instances. A versioned private
worker contract distinguishes table execution from the existing hit-preserving
contract. Output validation depends on the operator's cardinality contract;
changing data shape does not disable validation.

Runtime 0.3.0 uses private execution version 3 for TABLE; existing transformations
continue to use version 2. Worker responses must acknowledge the requested
version. The public capability is `function_chain_post_table_version=1`.

`reduce` invokes a reduction-capable expression with all current key groups.
For Python, `transform_batch(params, batches, *, context)` returns exactly one
row per input batch. `context.table_result` and `context.reduction` are true.
The native operator owns grouping keys and provenance; UDF output aliases
describe independent payload columns. TABLE map uses the same batch API with
`reduction=False` and preserves batch row counts.

A keyed reduction with no groups invokes the UDF once on typed empty input to
establish the output schema, then discards the returned row. An unkeyed empty
reduction retains its one summary row. UDF implementations must handle these
empty batches; no extra schema probe occurs when real groups exist.

## SDK example

```python
from pymilvus import FunctionChain, FunctionChainStage
from pymilvus.function_chain import agg, col

post = (
    FunctionChain(FunctionChainStage.POST_PROCESS, result_mode="table")
    .concat()
    .group_by("document_id")
    .aggregate({
        "matches": agg.count(),
        "texts": agg.collect_list(col("text")),
        "queries": agg.collect_list(col("$query_index")),
    })
)
result = client.search(
    "documents", data=query_vectors, anns_field="vector", limit=20,
    function_chains=post,
)
print(result.schema, result.rows)
```

Choose `input="ann_inputs"` for the same pipeline over Hybrid branches before
fusion. Omit `concat()` to aggregate independently within each input group.
Install the matching PyMilvus checkout with `pip install -e '.[arrow]'`.

## Scope

The adapter is available through gRPC Search, HybridSearch and Query. REST
transformation requests are explicitly rejected. TABLE only processes the
finite candidates supplied to it, not all collection rows or streaming state
across requests. L0/L1 do not gain Python execution, and this change does not
add a WASM runtime. Matching Milvus, public protocol, PyMilvus and Python worker
versions are required; these are local implementation revisions, not a release.

## Verification

Local validation on 2026-10-12:

| Scope | Result |
| --- | --- |
| PyMilvus full unit suite | 5380 passed, 3 skipped; includes sync, coroutine and Future decoding/capability checks |
| Installed Python runtime | 88 passed and 65 subtests; runtime 0.3.0 |
| Go FunctionChain and PyUDF packages | Full package suites passed with required build tags, disabled optimizations and race detection |
| Proxy DQL, DML, ingestion, transformation I/O and HTTP | Full affected package suites passed; latest empty-branch fix also has focused race regression coverage |
| Distributed Proxy and client | Passed after providing the required isolated etcd fixture; first run without etcd failed |
| Error-contract guards | Full merr race suite passed |
| Protocol generation | Official public/private generators passed; Python SDK regeneration byte-identical |
| Local standalone execution | 16 sync/async probes, 5 empty branch/Query probes and the complete SDK example passed on the final binary |
| Legacy behavior | Existing ingestion, PRE, L2 and HITS POST example passed; plain search still returns ordinary hits |
| Complete repository gate | See delivery report for the official `make test-go` outcome |

Real service probes cover cross-NQ grouping, duplicate IDs across Hybrid
branches, per-branch limits, independent nested Struct output, Query reduction,
all-empty typed results and unchanged ordinary hits. Review found and fixed a
metadata-only source field in a completely empty ANN branch; a regression
traverses reduction, requery, organization and TABLE conversion for scalar,
Array, JSON and StructArray sources. Fault checks cover stale server/worker
responses, malformed Arrow data, invalid lineage, reduction cardinality and
cancellation. No external model APIs were called.
