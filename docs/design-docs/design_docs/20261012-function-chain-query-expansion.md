# FunctionChain PRE query expansion

- Related issues: #46565, #52319
- Related designs: [stages](20261010-function-chain-transform-stages.md), [POST tables](20261011-function-chain-post-tables.md), [runtime schemas](20261012-function-chain-runtime-schema.md)
- Status: implemented and locally verified in matching worktrees; draft integration under review

## Contract

PRE_PROCESS accepts `flat_map` and `explode`, in addition to existing `map` and
`select`. A function returns one List per current input row; expansion emits
each child as a separate prepared query. The engine preserves source query
lineage independently of projected payload fields. This adds query expansion
without registering a new operator.

The final `query` column must remain non-null text or a supported vector with
the existing target type/dimension checks. Each output has one original query
parent; original-parent order is preserved. An original query can yield no
children, but a whole ANN input with zero prepared queries is rejected before
native search. The configured NQ limit also applies after expansion.

On the public SDK/gRPC path, a successful initial search limiter check stores
the limiter, resolved database/collection/partition IDs and charged token count
in request context. After all PRE branches validate, the adapter reserves the
positive difference between total prepared queries and that charged watermark.
Successful reservation advances the watermark across tasks in the same RPC;
shrinking does not refund it. Quota failure or cancellation prevents publication
and native dispatch. The existing limiter's enabled state, rate policy and
punishment semantics remain in force; this is not a strict per-request budget.
Existing metadata-resolution fail-open behavior and direct internal calls
without an interceptor reservation remain unchanged. Python/model memory and
execution costs do not acquire separate quotas from this accounting.

Every prepared query is searched independently. For ordinary Search the
execution NQ and placeholders use the prepared count. For HybridSearch each
ANN branch has its own prepared count and origin mapping. The original request
NQ remains available for result interpretation and query context.

Actual non-identity expansion requires explicit POST TABLE output. Ordinary
Search accepts FUSED input. Expanded HybridSearch requires ANN_INPUTS, retaining
each branch's prepared-query groups before top-level fusion. HITS output and
ordinary Hybrid fusion do not invent a score-fusion policy for subqueries;
unsupported combinations fail before native search. Identity preprocessing
retains existing result behavior.

All branch transformations and validation complete before the request is
updated. A failed later branch does not partially publish an earlier branch's
expanded input. Existing request cloning and per-task execution guards prevent
preprocessing the same prepared request twice; this is not an exactly-once
guarantee for external model calls across independent retries.

## Query identity

`QueryContext` carries original and prepared snapshots plus per-ANN
`QueryOrigins`: prepared query index to original query index. During PRE,
`RowQueryOrigins` describes the current rows before each operator, derived from
protected lineage. PRE query snapshots describe stage-entry values, not arbitrary
intermediate List/Struct columns. After PRE, Proxy records final prepared
snapshots and origin mappings.

Python context exposes:

- `query_count`: original request NQ;
- `prepared_query_counts`: one snapshot count per ANN input;
- `query_origins`: per-ANN prepared-to-original mappings;
- `row_query_origins`: current PRE row origins;
- `original_queries` and `queries`: original and stage-appropriate snapshots.
- Rerank `query_index`: prepared query index; `original_query_index`: original
  parent index when unambiguous.

POST TABLE continues to receive all current query groups. Its source columns
are `$query_index` (original query index), `$subquery_index` (prepared query
index within its ANN branch), and `$ann_index` (-1 for FUSED, branch index for
ANN_INPUTS). Source coordinates and payload projection remain independent.
An empty candidate group does not lose its prepared query mapping.

`union_all().group_by("$query_index").aggregate(...)` combines subqueries for
each original question. Include `$ann_index` in keys to retain branch identity.
Grouping without `union_all` remains scoped to each current prepared-query group.

## SDK and worker compatibility

The server advertises `function_chain_pre_query_expansion_version=1` through
GetVersion. SDK requests containing PRE `flat_map` or `explode` require this
capability, including nested Hybrid branches and asynchronous/Future calls.
Existing identity `map`/`select` requests retain their previous capability checks.

Python runtime 0.5.0 adds private execution version 5 for expanded origin/row
mapping. Version 5 retains complete Arrow schema transport from version 4 and
requires exact worker acknowledgement. An old worker cannot silently discard
query provenance. Legacy execution versions remain accepted for their existing
contracts; identity calls do not require version 5 just because the host is new.

The [minimal call context](20261012-function-chain-call-context.md) uses runtime
0.6.0/version 6 when host scope is attached. It preserves the version-5 mappings
and adds per-invocation database/collection/authenticated user and operator log
labels. It does not introduce hosted connectors or a second deadline mechanism.

## Usage

```python
pre = FunctionChain(FunctionChainStage.PRE_PROCESS).flat_map(
    "query", fn.py_udf(col("query"), resource_name="expand_queries")
)
post = (
    FunctionChain(FunctionChainStage.POST_PROCESS, result_mode="table")
    .union_all()
    .group_by("$query_index")
    .aggregate({
        "texts": agg.collect_list(col("text")),
        "subqueries": agg.collect_list(col("$subquery_index")),
    })
)
# Pass both chains to search. The UDF must implement query expansion; its List
# element is the prepared query payload, not a separate query request object.
```

Request filter/topK/collection routing is not changed by returning extra PRE
columns. Parent-document lookup, conditional re-retrieval, token streaming,
durable jobs and persistent memory mutation remain separate integrations.

## Connector context: future interface

Trusted Python code can create its own MilvusClient if PyMilvus is installed in
the worker environment and network/authentication are configured. A local
read-only probe exercised real PyMilvus from a loaded UDF against a mock Milvus
gRPC endpoint. No user database or write operation was involved. The worker does
not currently expose a hosted connector client through context.

A future connector interface should separate a worker-level registry/connection
pool from request-scoped connector handles. The latter bind identity, database,
collection/namespace, deadline, cancellation, budgets and recursion policy.
The existing factory `PyUDFContext` can be retained by an instance and is suitable
for model initialization; `PyUDFCallContext` belongs to one invocation. Neither
currently contains hosted connectors. Shared pools must select connections by
connector and credential scope; per-user identity and request state must not be
stored on a concurrently reused UDF instance. Connector lifecycle should own
connection cleanup and credential refresh.
For example, `context.connectors["milvus"].query(...)` is a proposed interface,
not a currently implemented API. Python/WASM adapters can share this host
capability instead of embedding credentials or global mutable user state.

Nested SDK RPCs currently execute independently. Outer failure does not roll
back successful nested writes, and RPC cancellation does not forcibly terminate
running Python or its nested calls. Inserting into an ingestion-bound collection
re-enters its chain; recursive writes and effect replay require an explicit
policy. This change adds query expansion, not transactions or connector services.

## Local verification

- Go chain, expression, transform, PyUDF, ingestion, DQL and DML suites passed
  with `-tags dynamic,test`, `-gcflags="all=-N -l"` and the race detector.
  Final quota changes additionally passed complete DQL/taskmodel suites and
  focused Proxy tests including the existing rate-limit interceptor cases.
- The installed runtime 0.5.0 wheel passed 101 Python tests and 65 subtests.
  The matching SDK passed 5,428 tests; three tests were skipped.
- An isolated standalone executed two original queries as three prepared
  queries, retained all source indices, and aggregated their hits as 4/2.
  Hybrid branches executed different prepared counts (3/4), producing the
  expected per-original counts (4/2 and 2/6). The final binary was exercised
  after quota wiring was added.
- Standalone failure cases verified incompatible expanded HITS rejection
  (1100), zero total prepared-query rejection (2400), typed empty results,
  zero-child-parent provenance, and unchanged identity HITS behavior. POST UDF
  saw original NQ=3 after one original input produced no children.
- A fresh standalone with search rate 0.01 admitted one original query, then
  rejected the supplemental quota for three prepared queries. SDK received
  rate-limit code 8/compatible code 49 with the PRE quota context preserved.
- The existing deterministic chunking/translation/L2/highlight and POST TABLE
  examples passed with the new demo/runtime. No paid model API was invoked.

Evidence is under `/tmp/milvus-function-chain-pre-expansion-e2e-20261012/`
and the `/tmp/milvus-udf-pre-expansion-*` test logs. These checks are local,
not remote CI, a full repository suite, or deployment verification. Isolated
standalone SIGTERM cleanup reached the unchanged `CloseEtcdClient` path and
panicked with `context canceled`; graceful shutdown is not verified by these
search tests.
