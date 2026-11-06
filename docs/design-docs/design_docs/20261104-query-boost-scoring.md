# Boost Scoring and Global Ordering in Query

- **Created:** 2026-11-04
- **Author(s):** @xiaofan-luan
- **Status:** Draft; proposed API and execution contract, not implemented
- **Component:** SDK / Protocol / Proxy / QueryNode / Segcore
- **Related Issue:** [#55091](https://github.com/milvus-io/milvus/issues/55091)
- **Released:** N/A

## Summary

Extend the existing collection-based `query()` API with an optional Boost ranker,
a query-local score column, optional filtering on projected results, and exact
global ordering over the complete hard-filtered visible row set. Reuse existing
`Function`/`FunctionScore` types, native predicate-weight scoring, QueryScan
infrastructure, computed-result transport, and scalar ORDER BY machinery.

The public call stays flat: `collection_name`, `filter`, `output_fields`,
`order_by`, `offset`, and `limit` retain their roles. Add `ranker` and
`post_filter`. This proposal introduces no QueryPlan SDK object, recursive source,
SQL statement parser, subqueries, or new MATCH_SCORE operator.

This document replaces the earlier composable-query proposal in the same PR.
[#55090](https://github.com/milvus-io/milvus/issues/55090) was closed in favor of
Boost reuse. General arithmetic projections remain a separate expression-engine
workstream in [#50923](https://github.com/milvus-io/milvus/issues/50923).

## Motivation and scope

A rule-ranked recipe query has hard conditions (approved, selected cuisine,
excluded ingredients) and request-specific preferences. A taste rule matching
any of three spicy labels contributes 3 once; audience, season, and promotion
rules contribute 2, 1.5, and 1. The answer is ordered by total score descending,
then quality_score descending. All hard-filtered rows must participate before
any lossy limit.

The same path can consume text predicates when those predicates are supported.
[#55089](https://github.com/milvus-io/milvus/issues/55089) independently owns
phrase-prefix and AUTO-fuzzy extensions. Branch eligibility belongs in predicate
composition; this proposal does not add a general K-of-N expression or promise
bit-exact Elasticsearch scoring.

The first release supports finite-weight, filter-based Boost functions with Sum
combination, score projection/aliases, optional row post-filtering, and existing
finite query pagination. Out of scope: arbitrary numerical projection formulas,
random/model/PyUDF scorers, mixing ranker and public Function Chains, aggregation,
count, ordered iterators, nested query stages, and Search behavior changes.
[#55106](https://github.com/milvus-io/milvus/issues/55106) owns ordered snapshot
iteration. Search's existing function-rerank/ORDER BY restriction is separate;
this Query proposal does not automatically remove it.

## Public API

All ranked Query calls below are **proposed**, not currently runnable. Function
and FunctionScore construction reuse existing SDK types.

```python
from pymilvus import Function, FunctionScore, FunctionType

def boost(name, condition, weight):
    return Function(
        name=name,
        function_type=FunctionType.RERANK,
        input_field_names=[],
        params={"reranker": "boost", "filter": condition, "weight": weight},
    )

ranker = FunctionScore(
    functions=[
        boost("taste", 'ARRAY_CONTAINS_ANY(tastes, ["香辣", "麻辣", "酸辣"])', 3.0),
        boost("audience", 'ARRAY_CONTAINS(audiences, "儿童")', 2.0),
        boost("season", 'ARRAY_CONTAINS(seasons, "夏季")', 1.5),
        boost("promotion", 'ARRAY_CONTAINS(tags, "当季主推")', 1.0),
    ],
    params={"function_mode": "Sum", "boost_mode": "Sum"},
)

hard_filter = (
    'approved == true and cuisine == "粤菜" '
    'and not ARRAY_CONTAINS_ANY(ingredients, ["黄瓜", "青瓜"])'
)

rows = client.query(
    collection_name="recipes",
    filter=hard_filter,
    ranker=ranker,
    output_fields=["id", "name", "quality_score", "$score AS score"],
    order_by=["score:desc", "quality_score:desc", "id:asc"],
    limit=100,
)
```

Here id is the actual primary key. The rule weights can change with each request.
Each full filter contributes at most once, including the taste rule's internal
OR. A request that explicitly wants to discard non-positive scores adds:

```python
rows = client.query(
    collection_name="recipes",
    filter=hard_filter,
    ranker=ranker,
    output_fields=["id", "quality_score", "$score AS score"],
    post_filter="score > {minimum}",
    filter_params={"minimum": 0},
    order_by=["score:desc", "quality_score:desc", "id:asc"],
    limit=100,
)
```

Without post_filter, zero-score rows remain eligible. A caller can order by
`$score` without returning it. Existing order_by syntax and explicit NULL-placement
rules remain available. Initially require a nonempty explicit order_by and a
finite limit for ranked Query; do not invent an implicit ordering. Preserve
existing configured offset+limit bounds.

Java extends the existing QueryReq builder, reusing the existing FunctionScore
type. The setter is proposed and is not available in today's QueryReq:

```java
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;

QueryReq request = QueryReq.builder()
    .collectionName("recipes")
    .filter("approved == true")
    .functionScore(ranker)
    .outputFields(List.of("id", "quality_score", "$score AS score"))
    .orderByFields(List.of(
        OrderByField.builder().fieldName("score")
            .direction(AggDirection.DESC).build(),
        OrderByField.builder().fieldName("quality_score")
            .direction(AggDirection.DESC).build(),
        OrderByField.builder().fieldName("id")
            .direction(AggDirection.ASC).build()))
    .limit(100L)
    .build();
```

The Python `ranker` and Java `functionScore` serialize the same typed score
configuration. Do not infer support merely because a Python API accepts kwargs.
Every SDK must explicitly serialize the new options and decode score projections
into normal Query row fields, rather than Search distance metadata.

## Score and binding contract

### Query-specific Boost semantics

- The Query base score is zero. The result is the sum of matching rule weights.
- An absent function_mode or boost_mode means Sum in this new Query path.
  Explicit Sum is accepted; Multiply and other modes are rejected in the first
  release. Search's existing defaults and semantics remain unchanged.
- Validate the complete function list before calling shared scorer helpers:
  require at least one Boost function with the supported filter/weight shape.
  Reject non-Boost functions, random_score, unsupported fields, and unknown
  options; never silently skip an unrecognized function.
- Weights must be representable as finite Float32. Zero and negative finite
  weights are allowed; ordering is ordinary numeric ordering. Reject non-finite
  inputs and a non-finite accumulated result. An omitted rule filter matches all
  hard-filtered visible rows.
- A complete predicate contributes its weight only when its final value is TRUE.
  FALSE/UNKNOWN are misses. Repeated configured functions preserve multiplicity;
  do not deduplicate them just because predicates are equal.
- Reuse the native Boost Float32 weight and Sum accumulation contract in declared
  function order. It is not #55090's retired proposed double-accumulation contract
  and is not a claim of bit-exact ES compatibility. Each row's entire sum is
  computed before TopK; scores are not partial sums combined across shards.
- Native optional no-match results become non-nullable Float32 zero in Query.
  Do not reuse Search's no-function identity or default Multiply behavior.

### Names and scopes

`$score` is a virtual Float32, non-null column produced by the ranker. It is not a
stored collection FieldID. The initial projection subset is an existing field,
`$score`, or either identity followed by contextual `AS alias`. Arbitrary
arithmetic/function expressions in output_fields are not part of this release.

| Clause | Meaning |
|---|---|
| filter and Boost filters | Collection input fields; no score or output aliases |
| output_fields | Stored identities and the virtual score, optionally aliased |
| post_filter | Projected scalar output names plus the virtual score |
| order_by | Projected aliases first, virtual score, then unambiguous stored fields |

Post-filtering requires a ranker, returns BOOL, and retains only TRUE. Its first
release uses supported scalar comparisons/logical expressions on the resolved
output schema; reject unimplemented operations explicitly. Reuse shared scalar
predicate evaluation/type rules, not a second scoring language. filter_params
is one request-local value map shared by filter, ranker predicates, and post_filter;
conflicting type uses fail binding. Parameters cannot define field names/aliases.

An alias may shadow a stored field not separately returned: input filter `score`
still means the stored field, while post_filter/order_by `score` can mean the
exported `$score AS score`. Require unique final output names and reject aliases
in the system-column namespace. Peer aliases are not visible to other projection
items. Parse AS contextually, not by splitting text; preserve existing identifiers
and literals containing AS.

For ranked requests, `*` expands declared collection fields and does not implicitly
return `$score`. Arbitrary per-row dynamic keys do not become projected columns;
use supported explicit dynamic paths with resolved types/aliases. Do not let a
dynamic key overwrite a declared output. A stored `$score` name conflicts with
the virtual namespace and is rejected for ranked requests. Existing unranked
Query projection and dynamic behavior remain unchanged.

Keep required but non-returned sort fields, primary keys, MVCC/provenance, and
score as private execution slots. Do not expose them through star expansion.
Supported stored VECTOR/ARRAY/JSON identities may pass through existing deferred
output paths; reject a type if the selected new transport/operator path cannot
preserve it, rather than dropping it silently. Final computed results carry
their explicit name, type, validity, and column identity through SDK decoding.

## Execution design

Source baseline: Milvus `adf11c906f61d1fda952524bb0bfc06d63a904c4`.

[#54282](https://github.com/milvus-io/milvus/pull/54282) already connected ordinary
QueryScan, Grep, computed-result transport, and deferred output. Current QueryScan
accepts only one grep operator, and both Query and worker validation reject chain
ORDER BY/iterator/count/aggregation combinations. Reuse this infrastructure, but
do not simply remove the checks and pass ranked Query through Grep's execution.

The minimum logical and distributed pipeline is:

```text
Proxy: validate Query/ranker/scopes -> bind score and sort slots

each segment:
    visible hard-filtered offsets, in bounded scan batches
      -> native Boost sum -> score/field projection -> optional post_filter
      -> complete-key candidate retention

QueryNode/shard -> identity-safe complete-key merge
Proxy          -> global merge -> offset/limit -> final field assembly
```

Authorize the actual collection through the existing Query database, privilege,
partition, and policy/filter path. There is no recursive source or temporary
collection. Select one actual MVCC read context, keep required read views and
provenance alive through final deferred field fetch, and release them on all
exits. Reusing a GuaranteeTs lower bound alone does not establish a fixed view.

### Reuse native scoring, not another expression engine

Validate Query's supported subset and fill its Sum/Sum defaults before adapting
FunctionScore to the native ScoreFunction/ScoreOption structures. Existing search
helpers have Multiply defaults and may skip unsupported functions, so they are
not a sufficient Query validator. Use native filter compilation and WeightScorer /
ComputeFunctionScores over all eligible segment offsets; convert absent scores
to zero and carry the typed result as a score slot.

This is an internal ranked-Query physical path. Reuse QueryScan's storage access,
column/provenance handling, and computed-result transport where applicable. Do
not require clients to encode Boost as a Grep operator or publish a new expression
language. First-release ranker and public function_chains are mutually exclusive;
the existing Grep path and its supported API remain unchanged.

### Score before truncation

Final offset/limit may bound correctly ordered candidates, never the pre-score
candidate universe. Do not use a PK prefix, an ANN window, a per-batch top-K treated
as final, or a Grep match limit as the complete ranked input.

Current retrieve row caps can call find_first_n and warn that results may be
incomplete. Grep also propagates QueryLimit to native ScanRequest.Limit. Neither
is a valid exact-scoring contract. Ranked Query must enumerate the complete
eligible input, or fail when a scan/work budget is exceeded. It must not return
the best of an undisclosed prefix as successful global TopK.

Use the complete requested comparator, including quality_score and optional
primary-key tie-break, in every lossy retention and merge. Preserve existing
NULL/floating ordering for stored keys. Without a unique last key, exact ties
have no guaranteed relative order. Apply offset only after global reconciliation.

Materialize every sort key before candidate retention. The current Grep path
fetches ordinary output fields after PK/limit merge; quality_score cannot stay
at that late-fetch point. Only non-ranking payload fields can be deferred. The
new Materialize RPC currently rejects Query chains, so its existence does not
establish an available score-aware deferred-fetch path; integrate and validate
the chosen path explicitly.

Resolve MVCC/primary-key identity before effective candidate caps, or use an
ordered continuation/refill protocol that proves duplicate removal cannot hide
a unique winner. Per-segment top-K plus final dedup is not automatically a proof
of global unique TopK. Layout, growing/sealed overlap, compaction, and retries
must be covered by the reference tests before the implementation is released.

### Resources and failures

Stream offset/evaluation batches and retain live sort/score/identity slots. Load
wide output fields for final winners when existing snapshot/provenance contracts
permit. Reuse bounded ordered retention or enforce a resource budget before
allocation; the current SortBuffer retains rows before sorting/truncation, so
its limit alone is not an O(K) memory guarantee.

Budget scans, predicate work, score vectors, row widths, candidate/refill state,
transport buffers, and deferred fetches. Initial implementation does not require
spill. Exhaustion, unsupported workers, expression failures, and missing shard
responses fail explicitly; a warning or partial result is not complete success.
Propagate the outer deadline/cancellation through scan, scoring, merge, and fetch,
with ownership cleanup on every exit.

Physical batching alone is not a bound on retained input: current candidate
offsets may already cover the whole segment, and a heap selecting K indices does
not release its materialized input. Account for these structures before claiming
bounded memory. Initial enforced budgets are valid; an O(K) RSS claim is not.

## Protocol, compatibility, and implementation sequence

Retain the existing flat public QueryRequest. It already has function_chains=17,
but no FunctionScore field. Add an optional typed FunctionScore, reusing Search's
schema; define post_filter and projection/ordering options in the Query contract.
Assign exact wire tags in milvus-proto and regenerate generated files. Do not
encode the function list in an opaque JSON parameter or add a recursive AST.

Python query adds explicit ranker/post_filter serialization. Java QueryReq adds
the FunctionScore member/builder/serialization and the post-filter option.
Existing score types are reused; their current existence on Search does not
mean they are already connected to Query. Result decoding must use the computed
column's schema, not require it to exist in collection metadata.

Introduce a ranked-Query plan capability/version checked by SDK/Proxy and all
selected workers, with explicit execution acknowledgement/completeness. Older
servers must not ignore the new FunctionScore field and return an ordinary PK
query. Add a versioned result acknowledgement or equivalent validated status
metadata; seeing a field called score is not proof of execution. Confirm zero-row,
empty-segment, retry, and RPC fallback paths too. Retry/failover must retain the
read context and required capabilities or fail. Do not silently substitute Search,
client-side reranking, or capped scans.

Implement in this order:

1. Freeze flat API, score/alias/post-filter scopes, and Sum/zero semantics; add
   SDK/protocol serialization and fail-closed capability validation.
2. Add a validated Boost Query plan and native score slot over the complete
   hard-filtered scan, reusing QueryScan and scorer components.
3. Connect post-filter, complete-comparator retention/identity-safe merge, result
   aliases, deferred fetch, and explicit resource/failure behavior.
4. Enable the feature only after reference, mixed-version, layout, and resource
   tests pass. No generic QueryPlan/subquery milestone is required for delivery.

## Test plan

This is a design-only change. The following runtime tests are required of the
implementation and are not claimed to have run.

- **Recipe rules:** zero, one, and all rule matches; matching two spicy tags adds
  3 once; duplicate configured rules count separately; request weights change
  results; omitted filter matches all; zero/negative finite weights; reject
  non-finite inputs/results and unsupported scorer/mode options.
- **Zero and NULL:** all misses return non-null Float32 zero; zero rows remain
  without post_filter; FALSE/UNKNOWN do not add weight; explicit score > 0 runs
  before candidate caps; keep supported native Float32 accumulation semantics.
- **Full-set correctness:** compare to an independent full-scan oracle; put a
  winning row beyond the old PK/scan limit; exercise secondary-key winners,
  exact ties, inner OR, offset, growing/sealed overlap, duplicate versions,
  segment/shard layouts, compaction, and zero-row typed results.
- **Scopes/results:** aliased score and same-named stored field; hidden score/sort
  slots; AS identifiers/literals; duplicate/reserved names; star/dynamic-key
  collisions; unsupported identity types; normal Python/Java query-row decoding.
- **Compatibility/security:** unchanged unranked Query/Grep/Search; ranker+chain
  rejection; count/aggregate/iterator rejection; actual collection authorization
  and policy filters; unknown fields/parameters cannot silently bypass scoring;
  old SDK/Proxy/workers and failed execution acknowledgement.
- **Resources/failure:** broad scans and wide rows, scan/work/memory exhaustion,
  errors/cancellation in each stage, reader/Arrow release, incomplete workers,
  and retry/failover snapshot/capability preservation. No incomplete success.

Benchmark selective/broad hard filters, varying rule count and K, dense ties,
large offsets within allowed bounds, and wide deferred outputs. Measure latency,
evaluated rows, transferred/retained bytes, and resource failures. No throughput
or memory improvement is asserted without implementation measurements.

## Alternatives and follow-ups

- Client-side full retrieval remains an oracle, but adds transfer and application
  memory cost. Capped client reranking does not establish full-set TopK.
- A new MATCH_SCORE executor duplicates available predicate-weight scoring; Boost
  is the chosen route. Branch-threshold construction remains explicit, not an
  undocumented consequence of summing scores.
- General `expression AS alias` belongs to #50923. This first release only needs
  identity/score aliases and scalar post-filtering.
- QueryPlan/recursive source, independent inner ORDER BY/LIMIT, correlated queries,
  joins, and aggregate-as-source require separate later designs.
- Search Boost plus complete compound ordering is a separate integration task;
  its answer is over the explicitly defined ANN candidate window. Ordered Query
  iterator cursors remain #55106. Neither is implied by this Query change.

## References

- [Existing scalar Query ORDER BY](20260203-query-orderby.md)
- [Function Chain API](20260624-function-chain-api.md)
- [Row selection and result assembly](20261010-function-chain-row-selection.md)
- [Grep / QueryScan implementation PR](https://github.com/milvus-io/milvus/pull/54282)
- [Current QueryScan validation](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/util/function/chain/query_plan.go#L41)
- [Query function-chain routing](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/proxy/dql/query_function_chain.go#L28)
- [QueryScan execution](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/querynodev2/segments/query_scan.go#L44)
- [Native Boost scoring](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/core/src/rescores/BoostScoreRunner.cpp#L205)
- [Existing scorer parsing/defaults](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/parser/planparserv2/plan_parser_v2.go#L470)
- [Existing SortBuffer retention](https://github.com/milvus-io/milvus/blob/adf11c906f61d1fda952524bb0bfc06d63a904c4/internal/core/src/exec/SortBuffer.cpp#L73)
