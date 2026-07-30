# RESTful orderByFields / groupByFields review notes — 2026-07-30

## P1. Align global `count(*)` detection with Proxy

The REST helper should use `agg.MatchAggregationExpression` rather than compare
the raw string to `count(*)`. Proxy recognizes aggregation expressions with a
case-insensitive, whitespace-tolerant parser, so valid forms such as
`COUNT ( * )` currently receive REST's synthetic default limit and are then
rejected by Proxy as paginated global counts.

Suggested predicate:

```go
isAgg, name, field := agg.MatchAggregationExpression(strings.TrimSpace(output))
if isAgg && name == "count" && field == "*" {
    return true
}
```

Add coverage for `COUNT ( * )` with a real second aggregate such as
`sum(word_count)`. No synthetic limit should be forwarded. Explicit pagination
is a separate case: it must be forwarded, never silently dropped.

> Current worktree note: the Proxy prohibition on global `count(*) + limit`
> has been removed and REST now forwards every positive limit. Therefore this
> parser-specific REST special case is no longer needed; the parser alignment
> would only have been required by the earlier suppression approach.

## P2. REST's default `limit=100` and global aggregation

**Conclusion: the original review comment is obsolete in the current state.**

The original change suppressed every limit for a global `count(*)` request in
order to bypass Proxy's former validation. That made a synthetic REST default
indistinguishable from an explicit user limit and also caused Proxy to ignore
an accompanying offset (it parses offset only when a limit key exists).

The current implementation removes that Proxy validation and forwards every
positive REST limit. No JSON field-presence tracking is necessary.

This is semantically safe: `limit` does **not** cap aggregation input rows.
The C++ `AggregationNode` consumes every filtered input batch and accumulates
all rows. With no `GROUP BY`, it produces exactly one aggregate row. Only
after aggregation does Proxy's `SliceOperator(limit, offset)` paginate output
rows. Consequently, an omitted REST limit (the default 100) or an explicit
`limit: 10` both return the full value for a global `count(*)` over, for
example, 3000 matching entities. `offset: 1` skips the sole output row and
returns empty, which is a consistent but not especially useful pagination
meaning for a global aggregate.

Required regression coverage: a real REST E2E with more than 100 matching
entities, asserting that omitted limit and explicit `limit: 10` both return
the full `count(*)`. The current Proxy unit test only verifies request
validation, not the final aggregation result.

## P3. `search_aggregation` and `order_by_fields` must be rejected in Proxy

**Conclusion: valid review comment; Proxy-only change is needed.** REST v2
already rejects both `orderByFields` and legacy
`searchParams.order_by_fields` when `searchAggregation` is set. Direct SDK /
gRPC callers bypass that handler and can currently send both.

The direct request creates `aggCtx`, then parses `order_by_fields` into
`t.orderByFields`. Pipeline selection gives aggregation priority:

```text
aggCtx != nil -> searchWithAggPipe -> reduce -> agg
```

`searchWithAggPipe` has no `order_by` operator, so sorting is silently
ignored. The two features also have no coherent shared output meaning:
top-level `order_by_fields` sorts ordinary search hits, whereas search
aggregation returns aggregation buckets (and has its own `top_hits.sort`
semantics).

Add the same mutual-exclusion validation in Proxy,
`searchTask.initSearchAggregation()`, next to its existing
`group_by_field(s)` checks:

```go
orderByFields, err := funcutil.GetAttrByKeyFromRepeatedKV(
    OrderByFieldsKey, t.request.GetSearchParams(),
)
if err == nil && strings.TrimSpace(orderByFields) != "" {
    return merr.WrapErrParameterInvalidMsg(
        "order_by_fields and search_aggregation cannot be used simultaneously",
    )
}
```

Add a `TestSearchTask_initSearchAggregation` case containing both fields and
assert this error. Do not add another REST validation; the REST v2 handler
already rejects the combination.

## P4. A global aggregate cannot be mixed with a raw output field

**Conclusion: valid review comment; fixed in the current worktree test.** A
request with no `groupByFields` and
`outputFields: ["count(*)", "word_count"]` is equivalent to
`SELECT count(*), word_count ...` without a `GROUP BY`. `count(*)` has one
global row while `word_count` has one value per entity, so they cannot form a
row-aligned result.

`agg.NewAggregationFieldMap` intentionally rejects this shape before
execution: global aggregation output fields may only be aggregation
expressions. The old HTTP mock test did not invoke Proxy validation and thus
mistakenly treated the invalid payload as successful.

Use `["count(*)", "sum(word_count)"]` for a global mixed aggregate, or use
`groupByFields: ["word_count"]` with
`outputFields: ["word_count", "count(*)"]`. The handler unit test now uses
the first valid form; include that modification in the final change.

## P5. New REST fields need real endpoint coverage

**Conclusion: valid review comment; addressed with REST E2E coverage.** Handler
tests with a mocked Proxy verify only HTTP-to-gRPC parameter forwarding. They
cannot establish that `orderByFields` changes returned order or that
`groupByFields` creates correct aggregation rows.

The REST query suite now covers deterministic category grouping together with
group ordering, asserting each returned group key and its `count(*)`. It also
checks a global count over more than the REST default page size with omitted
and explicit limits, proving response pagination does not cap the aggregation
input. Existing REST order-by cases already verify actual returned row order.
