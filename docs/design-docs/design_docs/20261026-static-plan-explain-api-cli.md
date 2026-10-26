# MEP: Static plan explain through the RESTful API and the milvus CLI

- **Created:** 2026-10-26
- **Author(s):** @joshbahk
- **Status:** Draft
- **Component:** Proxy
- **Related Issues:** #53830 (this design), #53829 (the explainer this exposes)
- **Released:** n/a

## Summary

`POST /v2/vectordb/entities/explain` takes the body of a search and answers
with the plan that search would send to the query nodes, as the version-1
JSON document of #53829 or its text view, without executing anything.
`milvus explain` posts such a body from a file or stdin and prints the
answer. Both go through the search request builder and the search
authorization, so what they explain is exactly the request a search would
run.

## Motivation

#53829 made `ExplainPlan` a Go function; using it means writing Go against
the proxy. A user who wants to know how the rewriter reshaped a filter, which
`output_fields` a search really carries, or why an expression is rejected,
still has to run the search. The issue asks for one public API and one CLI
entry point that reuse the explainer and its response schema, honor
authorization and database scope, agree with each other for the same
request, keep the existing error behavior for invalid expressions and
unauthorized requests, never execute a search, and add no cross-component
protocol.

## Public Interfaces

RESTful API, v2:

```
POST /v2/vectordb/entities/explain
```

The body is a search body (`collectionName`, `data`, `filter`, `exprParams`,
`limit`, `offset`, `outputFields`, `searchParams`, `annsField`, `dbName`,
and the rest of `SearchReqV2`) plus `format`: `json` (default) or `text`.
Defaults are the search defaults (`limit` 100, `dbName` `default`). The
response is the usual envelope: `{"code": 0, "data": ...}` where `data` is
the explanation object (`version`, `plan_type`, `vector_search`,
`predicates`, `output_fields`, ... as pinned by #53829) or, with `text`,
the text rendering as one string. Failures use the usual
`{"code": N, "message": "..."}` envelope with the same codes a search
returns; authentication failures are HTTP 401 and missing privileges
HTTP 403, as for a search.

CLI:

```
milvus explain [-address http://localhost:19530] [-token root:Milvus] [-format json|text] [-file search.json]
```

The file (or stdin with `-file -`) holds the search body. The command sets
`format` from the flag (replacing one in the body), posts the body to the
route above, and prints `data`: the JSON indented, or the text as is. A
non-zero `code` is printed to stderr with the server's message and the
process exits non-zero.

Go, proxy process only; no proto, RPC, SDK, config, metric, or segcore
change:

```go
func (node *Proxy) ExplainSearch(ctx context.Context, request *milvuspb.SearchRequest) (*planparserv2.PlanExplanation, error)
func dql.ExplainSearch(ctx context.Context, node taskmodel.TaskNode, request *milvuspb.SearchRequest) (*planparserv2.PlanExplanation, error)
```

## Design Details

- The request is the search's request. The part of the `search` handler
  that turns a `SearchReqV2` into a `milvuspb.SearchRequest` is extracted
  into `buildSearchRequest` (a pure move); `search` and `explain` both call
  it. `ExplainReqV2` embeds `SearchReqV2`, so the body, its validation, and
  the `dbName`/`collectionName` resolution are the search's, and a test
  asserts that the two routes hand the proxy `proto.Equal` requests for the
  same body. The builder resolves the collection schema the way every v2
  route does: from the proxy's meta cache, else through the proxy's
  `DescribeCollection` (which, with `proxy.enableCachedServiceProvider`
  off, enqueues a `DescribeCollectionTask` on the DDL queue). That is a
  metadata read, the same one `/search` makes, not a search.
- Authorization is the search's. The built `SearchRequest` goes through
  `wrapperProxy`, whose privilege interceptor derives the `Search` privilege
  on the collection, in the database named by the request. The route sits
  outside the DQL quota: the search limiter meters reads from the query
  nodes by the search rate and denies them under
  `quotaAndLimits.limitReading.forceDeny`; explain reads nothing from a
  query node, so it is neither charged nor denied. Its cost is the
  proxy-side part of a search, up to and including PreExecute, like the
  metadata routes that also run unmetered.
- The interceptor and the hooks see
  `/milvus.proto.milvus.MilvusService/ExplainSearch` as the method name.
  There is no such RPC: the name is in the form every hook and the forward
  interceptor are written against, and its last segment is the
  `ProxyFunctionCall` method tag. A hook's refusal is counted as a
  rejection, as on `/search`. `ForwardLegacyProxyUnaryServerInterceptor`
  matches the six DML/DQL RPC names exactly, so nothing is forwarded; the
  `Search` RPC name would send the request to a legacy proxy in standalone
  mode, which would run a real search there. A hook that keys on RPC names
  sees a distinct method and does not take an explain for a search; one
  that keys on the request type instead cannot tell them apart, which is
  what the hook API passes the method name for. If such a hook answers
  the request itself with search results, the route returns an internal
  error, not a plan.
- The route runs behind `timeoutMiddleware` only. The DQL middlewares do not
  apply: the search task is never enqueued (`dqlAdmission`), the answer is
  one explanation rather than a result set (`dqlTimeoutMiddleware`), and
  the byte metrics are labeled inside `Proxy.Search`, which explain never
  calls (`restfulSizeMiddleware`). A proxy whose DQL queue is full therefore
  still answers explains, at the proxy-side cost of a PreExecute each:
  `dqlAdmission` pre-fires the verdict the scheduler would give, and explain
  never reaches the scheduler. Wrapping the route in `dqlAdmission` would
  refuse explains under DQL overload instead.
- `Proxy.ExplainSearch` is a Go method, not an RPC. The REST server reaches
  it through an interface assertion on its `types.ProxyComponent`, with a
  compile-time probe so a rename fails the build. The REST server runs in
  the proxy process in both standalone and cluster mode, so the assertion
  holds in production; a proxy that does not implement it gets
  `ErrServiceUnimplemented`.
- `dql.ExplainSearch` builds the search task, calls its `PreExecute`,
  decodes the `SerializedExprPlan` PreExecute produced, which is what a
  query node would receive, and hands it with the collection's
  `SchemaHelper` to `ExplainPlan`. The task is never enqueued: no scheduler,
  no `Execute`, no `PostExecute`, no query node RPC. The task's own errors
  are returned unchanged, so an invalid expression or an unknown collection
  fails exactly as the search would. The scheduler stamps a non-strong read
  with the local clock before PreExecute; explain stamps the task the same
  way, since PreExecute derives the collection TTL window from it.
- One step of PreExecute is skipped: for a collection whose vector field is
  filled by a text embedding function, PreExecute sends the query text to
  the function's model provider after the plan is serialized, to prepare
  the vectors Execute would send. Explain sets `planOnly` on the task and
  PreExecute skips that call; with semantic highlighting rejected (next
  bullet), no third-party service is reached. The explanation does not
  depend on it. The checks that step makes before the call (an empty query
  string, more queries than the model's batch limit, the provider's
  configuration) are skipped with it, so a search of such a request can
  fail where its explain succeeded.
- Hybrid search (`subReqs`), search by primary key (non-empty `ids`, the
  test the search preflight uses) and semantic highlighting are rejected
  with `ErrParameterInvalid`: the first builds one plan per sub-request, the
  second resolves the ids through a query before any plan exists, the third
  connects to its model provider when PreExecute builds the highlighter,
  which `planOnly` does not gate. The REST body has neither `subReqs` nor a
  highlighter field (an unknown key is ignored, as on `/search`), so the
  first and third rejections only concern callers of the Go functions. Lexical
  highlighting is built from the schema alone and its fields reach the
  plan's `output_fields` as they do for the search, so it is explained as
  is. Supporting semantic highlighting means building the highlighter's
  field metadata without its model client; a follow-up.
- Version 1 explains with no `ExplainOptions`: template values are not
  redacted, because the caller supplied them and holds `Search` on the
  collection, and no candidate indexes are attached, because that needs the
  collection's index descriptions, a different privilege. Both are
  follow-ups.
- `data` for `json` is the document `PlanExplanation.JSON()` renders,
  embedded in the envelope as a `json.RawMessage`, so the key set and the
  version rule of #53829 hold by construction rather than through a second
  encoding of the struct. `text` is `PlanExplanation.Text()`.
- Observability: the trace log redacts `exprParams` as it does for a search;
  the access log records the request as for other REST routes;
  `ProxyFunctionCall` counts the route under the method `ExplainSearch`
  like every other v2 route. A failed explain lands in `fail` with the
  error's cause, where the same body fails on `/search`: the handler hands
  the proxy's error back as a status, as `Proxy.Search` does for a failed
  task, so the metric sees a processed failure rather than a rejection
  around processing. What fails before the proxy sees the request (a bad
  `format`, a body the handler cannot build a request from, an unauthorized
  user) is a rejection, as on `/search`, and so is a proxy without
  `ExplainSearch`. No new metric. The two histograms PreExecute itself
  feeds, expression parse latency and sparse query nnz, count an explain as
  a search.
- The CLI is a thin HTTP client. It decodes the body only far enough to add
  `format` (`map[string]json.RawMessage`), so numbers and vectors reach the
  server as written, and prints what the server answered.

## Compatibility, Deprecation, and Migration Plan

Additive. The `search` route behaves as before: the extraction is a pure
move, and the `planOnly` gate in PreExecute is off for every task the
scheduler runs. No wire, proto, config, or persisted-state change; nothing
to replicate on a CDC deployment. The JSON contract evolves under the
version rule of #53829.

## Test Plan

Unit tests, run with `-tags dynamic,test -gcflags="all=-N -l"`:

- `internal/proxy/dql/explain_test.go`: against a mocked meta cache, the
  plan PreExecute built (vector field, metric, topk, predicate, template
  name and value, the primary key PreExecute adds to `output_fields`);
  invalid expression and unknown collection return the search's error
  codes; hybrid, search-by-id and semantic highlighting are rejected while
  empty ids are an ordinary search; a lexical highlighter's field appears
  in `output_fields`; a collection with a text embedding function explains
  without a call to the model provider, which a search task does make.
- `internal/proxy/proxy_test.go`: an unhealthy proxy refuses to explain.
- `internal/distributed/proxy/httpserver/handler_v2_explain_test.go`: the
  same body, naming a non-default database and no `limit`, through
  `/search` and `/explain` yields `proto.Equal` requests with the default
  `limit`, and `data` equals `PlanExplanation.JSON()`; `text` format; an
  unknown `format` is rejected before the proxy is asked; the proxy's error
  code survives; `ProxyFunctionCall` counts a success, and a failure in the
  search's bucket, under the route's method; a hook is called with the
  `ExplainSearch` name, its refusal is counted as a rejection, and search
  results it mocks are an internal error; a proxy without `ExplainSearch`
  is unimplemented; with authorization on, a user without `Search` on the
  collection is 403 and gets 200 once granted.
  `trace_log_expr_params_test.go` covers the redaction.
- `cmd/milvus/explain_test.go`: against an `httptest` server, the path
  (also with a trailing slash on the address), bearer token, and body are
  forwarded with `format` set and a 2^53+1 integer intact; JSON is
  indented; text is printed as is; the body can come from a file; a
  non-zero code and a non-envelope response are reported; a body that is
  not an object (an array, `null`) is refused and the server sees no
  request.

Not in this change: a `tests/integration/httpserver` case against a running
cluster. The REST wiring is covered against a mocked proxy and the dql path
against a mocked meta cache; a cluster case can follow once the route is
agreed.

## Rejected Alternatives

- An `Explain` RPC on `MilvusService`: a new cross-component protocol and
  SDK surface, which the issue rules out; the REST server already lives in
  the proxy process.
- An `explain: true` flag on `/search`: a mistyped flag would run a real
  search, and the DQL middlewares would have to special-case the flag.
- Passing the `Search` RPC name to the interceptor so hooks and metrics see
  explain as a search: the forward interceptor would run a real search on a
  legacy proxy.
- Building the plan in the handler with `CreateSearchPlan` directly: it would
  duplicate PreExecute (partition key, output fields, templates, defaults,
  the rewriter) and drift from what a search sends.
- A gRPC CLI: needs a milvus-proto change; the HTTP client makes the CLI and
  the API agree by construction.
- Decoding the CLI body into Go values: integers above 2^53 and vectors
  would change through float64.

## References

- #53829, #53830
- `docs/design-docs/design_docs/20261015-static-plan-explain.md`
- `internal/proxy/dql/explain.go`, `internal/proxy/dql/task_search.go`,
  `internal/proxy/proxy.go`
- `internal/distributed/proxy/httpserver/handler_v2.go`, `request_v2.go`
- `cmd/milvus/explain.go`, `cmd/milvus/help.go`
- `internal/distributed/streaming/forward.go`
