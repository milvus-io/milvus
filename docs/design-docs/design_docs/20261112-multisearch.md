# MultiSearch

## Motivation

A retrieval application often needs several independent searches for one user
request. It may search the same collection with different filters or search
collections with different schemas across databases. A single MultiSearch
call lets the server schedule those searches and return their outcomes in
input order. Each item keeps the semantics and result shape of ordinary Search.

Related request [#41791](https://github.com/milvus-io/milvus/issues/41791).

## API

MultiSearch accepts an ordered, nonempty list. Each item wraps one Search
request. The operation discriminator leaves room for additional operations in a
future protocol revision. Each Search can specify its own target, filter, search
parameters, output fields, and query vectors.

| Concept | gRPC | REST V2 |
| --- | --- | --- |
| Entry point | `MilvusService.MultiSearch` | `POST /v2/vectordb/entities/multi_search` |
| Search list | `requests[].search` | `requests[].search` |
| In-flight items per batch | `max_concurrency` | `maxConcurrency` |
| Optional execution budget in milliseconds | `timeout_ms` | `timeoutMs` |
| Ordered results | `results[].search` | `data[].search` |

For example, this REST payload searches two collections, with two query vectors
in the first item and one in the second.

```json
{
  "requests": [
    {"search": {"collectionName": "papers", "data": [[1, 0], [0, 1]], "limit": 5}},
    {"search": {"dbName": "archive", "collectionName": "guidelines", "data": [[1, 0]], "limit": 3}}
  ],
  "maxConcurrency": 2,
  "timeoutMs": 5000
}
```

An outer success status means that the response contains one item per request
in the same order. Each successful item contains the ordinary Search response,
including multi-vector grouping metadata and REST Int64 encoding. Zero hits
and fewer hits than requested are successful outcomes. Each failed item
contains its Search error. An entire list of item failures is a valid batch
response.

Invalid authentication, an empty batch, an unrecognized operation, or a full
batch queue fails the entire request before any Search begins. Malformed fields
inside a recognized Search, such as a missing collection name, fail that item.
A transport error can prevent delivery of the response after searches have
already run. Clients can identify a Proxy that predates MultiSearch through
the gRPC unimplemented-method status.

## Execution

The Proxy admits the outer request into a bounded orchestration queue outside
the DQL worker pool. REST checks this queue before decoding the body, and
admission checks it again atomically when enqueuing. Only the individual
searches enter the ordinary DQL scheduler. Internal DQL work such as requery
and lexical highlight can dispatch while the main DQL worker pool is busy.
Using DQL workers for the outer request would let a saturated pool of waiting
parents block their own searches.

An item takes its database from its request, the call's database header or
metadata, and finally the default database, in that order. It then uses the
resolved database for authorization, schema lookup, quota accounting, and
execution. The item inherits the caller's identity and trace and owns its
mutable request state. An unused call-level database does not affect items
that resolve their own targets. Every item checks ordinary Search privileges,
including on a schema cache hit.

Omitting the concurrency value or setting it to zero uses the Proxy default.
Positive values bound the number of active items for the batch, up to the
number of requests. Waiting in DQL and waiting between attempts both occupy
one item slot. When an item reaches a terminal result, the batch can start the
next item.

Only a temporary rate-limit refusal or rejection at DQL admission triggers
a batch-level retry. Each attempt repeats the ordinary quota check for the
item's resolved target and NQ. There are at most ten attempts, with delays
starting at 200 ms and doubling up to 3 s. If the next delay does not fit in the
remaining budget, the item retains its last refusal while execution time remains.
Permission, invalid parameter, and force-deny failures end the item immediately.

## Execution budget

Omitting `timeoutMs` leaves REST execution governed by the parent request.
An explicit timeout starts at batch receipt, before REST decodes the request
body. It covers preparation, both queues, execution and retries. The budget
accepts whole milliseconds from 0 to 9,223,372,036,854. Zero exhausts the
budget immediately.
When time runs out, the Proxy stops dispatching and cancels outstanding work.
It returns recorded results and reports timeouts for the remaining positions
while the parent call can still receive a response. REST applies an explicit
`Request-Timeout` header in seconds as a separate transport deadline through
response delivery. The endpoint has no default HTTP request deadline. A parent
cancellation ends the call and propagates to the searches.

Completed results are retained until the response is serialized. Per-Search
limits and the concurrency setting bound individual work. Retained result
memory grows with the number and size of successful items in the batch.

## Verification

Compare the REST and gRPC results with ordinary Search using the same request
parameters. Exercise mixed and all-item failures, multiple databases with the
same collection name, multi-vector results, and authorization for each target.
Control the scheduler and quota boundaries in Proxy tests to verify admission,
requery progress, bounded retries, cancellation, and timeout finalization.

Measure ordinary Search fan-out and MultiSearch with matching requests and
concurrency. Record round latency, success rate, resource usage, and ordinary
Search behavior under mixed traffic before tuning the default concurrency
and queue capacity.
