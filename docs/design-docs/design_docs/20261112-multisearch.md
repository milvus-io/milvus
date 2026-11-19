# MultiSearch

## Motivation

A retrieval application often needs several independent searches for one user
request. It may search the same collection with different filters or search
collections with different schemas across databases. A single MultiSearch
call lets the server schedule those searches and return their outcomes in
input order. Each item keeps the semantics and result shape of ordinary Search.

Related request [#41791](https://github.com/milvus-io/milvus/issues/41791).

## API

MultiSearch accepts an ordered, nonempty list of Search requests. The gRPC
request and response contain `SearchRequest` and `SearchResults` directly.
Each Search can specify its own target, filter, search
parameters, output fields, and query vectors.

| Concept | gRPC | REST V2 |
| --- | --- | --- |
| Entry point | `MilvusService.MultiSearch` | `POST /v2/vectordb/entities/multi_search` |
| Search list | `requests[]` | `requests[].search` |
| In-flight items per batch | `max_concurrency` | `maxConcurrency` |
| Optional execution budget in milliseconds | `timeout_ms` | `timeoutMs` |
| Ordered results | `results[]` | `data[].search` |

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

Invalid authentication, an empty batch, or a full batch queue fails the entire
request before any Search begins. REST also rejects an item without a recognized
`search` object at this stage. Malformed fields inside a Search, such as a
missing collection name or an empty gRPC Search request, fail that item.
A transport error can prevent delivery of the response after searches have
already run. Clients can identify a Proxy that predates MultiSearch through
the gRPC unimplemented-method status.

## Execution

The Proxy models the outer request as a `MultiSearchTask` on the existing
`TaskScheduler`. Its independent `MSearchQueue` has 64 waiting slots and eight
workers outside the DQL worker pool. REST checks this queue before decoding the body, and
admission checks it again atomically when enqueuing. Only the individual
searches enter the ordinary DQL scheduler. Internal DQL work such as requery
and lexical highlight can dispatch while the main DQL worker pool is busy.
Using DQL workers for the outer request would let a saturated pool of waiting
parents block their own searches.

The batch reuses `BaseTaskQueue` admission and task ID allocation (without TSO),
`processTask`, and `TaskCondition`. `PreExecute` checks the execution context and
sets the batch concurrency; `Execute` prepares and dispatches items within that
limit, retries, and records outcomes. `PostExecute` has no work. After waiting
for completion or execution expiry, `RunMultiSearch` freezes the result slots
under their mutex and takes ownership of the results directly. It fills any
unfinished slots and records terminal counts before returning; late searches
cannot overwrite or count those results again. Canceled queued tasks
release their waiting slot. A running task keeps its worker until outstanding
searches exit, even if its response has already been delivered. Scheduler close
cancels the batches and waits for those workers.

Each REST or gRPC Search attempt owns a request-metrics scope and records its
resolved database, collection and resource group, including refusals and
retries. The outer MultiSearch counter has an empty database label because a
batch can span databases, and child resource groups do not alter its attribution.
Batch, terminal-item, attempt and retry counters describe separate events.
Accepted results record their elapsed time and completion trace event immediately;
finalization records these observations for unfinished items. Rejections and
actual retries are logged with the item index and request context.

An item takes its database from its request, the call's database header or
metadata, and finally the default database, in that order. It then uses the
resolved database for authorization, schema lookup, quota accounting, and
execution. The item inherits the caller's identity and trace and owns its
mutable request state. An unused call-level database does not affect items
that resolve their own targets. Every item checks ordinary Search privileges,
including on a schema cache hit.

The Proxy setting `proxy.multiSearch.maxConcurrency` limits active Search items
per batch for both gRPC and REST. With eight batch workers, the factory value
of 16 allows up to 128 active items on one Proxy. The
[MultiSearch prototype](https://github.com/milvus-io/milvus/pull/54940)
used the same per-batch limit. When the client omits the concurrency value
or sets it to zero, the requested concurrency defaults to four. The Proxy
uses the smallest of the requested concurrency, the configured maximum, and
the number of items. If a batch requests more concurrency than allowed, the
Proxy schedules all its items under the configured limit. Waiting in DQL and
waiting between attempts both occupy one item slot. When an item reaches a
terminal result, the batch can start the next item.

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

## List and cancel

Both transports register the admitted batch as one `MultiSearch` request before
it enters the MSearch queue. `ListRunningRequests` exposes its stable request ID,
caller, trace, queue state and scheduler task IDs. The registration remains in
place during item preparation and between retries. Child Searches reuse this
registration; completing a child does not finish or unregister the batch.

`CancelRequests` on that ID stops further dispatch and cancels all unfinished
preparation, Searches and retry waits. The batch returns the existing
non-retriable `RequestCanceled` status (3002), including the cancel reason, with
no result list. Execution-budget expiry retains its existing partial-result
semantics. Cancellation and completion use the registry's finish boundary: a
cancel accepted before it wins the response; a later cancel is not found.
Cancellation is asynchronous, so the response does not promise that every
QueryNode has already released its resources.

A batch can span databases and collections. Its single-target fields (`db_name`,
`collection_name`, `nq`, `topk`, `expr`) are therefore left empty or zero. List
without database/collection filters, optionally filtering by user or elapsed
time, to find batches. Independent child cancellation and target-set filtering
are outside this change; neither a Search attempt nor a scheduler task ID is a
separate MultiSearch cancellation handle.

## Verification

Compare the REST and gRPC results with ordinary Search using the same request
parameters. Exercise mixed and all-item failures, multiple databases with the
same collection name, multi-vector results, and authorization for each target.
Control the scheduler and quota boundaries in Proxy tests to verify admission,
requery progress, bounded retries, cancellation, and timeout finalization.
Check the peak number of active items with omitted, zero, and explicit
concurrency, including a configured maximum below the default. Verify that a
batch requesting more concurrency than allowed still returns every item in order.
List and cancel the batch while queued, preparing, executing and backing off;
verify one stable registration, child completion isolation, cancellation/result
races and capacity recovery. Exercise the public List/Cancel path against both
gRPC and REST batches and check the outer 3002 response.

Measure ordinary Search fan-out and MultiSearch with matching requests and
concurrency. Record round latency, success rate, resource usage, and ordinary
Search behavior under mixed traffic before tuning the default concurrency
and queue capacity.
