# MultiSearch through the existing Milvus service

Related issue: https://github.com/milvus-io/milvus/issues/41791.

This prototype targets Milvus v3.0.5 (`af15c80751`) and extends the existing
`MilvusService` with `MultiSearch`. The companion protocol and PyMilvus patches
are in `examples/multisearch/patches`. They modify the actual public proto and
`MilvusClient`/`GrpcHandler`; there is no separate public service or Python adapter.

## Contract

`MultiSearchRequest.items` contains `{id, search: SearchRequest}` entries.
Each item can select its own database and collection and uses ordinary Search
parameters. Omitted databases inherit the client default (or ordinary request
metadata for direct gRPC callers). There is no database field on the envelope.
Item IDs are unique and results retain input order. Collections may have different
schemas, dimensions, metrics, output fields, filters and topK. The API does not
merge rankings or establish a cross-collection atomic snapshot.

`max_concurrent_searches = 0` uses the server default. A positive value can lower
concurrency and is capped by `proxy.multiSearch.parallelism`. A shared per-Proxy
permit pool bounds active MultiSearch calls across envelopes. Separate envelope,
item-count and byte limits bound admission and retained responses. These limits
do not reserve capacity for ordinary Search or establish tenant fairness.

Each outcome preserves ordinary `SearchResults`/`Status` or records a gRPC code
and message. Item errors do not cancel other items. Invalid envelopes, admission
rejection and aggregate response overflow fail the envelope. Parent deadline or
cancellation returns a transport error; unary transport failure cannot return
already-completed items. Automatic whole-batch retry is deliberately absent in
the Python method. Existing Search execution/retry behavior remains in place.
Full gRPC error details/trailers and resumable responses are not implemented.

## Integration and execution

1. `MilvusClient.multi_search()` accepts dictionaries of ordinary search arguments,
   plus `id` and `db_name`. It does not mutate the connection's database.
2. `GrpcHandler._prepare_search_request()` is shared with ordinary Search, including
   parameter checks, binary-vector schema preparation and database-scoped session
   timestamps. `GrpcHandler.multi_search()` uses the existing authenticated channel
   and generated `MilvusServiceStub.MultiSearch`. Metadata lookups may occur for
   request types that already require them; the searches use one MultiSearch RPC.
3. `grpcproxy.Server.MultiSearch()` delegates to the existing `Proxy.MultiSearch()` method.
   The Proxy owns batch limits and shared permits; the transport owns no executor.
4. Fixed dispatcher goroutines acquire a shared permit and invoke the shared Search
   entry chain, then directly `Proxy.Search -> dqQueue`. A method call alone
   does not run gRPC interceptors; the callback explicitly runs the ordinary chain.
5. Each ordinary search task retains its existing schema resolution, scheduling,
   shard routing, execution and reduction. QueryNode and native code are unchanged.
6. The handler waits for all dispatcher workers. This parks a Go goroutine and holds
   response state, but does not occupy a parent DQL worker slot. Only leaf searches
   enter the DQL queue, avoiding parent/child pool starvation.

The shared entry chain uses v3.0.5's live metadata-cache provider. Per-item request
clones and independent RPC statistics prevent mutable state sharing. The envelope
uses authentication; each child receives database normalization, authentication,
collection authorization, quota and existing hooks. Private Cloud billing/gateway
behavior has not been validated. Envelope wire-byte accounting is a follow-up.

The transport installs the shared Search-entry callback on the Proxy once before
serving. It terminates directly at the same Proxy component's Search method;
children never call back into Server.Search. Until installation, MultiSearch
returns a retriable ServiceNotReady status rather than bypassing entry checks.
Callback publication is atomic and admission channels cannot be reset by a
second installation. Active batches also observe the Proxy component context's
cancellation. These are coordination controls, not a new search worker pool.

## Bounds and limitations

Server defaults: 128 items, 16 workers per batch, 64 shared active calls per Proxy,
8 admitted batches, 4 MiB request budget, 16 MiB response budget, 30 second timeout.
The benchmark config raises shared permits to 128 and admitted batches to 32 so
128 logical requests can be compared with ordinary Search without a lower API cap.

Permits are released when Search returns; cancellation can return before downstream
cleanup finishes. The permit count is not proof of a hard bound on all outstanding
QueryNode work during cancellation. Retained-response bounds do not cap every
transient allocation. A batch is concentrated on one Proxy; distributed load
balancing and tenant fairness require further load testing.

## Verification and performance methodology

The functional fixture has two databases with the same four collection names,
160,000 total rows, dimensions 128/192/256, and L2/IP/COSINE FLAT indexes. Tests
compare 50-item results to ordinary Search and exercise per-database RBAC, input
isolation, item errors, limits and deadlines. Dispatcher tests run with the race
detector and the required dynamic/test tags and disabled compiler optimizations.

Performance compares the official v3.0.5 image, ordinary Search on the modified
binary, and MultiSearch on that binary. All variants perform groups of eight
identical logical searches and include an ordinary-Search probe. The Go load
generator avoids comparing a Python fan-out bottleneck with a compiled server.
The native libraries and Go version/build tags match the release. The checked-in
PGO profile is empty; release build provenance beyond binary build info was not
independently reconstructed.

Containers have a four-CPU budget, 16 GiB memory limit and identical bounded tmpfs
storage. Only the active instance uses CPUs 64-67; the inactive instance stays
alive on CPUs 68-71. The client uses CPUs 32-39. This is a CPU/read experiment,
not a durable-storage or PSC-network benchmark. Pausing containers is unsuitable:
embedded etcd leases expired in an initial discarded harness experiment.

Closed-loop capacity tests and matched offered-rate tests are separate. Record
logical throughput, group completion latency, probe latency, errors, dropped load,
CPU utilization, throttling, memory, goroutines and DQL queue gauges. The probe is
best-effort synchronous sampling, not an independent constant-arrival SLA test.
Claims must follow the recorded results and their workload limits.

A separate saturation experiment reduces the CPU quota and cpuset to two CPUs
for both binaries. GOMAXPROCS stays at four in both, making this a CPU-contention
stress case rather than a tuned two-core deployment. Its utilization denominator
is the two-core quota. These samples are reported separately from four-core runs.

## Recorded prototype results (before the ownership refactor)

At approximately 97% CPU utilization with a two-CPU budget, 128 logical requests
in flight and groups of eight searches, two reversed-order repetitions produced:

| Path | Logical searches/s | Group p99 | Ordinary Search probe p99 |
| --- | ---: | ---: | ---: |
| Stock v3.0.5 Search | 1,151.1 | 156.84 ms | 132.27 ms |
| Modified ordinary Search | 1,146.1 | 161.96 ms | 128.91 ms |
| Modified MultiSearch | 1,245.9 | 148.58 ms | 122.82 ms |

Throughput is the mean of the repetitions; p99 pools their raw samples.
MultiSearch throughput was 8.2% higher than stock in this fixture. The modified
ordinary Search path had a small observed group-p99 regression of 3.3%, with
throughput 0.4% lower. More repetitions and a matched rebuild control are needed
before attributing that difference to the API integration. CPU contention still
raises absolute latency: MultiSearch group p99 rises from about 89 ms on four
CPUs to about 149 ms on two CPUs.

Across 30 performance runs, 1,314,456 logical group searches completed with zero
recorded group/probe errors and zero dropped groups. This includes 24 four-CPU
runs and six two-CPU contention runs, totaling 990 measurement seconds. The
short runs and FLAT/nq=1/eight-item workload do not establish general performance,
tenant fairness, or long-duration leak freedom. Go runtime thread peaks were
44 for the modified binary and 45 for stock in the two-CPU tests; these do not
count every native thread.

See the [English engineering report](../../artifacts/multisearch-v2/multisearch-report.html),
[per-run measurements](../../artifacts/multisearch-v2/performance-summary.json),
and [verification scope](../../artifacts/multisearch-v2/verification-summary.json).

## Resident Search with added MultiSearch traffic (before the ownership refactor)

A further experiment keeps 128 ordinary Search callers active through baseline,
traffic injection and recovery. CPU is already approximately 97% utilized before
injection. The secondary client adds identical groups of eight searches using
individual RPCs or MultiSearch. Both variants wait for all eight before completing
a group; no scheduler priority or server binary is changed.

At 480 additional logical searches/s, resident Search retains 578.4 QPS with
ordinary-Search injection versus 625.3 QPS with MultiSearch injection. Its pooled
p99 is 286.2 ms versus 271.2 ms. Both injected workloads achieve approximately
480 logical searches/s. Resident throughput returns to approximately 1,090 QPS
after either secondary workload stops. The continuously backlogged comparison
also serves both clients; the secondary completion share is approximately 46.9%
for both API variants. The group barrier means 50/50 is not an expected invariant.

Fourteen valid runs completed 991,884 logical searches, including warmup and
transitions, with no recorded RPC/item errors, admission drops or late-generated
groups. An operator checksum command contaminated one earlier run by sharing the
tested CPU quota. Both members of that pair were excluded and rerun; the raw data
and exclusion reason are retained. These short same-identity FLAT tests support
normal resource competition in this fixture, not tenant fairness or isolation.

See the [highlighted mixed-traffic report](../../artifacts/multisearch-mixed/mixed-report.html),
[PDF](../../artifacts/multisearch-mixed/mixed-report.pdf),
[measurements](../../artifacts/multisearch-mixed/summary.json), and
[exclusion audit](../../artifacts/multisearch-mixed/excluded-runs.json).

## Proxy ownership refactor

The current implementation removes `MultiSearchExecutor` and the Server field.
`Proxy.MultiSearch` performs bounded coordination and uses the existing Search
entry checks and task scheduler. Ordinary Search execution, the DQL scheduler,
QueryNode execution, the public protocol and the Python SDK are unchanged by
this refactor. The earlier performance tables above identify the previous binary;
they are not silently attributed to the new build.

The [refactor report](../../artifacts/multisearch-proxy-refactor/report.html) and
[PDF](../../artifacts/multisearch-proxy-refactor/report.pdf) record the current
source integration, binary identity, regression checks and fresh measurements.
