# Cold Search progress and stage attribution

Normal `DefaultBalancer.apply` does not open a process-wide dirty-view batch, and
`ShardViewManager.AddPreparing` acquires its DataView reference outside the
manager lock. Each `AddPreparing` still submits one complete shard event: old
views enter Dropping together with the new Preparing view. The scheduler still
serializes one shard, packs independent shards up to its existing transaction
limit, saves before syncing, and releases references only after durable
deletion. Recovery retains its explicit batch so observers and indexes are
installed before replay.

`TestBalancerApplyDoesNotHoldOtherShardEventsDuringReferenceWait` blocks one
shard's reference acquisition and requires another shard's event to flush. This
proves independence of those lanes; it does not quantify the latency improvement
or catalog write amplification on a production workload.

## Metrics and populations

Existing metric names and labels are unchanged. In particular,
`milvus_proxy_req_count` retains its database and collection labels and master's
existing optional collection-level aggregation behavior. The new stage metrics
do not change that request-counter contract. New durations use seconds:

- `milvus_qv_stage_duration_seconds{component,operation,stage,result}`
- `milvus_qv_stage_inflight{component,operation,stage}`
- `milvus_qv_stage_items_total{component,operation,stage,kind}`
- `milvus_qv_request_stage_duration_seconds{operation,path,latency_class,stage,result}`
- `milvus_qv_flush_oldest_pending_seconds`

Labels for these stage families are fixed enums. No collection, shard, segment,
node, request ID, error string, or manifest path becomes a stage label. Histogram
result series are created on first observation and then cached. Inflight starts
at Begin, including hangs. Stages that reuse existing completed intervals via
Observe have no inflight series; absence is not zero. `result` distinguishes success, error,
canceled, timeout, superseded, and expected `not_ready` checks. Embedded RPC
Status is inspected; a nil Go error is not sufficient to classify success.
Go cancellation outcomes recognize context/gRPC cancellation, operator request
cancellation, and the segcore FollyCancel code after the CStatus/Status round trip.
Errors whose upstream API has erased that identity remain `error`; this PR does
not rewrite upstream error policy. Native timers use the coarser success/error
split, with cancellation included in error. A deadline converted to a native
cancellation cannot be distinguished from other cancellations at that boundary.

| Component / operation | Stages | Unit and boundary |
| --- | --- | --- |
| Proxy / Search, HybridSearch, Query | request | One logical Proxy method, including readiness and early failure |
| Request cohort histogram | total, readiness, execution, retry_wait | Same completed requests; total = readiness + execution + retry_wait, including zero execution on readiness failure |
| Coord / auto_load | shared_lifecycle, load_submit, ready_wait, caller_wait | QueryCoord owns automatic loading. Lifecycle stages count once per singleflight worker; caller_wait counts each EnsureCollectionReady RPC that joined it |
| Coord / readiness | check | One EnsureCollectionReady fast-path check; `not_ready` when the caller must join a load |
| Coord / reconcile | trigger_wait, snapshot, plan, apply, total | Coalesced trigger batch's oldest enqueue; one reconciliation for the others |
| Coord / apply | ensure, prepare, release, shard_lock_wait, shard_lock_hold, pin_dataview, publish_stats | One prepare/release; pin_dataview runs outside the manager lock, and the lock stages cover the critical section that follows it |
| Coord / flush | pending_total, batch_hold, same_shard_wait, eligible_wait | One merged pending shard cohort; original enqueue survives merges; reasons are exclusive |
| Coord / flush | pack, catalog_save, after_persist, sync_enqueue, execute | One claimed batch; catalog_save/views counts attempted persisted entries |
| Coord / view_load | created_to_persisted, persisted_to_all_ready, all_ready_to_up_enqueue, up_enqueue_to_confirmed, total | One locally created view, first boundary only; terminal completion/preemption/release removes state |
| Coord / sync | send_queue, send, callback, apply_report | One pending target until first queue drain; Send is a batch; callbacks are per response |
| Coord / sync_preparing, sync_up, sync_cleanup | roundtrip | First enqueue to matching worker response; includes queueing and worker work; survives reconnect |
| Coord / load_info | build_snapshot | One GetQueryViewSegmentLoadInfo batch |
| QueryNode / load_info | fetch | One segment resource's load-info fetch; includes batching and transport |
| QueryNode / view_load | prepare | View resources acquired through local readiness; aborted prepares are counted and removed |
| QueryNode / prepare | collection_runtime, collection_metadata | Collection runtime acquire and metadata load attempt |
| QueryNode / segment_load | total, reserve, physical_load, release_reservation, on_loaded | One segment read preparation attempt; physical_load only when no reusable base exists; on_loaded prepares the read state |
| QueryNode / physical_load | new_segment, load_segment, delta, pk | Actually entered phase of one segment load attempt |
| QueryNode / segment_create | pool_wait, collection_lock_wait, collection_lock_hold, convert_load_info, marshal, cgo_create, commit_timestamp, initialize | Each entered call; cgo wall time is not C++ body time |
| Storage / manifest | total, properties, transaction_begin, get_manifest, extract_stats_total | Go/FFI calls, not a claim of pure network time; extract_stats_total contains manifest access |
| StreamingNode / view_load | prepare, catalog_save | View resource acquire to Ready; actual SaveQueryViews call returns before catalog_save ends |
| StreamingNode / prepare | runtime_initialize, data_version | Runtime initialization/version preparation, including errors |
| StreamingNode / bm25_stats | ensure_materialized, shared_wait, load, read_decode | Query-time lazy materialization, wait by a caller joining another's materialization, per-segment load, per-file streaming read and decode |
| Proxy / shard_query | get_plan, fanout, node_dispatch | Each shard attempt and work-node dispatch; attempt/attempts counts retries too |
| StreamingNode / query_plan | total, lease, mvcc, global_optimize, build_nodes | One plan RPC; global optimizer includes lazy BM25 work |
| QueryNode / query_acquire; StreamingNode / query_acquire | lease, local_optimize, visibility_wait, segment_handles | Each Search/Query acquire path |
| Worker / Search | total, acquire_tasks, execute_tasks | Shared SearchOnView execution code; includes QN and SN, distinguish deployment via target labels |
| QueryNode / search_task | total, prepare_request, segment_search, reduce_total, prepare_export, arrow_export, unattributed | Execution on selected segments; primary sequential phases are prepare_request, segment_search, reduce_total; cleanup/glue is unattributed |
| QueryNode / reduce | heap_merge, marshal_reduce, fill_output_fields, decode_output_fields, encode_result | One entered reduce/output call; multiple slices may run per task |

`retry_wait` is the retry library's backoff and retry-decision interval between
attempts. `path` is the readiness state QueryCoord observed for this caller and
returned in `Status.ExtraInfo["readiness_path"]`: unloaded, loading, or ready. It
is disabled when automatic loading is off and unknown when the coordinator does
not report it. A retried request keeps the coldest path it observed. It is not an
assertion that the caller won singleflight or that native caches are warm.
`latency_class` is `le1s`/`gt1s`, chosen from that same request's total at
completion. Comparing readiness, execution, and retry_wait for `gt1s` accounts
for the whole slow-request cohort and forms its exact partition even when one
logical request makes multiple attempts.

Different rows have different populations and parallelism. Never sum per-segment
latencies into request latency, sum stage P99s, or average pod P99s. Parent stages
contain children. The same-cohort request phases and the exclusive flush wait
reasons are the intended additive groups. Search task `unattributed` reports
elapsed time outside its three main sequential phases, including cleanup.

## Pending work, request summaries, and limits

All new stage measurements write directly to Prometheus metrics. They cover
operations regardless of whether tracing is configured. The metrics timers do
not create spans or trace links, change trace context, or require an OTel provider.
Existing tracing outside this patch is unchanged. Cross-service per-request
critical paths remain outside this patch's aggregate metrics.

Slow (>1 second) and unsuccessful completions are candidates for a request
summary. A process-wide limiter allows 2/second with burst 10;
`slow_summary/{candidate,emitted,dropped}` exposes its coverage. Summaries do not
replace whole-population histograms or retain every slow request.

View timestamps never reset on repeated progress reports. Recovered views lack
an original Created timestamp and are excluded from new-view latency. A worker
callback can race the return of SyncViews: if boundaries are missing/out of
order, `view_load/coverage/incomplete_order` increments and total is still
recorded; an artificial timestamp is never inserted. This coverage counter is
for completed, locally created views, not all recovered/resident views. Release,
preemption, node failure, coordinator close, and SN handoff clean active timers.
A crashed process cannot emit terminal observations: use inflight/oldest age
before the crash and process restart metrics alongside durations.

The dashboard also references existing native cache load and storage metrics.
Those measure their existing populations and success semantics. Native cache
lookup vs waiting for another loader, pure remote I/O vs FFI/decode, native
manifest column-group access, and client/SDK/network time do not yet have a
complete correlated breakdown in this patch. The native cache implementation
is supplied by milvus-common. A green stage dashboard must not be described as
100% client-to-storage attribution. Use the explicit unattributed interval and
same-window native cache/storage/CPU profiles to identify the remaining work;
extending those native internals needs a separately validated dependency change.

## Verification and rollout

Tests cover independent shard progress, durable persist-before-sync, deletion
before unpin, same-shard serialization, recovery, pending-cohort timing, repeated
lifecycle events, incomplete boundary order, aborted prepares, embedded RPC
errors, the readiness path report, and the same-request phase partition.
The timer benchmark measures a warmed metrics recorder; it does not measure
scrape overhead or full-load impact.

Before evaluating a deployment, import `cold-search-dashboard.json`, select the
Prometheus data source/instances, and retain one identical workload window for
both versions. Compare logical request totals and slow cohorts first, then view
completion, flush write count/batch size, etcd latency, and node loading/execution.
The patch does not deploy itself or produce a new performance result. Native
cache misses at query time and collection readiness are separate axes.

## Node scheduler telemetry

The scheduler exports `milvus_node_scheduler_concurrency`,
`pending_task_count`, `running_task_count`, `task_queue_duration_seconds`,
`task_execution_duration_seconds`, and `task_execution_total` under the same
`milvus_node_scheduler_` prefix. Capacity is summed across live scheduler
instances on the node; resize and close adjust only that instance's contribution.
Task series have fixed concrete task types (or a stable wrapper-provided type),
not request identifiers. The existing node ID label identifies the process.
QN can create its scheduler before session registration. Gauge contributions
move to the assigned node ID on the next update (or the five-second reporter
for an idle scheduler); observations already recorded before registration keep
their original label.

Pending counts and queue duration include delayed retry backoff. Queue duration
ends when a task is dequeued for execution or canceled while pending. Execution
counters count attempts with success, retry, fail, or cancel outcomes; a task
canceled before execution contributes no execution attempt. Five-second summary
logs report per-type counts and queue/execution durations. This instrumentation
preserves master's priority ordering, fairness and cumulative worker limits.
Changing a queued task's priority or promoting a delayed retry does not count
another enqueue or reset its telemetry wait interval. Metrics remain aggregated
by task type; they do not add priority labels or per-band capacity gauges.
