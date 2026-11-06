# Streaming Reduce for Query and Search

## Problem and Decision

Batch fan-in retains complete child results before reduction. Replace that
collection step with request-scoped `ReduceStream` composition to reduce retained
input and avoid receiving unused result tails. Reuse the QueryView execution
introduced by [PR #52977](https://github.com/milvus-io/milvus/pull/52977).
Keep Batch as the disabled/unsupported fallback; streaming segment execution and
cross-request iterator sessions are outside this change.

## Data Flow

```text
QN/SN worker processes                  Proxy process
                                       (both reduction levels are in-process)
workers for vchannel 0 -- gRPC Chunks --> [vchannel ReduceStream 0] --+
workers for vchannel 1 -- gRPC Chunks --> [vchannel ReduceStream 1] --+
                                                                  |
                                                                  v
                                                      [final ReduceStream]
                                                                  |
                                                          Recv() / Close()
                                                                  v
                                             response assembly + postprocessing
                                                                  |
                                                          unary SDK response
```

[Workers](../../../internal/views/viewquery/server.go#L130) still materialize
segment results with Batch reduction, then split and send them over bidirectional
gRPC. Each [per-vchannel reducer](../../../internal/views/queryclient/shard_client.go#L124)
owns streams for its QueryPlan's workers; the
[final reducer](../../../internal/views/queryclient/legacy_client.go#L178) owns the
vchannel streams. Hybrid Search uses one such tree per sub-search, sharing the
vchannel QueryPlan, then retains existing Proxy fusion.

## Unit, Chunk, and Buffer

```text
child Chunk: [Unit][Unit] -- Recv() --> child Buffer: [Unit][Unit]
                                          |
                            one Buffer per child stream
                                          |
                                  reduction in Recv()
                                          v
                                 output Chunk: [Unit]...
```

A Unit is indivisible: a Search hit, Query row (including matched-element indexes),
or group key with partial aggregates. A Chunk is one result protobuf; a Buffer
retains its reducible data. The byte threshold `B` sums independently encoded
Unit fields, excluding result metadata and transport framing; it is not exact
message size or heap usage. Append a complete Unit before checking `B`: 80 + 30
bytes with `B=100` emits 110 bytes. A Unit larger than `B` is emitted alone,
flushing existing output first. gRPC message limits remain independent.
See [Search sizing](../../../internal/util/searchutil/reduce_stream.go#L224) and
[Query splitting](../../../internal/util/queryutil/reduce_stream.go#L703).

## Reduction and Lifecycle

`NewReduceStream()` selects the policy; `Recv()` returns one Chunk, EOF, or error.
The [shared receiver](../../../internal/util/reduce/child_streams.go#L78) permits
one outstanding receive per child and fills empty child Buffers concurrently.

| Policy | Input readiness and reduction |
| --- | --- |
| Query `OrderedReduceStream` | Each active child has a head or EOF; merge by PK and resolve shard-local duplicate PKs by timestamp. |
| Search `OrderedReduceStream` | Each active child has a head or EOF; merge by score per vector. Group selection preserves Batch pruning, not exhaustive group membership. |
| Query Group By `UnOrderedReduceStream` | Consume available Buffers into a stream-owned group map; drain all children before emitting partial aggregates. Finalize at Proxy, preserving AVG sum/count and NULL state between levels. |

Proxy `Execute()` consumes results and stops at the request's row, element, or
per-vector window, or EOF; unlimited Query drains input. Search skips surplus
hits for earlier vectors to reach later vectors. Postprocessing retains requery,
ordering, aggregation, field assembly and metadata without repeating Batch
reduction. `Close()` cancels children, joins receives and releases resources;
`Interrupt()` remains unimplemented. On a retryable ViewError, Proxy discards the
attempt's output/metadata and recreates its streams within the retry budget,
even after internal Chunks arrived. Other errors fail the attempt; streams do
not resume. See [Search](../../../internal/proxy/dql/task_search.go#L1569) and
[Query](../../../internal/proxy/dql/task_query.go#L1167) attempt ownership.

## Controls and Compatibility

| Server YAML key | Default |
| --- | --- |
| `proxy.queryView.enableReduceStream` | `false` |
| `proxy.queryView.reduceStreamChunkBytes` | `262144` |

Both [settings](../../../configs/milvus.yaml#L528) are refreshable: Proxy captures
them once before retries and propagates Chunk bytes to workers. New requests use
refreshed values without QN/SN restart; in-flight requests retain their values.
Existing configuration-source precedence applies. SDK parameters cannot override
them. Eligibility preserves existing API validation: supported ordinary/iterator
Query, ANN variants, Search grouping/aggregation and legacy Hybrid fusion can
stream. Query ORDER BY/global aggregation/function chains, Hybrid Group By and
unsupported reranking remain Batch, as does retained-plan requery. Exact gates:
[Query](../../../internal/views/queryclient/legacy_client.go#L290),
[Query function chains](../../../internal/proxy/dql/task_query.go#L1172),
[Search/Hybrid](../../../internal/proxy/dql/task_search.go#L1678).

## Limits and Verification

The target is Chunk-scale retained child input, not a hard process-memory bound:
worker materialization, group maps, source Chunks referenced by output, final
responses and gRPC run-ahead remain separate costs. Application credit control
and Batch's optional two-stage Search optimization are not implemented here.
Validate Batch parity across Chunk boundaries, NQ, NULL/element fields, grouped
partials, limits and empty results; test cancellation, retry isolation and config
refresh with race detection. ARM package/build verification is recorded in
[PR #55219](https://github.com/milvus-io/milvus/pull/55219); live SDK E2E and
performance qualification on the master base remain pending. No performance
improvement is claimed by this document.
