# Streaming Reduce for Query and Search

## Overview

This document describes the implementation submitted in
[PR #55219](https://github.com/milvus-io/milvus/pull/55219). The overview is followed
by the detailed contracts and execution flow. Deferred work is explicitly marked;
it is not part of the current runtime contract.

### Problem and Decision

Batch fan-in retains complete child results before reduction. Replace that
collection step with request-scoped `ReduceStream` composition to reduce retained
input and avoid receiving unused result tails. Reuse the QueryView execution
introduced by [PR #52977](https://github.com/milvus-io/milvus/pull/52977).
Keep Batch as the disabled/unsupported fallback; streaming segment execution and
cross-request iterator sessions are outside this change.

### Data Flow

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

### Unit, Chunk, and Buffer

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
group key with partial aggregates, or global aggregate row. A Chunk is one result protobuf; a Buffer
retains its reducible data. The byte threshold `B` sums independently encoded
Unit fields, excluding result metadata and transport framing; it is not exact
message size or heap usage. Append a complete Unit before checking `B`: 80 + 30
bytes with `B=100` emits 110 bytes. A Unit larger than `B` is emitted alone,
flushing existing output first. gRPC message limits remain independent.
See [Search sizing](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L224) and
[Query splitting](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L726).

### Reduction and Lifecycle

`NewReduceStream()` selects the policy; `Recv()` returns one Chunk, EOF, or error.
The [shared receiver](../../../internal/util/reduce/reduce_stream.go#L78) permits
one outstanding receive per child and fills empty child Buffers concurrently.

| Policy | Input readiness and reduction |
| --- | --- |
| Query `OrderedReduceStream` | Each active child has a head or EOF; merge by PK and resolve shard-local duplicate PKs by timestamp. |
| Plain Query ORDER BY | Each vchannel drains workers into a PK map, resolves versions, sorts and selects candidates before emitting Chunks. Final merge uses scalar ordering and rejects cross-vchannel duplicate PKs. |
| Search `OrderedReduceStream` | Each active child has a head or EOF; merge by score per vector. Group selection preserves Batch pruning, not exhaustive group membership. |
| Query Group By `OrderedReduceStream` | Merge all child heads with the least full group key into one Unit. Stop at `offset + limit` complete groups or EOF. |
| Global Query aggregation `UnOrderedReduceStream` | Merge available partials into one retained partial; drain all children before emitting one aggregate row. Preserve AVG sum/count and NULL state until Proxy finalization. |

Proxy `Execute()` consumes results and stops at the request's row, element, or
per-vector window, or EOF; unlimited Query drains input. Plain Query ORDER BY
drains finalized vchannel candidates to validate duplicates, retaining only its
requested window. Search skips surplus
hits for earlier vectors to reach later vectors. Group By preserves the full key order through pagination. Search bypasses repeated Batch
candidate reduction; Query passes its merged result through existing pagination
and aggregate finalization. `Close()` cancels children, joins receives and releases
resources; `Interrupt()` remains unimplemented. On a retryable ViewError or gRPC
`UNAVAILABLE`, Query and ordinary Search can replan a failed vchannel before it
emits any Chunk. After output, after shard retries are exhausted, and for Hybrid
receive failures, Proxy discards the attempt's output/metadata and recreates all
shard streams within its retry budget. Other errors fail the attempt; streams do
not resume from a partial position. See [Search](../../../internal/proxy/dql/task_search.go#L1569) and
[Query](../../../internal/proxy/dql/task_query.go#L1167) attempt ownership.

### Controls and Compatibility

| Server YAML key | Default |
| --- | --- |
| `proxy.queryView.enableReduceStream` | `true` |
| `proxy.queryView.reduceStreamChunkBytes` | `262144` |

Both [settings](../../../configs/milvus.yaml#L528) are refreshable: Proxy captures
them once before retries and propagates Chunk bytes to workers. New requests use
refreshed values without QN/SN restart; in-flight requests retain their values.
Existing configuration-source precedence applies. SDK parameters cannot override
them. Eligibility preserves existing API validation: supported ordinary/iterator
Query, ANN variants, Search grouping/aggregation and legacy Hybrid fusion can
stream, including Query Group By + ORDER BY, global aggregates, valid QueryScan
function chains, ordinary ungrouped L0/L1/L2 reranking, and legacy Hybrid Group By.
Unsupported reranking remains Batch, as does retained-plan requery. Exact gates:
[Query](../../../internal/views/queryclient/legacy_client.go#L290),
[QueryScan validation](../../../internal/proxy/dql/query_function_chain.go#L29),
[Search/Hybrid](../../../internal/proxy/dql/task_search.go#L1678).

### Limits and Verification

The target is Chunk-scale retained child input, not a hard process-memory bound:
worker materialization, group maps, source Chunks referenced by output, final
responses and gRPC run-ahead remain separate costs. Application credit control
is not implemented here. Eligible ordinary Search can reuse the Batch two-stage
filter preparation before opening ANN streams; filter metadata is merged once
per worker, including when the ANN result is empty.
Validate Batch parity across Chunk boundaries, NQ, NULL/element fields, grouped
partials, limits and empty results; test cancellation, retry isolation and config
refresh with race detection. ARM package/build verification is recorded in
[PR #55219](https://github.com/milvus-io/milvus/pull/55219); live SDK E2E and
performance qualification on the master base remain pending. No performance
improvement is claimed by this document.

## Detailed Design

1. [Architecture and alternatives](#1-architecture-and-alternatives)
2. [APIs and ownership](#2-apis-and-ownership)
3. [Unit and byte contracts](#3-unit-and-byte-contracts)
4. [Receive scheduling and implementation comparison](#4-receive-scheduling-and-implementation-comparison)
5. [Ordered Query](#5-ordered-query)
6. [Ordered Search](#6-ordered-search)
7. [Query Aggregation](#7-query-aggregation)
8. [Worker-to-Proxy execution](#8-worker-to-proxy-execution)
9. [Metadata and lifecycle](#9-metadata-and-lifecycle)
10. [Configuration and supported paths](#10-configuration-and-supported-paths)
11. [Memory and backpressure](#11-memory-and-backpressure)
12. [Validation and deferred work](#12-validation-and-deferred-work)

### 1. Architecture and Alternatives

Both modes use QueryView planning and the same worker-side search/query execution.
The difference is how complete worker results enter Proxy and are reduced there.
The diagrams show result flow, not request routing. A worker can participate in
more than one vchannel; these are QueryPlan-selected inputs, not dedicated nodes.

```text
BATCH
QN/SN processes                         Proxy process
[worker 0: complete result] -- unary --> [complete child result 0] --+
[worker 1: complete result] -- unary --> [complete child result 1] --+
                                                                  v
                                            vchannel/final Batch reduction
                                                                  |
                                                   assemble unary SDK response

STREAMING
QN/SN processes                         Proxy process
[worker 0: complete result] -- Chunks -> [child Buffer 0] --+
[worker 1: complete result] -- Chunks -> [child Buffer 1] --+
                                                         v
                                               vchannel ReduceStream
                                                         |
                                                       Chunks
                                                         v
                                                  final ReduceStream
                                                         |
                                               assemble unary SDK response
```

Workers still compute a complete result and currently build the list of outgoing
Chunks before sending it. This change does not reduce segment-search work or
promise lower worker memory. Proxy can consume and reduce child input as it
arrives and cancel unnecessary tails. Both reduction levels are inside Proxy;
the boundary between them is an in-memory `Recv()`, not another RPC.
See [worker handlers](../../../internal/views/viewquery/server.go#L130) and
[stream composition](../../../internal/views/queryclient/legacy_client.go#L178).

| Alternative | Decision |
| --- | --- |
| Keep complete-result Batch fan-in | Retained as fallback, but it does not address full child-result retention. |
| Stream transport, then collect every Chunk before reduction | Rejected as the primary design: it recreates the same complete-input retention. |
| Compose request-scoped ReduceStreams | Selected; reuse worker execution and existing Proxy postprocessing. |
| Stream segment execution or persist sessions across SDK calls | Deferred; requires separate native execution and lifecycle changes. |

### 2. APIs and Ownership

Search and Query share one generic `reduce.ReduceStream[T]` interface. Their
package-level `ReduceStream` names are aliases specialized for the corresponding
protobuf result type, preserving compile-time type checking and existing callers.
Transport-backed children, in-process reducers and stream wrappers implement the
same contract. `ChildStreams` is a receive scheduler, not another stream type.

Operation-specific reduction implementations and their unit tests live in
`internal/util/queryutil/reduce_stream` and
`internal/util/searchutil/reduce_stream`, both named `reducestream`.
Their main implementation files are `query_reduce_stream.go` and
`search_reduce_stream.go`. The shared contract and child scheduling live in
`internal/util/reduce/reduce_stream.go`; `grpc_reduce_stream.go` in the same
package owns the shared transport lifecycle with typed Query and Search constructors.
Callers use the aliases `queryreducestream` and `searchreducestream` below.
Query streaming imports shared Query helpers from `queryutil`; Batch operators
remain in that parent package, which does not import the stream subpackage.
`queryutil/reduce_stream_helpers.go` exposes forwarding functions and a row
reference alias, preserving the existing private helpers and Batch callers.
Common receive scheduling and byte sizing remain in `internal/util/reduce`.

```text
reduce.ReduceStream[T]:
    Recv()      -> (T, error)
    Close()     -> error
    Interrupt() -> (T, error)     // currently returns an unimplemented error

searchreducestream.ReduceStream = reduce.ReduceStream[*internalpb.SearchResults]
queryreducestream.ReduceStream  = reduce.ReduceStream[*internalpb.RetrieveResults]

reduce.NewSearchGRPCReduceStream(ctx, client, request)
reduce.NewQueryGRPCReduceStream(ctx, client, request)

searchreducestream.NewReduceStream(request, childStreams, chunkBytes)
searchreducestream.NewShardReduceStream(request, childStreams, chunkBytes)
queryreducestream.NewReduceStream(request, schema, childStreams, chunkBytes)
queryreducestream.NewShardReduceStream(request, schema, childStreams, chunkBytes)
```

`NewShardReduceStream` enables the vchannel-level duplicate policy. The final
factory composes vchannel streams. Only global Query aggregation uses `UnOrderedReduceStream`.
Eligible Search candidate reduction uses `OrderedReduceStream`, including Search
Group By. Hybrid is decomposed before the factory, not merged as one ANN request.
Sources: [Search factories](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L369),
[Query factories](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L224).

| Owner | Owned objects and responsibility |
| --- | --- |
| Proxy task | Request settings, attempt output/metadata, final stream(s), stopping and retry decisions. |
| `legacyClient` | Resolves vchannels and constructs final stream(s); returns plans for existing postprocessing/requery. |
| `shardViewQueryClient` | Uses `executeShard` to plan and open worker streams; constructs each vchannel stream. |
| Each reducer | `childStreams`, `childBuffers`, policy-specific output Buffer and metadata. |
| `reduce.ChildStreams` | Per-child receiving/drained flags, completion channel, cancellation and pending receives. |
| gRPC-backed child | One request-scoped RPC and its cancellation function, not the shared client connection. |

`Recv()` calls on a given stream must be serial. `Close()` may run concurrently
to interrupt a blocked receive. Buffer mutation is serialized by the reducer's
`recvMu`; receive goroutines publish completions rather than mutating Buffers.
There is no additional generic `reduceState` object. Sources:
[shared contract](../../../internal/util/reduce/reduce_stream.go#L31),
[Search contract](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L38),
[Query contract](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L45),
[child receive state](../../../internal/util/reduce/reduce_stream.go#L46).

### 3. Unit and Byte Contracts

The same design terms apply at worker, vchannel and final reduction boundaries.

| Term | Meaning |
| --- | --- |
| Unit | Smallest indivisible reduction value. Represented by existing row/hit references or a grouped partial row, not a new public interface. |
| Chunk | One Search/Query result protobuf containing complete Units. On gRPC it is carried in a `ViewQueryService` stream response envelope. |
| Buffer | Reducer-owned data retained between consumption steps: row/hit references and a cursor, or a map for grouped accumulation. |

```text
WORKERS                     PROXY: vchannel stream          PROXY: final stream
child 0 -- Chunk [U][U] ---> child Buffer 0 [U][U] --+
child 1 -- Chunk [U] ------> child Buffer 1 [U] -----+-> Recv()
                                                       |
                                                  Chunk [U][U]
                                                       |
                                                       +-> child Buffer 0 --+
other vchannel streams ............ Chunks ..............-> child Buffers ...+-> Recv()
                                                                            |
                                                                       final Chunk
```

The dotted path is the same relationship for non-focused vchannels. Every
reducer owns one input Buffer per direct child; a vchannel stream is itself a
child of the final stream. Chunks are transferred values, not the mutable
accumulation map itself. Group output is encoded into protobuf columns.

#### Byte accounting

| Unit | Reducible payload included in its accounted bytes |
| --- | --- |
| Search hit | PK, score, returned fields, optional element index and group fields. |
| Query row | PK when present, returned fields, computed columns when present, optional matched-element indexes. |
| Grouped Query partial | Group columns and partial aggregate columns. |

Sizing reuses the existing row size calculator: fixed-width values count by
width, variable-width values by length, and nested array cells use their
protobuf size. There is no separate charge for validity flags, outer field
descriptors, result metadata or transport framing. The Chunk threshold is
a payload target, not a wire-size limit or a heap bound.

The [row size calculator](../../../internal/util/queryutil/helpers.go) reads
the existing columns without constructing a temporary one-row result.
Ordered sizing reuses one nullable-vector cursor per source Chunk; the
existing Batch quota keeps its own accounting and index preparation.

For threshold `B`, all three policies preserve complete Units:

```text
outputBytes = sum(outputBuffer Units' accounted sizes)
unitBytes   = next Unit's accounted size

if unitBytes > B and outputBuffer is not empty:
    emit outputBuffer; leave next Unit for the next Chunk
else:
    append the complete next Unit
    outputBytes += unitBytes
    if outputBytes >= B:
        emit outputBuffer
```

With `B=100`, sizes `80, 30` produce one 110-byte accounted Chunk. Sizes `80, 150`
produce an 80-byte Chunk followed by the 150-byte Unit alone. Input exhaustion,
an oversized next Unit, or Search's last-vector yield can produce a smaller
Chunk. A metadata-only Chunk is valid; it is not EOF. No Unit is truncated.
Query timestamp replacement has an additional trailing-Unit case described below.

The worker splitters apply the same threshold. They validate a positive byte
value, but the receiving reducers do not enforce a separate maximum incoming
Chunk byte budget. This is a production call-site contract, not a protection
against arbitrary oversized inputs. gRPC message-size limits remain independent.

### 4. Receive Scheduling and Implementation Comparison

The common lifecycle is receive, reduce into an output Buffer, then encode a
Chunk. An ordered reducer needs a head or EOF from each child; a global aggregate needs every contribution before it can emit.

```text
                         parent calls Recv()
                                 |
                         check closed / finished
                                 |
                  +----> getReadyBuffers()
                  |              |
                  |  consume according to policy
                  |              |
                  +-- no -- output ready?
                                 |
                                yes
                                 |
                    build Chunk + attach metadata
                                 |
                            return Chunk

STEP              ORDERED QUERY       ORDERED SEARCH       QUERY GROUP BY
receive           all active heads    all active heads     all active heads
consume           lowest PK/version   best score/vector    heads with least full key
accumulate        row references      hit references       one complete group per Unit
ready             byte boundary*      byte/window yield*   byte boundary*, limit or EOF
emit              RetrieveResults     SearchResults        partials as Chunks

* Also respects indivisible Units and exhaustion; Query resolves the trailing PK.
```

`childStreams[i]` means the direct input stream at index `i`, and
`childBuffers[i]` stores that input's retained data. No ordering is implied by
the index. The actual shared methods are `ReceiveUntilReady` and
`ReceiveAnyReady`; `getReadyBuffers()` supplies their policy-specific callbacks.

The following is a control-flow sketch of the existing receiver, not a new API:

```text
ReceiveUntilReady(hasUnit, acceptChunk):
    repeat:
        return cancellation error if done is closed
        allReady = true
        for i, stream in streams:
            if drained[i] or hasUnit(i):
                continue
            allReady = false
            if not receiving[i]:
                receiving[i] = true
                start one pending receive:
                    chunk, err = stream.Recv()
                    completions <- {childIndex: i, chunk: chunk, err: err}
        if allReady:
            return
        receive completion, or return cancellation error
        receiving[completion.childIndex] = false
        if completion.err == EOF:
            drained[completion.childIndex] = true
        else if completion.err != nil:
            return completion.err
        else:
            acceptChunk(completion.childIndex, completion.chunk)
```

`ReceiveAnyReady` uses the same completion/EOF/error handling. Its scan skips
drained children, returns immediately if a scanned Buffer already has input,
and otherwise starts one receive for each encountered child without a pending
receive. It returns when an input is accepted on a subsequent scan, or all
children drain. It does not wait for a slow child to establish cross-child order.
This is not a round-robin consumption contract or an application credit policy.

The completion channel carries `{childIndex, chunk, err}` from concurrent
receives back to the owning reducer. Its capacity is at least the number of
children, and at most one receive per child is outstanding. The reducer accepts
and validates a completion before using the data. Ordered metadata-only Chunks
are accepted without satisfying the need for a row head, so receiving continues.
Source: [child scheduling](../../../internal/util/reduce/reduce_stream.go#L37).

The older design's `B + y` prefetch Buffer is not implemented. Current ordered
receiving refills empty Buffers; there is no configurable low-watermark `y`, nor
direct concurrent writes into the row Buffers. Cancellation and cleanup are
specified in Section 9.

### 5. Ordered Query

Plain Query results without explicit ORDER BY are PK-ordered.
`NewShardReduceStream` resolves worker duplicates by timestamp.
The final stream checks the cross-vchannel PK contract rather than applying the
worker version-resolution policy a second time.

The core selection in `Recv()` is as follows. Error checks and metadata handling
are omitted from this sketch only; they remain in the implementation.

```text
outputBuffer = retained outputBuffer from the previous call, or empty
outputBytes = sum(unit.byteSize for unit in outputBuffer)

while deduplicatePKs or outputBytes < chunkBytes:
    readyBuffers = getReadyBuffers()
    if readyBuffers is empty:
        break
    winner = readyBuffers[0]
    for candidate in readyBuffers[1:]:
        if compareRetrieveUnits(candidate.front(), winner.front(), orderByFields) < 0:
            winner = candidate

    selected = winner.front()
    if deduplicatePKs and outputBuffer not empty and selected.pk() == lastPK:
        unit = winner.pop()
        if unit.timestamp() != 0 and unit.timestamp() > outputBuffer.last.timestamp():
            replace trailing Unit; update outputBytes
        continue
    if outputBytes >= chunkBytes:
        break
    unitBytes = selected.reducibleByteSize()
    if unitBytes > chunkBytes and outputBuffer not empty:
        break
    unit = winner.pop()
    unit.byteSize = unitBytes
    check PK consistency; update lastPK
    append unit to outputBuffer; add unit.byteSize to outputBytes

if trailing replacement became oversized and earlier Units exist:
    retain trailing Unit for the next Recv()
return buildRetrieveChunk(outputBuffer), or metadata-only Chunk / EOF
```

A byte boundary cannot freeze the trailing PK too early. For example, worker A
provides `PK=7, timestamp=10` at the end of one Chunk, while another input Chunk
contains `PK=7, timestamp=20`. The shard reducer continues resolving that PK
before emitting it. A replacement can change both row bytes and element indexes;
if it becomes oversized, it is retained and emitted alone on the next call.
Source: [Query Recv](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L286).

For `element_filter`, one Unit is still one document row with all matched indexes.
The requested page length counts elements, not document rows. Proxy assembles
complete Units, then the existing pagination logic trims the element-index list.
Nullable scalar/vector validity and element-level flags must survive splitting,
merging and output assembly. Sources:
[element validation](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L174),
[Proxy consumption](../../../internal/proxy/dql/task_query.go#L1386).

QueryScan uses the same PK-ordered reduction. Computed columns stay in
`RetrieveResults.ComputedResults` and follow the selected row, including timestamp
replacement, Chunk splitting and final pagination. Incoming columns are validated
once per Chunk and aligned by name/type, not collection field ID or column order.
Reducers retain empty typed column templates so an all-empty result preserves its
computed schema; consumed input rows are not retained for this purpose. Computed
payload contributes to Unit bytes and Proxy's output-size quota. Proxy appends it
separately from ordinary fields, then the existing Query pipeline publishes it
after schema-based processing. QueryScan's existing validation exclusions remain
unchanged. Sources: [computed helpers](../../../internal/util/queryutil/computed_results.go),
[QueryScan validation](../../../internal/proxy/dql/query_function_chain.go#L29).

The PK-ordered reducer has no request row-limit stopping counter. Proxy stops pulling at its
row/element window, or reads to EOF for valid unlimited Query. Iterator semantics
add a separate correctness boundary: when `ShouldStopWhenDrained(reduceType)` is
true and a drained child reported `HasMoreResult`, the reducer ends this page
rather than skipping that child's unseen later rows. `HasMoreResult` describes
rows outside the child's current result, not whether another Chunk is in flight.
See [getReadyBuffers](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L407).

Plain Query ORDER BY cannot emit worker heads immediately. Sorting by another
column can separate old and new versions of one PK, invalidating rows already
emitted. For example:

```text
ORDER BY price ASC LIMIT 2
worker 0: A(price=10, ts=100), B(price=20)
worker 1: C(price=25),         A(price=30, ts=200)
early ordered merge: A, B     correct version-resolved result: B, C
```

The vchannel's first `Recv()` drains all workers before exposing any row:

```text
worker Chunks -> PK -> newest timestamp row
                 |   replace only for a nonzero, strictly newer timestamp
                 v
             all workers at EOF
                 |
                 v
             ORDER BY + PK tie-break -> first (offset + limit) rows
                 |
                 v
             byte-sized Chunks -> final scalar-ordered merge
```

The PK map is updated as each input Buffer is consumed. It retains one selected
row reference per PK rather than a list of all received Chunks. References may
keep source Chunks alive. Equal or missing timestamps preserve the first selected
row, matching Batch; no topK pruning occurs before version resolution completes.
The existing `OrderByLimitOperator` then applies scalar ordering, NULL placement,
PK tie-breaking and the per-vchannel candidate limit. Sources:
[vchannel accumulation](../../../internal/util/queryutil/reduce_stream/order_by_stream.go),
[Batch timestamp rule](../../../internal/util/queryutil/dedup_pk_op.go#L124),
[sorting](../../../internal/util/queryutil/order_op.go#L76).

The final reducer rejects duplicate PKs across vchannels using a set, since equal
PKs need not be adjacent under scalar ordering. Proxy drains that final stream
even after collecting `offset + limit` rows, so duplicates beyond the page still
fail the request; it does not retain surplus rows. Existing Proxy pagination
applies the offset once. This qualifies only bounded plain Query ORDER BY.
Group By + ORDER BY uses the grouped path described below. ORDER BY without a limit is rejected by
shared Query validation before Batch/Streaming selection: segment-side sorting
can materialize all matching rows, and Proxy's output quota does not protect that
stage. Valid unlimited plain Query without ORDER BY remains streaming-eligible.
Sources: [eligibility](../../../internal/views/queryclient/legacy_client.go#L290),
[ORDER BY validation](../../../internal/proxy/dql/task_query.go#L905).

### 6. Ordered Search

Search Chunks contain flattened hits partitioned by vector through `Topks` and
`NumQueries`. Input is query-major: vector 0's hits precede vector 1's hits.
The reducer validates counts and metric consistency, keeps a cursor per child,
and processes one `currentQuery` at a time.

The existing method sequence is preserved here; it is not a new policy framework:

```text
Recv():
    outputBuffer = allocBuffer()
    while not isChunkReady(outputBuffer):
        readyBuffers = getReadyBuffers()
        oneReduceResult, chunkReady = produceNextUnits(readyBuffers, outputBuffer.byteSize)
        merge(outputBuffer, oneReduceResult)
        if chunkReady:
            break
    if outputBuffer.isEmpty():
        finish and close children; return pending metadata-only Chunk or EOF
    chunk = createOutputChunk(outputBuffer)
    attachMetadata(chunk)
    return chunk
```

`produceNextUnits()` selects the largest internal score among heads for
`currentQuery`, breaking equal scores by PK. It uses the existing normalized
score convention; Proxy restores metric direction in the final response. The
shard reducer deduplicates by PK, or `(PK, element_index)` for element results.
After consuming a duplicate or rejected group candidate, it returns to receiving
before choosing another head, so an emptied child cannot be ignored.
Sources: [Recv](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L428),
[selection](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L622),
[response assembly](../../../internal/proxy/dql/task_search.go#L1763).

#### Per-vector skipping versus request completion

For an earlier vector whose candidate window is full, the reducer discards that
vector's surplus input until every active child reaches the next vector or EOF.
This is necessary to reach later vectors with the query-major format. It is not
permission to declare the whole request complete.

For the last vector, the reducer yields a Chunk at the window boundary even if
its byte threshold is not reached. Proxy decides not to call `Recv()` again and
closes the tree. Calling `Recv()` again does not turn that window boundary into
automatic EOF. Earlier vectors may legitimately have fewer hits; Proxy's final
vector/window check relies on query-major delivery. Sources:
[skip/yield logic](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L622),
[Proxy stop condition](../../../internal/proxy/dql/task_search.go#L1887).

#### Search variants

| Variant | Reduction and downstream behavior |
| --- | --- |
| ANN, scalar filtering, range or iterative-filter | Workers keep their existing candidate/filter behavior. ReduceStream merges their returned candidates by score. |
| Element/embedding-list Search | Retain element identity through Chunks and shard deduplication; existing downstream collapse/assembly remains. |
| Search ORDER BY | Retain the ANN candidate window without early scalar-order pagination. Existing Proxy requery/order/pagination follows score reduction. |
| Search Group By | Track group keys and accepted member counts per vector across Chunks, while selecting candidates in score order. |
| Search aggregation | Reduce grouped ANN candidates, then run the existing Proxy aggregation computation over those candidates. |

For Group By, each shard uses `topK` groups and `group_size` members, without the
final group offset. The final stream applies group offset and member limits.
Composite keys compare their actual values after hashing. Ordinary grouped output
is arranged into contiguous groups at Proxy; composite-key Search aggregation
retains the accepted-hit walk order used by Batch. A lower-level discarded group
member cannot be recovered when its group later wins globally. This deliberately
matches Batch intermediate pruning; it is not exhaustive global group selection.

Search aggregation uses the existing hierarchy-derived candidate window and
group fields. Unlike Query Group By, it does not compute aggregates over
matching rows that workers did not return as candidates. Sources:
[group selection](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L685),
[candidate assembly](../../../internal/proxy/dql/task_search.go#L1887),
[Proxy aggregation](../../../internal/proxy/dql/search_pipeline.go#L668).

### 7. Query Aggregation

Batch and ReduceStream use the same Query GROUP BY order. Without ORDER BY,
both compare the group keys in declaration order (ascending, NULLs last).
An explicit ORDER BY takes precedence; remaining group keys break ties.
Workers apply this order before selecting their local top K, and Proxy applies
it before pagination. No implicit ORDER BY is added to the request. For
keys-only Batch queries, the worker now merges distinct groups before choosing
the first K, rather than retaining any K; the existing group cap still applies.

```text
QN 1: (A,2) (C,1) --> vchannel 1: (A,5) (B,4) (C,1)
QN 2: (A,3) (B,4) --/                     |
                                          +--> Proxy, LIMIT 2: (A,6) (B,4); close
                      vchannel 2: (A,1) (D,8) --/
```

Both ordered reducers wait for a head or EOF from every child. Each child emits
one row per full group key, so all contributions to the least key are at the
heads. The reducer merges them into one Unit; it need not read another Chunk
to prove the group complete. AVG stays as SUM and COUNT until Proxy maps the
output columns. Empty grouped input yields typed columns with zero rows.

Both levels stop at the internal `offset + limit`; only Proxy skips the offset.
The shared Proxy pipeline re-sorts that bounded prefix before slicing the page,
without reading more groups from the stream.
An unbounded merge drains to EOF. The group cap still fails the query when a
reducer visits too many distinct groups. It does not trigger a scan of an unread
tail after the page is complete. Workers still materialize their local result:
their hash merge can hit the cap before the first Chunk, even for a small LIMIT.
See the [worker order and group merge](../../../internal/util/queryutil/reduce_stream/group_by_stream.go).

Global aggregation (no grouping keys) still uses `UnOrderedReduceStream`:
combine partials until every child reaches EOF, then emit one row. For empty
input COUNT is zero and SUM/AVG/MIN/MAX are NULL.

### 8. Worker-to-Proxy Execution

The RPC schema is bidirectional, while the implemented workflow sends one
initial request and receives result Chunks. There is no application credit or
working interrupt exchange yet. Search and Query have separate typed envelopes.
Source: [ViewQueryService](../../../pkg/proto/view.proto#L402).

```text
Proxy task       legacyClient       shard client        QN/SN service
    |                  |                  |                   |
    | Execute()        |                  |                   |
    +----------------->| resolve vchannels                    |
    |                  +----------------->| GetQueryPlan       |
    |                  |                  | open worker RPCs   |
    |                  |                  +------------------>| receive request
    |                  |                  |                   | Batch execution
    |                  |                  |                   | split result
    |                  |<-----------------+ vchannel stream   |
    |<-----------------+ final stream(s)   |                   |
    |                  |                  |                   |
    | final.Recv() --> vchannel.Recv() --> worker.Recv() <-----+ Send(Chunk)
    |        repeat until caller window or EOF               |
    | Close() --------> Close() --------> cancel RPC -------->|
    |                  |                  |                   |
    | successful attempt -> PostExecute() -> unary SDK reply |
```

The request's context outlives the short fan-out used to open worker streams.
Using that fan-out's canceled completion context for the RPC would close it
before later `Recv()` calls. QueryPlan-selected segments, versions, worker MVCC
and execution summary remain part of existing routing, rather than being
reconstructed by a reducer.

Each vchannel sub-search has its own worker context, shared only by workers in
that sub-search. If a worker open fails, the vchannel cancels its sub-search
contexts before retrying; fresh contexts are created for the next attempt.
During setup, legacyClient tracks those cancel functions so a terminal vchannel
failure can cancel blocked sibling opens. After setup succeeds, each shard
reducer owns its worker context; closing one sub-search does not cancel another.
Query and ordinary Search retain their existing pre-output shard retry policy;
Hybrid sub-searches do not independently retry.

For ordinary Search, `SearchStreams()` returns one stream per vchannel; Query
uses `QueryStream()`. `legacyClient` composes the final stream(s). Opening a
stream does not mean a worker result was fully received. The worker handlers
currently execute `SearchOnView`/`queryOnView`, split the complete result, then
send each response Chunk. Sources:
[shard construction](../../../internal/views/queryclient/shard_client.go#L124),
[worker handlers](../../../internal/views/viewquery/server.go#L130),
[gRPC adapter](../../../internal/util/reduce/grpc_reduce_stream.go#L98).

Hybrid shares one optimized QueryPlan per vchannel and opens a separate stream
for each non-skipped sub-search on each selected worker. Each sub-search retains
its own metric, candidate window and selections. Proxy consumes those final
streams concurrently; failure closes sibling streams. RRF/weighted fusion happens
after sub-search reduction, not as a k-way merge between different searches.
For Hybrid Group By, each final sub-search stream forwards all per-vchannel
candidates in Batch-equivalent per-query order. Proxy drains the streams before
fusion and final group selection; it does not stop at the sub-search topK.
See [Hybrid consumption](../../../internal/proxy/dql/task_search.go#L1977).

Search passes its already-reduced candidate results into `RunFromReduced`,
preserving requery, ordering, aggregation, fusion and field assembly as applicable.
Query assembles one `RetrieveResults` and retains its existing pipeline's checks,
pagination and aggregate finalization. Neither collects the original complete
worker result set again. Retained-plan Search requery remains unary. Sources:
[Search pipeline handoff](../../../internal/proxy/dql/search_pipeline.go#L3116),
[Query pipeline](../../../internal/proxy/dql/query_pipeline.go#L193),
[requery routing](../../../internal/views/queryclient/legacy_client.go#L245).

### 9. Metadata and Lifecycle

#### Metadata and EOF

Worker splitters put aggregate result metadata on the first Chunk, including an
empty Chunk when needed. Ordered reducers obtain initial data/EOF from every
active child before emitting and combine metadata once. Group reducers collect
it while draining. Subsequent data Chunks do not repeat summed costs. Structural
fields such as `ElementLevel` still accompany the Chunks that require them.

Metadata includes existing segment/channel identities, retrieval/search counts,
costs, scanned bytes, feature flags and MVCC information. Each field keeps its
existing aggregation rule; there is no new generic sum-every-field operation.
Query `HasMoreResult` is retained after a child's Buffer empties. EOF terminates
the current stream result, not necessarily the entire logical SDK iterator.
Sources: [Search metadata](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L552),
[Query metadata](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L453).

#### Close and cancellation

`Close()` is idempotent and used after success, early stopping, failure or
cancellation. The reducer first signals `ChildStreams.Cancel()` so a `Recv()`
holding `recvMu` can wake up, then acquires that lock and closes its children.
Children must unblock pending receives when closed. The scheduler joins receive
goroutines and drains their completion channel; reducers release input/output
references and metadata. Cleanup errors are returned, not silently treated as
successful completion.

```text
Close():
    childStreams.Cancel()
    lock recvMu
    if already closed:
        return saved closeErr
    mark closed
    closeErr = childStreams.Close()   // close each child, join receives, drain completions
    release retained Buffers and metadata
    return closeErr
```

The transport adapter closes its send side and cancels that RPC's context. It
does not close the shared `grpc.ClientConn`. Natural reducer completion also
closes its children. Sources:
[scheduler cleanup](../../../internal/util/reduce/reduce_stream.go#L196),
[Search Close](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L925),
[Query Close](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L815),
[RPC Close](../../../internal/util/reduce/grpc_reduce_stream.go#L163).

#### Retry ownership

Opening streams does not prefetch a Chunk. Query and ordinary Search retain
Batch's `executeShard()` retry state across stream opening and consumption:
attempt count, replica history and failed-worker/session information share one
shard retry budget. Before the first per-vchannel Chunk is returned, a retryable
receive failure closes and discards that vchannel reducer, including buffered
rows and metadata, then replans and constructs a replacement reducer. Healthy
vchannels remain open. The replacement starts from the beginning; it does not
resume the failed worker's stream.

Once any per-vchannel Chunk has been returned, including metadata-only output,
the parent may have consumed it. A later failure must propagate to Proxy rather
than silently restart the vchannel. Exhausted shard retries also propagate.
Hybrid receive failures always propagate because its concurrent sub-searches
share one QueryPlan; independent replanning could mix plans.

Proxy `Execute()` then closes the entire tree, discards the attempt's rows,
metadata and plans, and replans all vchannels within its retry budget. Internal
Chunks are not published incrementally to the SDK, so discarding an entire
attempt does not duplicate SDK output. Stream close cancels pending receiving
or replanning and joins it before callers read the successful retained plans.

```text
capture server settings once
for attempt in allowed attempts:
    create new streams and attempt-local output/metadata
    consume until request window is satisfied or EOF
        retry Query/ordinary Search vchannels locally only before their first Chunk
    classify original receive error before combining cleanup errors
    close all attempt streams
    if canceled or deadline expired:
        return error
    if receive error is a retryable ViewError or gRPC UNAVAILABLE:
        discard this attempt; continue with new planning
    if receive or cleanup failed:
        return error
    publish this attempt's output to the task; return success
return last attempt error
```

The retryable ViewError codes are `VIEW_INVALIDATED`, `VIEW_NOT_FOUND`,
`ON_SHUTDOWN` and `NOT_PRIMARY`; raw gRPC `UNAVAILABLE` also permits replanning.
Worker errors retain the vchannel, replica and worker identity. Routing prefers
an untried replica when available, while a generic `UNAVAILABLE` does not
permanently exclude a worker. Session/SubConn failures retain the existing Batch
worker exclusion and session-change behavior.

Rows and metadata from different attempts must never mix. Only the successful
attempt's plans, MVCC and execution summaries are retained, including empty
results and Search requery. Live qualification must still verify failure handling
through actual worker gRPC connections. Sources:
[classification](../../../internal/views/viewerror/error.go#L55),
[shard retries](../../../internal/views/queryclient/shard_client.go),
[retained plans](../../../internal/views/queryclient/legacy_client.go),
[Search attempts](../../../internal/proxy/dql/task_search.go#L1569),
[Query attempts](../../../internal/proxy/dql/task_query.go#L1167).

#### Interrupt: reserved, not implemented

The API and protobuf contain interrupt/metadata variants, but `Interrupt()`
currently returns an unimplemented error. Ordinary `Recv()` rejects a separate
metadata response variant; current metadata travels with Chunks. Early completion
uses `Close()`, not an interrupt handshake.

The historical intention was to ask children to stop producing rows, collect
their terminal metadata, combine it and release resources. That remains deferred:
it needs handling for in-flight Chunks, errors, cancellation and metadata completion
before it can replace `Close()`. Bidirectionality alone does not implement it.
Sources: [reserved messages](../../../pkg/proto/view.proto#L450),
[adapter behavior](../../../internal/util/reduce/grpc_reduce_stream.go#L146).

### 10. Configuration and Supported Paths

```yaml
proxy:
  queryView:
    enableReduceStream: true
    reduceStreamChunkBytes: 262144
```

Both ParamItems are refreshable. Proxy snapshots enablement and bytes before its
retry loop, then supplies the same values to all vchannels and Hybrid sub-searches.
Workers receive a positive `stream_chunk_bytes` in each internal request; no QN/SN
restart is needed to apply a newly refreshed value to a new request. Changes do
not resize an existing stream. Existing config-source precedence and refresh
delivery still apply. No SDK Search/Query parameter changes these controls.
Sources: [ParamItems](../../../pkg/util/paramtable/component_param.go#L2824),
[defaults](../../../configs/milvus.yaml#L528),
[refresh tests](../../../pkg/util/paramtable/component_param_test.go#L161).

The following matrix describes routing when streaming is enabled. Existing API
validation remains in force; an unsupported valid shape falls back to Batch,
whereas an invalid request is not made valid by this feature.

| Category | Request shape | Current path |
| --- | --- | --- |
| Query | Plain bounded Query, including scalar filters | Ordered streaming. |
| Query | Valid filtered unlimited Query, including iterator requests with an omitted page limit | Ordered streaming; Proxy reads until EOF, retaining iterator drain rules. |
| Query | Bounded element-level Query and iterator | Ordered streaming; retain complete rows and matched-element indexes. |
| Query | Group By and grouped COUNT/SUM/AVG/MIN/MAX, without ORDER BY | Merge by full group key, then finalize and paginate at Proxy. |
| Query | Bounded plain ORDER BY | Drain/version-resolve each vchannel, sort and emit Chunks; final merge drains for cross-vchannel integrity checks. |
| Query | Group By + ORDER BY | Merge by the requested order with remaining keys as tie-breakers; Proxy re-sorts the bounded prefix and applies the offset. |
| Query | ORDER BY without an explicit limit | Rejected by shared Query validation in both Batch and Streaming modes. |
| Query | Global COUNT/SUM/AVG/MIN/MAX without Group By | Unordered streaming; merge one partial across all children, then finalize at Proxy. |
| Query | Valid QueryScan function chains | Ordered streaming; preserve separately named computed columns, row alignment, validity and empty schema. |
| Search | Ordinary ANN with/without filters; plain ANN iterator | Ordered streaming. |
| Search | Element-level and embedding-list variants | Ordered streaming within existing field/metric validation. |
| Search | Range and iterative-filter | Ordered streaming of worker candidates; range/iterator combinations are not separately qualified. |
| Search | ORDER BY | Ordered ANN streaming, then Proxy scalar ordering. |
| Search | Group By, group_size > 1 and supported composite keys | Ordered grouped candidate selection with Batch pruning. |
| Search | Search aggregation | Ordered candidate reduction, then existing Proxy aggregation. |
| Search | Ordinary ungrouped Proxy-side function/model reranking and L2-only chains | Ordered ANN streaming, then existing Proxy reranking over the complete candidate window. |
| Search | Ordinary ungrouped L0/L1 chains, including combinations with L2 | Workers execute L0/L1 before Chunk splitting; ordered streaming preserves their candidates, followed by optional Proxy L2. |
| Hybrid | Legacy RRF/weighted fusion with eligible ANN, range, iterative-filter, element/embedding-list sub-searches | One streaming tree per sub-search, then Proxy fusion/collapse. |
| Hybrid | Group By with legacy RRF/weighted fusion | Per-vchannel grouped selection, followed by candidate-preserving final streams; Proxy drains before fusion and final grouping. |
| Hybrid | Top-level model/function reranking beyond legacy fusion; sub-search L0/L1 chains | Batch. |

Search Group By/aggregation iterators and Hybrid aggregation/ORDER BY are rejected
by existing API validation, not newly rejected by streaming. Retained-plan requery
stays Batch regardless of enablement. Sources:
[Query routing](../../../internal/views/queryclient/legacy_client.go#L245),
[QueryScan validation](../../../internal/proxy/dql/query_function_chain.go#L29),
[Search/Hybrid gate](../../../internal/proxy/dql/task_search.go#L1678).

For ordinary L2 reranking, Proxy first collects the same ANN candidate window as
Batch, including Batch's pre-rerank request offset. It then invokes the existing
reranking pipeline through `RunFromReduced()`. A smaller L2 `limit` does not reduce
the ANN candidate window. Reranker input fields (including JSON/dynamic roots)
travel with the candidates; output-field requery and field assembly remain in
their existing pipeline positions. L2 is not invoked separately for each transport
Chunk, and its complete candidate list still occupies memory at Proxy.

Ordinary ungrouped L0/L1 chains reuse the same worker execution as Batch. L0 runs
before segment reduction; L1 runs after worker reduction and normalizes the
selected candidates to score-descending/PK-ascending order. An L1 `limit` applies
to the worker's per-vector candidate list, not to individual transport Chunks.
Neither stage is re-executed by ReduceStream. Worker-only chains do not require
Proxy `rerankMeta`; combinations with L2 keep the existing Proxy rerank pipeline.
Iterator/ORDER BY restrictions and Hybrid eligibility remain unchanged.

### 11. Memory and Backpressure

The design targets less retained *direct-child input* in Proxy. It does not set
a hard `B * childCount` process-memory limit. For one ordered reducer, current
child Buffers hold input Chunks, while in-progress output holds row references
that can keep already-consumed source Chunks alive. Encoded output and the final
unary response require additional storage. Both reduction levels contribute.

```text
Proxy retained memory includes:
  current child Chunks + source Chunks referenced by output
  + output encoding + final response + metadata / deduplication / group state
  + gRPC transport buffers

Worker retained memory includes:
  Batch segment/results state + complete reduced result + prepared Chunk list
```

Plain Query ORDER BY retains selected row references for every distinct PK in a
vchannel until workers drain, then materializes and sorts them. Referenced source
Chunks, the sorted result and prepared output Chunks may overlap in memory.
Its final PK-validation set grows with received vchannel candidates. This path
does not have a Chunk-bytes-times-child-count memory bound; it trades early output
for correct timestamp resolution and retains Batch's intermediate output quota.

Worker group maps still grow with distinct keys. Vchannel and final grouped
reducers retain input Chunks and the outgoing Chunk, not a map of all groups.
QueryTask retains the requested window and applies its existing output-size
guard. Oversized Units can exceed `B`. Standard output
quotas and gRPC message limits are independent of the Chunk threshold. Tests of
retained input are not measurements of total RSS or Go heap.

The current transport relies on gRPC's natural backpressure. Stopping application
`Recv()` or calling `Close()` cannot retract Chunks already buffered or sent.
Smaller Chunks change receive frequency and transport overhead; they do not alone
cap transport run-ahead. No application credit coordination, static-window tuning
control or low-watermark prefetch is introduced by this feature. Sources:
[worker materialization](../../../internal/views/viewquery/server.go#L155),
[output references](../../../internal/util/searchutil/reduce_stream/search_reduce_stream.go#L46),
[group output](../../../internal/util/queryutil/reduce_stream/query_reduce_stream.go#L553).

### 12. Validation and Deferred Work

#### Verification requirements

| Area | Required evidence |
| --- | --- |
| Ordered equivalence | Batch parity for PK/score order, ties, deduplication, timestamps, NQ, offsets and partial final Chunks. |
| Query ORDER BY | Late newer versions changing topK, equal/missing timestamps, ASC/DESC/NULL ordering, nested reduction, non-adjacent cross-vchannel duplicates beyond the page, and errors before worker drain. |
| Byte contract | Crossing `B`, exact boundary, oversized Unit alone, timestamp replacement, nullable/nested/element fields and metadata exclusion. |
| Grouped Query | Verify worker ordering before local truncation, equal keys across Chunks and vchannels, and AVG/NULL partials. Check ASC/DESC, tie-breaks and offset against Batch, and verify cutoff without reading the tail. |
| Global Query aggregation | Standalone COUNT and mixed aggregate projections; nested reduction, unequal AVG counts, empty/NULL/zero values, empty shards, late errors and cancellation. Verify one output row, metadata and Batch finalization parity. |
| QueryScan | Computed JSON/scalar columns across worker splitting, nested reduction and Proxy pagination; timestamp winners, named-column reordering, NULL/empty schema, malformed columns, Chunk bytes and output quota. Verify native Grep output and gRPC round trips. |
| Search variants | Group pruning parity, aggregation candidate order, range/filter behavior, Hybrid sub-search windows, element identity and downstream assembly. |
| Lifecycle | Cancellation while blocked, concurrent Close, receive cleanup, error propagation, retry isolation after internal output, and exhausted retry budgets. |
| Configuration | In-process YAML refresh, stable in-flight/retry settings, unsupported-path fallback and no SDK override. |
| Performance | Same request/data/topology with Batch and Streaming; record fan-in, bytes, Chunk threshold, concurrency, QPS, latency and clearly defined retained/process memory. |

Existing test entry points include the
[Search reducer suite](../../../internal/util/searchutil/reduce_stream/search_reduce_stream_test.go),
[Query reducer suite](../../../internal/util/queryutil/reduce_stream/query_reduce_stream_test.go),
[receiver suite](../../../internal/util/reduce/reduce_stream_test.go),
[QueryView parity suite](../../../internal/views/queryclient/client_legacy_test.go),
[Proxy retry suite](../../../internal/proxy/dql/task_query_view_legacy_test.go), and
[configuration suite](../../../internal/proxy/dql/streaming_config_test.go).
Run Go verification with the repository's `dynamic,test` tags and
`-gcflags="all=-N -l"`; use race detection for these concurrent paths.

The master-based PR records ARM package/race and build results for pinned commits.
Those are not a claim of live SDK-to-cluster qualification on this base, nor a
performance conclusion. The PR also records outstanding branch-wide static-check
findings. Documentation-only updates do not upgrade that verification status.

#### Historical proposals versus current implementation

| Earlier description | Current contract |
| --- | --- |
| M1A enables only iterator requests | Later approved ordinary Query/Search and eligible Hybrid paths are included; use the matrix above. |
| Fixed Unit-count Chunks | Byte threshold with complete-Unit crossing and oversized-Unit handling. |
| `B + y` concurrent prefetch Buffers | Refill empty inputs through one outstanding receive per child; no `y` control. |
| Query Group By vchannel forwarding or reducer-owned hidden map | Equal full keys are reduced at both vchannel and Proxy; only global aggregation requires EOF before output. |
| Query ORDER BY only needs a different comparator | Plain ORDER BY first drains and version-resolves each vchannel; grouped ORDER BY merges complete keys incrementally. |
| Proxy consumes in PostExecute, or retry ends at first internal Chunk | Execute owns whole-attempt consumption/retry and publishes only successful output. |
| Interrupt returns aggregated terminal metadata | Reserved API only; current early termination uses Close. |
| Per-request SDK streaming overrides | Removed; refreshable server YAML controls only. |

The [historical full design](https://github.com/jamesgao-jpg/milvus/blob/7b3121b7b1c4ea5e60718155a30b41aef00cb9f4/docs/design-docs/design_docs/20270722-streaming_reduce_design.md)
and its assets remain unchanged. This document restores the detailed explanation
using ASCII and current contracts, rather than copying those historical plans
as implemented features.

Deferred work includes segment-level streaming and stateful SDK iteration;
terminal metadata through Interrupt; application credit/prefetch coordination;
remaining function-chain/reranking paths; and live failure/performance
qualification. Each requires its own
behavior contract and verification before changing the current path.
