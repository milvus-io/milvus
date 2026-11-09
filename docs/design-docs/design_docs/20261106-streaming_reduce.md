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
or group key with partial aggregates. A Chunk is one result protobuf; a Buffer
retains its reducible data. The byte threshold `B` sums independently encoded
Unit fields, excluding result metadata and transport framing; it is not exact
message size or heap usage. Append a complete Unit before checking `B`: 80 + 30
bytes with `B=100` emits 110 bytes. A Unit larger than `B` is emitted alone,
flushing existing output first. gRPC message limits remain independent.
See [Search sizing](../../../internal/util/searchutil/reduce_stream.go#L224) and
[Query splitting](../../../internal/util/queryutil/reduce_stream.go#L703).

### Reduction and Lifecycle

`NewReduceStream()` selects the policy; `Recv()` returns one Chunk, EOF, or error.
The [shared receiver](../../../internal/util/reduce/child_streams.go#L78) permits
one outstanding receive per child and fills empty child Buffers concurrently.

| Policy | Input readiness and reduction |
| --- | --- |
| Query `OrderedReduceStream` | Each active child has a head or EOF; merge by PK and resolve shard-local duplicate PKs by timestamp. |
| Plain Query ORDER BY | Each vchannel drains workers into a PK map, resolves versions, sorts and selects candidates before emitting Chunks. Final merge uses scalar ordering and rejects cross-vchannel duplicate PKs. |
| Search `OrderedReduceStream` | Each active child has a head or EOF; merge by score per vector. Group selection preserves Batch pruning, not exhaustive group membership. |
| Query Group By `UnOrderedReduceStream` | Consume available Buffers into a stream-owned group map; drain all children before emitting partial aggregates. Finalize at Proxy, preserving AVG sum/count and NULL state between levels. |

Proxy `Execute()` consumes results and stops at the request's row, element, or
per-vector window, or EOF; unlimited Query drains input. Plain Query ORDER BY
drains finalized vchannel candidates to validate duplicates, retaining only its
requested window. Search skips surplus
hits for earlier vectors to reach later vectors. Search bypasses repeated Batch
candidate reduction; Query passes its merged result through existing pagination
and aggregate finalization. `Close()` cancels children, joins receives and releases
resources; `Interrupt()` remains unimplemented. On a retryable ViewError, Proxy discards the
attempt's output/metadata and recreates its streams within the retry budget,
even after internal Chunks arrived. Other errors fail the attempt; streams do
not resume. See [Search](../../../internal/proxy/dql/task_search.go#L1569) and
[Query](../../../internal/proxy/dql/task_query.go#L1167) attempt ownership.

### Controls and Compatibility

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
stream. Query Group By + ORDER BY/global aggregation/function chains, Hybrid Group By and
unsupported reranking remain Batch, as does retained-plan requery. Exact gates:
[Query](../../../internal/views/queryclient/legacy_client.go#L290),
[Query function chains](../../../internal/proxy/dql/task_query.go#L1172),
[Search/Hybrid](../../../internal/proxy/dql/task_search.go#L1678).

### Limits and Verification

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

## Detailed Design

1. [Architecture and alternatives](#1-architecture-and-alternatives)
2. [APIs and ownership](#2-apis-and-ownership)
3. [Unit and byte contracts](#3-unit-and-byte-contracts)
4. [Receive scheduling and implementation comparison](#4-receive-scheduling-and-implementation-comparison)
5. [Ordered Query](#5-ordered-query)
6. [Ordered Search](#6-ordered-search)
7. [Unordered Query Group By](#7-unordered-query-group-by)
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

Search and Query expose parallel typed interfaces, not one interface that mixes
the two protobuf types. A transport-backed child implements the same contract as
an in-process reducer. The signatures below retain the existing API names;
`result` stands for the corresponding protobuf, not a new wrapper type.

```text
searchutil.ReduceStream: result = *internalpb.SearchResults
queryutil.ReduceStream:  result = *internalpb.RetrieveResults

Recv()      -> (result, error)
Close()     -> error
Interrupt() -> (result, error)     // currently returns an unimplemented error

searchutil.NewReduceStream(request, childStreams, chunkBytes)
searchutil.NewShardReduceStream(request, childStreams, chunkBytes)
queryutil.NewReduceStream(request, schema, childStreams, chunkBytes)
queryutil.NewShardReduceStream(request, schema, childStreams, chunkBytes)
```

`NewShardReduceStream` enables the vchannel-level duplicate policy. The final
factory composes vchannel streams. Query chooses `UnOrderedReduceStream` when
grouping is requested; other eligible Query requests use `OrderedReduceStream`.
Eligible Search candidate reduction uses `OrderedReduceStream`, including Search
Group By. Hybrid is decomposed before the factory, not merged as one ANN request.
Sources: [Search factories](../../../internal/util/searchutil/reduce_stream.go#L369),
[Query factories](../../../internal/util/queryutil/reduce_stream.go#L219).

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
[Search contract](../../../internal/util/searchutil/reduce_stream.go#L34),
[Query contract](../../../internal/util/queryutil/reduce_stream.go#L37),
[child receive state](../../../internal/util/reduce/child_streams.go#L26).

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
| Query row | PK when present, returned fields, optional matched-element indexes. |
| Grouped Query partial | Group columns and partial aggregate columns. |

Sizing counts the protobuf encoding of each Unit's reducible fields, including
tags, lengths, wrappers, validity data and retained field descriptors. Shared
wrappers are counted independently per Unit. It excludes result-level status,
billing/MVCC/segment metadata and transport framing. Thus the sum of Unit sizes
is neither `proto.Size(wholeChunk)` nor retained heap bytes.

Calculations use existing field data directly, without constructing a temporary
one-row result. Nullable-field mappings are reused per input Chunk; variable
lengths and varint widths are counted where required. The shared implementation
is in [row sizing](../../../internal/util/reduce/field_data_size.go), with callers in
[Search](../../../internal/util/searchutil/reduce_stream.go#L68) and
[Query](../../../internal/util/queryutil/reduce_stream.go#L114).

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
Chunk. The concrete output-readiness rules differ; forcing Query Group By into
the row-at-a-time loop would obscure its need to drain all inputs.

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
receive           all active heads    all active heads     any ready input
consume           lowest PK/version   best score/vector    all grouped rows
accumulate        row references      hit references       group map
ready             byte boundary*      byte/window yield*   all inputs drained
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
Source: [child scheduling](../../../internal/util/reduce/child_streams.go#L37).

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
Source: [Query Recv](../../../internal/util/queryutil/reduce_stream.go#L272).

For `element_filter`, one Unit is still one document row with all matched indexes.
The requested page length counts elements, not document rows. Proxy assembles
complete Units, then the existing pagination logic trims the element-index list.
Nullable scalar/vector validity and element-level flags must survive splitting,
merging and output assembly. Sources:
[element validation](../../../internal/util/queryutil/reduce_stream.go#L173),
[Proxy consumption](../../../internal/proxy/dql/task_query.go#L1386).

The PK-ordered reducer has no request row-limit stopping counter. Proxy stops pulling at its
row/element window, or reads to EOF for valid unlimited Query. Iterator semantics
add a separate correctness boundary: when `ShouldStopWhenDrained(reduceType)` is
true and a drained child reported `HasMoreResult`, the reducer ends this page
rather than skipping that child's unseen later rows. `HasMoreResult` describes
rows outside the child's current result, not whether another Chunk is in flight.
See [getReadyBuffers](../../../internal/util/queryutil/reduce_stream.go#L387).

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
[vchannel accumulation](../../../internal/util/queryutil/order_by_stream.go),
[Batch timestamp rule](../../../internal/util/queryutil/dedup_pk_op.go#L124),
[sorting](../../../internal/util/queryutil/order_op.go#L76).

The final reducer rejects duplicate PKs across vchannels using a set, since equal
PKs need not be adjacent under scalar ordering. Proxy drains that final stream
even after collecting `offset + limit` rows, so duplicates beyond the page still
fail the request; it does not retain surplus rows. Existing Proxy pagination
applies the offset once. This qualifies only bounded plain Query ORDER BY.
Group By + ORDER BY remains on Batch. ORDER BY without a limit is rejected by
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
Sources: [Recv](../../../internal/util/searchutil/reduce_stream.go#L428),
[selection](../../../internal/util/searchutil/reduce_stream.go#L622),
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
[skip/yield logic](../../../internal/util/searchutil/reduce_stream.go#L622),
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
group fields. It is not the Query Group By map over all matching rows, and it
does not compute aggregates over candidates never returned by workers. Sources:
[group selection](../../../internal/util/searchutil/reduce_stream.go#L685),
[candidate assembly](../../../internal/proxy/dql/task_search.go#L1887),
[Proxy aggregation](../../../internal/proxy/dql/search_pipeline.go#L668).

### 7. Unordered Query Group By

Query Group By can combine partial aggregates without cross-child head ordering.
It cannot finalize a group on first arrival: later Chunks may update that group.
Both vchannel and final reducers therefore accumulate a group map and drain
their children before exposing output Chunks. The vchannel reducer does not
merely forward child pairs.

The stream owns `GroupAggBuffer`; `GroupAggReducer` supplies layout, decoding and
encoding. Its ordinary `Merge(ctx, partials)` is a stateless operation, not a
hidden map retained across calls. Incremental streaming calls the explicitly
owned Buffer's `Merge()` instead. The Buffer can remain stateful without changing
the `PartialAggregationResult` contract.

```text
construction:
    reducer = NewGroupAggReducer(groupByFieldIDs, aggregates, -1, schema)
    outputBuffer = reducer.NewBuffer()

Recv():
    if outputChunks is not built:
        repeat:
            readyBuffers = getReadyBuffers()
            if readyBuffers is empty:
                break
            partialResults = []
            for readyBuffer in readyBuffers:
                partialResults.append(reducer.DecodePartial(readyBuffer.FieldsData, 0))
            outputBuffer.Merge(partialResults)
            clear childBuffers
        reduced = outputBuffer.Result()
        result = reducer.EncodePartial(reduced)
        attachMetadata(result)
        outputChunks = SplitRetrieveResult(result, chunkBytes)
        release reducer and outputBuffer
    if outputIndex < len(outputChunks):
        return outputChunks[outputIndex++] and release that stored reference
    close children; return EOF
```

Every accepted Buffer's grouped rows are consumed, not just one key/value pair.
`DecodePartial(..., 0)` avoids counting retrieval metadata again through the
aggregate Buffer; the stream separately combines metadata. The current
implementation materializes grouped output and its Chunk list after draining,
so the grouped path is not bounded by one outgoing Chunk in memory.
Sources: [group Recv](../../../internal/util/queryutil/reduce_stream.go#L530),
[GroupAggBuffer](../../../internal/agg/aggregate_reducer.go#L338).

```text
worker partials --> vchannel map --> EncodePartial --> final map
                                                         |
                                                   EncodePartial
                                                         |
                                            Proxy Finalize + output mapping
```

Empty input is an identity, not a fabricated zero or NULL group. Real zero and
NULL values remain distinct. AVG stays mergeable as sum/count through both levels;
division belongs to final output mapping. Finalization and column naming do not
mutate partials retained upstream. Query's existing pipeline consumes the single
merged result and still performs aggregate finalization and pagination. Sources:
[partial contract](../../../internal/agg/aggregate_reducer.go#L515),
[Query finalization](../../../internal/proxy/dql/query_pipeline.go#L280).

Global aggregates without group keys and Group By + ORDER BY remain outside the
current streaming gate. The former needs a no-group accumulation contract; the
latter remains explicitly deferred independently of plain Query ORDER BY. This document
does not enable them by describing their potential algorithms.

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

For ordinary Search, `SearchStreams()` returns one stream per vchannel; Query
uses `QueryStream()`. `legacyClient` composes the final stream(s). Opening a
stream does not mean a worker result was fully received. The worker handlers
currently execute `SearchOnView`/`queryOnView`, split the complete result, then
send each response Chunk. Sources:
[shard construction](../../../internal/views/queryclient/shard_client.go#L124),
[worker handlers](../../../internal/views/viewquery/server.go#L130),
[gRPC adapter](../../../internal/util/searchutil/grpc_stream.go#L42).

Hybrid shares one optimized QueryPlan per vchannel and opens a separate stream
for each non-skipped sub-search on each selected worker. Each sub-search retains
its own metric, candidate window and selections. Proxy consumes those final
streams concurrently; failure closes sibling streams. RRF/weighted fusion happens
after sub-search reduction, not as a k-way merge between different searches.
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
Sources: [Search metadata](../../../internal/util/searchutil/reduce_stream.go#L556),
[Query metadata](../../../internal/util/queryutil/reduce_stream.go#L429).

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
[scheduler cleanup](../../../internal/util/reduce/child_streams.go#L196),
[Search Close](../../../internal/util/searchutil/reduce_stream.go#L925),
[Query Close](../../../internal/util/queryutil/reduce_stream.go#L792),
[RPC Close](../../../internal/util/searchutil/grpc_stream.go#L98).

#### Retry ownership

One ReduceStream tree represents one attempt; it never reconnects and resumes
itself. Proxy `Execute()` owns planning, consumption and whole-attempt retries.
Internal Chunks are not published to the SDK incrementally, so a classified
retryable failure can discard the entire attempt even after Chunks arrived.

```text
capture server settings once
for attempt in allowed attempts:
    create new streams and attempt-local output/metadata
    consume until request window is satisfied or EOF
    classify original receive error before combining cleanup errors
    close all attempt streams
    if canceled or deadline expired:
        return error
    if receive error is a retryable ViewError:
        discard this attempt; continue with new planning
    if receive or cleanup failed:
        return error
    publish this attempt's output to the task; return success
return last attempt error
```

The retryable ViewError codes are `VIEW_INVALIDATED`, `VIEW_NOT_FOUND`,
`ON_SHUTDOWN` and `NOT_PRIMARY`. A raw gRPC `UNAVAILABLE` is not sufficient for
QueryView replanning. Rows and metadata from different attempts must never mix.
The existing shard setup retry remains; Proxy additionally owns failures during
consumption. Live qualification must verify error preservation from the actual
worker through gRPC, not only the local classification helper. Sources:
[classification](../../../internal/views/viewerror/error.go#L55),
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
[adapter behavior](../../../internal/util/queryutil/grpc_stream.go#L70).

### 10. Configuration and Supported Paths

```yaml
proxy:
  queryView:
    enableReduceStream: false
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
| Query | Group By and grouped COUNT/SUM/AVG/MIN/MAX, without ORDER BY | Unordered grouped streaming, then Proxy finalization. |
| Query | Bounded plain ORDER BY | Drain/version-resolve each vchannel, sort and emit Chunks; final merge drains for cross-vchannel integrity checks. |
| Query | Group By + ORDER BY | Batch, subject to existing validation. |
| Query | ORDER BY without an explicit limit | Rejected by shared Query validation in both Batch and Streaming modes. |
| Query | Global COUNT/SUM/AVG/MIN/MAX without Group By | Batch. |
| Query | QueryScan function chains | Batch; streaming assembly does not preserve separate computed columns. |
| Search | Ordinary ANN with/without filters; plain ANN iterator | Ordered streaming. |
| Search | Element-level and embedding-list variants | Ordered streaming within existing field/metric validation. |
| Search | Range and iterative-filter | Ordered streaming of worker candidates; range/iterator combinations are not separately qualified. |
| Search | ORDER BY | Ordered ANN streaming, then Proxy scalar ordering. |
| Search | Group By, group_size > 1 and supported composite keys | Ordered grouped candidate selection with Batch pruning. |
| Search | Search aggregation | Ordered candidate reduction, then existing Proxy aggregation. |
| Search | Ungrouped explicit function/model reranking or L0/L1/L2 chains | Batch. |
| Hybrid | Legacy RRF/weighted fusion with eligible ANN, range, iterative-filter, element/embedding-list sub-searches | One streaming tree per sub-search, then Proxy fusion/collapse. |
| Hybrid | Group By | Batch; final fusion needs a candidate-preserving grouping policy. |
| Hybrid | Top-level model/function reranking beyond legacy fusion; sub-search L0/L1 chains | Batch. |

Search Group By/aggregation iterators and Hybrid aggregation/ORDER BY are rejected
by existing API validation, not newly rejected by streaming. Retained-plan requery
stays Batch regardless of enablement. Sources:
[Query routing](../../../internal/views/queryclient/legacy_client.go#L245),
[Query function guard](../../../internal/proxy/dql/task_query.go#L1172),
[Search/Hybrid gate](../../../internal/proxy/dql/task_search.go#L1678).

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

Group maps grow with distinct keys; their complete encoded output and Chunk list
are additional materialization. Oversized Units can exceed `B`. Standard output
quotas and gRPC message limits are independent of the Chunk threshold. Tests of
retained input are not measurements of total RSS or Go heap.

The current transport relies on gRPC's natural backpressure. Stopping application
`Recv()` or calling `Close()` cannot retract Chunks already buffered or sent.
Smaller Chunks change receive frequency and transport overhead; they do not alone
cap transport run-ahead. No application credit coordination, static-window tuning
control or low-watermark prefetch is introduced by this feature. Sources:
[worker materialization](../../../internal/views/viewquery/server.go#L155),
[output references](../../../internal/util/searchutil/reduce_stream.go#L46),
[group output](../../../internal/util/queryutil/reduce_stream.go#L566).

### 12. Validation and Deferred Work

#### Verification requirements

| Area | Required evidence |
| --- | --- |
| Ordered equivalence | Batch parity for PK/score order, ties, deduplication, timestamps, NQ, offsets and partial final Chunks. |
| Query ORDER BY | Late newer versions changing topK, equal/missing timestamps, ASC/DESC/NULL ordering, nested reduction, non-adjacent cross-vchannel duplicates beyond the page, and errors before worker drain. |
| Byte contract | Crossing `B`, exact boundary, oversized Unit alone, timestamp replacement, nullable/nested/element fields and metadata exclusion. |
| Grouped Query | Multiple Chunks per child, nested vchannel/final accumulation, unequal AVG counts, empty/NULL/zero values and partial-state preservation. |
| Search variants | Group pruning parity, aggregation candidate order, range/filter behavior, Hybrid sub-search windows, element identity and downstream assembly. |
| Lifecycle | Cancellation while blocked, concurrent Close, receive cleanup, error propagation, retry isolation after internal output, and exhausted retry budgets. |
| Configuration | In-process YAML refresh, stable in-flight/retry settings, unsupported-path fallback and no SDK override. |
| Performance | Same request/data/topology with Batch and Streaming; record fan-in, bytes, Chunk threshold, concurrency, QPS, latency and clearly defined retained/process memory. |

Existing test entry points include the
[Search reducer suite](../../../internal/util/searchutil/reduce_stream_test.go),
[Query reducer suite](../../../internal/util/queryutil/reduce_stream_test.go),
[receiver suite](../../../internal/util/reduce/child_streams_test.go),
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
| Query Group By vchannel forwarding or reducer-owned hidden map | Each grouped stream owns `GroupAggBuffer` and emits mergeable partials after draining. |
| Query ORDER BY only needs a different comparator | Plain ORDER BY first drains and version-resolves each vchannel; Group By + ORDER BY remains deferred. |
| Proxy consumes in PostExecute, or retry ends at first internal Chunk | Execute owns whole-attempt consumption/retry and publishes only successful output. |
| Interrupt returns aggregated terminal metadata | Reserved API only; current early termination uses Close. |
| Per-request SDK streaming overrides | Removed; refreshable server YAML controls only. |

The [historical full design](https://github.com/jamesgao-jpg/milvus/blob/7b3121b7b1c4ea5e60718155a30b41aef00cb9f4/docs/design-docs/design_docs/20270722-streaming_reduce_design.md)
and its assets remain unchanged. This document restores the detailed explanation
using ASCII and current contracts, rather than copying those historical plans
as implemented features.

Deferred work includes segment-level streaming and stateful SDK iteration;
terminal metadata through Interrupt; application credit/prefetch coordination;
Query Group By + ORDER BY and global aggregation; candidate-preserving Hybrid Group By;
remaining function-chain/reranking paths; Batch's optional two-stage Search
optimization; and live failure/performance qualification. Each requires its own
behavior contract and verification before changing the current path.
