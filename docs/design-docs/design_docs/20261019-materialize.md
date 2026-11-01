# Materialize: fetch rows as Arrow IPC, relayed by the delegator without merging

- **Created:** 2026-10-19
- **Author(s):** @tedxu
- **Status:** draft
- **Component:** Proxy | QueryNode (delegator and worker) | pkg/util/resource
  (codec) | internal/util/queryutil
- **Related Issues:** to be filed
- **Related:**
  - `20261005-arrow-retrieve-no-fielddata.md`: the worker's Arrow retrieve
    path.

## Summary

Search spends most of its time in the requery: on a large production
cluster the requery is 6.86 of 10.7 ms of average search latency (64%), and
6.5 of 9.64 ms at p50 (67%). Its workers' query takes 0.28 ms on average.
The rest is the wait for the slowest worker, the network, and handling the
payload at every hop; the split between them is not measured. Today every
hop handles the payload, although the requery discards no row:

1. Each worker builds `FieldData`, marshals it and compresses it.
2. The delegator decompresses N results, decodes them, merges them by pk,
   then encodes and compresses again.
3. The proxy decompresses, decodes, merges and reorders once more.

**Materialize** is a new pair of QueryNode RPCs for requests that only fetch
rows. Phase 1 serves the search requery only. Its response
has two parts:

- **Header** (protobuf): the status, the statistics and the body's schema,
  or only its fingerprint when the proxy already holds the schema.
- **Body:** field data only, as Arrow IPC record-batch messages, pk and
  timestamp included as columns. Compression is Arrow's own, per buffer
  (§2).

Each hop does only what it must:

- **Worker:** writes its rows once.
- **Delegator:** reads only the headers and relays the bodies untouched: no
  decompression, no decode, no merge, no copy.
- **Proxy:** deduplicates pks on the raw pk and timestamp columns, then
  decompresses and gathers each column once, into the `QueryResults`
  `Query` returns today. The SDK response, hooks and the access log are
  unchanged.

The intended benefit is **latency**: the delegator's merge, encode and
compress leave the critical path, and the proxy deduplicates instead.
Offline (Impact, N = 8 workers per shard, compression off), Materialize
shortens the critical path of a requery of one query by 42–73% (vec768
topK 1000: 1380 → 373 µs) and of four queries by 14–56%, for 36–73% less
total CPU. With compression on it stays ahead of `Query` on latency and CPU, at
the price of more bytes on text (Impact). Filter queries could also use
it, but they would return rows in arrival order rather than sorted by pk
(§7), which users and e2e tests depend on, so they stay on `Query` for now.

## Background

### The path today

- **Worker** (`QuerySegments`): retrieve per segment, `ReduceByPKTS`, build
  `FieldData`, marshal, gRPC zstd when enabled.
- **Delegator** (`Query`): decompress N results, decode, reduce by pk and
  timestamp while gathering every column, marshal, compress.
- **Proxy** (`QueryTask.PostExecute`): decompress, decode, reduce across
  shards and gather again; for a requery, reorder into search order row by
  row (`organize`); SDK marshal.

### What it costs

Measured on a large production cluster (Milvus 2.6.x), 1-hour averages
read on 2026-10-16:

| measure | value |
|---|---|
| search end to end, avg / p50 | 10.7 / 9.64 ms |
| requery including proxy ↔ delegator round trips, avg / p50 | 6.86 ms (64%) / 6.5 ms (67%) |
| requery on the delegator / on its workers | 2.55 / 0.28 ms |
| requery's share of the proxy ↔ delegator round trips (incl. compression on both ends) | 4.31 ms |
| payload per Search RPC to the client (uncompressed) / per search task | 1062 KB / ~345 KB |
| compression ratio on the delegator → proxy hop | ≥ 3× (mostly text) |
| delegator CPU | ~2 ms per request, 12 ms per MB of payload; 10–12% of cores used |

What this establishes:

- **Most of the requery's latency is outside the workers' query.** The
  delegator takes 2.3 ms more than its workers' average query (p99 1.7
  ms); for search the gap is 0.6 ms. The gap holds the wait for the
  slowest worker, the network and the payload handling at both ends;
  these figures do not split it.
- **The cost scales with load** (7 days, CPU against bytes r = 0.998,
  against requests r = 0.999). Bytes and requests move together, so the
  figures cannot say which drives it.
- **Compression dominates the delegator's CPU on this cluster.** A pprof
  of one delegator under search load: ~36% compressing replies to the
  proxy (search and requery alike), ~27% decompressing the workers'
  **search** results, ~9% decoding local search results. Two limits: gRPC
  compression is on (cross-AZ) and the payload is text-heavy, so on a
  cluster without compression decode and merge take a larger share; and
  the profile does not isolate the requery.
- **Delegators are not CPU-bound,** so the cost shows as latency, not
  capacity.
- **Compression cannot simply be turned off.** Resource Group allocation
  sends worker ↔ delegator traffic across AZs. Text needs compressing;
  dense vectors do not compress (measured offline).

## Goals

1. **Lower requery latency** by taking payload work off the delegator's
   critical path.
2. **Compress and encode once.** The worker writes and compresses; the
   proxy decompresses and decodes. Nothing in between opens the body.
3. **Compress only what pays.** Text and JSON may travel compressed, dense
   vectors raw. Compression stays available for cross-AZ hops.
4. **No intermediate merge.** Only the proxy merges.
5. **No change visible to clients:** the SDK response, request hooks, the
   access log and query semantics (apart from the row order of §7).

## Scope

Materialize accepts a request only if it reduces no rows other than
duplicate pks:

- **Accepted:** the search requery (`pk IN (...)`, no limit), whose output
  follows the search ids.
- **Not accepted; these keep `Query`:**
  - every filter query, upsert's pk lookup and fetch-by-IDs: their rows
    would come back in arrival order, and that order changes between runs
    when a shard has both growing and sealed hits. The filter path in the
    fill and the benchmarks below stay, for a later opt-in;
  - a limit or offset, iterators included;
  - ORDER BY, aggregation, GROUP BY and `count(*)`;
  - element-level output (`ElementIndices`, struct and vector-array
    fields): a batch has no place for the indices;
  - output fields the Arrow export does not carry as Arrow: ARRAY, sparse
    float vectors, vector-array, and types without an exporter branch
    (20261005 doc).
- **Not covered:** `QueryStream` and the search RPCs. Search's inline
  output fields could follow later.

External collections are in scope like any other: their take path fills the
same columns the Arrow export reads.

The proxy routes by this scope. A worker that still gets a request it
cannot serve, or a format version it does not use (§9), answers
`ErrServiceUnimplemented` and the request falls back (§4, §8).

## Public interfaces

- **RPCs:** `QueryNode.Materialize` and `QueryNode.MaterializeSegments`
  (§1). Internal only; no SDK change.
- **Proto:** `internal.MaterializeResults`, `internal.MaterializeHeader`,
  and one field on `QueryRequest` (§2).
- **Configuration**, none listed in `milvus.yaml` (set through the
  config file or etcd like any unlisted key):
  - `proxy.materialize.enabled` (default `false`, refreshable; turn it on
    after the rolling upgrade, §8);
  - `queryNode.materialize.codec` (`none | zstd`, default `none`,
    §2).
  - `queryNode.materialize.zstdMinBodySize` (default 16 KB, refreshable,
    §2).
- **Metrics:** two new proxy counters; the QueryNode reuses its query
  metrics (Observability).

## Design

### 1. RPCs

```proto
// QueryNode service
rpc Materialize(QueryRequest) returns (internal.MaterializeResults);          // proxy → delegator
rpc MaterializeSegments(QueryRequest) returns (internal.MaterializeResults);  // delegator → worker
```

- **The request is `QueryRequest`,** so the plan, the RLS predicate, the
  MVCC and guarantee timestamps and the partitions mean what they do
  today. The delegator's steps up to the sub-task fan-out are unchanged:
  the tsafe wait, pinning readable segments, pk pruning and the split.
- **No gRPC compression on either call.** The client sends with the
  identity compressor (appended after the caller's options, so it wins)
  and the server replies uncompressed. Compression lives in the body.
- **Default codec.** Both calls use the registered `"proto"` codec like
  every other RPC; for `MaterializeResults` it copies no body byte (§6).

### 2. Wire

```proto
message MaterializeResults {
  MaterializeHeader header = 1;
  repeated bytes body = 2;  // one Arrow IPC RecordBatch message per entry
}
// QueryRequest
uint64 materialize_schema_fingerprint = 6;  // schema the caller holds; 0 = none
```

`MaterializeHeader` fields and their readers:

| # | field | meaning | read by |
|---|---|---|---|
| 1 | `status` | | delegator: fail fast; proxy: errors and the fallback (§8) |
| 2 | `body_schema` | Arrow IPC Schema message; empty when the request named its fingerprint | delegator, proxy |
| 3 | `schema_fingerprint` | xxhash64 of the Schema message; set when the body is non-empty | delegator, proxy |
| 4 | `body_raw_size` | body size after decompression | proxy: bounds a compressed var-len buffer (§3) |
| 5 | `source_id` | worker node id | delegator: cost aggregation (§5) |
| 6 | `all_retrieve_count` | | proxy: `allQueryCnt` |
| 7, 8 | `scanned_remote_bytes`, `scanned_total_bytes` | | proxy: storage cost |
| 9 | `cost_aggregation` | | proxy: load-balancer cost metrics, `TotalRelatedDataSize` |
| 10 | `mvcc_timestamp` | | proxy: the channel's MVCC timestamp |
| 11 | `result_size` | what `Query`'s `maxOutputSize` guard charges the rows | delegator: size guard (§5) |
| 12 | `feature_bits` | execution features used (as `RetrieveResults.feature_bits`); the delegator ORs in its own (segment pruning) | proxy: feature-usage counters |

A worker reports its own values; the delegator reports the aggregate.
`RetrieveResults`' other fields (`reqID`, segment and channel id lists,
`has_more_result`, `element_*`) have no reader on this path and are not
carried. Another body format later (Vortex, say) adds a `body_format` enum
with `0 = ARROW_IPC`, compatible with messages that lack it.

**Body.** Each entry is one encapsulated Arrow IPC `RecordBatch` message.
There is no schema message, dictionary or end-of-stream marker in the body;
the schema message followed by the entries is a valid Arrow IPC stream. One
entry per batch lets the delegator forward entries without joining them.

- **Columns:** `$pk` (int64 or utf8), then the system columns
  `$sys:<field id>` (timestamp, int64), then the output fields as the
  worker's Arrow record holds them (the `FieldDataToArrow` mapping). pk and
  timestamp come from the worker's reduce, not from the records. A pk that
  is also an output field appears twice.
- **Schema.** One per response, carried rather than derived from the
  collection schema, so the reader needs no copy of the Milvus → Arrow type
  mapping; field order and validity stay schema metadata
  (`milvus.field_order`, `milvus.valid_data_fields`). A mapping change
  between versions shows up as differing schemas (§9).
- **Schema delivery by fingerprint.**
  - The proxy keeps a process-wide LRU (1024 entries each) of (collection,
    schema version, output field ids) → fingerprint and fingerprint →
    Schema message. It resolves the request's schema once, keeps it for the
    call (an eviction cannot strand a reply), and sends its fingerprint (0
    on a miss).
  - The worker always sets `schema_fingerprint` for a non-empty body, and
    `body_schema` only when it differs from the request's fingerprint. The
    key only proposes a schema; the worker compares it with the real one,
    so a stale entry costs the schema bytes, never a wrong decode.
  - The delegator forwards the request's fingerprint, requires every
    non-empty reply to carry the same fingerprint, and relays it with the
    first schema bytes any worker sent.
  - The proxy checks schema bytes against the fingerprint and caches them;
    a reply without bytes must have the fingerprint the request named.
- **Compression** is Arrow's `BodyCompression`, one codec per batch,
  applied per buffer; Materialize writes and reads only `ZSTD`. The writer
  keeps a buffer raw (uncompressed length −1) when compression saves under
  10%. Arrow readers from 12.0 on read the result.
- **`queryNode.materialize.codec`.** `none` (default) or `zstd`. It is
  independent of `grpc.client.*`, whose algorithm, level, CRC and
  concurrency do not apply to Materialize. A cluster with gRPC compression
  on that enables Materialize sends raw until it also sets `zstd`; the
  QueryNode logs a warning at startup for that combination. The zstd level
  and CRC are the Arrow writer's defaults. Arrow's `BodyCompression` carries
  only ZSTD and LZ4_FRAME, so there is no s2 option.
- **Per-reply rule under `zstd`.** The worker writes a batch raw when it
  carries a vector column (any `FIXED_SIZE_BINARY`, i.e. every carried
  vector type) or its uncompressed body is under
  `queryNode.materialize.zstdMinBodySize` (default 16 KB); otherwise zstd.
  A size of 0 turns off both rules, for clusters that pay for bytes more
  than for CPU.
  Vector buffers save under 10% and were stored raw anyway after paying
  the full compression; a small reply pays a fixed zstd cost larger than
  the bytes it saves. The size is estimated from the selection before
  encoding (fixed widths × rows plus selected var-len lengths, stopping at
  the threshold), not by trial compression. The reader decides per entry
  and the schema does not depend on the codec, so mixed replies need
  nothing else. The threshold was measured (Impact): 16 KB caps what a raw
  reply gives up at about 10 KB; 4 KB left most of the CPU cost, 64 KB sent
  typical topK-1000 text replies raw at 2.9× the bytes.

### 3. Arrow writer and reader

Materialize writes with arrow-go's IPC encoder (the v17 fork `go.mod`
already uses) or a fused writer built on it, and reads with a small reader
of its own that never builds Arrow arrays (`internal/util/materialize`).

- **Fused writer (every raw batch).** A raw batch — codec `none`, or a
  batch the per-reply rule sends raw — is written in one pass straight from
  the worker's selection into the message:
  - *Per layout, once* (bounded cache): the Schema message, and a template
    RecordBatch message of a one-row batch encoded by the stock writer.
    Every data-dependent value in its metadata (batch length, body length,
    each node's length and null count, each buffer's offset and length) is
    a fixed-size slot that is never FlatBuffers' default 0, so any batch of
    the layout is the template with those slots overwritten.
  - *Per call:* a sizing pass derives every buffer's length by the stock
    writer's rules; one allocation of the exact message size; the template
    copied in and patched; then each selected value copied once (pk and
    system columns from the reduce, user columns from the segment records;
    nulls left zero). The same pass yields `body_raw_size` and
    `result_size`.
  - The output is byte-identical to the stock two-step path (builders,
    then the stock writer), checked by a differential test over every
    supported type, nulls, sliced sources, both pk types and many row
    counts. It handles bool, int8–64, float32/64, utf8, binary and
    fixed_size_binary — everything the export emits.
- **Two-step writer (zstd batches and the rest).** Arrow builders gather
  the selection into a record and the stock writer serializes it, into one
  buffer of the exact encapsulated size (a payload writer of ours; the
  stock `ipc.NewWriter` ignores the minimum-saving option, the payload
  writer path honors it). Writers are pooled per layout and codec, so the
  Schema message is produced once per layout and the zstd encoder is
  reused. Inputs the fused writer declines go here too, and get the errors
  the two-step path reports.
- **Reader.** The proxy parses the Schema message with the stock reader,
  once per fingerprint, and caches the layout. Body entries arrive as the
  pieces gRPC received them in, split anywhere (§6). The reader parses the
  framing across pieces and reads the RecordBatch metadata with FlatBuffers
  accessors copied from the fork (arrow-go keeps them internal; Apache-2.0
  headers kept). It copies out only the O(rows) data: pk and system
  columns and var-len offsets; validity bitmaps and bool values are views,
  copied only when they straddle pieces. Every other value stays in the
  pieces, so a read batch must not outlive its body. It reads the same
  type table as the fused writer and rejects anything else.
- **Bounds.** Every buffer range, node length and null count is checked
  against the entry and the schema, and offsets in full (monotone, within
  the values). A malformed entry is an error, never a panic or an
  out-of-range read.
- **Compressed buffers.** Each is an int64 uncompressed length and a zstd
  frame. Before allocating, the reader bounds the length by what the column
  can need (bitmap, offsets or fixed width for its row count, 64-byte
  padded); var-len values by the reply's `body_raw_size` (2 GiB when
  unknown). It decompresses into exactly that length with a shared decoder
  that cannot write past it, and requires the exact length back.
- **Gather.** `Schema.Gather` copies the selected rows, in output order,
  straight from the pieces into the final `FieldData`, each value once. The
  result equals `queryutil.MaterializeArrowSelection` over the same rows
  (differential test over every carried type, nulls, both pk types and
  both codecs).

**Left for the fork (zstd only).** The stock writer compresses every buffer
before checking the saving, so a batch that mixes text and vectors cannot
compress the text alone; the per-reply rule sends such a batch raw. Each
buffer is its own zstd frame with a fixed setup cost. pk and timestamp are
decompressed with the rest. A fork patch (skip given fields, shared
encoder) would address these; it is a measured follow-up (Phasing).

### 4. Worker (`MaterializeSegments`)

1. Retrieve per segment on the Arrow path and run `ReduceByPKTS` as today;
   the reduce yields records plus row references, without gathering.
2. Write **one record batch per worker** from that selection (§3), not one
   per segment: a requery touches a few rows in many segments, and each
   batch has a fixed cost.
3. Fill the header (§2).

The worker always uses the Arrow retrieve path: `common.interface.zeroCopy`
gates only `Query`, and the proxy cannot see it. The mode is node-local, a
materialize task, not a request field.

It answers `merr.ErrServiceUnimplemented`, so the request falls back (§8),
when it cannot serve the request: a requested output field its exporter
does not carry as Arrow (a newer proxy may treat a type as in scope), an
element-level result, or a limited request.

### 5. Delegator (`Materialize`)

The delegator fans out `MaterializeSegments`; its own segments go through
the in-process worker, whose entry is already in wire form. Then it:

1. decodes each worker's header;
2. requires every non-empty reply to carry the same fingerprint, and any
   schema bytes to hash to it (a mismatch would misattribute columns);
   differing fingerprints across workers are `ErrServiceUnimplemented`
   (version skew, §9);
3. fills its own header: the fingerprint, the first schema bytes, the sums
   of `body_raw_size` and `result_size`, and the statistics. Cost
   aggregation keeps today's rule: all workers' costs when
   `EnableWorkerSQCostMetrics` is on, else only the local node's, by
   `source_id`;
4. applies the size guard (below);
5. sends the workers' entries as its own, in the order today's reduce reads
   them, without copying them (§6).

It never decompresses, decodes, merges or deduplicates field data.
Everything before the reply is unchanged: partial-result evaluation,
retries, marking segments offline. One exception: a worker's
`ErrServiceUnimplemented` fails the request before the partial-result
evaluator runs, so it is never accepted as a partial result.

**Duplicate pks.** One segment can hold a pk more than once (inserted
twice before compaction), and copies can sit in segments on different
workers. Today the worker and the delegator each run `ReduceByPKTS` (keep
the highest timestamp; on a tie the first in merge order), and the proxy
fails a pk seen in two shards with `DataIntegrity`. Materialize keeps the
worker's reduce, moves the delegator's to the proxy with the same rule
(§7), so the relay carries today's rows plus duplicates across workers.

**Size guard.** `Query` applies `maxOutputSize` on the delegator's reduce,
to the shard's deduplicated rows. Materialize bounds the relayed reply
there too, so a shard never sends the proxy W workers × the limit. Each
worker reports in `result_size` what that reduce charges its rows (8 bytes
per system column, a fixed width per user column, a vector only when not
null, a var-len value's length — not `body_raw_size`, which also counts the
pk column and padding). One implementation of this rule serves the worker
and the proxy, pinned to `queryutil`'s by a test. The delegator fails
with `Query`'s error, `merr.WrapErrParameterInvalidMsg("query results
exceed the maxOutputSize Limit ...")`, when the sum exceeds the limit. The
accepted gap: a pk two workers of the shard both return (a handoff's
transient overlap) is counted twice, so near the limit such a shard can be
rejected where `Query` would not.

### 6. Codec and copies

A zero-copy path inside the default `"proto"` codec, keyed on the message
type (`pkg/util/resource`). The wire stays standard protobuf,
byte-identical to `proto.Marshal`.

- **Receive.** gRPC hands the codec a message as pooled transport buffers
  (≤ 16 KB) and frees them once `Unmarshal` returns. The codec parses the
  framing across them, decodes the header (copying it only if it spans
  buffers), and records each body entry as the buffer ranges it covers,
  taking one reference on each buffer so gRPC's free does not return it to
  the pool. Unknown fields are dropped. Malformed input fails with
  `ErrServiceInternal` and holds nothing.
- **Where the body lives.** The generated `Body [][]byte` cannot hold an
  entry that spans buffers, so the codec attaches the body to the message
  in a side table keyed weakly by the message pointer.
  `resource.TakeMaterializeBody` detaches it; over a message without one it
  wraps the plain `Body`, so in-process messages read the same way.
- **Send.** The header and each entry's tag and length go into one small
  buffer; each piece follows as a view holding its own reference. Marshal
  consumes the body, so gRPC's free after writing returns the receive
  buffers to the pool. `MaterializeResults` must therefore never be
  pinnable.
- **Lifetimes.** Whoever takes a body releases it, once (release is
  idempotent; no buffer is freed twice). The delegator takes every reply's
  body, releases them all on any error (including replies of workers that
  succeeded when a sibling failed), and attaches the merged body to its
  reply. The proxy's fill takes each reply's body on arrival and holds it
  until finish, fallback or failure. A message dropped with its body
  attached (gRPC discarding a reply after a late failure) is forgotten by
  the side table once collected: its buffers are garbage collected instead
  of pooled — a pool miss, not a leak. Nothing that outlives the request
  points into a receive buffer.
- **Copies.** Worker: each selected value once (fused writer); twice plus
  compression on the two-step path. Delegator: no body byte, about 100 B of
  bookkeeping per 16 KB frame. Proxy: the O(rows) columns on arrival, each
  selected value once at finish, a compressed buffer once more. gRPC's own
  transport copies come on top. In process (the delegator's local worker, a
  proxy co-located with the delegator) messages pass as Go values with no
  codec, and the body is taken the same way.
- **`Unimplemented` on a node connection.** The gRPC client wrapper
  force-resets a connection on `Unimplemented` only for coordinators, where
  it means a stale connection to a replaced coordinator. For a node client
  it only means an older version (a replaced node is caught by the
  server-ID check), so the call fails fast with `ErrServiceUnimplemented`,
  without retry or reset of the shared connection.

Codec cost (`BenchmarkMaterializeCodec`, 8 entries in 16 KB fragments,
Apple M1 Pro), generic proto codec → this codec, the delegator's relay of
40 KB / 400 KB / 4 MB: 8.4 → 3.2 µs, 57 → 5.9 µs, 443 → 24 µs; allocations
44 KB → 3 KB, 460 KB → 8 KB, 4.2 MB → 68 KB.

### 7. Proxy

Shards do not matter once a request is routed. The proxy treats the body
entries of all replies as one flat list and fills the final result straight
from them, with no per-shard reduce and no cross-shard query pipeline.

1. **Read on arrival.** On each shard's goroutine, once the reply is OK,
   the proxy resolves its schema (§2), reads each entry in place (§3), and
   hands the reply's body to the fill, which holds it until release.
   Reading runs outside the fill's lock, and so does a requery's slot lookup
   (its pk table is complete from the start); a filter query's slot
   insertion takes the lock.
2. **Fill.** One slot per distinct pk — its source (entry, row), shard and
   timestamp — in one pk table typed by the collection's pk type.
   - **Requery:** the slots are built up front from the ids the requery
     asks for (`QueryTask.ids`, set with its `pk IN ids` plan), in search
     order. A row whose pk was not asked for is dropped. A pk asked for
     twice shares one slot and appears twice in the output.
   - **Filter query:** a slot is added when its pk first arrives, so the
     output is in **arrival order** (replies as they arrive, entries in
     relay order, rows in pk order within an entry), **not sorted by pk**
     as `Query`'s is. This is intended: pk order served limit/offset
     paging, the pk-cursor iterator, limit early stop and merge dedup,
     none of which Materialize takes.
   - **Duplicates:** from the same shard, a strictly greater non-zero
     timestamp replaces the row, so ties keep the first seen, as the
     delegator's `ReduceByPKTS` does today. From two shards:
     `merr.WrapErrDataIntegrityMsg("duplicate PK %v found across
     shards")`, today's error.
3. **Errors stay away from the load balancer.** The load balancer
   blacklists a node on a non-retriable error, but a proxy-side failure (a
   body or schema it cannot read, a cross-shard duplicate) is not the
   delegator's fault. Such failures are kept on the fill (first one wins)
   and the per-shard call returns success, so no node is blacklisted and no
   other replica is tried. A fallback cause (§8) is kept the same way, with
   its reason, and takes precedence over any other error, a shard's failed
   call included (the rerun meets that error again if it persists); once
   one is recorded, later replies are released unread. After the fan-out:
   on a fallback the fill is discarded and the request reruns with
   `Query`; else any error fails the request; else the fill finishes. The
   per-shard side effects `Query` applies on a reply (the channel's MVCC
   timestamp, the load balancer's cost metrics) are applied only once the
   fill has finished, so a request that falls back reports only its
   `Query` replies; shard-leader cache invalidation on a failed call stays
   immediate.
4. **Finish.** First the size guard, as `Query` applies it: per shard, on
   the deduplicated rows, each slot charged once by the rule of §5 (a pk a
   requery asks for twice counts once). Over the limit the request fails
   with today's error. Then one gather (§3) copies every selected value
   into the final `FieldData` in slot order, with the pk and timestamp
   columns, and the fill releases the bodies. A requested pk that no reply
   returned is left out. The result goes through `PostExecute` as the only
   result (the pk reduce passes a single result through), then
   `complement_fields`, Geometry WKB → WKT and struct reconstruction run as
   today.
5. **Requery: organize skips its gather.** When `organize` reorders by one
   ids list and its fields' pk column already holds exactly those ids in
   that order, it passes the fields on as they are. It checks this itself;
   nothing passes between the requery and organize, and a `Query` result in
   that order takes the same path. In practice:
   - search with requery or ORDER BY requeries the deduplicated ids and
     organizes by the search result's: the fast path holds for one query,
     or several whose hits do not repeat;
   - rerank and hybrid search requery the rank result's ids, repeats
     included, and organize by the same ids: the fast path holds for any
     nq;
   - hybrid search with rerank by field data reads the requery's fields in
     two organize nodes. The pipeline builder turns the fast path off for an
     organize node whose fields input another node also reads, so its
     output never aliases shared fields.

   A missing requested id takes the general path and reports
   `InconsistentRequery`, as today. Deduplicating the requery ids now keeps
   first-hit order (it was map order); `Query`'s results do not depend on
   it.

**Values under nulls** are zero placeholders rather than whatever segcore
stored there. Clients do not read them; equivalence tests normalize them.

**Copies.** Each selected value is copied once on the proxy, however many
shards there are. `Query` gathers once on the delegator, again across
shards on the proxy, and a requery's `organize` once more.

### 8. Routing and rollout

The proxy calls `Materialize` when `proxy.materialize.enabled` is true and
the request is in scope; otherwise `Query`. Turn the switch on after the
rolling upgrade. Turning it on earlier, or downgrading with it on, may
return errors; the fallback below covers part of that. Turning it off
returns the cluster to `Query` with nothing to clean up.

**Fallback.** These causes rerun the whole request with `Query`, and the
client does not see them:

- a node that answers gRPC `Unimplemented` (no such RPC);
- a worker that cannot serve the request (§4), or workers of one shard
  with different schemas (§5); the delegator fails the whole `Materialize`
  with `ErrServiceUnimplemented` and never converts an old worker's
  `FieldData`;
- two shards whose replies carry different schemas.

The proxy records the cause on the fill and returns success to the load
balancer (§7), so no node is blacklisted. After the fan-out it discards
everything read from the Materialize replies and reruns with `Query`: the
two result forms never mix within a request.

### 9. Errors

- **Version skew:** differing schema fingerprints, across workers or
  across shards, are `ErrServiceUnimplemented` and fall back (§8). An
  encoding change that every worker agrees on does not show in the
  fingerprint, so the request also carries `materialize_format_version`. A
  node that does not use that version answers `ErrServiceUnimplemented`,
  which keeps a mid-upgrade proxy on `Query`.
- **Malformed response:** a batch that disagrees with its schema, a buffer
  or offset outside its entry, a compressed length beyond what its column
  can need or not matching what the frame decodes to, an undecodable body
  (including an unknown codec), schema bytes that do not hash to their
  fingerprint, or a reply without schema bytes whose fingerprint the
  request did not name. These are `ErrServiceInternal` — a Milvus bug,
  never input — with the field and range named and the decoder's error as
  the cause. They are not retried, do not fall back, and stay away from
  the load balancer (§7). A cross-shard duplicate pk (`DataIntegrity`)
  takes the same route.
- **Oversized results:** checked on the delegator and again on the proxy
  (§5, §7). The bodies are received in full before the proxy's check, as
  `Query`'s reply is built in full by the delegator. The reader allocates
  only O(rows) columns and bounded decompression targets (§3), so a corrupt
  length or frame is an error, not a large allocation.
- **Transient errors** (not ready, timeout, cancel) keep their codes, from
  the same steps as today. Materialize adds no retry of its own.

## Impact

Production figures come from the cluster above. The rest come from
`BenchmarkMaterializePipeline` (Verification plan): production code on
every hop, Apple M1 Pro, optimized build, N = 8 workers per shard. None
is a production measurement of Materialize. Single cells spread ±20%; read
differences under about 15% as noise, except worker CPU, which is stable.

Shapes: S = 1 or 4 shards, topK for the whole request, output fields
vec768, text (Zipf over a 2500-word vocabulary, ≥ 300 characters per row)
or both, each with the pk. Request shapes: a filter query; a requery of one
query; a requery of four queries, a quarter of whose hits repeat another's.
Two times per request:

- **Critical path:** the slowest shard's chain (its slowest worker, its
  delegator's slowest single receive, the delegator's serial part, the
  proxy's handling of that reply on arrival), then the proxy's work after
  the fan-out (cross-shard pipeline for `Query`, the fill's finish for
  Materialize, then `organize` for a requery).
- **Total CPU:** every step summed, over all workers and shards.

### Latency and CPU, compression off

µs per request, `Query` / Materialize:

| shape | fields | S | topK | critical path | total CPU | worker | delegator | proxy |
|---|---|---|---|---|---|---|---|---|
| filter | vec768 | 1 | 100 | 135 / 58 | 254 / 98 | 105 / 39 | 113 / 15 | 34 / 44 |
| filter | vec768 | 1 | 1000 | 1090 / 390 | 1984 / 704 | 836 / 362 | 892 / 45 | 255 / 309 |
| filter | vec768 | 4 | 100 | 108 / 60 | 472 / 222 | 213 / 83 | 160 / 44 | 100 / 92 |
| filter | vec768 | 4 | 1000 | 729 / 419 | 2752 / 1124 | 1011 / 502 | 986 / 101 | 760 / 521 |
| filter | text | 1 | 100 | 46 / 44 | 100 / 71 | 45 / 25 | 38 / 12 | 16 / 33 |
| filter | text | 1 | 1000 | 386 / 220 | 679 / 318 | 237 / 115 | 294 / 19 | 148 / 184 |
| filter | text | 4 | 100 | 51 / 47 | 299 / 203 | 149 / 76 | 94 / 45 | 56 / 80 |
| filter | text | 4 | 1000 | 427 / 171 | 1222 / 497 | 359 / 164 | 379 / 46 | 483 / 280 |
| filter | vec768 + text | 1 | 100 | 154 / 63 | 307 / 109 | 126 / 46 | 137 / 14 | 43 / 49 |
| filter | vec768 + text | 1 | 1000 | 1238 / 454 | 2348 / 871 | 981 / 460 | 1022 / 46 | 348 / 362 |
| filter | vec768 + text | 4 | 100 | 118 / 83 | 623 / 302 | 287 / 126 | 205 / 48 | 132 / 120 |
| filter | vec768 + text | 4 | 1000 | 825 / 401 | 3375 / 1205 | 1242 / 582 | 1178 / 90 | 965 / 534 |
| requery, 1 query | vec768 | 1 | 100 | 164 / 58 | 278 / 101 | 102 / 43 | 112 / 15 | 66 / 44 |
| requery, 1 query | vec768 | 1 | 1000 | 1380 / 373 | 2272 / 675 | 827 / 333 | 895 / 46 | 564 / 298 |
| requery, 1 query | vec768 | 4 | 100 | 119 / 57 | 470 / 207 | 203 / 80 | 152 / 44 | 119 / 82 |
| requery, 1 query | vec768 | 4 | 1000 | 1128 / 320 | 3262 / 874 | 1089 / 416 | 1076 / 88 | 1106 / 375 |
| requery, 1 query | text | 1 | 100 | 58 / 33 | 116 / 57 | 47 / 22 | 42 / 11 | 27 / 24 |
| requery, 1 query | text | 1 | 1000 | 391 / 132 | 620 / 210 | 168 / 82 | 239 / 15 | 213 / 113 |
| requery, 1 query | text | 4 | 100 | 75 / 43 | 340 / 190 | 164 / 75 | 103 / 44 | 73 / 71 |
| requery, 1 query | text | 4 | 1000 | 391 / 139 | 1041 / 392 | 304 / 163 | 294 / 46 | 453 / 183 |
| requery, 1 query | vec768 + text | 1 | 100 | 184 / 59 | 339 / 103 | 128 / 44 | 136 / 14 | 74 / 45 |
| requery, 1 query | vec768 + text | 1 | 1000 | 1648 / 489 | 2824 / 884 | 1046 / 450 | 1066 / 44 | 713 / 395 |
| requery, 1 query | vec768 + text | 4 | 100 | 148 / 74 | 636 / 264 | 281 / 114 | 199 / 46 | 157 / 104 |
| requery, 1 query | vec768 + text | 4 | 1000 | 1043 / 437 | 3569 / 1188 | 1258 / 610 | 1129 / 92 | 1174 / 493 |
| requery, 4 queries | vec768 | 1 | 100 | 170 / 96 | 289 / 136 | 105 / 44 | 115 / 17 | 72 / 70 |
| requery, 4 queries | vec768 | 1 | 1000 | 1419 / 701 | 2321 / 1040 | 822 / 386 | 891 / 52 | 612 / 608 |
| requery, 4 queries | vec768 | 4 | 100 | 128 / 97 | 483 / 254 | 201 / 88 | 157 / 47 | 124 / 120 |
| requery, 4 queries | vec768 | 4 | 1000 | 1166 / 636 | 3268 / 1221 | 1081 / 467 | 1081 / 91 | 1104 / 688 |
| requery, 4 queries | text | 1 | 100 | 71 / 45 | 134 / 69 | 53 / 23 | 48 / 11 | 33 / 34 |
| requery, 4 queries | text | 1 | 1000 | 417 / 248 | 649 / 329 | 167 / 87 | 254 / 16 | 225 / 226 |
| requery, 4 queries | text | 4 | 100 | 69 / 59 | 326 / 210 | 156 / 78 | 99 / 45 | 71 / 87 |
| requery, 4 queries | text | 4 | 1000 | 389 / 282 | 1045 / 564 | 301 / 185 | 292 / 49 | 456 / 329 |
| requery, 4 queries | vec768 + text | 1 | 100 | 189 / 89 | 343 / 134 | 128 / 46 | 136 / 15 | 79 / 75 |
| requery, 4 queries | vec768 + text | 1 | 1000 | 1826 / 800 | 3068 / 1247 | 1128 / 503 | 1136 / 47 | 811 / 698 |
| requery, 4 queries | vec768 + text | 4 | 100 | 151 / 95 | 643 / 281 | 282 / 110 | 198 / 46 | 163 / 127 |
| requery, 4 queries | vec768 + text | 4 | 1000 | 1104 / 704 | 3690 / 1477 | 1292 / 636 | 1150 / 93 | 1246 / 749 |

- **Critical path:** shorter in every shape. Requery of one query −42% to
  −73%; four queries −14% to −56%, where the proxy's reorder of repeated
  hits dominates; filter queries, which are not routed here (Scope), −4% to
  −64%.
- **Total CPU:** −29% to −73%. The fused writer makes even a reply of a few
  rows cheaper than `Query`'s `FieldData` (text topK 100, S = 4, 32
  workers of ~3 rows: worker 75 vs 164 µs).
- **What remains on the worker** is the copy of the selected bytes, the
  zeroing of the message buffer, and about 1 µs of fixed cost per batch.

### Latency and CPU, zstd

`Query` with gRPC whole-message zstd (the delegator decompresses, merges
and recompresses) against Materialize with `codec: zstd` and the per-reply
rule; requery of one query, median of 5 runs. Each cell is `Query` /
Materialize; the last column is Materialize with codec `none` (critical /
total µs) for reference:

| fields, S, topK | critical µs | total CPU µs | worker µs | proxy µs | d2p KB | `none` |
|---|---|---|---|---|---|---|
| vec768, 1, 100 | 425 / 61 | 771 / 104 | 317 / 44 | 142 / 44 | 308 / 312 | 58 / 101 |
| vec768, 1, 1000 | 4246 / 387 | 6534 / 686 | 2026 / 338 | 1467 / 299 | 3061 / 3098 | 373 / 675 |
| vec768, 4, 100 | 210 / 56 | 1071 / 211 | 547 / 83 | 202 / 84 | 308 / 319 | 57 / 207 |
| vec768, 4, 1000 | 1604 / 342 | 6504 / 900 | 2177 / 440 | 1712 / 374 | 3077 / 3105 | 320 / 874 |
| text, 1, 100 | 530 / 34 | 1068 / 61 | 462 / 26 | 135 / 24 | 15 / 50 | 33 / 57 |
| text, 1, 1000 | 5086 / 1901 | 9509 / 4698 | 4029 / 3311 | 1277 / 1374 | 128 / 163 | 132 / 210 |
| text, 4, 100 | 261 / 58 | 1834 / 231 | 837 / 97 | 231 / 84 | 18 / 60 | 43 / 190 |
| text, 4, 1000 | 1584 / 151 | 9985 / 448 | 4223 / 204 | 1492 / 196 | 140 / 485 | 139 / 392 |
| vec768+text, 1, 100 | 1146 / 62 | 1756 / 109 | 624 / 47 | 308 / 47 | 327 / 358 | 59 / 103 |
| vec768+text, 1, 1000 | 8546 / 574 | 14128 / 1038 | 4843 / 522 | 2521 / 465 | 3187 / 3548 | 489 / 884 |
| vec768+text, 4, 100 | 335 / 70 | 1891 / 260 | 1027 / 115 | 261 / 100 | 353 / 368 | 74 / 264 |
| vec768+text, 4, 1000 | 3406 / 424 | 14838 / 1162 | 4975 / 614 | 3084 / 466 | 3263 / 3559 | 437 / 1188 |

A worker reply here is 2–60 KB of text or 12–440 KB with a vector. Only
text, S = 1, topK 1000 (59 KB per reply) stays zstd. It is the one shape
where Materialize spends more proxy CPU than `Query` (1374 vs 1277 µs),
though less in total, and its critical path is ~14× its codec-`none` time.
Materialize's critical path and total CPU are below `Query`'s in all twelve
(−63% to −95%, −51% to −96%). The CPU comes back as bytes (next section).

Against `Query`'s other options on a compressed cluster, same shapes, median
of 5 runs (critical µs / total CPU µs / delegator → proxy KB):

| fields, S, topK | gRPC zstd | gRPC s2 | gRPC zstd, delegator → proxy raw | Materialize zstd |
|---|---|---|---|---|
| text, 1, 1000 | 4122 / 7853 / 128 | 2052 / 3516 / 183 | 911 / 4490 / 451 | 1693 / 4302 / 163 |
| text, 4, 1000 | 1585 / 9930 / 140 | 850 / 4016 / 197 | 638 / 6187 / 451 | 163 / 462 / 485 |
| vec768, 1, 1000 | 4504 / 6852 / 3061 | 3435 / 5496 / 3077 | 1936 / 4300 / 3077 | 437 / 754 / 3098 |
| vec768+text, 4, 1000 | 3548 / 14828 / 3263 | 1852 / 7247 / 3493 | 1801 / 8988 / 3524 | 598 / 1453 / 3559 |

Where the replies stay compressed (text, S = 1, topK 1000) Materialize has
no clear lead over s2 or a raw last hop: s2 spends less CPU, and the raw hop
has the shorter critical path at 2.8× the bytes. Elsewhere Materialize is
ahead on latency and CPU, but on text it sends raw what s2 compresses
(485 vs 197 KB).

Choosing the threshold, text shapes (critical / total µs, d2p KB): 4 KB
sent only the 2 KB replies raw and left text S = 4 topK 1000 at 896 / 6435
/ 197; 16 KB also covers the 6 KB and 15 KB replies (169 / 518 / 473); 64
KB also sent the 59 KB replies raw (S = 1 topK 1000: 167 / 262 / 475,
against 1778 / 4434 / 160).

What stays on the critical path in every case: the wait for the slowest
worker, the network, the worker's encode and the proxy's decode. How much
of the production requery's 6.86 ms Materialize removes is **not
established**; it needs a profile that separates the requery (Open
questions) and a loopback benchmark with real gRPC. These benchmarks do not
charge the network for the extra bytes.

### Bytes

- **Compression off:** +1% on vectors, +4 to +10% on text, on both hops
  (Arrow framing, offsets, the pk and timestamp columns).
- **zstd, delegator → proxy, against `Query`'s whole-message zstd:**
  - text replies under 16 KB travel raw: 3.2–3.5× `Query`'s bytes (S = 1
    topK 100: 49 vs 15 KB; S = 4 topK 1000: 473 vs 137 KB);
  - text that stays compressed: +28% (160 vs 125 KB), because each worker's
    buffers are compressed on their own, with no matches across workers
    and a frame per buffer (at N = 1 the gap is +6 to +9%);
  - vector + text: +9 to +11% (3465 vs 3113 KB), since the text next to the
    vectors is no longer compressed;
  - pure vectors: +1%, as with compression off.

  Where bytes cost more than CPU (cross-AZ), these shapes lose to `Query`;
  compressing text but not vectors in one batch needs the fork patch (§3).
- **Cross-worker duplicate pks** now also cross the delegator → proxy hop.
  They are rare.
- Vectors are never compressed. They did not compress in the data
  measured; real embeddings may lose a few percent.

### Memory

- **Delegator:** holds the workers' receive buffers, referenced, until its
  reply is written. Today it holds decoded messages plus the merged copy
  (14.1 MB per request at vec768 + text topK 1000, offline).
- **Proxy:** holds every shard's receive buffers until the fill finishes,
  plus the O(rows) columns and the gathered `FieldData`. Its peak is about
  the sum of the shards' raw bodies (duplicates, pk, system columns and
  padding included) plus the result, where `Query` can drop each shard's
  decoded reply into the merge. A known cost.

### Compatibility

- **`QueryNodeSQCount{fail}` on the delegator.** The delegator-side read
  path `Query` and Materialize share now counts a request whose channel has
  no delegator (`ChannelNotFound`) as failed. Before, it was counted in
  `total` but never in `fail`. Fixing the undercount shows as a rise in the
  failure rate on nodes that receive misrouted requests.
- A request a worker cannot serve is rerun with `Query` (§8), not failed.
- Filter queries stay on `Query` in this phase (Scope), so no client sees
  arrival order.

## Observability

- `milvus_proxy_materialize_shard_total{node_id}`: shard replies a finished
  Materialize result was built from; requests that fell back or failed are
  not counted.
- `milvus_proxy_materialize_fallback_total{node_id, reason}`:
  `unimplemented` (a node without the RPC), `unsupported` (a node that
  cannot serve the request: a field, an element-level result, a limited
  request, workers of one shard with different schemas), `schema_skew`
  (shards with different schemas).
- The QueryNode reuses `QueryNodeSQCount` and `QueryNodeSQReqLatency` for
  both RPCs, labelled like `Query` (`Leader` or `FromLeader`). They do not
  tell Materialize from `Query`; the proxy counters do.
- `milvus_proxy_query_execute_latency{node_id, query_type, path}`: the
  fan-out of a query task, `path` being `query`, `materialize` or `fallback`
  (Materialize, then the request again on `Query`). It compares the paths
  in a canary.
- Deferred to phase 2: body bytes per hop, compressed and raw, and sampled
  per-column compression ratios; latency metrics of the RPCs' own.

## Verification plan

1. **Equivalence.** The proxy's `QueryResults` from Materialize equals
   `Query`'s, after normalizing values under nulls and, for a filter query,
   row order. Cover every in-scope type, nullable and dynamic fields,
   partition keys, RLS; a requery and an unlimited query; N = 1, 2, 8 and S
   = 1, 4; duplicate pks within a segment, across segments, across workers
   and with equal timestamps.
2. **Arrow interoperability.** The body with the schema prepended reads
   with stock arrow-go and Arrow C++.
3. **Failure modes, each traced to its outcome:**
   - schema fingerprints differing across workers or shards; schema bytes
     not matching their fingerprint; a reply omitting a schema the request
     did not name;
   - a corrupt batch or buffer: `ErrServiceInternal`, never a panic
     (differential tests against the stock reader over entries cut at every
     offset, malformed metadata and zstd cases, a fuzz target);
   - a receive buffer read after gRPC freed it (freed buffers poisoned,
     pool gets and puts counted);
   - one shard failing while others succeed: the request fails;
   - `maxOutputSize`: a shard's deduplicated rows over it; the delegator's
     summed `result_size` over it, and under it while the raw bodies sum
     over it; cross-worker duplicates whose bodies sum over it while the
     kept rows do not (accepted, as `Query` accepts them);
   - `Unimplemented` on a node connection: no reset, no retry; on a
     coordinator connection: reset;
   - a client cancel during the delegator's send: every reference
     released;
   - a missing requery id (`InconsistentRequery`); a segment released
     between search and requery;
   - out-of-scope requests routed to `Query`; the switch off by default;
   - an old worker behind a new delegator, and an old delegator: both end
     in `Query` with an equal result, no node blacklisted;
   - a newer proxy requesting a field an older worker cannot carry;
   - a worker's `ErrServiceUnimplemented` with partial results enabled:
     never accepted as partial.
4. **Performance.** `BenchmarkMaterializePipeline`
   (`internal/proxy/dql/materialize_pipeline_bench_test.go`) runs both
   paths on the production code from the workers' reduce output to the
   proxy's final result, over the shapes of Impact, with codec `none` or
   `zstd`. It models gRPC's receive (16 KB pooled buffers freed after
   `Unmarshal`) and the proxy's concurrent per-shard receive; each
   worker's rows are scattered over 8 segment records. Its companion tests
   check, with freed buffers poisoned, that both paths return the same rows
   and that every buffer went back to the pool, and pin the arrival order.
   Still to do: a loopback gRPC benchmark of the relay, and a production
   A/B on the cluster above.

## Phasing

| phase | scope | depends on |
|---|---|---|
| 0 | independent: FieldData presize (#54371) | none |
| 1 | this design: RPCs, wire, fused and two-step writers, the reader, the zero-copy codec path, the delegator relay, the proxy flat fill, per-reply codec, the switch (default off) | the worker's Arrow retrieve (20261005 doc) |
| 2 | fork patch: skip compressing given fields, shared zstd encoder, so a batch can compress text and not vectors | phase 1, measured on a compressing cluster |
| 3 | the proxy writes the SDK `fields_data` straight from Arrow (≈ −50% proxy CPU on vectors, offline); ARRAY and sparse as native Arrow; Vortex once its FFI exposes IPC | |

## Alternatives considered

- **A protobuf container per column group** (an earlier draft). Arrow IPC
  already compresses per buffer, encodes the columns and gives the offsets
  for partial access; a container adds only a second framing.
- **A flag on `Query` instead of new RPCs.** A gRPC method has one response
  type, and an old node would ignore the flag silently instead of answering
  `Unimplemented`.
- **Deduplicating on the delegator.** It costs header decoding on the
  per-shard bottleneck to do what the proxy does with the same rule.
- **Limited queries through Materialize.** Without a merge on the
  delegator, the proxy would receive N × `limit` rows per shard; pruning
  brings back the intermediate merge.
- **Proxy → workers directly.** It removes the delegator hop, but needs
  per-hit source workers, a fallback for segments released between search
  and requery, and proxy × QueryNode connections. A larger change that can
  reuse this wire format.
- **A streaming variant, or reusing `QueryStream`.** `QueryStream` already
  relays worker messages unmerged, but as gRPC-compressed
  `RetrieveResults`. Streaming would overlap transfer with the wait for the
  slowest worker, but the proxy's deduplication must wait for a whole shard
  anyway, and a unary reply keeps fallback and error handling simple. A
  follow-up.
- **A complete writer of our own, with per-column compression.** The fused
  writer only patches templates the stock writer produces, for raw batches;
  writing compressed batches too would mean owning much more of Arrow's
  internal IPC bindings. The fork patch (Phasing) gets per-column
  compression more cheaply.
- **Compression changes alone:** s2 instead of zstd, or column compression
  inside `FieldData`. s2 costs 30–35% more bytes and needs every node
  upgraded; neither removes the delegator's decode and merge.

## Open questions

1. A delegator profile that separates the requery from search, and one
   from a cluster without gRPC compression, to size decode and merge there.
2. Real-data compression of text and JSON, zstd per buffer against
   whole-message zstd.
3. A loopback gRPC benchmark, to charge the extra bytes under zstd to
   latency.
