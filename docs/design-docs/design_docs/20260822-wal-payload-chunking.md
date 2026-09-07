# WAL payload chunking: oversized records split at the storage layer

- Status: Implementing (this PR)
- Date: 2026-08-22
- Scope: `pkg/streaming/util/message/{builder,chunk}.go`, `pkg/util/fastpb/insert_request_*.go`, `internal/streamingnode/server/wal/adaptor/{wal_adaptor,scanner_adaptor}.go`, `internal/streamingnode/server/flusher/flusherimpl/wal_flusher.go`, `internal/streamingnode/server/service/handler/producer/produce_server.go`, `internal/proxy/{task_insert_streaming,task_delete,task_upsert_streaming}.go`, `internal/proxy/channelmgr/msg_pack.go`, `pkg/util/paramtable/{component_param,service_param}.go`
- Related: #52474 (woodpecker.maxMessageSize); supersedes the size-packing half and reuses the direct-encoding half of the `insert-repack-view-encoder` line of work

## 1. Problem

Pulsar and Kafka enforce hard caps on a single record: Pulsar rejects above
`pulsar.maxMessageSize` (default 2 MiB, and the broker's own cap applies
regardless), and Kafka above `message.max.bytes`. Woodpecker has no equivalent
single-entry hard cap; this PR adds `woodpecker.maxMessageSize` as a Milvus WAL
chunk threshold so Woodpecker records use the same bounded granularity. When a
record crosses an enforced backend cap, the failure surfaces as an append error that
`appendOneWithRetry` classifies as recoverable — an infinite backoff loop.
Nothing between the producer and the broker can shrink the message, so **one
oversized insert permanently stalls the pchannel's write path**.

The proxy's row packing targets the Pulsar-shaped threshold, but it budgets
entity bytes only: the final materialized record adds the streaming header,
properties, and cipher expansion on top, and only the partial-update-CAS path
re-validates and re-splits the built message. A normal insert whose final
envelope crosses the backend limit — envelope growth, cipher expansion, or any
drift between the Milvus config and the broker's — reaches the WAL as one
oversized record.

## 2. Design

**P1 — Chunk at the storage layer, below the interceptor chain.** The payload
is an opaque byte blob there: no protobuf is unmarshaled or re-marshaled. The
exact bytes the backend would have stored are sliced in place. Every
interceptor (txn, timetick, fencing) and every consumer above the WAL sees
complete messages only — chunking is invisible to them.

**P2 — The reader reassembles before any interpretation.** The scanner
adaptor feeds every incoming message through a `ChunkAssembler` at the head of
`handleUpstream`, before filtering, reordering, or the txn buffer. A chunk is
never a valid message body, so nothing downstream ever parses one.

**P3 — Chunks are self-describing; the log needs no contiguity.** Every
chunk carries the original message's time tick (unique per message on a
pchannel) plus its index/total markers, so packs may interleave freely with
any other traffic in the log — the consumer pairs chunks by time tick, not
by adjacency. No write-side coordination is added on a path where master
runs appends fully concurrently.

**P4 — The successful first-chunk attempt's message ID is the logical message
ID.** The append caller is acked with the ID returned by the successful append
of chunk 0. A backend may persist an earlier attempt and still return an error,
so the reader replaces a payload-identical duplicate slot with the later log
observation. The reassembled message therefore carries the same successful
chunk-0 ID whether it comes from WAB tailing or durable catch-up.
`LastConfirmedMessageID` remains conservative: it is derived from the logical
ID after every chunk of the run has already been persisted.

### 2.1 Chunk format

```
payload (bytes)  ──slice──▶  [ c0 | c1 | ... | cN-1 ]   each ≤ limit - reserve
chunk record     =  payload slice + FULL clone of the original properties
                    + _ci (0-based index) + _ct (total count)
reassembled      =  concat(slices), properties of c0 minus _ci/_ct, ID of c0
```

Every chunk carries the complete property set, not a skeleton: backends and
walimpls-level delivery filters make per-record decisions from properties, so
each chunk must be delivered exactly where the whole message would have been.
The reserve (`pulsar.messageReserveSize`, default 64 KiB) absorbs the
per-record envelope — properties clone, cipher metadata/expansion, broker
metadata — so a chunk record never crosses the backend cap. The *effective*
reserve is clamped to at least 1 KiB: a smaller value cannot cover any
envelope, so a full-budget chunk would still be rejected and retried forever —
the very stall this design removes. A configured reserve below the minimum, or
one that does not fit under the active limit, falls back to the 64 KiB default.
Every bounded WAL message-size configuration is itself clamped to at least
256 KiB, so the default reserve always fits. External Pulsar broker/proxy and
Kafka broker/topic caps must be no smaller than the corresponding effective
Milvus limit; clamping Milvus configuration cannot raise an external cap.

`_ci`/`_ct` are reserved: chunks are created below the interceptor chain and the
markers are stripped again on reassembly, so a message arriving at the produce
API can never legitimately carry them. `ProduceServer.validateMessage` rejects
one that does. Without that check a foreign record would be read back as a
corrupted chunk run and fail-stop the pchannel (§2.3), turning a bad input into
a channel-wide outage.

`SplitIntoChunks` returns the message unchanged when the payload fits or the
budget is zero (for an unbounded backend such as RocksMQ); `IsChunkedPayload`
recognizes a record carrying either reserved marker, and the assembler then
requires the `_ci`/`_ct` pair to be valid.

### 2.2 Write path

Two independent, live-refreshable switches control the write path:

- `proxy.splitChunk` defaults to `true`. While true, Proxy retains the
  legacy row-based size packing path; while false, it builds one logical
  message per channel/partition.
- `streaming.splitChunkSN` defaults to `false`. While false, StreamingNode calls
  `appendOneWithRetry` without creating physical chunks; while true, it computes
  the backend payload budget and runs
  `SplitIntoChunks(msg, chunkPayloadSize())` → `appendOneWithRetry` per chunk →
  return chunk 0's ID.

Records that fit, and backends without a per-record cap, still use the
single-record path. Separating the switches gives the rollout a safe bridge
state: enable SN chunking everywhere while Proxy packing remains enabled, then
disable Proxy packing. There is no assignment watch or global StreamingVersion
dependency in the append path.

There is no lock anywhere on this path: master runs
appends fully concurrently and that property is preserved for all traffic,
oversized included. A run that fails unrecoverably midway leaves the caller
un-acked with a partial run in the log; the reader never assembles it and the
scanner keeps it incomplete, while the client's retry writes a fresh run under
a newly assigned time tick.

`chunkPayloadSize()` = `WALMaxMessageSize(backend) - reserve`, served from a
1-second cache because `GetAsInt` on refreshable items is not per-append hot
path work. A downward live config refresh takes effect within that window.

`WALMaxMessageSize` is the single place a WAL name maps to its chunking limit:
Pulsar and Kafka return their effective configured per-record limits, while
Woodpecker returns the Milvus-configured `woodpecker.maxMessageSize` threshold.
All three configuration items clamp numeric values below 256 KiB to 256 KiB;
malformed or out-of-range values use their shipped backend default. **Anything else,
including RocksMQ, returns 0**, which disables chunking entirely: RocksMQ's
page size is not a per-entry cap, and its pre-chunking behavior (store the
oversized record as-is) is preserved.

### 2.3 Read path

`ChunkAssembler.Push(msg)` at the head of `handleUpstream`, pairing chunks
into per-time-tick runs (packs may interleave; §3):

| Input | State | Result |
|---|---|---|
| ordinary non-chunk message | state untouched | process normally |
| TimeTick T | discard incomplete runs at or below T | process normally |
| first chunk of an unseen time tick | open a run | swallow |
| chunk filling a missing slot of its run | buffer at its index | swallow; if all `_ct` slots filled → emit reassembled message |
| chunk duplicating an already-filled slot payload byte-for-byte | redelivery (persisted-but-unacked retry rewrites it under a new message ID) | replace the slot with the later observation, then swallow |
| malformed markers, same slot with different bytes, or total mismatch inside one time tick | corruption | fail the scanner; the flusher marks the current WAL unavailable, so recovery cannot advance its checkpoint and new writes are rejected |
| middle chunk of an unknown time tick | nothing joinable | swallow |

There is no count/byte limit and no silent eviction. The number of concurrently
open runs does not prove that the oldest writer is dead; rejecting or evicting
it can make a successfully persisted WAL impossible to replay or silently lose
a message whose writer later persists the remaining chunks and is acknowledged.
The next TimeTick is a safe cleanup barrier: after observing T, no live writer
can still append chunks for a run whose time tick is at or below T. The
assembler discards those proven-orphan runs while retaining newer runs.

An interrupted run never completes on its own. It remains local until a
TimeTick proves it orphaned (or the scanner closes); the producer was never
successfully acknowledged, and a client retry uses a new timetick. Durable WAL
records pass through one assembler before legacy-v0 conversion and all later
filter/reorder/transaction handling. Tailing reads use the write-ahead buffer's
already-logical messages. A corrupt run is propagated through the producer
loop and fail-stops the scanner, so recovery cannot move its checkpoint past
incomplete acknowledged data.

### 2.4 Proxy side: size-based packing retained as the compatibility mode

While `proxy.splitChunk=true`, Proxy keeps its existing size-driven packing.
Once that switch is disabled (after `streaming.splitChunkSN=true` is active on
every possible SN owner), Proxy uses the logical-message path:

- **Insert**: one message per (channel, partition) group. Existing
  `InsertRequestViewEncoder` borrows the source columns and ordered row offsets,
  computes the exact wire size, and writes the selection directly into the
  final protobuf payload through `WithBodyEncoder`. It does not materialize
  destination `FieldsData`, row-ID, or timestamp slices. The normal builder
  still owns header creation, optional encryption, and the final payload.
  There are no entity-size estimates, envelope re-validation, or single-row
  rejection: chunking slices bytes without row semantics, so even one row can
  ride through up to the logical-message bound in §2.5.
- **Delete**: one tombstone batch per hashed channel, bounded by the 16 MiB
  logical-message limit in §2.5. This also eliminates a latent master bug where
  a first PK already over the old backend-derived packing limit produced an
  empty first `DeleteMsg` that was still allocated an ID and appended.
- **Partial-update CAS** survives everything by construction: it is written
  into the encoder's fresh `InsertRequest` template before exact-size planning,
  then `MarkPartialUpdateCASForBodyEncoder` adds the message-property marker.
  Both ride along — body verbatim through slicing/reassembly, properties via
  the full per-chunk clone (§2.1).
- The proxy→SN transport is not the bottleneck: the streaming gRPC channel
  pins all four message-size caps at 256 MB (`streamingNode.grpc.*`,
  `configs/milvus.yaml`), far above any realistic record.

The accepted price is memory granularity: while assembling an N-byte logical
message, one scanner temporarily retains about N bytes across the physical
chunk payloads and allocates another contiguous N-byte reassembly buffer. Peak
assembly memory is therefore about 2N per scanner, before any additional
downstream protobuf decoding, and multiple scanners on the same pchannel can
multiply that transient cost. After reassembly returns, the chunk references
become reclaimable and the contiguous N-byte message remains as one unit for
the flusher or txn buffer. Re-*splitting* it would reintroduce size-based
packing somewhere; deliberately NOT done. Re-*bounding* it is a different
question, answered next.

### 2.5 Bound Proxy-materialized logical writes

Removing size-based packing removes the backend-record bottleneck, but it also
makes one logical insert the unit retained by the StreamingNode and each
scanner. Chunking temporarily retains about N bytes of chunks and allocates an
additional contiguous N-byte reassembly buffer per scanner. The Segment seal
threshold is not a hard message-size constraint: every growing segment may
accept the indivisible allocation that first takes it over the threshold and
then seal. The upstream bound is therefore a resource guard, not a derivation
from Segment capacity.

`quotaAndLimits.limits.maxInsertSize` changes its default from `-1` (no limit)
to 64 MiB. `insertTask.PreExecute` checks at the end of all Proxy-side insert
transformations. `upsertTask.insertPreExecute` checks after partial-upsert query
results have reconstructed the complete insert row; CAS retries call the same
method after every reconstruction. Both measure `InsertMsg.Size()`, the
protobuf size of the materialized `InsertRequest`. An oversized write returns
the deterministic `InputError` `ErrParameterTooLarge` (code 1102) before
`Execute` can append it to the WAL, although field generation, query work, and
ID allocation have already occurred.

The 64 MiB default deliberately leaves headroom below the shipped 128 MiB
Proxy ingress limit and 256 MiB Proxy-to-StreamingNode limit for RPC wrappers,
message properties, and later growth. It is a conservative operational margin,
not an end-to-end proof of the final transport size.

This is intentionally not the exact final WAL-record size. It excludes later
per-channel/partition message properties, partial-update CAS metadata,
encryption expansion, StreamingNode-generated BM25/MinHash fields, segment
assignment, chunk markers, and broker envelope. WAL-layer chunking remains
responsible for the backend's physical record limit. The Proxy limit applies
to the whole materialized request before channel/partition fanout, and `-1`
still disables it.

`quotaAndLimits.limits.maxDeleteSize` independently defaults to 16 MiB. After
primary keys and timestamps have been materialized and routed,
`repackDeleteMsgByHash` measures the protobuf body of every per-vchannel
`DeleteMsg`; this shared path covers both normal Delete and the tombstone side
of Upsert. The largest body must fit the limit, so multiple vchannels may
collectively exceed 16 MiB while no individual logical WAL message does. The
same deterministic `InputError`/`ErrParameterTooLarge` behavior applies, and
`-1` disables the limit. Like `maxInsertSize`, it measures plaintext application
data rather than streaming properties, encryption expansion, chunk metadata,
or the broker envelope.

## 3. Concurrency & ordering

- **The timetick watermark is enforced by the ack machinery, not by write
  adjacency.** A TimeTick(ts=T) record asserts that every record carrying
  ts ≤ T is already durable; reorder-buffer release, checkpoint advance,
  txn commit visibility, and crash recovery all consume that assertion.
  The interceptor acknowledges a message only when its append has fully
  returned — for a chunked insert, when the WHOLE pack is persisted — and
  the sync operator publishes `ts = lastAllAcknowledgedTimestamp()`, the
  consecutive acknowledged prefix (its own comment: "some message sent
  operation is blocked, new TT cannot be pushed forward"). So TimeTick(T)
  can never enter the log while any ts ≤ T message is still mid-pack,
  regardless of how records interleave. No write-side lock is needed to
  protect it; this is why the design needs none.
- Consequently packs interleave freely with other traffic in the log, and
  the consumer pairs chunks by time tick + index/total markers instead of
  by position (§2.3). Within one run the chunks are still sent
  sequentially by a single goroutine, so backend per-producer FIFO keeps
  their relative order — though even that is only an optimization, not a
  correctness requirement of the assembler.
- Chunk appends complete before the logical ID is returned upward, so the
  timetick interceptor's `LastConfirmedMessageID` never advances past
  un-persisted chunks — it under-reports at worst, by the tail chunks of
  the last run, until the next timetick confirms them.
- The assembler is scanner-local and single-goroutine (upstream delivery is
  serialized per scanner); its keyed state machine needs no locking.

## 4. Activation switches

Chunk records are readable only by StreamingNodes carrying this change.
Instead of deriving that fact from the cumulative StreamingVersion ladder, two
explicit switches independently control the producing roles:

| `proxy.splitChunk` | `streaming.splitChunkSN` | Write behavior | Rollout state |
|---|---|---|---|
| `true` | `false` | Proxy row-packs; SN writes each packed message as one WAL record | Safe initial/default state |
| `true` | `true` | Proxy still row-packs; SN additionally enforces the physical WAL record limit | Safe bridge state |
| `false` | `true` | Proxy sends logical messages; SN creates bounded physical WAL records | Safe target state |
| `false` | `false` | Neither role splits an oversized logical message | **Unsafe:** a backend-size rejection can retry forever |

The reader always runs `ChunkAssembler`, independently of both switches.
Turning `streaming.splitChunkSN` off stops creation of new chunks but must not
make historical chunk records unreadable. Both parameters support live refresh.
A config update may therefore change subsequent writes without restarting the
role, which makes ordering mandatory: first set `streaming.splitChunkSN=true`
and confirm that every possible pchannel owner has observed it; only then set
`proxy.splitChunk=false`. Propagation may temporarily mix old and new values
within one role, but each intermediate state is safe in that order.

This is an operational compatibility boundary, not an automatic capability
barrier. Before any Proxy observes `proxy.splitChunk=false`, every StreamingNode
that can own a pchannel must be upgraded and must have observed
`streaming.splitChunkSN=true`. The cumulative global streaming version and
StreamingCoord assignment metadata are unchanged.

## 5. Alternatives rejected

- **Permanently keeping proxy-side size splitting (status quo ante).** Splits are visible
  above the WAL: multiple messages per user insert complicate txn/timetick
  semantics, replication, and every interceptor — and the packing itself was
  the cost center (entity-size estimates that still missed the envelope,
  re-marshaling per split, a single-row rejection wall). This PR removes the
  need for those splits after activation, but keeps the old path behind
  `proxy.splitChunk=true` as the rolling-upgrade compatibility mode.
- **Splitting inside the SN interceptor chain.** Each interceptor would
  observe partial messages, and the split would have to be undone before the
  chain's bookkeeping (timetick, txn state machine) anyway.
- **A synthetic transaction wrapping the chunks.** A ghost begin/commit pair
  around every chunked insert adds txn-state machinery and extra records to
  solve a pure transport problem.
- **Marker records (begin/end) instead of property markers.** Doubles the
  record count for a run; `_ci`/`_ct` on each record carry the same
  information.
- **Raising the backend limits.** Not always available: the Pulsar broker's
  `maxMessageSize` is a broker-side cap an operator may not control.
- **Splitting single-message size at the proxy**: re-splitting reintroduces
  size-driven packing and multiple messages per user write; rejected in favor
  of passing client batches through whole (§2.4). Note this rejects *splitting*,
  not *bounding* — see §2.5 for the bound that is required.
- **Persisting a per-channel or global chunk capability bit.** The bit would have to be
  committed before the first chunk but remain consistent with a failed append,
  ownership changes, and recovery. An explicit deployment switch keeps the
  cumulative StreamingVersion state machine untouched.

## 6. Accepted gaps

| # | Gap | Trigger | Impact | Why accepted |
|---|---|---|---|---|
| 1 | In-place downgrade after activation | A StreamingNode is replaced by a binary without chunk support after `streaming.splitChunkSN=true` has written chunks | That owner cannot reassemble historical chunk records | Turning SN chunking off only stops new chunks; making old WAL history safe for an old binary requires a drain/watermark protocol |
| 2 | Unbounded per-scanner assembly memory | Many chunk runs interleave, a very large logical message reaches the WAL, or many scanners consume one pchannel | Each scanner retains every incomplete run and allocates a contiguous reassembly buffer; memory is multiplied by scanner count | A reader-side hard limit could reject a durable WAL layout that the writer already acknowledged. The 64 MiB Proxy materialized-message default bounds the normal insert path and leaves headroom for later RPC envelopes; disabling it or later StreamingNode expansion accepts this resource risk |

## 7. Rollout & rollback

This is an additive WAL property encoding (`_ci`/`_ct`) with no protobuf schema
change. New StreamingNodes can read old complete records and new chunk records;
old StreamingNodes cannot interpret chunk records. New configs
(`proxy.splitChunk`, `streaming.splitChunkSN`,
`woodpecker.maxMessageSize`, and `pulsar.messageReserveSize`) use safe shipped
defaults. Numeric Pulsar, Kafka, and Woodpecker message-size values below
256 KiB are clamped to 256 KiB; parse errors use the backend default. A reserve
that is too small or does not fit under the active limit falls back rather than
producing a budget with no envelope headroom (§2.1).

The intended live-write rollout order is:

1. Deploy binaries containing chunk read/write support with the shipped
   defaults: `proxy.splitChunk=true` and
   `streaming.splitChunkSN=false`. Proxy continues legacy row packing and no SN
   creates chunk records. This state is safe while binaries roll in any order.
2. Set `streaming.splitChunkSN=true`. Wait until every StreamingNode that can
   own a pchannel has upgraded and observed the new value. Proxy remains on
   `proxy.splitChunk=true`, so traffic remains safe while SN configuration
   propagation is mixed.
3. Set `proxy.splitChunk=false` only after step 2 is confirmed. Mixed
   Proxies are safe in this phase: Proxies still observing `true` keep sending
   packed messages, while those observing `false` send logical messages to SNs
   that all chunk oversized WAL payloads.

The two switches remove any dependency on Deployment restart order for the
initial binary rollout, including Helm's unordered Deployments. They do not
provide an automatic capability barrier: an operator or rollout controller must
confirm step 2 before step 3. Disabling Proxy packing while any possible SN
owner is legacy or still observes `streaming.splitChunkSN=false` is unsupported
because a large logical record can take the unsplit backend path.

To return to the compatibility write mode while retaining the new binaries,
reverse the transition safely: first set `proxy.splitChunk=true` and confirm all
Proxies observe it, then set `streaming.splitChunkSN=false`. This only controls
new writes.
Downgrading or reintroducing a legacy StreamingNode after chunks have been
written remains unsupported because historical WAL may already contain chunks;
that requires a separate drain/watermark protocol.

## 8. Testing

- `pkg/streaming/util/message/chunk_test.go` (runs with `-tags dynamic,test`):
  round-trip split→assemble (properties, payload, first-chunk ID), exact
  boundary fit, empty payload, non-positive chunk size, assembler
  reassemble / pass-through / retain-incomplete-across-interleaving; 1,025
  interleaved live runs are retained without a reader-only rejection bound; a
  duplicate chunk-0 retry uses the later successful observation's ID and
  properties; malformed/corrupt markers fail the scanner explicitly, declared
  totals do not preallocate slots, and TimeTick
  barriers discard only proven-orphan runs.
- `internal/proxy/task_insert_streaming_test.go` (new): `proxy.splitChunk`
  selects Proxy-packed messages while true and one logical message while false;
  row-selection parity, CAS metadata preservation, and empty selection cover
  the SN-owned path.
- `pkg/util/fastpb/insert_request_*_test.go`: differential wire parity against
  the official protobuf encoder covers scalar, document, nested, dense,
  nullable, sparse, and ArrayOfVector selections; descriptor tripwires force
  explicit review when the hand-written codec's protobuf contracts change.
- `pkg/streaming/util/message/{builder,partial_update_cas}_test.go`: direct body
  encoders are consumed synchronously, cannot be combined with `WithBody`, and
  preserve CAS metadata through optional encryption.
- `internal/proxy/task_upsert_test.go`: CAS inserts retain Proxy envelope-aware
  splitting while `proxy.splitChunk=true` and become one logical message while
  false; malformed source columns fail alignment checks before direct encoding.
- `internal/proxy/task_insert_test.go`: the materialized-message bound of §2.5
  runs after insert field processing, and an upsert fixture containing
  query-reconstructed fields is measured through `insertPreExecute`; `-1`
  still disables it.
- `internal/proxy/routing_table_hash_test.go`: the per-vchannel materialized
  Delete bound accepts its exact boundary, rejects the next byte with
  `ErrParameterTooLarge`, and `-1` still disables it.
- `pkg/util/paramtable/service_param_test.go`: known bounded backends clamp
  message-size limits below 256 KiB, malformed values use backend defaults,
  and an invalid reserve still leaves a positive payload budget; defensive
  direct-helper coverage also verifies the per-record envelope minimum (§2.1).
- `internal/streamingnode/server/service/handler/producer/produce_server_test.go`:
  a message carrying the reserved `_ci`/`_ct` markers is rejected at ingress.
- `internal/streamingnode/server/wal/adaptor/{wal_adaptor_trace,wal_adaptor_fence}_test.go`:
  SN chunking creates and reassembles physical records while the switch is
  true; while false, the same oversized logical input follows the legacy single
  record path through the interceptor chain.
- Segment stats tests verify that every growing segment accepts the one
  indivisible allocation that crosses its soft target, rejects later
  allocations, seals an over-target segment immediately after recovery, and
  recovers a dropped capacity notification through the periodic scan.
- `pkg/util/paramtable/component_param_test.go` verifies that
  `proxy.splitChunk` defaults to true,
  `streaming.splitChunkSN` defaults to false, and both values can be refreshed.
