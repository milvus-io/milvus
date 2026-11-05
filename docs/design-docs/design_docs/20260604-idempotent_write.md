# Idempotent Write

- Feature DRI: @tinswzy
- Primary Approver: @chyezh
- Independent Approver: @czs007
- Design Review: 2026-06-04

- **Created:** 2026-06-04
- **Status:** Under review
- **Component:** Proxy | StreamingNode | Metastore | Storage | Client
- **Related Issues:** milvus-io/milvus#50007
- **Released:** TBD

## Summary

A client that loses the response to an `Insert` has no safe recovery: retrying may
double-write the rows, not retrying may lose them. Idempotent write makes an insert
retry a no-op on the server and return the original result.

The mechanism has two halves. An **idempotency key** rides on the write, and a
per-vchannel **dedup window** in the streaming node answers a repeated key from the
first attempt's result instead of appending again. Behind the window, a **summary
store** durably records what the pchannel wrote, so the window can be rebuilt after a
restart or a WAL failover rather than only covering in-process retries.

This document owns the client contract and dedup-window behavior.
[WALSummary Design](wal/summary.md) owns the shared storage format, persistence,
recovery, retention and GC protocols. RecoveryStorage restores interceptor
windows at the startup barrier using a bounded newest-history read; see
[WALSummary integration](wal/summary.md#7-implementation-and-integration-status).

Idempotency is always available for `Insert`: a non-empty client-supplied key
opts that request into deduplication. A request without a key is an ordinary
write; the proxy never generates a key from its payload. There is no global or
collection-level enable switch. WALSummary is always on, independently of
whether any request carries a key.

## Motivation

### The gap

The write path between a client and the WAL has several points where a request can
succeed while its response is lost: client timeout, proxy crash after append,
streaming node failover, network partition. The client sees an error and cannot tell
"not written" from "written, response lost".

Both recoveries are wrong:

- **Retry** — if the first attempt landed, the rows are written twice. With autoID
  the duplicates are not even detectable by primary key, because the retry allocates
  fresh IDs.
- **Do not retry** — if the first attempt did not land, the rows are silently lost.

Every production ingestion pipeline has to solve this above Milvus, usually with its
own dedup table keyed by a business ID. That work is repeated by every user, and it
cannot be done correctly for autoID collections at all.

### Why the existing mechanisms do not cover it

- **Primary key uniqueness** is not dedup: Milvus insert semantics allow duplicate
  primary keys, and autoID assigns a new key per attempt.
- **The WAL's own delivery guarantees** cover the streaming node's internal replay,
  not a client-initiated retry that produces a *new* message.
- **Upsert** is not a substitute: it requires a client-owned primary key, and it
  changes the write semantics (delete + insert) and cost.

### Goals

- An `Insert` retried with the same idempotency key is applied at most once.
- A duplicate retry returns the original attempt's primary keys, so the client's view
  of assigned IDs is stable across retries.
- Retained records survive streaming node restart and WAL failover within the
  configured PChannel TTL and size budget. Process-local access refreshes are not
  a recovery guarantee.
- No data loss in the summary store under any crash point.
- Keyless inserts bypass request-level deduplication and create no idempotency records.

### Non-goals

- **Encrypted collections.** An insert into a collection with an encryption zone is
  refused when a non-empty idempotency key is supplied. The duplicate
  answer is the first attempt's primary keys and it rides in the message HEADER, which the
  builder serializes into a plaintext property — the cipher covers the body only — and the
  client key sits beside it in another plaintext property. The summary store then writes
  both again to object storage. Supporting this means giving the header, the key property
  and the durable record a cipher-protected representation of their own; until then the
  combination is rejected rather than quietly weakened.
- `Delete` and `Upsert`. Only `Insert` reads the key: the proxy takes it from the
  incoming metadata in `Proxy.Insert` and nowhere else, so a key sent on either of the
  others is **ignored, not refused**. The Go client refuses one on `Upsert` before it is
  sent (`client/milvusclient`), but REST, pymilvus and raw gRPC callers reach the server,
  where it is dropped silently. Deleting the same rows twice is already effectively a
  no-op; `Upsert` needs its delete leg deduped as well, which is a separate design.
- Cross-cluster dedup. Replicated writes bypass the window entirely (see
  [Replication and CDC](#replication-and-cdc)).
- Unbounded retention. Duplicate visibility is a bounded window (see
  [Retention](#retention)).

## Public Interfaces

### Request opt-in

A non-empty `idempotency-key` on an Insert is the only opt-in. No collection
property or deployment switch is required. An absent or empty key means every
request is a new write, even when its payload is identical to an earlier one.
Legacy global or collection enable settings have no effect.

### Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `streaming.idempotency.ttlPerPChannel` | `1h` | Idle TTL for committed records in a PChannel. Duplicate access refreshes it in memory; recovery uses original commit time. |
| `streaming.idempotency.maxSizePerPChannel` | `256MB` | Estimated record bytes shared across all VChannels in one PChannel, evicted by last access. Recovery plans newest chunks by encoded Insert + Idempotency section bytes, including one boundary chunk. |
| `streaming.idempotency.bestEffortMinCountPerVChannel` | `16` | Soft per-VChannel count preference under capacity pressure; TTL and PChannel capacity override it. Recovery reserves no minimum. |
| `streaming.summary.maxBytesPerPChannel` | `4GB` | Shared WALSummary retained-object byte budget per pchannel. GC-eligible chunks remain retained until this budget is exceeded; `0` disables the byte bound. |
| `streaming.idempotency.maxKeyLength` | `256` | Maximum accepted explicit key length in bytes. |

Idempotency retention is captured when a WAL opens; these three settings are
not dynamically refreshed. Nonpositive TTL or maxSize uses the default;
minCount zero disables the soft preference. The former
`streaming.idempotency.maxBytesPerWindow` remains registered but no longer
controls retention. No alias is provided for the unreleased unscoped names.

The shared retained-object budget remains independent of the idempotency cache;
see [Retention](#retention) and
[WALSummary configuration](wal/summary.md#34-configuration-and-checkpoint-integration).

### Client API

```go
client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("coll").
    WithIdempotencyKey("order-4711"))
```

Also available on the row-based option. The key travels in the gRPC metadata header
`idempotency-key`.

Supplying a key to `Upsert` returns an error before the RPC is issued. The client
short-circuits that error rather than routing it into the schema-mismatch retry,
because no schema refresh can fix a caller mistake.

**Contract:** an explicit key must not be reused for a different payload. Reuse within
the retention window returns the first payload's result and does not write the new
rows.

If no explicit key is supplied, the request is not deduplicated. A client must
reuse its explicit key for retries and choose a new key for each new logical
write, including intentional writes with identical payloads.

### Wire protocol

- Message property `_ik` carries the key. A property rather than a header field, so
  one accessor serves every message type and both the mutable (interceptor) and
  immutable (recovery) sides. An empty key materializes no property at all.
- `InsertMessageHeader.idempotent_result` carries `{row_offsets, ids}` — the primary
  keys this write unit produced and where each row came from in the original request.
  See [Why the result is on the wire](#why-the-result-is-on-the-wire).

### Metrics

| Metric | Meaning |
| --- | --- |
| `idempotency_window_entries` | Retained entries per vchannel window. |
| `idempotency_window_inflight` | Keys currently being appended. |
| `idempotency_duplicate_total` | Duplicate hits served. |
| `idempotency_eviction_total` | Entries evicted from a window. |
| `idempotency_reader_physical_dedup_drop_total` | Scanner-side physical duplicate drops (see [Reader-side physical dedup](#reader-side-physical-dedup)). |

### Storage layout

The durable history is held by [WALSummary](wal/summary.md), shared with other
consumers. See its [object layout](wal/summary.md#22-objects-object-storage),
[section format](wal/summary.md#25-chunk-format-and-read-validation), and
[idempotency reader contract](wal/summary.md#51-idempotency).

## Design Details

### Architecture

```
client ── idempotency-key header ──► proxy
                                       │  validate explicit key, stamp `_ik`
                                       │  stamp per-write-unit insert result
                                       ▼
                            fan out by vchannel (+ split by size)
                                       │
                                       ▼
                          streamingnode: idempotency interceptor
                             ├── key seen?  ──► answer from window, do NOT append
                             └── new key    ──► append, record result in window
                                       │
                                       ▼
                                      WAL
                                       │
                                       ▼
                              summary store (durable)
                              manifest ── chunk objects
                                       │
                          restart ─────┴──► rebuild windows at WAL open
```

Two layers, deliberately separated:

- **The summary store** is a record of what a pchannel durably wrote. It stores
  committed write facts and nothing about why anyone wants them.
- **The dedup window** builds meaning on top. It decides what to keep and for how
  long; the store neither knows nor records that decision.

### Key identity

The explicit client key is used as-is after length validation. The dedup window
is scoped by vchannel, which identifies a collection shard. Clients must not
reuse a key for different logical writes within that scope, including writes
to different partitions or namespaces. The proxy does not hash the payload or
destination to invent a request identity.

### autoID and stable shard routing

Rows are routed to shards by `hash(primaryKey) % numChannels`. With autoID the proxy
allocates the keys, so a naive retry allocates different keys, routes rows to different
shards, and the per-shard dedup no longer lines up with the first attempt.

`reassignAutoIDForStableIdempotency` fixes the routing rather than the keys: it
allocates candidate IDs in rounds and keeps only those that hash into the bucket
matching the row's own offset (`offset % numChannels`), so row *i* always lands on the
same shard on every attempt.

**Accepted cost:** a candidate that hashes into an already-satisfied bucket is
discarded, and each top-up round deliberately over-allocates (`missing * numChannels`)
so that one round almost always suffices. ID amplification therefore grows with shard
count and shrinks with batch size: measured at ~1.01x for 100k rows over 4 shards,
~1.25x for 10k over 16, and ~21x for a 100-row insert over 64 shards, where the
over-allocation dominates — the last case is still only about 2k IDs in absolute terms,
and the ID space is `int64`. The loop is bounded at 256 rounds so a pathological hash
distribution fails loudly instead of burning the ID space forever; the common case is
one round. The cost applies only to autoID inserts carrying an explicit idempotency key.

The alternatives do not work: deriving the shard from the row offset directly breaks
`Delete`/`Upsert`, whose index-based routing hashes the primary key against the same
channel list, so the insert's row→shard assignment MUST equal `hash(assignedPK) % n`;
and deterministic PRNG-generated IDs cannot guarantee global uniqueness.

The channel list must not be permuted while doing this — `HashPK2Channels` is
index-based, and `Delete` hashes against the same unpermuted list.

### The dedup window

One window per vchannel, keyed by idempotency key. `Begin(key)` returns one of:

- **Owner** — first sighting. The append proceeds; on success the result is recorded.
- **Wait** — another request owns the key and is still appending. The waiter blocks on
  the owner's outcome, so concurrent duplicates converge on one append.
- **Duplicate** — the key has a completed entry. The stored result is returned and
  **no append happens**.

### Retention

**Committed records in memory, per PChannel.** All VChannel windows share one
estimated byte counter and last-access order. A completed append starts its idle
TTL. A duplicate access refreshes the TTL and moves the record to the newest
position. Expired records cannot answer duplicates and are removed in bounded
cleanup batches even when the channel is idle. Pending owners and waiters retain
their existing lifecycle and do not count as committed cache entries.

When `maxSizePerPChannel` is exceeded, capacity eviction first selects the oldest
record among VChannels with more than `bestEffortMinCountPerVChannel` records. If none is
above that count, it evicts the globally oldest record. TTL always takes
precedence. The count is a soft preference, with no guaranteed retention period
or per-VChannel memory reservation. Whole records are evicted; an indivisible
record larger than the PChannel budget cannot remain cached. The byte estimate
counts retained request/result data, not all Go container overhead or process RSS.

**Recovery, per PChannel.** Before accepting appends, RecoveryStorage captures an
atomic pending/sealed/durable Summary view. Before object I/O it selects a newest
chunk suffix using manifest TimeTicks and the sum of the eligible VChannels'
Insert and Idempotency section lengths. Pending records use their encoded fact
size; sealed sections use equivalent protobuf lengths. These bytes are charged
before key deduplication. One whole boundary chunk is included, so selection
can exceed the byte target by that boundary block. There is no per-VChannel
count reservation or decode-dependent extension into older chunks.

Selected durable chunks are scheduled oldest first in batches of at most four
concurrent reads through the shared cache. After each batch joins, acquired
payloads are re-admitted/promoted oldest to newest, so out-of-order downloads
cannot leave old chunks favored over new ones. The encoded bytes remain
available for TransformLog reads under the same cache budget. Sections are
processed individually, without a whole-chunk candidate array and sort. Ordered chunk boundaries and
each VChannel's ordered section survive into the startup snapshot. A newer key
version removes the old slot and appends at its chronological position; stable
compaction discards obsolete slots, without sorting. The compatibility map and
ordered view share the same immutable Record/result data.

The interceptor merges sections within each chunk directly into its PChannel
window, with O(VChannel-count-in-chunk) cursors. The common single-section case
needs no heap maintenance. There is no per-VChannel recovery sort or subsequent
full-record aggregation/global sort on WAL startup. Generic unordered Snapshot
callers retain the sorting fallback. This changes neither original-age TTL nor
capacity/soft-floor eviction decisions. Required read/decode errors
cancel and join the batch and fail recovery without a partial window.

Original commit age is used for TTL filtering and window reconstruction; access
refreshes are never persisted. The live window still applies its estimated
record-byte cap before accepting appends. Whole-object transfer, decoding and
bounded concurrent payloads have additional costs: the selection target is not
a download or RSS limit. A frequently retried old key may disappear on restart
even if it was still cached in the former process.

**Store objects, per PChannel.** Durable shared history follows
[WALSummary retention](wal/summary.md#4-retention-gc), independently of the cache
TTL and byte budget. Idempotency accepts expiry under that shared budget;
materialization consumers retain their own durable GC frontiers. This change
neither filters other consumers' records nor changes checkpoint/GC safety. The
cache can retain less history than the shared store, and recovery can return
less history than the former process's cache. Neither TTL nor minCount promises
a minimum duplicate visibility duration under capacity pressure.

### DDL that empties a collection

An idempotency record describes an executed request, not whether its rows still
exist. `DropCollection`, `TruncateCollection` and `DropPartition` do not clear the
interceptor's window or filter retained WALSummary records. Within the retained
window and the same vchannel scope, a delayed retry returns the original result
without executing the insert again, even if another operation removed its data.
Otherwise, a timeout followed by truncate and a late retry would reinsert data
that the truncate had removed.

A new logical write needs a new client key, or no key when retry deduplication
is not requested. DDL does not discard request history. Drop and recreation
under a new collection ID creates new vchannels and hence a different deduplication scope. Normal
resource validation still applies; this does not promise that a request against
a removed collection bypasses validation or always returns a cached response.

History is bounded by the existing retention policy. Closing an interceptor
releases its in-memory state; summary retention GC releases durable chunks.
The summary contains no DDL invalidation markers.

This follows the request-identity contract described by
[Stripe's idempotent requests](https://docs.stripe.com/api/idempotent_requests)
and the deleted-resource, late-retry example in
[AWS Builders' Library](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/).

### Transactions

One insert fans out to one message per vchannel, and a vchannel's rows are further
split into several messages when they exceed `pulsar.maxMessageSize`. The producer
groups all messages of one vchannel into a **transaction** whenever there is more than
one, and stamps the idempotency key on the synthesized `CommitTxn` message only.

The interceptor therefore never dedups a txn body — bodies are appended normally and
their insert results are buffered per `(vchannel, txnID)`. Dedup happens once, on the
commit. A duplicate commit is short-circuited, the transaction is never committed, and
the scanner discards the uncommitted bodies. A partial write unit can never land.

Three consequences worth stating:

- A duplicate commit **synthesizes a rollback** for the retried transaction, whose
  `BeginTxn` and bodies were already appended under a new txnID. Without it the session
  lingers until keepalive expiry, stalling checkpoint advancement and accumulating WAL
  garbage per retry. The rollback is only synthesized for a transaction positively
  known to be still open.
- If the txn buffer expired before the commit arrives, completing with a nil result
  would permanently store an entry whose duplicates return the retry's own unpersisted
  IDs. The commit fails with `TransactionExpired` instead — deliberately an
  *unrecoverable* code, so the resumable producer rebuilds the whole transaction
  (re-appending the bodies repopulates the buffer) rather than hot-retrying a commit
  that can never succeed.
- The buffer is reclaimed only when the **owner** resolved the entry. A waiter that
  exited on its own context must not reclaim it: the owner may still sit between
  `Begin` and `Build`, and dropping the buffer would leave a committed entry with no
  result.

### Why the result is on the wire

`InsertMessageHeader.idempotent_result` carries `{row_offsets, ids}` per write unit.
Both halves are needed and neither is derivable at the streaming node:

- `ids` — for autoID collections the primary keys are server-allocated and a retry
  allocates different ones, so the duplicate answer must carry the originals. They do
  exist in the message body, but the streaming node never decodes an insert body on the
  append path (segment assignment and size estimation all read the header), and
  decoding would materialize every column including vectors to extract an 8-byte key
  per row, on the write hot path, without even having the collection schema to locate
  the primary column.
- `row_offsets` — the mapping back to the original request's row order exists only in
  the proxy; it is not in the body at all. It could in principle be recomputed on retry
  since routing is deterministic, but the size-driven message split boundaries would
  also have to match between attempts. A `maxMessageSize` change or a schema change
  moves them, and a recomputed mapping would then scatter primary keys to the wrong
  rows silently.

The `ids` payload is redundant for client-supplied primary keys, since the retry
already has them. The first version stamps it unconditionally to keep the write path
uniform; making the stamp conditional on autoID is tracked as follow-up work.

The interceptor enforces the pairing invariant: an insert carrying a result but no key
is rejected, because it would be appended outside the window and its result could never
be served.

## The summary store

[WALSummary Design](wal/summary.md) is the authoritative storage design. In
particular, it defines [manifest coverage](wal/summary.md#24-manifest-and-sections),
[chunk encoding and validation](wal/summary.md#25-chunk-format-and-read-validation),
and the [paired sections consumed by idempotency](wal/summary.md#51-idempotency).

## Normal operation

### Write path

```text
append(msg with `_ik`)
   -> window.Begin(key)
        Duplicate -> return stored result, no WAL append
        Wait      -> wait for the owner's outcome
        Owner     -> append to WAL, then window.Complete(key, result)
```

The append interceptor updates its in-memory window. RecoveryStorage's ordered
WAL consumer separately calls `WALSummary.ObserveMessage`;
summary staging is not a synchronous step of the interceptor's append path.
See [WALSummary lifecycle](wal/summary.md#3-lifecycle-and-persistence).

### Persist path

Use the [WALSummary persistence and checkpoint contract](wal/summary.md#34-configuration-and-checkpoint-integration).
The summary owns asynchronous persistence; the recovery integration must honor
its confirmation frontier before allowing WAL truncation.

## Startup and recovery

The idempotency consumer rebuilds each window from its retained summary
sections and WAL replay, preserving the original result and dedup scope.
The shared recovery algorithm, failure handling and physical replay position
are defined in [WALSummary recovery](wal/summary.md#6-recovery-and-term-takeover).
See [integration status](wal/summary.md#7-implementation-and-integration-status)
for what is wired on this branch.

## Chunk GC

See [WALSummary retention GC](wal/summary.md#4-retention-gc) and the separate
[cross-owner GC TODO](wal/summary.md#9-gc-design-cross-owner-coordination-todo).
The idempotency consumer accepts bounded history expiry. Keyless traffic does
not clear retained history or authorize deleting data needed by other summary consumers.

## Split-brain fencing

See [WALSummary term arbitration](wal/summary.md#23-term-arbitration) and
[checkpoint ownership](wal/summary.md#62-checkpoint-ownership-and-truncation).
These guarantees belong to the shared recovery integration, not the interceptor.

## Interaction with WAL truncation

See the [confirmation and truncation contract](wal/summary.md#62-checkpoint-ownership-and-truncation).
A checkpoint must not discard WAL containing request history that cannot yet
be recovered from WALSummary.

## Replication and CDC

Replicated messages **bypass the window entirely**. The replicate stream has its own
exactly-once delivery via source-timetick checkpoints, and the idempotency key inside a
replicated message belongs to the *source* cluster's window history. Deduplicating
against the local window would silently drop replicated writes whenever the key happens
to sit in this cluster's window — after a demotion, or after the source released the key
and a client legally re-issued it.

The recovery observer applies the same rule: a replicated write becomes a *keyless*
committed write in Summary, so a foreign key can never materialize a local dedup
entry. Its local WAL positions still participate in Summary persistence and
checkpoint gating.

## Reader-side physical dedup

Switching between the write-ahead buffer stream and the WAL scanner stream can deliver
the same logical message twice with *different* message IDs, which the existing
message-ID dedup cannot catch. The reorder buffer additionally drops a non-TimeTick
message whose timetick was already seen.

**Invariant:** the timetick interceptor assigns a unique timetick to every appended
message, so two genuinely distinct non-TimeTick messages never share a timetick while
both are retained. A repeated timetick can therefore only be a physical replay. If a
future code path ever lets two genuinely distinct messages reach this buffer with the
same timetick, the second is silently dropped — **this invariant must be preserved.**
Drops are surfaced by a warn log and `idempotency_reader_physical_dedup_drop_total`.

Physical deduplication is always active and does not depend on a request key.
Legacy VersionOld messages are exempt from TimeTick deduplication because
several messages split from one old insert can share a TimeTick.

## Design Decisions

Trade-offs that were argued and settled, with what was rejected and why.

### PChannel TTL and budget with soft VChannel protection

A global byte budget bounds retained result data without multiplying a per-window
budget by the number of VChannels. Idle TTL reclaims inactive history. Last-access
ordering lets active retries extend their in-memory visibility; those refreshes
are deliberately not a durability promise. A soft record count gives quiet
VChannels some preference without adding per-VChannel recovery scans or strong
reservations. Recovery instead reads only the newest eligible suffix; see
[Retention](#retention).

The shared object budget and consumer-frontier decisions remain documented in
[WALSummary retention](wal/summary.md#4-retention-gc).

### DDL preserves request history

**Chosen:** retain executed requests across DDL until ordinary retention removes
them. A retry must not repeat its side effects after a different request deletes
the data. New intent is expressed with a new key.

**Rejected — clearing the window or filtering summary records at the DDL timetick.**
Both turn a delayed retry into a new insert. Clearing only the in-memory window
also makes behavior differ before and after recovery. Request identity is
explicitly supplied by the client and is not changed when data is deleted.

### Shared storage decisions

Manifest content, section separation, immutable chunk identity, asynchronous
publication and takeover ordering are defined in
[WALSummary Design](wal/summary.md). They are shared by all summary consumers
and are not separately specified by the idempotency feature.

## Compatibility, Deprecation, and Migration Plan

**Compatibility.** Keyless inserts remain ordinary writes and carry no `_ik`
property or idempotent result. Inserts with a non-empty explicit key are
idempotent without configuring a global or collection switch. Requests written
without a key do not acquire an idempotency identity retroactively.

`InsertMessageHeader.idempotent_result` remains an optional field; older readers
ignore it. Reader-side physical deduplication and WALSummary are always active.
RecoveryStorage reads Summary for transform recovery and bounded idempotency
restoration, and runs shared retention GC. Existing Summary data remains readable;
no access timestamp or per-key eviction cursor is added to the durable format.

**No data migration.** The feature is unreleased; there is no earlier on-disk format.

## Test Coverage

**Unit** — proxy explicit-key opt-in, keyless pass-through, ignored legacy
switches, key length validation, autoID routing stability and result merging; window
owner/wait/duplicate decisions, cross-VChannel LRU eviction, idle TTL cleanup,
soft-count preference, original-age restoration, restore-from-snapshot,
transaction commit dedup with rollback synthesis, expired transaction buffers,
replicated bypass, and DDL preservation of request history.

Shared codec, persistence, recovery and GC tests are described in
[WALSummary validation](wal/summary.md#10-validation-and-source-map).

**StreamingNode integration** — `TestWALIdempotencyAppend` in
`wal_idempotency_test.go` checks duplicate responses within one open WAL,
including the original message ID, TimeTick and last-confirmed position.
`TestRecoveryStartsWALSummary` reopens the WAL and verifies the
original append result. Bounded recovery tests assert actual chunk-read counts,
TTL boundaries, duplicate-key selection, keyless skipping, cancellation, and
snapshot consistency across concurrent seal/upload/append transitions.

A local SDK → proxy → full interceptor-chain experiment with a one-minute TTL
also checks duplicate autoIDs before and after restarting the same persistent
namespace: local accesses keep an old entry alive, a recent entry restores its
original result, and the old entry's access refresh does not survive restart.

**Known gaps:** an automated SDK restart harness, failover during an in-flight
append, and long-running memory tests under skewed shard load.

## Future Work

- **Stamp `idempotent_result` only for autoID collections.** For client-supplied primary
  keys the retry already has them.
- **`Upsert` support**, which requires deduping the delete leg as well.
- **Lazy window restoration.** Use the existing
  [section indexes](wal/summary.md#25-chunk-format-and-read-validation) to load
  keys and offsets first and fetch primary keys only when a duplicate is served.
  This requires a consumer read path that does not eagerly rebuild full results.

## Known Limitations

- **Ambiguous append errors.** Releasing the key on append failure assumes an error means
  nothing was written, but some WAL implementations may land the write despite returning
  an error (the pulsar walimpls documents exactly this). In that window a same-key retry
  re-owns the key and appends again, producing duplicate rows — the same outcome a retry
  without idempotency would produce. WAL open reconstructs retained landed keys
  from Summary and replay within the configured TTL and budget. Closing the
  live-process gap requires reconciliation against the recovery-side observer.
- **Retention is bounded.** Capacity pressure can shorten the effective retry
  horizon; process-local access refreshes are not restored after failover.
- **The byte budget is not RSS.** Expiry removes cached records, while window
  metadata, in-flight requests, Summary objects and decoding have separate costs.
- **Shared-store limitations.** Assignment identity, checkpoint fencing and
  unresolved cross-owner GC constraints are described in
  [WALSummary](wal/summary.md#63-storage-lifetime-and-failure-boundaries).
- **Partial fan-out retries.** A retry after an attempt that reached only some shards is
  deduplicated on the landed shards and appended fresh on the missing ones — the intended
  outcome. The proxy cannot distinguish it from the pathological case where one shard's
  window released a key its siblings still hold, so the mix is logged rather than
  rejected: failing would break the legitimate case.

## References

- Implementation: milvus-io/milvus#50007
- Shared storage design: [WALSummary](wal/summary.md)
- Streaming system guide: `docs/agent_guides/streaming-system/streaming-system.md`
