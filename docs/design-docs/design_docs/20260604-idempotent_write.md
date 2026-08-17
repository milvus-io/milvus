# Idempotent Write

- Feature DRI: @tinswzy
- Primary Approver: @chyezh
- Independent Approver: @czs007
- Design Review: 2026-06-04

- **Created:** 2026-06-04
- **Status:** Reviewed; implemented in milvus-io/milvus#50007
- **Component:** Proxy | StreamingNode | Metastore | Storage | Client
- **Related Issues:** milvus-io/milvus#50007
- **Released:** TBD

This document records the design as reviewed and built. It is the design of record
for idempotent write, not a proposal: the design review listed above has been held
and the implementation follows what is written here. Where the review settled a
trade-off, the decision and its rejected alternatives are recorded in
[Design Decisions](#design-decisions).

## Summary

A client that loses the response to an `Insert` has no safe recovery: retrying may
double-write the rows, not retrying may lose them. Idempotent write makes an insert
retry a no-op on the server and return the original result.

The mechanism is an **idempotency key** carried on the write, and a per-vchannel
**dedup window** in the streaming node that answers a repeated key from the first
attempt's result instead of appending again. The window is rebuilt after a restart
from a durable **summary store**, so the guarantee survives streaming node failover
rather than only covering in-process retries.

The feature is off by default and is enabled per collection on top of a global
switch. It applies to `Insert` only.

## Motivation

### The gap

The write path between a client and the WAL has several points where a request can
succeed while its response is lost: client timeout, proxy crash after append,
streaming node failover, network partition. The client sees an error and cannot
tell "not written" from "written, response lost".

Both recoveries are wrong:

- **Retry** — if the first attempt landed, the rows are written twice. With autoID
  the duplicates are not even detectable by primary key, because the retry allocates
  fresh IDs.
- **Do not retry** — if the first attempt did not land, the rows are silently lost.

Every production ingestion pipeline has to solve this above Milvus, usually by
maintaining its own dedup table keyed by a business ID. That work is repeated by
every user, and it cannot be done correctly for autoID collections at all.

### Why the existing mechanisms do not cover it

- **Primary key uniqueness** is not dedup: Milvus insert semantics allow duplicate
  primary keys, and autoID assigns a new key per attempt.
- **The WAL's own delivery guarantees** cover the streaming node's internal replay,
  not a client-initiated retry that produces a *new* message.
- **Upsert** is not a substitute: it requires a client-owned primary key, and it
  changes the write semantics (delete + insert) and cost.

### Goals

- An `Insert` retried with the same idempotency key is applied at most once.
- A duplicate retry returns the original attempt's primary keys, so the client's
  view of assigned IDs is stable across retries.
- The guarantee survives streaming node restart and WAL failover, bounded by an
  explicit retention window.
- Zero cost when the feature is off.

### Non-goals

- `Delete` and `Upsert`. Both are rejected with an explicit error when an
  idempotency key is supplied. Deleting the same rows twice is already effectively a
  no-op; `Upsert` needs its delete leg deduped as well, which is a separate design.
- Cross-cluster dedup. Replicated writes bypass the window entirely (see
  [Replication](#replication-and-cdc)).
- Unbounded retention. Duplicate visibility is a bounded window, not forever.

## Public Interfaces

### Collection property

```
collection.insert.idempotency.enabled = "true" | "false"
```

Set at `CreateCollection` / `AlterCollection`. An unparseable value is rejected at
DDL time rather than silently downgrading a durability guarantee the operator
believes is on.

Both this property **and** the global `streaming.idempotency.enabled` must be true
for a collection's inserts to be idempotent.

### Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `streaming.idempotency.enabled` | `false` | Global kill switch. |
| `streaming.idempotency.windowTTL` | `10m` | Retention target for completed entries. Duplicate visibility does not extend past this TTL. |
| `streaming.idempotency.minEntriesPerWindow` | `1000` | Entry floor per vchannel after TTL eviction. Does **not** extend visibility past TTL. |
| `streaming.idempotency.maxBytesPerWindow` | `16MiB` | Hard byte cap per vchannel; overrides the floor and may evict before TTL. `0` disables. |
| `streaming.idempotency.snapshotInterval` | `10s` | Interval for persisting summary checkpoints. Independent of `walRecovery.persistInterval`. |
| `streaming.idempotency.maxKeyLength` | `1024` | Maximum accepted explicit key length in bytes. |

The byte cap exists because an entry-count floor cannot bound memory: each retained
entry carries the per-row primary keys of its insert, so one entry can be arbitrarily
large.

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

**Contract:** an explicit key must not be reused for a different payload. Reuse
within the retention window returns the first payload's result and does not write
the new rows.

If no explicit key is supplied, the proxy derives one from the request
(see [Key derivation](#key-derivation)), so an unmodified client still gets
idempotency for a byte-identical retry.

### Wire protocol

- Message property `_ik` carries the key. A property rather than a header field, so
  one accessor serves every message type and both the mutable (interceptor) and
  immutable (recovery) sides. An empty key materializes no property at all.
- `InsertMessageHeader.idempotent_result` carries `{row_offsets, ids}` — the primary
  keys this write unit produced and where each row came from in the original
  request. See [Why the result is on the wire](#why-the-result-is-on-the-wire).

### Metrics

| Metric | Meaning |
| --- | --- |
| `idempotency_window_entries` | Retained entries per vchannel window. |
| `idempotency_window_inflight` | Keys currently being appended. |
| `idempotency_duplicate_total` | Duplicate hits served. |
| `idempotency_eviction_total` | Entries evicted from a window. |
| `idempotency_snapshot_total` | Summary persist cycles. |
| `idempotency_snapshot_checkpoint_lag_seconds` | How far the durable summary checkpoint trails the consume checkpoint. Drives the WAL truncation clamp — the primary alert. |
| `idempotency_reader_physical_dedup_drop_total` | Scanner-side physical duplicate drops (see [Reader-side dedup](#reader-side-physical-dedup)). |

### Storage layout

etcd, under the streaming node catalog root:

```
streamingnode/<pchannel>/summary-store/pchannel-summary-meta
streamingnode/<pchannel>/summary-store/vchannel-summary-meta/<view_type>/<vchannel>
```

Object storage:

```
<chunk-manager-root>/streamingnode/summary-store/<pchannel>/chunks/chunk.<generation>.term<term>.psc
```

## Design Details

### Architecture

```
client ── idempotency-key header ──► proxy
                                       │  derive/validate key, stamp `_ik`
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
                        recovery storage: summary store (durable)
                             etcd meta + object-storage chunks
                                       │
                          restart ─────┴──► rebuild window at WAL open
```

Two layers, deliberately separated:

- **The summary store** is a business-agnostic record of what a pchannel durably
  wrote. It stores committed write facts and nothing about why anyone wants them.
- **A view** builds meaning on top. The idempotency window is today's only view; a
  primary-key index is the second planned one. Every logical metadata record is
  scoped by a `view_type` so views do not collide.

### Key derivation

An explicit client key is used as-is (length-checked). Otherwise the proxy derives
one deterministically:

```
key = SHA256( destination ‖ numRows ‖ canonical(client-supplied columns) )
```

**The destination must be in the key.** The dedup window is keyed by vchannel, and a
vchannel is a *collection shard* — not a partition, not a namespace. Two logically
distinct inserts of the same rows into two partitions of one collection would
otherwise hash to the same key, and the second would be answered as a duplicate with
the first insert's primary keys while its rows never reached the WAL. A derived key
is not under the caller's control, so the "do not reuse a key" contract that covers
an explicit key does not excuse that collision. The destination covers
`(dbName, collectionName, partitionName, namespace)`, every string length-prefixed
so neighbouring fields cannot shift into each other. `namespace` distinguishes unset
from empty: unset routes by primary key, empty routes by namespace.

The destination is hashed **exactly as the client sent it**, before the proxy resolves
an empty partition name to the default. A retry resends the same request, so the key
stays stable; spelling one destination two ways costs a missed dedup, never a merge
of two destinations.

The payload side covers only the client-supplied columns plus `numRows`: derivation
runs before the proxy fills field properties, function output, dynamic and namespace
fields, so by design it must not depend on any of them.

For autoID collections the primary column is excluded from the hash — it is
server-assigned and differs per attempt.

### autoID and stable shard routing

Rows are routed to shards by `hash(primaryKey) % numChannels`. With autoID the proxy
allocates the keys, so a naive retry allocates different keys, routes rows to
different shards, and the per-shard dedup no longer lines up with the first attempt.

`reassignAutoIDForStableIdempotency` fixes the routing rather than the keys: it
allocates candidate IDs in rounds and keeps only those that hash into the bucket
matching the row's own offset (`offset % numChannels`), so row *i* always lands on
the same shard on every attempt.

**Accepted cost:** a candidate that hashes into an already-satisfied bucket is
discarded, and each top-up round deliberately over-allocates (`missing * numChannels`)
so that one round almost always suffices. ID amplification therefore grows with shard
count and shrinks with batch size: measured at ~1.01x for 100k rows over 4 shards,
~1.25x for 10k over 16, and ~21x for a 100-row insert over 64 shards, where the
over-allocation dominates — the last case is still only about 2k IDs in absolute
terms, and the ID space is `int64`. The loop is bounded at 256 rounds so a
pathological hash distribution fails loudly instead of burning the ID space forever;
the common case is one round. The cost applies only to idempotency-enabled autoID
collections.

The alternatives do not work: deriving the shard from the row offset directly breaks
`Delete`/`Upsert`, whose index-based routing hashes the primary key against the same
channel list, so the insert's row→shard assignment MUST equal
`hash(assignedPK) % n`; and deterministic PRNG-generated IDs cannot guarantee global
uniqueness.

The channel list must not be permuted while doing this — `HashPK2Channels` is
index-based, and `Delete` hashes against the same unpermuted list.

### The dedup window

One window per vchannel, keyed by idempotency key. `Begin(key)` returns one of:

- **Owner** — first sighting. The append proceeds; on success the result is recorded.
- **Wait** — another request owns the key and is still appending. The waiter blocks
  on the owner's outcome, so concurrent duplicates converge on one append.
- **Duplicate** — the key has a completed entry. The stored result is returned and
  **no append happens**.

Retention is three bounds, strongest first:

1. `maxBytesPerWindow` — hard cap, may evict before TTL.
2. `windowTTL` — an entry past TTL never answers a duplicate, even while the
   `minEntries` floor still keeps it in memory. Without this rule the floor would
   extend duplicate visibility forever on a quiet shard.
3. `minEntriesPerWindow` — keeps recent entries alive on a low-traffic shard.

Eviction order is by commit timetick, derived from the *message's* timetick rather
than local wall clock, so the live window and the clock-free recovery-side window
retain the same key set under NTP skew.

TTL sweeps are driven by the periodic TimeTick append (rate-limited), so an idle
vchannel releases its retained primary-key memory without waiting for its next write.

### Transactions

One insert fans out to one message per vchannel, and a vchannel's rows are further
split into several messages when they exceed `pulsar.maxMessageSize`. The producer
groups all messages of one vchannel into a **transaction** whenever there is more
than one, and stamps the idempotency key on the synthesized `CommitTxn` message only.

The interceptor therefore never dedups a txn body — bodies are appended normally and
their insert results are buffered per `(vchannel, txnID)`. Dedup happens once, on the
commit. A duplicate commit is short-circuited, the transaction is never committed, and
the scanner discards the uncommitted bodies. A partial write unit can never land.

Three consequences worth stating:

- A duplicate commit **synthesizes a rollback** for the retried transaction, whose
  `BeginTxn` and bodies were already appended under a new txnID. Without it the
  session lingers until keepalive expiry, stalling checkpoint advancement and
  accumulating WAL garbage per retry. The rollback is only synthesized for a
  transaction positively known to be still open.
- If the txn buffer expired before the commit arrives, completing with a nil result
  would permanently store an entry whose duplicates return the retry's own
  unpersisted IDs. The commit fails with `TransactionExpired` instead — deliberately
  an *unrecoverable* code, so the resumable producer rebuilds the whole transaction
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
  exist in the message body, but the streaming node never decodes an insert body on
  the append path (segment assignment and size estimation all read the header), and
  decoding would materialize every column including vectors to extract an 8-byte key
  per row, on the write hot path, without even having the collection schema to locate
  the primary column.
- `row_offsets` — the mapping back to the original request's row order exists only in
  the proxy; it is not in the body at all. It could in principle be recomputed on
  retry since routing is deterministic, but the size-driven message split boundaries
  would also have to match between attempts. A `maxMessageSize` change or a schema
  change moves them, and a recomputed mapping would then scatter primary keys to the
  wrong rows silently.

The `ids` payload is redundant for client-supplied primary keys, since the retry
already has them. The first version stamps it unconditionally to keep the write path
uniform; making the stamp conditional on autoID is tracked as follow-up work.

The interceptor enforces the pairing invariant: an insert carrying a result but no
key is rejected, because it would be appended outside the window and its result could
never be served.

### The summary store

Three layers.

**1. Metadata (etcd).** One `PChannelSummaryMeta` per pchannel — latest generation,
GC boundaries, owner term, chunk manifest. One `VChannelSummaryMeta` per
`(view_type, vchannel)` — that view's checkpoint hint and the oldest generation it
still needs.

**2. Chunk payloads (object storage).** One object per generation, per pchannel:

```
┌────────────────────────────────────────────────┐
│ header 16B: magic "PSCCH001" | version | size  │
├────────────────────────────────────────────────┤
│ vchannel chunk payload (proto)                 │  ← one per vchannel
│ ...                                            │
├────────────────────────────────────────────────┤
│ footer (proto): pchannel, generation, term,    │
│   source checkpoint, object-wide tt span,      │
│   per-vchannel index {offset, length,          │
│   checksum, count, that vchannel's tt span}    │
├────────────────────────────────────────────────┤
│ sha256 over the exact footer bytes             │
│ footer length (4B) | magic "PSCFT001"          │
└────────────────────────────────────────────────┘
```

The format version lives **only** in the fixed binary header, checked before any
proto in the object is trusted; a second copy could only ever agree with the first.
The footer checksum covers the exact stored bytes and is carried in the trailer, so
verification never re-marshals a parsed footer — proto marshaling is not guaranteed
byte-stable across library versions, and re-deriving would flag a healthy chunk as
corrupt the day the encoding shifts.

A physical WAL position is recorded only at pchannel level. Everything below it is
addressed by timetick, and which generation to open comes from
`min_required_generation`.

**3. Materialization (memory).** At WAL open, recovery replays the chunks it still
needs, deduplicates by key, applies retention, and hands each view a
`VChannelSummarySnapshot` — retained entries plus the retention state that decides
which of them may still be served. It is built once, consumed once, never stored;
it is a plain Go struct, not a proto.

### Split-brain fencing

WAL ownership can move while an old owner is still running. Every chunk and every
metadata write carries the owner's **WAL assignment term**:

- Metadata is published through catalog CAS; an owner refuses to update a meta
  carrying a newer term than its own assignment and stops persisting.
- Chunk object keys include both generation and term, and the pchannel meta carries a
  **manifest** mapping generation ranges to terms. Recovery only reads chunks
  published through that manifest.
- Writing a generation that already exists is arbitrated on the decoded footer: a
  newer term wins, an older term is fenced. Same term with different bytes is not
  automatically a conflict — the encoding is not guaranteed byte-stable — so the
  decoded footer identity is compared instead, keeping an identical rewrite
  idempotent while still detecting genuine corruption.

### Interaction with WAL truncation

**This is the coupling that most deserves review.** The durable summary source
checkpoint clamps the consume checkpoint, which in turn gates WAL truncation. The WAL
cannot be truncated past what the summary store has durably covered, because a
referenced chunk is the only durable copy of the idempotency keys below the consume
checkpoint once the WAL is gone.

Consequences:

- A stalled summary persist holds back WAL truncation. `idempotency_snapshot_checkpoint_lag_seconds`
  is the metric to alert on.
- An idle pchannel would freeze the durable source checkpoint (nothing marks a summary
  dirty, so no chunk is written). A meta-only checkpoint advance runs on the
  background task to keep truncation moving.
- Corruption of *referenced* summary state **fails the WAL open** rather than
  resetting to an empty window. Silently starting empty would accept in-TTL client
  retries as fresh writes — duplicate data with no error anywhere. Orphan-chunk
  corruption (above the durable latest generation, never referenced) self-heals inline.

### Chunk GC

The cleaner deletes generations in `[min_available, min_in_use)` and then advances
`min_available`. The advanced `min_in_use` is persisted **before** any deletion:
recovery replays from the persisted boundary upward and hard-fails on a missing chunk,
so an interruption between a delete and a meta save would otherwise leave the boundary
pointing into the deleted range and permanently fail every WAL open. With the advance
durable first, an interruption only leaks chunks below the new boundary, and the next
cycle re-deletes them idempotently.

`min_in_use` comes from a **durable-retention ledger** — one row per persisted
generation — not from the entries materialized in memory, which are cleared on every
persist. A boundary derived from memory would collapse to the latest generation each
cycle, GC would trim everything below, and a restart could rebuild only about one
snapshot interval of the window instead of a TTL's worth.

### Replication and CDC

Replicated messages **bypass the window entirely**. The replicate stream has its own
exactly-once delivery via source-timetick checkpoints, and the idempotency key inside
a replicated message belongs to the *source* cluster's window history. Deduplicating
against the local window would silently drop replicated writes whenever the key
happens to sit in this cluster's window — after a demotion, or after the source
evicted the key by TTL and a client legally re-issued it.

The recovery observer applies the same rule: a replicated write becomes a *keyless*
committed write (checkpoint bookkeeping only), so a foreign key can never materialize
a local entry.

### Reader-side physical dedup

Switching between the write-ahead buffer stream and the WAL scanner stream can
deliver the same logical message twice with *different* message IDs, which the
existing message-ID dedup cannot catch. The reorder buffer additionally drops a
non-TimeTick message whose timetick was already seen.

**Invariant:** the timetick interceptor assigns a unique timetick to every appended
message, so two genuinely distinct non-TimeTick messages never share a timetick while
both are retained. A repeated timetick can therefore only be a physical replay. If a
future code path ever lets two genuinely distinct messages reach this buffer with the
same timetick, the second is silently dropped — **this invariant must be preserved.**
Drops are surfaced by a warn log and
`idempotency_reader_physical_dedup_drop_total`.

The rule is gated on `streaming.idempotency.enabled`, so the flag is a real kill
switch that restores the pre-idempotency scanner behavior.

## Design Decisions

Trade-offs that were raised and settled. Each records what was chosen, and what was
rejected and why, so a later reader does not reopen a closed question without new
information.

### The key travels as a message property, not a header field

**Chosen:** the key lives in the `_ik` message property.

**Rejected:** a field on `InsertMessageHeader` (and a second one on
`CommitTxnMessageHeader`). A header field would need to be added to, and read from,
every message type that can be deduplicated, and the reader would have to decode a
specialized header before it could even tell whether a message carries a key. A
property is readable uniformly by one accessor across every message type and on both
the mutable (interceptor) and immutable (recovery) sides.

The specialized header is itself stored as a property, so this costs nothing on the
wire and removes a per-message-type change from every future view.

### The insert result stays on the wire

**Chosen:** keep `InsertMessageHeader.idempotent_result`.

**Rejected:** drop it and derive the result at the streaming node from the assembled
insert data. The reasoning is in
[Why the result is on the wire](#why-the-result-is-on-the-wire): `row_offsets` does
not exist anywhere below the proxy, and recovering `ids` would put a full insert-body
decode — vectors included, without the schema needed to locate the primary column —
on the WAL append hot path.

**Also rejected:** not storing the result in the summary store and re-deriving it on
replay. This trades a bounded, explicit storage cost for an implicit reconstruction
step on the recovery path, which is both slower and harder to keep correct as the
write path evolves.

### The store is business-agnostic; idempotency is a view on top

**Chosen:** a *summary store* that records committed write facts, with per-view
metadata scoped by `view_type`, and the dedup window built on top as one view.

**Rejected:** naming and shaping the storage after the idempotency window. A
primary-key index is the second planned consumer of the same facts; a store named and
structured for one consumer would have to be renamed and reshaped for the second. The
naming rule is enforced in the package documentation: "summary" is the durable,
application-neutral data, "window" belongs to the interceptor.

### Chunk payloads are protobuf, with the version in a fixed binary header

**Chosen:** protobuf payloads inside a framed object whose 16-byte fixed header
carries the format version.

**Rejected:** JSON payloads. **Rejected:** repeating the version inside the footer or
in the etcd metadata — the version must be readable before any proto in the object is
trusted, and a second copy could only ever agree with the first.

Because this is a first version with nothing released, no compatibility path to an
earlier chunk encoding exists or is needed.

### Retention is bounded by bytes, not only by entry count

**Chosen:** a hard `maxBytesPerWindow` cap that overrides both the TTL horizon and the
`minEntries` floor.

**Rejected:** an entry-count floor alone. Each retained entry carries the per-row
primary keys of its insert, so entry count says nothing about memory; one entry can be
arbitrarily large.

**Also settled:** the `minEntries` floor must not extend duplicate *visibility* past
TTL. An entry retained by the floor but past TTL is kept in memory and does not answer
duplicates — otherwise a quiet shard would answer retries forever.

### Referenced-state corruption fails the WAL open

**Chosen:** fail the WAL open, with operator remediation in the error.

**Rejected:** reset to an empty window and continue. The summary snapshot checkpoint
is what allowed the WAL to be truncated past those keys, so a referenced chunk is
their only durable copy. Starting empty would accept in-TTL client retries as fresh
writes — duplicate data, no error anywhere. Orphan chunks (above the durable latest
generation, never referenced) are a different case and self-heal inline.

### Split-brain is arbitrated on decoded content, not bytes

**Chosen:** compare the decoded footer identity when the same generation is rewritten
by the same term.

**Rejected:** byte equality. Protobuf guarantees deterministic output within a build
but not across library versions, so a retry spanning a binary upgrade would re-encode
identical records differently and be misread as corruption.

## Compatibility, Deprecation, and Migration Plan

**Compatibility.** The feature is off by default and adds no cost when off: no
summary store is written, the reader-side drop rule is disabled, and a non-idempotent
write carries no idempotency property at all (not an empty-valued one), so its
messages are byte-identical to before.

`InsertMessageHeader.idempotent_result` is a new optional field; older readers ignore
it. The `_ik` message property is absent unless a key exists.

**Enabling.** Set the global flag, then the collection property. Idempotency starts
from the current checkpoint; nothing historical is scanned.

**Disabling / rollback.** Turn off `streaming.idempotency.enabled`. On the next WAL
open the durable summary store is dropped automatically: with the feature off nothing
is recorded, checkpoints advance freely and the WAL gets truncated past the stored
source checkpoint, so a retained store would be stale by definition and, worse, on
re-enable its source checkpoint would rewind recovery to a position that may no longer
exist in the WAL. Deletion order is chosen for crash safety — vchannel metas, then the
pchannel meta, then the chunk objects — and is best-effort, retried on the next open.

Re-enabling later bootstraps cleanly from the then-current checkpoint. The dedup state
of the disabled period is lost, which is inherent to disabling the feature.

**No data migration.** No existing on-disk format changes.

## Test Coverage

Delivered with the implementation:

- **Proxy** — key derivation determinism and destination separation, key length
  validation, autoID reassignment stability and its interaction with delete routing,
  duplicate result merge back into the original row order, message size guard, DDL
  property validation.
- **Interceptor** — owner/wait/duplicate decisions, concurrent duplicates converging
  on one append, TTL and byte-cap eviction, restore-from-snapshot TTL bound, txn
  commit dedup with rollback synthesis, expired txn buffer classification, replicated
  bypass, vchannel reclamation.
- **Summary store** — chunk codec round-trip, checksum coverage of the exact stored
  bytes, corrupt-chunk rejection, referenced vs orphan corruption handling, generation
  manifest, term-based fencing (stale owner refused, newer owner overwrites, same-term
  different-content is corruption), GC boundary ordering, drop-while-disabled,
  concurrency and lock ordering.
- **End-to-end** — `wal_idempotency_test.go` drives append → duplicate → restart →
  duplicate-after-restart through a real WAL.

Two gaps are known and deliberately left open: chaos-level failover during an
in-flight idempotent append, and a long-running soak measuring window memory against
the byte cap under skewed shard load.

## Future Work

- **Stamp `idempotent_result` only for autoID collections.** For client-supplied
  primary keys the retry already has them, so the payload is redundant.
- **`Upsert` support.** Requires deduping the delete leg as well.
- **Primary-key index view.** The second consumer of the summary store; the
  `view_type` axis exists for it.
- **Live-process reconciliation.** See below.

## Known Limitations

- **Ambiguous append errors.** Releasing the key on append failure assumes an error
  means nothing was written, but some WAL implementations may land the write despite
  returning an error (the pulsar walimpls documents exactly this). In that window a
  same-key retry re-owns the key and appends again, producing duplicate rows — the
  same outcome a retry without idempotency would produce. Crash recovery is
  unaffected: the persisted window re-materializes landed keys at WAL open. Closing
  the live-process gap requires the interceptor window to reconcile from the
  recovery-side observer.
- **Bounded visibility.** Duplicate visibility ends at `windowTTL`, and the hard byte
  cap may shorten it further per vchannel under skewed load. A retry after that window
  is a fresh write. `idempotency_eviction_total` makes cap pressure observable.
- **Partial fan-out retries.** A retry after an attempt that reached only some shards
  is deduplicated on the landed shards and appended fresh on the missing ones — the
  intended outcome. The proxy cannot distinguish it from the pathological case where
  one shard's window forgot a key its siblings still hold, so the mix is logged rather
  than rejected: failing would break the legitimate case.

## References

- Implementation: milvus-io/milvus#50007
- Package documentation: `internal/streamingnode/server/wal/recovery/doc.go`
- Streaming system guide: `docs/agent_guides/streaming-system/streaming-system.md`
