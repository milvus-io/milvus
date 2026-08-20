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

The feature is off by default and is enabled per collection on top of a global
switch. It applies to `Insert` only.

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
- The guarantee survives streaming node restart and WAL failover, including an outage
  long enough that wall-clock TTLs would have expired.
- No data loss in the summary store under any crash point.
- Zero cost when the feature is off.

### Non-goals

- `Delete` and `Upsert`. Both are rejected with an explicit error when an idempotency
  key is supplied. Deleting the same rows twice is already effectively a no-op;
  `Upsert` needs its delete leg deduped as well, which is a separate design.
- Cross-cluster dedup. Replicated writes bypass the window entirely (see
  [Replication and CDC](#replication-and-cdc)).
- Unbounded retention. Duplicate visibility is a bounded window (see
  [Retention](#retention)).

## Public Interfaces

### Collection property

```
collection.insert.idempotency.enabled = "true" | "false"
```

Set at `CreateCollection` / `AlterCollection`. An unparseable value is rejected at DDL
time rather than silently downgrading a durability guarantee the operator believes is
on.

Both this property **and** the global `streaming.idempotency.enabled` must be true for
a collection's inserts to be idempotent.

### Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `streaming.idempotency.enabled` | `false` | Global kill switch. |
| `streaming.idempotency.maxBytesPerWindow` | `16MiB` | Per-vchannel in-memory window cap. Nothing is evicted until this is reached; then oldest-first. |
| `streaming.idempotency.retainedBytesPerVChannel` | `64MiB` | Durable retention budget per vchannel. Sets how far back chunks are kept, and therefore how much window can be rebuilt after a restart. |
| `streaming.idempotency.chunkTargetBytes` | `4MiB` | Size trigger for writing a chunk. |
| `streaming.idempotency.persistInterval` | `10s` | Time trigger for writing a chunk. |
| `streaming.idempotency.manifestChunkInterval` | `5` | Write the manifest after this many chunks. |
| `streaming.idempotency.maxKeyLength` | `1024` | Maximum accepted explicit key length in bytes. |

`retainedBytesPerVChannel` should be at least `maxBytesPerWindow`; otherwise the store
discards history a window would still have room to hold, and a restart rebuilds less
than the running process had.

There is deliberately **no TTL and no minimum entry count**. See
[Retention](#retention).

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

If no explicit key is supplied, the proxy derives one from the request (see
[Key derivation](#key-derivation)), so an unmodified client still gets idempotency for
a byte-identical retry.

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
| `idempotency_persist_total` | Chunk persist cycles, labeled by outcome. |
| `idempotency_persist_watermark_lag_seconds` | How far the durable persist watermark trails the consume checkpoint. Gates WAL truncation — the primary alert. |
| `idempotency_pending_gc_entries` | Ranges awaiting deletion. Grows without bound if GC is stuck; the direct GC health signal. |
| `idempotency_reader_physical_dedup_drop_total` | Scanner-side physical duplicate drops (see [Reader-side physical dedup](#reader-side-physical-dedup)). |

### Storage layout

etcd, under the streaming node catalog root:

```
streamingnode/<pchannel>/summary-store/pchannel-summary-meta
```

Object storage:

```
<root>/streamingnode/summary-store/<pchannel>/<pchannel>.manifest.<term>
<root>/streamingnode/summary-store/<pchannel>/chunks/chunk.<generation>.term<term>.psc
```

Nothing is stored per vchannel. How a consumer retains and evicts what it is handed is
entirely its own business, so the store keeps no per-consumer progress.

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
                              summary store (durable)
                       etcd meta ── manifest ── chunk objects
                                       │
                          restart ─────┴──► rebuild windows at WAL open
```

Two layers, deliberately separated:

- **The summary store** is a record of what a pchannel durably wrote. It stores
  committed write facts and nothing about why anyone wants them.
- **The dedup window** builds meaning on top. It decides what to keep and for how
  long; the store neither knows nor records that decision.

### Key derivation

An explicit client key is used as-is (length-checked). Otherwise the proxy derives one
deterministically:

```
key = SHA256( destination ‖ numRows ‖ canonical(client-supplied columns) )
```

**The destination must be in the key.** The dedup window is keyed by vchannel, and a
vchannel is a *collection shard* — not a partition, not a namespace. Two logically
distinct inserts of the same rows into two partitions of one collection would
otherwise hash to the same key, and the second would be answered as a duplicate with
the first insert's primary keys while its rows never reached the WAL. A derived key is
not under the caller's control, so the "do not reuse a key" contract that covers an
explicit key does not excuse that collision. The destination covers
`(dbName, collectionName, partitionName, namespace)`, every string length-prefixed so
neighbouring fields cannot shift into each other. `namespace` distinguishes unset from
empty: unset routes by primary key, empty routes by namespace.

The destination is hashed **exactly as the client sent it**, before the proxy resolves
an empty partition name to the default. A retry resends the same request, so the key
stays stable; spelling one destination two ways costs a missed dedup, never a merge of
two destinations.

The payload side covers only the client-supplied columns plus `numRows`: derivation
runs before the proxy fills field properties, function output, dynamic and namespace
fields, so by design it must not depend on any of them.

For autoID collections the primary column is excluded from the hash — it is
server-assigned and differs per attempt.

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
one round. The cost applies only to idempotency-enabled autoID collections.

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

Retention is **byte-bounded only**, at both layers.

**Window (memory).** Nothing is evicted while the window is under
`maxBytesPerWindow`. Once it is full, entries are replaced oldest-first. There is no
TTL and no minimum entry count.

**Store (objects).** Chunks are kept newest-first until the cumulative size reaches
`retainedBytesPerVChannel`; older ones are released. The boundary is computed **per
vchannel** by accumulating that vchannel's own `length` values from the manifest index,
then taking the minimum across vchannels. Without the per-vchannel split, one hot
vchannel would consume the whole budget and push a cold vchannel's history out of
retention even though the cold vchannel's window is nowhere near full.

Two consequences follow, and both must be stated plainly because they change what the
feature promises:

- **Duplicate visibility is measured in bytes of writes, not in time.** On a busy
  shard the retained window may span minutes; on a quiet one it may span days. A key
  written long ago on an idle shard still answers as a duplicate.
- **An idle vchannel does not release its window over time.** Memory is bounded by
  `maxBytesPerWindow`, not reclaimed by inactivity.

This is deliberate. A time-based rule cannot survive the case the feature exists for:
after an outage of any length, an upstream that resumes from its breakpoint must still
be deduplicated. Any horizon expressed in time is, by construction, invalidated by time
passing. A byte-bounded rule is invalidated only by new data, which is exactly the
condition under which forgetting old keys is safe.

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

### Three artifacts

**1. `PChannelSummaryMeta` (etcd), one per pchannel.**

```protobuf
message PChannelSummaryMeta {
    string pchannel                                = 1;
    uint64 source_checkpoint_timetick              = 2;
    common.MessageID source_checkpoint_message_id  = 3;
    int64 term                                     = 4;   // owner's WAL assignment term
    string manifest_path                           = 5;   // newest manifest written
}
```

It is a pointer and a checkpoint, nothing more. It carries no chunk inventory and no
GC boundaries.

**2. The manifest (object storage), one live version per pchannel.**

Path `{pchannel}.manifest.{term}`. Three sections, with three different jobs:

```protobuf
message PChannelSummaryManifest {
    // What recovery must read.
    repeated PChannelSummaryChunkTermRange ranges       = 1;
    // Redundant copy of every retained chunk's footer index, so a consumer can
    // locate its own vchannel inside a chunk without opening the chunk.
    repeated PChannelSummaryChunkIndexEntry chunk_index = 2;
    // What GC must delete. Carries the term, so GC needs no lookup and no listing.
    repeated PChannelSummaryChunkTermRange pending_gc   = 3;
}
```

`ranges` maps a contiguous generation range to the term that wrote it. `chunk_index`
holds, per generation, the per-vchannel `{offset, length, checksum, record_count}` copied
from that chunk's footer — this is what makes a lazy, ranged read of a single vchannel
possible, and it is also the input to the per-vchannel retention computation.

**A manifest is always written as the previous manifest plus amendments.** Inheritance
is structural, not a checklist: whatever a previous owner left unfinished — an unsealed
range, a pending GC entry — is carried forward because the new manifest is derived from
the old one rather than assembled from scratch.

**3. Chunk objects (object storage), one per generation.**

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

The format version lives **only** in the fixed binary header, checked before any proto
in the object is trusted; a second copy could only ever agree with the first. The footer
checksum covers the exact stored bytes and is carried in the trailer, so verification
never re-marshals a parsed footer — proto marshaling is not guaranteed byte-stable
across library versions, and re-deriving would flag a healthy chunk as corrupt the day
the encoding shifts.

A physical WAL position is recorded only at pchannel level. Everything below it is
addressed by timetick.

### The three watermarks

Everything in this design is ordered by three monotonic points:

```
  GC progress          retention boundary            persist watermark
       │                        │                            │
       ▼                        ▼                            ▼
 ──────┴────────────────────────┴────────────────────────────┴──────▶ generation
   deleted, or          oldest generation            newest contiguously
   queued in            recovery may read            confirmed chunk
   pending_gc
```

| Watermark | Meaning | Where it lives | Who advances it |
| --- | --- | --- | --- |
| **Persist watermark** | Source checkpoint of the newest **contiguously confirmed** chunk | meta's source checkpoint; newest range in the manifest | the persist path, on chunk durability |
| **Retention boundary** | Oldest generation still readable | the oldest entry of the manifest's `ranges` | the persist path, when the byte budget slides |
| **GC progress** | What has been physically deleted | implicit: `pending_gc` empty means done | the GC worker |

Invariants:

1. `GC progress ≤ retention boundary ≤ persist watermark`
2. All three advance monotonically.
3. Recovery reads only `[retention boundary, newest discovered generation]`, so it
   never touches a range GC is working on.

The persist watermark advances only over a **contiguous** prefix of confirmed writes.
Chunks may be written concurrently under load, but if generations G+1 and G+3 succeed
while G+2 fails, the watermark stops at G+1. G+3 physically exists and is simply not
counted; it becomes an orphan that later GC reclaims. This rule is what makes
"probe forward, stop at the first gap" a correct recovery procedure — the writer and
the reader agree on exactly the same boundary.

## Normal operation

### Write path

```
append(msg with `_ik`)
   │
   ├─ window.Begin(key)
   │     ├─ Duplicate → return stored result, no WAL append          ← terminates here
   │     ├─ Wait      → block on the owner's outcome
   │     └─ Owner     → continue
   │
   ├─ append to WAL
   ├─ window.Complete(key, result)          ← entry now serves duplicates
   └─ hand the committed fact to the summary store (in-memory, per vchannel)
```

The summary store buffers committed facts per vchannel and turns them into chunks
asynchronously. Nothing on the append path waits for object storage.

### Persist path

A chunk is written when any of these fires:

| Trigger | Condition |
| --- | --- |
| size | buffered bytes reach `chunkTargetBytes` |
| time | `persistInterval` elapsed with buffered data |
| forced | the recovery WAL checkpoint is about to advance |

The order within a cycle is fixed:

```
1. write chunk object          chunk.{g}.term{T}.psc
2. confirm durability          advance persist watermark over the contiguous prefix
3. every `manifestChunkInterval` chunks:
      recompute the retention boundary from the byte budget
      move released ranges + their index entries into `pending_gc`
      write the manifest                                    ← single PUT
4. occasionally: update the etcd meta (checkpoint, manifest path)
```

**The WAL checkpoint waits only on step 2.** It does not wait for the manifest and it
does not wait for the meta. This is what keeps object-storage latency off the WAL
truncation path, and it is the reason recovery must be able to discover chunks the
manifest does not yet know about — see [Startup and recovery](#startup-and-recovery).

Step 3 is a single object PUT that atomically does two things: it removes released
ranges from `ranges` (so recovery immediately stops depending on them) and adds them to
`pending_gc` (so GC keeps the term it needs to delete them). There is no window in which
a range is in neither place.

## Startup and recovery

### Sequence

```
1. read PChannelSummaryMeta from etcd            → term T_meta, manifest path
2. locate the newest manifest
      probe {pchannel}.manifest.{t} for t descending from T_now
      take the highest that exists                → manifest M, term T_M
3. probe forward for chunks the manifest does not know about
      for g = M.newest_generation + 1, 2, ...
          read chunk.{g}.term{T_M}
          stop at the first miss
4. write manifest.{T_new}
      = inherit M
      + seal T_M's range at the last discovered generation
      + open a new range for T_new
      + carry pending_gc forward unchanged
5. only now may this owner write its first chunk
6. read chunks in [retention boundary, newest discovered], rebuild the per-vchannel
   entry sets, hand each vchannel's set to its window
7. rewind WAL consumption to the persist watermark and replay forward
```

### Why one term is enough to probe

Step 3 probes a **single** term, not a range of terms, because of the ordering in step
4/5: **a term writes its manifest before it writes any chunk.** Therefore:

- a manifest exists for term T ⟹ every chunk of term T was written after it
- no manifest for term T ⟹ term T wrote no chunks

So the highest manifest that exists already covers every term that produced data, and
anything beyond it belongs to that same term. Probing degenerates from a
term × generation search into a single forward scan.

This is the load-bearing invariant of the whole recovery procedure. **A term that
cannot write its manifest must not write a chunk** — that has to be a hard failure in
code, not a best-effort ordering, because a chunk written before its manifest is a
chunk recovery can never find.

### Why nothing is lost

Three separate gaps could lose data, and each is closed by a different rule:

| Gap | Closed by |
| --- | --- |
| Chunks written after the last manifest | forward probing (step 3) |
| Writes after the last chunk | WAL replay from the persist watermark (step 7) |
| A previous term's tail discovered by probing | folded into the new manifest (step 4) |

The third deserves emphasis. If recovery probes T1's tail, finds generations 95–99, and
writes a manifest that still records T1 as ending at 94, then the *next* recovery reads
that manifest, probes forward only within its own term, and **95–99 are never read
again**. The loss is silent. This is why step 4 is expressed as *inherit and amend*
rather than *construct*: a manifest built from the previous one cannot drop what the
previous one knew, and cannot drop what this recovery just learned.

Because the WAL cannot be truncated past the persist watermark (see
[Interaction with WAL truncation](#interaction-with-wal-truncation)), the second gap is
always replayable. Together the three rules give: **every committed write is either in a
chunk recovery reads, or in the WAL segment recovery replays.**

### Crash matrix

Every crash point resolves without operator action:

| Crash point | On-disk state | Next startup |
| --- | --- | --- |
| Between chunk write and durability confirmation | chunk may exist beyond the watermark | probing stops at the first gap; the orphan is reclaimed by GC |
| Concurrent chunk writes, middle one failed | G+1, G+3 exist; G+2 missing | watermark is G+1; probing stops there; G+3 is an orphan |
| After chunks, before the manifest | chunks beyond the manifest | probing discovers them and seals them into the new manifest |
| After the manifest, before the meta | meta points at an older manifest | manifest probing finds the newer one by term |
| While writing the manifest | single PUT — old or new, never partial | both states are self-consistent |
| After moving ranges into `pending_gc`, before deleting | ranges gone from `ranges`, present in `pending_gc` | recovery does not read them; GC deletes them |
| After deleting, before recording completion | objects gone, `pending_gc` still lists them | GC re-issues deletes; each is a no-op |
| Manifest object fails its checksum | — | **WAL open fails.** See below. |

**Manifest corruption fails the WAL open.** It is the only index into the chunk
inventory, so a corrupt manifest means recovery cannot know what it is missing.
Silently starting with an empty window would accept in-retention client retries as fresh
writes — duplicate data with no error anywhere. Object storage is expected to return
what was written; a checksum failure is an infrastructure fault, and the honest response
is to stop and say so rather than to guess.

## Chunk GC

### Mechanism

GC is a worker owned by the manifest manager. Its entire input is the `pending_gc`
section of the manifest it already holds.

```
for each entry {term T, generations [a, b]} in pending_gc:
        for g in a..b:
                DELETE chunk.{g}.term{T}.psc        ← exact key; missing is a no-op
        mark the entry done in memory

at the next manifest persist: completed entries are dropped from pending_gc
```

That is the whole design. Three properties fall out of it:

**Clean.** GC never lists a prefix, never scans, and never consults `ranges`. The term
is carried in the entry that asked for the deletion, so every key is constructed
exactly. Prefix deletion is retained only as an operator repair tool, never on the
normal path — listing semantics differ across object-storage backends and cannot be
relied on.

**Idempotent.** Deleting an absent object succeeds everywhere. Completion is recorded
in memory and becomes durable at the next manifest write, so the worst case at any crash
point is that a completed batch is replayed as a sequence of no-ops. There is no state
that can be half-applied: a range is either in `pending_gc` or it is not, and that
transition is a single object PUT.

**Efficient.** One DELETE per object, no metadata round trips, no cross-store
coordination, no locks. GC and the persist path write the same manifest but never race
for it: both run inside the single owner, and completion is folded into the persist the
owner was going to do anyway.

### Why there is no separate GC watermark

`pending_gc` is simultaneously the work queue and the progress record: an entry present
means "not finished", an entry absent means "finished". A watermark alongside it would
be a second source of truth, and the two could disagree — a watermark that has advanced
past an entry still listed, or an entry dropped while the watermark lags. One structure
cannot contradict itself.

### Orphans

Two kinds of object are never referenced by any manifest:

- chunks written beyond the persist watermark (a failed concurrent write, or a crash
  between write and confirmation)
- chunks written by a fenced owner at a generation another term also used

Both sit above the retention boundary at the time they are created, and both are swept
into `pending_gc` when the boundary eventually passes them — the deletion is by
generation and term, and the entry that covers that generation range covers them too.
No separate reaping path is required.

## Split-brain fencing

WAL ownership can move while an old owner is still running. Every chunk and every
metadata write carries the owner's **WAL assignment term**:

- The etcd meta is published through catalog CAS; an owner refuses to update a meta
  carrying a newer term than its own assignment and stops persisting.
- Chunk object keys include both generation and term, so an old owner and a new owner
  writing the same generation produce two distinct objects rather than overwriting each
  other. The manifest decides which one is real; the other is an orphan.
- A new owner's manifest is written under its own term before it writes anything, so a
  stale owner's later chunks can never be mistaken for the current sequence.

A fenced owner may still be executing its own `pending_gc` deletions. This is safe: its
retention boundary was computed over strictly less data than the current owner's, so it
is strictly more conservative, and it deletes only what the current owner has also
released. It stops on its next manifest write, which fails the CAS.

## Interaction with WAL truncation

**This is the coupling that most deserves review.** The persist watermark clamps the
consume checkpoint, which in turn gates WAL truncation. The WAL cannot be truncated past
what the summary store has durably covered, because everything after the watermark
exists only in the WAL and must be replayable at the next open.

Consequences:

- A stalled persist path holds back WAL truncation.
  `idempotency_persist_watermark_lag_seconds` is the metric to alert on.
- An idle pchannel would freeze the watermark, since nothing marks a summary dirty and
  no chunk is written. A checkpoint-only advance runs on the background task to keep
  truncation moving.
- The manifest and the meta are explicitly **not** on this path. They may lag
  arbitrarily; recovery repairs the lag by probing. Only chunk durability gates the
  checkpoint.

## Replication and CDC

Replicated messages **bypass the window entirely**. The replicate stream has its own
exactly-once delivery via source-timetick checkpoints, and the idempotency key inside a
replicated message belongs to the *source* cluster's window history. Deduplicating
against the local window would silently drop replicated writes whenever the key happens
to sit in this cluster's window — after a demotion, or after the source released the key
and a client legally re-issued it.

The recovery observer applies the same rule: a replicated write becomes a *keyless*
committed write (checkpoint bookkeeping only), so a foreign key can never materialize a
local entry.

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

The rule is gated on `streaming.idempotency.enabled`, so the flag is a real kill switch
that restores the pre-idempotency scanner behavior.

## Design Decisions

Trade-offs that were argued and settled, with what was rejected and why.

### Retention is byte-bounded, with no TTL and no entry floor

**Chosen:** the window evicts oldest-first only when `maxBytesPerWindow` is reached;
the store keeps the newest `retainedBytesPerVChannel` per vchannel.

**Rejected — a TTL.** Any horizon expressed in time is invalidated by time passing.
After an outage, everything in the restored window is older than `now − TTL` and the
window is empty exactly when the resuming upstream needs it. Anchoring the horizon to
the last-persisted timetick instead of wall clock does not fix it either: a single new
write advances the anchor by the whole outage duration and wipes the window.

**Rejected — an entry-count floor.** It reintroduces the problem the byte cap exists to
solve: one entry carries the per-row primary keys of its insert, so a count says nothing
about memory. It also forces the store to ask each consumer what it still needs.

The cost of the choice is stated in [Retention](#retention): visibility is measured in
bytes, and idle windows are not released over time.

### The store keeps no per-consumer state

**Chosen:** no per-vchannel metadata of any kind. The store hands entries to a consumer
and forgets them.

**Rejected — per-vchannel metadata scoped by a consumer type.** Its purpose would be to
persist each consumer's retention boundary so GC could respect it. With byte-bounded
retention, GC derives its boundary from the manifest alone, so such metadata would have
no reader.

**Rejected — consumers reporting their in-use boundary to the pchannel.** This was
considered and dropped with the entry floor. It required a completeness gate (a missing
report must block GC, never be read as "no constraint"), and the expected-reporter set
had to track vchannel lifecycle events or GC would stall forever on a dropped vchannel.
All of that disappears when retention is a function of the manifest.

### GC state is the work queue

**Chosen:** `pending_gc` inside the manifest is both the list of work and the record of
progress.

**Rejected — a GC watermark in etcd.** GC would then need the generation→term mapping
from somewhere, and the only sources are the manifest's `ranges` (which couples GC to
the manifest's compaction lifetime) or a prefix listing (which is not portable across
object-storage backends).

**Rejected — an etcd deletion queue written alongside the manifest.** The manifest is in
object storage and the queue in etcd; there is no transaction across them. The safe
ordering (manifest first) leaves a crash window in which the objects are unreferenced
*and* unqueued, leaking permanently, which then needs a prefix-scan sweeper to reclaim —
reintroducing exactly what the queue was meant to avoid.

Keeping both sides in one object removes the coordination problem instead of solving it.

### A term writes its manifest before its first chunk

**Chosen:** manifest first, hard-failing if it cannot be written.

**Rejected — writing chunks first and reconstructing the term mapping at recovery.**
Recovery would have to probe every term in a range against every generation, and the
stop rule becomes unsound: a miss at one term cannot be distinguished from the end of
the sequence, so recovery would either truncate valid data or scan unboundedly.

### The persist watermark advances only over a contiguous prefix

**Chosen:** concurrent chunk writes are allowed, but the watermark stops at the first
unconfirmed generation.

**Rejected — counting every successful write.** Recovery's forward probe stops at the
first gap, so a watermark that ran past a gap would claim coverage for data recovery
cannot reach, and the WAL would be truncated past writes that are then unrecoverable.

### The chunk key carries the term

**Chosen:** `chunk.{generation}.term{term}.psc`.

**Rejected — a term-free key.** A fenced owner and the current owner would collide at
the same generation and overwrite each other. Conditional writes do not help: the
current owner cannot distinguish its own retry from a fenced owner's write.

## Compatibility, Deprecation, and Migration Plan

**Compatibility.** The feature is off by default and adds no cost when off: no summary
store is written, the reader-side drop rule is disabled, and a non-idempotent write
carries no idempotency property at all (not an empty-valued one), so its messages are
byte-identical to a build without the feature.

`InsertMessageHeader.idempotent_result` is a new optional field; older readers ignore
it. The `_ik` message property is absent unless a key exists.

**Enabling.** Set the global flag, then the collection property. Idempotency starts from
the current checkpoint; nothing historical is scanned.

**Disabling / rollback.** Turn off `streaming.idempotency.enabled`. On the next WAL open
the durable summary store is dropped: with the feature off nothing is recorded,
checkpoints advance freely and the WAL is truncated past the stored checkpoint, so a
retained store would be stale by definition and, on re-enable, would rewind recovery to a
position that may no longer exist in the WAL. Deletion order is chosen for crash safety —
manifest first (so recovery stops depending on the chunks), then the chunk objects, then
the meta — and is best-effort, retried on the next open.

Re-enabling later bootstraps cleanly from the then-current checkpoint. The dedup state of
the disabled period is lost, which is inherent to disabling the feature.

**No data migration.** The feature is unreleased; there is no earlier on-disk format.

## Test Coverage

**Unit** — proxy key derivation and destination separation, key length validation, autoID
reassignment stability and its interaction with delete routing, duplicate result merge
back into original row order, DDL property validation; window owner/wait/duplicate
decisions, byte-cap eviction, restore-from-snapshot, txn commit dedup with rollback
synthesis, expired txn buffer classification, replicated bypass; chunk codec round-trip
and checksum coverage, corrupt-chunk rejection, manifest inheritance, forward probing and
its stop rule, `pending_gc` transitions, GC idempotency across each crash point.

**StreamingNode integration** — `wal_idempotency_test.go` opens a real WAL through the
real opener and interceptor chain, appends, hits a duplicate, closes and reopens the WAL,
and hits the duplicate again after recovery. This exercises the interceptor, the summary
store, real chunk objects on local storage, and the manifest, but it is **not** an
end-to-end test: there is no proxy, no client, and no real object store or etcd.

**Known gaps.**

- No test spans SDK → proxy → streaming node → WAL. The proxy half (key derivation,
  autoID stabilization, duplicate result merge) and the streaming node half have never
  been exercised together.
- The integration test mounts only the idempotency and timetick interceptors. The
  production chain has seven, and the interactions with `redo` (which retries),
  `shard`/`partialupdate` (which run after idempotency), and `replicate` are uncovered.
- No chaos test for failover during an in-flight idempotent append.
- No soak test measuring window memory against the byte cap under skewed shard load.

## Future Work

- **Stamp `idempotent_result` only for autoID collections.** For client-supplied primary
  keys the retry already has them.
- **`Upsert` support**, which requires deduping the delete leg as well.
- **Lazy per-vchannel recovery.** The manifest already carries every chunk's per-vchannel
  index, so a consumer can be restored with ranged reads of only its own bytes instead of
  reading whole chunks. The layout is in place; the read path is not.
- **A second consumer.** A primary-key index would want full history rather than a
  byte-bounded tail, so it needs its own retention input; the current store is shaped for
  a single, byte-bounded consumer.

## Known Limitations

- **Ambiguous append errors.** Releasing the key on append failure assumes an error means
  nothing was written, but some WAL implementations may land the write despite returning
  an error (the pulsar walimpls documents exactly this). In that window a same-key retry
  re-owns the key and appends again, producing duplicate rows — the same outcome a retry
  without idempotency would produce. Crash recovery is unaffected: the persisted store
  re-materializes landed keys at WAL open. Closing the live-process gap requires the
  window to reconcile against the recovery-side observer.
- **Visibility is a byte budget, not a promise in time.** Two shards with different write
  rates have very different effective dedup horizons.
- **Idle windows are not released.** Memory is bounded by `maxBytesPerWindow` but is not
  reclaimed by inactivity.
- **Partial fan-out retries.** A retry after an attempt that reached only some shards is
  deduplicated on the landed shards and appended fresh on the missing ones — the intended
  outcome. The proxy cannot distinguish it from the pathological case where one shard's
  window released a key its siblings still hold, so the mix is logged rather than
  rejected: failing would break the legitimate case.

## References

- Implementation: milvus-io/milvus#50007
- Package documentation: `internal/streamingnode/server/wal/recovery/doc.go`
- Streaming system guide: `docs/agent_guides/streaming-system/streaming-system.md`
