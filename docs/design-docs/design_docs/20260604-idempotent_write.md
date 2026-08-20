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
| `streaming.idempotency.minRetainedBytesPerVChannel` | `64MiB` | Durable retention FLOOR, per vchannel. Chunks holding this much of a vchannel's bytes are kept regardless of age. |
| `streaming.idempotency.retentionTTL` | `10m` | Age at which a chunk may be released — but only where the floor already allows it. |
| `streaming.idempotency.maxRetainedChunks` | `256` | Hard CAP on retained chunks per pchannel. Overrides the floor. |
| `streaming.idempotency.gcInterval` | `10s` | How often queued deletions are swept. |
| `streaming.idempotency.maxKeyLength` | `1024` | Maximum accepted explicit key length in bytes. |

There is **no persist interval and no chunk size trigger**. A chunk is written
synchronously from the WAL checkpoint's dirty persist and from nowhere else, so its
batching is the checkpoint's batching. See [Persist path](#persist-path).

`minRetainedBytesPerVChannel` should be at least `maxBytesPerWindow`; otherwise the
store discards history a window would still have room to hold, and a restart rebuilds
less than the running process had.

The three retention knobs are not independent — the cap beats the floor and the floor
beats the TTL. See [Retention](#retention).

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
| `idempotency_persist_total` | Chunk persist cycles, labeled by outcome. A sustained failure rate stalls the WAL checkpoint itself, because the chunk is written inside it. |
| `idempotency_pending_gc_chunks` | Chunks awaiting deletion. Grows without bound if GC is stuck; the direct GC health signal. |
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

**Store (objects).** Three bounds decide which chunks survive, and they are deliberately
asymmetric:

| Bound | Kind | Beats |
| --- | --- | --- |
| `minRetainedBytesPerVChannel` | floor — keep at least this much | the TTL |
| `retentionTTL` | ceiling on age | — |
| `maxRetainedChunks` | cap — keep at most this many | the floor |

The releasable prefix is `min(floor boundary, TTL boundary)`, then forced down by the
cap.

**The floor beats the TTL** because a horizon expressed in time is invalidated by time
passing: a time-only rule empties the store after an outage, which is exactly when a
resuming client needs to be deduplicated.

**The cap beats the floor** because the floor bounds bytes while recovery pays per
*chunk*. A workload writing little per checkpoint fills the floor with a great many tiny
chunks, and both the manifest and the replay would grow without limit. When the cap
binds, the retained window is smaller than the floor asks for — a deliberate trade of
dedup coverage for a bounded recovery time.

The floor is accounted **per vchannel**: each vchannel's own section lengths are summed
from the manifest index, the oldest chunk each one still needs is resolved, and the
boundary is the minimum across them. Accounting whole object sizes instead would let one
hot vchannel consume the floor and evict a cold vchannel's keys — and a cold vchannel
mid-retry is exactly what this feature protects. Resolving per vchannel rather than
stopping the scan at the first fully-satisfied chunk matters for the same reason: an idle
vchannel's bytes live only in old chunks, so an early stop would never evaluate its floor
at all and would release its keys on a busy neighbour's schedule.

Two consequences follow, and both must be stated plainly because they change what the
feature promises:

- **Duplicate visibility is measured in bytes of writes, not in time.** On a busy
  shard the retained window may span minutes; on a quiet one it may span days. A key
  written long ago on an idle shard still answers as a duplicate.
- **An idle vchannel does not release its window over time.** Memory is bounded by
  `maxBytesPerWindow`, not reclaimed by inactivity.

This is deliberate. A byte-bounded rule is invalidated only by new data arriving, which
is exactly the condition under which forgetting old keys is safe. The TTL and the chunk
cap exist to bound cost, not to define the guarantee.

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
    string pchannel = 1;
    int64 term      = 2;   // owner's WAL assignment term
}
```

One field of substance. It is not a checkpoint and not an inventory; it exists only to
give recovery a floor when it probes for the newest manifest, so a pchannel with a large
assignment term but no store does not pay one probe per term.

**There is no persist watermark anywhere in this design.** A chunk is written
synchronously before the WAL consume checkpoint that covers it, so the two are aligned by
construction: everything below the checkpoint is already in a chunk, everything above it
is still in the WAL. A second recorded position could only ever disagree with it.

**2. The manifest (object storage), one per term.**

Path `{pchannel}.manifest.{term}`. Two lists, with two different jobs:

```protobuf
message PChannelSummaryManifest {
    // Every chunk recovery may read. Generations are contiguous: retention only
    // ever releases from the oldest end, never from the middle.
    repeated PChannelSummaryChunkIndexEntry chunks     = 1;
    // Chunks released by retention and not yet deleted. Present means "not deleted
    // yet"; absent means "done".
    repeated PChannelSummaryChunkRef        pending_gc = 2;
}

message PChannelSummaryChunkIndexEntry {
    uint64 generation = 1;
    int64  term       = 2;
    uint64 start_timetick = 3;
    uint64 end_timetick   = 4;
    repeated VChannelSummaryChunkIndex vchannels = 5;   // the chunk footer's index, copied
}

message PChannelSummaryChunkRef {   // all gc needs to build the object key
    uint64 generation = 1;
    int64  term       = 2;
}
```

Each entry carries its own term, so nothing has to be looked up to address a chunk
object. Copying the footer's per-vchannel index into the manifest is what makes a lazy,
ranged read of a single vchannel possible without opening the chunk, and it is also the
input to the retention computation — which therefore needs no reads at all.

**A manifest is always written as the previous manifest plus amendments.** Inheritance
is structural, not a checklist: whatever a previous owner left unfinished — a chunk it
wrote but never recorded, an entry still queued for deletion — is carried forward because
the new manifest is derived from the old one rather than assembled from scratch.

**3. Chunk objects (object storage), one per generation.**

A vchannel's records are stored as **separate sections**, split by which consumer needs
them, not as one message:

```
┌──────────────────────────────────────────────────────────┐
│ header 16B: magic "PSCCH001" | version | header size     │
├──────────────────────────────────────────────────────────┤
│ v0 §1 idempotency  {key, row_offsets}[]      ← optional  │
│ v0 §2 inserts      {msgID, tt, lastConfirmed, ids}[]     │
│ v1 §1 ...                                                │
│ v1 §2 ...                                                │
│                     (§3 reserved for deletes)            │
├──────────────────────────────────────────────────────────┤
│ footer (proto): pchannel, generation, term, tt span,     │
│   per vchannel: tt span + one {offset, length,           │
│   record_count} ref PER SECTION                          │
├──────────────────────────────────────────────────────────┤
│ sha256 over the exact footer bytes                       │
│ footer length (4B) | magic "PSCFT001"                    │
└──────────────────────────────────────────────────────────┘
```

A reader range-reads only its own section. A future primary-key index reads §2 without
paying for the idempotency keys — and since §2 already holds the primary keys, it does
not store them a second time. Turning idempotency off stops §1 from being written without
changing anything an insert records.

The two sections of one vchannel correspond **by position**: record *i* of §1 annotates
record *i* of §2. They are built in one pass over one sorted record slice, so there is no
way for them to disagree, and each ref's `record_count` makes the pairing verifiable on
read.

The **payload region has no self-describing boundaries** — locating a section is entirely
the footer's job. That is deliberate: it is what allows a single ranged GET.

Section refs carry **no checksum**. The object store already guarantees the bytes a read
returns are the bytes that were written, so a checksum there would only re-verify that.
The failure this format must catch is our own mislocation — an offset or length computed
wrong — and that is caught without one: the bounds check rejects a ref that leaves the
payload region, a decode of the wrong bytes fails to parse, and the record count must
match what decoded. The footer keeps its checksum, because every offset in the object
derives from it and a silently wrong footer would send every read to the wrong place.

The format version lives **only** in the fixed binary header, checked before any proto in
the object is trusted. The footer checksum covers the exact stored bytes and is carried in
the trailer, so verification never re-marshals a parsed footer — proto marshaling is not
guaranteed byte-stable across library versions, and re-deriving would flag a healthy chunk
as corrupt the day the encoding shifts.

A physical WAL position is recorded only at pchannel level. Everything below it is
addressed by timetick.

### The two positions

The watermark machinery this design started with is gone. Two positions remain, and both
live in the manifest:

```
  gc progress            retention boundary                 newest chunk
       │                        │                                │
       ▼                        ▼                                ▼
 ──────┴────────────────────────┴────────────────────────────────┴────▶ generation
   in pending_gc,          oldest generation                 == the WAL
   deleted or not          recovery may read                 consume checkpoint
```

| Position | Meaning | Where it lives | Who advances it |
| --- | --- | --- | --- |
| **Retention boundary** | Oldest generation still readable | first entry of the manifest's `chunks` | the persist path, when retention slides |
| **GC progress** | What has been physically deleted | implicit: absent from `pending_gc` means done | the GC worker |

Invariants:

1. `gc progress ≤ retention boundary ≤ newest chunk`
2. Both advance monotonically.
3. Recovery reads exactly `chunks`, so it never touches anything GC is working on.

The third invariant needs no cross-checking anywhere, because releasing a chunk and
queueing it for deletion are **the same manifest write**: recovery stops depending on the
object at the exact moment GC gains the term it needs to name it. There is no window in
which a chunk is in neither list, and GC never re-decides what is releasable.

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
   └─ stage the committed fact in the summary store (in memory, per vchannel)
```

Staging is a pure buffer append. Nothing on the append path touches object storage.

A **keyless** committed write is never staged: it materializes nothing for any consumer.
It only moves the vchannel's applied timetick, which exists solely to keep replay
idempotent.

### Persist path

There is exactly one trigger: the recovery storage's dirty-snapshot persist, which is
also what saves the WAL consume checkpoint. No timer of its own, no size trigger, no
background persist.

```
persistRecoverySnapshot:
   1. drain the staged records of every vchannel
   2. write chunk object            chunk.{g}.term{T}.psc        ← blocking
   3. record it in the manifest, recompute retention, write the manifest  ← blocking
   4. save the WAL consume checkpoint                            ← only now
```

Two consequences, and they are why this design is small:

- **The chunk covering a range is durable BEFORE the checkpoint covering that range is
  saved.** The checkpoint is therefore itself the boundary between what a chunk holds and
  what the WAL still holds. Nothing needs a second position, nothing needs to clamp the
  checkpoint, and recovery needs no rewind.
- **Batching is inherited.** The checkpoint persist already fires on accumulated data
  (`walRecovery.persistInterval`, or `walRecovery.maxDirtyMessage`, whichever comes
  first), so chunks are as large as the checkpoint's own batch instead of being cut by a
  second, independent timer.

The cost is deliberate: object-storage latency now sits on the checkpoint persist path,
and a failure at step 2 or 3 **fails the whole checkpoint persist**. That is the correct
outcome — a checkpoint that advanced past a chunk which was never written would leave
those keys in neither the store nor the replayable WAL, and nothing downstream could
detect it. Blocking on an unavailable object store is not a degradation to work around.

Within a cycle, **the chunk is written before the manifest, always.** A crash between the
two leaves a durable chunk the manifest does not name, which recovery repairs by probing
forward. The reverse order would leave the manifest naming an object that does not exist,
which it could not repair at all.

Step 3 is a single object PUT that does two things at once: it drops released chunks from
`chunks` and adds them to `pending_gc`.

## Startup and recovery

### Sequence

```
1. read PChannelSummaryMeta from etcd                    → floor term
      meta term > our term  → FENCED, refuse the open
2. locate the newest manifest
      probe {pchannel}.manifest.{t} for t descending from our term down to the floor
      take the highest that exists                       → manifest M, term T_M
3. probe forward for chunks M does not know about
      g = (M's newest generation) + 1, or 0 if M names none
      read chunk.{g}.term{T_M}, stop at the first miss
4. write manifest.{T_now}
      = inherit M + fold in every chunk discovered in step 3
      + carry pending_gc forward unchanged
5. only now may this owner write a chunk
6. read every chunk in M.chunks, rebuild each vchannel's record set,
   hand it to that vchannel's window
7. resume WAL consumption from the consume checkpoint — unchanged
```

**WAL replay is never rewound.** Step 7 uses the checkpoint exactly as it was saved,
because the chunk covering it was durable before it was saved.

### Why one term is enough to probe

Step 3 probes a **single** term because of the ordering in steps 4/5: *a term writes its
manifest before it writes any chunk*. Therefore:

- a manifest exists for term T ⟹ every chunk of term T was written after it
- no manifest for term T ⟹ term T wrote no chunks

So the highest manifest that exists already covers every term that produced data, and
anything beyond it belongs to that same term. Probing degenerates from a
term × generation search into a single forward scan, which in practice terminates after
one or two `Exist` calls — the manifest is written on every persist, so at most one chunk
can be unrecorded.

This is the load-bearing invariant of the whole procedure. **A term that cannot write its
manifest must not write a chunk** — a hard failure in code, not a best-effort ordering,
because a chunk written before its manifest is a chunk recovery can never find.

The probe starts at generation **0** when the manifest names no chunk at all. That is not
a degenerate case: a term that published its manifest, wrote its first chunk and then died
leaves exactly that state, and starting above "the newest recorded generation" would drop
that chunk permanently.

### Why nothing is lost

Two gaps could lose data, and each is closed by a different rule:

| Gap | Closed by |
| --- | --- |
| A chunk written after the last manifest write | forward probing (step 3) |
| Writes after the last chunk | WAL replay from the consume checkpoint (step 7) |

And one more rule keeps the first from re-opening later:

| Risk | Closed by |
| --- | --- |
| A previous term's tail found by probing, then forgotten | folding it into the new manifest (step 4) |

That third one deserves emphasis. If recovery probes T1's tail, finds generation 99, and
writes a manifest that still records T1 as ending at 98, then the *next* recovery reads
that manifest, probes forward only within its own term, and **99 is never read again**.
The loss is silent. This is why step 4 is *inherit and amend* rather than *construct*.

Together: **every committed write is either in a chunk recovery reads, or in the WAL
segment recovery replays.**

### Crash and failure matrix

| Failure | On-disk state | Next startup |
| --- | --- | --- |
| Crash between chunk write and manifest write | a chunk beyond `chunks` | probing discovers it and folds it into the new manifest |
| Crash while writing the manifest | single PUT — old or new, never partial | both states are self-consistent |
| Crash after the manifest, before the consume checkpoint is saved | chunk and manifest both ahead of the checkpoint | replay re-applies the messages; the records land in the staging buffer again and are written to a NEW generation. The old chunk is an orphan, reclaimed when retention passes it. Duplicate keys are idempotent on replay: the record set keeps the first sighting. |
| Object storage unavailable during persist | nothing written | the checkpoint persist fails and retries. WAL append keeps working; only the checkpoint stalls. |
| Crash after releasing chunks into `pending_gc`, before deleting | gone from `chunks`, present in `pending_gc` | recovery does not read them; GC deletes them |
| Crash after deleting, before completion reached the manifest | objects gone, `pending_gc` still lists them | GC re-issues the deletes; each is a no-op |
| A chunk `chunks` still names fails to decode | — | **WAL open fails.** See below. |
| A chunk found by probing fails to decode | an unreadable object above the manifest | dropped and the probe stops. Its writes are still in the WAL — the persist that wrote it had to write the manifest next, and failing that fails the whole checkpoint persist — so replay recovers them. |
| Manifest object fails its checksum | — | **WAL open fails.** |
| etcd meta names a newer term | — | **WAL open fails, fenced.** Another owner has taken the store; the stale open must not overwrite it. |

**Corruption of state the manifest still retains fails the WAL open.** The WAL is
truncated on the consume checkpoint, which advanced only after that chunk was durable, so
the chunk is the only remaining copy of those keys. Silently starting with an empty window
would accept in-retention client retries as fresh writes — duplicate data with no error
anywhere. Object storage is expected to return what was written; a checksum failure is an
infrastructure fault, and the honest response is to stop and say so.

The remediation is explicit in the error: set `streaming.idempotency.enabled=false` and
restart to drop the corrupted store, then re-enable for a clean bootstrap. Idempotency
history is lost either way; this way the loss is visible.

## Chunk GC

### Mechanism

GC is a worker owned by the summary manager. Its entire input is the `pending_gc` list of
the manifest it already holds.

```
every gcInterval:
    for each {generation g, term T} in pending_gc:
            skip if already completed in memory
            DELETE chunk.{g}.term{T}.psc        ← exact key; missing is a no-op
            mark completed in memory

at the next manifest write: completed entries are dropped from pending_gc
```

That is the whole design. Three properties fall out of it:

**Clean.** GC never lists a prefix, never scans, and never cross-checks `chunks`. The
term is carried in the entry that asked for the deletion, so every key is constructed
exactly. An entry only ever lands in `pending_gc` by the same manifest write that removed
it from `chunks`, so the decision was already made atomically and GC has nothing to
re-derive. Prefix deletion survives only on the disabled-idempotency drop path, never on
the normal one — listing semantics differ across object-storage backends.

**Idempotent.** Deleting an absent object succeeds everywhere. Completion is recorded in
memory and becomes durable at the next manifest write, so the worst case at any crash
point is that a completed batch is replayed as a sequence of no-ops. There is no state
that can be half-applied: a chunk is either in `pending_gc` or it is not, and that
transition is a single object PUT.

**Efficient.** One DELETE per object, no metadata round trips, no cross-store
coordination, no locks. GC and the persist path write the same manifest but never race for
it: both run inside the single owner, and completion rides along on a manifest write the
owner was going to do anyway.

A delete that fails leaves its entry queued and returns; the next cycle retries it.
Stopping costs nothing — the objects are already unreferenced by every reader.

### Why there is no separate GC watermark

`pending_gc` is simultaneously the work queue and the progress record: an entry present
means "not finished", absent means "finished". A watermark alongside it would be a second
source of truth, and the two could disagree — a watermark advanced past an entry still
listed, or an entry dropped while the watermark lags. One structure cannot contradict
itself.

### Orphans

Two kinds of object are never named by any manifest:

- a chunk written by a persist whose manifest write then failed, or whose checkpoint save
  then failed
- a chunk written by a fenced owner at a generation another term also used

Neither is reachable by recovery, and neither is in `pending_gc`. They are reclaimed when
retention passes their generation range, since a released entry's key is built from the
generation and term the manifest recorded. An object at a generation the manifest never
recorded is swept by the disabled-idempotency drop path, which is the one place a prefix
delete is used.

## Split-brain fencing

WAL ownership can move while an old owner is still running. Every chunk and every
metadata write carries the owner's **WAL assignment term**:

- The etcd meta is published through catalog CAS; an owner that reads a meta carrying a
  newer term than its own assignment refuses the open outright rather than persisting
  into a store it no longer owns.
- Chunk object keys include both generation and term, so an old owner and a new owner
  writing the same generation produce two distinct objects rather than overwriting each
  other. The manifest decides which one is real; the other is an orphan.
- A new owner's manifest is written under its own term before it writes anything, so a
  stale owner's later chunks can never be mistaken for the current sequence.

Chunk writes are additionally arbitrated at the object: `Exist`→`Write` is not atomic, so
two split-brain owners can both pass the absence check for one generation. The write path
decodes the stored footer and compares terms — the newer term overwrites, the older is
fenced, and only a same-term content mismatch is treated as corruption. A same-term retry
is recognised by comparing the decoded RECORDS rather than the bytes, because proto
encoding is not guaranteed byte-stable across library versions and a retry spanning a
binary upgrade would otherwise be reported as corruption.

A fenced owner may still be executing its own `pending_gc` deletions. This is safe: its
retention boundary was computed over strictly less data than the current owner's, so it is
strictly more conservative, and it deletes only what the current owner has also released.

## Interaction with WAL truncation

**This is the coupling that most deserves review**, and the synchronous persist is what
makes it simple. The store can never lag the consume checkpoint, so it can never require
WAL that truncation would remove — there is no summary term in the truncation minimum at
all.

What the coupling costs instead is **liveness**: object-storage latency now sits on the
checkpoint persist path, and a persist failure fails the checkpoint. Consequences:

- A stalled object store stalls checkpoint advancement, and therefore WAL truncation.
  `idempotency_persist_total{status="fail"}` is the metric to alert on. WAL append itself
  keeps working; only the checkpoint stops moving.
- An idle pchannel writes no chunk at all — there is nothing staged, so the persist is a
  no-op and the checkpoint advances on its own.
- The etcd meta is explicitly not on this path. It is read once at open and written once
  at ownership change.

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

### Retention is byte-bounded, with the TTL subordinate to a byte floor

**Chosen:** the window evicts oldest-first only when `maxBytesPerWindow` is reached. The
store keeps at least `minRetainedBytesPerVChannel` per vchannel regardless of age, expires
by `retentionTTL` only outside that floor, and caps the whole thing at
`maxRetainedChunks`.

**Rejected — a TTL as the primary bound.** Any horizon expressed in time is invalidated
by time passing. After an outage, everything in the restored window is older than
`now − TTL` and the window is empty exactly when the resuming upstream needs it. Anchoring
the horizon to the last-covered timetick instead of wall clock does not fix it either: a
single new write advances the anchor by the whole outage duration and wipes the window.
The TTL survives as a cost bound *behind* the floor, where it cannot do that.

**Rejected — an entry-count floor.** It reintroduces the problem the byte cap exists to
solve: one entry carries the per-row primary keys of its insert, so a count says nothing
about memory. It also forces the store to ask each consumer what it still needs.

**Rejected — bytes alone, with no chunk cap.** The floor bounds bytes, but recovery pays
per chunk: a workload writing little per checkpoint fills the floor with an unbounded
number of tiny chunks, growing the manifest and the replay without limit. The cap is the
one bound that closes this, and it therefore has to override the floor.

The cost of the choice is stated in [Retention](#retention): visibility is measured in
bytes, idle windows are not released over time, and a bound window may be shorter than the
floor promises.

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
from somewhere, and the only sources are the manifest's chunk list (which couples GC to
the manifest's compaction lifetime) or a prefix listing (which is not portable across
object-storage backends). Carrying the term on the queued entry itself removes the
lookup entirely.

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

### The persist is synchronous with the WAL checkpoint

**Chosen:** the chunk is written inside the checkpoint's dirty persist, before the
checkpoint is saved, and a failure fails the checkpoint.

**Rejected — an independently scheduled persist with a durable watermark.** That was the
original design. It required a persist watermark in etcd, a clamp so the consume
checkpoint could not outrun it, a rewind at recovery to replay the gap, and a rule that
the watermark advance only over a contiguous prefix of confirmed chunks. All four exist
only to reconcile two positions that drift. Making the write ordered against the
checkpoint removes the drift, and all four disappear — along with the ~3000 lines that
implemented them.

The trade is explicit: object-storage latency moves onto the checkpoint path. It is the
right trade because a checkpoint that outran its chunk loses data undetectably, whereas a
stalled checkpoint is visible and self-healing.

### The vchannel payload is split into per-consumer sections

**Chosen:** one section for the idempotency annotation (`key`, `row_offsets`), one for the
write itself (identity + primary keys), each indexed separately by the footer.

**Rejected — one message per vchannel.** Every reader would decode all of it, and a future
primary-key index would have to store the primary keys a second time. The split also lets
idempotency be turned off without changing what an insert records, and leaves room for a
delete section without touching either existing one.

**Rejected — an explicit cross-reference between the sections.** Position is enough: both
are built in one pass over one sorted slice, so they cannot disagree, and each ref's
`record_count` makes the pairing checkable on read. A sparse mapping can be added later as
an optional field if a case ever needs it.

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
the durable summary store is dropped: with the feature off nothing is recorded and the
WAL is truncated past what the store covers, so a retained store would be stale by
definition. Deletion is a prefix sweep of the chunk directory followed by the etcd meta —
the one place a prefix delete is used, because the drop path has no later retention pass
to reclaim a chunk written but never recorded. It is best-effort and retried on the next
open.

Re-enabling later bootstraps cleanly from the then-current checkpoint. The dedup state of
the disabled period is lost, which is inherent to disabling the feature.

**No data migration.** The feature is unreleased; there is no earlier on-disk format.

## Test Coverage

**Unit** — proxy key derivation and destination separation, key length validation, autoID
reassignment stability and its interaction with delete routing, duplicate result merge
back into original row order, DDL property validation; window owner/wait/duplicate
decisions, byte-cap eviction, restore-from-snapshot, txn commit dedup with rollback
synthesis, expired txn buffer classification, replicated bypass; chunk codec round-trip
and frame damage rejection, section misalignment and out-of-bounds section refs, the
absent-idempotency-section case, single-section ranged read, retention across all three
bounds (including the idle-vchannel case and the chunk cap overriding the floor),
manifest inheritance, forward probing including the probe-from-generation-zero case,
term arbitration on a concurrent same-generation write, `pending_gc` transitions, and GC
idempotency across each crash point.

**StreamingNode integration** — `wal_idempotency_test.go` opens a real WAL through the
real opener and interceptor chain, appends, hits a duplicate, closes and reopens the WAL,
and hits the duplicate again after recovery. This exercises the interceptor, the summary
store, real chunk objects on local storage, and the manifest, but it is **not** an
end-to-end test: there is no proxy, no client, and no real object store or etcd. It also
pins the response contract: a duplicate returns the first attempt's message id, timetick
and last-confirmed position unchanged.

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
- **Lazy per-vchannel, per-section recovery.** The manifest already carries a
  `{offset, length}` ref per section per vchannel, so a consumer can be restored with
  ranged reads of only the bytes it needs. In particular the window could be rebuilt from
  the idempotency section alone — keys and offsets, without the primary keys, which
  dominate a chunk's size — and fetch the insert section only when a duplicate actually
  hits. The layout is in place; the read path is not.
- **A delete section.** The chunk format reserves the third section and the vchannel index
  reserves its field number; nothing else in the design has to move.
- **A primary-key index as a second consumer.** It reads the insert section, which already
  holds every primary key, so nothing is stored twice. It would want full history rather
  than a bounded tail, so it needs its own retention input; the current store is shaped for
  a single, bounded consumer.

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
