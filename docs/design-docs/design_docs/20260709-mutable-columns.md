# Mutable Columns: In-Place Partial Update via Merge-on-Read Patches

- Status: Draft (revision 2)
- Author: Xiaofan Luan
- Date: 2026-07-09; revision 2: 2026-11-01
- Related: [discussion #51115](https://github.com/milvus-io/milvus/discussions/51115),
  issue #52063 (mutable columns), issue #54943 (ARRAY_TRIM and array op
  semantics under retry / idempotency)

## Revision 2 Summary

Revision 1 (2026-07-09) established the mechanism: patches are deletes with a
payload, an in-memory overlay with version chains serves reads, and patch
compaction plus column folding bound the overlay. Revision 2 keeps all of
that and changes the following:

| # | Revision 1 | Revision 2 | Why |
|---|---|---|---|
| R1 | Overlay served through a `PatchedColumnView` at the Raw chunk accessor / `bulk_subscript` | Mutable columns live in their **own column group**, written as **Vortex**, loaded with `local_format=vortex`; the overlay is applied at the two Vortex decode outlets (Scan batches and Take results) | Every sealed read of a Vortex column already funnels through `VortexColumn` Scan/Take (see "Read Path"); the Raw chunk accessors are scattered across ~6 families of call sites |
| R2 | Numeric, BOOL, TIMESTAMPTZ, ARRAY only; VARCHAR / JSON / GEOMETRY / struct immutable | **All non-vector types in Phase 1** (adds VARCHAR, TEXT, JSON, `$meta`, GEOMETRY, native-list ARRAY, ArrayOfStruct with scalar sub-fields); vectors, sparse vectors and EmbList in Phase 2 | At a Vortex outlet a variable-length value is patched by swapping one view pointer; text match moves to a tantivy delta that is isomorphic to the growing text index |
| R3 | Ops SET / INCR / APPEND / POP_FRONT / REMOVE | Ops **SET / APPEND / REMOVE / TRIM / PATH_REPLACE**; INCR deferred; POP_FRONT replaced by TRIM (Redis `LTRIM`) | One Redis-aligned op set; TRIM covers pop-front, pop-back and keep-last-N |
| R4 | `(R, S)` base-free folding for any APPEND/REMOVE interleaving | Base-free folding only for **SET absorbing earlier ops** and **concatenating consecutive APPENDs** | `max_capacity` clamps after every op, which makes REMOVE/TRIM results depend on the current length |
| R5 | Retries out of scope | **Request-level idempotency key**; no CAS for patch ops | Today the SDKs auto-retry Upsert and there is no Upsert idempotency key, so non-idempotent array ops can apply twice (#54943) |
| R6 | Three phases | **Two phases**: Phase 1 brute force only; Phase 2 index-backed search (scalar index result correction, text match delta, vector search correction) | |
| R7 | — | Two delete-path invariants made explicit: patch entries dedupe by `(offset, column, ts, seq)` exactly like `DeletedRecord`'s `(ts, offset)` set, and `fold_ts` never exceeds the channel's L0-distributed watermark | Unlike deletes, APPEND is not idempotent and a late older SET must not override a folded newer one |

Sections below are revision 1 text unless they carry a "(rev 2)" marker or
were rewritten for the changes above.

## Motivation

Today any change to a single scalar value in Milvus requires a full-row
upsert: the entire row (including vectors) is rewritten, a delete tombstone
is emitted, and compaction pressure grows. For frequently-updated scalar
fields — counters, inventory, timestamps, status flags, ACL id lists — this
is prohibitively expensive, and users work around it by joining against an
external KV store.

A concrete example of the demand is
[discussion #51115](https://github.com/milvus-io/milvus/discussions/51115):
continuously updating a single `offline_time` timestamp column at 50k
updates / 30s (~1,700 rows/s) while embeddings and every other field stay
unchanged. Today each such update is a delete + reinsert; once a segment's
update ratio crosses the compaction threshold (~20%), the whole segment —
vectors included — is rewritten and its vector index rebuilt, so a steady
update stream becomes a permanent compaction-and-indexing storm that only a
large cluster can absorb. Under this design the same stream is ~1,700 tiny
SET ops/s down the delete-shaped write path; overlay memory grows only with
the number of distinct touched rows, and folding rewrites just the 8-byte
column file — vector data and vector indexes are never touched.

This design introduces **mutable columns**: fields that can be updated in
place through a partial-update API, without rewriting the row, touching the
vector data, or invalidating any index.

## Design Summary: a Generalization of the Delete Path

The entire design reduces to one observation: **a delete is a patch that
makes a row disappear; an update is a patch that changes a value.** Milvus
already runs the full machinery for the first kind — WAL message, L0 →
PK-routed deltalogs, in-memory application, ts-based MVCC, a monotone
bitmask over index results, compaction-time folding. Updates reuse all of
it and add exactly one structural element on the read side:

```
result = (index_bitset AND NOT delete_bitset AND NOT patched_bitset)  -- index answers untouched rows
         OR brute_force(patched rows, materialized at query_ts,       -- dirty rows answered by scan
                        base fallback when no version ≤ ts)
```

Deletes only mask (`AND NOT`); updates mask **and add the OR-back leg**. In
v1 (no indexes) the formula degenerates to the brute-force scan with
overlay fix-up.

Two loops keep it healthy:

- **Correctness loop**: version chains materialize at any query_ts, with
  base fallback — every reader sees the state as of its timestamp.
- **Performance loop**: more patched rows → more expensive OR leg → past a
  threshold, fall back to full brute force → folding zeroes the patched set
  and rebuilds the index → back on the index fast path.

In one sentence: the write side is "the delete path with a payload", the
read side is "the delete mask plus one OR-back leg", and the background is
"two new members of the compaction family (patch compaction, column
folding)". No mechanism in this design is invented from scratch.

## Terminology

| Term | Meaning |
|---|---|
| patch | One logged mutation, `(pk, ts, seq, op, operand)`; op ∈ {SET, APPEND, REMOVE, TRIM, PATH_REPLACE} (INCR reserved) |
| patch deltalog | Persisted file of patches for one (segment, mutable column); one emitted per flush cycle; internally ts-ordered |
| overlay | Per-(segment, mutable column) in-memory structure serving current values; chunk-partitioned `hashmap<offset, version chain>` |
| version chain | Per-row list of `(ts, seq, op, operand)` nodes providing MVCC |
| floor node | The single chain node holding everything folded below safe_ts |
| safe timestamp (safe_ts) | Lower bound of any timestamp a current or future reader may use; history below it is dead and purgeable |
| materialization | Computing an absolute value from base + ops; happens only at read, pruning and column folding |
| patch compaction | Background merge of deltalogs with per-PK op folding; output is still deltalog format |
| column folding | Background rewrite of the mutable column group with patches applied, swapped in atomically via the segment manifest |
| fold_ts | Per-(segment, column group) watermark recorded in the manifest; load replays only entries with `ts > fold_ts` |
| L0-distributed watermark | Per-channel ts below which every L0 patch has already been routed to its target segments (rev 2) |
| request_id | Client-supplied idempotency key of one `update()` call, reused across retries (rev 2) |
| patched_bitset | Monotone per-(segment, column) bitmap of ever-patched rows; used for index result correction (Phase 2) |

## Goals

- Partial update of designated (`mutable=true`) columns by primary key, with
  **no read on the write path and no CAS**.
- **Phase 1 types**: BOOL, INT8/16/32/64, FLOAT, DOUBLE, TIMESTAMPTZ,
  VARCHAR/STRING, TEXT, JSON, the dynamic field `$meta`, GEOMETRY, ARRAY
  (legacy and native list: element-nullable and nested), and ArrayOfStruct
  whose sub-fields are scalar. Future fixed-width types (DECIMAL, DATE, TIME)
  follow the fixed-width rules when they land.
- **Phase 1 reads**: mutable columns may appear in filters and outputs; dirty
  rows are evaluated by brute-force fix-up. **Phase 2** adds index-backed
  search on mutable columns and mutable vector / sparse / EmbList fields.
- **Redis-aligned array semantics** (`RPUSH`, `LREM 0`, `LTRIM`, `LSET`)
  that are deterministic, replicable and replayable.
- **Exactly-once under client retry** through a request-level idempotency key.
- Reuse existing subsystems: DML channel/WAL, PK bloom-filter routing,
  L0→L1 compaction, manifest-based atomic column-group replacement. No new
  storage subsystem.
- MVCC semantics identical to deletes: timestamp-based visibility, same
  consistency guarantees.

## Non-Goals

- **INCR** is deferred. Its analysis is kept under "SET and INCR" for when it
  returns.
- Set-semantics and positional array ops beyond PATH_REPLACE: AddUnique /
  `$addToSet`, PREPEND (`LPUSH`), INSERT (`LINSERT`), REMOVE with a count,
  REMOVE-at-index, and value-returning pops (`LPOP`/`RPOP` return values are
  reads). The op enum is append-only, so any of these can be added later.
- Conditional / CAS-style updates. Business-version conditional upsert
  (#54532 / #54697) stays on the read-merge-write path.
- Atomicity of one batch across vchannels. Each vchannel commits
  independently, exactly as insert and delete do today.
- Mutation of the primary key, partition key or clustering key. Function
  output fields are not written directly; Phase 2 recomputes them in the proxy
  from new inputs.
- Raw local format for mutable columns. This becomes possible once local
  format phase 2 moves all sealed reads behind `ColumnInterface`.
- Synchronous feedback on whether an update matched a row (see API).

## Schema and API Semantics

A field may be declared with `mutable=true`. Constraints:

- **Not allowed** for: the primary key, partition key, clustering key, or a
  function output field. Vector, sparse vector and EmbList fields are allowed
  only from Phase 2.
- **Storage**: the split policy places mutable fields in their own column
  group(s). That group is written in the Vortex physical format and loaded
  with `local_format=vortex`. The PK never shares a group with a mutable
  field.
- **Phase 1 only**: a mutable field may not carry a scalar index, text match,
  NGRAM, FM, TRIE or RTREE. JSON stats are allowed; dirty rows bypass them.
  Output values must come from column data, never reverse-looked-up from an
  index.
- **`$meta`**: allowed. It is treated like JSON (see "JSON and the dynamic
  field").

New DML API (rev 2):

```
update(collection,
       pks=[...],
       ops={
         "status":  Set(v),                    # any mutable type, including NULL
         "tags":    Append([v, ...]),          # array tail (RPUSH)
         "blocked": Remove([v, ...]),          # remove all occurrences (LREM 0)
         "events":  Remove(where=("id", 7)),   # ArrayOfStruct: by scalar sub-field value
         "recent":  Trim(start, stop),         # keep [start, stop] (LTRIM)
         "meta":    PathReplace('["a"][0]', v) # positional / JSON path (LSET-like)
       },
       request_id=None,      # SDK generates one per logical call, reuses it on retry
       check_exists=False)   # optional best-effort PK existence check
```

Rules:

- **One op per field per request.** Ordering inside a request is never
  ambiguous; a multi-step change is several requests, ordered by ts.
- **Operands** are either broadcast to every PK or given per PK.
- **Missing PK: silent ignore**, like a delete whose PK matches no segment.
  The call returns "accepted", not "applied". With `check_exists=True` the
  proxy rejects PKs that the PK bloom filter / PK index cannot find. This is
  best effort: a row deleted after the check makes the patch a no-op.
- **Errors that need the current value cannot be reported** on the write
  path. Out-of-range PATH_REPLACE, a missing JSON parent and similar cases are
  deterministic no-ops on every replica.
- **`upsert(partial_update=True)` is unchanged.** It remains a read-merge-write
  path with CAS, synchronous errors and insert-on-missing-PK. Users who need
  those semantics keep using it.
- **Idempotency**: see "Write Path → Request-level idempotency".

### Supported types × operations (rev 2)

| Type | SET | APPEND | REMOVE | TRIM | PATH_REPLACE | Phase 1 | Phase 2 |
|---|---|---|---|---|---|---|---|
| BOOL, INT8–64, FLOAT, DOUBLE | ✓ | — | — | — | — | brute force | STL_SORT / BITMAP / INVERTED / HYBRID (and bit-sliced, #54373) via result correction |
| TIMESTAMPTZ | ✓ | — | — | — | — | brute force | STL_SORT via result correction |
| VARCHAR / STRING | ✓ | — | — | — | — | brute force | INVERTED / BITMAP / TRIE / STL_SORT / NGRAM / FM via result correction; text / phrase match via tantivy delta |
| TEXT (LOB) | ✓ | — | — | — | — | brute force | text / phrase match via tantivy delta |
| JSON | ✓ | — | — | — | ✓ JSON path | brute force; JSON stats bypassed for dirty rows | JSON path index / INVERTED / NGRAM via result correction |
| `$meta` | ✓ (top-level key merge) | — | — | — | ✓ JSON path | as JSON | as JSON |
| GEOMETRY | ✓ (WKB) | — | — | — | — | brute force | RTREE via result correction |
| ARRAY, legacy | ✓ | ✓ | ✓ by value | ✓ | ✓ `[i]` | brute force | INVERTED / BITMAP / HYBRID / STL_SORT via result correction |
| ARRAY, native list (element-nullable / nested) | ✓ | ✓ outer list | ✓ by value (NULL matches NULL; nested: deep equality) | ✓ outer list | ✓ `[i]` | brute force | element-level indexes when #51393 lands |
| ArrayOfStruct (scalar sub-fields) | ✓ | ✓ elements | ✓ `where child == v` | ✓ | ✓ `[i]`, `[i].child` | brute force | sub-field indexes via result correction |
| ArrayOfStruct with vector sub-fields, EmbList | — | — | — | — | — | — | SET / APPEND / TRIM via vector search correction |
| FLOAT / BINARY / FP16 / BF16 / INT8 vector, SPARSE_FLOAT_VECTOR | — | — | — | — | — | — | SET via vector search correction |
| PK, partition key, clustering key | — | — | — | — | — | — | — |

## Update Semantics

### SET and INCR

> (rev 2) INCR is deferred; the INCR analysis below is kept for when it
> returns. SET semantics are unchanged.

Patch entries are `(pk, ts, op, operand)` with op ∈ {SET, INCR}.

- **SET** is last-write-wins by timestamp.
- **INCR** (numeric only) exists because LWW SET loses updates for the
  flagship counter use case: two clients doing read→increment→write
  concurrently overwrite each other, and Milvus has no transactions to
  compensate. INCR is commutative and needs no read of the old value.

**Deltas are stored as deltas, materialized only at read and fold time.**
This is a hard rule, not an optimization: materializing an INCR into an
absolute value requires reading the base column — DataNodes never load
sealed column data, and materializing on QueryNodes would trigger random
reads into possibly-evicted (mmap) base chunks on the apply path. Instead:

- WAL messages, patch deltalogs, and the overlay all carry raw ops.
- Folding rules are associative and never need the base: a run of INCRs
  folds to one INCR carrying the sum; SET absorbs everything before it;
  `SET(v)` followed by INCRs folds to `SET(v + Σdeltas)`. Patch compaction
  applies these rules, so a fully compacted entry is always a single op.
- A read at timestamp `ts` computes `base_or_SET + Σ(deltas ≤ ts)` from the
  overlay's version chain.

Edge semantics to fix during implementation (listed in Open Questions):
INCR on a NULL value, integer overflow behavior, float accumulation error.

### Array operations (rev 2: Redis-aligned)

**Admission rule** (unchanged from revision 1): an op must be loggable
without reading the old value, and applying it must be a deterministic state
transition. Determinism, together with the WAL's per-channel total order and
PK→vchannel affinity, is what makes replication, CDC and recovery replay
converge. Commutativity is not required.

| Op | Redis analogue | Semantics | Idempotent (same request applied twice) |
|---|---|---|---|
| `SET(arr)` | `SET` | Replace the whole value; `SET(NULL)` is legal | yes |
| `APPEND(elems)` | `RPUSH` | Append at the tail, then clamp to `max_capacity` | **no** |
| `REMOVE(values)` | `LREM key 0 v` | Remove **all** elements equal to any of `values`. This keeps the semantics of today's `ARRAY_REMOVE` (`typeutil.removeArrayRow`) | yes |
| `TRIM(start, stop)` | `LTRIM` | Keep elements `[start, stop]`, both inclusive and 0-based. Negative indexes count from the end; out-of-range indexes are clamped; `start > stop` or `start >= len` gives `[]` | window form (`TRIM(-N,-1)`, `TRIM(0,N-1)`): yes; pop form (`TRIM(1,-1)`, `TRIM(0,-2)`): **no** |
| `PATH_REPLACE([i], v)` | `LSET` | Replace element `i` (negative counts from the end); out of range ⇒ no-op | yes |

On `[a, b, c, d, e]`: `TRIM(-3,-1)` → `[c, d, e]` (keep the most recent N),
`TRIM(1,-1)` → `[b, c, d, e]` (pop front), `TRIM(0,-2)` → `[a, b, c, d]` (pop
back).

**Positional ops under concurrency.** PATH_REPLACE `[i]` and TRIM resolve
against whatever the list is at their ts. A concurrent APPEND or TRIM can
shift which element `[i]` designates. The result is still deterministic and
identical on every replica, which is the same contract Redis gives. This is
documented rather than prevented.

**`max_capacity` clamps after every op (rev 2).** APPEND that overflows keeps
the most recent `max_capacity` elements and drops the oldest, giving
sliding-window behaviour. This differs from today's
`upsert(partial_update=True)`, which rejects an overflowing APPEND; that path
is unchanged. The clamp must run after each op, in op order, because clamping
only once at the end gives a different answer:

```
max_capacity = 3, base [a, b, c]
APPEND(d), then TRIM(0, 1)
clamp after each op:  [b, c, d] → [b, c]
clamp at the end:     [a, b, c, d] → [a, b]        (wrong)
```

**Folding: which forms are base-free (rev 2 replaces the `(R, S)` rule).**
Every Milvus ARRAY has a `max_capacity`, so the clamp can always fire. Once
it can fire, the result of REMOVE and TRIM depends on the current length,
which is not known during base-free folding. Only two foldings stay exact:

1. **SET absorbs everything before it.**
2. **Consecutive APPENDs concatenate.** Keeping the last `cap` elements after
   each append is the same as keeping the last `cap` after appending the
   concatenation.

Every other sequence (REMOVE, TRIM, PATH_REPLACE, and their interleavings
with APPEND) is kept as an ordered op list. It is materialized against the
base at read, at column folding and at overlay pruning. Pruning reads the
base off the per-patch apply path, as revision 1 already allowed for
POP_FRONT.

**Equality and NULL rules.** These are defined once and executed by a single
implementation shared by proxy (Go), segcore (C++) and folding, or verified
against shared golden vectors:

- NaN is never equal to anything. This matches today's `ARRAY_REMOVE`.
- `+0.0 == -0.0`.
- Strings compare bytewise.
- A NULL element equals a NULL element, so `REMOVE([NULL])` removes all null
  elements.
- Nested arrays use deep equality.
- On a NULL array, APPEND treats the base as `[]`; REMOVE, TRIM and
  PATH_REPLACE are no-ops.

**Native-list arrays** (element-nullable / nested, #54513). Ops apply to the
outer list, and an appended element may be NULL. Overlay values are stored as
Arrow lists with element validity, never as protobuf blobs. Today's proxy
rejects APPEND/REMOVE on these fields (`task_upsert_partial_op.go`); the
mutable path lifts that.

**ArrayOfStruct: lockstep rule.** A struct array row is a list of struct
elements stored as one array column per sub-field, aligned by element index.

- The version chain is kept **per struct field**, not per sub-field. Every op
  applies to all sub-fields at the same ts.
- A read of one sub-field materializes the struct chain and projects that
  sub-field.
- Column folding replaces all sub-field columns with the same `fold_ts` in
  one manifest transaction.
- `max_capacity` clamps at the same position for every sub-field.
- Ops:
  - APPEND struct elements;
  - TRIM;
  - `REMOVE(where child == v)`: remove all elements whose scalar sub-field
    `child` equals `v`, using the equality rules above (vector sub-fields
    cannot be the key);
  - PATH_REPLACE `[i]` (whole element) and `[i].child` (one component).
- Sub-field evolution (#54514): an older APPEND operand that lacks a newly
  added sub-field materializes NULL or the default value; dropping a
  sub-field discards its component from chains.
- Invariant tested in Phase 1: all sub-field lengths of a row are always
  equal.

### JSON and the dynamic field (rev 2)

- `SET(doc)` replaces the whole document.
- `PATH_REPLACE(path, v)` uses today's grammar (`["key"][i]…`, depth ≤ 64).
  It becomes a blind op: a missing intermediate key, a wrong parent type or
  an out-of-range index ⇒ no-op. A missing final key is created, as today.
- `$meta` follows the same rules. Its `SET` keeps today's partial-update
  meaning, a merge of top-level keys.
- Dirty rows bypass JSON stats / shredding and are evaluated on the raw JSON.
  This also applies when JSON shredding serves group-by keys (#53412).

### VARCHAR, TEXT, GEOMETRY (rev 2)

- SET only.
- **TEXT**:
  - A patch value larger than the LOB inline threshold (64 KiB) is written as
    a LOB file by the flusher, and the deltalog stores the LOB reference.
  - The Vortex outlet patches the encoded reference column consistently with
    `bulk_subscript_text_impl`'s LOB resolution.
  - Column folding uses the existing LOB REUSE/REWRITE compaction logic.
- **GEOMETRY**: patch values are always stored as WKB.

## Write Path

Update messages flow through the existing DML channel as a new message type
(registered via `codegen/reflect_info.json` like other DML types), encoded
as `(pk, ts, {col: (op, operand)})` — structurally "a delete with a
payload". The timestamp is the per-PChannel TimeTick assigned by the
StreamingNode TSO; ordering is **total per channel, not global**. The
determinism argument for replication therefore requires **PK→channel
affinity**: update messages hash to the same vchannel as the PK's inserts
and deletes (as deletes do), so all ops touching one row are totally
ordered. CDC replication and recovery replay apply to the new type
automatically (user DML messages get the replicate header and are replayed
from checkpoint in TimeTick order like deletes).

1. **L0 patch deltalogs.** Like deletes, updates first land in L0 files,
   unrouted.
2. **PK routing.** L0 compaction routes patches to per-segment, per-column
   patch deltalogs using the existing PK bloom filter / PK index machinery.
3. **Streaming consumption.** QueryNodes consuming the DML stream apply
   updates to an in-memory overlay (below) so they are visible per the
   collection's consistency level, exactly as deletes are.
4. **Growing segment flush.** When a growing segment seals, its accumulated
   patches are flushed as **patch deltalogs alongside** the insert binlogs —
   never materialized into the base. This preserves the invariant that an
   insert binlog contains rows exactly as inserted (which CDC, backup, and
   replay all rely on); the base/patch separation is uniform across growing
   and sealed segments.

### No CAS, and request-level idempotency (rev 2)

**Patch ops need no CAS.** Every op is blind and deterministic, so the only
ordering requirement is the one the WAL already provides. The StreamingNode
`partialupdate` interceptor's PK row locks (#54216) guarantee that a later
write on a PK gets a later TimeTick. Patch messages still pass through that
interceptor and publish their PKs into the version index. This lets a
concurrent read-merge-write `upsert(partial_update=True)` detect them through
its existing CAS.

**Retries are the remaining hazard.** Current state on master:

- pymilvus retries `upsert` on gRPC errors up to 75 times and ignores
  `retriable`. The Go client retries `Unavailable` / `ResourceExhausted` up
  to 6 times.
- There is no idempotency key for Upsert.
- An ambiguous commit can surface as a conflict (`TRANSACTION_EXPIRED` is
  mapped to the CAS retry signal) or as a generic error.

A retried APPEND or pop-form TRIM therefore applies twice (#54943).
Revision 2 adds:

1. **`request_id`.** The SDK generates one per logical `update()` call and
   reuses it on every retry. Users may also supply their own.
2. **Dedup at the WAL owner.** The StreamingNode keeps a TTL-bounded table
   keyed by `(collection, request_id, vchannel)`. The dedup check and the
   append run atomically under the same row-lock / vchannel serialization, so
   two in-flight copies of one request cannot both commit. The `request_id`
   is persisted in the WAL message, so the table is rebuilt from the WAL
   after failover, and the TTL is bounded by WAL retention.
3. **Multi-vchannel batches.** Each vchannel dedups independently. Retrying a
   partially committed batch re-applies only the vchannels that did not
   commit. An ambiguous outcome answers "already applied" on retry.
4. **SDK retry policy.** `update()` is auto-retried only with a
   `request_id`, and an SDK without `request_id` support cannot call
   `update()`. CDC carries `request_id` to the target cluster, which dedups
   the same way.

### Growing segments use the same overlay

A patch to a row in a growing segment goes through the **same overlay**, not
an in-place mutation of the growing column. In-place is tempting (the
growing column is already mutable memory) but wrong on three counts:

- **MVCC.** Growing segments are queryable; overwriting destroys the old
  value that a reader at `ts < patch_ts` must still see. Keeping versions
  *is* the overlay.
- **No-read write path.** In-place INCR is a read-modify-write; the design
  forbids reads on the write path even when the data is local.
- **Uniform seal/recovery.** With one overlay, seal is trivial (base →
  insert binlog, overlay → patch deltalog) and recovery is one replay path.
  In-place would fork both.

Growing segments actually have the *cheapest* PK→offset resolution (the
insert append position is the offset, and the growing segment already keeps
a PK→offset map). WAL per-channel total order + PK→channel affinity
guarantees `insert(ts)` precedes `patch(ts')` in the stream, so the row
exists before its patch applies; a patch to a not-yet-inserted PK is
silently ignored, consistent with the sealed case.

## Patch Deltalog Format

Patches persist exactly the way delete deltalogs do: **each flush cycle
emits one patch deltalog per (segment, mutable column)**, so a column
accumulates **multiple** deltalog files on object storage over time. Each
file contains sparse `(pk, ts, op, operand)` entries, is internally
ts-ordered, and covers a ts range. Columns are never combined into one
file, because:

- Column folding rewrites only the affected column file, and its deltalog
  GC must not be entangled with other columns'.
- Different mutable columns can have update rates that differ by orders of
  magnitude; combining them couples their compaction schedules.

Patch compaction merges many small deltalogs into fewer folded ones —
**still deltalog format**. The only point where patch content returns to
native columnar format is column folding, which materializes deltalogs into
a new column binlog on disk.

Entries are keyed by PK, not offset: L0 files predate routing, and after a
segment is compacted away its patches must replay to the successor segment,
where old offsets are meaningless. PK→offset resolution happens once, at
apply time. Scalar operands are fixed-width; ARRAY operands use the same
length-prefixed encoding as existing array binlogs.

## In-Memory Overlay

Per segment, per mutable column: `hashmap<offset, version_chain>`,
**partitioned by chunk** (or offset-sorted) so the expression fix-up pass
can enumerate exactly the patched offsets of one chunk in O(patches in
chunk), and an empty chunk costs one lookup. A per-chunk patched-count
makes the "no patches here" fast path a single branch.

- Fixed-width values (all scalar mutable types) are stored inline in chain
  nodes. ARRAY values — the only variable-length type — go into a
  per-column arena referenced by the chain nodes.
- Version-chain nodes carry `(ts, op, operand)`; for INCR runs a node may
  carry a folded cumulative delta. The chain exists only for MVCC: readers
  at older timestamps must see older values. A background pruner folds
  versions below the **safe timestamp** into a single floor node. The safe
  timestamp is the lower bound of any timestamp a current *or future*
  reader may use — the minimum over in-flight queries' timestamps and the
  consistency staleness window; a query below it is impossible by
  construction, so history below it is dead and purgeable (the same
  contract that lets MVCC databases vacuum). The pruner is also what
  recycles arena blocks. For base-free folds (SET absorbing earlier ops,
  concatenated APPENDs) the floor node is relative and needs no base; for
  an array row holding REMOVE / TRIM / PATH_REPLACE ops the pruner reads the
  (locally available) base to collapse the chain into a floor value (allowed
  here because pruning is off the per-patch apply path — see "Array
  operations"). In
  steady state each updated row holds one live node — overlay memory is
  bounded by the number of *distinct* updated rows, not by update
  frequency. A hot counter updated 1000×/s occupies one slot.
- Overlay memory is accounted against the query/data node memory quota and
  participates in backpressure.
- **The overlay is heap-resident only — it cannot be file-backed mmap'd**
  like base columns: it mutates continuously (streaming applies, chain
  pruning, arena recycling) and has no local persistent form (its truth is
  deltalogs + WAL). Everything mmap buys — eviction, zero deserialization,
  read-only sharing — presupposes an immutable file. Consequence: folding
  is the only mechanism that converts un-mmapable overlay bytes back into
  mmap-able immutable column files, so folding triggers should consider
  overlay memory pressure in addition to patch ratio. (A possible future
  mitigation — snapshotting cold overlay partitions into immutable local
  files with a small mutable tail, i.e. a node-local memtable/L0 — is
  deliberately out of scope for v1.)

### Duplicate delivery (rev 2)

The same patch can reach a segment's overlay more than once:

- the streaming forward and the delta buffer (`ListAfter`) overlap with L0
  load;
- during growing→sealed handoff;
- during recovery replay from a checkpoint.

Deletes tolerate this because `DeletedRecord` stores `(ts, offset)` in a
sorted set (`segcore/DeletedRecord.h`, `accessor.insert(entry).second`), so a
duplicate is never inserted. Patches use the same rule. A chain node is
identified by `(offset, column, ts, seq)`, where `seq` is the entry's
position within its WAL message, and inserting an existing identity is a
no-op. Every delivery path goes through this check. This matters more for
patches than for deletes, because APPEND and pop-form TRIM are not
idempotent.

### Building the overlay from deltalogs

At segment load, the overlay is constructed by **replay, not by
merge-on-read across files**:

1. List the column's deltalogs from segment meta; skip files entirely below
   `fold_ts`, and skip entries `ts ≤ fold_ts` inside a file that spans the
   watermark.
2. Replay the remaining entries in ts order (files are ts-range-ordered and
   internally ts-ordered). Each entry costs one PK→offset lookup against the
   segment's PK index, then an append/eager-fold at the tail of that
   offset's version chain — replay order keeps every chain sorted for free,
   and same-offset entries fold immediately by the op rules (INCR sums, SET
   absorbs, array-op algebra). Entries below the safe timestamp go straight
   into the floor node.
3. Resume WAL consumption from the checkpoint; streaming entries apply the
   same way.

Two contrasts worth stating explicitly:

- **Eager merge, not LSM levels.** A query never sees a stack of deltalog
  files to be merged at read time; all multi-file merging happened at
  build/apply time, so the read path faces exactly one overlay.
- **The merge target is not a rebuilt columnar array.** The base column
  stays as immutable, mmap-shared chunks; the overlay hangs beside it as a
  sparse structure. Rebuilding the column in memory on every load would
  double memory and break mmap sharing — patches return to columnar format
  only at column folding, on disk, in the background.

## Overlay Concurrency and Deltalog Lifecycle

The overlay is mutated by streaming apply, pruning, and fold-drop while many
queries read it concurrently. The model is **single-writer / multi-reader
with epoch-based reclamation** — the same shape Vespa uses for in-memory
mutable attributes serving concurrent queries.

**One writer per (segment, replica).** A segment's patch stream is consumed
serially by one channel consumer, so exactly one thread ever mutates a given
overlay. Each replica has its own node-local overlay with its own single
writer; there is no cross-node coordination.

**Version chain = immutable, prepend-only, published by an atomic pointer.**

- Each row's chain is a newest-first singly linked list; a node is immutable
  once published.
- *Apply* allocates `Node{ts, op, operand, next = current_head}` and does a
  release-store of the head into the offset's slot. Because there is a
  single writer, **no CAS is needed** — one atomic store; readers never see
  a torn node. First patch to a clean chunk allocates and publishes the
  chunk overlay and bumps its patched-count with a release store.
- *Read* acquire-loads the per-chunk overlay pointer; null (patched-count 0)
  takes the clean fast path. Otherwise acquire-load the head and walk to the
  first node with `ts ≤ query_ts`. Readers never lock, allocate, or block.

**Pruning runs on the writer thread.** Folding `≤ safe_ts` nodes into a
floor node is done lazily by the same single apply thread (when it touches
an offset, or on a periodic chunk sweep). Keeping *all* chain mutation on
one thread makes correctness trivial — there is never a writer-writer race.

**Reclamation is epoch-based (EBR/RCU).** A node unlinked by pruning or
fold-drop may still be under a reader's walk. Readers publish a per-query
epoch; retired nodes are freed only once every epoch that could reach them
has advanced. Reuse Milvus's existing generation/hazard utility if present;
otherwise a global epoch counter plus per-query snapshot suffices.

**Fold is a reader of the overlay.** The folder computes `col.bin'` from
base + overlay at `fold_ts` without mutating the overlay. The one ordering
constraint: while a fold is in flight the writer must not prune the
`≤ fold_ts` region (it keeps appending `> fold_ts` freely); after the
manifest swap commits, the writer drops the `≤ fold_ts` entries (freed via
EBR).

**Invariants (the correctness contract to test).** The model reduces to five
invariants; a Phase-1 TSan/stress harness must exercise all five:

- **I1 — single mutator.** Exactly one thread mutates a given overlay; apply,
  prune, and fold-drop all run on that writer thread. No writer-writer race
  exists by construction.
- **I2 — immutable nodes.** A version-chain node is never modified after
  publication; the only write to a live slot is an atomic release-store of a
  new head pointer.
- **I3 — publication safety.** A reader's acquire-load of a head is paired
  with the writer's release-store, so it observes a fully-constructed node
  (no torn reads).
- **I4 — no use-after-free.** A node unlinked by prune/fold-drop is freed
  only after every epoch that could still reference it has advanced (EBR).
- **I5 — fold/prune ordering.** While a fold is in flight the writer does not
  prune `≤ fold_ts`; it drops that region only after the manifest swap
  commits.

Memory ordering is release/acquire on the per-offset head pointer and on the
per-chunk overlay pointer + patched-count; everything else is single-writer
and needs no atomics. This is the same generation-guard shape Vespa runs in
production for mutable attributes, but it is **designed here, not yet
stress-tested** — see "Validation Status".

### Deltalog lifecycle — immutable, never edited in place

Deltalogs are immutable, exactly like delete deltalogs. Nothing is ever
modified in place; the lifecycle is append → flush → compact → materialize →
GC:

- **Append.** The apply thread appends each patch to an in-memory deltalog
  buffer *in addition to* the overlay — the same way an insert feeds both
  the growing column and the insert-binlog buffer. Single writer, append
  only.
- **Flush.** At a flush boundary the writer **atomically rotates the
  buffer**: it hands the sealed buffer to the flusher and starts a fresh
  one. The flusher writes the sealed buffer to object storage as one
  immutable deltalog file while the writer keeps appending to the new
  buffer — no lock on the hot append path.
- **Compact / materialize / GC.** Patch compaction reads N immutable
  deltalogs and writes M<N new immutable ones (op-folded), then GCs the N;
  column folding materializes them into a new base column file. A later SET
  that "overrides" an earlier one is just a higher-`ts` entry resolved at
  fold time — never an in-place edit.

This immutability is what makes recovery (replay the files) and CDC (ship
the files) trivial, and it is why the whole design can reuse the delete
path's file lifecycle unchanged.

## Read Path

- **Where the overlay is applied (rev 2).** Mutable columns are Vortex
  local-format columns. On master every sealed read of a Vortex column goes
  through `VortexColumn` (`internal/core/src/mmap/VortexColumn.cpp`), which
  implements `ChunkedColumnInterface`:
  - expression evaluation uses Scan cursors (`SegmentExpr::EnsureDataScanCursor`
    in `exec/expression/Expr.h`, and `SegmentChunkReader` routes Vortex
    columns to Scan);
  - offset reads use Take;
  - `bulk_subscript` and group-by use Take.

  The overlay is therefore applied at exactly two decode outlets:
  - **Iterator / Scan.** Each `ScanBatch` covers `[row_id_start, +size)`.
    Binary-search the query snapshot for dirty offsets in that range. If
    there are none, the batch is returned untouched.
    - For view targets (`StringView`, `Json`, `ArrayView`, `ArrayValueView`),
      copy the view array and repoint the dirty slots to overlay arena
      values. No payload bytes are copied.
    - For primitive targets, copy the value array and overwrite the dirty
      slots.
    - Validity is rewritten only when a patch flips nullness.
    - The batch `owner` holds the original owner, the new arrays and the
      snapshot.

    Types without a direct scan (ARRAY, WKT geometry) go through
    `MaterializeChunk`, which uses the same helper.
  - **Take.** The Vortex Take result is already owned, so dirty positions
    are replaced in place. If every requested offset is dirty, decoding can
    be skipped.
- **Legacy chunk-view APIs** (`DataOfChunk`, `Span`, `StringViews`,
  `*ViewsByOffsets`) throw for a column that has an overlay, so a read can
  never silently return stale data. Local format phase 2 removes them.
- **One snapshot per query.** Scan, Take, outputs and fix-up all use the same
  overlay snapshot chosen at the query timestamp. It is passed through the
  query context.
- **Growing segments** apply the same overlay at the growing data-access
  layer. Growing segments do not use Vortex.
- **Output fields** are materialized through the same outlets after
  search/query.
- The vector search path is unaware of mutable scalar columns. Phase 2 covers
  mutable vector fields.
- **Consistency.** Visibility is governed by the same timestamp mechanism as
  deletes. Strong and Session consistency read the caller's own writes;
  Bounded and Eventually do not, exactly as for insert and delete.

### Expression evaluation over patched columns

Mutable columns integrate at the **leaf predicate** level of the segcore
PhyExpr tree; AND/OR/NOT combination over bitsets is unchanged.

For each predicate node touching a mutable column, per chunk:

1. Evaluate the predicate vectorized over the **base** column data as today
   (SIMD path untouched), producing the chunk bitset.
2. For the (sparse) set of offsets in this chunk present in the overlay,
   re-evaluate the predicate row-wise using the patched value at the query
   timestamp, and **fix up** the corresponding bits.
3. Bitset combination up the tree proceeds normally, unaware of patches.

Cost is the existing vectorized scan plus O(#patched rows in chunk) scalar
re-evaluations; chunks with an empty overlay pay one branch. Multi-column
predicates (e.g. `mutable_a + b > 10`) are fixed up the same way — the
row-wise re-evaluation point-reads the other columns at that offset. Array
predicates (`array_contains` etc.) evaluate row-wise against the arena
value. Existing conjunct reordering should schedule mutable-column
predicates after indexed/cheap predicates so they run on surviving rows.

**Dense-overlay chunks — measured, and the fix-up path is optimal.** Under
wide-update workloads (the `offline_time` scenario in Motivation) a large
fraction of a chunk may be patched between folds, and re-evaluating those
rows one at a time is genuinely expensive. A standalone microbenchmark
reproducing the segcore kernel (32768-row int64 chunk, `x > c` predicate,
overlay lookup + version-chain materialize per dirty row; Apple M-series,
clang -O3 -march=native) quantifies it:

read cost as a multiple of the clean base scan (base = 4.0 µs):

| patched fraction | fix-up SET | fix-up INCR (×3 chain) |
|---|---|---|
| 0% (clean) | 1.00× | 1.00× |
| 5% | 2.3× | 2.9× |
| 10% | 3.6× | 4.8× |
| 20% | 5.2× | 7.3× |
| 50% | 9.3× | 14.1× |

Two conclusions:

- **Clean chunks cost exactly the base scan** (1.00×). The per-chunk
  patched-count fast path is free; an empty overlay is one branch. Every
  non-updated column, and the whole vector-search path, always stays at 1×.
- **A dense chunk is genuinely expensive** — up to 9–14× at 50% patched.
  The concern is real; INCR is steeper because each dirty row walks a
  version chain.

An earlier draft proposed a *materialize-then-scan* fallback for dense
chunks (gather current values into a scratch buffer, run the vectorized
kernel). The benchmark **refutes it**: materializing a 32768-row chunk
requires a 256KB memcpy costing ~3.1 µs — ~78% of a full scan — and since
both paths pay the identical per-row materialize, materialize-then-scan is
strictly memcpy-worse at *every* density, including 50%. Fix-up is already
optimal: it does exactly one SIMD base scan plus O(dirty) scalar work; its
only waste is scanning dirty rows twice, and half a wasted SIMD scan
(~2 µs) is cheaper than the memcpy a scratch buffer costs.

**Therefore there is no read-path fallback; the lever is folding.** A fold
materializes the column to a fresh native file, so **after folding a
mutable column reads at exactly 1× — identical to any immutable column**;
folding erases the penalty entirely. This is why per-query materialization
loses but folding wins: folding pays the materialize *once*, in the
background, persisted, and shared by every replica, whereas a read-path
fallback would re-pay it per query per replica.

What a query actually experiences is a sawtooth: the hot column's read cost
ramps with density between folds and snaps back to 1× on each fold. The
fold threshold is therefore the read-cost knob:

| fold at | pre-fold avg (SET / INCR) | pre-fold peak | post-fold |
|---|---|---|---|
| 5% | 1.5× / 1.7× | 2.3× / 3.0× | 1.0× |
| 10% | 2.4× / 2.9× | 3.4× / 4.9× | 1.0× |
| 20% | 3.4× / 5.0× | 5.1× / 7.3× | 1.0× |

For a heartbeat column like `offline_time`, folding aggressively (5–10%)
keeps its steady-state read near 1.5–2.9× and touches only that one narrow
column; folding is a background rewrite of an 8-byte-wide file, no vectors,
no indexes.

Implementation decisions (fixed here so the implementation doesn't
re-litigate them):

- The row-wise fix-up leg reuses the existing offset-based evaluation path
  (`has_offset_input_` / `OffsetVector` in `exec/expression/Expr.h`, built
  for iterative filters) rather than adding per-expr fix-up logic.
- (rev 2) Overlay values are served at the two Vortex decode outlets (Scan
  batches and Take results, see "Read Path") and at the growing data-access
  layer, not inside individual expression classes — one integration point
  per backend covers every expression type and output-field
  materialization. Unpatched columns must pay effectively zero overhead on
  this hot path (a null-check branch).
- Nullable interaction: SET carries validity (SET NULL is legal; a SET on a
  previously-NULL row flips its validity bit), so the validity bitmap needs
  overlay treatment too. INCR on NULL is a no-op (see Open Questions).

### Interaction with chunk skip stats and the expression cache

Two filter-acceleration mechanisms consult column values and MUST be
patch-aware in **v1** — numeric columns are exactly what they serve:

- **SkipIndex** (`index/SkipIndex.h`, per-chunk min/max used by
  `CanSkipUnaryRange`): a patched value may fall outside the chunk's base
  min/max, so a "skipped" chunk can still contain matching patched rows.
  Skipping remains valid for unpatched rows (their base values genuinely
  cannot match), so the rule is simply that the **overlay fix-up pass must
  run unconditionally, independent of chunk-skip decisions** — a skipped
  chunk's bitset starts all-zero and fix-up overwrites the patched offsets.
  No stats need updating.
- **Expression result cache** (`exec/expression/ExprCacheHelper.h`): a
  cached bitset for an expression referencing a mutable column is
  invalidated by any patch to that column. The cache key must incorporate a
  per-(segment, column) overlay version, or caching is disabled for such
  expressions in v1 (recommended: disable, revisit with versioned keys).
- (rev 2) **Vortex zone maps and scan pushdown** (loaded but not wired into
  production queries on master): when enabled, a cell containing dirty rows
  must not be pruned or answered by pushdown; assert this. **JSON stats /
  shredding** are bypassed for dirty rows.

## Folding and Compaction

Two levels keep patch volume bounded:

1. **Patch compaction.** Multiple deltalogs for a segment are merged and
   folded per PK below the safe timestamp. Output is still deltalog format.
   Under revision 2's rules (see "Array operations"):
   - SET absorbs every op before it;
   - consecutive APPENDs concatenate;
   - everything else is kept as an ordered op list. That includes REMOVE,
     TRIM, PATH_REPLACE and their interleavings with APPEND, which depend on
     the current length because of the `max_capacity` clamp.
2. **Column folding.** When the patch ratio for (segment, column) exceeds a
   threshold, rewrite that single column file with patches applied
   (materializing INCRs against the base — the one place deltas become
   absolute values) and swap it in atomically via the segment manifest.
   Vector files and index files are untouched.

Regular segment merge compaction folds all outstanding patches into the new
base as a side effect, so a successor segment starts with an empty patch
set.

**Folding watermark.** A fold materializes ops up to some timestamp
`fold_ts`; the manifest records `fold_ts` per (segment, column) alongside
the new column file. Segment load replays only patch entries with
`ts > fold_ts` — without the watermark, recovery would double-apply folded
patches (harmless for SET, **wrong for INCR/APPEND**). Old deltalogs are
GC-eligible only after the manifest swap commits and no reader pins the old
version; the swap itself rides the existing segment-reopen atomic
read-update COW mechanism (see `20260627-segment-reopen-atomic-read-update-cow`).

**`fold_ts` is bounded by the L0-distributed watermark (rev 2).** Deletes are
order-insensitive: a late delete compares against the row's insert ts and is
still correct. A patch is not order-insensitive. Consider this sequence:

```
ts=70  SET(price=10)   still in an undistributed L0 segment
ts=80  SET(price=20)   already routed to the segment
fold at S=100          base := 20
L0 routes ts=70 later  applied on top of the folded base → 10   (wrong; should stay 20)
```

Dropping late entries with `ts ≤ fold_ts` is not enough on its own. If the
segment had no ts=80 patch, the fold would leave base=5 and the late ts=70
SET would then be dropped: a lost update. The rule is therefore:

> `fold_ts = min(S, L0-distributed watermark of the channel)`. This is the
> earliest start ts among the channel's L0 segments that L0 compaction has
> not yet processed.

Every patch with `ts ≤ fold_ts` is then guaranteed to be in the segment's own
deltalogs and included in the fold. Every later L0 delivery has
`ts > fold_ts` and applies normally. The existing channel-level mutual
exclusion between L0 compaction and mix / sort / clustering / schema-bump
compaction (`datacoord/compaction_inspector.go`) keeps the watermark stable
while a fold is planned.

**Column-group replace (rev 2).** A fold rewrites only the mutable column
group. It commits as a `SegmentManifestDelta` replace operation, the same
primitive backfill uses (#53491), and records `fold_ts` per (segment, column
group) in manifest metadata. The manifest's delta logs model deletes only, so
patch deltalogs need their own manifest entry type. For ArrayOfStruct, all
sub-field columns are replaced in one transaction with the same `fold_ts`.
The Vortex writer always re-encodes its Arrow input, which is another reason
mutable fields get their own group: a fold never re-encodes unrelated
columns.

**Local-cache consequence of a fold.** Folding runs on a DataNode and
writes the new column binlog to object storage; it does not touch any
QueryNode cache directly. Each QueryNode holding the segment then does a
**column-level** cache update: fetch the new `col.bin'`, mmap it, and drop
the old `col.bin` plus the folded deltalogs and their overlay entries. This
is *not* a segment reload — vector files, index files, and every other
column stay cached and mmap'd. The re-fetched data is one narrow column
(e.g. 8 bytes × rows ≈ single-digit MB per segment), versus the GBs a
full-segment compaction moves (vectors + index rebuild). That
~two-orders-of-magnitude difference is the point of the feature; but the
fetch still fans out to every replica, so the fold trigger must account for
it (another reason not to fold every few seconds). A future optimization
avoids the fetch entirely: a QueryNode already holds base + overlay = the
identical data, so it can materialize `col.bin'` locally under the same
`fold_ts` (bit-identical) instead of re-downloading — deferred with the
rest of the node-local-materialization idea (out of scope for v1).

**Delta cleanup happens only through compaction — nothing else ever deletes
a deltalog.** Patch compaction shrinks the file set; column folding is the
terminal cleaner (materialize, swap, then GC); segment merge compaction
folds everything as a side effect.

**Folding is memory-driven; read-cost is a secondary QoS knob.** The
per-column patch ratio is a local read-cost signal, but the hard
operational constraint is the node overlay memory budget, so memory is the
primary trigger:

- **Soft watermark** (e.g. 70% of the overlay budget): a background folder
  selects victims by benefit/cost score
  `reclaimable_bytes / rewrite_cost`, where `reclaimable_bytes` is the
  (segment, column) overlay below `safe_ts` (actually foldable) and
  `rewrite_cost` is that column's binlog size plus the per-replica re-fetch
  fan-out. The score naturally prefers **large-overlay, narrow-base**
  columns — exactly the hot wide-update columns like `offline_time`. Fold
  until back under the soft watermark.
- **Hard watermark** (e.g. 90%): the WAL consumer is backpressured (apply
  throttled) until folding catches up. This is the honest steady state —
  folding is a continuous background process paced by memory, and if the
  write rate exceeds fold throughput the write path throttles rather than
  the node OOMing.
- **Optional read-QoS floor**: a per-collection "max read amplification"
  knob folds a hot column at a ratio threshold even when memory is fine
  (using the sawtooth numbers in "Dense-overlay chunks" to pick the ratio).

Load-replay time and deltalog count are additional inputs (a column with
many small deltalogs is cheap to fold and worth folding early).

**Wide-update pacing.** Workloads that touch most rows of most segments
(heartbeat-style `offline_time` updates) push all segments across the
folding threshold at roughly the same time. Per-segment folding cost is
small — one narrow column rewrite, no vectors, no indexes — but the
scheduler must still stagger folds to avoid manifest-update and IO
bursts. The fold ratio is also the read-cost knob: because a dense chunk
costs ~9–14× a clean scan (see "Dense-overlay chunks"), folding at ~20%
keeps steady-state read overhead on the hot column near 5×. The same
reasoning carries into Phase 2: **very-high-churn columns should stay
unindexed even once indexes are available** — their patched fraction sits
high between folds, so result correction degrades toward brute force
anyway while forcing continuous index-rebuild churn; plain brute-force
fix-up plus an aggressive fold cadence is the right plan for them.

## Path to Indexed Mutable Columns (Phase 2)

The patch mechanism composes with indexes without modifying any index
structure, using **result correction**. The overlay already yields, per
(segment, column), a monotonically growing `patched_bitset` of rows whose
base value is stale. For any scalar index (all of them answer boolean
predicates with a bitset):

```
result = (index_bitset AND NOT patched_bitset) OR brute_force(patched_rows)
```

The index answers for all unpatched rows (their base values are still
correct); patched rows are masked out and re-evaluated row-wise against the
overlay — the same pattern as the existing
`index_result AND NOT delete_bitmask`. Cost grows with patch count; past a
threshold the planner skips the index and falls back to full brute force.

Two properties of the correction machinery, fixed here because they are
easy to get wrong:

- **The bitset is monotone ("ever patched"), not ts-versioned.** A
  ts-versioned bitmap would be an order of magnitude more expensive to
  maintain and snapshot. Monotonicity is correct only in combination with
  the next rule.
- **Base fallback in the correction leg.** For a masked row whose version
  chain has no entry `≤ query_ts` (patched only after the query's
  timestamp), the re-evaluation MUST fall back to the base value — giving
  exactly the answer the index would have given, closing the MVCC loop. In
  the v1 brute-force fix-up this fallback is implicit (no chain hit → the
  base-scan bit is left untouched); in index correction the row was never
  scanned, so the base point-read must be explicit. The materializer
  already point-reads base for INCR chains, so the capability exists — the
  semantics just must be stated. The overlay itself needs no redesign for
  Phase 2; the additions are the materialized bitset (snapshot-readable,
  maintained at apply time, surviving folds until index rebuild) and this
  fallback rule.

**Phase 2 coverage (rev 2).** Result correction needs only a row-wise
evaluation of the leaf predicate on the dirty rows. That evaluation reuses
`ProcessDataByOffsets` and reads through the already-patched Take outlet.
Because of this it applies to **every scalar index type**:

- STL_SORT, BITMAP, HYBRID, INVERTED (including array inverted), TRIE, NGRAM,
  FMINDEX and RTREE;
- JSON path index and JSON stats;
- the bit-sliced index (#54373).

No index structure is modified. Integration points are `ProcessIndexChunks`
(`exec/expression/Expr.h`) plus any leaf that pins an index directly
(roughly 18 `PinIndex` sites, 14 JSON-stats sites and NGRAM). That list must
be verified before Phase 2 is sized.

**Text match / phrase match.** These have no row-wise raw-data evaluator, and
a hand-written token or slop matcher would drift from tantivy semantics.
Mutable text fields therefore use **base + tombstone + a tantivy delta**:

- the delta is built by the same writer as the growing text index
  (`TantivyGrowingTextIndex`);
- it uses the same analyzer configuration, and for multi-analyzer fields the
  row's *current* selector value;
- it is queried by the same text / phrase match code;
- each delta document carries `(offset, ts)`, and a delta hit is kept only if
  it is the version the query snapshot selects;
- not-yet-committed delta entries are covered by a synchronous commit before
  the query, or by a transient in-memory index queried with the same code.

Matching is boolean (no scoring), so `(base AND NOT dirty) ∪ delta` equals
indexing the new documents in one index. This is the same equivalence that
growing and sealed text indexes already rely on.

**Vectors, sparse vectors, EmbList.** Search masks dirty rows out of the base
index through the search bitset and brute-forces the dirty rows' new values.
For EmbList it computes MaxSim. The results are merged into the top-K. This
covers range search, search iterators, group-by search and every leg of
hybrid search. Output fields are patched at the vector raw-data outlet.
Vector fields are not Vortex local-format, so this is a separate integration
point. Folding a vector column rewrites it and rebuilds its index, so its
fold threshold is tuned separately.

**Function fields.** For BM25 and embedding functions, the proxy recomputes
the output from the new inputs carried in the request and emits it as a SET.
If the inputs are incomplete, the request falls back to the read-merge-write
path. BM25 corpus statistics (df, avgdl) are approximate until folding.
Exact IDF maintenance would need the old document's tokens, which is the
revision 1 conclusion. Phase 2 must match today's delete behaviour for these
statistics; whether deletes decrement them is still to be verified.

### Fold / rebuild atomicity

Column folding rewrites the base column file; the old index still reflects
the old base. The `patched_bitset` therefore MUST NOT be reset at fold time.
The segment continues serving with old-index + correction until the new
index is built, then the manifest swaps the new column file, new index, and
cleared bitset **atomically**. Resetting the bitset before the index rebuild
completes would silently return stale index results.

### Alternative considered: updating indexes in place / at load — rejected

Instead of result correction, patches could be applied to the index itself,
e.g. at segment load time. Feasibility varies by structure — BITMAP is easy
(`bitmap[v_old].remove(o); bitmap[v_new].add(o)`, old value available from
base/overlay), tantivy INVERTED is medium (delete-by-offset + add on a
local writable copy), STL_SORT is a full rebuild (sorted array) — which
already reintroduces the per-index mutation code that result correction
exists to avoid. Three further reasons this loses as the primary mechanism:

1. Patches keep streaming in after load, so query-time correction is needed
   anyway — index mutation would be a second mechanism, not a replacement.
2. MVCC: an index holding latest values still needs the row-wise correction
   window for readers whose timestamp predates recent patches.
3. It breaks read-only mmap sharing (whole index becomes resident and
   writable), and repeats the same work per replica per load, producing
   node-local index states.

The batch form of "update the index" already exists in this design: column
folding + index rebuild — done once in the background, persisted via
manifest, shared by all replicas. When correction gets slow because patches
accumulated, the right response is to trigger folding, not to patch indexes
on the load path. One future exception worth benchmarking: per-patch
incremental maintenance of BITMAP indexes (the one cheap-and-exact case)
for low-cardinality high-frequency columns.

### Alternatives for indexed queries, surveyed

The full space of "keep indexes usable under updates" has four families;
each of the other three breaks a Milvus invariant:

1. **Mutable index structures** (OLTP style: B-tree + buffer pool + page
   WAL). Milvus indexes are read-only artifacts on object storage, shared
   by replicas and mmap-friendly flat structures (e.g. STL_SORT is a sorted
   POD array). Mutability means per-replica private writable copies or a
   shared-write storage engine — an architecture transplant.
2. **Delete + reinsert** (Elasticsearch model; Milvus's existing upsert).
   Correct by construction, but rewrites the whole row including vectors —
   this is precisely the cost the feature exists to eliminate. ES "partial
   update" is server-side GET + merge + full reindex, same family.
3. **Server-side read-modify-write** at the proxy. Violates the no-read
   write path (throughput, races), and destroys the lock-free semantics of
   INCR/APPEND/REMOVE.
4. **Patch-side index / LSM view**: base index untouched, patches get their
   own small index, queries union the two with the patched rows masked —
   this *is* result correction with an indexed correction leg. The adopted
   design is its degenerate form (row-wise overlay evaluation, free for
   scalars); the overlay text index analysis was its full form.

Industry convergence on the same shape — Delta deletion vectors, Hudi MOR,
Iceberg v2 delete files, ClickHouse lightweight deletes + background
mutations, and even InnoDB's change buffer — is the external evidence that
immutable base + delta + merge-on-read + background folding is the stable
design point for this problem.

To be precise, this is a claim about *this architecture*, not about
databases in general: systems that keep indexes as node-local mutable state
support indexed updates directly (family 1 — Postgres/InnoDB; Vespa even
does in-place attribute updates plus live HNSW mutation in the vector
domain), and LSM systems make the index itself append+merge+compact
(TiDB/Cassandra). What they give up is exactly the invariant Milvus keeps:
immutable artifacts on object storage shared read-only by cheap replicas.
Under that invariant, family 4 (base + delta + read-time correction +
background rewrite — the fifty-year-old differential-file idea, also HANA
delta/main, StarRocks PK tables, Kudu) is the payable cost.

## Recovery and Failover

Nothing new: patches are recovered exactly as delete records are — replay
the DML stream from the last flush checkpoint; flushed patch deltalogs are
part of the segment's file set and are reloaded with the segment (see
"Building the overlay from deltalogs").

## Interaction with Existing Features

- **Delete / upsert.** A delete shadows all patches for that PK at a later
  timestamp. A full-row upsert (delete + insert) starts the new row with
  fresh base values; patches on the old row version are dead and removed at
  patch compaction. The application rule is uniform: a patch applies to
  **every segment whose PK filter matches**, at that segment's local offset
  for the PK; on offsets already delete-masked the patch is invisible by
  construction (reads apply the delete mask first), so re-inserted rows are
  handled with no special casing — the old offset's patches are masked, the
  new offset accepts patches with `ts >` its insert ts.
- **CDC / backup / snapshot export.** Patch deltalogs are part of the
  segment file set and travel with it; the update message type must be
  added to CDC replication, and backup/restore and snapshot export/restore
  must include patch deltalogs — missing either is silent data loss.
- **Bulk import.** Imported segments start with empty overlays; updates apply
  only after the segment is visible.
- **Compaction.** In-flight patches routed to a segment that gets compacted
  away are replayed to the successor segment, reusing the delete path's
  existing mechanism.
- **Schema evolution.** Altering an existing column to `mutable=true` is
  cheap (segments start with empty overlays). Altering back to immutable
  requires folding all outstanding patches first. Interactions with
  add-field/drop-field need a pass during implementation.
- **Rolling upgrade.** Older query/data nodes cannot parse update messages
  or patch deltalogs. Mutable columns are gated behind a collection-level
  feature flag that can only be enabled once the whole cluster runs a
  supporting version.
- (rev 2) **`update()` vs `upsert(partial_update=True)`.** The two APIs
  coexist: `update()` is blind (patch path, no CAS, no synchronous errors,
  missing PK ignored); partial upsert stays read-merge-write with CAS,
  synchronous errors and insert-on-missing-PK. Both publish PK versions, so
  a partial upsert's CAS detects concurrent patches.
- (rev 2) **Struct sub-field evolution** (#54514): adding a sub-field
  starts it with no patch components (older APPEND operands materialize
  NULL/default); dropping a sub-field discards its components.

## Known Risks

- **Overlay memory — bounded by construction, not a blow-up risk.**
  Memory-driven folding plus the hard-watermark backpressure make it
  physically impossible for the overlay to exceed its budget: the write
  path throttles before the node OOMs. At a steady state of ~48 bytes per
  distinct dirty row (one floor node + slot), a 2 GB budget holds ~42M
  dirty rows; the `offline_time` workload (~1,700 distinct rows/s, an
  event stream over a fraction of the collection) sits in the tens of MB
  and folds every few minutes. The residual is throughput, not safety:
  fold rate must keep up with the write rate, else the write path throttles
  (the intended behavior). Effective capacity in rows = budget / ~48 B —
  size the budget accordingly. ARRAY is the one type with higher memory
  density (arena, variable-length values) and is worth its own stress test;
  fixed-width scalars are not a concern.
- **PK apply cost = the delete path's cost.** The overlay is keyed by
  offset, so each patch needs a PK→offset resolution before it applies:
  a PK-index lookup on sealed segments (the *same* index delete application
  uses), an O(1) map lookup on growing segments. On the routing side, L0
  compaction bloom-tests a patch's PK against segments exactly as deletes
  do. Both facets inherit the delete path's already-optimized machinery, so
  at ~1,700/s the cost is negligible; the only ceiling — bloom fan-out
  across all segments × query nodes at extreme rates — is identical for
  patches and deletes and is not newly introduced here. Benchmark against
  the delete-path baseline to confirm, but treat it as delete-equivalent,
  not a novel risk.
- **Load-time replay.** Segment load replays patch deltalogs into the
  overlay. Folding discipline bounds this.
- **Observability.** Patch ratio, overlay bytes, fold rate/lag, and
  backpressure events need first-class metrics, or query-performance
  regressions and write throttling become undiagnosable.
- (rev 2) **Capacity-dependent folding.** Folding REMOVE/TRIM sequences
  without the base silently diverges from read-time materialization; only
  SET-absorb and APPEND-concat may fold base-free.
- (rev 2) **Divergent implementations.** Equality, NULL and clamp rules
  evaluated by Go (proxy) and C++ (segcore, folding) must be identical, or
  replicas diverge. One shared implementation or shared golden vectors.
- (rev 2) **Per-group writer format.** Phase 1 depends on writing the
  mutable column group as Vortex while the rest of the segment keeps the
  global writer format.

## Validation Status

Stated explicitly so a reviewer can see the confidence level of each claim.

**Asserted from code/doc reading, not build-verified:**

- Reuse of the offset-input evaluation path, and the Vortex decode outlets
  (Scan / Take) as the single sealed integration point (rev 2).
- Streaming integration: the new codegen message type, PK→channel affinity,
  L0 routing, and CDC/recovery reuse (checked against the streaming-system
  docs, not a build).
- PK→offset apply cost is delete-equivalent.

**To validate during implementation (Phase-1 gates, in priority order):**

1. **Overlay concurrency (I1–I5) under TSan** with concurrent readers +
   writer + pruner + an in-flight fold. Highest-risk unproven piece; the
   model is designed but not stress-tested.
2. **Folding algebra and MVCC correctness** with implementation-level
   property tests covering SET, APPEND-concat folding, REMOVE/TRIM ordered
   materialization with the clamp-after-each-op rule, the `fold_ts`
   watermark bounded by L0 distribution, and `(offset, column, ts, seq)`
   dedup across every delivery path, end to end.
3. **Read-path performance** in the production segcore path: clean-chunk
   overhead, patched-row fix-up cost across densities, and fold thresholds.
4. **Overlay memory and PK-apply throughput** at the target write rate,
   benchmarked against the delete-path baseline; ARRAY arena memory density
   separately.

## Implementation Phases (rev 2)

Two phases, each independently shippable behind the collection-level feature
gate.

### Phase 1 — Brute force only

**Scope**: every type in the Phase 1 column of "Supported types ×
operations", with ops SET / APPEND / REMOVE / TRIM / PATH_REPLACE. Dirty rows
are evaluated row-wise. No index is read or modified for a mutable column.

- **Schema and API**
  - `mutable=true` and constraint validation;
  - the split policy gives mutable fields their own Vortex column group;
  - the `update()` API with `request_id` and `check_exists`;
  - the new WAL message type via codegen, with an op enum that already
    reserves INCR;
  - `ARRAY_TRIM` in `FieldPartialUpdateOp` (#54943).
- **Write path**
  - proxy → WAL (PK→vchannel affinity, row locks, PK version publication) →
    L0 materializer → PK-routed per-segment patch deltalogs;
  - a manifest entry type for patch deltalogs and per-column-group
    `fold_ts`;
  - growing-segment flush emits patch deltalogs;
  - TEXT LOB spill for large values;
  - recovery replay;
  - CDC with `request_id`;
  - backup and snapshot include patch deltalogs.
- **StreamingNode**: the `request_id` dedup table (atomic with append,
  rebuilt from the WAL).
- **QueryNode**
  - overlay with the I1–I5 concurrency model, `(offset, column, ts, seq)`
    dedup, safe-ts pruning and a memory quota;
  - the two Vortex outlets (Scan, Take) plus `MaterializeChunk`, and the
    growing data-access layer;
  - legacy chunk-view APIs throw for overlaid columns;
  - fix-up through the offset-input path;
  - skip-index unconditional fix-up; ExprCache disabled; Vortex zone-map and
    pushdown disabled for cells with dirty rows (asserted); JSON stats
    bypassed for dirty rows.
- **Shared materializer**: one implementation of the SET / APPEND / REMOVE /
  TRIM / PATH_REPLACE semantics, equality and NULL rules, and the
  clamp-after-each-op rule. It is used by the read path, pruning and folding,
  or all implementations are checked against shared golden vectors.
- **Background**: patch compaction (SET-absorb and APPEND-concat only);
  column folding with `fold_ts = min(S, L0-distributed watermark)` through a
  column-group replace; folding scheduling v0, driven by memory.
- **Ops**: the feature gate; metrics for patch ratio, overlay bytes, fold lag,
  dedup hits and backpressure.

**Exit criteria**

1. **Differential test.** The same random op sequence goes through the old
   path (delete + full insert) and through `update()`. Results must be
   identical for every query shape: filter, search+filter, iterative filter,
   outputs, order by, aggregation, group-by (including JSON-shredded keys),
   and query/search iterators.
2. **Interleaving tests.** Patches sit in L0, segment deltalogs and the delta
   buffer, interleaved with every compaction type, growing→sealed handoff and
   QueryNode restart.
3. **Exactly-once.** Retries, duplicate deliveries and failover never apply a
   non-idempotent op twice.
4. **Struct invariant.** Sub-field lengths of a row are always equal.
5. **Overlay TSan stress** covering I1–I5.
6. **Read-amplification curves** that set the fold thresholds.

### Phase 2 — Index-backed search

**Scope**: lifts the no-index restriction and adds mutable vector fields.

- `patched_bitset` per (segment, column), rebuilt at load.
- The correction wrapper on every index evaluation path, with base fallback.
- The planner falls back to full brute force above a patch-ratio threshold.
- The fold / rebuild atomic-swap state machine (revision 1 "Fold / rebuild
  atomicity").
- The text / phrase match tantivy delta.
- Vector / sparse / EmbList search correction and the vector raw-data outlet.
- Proxy recomputation of function outputs.
- Optional accelerations, added only when measurements demand them:
  - BITMAP and RTREE in-place updates below the safe timestamp;
  - an STL_SORT B-tree delta;
  - a tantivy delta for INVERTED.

**Exit criteria**

- Indexed queries are correct across the fold / rebuild / swap lifecycle,
  including crash points.
- Vector recall matches the delete + insert path.
- Performance-vs-patch-ratio curves are published and the thresholds are set
  from them.

### Dependencies

| Dependency | State on master (2026-11-01) | Impact |
|---|---|---|
| Per-column-group writer format, so that mutable groups are written as Vortex | `dataNode.storage.format` is global and defaults to parquet | **Blocks Phase 1** |
| Manifest entries for patch deltalogs and per-group `fold_ts` | Delta logs model deletes only; column-group replace exists (#53491) | Phase 1, milvus-storage change |
| Vortex local format | Phase 1 merged; mmap #54284 draft; zone map and pushdown not wired in production | Outlets build on the current state; any future pruning must honour the dirty-cell rule |
| Native-list arrays and struct partial update | #54795 open (lifts #54739); proxy rejects struct sub-field names and struct APPEND/REMOVE today | Gates the native-list and struct parts of Phase 1 |
| Local format phase 2 (`ColumnInterface`) | No PR | Not blocking; when it lands, the outlets cover Raw too |
| LSM PK index stack (#53596 and others) | Open | Not blocking; lowers PK→offset apply cost |

## Open Questions

- Fold triggers: the default patch-ratio and memory watermarks per column
  type. Variable-length and struct columns need their own stress tests.
- Dedup table: the TTL and how it relates to WAL retention and checkpoints.
- `check_exists`: whether it is worth shipping in Phase 1.
- Whether deletes decrement BM25 statistics today. This is the baseline for
  Phase 2 function fields.
- The verified list of index-pinning call sites outside `ProcessIndexChunks`,
  which sizes Phase 2.
- Whether Vortex decode outputs are writable, which would avoid one copy for
  primitive Scan batches.
- Whether any delegator / coordinator logic prunes whole segments using
  scalar statistics. If so, it must ignore mutable columns or use widened
  statistics.
- INCR return: overflow and NULL semantics (revision 1 recommendations
  stand). INCR also interacts with the capacity-free fixed-width folding.
