# Mutable Columns: In-Place Partial Update via Merge-on-Read Patches

- Status: Draft
- Author: Xiaofan Luan
- Date: 2026-07-09
- Related: [discussion #51115](https://github.com/milvus-io/milvus/discussions/51115)

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
| patch | One logged mutation, `(pk, ts, op, operand)`; op ∈ {SET, INCR, APPEND, POP_FRONT, REMOVE} |
| patch deltalog | Persisted file of patches for one (segment, mutable column); one emitted per flush cycle; internally ts-ordered |
| overlay | Per-(segment, mutable column) in-memory structure serving current values; chunk-partitioned `hashmap<offset, version chain>` |
| version chain | Per-row list of `(ts, op, operand)` nodes providing MVCC |
| floor node | The single chain node holding everything folded below safe_ts; still a relative op, never materialized |
| safe timestamp (safe_ts) | Lower bound of any timestamp a current or future reader may use; history below it is dead and purgeable |
| materialization | Computing an absolute value from base + ops; happens only at read time and at column folding |
| patch compaction | Background merge of deltalogs with per-PK op folding; output is still deltalog format |
| column folding | Background rewrite of one column file with patches applied, swapped in atomically via the segment manifest |
| fold_ts | Per-(segment, column) watermark recorded in the manifest; load replays only entries with `ts > fold_ts` |
| patched_bitset | Monotone per-(segment, column) bitmap of ever-patched rows; used for index result correction (Phase 3) |

## Goals

- Partial update of designated columns by primary key.
- Supported types: **numeric scalars (INT8/16/32/64, FLOAT, DOUBLE), BOOL,
  TIMESTAMPTZ, and ARRAY** (SET / APPEND / POP_FRONT / REMOVE).
- Counter support via **INCR delta ops**: an increment is just `+1` written
  to the log — no read-modify-write anywhere in the system.
- Mutable columns can appear in filter expressions, evaluated by brute-force
  scan in v1 (indexes return post-v1 via result correction).
- Reuse existing subsystems: DML channel/WAL, PK bloom-filter routing,
  L0→L1 compaction, manifest-based atomic file replacement. No new storage
  subsystem.
- MVCC semantics identical to deletes: timestamp-based visibility, same
  consistency guarantees.

## Non-Goals

- **VARCHAR and JSON scalars are immutable.** This deliberately removes the
  heaviest machinery an earlier draft required: no overlay text index, no
  tokenization on the patch path, no JSON parsing/merge-patch, and the
  BM25 / embedding-function questions become moot (function inputs are text,
  which is immutable). String-tag use cases are expressed as ARRAY of
  numeric ids instead.
- **No index support on mutable columns in v1.** This is a scheduling
  choice, not an architectural one — see "Path to Indexed Mutable Columns".
- No mutation of vector fields, primary key, partition key, or clustering key.
- No index-addressed array ops (`arr[3] = x`, insert-at) and no
  value-returning ops (LPOP-style pop that returns the element) — see
  "Array mutation ops" for the admission rule that permanently excludes
  them. Supported array ops are SET / APPEND / POP_FRONT / REMOVE(value).
- No read-your-write feedback on whether an update hit an existing PK (see
  Write Path).

## Schema and API Semantics

A field may be declared with `mutable=true`. Constraints:

- Allowed types: INT8/16/32/64, FLOAT, DOUBLE, BOOL, TIMESTAMPTZ, ARRAY.
  v1 restricts array element types to numeric/bool/timestamptz; VARCHAR
  elements are a possible follow-up (the arena mechanism below already
  covers them; text match does not apply to arrays, so nothing heavy
  returns).
- A mutable field cannot be: primary key, partition key, clustering key,
  vector field, function-field input, or the target of `create_index`.
- Mutable fields may appear in filter expressions and as output fields.

New DML API:

```
update(collection, pk, {col: value, ...})            # SET
update(collection, pk, {col: Incr(delta), ...})      # INCR, numeric only
update(collection, pk, {arr: Append([v, ...]), ...}) # array: Append / PopFront / Remove(v)
```

- Only mutable columns may appear in the value map; touching a non-mutable
  column is a client-side error (full-row upsert remains the path for those).
- Array values are validated against max_capacity and element type at the
  proxy, like inserts.
- **Missing-PK semantics: silent ignore.** Like a delete whose PK matches no
  segment, the update message is broadcast and dropped wherever the PK bloom
  filter misses. The write path never reads, so throughput and latency match
  the delete path. Clients are not told whether the update took effect.
- **Updates are PK-addressed only.** Batching by PK list is supported (like
  delete). There is no `update ... where <expr>` — predicate-addressed
  updates require a read to resolve targets, which the write path forbids;
  clients can query PKs first and update by PK.

### Supported types × operations

| Type | SET | INCR | Notes |
|---|---|---|---|
| INT8/16/32/64 | ✓ | ✓ | overflow semantics: see Open Questions |
| FLOAT / DOUBLE | ✓ | ✓ | accumulation error accepted |
| BOOL | ✓ | — | |
| TIMESTAMPTZ | ✓ | — | INCR semantically unclear; excluded in v1 |
| ARRAY (numeric/bool/timestamptz elements) | ✓ whole-value | — | plus APPEND / POP_FRONT / REMOVE(value, all occurrences); list semantics, order and duplicates preserved |
| VARCHAR / JSON / GEOMETRY / vector / struct | — | — | immutable |

## Update Semantics

### SET and INCR

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

### Array mutation ops

The admission rule for any op: **it must be loggable without reading the
old value, and its application must be a deterministic state transition.**
Determinism is what guarantees replication — every replica consumes the
same WAL in the same timestamp order, so deterministic ops converge on all
replicas and across recovery replays. (Note this is weaker than requiring
CRDT commutativity: commutativity is needed only without a total order,
and the WAL provides one.) Foldability without base is desired on top, for
patch-compaction quality.

Arrays keep plain **list semantics** (duplicates and order preserved) with
three ops besides SET:

- **`APPEND(elems)`** — append at the tail (Redis RPUSH).
- **`POP_FRONT`** — remove the first element; **returns nothing** (unlike
  Redis LPOP — a returned value would be a read). Pop of an empty list is
  a no-op. `APPEND` + `POP_FRONT` is a zero-read FIFO queue — the natural
  implementation of "keep the most recent N events".
- **`REMOVE(value)`** — remove **all** occurrences of `value`. The
  all-occurrences semantics is deliberate: removing only the *first*
  occurrence is the one variant that breaks foldability, because whether
  the first match lives in the base or in the appended suffix is unknowable
  without reading the base.

Folding: any interleaving of APPEND/POP_FRONT folds exactly to
`(pop_count k, suffix S)` — pops always consume the current head and
appends always extend the tail, so the result is `(base ⧺ S)` minus its
first `k` elements, computable at read time with no base access during
compaction. Any interleaving of APPEND/REMOVE folds exactly to
`(remove_set R, surviving suffix S)`. Sequences interleaving POP_FRONT
*with* REMOVE do not fully compress (their order of action on the unseen
base matters); patch compaction keeps them as a compacted symbolic op chain
and full materialization happens at column folding, where the base is in
hand. Correctness is unaffected; only compression ratio suffers, and the
two realistic workloads (sliding window = APPEND+POP, tags = APPEND+REMOVE)
each fold perfectly.

**What an array patch actually stores:** an op's operand is only its
arguments — APPEND stores just the appended elements, REMOVE stores one
value, POP_FRONT stores nothing; **SET is the only op that carries a full
array**. Neither the deltalog nor the overlay ever holds a materialized
full array: materialization happens per read (base ⊕ folded ops), and the
only durable full-array form is the column file written at folding. A row
appended to 10,000 times costs the overlay one folded suffix, not 10,000
array copies.

Rejected by the admission rule, permanently: index-addressed ops
(LSET/LINSERT — ambiguous under concurrent APPEND, weak use case),
value-returning ops (LPOP/RPOP return the popped value — a read), and any
conditional/CAS-style update.

Consequences:

- **max_capacity cannot be enforced on the write path** (checking fullness
  requires the current length). It is enforced at materialization: reads
  and folds clamp to max_capacity in materialization order, and the
  overflow tail is dropped. This must be documented prominently — it is a
  more surprising flavor of silent-ignore than the missing-PK case. Note
  that with POP_FRONT available, clients that care can maintain window size
  themselves.
- Long op chains cost O(chain) at read until the safe-timestamp pruner
  folds them into a floor node (still relative ops, never needing base).
  Hot-row read cost within the pruning window needs benchmarking.

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
  recycles arena blocks. In steady state each updated row holds one live
  node — overlay memory is bounded by the number of *distinct* updated
  rows, not by update frequency. A hot counter updated 1000×/s occupies
  one slot.
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

## Read Path

- **Expression evaluation** on a mutable column reads the base column chunk
  and consults the overlay at the query timestamp. Evaluation is
  brute-force only in v1; since no index can exist on these columns yet, no
  planner change is needed beyond rejecting `create_index`.
- **Output fields** are materialized the same way after search/query.
- The vector search path is entirely unaware of mutable columns.
- Consistency: visibility is governed by the same timestamp mechanism as
  deletes; guaranteed-timestamp / bounded / eventually all behave identically.

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
- Overlay values are served through a **PatchedColumnView at the data
  access layer** (chunk accessor / bulk_subscript), not inside individual
  expression classes — one integration point covers every expression type
  and output-field materialization. Unpatched columns must pay effectively
  zero overhead on this hot path (a null-check branch).
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

## Folding and Compaction

Two levels keep patch volume bounded:

1. **Patch compaction.** Multiple deltalogs for a segment are merged and
   folded per PK using the op folding rules (SET absorbs, INCR sums, array
   ops fold to `(k, S)` / `(R, S)` or a compacted symbolic chain), below
   the safe timestamp. This is what absorbs high-frequency counter
   workloads. Output is still deltalog format.
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

**Delta cleanup happens only through compaction — nothing else ever deletes
a deltalog.** Patch compaction shrinks the file set; column folding is the
terminal cleaner (materialize, swap, then GC); segment merge compaction
folds everything as a side effect. If compaction lags, the debt is visible
and bounded in form: load-replay time and overlay memory grow. This is why
folding triggers must include overlay memory pressure and deltalog count
(not just patch ratio), and why the overlay quota's backpressure is the
final backstop that throttles the write path when cleanup cannot keep up.

**Wide-update pacing.** Workloads that touch most rows of most segments
(heartbeat-style `offline_time` updates) push all segments across the
folding threshold at roughly the same time. Per-segment folding cost is
small — one narrow column rewrite, no vectors, no indexes — but the
scheduler must still stagger folds to avoid manifest-update and IO
bursts. The fold ratio is also the read-cost knob: because a dense chunk
costs ~9–14× a clean scan (see "Dense-overlay chunks"), folding at ~20%
keeps steady-state read overhead on the hot column near 5×. The same
reasoning carries into Phase 3: **very-high-churn columns should stay
unindexed even once indexes are available** — their patched fraction sits
high between folds, so result correction degrades toward brute force
anyway while forcing continuous index-rebuild churn; plain brute-force
fix-up plus an aggressive fold cadence is the right plan for them.

## Path to Indexed Mutable Columns (Post-v1)

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
  Phase 3; the additions are the materialized bitset (snapshot-readable,
  maintained at apply time, surviving folds until index rebuild) and this
  fallback rule.

With the v1 type set, the relevant indexes are **STL_SORT, BITMAP, HYBRID,
and INVERTED** (including inverted on arrays for `array_contains`); the
correction leg is a plain comparison against the overlay value. The
text-oriented indexes (TRIE, NGRAM, FM, text match) and RTREE apply only to
immutable types and need nothing. Vector indexes and the PK index are
untouched by construction.

An earlier draft covering mutable VARCHAR analyzed text match support via a
per-segment overlay tantivy index, and established that BM25 can never be
supported (exact IDF maintenance requires reading the old document's tokens
— a read-before-write the append-only write path forbids). Recorded here so
the analysis isn't redone if mutable strings are ever revisited.

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

## Known Risks

- **Overlay memory under wide updates.** Hot-row workloads are bounded, but
  a workload touching many distinct rows grows the overlay linearly until
  folding. With the v1 type set this is far smaller than a design including
  text/JSON blobs; ARRAY is the one type needing arena management and is
  the main memory risk. Stress-test first. (The read-side counterpart of
  wide updates — a dense overlay chunk costing ~9–14× a clean scan — is
  bounded by the fold cadence, not a read-path fallback; see "Dense-overlay
  chunks".)
- **PK routing amplification.** Every update probes segment bloom filters on
  every query node, the same cost structure as heavy-delete workloads —
  already a known pain point. High-frequency updates double down on it;
  benchmark against the delete path early.
- **Load-time replay.** Segment load replays patch deltalogs into the
  overlay. Folding discipline bounds this.
- **Observability.** Patch ratio, overlay bytes, and folding lag need
  first-class metrics, or query-performance regressions become
  undiagnosable.

## Implementation Phases

Three phases, each independently shippable behind the collection-level
feature gate. The expensive foundations (write path, overlay, data-access
integration) all land in Phase 1 on the simplest value types; later phases
are additive. Phase 2 and Phase 3 are independent of each other and can
proceed in parallel.

### Phase 1 — MOR foundation + fixed-width scalars (SET / INCR)

Scope: the entire vertical slice for INT/FLOAT/DOUBLE/BOOL/TIMESTAMPTZ.

- Schema `mutable=true` + constraint validation; `update()` API; proto and
  the new WAL message type via codegen (**the op field and operand encoding
  cover array ops from day one** — file/wire formats don't change in
  Phase 2, only new op values activate).
- Write path: proxy → WAL (PK→channel affinity) → L0 → PK-routed
  per-segment patch deltalogs; growing-segment flush of patch deltalogs;
  recovery replay; CDC message type.
- QueryNode: overlay (chunk-partitioned, inline fixed-width version chains,
  safe-ts pruning, memory quota); **PatchedColumnView** at the data access
  layer; expression fix-up via the offset-input path (row-wise at all
  densities — see "Dense-overlay chunks" for why no fallback is needed);
  SkipIndex unconditional-fix-up rule; expression-cache disable;
  output-field materialization; nullable/validity overlay.
- Background: patch compaction (SET/INCR folding), column folding with
  `fold_ts` watermark + manifest swap, folding scheduling policy v0.
- Ops: feature gate, metrics (patch ratio / overlay bytes / fold lag).

Exit criteria: end-to-end SET/INCR on scalar columns with correct
brute-force filtering and outputs under MVCC; folding keeps overlay and
load-replay bounded under sustained load; **stress results for the two
known risks** — overlay memory under wide updates, PK-routing cost vs the
delete-path baseline.

### Phase 2 — ARRAY ops (APPEND / POP_FRONT / REMOVE)

Scope: additive on Phase 1; no write-path or format changes.

- Overlay arena for variable-length values; op-chain version nodes with
  `(k, S)` / `(R, S)` exact folding and the symbolic-chain fallback;
  property tests for folding rules.
- Proxy validation for array ops; max_capacity clamping at
  materialization; NULL-base semantics; array predicate fix-up
  (`array_contains`, `array_length`) through PatchedColumnView.

Exit criteria: array predicates and outputs correct under all op
interleavings (property-tested); hot-row read cost within the pruning
window benchmarked.

### Phase 3 — Index support via result correction

Scope: lifts the "no index" restriction for STL_SORT / BITMAP / HYBRID /
INVERTED (including array inverted).

- `patched_bitset` formalized per (segment, column), rebuilt at load.
- Correction wrapper on the index evaluation path (reuses Phase 1's
  materializer + row-wise evaluation as the brute-force leg) + the base
  fallback rule.
- Planner threshold fallback to full brute force; per-index-type threshold
  benchmarks.
- Fold/rebuild atomic-swap state machine (bitset survives folding until new
  index commits) + datacoord rebuild scheduling.

Exit criteria: indexed queries on mutable columns correct across the fold /
rebuild / swap lifecycle (including crash points); performance-vs-patch-ratio
curves published and thresholds set from them.

## Open Questions

- INCR edge semantics: INCR on NULL (recommend no-op, consistent with the
  silent-ignore philosophy), integer overflow (recommend wraparound, i.e.
  native two's-complement, documented), float accumulation error (accept).
- Array ops on a NULL base: recommend APPEND treats NULL as empty list
  (Redis RPUSH-creates-the-list semantics), POP_FRONT/REMOVE on NULL are
  no-ops.
- Whether ARRAY-of-VARCHAR elements ship in v1 or as a follow-up.
- Per-collection metrics for overlay size / patch ratio to guide folding and
  index-fallback thresholds (likely yes).
- The patch-ratio threshold at which index result correction becomes slower
  than pure brute force (needs benchmarking per index type).
- The column-folding trigger ratio that best balances read-cost bound
  (dense-chunk overhead) against fold IO. A microbenchmark puts a 50%-dirty
  chunk at ~9–14× the clean scan; folding at ~20% caps the hot column near
  5×. Confirm on the real segcore kernel with bit-packed TargetBitmap
  (which makes the clean scan cheaper and thus the dense multiplier larger).
