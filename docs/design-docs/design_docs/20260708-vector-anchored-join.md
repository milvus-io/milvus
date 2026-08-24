# MEP: Vector-Anchored Join — Non-Vector Tables, Global KV Index, BF Pushdown, and Lateral Vector Search

- **Created:** 2026-07-08
- **Author(s):** @xiaofanluan
- **Status:** Draft
- **Component:** C++ Core (segcore, index, ANN batch search), QueryNode/Delegator, Proxy, Go plan parser (planparserv2), DataCoord, Function framework (embedding), a new Global KV Index service, scalar-only (non-vector) table type
- **Related Issues:** [#51185](https://github.com/milvus-io/milvus/issues/51185)
- **Related PR:** TBD
- **Released:** TBD

## Summary

Milvus today serves a single collection well: filtered vector search over one
set of entities, where every filterable field lives *inside* the same segments
as the vectors. It has no first-class way to **join** a vector collection
against a *separate, frequently-updated* table keyed by a shared identifier —
the classic "vectors are stable, their business metadata mutates constantly"
problem. Users work around it by either (a) denormalizing mutable fields into
the vector collection (which forces `delete + re-insert` of the whole row,
re-writing the vector on every metadata change — unacceptable write
amplification), or (b) joining in the application layer with N point lookups
per query (correct, but pushes the join out of the engine and loses any chance
of pushdown).

This MEP adds a **deliberately narrow** join capability. It rests on a new data
model and two access-path primitives, and exposes three join shapes — all of
which keep vector search on the critical path.

**Data model:** a first-class **scalar-only (non-vector) table** — a collection
with no vector field, mutability-first storage, and standalone point-lookup /
filter / upsert. It is the native *side* (and, for lateral search, the *driver*)
of the joins below, and is independently queryable.

**Primitives:**

1. A **Global KV Index** — a mutable, LSM-style secondary index keyed by an
   arbitrary scalar key, mapping `key → {primary key, optional value payload}`
   across the whole collection (not per-segment). Decoupled from the immutable
   vector-segment lifecycle so it absorbs a high update rate without triggering
   vector compaction.
2. **Bloom-filter pushdown** generalized from the existing per-segment PK bloom
   filters to *arbitrary join keys*, driving both **segment-level pruning** and
   **row-level inline filtering** during ANN traversal.

Neither primitive is a hard prerequisite. All three shapes below can be built
**today** on Milvus's existing access paths (per-segment PK lookup, scalar-filter
scan, the existing BF pushdown, `nq > 1` batch search, the function framework) —
just *slower*. The Global KV Index and generalized BF pushdown are **fast-path
accelerators** that change the constant factor, not the semantics, and land
incrementally (see Rollout).

**Three join shapes:**

| # | Capability | SQL semantics | Physical operator | Primitive |
|---|---|---|---|---|
| 1 | Vector-driven **enrichment join** | `INNER` / `LEFT OUTER` | index nested-loop: vector top-K drives, probe KV index by key | Global KV Index |
| 2 | External-predicate **pre-filter** | `SEMI` | foreign filter yields a key-set → bloom/bitmap inline into ANN traversal | BF pushdown |
| 3 | **Lateral vector search** (driver-side) | `LATERAL` / correlated | scalar table drives; one batched ANN (`nq = N`) runs vector search as the inner per driver row | Global KV Index / Function |

Shapes 1–2 are the case where **vector search drives** and the scalar table is
the inner (enrichment / pre-filter). Shape 3 is the **transpose**: a scalar
table drives and vector search is the inner, run once per driver row. All three
have vector search on the critical path.

Everything else — two scalar-only tables joined together, *unbounded*
vector-to-vector (all-pairs KNN) similarity join, non-equi / range joins,
`FULL OUTER`, `CROSS` — is explicitly **out of scope** and left to the
compute/lakehouse layer. The governing principle is: *Milvus does vector-anchored
joins, not general joins. Every supported join has vector search on the critical
path (as driver, beneficiary, or inner) — but a scalar-only table may be queried
on its own as ordinary table functionality.*

## Motivation

### Vector-driven workloads (shapes 1–2)

A recommendation, search, or RAG system stores item embeddings in Milvus. The
embeddings are computed once and rarely change. But the *business state* of each
item — price, stock, `is_active`, moderation status, per-user permission, A/B
bucket — changes constantly, lives in an OLTP/KV system, and must participate in
the query:

- *Enrichment (shape 1):* "vector-search the catalog, return the top 50, and
  attach each item's current price and stock." The mutable fields are needed on
  the **output**, and the query should not drop a result just because its
  metadata row has not been written yet (→ `LEFT OUTER` semantics).
- *Pre-filter (shape 2):* "vector-search only over items that are currently
  `in_stock` and `not_blocked`," where those flags are maintained in the
  frequently-updated table. The mutable fields **constrain recall** (→ `SEMI`
  semantics).

### Driver-side workloads (shape 3)

Some workloads are naturally *scalar-driven*:

- **Per-entity recommendation.** "For each currently-active user (a row in a
  frequently-updated user table), find 10 items similar to their last-viewed
  item."
- **Per-row semantic search / batch RAG.** "For each open support ticket
  (filtered by status), retrieve the 5 most relevant KB articles."
- **Bounded entity resolution / dedup.** "For each record flagged
  `needs_review`, find its nearest neighbors in the canonical collection."

Today all of these force a client-side loop (filter externally, fire N search
RPCs), losing batching, shared-filter factoring, and any engine-side bound.

### Why Milvus cannot do this well today

- **Denormalization is a trap.** Sealed segments are immutable; the vector index
  is built once per sealed segment. Updating one scalar field is an `upsert`,
  which is `delete + insert` — the entire row, *including the vector*, is
  re-written, the old row becomes a tombstone in the delete bitmap, and
  compaction pressure grows. A frequently-updated field turns the vector
  collection into a churn engine.
- **App-layer join loses pushdown.** Doing the point lookups (or the N searches)
  outside Milvus works but forfeits segment pruning, inline filtering, batching,
  and any cost-based choice. The engine is blind to the join.

## Goals / Non-Goals

### Goals

- A first-class **scalar-only (non-vector) table** type — mutability-first
  storage with standalone point-lookup / filter / upsert — serving as the
  native side/driver of the joins.
- `INNER` / `LEFT OUTER` **enrichment join** from a vector search result to a
  keyed side table, executed as an index nested-loop with a global KV probe.
- `SEMI` **pre-filter** of a vector search by a predicate evaluated on a keyed
  side table, executed via bloom/bitmap pushdown into the ANN scan.
- `LATERAL` **driver-side vector search**: a scalar table drives; a
  single-collection top-K ANN runs per driver row; compiled to **one batched
  ANN** (`nq = |filtered driver|`), not N RPCs; with mandatory cardinality
  guardrails.
- A cost-based choice, driven by estimated cardinalities, among the physical
  operators (see Cost-based operator selection).
- A Global KV Index that sustains a high update rate independently of vector
  segment compaction.

### Non-Goals

- **General join engine.** No join between two scalar-only tables. No
  *unbounded* vector-to-vector (all-pairs KNN) join. Those belong to the compute
  layer; Milvus at most exposes its result stream / key-set to it. (Querying a
  single scalar-only table on its own *is* in scope — that is table
  functionality, not a join.)
- **Non-equi and range joins.** Joins are equality on the KV-indexed key, plus
  arbitrary *residual* single-side predicates. Cross-table range joins are out.
- **`FULL OUTER` / `RIGHT OUTER` / `CROSS`.** Only the vector side may be the
  outer (preserved) side for shapes 1–2; for shape 3 only the driver may be
  preserved (`LEFT` lateral).
- **Multi-way joins in v1.** One vector collection joined to one side table (or
  one driver to one vector collection). Star-schema fan-out and multi-way
  lateral are follow-ups, not part of this MEP.

## Design

### Entities and keys

Throughout, three objects and one invariant:

- **`V`** — the **vector collection**: embeddings plus a set of *stable*
  filterable fields, indexed the normal Milvus way. Critically, `V` **also
  carries the `join_key` as an ordinary scalar column** — this is the anchor
  every join hangs off.
- **`S`** — the **scalar-only (non-vector) side table**: the frequently-updated,
  keyed metadata, keyed by the same `join_key` (its primary key may *be* the
  `join_key`).
- **`GlobalKVIndex`** — declared on `S.join_key`, mapping
  `join_key → {S.pk, payload?}`.

Invariant: **a join is always equality on `join_key`, and `V` filters/probes on
its own `join_key` column.** So the SEMI pre-filter (shape 2) evaluates
`v.join_key ∈ S_qualified` against `V`'s *local* `join_key` column; the
enrichment probe (shape 1) takes each result row's `v.join_key` and looks it up
in the `GlobalKVIndex` on `S`. `V` and `S` share the key space but neither needs
the other's storage layout.

```
V (vector collection)              S (scalar-only side table)
  pk           INT64  (auto)         item_id    INT64  PRIMARY KEY  = join_key
  item_id      INT64  ← join_key     price      FLOAT
  embedding    FLOAT_VECTOR          stock      INT32
  category     VARCHAR (stable)      is_active  BOOL
  ...          (indexed normally)    updated_ts INT64
                                     -- GlobalKVIndex on item_id
```

### The non-vector (scalar-only) table

All three joins need a scalar side — the keyed, frequently-updated data that a
vector result is enriched with / pre-filtered by (shapes 1–2), or that *drives*
the search (shape 3). This MEP makes that a **first-class Milvus table type — a
collection with no vector field at all**. Today Milvus requires every collection
to carry at least one vector field and routes every query through a segment/ANN
path; a scalar-only table lifts both assumptions.

**Data model.** A primary key, a declared **join key** (may equal the PK), and
arbitrary scalar / JSON / VARCHAR fields. No vector field, no ANN index.

**Storage is mutability-first.** This is the "frequently updated" side, so it is
backed by an LSM / delta store (growing-segment + delete-log lineage, or a
dedicated KV/LSM), *not* the immutable sealed-segment column store built for
vectors. An upsert is an in-place LSM write; there is no vector to re-encode and
no ANN index to rebuild. That is the whole reason a scalar-only table can absorb
a high update rate where a denormalized vector collection cannot. Whether this
reuses the existing growing-segment + delta-log machinery or introduces a
dedicated LSM store is an open implementation choice (see Open Questions).

**Standalone query capability** (ordinary table functionality, not a join):

- Point lookup / get by PK or join key — served by the Global KV Index, cheap
  regardless of update rate.
- Scalar filter + scan / projection — `expr` predicates, the same planparserv2
  grammar as today.
- Upsert / delete under Milvus consistency levels.

**The filter-efficiency caveat — stated honestly.** A scalar-only table under
heavy mutation cannot cheaply keep every classic scalar index (inverted /
bitmap) fresh. We therefore split access paths by role:

- The **join key** always carries the Global KV Index (mutable, LSM). So the
  *hot* path — point lookup for enrichment, key-set generation for the SEMI
  pre-filter, driver scan for lateral search — is always fast, independent of
  update rate.
- **Other filter columns** get *optional* mutable scalar indexes where the
  update rate allows, and fall back to scan otherwise. We make the **join** path
  fast unconditionally, and treat an arbitrary standalone filter on a cold
  column as best-effort.

### Primitive 1: Global KV Index

A new logical index type declared on a **join key** field (which need not be the
primary key). Semantics:

```
GlobalKVIndex : join_key  ->  { primary_key, value_payload? }
```

Properties:

- **Global, not per-segment.** A single logical structure spans the whole
  collection, so a probe by `join_key` is one lookup, not a fan-out over
  segments. This is what makes index-NLJ cheap.
- **Mutable, LSM-backed, decoupled from segments.** Backed by an LSM/KV store
  (embedded RocksDB per shard, or a shared KV service — see Open Questions),
  *not* by the sealed-segment column store. Updates to the side table mutate the
  KV index in place (LSM merge) and never touch vector segments. This is the
  single most important structural decision: it is what lets the "frequently
  updated" side be frequently updated.
- **Two population modes:**
  1. *Internal* — the join key is a field of a Milvus collection: the vector
     collection itself, or a **scalar-only (non-vector) table** holding the
     mutable side (see above). Milvus maintains the KV index on write.
  2. *External* — the key-set / values are fed from an external mutable store
     (CDC stream, or the existing external-table mechanism). Milvus maintains
     only the index structure and a freshness watermark.
- **Value payload is optional.** For pure `SEMI` pre-filter we only need key
  membership (a set); for enrichment we need the payload (the columns to
  attach); for by-reference lateral search we need the referenced vector. The
  payload is opt-in to keep the index small when only membership is required.
- **Batched multi-get.** The index exposes a vectorized multi-get so that
  resolving N keys at once (lateral search, batch enrichment) is one call, not N
  point lookups.

### Primitive 2: Generalized Bloom-Filter Pushdown

Milvus already maintains per-segment PK bloom filters for delete routing and
segment pruning. We generalize this to arbitrary join keys and to two pushdown
targets:

- **Segment-level pruning.** From a key-set, build a bloom filter (and a
  min/max key range where the key is ordered). Before scanning a segment, test
  the segment's key summary against it; skip segments that provably share no
  key. This is usually the largest win — whole segments never get read.
- **Row-level inline filtering.** During ANN graph traversal, each candidate
  node is tested against the pushed-down bloom filter / bitmap *before* it is
  admitted to the result heap — the vector-search analogue of index-condition
  pushdown. A false-positive bloom hit is later reconciled against the exact
  key-set (bloom filters have no false negatives, so nothing valid is dropped).

This is the mechanism behind the `SEMI` pre-filter and, symmetrically, is
exactly the "runtime filter / sideways information passing" pattern from classic
hash joins: the small side generates a filter that prunes the large side's scan.

### Capability 1: Vector-driven enrichment join (INNER / LEFT OUTER)

Query shape:

```
SELECT v.id, v.<...>, s.price, s.stock
FROM   vectors v  LEFT JOIN  side s  ON v.join_key = s.join_key
WHERE  <vector search on v with v's own stable filters>
ORDER BY <vector distance>
LIMIT  K
```

Execution — **index nested-loop, vector side drives**:

1. Run filtered ANN on `v` → top-K rows (K is small: tens–thousands).
2. For each of the K rows, probe the Global KV Index by `join_key` → payload
   (batched multi-get).
3. `INNER`: drop rows with no KV match. `LEFT OUTER`: keep the row, null-fill
   the side columns. **Default to `LEFT OUTER`** so a not-yet-written metadata
   row never silently removes a valid vector result.
4. Project the requested side columns onto the result.

Plan:

```
    Project [ v.*, s.price, s.stock ]
        │
    LeftOuterJoin  (index-NLJ, probe by v.join_key)
    ├── VectorSearch(V)  filter=<stable>  limit=K      ← driver (top-K)
    └── SideProbe(side)  by join_key                   ← inner (batched)
          fast path : GlobalKVIndex multi-get, O(1)/key
          baseline  : per-segment PK/key lookup fan-out (or scan on non-PK key)
```

Because we probe only K keys, the mutable side's own (inefficient) filter index
is never used — only its cheap by-key point lookup. This is the whole reason the
"frequently updated + poorly indexed" side is not a problem here.

### Capability 2: External-predicate pre-filter (SEMI)

Query shape:

```
SELECT v.id, v.<...>
FROM   vectors v
WHERE  <vector search on v>
  AND  v.join_key IN (SELECT join_key FROM side s WHERE s.in_stock AND NOT s.blocked)
ORDER BY <vector distance>
LIMIT  K
```

Execution — **semi-join pushed into the ANN scan**, with a cost-based choice on
the estimated cardinality `|S|` of the qualifying key-set:

- **`|S|` tiny (≤ a few hundred):** materialize the exact key-set, resolve the
  corresponding vectors via the KV index, and **brute-force** distances. Recall
  is exact and we avoid the sparse-bitmap graph-connectivity collapse that
  plagues filtered HNSW at low selectivity.
- **`|S|` medium:** materialize an exact **bitmap** over candidate row-ids and
  do inline filtering during ANN traversal.
- **`|S|` large (low selectivity):** build a **bloom filter** from the key-set,
  push it down for **segment pruning** + **inline row filtering**. High
  pass-rate means low rejection — ANN is barely slowed; this is the easy end of
  the spectrum, ironically, not the hard one. The exact key-set is still kept for
  the **final recheck** that removes bloom false-positives (a SEMI filter *admits*
  rows, so an unrechecked false-positive would wrongly include a vector); the
  bloom only makes segment pruning and the inline pre-test cheap.

Plan:

```
    Project [ v.* ]
        │
    VectorSearch(V)  limit=K
        │   ⟵ pushdown: v.join_key ∈ S
        │        |S| tiny   → materialize subset + brute-force
        │        |S| medium → exact bitmap, inline during traversal
        │        |S| large  → bloom filter: segment prune + inline
        └── BuildKeySet(S) once
              fast path : KV-assisted eval of the side predicate
              baseline  : full scan + filter of the side table
```

The qualifying key-set is produced *once* per query, either by evaluating the
foreign predicate through the Global KV Index (internal mode) or by receiving it
from the external store (external mode). It is never expanded into a giant
`IN (...)` literal.

### Capability 3: Lateral vector search (driver-side, batched ANN)

The transpose of shapes 1–2: a scalar table **drives** and vector search is the
**inner**, run once per driver row.

Query shape:

```
SELECT d.id, m.match_id, m.distance
FROM   driver d,
       LATERAL vector_search(V, query_vector_of(d), top_k => K, filter => <...>) AS m
WHERE  d.<scalar predicate>
ORDER BY d.id, m.distance
```

For each driver row surviving `d`'s filter, run a top-K ANN over `V` and emit
`driver-row ⋈ its K nearest matches`. Result cardinality is
`|filtered driver| × K`. Semantics: `INNER` lateral by default (a driver row
with no matches contributes nothing); `LEFT` lateral opt-in (keep the driver row
once with null matches). Output is naturally partitioned by driver row: an
`nq`-query batch search already returns one result block per query vector, so the
K matches for driver row *i* are exactly result block *i* — no extra grouping
pass is needed. (This reuses the multi-query result layout, not the `group_by`
search feature.)

**Query-vector binding.** A scalar-only driver has no vector, so the operator
must *produce* one per row (a request picks exactly one binding):

1. **By-reference (Global KV Index resolve).** The driver row carries a key that
   identifies an entity whose vector lives in `V`; resolve `key → vector` via
   the KV index **batched multi-get**. "Similar to the entity this row points
   at."
2. **By-function (embedding).** The driver row carries raw content (text, etc.);
   a declared embedding function (existing Milvus function framework) maps
   `content → query vector`, **batched** across all filtered driver rows.
   "Search by this row's text."

(A raw client-supplied per-row query vector needs no join — that is just the
existing `nq > 1` batch-search API. This shape is for vectors *derived from
stored driver rows*.)

**Physical execution — one batched ANN, not N searches:**

```
1. Scan driver, apply scalar predicate      → filtered driver rows (bounded, see guardrails)
2. Batch-resolve / batch-embed               → N query vectors, index-aligned to driver rows
3. Factor filters (shared vs per-row)        → one bitset, or grouped bitsets
4. Single grouped ANN search over V (nq = N) → per-query top-K
5. Stitch: attach each query's K matches back to its driver row; project
```

Plan:

```
    Stitch / GroupByDriver
        │
    BatchedVectorSearch(V, nq = N, top_k = K)
        │   ⟵ filter: shared bitset (or grouped by signature)
    QueryVectorBind → N vectors (index-aligned to driver rows)
        ├── by-reference : GlobalKVIndex batch multi-get
        │                  (baseline: per-segment resolve fan-out)
        └── by-function  : embed(driver.content)   [no KV needed]
        │
    DriverScan(driver, pred)  +  guardrail: cap(N), opt-in
```

- **Batch step 2.** N serial KV lookups / embedding calls defeat the purpose;
  both bindings expose a vectorized batch path.
- **Shared-filter fast path (step 3).** If every driver row imposes the *same*
  filter on `V` (or none), compute the bitset **once** and reuse it for all N
  queries — the common, cheap case.
- **Per-row filter (harder).** If each driver row parameterizes a *different*
  filter, group driver rows by distinct filter signature and run one batched
  search per group; fall back to per-row only for fully-distinct filters.
- **Stitch (step 5).** Keep query-index ↔ driver-row alignment so `nq`-indexed
  results map back without a re-join.

**Cardinality guardrails (the core risk).** `|filtered driver|` *is* the number
of ANN queries; unbounded, it is a foot-gun. Non-negotiable controls:

- **Hard cap** on `N` after the scalar filter; exceeding it is an explicit error
  (not a silent truncation), pointing at pagination.
- **Cost estimate gate.** Estimate `N × cost(top-K ANN over V)` from driver
  cardinality statistics and reject / require override above a budget.
- **Pagination over the driver.** Drive the fan-out in bounded batches.
- **Opt-in.** Not implicitly triggered by ordinary search; an explicit lateral
  request.

**Relationship to KNN-join.** Lateral vector search *is* the **bounded** form of
a KNN join: driver = filtered scalar set, inner = ANN over `V`. The *unbounded*
all-pairs form stays out of scope (compute layer).

### End-to-end data flow

The plan trees above are the operator view; this is the component view, across
Proxy → QueryNode (Delegator = shard leader) → segments. The key thing it shows
is *where in the query pipeline each shape's join work lands* — and the three are
deliberately different:

- **Shape 1 (enrichment): probe *after* the reduce.** Side columns do not affect
  ranking, so the join is a post-processing step on the final top-K.
- **Shape 2 (SEMI): pushed *down before* the reduce.** The predicate constrains
  recall, so it must be inside the segment ANN scan.
- **Shape 3 (lateral): driver *first*, then one batched search.** The scalar
  table is the source of the query vectors, so it runs ahead of the ANN.

```
Shape 1 · enrichment (INNER / LEFT OUTER)
  Client ──search(V, expr, limit=K, output=[S.price,S.stock])──► Proxy
                                              │ scatter
                       ┌──────────────────────┴──────────────────────┐
                       ▼                                              ▼
                 Delegator #1  ...                             Delegator #n
                 filtered ANN over its growing+sealed segments → local top-K
                 (each result row carries v.join_key)
                       └────────────────── local top-K ──────────────┘
                                          ▼
                       Proxy: global reduce → final top-K
                                          ▼
                       batched multi-get on the K join_keys
                          fast: GlobalKVIndex(S)  │  baseline: per-segment lookup on S
                                          ▼
                       LEFT-OUTER enrich + project ──► Client
   note: INNER (which can drop rows) over-fetches K'>K before the probe;
         probe placement (Proxy vs Delegator) is an Open Question.

Shape 2 · SEMI pre-filter
  Client ──search(V, expr, limit=K, semi: S WHERE <pred>)──► Proxy
    1) BuildKeySet(S, pred) once → S_qualified (+ bloom/bitmap)
         fast: KV-assisted  │  baseline: scan + filter S
    2) broadcast key-set / bloom to delegators
                                          ▼
       Delegator: ANN over segments with  v.join_key ∈ S  pushed INLINE
         |S| tiny → brute-force subset · medium → bitmap · large → bloom + segment-prune, then recheck
         → local top-K ──► Proxy global reduce ──► Client

Shape 3 · lateral (driver-side, batched ANN)
  Client ──lateral(driver = S WHERE <pred>, V, top_k=K, bind)──► Proxy
    1) DriverScan(S, pred) → N rows        [guardrail: cap(N), opt-in]
    2) Bind → N query vectors
         by-reference: GlobalKVIndex multi-get  │  by-function: embed(content)
    3) BatchedSearch(V, nq=N, top_k=K, shared/grouped filter) ── scatter ──► Delegators
                                          ▼
       per-query top-K blocks → Stitch (block i ↔ driver row i) ──► Client
```

### The primitives are accelerators, not prerequisites

None of the three shapes is *blocked* on the Global KV Index; each has a
correct, if slower, execution on primitives Milvus already has. The KV index and
generalized BF pushdown replace a slow by-key access path with a fast one — they
change the constant factor, not the semantics.

| Shape | Fast path (this MEP) | Baseline available today (slower) |
|---|---|---|
| 1 · enrichment | global KV batched multi-get, O(1)/key | per-segment PK/key lookup fan-out (already how Milvus does `get` by PK); non-PK join key → scalar-filter scan |
| 2 · SEMI pre-filter | KV-assisted key-set build + generalized BF pushdown | full scan + filter of the side table to build the key-set, then the *existing* bitmap/BF pushdown into ANN |
| 3 · lateral | KV batched multi-get for by-reference resolve | by-function (embedding) needs no KV at all; by-reference resolves via per-segment lookup fan-out |

Consequence for delivery: we ship the **join operators and semantics first** on
the baseline paths, then land the Global KV Index and generalized BF pushdown as
pure performance work behind the same API. The feature is not gated on the KV
index — the KV index makes it fast.

### Cost-based operator selection

The planner needs cardinality estimates for:

- `K` — the vector search limit (known) for the enrichment probe.
- `|S|` — size of the qualifying key-set for the `SEMI` pre-filter, from
  KV-index statistics or the foreign predicate's selectivity.
- `N` — size of the filtered driver for lateral search.

Decision sketch:

```
if enrichment join:                     # shape 1
    index-NLJ (K is small and bounded)
elif semi pre-filter:                   # shape 2
    if |S| <= brute_force_threshold:   materialize + brute-force
    elif |S| <= bitmap_threshold:      exact bitmap inline
    else:                              bloom-filter (segment prune + inline)
elif lateral vector search:             # shape 3
    enforce guardrails on N; batched ANN (nq = N), shared/grouped filter
```

Without real cardinality estimates the adaptive choices degrade to guesses, so
KV-index cardinality statistics are a hard dependency, not a nicety.

### Update & consistency semantics

- **MVCC, not a new model.** The Global KV Index and the scalar table are
  timestamp-versioned like everything else in Milvus: a query reads them at its
  `guarantee_timestamp`, and the consistency level (Strong / Bounded /
  Eventually) selects that timestamp exactly as it does for a normal search. A KV
  entry stale relative to the read timestamp yields stale *side columns*, never a
  wrong or missing *vector* result — there is no separate consistency model to
  reason about.
- **Write path.** Updates to `S` flow through the normal Milvus write path
  (proxy → streaming / WAL → the LSM-backed scalar store); DataCoord schedules
  the KV-index merge and the scalar table's compaction, just as it schedules
  segment and index build for vector collections. No new write API.
- **Update cost.** Because the KV index and scalar store are decoupled from
  vector segments, a burst of side-table updates produces LSM writes only —
  **no vector re-encode, no ANN rebuild, no vector-segment compaction**. This is
  the property that makes the "frequently updated side" affordable.
- **Deletes.** A delete on `S` is an LSM tombstone; a `SEMI` pre-filter always
  reconciles against the exact post-tombstone key-set, so a just-deleted key
  cannot leak its vector into the result.
- **Cross-source freshness.** For lateral search the driver (`S`) and `V` may sit
  at different timestamps — see Open Questions on whether a request carries one
  `guarantee_timestamp` for both or allows them to differ.

## API sketch

Two surfaces (to be settled in review):

1. **Explicit side-table + join predicate / lateral request** (most general):
   register the mutable data as a scalar-only table (or external table), declare
   a `GlobalKVIndex` on its join key, and express enrichment/pre-filter in the
   search request's filter / output-fields, or a lateral search as a search
   variant taking a *driver spec* (table + predicate + binding) plus `V`,
   `top_k`, `filter`.
2. **"Linked field" sugar** (least surprise for current users): declare on the
   vector collection that field `X` is *linked* to a keyed source;
   `output_fields=[X]` triggers the enrichment probe, and `expr` predicates on
   `X` trigger the `SEMI` pre-filter — the join stays implicit.

Recommendation: ship the sugar (2) as the user-facing default for shapes 1–2
(it matches how people already think about Milvus fields), and the explicit
form (1) as the escape hatch and the required surface for lateral search (which
carries the guardrails explicitly).

## Alternatives considered

- **Denormalize into the vector collection.** Rejected: forces vector re-write
  on every metadata change (see Motivation), and for lateral search re-pins a
  derived vector onto a frequently-updated driver.
- **App-layer join / client-side search loop.** Rejected as the *engine*
  answer: loses segment pruning, inline filtering, batching, and cost-based
  choice — though it remains a valid client pattern this MEP does not remove.
- **General hash/sort-merge join in Milvus.** Rejected for scope: turns Milvus
  into a half-built OLAP engine. Large-vs-large and unbounded vector-vs-vector
  joins are delegated to the compute/lakehouse layer.
- **Reuse only the per-segment PK bloom filters.** Insufficient: they are
  per-segment and PK-only. We need a *global* by-key structure for the NLJ probe
  and *arbitrary-key* bloom filters for pushdown.
- **Treat lateral search as `nq > 1` batch search only.** Insufficient: that API
  assumes the caller already has the query vectors; the point here is that the
  vectors are *derived from filtered stored driver rows* (resolve/embed), which
  needs the driver scan + binding + guardrails.

## Rollout / phasing

The feature is **not gated on the Global KV Index.** We ship join semantics
first on existing access paths, then accelerate. Phase 0 is doable today.

- **Phase 0 — Join operators on baseline access paths (no new primitive).**
  Scalar-only table type + all three shapes on what Milvus already has:
  enrichment (INNER/LEFT) via per-segment PK/key lookup; SEMI pre-filter reusing
  the *existing* bitmap/BF pushdown with a scan-built key-set; lateral search
  (by-function embedding needs no KV, by-reference via per-segment resolve) with
  hard cap + opt-in. Correct semantics, slower by-key access. Proves the API and
  the operators before any storage work.
- **Phase 1 — Global KV Index (internal mode).** Replace the slow by-key paths
  from Phase 0 (enrichment probe, SEMI key-set build, lateral by-reference
  resolve) with a global batched multi-get. Pure performance, same API.
- **Phase 2 — Generalized BF pushdown** to arbitrary join keys (segment pruning
  + inline filtering) and cost-based operator selection across the `|S|` tiers.
- **Phase 3 — External mode** (CDC / external-table-fed KV index) + freshness
  watermark; **per-row filters + pagination** for lateral search.
- **Follow-ups (separate MEP):** `ANTI` (blocklist) via the same machinery;
  broadcast hash join to small dimension tables (star-schema enrichment).

## Open questions

- **Scalar-table storage engine:** reuse the growing-segment + delta-log
  machinery (fastest to build, but that machinery is tuned for eventually-sealed
  segments, not indefinitely-mutable rows) vs. a dedicated LSM store (a better
  fit for sustained mutation, but more to build and operate).
- **KV-index backing store:** embedded RocksDB per shard (simple, but sharding
  and rebalancing become Milvus's problem) vs. a shared distributed KV service
  (operationally heavier, but natural global scope and independent scaling).
- **Topology placement of the KV index:** co-located with the Delegator /
  QueryNode (probe locality for enrichment) vs. a standalone service (clean
  update path for the mutable side). These pull in opposite directions.
- **Cardinality statistics** granularity and refresh cost on the KV index — the
  cost-based selector and lateral guardrails both depend on it.
- **Lateral guardrail defaults:** default hard cap on `N`, and whether it is a
  cluster config, per-collection setting, or per-request parameter (with a
  cluster ceiling); whether total returned rows (`|driver| × K`) is capped
  independently of `N` and `K`.
- **Per-row filter grouping for lateral search:** worth the group-by-distinct-
  filter machinery, or restrict early phases to shared-filter only?
- **Consistency for lateral search:** one consistency level for driver and `V`,
  or allow them to differ?
- **Multi-tenancy / partition-key interaction:** should the Global KV Index be
  partition-key aware so pruning aligns with existing partitions?
