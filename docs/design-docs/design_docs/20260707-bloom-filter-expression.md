# Bloom Filter Membership Expression: `bloom_match`

- Status: Draft
- Date: 2026-07-07
- Issue: https://github.com/milvus-io/milvus/issues/51139
- Authors: @xiaofanluan

## Summary

Add an approximate membership filter expression `bloom_match(field, {set})` that evaluates
"is this row's scalar value probably a member of a user-provided set" via a Bloom filter,
as a compact alternative to exact `IN [...]` (TermExpr) for large membership sets.

The client builds a compact Bloom filter from its ID set with a user-tunable false-positive
rate (default 0.5%) and passes the blob to `bloom_match`. The wire and execution format is the
Parquet Split-Block Bloom Filter (SBBF), an existing cross-language specification whose C++
implementation already ships in Milvus's vendored Arrow.

## Motivation

A common pattern keeps the source of truth for a membership set (a user's curated list, an
audience segment, a "seen" set) in an external system (DynamoDB, Redis, an OLTP database),
stores only the member key (e.g. `creator_id`) per row in Milvus, and filters at query time:

```
creator_id in [...]        -- include: search within my list
creator_id not in [...]    -- exclude: hide what I've already seen
```

This works well for bounded sets (thousands of values), but degrades for large sets:

1. **Transfer cost.** A TermExpr serializes every value per request. 10M random int64 IDs
   are ~80 MB of protobuf — beyond the default 64 MB gRPC receive limit, and re-sent on
   every query.
2. **Per-query rebuild.** Each segment rebuilds a hash set from the value list on every
   query (`SetElement` in segcore); with many segments this dominates p99.
3. **No compact encoding exists** for "a large set of values" in the expression API today.
   Expression templates (`expr_template_values`) still carry every value per request.

Distributed query engines solved the same problem for semi-joins: the build side is
condensed into a compact, lossy filter (a Bloom filter) and pushed down to the probe-side
scan (Trino dynamic filtering, Spark runtime bloom filter join, Druid's client-supplied
bloom filter). `bloom_match` exposes exactly that capability as a public expression, with
the client (or proxy) playing the role of the join build side.

SBBF costs ~9.7 bits/element at 1% FPR, ~11.0 at 0.5% (default), ~14.6 at 0.1%, ~21.1 at
0.01%. The builder rounds the body **up to a power-of-two byte size** (Arrow's
`OptimalNumOfBytes`) and clamps it to the **128 MiB** ceiling, so the *allocated* blob is the
next power of two above the ideal size — the table below is the actual on-wire size, not the
pre-rounding estimate:

| Members | Exact int64 list (proto) | SBBF @ 1% | SBBF @ 0.5% (default) | SBBF @ 0.01% |
|---|---|---|---|---|
| 1 M | ~8–10 MB | 2 MiB | 2 MiB | 4 MiB |
| 10 M | ~80–100 MB (raw list exceeds the 64 MiB proxy recv limit) | 16 MiB | **16 MiB** | 32 MiB |
| 100 M | ~800 MB (impossible) | 128 MiB | 128 MiB (at ceiling; FPR degraded) | 128 MiB (degraded) |

Bloom filter size depends only on member count and FPR — **not** on the value domain. It
works unchanged for random 64-bit IDs and for strings, with no dense-ID remapping
prerequisite.

**Transport reality and the practical ceiling.** The proxy's default gRPC receive limit is
**64 MiB** (`proxy.grpc.serverMaxRecvSize`). This is precisely why the filter is **always
built on the client**: a raw 10 M-member list is ~90 MB and would be rejected outright,
whereas its blob is ~16 MiB at the default FPR. The blob travels as a native protobuf `bytes`
value with **no base64 inflation**. Because the SBBF body is rounded **up to a power of two**,
usable capacity comes in size tiers, so the practical ceilings under the default FPR (0.5%,
~11 bits/member) are:

- **32 MiB blob → ~24 M members**, the largest tier that fits comfortably under the default
  64 MiB recv limit. This is the effective out-of-the-box cap.
- **64 MiB blob → ~48 M members**, but a 64 MiB body plus envelope and request framing
  exceeds the default limit, so this tier requires **raising `serverMaxRecvSize`**.
- **128 MiB blob → ~97 M members**, the C++ prober's hard cap on a single blob.

Coarsening the FPR shifts every tier up (e.g. a 32 MiB blob holds ~39 M members at the 5%
maximum FPR). Beyond these, callers raise the recv limit, coarsen the FPR, or post-filter
through an external KV / a future registered-filter handle (Future Work). The design does not
chase absolute maximum sizing — solid support into the tens of millions is the goal.

## Goals

- Approximate membership filtering for `INT8/16/32/64` and `VARCHAR` fields — and for
  **JSON paths** (including dynamic fields), strictly typed by the value stored at the
  path (see Semantics §5) — via a compact Bloom filter, usable in `filter`
  expressions for search and query. (Delete-by-expr is explicitly out of scope — see
  Non-goals.)
- The filter blob is **built on the client** (SDK helper `NewBloomFilterBlob(members, fpr)`);
  the client chooses the FPR at build time (default 0.005, allowed range [0.0001, 0.05]) and
  bakes it into the blob header. The proxy validates and embeds the blob but never builds one.
- Cross-language, versioned wire format so any SDK can pre-build the filter.
- Fail-closed behavior on version skew: an old node must reject the expression, never
  silently skip filtering.

## Non-goals

- No scalar-index acceleration in v1: `bloom_match` always evaluates on the data path
  (per-row probe). An index-enumeration optimization is Future Work.
- No exact-set (roaring bitset) template in this design; it is a sibling encoding tracked
  separately (Future Work).
- Not usable in delete expressions in v1: deletes are destructive; an approximate filter
  with false positives would delete rows outside the set. The parser rejects `bloom_match`
  in delete expressions.
- No server-side persistent/registered filter handles in v1 (Future Work).

## Prior art

| System | Mechanism | Relation |
|---|---|---|
| ClickHouse | roaring bitmaps as first-class values (`groupBitmap`, `bitmapContains`) for audience segmentation | Same semantics with an exact encoding; validates the "dense set membership pushdown" pattern at scale |
| Druid | client embeds a base64 `BloomKFilter` in the query JSON as a filter | Direct precedent for client-supplied serialized bloom filters |
| Elasticsearch | `terms` lookup (set referenced by document id, 65536-term default cap) | The "set by reference" shape; its cap shows reference-without-compact-representation doesn't scale |
| Trino / Spark | dynamic filtering / runtime bloom filter join: build side condensed into a filter pushed to probe scans | The same idea, automatic and internal; `bloom_match` is the public, cross-system version |
| FAISS | `IDSelectorBitmap` — caller-supplied bitmap consulted during search | Library-level precedent for caller-supplied membership filters in vector search |

## API design

### Expression syntax

```
bloom_match(<field>, {<blob>})
not bloom_match(...)
```

- Parsed through the existing generic `CallExpr` mechanism (`Plan.g4` `Identifier '(' ... ')'`),
  like `text_match` / `phrase_match`. No new grammar, no new reserved word.
- `<field>`: a scalar field of type INT8/INT16/INT32/INT64/VARCHAR, or a **JSON path** —
  `bloom_match(meta["user_id"], {bf})`, nested paths (`meta["a"]["b"]`), the whole JSON
  document (`bloom_match(meta, {bf})`, useful when rows hold bare scalars), and dynamic
  fields (an unknown identifier resolves to a `$meta` path) all work. Not ARRAY elements.
- **Not inside `MATCH_*` / `element_filter`**: bloom_match is rejected inside a struct-array
  element predicate. Its one-sided error (false positives) is only safe for monotonic
  aggregations; `MATCH_MOST` / `MATCH_EXACT` bound the per-row hit count from above, so a
  Bloom false positive on one element would push the count over the bound and wrongly drop a
  true row (row-level false negative). Rather than ship a per-MatchType error-semantics
  matrix, all element-level bloom_match is rejected (Future work).
- `<blob>`: an expression template placeholder resolved from `expr_template_values`, carrying
  a **client pre-built filter blob** as a **raw bytes** template value
  (`schemapb.TemplateValue.bytes_val`). There is no proxy-side build and no raw-list path: the
  client always builds the MBF1/SBBF blob (`client/sbbf`, reproducible cross-language) and
  ships the compact ~32 MiB blob. proto3 `bytes` carries the blob with **zero base64 inflation**.
  The proxy validates the MBF1 envelope (`sbbf.Parse`) and embeds it verbatim.
- **No `<fpr>` argument.** The FPR is chosen by the client at build time
  (`NewBloomFilterBlob(members, fpr)`) and encoded in the blob header; the expression carries
  only the field and the blob.
- **Why client-only build:** it removes the client→proxy transfer cliff by construction (a raw
  10M-member list is ~90 MB and exceeds the 64 MiB proxy recv limit, but its blob is ~16 MiB
  at the default FPR),
  eliminates per-query proxy build CPU, and uses one uniform wire form that the roaring/bitset
  exact encodings (below) reuse without change.

### SDK usage (sketch)

The client always builds the blob and passes it as the `{bf}` parameter (raw bytes). The Go
SDK ships `milvusclient.NewBloomFilterBlob(members, fpr)`; any SDK builds the same MBF1/SBBF
bytes from the shared spec.

```go
// include: search within my circle. fpr is chosen here, baked into the blob.
bf, _ := milvusclient.NewBloomFilterBlob(creatorIDs, sbbf.DefaultFPR)  // 0.5%; []int64 or []string
client.Search(ctx, milvusclient.NewSearchOption("posts", topK, []entity.Vector{qv}).
    WithFilter("bloom_match(creator_id, {bf})").
    WithTemplateParam("bf", bf))

// exclude: hide what I've seen (over-hides ~fpr, safe direction)
//   WithFilter("not bloom_match(creator_id, {seen})")
```

### Why function syntax instead of overloading `in`

1. **Lossiness is visible in the expression text.** The same `id in {x}` silently switching
   between exact and approximate depending on a parameter type is an audit hazard.
2. **Zero new reserved words.** The *surface* syntax `bloom_match(...)` is lexed by the
   existing generic function-call grammar rule (like `text_match`), so there is no grammar or
   keyword change.
3. **Fail-closed rolling upgrade.** Note the surface syntax does NOT stay a generic `CallExpr`
   on the wire — the parser rewrites it into a dedicated `BloomFilterExpr` plan oneof
   (`plan.proto` field 22). Fail-closed comes from **how an old querynode parses that plan**,
   not from the function name: an old node whose protobuf schema lacks field 22 deserializes
   the `Expr` oneof as *unset*, and `ProtoParser::ParseExprs` rejects an unknown/unset expr node
   in its `default` case (`ThrowInfo(ExprInvalid, "unsupported expr proto node")`), so plan
   creation fails loudly instead of silently returning unfiltered rows. This relies on the
   parser's reject-unknown-expr default — a new oneof field is otherwise silently ignored by an
   old reader, which is exactly the failure mode this default guards against.

## Semantics

1. `bloom_match` is **approximate with one-sided error**: it may match rows whose value is
   NOT in the set (false positives, rate ≈ fpr), and never misses rows whose value IS in
   the set (no false negatives).
   - Include usage: results may contain up to ~fpr extra out-of-set rows. Callers needing
     exactness either post-filter the returned rows against their source of truth, or choose
     an **exact encoding** (roaring / dense bitset — see Future work) which has no false
     positives. (A built-in bloom "strict" recheck helper was considered and dropped: once an
     exact encoding exists, exactness is an encoding choice, not a separate query mode.)
   - Exclude usage (`not bloom_match`): over-excludes ~fpr of out-of-set rows, never
     under-excludes. Safe direction for "hide seen" semantics.
2. **NULL never matches**, under either `bloom_match` or `not bloom_match` — consistent
   with TermExpr three-valued logic for `IN` / `NOT IN`.
3. **Type widening**: INT8/16/32 values are widened to int64 before hashing, so one filter
   built from int64 values works against any integer field width.
4. The declared `n` and `fpr` in the blob header are informational (logged, exported as
   metrics); the engine executes whatever the blob encodes.
5. **JSON paths are STRICTLY TYPED.** The hash domain has exactly two kinds — int64
   (8-byte LE) and raw UTF-8 bytes — and a JSON value probes a domain only when it is
   stored AS that type:
   - a JSON **string** hashes its raw UTF-8 bytes — exactly a VARCHAR probe;
   - a JSON **int64** hashes as int64 (simdjson yields int64 for every integer that fits;
     uint64 appears only beyond `INT64_MAX`);
   - a JSON **double** (including an integral `5.0`) and a **uint64 beyond int64** are
     **never members**: the row evaluates `false` with `valid=true`, so `not bloom_match`
     returns it. This is a **deliberate divergence from exact `in`**, whose JSON semantics
     unify `5.0 == 5` — a row storing `5.0` is returned by `meta["x"] in [5]` but NOT by
     `bloom_match(meta["x"], {bf})`. Rationale: a numeric canonicalization rule would have
     to be kept bit-identical across every prober and engine version forever, and a subtle
     mismatch silently drops members; a strictly-typed hash domain has no such invariant.
     Writers whose integers may be float-encoded (common from JS/Python JSON encoders)
     should normalize on write or use a typed scalar field. Consequence: on JSON paths the
     "bloom ⊇ exact `in`" superset property holds only within the strictly-typed domain
     (rows storing int64s and strings).
   - **missing key, JSON null, bool, object, array**: there is no probe value. The row is
     `false` under BOTH polarities (`res=false`, `valid=false`), consistent with TermExpr
     three-valued gaps. Note for exclude semantics: `not bloom_match(meta["x"], {bf})` does
     NOT return rows lacking the key; callers who want them add
     `or not exists(meta["x"])` explicitly.
   - **Type domains stay distinct**: JSON int `5` never aliases the string `"5"` — they hash
     different byte encodings, matching `5 != "5"` in Milvus JSON comparison semantics.

## Exactness — an application-side pattern (NOT shipped in this PR)

Some callers want the compact pushdown of `bloom_match` **and** exact results (no false
positives). This PR deliberately **does not** ship a built-in "strict" mode or SDK recheck
helper: exactness is left to the caller, who has two paths.

1. **Choose an exact encoding.** A future roaring / dense-bitset `algo` (see Future work) has
   no false positives, so exactness becomes an encoding choice at build time — no separate
   query mode, no recheck. This is the preferred long-term answer and the reason a bloom-only
   strict helper was dropped from scope.
2. **Post-filter application-side.** With the bloom encoding, over-fetch and recheck the
   returned rows against the source of truth, dropping the ~fpr false positives and refilling
   until `top-k` exact results are collected.

The rest of this section records **why the recheck belongs application-side** (design
rationale for path 2), not a component this PR delivers.

### Why the recheck must live application-side — not proxy/querynode

Exact membership testing needs the **full set**: a Bloom filter is the lossy *approximate*
form, and no structure smaller than the set itself can answer membership exactly
(information-theoretic lower bound). Combined with the design's core invariant — *never
materialize the large set in a new place* — this rules out every in-engine layer:

- **querynode**: holds only the compressed blob → cannot recheck.
- **proxy**: rechecking would re-materialize the full set in memory (the very memory/transfer
  cost `bloom_match` exists to avoid), and in the client-prebuilt-blob path the proxy has no
  set at all. → rejected.
- **application**: the source of truth (DynamoDB, Redis, an OLTP store) is adjacent to the
  application, not to Milvus. → the correct and only non-regressing layer.

### The key property: recheck touches only the returned results, never the whole set

Recheck operates on the **`k` returned candidates**, not the set — `k` is at most a few
thousand, so the large set is never materialized for it. Two sub-strategies by set size:

- **Small/medium set** (fits in client memory): the client already holds the values it used
  to build the filter; recheck each returned row against that in-process set. Zero extra queries.
- **Large set** (tens of millions+): do not materialize. Point-query the source of truth for
  the returned candidates' keys (`DynamoDB BatchGetItem`, `Redis SISMEMBER`). `k` lookups.

The only step that ever traverses the whole set is **building the filter**, a one-time
streaming pass (insert while paging through the source of truth; never holds the whole set in
RAM), cacheable for stable sets (see Future Work: registered handles).

### Over-fetch and iterative refill (the application pattern)

Dropping false positives can leave fewer than `k` results, so an exact caller:

1. Over-fetches `k' = k + margin` (a small constant suffices — at fpr=0.005 the expected
   false positives among the top 100 is ~0.5).
2. Rechecks `k'`, keeps exact members.
3. **If still short of `k`, keeps pulling** (deeper fetch or a `SearchIterator`) and rechecks
   the next batch until `k` exact results are collected or the stream is exhausted. Each round
   is `O(batch)` point lookups; rounds are rare because the drop rate is ~fpr.

### Polarity: exactness recovery applies to the include direction

For **`bloom_match`** (include) the error is false *inclusion* — extra rows in the results,
removable by rechecking the returned rows. For **`not bloom_match`** (exclude) the error is
false *exclusion*: a non-member that collides in the filter is wrongly dropped and never
appears, so rechecking returned rows cannot recover it. Exact exclude instead uses the
standard post-filter pattern (search without/with a coarser filter, over-fetch, recheck-drop
against the source of truth) — a search pattern, not a property of the expression itself.

A future transparent proxy-side or SDK helper for path 2 is listed under Future work; it is a
convenience for thin/REST clients and is not needed for the large-set case, which is
application-side regardless.

## Wire format: MBF1 envelope + Parquet SBBF

### Why Parquet Split-Block Bloom Filter

- **Formal cross-language spec** (parquet-format `BloomFilter.md`), designed for interop;
  independent implementations exist in C++ (Arrow), Java (parquet-mr), Rust (parquet-rs).
- **The C++ implementation is already in Milvus's dependency tree**: our vendored Arrow is
  built with `arrow/*:parquet=True`, providing `parquet::BlockSplitBloomFilter` (XXH64).
  segcore gets a battle-tested, SIMD-friendly prober for free.
- **Probe cost**: one XXH64 + eight salted probes within a single 32-byte block — one cache
  line per row.
- Trade-off: ~15–25% more space than a classic Bloom filter at equal FPR. Accepted.

The existing Go-side bloom filters (`internal/util/bloomfilter`: blobloom / bits-and-blooms,
used for PK statistics) are NOT reused for the wire format: their serialization is
library-internal JSON with no cross-language stability contract. Their value-hashing
convention (int64 → little-endian 8 bytes; varchar → raw UTF-8 bytes) IS retained.

### Value byte encoding (must be bit-identical in every implementation)

```
INT8/16/32/64 -> widen to int64 -> 8 bytes little-endian -> XXH64(seed=0)
VARCHAR       -> raw UTF-8 bytes (no length prefix)      -> XXH64(seed=0)

JSON path values (probe side; the blob is built from int64s or strings as above;
strictly typed -- a double NEVER probes the int64 domain, see Semantics 5):
  string                          -> raw UTF-8 bytes -> XXH64(seed=0)
  int64                           -> 8 bytes LE      -> XXH64(seed=0)
  double / uint64 beyond int64    -> never a member  (no hash; res=false, valid=true)
  missing/null/bool/object/array  -> no probe value  (res=false, valid=false)
```

This matches Parquet's plain encoding for INT64 / BYTE_ARRAY, so filters are also
compatible with parquet-ecosystem tooling.

### MBF1 envelope

```
offset  size  field
0       4     magic "MBF1"
4       2     version        (uint16 LE, = 1)
6       2     algo           (uint16 LE, 1 = parquet_sbbf_xxh64)
8       8     n_declared     (uint64 LE, informational)
16      8     fpr_declared   (float64 LE, informational)
24      4     num_blocks     (uint32 LE; body length must equal num_blocks * 32)
28      4     reserved       (must be 0)
32      ...   body: SBBF blocks, bit-identical to the parquet-format spec layout
```

Validation on receipt (proxy for pre-built blobs, querynode always): magic, version, algo,
`body_len == num_blocks * 32`, `num_blocks` power-of-two per SBBF spec, total size within
the C++ SBBF 128 MB ceiling. Any mismatch → request rejected with a parameter error.

### Conformance: golden test vectors

A checked-in vector file (fixed value lists → exact expected blob bytes + membership
truth table) is consumed by the Go builder tests AND the C++ prober tests. Arrow C++'s
`BlockSplitBloomFilter` acts as the reference implementation; any SDK builder (Python,
Java, ...) must reproduce the vectors bit-for-bit.

## Execution design

### Build path (client builds, proxy validates & embeds)

1. **Client** builds the SBBF with the spec-conformant Go package `client/sbbf`
   (`NewBloomFilterBlob(members, fpr)`; XXH64 via `cespare/xxhash/v2`), sizes it from (n, fpr)
   per the SBBF formula, and wraps it in the MBF1 envelope. Any SDK reproduces the same bytes
   from the shared spec. There is **no proxy-side build** and no raw-list path.
2. The blob travels as a **raw bytes** template value (`schemapb.TemplateValue.bytes_val`,
   added in milvus-proto) — zero base64 inflation. It is bounded by the C++ SBBF 128 MB
   ceiling and by gRPC transport limits.
3. **Proxy** — `FillBloomMatchExpressionValue` reads the bytes template value, validates the
   MBF1 envelope via `sbbf.Parse`, and embeds the blob verbatim into the plan
   (`BloomFilterExpr { ColumnInfo column_info; bytes filter_blob; }` in `pkg/proto/plan.proto`).
   No rebuild. Downstream fanout to querynodes carries ~17 bits/member instead of
   ≥64 bits/member, and querynodes never rebuild anything.

**Roaring/bitset groundwork.** The MBF1 envelope's `algo` field is the dispatch point
(`algo=1` = parquet SBBF today). Exact encodings slot in as `algo=2` (roaring) / `algo=3`
(dense bitset): a new client builder emits the same envelope with a different `algo` and body,
and segcore's `Parse` dispatches to a different per-row `Test`. The whole wire path
(`bytes_val` template value → validate → embed → `filter_blob`), the NULL/offset-input/cache-key
framework, and the delete-safety guard are all encoding-agnostic and reused unchanged.

### Probe path (segcore, C++)

- ProtoParser maps `BloomFilterExpr` to a new physical expression
  (`exec/expression/BloomFilterExpr.{h,cpp}`), a sibling of the `TermExpr` family.
- **Data path by default.** Per row: widen/encode value → XXH64 → one-block probe directly
  against the blob buffer (zero-copy; header validated once per query). Sealed and growing
  segments behave identically when raw field data is present.
- **Index-only fallback.** When a sealed field is loaded index-only (no raw data), the probe
  recovers each row's value via the scalar index's `Reverse_Lookup` and probes that —
  preserving exact per-row semantics (NULL never matches). This is used **only** for indexes
  whose reverse lookup is cheap (STL_SORT O(1), MARISA O(strlen)); a **BITMAP index without
  its offset cache** reverse-looks-up in O(cardinality) per row, so it is excluded
  (`ScalarIndex::SupportFastReverseLookup()`), and the query fails with a clear, retriable
  error rather than silently running an O(rows×cardinality) scan. The fallback is
  force-batched (`CanExecuteAllAtOnce()==false`) so it never materializes a whole-segment
  offset vector.
- One read-only view of the blob is shared by all segments of a query on a querynode —
  no per-segment copies.
- NULL handling via the valid bitmap: NULL rows produce `false` before the probe.
- **JSON paths are data-path only.** Per row: parse the JSON, extract the value at the
  RFC 6901 pointer built from the nested path, dispatch on its runtime type per
  Semantics §5. No scalar index offers a per-row reverse lookup for a JSON path, so the
  index-only fallback does not apply; an index-only sealed JSON field fails with a clear
  error telling the operator to load raw field data. Cost is one simdjson parse per row —
  the same class as every other JSON predicate (a future JSON-key-stats/shredded fast path
  can lift this, see Future work).
- A predicate containing `bloom_match` is **not cacheable**: `BloomFilterExpr::IsCacheable()`
  returns false and propagates up through AND/OR/NOT, so `PhyFilterBitsNode` never stores a bloom
  result bitmap in `ExprResCacheManager`. Two distinct client-supplied filters therefore can
  never collide into reusing each other's bitmap. (`BloomFilterExpr::ToString` stays a slim
  summary — blob length plus declared element count, no blob dump — used only for logging.)

### Resource protection

- **`proxy.maxBloomFilterSize`** (default 32 MiB, refreshable): operator-tunable **per-blob**
  cap on the SBBF **body**, enforced in `validateBloomFilterBlob`, so an oversized blob is
  rejected at the proxy rather than fanned out to QueryNodes. The fixed 32-byte MBF1 header is
  allowed on top of the budget: the SBBF body is always a power of two, so budgeting the whole
  blob at exactly 32 MiB would reject a full 32 MiB body (32 MiB + 32 B) and silently halve the
  usable ceiling to a 16 MiB body (~12M members instead of ~24M). Budgeting the body keeps the
  default a clean power of two and admits a full 32 MiB body (~24M int64 members at the default
  FPR). The 128 MiB MBF1 num_blocks format cap remains the hard per-blob ceiling on both sides.
- **`proxy.maxBloomFilterPlanSize`** (default 128 MiB, refreshable): request-wide cap on the
  serialized size of every bloom-bearing plan, enforced with `proto.Size(plan)` before
  `proto.Marshal`. Reusing one `{bf}` in multiple expressions therefore consumes the budget for
  every embedded copy. Hybrid search accumulates all bloom-bearing sub-plans against the same
  request budget, and scorer filters are included. Exceeding the cap returns
  `ErrParameterTooLarge` (1102, InputError, non-retriable) at the proxy, before QueryNode routing,
  so an over-budget Bloom request cannot turn into a gRPC `ResourceExhausted` or blacklist a
  healthy QueryNode. Invalid, zero, negative, or integer-overflowing configuration values fall
  back to 128 MiB.
- **Log redaction is copy-free**: when a bloom-bearing plan is debug-logged, the blob byte
  slices are temporarily swapped for a `{N bytes}` marker (pointer assignment, no
  duplication of the up-to-tens-of-MiB body), stringified, then restored — never `proto.Clone`d.

- The C++ SBBF 128 MB ceiling caps the blob size admitted per expression; larger blobs are
  rejected during envelope validation.
- The proxy plan-total budget leaves headroom below the default QueryNode gRPC receive limit;
  gRPC transport limits remain the final backstop for the complete internal request.
- Envelope validation before any allocation sized from untrusted fields.

### Rolling upgrade / compatibility

- The parser rewrites `bloom_match` into a dedicated `BloomFilterExpr` plan oneof
  (`plan.proto` field 22). An old querynode that lacks that field deserializes the `Expr`
  oneof as unset and `ParseExprs` rejects it in its `default` case (`ThrowInfo(ExprInvalid,
  ...)`) → fail-closed at plan creation, never silent unfiltered results.
- The new plan.proto message is only emitted for the new expression; plans without
  `bloom_match` are unchanged.

## Observability

- Proxy logs (ctx-scoped, mlog) at embed time: declared member count and declared fpr read
  from the MBF1 header (`sbbf.Parse`), plus blob size. The proxy does not build the filter, so
  these are the client's declared values, not proxy-computed ones.
- **Log redaction**: a bloom_match blob is up to tens of MiB of binary. The proxy's plan
  debug log (`task_search.go`) renders the plan through `RedactPlanForLog`, which replaces
  every `filter_blob` with a `{N bytes elided}` marker (lazy: no cost when the log level is
  off or the plan has no bloom_match). The trace-detail request log
  (`GetRequestFieldWithoutSensitiveInfo`) likewise elides `bytes_val` expression-template
  values. Neither path emits the raw blob.
- Metrics (**Future work**, not in this PR): a proxy blob-size histogram and a per-query
  bloom probe selectivity (rows passed / rows evaluated) to let users validate their fpr
  choice. The existing `QueryNodeSegmentFilter{Hit,Skipped}SegmentNum` metrics cover
  segment-level skip-index behavior but not per-expression bloom selectivity.

## Testing plan

1. Go unit: `client/sbbf` golden vectors; empirical FPR check (build from set A, probe
   disjoint set B, assert measured FPR ≈ declared within tolerance); envelope validation
   fuzz (truncated/corrupt headers).
2. Parser unit: syntax, arg validation (exactly two args; second must be a `{template}` bytes
   placeholder, not a literal or a raw list), MBF1 envelope validation, field type checks,
   template resolution, rejection in delete expressions.
3. C++ unit: prober consumes the same golden vector files — bit-identical membership
   answers; NULL bitmap handling; header rejection cases.
4. Integration/e2e: include and exclude searches over mixed sealed+growing segments;
   assert zero false negatives (every true member retrievable) and FP-rate sanity;
   large-set (1M+) latency smoke.

## Future work

1. **Index-enumeration probe**: for segments with a BITMAP/INVERTED index on the field,
   iterate the index's distinct values, probe each once, union posting lists —
   O(distinct) instead of O(rows). (Distinct from the current index-only *fallback*, which is
   still O(rows) via per-row reverse lookup and exists only to serve index-only layouts.)
2. **Exact roaring bitset template**: sibling encoding for callers that cannot tolerate
   false positives and have dense integer domains. Reuses the MBF1 envelope behind a new
   `algo` byte, so the wire form and the client→proxy→querynode path are unchanged.
3. **Registered filter handles**: upload a filter once (`CreateFilter → handle`),
   reference it in queries, cache per-(handle, segment) results — amortizes transfer and
   probe cost for reused sets; subsumes the ES terms-lookup shape.
4. **`bloom_match` in delete expressions**: intentionally excluded until a
   verified-exact re-check path exists.
5. **SDK exactness helper** (Go / pymilvus): over-fetch → recheck-against-source-of-truth →
   iterative refill, per the Exactness section. An application-side convenience; this PR
   ships only the primitive, not the helper. (A proxy-side variant is not viable: the reshaped
   design builds the blob on the client, so the proxy never holds the raw member list to
   recheck against — exactness is application-side or via an exact encoding, never proxy-side.)
6. **JSON-key-stats fast path for JSON-path probes**: when a path is shredded into typed
   columns (JSON key stats), probe the shredded INT64/STRING columns directly (strictly
   typed: the DOUBLE column never probes) instead of parsing raw JSON per row — the same
   shape TermExpr's shredding executor uses. Pure optimization; semantics unchanged.
7. **ARRAY / struct-array element membership**: `bloom_match` over array elements — top-level
   `bloom_match(array_field, {bf})` ("any element in set", aligned with `array_contains_any`)
   and the `MATCH_ANY(struct, bloom_match($[sub], {bf}))` form. Deferred because the one-sided
   error is only safe for monotonic aggregations: `MATCH_MOST` / `MATCH_EXACT` bound the
   per-row hit count from above, so a Bloom false positive can wrongly drop a true row. A
   future version can enable the monotonic subset (ANY / ALL / LEAST) with per-MatchType
   guards and tests.
## Design decisions log

| Decision | Alternatives considered | Rationale |
|---|---|---|
| Parquet SBBF + XXH64 | blobloom (Go-native), custom format | Formal cross-language spec; C++ impl already vendored via Arrow; Go builder is ~100 lines |
| Function syntax `bloom_match` | Overload `in` + typed template; global `hints` | Explicit lossiness; zero reserved words; fail-closed on old nodes; hints are request-global (too coarse) |
| Client builds the blob; proxy only validates + embeds | Proxy builds from a raw list | Proxy never materializes the large set; the compact blob is the only thing on the wire; blob travels as a native protobuf `bytes` template value (no base64 inflation). Leaves room for future exact encodings behind the same `algo` byte |
| Data path only in v1 | Index-enumeration probe | Smaller surface; uniform sealed/growing behavior; optimization is additive later |
| Default fpr 0.005 (client-chosen at build time) | 0.001 / 0.01 / 0.05 | ~0.5 expected stray results at top-100 include; imperceptible over-hiding on exclude; only ~+14% size vs 1% while ~5x fewer false positives, so a good balance point. fpr is baked into the blob by the client builder — not a query argument |
| Reject in delete exprs | Allow with warning | False positives would delete out-of-set rows; destructive + approximate don't mix |
| No built-in strict mode; exactness is application-side | proxy-side recheck; querynode-side recheck; a shipped SDK strict helper | Exact membership needs the full set; proxy/querynode rechecking re-materializes the large set (the cost bloom_match removes). Recheck touches only the k results → k point lookups against the source of truth; large set never materialized. Left to the caller (or a future exact encoding) rather than shipped as a mode |
