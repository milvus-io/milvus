# MEP: GPU_HNSW Filtered Search — CPU-parity delete / TTL / partition filtering on GPU

- **Created:** 2026-07-18
- **Author(s):** @6si
- **Status:** Implemented (GPU-side filtered search landed; validated at CPU-HNSW
  parity across L2/IP/COSINE and delete ratios on sealed, GPU-indexed segments)
- **Component:** Index | QueryNode
- **Related Issues:** milvus-io/milvus#50653, zilliztech/knowhere#1686
- **Related design docs:** [20260619-gpu-hnsw.md](20260619-gpu-hnsw.md),
  [20260711-gpu-hnsw-ocq-removal.md](20260711-gpu-hnsw-ocq-removal.md)
- **Released:** N/A

> **Revision 2 (2026-07-18)** — incorporates first-round design-review feedback.
> Resolved: **I1** concrete two-tier smem layout (`ef_valid = ef` unchanged,
> validity packed into `is_expanded`, separate `ef_inv` frontier); **I2**
> device-side `needs_bf[]` short-query fallback (no mid-search host round-trip —
> §5.4); **I3** batch-level alpha gate in the post-sort serial section (§5.3).
> Minor items M1–M6 folded into §5.2/§5.4/§7.
>
> **Revision 3 (2026-07-18)** — second review round. **N1** cross-beam merge /
> eviction / parent-selection decision tree spelled out (§5.3.1). Added
> `needs_bf_count` and `accumulated_alpha` init details (§5.4). **Rejected** the
> "12 B/slot" smem correction: the code has **six** `ef`-length arrays
> (`result_*` + `merged_*`, `GpuHnswSearchKernel.cuh:731-741,1109-1118`) and a
> `/24` divisor (`GpuHnswSearch.cuh:214-219`); the merge is **not** in-place
> (`merged_*` written at `:478-484`, copied back at `:491-493`). Base stays
> 24 B/ef ⇒ with the +12 B invalid frontier, `max_ef ≈ (smem−overhead)/36`
> (§5.5).

## 1. Summary

Today `GPU_HNSW` / `GPU_HNSW_SQ` **rejects** any search that carries a non-empty
`BitsetView` (deletes, TTL expiry, partition/visibility) with
`Status::invalid_args, "GPU_HNSW does not support filtered search"`. That limits
GPU_HNSW to append-mostly / immutable collections.

This MEP specifies **full CPU-HNSW-parity filtered search on the GPU**: the GPU
kernel will consume the same delete bitset Milvus already produces, exclude
filtered rows from results, keep traversing filtered rows as graph waypoints (so
recall is preserved), and fall back to a brute-force scan at high filter ratios —
matching the exact semantics of the CPU HNSW path in Knowhere.

**Scope of change:** faiss CUDA kernel (primary) + knowhere plumbing (minor) +
Milvus docs/tests (no functional Milvus code). Milvus already builds and passes
the bitset to `Search()`; nothing in the delegator / segcore delete pipeline
changes.

## 2. Background — how deletes reach the index

Milvus deletes are soft and MVCC-based (see the delete pipeline: L0 segments +
delta logs → per-segment `BitsetView` at query time, gated by `TSafe` /
guarantee timestamp). By the time a search reaches Knowhere, the delete / TTL /
partition state for a segment has been collapsed into a single `BitsetView` over
the segment's row offsets: **bit set ⇒ row is filtered out**. CPU indexes honor
this bitset; GPU_HNSW currently rejects it.

Current rejection (`knowhere/src/index/hnsw/faiss_hnsw.cc`,
`GpuHnswIndexNode::Search()`):

```cpp
if (!bitset.empty() && bitset.data() != nullptr) {
    return expected<DataSetPtr>::Err(Status::invalid_args,
                                     "GPU_HNSW does not support filtered search");
}
```

## 3. Target behavior — CPU HNSW parity (authoritative reference)

The GPU path must reproduce the CPU HNSW filtered-search semantics. Those live in
Knowhere:

### 3.1 Two-tier traversal: valid results vs. invalid frontier
`faiss/cppcontrib/knowhere/impl/Neighbor.h` — `NeighborSetDoublePopList` keeps
**two** lists:
- `valid_ns_` — the result beam; only **non-filtered** nodes. Produces top-k.
- `invalid_ns_` — filtered nodes retained **only for expansion** (navigation),
  and only while their distance beats the valid beam's back distance.

The search frontier pops the globally-closest of the two (`pop_based_on_distance`),
so filtered nodes are still expanded in distance order to preserve graph
connectivity, but never enter the result set (`Neighbor::kValid` /
`Neighbor::kInvalid` status, checked at `insert`).

### 3.2 `accumulated_alpha` / `kAlpha` admission gate
`faiss/cppcontrib/knowhere/impl/HnswSearcher.h` — `evaluate_single_node`:

```cpp
if (!filter.is_member(v1)) {          // v1 is filtered out (deleted)
    status = knowhere::Neighbor::kInvalid;
    accumulated_alpha += kAlpha;       // kAlpha = bitset.filter_ratio() * 0.7
    if (accumulated_alpha < 1.0f) {
        continue;                      // skip: don't even expand this filtered node
    }
    accumulated_alpha -= 1.0f;         // admit it as an (invalid) frontier node
}
```

Effect: at low filter ratios filtered nodes are rarely admitted as waypoints
(cheap); as the filter ratio rises `kAlpha → ~0.65` so more filtered nodes are
kept for navigation, preserving recall. `accumulated_alpha` starts at `1.0`, or
`FLT_MAX` (always-admit) once the filter ratio crosses the BF threshold.

### 3.3 Brute-force fallback (two triggers)
Thresholds in `thirdparty/hnswlib/hnswlib/hnswalg.h`:
```cpp
constexpr float kHnswSearchKnnBFFilterThreshold = 0.93f; // filtered fraction
constexpr float kHnswSearchBFTopkThreshold      = 0.5f;  // k vs live count
```
1. **Up-front:** if `filtered_out >= 0.93 * ntotal` **or** `k >= 0.5 * live`,
   skip graph search and scan all live rows directly.
2. **Per-query:** after graph search, if the number of valid results `< k` (and
   `>= k` live rows exist), redo that query as brute force
   (`bf_search_needed()` in `faiss_hnsw.cc`; guarded by
   `disable_fallback_brute_force`).

**These three mechanisms together are "CPU parity."** A GPU implementation that
only drops deleted ids at copy-out is NOT parity — it collapses recall exactly in
the cases (2) and (3) exist for. This MEP therefore specifies all three.

## 4. Requirements & non-goals

**Requirements**
- R1: Non-empty bitset no longer rejected; filtered rows never appear in results.
- R2: Recall on the surviving set matches CPU HNSW within tolerance across the
  full delete-ratio range (0–99%), for all 5 dtypes (fp32/fp16/bf16/int8-generic/
  int8-DP4A) and L2 / IP / COSINE.
- R3: Guarantee `k` valid results whenever `>= k` live rows exist (via the BF
  fallback), matching CPU.
- R4: No new memcheck / initcheck / racecheck findings; concurrent filtered
  searches + reload remain safe.
- R5: No functional Milvus code change; the existing delete/L0/bitset pipeline is
  untouched.

**Non-goals**
- Partition-key multi-index routing (`getIndexToSearchByScalarInfo`) — GPU_HNSW is
  single-index per segment; out of scope.
- Iterator / range-search filtered paths on GPU (knn only in phase 1).
- Changing the delete pipeline, L0 compaction, or TSafe logic.

## 5. Design

### 5.1 ID-space mapping (must be proven first)
The kernel's `result_ids` are FAISS internal ids; knowhere returns them straight
to `GenResultDataSet` with **no remap**. For a single `IndexHNSWFlat` /
`HNSW_SQ` segment the storage id == add order == segment row offset == the
`BitsetView` index. So `bitset.test(node_id)` is a direct 1:1 lookup.

**Action (blocking):** add a test asserting GPU raw id == Milvus row offset ==
bitset index on a segment with known deletes, before relying on it. If any
reorder exists this is a silent correctness bug. (The CPU multi-index
`label_to_internal_offset` / `internal_offset_to_most_external_id` path is not
used for GPU_HNSW.)

### 5.2 Bitset upload to device
- The bitset is identical for all `nq` queries in a `Search()` call → upload
  **once per call**, not per query. Size = `ceil(N/8)` bytes.
- Add to `GpuHnswSearchScratch` (`GpuHnswTypes.h`): `uint8_t* d_bitset` +
  `size_t bitset_bytes`, allocated in `ensure()` next to the visited bitmap. One
  buffer per scratch-pool slot ⇒ concurrent searches stay isolated (R4).
- Knowhere `Search()` `cudaMemcpyAsync`s the host bitset words onto the slot's
  stream before launch, and passes `d_bitset` (nullable ⇒ no filter), `N`, and
  the precomputed `filter_ratio` / `kAlpha` into `GpuHnswSearchParams`.
- VRAM cost: N=1M ⇒ 125 KB × pool_size(4) = 500 KB per segment. Negligible.
- **Stream ordering + lifetime (M2).** The upload and the kernel launch are on
  the **same** slot stream, so stream ordering guarantees the copy completes
  before the kernel reads `d_bitset` — no explicit sync needed. `Search()` is
  synchronous (it blocks on results), so Milvus's `BitsetView` host memory stays
  valid for the whole call. **Invariant to preserve:** if `Search()` is ever made
  async, the host bitset must be copied into pinned scratch first, or its
  lifetime extended — called out here so the invariant is not silently broken.
- **Reallocation on N change (M3).** `d_bitset` has the same N-dependency as the
  existing `d_visited_bitmaps`; `ensure()` tracks `bitset_bytes` alongside
  `bitmap_bytes` and reallocates when a pooled slot is reused for a segment with
  a different `N` (identical pattern to the visited bitmap).

### 5.3 Kernel: two-tier beam + alpha gate (`GpuHnswSearchKernel.cuh`)
Mirror §3.1–3.2 inside `layer0_beam_search_kernel`:
- **Device bitset test:** `__device__ bool is_filtered(const uint8_t* b, uint32_t id)`
  (word/bit test; `b==nullptr ⇒ false`).
- **Two-tier beam — concrete smem layout (resolves I1).** The result beam is
  **not** split or shrunk: `ef_valid = ef` (the caller's `ef`), so at 0% filter
  the result beam is byte-for-byte today's beam and recall cannot regress. The
  invalid frontier is a **separate, additional** region of size `ef_inv` (default
  `ef_inv = ef`, host-tunable/cappable). Validity is encoded as a **flag bit in
  the existing `is_expanded` word** (currently only 0/1) — **no new per-slot
  `status` array**, so the result-beam per-slot cost stays 24 B (see below). New
  smem layout:
  ```
  result_ids[ef]  result_dists[ef]  is_expanded[ef]      (24 B/ef — unchanged; validity packed into is_expanded)
  merged_ids[ef]  merged_dists[ef]  merged_expanded[ef]  (already present)
  invalid_ids[ef_inv]  invalid_dists[ef_inv]  invalid_exp[ef_inv]   (NEW: 12 B/ef_inv)
  staging / parent_ids / meta                            (unchanged)
  ```
  The invalid frontier is admitted only while its distance beats the valid
  beam's worst (`result_dists[meta[0]-1]`) — the GPU analog of `invalid_ns_`
  gated by `valid_ns_->at_search_back_dist()`.
- **Alpha gate — parallel semantics (resolves I3).** CPU's `accumulated_alpha`
  is a *sequential rate-limiter* ("admit one filtered node, skip the next
  ~1/kAlpha"), which has no meaning across parallel warps. We adopt the
  reviewer's **Option 3 (batch-level, after sort)**: the alpha gate runs in the
  **already-serial** post-`bitonic_sort_staging` section of
  `parallel_merge_into_result`, single-threaded, walking the distance-sorted
  staging list and applying the exact `+= kAlpha; if (<1) skip; else -= 1` logic
  to each filtered candidate before it may enter the invalid frontier.
  `accumulated_alpha` is a per-query value carried in `meta[]` across iterations.
  This is deterministic and preserves the rate-limiting *effect* in distance
  order; it is **not** bit-identical to CPU graph-neighbor encounter order, so
  parity is asserted **empirically** by the recall gate (§9), not by identical
  traversal. `kAlpha = filter_ratio * 0.7` is passed from the host.
- **`search_width` / staging unchanged (M4).** Staging still holds *all*
  discovered neighbors (filtered included); `max_staging = search_width *
  max_degree0` is unaffected. Filtering only changes (a) result admission and
  (b) the alpha-gated entry into the invalid frontier.
- **Upper layers unchanged.** `upper_layer_search_kernel` only routes; results
  come from layer 0. Leave greedy descent alone — deleted nodes still route.
- **Copy-out** (`GpuHnswSearchKernel.cuh:1052-1073`): emit only valid ids into the `k`
  outputs; pad with sentinels as today when fewer than `k` valid survive (the BF
  fallback in §5.4 then fills those queries).
- Thread `d_bitset` / `N` / `kAlpha` through **all 5** layer-0 specializations
  (`<float>`, `<half>`, `<__nv_bfloat16>`, `<int8,float>`, `<int8,int8,DP4A>`).
  The filter is dtype-independent (operates on ids).

#### 5.3.1 Cross-beam merge, eviction, and parent selection (resolves N1)
Today `parallel_merge_into_result` merges the distance-sorted staging list into a
**single** beam and `is_expanded` drives parent selection from that one beam
(`meta[2]`). With two tiers this must become explicit. Concrete decision tree,
applied while consuming the sorted staging list (single-threaded alpha-gate
section, then the parallel merge):

1. **Per candidate (in distance order):**
   - `is_filtered(id) == false` → merge into the **result beam** exactly as today
     (binary-search insert into `result_ids/result_dists`, mark `is_expanded=0`,
     validity-bit=valid). Standard `ef`-capacity eviction (worst tail drops).
   - `is_filtered(id) == true` → apply the alpha gate (§5.3). If admitted, insert
     into the **invalid frontier** (`invalid_ids/invalid_dists`) **iff** its
     distance `< result_dists[meta[0]-1]` (the valid beam's current worst) — the
     GPU analog of `NeighborSetDoublePopList::insert`'s
     `nbr.distance < valid_ns_->at_search_back_dist()` guard. Frontier eviction:
     `ef_inv`-capacity, worst-distance tail drops (a filtered node only matters
     as a *near* waypoint).
2. **Parent selection each iteration (the `pop_based_on_distance` analog).** Build
   the next `search_width` parents by selecting the globally-closest **unexpanded**
   nodes across **both** beams:
   - track an unexpanded cursor for each beam (result via the existing
     `is_expanded` flags; frontier via its own `invalid_exp` flags);
   - repeatedly take the smaller-distance head of the two cursors until
     `search_width` parents are chosen or both are exhausted;
   - mark each chosen node expanded in its own beam.
   Filtered parents expand their neighbors (preserving connectivity) but never
   contribute to the top-k copy-out. This mirrors CPU `has_next()` continuing
   while `invalid_ns_` has a candidate closer than the valid beam's back.
3. **Termination.** Stop when no unexpanded node in either beam is closer than the
   valid beam's worst (matches CPU's `has_next()`), or `max_iterations` is hit.

This cross-beam selection is the most intricate part of the kernel and must be
covered by the racecheck run and the recall gate (§9). If it proves too costly at
large `ef_inv`, the graceful degrade is the BF path (§5.4), never a silent recall
drop.

### 5.4 Brute-force fallback kernel (§3.3 parity)
Add a device brute-force top-k over live rows (a distance kernel that skips
`is_filtered` ids + a top-k selection). GPU BF is embarrassingly parallel and
fast for a single segment. It writes to the **same** `d_neighbors` /
`d_distances` scratch buffers as the graph kernel (M1), using the **same** metric
helpers (L2 / negated-IP / cosine via `d_inv_norms`) so scores are byte-identical
to the graph path and to CPU.
- **Up-front trigger** (host, in knowhere `Search()`): if
  `filtered >= 0.93*N` or `k >= 0.5*live`, launch BF directly and **skip the
  graph kernel entirely** (so BF and graph never write the same buffer in one
  call — resolves the M1 buffer-conflict concern). Matches
  `kHnswSearchKnnBFFilterThreshold` / `kHnswSearchBFTopkThreshold`.
- **Short-query fallback — device-side, no host round-trip (resolves I2).**
  The naive per-query design (sync → host reads valid_count → host relaunches
  selected queries) forces a graph-kernel/BF serial dependency + a mid-search
  host round-trip. Instead: the graph kernel writes each query's `valid_count`
  into a small device buffer and **atomically appends** the query index to a
  `needs_bf[]` list when `valid_count < k` (and `>= k` live rows exist). A
  **single** batched BF kernel then runs over exactly the appended queries, on
  the same stream (one extra launch, chained by stream order — no host sync
  mid-search). If `needs_bf` is empty the launch is a no-op (a 0-grid guard).
  This keeps the CPU per-query guarantee (R3) without the round-trip.
  - *Rationale over "BF-all" or "up-front-only":* BF-all wastes work when only a
    few queries are short; up-front-only with a lower threshold would run BF on
    whole batches that don't need it. The device-append variant pays BF cost
    only for the queries that actually came up short.
  - *`needs_bf_count` init:* zeroed by a `cudaMemsetAsync` on the slot stream
    before the graph launch (chained by stream order); the graph kernel
    `atomicAdd`s into it. Cheap (4 bytes) and keeps the count device-resident.
  - *`accumulated_alpha` init:* stored per query in `meta[]` and initialized to
    **`1.0f`** at kernel start (matching CPU's `workspace.accumulated_alpha = 1.0f`
    for the non-BF branch; the BF branch is handled up-front, so the kernel never
    needs the CPU `FLT_MAX` always-admit init).
- **`disable_fallback_brute_force` (M5).** Threaded as a `bool` in
  `GpuHnswSearchParams`, read by knowhere `Search()` from the same config key
  the CPU path uses; when set, the short-query fallback launch is skipped and
  short queries return padded sentinels (matching CPU).

### 5.5 Shared-memory budget (concrete, resolves I1/§5.5)
The current per-`ef` cost is **24 B/slot — six `ef`-length arrays**, verified
against source (not 12 B):
- The smem pointer layout allocates **three result arrays and three merge
  arrays**, each length `ef`: `result_ids/result_dists/is_expanded`
  (`GpuHnswSearchKernel.cuh:731-733`) **and**
  `merged_ids/merged_dists/merged_expanded` (`GpuHnswSearchKernel.cuh:739-741`).
- `calc_layer0_smem_size` sums all six (`GpuHnswSearchKernel.cuh:1109-1118`).
- The fit check divisor is explicit: `int max_ef = (smem_max − smem_overhead) / 24;`
  with the comment *"3 result arrays + 3 merge arrays = 6 × 4 = 24 bytes/slot"*
  (`GpuHnswSearch.cuh:214-219`).
- The merge is **not** in-place: `parallel_merge_into_result` writes candidates
  into the `merged_*` arrays (`:478-484`) then copies them back into `result_*`
  (`:491-493`). The `merged_*` arrays are what make the base 24 B, not 12 B.
  (The `/12` and "no merged arrays" reading cites `GpuHnswSearch.cuh:165`, which
  is the `block_size` default, not the `max_ef` calc at `:219`.)

Because validity is packed into `is_expanded` (no new result-beam array), the
result beam stays **24 B/ef**. The invalid frontier adds **12 B/ef_inv**
(`invalid_ids/invalid_dists/invalid_exp` — a frontier, not a merge target, so no
`merged_*` twin). With `ef_inv = ef` the new fit becomes:
```
max_ef ≈ (smem_max − smem_overhead) / 36        # 24 (result+merge) + 12 (invalid)
```
On an L40S (opt-in smem ≈ 100 KB; `cudaDevAttrMaxSharedMemoryPerBlockOptin`) with
~2 KB overhead this is `max_ef ≈ 2,720`; on the default 48 KB budget it is
`max_ef ≈ 1,280`. Typical `ef` (200) and every current test config sit far below
both, so **no existing configuration regresses**. The implementation must print
the recomputed `max_ef` and keep the existing clamp+warning. If `ef_inv = ef`
ever pushes a large-`ef`/large-`M` search past the budget, **degrade to the BF
path** (which needs no per-block beam smem) rather than clamp `ef` below `k`. A
cap `ef_inv = min(ef, ef_inv_cap)` is available to trade invalid-frontier depth
for smem headroom without touching the result beam.

### 5.6 Concurrency (R4)
`d_bitset` is per scratch-pool slot and **read-only** in the kernel — no new write
hazards. The existing per-slot stream + scratch isolation covers concurrent
filtered searches; the reload path (`Deserialize` under `gpu_mutex_`) is
unchanged. Re-run the p1 concurrent-search + reload racecheck with filtering on.

## 6. Knowhere changes (`src/index/hnsw/faiss_hnsw.cc`)
- `GpuHnswIndexNode::Search()`: **remove** the reject guard; compute
  `filter_ratio` / `kAlpha` / BF-trigger from `bitset`; upload the bitset to the
  slot's `d_bitset`; pass params down; select graph vs BF path.
- `HasRawData()` / `GetVectorByIds()` unchanged (still `false` /
  `not_implemented`; vector output is served from raw field data by segcore, per
  the existing Failure-Modes analysis — unaffected by filtering).
- Add the ID-space assertion test (§5.1) and dtype × filter-ratio recall tests.

## 7. faiss changes
- `faiss/gpu/impl/GpuHnswTypes.h`: `d_bitset` + `bitset_bytes` in
  `GpuHnswSearchScratch`; `ef`/`kAlpha`/filter fields in `GpuHnswSearchParams`.
- `faiss/gpu/impl/GpuHnswSearch.cuh`: bitset upload + BF-vs-graph launch; thread
  params into all specializations.
- `faiss/gpu/impl/GpuHnswSearchKernel.cuh`: `is_filtered`, two-tier beam, alpha
  gate, filtered copy-out.
- **`faiss/gpu/impl/GpuHnswBruteForce.cuh` (NEW file):** the BF top-k kernel +
  the device `needs_bf[]` append/short-query path (§5.4). Because it is a new
  file, the vendoring checklist must add it to the knowhere GPU source list
  (`faiss_gpu_hnsw` target in `cmake/libs/libfaiss.cmake`, the sole GPU
  compilation path documented in the delta-5 CMake comment block) — it is **not**
  picked up by the `FAISS_SRCS` glob, which excludes `faiss/gpu/*` (M6).
- `faiss/gpu/GpuIndexHNSW.{h,cu}`: extend `searchHost` / `searchHostInt8` to
  accept the bitset + params (incl. `disable_fallback_brute_force`).
- Re-vendor the changed/new GPU files into `knowhere/thirdparty/faiss`
  byte-identically (`cmp`-verified), as with every prior GPU change.

## 8. Milvus changes (docs/tests only — no logic)
- Flip the ⚠️ filtered-search callout and the **Failure Modes** row in
  [20260619-gpu-hnsw.md](20260619-gpu-hnsw.md) and the limitations note in
  [20260711-gpu-hnsw-ocq-removal.md](20260711-gpu-hnsw-ocq-removal.md) from
  "not supported" to "supported (CPU-parity; BF fallback at high filter ratio)".
- Flip the delete-then-query expectation in the `idx_gpu_hnsw*` Python specs from
  expecting `invalid_args` to expecting **deleted rows absent + non-deleted rows
  present** (the same assertion the CPU-HNSW baseline uses).
- Confirm (grep) no Milvus-side heuristic assumes GPU segments are delete-free
  (compaction / routing). Expected: none.

## 9. Testing & validation gates (all must pass before deploy)
- **G-ID:** GPU id == row offset == bitset index (§5.1) — blocking correctness.
- **Recall parity:** GPU vs CPU HNSW on the same data+bitset, delete ratios
  {0, 10, 50, 90, 95, 99}%, all 5 dtypes, L2/IP/COSINE. Filtered ids must never
  appear; recall on survivors within tolerance of CPU.
- **k-guarantee:** every query returns `k` valid results when `>= k` live rows
  exist (exercises the BF fallback).
- **Sanitizers:** memcheck / initcheck / racecheck (incl. the p1 concurrent +
  reload test with filtering on).
- **Milvus e2e:** create GPU_HNSW collection → seal + GPU-index → delete keys in
  the sealed segment → query → deleted rows absent (was `invalid_args`), against
  the CPU-HNSW baseline for side-by-side parity.
- **Perf:** measure the filtered-search throughput/recall vs the append-only
  baseline (v87) so the alpha-gate / BF-fallback cost is characterized.

## 10. Phasing, effort, risk
Single phase (parity is the requirement — no "v1 copy-out-only" shortcut, since
that fails R2/R3). Rough size **L–XL**, dominated by the two-tier beam +
shared-memory rework, the BF-fallback kernel, and the full recall/sanitizer gate
across 5 dtypes × delete ratios.

**Risks**
- Invalidates the validated v87 binary ⇒ full rebuild + re-gate + new immutable
  candidate tag (**v88**). Do not fold into v87.
- Shared-memory pressure may cap usable `ef` at high M (§5.5) — mitigated by
  degrading to BF rather than clamping below `k`.
- ID-space assumption (§5.1) is load-bearing — proven by G-ID before anything
  else.
- Alpha-gate tuning: `kAlpha = filter_ratio*0.7` and the 0.93/0.5 thresholds are
  copied verbatim from CPU for parity; revisit only with data.

## 11. Rollout
Land faiss + knowhere on `gpu-hnsw-faiss`, re-vendor, build immutable
`gpu-hnsw-v88`, run the full gate above, deploy-verify on `mpd_v2` with a
delete workload, benchmark vs v87, then scale to 0 / reset to the v83 baseline.
v84-r2 remains production; v88 becomes the filtered-search candidate.
