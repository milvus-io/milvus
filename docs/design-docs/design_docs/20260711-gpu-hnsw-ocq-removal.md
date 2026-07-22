# Decision Record: Remove the OCQ (Overflow Candidate Queue) from GPU HNSW

- **Date:** 2026-07-11
- **Status:** Accepted
- **Affects:** `6si/faiss` (`faiss/gpu/*`), `6si/knowhere` (vendored `thirdparty/faiss/faiss/gpu/*` + `src/index/hnsw/faiss_hnsw.cc`), `6si/milvus` design doc `20260619-gpu-hnsw.md`
- **Related PRs:** 6si/faiss#1, 6si/knowhere#2, 6si/milvus#6

## Context

The GPU HNSW search kernel shipped with an "Overflow Candidate Queue" (OCQ):
a per-query auxiliary buffer intended to hold runner-up candidates. The idea was
that when a beam-search iteration finds no unvisited neighbors to expand
(`num_parents == 0`), the walk could pull backup candidates from the overflow
queue instead of terminating early, recovering recall at a fixed `ef`. The
feature was described in the PR titles/descriptions and the design doc as the
distinguishing characteristic of the kernel.

## Problem

Code review established that the OCQ was **never functional — it was dead code**:

- `overflow_insert()` (the routine that would push runner-up candidates into the
  queue) had **no call site** anywhere in the kernel.
- `d_overflow_count[query_idx]` was only ever **set to 0** (init) and **read**
  (in the `num_parents == 0` fallback). Nothing ever incremented it.
- Therefore the fallback loop always saw an empty queue and did nothing. The
  kernel was, in effect, a plain parallel beam search.

Cost of keeping it: `overflow_factor` defaulted to `2`, so every search
allocated `nq * 2 * ef * (4+4+4)` bytes of scratch VRAM and issued a
per-search `cudaMemsetAsync` — all for a queue that was never populated. The
docs also made a correctness claim ("OCQ beam search", "a zero here would
silently drop recall") that did not reflect reality.

## Options considered

1. **Wire OCQ up** — implement `overflow_insert` into the staging/merge path so
   the queue is actually populated and consumed.
2. **Remove OCQ** — delete the overflow machinery, default to a plain beam
   search, and correct the docs. (Chosen.)

## Decision

**Remove the OCQ machinery.** Reasoning:

- **OCQ is a recall mechanism, not a speed one.** Wiring it up would *add*
  per-iteration work (maintaining a sorted overflow list) plus the VRAM scratch
  and memset it already paid for — i.e. slightly slower searches.
- **Its upside is bounded and usually small.** OCQ only does anything on
  iterations where the beam dead-ends (`num_parents == 0`) before `ef` is
  satisfied. On a well-connected HNSW graph with a reasonable `ef`, that is
  rare, so the recall benefit is typically negligible.
- **There is a simpler lever for the same goal.** The standard way to raise
  recall is to increase `ef`, which is already exposed as a search parameter.
  OCQ only helps in the narrow regime of a hard `ef` ceiling (VRAM/latency
  bound) where `ef` cannot be raised — not our situation.
- **Honesty.** Keeping non-functional code that the docs describe as active
  misleads reviewers and future maintainers.

If a future workload is proven to be `ef`-ceiling-bound and short on recall, OCQ
(or an equivalent) can be reintroduced as a real, benchmarked feature.

## Changes made

- **faiss / vendored faiss:** removed `SearchParametersGpuHNSW::overflow_factor`
  and `GpuHnswSearchParams::overflow_factor`; removed the `d_overflow_*` scratch
  fields, their allocation in `GpuHnswSearchScratch::ensure()` (signature dropped
  the `overflow_ef` parameter) and destructor frees; removed `overflow_insert()`,
  the overflow kernel parameters, the per-query overflow locals/init, and the
  dead `num_parents == 0` fallback; removed the `overflow_ef` computation and
  memset in the search host wrapper. Kernel is now a plain parallel beam search.
- **knowhere `faiss_hnsw.cc`:** dropped the explicit `gsp.overflow_factor = 2`
  in the search path (only `ef` is set now).
- **milvus design doc `20260619-gpu-hnsw.md`:** replaced "OCQ" references with
  "parallel beam-search kernel (recall tuned via `ef`)".

## Consequences

- Lower VRAM footprint per search and one fewer memset per search.
- No behavioral regression: the search path already ran with an empty (no-op)
  queue, so results are unchanged. Recall is tuned via `ef` as before.
- PR titles/descriptions and the design doc no longer claim OCQ behavior.

---

# Related Decision: Filtered search (delete / TTL / partition bitset) is unsupported

- **Status:** ~~Accepted (documented limitation; GPU-side filtering is a follow-up)~~
  **SUPERSEDED (2026-07-18)** by
  [20260718-gpu-hnsw-filtered-search.md](20260718-gpu-hnsw-filtered-search.md).
  Option 2 below (GPU-side bitset filtering) has since been implemented and
  validated at CPU-HNSW parity on GPU capacity, so the hard reject is removed and
  GPU_HNSW is no longer scoped to append-mostly / immutable collections. The
  section is retained for historical context.

## Context

Milvus passes a `BitsetView` into `Search()` to mask out rows that must not be
returned — deleted rows, TTL-expired rows, and partition/visibility filters.
Sealed segments accumulate these routinely, so on any collection with deletes a
normal query eventually carries a non-empty bitset.

`GpuHnswIndexNode::Search()` rejects a non-empty bitset with `Status::invalid_args`
(guarded on `bitset.data() != nullptr`, not `count()`, so a filter cannot slip
through when the caller omits `num_filtered_out_bits`).

## Options considered

1. **In-node CPU fallback when a bitset is present.** Rejected: the CPU index
   copy is freed immediately after the GPU upload (to save host RAM and because
   `HasRawData()` is `false`), so there is no CPU index left in the node to fall
   back to. Keeping a CPU copy alongside the GPU copy would defeat the memory
   rationale for GPU_HNSW.
2. **GPU-side bitset filtering** (mask filtered IDs inside the kernel and
   over-fetch to backfill `k`). This is the correct long-term fix but is real
   kernel work and — critically — **cannot be validated without GPU capacity**,
   which is currently unavailable. Shipping an unverified kernel behavior change
   would violate the verification gate.
3. **Silently ignore the bitset / return unfiltered results.** Rejected:
   returns deleted/expired/out-of-partition rows — a correctness violation.
4. **Reject + document the limitation, and treat GPU_HNSW as an
   append-mostly/immutable-collection index.** Chosen for now.

## Decision

> **Superseded (2026-07-18).** The original decision below was to keep the hard
> reject until GPU capacity was available. Option 2 has since been implemented and
> validated, so the reject is removed. Historical decision preserved:

~~Keep the hard reject, document it as an explicit failure mode in
`20260619-gpu-hnsw.md`, and scope GPU_HNSW/GPU_HNSW_SQ to append-mostly /
immutable collections until GPU-side bitset filtering (option 2) is implemented
and benchmarked on real GPU capacity.~~

## Follow-up

**Done (2026-07-18):** in-kernel bitset filtering with brute-force fallback
(option 2) is implemented and validated at CPU-HNSW parity across L2/IP/COSINE
and delete ratios on sealed, GPU-indexed segments. See
[20260718-gpu-hnsw-filtered-search.md](20260718-gpu-hnsw-filtered-search.md) for
the design and the delete-then-query integration tests
(`tests/python_client/testcases/indexes/test_gpu_hnsw.py`).

---

# Related Decision: GPU_HNSW_SQ is a registered alias of GPU_HNSW

- **Status:** Accepted

## Context

Milvus exposes a `GPU_HNSW_SQ` index type (constant, param spec, GPU
resource/routing handling, and python test suites), but Knowhere originally
registered only `GPU_HNSW`. A user creating `GPU_HNSW_SQ` would hit no backend.

## Decision

Register `GPU_HNSW_SQ` in Knowhere as a thin subclass of `GpuHnswIndexNode`
(`GpuHnswSQIndexNode`) that only overrides `Type()`. The GPU search path is
identical — both take a CPU-built HNSW/HNSW_SQ index and upload it to
`GpuIndexHNSW` — so no separate kernel or storage path is needed; the distinct
`Type()` keeps Milvus's configured `index_type` consistent with the loaded node.
This is additive and non-destructive (preserves Milvus's existing API/tests)
rather than dropping the type. If a distinct SQ-specific backend is ever needed,
the subclass is the extension point.

Registered for `VECTOR_FLOAT`, `VECTOR_FLOAT16`, `VECTOR_BFLOAT16` and
`VECTOR_INT8` (matching `GPU_HNSW`; see the native FP16/BF16 decision below).

## Go param checker (GPU_HNSW / GPU_HNSW_SQ)

`newGpuHnswChecker()` existed but was never registered, and `GetChecker()`
short-circuits any vec-index type to the unified `vecIndexChecker` (whose
knowhere `ValidateIndexParams` is a no-op for GPU_HNSW), so the dedicated
M/efConstruction range validation never ran. Fixed by registering the checker
for `GPU_HNSW`/`GPU_HNSW_SQ` and routing those two types to it ahead of the
unified-checker short-circuit. The checker unit tests, which previously
silently `return`ed (reported PASS) when `GetChecker` yielded nil, now `t.Skip`
honestly on non-GPU builds and a pure-Go routing test asserts the correct
checker type is returned.

# Related Decision: Native FP16 / BF16 support

## Context

GPU_HNSW initially supported only FP32 (HNSW-Flat) and INT8 (HNSW_SQ
`QT_8bit_direct_signed`). FP16/BF16 collections were rejected at the Milvus
checker (knowhere advertised only `FLOAT32|GPU` and `INT8|GPU`). INT8 already
had a *native* device path — 1 byte/element on the GPU, a dedicated `load_elem`
that up-converts to fp32 for accumulation — so FP16/BF16 were the only
low-precision float formats missing, and CPU HNSW already supports both.

Two options were weighed:
- **Decode-to-fp32 on upload:** zero CUDA kernel work (reuses the fp32 path),
  but the device buffer is fp32 — a fp16 collection would cost the *same* VRAM
  as fp32, defeating the reason to pick fp16.
- **Native low-precision storage (chosen):** mirror the INT8 precedent — store
  fp16/bf16 at 2 bytes/element on the device and up-convert per element in the
  kernel. Preserves the memory win and is consistent with how INT8 works.

## Decision

Implement **native** FP16/BF16. faiss stores fp16/bf16 HNSW_SQ codes
(`QT_fp16`/`QT_bf16`) row-major as raw IEEE half / bfloat16, `code_size = d*2`,
which are bit-compatible with CUDA `half` / `__nv_bfloat16`. The upload path
(`from_faiss_hnsw_sq` → `upload_halfwidth_dataset`) copies those bytes verbatim
to the device; the search kernel dispatches on a new `GpuHnswDatasetType` enum
(`FP32`/`INT8`/`FP16`/`BF16`) and the templated distance helpers select the
matching `load_elem` overload (`__half2float` / `__bfloat162float`). Cosine
inverse-L2-norms are computed from the fp32-decoded values and applied at search
time, mirroring the INT8 path (stored codes are not normalized). The graph-walk
algorithm is unchanged. Knowhere registers FP16/BF16 for both GPU_HNSW and
GPU_HNSW_SQ (`feature::FP16|GPU`, `feature::BF16|GPU`); the Milvus checker then
auto-accepts them through the cgo feature flags (no Go logic change).

## Verification status

The CUDA changes are **GPU-runtime validated**. FP16/BF16 storage and cosine
recall are exercised by dedicated GPU tests in both repos:

- faiss `TestGpuIndexHNSW`: `SQ_Fp16_Cosine` and `SQ_Bf16_Cosine` (recall bars
  0.85 / 0.80), alongside the fp16 L2 and int8 L2 gates (9/9 pass).
- knowhere GPU HNSW suite: FP16/BF16 deserialization + search tests, a
  brute-force cosine oracle comparison, a CUDA upload fault-injection test, and
  an FP16 P1 regression test (16 tests, 202,360 assertions, 0 failures).

Measured recall: fp16-COSINE 0.998, bf16-COSINE 0.983, int8-COSINE 0.9975,
sq8-COSINE 0.9855. End-to-end cluster eval on `milvus-gpu:gpu-hnsw-faiss-native-v1`
covers the full dtype × metric matrix (FP32/FP16/BF16/INT8 × L2/IP/COSINE),
filtered search, deletes/TTL/partition visibility, and VRAM/OOM behavior.
