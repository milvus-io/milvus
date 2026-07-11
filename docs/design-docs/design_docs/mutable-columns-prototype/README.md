# Mutable Columns — prototypes

Two standalone prototypes validating claims in
[`20260709-mutable-columns.md`](../20260709-mutable-columns.md), with no
segcore dependency, so they build and run anywhere.

- **`patch_bench.cpp`** — read-path *performance*. Reproduces the segcore
  evaluation shape (32768-row chunk, `x > c` `UnaryElementFunc` kernel,
  `ProcessDataByOffsets`-style offset fix-up).
- **`fold_correctness.cpp`** — folding-algebra + MVCC *correctness*.
  Property tests comparing the design's folded representation against a
  naive full-replay reference over 200k random op sequences each.

## Build & run

```bash
xcrun clang++ -std=c++17 -O3 -march=native -DNDEBUG patch_bench.cpp -o patch_bench && ./patch_bench
xcrun clang++ -std=c++17 -O2 fold_correctness.cpp -o fold_correctness && ./fold_correctness
# Linux: replace `xcrun clang++` with `clang++` (or g++)
```

## Correctness results (`fold_correctness`)

| Property | Result |
|---|---|
| Scalar SET/INCR fold vs naive (all query ts ≥ safe_ts) | 200k/200k pass |
| Scalar watermark (fold_ts, no INCR double-apply) | 200k/200k pass |
| Array APPEND/REMOVE `(R,S)` closed form | 200k/200k pass |
| Array APPEND/POP_FRONT `(k,S)` closed form | **REFUTED, ~12% fail** |
| Array APPEND/POP_FRONT symbolic chain (base-materialized) | 200k/200k pass |

**The test found a real spec bug.** The design originally claimed
APPEND/POP_FRONT folds exactly to `(pop_count k, suffix S)`. It does not:
pop-on-empty is a no-op whose detection needs `len(base)`, unavailable
during base-free folding, so `(k,S)` drops a later-appended element (e.g.
`base=[]`, ops `POP, APPEND(7)` → naive `[7]`, `(k,S)` → `[]`). The design
was corrected: POP_FRONT has **no base-free closed form**; it is kept as a
coalesced symbolic op-chain and materialized against base at read, at column
folding, and at overlay pruning (which for POP rows reads the locally-
available base — allowed off the per-patch apply path). The exact base-free
floors are only scalar SET/INCR and array APPEND/REMOVE `(R,S)`.

## What it validates

| Claim in the design | Result |
|---|---|
| Clean chunk = zero overhead (per-chunk patched-count fast path) | **Confirmed** — 0% patched ties the base scan (4.08 µs = 4.08 µs) |
| Row-wise fix-up cost is O(#patched) | **Confirmed** — linear; 50% dirty = 9× (SET) / 14× (INCR ×3) the clean scan |
| Materialize-then-scan beats fix-up on dense chunks (~5–10% crossover) | **Refuted** — the 256KB chunk memcpy (~3.1 µs, ≈78% of a scan) makes it slower at every density; row-wise fix-up is optimal |

The refutation changed the design: there is **no read-path fallback**;
dense chunks are bounded by the **folding cadence** instead. After a fold
the column reads at exactly 1× (native), so the fold threshold is the
read-cost knob. The benchmark prints the resulting sawtooth (hot column
only; every other column and vector search stays 1×):

```
fold at  5% -> read avg 1.5x/1.7x (SET/INCR), peak 2.3x/3.0x, post-fold 1.0x
fold at 10% -> read avg 2.4x/2.9x,             peak 3.4x/4.9x, post-fold 1.0x
fold at 20% -> read avg 3.4x/5.0x,             peak 5.1x/7.3x, post-fold 1.0x
```

## Caveats

- Models fixed-width scalar SET/INCR only (Phase 1). Array ops and the
  expensive-materialize case are not covered.
- Writes results to a `uint8_t` buffer, not a bit-packed `TargetBitmap`.
  Bit-packing makes the clean scan cheaper, which makes the dense-chunk
  multiplier *larger* and the memcpy penalty *relatively worse* — i.e. the
  conclusions are conservative. Confirm on the real kernel before setting
  the production fold threshold.
- Single-core, single machine (Apple M-series). Absolute numbers vary;
  the ratios are the point.
