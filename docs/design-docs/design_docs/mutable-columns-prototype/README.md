# Mutable Columns — read-path prototype

Standalone microbenchmark validating the overlay fix-up read path from
[`20260709-mutable-columns.md`](../20260709-mutable-columns.md). It
reproduces the segcore evaluation shape (32768-row chunk, `x > c`
`UnaryElementFunc` kernel, `ProcessDataByOffsets`-style offset fix-up) with
no segcore dependency, so it builds and runs anywhere.

## Build & run

```bash
xcrun clang++ -std=c++17 -O3 -march=native -DNDEBUG patch_bench.cpp -o patch_bench   # macOS
# or: clang++ -std=c++17 -O3 -march=native -DNDEBUG patch_bench.cpp -o patch_bench   # Linux
./patch_bench
```

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
