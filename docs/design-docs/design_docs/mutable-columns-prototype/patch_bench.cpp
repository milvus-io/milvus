// Standalone microbenchmark for the mutable-columns "overlay fix-up" read path.
//
// Faithfully reproduces the segcore evaluation shape:
//   - chunk_rows = 32768 (SegcoreConfig default)
//   - base predicate kernel:  res[i] = src[i] > val   (UnaryElementFunc, SIMD)
//   - offset fix-up kernel:    res[o] = materialize(o) > val  (ProcessDataByOffsets)
//
// It measures the three performance claims in the design doc:
//   (A) clean chunk (0% patched)  ->  ~= base scan (zero overhead)
//   (B) row-wise fix-up cost      ->  O(#patched)
//   (C) materialize-then-scan     ->  crossover vs fix-up, claimed 5-10%
//
// Two overlay op models are benchmarked:
//   SET  : version chain holds an absolute value (offline_time scenario)
//   INCR : version chain holds a delta; materialize = base[o] + sum(deltas<=ts)

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
#include <random>
#include <chrono>
#include <algorithm>
#include <unordered_map>

static constexpr int    CHUNK_ROWS = 32 * 1024;   // segcore default
static constexpr int    REPS       = 4000;        // per measurement
static constexpr int    WARMUP     = 400;

using clk = std::chrono::steady_clock;

// ---- overlay: one version-chain node. Kept tiny; SET chains are ~1 node,
// INCR chains fold to a floor + a few recent. We model chain_len nodes.
struct Node { uint64_t ts; uint8_t op; int64_t operand; }; // op 0=SET 1=INCR
enum { OP_SET = 0, OP_INCR = 1 };

struct Overlay {
    // chunk-partitioned: offset -> chain. std::unordered_map stands in for the
    // real flat hashmap; the access pattern (one lookup per dirty row) matches.
    std::unordered_map<uint32_t, std::vector<Node>> chains;
    std::vector<uint32_t> dirty; // sorted offsets present in this chunk
};

// materialize value at query_ts from base + chain (walk to last node <= ts)
static inline int64_t materialize(const int64_t* base, uint32_t off,
                                  const std::vector<Node>& chain, uint64_t qts) {
    int64_t acc = base[off];
    bool have_set = false;
    int64_t set_val = 0, incr_sum = 0;
    for (const auto& n : chain) {
        if (n.ts > qts) break;              // MVCC: ignore newer versions
        if (n.op == OP_SET) { have_set = true; set_val = n.operand; incr_sum = 0; }
        else                { incr_sum += n.operand; }
    }
    if (have_set) return set_val + incr_sum;
    return acc + incr_sum;
}

// (A)/(B): base vectorized scan, then row-wise fix-up over dirty offsets.
static uint64_t eval_fixup(const int64_t* src, int64_t val, uint8_t* res,
                           const Overlay& ov, uint64_t qts, const int64_t* base) {
    // base scan — this is the SIMD kernel clang autovectorizes
    for (int i = 0; i < CHUNK_ROWS; ++i) res[i] = (src[i] > val);
    // fix-up: only dirty offsets, one hashmap lookup + materialize each
    for (uint32_t off : ov.dirty) {
        auto it = ov.chains.find(off);
        int64_t v = materialize(base, off, it->second, qts);
        res[off] = (v > val);
    }
    uint64_t s = 0; for (int i = 0; i < CHUNK_ROWS; ++i) s += res[i];
    return s;
}

// (C): materialize whole chunk (base overlaid with current values) then SIMD scan.
static uint64_t eval_materialize_scan(const int64_t* base, int64_t val, uint8_t* res,
                                      const Overlay& ov, uint64_t qts, int64_t* scratch) {
    std::memcpy(scratch, base, CHUNK_ROWS * sizeof(int64_t));
    for (uint32_t off : ov.dirty) {
        auto it = ov.chains.find(off);
        scratch[off] = materialize(base, off, it->second, qts);
    }
    for (int i = 0; i < CHUNK_ROWS; ++i) res[i] = (scratch[i] > val);
    uint64_t s = 0; for (int i = 0; i < CHUNK_ROWS; ++i) s += res[i];
    return s;
}

template <class F>
static double bench(F&& f) {
    volatile uint64_t sink = 0;
    for (int r = 0; r < WARMUP; ++r) sink += f();
    auto t0 = clk::now();
    for (int r = 0; r < REPS; ++r) sink += f();
    auto t1 = clk::now();
    (void)sink;
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / REPS;
}

// variant of fix-up that writes into scratch (materialize dirty only, no memcpy)
// then does NOT rescan — reuses base-scan bits and scatters. This is the
// "smart" fix-up. Included to prove memcpy is what kills mat+scan.
static uint64_t eval_fixup_scatter(const int64_t* src, int64_t val, uint8_t* res,
                                   const Overlay& ov, uint64_t qts, const int64_t* base,
                                   int chain_reps) {
    for (int i = 0; i < CHUNK_ROWS; ++i) res[i] = (src[i] > val);
    for (uint32_t off : ov.dirty) {
        auto it = ov.chains.find(off);
        int64_t v = 0;
        for (int r = 0; r < chain_reps; ++r) v += materialize(base, off, it->second, qts);
        res[off] = (v > val);
    }
    uint64_t s = 0; for (int i = 0; i < CHUNK_ROWS; ++i) s += res[i];
    return s;
}

int main() {
    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<int64_t> vdist(0, 1'000'000);

    std::vector<int64_t> base(CHUNK_ROWS), src(CHUNK_ROWS), scratch(CHUNK_ROWS);
    for (int i = 0; i < CHUNK_ROWS; ++i) { base[i] = vdist(rng); src[i] = base[i]; }
    std::vector<uint8_t> res(CHUNK_ROWS);
    const int64_t threshold = 500'000; // ~50% selectivity
    const uint64_t qts = 1'000'000;

    // DVFS warmup: spin ~200ms of real scan work so the CPU boosts to a
    // performance core / high frequency before any measurement. Without this,
    // the first bench() calls run cold and read ~3x slow (pure artifact).
    {
        volatile uint64_t sink = 0;
        auto t0 = clk::now();
        while (std::chrono::duration<double, std::milli>(clk::now() - t0).count() < 200.0) {
            for (int i = 0; i < CHUNK_ROWS; ++i) res[i] = (src[i] > threshold);
            for (int i = 0; i < CHUNK_ROWS; ++i) sink += res[i];
        }
        (void)sink;
    }

    // isolate memcpy cost of one chunk
    double memcpy_ns = bench([&]{
        std::memcpy(scratch.data(), base.data(), CHUNK_ROWS * sizeof(int64_t));
        return (uint64_t)scratch[rng() % CHUNK_ROWS];
    });
    printf("chunk memcpy (256KB) = %.0f ns   [mat+scan pays this unconditionally]\n",
           memcpy_ns);

    // fractions to sweep — focus on the fold-threshold region (0-25%)
    double fracs[] = {0.0, 0.005, 0.01, 0.025, 0.05, 0.075, 0.10, 0.15, 0.20, 0.25, 0.50};

    for (int model = 0; model < 2; ++model) {
        const char* mname = model == OP_SET ? "SET " : "INCR";
        printf("\n=== overlay op = %s | chunk=%d rows | int64 | pred: x > %ld ===\n",
               mname, CHUNK_ROWS, (long)threshold);
        printf("%-9s %-9s %12s %12s %12s   %s\n",
               "patched%", "#dirty", "base+fixup", "mat+scan", "base-only", "winner");
        printf("%-9s %-9s %12s %12s %12s\n", "", "", "(ns)", "(ns)", "(ns)");

        // Collect all measurements first; the clean-chunk base cost is defined
        // empirically as the minimum observed fix-up cost (the near-clean point),
        // which sidesteps memory-state / DVFS outliers on any single sample.
        int NF = sizeof(fracs) / sizeof(fracs[0]);
        std::vector<double> fixv(NF), matv(NF);
        std::vector<int> ndv(NF);
        for (int fi = 0; fi < NF; ++fi) {
            double frac = fracs[fi];
            int ndirty = (int)(frac * CHUNK_ROWS);
            Overlay ov;
            // pick ndirty distinct offsets
            std::vector<uint32_t> offs(CHUNK_ROWS);
            for (int i = 0; i < CHUNK_ROWS; ++i) offs[i] = i;
            std::shuffle(offs.begin(), offs.end(), rng);
            offs.resize(ndirty);
            std::sort(offs.begin(), offs.end());
            ov.dirty = offs;
            int chain_len = (model == OP_SET) ? 1 : 3; // INCR: floor + 2 recent
            for (uint32_t o : offs) {
                std::vector<Node> ch;
                if (model == OP_SET) {
                    ch.push_back({500, OP_SET, vdist(rng)});
                } else {
                    ch.push_back({300, OP_INCR, 1});  // floor
                    ch.push_back({600, OP_INCR, 2});
                    ch.push_back({900, OP_INCR, 1});
                }
                (void)chain_len;
                ov.chains.emplace(o, std::move(ch));
            }

            double fixup_ns = bench([&]{ return eval_fixup(src.data(), threshold,
                                               res.data(), ov, qts, base.data()); });
            double mat_ns = bench([&]{ return eval_materialize_scan(base.data(), threshold,
                                               res.data(), ov, qts, scratch.data()); });
            fixv[fi] = fixup_ns; matv[fi] = mat_ns; ndv[fi] = ndirty;
        }

        double base_ns = 1e18;
        for (double v : fixv) base_ns = std::min(base_ns, v);
        for (int fi = 0; fi < NF; ++fi) {
            printf("%-9.3f %-9d %12.0f %12.0f %12.0f   %5.2fx\n",
                   fracs[fi] * 100, ndv[fi], fixv[fi], matv[fi], base_ns,
                   fixv[fi] / base_ns);
        }

        // sawtooth: read density ramps 0->T uniformly then folds. Average read
        // multiplier ~= multiplier at density T/2; peak = at T. Report both.
        printf("  -- sawtooth (linear ramp to fold threshold, then fold) --\n");
        double folds[] = {0.05, 0.10, 0.20};
        for (double T : folds) {
            // measure at T/2 (avg) and T (peak)
            auto measure_at = [&](double frac)->double {
                int nd = (int)(frac * CHUNK_ROWS);
                Overlay ov; std::vector<uint32_t> o(CHUNK_ROWS);
                for (int i=0;i<CHUNK_ROWS;++i) o[i]=i;
                std::shuffle(o.begin(),o.end(),rng); o.resize(nd); std::sort(o.begin(),o.end());
                ov.dirty=o;
                for (uint32_t off:o){ std::vector<Node> ch;
                    if(model==OP_SET) ch.push_back({500,OP_SET,vdist(rng)});
                    else { ch.push_back({300,OP_INCR,1}); ch.push_back({600,OP_INCR,2}); ch.push_back({900,OP_INCR,1}); }
                    ov.chains.emplace(off,std::move(ch)); }
                if(nd==0) return base_ns;
                return bench([&]{ return eval_fixup(src.data(),threshold,res.data(),ov,qts,base.data()); });
            };
            double avg = measure_at(T/2) / base_ns;
            double peak = measure_at(T) / base_ns;
            printf("    fold at %4.0f%% -> hot-column read: avg %.2fx, peak %.2fx (post-fold 1.00x)\n",
                   T*100, avg, peak);
        }
    }
    printf("\n");
    return 0;
}
