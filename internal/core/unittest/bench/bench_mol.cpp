// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

// MOL fingerprint pre-filter benchmark using Knowhere BruteForce RangeSearch.
//
// Measures the time of SUBSTRUCTURE screening on 2048-bit PatternFingerprints
// at 10K / 100K / 1M scale, which is the Stage-1 coarse filter in mol_contains.
//
// Build:
//   cd cmake_build_core && make mol_bench
// Run:
//   ./unittest/bench/mol_bench

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

static constexpr int32_t DIM = 2048;  // bits
static constexpr int32_t CODE_SIZE = DIM / 8;  // 256 bytes

// Generate random binary fingerprint data.
// query: single 2048-bit FP with ~25% bit density.
// base:  nrows FPs; `match_rate` fraction are forced substructure matches.
static void
GenerateData(std::vector<uint8_t>& query,
             std::vector<uint8_t>& base,
             int64_t nrows,
             double match_rate,
             uint32_t seed = 42) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint8_t> dist(0, 255);
    std::uniform_real_distribution<double> prob(0.0, 1.0);

    query.resize(CODE_SIZE);
    base.resize(nrows * CODE_SIZE);

    // ~25% bit density for query
    for (int i = 0; i < CODE_SIZE; ++i) {
        query[i] = dist(rng) & dist(rng);
    }

    for (int64_t j = 0; j < nrows; ++j) {
        uint8_t* row = base.data() + j * CODE_SIZE;
        if (prob(rng) < match_rate) {
            // Force match: row |= query bits
            for (int i = 0; i < CODE_SIZE; ++i) {
                row[i] = query[i] | dist(rng);
            }
        } else {
            for (int i = 0; i < CODE_SIZE; ++i) {
                row[i] = dist(rng);
            }
        }
    }
}

// ── BruteForce RangeSearch with SUBSTRUCTURE metric ──
// This is the exact path used by MolFunctionFilterExpr::TryMolPatternIndex().
static void
BM_BruteForce_Substructure(benchmark::State& state) {
    auto nrows = state.range(0);
    double match_rate = 0.01;

    std::vector<uint8_t> query, base;
    GenerateData(query, base, nrows, match_rate);

    auto base_ds = knowhere::GenDataSet(nrows, DIM, base.data());
    auto query_ds = knowhere::GenDataSet(1, DIM, query.data());

    knowhere::Json cfg;
    cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::SUBSTRUCTURE;
    cfg[knowhere::meta::RADIUS] = 1.0f;
    cfg[knowhere::meta::RANGE_SEARCH_K] = -1;

    knowhere::BitsetView empty_bitset;

    // warmup
    knowhere::BruteForce::RangeSearch<knowhere::bin1>(
        base_ds, query_ds, cfg, empty_bitset, nullptr);

    for (auto _ : state) {
        auto result = knowhere::BruteForce::RangeSearch<knowhere::bin1>(
            base_ds, query_ds, cfg, empty_bitset, nullptr);
        benchmark::DoNotOptimize(result);
    }

    // Report candidates found
    auto result = knowhere::BruteForce::RangeSearch<knowhere::bin1>(
        base_ds, query_ds, cfg, empty_bitset, nullptr);
    if (result.has_value()) {
        auto lims = result.value()->GetLims();
        state.counters["candidates"] = lims ? lims[1] : 0;
    }
}

BENCHMARK(BM_BruteForce_Substructure)
    ->Arg(10000)
    ->Arg(100000)
    ->Arg(1000000)
    ->Unit(benchmark::kNanosecond)
    ->Iterations(10);
