// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Standalone benchmark for RandomSampleNode sampling algorithms.
// No Milvus dependencies — compile with:
//   g++ -O2 -std=c++17 -o /tmp/bench RandomSampleBench.cpp && /tmp/bench

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <numeric>
#include <random>
#include <unordered_set>
#include <vector>

// ============================================================================
// Shared: thread-local PRNG (used by NEW implementations)
// ============================================================================

std::mt19937&
GetThreadLocalGen() {
    thread_local std::mt19937 gen(std::random_device{}());
    return gen;
}

// ============================================================================
// Shared: dispatch threshold (same logic as production code)
// ============================================================================

bool
UseFloydPath(uint32_t N, float factor) {
    float epsilon = std::numeric_limits<float>::epsilon();
    return factor <= 0.02 + epsilon ||
           (N <= 10000000 && factor <= 0.045 + epsilon) ||
           (N <= 60000000 && factor <= 0.025 + epsilon);
}

// ============================================================================
// OLD implementations (baseline — per-call seeding, vector materialization)
// ============================================================================

std::vector<uint32_t>
OldHashsetSample(uint32_t N, uint32_t M, std::mt19937& gen) {
    std::uniform_int_distribution<> dis(0, N - 1);
    std::unordered_set<uint32_t> sampled;
    sampled.reserve(N);  // bug: reserves N instead of M
    while (sampled.size() < M) {
        sampled.insert(dis(gen));
    }
    return std::vector<uint32_t>(sampled.begin(), sampled.end());
}

std::vector<uint32_t>
OldStandardSample(uint32_t N, uint32_t M, std::mt19937& gen) {
    std::vector<uint32_t> inputs(N);
    std::vector<uint32_t> outputs(M);
    std::iota(inputs.begin(), inputs.end(), 0);
    std::sample(inputs.begin(), inputs.end(), outputs.begin(), M, gen);
    return outputs;
}

std::vector<uint32_t>
OldSample(uint32_t N, float factor) {
    uint32_t M = std::max(static_cast<uint32_t>(N * factor), 1u);
    std::random_device rd;
    std::mt19937 gen(rd());
    if (UseFloydPath(N, factor)) {
        return OldHashsetSample(N, M, gen);
    }
    return OldStandardSample(N, M, gen);
}

// Old source path: Sample() → vector → scatter → flip
void
OldSourcePath(std::vector<bool>& bitmap, float factor) {
    uint32_t N = bitmap.size();
    bool need_flip = true;
    float f = factor;
    if (f > 0.5) {
        need_flip = false;
        f = 1.0 - f;
    }
    auto sampled = OldSample(N, f);
    for (auto n : sampled) {
        bitmap[n] = true;
    }
    if (need_flip) {
        bitmap.flip();
    }
}

// Old filtered path: materialize positions → sample → scatter
void
OldFilteredSample(std::vector<bool>& bitmap, float factor) {
    size_t false_count = 0;
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (!bitmap[i])
            false_count++;
    }
    if (false_count == 0)
        return;

    std::vector<uint32_t> pos;
    pos.reserve(false_count);
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (!bitmap[i])
            pos.push_back(i);
    }

    std::fill(bitmap.begin(), bitmap.end(), true);
    auto sampled = OldSample(false_count, factor);
    for (auto idx : sampled) {
        bitmap[pos[idx]] = false;
    }
}

// ============================================================================
// NEW implementations (thread-local PRNG, direct bitmap writes)
// ============================================================================

std::vector<uint32_t>
NewFloydSample(uint32_t N, uint32_t M, std::mt19937& gen) {
    std::unordered_set<uint32_t> selected;
    selected.reserve(M);
    for (uint32_t j = N - M; j < N; ++j) {
        std::uniform_int_distribution<uint32_t> dis(0, j);
        uint32_t t = dis(gen);
        if (!selected.insert(t).second) {
            selected.insert(j);
        }
    }
    return std::vector<uint32_t>(selected.begin(), selected.end());
}

// New source path: direct bitmap write, thread-local PRNG, no intermediate vector
void
NewSourcePath(std::vector<bool>& bitmap, float factor) {
    uint32_t N = bitmap.size();
    bool need_flip = true;
    float f = factor;
    if (f > 0.5) {
        need_flip = false;
        f = 1.0 - f;
    }
    uint32_t M = std::max(static_cast<uint32_t>(N * f), 1u);
    auto& gen = GetThreadLocalGen();

    if (UseFloydPath(N, f)) {
        auto sampled = NewFloydSample(N, M, gen);
        for (auto n : sampled) {
            bitmap[n] = true;
        }
    } else {
        // Selection sampling: write directly into bitmap
        uint32_t remaining = N, needed = M;
        for (uint32_t i = 0; i < N && needed > 0; ++i) {
            if (std::uniform_int_distribution<uint32_t>(0, remaining - 1)(
                    gen) < needed) {
                bitmap[i] = true;
                --needed;
            }
            --remaining;
        }
    }

    if (need_flip) {
        bitmap.flip();
    }
}

// New filtered path: inline selection, thread-local PRNG
void
NewFilteredSample(std::vector<bool>& bitmap, float factor) {
    size_t false_count = 0;
    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (!bitmap[i])
            false_count++;
    }
    if (false_count == 0)
        return;

    size_t remaining = false_count;
    size_t needed =
        std::max(static_cast<size_t>(false_count * factor), static_cast<size_t>(1));
    auto& gen = GetThreadLocalGen();

    for (size_t i = 0; i < bitmap.size(); ++i) {
        if (bitmap[i])
            continue;
        if (needed > 0 &&
            std::uniform_int_distribution<size_t>(0, remaining - 1)(gen) <
                needed) {
            --needed;
        } else {
            bitmap[i] = true;
        }
        --remaining;
    }
}

// ============================================================================
// Benchmark harness
// ============================================================================

struct BenchResult {
    double mean_us;
    double min_us;
    double max_us;
};

template <typename Func>
BenchResult
Benchmark(Func&& fn, int iterations = 10) {
    double total = 0, min_v = 1e18, max_v = 0;
    for (int i = 0; i < iterations; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        fn();
        auto end = std::chrono::high_resolution_clock::now();
        double us =
            std::chrono::duration<double, std::micro>(end - start).count();
        total += us;
        min_v = std::min(min_v, us);
        max_v = std::max(max_v, us);
    }
    return {total / iterations, min_v, max_v};
}

void
PrintResult(const char* label, BenchResult r) {
    printf("  %-40s  mean=%10.0f us  min=%10.0f us  max=%10.0f us\n", label,
           r.mean_us, r.min_us, r.max_us);
}

size_t
CountFalse(const std::vector<bool>& bm) {
    size_t count = 0;
    for (size_t i = 0; i < bm.size(); ++i) {
        if (!bm[i])
            count++;
    }
    return count;
}

int
main() {
    // =================================================================
    // Source path benchmark (bitmap starts all-false, sample into it)
    // =================================================================
    printf("=== Source Path Benchmark ===\n\n");

    uint32_t Ns[] = {1000000, 10000000};
    float factors[] = {0.01f, 0.05f, 0.1f, 0.5f, 0.9f};

    for (auto N : Ns) {
        for (auto f : factors) {
            uint32_t expected_M = std::max(static_cast<uint32_t>(N * f), 1u);
            printf("N=%u, factor=%.2f (expected M=%u):\n", N, f, expected_M);

            // Correctness check
            std::vector<bool> bm_old(N, false);
            OldSourcePath(bm_old, f);
            size_t old_false = CountFalse(bm_old);

            std::vector<bool> bm_new(N, false);
            NewSourcePath(bm_new, f);
            size_t new_false = CountFalse(bm_new);

            bool old_ok = (old_false >= expected_M - 1 &&
                           old_false <= expected_M + 1);
            bool new_ok = (new_false >= expected_M - 1 &&
                           new_false <= expected_M + 1);

            auto r1 = Benchmark([&]() {
                std::vector<bool> bm(N, false);
                OldSourcePath(bm, f);
            });
            PrintResult("OLD Source", r1);

            auto r2 = Benchmark([&]() {
                std::vector<bool> bm(N, false);
                NewSourcePath(bm, f);
            });
            PrintResult("NEW Source", r2);

            double speedup = r1.mean_us / r2.mean_us;
            printf(
                "  Speedup: %.2fx  |  false: old=%zu new=%zu  count_ok: "
                "old=%s new=%s\n\n",
                speedup, old_false, new_false, old_ok ? "PASS" : "FAIL",
                new_ok ? "PASS" : "FAIL");
        }
    }

    // =================================================================
    // Filtered path benchmark
    // =================================================================
    printf("=== Filtered Path Benchmark ===\n\n");
    uint32_t total = 1000000;
    uint32_t false_counts[] = {100000, 500000, 900000};
    float filter_factors[] = {0.01f, 0.1f, 0.5f};

    for (auto K : false_counts) {
        for (auto f : filter_factors) {
            uint32_t expected_sampled =
                std::max(static_cast<uint32_t>(K * f), 1u);
            printf(
                "Total=%u, K(false)=%u, factor=%.2f (expect ~%u sampled):\n",
                total, K, f, expected_sampled);

            // Pre-build the template bitmap once (outside timing)
            std::vector<bool> template_bm(total, true);
            {
                std::mt19937 rng(42);
                std::vector<uint32_t> indices(total);
                std::iota(indices.begin(), indices.end(), 0);
                std::shuffle(indices.begin(), indices.end(), rng);
                for (uint32_t i = 0; i < K; ++i) {
                    template_bm[indices[i]] = false;
                }
            }

            // Correctness check
            auto bm_old = template_bm;
            OldFilteredSample(bm_old, f);
            size_t old_sampled = CountFalse(bm_old);
            auto bm_new = template_bm;
            NewFilteredSample(bm_new, f);
            size_t new_sampled = CountFalse(bm_new);
            bool old_ok = (old_sampled >= expected_sampled - 1 &&
                           old_sampled <= expected_sampled + 1);
            bool new_ok = (new_sampled >= expected_sampled - 1 &&
                           new_sampled <= expected_sampled + 1);

            auto r1 = Benchmark([&]() {
                auto bm = template_bm;
                OldFilteredSample(bm, f);
            });
            PrintResult("OLD Filtered", r1);

            auto r2 = Benchmark([&]() {
                auto bm = template_bm;
                NewFilteredSample(bm, f);
            });
            PrintResult("NEW Filtered", r2);

            double speedup = r1.mean_us / r2.mean_us;
            printf(
                "  Speedup: %.2fx  |  sampled: old=%zu new=%zu  count_ok: "
                "old=%s new=%s\n\n",
                speedup, old_sampled, new_sampled, old_ok ? "PASS" : "FAIL",
                new_ok ? "PASS" : "FAIL");
        }
    }

    return 0;
}
