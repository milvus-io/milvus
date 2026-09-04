// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <array>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include <benchmark/benchmark.h>

#include "exec/expression/JsonNumberComparison.h"

namespace milvus::exec {
namespace {

using LookupStrategy = JsonNumberLookupStrategy;

enum class ProbePattern {
    MISS,
    HIT_FIRST,
    HIT_LAST,
};

std::vector<proto::plan::GenericValue>
MakeAlternatingCandidates(size_t count) {
    std::vector<proto::plan::GenericValue> candidates(count);
    for (size_t i = 0; i < count; ++i) {
        if (i % 2 == 0) {
            candidates[i].set_int64_val(static_cast<int64_t>(i));
        } else {
            candidates[i].set_float_val(static_cast<double>(i) + 0.5);
        }
    }
    return candidates;
}

std::vector<proto::plan::GenericValue>
MakeEquivalentCandidates(size_t count) {
    std::vector<proto::plan::GenericValue> candidates(count);
    for (size_t i = 0; i < count; ++i) {
        if (i % 2 == 0) {
            candidates[i].set_int64_val(2);
        } else {
            candidates[i].set_float_val(2.0);
        }
    }
    return candidates;
}

template <typename T>
T
MakeProbe(ProbePattern pattern, size_t candidate_count) {
    if (pattern == ProbePattern::MISS) {
        if constexpr (std::is_same_v<T, int64_t>) {
            return 1'000'000;
        } else {
            return 1'000'000.25;
        }
    }
    if (pattern == ProbePattern::HIT_FIRST) {
        return T{0};
    }

    size_t last_position = candidate_count - 1;
    if constexpr (std::is_same_v<T, int64_t>) {
        // Integer candidates occupy even positions. For an even candidate
        // count, probe the final integer rather than truncating the trailing
        // x.5 double and accidentally benchmarking a miss.
        if (last_position % 2 != 0) {
            --last_position;
        }
    }
    if (last_position % 2 == 0) {
        return static_cast<T>(last_position);
    }
    return static_cast<T>(static_cast<double>(last_position) + 0.5);
}

template <typename Matcher, LookupStrategy Strategy>
void
RunMatcherConstruction(benchmark::State& state) {
    auto candidates =
        MakeAlternatingCandidates(static_cast<size_t>(state.range(0)));
    for (auto _ : state) {
        auto matcher = std::make_unique<Matcher>(candidates, Strategy);
        benchmark::DoNotOptimize(matcher->lookup_strategy());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * state.range(0));
}

template <LookupStrategy Strategy, typename T, ProbePattern Pattern>
void
RunMatchesAny(benchmark::State& state) {
    const auto candidate_count = static_cast<size_t>(state.range(0));
    auto candidates = MakeAlternatingCandidates(candidate_count);
    JsonNumberMembershipMatcher matcher(candidates, Strategy);
    const auto probe = MakeProbe<T>(Pattern, candidate_count);

    for (auto _ : state) {
        benchmark::DoNotOptimize(matcher.MatchesAny(probe));
    }
    state.SetItemsProcessed(state.iterations());
}

template <LookupStrategy Strategy, typename T, ProbePattern Pattern>
void
RunVisitMatchingPositions(benchmark::State& state) {
    const auto candidate_count = static_cast<size_t>(state.range(0));
    auto candidates = MakeAlternatingCandidates(candidate_count);
    JsonNumberCandidatePositionIndex position_index(candidates, Strategy);
    const auto probe = MakeProbe<T>(Pattern, candidate_count);

    for (auto _ : state) {
        size_t matched_position_count = 0;
        position_index.VisitMatchingPositions(
            probe, [&](size_t candidate_position) {
                benchmark::DoNotOptimize(candidate_position);
                ++matched_position_count;
                return false;
            });
        benchmark::DoNotOptimize(matched_position_count);
    }
    state.SetItemsProcessed(state.iterations());
}

template <LookupStrategy Strategy>
void
RunVisitEquivalentPositions(benchmark::State& state) {
    const auto candidate_count = static_cast<size_t>(state.range(0));
    auto candidates = MakeEquivalentCandidates(candidate_count);
    JsonNumberCandidatePositionIndex position_index(candidates, Strategy);

    for (auto _ : state) {
        size_t matched_position_count = 0;
        position_index.VisitMatchingPositions(
            int64_t{2}, [&](size_t candidate_position) {
                benchmark::DoNotOptimize(candidate_position);
                ++matched_position_count;
                return false;
            });
        benchmark::DoNotOptimize(matched_position_count);
    }
    state.SetItemsProcessed(state.iterations() *
                            static_cast<int64_t>(candidate_count));
}

void
ApplyCandidateCounts(benchmark::internal::Benchmark* benchmark) {
    // Include both sides of the current automatic-strategy boundary.
    for (auto count :
         std::array<int64_t, 12>{1, 2, 3, 4, 5, 7, 8, 9, 16, 32, 64, 128}) {
        benchmark->Arg(count);
    }
    benchmark->ArgName("candidate_count");
}

#define JSON_NUMBER_BUILD_BENCHMARK(Name, Matcher, Strategy)              \
    void Name(benchmark::State& state) {                                  \
        RunMatcherConstruction<Matcher, LookupStrategy::Strategy>(state); \
    }                                                                     \
    BENCHMARK(Name)->Apply(ApplyCandidateCounts)

#define JSON_NUMBER_LOOKUP_BENCHMARK(Name, Runner, Strategy, Type, Pattern)   \
    void Name(benchmark::State& state) {                                      \
        Runner<LookupStrategy::Strategy, Type, ProbePattern::Pattern>(state); \
    }                                                                         \
    BENCHMARK(Name)->Apply(ApplyCandidateCounts)

#define JSON_NUMBER_EQUIVALENT_VISIT_BENCHMARK(Name, Strategy)        \
    void Name(benchmark::State& state) {                              \
        RunVisitEquivalentPositions<LookupStrategy::Strategy>(state); \
    }                                                                 \
    BENCHMARK(Name)->Apply(ApplyCandidateCounts)

JSON_NUMBER_BUILD_BENCHMARK(BM_BuildMembershipLinearScan,
                            JsonNumberMembershipMatcher,
                            LINEAR_SCAN);
JSON_NUMBER_BUILD_BENCHMARK(BM_BuildMembershipHashLookup,
                            JsonNumberMembershipMatcher,
                            HASH_LOOKUP);
JSON_NUMBER_BUILD_BENCHMARK(BM_BuildMembershipAutomaticStrategy,
                            JsonNumberMembershipMatcher,
                            AUTO);
JSON_NUMBER_BUILD_BENCHMARK(BM_BuildPositionIndexLinearScan,
                            JsonNumberCandidatePositionIndex,
                            LINEAR_SCAN);
JSON_NUMBER_BUILD_BENCHMARK(BM_BuildPositionIndexHashLookup,
                            JsonNumberCandidatePositionIndex,
                            HASH_LOOKUP);
JSON_NUMBER_BUILD_BENCHMARK(BM_BuildPositionIndexAutomaticStrategy,
                            JsonNumberCandidatePositionIndex,
                            AUTO);

JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64MissLinearScan,
                             RunMatchesAny,
                             LINEAR_SCAN,
                             int64_t,
                             MISS);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64MissHashLookup,
                             RunMatchesAny,
                             HASH_LOOKUP,
                             int64_t,
                             MISS);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64FirstLinearScan,
                             RunMatchesAny,
                             LINEAR_SCAN,
                             int64_t,
                             HIT_FIRST);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64FirstHashLookup,
                             RunMatchesAny,
                             HASH_LOOKUP,
                             int64_t,
                             HIT_FIRST);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64LastLinearScan,
                             RunMatchesAny,
                             LINEAR_SCAN,
                             int64_t,
                             HIT_LAST);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyInt64LastHashLookup,
                             RunMatchesAny,
                             HASH_LOOKUP,
                             int64_t,
                             HIT_LAST);

JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyDoubleMissLinearScan,
                             RunMatchesAny,
                             LINEAR_SCAN,
                             double,
                             MISS);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyDoubleMissHashLookup,
                             RunMatchesAny,
                             HASH_LOOKUP,
                             double,
                             MISS);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyDoubleLastLinearScan,
                             RunMatchesAny,
                             LINEAR_SCAN,
                             double,
                             HIT_LAST);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_MatchesAnyDoubleLastHashLookup,
                             RunMatchesAny,
                             HASH_LOOKUP,
                             double,
                             HIT_LAST);

JSON_NUMBER_LOOKUP_BENCHMARK(BM_VisitInt64LastPositionLinearScan,
                             RunVisitMatchingPositions,
                             LINEAR_SCAN,
                             int64_t,
                             HIT_LAST);
JSON_NUMBER_LOOKUP_BENCHMARK(BM_VisitInt64LastPositionHashLookup,
                             RunVisitMatchingPositions,
                             HASH_LOOKUP,
                             int64_t,
                             HIT_LAST);
JSON_NUMBER_EQUIVALENT_VISIT_BENCHMARK(BM_VisitAllEquivalentPositionsLinearScan,
                                       LINEAR_SCAN);
JSON_NUMBER_EQUIVALENT_VISIT_BENCHMARK(BM_VisitAllEquivalentPositionsHashLookup,
                                       HASH_LOOKUP);

#undef JSON_NUMBER_BUILD_BENCHMARK
#undef JSON_NUMBER_LOOKUP_BENCHMARK
#undef JSON_NUMBER_EQUIVALENT_VISIT_BENCHMARK

}  // namespace
}  // namespace milvus::exec

BENCHMARK_MAIN();
