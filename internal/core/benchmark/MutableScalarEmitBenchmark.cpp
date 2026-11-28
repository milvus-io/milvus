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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "bitset/detail/platform/dynamic.h"

namespace {

constexpr uint32_t kDefaultRows = 10'000'000;

// TargetBitmap's bitset, policy, proxy and word layout. Only the backing
// allocator differs (std::vector instead of folly::fbvector); allocation and
// clearing are outside timing, as in SortedInt64LookupBenchmark's standalone
// setup. No Milvus runtime or SIMD dispatch is needed by these two kernels.
using Bitmap = milvus::bitset::Bitset<
    milvus::bitset::detail::VectorizedElementWiseBitsetPolicy<
        uint64_t,
        milvus::bitset::detail::VectorizedDynamic>,
    std::vector<uint8_t>,
    true>;

struct Input {
    uint32_t rows;
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> words;
};

void
EmitOffsets(const Input& input, Bitmap& result) {
    for (const auto offset : input.offsets) {
        result[offset] = true;
    }
}

void
EmitWords(const Input& input, Bitmap& result) {
    for (size_t i = 0; i < input.words.size(); ++i) {
        result.data()[i] |= input.words[i];
    }
}

void
Verify(const Input& input, const Bitmap& result) {
    if (!std::equal(input.words.begin(), input.words.end(), result.data())) {
        throw std::runtime_error("emission membership mismatch");
    }
}

std::shared_ptr<const Input>
MakeInput(uint32_t rows,
          uint32_t count,
          bool contiguous,
          const std::vector<uint32_t>& shuffled) {
    auto input = std::make_shared<Input>();
    input->rows = rows;
    input->offsets.resize(count);
    if (contiguous || count == rows) {
        // A contiguous suffix keeps max_offset and word coverage identical to
        // the uniformly scattered case, without changing the emission kernel.
        std::iota(input->offsets.begin(), input->offsets.end(), rows - count);
    } else {
        std::copy_n(shuffled.begin(), count - 1, input->offsets.begin());
        input->offsets.back() = rows - 1;
        std::sort(input->offsets.begin(), input->offsets.end());
    }
    input->words.resize((static_cast<size_t>(rows) + 63) / 64, 0);
    for (auto offset : input->offsets) {
        input->words[offset / 64] |= uint64_t{1} << (offset % 64);
    }
    return input;
}

template <bool Words>
void
Run(benchmark::State& state, const Input& input, size_t inner_repeats) {
    Bitmap result(input.rows, false);
    auto emit = [&] {
        if constexpr (Words) {
            EmitWords(input, result);
        } else {
            EmitOffsets(input, result);
        }
    };
    emit();
    Verify(input, result);
    // Explicit warmup. The test measures repeated warm-cache emission, not
    // first touch, tree lookup, bitmap construction, or reader concurrency.
    for (int i = 0; i < 32; ++i) {
        std::fill_n(result.data(), input.words.size(), uint64_t{0});
        emit();
        benchmark::ClobberMemory();
    }
    for (auto _ : state) {
        std::fill_n(result.data(), input.words.size(), uint64_t{0});
        benchmark::DoNotOptimize(result.data());
        benchmark::ClobberMemory();
        const auto begin = std::chrono::steady_clock::now();
        // Amortize clock overhead for very small domains. Reapply the same
        // membership without clearing inside this interval: both kernels
        // unconditionally read/modify/write, regardless of the existing bits.
        // The barrier preserves every emission, even though OR is idempotent.
        for (size_t i = 0; i < inner_repeats; ++i) {
            emit();
            benchmark::ClobberMemory();
        }
        const auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
            std::chrono::duration<double>(end - begin).count() / inner_repeats);
    }
    Verify(input, result);
    state.counters["offset_count"] = input.offsets.size();
    state.counters["density"] = double(input.offsets.size()) / input.rows;
    state.counters["row_count"] = input.rows;
    state.counters["word_count"] = input.words.size();
    state.counters["inner_repeats"] = inner_repeats;
    state.counters["offset_bytes"] = input.offsets.size() * sizeof(uint32_t);
    state.counters["bitmap_bytes"] = input.words.size() * sizeof(uint64_t);
}

void
TimerFloor(benchmark::State& state, size_t inner_repeats) {
    for (auto _ : state) {
        const auto begin = std::chrono::steady_clock::now();
        for (size_t i = 0; i < inner_repeats; ++i) {
            benchmark::ClobberMemory();
        }
        const auto end = std::chrono::steady_clock::now();
        state.SetIterationTime(
            std::chrono::duration<double>(end - begin).count() / inner_repeats);
    }
}

}  // namespace

int
main(int argc, char** argv) {
    try {
        uint32_t rows = kDefaultRows;
        size_t inner_repeats = 1;
        std::vector<uint32_t> counts{
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};
        std::string distribution = "uniform";
        // Strip our options before passing standard options to Benchmark.
        int next = 1;
        for (int i = 1; i < argc; ++i) {
            const std::string arg(argv[i]);
            if (arg.starts_with("--counts=")) {
                counts.clear();
                auto text = arg.substr(9);
                while (!text.empty()) {
                    const auto comma = text.find(',');
                    const auto part = text.substr(0, comma);
                    size_t consumed = 0;
                    const auto count = std::stoull(part, &consumed);
                    if (consumed != part.size() || count == 0 ||
                        count > std::numeric_limits<uint32_t>::max()) {
                        throw std::invalid_argument(
                            "count must be a positive uint32");
                    }
                    counts.push_back(static_cast<uint32_t>(count));
                    if (comma == std::string::npos)
                        break;
                    text.erase(0, comma + 1);
                }
                if (counts.empty())
                    throw std::invalid_argument("empty counts");
            } else if (arg.starts_with("--distribution=")) {
                distribution = arg.substr(15);
            } else if (arg.starts_with("--rows=")) {
                const auto part = arg.substr(7);
                size_t consumed = 0;
                const auto value = std::stoull(part, &consumed);
                if (consumed != part.size() || value == 0 ||
                    value > std::numeric_limits<uint32_t>::max()) {
                    throw std::invalid_argument(
                        "rows must be a positive uint32");
                }
                rows = static_cast<uint32_t>(value);
            } else if (arg.starts_with("--inner-repeats=")) {
                const auto part = arg.substr(16);
                size_t consumed = 0;
                const auto value = std::stoull(part, &consumed);
                if (consumed != part.size() || value == 0 || value > 1000000) {
                    throw std::invalid_argument(
                        "inner repeats must be in [1, 1000000]");
                }
                inner_repeats = value;
            } else {
                argv[next++] = argv[i];
            }
        }
        argc = next;
        argv[argc] = nullptr;
        if (distribution != "uniform" && distribution != "contiguous") {
            throw std::invalid_argument(
                "distribution must be uniform or contiguous");
        }
        for (auto count : counts) {
            if (count > rows)
                throw std::invalid_argument("offset count exceeds rows");
        }
        benchmark::Initialize(&argc, argv);
        if (benchmark::ReportUnrecognizedArguments(argc, argv))
            return 1;
        std::vector<uint32_t> shuffled;
        if (distribution == "uniform") {
            shuffled.resize(rows - 1);
            std::iota(shuffled.begin(), shuffled.end(), uint32_t{0});
            std::mt19937 random(56004);
            std::shuffle(shuffled.begin(), shuffled.end(), random);
        }
        benchmark::AddCustomContext("rows", std::to_string(rows));
        benchmark::AddCustomContext("inner_repeats",
                                    std::to_string(inner_repeats));
        benchmark::AddCustomContext("distribution", distribution);
        benchmark::AddCustomContext("offset_type", "uint32_t");
        benchmark::AddCustomContext("timing",
                                    "warm emission only; manual steady_clock");
        benchmark::RegisterBenchmark("TimerFloor",
                                     [inner_repeats](benchmark::State& state) {
                                         TimerFloor(state, inner_repeats);
                                     })
            ->UseManualTime()
            ->Iterations(10000)
            ->Unit(benchmark::kMicrosecond);
        for (const auto count : counts) {
            auto input =
                MakeInput(rows, count, distribution == "contiguous", shuffled);
            // Bound total work even for tiny kernels: reset is deliberately
            // unmeasured, so auto-calibration would otherwise run excessive resets.
            const auto iterations =
                std::clamp<int64_t>(10'000'000 / count, 32, 500);
            const auto name = distribution + "/" + std::to_string(count);
            benchmark::RegisterBenchmark(
                ("Offsets/" + name).c_str(),
                [input, inner_repeats](benchmark::State& state) {
                    Run<false>(state, *input, inner_repeats);
                })
                ->UseManualTime()
                ->Iterations(iterations)
                ->Unit(benchmark::kMicrosecond);
            benchmark::RegisterBenchmark(
                ("Words/" + name).c_str(),
                [input, inner_repeats](benchmark::State& state) {
                    Run<true>(state, *input, inner_repeats);
                })
                ->UseManualTime()
                ->Iterations(iterations)
                ->Unit(benchmark::kMicrosecond);
        }
        // Input preparation is complete; don't retain the sampling workspace.
        std::vector<uint32_t>().swap(shuffled);
        benchmark::RunSpecifiedBenchmarks();
        benchmark::Shutdown();
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
