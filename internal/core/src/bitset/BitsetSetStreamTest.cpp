// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <vector>

#include "common/Types.h"

namespace milvus {

namespace {

// Generate sorted, evenly spaced indices for repeatability and speed.
static std::vector<size_t>
GenerateSortedIndices(size_t num_bits, size_t num_set) {
    std::vector<size_t> idxs;
    idxs.reserve(num_set);
    if (num_set == 0) {
        return idxs;
    }
    const size_t step = std::max<size_t>(1, num_bits / (num_set + 1));
    size_t cur = step;
    for (size_t i = 0; i < num_set; ++i) {
        if (cur >= num_bits) {
            cur = num_bits - 1;
        }
        idxs.emplace_back(cur);
        cur += step;
    }
    std::sort(idxs.begin(), idxs.end());
    idxs.erase(std::unique(idxs.begin(), idxs.end()), idxs.end());
    return idxs;
}

}  // namespace

TEST(BitsetSetStreamTest, ComparePerformanceAndCorrectness) {
    // Problem size
    // const size_t num_bits = 1ull << 20;   // 1,048,576 bits
    // const size_t num_set = num_bits / 8;  // 12.5% sparsity

    // auto idxs = GenerateSortedIndices(num_bits, num_set);
    // ASSERT_FALSE(idxs.empty());
    // ASSERT_LT(idxs.back(), num_bits);

    const size_t num_bits = 1ull << 20;
    std::vector<uint32_t> idxs;
    for (size_t i = 0; i < num_bits; i++) {
        idxs.push_back(i);
    }
    // Single set version
    BitsetType bs_single(num_bits, false);
    auto t1 = std::chrono::high_resolution_clock::now();
    for (size_t j = 0; j < 10; j++) {
        for (size_t i = 0; i < idxs.size(); ++i) {
            bs_single.set(idxs[i], true);
        }
    }
    auto t2 = std::chrono::high_resolution_clock::now();

    BitsetType bs_stream(num_bits, false);
    auto t3 = std::chrono::high_resolution_clock::now();
    for (size_t j = 0; j < 10; j++) {
        bs_stream.set_sorted(idxs.data(), idxs.size());
    }
    auto t4 = std::chrono::high_resolution_clock::now();

    BitsetType bs_stream_overwrite(num_bits, false);
    auto t5 = std::chrono::high_resolution_clock::now();
    for (size_t j = 0; j < 10; j++) {
        bs_stream_overwrite.set_sorted(idxs.data(), idxs.size(), true);
    }
    auto t6 = std::chrono::high_resolution_clock::now();

    // Verify correctness (all should match)
    EXPECT_TRUE(bs_single == bs_stream);
    EXPECT_TRUE(bs_single == bs_stream_overwrite);

    // Print timing results (ms)
    const auto single_ms =
        std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    const auto stream_ms =
        std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();
    const auto stream_over_ms =
        std::chrono::duration_cast<std::chrono::microseconds>(t6 - t5).count();

    std::cout << "Bitset set() single: " << single_ms << " ms\n";
    std::cout << "Bitset set_stream(): " << stream_ms << " ms\n";
    std::cout << "Bitset set_stream_overwrite(): " << stream_over_ms << " ms\n";
}

}  // namespace milvus
