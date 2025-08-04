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

#include <benchmark/benchmark.h>
#include <filesystem>
#include "common/File.h"
#include <common/Types.h>

static std::vector<int>
BF_SCAN_OFFSET(int n, milvus::BitsetType& bitset) {
    std::vector<int> res;
    res.reserve(3);
    for (int i = 0; i < n; i++) {
        if (!bitset[i]) {
            res.emplace_back(i);
        }
    }
    return res;
}

static void
BM_BITSET_BRUTEFORCE(benchmark::State& stats) {
    // BitsetBlockType bitset;
    milvus::BitsetType bitset(640000);
    bitset.flip();
    bitset.set(10000, false);
    bitset.set(190000, false);
    bitset.set(610000, false);

    for (auto _ : stats) {
        auto res = BF_SCAN_OFFSET(640000, bitset);
    }
}
BENCHMARK(BM_BITSET_BRUTEFORCE);

static std::vector<int>
BS_FINDFIRST(int n, milvus::BitsetType& bitset) {
    bitset.flip();
    int i = 0;
    std::vector<int> res;
    res.reserve(3);
    while (i <= n) {
        auto next = bitset.find_next(i);
        if (!next.has_value()) {
            break;
        }
        i = next.value() + 1;
        res.emplace_back(i);
    }
    bitset.flip();
    return res;
}

static void
BM_BITSET_FINDFIRST(benchmark::State& stats) {
    milvus::BitsetType bitset(640000);
    bitset.flip();
    bitset.set(10000, false);
    bitset.set(190000, false);
    bitset.set(610000, false);

    for (auto _ : stats) {
        auto res = BS_FINDFIRST(640000, bitset);
    }
}

BENCHMARK(BM_BITSET_FINDFIRST);