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
#include <vector>
#include <algorithm>
#include <common/Types.h>
#include <folly/FBVector.h>

static void
apply_hits(milvus::BitsetType& bitset, std::vector<size_t>& hits, bool v) {
    for (auto v : hits) {
        bitset[v] = v;
    }
}

static void
apply_hits_elementwise(milvus::BitsetType& bitset,
                       std::vector<size_t>& hits,
                       bool v) {
    std::sort(hits.begin(), hits.end());
    uint64_t* data = bitset.data();
    size_t j = 0;
    while (j < hits.size()) {
        size_t index = hits[j];
        size_t word_idx = index / 64;
        uint64_t mask = 0;
        do {
            mask |= (1ULL << (hits[j] % 64));
            ++j;
        } while (j < hits.size() && (hits[j] / 64) == word_idx);

        if (v) {
            data[word_idx] |= mask;
        } else {
            data[word_idx] &= ~mask;
        }
    }
}

static void
BM_BITSET_APPLYHITS_BRUTEFORCE(benchmark::State& stats) {
    auto hits = std::vector<size_t>(600000);
    for (size_t i = 0; i < 600000; i++) {
        hits.emplace_back(i);
    }

    for (auto _ : stats) {
        milvus::BitsetType bitset(600000);
        apply_hits(bitset, hits, true);
    }
}

BENCHMARK(BM_BITSET_APPLYHITS_BRUTEFORCE);

static void
BM_BITSET_APPLYHITS_ELEMENTWISE(benchmark::State& stats) {
    auto hits = std::vector<size_t>(600000);
    for (size_t i = 0; i < 600000; i++) {
        hits.emplace_back(i);
    }

    for (auto _ : stats) {
        milvus::BitsetType bitset(600000);
        apply_hits_elementwise(bitset, hits, true);
    }
}

BENCHMARK(BM_BITSET_APPLYHITS_ELEMENTWISE);
