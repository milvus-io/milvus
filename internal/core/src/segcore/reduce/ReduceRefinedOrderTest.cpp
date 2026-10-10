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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

#include "common/QueryResult.h"
#include "segcore/reduce/Reduce.h"

namespace {

// Expose ApplyRefinedOrderForOneNQ for direct unit testing.
class TestableReduceHelper : public milvus::segcore::ReduceHelper {
 public:
    using milvus::segcore::ReduceHelper::ApplyRefinedOrderForOneNQ;
    using milvus::segcore::ReduceHelper::ReduceHelper;
};

// Sort indices descending by new_distances, matching RefineOneSegment.
std::vector<size_t>
SortIndicesByNewDistances(const std::vector<float>& new_distances) {
    std::vector<size_t> indices(new_distances.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::sort(indices.begin(), indices.end(), [&](size_t a, size_t b) {
        return new_distances[a] > new_distances[b];
    });
    return indices;
}

// Drive ApplyRefinedOrderForOneNQ on a synthetic single-NQ SearchResult
// and assert distances_/seg_offsets_/element_indices_ end up sorted
// descending by the refined distances.
void
RunApplyRefinedOrderCheck(const std::vector<float>& coarse_distances,
                          const std::vector<int64_t>& initial_offsets,
                          const std::vector<float>& new_distances,
                          bool element_level = false) {
    ASSERT_EQ(coarse_distances.size(), initial_offsets.size());
    ASSERT_EQ(coarse_distances.size(), new_distances.size());

    milvus::SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = static_cast<int64_t>(coarse_distances.size());
    sr.total_data_cnt_ = 0;
    sr.segment_ = nullptr;
    sr.distances_ = coarse_distances;
    sr.seg_offsets_ = initial_offsets;
    sr.element_level_ = element_level;
    std::vector<int32_t> initial_elem_indices;
    if (element_level) {
        initial_elem_indices.resize(coarse_distances.size());
        for (size_t i = 0; i < initial_elem_indices.size(); ++i) {
            initial_elem_indices[i] = static_cast<int32_t>(100 + i);
        }
        sr.element_indices_ = initial_elem_indices;
    }

    std::vector<milvus::SearchResult*> srs = {&sr};
    int64_t slice_nq = 1;
    int64_t slice_topk = sr.unity_topK_;
    TestableReduceHelper helper(srs,
                                /*plan=*/nullptr,
                                /*placeholder_group=*/nullptr,
                                &slice_nq,
                                &slice_topk,
                                /*slice_num=*/1,
                                /*trace_ctx=*/nullptr);

    auto indices = SortIndicesByNewDistances(new_distances);
    std::vector<float> expected_distances(new_distances.size());
    std::vector<int64_t> expected_offsets(initial_offsets.size());
    std::vector<int32_t> expected_elem_indices(initial_offsets.size());
    for (size_t i = 0; i < indices.size(); ++i) {
        expected_distances[i] = new_distances[indices[i]];
        expected_offsets[i] = initial_offsets[indices[i]];
        if (element_level) {
            expected_elem_indices[i] = initial_elem_indices[indices[i]];
        }
    }

    helper.ApplyRefinedOrderForOneNQ(&sr, 0, indices, new_distances);

    EXPECT_EQ(sr.distances_, expected_distances);
    EXPECT_EQ(sr.seg_offsets_, expected_offsets);
    if (element_level) {
        EXPECT_EQ(sr.element_indices_, expected_elem_indices);
    }
}

}  // namespace

// Regression: when coarse order and refined order already agree for every
// position, indices[i] == i for all i. Previous implementation skipped
// these slots, leaving coarse distances untouched.
TEST(ReduceRefinedOrderTest, AllFixedPointsGetRefinedDistances) {
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.9f, 0.5f, 0.1f},
                              /*initial_offsets=*/{10, 20, 30},
                              /*new_distances=*/{1.0f, 0.6f, 0.2f});
}

// Regression: the in-place cycle marks processed slots with
// indices[curr] = curr. The outer loop must not treat those as real
// fixed points and overwrite the value just written by the cycle.
TEST(ReduceRefinedOrderTest, TwoElementSwap) {
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.1f, 0.9f},
                              /*initial_offsets=*/{100, 200},
                              /*new_distances=*/{0.2f, 1.0f});
}

TEST(ReduceRefinedOrderTest, ThreeElementRotation) {
    // new_distances sorts to indices = [1, 2, 0] — a single 3-cycle.
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.5f, 0.9f, 0.3f},
                              /*initial_offsets=*/{1, 2, 3},
                              /*new_distances=*/{0.3f, 1.0f, 0.7f});
}

// 2-cycle plus a real fixed point in the middle.
TEST(ReduceRefinedOrderTest, ReverseOrderOddLength) {
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.1f, 0.5f, 0.9f},
                              /*initial_offsets=*/{10, 20, 30},
                              /*new_distances=*/{0.2f, 0.6f, 1.0f});
}

// Fixed point at position 0 plus a {1,2} swap — original bug left
// distances[0] as the coarse value.
TEST(ReduceRefinedOrderTest, FixedPointMixedWithCycle) {
    // indices end up as [0, 2, 1].
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.9f, 0.5f, 0.1f},
                              /*initial_offsets=*/{10, 20, 30},
                              /*new_distances=*/{1.0f, 0.2f, 0.6f});
}

TEST(ReduceRefinedOrderTest, ElementLevelFollowsPermutation) {
    RunApplyRefinedOrderCheck(/*coarse_distances=*/{0.5f, 0.9f, 0.3f},
                              /*initial_offsets=*/{1, 2, 3},
                              /*new_distances=*/{0.3f, 1.0f, 0.7f},
                              /*element_level=*/true);
}

// Two disjoint cycles of different lengths in one call.
TEST(ReduceRefinedOrderTest, MultipleDisjointCycles) {
    // indices = [3, 0, 4, 1, 2]: cycles (0 -> 3 -> 1 -> 0) and (2 -> 4 -> 2).
    RunApplyRefinedOrderCheck(
        /*coarse_distances=*/{0.9f, 0.7f, 0.5f, 0.3f, 0.1f},
        /*initial_offsets=*/{11, 22, 33, 44, 55},
        /*new_distances=*/{0.8f, 0.4f, 0.2f, 1.0f, 0.6f});
}
