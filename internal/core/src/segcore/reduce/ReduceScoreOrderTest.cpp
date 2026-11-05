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

#include <gtest/gtest.h>

#include <cmath>
#include <limits>

#include "segcore/reduce/Reduce.h"
#include "segcore/reduce/StreamReduce.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

class ScoreOrderReduceHelper : public ReduceHelper {
 public:
    using ReduceHelper::ReduceHelper;
    using ReduceHelper::ReduceResultData;
    using ReduceHelper::SortEqualScoresByPks;
};

class ScoreOrderStreamReducerHelper : public StreamReducerHelper {
 public:
    using StreamReducerHelper::InitializeReduceRecords;
    using StreamReducerHelper::ReduceResultData;
    using StreamReducerHelper::StreamReducerHelper;
};

PkType
MakeScoreOrderPk(int64_t id, bool string_pk) {
    if (string_pk) {
        return std::to_string(id);
    }
    return id;
}

}  // namespace

TEST(ReduceScoreOrder, CloseScoresKeepRowAssociation) {
    const float low = 0.042794593f;
    const float high =
        std::nextafter(low, std::numeric_limits<float>::infinity());
    ASSERT_GT(high, low);

    for (bool string_pk : {false, true}) {
        SCOPED_TRACE(string_pk);
        SearchResult sr;
        sr.total_nq_ = 2;
        sr.unity_topK_ = 4;
        // The second query is the L2 example after score sign normalization.
        sr.distances_ = {high, high, low, low, 0.0f, -1e-10f};
        sr.seg_offsets_ = {40, 30, 20, 10, 60, 50};
        sr.element_level_ = true;
        sr.element_indices_ = {4, 3, 2, 1, 6, 5};
        sr.topk_per_nq_prefix_sum_ = {0, 4, 6};
        for (int64_t id : {7176, 7175, 5990, 5989, 2, 1}) {
            sr.primary_keys_.push_back(MakeScoreOrderPk(id, string_pk));
        }
        sr.group_by_values_.emplace();
        for (int64_t value : {4, 3, 2, 1, 6, 5}) {
            sr.group_by_values_->emplace_back(std::in_place, value);
        }

        std::vector<SearchResult*> results{&sr};
        int64_t nqs[] = {2};
        int64_t topks[] = {4};
        ScoreOrderReduceHelper helper(results, nullptr, nqs, topks, 1, nullptr);
        helper.SortEqualScoresByPks();

        std::vector<PkType> expected_pks;
        for (int64_t id : {7175, 7176, 5989, 5990, 2, 1}) {
            expected_pks.push_back(MakeScoreOrderPk(id, string_pk));
        }
        EXPECT_EQ(sr.primary_keys_, expected_pks);
        EXPECT_EQ(sr.seg_offsets_,
                  std::vector<int64_t>({30, 40, 10, 20, 60, 50}));
        EXPECT_EQ(sr.element_indices_,
                  std::vector<int32_t>({3, 4, 1, 2, 6, 5}));
        // Exact comparisons catch adjacent float32 scores that EXPECT_FLOAT_EQ
        // would accept as equal.
        EXPECT_EQ(sr.distances_,
                  std::vector<float>({high, high, low, low, 0.0f, -1e-10f}));
        const std::vector<int64_t> expected_groups{3, 4, 1, 2, 6, 5};
        for (size_t i = 0; i < expected_groups.size(); ++i) {
            EXPECT_EQ(std::get<int64_t>(sr.group_by_values_->at(i).value()),
                      expected_groups[i]);
        }
    }
}

TEST(ReduceScoreOrder, EqualInfiniteScores) {
    SearchResult sr;
    const float inf = std::numeric_limits<float>::infinity();
    sr.total_nq_ = 2;
    sr.unity_topK_ = 2;
    sr.distances_ = {inf, inf, -inf, -inf};
    sr.primary_keys_ = {int64_t(2), int64_t(1), int64_t(4), int64_t(3)};
    sr.seg_offsets_ = {20, 10, 40, 30};
    sr.topk_per_nq_prefix_sum_ = {0, 2, 4};
    std::vector<SearchResult*> results{&sr};
    int64_t nqs[] = {2};
    int64_t topks[] = {2};
    ScoreOrderReduceHelper helper(results, nullptr, nqs, topks, 1, nullptr);

    helper.SortEqualScoresByPks();

    EXPECT_EQ(
        sr.primary_keys_,
        std::vector<PkType>({int64_t(1), int64_t(2), int64_t(3), int64_t(4)}));
    EXPECT_EQ(sr.seg_offsets_, std::vector<int64_t>({10, 20, 30, 40}));
}

TEST(ReduceScoreOrder, CrossSegmentTopOne) {
    const float low = 0.042794593f;
    const float high =
        std::nextafter(low, std::numeric_limits<float>::infinity());
    const float inf = std::numeric_limits<float>::infinity();
    for (const auto& scores : {std::pair{low, high},
                               std::pair{-high, -low},
                               std::pair{-1e-10f, 0.0f},
                               std::pair{0.5f, 0.5f},
                               std::pair{inf, inf},
                               std::pair{-inf, -inf}}) {
        for (bool string_pk : {false, true}) {
            for (bool reverse_inputs : {false, true}) {
                for (bool streaming : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << scores.first << ", " << scores.second
                                 << ", string_pk=" << string_pk
                                 << ", reverse=" << reverse_inputs
                                 << ", streaming=" << streaming);
                    SearchResult first;
                    SearchResult second;
                    first.total_nq_ = second.total_nq_ = 1;
                    first.unity_topK_ = second.unity_topK_ = 1;
                    first.topk_per_nq_prefix_sum_ = {0, 1};
                    second.topk_per_nq_prefix_sum_ = {0, 1};
                    first.primary_keys_ = {MakeScoreOrderPk(1, string_pk)};
                    second.primary_keys_ = {MakeScoreOrderPk(2, string_pk)};
                    first.distances_ = {scores.first};
                    second.distances_ = {scores.second};
                    first.seg_offsets_ = {10};
                    second.seg_offsets_ = {20};
                    std::vector<SearchResult*> results{&first, &second};
                    if (reverse_inputs) {
                        std::reverse(results.begin(), results.end());
                    }
                    int64_t nqs[] = {1};
                    int64_t topks[] = {1};
                    if (streaming) {
                        ScoreOrderStreamReducerHelper helper(
                            nullptr, nqs, topks, 1);
                        helper.SetSearchResultsToMerge(results);
                        helper.InitializeReduceRecords();
                        helper.ReduceResultData();
                    } else {
                        ScoreOrderReduceHelper helper(
                            results, nullptr, nqs, topks, 1, nullptr);
                        helper.ReduceResultData();
                    }
                    // Distinct scores prefer the higher score even when its PK
                    // is larger; exact ties retain the smaller-PK tie-break.
                    const bool second_wins = scores.second > scores.first;
                    EXPECT_EQ(first.result_offsets_,
                              second_wins ? std::vector<int64_t>{}
                                          : std::vector<int64_t>{0});
                    EXPECT_EQ(second.result_offsets_,
                              second_wins ? std::vector<int64_t>{0}
                                          : std::vector<int64_t>{});
                }
            }
        }
    }
}
