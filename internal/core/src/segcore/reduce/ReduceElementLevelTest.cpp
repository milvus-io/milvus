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

#include "exec/operator/Utils.h"
#include "segcore/reduce/GroupReduce.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce/StreamReduce.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

class TestReduceHelper : public ReduceHelper {
 public:
    using ReduceHelper::FilterInvalidSearchResult;
    using ReduceHelper::ReduceHelper;
    using ReduceHelper::ReduceResultData;

    const std::vector<std::vector<std::vector<int64_t>>>&
    FinalSearchRecordsForTest() const {
        return final_search_records_;
    }
};

class TestGroupReduceHelper : public GroupReduceHelper {
 public:
    using GroupReduceHelper::FilterInvalidSearchResult;
    using GroupReduceHelper::GroupReduceHelper;
    using GroupReduceHelper::ReduceResultData;

    const std::vector<std::vector<std::vector<int64_t>>>&
    FinalSearchRecordsForTest() const {
        return final_search_records_;
    }
};

class TestStreamReducerHelper : public StreamReducerHelper {
 public:
    using StreamReducerHelper::InitializeReduceRecords;
    using StreamReducerHelper::ReduceResultData;
    using StreamReducerHelper::StreamReducerHelper;

    MergedSearchResult*
    MutableMergedResultForTest() {
        return merged_search_result.get();
    }
};

SearchResult
MakeElementLevelSearchResult() {
    SearchResult result;
    result.total_nq_ = 1;
    result.topk_per_nq_prefix_sum_ = {0, 4};
    result.primary_keys_ = {
        int64_t(5),
        int64_t(5),
        int64_t(5),
        int64_t(6),
    };
    result.distances_ = {0.99f, 0.98f, 0.97f, 0.96f};
    result.seg_offsets_ = {10, 10, 10, 11};
    result.element_level_ = true;
    result.element_indices_ = {0, 1, 1, 0};
    return result;
}

GroupByValueType
MakeInt64GroupValue(int64_t value) {
    return GroupByValueType(std::in_place, value);
}

}  // namespace

TEST(ReduceElementLevel, KeepsDistinctElementHitsWithSamePK) {
    auto seg0 = MakeElementLevelSearchResult();

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestReduceHelper helper(search_results,
                            nullptr,
                            slice_nqs.data(),
                            slice_topks.data(),
                            slice_nqs.size(),
                            nullptr);

    helper.ReduceResultData();

    EXPECT_EQ(seg0.result_offsets_, std::vector<int64_t>({0, 1, 2}));
    ASSERT_EQ(helper.FinalSearchRecordsForTest().size(), 1);
    ASSERT_EQ(helper.FinalSearchRecordsForTest()[0].size(), 1);
    EXPECT_EQ(helper.FinalSearchRecordsForTest()[0][0],
              std::vector<int64_t>({0, 1, 3}));
}

TEST(ReduceElementLevel, RejectsMismatchedElementIndicesBeforeFiltering) {
    auto seg0 = MakeElementLevelSearchResult();
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 4;
    seg0.element_indices_.pop_back();

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestReduceHelper helper(search_results,
                            nullptr,
                            slice_nqs.data(),
                            slice_topks.data(),
                            slice_nqs.size(),
                            nullptr);

    EXPECT_ANY_THROW(helper.FilterInvalidSearchResult(&seg0));
}

TEST(ReduceElementLevel, RejectsMismatchedElementIndicesBeforeReducing) {
    auto seg0 = MakeElementLevelSearchResult();
    seg0.element_indices_.pop_back();

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestReduceHelper helper(search_results,
                            nullptr,
                            slice_nqs.data(),
                            slice_topks.data(),
                            slice_nqs.size(),
                            nullptr);

    EXPECT_ANY_THROW(helper.ReduceResultData());
}

TEST(ReduceElementLevel, GroupReduceRejectsMismatchedElementIndices) {
    auto seg0 = MakeElementLevelSearchResult();
    seg0.element_indices_.pop_back();

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestGroupReduceHelper helper(search_results,
                                 nullptr,
                                 slice_nqs.data(),
                                 slice_topks.data(),
                                 slice_nqs.size(),
                                 nullptr);

    EXPECT_ANY_THROW(helper.FilterInvalidSearchResult(&seg0));
}

TEST(ReduceElementLevel, GroupReduceKeepsDistinctElementHitsWithSamePK) {
    auto seg0 = MakeElementLevelSearchResult();
    seg0.group_size_ = 2;
    seg0.group_by_values_ = std::vector<GroupByValueType>{
        MakeInt64GroupValue(5),
        MakeInt64GroupValue(5),
        MakeInt64GroupValue(5),
        MakeInt64GroupValue(6),
    };

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{2};
    TestGroupReduceHelper helper(search_results,
                                 nullptr,
                                 slice_nqs.data(),
                                 slice_topks.data(),
                                 slice_nqs.size(),
                                 nullptr);

    helper.ReduceResultData();

    EXPECT_EQ(seg0.result_offsets_, std::vector<int64_t>({0, 1, 2}));
    ASSERT_EQ(helper.FinalSearchRecordsForTest().size(), 1);
    ASSERT_EQ(helper.FinalSearchRecordsForTest()[0].size(), 1);
    EXPECT_EQ(helper.FinalSearchRecordsForTest()[0][0],
              std::vector<int64_t>({0, 1, 3}));
}

TEST(ReduceElementLevel, RescoreSortKeepsElementIndicesAligned) {
    SearchResult result;
    result.total_nq_ = 2;
    result.unity_topK_ = 3;
    result.distances_ = {0.2f, 0.9f, 0.5f, 0.1f, 0.8f, 0.7f};
    result.seg_offsets_ = {10, 11, 12, -1, 20, 21};
    result.element_indices_ = {2, 0, 1, -1, 5, 4};

    milvus::exec::sort_search_result(result, true);

    EXPECT_EQ(result.seg_offsets_,
              std::vector<int64_t>({11, 12, 10, 20, 21, -1}));
    EXPECT_EQ(result.element_indices_,
              std::vector<int32_t>({0, 1, 2, 5, 4, -1}));
    EXPECT_EQ(result.distances_,
              std::vector<float>({0.9f, 0.5f, 0.2f, 0.8f, 0.7f, 0.1f}));
}

TEST(ReduceElementLevel, StreamReduceKeepsDistinctElementHitsWithSamePK) {
    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.topk_per_nq_prefix_sum_ = {0, 2};
    seg0.primary_keys_ = {
        int64_t(5),
        int64_t(6),
    };
    seg0.distances_ = {0.99f, 0.96f};
    seg0.seg_offsets_ = {10, 11};
    seg0.element_level_ = true;
    seg0.element_indices_ = {0, 0};

    SearchResult seg1;
    seg1.total_nq_ = 1;
    seg1.topk_per_nq_prefix_sum_ = {0, 3};
    seg1.primary_keys_ = {
        int64_t(5),
        int64_t(5),
        int64_t(6),
    };
    seg1.distances_ = {0.98f, 0.97f, 0.95f};
    seg1.seg_offsets_ = {20, 20, 21};
    seg1.element_level_ = true;
    seg1.element_indices_ = {1, 1, 0};

    std::vector<SearchResult*> search_results{&seg0, &seg1};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{5};
    TestStreamReducerHelper helper(
        nullptr, slice_nqs.data(), slice_topks.data(), slice_nqs.size());
    helper.SetSearchResultsToMerge(search_results);

    helper.InitializeReduceRecords();
    helper.ReduceResultData();

    EXPECT_EQ(seg0.result_offsets_, std::vector<int64_t>({0, 2}));
    EXPECT_EQ(seg1.result_offsets_, std::vector<int64_t>({1}));
}

TEST(ReduceElementLevel, StreamReduceDedupsMergedResultsByElementIdentity) {
    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.topk_per_nq_prefix_sum_ = {0, 2};
    seg0.primary_keys_ = {
        int64_t(5),
        int64_t(5),
    };
    seg0.distances_ = {0.98f, 0.97f};
    seg0.seg_offsets_ = {20, 20};
    seg0.element_level_ = true;
    seg0.element_indices_ = {1, 0};

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestStreamReducerHelper helper(
        nullptr, slice_nqs.data(), slice_topks.data(), slice_nqs.size());
    helper.SetSearchResultsToMerge(search_results);

    auto merged = helper.MutableMergedResultForTest();
    merged->has_result_ = true;
    merged->element_level_ = true;
    merged->topk_per_nq_prefix_sum_ = {0, 2};
    merged->primary_keys_ = {
        int64_t(5),
        int64_t(6),
    };
    merged->distances_ = {0.99f, 0.95f};
    merged->element_indices_ = {0, 0};

    helper.InitializeReduceRecords();
    helper.ReduceResultData();

    EXPECT_EQ(merged->reduced_offsets_, std::vector<int64_t>({0, 2}));
    EXPECT_EQ(seg0.result_offsets_, std::vector<int64_t>({1}));
}
