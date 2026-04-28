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
#include <stddef.h>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/Collection.h"
#include "segcore/ReduceStructure.h"
#include "segcore/collection_c.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/PbHelper.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::test;

namespace {

class TestReduceHelper : public ReduceHelper {
 public:
    using ReduceHelper::ApplyRefinedOrderForOneNQ;
    using ReduceHelper::CanUseGlobalRefine;
    using ReduceHelper::IsSearchResultRefineEnabled;
    using ReduceHelper::ReduceHelper;
    using ReduceHelper::ReduceResultData;

    const std::vector<std::vector<std::vector<int64_t>>>&
    FinalSearchRecordsForTest() const {
        return final_search_records_;
    }

    void
    TruncateForTest() {
        TruncateToRefineTopk();
    }

    bool
    CanUseGlobalRefineForTest() const {
        return CanUseGlobalRefine();
    }

    void
    RefineDistancesForTest() {
        RefineDistances();
    }

    void
    ApplyRefinedOrderForTest(SearchResult* search_result,
                             size_t nq_begin,
                             const std::vector<size_t>& indices,
                             const std::vector<float>& new_distances) {
        auto reorder_indices = indices;
        ApplyRefinedOrderForOneNQ(
            search_result, nq_begin, reorder_indices, new_distances);
    }

    void
    SetSearchResultRefineEnabledForTest(bool enabled) {
        search_result_refine_enabled_for_test_ = enabled;
    }

    void
    SetSearchResultRefineEnabledForTest(SearchResult* search_result,
                                        bool enabled) {
        search_result_refine_enabled_by_result_for_test_[search_result] =
            enabled;
    }

 protected:
    bool
    IsSearchResultRefineEnabled(SearchResult* search_result) const override {
        auto it = search_result_refine_enabled_by_result_for_test_.find(
            search_result);
        if (it != search_result_refine_enabled_by_result_for_test_.end()) {
            return it->second;
        }
        if (search_result_refine_enabled_for_test_.has_value()) {
            return search_result_refine_enabled_for_test_.value();
        }
        return ReduceHelper::IsSearchResultRefineEnabled(search_result);
    }

 private:
    std::optional<bool> search_result_refine_enabled_for_test_;
    std::unordered_map<SearchResult*, bool>
        search_result_refine_enabled_by_result_for_test_;
};

}  // namespace

TEST(CApiTest, ReduceNullResult) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);
    int64_t offset;

    PreInsert(segment, N, &offset);
    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;

    auto blob = generate_max_float_query_data(num_queries, num_queries / 2);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    {
        auto slice_nqs = std::vector<int64_t>{10};
        auto slice_topKs = std::vector<int64_t>{1};
        std::vector<CSearchResult> results;
        CSearchResult res;
        status = CSearch(segment, plan, placeholderGroup, MAX_TIMESTAMP, &res);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res);
        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                placeholderGroup,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);

        auto search_result = (SearchResult*)results[0];
        auto size = search_result->result_offsets_.size();
        EXPECT_EQ(size, num_queries / 2);

        DeleteSearchResult(res);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceRemoveDuplicates) {
    auto collection = NewCollection(get_default_schema_config().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    int N = 10000;
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    int num_queries = 10;
    int topK = 10;

    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    {
        auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
        auto slice_topKs = std::vector<int64_t>{topK / 2, topK};
        std::vector<CSearchResult> results;
        CSearchResult res1, res2;
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res2);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res1);
        results.push_back(res2);

        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                placeholderGroup,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);
        // TODO:: insert no duplicate pks and check reduce results
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }
    {
        int nq1 = num_queries / 3;
        int nq2 = num_queries / 3;
        int nq3 = num_queries - nq1 - nq2;
        auto slice_nqs = std::vector<int64_t>{nq1, nq2, nq3};
        auto slice_topKs = std::vector<int64_t>{topK / 2, topK, topK};
        std::vector<CSearchResult> results;
        CSearchResult res1, res2, res3;
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res2);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            segment, plan, placeholderGroup, dataset.timestamps_[0], &res3);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res1);
        results.push_back(res2);
        results.push_back(res3);
        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
                                                placeholderGroup,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        ASSERT_EQ(status.error_code, Success);
        // TODO:: insert no duplicate pks and check reduce results
        CheckSearchResultDuplicate(results);

        DeleteSearchResult(res1);
        DeleteSearchResult(res2);
        DeleteSearchResult(res3);
        DeleteSearchResultDataBlobs(cSearchResultData);
    }

    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceKeepsDistinctElementLevelHitsWithSamePK) {
    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.topk_per_nq_prefix_sum_ = {0, 4};
    seg0.primary_keys_ = {
        int64_t(5),
        int64_t(5),
        int64_t(5),
        int64_t(6),
    };
    seg0.distances_ = {0.99f, 0.98f, 0.97f, 0.96f};
    seg0.seg_offsets_ = {10, 10, 10, 11};
    seg0.element_level_ = true;
    seg0.element_indices_ = {0, 1, 1, 0};

    std::vector<SearchResult*> search_results{&seg0};
    std::vector<int64_t> slice_nqs{1};
    std::vector<int64_t> slice_topks{4};
    TestReduceHelper helper(search_results,
                            nullptr,
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

template <class TraitType>
void
testReduceSearchWithExpr(int N,
                         int topK,
                         int num_queries,
                         bool filter_all = false) {
    std::cerr << "testReduceSearchWithExpr(" << N << ", " << topK << ", "
              << num_queries << ")" << std::endl;

    auto collection =
        NewCollection(get_default_schema_config<TraitType>().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);

    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    // Create schema handle for parsing search expressions
    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);

    // Build expression: empty for no filter, or "age > N" to filter all data
    std::string expr = "";
    if (filter_all) {
        expr = "age > " + std::to_string(N);
    }

    auto blob = generate_query_data<TraitType>(num_queries);

    void* plan = nullptr;
    auto binary_plan =
        schema_handle.ParseSearch(expr,                  // expression
                                  "fakevec",             // vector field name
                                  topK,                  // topK
                                  "L2",                  // metric_type
                                  R"({"nprobe": 10})");  // search_params
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CPlaceholderGroup> placeholderGroups;
    placeholderGroups.push_back(placeholderGroup);
    dataset.timestamps_.clear();
    dataset.timestamps_.push_back(1);

    std::vector<CSearchResult> results;
    CSearchResult res1;
    CSearchResult res2;
    auto res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res1);
    ASSERT_EQ(res.error_code, Success);
    res = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res2);
    ASSERT_EQ(res.error_code, Success);
    results.push_back(res1);
    results.push_back(res2);

    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    if (num_queries == 1) {
        slice_nqs = std::vector<int64_t>{num_queries};
    }
    auto slice_topKs = std::vector<int64_t>{topK / 2, topK};
    if (topK == 1) {
        slice_topKs = std::vector<int64_t>{topK, topK};
    }

    // 1. reduce
    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            placeholderGroup,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    auto search_result_data_blobs =
        reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(
            cSearchResultData);

    // check result
    for (size_t i = 0; i < slice_nqs.size(); i++) {
        milvus::proto::schema::SearchResultData search_result_data;
        auto suc = search_result_data.ParseFromArray(
            search_result_data_blobs->blobs[i].data(),
            search_result_data_blobs->blobs[i].size());
        ASSERT_TRUE(suc);
        ASSERT_EQ(search_result_data.num_queries(), slice_nqs[i]);
        ASSERT_EQ(search_result_data.top_k(), slice_topKs[i]);
        ASSERT_EQ(search_result_data.ids().int_id().data_size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);
        ASSERT_EQ(search_result_data.scores().size(),
                  search_result_data.topks().at(0) * slice_nqs[i]);

        // check real topks
        ASSERT_EQ(search_result_data.topks().size(), slice_nqs[i]);
        for (auto real_topk : search_result_data.topks()) {
            ASSERT_LE(real_topk, slice_topKs[i]);
            if (filter_all) {
                ASSERT_EQ(real_topk, 0);
            }
        }
    }

    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, ReduceSearchWithExpr) {
    // float32
    testReduceSearchWithExpr<milvus::FloatVector>(2, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::FloatVector>(100, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(100, 10, 10);
    testReduceSearchWithExpr<milvus::FloatVector>(10000, 1, 1);
    testReduceSearchWithExpr<milvus::FloatVector>(10000, 10, 10);
    // float16
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::Float16Vector>(100, 10, 10);
    // bfloat16
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::BFloat16Vector>(100, 10, 10);
    // int8
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 10, 10);
    testReduceSearchWithExpr<milvus::Int8Vector>(100, 10, 10);
}

// Test that after reduce with multiple segments, unselected segments
// have their distances_/seg_offsets_ cleared (resized to 0).
// This prevents unnecessary FillTargetEntry calls on unselected segments,
// which is critical for external tables where each fill triggers S3 reads.
TEST(CApiTest, ReduceRefreshClearsUnselectedSegments) {
    int num_segments = 5;
    int N = 100;
    int num_queries = 1;
    int topK = 1;

    auto schema_config = get_default_schema_config();
    auto collection = NewCollection(schema_config.c_str());
    auto schema = ((milvus::segcore::Collection*)collection)->get_schema();

    // Create multiple segments and search each
    std::vector<CSegmentInterface> segments(num_segments);
    std::vector<CSearchResult> results(num_segments);

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto binary_plan = schema_handle.ParseSearch(
        "", "fakevec", topK, "L2", R"({"nprobe": 10})");

    void* plan = nullptr;
    auto status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    auto blob = generate_query_data<milvus::FloatVector>(num_queries);
    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    for (int i = 0; i < num_segments; i++) {
        status = NewSegment(collection, Growing, -1, &segments[i], false);
        ASSERT_EQ(status.error_code, Success);

        // Use different seed per segment to get different data/distances
        auto dataset = DataGen(schema, N, /*seed=*/42 + i);
        int64_t offset;
        PreInsert(segments[i], N, &offset);
        auto insert_data = serialize(dataset.raw_);
        status = Insert(segments[i],
                        offset,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        insert_data.data(),
                        insert_data.size());
        ASSERT_EQ(status.error_code, Success);

        status = CSearch(
            segments[i], plan, placeholderGroup, MAX_TIMESTAMP, &results[i]);
        ASSERT_EQ(status.error_code, Success);
    }

    // Reduce: global topk=1 across 5 segments
    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};
    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            placeholderGroup,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    // Verify: after reduce, only selected segments should have non-empty
    // distances_. Unselected segments must have distances_.size() == 0
    // so that FillEntryData skips them (avoiding unnecessary I/O).
    int selected_count = 0;
    int unselected_count = 0;
    for (int i = 0; i < num_segments; i++) {
        auto* sr = (SearchResult*)results[i];
        if (sr->result_offsets_.size() > 0) {
            // Selected segment: should have exactly topK results
            EXPECT_EQ(sr->distances_.size(), topK)
                << "Selected segment " << i << " should have topK distances";
            selected_count++;
        } else {
            // Unselected segment: distances_ must be cleared to 0
            EXPECT_EQ(sr->distances_.size(), 0)
                << "Unselected segment " << i
                << " should have empty distances after refresh";
            EXPECT_EQ(sr->seg_offsets_.size(), 0)
                << "Unselected segment " << i
                << " should have empty seg_offsets after refresh";
            unselected_count++;
        }
    }
    // With topk=1 and nq=1, exactly 1 segment should be selected
    EXPECT_EQ(selected_count, 1);
    EXPECT_EQ(unselected_count, num_segments - 1);

    // Cleanup
    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    for (int i = 0; i < num_segments; i++) {
        DeleteSearchResult(results[i]);
        DeleteSegment(segments[i]);
    }
    DeleteCollection(collection);
}

TEST(CApiTest, ReduceSearchWithExprFilterAll) {
    // float32
    testReduceSearchWithExpr<milvus::FloatVector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::FloatVector>(2, 10, 10, true);
    // float16
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::Float16Vector>(2, 10, 10, true);
    // bfloat16
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::BFloat16Vector>(2, 10, 10, true);
    // int8
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 1, 1, true);
    testReduceSearchWithExpr<milvus::Int8Vector>(2, 10, 10, true);
}

// Helper: run search + reduce with given topk ratios, return deserialized
// search result data for the first slice.
template <class TraitType>
milvus::proto::schema::SearchResultData
runSearchReduceWithGlobalRefine(int N,
                                int topK,
                                int num_queries,
                                float search_topk_ratio,
                                float refine_topk_ratio) {
    auto collection =
        NewCollection(get_default_schema_config<TraitType>().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    EXPECT_EQ(status.error_code, Success);

    auto schema = ((Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    EXPECT_EQ(ins_res.error_code, Success);

    ScopedSchemaHandle schema_handle(*schema);
    auto blob = generate_query_data<TraitType>(num_queries);

    void* plan = nullptr;
    auto binary_plan = schema_handle.ParseSearch("",
                                                 "fakevec",
                                                 topK,
                                                 "L2",
                                                 R"({"nprobe": 10})",
                                                 -1,
                                                 "",
                                                 false,
                                                 search_topk_ratio,
                                                 refine_topk_ratio);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    EXPECT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    EXPECT_EQ(status.error_code, Success);

    CSearchResult res1, res2;
    status = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res1);
    EXPECT_EQ(status.error_code, Success);
    status = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res2);
    EXPECT_EQ(status.error_code, Success);

    std::vector<CSearchResult> results{res1, res2};
    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            placeholderGroup,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    EXPECT_EQ(status.error_code, Success);

    auto search_result_data_blobs =
        reinterpret_cast<SearchResultDataBlobs*>(cSearchResultData);

    milvus::proto::schema::SearchResultData result_data;
    result_data.ParseFromArray(search_result_data_blobs->blobs[0].data(),
                               search_result_data_blobs->blobs[0].size());

    DeleteSearchResult(res1);
    DeleteSearchResult(res2);
    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);

    return result_data;
}

milvus::proto::schema::SearchResultData
runSearchReduceWithForcedSealedRefine(int N,
                                      int topK,
                                      int num_queries,
                                      float search_topk_ratio,
                                      float refine_topk_ratio) {
    auto collection =
        NewCollection(get_default_schema_config<milvus::FloatVector>().c_str());
    auto schema = ((Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N);

    ScopedSchemaHandle schema_handle(*schema);
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    auto binary_plan = schema_handle.ParseSearch("",
                                                 "fakevec",
                                                 topK,
                                                 "L2",
                                                 R"({"nprobe": 10})",
                                                 -1,
                                                 "",
                                                 false,
                                                 search_topk_ratio,
                                                 refine_topk_ratio);
    auto status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    EXPECT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    EXPECT_EQ(status.error_code, Success);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "64"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(DIM)}};
    FieldIndexMeta field_index_meta(
        FieldId(100), std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> field_map = {
        {FieldId(100), std::move(field_index_meta)}};
    auto collection_index_meta =
        std::make_shared<CollectionIndexMeta>(226985, std::move(field_map));

    auto segcore_config = SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    segcore_config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR);
    segcore_config.set_nlist(16);
    segcore_config.set_chunk_rows(1024);

    auto segment =
        CreateSealedSegment(schema, collection_index_meta, 0, segcore_config);
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareInsertBinlog(
        kCollectionID, kPartitionID, kSegmentID, dataset, cm);
    status = LoadFieldData(segment.get(), &load_info);
    EXPECT_EQ(status.error_code, Success);
    EXPECT_TRUE(segment->HasIndex(FieldId(100)));

    CSearchResult res;
    status =
        CSearch(segment.get(), plan, placeholderGroup, MAX_TIMESTAMP, &res);
    EXPECT_EQ(status.error_code, Success);

    auto* search_result = reinterpret_cast<SearchResult*>(res);
    EXPECT_EQ(search_result->total_nq_, num_queries);
    EXPECT_GE(search_result->unity_topK_, topK);

    std::vector<SearchResult*> search_results{search_result};
    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};
    auto* plan_ptr = static_cast<milvus::query::Plan*>(plan);
    auto* placeholder_group_ptr =
        static_cast<const milvus::query::PlaceholderGroup*>(placeholderGroup);

    TestReduceHelper helper(search_results,
                            plan_ptr,
                            placeholder_group_ptr,
                            slice_nqs.data(),
                            slice_topKs.data(),
                            slice_nqs.size(),
                            nullptr);
    helper.SetSearchResultRefineEnabledForTest(true);
    helper.Reduce();
    helper.Marshal();

    auto search_result_data_blobs = reinterpret_cast<SearchResultDataBlobs*>(
        helper.GetSearchResultDataBlobs());

    milvus::proto::schema::SearchResultData result_data;
    auto parsed =
        result_data.ParseFromArray(search_result_data_blobs->blobs[0].data(),
                                   search_result_data_blobs->blobs[0].size());
    EXPECT_TRUE(parsed);

    DeleteSearchResultDataBlobs(
        reinterpret_cast<CSearchResultDataBlobs>(search_result_data_blobs));
    DeleteSearchResult(res);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    segment.reset();
    DeleteCollection(collection);

    return result_data;
}

TEST(CApiTest, ReduceWithGlobalRefine) {
    int N = 1000;
    int topK = 10;
    int num_queries = 4;

    // Run without global refine (baseline, ratios=0 means disabled)
    auto result_no_refine =
        runSearchReduceWithGlobalRefine<milvus::FloatVector>(
            N, topK, num_queries, 0, 0);
    ASSERT_EQ(result_no_refine.num_queries(), num_queries);
    ASSERT_EQ(result_no_refine.top_k(), topK);
    for (auto real_topk : result_no_refine.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }

    // Run with global refine enabled (search_topk_ratio=2.0, refine_topk_ratio=1.5)
    auto result_with_refine =
        runSearchReduceWithGlobalRefine<milvus::FloatVector>(
            N, topK, num_queries, 2.0f, 1.5f);
    ASSERT_EQ(result_with_refine.num_queries(), num_queries);
    ASSERT_EQ(result_with_refine.top_k(), topK);
    for (auto real_topk : result_with_refine.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }

    // Both should return same number of results per query
    ASSERT_EQ(result_no_refine.topks_size(), result_with_refine.topks_size());

    // Refined results should have valid scores (not NaN or Inf)
    for (int i = 0; i < result_with_refine.scores_size(); i++) {
        ASSERT_FALSE(std::isnan(result_with_refine.scores(i)));
        ASSERT_FALSE(std::isinf(result_with_refine.scores(i)));
    }
}

TEST(CApiTest, ReduceWithGlobalRefineUsesExplicitPlaceholderGroup) {
    int N = 1000;
    int topK = 10;
    int num_queries = 4;

    auto result = runSearchReduceWithGlobalRefine<milvus::FloatVector>(
        N, topK, num_queries, 2.0f, 1.5f);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
    for (int i = 0; i < result.scores_size(); i++) {
        ASSERT_FALSE(std::isnan(result.scores(i)));
        ASSERT_FALSE(std::isinf(result.scores(i)));
    }
}

TEST(CApiTest, ReduceWithForcedSealedRefine) {
    int N = 1000;
    int topK = 10;
    int num_queries = 4;

    auto result =
        runSearchReduceWithForcedSealedRefine(N, topK, num_queries, 2.0f, 1.5f);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    ASSERT_EQ(result.topks_size(), num_queries);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
    for (int i = 0; i < result.scores_size(); i++) {
        ASSERT_FALSE(std::isnan(result.scores(i)));
        ASSERT_FALSE(std::isinf(result.scores(i)));
    }
}

TEST(CApiTest, GlobalRefineTruncateMergesBeforeSegmentPruning) {
    auto schema = std::make_shared<Schema>();
    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();
    plan.plan_node_->search_info_.refine_topk_ratio_ = 0.5;

    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 2;
    seg0.distances_ = {0.95f, 0.94f};
    seg0.seg_offsets_ = {100, 101};
    seg0.topk_per_nq_prefix_sum_ = {0, 2};

    SearchResult seg1;
    seg1.total_nq_ = 1;
    seg1.unity_topK_ = 2;
    seg1.distances_ = {0.93f, 0.92f};
    seg1.seg_offsets_ = {200, 201};
    seg1.topk_per_nq_prefix_sum_ = {0, 2};

    std::vector<SearchResult*> search_results{&seg0, &seg1};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {2};
    TestReduceHelper helper(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);

    helper.TruncateForTest();

    // refine_topk = ceil(0.5 * max(slice_topk=2, 0 since ef/search_list not specified)) = 1
    // merge top-1 across segments: seg0 keeps only 0.95, seg1 gets none
    ASSERT_EQ(seg0.distances_.size(), 1);
    ASSERT_EQ(seg0.seg_offsets_.size(), 1);
    EXPECT_FLOAT_EQ(seg0.distances_[0], 0.95f);
    EXPECT_EQ(seg0.seg_offsets_[0], 100);
    EXPECT_EQ(seg0.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 1}));

    ASSERT_TRUE(seg1.distances_.empty());
    ASSERT_TRUE(seg1.seg_offsets_.empty());
    EXPECT_EQ(seg1.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 0}));
}

TEST(CApiTest, GlobalRefineTruncateHandlesMixedSegmentUnityTopk) {
    auto schema = std::make_shared<Schema>();
    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();
    plan.plan_node_->search_info_.refine_topk_ratio_ = 1.0;
    knowhere::Json search_params;
    search_params["ef"] = 3;
    plan.plan_node_->search_info_.search_params_ = search_params;

    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 3;
    seg0.distances_ = {0.99f, 0.95f, 0.90f};
    seg0.seg_offsets_ = {100, 101, 102};
    seg0.topk_per_nq_prefix_sum_ = {0, 3};

    SearchResult seg1;
    seg1.total_nq_ = 1;
    seg1.unity_topK_ = 1;
    seg1.distances_ = {0.98f};
    seg1.seg_offsets_ = {200};
    seg1.topk_per_nq_prefix_sum_ = {0, 1};

    std::vector<SearchResult*> search_results{&seg0, &seg1};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {2};
    TestReduceHelper helper(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);

    helper.TruncateForTest();

    // refine_topk = ceil(1.0 * max(slice_topk=2, ef/search_list=3)) = 3
    // merge top-3 across segments: 0.99(seg0), 0.98(seg1), 0.95(seg0)
    ASSERT_EQ(seg0.distances_.size(), 2);
    ASSERT_EQ(seg0.seg_offsets_.size(), 2);
    EXPECT_FLOAT_EQ(seg0.distances_[0], 0.99f);
    EXPECT_FLOAT_EQ(seg0.distances_[1], 0.95f);
    EXPECT_EQ(seg0.seg_offsets_[0], 100);
    EXPECT_EQ(seg0.seg_offsets_[1], 101);
    EXPECT_EQ(seg0.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 2}));

    ASSERT_EQ(seg1.distances_.size(), 1);
    ASSERT_EQ(seg1.seg_offsets_.size(), 1);
    EXPECT_FLOAT_EQ(seg1.distances_[0], 0.98f);
    EXPECT_EQ(seg1.seg_offsets_[0], 200);
    EXPECT_EQ(seg1.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 1}));
}

TEST(CApiTest, GlobalRefineRequiresPlaceholderGroup) {
    auto schema = std::make_shared<Schema>();
    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();
    plan.plan_node_->search_info_.global_refine_enable_ = true;

    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 1;
    seg0.distances_ = {0.95f};
    seg0.seg_offsets_ = {100};
    seg0.topk_per_nq_prefix_sum_ = {0, 1};

    std::vector<SearchResult*> search_results{&seg0};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {1};

    TestReduceHelper helper_without_placeholder(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);
    helper_without_placeholder.SetSearchResultRefineEnabledForTest(true);
    EXPECT_FALSE(helper_without_placeholder.CanUseGlobalRefineForTest());

    query::Placeholder placeholder;
    placeholder.num_of_queries_ = 1;
    placeholder.blob_.resize(sizeof(float), 0);
    query::PlaceholderGroup placeholder_group;
    placeholder_group.push_back(std::move(placeholder));

    TestReduceHelper helper_with_placeholder(search_results,
                                             &plan,
                                             &placeholder_group,
                                             slice_nqs,
                                             slice_topks,
                                             1,
                                             nullptr);
    helper_with_placeholder.SetSearchResultRefineEnabledForTest(true);
    EXPECT_TRUE(helper_with_placeholder.CanUseGlobalRefineForTest());

    TestReduceHelper helper_with_disabled_segment(search_results,
                                                  &plan,
                                                  &placeholder_group,
                                                  slice_nqs,
                                                  slice_topks,
                                                  1,
                                                  nullptr);
    helper_with_disabled_segment.SetSearchResultRefineEnabledForTest(false);
    EXPECT_FALSE(helper_with_disabled_segment.CanUseGlobalRefineForTest());

    SearchResult seg1;
    seg1.total_nq_ = seg0.total_nq_;
    seg1.unity_topK_ = seg0.unity_topK_;
    seg1.distances_ = seg0.distances_;
    seg1.seg_offsets_ = seg0.seg_offsets_;
    seg1.topk_per_nq_prefix_sum_ = seg0.topk_per_nq_prefix_sum_;
    std::vector<SearchResult*> mixed_search_results{&seg0, &seg1};
    int64_t mixed_slice_nqs[] = {1};
    int64_t mixed_slice_topks[] = {1};
    TestReduceHelper helper_with_mixed_segments(mixed_search_results,
                                                &plan,
                                                &placeholder_group,
                                                mixed_slice_nqs,
                                                mixed_slice_topks,
                                                1,
                                                nullptr);
    helper_with_mixed_segments.SetSearchResultRefineEnabledForTest(&seg0,
                                                                   false);
    helper_with_mixed_segments.SetSearchResultRefineEnabledForTest(&seg1, true);
    EXPECT_TRUE(helper_with_mixed_segments.CanUseGlobalRefineForTest());
}

TEST(CApiTest, GlobalRefineSkipsDisabledSegmentsDuringRefine) {
    auto schema = std::make_shared<Schema>();
    auto field_id =
        schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, 4, "L2");

    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();
    plan.plan_node_->search_info_.field_id_ = field_id;
    plan.plan_node_->search_info_.metric_type_ = knowhere::metric::L2;
    plan.plan_node_->search_info_.global_refine_enable_ = true;

    query::Placeholder placeholder;
    placeholder.num_of_queries_ = 1;
    placeholder.blob_.resize(sizeof(float) * 4, 0);
    query::PlaceholderGroup placeholder_group;
    placeholder_group.push_back(std::move(placeholder));

    SearchResult enabled_segment;
    enabled_segment.total_nq_ = 1;
    enabled_segment.unity_topK_ = 0;
    enabled_segment.topk_per_nq_prefix_sum_ = {0, 0};

    SearchResult disabled_segment;
    disabled_segment.total_nq_ = 1;
    disabled_segment.unity_topK_ = 1;
    disabled_segment.seg_offsets_ = {0};
    disabled_segment.distances_ = {0.5f};
    disabled_segment.topk_per_nq_prefix_sum_ = {0, 1};

    std::vector<SearchResult*> search_results{&enabled_segment,
                                              &disabled_segment};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {1};

    TestReduceHelper helper(search_results,
                            &plan,
                            &placeholder_group,
                            slice_nqs,
                            slice_topks,
                            1,
                            nullptr);
    helper.SetSearchResultRefineEnabledForTest(&enabled_segment, true);
    helper.SetSearchResultRefineEnabledForTest(&disabled_segment, false);
    // Should not crash — disabled segment is skipped during refine.
    helper.RefineDistancesForTest();
}

TEST(CApiTest, RefineReorderKeepsElementIndicesAligned) {
    auto schema = std::make_shared<Schema>();
    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();

    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 3;
    seg0.element_level_ = true;
    seg0.distances_ = {0.1f, 0.2f, 0.3f};
    seg0.seg_offsets_ = {10, 11, 12};
    seg0.element_indices_ = {100, 101, 102};
    seg0.topk_per_nq_prefix_sum_ = {0, 3};

    std::vector<SearchResult*> search_results{&seg0};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {3};

    TestReduceHelper helper(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);

    std::vector<size_t> indices = {1, 2, 0};
    std::vector<float> new_distances = {5.0f, 9.0f, 7.0f};
    helper.ApplyRefinedOrderForTest(&seg0, 0, indices, new_distances);

    EXPECT_EQ(seg0.distances_, std::vector<float>({9.0f, 7.0f, 5.0f}));
    EXPECT_EQ(seg0.seg_offsets_, std::vector<int64_t>({11, 12, 10}));
    EXPECT_EQ(seg0.element_indices_, std::vector<int32_t>({101, 102, 100}));
}

TEST(CApiTest, ReduceWithGlobalRefineHighRatio) {
    int N = 500;
    int topK = 5;
    int num_queries = 2;

    // search_topk_ratio=4.0, refine_topk_ratio=3.0
    auto result = runSearchReduceWithGlobalRefine<milvus::FloatVector>(
        N, topK, num_queries, 4.0f, 3.0f);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
    for (int i = 0; i < result.scores_size(); i++) {
        ASSERT_FALSE(std::isnan(result.scores(i)));
        ASSERT_FALSE(std::isinf(result.scores(i)));
    }
}

TEST(CApiTest, ReduceWithGlobalRefineSearchRatioOnly) {
    // global refine disabled (ratios=0), should take the non-refine path
    int N = 500;
    int topK = 5;
    int num_queries = 2;

    auto result = runSearchReduceWithGlobalRefine<milvus::FloatVector>(
        N, topK, num_queries, 0, 0);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
}

TEST(CApiTest, ReduceWithGlobalRefineFloat16) {
    int N = 500;
    int topK = 5;
    int num_queries = 2;

    auto result = runSearchReduceWithGlobalRefine<milvus::Float16Vector>(
        N, topK, num_queries, 2.0f, 1.5f);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
}

TEST(CApiTest, ReduceWithGlobalRefineBFloat16) {
    int N = 500;
    int topK = 5;
    int num_queries = 2;

    auto result = runSearchReduceWithGlobalRefine<milvus::BFloat16Vector>(
        N, topK, num_queries, 2.0f, 1.5f);
    ASSERT_EQ(result.num_queries(), num_queries);
    ASSERT_EQ(result.top_k(), topK);
    for (auto real_topk : result.topks()) {
        ASSERT_GT(real_topk, 0);
        ASSERT_LE(real_topk, topK);
    }
}

TEST(CApiTest, ReduceWithGlobalRefineFilterAll) {
    // With global refine enabled but all results filtered,
    // should still return 0 results without errors
    int N = 100;
    int topK = 10;
    int num_queries = 2;

    auto collection =
        NewCollection(get_default_schema_config<milvus::FloatVector>().c_str());
    CSegmentInterface segment;
    auto status = NewSegment(collection, Growing, -1, &segment, false);
    ASSERT_EQ(status.error_code, Success);

    auto schema = ((Collection*)collection)->get_schema();
    auto dataset = DataGen(schema, N);

    int64_t offset;
    PreInsert(segment, N, &offset);
    auto insert_data = serialize(dataset.raw_);
    auto ins_res = Insert(segment,
                          offset,
                          N,
                          dataset.row_ids_.data(),
                          dataset.timestamps_.data(),
                          insert_data.data(),
                          insert_data.size());
    ASSERT_EQ(ins_res.error_code, Success);

    ScopedSchemaHandle schema_handle(*schema);
    // Filter all: age > N ensures no results match
    std::string filter_expr = "age > " + std::to_string(N);
    auto blob = generate_query_data<milvus::FloatVector>(num_queries);

    void* plan = nullptr;
    auto binary_plan = schema_handle.ParseSearch(filter_expr,
                                                 "fakevec",
                                                 topK,
                                                 "L2",
                                                 R"({"nprobe": 10})",
                                                 -1,
                                                 "",
                                                 false,
                                                 2.0,
                                                 1.5);
    status = CreateSearchPlanByExpr(
        collection, binary_plan.data(), binary_plan.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    void* placeholderGroup = nullptr;
    status = ParsePlaceholderGroup(
        plan, blob.data(), blob.length(), &placeholderGroup);
    ASSERT_EQ(status.error_code, Success);

    CSearchResult res;
    status = CSearch(
        segment, plan, placeholderGroup, dataset.timestamps_[N - 1], &res);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CSearchResult> results{res};
    auto slice_nqs = std::vector<int64_t>{num_queries};
    auto slice_topKs = std::vector<int64_t>{topK};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            plan,
                                            placeholderGroup,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success);

    auto search_result_data_blobs =
        reinterpret_cast<SearchResultDataBlobs*>(cSearchResultData);
    milvus::proto::schema::SearchResultData result_data;
    result_data.ParseFromArray(search_result_data_blobs->blobs[0].data(),
                               search_result_data_blobs->blobs[0].size());

    // All results should be filtered
    for (auto real_topk : result_data.topks()) {
        ASSERT_EQ(real_topk, 0);
    }

    DeleteSearchResult(res);
    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(plan);
    DeletePlaceholderGroup(placeholderGroup);
    DeleteCollection(collection);
    DeleteSegment(segment);
}

TEST(CApiTest, GlobalRefineRejectsGroupBy) {
    auto schema = std::make_shared<Schema>();
    query::Plan plan(schema);
    plan.plan_node_ = std::make_unique<query::VectorPlanNode>();
    plan.plan_node_->search_info_.global_refine_enable_ = true;
    plan.plan_node_->search_info_.group_by_field_ids_.push_back(FieldId(101));

    SearchResult seg0;
    seg0.total_nq_ = 1;
    seg0.unity_topK_ = 2;
    seg0.distances_ = {0.9f, 0.8f};
    seg0.seg_offsets_ = {100, 101};
    seg0.topk_per_nq_prefix_sum_ = {0, 2};

    std::vector<SearchResult*> search_results{&seg0};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {2};
    TestReduceHelper helper(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);

    ASSERT_THROW(helper.Reduce(), std::runtime_error);
}
