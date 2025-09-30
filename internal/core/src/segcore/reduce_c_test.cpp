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

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/reduce_c.h"

#include "test_utils/c_api_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::test;

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
        status = CSearch(segment, plan, placeholderGroup, 1L << 63, &res);
        ASSERT_EQ(status.error_code, Success);
        results.push_back(res);
        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                plan,
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

    auto fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
               topK;

    // construct the predicate that filter out all data
    if (filter_all) {
        fmt = boost::format(R"(vector_anns: <
                                            field_id: 100
                                            predicates: <
                                                unary_range_expr: <
                                                    column_info: <
                                                    field_id: 101
                                                    data_type: Int64
                                                    >
                                                    op: GreaterThan
                                                    value: <
                                                    int64_val: %2%
                                                    >
                                                >
                                            >
                                            query_info: <
                                                topk: %1%
                                                metric_type: "L2"
                                                search_params: "{\"nprobe\": 10}"
                                            >
                                            placeholder_tag: "$0">
                                            output_field_ids: 100)") %
              topK % N;
    }
    auto serialized_expr_plan = fmt.str();
    auto blob = generate_query_data<TraitType>(num_queries);

    void* plan = nullptr;
    auto binary_plan =
        translate_text_plan_to_binary_plan(serialized_expr_plan.data());
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