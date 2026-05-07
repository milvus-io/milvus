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

#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/PlanProto.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/Utils.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/search_result_export_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using milvus::DataType;
using milvus::FieldId;
using milvus::GroupByValueType;
using milvus::PkType;
using milvus::Schema;
using milvus::SearchResult;
using milvus::query::Plan;
using milvus::query::VectorPlanNode;
using milvus::segcore::ReduceHelper;
using milvus::segcore::SortEqualScoresByPks;

static std::string
BuildSimpleVectorSearchPlan(milvus::FieldId vec_fid, int topk);

// ---------------------------------------------------------------------------
// SortEqualScoresByPks
// ---------------------------------------------------------------------------

TEST(SearchResultExport, SortEqualScoresByPks_Basic) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 4;

    // Scores: all equal → should sort by PK ASC
    sr.distances_ = {1.0f, 1.0f, 1.0f, 1.0f};
    sr.seg_offsets_ = {30, 10, 40, 20};
    sr.primary_keys_ = {PkType(int64_t(300)),
                        PkType(int64_t(100)),
                        PkType(int64_t(400)),
                        PkType(int64_t(200))};

    // Build prefix sum (required by SortEqualScoresByPks)
    sr.topk_per_nq_prefix_sum_ = {0, 4};

    SortEqualScoresByPks(&sr);

    // After sort: PKs should be in ASC order
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[0]), 100);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[1]), 200);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[2]), 300);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[3]), 400);

    // seg_offsets should follow the same permutation
    EXPECT_EQ(sr.seg_offsets_[0], 10);
    EXPECT_EQ(sr.seg_offsets_[1], 20);
    EXPECT_EQ(sr.seg_offsets_[2], 30);
    EXPECT_EQ(sr.seg_offsets_[3], 40);
}

TEST(SearchResultExport, SortEqualScoresByPks_MixedScores) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 6;

    // Two equal-score groups: [5.0, 5.0, 5.0] and [3.0, 3.0, 3.0]
    sr.distances_ = {5.0f, 5.0f, 5.0f, 3.0f, 3.0f, 3.0f};
    sr.seg_offsets_ = {30, 10, 20, 60, 40, 50};
    sr.primary_keys_ = {PkType(int64_t(300)),
                        PkType(int64_t(100)),
                        PkType(int64_t(200)),
                        PkType(int64_t(600)),
                        PkType(int64_t(400)),
                        PkType(int64_t(500))};
    sr.topk_per_nq_prefix_sum_ = {0, 6};

    SortEqualScoresByPks(&sr);

    // First group (score=5.0): PKs sorted ASC
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[0]), 100);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[1]), 200);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[2]), 300);
    // Second group (score=3.0): PKs sorted ASC
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[3]), 400);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[4]), 500);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[5]), 600);

    // Distances unchanged (all equal within groups)
    EXPECT_FLOAT_EQ(sr.distances_[0], 5.0f);
    EXPECT_FLOAT_EQ(sr.distances_[3], 3.0f);
}

TEST(SearchResultExport, SortEqualScoresByPks_WithElementIndices) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 3;
    sr.element_level_ = true;

    sr.distances_ = {1.0f, 1.0f, 1.0f};
    sr.seg_offsets_ = {30, 10, 20};
    sr.primary_keys_ = {
        PkType(int64_t(300)), PkType(int64_t(100)), PkType(int64_t(200))};
    sr.element_indices_ = {33, 11, 22};
    sr.topk_per_nq_prefix_sum_ = {0, 3};

    SortEqualScoresByPks(&sr);

    // element_indices should follow the same permutation as PKs
    EXPECT_EQ(sr.element_indices_[0], 11);  // was at index 1 (PK=100)
    EXPECT_EQ(sr.element_indices_[1], 22);  // was at index 2 (PK=200)
    EXPECT_EQ(sr.element_indices_[2], 33);  // was at index 0 (PK=300)
}

TEST(SearchResultExport, SortEqualScoresByPks_WithGroupBy) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 3;

    sr.distances_ = {1.0f, 1.0f, 1.0f};
    sr.seg_offsets_ = {30, 10, 20};
    sr.primary_keys_ = {
        PkType(int64_t(300)), PkType(int64_t(100)), PkType(int64_t(200))};
    auto make_composite = [](int64_t v) {
        milvus::CompositeGroupKey key;
        key.Add(milvus::GroupByValueType(v));
        return key;
    };
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{
        make_composite(3), make_composite(1), make_composite(2)};
    sr.topk_per_nq_prefix_sum_ = {0, 3};

    SortEqualScoresByPks(&sr);

    // composite_group_by_values should follow the same permutation
    auto& gbv = sr.composite_group_by_values_.value();
    EXPECT_EQ(std::get<int64_t>(gbv[0][0].value()), 1);
    EXPECT_EQ(std::get<int64_t>(gbv[1][0].value()), 2);
    EXPECT_EQ(std::get<int64_t>(gbv[2][0].value()), 3);
}

TEST(SearchResultExport, SortEqualScoresByPks_MultiNQ) {
    SearchResult sr;
    sr.total_nq_ = 2;
    sr.unity_topK_ = 3;

    // NQ0: 3 results with equal scores, unsorted PKs
    // NQ1: 3 results with equal scores, unsorted PKs
    sr.distances_ = {1.0f, 1.0f, 1.0f, 2.0f, 2.0f, 2.0f};
    sr.seg_offsets_ = {30, 10, 20, 60, 40, 50};
    sr.primary_keys_ = {PkType(int64_t(30)),
                        PkType(int64_t(10)),
                        PkType(int64_t(20)),
                        PkType(int64_t(60)),
                        PkType(int64_t(40)),
                        PkType(int64_t(50))};
    sr.topk_per_nq_prefix_sum_ = {0, 3, 6};

    SortEqualScoresByPks(&sr);

    // NQ0: sorted
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[0]), 10);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[1]), 20);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[2]), 30);
    // NQ1: sorted
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[3]), 40);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[4]), 50);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[5]), 60);
}

TEST(SearchResultExport, SortEqualScoresByPks_EmptyElementIndices) {
    // element_level_ is true but element_indices_ is empty — should not crash
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 2;
    sr.element_level_ = true;

    sr.distances_ = {1.0f, 1.0f};
    sr.seg_offsets_ = {20, 10};
    sr.primary_keys_ = {PkType(int64_t(200)), PkType(int64_t(100))};
    // element_indices_ intentionally left empty
    sr.topk_per_nq_prefix_sum_ = {0, 2};

    // Should not crash (the fix checks !element_indices_.empty())
    SortEqualScoresByPks(&sr);

    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[0]), 100);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[1]), 200);
}

TEST(SearchResultExport, SortEqualScoresByPks_SingleElement) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 1;

    sr.distances_ = {1.0f};
    sr.seg_offsets_ = {10};
    sr.primary_keys_ = {PkType(int64_t(100))};
    sr.topk_per_nq_prefix_sum_ = {0, 1};

    // Single element — should be a no-op
    SortEqualScoresByPks(&sr);
    EXPECT_EQ(std::get<int64_t>(sr.primary_keys_[0]), 100);
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowStream_RejectsUnpreparedRows) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 1;
    sr.seg_offsets_ = {10};
    sr.distances_ = {1.0f};
    sr.primary_keys_ = {PkType(int64_t(100))};

    ArrowArrayStream stream{};
    auto status =
        ExportSearchResultAsArrowStream(reinterpret_cast<CSearchResult>(&sr),
                                        reinterpret_cast<CSearchPlan>(&plan),
                                        nullptr,
                                        0,
                                        &stream);
    EXPECT_NE(status.error_code, 0);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("topk_per_nq_prefix_sum_"),
              std::string::npos)
        << status.error_msg;
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowStream_AllowsEmptyResultWithoutPrefix) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 0;

    ArrowArrayStream stream{};
    auto status =
        ExportSearchResultAsArrowStream(reinterpret_cast<CSearchResult>(&sr),
                                        reinterpret_cast<CSearchPlan>(&plan),
                                        nullptr,
                                        0,
                                        &stream);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto reader_result = arrow::ImportRecordBatchReader(&stream);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();

    auto batch_result = (*reader_result)->Next();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    EXPECT_EQ((*batch_result)->num_columns(), 3);

    auto end_result = (*reader_result)->Next();
    ASSERT_TRUE(end_result.ok()) << end_result.status().ToString();
    EXPECT_EQ(*end_result, nullptr);
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowStream_MultiFieldGroupByColumns) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid_1 = schema->AddDebugField("group1", DataType::INT64);
    auto group_fid_2 = schema->AddDebugField("group2", DataType::VARCHAR);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid_1,
                                                         group_fid_2};

    auto make_group_key = [](GroupByValueType first,
                             GroupByValueType second) {
        milvus::CompositeGroupKey key;
        key.Add(std::move(first));
        key.Add(std::move(second));
        return key;
    };

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 2;
    sr.pk_type_ = DataType::INT64;
    sr.seg_offsets_ = {10, 20};
    sr.distances_ = {0.9f, 0.8f};
    sr.primary_keys_ = {PkType(int64_t(100)), PkType(int64_t(200))};
    sr.topk_per_nq_prefix_sum_ = {0, 2};
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{
        make_group_key(GroupByValueType(int64_t(10)),
                       GroupByValueType(std::string("a"))),
        make_group_key(GroupByValueType(int64_t(20)),
                       GroupByValueType(std::string("b")))};

    ArrowArrayStream stream{};
    auto status =
        ExportSearchResultAsArrowStream(reinterpret_cast<CSearchResult>(&sr),
                                        reinterpret_cast<CSearchPlan>(&plan),
                                        nullptr,
                                        0,
                                        &stream);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto reader_result = arrow::ImportRecordBatchReader(&stream);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();

    auto batch_result = (*reader_result)->Next();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_columns(), 5);

    auto group_by_field_1 = (*batch_result)->schema()->field(3);
    EXPECT_EQ(group_by_field_1->name(),
              "$group_by_" + std::to_string(group_fid_1.get()));
    EXPECT_EQ(group_by_field_1->type()->id(), arrow::Type::INT64);
    ASSERT_NE(group_by_field_1->metadata(), nullptr);
    auto field_id_result_1 =
        group_by_field_1->metadata()->Get("milvus.field_id");
    ASSERT_TRUE(field_id_result_1.ok())
        << field_id_result_1.status().ToString();
    EXPECT_EQ(*field_id_result_1, std::to_string(group_fid_1.get()));
    auto data_type_result_1 =
        group_by_field_1->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(data_type_result_1.ok())
        << data_type_result_1.status().ToString();
    EXPECT_EQ(*data_type_result_1,
              std::to_string(static_cast<int32_t>(DataType::INT64)));

    auto group_by_field_2 = (*batch_result)->schema()->field(4);
    EXPECT_EQ(group_by_field_2->name(),
              "$group_by_" + std::to_string(group_fid_2.get()));
    EXPECT_EQ(group_by_field_2->type()->id(), arrow::Type::STRING);
    ASSERT_NE(group_by_field_2->metadata(), nullptr);
    auto field_id_result_2 =
        group_by_field_2->metadata()->Get("milvus.field_id");
    ASSERT_TRUE(field_id_result_2.ok())
        << field_id_result_2.status().ToString();
    EXPECT_EQ(*field_id_result_2, std::to_string(group_fid_2.get()));
    auto data_type_result_2 =
        group_by_field_2->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(data_type_result_2.ok())
        << data_type_result_2.status().ToString();
    EXPECT_EQ(*data_type_result_2,
              std::to_string(static_cast<int32_t>(DataType::VARCHAR)));

    auto group_by_array_1 =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(3));
    EXPECT_EQ(group_by_array_1->Value(0), 10);
    EXPECT_EQ(group_by_array_1->Value(1), 20);
    auto group_by_array_2 =
        std::static_pointer_cast<arrow::StringArray>(
            (*batch_result)->column(4));
    EXPECT_EQ(group_by_array_2->GetString(0), "a");
    EXPECT_EQ(group_by_array_2->GetString(1), "b");
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowStream_RejectsMultiFieldJsonPathGroupBy) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid_1 = schema->AddDebugField("group1", DataType::JSON);
    auto group_fid_2 = schema->AddDebugField("group2", DataType::JSON);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid_1,
                                                         group_fid_2};
    plan.plan_node_->search_info_.json_path_ = "/brand";
    plan.plan_node_->search_info_.json_type_ = DataType::VARCHAR;

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 0;
    sr.topk_per_nq_prefix_sum_ = {0, 0};

    ArrowArrayStream stream{};
    auto status =
        ExportSearchResultAsArrowStream(reinterpret_cast<CSearchResult>(&sr),
                                        reinterpret_cast<CSearchPlan>(&plan),
                                        nullptr,
                                        0,
                                        &stream);
    EXPECT_NE(status.error_code, 0);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(
        std::string(status.error_msg)
            .find("JSON-path group_by for exactly one field"),
        std::string::npos)
        << status.error_msg;
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport, ExportSearchResultAsArrowStream_GroupByMetadata) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid = schema->AddDebugField("geom", DataType::GEOMETRY);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid};

    auto make_group_key = [](GroupByValueType value) {
        milvus::CompositeGroupKey key;
        key.Add(std::move(value));
        return key;
    };

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 2;
    sr.pk_type_ = DataType::INT64;
    sr.seg_offsets_ = {10, 20};
    sr.distances_ = {0.9f, 0.8f};
    sr.primary_keys_ = {PkType(int64_t(100)), PkType(int64_t(200))};
    sr.topk_per_nq_prefix_sum_ = {0, 2};
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{
        make_group_key(GroupByValueType(std::string("wkb"))),
        make_group_key(GroupByValueType{})};

    ArrowArrayStream stream{};
    auto status =
        ExportSearchResultAsArrowStream(reinterpret_cast<CSearchResult>(&sr),
                                        reinterpret_cast<CSearchPlan>(&plan),
                                        nullptr,
                                        0,
                                        &stream);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto reader_result = arrow::ImportRecordBatchReader(&stream);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();

    auto batch_result = (*reader_result)->Next();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_columns(), 4);

    auto group_by_field = (*batch_result)->schema()->field(3);
    EXPECT_EQ(group_by_field->name(),
              "$group_by_" + std::to_string(group_fid.get()));
    EXPECT_EQ(group_by_field->type()->id(), arrow::Type::STRING);
    EXPECT_TRUE(group_by_field->nullable());
    ASSERT_NE(group_by_field->metadata(), nullptr);

    auto field_id_result = group_by_field->metadata()->Get("milvus.field_id");
    ASSERT_TRUE(field_id_result.ok()) << field_id_result.status().ToString();
    EXPECT_EQ(*field_id_result, std::to_string(group_fid.get()));

    auto data_type_result = group_by_field->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(data_type_result.ok()) << data_type_result.status().ToString();
    EXPECT_EQ(*data_type_result,
              std::to_string(static_cast<int32_t>(DataType::GEOMETRY)));

    auto group_by_array = (*batch_result)->column(3);
    EXPECT_EQ(group_by_array->null_count(), 1);
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowStream_NullableExtraFieldPreservesNulls) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto extra_fid =
        schema->AddDebugField("nullable_i64", DataType::INT64, true);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto raw_data = DataGen(schema, 4, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/3);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 3;
    sr.pk_type_ = DataType::INT64;
    sr.segment_ = segment.get();
    sr.seg_offsets_ = {0, 1, 2};
    sr.distances_ = {0.9f, 0.8f, 0.7f};
    sr.primary_keys_ = {
        PkType(int64_t(100)), PkType(int64_t(101)), PkType(int64_t(102))};
    sr.topk_per_nq_prefix_sum_ = {0, 3};

    int64_t extra_fields[] = {extra_fid.get()};
    ArrowArrayStream stream{};
    auto status = ExportSearchResultAsArrowStream(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        extra_fields,
        1,
        &stream);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto reader_result = arrow::ImportRecordBatchReader(&stream);
    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();

    auto batch_result = (*reader_result)->Next();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_rows(), 3);
    ASSERT_EQ((*batch_result)->num_columns(), 4);

    auto extra_field = (*batch_result)->schema()->field(3);
    EXPECT_EQ(extra_field->name(), "nullable_i64");
    EXPECT_TRUE(extra_field->nullable());

    auto extra_array = (*batch_result)->column(3);
    EXPECT_EQ(extra_array->null_count(), 1);
    EXPECT_FALSE(extra_array->IsNull(0));
    EXPECT_TRUE(extra_array->IsNull(1));
    EXPECT_FALSE(extra_array->IsNull(2));
}

// ---------------------------------------------------------------------------
// PrepareSearchResultsForExport — CGO entry for the pre-export reduce phase
// (filter invalid rows + optional Global Refine truncate/refine + fill PKs).
// Exercised end-to-end via real segment construction from DataGen.
// ---------------------------------------------------------------------------

TEST(SearchResultExport, PrepareSearchResultsForExport_NumSegmentsZero) {
    // Must assert when num_segments <= 0. No segment pointers accessed so this
    // is safe without any fixture; verifies the guard clause directly.
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {10};
    CTraceContext trace{0, 0, 0};
    auto status = PrepareSearchResultsForExport(trace,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                /*num_segments=*/0,
                                                slice_nqs,
                                                /*num_slices=*/1,
                                                slice_topks,
                                                nullptr);
    EXPECT_NE(status.error_code, 0);
    free(const_cast<char*>(status.error_msg));
}

// Helper: build a plan proto for a simple vector ANN search with no filter,
// no group_by, no global_refine. Returns the serialized plan bytes.
static std::string
BuildSimpleVectorSearchPlan(milvus::FieldId vec_fid, int topk) {
    namespace planpb = milvus::proto::plan;
    planpb::PlanNode plan_node;
    auto* vanns = plan_node.mutable_vector_anns();
    vanns->set_vector_type(planpb::VectorType::FloatVector);
    vanns->set_field_id(vec_fid.get());
    vanns->set_placeholder_tag("$0");
    auto* query_info = vanns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_metric_type("L2");
    query_info->set_search_params("{\"ef\": 16}");
    query_info->set_round_decimal(-1);
    return plan_node.SerializeAsString();
}

TEST(SearchResultExport, PrepareSearchResultsForExport_FillsPrimaryKeys) {
    using namespace milvus;
    using namespace milvus::segcore;

    // 1. Schema: int64 PK + float vector.
    int dim = 16;
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);

    // 2. Build 2 sealed segments with raw data; HNSW not needed since we only
    //    exercise the reduce (post-search) path — we will craft SearchResults
    //    manually to avoid running segment search.
    size_t N = 20;
    auto raw_data_a = DataGen(schema, N, /*seed=*/1);
    auto raw_data_b = DataGen(schema, N, /*seed=*/2);
    auto seg_a = CreateSealedWithFieldDataLoaded(schema, raw_data_a);
    auto seg_b = CreateSealedWithFieldDataLoaded(schema, raw_data_b);

    // 3. Build a minimal plan (no filter, no global_refine).
    int topk = 3;
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, topk);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    // 4. Build placeholder group (single query vector) — unused for this
    //    non-refine path but required by the CGO signature.
    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024);
    auto ph_group = milvus::query::ParsePlaceholderGroup(
        plan.get(), ph_group_raw.SerializeAsString());

    // 5. Craft SearchResults manually. 3 valid offsets + 1 invalid = checks
    //    FilterInvalidSearchResult drops the INVALID_SEG_OFFSET.
    SearchResult sr_a;
    sr_a.total_nq_ = 1;
    sr_a.unity_topK_ = 3;
    sr_a.total_data_cnt_ = N;
    sr_a.segment_ = seg_a.get();
    sr_a.seg_offsets_ = {0, INVALID_SEG_OFFSET, 5};
    sr_a.distances_ = {1.0f, 2.0f, 3.0f};

    SearchResult sr_b;
    sr_b.total_nq_ = 1;
    sr_b.unity_topK_ = 3;
    sr_b.total_data_cnt_ = N;
    sr_b.segment_ = seg_b.get();
    sr_b.seg_offsets_ = {1, 7, 10};
    sr_b.distances_ = {1.5f, 2.5f, 3.5f};

    SearchResult sr_no_hit;
    sr_no_hit.total_nq_ = 1;
    sr_no_hit.unity_topK_ = 3;
    sr_no_hit.total_data_cnt_ = N;
    sr_no_hit.segment_ = seg_b.get();
    sr_no_hit.seg_offsets_ = {
        INVALID_SEG_OFFSET, INVALID_SEG_OFFSET, INVALID_SEG_OFFSET};
    sr_no_hit.distances_ = {4.0f, 5.0f, 6.0f};

    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&sr_a),
        reinterpret_cast<CSearchResult>(&sr_b),
        reinterpret_cast<CSearchResult>(&sr_no_hit)};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {topk};
    CTraceContext trace{0, 0, 0};
    int64_t all_search_count = 0;

    // 6. Run prep.
    auto status = PrepareSearchResultsForExport(
        trace,
        reinterpret_cast<CSearchPlan>(plan.get()),
        reinterpret_cast<CPlaceholderGroup>(ph_group.get()),
        c_results.data(),
        c_results.size(),
        slice_nqs,
        /*num_slices=*/1,
        slice_topks,
        &all_search_count);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    ASSERT_EQ(all_search_count, sr_a.total_data_cnt_ + sr_b.total_data_cnt_);
    // status.error_msg is strdup'd on failure; intentionally leak in-test.

    // 7. Assertions on the mutated SearchResults:
    //    - Invalid row dropped from sr_a (3 → 2 rows).
    EXPECT_EQ(sr_a.seg_offsets_.size(), 2u);
    EXPECT_EQ(sr_a.distances_.size(), 2u);
    EXPECT_EQ(sr_a.seg_offsets_[0], 0);
    EXPECT_EQ(sr_a.seg_offsets_[1], 5);

    //    - sr_b unchanged (3 → 3 rows).
    EXPECT_EQ(sr_b.seg_offsets_.size(), 3u);

    //    - Primary keys filled. The int64 PKs are data-generated per row;
    //      we just verify the vector is populated to the correct length.
    EXPECT_EQ(sr_a.primary_keys_.size(), 2u);
    EXPECT_EQ(sr_b.primary_keys_.size(), 3u);
    EXPECT_EQ(sr_a.pk_type_, DataType::INT64);
    EXPECT_EQ(sr_b.pk_type_, DataType::INT64);

    //    - topk_per_nq_prefix_sum_ rebuilt: sr_a has 2 rows in NQ 0,
    //      sr_b has 3 rows in NQ 0.
    ASSERT_EQ(sr_a.topk_per_nq_prefix_sum_.size(), 2u);
    EXPECT_EQ(sr_a.topk_per_nq_prefix_sum_[0], 0);
    EXPECT_EQ(sr_a.topk_per_nq_prefix_sum_[1], 2);
    ASSERT_EQ(sr_b.topk_per_nq_prefix_sum_.size(), 2u);
    EXPECT_EQ(sr_b.topk_per_nq_prefix_sum_[0], 0);
    EXPECT_EQ(sr_b.topk_per_nq_prefix_sum_[1], 3);
}

// ---------------------------------------------------------------------------
// Global Refine — synthetic-data tests
//
// These tests drive ReduceHelper's refine-related private methods directly on
// hand-crafted SearchResult objects, so they exercise the merge/truncate
// logic without needing real sealed segments. The per-segment
// IsSearchResultRefineEnabled check is overridden via a test subclass so
// tests can force refine-capability on/off independently of the underlying
// knowhere index.
//
// Migrated from PR milvus-io/milvus#48895 (reduce_c_test.cpp, which was
// removed when the Go reduce pipeline replaced the C++ reduce-and-fill path in
// commit 52a5083ded).
// ---------------------------------------------------------------------------

namespace {

// Test subclass that exposes protected ReduceHelper methods and allows
// overriding IsSearchResultRefineEnabled per SearchResult or globally.
class TestReduceHelper : public ReduceHelper {
 public:
    using ReduceHelper::ApplyRefinedOrderForOneNQ;
    using ReduceHelper::CanUseGlobalRefine;
    using ReduceHelper::IsSearchResultRefineEnabled;
    using ReduceHelper::ReduceHelper;

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

// TruncateToRefineTopk: with refine_topk_ratio=0.5, topk=2, both segments
// lacking an explicit ef/search range → refine_topk =
// ceil(0.5 * max(slice_topk=2, search_range=0)) = 1. k-way merge across
// segments keeps only the single best (0.95 in seg0); seg1 loses all of its
// candidates.
TEST(SearchResultExport, GlobalRefineTruncate_MergesBeforeSegmentPruning) {
    auto schema = std::make_shared<Schema>();
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
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

    ASSERT_EQ(seg0.distances_.size(), 1u);
    ASSERT_EQ(seg0.seg_offsets_.size(), 1u);
    EXPECT_FLOAT_EQ(seg0.distances_[0], 0.95f);
    EXPECT_EQ(seg0.seg_offsets_[0], 100);
    EXPECT_EQ(seg0.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 1}));

    ASSERT_TRUE(seg1.distances_.empty());
    ASSERT_TRUE(seg1.seg_offsets_.empty());
    EXPECT_EQ(seg1.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 0}));
}

// TruncateToRefineTopk with mixed per-segment result sizes and ef=3:
// refine_topk = ceil(1.0 * max(slice_topk=2, ef=3)) = 3. Global
// top-3 by distance: 0.99 (seg0), 0.98 (seg1), 0.95 (seg0) — seg0 keeps 2,
// seg1 keeps 1.
TEST(SearchResultExport, GlobalRefineTruncate_HandlesMixedSegmentUnityTopk) {
    auto schema = std::make_shared<Schema>();
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
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

    ASSERT_EQ(seg0.distances_.size(), 2u);
    ASSERT_EQ(seg0.seg_offsets_.size(), 2u);
    EXPECT_FLOAT_EQ(seg0.distances_[0], 0.99f);
    EXPECT_FLOAT_EQ(seg0.distances_[1], 0.95f);
    EXPECT_EQ(seg0.seg_offsets_[0], 100);
    EXPECT_EQ(seg0.seg_offsets_[1], 101);
    EXPECT_EQ(seg0.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 2}));

    ASSERT_EQ(seg1.distances_.size(), 1u);
    ASSERT_EQ(seg1.seg_offsets_.size(), 1u);
    EXPECT_FLOAT_EQ(seg1.distances_[0], 0.98f);
    EXPECT_EQ(seg1.seg_offsets_[0], 200);
    EXPECT_EQ(seg1.topk_per_nq_prefix_sum_, std::vector<size_t>({0, 1}));
}

// CanUseGlobalRefine requires BOTH a non-empty placeholder group AND at
// least one segment reporting IsSearchResultRefineEnabled=true. Covers:
//   1. no placeholder → false even when segments are refine-capable
//   2. placeholder + all segments refine-capable → true
//   3. placeholder + all segments disabled → false
//   4. placeholder + mixed per-segment refine capability → true (any_of)
TEST(SearchResultExport, GlobalRefine_CanUseRequiresPlaceholderAndCapability) {
    auto schema = std::make_shared<Schema>();
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
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

    // Case 1: no placeholder — false even though we force refine-enabled.
    TestReduceHelper without_placeholder(
        search_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);
    without_placeholder.SetSearchResultRefineEnabledForTest(true);
    EXPECT_FALSE(without_placeholder.CanUseGlobalRefineForTest());

    // Build a minimal placeholder group.
    milvus::query::Placeholder placeholder;
    placeholder.num_of_queries_ = 1;
    placeholder.blob_.resize(sizeof(float), 0);
    milvus::query::PlaceholderGroup placeholder_group;
    placeholder_group.push_back(std::move(placeholder));

    // Case 2: placeholder present + refine-enabled → true.
    TestReduceHelper with_placeholder(search_results,
                                      &plan,
                                      &placeholder_group,
                                      slice_nqs,
                                      slice_topks,
                                      1,
                                      nullptr);
    with_placeholder.SetSearchResultRefineEnabledForTest(true);
    EXPECT_TRUE(with_placeholder.CanUseGlobalRefineForTest());

    // Case 3: placeholder present but all segments disabled → false.
    TestReduceHelper disabled_segment(search_results,
                                      &plan,
                                      &placeholder_group,
                                      slice_nqs,
                                      slice_topks,
                                      1,
                                      nullptr);
    disabled_segment.SetSearchResultRefineEnabledForTest(false);
    EXPECT_FALSE(disabled_segment.CanUseGlobalRefineForTest());

    // Case 4: placeholder present + mixed — seg0 disabled, seg1 enabled
    // should still return true (any_of semantics). SearchResult holds
    // unique_ptr members so we can't copy-construct; populate seg1 manually.
    SearchResult seg1;
    seg1.total_nq_ = seg0.total_nq_;
    seg1.unity_topK_ = seg0.unity_topK_;
    seg1.distances_ = seg0.distances_;
    seg1.seg_offsets_ = seg0.seg_offsets_;
    seg1.topk_per_nq_prefix_sum_ = seg0.topk_per_nq_prefix_sum_;
    std::vector<SearchResult*> mixed{&seg0, &seg1};
    int64_t mixed_nqs[] = {1};
    int64_t mixed_topks[] = {1};
    TestReduceHelper mixed_helper(
        mixed, &plan, &placeholder_group, mixed_nqs, mixed_topks, 1, nullptr);
    mixed_helper.SetSearchResultRefineEnabledForTest(&seg0, false);
    mixed_helper.SetSearchResultRefineEnabledForTest(&seg1, true);
    EXPECT_TRUE(mixed_helper.CanUseGlobalRefineForTest());
}

// RefineDistances iterates over segments; segments whose refine is disabled
// must be skipped without crashing, even when their SearchResult has rows
// (they retain coarse distances and are merged via the standard path).
TEST(SearchResultExport, GlobalRefine_SkipsDisabledSegmentsDuringRefine) {
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);

    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.field_id_ = field_id;
    plan.plan_node_->search_info_.metric_type_ = knowhere::metric::L2;
    plan.plan_node_->search_info_.global_refine_enable_ = true;

    milvus::query::Placeholder placeholder;
    placeholder.num_of_queries_ = 1;
    placeholder.blob_.resize(sizeof(float) * 4, 0);
    milvus::query::PlaceholderGroup placeholder_group;
    placeholder_group.push_back(std::move(placeholder));

    // Enabled segment has zero rows — nothing to refine, must not touch its
    // empty vectors. Disabled segment has one row that must be preserved as
    // coarse distance.
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
    // enabled_segment has no rows so it is a no-op; disabled_segment is
    // skipped by the refine-capability check and its coarse distance is
    // left untouched.
    helper.RefineDistancesForTest();
    EXPECT_EQ(disabled_segment.distances_.size(), 1u);
    EXPECT_FLOAT_EQ(disabled_segment.distances_[0], 0.5f);
}

// End-to-end: build a real sealed segment with IVF_FLAT interim index,
// run segment->Search to get a real SearchResult, then drive
// ReduceHelper::PreReduce() with refine-enabled forced on. This exercises
// PreReduce → TruncateToRefineTopk → RefineDistances → CalcDistByIDs →
// ApplyRefinedOrderForOneNQ and verifies the prepared SearchResult shape.
TEST(SearchResultExport, GlobalRefine_EndToEnd_ForcedSealedRefine) {
    using milvus::segcore::DataGen;
    using milvus::segcore::ScopedSchemaHandle;
    using milvus::segcore::SegcoreConfig;

    // --- 1. Schema: int64 PK + 16-dim float vector (L2). ---
    const int dim = 16;
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);

    // --- 2. IVF_FLAT index meta + interim-index config. ---
    //
    // Interim index auto-builds when row_count >= nlist * 39. nlist=16 ⇒
    // threshold=624. We insert 1000 rows, safely above the threshold.
    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", std::string{knowhere::metric::L2}},
        {"nlist", "16"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    milvus::FieldIndexMeta field_index_meta(
        vec_fid, std::move(index_params), std::move(type_params));
    std::map<FieldId, milvus::FieldIndexMeta> field_map;
    field_map.emplace(vec_fid, std::move(field_index_meta));
    auto collection_index_meta = std::make_shared<milvus::CollectionIndexMeta>(
        /*max_index_row_cnt=*/226985, std::move(field_map));

    auto& segcore_config = SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    segcore_config.set_nlist(16);
    segcore_config.set_chunk_rows(1024);

    // --- 3. Create sealed segment, load 1000 rows → interim index builds. ---
    const int N = 1000;
    auto segment =
        milvus::segcore::CreateSealedSegment(schema, collection_index_meta);
    auto dataset = DataGen(schema, N, /*seed=*/1);
    LoadGeneratedDataIntoSegment(dataset, segment.get());
    ASSERT_TRUE(segment->HasIndex(vec_fid))
        << "interim index should be built after loading 1000 rows with "
           "nlist=16 (threshold=624)";

    // --- 4. Build search plan carrying global-refine ratios. ---
    const int topK = 5;
    const int num_queries = 2;
    ScopedSchemaHandle schema_handle(*schema);
    auto plan_bytes =
        schema_handle.ParseSearch(/*expr=*/"",
                                  /*vector_field_name=*/"fakevec",
                                  /*topk=*/topK,
                                  /*metric_type=*/"L2",
                                  /*search_params=*/R"({"nprobe": 10})",
                                  /*round_decimal=*/-1,
                                  /*hints=*/"",
                                  /*materialized_view_involved=*/false,
                                  /*search_topk_ratio=*/2.0f,
                                  /*refine_topk_ratio=*/1.5f);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    // --- 5. Build placeholder group with num_queries float vectors. ---
    auto ph_group_raw =
        milvus::segcore::CreatePlaceholderGroup(num_queries, dim, /*seed=*/999);
    auto ph_group = milvus::query::ParsePlaceholderGroup(
        plan.get(), ph_group_raw.SerializeAsString());

    // --- 6. Execute search on the sealed segment. ---
    auto sr =
        segment->Search(plan.get(), ph_group.get(), milvus::MAX_TIMESTAMP);
    ASSERT_NE(sr, nullptr);
    ASSERT_EQ(sr->total_nq_, num_queries);

    // --- 7. Run PreReduce with forced refine-enabled. ---
    //
    // IsSearchResultRefineEnabled on a plain IVF_FLAT interim index returns
    // false in knowhere (no IndexRefine wrapper), so we force it true via
    // TestReduceHelper to actually drive the refine branch. CalcDistByIDs
    // will be called on the real segment; it may succeed or fail depending
    // on the interim index's capabilities, but the pipeline must complete.
    std::vector<SearchResult*> search_results;
    search_results.push_back(sr.get());
    int64_t slice_nqs[] = {num_queries};
    int64_t slice_topks[] = {topK};
    TestReduceHelper helper(search_results,
                            plan.get(),
                            ph_group.get(),
                            slice_nqs,
                            slice_topks,
                            /*slice_num=*/1,
                            /*trace_ctx=*/nullptr);
    helper.SetSearchResultRefineEnabledForTest(true);
    EXPECT_TRUE(helper.CanUseGlobalRefineForTest());
    helper.PreReduce();

    // --- 8. Sanity-check prepared SearchResult shape/finiteness. ---
    ASSERT_EQ(sr->topk_per_nq_prefix_sum_.size(),
              static_cast<size_t>(num_queries + 1));
    ASSERT_EQ(sr->distances_.size(), sr->seg_offsets_.size());
    ASSERT_EQ(sr->distances_.size(), sr->primary_keys_.size());
    for (int i = 0; i < num_queries; ++i) {
        auto real_topk = sr->topk_per_nq_prefix_sum_[i + 1] -
                         sr->topk_per_nq_prefix_sum_[i];
        EXPECT_GT(real_topk, 0);
        EXPECT_LE(real_topk, static_cast<size_t>(std::ceil(1.5 * topK)));
    }
    for (auto score : sr->distances_) {
        EXPECT_FALSE(std::isnan(score));
        EXPECT_FALSE(std::isinf(score));
    }
}
