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

#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/PrometheusClient.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Json.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/cgo_msg.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
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

static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
ImportExportedRecordBatch(ArrowArray* array, ArrowSchema* schema) {
    ARROW_ASSIGN_OR_RAISE(auto imported_schema, arrow::ImportSchema(schema));
    return arrow::ImportRecordBatch(array, imported_schema);
}

static milvus::proto::cgo::FunctionChainInput
ScalarInputProjection(FieldId field_id,
                      DataType data_type,
                      std::string_view name) {
    milvus::proto::cgo::FunctionChainInput input;
    input.set_source_field_id(field_id.get());
    input.set_target_data_type(
        static_cast<milvus::proto::schema::DataType>(data_type));
    input.set_logical_name(std::string(name));
    return input;
}

static milvus::proto::cgo::FunctionChainInput
JSONInputProjection(FieldId field_id,
                    DataType data_type,
                    std::string_view name,
                    const std::vector<std::string>& path) {
    auto input = ScalarInputProjection(field_id, data_type, name);
    input.set_is_json_path(true);
    for (const auto& token : path) {
        input.add_nested_path(token);
    }
    return input;
}

static std::string
SerializeInputPlan(
    std::initializer_list<milvus::proto::cgo::FunctionChainInput> inputs) {
    milvus::proto::cgo::FunctionChainInputPlan plan;
    for (const auto& input : inputs) {
        *plan.add_inputs() = input;
    }
    return plan.SerializeAsString();
}

static void
ReleaseChunkSizes(int64_t* chunk_sizes) {
    free(chunk_sizes);
}

using ChunkSizesPtr = std::unique_ptr<int64_t, decltype(&ReleaseChunkSizes)>;

static ChunkSizesPtr
AdoptChunkSizes(int64_t* chunk_sizes) {
    return ChunkSizesPtr(chunk_sizes, ReleaseChunkSizes);
}

static uint64_t
CGOCallMetricCount(std::string_view metrics, std::string_view function) {
    auto marker = std::string("milvus_cgocall_duration_seconds_count{func=\"") +
                  std::string(function) + "\"} ";
    auto value_begin = metrics.find(marker);
    if (value_begin == std::string_view::npos) {
        return 0;
    }
    value_begin += marker.size();
    auto value_end = metrics.find('\n', value_begin);
    return std::stoull(
        std::string(metrics.substr(value_begin, value_end - value_begin)));
}

static void
AttachSealedRequestLease(SearchResult& result,
                         milvus::segcore::SegmentInterface* segment) {
    auto* sealed =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(segment);
    ASSERT_NE(sealed, nullptr);
    result.segment_ = segment;
    result.read_lease_ = sealed->AcquireReadLease(folly::CancellationToken());
}

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

TEST(SearchResultExport, SortEqualScoresByPks_StringPk) {
    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 4;

    sr.distances_ = {1.0f, 1.0f, 1.0f, 1.0f};
    sr.seg_offsets_ = {30, 10, 40, 20};
    sr.primary_keys_ = {PkType(std::string("pk-300")),
                        PkType(std::string("pk-100")),
                        PkType(std::string("pk-400")),
                        PkType(std::string("pk-200"))};
    sr.topk_per_nq_prefix_sum_ = {0, 4};

    SortEqualScoresByPks(&sr);

    EXPECT_EQ(std::get<std::string>(sr.primary_keys_[0]), "pk-100");
    EXPECT_EQ(std::get<std::string>(sr.primary_keys_[1]), "pk-200");
    EXPECT_EQ(std::get<std::string>(sr.primary_keys_[2]), "pk-300");
    EXPECT_EQ(std::get<std::string>(sr.primary_keys_[3]), "pk-400");

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
     ExportSearchResultAsArrowRecordBatchWithInputPlan_RejectsUnpreparedRows) {
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

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    EXPECT_NE(status.error_code, 0);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("topk_per_nq_prefix_sum_"),
              std::string::npos)
        << status.error_msg;
    free(const_cast<char*>(status.error_msg));
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_AllowsEmptyResultWithoutPrefix) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 0;

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    EXPECT_EQ((*batch_result)->num_columns(), 3);
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyPlanPreservesChunksAndCancellation) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();

    SearchResult sr;
    sr.total_nq_ = 3;
    sr.unity_topK_ = 2;
    sr.pk_type_ = DataType::INT64;
    sr.seg_offsets_ = {10, 20, 30};
    sr.distances_ = {0.9f, 0.8f, 0.7f};
    sr.primary_keys_ = {
        PkType(int64_t(100)), PkType(int64_t(200)), PkType(int64_t(300))};
    sr.topk_per_nq_prefix_sum_ = {0, 2, 2, 3};

    for (bool cancelled : {false, true}) {
        folly::CancellationSource cancellation;
        if (cancelled) {
            cancellation.requestCancellation();
        }
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        int64_t* chunk_sizes = nullptr;
        int64_t num_chunks = 0;
        auto before = CGOCallMetricCount(
            milvus::monitor::getPrometheusClient().GetMetrics(),
            "ExportSearchResultAsArrowRecordBatchWithInputPlan");
        auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
            reinterpret_cast<CSearchResult>(&sr),
            reinterpret_cast<CSearchPlan>(&plan),
            nullptr,
            0,
            &out_schema,
            &out_array,
            &chunk_sizes,
            &num_chunks,
            &cancellation);
        [[maybe_unused]] auto chunks_guard = AdoptChunkSizes(chunk_sizes);
        EXPECT_EQ(CGOCallMetricCount(
                      milvus::monitor::getPrometheusClient().GetMetrics(),
                      "ExportSearchResultAsArrowRecordBatchWithInputPlan"),
                  before + 1);
        if (cancelled) {
            EXPECT_EQ(status.error_code,
                      static_cast<int>(milvus::ErrorCode::FollyCancel));
            EXPECT_EQ(out_schema.release, nullptr);
            EXPECT_EQ(out_array.release, nullptr);
            EXPECT_EQ(chunk_sizes, nullptr);
            EXPECT_EQ(num_chunks, 0);
            free(const_cast<char*>(status.error_msg));
            continue;
        }
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        ASSERT_EQ(num_chunks, 3);
        EXPECT_EQ(chunk_sizes[0], 2);
        EXPECT_EQ(chunk_sizes[1], 0);
        EXPECT_EQ(chunk_sizes[2], 1);
        auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        ASSERT_EQ((*batch)->num_columns(), 3);
        ASSERT_EQ((*batch)->num_rows(), 3);
        EXPECT_EQ((*batch)->schema()->field(0)->name(), "$id");
        EXPECT_EQ((*batch)->schema()->field(1)->name(), "$score");
        EXPECT_EQ((*batch)->schema()->field(2)->name(), "$seg_offset");
        auto ids =
            std::static_pointer_cast<arrow::Int64Array>((*batch)->column(0));
        auto scores =
            std::static_pointer_cast<arrow::FloatArray>((*batch)->column(1));
        auto offsets =
            std::static_pointer_cast<arrow::Int64Array>((*batch)->column(2));
        for (int row = 0; row < 3; ++row) {
            EXPECT_EQ(ids->Value(row), (row + 1) * 100);
            EXPECT_FLOAT_EQ(scores->Value(row), sr.distances_[row]);
            EXPECT_EQ(offsets->Value(row), sr.seg_offsets_[row]);
        }
    }
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyElementLevelResultHasElementIndices) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 0;
    sr.element_level_ = true;

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    ASSERT_EQ((*batch_result)->num_columns(), 4);
    EXPECT_EQ((*batch_result)->schema()->field(3)->name(), "$element_indices");
    EXPECT_TRUE(
        (*batch_result)->schema()->field(3)->type()->Equals(arrow::int32()));
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_MultiFieldGroupByColumns) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid_1 = schema->AddDebugField("group1", DataType::INT64);
    auto group_fid_2 = schema->AddDebugField("group2", DataType::VARCHAR);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid_1,
                                                         group_fid_2};

    auto make_group_key = [](GroupByValueType first, GroupByValueType second) {
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

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
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
    auto group_by_array_2 = std::static_pointer_cast<arrow::StringArray>(
        (*batch_result)->column(4));
    EXPECT_EQ(group_by_array_2->GetString(0), "a");
    EXPECT_EQ(group_by_array_2->GetString(1), "b");
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyGroupByLayout) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid = schema->AddDebugField("group", DataType::INT64);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid};

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 10;
    sr.pk_type_ = DataType::INT64;
    sr.group_size_ = 3;
    sr.topk_per_nq_prefix_sum_ = {0, 0};
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{};

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    ASSERT_EQ((*batch_result)->num_columns(), 4);

    auto group_by_field = (*batch_result)->schema()->field(3);
    EXPECT_EQ(group_by_field->name(),
              "$group_by_" + std::to_string(group_fid.get()));
    EXPECT_EQ(group_by_field->type()->id(), arrow::Type::INT64);
    EXPECT_TRUE(group_by_field->nullable());

    auto group_by_array =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(3));
    EXPECT_EQ(group_by_array->length(), 0);
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyMultiFieldGroupByLayout) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto group_fid_1 = schema->AddDebugField("group1", DataType::INT64);
    auto group_fid_2 = schema->AddDebugField("group2", DataType::VARCHAR);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {group_fid_1,
                                                         group_fid_2};

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 10;
    sr.pk_type_ = DataType::INT64;
    sr.group_size_ = 3;
    sr.topk_per_nq_prefix_sum_ = {0, 0};
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{};

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    ASSERT_EQ((*batch_result)->num_columns(), 5);

    auto group_by_field_1 = (*batch_result)->schema()->field(3);
    EXPECT_EQ(group_by_field_1->name(),
              "$group_by_" + std::to_string(group_fid_1.get()));
    EXPECT_EQ(group_by_field_1->type()->id(), arrow::Type::INT64);
    EXPECT_TRUE(group_by_field_1->nullable());
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
    auto group_by_array_1 =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(3));
    EXPECT_EQ(group_by_array_1->length(), 0);

    auto group_by_field_2 = (*batch_result)->schema()->field(4);
    EXPECT_EQ(group_by_field_2->name(),
              "$group_by_" + std::to_string(group_fid_2.get()));
    EXPECT_EQ(group_by_field_2->type()->id(), arrow::Type::STRING);
    EXPECT_TRUE(group_by_field_2->nullable());
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
    auto group_by_array_2 = std::static_pointer_cast<arrow::StringArray>(
        (*batch_result)->column(4));
    EXPECT_EQ(group_by_array_2->length(), 0);
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_AllowsSingleJsonFieldWithScalarGroupBy) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json_group", DataType::JSON);
    auto scalar_fid = schema->AddDebugField("scalar_group", DataType::INT64);
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.group_by_field_ids_ = {json_fid, scalar_fid};
    plan.plan_node_->search_info_.json_path_ = "/brand";
    plan.plan_node_->search_info_.json_type_ = DataType::VARCHAR;

    SearchResult sr;
    sr.total_nq_ = 1;
    sr.unity_topK_ = 10;
    sr.pk_type_ = DataType::INT64;
    sr.group_size_ = 3;
    sr.topk_per_nq_prefix_sum_ = {0, 0};
    sr.composite_group_by_values_ = std::vector<milvus::CompositeGroupKey>{};

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    EXPECT_EQ((*batch_result)->num_rows(), 0);
    ASSERT_EQ((*batch_result)->num_columns(), 5);

    auto json_group_by_field = (*batch_result)->schema()->field(3);
    EXPECT_EQ(json_group_by_field->name(),
              "$group_by_" + std::to_string(json_fid.get()));
    EXPECT_EQ(json_group_by_field->type()->id(), arrow::Type::STRING);
    ASSERT_NE(json_group_by_field->metadata(), nullptr);
    auto json_data_type_result =
        json_group_by_field->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(json_data_type_result.ok())
        << json_data_type_result.status().ToString();
    EXPECT_EQ(*json_data_type_result,
              std::to_string(static_cast<int32_t>(DataType::VARCHAR)));

    auto scalar_group_by_field = (*batch_result)->schema()->field(4);
    EXPECT_EQ(scalar_group_by_field->name(),
              "$group_by_" + std::to_string(scalar_fid.get()));
    EXPECT_EQ(scalar_group_by_field->type()->id(), arrow::Type::INT64);
    ASSERT_NE(scalar_group_by_field->metadata(), nullptr);
    auto scalar_data_type_result =
        scalar_group_by_field->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(scalar_data_type_result.ok())
        << scalar_data_type_result.status().ToString();
    EXPECT_EQ(*scalar_data_type_result,
              std::to_string(static_cast<int32_t>(DataType::INT64)));
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_RejectsMultiJsonFieldGroupBy) {
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

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    EXPECT_NE(status.error_code, 0);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(
        std::string(status.error_msg).find("at most one JSON group_by field"),
        std::string::npos)
        << status.error_msg;
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowRecordBatchWithInputPlan_GroupByMetadata) {
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

    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(&plan),
        nullptr,
        0,
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
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

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_NullableExtraFieldPreservesNulls) {
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
    AttachSealedRequestLease(sr, segment.get());
    sr.seg_offsets_ = {0, 1, 2};
    sr.distances_ = {0.9f, 0.8f, 0.7f};
    sr.primary_keys_ = {
        PkType(int64_t(100)), PkType(int64_t(101)), PkType(int64_t(102))};
    sr.topk_per_nq_prefix_sum_ = {0, 3};

    auto input_plan = SerializeInputPlan({ScalarInputProjection(
        extra_fid,
        schema->operator[](extra_fid).get_data_type(),
        schema->operator[](extra_fid).get_name().get())});
    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
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

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyExtraFieldNarrowIntsUseDeclaredType) {
    using namespace milvus;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto int8_fid = schema->AddDebugField("extra_i8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("extra_i16", DataType::INT16);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    SearchResult empty_sr;
    empty_sr.total_nq_ = 1;
    empty_sr.unity_topK_ = 0;
    empty_sr.pk_type_ = DataType::INT64;

    auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(int8_fid, DataType::INT8, "extra_i8"),
         ScalarInputProjection(int16_fid, DataType::INT16, "extra_i16")});
    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&empty_sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_columns(), 5);
    EXPECT_TRUE(
        (*batch_result)->schema()->field(3)->type()->Equals(arrow::int8()));
    EXPECT_TRUE(
        (*batch_result)->schema()->field(4)->type()->Equals(arrow::int16()));
}

TEST(SearchResultExport, FunctionChainInputPlanRejectsMalformedPayload) {
    using namespace milvus;
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto scalar_fid = schema->AddDebugField("scalar", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, 1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    auto c_result = reinterpret_cast<CSearchResult>(&result);
    auto scalar_with_path =
        ScalarInputProjection(scalar_fid, DataType::INT64, "scalar");
    scalar_with_path.add_nested_path("key");
    const std::vector<std::string> invalid_plans = {
        std::string(1, '\xff'),
        std::string("\x10\x01", 2),  // Unknown field only: no inputs.
        SerializeInputPlan(
            {ScalarInputProjection(scalar_fid, DataType::INT64, "")}),
        SerializeInputPlan({scalar_with_path}),
        SerializeInputPlan(
            {JSONInputProjection(json_fid, DataType::INT64, "value", {})}),
        SerializeInputPlan(
            {JSONInputProjection(json_fid, DataType::JSON, "value", {"key"})}),
        SerializeInputPlan({JSONInputProjection(
            scalar_fid, DataType::INT64, "value", {"key"})}),
        SerializeInputPlan(
            {ScalarInputProjection(scalar_fid, DataType::DOUBLE, "scalar")}),
    };
    for (const auto& blob : invalid_plans) {
        for (bool l0 : {true, false}) {
            ArrowSchema out_schema{};
            ArrowArray out_array{};
            int64_t* chunks = nullptr;
            int64_t num_chunks = 0;
            auto status =
                l0 ? ExportSearchResultAsArrowRecordBatchWithInputPlan(
                         c_result,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         blob.data(),
                         blob.size(),
                         &out_schema,
                         &out_array,
                         &chunks,
                         &num_chunks,
                         nullptr)
                   : FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
                         &c_result,
                         1,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         blob.data(),
                         blob.size(),
                         nullptr,
                         nullptr,
                         0,
                         &out_schema,
                         &out_array,
                         nullptr);
            EXPECT_NE(status.error_code, 0);
            EXPECT_NE(status.error_code, milvus::DataFormatBroken);
            EXPECT_EQ(out_schema.release, nullptr);
            EXPECT_EQ(out_array.release, nullptr);
            EXPECT_EQ(chunks, nullptr);
            free(const_cast<char*>(status.error_msg));
        }
    }
}

TEST(SearchResultExport, FunctionChainInputPlanPreservesPathTokens) {
    using namespace milvus;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 1, 1);
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() == json_fid.get()) {
            field.mutable_scalars()->mutable_json_data()->set_data(
                0, R"({"a/b":{"~1":{"中文":{"key":42,"nul\u0000key":43}}}})");
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, 1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    result.total_nq_ = 1;
    result.unity_topK_ = 1;
    result.pk_type_ = DataType::INT64;
    result.seg_offsets_ = {0};
    result.primary_keys_ = {PkType(int64_t(1))};
    result.distances_ = {1.0f};
    result.topk_per_nq_prefix_sum_ = {0, 1};
    auto c_result = reinterpret_cast<CSearchResult>(&result);
    const auto blob = SerializeInputPlan({
        JSONInputProjection(json_fid,
                            DataType::INT64,
                            "projected",
                            {"a/b", "~1", "中文", "key"}),
        JSONInputProjection(json_fid,
                            DataType::INT64,
                            "escaped",
                            {"a/b", "~1", "中文", std::string("nul\0key", 7)}),
    });
    for (bool l0 : {true, false}) {
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        int64_t* chunks = nullptr;
        int64_t num_chunks = 0;
        int32_t seg_index = 0;
        int64_t offset = 0;
        auto status = l0 ? ExportSearchResultAsArrowRecordBatchWithInputPlan(
                               c_result,
                               reinterpret_cast<CSearchPlan>(plan.get()),
                               blob.data(),
                               blob.size(),
                               &out_schema,
                               &out_array,
                               &chunks,
                               &num_chunks,
                               nullptr)
                         : FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
                               &c_result,
                               1,
                               reinterpret_cast<CSearchPlan>(plan.get()),
                               blob.data(),
                               blob.size(),
                               &seg_index,
                               &offset,
                               1,
                               &out_schema,
                               &out_array,
                               nullptr);
        auto chunks_guard = AdoptChunkSizes(chunks);
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        auto values = std::dynamic_pointer_cast<arrow::Int64Array>(
            (*batch)->GetColumnByName("projected"));
        ASSERT_NE(values, nullptr);
        ASSERT_FALSE(values->IsNull(0));
        EXPECT_EQ(values->Value(0), 42);
        // The common On-Demand reader matches raw key text, not an unescaped
        // NUL token against the JSON spelling "nul\\u0000key".
        EXPECT_TRUE((*batch)->GetColumnByName("escaped")->IsNull(0));
    }
}

TEST(SearchResultExport, FunctionChainNarrowIntegersPreserveDeclaredTypes) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto int8_fid = schema->AddDebugField("value_i8", DataType::INT8, true);
    auto int16_fid = schema->AddDebugField("value_i16", DataType::INT16, true);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 3, /*seed=*/1);
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() != int8_fid.get() &&
            field.field_id() != int16_fid.get()) {
            continue;
        }
        auto* values =
            field.mutable_scalars()->mutable_int_data()->mutable_data();
        values->Clear();
        values->Add(field.field_id() == int8_fid.get() ? -128 : -32768);
        values->Add(0);
        values->Add(field.field_id() == int8_fid.get() ? 127 : 32767);
        auto* valid = MutableFieldDataRowValidData(&field);
        valid->Clear();
        valid->Add(true);
        valid->Add(false);
        valid->Add(true);
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/3);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(int8_fid, DataType::INT8, "value_i8"),
         ScalarInputProjection(int16_fid, DataType::INT16, "value_i16")});

    // Cover empty, all-null, and reordered rows containing both signed limits.
    for (const auto& offsets :
         std::vector<std::vector<int64_t>>{{}, {1}, {2, 1, 0}}) {
        for (bool l0 : {true, false}) {
            SCOPED_TRACE(l0 ? "L0" : "L1");
            SCOPED_TRACE(offsets.size());
            SearchResult result;
            AttachSealedRequestLease(result, segment.get());
            result.total_nq_ = 1;
            result.unity_topK_ = offsets.size();
            result.pk_type_ = DataType::INT64;
            result.seg_offsets_ = offsets;
            result.topk_per_nq_prefix_sum_ = {0, offsets.size()};
            for (auto offset : offsets) {
                result.primary_keys_.emplace_back(int64_t(offset));
                result.distances_.push_back(static_cast<float>(offset));
            }
            CSearchResult c_result = reinterpret_cast<CSearchResult>(&result);
            std::vector<int32_t> segment_indices(offsets.size(), 0);
            ArrowSchema out_schema{};
            ArrowArray out_array{};
            int64_t* chunk_sizes = nullptr;
            int64_t num_chunks = 0;
            auto status =
                l0 ? ExportSearchResultAsArrowRecordBatchWithInputPlan(
                         c_result,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         input_plan.data(),
                         input_plan.size(),
                         &out_schema,
                         &out_array,
                         &chunk_sizes,
                         &num_chunks,
                         nullptr)
                   : FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
                         &c_result,
                         1,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         input_plan.data(),
                         input_plan.size(),
                         segment_indices.data(),
                         offsets.data(),
                         offsets.size(),
                         &out_schema,
                         &out_array,
                         nullptr);
            auto chunk_sizes_guard = AdoptChunkSizes(chunk_sizes);
            ASSERT_EQ(status.error_code, 0) << status.error_msg;
            auto batch_result =
                ImportExportedRecordBatch(&out_array, &out_schema);
            ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
            auto batch = *batch_result;
            auto i8 = std::dynamic_pointer_cast<arrow::Int8Array>(
                batch->GetColumnByName("value_i8"));
            auto i16 = std::dynamic_pointer_cast<arrow::Int16Array>(
                batch->GetColumnByName("value_i16"));
            ASSERT_NE(i8, nullptr);
            ASSERT_NE(i16, nullptr);
            ASSERT_EQ(i8->length(), offsets.size());
            ASSERT_EQ(i16->length(), offsets.size());
            for (size_t i = 0; i < offsets.size(); ++i) {
                EXPECT_EQ(i8->IsNull(i), offsets[i] == 1);
                EXPECT_EQ(i16->IsNull(i), offsets[i] == 1);
                if (offsets[i] != 1) {
                    EXPECT_EQ(i8->Value(i), offsets[i] == 0 ? -128 : 127);
                    EXPECT_EQ(i16->Value(i), offsets[i] == 0 ? -32768 : 32767);
                }
            }
        }
    }
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_EmptyGeometryExtraField) {
    using namespace milvus;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto geom_fid = schema->AddDebugField("extra_geom", DataType::GEOMETRY);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    SearchResult empty_sr;
    empty_sr.total_nq_ = 1;
    empty_sr.unity_topK_ = 0;
    empty_sr.pk_type_ = DataType::INT64;

    auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(geom_fid, DataType::GEOMETRY, "extra_geom")});
    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&empty_sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto batch = *batch_result;
    EXPECT_EQ(batch->num_rows(), 0);
    auto geom_col = batch->GetColumnByName("extra_geom");
    ASSERT_NE(geom_col, nullptr);
    EXPECT_TRUE(geom_col->type()->Equals(arrow::binary()));
}

TEST(
    SearchResultExport,
    ExportSearchResultAsArrowRecordBatchWithInputPlan_ExtraFieldOrderMatchesRequest) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto extra_low_fid = schema->AddDebugField("extra_low", DataType::INT64);
    auto extra_high_fid = schema->AddDebugField("extra_high", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto raw_data = DataGen(schema, 4, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(extra_high_fid, DataType::INT64, "extra_high"),
         ScalarInputProjection(extra_low_fid, DataType::INT64, "extra_low")});

    SearchResult empty_sr;
    empty_sr.total_nq_ = 1;
    empty_sr.unity_topK_ = 0;
    empty_sr.pk_type_ = DataType::INT64;

    ArrowSchema empty_stream_schema{};
    ArrowArray empty_stream_array{};
    int64_t* empty_stream_chunk_sizes = nullptr;
    int64_t empty_stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&empty_sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &empty_stream_schema,
        &empty_stream_array,
        &empty_stream_chunk_sizes,
        &empty_stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto empty_stream_chunk_sizes_guard =
        AdoptChunkSizes(empty_stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto empty_batch_result =
        ImportExportedRecordBatch(&empty_stream_array, &empty_stream_schema);
    ASSERT_TRUE(empty_batch_result.ok())
        << empty_batch_result.status().ToString();
    ASSERT_NE(*empty_batch_result, nullptr);
    ASSERT_EQ((*empty_batch_result)->num_columns(), 5);

    SearchResult non_empty_sr;
    non_empty_sr.total_nq_ = 1;
    non_empty_sr.unity_topK_ = 2;
    non_empty_sr.pk_type_ = DataType::INT64;
    AttachSealedRequestLease(non_empty_sr, segment.get());
    non_empty_sr.seg_offsets_ = {0, 1};
    non_empty_sr.distances_ = {0.9f, 0.8f};
    non_empty_sr.primary_keys_ = {PkType(int64_t(100)), PkType(int64_t(101))};
    non_empty_sr.topk_per_nq_prefix_sum_ = {0, 2};

    ArrowSchema non_empty_stream_schema{};
    ArrowArray non_empty_stream_array{};
    int64_t* non_empty_stream_chunk_sizes = nullptr;
    int64_t non_empty_stream_num_chunks = 0;
    status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&non_empty_sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &non_empty_stream_schema,
        &non_empty_stream_array,
        &non_empty_stream_chunk_sizes,
        &non_empty_stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto non_empty_stream_chunk_sizes_guard =
        AdoptChunkSizes(non_empty_stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto non_empty_batch_result = ImportExportedRecordBatch(
        &non_empty_stream_array, &non_empty_stream_schema);
    ASSERT_TRUE(non_empty_batch_result.ok())
        << non_empty_batch_result.status().ToString();
    ASSERT_NE(*non_empty_batch_result, nullptr);
    ASSERT_EQ((*non_empty_batch_result)->num_columns(), 5);

    EXPECT_EQ((*empty_batch_result)->schema()->field(3)->name(), "extra_high");
    EXPECT_EQ((*empty_batch_result)->schema()->field(4)->name(), "extra_low");
    EXPECT_EQ((*non_empty_batch_result)->schema()->field(3)->name(),
              (*empty_batch_result)->schema()->field(3)->name());
    EXPECT_EQ((*non_empty_batch_result)->schema()->field(4)->name(),
              (*empty_batch_result)->schema()->field(4)->name());
}

TEST(SearchResultExport,
     ExportSearchResultAsArrowRecordBatchWithInputPlan_ExtraFieldIds) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto extra_fid = schema->AddDebugField("extra_i64", DataType::INT64);
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
    AttachSealedRequestLease(sr, segment.get());
    sr.seg_offsets_ = {0, 1, 2};
    sr.distances_ = {0.9f, 0.8f, 0.7f};
    sr.primary_keys_ = {
        PkType(int64_t(100)), PkType(int64_t(101)), PkType(int64_t(102))};
    sr.topk_per_nq_prefix_sum_ = {0, 3};

    auto input_plan = SerializeInputPlan({ScalarInputProjection(
        extra_fid,
        schema->operator[](extra_fid).get_data_type(),
        schema->operator[](extra_fid).get_name().get())});
    ArrowSchema stream_schema{};
    ArrowArray stream_array{};
    int64_t* stream_chunk_sizes = nullptr;
    int64_t stream_num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&sr),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &stream_schema,
        &stream_array,
        &stream_chunk_sizes,
        &stream_num_chunks,
        nullptr);
    [[maybe_unused]] auto stream_chunk_sizes_guard =
        AdoptChunkSizes(stream_chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result =
        ImportExportedRecordBatch(&stream_array, &stream_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_columns(), 4);

    auto extra_field = (*batch_result)->schema()->field(3);
    EXPECT_EQ(extra_field->name(), "extra_i64");
}

TEST(SearchResultExport,
     FillFieldsOrderedAsArrowRecordBatchWithInputPlan_EmptyRowsPreserveSchema) {
    using namespace milvus;

    auto schema = std::make_shared<Schema>();
    auto int8_fid = schema->AddDebugField("empty_i8", DataType::INT8);
    auto int64_fid = schema->AddDebugField("empty_i64", DataType::INT64);
    auto string_fid = schema->AddDebugField("empty_string", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = milvus::segcore::DataGen(schema, 1, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(int8_fid, DataType::INT8, "empty_i8"),
         ScalarInputProjection(int64_fid, DataType::INT64, "empty_i64"),
         ScalarInputProjection(string_fid, DataType::VARCHAR, "empty_string")});
    ArrowSchema out_schema{};
    ArrowArray out_array{};

    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        nullptr,
        nullptr,
        0,
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_rows(), 0);
    ASSERT_EQ((*batch_result)->num_columns(), 3);
    EXPECT_TRUE(
        (*batch_result)->schema()->field(0)->type()->Equals(arrow::int8()));
    EXPECT_TRUE(
        (*batch_result)->schema()->field(1)->type()->Equals(arrow::int64()));
    EXPECT_TRUE(
        (*batch_result)->schema()->field(2)->type()->Equals(arrow::utf8()));
    EXPECT_EQ((*batch_result)->schema()->field(0)->name(), "empty_i8");
    EXPECT_EQ((*batch_result)->schema()->field(1)->name(), "empty_i64");
    EXPECT_EQ((*batch_result)->schema()->field(2)->name(), "empty_string");
}

TEST(
    SearchResultExport,
    FillFieldsOrderedAsArrowRecordBatchWithInputPlan_PreservesFieldOrderAndMetadata) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto int64_fid = schema->AddDebugField("ordered_i64", DataType::INT64);
    auto string_fid =
        schema->AddDebugField("ordered_string", DataType::VARCHAR);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 3, /*seed=*/1);
    auto int64_values = raw_data.get_col<int64_t>(int64_fid);
    auto string_values = raw_data.get_col<std::string>(string_fid);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(string_fid, DataType::VARCHAR, "ordered_string"),
         ScalarInputProjection(int64_fid, DataType::INT64, "ordered_i64")});
    int32_t segment_indices[] = {0, 0};
    int64_t segment_offsets[] = {2, 0};
    ArrowSchema out_schema{};
    ArrowArray out_array{};

    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        2,
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_columns(), 2);
    EXPECT_EQ((*batch_result)->schema()->field(0)->name(), "ordered_string");
    EXPECT_EQ((*batch_result)->schema()->field(1)->name(), "ordered_i64");
    EXPECT_EQ((*batch_result)->schema()->field(0)->type()->id(),
              arrow::Type::STRING);
    EXPECT_EQ((*batch_result)->schema()->field(1)->type()->id(),
              arrow::Type::INT64);
    for (int i = 0; i < 2; ++i) {
        const auto& field = (*batch_result)->schema()->field(i);
        ASSERT_NE(field->metadata(), nullptr);
        auto field_id_result = field->metadata()->Get("milvus.field_id");
        ASSERT_TRUE(field_id_result.ok())
            << field_id_result.status().ToString();
        auto data_type_result = field->metadata()->Get("milvus.data_type");
        ASSERT_TRUE(data_type_result.ok())
            << data_type_result.status().ToString();
        if (i == 0) {
            EXPECT_EQ(*field_id_result, std::to_string(string_fid.get()));
            EXPECT_EQ(*data_type_result,
                      std::to_string(static_cast<int32_t>(DataType::VARCHAR)));
        } else {
            EXPECT_EQ(*field_id_result, std::to_string(int64_fid.get()));
            EXPECT_EQ(*data_type_result,
                      std::to_string(static_cast<int32_t>(DataType::INT64)));
        }
    }
    auto strings = std::static_pointer_cast<arrow::StringArray>(
        (*batch_result)->column(0));
    auto ints =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(1));
    EXPECT_EQ(strings->GetString(0), string_values[2]);
    EXPECT_EQ(strings->GetString(1), string_values[0]);
    EXPECT_EQ(ints->Value(0), int64_values[2]);
    EXPECT_EQ(ints->Value(1), int64_values[0]);
}

TEST(SearchResultExport,
     FunctionChainProjectionAPIsProjectJSONPathsAndPreserveScalarFields) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scalar_fid = schema->AddDebugField("scalar", DataType::INT64);
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto raw_data = DataGen(schema, 5, /*seed=*/1);
    const std::vector<std::string> json_rows = {
        // Duplicate keys must not append a second value or override the first
        // occurrence, even when the first occurrence is missing/mismatched.
        R"({"i":10,"i":99,"d":1.5,"non_numeric_d":null,"b":true,"items":[{"name":"alpha","name":"ignored"}],"unrelated":1e400})",
        R"({"i":"bad","i":42,"d":-1e-400,"non_numeric_d":false,"b":false,"items":[],"items":[{"name":"ignored"}]})",
        R"({"i":30,"d":1e-400,"non_numeric_d":"bad","b":"bad","items":[{"name":3}]})",
        R"({"d":1e400,"non_numeric_d":{}})",
        R"({"d":0e999999999999999999999999   ,"non_numeric_d":[]})",
    };
    bool found_json = false;
    for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
        if (field_data.field_id() != json_fid.get()) {
            continue;
        }
        found_json = true;
        auto* values =
            field_data.mutable_scalars()->mutable_json_data()->mutable_data();
        values->Clear();
        for (const auto& row : json_rows) {
            values->Add(std::string(row));
        }
    }
    ASSERT_TRUE(found_json);
    auto scalar_values = raw_data.get_col<int64_t>(scalar_fid);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/5);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kIntName = R"(metadata["i"])";
    constexpr auto kIntAliasName = R"(metadata['i'])";
    constexpr auto kDoubleName = R"(metadata["d"])";
    constexpr auto kNestedName = R"(metadata["items"][0]["name"])";
    constexpr auto kBoolName = R"(metadata["b"])";
    constexpr auto kNonNumericDoubleName = R"(metadata["non_numeric_d"])";
    const std::vector<std::string> kIntPath = {"i"};
    const std::vector<std::string> kDoublePath = {"d"};
    const std::vector<std::string> kNestedPath = {"items", "0", "name"};
    const std::vector<std::string> kBoolPath = {"b"};
    const std::vector<std::string> kNonNumericDoublePath = {"non_numeric_d"};
    constexpr auto kScalarName = "scalar";
    const auto input_plan = SerializeInputPlan({
        JSONInputProjection(json_fid, DataType::INT64, kIntName, kIntPath),
        ScalarInputProjection(scalar_fid, DataType::INT64, kScalarName),
        JSONInputProjection(
            json_fid, DataType::DOUBLE, kDoubleName, kDoublePath),
        JSONInputProjection(
            json_fid, DataType::VARCHAR, kNestedName, kNestedPath),
        JSONInputProjection(json_fid, DataType::BOOL, kBoolName, kBoolPath),
        JSONInputProjection(json_fid,
                            DataType::DOUBLE,
                            kNonNumericDoubleName,
                            kNonNumericDoublePath),
        JSONInputProjection(
            json_fid, DataType::DOUBLE, kIntAliasName, kIntPath),
    });

    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    int32_t segment_indices[] = {0, 0, 0, 0, 0};
    int64_t segment_offsets[] = {0, 1, 2, 3, 4};
    ArrowSchema ordered_schema{};
    ArrowArray ordered_array{};
    auto metrics_before_ordered =
        milvus::monitor::getPrometheusClient().GetMetrics();
    auto ordered_projection_count =
        CGOCallMetricCount(metrics_before_ordered,
                           "FillFieldsOrderedAsArrowRecordBatchWithInputPlan");
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        5,
        &ordered_schema,
        &ordered_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto metrics_after_ordered =
        milvus::monitor::getPrometheusClient().GetMetrics();
    EXPECT_EQ(
        CGOCallMetricCount(metrics_after_ordered,
                           "FillFieldsOrderedAsArrowRecordBatchWithInputPlan"),
        ordered_projection_count + 1);
    auto ordered_batch =
        ImportExportedRecordBatch(&ordered_array, &ordered_schema);
    ASSERT_TRUE(ordered_batch.ok()) << ordered_batch.status().ToString();
    ASSERT_EQ((*ordered_batch)->num_columns(), 7);
    EXPECT_EQ((*ordered_batch)->schema()->field(0)->name(), kIntName);
    EXPECT_EQ((*ordered_batch)->schema()->field(1)->name(), kScalarName);
    EXPECT_EQ((*ordered_batch)->schema()->field(2)->name(), kDoubleName);
    EXPECT_EQ((*ordered_batch)->schema()->field(3)->name(), kNestedName);
    EXPECT_EQ((*ordered_batch)->schema()->field(4)->name(), kBoolName);
    EXPECT_EQ((*ordered_batch)->schema()->field(5)->name(),
              kNonNumericDoubleName);
    EXPECT_EQ((*ordered_batch)->schema()->field(6)->name(), kIntAliasName);

    auto ints = std::static_pointer_cast<arrow::Int64Array>(
        (*ordered_batch)->column(0));
    EXPECT_EQ(ints->Value(0), 10);
    EXPECT_TRUE(ints->IsNull(1));
    EXPECT_EQ(ints->Value(2), 30);
    EXPECT_TRUE(ints->IsNull(3));
    EXPECT_TRUE(ints->IsNull(4));
    auto scalar = std::static_pointer_cast<arrow::Int64Array>(
        (*ordered_batch)->column(1));
    for (int64_t row = 0; row < 5; ++row) {
        EXPECT_EQ(scalar->Value(row), scalar_values[row]);
    }
    auto doubles = std::static_pointer_cast<arrow::DoubleArray>(
        (*ordered_batch)->column(2));
    EXPECT_DOUBLE_EQ(doubles->Value(0), 1.5);
    EXPECT_TRUE(doubles->IsValid(1));
    EXPECT_DOUBLE_EQ(doubles->Value(1), 0.0);
    EXPECT_TRUE(std::signbit(doubles->Value(1)));
    EXPECT_TRUE(doubles->IsValid(2));
    EXPECT_DOUBLE_EQ(doubles->Value(2), 0.0);
    EXPECT_FALSE(std::signbit(doubles->Value(2)));
    EXPECT_TRUE(doubles->IsNull(3));
    // No special rescue of zero literals when the parser reports an error.
    EXPECT_TRUE(doubles->IsNull(4));
    auto strings = std::static_pointer_cast<arrow::StringArray>(
        (*ordered_batch)->column(3));
    EXPECT_EQ(strings->GetString(0), "alpha");
    EXPECT_TRUE(strings->IsNull(1));
    EXPECT_TRUE(strings->IsNull(2));
    EXPECT_TRUE(strings->IsNull(3));
    EXPECT_TRUE(strings->IsNull(4));
    auto bools = std::static_pointer_cast<arrow::BooleanArray>(
        (*ordered_batch)->column(4));
    EXPECT_TRUE(bools->Value(0));
    EXPECT_FALSE(bools->Value(1));
    EXPECT_TRUE(bools->IsNull(2));
    EXPECT_TRUE(bools->IsNull(3));
    EXPECT_TRUE(bools->IsNull(4));

    auto non_numeric_doubles = std::static_pointer_cast<arrow::DoubleArray>(
        (*ordered_batch)->column(5));
    EXPECT_EQ(non_numeric_doubles->null_count(), 5);

    auto int_aliases = std::static_pointer_cast<arrow::DoubleArray>(
        (*ordered_batch)->column(6));
    EXPECT_DOUBLE_EQ(int_aliases->Value(0), 10.0);
    EXPECT_TRUE(int_aliases->IsNull(1));
    EXPECT_DOUBLE_EQ(int_aliases->Value(2), 30.0);
    EXPECT_TRUE(int_aliases->IsNull(3));
    EXPECT_TRUE(int_aliases->IsNull(4));

    for (size_t index :
         {size_t{0}, size_t{2}, size_t{3}, size_t{4}, size_t{5}, size_t{6}}) {
        const auto& field = (*ordered_batch)->schema()->field(index);
        ASSERT_NE(field->metadata(), nullptr);
        EXPECT_FALSE(field->metadata()->Get("milvus.field_id").ok());
        EXPECT_TRUE(field->metadata()->Get("milvus.data_type").ok());
    }
    EXPECT_TRUE((*ordered_batch)
                    ->schema()
                    ->field(1)
                    ->metadata()
                    ->Get("milvus.field_id")
                    .ok());

    SearchResult search_result;
    search_result.total_nq_ = 1;
    search_result.unity_topK_ = 5;
    search_result.pk_type_ = DataType::INT64;
    AttachSealedRequestLease(search_result, segment.get());
    search_result.seg_offsets_ = {0, 1, 2, 3, 4};
    search_result.distances_ = {0.9f, 0.8f, 0.7f, 0.6f, 0.5f};
    search_result.primary_keys_ = {PkType(int64_t(100)),
                                   PkType(int64_t(101)),
                                   PkType(int64_t(102)),
                                   PkType(int64_t(103)),
                                   PkType(int64_t(104))};
    search_result.topk_per_nq_prefix_sum_ = {0, 5};
    ArrowSchema search_schema{};
    ArrowArray search_array{};
    int64_t* chunk_sizes = nullptr;
    int64_t num_chunks = 0;
    auto metrics_before_export =
        milvus::monitor::getPrometheusClient().GetMetrics();
    auto export_projection_count =
        CGOCallMetricCount(metrics_before_export,
                           "ExportSearchResultAsArrowRecordBatchWithInputPlan");
    status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&search_result),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &search_schema,
        &search_array,
        &chunk_sizes,
        &num_chunks,
        nullptr);
    [[maybe_unused]] auto chunk_sizes_guard = AdoptChunkSizes(chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto metrics_after_export =
        milvus::monitor::getPrometheusClient().GetMetrics();
    EXPECT_EQ(
        CGOCallMetricCount(metrics_after_export,
                           "ExportSearchResultAsArrowRecordBatchWithInputPlan"),
        export_projection_count + 1);
    auto search_batch =
        ImportExportedRecordBatch(&search_array, &search_schema);
    ASSERT_TRUE(search_batch.ok()) << search_batch.status().ToString();
    ASSERT_EQ((*search_batch)->num_columns(), 3 + 7);
    EXPECT_EQ((*search_batch)->schema()->field(0)->name(), "$id");
    EXPECT_EQ((*search_batch)->schema()->field(1)->name(), "$score");
    EXPECT_EQ((*search_batch)->schema()->field(2)->name(), "$seg_offset");
    EXPECT_EQ((*search_batch)->schema()->field(3)->name(), kIntName);
    EXPECT_EQ((*search_batch)->schema()->field(4)->name(), kScalarName);
    EXPECT_EQ((*search_batch)->schema()->field(9)->name(), kIntAliasName);
    EXPECT_EQ((*search_batch)->schema()->GetFieldIndex("metadata"), -1);
    EXPECT_TRUE((*search_batch)->column(5)->Equals(doubles));
    EXPECT_TRUE(std::signbit(
        std::static_pointer_cast<arrow::DoubleArray>((*search_batch)->column(5))
            ->Value(1)));
}

TEST(SearchResultExport,
     FunctionChainL0ProjectionEmptyResultPreservesTypedSchema) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scalar_fid = schema->AddDebugField("scalar", DataType::INT64);
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kJSONLogicalName = R"(metadata["rank"])";
    const std::vector<std::string> kJSONPath = {"rank"};
    constexpr auto kScalarName = "scalar";
    const auto input_plan = SerializeInputPlan({
        JSONInputProjection(
            json_fid, DataType::INT64, kJSONLogicalName, kJSONPath),
        ScalarInputProjection(scalar_fid, DataType::INT64, kScalarName),
    });

    SearchResult result;
    result.total_nq_ = 1;
    result.unity_topK_ = 0;
    result.pk_type_ = DataType::INT64;
    result.topk_per_nq_prefix_sum_ = {0, 0};

    ArrowSchema out_schema{};
    ArrowArray out_array{};
    int64_t* chunk_sizes = nullptr;
    int64_t num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&result),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &out_schema,
        &out_array,
        &chunk_sizes,
        &num_chunks,
        nullptr);
    [[maybe_unused]] auto chunk_sizes_guard = AdoptChunkSizes(chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    ASSERT_EQ(num_chunks, 1);
    ASSERT_NE(chunk_sizes, nullptr);
    EXPECT_EQ(chunk_sizes[0], 0);

    auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    ASSERT_NE(*batch, nullptr);
    EXPECT_EQ((*batch)->num_rows(), 0);
    ASSERT_EQ((*batch)->num_columns(), 3 + 2);
    EXPECT_EQ((*batch)->schema()->field(3)->name(), kJSONLogicalName);
    EXPECT_TRUE((*batch)->schema()->field(3)->type()->Equals(arrow::int64()));
    EXPECT_EQ((*batch)->schema()->field(4)->name(), kScalarName);
    EXPECT_TRUE((*batch)->schema()->field(4)->type()->Equals(arrow::int64()));
    EXPECT_EQ((*batch)->schema()->GetFieldIndex("metadata"), -1);

    const auto& json_field = (*batch)->schema()->field(3);
    ASSERT_NE(json_field->metadata(), nullptr);
    EXPECT_FALSE(json_field->metadata()->Get("milvus.field_id").ok());
    auto json_data_type = json_field->metadata()->Get("milvus.data_type");
    ASSERT_TRUE(json_data_type.ok()) << json_data_type.status().ToString();
    EXPECT_EQ(*json_data_type,
              std::to_string(static_cast<int32_t>(DataType::INT64)));

    const auto& scalar_field = (*batch)->schema()->field(4);
    ASSERT_NE(scalar_field->metadata(), nullptr);
    auto scalar_field_id = scalar_field->metadata()->Get("milvus.field_id");
    ASSERT_TRUE(scalar_field_id.ok()) << scalar_field_id.status().ToString();
    EXPECT_EQ(*scalar_field_id, std::to_string(scalar_fid.get()));
}

TEST(SearchResultExport,
     FunctionChainProjectionReturnsNullForIntMismatchesAndScalarRoots) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    const std::vector<std::string> json_rows = {
        R"({"value":7})",
        R"({"value":1.5})",
        R"({"value":1e2})",
        R"({"value":9223372036854775808})",
        R"(null)",
        R"(42)",
        R"("text")",
        R"({})",
        R"({"value":null})",
        R"({"value":{}})",
        R"({"value":[]})",
        R"({"value":"bad","value":8})",
        R"({"value":01})",
        R"({"value":1e+})",
        R"({"value":"\q"})",
        R"({"value":truX})",
    };
    auto raw_data = DataGen(schema, json_rows.size(), /*seed=*/1);
    bool found_json = false;
    for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
        if (field_data.field_id() != json_fid.get()) {
            continue;
        }
        found_json = true;
        auto* values =
            field_data.mutable_scalars()->mutable_json_data()->mutable_data();
        values->Clear();
        for (const auto& row : json_rows) {
            values->Add(std::string(row));
        }
    }
    ASSERT_TRUE(found_json);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(
        vec_fid, static_cast<int>(json_rows.size()));
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kLogicalName = R"(metadata["value"])";
    const std::vector<std::string> kNestedPath = {"value"};
    const auto input_plan = SerializeInputPlan({JSONInputProjection(
        json_fid, DataType::INT64, kLogicalName, kNestedPath)});
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    std::vector<int32_t> segment_indices(json_rows.size(), 0);
    std::vector<int64_t> segment_offsets;
    for (size_t row = 0; row < json_rows.size(); ++row) {
        segment_offsets.push_back(static_cast<int64_t>(row));
    }
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices.data(),
        segment_offsets.data(),
        static_cast<int64_t>(json_rows.size()),
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    auto values = std::static_pointer_cast<arrow::Int64Array>(
        (*batch)->GetColumnByName(kLogicalName));
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->length(), static_cast<int64_t>(json_rows.size()));
    EXPECT_EQ(values->null_count(), static_cast<int64_t>(json_rows.size() - 1));
    ASSERT_TRUE(values->IsValid(0));
    EXPECT_EQ(values->Value(0), 7);

    SearchResult search_result;
    search_result.total_nq_ = 1;
    search_result.unity_topK_ = json_rows.size();
    search_result.pk_type_ = DataType::INT64;
    AttachSealedRequestLease(search_result, segment.get());
    search_result.seg_offsets_ = segment_offsets;
    search_result.distances_.resize(json_rows.size(), 0.5f);
    for (size_t row = 0; row < json_rows.size(); ++row) {
        search_result.primary_keys_.emplace_back(
            PkType(static_cast<int64_t>(row)));
    }
    search_result.topk_per_nq_prefix_sum_ = {0, json_rows.size()};
    ArrowSchema search_schema{};
    ArrowArray search_array{};
    int64_t* chunk_sizes = nullptr;
    int64_t num_chunks = 0;
    status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&search_result),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &search_schema,
        &search_array,
        &chunk_sizes,
        &num_chunks,
        nullptr);
    [[maybe_unused]] auto chunk_sizes_guard = AdoptChunkSizes(chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto search_batch =
        ImportExportedRecordBatch(&search_array, &search_schema);
    ASSERT_TRUE(search_batch.ok()) << search_batch.status().ToString();
    auto search_values = std::static_pointer_cast<arrow::Int64Array>(
        (*search_batch)->GetColumnByName(kLogicalName));
    ASSERT_NE(search_values, nullptr);
    EXPECT_EQ(search_values->length(), static_cast<int64_t>(json_rows.size()));
    EXPECT_EQ(search_values->null_count(),
              static_cast<int64_t>(json_rows.size() - 1));
    ASSERT_TRUE(search_values->IsValid(0));
    EXPECT_EQ(search_values->Value(0), 7);
}

TEST(SearchResultExport,
     FunctionChainProjectionReleasesUnprojectedJSONBuffers) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto scalar_fid = schema->AddDebugField("scalar", DataType::INT64);
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    auto raw_data = DataGen(schema, 1, /*seed=*/1);
    const auto json = std::string(R"({"value":7,"padding":")") +
                      std::string(1024 * 1024, 'x') + R"("})";
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() == json_fid.get()) {
            *field.mutable_scalars()->mutable_json_data()->mutable_data(0) =
                json;
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    result.total_nq_ = 1;
    result.unity_topK_ = 1;
    result.pk_type_ = DataType::INT64;
    result.seg_offsets_ = {0};
    result.distances_ = {1.0f};
    result.primary_keys_ = {PkType(int64_t(1))};
    result.topk_per_nq_prefix_sum_ = {0, 1};
    CSearchResult c_result = reinterpret_cast<CSearchResult>(&result);
    constexpr auto kName = R"(metadata["value"])";
    const auto input_plan = SerializeInputPlan(
        {JSONInputProjection(json_fid, DataType::INT64, kName, {"value"}),
         ScalarInputProjection(scalar_fid, DataType::INT64, "scalar")});

    auto* pool = arrow::default_memory_pool();
    for (bool l0 : {true, false}) {
        SCOPED_TRACE(l0 ? "L0" : "L1");
        const auto baseline = pool->bytes_allocated();
        {
            ArrowSchema out_schema{};
            ArrowArray out_array{};
            int64_t* chunk_sizes = nullptr;
            int64_t num_chunks = 0;
            auto cleanup = folly::makeGuard([&] {
                if (out_array.release)
                    out_array.release(&out_array);
                if (out_schema.release)
                    out_schema.release(&out_schema);
                free(chunk_sizes);
            });
            int32_t seg_index = 0;
            int64_t seg_offset = 0;
            auto status =
                l0 ? ExportSearchResultAsArrowRecordBatchWithInputPlan(
                         c_result,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         input_plan.data(),
                         input_plan.size(),
                         &out_schema,
                         &out_array,
                         &chunk_sizes,
                         &num_chunks,
                         nullptr)
                   : FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
                         &c_result,
                         1,
                         reinterpret_cast<CSearchPlan>(plan.get()),
                         input_plan.data(),
                         input_plan.size(),
                         &seg_index,
                         &seg_offset,
                         1,
                         &out_schema,
                         &out_array,
                         nullptr);
            ASSERT_EQ(status.error_code, 0) << status.error_msg;
            // Keep the exported scalar/system columns alive: their ownership
            // must not pin the discarded 1 MiB JSON column.
            EXPECT_LT(pool->bytes_allocated() - baseline, 64 * 1024);
            EXPECT_EQ(out_array.length, 1);
            EXPECT_EQ(out_array.n_children, l0 ? 5 : 2);
        }
        EXPECT_EQ(pool->bytes_allocated(), baseline);
    }
}

TEST(SearchResultExport, JSONErrorClassification) {
    using milvus::SimdjsonParseErrorToErrorCode;
    EXPECT_EQ(SimdjsonParseErrorToErrorCode(simdjson::MEMALLOC),
              milvus::MemAllocateFailed);
    EXPECT_EQ(SimdjsonParseErrorToErrorCode(simdjson::IO_ERROR),
              milvus::FileReadFailed);
    EXPECT_EQ(SimdjsonParseErrorToErrorCode(simdjson::DEPTH_ERROR),
              milvus::DataFormatBroken);
    EXPECT_EQ(SimdjsonParseErrorToErrorCode(simdjson::STRING_ERROR),
              milvus::DataFormatBroken);
    for (auto error : {simdjson::CAPACITY,
                       simdjson::UNINITIALIZED,
                       simdjson::INSUFFICIENT_PADDING,
                       simdjson::UNEXPECTED_ERROR,
                       simdjson::PARSER_IN_USE,
                       simdjson::OUT_OF_ORDER_ITERATION}) {
        EXPECT_EQ(SimdjsonParseErrorToErrorCode(error), milvus::UnexpectedError)
            << simdjson::error_message(error);
    }
}

TEST(SearchResultExport, JSONDepthErrorPreservesDataFormatBroken) {
    // The DOM parser enforces its depth limit; the on-demand projection parser
    // does not enforce that limit in release builds.
    const std::string raw =
        std::string(2048, '[') + "0" + std::string(2048, ']');
    const milvus::Json json{simdjson::padded_string(raw)};
    try {
        json.dom_doc();
        FAIL() << "expected the parser depth limit to reject the document";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::DataFormatBroken);
    }
}

TEST(SearchResultExport, FunctionChainProjectionReportsJSONReadErrors) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kLogicalName = R"(metadata["value"])";
    const std::vector<std::string> kNestedPath = {"value"};
    const auto input_plan = SerializeInputPlan({JSONInputProjection(
        json_fid, DataType::VARCHAR, kLogicalName, kNestedPath)});
    const std::vector<std::string> malformed_json = {
        R"({"value":1)",
        R"({"value":"\q"})",
    };
    for (const auto& json : malformed_json) {
        SCOPED_TRACE(json);
        auto raw_data = DataGen(schema, 1, /*seed=*/1);
        for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
            if (field_data.field_id() == json_fid.get()) {
                auto* values = field_data.mutable_scalars()
                                   ->mutable_json_data()
                                   ->mutable_data();
                *values->Mutable(0) = json;
            }
        }
        auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
        SearchResult result;
        AttachSealedRequestLease(result, segment.get());
        std::vector<CSearchResult> c_results = {
            reinterpret_cast<CSearchResult>(&result)};
        int32_t segment_indices[] = {0};
        int64_t segment_offsets[] = {0};
        ArrowSchema out_schema{};
        ArrowArray out_array{};
        auto* pool = arrow::default_memory_pool();
        const auto baseline = pool->bytes_allocated();
        auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
            c_results.data(),
            c_results.size(),
            reinterpret_cast<CSearchPlan>(plan.get()),
            input_plan.data(),
            input_plan.size(),
            segment_indices,
            segment_offsets,
            1,
            &out_schema,
            &out_array,
            nullptr);
        EXPECT_EQ(status.error_code, milvus::DataFormatBroken)
            << status.error_msg;
        EXPECT_EQ(out_schema.release, nullptr);
        EXPECT_EQ(out_array.release, nullptr);
        ASSERT_NE(status.error_msg, nullptr);
        free(const_cast<char*>(status.error_msg));
        EXPECT_EQ(pool->bytes_allocated(), baseline);

        result.total_nq_ = 1;
        result.unity_topK_ = 1;
        result.pk_type_ = DataType::INT64;
        result.seg_offsets_ = {0};
        result.distances_ = {1.0f};
        result.primary_keys_ = {PkType(int64_t(1))};
        result.topk_per_nq_prefix_sum_ = {0, 1};
        int64_t* chunk_sizes = nullptr;
        int64_t num_chunks = 0;
        status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
            c_results[0],
            reinterpret_cast<CSearchPlan>(plan.get()),
            input_plan.data(),
            input_plan.size(),
            &out_schema,
            &out_array,
            &chunk_sizes,
            &num_chunks,
            nullptr);
        EXPECT_EQ(status.error_code, milvus::DataFormatBroken)
            << status.error_msg;
        EXPECT_EQ(out_schema.release, nullptr);
        EXPECT_EQ(out_array.release, nullptr);
        // Chunk sizes have already been allocated when JSON parsing throws.
        EXPECT_EQ(chunk_sizes, nullptr);
        EXPECT_EQ(num_chunks, 0);
        free(chunk_sizes);
        free(const_cast<char*>(status.error_msg));
        EXPECT_EQ(pool->bytes_allocated(), baseline);
    }
}

TEST(SearchResultExport, FunctionChainProjectionReadsOnlyRequestedPaths) {
    using namespace milvus;
    using namespace milvus::segcore;
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    const std::vector<std::string> rows = {
        R"({"value":7,"unread":"\q"})",
        R"({"value":7,"unread":truX})",
        R"({"value":7,"unread":01})",
        R"({"unread":"\q","value":7})",
        R"({"value":7} {"unread":2})",
    };
    auto raw_data = DataGen(schema, rows.size(), 1);
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() == json_fid.get()) {
            auto* data =
                field.mutable_scalars()->mutable_json_data()->mutable_data();
            data->Clear();
            for (const auto& row : rows) {
                data->Add(std::string(row));
            }
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, rows.size());
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    auto c_result = reinterpret_cast<CSearchResult>(&result);
    const auto input_plan = SerializeInputPlan(
        {JSONInputProjection(json_fid, DataType::INT64, "value", {"value"})});
    std::vector<int32_t> seg_indices(rows.size(), 0);
    std::vector<int64_t> offsets;
    for (size_t row = 0; row < rows.size(); ++row) {
        offsets.push_back(row);
    }
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        &c_result,
        1,
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        seg_indices.data(),
        offsets.data(),
        rows.size(),
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    auto values =
        std::dynamic_pointer_cast<arrow::Int64Array>((*batch)->column(0));
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(values->null_count(), 0);
    for (int64_t row = 0; row < values->length(); ++row) {
        EXPECT_EQ(values->Value(row), 7);
    }
}

TEST(SearchResultExport, FunctionChainProjectionUsesPublicJsonKeyMatching) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("metadata", DataType::JSON);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 1, /*seed=*/1);
    for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
        if (field_data.field_id() == json_fid.get()) {
            auto* values = field_data.mutable_scalars()
                               ->mutable_json_data()
                               ->mutable_data();
            *values->Mutable(0) = R"({"\u0061":7})";
        }
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kLogicalName = R"(metadata["a"])";
    const std::vector<std::string> kNestedPath = {"a"};
    const auto input_plan = SerializeInputPlan({JSONInputProjection(
        json_fid, DataType::INT64, kLogicalName, kNestedPath)});
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    int32_t segment_indices[] = {0};
    int64_t segment_offsets[] = {0};
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        1,
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    auto values = std::static_pointer_cast<arrow::Int64Array>(
        (*batch)->GetColumnByName(kLogicalName));
    ASSERT_NE(values, nullptr);
    // Match the existing public reader: the raw key "\\u0061" is not "a".
    EXPECT_TRUE(values->IsNull(0));
}

TEST(SearchResultExport,
     FunctionChainL0ProjectionReturnsNullForFieldAbsentFromOldSegment) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto old_schema = std::make_shared<Schema>();
    old_schema->set_schema_version(1);
    auto old_pk_fid = old_schema->AddDebugField("pk", DataType::INT64);
    old_schema->set_primary_field_id(old_pk_fid);
    auto old_vec_fid = old_schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto evolved_schema = std::make_shared<Schema>();
    evolved_schema->set_schema_version(2);
    auto pk_fid = evolved_schema->AddDebugField("pk", DataType::INT64);
    evolved_schema->set_primary_field_id(pk_fid);
    auto vec_fid = evolved_schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto json_fid =
        evolved_schema->AddDebugField("added_metadata", DataType::JSON, true);
    ASSERT_EQ(pk_fid, old_pk_fid);
    ASSERT_EQ(vec_fid, old_vec_fid);

    auto raw_data = DataGen(old_schema, 2, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(old_schema, raw_data);
    ASSERT_FALSE(segment->is_field_exist(json_fid));
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        evolved_schema, plan_bytes.data(), plan_bytes.size());

    constexpr auto kLogicalName = R"(added_metadata["value"])";
    const std::vector<std::string> kNestedPath = {"value"};
    const auto input_plan = SerializeInputPlan({JSONInputProjection(
        json_fid, DataType::INT64, kLogicalName, kNestedPath)});
    SearchResult result;
    result.total_nq_ = 1;
    result.unity_topK_ = 2;
    result.pk_type_ = DataType::INT64;
    AttachSealedRequestLease(result, segment.get());
    result.seg_offsets_ = {0, 1};
    result.distances_ = {0.9f, 0.8f};
    result.primary_keys_ = {PkType(int64_t(100)), PkType(int64_t(101))};
    result.topk_per_nq_prefix_sum_ = {0, 2};

    ArrowSchema out_schema{};
    ArrowArray out_array{};
    int64_t* chunk_sizes = nullptr;
    int64_t num_chunks = 0;
    auto status = ExportSearchResultAsArrowRecordBatchWithInputPlan(
        reinterpret_cast<CSearchResult>(&result),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        &out_schema,
        &out_array,
        &chunk_sizes,
        &num_chunks,
        nullptr);
    [[maybe_unused]] auto chunk_sizes_guard = AdoptChunkSizes(chunk_sizes);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    auto values = std::static_pointer_cast<arrow::Int64Array>(
        (*batch)->GetColumnByName(kLogicalName));
    ASSERT_NE(values, nullptr);
    EXPECT_EQ(values->length(), 2);
    EXPECT_EQ(values->null_count(), 2);
}

TEST(SearchResultExport,
     FillFieldsOrderedAsArrowRecordBatchWithInputPlan_InterleavesSegments) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto field_fid = schema->AddDebugField("ordered_i64", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto raw_data_a = DataGen(schema, 4, /*seed=*/1);
    auto raw_data_b = DataGen(schema, 4, /*seed=*/2);
    auto values_a = raw_data_a.get_col<int64_t>(field_fid);
    auto values_b = raw_data_b.get_col<int64_t>(field_fid);
    auto segment_a = CreateSealedWithFieldDataLoaded(schema, raw_data_a);
    auto segment_b = CreateSealedWithFieldDataLoaded(schema, raw_data_b);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/3);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());

    SearchResult result_a;
    AttachSealedRequestLease(result_a, segment_a.get());
    SearchResult result_b;
    AttachSealedRequestLease(result_b, segment_b.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result_a),
        reinterpret_cast<CSearchResult>(&result_b)};

    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(field_fid, DataType::INT64, "ordered_i64")});
    int32_t seg_indices[] = {1, 0, 1};
    int64_t seg_offsets[] = {2, 1, 0};
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        seg_indices,
        seg_offsets,
        3,
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    auto batch_result = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    ASSERT_EQ((*batch_result)->num_rows(), 3);
    ASSERT_EQ((*batch_result)->num_columns(), 1);
    EXPECT_EQ((*batch_result)->schema()->field(0)->name(), "ordered_i64");

    auto values =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(0));
    EXPECT_EQ(values->Value(0), values_b[2]);
    EXPECT_EQ(values->Value(1), values_a[1]);
    EXPECT_EQ(values->Value(2), values_b[0]);
}

TEST(
    SearchResultExport,
    FillFieldsOrderedAsArrowRecordBatchWithInputPlan_NullableValuesFollowInterleavedRows) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto nullable_fid =
        schema->AddDebugField("nullable_i64", DataType::INT64, true);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data_a = DataGen(schema, 4, /*seed=*/1);
    auto raw_data_b = DataGen(schema, 4, /*seed=*/2);
    auto values_a = raw_data_a.get_col<int64_t>(nullable_fid);
    auto values_b = raw_data_b.get_col<int64_t>(nullable_fid);
    auto valid_a = raw_data_a.get_col_valid(nullable_fid);
    auto valid_b = raw_data_b.get_col_valid(nullable_fid);
    auto segment_a = CreateSealedWithFieldDataLoaded(schema, raw_data_a);
    auto segment_b = CreateSealedWithFieldDataLoaded(schema, raw_data_b);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/3);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result_a;
    AttachSealedRequestLease(result_a, segment_a.get());
    SearchResult result_b;
    AttachSealedRequestLease(result_b, segment_b.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result_a),
        reinterpret_cast<CSearchResult>(&result_b)};
    ASSERT_TRUE(valid_a[0]);
    ASSERT_TRUE(valid_a[2]);
    ASSERT_FALSE(valid_a[1]);
    ASSERT_TRUE(valid_b[0]);
    ASSERT_FALSE(valid_b[1]);
    ASSERT_TRUE(valid_b[2]);
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(nullable_fid, DataType::INT64, "nullable_i64")});
    int32_t segment_indices[] = {1, 0, 1, 0, 1, 0};
    int64_t segment_offsets[] = {1, 0, 2, 2, 0, 1};
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        4,
        &out_schema,
        &out_array,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto batch_result = ImportExportedRecordBatch(&out_array, &out_schema);
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    ASSERT_NE(*batch_result, nullptr);
    auto values =
        std::static_pointer_cast<arrow::Int64Array>((*batch_result)->column(0));
    ASSERT_EQ(values->length(), 4);
    const std::vector<int64_t> expected_values = {
        values_b[1], values_a[0], values_b[2], values_a[2]};
    const std::vector<bool> expected_valid = {
        valid_b[1], valid_a[0], valid_b[2], valid_a[2]};
    for (int i = 0; i < 4; ++i) {
        EXPECT_EQ(values->IsValid(i), expected_valid[i]);
        if (expected_valid[i]) {
            EXPECT_EQ(values->Value(i), expected_values[i]);
        }
    }
    EXPECT_EQ(values->null_count(),
              static_cast<int64_t>(4 - std::count(expected_valid.begin(),
                                                  expected_valid.end(),
                                                  true)));
}

TEST(
    SearchResultExport,
    FillFieldsOrderedAsArrowRecordBatchWithInputPlan_InvalidSegmentIndexReturnsError) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto field_fid = schema->AddDebugField("ordered_i64", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 1, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(field_fid, DataType::INT64, "ordered_i64")});
    int32_t segment_indices[] = {1};
    int64_t segment_offsets[] = {0};
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        1,
        &out_schema,
        &out_array,
        nullptr);
    EXPECT_NE(status.error_code, 0);
    EXPECT_EQ(out_schema.release, nullptr);
    EXPECT_EQ(out_array.release, nullptr);
    ASSERT_NE(status.error_msg, nullptr);
    free(const_cast<char*>(status.error_msg));
}

TEST(
    SearchResultExport,
    FillFieldsOrderedAsArrowRecordBatchWithInputPlan_CancellationReturnsFollyCancel) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto field_fid = schema->AddDebugField("ordered_i64", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 1, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    const auto input_plan = SerializeInputPlan(
        {ScalarInputProjection(field_fid, DataType::INT64, "ordered_i64")});
    int32_t segment_indices[] = {0};
    int64_t segment_offsets[] = {0};
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();
    ArrowSchema out_schema{};
    ArrowArray out_array{};
    auto status = FillFieldsOrderedAsArrowRecordBatchWithInputPlan(
        c_results.data(),
        c_results.size(),
        reinterpret_cast<CSearchPlan>(plan.get()),
        input_plan.data(),
        input_plan.size(),
        segment_indices,
        segment_offsets,
        1,
        &out_schema,
        &out_array,
        &cancellation_source);
    EXPECT_EQ(status.error_code, milvus::FollyCancel);
    EXPECT_NE(status.error_code, 0);
    EXPECT_EQ(out_schema.release, nullptr);
    EXPECT_EQ(out_array.release, nullptr);
    ASSERT_NE(status.error_msg, nullptr);
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport, FillOutputFieldsOrdered_Basic) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto output_fid = schema->AddDebugField("output_i64", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto raw_data = DataGen(schema, 4, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    plan->target_entries_.push_back(pk_fid);
    plan->target_entries_.push_back(output_fid);
    EXPECT_TRUE(HasTargetEntries(reinterpret_cast<CSearchPlan>(plan.get())));

    SearchResult sr;
    AttachSealedRequestLease(sr, segment.get());

    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&sr)};
    int32_t seg_indices[] = {0, 0};
    int64_t seg_offsets[] = {0, 1};

    CProto c_proto{};
    auto status =
        FillOutputFieldsOrdered(c_results.data(),
                                c_results.size(),
                                reinterpret_cast<CSearchPlan>(plan.get()),
                                seg_indices,
                                seg_offsets,
                                /*total_rows=*/2,
                                &c_proto,
                                nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    ASSERT_GT(c_proto.proto_size, 0);
    milvus::proto::schema::SearchResultData result_data;
    ASSERT_TRUE(
        result_data.ParseFromArray(c_proto.proto_blob, c_proto.proto_size));
    ASSERT_EQ(result_data.fields_data_size(), 2);
    EXPECT_EQ(result_data.fields_data(0).field_id(), pk_fid.get());
    EXPECT_EQ(result_data.fields_data(0).scalars().long_data().data_size(), 2);
    EXPECT_EQ(result_data.fields_data(1).field_id(), output_fid.get());
    EXPECT_EQ(result_data.fields_data(1).scalars().long_data().data_size(), 2);

    free(const_cast<void*>(c_proto.proto_blob));
}

TEST(SearchResultExport,
     FillOutputFieldsOrdered_CancellationReturnsFollyCancel) {
    using namespace milvus;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto output_fid = schema->AddDebugField("output_i64", DataType::INT64);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto raw_data = DataGen(schema, 1, /*seed=*/1);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/1);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    plan->target_entries_.push_back(output_fid);
    SearchResult result;
    AttachSealedRequestLease(result, segment.get());
    std::vector<CSearchResult> c_results = {
        reinterpret_cast<CSearchResult>(&result)};
    int32_t segment_indices[] = {0};
    int64_t segment_offsets[] = {0};
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();
    CProto c_proto{};
    auto status =
        FillOutputFieldsOrdered(c_results.data(),
                                c_results.size(),
                                reinterpret_cast<CSearchPlan>(plan.get()),
                                segment_indices,
                                segment_offsets,
                                /*total_rows=*/1,
                                &c_proto,
                                &cancellation_source);
    EXPECT_EQ(status.error_code, milvus::FollyCancel) << status.error_msg;
    EXPECT_EQ(c_proto.proto_blob, nullptr);
    EXPECT_EQ(c_proto.proto_size, 0);
    ASSERT_NE(status.error_msg, nullptr);
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport, HasTargetEntries) {
    using namespace milvus;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    EXPECT_FALSE(HasTargetEntries(reinterpret_cast<CSearchPlan>(plan.get())));

    plan->target_entries_.push_back(pk_fid);
    EXPECT_TRUE(HasTargetEntries(reinterpret_cast<CSearchPlan>(plan.get())));
}

TEST(SearchResultExport,
     FillOutputFieldsOrdered_EmptyArrayOutputsPreserveElementType) {
    using namespace milvus;

    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scalar_fid = schema->AddDebugField("scalar_i64", DataType::INT64);
    auto array_fid =
        schema->AddDebugArrayField("array_i64", DataType::INT64, false);
    auto vector_array_fid = schema->AddDebugVectorArrayField(
        "array_float_vector", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);

    auto plan_bytes = BuildSimpleVectorSearchPlan(vec_fid, /*topk=*/2);
    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_bytes.data(), plan_bytes.size());
    plan->target_entries_.push_back(pk_fid);
    plan->target_entries_.push_back(scalar_fid);
    plan->target_entries_.push_back(vec_fid);
    plan->target_entries_.push_back(array_fid);
    plan->target_entries_.push_back(vector_array_fid);

    CProto c_proto{};
    auto status =
        FillOutputFieldsOrdered(nullptr,
                                0,
                                reinterpret_cast<CSearchPlan>(plan.get()),
                                nullptr,
                                nullptr,
                                /*total_rows=*/0,
                                &c_proto,
                                nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    ASSERT_GT(c_proto.proto_size, 0);
    milvus::proto::schema::SearchResultData result_data;
    ASSERT_TRUE(
        result_data.ParseFromArray(c_proto.proto_blob, c_proto.proto_size));
    ASSERT_EQ(result_data.fields_data_size(), 5);

    const auto& pk_field = result_data.fields_data(0);
    EXPECT_EQ(pk_field.field_id(), pk_fid.get());
    EXPECT_EQ(pk_field.type(), milvus::proto::schema::DataType::Int64);
    EXPECT_TRUE(pk_field.has_scalars());
    EXPECT_TRUE(pk_field.scalars().has_long_data());

    const auto& scalar_field = result_data.fields_data(1);
    EXPECT_EQ(scalar_field.field_id(), scalar_fid.get());
    EXPECT_EQ(scalar_field.type(), milvus::proto::schema::DataType::Int64);
    EXPECT_TRUE(scalar_field.has_scalars());
    EXPECT_TRUE(scalar_field.scalars().has_long_data());

    const auto& vector_field = result_data.fields_data(2);
    EXPECT_EQ(vector_field.field_id(), vec_fid.get());
    EXPECT_EQ(vector_field.type(),
              milvus::proto::schema::DataType::FloatVector);
    EXPECT_TRUE(vector_field.has_vectors());
    EXPECT_TRUE(vector_field.vectors().has_float_vector());

    const auto& array_field = result_data.fields_data(3);
    EXPECT_EQ(array_field.field_id(), array_fid.get());
    EXPECT_EQ(array_field.type(), milvus::proto::schema::DataType::Array);
    EXPECT_TRUE(array_field.has_scalars());
    EXPECT_TRUE(array_field.scalars().has_array_data());
    EXPECT_EQ(array_field.scalars().array_data().element_type(),
              milvus::proto::schema::DataType::Int64);

    const auto& vector_array_field = result_data.fields_data(4);
    EXPECT_EQ(vector_array_field.field_id(), vector_array_fid.get());
    EXPECT_EQ(vector_array_field.type(),
              milvus::proto::schema::DataType::ArrayOfVector);
    EXPECT_TRUE(vector_array_field.has_vectors());
    EXPECT_TRUE(vector_array_field.vectors().has_vector_array());
    EXPECT_EQ(vector_array_field.vectors().vector_array().element_type(),
              milvus::proto::schema::DataType::FloatVector);

    free(const_cast<void*>(c_proto.proto_blob));
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
    int64_t all_search_count = 0;
    auto status = PrepareSearchResultsForExport(trace,
                                                nullptr,
                                                nullptr,
                                                nullptr,
                                                /*num_segments=*/0,
                                                slice_nqs,
                                                /*num_slices=*/1,
                                                slice_topks,
                                                &all_search_count,
                                                nullptr);
    EXPECT_NE(status.error_code, 0);
    free(const_cast<char*>(status.error_msg));
}

TEST(SearchResultExport,
     PrepareSearchResultsForExport_CancellationReturnsFollyCancel) {
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {1};
    CTraceContext trace{0, 0, 0};
    int64_t all_search_count = 0;
    folly::CancellationSource cancellation_source;
    cancellation_source.requestCancellation();
    CSearchResult c_result = nullptr;
    auto status = PrepareSearchResultsForExport(trace,
                                                nullptr,
                                                nullptr,
                                                &c_result,
                                                /*num_segments=*/1,
                                                slice_nqs,
                                                /*num_slices=*/1,
                                                slice_topks,
                                                &all_search_count,
                                                &cancellation_source);
    EXPECT_EQ(status.error_code, milvus::FollyCancel);
    EXPECT_EQ(all_search_count, 0);
    ASSERT_NE(status.error_msg, nullptr);
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
    AttachSealedRequestLease(sr_a, seg_a.get());
    sr_a.seg_offsets_ = {0, INVALID_SEG_OFFSET, 5};
    sr_a.distances_ = {1.0f, 2.0f, 3.0f};

    SearchResult sr_b;
    sr_b.total_nq_ = 1;
    sr_b.unity_topK_ = 3;
    sr_b.total_data_cnt_ = N;
    AttachSealedRequestLease(sr_b, seg_b.get());
    sr_b.seg_offsets_ = {1, 7, 10};
    sr_b.distances_ = {1.5f, 2.5f, 3.5f};

    SearchResult sr_no_hit;
    sr_no_hit.total_nq_ = 1;
    sr_no_hit.unity_topK_ = 3;
    sr_no_hit.total_data_cnt_ = N;
    AttachSealedRequestLease(sr_no_hit, seg_b.get());
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

    // 6. Run prep.
    int64_t all_search_count = 0;
    auto status = PrepareSearchResultsForExport(
        trace,
        reinterpret_cast<CSearchPlan>(plan.get()),
        reinterpret_cast<CPlaceholderGroup>(ph_group.get()),
        c_results.data(),
        c_results.size(),
        slice_nqs,
        /*num_slices=*/1,
        slice_topks,
        &all_search_count,
        nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    EXPECT_EQ(all_search_count, sr_a.total_data_cnt_ + sr_b.total_data_cnt_);

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

// SearchResult distances use a larger-is-better representation: AsyncSearch
// negates distances for metrics that are not positively related before the
// global-refine reducer sees them.
TEST(SearchResultExport, GlobalRefineTruncate_UsesNormalizedMetricScores) {
    auto schema = std::make_shared<Schema>();
    Plan plan(schema);
    plan.plan_node_ = std::make_unique<VectorPlanNode>();
    plan.plan_node_->search_info_.refine_topk_ratio_ = 0.5;

    SearchResult l2_seg0;
    l2_seg0.total_nq_ = 1;
    l2_seg0.unity_topK_ = 1;
    l2_seg0.distances_ = {-1.0f};
    l2_seg0.seg_offsets_ = {10};
    l2_seg0.topk_per_nq_prefix_sum_ = {0, 1};

    SearchResult l2_seg1;
    l2_seg1.total_nq_ = 1;
    l2_seg1.unity_topK_ = 1;
    l2_seg1.distances_ = {-2.0f};
    l2_seg1.seg_offsets_ = {20};
    l2_seg1.topk_per_nq_prefix_sum_ = {0, 1};

    std::vector<SearchResult*> l2_results{&l2_seg0, &l2_seg1};
    int64_t slice_nqs[] = {1};
    int64_t slice_topks[] = {2};
    plan.plan_node_->search_info_.metric_type_ = knowhere::metric::L2;
    TestReduceHelper l2_helper(
        l2_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);
    l2_helper.TruncateForTest();

    ASSERT_EQ(l2_seg0.seg_offsets_, std::vector<int64_t>({10}));
    EXPECT_TRUE(l2_seg1.seg_offsets_.empty());

    SearchResult ip_seg0;
    ip_seg0.total_nq_ = 1;
    ip_seg0.unity_topK_ = 1;
    ip_seg0.distances_ = {4.0f};
    ip_seg0.seg_offsets_ = {10};
    ip_seg0.topk_per_nq_prefix_sum_ = {0, 1};

    SearchResult ip_seg1;
    ip_seg1.total_nq_ = 1;
    ip_seg1.unity_topK_ = 1;
    ip_seg1.distances_ = {3.0f};
    ip_seg1.seg_offsets_ = {20};
    ip_seg1.topk_per_nq_prefix_sum_ = {0, 1};

    std::vector<SearchResult*> ip_results{&ip_seg0, &ip_seg1};
    plan.plan_node_->search_info_.metric_type_ = knowhere::metric::IP;
    TestReduceHelper ip_helper(
        ip_results, &plan, nullptr, slice_nqs, slice_topks, 1, nullptr);
    ip_helper.TruncateForTest();

    ASSERT_EQ(ip_seg0.seg_offsets_, std::vector<int64_t>({10}));
    EXPECT_TRUE(ip_seg1.seg_offsets_.empty());
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
        auto real_topk =
            sr->topk_per_nq_prefix_sum_[i + 1] - sr->topk_per_nq_prefix_sum_[i];
        EXPECT_GT(real_topk, 0);
        EXPECT_LE(real_topk, static_cast<size_t>(std::ceil(1.5 * topK)));
    }
    for (auto score : sr->distances_) {
        EXPECT_FALSE(std::isnan(score));
        EXPECT_FALSE(std::isinf(score));
    }
}
