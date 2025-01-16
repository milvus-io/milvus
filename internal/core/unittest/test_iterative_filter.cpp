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
#include "common/Schema.h"
#include "query/Plan.h"

#include "segcore/reduce_c.h"
#include "segcore/plan_c.h"
#include "segcore/segment_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/c_api_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;

/**
 * this UT is to cover Iterative filtering execution logic (knowhere iterator next() -> scalar filtering)
 * so we will not cover all expr type here, just some examples
 */

void
prepareSegmentFieldData(const std::unique_ptr<SegmentSealed>& segment,
                        size_t row_count,
                        GeneratedData& data_set) {
    auto field_data =
        std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64, false);
    field_data->FillFieldData(data_set.row_ids_.data(), row_count);
    auto arrow_data_wrapper =
        storage::ConvertFieldDataToArrowDataWrapper(field_data);
    auto field_data_info = FieldDataInfo{
        RowFieldID.get(),
        row_count,
        std::vector<std::shared_ptr<ArrowDataWrapper>>{arrow_data_wrapper}};
    segment->LoadFieldData(RowFieldID, field_data_info);

    field_data =
        std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64, false);
    field_data->FillFieldData(data_set.timestamps_.data(), row_count);
    auto ts_arrow_data_wrapper =
        storage::ConvertFieldDataToArrowDataWrapper(field_data);
    field_data_info = FieldDataInfo{
        TimestampFieldID.get(),
        row_count,
        std::vector<std::shared_ptr<ArrowDataWrapper>>{ts_arrow_data_wrapper}};
    segment->LoadFieldData(TimestampFieldID, field_data_info);
}

void
CheckFilterSearchResult(const SearchResult& search_result_by_iterative_filter,
                        const SearchResult& search_result_by_pre_filter,
                        int topK,
                        int nq) {
    ASSERT_EQ(search_result_by_pre_filter.seg_offsets_.size(), topK * nq);
    ASSERT_EQ(search_result_by_pre_filter.distances_.size(), topK * nq);
    ASSERT_EQ(search_result_by_iterative_filter.seg_offsets_.size(), topK * nq);
    ASSERT_EQ(search_result_by_iterative_filter.distances_.size(), topK * nq);

    for (int i = 0; i < topK * nq; ++i) {
        std::cout << search_result_by_pre_filter.seg_offsets_[i] << " "
                  << search_result_by_pre_filter.distances_[i] << " "
                  << search_result_by_iterative_filter.seg_offsets_[i] << " "
                  << search_result_by_iterative_filter.distances_[i]
                  << std::endl;
        ASSERT_EQ(search_result_by_pre_filter.seg_offsets_[i],
                  search_result_by_iterative_filter.seg_offsets_[i]);
    }
}

TEST(IterativeFilter, SealedIndex) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    schema->set_primary_field_id(str_fid);
    auto segment = CreateSealedSegment(schema);
    size_t N = 50;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, false, false);
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        auto arrow_data_wrapper = storage::ConvertFieldDataToArrowDataWrapper(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.arrow_reader_channel->push(arrow_data_wrapper);
        info.arrow_reader_channel->close();

        segment->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentFieldData(segment, N, raw_data);

    //3. load index
    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index = std::move(indexing);
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);
    int topK = 10;
    int group_size = 3;

    // int8 binaryRange
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int8
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 100
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int8
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 100
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }

    // int16 Termexpr
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                       term_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int16
                                        >
                                        values:<int64_val:1> values:<int64_val:2 >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                       term_expr: <
                                        column_info: <
                                          field_id: 102
                                          data_type: Int16
                                        >
                                        values:<int64_val:1> values:<int64_val:2 >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }

    // no expr
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }
}

TEST(IterativeFilter, SealedData) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    schema->set_primary_field_id(str_fid);
    auto segment = CreateSealedSegment(schema);
    size_t N = 100;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, false, false);
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        auto arrow_data_wrapper = storage::ConvertFieldDataToArrowDataWrapper(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.arrow_reader_channel->push(arrow_data_wrapper);
        info.arrow_reader_channel->close();

        segment->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentFieldData(segment, N, raw_data);

    int topK = 10;
    // int8 binaryRange
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int8
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 100
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 100
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 101
                                          data_type: Int8
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 100
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }
}

TEST(IterativeFilter, GrowingRawData) {
    int dim = 128;
    uint64_t seed = 512;
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto int64_field_id = schema->AddDebugField("int64", DataType::INT64);
    auto int32_field_id = schema->AddDebugField("int32", DataType::INT32);
    auto vec_field_id = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field_id);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(8);
    config.set_enable_interim_segment_index(
        false);  //no growing index, test brute force
    auto segment_growing = CreateGrowingSegment(schema, nullptr, 1, config);
    auto segment_growing_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t rows_per_batch = 30;
    int n_batch = 1;
    for (int i = 0; i < n_batch; i++) {
        auto data_set =
            DataGen(schema, rows_per_batch, 42, 0, 8, 10, false, false);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    auto topK = 10;
    // int8 binaryRange
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 100
                                          data_type: Int64
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 1
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment_growing_impl->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 102
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 100
                                          data_type: Int64
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 1
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment_growing_impl->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }
}

TEST(IterativeFilter, GrowingIndex) {
    int dim = 128;
    uint64_t seed = 512;
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto int64_field_id = schema->AddDebugField("int64", DataType::INT64);
    auto int32_field_id = schema->AddDebugField("int32", DataType::INT32);
    auto vec_field_id = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field_id);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metric_type},
        {"nlist", "4"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec_field_id, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> fieldMap = {
        {vec_field_id, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(10000, std::move(fieldMap));

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(16);
    config.set_enable_interim_segment_index(true);  // test growing inter index
    config.set_nlist(4);
    config.set_nlist(4);
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment_growing_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    //1. prepare raw data in growing segment
    int64_t rows_per_batch = 100;
    int n_batch = 1;
    for (int i = 0; i < n_batch; i++) {
        auto data_set =
            DataGen(schema, rows_per_batch, 42, 0, 8, 10, false, false);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    auto topK = 10;
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 100
                                          data_type: Int64
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 1
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          hints: "iterative_filter"
                                          search_params: "{\"nprobe\": 4}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(*schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment_growing_impl->Search(plan.get(), ph_group.get(), 1L << 63);

        const char* raw_plan2 = R"(vector_anns: <
                                        field_id: 102
                                        predicates: <
                                      binary_range_expr: <
                                        column_info: <
                                          field_id: 100
                                          data_type: Int64
                                        >
                                        lower_inclusive: true,
                                        upper_inclusive: false,
                                        lower_value: <
                                          int64_val: -1
                                        >
                                        upper_value: <
                                          int64_val: 1
                                        >
                                        >
                                      >
                                        query_info: <
                                          topk: 10
                                          metric_type: "L2"
                                          search_params: "{\"nprobe\": 4}"
                                        >
                                        placeholder_tag: "$0">)";
        proto::plan::PlanNode plan_node2;
        auto ok2 = google::protobuf::TextFormat::ParseFromString(raw_plan2,
                                                                 &plan_node2);
        auto plan2 = CreateSearchPlanFromPlanNode(*schema, plan_node2);
        auto search_result2 =
            segment_growing_impl->Search(plan2.get(), ph_group.get(), 1L << 63);
        CheckFilterSearchResult(
            *search_result, *search_result2, topK, num_queries);
    }
}