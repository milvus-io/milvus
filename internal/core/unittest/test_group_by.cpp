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
#include "segcore/SegmentSealedImpl.h"
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

const char* METRICS_TYPE = "metric_type";

void
prepareSegmentSystemFieldData(const std::unique_ptr<SegmentSealed>& segment,
                              size_t row_count,
                              GeneratedData& data_set) {
    auto field_data =
        std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
    field_data->FillFieldData(data_set.row_ids_.data(), row_count);
    auto field_data_info =
        FieldDataInfo{RowFieldID.get(),
                      row_count,
                      std::vector<milvus::FieldDataPtr>{field_data}};
    segment->LoadFieldData(RowFieldID, field_data_info);

    field_data = std::make_shared<milvus::FieldData<int64_t>>(DataType::INT64);
    field_data->FillFieldData(data_set.timestamps_.data(), row_count);
    field_data_info =
        FieldDataInfo{TimestampFieldID.get(),
                      row_count,
                      std::vector<milvus::FieldDataPtr>{field_data}};
    segment->LoadFieldData(TimestampFieldID, field_data_info);
}

int
GetSearchResultBound(const SearchResult& search_result) {
    int i = 0;
    for (; i < search_result.seg_offsets_.size(); i++) {
        if (search_result.seg_offsets_[i] == INVALID_SEG_OFFSET)
            break;
    }
    return i - 1;
}

void
CheckGroupBySearchResult(const SearchResult& search_result,
                         int topK,
                         int nq,
                         bool strict) {
    int total = topK * nq;
    ASSERT_EQ(search_result.group_by_values_.value().size(), total);
    ASSERT_EQ(search_result.seg_offsets_.size(), total);
    ASSERT_EQ(search_result.distances_.size(), total);
    ASSERT_TRUE(search_result.seg_offsets_[0] != INVALID_SEG_OFFSET);
    int res_bound = GetSearchResultBound(search_result);
    ASSERT_TRUE(res_bound > 0);
    if (strict) {
        ASSERT_TRUE(res_bound == total - 1);
    } else {
        ASSERT_TRUE(res_bound == total - 1 ||
                    search_result.seg_offsets_[res_bound + 1] ==
                        INVALID_SEG_OFFSET);
    }
}

TEST(GroupBY, SealedIndex) {
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
    auto raw_data = DataGen(schema, N);
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();

        segment->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentSystemFieldData(segment, N, raw_data);

    //3. load index
    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index = std::move(indexing);
    load_index_info.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);
    int topK = 100;

    //4. search group by int8
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                        >
                                        placeholder_tag: "$0"

         >)";
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();
        std::unordered_set<int8_t> i8_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int8_t>(group_by_values[i])) {
                int8_t g_val = std::get<int8_t>(group_by_values[i]);
                ASSERT_FALSE(i8_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i8_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }

    //4. search group by int16
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 102
                                        >
                                        placeholder_tag: "$0"

         >)";

        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();
        std::unordered_set<int16_t> i16_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int16_t>(group_by_values[i])) {
                int16_t g_val = std::get<int16_t>(group_by_values[i]);
                ASSERT_FALSE(i16_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i16_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }

    //4. search group by int32
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 103
                                        >
                                        placeholder_tag: "$0"

         >)";

        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();

        std::unordered_set<int32_t> i32_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int32_t>(group_by_values[i])) {
                int16_t g_val = std::get<int32_t>(group_by_values[i]);
                ASSERT_FALSE(i32_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i32_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }

    //4. search group by int64
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 104
                                        >
                                        placeholder_tag: "$0"

         >)";

        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values = search_result->group_by_values_.value();

        int size = group_by_values.size();
        std::unordered_set<int64_t> i64_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int64_t>(group_by_values[i])) {
                int16_t g_val = std::get<int64_t>(group_by_values[i]);
                ASSERT_FALSE(i64_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i64_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }

    //4. search group by string
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 105
                                        >
                                        placeholder_tag: "$0"

         >)";

        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();
        std::unordered_set<std::string> strs_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<std::string>(group_by_values[i])) {
                std::string g_val =
                    std::move(std::get<std::string>(group_by_values[i]));
                ASSERT_FALSE(strs_set.count(g_val) >
                             0);  //no repetition on groupBy field
                strs_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }

    //4. search group by bool
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 106
                                        >
                                        placeholder_tag: "$0"

         >)";

        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();
        std::unordered_set<bool> bools_set;
        int boolValCount = 0;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<bool>(group_by_values[i])) {
                bool g_val = std::get<bool>(group_by_values[i]);
                ASSERT_FALSE(bools_set.count(g_val) >
                             0);  //no repetition on groupBy field
                bools_set.insert(g_val);
                boolValCount += 1;
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
            ASSERT_TRUE(boolValCount <= 2);  //bool values cannot exceed two
        }
    }
}

TEST(GroupBY, SealedData) {
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
    auto raw_data = DataGen(schema, N);
    auto fields = schema->get_fields();
    for (auto field_data : raw_data.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();

        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();

        segment->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentSystemFieldData(segment, N, raw_data);

    int topK = 100;
    //3. search group by int8
    {
        const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                        >
                                        placeholder_tag: "$0"

         >)";
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values = search_result->group_by_values_.value();
        int size = group_by_values.size();
        std::unordered_set<int8_t> i8_set;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int8_t>(group_by_values[i])) {
                int8_t g_val = std::get<int8_t>(group_by_values[i]);
                ASSERT_FALSE(i8_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i8_set.insert(g_val);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[i], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[i], 0.0);
            }
        }
    }
}

TEST(GroupBY, Reduce) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto fp16_fid = schema->AddDebugField(
        "fakevec_fp16", DataType::VECTOR_FLOAT16, dim, knowhere::metric::L2);
    auto bf16_fid = schema->AddDebugField(
        "fakevec_bf16", DataType::VECTOR_BFLOAT16, dim, knowhere::metric::L2);
    schema->set_primary_field_id(int64_fid);
    auto segment1 = CreateSealedSegment(schema);
    auto segment2 = CreateSealedSegment(schema);

    //1. load raw data
    size_t N = 100;
    uint64_t seed = 512;
    uint64_t ts_offset = 0;
    int repeat_count_1 = 2;
    int repeat_count_2 = 5;
    auto raw_data1 = DataGen(schema, N, seed, ts_offset, repeat_count_1);
    auto raw_data2 = DataGen(schema, N, seed, ts_offset, repeat_count_2);

    auto fields = schema->get_fields();
    //load segment1 raw data
    for (auto field_data : raw_data1.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();
        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();
        segment1->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentSystemFieldData(segment1, N, raw_data1);

    //load segment2 raw data
    for (auto field_data : raw_data2.raw_->fields_data()) {
        int64_t field_id = field_data.field_id();
        auto info = FieldDataInfo(field_data.field_id(), N);
        auto field_meta = fields.at(FieldId(field_id));
        info.channel->push(
            CreateFieldDataFromDataArray(N, &field_data, field_meta));
        info.channel->close();
        segment2->LoadFieldData(FieldId(field_id), info);
    }
    prepareSegmentSystemFieldData(segment2, N, raw_data2);

    //3. load index
    auto vector_data_1 = raw_data1.get_col<float>(vec_fid);
    auto indexing_1 = GenVecIndexing(
        N, dim, vector_data_1.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_1;
    load_index_info_1.field_id = vec_fid.get();
    load_index_info_1.index = std::move(indexing_1);
    load_index_info_1.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment1->LoadIndex(load_index_info_1);

    auto vector_data_2 = raw_data2.get_col<float>(vec_fid);
    auto indexing_2 = GenVecIndexing(
        N, dim, vector_data_2.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_2;
    load_index_info_2.field_id = vec_fid.get();
    load_index_info_2.index = std::move(indexing_2);
    load_index_info_2.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment2->LoadIndex(load_index_info_2);

    //4. search group by respectively
    const char* raw_plan = R"(vector_anns: <
                                        field_id: 100
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                        >
                                        placeholder_tag: "$0"

         >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 10;
    auto topK = 100;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    CPlaceholderGroup c_ph_group = ph_group.release();
    CSearchPlan c_plan = plan.release();

    CSegmentInterface c_segment_1 = segment1.release();
    CSegmentInterface c_segment_2 = segment2.release();
    CSearchResult c_search_res_1;
    CSearchResult c_search_res_2;
    auto status =
        Search({}, c_segment_1, c_plan, c_ph_group, 1L << 63, &c_search_res_1);
    ASSERT_EQ(status.error_code, Success);
    status =
        Search({}, c_segment_2, c_plan, c_ph_group, 1L << 63, &c_search_res_2);
    ASSERT_EQ(status.error_code, Success);
    std::vector<CSearchResult> results;
    results.push_back(c_search_res_1);
    results.push_back(c_search_res_2);

    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    auto slice_topKs = std::vector<int64_t>{topK / 2, topK};
    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            c_plan,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    CheckSearchResultDuplicate(results);
    DeleteSearchResult(c_search_res_1);
    DeleteSearchResult(c_search_res_2);
    DeleteSearchResultDataBlobs(cSearchResultData);

    DeleteSearchPlan(c_plan);
    DeletePlaceholderGroup(c_ph_group);
    DeleteSegment(c_segment_1);
    DeleteSegment(c_segment_2);
}

TEST(GroupBY, GrowingRawData) {
    //0. set up growing segment
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
    config.set_chunk_rows(128);
    config.set_enable_interim_segment_index(
        false);  //no growing index, test brute force
    auto segment_growing = CreateGrowingSegment(schema, nullptr, 1, config);
    auto segment_growing_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    //1. prepare raw data in growing segment
    int64_t rows_per_batch = 512;
    int n_batch = 3;
    for (int i = 0; i < n_batch; i++) {
        auto data_set = DataGen(schema, rows_per_batch);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    //2. Search group by
    const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                        >
                                        placeholder_tag: "$0"

         >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 10;
    auto topK = 100;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment_growing_impl->Search(plan.get(), ph_group.get(), 1L << 63);
    CheckGroupBySearchResult(*search_result, topK, num_queries, true);

    auto& group_by_values = search_result->group_by_values_.value();
    int idx = 0;
    for (int i = 0; i < num_queries; i++) {
        std::unordered_set<int32_t> i32_set;
        float lastDistance = 0.0;
        for (int j = 0; j < topK; j++) {
            if (std::holds_alternative<int32_t>(group_by_values[idx])) {
                int32_t g_val = std::get<int32_t>(group_by_values[idx]);
                ASSERT_FALSE(i32_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i32_set.insert(g_val);
                auto distance = search_result->distances_.at(idx);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[idx], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[idx], 0.0);
            }
            idx++;
        }
    }
}

TEST(GroupBY, GrowingIndex) {
    //0. set up growing segment
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
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec_field_id, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> fieldMap = {
        {vec_field_id, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(10000, std::move(fieldMap));

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(128);
    config.set_enable_interim_segment_index(
        true);  //no growing index, test growing inter index
    config.set_nlist(128);
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment_growing_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    //1. prepare raw data in growing segment
    int64_t rows_per_batch = 1024;
    int n_batch = 10;
    for (int i = 0; i < n_batch; i++) {
        auto data_set = DataGen(schema, rows_per_batch);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    //2. Search group by int32
    const char* raw_plan = R"(vector_anns: <
                                        field_id: 102
                                        query_info: <
                                          topk: 100
                                          metric_type: "L2"
                                          search_params: "{\"ef\": 10}"
                                          group_by_field_id: 101
                                        >
                                        placeholder_tag: "$0"

         >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    auto num_queries = 10;
    auto topK = 100;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment_growing_impl->Search(plan.get(), ph_group.get(), 1L << 63);
    CheckGroupBySearchResult(*search_result, topK, num_queries, true);

    auto& group_by_values = search_result->group_by_values_.value();
    int idx = 0;
    for (int i = 0; i < num_queries; i++) {
        std::unordered_set<int32_t> i32_set;
        float lastDistance = 0.0;
        for (int j = 0; j < topK; j++) {
            if (std::holds_alternative<int32_t>(group_by_values[idx])) {
                int32_t g_val = std::get<int32_t>(group_by_values[idx]);
                ASSERT_FALSE(i32_set.count(g_val) >
                             0);  //no repetition on groupBy field
                i32_set.insert(g_val);
                auto distance = search_result->distances_.at(idx);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            } else {
                //check padding
                ASSERT_EQ(search_result->seg_offsets_[idx], INVALID_SEG_OFFSET);
                ASSERT_EQ(search_result->distances_[idx], 0.0);
            }
            idx++;
        }
    }
}