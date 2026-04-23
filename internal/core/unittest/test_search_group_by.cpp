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

#include <folly/FBVector.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "common/common_type_c.h"
#include "common/protobuf_utils.h"
#include "common/type_c.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/Utils.h"
#include "segcore/Collection.h"
#include "segcore/ReduceStructure.h"
#include "segcore/ReduceUtils.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/plan_c.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "storage/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::storage;
using namespace milvus::tracer;
using namespace milvus::exec;

namespace {

CompositeGroupKey
MakeInt32CompositeKey(int32_t v) {
    CompositeGroupKey key;
    key.Add(GroupByValueType(std::in_place, v));
    return key;
}

}  // namespace

const char* METRICS_TYPE = "metric_type";

TEST(GroupBY, SealedIndex) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);  // id 101
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto timestamptz_fid =
        schema->AddDebugField("timestamptz", DataType::TIMESTAMPTZ);
    schema->set_primary_field_id(str_fid);
    size_t N = 50;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, 1, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    //3. load index
    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);
    int topK = 15;
    int group_size = 3;

    // Create ScopedSchemaHandle for parsing expressions
    ScopedSchemaHandle handle(*schema);

    //4. search group by int8
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      topK,       // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",  // search params
                                      int8_fid.get(),  // group_by_field_id
                                      group_size       // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        ASSERT_EQ(20, group_by_values.size());
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        int size = group_by_values.size();
        std::unordered_map<int8_t, int> i8_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int8_t>(group_by_values[i][0].value())) {
                int8_t g_val = std::get<int8_t>(group_by_values[i][0].value());
                i8_map[g_val] += 1;
                ASSERT_TRUE(i8_map[g_val] <= group_size);
                //for every group, the number of hits should not exceed group_size
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(i8_map.size() <= topK);
        ASSERT_TRUE(i8_map.size() == 7);
    }

    //5. search group by int16
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      100,        // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",   // search params
                                      int16_fid.get(),  // group_by_field_id
                                      group_size        // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int16_t, int> i16_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int16_t>(
                    group_by_values[i][0].value())) {
                int16_t g_val =
                    std::get<int16_t>(group_by_values[i][0].value());
                i16_map[g_val] += 1;
                ASSERT_TRUE(i16_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(i16_map.size() == 7);
    }
    //6. search group by int32
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      100,        // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",   // search params
                                      int32_fid.get(),  // group_by_field_id
                                      group_size        // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int32_t, int> i32_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int32_t>(
                    group_by_values[i][0].value())) {
                int16_t g_val =
                    std::get<int32_t>(group_by_values[i][0].value());
                i32_map[g_val] += 1;
                ASSERT_TRUE(i32_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(i32_map.size() == 7);
    }

    //7. search group by int64
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      100,        // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",   // search params
                                      int64_fid.get(),  // group_by_field_id
                                      group_size        // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int64_t, int> i64_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int64_t>(
                    group_by_values[i][0].value())) {
                int16_t g_val =
                    std::get<int64_t>(group_by_values[i][0].value());
                i64_map[g_val] += 1;
                ASSERT_TRUE(i64_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(i64_map.size() == 7);
    }

    //8. search group by string
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      100,        // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",  // search params
                                      str_fid.get(),   // group_by_field_id
                                      group_size       // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        ASSERT_EQ(20, group_by_values.size());
        int size = group_by_values.size();

        std::unordered_map<std::string, int> strs_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<std::string>(
                    group_by_values[i][0].value())) {
                std::string g_val = std::move(
                    std::get<std::string>(group_by_values[i][0].value()));
                strs_map[g_val] += 1;
                ASSERT_TRUE(strs_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(strs_map.size() == 7);
    }

    //9. search group by bool
    {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      100,        // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",  // search params
                                      bool_fid.get(),  // group_by_field_id
                                      group_size       // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        ASSERT_EQ(size, 6);
        //as there are only two possible values: true, false
        //for each group, there are at most 3 items, so the final size of group_by_vals is 3 * 2 = 6

        std::unordered_map<bool, int> bools_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<bool>(group_by_values[i][0].value())) {
                bool g_val = std::get<bool>(group_by_values[i][0].value());
                bools_map[g_val] += 1;
                ASSERT_TRUE(bools_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(bools_map.size() == 2);  //bool values cannot exceed two
    }

    //10. search group by timestamptz
    {
        auto plan_str = handle.ParseGroupBySearch(
            "",                     // empty filter expression
            "fakevec",              // vector field name
            100,                    // topk
            "L2",                   // metric type
            "{\"ef\": 10}",         // search params
            timestamptz_fid.get(),  // group_by_field_id
            group_size              // group_size
        );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);
        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int64_t, int> timestamptz_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int64_t>(
                    group_by_values[i][0].value())) {
                int16_t g_val =
                    std::get<int64_t>(group_by_values[i][0].value());
                timestamptz_map[g_val] += 1;
                ASSERT_TRUE(timestamptz_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(i);
                ASSERT_TRUE(lastDistance <= distance);
                lastDistance = distance;
            }
        }
        ASSERT_TRUE(timestamptz_map.size() == 7);
    }
}

TEST(GroupBY, SealedData) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, true);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("int32", DataType::INT32);
    schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->AddDebugField("bool", DataType::BOOL);
    schema->set_primary_field_id(str_fid);
    size_t N = 100;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 20, 10, 1, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    int topK = 10;
    int group_size = 5;

    // Create ScopedSchemaHandle for parsing expressions
    ScopedSchemaHandle handle(*schema);

    //3. search group by int8
    {
        auto plan_str = handle.ParseGroupBySearch(
            "",                                     // empty filter expression
            "fakevec",                              // vector field name
            topK,                                   // topk
            "L2",                                   // metric type
            "{\"ef\": 10}",                         // search params
            int8_fid.get(),                         // group_by_field_id
            group_size,                             // group_size
            "",                                     // json_path (not used)
            milvus::proto::schema::DataType::None,  // json_type (not used)
            true                                    // strict_group_size
        );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        CheckGroupBySearchResult(*search_result, topK, num_queries, false);

        auto& group_by_values =
            search_result->composite_group_by_values_.value();
        int size = group_by_values.size();
        // groups are: (0, 1, 2, 3, 4, null), counts are: (10, 10, 10, 10 ,10, 50)
        ASSERT_EQ(30, size);

        std::unordered_map<int8_t, int> i8_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (group_by_values[i][0].has_value()) {
                int8_t g_val = std::get<int8_t>(group_by_values[i][0].value());
                i8_map[g_val] += 1;
            } else {
                i8_map[-1] += 1;
            }
            auto distance = search_result->distances_.at(i);
            ASSERT_TRUE(
                lastDistance <=
                distance);  //distance should be decreased as metrics_type is L2
            lastDistance = distance;
        }

        ASSERT_EQ(i8_map.size(), 6);
        for (const auto& it : i8_map) {
            ASSERT_TRUE(it.second == group_size)
                << "unexpected count on group " << it.first;
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
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, false);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, false);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16, false);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, false);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL, false);
    auto string_fid = schema->AddDebugField("string", DataType::VARCHAR, false);
    schema->AddDebugField(
        "fakevec_fp16", DataType::VECTOR_FLOAT16, dim, knowhere::metric::L2);
    schema->AddDebugField(
        "fakevec_bf16", DataType::VECTOR_BFLOAT16, dim, knowhere::metric::L2);
    schema->set_primary_field_id(int64_fid);

    //1. load raw data
    size_t N = 100;
    uint64_t seed = 512;
    uint64_t ts_offset = 0;
    int repeat_count_1 = 2;
    int repeat_count_2 = 5;
    auto raw_data1 = DataGen(
        schema, N, seed, ts_offset, repeat_count_1, 10, 10, false, false);
    auto raw_data2 = DataGen(
        schema, N, seed, ts_offset, repeat_count_2, 10, 10, false, false);

    auto segment1 = CreateSealedWithFieldDataLoaded(schema, raw_data1);
    auto segment2 = CreateSealedWithFieldDataLoaded(schema, raw_data2);

    //3. load index
    auto vector_data_1 = raw_data1.get_col<float>(vec_fid);
    auto indexing_1 = GenVecIndexing(
        N, dim, vector_data_1.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_1;
    load_index_info_1.field_id = vec_fid.get();
    load_index_info_1.index_params = GenIndexParams(indexing_1.get());
    load_index_info_1.cache_index =
        CreateTestCacheIndex("test", std::move(indexing_1));
    load_index_info_1.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment1->LoadIndex(load_index_info_1);

    auto vector_data_2 = raw_data2.get_col<float>(vec_fid);
    auto indexing_2 = GenVecIndexing(
        N, dim, vector_data_2.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info_2;
    load_index_info_2.field_id = vec_fid.get();
    load_index_info_2.index_params = GenIndexParams(indexing_2.get());
    load_index_info_2.cache_index =
        CreateTestCacheIndex("test", std::move(indexing_2));
    load_index_info_2.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment2->LoadIndex(load_index_info_2);
    CSegmentInterface c_segment_1 = segment1.release();
    CSegmentInterface c_segment_2 = segment2.release();

    //4. search group by respectively
    auto num_queries = 10;
    auto topK = 10;
    int group_size = 3;
    auto slice_nqs = std::vector<int64_t>{num_queries / 2, num_queries / 2};
    auto slice_topKs = std::vector<int64_t>{topK / 2, topK};

    // Create ScopedSchemaHandle for parsing expressions
    ScopedSchemaHandle handle(*schema);

    // Lambda function to execute search and reduce with given group_by_field_id
    auto executeGroupBySearchAndReduce = [&](int64_t group_by_field_id) {
        auto plan_str =
            handle.ParseGroupBySearch("",         // empty filter expression
                                      "fakevec",  // vector field name
                                      topK,       // topk
                                      "L2",       // metric type
                                      "{\"ef\": 10}",     // search params
                                      group_by_field_id,  // group_by_field_id
                                      group_size          // group_size
            );
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        CPlaceholderGroup c_ph_group = ph_group.release();
        CSearchPlan c_plan = plan.release();

        CSearchResult c_search_res_1;
        CSearchResult c_search_res_2;
        auto status = CSearch(
            c_segment_1, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_1);
        ASSERT_EQ(status.error_code, Success);
        status = CSearch(
            c_segment_2, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_2);
        ASSERT_EQ(status.error_code, Success);
        std::vector<CSearchResult> results;
        results.push_back(c_search_res_1);
        results.push_back(c_search_res_2);

        CSearchResultDataBlobs cSearchResultData;
        status = ReduceSearchResultsAndFillData({},
                                                &cSearchResultData,
                                                c_plan,
                                                c_ph_group,
                                                results.data(),
                                                results.size(),
                                                slice_nqs.data(),
                                                slice_topKs.data(),
                                                slice_nqs.size());
        CheckGroupByReducedSearchResult(cSearchResultData, group_size);
        DeleteSearchResult(c_search_res_1);
        DeleteSearchResult(c_search_res_2);
        DeleteSearchResultDataBlobs(cSearchResultData);
        DeleteSearchPlan(c_plan);
        DeletePlaceholderGroup(c_ph_group);
    };

    // Execute the test with group by INT64 field
    executeGroupBySearchAndReduce(int64_fid.get());

    // Test Case: Group by INT32 field
    executeGroupBySearchAndReduce(int32_fid.get());

    // Test Case: Group by INT16 field
    executeGroupBySearchAndReduce(int16_fid.get());

    // Test Case: Group by INT8 field
    executeGroupBySearchAndReduce(int8_fid.get());

    // Test Case: Group by BOOL field
    executeGroupBySearchAndReduce(bool_fid.get());

    // Test Case: Group by VARCHAR field
    executeGroupBySearchAndReduce(string_fid.get());

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
    schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field_id);

    auto config = SegcoreConfig::default_config();
    auto original_chunk_rows = config.get_chunk_rows();
    DeferLambda([&]() { config.set_chunk_rows(original_chunk_rows); });
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
        auto data_set =
            DataGen(schema, rows_per_batch, 42, 0, 8, 10, 1, false, false);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    //2. Search group by
    auto num_queries = 10;
    auto topK = 100;
    int group_size = 1;

    // Create ScopedSchemaHandle for parsing expressions
    ScopedSchemaHandle handle(*schema);

    auto plan_str =
        handle.ParseGroupBySearch("",              // empty filter expression
                                  "embeddings",    // vector field name
                                  topK,            // topk
                                  "L2",            // metric type
                                  "{\"ef\": 10}",  // search params
                                  int32_field_id.get(),  // group_by_field_id
                                  group_size             // group_size
        );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment_growing_impl->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    CheckGroupBySearchResult(*search_result, topK, num_queries, true);

    auto& group_by_values = search_result->composite_group_by_values_.value();
    int size = group_by_values.size();
    ASSERT_EQ(size, 640);
    //as the number of data is 512 and repeated count is 8, the group number is 64 for every query
    //and the total group number should be 640
    int expected_group_count = 64;
    int idx = 0;
    for (int i = 0; i < num_queries; i++) {
        std::unordered_set<int32_t> i32_set;
        float lastDistance = 0.0;
        for (int j = 0; j < expected_group_count; j++) {
            if (std::holds_alternative<int32_t>(
                    group_by_values[idx][0].value())) {
                int32_t g_val =
                    std::get<int32_t>(group_by_values[idx][0].value());
                ASSERT_FALSE(
                    i32_set.count(g_val) >
                    0);  //as the group_size is 1, there should not be any duplication for group_by value
                i32_set.insert(g_val);
                auto distance = search_result->distances_.at(idx);
                ASSERT_TRUE(lastDistance <= distance);
                lastDistance = distance;
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
    auto original_chunk_rows = config.get_chunk_rows();
    DeferLambda([&]() { config.set_chunk_rows(original_chunk_rows); });
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
        auto data_set =
            DataGen(schema, rows_per_batch, 42, 0, 8, 10, 10, false, false);
        auto offset = segment_growing_impl->PreInsert(rows_per_batch);
        segment_growing_impl->Insert(offset,
                                     rows_per_batch,
                                     data_set.row_ids_.data(),
                                     data_set.timestamps_.data(),
                                     data_set.raw_);
    }

    //2. Search group by int32
    auto num_queries = 10;
    auto topK = 100;
    int group_size = 3;

    // Create ScopedSchemaHandle for parsing expressions
    ScopedSchemaHandle handle(*schema);

    auto plan_str = handle.ParseGroupBySearch(
        "",                                     // empty filter expression
        "embeddings",                           // vector field name
        topK,                                   // topk
        "L2",                                   // metric type
        "{\"ef\": 10}",                         // search params
        int32_field_id.get(),                   // group_by_field_id
        group_size,                             // group_size
        "",                                     // json_path (not used)
        milvus::proto::schema::DataType::None,  // json_type (not used)
        true                                    // strict_group_size
    );
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment_growing_impl->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    CheckGroupBySearchResult(*search_result, topK, num_queries, true);

    auto& group_by_values = search_result->composite_group_by_values_.value();
    auto size = group_by_values.size();
    int expected_group_count = 100;
    ASSERT_EQ(size, expected_group_count * group_size * num_queries);
    int idx = 0;
    for (int i = 0; i < num_queries; i++) {
        std::unordered_map<int32_t, int> i32_map;
        float lastDistance = 0.0;
        for (int j = 0; j < expected_group_count * group_size; j++) {
            if (std::holds_alternative<int32_t>(
                    group_by_values[idx][0].value())) {
                int32_t g_val =
                    std::get<int32_t>(group_by_values[idx][0].value());
                i32_map[g_val] += 1;
                ASSERT_TRUE(i32_map[g_val] <= group_size);
                auto distance = search_result->distances_.at(idx);
                ASSERT_TRUE(
                    lastDistance <=
                    distance);  //distance should be decreased as metrics_type is L2
                lastDistance = distance;
            }
            idx++;
        }
        ASSERT_EQ(i32_map.size(), expected_group_count);
        for (const auto& map_pair : i32_map) {
            ASSERT_EQ(group_size, map_pair.second);
        }
    }
}

// =========================================================================
// Multi-field composite group_by tests
// =========================================================================

// Semantic verification: for each result doc, read back the ground truth
// field values from raw_data using seg_offset, and assert they match the
// composite key returned by the search.
TEST(GroupBY, CompositeKeySealedIndex) {
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // N=100, repeat_count=5, random_val=false =>
    //   int32[i] = i/5, int64[i] = i/5 (since int64 is PK with random_pk=false)
    //   so (int32, int64) are always identical => effectively same as single-field
    //   with 20 unique values (0..19), each repeated 5 times
    //
    // To get a truly multi-field test, use repeat_count=10 so:
    //   int32[i] = i/10 => 10 unique values (0..9)
    //   int64[i] = i/10 => 10 unique values (0..9) — same mapping
    //   composite key (int32, int64) = (v, v) — 10 unique combos
    //
    // With topK=8, group_size=2: expect 8 groups × 2 = 16 results
    size_t N = 100;
    int repeat_count = 10;
    auto raw_data =
        DataGen(schema, N, 42, 0, repeat_count, 10, 1, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);

    int topK = 8;
    int group_size = 2;

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseCompositeGroupBySearch("",
                                           "fakevec",
                                           topK,
                                           "L2",
                                           "{\"ef\": 100}",
                                           {int32_fid.get(), int64_fid.get()},
                                           group_size,
                                           true /* strict_group_size */);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);

    ASSERT_TRUE(search_result->composite_group_by_values_.has_value());
    auto& group_by_values = search_result->composite_group_by_values_.value();

    // Ground truth data for verification
    auto gt_int32 = raw_data.get_col<int32_t>(int32_fid);
    auto gt_int64 = raw_data.get_col<int64_t>(int64_fid);

    // 1) Every composite key has exactly 2 fields
    for (const auto& key : group_by_values) {
        ASSERT_EQ(key.Size(), 2);
    }

    // 2) Verify composite key values match ground truth at each seg_offset
    std::unordered_map<CompositeGroupKey, int, CompositeGroupKeyHash>
        group_count;
    float lastDistance = 0.0;
    for (size_t i = 0; i < group_by_values.size(); i++) {
        auto offset = search_result->seg_offsets_[i];
        ASSERT_GE(offset, 0);
        ASSERT_LT(offset, static_cast<int64_t>(N));

        int32_t expected_int32 = gt_int32[offset];
        int64_t expected_int64 = gt_int64[offset];
        int32_t actual_int32 = std::get<int32_t>(group_by_values[i][0].value());
        int64_t actual_int64 = std::get<int64_t>(group_by_values[i][1].value());

        ASSERT_EQ(expected_int32, actual_int32)
            << "int32 mismatch at result index " << i
            << ", seg_offset=" << offset;
        ASSERT_EQ(expected_int64, actual_int64)
            << "int64 mismatch at result index " << i
            << ", seg_offset=" << offset;

        group_count[group_by_values[i]] += 1;
        ASSERT_LE(group_count[group_by_values[i]], group_size)
            << "group_size exceeded for key (" << actual_int32 << ", "
            << actual_int64 << ")";

        auto distance = search_result->distances_.at(i);
        ASSERT_LE(lastDistance, distance)
            << "distances not non-decreasing at index " << i;
        lastDistance = distance;
    }

    // 3) Exact group count: 10 unique combos, topK=8 => exactly 8 groups
    ASSERT_EQ(static_cast<int>(group_count.size()), topK);
    // 4) strict_group_size=true => every group is fully filled
    for (const auto& [key, cnt] : group_count) {
        ASSERT_EQ(cnt, group_size)
            << "with strict_group_size, every group must have exactly "
               "group_size results";
    }
    // 5) Total results: 8 groups × 2 = 16
    ASSERT_EQ(static_cast<int>(group_by_values.size()), topK * group_size);
}

TEST(GroupBY, CompositeKeyGrowing) {
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // N=60, repeat_count=10, random_val=false =>
    //   int8[i] = i/10 => 6 unique (0..5)
    //   int32[i] = i/10 => 6 unique (0..5) — same values
    //   composite (int8, int32): 6 unique combos, each repeated 10 times
    size_t N = 60;
    int repeat_count = 10;
    auto raw_data =
        DataGen(schema, N, 42, 0, repeat_count, 10, 1, false, false);

    auto segment_growing = CreateGrowingSegment(schema, empty_index_meta);
    segment_growing->PreInsert(N);
    segment_growing->Insert(0,
                            N,
                            raw_data.row_ids_.data(),
                            raw_data.timestamps_.data(),
                            raw_data.raw_);

    int topK = 10;
    int group_size = 3;

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseCompositeGroupBySearch("",
                                           "fakevec",
                                           topK,
                                           "L2",
                                           "{}",
                                           {int8_fid.get(), int32_fid.get()},
                                           group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment_growing->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);

    ASSERT_TRUE(search_result->composite_group_by_values_.has_value());
    auto& group_by_values = search_result->composite_group_by_values_.value();

    auto gt_int8 = raw_data.get_col<int8_t>(int8_fid);
    auto gt_int32 = raw_data.get_col<int32_t>(int32_fid);

    std::unordered_map<CompositeGroupKey, int, CompositeGroupKeyHash>
        group_count;
    for (size_t i = 0; i < group_by_values.size(); i++) {
        ASSERT_EQ(group_by_values[i].Size(), 2);
        auto offset = search_result->seg_offsets_[i];
        ASSERT_GE(offset, 0);

        int8_t actual_int8 = std::get<int8_t>(group_by_values[i][0].value());
        int32_t actual_int32 = std::get<int32_t>(group_by_values[i][1].value());
        ASSERT_EQ(gt_int8[offset], actual_int8);
        ASSERT_EQ(gt_int32[offset], actual_int32);

        group_count[group_by_values[i]] += 1;
        ASSERT_LE(group_count[group_by_values[i]], group_size);
    }

    // 6 unique combos, topK=10 => all 6 groups returned
    ASSERT_EQ(static_cast<int>(group_count.size()), 6);
    // 6 groups × 3 = 18 results
    ASSERT_EQ(static_cast<int>(group_by_values.size()), 6 * group_size);
    // Every group should be fully filled (10 rows per group > group_size=3)
    for (const auto& [key, cnt] : group_count) {
        ASSERT_EQ(cnt, group_size)
            << "every group should have exactly group_size results";
    }
}

TEST(GroupBY, CompositeKeyReduce) {
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, false);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, false);
    schema->set_primary_field_id(int64_fid);

    size_t N = 100;
    uint64_t seed = 512;
    int repeat_count = 10;
    auto raw_data1 =
        DataGen(schema, N, seed, 0, repeat_count, 10, 10, false, false);
    auto raw_data2 =
        DataGen(schema, N, seed + 1, 0, repeat_count, 10, 10, false, false);

    auto segment1 = CreateSealedWithFieldDataLoaded(schema, raw_data1);
    auto segment2 = CreateSealedWithFieldDataLoaded(schema, raw_data2);

    auto vector_data_1 = raw_data1.get_col<float>(vec_fid);
    auto indexing_1 = GenVecIndexing(
        N, dim, vector_data_1.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo li1;
    li1.field_id = vec_fid.get();
    li1.index_params = GenIndexParams(indexing_1.get());
    li1.cache_index = CreateTestCacheIndex("test", std::move(indexing_1));
    li1.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment1->LoadIndex(li1);

    auto vector_data_2 = raw_data2.get_col<float>(vec_fid);
    auto indexing_2 = GenVecIndexing(
        N, dim, vector_data_2.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo li2;
    li2.field_id = vec_fid.get();
    li2.index_params = GenIndexParams(indexing_2.get());
    li2.cache_index = CreateTestCacheIndex("test", std::move(indexing_2));
    li2.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment2->LoadIndex(li2);

    CSegmentInterface c_segment_1 = segment1.release();
    CSegmentInterface c_segment_2 = segment2.release();

    int num_queries = 5;
    int topK = 8;
    int group_size = 2;
    auto slice_nqs = std::vector<int64_t>{3, 2};
    auto slice_topKs = std::vector<int64_t>{topK, topK};

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseCompositeGroupBySearch("",
                                           "fakevec",
                                           topK,
                                           "L2",
                                           "{\"ef\": 100}",
                                           {int32_fid.get(), int64_fid.get()},
                                           group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    CPlaceholderGroup c_ph_group = ph_group.release();
    CSearchPlan c_plan = plan.release();

    CSearchResult c_search_res_1;
    CSearchResult c_search_res_2;
    auto status = CSearch(
        c_segment_1, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_1);
    ASSERT_EQ(status.error_code, Success);
    status = CSearch(
        c_segment_2, c_plan, c_ph_group, MAX_TIMESTAMP, &c_search_res_2);
    ASSERT_EQ(status.error_code, Success);

    std::vector<CSearchResult> results{c_search_res_1, c_search_res_2};

    CSearchResultDataBlobs cSearchResultData;
    status = ReduceSearchResultsAndFillData({},
                                            &cSearchResultData,
                                            c_plan,
                                            c_ph_group,
                                            results.data(),
                                            results.size(),
                                            slice_nqs.data(),
                                            slice_topKs.data(),
                                            slice_nqs.size());
    ASSERT_EQ(status.error_code, Success)
        << "Reduce failed: " << status.error_msg;

    // Post-reduce: verify no duplicate PKs and group_size respected per composite key
    CheckGroupByReducedSearchResult(cSearchResultData, group_size);

    DeleteSearchResult(c_search_res_1);
    DeleteSearchResult(c_search_res_2);
    DeleteSearchResultDataBlobs(cSearchResultData);
    DeleteSearchPlan(c_plan);
    DeletePlaceholderGroup(c_ph_group);
    DeleteSegment(c_segment_1);
    DeleteSegment(c_segment_2);
}

TEST(GroupBY, SingleFieldViaCompositeProtoPath) {
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->set_primary_field_id(str_fid);

    // N=50, repeat_count=8 => int32 values: 0,0,...,0(8x),1,1,...(8x),...,6(2x)
    // 7 unique values, last group has only 2 rows
    size_t N = 50;
    int repeat_count = 8;
    auto raw_data =
        DataGen(schema, N, 42, 0, repeat_count, 10, 1, false, false);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    auto vector_data = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vector_data.data(), knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params[METRICS_TYPE] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);

    int topK = 15;
    int group_size = 3;

    ScopedSchemaHandle handle(*schema);

    // Use new group_by_field_ids (plural) proto path with single field
    auto plan_str = handle.ParseCompositeGroupBySearch("",
                                                       "fakevec",
                                                       topK,
                                                       "L2",
                                                       "{\"ef\": 10}",
                                                       {int32_fid.get()},
                                                       group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto search_result =
        segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);

    ASSERT_TRUE(search_result->composite_group_by_values_.has_value());
    auto& group_by_values = search_result->composite_group_by_values_.value();

    auto gt_int32 = raw_data.get_col<int32_t>(int32_fid);

    // key.Size() == 1 for single-field path
    for (const auto& key : group_by_values) {
        ASSERT_EQ(key.Size(), 1);
    }

    // Verify values match ground truth + exact group structure
    // 7 unique values, group_size=3: groups 0..5 have 3 results, group 6 has 2
    // total = 6*3 + 1*2 = 20
    ASSERT_EQ(static_cast<int>(group_by_values.size()), 20);

    std::unordered_map<int32_t, int> group_count;
    for (size_t i = 0; i < group_by_values.size(); i++) {
        auto offset = search_result->seg_offsets_[i];
        int32_t actual = std::get<int32_t>(group_by_values[i][0].value());
        ASSERT_EQ(gt_int32[offset], actual)
            << "ground truth mismatch at index " << i;
        group_count[actual] += 1;
        ASSERT_LE(group_count[actual], group_size);
    }
    ASSERT_EQ(static_cast<int>(group_count.size()), 7);
}

// --- CompositeGroupByMap boundary tests (search group-by hot path) ---

TEST(GroupBY, CompositeGroupByMapZeroCapacity) {
    CompositeGroupByMap m(0, 2, false);
    auto k = MakeInt32CompositeKey(1);
    ASSERT_FALSE(m.Push(k));
    ASSERT_EQ(m.GetGroupCount(), 0);
    ASSERT_FALSE(m.Push(MakeInt32CompositeKey(2)));
    ASSERT_EQ(m.GetGroupCount(), 0);
}

TEST(GroupBY, CompositeGroupByMapZeroGroupSize) {
    // group_size == 0: Push must reject; count is treated as 0 for new keys
    // (same as legacy operator[] path: 0 >= 0). Map may retain a zero-count
    // entry after try_emplace — document so regressions are visible.
    CompositeGroupByMap m(8, 0, false);
    auto k = MakeInt32CompositeKey(42);
    ASSERT_FALSE(m.Push(k));
    ASSERT_EQ(m.GetGroupCount(), 1);
    ASSERT_FALSE(m.Push(k));
    ASSERT_EQ(m.GetGroupCount(), 1);
}

TEST(GroupBY, CompositeGroupByMapStrictGroupSizeEnough) {
    CompositeGroupByMap m(2, 2, true);
    ASSERT_FALSE(m.IsGroupResEnough());

    ASSERT_TRUE(m.Push(MakeInt32CompositeKey(1)));
    ASSERT_FALSE(m.IsGroupResEnough());

    ASSERT_TRUE(m.Push(MakeInt32CompositeKey(1)));
    ASSERT_FALSE(m.IsGroupResEnough());

    ASSERT_TRUE(m.Push(MakeInt32CompositeKey(2)));
    ASSERT_FALSE(m.IsGroupResEnough());

    ASSERT_TRUE(m.Push(MakeInt32CompositeKey(2)));
    ASSERT_TRUE(m.IsGroupResEnough());
    ASSERT_EQ(m.GetGroupCount(), 2);
    ASSERT_EQ(m.GetEnoughGroupCount(), 2);
}

TEST(GroupBY, CompositeGroupByMapSameKeyUntilFull) {
    CompositeGroupByMap m(4, 3, false);
    auto k = MakeInt32CompositeKey(7);
    ASSERT_TRUE(m.Push(k));
    ASSERT_TRUE(m.Push(k));
    ASSERT_TRUE(m.Push(k));
    ASSERT_FALSE(m.Push(k));
    ASSERT_EQ(m.GetGroupCount(), 1);
    ASSERT_EQ(m.GetEnoughGroupCount(), 1);
}

// --- AssembleCompositeGroupByValues empty-result regression guards ---
//
// These two tests lock in the fix for the early-return regression at
// ReduceUtils.cpp. Pre-PR AssembleGroupByValues wrote field metadata
// (field_id, name, type) even when the result set was empty; a naïve
// port that early-returned on `composite_group_by_vals.empty()` dropped
// that metadata and flipped downstream "type == 0 means no group_by"
// heuristics. Both tests must continue to pass.

TEST(GroupBY, AssembleEmptyResultSingleFieldPreservesMetadata) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->set_primary_field_id(str_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch("",
                                                       "fakevec",
                                                       /*topk=*/10,
                                                       "L2",
                                                       "{\"ef\": 10}",
                                                       {int32_fid.get()},
                                                       /*group_size=*/3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    std::vector<CompositeGroupKey> empty_vals;  // 0 hits

    AssembleCompositeGroupByValues(search_result, empty_vals, plan.get());

    // Field 17 (plural): must have 1 entry with type set + empty data
    ASSERT_EQ(search_result->group_by_field_values_size(), 1);
    const auto& entry = search_result->group_by_field_values(0);
    ASSERT_EQ(entry.field_id(), int32_fid.get());
    ASSERT_EQ(entry.field_name(), "int32");
    ASSERT_EQ(entry.type(), milvus::proto::schema::DataType::Int32);
    ASSERT_EQ(entry.scalars().int_data().data_size(), 0);
    ASSERT_EQ(entry.valid_data_size(), 0);

    // Field 8 (singular, backward compat): single-field path must populate it
    // via CopyFrom so legacy Go consumers still see the type metadata.
    ASSERT_TRUE(search_result->has_group_by_field_value());
    const auto& legacy = search_result->group_by_field_value();
    ASSERT_EQ(legacy.field_id(), int32_fid.get());
    ASSERT_EQ(legacy.type(), milvus::proto::schema::DataType::Int32);
    ASSERT_EQ(legacy.scalars().int_data().data_size(), 0);
}

TEST(GroupBY, AssembleEmptyResultMultiFieldPreservesMetadata) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str =
        handle.ParseCompositeGroupBySearch("",
                                           "fakevec",
                                           /*topk=*/10,
                                           "L2",
                                           "{\"ef\": 10}",
                                           {int32_fid.get(), int64_fid.get()},
                                           /*group_size=*/2);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    std::vector<CompositeGroupKey> empty_vals;  // 0 hits

    AssembleCompositeGroupByValues(search_result, empty_vals, plan.get());

    // Field 17 (plural): N entries, each with type + empty data
    ASSERT_EQ(search_result->group_by_field_values_size(), 2);

    const auto& e0 = search_result->group_by_field_values(0);
    ASSERT_EQ(e0.field_id(), int32_fid.get());
    ASSERT_EQ(e0.type(), milvus::proto::schema::DataType::Int32);
    ASSERT_EQ(e0.scalars().int_data().data_size(), 0);

    const auto& e1 = search_result->group_by_field_values(1);
    ASSERT_EQ(e1.field_id(), int64_fid.get());
    ASSERT_EQ(e1.type(), milvus::proto::schema::DataType::Int64);
    ASSERT_EQ(e1.scalars().long_data().data_size(), 0);

    // Field 8 (singular): only populated for size==1 path, must remain unset
    // for multi-field to avoid ambiguity with old single-field semantics.
    ASSERT_FALSE(search_result->has_group_by_field_value());
}

// --- AssembleCompositeGroupByValues non-empty / per-type assembly guards ---
//
// These tests lock the per-alternative serialization contract inside
// AssembleSingleFieldFromCompositeKeys: for each DataType case we verify
// (1) proto type is set correctly, (2) non-null values round-trip to the
// right scalar sub-message (int_data / long_data / bool_data / string_data /
// timestamptz_data), (3) null (std::nullopt) entries flip valid_data[i]=false
// and write the documented placeholder (0 / false / "").

namespace {

// Build a GroupByValueType holding alternative T. Two-step emplace avoids any
// variant overload-resolution ambiguity (e.g. bool vs int).
template <typename T>
GroupByValueType
MakeGroupByValue(T v) {
    GroupByValueType result;
    result.emplace();  // engage optional; variant default = monostate
    result.value().template emplace<T>(std::move(v));
    return result;
}

// Build a null GroupByValueType (disengaged optional).
GroupByValueType
MakeNullGroupByValue() {
    return GroupByValueType{};
}

// Wrap a list of GroupByValueType into a single-column CompositeGroupKey.
CompositeGroupKey
MakeSingleColKey(GroupByValueType v) {
    CompositeGroupKey key;
    key.Add(std::move(v));
    return key;
}

}  // namespace

TEST(GroupBY, AssembleCompositeInt8) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("i8", DataType::INT8);
    schema->set_primary_field_id(int8_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {int8_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int8_t>(7)));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int8_t>(-3)));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    ASSERT_EQ(search_result->group_by_field_values_size(), 1);
    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Int8);
    ASSERT_EQ(fd.scalars().int_data().data_size(), 3);
    EXPECT_EQ(fd.scalars().int_data().data(0), 7);
    EXPECT_EQ(fd.scalars().int_data().data(1), 0);  // null placeholder
    EXPECT_EQ(fd.scalars().int_data().data(2), -3);
    ASSERT_EQ(fd.valid_data_size(), 3);
    EXPECT_TRUE(fd.valid_data(0));
    EXPECT_FALSE(fd.valid_data(1));
    EXPECT_TRUE(fd.valid_data(2));
}

TEST(GroupBY, AssembleCompositeInt16) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int16_fid = schema->AddDebugField("i16", DataType::INT16);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {int16_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int16_t>(1234)));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int16_t>(-32000)));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    ASSERT_EQ(search_result->group_by_field_values_size(), 1);
    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Int16);
    ASSERT_EQ(fd.scalars().int_data().data_size(), 2);
    EXPECT_EQ(fd.scalars().int_data().data(0), 1234);
    EXPECT_EQ(fd.scalars().int_data().data(1), -32000);
}

TEST(GroupBY, AssembleCompositeInt32) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int32_fid = schema->AddDebugField("i32", DataType::INT32);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {int32_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int32_t>(100000)));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Int32);
    EXPECT_EQ(fd.scalars().int_data().data(0), 100000);
    EXPECT_TRUE(fd.valid_data(0));
    EXPECT_FALSE(fd.valid_data(1));
}

TEST(GroupBY, AssembleCompositeInt64) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("i64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {int64_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int64_t>(5000000000LL)));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int64_t>(-1LL)));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Int64);
    ASSERT_EQ(fd.scalars().long_data().data_size(), 3);
    EXPECT_EQ(fd.scalars().long_data().data(0), 5000000000LL);
    EXPECT_EQ(fd.scalars().long_data().data(1), -1LL);
    EXPECT_EQ(fd.scalars().long_data().data(2), 0LL);  // null placeholder
    EXPECT_FALSE(fd.valid_data(2));
}

TEST(GroupBY, AssembleCompositeBool) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto bool_fid = schema->AddDebugField("b", DataType::BOOL);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {bool_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<bool>(true)));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<bool>(false)));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Bool);
    ASSERT_EQ(fd.scalars().bool_data().data_size(), 3);
    EXPECT_TRUE(fd.scalars().bool_data().data(0));
    EXPECT_FALSE(fd.scalars().bool_data().data(1));
    EXPECT_FALSE(fd.scalars().bool_data().data(2));  // null placeholder
    EXPECT_TRUE(fd.valid_data(0));
    EXPECT_TRUE(fd.valid_data(1));
    EXPECT_FALSE(fd.valid_data(2));
}

TEST(GroupBY, AssembleCompositeVarchar) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto str_fid = schema->AddDebugField("s", DataType::VARCHAR);
    schema->set_primary_field_id(str_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {str_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<std::string>("hello")));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<std::string>("")));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<std::string>("üñîcødé")));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::VarChar);
    ASSERT_EQ(fd.scalars().string_data().data_size(), 4);
    EXPECT_EQ(fd.scalars().string_data().data(0), "hello");
    EXPECT_EQ(fd.scalars().string_data().data(1), "");
    EXPECT_EQ(fd.scalars().string_data().data(2), "üñîcødé");
    EXPECT_EQ(fd.scalars().string_data().data(3), "");  // null placeholder
    EXPECT_TRUE(fd.valid_data(0));
    EXPECT_TRUE(fd.valid_data(1));  // empty string is NOT null
    EXPECT_TRUE(fd.valid_data(2));
    EXPECT_FALSE(fd.valid_data(3));  // actual null
}

TEST(GroupBY, AssembleCompositeTimestamptz) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto ts_fid = schema->AddDebugField("ts", DataType::TIMESTAMPTZ);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {ts_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int64_t>(1700000000LL)));
    vals.push_back(MakeSingleColKey(MakeNullGroupByValue()));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    ASSERT_EQ(fd.type(), milvus::proto::schema::DataType::Timestamptz);
    ASSERT_EQ(fd.scalars().timestamptz_data().data_size(), 2);
    EXPECT_EQ(fd.scalars().timestamptz_data().data(0), 1700000000LL);
    EXPECT_EQ(fd.scalars().timestamptz_data().data(1), 0LL);
    EXPECT_TRUE(fd.valid_data(0));
    EXPECT_FALSE(fd.valid_data(1));
}

TEST(GroupBY, AssembleCompositeMultiFieldValues) {
    // 3-field composite: INT32 + VARCHAR + BOOL.  Verifies (a) field 17 has 3
    // FieldData entries in OwnFieldIDs order, (b) field_id/name metadata aligns
    // with schema, (c) each column's scalars use the right proto sub-message.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("c_int", DataType::INT32);
    auto str_fid = schema->AddDebugField("c_str", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("c_bool", DataType::BOOL);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "",
        "fakevec",
        10,
        "L2",
        "{\"ef\": 10}",
        {i32_fid.get(), str_fid.get(), bool_fid.get()},
        3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    {
        CompositeGroupKey k;
        k.Add(MakeGroupByValue<int32_t>(11));
        k.Add(MakeGroupByValue<std::string>("a"));
        k.Add(MakeGroupByValue<bool>(true));
        vals.push_back(std::move(k));
    }
    {
        CompositeGroupKey k;
        k.Add(MakeGroupByValue<int32_t>(22));
        k.Add(MakeGroupByValue<std::string>("b"));
        k.Add(MakeGroupByValue<bool>(false));
        vals.push_back(std::move(k));
    }

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    ASSERT_EQ(search_result->group_by_field_values_size(), 3);

    const auto& e0 = search_result->group_by_field_values(0);
    EXPECT_EQ(e0.field_id(), i32_fid.get());
    EXPECT_EQ(e0.field_name(), "c_int");
    EXPECT_EQ(e0.type(), milvus::proto::schema::DataType::Int32);
    EXPECT_EQ(e0.scalars().int_data().data(0), 11);
    EXPECT_EQ(e0.scalars().int_data().data(1), 22);

    const auto& e1 = search_result->group_by_field_values(1);
    EXPECT_EQ(e1.field_id(), str_fid.get());
    EXPECT_EQ(e1.field_name(), "c_str");
    EXPECT_EQ(e1.type(), milvus::proto::schema::DataType::VarChar);
    EXPECT_EQ(e1.scalars().string_data().data(0), "a");
    EXPECT_EQ(e1.scalars().string_data().data(1), "b");

    const auto& e2 = search_result->group_by_field_values(2);
    EXPECT_EQ(e2.field_id(), bool_fid.get());
    EXPECT_EQ(e2.field_name(), "c_bool");
    EXPECT_EQ(e2.type(), milvus::proto::schema::DataType::Bool);
    EXPECT_TRUE(e2.scalars().bool_data().data(0));
    EXPECT_FALSE(e2.scalars().bool_data().data(1));

    // Multi-field: field 8 must stay empty to avoid confusing legacy single-
    // field consumers.
    EXPECT_FALSE(search_result->has_group_by_field_value());
}

TEST(GroupBY, AssembleCompositeSingleFieldBackwardCompatCopy) {
    // Single-field + non-empty vals: field 8 CopyFrom must include scalars +
    // valid_data, not just type metadata.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("i32", DataType::INT32);
    schema->set_primary_field_id(i32_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {i32_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int32_t>(777)));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int32_t>(888)));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    ASSERT_TRUE(search_result->has_group_by_field_value());
    const auto& legacy = search_result->group_by_field_value();
    EXPECT_EQ(legacy.field_id(), i32_fid.get());
    EXPECT_EQ(legacy.type(), milvus::proto::schema::DataType::Int32);
    ASSERT_EQ(legacy.scalars().int_data().data_size(), 2);
    EXPECT_EQ(legacy.scalars().int_data().data(0), 777);
    EXPECT_EQ(legacy.scalars().int_data().data(1), 888);
    ASSERT_EQ(legacy.valid_data_size(), 2);
    EXPECT_TRUE(legacy.valid_data(0));
    EXPECT_TRUE(legacy.valid_data(1));
}

TEST(GroupBY, AssembleCompositeJsonFallbackVarchar) {
    // JSON field with json_type unset → effective_type falls back to VARCHAR,
    // so values must be stored as std::string alternative in the variant.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto json_fid = schema->AddDebugField("j", DataType::JSON);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "",
        "fakevec",
        10,
        "L2",
        "{\"ef\": 10}",
        {json_fid.get()},
        /*group_size=*/3,
        /*strict_group_size=*/false,
        /*json_path=*/"/cat",
        /*json_type=*/milvus::proto::schema::DataType::None);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<std::string>("x")));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<std::string>("y")));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    EXPECT_EQ(fd.type(), milvus::proto::schema::DataType::VarChar);
    EXPECT_EQ(fd.scalars().string_data().data(0), "x");
    EXPECT_EQ(fd.scalars().string_data().data(1), "y");
}

TEST(GroupBY, AssembleCompositeJsonExplicitInt64) {
    // JSON field with json_type=Int64 → variant must hold int64_t alternative.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto json_fid = schema->AddDebugField("j", DataType::JSON);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "",
        "fakevec",
        10,
        "L2",
        "{\"ef\": 10}",
        {json_fid.get()},
        /*group_size=*/3,
        /*strict_group_size=*/false,
        /*json_path=*/"/n",
        /*json_type=*/milvus::proto::schema::DataType::Int64);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int64_t>(42LL)));
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int64_t>(-7LL)));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    AssembleCompositeGroupByValues(search_result, vals, plan.get());

    const auto& fd = search_result->group_by_field_values(0);
    EXPECT_EQ(fd.type(), milvus::proto::schema::DataType::Int64);
    ASSERT_EQ(fd.scalars().long_data().data_size(), 2);
    EXPECT_EQ(fd.scalars().long_data().data(0), 42LL);
    EXPECT_EQ(fd.scalars().long_data().data(1), -7LL);
}

TEST(GroupBY, AssembleCompositeKeySizeMismatchTriggersAssert) {
    // Column-count invariant guard: a CompositeGroupKey with fewer columns than
    // group_by_field_ids.size() must fail loud via AssertInfo, not via UB in
    // the per-case operator[] read.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto a_fid = schema->AddDebugField("a", DataType::INT32);
    auto b_fid = schema->AddDebugField("b", DataType::INT32);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseCompositeGroupBySearch(
        "", "fakevec", 10, "L2", "{\"ef\": 10}", {a_fid.get(), b_fid.get()}, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    // Intentionally build a 1-column key when plan expects 2 columns.
    std::vector<CompositeGroupKey> vals;
    vals.push_back(MakeSingleColKey(MakeGroupByValue<int32_t>(1)));

    auto search_result =
        std::make_unique<milvus::proto::schema::SearchResultData>();
    EXPECT_THROW(
        AssembleCompositeGroupByValues(search_result, vals, plan.get()),
        std::exception);
}
