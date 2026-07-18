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
#include "exec/Task.h"
#include "exec/operator/SearchGroupByNode.h"
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
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/plan_c.h"
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

class FixedVectorIterator : public VectorIterator {
 public:
    explicit FixedVectorIterator(std::vector<std::pair<int64_t, float>> results)
        : results_(std::move(results)) {
    }

    bool
    HasNext() override {
        return pos_ < results_.size();
    }

    std::optional<std::pair<int64_t, float>>
    Next() override {
        if (!HasNext()) {
            return std::nullopt;
        }
        return results_[pos_++];
    }

 private:
    std::vector<std::pair<int64_t, float>> results_;
    size_t pos_{0};
};

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

        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        ASSERT_EQ(20, group_by_values.size());
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        int size = group_by_values.size();
        std::unordered_map<int8_t, int> i8_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int8_t>(group_by_values[i].value())) {
                int8_t g_val = std::get<int8_t>(group_by_values[i].value());
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

        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int16_t, int> i16_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int16_t>(group_by_values[i].value())) {
                int16_t g_val = std::get<int16_t>(group_by_values[i].value());
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

        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int32_t, int> i32_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int32_t>(group_by_values[i].value())) {
                int16_t g_val = std::get<int32_t>(group_by_values[i].value());
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
        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int64_t, int> i64_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int64_t>(group_by_values[i].value())) {
                int16_t g_val = std::get<int64_t>(group_by_values[i].value());
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
        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        ASSERT_EQ(20, group_by_values.size());
        int size = group_by_values.size();

        std::unordered_map<std::string, int> strs_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<std::string>(
                    group_by_values[i].value())) {
                std::string g_val = std::move(
                    std::get<std::string>(group_by_values[i].value()));
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

        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        ASSERT_EQ(size, 6);
        //as there are only two possible values: true, false
        //for each group, there are at most 3 items, so the final size of group_by_vals is 3 * 2 = 6

        std::unordered_map<bool, int> bools_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<bool>(group_by_values[i].value())) {
                bool g_val = std::get<bool>(group_by_values[i].value());
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
        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        ASSERT_EQ(20, size);
        //as the total data is 0,0,....6,6, so there will be 7 buckets with [3,3,3,3,3,3,2] items respectively
        //so there will be 20 items returned

        std::unordered_map<int64_t, int> timestamptz_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (std::holds_alternative<int64_t>(group_by_values[i].value())) {
                int16_t g_val = std::get<int64_t>(group_by_values[i].value());
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

        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        int size = group_by_values.size();
        // groups are: (0, 1, 2, 3, 4, null), counts are: (10, 10, 10, 10 ,10, 50)
        ASSERT_EQ(30, size);

        std::unordered_map<int8_t, int> i8_map;
        float lastDistance = 0.0;
        for (size_t i = 0; i < size; i++) {
            if (group_by_values[i].has_value()) {
                int8_t g_val = std::get<int8_t>(group_by_values[i].value());
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

TEST(GroupBY, ElementLevelKeepsElementIndices) {
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    size_t N = 3;
    int array_len = 3;
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    auto array_offsets = growing->GetArrayOffsets(vec_fid);
    ASSERT_NE(array_offsets, nullptr);

    SearchInfo search_info;
    search_info.topk_ = 10;
    search_info.group_size_ = 2;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.group_by_field_ids_.push_back(pk_fid);
    search_info.array_offsets_ = array_offsets;

    // Element IDs map to rows as:
    // row 0: element IDs 0, 1, 2
    // row 1: element IDs 3, 4, 5
    // row 2: element IDs 6, 7, 8
    // group_size=2 should keep only two hits from row 0 while preserving
    // element indices for all accepted hits.
    std::vector<std::shared_ptr<VectorIterator>> iterators{
        std::make_shared<FixedVectorIterator>(
            std::vector<std::pair<int64_t, float>>{
                {0, 0.1F},
                {1, 0.2F},
                {2, 0.3F},
                {3, 0.4F},
                {4, 0.5F},
                {6, 0.6F},
            })};

    OpContext op_context;
    std::vector<CompositeGroupKey> group_by_values;
    std::vector<int64_t> seg_offsets;
    std::vector<float> distances;
    std::vector<size_t> topk_per_nq_prefix_sum;
    std::vector<int32_t> element_indices;

    SearchGroupBy(&op_context,
                  iterators,
                  search_info,
                  group_by_values,
                  *growing,
                  seg_offsets,
                  distances,
                  topk_per_nq_prefix_sum,
                  &element_indices);

    ASSERT_EQ(seg_offsets, (std::vector<int64_t>{0, 0, 1, 1, 2}));
    ASSERT_EQ(element_indices, (std::vector<int32_t>{0, 1, 0, 1, 0}));
    ASSERT_EQ(topk_per_nq_prefix_sum, (std::vector<size_t>{0, 5}));
    ASSERT_EQ(group_by_values.size(), seg_offsets.size());
    ASSERT_EQ(distances, (std::vector<float>{0.1F, 0.2F, 0.4F, 0.5F, 0.6F}));
}

TEST(GroupBY, SearchGroupByNodeKeepsElementIndices) {
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    size_t N = 3;
    int array_len = 3;
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    auto array_offsets = growing->GetArrayOffsets(vec_fid);
    ASSERT_NE(array_offsets, nullptr);

    SearchInfo search_info;
    search_info.topk_ = 10;
    search_info.group_size_ = 2;
    search_info.metric_type_ = knowhere::metric::L2;
    search_info.group_by_field_ids_.push_back(pk_fid);
    search_info.array_offsets_ = array_offsets;

    SearchResult search_result;
    search_result.total_nq_ = 1;
    search_result.unity_topK_ = search_info.topk_;
    search_result.element_level_ = true;
    search_result.vector_iterators_ =
        std::vector<std::shared_ptr<VectorIterator>>{
            std::make_shared<FixedVectorIterator>(
                std::vector<std::pair<int64_t, float>>{
                    {0, 0.1F},
                    {1, 0.2F},
                    {2, 0.3F},
                    {3, 0.4F},
                    {4, 0.5F},
                    {6, 0.6F},
                })};

    OpContext op_context;
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        "search_group_by_node_element_level",
        growing,
        N,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<milvus::exec::QueryConfig>(
            std::unordered_map<std::string, std::string>{}));
    query_context->set_search_info(search_info);
    query_context->set_array_offsets(array_offsets);
    query_context->set_search_result(std::move(search_result));
    query_context->set_op_context(&op_context);

    auto group_by_plan =
        std::make_shared<milvus::plan::SearchGroupByNode>("group_by");
    auto task =
        milvus::exec::Task::Create("search_group_by_node_element_level",
                                   milvus::plan::PlanFragment(group_by_plan),
                                   0,
                                   query_context);
    milvus::exec::DriverContext driver_context(task, 0, 0, 0, 0);
    milvus::exec::PhySearchGroupByNode node(1, &driver_context, group_by_plan);

    auto input = std::make_shared<RowVector>(std::vector<VectorPtr>{});
    node.AddInput(input);
    node.NoMoreInput();
    auto output = node.GetOutput();
    ASSERT_NE(output, nullptr);

    auto grouped = query_context->get_search_result();
    ASSERT_TRUE(grouped.element_level_);
    ASSERT_EQ(grouped.seg_offsets_, (std::vector<int64_t>{0, 0, 1, 1, 2}));
    ASSERT_EQ(grouped.element_indices_, (std::vector<int32_t>{0, 1, 0, 1, 0}));
    ASSERT_EQ(grouped.topk_per_nq_prefix_sum_, (std::vector<size_t>{0, 5}));
    ASSERT_TRUE(grouped.composite_group_by_values_.has_value());
    ASSERT_EQ(grouped.composite_group_by_values_->size(),
              grouped.seg_offsets_.size());
    ASSERT_TRUE(grouped.group_size_.has_value());
    ASSERT_EQ(grouped.group_size_.value(), 2);
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

    auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
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
            if (std::holds_alternative<int32_t>(group_by_values[idx].value())) {
                int32_t g_val = std::get<int32_t>(group_by_values[idx].value());
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

    auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
    auto size = group_by_values.size();
    int expected_group_count = 100;
    ASSERT_EQ(size, expected_group_count * group_size * num_queries);
    int idx = 0;
    for (int i = 0; i < num_queries; i++) {
        std::unordered_map<int32_t, int> i32_map;
        float lastDistance = 0.0;
        for (int j = 0; j < expected_group_count * group_size; j++) {
            if (std::holds_alternative<int32_t>(group_by_values[idx].value())) {
                int32_t g_val = std::get<int32_t>(group_by_values[idx].value());
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

// Group-by on a NULLABLE scalar field must not crash and must preserve NULL
// rows as a distinct null group. Guards SealedDataGetter's null handling; the
// index-only Reverse_Lookup==nullopt branch (which previously threw
// "field data not found") is additionally exercised on CI.
TEST(GroupBY, SealedNullableGroupByField) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto str_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto i64_null_fid =
        schema->AddDebugField("int64_null", DataType::INT64, true);
    schema->set_primary_field_id(str_fid);
    size_t N = 100;

    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, 1, false, false);
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

    ScopedSchemaHandle handle(*schema);
    auto plan_str = handle.ParseGroupBySearch(
        "", "fakevec", 20, "L2", "{\"ef\": 10}", i64_null_fid.get(), 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    ASSERT_NO_THROW({
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
        ASSERT_NE(search_result, nullptr);
        auto group_by_values = ExtractFirstFieldGroupByValues(*search_result);
        ASSERT_FALSE(group_by_values.empty());
    });
}
