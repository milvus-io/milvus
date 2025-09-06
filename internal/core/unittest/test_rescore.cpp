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
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(Rescorer, Normal) {
    //0. prepare schema
    int dim = 64;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto str_fid = schema->AddDebugField("string", DataType::VARCHAR);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    schema->set_primary_field_id(str_fid);
    size_t N = 50;

    //2. load raw data
    auto raw_data = DataGen(schema, N, 42, 0, 8, 10, false, false);
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
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);
    int topK = 10;
    int group_size = 3;

    // no result after search
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
                                                int64_val: 100
                                            >
                                            upper_value: <
                                                int64_val: -1
                                            >
                                        >
                                    >
                                    query_info: <
                                        topk: 10
                                        metric_type: "L2"
                                        search_params: "{\"ef\": 50}"
                                    >
                                    placeholder_tag: "$0"
                                >
                                scorers: <
                                    weight: 4
                                >)";

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // search result not empty but no boost filter
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
                                        search_params: "{\"ef\": 50}"
                                    >
                                    placeholder_tag: "$0"
                                >
                                scorers: <
                                    weight: 4
                                >)";

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // random function with seed
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
                                        search_params: "{\"ef\": 50}"
                                    >
                                    placeholder_tag: "$0"
                                >
                                scorers: <
                                    type: 1
                                    weight: 1
                                    seed: 123
                                >)";

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // random function with field as random seed
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
                                        search_params: "{\"ef\": 50}"
                                    >
                                    placeholder_tag: "$0"
                                >
                                scorers: <
                                    type: 1
                                    weight: 1
                                    field: "int64"
                                >)";

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // random function with field and seed
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
                                        search_params: "{\"ef\": 50}"
                                    >
                                    placeholder_tag: "$0"
                                >
                                scorers: <
                                    type: 1
                                    filter: <
                                    >
                                    weight: 1
                                    seed: 123
                                >)";

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);
    }
}