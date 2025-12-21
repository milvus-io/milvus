// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License

#include <gtest/gtest.h>
#include <boost/format.hpp>
#include <iostream>
#include <random>

#include "common/Schema.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

TEST(MatchExprTest, MatchAllGrowing) {
    // Step 1: Create schema with struct array sub-fields
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("vec", DataType::VECTOR_FLOAT, 4,
                                         knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // Struct array sub-fields: struct_array[sub_str], struct_array[sub_int]
    auto sub_str_fid = schema->AddDebugArrayField("struct_array[sub_str]",
                                                  DataType::VARCHAR, false);
    auto sub_int_fid = schema->AddDebugArrayField("struct_array[sub_int]",
                                                  DataType::INT32, false);

    size_t N = 1000;
    int array_len = 5;
    std::default_random_engine rng(42);
    std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
    std::uniform_int_distribution<> str_dist(0, 2);
    std::uniform_int_distribution<> int_dist(50, 150);

    // Step 2: Generate custom test data
    auto insert_data = std::make_unique<InsertRecordProto>();

    // Generate vector field
    std::vector<float> vec_data(N * 4);
    std::normal_distribution<float> vec_dist(0, 1);
    for (auto& v : vec_data) {
        v = vec_dist(rng);
    }
    auto vec_array = CreateDataArrayFrom(vec_data.data(), nullptr, N,
                                         schema->operator[](vec_fid));
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

    // Generate id field
    std::vector<int64_t> id_data(N);
    for (size_t i = 0; i < N; ++i) {
        id_data[i] = i;
    }
    auto id_array = CreateDataArrayFrom(id_data.data(), nullptr, N,
                                        schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    // Generate struct_array[sub_str]: equal probability "aaa", "bbb", "ccc"
    std::vector<milvus::proto::schema::ScalarField> sub_str_data(N);
    for (size_t i = 0; i < N; ++i) {
        for (int j = 0; j < array_len; ++j) {
            sub_str_data[i].mutable_string_data()->add_data(
                str_choices[str_dist(rng)]);
        }
    }
    auto sub_str_array = CreateDataArrayFrom(sub_str_data.data(), nullptr, N,
                                             schema->operator[](sub_str_fid));
    insert_data->mutable_fields_data()->AddAllocated(sub_str_array.release());

    // Generate struct_array[sub_int]: random 50-150
    std::vector<milvus::proto::schema::ScalarField> sub_int_data(N);
    for (size_t i = 0; i < N; ++i) {
        for (int j = 0; j < array_len; ++j) {
            sub_int_data[i].mutable_int_data()->add_data(int_dist(rng));
        }
    }
    auto sub_int_array = CreateDataArrayFrom(sub_int_data.data(), nullptr, N,
                                             schema->operator[](sub_int_fid));
    insert_data->mutable_fields_data()->AddAllocated(sub_int_array.release());

    insert_data->set_num_rows(N);

    // Generate row_ids and timestamps
    std::vector<idx_t> row_ids(N);
    std::vector<Timestamp> timestamps(N);
    for (size_t i = 0; i < N; ++i) {
        row_ids[i] = i;
        timestamps[i] = i;
    }

    // Insert data
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    seg->PreInsert(N);
    seg->Insert(0, N, row_ids.data(), timestamps.data(), insert_data.get());

    // Step 3: Create MatchExpr plan using text proto format
    // Query: match_all(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100)
    std::string raw_plan = boost::str(boost::format(R"(vector_anns: <
        field_id: %1%
        predicates: <
            match_expr: <
                struct_name: "struct_array"
                match_type: MatchLeast
                count: 3
                predicate: <
                    binary_expr: <
                        op: LogicalAnd
                        left: <
                            unary_range_expr: <
                                column_info: <
                                    field_id: %2%
                                    data_type: Array
                                    element_type: VarChar
                                    nested_path: "sub_str"
                                    is_element_level: true
                                >
                                op: Equal
                                value: <
                                    string_val: "aaa"
                                >
                            >
                        >
                        right: <
                            unary_range_expr: <
                                column_info: <
                                    field_id: %3%
                                    data_type: Array
                                    element_type: Int32
                                    nested_path: "sub_int"
                                    is_element_level: true
                                >
                                op: GreaterThan
                                value: <
                                    int64_val: 100
                                >
                            >
                        >
                    >
                >
            >
        >
        query_info: <
            topk: 10
            round_decimal: 3
            metric_type: "L2"
            search_params: "{\"nprobe\": 10}"
        >
        placeholder_tag: "$0"
    >)") % vec_fid.get() % sub_str_fid.get() %
                                      sub_int_fid.get());

    // Step 4: Parse and create plan
    proto::plan::PlanNode plan_node;
    auto ok =
        google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok) << "Failed to parse MatchExpr plan";

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Step 5: Execute search
    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 4, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = seg->Search(plan.get(), ph_group.get(), 1L << 63);

    // Print search results and verify MatchLeast condition
    std::cout << "=== Search Results ===" << std::endl;
    std::cout << "total_nq: " << search_result->total_nq_ << std::endl;
    std::cout << "unity_topK: " << search_result->unity_topK_ << std::endl;
    std::cout << "num_results: " << search_result->seg_offsets_.size()
              << std::endl;

    // Helper to count matching elements: sub_str == "aaa" && sub_int > 100
    auto count_matching_elements = [&](int64_t row_idx) -> int {
        int count = 0;
        const auto& str_field = sub_str_data[row_idx];
        const auto& int_field = sub_int_data[row_idx];
        for (int j = 0; j < array_len; ++j) {
            bool str_match = (str_field.string_data().data(j) == "aaa");
            bool int_match = (int_field.int_data().data(j) > 100);
            if (str_match && int_match) {
                ++count;
            }
        }
        return count;
    };

    for (int64_t i = 0; i < search_result->total_nq_; ++i) {
        std::cout << "Query " << i << ":" << std::endl;
        for (int64_t k = 0; k < search_result->unity_topK_; ++k) {
            int64_t idx = i * search_result->unity_topK_ + k;
            auto offset = search_result->seg_offsets_[idx];
            auto distance = search_result->distances_[idx];
            std::cout << "  [" << k << "] offset=" << offset
                      << ", distance=" << distance;

            if (offset >= 0 && offset < static_cast<int64_t>(N)) {
                // Print sub_str array
                std::cout << ", sub_str=[";
                const auto& str_field = sub_str_data[offset];
                for (int j = 0; j < str_field.string_data().data_size(); ++j) {
                    if (j > 0)
                        std::cout << ",";
                    std::cout << str_field.string_data().data(j);
                }
                std::cout << "]";

                // Print sub_int array
                std::cout << ", sub_int=[";
                const auto& int_field = sub_int_data[offset];
                for (int j = 0; j < int_field.int_data().data_size(); ++j) {
                    if (j > 0)
                        std::cout << ",";
                    std::cout << int_field.int_data().data(j);
                }
                std::cout << "]";

                // Verify MatchLeast(3) condition
                int match_count = count_matching_elements(offset);
                std::cout << ", match_count=" << match_count;
                EXPECT_GE(match_count, 3)
                    << "Row " << offset
                    << " should have at least 3 elements matching "
                    << "(sub_str == 'aaa' && sub_int > 100)";
            }
            std::cout << std::endl;
        }
    }
    std::cout << "======================" << std::endl;
}
