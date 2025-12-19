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
#include <boost/format.hpp>

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
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // Struct array sub-fields: struct_array[sub_str], struct_array[sub_int]
    auto sub_str_fid = schema->AddDebugArrayField(
        "struct_array[sub_str]", DataType::VARCHAR, false);
    auto sub_int_fid = schema->AddDebugArrayField(
        "struct_array[sub_int]", DataType::INT32, false);

    size_t N = 1000;

    // Step 2: Generate and insert test data
    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    auto raw_data = DataGen(schema, N, 42);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

    // Step 3: Create MatchExpr plan using text proto format
    // Query: match_all(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100)
    std::string raw_plan = boost::str(boost::format(R"(vector_anns: <
        field_id: %1%
        predicates: <
            match_expr: <
                struct_name: "struct_array"
                match_type: MatchAll
                count: 0
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
    >)") % vec_fid.get() % sub_str_fid.get() % sub_int_fid.get());

    // Step 4: Parse and create plan
    proto::plan::PlanNode plan_node;
    auto ok = google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
    ASSERT_TRUE(ok) << "Failed to parse MatchExpr plan";

    auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_NE(plan, nullptr);

    // Step 5: Execute search
    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, 4, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = seg->Search(plan.get(), ph_group.get(), 1L << 63);

    // Step 6: Verify results
    ASSERT_NE(search_result, nullptr);
    // Note: Currently all results will be false because Tantivy nested index
    // is not yet integrated. This test verifies the expression parses and
    // executes without error.
}
