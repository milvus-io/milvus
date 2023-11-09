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
#include <cstdint>
#include <memory>
#include <regex>
#include <vector>
#include <chrono>

#include "common/Types.h"
#include "pb/plan.pb.h"
#include "query/Expr.h"
#include "query/ExprImpl.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/generated/ExecExprVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "simdjson/padded_string.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"

TEST(Expr, TestArrayRange) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val < 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              lower_inclusive: false,
              upper_inclusive: false,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val < 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              lower_inclusive: true,
              upper_inclusive: false,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val < 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              lower_inclusive: true,
              upper_inclusive: false,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val < 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              lower_inclusive: false,
              upper_inclusive: true,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 < val && val <= 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              lower_inclusive: false,
              upper_inclusive: true,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 < val && val <= 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return 1 <= val && val <= 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                int64_val: 1
              >
              upper_value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return 1 <= val && val <= 10000;
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:VarChar
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                string_val: "aaa"
              >
              upper_value: <
                string_val: "zzz"
              >
        >)",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return "aaa" <= val && val <= "zzz";
             }},
            {R"(binary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
              lower_inclusive: true,
              upper_inclusive: true,
              lower_value: <
                float_val: 1.1
              >
              upper_value: <
                float_val: 2048.12
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return 1.1 <= val && val <= 2048.12;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: GreaterEqual,
              value: <
                int64_val: 10000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val >= 10000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: GreaterThan,
              value: <
                int64_val: 2000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val > 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: LessEqual,
              value: <
                int64_val: 2000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val <= 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: LessThan,
              value: <
                int64_val: 2000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val < 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: Equal,
              value: <
                int64_val: 2000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val == 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              op: NotEqual,
              value: <
                int64_val: 2000
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val != 2000;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"0"
                element_type:Bool
              >
              op: Equal,
              value: <
                bool_val: false
              >
        >)",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:VarChar
              >
              op: Equal,
              value: <
                string_val: "abc"
              >
        >)",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc";
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
              op: Equal,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: Equal,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val == 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: NotEqual,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val != 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: GreaterEqual,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val >= 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: GreaterThan,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val > 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: LessEqual,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val <= 2.2;
             }},
            {R"(unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              op: LessThan,
              value: <
                float_val: 2.2
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val < 2.2;
             }},

        };
    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto float_array_fid =
        schema->AddDebugField("double_array", DataType::ARRAY, DataType::FLOAT);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        auto new_bool_array_col = raw_data.get_col<ScalarArray>(bool_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarArray>(string_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarArray>(float_array_fid);

        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, array_type, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, TestArrayEqual) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<
        std::tuple<std::string, std::function<bool(std::vector<int64_t>)>>>
        testcases = {
            {R"(unary_range_expr: <
                column_info: <
                    field_id: 102
                    data_type: Array
                    element_type:Int64
                  >
                op:Equal
                value:<
                    array_val:<array:<int64_val:1 > array:<int64_val:2 > array:<int64_val:3 >
                        same_type:true
                        element_type:Int64
                    >>
        >)",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return false;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return false;
                     }
                 }
                 return true;
             }},
            {R"(unary_range_expr: <
                column_info: <
                  field_id: 102
                  data_type: Array
                  element_type:Int64
                >
                op:NotEqual
                value:<array_val:<array:<int64_val:1 > array:<int64_val:2 > array:<int64_val:3 >
                    same_type:true
                    element_type:Int64
                >>
        >)",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return true;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return true;
                     }
                 }
                 return false;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarArray> long_array_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);
    for (auto [clause, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(long_array_col[i]);
            std::vector<int64_t> array_values(array.length());
            for (int j = 0; j < array.length(); ++j) {
                array_values.push_back(array.get_data<int64_t>(j));
            }
            auto ref = ref_func(array_values);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, PraseArrayContainsExpr) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    std::vector<const char*> raw_plans{
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:Array
                        element_type:Int64
                    >
                    elements:<int64_val:1 >
                    op:Contains
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:Array
                        element_type:Int64
                    >
                    elements:<int64_val:1 > elements:<int64_val:2 > elements:<int64_val:3 >
                    op:ContainsAll
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
        R"(vector_anns:<
            field_id:100
            predicates:<
                json_contains_expr:<
                    column_info:<
                        field_id:101
                        data_type:Array
                        element_type:Int64
                    >
                    elements:<int64_val:1 > elements:<int64_val:2 > elements:<int64_val:3 >
                    op:ContainsAny
                    elements_same_type:true
                >
            >
            query_info:<
                topk: 10
                round_decimal: 3
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            > placeholder_tag:"$0"
        >)",
    };

    for (auto& raw_plan : raw_plans) {
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto schema = std::make_shared<Schema>();
        schema->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        schema->AddField(
            FieldName("array"), FieldId(101), DataType::ARRAY, DataType::INT64);
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    }
}

template <typename T>
struct ArrayTestcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
};

TEST(Expr, TestArrayContains) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col = raw_data.get_col<ScalarArray>(int_array_fid);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        auto new_bool_array_col = raw_data.get_col<ScalarArray>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarArray>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarArray>(double_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarArray>(string_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

    std::vector<ArrayTestcase<bool>> bool_testcases{{{true, true}, {}},
                                               {{false, false}, {}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<bool>>(
            ColumnInfo(bool_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            proto::plan::GenericValue::ValCase::kBoolVal);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["bool"][i]);
            std::vector<bool> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<bool>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<ArrayTestcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<double>>(
            ColumnInfo(double_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            proto::plan::GenericValue::ValCase::kFloatVal);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["double"][i]);
            std::vector<double> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<double>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<float>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<double>>(
            ColumnInfo(float_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            proto::plan::GenericValue::ValCase::kFloatVal);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["float"][i]);
            std::vector<float> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<float>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<ArrayTestcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<int64_t>>(
            ColumnInfo(int_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            proto::plan::GenericValue::ValCase::kInt64Val);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["int"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<int64_t>>(
            ColumnInfo(long_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            proto::plan::GenericValue::ValCase::kInt64Val);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }

    std::vector<ArrayTestcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<JsonContainsExprImpl<std::string>>(
            ColumnInfo(string_array_fid, DataType::ARRAY),
            testcase.term,
            true,
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            proto::plan::GenericValue::ValCase::kStringVal);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data<std::string_view>(j));
            }
            ASSERT_EQ(ans, check(res));
        }
    }
}

TEST(Expr, TestArrayBinaryArith) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col = raw_data.get_col<ScalarArray>(int_array_fid);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarArray>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarArray>(double_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int8
              >
              arith_op:Add
              right_operand:<int64_val:2 >
              op:Equal
              value:<int64_val:5 >
        >)",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 == 5;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int8
              >
              arith_op:Add
              right_operand:<int64_val:2 >
              op:NotEqual
              value:<int64_val:5 >
        >)",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val + 2 != 5;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Sub
              right_operand:<int64_val:1 >
              op:Equal
              value:<int64_val:144 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 == 144;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Sub
              right_operand:<int64_val:1 >
              op:NotEqual
              value:<int64_val:144 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val - 1 != 144;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
              arith_op:Add
              right_operand:<float_val:2.2 >
              op:Equal
              value:<float_val:133.2 >
        >)",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 == 133.2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
              arith_op:Add
              right_operand:<float_val:2.2 >
              op:NotEqual
              value:<float_val:133.2 >
        >)",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val + 2.2 != 133.2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:Double
              >
              arith_op:Sub
              right_operand:<float_val:11.1 >
              op:Equal
              value:<float_val:125.7 >
        >)",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 == 125.7;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:Double
              >
              arith_op:Sub
              right_operand:<float_val:11.1 >
              op:NotEqual
              value:<float_val:125.7 >
        >)",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val - 11.1 != 125.7;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Mul
              right_operand:<int64_val:2 >
              op:Equal
              value:<int64_val:8 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 == 8;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Mul
              right_operand:<int64_val:2 >
              op:NotEqual
              value:<int64_val:20 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val * 2 != 20;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Div
              right_operand:<int64_val:2 >
              op:Equal
              value:<int64_val:8 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 == 8;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Div
              right_operand:<int64_val:2 >
              op:NotEqual
              value:<int64_val:20 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val / 2 != 20;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Mod
              right_operand:<int64_val:3 >
              op:Equal
              value:<int64_val:0 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 == 0;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              arith_op:Mod
              right_operand:<int64_val:3 >
              op:NotEqual
              value:<int64_val:2 >
        >)",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data<int64_t>(0);
                 return val % 3 != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              arith_op:Add
              right_operand:<float_val:2.2 >
              op:Equal
              value:<float_val:133.2 >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 == 133.2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"1024"
                element_type:Float
              >
              arith_op:Add
              right_operand:<float_val:2.2 >
              op:NotEqual
              value:<float_val:133.2 >
        >)",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val + 2.2 != 133.2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"1024"
                element_type:Double
              >
              arith_op:Sub
              right_operand:<float_val:11.1 >
              op:Equal
              value:<float_val:125.7 >
        >)",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 == 125.7;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"1024"
                element_type:Double
              >
              arith_op:Sub
              right_operand:<float_val:11.1 >
              op:NotEqual
              value:<float_val:125.7 >
        >)",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<double>(1024);
                 return val - 11.1 != 125.7;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Mul
              right_operand:<int64_val:2 >
              op:Equal
              value:<int64_val:8 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 == 8;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Mul
              right_operand:<int64_val:2 >
              op:NotEqual
              value:<int64_val:20 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val * 2 != 20;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Div
              right_operand:<int64_val:2 >
              op:Equal
              value:<int64_val:8 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 == 8;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Div
              right_operand:<int64_val:2 >
              op:NotEqual
              value:<int64_val:20 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val / 2 != 20;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Mod
              right_operand:<int64_val:3 >
              op:Equal
              value:<int64_val:0 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 == 0;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"1024"
                element_type:Int64
              >
              arith_op:Mod
              right_operand:<int64_val:3 >
              op:NotEqual
              value:<int64_val:2 >
        >)",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<int64_t>(1024);
                 return val % 3 != 2;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int8
              >
              arith_op:ArrayLength
              op:Equal
              value:<int64_val:10 >
        >)",
             "int",
             [](milvus::Array& array) {
                 return array.length() == 10;
             }},
            {R"(binary_arith_op_eval_range_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int8
              >
              arith_op:ArrayLength
              op:NotEqual
              value:<int64_val:8 >
        >)",
             "int",
             [](milvus::Array& array) {
                 return array.length() != 8;
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";
    for (auto [clause, array_type, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
        }
    }
}

template <typename T>
struct UnaryRangeTestcase {
    milvus::OpType op_type;
    T value;
    std::vector<std::string> nested_path;
    std::function<bool(milvus::Array&)> check_func;
};

TEST(Expr, TestArrayStringMatch) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_string_array_col =
            raw_data.get_col<ScalarArray>(string_array_fid);
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

    std::vector<UnaryRangeTestcase<std::string>> prefix_testcases{
        {OpType::PrefixMatch,
         "abc",
         {"0"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(0), "abc");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data<std::string_view>(1), "def");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1024"},
         [](milvus::Array& array) {
             if (array.length() <= 1024) {
                 return false;
             }
             return PrefixMatch(array.get_data<std::string_view>(1024), "def");
         }},
    };
    //vector_anns:<field_id:201 predicates:<unary_range_expr:<column_info:<field_id:131 data_type:Array nested_path:"0" element_type:VarChar > op:PrefixMatch value:<string_val:"abc" > > > query_info:<> placeholder_tag:"$0" >
    for (auto& testcase : prefix_testcases) {
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<UnaryRangeExprImpl<std::string>>(
            ColumnInfo(string_array_fid, DataType::ARRAY, testcase.nested_path),
            testcase.op_type,
            testcase.value,
            proto::plan::GenericValue::ValCase::kStringVal);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
        }
    }
}

TEST(Expr, TestArrayInTerm) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        auto new_bool_array_col = raw_data.get_col<ScalarArray>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarArray>(float_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarArray>(string_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

        std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            {R"(term_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
              values:<int64_val:1 > values:<int64_val:2 > values:<int64_val:3 >
        >)",
       "long",
       [](milvus::Array& array) {
           auto val = array.get_data<int64_t>(0);
           return val == 1 || val ==2 || val == 3;
       }},
            {R"(term_expr: <
              column_info: <
                field_id: 101
                data_type: Array
                nested_path:"0"
                element_type:Int64
              >
        >)",
             "long",
             [](milvus::Array& array) {
                 return false;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Bool
              >
                values:<bool_val:false > values:<bool_val:false >
        >)",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data<bool>(0);
                 return !val;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 102
                data_type: Array
                nested_path:"0"
                element_type:Bool
              >
        >)",
             "bool",
             [](milvus::Array& array) {
                 return false;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
                values:<float_val:1.23 > values:<float_val:124.31 >
        >)",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data<double>(0);
                 return val == 1.23 || val == 124.31;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 103
                data_type: Array
                nested_path:"0"
                element_type:Float
              >
        >)",
             "float",
             [](milvus::Array& array) {
                 return false;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:VarChar
              >
                values:<string_val:"abc" > values:<string_val:"idhgf1s" >
        >)",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data<std::string_view>(0);
                 return val == "abc" || val == "idhgf1s";
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"0"
                element_type:VarChar
              >
        >)",
             "string",
             [](milvus::Array& array) {
                 return false;
             }},
            {R"(term_expr: <
              column_info: <
                field_id: 104
                data_type: Array
                nested_path:"1024"
                element_type:VarChar
              >
                values:<string_val:"abc" > values:<string_val:"idhgf1s" >
        >)",
             "string",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data<std::string_view>(1024);
                 return val == "abc" || val == "idhgf1s";
             }},
        };

    std::string raw_plan_tmp = R"(vector_anns: <
                                    field_id: 100
                                    predicates: <
                                      @@@@
                                    >
                                    query_info: <
                                      topk: 10
                                      round_decimal: 3
                                      metric_type: "L2"
                                      search_params: "{\"nprobe\": 10}"
                                    >
                                    placeholder_tag: "$0"
     >)";

    for (auto [clause, array_type, ref_func] : testcases) {
        auto loc = raw_plan_tmp.find("@@@@");
        auto raw_plan = raw_plan_tmp;
        raw_plan.replace(loc, 4, clause);
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
        auto plan =
            CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
        auto final = visitor.call_child(*plan->plan_node_->predicate_.value());
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            ASSERT_EQ(ans, ref_func(array));
        }
    }
}

TEST(Expr, TestTermInArray) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarArray>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col = raw_data.get_col<ScalarArray>(long_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ExecExprVisitor visitor(
        *seg_promote, seg_promote->get_row_count(), MAX_TIMESTAMP);

    struct TermTestCases {
        std::vector<int64_t> values;
        std::vector<std::string> nested_path;
        std::function<bool(milvus::Array&)> check_func;
    };
    std::vector<TermTestCases> testcases = {
        {{100},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 100) {
                     return true;
                 }
             }
             return false;
         }},
        {{1024},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data<int64_t>(i);
                 if (val == 1024) {
                     return true;
                 }
             }
             return false;
         }},
    };

    for (auto& testcase : testcases) {
        RetrievePlanNode plan;
        plan.predicate_ = std::make_unique<TermExprImpl<int64_t>>(
            ColumnInfo(long_array_fid, DataType::ARRAY, testcase.nested_path),
            testcase.values,
            proto::plan::GenericValue::ValCase::kInt64Val,
            true);
        auto start = std::chrono::steady_clock::now();
        auto final = visitor.call_child(*plan.predicate_.value());
        std::cout << "cost"
                  << std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count()
                  << std::endl;
        EXPECT_EQ(final.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
        }
    }
}
