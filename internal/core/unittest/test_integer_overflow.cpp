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

#include <boost/format.hpp>
#include <gtest/gtest.h>
#include <cstdint>
#include <memory>
#include <regex>
#include <vector>
#include <chrono>

#include "common/Types.h"
#include "query/Expr.h"
#include "query/Plan.h"
#include "query/generated/ExecExprVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;

TEST(Expr, IntegerOverflow) {
    using namespace milvus::query;
    using namespace milvus::segcore;
    std::vector<std::tuple<std::string, std::function<bool(int8_t)>>> testcases = {
        /////////////////////////////////////////////////////////// term
        {
            R"(
term_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  values: <
    int64_val: 20230704
  >
  values: <
    int64_val: 1
  >
  values: <
    int64_val: -1
  >
>
            )",
            [](int8_t v) { return v == 1 || v == -1; }},
        {
            R"(
unary_expr: <
  op: Not
  child: <
    term_expr: <
      column_info: <
        field_id: 101
        data_type: Int8
      >
      values: <
        int64_val: 20230704
      >
      values: <
        int64_val: 1
      >
      values: <
        int64_val: -1
      >
    >
  >
>
            )",
            [](int8_t v) { return v != 1 && v != -1; }},

        /////////////////////////////////////////////////////////// unary range
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: Equal
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return false; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: NotEqual
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: GreaterEqual
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return false; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: GreaterEqual
  value: <
    int64_val: -20230704
  >
>
            )",
            [](int8_t v) { return true; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: GreaterThan
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return false; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: GreaterThan
  value: <
    int64_val: -20230704
  >
>
            )",
            [](int8_t v) { return true; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: LessEqual
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: LessEqual
  value: <
    int64_val: -20230704
  >
>
            )",
            [](int8_t v) { return false; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: LessThan
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},
        {
            R"(
unary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  op: LessThan
  value: <
    int64_val: -20230704
  >
>
            )",
            [](int8_t v) { return false; }},

        /////////////////////////////////////////////////////////// binary range
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 1
  >
>
            )",
            [](int8_t v) { return v < 1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_value: <
    int64_val: -1
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v > -1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},

        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 1
  >
>
            )",
            [](int8_t v) { return v < 1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  lower_value: <
    int64_val: -1
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v >= -1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},

        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  upper_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 1
  >
>
            )",
            [](int8_t v) { return v <= 1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  upper_inclusive: true
  lower_value: <
    int64_val: -1
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v > -1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  upper_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},

        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  upper_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 1
  >
>
            )",
            [](int8_t v) { return v <= 1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  upper_inclusive: true
  lower_value: <
    int64_val: -1
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v >= -1; }},
        {
            R"(
binary_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  lower_inclusive: true
  upper_inclusive: true
  lower_value: <
    int64_val: -20230704
  >
  upper_value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return true; }},

        /////////////////////////////////////////////////////////// binary arithmetic range
        // Add
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Add
  right_operand: <
    int64_val: 2560
  >
  op: Equal
  value: <
    int64_val: 2450
  >
>
            )",
            [](int8_t v) { return v + 2560 == 2450; }},
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Add
  right_operand: <
    int64_val: 2560
  >
  op: NotEqual
  value: <
    int64_val: 2450
  >
>
            )",
            [](int8_t v) { return v + 2560 != 2450; }},
        // Sub
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Sub
  right_operand: <
    int64_val: 2560
  >
  op: Equal
  value: <
    int64_val: 2450
  >
>
            )",
            [](int8_t v) { return v - 2560 == 2450; }},
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Sub
  right_operand: <
    int64_val: 2560
  >
  op: NotEqual
  value: <
    int64_val: 2450
  >
>
            )",
            [](int8_t v) { return v - 2560 != 2450; }},
        // Mul
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Mul
  right_operand: <
    int64_val: 256
  >
  op: Equal
  value: <
    int64_val: 16384
  >
>
            )",
            [](int8_t v) { return v * 256 == 16384; }},
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Mul
  right_operand: <
    int64_val: 256
  >
  op: NotEqual
  value: <
    int64_val: 16384
  >
>
            )",
            [](int8_t v) { return v * 256 != 16384; }},
        // Div
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Div
  right_operand: <
    int64_val: 256
  >
  op: Equal
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v / 256 == 20230704; }},
        {
            R"(
binary_arith_op_eval_range_expr: <
  column_info: <
    field_id: 101
    data_type: Int8
  >
  arith_op: Div
  right_operand: <
    int64_val: 256
  >
  op: NotEqual
  value: <
    int64_val: 20230704
  >
>
            )",
            [](int8_t v) { return v / 256 != 20230704; }},
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
    auto i8_fid = schema->AddDebugField("age", DataType::INT8);
    auto i64_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int8_t> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int8_t>(i8_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
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

            auto val = age_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << clause << "@" << i << "!!" << static_cast<int64_t>(val);
        }
    }
}
