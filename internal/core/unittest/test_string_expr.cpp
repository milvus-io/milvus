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
#include <memory>
#include <regex>

#include "common/Tracer.h"
#include "pb/plan.pb.h"
#include "query/PlanProto.h"
#include "query/SearchBruteForce.h"
#include "query/Utils.h"
#include "query/PlanNodeVisitor.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {
auto
GenQueryInfo(int64_t topk,
             std::string metric_type,
             std::string search_params,
             int64_t round_decimal = -1) {
    auto query_info = new proto::plan::QueryInfo();
    query_info->set_topk(topk);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(search_params);
    query_info->set_round_decimal(round_decimal);
    return query_info;
}

auto
GenAnns(proto::plan::Expr* predicate,
        proto::plan::VectorType vectorType,
        int64_t field_id,
        std::string placeholder_tag = "$0") {
    auto query_info = GenQueryInfo(10, "L2", "{\"nprobe\": 10}", -1);
    auto anns = new proto::plan::VectorANNS();
    anns->set_vector_type(vectorType);
    anns->set_field_id(field_id);
    anns->set_allocated_predicates(predicate);
    anns->set_allocated_query_info(query_info);
    anns->set_placeholder_tag(placeholder_tag);
    return anns;
}

template <typename T>
auto
GenTermExpr(const std::vector<T>& values) {
    auto term_expr = new proto::plan::TermExpr();
    for (int i = 0; i < values.size(); i++) {
        auto add_value = term_expr->add_values();
        if constexpr (std::is_same_v<T, bool>) {
            add_value->set_bool_val(static_cast<T>(values[i]));
        } else if constexpr (std::is_integral_v<T>) {
            add_value->set_int64_val(static_cast<int64_t>(values[i]));
        } else if constexpr (std::is_floating_point_v<T>) {
            add_value->set_float_val(static_cast<double>(values[i]));
        } else if constexpr (std::is_same_v<T, std::string>) {
            add_value->set_string_val(static_cast<T>(values[i]));
        } else {
            static_assert(always_false<T>);
        }
    }
    return term_expr;
}

auto
GenCompareExpr(proto::plan::OpType op) {
    auto compare_expr = new proto::plan::CompareExpr();
    compare_expr->set_op(op);
    return compare_expr;
}

template <typename T>
auto
GenBinaryRangeExpr(bool lb_inclusive, bool ub_inclusive, T lb, T ub) {
    auto binary_range_expr = new proto::plan::BinaryRangeExpr();
    binary_range_expr->set_lower_inclusive(lb_inclusive);
    binary_range_expr->set_upper_inclusive(ub_inclusive);
    auto lb_generic = test::GenGenericValue(lb);
    auto ub_generic = test::GenGenericValue(ub);
    binary_range_expr->set_allocated_lower_value(lb_generic);
    binary_range_expr->set_allocated_upper_value(ub_generic);
    return binary_range_expr;
}

auto
GenNotExpr() {
    auto not_expr = new proto::plan::UnaryExpr();
    not_expr->set_op(proto::plan::UnaryExpr_UnaryOp_Not);
    return not_expr;
}

auto
GenPlanNode() {
    return std::make_unique<proto::plan::PlanNode>();
}

void
SetTargetEntry(std::unique_ptr<proto::plan::PlanNode>& plan_node,
               const std::vector<int64_t>& output_fields) {
    for (auto id : output_fields) {
        plan_node->add_output_field_ids(id);
    }
}

auto
GenTermPlan(const FieldMeta& fvec_meta,
            const FieldMeta& str_meta,
            const std::vector<std::string>& strs)
    -> std::unique_ptr<proto::plan::PlanNode> {
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto term_expr = GenTermExpr<std::string>(strs);
    term_expr->set_allocated_column_info(column_info);

    auto expr = test::GenExpr().release();
    expr->set_allocated_term_expr(term_expr);

    proto::plan::VectorType vector_type;
    if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
        vector_type = proto::plan::VectorType::FloatVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
        vector_type = proto::plan::VectorType::BinaryVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
        vector_type = proto::plan::VectorType::Float16Vector;
    }

    auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

    auto plan_node = GenPlanNode();
    plan_node->set_allocated_vector_anns(anns);
    return plan_node;
}

auto
GenAlwaysFalseExpr(const FieldMeta& fvec_meta, const FieldMeta& str_meta) {
    auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                           proto::schema::DataType::VarChar,
                                           false,
                                           false);
    auto term_expr =
        GenTermExpr<std::string>({});  // in empty set, always false.
    term_expr->set_allocated_column_info(column_info);

    auto expr = test::GenExpr().release();
    expr->set_allocated_term_expr(term_expr);
    return expr;
}

auto
GenAlwaysTrueExprIfValid(const FieldMeta& fvec_meta,
                         const FieldMeta& str_meta) {
    auto always_false_expr = GenAlwaysFalseExpr(fvec_meta, str_meta);
    auto not_expr = GenNotExpr();
    not_expr->set_allocated_child(always_false_expr);
    auto expr = test::GenExpr().release();
    expr->set_allocated_unary_expr(not_expr);
    return expr;
}

auto
GenAlwaysFalsePlan(const FieldMeta& fvec_meta, const FieldMeta& str_meta) {
    auto always_false_expr = GenAlwaysFalseExpr(fvec_meta, str_meta);
    proto::plan::VectorType vector_type;
    if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
        vector_type = proto::plan::VectorType::FloatVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
        vector_type = proto::plan::VectorType::BinaryVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
        vector_type = proto::plan::VectorType::Float16Vector;
    }
    auto anns =
        GenAnns(always_false_expr, vector_type, fvec_meta.get_id().get(), "$0");

    auto plan_node = GenPlanNode();
    plan_node->set_allocated_vector_anns(anns);
    return plan_node;
}

auto
GenAlwaysTruePlan(const FieldMeta& fvec_meta, const FieldMeta& str_meta) {
    auto always_true_expr = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    proto::plan::VectorType vector_type;
    if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
        vector_type = proto::plan::VectorType::FloatVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
        vector_type = proto::plan::VectorType::BinaryVector;
    } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
        vector_type = proto::plan::VectorType::Float16Vector;
    }
    auto anns =
        GenAnns(always_true_expr, vector_type, fvec_meta.get_id().get(), "$0");

    auto plan_node = GenPlanNode();
    plan_node->set_allocated_vector_anns(anns);
    return plan_node;
}

SchemaPtr
GenTestSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    return schema;
}

SchemaPtr
GenStrPKSchema() {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    return schema;
}
}  // namespace

TEST(StringExpr, Term) {
    auto schema = GenTestSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto vec_2k_3k = []() -> std::vector<std::string> {
        std::vector<std::string> ret;
        for (int i = 2000; i < 3000; i++) {
            ret.push_back(std::to_string(i));
        }
        return ret;
    }();

    std::map<int, std::vector<std::string>> terms = {
        {0, {"2000", "3000"}},
        {1, {"2000"}},
        {2, {"3000"}},
        {3, {}},
        {4, {vec_2k_3k}},
    };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [_, term] : terms) {
        auto plan_proto = GenTermPlan(fvec_meta, str_meta, term);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = str_col[i];
            auto ref = std::find(term.begin(), term.end(), val) != term.end();
            ASSERT_EQ(ans, ref) << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref) << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, TermNullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto vec_2k_3k = []() -> std::vector<std::string> {
        std::vector<std::string> ret;
        for (int i = 2000; i < 3000; i++) {
            ret.push_back(std::to_string(i));
        }
        return ret;
    }();

    std::map<int, std::vector<std::string>> terms = {
        {0, {"2000", "3000"}},
        {1, {"2000"}},
        {2, {"3000"}},
        {3, {}},
        {4, {vec_2k_3k}},
    };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [_, term] : terms) {
        auto plan_proto = GenTermPlan(fvec_meta, str_meta, term);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                continue;
            }
            auto val = str_col[i];
            auto ref = std::find(term.begin(), term.end(), val) != term.end();
            ASSERT_EQ(ans, ref) << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref) << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, Compare) {
    auto schema = GenTestSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));
    const auto& another_str_meta = schema->operator[](FieldName("another_str"));

    auto gen_compare_plan =
        [&, fvec_meta, str_meta, another_str_meta](
            proto::plan::OpType op) -> std::unique_ptr<proto::plan::PlanNode> {
        auto str_col_info =
            test::GenColumnInfo(str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);
        auto another_str_col_info =
            test::GenColumnInfo(another_str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);

        auto compare_expr = GenCompareExpr(op);
        compare_expr->set_allocated_left_column_info(str_col_info);
        compare_expr->set_allocated_right_column_info(another_str_col_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_compare_expr(compare_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    std::vector<std::tuple<proto::plan::OpType,
                           std::function<bool(std::string&, std::string&)>>>
        testcases{
            {proto::plan::OpType::GreaterThan,
             [](std::string& v1, std::string& v2) { return v1 > v2; }},
            {proto::plan::OpType::GreaterEqual,
             [](std::string& v1, std::string& v2) { return v1 >= v2; }},
            {proto::plan::OpType::LessThan,
             [](std::string& v1, std::string& v2) { return v1 < v2; }},
            {proto::plan::OpType::LessEqual,
             [](std::string& v1, std::string& v2) { return v1 <= v2; }},
            {proto::plan::OpType::Equal,
             [](std::string& v1, std::string& v2) { return v1 == v2; }},
            {proto::plan::OpType::NotEqual,
             [](std::string& v1, std::string& v2) { return v1 != v2; }},
            {proto::plan::OpType::PrefixMatch,
             [](std::string& v1, std::string& v2) {
                 return PrefixMatch(v1, v2);
             }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    std::vector<std::string> another_str_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto reserve_col = [&, raw_data](const FieldMeta& field_meta,
                                         std::vector<std::string>& str_col) {
            auto new_str_col = raw_data.get_col(field_meta.get_id());
            auto begin = FIELD_DATA(new_str_col, string).begin();
            auto end = FIELD_DATA(new_str_col, string).end();
            str_col.insert(str_col.end(), begin, end);
        };

        reserve_col(str_meta, str_col);
        reserve_col(another_str_meta, another_str_col);

        {
            seg->PreInsert(N);
            seg->Insert(iter * N,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = str_col[i];
            auto another_val = another_str_col[i];
            auto ref = ref_func(val, another_val);
            ASSERT_EQ(ans, ref) << "@" << op << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << op << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, CompareNullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));
    const auto& another_str_meta = schema->operator[](FieldName("another_str"));

    auto gen_compare_plan =
        [&, fvec_meta, str_meta, another_str_meta](
            proto::plan::OpType op) -> std::unique_ptr<proto::plan::PlanNode> {
        auto str_col_info =
            test::GenColumnInfo(str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);
        auto another_str_col_info =
            test::GenColumnInfo(another_str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);

        auto compare_expr = GenCompareExpr(op);
        compare_expr->set_allocated_left_column_info(str_col_info);
        compare_expr->set_allocated_right_column_info(another_str_col_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_compare_expr(compare_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    std::vector<std::tuple<proto::plan::OpType,
                           std::function<bool(std::string&, std::string&)>>>
        testcases{
            {proto::plan::OpType::GreaterThan,
             [](std::string& v1, std::string& v2) { return v1 > v2; }},
            {proto::plan::OpType::GreaterEqual,
             [](std::string& v1, std::string& v2) { return v1 >= v2; }},
            {proto::plan::OpType::LessThan,
             [](std::string& v1, std::string& v2) { return v1 < v2; }},
            {proto::plan::OpType::LessEqual,
             [](std::string& v1, std::string& v2) { return v1 <= v2; }},
            {proto::plan::OpType::Equal,
             [](std::string& v1, std::string& v2) { return v1 == v2; }},
            {proto::plan::OpType::NotEqual,
             [](std::string& v1, std::string& v2) { return v1 != v2; }},
            {proto::plan::OpType::PrefixMatch,
             [](std::string& v1, std::string& v2) {
                 return PrefixMatch(v1, v2);
             }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    std::vector<std::string> another_str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto reserve_col = [&, raw_data](const FieldMeta& field_meta,
                                         std::vector<std::string>& str_col) {
            auto new_str_col = raw_data.get_col(field_meta.get_id());
            auto begin = FIELD_DATA(new_str_col, string).begin();
            auto end = FIELD_DATA(new_str_col, string).end();
            str_col.insert(str_col.end(), begin, end);
        };

        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());

        reserve_col(str_meta, str_col);
        reserve_col(another_str_meta, another_str_col);

        {
            seg->PreInsert(N);
            seg->Insert(iter * N,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                continue;
            }
            auto val = str_col[i];
            auto another_val = another_str_col[i];
            auto ref = ref_func(val, another_val);
            ASSERT_EQ(ans, ref) << "@" << op << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << op << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, CompareNullable2) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR);
    schema->AddDebugField("another_str", DataType::VARCHAR, true);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));
    const auto& another_str_meta = schema->operator[](FieldName("another_str"));

    auto gen_compare_plan =
        [&, fvec_meta, str_meta, another_str_meta](
            proto::plan::OpType op) -> std::unique_ptr<proto::plan::PlanNode> {
        auto str_col_info =
            test::GenColumnInfo(str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);
        auto another_str_col_info =
            test::GenColumnInfo(another_str_meta.get_id().get(),
                                proto::schema::DataType::VarChar,
                                false,
                                false);

        auto compare_expr = GenCompareExpr(op);
        compare_expr->set_allocated_left_column_info(str_col_info);
        compare_expr->set_allocated_right_column_info(another_str_col_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_compare_expr(compare_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    std::vector<std::tuple<proto::plan::OpType,
                           std::function<bool(std::string&, std::string&)>>>
        testcases{
            {proto::plan::OpType::GreaterThan,
             [](std::string& v1, std::string& v2) { return v1 > v2; }},
            {proto::plan::OpType::GreaterEqual,
             [](std::string& v1, std::string& v2) { return v1 >= v2; }},
            {proto::plan::OpType::LessThan,
             [](std::string& v1, std::string& v2) { return v1 < v2; }},
            {proto::plan::OpType::LessEqual,
             [](std::string& v1, std::string& v2) { return v1 <= v2; }},
            {proto::plan::OpType::Equal,
             [](std::string& v1, std::string& v2) { return v1 == v2; }},
            {proto::plan::OpType::NotEqual,
             [](std::string& v1, std::string& v2) { return v1 != v2; }},
            {proto::plan::OpType::PrefixMatch,
             [](std::string& v1, std::string& v2) {
                 return PrefixMatch(v1, v2);
             }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    std::vector<std::string> another_str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);

        auto reserve_col = [&, raw_data](const FieldMeta& field_meta,
                                         std::vector<std::string>& str_col) {
            auto new_str_col = raw_data.get_col(field_meta.get_id());
            auto begin = FIELD_DATA(new_str_col, string).begin();
            auto end = FIELD_DATA(new_str_col, string).end();
            str_col.insert(str_col.end(), begin, end);
        };

        auto new_str_valid_col =
            raw_data.get_col_valid(another_str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());

        reserve_col(str_meta, str_col);
        reserve_col(another_str_meta, another_str_col);

        {
            seg->PreInsert(N);
            seg->Insert(iter * N,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], false);
                }
                continue;
            }
            auto val = str_col[i];
            auto another_val = another_str_col[i];
            auto ref = ref_func(val, another_val);
            ASSERT_EQ(ans, ref) << "@" << op << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << op << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, UnaryRange) {
    auto schema = GenTestSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_unary_range_plan =
        [&, fvec_meta, str_meta](
            proto::plan::OpType op,
            std::string value) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr = test::GenUnaryRangeExpr(op, value);
        unary_range_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    std::vector<std::tuple<proto::plan::OpType,
                           std::string,
                           std::function<bool(std::string&)>>>
        testcases{
            {proto::plan::OpType::GreaterThan,
             "2000",
             [](std::string& val) { return val > "2000"; }},
            {proto::plan::OpType::GreaterEqual,
             "2000",
             [](std::string& val) { return val >= "2000"; }},
            {proto::plan::OpType::LessThan,
             "3000",
             [](std::string& val) { return val < "3000"; }},
            {proto::plan::OpType::LessEqual,
             "3000",
             [](std::string& val) { return val <= "3000"; }},
            {proto::plan::OpType::PrefixMatch,
             "a",
             [](std::string& val) { return PrefixMatch(val, "a"); }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, value, ref_func] : testcases) {
        auto plan_proto = gen_unary_range_plan(op, value);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = str_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << "@" << op << "@" << value << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << op << "@" << value << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, UnaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_unary_range_plan =
        [&, fvec_meta, str_meta](
            proto::plan::OpType op,
            std::string value) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr = test::GenUnaryRangeExpr(op, value);
        unary_range_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    std::vector<std::tuple<proto::plan::OpType,
                           std::string,
                           std::function<bool(std::string&)>>>
        testcases{
            {proto::plan::OpType::GreaterThan,
             "2000",
             [](std::string& val) { return val > "2000"; }},
            {proto::plan::OpType::GreaterEqual,
             "2000",
             [](std::string& val) { return val >= "2000"; }},
            {proto::plan::OpType::LessThan,
             "3000",
             [](std::string& val) { return val < "3000"; }},
            {proto::plan::OpType::LessEqual,
             "3000",
             [](std::string& val) { return val <= "3000"; }},
            {proto::plan::OpType::PrefixMatch,
             "a",
             [](std::string& val) { return PrefixMatch(val, "a"); }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, value, ref_func] : testcases) {
        auto plan_proto = gen_unary_range_plan(op, value);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                continue;
            }
            auto val = str_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << "@" << op << "@" << value << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << op << "@" << value << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, NullExpr) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_plan =
        [&, fvec_meta, str_meta](
            NullExprType op) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false,
                                               proto::schema::DataType::None,
                                               true);
        auto null_expr = test::GenNullExpr(op);
        null_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_null_expr(null_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }
    std::vector<NullExprType> ops{NullExprType::NullExpr_NullOp_IsNull,
                                  NullExprType::NullExpr_NullOp_IsNotNull};

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    // is_null
    for (const auto op : ops) {
        auto plan_proto = gen_plan(op);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (op == NullExprType::NullExpr_NullOp_IsNull) {
                ASSERT_EQ(ans, !valid_data[i]);
            } else {
                ASSERT_EQ(ans, valid_data[i]);
            }
        }
    }
}

TEST(StringExpr, BinaryRange) {
    auto schema = GenTestSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_binary_range_plan =
        [&, fvec_meta, str_meta](
            bool lb_inclusive,
            bool ub_inclusive,
            std::string lb,
            std::string ub) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto binary_range_expr =
            GenBinaryRangeExpr(lb_inclusive, ub_inclusive, lb, ub);
        binary_range_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_binary_range_expr(binary_range_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    // bool lb_inclusive, bool ub_inclusive, std::string lb, std::string ub
    std::vector<std::tuple<bool,
                           bool,
                           std::string,
                           std::string,
                           std::function<bool(std::string&)>>>
        testcases{
            {false,
             false,
             "2000",
             "3000",
             [](std::string& val) { return val > "2000" && val < "3000"; }},
            {false,
             true,
             "2000",
             "3000",
             [](std::string& val) { return val > "2000" && val <= "3000"; }},
            {true,
             false,
             "2000",
             "3000",
             [](std::string& val) { return val >= "2000" && val < "3000"; }},
            {true,
             true,
             "2000",
             "3000",
             [](std::string& val) { return val >= "2000" && val <= "3000"; }},
            {true,
             true,
             "2000",
             "1000",
             [](std::string& val) { return false; }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [lb_inclusive, ub_inclusive, lb, ub, ref_func] :
         testcases) {
        auto plan_proto =
            gen_binary_range_plan(lb_inclusive, ub_inclusive, lb, ub);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];

            auto val = str_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << "@" << lb_inclusive << "@" << ub_inclusive << "@" << lb
                << "@" << ub << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << lb_inclusive << "@" << ub_inclusive << "@" << lb
                    << "@" << ub << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(StringExpr, BinaryRangeNullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_binary_range_plan =
        [&, fvec_meta, str_meta](
            bool lb_inclusive,
            bool ub_inclusive,
            std::string lb,
            std::string ub) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto binary_range_expr =
            GenBinaryRangeExpr(lb_inclusive, ub_inclusive, lb, ub);
        binary_range_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_binary_range_expr(binary_range_expr);

        proto::plan::VectorType vector_type;
        if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT) {
            vector_type = proto::plan::VectorType::FloatVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_BINARY) {
            vector_type = proto::plan::VectorType::BinaryVector;
        } else if (fvec_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
            vector_type = proto::plan::VectorType::Float16Vector;
        }
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    // bool lb_inclusive, bool ub_inclusive, std::string lb, std::string ub
    std::vector<std::tuple<bool,
                           bool,
                           std::string,
                           std::string,
                           std::function<bool(std::string&)>>>
        testcases{
            {false,
             false,
             "2000",
             "3000",
             [](std::string& val) { return val > "2000" && val < "3000"; }},
            {false,
             true,
             "2000",
             "3000",
             [](std::string& val) { return val > "2000" && val <= "3000"; }},
            {true,
             false,
             "2000",
             "3000",
             [](std::string& val) { return val >= "2000" && val < "3000"; }},
            {true,
             true,
             "2000",
             "3000",
             [](std::string& val) { return val >= "2000" && val <= "3000"; }},
            {true,
             true,
             "2000",
             "1000",
             [](std::string& val) { return false; }},
        };

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<std::string> str_col;
    FixedVector<bool> valid_data;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_col = raw_data.get_col(str_meta.get_id());
        auto begin = FIELD_DATA(new_str_col, string).begin();
        auto end = FIELD_DATA(new_str_col, string).end();
        str_col.insert(str_col.end(), begin, end);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [lb_inclusive, ub_inclusive, lb, ub, ref_func] :
         testcases) {
        auto plan_proto =
            gen_binary_range_plan(lb_inclusive, ub_inclusive, lb, ub);
        auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
        query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            if (!valid_data[i]) {
                ASSERT_EQ(ans, false);
                if (i % 2 == 0) {
                    ASSERT_EQ(view[int(i / 2)], false);
                }
                continue;
            }
            auto val = str_col[i];
            auto ref = ref_func(val);
            ASSERT_EQ(ans, ref)
                << "@" << lb_inclusive << "@" << ub_inclusive << "@" << lb
                << "@" << ub << "@" << i << "!!" << val;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << "@" << lb_inclusive << "@" << ub_inclusive << "@" << lb
                    << "@" << ub << "@" << i << "!!" << val;
            }
        }
    }
}

TEST(AlwaysTrueStringPlan, SearchWithOutputFields) {
    auto schema = GenStrPKSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto N = 100000;
    auto dim = fvec_meta.get_dim();
    auto round_decimal = -1;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fvec_meta.get_id());
    auto str_col =
        dataset.get_col(str_meta.get_id())->scalars().string_data().data();
    auto query_ptr = vec_col.data();
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto plan_proto = GenAlwaysTruePlan(fvec_meta, str_meta);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(*schema).CreatePlan(*plan_proto);
    auto num_queries = 5;
    auto topk = 10;
    auto ph_group_raw =
        CreatePlaceholderGroupFromBlob(num_queries, 16, query_ptr);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto index_info = std::map<std::string, std::string>{};

    std::vector<const PlaceholderGroup*> ph_group_arr = {ph_group.get()};

    MetricType metric_type = knowhere::metric::L2;
    query::dataset::SearchDataset search_dataset{
        metric_type,  //
        num_queries,  //
        topk,         //
        round_decimal,
        dim,       //
        query_ptr  //
    };

    SearchInfo search_info;
    search_info.topk_ = topk;
    search_info.round_decimal_ = round_decimal;
    search_info.metric_type_ = metric_type;
    auto raw_dataset = query::dataset::RawDataset{0, dim, N, vec_col.data()};
    auto sub_result = BruteForceSearch(search_dataset,
                                       raw_dataset,
                                       search_info,
                                       index_info,
                                       nullptr,
                                       DataType::VECTOR_FLOAT);

    auto sr = segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    segment->FillPrimaryKeys(plan.get(), *sr);
    segment->FillTargetEntry(plan.get(), *sr);
    ASSERT_EQ(sr->pk_type_, DataType::VARCHAR);
    ASSERT_TRUE(sr->output_fields_data_.find(str_meta.get_id()) !=
                sr->output_fields_data_.end());
    auto retrieved_str_col = sr->output_fields_data_[str_meta.get_id()]
                                 ->scalars()
                                 .string_data()
                                 .data();
    for (auto q = 0; q < num_queries; q++) {
        for (auto k = 0; k < topk; k++) {
            auto offset = q * topk + k;
            auto seg_offset = sub_result.get_seg_offsets()[offset];
            ASSERT_EQ(std::get<std::string>(sr->primary_keys_[offset]),
                      str_col[seg_offset]);
            ASSERT_EQ(retrieved_str_col[offset], str_col[seg_offset]);
        }
    }
}

TEST(AlwaysTrueStringPlan, QueryWithOutputFields) {
    auto schema = GenStrPKSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto N = 10000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fvec_meta.get_id());
    auto str_col =
        dataset.get_col(str_meta.get_id())->scalars().string_data().data();
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto expr_proto = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    auto plan_proto = GenPlanNode();
    plan_proto->mutable_query()->set_allocated_predicates(expr_proto);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(*schema).CreateRetrievePlan(*plan_proto);

    Timestamp time = MAX_TIMESTAMP;

    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), time, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->ids().str_id().data().size(), N);
    ASSERT_EQ(retrieved->offset().size(), N);
    ASSERT_EQ(retrieved->fields_data().size(), 1);
    ASSERT_EQ(retrieved->fields_data(0).scalars().string_data().data().size(),
              N);
    ASSERT_EQ(retrieved->fields_data(0).valid_data_size(), 0);
}

TEST(AlwaysTrueStringPlan, QueryWithOutputFieldsNullable) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("another_str", DataType::VARCHAR);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto N = 10000;
    auto dataset = DataGen(schema, N);
    auto vec_col = dataset.get_col<float>(fvec_meta.get_id());
    auto str_col =
        dataset.get_col(str_meta.get_id())->scalars().string_data().data();
    auto valid_data = dataset.get_col_valid(str_meta.get_id());
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    auto expr_proto = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    auto plan_proto = GenPlanNode();
    plan_proto->mutable_query()->set_allocated_predicates(expr_proto);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(*schema).CreateRetrievePlan(*plan_proto);

    Timestamp time = MAX_TIMESTAMP;

    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), time, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->offset().size(), N / 2);
    ASSERT_EQ(retrieved->fields_data().size(), 1);
    ASSERT_EQ(retrieved->fields_data(0).scalars().string_data().data().size(),
              N / 2);
    ASSERT_EQ(retrieved->fields_data(0).valid_data().size(), N / 2);
}
