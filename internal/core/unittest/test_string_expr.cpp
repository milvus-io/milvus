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
#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/PlanProto.h"
#include "query/SearchBruteForce.h"
#include "query/SubSearchResult.h"
#include "query/helper.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "common/RegexQuery.h"
#include "common/Volnitsky.h"
#include "index/scalar/ngram/NgramRegex.h"
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

    proto::plan::VectorType vector_type = proto::plan::VectorType::FloatVector;
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
GenAlwaysTruePlan(const FieldMeta& fvec_meta, const FieldMeta& str_meta) {
    auto always_true_expr = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    proto::plan::VectorType vector_type = proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [_, term] : terms) {
        auto plan_proto = GenTermPlan(fvec_meta, str_meta, term);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [_, term] : terms) {
        auto plan_proto = GenTermPlan(fvec_meta, str_meta, term);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                        raw_data.SharedRaw());
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                        raw_data.SharedRaw());
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                        raw_data.SharedRaw());
        }
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, ref_func] : testcases) {
        auto plan_proto = gen_compare_plan(op);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, value, ref_func] : testcases) {
        auto plan_proto = gen_unary_range_plan(op, value);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [op, value, ref_func] : testcases) {
        auto plan_proto = gen_unary_range_plan(op, value);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }
    std::vector<NullExprType> ops{NullExprType::NullExpr_NullOp_IsNull,
                                  NullExprType::NullExpr_NullOp_IsNotNull};

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    // is_null
    for (const auto op : ops) {
        auto plan_proto = gen_plan(op);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [lb_inclusive, ub_inclusive, lb, ub, ref_func] :
         testcases) {
        auto plan_proto =
            gen_binary_range_plan(lb_inclusive, ub_inclusive, lb, ub);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [lb_inclusive, ub_inclusive, lb, ub, ref_func] :
         testcases) {
        auto plan_proto =
            gen_binary_range_plan(lb_inclusive, ub_inclusive, lb, ub);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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
                    dataset.SharedRaw());

    auto plan_proto = GenAlwaysTruePlan(fvec_meta, str_meta);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
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
                                       DataType::VECTOR_FLOAT,
                                       DataType::NONE,
                                       nullptr);

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
            auto seg_offset = sub_result.get_offsets()[offset];
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
                    dataset.SharedRaw());

    auto expr_proto = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    auto plan_proto = GenPlanNode();
    plan_proto->mutable_query()->set_allocated_predicates(expr_proto);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(schema).CreateRetrievePlan(*plan_proto);

    Timestamp time = MAX_TIMESTAMP;

    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), time, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->ids().str_id().data().size(), N);
    ASSERT_EQ(retrieved->offset().size(), N);
    ASSERT_EQ(retrieved->fields_data().size(), 1);
    ASSERT_EQ(retrieved->fields_data(0).scalars().string_data().data().size(),
              N);
    ASSERT_TRUE(GetFieldDataRowValidData(retrieved->fields_data(0)).empty());
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
                    dataset.SharedRaw());

    auto expr_proto = GenAlwaysTrueExprIfValid(fvec_meta, str_meta);
    auto plan_proto = GenPlanNode();
    plan_proto->mutable_query()->set_allocated_predicates(expr_proto);
    SetTargetEntry(plan_proto, {str_meta.get_id().get()});
    auto plan = ProtoParser(schema).CreateRetrievePlan(*plan_proto);

    Timestamp time = MAX_TIMESTAMP;

    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), time, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->offset().size(), N / 2);
    ASSERT_EQ(retrieved->fields_data().size(), 1);
    ASSERT_EQ(retrieved->fields_data(0).scalars().string_data().data().size(),
              N / 2);
    ASSERT_EQ(GetFieldDataRowValidData(retrieved->fields_data(0)).size(),
              N / 2);
}

// Test: NOT (a IS NOT NULL) - verifies that the result valid bits are always true
// because IsNull/IsNotNull expressions always produce valid (non-NULL) results,
// and NOT applied to valid results also produces valid results.
TEST(StringExpr, NotIsNotNullExpr) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    FixedVector<bool> valid_data;
    int num_iters = 10;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Create NullExpr for IS NOT NULL
    auto null_expr = std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR, {}, true),
        NullExprType::NullExpr_NullOp_IsNotNull);

    // Wrap with LogicalUnaryExpr (NOT)
    auto not_expr = std::make_shared<milvus::expr::LogicalUnaryExpr>(
        milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, null_expr);

    // Execute the expression using ExecuteQueryExpr
    auto plan = milvus::test::CreateRetrievePlanByExpr(not_expr);
    auto filter_node = plan->sources()[0];  // MvccNode -> FilterBitsNode
    BitsetType final = ExecuteQueryExpr(
        filter_node, seg_promote, N * num_iters, MAX_TIMESTAMP);

    EXPECT_EQ(final.size(), N * num_iters);

    for (int i = 0; i < N * num_iters; ++i) {
        // NOT (IS NOT NULL) should equal IS NULL
        // data[i] should be true if original was null, false if original was valid
        bool expected_data = !valid_data[i];
        ASSERT_EQ(final[i], expected_data) << "Data mismatch at index " << i;
    }
}

// Test: NOT (a IS NULL) - verifies that the result valid bits are always true
TEST(StringExpr, NotIsNullExpr) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    FixedVector<bool> valid_data;
    int num_iters = 10;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_valid_col = raw_data.get_col_valid(str_meta.get_id());
        valid_data.insert(valid_data.end(),
                          new_str_valid_col.begin(),
                          new_str_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Create NullExpr for IS NULL
    auto null_expr = std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR, {}, true),
        NullExprType::NullExpr_NullOp_IsNull);

    // Wrap with LogicalUnaryExpr (NOT)
    auto not_expr = std::make_shared<milvus::expr::LogicalUnaryExpr>(
        milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, null_expr);

    // Execute the expression using ExecuteQueryExpr
    auto plan = milvus::test::CreateRetrievePlanByExpr(not_expr);
    auto filter_node = plan->sources()[0];  // MvccNode -> FilterBitsNode
    BitsetType final = ExecuteQueryExpr(
        filter_node, seg_promote, N * num_iters, MAX_TIMESTAMP);

    EXPECT_EQ(final.size(), N * num_iters);

    for (int i = 0; i < N * num_iters; ++i) {
        // NOT (IS NULL) should equal IS NOT NULL
        // data[i] should be true if original was valid, false if original was null
        bool expected_data = valid_data[i];
        ASSERT_EQ(final[i], expected_data) << "Data mismatch at index " << i;
    }
}

// Test: (a IS NOT NULL) AND (b IS NOT NULL)
// This tests the interaction between NullExpr and three-valued logic
TEST(StringExpr, IsNotNullAndNullCondition) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("str", DataType::VARCHAR, true);
    schema->AddDebugField("int_val", DataType::INT64, true);  // nullable int
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto& str_meta = schema->operator[](FieldName("str"));
    const auto& int_meta = schema->operator[](FieldName("int_val"));

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    FixedVector<bool> str_valid_data;
    FixedVector<bool> int_valid_data;
    int num_iters = 10;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_str_valid = raw_data.get_col_valid(str_meta.get_id());
        auto new_int_valid = raw_data.get_col_valid(int_meta.get_id());
        str_valid_data.insert(
            str_valid_data.end(), new_str_valid.begin(), new_str_valid.end());
        int_valid_data.insert(
            int_valid_data.end(), new_int_valid.begin(), new_int_valid.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // Create: (str IS NOT NULL) AND (int_val IS NOT NULL)
    auto str_is_not_null = std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(str_meta.get_id(), DataType::VARCHAR, {}, true),
        NullExprType::NullExpr_NullOp_IsNotNull);

    auto int_is_not_null = std::make_shared<milvus::expr::NullExpr>(
        expr::ColumnInfo(int_meta.get_id(), DataType::INT64, {}, true),
        NullExprType::NullExpr_NullOp_IsNotNull);

    auto and_expr = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And,
        str_is_not_null,
        int_is_not_null);

    // Execute the expression using ExecuteQueryExpr
    auto plan = milvus::test::CreateRetrievePlanByExpr(and_expr);
    auto filter_node = plan->sources()[0];  // MvccNode -> FilterBitsNode
    BitsetType final = ExecuteQueryExpr(
        filter_node, seg_promote, N * num_iters, MAX_TIMESTAMP);

    EXPECT_EQ(final.size(), N * num_iters);

    for (int i = 0; i < N * num_iters; ++i) {
        // (str IS NOT NULL) AND (int IS NOT NULL)
        // Both operands always have valid=true, so result is always valid
        bool expected_data = str_valid_data[i] && int_valid_data[i];
        ASSERT_EQ(final[i], expected_data) << "Data mismatch at index " << i;
    }
}

TEST(StringExpr, RegexMatch) {
    auto schema = GenTestSchema();
    const auto& fvec_meta = schema->operator[](FieldName("fvec"));
    const auto& str_meta = schema->operator[](FieldName("str"));

    auto gen_regex_plan =
        [&, fvec_meta, str_meta](
            std::string pattern) -> std::unique_ptr<proto::plan::PlanNode> {
        auto column_info = test::GenColumnInfo(str_meta.get_id().get(),
                                               proto::schema::DataType::VarChar,
                                               false,
                                               false);
        auto unary_range_expr =
            test::GenUnaryRangeExpr(proto::plan::OpType::RegexMatch, pattern);
        unary_range_expr->set_allocated_column_info(column_info);

        auto expr = test::GenExpr().release();
        expr->set_allocated_unary_range_expr(unary_range_expr);

        proto::plan::VectorType vector_type =
            proto::plan::VectorType::FloatVector;
        auto anns = GenAnns(expr, vector_type, fvec_meta.get_id().get(), "$0");

        auto plan_node = std::make_unique<proto::plan::PlanNode>();
        plan_node->set_allocated_vector_anns(anns);
        return plan_node;
    };

    // Test cases: {pattern, expected_match_func}
    // PartialRegexMatcher uses RE2::PartialMatch (substring semantics)
    std::vector<
        std::tuple<std::string, std::function<bool(const std::string&)>>>
        testcases{
            // Pure literal — substring match
            {"abc",
             [](const std::string& val) {
                 return val.find("abc") != std::string::npos;
             }},
            // Regex with character class
            {"[0-9]+",
             [](const std::string& val) {
                 PartialRegexMatcher m("[0-9]+");
                 return m(val);
             }},
            // Anchored start
            {"^[a-z]",
             [](const std::string& val) {
                 PartialRegexMatcher m("^[a-z]");
                 return m(val);
             }},
            // Empty pattern — matches everything
            {"", [](const std::string& val) { return true; }},
            // Dot matches any
            {"a.c",
             [](const std::string& val) {
                 PartialRegexMatcher m("a.c");
                 return m(val);
             }},
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
                    raw_data.SharedRaw());
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    for (const auto& [pattern, ref_func] : testcases) {
        auto plan_proto = gen_regex_plan(pattern);
        auto plan = ProtoParser(schema).CreatePlan(*plan_proto);
        BitsetType final_result;
        final_result = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final_result.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final_result[i];
            auto ref = ref_func(str_col[i]);
            ASSERT_EQ(ans, ref)
                << "RegexMatch pattern=\"" << pattern << "\" index=" << i
                << " val=\"" << str_col[i] << "\"";
        }
    }
}

TEST(StringExpr, RegexMatchPartialMatchSemantics) {
    // Verify that RegexMatch uses PartialMatch (substring), not FullMatch
    PartialRegexMatcher partial("abc");
    EXPECT_TRUE(partial(std::string("xyzabcdef")));  // substring match
    EXPECT_TRUE(partial(std::string("abc")));        // exact match
    EXPECT_TRUE(partial(std::string("123abc456")));  // middle match
    EXPECT_FALSE(partial(std::string("ab")));        // no match
    EXPECT_FALSE(partial(std::string("ABc")));       // case sensitive

    // Contrast with RegexMatcher (FullMatch) — only matches entire string
    RegexMatcher full("abc");
    EXPECT_FALSE(full(std::string("xyzabcdef")));
    EXPECT_TRUE(full(std::string("abc")));

    // Anchors
    PartialRegexMatcher start_anchor("^hello");
    EXPECT_TRUE(start_anchor(std::string("hello world")));
    EXPECT_FALSE(start_anchor(std::string("say hello")));

    PartialRegexMatcher end_anchor("world$");
    EXPECT_TRUE(end_anchor(std::string("hello world")));
    EXPECT_FALSE(end_anchor(std::string("world cup")));

    PartialRegexMatcher both_anchors("^exact$");
    EXPECT_TRUE(both_anchors(std::string("exact")));
    EXPECT_FALSE(both_anchors(std::string("not exact")));

    // Case insensitive flag
    PartialRegexMatcher case_insensitive("(?i)hello");
    EXPECT_TRUE(case_insensitive(std::string("HELLO")));
    EXPECT_TRUE(case_insensitive(std::string("Hello World")));
    EXPECT_TRUE(case_insensitive(std::string("say hello")));

    // dot_nl: dot matches newline (RE2 dot_nl=true)
    PartialRegexMatcher dot_nl("a.b");
    EXPECT_TRUE(dot_nl(std::string("a\nb")));
    EXPECT_TRUE(dot_nl(std::string("aXb")));
}

TEST(StringExpr, ExtractLiteralsFromRegex) {
    // Test the regex literal extraction function used by ngram index.
    // We test via PartialRegexMatcher to verify end-to-end correctness,
    // and also verify that the patterns the extractor CAN'T handle
    // still produce correct results via Phase-2 brute-force.

    // Pure literal — should match as substring
    PartialRegexMatcher m1("timeout");
    EXPECT_TRUE(m1(std::string("connection timeout error")));
    EXPECT_FALSE(m1(std::string("connection error")));

    // Multiple literals separated by regex constructs
    PartialRegexMatcher m2("error.*timeout");
    EXPECT_TRUE(m2(std::string("error: connection timeout")));
    EXPECT_FALSE(m2(std::string("timeout then error")));  // wrong order

    // Character classes — no literals extractable
    PartialRegexMatcher m3("[a-z]+");
    EXPECT_TRUE(m3(std::string("hello")));
    EXPECT_FALSE(m3(std::string("12345")));

    // Escaped metacharacters should be literal
    PartialRegexMatcher m4("file\\.txt");
    EXPECT_TRUE(m4(std::string("open file.txt now")));
    EXPECT_FALSE(m4(std::string("open fileTtxt now")));  // dot is literal

    // Complex pattern with mixed literals and regex
    PartialRegexMatcher m5("user_[0-9]+@gmail\\.com");
    EXPECT_TRUE(m5(std::string("contact user_123@gmail.com today")));
    EXPECT_FALSE(m5(std::string("contact user_abc@gmail.com today")));

    // ── Patterns that previously caused false negatives (Issue #1) ──

    // Alternation: foo|bar — only one branch needs to match
    PartialRegexMatcher m6("foo|bar");
    EXPECT_TRUE(m6(std::string("just foo")));
    EXPECT_TRUE(m6(std::string("just bar")));
    EXPECT_FALSE(m6(std::string("just baz")));

    // Optional character: colou?r
    PartialRegexMatcher m7("colou?r");
    EXPECT_TRUE(m7(std::string("my color")));     // no 'u'
    EXPECT_TRUE(m7(std::string("my colour")));    // with 'u'
    EXPECT_FALSE(m7(std::string("my colouur")));  // two 'u's — doesn't match

    // Optional with *: ab*c
    PartialRegexMatcher m8("ab*c");
    EXPECT_TRUE(m8(std::string("xacy")));    // zero 'b'
    EXPECT_TRUE(m8(std::string("xabcy")));   // one 'b'
    EXPECT_TRUE(m8(std::string("xabbcy")));  // two 'b'

    // Inline flags: (?i)error
    PartialRegexMatcher m9("(?i)error");
    EXPECT_TRUE(m9(std::string("ERROR happened")));
    EXPECT_TRUE(m9(std::string("Error Log")));

    // Group with alternation: (cat|dog)food
    PartialRegexMatcher m10("(cat|dog)food");
    EXPECT_TRUE(m10(std::string("buy catfood")));
    EXPECT_TRUE(m10(std::string("buy dogfood")));
    EXPECT_FALSE(m10(std::string("buy hamsterfood")));

    // ── Named group with 'i' in name must NOT be treated as (?i) ──
    // Regression: (?P<id>...) was falsely triggering case-insensitive
    // detection because the 'i' in 'id' matched the flag scanner.
    PartialRegexMatcher named_i("(?P<id>[a-z]+)xyz");
    EXPECT_TRUE(named_i(std::string("abcxyz")));
    EXPECT_FALSE(named_i(std::string("ABCxyz")));  // case sensitive!

    PartialRegexMatcher named_i2("(?P<item>\\d+)abc");
    EXPECT_TRUE(named_i2(std::string("123abc")));
    EXPECT_FALSE(named_i2(std::string("123ABC")));  // case sensitive!

    // Actual (?i) flag should still work
    PartialRegexMatcher real_ci("(?i)abc");
    EXPECT_TRUE(real_ci(std::string("ABC")));

    // (?mi) — multiline + case-insensitive
    PartialRegexMatcher multi_ci("(?mi)^hello");
    EXPECT_TRUE(multi_ci(std::string("world\nHELLO")));

    // ── Group penetration: abc(de)fg should match as "abcdefg" ──
    PartialRegexMatcher gp1("abc(de)fg");
    EXPECT_TRUE(gp1(std::string("xabcdefgy")));
    EXPECT_FALSE(gp1(std::string("xabcfgy")));  // "de" is required

    // Non-capturing group: abc(?:de)fg
    PartialRegexMatcher gp2("abc(?:de)fg");
    EXPECT_TRUE(gp2(std::string("xabcdefgy")));
    EXPECT_FALSE(gp2(std::string("xabcfgy")));

    // Optional group: abc(de)?fg — "de" is optional
    PartialRegexMatcher gp3("abc(de)?fg");
    EXPECT_TRUE(gp3(std::string("xabcdefgy")));
    EXPECT_TRUE(gp3(std::string("xabcfgy")));  // "de" absent, still matches

    // Quantified group: abc(de)+fg — "de" required (1+)
    PartialRegexMatcher gp4("abc(de)+fg");
    EXPECT_TRUE(gp4(std::string("xabcdefgy")));
    EXPECT_TRUE(gp4(std::string("xabcdededefgy")));
    EXPECT_FALSE(gp4(std::string("xabcfgy")));

    // Nested required groups: abc((de)fg(hi))jk
    PartialRegexMatcher gp5("abc((de)fg(hi))jk");
    EXPECT_TRUE(gp5(std::string("abcdefghijk")));
    EXPECT_FALSE(gp5(std::string("abcdefgjk")));  // missing "hi"

    // Escaped hyphen \- treated as literal
    PartialRegexMatcher esc_hyphen("a\\-b");
    EXPECT_TRUE(esc_hyphen(std::string("xa-by")));
    EXPECT_FALSE(esc_hyphen(std::string("xaby")));

    // ── Unicode property \p{...} ──
    // \p{Han} matches CJK characters
    PartialRegexMatcher m11("\\p{Han}+foo");
    EXPECT_TRUE(
        m11(std::string("\xe4\xb8\xad"
                        "foo")));  // 中foo
    EXPECT_TRUE(
        m11(std::string("\xe4\xb8\xad\xe6\x96\x87"
                        "foo")));              // 中文foo
    EXPECT_FALSE(m11(std::string("abcfoo")));  // no Han chars

    // \p{Latin} matches Latin characters
    PartialRegexMatcher m12("\\p{Latin}+123");
    EXPECT_TRUE(m12(std::string("abc123")));
    EXPECT_FALSE(m12(std::string("123123")));

    // Named group (?P<name>...)
    PartialRegexMatcher m13("(?P<user>[a-z]+)@host");
    EXPECT_TRUE(m13(std::string("alice@host")));
    EXPECT_FALSE(m13(std::string("ALICE@host")));  // lowercase only

    // dot_nl: dot matches newline (RE2 dot_nl=true)
    PartialRegexMatcher m14("start.*end");
    EXPECT_TRUE(m14(std::string("start\nmiddle\nend")));

    // ── Quantifiers {n}, {n,m}, {n,} ──
    PartialRegexMatcher q1("a{3}");
    EXPECT_TRUE(q1(std::string("xaaay")));
    EXPECT_FALSE(q1(std::string("xaay")));

    PartialRegexMatcher q2("a{2,4}b");
    EXPECT_TRUE(q2(std::string("aab")));
    EXPECT_TRUE(q2(std::string("aaaab")));
    EXPECT_FALSE(q2(std::string("ab")));
    // "aaaaab" matches because PartialMatch finds substring "aaaab" (4 a's + b)
    EXPECT_TRUE(q2(std::string("aaaaab")));

    PartialRegexMatcher q3("x{1,}y");
    EXPECT_TRUE(q3(std::string("xy")));
    EXPECT_TRUE(q3(std::string("xxxy")));
    EXPECT_FALSE(q3(std::string("y")));

    // ── Non-capturing group (?:...) ──
    PartialRegexMatcher nc1("(?:abc)+");
    EXPECT_TRUE(nc1(std::string("abcabc")));
    EXPECT_TRUE(nc1(std::string("xabcy")));
    EXPECT_FALSE(nc1(std::string("xyz")));

    // ── Control character escapes \n, \t, \r ──
    PartialRegexMatcher ctl1("hello\\tworld");
    EXPECT_TRUE(ctl1(std::string("hello\tworld")));
    EXPECT_FALSE(ctl1(std::string("hellotworld")));  // literal t, not tab

    PartialRegexMatcher ctl2("line1\\nline2");
    EXPECT_TRUE(ctl2(std::string("line1\nline2")));
    EXPECT_FALSE(ctl2(std::string("line1nline2")));

    // ── Hex escape \x41 = 'A' ──
    PartialRegexMatcher hex1("\\x41BC");
    EXPECT_TRUE(hex1(std::string("ABC")));
    EXPECT_FALSE(hex1(std::string("xBC")));

    // ── Negated character class [^...] ──
    PartialRegexMatcher neg1("[^0-9]+abc");
    EXPECT_TRUE(neg1(std::string("xxxabc")));
    EXPECT_FALSE(neg1(std::string("123abc")));  // digits only before abc

    // ── Complex real-world patterns ──
    // Email-like
    PartialRegexMatcher email("[a-zA-Z0-9.]+@[a-zA-Z0-9]+\\.[a-zA-Z]{2,}");
    EXPECT_TRUE(email(std::string("user@example.com")));
    EXPECT_TRUE(email(std::string("contact user@test.org today")));
    EXPECT_FALSE(email(std::string("user@.com")));

    // IP address-like
    PartialRegexMatcher ip("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
    EXPECT_TRUE(ip(std::string("192.168.1.1")));
    EXPECT_TRUE(ip(std::string("server at 10.0.0.1 is down")));
    EXPECT_FALSE(ip(std::string("192.168.1")));

    // Phone number
    PartialRegexMatcher phone("\\d{3}-\\d{3}-\\d{4}");
    EXPECT_TRUE(phone(std::string("call 555-123-4567 now")));
    EXPECT_FALSE(phone(std::string("call 55-123-4567 now")));

    // ── Multiline flag (?m) ──
    PartialRegexMatcher ml("(?m)^ERROR");
    EXPECT_TRUE(ml(std::string("OK\nERROR: fail")));
    EXPECT_TRUE(ml(std::string("ERROR at start")));

    // ── Edge cases ──
    // Empty string matching
    PartialRegexMatcher empty_pat("");
    EXPECT_TRUE(empty_pat(std::string("anything")));
    EXPECT_TRUE(empty_pat(std::string("")));

    // Only metacharacters
    PartialRegexMatcher only_meta(".*");
    EXPECT_TRUE(only_meta(std::string("anything")));
    EXPECT_TRUE(only_meta(std::string("")));

    // ^$ matches only empty string in PartialMatch
    PartialRegexMatcher caret_dollar("^$");
    EXPECT_TRUE(caret_dollar(std::string("")));
    EXPECT_FALSE(caret_dollar(std::string("notempty")));

    // Escaped backslash
    PartialRegexMatcher esc_bs("a\\\\b");
    EXPECT_TRUE(esc_bs(std::string("a\\b")));
    EXPECT_FALSE(esc_bs(std::string("a/b")));

    // Alternation in group
    PartialRegexMatcher alt_grp("(https?|ftp)://");
    EXPECT_TRUE(alt_grp(std::string("https://example.com")));
    EXPECT_TRUE(alt_grp(std::string("ftp://files.com")));
    EXPECT_FALSE(alt_grp(std::string("smtp://mail.com")));

    // Unicode: mixed CJK and ASCII
    PartialRegexMatcher cjk("\\p{Han}+[0-9]+");
    EXPECT_TRUE(
        cjk(std::string("\xe4\xb8\xad\xe6\x96\x87"
                        "123")));  // 中文123
    EXPECT_FALSE(cjk(std::string("abc123")));
}

// Tests ported from ClickHouse's regex edge cases
// (gtest_optimize_re.cpp, 00144_empty_regexp.sql, 00218_like_regexp_newline.sql)
TEST(StringExpr, RegexMatchClickHouseEdgeCases) {
    // ── Empty pattern matches everything (ClickHouse: match('Hello','')=1) ──
    PartialRegexMatcher empty("");
    EXPECT_TRUE(empty(std::string("Hello")));
    EXPECT_TRUE(empty(std::string("")));
    EXPECT_TRUE(empty(std::string("anything at all")));

    // ── ".*" and ".*?" match everything ──
    PartialRegexMatcher dotstar(".*");
    EXPECT_TRUE(dotstar(std::string("")));
    EXPECT_TRUE(dotstar(std::string("abc")));
    EXPECT_TRUE(dotstar(std::string("abc\ndef")));

    PartialRegexMatcher dotstar_lazy(".*?");
    EXPECT_TRUE(dotstar_lazy(std::string("")));
    EXPECT_TRUE(dotstar_lazy(std::string("abc")));

    // ── dot_nl: . matches \n (RE2 dot_nl=true) ──
    // ClickHouse default: . matches \n
    PartialRegexMatcher dot_nl1("c.d");
    EXPECT_TRUE(dot_nl1(std::string("abc\ndef")));  // c\nd matches c.d
    EXPECT_TRUE(dot_nl1(std::string("abcXdef")));   // cXd matches

    PartialRegexMatcher dot_nl2("abc.def");
    EXPECT_TRUE(dot_nl2(std::string("abc\ndef")));  // \n matches .
    EXPECT_TRUE(dot_nl2(std::string("abcXdef")));

    // Multi-line with . spanning \n
    PartialRegexMatcher dot_nl3("a.*z");
    EXPECT_TRUE(dot_nl3(std::string("a\n\n\nz")));  // .* spans multiple \n

    // ── (?-s) disables dot_nl: . should NOT match \n ──
    PartialRegexMatcher no_dot_nl("(?-s)c.d");
    EXPECT_FALSE(no_dot_nl(std::string("abc\ndef")));  // \n does NOT match .
    EXPECT_TRUE(no_dot_nl(std::string("abcXdef")));    // X matches .

    // ── Alternation edge cases ──
    // Simple alternation
    PartialRegexMatcher alt1("abc|xyz");
    EXPECT_TRUE(alt1(std::string("xxxabcyyy")));
    EXPECT_TRUE(alt1(std::string("xxxxyzzz")));  // xyz at end
    EXPECT_FALSE(alt1(std::string("xxxyyy")));

    // Alternation with empty branch: (abc|) matches everything
    PartialRegexMatcher alt_empty("(abc|)");
    EXPECT_TRUE(alt_empty(std::string("xyz")));  // empty branch matches
    EXPECT_TRUE(alt_empty(std::string("abc")));

    // Three-way alternation
    PartialRegexMatcher alt3("abc|fgk|xyz");
    EXPECT_TRUE(alt3(std::string("fgk")));
    EXPECT_TRUE(alt3(std::string("xxxyz")));  // xyz at end
    EXPECT_FALSE(alt3(std::string("hello")));

    // ── Optional group: (de)?  ──
    PartialRegexMatcher opt_grp("abc(de)?fg");
    EXPECT_TRUE(opt_grp(std::string("abcdefg")));  // with group
    EXPECT_TRUE(opt_grp(std::string("abcfg")));    // without group
    EXPECT_FALSE(opt_grp(std::string("abceg")));

    // ── Quantified group: (abc){2,3} ──
    PartialRegexMatcher rep_grp("(ab){2,3}c");
    EXPECT_TRUE(rep_grp(std::string("ababc")));
    EXPECT_TRUE(rep_grp(std::string("abababc")));
    EXPECT_FALSE(rep_grp(std::string("abc")));

    // ── Named groups — three syntax variants (ClickHouse tests all three) ──
    // (?P<name>...) — Python/RE2 style
    PartialRegexMatcher ng1("(?P<num>\\d+)abc");
    EXPECT_TRUE(ng1(std::string("123abc")));
    EXPECT_FALSE(ng1(std::string("xyzabc")));

    // ── Unicode multi-byte characters ──
    // ¥ (U+00A5) — use RE2 Unicode codepoint syntax \x{00A5}
    PartialRegexMatcher yen("\\x{00A5}\\d+");
    EXPECT_TRUE(
        yen(std::string("\xc2\xa5"
                        "100")));  // ¥100
    EXPECT_FALSE(yen(std::string("$100")));

    // Emoji: 😀 (U+1F600) — RE2 hex codepoint
    PartialRegexMatcher emoji(".+\\x{1F600}");
    EXPECT_TRUE(emoji(std::string("hello\xf0\x9f\x98\x80")));  // hello😀
    EXPECT_FALSE(emoji(std::string("hello")));

    // ── Strings with embedded \n, \t, \r ──
    PartialRegexMatcher tab("a\\tb");
    EXPECT_TRUE(tab(std::string("a\tb")));
    EXPECT_FALSE(tab(std::string("a b")));
    EXPECT_FALSE(tab(std::string("atb")));

    PartialRegexMatcher cr("a\\rb");
    EXPECT_TRUE(cr(std::string("a\rb")));

    // ── Non-greedy quantifiers ──
    PartialRegexMatcher lazy("a.+?b");
    EXPECT_TRUE(lazy(std::string("aXb")));
    EXPECT_TRUE(lazy(std::string("aXXXb")));

    // ── Nested groups ──
    PartialRegexMatcher nested("((a)(b(c)))");
    EXPECT_TRUE(nested(std::string("xabcy")));
    EXPECT_FALSE(nested(std::string("xacy")));

    // ── Backreference rejected by RE2 ──
    // \1 should fail to compile in RE2
    EXPECT_THROW(PartialRegexMatcher("(a)\\1"), std::exception);

    // ── Very long literal match ──
    std::string long_lit(200, 'a');
    PartialRegexMatcher long_m(long_lit);
    EXPECT_TRUE(long_m(std::string(200, 'a')));
    EXPECT_TRUE(long_m(std::string(300, 'a')));
    EXPECT_FALSE(long_m(std::string(199, 'a')));

    // ── Escaped metacharacters as literals ──
    PartialRegexMatcher esc1("a\\.b\\.c");
    EXPECT_TRUE(esc1(std::string("a.b.c")));
    EXPECT_FALSE(esc1(std::string("aXbXc")));

    PartialRegexMatcher esc2("\\(\\)\\[\\]\\{\\}");
    EXPECT_TRUE(esc2(std::string("()[]{}stuff")));
    EXPECT_FALSE(esc2(std::string("stuff")));

    // ── \b word boundary ──
    PartialRegexMatcher wb("\\berror\\b");
    EXPECT_TRUE(wb(std::string("an error occurred")));
    EXPECT_FALSE(wb(std::string("noerrorhere")));

    // ── Case sensitivity ──
    PartialRegexMatcher cs("Hello");
    EXPECT_TRUE(cs(std::string("say Hello")));
    EXPECT_FALSE(cs(std::string("say hello")));  // case sensitive

    PartialRegexMatcher ci("(?i)hello");
    EXPECT_TRUE(ci(std::string("say HELLO")));
    EXPECT_TRUE(ci(std::string("say hello")));
    EXPECT_TRUE(ci(std::string("say Hello")));
}

namespace {

// Canonical plan text: same-op children flattened and sorted, so RE2's
// factoring and AND ordering do not leak into the expectations.
std::string
DescribeNgramRegexPlan(const RegexLiteralCondition& plan) {
    using Op = RegexLiteralCondition::Op;
    if (plan.IsTrue()) {
        return "TRUE";
    }
    if (plan.op == Op::Literal) {
        return plan.literal;
    }
    std::vector<std::string> parts;
    std::vector<const RegexLiteralCondition*> pending{&plan};
    while (!pending.empty()) {
        const auto* node = pending.back();
        pending.pop_back();
        for (const auto& child : node->children) {
            if (child.op == plan.op) {
                pending.push_back(&child);
            } else {
                parts.push_back(DescribeNgramRegexPlan(child));
            }
        }
    }
    std::sort(parts.begin(), parts.end());
    std::string result = "(";
    for (size_t i = 0; i < parts.size(); ++i) {
        result += i == 0 ? "" : plan.op == Op::And ? " & " : " | ";
        result += parts[i];
    }
    return result + ")";
}

}  // namespace

// Direct unit tests for NGRAM regex candidate planning. The plan is the
// bounded RE2 condition (shared with FMINDEX) restricted to literals of at
// least min_gram characters; TRUE means the executor scans. Every planned
// literal MUST appear in any string that matches the regex.
TEST(StringExpr, NgramRegexPlanDirect) {
    using milvus::index::PlanNgramRegex;

    auto expect_plan = [](const std::string& pattern,
                          size_t min_gram,
                          const std::string& expected) {
        EXPECT_EQ(DescribeNgramRegexPlan(PlanNgramRegex(pattern, min_gram)),
                  expected)
            << "pattern: \"" << pattern << "\" min_gram=" << min_gram;
    };

    // ── Literals and AND of required literals ──
    expect_plan("hello", 2, "hello");
    expect_plan("abc def", 2, "abc def");
    expect_plan("abc.*def", 2, "(abc & def)");
    expect_plan("abc+def", 2, "(abc & cdef)");
    expect_plan("^hello$", 2, "hello");
    expect_plan("file\\.txt", 2, "file.txt");
    expect_plan("a\\+b", 2, "a+b");
    expect_plan("a\\\\b", 2, "a\\b");
    expect_plan("\\d+hello", 2, "hello");
    expect_plan("hello\\w+world", 2, "(hello & world)");
    expect_plan("(?P<id>\\d+)abc", 2, "abc");
    expect_plan("colou?r", 2, "colo");
    expect_plan("abc(de)?fg", 2, "(abc & fg)");
    expect_plan("abc(de)?fg", 3, "abc");
    expect_plan("a{3}bc", 2, "aaabc");
    expect_plan("a{2,4}bc", 2, "aabc");
    expect_plan("xa+bc", 2, "(abc & xa)");
    expect_plan("xa+bc", 3, "abc");
    expect_plan("abc(de)fg", 2, "abcdefg");
    expect_plan("abc(de)+fg", 2, "(abcde & defg)");
    expect_plan("\\p{Han}foo", 2, "foo");
    expect_plan("[a-z]+hello", 2, "hello");
    expect_plan("abc[0-9]def", 2, "(abc & def)");
    // The old hand-written extractor read `{10,20}` after `.` as the literal
    // "10,20", which dropped matching rows.
    expect_plan("foo.{10,20}bar", 3, "(bar & foo)");

    // ── Alternation: AND inside a branch, OR across branches ──
    expect_plan("foo|bar", 2, "(bar | foo)");
    expect_plan("^(cat|dog)$", 3, "(cat | dog)");
    expect_plan("(cat|dog)food", 3, "((cat | dog) & food)");
    expect_plan("(ERROR|WARN).*timeout", 3, "((ERROR | WARN) & timeout)");
    expect_plan("(?:cat|dog)(?:food|toy)", 3, "((cat | dog) & (food | toy))");
    expect_plan("abc(de|fg)hi", 2, "((de | fg) & abc & hi)");
    // A short OR inside an AND is dropped; the AND keeps its other literals.
    expect_plan("abc(de|fg)hi", 3, "abc");
    expect_plan("hello|\\bworld", 3, "(hello | world)");
    expect_plan("你好|世界", 2, "(世界 | 你好)");

    // ── Escaped and quoted `|` are literal bytes, not alternation ──
    expect_plan("a\\|bcd", 2, "a|bcd");
    expect_plan("\\Qa|b\\E|tested", 3, "(a|b | tested)");

    // ── A branch without a usable literal disables pruning ──
    expect_plan("cat|do", 2, "(cat | do)");
    expect_plan("cat|do", 3, "TRUE");
    expect_plan("你好|世界", 3, "TRUE");
    expect_plan("foo|", 2, "TRUE");
    expect_plan("hello|(?i)world", 2, "TRUE");

    // ── Case folding, empty matches and no literal → scan ──
    expect_plan("(?i)hello", 2, "TRUE");
    expect_plan("ab*c", 2, "TRUE");
    expect_plan("", 2, "TRUE");
    expect_plan(".*", 2, "TRUE");
    expect_plan("[a-z]+", 2, "TRUE");
    expect_plan("\\d+", 2, "TRUE");
    expect_plan("(?:hello|tested)?", 2, "TRUE");

    // ── Over the 127-node budget → scan, never a dropped branch ──
    // Distinct leading bytes keep RE2 from factoring the branches: each one
    // costs three nodes (AND of two literals) plus one binary OR node.
    auto branches = [](size_t count) {
        const std::string heads =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        std::string pattern;
        for (size_t i = 0; i < count; ++i) {
            const auto head = heads.substr(i, 1);
            pattern += (i == 0 ? "" : "|") + head + "q.*z" + head + "w";
        }
        return pattern;
    };
    expect_plan(branches(2), 2, "((aq & zaw) | (bq & zbw))");
    EXPECT_EQ(PlanNgramRegex(branches(32), 2).NodeCount(),
              RegexLiteralCondition::kMaxNodes);
    expect_plan(branches(33), 2, "TRUE");
    expect_plan(branches(60), 2, "TRUE");

    EXPECT_ANY_THROW(PlanNgramRegex("(", 2));

    // ── Verify no false negatives: if RE2 PartialMatch succeeds, the plan
    //    evaluated as substring membership must accept the input. ──
    auto verify_no_false_neg = [](const std::string& pattern,
                                  const std::string& input) {
        PartialRegexMatcher matcher(pattern);
        if (!matcher(input)) {
            return;
        }
        for (size_t min_gram = 1; min_gram <= 4; ++min_gram) {
            const auto plan = PlanNgramRegex(pattern, min_gram);
            EXPECT_TRUE(plan.Evaluate(
                [&](const std::string& literal) {
                    return input.find(literal) != std::string::npos;
                },
                [] { return true; }))
                << "FALSE NEGATIVE: pattern=\"" << pattern << "\" input=\""
                << input << "\" min_gram=" << min_gram;
        }
    };

    verify_no_false_neg("error.*timeout", "error: connection timeout");
    verify_no_false_neg("colou?r", "color red");
    verify_no_false_neg("colou?r", "colour blue");
    verify_no_false_neg("\\p{Han}+foo",
                        "\xe4\xb8\xad"
                        "foo");
    verify_no_false_neg("(?P<id>\\d+)abc", "123abc");
    verify_no_false_neg("a{3}bc", "aaabc");
    verify_no_false_neg("file\\.txt", "open file.txt now");
    verify_no_false_neg("abc(de)fg", "xabcdefgy");
    verify_no_false_neg("abc(de)?fg", "xabcfgy");
    verify_no_false_neg("foo.{10,20}bar", "foo0123456789bar");
    verify_no_false_neg("cat|dog", "hotdog");
    verify_no_false_neg("(ERROR|WARN).*timeout", "WARN: read timeout");
    verify_no_false_neg("^(cat|dog)$", "cat");
    verify_no_false_neg("a\\|bcd", "xa|bcdx");
    verify_no_false_neg("abc(de|fg)hi", "abcfghi");
    verify_no_false_neg("(?:cat|dog)(?:food|toy)", "dogtoy");
    verify_no_false_neg("你好|世界", "世界和平");
    verify_no_false_neg(branches(32), "xFq--zFwx");
    verify_no_false_neg(branches(60), "xZq--zZwx");
}
