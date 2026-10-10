// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <boost/format.hpp>
#include <boost/optional/optional.hpp>
#include <folly/FBVector.h>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "ExprTestBase.h"
#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestJsonBigIntModulo) {
    // test (bigint mod 10 == 0)
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto insert_data = std::make_unique<InsertRecordProto>();
    {
        // insert pk fid
        auto field_meta = schema->operator[](int64_fid);
        std::vector<int64_t> data(N);
        for (int i = 0; i < N; i++) {
            data[i] = i;
        }
        InsertCol(insert_data.get(), data, field_meta, false);
    }

    BitsetType expect(N, false);
    {
        auto field_meta = schema->operator[](json_fid);
        std::vector<std::string> data(N);

        auto start = 1ULL << 54;
        for (int i = 0; i < N; i++) {
            data[i] = R"({"meta":)" + std::to_string(start + i) + "}";
            if ((start + i) % 10 == 0) {
                expect.set(i);
            }
        }
        InsertCol(insert_data.get(), data, field_meta, false);
    }

    GeneratedData raw_data;
    raw_data.schema_ = schema;
    raw_data.raw_ = insert_data.release();
    raw_data.raw_->set_num_rows(N);
    for (int i = 0; i < N; ++i) {
        raw_data.row_ids_.push_back(i);
        raw_data.timestamps_.push_back(i);
    }

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    proto::plan::GenericValue val1;
    val1.set_int64_val(10);
    proto::plan::GenericValue val2;
    val2.set_int64_val(0);
    auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"meta"}),
        proto::plan::OpType::Equal,
        proto::plan::ArithOpType::Mod,
        val2,
        val1);

    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), expect.size())
        << "final size: " << final.size() << " expect size: " << expect.size();
    for (auto i = 0; i < final.size(); i++) {
        EXPECT_EQ(final[i], expect[i])
            << "i: " << i << " final: " << final[i] << " expect: " << expect[i];
    }
}

TEST_P(ExprTest, TestMultipleEqualitiesOrOptimization) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField("bool", DataType::BOOL);
    schema->AddDebugField("bool1", DataType::BOOL);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int81", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("int161", DataType::INT16);
    schema->AddDebugField("int32", DataType::INT32);
    schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->AddDebugField("int641", DataType::INT64);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->AddDebugField("float1", DataType::FLOAT);
    schema->AddDebugField("double", DataType::DOUBLE);
    schema->AddDebugField("double1", DataType::DOUBLE);
    schema->AddDebugField("string1", DataType::VARCHAR);
    schema->AddDebugField("string2", DataType::VARCHAR);
    schema->AddDebugField("json", DataType::JSON, false);
    schema->AddDebugField("str_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(pk);

    auto seg = CreateSealedSegment(schema);
    size_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int index) -> expr::TypedExprPtr {
        switch (index) {
            case 0: {
                proto::plan::GenericValue val1;
                val1.set_int64_val(100);
                auto expr1 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val1);
                proto::plan::GenericValue val2;
                val2.set_int64_val(200);
                auto expr2 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val2);
                auto expr3 = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::Or, expr1, expr2);
                proto::plan::GenericValue val3;
                val3.set_int64_val(300);
                auto expr4 = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::Equal,
                    val3);
                return std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::Or, expr3, expr4);
            };
            default:
                ThrowInfo(ErrorCode::UnexpectedError, "not implement");
        }
    };

    auto expr = build_expr(0);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final1 = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    auto prev_optimize_expr_enabled = OPTIMIZE_EXPR_ENABLED.load();
    OPTIMIZE_EXPR_ENABLED.store(false);
    auto final2 = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final1.size(), final2.size());
    for (auto i = 0; i < final1.size(); i++) {
        EXPECT_EQ(final1[i], final2[i]);
    }
    OPTIMIZE_EXPR_ENABLED.store(prev_optimize_expr_enabled,
                                std::memory_order_release);
}

TEST(Expr, TestNotPreservesNulls) {
    auto schema = std::make_shared<Schema>();
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, true);
    schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16, true);
    schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, true);
    schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);
    schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT, true);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE, true);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    auto seg = CreateSealedSegment(schema);
    FixedVector<bool> valid_data_i8;
    FixedVector<bool> valid_data_i16;
    FixedVector<bool> valid_data_i32;
    FixedVector<bool> valid_data_i64;
    FixedVector<bool> valid_data_str;
    FixedVector<bool> valid_data_float;
    FixedVector<bool> valid_data_double;
    int N = 1000;
    test::ExprBatchSizeGuard batch_guard(N);
    auto raw_data = DataGen(schema, N);
    valid_data_i8 = raw_data.get_col_valid(int8_fid);
    valid_data_i16 = raw_data.get_col_valid(int16_fid);
    valid_data_i32 = raw_data.get_col_valid(int32_fid);
    valid_data_i64 = raw_data.get_col_valid(int64_fid);
    valid_data_str = raw_data.get_col_valid(str2_fid);
    valid_data_float = raw_data.get_col_valid(float_fid);
    valid_data_double = raw_data.get_col_valid(double_fid);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    enum ExprType {
        UnaryRangeExpr = 0,
        TermExprImpl = 1,
        CompareExpr = 2,
        LogicalUnaryExpr = 3,
        BinaryRangeExpr = 4,
        LogicalBinaryExpr = 5,
        BinaryArithOpEvalRangeExpr = 6,
    };

    auto build_unary_range_expr = [&](DataType data_type,
                                      int64_t value) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_int64_val(value);
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_float_val(float(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val;
            val.set_string_val(std::to_string(value));
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::LessThan,
                val,
                std::vector<proto::plan::GenericValue>{});
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_binary_range_expr = [&](DataType data_type,
                                       int64_t low,
                                       int64_t high) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(low);
            proto::plan::GenericValue val2;
            val2.set_int64_val(high);
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(low));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(high));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else if (IsStringDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_string_val(std::to_string(low));
            proto::plan::GenericValue val2;
            val2.set_string_val(std::to_string(low));
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                val1,
                val2,
                true,
                true);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_compare_expr = [&](DataType data_type) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type) || IsFloatDataType(data_type) ||
            IsStringDataType(data_type)) {
            return std::make_shared<expr::CompareExpr>(
                fids[data_type],
                fids[data_type],
                data_type,
                data_type,
                proto::plan::OpType::LessThan);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 10);
        auto child2_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
    };

    auto build_multi_logical_binary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child1_expr = build_unary_range_expr(data_type, 100);
        auto child2_expr = build_unary_range_expr(data_type, 100);
        auto child3_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child4_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        auto child5_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        auto child6_expr = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child3_expr, child4_expr);
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child5_expr, child6_expr);
    };

    auto build_arith_op_expr = [&](DataType data_type,
                                   int64_t right_val,
                                   int64_t val) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_int64_val(right_val);
            proto::plan::GenericValue val2;
            val2.set_int64_val(val);
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else if (IsFloatDataType(data_type)) {
            proto::plan::GenericValue val1;
            val1.set_float_val(float(right_val));
            proto::plan::GenericValue val2;
            val2.set_float_val(float(val));
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(fids[data_type], data_type),
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Add,
                val1,
                val2);
        } else {
            throw std::runtime_error("not supported type");
        }
    };

    auto build_logical_unary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
    };

    auto test_ans = [=, &seg](expr::TypedExprPtr expr,
                              FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto positive_plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto positive = milvus::test::gen_filter_res(
            positive_plan.get(), seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(positive->size(), N);
        BitsetTypeView positive_bits(positive->GetRawData(), N);
        auto negated = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, expr);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, negated);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        ASSERT_EQ(view.size(), N / 2);
        BitsetTypeView offset_validity(col_vec->GetValidRawData(),
                                       col_vec->size());
        for (int i = 0; i < N; i++) {
            const bool expected = valid_data[i] && !positive_bits[i];
            EXPECT_EQ(final[i], expected) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[i / 2], expected) << i;
                EXPECT_EQ(offset_validity[i / 2], valid_data[i]) << i;
            }
        }
    };

    auto expr = build_unary_range_expr(DataType::INT8, 10);
    test_ans(expr, valid_data_i8);
    expr = build_unary_range_expr(DataType::INT16, 10);
    test_ans(expr, valid_data_i16);
    expr = build_unary_range_expr(DataType::INT32, 10);
    test_ans(expr, valid_data_i32);
    expr = build_unary_range_expr(DataType::INT64, 10);
    test_ans(expr, valid_data_i64);
    expr = build_unary_range_expr(DataType::FLOAT, 10);
    test_ans(expr, valid_data_float);
    expr = build_unary_range_expr(DataType::DOUBLE, 10);
    test_ans(expr, valid_data_double);
    expr = build_unary_range_expr(DataType::VARCHAR, 10);
    test_ans(expr, valid_data_str);

    expr = build_binary_range_expr(DataType::INT8, 10, 100);
    test_ans(expr, valid_data_i8);
    expr = build_binary_range_expr(DataType::INT16, 10, 100);
    test_ans(expr, valid_data_i16);
    expr = build_binary_range_expr(DataType::INT32, 10, 100);
    test_ans(expr, valid_data_i32);
    expr = build_binary_range_expr(DataType::INT64, 10, 100);
    test_ans(expr, valid_data_i64);
    expr = build_binary_range_expr(DataType::FLOAT, 10, 100);
    test_ans(expr, valid_data_float);
    expr = build_binary_range_expr(DataType::DOUBLE, 10, 100);
    test_ans(expr, valid_data_double);
    expr = build_binary_range_expr(DataType::VARCHAR, 10, 100);
    test_ans(expr, valid_data_str);

    expr = build_compare_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_compare_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_compare_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_compare_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_compare_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_compare_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_compare_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_arith_op_expr(DataType::INT8, 10, 100);
    test_ans(expr, valid_data_i8);
    expr = build_arith_op_expr(DataType::INT16, 10, 100);
    test_ans(expr, valid_data_i16);
    expr = build_arith_op_expr(DataType::INT32, 10, 100);
    test_ans(expr, valid_data_i32);
    expr = build_arith_op_expr(DataType::INT64, 10, 100);
    test_ans(expr, valid_data_i64);
    expr = build_arith_op_expr(DataType::FLOAT, 10, 100);
    test_ans(expr, valid_data_float);
    expr = build_arith_op_expr(DataType::DOUBLE, 10, 100);
    test_ans(expr, valid_data_double);

    expr = build_logical_unary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_logical_unary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_logical_unary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_logical_unary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_logical_unary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_logical_unary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_logical_unary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_logical_binary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_logical_binary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_logical_binary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_logical_binary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_logical_binary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_logical_binary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_logical_binary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);

    expr = build_multi_logical_binary_expr(DataType::INT8);
    test_ans(expr, valid_data_i8);
    expr = build_multi_logical_binary_expr(DataType::INT16);
    test_ans(expr, valid_data_i16);
    expr = build_multi_logical_binary_expr(DataType::INT32);
    test_ans(expr, valid_data_i32);
    expr = build_multi_logical_binary_expr(DataType::INT64);
    test_ans(expr, valid_data_i64);
    expr = build_multi_logical_binary_expr(DataType::FLOAT);
    test_ans(expr, valid_data_float);
    expr = build_multi_logical_binary_expr(DataType::DOUBLE);
    test_ans(expr, valid_data_double);
    expr = build_multi_logical_binary_expr(DataType::VARCHAR);
    test_ans(expr, valid_data_str);
}

TEST_P(ExprTest, TestPrimaryKeyTermFilter) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("string1", DataType::VARCHAR);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    std::vector<proto::plan::GenericValue> retrieve_ints;
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i);
        retrieve_ints.push_back(val);
    }
    auto expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    BitsetType final;
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(final[i], true);
    }
    for (int i = 10; i < N; ++i) {
        EXPECT_EQ(final[i], false);
    }
    retrieve_ints.clear();
    for (int i = 0; i < 10; ++i) {
        proto::plan::GenericValue val;
        val.set_int64_val(i + N);
        retrieve_ints.push_back(val);
    }
    expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), retrieve_ints);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.size(), N);

    // specify some offsets and do scalar filtering on these offsets
    milvus::exec::OffsetVector offsets;
    offsets.reserve(N / 2);
    for (auto i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            offsets.emplace_back(i);
        }
    }
    auto col_vec = milvus::test::gen_filter_res(
        plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    EXPECT_EQ(view.size(), N / 2);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(final[i], false);
        if (i % 2 == 0) {
            EXPECT_EQ(view[int(i / 2)], false);
        }
    }
}

TEST_P(ExprTest, TestGrowingSegmentExpressionBatches) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    const auto values = raw_data.get_col<int8_t>(int8_fid);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                std::make_shared<InsertRecordProto>(*raw_data.raw_));

    proto::plan::GenericValue val;
    val.set_int64_val(10);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(int8_fid, DataType::INT8),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>{});
    auto plan_node =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

    std::vector<int64_t> test_batch_size = {
        1, 128, 250, 333, 1000, 8192, 10240, 20480, 30720, 40960,
        102400, 204800, 307200};

    for (const auto& batch_size : test_batch_size) {
        test::ExprBatchSizeGuard batch_guard(batch_size);
        const auto evaluated = test::EvalExprInBatches(expr, seg.get(), N);
        std::vector<int64_t> expected_batches;
        expected_batches.reserve(upper_div(N, batch_size));
        for (int64_t row = 0; row < N; row += batch_size) {
            expected_batches.push_back(std::min<int64_t>(batch_size, N - row));
        }
        EXPECT_EQ(evaluated.batch_sizes, expected_batches);
        ASSERT_EQ(evaluated.result->size(), N);
        BitsetTypeView bits(evaluated.result->GetRawData(), N);
        BitsetTypeView validity(evaluated.result->GetValidRawData(), N);
        const auto filtered =
            ExecuteQueryExpr(plan_node, seg.get(), N, MAX_TIMESTAMP);
        ASSERT_EQ(filtered.size(), N);
        for (int64_t row = 0; row < N; ++row) {
            EXPECT_TRUE(validity[row]) << row;
            EXPECT_EQ(bits[row], values[row] > 10) << row;
            EXPECT_EQ(filtered[row], values[row] > 10) << row;
        }
    }
}

TEST_P(ExprTest, TestConjunction) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int81", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("int161", DataType::INT16);
    schema->AddDebugField("int32", DataType::INT32);
    schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->AddDebugField("string2", DataType::VARCHAR);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);
    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int l, int r) -> expr::TypedExprPtr {
        ::milvus::proto::plan::GenericValue value;
        value.set_int64_val(l);
        auto left = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});
        value.set_int64_val(r);
        auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_fid, DataType::INT64),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, left, right);
    };

    std::vector<std::pair<int, int>> test_case = {
        {100, 0}, {0, 100}, {8192, 8194}};
    for (auto& pair : test_case) {
        auto expr = build_expr(pair.first, pair.second);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(final[i], pair.first < i && i < pair.second) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[int(i / 2)], pair.first < i && i < pair.second)
                    << i;
            }
        }
    }
}

TEST_P(ExprTest, TestNullableConjunction) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("fakevec", data_type, 16, metric_type);
    schema->AddDebugField("int8", DataType::INT8);
    schema->AddDebugField("int8_nullable", DataType::INT8);
    schema->AddDebugField("int16", DataType::INT16);
    schema->AddDebugField("int16_nullable", DataType::INT16);
    schema->AddDebugField("int32", DataType::INT32);
    schema->AddDebugField("int32_nullable", DataType::INT32);
    schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int64_nullable", DataType::INT64, true);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->AddDebugField("string2", DataType::VARCHAR);
    schema->AddDebugField("float", DataType::FLOAT);
    schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    const auto values = raw_data.get_col<int64_t>(int64_nullable_fid);
    const auto validity = raw_data.get_col_valid(int64_nullable_fid);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    auto build_expr = [&](int l, int r) -> expr::TypedExprPtr {
        ::milvus::proto::plan::GenericValue value;
        value.set_int64_val(l);
        auto left = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_nullable_fid, DataType::INT64),
            proto::plan::OpType::GreaterThan,
            value,
            std::vector<proto::plan::GenericValue>{});
        value.set_int64_val(r);
        auto right = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(int64_nullable_fid, DataType::INT64),
            proto::plan::OpType::LessThan,
            value,
            std::vector<proto::plan::GenericValue>{});

        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, left, right);
    };

    std::vector<std::pair<int, int>> test_case = {
        {100, 0}, {0, 100}, {8192, 8194}};
    for (auto& pair : test_case) {
        auto expr = build_expr(pair.first, pair.second);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final;
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N / 2);
        for (auto i = 0; i < N; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg.get(), N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);
        for (int i = 0; i < N; ++i) {
            const bool expected = validity[i] && pair.first < values[i] &&
                                  values[i] < pair.second;
            EXPECT_EQ(final[i], expected) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[int(i / 2)], expected) << i;
            }
        }
    }
}

TEST(Expr, TestNullPredicatesAcrossScalarTypes) {
    auto schema = std::make_shared<Schema>();
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL, true);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8, true);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16, true);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32, true);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR, true);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT, true);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE, true);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::BOOL, bool_fid},
                                        {DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    std::map<DataType, FieldId> fids_not_nullable = {
        {DataType::BOOL, bool_1_fid},
        {DataType::INT8, int8_1_fid},
        {DataType::INT16, int16_1_fid},
        {DataType::INT32, int32_1_fid},
        {DataType::INT64, int64_1_fid},
        {DataType::VARCHAR, str1_fid},
        {DataType::FLOAT, float_1_fid},
        {DataType::DOUBLE, double_1_fid}};

    auto seg = CreateSealedSegment(schema);
    constexpr int64_t N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    for (const auto& [data_type, nullable_id] : fids) {
        const auto validity = raw_data.get_col_valid(nullable_id);
        for (const auto op : {proto::plan::NullExpr_NullOp_IsNull,
                              proto::plan::NullExpr_NullOp_IsNotNull}) {
            const bool is_null = op == proto::plan::NullExpr_NullOp_IsNull;
            for (const bool nullable : {true, false}) {
                SCOPED_TRACE(::testing::Message()
                             << "type=" << static_cast<int>(data_type)
                             << " nullable=" << nullable << " is_null=" << is_null);
                const auto field_id = nullable ? nullable_id
                                               : fids_not_nullable.at(data_type);
                auto logical = std::make_shared<expr::NullExpr>(
                    expr::ColumnInfo(field_id, data_type, {}, nullable), op);
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, logical);
                const auto final =
                    ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
                ASSERT_EQ(final.size(), N);
                for (int64_t i = 0; i < N; ++i) {
                    const bool valid = !nullable || validity[i];
                    EXPECT_EQ(final[i], is_null ? !valid : valid) << i;
                }
            }
        }
    }
}
