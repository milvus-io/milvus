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

#include "ExprTestBase.h"

EXPR_TEST_INSTANTIATE();

TEST_P(ExprTest, TestBinaryArithOpEvalRangeExpr_forbigint_mod) {
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

TEST_P(ExprTest, TestMutiInConvert) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    auto bool_fid = schema->AddDebugField("bool", DataType::BOOL);
    auto bool_1_fid = schema->AddDebugField("bool1", DataType::BOOL);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, false);
    auto str_array_fid =
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

TEST(Expr, TestExprPerformance) {
    GTEST_SKIP() << "Skip performance test, open it when test performance";
    auto schema = std::make_shared<Schema>();
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    std::map<DataType, FieldId> fids = {{DataType::INT8, int8_fid},
                                        {DataType::INT16, int16_fid},
                                        {DataType::INT32, int32_fid},
                                        {DataType::INT64, int64_fid},
                                        {DataType::VARCHAR, str2_fid},
                                        {DataType::FLOAT, float_fid},
                                        {DataType::DOUBLE, double_fid}};

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

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

    auto build_term_expr =
        [&](DataType data_type,
            std::vector<int64_t> in_vals) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_int64_val(v);
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsFloatDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_float_val(float(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsStringDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_string_val(std::to_string(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
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

    auto build_logical_unary_expr =
        [&](DataType data_type) -> expr::TypedExprPtr {
        auto child_expr = build_unary_range_expr(data_type, 10);
        return std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
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

    auto test_case_base = [=, &seg](expr::TypedExprPtr expr) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 100; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);
        }
    };

    auto expr = build_unary_range_expr(DataType::INT8, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT16, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT32, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::INT64, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::FLOAT, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::DOUBLE, 10);
    test_case_base(expr);
    expr = build_unary_range_expr(DataType::VARCHAR, 10);
    test_case_base(expr);

    expr = build_binary_range_expr(DataType::INT8, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT16, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT32, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::INT64, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::FLOAT, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::DOUBLE, 10, 100);
    test_case_base(expr);
    expr = build_binary_range_expr(DataType::VARCHAR, 10, 100);
    test_case_base(expr);

    expr = build_compare_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_compare_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_compare_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_compare_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_compare_expr(DataType::VARCHAR);
    test_case_base(expr);

    expr = build_arith_op_expr(DataType::INT8, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT16, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT32, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::INT64, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::FLOAT, 10, 100);
    test_case_base(expr);
    expr = build_arith_op_expr(DataType::DOUBLE, 10, 100);
    test_case_base(expr);

    expr = build_logical_unary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_logical_unary_expr(DataType::VARCHAR);
    test_case_base(expr);

    expr = build_logical_binary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_logical_binary_expr(DataType::VARCHAR);
    test_case_base(expr);

    expr = build_multi_logical_binary_expr(DataType::INT8);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT16);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT32);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::INT64);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::FLOAT);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::DOUBLE);
    test_case_base(expr);
    expr = build_multi_logical_binary_expr(DataType::VARCHAR);
    test_case_base(expr);
}

TEST(Expr, TestExprNOT) {
    auto schema = std::make_shared<Schema>();
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

    auto build_term_expr =
        [&](DataType data_type,
            std::vector<int64_t> in_vals) -> expr::TypedExprPtr {
        if (IsIntegerDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_int64_val(v);
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsFloatDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_float_val(float(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
        } else if (IsStringDataType(data_type)) {
            std::vector<proto::plan::GenericValue> vals;
            for (auto& v : in_vals) {
                proto::plan::GenericValue val;
                val.set_string_val(std::to_string(v));
                vals.push_back(val);
            }
            return std::make_shared<expr::TermFilterExpr>(
                expr::ColumnInfo(fids[data_type], data_type), vals, false);
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
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
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

        for (int i = 0; i < N; i++) {
            if (!valid_data[i]) {
                EXPECT_EQ(final[i], false);
                if (i % 2 == 0) {
                    EXPECT_EQ(view[int(i / 2)], false);
                }
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

TEST_P(ExprTest, test_term_pk) {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("Timestamp"),
                     FieldId(1),
                     DataType::INT64,
                     false,
                     std::nullopt);
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
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

TEST_P(ExprTest, TestGrowingSegmentGetBatchSize) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                raw_data.raw_);

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
        8192, 10240, 20480, 30720, 40960, 102400, 204800, 307200};

    for (const auto& batch_size : test_batch_size) {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_size);
        auto plan = plan::PlanFragment(plan_node);
        auto query_context = std::make_shared<milvus::exec::QueryContext>(
            "query id", seg.get(), N, MAX_TIMESTAMP);

        auto task =
            milvus::exec::Task::Create("task_expr", plan, 0, query_context);
        auto last_num = N % batch_size;
        auto iter_num = last_num == 0 ? N / batch_size : N / batch_size + 1;
        int iter = 0;
        for (;;) {
            auto result = task->Next();
            if (!result) {
                break;
            }
            auto childrens = result->childrens();
            if (++iter != iter_num) {
                EXPECT_EQ(childrens[0]->size(), batch_size);
            } else {
                EXPECT_EQ(childrens[0]->size(), last_num);
            }
        }
    }
}

TEST_P(ExprTest, TestConjuctExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
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

TEST_P(ExprTest, TestConjuctExprNullable) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_nullable_fid =
        schema->AddDebugField("int8_nullable", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_nullable_fid =
        schema->AddDebugField("int16_nullable", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_nullable_fid =
        schema->AddDebugField("int32_nullable", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_nullable_fid =
        schema->AddDebugField("int64_nullable", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
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
            EXPECT_EQ(final[i], pair.first < i && i < pair.second) << i;
            if (i % 2 == 0) {
                EXPECT_EQ(view[int(i / 2)], pair.first < i && i < pair.second)
                    << i;
            }
        }
    }
}

TEST_P(ExprTest, TestUnaryBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};
    for (const auto& pair : test_cases) {
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(10);
        } else {
            val.set_int64_val(10);
        }
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 10; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST_P(ExprTest, TestBinaryRangeBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        proto::plan::GenericValue lower;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            lower.set_float_val(10);
        } else {
            lower.set_int64_val(10);
        }
        proto::plan::GenericValue upper;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            upper.set_float_val(45);
        } else {
            upper.set_int64_val(45);
        }
        auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            lower,
            upper,
            true,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 10; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST_P(ExprTest, TestLogicalUnaryBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(10);
        } else {
            val.set_int64_val(10);
        }
        auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::GreaterThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto expr = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 50; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST_P(ExprTest, TestBinaryLogicalBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    // load field data
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(-1000000);
        } else {
            val.set_int64_val(-1000000);
        }
        proto::plan::GenericValue val1;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val1.set_float_val(-100);
        } else {
            val1.set_int64_val(-100);
        }
        auto child1_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::LessThan,
            val,
            std::vector<proto::plan::GenericValue>{});
        auto child2_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::NotEqual,
            val1,
            std::vector<proto::plan::GenericValue>{});
        auto expr = std::make_shared<const expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, child1_expr, child2_expr);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 50; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeBenchExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<std::pair<FieldId, DataType>> test_cases = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE}};

    for (const auto& pair : test_cases) {
        proto::plan::GenericValue val;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            val.set_float_val(100);
        } else {
            val.set_int64_val(100);
        }
        proto::plan::GenericValue right;
        if (pair.second == DataType::FLOAT || pair.second == DataType::DOUBLE) {
            right.set_float_val(10);
        } else {
            right.set_int64_val(10);
        }
        auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(pair.first, pair.second),
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::Add,
            val,
            right);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 50; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST(BitmapIndexTest, PatternMatchTest) {
    // Initialize bitmap index
    using namespace milvus::index;
    BitmapIndex<std::string> index;

    // Add test data
    std::vector<std::string> data = {"apple", "banana", "orange", "pear"};

    // Build index
    index.Build(data.size(), data.data(), nullptr);

    // Create test datasets with different operators
    auto prefix_dataset = std::make_shared<Dataset>();
    prefix_dataset->Set(OPERATOR_TYPE, OpType::PrefixMatch);
    prefix_dataset->Set(MATCH_VALUE, std::string("a"));

    auto contains_dataset = std::make_shared<Dataset>();
    contains_dataset->Set(OPERATOR_TYPE, OpType::InnerMatch);
    contains_dataset->Set(MATCH_VALUE, std::string("an"));

    auto posix_dataset = std::make_shared<Dataset>();
    posix_dataset->Set(OPERATOR_TYPE, OpType::PostfixMatch);
    posix_dataset->Set(MATCH_VALUE, std::string("a"));

    // Execute queries
    auto prefix_result = index.Query(prefix_dataset);
    auto contains_result = index.Query(contains_dataset);
    auto posix_result = index.Query(posix_dataset);

    // Verify results
    EXPECT_TRUE(prefix_result[0]);
    EXPECT_FALSE(prefix_result[2]);

    EXPECT_FALSE(contains_result[0]);
    EXPECT_TRUE(contains_result[1]);
    EXPECT_TRUE(contains_result[2]);

    EXPECT_FALSE(posix_result[0]);
    EXPECT_TRUE(posix_result[1]);
    EXPECT_FALSE(posix_result[2]);

    auto prefix_result2 =
        index.PatternMatch(std::string("a"), OpType::PrefixMatch);
    auto contains_result2 =
        index.PatternMatch(std::string("an"), OpType::InnerMatch);
    auto posix_result2 =
        index.PatternMatch(std::string("a"), OpType::PostfixMatch);

    EXPECT_TRUE(prefix_result == prefix_result2);
    EXPECT_TRUE(contains_result == contains_result2);
    EXPECT_TRUE(posix_result == posix_result2);
}

TEST(Expr, TestExprNull) {
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
    FixedVector<bool> valid_data_bool;
    FixedVector<bool> valid_data_i8;
    FixedVector<bool> valid_data_i16;
    FixedVector<bool> valid_data_i32;
    FixedVector<bool> valid_data_i64;
    FixedVector<bool> valid_data_str;
    FixedVector<bool> valid_data_float;
    FixedVector<bool> valid_data_double;

    int N = 1000;
    auto raw_data = DataGen(schema, N);
    valid_data_bool = raw_data.get_col_valid(bool_fid);
    valid_data_i8 = raw_data.get_col_valid(int8_fid);
    valid_data_i16 = raw_data.get_col_valid(int16_fid);
    valid_data_i32 = raw_data.get_col_valid(int32_fid);
    valid_data_i64 = raw_data.get_col_valid(int64_fid);
    valid_data_str = raw_data.get_col_valid(str2_fid);
    valid_data_float = raw_data.get_col_valid(float_fid);
    valid_data_double = raw_data.get_col_valid(double_fid);

    FixedVector<bool> valid_data_all_true(N, true);

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    auto build_nullable_expr = [&](DataType data_type,
                                   NullExprType op) -> expr::TypedExprPtr {
        return std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(fids[data_type], data_type, {}, true), op);
    };

    auto build_not_nullable_expr = [&](DataType data_type,
                                       NullExprType op) -> expr::TypedExprPtr {
        return std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(
                fids_not_nullable[data_type], data_type, {}, false),
            op);
    };

    auto test_is_null_ans = [=, &seg](expr::TypedExprPtr expr,
                                      FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);
        for (int i = 0; i < N; i++) {
            EXPECT_NE(final[i], valid_data[i]);
        }
    };

    auto test_is_not_null_ans = [=, &seg](expr::TypedExprPtr expr,
                                          FixedVector<bool> valid_data) {
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N);
        for (int i = 0; i < N; i++) {
            EXPECT_EQ(final[i], valid_data[i]);
        }
    };

    auto expr = build_nullable_expr(DataType::BOOL,
                                    proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_bool);
    expr = build_nullable_expr(DataType::INT8,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i8);
    expr = build_nullable_expr(DataType::INT16,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i16);
    expr = build_nullable_expr(DataType::INT32,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i32);
    expr = build_nullable_expr(DataType::INT64,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_i64);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::BOOL,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_bool);
    expr = build_nullable_expr(DataType::INT8,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i8);
    expr = build_nullable_expr(DataType::INT16,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i16);
    expr = build_nullable_expr(DataType::INT32,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i32);
    expr = build_nullable_expr(DataType::INT64,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_i64);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_double);
    expr = build_nullable_expr(DataType::FLOAT,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_float);
    expr = build_nullable_expr(DataType::DOUBLE,
                               proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_double);
    //not nullable expr
    expr = build_not_nullable_expr(DataType::BOOL,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT8,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT16,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT32,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT64,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNull);
    test_is_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::BOOL,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT8,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT16,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT32,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::INT64,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::FLOAT,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
    expr = build_not_nullable_expr(DataType::DOUBLE,
                                   proto::plan::NullExpr_NullOp_IsNotNull);
    test_is_not_null_ans(expr, valid_data_all_true);
}

TEST_P(ExprTest, TestCompareExprBenchTest) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto float_1_fid = schema->AddDebugField("float1", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    auto double_1_fid = schema->AddDebugField("double1", DataType::DOUBLE);

    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);

    std::vector<
        std::pair<std::pair<FieldId, DataType>, std::pair<FieldId, DataType>>>
        test_cases = {
            {{int8_fid, DataType::INT8}, {int8_1_fid, DataType::INT8}},
            {{int16_fid, DataType::INT16}, {int16_fid, DataType::INT16}},
            {{int32_fid, DataType::INT32}, {int32_1_fid, DataType::INT32}},
            {{int64_fid, DataType::INT64}, {int64_1_fid, DataType::INT64}},
            {{float_fid, DataType::FLOAT}, {float_1_fid, DataType::FLOAT}},
            {{double_fid, DataType::DOUBLE}, {double_1_fid, DataType::DOUBLE}}};

    for (const auto& pair : test_cases) {
        auto expr = std::make_shared<expr::CompareExpr>(pair.first.first,
                                                        pair.second.first,
                                                        pair.first.second,
                                                        pair.second.second,
                                                        OpType::LessThan);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        for (int i = 0; i < 10; i++) {
            final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
        }
    }
}

TEST_P(ExprTest, TestRefactorExprs) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int8_1_fid = schema->AddDebugField("int81", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int16_1_fid = schema->AddDebugField("int161", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int32_1_fid = schema->AddDebugField("int321", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto int64_1_fid = schema->AddDebugField("int641", DataType::INT64);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);

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

    auto build_expr = [&](enum ExprType test_type,
                          int n) -> expr::TypedExprPtr {
        switch (test_type) {
            case UnaryRangeExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                return std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
            }
            case TermExprImpl: {
                std::vector<proto::plan::GenericValue> retrieve_ints;
                for (int i = 0; i < n; ++i) {
                    proto::plan::GenericValue val;
                    val.set_float_val(i);
                    retrieve_ints.push_back(val);
                }
                return std::make_shared<expr::TermFilterExpr>(
                    expr::ColumnInfo(double_fid, DataType::DOUBLE),
                    retrieve_ints);
            }
            case CompareExpr: {
                auto compare_expr =
                    std::make_shared<expr::CompareExpr>(int8_fid,
                                                        int8_1_fid,
                                                        DataType::INT8,
                                                        DataType::INT8,
                                                        OpType::LessThan);
                return compare_expr;
            }
            case BinaryRangeExpr: {
                proto::plan::GenericValue lower;
                lower.set_int64_val(10);
                proto::plan::GenericValue upper;
                upper.set_int64_val(45);
                return std::make_shared<expr::BinaryRangeFilterExpr>(
                    expr::ColumnInfo(int64_fid, DataType::INT64),
                    lower,
                    upper,
                    true,
                    true);
            }
            case LogicalUnaryExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                auto child_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                return std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
            }
            case LogicalBinaryExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                auto child1_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                auto child2_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::NotEqual,
                    val,
                    std::vector<proto::plan::GenericValue>{});
                ;
                return std::make_shared<const expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And,
                    child1_expr,
                    child2_expr);
            }
            case BinaryArithOpEvalRangeExpr: {
                proto::plan::GenericValue val;
                val.set_int64_val(100);
                proto::plan::GenericValue right;
                right.set_int64_val(10);
                return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::Equal,
                    proto::plan::ArithOpType::Add,
                    val,
                    right);
            }
            default: {
                proto::plan::GenericValue val;
                val.set_int64_val(10);
                return std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(int8_fid, DataType::INT8),
                    proto::plan::OpType::GreaterThan,
                    val,
                    std::vector<proto::plan::GenericValue>{});
            }
        }
    };
    auto test_case = [&](int n) {
        auto expr = build_expr(UnaryRangeExpr, n);
        query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    };
    test_case(3);
    test_case(10);
    test_case(20);
    test_case(30);
    test_case(50);
    test_case(100);
    test_case(200);
    // test_case(500);
}

TEST_P(ExprTest, TestCompareWithScalarIndexMaris) {
    std::vector<
        std::tuple<std::string, std::function<bool(std::string, std::string)>>>
        testcases = {
            {"string1 < string2",
             [](std::string a, std::string b) { return a.compare(b) < 0; }},
            {"string1 <= string2",
             [](std::string a, std::string b) { return a.compare(b) <= 0; }},
            {"string1 > string2",
             [](std::string a, std::string b) { return a.compare(b) > 0; }},
            {"string1 >= string2",
             [](std::string a, std::string b) { return a.compare(b) >= 0; }},
            {"string1 == string2",
             [](std::string a, std::string b) { return a.compare(b) == 0; }},
            {"string1 != string2",
             [](std::string a, std::string b) { return a.compare(b) != 0; }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto str2_fid = schema->AddDebugField("string2", DataType::VARCHAR);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for string1 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for string2 field
    auto str2_col = raw_data.get_col<std::string>(str2_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, str2_col.data());
    load_index_info.field_id = str2_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto binary_plan = create_search_plan_from_expr(expr_str);
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
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
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = str2_col[i];
            auto ref = ref_func(val1, val2);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexMarisNullable) {
    std::vector<std::tuple<std::string,
                           std::function<bool(std::string, std::string, bool)>>>
        testcases = {
            {"string1 < nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) < 0;
             }},
            {"string1 <= nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) <= 0;
             }},
            {"string1 > nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) > 0;
             }},
            {"string1 >= nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) >= 0;
             }},
            {"string1 == nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) == 0;
             }},
            {"string1 != nullable_fid",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) != 0;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto nullable_fid =
        schema->AddDebugField("nullable_fid", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for string1 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for nullable_fid field
    auto nullable_col = raw_data.get_col<std::string>(nullable_fid);
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto binary_plan = create_search_plan_from_expr(expr_str);
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
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
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = str1_col[i];
            auto val2 = nullable_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}

TEST_P(ExprTest, TestCompareWithScalarIndexMarisNullable2) {
    std::vector<std::tuple<std::string,
                           std::function<bool(std::string, std::string, bool)>>>
        testcases = {
            {"nullable_fid < string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) < 0;
             }},
            {"nullable_fid <= string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) <= 0;
             }},
            {"nullable_fid > string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) > 0;
             }},
            {"nullable_fid >= string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) >= 0;
             }},
            {"nullable_fid == string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) == 0;
             }},
            {"nullable_fid != string1",
             [](std::string a, std::string b, bool valid) {
                 if (!valid) {
                     return false;
                 }
                 return a.compare(b) != 0;
             }},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto str1_fid = schema->AddDebugField("string1", DataType::VARCHAR);
    auto nullable_fid =
        schema->AddDebugField("nullable_fid", DataType::VARCHAR, true);
    schema->set_primary_field_id(str1_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    // load index for string1 field
    auto str1_col = raw_data.get_col<std::string>(str1_fid);
    auto str1_index = milvus::index::CreateStringIndexMarisa();
    str1_index->Build(N, str1_col.data());
    load_index_info.field_id = str1_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str1_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str1_index));
    seg->LoadIndex(load_index_info);

    // load index for nullable_fid field
    auto nullable_col = raw_data.get_col<std::string>(nullable_fid);
    auto valid_data_col = raw_data.get_col_valid(nullable_fid);
    auto str2_index = milvus::index::CreateStringIndexMarisa();
    str2_index->Build(N, nullable_col.data(), valid_data_col.data());
    load_index_info.field_id = nullable_fid.get();
    load_index_info.field_type = DataType::VARCHAR;
    load_index_info.index_params = GenIndexParams(str2_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(str2_index));
    seg->LoadIndex(load_index_info);

    query::ExecPlanNodeVisitor visitor(*seg, MAX_TIMESTAMP);
    SetSchema(schema);
    for (auto [expr_str, ref_func] : testcases) {
        auto binary_plan = create_search_plan_from_expr(expr_str);
        auto plan = CreateSearchPlanByExpr(
            schema, binary_plan.data(), binary_plan.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg.get(),
            N,
            MAX_TIMESTAMP);
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
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg.get(),
            N,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N / 2);

        for (int i = 0; i < N; ++i) {
            auto ans = final[i];
            auto val1 = nullable_col[i];
            auto val2 = str1_col[i];
            auto ref = ref_func(val1, val2, valid_data_col[i]);
            ASSERT_EQ(ans, ref) << expr_str << "@" << i << "!!"
                                << boost::format("[%1%, %2%]") % val1 % val2;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref)
                    << expr_str << "@" << i << "!!"
                    << boost::format("[%1%, %2%]") % val1 % val2;
            }
        }
    }
}
