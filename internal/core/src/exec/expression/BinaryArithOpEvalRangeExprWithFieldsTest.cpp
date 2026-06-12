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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ExprTestBase.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowing.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

EXPR_TEST_INSTANTIATE();

namespace {

struct NumericField {
    FieldId id;
    DataType type;
};

proto::plan::GenericValue
IntValue(int64_t value) {
    proto::plan::GenericValue generic_value;
    generic_value.set_int64_val(value);
    return generic_value;
}

expr::TypedExprPtr
MakeTwoFieldExpr(const NumericField& left,
                 const NumericField& right,
                 proto::plan::OpType cmp_op,
                 proto::plan::ArithOpType arith_op,
                 proto::plan::GenericValue value) {
    return std::make_shared<expr::BinaryArithOpEvalRangeExprWithFields>(
        expr::ColumnInfo(left.id, left.type),
        expr::ColumnInfo(right.id, right.type),
        cmp_op,
        arith_op,
        std::move(value));
}

template <typename T>
void
InsertNumericColumn(InsertRecordProto* insert_data,
                    const std::vector<T>& values,
                    const SchemaPtr& schema,
                    FieldId field_id) {
    InsertCol(insert_data, values, schema->operator[](field_id), false);
}

GeneratedData
BuildGeneratedData(const SchemaPtr& schema,
                   std::unique_ptr<InsertRecordProto> insert_data,
                   int64_t row_count) {
    GeneratedData raw_data;
    raw_data.schema_ = schema;
    raw_data.raw_ = insert_data.release();
    raw_data.raw_->set_num_rows(row_count);
    raw_data.row_ids_.reserve(row_count);
    raw_data.timestamps_.reserve(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        raw_data.row_ids_.push_back(i);
        raw_data.timestamps_.push_back(i);
    }
    return raw_data;
}

}  // namespace

TEST_P(ExprTest, TestBinaryArithOpEvalRangeExprWithFieldsNumericDispatch) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto int8_fid = schema->AddDebugField("int8", DataType::INT8);
    auto int16_fid = schema->AddDebugField("int16", DataType::INT16);
    auto int32_fid = schema->AddDebugField("int32", DataType::INT32);
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    auto float_fid = schema->AddDebugField("float", DataType::FLOAT);
    auto double_fid = schema->AddDebugField("double", DataType::DOUBLE);
    schema->set_primary_field_id(pk_fid);

    constexpr int64_t N = 64;
    std::vector<int64_t> pk(N);
    std::vector<int8_t> int8_values(N);
    std::vector<int16_t> int16_values(N);
    std::vector<int32_t> int32_values(N);
    std::vector<int64_t> int64_values(N);
    std::vector<float> float_values(N);
    std::vector<double> double_values(N);
    for (int64_t i = 0; i < N; ++i) {
        pk[i] = i;
        int8_values[i] = static_cast<int8_t>(i % 31 + 1);
        int16_values[i] = static_cast<int16_t>(i + 2);
        int32_values[i] = static_cast<int32_t>(i + 3);
        int64_values[i] = i + 4;
        float_values[i] = static_cast<float>(i + 5);
        double_values[i] = static_cast<double>(i + 6);
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    InsertNumericColumn(insert_data.get(), pk, schema, pk_fid);
    InsertNumericColumn(insert_data.get(), int8_values, schema, int8_fid);
    InsertNumericColumn(insert_data.get(), int16_values, schema, int16_fid);
    InsertNumericColumn(insert_data.get(), int32_values, schema, int32_fid);
    InsertNumericColumn(insert_data.get(), int64_values, schema, int64_fid);
    InsertNumericColumn(insert_data.get(), float_values, schema, float_fid);
    InsertNumericColumn(insert_data.get(), double_values, schema, double_fid);

    auto raw_data = BuildGeneratedData(schema, std::move(insert_data), N);
    auto seg = CreateSealedSegment(schema);
    LoadGeneratedDataIntoSegment(raw_data, seg.get(), true);

    std::vector<NumericField> fields = {
        {int8_fid, DataType::INT8},
        {int16_fid, DataType::INT16},
        {int32_fid, DataType::INT32},
        {int64_fid, DataType::INT64},
        {float_fid, DataType::FLOAT},
        {double_fid, DataType::DOUBLE},
    };

    for (const auto& left : fields) {
        for (const auto& right : fields) {
            auto expr = MakeTwoFieldExpr(left,
                                         right,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Add,
                                         IntValue(0));
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);
            for (int64_t i = 0; i < N; ++i) {
                EXPECT_TRUE(final[i]) << "row: " << i;
            }
        }
    }
}

TEST_P(ExprTest, TestBinaryArithOpEvalRangeExprWithFieldsOpsAndOffsets) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto left_fid = schema->AddDebugField("left", DataType::INT64);
    auto right_fid = schema->AddDebugField("right", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    constexpr int64_t N = 96;
    std::vector<int64_t> pk(N);
    std::vector<int64_t> left_values(N);
    std::vector<int64_t> right_values(N);
    for (int64_t i = 0; i < N; ++i) {
        pk[i] = i;
        left_values[i] = i + 10;
        right_values[i] = i % 7;
    }

    auto insert_data = std::make_unique<InsertRecordProto>();
    InsertNumericColumn(insert_data.get(), pk, schema, pk_fid);
    InsertNumericColumn(insert_data.get(), left_values, schema, left_fid);
    InsertNumericColumn(insert_data.get(), right_values, schema, right_fid);

    auto raw_data = BuildGeneratedData(schema, std::move(insert_data), N);
    auto sealed = CreateSealedSegment(schema);
    LoadGeneratedDataIntoSegment(raw_data, sealed.get(), true);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    growing->PreInsert(N);
    growing->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    NumericField left{left_fid, DataType::INT64};
    NumericField right{right_fid, DataType::INT64};

    struct Case {
        proto::plan::ArithOpType arith_op;
        proto::plan::OpType cmp_op;
        int64_t value;
        std::function<bool(int64_t, int64_t)> expected;
    };

    std::vector<Case> cases = {
        {proto::plan::ArithOpType::Add,
         proto::plan::OpType::GreaterThan,
         30,
         [](int64_t left, int64_t right) { return left + right > 30; }},
        {proto::plan::ArithOpType::Sub,
         proto::plan::OpType::LessThan,
         20,
         [](int64_t left, int64_t right) { return left - right < 20; }},
        {proto::plan::ArithOpType::Mul,
         proto::plan::OpType::Equal,
         66,
         [](int64_t left, int64_t right) { return left * right == 66; }},
        {proto::plan::ArithOpType::Div,
         proto::plan::OpType::GreaterEqual,
         3,
         [](int64_t left, int64_t right) {
             return right != 0 && left / right >= 3;
         }},
        {proto::plan::ArithOpType::Mod,
         proto::plan::OpType::NotEqual,
         0,
         [](int64_t left, int64_t right) {
             return right != 0 && left % right != 0;
         }},
        {proto::plan::ArithOpType::Add,
         proto::plan::OpType::LessEqual,
         50,
         [](int64_t left, int64_t right) { return left + right <= 50; }},
    };

    std::vector<const milvus::segcore::SegmentInternalInterface*> segments = {
        sealed.get(), growing.get()};
    for (const auto* segment : segments) {
        for (const auto& test_case : cases) {
            auto expr = MakeTwoFieldExpr(left,
                                         right,
                                         test_case.cmp_op,
                                         test_case.arith_op,
                                         IntValue(test_case.value));
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto final = ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);

            milvus::exec::OffsetVector offsets;
            offsets.reserve(N / 3);
            for (int64_t i = 0; i < N; ++i) {
                if (i % 3 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(
                plan.get(), segment, N, MAX_TIMESTAMP, &offsets);
            BitsetTypeView offset_view(col_vec->GetRawData(), col_vec->size());
            EXPECT_EQ(offset_view.size(), offsets.size());

            for (int64_t i = 0; i < N; ++i) {
                auto expected =
                    test_case.expected(left_values[i], right_values[i]);
                EXPECT_EQ(final[i], expected) << "row: " << i;
                if (i % 3 == 0) {
                    EXPECT_EQ(offset_view[i / 3], expected) << "row: " << i;
                }
            }
        }
    }
}
