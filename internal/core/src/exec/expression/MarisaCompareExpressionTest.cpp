// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Schema.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "index/Meta.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "test_utils/GenExprProto.h"

namespace milvus {
namespace {

void
CheckMarisaColumnComparisons(bool nullable_left, bool nullable_right) {
    const std::vector<std::string> left{
        "a", "b", "c", "", "z", "same", "a", "b", "c", "", "z", "same"};
    const std::vector<std::string> right{
        "b", "b", "b", "", "a", "same", "a", "c", "a", "x", "", "same"};
    const int64_t count = left.size();
    std::vector<uint8_t> left_validity((count + 7) / 8, 0);
    std::vector<uint8_t> right_validity((count + 7) / 8, 0);
    for (int64_t row = 0; row < count; ++row) {
        if (!nullable_left || row % 4 != 0) {
            left_validity[row / 8] |= uint8_t{1} << (row % 8);
        }
        if (!nullable_right || row % 4 != 1) {
            right_validity[row / 8] |= uint8_t{1} << (row % 8);
        }
    }

    auto schema = std::make_shared<Schema>();
    const auto pk = schema->AddDebugField("pk", DataType::INT64);
    const auto left_id =
        schema->AddDebugField("left", DataType::VARCHAR, nullable_left);
    const auto right_id =
        schema->AddDebugField("right", DataType::VARCHAR, nullable_right);
    schema->set_primary_field_id(pk);

    // Load only the primary key to establish the segment row count. Compared
    // columns have no raw data, so execution must use the installed readers.
    test::expr_index::RawFieldFiles raw_files;
    auto segment = segcore::CreateSealedSegment(schema);
    std::vector<int64_t> ids(count);
    std::iota(ids.begin(), ids.end(), int64_t{0});
    auto pk_data = std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
    pk_data->FillFieldData(ids.data(), ids.size());
    segment->LoadFieldData(raw_files.Prepare(pk, {pk_data}));

    auto left_data = test::expr_index::StringField(
        left, nullable_left, left_validity.data());
    auto right_data = test::expr_index::StringField(
        right, nullable_right, right_validity.data());
    test::expr_index::InstallIndex(
        *segment,
        left_id,
        DataType::VARCHAR,
        test::expr_index::BuildIndex(
            left_id, DataType::VARCHAR, index::MARISA_TRIE, {left_data}));
    test::expr_index::InstallIndex(
        *segment,
        right_id,
        DataType::VARCHAR,
        test::expr_index::BuildIndex(
            right_id, DataType::VARCHAR, index::MARISA_TRIE, {right_data}));
    ASSERT_EQ(segment->get_row_count(), count);
    ASSERT_EQ(segment->num_chunk_data(left_id), 0);
    ASSERT_EQ(segment->num_chunk_data(right_id), 0);

    exec::OffsetVector offsets;
    for (const int32_t row : {11, 0, 7, 4, 1, 7, 9, 2, 3}) {
        offsets.emplace_back(row);
    }

    const auto reference = [](proto::plan::OpType op, int compared) {
        switch (op) {
            case proto::plan::OpType::LessThan:
                return compared < 0;
            case proto::plan::OpType::LessEqual:
                return compared <= 0;
            case proto::plan::OpType::GreaterThan:
                return compared > 0;
            case proto::plan::OpType::GreaterEqual:
                return compared >= 0;
            case proto::plan::OpType::Equal:
                return compared == 0;
            case proto::plan::OpType::NotEqual:
                return compared != 0;
            default:
                ADD_FAILURE() << "unexpected comparison operator";
                return false;
        }
    };
    const auto is_valid = [&](int64_t row) {
        return (left_validity[row / 8] & (uint8_t{1} << (row % 8))) != 0 &&
               (right_validity[row / 8] & (uint8_t{1} << (row % 8))) != 0;
    };

    test::ExprBatchSizeGuard batch_guard(5);
    for (const auto op : {proto::plan::OpType::LessThan,
                          proto::plan::OpType::LessEqual,
                          proto::plan::OpType::GreaterThan,
                          proto::plan::OpType::GreaterEqual,
                          proto::plan::OpType::Equal,
                          proto::plan::OpType::NotEqual}) {
        for (const bool negate : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "op=" << static_cast<int>(op) << " negate=" << negate);
            expr::TypedExprPtr logical = std::make_shared<expr::CompareExpr>(
                left_id, right_id, DataType::VARCHAR, DataType::VARCHAR, op);
            if (negate) {
                logical = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, logical);
            }
            auto evaluation =
                test::EvalExprInBatches(logical, segment.get(), count);
            EXPECT_EQ(evaluation.batch_sizes, (std::vector<int64_t>{5, 5, 2}));
            ASSERT_EQ(evaluation.result->size(), count);
            BitsetTypeView bits(evaluation.result->GetRawData(), count);
            BitsetTypeView validity(evaluation.result->GetValidRawData(), count);
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, logical);
            auto filtered = query::ExecuteQueryExpr(
                plan, segment.get(), count, MAX_TIMESTAMP);
            ASSERT_EQ(filtered.size(), count);

            auto selected = test::gen_filter_res(
                plan.get(), segment.get(), count, MAX_TIMESTAMP, &offsets);
            ASSERT_EQ(selected->size(), offsets.size());
            BitsetTypeView selected_bits(selected->GetRawData(),
                                         selected->size());
            BitsetTypeView selected_validity(selected->GetValidRawData(),
                                             selected->size());
            const auto expected = [&](int64_t row) {
                const bool compared =
                    reference(op, left[row].compare(right[row]));
                return is_valid(row) && (negate ? !compared : compared);
            };
            for (int64_t row = 0; row < count; ++row) {
                EXPECT_EQ(validity[row], is_valid(row)) << row;
                EXPECT_EQ(bits[row], expected(row)) << row;
                EXPECT_EQ(filtered[row], expected(row)) << row;
            }
            for (size_t i = 0; i < offsets.size(); ++i) {
                EXPECT_EQ(selected_validity[i], is_valid(offsets[i])) << i;
                EXPECT_EQ(selected_bits[i], expected(offsets[i])) << i;
            }
        }
    }
}

TEST(MarisaCompareExpressionTest, NonNullableColumns) {
    CheckMarisaColumnComparisons(false, false);
}

TEST(MarisaCompareExpressionTest, NullableRightColumn) {
    CheckMarisaColumnComparisons(false, true);
}

TEST(MarisaCompareExpressionTest, NullableLeftColumn) {
    CheckMarisaColumnComparisons(true, false);
}

TEST(MarisaCompareExpressionTest, NullableBothColumns) {
    CheckMarisaColumnComparisons(true, true);
}

}  // namespace
}  // namespace milvus
