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

#include <boost/container/vector.hpp>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "common/Array.h"
#include "common/Schema.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

template <typename T>
std::shared_ptr<FieldData<Array>>
ArrayField(const std::vector<boost::container::vector<T>>& rows,
           bool nullable = false,
           const uint8_t* validity = nullptr) {
    std::vector<Array> values;
    values.reserve(rows.size());
    for (const auto& row : rows) {
        proto::schema::ScalarField data;
        if constexpr (std::is_same_v<T, bool>) {
            auto* output = data.mutable_bool_data();
            for (const auto value : row) output->add_data(value);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            auto* output = data.mutable_long_data();
            for (const auto value : row) output->add_data(value);
        } else if constexpr (std::is_integral_v<T>) {
            auto* output = data.mutable_int_data();
            for (const auto value : row) output->add_data(value);
        } else if constexpr (std::is_same_v<T, float>) {
            auto* output = data.mutable_float_data();
            for (const auto value : row) output->add_data(value);
        } else if constexpr (std::is_same_v<T, double>) {
            auto* output = data.mutable_double_data();
            for (const auto value : row) output->add_data(value);
        } else {
            auto* output = data.mutable_string_data();
            for (const auto& value : row) output->add_data(value);
        }
        values.emplace_back(data);
    }
    auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, nullable);
    if (nullable) {
        field->FillFieldData(values.data(), validity, values.size(), 0);
    } else {
        field->FillFieldData(values.data(), values.size());
    }
    return field;
}

template <typename T>
T
ArrayElement(size_t key) {
    if constexpr (std::is_same_v<T, std::string>) {
        return "value-" + std::to_string(key % 31);
    } else if constexpr (std::is_same_v<T, bool>) {
        return key % 2 != 0;
    } else if constexpr (std::is_floating_point_v<T>) {
        return static_cast<T>(static_cast<int>(key % 31) - 15) + T(0.25);
    } else {
        return static_cast<T>(static_cast<int>(key % 31) - 15);
    }
}

}  // namespace

template <typename T>
class ArrayIndexExpressionTest : public ::testing::Test {
 public:
    void SetUp() override {
        schema_ = std::make_shared<Schema>();
        array_id_ = schema_->AddDebugArrayField(
            "array", index::CppDataType<T>(), false);
        N_ = 3000;
        vec_of_array_.reserve(N_);
        for (size_t row = 0; row < N_; ++row) {
            auto& values = vec_of_array_.emplace_back();
            const auto length = row == 0 ? 3 : row % 7;
            values.reserve(length);
            for (size_t element = 0; element < length; ++element) {
                values.push_back(ArrayElement<T>(row + element));
            }
        }
        auto field = ArrayField(vec_of_array_);
        seg_ = CreateSealedSegment(schema_);
        auto raw_info = raw_files_.Prepare(array_id_, {field});
        seg_->LoadFieldData(raw_info);
        auto opened = test::expr_index::BuildIndex(
            array_id_, DataType::ARRAY, index::INVERTED_INDEX_TYPE, {field},
            Config::object(), index::CppDataType<T>());
        test::expr_index::InstallIndex(*seg_, array_id_, DataType::ARRAY,
                                       std::move(opened), index::CppDataType<T>());
    }

    SchemaPtr schema_;
    FieldId array_id_;
    test::expr_index::RawFieldFiles raw_files_;
    SegmentSealedUPtr seg_;
    int64_t N_;
    std::vector<boost::container::vector<T>> vec_of_array_;
};

TYPED_TEST_SUITE_P(ArrayIndexExpressionTest);

TYPED_TEST_P(ArrayIndexExpressionTest, ArrayContainsAny) {
    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto contains_expr = std::make_unique<proto::plan::JSONContainsExpr>();
    contains_expr->set_allocated_column_info(column_info);
    contains_expr->set_op(proto::plan::JSONContainsExpr_JSONOp::
                              JSONContainsExpr_JSONOp_ContainsAny);
    contains_expr->set_elements_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto t = test::GenGenericValue(elem);
        contains_expr->mutable_elements()->AddAllocated(t);
    }
    auto expr = test::GenExpr();
    expr->set_allocated_json_contains_expr(contains_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    std::unordered_set<TypeParam> elems(this->vec_of_array_[0].begin(),
                                        this->vec_of_array_[0].end());
    auto ref = [this, &elems](size_t offset) -> bool {
        std::unordered_set<TypeParam> row(this->vec_of_array_[offset].begin(),
                                          this->vec_of_array_[offset].end());
        if (elems.empty()) {
            return false;
        }

        for (const auto& elem : elems) {
            if (row.find(elem) != row.end()) {
                return true;
            }
        }
        return false;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

TYPED_TEST_P(ArrayIndexExpressionTest, ArrayContainsAll) {
    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto contains_expr = std::make_unique<proto::plan::JSONContainsExpr>();
    contains_expr->set_allocated_column_info(column_info);
    contains_expr->set_op(proto::plan::JSONContainsExpr_JSONOp::
                              JSONContainsExpr_JSONOp_ContainsAll);
    contains_expr->set_elements_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto t = test::GenGenericValue(elem);
        contains_expr->mutable_elements()->AddAllocated(t);
    }
    auto expr = test::GenExpr();
    expr->set_allocated_json_contains_expr(contains_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    std::unordered_set<TypeParam> elems(this->vec_of_array_[0].begin(),
                                        this->vec_of_array_[0].end());
    auto ref = [this, &elems](size_t offset) -> bool {
        std::unordered_set<TypeParam> row(this->vec_of_array_[offset].begin(),
                                          this->vec_of_array_[offset].end());
        if (elems.empty()) {
            return true;
        }

        for (const auto& elem : elems) {
            if (row.find(elem) == row.end()) {
                return false;
            }
        }
        return true;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

TYPED_TEST_P(ArrayIndexExpressionTest, ArrayEqual) {
    if (std::is_floating_point_v<TypeParam>) {
        GTEST_SKIP() << "not accurate to perform equal comparison on floating "
                        "point number";
    }

    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto unary_range_expr = std::make_unique<proto::plan::UnaryRangeExpr>();
    unary_range_expr->set_allocated_column_info(column_info);
    unary_range_expr->set_op(proto::plan::OpType::Equal);
    auto arr = new proto::plan::GenericValue;
    arr->mutable_array_val()->set_element_type(
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    arr->mutable_array_val()->set_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto e = test::GenGenericValue(elem);
        arr->mutable_array_val()->mutable_array()->AddAllocated(e);
    }
    unary_range_expr->set_allocated_value(arr);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    milvus::test::ExprBatchSizeGuard batch_size_guard(1024);
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        typed_expr, this->seg_.get(), this->N_));
    EXPECT_EQ(milvus::test::EvalExprBatchSizes(
                  typed_expr, this->seg_.get(), this->N_),
              (std::vector<int64_t>{1024, 1024, 952}));

    auto unary_expr =
        std::dynamic_pointer_cast<const expr::UnaryRangeFilterExpr>(typed_expr);
    ASSERT_NE(unary_expr, nullptr);
    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        unary_expr->column_,
        proto::plan::OpType::GreaterThan,
        unary_expr->val_,
        std::vector<proto::plan::GenericValue>{});
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        greater_expr, this->seg_.get(), this->N_));

    auto empty_value = unary_expr->val_;
    empty_value.mutable_array_val()->clear_array();
    auto empty_equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        unary_expr->column_,
        proto::plan::OpType::Equal,
        empty_value,
        std::vector<proto::plan::GenericValue>{});
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        empty_equal_expr, this->seg_.get(), this->N_));

    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    auto ref = [this](size_t offset) -> bool {
        if (this->vec_of_array_[0].size() !=
            this->vec_of_array_[offset].size()) {
            return false;
        }
        auto size = this->vec_of_array_[0].size();
        for (size_t i = 0; i < size; i++) {
            if (this->vec_of_array_[0][i] != this->vec_of_array_[offset][i]) {
                return false;
            }
        }
        return true;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

using ElementType = testing::
    Types<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string>;

REGISTER_TYPED_TEST_CASE_P(ArrayIndexExpressionTest,
                           ArrayContainsAny,
                           ArrayContainsAll,
                           ArrayEqual);

INSTANTIATE_TYPED_TEST_SUITE_P(Naive, ArrayIndexExpressionTest, ElementType);

namespace {

proto::plan::GenericValue
MakeInt64ArrayValue(const std::vector<int64_t>& values) {
    proto::plan::GenericValue value;
    auto* array = value.mutable_array_val();
    array->set_element_type(proto::schema::DataType::Int64);
    array->set_same_type(true);
    for (auto element : values) {
        array->add_array()->set_int64_val(element);
    }
    return value;
}

}  // namespace

TEST(ArrayIndexExpressionRegression,
     NestedMaterializationKeepsRowNullsOutOfElementValidity) {
    const std::vector<boost::container::vector<int64_t>> arrays = {
        {10, 20}, {}, {}, {30}, {}};
    const uint8_t parents = 0x0B;
    auto field = ArrayField(arrays, true, &parents);
    auto schema = std::make_shared<Schema>();
    const auto field_id = schema->AddDebugArrayField(
        "structA[array]", DataType::INT64, true);
    test::expr_index::RawFieldFiles raw_files;
    auto segment = CreateSealedSegment(schema);
    auto raw_info = raw_files.Prepare(field_id, {field});
    segment->LoadFieldData(raw_info);
    auto opened = test::expr_index::BuildIndex(
        field_id, DataType::ARRAY, index::INVERTED_INDEX_TYPE, {field},
        Config::object(), DataType::INT64, true);
    ASSERT_EQ(opened.reader->CoordDomain(), index::Domain::Element);
    ASSERT_EQ(opened.reader->Count(), 3);
    const auto* null_reader =
        dynamic_cast<const index::INullReader*>(opened.reader.get());
    const auto* predicate =
        dynamic_cast<const index::IScalarPredicateReader<int64_t>*>(
            opened.reader.get());
    ASSERT_NE(null_reader, nullptr);
    ASSERT_NE(predicate, nullptr);
    auto is_null = null_reader->IsNull();
    auto is_not_null = null_reader->IsNotNull();
    ASSERT_EQ(is_null.size(), 3);
    ASSERT_EQ(is_not_null.size(), 3);
    EXPECT_EQ(is_null.count(), 0);
    EXPECT_EQ(is_not_null.count(), 3);
    const int64_t excluded = 10;
    auto not_in = predicate->NotIn(1, &excluded);
    ASSERT_EQ(not_in.size(), 3);
    EXPECT_FALSE(not_in[0]);
    EXPECT_TRUE(not_in[1]);
    EXPECT_TRUE(not_in[2]);

    test::expr_index::InstallIndex(*segment, field_id, DataType::ARRAY,
                                   std::move(opened), DataType::INT64);
    proto::plan::GenericValue ten;
    ten.set_int64_val(10);
    proto::plan::GenericValue thirty;
    thirty.set_int64_val(30);
    auto expression = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(field_id, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        true,
        std::vector<proto::plan::GenericValue>{ten, thirty});
    EXPECT_TRUE(test::CanExprExecuteAllAtOnce(
        expression, segment.get(), arrays.size()));
    auto evaluated = test::EvalExprInBatches(
        expression, segment.get(), arrays.size());
    TargetBitmapView values(evaluated.result->GetRawData(), arrays.size());
    TargetBitmapView validity(evaluated.result->GetValidRawData(), arrays.size());
    const std::vector<bool> expected = {true, false, false, true, false};
    for (size_t row = 0; row < arrays.size(); ++row) {
        EXPECT_EQ(validity[row], (parents & (1u << row)) != 0) << "row=" << row;
        EXPECT_EQ(values[row], expected[row]) << "row=" << row;
    }
}

TEST(ArrayIndexExpressionRegression,
     ArrayNotEqualUsesIndexCandidatesAsRowLevelPrefilter) {
    const std::vector<std::vector<int64_t>> arrays = {
        {1, 1, 2},     // equal
        {},            // null
        {1, 2},        // missing duplicate
        {},            // null
        {1, 1, 2, 3},  // extra element
        {},            // null
        {1, 2, 1},     // different order
        {},            // null
        {1, 1, 1, 2},  // different duplicate count
        {},            // null
        {1, 3},        // missing indexed element
        {},            // null
        {3, 4},        // no indexed element
        {},            // null
    };
    const auto row_count = static_cast<int64_t>(arrays.size());
    std::vector<bool> expected_validity(row_count);
    std::vector<bool> expected_values(row_count);
    const std::vector<int64_t> literal = {1, 1, 2};
    for (int64_t row = 0; row < row_count; ++row) {
        const bool valid = row % 2 == 0;
        expected_validity[row] = valid;
        expected_values[row] = valid && arrays[row] != literal;
    }

    milvus::test::ExprBatchSizeGuard batch_size_guard(4);
    for (bool nested_index : {false, true}) {
        auto schema = std::make_shared<Schema>();
        auto array_fid = schema->AddDebugArrayField(
            nested_index ? "structA[array]" : "array", DataType::INT64, true);

        std::vector<boost::container::vector<int64_t>> index_arrays;
        index_arrays.reserve(row_count);
        std::vector<uint8_t> parent_validity((row_count + 7) / 8, 0);
        for (int64_t row = 0; row < row_count; ++row) {
            index_arrays.emplace_back(arrays[row].begin(), arrays[row].end());
            if (expected_validity[row]) {
                parent_validity[row >> 3] |= uint8_t(1u << (row & 7));
            }
        }
        auto field = ArrayField(index_arrays, true, parent_validity.data());
        test::expr_index::RawFieldFiles raw_files;
        auto raw_segment = CreateSealedSegment(schema);
        auto indexed_segment = CreateSealedSegment(schema);
        auto raw_info = raw_files.Prepare(array_fid, {field});
        raw_segment->LoadFieldData(raw_info);
        indexed_segment->LoadFieldData(raw_info);
        auto logical_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(
                array_fid, DataType::ARRAY, DataType::INT64, {}, true),
            proto::plan::OpType::NotEqual,
            MakeInt64ArrayValue(literal),
            std::vector<proto::plan::GenericValue>{});
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           logical_expr);

        auto raw_eval = milvus::test::EvalExprInBatches(
            logical_expr, raw_segment.get(), row_count);
        EXPECT_EQ(raw_eval.batch_sizes, (std::vector<int64_t>{4, 4, 4, 2}));
        EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
            logical_expr, raw_segment.get(), row_count));

        auto opened = test::expr_index::BuildIndex(
            array_fid, DataType::ARRAY, index::INVERTED_INDEX_TYPE, {field},
            Config::object(), DataType::INT64, nested_index);
        test::expr_index::InstallIndex(*indexed_segment, array_fid,
                                       DataType::ARRAY, std::move(opened),
                                       DataType::INT64);

        EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
            logical_expr, indexed_segment.get(), row_count));
        auto indexed_eval = milvus::test::EvalExprInBatches(
            logical_expr, indexed_segment.get(), row_count);
        EXPECT_EQ(indexed_eval.batch_sizes, (std::vector<int64_t>{4, 4, 4, 2}));

        TargetBitmapView raw_values(raw_eval.result->GetRawData(), row_count);
        TargetBitmapView raw_validity(raw_eval.result->GetValidRawData(),
                                      row_count);
        TargetBitmapView indexed_values(indexed_eval.result->GetRawData(),
                                        row_count);
        TargetBitmapView indexed_validity(
            indexed_eval.result->GetValidRawData(), row_count);
        for (int64_t row = 0; row < row_count; ++row) {
            EXPECT_EQ(indexed_validity[row], expected_validity[row])
                << "nested=" << nested_index << ", row=" << row;
            EXPECT_EQ(indexed_validity[row], raw_validity[row])
                << "nested=" << nested_index << ", row=" << row;
            EXPECT_EQ(indexed_values[row], expected_values[row])
                << "nested=" << nested_index << ", row=" << row;
            EXPECT_EQ(indexed_values[row], raw_values[row])
                << "nested=" << nested_index << ", row=" << row;
        }

        exec::OffsetVector offsets = {0, 1, 2, 4, 6, 8, 10, 12, 13};
        auto raw_offset_result = milvus::test::gen_filter_res(
            plan.get(), raw_segment.get(), row_count, MAX_TIMESTAMP, &offsets);
        auto indexed_offset_result =
            milvus::test::gen_filter_res(plan.get(),
                                         indexed_segment.get(),
                                         row_count,
                                         MAX_TIMESTAMP,
                                         &offsets);
        TargetBitmapView raw_offset_values(raw_offset_result->GetRawData(),
                                           offsets.size());
        TargetBitmapView raw_offset_validity(
            raw_offset_result->GetValidRawData(), offsets.size());
        TargetBitmapView indexed_offset_values(
            indexed_offset_result->GetRawData(), offsets.size());
        TargetBitmapView indexed_offset_validity(
            indexed_offset_result->GetValidRawData(), offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto row = offsets[i];
            EXPECT_EQ(indexed_offset_validity[i], expected_validity[row])
                << "nested=" << nested_index << ", offset row=" << row;
            EXPECT_EQ(indexed_offset_validity[i], raw_offset_validity[i])
                << "nested=" << nested_index << ", offset row=" << row;
            EXPECT_EQ(indexed_offset_values[i], expected_values[row])
                << "nested=" << nested_index << ", offset row=" << row;
            EXPECT_EQ(indexed_offset_values[i], raw_offset_values[i])
                << "nested=" << nested_index << ", offset row=" << row;
        }

        auto mixed_literal = MakeInt64ArrayValue({1});
        mixed_literal.mutable_array_val()->add_array()->set_float_val(1.5);
        mixed_literal.mutable_array_val()->set_same_type(false);
        auto mixed_not_equal = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(
                array_fid, DataType::ARRAY, DataType::INT64, {}, true),
            proto::plan::OpType::NotEqual,
            std::move(mixed_literal),
            std::vector<proto::plan::GenericValue>{});
        EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
            mixed_not_equal, indexed_segment.get(), row_count));
        auto raw_mixed = milvus::test::EvalExprInBatches(
            mixed_not_equal, raw_segment.get(), row_count);
        auto indexed_mixed = milvus::test::EvalExprInBatches(
            mixed_not_equal, indexed_segment.get(), row_count);
        TargetBitmapView raw_mixed_values(raw_mixed.result->GetRawData(),
                                          row_count);
        TargetBitmapView raw_mixed_validity(raw_mixed.result->GetValidRawData(),
                                            row_count);
        TargetBitmapView indexed_mixed_values(
            indexed_mixed.result->GetRawData(), row_count);
        TargetBitmapView indexed_mixed_validity(
            indexed_mixed.result->GetValidRawData(), row_count);
        for (int64_t row = 0; row < row_count; ++row) {
            EXPECT_EQ(indexed_mixed_validity[row], expected_validity[row])
                << "nested=" << nested_index << ", mixed row=" << row;
            EXPECT_EQ(indexed_mixed_validity[row], raw_mixed_validity[row])
                << "nested=" << nested_index << ", mixed row=" << row;
            EXPECT_EQ(indexed_mixed_values[row], expected_validity[row])
                << "nested=" << nested_index << ", mixed row=" << row;
            EXPECT_EQ(indexed_mixed_values[row], raw_mixed_values[row])
                << "nested=" << nested_index << ", mixed row=" << row;
        }
    }
}
