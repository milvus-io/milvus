// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICEN_SE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRAN_TIES OR CON_DITION_S OF AN_Y KIN_D, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <boost/container/vector.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/PrometheusClient.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "gtest/gtest.h"
#include "index/InvertedIndexTantivy.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

template <typename T>
SchemaPtr
GenTestSchema() {
    auto schema_ = std::make_shared<Schema>();
    schema_->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema_->AddDebugField("pk", DataType::INT64);
    schema_->set_primary_field_id(pk);

    if constexpr (std::is_same_v<T, bool>) {
        schema_->AddDebugArrayField("array", DataType::BOOL, false);
    } else if constexpr (std::is_same_v<T, int8_t>) {
        schema_->AddDebugArrayField("array", DataType::INT8, false);
    } else if constexpr (std::is_same_v<T, int16_t>) {
        schema_->AddDebugArrayField("array", DataType::INT16, false);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        schema_->AddDebugArrayField("array", DataType::INT32, false);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        schema_->AddDebugArrayField("array", DataType::INT64, false);
    } else if constexpr (std::is_same_v<T, float>) {
        schema_->AddDebugArrayField("array", DataType::FLOAT, false);
    } else if constexpr (std::is_same_v<T, double>) {
        schema_->AddDebugArrayField("array", DataType::DOUBLE, false);
    } else if constexpr (std::is_same_v<T, std::string>) {
        schema_->AddDebugArrayField("array", DataType::VARCHAR, false);
    }

    return schema_;
}

template <typename T>
class ArrayInvertedIndexTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        schema_ = GenTestSchema<T>();
        N_ = 3000;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema_, N_, seed);
        auto array_col =
            raw_data.get_col(schema_->get_field_id(FieldName("array")))
                ->scalars()
                .array_data()
                .data();
        for (size_t i = 0; i < N_; i++) {
            boost::container::vector<T> array;
            if constexpr (std::is_same_v<T, bool>) {
                for (size_t j = 0; j < array_col[i].bool_data().data_size();
                     j++) {
                    array.push_back(array_col[i].bool_data().data(j));
                }
            } else if constexpr (std::is_same_v<T, int64_t>) {
                for (size_t j = 0; j < array_col[i].long_data().data_size();
                     j++) {
                    array.push_back(array_col[i].long_data().data(j));
                }
            } else if constexpr (std::is_integral_v<T>) {
                for (size_t j = 0; j < array_col[i].int_data().data_size();
                     j++) {
                    array.push_back(array_col[i].int_data().data(j));
                }
            } else if constexpr (std::is_floating_point_v<T>) {
                for (size_t j = 0; j < array_col[i].float_data().data_size();
                     j++) {
                    array.push_back(array_col[i].float_data().data(j));
                }
            } else if constexpr (std::is_same_v<T, std::string>) {
                for (size_t j = 0; j < array_col[i].string_data().data_size();
                     j++) {
                    array.push_back(array_col[i].string_data().data(j));
                }
            }
            vec_of_array_.push_back(array);
        }
        seg_ = CreateSealedWithFieldDataLoaded(schema_, raw_data);
        LoadInvertedIndex();
    }

    void
    TearDown() override {
    }

    void
    LoadInvertedIndex() {
        auto index = std::make_unique<index::InvertedIndexTantivy<T>>();
        Config cfg;
        cfg["is_array"] = true;
        index->BuildWithRawDataForUT(N_, vec_of_array_.data(), cfg);
        LoadIndexInfo info{};
        info.field_id = schema_->get_field_id(FieldName("array")).get();
        info.index_params = GenIndexParams(index.get());
        info.cache_index = CreateTestCacheIndex("test", std::move(index));
        seg_->LoadIndex(info);
    }

 public:
    SchemaPtr schema_;
    SegmentSealedUPtr seg_;
    int64_t N_;
    std::vector<boost::container::vector<T>> vec_of_array_;
};

TYPED_TEST_SUITE_P(ArrayInvertedIndexTest);

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayContainsAny) {
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

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayContainsAll) {
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

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayEqual) {
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

REGISTER_TYPED_TEST_CASE_P(ArrayInvertedIndexTest,
                           ArrayContainsAny,
                           ArrayContainsAll,
                           ArrayEqual);

INSTANTIATE_TYPED_TEST_SUITE_P(Naive, ArrayInvertedIndexTest, ElementType);

namespace {

class NullableInt64ArrayInvertedIndex
    : public index::InvertedIndexTantivy<int64_t> {
 public:
    void
    SetNullOffsets(std::vector<size_t> offsets) {
        null_offset_ = std::move(offsets);
    }
};

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

TEST(ArrayInvertedIndexRegression,
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
        schema->AddDebugField(
            "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
        auto array_fid = schema->AddDebugArrayField(
            nested_index ? "structA[array]" : "array", DataType::INT64, true);
        schema->set_primary_field_id(pk_fid);

        auto raw_data = DataGen(schema, row_count, 42, 0, 1, 3);
        proto::schema::FieldData* array_field = nullptr;
        for (auto& field : *raw_data.raw_->mutable_fields_data()) {
            if (field.field_id() == array_fid.get()) {
                array_field = &field;
                break;
            }
        }
        ASSERT_NE(array_field, nullptr);
        auto* array_data = array_field->mutable_scalars()->mutable_array_data();
        ASSERT_EQ(array_data->data_size(), row_count);
        auto* valid_data = array_field->mutable_valid_data();
        ASSERT_EQ(valid_data->size(), row_count);

        std::vector<boost::container::vector<int64_t>> index_arrays;
        index_arrays.reserve(row_count);
        std::vector<size_t> null_offsets;
        for (int64_t row = 0; row < row_count; ++row) {
            const bool valid = expected_validity[row];
            valid_data->at(row) = valid;

            auto* values = array_data->mutable_data(row)
                               ->mutable_long_data()
                               ->mutable_data();
            values->Clear();
            values->Add(arrays[row].begin(), arrays[row].end());

            if (valid) {
                index_arrays.emplace_back(arrays[row].begin(),
                                          arrays[row].end());
            } else {
                index_arrays.emplace_back();
                null_offsets.push_back(row);
            }
        }

        auto raw_segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
        auto indexed_segment =
            CreateSealedWithFieldDataLoaded(schema, raw_data);
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

        auto index = std::make_unique<NullableInt64ArrayInvertedIndex>();
        Config config;
        config["is_array"] = true;
        if (nested_index) {
            config["is_nested_index"] = true;
        }
        index->BuildWithRawDataForUT(row_count, index_arrays.data(), config);
        index->SetNullOffsets(null_offsets);

        LoadIndexInfo load_info{};
        load_info.field_id = array_fid.get();
        load_info.field_type = DataType::ARRAY;
        load_info.element_type = DataType::INT64;
        load_info.index_params = GenIndexParams(index.get());
        load_info.cache_index =
            CreateTestCacheIndex(nested_index ? "array_not_equal_nested"
                                              : "array_not_equal_row_level",
                                 std::move(index));
        indexed_segment->LoadIndex(load_info);

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
