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

#include <gtest/gtest.h>
#include <regex>

#include "pb/plan.pb.h"
#include "index/InvertedIndexTantivy.h"
#include "common/Schema.h"

#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"

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
        LoadIndexInfo info{
            .field_id = schema_->get_field_id(FieldName("array")).get(),
            .index = std::move(index),
        };
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

    auto parser = ProtoParser(*this->schema_);
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

    auto parser = ProtoParser(*this->schema_);
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

    auto parser = ProtoParser(*this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
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
