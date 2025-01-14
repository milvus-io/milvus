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

#include <random>
#include <gtest/gtest.h>

#include "pb/plan.pb.h"
#include "segcore/SegmentSealedImpl.h"
#include "index/InvertedIndexTantivy.h"
#include "test_utils/DataGen.h"
#include "common/Schema.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

template <typename T>
SchemaPtr
GenSchema() {
    auto schema_ = std::make_shared<Schema>();
    auto pk = schema_->AddDebugField("pk", DataType::INT64);
    schema_->set_primary_field_id(pk);

    if constexpr (std::is_same_v<T, bool>) {
        schema_->AddDebugField("index", DataType::BOOL, false);
    } else if constexpr (std::is_same_v<T, int8_t>) {
        schema_->AddDebugField("index", DataType::INT8, false);
    } else if constexpr (std::is_same_v<T, int16_t>) {
        schema_->AddDebugField("index", DataType::INT16, false);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        schema_->AddDebugField("index", DataType::INT32, false);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        schema_->AddDebugField("index", DataType::INT64, false);
    } else if constexpr (std::is_same_v<T, float>) {
        schema_->AddDebugField("index", DataType::FLOAT, false);
    } else if constexpr (std::is_same_v<T, double>) {
        schema_->AddDebugField("index", DataType::DOUBLE, false);
    } else if constexpr (std::is_same_v<T, std::string>) {
        schema_->AddDebugField("index", DataType::VARCHAR, false);
    }

    return schema_;
}

template <typename T>
class BuildInvertedIndexWithSingleSegmentTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        schema_ = GenSchema<T>();
        seg_ = CreateSealedSegment(schema_);
        N_ = 3000;
        uint64_t seed = 1234;
        auto raw_data = DataGen(schema_, N_, seed);

        if constexpr (std::is_same_v<T, bool>) {
            auto index_col =
                raw_data.get_col(schema_->get_field_id(FieldName("index")))
                    ->scalars()
                    .bool_data()
                    .data();
            for (size_t i = 0; i < N_; i++) {
                index_column_data_.push_back(index_col[i]);
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            auto index_col =
                raw_data.get_col(schema_->get_field_id(FieldName("index")))
                    ->scalars()
                    .long_data()
                    .data();
            for (size_t i = 0; i < N_; i++) {
                index_column_data_.push_back(index_col[i]);
            }
        } else if constexpr (std::is_integral_v<T>) {
            auto index_col =
                raw_data.get_col(schema_->get_field_id(FieldName("index")))
                    ->scalars()
                    .int_data()
                    .data();
            for (size_t i = 0; i < N_; i++) {
                index_column_data_.push_back(index_col[i]);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            auto index_col =
                raw_data.get_col(schema_->get_field_id(FieldName("index")))
                    ->scalars()
                    .float_data()
                    .data();
            for (size_t i = 0; i < N_; i++) {
                index_column_data_.push_back(index_col[i]);
            }
        } else if constexpr (std::is_same_v<T, std::string>) {
            auto index_col =
                raw_data.get_col(schema_->get_field_id(FieldName("index")))
                    ->scalars()
                    .string_data()
                    .data();
            for (size_t i = 0; i < N_; i++) {
                index_column_data_.push_back(index_col[i]);
            }
        }
        SealedLoadFieldData(raw_data, *seg_);
        LoadInvertedIndex();
    }

    void
    TearDown() override {
    }

    void
    LoadInvertedIndex() {
        auto index = std::make_unique<index::InvertedIndexTantivy<T>>();
        Config cfg;
        cfg["inverted_list_single_segment"] = true;
        index->BuildWithRawData(N_, index_column_data_.data(), cfg);
        LoadIndexInfo info{
            .field_id = schema_->get_field_id(FieldName("index")).get(),
            .index = std::move(index),
        };
        seg_->LoadIndex(info);
    }

    T
    FieldValueAt(int64_t offset) {
        return index_column_data_[offset];
    }

 public:
    SchemaPtr schema_;
    SegmentSealedUPtr seg_;
    int64_t N_;
    boost::container::vector<T> index_column_data_;
};

TYPED_TEST_SUITE_P(BuildInvertedIndexWithSingleSegmentTest);

TYPED_TEST_P(BuildInvertedIndexWithSingleSegmentTest,
             ReadFromSingleSegmentIndex) {
    const auto& meta = this->schema_->operator[](FieldName("index"));
    for (size_t i = 0; i < 10; i++) {
        auto column_info = test::GenColumnInfo(
            meta.get_id().get(),
            static_cast<proto::schema::DataType>(meta.get_data_type()),
            false,
            false,
            static_cast<proto::schema::DataType>(meta.get_element_type()));

        std::random_device rd;
        std::mt19937 gen(rd());

        std::uniform_int_distribution<> int_dist(1, this->N_);
        int random_int = int_dist(gen) - 1;

        auto unary_range_expr = std::make_unique<proto::plan::UnaryRangeExpr>();
        unary_range_expr->set_allocated_column_info(column_info);
        unary_range_expr->set_op(proto::plan::OpType::Equal);
        auto val_to_filter = this->FieldValueAt(random_int);
        unary_range_expr->set_allocated_value(
            test::GenGenericValue(val_to_filter));

        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary_range_expr.release());
        auto parser = ProtoParser(*this->schema_);
        auto typed_expr = parser.ParseExprs(*expr);
        auto parsed = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        auto segpromote = dynamic_cast<SegmentSealedImpl*>(this->seg_.get());
        BitsetType final;
        final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);
        auto ref = [this, val_to_filter](size_t offset) -> bool {
            return this->index_column_data_[offset] == val_to_filter;
        };
        ASSERT_EQ(final.size(), this->N_);
        ASSERT_TRUE(final[random_int]);
        for (size_t i = 0; i < this->N_; i++) {
            ASSERT_EQ(final[i], ref(i))
                << "i: " << i << ", final[i]: " << final[i]
                << ", ref(i): " << ref(i);
        }
    }
}

REGISTER_TYPED_TEST_CASE_P(BuildInvertedIndexWithSingleSegmentTest,
                           ReadFromSingleSegmentIndex);

using ElementType = testing::
    Types<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string>;
INSTANTIATE_TYPED_TEST_SUITE_P(Naive,
                               BuildInvertedIndexWithSingleSegmentTest,
                               ElementType);
