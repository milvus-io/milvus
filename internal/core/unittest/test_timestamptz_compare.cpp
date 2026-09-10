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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/ScalarIndex.h"
#include "knowhere/comp/index_param.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegcoreConfig.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

namespace {

std::shared_ptr<milvus::expr::ITypeExpr>
MakeTstzFieldCompare(FieldId left_fid,
                     FieldId right_fid,
                     proto::plan::OpType op) {
    return std::make_shared<milvus::expr::CompareExpr>(
        left_fid, right_fid, DataType::TIMESTAMPTZ, DataType::TIMESTAMPTZ, op);
}

bool
ExpectedGreaterThan(int64_t left,
                    bool left_valid,
                    int64_t right,
                    bool right_valid) {
    if (!left_valid || !right_valid) {
        return false;
    }
    return left > right;
}

}  // namespace

class TimestamptzCompareCorrectnessTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto pk_fid = schema_->AddDebugField("pk", DataType::INT64);
        ts_a_fid_ = schema_->AddDebugField("ts_a", DataType::TIMESTAMPTZ, true);
        ts_b_fid_ = schema_->AddDebugField("ts_b", DataType::TIMESTAMPTZ, true);
        schema_->set_primary_field_id(pk_fid);

        dataset_ = std::make_unique<GeneratedData>(DataGen(schema_, N, 42));
        ts_a_ = dataset_->get_col<int64_t>(ts_a_fid_);
        ts_b_ = dataset_->get_col<int64_t>(ts_b_fid_);
        ts_a_valid_ = dataset_->get_col_valid(ts_a_fid_);
        ts_b_valid_ = dataset_->get_col_valid(ts_b_fid_);

        size_t nulls = 0;
        size_t both_valid_gt = 0;
        for (size_t i = 0; i < N; ++i) {
            if (!ts_a_valid_[i] || !ts_b_valid_[i]) {
                nulls++;
            } else if (ts_a_[i] > ts_b_[i]) {
                both_valid_gt++;
            }
        }
        ASSERT_GT(nulls, 0u);
        ASSERT_LT(nulls, N);
        ASSERT_GT(both_valid_gt, 0u);

        SegcoreConfig config = SegcoreConfig::default_config();
        config.set_chunk_rows(8);
        growing_ = CreateGrowingSegment(schema_, empty_index_meta, 1, config);
        growing_->PreInsert(N);
        growing_->Insert(0,
                         N,
                         dataset_->row_ids_.data(),
                         dataset_->timestamps_.data(),
                         dataset_->raw_);
        sealed_ = CreateSealedWithFieldDataLoaded(schema_, *dataset_);
    }

    void
    LoadTimestamptzIndex(FieldId fid,
                         const int64_t* values,
                         const bool* valid) {
        auto scalar_index = milvus::index::CreateScalarIndexSort<int64_t>();
        scalar_index->Build(N, values, valid);

        LoadIndexInfo load_index_info;
        load_index_info.field_id = fid.get();
        load_index_info.field_type = DataType::TIMESTAMPTZ;
        load_index_info.index_params = GenIndexParams(scalar_index.get());
        load_index_info.cache_index = milvus::CreateTestCacheIndex(
            "timestamptz", std::move(scalar_index));
        sealed_->LoadIndex(load_index_info);
        ASSERT_TRUE(sealed_->HasIndex(fid));
    }

    void
    AssertGreaterThan(SegmentInternalInterface* segment) {
        auto typed_expr = MakeTstzFieldCompare(
            ts_a_fid_, ts_b_fid_, proto::plan::OpType::GreaterThan);
        auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
        auto final = query::ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        for (size_t i = 0; i < N; ++i) {
            const bool expected = ExpectedGreaterThan(
                ts_a_[i], ts_a_valid_[i], ts_b_[i], ts_b_valid_[i]);
            ASSERT_EQ(final[i], expected)
                << "row " << i << " a=" << ts_a_[i]
                << " a_valid=" << ts_a_valid_[i] << " b=" << ts_b_[i]
                << " b_valid=" << ts_b_valid_[i];
        }
    }

    void
    AssertOffsetInputGreaterThan(SegmentInternalInterface* segment) {
        std::vector<int32_t> null_rows, valid_gt_rows, valid_le_rows;
        for (size_t i = 0; i < N; ++i) {
            if (!ts_a_valid_[i] || !ts_b_valid_[i]) {
                null_rows.push_back(static_cast<int32_t>(i));
                continue;
            }
            if (ts_a_[i] > ts_b_[i]) {
                valid_gt_rows.push_back(static_cast<int32_t>(i));
            } else {
                valid_le_rows.push_back(static_cast<int32_t>(i));
            }
        }
        ASSERT_GE(null_rows.size(), 2u);
        ASSERT_GE(valid_gt_rows.size(), 1u);
        ASSERT_GE(valid_le_rows.size(), 1u);

        OffsetVector offsets;
        offsets.emplace_back(null_rows.back());
        offsets.emplace_back(valid_gt_rows.front());
        offsets.emplace_back(null_rows.front());
        offsets.emplace_back(valid_le_rows.front());

        auto typed_expr = MakeTstzFieldCompare(
            ts_a_fid_, ts_b_fid_, proto::plan::OpType::GreaterThan);
        auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        auto col_vec = milvus::test::gen_filter_res(
            filter_node.get(), segment, N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView res(col_vec->GetRawData(), col_vec->size());
        ASSERT_EQ(res.size(), offsets.size());

        for (size_t k = 0; k < offsets.size(); ++k) {
            const auto row = offsets[k];
            const bool expected = ExpectedGreaterThan(
                ts_a_[row], ts_a_valid_[row], ts_b_[row], ts_b_valid_[row]);
            ASSERT_EQ(res[k], expected)
                << "candidate k=" << k << " row=" << row;
        }
    }

    static constexpr size_t N = 32;

    SchemaPtr schema_;
    FieldId ts_a_fid_;
    FieldId ts_b_fid_;
    std::unique_ptr<GeneratedData> dataset_;
    FixedVector<int64_t> ts_a_;
    FixedVector<int64_t> ts_b_;
    FixedVector<bool> ts_a_valid_;
    FixedVector<bool> ts_b_valid_;
    SegmentGrowingPtr growing_;
    std::unique_ptr<SegmentSealed> sealed_;
};

TEST_F(TimestamptzCompareCorrectnessTest, GrowingNullSemantics) {
    AssertGreaterThan(growing_.get());
}

TEST_F(TimestamptzCompareCorrectnessTest, GrowingOffsetInputNullSemantics) {
    AssertOffsetInputGreaterThan(growing_.get());
}

TEST_F(TimestamptzCompareCorrectnessTest, SealedNullSemantics) {
    AssertGreaterThan(sealed_.get());
}

TEST_F(TimestamptzCompareCorrectnessTest, SealedOffsetInputNullSemantics) {
    AssertOffsetInputGreaterThan(sealed_.get());
}

TEST_F(TimestamptzCompareCorrectnessTest, SealedScalarIndexOnLeftGreaterThan) {
    LoadTimestamptzIndex(ts_a_fid_, ts_a_.data(), ts_a_valid_.data());
    AssertGreaterThan(sealed_.get());
}

TEST_F(TimestamptzCompareCorrectnessTest,
       SealedOffsetInputScalarIndexOnLeftGreaterThan) {
    LoadTimestamptzIndex(ts_a_fid_, ts_a_.data(), ts_a_valid_.data());
    AssertOffsetInputGreaterThan(sealed_.get());
}
