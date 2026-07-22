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
#include "knowhere/comp/index_param.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegcoreConfig.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

namespace {

std::shared_ptr<milvus::expr::ITypeExpr>
MakeTstzPlusOneMonthCompare(FieldId tstz_fid,
                            int64_t compare_us,
                            bool negated) {
    proto::plan::Interval interval;
    interval.set_months(1);
    proto::plan::GenericValue compare_value;
    compare_value.set_int64_val(compare_us);
    std::shared_ptr<milvus::expr::ITypeExpr> typed_expr =
        std::make_shared<milvus::expr::TimestamptzArithCompareExpr>(
            expr::ColumnInfo(tstz_fid, DataType::TIMESTAMPTZ),
            proto::plan::ArithOpType::Add,
            interval,
            proto::plan::OpType::LessThan,
            compare_value);
    if (negated) {
        typed_expr = std::make_shared<milvus::expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
    }
    return typed_expr;
}

constexpr int64_t kFarFutureUs = 4102444800LL * 1000000;  // 2100-01-01
constexpr int64_t kFarPastUs = -2208988800LL * 1000000;   // 1900-01-01

}  // namespace

class TimestamptzArithCompareCorrectnessTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto pk_fid = schema_->AddDebugField("pk", DataType::INT64);
        tstz_fid_ = schema_->AddDebugField("ts", DataType::TIMESTAMPTZ, true);
        schema_->set_primary_field_id(pk_fid);

        dataset_ = std::make_unique<GeneratedData>(DataGen(schema_, N, 42));
        tstz_valid_ = dataset_->get_col_valid(tstz_fid_);

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
    AssertNullSemantics(SegmentInternalInterface* segment) {
        size_t nulls = 0;
        for (const auto valid : tstz_valid_) {
            nulls += valid ? 0 : 1;
        }
        ASSERT_GT(nulls, 0u);
        ASSERT_LT(nulls, N);

        struct Case {
            int64_t compare_us;
            bool negated;
            bool valid_row_expected;
        };
        const Case cases[] = {
            {kFarFutureUs, false, true},
            {kFarPastUs, true, true},
            {kFarPastUs, false, false},
            {kFarFutureUs, true, false},
        };

        for (const auto& c : cases) {
            auto typed_expr =
                MakeTstzPlusOneMonthCompare(tstz_fid_, c.compare_us, c.negated);
            auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
            auto final =
                query::ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            for (size_t i = 0; i < N; ++i) {
                const bool expected =
                    tstz_valid_[i] ? c.valid_row_expected : false;
                ASSERT_EQ(final[i], expected)
                    << "row " << i << " (valid=" << tstz_valid_[i]
                    << ") compare_us=" << c.compare_us
                    << " negated=" << c.negated;
            }
        }
    }

    void
    AssertOffsetInputNullSemantics(SegmentInternalInterface* segment) {
        std::vector<int32_t> null_rows, valid_rows;
        for (size_t i = 0; i < N; ++i) {
            (tstz_valid_[i] ? valid_rows : null_rows)
                .push_back(static_cast<int32_t>(i));
        }
        ASSERT_GE(null_rows.size(), 2u);
        ASSERT_GE(valid_rows.size(), 2u);

        OffsetVector offsets;
        offsets.emplace_back(null_rows.back());
        offsets.emplace_back(valid_rows.back());
        offsets.emplace_back(null_rows.front());
        offsets.emplace_back(valid_rows.front());

        auto typed_expr =
            MakeTstzPlusOneMonthCompare(tstz_fid_, kFarFutureUs, false);
        auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        auto col_vec = milvus::test::gen_filter_res(
            filter_node.get(), segment, N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView res(col_vec->GetRawData(), col_vec->size());
        ASSERT_EQ(res.size(), offsets.size());

        for (size_t k = 0; k < offsets.size(); ++k) {
            const bool expected = tstz_valid_[offsets[k]];
            ASSERT_EQ(res[k], expected)
                << "candidate k=" << k << " row=" << offsets[k];
        }
    }

    static constexpr size_t N = 32;

    SchemaPtr schema_;
    FieldId tstz_fid_;
    std::unique_ptr<GeneratedData> dataset_;
    FixedVector<bool> tstz_valid_;
    SegmentGrowingPtr growing_;
    std::unique_ptr<SegmentSealed> sealed_;
};

TEST_F(TimestamptzArithCompareCorrectnessTest, GrowingNullSemantics) {
    AssertNullSemantics(growing_.get());
}

TEST_F(TimestamptzArithCompareCorrectnessTest,
       GrowingOffsetInputNullSemantics) {
    AssertOffsetInputNullSemantics(growing_.get());
}

TEST_F(TimestamptzArithCompareCorrectnessTest, SealedNullSemantics) {
    AssertNullSemantics(sealed_.get());
}

TEST_F(TimestamptzArithCompareCorrectnessTest, SealedOffsetInputNullSemantics) {
    AssertOffsetInputNullSemantics(sealed_.get());
}
