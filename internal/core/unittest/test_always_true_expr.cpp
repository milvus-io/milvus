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

#include <gtest/gtest.h>
#include <memory>
#include <regex>
#include <vector>
#include <chrono>

#include "common/Types.h"
#include "query/Expr.h"
#include "query/generated/ExecExprVisitor.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"

class ExprAlwaysTrueTest : public ::testing::TestWithParam<milvus::DataType> {};

INSTANTIATE_TEST_SUITE_P(
    ExprAlwaysTrueParameters,
    ExprAlwaysTrueTest,
    ::testing::Values(milvus::DataType::VECTOR_FLOAT,
                      milvus::DataType::VECTOR_SPARSE_FLOAT));

TEST_P(ExprAlwaysTrueTest, AlwaysTrue) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    auto data_type = GetParam();
    auto metric_type = data_type == DataType::VECTOR_FLOAT
                           ? knowhere::metric::L2
                           : knowhere::metric::IP;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField("fakevec", data_type, 16, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_age_col = raw_data.get_col<int>(i64_fid);
        age_col.insert(age_col.end(), new_age_col.begin(), new_age_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    query::ExecPlanNodeVisitor visitor(*seg_promote, MAX_TIMESTAMP);
    auto expr = std::make_shared<milvus::expr::AlwaysTrueExpr>();
    BitsetType final;
    std::shared_ptr<milvus::plan::PlanNode> plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    visitor.ExecuteExprNode(plan, seg_promote, N * num_iters, final);
    EXPECT_EQ(final.size(), N * num_iters);

    for (int i = 0; i < N * num_iters; ++i) {
        auto ans = final[i];

        auto val = age_col[i];
        ASSERT_EQ(ans, true) << "@" << i << "!!" << val;
    }
}
