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

#pragma once

#include <memory>
#include <string>

#include "exec/Driver.h"
#include "exec/expression/Expr.h"
#include "exec/operator/Operator.h"
#include "exec/QueryContext.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"

// difference between FilterBitsNode and RescoresNode is that
// FilterBitsNode will go through whole segment and return bitset to indicate which offset is filtered out or not
// RescoresNode will accept offsets array and execute over these and generate result valid offsets
namespace milvus::exec {
class PhyRescoresNode : public Operator {
 public:
    PhyRescoresNode(int32_t operator_id,
                    DriverContext* ctx,
                    const std::shared_ptr<const plan::RescoresNode>& scorer);

    bool
    IsFilter() override {
        return true;
    }

    bool
    NeedInput() const override {
        return !is_finished_;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override;

    void
    Close() override {
        Operator::Close();
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    virtual std::string
    ToString() const override {
        return "PhyRescoresNode";
    }

 private:
    std::vector<std::shared_ptr<rescores::Scorer>> scorers_;
    const proto::plan::ScoreOption* option_;
    bool is_finished_{false};
};

// Evaluate a boost filter whose expression cannot consume offset input
// (text match, GIS) over the whole segment. Each ExprSet::Eval call only
// advances the expression by one internal batch
// (DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE rows), while the topk offsets being
// scored may reference any row of the segment -- accumulate every batch so
// the returned bitset covers all active rows. UNKNOWN (NULL) rows are folded
// to FALSE so they never receive a boost.
TargetBitmap
EvalNonNativeBoostFilterAllBatches(ExecContext* exec_context,
                                   ExprSet* expr_set,
                                   EvalCtx& eval_ctx,
                                   const expr::TypedExprPtr& filter);
}  // namespace milvus::exec
