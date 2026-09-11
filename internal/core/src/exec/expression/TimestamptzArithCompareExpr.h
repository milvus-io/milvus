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

#pragma once

#include <memory>
#include "common/Vector.h"
#include "exec/expression/Element.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "pb/plan.pb.h"

namespace milvus::exec {

namespace detail {
bool
EvaluateTimestamp(int64_t current_ts_us,
                  proto::plan::ArithOpType arith_op,
                  const proto::plan::Interval& interval,
                  proto::plan::OpType compare_op,
                  int64_t compare_us);
}  // namespace detail

// TIMESTAMPTZ `column (+|-) interval <cmp> value` over raw int64 microseconds.
struct TimestamptzArithCompareKernel {
    proto::plan::ArithOpType arith_op;
    proto::plan::OpType compare_op;
    const proto::plan::Interval* interval;
    int64_t compare_us;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<int64_t>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            // A NULL row's payload is a placeholder; arithmetic on it may throw.
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (!b.IsCandidate(i)) {
                continue;
            }
            if (detail::EvaluateTimestamp(
                    b.data[i], arith_op, *interval, compare_op, compare_us)) {
                out.SetTrue(i);
            }
        }
    }
};

class PhyTimestamptzArithCompareExpr : public SegmentExpr {
 public:
    PhyTimestamptzArithCompareExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::TimestamptzArithCompareExpr>&
            expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::TIMESTAMPTZ,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override;

    std::string
    ToString() const override;

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    template <typename T>
    VectorPtr
    ExecCompareVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecCompareVisitorImplForAll(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecCompareVisitorImplForIndex(OffsetVector* input);

 private:
    std::shared_ptr<const milvus::expr::TimestamptzArithCompareExpr> expr_;
    bool arg_inited_{false};
    proto::plan::Interval interval_;
    SingleElement compare_value_;
};

}  // namespace milvus::exec
