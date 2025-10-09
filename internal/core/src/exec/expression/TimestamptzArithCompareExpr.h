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
#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "exec/expression/Element.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "pb/plan.pb.h"

namespace milvus::exec {

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
                      expr->timestamp_column_.field_id_,
                      expr->timestamp_column_.nested_path_,
                      DataType::TIMESTAMPTZ,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const override;

    bool
    IsSource() const override {
        return true;
    }

 private:
    template <typename T>
    VectorPtr
    ExecCompareVisitorImpl(OffsetVector* input);

    template <typename T>
    VectorPtr
    ExecCompareVisitorImplForAll(OffsetVector* input);

 private:
    std::shared_ptr<PhyBinaryArithOpEvalRangeExpr> helperPhyExpr_;
    std::shared_ptr<const milvus::expr::TimestamptzArithCompareExpr> expr_;
    bool arg_inited_{false};
    proto::plan::Interval interval_;
    SingleElement compare_value_;
};

}  // namespace milvus::exec