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

#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyJsonContainsFilterExpr : public SegmentExpr {
 public:
    PhyJsonContainsFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::JsonContainsExpr>& expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      expr->vals_.empty()
                          ? DataType::NONE
                          : FromValCase(expr->vals_[0].val_case()),
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

 private:
    VectorPtr
    EvalJsonContainsForDataSegment(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContains(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsByKeyIndex();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContains(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAll(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAllByKeyIndex();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsAll(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArrayByKeyIndex();

    VectorPtr
    ExecJsonContainsAllArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllArrayByKeyIndex();

    VectorPtr
    ExecJsonContainsAllWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllWithDiffTypeByKeyIndex();

    VectorPtr
    ExecJsonContainsWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsWithDiffTypeByKeyIndex();

    VectorPtr
    EvalArrayContainsForIndexSegment(DataType data_type);

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsForIndexSegmentImpl();

 private:
    std::shared_ptr<const milvus::expr::JsonContainsExpr> expr_;
    bool arg_inited_{false};
    std::shared_ptr<MultiElement> arg_set_;
};
}  //namespace exec
}  // namespace milvus
