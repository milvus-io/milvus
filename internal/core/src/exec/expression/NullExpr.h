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
#include "expr/ITypeExpr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyNullExpr : public SegmentExpr {
 public:
    PhyNullExpr(const std::vector<std::shared_ptr<Expr>>& input,
                const std::shared_ptr<const milvus::expr::NullExpr>& expr,
                const std::string& name,
                const segcore::SegmentInternalInterface* segment,
                int64_t active_count,
                int64_t batch_size,
                int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

 private:
    ColumnVectorPtr
    PreCheckNullable(OffsetVector* input);

    template <typename T>
    VectorPtr
    ExecVisitorImpl(OffsetVector* input);

 private:
    std::shared_ptr<const milvus::expr::NullExpr> expr_;
    ColumnVectorPtr cached_precheck_res_{nullptr};
    int64_t precheck_pos_{0};
};
}  //namespace exec
}  // namespace milvus