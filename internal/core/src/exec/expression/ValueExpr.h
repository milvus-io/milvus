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
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyValueExpr : public Expr {
 public:
    PhyValueExpr(const std::vector<std::shared_ptr<Expr>>& input,
                 const std::shared_ptr<const milvus::expr::ValueExpr> expr,
                 const std::string& name,
                 const segcore::SegmentInternalInterface* segment,
                 int64_t active_count,
                 int64_t batch_size)
        : Expr(expr->type(), std::move(input), name),
          expr_(expr),
          active_count_(active_count),
          batch_size_(batch_size) {
        AssertInfo(input.empty(),
                   "PhyValueExpr should not have input, but got " +
                       std::to_string(input.size()));
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
        int64_t real_batch_size = current_pos_ + batch_size_ >= active_count_
                                      ? active_count_ - current_pos_
                                      : batch_size_;

        current_pos_ += real_batch_size;
    }

 private:
    std::shared_ptr<const milvus::expr::ValueExpr> expr_;
    const int64_t active_count_;
    int64_t current_pos_{0};
    const int64_t batch_size_;
};

}  //namespace exec
}  // namespace milvus
