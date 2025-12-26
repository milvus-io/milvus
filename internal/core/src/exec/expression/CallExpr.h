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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/OpContext.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/FunctionFactory.h"
#include "expr/ITypeExpr.h"
#include "fmt/core.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyCallExpr : public Expr {
 public:
    PhyCallExpr(const std::vector<std::shared_ptr<Expr>>& input,
                const std::shared_ptr<const milvus::expr::CallExpr>& expr,
                const std::string& name,
                milvus::OpContext* op_ctx,
                const segcore::SegmentInternalInterface* segment,
                int64_t active_count,
                int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name, op_ctx), expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            for (auto input : inputs_) {
                input->MoveCursor();
            }
        }
    }

    std::string
    ToString() const {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return false;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    bool
    CanExecuteAllAtOnce() const override {
        for (const auto& input : inputs_) {
            if (!input->CanExecuteAllAtOnce()) {
                return false;
            }
        }
        return true;
    }

    void
    SetExecuteAllAtOnce() override {
        for (auto& input : inputs_) {
            input->SetExecuteAllAtOnce();
        }
    }

 private:
    std::shared_ptr<const milvus::expr::CallExpr> expr_;
};

}  // namespace exec
}  // namespace milvus
