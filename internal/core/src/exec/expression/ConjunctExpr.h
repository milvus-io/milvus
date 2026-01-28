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
#include "common/OpContext.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyConjunctFilterExpr : public Expr {
 public:
    PhyConjunctFilterExpr(std::vector<ExprPtr>&& inputs,
                          bool is_and,
                          milvus::OpContext* op_ctx)
        : Expr(DataType::BOOL,
               std::move(inputs),
               "PhyConjunctFilterExpr",
               op_ctx),
          is_and_(is_and) {
        std::vector<DataType> input_types;
        input_types.reserve(inputs_.size());

        std::transform(inputs_.begin(),
                       inputs_.end(),
                       std::back_inserter(input_types),
                       [](const ExprPtr& expr) { return expr->type(); });

        ResolveType(input_types);
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            for (auto& input : inputs_) {
                input->MoveCursor();
            }
        }
    }

    bool
    SupportOffsetInput() override {
        for (auto& input : inputs_) {
            if (!(input->SupportOffsetInput())) {
                return false;
            }
        }
        return true;
    }

    std::string
    ToString() const {
        if (!input_order_.empty()) {
            std::vector<std::string> inputs;
            for (auto& i : input_order_) {
                inputs.push_back(inputs_[i]->ToString());
            }
            std::string input_str =
                is_and_ ? Join(inputs, " && ") : Join(inputs, " || ");
            return fmt::format("[ConjuctExpr:{}]", input_str);
        }
        std::vector<std::string> inputs;
        for (auto& in : inputs_) {
            inputs.push_back(in->ToString());
        }
        std::string input_str =
            is_and_ ? Join(inputs, " && ") : Join(inputs, "||");
        return fmt::format("[ConjuctExpr:{}]", input_str);
    }

    bool
    IsSource() const override {
        return false;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    void
    Reorder(const std::vector<size_t>& exprs_order) {
        input_order_ = exprs_order;
    }

    std::vector<size_t>
    GetReorder() {
        return input_order_;
    }

    // Add a new expression to inputs and return its index
    size_t
    AddInput(std::shared_ptr<Expr> expr) {
        inputs_.push_back(std::move(expr));
        return inputs_.size() - 1;
    }

    // Set the bitmap input for the next expression in the conjunction.
    // The bitmap indicates which rows still need to be evaluated.
    //
    // For AND: A row needs evaluation if it's currently TRUE or NULL
    //   - TRUE rows: need to check if they remain TRUE after AND
    //   - NULL rows: need to check if result becomes FALSE (NULL AND FALSE = FALSE)
    //   - FALSE rows: already determined, no need to evaluate
    //   => bitmap = data | ~valid (TRUE or NULL)
    //
    // For OR: A row needs evaluation if it's currently FALSE or NULL
    //   - FALSE rows: need to check if they become TRUE after OR
    //   - NULL rows: need to check if result becomes TRUE (NULL OR TRUE = TRUE)
    //   - TRUE rows: already determined, no need to evaluate
    //   => bitmap = ~data | ~valid (FALSE or NULL)
    void
    SetNextExprBitmapInput(const ColumnVectorPtr& vec, EvalCtx& context) {
        const size_t size = vec->size();
        TargetBitmapView data(vec->GetRawData(), size);
        TargetBitmapView valid(vec->GetValidRawData(), size);

        if (is_and_) {
            // bitmap = data | ~valid
            // Using De Morgan's law: data | ~valid = ~(~data & valid) = ~(valid & ~data)
            // Use inplace_sub which computes: this = this & ~other
            TargetBitmap next_input_bitmap(valid);      // copy valid
            next_input_bitmap.inplace_sub(data, size);  // valid & ~data
            next_input_bitmap.flip();  // ~(valid & ~data) = data | ~valid
            context.set_bitmap_input(std::move(next_input_bitmap));
        } else {
            // bitmap = ~data | ~valid
            // Using De Morgan's law: ~data | ~valid = ~(data & valid)
            TargetBitmap next_input_bitmap(data);        // copy data
            next_input_bitmap.inplace_and(valid, size);  // data & valid
            next_input_bitmap.flip();  // ~(data & valid) = ~data | ~valid
            context.set_bitmap_input(std::move(next_input_bitmap));
        }
    }

    void
    ClearBitmapInput(EvalCtx& context) {
        context.clear_bitmap_input();
    }

    bool
    IsAnd() {
        return is_and_;
    }

    bool
    IsOr() {
        return !is_and_;
    }

 private:
    int64_t
    UpdateResult(ColumnVectorPtr& input_result,
                 EvalCtx& ctx,
                 ColumnVectorPtr& result);

    static DataType
    ResolveType(const std::vector<DataType>& inputs);

    bool
    CanSkipFollowingExprs(ColumnVectorPtr& vec);

    void
    SkipFollowingExprs(int start);
    // true if conjunction (and), false if disjunction (or).
    bool is_and_;
    std::vector<size_t> input_order_;
};
}  //namespace exec
}  // namespace milvus
