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
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/OpContext.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"

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
    ToString() const override {
        if (!input_order_.empty()) {
            std::vector<std::string> inputs;
            inputs.reserve(input_order_.size());
            for (const auto& i : input_order_) {
                if (i < inputs_.size()) {
                    inputs.push_back(inputs_[i]->ToString());
                } else {
                    // Reserved position for runtime-created expressions (e.g., PhyLikeConjunctExpr)
                    // This can happen during optimization phase before the expression is actually created
                    inputs.push_back(
                        fmt::format("[RuntimeExpr:index={}:pending]", i));
                }
            }
            std::string input_str =
                is_and_ ? Join(inputs, " && ") : Join(inputs, " || ");
            return fmt::format("[ConjuctExpr:{}]", input_str);
        }
        // Fallback: no reordering applied yet
        std::vector<std::string> inputs;
        inputs.reserve(inputs_.size());
        for (const auto& in : inputs_) {
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

    // This conjunction feeds a null-rejecting consumer: one that treats an
    // UNKNOWN (NULL) output row exactly like FALSE. Under AND, an UNKNOWN
    // row can never become TRUE (UNKNOWN AND x is UNKNOWN or FALSE), so a
    // null-rejecting AND may stop evaluating it early; the row then stays
    // UNKNOWN instead of possibly collapsing to FALSE, which is
    // indistinguishable downstream. The property crosses AND/OR — their
    // combination cannot turn an operand's UNKNOWN into TRUE when the root
    // folds UNKNOWN into FALSE — so it propagates to the inputs; any other
    // node keeps the no-op default and stops the propagation.
    void
    MarkNullRejecting() override {
        null_rejecting_ = true;
        for (auto& input : inputs_) {
            input->MarkNullRejecting();
        }
    }

    bool
    IsNullRejecting() const {
        return null_rejecting_;
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

    void
    SetLikeIndices(std::vector<size_t>&& indices) {
        like_indices_ = std::move(indices);
    }

    const std::vector<size_t>&
    GetLikeIndices() const {
        return like_indices_;
    }

 private:
    // Build the bitmap of rows that still need the following expressions:
    // its count drives the batch-level early exit and the bitmap itself
    // becomes the row-level input of the next expression.
    TargetBitmap
    BuildActiveBitmap(const ColumnVectorPtr& vec);

    static DataType
    ResolveType(const std::vector<DataType>& inputs);

    void
    SkipFollowingExprs(int start);
    // true if conjunction (and), false if disjunction (or).
    bool is_and_;
    // true if the consumer of this expression's output treats UNKNOWN like
    // FALSE (see MarkNullRejecting).
    bool null_rejecting_{false};
    std::vector<size_t> input_order_;
    // Indices of LIKE expressions for potential batch ngram optimization (AND only)
    std::vector<size_t> like_indices_;
    // Flag to indicate if batch ngram optimization has been initialized
    bool like_batch_initialized_{false};
    // Indices of expressions executed via batch ngram (to skip in normal iteration)
    std::set<size_t> batch_ngram_indices_;
};
}  //namespace exec
}  // namespace milvus
