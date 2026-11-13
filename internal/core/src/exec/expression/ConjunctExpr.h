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

    // Replace the whole inputs list (used by the GIS split-fusion pre-pass that
    // rewrites same-column geometry predicates into Coarse + Refine nodes).
    void
    RebuildInputs(std::vector<std::shared_ptr<Expr>>&& inputs) {
        inputs_ = std::move(inputs);
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

    bool
    IsAnd() {
        return is_and_;
    }

    bool
    IsOr() {
        return !is_and_;
    }

 private:
    // Rows that still need following expressions for this consumer.
    TargetBitmap
    BuildActiveBitmap(const ColumnVectorPtr& vec);

    static DataType
    ResolveType(const std::vector<DataType>& inputs);

    void
    SkipFollowingExprs(int start);
    // true if conjunction (and), false if disjunction (or).
    bool is_and_;
    // Whether the consumer treats UNKNOWN exactly like FALSE.
    bool null_rejecting_{false};
    std::vector<size_t> input_order_;
};
}  //namespace exec
}  // namespace milvus
