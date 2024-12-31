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

template <bool is_and>
struct ConjunctElementFunc {
    int64_t
    operator()(ColumnVectorPtr& input_result, ColumnVectorPtr& result) {
        TargetBitmapView input_data(input_result->GetRawData(),
                                    input_result->size());
        TargetBitmapView res_data(result->GetRawData(), result->size());

        /*
        // This is the original code, kept here for the documentation purposes        
        int64_t activate_rows = 0;
        for (int i = 0; i < result->size(); ++i) {
            if constexpr (is_and) {
                res_data[i] &= input_data[i];
                if (res_data[i]) {
                    activate_rows++;
                }
            } else {
                res_data[i] |= input_data[i];
                if (!res_data[i]) {
                    activate_rows++;
                }
            }
        }
        */

        if constexpr (is_and) {
            return (int64_t)res_data.inplace_and_with_count(input_data,
                                                            res_data.size());
        } else {
            return (int64_t)res_data.inplace_or_with_count(input_data,
                                                           res_data.size());
        }
    }
};

class PhyConjunctFilterExpr : public Expr {
 public:
    PhyConjunctFilterExpr(std::vector<ExprPtr>&& inputs, bool is_and)
        : Expr(DataType::BOOL, std::move(inputs), is_and ? "and" : "or"),
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
    std::vector<int32_t> input_order_;
};
}  //namespace exec
}  // namespace milvus
