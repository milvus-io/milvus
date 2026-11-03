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

#include "ConjunctExpr.h"

#include <folly/ScopeGuard.h>
#include "common/ValueOp.h"

namespace milvus {
namespace exec {

DataType
PhyConjunctFilterExpr::ResolveType(const std::vector<DataType>& inputs) {
    AssertInfo(
        inputs.size() > 0,
        fmt::format(
            "Conjunct expressions expect at least one argument, received: {}",
            inputs.size()));

    for (const auto& type : inputs) {
        AssertInfo(
            type == DataType::BOOL,
            fmt::format("Conjunct expressions expect BOOLEAN, received: {}",
                        type));
    }
    return DataType::BOOL;
}

TargetBitmap
PhyConjunctFilterExpr::BuildActiveBitmap(const ColumnVectorPtr& vec) {
    // AND still needs TRUE or UNKNOWN rows; OR needs FALSE or UNKNOWN rows.
    const size_t size = vec->size();
    TargetBitmapView data(vec->GetRawData(), size);
    TargetBitmapView valid(vec->GetValidRawData(), size);
    if (is_and_) {
        TargetBitmap active_rows(valid);
        active_rows.inplace_sub(data, size);  // valid & ~data
        active_rows.flip();                   // data | ~valid
        return active_rows;
    }
    TargetBitmap active_rows(data);
    active_rows.inplace_and(valid, size);  // data & valid
    active_rows.flip();                    // ~data | ~valid
    return active_rows;
}

void
PhyConjunctFilterExpr::SkipFollowingExprs(int start) {
    for (int i = start; i < input_order_.size(); ++i) {
        inputs_[input_order_[i]]->MoveCursor();
    }
}

void
PhyConjunctFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyConjunctFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("is_and", is_and_);

    auto inherited_bitmap = context.get_bitmap_input().clone();
    auto restore_bitmap = folly::makeGuard([&context, &inherited_bitmap] {
        context.set_bitmap_input(std::move(inherited_bitmap));
    });

    if (input_order_.empty()) {
        input_order_.resize(inputs_.size());
        for (size_t i = 0; i < inputs_.size(); i++) {
            input_order_[i] = i;
        }
    }
    for (size_t i = 0; i < input_order_.size(); ++i) {
        VectorPtr input_result;
        inputs_[input_order_[i]]->Eval(context, input_result);

        ColumnVectorPtr all_flat_result;
        if (i == 0) {
            result = input_result;
            all_flat_result = GetColumnVector(result);
        } else {
            auto input_flat_result = GetColumnVector(input_result);
            all_flat_result = GetColumnVector(result);
            if (is_and_) {
                common::ThreeValuedLogicOp::And(all_flat_result,
                                                input_flat_result);
            } else {
                common::ThreeValuedLogicOp::Or(all_flat_result,
                                               input_flat_result);
            }
        }

        // The last evaluated expression has no successor to prune or skip.
        if (i + 1 == input_order_.size()) {
            break;
        }

        auto active_rows = BuildActiveBitmap(all_flat_result);
        // Results outside the parent's candidates are unspecified.
        if (!inherited_bitmap.empty()) {
            active_rows.inplace_and(inherited_bitmap, active_rows.size());
        }
        if (active_rows.none()) {
            SkipFollowingExprs(i + 1);
            return;
        }
        context.set_bitmap_input(std::move(active_rows));
    }
}

}  //namespace exec
}  // namespace milvus
