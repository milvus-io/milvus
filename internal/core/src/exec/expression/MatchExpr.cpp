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

#include "MatchExpr.h"
#include <utility>
#include "common/Tracer.h"
#include "common/Types.h"

namespace milvus {
namespace exec {

void PhyMatchFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span("PhyMatchFilterExpr::Eval", tracer::GetRootSpan());

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));

    auto schema = segment_->get_schema();
    auto field_meta =
        schema.GetFirstArrayFieldInStruct(expr_->get_struct_name());

    auto array_offsets = segment_->GetArrayOffsets(field_meta.get_id());
    AssertInfo(array_offsets != nullptr, "Array offsets not available");

    int64_t row_count =
        context.get_exec_context()->get_query_context()->get_active_count();
    result = std::make_shared<ColumnVector>(TargetBitmap(row_count, false),
                                            TargetBitmap(row_count, true));
    auto element_count = array_offsets->ElementIDRangeOfRow(row_count).first;

    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);
    auto col_vec_size = col_vec->size();
    AssertInfo(col_vec != nullptr, "Result should be ColumnVector");
    AssertInfo(col_vec->IsBitmap(), "Result should be bitmap");
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec_size);

    // FIXME: make it more efficient
    bitset_view.flip();
    FixedVector<int32_t> element_offsets =
        array_offsets->RowBitsetToElementOffsets(bitset_view, 0);
    bitset_view.flip();

    EvalCtx eval_ctx(context.get_exec_context(), &element_offsets);

    VectorPtr match_result;
    inputs_[0]->Eval(eval_ctx, match_result);
    auto match_result_col_vec =
        std::dynamic_pointer_cast<ColumnVector>(match_result);
    AssertInfo(match_result_col_vec != nullptr,
               "Match result should be ColumnVector");
    AssertInfo(match_result_col_vec->IsBitmap(),
               "Match result should be bitmap");
    TargetBitmapView match_result_bitset_view(
        match_result_col_vec->GetRawData(), match_result_col_vec->size());

    for (int64_t i = 0; i < row_count; ++i) {
        auto [first_elem, last_elem] = array_offsets->ElementIDRangeOfRow(i);
        int64_t hit_count = 0;
        for (auto j = first_elem; j < last_elem; ++j) {
            if (match_result_bitset_view[j]) {
                ++hit_count;
            }
        }
        if (MatchExprHit(hit_count, last_elem - first_elem)) {
            bitset_view[i] = true;
        }
    }
}

}  // namespace exec
}  // namespace milvus
