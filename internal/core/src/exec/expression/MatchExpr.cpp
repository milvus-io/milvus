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
#include <numeric>
#include <utility>
#include "common/Tracer.h"
#include "common/Types.h"

namespace milvus {
namespace exec {

using MatchType = milvus::expr::MatchType;

template <MatchType match_type, bool all_valid>
void
ProcessMatchRows(int64_t row_count,
                 const IArrayOffsets* array_offsets,
                 const TargetBitmapView& match_bitset,
                 const TargetBitmapView& valid_bitset,
                 TargetBitmapView& result_bitset,
                 int64_t threshold) {
    for (int64_t i = 0; i < row_count; ++i) {
        auto [first_elem, last_elem] = array_offsets->ElementIDRangeOfRow(i);
        int64_t hit_count = 0;
        int64_t element_count = last_elem - first_elem;
        bool early_fail = false;

        if constexpr (all_valid) {
            for (auto j = first_elem; j < last_elem; ++j) {
                bool matched = match_bitset[j];
                if (matched) {
                    ++hit_count;
                }

                if constexpr (match_type == MatchType::MatchAny) {
                    if (hit_count > 0) {
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchAll) {
                    if (!matched) {
                        early_fail = true;
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchLeast) {
                    if (hit_count >= threshold) {
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchMost ||
                                     match_type == MatchType::MatchExact) {
                    if (hit_count > threshold) {
                        early_fail = true;
                        break;
                    }
                }
            }
        } else {
            element_count = 0;
            for (auto j = first_elem; j < last_elem; ++j) {
                if (!valid_bitset[j]) {
                    continue;
                }
                ++element_count;
                bool matched = match_bitset[j];
                if (matched) {
                    ++hit_count;
                }

                if constexpr (match_type == MatchType::MatchAny) {
                    if (hit_count > 0) {
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchAll) {
                    if (!matched) {
                        early_fail = true;
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchLeast) {
                    if (hit_count >= threshold) {
                        break;
                    }
                } else if constexpr (match_type == MatchType::MatchMost ||
                                     match_type == MatchType::MatchExact) {
                    if (hit_count > threshold) {
                        early_fail = true;
                        break;
                    }
                }
            }
        }

        if (early_fail) {
            continue;
        }

        bool is_match = false;
        if constexpr (match_type == MatchType::MatchAny) {
            is_match = hit_count > 0;
        } else if constexpr (match_type == MatchType::MatchAll) {
            is_match = hit_count == element_count;
        } else if constexpr (match_type == MatchType::MatchLeast) {
            is_match = hit_count >= threshold;
        } else if constexpr (match_type == MatchType::MatchMost) {
            is_match = hit_count <= threshold;
        } else if constexpr (match_type == MatchType::MatchExact) {
            is_match = hit_count == threshold;
        }

        if (is_match) {
            result_bitset[i] = true;
        }
    }
}

template <MatchType match_type>
void
DispatchByValidity(bool all_valid,
                   int64_t row_count,
                   const IArrayOffsets* array_offsets,
                   const TargetBitmapView& match_bitset,
                   const TargetBitmapView& valid_bitset,
                   TargetBitmapView& result_bitset,
                   int64_t threshold) {
    if (all_valid) {
        ProcessMatchRows<match_type, true>(row_count,
                                           array_offsets,
                                           match_bitset,
                                           valid_bitset,
                                           result_bitset,
                                           threshold);
    } else {
        ProcessMatchRows<match_type, false>(row_count,
                                            array_offsets,
                                            match_bitset,
                                            valid_bitset,
                                            result_bitset,
                                            threshold);
    }
}

void
PhyMatchFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span("PhyMatchFilterExpr::Eval", tracer::GetRootSpan());

    auto input = context.get_offset_input();
    AssertInfo(input != nullptr,
               "Offset input in match filter expr is not implemented now");

    auto schema = segment_->get_schema();
    auto field_meta =
        schema.GetFirstArrayFieldInStruct(expr_->get_struct_name());

    auto array_offsets = segment_->GetArrayOffsets(field_meta.get_id());
    AssertInfo(array_offsets != nullptr, "Array offsets not available");

    int64_t row_count =
        context.get_exec_context()->get_query_context()->get_active_count();
    result = std::make_shared<ColumnVector>(TargetBitmap(row_count, false),
                                            TargetBitmap(row_count, true));

    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);
    auto col_vec_size = col_vec->size();
    AssertInfo(col_vec != nullptr, "Result should be ColumnVector");
    AssertInfo(col_vec->IsBitmap(), "Result should be bitmap");
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec_size);

    auto [total_elements, _] = array_offsets->ElementIDRangeOfRow(row_count);
    FixedVector<int32_t> element_offsets(total_elements);
    std::iota(element_offsets.begin(), element_offsets.end(), 0);

    EvalCtx eval_ctx(context.get_exec_context(), &element_offsets);

    VectorPtr match_result;
    // TODO(SpadeA): can be executed in batch
    inputs_[0]->Eval(eval_ctx, match_result);
    auto match_result_col_vec =
        std::dynamic_pointer_cast<ColumnVector>(match_result);
    AssertInfo(match_result_col_vec != nullptr,
               "Match result should be ColumnVector");
    AssertInfo(match_result_col_vec->IsBitmap(),
               "Match result should be bitmap");
    TargetBitmapView match_result_bitset_view(
        match_result_col_vec->GetRawData(), match_result_col_vec->size());
    TargetBitmapView match_result_valid_view(
        match_result_col_vec->GetValidRawData(), match_result_col_vec->size());

    bool all_valid = match_result_valid_view.all();
    auto match_type = expr_->get_match_type();
    int64_t threshold = expr_->get_count();

    switch (match_type) {
        case MatchType::MatchAny:
            DispatchByValidity<MatchType::MatchAny>(all_valid,
                                                    row_count,
                                                    array_offsets.get(),
                                                    match_result_bitset_view,
                                                    match_result_valid_view,
                                                    bitset_view,
                                                    threshold);
            break;
        case MatchType::MatchAll:
            DispatchByValidity<MatchType::MatchAll>(all_valid,
                                                    row_count,
                                                    array_offsets.get(),
                                                    match_result_bitset_view,
                                                    match_result_valid_view,
                                                    bitset_view,
                                                    threshold);
            break;
        case MatchType::MatchLeast:
            DispatchByValidity<MatchType::MatchLeast>(all_valid,
                                                      row_count,
                                                      array_offsets.get(),
                                                      match_result_bitset_view,
                                                      match_result_valid_view,
                                                      bitset_view,
                                                      threshold);
            break;
        case MatchType::MatchMost:
            DispatchByValidity<MatchType::MatchMost>(all_valid,
                                                     row_count,
                                                     array_offsets.get(),
                                                     match_result_bitset_view,
                                                     match_result_valid_view,
                                                     bitset_view,
                                                     threshold);
            break;
        case MatchType::MatchExact:
            DispatchByValidity<MatchType::MatchExact>(all_valid,
                                                      row_count,
                                                      array_offsets.get(),
                                                      match_result_bitset_view,
                                                      match_result_valid_view,
                                                      bitset_view,
                                                      threshold);
            break;
        default:
            ThrowInfo(OpTypeInvalid,
                      "Unsupported match type: {}",
                      static_cast<int>(match_type));
    }
}

}  // namespace exec
}  // namespace milvus
