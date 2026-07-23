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

#include <algorithm>
#include <cstddef>

#include "bitset/bitset.h"
#include "common/ArrayOffsets.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "folly/FBVector.h"

namespace milvus {
namespace exec {

using MatchType = milvus::expr::MatchType;

// Core matching logic for a single row's elements
// Returns true if the row matches the condition
template <MatchType match_type, bool all_valid>
bool
MatchSingleRow(int64_t bitset_start,
               int64_t row_elem_count,
               const TargetBitmapView& match_bitset,
               const TargetBitmapView& valid_bitset,
               int64_t threshold) {
    int64_t hit_count = 0;
    int64_t element_count = row_elem_count;

    if constexpr (all_valid) {
        for (int64_t j = 0; j < row_elem_count; ++j) {
            bool matched = match_bitset[bitset_start + j];
            if (matched) {
                ++hit_count;
            }

            // Early exit conditions
            if constexpr (match_type == MatchType::MatchAny) {
                if (hit_count > 0)
                    return true;
            } else if constexpr (match_type == MatchType::MatchAll) {
                if (!matched)
                    return false;
            } else if constexpr (match_type == MatchType::MatchLeast) {
                if (hit_count >= threshold)
                    return true;
            } else if constexpr (match_type == MatchType::MatchMost ||
                                 match_type == MatchType::MatchExact) {
                if (hit_count > threshold)
                    return false;
            }
        }
    } else {
        element_count = 0;
        for (int64_t j = 0; j < row_elem_count; ++j) {
            if (!valid_bitset[bitset_start + j]) {
                continue;
            }
            ++element_count;
            bool matched = match_bitset[bitset_start + j];
            if (matched) {
                ++hit_count;
            }

            // Early exit conditions
            if constexpr (match_type == MatchType::MatchAny) {
                if (hit_count > 0)
                    return true;
            } else if constexpr (match_type == MatchType::MatchAll) {
                if (!matched)
                    return false;
            } else if constexpr (match_type == MatchType::MatchLeast) {
                if (hit_count >= threshold)
                    return true;
            } else if constexpr (match_type == MatchType::MatchMost ||
                                 match_type == MatchType::MatchExact) {
                if (hit_count > threshold)
                    return false;
            }
        }
    }

    // Final match decision
    if constexpr (match_type == MatchType::MatchAny) {
        return hit_count > 0;
    } else if constexpr (match_type == MatchType::MatchAll) {
        // Empty array returns true (vacuous truth)
        return hit_count == element_count;
    } else if constexpr (match_type == MatchType::MatchLeast) {
        return hit_count >= threshold;
    } else if constexpr (match_type == MatchType::MatchMost) {
        return hit_count <= threshold;
    } else if constexpr (match_type == MatchType::MatchExact) {
        return hit_count == threshold;
    }
    return false;
}

bool
MatchEmptyElements(MatchType match_type, int64_t threshold) {
    switch (match_type) {
        case MatchType::MatchAny:
            return false;
        case MatchType::MatchAll:
            return true;
        case MatchType::MatchLeast:
            return threshold <= 0;
        case MatchType::MatchMost:
            return threshold >= 0;
        case MatchType::MatchExact:
            return threshold == 0;
        default:
            ThrowInfo(OpTypeInvalid,
                      "Unsupported match type: {}",
                      static_cast<int>(match_type));
    }
    return false;
}

// Process contiguous rows [row_start, row_start + row_count)
template <MatchType match_type, bool all_valid>
void
ProcessContiguousRows(int64_t row_count,
                      int64_t row_start,
                      int64_t elem_start,
                      const IArrayOffsets* array_offsets,
                      const TargetBitmapView& match_bitset,
                      const TargetBitmapView& valid_bitset,
                      TargetBitmapView& result_bitset,
                      int64_t threshold) {
    for (int64_t i = 0; i < row_count; ++i) {
        auto [first_elem, last_elem] =
            array_offsets->ElementIDRangeOfRow(row_start + i);
        int64_t bitset_start = first_elem - elem_start;
        int64_t row_elem_count = last_elem - first_elem;

        bool matched = MatchSingleRow<match_type, all_valid>(bitset_start,
                                                             row_elem_count,
                                                             match_bitset,
                                                             valid_bitset,
                                                             threshold);
        if (matched) {
            result_bitset[i] = true;
        }
    }
}

// Process non-contiguous rows specified by offset_input
template <MatchType match_type, bool all_valid>
void
ProcessOffsetRows(const OffsetVector* row_offsets,
                  const IArrayOffsets* array_offsets,
                  const TargetBitmapView& match_bitset,
                  const TargetBitmapView& valid_bitset,
                  TargetBitmapView& result_bitset,
                  int64_t threshold) {
    int64_t elem_cursor = 0;
    for (size_t i = 0; i < row_offsets->size(); ++i) {
        auto [first_elem, last_elem] =
            array_offsets->ElementIDRangeOfRow((*row_offsets)[i]);
        int64_t row_elem_count = last_elem - first_elem;

        if (MatchSingleRow<match_type, all_valid>(elem_cursor,
                                                  row_elem_count,
                                                  match_bitset,
                                                  valid_bitset,
                                                  threshold)) {
            result_bitset[i] = true;
        }
        elem_cursor += row_elem_count;
    }
}

template <MatchType match_type, bool all_valid>
void
DispatchMatchProcessing(bool use_offset_input,
                        int64_t row_count,
                        int64_t row_start,
                        int64_t elem_start,
                        const OffsetVector* row_offsets,
                        const IArrayOffsets* array_offsets,
                        const TargetBitmapView& match_bitset,
                        const TargetBitmapView& valid_bitset,
                        TargetBitmapView& result_bitset,
                        int64_t threshold) {
    if (use_offset_input) {
        ProcessOffsetRows<match_type, all_valid>(row_offsets,
                                                 array_offsets,
                                                 match_bitset,
                                                 valid_bitset,
                                                 result_bitset,
                                                 threshold);
    } else {
        ProcessContiguousRows<match_type, all_valid>(row_count,
                                                     row_start,
                                                     elem_start,
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
    SetHasOffsetInput(input != nullptr);

    auto field_meta =
        schema_snapshot_->GetFirstArrayFieldInStruct(expr_->get_struct_name());

    auto array_offsets = segment_->GetArrayOffsets(field_meta.get_id());
    AssertInfo(array_offsets != nullptr, "Array offsets not available");

    int64_t batch_rows;
    int64_t elem_start;
    int64_t elem_count;
    FixedVector<int32_t> element_offsets_storage;
    EvalCtx eval_ctx(context.get_exec_context());

    if (has_offset_input_) {
        // offset_input mode: process all input row_ids at once
        batch_rows = input->size();
        element_offsets_storage =
            array_offsets->RowOffsetsToElementOffsets(*input);
        eval_ctx.set_offset_input(&element_offsets_storage);
        elem_start = 0;
        elem_count = element_offsets_storage.size();
    } else {
        // Sequential batch mode
        batch_rows = std::min(batch_size_, active_count_ - current_pos_);
        auto start_range = array_offsets->ElementIDRangeOfRow(current_pos_);
        auto end_range =
            array_offsets->ElementIDRangeOfRow(current_pos_ + batch_rows);
        elem_start = start_range.first;
        elem_count = end_range.first - elem_start;
    }

    if (batch_rows <= 0) {
        result = nullptr;
        return;
    }

    result = std::make_shared<ColumnVector>(TargetBitmap(batch_rows, false),
                                            TargetBitmap(batch_rows, true));

    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(result);
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());

    auto match_type = expr_->get_match_type();
    int64_t threshold = expr_->get_count();

    VectorPtr match_result;
    if (elem_count > 0) {
        inputs_[0]->Eval(eval_ctx, match_result);
    } else if (!has_offset_input_) {
        // Keep element-level child expressions aligned across all-empty batches.
        inputs_[0]->MoveCursor();
    }
    if (match_result == nullptr) {
        AssertInfo(elem_count == 0,
                   "Match child returned empty result for non-empty element "
                   "batch, elem_count={}",
                   elem_count);
        if (MatchEmptyElements(match_type, threshold)) {
            bitset_view.set();
        }
        ApplyStructRowValidity(
            col_vec.get(), field_meta.get_id(), input, batch_rows);
        if (!has_offset_input_) {
            current_pos_ += batch_rows;
        }
        return;
    }
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

    auto dispatch = [&]<bool all_valid_v>() {
        switch (match_type) {
            case MatchType::MatchAny:
                DispatchMatchProcessing<MatchType::MatchAny, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchAll:
                DispatchMatchProcessing<MatchType::MatchAll, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchLeast:
                DispatchMatchProcessing<MatchType::MatchLeast, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchMost:
                DispatchMatchProcessing<MatchType::MatchMost, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
                    array_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchExact:
                DispatchMatchProcessing<MatchType::MatchExact, all_valid_v>(
                    has_offset_input_,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    input,
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
    };

    if (all_valid) {
        dispatch.template operator()<true>();
    } else {
        dispatch.template operator()<false>();
    }

    ApplyStructRowValidity(
        col_vec.get(), field_meta.get_id(), input, batch_rows);
    if (!has_offset_input_) {
        current_pos_ += batch_rows;
    }
}

void
PhyMatchFilterExpr::ApplyStructRowValidity(ColumnVector* col_vec,
                                           FieldId field_id,
                                           const OffsetVector* input,
                                           int64_t batch_rows) {
    TargetBitmapView value_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    if (input != nullptr) {
        AssertInfo(static_cast<int64_t>(input->size()) == batch_rows,
                   "offset input size {} does not match batch row count {}",
                   input->size(),
                   batch_rows);
        std::vector<int64_t> row_offsets(batch_rows);
        for (int64_t i = 0; i < batch_rows; ++i) {
            row_offsets[i] = static_cast<int64_t>((*input)[i]);
        }
        segment_->ApplyFieldValidDataByOffsets(
            op_ctx_, field_id, row_offsets.data(), batch_rows, valid_view);
    } else {
        int64_t processed = 0;
        int64_t row_offset = current_pos_;
        while (processed < batch_rows) {
            auto [chunk_id, offset_in_chunk] =
                segment_->get_chunk_by_offset(field_id, row_offset);
            auto count = std::min(
                batch_rows - processed,
                segment_->chunk_size(field_id, chunk_id) - offset_in_chunk);
            AssertInfo(count > 0,
                       "invalid field validity range at row offset {} for "
                       "field {}",
                       row_offset,
                       field_id.get());
            segment_->ApplyFieldValidData(op_ctx_,
                                          field_id,
                                          chunk_id,
                                          offset_in_chunk,
                                          count,
                                          valid_view + processed);
            processed += count;
            row_offset += count;
        }
    }

    for (int64_t i = 0; i < batch_rows; ++i) {
        if (!valid_view[i]) {
            value_view[i] = false;
        }
    }
}

}  // namespace exec
}  // namespace milvus
