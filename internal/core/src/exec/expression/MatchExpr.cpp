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
//
// Word-wise implementation: the row's element bits are consumed in 64-bit
// chunks (bitmap word width) instead of bit by bit. Invalid elements are
// masked out with the valid bitmap, which preserves the per-bit semantics:
// only valid elements are counted, and MatchAll requires every *valid*
// element to match (vacuously true when the row has no valid elements).
template <MatchType match_type, bool all_valid>
bool
MatchSingleRow(int64_t bitset_start,
               int64_t row_elem_count,
               const TargetBitmapView& match_bitset,
               const TargetBitmapView& valid_bitset,
               int64_t threshold) {
    using word_t = TargetBitmapView::policy_type::data_type;
    constexpr int64_t kWordBits = static_cast<int64_t>(8 * sizeof(word_t));

    int64_t hit_count = 0;

    for (int64_t done = 0; done < row_elem_count; done += kWordBits) {
        const size_t n =
            static_cast<size_t>(std::min(kWordBits, row_elem_count - done));
        const size_t pos = static_cast<size_t>(bitset_start + done);
        word_t m = match_bitset.read(pos, n);
        if constexpr (all_valid) {
            if constexpr (match_type == MatchType::MatchAll) {
                const word_t full = (n == static_cast<size_t>(kWordBits))
                                        ? ~word_t(0)
                                        : ((word_t(1) << n) - 1);
                if (m != full) {
                    return false;
                }
                continue;
            }
        } else {
            const word_t v = valid_bitset.read(pos, n);
            m &= v;
            if constexpr (match_type == MatchType::MatchAll) {
                // Some valid element unmatched?
                if (m != v) {
                    return false;
                }
                continue;
            }
        }
        if constexpr (match_type == MatchType::MatchAny) {
            if (m != 0) {
                return true;
            }
        } else if constexpr (match_type == MatchType::MatchLeast) {
            hit_count += __builtin_popcountll(m);
            if (hit_count >= threshold) {
                return true;
            }
        } else if constexpr (match_type == MatchType::MatchMost ||
                             match_type == MatchType::MatchExact) {
            hit_count += __builtin_popcountll(m);
            if (hit_count > threshold) {
                return false;
            }
        }
    }

    // Final match decision
    if constexpr (match_type == MatchType::MatchAny) {
        return false;
    } else if constexpr (match_type == MatchType::MatchAll) {
        // Every (valid) element matched; empty array returns true
        // (vacuous truth).
        return true;
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
    if constexpr (match_type == MatchType::MatchAny && all_valid) {
        // Fast path: ANY-semantics reduction over contiguous rows. The match
        // bitmap covers exactly the element ranges of
        // [row_start, row_start + row_count) shifted by elem_start; jump
        // between set bits word-wise instead of walking rows element by
        // element.
        array_offsets->ElementBitsetToRowBitsetAny(
            match_bitset, elem_start, row_start, result_bitset);
        return;
    }
    // ONE batched virtual call for the whole batch instead of one
    // ElementIDRangeOfRow per row (on growing segments the per-row call is a
    // shared_lock acquire/release each, contending with Insert's unique
    // lock).
    FixedVector<int32_t> row_elem_starts(row_count + 1);
    array_offsets->CopyRowElementStarts(
        row_start, row_count, row_elem_starts.data());
    for (int64_t i = 0; i < row_count; ++i) {
        int64_t bitset_start = row_elem_starts[i] - elem_start;
        int64_t row_elem_count = row_elem_starts[i + 1] - row_elem_starts[i];

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
    // ONE batched virtual call for all requested rows instead of one
    // ElementIDRangeOfRow per row (single shared_lock on growing).
    const auto row_count = static_cast<int64_t>(row_offsets->size());
    FixedVector<std::pair<int32_t, int32_t>> ranges(row_count);
    array_offsets->CopyRowElementRanges(
        row_offsets->data(), row_count, ranges.data());

    int64_t elem_cursor = 0;
    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_elem_count = ranges[i].second - ranges[i].first;

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

    auto schema = segment_->get_schema();
    auto field_meta =
        schema.GetFirstArrayFieldInStruct(expr_->get_struct_name());

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
        MaskNullRows(col_vec.get(), field_meta.get_id(), input, batch_rows);
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

    MaskNullRows(col_vec.get(), field_meta.get_id(), input, batch_rows);
    if (!has_offset_input_) {
        current_pos_ += batch_rows;
    }
}

void
PhyMatchFilterExpr::MaskNullRows(ColumnVector* col_vec,
                                 FieldId field_id,
                                 const OffsetVector* input,
                                 int64_t batch_rows) {
    // Build the physical row-offset list for this batch (offset-input vs
    // sequential scan), then ask the segment to clear the valid bits of NULL
    // field rows. ApplyFieldValidDataByOffsets only CLEARS invalid rows and is
    // a no-op for a non-nullable field, so this is safe for all field kinds and
    // for both sealed and growing segments.
    std::vector<int64_t> row_offsets(batch_rows);
    for (int64_t i = 0; i < batch_rows; ++i) {
        row_offsets[i] = has_offset_input_ ? static_cast<int64_t>((*input)[i])
                                           : (current_pos_ + i);
    }
    TargetBitmapView bitset_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    segment_->ApplyFieldValidDataByOffsets(
        op_ctx_, field_id, row_offsets.data(), batch_rows, valid_view);
    for (int64_t i = 0; i < batch_rows; ++i) {
        if (!valid_view[i]) {
            bitset_view[i] = false;
        }
    }
}

}  // namespace exec
}  // namespace milvus
