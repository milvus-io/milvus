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
#include "common/StructElementOffsets.h"
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
// chunks (bitmap word width) instead of bit by bit. An UNKNOWN element (valid
// bit 0) counts as a miss: it adds no hit, and MatchAll requires every element
// to be a hit (vacuously true for an empty row).
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
        if constexpr (!all_valid) {
            m &= valid_bitset.read(pos, n);
        }
        if constexpr (match_type == MatchType::MatchAll) {
            const word_t full = (n == static_cast<size_t>(kWordBits))
                                    ? ~word_t(0)
                                    : ((word_t(1) << n) - 1);
            if (m != full) {
                return false;
            }
            continue;
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
        // Every element matched; empty array returns true
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
            ThrowInfo(UnexpectedError,
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
                      const IStructElementOffsets* struct_element_offsets,
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
        struct_element_offsets->ElementBitsetToRowBitsetAny(
            match_bitset, elem_start, row_start, result_bitset);
        return;
    }
    // ONE batched virtual call for the whole batch instead of one
    // ElementIDRangeOfRow per row. Growing segments acquire-load one published
    // watermark and read the whole batch from that lock-free snapshot.
    FixedVector<int32_t> row_elem_starts(row_count + 1);
    struct_element_offsets->CopyRowElementStarts(
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
                  const OffsetVector* output_positions,
                  const IStructElementOffsets* struct_element_offsets,
                  const TargetBitmapView& match_bitset,
                  const TargetBitmapView& valid_bitset,
                  TargetBitmapView& result_bitset,
                  int64_t threshold) {
    // ONE batched virtual call for all requested rows instead of one
    // ElementIDRangeOfRow per row (one lock-free snapshot on growing).
    const auto row_count = static_cast<int64_t>(row_offsets->size());
    FixedVector<std::pair<int32_t, int32_t>> ranges(row_count);
    struct_element_offsets->CopyRowElementRanges(
        row_offsets->data(), row_count, ranges.data());

    int64_t elem_cursor = 0;
    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_elem_count = ranges[i].second - ranges[i].first;

        if (MatchSingleRow<match_type, all_valid>(elem_cursor,
                                                  row_elem_count,
                                                  match_bitset,
                                                  valid_bitset,
                                                  threshold)) {
            result_bitset[output_positions ? (*output_positions)[i] : i] = true;
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
                        const OffsetVector* output_positions,
                        const IStructElementOffsets* struct_element_offsets,
                        const TargetBitmapView& match_bitset,
                        const TargetBitmapView& valid_bitset,
                        TargetBitmapView& result_bitset,
                        int64_t threshold) {
    if (use_offset_input) {
        ProcessOffsetRows<match_type, all_valid>(row_offsets,
                                                 output_positions,
                                                 struct_element_offsets,
                                                 match_bitset,
                                                 valid_bitset,
                                                 result_bitset,
                                                 threshold);
    } else {
        ProcessContiguousRows<match_type, all_valid>(row_count,
                                                     row_start,
                                                     elem_start,
                                                     struct_element_offsets,
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

    auto schema = segment_chunk_reader_.GetSchemaSnapshot();
    const auto& field_meta =
        schema->GetFirstArrayFieldInStruct(expr_->get_struct_name());

    auto struct_element_offsets =
        segment_chunk_reader_.GetStructElementOffsets(field_meta.get_id());
    AssertInfo(struct_element_offsets != nullptr,
               "Struct element offsets not available");

    const int64_t batch_rows =
        has_offset_input_ ? input->size()
                          : std::min(batch_size_, active_count_ - current_pos_);
    if (batch_rows <= 0) {
        result = nullptr;
        return;
    }

    const auto& candidates = context.get_bitmap_input();
    const bool has_candidates = !candidates.empty() && !candidates.all();
    bool sparse_batch = false;
    FixedVector<int32_t> starts;
    if (has_candidates && !has_offset_input_) {
        starts.resize(batch_rows + 1);
        struct_element_offsets->CopyRowElementStarts(
            current_pos_, batch_rows, starts.data());
        int64_t selected_elements = 0;
        const int64_t total_elements = starts.back() - starts.front();
        for (int64_t i = 0; i < batch_rows; ++i) {
            if (candidates[i]) {
                selected_elements += starts[i + 1] - starts[i];
            }
        }
        // Offsets pay for expansion and random row access. Keep dense batches
        // sequential, and never replace an index query with raw-data gathers.
        // This conservative 5% cutoff is based on elements, not parent rows.
        if (total_elements > 0 && selected_elements <= total_elements / 20) {
            inputs_[0]->WaitPrefetch();
            sparse_batch = inputs_[0]->CanUseOffsetsForSequentialBatch();
        }
    }
    const bool use_offsets = has_offset_input_ || sparse_batch;
    const bool select_rows = has_candidates && use_offsets;
    OffsetVector selected_rows;
    OffsetVector output_positions;
    const OffsetVector* row_offsets = input;
    if (select_rows || (use_offsets && input == nullptr)) {
        selected_rows.reserve(batch_rows);
        if (select_rows) {
            output_positions.reserve(batch_rows);
        }
        for (int64_t i = 0; i < batch_rows; ++i) {
            // In offset mode the bitmap is indexed by input position, not
            // by the segment row ID. Retain duplicates and input order.
            if (select_rows && !candidates[i]) {
                continue;
            }
            selected_rows.push_back(input ? (*input)[i] : current_pos_ + i);
            if (select_rows) {
                output_positions.push_back(i);
            }
        }
        row_offsets = &selected_rows;
    }

    int64_t elem_start;
    int64_t elem_count;
    FixedVector<int32_t> element_offsets_storage;
    EvalCtx eval_ctx(context.get_exec_context());

    if (use_offsets) {
        // Expand only candidate rows, then reduce/scatter in the same order.
        element_offsets_storage =
            struct_element_offsets->RowOffsetsToElementOffsets(*row_offsets);
        eval_ctx.set_offset_input(&element_offsets_storage);
        elem_start = 0;
        elem_count = element_offsets_storage.size();
    } else {
        // Sequential batch mode
        auto start_range =
            struct_element_offsets->ElementIDRangeOfRow(current_pos_);
        auto end_range = struct_element_offsets->ElementIDRangeOfRow(
            current_pos_ + batch_rows);
        elem_start = start_range.first;
        elem_count = end_range.first - elem_start;
        if (has_candidates) {
            // Keep index/dense execution sequential and expose candidates in
            // the same element coordinates as this batch's result.
            TargetBitmap element_candidates(elem_count, false);
            for (int64_t i = 0; i < batch_rows; ++i) {
                if (candidates[i] && starts[i] < starts[i + 1]) {
                    element_candidates.set(starts[i] - elem_start,
                                           starts[i + 1] - starts[i],
                                           true);
                }
            }
            eval_ctx.set_bitmap_input(std::move(element_candidates));
        }
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
    } else if (!use_offsets) {
        // Keep element-level child expressions aligned across all-empty batches.
        inputs_[0]->MoveCursor();
    }
    if (sparse_batch) {
        // Offset Eval and offset short-circuiting leave sequential state alone.
        // Advance every leaf once, including leaves skipped by AND/OR, so the
        // next batch can use either sequential or offset execution safely.
        inputs_[0]->AdvanceSequentialBatch();
    }
    if (match_result == nullptr) {
        AssertInfo(elem_count == 0,
                   "Match child returned empty result for non-empty element "
                   "batch, elem_count={}",
                   elem_count);
        if (MatchEmptyElements(match_type, threshold)) {
            if (select_rows) {
                for (const auto position : output_positions) {
                    bitset_view[position] = true;
                }
            } else {
                bitset_view.set();
            }
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
    AssertInfo(match_result_col_vec->size() == elem_count,
               "Match predicate result size {} should equal element count {}",
               match_result_col_vec->size(),
               elem_count);
    TargetBitmapView match_result_bitset_view(
        match_result_col_vec->GetRawData(), match_result_col_vec->size());
    TargetBitmapView match_result_valid_view(
        match_result_col_vec->GetValidRawData(), match_result_col_vec->size());

    bool all_valid = match_result_valid_view.all();

    auto dispatch = [&]<bool all_valid_v>() {
        switch (match_type) {
            case MatchType::MatchAny:
                DispatchMatchProcessing<MatchType::MatchAny, all_valid_v>(
                    use_offsets,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    row_offsets,
                    select_rows ? &output_positions : nullptr,
                    struct_element_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchAll:
                DispatchMatchProcessing<MatchType::MatchAll, all_valid_v>(
                    use_offsets,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    row_offsets,
                    select_rows ? &output_positions : nullptr,
                    struct_element_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchLeast:
                DispatchMatchProcessing<MatchType::MatchLeast, all_valid_v>(
                    use_offsets,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    row_offsets,
                    select_rows ? &output_positions : nullptr,
                    struct_element_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchMost:
                DispatchMatchProcessing<MatchType::MatchMost, all_valid_v>(
                    use_offsets,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    row_offsets,
                    select_rows ? &output_positions : nullptr,
                    struct_element_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            case MatchType::MatchExact:
                DispatchMatchProcessing<MatchType::MatchExact, all_valid_v>(
                    use_offsets,
                    batch_rows,
                    current_pos_,
                    elem_start,
                    row_offsets,
                    select_rows ? &output_positions : nullptr,
                    struct_element_offsets.get(),
                    match_result_bitset_view,
                    match_result_valid_view,
                    bitset_view,
                    threshold);
                break;
            default:
                ThrowInfo(UnexpectedError,
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
    }
    auto offsets = segment_chunk_reader_.GetStructElementOffsets(field_id);
    if (offsets && offsets->HasRowValidity()) {
        for (int64_t i = 0; i < batch_rows; ++i) {
            const auto row_id = input ? (*input)[i] : current_pos_ + i;
            if (!offsets->IsRowValid(row_id)) {
                value_view[i] = false;
                valid_view[i] = false;
            }
        }
        return;
    }
    if (input != nullptr) {
        std::vector<int64_t> row_offsets(batch_rows);
        for (int64_t i = 0; i < batch_rows; ++i) {
            row_offsets[i] = static_cast<int64_t>((*input)[i]);
        }
        segment_chunk_reader_.ApplyFieldValidDataByOffsets(
            op_ctx_, field_id, row_offsets.data(), batch_rows, valid_view);
    } else {
        int64_t processed = 0;
        int64_t row_offset = current_pos_;
        while (processed < batch_rows) {
            auto [chunk_id, offset_in_chunk] =
                segment_chunk_reader_.GetChunkByOffset(field_id, row_offset);
            auto count =
                std::min(batch_rows - processed,
                         segment_chunk_reader_.ChunkSize(field_id, chunk_id) -
                             offset_in_chunk);
            AssertInfo(count > 0,
                       "invalid field validity range at row offset {} for "
                       "field {}",
                       row_offset,
                       field_id.get());
            segment_chunk_reader_.ApplyFieldValidData(op_ctx_,
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
