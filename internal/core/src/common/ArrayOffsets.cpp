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

#include "ArrayOffsets.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <type_traits>

#include "bitset/bitset.h"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/OpContext.h"
#include "common/VectorArray.h"
#include "glog/logging.h"
#include "log/Log.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/SegmentInterface.h"

namespace milvus {

namespace {

// Word-wise ANY-semantics reduction shared by the sealed and growing
// implementations of ElementBitsetToRowBitsetAny.
//
// `starts` is the row -> element-start table, indexable over
// [row_start, row_start + row_count]. Bit j of `elem_bitset` corresponds to
// global element id (elem_offset + j).
//
// Linear merge of the element bitmap (consumed one 64-bit word at a time)
// with the sorted row-start table: zero words are skipped with a single
// compare, a set bit advances the monotone row cursor (amortized
// O(row_count) over the whole call, no binary search), and once a row is
// marked the scan jumps directly to the row's end, skipping its remaining
// words. Complexity is O(total_elements / 64 + row_count + hit_rows)
// instead of the per-bit O(total_elements).
void
ElementBitsetAnyReduce(const int32_t* starts,
                       const TargetBitmapView& elem_bitset,
                       int64_t elem_offset,
                       int64_t row_start,
                       int64_t row_count,
                       TargetBitmapView row_result) {
    using word_t = TargetBitmapView::policy_type::data_type;
    constexpr int64_t kWordBits = static_cast<int64_t>(8 * sizeof(word_t));

    if (row_count == 0) {
        return;
    }
    const int64_t first_bit = starts[row_start] - elem_offset;
    const int64_t last_bit = starts[row_start + row_count] - elem_offset;
    AssertInfo(
        first_bit >= 0 && last_bit <= static_cast<int64_t>(elem_bitset.size()),
        "element bitset does not cover rows [{}, {}): bits [{}, {}), "
        "bitset size {}",
        row_start,
        row_start + row_count,
        first_bit,
        last_bit,
        elem_bitset.size());

    int64_t pos = first_bit;
    int64_t row = row_start;
    while (pos < last_bit) {
        const int64_t n = std::min(kWordBits, last_bit - pos);
        word_t word =
            elem_bitset.read(static_cast<size_t>(pos), static_cast<size_t>(n));
        if (word == 0) {
            pos += n;
            continue;
        }
        int64_t next_pos = pos + n;
        do {
            const int64_t bit = pos + __builtin_ctzll(word);
            const int32_t elem_id = static_cast<int32_t>(bit + elem_offset);
            // Monotone row cursor; empty rows are skipped by the same loop.
            while (starts[row + 1] <= elem_id) {
                ++row;
            }
            row_result[row - row_start] = true;
            // Skip the rest of this row's elements.
            const int64_t row_end = starts[row + 1] - elem_offset;
            if (row_end >= pos + n) {
                next_pos = row_end;
                break;
            }
            // row_end falls inside the current word: clear bits below it.
            // (0 < row_end - pos < kWordBits, so the shift is well-defined.)
            word &= ~word_t(0) << (row_end - pos);
        } while (word != 0);
        pos = next_pos;
    }
}

}  // namespace

std::pair<int32_t, int32_t>
ArrayOffsetsSealed::ElementIDToRowID(int32_t elem_id) const {
    assert(elem_id >= 0 && elem_id < GetTotalElementCount());

    // Binary search: find the row where elem_id belongs
    // row_to_element_start_[row_id] <= elem_id < row_to_element_start_[row_id + 1]
    auto it = std::upper_bound(
        row_to_element_start_.begin(), row_to_element_start_.end(), elem_id);
    int32_t row_id = static_cast<int32_t>(
        std::distance(row_to_element_start_.begin(), it) - 1);

    int32_t elem_idx = elem_id - row_to_element_start_[row_id];
    return {row_id, elem_idx};
}

std::pair<int32_t, int32_t>
ArrayOffsetsSealed::ElementIDRangeOfRow(int32_t row_id) const {
    int32_t row_count = GetRowCount();
    assert(row_id >= 0 && row_id <= row_count);

    if (row_id == row_count) {
        auto total = row_to_element_start_[row_count];
        return {total, total};
    }
    return {row_to_element_start_[row_id], row_to_element_start_[row_id + 1]};
}

void
ArrayOffsetsSealed::CopyRowElementRanges(
    const int32_t* row_ids,
    int64_t count,
    std::pair<int32_t, int32_t>* out) const {
    const auto row_count = static_cast<int32_t>(GetRowCount());
    const int32_t* starts = row_to_element_start_.data();
    for (int64_t i = 0; i < count; ++i) {
        const int32_t row_id = row_ids[i];
        assert(row_id >= 0 && row_id <= row_count);
        if (row_id == row_count) {
            out[i] = {starts[row_count], starts[row_count]};
        } else {
            out[i] = {starts[row_id], starts[row_id + 1]};
        }
    }
}

void
ArrayOffsetsSealed::CopyRowElementStarts(int64_t row_start,
                                         int64_t row_count,
                                         int32_t* out) const {
    AssertInfo(row_start >= 0 && row_count >= 0 &&
                   row_start + row_count <= GetRowCount(),
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               GetRowCount());
    std::memcpy(out,
                row_to_element_start_.data() + row_start,
                sizeof(int32_t) * (row_count + 1));
}

std::pair<TargetBitmap, TargetBitmap>
ArrayOffsetsSealed::RowBitsetToElementBitset(
    const TargetBitmapView& row_bitset,
    const TargetBitmapView& valid_row_bitset,
    int64_t row_start) const {
    int64_t row_count = row_bitset.size();
    AssertInfo(row_start >= 0 && row_start + row_count <= GetRowCount(),
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               GetRowCount());

    int64_t element_start = row_to_element_start_[row_start];
    int64_t element_end = row_to_element_start_[row_start + row_count];
    int64_t element_count = element_end - element_start;

    TargetBitmap element_bitset(element_count);
    TargetBitmap valid_element_bitset(element_count);

    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_id = row_start + i;
        int64_t start = row_to_element_start_[row_id] - element_start;
        int64_t end = row_to_element_start_[row_id + 1] - element_start;
        if (start < end) {
            element_bitset.set(start, end - start, row_bitset[i]);
            valid_element_bitset.set(start, end - start, valid_row_bitset[i]);
        }
    }

    return {std::move(element_bitset), std::move(valid_element_bitset)};
}

FixedVector<int32_t>
ArrayOffsetsSealed::RowBitsetToElementOffsets(
    const TargetBitmapView& row_bitset, int64_t row_start) const {
    int64_t row_count = row_bitset.size();
    int64_t total_rows = GetRowCount();
    AssertInfo(row_start >= 0 && row_start + row_count <= total_rows,
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               total_rows);

    int64_t selected_rows = row_bitset.count();
    FixedVector<int32_t> element_offsets;
    if (selected_rows == 0) {
        return element_offsets;
    }

    int64_t avg_elem_per_row = GetTotalElementCount() / total_rows;

    element_offsets.reserve(selected_rows * avg_elem_per_row);

    for (int64_t i = 0; i < row_count; ++i) {
        if (row_bitset[i]) {
            int64_t row_id = row_start + i;
            int32_t first_elem = row_to_element_start_[row_id];
            int32_t last_elem = row_to_element_start_[row_id + 1];
            for (int32_t elem_id = first_elem; elem_id < last_elem; ++elem_id) {
                element_offsets.push_back(elem_id);
            }
        }
    }

    return element_offsets;
}

FixedVector<int32_t>
ArrayOffsetsSealed::RowOffsetsToElementOffsets(
    const FixedVector<int32_t>& row_offsets) const {
    FixedVector<int32_t> element_offsets;
    if (row_offsets.empty()) {
        return element_offsets;
    }

    int32_t row_count = GetRowCount();
    int64_t avg_elem_per_row = GetTotalElementCount() / row_count;

    element_offsets.reserve(row_offsets.size() * avg_elem_per_row);
    for (auto row_id : row_offsets) {
        assert(row_id >= 0 && row_id < row_count);
        int32_t first_elem = row_to_element_start_[row_id];
        int32_t last_elem = row_to_element_start_[row_id + 1];
        for (int32_t elem_id = first_elem; elem_id < last_elem; ++elem_id) {
            element_offsets.push_back(elem_id);
        }
    }

    return element_offsets;
}

TargetBitmap
ArrayOffsetsSealed::ForEachRowElementRange(
    const ElementRangePredicate& predicate,
    int64_t row_start,
    int64_t row_count) const {
    AssertInfo(row_start >= 0 && row_start + row_count <= GetRowCount(),
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               GetRowCount());

    TargetBitmap result(row_count);

    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_id = row_start + i;
        int32_t elem_start = row_to_element_start_[row_id];
        int32_t elem_end = row_to_element_start_[row_id + 1];
        result[i] = predicate(elem_start, elem_end);
    }

    return result;
}

void
ArrayOffsetsSealed::ElementBitsetToRowBitsetAny(
    const TargetBitmapView& elem_bitset,
    int64_t elem_offset,
    int64_t row_start,
    TargetBitmapView row_result) const {
    const int64_t row_count = row_result.size();
    AssertInfo(row_start >= 0 && row_start + row_count <= GetRowCount(),
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               GetRowCount());

    ElementBitsetAnyReduce(row_to_element_start_.data(),
                           elem_bitset,
                           elem_offset,
                           row_start,
                           row_count,
                           row_result);
}

std::shared_ptr<ArrayOffsetsSealed>
ArrayOffsetsSealed::BuildFromSegment(const void* segment,
                                     const FieldMeta& field_meta) {
    auto seg = static_cast<const segcore::SegmentInternalInterface*>(segment);

    int64_t row_count = seg->get_row_count();
    if (row_count == 0) {
        LOG_INFO(
            "ArrayOffsetsSealed::BuildFromSegment: empty segment for struct "
            "'{}'",
            field_meta.get_name().get());
        return ArrayOffsetsSealed::BuildAllZeros(0);
    }

    FieldId field_id = field_meta.get_id();
    auto data_type = field_meta.get_data_type();

    std::vector<int32_t> row_to_element_start(row_count + 1);

    auto temp_op_ctx = std::make_unique<OpContext>();
    auto op_ctx_ptr = temp_op_ctx.get();

    int64_t num_chunks = seg->num_chunk(field_id);
    int32_t current_row_id = 0;
    int32_t total_elements = 0;

    if (data_type == DataType::VECTOR_ARRAY) {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper = seg->chunk_view<VectorArrayView>(
                op_ctx_ptr, field_id, chunk_id);
            const auto& [vector_array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < vector_array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = vector_array_views[i].length();
                }

                row_to_element_start[current_row_id] = total_elements;
                total_elements += array_len;
                current_row_id++;
            }
        }
    } else {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper =
                seg->chunk_view<ArrayView>(op_ctx_ptr, field_id, chunk_id);
            const auto& [array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = array_views[i].length();
                }

                row_to_element_start[current_row_id] = total_elements;
                total_elements += array_len;
                current_row_id++;
            }
        }
    }

    row_to_element_start[row_count] = total_elements;

    AssertInfo(current_row_id == row_count,
               "Row count mismatch: expected {}, got {}",
               row_count,
               current_row_id);

    LOG_INFO(
        "ArrayOffsetsSealed::BuildFromSegment: struct_name='{}', "
        "field_id={}, row_count={}, total_elements={}",
        field_meta.get_name().get(),
        field_meta.get_id().get(),
        row_count,
        total_elements);

    auto result =
        std::make_shared<ArrayOffsetsSealed>(std::move(row_to_element_start));
    result->resource_size_ = 4 * (row_count + 1);
    cachinglayer::Manager::GetInstance().ChargeLoadedResource(
        cachinglayer::ResourceUsage{result->resource_size_, 0});
    return result;
}

std::shared_ptr<ArrayOffsetsSealed>
ArrayOffsetsSealed::BuildFromColumn(const ChunkedColumnInterface& column,
                                    const FieldMeta& field_meta,
                                    int64_t row_count) {
    if (row_count == 0) {
        LOG_INFO(
            "ArrayOffsetsSealed::BuildFromColumn: empty segment for struct "
            "'{}'",
            field_meta.get_name().get());
        return std::make_shared<ArrayOffsetsSealed>(std::vector<int32_t>{0});
    }

    auto data_type = field_meta.get_data_type();

    std::vector<int32_t> row_to_element_start(row_count + 1);

    auto temp_op_ctx = std::make_unique<OpContext>();
    auto op_ctx_ptr = temp_op_ctx.get();

    int64_t num_chunks = column.num_chunks();
    int32_t current_row_id = 0;
    int32_t total_elements = 0;

    if (data_type == DataType::VECTOR_ARRAY) {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper =
                column.VectorArrayViews(op_ctx_ptr, chunk_id, std::nullopt);
            const auto& [vector_array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < vector_array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = vector_array_views[i].length();
                }

                row_to_element_start[current_row_id] = total_elements;
                total_elements += array_len;
                current_row_id++;
            }
        }
    } else {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper =
                column.ArrayViews(op_ctx_ptr, chunk_id, std::nullopt);
            const auto& [array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = array_views[i].length();
                }

                row_to_element_start[current_row_id] = total_elements;
                total_elements += array_len;
                current_row_id++;
            }
        }
    }

    row_to_element_start[row_count] = total_elements;

    AssertInfo(current_row_id == row_count,
               "Row count mismatch: expected {}, got {}",
               row_count,
               current_row_id);

    LOG_INFO(
        "ArrayOffsetsSealed::BuildFromColumn: struct_name='{}', "
        "field_id={}, row_count={}, total_elements={}",
        field_meta.get_name().get(),
        field_meta.get_id().get(),
        row_count,
        total_elements);

    auto result =
        std::make_shared<ArrayOffsetsSealed>(std::move(row_to_element_start));
    result->resource_size_ = 4 * (row_count + 1);
    cachinglayer::Manager::GetInstance().ChargeLoadedResource(
        cachinglayer::ResourceUsage{result->resource_size_, 0});
    return result;
}

std::pair<int32_t, int32_t>
ArrayOffsetsGrowing::ElementIDToRowID(int32_t elem_id) const {
    std::shared_lock lock(mutex_);
    int64_t total_elements =
        row_to_element_start_.empty() ? 0 : row_to_element_start_.back();
    assert(elem_id >= 0 && elem_id < total_elements);

    // Binary search: find the row where elem_id belongs
    auto it = std::upper_bound(
        row_to_element_start_.begin(), row_to_element_start_.end(), elem_id);
    int32_t row_id = static_cast<int32_t>(
        std::distance(row_to_element_start_.begin(), it) - 1);

    int32_t elem_idx = elem_id - row_to_element_start_[row_id];
    return {row_id, elem_idx};
}

std::pair<int32_t, int32_t>
ArrayOffsetsGrowing::ElementIDRangeOfRow(int32_t row_id) const {
    std::shared_lock lock(mutex_);
    assert(row_id >= 0 && row_id <= committed_row_count_);

    if (row_id == committed_row_count_) {
        auto total = row_to_element_start_[committed_row_count_];
        return {total, total};
    }
    return {row_to_element_start_[row_id], row_to_element_start_[row_id + 1]};
}

void
ArrayOffsetsGrowing::CopyRowElementRanges(
    const int32_t* row_ids,
    int64_t count,
    std::pair<int32_t, int32_t>* out) const {
    // ONE shared lock for the whole batch (the per-row method locks per
    // call, which contends with Insert's unique lock on hot scan paths).
    std::shared_lock lock(mutex_);
    const int32_t* starts = row_to_element_start_.data();
    for (int64_t i = 0; i < count; ++i) {
        const int32_t row_id = row_ids[i];
        // Out-of-range insurance: with zero committed rows the vector is
        // empty, and a not-yet-committed row has no range yet.
        if (row_id < 0 || row_id > committed_row_count_ ||
            row_to_element_start_.empty()) {
            out[i] = {0, 0};
            continue;
        }
        if (row_id == committed_row_count_) {
            auto total = starts[committed_row_count_];
            out[i] = {total, total};
        } else {
            out[i] = {starts[row_id], starts[row_id + 1]};
        }
    }
}

void
ArrayOffsetsGrowing::CopyRowElementStarts(int64_t row_start,
                                          int64_t row_count,
                                          int32_t* out) const {
    AssertInfo(row_start >= 0 && row_count >= 0,
               "invalid row range: row_start={}, row_count={}",
               row_start,
               row_count);
    std::shared_lock lock(mutex_);
    if (row_to_element_start_.empty()) {
        // No committed rows yet: every requested row clamps to total == 0.
        std::fill(out, out + row_count + 1, 0);
        return;
    }
    const int32_t* starts = row_to_element_start_.data();
    const int64_t committed = committed_row_count_;
    if (row_start + row_count <= committed) {
        // Fast path: fully committed range, straight copy under the lock.
        std::memcpy(
            out, starts + row_start, sizeof(int32_t) * (row_count + 1));
        return;
    }
    // Rows at or beyond the committed count clamp to the committed total
    // ({total, total}-style safety: not-yet-committed rows read as empty).
    const int32_t total = starts[committed];
    for (int64_t i = 0; i <= row_count; ++i) {
        const int64_t row = row_start + i;
        out[i] = row <= committed ? starts[row] : total;
    }
}

std::pair<TargetBitmap, TargetBitmap>
ArrayOffsetsGrowing::RowBitsetToElementBitset(
    const TargetBitmapView& row_bitset,
    const TargetBitmapView& valid_row_bitset,
    int64_t row_start) const {
    std::shared_lock lock(mutex_);

    int64_t row_count = row_bitset.size();
    AssertInfo(row_start >= 0 && row_start + row_count <= committed_row_count_,
               "row range out of bounds: row_start={}, row_count={}, "
               "committed_rows={}",
               row_start,
               row_count,
               committed_row_count_);

    int64_t element_start = row_to_element_start_[row_start];
    int64_t element_end = row_to_element_start_[row_start + row_count];
    int64_t element_count = element_end - element_start;

    TargetBitmap element_bitset(element_count);
    TargetBitmap valid_element_bitset(element_count);

    // Use row-based iteration (more efficient than element-based)
    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_id = row_start + i;
        int64_t start = row_to_element_start_[row_id] - element_start;
        int64_t end = row_to_element_start_[row_id + 1] - element_start;
        if (start < end) {
            element_bitset.set(start, end - start, row_bitset[i]);
            valid_element_bitset.set(start, end - start, valid_row_bitset[i]);
        }
    }

    return {std::move(element_bitset), std::move(valid_element_bitset)};
}

FixedVector<int32_t>
ArrayOffsetsGrowing::RowBitsetToElementOffsets(
    const TargetBitmapView& row_bitset, int64_t row_start) const {
    std::shared_lock lock(mutex_);

    int64_t row_count = row_bitset.size();
    AssertInfo(row_start >= 0 && row_start + row_count <= committed_row_count_,
               "row range out of bounds: row_start={}, row_count={}, "
               "committed_rows={}",
               row_start,
               row_count,
               committed_row_count_);

    int64_t selected_rows = row_bitset.count();
    FixedVector<int32_t> element_offsets;
    if (selected_rows == 0) {
        return element_offsets;
    }

    int64_t total_elements = row_to_element_start_.back();
    int64_t avg_elem_per_row = total_elements / committed_row_count_;
    element_offsets.reserve(selected_rows * avg_elem_per_row);

    for (int64_t i = 0; i < row_count; ++i) {
        if (row_bitset[i]) {
            int64_t row_id = row_start + i;
            int32_t first_elem = row_to_element_start_[row_id];
            int32_t last_elem = row_to_element_start_[row_id + 1];
            for (int32_t elem_id = first_elem; elem_id < last_elem; ++elem_id) {
                element_offsets.push_back(elem_id);
            }
        }
    }

    return element_offsets;
}

FixedVector<int32_t>
ArrayOffsetsGrowing::RowOffsetsToElementOffsets(
    const FixedVector<int32_t>& row_offsets) const {
    std::shared_lock lock(mutex_);

    FixedVector<int32_t> element_offsets;
    if (row_offsets.empty()) {
        return element_offsets;
    }

    int64_t total_elements = row_to_element_start_.back();
    int64_t avg_elem_per_row = total_elements / committed_row_count_;
    element_offsets.reserve(row_offsets.size() * avg_elem_per_row);

    for (auto row_id : row_offsets) {
        assert(row_id >= 0 && row_id < committed_row_count_);
        int32_t first_elem = row_to_element_start_[row_id];
        int32_t last_elem = row_to_element_start_[row_id + 1];
        for (int32_t elem_id = first_elem; elem_id < last_elem; ++elem_id) {
            element_offsets.push_back(elem_id);
        }
    }

    return element_offsets;
}

TargetBitmap
ArrayOffsetsGrowing::ForEachRowElementRange(
    const ElementRangePredicate& predicate,
    int64_t row_start,
    int64_t row_count) const {
    std::shared_lock lock(mutex_);

    AssertInfo(row_start >= 0 && row_start + row_count <= committed_row_count_,
               "row range out of bounds: row_start={}, row_count={}, "
               "committed_rows={}",
               row_start,
               row_count,
               committed_row_count_);

    TargetBitmap result(row_count);

    for (int64_t i = 0; i < row_count; ++i) {
        int64_t row_id = row_start + i;
        int32_t elem_start = row_to_element_start_[row_id];
        int32_t elem_end = row_to_element_start_[row_id + 1];
        result[i] = predicate(elem_start, elem_end);
    }

    return result;
}

void
ArrayOffsetsGrowing::ElementBitsetToRowBitsetAny(
    const TargetBitmapView& elem_bitset,
    int64_t elem_offset,
    int64_t row_start,
    TargetBitmapView row_result) const {
    std::shared_lock lock(mutex_);

    const int64_t row_count = row_result.size();
    AssertInfo(row_start >= 0 && row_start + row_count <= committed_row_count_,
               "row range out of bounds: row_start={}, row_count={}, "
               "committed_rows={}",
               row_start,
               row_count,
               committed_row_count_);

    ElementBitsetAnyReduce(row_to_element_start_.data(),
                           elem_bitset,
                           elem_offset,
                           row_start,
                           row_count,
                           row_result);
}

void
ArrayOffsetsGrowing::Insert(int64_t row_id_start,
                            const int32_t* array_lengths,
                            int64_t count) {
    std::unique_lock lock(mutex_);

    // NOTE: no reserve() here on purpose. An exact-size reserve per insert
    // batch (row_id_start + count + 1) defeats the vector's geometric growth:
    // every batch whose target exceeded the previous exact capacity forced a
    // full reallocation + memcpy of the whole table while holding the writer
    // lock, turning N batched inserts into O(N^2) copied bytes. push_back's
    // amortized doubling is the right behavior.
    for (int64_t i = 0; i < count; ++i) {
        int32_t row_id = row_id_start + i;
        int32_t array_len = array_lengths[i];

        if (row_id == committed_row_count_) {
            // Get current total element count (from sentinel or compute)
            int32_t current_total = row_to_element_start_.empty()
                                        ? 0
                                        : row_to_element_start_.back();

            // Record the start position for this row
            if (row_to_element_start_.size() >
                static_cast<size_t>(committed_row_count_)) {
                // Sentinel exists, overwrite it with row start
                row_to_element_start_[committed_row_count_] = current_total;
            } else {
                row_to_element_start_.push_back(current_total);
            }

            // Update sentinel (new total after this row)
            int32_t new_total = current_total + array_len;
            if (row_to_element_start_.size() >
                static_cast<size_t>(committed_row_count_ + 1)) {
                row_to_element_start_[committed_row_count_ + 1] = new_total;
            } else {
                row_to_element_start_.push_back(new_total);
            }

            committed_row_count_++;
        } else {
            pending_rows_[row_id] = {row_id, array_len};
        }
    }

    DrainPendingRows();
}

void
ArrayOffsetsGrowing::DrainPendingRows() {
    while (true) {
        auto it = pending_rows_.find(committed_row_count_);
        if (it == pending_rows_.end()) {
            break;
        }

        const auto& pending = it->second;

        // Get current total element count
        int32_t current_total =
            (committed_row_count_ > 0)
                ? row_to_element_start_[committed_row_count_]
                : 0;

        // If sentinel exists at current position, overwrite it; otherwise push_back
        if (row_to_element_start_.size() >
            static_cast<size_t>(committed_row_count_)) {
            row_to_element_start_[committed_row_count_] = current_total;
        } else {
            row_to_element_start_.push_back(current_total);
        }

        // Update sentinel for next row
        int32_t new_total = current_total + pending.array_len;
        if (row_to_element_start_.size() >
            static_cast<size_t>(committed_row_count_ + 1)) {
            row_to_element_start_[committed_row_count_ + 1] = new_total;
        } else {
            row_to_element_start_.push_back(new_total);
        }

        committed_row_count_++;

        pending_rows_.erase(it);
    }
}

}  // namespace milvus
