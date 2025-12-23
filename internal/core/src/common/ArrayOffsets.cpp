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
#include "segcore/SegmentInterface.h"
#include "log/Log.h"
#include "common/EasyAssert.h"

namespace milvus {

std::pair<int32_t, int32_t>
ArrayOffsetsSealed::ElementIDToRowID(int32_t elem_id) const {
    assert(elem_id >= 0 && elem_id < GetTotalElementCount());

    int32_t row_id = element_row_ids_[elem_id];
    // Compute elem_idx: elem_idx = elem_id - start_of_this_row
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
    AssertInfo(row_start >= 0 && row_start + row_count <= GetRowCount(),
               "row range out of bounds: row_start={}, row_count={}, "
               "total_rows={}",
               row_start,
               row_count,
               GetRowCount());

    int64_t selected_rows = row_bitset.count();
    FixedVector<int32_t> element_offsets;
    if (selected_rows == 0) {
        return element_offsets;
    }

    int64_t avg_elem_per_row =
        static_cast<int64_t>(element_row_ids_.size()) /
        (static_cast<int64_t>(row_to_element_start_.size()) - 1);

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
        return std::make_shared<ArrayOffsetsSealed>(std::vector<int32_t>{},
                                                    std::vector<int32_t>{0});
    }

    FieldId field_id = field_meta.get_id();
    auto data_type = field_meta.get_data_type();

    std::vector<int32_t> element_row_ids;
    // Size is row_count + 1, last element stores total_element_count
    std::vector<int32_t> row_to_element_start(row_count + 1);

    auto temp_op_ctx = std::make_unique<OpContext>();
    auto op_ctx_ptr = temp_op_ctx.get();

    int64_t num_chunks = seg->num_chunk(field_id);
    int32_t current_row_id = 0;

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

                // Record the start position for this row
                row_to_element_start[current_row_id] = element_row_ids.size();

                // Add row_id for each element (elem_idx computed on access)
                for (int32_t j = 0; j < array_len; ++j) {
                    element_row_ids.emplace_back(current_row_id);
                }

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

                // Record the start position for this row
                row_to_element_start[current_row_id] = element_row_ids.size();

                // Add row_id for each element (elem_idx computed on access)
                for (int32_t j = 0; j < array_len; ++j) {
                    element_row_ids.emplace_back(current_row_id);
                }

                current_row_id++;
            }
        }
    }

    // Store total element count as the last entry
    row_to_element_start[row_count] = element_row_ids.size();

    AssertInfo(current_row_id == row_count,
               "Row count mismatch: expected {}, got {}",
               row_count,
               current_row_id);

    int64_t total_elements = element_row_ids.size();

    LOG_INFO(
        "ArrayOffsetsSealed::BuildFromSegment: struct_name='{}', "
        "field_id={}, row_count={}, total_elements={}",
        field_meta.get_name().get(),
        field_meta.get_id().get(),
        row_count,
        total_elements);

    auto result = std::make_shared<ArrayOffsetsSealed>(
        std::move(element_row_ids), std::move(row_to_element_start));
    result->resource_size_ = 4 * (row_count + 1) + 4 * total_elements;
    cachinglayer::Manager::GetInstance().ChargeLoadedResource(
        cachinglayer::ResourceUsage{result->resource_size_, 0});
    return result;
}

std::pair<int32_t, int32_t>
ArrayOffsetsGrowing::ElementIDToRowID(int32_t elem_id) const {
    std::shared_lock lock(mutex_);
    assert(elem_id >= 0 &&
           elem_id < static_cast<int32_t>(element_row_ids_.size()));
    int32_t row_id = element_row_ids_[elem_id];
    // Compute elem_idx: elem_idx = elem_id - start_of_this_row
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

    for (int64_t elem_id = element_start; elem_id < element_end; ++elem_id) {
        auto row_id = element_row_ids_[elem_id];
        int64_t bitset_idx = row_id - row_start;
        element_bitset[elem_id - element_start] = row_bitset[bitset_idx];
        valid_element_bitset[elem_id - element_start] =
            valid_row_bitset[bitset_idx];
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
    int64_t avg_elem_per_row =
        static_cast<int64_t>(element_row_ids_.size()) /
        (static_cast<int64_t>(row_to_element_start_.size()) - 1);
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

void
ArrayOffsetsGrowing::Insert(int64_t row_id_start,
                            const int32_t* array_lengths,
                            int64_t count) {
    std::unique_lock lock(mutex_);

    row_to_element_start_.reserve(row_id_start + count + 1);

    int32_t original_committed_count = committed_row_count_;

    for (int64_t i = 0; i < count; ++i) {
        int32_t row_id = row_id_start + i;
        int32_t array_len = array_lengths[i];

        if (row_id == committed_row_count_) {
            // Record the start position for this row
            // If sentinel exists at current position, overwrite it; otherwise push_back
            if (row_to_element_start_.size() >
                static_cast<size_t>(committed_row_count_)) {
                row_to_element_start_[committed_row_count_] =
                    element_row_ids_.size();
            } else {
                row_to_element_start_.push_back(element_row_ids_.size());
            }

            // Add row_id for each element (elem_idx computed on access)
            for (int32_t j = 0; j < array_len; ++j) {
                element_row_ids_.emplace_back(row_id);
            }

            committed_row_count_++;
        } else {
            pending_rows_[row_id] = {row_id, array_len};
        }
    }

    DrainPendingRows();

    // Update the sentinel (total element count) only if we committed new rows
    if (committed_row_count_ > original_committed_count) {
        if (row_to_element_start_.size() ==
            static_cast<size_t>(committed_row_count_)) {
            row_to_element_start_.push_back(element_row_ids_.size());
        } else {
            row_to_element_start_[committed_row_count_] =
                element_row_ids_.size();
        }
    }
}

void
ArrayOffsetsGrowing::DrainPendingRows() {
    while (true) {
        auto it = pending_rows_.find(committed_row_count_);
        if (it == pending_rows_.end()) {
            break;
        }

        const auto& pending = it->second;

        // If sentinel exists at current position, overwrite it; otherwise push_back
        if (row_to_element_start_.size() >
            static_cast<size_t>(committed_row_count_)) {
            row_to_element_start_[committed_row_count_] =
                element_row_ids_.size();
        } else {
            row_to_element_start_.push_back(element_row_ids_.size());
        }

        for (int32_t j = 0; j < pending.array_len; ++j) {
            element_row_ids_.emplace_back(static_cast<int32_t>(pending.row_id));
        }

        committed_row_count_++;

        pending_rows_.erase(it);
    }
}

}  // namespace milvus
