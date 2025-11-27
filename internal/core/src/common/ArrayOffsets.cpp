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

namespace {
ArrayOffsetsSealed
BuildArrayOffsetsSealedFromColumn(
    const segcore::SegmentInternalInterface* segment,
    const FieldMeta& field_meta,
    int64_t row_count) {
    FieldId field_id = field_meta.get_id();
    auto data_type = field_meta.get_data_type();

    ArrayOffsetsSealed result;
    result.doc_count = row_count;
    // Size is doc_count + 1, last element stores total_element_count
    result.doc_to_element_start_.resize(row_count + 1);

    auto temp_op_ctx = std::make_unique<OpContext>();
    auto op_ctx_ptr = temp_op_ctx.get();

    int64_t num_chunks = segment->num_chunk(field_id);
    int32_t current_doc_id = 0;

    if (data_type == DataType::VECTOR_ARRAY) {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper = segment->chunk_view<VectorArrayView>(
                op_ctx_ptr, field_id, chunk_id);
            const auto& [vector_array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < vector_array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = vector_array_views[i].length();
                }

                // Record the start position for this doc
                result.doc_to_element_start_[current_doc_id] =
                    result.element_doc_ids.size();

                // Add doc_id for each element (elem_idx computed on access)
                for (int32_t j = 0; j < array_len; ++j) {
                    result.element_doc_ids.emplace_back(current_doc_id);
                }

                current_doc_id++;
            }
        }
    } else {
        for (int64_t chunk_id = 0; chunk_id < num_chunks; ++chunk_id) {
            auto pin_wrapper =
                segment->chunk_view<ArrayView>(op_ctx_ptr, field_id, chunk_id);
            const auto& [array_views, valid_flags] = pin_wrapper.get();

            for (size_t i = 0; i < array_views.size(); ++i) {
                int32_t array_len = 0;
                if (valid_flags.empty() || valid_flags[i]) {
                    array_len = array_views[i].length();
                }

                // Record the start position for this doc
                result.doc_to_element_start_[current_doc_id] =
                    result.element_doc_ids.size();

                // Add doc_id for each element (elem_idx computed on access)
                for (int32_t j = 0; j < array_len; ++j) {
                    result.element_doc_ids.emplace_back(current_doc_id);
                }

                current_doc_id++;
            }
        }
    }

    // Store total element count as the last entry
    result.doc_to_element_start_[row_count] = result.element_doc_ids.size();

    AssertInfo(current_doc_id == row_count,
               "Document count mismatch: expected {}, got {}",
               row_count,
               current_doc_id);

    return result;
}

}  // anonymous namespace

std::pair<int64_t, int64_t>
ArrayOffsetsSealed::ElementIDToDoc(int64_t elem_id) const {
    assert(elem_id >= 0 && elem_id < GetTotalElementCount());

    int32_t doc_id = element_doc_ids[elem_id];
    // Compute elem_idx: elem_idx = elem_id - start_of_this_doc
    int32_t elem_idx = elem_id - doc_to_element_start_[doc_id];
    return {doc_id, elem_idx};
}

std::pair<int64_t, int64_t>
ArrayOffsetsSealed::DocIDToElementID(int64_t doc_id) const {
    assert(doc_id >= 0 && doc_id <= doc_count);

    if (doc_id == doc_count) {
        auto total = doc_to_element_start_[doc_count];
        return {total, total};
    }
    return {doc_to_element_start_[doc_id], doc_to_element_start_[doc_id + 1]};
}

std::pair<TargetBitmap, TargetBitmap>
ArrayOffsetsSealed::DocBitsetToElementBitset(
    const TargetBitmapView& doc_bitset,
    const TargetBitmapView& valid_doc_bitset) const {
    int64_t element_count = element_doc_ids.size();
    TargetBitmap element_bitset(element_count);
    TargetBitmap valid_element_bitset(element_count);

    for (size_t elem_id = 0; elem_id < element_doc_ids.size(); ++elem_id) {
        auto doc_id = element_doc_ids[elem_id];
        element_bitset[elem_id] = doc_bitset[doc_id];
        valid_element_bitset[elem_id] = valid_doc_bitset[doc_id];
    }

    return {std::move(element_bitset), std::move(valid_element_bitset)};
}

std::pair<int64_t, int64_t>
ArrayOffsetsGrowing::ElementIDToDoc(int64_t elem_id) const {
    std::shared_lock lock(mutex_);
    assert(elem_id >= 0 &&
           elem_id < static_cast<int64_t>(element_doc_ids_.size()));
    int32_t doc_id = element_doc_ids_[elem_id];
    // Compute elem_idx: elem_idx = elem_id - start_of_this_doc
    int32_t elem_idx = elem_id - doc_to_element_start_[doc_id];
    return {doc_id, elem_idx};
}

std::pair<int64_t, int64_t>
ArrayOffsetsGrowing::DocIDToElementID(int64_t doc_id) const {
    std::shared_lock lock(mutex_);
    assert(doc_id >= 0 && doc_id <= committed_doc_count_);

    if (doc_id == committed_doc_count_) {
        auto total = doc_to_element_start_[committed_doc_count_];
        return {total, total};
    }
    return {doc_to_element_start_[doc_id], doc_to_element_start_[doc_id + 1]};
}

std::pair<TargetBitmap, TargetBitmap>
ArrayOffsetsGrowing::DocBitsetToElementBitset(
    const TargetBitmapView& doc_bitset,
    const TargetBitmapView& valid_doc_bitset) const {
    std::shared_lock lock(mutex_);

    int64_t element_count = element_doc_ids_.size();
    TargetBitmap element_bitset(element_count);
    TargetBitmap valid_element_bitset(element_count);

    // Direct access to element_doc_ids_, no virtual function calls
    for (size_t elem_id = 0; elem_id < element_doc_ids_.size(); ++elem_id) {
        auto doc_id = element_doc_ids_[elem_id];
        element_bitset[elem_id] = doc_bitset[doc_id];
        valid_element_bitset[elem_id] = valid_doc_bitset[doc_id];
    }

    return {std::move(element_bitset), std::move(valid_element_bitset)};
}

ArrayOffsetsSealed
ArrayOffsetsSealed::BuildFromSegment(const void* segment,
                                     const FieldMeta& field_meta) {
    auto seg = static_cast<const segcore::SegmentInternalInterface*>(segment);

    int64_t row_count = seg->get_row_count();
    if (row_count == 0) {
        LOG_INFO(
            "ArrayOffsetsSealed::BuildFromSegment: empty segment for struct "
            "'{}'",
            field_meta.get_name().get());
        ArrayOffsetsSealed result;
        result.doc_count = 0;
        return result;
    }

    ArrayOffsetsSealed result =
        BuildArrayOffsetsSealedFromColumn(seg, field_meta, row_count);

    int64_t total_elements = result.GetTotalElementCount();

    LOG_INFO(
        "ArrayOffsetsSealed::BuildFromSegment: struct_name='{}', "
        "field_id={}, row_count={}, total_elements={}",
        field_meta.get_name().get(),
        field_meta.get_id().get(),
        row_count,
        total_elements);

    return result;
}

void
ArrayOffsetsGrowing::Insert(int64_t doc_id_start,
                            const int32_t* array_lengths,
                            int64_t count) {
    std::unique_lock lock(mutex_);

    for (int64_t i = 0; i < count; ++i) {
        int32_t doc_id = doc_id_start + i;
        int32_t array_len = array_lengths[i];

        if (doc_id == committed_doc_count_) {
            // Record the start position for this doc
            doc_to_element_start_.push_back(element_doc_ids_.size());

            // Add doc_id for each element (elem_idx computed on access)
            for (int32_t j = 0; j < array_len; ++j) {
                element_doc_ids_.emplace_back(doc_id);
            }

            committed_doc_count_++;

            DrainPendingDocs();
        } else {
            pending_docs_[doc_id] = {doc_id, array_len};
        }
    }

    // Update the sentinel (total element count)
    if (doc_to_element_start_.size() ==
        static_cast<size_t>(committed_doc_count_)) {
        doc_to_element_start_.push_back(element_doc_ids_.size());
    } else {
        doc_to_element_start_[committed_doc_count_] = element_doc_ids_.size();
    }
}

void
ArrayOffsetsGrowing::DrainPendingDocs() {
    while (true) {
        auto it = pending_docs_.find(committed_doc_count_);
        if (it == pending_docs_.end()) {
            break;
        }

        const auto& pending = it->second;

        doc_to_element_start_.push_back(element_doc_ids_.size());

        for (int32_t j = 0; j < pending.array_len; ++j) {
            element_doc_ids_.emplace_back(static_cast<int32_t>(pending.doc_id));
        }

        committed_doc_count_++;

        pending_docs_.erase(it);
    }
}

}  // namespace milvus
