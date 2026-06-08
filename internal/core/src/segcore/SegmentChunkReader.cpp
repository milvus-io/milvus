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
#include "segcore/SegmentChunkReader.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "cachinglayer/CacheSlot.h"
#include "common/EasyAssert.h"
#include "common/Span.h"
#include "common/type_c.h"
#include "folly/FBVector.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus::segcore {
namespace {
std::pair<const index::IndexBase*, int64_t>
GetIndexAndBaseOffset(const SegmentInternalInterface* segment,
                      FieldId field_id,
                      int chunk_id,
                      PinnedIndexView pinned_index) {
    if (pinned_index.empty()) {
        return {nullptr, 0};
    }

    if (chunk_id >= 0 && segment->type() == SegmentType::Sealed &&
        segment->is_chunked() && pinned_index.size() == 1) {
        auto base_offset =
            chunk_id == 0 ? 0
                          : segment->num_rows_until_chunk(field_id, chunk_id);
        return {pinned_index[0].get(), base_offset};
    }

    if (chunk_id >= 0 && static_cast<size_t>(chunk_id) < pinned_index.size()) {
        return {pinned_index[static_cast<size_t>(chunk_id)].get(), 0};
    }

    return {nullptr, 0};
}
}  // namespace

template <typename T>
MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkDataAccessor(
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index) const {
    const index::IndexBase* index = nullptr;
    if (current_chunk_id < pinned_index.size()) {
        index = pinned_index[current_chunk_id].get();
    }
    if (index) {
        auto index_ptr = dynamic_cast<const index::ScalarIndex<T>*>(index);
        if (index_ptr->HasRawData()) {
            return
                [&,
                 index_ptr = std::move(index_ptr)]() -> const data_access_type {
                    if (current_chunk_pos >= active_count_) {
                        return std::nullopt;
                    }
                    auto raw = index_ptr->Reverse_Lookup(current_chunk_pos++);
                    if (!raw.has_value()) {
                        return std::nullopt;
                    }
                    return raw.value();
                };
        }
    }
    auto num_chunks = segment_->num_chunk_data(field_id);
    AssertInfo(current_chunk_id < num_chunks,
               "field {} cursor chunk_id {} exceeds num_chunks {}",
               field_id.get(),
               current_chunk_id,
               num_chunks);
    // pw is captured by value, each time we need to access a new chunk, we need to
    // pin a new Chunk.
    auto pw = segment_->chunk_data<T>(op_ctx_, field_id, current_chunk_id);
    auto chunk_info = pw.get();
    auto chunk_data = chunk_info.data();
    auto chunk_valid_data = chunk_info.valid_data();
    auto current_chunk_size = segment_->chunk_size(field_id, current_chunk_id);
    return [=,
            pw = std::move(pw),
            &current_chunk_id,
            &current_chunk_pos]() mutable -> const data_access_type {
        if (current_chunk_pos >= current_chunk_size) {
            current_chunk_id++;
            current_chunk_pos = 0;
            AssertInfo(current_chunk_id < num_chunks,
                       "field {} cursor chunk_id {} exceeds num_chunks {}",
                       field_id.get(),
                       current_chunk_id,
                       num_chunks);
            // the old chunk will be unpinned, pw will now pin the new chunk.
            pw = segment_->chunk_data<T>(op_ctx_, field_id, current_chunk_id);
            chunk_data = pw.get().data();
            chunk_valid_data = pw.get().valid_data();
            current_chunk_size =
                segment_->chunk_size(field_id, current_chunk_id);
        }
        if (chunk_valid_data && !chunk_valid_data[current_chunk_pos]) {
            current_chunk_pos++;
            return std::nullopt;
        }
        return chunk_data[current_chunk_pos++];
    };
}

template <>
MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkDataAccessor<std::string>(
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index) const {
    const index::IndexBase* index = nullptr;
    if (current_chunk_id < pinned_index.size()) {
        index = pinned_index[current_chunk_id].get();
    }
    if (index) {
        auto index_ptr =
            dynamic_cast<const index::ScalarIndex<std::string>*>(index);
        if (index_ptr->HasRawData()) {
            return [&, index_ptr = std::move(index_ptr)]() mutable
                   -> const data_access_type {
                if (current_chunk_pos >= active_count_) {
                    return std::nullopt;
                }
                auto raw = index_ptr->Reverse_Lookup(current_chunk_pos++);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    auto num_chunks = segment_->num_chunk_data(field_id);
    AssertInfo(current_chunk_id < num_chunks,
               "field {} cursor chunk_id {} exceeds num_chunks {}",
               field_id.get(),
               current_chunk_id,
               num_chunks);
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto pw = segment_->chunk_data<std::string>(
            op_ctx_, field_id, current_chunk_id);
        auto chunk_info = pw.get();
        auto chunk_data = chunk_info.data();
        auto chunk_valid_data = chunk_info.valid_data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [pw = std::move(pw),
                this,
                field_id,
                chunk_data,
                chunk_valid_data,
                current_chunk_size,
                num_chunks,
                // pw = std::move(pw),
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                AssertInfo(current_chunk_id < num_chunks,
                           "field {} cursor chunk_id {} exceeds num_chunks {}",
                           field_id.get(),
                           current_chunk_id,
                           num_chunks);
                pw = segment_->chunk_data<std::string>(
                    op_ctx_, field_id, current_chunk_id);
                chunk_data = pw.get().data();
                chunk_valid_data = pw.get().valid_data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }
            if (chunk_valid_data && !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }
            return data_access_type(
                std::string_view(chunk_data[current_chunk_pos++]));
        };
    } else {
        auto pw = segment_->chunk_view<std::string_view>(
            op_ctx_, field_id, current_chunk_id);
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                pw = std::move(pw),
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                AssertInfo(current_chunk_id < num_chunks,
                           "field {} cursor chunk_id {} exceeds num_chunks {}",
                           field_id.get(),
                           current_chunk_id,
                           num_chunks);
                pw = segment_->chunk_view<std::string_view>(
                    op_ctx_, field_id, current_chunk_id);
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }
            auto& chunk_data = pw.get().first;
            auto& chunk_valid_data = pw.get().second;
            if (current_chunk_pos < chunk_valid_data.size() &&
                !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }
            return data_access_type(chunk_data[current_chunk_pos++]);
        };
    }
}

MultipleChunkDataAccessor
SegmentChunkReader::GetMultipleChunkDataAccessor(
    DataType data_type,
    FieldId field_id,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos,
    PinnedIndexView pinned_index) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetMultipleChunkDataAccessor<bool>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::INT8:
            return GetMultipleChunkDataAccessor<int8_t>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::INT16:
            return GetMultipleChunkDataAccessor<int16_t>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::INT32:
            return GetMultipleChunkDataAccessor<int32_t>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::INT64:
            return GetMultipleChunkDataAccessor<int64_t>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::TIMESTAMPTZ:
            return GetMultipleChunkDataAccessor<int64_t>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::FLOAT:
            return GetMultipleChunkDataAccessor<float>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::DOUBLE:
            return GetMultipleChunkDataAccessor<double>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        case DataType::VARCHAR: {
            return GetMultipleChunkDataAccessor<std::string>(
                field_id, current_chunk_id, current_chunk_pos, pinned_index);
        }
        default:
            ThrowInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

template <typename T>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(FieldId field_id,
                                         int chunk_id,
                                         PinnedIndexView pinned_index) const {
    auto [index, base_offset] =
        GetIndexAndBaseOffset(segment_, field_id, chunk_id, pinned_index);
    auto index_ptr = dynamic_cast<const index::ScalarIndex<T>*>(index);
    if (index_ptr != nullptr && index_ptr->HasRawData()) {
        return
            [index_ptr, base_offset](int i) mutable -> const data_access_type {
                auto raw = index_ptr->Reverse_Lookup(base_offset + i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
    }
    auto num_chunks = segment_->num_chunk_data(field_id);
    AssertInfo(chunk_id >= 0 && chunk_id < num_chunks,
               "field {} chunk_id {} exceeds raw data chunks {}",
               field_id.get(),
               chunk_id,
               num_chunks);
    auto pw = segment_->chunk_data<T>(op_ctx_, field_id, chunk_id);
    return [pw = std::move(pw)](int i) mutable -> const data_access_type {
        auto chunk_info = pw.get();
        auto chunk_data = chunk_info.data();
        auto chunk_valid_data = chunk_info.valid_data();
        if (chunk_valid_data && !chunk_valid_data[i]) {
            return std::nullopt;
        }
        return chunk_data[i];
    };
}

template <>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor<std::string>(
    FieldId field_id, int chunk_id, PinnedIndexView pinned_index) const {
    auto [index, base_offset] =
        GetIndexAndBaseOffset(segment_, field_id, chunk_id, pinned_index);
    auto index_ptr =
        dynamic_cast<const index::ScalarIndex<std::string>*>(index);
    if (index_ptr != nullptr && index_ptr->HasRawData()) {
        return
            [index_ptr, base_offset](int i) mutable -> const data_access_type {
                auto raw = index_ptr->Reverse_Lookup(base_offset + i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
    }
    auto num_chunks = segment_->num_chunk_data(field_id);
    AssertInfo(chunk_id >= 0 && chunk_id < num_chunks,
               "field {} chunk_id {} exceeds raw data chunks {}",
               field_id.get(),
               chunk_id,
               num_chunks);
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto pw =
            segment_->chunk_data<std::string>(op_ctx_, field_id, chunk_id);
        return [pw = std::move(pw)](int i) mutable -> const data_access_type {
            auto chunk_data = pw.get().data();
            auto chunk_valid_data = pw.get().valid_data();
            if (chunk_valid_data && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return data_access_type(std::string_view(chunk_data[i]));
        };
    } else {
        auto pw =
            segment_->chunk_view<std::string_view>(op_ctx_, field_id, chunk_id);
        return [pw = std::move(pw)](int i) mutable -> const data_access_type {
            auto& chunk_data = pw.get().first;
            auto& chunk_valid_data = pw.get().second;
            if (i < chunk_valid_data.size() && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return data_access_type(chunk_data[i]);
        };
    }
}

ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(DataType data_type,
                                         FieldId field_id,
                                         int chunk_id,
                                         PinnedIndexView pinned_index) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkDataAccessor<bool>(field_id, chunk_id, pinned_index);
        case DataType::INT8:
            return GetChunkDataAccessor<int8_t>(
                field_id, chunk_id, pinned_index);
        case DataType::INT16:
            return GetChunkDataAccessor<int16_t>(
                field_id, chunk_id, pinned_index);
        case DataType::INT32:
            return GetChunkDataAccessor<int32_t>(
                field_id, chunk_id, pinned_index);
        case DataType::TIMESTAMPTZ:
        case DataType::INT64:
            return GetChunkDataAccessor<int64_t>(
                field_id, chunk_id, pinned_index);
        case DataType::FLOAT:
            return GetChunkDataAccessor<float>(
                field_id, chunk_id, pinned_index);
        case DataType::DOUBLE:
            return GetChunkDataAccessor<double>(
                field_id, chunk_id, pinned_index);
        case DataType::VARCHAR:
        case DataType::TEXT: {
            return GetChunkDataAccessor<std::string>(
                field_id, chunk_id, pinned_index);
        }
        default:
            ThrowInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

}  // namespace milvus::segcore
