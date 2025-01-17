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

namespace milvus::segcore {
template <typename T>
MultipleChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(FieldId field_id,
                                         bool index,
                                         int64_t& current_chunk_id,
                                         int64_t& current_chunk_pos) const {
    if (index) {
        auto& indexing = const_cast<index::ScalarIndex<T>&>(
            segment_->chunk_scalar_index<T>(field_id, current_chunk_id));

        if (indexing.HasRawData()) {
            return [&]() -> const data_access_type {
                if (current_chunk_pos >= active_count_) {
                    return std::nullopt;
                }
                auto raw = indexing.Reverse_Lookup(current_chunk_pos++);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    auto chunk_info = segment_->chunk_data<T>(field_id, current_chunk_id);
    auto chunk_data = chunk_info.data();
    auto chunk_valid_data = chunk_info.valid_data();
    auto current_chunk_size = segment_->chunk_size(field_id, current_chunk_id);
    return [=,
            &current_chunk_id,
            &current_chunk_pos]() mutable -> const data_access_type {
        if (current_chunk_pos >= current_chunk_size) {
            current_chunk_id++;
            current_chunk_pos = 0;
            auto chunk_info =
                segment_->chunk_data<T>(field_id, current_chunk_id);
            chunk_data = chunk_info.data();
            chunk_valid_data = chunk_info.valid_data();
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
SegmentChunkReader::GetChunkDataAccessor<std::string>(
    FieldId field_id,
    bool index,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos) const {
    if (index) {
        auto& indexing = const_cast<index::ScalarIndex<std::string>&>(
            segment_->chunk_scalar_index<std::string>(field_id,
                                                      current_chunk_id));
        if (indexing.HasRawData()) {
            return [&]() mutable -> const data_access_type {
                if (current_chunk_pos >= active_count_) {
                    return std::nullopt;
                }
                auto raw = indexing.Reverse_Lookup(current_chunk_pos++);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto chunk_info =
            segment_->chunk_data<std::string>(field_id, current_chunk_id);
        auto chunk_data = chunk_info.data();
        auto chunk_valid_data = chunk_info.valid_data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data =
                    segment_
                        ->chunk_data<std::string>(field_id, current_chunk_id)
                        .data();
                chunk_valid_data =
                    segment_
                        ->chunk_data<std::string>(field_id, current_chunk_id)
                        .valid_data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }
            if (chunk_valid_data && !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }
            return chunk_data[current_chunk_pos++];
        };
    } else {
        auto chunk_info =
            segment_->chunk_view<std::string_view>(field_id, current_chunk_id);

        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_info = segment_->chunk_view<std::string_view>(
                    field_id, current_chunk_id);
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }
            auto& chunk_data = chunk_info.first;
            auto& chunk_valid_data = chunk_info.second;
            if (current_chunk_pos < chunk_valid_data.size() &&
                !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }

            return std::string(chunk_data[current_chunk_pos++]);
        };
    }
}

MultipleChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(DataType data_type,
                                         FieldId field_id,
                                         bool index,
                                         int64_t& current_chunk_id,
                                         int64_t& current_chunk_pos) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkDataAccessor<bool>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT8:
            return GetChunkDataAccessor<int8_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT16:
            return GetChunkDataAccessor<int16_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT32:
            return GetChunkDataAccessor<int32_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::INT64:
            return GetChunkDataAccessor<int64_t>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::FLOAT:
            return GetChunkDataAccessor<float>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::DOUBLE:
            return GetChunkDataAccessor<double>(
                field_id, index, current_chunk_id, current_chunk_pos);
        case DataType::VARCHAR: {
            return GetChunkDataAccessor<std::string>(
                field_id, index, current_chunk_id, current_chunk_pos);
        }
        default:
            PanicInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

template <typename T>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(FieldId field_id,
                                         int chunk_id,
                                         int data_barrier) const {
    if (chunk_id >= data_barrier) {
        auto& indexing = segment_->chunk_scalar_index<T>(field_id, chunk_id);
        if (indexing.HasRawData()) {
            return [&indexing](int i) -> const data_access_type {
                auto raw = indexing.Reverse_Lookup(i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    auto chunk_data = segment_->chunk_data<T>(field_id, chunk_id).data();
    auto chunk_valid_data =
        segment_->chunk_data<T>(field_id, chunk_id).valid_data();
    return [chunk_data, chunk_valid_data](int i) -> const data_access_type {
        if (chunk_valid_data && !chunk_valid_data[i]) {
            return std::nullopt;
        }
        return chunk_data[i];
    };
}

template <>
ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor<std::string>(FieldId field_id,
                                                      int chunk_id,
                                                      int data_barrier) const {
    if (chunk_id >= data_barrier) {
        auto& indexing =
            segment_->chunk_scalar_index<std::string>(field_id, chunk_id);
        if (indexing.HasRawData()) {
            return [&indexing](int i) -> const data_access_type {
                auto raw = indexing.Reverse_Lookup(i);
                if (!raw.has_value()) {
                    return std::nullopt;
                }
                return raw.value();
            };
        }
    }
    if (segment_->type() == SegmentType::Growing &&
        !storage::MmapManager::GetInstance()
             .GetMmapConfig()
             .growing_enable_mmap) {
        auto chunk_data =
            segment_->chunk_data<std::string>(field_id, chunk_id).data();
        auto chunk_valid_data =
            segment_->chunk_data<std::string>(field_id, chunk_id).valid_data();
        return [chunk_data, chunk_valid_data](int i) -> const data_access_type {
            if (chunk_valid_data && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return chunk_data[i];
        };
    } else {
        auto chunk_info =
            segment_->chunk_view<std::string_view>(field_id, chunk_id);
        return [chunk_data = std::move(chunk_info.first),
                chunk_valid_data = std::move(chunk_info.second)](
                   int i) -> const data_access_type {
            if (i < chunk_valid_data.size() && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return std::string(chunk_data[i]);
        };
    }
}

ChunkDataAccessor
SegmentChunkReader::GetChunkDataAccessor(DataType data_type,
                                         FieldId field_id,
                                         int chunk_id,
                                         int data_barrier) const {
    switch (data_type) {
        case DataType::BOOL:
            return GetChunkDataAccessor<bool>(field_id, chunk_id, data_barrier);
        case DataType::INT8:
            return GetChunkDataAccessor<int8_t>(
                field_id, chunk_id, data_barrier);
        case DataType::INT16:
            return GetChunkDataAccessor<int16_t>(
                field_id, chunk_id, data_barrier);
        case DataType::INT32:
            return GetChunkDataAccessor<int32_t>(
                field_id, chunk_id, data_barrier);
        case DataType::INT64:
            return GetChunkDataAccessor<int64_t>(
                field_id, chunk_id, data_barrier);
        case DataType::FLOAT:
            return GetChunkDataAccessor<float>(
                field_id, chunk_id, data_barrier);
        case DataType::DOUBLE:
            return GetChunkDataAccessor<double>(
                field_id, chunk_id, data_barrier);
        case DataType::VARCHAR: {
            return GetChunkDataAccessor<std::string>(
                field_id, chunk_id, data_barrier);
        }
        default:
            PanicInfo(DataTypeInvalid, "unsupported data type: {}", data_type);
    }
}

}  // namespace milvus::segcore
