// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "segcore/SegmentChunkReader.h"
namespace milvus::segcore {
template <>
MultipleChunkDataAccessor
SegmentChunkReader::GetChunkData<std::string>(
    FieldId field_id,
    bool index,
    int64_t& current_chunk_id,
    int64_t& current_chunk_pos) const {
    if (index) {
        auto& indexing = const_cast<index::ScalarIndex<std::string>&>(
            segment_->chunk_scalar_index<std::string>(field_id,
                                                      current_chunk_id));
        auto current_chunk_size = segment_->type() == SegmentType::Growing
                                      ? SizePerChunk()
                                      : active_count_;

        if (indexing.HasRawData()) {
            return [&, current_chunk_size]() mutable -> const data_access_type {
                if (current_chunk_pos >= current_chunk_size) {
                    current_chunk_id++;
                    current_chunk_pos = 0;
                    indexing = const_cast<index::ScalarIndex<std::string>&>(
                        segment_->chunk_scalar_index<std::string>(
                            field_id, current_chunk_id));
                }
                auto raw = indexing.Reverse_Lookup(current_chunk_pos);
                current_chunk_pos++;
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
            segment_->chunk_data<std::string>(field_id, current_chunk_id)
                .data();
        auto chunk_valid_data =
            segment_->chunk_data<std::string>(field_id, current_chunk_id)
                .valid_data();
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
        auto chunk_data =
            segment_->chunk_view<std::string_view>(field_id, current_chunk_id)
                .first.data();
        auto chunk_valid_data =
            segment_->chunk_data<std::string_view>(field_id, current_chunk_id)
                .valid_data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data = segment_
                                 ->chunk_view<std::string_view>(
                                     field_id, current_chunk_id)
                                 .first.data();
                chunk_valid_data = segment_
                                       ->chunk_data<std::string_view>(
                                           field_id, current_chunk_id)
                                       .valid_data();
                current_chunk_size =
                    segment_->chunk_size(field_id, current_chunk_id);
            }
            if (chunk_valid_data && !chunk_valid_data[current_chunk_pos]) {
                current_chunk_pos++;
                return std::nullopt;
            }

            return std::string(chunk_data[current_chunk_pos++]);
        };
    }
}

template <>
ChunkDataAccessor
SegmentChunkReader::GetChunkData<std::string>(FieldId field_id,
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
        auto chunk_data = chunk_info.first.data();
        auto chunk_valid_data = chunk_info.second.data();
        return [chunk_data, chunk_valid_data](int i) -> const data_access_type {
            if (chunk_valid_data && !chunk_valid_data[i]) {
                return std::nullopt;
            }
            return std::string(chunk_data[i]);
        };
    }
}
}  // namespace milvus::segcore
