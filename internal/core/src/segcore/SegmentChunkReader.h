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
#pragma once

#include <fmt/core.h>
#include <boost/variant.hpp>
#include <optional>

#include "common/Types.h"
#include "segcore/SegmentInterface.h"

namespace milvus::segcore {

using data_access_type = std::optional<boost::variant<bool,
                                                      int8_t,
                                                      int16_t,
                                                      int32_t,
                                                      int64_t,
                                                      float,
                                                      double,
                                                      std::string>>;

using ChunkDataAccessor = std::function<const data_access_type(int)>;
using MultipleChunkDataAccessor = std::function<const data_access_type()>;

class SegmentChunkReader {
 public:
    SegmentChunkReader(const segcore::SegmentInternalInterface* segment,
                       int64_t active_count)
        : segment_(segment), active_count_(active_count) {
    }

    template <typename T>
    MultipleChunkDataAccessor
    GetChunkData(FieldId field_id,
                 bool index,
                 int64_t& current_chunk_id,
                 int64_t& current_chunk_pos) const {
        if (index) {
            auto& indexing = const_cast<index::ScalarIndex<T>&>(
                segment_->chunk_scalar_index<T>(field_id, current_chunk_id));
            auto current_chunk_size = segment_->type() == SegmentType::Growing
                                          ? SizePerChunk()
                                          : active_count_;

            if (indexing.HasRawData()) {
                return [&, current_chunk_size]() -> const data_access_type {
                    if (current_chunk_pos >= current_chunk_size) {
                        current_chunk_id++;
                        current_chunk_pos = 0;
                        indexing = const_cast<index::ScalarIndex<T>&>(
                            segment_->chunk_scalar_index<T>(field_id,
                                                            current_chunk_id));
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
        auto chunk_data =
            segment_->chunk_data<T>(field_id, current_chunk_id).data();
        auto chunk_valid_data =
            segment_->chunk_data<T>(field_id, current_chunk_id).valid_data();
        auto current_chunk_size =
            segment_->chunk_size(field_id, current_chunk_id);
        return [=,
                &current_chunk_id,
                &current_chunk_pos]() mutable -> const data_access_type {
            if (current_chunk_pos >= current_chunk_size) {
                current_chunk_id++;
                current_chunk_pos = 0;
                chunk_data =
                    segment_->chunk_data<T>(field_id, current_chunk_id).data();
                chunk_valid_data =
                    segment_->chunk_data<T>(field_id, current_chunk_id)
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
    }

    template <typename T>
    ChunkDataAccessor
    GetChunkData(FieldId field_id, int chunk_id, int data_barrier) const {
        if (chunk_id >= data_barrier) {
            auto& indexing =
                segment_->chunk_scalar_index<T>(field_id, chunk_id);
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

    int64_t
    SizePerChunk() const {
        return segment_->size_per_chunk();
    }

    const segcore::SegmentInternalInterface* segment_;
    const int64_t active_count_;
};
}  // namespace milvus::segcore
