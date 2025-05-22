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
        : segment_(segment),
          active_count_(active_count),
          size_per_chunk_(segment->size_per_chunk()) {
    }

    MultipleChunkDataAccessor
    GetChunkDataAccessor(DataType data_type,
                         FieldId field_id,
                         bool index,
                         int64_t& current_chunk_id,
                         int64_t& current_chunk_pos) const;

    ChunkDataAccessor
    GetChunkDataAccessor(DataType data_type,
                         FieldId field_id,
                         int chunk_id,
                         int data_barrier) const;

    void
    MoveCursorForMultipleChunk(int64_t& current_chunk_id,
                               int64_t& current_chunk_pos,
                               const FieldId field_id,
                               const int64_t num_chunk,
                               const int64_t batch_size) const {
        int64_t segment_row_count = segment_->get_row_count();
        int64_t current_offset =
            segment_->num_rows_until_chunk(field_id, current_chunk_id) +
            current_chunk_pos;
        int64_t target_offset = current_offset + batch_size;

        if (target_offset >= segment_row_count) {
            current_chunk_id = num_chunk - 1;
            current_chunk_pos =
                segment_row_count -
                segment_->num_rows_until_chunk(field_id, current_chunk_id);
            return;
        }
        auto [chunk_id, chunk_pos] =
            segment_->get_chunk_by_offset(field_id, target_offset);
        current_chunk_id = chunk_id;
        current_chunk_pos = chunk_pos;
    }

    void
    MoveCursorForSingleChunk(int64_t& current_chunk_id,
                             int64_t& current_chunk_pos,
                             const int64_t num_chunk,
                             const int64_t batch_size) const {
        int64_t processed_rows = 0;
        for (int64_t chunk_id = current_chunk_id; chunk_id < num_chunk;
             ++chunk_id) {
            auto chunk_size = chunk_id == num_chunk - 1
                                  ? active_count_ - chunk_id * SizePerChunk()
                                  : SizePerChunk();

            for (int64_t i = chunk_id == current_chunk_id ? current_chunk_pos
                                                          : 0;
                 i < chunk_size;
                 ++i) {
                if (++processed_rows >= batch_size) {
                    current_chunk_id = chunk_id;
                    current_chunk_pos = i + 1;
                }
            }
        }
    }

    int64_t
    SizePerChunk() const {
        return size_per_chunk_;
    }

    const int64_t active_count_;
    const segcore::SegmentInternalInterface* segment_;

 private:
    template <typename T>
    MultipleChunkDataAccessor
    GetChunkDataAccessor(FieldId field_id,
                         bool index,
                         int64_t& current_chunk_id,
                         int64_t& current_chunk_pos) const;

    template <typename T>
    ChunkDataAccessor
    GetChunkDataAccessor(FieldId field_id,
                         int chunk_id,
                         int data_barrier) const;

    const int64_t size_per_chunk_;
};

}  // namespace milvus::segcore
