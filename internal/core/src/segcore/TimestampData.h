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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "common/Chunk.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnInterface.h"
#include "segcore/storagev1translator/ChunkTranslator.h"

namespace milvus::segcore {

// TimestampData provides read-only access to timestamp data for sealed segments.
// Supports segmented (multi-chunk) access with two storage modes:
// - Pin mode: zero-copy from ChunkedColumnInterface chunks (StorageV2).
//   Holds column reference and chunk pins to prevent eviction.
// - Own mode: owns a std::vector<Timestamp> (StorageV1).
//
// Uses prefix-sum array (num_rows_until_chunk_) + virtual-chunk index
// (vcid_to_cid_arr_) for O(1) offset-to-chunk lookup, consistent with
// the pattern in ChunkTranslator / ChunkedColumn.
class TimestampData {
 public:
    TimestampData() = default;

    TimestampData(const TimestampData&) = delete;
    TimestampData&
    operator=(const TimestampData&) = delete;
    TimestampData(TimestampData&&) = default;
    TimestampData&
    operator=(TimestampData&&) = default;

    // Pin mode: zero-copy from column chunks (single or multi-chunk).
    // Extracts data pointers from each pinned FixedWidthChunk.
    void
    InitFromPinnedChunks(std::shared_ptr<ChunkedColumnInterface> column,
                         std::vector<cachinglayer::PinWrapper<Chunk*>> pins) {
        column_ = std::move(column);
        pins_ = std::move(pins);
        chunk_data_.reserve(pins_.size());
        num_rows_until_chunk_.reserve(pins_.size() + 1);
        num_rows_until_chunk_.push_back(0);
        for (auto& pin : pins_) {
            auto* fixed_chunk = static_cast<FixedWidthChunk*>(pin.get());
            auto span = fixed_chunk->Span();
            chunk_data_.push_back(static_cast<const Timestamp*>(span.data()));
            num_rows_until_chunk_.push_back(
                num_rows_until_chunk_.back() +
                static_cast<int64_t>(span.row_count()));
        }
        total_size_ = num_rows_until_chunk_.back();
        build_virtual_chunk_index();
    }

    // Own mode: takes ownership of contiguous timestamp data.
    void
    InitFromOwnedData(std::vector<Timestamp> data) {
        total_size_ = static_cast<int64_t>(data.size());
        owned_ = std::move(data);
        chunk_data_.push_back(owned_.data());
        num_rows_until_chunk_.push_back(0);
        num_rows_until_chunk_.push_back(total_size_);
        // single chunk, no need for virtual chunk index
        virt_chunk_order_ = 0;
        vcid_to_cid_arr_.assign(1, 0);
    }

    Timestamp
    operator[](int64_t i) const {
        // Fast path: single chunk
        if (chunk_data_.size() == 1) {
            return chunk_data_[0][i];
        }
        // O(1) lookup via virtual chunk index
        auto cid = vcid_to_cid_arr_[i >> virt_chunk_order_];
        while (cid < static_cast<int64_t>(chunk_data_.size()) - 1 &&
               i >= num_rows_until_chunk_[cid + 1]) {
            ++cid;
        }
        return chunk_data_[cid][i - num_rows_until_chunk_[cid]];
    }

    int64_t
    size() const {
        return total_size_;
    }

    bool
    empty() const {
        return total_size_ == 0;
    }

    // Chunk-level access for callers that need raw data pointers
    int64_t
    num_chunks() const {
        return static_cast<int64_t>(chunk_data_.size());
    }

    const Timestamp*
    chunk_data(int64_t chunk_id) const {
        return chunk_data_[chunk_id];
    }

    int64_t
    chunk_row_count(int64_t chunk_id) const {
        return num_rows_until_chunk_[chunk_id + 1] -
               num_rows_until_chunk_[chunk_id];
    }

    int64_t
    chunk_start_offset(int64_t chunk_id) const {
        return num_rows_until_chunk_[chunk_id];
    }

    void
    clear() {
        chunk_data_.clear();
        num_rows_until_chunk_.clear();
        vcid_to_cid_arr_.clear();
        virt_chunk_order_ = 0;
        total_size_ = 0;
        column_.reset();
        pins_.clear();
        owned_.clear();
        owned_.shrink_to_fit();
    }

 private:
    void
    build_virtual_chunk_index() {
        storagev1translator::virtual_chunk_config(
            total_size_,
            static_cast<int64_t>(chunk_data_.size()),
            num_rows_until_chunk_,
            virt_chunk_order_,
            vcid_to_cid_arr_);
    }

    // Per-chunk raw data pointers (parallel with pins_)
    std::vector<const Timestamp*> chunk_data_;
    // Prefix-sum: num_rows_until_chunk_[i] = total rows before chunk i.
    // Size = num_chunks + 1. num_rows_until_chunk_[num_chunks] = total_size_.
    std::vector<int64_t> num_rows_until_chunk_;
    // Virtual chunk index for O(1) offset->chunk_id lookup
    int64_t virt_chunk_order_ = 0;
    std::vector<int64_t> vcid_to_cid_arr_;
    int64_t total_size_ = 0;

    // Pin mode: hold column reference and pins to prevent eviction
    std::shared_ptr<ChunkedColumnInterface> column_;
    std::vector<cachinglayer::PinWrapper<Chunk*>> pins_;
    // Own mode: self-owned data
    std::vector<Timestamp> owned_;
};

}  // namespace milvus::segcore
