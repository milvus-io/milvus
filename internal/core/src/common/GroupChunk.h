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

#include <cstdint>
#include <memory>
#include <utility>
#include "common/Types.h"
#include "common/Chunk.h"

namespace milvus {

// GroupChunk represents a collection of chunks for different fields in a group
class GroupChunk {
 public:
    GroupChunk() = default;
    explicit GroupChunk(
        const std::unordered_map<FieldId, std::shared_ptr<Chunk>>& chunks)
        : chunks_(chunks) {
    }

    virtual ~GroupChunk() = default;

    // Get the chunk for a specific field
    std::shared_ptr<Chunk>
    GetChunk(FieldId field_id) const {
        auto it = chunks_.find(field_id);
        if (it == chunks_.end()) {
            return nullptr;
        }
        return it->second;
    }

    // Add a chunk for a specific field
    void
    AddChunk(FieldId field_id, std::shared_ptr<Chunk> chunk) {
        if (chunks_.find(field_id) != chunks_.end()) {
            PanicInfo(ErrorCode::FieldAlreadyExist,
                      "Field {} already exists in GroupChunk",
                      field_id.get());
        }
        chunks_[field_id] = std::move(chunk);
    }

    uint64_t
    Size() const {
        uint64_t total_size = 0;
        for (const auto& chunk : chunks_) {
            total_size += chunk.second->Size();
        }
        return total_size;
    }

    // Get all chunks
    const std::unordered_map<FieldId, std::shared_ptr<Chunk>>&
    GetChunks() const {
        return chunks_;
    }

    size_t
    CellByteSize() const {
        size_t total_size = 0;
        for (const auto& chunk : chunks_) {
            total_size += chunk.second->CellByteSize();
        }
        return total_size;
    }

    // Get the number of rows in this group chunk
    int64_t
    RowNums() const {
        if (chunks_.empty()) {
            return 0;
        }
        return chunks_.begin()->second->RowNums();
    }

    // Check if the chunk for a specific field exists
    bool
    HasChunk(FieldId field_id) const {
        return chunks_.find(field_id) != chunks_.end();
    }

 private:
    std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks_;
};

}  // namespace milvus