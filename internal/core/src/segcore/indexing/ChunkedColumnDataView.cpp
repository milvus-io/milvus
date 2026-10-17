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

#include "segcore/indexing/ChunkedColumnDataView.h"

#include <cstdint>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/EasyAssert.h"
#include "mmap/ChunkedColumnInterface.h"

namespace milvus::segcore {
namespace {

struct ChunkedColumnDataViewState {
    std::shared_ptr<ChunkedColumnInterface> column;
    milvus::OpContext* op_ctx;
    std::mutex mutex;
    std::unordered_map<int64_t, cachinglayer::PinWrapper<Chunk*>> pins;
};

}  // namespace

storage::VectorDataViewScope
CreateChunkedColumnDataViewScope(ChunkedColumnResolver resolve_column) {
    AssertInfo(static_cast<bool>(resolve_column),
               "chunked-column resolver is empty");
    return [resolve_column = std::move(resolve_column)](
               milvus::OpContext* op_ctx) -> storage::VectorDataView {
        auto column = resolve_column();
        AssertInfo(column != nullptr, "raw vector column is unavailable");
        auto state = std::make_shared<ChunkedColumnDataViewState>();
        state->column = std::move(column);
        state->op_ctx = op_ctx;
        return [state = std::move(state)](size_t logical_offset) {
            AssertInfo(
                logical_offset <=
                    static_cast<size_t>(std::numeric_limits<int64_t>::max()),
                "raw vector offset {} exceeds int64",
                logical_offset);
            const auto [chunk_id, offset_in_chunk] =
                state->column->GetChunkIDByOffset(
                    static_cast<int64_t>(logical_offset));

            std::lock_guard lock(state->mutex);
            const auto id = static_cast<int64_t>(chunk_id);
            auto it = state->pins.find(id);
            if (it == state->pins.end()) {
                auto pin = state->column->GetChunk(state->op_ctx, id);
                it = state->pins.emplace(id, std::move(pin)).first;
            }
            auto* chunk = it->second.get();
            AssertInfo(
                chunk != nullptr, "raw vector chunk {} is unavailable", id);
            const auto physical_offset =
                state->column->IsNullable()
                    ? chunk->PhysicalOffsetOf(offset_in_chunk)
                    : offset_in_chunk;
            return static_cast<const void*>(chunk->ValueAt(physical_offset));
        };
    };
}

}  // namespace milvus::segcore
