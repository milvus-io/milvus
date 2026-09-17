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

#include "index/scalar/spatial/RTreeIndexReader.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus::index {
namespace {

class GeosContextGuard {
 public:
    GeosContextGuard() : context_(GEOS_init_r()) {
        if (context_ == nullptr) {
            ThrowInfo(UnexpectedError,
                      "failed to initialize GEOS for R-Tree query");
        }
    }

    GeosContextGuard(const GeosContextGuard&) = delete;
    GeosContextGuard&
    operator=(const GeosContextGuard&) = delete;

    ~GeosContextGuard() {
        GEOS_finish_r(context_);
    }

    GEOSContextHandle_t
    Get() const {
        return context_;
    }

 private:
    GEOSContextHandle_t context_;
};

}  // namespace

RTreeIndexState::RTreeIndexState(
    std::vector<std::shared_ptr<const RTreeQueryEngine>> engines,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t total_num_rows,
    int64_t memory_usage)
    : engines_(std::move(engines)),
      null_offsets_(std::move(null_offsets)),
      total_num_rows_(total_num_rows),
      memory_usage_(memory_usage) {
}

std::shared_ptr<const RTreeIndexState>
RTreeIndexState::Create(std::shared_ptr<const RTreeQueryEngine> engine,
                        std::shared_ptr<const std::vector<size_t>> null_offsets,
                        int64_t total_num_rows) {
    AssertInfo(engine != nullptr, "R-Tree state engine must not be null");
    AssertInfo(null_offsets != nullptr,
               "R-Tree state NULL offsets must not be null");
    AssertInfo(total_num_rows >= 0,
               "R-Tree state row count must not be negative");
    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets) {
        if ((!first && offset <= previous) ||
            offset >= static_cast<size_t>(total_num_rows)) {
            ThrowInfo(DataFormatBroken,
                      "invalid R-Tree null offset {} for row count {}",
                      offset,
                      total_num_rows);
        }
        previous = offset;
        first = false;
    }
    engine->ValidateCoordinates(total_num_rows, *null_offsets);
    std::vector<std::shared_ptr<const RTreeQueryEngine>> engines;
    engines.push_back(std::move(engine));
    return CreateFromValidatedShards(
        std::move(engines), std::move(null_offsets), total_num_rows);
}

std::shared_ptr<const RTreeIndexState>
RTreeIndexState::CreateFromValidatedShards(
    std::vector<std::shared_ptr<const RTreeQueryEngine>> engines,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    int64_t total_num_rows) {
    AssertInfo(null_offsets != nullptr,
               "R-Tree state NULL offsets must not be null");
    AssertInfo(total_num_rows >= 0,
               "R-Tree state row count must not be negative");
    int64_t memory_usage = static_cast<int64_t>(
        sizeof(RTreeIndexState) + sizeof(std::vector<size_t>) +
        sizeof(std::vector<std::shared_ptr<const RTreeQueryEngine>>));
    const auto add_bytes = [&](uint64_t bytes) {
        if (bytes > static_cast<uint64_t>(
                        std::numeric_limits<int64_t>::max() - memory_usage)) {
            memory_usage = std::numeric_limits<int64_t>::max();
        } else {
            memory_usage += static_cast<int64_t>(bytes);
        }
    };
    const auto add_capacity = [&](size_t count, size_t item_bytes) {
        if (count > static_cast<size_t>(
                        (std::numeric_limits<int64_t>::max() - memory_usage) /
                        static_cast<int64_t>(item_bytes))) {
            memory_usage = std::numeric_limits<int64_t>::max();
        } else {
            add_bytes(static_cast<uint64_t>(count * item_bytes));
        }
    };
    add_capacity(null_offsets->capacity(), sizeof(size_t));
    add_capacity(engines.capacity(),
                 sizeof(std::shared_ptr<const RTreeQueryEngine>));
    for (const auto& engine : engines) {
        AssertInfo(engine != nullptr, "R-Tree state engine must not be null");
        const auto engine_bytes = engine->ByteSize();
        AssertInfo(engine_bytes >= 0,
                   "R-Tree engine memory size must not be negative");
        add_bytes(static_cast<uint64_t>(engine_bytes));
    }
    return std::shared_ptr<const RTreeIndexState>(
        new RTreeIndexState(std::move(engines),
                            std::move(null_offsets),
                            total_num_rows,
                            memory_usage));
}

const std::vector<std::shared_ptr<const RTreeQueryEngine>>&
RTreeIndexState::Engines() const {
    return engines_;
}

const std::vector<size_t>&
RTreeIndexState::NullOffsets() const {
    return *null_offsets_;
}

int64_t
RTreeIndexState::Count() const {
    return total_num_rows_;
}

int64_t
RTreeIndexState::MemoryUsage() const {
    return memory_usage_;
}

RTreeIndexReader::RTreeIndexReader(std::shared_ptr<const RTreeIndexState> state)
    : state_(std::move(state)) {
    AssertInfo(state_ != nullptr, "R-Tree reader state must not be null");
}

RTreeIndexReader::~RTreeIndexReader() = default;

ReaderCaps
RTreeIndexReader::Caps() const {
    // The MBR filter returns a superset; execution refines against exact geometry.
    return ReaderCaps{.spatial = true, .exact = false};
}

Domain
RTreeIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
RTreeIndexReader::Count() const {
    return state_->Count();
}

DataType
RTreeIndexReader::ValueType() const {
    return DataType::GEOMETRY;
}

int64_t
RTreeIndexReader::MemoryUsage() const {
    return state_->MemoryUsage();
}

cachinglayer::ResourceUsage
RTreeIndexReader::CellByteSize() const {
    // Every referenced Boost shard is resident. No mmap/file-backed bytes
    // remain owned by this reader. MemoryUsage counts a shared shard once per
    // generation and is not additive across concurrently pinned generations;
    // growing accounting must take the union of their shared engine owners.
    return {MemoryUsage(), 0};
}

TargetBitmap
RTreeIndexReader::Candidates(SpatialOp op, const Geometry& query_geom) const {
    static_cast<void>(op);
    if (!query_geom.IsValid() || state_->Engines().empty()) {
        return IsNotNull();
    }
    GeosContextGuard context;
    TargetBitmap result(static_cast<size_t>(state_->Count()), false);
    for (const auto& engine : state_->Engines()) {
        const auto has_query_box = engine->ForEachCandidate(
            query_geom.GetGeometry(), context.Get(), [&](int64_t offset) {
                AssertInfo(offset >= 0 && offset < state_->Count(),
                           "R-Tree candidate offset {} is outside [0, {})",
                           offset,
                           state_->Count());
                result.set(static_cast<size_t>(offset));
            });
        if (!has_query_box) {
            return IsNotNull();
        }
    }
    return result;
}

TargetBitmap
RTreeIndexReader::IsNull() const {
    TargetBitmap result(static_cast<size_t>(state_->Count()), false);
    for (const auto offset : state_->NullOffsets()) {
        result.set(offset);
    }
    return result;
}

TargetBitmap
RTreeIndexReader::IsNotNull() const {
    auto result = IsNull();
    result.flip();
    return result;
}

}  // namespace milvus::index
