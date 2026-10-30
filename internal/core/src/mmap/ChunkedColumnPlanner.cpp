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

#include "mmap/ChunkedColumnInterface.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace milvus {
namespace {

inline CellLocation
LocateInBoundaries(const std::vector<int64_t>& boundaries,
                   int64_t segment_offset,
                   int64_t uniform_cell_rows) {
    AssertInfo(segment_offset >= 0 && segment_offset < boundaries.back(),
               "segment offset {} out of planner rows {}",
               segment_offset,
               boundaries.back());
    if (boundaries.size() == 2) {
        return CellLocation{0, segment_offset};
    }
    if (uniform_cell_rows != 0) {
        return CellLocation{segment_offset / uniform_cell_rows,
                            segment_offset % uniform_cell_rows};
    }
    // The first boundary is zero and the checked row is inside the column.
    // Keep the rightmost boundary <= row; repeated boundaries (empty cells)
    // therefore have the same semantics as upper_bound - 1. Unlike a
    // data-dependent binary-search branch, each step uses a pointer select.
    const auto* base = boundaries.data();
    auto remaining = boundaries.size();
    while (remaining > 1) {
        const auto half = remaining / 2;
        base += base[half] <= segment_offset ? half : 0;
        remaining -= half;
    }
    const auto cell_id = static_cast<int64_t>(base - boundaries.data());
    return CellLocation{cell_id, segment_offset - boundaries[cell_id]};
}

}  // namespace

ColumnPlanner::ColumnPlanner(const std::vector<int64_t>& num_rows_until_cell)
    : num_rows_until_cell_(&num_rows_until_cell) {
    ValidateBoundaries();
}

ColumnPlanner::ColumnPlanner(std::vector<int64_t>&& num_rows_until_cell)
    : owned_num_rows_until_cell_(std::make_shared<const std::vector<int64_t>>(
          std::move(num_rows_until_cell))),
      num_rows_until_cell_(owned_num_rows_until_cell_.get()) {
    ValidateBoundaries();
}

void
ColumnPlanner::ValidateBoundaries() {
    AssertInfo(num_rows_until_cell_ != nullptr,
               "column planner Cell boundaries are null");
    const auto& boundaries = *num_rows_until_cell_;
    AssertInfo(boundaries.size() >= 2,
               "column planner requires at least one Cell boundary");
    AssertInfo(boundaries.front() == 0,
               "column planner first Cell boundary is {}, expected 0",
               boundaries.front());
    for (size_t i = 1; i < boundaries.size(); ++i) {
        AssertInfo(boundaries[i] >= boundaries[i - 1],
                   "column planner Cell boundaries are not monotonic at {}: "
                   "{} < {}",
                   i,
                   boundaries[i],
                   boundaries[i - 1]);
    }
    // Equal complete Cells and a possibly short final Cell need no boundary
    // search. Never extend this proof across an empty/irregular interior Cell.
    auto width = boundaries[1];
    for (size_t i = 1; width != 0 && i + 1 < boundaries.size(); ++i) {
        const auto rows = boundaries[i + 1] - boundaries[i];
        if (rows != width && (i + 2 != boundaries.size() || rows > width)) {
            width = 0;
        }
    }
    uniform_cell_rows_ = width;
}

CellLocation
ColumnPlanner::Locate(int64_t segment_offset) const {
    return LocateInBoundaries(
        *num_rows_until_cell_, segment_offset, uniform_cell_rows_);
}

void
ColumnPlanner::LocateBatch(OffsetView offsets,
                           int64_t first,
                           int64_t count,
                           CellLocation* output) const {
    AssertInfo(offsets.size >= 0 && first >= 0 && count >= 0 &&
                   first <= offsets.size && count <= offsets.size - first,
               "invalid planner offset slice first={} count={} size={}",
               first,
               count,
               offsets.size);
    if (count == 0) {
        return;
    }
    AssertInfo(offsets.data != nullptr && output != nullptr,
               "non-empty planner offset slice requires input and output");
    const auto& boundaries = *num_rows_until_cell_;
    const auto locate = [&](const auto* values) {
        for (int64_t i = 0; i < count; ++i) {
            output[i] = LocateInBoundaries(
                boundaries, values[first + i], uniform_cell_rows_);
        }
    };
    switch (offsets.element_type) {
        case OffsetElementType::Int32:
            locate(static_cast<const int32_t*>(offsets.data));
            return;
        case OffsetElementType::Int64:
            locate(static_cast<const int64_t*>(offsets.data));
            return;
        default:
            ThrowInfo(Unsupported, "unsupported planner offset element type");
    }
}

int64_t
ColumnPlanner::CellStart(int64_t cell_id) const {
    AssertInfo(cell_id >= 0 && cell_id <= NumCells(),
               "planner Cell boundary {} out of range {}",
               cell_id,
               NumCells());
    return (*num_rows_until_cell_)[cell_id];
}

int64_t
ColumnPlanner::CellRows(int64_t cell_id) const {
    return CellStart(cell_id + 1) - CellStart(cell_id);
}

int64_t
ColumnPlanner::NumCells() const {
    return static_cast<int64_t>(num_rows_until_cell_->size()) - 1;
}

int64_t
ColumnPlanner::NumRows() const {
    return num_rows_until_cell_->back();
}

const std::vector<int64_t>&
ColumnPlanner::CellBoundaries() const {
    return *num_rows_until_cell_;
}

const ColumnPlanner&
ChunkedColumnInterface::Planner() const {
    std::call_once(planner_once_, [this]() {
        planner_ = BuildPlanner();
        AssertInfo(planner_ != nullptr, "column planner is null");
    });
    return *planner_;
}

std::shared_ptr<const ColumnPlanner>
ChunkedColumnInterface::PlannerHandle() const {
    Planner();
    return planner_;
}

std::unique_ptr<ColumnPlanner>
ChunkedColumnInterface::BuildPlanner() const {
    return std::make_unique<ColumnPlanner>(GetNumRowsUntilChunk());
}

}  // namespace milvus
