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
#include "mmap/ColumnOffsetLookup.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace milvus {

ColumnPlanner::ColumnPlanner(const std::vector<int64_t>& num_rows_until_cell,
                             OffsetLookupOptions lookup_options)
    : num_rows_until_cell_(&num_rows_until_cell),
      lookup_options_(lookup_options) {
    ValidateBoundaries();
}

ColumnPlanner::ColumnPlanner(std::vector<int64_t>&& num_rows_until_cell,
                             OffsetLookupOptions lookup_options)
    : owned_num_rows_until_cell_(std::make_shared<const std::vector<int64_t>>(
          std::move(num_rows_until_cell))),
      num_rows_until_cell_(owned_num_rows_until_cell_.get()),
      lookup_options_(lookup_options) {
    ValidateBoundaries();
}

void
ColumnPlanner::ValidateBoundaries() const {
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
    if (lookup_options_.strategy == OffsetLookupStrategy::Bucket) {
        AssertInfo(lookup_options_.bucket_cells != nullptr &&
                       lookup_options_.bucket_order >= 0 &&
                       lookup_options_.bucket_order < 63,
                   "column planner bucket index is invalid");
        AssertInfo(NumCells() <= 1 || NumRows() == 0 ||
                       lookup_options_.bucket_cells->size() >
                           ((NumRows() - 1) >> lookup_options_.bucket_order),
                   "column planner bucket index does not cover all rows");
    } else {
        AssertInfo(
            lookup_options_.strategy == OffsetLookupStrategy::PerOffset ||
                lookup_options_.strategy == OffsetLookupStrategy::BatchBinary,
            "column planner lookup strategy is invalid");
    }
}

CellLocation
ColumnPlanner::Locate(int64_t segment_offset) const {
    AssertInfo(segment_offset >= 0 && segment_offset < NumRows(),
               "segment offset {} out of planner rows {}",
               segment_offset,
               NumRows());
    const auto& boundaries = *num_rows_until_cell_;
    if (boundaries.size() == 2) {
        return CellLocation{0, segment_offset};
    }
    const auto it =
        std::upper_bound(boundaries.begin(), boundaries.end(), segment_offset);
    const auto cell_id =
        static_cast<int64_t>(std::distance(boundaries.begin(), it)) - 1;
    return CellLocation{cell_id, segment_offset - boundaries[cell_id]};
}

std::pair<std::vector<int64_t>, std::vector<int64_t>>
ColumnPlanner::LocateBatch(OffsetView offsets) const {
    AssertInfo(
        offsets.size >= 0 && (offsets.size == 0 || offsets.data != nullptr),
        "column planner batch offsets are invalid");
    const auto locate = [this, count = offsets.size](const auto* input) {
        if (lookup_options_.strategy == OffsetLookupStrategy::PerOffset) {
            std::pair<std::vector<int64_t>, std::vector<int64_t>> result{
                std::vector<int64_t>(count), std::vector<int64_t>(count)};
            for (int64_t i = 0; i < count; ++i) {
                const auto location = Locate(input[i]);
                result.first[i] = location.cell_id;
                result.second[i] = location.cell_offset;
            }
            return result;
        }
        const auto rows = NumRows();
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(input[i] >= 0 && input[i] < rows,
                       "segment offset {} out of planner rows {}",
                       input[i],
                       rows);
        }
        const auto& boundaries = *num_rows_until_cell_;
        if (lookup_options_.strategy == OffsetLookupStrategy::Bucket) {
            return detail::LocateOffsetsByBuckets(
                input,
                count,
                boundaries,
                lookup_options_.bucket_order,
                *lookup_options_.bucket_cells);
        }
        return detail::LocateOffsetsByBinary(input, count, boundaries);
    };
    return offsets.element_type == OffsetElementType::Int32
               ? locate(static_cast<const int32_t*>(offsets.data))
               : locate(static_cast<const int64_t*>(offsets.data));
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
