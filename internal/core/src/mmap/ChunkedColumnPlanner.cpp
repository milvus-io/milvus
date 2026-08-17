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

ColumnPlanner::ColumnPlanner(std::vector<int64_t> num_rows_until_cell)
    : num_rows_until_cell_(std::move(num_rows_until_cell)) {
    AssertInfo(num_rows_until_cell_.size() >= 2,
               "column planner requires at least one Cell boundary");
    AssertInfo(num_rows_until_cell_.front() == 0,
               "column planner first Cell boundary is {}, expected 0",
               num_rows_until_cell_.front());
    for (size_t i = 1; i < num_rows_until_cell_.size(); ++i) {
        AssertInfo(num_rows_until_cell_[i] >= num_rows_until_cell_[i - 1],
                   "column planner Cell boundaries are not monotonic at {}: "
                   "{} < {}",
                   i,
                   num_rows_until_cell_[i],
                   num_rows_until_cell_[i - 1]);
    }
}

CellLocation
ColumnPlanner::Locate(int64_t segment_offset) const {
    AssertInfo(segment_offset >= 0 && segment_offset < NumRows(),
               "segment offset {} out of planner rows {}",
               segment_offset,
               NumRows());
    const auto it = std::upper_bound(num_rows_until_cell_.begin(),
                                     num_rows_until_cell_.end(),
                                     segment_offset);
    const auto cell_id =
        static_cast<int64_t>(std::distance(num_rows_until_cell_.begin(), it)) -
        1;
    return CellLocation{cell_id,
                        segment_offset - num_rows_until_cell_[cell_id]};
}

int64_t
ColumnPlanner::CellStart(int64_t cell_id) const {
    AssertInfo(cell_id >= 0 && cell_id <= NumCells(),
               "planner Cell boundary {} out of range {}",
               cell_id,
               NumCells());
    return num_rows_until_cell_[cell_id];
}

int64_t
ColumnPlanner::CellRows(int64_t cell_id) const {
    return CellStart(cell_id + 1) - CellStart(cell_id);
}

int64_t
ColumnPlanner::NumCells() const {
    return static_cast<int64_t>(num_rows_until_cell_.size()) - 1;
}

int64_t
ColumnPlanner::NumRows() const {
    return num_rows_until_cell_.back();
}

const std::vector<int64_t>&
ColumnPlanner::CellBoundaries() const {
    return num_rows_until_cell_;
}

bool
ColumnPlanner::ShouldSkipCell(int64_t cell_id,
                              const CellSkipPredicate& predicate) const {
    AssertInfo(cell_id >= 0 && cell_id < NumCells(),
               "planner Cell {} out of range {}",
               cell_id,
               NumCells());
    return predicate && predicate(cell_id);
}

TakePlan
ColumnPlanner::PlanTake(const OffsetView& offsets) const {
    AssertInfo(offsets.size >= 0,
               "take offset count must be non-negative, got {}",
               offsets.size);
    if (offsets.size > 0) {
        AssertInfo(offsets.data != nullptr,
                   "take offsets are null with count {}",
                   offsets.size);
    }

    TakePlan plan;
    plan.locations.reserve(offsets.size);
    for (int64_t i = 0; i < offsets.size; ++i) {
        const auto location = Locate(offsets[i]);
        plan.locations.emplace_back(TakeLocation{
            location.cell_id, static_cast<size_t>(location.cell_offset)});
    }
    return plan;
}

ScanPlan
ColumnPlanner::PlanScan(int64_t row_start,
                        int64_t row_count,
                        const CellSkipPredicate& preloaded_skip) const {
    AssertInfo(row_start >= 0 && row_start <= NumRows() && row_count >= 0 &&
                   row_count <= NumRows() - row_start,
               "scan planning range [{}, {}) out of rows {}",
               row_start,
               row_start + row_count,
               NumRows());

    ScanPlan plan;
    const auto row_end = row_start + row_count;
    if (row_start == row_end) {
        return plan;
    }
    const auto first = Locate(row_start);
    const auto last = Locate(row_end - 1);
    plan.cells.reserve(last.cell_id - first.cell_id + 1);

    auto position = row_start;
    while (position < row_end) {
        const auto location = Locate(position);
        const auto cell_end = CellStart(location.cell_id + 1);
        const auto range_end = std::min(row_end, cell_end);
        plan.cells.emplace_back(
            PlannedCellRange{location.cell_id,
                             position,
                             range_end - position,
                             ShouldSkipCell(location.cell_id, preloaded_skip)});
        position = range_end;
    }
    return plan;
}

const ColumnPlanner&
ChunkedColumnInterface::Planner() const {
    std::call_once(planner_once_, [this]() {
        planner_ = BuildPlanner();
        AssertInfo(planner_ != nullptr, "column planner is null");
    });
    return *planner_;
}

std::unique_ptr<ColumnPlanner>
ChunkedColumnInterface::BuildPlanner() const {
    return std::make_unique<ColumnPlanner>(GetNumRowsUntilChunk());
}

}  // namespace milvus
