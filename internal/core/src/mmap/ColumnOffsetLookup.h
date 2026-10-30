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
#include <utility>
#include <vector>

namespace milvus::detail {

// Shared by legacy Raw reads and Planner; callers validate offsets first.
template <typename Offset>
std::pair<std::vector<int64_t>, std::vector<int64_t>>
LocateOffsetsByBinary(const Offset* offsets,
                      int64_t count,
                      const std::vector<int64_t>& boundaries) {
    std::vector<int64_t> cells(count, 1);
    std::vector<int64_t> cell_offsets(count);
    int64_t len = boundaries.size() - 1;
    while (len > 1) {
        const int64_t half = len / 2;
        len -= half;
        for (int64_t i = 0; i < count; ++i) {
            const bool cmp = boundaries[cells[i] + half - 1] <
                             static_cast<int64_t>(offsets[i]) + 1;
            cells[i] += static_cast<int64_t>(cmp) * half;
        }
    }
    for (int64_t i = 0; i < count; ++i) {
        cell_offsets[i] = offsets[i] - boundaries[--cells[i]];
    }
    return {std::move(cells), std::move(cell_offsets)};
}

template <typename Offset>
std::pair<std::vector<int64_t>, std::vector<int64_t>>
LocateOffsetsByBuckets(const Offset* offsets,
                       int64_t count,
                       const std::vector<int64_t>& boundaries,
                       int64_t bucket_order,
                       const std::vector<int64_t>& bucket_cells) {
    std::vector<int64_t> cells(count, 0);
    std::vector<int64_t> cell_offsets(count);
    if (boundaries.size() == 2) {
        for (int64_t i = 0; i < count; ++i) {
            cell_offsets[i] = offsets[i];
        }
        return {std::move(cells), std::move(cell_offsets)};
    }
    for (int64_t i = 0; i < count; ++i) {
        const int64_t offset = offsets[i];
        auto cell = bucket_cells[offset >> bucket_order];
        while (cell < boundaries.size() - 1 && offset >= boundaries[cell + 1]) {
            ++cell;
        }
        cells[i] = cell;
        cell_offsets[i] = offset - boundaries[cell];
    }
    return {std::move(cells), std::move(cell_offsets)};
}

}  // namespace milvus::detail
