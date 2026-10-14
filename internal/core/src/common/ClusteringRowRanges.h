// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace milvus {
// Immutable per-request hints. Invalid hints always leave the original mask intact.
struct ClusteringRowRanges {
    int64_t expected_rows;
    std::vector<std::pair<int64_t, int64_t>>
        ranges;  // [offset, offset + count)

    template <typename Bitmap>
    bool
    Apply(Bitmap& mask, int64_t rows) const {
        if (rows <= 0 || expected_rows != rows || mask.size() != rows)
            return false;
        int64_t end = 0;
        for (const auto& [offset, count] : ranges) {
            if (offset < end || offset < 0 || count <= 0 || offset > rows ||
                count > rows - offset)
                return false;
            end = offset + count;
        }
        int64_t cursor = 0;
        for (const auto& [offset, count] : ranges) {
            if (offset > cursor)
                mask.set(cursor, offset - cursor, true);
            cursor = offset + count;
        }
        if (cursor < rows)
            mask.set(cursor, rows - cursor, true);
        return true;
    }
};
using ClusteringRowRangesPtr = std::shared_ptr<const ClusteringRowRanges>;
}  // namespace milvus
