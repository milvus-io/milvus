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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <vector>

namespace milvus::index::detail {

// Below this size, avoid allocating and sorting a query copy.
inline constexpr size_t kSortedInt64BatchThreshold = 128;

// Visit matching row offsets in a sorted range of {a_, idx_} entries. The
// caller owns bitmap initialization (all false for IN, validity for NOT IN).
// Iterators may refer to heap or mmap storage; neither input is modified.
template <typename Iterator, typename Visitor>
void
VisitSortedInt64Matches(Iterator first,
                        Iterator last,
                        size_t n,
                        const int64_t* values,
                        Visitor visit) {
    if (n == 0 || first == last) {
        return;
    }
    const auto less = [](const auto& entry, int64_t value) {
        return entry.a_ < value;
    };
    // Sorting N terms is O(N log N); probing M indexed entries is O(N log M).
    // When the query outgrows the index, keep the allocation-free probe path.
    // Use the actual entry range, not the row count (which includes NULLs).
    if (n < kSortedInt64BatchThreshold ||
        n > static_cast<size_t>(last - first)) {
        for (size_t i = 0; i < n; ++i) {
            auto lb = std::lower_bound(first, last, values[i], less);
            auto ub = std::upper_bound(
                lb, last, values[i], [](int64_t value, const auto& entry) {
                    return value < entry.a_;
                });
            for (; lb != ub; ++lb) {
                visit(lb->idx_);
            }
        }
        return;
    }

    std::vector<int64_t> queries(values, values + n);
    std::sort(queries.begin(), queries.end());
    queries.erase(std::unique(queries.begin(), queries.end()), queries.end());

    auto cursor = first;
    for (const auto value : queries) {
        if (cursor == last) {
            break;
        }
        if (cursor->a_ < value) {
            // Gallop over the gap, then binary-search only that bracket.
            // Bound the doubling before addition to avoid overflow, including
            // for a target beyond the final entry. Never subtract values:
            // INT64_MIN and INT64_MAX are ordinary comparison operands.
            using Distance =
                typename std::iterator_traits<Iterator>::difference_type;
            const Distance remaining = last - cursor;
            Distance lo = 1;
            Distance hi = 1;
            while (hi < remaining && cursor[hi].a_ < value) {
                lo = hi + 1;
                hi = hi > remaining / 2 ? remaining : hi * 2;
            }
            cursor = std::lower_bound(cursor + lo, cursor + hi, value, less);
        }
        // Each matching entry must be visited anyway. Walking its equal run
        // avoids a second search, and the next query starts after that run.
        while (cursor != last && cursor->a_ == value) {
            visit(cursor->idx_);
            ++cursor;
        }
    }
}

}  // namespace milvus::index::detail
