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
#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/ScalarComparison.h"

namespace milvus::index::detail {

// Queries must be sorted and distinct, and indexed values must be ordered by
// the same comparison. Accessors let numeric entries and string dictionaries
// (including mmap dictionaries) share the cursor without copying index data.
template <typename Queries, typename ValueAt, typename Match, typename Continue>
void
VisitOrderedMatches(size_t size,
                    const Queries& queries,
                    ValueAt value_at,
                    Match match,
                    Continue continue_matching) {
    size_t cursor = 0;
    for (const auto value : queries) {
        if (cursor == size) {
            break;
        }
        if (ScalarLess(value_at(cursor), value)) {
            const size_t remaining = size - cursor;
            size_t lo = 1;
            size_t hi = 1;
            while (hi < remaining && ScalarLess(value_at(cursor + hi), value)) {
                lo = hi + 1;
                hi = hi > remaining / 2 ? remaining : hi * 2;
            }
            // Lower bound inside the galloped bracket. Never subtract values:
            // signed extremes and infinities are ordinary comparison operands.
            while (lo < hi) {
                const size_t mid = lo + (hi - lo) / 2;
                if (ScalarLess(value_at(cursor + mid), value)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            cursor += lo;
        }
        while (cursor != size && continue_matching(value, value_at(cursor))) {
            match(cursor, value);
            ++cursor;
        }
    }
}

template <typename Queries, typename ValueAt, typename Match>
void
VisitOrderedMatches(size_t size,
                    const Queries& queries,
                    ValueAt value_at,
                    Match match) {
    VisitOrderedMatches(size,
                        queries,
                        value_at,
                        match,
                        [](const auto& query, const auto& indexed) {
                            return ScalarEqual(indexed, query);
                        });
}

// Visit matching original row offsets. Callers initialize IN to false and
// NOT IN to validity. Neither the query buffer nor heap/mmap entries change.
template <typename Iterator,
          typename Value,
          typename Visitor,
          typename Validator>
void
VisitSortedMatches(Iterator first,
                   Iterator last,
                   size_t n,
                   const Value* values,
                   Visitor visit,
                   Validator validate) {
    if (n == 0 || first == last) {
        return;
    }
    const auto value_at = [&](size_t i) { return first[i].a_; };
    const auto match = [&](size_t i, Value value) {
        validate(value, first[i]);
        visit(first[i].idx_);
    };
    const auto upper_bound_match = [](const auto& query, const auto& indexed) {
        return !ScalarLess(query, indexed);
    };
    const size_t size = static_cast<size_t>(last - first);
    if constexpr (std::is_same_v<Value, bool>) {
        std::array<bool, 2> present{false, false};
        for (size_t i = 0; i < n; ++i) {
            present[values[i]] = true;
            if (present[0] && present[1]) {
                break;
            }
        }
        // No query allocation or sorting is needed for a two-value domain.
        if (present[0] && present[1]) {
            VisitOrderedMatches(size,
                                std::array<bool, 2>{false, true},
                                value_at,
                                match,
                                upper_bound_match);
        } else {
            VisitOrderedMatches(size,
                                std::array<bool, 1>{present[1]},
                                value_at,
                                match,
                                upper_bound_match);
        }
    } else {
        std::vector<Value> queries(values, values + n);
        std::sort(queries.begin(), queries.end(), ScalarLessThan<Value>{});
        queries.erase(
            std::unique(queries.begin(), queries.end(), ScalarEqualTo<Value>{}),
            queries.end());
        // Use upper-bound semantics so malformed ranges still reach validate.
        VisitOrderedMatches(size, queries, value_at, match, upper_bound_match);
    }
}

template <typename Iterator, typename Value, typename Visitor>
void
VisitSortedMatches(Iterator first,
                   Iterator last,
                   size_t n,
                   const Value* values,
                   Visitor visit) {
    VisitSortedMatches(
        first, last, n, values, visit, [](const auto&, const auto&) {});
}

// Borrow only string views for the duration of the synchronous query. Sorting
// and deduplication copy O(N) views, not the variable-length character buffers.
template <typename ValueAt, typename Match>
void
VisitSortedStringMatches(size_t size,
                         size_t n,
                         const std::string* values,
                         ValueAt value_at,
                         Match match) {
    if (n == 0 || size == 0) {
        return;
    }
    std::vector<std::string_view> queries;
    queries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        queries.emplace_back(values[i]);
    }
    std::sort(queries.begin(), queries.end());
    queries.erase(std::unique(queries.begin(), queries.end()), queries.end());
    VisitOrderedMatches(
        size, queries, value_at, [&](size_t i, auto) { match(i); });
}

}  // namespace milvus::index::detail
