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
#include <cmath>
#include <cstddef>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace milvus::index::detail {

inline constexpr size_t kMaxBinaryMembershipSize = 8;

// Keep the original binary-search lookup for short lists and exceptional
// floating-point queries. Matching ranges still reach the diagnostic callback.
template <typename Iterator,
          typename Value,
          typename Visitor,
          typename Validator>
void
VisitBinaryMatches(Iterator first,
                   Iterator last,
                   size_t n,
                   const Value* values,
                   Visitor visit,
                   Validator validate) {
    if (n == 0 || first == last) {
        return;
    }
    for (size_t i = 0; i < n; ++i) {
        const Value value = values[i];
        auto lb = std::lower_bound(
            first, last, value, [](const auto& entry, Value value) {
                return entry.a_ < value;
            });
        auto ub = std::upper_bound(
            lb, last, value, [](Value value, const auto& entry) {
                return value < entry.a_;
            });
        for (; lb != ub; ++lb) {
            validate(value, *lb);
            visit(lb->idx_);
        }
    }
}

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
        if (value_at(cursor) < value) {
            const size_t remaining = size - cursor;
            size_t lo = 1;
            size_t hi = 1;
            while (hi < remaining && value_at(cursor + hi) < value) {
                lo = hi + 1;
                hi = hi > remaining / 2 ? remaining : hi * 2;
            }
            // Lower bound inside the galloped bracket. Never subtract values:
            // signed extremes and infinities are ordinary comparison operands.
            while (lo < hi) {
                const size_t mid = lo + (hi - lo) / 2;
                if (value_at(cursor + mid) < value) {
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
                            return indexed == query;
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
    if constexpr (std::is_same_v<Value, bool>) {
        std::array<bool, 2> present{false, false};
        for (size_t i = 0; i < n; ++i) {
            present[values[i]] = true;
            if (present[0] && present[1]) {
                break;
            }
        }
        // The two-value domain always uses binary lookup without sorting.
        if (present[0] && present[1]) {
            const std::array<bool, 2> queries{false, true};
            VisitBinaryMatches(
                first, last, queries.size(), queries.data(), visit, validate);
        } else {
            const bool query = present[1];
            VisitBinaryMatches(first, last, 1, &query, visit, validate);
        }
    } else {
        if constexpr (std::is_floating_point_v<Value>) {
            // NaN is not a strict weak ordering operand. Preserve the original
            // lower/upper-bound behavior (and validator diagnostics) for these
            // exceptional queries instead of feeding NaNs into sort/unique.
            // Stored entries have the existing sorted-index precondition;
            // inserting NaNs is rejected by scalar input validation.
            if (std::any_of(values, values + n, [](Value value) {
                    return std::isnan(value);
                })) {
                VisitBinaryMatches(first, last, n, values, visit, validate);
                return;
            }
        }
        if (n <= kMaxBinaryMembershipSize) {
            // Deduplicate short lists on the stack without sorting or a cursor.
            std::array<Value, kMaxBinaryMembershipSize> queries;
            size_t count = 0;
            for (size_t i = 0; i < n; ++i) {
                const auto end = queries.begin() + count;
                if (std::find(queries.begin(), end, values[i]) == end) {
                    queries[count++] = values[i];
                }
            }
            VisitBinaryMatches(
                first, last, count, queries.data(), visit, validate);
            return;
        }
        std::vector<Value> queries(values, values + n);
        std::sort(queries.begin(), queries.end());
        queries.erase(std::unique(queries.begin(), queries.end()),
                      queries.end());
        if (queries.size() <= kMaxBinaryMembershipSize) {
            VisitBinaryMatches(
                first, last, queries.size(), queries.data(), visit, validate);
            return;
        }
        const auto value_at = [&](size_t i) { return first[i].a_; };
        const auto match = [&](size_t i, Value value) {
            validate(value, first[i]);
            visit(first[i].idx_);
        };
        const auto upper_bound_match = [](const auto& query,
                                          const auto& indexed) {
            return !(query < indexed);
        };
        const size_t size = static_cast<size_t>(last - first);
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

template <typename ValueAt, typename Match>
void
VisitBinaryStringMatches(size_t size,
                         size_t n,
                         const std::string_view* values,
                         ValueAt value_at,
                         Match match) {
    if (n == 0 || size == 0) {
        return;
    }
    for (size_t i = 0; i < n; ++i) {
        const auto value = values[i];
        size_t lo = 0, hi = size;
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (value_at(mid) < value) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo != size && value_at(lo) == value) {
            match(lo);
        }
    }
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
    if (n <= kMaxBinaryMembershipSize) {
        std::array<std::string_view, kMaxBinaryMembershipSize> queries;
        size_t count = 0;
        for (size_t i = 0; i < n; ++i) {
            const std::string_view value(values[i]);
            const auto end = queries.begin() + count;
            if (std::find(queries.begin(), end, value) == end) {
                queries[count++] = value;
            }
        }
        VisitBinaryStringMatches(size, count, queries.data(), value_at, match);
        return;
    }
    std::vector<std::string_view> queries;
    queries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        queries.emplace_back(values[i]);
    }
    std::sort(queries.begin(), queries.end());
    queries.erase(std::unique(queries.begin(), queries.end()), queries.end());
    if (queries.size() <= kMaxBinaryMembershipSize) {
        VisitBinaryStringMatches(
            size, queries.size(), queries.data(), value_at, match);
        return;
    }
    VisitOrderedMatches(
        size, queries, value_at, [&](size_t i, auto) { match(i); });
}

}  // namespace milvus::index::detail
