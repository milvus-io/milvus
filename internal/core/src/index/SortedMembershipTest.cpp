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

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "index/IndexStructure.h"
#include "index/SortedMembership.h"

namespace milvus::index {
namespace {

template <typename T>
class SortedMembershipTest : public testing::Test {};
using SortedMembershipTypes =
    testing::Types<int8_t, int16_t, int32_t, int64_t, bool, float, double>;
TYPED_TEST_SUITE(SortedMembershipTest, SortedMembershipTypes);

TYPED_TEST(SortedMembershipTest, RandomizedScanOracle) {
    using T = TypeParam;
    using Entry = IndexStructure<T>;
    std::mt19937_64 rng(53853);
    for (size_t row_count : {0, 1, 2, 63, 64, 65, 127, 128, 129, 1024}) {
        for (int null_mode : {0, 1, 2}) {
            std::vector<Entry> entries;
            std::vector<T> rows(row_count);
            std::vector<bool> valid(row_count);
            for (size_t i = 0; i < row_count; ++i) {
                if constexpr (std::is_same_v<T, bool>) {
                    rows[i] = rng() % 2;
                } else {
                    rows[i] =
                        static_cast<T>(static_cast<int>(rng() % 101) - 50);
                    if (i % 19 == 0)
                        rows[i] = std::numeric_limits<T>::lowest();
                    if (i % 19 == 1)
                        rows[i] = std::numeric_limits<T>::max();
                    if constexpr (std::is_floating_point_v<T>) {
                        if (i % 19 == 2)
                            rows[i] = -std::numeric_limits<T>::infinity();
                        if (i % 19 == 3)
                            rows[i] = std::numeric_limits<T>::infinity();
                        if (i % 19 == 4)
                            rows[i] = T(-0.0);
                        if (i % 19 == 5)
                            rows[i] = T(0.0);
                        if (i % 19 == 6)
                            rows[i] = T(1.25);
                        if (i % 19 == 7)
                            rows[i] = std::nextafter(T(1.25), T(2));
                        if (i % 19 == 8)
                            rows[i] = -std::numeric_limits<T>::denorm_min();
                    }
                }
                valid[i] = null_mode == 0 || (null_mode == 1 && i % 3 != 0);
                if (valid[i])
                    entries.emplace_back(rows[i], i);
            }
            std::sort(entries.begin(), entries.end());
            for (size_t n : {0, 1, 2, 127, 128, 129, 2048}) {
                SCOPED_TRACE(testing::Message()
                             << "rows=" << row_count
                             << " null_mode=" << null_mode << " terms=" << n);
                auto queries = std::make_unique<T[]>(n);
                for (size_t i = 0; i < n; ++i) {
                    if (row_count && i % 2 == 0)
                        queries[i] = rows[rng() % row_count];
                    else if constexpr (std::is_same_v<T, bool>)
                        queries[i] = rng() % 2;
                    else
                        queries[i] =
                            static_cast<T>(static_cast<int>(rng() % 121) - 60);
                }
                for (int order : {0, 1, 2}) {
                    if (n && order == 1)
                        std::sort(queries.get(), queries.get() + n);
                    if (n && order == 2)
                        std::reverse(queries.get(), queries.get() + n);
                    std::vector<unsigned char> original(n * sizeof(T));
                    if (n)
                        std::memcpy(
                            original.data(), queries.get(), original.size());
                    std::vector<int> visits(row_count, 0);
                    std::vector<bool> in(row_count, false), not_in = valid;
                    size_t validations = 0;
                    // Null pointers model a loaded all-NULL index.
                    const Entry* first =
                        entries.empty() ? nullptr : entries.data();
                    const Entry* last =
                        entries.empty() ? nullptr : first + entries.size();
                    detail::VisitSortedMatches(
                        first,
                        last,
                        n,
                        n ? queries.get() : nullptr,
                        [&](int32_t row) {
                            ++visits[row];
                            in[row] = true;
                            not_in[row] = false;
                        },
                        [&](T value, const Entry& entry) {
                            EXPECT_EQ(entry.a_, value);
                            ++validations;
                        });
                    size_t hits = 0;
                    for (size_t row = 0; row < row_count; ++row) {
                        bool hit = false;
                        for (size_t i = 0; i < n; ++i)
                            hit |= rows[row] == queries[i];
                        EXPECT_EQ(in[row], valid[row] && hit);
                        EXPECT_EQ(not_in[row], valid[row] && !hit);
                        EXPECT_EQ(visits[row], valid[row] && hit ? 1 : 0);
                        hits += valid[row] && hit;
                    }
                    EXPECT_EQ(validations, hits);
                    if (n)
                        EXPECT_EQ(std::memcmp(original.data(),
                                              queries.get(),
                                              original.size()),
                                  0);
                }
            }
        }
    }
}

template <typename T>
struct ObservedValue {
    T value;
    size_t* reads;

    operator T() const {
        ++*reads;
        return value;
    }
};

template <typename T>
struct ObservedEntry {
    ObservedValue<T> a_;
    int32_t idx_;
};

TYPED_TEST(SortedMembershipTest, FewDistinctTermsUseBinaryLookup) {
    using T = TypeParam;
    size_t reads = 0;
    std::vector<ObservedEntry<T>> entries;
    for (int32_t group = 0; group < 9; ++group) {
        for (int32_t i = 0; i < 1024; ++i) {
            entries.push_back({{static_cast<T>(group), &reads},
                               static_cast<int32_t>(entries.size())});
        }
    }
    for (size_t n : {1, 2, 8, 9, 127, 128, 129}) {
        SCOPED_TRACE(testing::Message() << "terms=" << n);
        auto queries = std::make_unique<T[]>(n);
        for (size_t i = 0; i < n; ++i) {
            queries[i] = static_cast<T>(i % 8);
        }
        reads = 0;
        size_t validations = 0;
        std::vector<int> visits(entries.size());
        detail::VisitSortedMatches(
            entries.begin(),
            entries.end(),
            n,
            queries.get(),
            [&](int32_t row) { ++visits[row]; },
            [&](T value, const auto& entry) {
                EXPECT_EQ(entry.a_.value, value);
                ++validations;
            });
        size_t hits = 0;
        for (size_t row = 0; row < entries.size(); ++row) {
            const bool hit =
                std::find(queries.get(),
                          queries.get() + n,
                          entries[row].a_.value) != queries.get() + n;
            EXPECT_EQ(visits[row], hit ? 1 : 0);
            hits += hit;
        }
        EXPECT_EQ(validations, hits);
        // Ordering comparisons stay logarithmic even when thousands of matching
        // rows reach the callbacks. A cursor compares every matching value.
        EXPECT_LT(reads, 256);
    }
    if constexpr (!std::is_same_v<T, bool>) {
        const T queries[] = {
            T(8), T(7), T(6), T(5), T(4), T(3), T(2), T(1), T(0)};
        reads = 0;
        size_t visits = 0;
        detail::VisitSortedMatches(entries.begin(),
                                   entries.end(),
                                   std::size(queries),
                                   queries,
                                   [&](int32_t) { ++visits; });
        EXPECT_EQ(visits, entries.size());
        // More than eight distinct terms retain the ordered cursor.
        EXPECT_GE(reads, entries.size());
    }
}

template <typename T>
class SortedFloatingMembershipTest : public testing::Test {};
using FloatingMembershipTypes = testing::Types<float, double>;
TYPED_TEST_SUITE(SortedFloatingMembershipTest, FloatingMembershipTypes);

TYPED_TEST(SortedFloatingMembershipTest, NaNRetainsBinarySearchSemantics) {
    using T = TypeParam;
    using Entry = IndexStructure<T>;
    std::vector<Entry> entries;
    const T rows[] = {-std::numeric_limits<T>::infinity(),
                      T(-1),
                      T(-0.0),
                      T(0.0),
                      std::numeric_limits<T>::denorm_min(),
                      T(1),
                      std::numeric_limits<T>::infinity()};
    for (size_t i = 0; i < std::size(rows); ++i)
        entries.emplace_back(rows[i], i);
    const T nan = std::numeric_limits<T>::quiet_NaN();
    std::vector<std::vector<T>> query_lists{
        {nan},
        {nan, T(0), nan},
        {T(1), nan, T(-1)},
        {nan, -nan, std::numeric_limits<T>::infinity()}};
    for (size_t n : {8, 9, 127, 128, 129}) {
        std::vector<T> queries(n, T(1));
        queries[n / 2] = nan;
        query_lists.push_back(std::move(queries));
    }
    for (const auto& queries : query_lists) {
        auto original = queries;
        std::vector<int32_t> expected, actual;
        size_t validations = 0;
        // Independent copy of the pre-optimization lookup, including
        // repeated visits and the values passed to the diagnostic callback.
        for (T value : queries) {
            auto lb =
                std::lower_bound(entries.begin(), entries.end(), Entry(value));
            auto ub = std::upper_bound(lb, entries.end(), Entry(value));
            for (; lb != ub; ++lb) expected.push_back(lb->idx_);
        }
        detail::VisitSortedMatches(
            entries.begin(),
            entries.end(),
            queries.size(),
            queries.data(),
            [&](int32_t row) { actual.push_back(row); },
            [&](T value, const Entry& entry) {
                EXPECT_TRUE(std::isnan(value) || entry.a_ == value);
                ++validations;
            });
        EXPECT_EQ(actual, expected);
        EXPECT_EQ(validations, expected.size());
        EXPECT_EQ(
            std::memcmp(
                queries.data(), original.data(), queries.size() * sizeof(T)),
            0);
    }
}

TEST(SortedStringMembershipTest, BorrowedViewsAndScanOracle) {
    std::vector<std::string> dictionary{"",
                                        "a",
                                        "aa",
                                        "ab",
                                        std::string("a\0b", 3),
                                        "\x80",
                                        "\xff",
                                        "\xe4\xb8\xad\xe6\x96\x87",
                                        std::string(8192, 'x'),
                                        std::string(8192, 'x') + "a",
                                        std::string(8192, 'x') + "b"};
    std::mt19937_64 rng(53853);
    for (size_t i = 0; i < 257; ++i)
        dictionary.push_back("prefix/" + std::to_string(i));
    std::sort(dictionary.begin(), dictionary.end());
    const auto original_dictionary = dictionary;
    for (size_t size : {size_t{0}, size_t{1}, size_t{65}, dictionary.size()}) {
        for (size_t n : {0, 1, 127, 128, 129, 1024}) {
            std::vector<std::string> queries(n);
            for (size_t i = 0; i < n; ++i) {
                queries[i] =
                    i % 3 ? dictionary[rng() % dictionary.size()] : "missing";
            }
            for (int order : {0, 1, 2}) {
                if (order == 1)
                    std::sort(queries.begin(), queries.end());
                if (order == 2)
                    std::reverse(queries.begin(), queries.end());
                const auto original = queries;
                std::vector<int> visits(size);
                detail::VisitSortedStringMatches(
                    size,
                    n,
                    n ? queries.data() : nullptr,
                    [&](size_t i) { return std::string_view(dictionary[i]); },
                    [&](size_t i) { ++visits[i]; });
                for (size_t i = 0; i < size; ++i) {
                    const bool hit = std::find(queries.begin(),
                                               queries.end(),
                                               dictionary[i]) != queries.end();
                    EXPECT_EQ(visits[i], hit ? 1 : 0);
                }
                EXPECT_EQ(queries, original);
                EXPECT_EQ(dictionary, original_dictionary);
            }
        }
    }
}

TEST(SortedStringMembershipTest, FewDistinctTermsUseBinaryLookup) {
    std::vector<std::string> dictionary;
    for (size_t i = 0; i < 16384; ++i) {
        dictionary.push_back(std::to_string(i));
    }
    std::sort(dictionary.begin(), dictionary.end());
    for (size_t n : {1, 2, 8, 9, 127, 128, 129}) {
        SCOPED_TRACE(testing::Message() << "terms=" << n);
        std::vector<std::string> queries(n);
        for (size_t i = 0; i < n; ++i) {
            queries[i] = dictionary[dictionary.size() - 1 - i % 8];
        }
        const auto original = queries;
        size_t reads = 0;
        std::vector<int> visits(dictionary.size());
        detail::VisitSortedStringMatches(
            dictionary.size(),
            queries.size(),
            queries.data(),
            [&](size_t i) {
                ++reads;
                return std::string_view(dictionary[i]);
            },
            [&](size_t i) { ++visits[i]; });
        for (size_t i = 0; i < dictionary.size(); ++i) {
            const bool hit =
                std::find(queries.begin(), queries.end(), dictionary[i]) !=
                queries.end();
            EXPECT_EQ(visits[i], hit ? 1 : 0);
        }
        EXPECT_EQ(queries, original);
        // At most eight independent lower bounds, including equality checks.
        EXPECT_LE(reads, 128);
        if (n == 1) {
            // A single high-end term must not gallop over the dictionary.
            EXPECT_LE(reads, 16);
        }
    }
}

TEST(SortedMembershipTest, ValidatorSeesOutOfOrderEntriesInBoundRange) {
    using Entry = IndexStructure<int64_t>;
    std::vector<Entry> entries{{1, 0}, {3, 1}, {2, 2}, {4, 3}};
    // These malformed ranges reach diagnostics in the binary and ordered paths.
    for (const auto& queries : std::vector<std::vector<int64_t>>{
             {2}, {3, 5, 6, 7, 8, 9, 10, 11, 12}}) {
        std::vector<int32_t> visited;
        size_t mismatches = 0;
        detail::VisitSortedMatches(
            entries.begin(),
            entries.end(),
            queries.size(),
            queries.data(),
            [&](int32_t row) { visited.push_back(row); },
            [&](int64_t value, const Entry& entry) {
                mismatches += entry.a_ != value;
            });

        EXPECT_EQ(visited, (std::vector<int32_t>{1, 2}));
        EXPECT_EQ(mismatches, 1);
    }
}

}  // namespace
}  // namespace milvus::index
