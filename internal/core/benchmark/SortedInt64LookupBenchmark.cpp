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

// Standalone component benchmark: see the IX01 design doc for build/run notes.
#include <algorithm>
#include <ctime>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/detail/element_wise.h"
#include "index/IndexStructure.h"
#include "index/SortedMembership.h"

namespace {

using Entry = milvus::index::IndexStructure<int64_t>;
// Same bitset implementation and per-bit writes as TargetBitmap, with a
// scalar policy/std::vector so this executable needs no Folly/SIMD runtime.
using Bitmap = milvus::bitset::Bitset<
    milvus::bitset::detail::ElementWiseBitsetPolicy<uint64_t>,
    std::vector<uint8_t>,
    true>;

Bitmap
Lookup(const std::vector<Entry>& entries,
       const Bitmap& valid,
       const std::vector<int64_t>& queries,
       bool not_in,
       bool batch) {
    auto result = not_in ? valid.clone() : Bitmap(valid.size(), false);
    auto visit = [&](int32_t row) { result[row] = !not_in; };
    if (batch) {
        milvus::index::detail::VisitSortedMatches(entries.begin(),
                                                  entries.end(),
                                                  queries.size(),
                                                  queries.data(),
                                                  visit);
    } else if (!entries.empty()) {
        // The original full-range lower_bound/upper_bound loop, including
        // repeated writes for duplicate query values.
        for (const auto value : queries) {
            const Entry target(value);
            auto lb = std::lower_bound(entries.begin(), entries.end(), target);
            auto ub = std::upper_bound(lb, entries.end(), target);
            for (; lb != ub; ++lb) {
                visit(lb->idx_);
            }
        }
    }
    return result;
}

void
Verify(const std::vector<int64_t>& rows,
       const Bitmap& valid,
       const std::vector<Entry>& entries,
       const std::vector<int64_t>& queries) {
    const auto original = queries;
    const std::unordered_set<int64_t> terms(queries.begin(), queries.end());
    for (bool not_in : {false, true}) {
        auto baseline = Lookup(entries, valid, queries, not_in, false);
        auto batch = Lookup(entries, valid, queries, not_in, true);
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool hit = terms.count(rows[row]) != 0;
            const bool expected = valid[row] && (not_in ? !hit : hit);
            if (baseline[row] != expected || batch[row] != expected) {
                std::cerr << "membership mismatch at row " << row << '\n';
                std::exit(1);
            }
        }
    }
    if (queries != original) {
        std::cerr << "query input was modified\n";
        std::exit(1);
    }
}

void
CheckEdgeCases() {
    std::mt19937_64 rng(53853);
    for (size_t row_count : {1, 2, 63, 64, 65, 127, 128, 129, 257, 4096}) {
        for (int null_mode : {0, 1, 2}) {
            std::vector<int64_t> rows(row_count);
            std::vector<Entry> entries;
            Bitmap valid(row_count, false);
            for (size_t i = 0; i < row_count; ++i) {
                rows[i] = static_cast<int64_t>(rng() % 2001) - 1000;
                if (i % 13 == 0) {
                    rows[i] = std::numeric_limits<int64_t>::min();
                } else if (i % 13 == 1) {
                    rows[i] = std::numeric_limits<int64_t>::max();
                }
                if (null_mode == 0 || (null_mode == 1 && i % 3 != 0)) {
                    valid[i] = true;
                    entries.emplace_back(rows[i], i);
                }
            }
            std::sort(entries.begin(), entries.end());
            for (size_t n : {0, 1, 8, 63, 64, 127, 128, 129, 1024, 8192}) {
                for (int shape : {0, 1, 2, 3}) {
                    std::vector<int64_t> queries(n);
                    for (size_t i = 0; i < n; ++i) {
                        if (shape == 0) {
                            queries[i] = rows[rng() % row_count];
                        } else if (shape == 1) {
                            queries[i] = 2000;
                        } else if (shape == 2) {
                            queries[i] = -2000;
                        } else {
                            queries[i] =
                                static_cast<int64_t>(rng() % 4001) - 2000;
                        }
                    }
                    Verify(rows, valid, entries, queries);
                }
            }
        }
    }
    // A reloaded all-NULL index can have null data pointers. Do not subtract
    // or dereference them, even with a large query list.
    const Entry* empty = nullptr;
    std::vector<int64_t> queries(1024, 42);
    milvus::index::detail::VisitSortedMatches(
        empty, empty, queries.size(), queries.data(), [](int32_t) {
            std::abort();
        });
    milvus::index::detail::VisitSortedMatches(
        empty, empty, 0, static_cast<const int64_t*>(nullptr), [](int32_t) {
            std::abort();
        });
}

void
CheckValidationCallbacks() {
    std::vector<Entry> entries;
    for (int32_t i = 0; i < 256; ++i) {
        entries.emplace_back(i, i);
    }

    size_t validations = 0;
    auto validate = [&](const int64_t expected, const Entry& entry) {
        if (entry.a_ != expected) {
            std::cerr << "validation callback received a mismatched entry\n";
            std::exit(1);
        }
        ++validations;
    };
    auto visit = [](int32_t) {};

    const std::vector<int64_t> small_query{17};
    milvus::index::detail::VisitSortedMatches(entries.begin(),
                                              entries.end(),
                                              small_query.size(),
                                              small_query.data(),
                                              visit,
                                              validate);
    if (validations != 1) {
        std::cerr << "single-term validation callback count mismatch\n";
        std::exit(1);
    }

    std::vector<int64_t> batch_query(128);
    std::iota(batch_query.begin(), batch_query.end(), 0);
    validations = 0;
    milvus::index::detail::VisitSortedMatches(entries.begin(),
                                              entries.end(),
                                              batch_query.size(),
                                              batch_query.data(),
                                              visit,
                                              validate);
    if (validations != batch_query.size()) {
        std::cerr << "batch validation callback count mismatch\n";
        std::exit(1);
    }
}

volatile size_t checksum = 0;

template <typename Function>
double
Time(Function fn) {
    checksum = fn().count();  // Warm each path before timing.
    const auto start = std::clock();
    size_t iterations = 0;
    double elapsed;
    do {
        for (int batch = 0; batch < 16; ++batch) {
            checksum = fn().count();
            ++iterations;
        }
        elapsed = 1e6 * (std::clock() - start) / CLOCKS_PER_SEC;
    } while (elapsed < 5000);
    return elapsed / iterations;
}

void
Sweep(bool small_index) {
    std::mt19937_64 rng(53853);
    std::cout << "rows,cardinality,list_size,repeat_factor,hit_percent,"
                 "unique_terms,selectivity,operation,baseline_cpu_us,"
                 "batch_cpu_us,"
                 "speedup\n";
    const std::vector<size_t> row_counts =
        small_index ? std::vector<size_t>{1, 16, 128, 4096}
                    : std::vector<size_t>{4096, 262144};
    const std::vector<size_t> list_sizes =
        small_index ? std::vector<size_t>{128, 4096, 65536}
                    : std::vector<size_t>{1, 8, 64, 127, 128, 129, 512, 4096};
    for (size_t row_count : row_counts) {
        const auto cardinalities =
            small_index ? std::vector<size_t>{row_count}
                        : std::vector<size_t>{8, 1024, row_count};
        for (size_t cardinality : cardinalities) {
            std::vector<int64_t> rows(row_count);
            for (size_t i = 0; i < row_count; ++i) {
                rows[i] = 2 * static_cast<int64_t>(i % cardinality);
            }
            std::shuffle(rows.begin(), rows.end(), rng);
            Bitmap valid(row_count, false);
            std::vector<Entry> entries;
            for (size_t i = 0; i < row_count; ++i) {
                if ((small_index && i == 0) || i % 7 != 0) {
                    valid[i] = true;
                    entries.emplace_back(rows[i], i);
                }
            }
            std::sort(entries.begin(), entries.end());
            for (size_t n : list_sizes) {
                for (size_t repeat : {1, 8}) {
                    for (size_t hit_percent : {0, 10, 100}) {
                        std::vector<int64_t> queries(n);
                        for (size_t i = 0; i < n; ++i) {
                            const auto j = i / repeat;
                            const auto key = (j * 104729) % cardinality;
                            const auto unique_count = (n + repeat - 1) / repeat;
                            const auto hit_count =
                                unique_count * hit_percent / 100;
                            queries[i] = j < hit_count
                                             ? 2 * static_cast<int64_t>(key)
                                             : 2 * static_cast<int64_t>(j) + 1;
                        }
                        std::shuffle(queries.begin(), queries.end(), rng);
                        Verify(rows, valid, entries, queries);
                        const std::unordered_set<int64_t> unique(
                            queries.begin(), queries.end());
                        const auto hits =
                            Lookup(entries, valid, queries, false, true)
                                .count();
                        for (bool not_in : {false, true}) {
                            std::vector<double> old_times, new_times;
                            for (int trial = 0; trial < 5; ++trial) {
                                auto old_fn = [&] {
                                    return Lookup(
                                        entries, valid, queries, not_in, false);
                                };
                                auto new_fn = [&] {
                                    return Lookup(
                                        entries, valid, queries, not_in, true);
                                };
                                if (trial % 2 == 0) {
                                    old_times.push_back(Time(old_fn));
                                    new_times.push_back(Time(new_fn));
                                } else {
                                    new_times.push_back(Time(new_fn));
                                    old_times.push_back(Time(old_fn));
                                }
                            }
                            std::sort(old_times.begin(), old_times.end());
                            std::sort(new_times.begin(), new_times.end());
                            const auto selectivity =
                                static_cast<double>(hits) / valid.count();
                            std::cout
                                << row_count << ',' << cardinality << ',' << n
                                << ',' << repeat << ',' << hit_percent << ','
                                << unique.size() << ',' << selectivity << ','
                                << (not_in ? "NOT_IN" : "IN") << ','
                                << old_times[2] << ',' << new_times[2] << ','
                                << old_times[2] / new_times[2] << '\n';
                        }
                    }
                }
            }
        }
    }
}

}  // namespace

int
main(int argc, char** argv) {
    const auto mode =
        argc == 2 ? std::string_view(argv[1]) : std::string_view{};
    if (argc > 2 || (argc == 2 && mode != "--verify-only" &&
                     mode != "--small-index-only")) {
        std::cerr << "Usage: sorted_int64_lookup_benchmark "
                     "[--verify-only|--small-index-only]\n";
        return 1;
    }
    CheckEdgeCases();
    CheckValidationCallbacks();
    std::cerr << "2400 scan/baseline comparisons passed for IN and NOT IN\n";
    if (mode != "--verify-only") {
        std::cout << std::setprecision(6);
        Sweep(mode == "--small-index-only");
    }
}
