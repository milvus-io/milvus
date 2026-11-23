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

// Standalone component benchmark for the Int32 bit-sliced index kernel
// (issue #53831). Reports build time, index size and query latency, and
// compares each query with the path Milvus uses for an unindexed int32 field,
// ANDed with the validity bitmap:
// - comparisons and ranges: a SIMD scan of the raw column
//   (TargetBitmap::inplace_compare_val / inplace_within_range_val, as in
//   UnaryExpr / BinaryRangeExpr);
// - IN / NOT IN: the dispatch of TermExpr::ExecVisitorImplForData, which runs
//   the SIMD batch filter (exec::simdFilterChunk, as SimdBatchElement) for
//   lists of at most simdLaneCount<int32_t>() * 8 keys and a per-row
//   ankerl::unordered_dense set probe (as SetElement) for longer lists. The
//   limit depends on the CPU, so the CSV says which one ran.
// Every query result is checked against that baseline before it is timed; a
// mismatch aborts the run. Index and scan are timed in interleaved trials,
// median of 7 each.
//
// Usage: bit_sliced_index_benchmark [--rows=N] [--quick]
//   default rows: 1048576 and 8388608; --quick: 65536 only.
// Output: CSV on stdout, one row per (dataset, query).

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "ankerl/unordered_dense.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "exec/expression/SimdFilter.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/bsi/BitSlicedIndex.h"

namespace {

using milvus::TargetBitmap;
using milvus::index::BitSlicedIndexInt32;
using milvus::index::CompareOp;
using milvus::index::ScalarBuildBatch;
using milvus::index::ScalarBuildInput;
using Clock = std::chrono::steady_clock;

volatile size_t sink = 0;

// Mean time per call over one trial of at least 30 ms.
template <typename F>
double
TrialMicros(F& fn) {
    const auto start = Clock::now();
    size_t calls = 0;
    double elapsed_us = 0;
    do {
        sink = sink + fn().size();
        ++calls;
        elapsed_us =
            std::chrono::duration<double, std::micro>(Clock::now() - start)
                .count();
    } while (elapsed_us < 30000);
    return elapsed_us / static_cast<double>(calls);
}

// Times the index and the scan for one query with interleaved trials
// (index, scan, index, scan, ...), so a transient slowdown of the machine
// lands on both sides rather than on one, and returns the median of 7 trials
// per side. Each side is warmed up with one call first.
template <typename A, typename B>
std::pair<double, double>
InterleavedMedianMicros(A index_fn, B scan_fn) {
    sink = sink + index_fn().size() + scan_fn().size();
    std::vector<double> index_us, scan_us;
    for (int t = 0; t < 7; ++t) {
        index_us.push_back(TrialMicros(index_fn));
        scan_us.push_back(TrialMicros(scan_fn));
    }
    std::sort(index_us.begin(), index_us.end());
    std::sort(scan_us.begin(), scan_us.end());
    return {index_us[index_us.size() / 2], scan_us[scan_us.size() / 2]};
}

struct Dataset {
    std::string name;
    std::vector<int32_t> values;
    std::unique_ptr<bool[]> valid;
    size_t rows = 0;
    double null_rate = 0;
};

Dataset
MakeDataset(const std::string& shape,
            size_t rows,
            double null_rate,
            uint64_t seed) {
    Dataset d;
    d.name = shape;
    d.rows = rows;
    d.null_rate = null_rate;
    d.values.resize(rows);
    d.valid = std::make_unique<bool[]>(rows);
    std::mt19937_64 rng(seed);
    std::bernoulli_distribution is_null(null_rate);
    for (size_t i = 0; i < rows; ++i) {
        if (shape == "narrow") {
            // 1000 distinct values -> 10 slices.
            d.values[i] = static_cast<int32_t>(rng() % 1000);
        } else {
            // Full int32 range -> 32 slices.
            d.values[i] = static_cast<int32_t>(static_cast<uint32_t>(rng()));
        }
        d.valid[i] = !is_null(rng);
    }
    return d;
}

TargetBitmap
ValidBitmap(const Dataset& d) {
    TargetBitmap valid(d.rows, false);
    for (size_t i = 0; i < d.rows; ++i) {
        if (d.valid[i]) {
            valid.set(i);
        }
    }
    return valid;
}

// Baselines: the unindexed raw-column paths.
TargetBitmap
ScanCompare(const Dataset& d,
            const TargetBitmap& valid,
            int32_t c,
            milvus::bitset::CompareOpType op) {
    TargetBitmap res(d.rows, false);
    res.inplace_compare_val(d.values.data(), d.rows, c, op);
    res &= valid;
    return res;
}

TargetBitmap
ScanRange(const Dataset& d, const TargetBitmap& valid, int32_t lo, int32_t hi) {
    TargetBitmap res(d.rows, false);
    res.inplace_within_range_val(
        lo, hi, d.values.data(), d.rows, milvus::bitset::RangeType::IncInc);
    res &= valid;
    return res;
}

// IN baseline. TermExpr::ExecVisitorImplForData evaluates a numeric IN list
// with the SIMD batch filter (SimdBatchElement) when it has at most
// simdLaneCount<T>() * 8 keys, and with a per-row hash set probe (SetElement,
// load factor 0.5) otherwise. Milvus sorts and deduplicates the keys and
// builds the set once per plan, so that preparation happens in the
// constructor, outside the timed call.
class InBaseline {
 public:
    explicit InBaseline(std::vector<int32_t> keys) : keys_(std::move(keys)) {
        std::sort(keys_.begin(), keys_.end());
        keys_.erase(std::unique(keys_.begin(), keys_.end()), keys_.end());
        if (!UsesSimd()) {
            set_.max_load_factor(0.5f);
            set_.insert(keys_.begin(), keys_.end());
        }
    }

    static size_t
    SimdKeyLimit() {
        return static_cast<size_t>(milvus::exec::simdLaneCount<int32_t>()) * 8;
    }

    bool
    UsesSimd() const {
        return keys_.size() <= SimdKeyLimit();
    }

    const char*
    Name() const {
        return UsesSimd() ? "simd" : "hash";
    }

    // Sorted and unique, as the Go rewriter hands them to TermExpr.
    const std::vector<int32_t>&
    Keys() const {
        return keys_;
    }

    // Valid rows whose value is in the key list, or not in it when `negate`.
    TargetBitmap
    Run(const Dataset& d, const TargetBitmap& valid, bool negate) const {
        TargetBitmap res(d.rows, false);
        if (UsesSimd()) {
            // ORs the matching rows into a byte-aligned bitmap; a fresh
            // TargetBitmap starts on a word boundary.
            milvus::exec::simdFilterChunk<int32_t>(
                d.values.data(),
                static_cast<int>(d.rows),
                reinterpret_cast<uint8_t*>(res.data()),
                keys_.data(),
                static_cast<int>(keys_.size()));
        } else {
            for (size_t i = 0; i < d.rows; ++i) {
                res[i] = set_.contains(d.values[i]);
            }
        }
        if (negate) {
            res.flip();
        }
        res &= valid;
        return res;
    }

 private:
    std::vector<int32_t> keys_;
    ankerl::unordered_dense::set<int32_t> set_;
};

void
Verify(const std::string& what, TargetBitmap a, TargetBitmap b) {
    if (a.size() != b.size() || !(a == b)) {
        std::cerr << "MISMATCH between index and scan for " << what << '\n';
        std::exit(1);
    }
}

void
RunDataset(const Dataset& d) {
    const ScalarBuildBatch<int32_t> batch{
        std::span<const int32_t>(d.values),
        d.null_rate > 0 ? milvus::ValidityView::FromExpanded(d.valid.get())
                        : milvus::ValidityView{}};
    const ScalarBuildInput<int32_t> input{
        std::span<const ScalarBuildBatch<int32_t>>(&batch, 1)};

    // Build time: median of 5 builds.
    std::vector<double> build_ms;
    for (int t = 0; t < 5; ++t) {
        const auto start = Clock::now();
        BitSlicedIndexInt32 tmp(input);
        build_ms.push_back(
            std::chrono::duration<double, std::milli>(Clock::now() - start)
                .count());
        sink = sink + tmp.Count();
    }
    std::sort(build_ms.begin(), build_ms.end());
    const BitSlicedIndexInt32 index(input);
    const auto valid = ValidBitmap(d);

    // Query constants from percentiles of the valid values.
    std::vector<int32_t> sorted;
    for (size_t i = 0; i < d.rows; ++i) {
        if (d.valid[i]) {
            sorted.push_back(d.values[i]);
        }
    }
    std::sort(sorted.begin(), sorted.end());
    if (sorted.empty()) {
        std::cerr << "skipping dataset " << d.name << " with " << d.rows
                  << " rows: no valid values\n";
        return;
    }
    const auto pct = [&](double p) {
        return sorted[std::min(sorted.size() - 1,
                               static_cast<size_t>(p * sorted.size()))];
    };
    // n keys spread evenly over the value distribution (scattered runs).
    const auto spread_keys = [&](int n) {
        std::vector<int32_t> keys;
        for (int i = 0; i < n; ++i) {
            keys.push_back(pct((i + 0.5) / n));
        }
        return keys;
    };
    const InBaseline in8(spread_keys(8)), in32(spread_keys(32)),
        in128(spread_keys(128)), in512(spread_keys(512));

    struct Query {
        std::string name;
        std::string baseline;  // "simd" or "hash", see InBaseline
        std::function<TargetBitmap()> bsi;
        std::function<TargetBitmap()> scan;
    };
    const auto in_query =
        [&](const std::string& name, const InBaseline& in, bool negate) {
            return Query{
                name,
                in.Name(),
                [&index, &in, negate] {
                    const auto& keys = in.Keys();
                    return negate ? index.NotIn(keys.size(), keys.data())
                                  : index.In(keys.size(), keys.data());
                },
                [&d, &valid, &in, negate] { return in.Run(d, valid, negate); }};
        };
    using Op = milvus::bitset::CompareOpType;
    const int32_t p01 = pct(0.01), p45 = pct(0.45), p50 = pct(0.50),
                  p55 = pct(0.55), p99 = pct(0.99);
    const std::vector<Query> queries{
        {"eq_p50",
         "simd",
         [&] { return index.Range(p50, CompareOp::Equal); },
         [&] { return ScanCompare(d, valid, p50, Op::EQ); }},
        {"ne_p50",
         "simd",
         [&] { return index.Range(p50, CompareOp::NotEqual); },
         [&] { return ScanCompare(d, valid, p50, Op::NE); }},
        {"lt_p01",
         "simd",
         [&] { return index.Range(p01, CompareOp::LessThan); },
         [&] { return ScanCompare(d, valid, p01, Op::LT); }},
        {"lt_p50",
         "simd",
         [&] { return index.Range(p50, CompareOp::LessThan); },
         [&] { return ScanCompare(d, valid, p50, Op::LT); }},
        {"ge_p99",
         "simd",
         [&] { return index.Range(p99, CompareOp::GreaterEqual); },
         [&] { return ScanCompare(d, valid, p99, Op::GE); }},
        {"range_p45_p55",
         "simd",
         [&] { return index.Range(p45, true, p55, true); },
         [&] { return ScanRange(d, valid, p45, p55); }},
        in_query("in_8", in8, false),
        in_query("in_32", in32, false),
        in_query("in_128", in128, false),
        in_query("in_512", in512, false),
        in_query("not_in_8", in8, true),
    };

    const auto raw_bytes = d.rows * sizeof(int32_t) + (d.rows + 7) / 8;
    for (const auto& q : queries) {
        auto bsi_result = q.bsi();
        const double selectivity =
            static_cast<double>(bsi_result.count()) / d.rows;
        Verify(d.name + "/" + q.name, std::move(bsi_result), q.scan());
        const auto [bsi_us, scan_us] = InterleavedMedianMicros(q.bsi, q.scan);
        std::cout << d.name << ',' << d.rows << ',' << d.null_rate << ','
                  << index.SliceCount() << ',' << build_ms[2] << ','
                  << index.MemoryUsage() << ',' << raw_bytes << ',' << q.name
                  << ',' << q.baseline << ',' << selectivity << ',' << bsi_us
                  << ',' << scan_us << ',' << scan_us / bsi_us << '\n';
    }
}

}  // namespace

int
main(int argc, char** argv) {
    std::vector<size_t> row_counts{size_t{1} << 20, size_t{1} << 23};
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--quick") == 0) {
            row_counts = {size_t{1} << 16};
        } else if (std::strncmp(argv[i], "--rows=", 7) == 0) {
            const auto rows = std::strtoull(argv[i] + 7, nullptr, 10);
            // simdFilterChunk takes the row count as int.
            if (rows == 0 ||
                rows > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
                std::cerr << "--rows must be a positive integer below 2^31\n";
                return 2;
            }
            row_counts = {static_cast<size_t>(rows)};
        } else {
            std::cerr << "usage: " << argv[0] << " [--rows=N] [--quick]\n";
            return 2;
        }
    }
    std::cerr << "IN lists of at most " << InBaseline::SimdKeyLimit()
              << " keys use the SIMD baseline, longer ones the hash set\n";
    std::cout << "dataset,rows,null_rate,slices,build_ms,index_bytes,"
                 "raw_column_bytes,query,baseline,selectivity,index_us,"
                 "scan_us,scan_over_index\n";
    uint64_t seed = 53831;
    for (const size_t rows : row_counts) {
        for (const std::string shape : {"narrow", "wide"}) {
            for (const double null_rate : {0.0, 0.1}) {
                RunDataset(MakeDataset(shape, rows, null_rate, seed++));
            }
        }
    }
    std::cerr << "all index results matched the scan baseline\n";
    return 0;
}
