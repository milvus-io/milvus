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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/ValidityView.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/scalar/bsi/BitSlicedIndex.h"

namespace milvus::index {
namespace {

// The interface is written for signed integers up to 32 bits; wider or
// unsigned types must be rejected at compile time rather than silently
// overflowing the offset/bound arithmetic.
static_assert(BitSlicedIndexValue<int8_t> && BitSlicedIndexValue<int16_t> &&
              BitSlicedIndexValue<int32_t>);
static_assert(!BitSlicedIndexValue<int64_t> && !BitSlicedIndexValue<uint32_t> &&
              !BitSlicedIndexValue<bool> && !BitSlicedIndexValue<float>);
static_assert(std::is_same_v<BitSlicedIndexInt32, BitSlicedIndex<int32_t>>);
// A column with a single distinct value (0 slices) always walks.
static_assert(BitSlicedIndexInt32::MaxInRunWalks(0) ==
              std::numeric_limits<size_t>::max());

constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
constexpr int32_t kMax = std::numeric_limits<int32_t>::max();

using Rows = std::vector<std::optional<int32_t>>;

enum class ValidityEncoding { Expanded, Packed, PackedWithOffset };

// Owns the value and validity buffers behind a multi-batch ScalarBuildInput.
// Batches without NULLs pass an empty ValidityView, as producers do.
class OwnedInput {
 public:
    OwnedInput(const Rows& rows,
               const std::vector<size_t>& batch_sizes,
               ValidityEncoding encoding) {
        size_t begin = 0;
        for (const size_t size : batch_sizes) {
            AddBatch(rows, begin, size, encoding);
            begin += size;
        }
        EXPECT_EQ(begin, rows.size());
        for (size_t i = 0; i < values_.size(); ++i) {
            batches_.push_back(ScalarBuildBatch<int32_t>{
                std::span<const int32_t>(values_[i]), views_[i]});
        }
    }

    ScalarBuildInput<int32_t>
    View() const {
        return ScalarBuildInput<int32_t>{
            std::span<const ScalarBuildBatch<int32_t>>(batches_)};
    }

 private:
    // Bit offset used by PackedWithOffset to exercise ValidityView::Subview.
    static constexpr size_t kPackedOffset = 5;

    void
    AddBatch(const Rows& rows,
             size_t begin,
             size_t size,
             ValidityEncoding encoding) {
        std::vector<int32_t> values(size);
        bool has_null = false;
        for (size_t i = 0; i < size; ++i) {
            const auto& row = rows[begin + i];
            // NULL rows carry a garbage value that must be ignored.
            values[i] = row.value_or(static_cast<int32_t>(0x5a5a5a5a));
            has_null |= !row.has_value();
        }
        values_.push_back(std::move(values));
        if (!has_null) {
            views_.emplace_back();
            return;
        }
        if (encoding == ValidityEncoding::Expanded) {
            auto expanded = std::make_unique<bool[]>(size);
            for (size_t i = 0; i < size; ++i) {
                expanded[i] = rows[begin + i].has_value();
            }
            views_.push_back(ValidityView::FromExpanded(expanded.get()));
            expanded_.push_back(std::move(expanded));
            return;
        }
        const size_t shift =
            encoding == ValidityEncoding::PackedWithOffset ? kPackedOffset : 0;
        // Pre-fill with ones so that a reader ignoring the bit offset would
        // see wrong validity.
        std::vector<uint8_t> packed((size + shift + 7) / 8, 0xff);
        for (size_t i = 0; i < size; ++i) {
            if (!rows[begin + i].has_value()) {
                const size_t bit = i + shift;
                packed[bit / 8] &= static_cast<uint8_t>(~(1U << (bit % 8)));
            }
        }
        auto view = ValidityView::FromPacked(packed.data())
                        .Subview(static_cast<int64_t>(shift));
        packed_.push_back(std::move(packed));
        views_.push_back(view);
    }

    std::vector<std::vector<int32_t>> values_;
    std::vector<std::unique_ptr<bool[]>> expanded_;
    std::vector<std::vector<uint8_t>> packed_;
    std::vector<ValidityView> views_;
    std::vector<ScalarBuildBatch<int32_t>> batches_;
};

BitSlicedIndexInt32
BuildSingleBatch(const Rows& rows,
                 ValidityEncoding encoding = ValidityEncoding::Expanded) {
    const OwnedInput input(rows, {rows.size()}, encoding);
    return BitSlicedIndexInt32(input.View());
}

// Reference evaluator: a full scan over the raw rows.
using Predicate = std::function<bool(int32_t)>;

std::vector<bool>
Scan(const Rows& rows, const Predicate& predicate) {
    std::vector<bool> result(rows.size(), false);
    for (size_t i = 0; i < rows.size(); ++i) {
        result[i] = rows[i].has_value() && predicate(*rows[i]);
    }
    return result;
}

::testing::AssertionResult
SameBits(const TargetBitmap& actual, const std::vector<bool>& expected) {
    if (actual.size() != expected.size()) {
        return ::testing::AssertionFailure()
               << "bitmap size " << actual.size() << " != expected "
               << expected.size();
    }
    for (size_t i = 0; i < expected.size(); ++i) {
        if (actual[i] != expected[i]) {
            return ::testing::AssertionFailure()
                   << "first mismatch at row " << i << ": got " << actual[i]
                   << ", expected " << expected[i];
        }
    }
    size_t expected_hits = 0;
    for (const bool bit : expected) {
        expected_hits += bit ? 1 : 0;
    }
    if (actual.count() != expected_hits) {
        // Catches stray bits beyond size() that a later count() would see.
        return ::testing::AssertionFailure()
               << "count() " << actual.count() << " != expected "
               << expected_hits;
    }
    return ::testing::AssertionSuccess();
}

std::string
OpName(CompareOp op) {
    switch (op) {
        case CompareOp::Equal:
            return "==";
        case CompareOp::NotEqual:
            return "!=";
        case CompareOp::GreaterThan:
            return ">";
        case CompareOp::GreaterEqual:
            return ">=";
        case CompareOp::LessThan:
            return "<";
        case CompareOp::LessEqual:
            return "<=";
    }
    return "?";
}

Predicate
CompareWith(int32_t c, CompareOp op) {
    switch (op) {
        case CompareOp::Equal:
            return [c](int32_t v) { return v == c; };
        case CompareOp::NotEqual:
            return [c](int32_t v) { return v != c; };
        case CompareOp::GreaterThan:
            return [c](int32_t v) { return v > c; };
        case CompareOp::GreaterEqual:
            return [c](int32_t v) { return v >= c; };
        case CompareOp::LessThan:
            return [c](int32_t v) { return v < c; };
        case CompareOp::LessEqual:
            return [c](int32_t v) { return v <= c; };
    }
    return [](int32_t) { return false; };
}

constexpr CompareOp kAllOps[] = {CompareOp::Equal,
                                 CompareOp::NotEqual,
                                 CompareOp::GreaterThan,
                                 CompareOp::GreaterEqual,
                                 CompareOp::LessThan,
                                 CompareOp::LessEqual};

// Checks every operator for one constant against the scan.
void
ExpectAllOpsMatchScan(const BitSlicedIndexInt32& index,
                      const Rows& rows,
                      int32_t c) {
    for (const auto op : kAllOps) {
        EXPECT_TRUE(
            SameBits(index.Range(c, op), Scan(rows, CompareWith(c, op))))
            << "x " << OpName(op) << " " << c;
    }
}

void
ExpectBetweenMatchesScan(const BitSlicedIndexInt32& index,
                         const Rows& rows,
                         int32_t lo,
                         int32_t hi) {
    for (const bool lo_inc : {false, true}) {
        for (const bool hi_inc : {false, true}) {
            const auto expected = Scan(rows, [=](int32_t v) {
                return (lo_inc ? v >= lo : v > lo) &&
                       (hi_inc ? v <= hi : v < hi);
            });
            EXPECT_TRUE(SameBits(index.Range(lo, lo_inc, hi, hi_inc), expected))
                << (lo_inc ? "[" : "(") << lo << ", " << hi
                << (hi_inc ? "]" : ")");
        }
    }
}

void
ExpectInMatchesScan(const BitSlicedIndexInt32& index,
                    const Rows& rows,
                    const std::vector<int32_t>& keys) {
    const auto contains = [&keys](int32_t v) {
        return std::find(keys.begin(), keys.end(), v) != keys.end();
    };
    EXPECT_TRUE(
        SameBits(index.In(keys.size(), keys.data()), Scan(rows, contains)))
        << "IN of " << keys.size() << " keys";
    EXPECT_TRUE(SameBits(index.NotIn(keys.size(), keys.data()),
                         Scan(rows, [&](int32_t v) { return !contains(v); })))
        << "NOT IN of " << keys.size() << " keys";
}

void
ExpectNullsMatch(const BitSlicedIndexInt32& index, const Rows& rows) {
    std::vector<bool> nulls(rows.size());
    std::vector<bool> not_nulls(rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        nulls[i] = !rows[i].has_value();
        not_nulls[i] = rows[i].has_value();
    }
    EXPECT_TRUE(SameBits(index.IsNull(), nulls));
    EXPECT_TRUE(SameBits(index.IsNotNull(), not_nulls));
}

// ---------------------------------------------------------------------------
// Deterministic cases
// ---------------------------------------------------------------------------

TEST(BitSlicedIndexInt32Test, EmptyInput) {
    const BitSlicedIndexInt32 index(ScalarBuildInput<int32_t>{});
    EXPECT_EQ(index.Count(), size_t{0});
    EXPECT_EQ(index.ValidCount(), size_t{0});
    EXPECT_EQ(index.SliceCount(), size_t{0});
    EXPECT_FALSE(index.Min().has_value());
    EXPECT_FALSE(index.Max().has_value());
    const int32_t key = 0;
    for (const auto op : kAllOps) {
        EXPECT_EQ(index.Range(key, op).size(), size_t{0});
    }
    EXPECT_EQ(index.In(1, &key).size(), size_t{0});
    EXPECT_EQ(index.NotIn(1, &key).size(), size_t{0});
    EXPECT_EQ(index.Range(kMin, true, kMax, true).size(), size_t{0});
    EXPECT_EQ(index.IsNull().size(), size_t{0});
    EXPECT_EQ(index.IsNotNull().size(), size_t{0});
}

TEST(BitSlicedIndexInt32Test, AllNullRowsNeverMatch) {
    const Rows rows(70, std::nullopt);
    for (const auto encoding :
         {ValidityEncoding::Expanded, ValidityEncoding::Packed}) {
        const auto index = BuildSingleBatch(rows, encoding);
        EXPECT_EQ(index.Count(), rows.size());
        EXPECT_EQ(index.ValidCount(), size_t{0});
        EXPECT_EQ(index.SliceCount(), size_t{0});
        for (const int32_t c : {kMin, -1, 0, 1, kMax}) {
            ExpectAllOpsMatchScan(index, rows, c);
            ExpectInMatchesScan(index, rows, {c});
        }
        ExpectBetweenMatchesScan(index, rows, kMin, kMax);
        ExpectNullsMatch(index, rows);
    }
}

TEST(BitSlicedIndexInt32Test, SingleDistinctValueUsesNoSlices) {
    Rows rows(65, int32_t{-7});
    rows[3] = std::nullopt;
    rows[64] = std::nullopt;
    const auto index = BuildSingleBatch(rows);
    EXPECT_EQ(index.SliceCount(), size_t{0});
    EXPECT_EQ(index.Min(), -7);
    EXPECT_EQ(index.Max(), -7);
    for (const int32_t c : {kMin, -8, -7, -6, 0, kMax}) {
        ExpectAllOpsMatchScan(index, rows, c);
    }
    ExpectBetweenMatchesScan(index, rows, -7, -7);
    ExpectBetweenMatchesScan(index, rows, -8, -7);
    ExpectBetweenMatchesScan(index, rows, -7, -6);
    ExpectInMatchesScan(index, rows, {-7, -7, 5});
    ExpectNullsMatch(index, rows);
}

TEST(BitSlicedIndexInt32Test, ExtremesNegativesAndZero) {
    const Rows rows{kMin,
                    kMin + 1,
                    -2,
                    -1,
                    0,
                    std::nullopt,
                    1,
                    2,
                    kMax - 1,
                    kMax,
                    0,
                    std::nullopt,
                    kMin,
                    kMax};
    const auto index = BuildSingleBatch(rows, ValidityEncoding::Packed);
    EXPECT_EQ(index.SliceCount(), size_t{32});
    EXPECT_EQ(index.Min(), kMin);
    EXPECT_EQ(index.Max(), kMax);
    EXPECT_EQ(index.ValidCount(), rows.size() - 2);

    const std::vector<int32_t> constants{kMin,
                                         kMin + 1,
                                         kMin + 2,
                                         -3,
                                         -2,
                                         -1,
                                         0,
                                         1,
                                         2,
                                         3,
                                         kMax - 2,
                                         kMax - 1,
                                         kMax};
    for (const int32_t c : constants) {
        ExpectAllOpsMatchScan(index, rows, c);
    }
    for (const int32_t lo : constants) {
        for (const int32_t hi : constants) {
            ExpectBetweenMatchesScan(index, rows, lo, hi);
        }
    }
    ExpectInMatchesScan(index, rows, {kMin, kMax});
    ExpectInMatchesScan(index, rows, {-1, 0, 1});
    ExpectInMatchesScan(index, rows, {kMax, kMax - 1, kMin + 1, kMin});
    ExpectNullsMatch(index, rows);
}

TEST(BitSlicedIndexInt32Test, ExclusiveBoundsAtExtremesAreEmpty) {
    const Rows rows{kMin, 0, kMax, std::nullopt};
    const auto index = BuildSingleBatch(rows);
    EXPECT_EQ(index.Range(kMin, CompareOp::LessThan).count(), size_t{0});
    EXPECT_EQ(index.Range(kMax, CompareOp::GreaterThan).count(), size_t{0});
    EXPECT_EQ(index.Range(kMax, false, kMax, true).count(), size_t{0});
    EXPECT_EQ(index.Range(kMin, true, kMin, false).count(), size_t{0});
    EXPECT_EQ(index.Range(5, true, 4, true).count(), size_t{0});
    EXPECT_EQ(index.Range(kMax, true, kMin, true).count(), size_t{0});
    // Full range selects every valid row but no NULL row.
    EXPECT_TRUE(SameBits(index.Range(kMin, true, kMax, true),
                         {true, true, true, false}));
}

TEST(BitSlicedIndexInt32Test, NarrowRangeTrimsSlices) {
    Rows rows;
    for (int32_t v = -50; v <= 50; ++v) {
        rows.emplace_back(v);
    }
    const auto index = BuildSingleBatch(rows);
    // max - min = 100 needs 7 bits.
    EXPECT_EQ(index.SliceCount(), size_t{7});
    for (int32_t c = -52; c <= 52; ++c) {
        ExpectAllOpsMatchScan(index, rows, c);
    }
}

TEST(BitSlicedIndexInt32Test, MultipleBatchesWithMixedValidity) {
    std::mt19937 rng(20261013);
    std::uniform_int_distribution<int32_t> value(-1000, 1000);
    std::bernoulli_distribution is_null(0.25);
    Rows rows(300);
    for (size_t i = 0; i < rows.size(); ++i) {
        // Keep the middle batch NULL-free so it passes an empty validity view.
        if (i < 100 || i >= 200) {
            if (is_null(rng)) {
                continue;
            }
        }
        rows[i] = value(rng);
    }
    for (const auto encoding : {ValidityEncoding::Expanded,
                                ValidityEncoding::Packed,
                                ValidityEncoding::PackedWithOffset}) {
        const OwnedInput input(rows, {100, 100, 0, 100}, encoding);
        const BitSlicedIndexInt32 index(input.View());
        EXPECT_EQ(index.Count(), rows.size());
        ExpectNullsMatch(index, rows);
        for (const int32_t c : {-1001, -1000, -1, 0, 1, 999, 1000, 1001}) {
            ExpectAllOpsMatchScan(index, rows, c);
        }
        ExpectBetweenMatchesScan(index, rows, -10, 10);
    }
}

TEST(BitSlicedIndexInt32Test, InHandlesDuplicatesRunsAndOutOfRangeKeys) {
    Rows rows;
    for (int32_t v = 0; v < 64; ++v) {
        rows.emplace_back(v % 3 == 0 ? std::optional<int32_t>{} : v);
    }
    const auto index = BuildSingleBatch(rows);
    ExpectInMatchesScan(index, rows, {});
    ExpectInMatchesScan(index, rows, {5, 5, 5});
    // A run of consecutive keys, a gap, and keys outside [min, max].
    ExpectInMatchesScan(index, rows, {kMin, -1, 10, 11, 12, 13, 20, 64, kMax});
    // Every value: equivalent to IS NOT NULL.
    std::vector<int32_t> all(64);
    for (int32_t v = 0; v < 64; ++v) {
        all[static_cast<size_t>(v)] = 63 - v;
    }
    ExpectInMatchesScan(index, rows, all);
}

// Independent count of the runs In() groups its keys into: keys outside
// [lo, hi] are dropped, duplicates collapse, consecutive values merge.
size_t
CountInRangeRuns(std::vector<int32_t> keys, int32_t lo, int32_t hi) {
    keys.erase(std::remove_if(keys.begin(),
                              keys.end(),
                              [&](int32_t k) { return k < lo || k > hi; }),
               keys.end());
    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    size_t runs = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i == 0 || int64_t{keys[i]} != int64_t{keys[i - 1]} + 1) {
            ++runs;
        }
    }
    return runs;
}

// In() walks the slices for at most MaxInRunWalks(SliceCount()) key runs and
// decodes row offsets for more. Every boundary case is derived from that
// function for the index under test, so changing the budget moves the cases
// with it, and each case asserts its exact in-range run count so edits to the
// key construction cannot move a case off the boundary either. Cases run
// against a narrow index (11 slices; decode probes a dense key bitmap) and a
// wide one (32 slices; decode probes a key hash set).
TEST(BitSlicedIndexInt32Test, InWalkAndDecodeStrategiesMatchScan) {
    // Values cover [-1000, 1000] (37 is coprime to 2001), with some NULLs.
    Rows narrow;
    for (int32_t i = 0; i < 2500; ++i) {
        if (i % 7 == 3) {
            narrow.emplace_back(std::nullopt);
        } else {
            narrow.emplace_back((i * 37) % 2001 - 1000);
        }
    }
    Rows wide = narrow;
    wide.emplace_back(kMin);
    wide.emplace_back(kMax);

    struct Case {
        size_t singles;  // isolated keys -1000, -998, ... (never adjacent)
        bool long_run;   // plus one consecutive run 40..60
        size_t expected_runs;
    };

    for (const bool use_wide : {false, true}) {
        const Rows& rows = use_wide ? wide : narrow;
        for (const auto encoding :
             {ValidityEncoding::Expanded, ValidityEncoding::PackedWithOffset}) {
            const auto index = BuildSingleBatch(rows, encoding);
            const int64_t span = int64_t{*index.Max()} - int64_t{*index.Min()};
            if (use_wide) {
                ASSERT_EQ(index.SliceCount(), size_t{32});
                ASSERT_GE(static_cast<uint64_t>(span),
                          BitSlicedIndexInt32::kMaxDenseKeySpan);
            } else {
                ASSERT_EQ(index.SliceCount(), size_t{11});
                ASSERT_EQ(span, 2000);
            }
            const size_t kT =
                BitSlicedIndexInt32::MaxInRunWalks(index.SliceCount());
            // Isolated keys are the even values -1000, -998, ...; up to
            // kT + 2 of them must fit below the long run 40..60 without
            // touching it (that holds for kT up to 517).
            ASSERT_GE(kT, size_t{2});
            ASSERT_LE(kT + 2, size_t{519});
            const Case cases[] = {
                {1, false, 1},
                {kT - 1, false, kT - 1},
                {kT, false, kT},          // walk: last count on the walk side
                {kT + 1, false, kT + 1},  // decode: first on the decode side
                {kT + 2, false, kT + 2},
                {kT - 1, true, kT},   // walk, singles plus a range run
                {kT, true, kT + 1},   // decode, singles plus a range run
                {1001, false, 1001},  // every even value in [-1000, 1000]
            };
            for (const auto& c : cases) {
                std::vector<int32_t> keys;
                for (size_t r = 0; r < c.singles; ++r) {
                    keys.push_back(-1000 + static_cast<int32_t>(2 * r));
                }
                if (c.long_run) {
                    for (int32_t v = 40; v <= 60; ++v) {
                        keys.push_back(v);
                    }
                }
                keys.push_back(keys.front());  // duplicates never add a run
                if (!use_wide) {
                    // Out of range for the narrow index only, so they must not
                    // add runs there.
                    keys.insert(keys.end(),
                                {kMin, -1005, -1002, 1002, 2000, kMax});
                }
                SCOPED_TRACE(::testing::Message()
                             << "wide=" << use_wide << " singles=" << c.singles
                             << " long_run=" << c.long_run);
                ASSERT_EQ(CountInRangeRuns(keys, *index.Min(), *index.Max()),
                          c.expected_runs);
                ExpectInMatchesScan(index, rows, keys);
            }
        }
    }
}

// The decode strategy switches from a dense key bitmap to a key hash set when
// max - min reaches kMaxDenseKeySpan; check both sides of that switch.
TEST(BitSlicedIndexInt32Test, InDecodeProbesAgreeAtDenseSpanLimit) {
    constexpr auto kLimit =
        static_cast<int64_t>(BitSlicedIndexInt32::kMaxDenseKeySpan);
    for (const int64_t span : {kLimit - 1, kLimit}) {
        SCOPED_TRACE(span);
        const auto top = static_cast<int32_t>(span);
        Rows rows{0, top, std::nullopt, 3, top - 1, 7, top, 11, 0};
        const auto index = BuildSingleBatch(rows);
        ASSERT_EQ(int64_t{*index.Max()} - int64_t{*index.Min()}, span);
        // More isolated keys than the walk limit, so decode runs; the ends of
        // the span are included.
        const size_t walk_limit =
            BitSlicedIndexInt32::MaxInRunWalks(index.SliceCount());
        std::vector<int32_t> keys{0, top};
        for (size_t r = 0; r < walk_limit + 2; ++r) {
            keys.push_back(3 + static_cast<int32_t>(4 * r));
        }
        ASSERT_LT(keys.back(), top - 1);
        ASSERT_GT(CountInRangeRuns(keys, *index.Min(), *index.Max()),
                  walk_limit);
        ExpectInMatchesScan(index, rows, keys);
    }
}

// Pins the error codes documented in the design doc, not just "throws".
template <typename F>
void
ExpectSegcoreError(ErrorCode expected, F&& operation) {
    try {
        std::forward<F>(operation)();
        ADD_FAILURE() << "operation expected a SegcoreError";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), expected);
    } catch (...) {
        ADD_FAILURE() << "operation threw a non-SegcoreError";
    }
}

TEST(BitSlicedIndexInt32Test, RejectsInvalidArguments) {
    const Rows rows{1, std::nullopt, 3};
    const auto index = BuildSingleBatch(rows);
    // A null key pointer is only valid together with n == 0; AssertInfo
    // reports caller bugs as UnexpectedError.
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { (void)index.In(1, nullptr); });
    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { (void)index.NotIn(2, nullptr); });
    EXPECT_TRUE(SameBits(index.In(0, nullptr), {false, false, false}));
    EXPECT_TRUE(SameBits(index.NotIn(0, nullptr), {true, false, true}));
    // An operator outside the CompareOp enumerators is rejected with
    // OpTypeInvalid, not silently treated as one of them.
    ExpectSegcoreError(ErrorCode::OpTypeInvalid, [&] {
        (void)index.Range(1, static_cast<CompareOp>(99));
    });
}

TEST(BitSlicedIndexInt32Test, ResultsPartitionRows) {
    std::mt19937 rng(7);
    std::uniform_int_distribution<int32_t> value(kMin, kMax);
    std::bernoulli_distribution is_null(0.2);
    Rows rows(513);
    for (auto& row : rows) {
        if (!is_null(rng)) {
            row = value(rng);
        }
    }
    const auto index = BuildSingleBatch(rows);
    const auto nulls = index.IsNull();
    for (int trial = 0; trial < 20; ++trial) {
        const int32_t c = value(rng);
        // <, ==, > and IS NULL partition the rows.
        auto lt = index.Range(c, CompareOp::LessThan);
        auto eq = index.Range(c, CompareOp::Equal);
        auto gt = index.Range(c, CompareOp::GreaterThan);
        EXPECT_EQ(lt.count() + eq.count() + gt.count() + nulls.count(),
                  rows.size());
        lt |= eq;
        lt |= gt;
        lt |= nulls;
        EXPECT_TRUE(lt.all());
    }
}

TEST(BitSlicedIndexInt32Test, MemoryUsageCountsSlicesAndExistence) {
    Rows rows;
    for (int32_t v = 0; v < 1000; ++v) {
        rows.emplace_back(v);
    }
    const auto index = BuildSingleBatch(rows);
    // 1000 rows -> 125 bytes per bitmap, rounded up to 8-byte words = 128;
    // max - min = 999 needs 10 slices, plus the existence bitmap.
    EXPECT_EQ(index.SliceCount(), size_t{10});
    EXPECT_EQ(index.MemoryUsage(), 11 * 128);
}

// ---------------------------------------------------------------------------
// Randomized comparison against a scan
// ---------------------------------------------------------------------------

enum class Distribution {
    FullRange,    // uniform over all of int32
    AroundZero,   // small range crossing zero
    Extremes,     // values clustered at INT32_MIN / -1 / 0 / 1 / INT32_MAX
    NearMax,      // narrow range touching INT32_MAX
    NearMin,      // narrow range touching INT32_MIN
    PowersOfTwo,  // +/- 2^k and neighbours, stresses slice boundaries
};

int32_t
Draw(Distribution distribution, std::mt19937& rng) {
    switch (distribution) {
        case Distribution::FullRange:
            return std::uniform_int_distribution<int32_t>(kMin, kMax)(rng);
        case Distribution::AroundZero:
            return std::uniform_int_distribution<int32_t>(-40, 40)(rng);
        case Distribution::Extremes: {
            static constexpr int32_t kValues[] = {
                kMin, kMin + 1, -2, -1, 0, 1, 2, kMax - 1, kMax};
            return kValues[std::uniform_int_distribution<size_t>(
                0, std::size(kValues) - 1)(rng)];
        }
        case Distribution::NearMax:
            return std::uniform_int_distribution<int32_t>(kMax - 300,
                                                          kMax)(rng);
        case Distribution::NearMin:
            return std::uniform_int_distribution<int32_t>(kMin,
                                                          kMin + 300)(rng);
        case Distribution::PowersOfTwo: {
            const int shift = std::uniform_int_distribution<int>(0, 30)(rng);
            const int64_t base = int64_t{1} << shift;
            const int64_t delta =
                std::uniform_int_distribution<int64_t>(-1, 1)(rng);
            const bool negative = std::bernoulli_distribution(0.5)(rng);
            const int64_t v = (negative ? -base : base) + delta;
            return static_cast<int32_t>(std::clamp<int64_t>(v, kMin, kMax));
        }
    }
    return 0;
}

// Constants concentrated where off-by-one errors live: stored values and
// their neighbours, the observed bounds, the type extremes and zero.
int32_t
DrawConstant(const std::vector<int32_t>& seen,
             Distribution distribution,
             std::mt19937& rng) {
    const int kind = std::uniform_int_distribution<int>(0, 4)(rng);
    if (kind <= 1 && !seen.empty()) {
        const int64_t v = seen[std::uniform_int_distribution<size_t>(
            0, seen.size() - 1)(rng)];
        const int64_t delta =
            kind == 0 ? 0 : std::uniform_int_distribution<int64_t>(-1, 1)(rng);
        return static_cast<int32_t>(std::clamp<int64_t>(v + delta, kMin, kMax));
    }
    if (kind == 2) {
        static constexpr int32_t kSpecial[] = {
            kMin, kMin + 1, -1, 0, 1, kMax - 1, kMax};
        return kSpecial[std::uniform_int_distribution<size_t>(
            0, std::size(kSpecial) - 1)(rng)];
    }
    if (kind == 3 && !seen.empty()) {
        const auto [lo, hi] = std::minmax_element(seen.begin(), seen.end());
        const int64_t edge = std::bernoulli_distribution(0.5)(rng)
                                 ? int64_t{*lo} - 1
                                 : int64_t{*hi} + 1;
        return static_cast<int32_t>(std::clamp<int64_t>(edge, kMin, kMax));
    }
    return Draw(distribution, rng);
}

struct RandomCase {
    Distribution distribution;
    double null_rate;
    size_t rows;
    ValidityEncoding encoding;
    uint32_t seed;
};

std::string
Describe(const RandomCase& c) {
    return "distribution=" + std::to_string(static_cast<int>(c.distribution)) +
           " null_rate=" + std::to_string(c.null_rate) +
           " rows=" + std::to_string(c.rows) +
           " encoding=" + std::to_string(static_cast<int>(c.encoding)) +
           " seed=" + std::to_string(c.seed);
}

void
RunRandomCase(const RandomCase& c) {
    SCOPED_TRACE(Describe(c));
    std::mt19937 rng(c.seed);
    std::bernoulli_distribution is_null(c.null_rate);

    Rows rows(c.rows);
    std::vector<int32_t> seen;
    for (auto& row : rows) {
        if (!is_null(rng)) {
            row = Draw(c.distribution, rng);
            seen.push_back(*row);
        }
    }

    // Random batch boundaries, including empty batches.
    std::vector<size_t> batch_sizes;
    size_t remaining = c.rows;
    while (remaining > 0) {
        const size_t size = std::min(
            remaining, std::uniform_int_distribution<size_t>(0, 200)(rng));
        batch_sizes.push_back(size);
        remaining -= size;
    }
    const OwnedInput input(rows, batch_sizes, c.encoding);
    const BitSlicedIndexInt32 index(input.View());

    ASSERT_EQ(index.Count(), rows.size());
    ASSERT_EQ(index.ValidCount(), seen.size());
    if (!seen.empty()) {
        const auto [lo, hi] = std::minmax_element(seen.begin(), seen.end());
        ASSERT_EQ(index.Min(), *lo);
        ASSERT_EQ(index.Max(), *hi);
        const auto span = static_cast<uint32_t>(int64_t{*hi} - int64_t{*lo});
        ASSERT_EQ(index.SliceCount(),
                  static_cast<size_t>(std::bit_width(span)));
    }
    ExpectNullsMatch(index, rows);

    for (int i = 0; i < 24; ++i) {
        ExpectAllOpsMatchScan(
            index, rows, DrawConstant(seen, c.distribution, rng));
    }
    for (int i = 0; i < 24; ++i) {
        int32_t lo = DrawConstant(seen, c.distribution, rng);
        int32_t hi = DrawConstant(seen, c.distribution, rng);
        // Mostly well-formed ranges, but keep some inverted ones.
        if (lo > hi && std::bernoulli_distribution(0.8)(rng)) {
            std::swap(lo, hi);
        }
        ExpectBetweenMatchesScan(index, rows, lo, hi);
    }
    for (int i = 0; i < 8; ++i) {
        // Short lists exercise the slice-walk strategy, long ones the decode
        // strategy.
        const size_t n = std::uniform_int_distribution<size_t>(0, 120)(rng);
        std::vector<int32_t> keys(n);
        for (auto& key : keys) {
            key = DrawConstant(seen, c.distribution, rng);
        }
        // Occasionally add a run of consecutive keys.
        if (!keys.empty() && std::bernoulli_distribution(0.5)(rng)) {
            const int64_t start = keys.front();
            for (int64_t k = 1; k <= 5 && start + k <= kMax; ++k) {
                keys.push_back(static_cast<int32_t>(start + k));
            }
        }
        ExpectInMatchesScan(index, rows, keys);
    }
}

TEST(BitSlicedIndexInt32Test, RandomizedAgainstScan) {
    uint32_t seed = 53831;
    for (const auto distribution : {Distribution::FullRange,
                                    Distribution::AroundZero,
                                    Distribution::Extremes,
                                    Distribution::NearMax,
                                    Distribution::NearMin,
                                    Distribution::PowersOfTwo}) {
        for (const double null_rate : {0.0, 0.3, 0.95}) {
            for (const size_t rows : {size_t{1},
                                      size_t{63},
                                      size_t{64},
                                      size_t{65},
                                      size_t{1000},
                                      size_t{4097}}) {
                for (const auto encoding :
                     {ValidityEncoding::Expanded,
                      ValidityEncoding::Packed,
                      ValidityEncoding::PackedWithOffset}) {
                    RunRandomCase(
                        {distribution, null_rate, rows, encoding, seed++});
                    if (::testing::Test::HasFatalFailure()) {
                        return;
                    }
                }
            }
        }
    }
}

}  // namespace
}  // namespace milvus::index
