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

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Types.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

template <typename T>
void
ExpectPredicateProfiles(std::string_view type_name) {
    for (const auto suffix : {std::string_view{},
                              std::string_view{"Mmap"},
                              std::string_view{"NonNull"},
                              std::string_view{"NonNullMmap"}}) {
        const auto name =
            "Bitmap" + std::string(type_name) + std::string(suffix);
        const auto& backend = ScalarReaderBackends().Get<T>(name);
        EXPECT_TRUE(backend.DeriveCaps().predicate) << name;
    }
}

TEST(BitmapIndexReaderTest, AdvertisesPrimitivePredicateMatrix) {
    ExpectPredicateProfiles<bool>("Bool");
    ExpectPredicateProfiles<int8_t>("Int8");
    ExpectPredicateProfiles<int16_t>("Int16");
    ExpectPredicateProfiles<int32_t>("Int32");
    ExpectPredicateProfiles<int64_t>("Int64");
    ExpectPredicateProfiles<float>("Float");
    ExpectPredicateProfiles<double>("Double");
    ExpectPredicateProfiles<std::string_view>("Varchar");
}

TEST(BitmapIndexReaderTest, ProvidesInt64Queries) {
    // This promise is checked without capability filtering, so a regression
    // cannot silently remove this backend from the shared query suites.
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.value_lookup);

    const std::array<int64_t, 1> values{42};
    const std::array<ScalarBuildBatch<int64_t>, 1> batches{
        ScalarBuildBatch<int64_t>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<int64_t>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().value_lookup);
    EXPECT_NE(
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get()),
        nullptr);
    EXPECT_NE(dynamic_cast<const IScalarValueReader<int64_t>*>(reader.get()),
              nullptr);
}

TEST(BitmapIndexReaderTest, ProvidesVarcharQueries) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("BitmapVarchar");
    const auto caps = backend.DeriveCaps();
    EXPECT_TRUE(caps.predicate);
    EXPECT_TRUE(caps.pattern_match);
    EXPECT_EQ(backend.PatternPolicy(PatternOp::Match),
              PatternQueryPolicy::UseAndRun);

    const std::array<std::string_view, 1> values{"value"};
    const std::array<ScalarBuildBatch<std::string_view>, 1> batches{
        ScalarBuildBatch<std::string_view>{values, {}}};
    auto reader = backend.Create(ScalarBuildInput<std::string_view>{batches});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_TRUE(reader->Caps().pattern_match);
    EXPECT_NE(dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                  reader.get()),
              nullptr);
    EXPECT_NE(dynamic_cast<const IPatternMatchReader*>(reader.get()), nullptr);
}

TEST(BitmapIndexReaderTest, NullablePackedWordBoundaryKeepsFinalAllocation) {
    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        const auto& backend = ScalarReaderBackends().Get<int32_t>(
            mmap ? "BitmapInt32Mmap" : "BitmapInt32");
        std::array<int64_t, 2> resident{};
        for (const size_t rows : {size_t{64}, size_t{65}}) {
            ScalarTestData<int32_t> data(
                std::vector<int32_t>(rows, int32_t{7}));
            data.validity.reset(0);
            const ScalarTestInput<int32_t> input(data);
            auto artifact = backend.Build(input.View(), {.row_count = rows});
            auto reader =
                OpenV3(backend, SerializeV3(*artifact), {.row_count = rows});
            ASSERT_NE(reader, nullptr);
            const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<int32_t>*>(
                    reader.get());
            ASSERT_NE(nulls, nullptr);
            ASSERT_NE(predicate, nullptr);
            ExpectHits(nulls->IsNull(), rows, {0});
            const int32_t key = 7;
            EXPECT_EQ(predicate->In(1, &key).count(), rows - 1);
            resident[rows == 64 ? 0 : 1] = reader->CellByteSize().memory_bytes;
        }
        // The 65th row expands one validity word and one dense posting word.
        // Retaining a second validity staging buffer would add another word.
        EXPECT_EQ(resident[1] - resident[0], 2 * sizeof(uint64_t));
    }
}

// Roaring layout (keys above the build-mode bound): broad ranges take the
// complement path, narrow ones the forward path, both must equal a scan.
TEST(BitmapIndexReaderTest, RoaringRangeMatchesScan) {
    constexpr size_t kRows = 4096;
    constexpr int64_t kKeys = 640;
    static_assert(kKeys > DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND);
    constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
    constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

    ScalarTestData<int64_t> data{std::vector<int64_t>(kRows)};
    for (size_t i = 0; i < kRows; ++i) {
        // Uneven key frequencies in [-320, 319].
        data.values[i] = static_cast<int64_t>((i * i) % kKeys) - kKeys / 2;
    }
    data.values[1] = kMin;
    data.values[2] = kMax;
    for (size_t i = 0; i < kRows; i += 7) {
        data.validity.reset(i);
    }
    const auto scan = [&](auto&& matches) {
        TargetBitmap expected(kRows, false);
        for (size_t i = 0; i < kRows; ++i) {
            if (data.validity[i] && matches(data.values[i])) {
                expected.set(i);
            }
        }
        return expected;
    };

    for (bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        const auto& backend = ScalarReaderBackends().Get<int64_t>(
            mmap ? "BitmapInt64Mmap" : "BitmapInt64");
        const ScalarTestInput<int64_t> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = kRows});
        auto reader =
            OpenV3(backend, SerializeV3(*artifact), {.row_count = kRows});
        ASSERT_NE(reader, nullptr);
        const auto* predicate =
            dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
        ASSERT_NE(predicate, nullptr);

        using Cmp = bool (*)(int64_t, int64_t);
        const std::pair<CompareOp, Cmp> ops[] = {
            {CompareOp::LessThan, [](int64_t v, int64_t t) { return v < t; }},
            {CompareOp::LessEqual, [](int64_t v, int64_t t) { return v <= t; }},
            {CompareOp::GreaterThan,
             [](int64_t v, int64_t t) { return v > t; }},
            {CompareOp::GreaterEqual,
             [](int64_t v, int64_t t) { return v >= t; }},
        };
        for (const int64_t threshold : {kMin,
                                        int64_t{-321},
                                        int64_t{-320},
                                        int64_t{0},
                                        int64_t{319},
                                        int64_t{320},
                                        kMax}) {
            for (const auto& [op, matches] : ops) {
                SCOPED_TRACE(threshold);
                SCOPED_TRACE(static_cast<int>(op));
                const auto expected = scan(
                    [&](int64_t value) { return matches(value, threshold); });
                EXPECT_TRUE(predicate->Range(threshold, op) == expected);
            }
        }

        struct Interval {
            int64_t lo;
            bool lo_inc;
            int64_t hi;
            bool hi_inc;
        };
        for (const auto& range : {Interval{kMin, true, kMax, true},
                                  Interval{kMin, false, kMax, false},
                                  Interval{-320, true, 319, true},
                                  Interval{-319, true, 318, true},
                                  Interval{-10, false, 10, false},
                                  Interval{0, true, 0, true},
                                  Interval{0, true, 0, false},
                                  Interval{0, false, 0, true},
                                  Interval{0, false, 0, false},
                                  Interval{5, true, -5, true}}) {
            SCOPED_TRACE(range.lo);
            SCOPED_TRACE(range.hi);
            const auto expected = scan([&](int64_t value) {
                return (range.lo_inc ? value >= range.lo : value > range.lo) &&
                       (range.hi_inc ? value <= range.hi : value < range.hi);
            });
            EXPECT_TRUE(predicate->Range(
                            range.lo, range.lo_inc, range.hi, range.hi_inc) ==
                        expected);
        }
    }
}

// Non-nested ARRAY rows sit in one posting per element, so a row with values
// on both sides of a range still matches: the complement path must stay off.
TEST(BitmapIndexReaderTest, ArrayRangeKeepsForwardSemantics) {
    constexpr size_t kRows = 640;
    static_assert(static_cast<int64_t>(kRows) >
                  DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND);
    constexpr int64_t kShared = 100000;
    std::vector<OwnedArrayValue> values;
    values.reserve(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        const std::array<int64_t, 2> row = {static_cast<int64_t>(i), kShared};
        values.push_back(
            OwnedArrayValue::FromFixed<int64_t>(DataType::INT64, row));
    }
    ScalarTestData<ArrayView> data(std::move(values));
    data.validity.reset(3);

    const auto& backend =
        ScalarReaderBackends().Get<ArrayView>("BitmapInt64Array");
    const ScalarTestInput<ArrayView> input(data);
    auto artifact = backend.Build(input.View(), {.row_count = kRows});
    auto reader = OpenV3(backend, SerializeV3(*artifact), {.row_count = kRows});
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<int64_t>*>(reader.get());
    ASSERT_NE(predicate, nullptr);

    // Rows outside [320, kShared] hold fewer elements than rows inside, so a
    // complement over the outside postings would drop rows 0..319.
    TargetBitmap expected(kRows, true);
    expected.reset(3);
    EXPECT_TRUE(predicate->Range(int64_t{320}, CompareOp::GreaterEqual) ==
                expected);
    EXPECT_TRUE(predicate->Range(int64_t{320}, true, kShared, true) ==
                expected);
}

}  // namespace
}  // namespace milvus::index::test
