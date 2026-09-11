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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/Array.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "simdjson/padded_string.h"

using namespace milvus;
using namespace milvus::exec;

namespace {

enum class Tri { False, True, Unknown };

std::ostream&
operator<<(std::ostream& os, Tri t) {
    switch (t) {
        case Tri::False:
            return os << "FALSE";
        case Tri::True:
            return os << "TRUE";
        case Tri::Unknown:
            return os << "UNKNOWN";
    }
    return os;
}

constexpr Tri kF = Tri::False;
constexpr Tri kT = Tri::True;
constexpr Tri kU = Tri::Unknown;

enum class Mode { Sequential, Random, RandomRowByRow };

const char*
ModeName(Mode mode) {
    switch (mode) {
        case Mode::Sequential:
            return "sequential";
        case Mode::Random:
            return "random";
        case Mode::RandomRowByRow:
            return "random-row-by-row";
    }
    return "?";
}

// Runs one kernel on a plain array with a stack TriStateOut initialized the
// way EvalKernel initializes it (match=0, known=1). No fold is applied.
template <typename K, typename V>
std::vector<Tri>
RunKernel(const K& kernel,
          Mode mode,
          const V* data,
          size_t n,
          ValidityView validity = ValidityView{},
          TargetBitmap* candidates = nullptr) {
    TargetBitmap match(n, false);
    TargetBitmap known(n, true);
    auto candidates_view = [&](size_t offset, size_t size) {
        return candidates == nullptr
                   ? TargetBitmapView{}
                   : TargetBitmapView(candidates->data(), offset, size);
    };

    if (mode == Mode::RandomRowByRow) {
        for (size_t i = 0; i < n; ++i) {
            const CandidateBatch<V> row{
                .data = data + i,
                .validity = validity ? validity.Subview(i) : ValidityView{},
                .candidates = candidates_view(i, 1),
                .segment_offsets = nullptr,
                .size = 1,
            };
            kernel.template Eval<FilterType::random>(
                row,
                TriStateOut{TargetBitmapView(match.data(), i, 1),
                            TargetBitmapView(known.data(), i, 1)});
        }
    } else {
        const CandidateBatch<V> batch{
            .data = data,
            .validity = validity,
            .candidates = candidates_view(0, n),
            .segment_offsets = nullptr,
            .size = n,
        };
        const TriStateOut out{TargetBitmapView(match), TargetBitmapView(known)};
        if (mode == Mode::Sequential) {
            kernel.template Eval<FilterType::sequential>(batch, out);
        } else {
            kernel.template Eval<FilterType::random>(batch, out);
        }
    }

    const TargetBitmap& cmatch = match;
    const TargetBitmap& cknown = known;
    std::vector<Tri> tri(n);
    for (size_t i = 0; i < n; ++i) {
        if (!cknown[i]) {
            EXPECT_FALSE(cmatch[i]) << "row " << i << " UNKNOWN with match=1";
            tri[i] = Tri::Unknown;
        } else {
            tri[i] = cmatch[i] ? Tri::True : Tri::False;
        }
    }
    return tri;
}

template <typename K, typename V>
void
ExpectAllModes(const K& kernel,
               const V* data,
               size_t n,
               const std::vector<Tri>& expected,
               ValidityView validity = ValidityView{},
               TargetBitmap* candidates = nullptr) {
    for (auto mode : {Mode::Sequential, Mode::Random, Mode::RandomRowByRow}) {
        EXPECT_EQ(RunKernel(kernel, mode, data, n, validity, candidates),
                  expected)
            << "mode=" << ModeName(mode);
    }
}

TargetBitmap
Bits(const std::vector<bool>& bits) {
    TargetBitmap bitmap(bits.size(), false);
    for (size_t i = 0; i < bits.size(); ++i) {
        bitmap[i] = bits[i];
    }
    return bitmap;
}

template <typename V>
BinaryRangeKernel<V>
ScalarKernel(BinaryRangeHighPrecisionType<V> lower,
             BinaryRangeHighPrecisionType<V> upper,
             bool lower_inclusive,
             bool upper_inclusive) {
    return BinaryRangeKernel<V>::FromBounds(
        ClampBinaryRangeBounds<V>(std::move(lower),
                                  std::move(upper),
                                  lower_inclusive,
                                  upper_inclusive),
        nullptr);
}

std::vector<Json>
Jsons(const std::vector<std::string>& docs) {
    std::vector<Json> out;
    out.reserve(docs.size());
    for (const auto& doc : docs) {
        out.push_back(Json(simdjson::padded_string(doc)));
    }
    return out;
}

struct ArrayRows {
    std::vector<Array> arrays;
    std::vector<ArrayView> views;
};

ArrayRows
Int64Arrays(const std::vector<std::vector<int64_t>>& rows) {
    ArrayRows out;
    out.arrays.reserve(rows.size());
    for (const auto& row : rows) {
        proto::schema::ScalarField field;
        for (auto v : row) {
            field.mutable_long_data()->add_data(v);
        }
        out.arrays.emplace_back(field);
    }
    out.views.reserve(out.arrays.size());
    for (auto& arr : out.arrays) {
        out.views.emplace_back(const_cast<char*>(arr.data()),
                               arr.length(),
                               arr.byte_size(),
                               arr.get_element_type(),
                               arr.get_offsets_data());
    }
    return out;
}

ArrayRows
StringArrays(const std::vector<std::vector<std::string>>& rows) {
    ArrayRows out;
    out.arrays.reserve(rows.size());
    for (const auto& row : rows) {
        proto::schema::ScalarField field;
        for (const auto& v : row) {
            field.mutable_string_data()->add_data(v);
        }
        out.arrays.emplace_back(field);
    }
    out.views.reserve(out.arrays.size());
    for (auto& arr : out.arrays) {
        out.views.emplace_back(const_cast<char*>(arr.data()),
                               arr.length(),
                               arr.byte_size(),
                               arr.get_element_type(),
                               arr.get_offsets_data());
    }
    return out;
}

proto::plan::GenericValue
Int64Bound(int64_t v) {
    proto::plan::GenericValue g;
    g.set_int64_val(v);
    return g;
}

proto::plan::GenericValue
FloatBound(double v) {
    proto::plan::GenericValue g;
    g.set_float_val(v);
    return g;
}

}  // namespace

// ---------------------------------------------------------------------------
// BinaryRangeKernel<T>
// ---------------------------------------------------------------------------

TEST(ScanKernelBinaryRange, ScalarInt64InclusiveExclusive) {
    const std::vector<int64_t> data{-5, 0, 5, 10, 15};
    struct Case {
        bool lower_inclusive;
        bool upper_inclusive;
        std::vector<Tri> expected;
    };
    const std::vector<Case> cases{
        {true, true, {kF, kT, kT, kT, kF}},
        {true, false, {kF, kT, kT, kF, kF}},
        {false, true, {kF, kF, kT, kT, kF}},
        {false, false, {kF, kF, kT, kF, kF}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(::testing::Message() << "li=" << c.lower_inclusive
                                          << " ui=" << c.upper_inclusive);
        ExpectAllModes(
            ScalarKernel<int64_t>(0, 10, c.lower_inclusive, c.upper_inclusive),
            data.data(),
            data.size(),
            c.expected);
    }
}

TEST(ScanKernelBinaryRange, ScalarDoubleSequentialMatchesRandom) {
    const std::vector<double> data{-0.5, 0.0, 9.99, 10.0, 10.01};
    ExpectAllModes(ScalarKernel<double>(0.0, 10.0, true, false),
                   data.data(),
                   data.size(),
                   {kF, kT, kT, kF, kF});
}

TEST(ScanKernelBinaryRange, ScalarStringViewAndString) {
    const std::vector<std::string> owned{"a", "b", "c", "d", "e"};
    std::vector<std::string_view> views(owned.begin(), owned.end());
    const std::vector<Tri> expected{kF, kT, kT, kF, kF};

    ExpectAllModes(ScalarKernel<std::string_view>(
                       std::string("b"), std::string("d"), true, false),
                   views.data(),
                   views.size(),
                   expected);
    ExpectAllModes(ScalarKernel<std::string>(
                       std::string("b"), std::string("d"), true, false),
                   owned.data(),
                   owned.size(),
                   expected);
}

TEST(ScanKernelBinaryRange, ScalarNullRowsAreLeftToFold) {
    const std::vector<int32_t> data{1, 2, 3, 4};
    const bool valid[] = {true, false, true, false};
    const auto kernel = ScalarKernel<int32_t>(0, 10, true, true);
    const auto validity = ValidityView::FromExpanded(valid);

    const auto seq =
        RunKernel(kernel, Mode::Sequential, data.data(), data.size(), validity);
    const auto rnd =
        RunKernel(kernel, Mode::Random, data.data(), data.size(), validity);
    const auto row = RunKernel(
        kernel, Mode::RandomRowByRow, data.data(), data.size(), validity);

    // Valid rows agree across modes.
    for (size_t i : {size_t{0}, size_t{2}}) {
        EXPECT_EQ(seq[i], kT);
        EXPECT_EQ(rnd[i], kT);
        EXPECT_EQ(row[i], kT);
    }
    // NULL rows: per-row paths do not touch them; the KernelAdapter NULL fold
    // makes them UNKNOWN. The SIMD path may write match, which that fold clears.
    for (size_t i : {size_t{1}, size_t{3}}) {
        EXPECT_EQ(rnd[i], kF);
        EXPECT_EQ(row[i], kF);
    }
}

TEST(ScanKernelBinaryRange, ScalarCandidatesMask) {
    const std::vector<int32_t> data{1, 20, 3, 4};
    auto candidates = Bits({true, true, false, true});
    const auto kernel = ScalarKernel<int32_t>(0, 10, true, true);

    const auto seq = RunKernel(kernel,
                               Mode::Sequential,
                               data.data(),
                               data.size(),
                               ValidityView{},
                               &candidates);
    const auto rnd = RunKernel(kernel,
                               Mode::Random,
                               data.data(),
                               data.size(),
                               ValidityView{},
                               &candidates);
    const auto row = RunKernel(kernel,
                               Mode::RandomRowByRow,
                               data.data(),
                               data.size(),
                               ValidityView{},
                               &candidates);

    // Candidate rows agree across modes.
    for (size_t i : {size_t{0}, size_t{1}, size_t{3}}) {
        EXPECT_EQ(seq[i], rnd[i]) << i;
        EXPECT_EQ(seq[i], row[i]) << i;
    }
    EXPECT_EQ(seq[0], kT);
    EXPECT_EQ(seq[1], kF);
    EXPECT_EQ(seq[3], kT);
    // Non-candidate row: the SIMD path computes it (reset by
    // FinalizeKernelBatch); per-row paths leave it at the initial value (0,1).
    EXPECT_EQ(seq[2], kT);
    EXPECT_EQ(rnd[2], kF);
    EXPECT_EQ(row[2], kF);
}

TEST(ScanKernelBinaryRange, ClampInt8Bounds) {
    const auto bounds =
        ClampBinaryRangeBounds<int8_t>(-1000, 1000, false, false);
    EXPECT_FALSE(bounds.always_false);
    EXPECT_EQ(bounds.lower, std::numeric_limits<int8_t>::min());
    EXPECT_TRUE(bounds.lower_inclusive);
    EXPECT_EQ(bounds.upper, std::numeric_limits<int8_t>::max());
    EXPECT_TRUE(bounds.upper_inclusive);

    const std::vector<int8_t> data{-128, 0, 127};
    ExpectAllModes(BinaryRangeKernel<int8_t>::FromBounds(bounds, nullptr),
                   data.data(),
                   data.size(),
                   {kT, kT, kT});

    const auto upper_only =
        ClampBinaryRangeBounds<int16_t>(-5, 40000, true, false);
    EXPECT_FALSE(upper_only.always_false);
    EXPECT_EQ(upper_only.lower, -5);
    EXPECT_TRUE(upper_only.lower_inclusive);
    EXPECT_EQ(upper_only.upper, std::numeric_limits<int16_t>::max());
    EXPECT_TRUE(upper_only.upper_inclusive);

    const auto at_max = ClampBinaryRangeBounds<int8_t>(127, 300, true, true);
    EXPECT_FALSE(at_max.always_false);
    const std::vector<int8_t> edge{126, 127};
    ExpectAllModes(BinaryRangeKernel<int8_t>::FromBounds(at_max, nullptr),
                   edge.data(),
                   edge.size(),
                   {kF, kT});
}

TEST(ScanKernelBinaryRange, OverflowAlwaysFalse) {
    EXPECT_TRUE(
        BinaryRangeKernel<int8_t>::FromBounds(
            ClampBinaryRangeBounds<int8_t>(128, 300, true, true), nullptr)
            .AlwaysFalse());
    EXPECT_TRUE(
        BinaryRangeKernel<int8_t>::FromBounds(
            ClampBinaryRangeBounds<int8_t>(-300, -129, true, true), nullptr)
            .AlwaysFalse());
    EXPECT_TRUE(BinaryRangeKernel<int32_t>::FromBounds(
                    ClampBinaryRangeBounds<int32_t>(
                        int64_t{std::numeric_limits<int32_t>::max()} + 1,
                        std::numeric_limits<int64_t>::max(),
                        false,
                        false),
                    nullptr)
                    .AlwaysFalse());
    EXPECT_TRUE(ClampBinaryRangeBounds<int16_t>(-100000, -40000, true, true)
                    .always_false);
    EXPECT_FALSE(
        BinaryRangeKernel<int64_t>::FromBounds(
            ClampBinaryRangeBounds<int64_t>(std::numeric_limits<int64_t>::min(),
                                            std::numeric_limits<int64_t>::max(),
                                            true,
                                            true),
            nullptr)
            .AlwaysFalse());
    EXPECT_FALSE(
        BinaryRangeKernel<double>::FromBounds(
            ClampBinaryRangeBounds<double>(1e300, 2e300, true, true), nullptr)
            .AlwaysFalse());
    EXPECT_FALSE(
        BinaryRangeKernel<bool>::FromBounds(
            ClampBinaryRangeBounds<bool>(false, true, true, true), nullptr)
            .AlwaysFalse());
}

// ---------------------------------------------------------------------------
// BinaryRangeJsonKernel<ValueType>
// ---------------------------------------------------------------------------

TEST(ScanKernelBinaryRange, JsonInt64InclusiveExclusive) {
    const auto docs = Jsons({R"({"k":0})",
                             R"({"k":5})",
                             R"({"k":10})",
                             R"({"k":10.5})",
                             R"({"k":-0.5})"});
    struct Case {
        bool lower_inclusive;
        bool upper_inclusive;
        std::vector<Tri> expected;
    };
    const std::vector<Case> cases{
        {true, true, {kT, kT, kT, kF, kF}},
        {true, false, {kT, kT, kF, kF, kF}},
        {false, true, {kF, kT, kT, kF, kF}},
        {false, false, {kF, kT, kF, kF, kF}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(::testing::Message() << "li=" << c.lower_inclusive
                                          << " ui=" << c.upper_inclusive);
        ExpectAllModes(
            BinaryRangeJsonKernel<int64_t>{
                .lower = 0,
                .upper = 10,
                .lower_inclusive = c.lower_inclusive,
                .upper_inclusive = c.upper_inclusive,
                .pointer = "/k",
            },
            docs.data(),
            docs.size(),
            c.expected);
    }
}

TEST(ScanKernelBinaryRange, JsonMissingPathIsUnknown) {
    const auto docs = Jsons({R"({"k":{"v":5}})",
                             R"({"k":{"w":5}})",
                             R"({"other":5})",
                             R"({"k":null})",
                             R"({"k":{"v":null}})"});
    ExpectAllModes(
        BinaryRangeJsonKernel<int64_t>{
            .lower = 0,
            .upper = 10,
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k/v",
        },
        docs.data(),
        docs.size(),
        {kT, kU, kU, kU, kU});
    ExpectAllModes(
        BinaryRangeJsonKernel<std::string>{
            .lower = "a",
            .upper = "z",
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k/v",
        },
        docs.data(),
        docs.size(),
        {kU, kU, kU, kU, kU});
}

TEST(ScanKernelBinaryRange, JsonTypeMismatchIsUnknown) {
    const auto docs = Jsons({R"({"k":"5"})",
                             R"({"k":true})",
                             R"({"k":[5]})",
                             R"({"k":{"x":5}})",
                             R"({"k":5})",
                             R"({"k":"b"})"});
    ExpectAllModes(
        BinaryRangeJsonKernel<int64_t>{
            .lower = 0,
            .upper = 10,
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kU, kU, kU, kU, kT, kU});
    ExpectAllModes(
        BinaryRangeJsonKernel<double>{
            .lower = 0.0,
            .upper = 10.0,
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kU, kU, kU, kU, kT, kU});
    ExpectAllModes(
        BinaryRangeJsonKernel<std::string>{
            .lower = "a",
            .upper = "c",
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        // Row 0 is a string outside the range: FALSE, not UNKNOWN.
        {kF, kU, kU, kU, kU, kT});
}

TEST(ScanKernelBinaryRange, JsonNullRowAndCandidates) {
    const auto docs =
        Jsons({R"({"k":5})", R"({"k":5})", R"({"k":5})", R"({"k":5})"});
    const bool valid[] = {true, false, true, false};
    auto candidates = Bits({true, true, false, false});
    // row0 candidate+valid -> TRUE; row1 candidate+NULL -> UNKNOWN;
    // rows 2,3 non-candidate -> untouched (0,1), NULL or not.
    ExpectAllModes(
        BinaryRangeJsonKernel<int64_t>{
            .lower = 0,
            .upper = 10,
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kT, kU, kF, kF},
        ValidityView::FromExpanded(valid),
        &candidates);
}

// ---------------------------------------------------------------------------
// BinaryRangeJsonPreciseNumericKernel
// ---------------------------------------------------------------------------

TEST(ScanKernelBinaryRange, JsonPreciseNumericInt64Exact) {
    const int64_t precise = (int64_t{1} << 53) + 1;
    const auto docs = Jsons({R"({"k":9007199254740993})",
                             R"({"k":9007199254740992})",
                             R"({"x":1})",
                             R"({"k":"9007199254740993"})",
                             R"({"k":18446744073709551615})"});
    ExpectAllModes(
        BinaryRangeJsonPreciseNumericKernel{
            .lower_bound = Int64Bound(precise),
            .upper_bound = Int64Bound(precise),
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kT, kF, kU, kU, kF});
}

TEST(ScanKernelBinaryRange, JsonPreciseNumericExclusiveAndNaNBound) {
    const auto docs = Jsons({R"({"k":0})",
                             R"({"k":0.5})",
                             R"({"k":10})",
                             R"({"k":9})",
                             R"({"other":1})"});
    ExpectAllModes(
        BinaryRangeJsonPreciseNumericKernel{
            .lower_bound = FloatBound(0.0),
            .upper_bound = Int64Bound(10),
            .lower_inclusive = false,
            .upper_inclusive = false,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kF, kT, kF, kT, kU});

    // A NaN bound compares as nullopt: known FALSE (BinaryRangeExpr.cpp:299-303),
    // while a missing path stays UNKNOWN.
    ExpectAllModes(
        BinaryRangeJsonPreciseNumericKernel{
            .lower_bound = FloatBound(std::numeric_limits<double>::quiet_NaN()),
            .upper_bound = Int64Bound(10),
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kF, kF, kF, kF, kU});
}

TEST(ScanKernelBinaryRange, JsonPreciseNumericNullAndCandidates) {
    const auto docs = Jsons({R"({"k":5})", R"({"k":5})", R"({"k":5})"});
    const bool valid[] = {true, false, false};
    auto candidates = Bits({true, true, false});
    ExpectAllModes(
        BinaryRangeJsonPreciseNumericKernel{
            .lower_bound = Int64Bound(0),
            .upper_bound = Int64Bound(10),
            .lower_inclusive = true,
            .upper_inclusive = true,
            .pointer = "/k",
        },
        docs.data(),
        docs.size(),
        {kT, kU, kF},
        ValidityView::FromExpanded(valid),
        &candidates);
}

// ---------------------------------------------------------------------------
// BinaryRangeArrayKernel<ValueType>
// ---------------------------------------------------------------------------

TEST(ScanKernelBinaryRange, ArrayIndexInclusiveExclusiveAndOutOfBounds) {
    const auto rows = Int64Arrays({{1, 2, 3}, {1, 5}, {7}, {1, 9, 9}});
    struct Case {
        bool lower_inclusive;
        bool upper_inclusive;
        std::vector<Tri> expected;
    };
    const std::vector<Case> cases{
        {true, true, {kT, kT, kU, kF}},
        {true, false, {kT, kF, kU, kF}},
        {false, true, {kF, kT, kU, kF}},
        {false, false, {kF, kF, kU, kF}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(::testing::Message() << "li=" << c.lower_inclusive
                                          << " ui=" << c.upper_inclusive);
        ExpectAllModes(
            BinaryRangeArrayKernel<int64_t>{
                .lower = 2,
                .upper = 5,
                .lower_inclusive = c.lower_inclusive,
                .upper_inclusive = c.upper_inclusive,
                .index = 1,
            },
            rows.views.data(),
            rows.views.size(),
            c.expected);
    }
}

TEST(ScanKernelBinaryRange, ArrayNullRowAndCandidates) {
    const auto rows = Int64Arrays({{1, 3}, {1, 3}, {1, 3}, {1, 3}});
    const bool valid[] = {true, false, true, false};
    auto candidates = Bits({true, true, false, false});
    ExpectAllModes(
        BinaryRangeArrayKernel<int64_t>{
            .lower = 2,
            .upper = 5,
            .lower_inclusive = true,
            .upper_inclusive = true,
            .index = 1,
        },
        rows.views.data(),
        rows.views.size(),
        {kT, kU, kF, kF},
        ValidityView::FromExpanded(valid),
        &candidates);
}

TEST(ScanKernelBinaryRange, ArrayStringElements) {
    const auto rows = StringArrays({{"a", "c"}, {"a", "z"}});
    ExpectAllModes(
        BinaryRangeArrayKernel<std::string>{
            .lower = "b",
            .upper = "d",
            .lower_inclusive = true,
            .upper_inclusive = true,
            .index = 1,
        },
        rows.views.data(),
        rows.views.size(),
        {kT, kF});
}

TEST(ScanKernelBinaryRange, ArrayRequiresNestedPath) {
    const auto rows = Int64Arrays({{1, 2}});
    const BinaryRangeArrayKernel<int64_t> kernel{
        .lower = 0,
        .upper = 10,
        .lower_inclusive = true,
        .upper_inclusive = true,
        .index = -1,
    };
    EXPECT_ANY_THROW(RunKernel(
        kernel, Mode::Sequential, rows.views.data(), rows.views.size()));
}
