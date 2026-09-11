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

#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/Expr.h"
#include "exec/expression/GISFunctionFilterExpr.h"
#include "exec/expression/MembershipFilterExpr.h"
#include "exec/expression/TimestamptzArithCompareExpr.h"
#include "pb/plan.pb.h"
#include "simdjson/padded_string.h"

namespace milvus::exec {
namespace {

enum class Tri { False, True, Unknown };

constexpr Tri kT = Tri::True;
constexpr Tri kF = Tri::False;
constexpr Tri kU = Tri::Unknown;

std::ostream&
operator<<(std::ostream& os, Tri t) {
    return os << (t == Tri::True ? "T" : t == Tri::False ? "F" : "U");
}

// Output buffers initialized the way EvalKernel initializes them: match=0,
// known=1.
struct OutBuffers {
    explicit OutBuffers(size_t n) : match(n, false), known(n, true) {
    }

    TriStateOut
    View() {
        return TriStateOut{TargetBitmapView(match), TargetBitmapView(known)};
    }

    TriStateOut
    RowView(size_t i) {
        return TriStateOut{TargetBitmapView(match.data(), i, 1),
                           TargetBitmapView(known.data(), i, 1)};
    }

    Tri
    At(size_t i) const {
        if (!known[i]) {
            EXPECT_FALSE(match[i]) << "row " << i << " is unknown but matches";
            return Tri::Unknown;
        }
        return match[i] ? Tri::True : Tri::False;
    }

    TargetBitmap match;
    TargetBitmap known;
};

// Runs the kernel once over the whole batch as Eval<sequential>, and once per
// row as Eval<random>; both must agree. Returns the kernel's pre-fold output.
template <typename K, typename T>
std::vector<Tri>
RunKernel(const K& kernel,
          const std::vector<T>& data,
          const bool* valid,
          TargetBitmap* candidates,
          const int32_t* segment_offsets = nullptr) {
    const size_t n = data.size();

    OutBuffers seq(n);
    CandidateBatch<T> batch{
        data.data(),
        valid != nullptr ? ValidityView::FromExpanded(valid) : ValidityView{},
        candidates != nullptr ? TargetBitmapView(*candidates)
                              : TargetBitmapView{},
        segment_offsets,
        n};
    kernel.template Eval<FilterType::sequential>(batch, seq.View());

    OutBuffers rnd(n);
    for (size_t i = 0; i < n; ++i) {
        CandidateBatch<T> row{
            &data[i],
            valid != nullptr ? ValidityView::FromExpanded(valid + i)
                             : ValidityView{},
            candidates != nullptr ? TargetBitmapView(candidates->data(), i, 1)
                                  : TargetBitmapView{},
            segment_offsets != nullptr ? segment_offsets + i : nullptr,
            1};
        kernel.template Eval<FilterType::random>(row, rnd.RowView(i));
    }

    std::vector<Tri> result(n);
    for (size_t i = 0; i < n; ++i) {
        result[i] = seq.At(i);
        EXPECT_EQ(result[i], rnd.At(i)) << "sequential vs random, row " << i;
    }
    return result;
}

// Spec 4.4 steps 1-3 at the three-valued level.
std::vector<Tri>
FoldLikeScan(std::vector<Tri> out,
             const bool* valid,
             const TargetBitmap* candidates,
             bool null_rows_known_false = false) {
    for (size_t i = 0; i < out.size(); ++i) {
        if (!null_rows_known_false && valid != nullptr && !valid[i]) {
            out[i] = Tri::Unknown;
        }
        if (candidates != nullptr && !(*candidates)[i]) {
            out[i] = Tri::False;
        }
    }
    return out;
}

std::vector<milvus::Json>
MakeJsonRows(const std::vector<std::string>& rows) {
    std::vector<milvus::Json> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        out.emplace_back(simdjson::padded_string(row));
    }
    return out;
}

// ---------------------------------------------------------------- Exists

TEST(ScanKernelSmallTest, ExistsNullRowStaysKnownFalse) {
    // Row 2 is NULL; its payload has the key, so evaluating it would say TRUE.
    const auto rows = MakeJsonRows(
        {R"({"a": 1})", R"({"b": 1})", R"({"a": 1})", R"({"a": "x"})"});
    const bool valid[] = {true, true, false, true};
    const ExistsKernel kernel{milvus::Json::pointer({"a"})};

    const auto raw = RunKernel(kernel, rows, valid, nullptr);
    EXPECT_EQ(raw, (std::vector<Tri>{kT, kF, kF, kT}));

    ASSERT_TRUE(ExistsKernel::kNullRowsKnownFalse);
    EXPECT_EQ(FoldLikeScan(raw, valid, nullptr, true),
              (std::vector<Tri>{kT, kF, kF, kT}));
}

TEST(ScanKernelSmallTest, ExistsSkipsPrunedCandidates) {
    const auto rows = MakeJsonRows(
        {R"({"a": 1})", R"({"a": 1})", R"({"a": 1})", R"({"a": 1})"});
    const bool valid[] = {true, true, false, true};
    TargetBitmap candidates(4, true);
    candidates[1] = false;
    candidates[2] = false;
    const ExistsKernel kernel{milvus::Json::pointer({"a"})};

    const auto raw = RunKernel(kernel, rows, valid, &candidates);
    EXPECT_EQ(raw, (std::vector<Tri>{kT, kF, kF, kT}));
    EXPECT_EQ(FoldLikeScan(raw, valid, &candidates, true),
              (std::vector<Tri>{kT, kF, kF, kT}));
}

// ------------------------------------------------------- Timestamptz arith

TEST(ScanKernelSmallTest, TimestamptzSkipsNullAndPrunedPlaceholders) {
    proto::plan::Interval one_month;
    one_month.set_months(1);
    constexpr int64_t kUsPerDay = 86400LL * 1000000;
    const int64_t feb_1st = 31 * kUsPerDay;  // 1970-01-01 + 1 month
    const TimestamptzArithCompareKernel kernel{proto::plan::ArithOpType::Add,
                                               proto::plan::OpType::LessThan,
                                               &one_month,
                                               feb_1st + 16};
    const int64_t kOverflow = std::numeric_limits<int64_t>::max();

    // A placeholder that overflows interval arithmetic must actually throw,
    // otherwise the skip assertions below prove nothing.
    EXPECT_ANY_THROW(
        RunKernel(kernel, std::vector<int64_t>{kOverflow}, nullptr, nullptr));

    // Row 2: NULL with an overflowing placeholder. Row 3: pruned overflow.
    const std::vector<int64_t> data = {0, 20, kOverflow, kOverflow, 16, 15};
    const bool valid[] = {true, true, false, true, true, true};
    TargetBitmap candidates(6, true);
    candidates[3] = false;

    const auto raw = RunKernel(kernel, data, valid, &candidates);
    EXPECT_EQ(raw, (std::vector<Tri>{kT, kF, kF, kF, kF, kT}));
    EXPECT_EQ(FoldLikeScan(raw, valid, &candidates),
              (std::vector<Tri>{kT, kF, kU, kF, kF, kT}));
}

TEST(ScanKernelSmallTest, TimestamptzUnknownArithComparesDirectly) {
    proto::plan::Interval ignored;
    ignored.set_months(1);
    const TimestamptzArithCompareKernel kernel{
        proto::plan::ArithOpType::Unknown,
        proto::plan::OpType::GreaterEqual,
        &ignored,
        16};
    EXPECT_EQ(RunKernel(kernel, std::vector<int64_t>{15, 16}, nullptr, nullptr),
              (std::vector<Tri>{kF, kT}));
}

// -------------------------------------------------------------- Membership

struct FakeProbe {
    std::set<int64_t> ints;
    std::set<std::string, std::less<>> strings;

    template <typename V>
    bool
    operator()(const V& v) const {
        if constexpr (std::is_same_v<V, std::string> ||
                      std::is_same_v<V, std::string_view>) {
            return strings.count(std::string_view(v)) != 0;
        } else {
            return ints.count(static_cast<int64_t>(v)) != 0;
        }
    }

    bool
    TestBytesValue(const void* data, size_t len) const {
        return strings.count(
                   std::string_view(static_cast<const char*>(data), len)) != 0;
    }

    bool
    TestInt64Value(int64_t v) const {
        return ints.count(v) != 0;
    }
};

TEST(ScanKernelSmallTest, MembershipScalarNullAndPrunedRows) {
    const FakeProbe probe{{7, -1}, {}};
    const MembershipScalarKernel<int64_t, FakeProbe> kernel{&probe};

    // Row 2: NULL member. Row 3: pruned NULL member.
    const std::vector<int64_t> data = {7, 8, 7, 7, -1};
    const bool valid[] = {true, true, false, false, true};
    TargetBitmap candidates(5, true);
    candidates[3] = false;

    const auto raw = RunKernel(kernel, data, valid, &candidates);
    EXPECT_EQ(raw, (std::vector<Tri>{kT, kF, kF, kF, kT}));
    // Pruned NULL stays (false, valid): the pinned
    // ScalarBitmapInputLeavesExcludedNullCandidatesUntouched contract.
    EXPECT_EQ(FoldLikeScan(raw, valid, &candidates),
              (std::vector<Tri>{kT, kF, kU, kF, kT}));
}

TEST(ScanKernelSmallTest, MembershipScalarStringView) {
    const FakeProbe probe{{}, {"x"}};
    const MembershipScalarKernel<std::string_view, FakeProbe> kernel{&probe};
    const std::vector<std::string_view> data = {"x", "y"};
    EXPECT_EQ(RunKernel(kernel, data, nullptr, nullptr),
              (std::vector<Tri>{kT, kF}));
}

TEST(ScanKernelSmallTest, MembershipJsonTypedProbe) {
    const FakeProbe probe{{5}, {"x"}};
    const MembershipJsonKernel<FakeProbe> kernel{
        &probe, milvus::Json::pointer({"uid"})};

    const auto rows = MakeJsonRows({
        R"({"uid": "x"})",   // 0 string member
        R"({"uid": 5})",     // 1 int64 member
        R"({"uid": 5.5})",   // 2 other number: FALSE, known
        R"({"uid": true})",  // 3 bool: UNKNOWN
        R"({"other": 1})",   // 4 missing key: UNKNOWN
        R"({"uid": null})",  // 5 JSON null: UNKNOWN
        R"({"uid": 5})",     // 6 whole-row NULL
        R"({"uid": 5})",     // 7 pruned
    });
    const bool valid[] = {true, true, true, true, true, true, false, true};
    TargetBitmap candidates(8, true);
    candidates[7] = false;

    const auto raw = RunKernel(kernel, rows, valid, &candidates);
    EXPECT_EQ(raw, (std::vector<Tri>{kT, kT, kF, kU, kU, kU, kF, kF}));
    EXPECT_EQ(FoldLikeScan(raw, valid, &candidates),
              (std::vector<Tri>{kT, kT, kF, kU, kU, kU, kU, kF}));
}

// --------------------------------------------------------------------- GIS

TEST(ScanKernelSmallTest, GeometryWkbAndCacheBranchesAgree) {
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();
    const Geometry square(ctx, "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))");
    const std::string inside = Geometry(ctx, "POINT(1 1)").to_wkb_string();
    const std::string outside = Geometry(ctx, "POINT(5 5)").to_wkb_string();
    const std::string corrupt = "not-wkb";

    // Row 2: NULL whose payload intersects. Row 3: corrupt WKB. Row 4: pruned.
    const std::vector<std::string> wkb = {
        inside, outside, inside, corrupt, inside, inside};
    const bool valid[] = {true, true, false, true, true, true};
    TargetBitmap candidates(6, true);
    candidates[4] = false;
    const std::vector<int32_t> segment_offsets = {100, 101, 102, 103, 104, 105};
    const std::vector<Tri> expected_raw = {kT, kF, kF, kF, kF, kT};
    const std::vector<Tri> expected_folded = {kT, kF, kU, kF, kF, kT};

    ASSERT_TRUE(GeometryScanKernel<std::string>::kNeedsSegmentOffsets);

    const GeometryScanKernel<std::string> wkb_kernel{
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        &square,
        0.0,
        nullptr};
    auto raw =
        RunKernel(wkb_kernel, wkb, valid, &candidates, segment_offsets.data());
    EXPECT_EQ(raw, expected_raw);
    EXPECT_EQ(FoldLikeScan(raw, valid, &candidates), expected_folded);

    // Cache branch resolves rows by absolute segment offset; `data` is unused.
    auto cache = std::make_shared<SimpleGeometryCache>();
    for (size_t i = 0; i < wkb.size(); ++i) {
        cache->AppendDataAt(segment_offsets[i], wkb[i].data(), wkb[i].size());
    }
    const std::vector<std::string_view> unused(wkb.size(), "unused");
    const GeometryScanKernel<std::string_view> cache_kernel{
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        &square,
        0.0,
        cache};
    raw = RunKernel(
        cache_kernel, unused, valid, &candidates, segment_offsets.data());
    EXPECT_EQ(raw, expected_raw);
}

TEST(ScanKernelSmallTest, GeometryIsValidNeedsNoQueryGeometry) {
    GEOSContextHandle_t ctx = GetThreadLocalGEOSContext();
    const std::vector<std::string> wkb = {
        Geometry(ctx, "POINT(1 1)").to_wkb_string(), "not-wkb"};
    const std::vector<int32_t> segment_offsets = {0, 1};
    const GeometryScanKernel<std::string> kernel{
        proto::plan::GISFunctionFilterExpr_GISOp_STIsValid,
        nullptr,
        0.0,
        nullptr};
    EXPECT_EQ(RunKernel(kernel, wkb, nullptr, nullptr, segment_offsets.data()),
              (std::vector<Tri>{kT, kF}));
}

}  // namespace
}  // namespace milvus::exec
