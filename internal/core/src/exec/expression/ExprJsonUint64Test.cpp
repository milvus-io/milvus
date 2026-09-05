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

// Tests for JSON numeric parsing, exact comparison, and candidate matching.
// Verifies that:
// 1. int64 JSON values are compared with full precision (no double round-trip).
// 2. uint64 and double JSON values fall back to double comparison, consistent
//    with the Tantivy index and JSON-stats paths.
// 3. The old static_cast<int64_t> truncation bug is eliminated.
// 4. Linear/hash strategies agree for membership and ContainsAll positions.

#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "common/Json.h"
#include "common/Types.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "exec/expression/JsonNumberComparison.h"
#include "exec/expression/UnaryExpr.h"
#include "simdjson/padded_string.h"

using namespace milvus;
using namespace milvus::exec;

// Helper: create a Json object from a raw JSON string.
static Json
makeJson(const std::string& s) {
    return Json(simdjson::padded_string(s));
}

// ═══════════════════════════════════════════════════════════════════════════════
// at_numeric() type detection
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, AtNumericUint64Detection) {
    auto j = makeJson(R"({"k": 9223372036854775808})");
    auto result = j.at_numeric("/k");
    ASSERT_FALSE(result.error());
    auto n = result.value();
    EXPECT_FALSE(n.is_int64());
    EXPECT_TRUE(n.is_uint64());
    EXPECT_EQ(n.get_uint64(), 9223372036854775808ULL);
}

TEST(JsonNumericTest, AtNumericInt64Detection) {
    auto j = makeJson(R"({"k": 9223372036854775807})");
    auto result = j.at_numeric("/k");
    ASSERT_FALSE(result.error());
    auto n = result.value();
    EXPECT_TRUE(n.is_int64());
    EXPECT_FALSE(n.is_uint64());
    EXPECT_EQ(n.get_int64(), std::numeric_limits<int64_t>::max());
}

TEST(JsonNumericTest, AtNumericNegativeIsInt64) {
    auto j = makeJson(R"({"k": -1})");
    auto result = j.at_numeric("/k");
    ASSERT_FALSE(result.error());
    auto n = result.value();
    EXPECT_TRUE(n.is_int64());
    EXPECT_EQ(n.get_int64(), -1);
}

TEST(JsonNumericTest, AtNumericDoubleDetection) {
    auto j = makeJson(R"({"k": 3.14})");
    auto result = j.at_numeric("/k");
    ASSERT_FALSE(result.error());
    auto n = result.value();
    EXPECT_TRUE(n.is_double());
    EXPECT_DOUBLE_EQ(n.get_double(), 3.14);
}

TEST(JsonNumericTest,
     MembershipAndPositionMatchersPreserveExactNumericSemantics) {
    std::vector<proto::plan::GenericValue> candidates(5);
    candidates[0].set_int64_val(2);
    candidates[1].set_float_val(2.0);
    candidates[2].set_float_val(2.5);
    candidates[3].set_int64_val(9007199254740993LL);
    candidates[4].set_float_val(9007199254740992.0);

    JsonNumberMembershipMatcher membership_matcher(candidates);
    JsonNumberCandidatePositionIndex position_index(candidates);
    auto matching_positions = [&](auto probe) {
        std::vector<size_t> positions;
        position_index.VisitMatchingPositions(
            probe, [&](size_t candidate_position) {
                positions.push_back(candidate_position);
                return false;
            });
        return positions;
    };

    EXPECT_TRUE(membership_matcher.MatchesAny(int64_t{2}));
    EXPECT_TRUE(membership_matcher.MatchesAny(2.0));
    EXPECT_TRUE(membership_matcher.MatchesAny(2.5));
    EXPECT_TRUE(membership_matcher.MatchesAny(int64_t{9007199254740993LL}));
    EXPECT_TRUE(membership_matcher.MatchesAny(9007199254740992.0));
    EXPECT_FALSE(membership_matcher.MatchesAny(int64_t{3}));
    EXPECT_EQ(matching_positions(int64_t{2}), (std::vector<size_t>{0, 1}));
    EXPECT_EQ(matching_positions(2.0), (std::vector<size_t>{0, 1}));
    EXPECT_EQ(matching_positions(2.5), (std::vector<size_t>{2}));
    EXPECT_EQ(matching_positions(int64_t{9007199254740993LL}),
              (std::vector<size_t>{3}));
    EXPECT_EQ(matching_positions(9007199254740992.0), (std::vector<size_t>{4}));

    proto::plan::GenericValue nan;
    nan.set_float_val(std::numeric_limits<double>::quiet_NaN());
    JsonNumberMembershipMatcher nan_only({nan});
    EXPECT_TRUE(nan_only.HasNumericCandidates());
    EXPECT_FALSE(nan_only.MatchesAny(std::numeric_limits<double>::quiet_NaN()));
}

TEST(JsonNumericTest, AutoStrategyUsesMatchableNumericCandidateCount) {
    auto make_int_candidates = [](size_t count) {
        std::vector<proto::plan::GenericValue> candidates(count);
        for (size_t i = 0; i < count; ++i) {
            candidates[i].set_int64_val(static_cast<int64_t>(i));
        }
        return candidates;
    };

    auto at_threshold_candidates =
        make_int_candidates(kMaxLinearScanJsonNumberCandidates);
    JsonNumberMembershipMatcher membership_at_threshold(
        at_threshold_candidates);
    JsonNumberCandidatePositionIndex positions_at_threshold(
        at_threshold_candidates);
    EXPECT_EQ(membership_at_threshold.lookup_strategy(),
              JsonNumberLookupStrategy::LINEAR_SCAN);
    EXPECT_EQ(positions_at_threshold.lookup_strategy(),
              JsonNumberLookupStrategy::LINEAR_SCAN);

    auto above_threshold_candidates =
        make_int_candidates(kMaxLinearScanJsonNumberCandidates + 1);
    JsonNumberMembershipMatcher membership_above_threshold(
        above_threshold_candidates);
    JsonNumberCandidatePositionIndex positions_above_threshold(
        above_threshold_candidates);
    EXPECT_EQ(membership_above_threshold.lookup_strategy(),
              JsonNumberLookupStrategy::HASH_LOOKUP);
    EXPECT_EQ(positions_above_threshold.lookup_strategy(),
              JsonNumberLookupStrategy::HASH_LOOKUP);

    auto candidates = make_int_candidates(kMaxLinearScanJsonNumberCandidates);
    proto::plan::GenericValue nan;
    nan.set_float_val(std::numeric_limits<double>::quiet_NaN());
    candidates.push_back(nan);
    proto::plan::GenericValue text;
    text.set_string_val("ignored");
    candidates.push_back(text);
    JsonNumberMembershipMatcher ignored_non_searchable(std::move(candidates));
    EXPECT_EQ(ignored_non_searchable.lookup_strategy(),
              JsonNumberLookupStrategy::LINEAR_SCAN);
}

TEST(JsonNumericTest, LinearScanAndHashLookupAgreeOnMatchesAndPositions) {
    std::vector<proto::plan::GenericValue> candidates(12);
    candidates[0].set_float_val(2.0);
    candidates[1].set_int64_val(2);
    candidates[2].set_float_val(2.5);
    candidates[3].set_int64_val(9007199254740993LL);
    candidates[4].set_float_val(9007199254740992.0);
    candidates[5].set_int64_val(-1);
    candidates[6].set_float_val(-1.0);
    candidates[7].set_float_val(-0.0);
    candidates[8].set_int64_val(0);
    candidates[9].set_float_val(std::numeric_limits<double>::infinity());
    candidates[10].set_int64_val(42);
    candidates[11].set_float_val(42.5);

    JsonNumberMembershipMatcher membership_linear_scan(
        candidates, JsonNumberLookupStrategy::LINEAR_SCAN);
    JsonNumberMembershipMatcher membership_hash_lookup(
        candidates, JsonNumberLookupStrategy::HASH_LOOKUP);
    JsonNumberMembershipMatcher membership_auto_selected(candidates);
    JsonNumberCandidatePositionIndex positions_linear_scan(
        candidates, JsonNumberLookupStrategy::LINEAR_SCAN);
    JsonNumberCandidatePositionIndex positions_hash_lookup(
        candidates, JsonNumberLookupStrategy::HASH_LOOKUP);
    JsonNumberCandidatePositionIndex positions_auto_selected(candidates);
    ASSERT_EQ(membership_auto_selected.lookup_strategy(),
              JsonNumberLookupStrategy::HASH_LOOKUP);
    ASSERT_EQ(positions_auto_selected.lookup_strategy(),
              JsonNumberLookupStrategy::HASH_LOOKUP);

    auto matching_positions = [](const JsonNumberCandidatePositionIndex& index,
                                 auto probe) {
        std::vector<size_t> positions;
        index.VisitMatchingPositions(probe, [&](size_t candidate_position) {
            positions.push_back(candidate_position);
            return false;
        });
        return positions;
    };
    auto expect_same = [&](auto probe) {
        EXPECT_EQ(membership_linear_scan.MatchesAny(probe),
                  membership_hash_lookup.MatchesAny(probe));
        EXPECT_EQ(membership_linear_scan.MatchesAny(probe),
                  membership_auto_selected.MatchesAny(probe));
        EXPECT_EQ(matching_positions(positions_linear_scan, probe),
                  matching_positions(positions_hash_lookup, probe));
        EXPECT_EQ(matching_positions(positions_linear_scan, probe),
                  matching_positions(positions_auto_selected, probe));
    };

    expect_same(int64_t{2});
    expect_same(2.0);
    expect_same(2.5);
    expect_same(int64_t{9007199254740993LL});
    expect_same(9007199254740992.0);
    expect_same(int64_t{-1});
    expect_same(-0.0);
    expect_same(0.0);
    expect_same(std::numeric_limits<double>::infinity());
    expect_same(std::numeric_limits<double>::quiet_NaN());
    expect_same(int64_t{999});

    EXPECT_EQ(matching_positions(positions_linear_scan, int64_t{2}),
              (std::vector<size_t>{0, 1}));
    EXPECT_EQ(matching_positions(positions_linear_scan, int64_t{-1}),
              (std::vector<size_t>{5, 6}));
    EXPECT_EQ(matching_positions(positions_linear_scan, int64_t{0}),
              (std::vector<size_t>{7, 8}));
    EXPECT_EQ(
        matching_positions(positions_linear_scan, int64_t{9007199254740993LL}),
        (std::vector<size_t>{3}));
    EXPECT_EQ(matching_positions(positions_linear_scan, 9007199254740992.0),
              (std::vector<size_t>{4}));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Verify uint64 values outside the int64 domain are compared without a
// narrowing cast.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, Uint64OutsideInt64DomainDoesNotMatchInt64Min) {
    auto json = makeJson(R"({"k": 9223372036854775808})");
    auto parsed = json.at_numeric("/k");
    ASSERT_FALSE(parsed.error());
    ASSERT_TRUE(parsed.value().is_uint64());

    proto::plan::GenericValue int64_min;
    int64_min.set_int64_val(std::numeric_limits<int64_t>::min());
    auto comparison = CompareJsonNumberToBoundWithUint64DoubleFallback(
        parsed.value(), int64_min);
    ASSERT_TRUE(comparison.has_value());
    EXPECT_GT(*comparison, 0);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Document the double fallback behavior: uint64 values are cast to double,
// which is consistent with how Tantivy and Stats store numeric JSON values.
// This means double(INT64_MAX) == double(INT64_MAX+1) due to precision limits.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, Uint64FallsBackToDouble) {
    auto j = makeJson(R"({"k": 9223372036854775808})");  // INT64_MAX + 1
    auto result = j.at_numeric("/k");
    ASSERT_FALSE(result.error());
    auto n = result.value();
    ASSERT_TRUE(n.is_uint64());

    // The fallback casts uint64 → double.
    double as_double = static_cast<double>(n.get_uint64());

    // For most int64 values, the double comparison is correct:
    int64_t small_val = 100;
    EXPECT_TRUE(as_double > small_val);    // 9.2e18 > 100 ✓
    EXPECT_FALSE(as_double < small_val);   // 9.2e18 < 100 ✗ ✓
    EXPECT_FALSE(as_double == small_val);  // 9.2e18 == 100 ✗ ✓

    // Known edge case: double(INT64_MAX) == double(INT64_MAX+1)
    // This is a shared precision limit across scan, index, and stats paths.
    int64_t max_val = std::numeric_limits<int64_t>::max();
    double max_as_double = static_cast<double>(max_val);
    EXPECT_EQ(as_double, max_as_double)
        << "double precision: INT64_MAX and INT64_MAX+1 are indistinguishable";
}

// ═══════════════════════════════════════════════════════════════════════════════
// BinaryRange with uint64 JSON values
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, BinaryRangeUint64OutsideSmallRange) {
    // uint64 values (as double) are far outside [-1000, 1000].
    const int N = 6;
    std::vector<Json> col;
    col.push_back(makeJson(R"({"k": 9223372036854775808})"));   // INT64_MAX + 1
    col.push_back(makeJson(R"({"k": 9223372036854775809})"));   // INT64_MAX + 2
    col.push_back(makeJson(R"({"k": 18446744073709551615})"));  // UINT64_MAX
    col.push_back(makeJson(R"({"k": 100})"));                   // normal int64
    col.push_back(makeJson(R"({"k": -50})"));                  // negative int64
    col.push_back(makeJson(R"({"k": 9223372036854775807})"));  // INT64_MAX

    std::string pointer = "/k";
    int64_t lo = -1000, hi = 1000;
    TargetBitmap res_bm(N, false);
    TargetBitmap valid_res_bm(N, true);
    TargetBitmap empty_bitmap{};

    BinaryRangeElementFuncForJson<int64_t, true, true> func;
    func(lo,
         hi,
         pointer,
         col.data(),
         nullptr,
         N,
         TargetBitmapView(res_bm),
         TargetBitmapView(valid_res_bm),
         empty_bitmap,
         0);

    EXPECT_FALSE(res_bm[0]) << "INT64_MAX+1 as double not in [-1000, 1000]";
    EXPECT_FALSE(res_bm[1]) << "INT64_MAX+2 as double not in [-1000, 1000]";
    EXPECT_FALSE(res_bm[2]) << "UINT64_MAX as double not in [-1000, 1000]";
    EXPECT_TRUE(res_bm[3]) << "100 in [-1000, 1000]";
    EXPECT_TRUE(res_bm[4]) << "-50 in [-1000, 1000]";
    EXPECT_FALSE(res_bm[5]) << "INT64_MAX not in [-1000, 1000]";
}

TEST(JsonNumericTest, BinaryRangeUint64WithMaxRange) {
    // With range [0, INT64_MAX]:
    // INT64_MAX+1 as double == INT64_MAX as double (precision limit),
    // so the range check [0, INT64_MAX] includes it.
    const int N = 3;
    std::vector<Json> col;
    col.push_back(makeJson(R"({"k": 9223372036854775808})"));  // INT64_MAX + 1
    col.push_back(makeJson(R"({"k": 9223372036854775807})"));  // INT64_MAX
    col.push_back(makeJson(R"({"k": 100})"));                  // normal

    std::string pointer = "/k";
    int64_t lo = 0, hi = std::numeric_limits<int64_t>::max();
    TargetBitmap res_bm(N, false);
    TargetBitmap valid_res_bm(N, true);
    TargetBitmap empty_bitmap{};

    BinaryRangeElementFuncForJson<int64_t, true, true> func;
    func(lo,
         hi,
         pointer,
         col.data(),
         nullptr,
         N,
         TargetBitmapView(res_bm),
         TargetBitmapView(valid_res_bm),
         empty_bitmap,
         0);

    // Double precision edge case: double(INT64_MAX+1) == double(INT64_MAX),
    // so INT64_MAX+1 appears to be within [0, INT64_MAX] after double cast.
    EXPECT_TRUE(res_bm[0]) << "INT64_MAX+1 as double looks like INT64_MAX";
    EXPECT_TRUE(res_bm[1]) << "INT64_MAX in [0, INT64_MAX]";
    EXPECT_TRUE(res_bm[2]) << "100 in [0, INT64_MAX]";
}

TEST(JsonNumericTest, DoubleValuesStillWorkInBinaryRange) {
    const int N = 3;
    std::vector<Json> col;
    col.push_back(makeJson(R"({"k": 1.5})"));
    col.push_back(makeJson(R"({"k": 50.0})"));
    col.push_back(makeJson(R"({"k": -10.5})"));

    std::string pointer = "/k";
    int64_t lo = 0, hi = 100;
    TargetBitmap res_bm(N, false);
    TargetBitmap valid_res_bm(N, true);
    TargetBitmap empty_bitmap{};

    BinaryRangeElementFuncForJson<int64_t, true, true> func;
    func(lo,
         hi,
         pointer,
         col.data(),
         nullptr,
         N,
         TargetBitmapView(res_bm),
         TargetBitmapView(valid_res_bm),
         empty_bitmap,
         0);

    EXPECT_TRUE(res_bm[0]) << "1.5 in [0, 100]";
    EXPECT_TRUE(res_bm[1]) << "50.0 in [0, 100]";
    EXPECT_FALSE(res_bm[2]) << "-10.5 not in [0, 100]";
}

TEST(JsonNumericTest, MixedTypeBinaryRange) {
    const int N = 7;
    std::vector<Json> col;
    col.push_back(makeJson(R"({"k": 50})"));                    // int64
    col.push_back(makeJson(R"({"k": 9223372036854775808})"));   // uint64
    col.push_back(makeJson(R"({"k": 3.14})"));                  // double
    col.push_back(makeJson(R"({"k": -999})"));                  // int64
    col.push_back(makeJson(R"({"k": 18446744073709551615})"));  // UINT64_MAX
    col.push_back(makeJson(R"({"other": 50})"));                // missing "k"
    col.push_back(makeJson(R"({"k": 100})"));                   // int64

    std::string pointer = "/k";
    int64_t lo = 0, hi = 100;
    TargetBitmap res_bm(N, false);
    TargetBitmap valid_res_bm(N, true);
    TargetBitmap empty_bitmap{};

    BinaryRangeElementFuncForJson<int64_t, true, true> func;
    func(lo,
         hi,
         pointer,
         col.data(),
         nullptr,
         N,
         TargetBitmapView(res_bm),
         TargetBitmapView(valid_res_bm),
         empty_bitmap,
         0);

    EXPECT_TRUE(res_bm[0]) << "50 in [0,100]";
    EXPECT_FALSE(res_bm[1]) << "uint64 as double not in [0,100]";
    EXPECT_TRUE(res_bm[2]) << "3.14 in [0,100]";
    EXPECT_FALSE(res_bm[3]) << "-999 not in [0,100]";
    EXPECT_FALSE(res_bm[4]) << "UINT64_MAX as double not in [0,100]";
    EXPECT_FALSE(res_bm[5]) << "missing field not in [0,100]";
    EXPECT_TRUE(res_bm[6]) << "100 in [0,100]";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Int64 precision: the main improvement of at_numeric() over at<double>()
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, Int64PrecisionPreservedInBinaryRange) {
    // Two int64 values that are adjacent but indistinguishable as double:
    // double(2^53 + 1) == double(2^53) for odd values near the boundary.
    int64_t precise_val = (1LL << 53) + 1;  // 9007199254740993
    int64_t adjacent = (1LL << 53);         // 9007199254740992

    // Confirm these are distinct as int64 but equal as double.
    ASSERT_NE(precise_val, adjacent);
    ASSERT_EQ(static_cast<double>(precise_val), static_cast<double>(adjacent));

    const int N = 2;
    std::vector<Json> col;
    col.push_back(makeJson(R"({"k": 9007199254740993})"));  // 2^53 + 1
    col.push_back(makeJson(R"({"k": 9007199254740992})"));  // 2^53

    std::string pointer = "/k";
    // Range: [2^53 + 1, 2^53 + 1] — only exact match should pass.
    int64_t lo = precise_val, hi = precise_val;
    TargetBitmap res_bm(N, false);
    TargetBitmap valid_res_bm(N, true);
    TargetBitmap empty_bitmap{};

    BinaryRangeElementFuncForJson<int64_t, true, true> func;
    func(lo,
         hi,
         pointer,
         col.data(),
         nullptr,
         N,
         TargetBitmapView(res_bm),
         TargetBitmapView(valid_res_bm),
         empty_bitmap,
         0);

    // at_numeric() preserves int64 precision: these are compared as int64.
    // With the old double fallback, both would match since double can't
    // distinguish them. With at_numeric(), only the exact match passes.
    EXPECT_TRUE(res_bm[0]) << "exact int64 match: 2^53+1 in [2^53+1, 2^53+1]";
    EXPECT_FALSE(res_bm[1]) << "int64 precision: 2^53 not in [2^53+1, 2^53+1]";
}

// Verify the old double-only approach would fail this precision test.
TEST(JsonNumericTest, OldDoublePathLosesPrecision) {
    // Simulates what the OLD code did: at<int64_t>() → succeed → cast to
    // double for range check.  Two adjacent int64 values become equal.
    int64_t a = (1LL << 53) + 1;  // 9007199254740993
    int64_t b = (1LL << 53);      // 9007199254740992
    double da = static_cast<double>(a);
    double db = static_cast<double>(b);
    // Old code would compare as double — can't distinguish these:
    EXPECT_EQ(da, db) << "double loses precision at 2^53 boundary";

    // New code compares as int64 — correctly distinguishes:
    EXPECT_NE(a, b) << "int64 preserves full precision";
}

// ═══════════════════════════════════════════════════════════════════════════════
// Double fallback precision: document the known INT64_MAX boundary edge case.
// This edge case is shared by all three paths (scan, index, stats).
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, DoubleFallbackPrecisionBoundary) {
    // JSON uint64 value = INT64_MAX + 1.  After double fallback:
    // double(INT64_MAX)   = 9223372036854775808.0  (rounds up)
    // double(INT64_MAX+1) = 9223372036854775808.0  (exact 2^63)
    // They are indistinguishable as double.
    int64_t max_val = std::numeric_limits<int64_t>::max();
    uint64_t over_val = static_cast<uint64_t>(max_val) + 1;
    EXPECT_EQ(static_cast<double>(max_val), static_cast<double>(over_val))
        << "double can't distinguish INT64_MAX and INT64_MAX+1";

    // This means:
    // - BinaryRange [0, INT64_MAX]: uint64 INT64_MAX+1 appears IN range (false positive)
    // - UnaryRange > INT64_MAX: uint64 INT64_MAX+1 appears NOT greater (false negative)
    //
    // These are known precision limits, consistent across scan/index/stats.
    // Only affects operands within ~1000 of INT64_MAX.

    // Verify: values far from the boundary are correctly handled.
    int64_t normal_val = 1000;
    double uint64_as_double = static_cast<double>(over_val);  // ≈ 9.22e18
    EXPECT_TRUE(uint64_as_double > normal_val);
    EXPECT_TRUE(uint64_as_double > 0);
    EXPECT_FALSE(uint64_as_double < normal_val);
}

// ═══════════════════════════════════════════════════════════════════════════════
// TermExpr (IN) numeric contract: compare the parsed number directly to the
// query literal, without converting the data to int64.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, TermComparisonWithAtNumeric) {
    auto check_in_terms =
        [](const Json& j, std::string_view pointer, int64_t term) -> bool {
        auto x_num = j.at_numeric(pointer);
        if (x_num.error())
            return false;
        proto::plan::GenericValue bound;
        bound.set_int64_val(term);
        auto comparison = CompareJsonNumberToBoundWithUint64DoubleFallback(
            x_num.value(), bound);
        return comparison.has_value() && *comparison == 0;
    };

    // int64 value: exact match
    EXPECT_TRUE(check_in_terms(makeJson(R"({"k": 42})"), "/k", 42));
    EXPECT_FALSE(check_in_terms(makeJson(R"({"k": 42})"), "/k", 43));

    // double integer-valued: 50.0 matches term 50
    EXPECT_TRUE(check_in_terms(makeJson(R"({"k": 50.0})"), "/k", 50));

    // double non-integer: 1.5 does NOT match term 1 (floor check fails)
    EXPECT_FALSE(check_in_terms(makeJson(R"({"k": 1.5})"), "/k", 1));

    // uint64 > INT64_MAX cannot alias an int64 term, including INT64_MIN.
    EXPECT_FALSE(
        check_in_terms(makeJson(R"({"k": 9223372036854775808})"), "/k", 100));
    EXPECT_FALSE(check_in_terms(makeJson(R"({"k": 9223372036854775808})"),
                                "/k",
                                std::numeric_limits<int64_t>::min()));

    EXPECT_TRUE(check_in_terms(
        makeJson(R"({"k": 9007199254740993})"), "/k", 9007199254740993LL));
    EXPECT_FALSE(check_in_terms(
        makeJson(R"({"k": 9007199254740993})"), "/k", 9007199254740992LL));

    // Missing field: error → false
    EXPECT_FALSE(check_in_terms(makeJson(R"({"x": 1})"), "/k", 1));
}

// ═══════════════════════════════════════════════════════════════════════════════
// Arithmetic expression logic: verify json_v + operand works correctly
// after at_numeric() for both int64 and double branches.
// ═══════════════════════════════════════════════════════════════════════════════

TEST(JsonNumericTest, ArithmeticExprWithAtNumeric) {
    // Simulates: json["k"] + right_operand == val
    auto check_arith_eq = [](const Json& j,
                             std::string_view pointer,
                             int64_t right_operand,
                             int64_t val) -> bool {
        auto x_num = j.at_numeric(pointer);
        if (x_num.error())
            return false;
        auto n = x_num.value();
        if (n.is_int64()) {
            auto json_v = n.get_int64();
            return json_v + right_operand == val;
        }
        auto json_v = n.is_uint64() ? static_cast<double>(n.get_uint64())
                                    : n.get_double();
        return json_v + right_operand == val;
    };

    // int64: 40 + 2 == 42
    EXPECT_TRUE(check_arith_eq(makeJson(R"({"k": 40})"), "/k", 2, 42));
    EXPECT_FALSE(check_arith_eq(makeJson(R"({"k": 40})"), "/k", 2, 43));

    // double: 1.5 + 2 == 3.5  (int val 3 won't match)
    EXPECT_FALSE(check_arith_eq(makeJson(R"({"k": 1.5})"), "/k", 2, 3));

    // int64 precision: 2^53+1 + 0 == 2^53+1 (exact as int64)
    int64_t big = (1LL << 53) + 1;
    std::string json_str = R"({"k": )" + std::to_string(big) + "}";
    EXPECT_TRUE(check_arith_eq(makeJson(json_str), "/k", 0, big));
    // 2^53 + 0 != 2^53+1 (would wrongly match with double)
    EXPECT_FALSE(
        check_arith_eq(makeJson(R"({"k": 9007199254740992})"), "/k", 0, big));
}
