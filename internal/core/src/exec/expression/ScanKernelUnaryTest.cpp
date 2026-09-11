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
#include <string>
#include <string_view>
#include <vector>

#include <simdjson.h>

#include "common/Array.h"
#include "common/Json.h"
#include "common/RegexQuery.h"
#include "common/Volnitsky.h"
#include "exec/expression/UnaryExpr.h"
#include "index/SkipIndex.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

using namespace milvus;
using namespace milvus::exec;

namespace {

enum class Tri { True, False, Unknown };

std::ostream&
operator<<(std::ostream& os, Tri t) {
    return os << (t == Tri::True    ? "TRUE"
                  : t == Tri::False ? "FALSE"
                                    : "UNKNOWN");
}

constexpr auto kBothModes = {FilterType::sequential, FilterType::random};

template <typename K, typename Container>
std::vector<Tri>
RunKernel(FilterType mode,
          const K& kernel,
          const Container& data,
          const FixedVector<bool>& valid = {},
          const FixedVector<bool>& candidates = {}) {
    using T = typename Container::value_type;
    const size_t n = data.size();
    TargetBitmap match(n, false);
    TargetBitmap known(n, true);
    TargetBitmap candidate_bits(candidates.size(), false);
    for (size_t i = 0; i < candidates.size(); ++i) {
        candidate_bits[i] = candidates[i];
    }
    TargetBitmapView match_view(match);
    TargetBitmapView known_view(known);
    TargetBitmapView candidate_view = candidates.empty()
                                          ? TargetBitmapView{}
                                          : TargetBitmapView(candidate_bits);
    const ValidityView validity =
        valid.empty() ? ValidityView{}
                      : ValidityView::FromExpanded(valid.data());

    if (mode == FilterType::sequential) {
        CandidateBatch<T> batch{.data = data.data(),
                                .validity = validity,
                                .candidates = candidate_view,
                                .segment_offsets = nullptr,
                                .size = n};
        kernel.template Eval<FilterType::sequential>(
            batch, TriStateOut{.match = match_view, .known = known_view});
    } else {
        for (size_t i = 0; i < n; ++i) {
            CandidateBatch<T> batch{
                .data = data.data() + i,
                .validity =
                    valid.empty() ? ValidityView{} : validity.Subview(i),
                .candidates = candidates.empty() ? TargetBitmapView{}
                                                 : candidate_view.view(i, 1),
                .segment_offsets = nullptr,
                .size = 1};
            kernel.template Eval<FilterType::random>(
                batch,
                TriStateOut{.match = match_view.view(i, 1),
                            .known = known_view.view(i, 1)});
        }
    }

    // Spec 4.4 fold, steps 1-3.
    std::vector<Tri> result(n);
    for (size_t i = 0; i < n; ++i) {
        bool k = known[i] && (valid.empty() || valid[i]);
        bool m = match[i] && k;
        if (!candidates.empty() && !candidates[i]) {
            m = false;
            k = true;
        }
        result[i] = !k ? Tri::Unknown : (m ? Tri::True : Tri::False);
    }
    return result;
}

Json
MakeJson(const std::string& text) {
    return Json(simdjson::padded_string(text));
}

proto::plan::Array
MakeInt64Literal(std::initializer_list<int64_t> values) {
    proto::plan::Array arr;
    arr.set_same_type(true);
    for (auto v : values) {
        arr.add_array()->set_int64_val(v);
    }
    return arr;
}

Array
MakeInt64Array(std::initializer_list<int64_t> values) {
    proto::schema::ScalarField field;
    for (auto v : values) {
        field.mutable_long_data()->add_data(v);
    }
    return Array(field);
}

Array
MakeStringArray(std::initializer_list<std::string> values) {
    proto::schema::ScalarField field;
    for (const auto& v : values) {
        field.mutable_string_data()->add_data(v);
    }
    return Array(field);
}

ArrayView
ViewOf(const Array& array) {
    return ArrayView(const_cast<char*>(array.data()),
                     array.length(),
                     array.byte_size(),
                     array.get_element_type(),
                     array.get_offsets_data());
}

}  // namespace

// ---------------------------------------------------------------------------
// UnaryRangeKernel<T>
// ---------------------------------------------------------------------------

TEST(ScanKernelUnaryTest, ScalarCompareOps) {
    const std::vector<int64_t> data = {1, 5, 10, 15};
    struct Case {
        proto::plan::OpType op;
        std::vector<Tri> expected;
    };
    const std::vector<Case> cases = {
        {proto::plan::GreaterThan,
         {Tri::False, Tri::False, Tri::False, Tri::True}},
        {proto::plan::GreaterEqual,
         {Tri::False, Tri::False, Tri::True, Tri::True}},
        {proto::plan::LessThan, {Tri::True, Tri::True, Tri::False, Tri::False}},
        {proto::plan::LessEqual, {Tri::True, Tri::True, Tri::True, Tri::False}},
        {proto::plan::Equal, {Tri::False, Tri::False, Tri::True, Tri::False}},
        {proto::plan::NotEqual, {Tri::True, Tri::True, Tri::False, Tri::True}},
    };
    for (auto mode : kBothModes) {
        for (const auto& c : cases) {
            UnaryRangeKernel<int64_t> kernel{.op = c.op, .val = 10};
            EXPECT_EQ(RunKernel(mode, kernel, data), c.expected)
                << "op=" << c.op << " mode=" << static_cast<int>(mode);
        }
    }
}

TEST(ScanKernelUnaryTest, ScalarDoubleAndBool) {
    const std::vector<double> doubles = {-1.5, 0.0, 2.25};
    const FixedVector<bool> bools = {true, false, true};
    for (auto mode : kBothModes) {
        UnaryRangeKernel<double> gt{.op = proto::plan::GreaterThan, .val = 0.0};
        EXPECT_EQ(RunKernel(mode, gt, doubles),
                  (std::vector<Tri>{Tri::False, Tri::False, Tri::True}));
        UnaryRangeKernel<bool> eq{.op = proto::plan::Equal, .val = true};
        EXPECT_EQ(RunKernel(mode, eq, bools),
                  (std::vector<Tri>{Tri::True, Tri::False, Tri::True}));
    }
}

TEST(ScanKernelUnaryTest, ScalarNullRowsFoldToUnknown) {
    const std::vector<int32_t> data = {20, 20, 1, 1};
    const FixedVector<bool> valid = {true, false, true, false};
    for (auto mode : kBothModes) {
        UnaryRangeKernel<int32_t> kernel{.op = proto::plan::GreaterThan,
                                         .val = 10};
        EXPECT_EQ(RunKernel(mode, kernel, data, valid),
                  (std::vector<Tri>{
                      Tri::True, Tri::Unknown, Tri::False, Tri::Unknown}));
    }
}

TEST(ScanKernelUnaryTest, ScalarCandidatesMask) {
    const std::vector<int64_t> data = {20, 20, 1, 20};
    const FixedVector<bool> valid = {true, false, true, false};
    const FixedVector<bool> candidates = {true, false, true, false};
    for (auto mode : kBothModes) {
        UnaryRangeKernel<int64_t> kernel{.op = proto::plan::GreaterThan,
                                         .val = 10};
        // Non-candidate rows fold to (0,1) even when NULL.
        EXPECT_EQ(
            RunKernel(mode, kernel, data, valid, candidates),
            (std::vector<Tri>{Tri::True, Tri::False, Tri::False, Tri::False}));
    }
}

TEST(ScanKernelUnaryTest, OverflowClassification) {
    auto lit = [](int64_t v) {
        proto::plan::GenericValue g;
        g.set_int64_val(v);
        return g;
    };
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::Equal, lit(300)),
              UnaryOverflow::AllFalse);
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::NotEqual, lit(300)),
              UnaryOverflow::AllTrue);
    EXPECT_EQ(
        ClassifyUnaryOverflow<int8_t>(proto::plan::GreaterThan, lit(-300)),
        UnaryOverflow::AllTrue);
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::GreaterThan, lit(300)),
              UnaryOverflow::AllFalse);
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::LessEqual, lit(300)),
              UnaryOverflow::AllTrue);
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::LessEqual, lit(-300)),
              UnaryOverflow::AllFalse);
    EXPECT_EQ(ClassifyUnaryOverflow<int8_t>(proto::plan::Equal, lit(100)),
              UnaryOverflow::None);
    EXPECT_EQ(ClassifyUnaryOverflow<int64_t>(proto::plan::Equal, lit(300)),
              UnaryOverflow::None);
}

TEST(ScanKernelUnaryTest, OverflowKernel) {
    const std::vector<int8_t> data = {0, 0, 0};
    const FixedVector<bool> valid = {true, false, true};

    UnaryRangeKernel<int8_t> all_false{.op = proto::plan::Equal,
                                       .overflow = UnaryOverflow::AllFalse};
    EXPECT_TRUE(all_false.AlwaysFalse());

    UnaryRangeKernel<int8_t> all_true{.op = proto::plan::NotEqual,
                                      .overflow = UnaryOverflow::AllTrue};
    EXPECT_FALSE(all_true.AlwaysFalse());
    EXPECT_TRUE(all_true.AlwaysTrue());
    EXPECT_FALSE(all_false.AlwaysTrue());
    // val is the T{} sentinel (0); comparing it would yield FALSE here.
    for (auto mode : kBothModes) {
        EXPECT_EQ(RunKernel(mode, all_true, data, valid),
                  (std::vector<Tri>{Tri::True, Tri::Unknown, Tri::True}));
    }
    // Must not consult SkipIndex with the sentinel. SkipIndex has no
    // user-declared constructor (index/SkipIndex.h:164); CanSkip returns before
    // touching it.
    SkipIndex skip_index;
    EXPECT_FALSE(all_true.CanSkip(skip_index, FieldId(100), 0));
    EXPECT_FALSE(all_false.CanSkip(skip_index, FieldId(100), 0));
}

TEST(ScanKernelUnaryTest, StringOps) {
    const std::vector<std::string_view> data = {"apple", "banana", "cherry"};
    const LikePatternMatcher like("%an%");
    const PartialRegexMatcher regex("an+a");
    const VolnitskySearcher volnitsky("an");
    struct Case {
        proto::plan::OpType op;
        std::string val;
        std::vector<Tri> expected;
    };
    const std::vector<Case> cases = {
        {proto::plan::Equal, "banana", {Tri::False, Tri::True, Tri::False}},
        {proto::plan::NotEqual, "banana", {Tri::True, Tri::False, Tri::True}},
        {proto::plan::GreaterThan,
         "banana",
         {Tri::False, Tri::False, Tri::True}},
        {proto::plan::LessEqual, "banana", {Tri::True, Tri::True, Tri::False}},
        {proto::plan::PrefixMatch, "ba", {Tri::False, Tri::True, Tri::False}},
        {proto::plan::PostfixMatch, "rry", {Tri::False, Tri::False, Tri::True}},
        {proto::plan::InnerMatch, "pl", {Tri::True, Tri::False, Tri::False}},
        {proto::plan::Match, "%an%", {Tri::False, Tri::True, Tri::False}},
        {proto::plan::RegexMatch, "an+a", {Tri::False, Tri::True, Tri::False}},
    };
    for (auto mode : kBothModes) {
        for (const auto& c : cases) {
            UnaryRangeKernel<std::string_view> kernel{.op = c.op,
                                                      .val = c.val,
                                                      .like_matcher = &like,
                                                      .regex_matcher = &regex};
            EXPECT_EQ(RunKernel(mode, kernel, data), c.expected)
                << "op=" << c.op;
        }
        UnaryRangeKernel<std::string_view> with_literal{
            .op = proto::plan::RegexMatch,
            .val = "an+a",
            .regex_matcher = &regex,
            .volnitsky = &volnitsky};
        EXPECT_EQ(RunKernel(mode, with_literal, data),
                  (std::vector<Tri>{Tri::False, Tri::True, Tri::False}));
    }
}

TEST(ScanKernelUnaryTest, OwningStringCandidatesAndNull) {
    const std::vector<std::string> data = {"banana", "banana", "banana"};
    const FixedVector<bool> valid = {true, false, true};
    const FixedVector<bool> candidates = {true, true, false};
    for (auto mode : kBothModes) {
        UnaryRangeKernel<std::string> kernel{.op = proto::plan::Equal,
                                             .val = "banana"};
        EXPECT_EQ(RunKernel(mode, kernel, data, valid, candidates),
                  (std::vector<Tri>{Tri::True, Tri::Unknown, Tri::False}));
    }
}

TEST(ScanKernelUnaryTest, NonStringPatternOpsThrow) {
    const std::vector<int64_t> data = {1};
    UnaryRangeKernel<int64_t> like{.op = proto::plan::Match, .val = 1};
    EXPECT_ANY_THROW(RunKernel(FilterType::sequential, like, data));
    UnaryRangeKernel<int64_t> regex{.op = proto::plan::RegexMatch, .val = 1};
    EXPECT_ANY_THROW(RunKernel(FilterType::random, regex, data));
}

// ---------------------------------------------------------------------------
// UnaryJsonKernel / UnaryJsonPreciseNumericKernel
// ---------------------------------------------------------------------------

TEST(ScanKernelUnaryTest, JsonInt64MissingAndTypeMismatchAreUnknown) {
    const std::vector<Json> data = {
        MakeJson(R"({"a":5})"),
        MakeJson(R"({"a":"x"})"),  // type mismatch
        MakeJson(R"({"b":1})"),    // missing path
        MakeJson(R"({"a":15})"),
        MakeJson(R"({})"),                          // NULL row
        MakeJson(R"({"a":18446744073709551615})"),  // uint64 -> double compare
    };
    const FixedVector<bool> valid = {true, true, true, true, false, true};
    for (auto mode : kBothModes) {
        UnaryJsonKernel<int64_t> kernel{
            .op = proto::plan::GreaterThan, .val = 10, .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, kernel, data, valid),
                  (std::vector<Tri>{Tri::False,
                                    Tri::Unknown,
                                    Tri::Unknown,
                                    Tri::True,
                                    Tri::Unknown,
                                    Tri::True}));
    }
}

TEST(ScanKernelUnaryTest, JsonStringBoolDouble) {
    const std::vector<Json> data = {
        MakeJson(R"({"a":"xyz"})"),
        MakeJson(R"({"a":5})"),
        MakeJson(R"({"a":true})"),
        MakeJson(R"({"a":2.5})"),
    };
    const std::vector<Json> string_and_double = {
        MakeJson(R"({"a":"xyz"})"),
        MakeJson(R"({"a":2.5})"),
    };
    const LikePatternMatcher like("x%");
    for (auto mode : kBothModes) {
        UnaryJsonKernel<std::string> eq{
            .op = proto::plan::Equal, .val = "xyz", .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, eq, data),
                  (std::vector<Tri>{
                      Tri::True, Tri::Unknown, Tri::Unknown, Tri::Unknown}));
        UnaryJsonKernel<std::string> match{.op = proto::plan::Match,
                                           .val = "x%",
                                           .pointer = "/a",
                                           .like_matcher = &like};
        EXPECT_EQ(RunKernel(mode, match, data),
                  (std::vector<Tri>{
                      Tri::True, Tri::Unknown, Tri::Unknown, Tri::Unknown}));
        UnaryJsonKernel<bool> b{
            .op = proto::plan::Equal, .val = true, .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, b, data),
                  (std::vector<Tri>{
                      Tri::Unknown, Tri::Unknown, Tri::True, Tri::Unknown}));
        UnaryJsonKernel<double> d{
            .op = proto::plan::LessThan, .val = 3.0, .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, d, string_and_double),
                  (std::vector<Tri>{Tri::Unknown, Tri::True}));
    }
}

TEST(ScanKernelUnaryTest, JsonArrayLiteral) {
    const std::vector<Json> data = {
        MakeJson(R"({"a":[1,2]})"),
        MakeJson(R"({"a":[1,3]})"),
        MakeJson(R"({"a":1})"),      // not an array
        MakeJson(R"({"b":[1,2]})"),  // missing path
    };
    for (auto mode : kBothModes) {
        UnaryJsonKernel<proto::plan::Array> eq{.op = proto::plan::Equal,
                                               .val = MakeInt64Literal({1, 2}),
                                               .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, eq, data),
                  (std::vector<Tri>{
                      Tri::True, Tri::False, Tri::Unknown, Tri::Unknown}));
        UnaryJsonKernel<proto::plan::Array> ne{.op = proto::plan::NotEqual,
                                               .val = MakeInt64Literal({1, 2}),
                                               .pointer = "/a"};
        EXPECT_EQ(RunKernel(mode, ne, data),
                  (std::vector<Tri>{
                      Tri::False, Tri::True, Tri::Unknown, Tri::Unknown}));
        // Pins current master behavior (see plan §6 D2: needs audit).
        UnaryJsonKernel<proto::plan::Array> gt{.op = proto::plan::GreaterThan,
                                               .val = MakeInt64Literal({1}),
                                               .pointer = "/a"};
        EXPECT_EQ(
            RunKernel(mode, gt, data),
            (std::vector<Tri>{Tri::False, Tri::False, Tri::False, Tri::False}));
    }
}

TEST(ScanKernelUnaryTest, JsonCandidatesMask) {
    const std::vector<Json> data = {MakeJson(R"({"a":20})"),
                                    MakeJson(R"({"b":1})"),
                                    MakeJson(R"({"a":20})")};
    const FixedVector<bool> candidates = {false, false, true};
    for (auto mode : kBothModes) {
        UnaryJsonKernel<int64_t> kernel{
            .op = proto::plan::GreaterThan, .val = 10, .pointer = "/a"};
        // Row 1 would be UNKNOWN (missing path) if it were a candidate.
        EXPECT_EQ(RunKernel(mode, kernel, data, {}, candidates),
                  (std::vector<Tri>{Tri::False, Tri::False, Tri::True}));
    }
}

TEST(ScanKernelUnaryTest, JsonPreciseNumeric) {
    proto::plan::GenericValue bound;
    bound.set_int64_val(9007199254740993LL);  // 2^53 + 1
    const std::vector<Json> data = {
        MakeJson(R"({"a":9007199254740993})"),
        MakeJson(R"({"a":9007199254740992})"),
        MakeJson(R"({"a":9007199254740992.0})"),
        MakeJson(R"({"a":"9007199254740993"})"),
        MakeJson(R"({})"),
    };
    for (auto mode : kBothModes) {
        UnaryJsonPreciseNumericKernel eq{
            .op = proto::plan::Equal, .pointer = "/a", .bound = &bound};
        EXPECT_EQ(RunKernel(mode, eq, data),
                  (std::vector<Tri>{Tri::True,
                                    Tri::False,
                                    Tri::False,
                                    Tri::Unknown,
                                    Tri::Unknown}));
        UnaryJsonPreciseNumericKernel lt{
            .op = proto::plan::LessThan, .pointer = "/a", .bound = &bound};
        EXPECT_EQ(
            RunKernel(mode, lt, data),
            (std::vector<Tri>{
                Tri::False, Tri::True, Tri::True, Tri::Unknown, Tri::Unknown}));
    }
}

// ---------------------------------------------------------------------------
// UnaryArrayKernel
// ---------------------------------------------------------------------------

TEST(ScanKernelUnaryTest, ArraySubscriptMissingIsUnknown) {
    const auto a0 = MakeInt64Array({1, 2, 3});
    const auto a1 = MakeInt64Array({7});
    const auto a2 = MakeInt64Array({0, 0});
    const std::vector<ArrayView> data = {ViewOf(a0), ViewOf(a1), ViewOf(a2)};
    const FixedVector<bool> valid = {true, true, false};
    for (auto mode : kBothModes) {
        UnaryArrayKernel<int64_t> gt{
            .op = proto::plan::GreaterThan, .val = 1, .index = 1};
        EXPECT_EQ(RunKernel(mode, gt, data, valid),
                  (std::vector<Tri>{Tri::True, Tri::Unknown, Tri::Unknown}));
        UnaryArrayKernel<int64_t> ne{
            .op = proto::plan::NotEqual, .val = 2, .index = 1};
        EXPECT_EQ(RunKernel(mode, ne, data),
                  (std::vector<Tri>{Tri::False, Tri::Unknown, Tri::True}));
    }
}

TEST(ScanKernelUnaryTest, ArrayWholeArrayEquality) {
    const auto a0 = MakeInt64Array({1, 2, 3});
    const auto a1 = MakeInt64Array({1, 2});
    const std::vector<ArrayView> data = {ViewOf(a0), ViewOf(a1)};
    const FixedVector<bool> candidates = {true, false};
    for (auto mode : kBothModes) {
        UnaryArrayKernel<proto::plan::Array> eq{
            .op = proto::plan::Equal, .val = MakeInt64Literal({1, 2, 3})};
        EXPECT_EQ(RunKernel(mode, eq, data),
                  (std::vector<Tri>{Tri::True, Tri::False}));
        UnaryArrayKernel<proto::plan::Array> ne{
            .op = proto::plan::NotEqual, .val = MakeInt64Literal({1, 2, 3})};
        EXPECT_EQ(RunKernel(mode, ne, data, {}, candidates),
                  (std::vector<Tri>{Tri::False, Tri::False}));
    }
}

TEST(ScanKernelUnaryTest, ArrayStringElementPatternOps) {
    const auto a0 = MakeStringArray({"abc", "zz"});
    const auto a1 = MakeStringArray({"xbc"});
    const std::vector<ArrayView> data = {ViewOf(a0), ViewOf(a1)};
    const LikePatternMatcher like("a%");
    for (auto mode : kBothModes) {
        UnaryArrayKernel<std::string> prefix{
            .op = proto::plan::PrefixMatch, .val = "ab", .index = 0};
        EXPECT_EQ(RunKernel(mode, prefix, data),
                  (std::vector<Tri>{Tri::True, Tri::False}));
        UnaryArrayKernel<std::string> match{.op = proto::plan::Match,
                                            .val = "a%",
                                            .index = 0,
                                            .like_matcher = &like};
        EXPECT_EQ(RunKernel(mode, match, data),
                  (std::vector<Tri>{Tri::True, Tri::False}));
        UnaryArrayKernel<std::string> second{
            .op = proto::plan::Equal, .val = "zz", .index = 1};
        EXPECT_EQ(RunKernel(mode, second, data),
                  (std::vector<Tri>{Tri::True, Tri::Unknown}));
    }
}

// ---------------------------------------------------------------------------
// LikeMatchRecheckKernel
// ---------------------------------------------------------------------------

TEST(ScanKernelUnaryTest, LikeMatchRecheck) {
    const std::vector<std::string_view> data = {"hello", "help", "world"};
    const FixedVector<bool> valid = {true, false, true};
    const LikePatternMatcher like("hel%");
    for (auto mode : kBothModes) {
        LikeMatchRecheckKernel kernel{.matcher = &like};
        EXPECT_EQ(RunKernel(mode, kernel, data, valid),
                  (std::vector<Tri>{Tri::True, Tri::Unknown, Tri::False}));
    }
}
