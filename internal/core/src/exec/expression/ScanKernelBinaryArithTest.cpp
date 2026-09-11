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
#include <simdjson.h>

#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/ArrayValue.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "common/VectorArray.h"
#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

using namespace milvus;
using namespace milvus::exec;
using AT = proto::plan::ArithOpType;
using OT = proto::plan::OpType;

static_assert(ScanKernel<BinaryArithScalarKernel<int64_t>, int64_t>);
static_assert(ScanKernel<BinaryArithScalarKernel<double>, double>);
static_assert(ScanKernel<BinaryArithScalarKernel<bool>, bool>);
static_assert(ScanKernel<BinaryArithJsonKernel<int64_t>, milvus::Json>);
static_assert(ScanKernel<BinaryArithJsonKernel<double>, milvus::Json>);
static_assert(ScanKernel<BinaryArithArrayKernel<int64_t>, milvus::ArrayView>);
static_assert(ScanKernel<ArrayLengthKernel<milvus::ArrayView, int64_t>,
                         milvus::ArrayView>);
static_assert(ScanKernel<ArrayLengthKernel<milvus::ArrayValueView, int64_t>,
                         milvus::ArrayValueView>);
static_assert(ScanKernel<ArrayLengthKernel<milvus::VectorArrayView, double>,
                         milvus::VectorArrayView>);

namespace {

constexpr AT kIntArith[] = {AT::Add, AT::Sub, AT::Mul, AT::Div, AT::Mod};
constexpr AT kFloatArith[] = {AT::Add, AT::Sub, AT::Mul, AT::Div};
constexpr OT kCmp[] = {OT::Equal, OT::NotEqual, OT::LessThan, OT::GreaterEqual};

// Stack-owned three-valued output, initialized like EvalKernel does (match=0,
// known=1).
struct TriBuf {
    explicit TriBuf(size_t n) : match(n, false), known(n, true) {
    }
    TriStateOut
    Whole() {
        return TriStateOut{match.view(), known.view()};
    }
    TriStateOut
    Row(size_t i) {
        return TriStateOut{match.view(i, 1), known.view(i, 1)};
    }
    TargetBitmap match;
    TargetBitmap known;
};

template <typename T>
CandidateBatch<T>
MakeBatch(const T* data,
          size_t n,
          ValidityView validity,
          TargetBitmapView candidates) {
    return CandidateBatch<T>{.data = data,
                             .validity = validity,
                             .candidates = candidates,
                             .segment_offsets = nullptr,
                             .size = n};
}

// One contiguous batch, as the chunk readers pass it.
template <typename K, typename T>
TriBuf
RunSequential(const K& kernel,
              const T* data,
              size_t n,
              ValidityView validity = {},
              TargetBitmap* candidates = nullptr) {
    TriBuf buf(n);
    kernel.template Eval<FilterType::sequential>(
        MakeBatch(data,
                  n,
                  validity,
                  candidates ? candidates->view() : TargetBitmapView{}),
        buf.Whole());
    return buf;
}

// One size-1 batch per row, as the offset readers pass it (data points at the
// row).
template <typename K, typename T>
TriBuf
RunRandom(const K& kernel,
          const T* data,
          size_t n,
          ValidityView validity = {},
          TargetBitmap* candidates = nullptr) {
    TriBuf buf(n);
    for (size_t i = 0; i < n; ++i) {
        kernel.template Eval<FilterType::random>(
            MakeBatch(data + i,
                      1,
                      validity ? validity.Subview(i) : ValidityView{},
                      candidates ? candidates->view(i, 1) : TargetBitmapView{}),
            buf.Row(i));
    }
    return buf;
}

// {match, known} per row, before the KernelAdapter fold.
void
ExpectTri(const TriBuf& buf,
          const std::vector<std::pair<bool, bool>>& expected,
          const std::string& trace) {
    ASSERT_EQ(buf.match.size(), expected.size()) << trace;
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(buf.match[i], expected[i].first) << trace << " row " << i;
        EXPECT_EQ(buf.known[i], expected[i].second) << trace << " row " << i;
    }
}

template <typename Fn>
void
ExpectExprInvalid(Fn&& fn, const std::string& message) {
    try {
        fn();
        ADD_FAILURE() << "expected ExprInvalid: " << message;
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ExprInvalid);
        EXPECT_NE(std::string(e.what()).find(message), std::string::npos)
            << e.what();
    }
}

template <typename Fn>
void
ExpectUnexpectedError(Fn&& fn, const std::string& message) {
    try {
        fn();
        ADD_FAILURE() << "expected UnexpectedError: " << message;
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::UnexpectedError);
        EXPECT_NE(std::string(e.what()).find(message), std::string::npos)
            << e.what();
    }
}

int64_t
RefArithInt(AT arith, int64_t x, int64_t r) {
    switch (arith) {
        case AT::Add:
            return x + r;
        case AT::Sub:
            return x - r;
        case AT::Mul:
            return x * r;
        case AT::Div:
            return x / r;
        case AT::Mod:
            return x % r;
        default:
            ADD_FAILURE() << "unexpected arith op";
            return 0;
    }
}

double
RefArithDouble(AT arith, double x, double r) {
    switch (arith) {
        case AT::Add:
            return x + r;
        case AT::Sub:
            return x - r;
        case AT::Mul:
            return x * r;
        case AT::Div:
            return x / r;
        default:
            ADD_FAILURE() << "unexpected arith op";
            return 0;
    }
}

template <typename L, typename V>
bool
RefCmp(OT cmp, L l, V v) {
    switch (cmp) {
        case OT::Equal:
            return l == v;
        case OT::NotEqual:
            return l != v;
        case OT::LessThan:
            return l < v;
        case OT::GreaterEqual:
            return l >= v;
        default:
            ADD_FAILURE() << "unexpected cmp op";
            return false;
    }
}

std::vector<milvus::Json>
MakeJsonRows(std::initializer_list<std::string_view> docs) {
    std::vector<milvus::Json> rows;
    rows.reserve(docs.size());
    for (auto doc : docs) {
        rows.emplace_back(simdjson::padded_string(doc));
    }
    return rows;
}

struct ArrayRows {
    std::vector<milvus::Array> owned;
    std::vector<milvus::ArrayView> views;
};

ArrayRows
MakeInt64Arrays(const std::vector<std::vector<int64_t>>& rows) {
    ArrayRows out;
    out.owned.reserve(rows.size());  // views point into these buffers
    for (const auto& row : rows) {
        milvus::proto::schema::ScalarField field;
        auto* longs = field.mutable_long_data();
        for (auto v : row) {
            longs->add_data(v);
        }
        out.owned.emplace_back(field);
    }
    out.views.reserve(rows.size());
    for (auto& array : out.owned) {
        out.views.emplace_back(const_cast<char*>(array.data()),
                               array.length(),
                               array.byte_size(),
                               array.get_element_type(),
                               array.get_offsets_data());
    }
    return out;
}

std::string
Trace(AT arith, OT cmp, double right, double value) {
    return "arith=" + std::to_string(static_cast<int>(arith)) +
           " cmp=" + std::to_string(static_cast<int>(cmp)) +
           " right=" + std::to_string(right) +
           " value=" + std::to_string(value);
}

}  // namespace

// add/sub/mul/div/mod x eq/ne/lt/ge. 21 rows = 16 SIMD-sized + 5 tail rows;
// random (per-row) must equal sequential and an independent reference.
TEST(ScanKernelBinaryArith, ScalarInt64SequentialMatchesRandom) {
    const std::vector<int64_t> data = {-100, -16, -9, -7, -4, -3, -1,
                                       0,    1,   2,  3,  4,  5,  6,
                                       7,    8,   9,  12, 13, 16, 100};
    for (auto arith : kIntArith) {
        for (auto cmp : kCmp) {
            for (int64_t right : {int64_t{3}, int64_t{-4}}) {
                for (int64_t value :
                     {int64_t{-2}, int64_t{0}, int64_t{1}, int64_t{4}}) {
                    const BinaryArithScalarKernel<int64_t> kernel{
                        .cmp_op = cmp,
                        .arith_op = arith,
                        .value = value,
                        .right_operand = right,
                        .op_ctx = nullptr};
                    auto seq = RunSequential(kernel, data.data(), data.size());
                    auto rnd = RunRandom(kernel, data.data(), data.size());
                    std::vector<std::pair<bool, bool>> expected;
                    for (auto x : data) {
                        expected.emplace_back(
                            RefCmp(cmp, RefArithInt(arith, x, right), value),
                            true);
                    }
                    const auto trace = Trace(arith, cmp, right, value);
                    ExpectTri(seq, expected, "seq " + trace);
                    ExpectTri(rnd, expected, "rnd " + trace);
                }
            }
        }
    }
}

// Exact binary fractions, so the SIMD Div rewrite (x < value*right) and
// scalar x/right agree.
TEST(ScanKernelBinaryArith, ScalarDoubleSequentialMatchesRandom) {
    const std::vector<double> data = {-8.0,
                                      -4.5,
                                      -2.0,
                                      -1.0,
                                      -0.5,
                                      0.0,
                                      0.5,
                                      1.0,
                                      1.5,
                                      2.0,
                                      3.0,
                                      4.0,
                                      6.0,
                                      8.0,
                                      -6.0,
                                      12.0,
                                      16.0,
                                      -16.0,
                                      0.25};
    for (auto arith : kFloatArith) {
        for (auto cmp : kCmp) {
            for (double right : {2.0, -4.0}) {
                for (double value : {-1.0, 0.0, 0.5, 2.0}) {
                    const BinaryArithScalarKernel<double> kernel{
                        .cmp_op = cmp,
                        .arith_op = arith,
                        .value = value,
                        .right_operand = right,
                        .op_ctx = nullptr};
                    auto seq = RunSequential(kernel, data.data(), data.size());
                    auto rnd = RunRandom(kernel, data.data(), data.size());
                    std::vector<std::pair<bool, bool>> expected;
                    for (auto x : data) {
                        expected.emplace_back(
                            RefCmp(cmp, RefArithDouble(arith, x, right), value),
                            true);
                    }
                    const auto trace = Trace(arith, cmp, right, value);
                    ExpectTri(seq, expected, "seq " + trace);
                    ExpectTri(rnd, expected, "rnd " + trace);
                }
            }
        }
    }
}

// NULL folding belongs to KernelAdapter: the scalar kernel writes every row and
// never clears known.
TEST(ScanKernelBinaryArith, ScalarKernelLeavesNullToFold) {
    const std::vector<int64_t> data = {1, 2, 3};
    const bool valid[] = {true, false, true};
    const BinaryArithScalarKernel<int64_t> kernel{.cmp_op = OT::Equal,
                                                  .arith_op = AT::Add,
                                                  .value = 3,
                                                  .right_operand = 1,
                                                  .op_ctx = nullptr};
    const auto validity = ValidityView::FromExpanded(valid);
    const std::vector<std::pair<bool, bool>> expected = {
        {false, true}, {true, true}, {false, true}};
    ExpectTri(RunSequential(kernel, data.data(), data.size(), validity),
              expected,
              "seq");
    ExpectTri(
        RunRandom(kernel, data.data(), data.size(), validity), expected, "rnd");
}

// ExprInvalid on every kernel invocation with a zero Div/Mod operand,
// independent of row values or NULLs (DriverTest pins the driver side).
TEST(ScanKernelBinaryArith, DivisionOrModulusByZeroThrows) {
    const std::vector<int64_t> data(9, 7);
    const bool all_null[9] = {};
    for (auto arith : {AT::Div, AT::Mod}) {
        const BinaryArithScalarKernel<int64_t> kernel{.cmp_op = OT::Equal,
                                                      .arith_op = arith,
                                                      .value = 1,
                                                      .right_operand = 0,
                                                      .op_ctx = nullptr};
        const std::string msg =
            "division or modulus by zero in arithmetic expression";
        ExpectExprInvalid(
            [&] { RunSequential(kernel, data.data(), data.size()); }, msg);
        ExpectExprInvalid([&] { RunRandom(kernel, data.data(), data.size()); },
                          msg);
        ExpectExprInvalid(
            [&] {
                RunSequential(kernel,
                              data.data(),
                              data.size(),
                              ValidityView::FromExpanded(all_null));
            },
            msg);
    }
    const BinaryArithScalarKernel<int64_t> add_zero{.cmp_op = OT::Equal,
                                                    .arith_op = AT::Add,
                                                    .value = 7,
                                                    .right_operand = 0,
                                                    .op_ctx = nullptr};
    EXPECT_NO_THROW(RunSequential(add_zero, data.data(), data.size()));

    // JSON: thrown even when every row misses the path.
    auto json = MakeJsonRows({R"({"b":1})", R"({"b":2})"});
    const BinaryArithJsonKernel<int64_t> json_div{.cmp_op = OT::Equal,
                                                  .arith_op = AT::Div,
                                                  .value = 1,
                                                  .right_operand = 0,
                                                  .pointer = "/a"};
    ExpectExprInvalid(
        [&] { RunSequential(json_div, json.data(), json.size()); },
        "division or modulus by zero in JSON field arithmetic expression");
    const BinaryArithJsonKernel<int64_t> json_len{.cmp_op = OT::Equal,
                                                  .arith_op = AT::ArrayLength,
                                                  .value = 0,
                                                  .right_operand = 0,
                                                  .pointer = "/a"};
    EXPECT_NO_THROW(RunSequential(json_len, json.data(), json.size()));

    auto arrays = MakeInt64Arrays({{1, 2}});
    const BinaryArithArrayKernel<int64_t> array_mod{.cmp_op = OT::Equal,
                                                    .arith_op = AT::Mod,
                                                    .value = 1,
                                                    .right_operand = 0,
                                                    .index = 0};
    ExpectExprInvalid(
        [&] { RunRandom(array_mod, arrays.views.data(), arrays.views.size()); },
        "division or modulus by zero in Array field arithmetic expression");
}

// #50979: missing operand, non-number and JSON null are UNKNOWN; uint64 beyond
// int64 compares as double; NULL / non-candidate rows are not parsed.
TEST(ScanKernelBinaryArith, JsonMissingOperandIsUnknown) {
    auto rows = MakeJsonRows({
        R"({"a":1})",                     // 1+1 != 2 -> FALSE
        R"({"b":1})",                     // missing -> UNKNOWN
        R"({"a":"x"})",                   // not a number -> UNKNOWN
        R"({"a":null})",                  // JSON null -> UNKNOWN
        R"({"a":1.5})",                   // 2.5 != 2 -> TRUE
        R"({"a":18446744073709551615})",  // uint64 -> double -> TRUE
        R"({"a":1})",                     // NULL row -> untouched
        R"({"b":1})",                     // non-candidate -> untouched
        R"({"a":3})",                     // 4 != 2 -> TRUE
    });
    const bool valid[] = {
        true, true, true, true, true, true, false, true, true};
    TargetBitmap candidates(rows.size(), true);
    candidates[7] = false;
    const BinaryArithJsonKernel<int64_t> kernel{.cmp_op = OT::NotEqual,
                                                .arith_op = AT::Add,
                                                .value = 2,
                                                .right_operand = 1,
                                                .pointer = "/a"};
    const std::vector<std::pair<bool, bool>> expected = {{false, true},
                                                         {false, false},
                                                         {false, false},
                                                         {false, false},
                                                         {true, true},
                                                         {true, true},
                                                         {false, true},
                                                         {false, true},
                                                         {true, true}};
    const auto validity = ValidityView::FromExpanded(valid);
    ExpectTri(
        RunSequential(kernel, rows.data(), rows.size(), validity, &candidates),
        expected,
        "seq");
    ExpectTri(
        RunRandom(kernel, rows.data(), rows.size(), validity, &candidates),
        expected,
        "rnd");

    // Same UNKNOWN for GreaterThan (so an outer NOT cannot turn it TRUE).
    const BinaryArithJsonKernel<int64_t> gt{.cmp_op = OT::GreaterThan,
                                            .arith_op = AT::Add,
                                            .value = 2,
                                            .right_operand = 1,
                                            .pointer = "/a"};
    auto gt_out = RunSequential(gt, rows.data(), 4);
    ExpectTri(gt_out,
              {{false, true}, {false, false}, {false, false}, {false, false}},
              "gt");
}

// JSON Mod on double goes through safe_mod -> fmod, unlike scalar columns
// (long(x) % long(r)).
TEST(ScanKernelBinaryArith, JsonDoubleModUsesFmod) {
    auto rows = MakeJsonRows({R"({"a":7.5})", R"({"a":8})", R"({"a":true})"});
    const BinaryArithJsonKernel<double> kernel{.cmp_op = OT::Equal,
                                               .arith_op = AT::Mod,
                                               .value = 1.5,
                                               .right_operand = 2.0,
                                               .pointer = "/a"};
    ExpectTri(RunSequential(kernel, rows.data(), rows.size()),
              {{true, true}, {false, true}, {false, false}},
              "fmod");
}

// JSON array_length extraction failure (non-array, missing, object) is UNKNOWN.
TEST(ScanKernelBinaryArith, JsonArrayLengthExtractionFailureIsUnknown) {
    auto rows = MakeJsonRows({R"({"arr":[1,2]})",
                              R"({"arr":[]})",
                              R"({"arr":3})",
                              R"({})",
                              R"({"arr":{"x":1}})"});
    const BinaryArithJsonKernel<int64_t> eq0{.cmp_op = OT::Equal,
                                             .arith_op = AT::ArrayLength,
                                             .value = 0,
                                             .right_operand = 0,
                                             .pointer = "/arr"};
    const std::vector<std::pair<bool, bool>> eq0_expected = {{false, true},
                                                             {true, true},
                                                             {false, false},
                                                             {false, false},
                                                             {false, false}};
    ExpectTri(
        RunSequential(eq0, rows.data(), rows.size()), eq0_expected, "seq");
    ExpectTri(RunRandom(eq0, rows.data(), rows.size()), eq0_expected, "rnd");

    const BinaryArithJsonKernel<int64_t> ge2{.cmp_op = OT::GreaterEqual,
                                             .arith_op = AT::ArrayLength,
                                             .value = 2,
                                             .right_operand = 0,
                                             .pointer = "/arr"};
    ExpectTri(RunSequential(ge2, rows.data(), rows.size()),
              {{true, true},
               {false, true},
               {false, false},
               {false, false},
               {false, false}},
              "ge2");
}

// ARRAY subscript out of range is UNKNOWN (#50979); NULL and non-candidate
// rows are not read.
TEST(ScanKernelBinaryArith, ArraySubscriptMissingElementIsUnknown) {
    auto arrays = MakeInt64Arrays({{1, 2}, {5}, {3, 4}, {9}, {1, 2}});
    const bool valid[] = {true, true, true, true, false};
    TargetBitmap candidates(arrays.views.size(), true);
    candidates[3] = false;
    const BinaryArithArrayKernel<int64_t> kernel{.cmp_op = OT::Equal,
                                                 .arith_op = AT::Add,
                                                 .value = 3,
                                                 .right_operand = 1,
                                                 .index = 1};
    const std::vector<std::pair<bool, bool>> expected = {
        {true, true},    // 2+1 == 3
        {false, false},  // index 1 out of range
        {false, true},   // 4+1 != 3
        {false, true},   // non-candidate, untouched
        {false, true}};  // NULL, untouched
    const auto validity = ValidityView::FromExpanded(valid);
    ExpectTri(RunSequential(kernel,
                            arrays.views.data(),
                            arrays.views.size(),
                            validity,
                            &candidates),
              expected,
              "seq");
    ExpectTri(RunRandom(kernel,
                        arrays.views.data(),
                        arrays.views.size(),
                        validity,
                        &candidates),
              expected,
              "rnd");
}

TEST(ScanKernelBinaryArith, ArrayLengthCompare) {
    auto arrays = MakeInt64Arrays({{1, 2}, {5}, {3, 4, 5}, {7, 8, 9}});
    const bool valid[] = {true, true, true, false};
    const auto validity = ValidityView::FromExpanded(valid);
    struct Case {
        OT cmp;
        std::vector<std::pair<bool, bool>> expected;
    };
    const std::vector<Case> cases = {
        {OT::Equal,
         {{true, true}, {false, true}, {false, true}, {false, true}}},
        {OT::NotEqual,
         {{false, true}, {true, true}, {true, true}, {false, true}}},
        {OT::LessThan,
         {{false, true}, {true, true}, {false, true}, {false, true}}},
        {OT::GreaterEqual,
         {{true, true}, {false, true}, {true, true}, {false, true}}},
    };
    for (const auto& c : cases) {
        const ArrayLengthKernel<milvus::ArrayView, int64_t> kernel{
            .cmp_op = c.cmp, .value = 2};
        const auto trace = std::to_string(static_cast<int>(c.cmp));
        ExpectTri(
            RunSequential(
                kernel, arrays.views.data(), arrays.views.size(), validity),
            c.expected,
            "seq " + trace);
        ExpectTri(
            RunRandom(
                kernel, arrays.views.data(), arrays.views.size(), validity),
            c.expected,
            "rnd " + trace);
    }
    const ArrayLengthKernel<milvus::ArrayView, double> lt_double{
        .cmp_op = OT::LessThan, .value = 2.5};
    ExpectTri(RunSequential(lt_double, arrays.views.data(), 3),
              {{true, true}, {true, true}, {false, true}},
              "double");
}

TEST(ScanKernelBinaryArith, UnsupportedOperatorsThrow) {
    const std::vector<int64_t> data = {1};
    const BinaryArithScalarKernel<int64_t> bad_arith{
        .cmp_op = OT::Equal,
        .arith_op = AT::ArrayLength,
        .value = 1,
        .right_operand = 1,
        .op_ctx = nullptr};
    ExpectUnexpectedError(
        [&] { RunSequential(bad_arith, data.data(), data.size()); },
        "unsupported arith type for binary arithmetic eval expr");
    const BinaryArithScalarKernel<int64_t> bad_cmp{.cmp_op = OT::PrefixMatch,
                                                   .arith_op = AT::Add,
                                                   .value = 1,
                                                   .right_operand = 1,
                                                   .op_ctx = nullptr};
    ExpectUnexpectedError(
        [&] { RunRandom(bad_cmp, data.data(), data.size()); },
        "unsupported operator type for binary arithmetic eval expr");
    auto arrays = MakeInt64Arrays({{1}});
    const ArrayLengthKernel<milvus::ArrayView, int64_t> bad_len{
        .cmp_op = OT::PrefixMatch, .value = 1};
    ExpectUnexpectedError(
        [&] {
            RunSequential(bad_len, arrays.views.data(), arrays.views.size());
        },
        "unsupported operator type for ARRAY length expression");
}
