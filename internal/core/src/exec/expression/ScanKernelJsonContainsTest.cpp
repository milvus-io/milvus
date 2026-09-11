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
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/Array.h"
#include "common/Json.h"
#include "common/Types.h"
#include "exec/expression/Element.h"
#include "exec/expression/Expr.h"
#include "exec/expression/JsonContainsExpr.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

namespace milvus::exec {
namespace {

enum class Tri { True, False, Unknown };

std::ostream&
operator<<(std::ostream& os, Tri value) {
    switch (value) {
        case Tri::True:
            return os << "TRUE";
        case Tri::False:
            return os << "FALSE";
        case Tri::Unknown:
            return os << "UNKNOWN";
    }
    return os;
}

struct KernelRun {
    TargetBitmap match;       // raw kernel output
    TargetBitmap known;       // raw kernel output
    std::vector<Tri> folded;  // after the KernelAdapter / EvalKernel fold
};

template <FilterType filter_type, typename T, typename Kernel>
KernelRun
RunKernel(Kernel& kernel,
          const std::vector<T>& rows,
          const FixedVector<bool>* validity = nullptr,
          TargetBitmap* candidates = nullptr) {
    const size_t n = rows.size();
    KernelRun run{TargetBitmap(n, false), TargetBitmap(n, true), {}};
    CandidateBatch<T> batch{
        rows.data(),
        validity != nullptr ? ValidityView::FromExpanded(validity->data())
                            : ValidityView{},
        candidates != nullptr ? TargetBitmapView(*candidates)
                              : TargetBitmapView{},
        nullptr,
        n};
    kernel.template Eval<filter_type>(
        batch, TriStateOut{run.match.view(), run.known.view()});
    run.folded.resize(n);
    for (size_t i = 0; i < n; ++i) {
        bool known = run.known[i] && (validity == nullptr || (*validity)[i]);
        bool match = run.match[i] && known;
        if (candidates != nullptr && !(*candidates)[i]) {
            match = false;
            known = true;
        }
        run.folded[i] =
            !known ? Tri::Unknown : (match ? Tri::True : Tri::False);
    }
    return run;
}

// Runs a fresh kernel through both filter types and checks they agree.
template <typename T, typename MakeKernel>
std::vector<Tri>
RunBothFilterTypes(MakeKernel make_kernel,
                   const std::vector<T>& rows,
                   const FixedVector<bool>* validity = nullptr,
                   TargetBitmap* candidates = nullptr) {
    auto sequential_kernel = make_kernel();
    auto sequential = RunKernel<FilterType::sequential, T>(
        sequential_kernel, rows, validity, candidates);
    auto random_kernel = make_kernel();
    auto random = RunKernel<FilterType::random, T>(
        random_kernel, rows, validity, candidates);
    EXPECT_EQ(sequential.folded, random.folded);
    return sequential.folded;
}

std::vector<milvus::Json>
JsonRows(const std::vector<std::string>& docs) {
    std::vector<milvus::Json> rows;
    rows.reserve(docs.size());
    for (const auto& doc : docs) {
        rows.emplace_back(simdjson::padded_string(doc));
    }
    return rows;
}

proto::plan::GenericValue
Int64Value(int64_t v) {
    proto::plan::GenericValue value;
    value.set_int64_val(v);
    return value;
}

proto::plan::GenericValue
StringValue(const std::string& v) {
    proto::plan::GenericValue value;
    value.set_string_val(v);
    return value;
}

proto::plan::GenericValue
BoolValue(bool v) {
    proto::plan::GenericValue value;
    value.set_bool_val(v);
    return value;
}

proto::plan::GenericValue
FloatValue(double v) {
    proto::plan::GenericValue value;
    value.set_float_val(v);
    return value;
}

proto::plan::Array
ArrayOf(const std::vector<proto::plan::GenericValue>& items) {
    proto::plan::Array array;
    for (const auto& item : items) {
        *array.add_array() = item;
    }
    return array;
}

proto::plan::GenericValue
ArrayValue(const proto::plan::Array& array) {
    proto::plan::GenericValue value;
    value.mutable_array_val()->CopyFrom(array);
    return value;
}

milvus::Array
LongArray(const std::vector<int64_t>& items) {
    ScalarFieldProto field;
    auto* data = field.mutable_long_data();
    for (auto item : items) {
        data->add_data(item);
    }
    return milvus::Array(field);
}

milvus::Array
StringArray(const std::vector<std::string>& items) {
    ScalarFieldProto field;
    auto* data = field.mutable_string_data();
    for (const auto& item : items) {
        data->add_data(item);
    }
    return milvus::Array(field);
}

using Int64TypedSet = ankerl::unordered_dense::set<int64_t>;

static_assert(ScanKernel<JsonContainsAnyKernel<int64_t>, milvus::Json>);
static_assert(
    ScanKernel<JsonContainsAllKernel<std::string_view>, milvus::Json>);
static_assert(ScanKernel<JsonContainsArrayKernel, milvus::Json>);
static_assert(ScanKernel<JsonContainsAllArrayKernel, milvus::Json>);
static_assert(ScanKernel<JsonContainsAnyWithDiffTypeKernel, milvus::Json>);
static_assert(ScanKernel<JsonContainsAllWithDiffTypeKernel, milvus::Json>);
static_assert(
    ScanKernel<ArrayContainsAnyKernel<ArrayView, int64_t, Int64TypedSet>,
               ArrayView>);
static_assert(
    ScanKernel<ArrayContainsAllKernel<ArrayValueView, std::string_view>,
               ArrayValueView>);

}  // namespace

TEST(ScanKernelJsonContainsTest, AnyInt64ThreeValuedAndSkippedRows) {
    const std::vector<proto::plan::GenericValue> vals{Int64Value(2)};
    SetElement<int64_t> elements(vals);
    auto rows = JsonRows({
        R"({"a": [1, 2, 3]})",   // 0 TRUE
        R"({"a": [4, 5]})",      // 1 FALSE
        R"({"a": [2.0]})",       // 2 TRUE: integral double
        R"({"a": [2.5, "2"]})",  // 3 FALSE: mismatched items are skipped
        R"({"a": "2"})",         // 4 UNKNOWN: not an array
        R"({"b": [2]})",         // 5 UNKNOWN: path missing
        R"({"a": null})",        // 6 UNKNOWN: JSON null
        R"({"a": []})",          // 7 FALSE: empty array
        R"({"a": [2]})",         // 8 column NULL
        R"({"a": [2]})",         // 9 pre-filtered
    });
    FixedVector<bool> validity(rows.size(), true);
    validity[8] = false;
    TargetBitmap candidates(rows.size(), true);
    candidates[9] = false;

    auto make_kernel = [&]() {
        return JsonContainsAnyKernel<int64_t>{milvus::Json::pointer({"a"}),
                                              &elements};
    };
    auto folded = RunBothFilterTypes<milvus::Json>(
        make_kernel, rows, &validity, &candidates);
    EXPECT_EQ(folded,
              (std::vector<Tri>{Tri::True,
                                Tri::False,
                                Tri::True,
                                Tri::False,
                                Tri::Unknown,
                                Tri::Unknown,
                                Tri::Unknown,
                                Tri::False,
                                Tri::Unknown,
                                Tri::False}));

    // Skipped rows are left exactly as EvalKernel initialised them.
    auto kernel = make_kernel();
    auto raw = RunKernel<FilterType::sequential, milvus::Json>(
        kernel, rows, &validity, &candidates);
    for (size_t i : {8, 9}) {
        EXPECT_FALSE(raw.match[i]) << "row " << i;
        EXPECT_TRUE(raw.known[i]) << "row " << i;
    }
    // A path error is written as UNKNOWN by the kernel itself.
    EXPECT_FALSE(raw.known[4]);
    EXPECT_FALSE(raw.match[4]);
}

TEST(ScanKernelJsonContainsTest, AnyStringAndBool) {
    const std::vector<proto::plan::GenericValue> string_vals{StringValue("y")};
    SetElement<std::string_view> strings(string_vals);
    auto string_rows = JsonRows({
        R"({"a": ["x", "y"]})",
        R"({"a": [1, "z"]})",
        R"({"a": {"x": 1}})",
    });
    EXPECT_EQ(RunBothFilterTypes<milvus::Json>(
                  [&]() {
                      return JsonContainsAnyKernel<std::string_view>{
                          milvus::Json::pointer({"a"}), &strings};
                  },
                  string_rows),
              (std::vector<Tri>{Tri::True, Tri::False, Tri::Unknown}));

    const std::vector<proto::plan::GenericValue> bool_vals{BoolValue(true)};
    SetElement<bool> bools(bool_vals);
    auto bool_rows = JsonRows({
        R"({"a": [false, true]})",
        R"({"a": [0]})",
    });
    EXPECT_EQ(RunBothFilterTypes<milvus::Json>(
                  [&]() {
                      return JsonContainsAnyKernel<bool>{
                          milvus::Json::pointer({"a"}), &bools};
                  },
                  bool_rows),
              (std::vector<Tri>{Tri::True, Tri::False}));
}

TEST(ScanKernelJsonContainsTest, AllInt64SmallTargetSet) {
    const std::set<int64_t> targets{1, 2};
    auto rows = JsonRows({
        R"({"a": [1, 2, 3]})",
        R"({"a": [1, 3]})",
        R"({"a": [2, 1.0]})",
        R"({"a": "x"})",
        R"({"a": []})",
    });
    EXPECT_EQ(RunBothFilterTypes<milvus::Json>(
                  [&]() {
                      return JsonContainsAllKernel<int64_t>(
                          milvus::Json::pointer({"a"}), targets);
                  },
                  rows),
              (std::vector<Tri>{
                  Tri::True, Tri::False, Tri::True, Tri::Unknown, Tri::False}));
}

TEST(ScanKernelJsonContainsTest, AllInt64LargeTargetSetResetsScratchPerRow) {
    std::set<int64_t> targets;
    std::string full = R"({"a": [)";
    std::string partial = R"({"a": [)";
    for (int64_t v = 0; v < 70; ++v) {
        targets.insert(v);
        full += (v == 0 ? "" : ",") + std::to_string(v);
        if (v < 69) {
            partial += (v == 0 ? "" : ",") + std::to_string(v);
        }
    }
    full += "]}";
    partial += "]}";
    auto rows = JsonRows({full, partial, full});
    EXPECT_EQ(RunBothFilterTypes<milvus::Json>(
                  [&]() {
                      return JsonContainsAllKernel<int64_t>(
                          milvus::Json::pointer({"a"}), targets);
                  },
                  rows),
              (std::vector<Tri>{Tri::True, Tri::False, Tri::True}));
}

TEST(ScanKernelJsonContainsTest, ContainsArrayAndContainsAllArray) {
    const std::vector<proto::plan::Array> any_targets{
        ArrayOf({Int64Value(1), Int64Value(2)})};
    auto any_rows = JsonRows({
        R"({"a": [[1, 2], [3]]})",
        R"({"a": [[2, 1]]})",
        R"({"a": [1, 2]})",
        R"({"a": {}})",
    });
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Json>(
            [&]() {
                return JsonContainsArrayKernel{milvus::Json::pointer({"a"}),
                                               &any_targets};
            },
            any_rows),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::False, Tri::Unknown}));

    const std::vector<proto::plan::Array> all_targets{
        ArrayOf({Int64Value(1)}), ArrayOf({Int64Value(2), Int64Value(3)})};
    auto all_rows = JsonRows({
        R"({"a": [[2, 3], [1]]})",
        R"({"a": [[1]]})",
        R"({"a": 5})",
        R"({"a": [[1], [2, 3], [4]]})",
    });
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Json>(
            [&]() {
                return JsonContainsAllArrayKernel{milvus::Json::pointer({"a"}),
                                                  all_targets};
            },
            all_rows),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::Unknown, Tri::True}));
}

TEST(ScanKernelJsonContainsTest, AnyAndAllWithDiffType) {
    const std::vector<proto::plan::GenericValue> any_vals{
        Int64Value(1),
        StringValue("x"),
        BoolValue(true),
        FloatValue(2.5),
        ArrayValue(ArrayOf({Int64Value(7)}))};
    auto any_rows = JsonRows({
        R"({"a": [0, "x"]})",
        R"({"a": [1.0]})",
        R"({"a": [false, "y"]})",
        R"({"a": [2.5]})",
        R"({"a": [[7]]})",
        R"({"a": 1})",
    });
    EXPECT_EQ(RunBothFilterTypes<milvus::Json>(
                  [&]() {
                      return JsonContainsAnyWithDiffTypeKernel{
                          milvus::Json::pointer({"a"}), &any_vals};
                  },
                  any_rows),
              (std::vector<Tri>{Tri::True,
                                Tri::True,
                                Tri::False,
                                Tri::True,
                                Tri::True,
                                Tri::Unknown}));

    const std::vector<proto::plan::GenericValue> all_vals{Int64Value(1),
                                                          StringValue("x")};
    auto all_rows = JsonRows({
        R"({"a": [1, "x"]})",
        R"({"a": ["x"]})",
        R"({"a": [1.0, "x", true]})",
        R"({"a": "x"})",
    });
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Json>(
            [&]() {
                return JsonContainsAllWithDiffTypeKernel{
                    milvus::Json::pointer({"a"}),
                    &all_vals,
                    std::unordered_set<int>{0, 1}};
            },
            all_rows),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::True, Tri::Unknown}));
}

TEST(ScanKernelJsonContainsTest, ArrayColumnAnyAndAll) {
    Int64TypedSet any_targets{2};
    std::vector<milvus::Array> long_rows;
    long_rows.push_back(LongArray({1, 2}));
    long_rows.push_back(LongArray({3}));
    long_rows.push_back(LongArray({}));
    long_rows.push_back(LongArray({2}));
    FixedVector<bool> validity{true, true, true, false};
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Array>(
            [&]() {
                return ArrayContainsAnyKernel<milvus::Array,
                                              int64_t,
                                              Int64TypedSet>{&any_targets};
            },
            long_rows,
            &validity),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::False, Tri::Unknown}));

    const std::set<std::string_view> all_targets{"a", "c"};
    std::vector<milvus::Array> string_rows;
    string_rows.push_back(StringArray({"a", "b", "c"}));
    string_rows.push_back(StringArray({"a"}));
    string_rows.push_back(StringArray({"c", "a"}));
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Array>(
            [&]() {
                return ArrayContainsAllKernel<milvus::Array, std::string_view>(
                    all_targets);
            },
            string_rows),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::True}));
}

TEST(ScanKernelJsonContainsTest,
     ArrayColumnAllLargeTargetSetResetsScratchPerRow) {
    std::set<int64_t> targets;
    std::vector<int64_t> full_items;
    for (int64_t v = 0; v < 70; ++v) {
        targets.insert(v);
        full_items.push_back(v);
    }
    std::vector<int64_t> partial_items(full_items.begin(),
                                       full_items.end() - 1);
    std::vector<milvus::Array> rows;
    rows.push_back(LongArray(full_items));
    rows.push_back(LongArray(partial_items));
    rows.push_back(LongArray(full_items));
    EXPECT_EQ(
        RunBothFilterTypes<milvus::Array>(
            [&]() {
                return ArrayContainsAllKernel<milvus::Array, int64_t>(targets);
            },
            rows),
        (std::vector<Tri>{Tri::True, Tri::False, Tri::True}));
}

}  // namespace milvus::exec
