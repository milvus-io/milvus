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
#include <any>
#include <cstdint>
#include <functional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/Array.h"
#include "common/Json.h"
#include "common/Types.h"
#include "exec/expression/Element.h"
#include "exec/expression/TermExpr.h"
#include "index/SkipIndex.h"
#include "simdjson/padded_string.h"

using namespace milvus;
using namespace milvus::exec;

static_assert(ScanKernel<TermScalarKernel<int64_t>, int64_t>);
static_assert(ScanKernel<TermScalarKernel<std::string_view>, std::string_view>);
static_assert(ScanKernel<TermScalarKernel<std::string>, std::string>);
static_assert(ScanKernel<TermScalarKernel<bool>, bool>);
static_assert(
    ScanKernel<TermArrayVariableInFieldKernel<int64_t>, milvus::ArrayView>);
static_assert(
    ScanKernel<TermArrayFieldInVariableKernel<int64_t>, milvus::ArrayView>);
static_assert(ScanKernel<TermJsonVariableInFieldKernel<int64_t>, milvus::Json>);
static_assert(
    ScanKernel<TermJsonFieldInVariableKernel<std::string>, milvus::Json>);

namespace {

enum class Tri { kFalse = 0, kTrue = 1, kUnknown = 2 };

// Result bitmaps for one batch. offset shifts bit 0 of both views so kernels
// see non-byte-aligned output slices, as KernelAdapter passes sub-views.
class OutBuffer {
 public:
    OutBuffer(size_t n, size_t offset)
        : n_(n),
          offset_(offset),
          match_(n + offset, false),
          known_(n + offset, true) {
    }

    TriStateOut
    Out() {
        return TriStateOut{TargetBitmapView(match_.data(), offset_, n_),
                           TargetBitmapView(known_.data(), offset_, n_)};
    }

    // Spec 4.4 rules 1-3.
    void
    Fold(ValidityView validity, const TargetBitmapView& candidates) {
        for (size_t i = 0; i < n_; ++i) {
            const size_t pos = offset_ + i;
            if (validity && !validity[i]) {
                known_[pos] = false;
            }
            if (!known_[pos]) {
                match_[pos] = false;
            }
            if (!candidates.empty() && !candidates[i]) {
                match_[pos] = false;
                known_[pos] = true;
            }
        }
    }

    Tri
    At(size_t i) const {
        const size_t pos = offset_ + i;
        if (!known_[pos]) {
            EXPECT_FALSE(bool(match_[pos])) << "known=0 but match=1 at " << i;
            return Tri::kUnknown;
        }
        return match_[pos] ? Tri::kTrue : Tri::kFalse;
    }

    size_t
    size() const {
        return n_;
    }

 private:
    size_t n_;
    size_t offset_;
    TargetBitmap match_;
    TargetBitmap known_;
};

void
ExpectTri(const OutBuffer& out,
          const std::vector<Tri>& expected,
          const std::string& label) {
    ASSERT_EQ(out.size(), expected.size()) << label;
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(static_cast<int>(out.At(i)), static_cast<int>(expected[i]))
            << label << " row " << i;
    }
}

// Runs Eval<sequential> and Eval<random> on the same batch and checks both
// against expected after folding.
template <typename K, typename T>
void
ExpectBothFilterTypes(const K& kernel,
                      const CandidateBatch<T>& batch,
                      const std::vector<Tri>& expected,
                      const std::string& label) {
    OutBuffer seq(batch.size, 0);
    kernel.template Eval<FilterType::sequential>(batch, seq.Out());
    seq.Fold(batch.validity, batch.candidates);
    ExpectTri(seq, expected, label + " sequential");

    OutBuffer rnd(batch.size, 5);
    kernel.template Eval<FilterType::random>(batch, rnd.Out());
    rnd.Fold(batch.validity, batch.candidates);
    ExpectTri(rnd, expected, label + " random");
}

Json
MakeJson(const std::string& s) {
    return Json(simdjson::padded_string(s));
}

class Int64Arrays {
 public:
    explicit Int64Arrays(std::vector<std::vector<int64_t>> rows)
        : storage_(std::move(rows)) {
        views_.reserve(storage_.size());
        for (auto& r : storage_) {
            views_.emplace_back(reinterpret_cast<char*>(r.data()),
                                static_cast<int>(r.size()),
                                r.size() * sizeof(int64_t),
                                DataType::INT64,
                                nullptr);
        }
    }
    const ArrayView*
    data() const {
        return views_.data();
    }

 private:
    std::vector<std::vector<int64_t>> storage_;
    std::vector<ArrayView> views_;
};

class StringArrays {
 public:
    explicit StringArrays(const std::vector<std::vector<std::string>>& rows)
        : buffers_(rows.size()), offsets_(rows.size()) {
        for (size_t r = 0; r < rows.size(); ++r) {
            for (const auto& s : rows[r]) {
                offsets_[r].push_back(
                    static_cast<uint32_t>(buffers_[r].size()));
                buffers_[r] += s;
            }
        }
        views_.reserve(rows.size());
        for (size_t r = 0; r < rows.size(); ++r) {
            views_.emplace_back(buffers_[r].data(),
                                static_cast<int>(rows[r].size()),
                                buffers_[r].size(),
                                DataType::VARCHAR,
                                offsets_[r].data());
        }
    }
    const ArrayView*
    data() const {
        return views_.data();
    }

 private:
    std::vector<std::string> buffers_;
    std::vector<std::vector<uint32_t>> offsets_;
    std::vector<ArrayView> views_;
};

class CountingInt64Element : public MultiElement {
 public:
    explicit CountingInt64Element(const std::vector<int64_t>& vals)
        : inner_(vals) {
    }
    bool
    In(const ValueType& value) const override {
        ++calls;
        return inner_.In(value);
    }
    bool
    Empty() const override {
        return inner_.Empty();
    }
    size_t
    Size() const override {
        return inner_.Size();
    }
    mutable int calls = 0;

 private:
    FlatVectorElement<int64_t> inner_;
};

template <typename T>
void
CheckSimdMatchesPerRow(size_t out_offset) {
    const std::vector<T> in_vals = {T(-7), T(0), T(3), T(11), T(19)};
    SimdBatchElement<T> simd_elem(in_vals);  // already sorted and unique
    const TermFilterChunkFn simd_fn =
        [&simd_elem](const void* d, int size, TargetBitmapView res) {
            simd_elem.FilterChunk(static_cast<const T*>(d), size, res);
        };
    FlatVectorElement<T> row_elem(in_vals);

    const size_t n = 517;
    std::mt19937 gen(42);
    std::uniform_int_distribution<int> dist(-20, 20);
    std::vector<T> data(n);
    FixedVector<bool> valid(n);
    TargetBitmap cand(n, true);
    for (size_t i = 0; i < n; ++i) {
        data[i] = static_cast<T>(dist(gen));
        valid[i] = (i % 7 != 3);
        if (i % 5 == 1) {
            cand[i] = false;
        }
    }
    const CandidateBatch<T> batch{
        .data = data.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView(cand),
        .segment_offsets = nullptr,
        .size = n};

    std::vector<Tri> expected(n);
    for (size_t i = 0; i < n; ++i) {
        if (!cand[i]) {
            expected[i] = Tri::kFalse;
        } else if (!valid[i]) {
            expected[i] = Tri::kUnknown;
        } else {
            expected[i] = std::find(in_vals.begin(), in_vals.end(), data[i]) !=
                                  in_vals.end()
                              ? Tri::kTrue
                              : Tri::kFalse;
        }
    }

    const TermScalarKernel<T> simd_kernel{.op_ctx = nullptr,
                                          .vals = &simd_elem,
                                          .simd_filter_fn = &simd_fn,
                                          .str_set_elem = nullptr,
                                          .skip_elements = nullptr};
    const TermScalarKernel<T> row_kernel{.op_ctx = nullptr,
                                         .vals = &row_elem,
                                         .simd_filter_fn = nullptr,
                                         .str_set_elem = nullptr,
                                         .skip_elements = nullptr};

    OutBuffer simd_out(n, out_offset);
    simd_kernel.template Eval<FilterType::sequential>(batch, simd_out.Out());
    simd_out.Fold(batch.validity, batch.candidates);
    ExpectTri(simd_out, expected, "simd sequential");

    OutBuffer row_out(n, out_offset);
    row_kernel.template Eval<FilterType::sequential>(batch, row_out.Out());
    row_out.Fold(batch.validity, batch.candidates);
    ExpectTri(row_out, expected, "per-row sequential");

    // Random must not take the SIMD path even when simd_filter_fn is set.
    OutBuffer random_out(n, out_offset);
    simd_kernel.template Eval<FilterType::random>(batch, random_out.Out());
    random_out.Fold(batch.validity, batch.candidates);
    ExpectTri(random_out, expected, "simd-kernel random");
}

}  // namespace

TEST(ScanKernelTermTest, SimdMatchesPerRow) {
    for (size_t offset : {size_t{0}, size_t{3}}) {
        SCOPED_TRACE(offset);
        CheckSimdMatchesPerRow<int8_t>(offset);
        CheckSimdMatchesPerRow<int16_t>(offset);
        CheckSimdMatchesPerRow<int32_t>(offset);
        CheckSimdMatchesPerRow<int64_t>(offset);
        CheckSimdMatchesPerRow<float>(offset);
        CheckSimdMatchesPerRow<double>(offset);
    }
}

TEST(ScanKernelTermTest, PerRowSkipsNullAndNonCandidateRows) {
    const std::vector<int64_t> data = {1, 1, 1, 2, 1, 2, 1, 1, 2, 1};
    FixedVector<bool> valid(data.size(), true);
    valid[2] = false;
    TargetBitmap cand(data.size(), true);
    cand[1] = false;
    cand[4] = false;
    cand[7] = false;

    CountingInt64Element elem({1});
    const TermScalarKernel<int64_t> kernel{.op_ctx = nullptr,
                                           .vals = &elem,
                                           .simd_filter_fn = nullptr,
                                           .str_set_elem = nullptr,
                                           .skip_elements = nullptr};
    const CandidateBatch<int64_t> batch{
        .data = data.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView(cand),
        .segment_offsets = nullptr,
        .size = data.size()};
    const std::vector<Tri> expected = {Tri::kTrue,
                                       Tri::kFalse,
                                       Tri::kUnknown,
                                       Tri::kFalse,
                                       Tri::kFalse,
                                       Tri::kFalse,
                                       Tri::kTrue,
                                       Tri::kFalse,
                                       Tri::kFalse,
                                       Tri::kTrue};

    OutBuffer out(data.size(), 0);
    kernel.Eval<FilterType::sequential>(batch, out.Out());
    EXPECT_EQ(elem.calls, 6);
    out.Fold(batch.validity, batch.candidates);
    ExpectTri(out, expected, "masked");

    elem.calls = 0;
    const CandidateBatch<int64_t> unmasked{
        .data = data.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView{},
        .segment_offsets = nullptr,
        .size = data.size()};
    OutBuffer out2(data.size(), 0);
    kernel.Eval<FilterType::random>(unmasked, out2.Out());
    EXPECT_EQ(elem.calls, 9);
}

TEST(ScanKernelTermTest, Strings) {
    const std::vector<std::string> owned = {
        "a", "bb", "ccc", "", "dddd", "a", "zz"};
    std::vector<std::string_view> views(owned.begin(), owned.end());
    FixedVector<bool> valid(owned.size(), true);
    valid[5] = false;

    // > kLinearScanThreshold values: SetElement + str_set_elem.
    SetElement<std::string> set_elem(
        std::vector<std::string>{"a", "ccc", "", "q1", "q2"});
    // <= kLinearScanThreshold values: FlatVectorElement, str_set_elem null.
    FlatVectorElement<std::string> flat_elem(
        std::vector<std::string>{"bb", "zz"});

    const std::vector<Tri> expected_set = {Tri::kTrue,
                                           Tri::kFalse,
                                           Tri::kTrue,
                                           Tri::kTrue,
                                           Tri::kFalse,
                                           Tri::kUnknown,
                                           Tri::kFalse};
    const std::vector<Tri> expected_flat = {Tri::kFalse,
                                            Tri::kTrue,
                                            Tri::kFalse,
                                            Tri::kFalse,
                                            Tri::kFalse,
                                            Tri::kUnknown,
                                            Tri::kTrue};

    const CandidateBatch<std::string_view> sv_batch{
        .data = views.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView{},
        .segment_offsets = nullptr,
        .size = views.size()};
    const CandidateBatch<std::string> str_batch{
        .data = owned.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView{},
        .segment_offsets = nullptr,
        .size = owned.size()};

    ExpectBothFilterTypes(
        TermScalarKernel<std::string_view>{.op_ctx = nullptr,
                                           .vals = &set_elem,
                                           .simd_filter_fn = nullptr,
                                           .str_set_elem = &set_elem,
                                           .skip_elements = nullptr},
        sv_batch,
        expected_set,
        "string_view set");
    ExpectBothFilterTypes(
        TermScalarKernel<std::string_view>{.op_ctx = nullptr,
                                           .vals = &flat_elem,
                                           .simd_filter_fn = nullptr,
                                           .str_set_elem = nullptr,
                                           .skip_elements = nullptr},
        sv_batch,
        expected_flat,
        "string_view flat");
    ExpectBothFilterTypes(
        TermScalarKernel<std::string>{.op_ctx = nullptr,
                                      .vals = &set_elem,
                                      .simd_filter_fn = nullptr,
                                      .str_set_elem = &set_elem,
                                      .skip_elements = nullptr},
        str_batch,
        expected_set,
        "string set");
    ExpectBothFilterTypes(
        TermScalarKernel<std::string>{.op_ctx = nullptr,
                                      .vals = &flat_elem,
                                      .simd_filter_fn = nullptr,
                                      .str_set_elem = nullptr,
                                      .skip_elements = nullptr},
        str_batch,
        expected_flat,
        "string flat");
}

TEST(ScanKernelTermTest, Bool) {
    const bool data[] = {true, false, true, false};
    FixedVector<bool> valid = {true, true, false, true};
    SetElement<bool> elem(std::vector<bool>{true});
    const CandidateBatch<bool> batch{
        .data = data,
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView{},
        .segment_offsets = nullptr,
        .size = 4};
    ExpectBothFilterTypes(TermScalarKernel<bool>{.op_ctx = nullptr,
                                                 .vals = &elem,
                                                 .simd_filter_fn = nullptr,
                                                 .str_set_elem = nullptr,
                                                 .skip_elements = nullptr},
                          batch,
                          {Tri::kTrue, Tri::kFalse, Tri::kUnknown, Tri::kFalse},
                          "bool");
}

TEST(ScanKernelTermTest, CanSkip) {
    SkipIndex skip_index;
    FlatVectorElement<int64_t> elem(std::vector<int64_t>{1});
    const std::any values = std::vector<int64_t>{1, 2};
    const std::any wrong_type = std::vector<int32_t>{1, 2};
    const std::any empty;

    auto make = [&](const std::any* skip_elements) {
        return TermScalarKernel<int64_t>{.op_ctx = nullptr,
                                         .vals = &elem,
                                         .simd_filter_fn = nullptr,
                                         .str_set_elem = nullptr,
                                         .skip_elements = skip_elements};
    };
    EXPECT_FALSE(make(nullptr).CanSkip(skip_index, FieldId(101), 0));
    EXPECT_FALSE(make(&empty).CanSkip(skip_index, FieldId(101), 0));
    EXPECT_FALSE(make(&wrong_type).CanSkip(skip_index, FieldId(101), 0));
    // No metrics loaded for the field: default metrics never skip.
    EXPECT_FALSE(make(&values).CanSkip(skip_index, FieldId(101), 0));
}

TEST(ScanKernelTermTest, JsonFieldInVariableInt64) {
    std::vector<Json> rows;
    rows.push_back(MakeJson(R"({"a": 1})"));
    rows.push_back(MakeJson(R"({"a": 2})"));
    rows.push_back(MakeJson(R"({"a": "1"})"));
    rows.push_back(MakeJson(R"({"b": 1})"));
    rows.push_back(MakeJson(R"({"a": null})"));
    rows.push_back(MakeJson(R"({"a": 5.0})"));
    rows.push_back(MakeJson(R"({"a": 1.5})"));
    rows.push_back(MakeJson(R"({"a": 1})"));
    rows.push_back(MakeJson(R"({"a": 5})"));
    rows.push_back(MakeJson(R"({"a": true})"));
    FixedVector<bool> valid(rows.size(), true);
    valid[7] = false;
    TargetBitmap cand(rows.size(), true);
    cand[8] = false;

    SetElement<int64_t> terms(std::vector<int64_t>{1, 5});
    const TermJsonFieldInVariableKernel<int64_t> kernel{.pointer = "/a",
                                                        .terms = &terms};
    const CandidateBatch<Json> batch{
        .data = rows.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView(cand),
        .segment_offsets = nullptr,
        .size = rows.size()};
    ExpectBothFilterTypes(kernel,
                          batch,
                          {Tri::kTrue,
                           Tri::kFalse,
                           Tri::kUnknown,  // type mismatch
                           Tri::kUnknown,  // missing path
                           Tri::kUnknown,  // JSON null
                           Tri::kTrue,     // integral double
                           Tri::kFalse,    // non-integral double
                           Tri::kUnknown,  // NULL column value
                           Tri::kFalse,    // non-candidate
                           Tri::kUnknown},
                          "json field in variable int64");
}

TEST(ScanKernelTermTest, JsonFieldInVariableString) {
    std::vector<Json> rows;
    rows.push_back(MakeJson(R"({"a": "x"})"));
    rows.push_back(MakeJson(R"({"a": "z"})"));
    rows.push_back(MakeJson(R"({"a": 1})"));
    rows.push_back(MakeJson(R"({})"));
    SetElement<std::string> terms(std::vector<std::string>{"x", "y"});
    const CandidateBatch<Json> batch{.data = rows.data(),
                                     .validity = ValidityView{},
                                     .candidates = TargetBitmapView{},
                                     .segment_offsets = nullptr,
                                     .size = rows.size()};
    ExpectBothFilterTypes(
        TermJsonFieldInVariableKernel<std::string>{.pointer = "/a",
                                                   .terms = &terms},
        batch,
        {Tri::kTrue, Tri::kFalse, Tri::kUnknown, Tri::kUnknown},
        "json field in variable string");
}

TEST(ScanKernelTermTest, JsonVariableInField) {
    std::vector<Json> rows;
    rows.push_back(MakeJson(R"({"arr": [1, 2, 3]})"));
    rows.push_back(MakeJson(R"({"arr": [1, "2", 3]})"));
    rows.push_back(MakeJson(R"({"arr": ["x", 2]})"));
    rows.push_back(MakeJson(R"({"arr": []})"));
    rows.push_back(MakeJson(R"({"arr": 2})"));
    rows.push_back(MakeJson(R"({})"));
    rows.push_back(MakeJson(R"({"arr": null})"));
    const CandidateBatch<Json> batch{.data = rows.data(),
                                     .validity = ValidityView{},
                                     .candidates = TargetBitmapView{},
                                     .segment_offsets = nullptr,
                                     .size = rows.size()};
    ExpectBothFilterTypes(
        TermJsonVariableInFieldKernel<int64_t>{.pointer = "/arr",
                                               .target_val = 2},
        batch,
        {Tri::kTrue,
         Tri::kFalse,     // mismatched element skipped, no match
         Tri::kTrue,      // mismatched element skipped, later match
         Tri::kFalse,     // empty array
         Tri::kUnknown,   // not an array
         Tri::kUnknown,   // missing path
         Tri::kUnknown},  // JSON null
        "json variable in field");
}

TEST(ScanKernelTermTest, ArrayFieldInVariable) {
    Int64Arrays arrays({{1, 2}, {5}, {3, 4}, {7, 9}, {0, 9}});
    FixedVector<bool> valid(5, true);
    valid[3] = false;
    TargetBitmap cand(5, true);
    cand[4] = false;
    SetElement<int64_t> terms(std::vector<int64_t>{2, 9});

    const CandidateBatch<ArrayView> batch{
        .data = arrays.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView(cand),
        .segment_offsets = nullptr,
        .size = 5};
    const TermArrayFieldInVariableKernel<int64_t> kernel{.index = 1,
                                                         .term_set = &terms};
    ExpectBothFilterTypes(kernel,
                          batch,
                          {Tri::kTrue,
                           Tri::kUnknown,  // index out of range
                           Tri::kFalse,
                           Tri::kUnknown,  // NULL row
                           Tri::kFalse},   // non-candidate
                          "array field in variable");

    const TermArrayFieldInVariableKernel<int64_t> no_path{.index = -1,
                                                          .term_set = &terms};
    OutBuffer out(5, 0);
    EXPECT_ANY_THROW(no_path.Eval<FilterType::sequential>(batch, out.Out()));
}

TEST(ScanKernelTermTest, ArrayVariableInFieldString) {
    StringArrays arrays({{"a", "bb"}, {"ccc"}, {"bb"}, {"xbb", "b"}});
    FixedVector<bool> valid(4, true);
    valid[2] = false;
    const CandidateBatch<ArrayView> batch{
        .data = arrays.data(),
        .validity = ValidityView::FromExpanded(valid.data()),
        .candidates = TargetBitmapView{},
        .segment_offsets = nullptr,
        .size = 4};
    ExpectBothFilterTypes(
        TermArrayVariableInFieldKernel<std::string>{.target_val = "bb"},
        batch,
        {Tri::kTrue, Tri::kFalse, Tri::kUnknown, Tri::kFalse},
        "array variable in field string");
}
