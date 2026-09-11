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
#include <ostream>
#include <string>
#include <vector>

#include "common/Json.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/Expr.h"
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

// Output buffers initialized the way Scan initializes them: match=0, known=1.
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

}  // namespace
}  // namespace milvus::exec
