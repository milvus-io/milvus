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

#pragma once

#include <fmt/core.h>
#include <folly/Unit.h>

#include <optional>
#include <utility>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "exec/expression/JsonNumberComparison.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "segcore/SegmentInterface.h"
#include "query/Utils.h"
#include "common/RegexQuery.h"
#include "common/Volnitsky.h"
#include "index/NgramInvertedIndex.h"
#include "exec/expression/Utils.h"
#include "common/bson_view.h"
#include "index/json_stats/bson_inverted.h"
#include "cachinglayer/CacheSlot.h"
#include "index/NgramInvertedIndex.h"

namespace milvus {
namespace exec {

// Optional context for UnaryCompare to hold pre-built objects that are
// expensive to construct per-row (e.g. LikePatternMatcher for Match ops).
// Callers on hot paths should pre-construct and reuse across rows.
struct UnaryCompareContext {
    const LikePatternMatcher* like_matcher = nullptr;
    const PartialRegexMatcher* regex_matcher = nullptr;
};

template <typename T, typename U>
bool
UnaryCompare(const T& get_value,
             const U& val,
             proto::plan::OpType op_type,
             const UnaryCompareContext* context = nullptr) {
    switch (op_type) {
        case proto::plan::GreaterThan:
            return get_value > val;
        case proto::plan::GreaterEqual:
            return get_value >= val;
        case proto::plan::LessThan:
            return get_value < val;
        case proto::plan::LessEqual:
            return get_value <= val;
        case proto::plan::Equal:
            return get_value == val;
        case proto::plan::NotEqual:
            return get_value != val;
        case proto::plan::InnerMatch:
        case proto::plan::PostfixMatch:
        case proto::plan::PrefixMatch:
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                return milvus::query::Match(get_value, val, op_type);
            } else {
                ThrowInfo(OpTypeInvalid,
                          "PrefixMatch/PostfixMatch/InnerMatch only supports "
                          "string type");
            }
        case proto::plan::Match:
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                if (context && context->like_matcher) {
                    return (*context->like_matcher)(get_value);
                }
                LikePatternMatcher fallback(val);
                return fallback(get_value);
            } else {
                ThrowInfo(OpTypeInvalid,
                          "Match operation only supports string type");
            }
        case proto::plan::RegexMatch:
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                if (context && context->regex_matcher) {
                    return (*context->regex_matcher)(get_value);
                }
                PartialRegexMatcher fallback(val);
                return fallback(get_value);
            } else {
                ThrowInfo(OpTypeInvalid,
                          "RegexMatch operation only supports string type");
            }
        default:
            ThrowInfo(UnexpectedError,
                      fmt::format("unsupported op_type:{} for UnaryCompare",
                                  op_type));
    }
}

template <typename T>
struct UnaryIndexFuncForMatch {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, IndexInnerType val, proto::plan::OpType op) {
        AssertInfo(op == proto::plan::OpType::Match ||
                       op == proto::plan::OpType::PostfixMatch ||
                       op == proto::plan::OpType::InnerMatch ||
                       op == proto::plan::OpType::PrefixMatch,
                   "op must be one of the following: Match, PrefixMatch, "
                   "PostfixMatch, InnerMatch");

        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            if (index->SupportPatternMatch()) {
                return index->PatternMatch(val, op);
            }

            if (!index->HasRawData()) {
                ThrowInfo(Unsupported,
                          "index don't support pattern match and don't have "
                          "raw data");
            }
            // retrieve raw data to do brute force query, may be very slow.
            auto cnt = index->Count();
            TargetBitmap res(cnt);
            if (op == proto::plan::OpType::InnerMatch ||
                op == proto::plan::OpType::PostfixMatch ||
                op == proto::plan::OpType::PrefixMatch) {
                for (int64_t i = 0; i < cnt; i++) {
                    auto raw = index->Reverse_Lookup(i);
                    if (!raw.has_value()) {
                        res[i] = false;
                        continue;
                    }
                    res[i] = milvus::query::Match(raw.value(), val, op);
                }
                return res;
            } else {
                LikePatternMatcher matcher(val);
                for (int64_t i = 0; i < cnt; i++) {
                    auto raw = index->Reverse_Lookup(i);
                    if (!raw.has_value()) {
                        res[i] = false;
                        continue;
                    }
                    res[i] = matcher(raw.value());
                }
                return res;
            }
        }
        ThrowInfo(ErrorCode::Unsupported,
                  "UnaryIndexFuncForMatch is only supported on string types");
    }
};

template <typename T, proto::plan::OpType op>
struct UnaryIndexFunc {
    using IndexInnerType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, IndexInnerType val) {
        if constexpr (op == proto::plan::OpType::Equal) {
            return index->In(1, &val);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            return index->NotIn(1, &val);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            return index->Range(val, OpType::GreaterThan);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            return index->Range(val, OpType::LessThan);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            return index->Range(val, OpType::GreaterEqual);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            return index->Range(val, OpType::LessEqual);
        } else if constexpr (op == proto::plan::OpType::PrefixMatch ||
                             op == proto::plan::OpType::Match ||
                             op == proto::plan::OpType::PostfixMatch ||
                             op == proto::plan::OpType::InnerMatch) {
            UnaryIndexFuncForMatch<T> func;
            return func(index, val, op);
        } else if constexpr (op == proto::plan::OpType::RegexMatch) {
            if constexpr (std::is_same_v<T, std::string> ||
                          std::is_same_v<T, std::string_view>) {
                // Prefer PatternMatch which iterates unique values
                // (O(unique) vs O(total_rows) for Reverse_Lookup)
                if (index->SupportPatternMatch()) {
                    return index->PatternMatch(val, op);
                }
                // Fallback to Reverse_Lookup for indexes without
                // PatternMatch support
                if (!index->HasRawData()) {
                    ThrowInfo(Unsupported,
                              "index doesn't have raw data for RegexMatch");
                }
                auto cnt = index->Count();
                TargetBitmap res(cnt);
                PartialRegexMatcher matcher(val);
                for (int64_t i = 0; i < cnt; i++) {
                    auto raw = index->Reverse_Lookup(i);
                    if (!raw.has_value()) {
                        res[i] = false;
                        continue;
                    }
                    res[i] = matcher(raw.value());
                }
                return res;
            }
            ThrowInfo(ErrorCode::Unsupported,
                      "RegexMatch is only supported on string types");
        } else {
            ThrowInfo(
                UnexpectedError,
                fmt::format("unsupported op_type:{} for UnaryIndexFunc", op));
        }
    }
};

// ---------------------------------------------------------------------------
// Scan kernels for PhyUnaryRangeFilterExpr
// ---------------------------------------------------------------------------

// How an integral literal that does not fit the column type T resolves.
enum class UnaryOverflow : uint8_t { None, AllFalse, AllTrue };

// Resolves an integral literal outside T's range to an all-FALSE or all-TRUE
// predicate.
template <typename T>
UnaryOverflow
ClassifyUnaryOverflow(proto::plan::OpType op,
                      const proto::plan::GenericValue& value) {
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        const auto val = GetValueFromProto<int64_t>(value);
        if (!milvus::query::out_of_range<T>(val)) {
            return UnaryOverflow::None;
        }
        switch (op) {
            case proto::plan::GreaterThan:
            case proto::plan::GreaterEqual:
                return milvus::query::lt_lb<T>(val) ? UnaryOverflow::AllTrue
                                                    : UnaryOverflow::AllFalse;
            case proto::plan::LessThan:
            case proto::plan::LessEqual:
                return milvus::query::gt_ub<T>(val) ? UnaryOverflow::AllTrue
                                                    : UnaryOverflow::AllFalse;
            case proto::plan::Equal:
                return UnaryOverflow::AllFalse;
            case proto::plan::NotEqual:
                return UnaryOverflow::AllTrue;
            default:
                ThrowInfo(UnexpectedError, "unsupported range node {}", op);
        }
    }
    return UnaryOverflow::None;
}

// Visits positions that are candidates and non-NULL. KernelAdapter and
// EvalKernel fold the others, so kernels only spend work on these rows.
template <typename T, typename Fn>
inline void
ForEachCandidateRow(const CandidateBatch<T>& b, Fn&& fn) {
    const bool has_candidates = !b.candidates.empty();
    for (size_t i = 0; i < b.size; ++i) {
        if ((has_candidates && !b.candidates[i]) ||
            (b.validity && !b.validity[i])) {
            continue;
        }
        fn(i);
    }
}

// Scalar columns (bool / integral / floating / VARCHAR) and element-level
// ARRAY elements of those types.
template <typename T>
struct UnaryRangeKernel {
    using ValueType =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

    proto::plan::OpType op{proto::plan::OpType::Invalid};
    // T{} when overflow != None: GetValueFromProto returns T() on overflow.
    ValueType val{};
    UnaryOverflow overflow{UnaryOverflow::None};
    // Owned by PhyUnaryRangeFilterExpr. like_matcher is required for Match and
    // regex_matcher for RegexMatch on string columns; volnitsky may be null.
    const LikePatternMatcher* like_matcher{nullptr};
    const PartialRegexMatcher* regex_matcher{nullptr};
    const VolnitskySearcher* volnitsky{nullptr};
    milvus::OpContext* op_ctx{nullptr};

    bool
    AlwaysFalse() const {
        return overflow == UnaryOverflow::AllFalse;
    }

    bool
    AlwaysTrue() const {
        return overflow == UnaryOverflow::AllTrue;
    }

    bool
    CanSkip(const SkipIndex& skip_index,
            FieldId field_id,
            int64_t chunk) const {
        // val is T{} for an overflowed literal; chunk min/max must not be
        // compared against it (e.g. int8 `< 300` would prune an all-zero
        // chunk to FALSE).
        if (overflow != UnaryOverflow::None) {
            return false;
        }
        return skip_index.CanSkipUnaryRange<T>(
            op_ctx, field_id, chunk, op, val);
    }

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& b, TriStateOut out) const {
        if (overflow == UnaryOverflow::AllTrue) {
            out.match.set();
            return;
        }
        if (overflow == UnaryOverflow::AllFalse) {
            return;
        }
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            EvalString(b, out);
        } else {
            EvalScalar<filter_type>(b, out);
        }
    }

 private:
    template <typename Pred>
    static void
    MatchRows(const CandidateBatch<T>& b, TriStateOut& out, Pred&& pred) {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            if (pred(b.data[i])) {
                out.SetTrue(i);
            }
        }
    }

    template <FilterType filter_type>
    void
    EvalScalar(const CandidateBatch<T>& b, TriStateOut& out) const {
        using milvus::bitset::CompareOpType;
        std::optional<CompareOpType> cmp;
        switch (op) {
            case proto::plan::GreaterThan:
                cmp = CompareOpType::GT;
                break;
            case proto::plan::GreaterEqual:
                cmp = CompareOpType::GE;
                break;
            case proto::plan::LessThan:
                cmp = CompareOpType::LT;
                break;
            case proto::plan::LessEqual:
                cmp = CompareOpType::LE;
                break;
            case proto::plan::Equal:
                cmp = CompareOpType::EQ;
                break;
            case proto::plan::NotEqual:
                cmp = CompareOpType::NE;
                break;
            case proto::plan::PrefixMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::InnerMatch:
                // Non-string T: query::Match's generic overload throws
                // NotImplemented.
                MatchRows(b, out, [this](const T& x) {
                    return milvus::query::Match(x, val, op);
                });
                return;
            case proto::plan::Match:
                ThrowInfo(OpTypeInvalid,
                          "Match operation only supports string type");
            case proto::plan::RegexMatch:
                ThrowInfo(OpTypeInvalid,
                          "RegexMatch operation only supports string type");
            default:
                ThrowInfo(
                    UnexpectedError,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op));
        }
        if constexpr (filter_type == FilterType::sequential) {
            // SIMD over the whole sub-batch; KernelAdapter and EvalKernel
            // clear NULL and non-candidate rows.
            out.match.inplace_compare_val<T>(b.data, b.size, val, *cmp);
        } else {
            MatchRows(b, out, [this](const T& x) {
                return UnaryCompare(x, val, op);
            });
        }
    }

    void
    EvalString(const CandidateBatch<T>& b, TriStateOut& out) const {
        switch (op) {
            case proto::plan::GreaterThan:
                return MatchRows(
                    b, out, [this](const T& x) { return x > val; });
            case proto::plan::GreaterEqual:
                return MatchRows(
                    b, out, [this](const T& x) { return x >= val; });
            case proto::plan::LessThan:
                return MatchRows(
                    b, out, [this](const T& x) { return x < val; });
            case proto::plan::LessEqual:
                return MatchRows(
                    b, out, [this](const T& x) { return x <= val; });
            case proto::plan::Equal:
                return MatchRows(
                    b, out, [this](const T& x) { return x == val; });
            case proto::plan::NotEqual:
                return MatchRows(
                    b, out, [this](const T& x) { return x != val; });
            case proto::plan::PrefixMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::InnerMatch:
                return MatchRows(b, out, [this](const T& x) {
                    return milvus::query::Match(x, val, op);
                });
            case proto::plan::Match: {
                AssertInfo(like_matcher != nullptr,
                           "LIKE matcher is required for Match");
                const auto* m = like_matcher;
                return MatchRows(b, out, [m](const T& x) { return (*m)(x); });
            }
            case proto::plan::RegexMatch: {
                AssertInfo(regex_matcher != nullptr,
                           "regex matcher is required for RegexMatch");
                const auto* m = regex_matcher;
                if (volnitsky != nullptr) {
                    const auto* s = volnitsky;
                    return MatchRows(b, out, [m, s](const T& x) {
                        return s->contains(x) && (*m)(x);
                    });
                }
                return MatchRows(b, out, [m](const T& x) { return (*m)(x); });
            }
            default:
                ThrowInfo(
                    UnexpectedError,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op));
        }
    }
};

// JSON column, literal of type ExprValueType at `pointer`.
template <typename ExprValueType>
struct UnaryJsonKernel {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;

    proto::plan::OpType op{proto::plan::OpType::Invalid};
    ExprValueType val{};
    std::string pointer;
    // Only for ExprValueType == std::string.
    const LikePatternMatcher* like_matcher{nullptr};
    const PartialRegexMatcher* regex_matcher{nullptr};

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        switch (op) {
            case proto::plan::GreaterThan:
                return CompareValues(
                    b, out, [this](const auto& value) { return value > val; });
            case proto::plan::GreaterEqual:
                return CompareValues(
                    b, out, [this](const auto& value) { return value >= val; });
            case proto::plan::LessThan:
                return CompareValues(
                    b, out, [this](const auto& value) { return value < val; });
            case proto::plan::LessEqual:
                return CompareValues(
                    b, out, [this](const auto& value) { return value <= val; });
            case proto::plan::Equal:
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return CompareArrayLiteral(b, out, /*negate=*/false);
                } else {
                    return CompareValues(b, out, [this](const auto& value) {
                        return value == val;
                    });
                }
            case proto::plan::NotEqual:
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    return CompareArrayLiteral(b, out, /*negate=*/true);
                } else {
                    return CompareValues(b, out, [this](const auto& value) {
                        return value != val;
                    });
                }
            case proto::plan::InnerMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::PrefixMatch:
                return CompareValues(b, out, [this](const auto& value) {
                    return milvus::query::Match(value, val, op);
                });
            case proto::plan::Match:
                if constexpr (std::is_same_v<ExprValueType, std::string>) {
                    AssertInfo(like_matcher != nullptr,
                               "LIKE matcher is required for Match");
                    const auto* m = like_matcher;
                    return CompareValues(
                        b, out, [m](const auto& value) { return (*m)(value); });
                } else {
                    ThrowInfo(OpTypeInvalid,
                              "Match operation only supports string type");
                }
            case proto::plan::RegexMatch:
                if constexpr (std::is_same_v<ExprValueType, std::string>) {
                    AssertInfo(regex_matcher != nullptr,
                               "regex matcher is required for RegexMatch");
                    const auto* m = regex_matcher;
                    return CompareValues(
                        b, out, [m](const auto& value) { return (*m)(value); });
                } else {
                    ThrowInfo(OpTypeInvalid,
                              "RegexMatch operation only supports string type");
                }
            default:
                ThrowInfo(
                    UnexpectedError,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op));
        }
    }

 private:
    // Missing path or type mismatch -> UNKNOWN.
    template <typename Cmp>
    void
    CompareValues(const CandidateBatch<milvus::Json>& b,
                  TriStateOut& out,
                  Cmp&& cmp) const {
        ForEachCandidateRow(b, [&](size_t i) {
            if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                // Ordering / pattern ops on an array literal: FALSE and known.
                return;
            } else if constexpr (std::is_same_v<GetType, int64_t>) {
                auto number = b.data[i].at_numeric(pointer);
                if (number.error()) {
                    out.SetUnknown(i);
                    return;
                }
                auto n = number.value();
                bool hit;
                if (n.is_int64()) {
                    hit = cmp(n.get_int64());
                } else {
                    const double value =
                        n.is_uint64() ? static_cast<double>(n.get_uint64())
                                      : n.get_double();
                    hit = cmp(value);
                }
                if (hit) {
                    out.SetTrue(i);
                }
            } else {
                auto x = b.data[i].template at<GetType>(pointer);
                if (x.error()) {
                    out.SetUnknown(i);
                    return;
                }
                if (cmp(x.value())) {
                    out.SetTrue(i);
                }
            }
        });
    }

    void
    CompareArrayLiteral(const CandidateBatch<milvus::Json>& b,
                        TriStateOut& out,
                        bool negate) const {
        ForEachCandidateRow(b, [&](size_t i) {
            // doc must outlive the array handle.
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                return;
            }
            if (CompareTwoJsonArray(array, val) != negate) {
                out.SetTrue(i);
            }
        });
    }
};

// JSON column, int64 literal that does not round-trip through double.
struct UnaryJsonPreciseNumericKernel {
    proto::plan::OpType op{proto::plan::OpType::Invalid};
    std::string pointer;
    const proto::plan::GenericValue* bound{
        nullptr};  // owned by the logical expr

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        ForEachCandidateRow(b, [&](size_t i) {
            auto number = b.data[i].at_numeric(pointer);
            if (number.error()) {
                out.SetUnknown(i);
                return;
            }
            auto comparison = CompareJsonNumberToBound(number.value(), *bound);
            if (comparison.has_value() &&
                JsonNumberMatchesOp(*comparison, op)) {
                out.SetTrue(i);
            }
        });
    }
};

// Row-level ARRAY column: `arr[index] op val`, or whole-array `arr op [..]`
// when ValueType is proto::plan::Array.
template <typename ValueType>
struct UnaryArrayKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;

    proto::plan::OpType op{proto::plan::OpType::Invalid};
    ValueType val{};
    int index{-1};
    // Only for ValueType == std::string.
    const LikePatternMatcher* like_matcher{nullptr};
    const PartialRegexMatcher* regex_matcher{nullptr};

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::ArrayView>& b, TriStateOut out) const {
        if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
            EvalWholeArray(b, out);
        } else {
            AssertInfo(index >= 0,
                       "array element predicate requires nested path");
            EvalElement(b, out);
        }
    }

 private:
    void
    EvalWholeArray(const CandidateBatch<milvus::ArrayView>& b,
                   TriStateOut& out) const {
        switch (op) {
            case proto::plan::Equal:
            case proto::plan::NotEqual: {
                const bool negate = op == proto::plan::NotEqual;
                return ForEachCandidateRow(b, [&](size_t i) {
                    if (b.data[i].is_same_array(val) != negate) {
                        out.SetTrue(i);
                    }
                });
            }
            case proto::plan::GreaterThan:
            case proto::plan::GreaterEqual:
            case proto::plan::LessThan:
            case proto::plan::LessEqual:
            case proto::plan::PrefixMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::InnerMatch:
                // Ordering / pattern ops on an array literal: FALSE and known.
                return;
            case proto::plan::Match:
                return ForEachCandidateRow(b, [](size_t) {
                    ThrowInfo(OpTypeInvalid,
                              "Match operation is not supported for nested "
                              "Array type");
                });
            case proto::plan::RegexMatch:
                return ForEachCandidateRow(b, [](size_t) {
                    ThrowInfo(OpTypeInvalid,
                              "RegexMatch operation is not supported for "
                              "nested Array type");
                });
            default:
                ThrowInfo(
                    UnexpectedError,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op));
        }
    }

    // Missing subscript -> UNKNOWN.
    template <typename Cmp>
    void
    CompareElement(const CandidateBatch<milvus::ArrayView>& b,
                   TriStateOut& out,
                   Cmp&& cmp) const {
        ForEachCandidateRow(b, [&](size_t i) {
            const auto& row = b.data[i];
            if (index >= row.length()) {
                out.SetUnknown(i);
                return;
            }
            if (cmp(row.template get_data<GetType>(index))) {
                out.SetTrue(i);
            }
        });
    }

    void
    EvalElement(const CandidateBatch<milvus::ArrayView>& b,
                TriStateOut& out) const {
        switch (op) {
            case proto::plan::GreaterThan:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x > val; });
            case proto::plan::GreaterEqual:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x >= val; });
            case proto::plan::LessThan:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x < val; });
            case proto::plan::LessEqual:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x <= val; });
            case proto::plan::Equal:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x == val; });
            case proto::plan::NotEqual:
                return CompareElement(
                    b, out, [this](const GetType& x) { return x != val; });
            case proto::plan::PrefixMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::InnerMatch:
                return CompareElement(b, out, [this](const GetType& x) {
                    return milvus::query::Match(x, val, op);
                });
            case proto::plan::Match:
                if constexpr (std::is_same_v<GetType, std::string_view>) {
                    AssertInfo(like_matcher != nullptr,
                               "LIKE matcher is required for Match");
                    const auto* m = like_matcher;
                    return CompareElement(
                        b, out, [m](std::string_view x) { return (*m)(x); });
                } else {
                    return ForEachCandidateRow(b, [](size_t) {
                        ThrowInfo(OpTypeInvalid,
                                  "Match operation only supports string type");
                    });
                }
            case proto::plan::RegexMatch:
                if constexpr (std::is_same_v<GetType, std::string_view>) {
                    AssertInfo(regex_matcher != nullptr,
                               "regex matcher is required for RegexMatch");
                    const auto* m = regex_matcher;
                    return CompareElement(
                        b, out, [m](std::string_view x) { return (*m)(x); });
                } else {
                    return ForEachCandidateRow(b, [](size_t) {
                        ThrowInfo(
                            OpTypeInvalid,
                            "RegexMatch operation only supports string type");
                    });
                }
            default:
                ThrowInfo(
                    UnexpectedError,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op));
        }
    }
};

// Recheck of FMINDEX LIKE candidates against VARCHAR raw data.
struct LikeMatchRecheckKernel {
    const LikePatternMatcher* matcher{nullptr};

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<std::string_view>& b, TriStateOut out) const {
        ForEachCandidateRow(b, [&](size_t i) {
            if ((*matcher)(b.data[i])) {
                out.SetTrue(i);
            }
        });
    }
};

static_assert(ScanKernel<UnaryRangeKernel<int64_t>, int64_t>);
static_assert(ScanKernel<UnaryRangeKernel<std::string_view>, std::string_view>);
static_assert(ScanKernel<UnaryJsonKernel<std::string>, milvus::Json>);
static_assert(ScanKernel<UnaryJsonPreciseNumericKernel, milvus::Json>);
static_assert(
    ScanKernel<UnaryArrayKernel<proto::plan::Array>, milvus::ArrayView>);
static_assert(ScanKernel<LikeMatchRecheckKernel, std::string_view>);
static_assert(KernelCanSkip<UnaryRangeKernel<int64_t>> &&
              KernelAlwaysFalse<UnaryRangeKernel<int64_t>> &&
              KernelAlwaysTrue<UnaryRangeKernel<int64_t>>);

template <typename T, typename U>
void
BatchUnaryCompare(const T* src,
                  size_t size,
                  U& val,
                  proto::plan::OpType op_type,
                  TargetBitmapView res) {
    if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
        using milvus::bitset::CompareOpType;
        switch (op_type) {
            case proto::plan::GreaterThan: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::GT);
                return;
            }
            case proto::plan::GreaterEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::GE);
                return;
            }
            case proto::plan::LessThan: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::LT);
                return;
            }
            case proto::plan::LessEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::LE);
                return;
            }
            case proto::plan::Equal: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::EQ);
                return;
            }
            case proto::plan::NotEqual: {
                res.inplace_compare_val<T>(
                    src, size, static_cast<T>(val), CompareOpType::NE);
                return;
            }
            default:
                break;
        }
    }
    switch (op_type) {
        case proto::plan::GreaterThan: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] > val;
            }
            break;
        }
        case proto::plan::GreaterEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] >= val;
            }
            break;
        }
        case proto::plan::LessThan: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] < val;
            }
            break;
        }
        case proto::plan::LessEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] <= val;
            }
            break;
        }
        case proto::plan::Equal: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] == val;
            }
            break;
        }
        case proto::plan::NotEqual: {
            for (int i = 0; i < size; ++i) {
                res[i] = src[i] != val;
            }
            break;
        }
        case proto::plan::InnerMatch:
        case proto::plan::PostfixMatch:
        case proto::plan::PrefixMatch: {
            for (int i = 0; i < size; ++i) {
                res[i] = milvus::query::Match(src[i], val, op_type);
            }
            break;
        }
        case proto::plan::Match: {
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                LikePatternMatcher matcher(val);
                for (int i = 0; i < size; ++i) {
                    res[i] = matcher(src[i]);
                }
                break;
            }
        }
        case proto::plan::RegexMatch: {
            if constexpr (std::is_same_v<U, std::string> ||
                          std::is_same_v<U, std::string_view>) {
                PartialRegexMatcher matcher(val);
                for (int i = 0; i < size; ++i) {
                    res[i] = matcher(src[i]);
                }
                break;
            }
        }
        default: {
            ThrowInfo(
                UnexpectedError,
                fmt::format("unsupported op_type:{} for BatchUnaryCompare",
                            op_type));
        }
    }
}

template <typename GetType, typename ValType>
class ShreddingExecutor {
    using InnerType =
        std::conditional_t<std::is_same_v<GetType, std::string_view>,
                           std::string,
                           GetType>;

 public:
    ShreddingExecutor(proto::plan::OpType op_type,
                      const std::string& pointer,
                      ValType val)
        : op_type_(op_type), val_(val), pointer_(pointer) {
    }

    void
    operator()(const GetType* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "need using ShreddingArrayBsonExecutor for array type in "
                      "shredding data");
        } else {
            ExecuteOperation(src, size, res);
            ApplyValidMask(valid, res, valid_res, size);
        }
    }

 private:
    void
    ExecuteOperation(const GetType* src, size_t size, TargetBitmapView res) {
        BatchUnaryCompare<GetType, InnerType>(src, size, val_, op_type_, res);
    }

    proto::plan::OpType op_type_;
    InnerType val_;
    std::string pointer_;
};

// Executor for shredding ARRAY type stored as BSON binary in variable-length
// columns (std::string_view). Only supports Equal/NotEqual.
class ShreddingArrayBsonExecutor {
 public:
    ShreddingArrayBsonExecutor(proto::plan::OpType op_type,
                               const std::string& pointer,
                               const proto::plan::Array& val)
        : op_type_(op_type), val_(val), pointer_(pointer) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool equal = CompareTwoJsonArray(array_view.value(), val_);
            switch (op_type_) {
                case proto::plan::Equal:
                    res[i] = equal;
                    break;
                case proto::plan::NotEqual:
                    res[i] = !equal;
                    break;
                default:
                    ThrowInfo(UnexpectedError,
                              fmt::format("unsupported op_type:{} for ARRAY in "
                                          "ShreddingArrayBsonExecutor",
                                          op_type_));
            }
        }
    }

 private:
    proto::plan::OpType op_type_;
    const proto::plan::Array& val_;
    std::string pointer_;
};

class PhyUnaryRangeFilterExpr : public SegmentExpr {
 public:
    PhyUnaryRangeFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level,
        const query::PlanOptions& plan_options = {},
        bool enable_sub_expr_cache_write = true)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      FromValCase(expr->val_.val_case()),
                      active_count,
                      batch_size,
                      consistency_level,
                      false,
                      false,
                      plan_options),
          expr_(expr),
          enable_sub_expr_cache_write_(enable_sub_expr_cache_write) {
        if (expr_->op_type_ == proto::plan::OpType::Match) {
            EnsureLikeMatcherCache();
        }
        auto val_type = FromValCase(expr_->val_.val_case());
        if ((val_type == DataType::STRING || val_type == DataType::VARCHAR) &&
            (expr_->op_type_ == proto::plan::OpType::InnerMatch ||
             expr_->op_type_ == proto::plan::OpType::Match ||
             expr_->op_type_ == proto::plan::OpType::PrefixMatch ||
             expr_->op_type_ == proto::plan::OpType::PostfixMatch ||
             expr_->op_type_ == proto::plan::OpType::RegexMatch)) {
            // try to pin ngram index for json
            auto field_id = expr_->column_.field_id_;
            auto schema = segment->get_schema_snapshot();
            auto field_meta = (*schema)[field_id];

            if (field_meta.is_json()) {
                auto pointer =
                    milvus::Json::pointer(expr_->column_.nested_path_);
                pinned_ngram_index_ =
                    segment->GetNgramIndexForJson(op_ctx_, field_id, pointer);
            } else {
                pinned_ngram_index_ = segment->GetNgramIndex(op_ctx_, field_id);
            }
        }
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override;

    bool
    SupportOffsetInput() override {
        if (IsTextIndexOpType(expr_->op_type_)) {
            return false;
        }
        return true;
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

    bool
    IsSource() const override {
        return true;
    }

    std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr>
    GetLogicalExpr() {
        return expr_;
    }

    int64_t
    GetActiveCount() const {
        return active_count_;
    }

    // The concrete string literal to hand to a scalar index's ShouldUseOp cost
    // guard, for the anchored pattern ops (PrefixMatch/PostfixMatch/InnerMatch)
    // and general LIKE (Match) whose index cost depends on the literal. Empty
    // for every other op (including the equality family, which FMINDEX declines
    // outright), so the guard is judged on the op alone. Lets FMINDEX decline
    // degenerate high-hit LIKE literals to the raw-data scan on the VARCHAR path.
    std::string
    StringLiteralForCostGuard() const;

    // Check if ngram index can be used (index exists + literal is valid + no offset input)
    bool
    CanUseNgramIndex() const override;

    // Execute ngram Phase1 only (index query), ANDs result into candidates
    // Requires: CanUseNgramIndex() == true
    // Requires: candidates must be non-empty (caller initializes with all-true,
    //           then ANDs with pre_filter/offset_input before calling)
    void
    ExecuteNgramPhase1(TargetBitmap& candidates);

    // Execute ngram Phase2 (post-filter verification) on candidate bitset
    // - segment_offset: starting position in segment
    // - batch_size: number of rows to process
    // - candidates: bitmap of size batch_size
    // Requires: CanUseNgramIndex() == true
    void
    ExecuteNgramPhase2(TargetBitmap& candidates,
                       int64_t segment_offset,
                       int64_t batch_size);

 private:
    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplJson(EvalCtx& context);

    VectorPtr
    ExecRangeVisitorImplJsonPreciseNumeric(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplJsonByStats();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForPk(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplArray(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplArrayForIndex(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecArrayEqualForIndex(EvalCtx& context, bool reverse);

    // Check overflow and cache result for performace
    template <typename T>
    ColumnVectorPtr
    PreCheckOverflow(int64_t batch_size);

    template <typename T>
    bool
    CanUseIndexForArray();

    VectorPtr
    ExecTextMatch();

    std::optional<VectorPtr>
    ExecNgramMatch(EvalCtx& context);

    bool
    CanUseFMMatch();

    bool
    PinnedIndexIsFMIndex() const;

    std::optional<VectorPtr>
    ExecFMMatch(EvalCtx& context);

    static std::pair<std::string, std::string>
    SplitAtFirstSlashDigit(std::string input);

    void
    PrefetchRawData() override;

    template <typename T>
    void
    PrefetchRawData();

 private:
    std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr> expr_;
    bool arg_inited_{false};
    SingleElement value_arg_;
    PinWrapper<index::NgramInvertedIndex*> pinned_ngram_index_{nullptr};
    PinWrapper<index::BsonInvertedIndex*> bson_index_{nullptr};
    bool enable_sub_expr_cache_write_{true};

    // Cached regex objects — constructed once per segment, reused across batches.
    bool regex_cache_inited_{false};
    std::unique_ptr<PartialRegexMatcher> cached_regex_matcher_;
    std::string cached_volnitsky_literal_;
    std::unique_ptr<VolnitskySearcher> cached_volnitsky_searcher_;

    void
    EnsureRegexCache() {
        if (regex_cache_inited_)
            return;
        regex_cache_inited_ = true;
        if (expr_->op_type_ != proto::plan::OpType::RegexMatch)
            return;
        auto pattern = GetValueFromProto<std::string>(expr_->val_);
        cached_regex_matcher_ = std::make_unique<PartialRegexMatcher>(pattern);
        auto lits = index::extract_literals_from_regex(pattern);
        for (const auto& l : lits) {
            if (l.size() > cached_volnitsky_literal_.size())
                cached_volnitsky_literal_ = l;
        }
        if (!cached_volnitsky_literal_.empty()) {
            cached_volnitsky_searcher_ =
                std::make_unique<VolnitskySearcher>(cached_volnitsky_literal_);
        }
    }

    // Cached LIKE pattern matcher — constructed once per segment, reused
    // across batches (the pattern is an expression constant).
    bool like_cache_inited_{false};
    std::unique_ptr<LikePatternMatcher> cached_like_matcher_;

    void
    EnsureLikeMatcherCache() {
        if (like_cache_inited_)
            return;
        like_cache_inited_ = true;
        if (expr_->op_type_ != proto::plan::OpType::Match)
            return;
        auto pattern = GetValueFromProto<std::string>(expr_->val_);
        cached_like_matcher_ = std::make_unique<LikePatternMatcher>(pattern);
    }
};
}  // namespace exec
}  // namespace milvus
