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
#include <simdjson.h>
#include <stdint.h>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "bitset/common.h"
#include "cachinglayer/CacheSlot.h"
#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "common/ValidityView.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/Element.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/JsonNumberComparison.h"
#include "expr/ITypeExpr.h"
#include "index/ScalarIndex.h"
#include "index/SkipIndex.h"
#include "index/json_stats/bson_inverted.h"
#include "pb/plan.pb.h"
#include "query/Utils.h"
#include "segcore/SegmentInterface.h"
#include "simdjson/error.h"

namespace milvus {
namespace exec {

template <typename T>
using BinaryRangeIndexInnerType =
    std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

template <typename T>
using BinaryRangeHighPrecisionType =
    std::conditional_t<std::is_integral_v<BinaryRangeIndexInnerType<T>> &&
                           !std::is_same_v<bool, T>,
                       int64_t,
                       BinaryRangeIndexInnerType<T>>;

template <bool lower_inclusive, bool upper_inclusive, typename B, typename V>
inline bool
BinaryRangeContains(const B& lower, const V& value, const B& upper) {
    if constexpr (lower_inclusive && upper_inclusive) {
        return lower <= value && value <= upper;
    } else if constexpr (lower_inclusive) {
        return lower <= value && value < upper;
    } else if constexpr (upper_inclusive) {
        return lower < value && value <= upper;
    } else {
        return lower < value && value < upper;
    }
}

template <typename T>
struct BinaryRangeBounds {
    BinaryRangeHighPrecisionType<T> lower;
    BinaryRangeHighPrecisionType<T> upper;
    bool lower_inclusive;
    bool upper_inclusive;
    bool always_false;
};

template <typename T>
BinaryRangeBounds<T>
ClampBinaryRangeBounds(BinaryRangeHighPrecisionType<T> lower,
                       BinaryRangeHighPrecisionType<T> upper,
                       bool lower_inclusive,
                       bool upper_inclusive) {
    BinaryRangeBounds<T> bounds{std::move(lower),
                                std::move(upper),
                                lower_inclusive,
                                upper_inclusive,
                                false};
    if constexpr (std::is_integral_v<T> && !std::is_same_v<bool, T>) {
        if (milvus::query::gt_ub<T>(bounds.lower)) {
            bounds.always_false = true;
            return bounds;
        } else if (milvus::query::lt_lb<T>(bounds.lower)) {
            bounds.lower = std::numeric_limits<T>::min();
            bounds.lower_inclusive = true;
        }

        if (milvus::query::gt_ub<T>(bounds.upper)) {
            bounds.upper = std::numeric_limits<T>::max();
            bounds.upper_inclusive = true;
        } else if (milvus::query::lt_lb<T>(bounds.upper)) {
            bounds.always_false = true;
            return bounds;
        }
    }
    return bounds;
}

template <typename T>
struct BinaryRangeKernel {
    using HighPrecisionType = BinaryRangeHighPrecisionType<T>;

    HighPrecisionType lower;
    HighPrecisionType upper;
    bool lower_inclusive;
    bool upper_inclusive;
    bool always_false;
    milvus::OpContext* op_ctx;

    static BinaryRangeKernel
    FromBounds(const BinaryRangeBounds<T>& bounds, milvus::OpContext* op_ctx) {
        return BinaryRangeKernel{
            .lower = bounds.lower,
            .upper = bounds.upper,
            .lower_inclusive = bounds.lower_inclusive,
            .upper_inclusive = bounds.upper_inclusive,
            .always_false = bounds.always_false,
            .op_ctx = op_ctx,
        };
    }

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& b, TriStateOut out) const {
        if (lower_inclusive && upper_inclusive) {
            EvalRange<filter_type, true, true>(b, out);
        } else if (lower_inclusive) {
            EvalRange<filter_type, true, false>(b, out);
        } else if (upper_inclusive) {
            EvalRange<filter_type, false, true>(b, out);
        } else {
            EvalRange<filter_type, false, false>(b, out);
        }
    }

    bool
    CanSkip(const SkipIndex& skip_index,
            FieldId field_id,
            int64_t chunk_id) const {
        return skip_index.CanSkipBinaryRange<T>(op_ctx,
                                                field_id,
                                                chunk_id,
                                                lower,
                                                upper,
                                                lower_inclusive,
                                                upper_inclusive);
    }

    bool
    AlwaysFalse() const {
        return always_false;
    }

 private:
    template <FilterType filter_type,
              bool range_lower_inclusive,
              bool range_upper_inclusive>
    void
    EvalRange(const CandidateBatch<T>& b, TriStateOut out) const {
        const T& lo = lower;
        const T& hi = upper;
        if constexpr (filter_type == FilterType::random ||
                      std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            const bool has_candidates = !b.candidates.empty();
            for (size_t i = 0; i < b.size; ++i) {
                if (has_candidates && !b.candidates[i]) {
                    continue;
                }
                if (b.validity && !b.validity[i]) {
                    continue;
                }
                out.match[i] = BinaryRangeContains<range_lower_inclusive,
                                                   range_upper_inclusive>(
                    lo, b.data[i], hi);
            }
            return;
        }

        if constexpr (range_lower_inclusive && range_upper_inclusive) {
            out.match
                .inplace_within_range_val<T, milvus::bitset::RangeType::IncInc>(
                    lo, hi, b.data, b.size);
        } else if constexpr (range_lower_inclusive) {
            out.match
                .inplace_within_range_val<T, milvus::bitset::RangeType::IncExc>(
                    lo, hi, b.data, b.size);
        } else if constexpr (range_upper_inclusive) {
            out.match
                .inplace_within_range_val<T, milvus::bitset::RangeType::ExcInc>(
                    lo, hi, b.data, b.size);
        } else {
            out.match
                .inplace_within_range_val<T, milvus::bitset::RangeType::ExcExc>(
                    lo, hi, b.data, b.size);
        }
    }
};

static_assert(KernelCanSkip<BinaryRangeKernel<int64_t>> &&
              KernelAlwaysFalse<BinaryRangeKernel<int64_t>>);

// For int64 values, at_numeric() extracts any JSON number in one parse; uint64
// and double values fall back to double comparison, consistent with the
// Tantivy index and JSON-stats paths.
template <typename ValueType>
struct BinaryRangeJsonKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;

    ValueType lower;
    ValueType upper;
    bool lower_inclusive;
    bool upper_inclusive;
    std::string pointer;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        if (lower_inclusive && upper_inclusive) {
            EvalRange<true, true>(b, out);
        } else if (lower_inclusive) {
            EvalRange<true, false>(b, out);
        } else if (upper_inclusive) {
            EvalRange<false, true>(b, out);
        } else {
            EvalRange<false, false>(b, out);
        }
    }

 private:
    template <bool range_lower_inclusive, bool range_upper_inclusive>
    void
    EvalRange(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            if (b.validity && !b.validity[i]) {
                out.SetUnknown(i);
                continue;
            }
            if constexpr (std::is_same_v<GetType, int64_t>) {
                auto x = b.data[i].at_numeric(pointer);
                if (x.error()) {
                    out.SetUnknown(i);
                    continue;
                }
                auto n = x.value();
                if (n.is_int64()) {
                    const auto value = n.get_int64();
                    out.match[i] = BinaryRangeContains<range_lower_inclusive,
                                                       range_upper_inclusive>(
                        lower, value, upper);
                } else {
                    const auto value = n.is_uint64()
                                           ? static_cast<double>(n.get_uint64())
                                           : n.get_double();
                    out.match[i] = BinaryRangeContains<range_lower_inclusive,
                                                       range_upper_inclusive>(
                        lower, value, upper);
                }
            } else {
                auto x = b.data[i].template at<GetType>(pointer);
                if (x.error()) {
                    out.SetUnknown(i);
                    continue;
                }
                const auto value = x.value();
                out.match[i] = BinaryRangeContains<range_lower_inclusive,
                                                   range_upper_inclusive>(
                    lower, value, upper);
            }
        }
    }
};

struct BinaryRangeJsonPreciseNumericKernel {
    proto::plan::GenericValue lower_bound;
    proto::plan::GenericValue upper_bound;
    bool lower_inclusive;
    bool upper_inclusive;
    std::string pointer;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            if (b.validity && !b.validity[i]) {
                out.SetUnknown(i);
                continue;
            }
            auto number = b.data[i].at_numeric(pointer);
            if (number.error()) {
                out.SetUnknown(i);
                continue;
            }
            const auto lower_comparison =
                CompareJsonNumberToBoundWithUint64DoubleFallback(number.value(),
                                                                 lower_bound);
            const auto upper_comparison =
                CompareJsonNumberToBoundWithUint64DoubleFallback(number.value(),
                                                                 upper_bound);
            if (!lower_comparison.has_value() ||
                !upper_comparison.has_value()) {
                continue;
            }
            const bool lower_matches = lower_inclusive ? *lower_comparison >= 0
                                                       : *lower_comparison > 0;
            const bool upper_matches = upper_inclusive ? *upper_comparison <= 0
                                                       : *upper_comparison < 0;
            out.match[i] = lower_matches && upper_matches;
        }
    }
};

template <typename ValueType>
struct BinaryRangeArrayKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;

    ValueType lower;
    ValueType upper;
    bool lower_inclusive;
    bool upper_inclusive;
    int index;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::ArrayView>& b, TriStateOut out) const {
        AssertInfo(index >= 0,
                   "array element range predicate requires nested path");
        if (lower_inclusive && upper_inclusive) {
            EvalRange<true, true>(b, out);
        } else if (lower_inclusive) {
            EvalRange<true, false>(b, out);
        } else if (upper_inclusive) {
            EvalRange<false, true>(b, out);
        } else {
            EvalRange<false, false>(b, out);
        }
    }

 private:
    template <bool range_lower_inclusive, bool range_upper_inclusive>
    void
    EvalRange(const CandidateBatch<milvus::ArrayView>& b,
              TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            if (b.validity && !b.validity[i]) {
                out.SetUnknown(i);
                continue;
            }
            if (index >= b.data[i].length()) {
                out.SetUnknown(i);
                continue;
            }
            const auto value = b.data[i].template get_data<GetType>(index);
            out.match[i] =
                BinaryRangeContains<range_lower_inclusive,
                                    range_upper_inclusive>(lower, value, upper);
        }
    }
};

template <typename T>
struct BinaryRangeIndexFunc {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;
    TargetBitmap
    operator()(Index* index,
               IndexInnerType val1,
               IndexInnerType val2,
               bool lower_inclusive,
               bool upper_inclusive) {
        return index->Range(val1, lower_inclusive, val2, upper_inclusive);
    }
};

class PhyBinaryRangeFilterExpr : public SegmentExpr {
 public:
    PhyBinaryRangeFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BinaryRangeFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level,
        const query::PlanOptions& plan_options = {})
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      FromValCase(expr->lower_val_.val_case()),
                      active_count,
                      batch_size,
                      consistency_level,
                      false,
                      false,
                      plan_options),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override;

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

 private:
    template <typename T>
    BinaryRangeBounds<T>
    GetBinaryRangeBounds();

    template <typename T>
    ColumnVectorPtr
    IndexOverflowBatch(int64_t batch_size, OffsetVector* input);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson(EvalCtx& context);

    VectorPtr
    ExecRangeVisitorImplForJsonPreciseNumeric(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJsonStats(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForPk(EvalCtx& context);

    void
    PrefetchRawData() override;

    template <typename T>
    void
    PrefetchRawData();

 private:
    std::shared_ptr<const milvus::expr::BinaryRangeFilterExpr> expr_;
    SingleElement lower_arg_;
    SingleElement upper_arg_;
    bool arg_inited_{false};
    PinWrapper<index::BsonInvertedIndex*> bson_index_{nullptr};
};
}  //namespace exec
}  // namespace milvus
