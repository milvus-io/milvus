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

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T,
          bool lower_inclusive,
          bool upper_inclusive,
          FilterType filter_type = FilterType::sequential>
struct BinaryRangeElementFunc {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisionType;
    void
    operator()(T val1,
               T val2,
               const T* src,
               size_t n,
               TargetBitmapView res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        if constexpr (filter_type == FilterType::random ||
                      std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            bool has_bitmap_input = !bitmap_input.empty();
            for (size_t i = 0; i < n; ++i) {
                if (has_bitmap_input && !bitmap_input[i + start_cursor]) {
                    continue;
                }
                auto offset = (offsets) ? offsets[i] : i;
                if constexpr (lower_inclusive && upper_inclusive) {
                    res[i] = val1 <= src[offset] && src[offset] <= val2;
                } else if constexpr (lower_inclusive && !upper_inclusive) {
                    res[i] = val1 <= src[offset] && src[offset] < val2;
                } else if constexpr (!lower_inclusive && upper_inclusive) {
                    res[i] = val1 < src[offset] && src[offset] <= val2;
                } else {
                    res[i] = val1 < src[offset] && src[offset] < val2;
                }
            }
            return;
        }

        if constexpr (lower_inclusive && upper_inclusive) {
            res.inplace_within_range_val<T, milvus::bitset::RangeType::IncInc>(
                val1, val2, src, n);
        } else if constexpr (lower_inclusive && !upper_inclusive) {
            res.inplace_within_range_val<T, milvus::bitset::RangeType::IncExc>(
                val1, val2, src, n);
        } else if constexpr (!lower_inclusive && upper_inclusive) {
            res.inplace_within_range_val<T, milvus::bitset::RangeType::ExcInc>(
                val1, val2, src, n);
        } else {
            res.inplace_within_range_val<T, milvus::bitset::RangeType::ExcExc>(
                val1, val2, src, n);
        }
    }
};

#define BinaryRangeJSONCompare(cmp)                                \
    do {                                                           \
        if (valid_data != nullptr && !valid_data[offset]) {        \
            res[i] = valid_res[i] = false;                         \
            break;                                                 \
        }                                                          \
        if (has_bitmap_input && !bitmap_input[i + start_cursor]) { \
            break;                                                 \
        }                                                          \
        auto x = src[offset].template at<GetType>(pointer);        \
        if (x.error()) {                                           \
            if constexpr (std::is_same_v<GetType, int64_t>) {      \
                auto x = src[offset].template at<double>(pointer); \
                if (!x.error()) {                                  \
                    auto value = x.value();                        \
                    res[i] = (cmp);                                \
                    break;                                         \
                }                                                  \
            }                                                      \
            res[i] = false;                                        \
            break;                                                 \
        }                                                          \
        auto value = x.value();                                    \
        res[i] = (cmp);                                            \
    } while (false)

template <typename ValueType,
          bool lower_inclusive,
          bool upper_inclusive,
          FilterType filter_type = FilterType::sequential>
struct BinaryRangeElementFuncForJson {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    void
    operator()(ValueType val1,
               ValueType val2,
               const std::string& pointer,
               const milvus::Json* src,
               const bool* valid_data,
               size_t n,
               TargetBitmapView res,
               TargetBitmapView valid_res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < n; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if constexpr (lower_inclusive && upper_inclusive) {
                BinaryRangeJSONCompare(val1 <= value && value <= val2);
            } else if constexpr (lower_inclusive && !upper_inclusive) {
                BinaryRangeJSONCompare(val1 <= value && value < val2);
            } else if constexpr (!lower_inclusive && upper_inclusive) {
                BinaryRangeJSONCompare(val1 < value && value <= val2);
            } else {
                BinaryRangeJSONCompare(val1 < value && value < val2);
            }
        }
    }
};

template <typename ValueType,
          bool lower_inclusive,
          bool upper_inclusive,
          FilterType filter_type = FilterType::sequential>
struct BinaryRangeElementFuncForArray {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    void
    operator()(ValueType val1,
               ValueType val2,
               int index,
               const milvus::ArrayView* src,
               const bool* valid_data,
               size_t n,
               TargetBitmapView res,
               TargetBitmapView valid_res,
               const TargetBitmap& bitmap_input,
               size_t start_cursor,
               const int32_t* offsets = nullptr) {
        bool has_bitmap_input = !bitmap_input.empty();
        for (size_t i = 0; i < n; ++i) {
            if (has_bitmap_input && !bitmap_input[i + start_cursor]) {
                continue;
            }
            size_t offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            if (valid_data != nullptr && !valid_data[offset]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            if constexpr (lower_inclusive && upper_inclusive) {
                if (index >= src[offset].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[offset].get_data<GetType>(index);
                res[i] = val1 <= value && value <= val2;
            } else if constexpr (lower_inclusive && !upper_inclusive) {
                if (index >= src[offset].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[offset].get_data<GetType>(index);
                res[i] = val1 <= value && value < val2;
            } else if constexpr (!lower_inclusive && upper_inclusive) {
                if (index >= src[offset].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[offset].get_data<GetType>(index);
                res[i] = val1 < value && value <= val2;
            } else {
                if (index >= src[offset].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[offset].get_data<GetType>(index);
                res[i] = val1 < value && value < val2;
            }
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
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      FromValCase(expr->lower_val_.val_case()),
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const {
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

 private:
    // Check overflow and cache result for performace
    template <
        typename T,
        typename IndexInnerType = std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>,
        typename HighPrecisionType = std::conditional_t<
            std::is_integral_v<IndexInnerType> && !std::is_same_v<bool, T>,
            int64_t,
            IndexInnerType>>
    ColumnVectorPtr
    PreCheckOverflow(HighPrecisionType& val1,
                     HighPrecisionType& val2,
                     bool& lower_inclusive,
                     bool& upper_inclusive,
                     OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJsonForIndex();

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray(EvalCtx& context);

 private:
    std::shared_ptr<const milvus::expr::BinaryRangeFilterExpr> expr_;
    int64_t overflow_check_pos_{0};
    SingleElement lower_arg_;
    SingleElement upper_arg_;
    bool arg_inited_{false};
};
}  //namespace exec
}  // namespace milvus
