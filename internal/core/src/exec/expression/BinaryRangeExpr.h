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
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T, bool lower_inclusive, bool upper_inclusive>
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
               const bool* valid_data,
               size_t n,
               TargetBitmapView res) {
        auto execute_sub_batch = [](T val1,
                                    T val2,
                                    const T* src,
                                    size_t n,
                                    TargetBitmapView res) {
            if (n == 0) {
                return;
            }
            if constexpr (lower_inclusive && upper_inclusive) {
                res.inplace_within_range_val<T,
                                             milvus::bitset::RangeType::IncInc>(
                    val1, val2, src, n);
            } else if constexpr (lower_inclusive && !upper_inclusive) {
                res.inplace_within_range_val<T,
                                             milvus::bitset::RangeType::IncExc>(
                    val1, val2, src, n);
            } else if constexpr (!lower_inclusive && upper_inclusive) {
                res.inplace_within_range_val<T,
                                             milvus::bitset::RangeType::ExcInc>(
                    val1, val2, src, n);
            } else {
                res.inplace_within_range_val<T,
                                             milvus::bitset::RangeType::ExcExc>(
                    val1, val2, src, n);
            }
        };
        if (valid_data == nullptr) {
            return execute_sub_batch(val1, val2, src, n, res);
        }
        for (int left = 0; left < n; left++) {
            for (int right = left; right < n; right++) {
                if (valid_data[right]) {
                    if (right == n - 1) {
                        execute_sub_batch(
                            val1, val2, src + left, right - left, res + left);
                    }
                    continue;
                }
                execute_sub_batch(
                    val1, val2, src + left, right - left, res + left);
                left = right;
                break;
            }
        }
    }
};

#define BinaryRangeJSONCompare(cmp)                           \
    do {                                                      \
        if (valid_data && !valid_data[i]) {                   \
            res[i] = false;                                   \
            continue;                                         \
        }                                                     \
        auto x = src[i].template at<GetType>(pointer);        \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = src[i].template at<double>(pointer); \
                if (!x.error()) {                             \
                    auto value = x.value();                   \
                    res[i] = (cmp);                           \
                    break;                                    \
                }                                             \
            }                                                 \
            res[i] = false;                                   \
            break;                                            \
        }                                                     \
        auto value = x.value();                               \
        res[i] = (cmp);                                       \
    } while (false)

template <typename ValueType, bool lower_inclusive, bool upper_inclusive>
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
               TargetBitmapView res) {
        for (size_t i = 0; i < n; ++i) {
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

template <typename ValueType, bool lower_inclusive, bool upper_inclusive>
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
               TargetBitmapView res) {
        for (size_t i = 0; i < n; ++i) {
            if (valid_data && !valid_data[i]) {
                res[i] = false;
                continue;
            }
            if constexpr (lower_inclusive && upper_inclusive) {
                if (index >= src[i].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[i].get_data<GetType>(index);
                res[i] = val1 <= value && value <= val2;
            } else if constexpr (lower_inclusive && !upper_inclusive) {
                if (index >= src[i].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[i].get_data<GetType>(index);
                res[i] = val1 <= value && value < val2;
            } else if constexpr (!lower_inclusive && upper_inclusive) {
                if (index >= src[i].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[i].get_data<GetType>(index);
                res[i] = val1 < value && value <= val2;
            } else {
                if (index >= src[i].length()) {
                    res[i] = false;
                    continue;
                }
                auto value = src[i].get_data<GetType>(index);
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
        int64_t batch_size)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      active_count,
                      batch_size),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

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
                     bool& upper_inclusive);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData();

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson();

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray();

 private:
    std::shared_ptr<const milvus::expr::BinaryRangeFilterExpr> expr_;
    ColumnVectorPtr cached_overflow_res_{nullptr};
    int64_t overflow_check_pos_{0};
};
}  //namespace exec
}  // namespace milvus
