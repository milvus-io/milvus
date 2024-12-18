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

#include <utility>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "index/Meta.h"
#include "segcore/SegmentInterface.h"
#include "query/Utils.h"
#include "common/RegexQuery.h"

namespace milvus {
namespace exec {

template <typename T>
struct UnaryElementFuncForMatch {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;

    void
    operator()(const T* src,
               size_t size,
               IndexInnerType val,
               TargetBitmapView res) {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(val);
        RegexMatcher matcher(regex_pattern);
        for (int i = 0; i < size; ++i) {
            res[i] = matcher(src[i]);
        }
    }
};

template <typename T, proto::plan::OpType op>
struct UnaryElementFunc {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    void
    operator()(const T* src,
               size_t size,
               IndexInnerType val,
               TargetBitmapView res) {
        if constexpr (op == proto::plan::OpType::Match) {
            UnaryElementFuncForMatch<T> func;
            func(src, size, val, res);
            return;
        }

        /*
        // This is the original code, which is kept for the documentation purposes
        for (int i = 0; i < size; ++i) {
            if constexpr (op == proto::plan::OpType::Equal) {
                res[i] = src[i] == val;
            } else if constexpr (op == proto::plan::OpType::NotEqual) {
                res[i] = src[i] != val;
            } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                res[i] = src[i] > val;
            } else if constexpr (op == proto::plan::OpType::LessThan) {
                res[i] = src[i] < val;
            } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                res[i] = src[i] >= val;
            } else if constexpr (op == proto::plan::OpType::LessEqual) {
                res[i] = src[i] <= val;
            } else if constexpr (op == proto::plan::OpType::PrefixMatch) {
                res[i] = milvus::query::Match(
                    src[i], val, proto::plan::OpType::PrefixMatch);
            } else {
                PanicInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported op_type:{} for UnaryElementFunc",
                                op));
            }
        }
        */

        if constexpr (op == proto::plan::OpType::PrefixMatch) {
            for (int i = 0; i < size; ++i) {
                res[i] = milvus::query::Match(
                    src[i], val, proto::plan::OpType::PrefixMatch);
            }
        } else if constexpr (op == proto::plan::OpType::Equal) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::EQ>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::NotEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::NE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessThan) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LT>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::GE>(
                src, size, val);
        } else if constexpr (op == proto::plan::OpType::LessEqual) {
            res.inplace_compare_val<T, milvus::bitset::CompareOpType::LE>(
                src, size, val);
        } else {
            PanicInfo(
                OpTypeInvalid,
                fmt::format("unsupported op_type:{} for UnaryElementFunc", op));
        }
    }
};

#define UnaryArrayCompare(cmp)                                          \
    do {                                                                \
        if constexpr (std::is_same_v<GetType, proto::plan::Array>) {    \
            res[i] = false;                                             \
        } else {                                                        \
            if (index >= src[i].length()) {                             \
                res[i] = false;                                         \
                continue;                                               \
            }                                                           \
            auto array_data = src[i].template get_data<GetType>(index); \
            res[i] = (cmp);                                             \
        }                                                               \
    } while (false)

template <typename ValueType, proto::plan::OpType op>
struct UnaryElementFuncForArray {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    void
    operator()(const ArrayView* src,
               size_t size,
               ValueType val,
               int index,
               TargetBitmapView res) {
        for (int i = 0; i < size; ++i) {
            if constexpr (op == proto::plan::OpType::Equal) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = src[i].is_same_array(val);
                } else {
                    if (index >= src[i].length()) {
                        res[i] = false;
                        continue;
                    }
                    auto array_data = src[i].template get_data<GetType>(index);
                    res[i] = array_data == val;
                }
            } else if constexpr (op == proto::plan::OpType::NotEqual) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = !src[i].is_same_array(val);
                } else {
                    if (index >= src[i].length()) {
                        res[i] = false;
                        continue;
                    }
                    auto array_data = src[i].template get_data<GetType>(index);
                    res[i] = array_data != val;
                }
            } else if constexpr (op == proto::plan::OpType::GreaterThan) {
                UnaryArrayCompare(array_data > val);
            } else if constexpr (op == proto::plan::OpType::LessThan) {
                UnaryArrayCompare(array_data < val);
            } else if constexpr (op == proto::plan::OpType::GreaterEqual) {
                UnaryArrayCompare(array_data >= val);
            } else if constexpr (op == proto::plan::OpType::LessEqual) {
                UnaryArrayCompare(array_data <= val);
            } else if constexpr (op == proto::plan::OpType::PrefixMatch) {
                UnaryArrayCompare(milvus::query::Match(array_data, val, op));
            } else if constexpr (op == proto::plan::OpType::Match) {
                if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                    res[i] = false;
                } else {
                    if (index >= src[i].length()) {
                        res[i] = false;
                        continue;
                    }
                    PatternMatchTranslator translator;
                    auto regex_pattern = translator(val);
                    RegexMatcher matcher(regex_pattern);
                    auto array_data = src[i].template get_data<GetType>(index);
                    res[i] = matcher(array_data);
                }
            } else {
                PanicInfo(OpTypeInvalid,
                          "unsupported op_type:{} for "
                          "UnaryElementFuncForArray",
                          op);
            }
        }
    }
};

template <typename T>
struct UnaryIndexFuncForMatch {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, IndexInnerType val) {
        if constexpr (!std::is_same_v<T, std::string_view> &&
                      !std::is_same_v<T, std::string>) {
            PanicInfo(Unsupported, "regex query is only supported on string");
        } else {
            if (index->SupportPatternMatch()) {
                return index->PatternMatch(val);
            }
            if (!index->HasRawData()) {
                PanicInfo(Unsupported,
                          "index don't support regex query and don't have "
                          "raw data");
            }

            // retrieve raw data to do brute force query, may be very slow.
            auto cnt = index->Count();
            TargetBitmap res(cnt);
            PatternMatchTranslator translator;
            auto regex_pattern = translator(val);
            RegexMatcher matcher(regex_pattern);
            for (int64_t i = 0; i < cnt; i++) {
                auto raw = index->Reverse_Lookup(i);
                res[i] = matcher(raw);
            }
            return res;
        }
    }
};

template <typename T, proto::plan::OpType op>
struct UnaryIndexFunc {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
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
        } else if constexpr (op == proto::plan::OpType::PrefixMatch) {
            auto dataset = std::make_unique<Dataset>();
            dataset->Set(milvus::index::OPERATOR_TYPE,
                         proto::plan::OpType::PrefixMatch);
            dataset->Set(milvus::index::PREFIX_VALUE, val);
            return index->Query(std::move(dataset));
        } else if constexpr (op == proto::plan::OpType::Match) {
            UnaryIndexFuncForMatch<T> func;
            return func(index, val);
        } else {
            PanicInfo(
                OpTypeInvalid,
                fmt::format("unsupported op_type:{} for UnaryIndexFunc", op));
        }
    }
};

class PhyUnaryRangeFilterExpr : public SegmentExpr {
 public:
    PhyUnaryRangeFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr>& expr,
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
    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData();

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplJson();

    template <typename ExprValueType>
    VectorPtr
    ExecRangeVisitorImplArray();

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplArrayForIndex();

    template <typename T>
    VectorPtr
    ExecArrayEqualForIndex(bool reverse);

    // Check overflow and cache result for performace
    template <typename T>
    ColumnVectorPtr
    PreCheckOverflow();

    template <typename T>
    bool
    CanUseIndex();

    template <typename T>
    bool
    CanUseIndexForArray();

 private:
    std::shared_ptr<const milvus::expr::UnaryRangeFilterExpr> expr_;
    ColumnVectorPtr cached_overflow_res_{nullptr};
    int64_t overflow_check_pos_{0};
};
}  // namespace exec
}  // namespace milvus
