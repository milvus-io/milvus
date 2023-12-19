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

#include <cmath>
#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op>
struct ArithOpElementFunc {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisonType;
    void
    operator()(const T* src,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand,
               bool* res) {
        for (int i = 0; i < size; ++i) {
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) == val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) != val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
    }
};

template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op>
struct ArithOpIndexFunc {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisonType;
    using Index = index::ScalarIndex<T>;
    FixedVector<bool>
    operator()(Index* index,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand) {
        FixedVector<bool> res_vec(size);
        bool* res = res_vec.data();
        for (size_t i = 0; i < size; ++i) {
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i) + right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i) - right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i) * right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i) / right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] =
                        (fmod(index->Reverse_Lookup(i), right_operand)) == val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i) + right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i) - right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i) * right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i) / right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] =
                        (fmod(index->Reverse_Lookup(i), right_operand)) != val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
        return res_vec;
    }
};

class PhyBinaryArithOpEvalRangeExpr : public SegmentExpr {
 public:
    PhyBinaryArithOpEvalRangeExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr>&
            expr,
        const std::string& name,
        const segcore::SegmentInternalInterface* segment,
        Timestamp query_timestamp,
        int64_t batch_size)
        : SegmentExpr(std::move(input),
                      name,
                      segment,
                      expr->column_.field_id_,
                      query_timestamp,
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

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson();

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray();

 private:
    std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr> expr_;
};
}  //namespace exec
}  // namespace milvus
