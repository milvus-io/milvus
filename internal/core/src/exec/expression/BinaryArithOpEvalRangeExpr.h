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

namespace {

template <proto::plan::OpType cmp_op>
struct CmpOpHelper {
    using op = void;
};
template <>
struct CmpOpHelper<proto::plan::OpType::Equal> {
    static constexpr auto op = milvus::bitset::CompareOpType::EQ;
};
template <>
struct CmpOpHelper<proto::plan::OpType::GreaterEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::GE;
};
template <>
struct CmpOpHelper<proto::plan::OpType::GreaterThan> {
    static constexpr auto op = milvus::bitset::CompareOpType::GT;
};
template <>
struct CmpOpHelper<proto::plan::OpType::LessEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::LE;
};
template <>
struct CmpOpHelper<proto::plan::OpType::LessThan> {
    static constexpr auto op = milvus::bitset::CompareOpType::LT;
};
template <>
struct CmpOpHelper<proto::plan::OpType::NotEqual> {
    static constexpr auto op = milvus::bitset::CompareOpType::NE;
};

template <proto::plan::ArithOpType arith_op>
struct ArithOpHelper {
    using op = void;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Add> {
    static constexpr auto op = milvus::bitset::ArithOpType::Add;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Sub> {
    static constexpr auto op = milvus::bitset::ArithOpType::Sub;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Mul> {
    static constexpr auto op = milvus::bitset::ArithOpType::Mul;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Div> {
    static constexpr auto op = milvus::bitset::ArithOpType::Div;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Mod> {
    static constexpr auto op = milvus::bitset::ArithOpType::Mod;
};

}  // namespace

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
               const bool* valid_data,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand,
               TargetBitmapView res) {
        /*
        // This is the original code, kept here for the documentation purposes
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
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) > val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) >= val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) < val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (src[i] + right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (src[i] - right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (src[i] * right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (src[i] / right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(src[i], right_operand)) <= val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
        */
        auto execute_sub_batch = [](const T* src,
                                    size_t size,
                                    HighPrecisonType val,
                                    HighPrecisonType right_operand,
                                    TargetBitmapView res) {
            if (size == 0) {
                return;
            }
            if constexpr (!std::is_same_v<decltype(CmpOpHelper<cmp_op>::op),
                                          void>) {
                constexpr auto cmp_op_cvt = CmpOpHelper<cmp_op>::op;
                if constexpr (!std::is_same_v<
                                  decltype(ArithOpHelper<arith_op>::op),
                                  void>) {
                    constexpr auto arith_op_cvt = ArithOpHelper<arith_op>::op;

                    res.inplace_arith_compare<T, arith_op_cvt, cmp_op_cvt>(
                        src, right_operand, val, size);
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else {
                PanicInfo(OpTypeInvalid,
                          fmt::format(
                              "unsupported cmp type:{} for ArithOpElementFunc",
                              cmp_op));
            }
        };
        if (valid_data == nullptr) {
            return execute_sub_batch(src, size, val, right_operand, res);
        }
        for (int left = 0; left < size; left++) {
            for (int right = left; right < size; right++) {
                if (valid_data[right]) {
                    if (right == size - 1) {
                        execute_sub_batch(src + left,
                                          right - left,
                                          val,
                                          right_operand,
                                          res + left);
                    }
                    continue;
                }
                execute_sub_batch(
                    src + left, right - left, val, right_operand, res + left);
                left = right;
                break;
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
    TargetBitmap
    operator()(Index* index,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand) {
        TargetBitmap res(size);
        for (size_t i = 0; i < size; ++i) {
            if (!index->Reverse_Lookup(i).has_value()) {
                res[i] = false;
                continue;
            }
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) == val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) != val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) > val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) >= val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) < val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (index->Reverse_Lookup(i).value() +
                              right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (index->Reverse_Lookup(i).value() -
                              right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (index->Reverse_Lookup(i).value() *
                              right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (index->Reverse_Lookup(i).value() /
                              right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (fmod(index->Reverse_Lookup(i).value(),
                                   right_operand)) <= val;
                } else {
                    PanicInfo(
                        OpTypeInvalid,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
        return res;
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
