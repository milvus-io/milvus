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
#include "exec/expression/Element.h"

namespace milvus {
namespace exec {

namespace {

template <typename T, typename U>
decltype(auto)
safe_mod(T a, U b) {
    if (b == 0) {
        ThrowInfo(ErrorCode::ExprInvalid,
                  "modulus by zero in arithmetic expression");
    }
    if constexpr (std::is_floating_point_v<T> || std::is_floating_point_v<U>) {
        return std::fmod(a, b);
    } else {
        return a % b;
    }
}

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
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitAnd> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitAnd;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitOr> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitOr;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::BitXor> {
    static constexpr auto op = milvus::bitset::ArithOpType::BitXor;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Shl> {
    static constexpr auto op = milvus::bitset::ArithOpType::Shl;
};
template <>
struct ArithOpHelper<proto::plan::ArithOpType::Shr> {
    static constexpr auto op = milvus::bitset::ArithOpType::Shr;
};

}  // namespace

template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op,
          FilterType filter_type = FilterType::sequential>
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
               TargetBitmapView res,
               const int32_t* offsets = nullptr) {
        // Validate divisor for division/modulo operations
        if constexpr (arith_op == proto::plan::ArithOpType::Div ||
                      arith_op == proto::plan::ArithOpType::Mod) {
            if (right_operand == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }

        // This is the original code, kept here for the documentation purposes
        // and also this code will be used for iterative filter since iterative filter does not execute as a batch manner
        if constexpr (filter_type == FilterType::random) {
            for (int i = 0; i < size; ++i) {
                auto offset = (offsets) ? offsets[i] : i;
                if constexpr (cmp_op == proto::plan::OpType::Equal) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) == val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) == val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) != val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) != val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                } else if constexpr (cmp_op ==
                                     proto::plan::OpType::GreaterThan) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) > val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) > val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                } else if constexpr (cmp_op ==
                                     proto::plan::OpType::GreaterEqual) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) >= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) >= val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) < val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) < val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                        res[i] = (src[offset] + right_operand) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Sub) {
                        res[i] = (src[offset] - right_operand) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mul) {
                        res[i] = (src[offset] * right_operand) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Div) {
                        res[i] = (src[offset] / right_operand) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Mod) {
                        res[i] =
                            (long(src[offset]) % long(right_operand)) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitAnd) {
                        res[i] =
                            (long(src[offset]) & long(right_operand)) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitOr) {
                        res[i] =
                            (long(src[offset]) | long(right_operand)) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::BitXor) {
                        res[i] =
                            (long(src[offset]) ^ long(right_operand)) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shl) {
                        res[i] =
                            (long(src[offset]) << long(right_operand)) <= val;
                    } else if constexpr (arith_op ==
                                         proto::plan::ArithOpType::Shr) {
                        res[i] =
                            (long(src[offset]) >> long(right_operand)) <= val;
                    } else {
                        ThrowInfo(UnexpectedError,
                                  fmt::format("unsupported arith type:{} for "
                                              "ArithOpElementFunc",
                                              arith_op));
                    }
                }
            }
            return;
        }

        // more efficient SIMD version
        if constexpr (!std::is_same_v<decltype(CmpOpHelper<cmp_op>::op),
                                      void>) {
            constexpr auto cmp_op_cvt = CmpOpHelper<cmp_op>::op;
            if constexpr (!std::is_same_v<decltype(ArithOpHelper<arith_op>::op),
                                          void>) {
                constexpr auto arith_op_cvt = ArithOpHelper<arith_op>::op;

                res.inplace_arith_compare<T, arith_op_cvt, cmp_op_cvt>(
                    src, right_operand, val, size);
            } else {
                ThrowInfo(
                    UnexpectedError,
                    fmt::format(
                        "unsupported arith type:{} for ArithOpElementFunc",
                        arith_op));
            }
        } else {
            ThrowInfo(
                UnexpectedError,
                fmt::format("unsupported cmp type:{} for ArithOpElementFunc",
                            cmp_op));
        }
    }
};

template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op,
          FilterType filter_type>
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
               HighPrecisonType right_operand,
               const int32_t* offsets = nullptr) {
        // Validate divisor for division/modulo operations
        if constexpr (arith_op == proto::plan::ArithOpType::Div ||
                      arith_op == proto::plan::ArithOpType::Mod) {
            if (right_operand == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }

        TargetBitmap res(size);
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            auto raw = index->Reverse_Lookup(offset);
            if (!raw.has_value()) {
                res[i] = false;
                continue;
            }
            if constexpr (cmp_op == proto::plan::OpType::Equal) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) == val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) == val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) != val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) != val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) > val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) > val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) >= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) >= val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) < val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) < val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
                if constexpr (arith_op == proto::plan::ArithOpType::Add) {
                    res[i] = (raw.value() + right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Sub) {
                    res[i] = (raw.value() - right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mul) {
                    res[i] = (raw.value() * right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Div) {
                    res[i] = (raw.value() / right_operand) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Mod) {
                    res[i] = (long(raw.value()) % long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitAnd) {
                    res[i] = (long(raw.value()) & long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitOr) {
                    res[i] = (long(raw.value()) | long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::BitXor) {
                    res[i] = (long(raw.value()) ^ long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shl) {
                    res[i] = (long(raw.value()) << long(right_operand)) <= val;
                } else if constexpr (arith_op ==
                                     proto::plan::ArithOpType::Shr) {
                    res[i] = (long(raw.value()) >> long(right_operand)) <= val;
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format(
                            "unsupported arith type:{} for ArithOpElementFunc",
                            arith_op));
                }
            }
        }
        return res;
    }
};

// Applies a single arithmetic op to a value. Shared by ArithOpElementFunc2
// and ArithOpIndexFunc2 (the depth-2 counterparts of ArithOpElementFunc /
// ArithOpIndexFunc above) to compose op1's contribution before the existing
// per-op comparison logic (reused via bitset::ArithCompareOperator2, for the
// batch/sequential path) or the inline chain below (for the random /
// iterative-filter path) applies op2 and compares.
template <typename HighPrecisonType, proto::plan::ArithOpType arith_op>
HighPrecisonType
ApplyArithOp(HighPrecisonType v, HighPrecisonType right_operand) {
    if constexpr (arith_op == proto::plan::ArithOpType::Add) {
        return v + right_operand;
    } else if constexpr (arith_op == proto::plan::ArithOpType::Sub) {
        return v - right_operand;
    } else if constexpr (arith_op == proto::plan::ArithOpType::Mul) {
        return v * right_operand;
    } else if constexpr (arith_op == proto::plan::ArithOpType::Div) {
        return v / right_operand;
    } else if constexpr (arith_op == proto::plan::ArithOpType::Mod) {
        return HighPrecisonType(long(v) % long(right_operand));
    } else if constexpr (arith_op == proto::plan::ArithOpType::BitAnd) {
        return HighPrecisonType(long(v) & long(right_operand));
    } else if constexpr (arith_op == proto::plan::ArithOpType::BitOr) {
        return HighPrecisonType(long(v) | long(right_operand));
    } else if constexpr (arith_op == proto::plan::ArithOpType::BitXor) {
        return HighPrecisonType(long(v) ^ long(right_operand));
    } else if constexpr (arith_op == proto::plan::ArithOpType::Shl) {
        return HighPrecisonType(long(v) << long(right_operand));
    } else if constexpr (arith_op == proto::plan::ArithOpType::Shr) {
        return HighPrecisonType(long(v) >> long(right_operand));
    } else {
        ThrowInfo(UnexpectedError,
                  fmt::format("unsupported arith type:{} for ApplyArithOp",
                              arith_op));
        return HighPrecisonType();
    }
}

// Compares a fully-composed arithmetic result to val. Shared tail of the
// random-filter path in ArithOpElementFunc2/ArithOpIndexFunc2.
template <typename HighPrecisonType, proto::plan::OpType cmp_op>
bool
CompareArithResult(HighPrecisonType result, HighPrecisonType val) {
    if constexpr (cmp_op == proto::plan::OpType::Equal) {
        return result == val;
    } else if constexpr (cmp_op == proto::plan::OpType::NotEqual) {
        return result != val;
    } else if constexpr (cmp_op == proto::plan::OpType::GreaterThan) {
        return result > val;
    } else if constexpr (cmp_op == proto::plan::OpType::GreaterEqual) {
        return result >= val;
    } else if constexpr (cmp_op == proto::plan::OpType::LessThan) {
        return result < val;
    } else if constexpr (cmp_op == proto::plan::OpType::LessEqual) {
        return result <= val;
    } else {
        ThrowInfo(UnexpectedError,
                  fmt::format("unsupported cmp type:{} for CompareArithResult",
                              cmp_op));
        return false;
    }
}

// Depth-2 counterpart of ArithOpElementFunc: composes two arithmetic ops
// before the comparison, ((src[i] arith_op1 right_operand1) arith_op2
// right_operand2) cmp_op val. Kept as a separate sibling struct (rather than
// extending ArithOpElementFunc with a defaultable second op parameter) so
// the existing single-op hot path's codegen is untouched.
template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op1,
          proto::plan::ArithOpType arith_op2,
          FilterType filter_type = FilterType::sequential>
struct ArithOpElementFunc2 {
    typedef std::conditional_t<std::is_integral_v<T> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               T>
        HighPrecisonType;
    void
    operator()(const T* src,
               size_t size,
               HighPrecisonType val,
               HighPrecisonType right_operand1,
               HighPrecisonType right_operand2,
               TargetBitmapView res,
               const int32_t* offsets = nullptr) {
        if constexpr (arith_op1 == proto::plan::ArithOpType::Div ||
                      arith_op1 == proto::plan::ArithOpType::Mod) {
            if (right_operand1 == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }
        if constexpr (arith_op2 == proto::plan::ArithOpType::Div ||
                      arith_op2 == proto::plan::ArithOpType::Mod) {
            if (right_operand2 == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }

        // Used for iterative filter, which does not execute in a batch
        // manner (mirrors ArithOpElementFunc's random-filter fallback).
        if constexpr (filter_type == FilterType::random) {
            for (int i = 0; i < size; ++i) {
                auto offset = (offsets) ? offsets[i] : i;
                auto intermediate = ApplyArithOp<HighPrecisonType, arith_op1>(
                    static_cast<HighPrecisonType>(src[offset]), right_operand1);
                auto result = ApplyArithOp<HighPrecisonType, arith_op2>(
                    intermediate, right_operand2);
                res[i] =
                    CompareArithResult<HighPrecisonType, cmp_op>(result, val);
            }
            return;
        }

        // Generic scalar two-op path (no dedicated SIMD kernel yet — see
        // bitset::inplace_arith_compare2; true fusion for chained
        // Add/Sub/Mul/Div is a possible follow-up).
        constexpr auto cmp_op_cvt = CmpOpHelper<cmp_op>::op;
        constexpr auto arith_op1_cvt = ArithOpHelper<arith_op1>::op;
        constexpr auto arith_op2_cvt = ArithOpHelper<arith_op2>::op;

        res.template inplace_arith_compare2<T,
                                            arith_op1_cvt,
                                            arith_op2_cvt,
                                            cmp_op_cvt>(
            src, right_operand1, right_operand2, val, size);
    }
};

// Depth-2 counterpart of ArithOpIndexFunc, reading through a ScalarIndex
// instead of a raw pointer. No SIMD path exists for the index case even for
// a single op (see ArithOpIndexFunc above), so this is a plain scalar loop.
template <typename T,
          proto::plan::OpType cmp_op,
          proto::plan::ArithOpType arith_op1,
          proto::plan::ArithOpType arith_op2,
          FilterType filter_type>
struct ArithOpIndexFunc2 {
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
               HighPrecisonType right_operand1,
               HighPrecisonType right_operand2,
               const int32_t* offsets = nullptr) {
        if constexpr (arith_op1 == proto::plan::ArithOpType::Div ||
                      arith_op1 == proto::plan::ArithOpType::Mod) {
            if (right_operand1 == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }
        if constexpr (arith_op2 == proto::plan::ArithOpType::Div ||
                      arith_op2 == proto::plan::ArithOpType::Mod) {
            if (right_operand2 == 0) {
                ThrowInfo(
                    ErrorCode::ExprInvalid,
                    "division or modulus by zero in arithmetic expression");
            }
        }

        TargetBitmap res(size);
        for (size_t i = 0; i < size; ++i) {
            auto offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = (offsets) ? offsets[i] : i;
            }
            auto raw = index->Reverse_Lookup(offset);
            if (!raw.has_value()) {
                res[i] = false;
                continue;
            }
            auto intermediate = ApplyArithOp<HighPrecisonType, arith_op1>(
                static_cast<HighPrecisonType>(raw.value()), right_operand1);
            auto result = ApplyArithOp<HighPrecisonType, arith_op2>(
                intermediate, right_operand2);
            res[i] = CompareArithResult<HighPrecisonType, cmp_op>(result, val);
        }
        return res;
    }
};

class PhyTimestamptzArithCompareExpr;
class PhyBinaryArithOpEvalRangeExpr : public SegmentExpr {
    friend class PhyTimestamptzArithCompareExpr;

 public:
    PhyBinaryArithOpEvalRangeExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr>&
            expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level)
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      DataType::NONE,
                      active_count,
                      batch_size,
                      consistency_level),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override {
        SegmentExpr::DetermineExecPath();
        if (exec_path_ != ExprExecPath::ScalarIndex) {
            return;
        }

        auto data_type = expr_->column_.data_type_;
        if (expr_->column_.element_level_) {
            data_type = expr_->column_.element_type_;
        }

        // JSON, ARRAY and VECTOR_ARRAY types cannot use index for arith ops.
        if (data_type == DataType::JSON || data_type == DataType::ARRAY ||
            data_type == DataType::VECTOR_ARRAY) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }

        // for basic types, need index raw data for arith evaluation
        bool has_raw = false;
        switch (data_type) {
            case DataType::BOOL:
                has_raw = IndexHasRawData<bool>();
                break;
            case DataType::INT8:
                has_raw = IndexHasRawData<int8_t>();
                break;
            case DataType::INT16:
                has_raw = IndexHasRawData<int16_t>();
                break;
            case DataType::INT32:
                has_raw = IndexHasRawData<int32_t>();
                break;
            case DataType::INT64:
                has_raw = IndexHasRawData<int64_t>();
                break;
            case DataType::FLOAT:
                has_raw = IndexHasRawData<float>();
                break;
            case DataType::DOUBLE:
                has_raw = IndexHasRawData<double>();
                break;
            default:
                has_raw = false;
        }
        if (!has_raw) {
            exec_path_ = ExprExecPath::RawData;
        }
    }

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

    void
    PrefetchRawData() override;

    template <typename T>
    void
    PrefetchRawData();

 private:
    template <typename T>
    VectorPtr
    ExecRangeVisitorImpl(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForIndex(OffsetVector* input = nullptr);

    template <typename T>
    VectorPtr
    ExecRangeVisitorImplForData(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForJson(OffsetVector* input = nullptr);

    template <typename ValueType>
    VectorPtr
    ExecRangeVisitorImplForArray(OffsetVector* input = nullptr);

    template <typename ArrayType, typename ValueType, bool ElementLevel>
    VectorPtr
    ExecArrayLength(OffsetVector* input = nullptr);

 private:
    std::shared_ptr<const milvus::expr::BinaryArithOpEvalRangeExpr> expr_;
    SingleElement right_operand_arg_;
    // Only initialized/used when expr_->has_second_op() is true.
    SingleElement right_operand2_arg_;
    SingleElement value_arg_;
    bool arg_inited_{false};
};

}  //namespace exec
}  // namespace milvus
