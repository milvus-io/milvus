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
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace milvus {
namespace bitset {

// this option is only somewhat supported
// #define BITSET_HEADER_ONLY

// `always inline` hint.
// It is introduced to deal with clang's behavior to reuse
//   once generated code. But if it is needed to generate
//   different machine code for multiple platforms based on
//   a single template, then such a behavior is undesired.
// `always inline` is applied for PolicyT methods. It is fine,
//   because they are not used directly and are wrapped
//   in BitsetBase methods. So, a compiler may decide whether
//   to really inline them, but it forces a compiler to
//   generate specialized code for every hardward platform.
// todo: MSVC has its own way to define `always inline`.
#define BITSET_ALWAYS_INLINE __attribute__((always_inline))

// a supporting utility
template <class>
inline constexpr bool always_false_v = false;

// a ? b
enum class CompareOpType {
    GT = 1,
    GE = 2,
    LT = 3,
    LE = 4,
    EQ = 5,
    NE = 6,
};

template <CompareOpType Op>
struct CompareOperator {
    template <typename T, typename U>
    static inline bool
    compare(const T& t, const U& u) {
        if constexpr (Op == CompareOpType::EQ) {
            return (t == u);
        } else if constexpr (Op == CompareOpType::GE) {
            return (t >= u);
        } else if constexpr (Op == CompareOpType::GT) {
            return (t > u);
        } else if constexpr (Op == CompareOpType::LE) {
            return (t <= u);
        } else if constexpr (Op == CompareOpType::LT) {
            return (t < u);
        } else if constexpr (Op == CompareOpType::NE) {
            return (t != u);
        } else {
            // unimplemented
            static_assert(always_false_v<T>, "unimplemented");
        }
    }
};

// a ? v && v ? b
enum class RangeType {
    // [a, b]
    IncInc,
    // [a, b)
    IncExc,
    // (a, b]
    ExcInc,
    // (a, b)
    ExcExc
};

template <RangeType Op>
struct RangeOperator {
    template <typename T>
    static inline bool
    within_range(const T& lower, const T& upper, const T& value) {
        if constexpr (Op == RangeType::IncInc) {
            return (lower <= value && value <= upper);
        } else if constexpr (Op == RangeType::ExcInc) {
            return (lower < value && value <= upper);
        } else if constexpr (Op == RangeType::IncExc) {
            return (lower <= value && value < upper);
        } else if constexpr (Op == RangeType::ExcExc) {
            return (lower < value && value < upper);
        } else {
            // unimplemented
            static_assert(always_false_v<T>, "unimplemented");
        }
    }
};

//
template <RangeType Op>
struct Range2Compare {
    static constexpr inline CompareOpType lower =
        (Op == RangeType::IncInc || Op == RangeType::IncExc)
            ? CompareOpType::LE
            : CompareOpType::LT;
    static constexpr inline CompareOpType upper =
        (Op == RangeType::IncInc || Op == RangeType::ExcInc)
            ? CompareOpType::LE
            : CompareOpType::LT;
};

// The following operation is Milvus-specific
enum class ArithOpType { Add, Sub, Mul, Div, Mod };

template <typename T>
using ArithHighPrecisionType =
    std::conditional_t<std::is_integral_v<T> && !std::is_same_v<bool, T>,
                       int64_t,
                       T>;

template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithCompareOperator {
    template <typename T>
    static inline bool
    compare(const T& left,
            const ArithHighPrecisionType<T>& right,
            const ArithHighPrecisionType<T>& value) {
        if constexpr (AOp == ArithOpType::Add) {
            return CompareOperator<CmpOp>::compare(left + right, value);
        } else if constexpr (AOp == ArithOpType::Sub) {
            return CompareOperator<CmpOp>::compare(left - right, value);
        } else if constexpr (AOp == ArithOpType::Mul) {
            return CompareOperator<CmpOp>::compare(left * right, value);
        } else if constexpr (AOp == ArithOpType::Div) {
            return CompareOperator<CmpOp>::compare(left / right, value);
        } else if constexpr (AOp == ArithOpType::Mod) {
            return CompareOperator<CmpOp>::compare(long(left) % long(right),
                                                   value);
        } else {
            // unimplemented
            static_assert(always_false_v<T>, "unimplemented");
        }
    }
};

}  // namespace bitset
}  // namespace milvus
