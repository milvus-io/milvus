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

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "bitset/common.h"

namespace milvus {
namespace bitset {
namespace detail {

// The default reference vectorizer.
// Its every function returns a boolean value whether a vectorized implementation
//   exists and was invoked. If not, then the caller code will use a default
//   non-vectorized implementation.
// The default vectorizer provides no vectorized implementation, forcing the
//   caller to use a defaut non-vectorized implementation every time.
struct VectorizedRef {
    // Fills a bitmask by comparing two arrays element-wise.
    // API requirement: size % 8 == 0
    template <typename T, typename U, CompareOpType Op>
    static inline bool
    op_compare_column(uint8_t* const __restrict output,
                      const T* const __restrict t,
                      const U* const __restrict u,
                      const size_t size) {
        return false;
    }

    // Fills a bitmask by comparing elements of a given array to a
    //   given value.
    // API requirement: size % 8 == 0
    template <typename T, CompareOpType Op>
    static inline bool
    op_compare_val(uint8_t* const __restrict output,
                   const T* const __restrict t,
                   const size_t size,
                   const T& value) {
        return false;
    }

    // API requirement: size % 8 == 0
    template <typename T, RangeType Op>
    static inline bool
    op_within_range_column(uint8_t* const __restrict data,
                           const T* const __restrict lower,
                           const T* const __restrict upper,
                           const T* const __restrict values,
                           const size_t size) {
        return false;
    }

    // API requirement: size % 8 == 0
    template <typename T, RangeType Op>
    static inline bool
    op_within_range_val(uint8_t* const __restrict data,
                        const T& lower,
                        const T& upper,
                        const T* const __restrict values,
                        const size_t size) {
        return false;
    }

    // API requirement: size % 8 == 0
    template <typename T, ArithOpType AOp, CompareOpType CmpOp>
    static inline bool
    op_arith_compare(uint8_t* const __restrict bitmask,
                     const T* const __restrict src,
                     const ArithHighPrecisionType<T>& right_operand,
                     const ArithHighPrecisionType<T>& value,
                     const size_t size) {
        return false;
    }
};

}  // namespace detail
}  // namespace bitset
}  // namespace milvus
