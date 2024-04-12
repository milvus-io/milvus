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

#include "bitset/common.h"

#include "sve-decl.h"

#ifdef BITSET_HEADER_ONLY
#include "sve-impl.h"
#endif

namespace milvus {
namespace bitset {
namespace detail {
namespace arm {

///////////////////////////////////////////////////////////////////////////

//
struct VectorizedSve {
    template <typename T, typename U, CompareOpType Op>
    static constexpr inline auto op_compare_column =
        sve::OpCompareColumnImpl<T, U, Op>::op_compare_column;

    template <typename T, CompareOpType Op>
    static constexpr inline auto op_compare_val =
        sve::OpCompareValImpl<T, Op>::op_compare_val;

    template <typename T, RangeType Op>
    static constexpr inline auto op_within_range_column =
        sve::OpWithinRangeColumnImpl<T, Op>::op_within_range_column;

    template <typename T, RangeType Op>
    static constexpr inline auto op_within_range_val =
        sve::OpWithinRangeValImpl<T, Op>::op_within_range_val;

    template <typename T, ArithOpType AOp, CompareOpType CmpOp>
    static constexpr inline auto op_arith_compare =
        sve::OpArithCompareImpl<T, AOp, CmpOp>::op_arith_compare;
};

}  // namespace arm
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
