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

// AVX512 instantiation

#include "bitset/common.h"

#ifndef BITSET_HEADER_ONLY

#include "avx512-decl.h"
#include "avx512-impl.h"

#include <cstddef>
#include <cstdint>

namespace milvus {
namespace bitset {
namespace detail {
namespace x86 {
namespace avx512 {

// a facility to run through all possible compare operations
#define ALL_COMPARE_OPS(FUNC, ...) \
    FUNC(__VA_ARGS__, EQ);         \
    FUNC(__VA_ARGS__, GE);         \
    FUNC(__VA_ARGS__, GT);         \
    FUNC(__VA_ARGS__, LE);         \
    FUNC(__VA_ARGS__, LT);         \
    FUNC(__VA_ARGS__, NE);

// a facility to run through all possible range operations
#define ALL_RANGE_OPS(FUNC, ...) \
    FUNC(__VA_ARGS__, IncInc);   \
    FUNC(__VA_ARGS__, IncExc);   \
    FUNC(__VA_ARGS__, ExcInc);   \
    FUNC(__VA_ARGS__, ExcExc);

// a facility to run through all possible arithmetic compare operations
#define ALL_ARITH_CMP_OPS(FUNC, ...) \
    FUNC(__VA_ARGS__, Add, EQ);      \
    FUNC(__VA_ARGS__, Add, GE);      \
    FUNC(__VA_ARGS__, Add, GT);      \
    FUNC(__VA_ARGS__, Add, LE);      \
    FUNC(__VA_ARGS__, Add, LT);      \
    FUNC(__VA_ARGS__, Add, NE);      \
    FUNC(__VA_ARGS__, Sub, EQ);      \
    FUNC(__VA_ARGS__, Sub, GE);      \
    FUNC(__VA_ARGS__, Sub, GT);      \
    FUNC(__VA_ARGS__, Sub, LE);      \
    FUNC(__VA_ARGS__, Sub, LT);      \
    FUNC(__VA_ARGS__, Sub, NE);      \
    FUNC(__VA_ARGS__, Mul, EQ);      \
    FUNC(__VA_ARGS__, Mul, GE);      \
    FUNC(__VA_ARGS__, Mul, GT);      \
    FUNC(__VA_ARGS__, Mul, LE);      \
    FUNC(__VA_ARGS__, Mul, LT);      \
    FUNC(__VA_ARGS__, Mul, NE);      \
    FUNC(__VA_ARGS__, Div, EQ);      \
    FUNC(__VA_ARGS__, Div, GE);      \
    FUNC(__VA_ARGS__, Div, GT);      \
    FUNC(__VA_ARGS__, Div, LE);      \
    FUNC(__VA_ARGS__, Div, LT);      \
    FUNC(__VA_ARGS__, Div, NE);      \
    FUNC(__VA_ARGS__, Mod, EQ);      \
    FUNC(__VA_ARGS__, Mod, GE);      \
    FUNC(__VA_ARGS__, Mod, GT);      \
    FUNC(__VA_ARGS__, Mod, LE);      \
    FUNC(__VA_ARGS__, Mod, LT);      \
    FUNC(__VA_ARGS__, Mod, NE);

///////////////////////////////////////////////////////////////////////////

//
#define INSTANTIATE_COMPARE_VAL_AVX512(TTYPE, OP)                             \
    template bool OpCompareValImpl<TTYPE, CompareOpType::OP>::op_compare_val( \
        uint8_t* const __restrict bitmask,                                    \
        const TTYPE* const __restrict src,                                    \
        const size_t size,                                                    \
        const TTYPE& val);

ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, int8_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, int16_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, int32_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, int64_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, float)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_VAL_AVX512, double)

#undef INSTANTIATE_COMPARE_VAL_AVX512

///////////////////////////////////////////////////////////////////////////

//
#define INSTANTIATE_COMPARE_COLUMN_AVX512(TTYPE, OP)                         \
    template bool                                                            \
    OpCompareColumnImpl<TTYPE, TTYPE, CompareOpType::OP>::op_compare_column( \
        uint8_t* const __restrict bitmask,                                   \
        const TTYPE* const __restrict left,                                  \
        const TTYPE* const __restrict right,                                 \
        const size_t size);

ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, int8_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, int16_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, int32_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, int64_t)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, float)
ALL_COMPARE_OPS(INSTANTIATE_COMPARE_COLUMN_AVX512, double)

#undef INSTANTIATE_COMPARE_COLUMN_AVX512

///////////////////////////////////////////////////////////////////////////

//
#define INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512(TTYPE, OP)                  \
    template bool                                                          \
    OpWithinRangeColumnImpl<TTYPE, RangeType::OP>::op_within_range_column( \
        uint8_t* const __restrict res_u8,                                  \
        const TTYPE* const __restrict lower,                               \
        const TTYPE* const __restrict upper,                               \
        const TTYPE* const __restrict values,                              \
        const size_t size);

ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, int8_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, int16_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, int32_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, int64_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, float)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512, double)

#undef INSTANTIATE_WITHIN_RANGE_COLUMN_AVX512

///////////////////////////////////////////////////////////////////////////

//
#define INSTANTIATE_WITHIN_RANGE_VAL_AVX512(TTYPE, OP)               \
    template bool                                                    \
    OpWithinRangeValImpl<TTYPE, RangeType::OP>::op_within_range_val( \
        uint8_t* const __restrict res_u8,                            \
        const TTYPE& lower,                                          \
        const TTYPE& upper,                                          \
        const TTYPE* const __restrict values,                        \
        const size_t size);

ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, int8_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, int16_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, int32_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, int64_t)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, float)
ALL_RANGE_OPS(INSTANTIATE_WITHIN_RANGE_VAL_AVX512, double)

#undef INSTANTIATE_WITHIN_RANGE_VAL_AVX512

///////////////////////////////////////////////////////////////////////////

//
#define INSTANTIATE_ARITH_COMPARE_AVX512(TTYPE, OP, CMP)                     \
    template bool                                                            \
    OpArithCompareImpl<TTYPE, ArithOpType::OP, CompareOpType::CMP>::         \
        op_arith_compare(uint8_t* const __restrict res_u8,                   \
                         const TTYPE* const __restrict src,                  \
                         const ArithHighPrecisionType<TTYPE>& right_operand, \
                         const ArithHighPrecisionType<TTYPE>& value,         \
                         const size_t size);

ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, int8_t)
ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, int16_t)
ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, int32_t)
ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, int64_t)
ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, float)
ALL_ARITH_CMP_OPS(INSTANTIATE_ARITH_COMPARE_AVX512, double)

#undef INSTANTIATE_ARITH_COMPARE_AVX512

///////////////////////////////////////////////////////////////////////////

//
#undef ALL_COMPARE_OPS
#undef ALL_RANGE_OPS
#undef ALL_ARITH_CMP_OPS

}  // namespace avx512
}  // namespace x86
}  // namespace detail
}  // namespace bitset
}  // namespace milvus

#endif
