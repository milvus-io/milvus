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

#include "dynamic.h"

#include <cstddef>
#include <cstdint>
#include <type_traits>

#if defined(__x86_64__)
#include "x86/instruction_set.h"
#include "x86/avx2.h"
#include "x86/avx512.h"

using namespace milvus::bitset::detail::x86;
#endif

#if defined(__aarch64__)
#include "arm/instruction_set.h"
#include "arm/neon.h"
#include "arm/sve.h"

using namespace milvus::bitset::detail::arm;

#endif

#include "vectorized_ref.h"

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

//
namespace milvus {
namespace bitset {
namespace detail {

/////////////////////////////////////////////////////////////////////////////
// op_compare_column

// Define pointers for op_compare
template <typename T, typename U, CompareOpType Op>
using OpCompareColumnPtr = bool (*)(uint8_t* const __restrict output,
                                    const T* const __restrict t,
                                    const U* const __restrict u,
                                    const size_t size);

#define DECLARE_OP_COMPARE_COLUMN(TTYPE, UTYPE, OP)                  \
    OpCompareColumnPtr<TTYPE, UTYPE, CompareOpType::OP>              \
        op_compare_column_##TTYPE##_##UTYPE##_##OP = VectorizedRef:: \
            template op_compare_column<TTYPE, UTYPE, CompareOpType::OP>;

ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, int8_t, int8_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, int16_t, int16_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, int32_t, int32_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, int64_t, int64_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, float, float)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_COLUMN, double, double)

#undef DECLARE_OP_COMPARE_COLUMN

//
namespace dynamic {

#define DISPATCH_OP_COMPARE_COLUMN_IMPL(TTYPE, OP)                           \
    template <>                                                              \
    bool                                                                     \
    OpCompareColumnImpl<TTYPE, TTYPE, CompareOpType::OP>::op_compare_column( \
        uint8_t* const __restrict bitmask,                                   \
        const TTYPE* const __restrict t,                                     \
        const TTYPE* const __restrict u,                                     \
        const size_t size) {                                                 \
        return op_compare_column_##TTYPE##_##TTYPE##_##OP(                   \
            bitmask, t, u, size);                                            \
    }

ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, int8_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, int16_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, int32_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, int64_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, float)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_COLUMN_IMPL, double)

#undef DISPATCH_OP_COMPARE_COLUMN_IMPL

}  // namespace dynamic

/////////////////////////////////////////////////////////////////////////////
// op_compare_val
template <typename T, CompareOpType Op>
using OpCompareValPtr = bool (*)(uint8_t* const __restrict output,
                                 const T* const __restrict t,
                                 const size_t size,
                                 const T& value);

#define DECLARE_OP_COMPARE_VAL(TTYPE, OP)                                     \
    OpCompareValPtr<TTYPE, CompareOpType::OP> op_compare_val_##TTYPE##_##OP = \
        VectorizedRef::template op_compare_val<TTYPE, CompareOpType::OP>;

ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, int8_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, int16_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, int32_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, int64_t)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, float)
ALL_COMPARE_OPS(DECLARE_OP_COMPARE_VAL, double)

#undef DECLARE_OP_COMPARE_VAL

namespace dynamic {

#define DISPATCH_OP_COMPARE_VAL_IMPL(TTYPE, OP)                        \
    template <>                                                        \
    bool OpCompareValImpl<TTYPE, CompareOpType::OP>::op_compare_val(   \
        uint8_t* const __restrict bitmask,                             \
        const TTYPE* const __restrict t,                               \
        const size_t size,                                             \
        const TTYPE& value) {                                          \
        return op_compare_val_##TTYPE##_##OP(bitmask, t, size, value); \
    }

ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, int8_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, int16_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, int32_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, int64_t)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, float)
ALL_COMPARE_OPS(DISPATCH_OP_COMPARE_VAL_IMPL, double)

#undef DISPATCH_OP_COMPARE_VAL_IMPL

}  // namespace dynamic

/////////////////////////////////////////////////////////////////////////////
// op_within_range column
template <typename T, RangeType Op>
using OpWithinRangeColumnPtr = bool (*)(uint8_t* const __restrict output,
                                        const T* const __restrict lower,
                                        const T* const __restrict upper,
                                        const T* const __restrict values,
                                        const size_t size);

#define DECLARE_OP_WITHIN_RANGE_COLUMN(TTYPE, OP)                 \
    OpWithinRangeColumnPtr<TTYPE, RangeType::OP>                  \
        op_within_range_column_##TTYPE##_##OP =                   \
            VectorizedRef::template op_within_range_column<TTYPE, \
                                                           RangeType::OP>;

ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, int8_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, int16_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, int32_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, int64_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, float)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_COLUMN, double)

#undef DECLARE_OP_WITHIN_RANGE_COLUMN

//
namespace dynamic {

#define DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL(TTYPE, OP)                    \
    template <>                                                            \
    bool                                                                   \
    OpWithinRangeColumnImpl<TTYPE, RangeType::OP>::op_within_range_column( \
        uint8_t* const __restrict output,                                  \
        const TTYPE* const __restrict lower,                               \
        const TTYPE* const __restrict upper,                               \
        const TTYPE* const __restrict values,                              \
        const size_t size) {                                               \
        return op_within_range_column_##TTYPE##_##OP(                      \
            output, lower, upper, values, size);                           \
    }

ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, int8_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, int16_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, int32_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, int64_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, float)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL, double)

#undef DISPATCH_OP_WITHIN_RANGE_COLUMN_IMPL
}  // namespace dynamic

/////////////////////////////////////////////////////////////////////////////
// op_within_range val
template <typename T, RangeType Op>
using OpWithinRangeValPtr = bool (*)(uint8_t* const __restrict output,
                                     const T& lower,
                                     const T& upper,
                                     const T* const __restrict values,
                                     const size_t size);

#define DECLARE_OP_WITHIN_RANGE_VAL(TTYPE, OP) \
    OpWithinRangeValPtr<TTYPE, RangeType::OP>  \
        op_within_range_val_##TTYPE##_##OP =   \
            VectorizedRef::template op_within_range_val<TTYPE, RangeType::OP>;

ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, int8_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, int16_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, int32_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, int64_t)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, float)
ALL_RANGE_OPS(DECLARE_OP_WITHIN_RANGE_VAL, double)

#undef DECLARE_OP_WITHIN_RANGE_VAL

//
namespace dynamic {

#define DISPATCH_OP_WITHIN_RANGE_VAL_IMPL(TTYPE, OP)                      \
    template <>                                                           \
    bool OpWithinRangeValImpl<TTYPE, RangeType::OP>::op_within_range_val( \
        uint8_t* const __restrict output,                                 \
        const TTYPE& lower,                                               \
        const TTYPE& upper,                                               \
        const TTYPE* const __restrict values,                             \
        const size_t size) {                                              \
        return op_within_range_val_##TTYPE##_##OP(                        \
            output, lower, upper, values, size);                          \
    }

ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, int8_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, int16_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, int32_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, int64_t)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, float)
ALL_RANGE_OPS(DISPATCH_OP_WITHIN_RANGE_VAL_IMPL, double)

}  // namespace dynamic

/////////////////////////////////////////////////////////////////////////////
// op_arith_compare
template <typename T, ArithOpType AOp, CompareOpType CmpOp>
using OpArithComparePtr =
    bool (*)(uint8_t* const __restrict output,
             const T* const __restrict src,
             const ArithHighPrecisionType<T>& right_operand,
             const ArithHighPrecisionType<T>& value,
             const size_t size);

#define DECLARE_OP_ARITH_COMPARE(TTYPE, AOP, CMPOP)                    \
    OpArithComparePtr<TTYPE, ArithOpType::AOP, CompareOpType::CMPOP>   \
        op_arith_compare_##TTYPE##_##AOP##_##CMPOP =                   \
            VectorizedRef::template op_arith_compare<TTYPE,            \
                                                     ArithOpType::AOP, \
                                                     CompareOpType::CMPOP>;

ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, int8_t)
ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, int16_t)
ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, int32_t)
ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, int64_t)
ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, float)
ALL_ARITH_CMP_OPS(DECLARE_OP_ARITH_COMPARE, double)

#undef DECLARE_OP_ARITH_COMPARE

//
namespace dynamic {

#define DISPATCH_OP_ARITH_COMPARE(TTYPE, AOP, CMPOP)                         \
    template <>                                                              \
    bool OpArithCompareImpl<TTYPE, ArithOpType::AOP, CompareOpType::CMPOP>:: \
        op_arith_compare(uint8_t* const __restrict output,                   \
                         const TTYPE* const __restrict src,                  \
                         const ArithHighPrecisionType<TTYPE>& right_operand, \
                         const ArithHighPrecisionType<TTYPE>& value,         \
                         const size_t size) {                                \
        return op_arith_compare_##TTYPE##_##AOP##_##CMPOP(                   \
            output, src, right_operand, value, size);                        \
    }

ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, int8_t)
ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, int16_t)
ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, int32_t)
ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, int64_t)
ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, float)
ALL_ARITH_CMP_OPS(DISPATCH_OP_ARITH_COMPARE, double)

}  // namespace dynamic

}  // namespace detail
}  // namespace bitset
}  // namespace milvus

//
static void
init_dynamic_hook() {
    using namespace milvus::bitset;
    using namespace milvus::bitset::detail;

#if defined(__x86_64__)
    // AVX512 ?
    if (cpu_support_avx512()) {
#define SET_OP_COMPARE_COLUMN_AVX512(TTYPE, UTYPE, OP)              \
    op_compare_column_##TTYPE##_##UTYPE##_##OP = VectorizedAvx512:: \
        template op_compare_column<TTYPE, UTYPE, CompareOpType::OP>;
#define SET_OP_COMPARE_VAL_AVX512(TTYPE, OP) \
    op_compare_val_##TTYPE##_##OP =          \
        VectorizedAvx512::template op_compare_val<TTYPE, CompareOpType::OP>;
#define SET_OP_WITHIN_RANGE_COLUMN_AVX512(TTYPE, OP)             \
    op_within_range_column_##TTYPE##_##OP =                      \
        VectorizedAvx512::template op_within_range_column<TTYPE, \
                                                          RangeType::OP>;
#define SET_OP_WITHIN_RANGE_VAL_AVX512(TTYPE, OP) \
    op_within_range_val_##TTYPE##_##OP =          \
        VectorizedAvx512::template op_within_range_val<TTYPE, RangeType::OP>;
#define SET_ARITH_COMPARE_AVX512(TTYPE, AOP, CMPOP)                   \
    op_arith_compare_##TTYPE##_##AOP##_##CMPOP =                      \
        VectorizedAvx512::template op_arith_compare<TTYPE,            \
                                                    ArithOpType::AOP, \
                                                    CompareOpType::CMPOP>;

        // assign AVX512-related pointers
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, int8_t, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, int16_t, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, int32_t, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, int64_t, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, float, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX512, double, double)

        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX512, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX512, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX512, double)

        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, int8_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, int16_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, int32_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, int64_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, float)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX512, double)

#undef SET_OP_COMPARE_COLUMN_AVX512
#undef SET_OP_COMPARE_VAL_AVX512
#undef SET_OP_WITHIN_RANGE_COLUMN_AVX512
#undef SET_OP_WITHIN_RANGE_VAL_AVX512
#undef SET_ARITH_COMPARE_AVX512

        return;
    }

    // AVX2 ?
    if (cpu_support_avx2()) {
#define SET_OP_COMPARE_COLUMN_AVX2(TTYPE, UTYPE, OP)              \
    op_compare_column_##TTYPE##_##UTYPE##_##OP = VectorizedAvx2:: \
        template op_compare_column<TTYPE, UTYPE, CompareOpType::OP>;
#define SET_OP_COMPARE_VAL_AVX2(TTYPE, OP) \
    op_compare_val_##TTYPE##_##OP =        \
        VectorizedAvx2::template op_compare_val<TTYPE, CompareOpType::OP>;
#define SET_OP_WITHIN_RANGE_COLUMN_AVX2(TTYPE, OP) \
    op_within_range_column_##TTYPE##_##OP =        \
        VectorizedAvx2::template op_within_range_column<TTYPE, RangeType::OP>;
#define SET_OP_WITHIN_RANGE_VAL_AVX2(TTYPE, OP) \
    op_within_range_val_##TTYPE##_##OP =        \
        VectorizedAvx2::template op_within_range_val<TTYPE, RangeType::OP>;
#define SET_ARITH_COMPARE_AVX2(TTYPE, AOP, CMPOP)                   \
    op_arith_compare_##TTYPE##_##AOP##_##CMPOP =                    \
        VectorizedAvx2::template op_arith_compare<TTYPE,            \
                                                  ArithOpType::AOP, \
                                                  CompareOpType::CMPOP>;

        // assign AVX2-related pointers
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, int8_t, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, int16_t, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, int32_t, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, int64_t, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, float, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_AVX2, double, double)

        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_AVX2, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_AVX2, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_AVX2, double)

        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, int8_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, int16_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, int32_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, int64_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, float)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_AVX2, double)

#undef SET_OP_COMPARE_COLUMN_AVX2
#undef SET_OP_COMPARE_VAL_AVX2
#undef SET_OP_WITHIN_RANGE_COLUMN_AVX2
#undef SET_OP_WITHIN_RANGE_VAL_AVX2
#undef SET_ARITH_COMPARE_AVX2

        return;
    }
#endif

#if defined(__aarch64__)
#if defined(__ARM_FEATURE_SVE)
    // sve
    if (arm::InstructionSet::GetInstance().supports_sve()) {
#define SET_OP_COMPARE_COLUMN_SVE(TTYPE, UTYPE, OP)              \
    op_compare_column_##TTYPE##_##UTYPE##_##OP = VectorizedSve:: \
        template op_compare_column<TTYPE, UTYPE, CompareOpType::OP>;
#define SET_OP_COMPARE_VAL_SVE(TTYPE, OP) \
    op_compare_val_##TTYPE##_##OP =       \
        VectorizedSve::template op_compare_val<TTYPE, CompareOpType::OP>;
#define SET_OP_WITHIN_RANGE_COLUMN_SVE(TTYPE, OP) \
    op_within_range_column_##TTYPE##_##OP =       \
        VectorizedSve::template op_within_range_column<TTYPE, RangeType::OP>;
#define SET_OP_WITHIN_RANGE_VAL_SVE(TTYPE, OP) \
    op_within_range_val_##TTYPE##_##OP =       \
        VectorizedSve::template op_within_range_val<TTYPE, RangeType::OP>;
#define SET_ARITH_COMPARE_SVE(TTYPE, AOP, CMPOP)                   \
    op_arith_compare_##TTYPE##_##AOP##_##CMPOP =                   \
        VectorizedSve::template op_arith_compare<TTYPE,            \
                                                 ArithOpType::AOP, \
                                                 CompareOpType::CMPOP>;

        // assign SVE-related pointers
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, int8_t, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, int16_t, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, int32_t, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, int64_t, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, float, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_SVE, double, double)

        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_SVE, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_SVE, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_SVE, double)

        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, int8_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, int16_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, int32_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, int64_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, float)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_SVE, double)

#undef SET_OP_COMPARE_COLUMN_SVE
#undef SET_OP_COMPARE_VAL_SVE
#undef SET_OP_WITHIN_RANGE_COLUMN_SVE
#undef SET_OP_WITHIN_RANGE_VAL_SVE
#undef SET_ARITH_COMPARE_SVE

        return;
    }
#endif
    // neon ?
    {
#define SET_OP_COMPARE_COLUMN_NEON(TTYPE, UTYPE, OP)              \
    op_compare_column_##TTYPE##_##UTYPE##_##OP = VectorizedNeon:: \
        template op_compare_column<TTYPE, UTYPE, CompareOpType::OP>;
#define SET_OP_COMPARE_VAL_NEON(TTYPE, OP) \
    op_compare_val_##TTYPE##_##OP =        \
        VectorizedNeon::template op_compare_val<TTYPE, CompareOpType::OP>;
#define SET_OP_WITHIN_RANGE_COLUMN_NEON(TTYPE, OP) \
    op_within_range_column_##TTYPE##_##OP =        \
        VectorizedNeon::template op_within_range_column<TTYPE, RangeType::OP>;
#define SET_OP_WITHIN_RANGE_VAL_NEON(TTYPE, OP) \
    op_within_range_val_##TTYPE##_##OP =        \
        VectorizedNeon::template op_within_range_val<TTYPE, RangeType::OP>;
#define SET_ARITH_COMPARE_NEON(TTYPE, AOP, CMPOP)                   \
    op_arith_compare_##TTYPE##_##AOP##_##CMPOP =                    \
        VectorizedNeon::template op_arith_compare<TTYPE,            \
                                                  ArithOpType::AOP, \
                                                  CompareOpType::CMPOP>;

        // assign NEON-related pointers
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, int8_t, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, int16_t, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, int32_t, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, int64_t, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, float, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_COLUMN_NEON, double, double)

        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, int8_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, int16_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, int32_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, int64_t)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, float)
        ALL_COMPARE_OPS(SET_OP_COMPARE_VAL_NEON, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_COLUMN_NEON, double)

        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, int8_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, int16_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, int32_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, int64_t)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, float)
        ALL_RANGE_OPS(SET_OP_WITHIN_RANGE_VAL_NEON, double)

        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, int8_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, int16_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, int32_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, int64_t)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, float)
        ALL_ARITH_CMP_OPS(SET_ARITH_COMPARE_NEON, double)

#undef SET_OP_COMPARE_COLUMN_NEON
#undef SET_OP_COMPARE_VAL_NEON
#undef SET_OP_WITHIN_RANGE_COLUMN_NEON
#undef SET_OP_WITHIN_RANGE_VAL_NEON
#undef SET_ARITH_COMPARE_NEON

        return;
    }

#endif
}

// no longer needed
#undef ALL_COMPARE_OPS
#undef ALL_RANGE_OPS
#undef ALL_ARITH_CMP_OPS

//
static int init_dynamic_ = []() {
    init_dynamic_hook();

    return 0;
}();
