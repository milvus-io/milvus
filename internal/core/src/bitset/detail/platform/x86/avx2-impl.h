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

// AVX2 implementation

#pragma once

#include <immintrin.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "avx2-decl.h"

#include "bitset/common.h"
#include "common.h"

namespace milvus {
namespace bitset {
namespace detail {
namespace x86 {
namespace avx2 {

namespace {

// count is expected to be in range [0, 32)
inline uint32_t
get_mask(const size_t count) {
    return (uint32_t(1) << count) - uint32_t(1);
}

//
template <CompareOpType Op>
struct CmpHelperI8 {};

template <>
struct CmpHelperI8<CompareOpType::EQ> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpeq_epi8(a, b);
    }
};

template <>
struct CmpHelperI8<CompareOpType::GE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi8(b, a), _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI8<CompareOpType::GT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi8(a, b);
    }
};

template <>
struct CmpHelperI8<CompareOpType::LE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi8(a, b), _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI8<CompareOpType::LT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi8(b, a);
    }
};

template <>
struct CmpHelperI8<CompareOpType::NE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpeq_epi8(a, b), _mm256_set1_epi32(-1));
    }
};

//
template <CompareOpType Op>
struct CmpHelperI16 {};

template <>
struct CmpHelperI16<CompareOpType::EQ> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpeq_epi16(a, b);
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_cmpeq_epi16(a, b);
    }
};

template <>
struct CmpHelperI16<CompareOpType::GE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi16(b, a),
                                _mm256_set1_epi32(-1));
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_xor_si128(_mm_cmpgt_epi16(b, a), _mm_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI16<CompareOpType::GT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi16(a, b);
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_cmpgt_epi16(a, b);
    }
};

template <>
struct CmpHelperI16<CompareOpType::LE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi16(a, b),
                                _mm256_set1_epi32(-1));
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_xor_si128(_mm_cmpgt_epi16(a, b), _mm_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI16<CompareOpType::LT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi16(b, a);
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_cmpgt_epi16(b, a);
    }
};

template <>
struct CmpHelperI16<CompareOpType::NE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpeq_epi16(a, b),
                                _mm256_set1_epi32(-1));
    }

    static inline __m128i
    compare(const __m128i a, const __m128i b) {
        return _mm_xor_si128(_mm_cmpeq_epi16(a, b), _mm_set1_epi32(-1));
    }
};

//
template <CompareOpType Op>
struct CmpHelperI32 {};

template <>
struct CmpHelperI32<CompareOpType::EQ> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpeq_epi32(a, b);
    }
};

template <>
struct CmpHelperI32<CompareOpType::GE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi32(b, a),
                                _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI32<CompareOpType::GT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi32(a, b);
    }
};

template <>
struct CmpHelperI32<CompareOpType::LE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi32(a, b),
                                _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI32<CompareOpType::LT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi32(b, a);
    }
};

template <>
struct CmpHelperI32<CompareOpType::NE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpeq_epi32(a, b),
                                _mm256_set1_epi32(-1));
    }
};

//
template <CompareOpType Op>
struct CmpHelperI64 {};

template <>
struct CmpHelperI64<CompareOpType::EQ> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpeq_epi64(a, b);
    }
};

template <>
struct CmpHelperI64<CompareOpType::GE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi64(b, a),
                                _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI64<CompareOpType::GT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi64(a, b);
    }
};

template <>
struct CmpHelperI64<CompareOpType::LE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpgt_epi64(a, b),
                                _mm256_set1_epi32(-1));
    }
};

template <>
struct CmpHelperI64<CompareOpType::LT> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_cmpgt_epi64(b, a);
    }
};

template <>
struct CmpHelperI64<CompareOpType::NE> {
    static inline __m256i
    compare(const __m256i a, const __m256i b) {
        return _mm256_xor_si256(_mm256_cmpeq_epi64(a, b),
                                _mm256_set1_epi32(-1));
    }
};

}  // namespace

///////////////////////////////////////////////////////////////////////////

//
template <CompareOpType Op>
bool
OpCompareValImpl<int8_t, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const int8_t* const __restrict src,
                                             const size_t size,
                                             const int8_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);
    const __m256i target = _mm256_set1_epi8(val);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const __m256i v0 = _mm256_loadu_si256((const __m256i*)(src + i));
        const __m256i cmp = CmpHelperI8<Op>::compare(v0, target);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        res_u32[i / 32] = mmask;
    }

    if (size32 != size) {
        // 8, 16 or 24 elements to process
        const __m256i mask =
            _mm256_setr_epi64x((size - size32 >= 8) ? (-1) : 0,
                               (size - size32 >= 16) ? (-1) : 0,
                               (size - size32 >= 24) ? (-1) : 0,
                               0);

        const __m256i v0 =
            _mm256_maskload_epi64((const long long*)(src + size32), mask);
        const __m256i cmp = CmpHelperI8<Op>::compare(v0, target);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        if (size - size32 >= 8) {
            res_u8[size32 / 8 + 0] = (mmask & 0xFF);
        }
        if (size - size32 >= 16) {
            res_u8[size32 / 8 + 1] = ((mmask >> 8) & 0xFF);
        }
        if (size - size32 >= 24) {
            res_u8[size32 / 8 + 2] = ((mmask >> 16) & 0xFF);
        }
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int16_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict src,
    const size_t size,
    const int16_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);
    const __m256i target = _mm256_set1_epi16(val);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const __m256i v0 = _mm256_loadu_si256((const __m256i*)(src + i));
        const __m256i cmp = CmpHelperI16<Op>::compare(v0, target);
        const __m256i pcmp = _mm256_packs_epi16(cmp, cmp);
        const __m256i qcmp =
            _mm256_permute4x64_epi64(pcmp, _MM_SHUFFLE(3, 1, 2, 0));
        const uint16_t mmask = _mm256_movemask_epi8(qcmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const __m128i v0 = _mm_loadu_si128((const __m128i*)(src + size16));
        const __m128i target0 = _mm_set1_epi16(val);
        const __m128i cmp = CmpHelperI16<Op>::compare(v0, target0);
        const __m128i pcmp = _mm_packs_epi16(cmp, cmp);
        const uint32_t mmask = _mm_movemask_epi8(pcmp) & 0xFF;

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int32_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict src,
    const size_t size,
    const int32_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256i target = _mm256_set1_epi32(val);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0 = _mm256_loadu_si256((const __m256i*)(src + i));
        const __m256i cmp = CmpHelperI32<Op>::compare(v0, target);
        const uint8_t mmask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<int64_t, Op>::op_compare_val(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict src,
    const size_t size,
    const int64_t& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256i target = _mm256_set1_epi64x(val);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0 = _mm256_loadu_si256((const __m256i*)(src + i));
        const __m256i v1 = _mm256_loadu_si256((const __m256i*)(src + i + 4));
        const __m256i cmp0 = CmpHelperI64<Op>::compare(v0, target);
        const __m256i cmp1 = CmpHelperI64<Op>::compare(v1, target);
        const uint8_t mmask0 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
        const uint8_t mmask1 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<float, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                            const float* const __restrict src,
                                            const size_t size,
                                            const float& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred = ComparePredicate<float, Op>::value;

    const __m256 target = _mm256_set1_ps(val);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256 v0 = _mm256_loadu_ps(src + i);
        const __m256 cmp = _mm256_cmp_ps(v0, target, pred);
        const uint8_t mmask = _mm256_movemask_ps(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareValImpl<double, Op>::op_compare_val(uint8_t* const __restrict res_u8,
                                             const double* const __restrict src,
                                             const size_t size,
                                             const double& val) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred = ComparePredicate<float, Op>::value;

    const __m256d target = _mm256_set1_pd(val);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256d v0 = _mm256_loadu_pd(src + i);
        const __m256d v1 = _mm256_loadu_pd(src + i + 4);
        const __m256d cmp0 = _mm256_cmp_pd(v0, target, pred);
        const __m256d cmp1 = _mm256_cmp_pd(v1, target, pred);
        const uint8_t mmask0 = _mm256_movemask_pd(cmp0);
        const uint8_t mmask1 = _mm256_movemask_pd(cmp1);

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

//
template <CompareOpType Op>
bool
OpCompareColumnImpl<int8_t, int8_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict left,
    const int8_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(left + i));
        const __m256i v0r = _mm256_loadu_si256((const __m256i*)(right + i));
        const __m256i cmp = CmpHelperI8<Op>::compare(v0l, v0r);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        res_u32[i / 32] = mmask;
    }

    if (size32 != size) {
        // 8, 16 or 24 elements to process
        const __m256i mask =
            _mm256_setr_epi64x((size - size32 >= 8) ? (-1) : 0,
                               (size - size32 >= 16) ? (-1) : 0,
                               (size - size32 >= 24) ? (-1) : 0,
                               0);

        const __m256i v0l =
            _mm256_maskload_epi64((const long long*)(left + size32), mask);
        const __m256i v0r =
            _mm256_maskload_epi64((const long long*)(right + size32), mask);
        const __m256i cmp = CmpHelperI8<Op>::compare(v0l, v0r);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        if (size - size32 >= 8) {
            res_u8[size32 / 8 + 0] = (mmask & 0xFF);
        }
        if (size - size32 >= 16) {
            res_u8[size32 / 8 + 1] = ((mmask >> 8) & 0xFF);
        }
        if (size - size32 >= 24) {
            res_u8[size32 / 8 + 2] = ((mmask >> 16) & 0xFF);
        }
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int16_t, int16_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict left,
    const int16_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(left + i));
        const __m256i v0r = _mm256_loadu_si256((const __m256i*)(right + i));
        const __m256i cmp = CmpHelperI16<Op>::compare(v0l, v0r);
        const __m256i pcmp = _mm256_packs_epi16(cmp, cmp);
        const __m256i qcmp =
            _mm256_permute4x64_epi64(pcmp, _MM_SHUFFLE(3, 1, 2, 0));
        const uint16_t mmask = _mm256_movemask_epi8(qcmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const __m128i v0l = _mm_loadu_si128((const __m128i*)(left + size16));
        const __m128i v0r = _mm_loadu_si128((const __m128i*)(right + size16));
        const __m128i cmp = CmpHelperI16<Op>::compare(v0l, v0r);
        const __m128i pcmp = _mm_packs_epi16(cmp, cmp);
        const uint32_t mmask = _mm_movemask_epi8(pcmp) & 0xFF;

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int32_t, int32_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict left,
    const int32_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(left + i));
        const __m256i v0r = _mm256_loadu_si256((const __m256i*)(right + i));
        const __m256i cmp = CmpHelperI32<Op>::compare(v0l, v0r);
        const uint8_t mmask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<int64_t, int64_t, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict left,
    const int64_t* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(left + i));
        const __m256i v1l = _mm256_loadu_si256((const __m256i*)(left + i + 4));
        const __m256i v0r = _mm256_loadu_si256((const __m256i*)(right + i));
        const __m256i v1r = _mm256_loadu_si256((const __m256i*)(right + i + 4));
        const __m256i cmp0 = CmpHelperI64<Op>::compare(v0l, v0r);
        const __m256i cmp1 = CmpHelperI64<Op>::compare(v1l, v1r);
        const uint8_t mmask0 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
        const uint8_t mmask1 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<float, float, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict left,
    const float* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred = ComparePredicate<float, Op>::value;

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256 v0l = _mm256_loadu_ps(left + i);
        const __m256 v0r = _mm256_loadu_ps(right + i);
        const __m256 cmp = _mm256_cmp_ps(v0l, v0r, pred);
        const uint8_t mmask = _mm256_movemask_ps(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <CompareOpType Op>
bool
OpCompareColumnImpl<double, double, Op>::op_compare_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict left,
    const double* const __restrict right,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred = ComparePredicate<double, Op>::value;

    // todo: aligned reads & writes
    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256d v0l = _mm256_loadu_pd(left + i);
        const __m256d v1l = _mm256_loadu_pd(left + i + 4);
        const __m256d v0r = _mm256_loadu_pd(right + i);
        const __m256d v1r = _mm256_loadu_pd(right + i + 4);
        const __m256d cmp0 = _mm256_cmp_pd(v0l, v0r, pred);
        const __m256d cmp1 = _mm256_cmp_pd(v1l, v1r, pred);
        const uint8_t mmask0 = _mm256_movemask_pd(cmp0);
        const uint8_t mmask1 = _mm256_movemask_pd(cmp1);

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

//
template <RangeType Op>
bool
OpWithinRangeColumnImpl<int8_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict lower,
    const int8_t* const __restrict upper,
    const int8_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(lower + i));
        const __m256i v0u = _mm256_loadu_si256((const __m256i*)(upper + i));
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI8<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m256i cmpu =
            CmpHelperI8<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        res_u32[i / 32] = mmask;
    }

    if (size32 != size) {
        // 8, 16 or 24 elements to process
        const __m256i mask =
            _mm256_setr_epi64x((size - size32 >= 8) ? (-1) : 0,
                               (size - size32 >= 16) ? (-1) : 0,
                               (size - size32 >= 24) ? (-1) : 0,
                               0);

        const __m256i v0l =
            _mm256_maskload_epi64((const long long*)(lower + size32), mask);
        const __m256i v0u =
            _mm256_maskload_epi64((const long long*)(upper + size32), mask);
        const __m256i v0v =
            _mm256_maskload_epi64((const long long*)(values + size32), mask);
        const __m256i cmpl =
            CmpHelperI8<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m256i cmpu =
            CmpHelperI8<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        if (size - size32 >= 8) {
            res_u8[size32 / 8 + 0] = (mmask & 0xFF);
        }
        if (size - size32 >= 16) {
            res_u8[size32 / 8 + 1] = ((mmask >> 8) & 0xFF);
        }
        if (size - size32 >= 24) {
            res_u8[size32 / 8 + 2] = ((mmask >> 16) & 0xFF);
        }
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int16_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict lower,
    const int16_t* const __restrict upper,
    const int16_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(lower + i));
        const __m256i v0u = _mm256_loadu_si256((const __m256i*)(upper + i));
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI16<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m256i cmpu =
            CmpHelperI16<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const __m256i pcmp = _mm256_packs_epi16(cmp, cmp);
        const __m256i qcmp =
            _mm256_permute4x64_epi64(pcmp, _MM_SHUFFLE(3, 1, 2, 0));
        const uint16_t mmask = _mm256_movemask_epi8(qcmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const __m128i v0l = _mm_loadu_si128((const __m128i*)(lower + size16));
        const __m128i v0u = _mm_loadu_si128((const __m128i*)(upper + size16));
        const __m128i v0v = _mm_loadu_si128((const __m128i*)(values + size16));
        const __m128i cmpl =
            CmpHelperI16<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m128i cmpu =
            CmpHelperI16<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m128i cmp = _mm_and_si128(cmpl, cmpu);
        const __m128i pcmp = _mm_packs_epi16(cmp, cmp);
        const uint32_t mmask = _mm_movemask_epi8(pcmp) & 0xFF;

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int32_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict lower,
    const int32_t* const __restrict upper,
    const int32_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(lower + i));
        const __m256i v0u = _mm256_loadu_si256((const __m256i*)(upper + i));
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI32<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m256i cmpu =
            CmpHelperI32<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint8_t mmask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<int64_t, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict lower,
    const int64_t* const __restrict upper,
    const int64_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0l = _mm256_loadu_si256((const __m256i*)(lower + i));
        const __m256i v1l = _mm256_loadu_si256((const __m256i*)(lower + i + 4));
        const __m256i v0u = _mm256_loadu_si256((const __m256i*)(upper + i));
        const __m256i v1u = _mm256_loadu_si256((const __m256i*)(upper + i + 4));
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i v1v =
            _mm256_loadu_si256((const __m256i*)(values + i + 4));
        const __m256i cmp0l =
            CmpHelperI64<Range2Compare<Op>::lower>::compare(v0l, v0v);
        const __m256i cmp0u =
            CmpHelperI64<Range2Compare<Op>::upper>::compare(v0v, v0u);
        const __m256i cmp1l =
            CmpHelperI64<Range2Compare<Op>::lower>::compare(v1l, v1v);
        const __m256i cmp1u =
            CmpHelperI64<Range2Compare<Op>::upper>::compare(v1v, v1u);
        const __m256i cmp0 = _mm256_and_si256(cmp0l, cmp0u);
        const __m256i cmp1 = _mm256_and_si256(cmp1l, cmp1u);
        const uint8_t mmask0 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
        const uint8_t mmask1 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<float, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const float* const __restrict lower,
    const float* const __restrict upper,
    const float* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred_lower =
        ComparePredicate<float, Range2Compare<Op>::lower>::value;
    constexpr auto pred_upper =
        ComparePredicate<float, Range2Compare<Op>::upper>::value;

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256 v0l = _mm256_loadu_ps(lower + i);
        const __m256 v0u = _mm256_loadu_ps(upper + i);
        const __m256 v0v = _mm256_loadu_ps(values + i);
        const __m256 cmpl = _mm256_cmp_ps(v0l, v0v, pred_lower);
        const __m256 cmpu = _mm256_cmp_ps(v0v, v0u, pred_upper);
        const __m256 cmp = _mm256_and_ps(cmpl, cmpu);
        const uint8_t mmask = _mm256_movemask_ps(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeColumnImpl<double, Op>::op_within_range_column(
    uint8_t* const __restrict res_u8,
    const double* const __restrict lower,
    const double* const __restrict upper,
    const double* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    constexpr auto pred_lower =
        ComparePredicate<double, Range2Compare<Op>::lower>::value;
    constexpr auto pred_upper =
        ComparePredicate<double, Range2Compare<Op>::upper>::value;

    // todo: aligned reads & writes
    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256d v0l = _mm256_loadu_pd(lower + i);
        const __m256d v1l = _mm256_loadu_pd(lower + i + 4);
        const __m256d v0u = _mm256_loadu_pd(upper + i);
        const __m256d v1u = _mm256_loadu_pd(upper + i + 4);
        const __m256d v0v = _mm256_loadu_pd(values + i);
        const __m256d v1v = _mm256_loadu_pd(values + i + 4);
        const __m256d cmp0l = _mm256_cmp_pd(v0l, v0v, pred_lower);
        const __m256d cmp0u = _mm256_cmp_pd(v0v, v0u, pred_upper);
        const __m256d cmp1l = _mm256_cmp_pd(v1l, v1v, pred_lower);
        const __m256d cmp1u = _mm256_cmp_pd(v1v, v1u, pred_upper);
        const __m256d cmp0 = _mm256_and_pd(cmp0l, cmp0u);
        const __m256d cmp1 = _mm256_and_pd(cmp1l, cmp1u);
        const uint8_t mmask0 = _mm256_movemask_pd(cmp0);
        const uint8_t mmask1 = _mm256_movemask_pd(cmp1);

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

template <RangeType Op>
bool
OpWithinRangeValImpl<int8_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int8_t& lower,
    const int8_t& upper,
    const int8_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint32_t* const __restrict res_u32 = reinterpret_cast<uint32_t*>(res_u8);
    const __m256i lower_v = _mm256_set1_epi8(lower);
    const __m256i upper_v = _mm256_set1_epi8(upper);

    // todo: aligned reads & writes

    const size_t size32 = (size / 32) * 32;
    for (size_t i = 0; i < size32; i += 32) {
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI8<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const __m256i cmpu =
            CmpHelperI8<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        res_u32[i / 32] = mmask;
    }

    if (size32 != size) {
        // 8, 16 or 24 elements to process
        const __m256i mask =
            _mm256_setr_epi64x((size - size32 >= 8) ? (-1) : 0,
                               (size - size32 >= 16) ? (-1) : 0,
                               (size - size32 >= 24) ? (-1) : 0,
                               0);

        const __m256i v0v =
            _mm256_maskload_epi64((const long long*)(values + size32), mask);
        const __m256i cmpl =
            CmpHelperI8<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const __m256i cmpu =
            CmpHelperI8<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint32_t mmask = _mm256_movemask_epi8(cmp);

        if (size - size32 >= 8) {
            res_u8[size32 / 8 + 0] = (mmask & 0xFF);
        }
        if (size - size32 >= 16) {
            res_u8[size32 / 8 + 1] = ((mmask >> 8) & 0xFF);
        }
        if (size - size32 >= 24) {
            res_u8[size32 / 8 + 2] = ((mmask >> 16) & 0xFF);
        }
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int16_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int16_t& lower,
    const int16_t& upper,
    const int16_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    uint16_t* const __restrict res_u16 = reinterpret_cast<uint16_t*>(res_u8);
    const __m256i lower_v = _mm256_set1_epi16(lower);
    const __m256i upper_v = _mm256_set1_epi16(upper);

    // todo: aligned reads & writes

    const size_t size16 = (size / 16) * 16;
    for (size_t i = 0; i < size16; i += 16) {
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI16<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const __m256i cmpu =
            CmpHelperI16<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const __m256i pcmp = _mm256_packs_epi16(cmp, cmp);
        const __m256i qcmp =
            _mm256_permute4x64_epi64(pcmp, _MM_SHUFFLE(3, 1, 2, 0));
        const uint16_t mmask = _mm256_movemask_epi8(qcmp);

        res_u16[i / 16] = mmask;
    }

    if (size16 != size) {
        // 8 elements to process
        const __m128i lower_v1 = _mm_set1_epi16(lower);
        const __m128i upper_v1 = _mm_set1_epi16(upper);
        const __m128i v0v = _mm_loadu_si128((const __m128i*)(values + size16));
        const __m128i cmpl =
            CmpHelperI16<Range2Compare<Op>::lower>::compare(lower_v1, v0v);
        const __m128i cmpu =
            CmpHelperI16<Range2Compare<Op>::upper>::compare(v0v, upper_v1);
        const __m128i cmp = _mm_and_si128(cmpl, cmpu);
        const __m128i pcmp = _mm_packs_epi16(cmp, cmp);
        const uint32_t mmask = _mm_movemask_epi8(pcmp) & 0xFF;

        res_u8[size16 / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int32_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int32_t& lower,
    const int32_t& upper,
    const int32_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256i lower_v = _mm256_set1_epi32(lower);
    const __m256i upper_v = _mm256_set1_epi32(upper);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i cmpl =
            CmpHelperI32<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const __m256i cmpu =
            CmpHelperI32<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const __m256i cmp = _mm256_and_si256(cmpl, cmpu);
        const uint8_t mmask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<int64_t, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const int64_t& lower,
    const int64_t& upper,
    const int64_t* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256i lower_v = _mm256_set1_epi64x(lower);
    const __m256i upper_v = _mm256_set1_epi64x(upper);

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256i v0v = _mm256_loadu_si256((const __m256i*)(values + i));
        const __m256i v1v =
            _mm256_loadu_si256((const __m256i*)(values + i + 4));
        const __m256i cmp0l =
            CmpHelperI64<Range2Compare<Op>::lower>::compare(lower_v, v0v);
        const __m256i cmp0u =
            CmpHelperI64<Range2Compare<Op>::upper>::compare(v0v, upper_v);
        const __m256i cmp1l =
            CmpHelperI64<Range2Compare<Op>::lower>::compare(lower_v, v1v);
        const __m256i cmp1u =
            CmpHelperI64<Range2Compare<Op>::upper>::compare(v1v, upper_v);
        const __m256i cmp0 = _mm256_and_si256(cmp0l, cmp0u);
        const __m256i cmp1 = _mm256_and_si256(cmp1l, cmp1u);
        const uint8_t mmask0 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
        const uint8_t mmask1 = _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<float, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const float& lower,
    const float& upper,
    const float* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256 lower_v = _mm256_set1_ps(lower);
    const __m256 upper_v = _mm256_set1_ps(upper);
    constexpr auto pred_lower =
        ComparePredicate<float, Range2Compare<Op>::lower>::value;
    constexpr auto pred_upper =
        ComparePredicate<float, Range2Compare<Op>::upper>::value;

    // todo: aligned reads & writes

    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256 v0v = _mm256_loadu_ps(values + i);
        const __m256 cmpl = _mm256_cmp_ps(lower_v, v0v, pred_lower);
        const __m256 cmpu = _mm256_cmp_ps(v0v, upper_v, pred_upper);
        const __m256 cmp = _mm256_and_ps(cmpl, cmpu);
        const uint8_t mmask = _mm256_movemask_ps(cmp);

        res_u8[i / 8] = mmask;
    }

    return true;
}

template <RangeType Op>
bool
OpWithinRangeValImpl<double, Op>::op_within_range_val(
    uint8_t* const __restrict res_u8,
    const double& lower,
    const double& upper,
    const double* const __restrict values,
    const size_t size) {
    // the restriction of the API
    assert((size % 8) == 0);

    //
    const __m256d lower_v = _mm256_set1_pd(lower);
    const __m256d upper_v = _mm256_set1_pd(upper);
    constexpr auto pred_lower =
        ComparePredicate<double, Range2Compare<Op>::lower>::value;
    constexpr auto pred_upper =
        ComparePredicate<double, Range2Compare<Op>::upper>::value;

    // todo: aligned reads & writes
    const size_t size8 = (size / 8) * 8;
    for (size_t i = 0; i < size8; i += 8) {
        const __m256d v0v = _mm256_loadu_pd(values + i);
        const __m256d v1v = _mm256_loadu_pd(values + i + 4);
        const __m256d cmp0l = _mm256_cmp_pd(lower_v, v0v, pred_lower);
        const __m256d cmp0u = _mm256_cmp_pd(v0v, upper_v, pred_upper);
        const __m256d cmp1l = _mm256_cmp_pd(lower_v, v1v, pred_lower);
        const __m256d cmp1u = _mm256_cmp_pd(v1v, upper_v, pred_upper);
        const __m256d cmp0 = _mm256_and_pd(cmp0l, cmp0u);
        const __m256d cmp1 = _mm256_and_pd(cmp1l, cmp1u);
        const uint8_t mmask0 = _mm256_movemask_pd(cmp0);
        const uint8_t mmask1 = _mm256_movemask_pd(cmp1);

        res_u8[i / 8] = mmask0 + mmask1 * 16;
    }

    return true;
}

///////////////////////////////////////////////////////////////////////////

namespace {

//
template <ArithOpType Op, CompareOpType CmpOp>
struct ArithHelperI64 {};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Add, CmpOp> {
    static inline __m256i
    op(const __m256i left, const __m256i right, const __m256i value) {
        // left + right ?? value
        return CmpHelperI64<CmpOp>::compare(_mm256_add_epi64(left, right),
                                            value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Sub, CmpOp> {
    static inline __m256i
    op(const __m256i left, const __m256i right, const __m256i value) {
        // left - right ?? value
        return CmpHelperI64<CmpOp>::compare(_mm256_sub_epi64(left, right),
                                            value);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperI64<ArithOpType::Mul, CmpOp> {
    static inline __m256i
    op(const __m256i left, const __m256i right, const __m256i value) {
        // left * right ?? value

        // draft: the code from Agner Fog's vectorclass library
        const __m256i a = left;
        const __m256i b = right;
        const __m256i bswap = _mm256_shuffle_epi32(b, 0xB1);  // swap H<->L
        const __m256i prodlh =
            _mm256_mullo_epi32(a, bswap);             // 32 bit L*H products
        const __m256i zero = _mm256_setzero_si256();  // 0
        const __m256i prodlh2 =
            _mm256_hadd_epi32(prodlh, zero);  // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
        const __m256i prodlh3 = _mm256_shuffle_epi32(
            prodlh2, 0x73);  // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
        const __m256i prodll =
            _mm256_mul_epu32(a, b);  // a0Lb0L,a1Lb1L, 64 bit unsigned products
        const __m256i prod = _mm256_add_epi64(
            prodll,
            prodlh3);  // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32

        return CmpHelperI64<CmpOp>::compare(prod, value);
    }
};

// todo: Mul, Div, Mod

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF32 {};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Add, CmpOp> {
    static inline __m256
    op(const __m256 left, const __m256 right, const __m256 value) {
        // left + right == value
        constexpr auto pred = ComparePredicate<float, CmpOp>::value;
        return _mm256_cmp_ps(_mm256_add_ps(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Sub, CmpOp> {
    static inline __m256
    op(const __m256 left, const __m256 right, const __m256 value) {
        // left - right == value
        constexpr auto pred = ComparePredicate<float, CmpOp>::value;
        return _mm256_cmp_ps(_mm256_sub_ps(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Mul, CmpOp> {
    static inline __m256
    op(const __m256 left, const __m256 right, const __m256 value) {
        // left * right == value
        constexpr auto pred = ComparePredicate<float, CmpOp>::value;
        return _mm256_cmp_ps(_mm256_mul_ps(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF32<ArithOpType::Div, CmpOp> {
    static inline __m256
    op(const __m256 left, const __m256 right, const __m256 value) {
        // left == right * value
        constexpr auto pred = ComparePredicate<float, CmpOp>::value;
        return _mm256_cmp_ps(left, _mm256_mul_ps(right, value), pred);
    }
};

// todo: Mod

//
template <ArithOpType AOp, CompareOpType CmpOp>
struct ArithHelperF64 {};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Add, CmpOp> {
    static inline __m256d
    op(const __m256d left, const __m256d right, const __m256d value) {
        // left + right == value
        constexpr auto pred = ComparePredicate<double, CmpOp>::value;
        return _mm256_cmp_pd(_mm256_add_pd(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Sub, CmpOp> {
    static inline __m256d
    op(const __m256d left, const __m256d right, const __m256d value) {
        // left - right == value
        constexpr auto pred = ComparePredicate<double, CmpOp>::value;
        return _mm256_cmp_pd(_mm256_sub_pd(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Mul, CmpOp> {
    static inline __m256d
    op(const __m256d left, const __m256d right, const __m256d value) {
        // left * right == value
        constexpr auto pred = ComparePredicate<double, CmpOp>::value;
        return _mm256_cmp_pd(_mm256_mul_pd(left, right), value, pred);
    }
};

template <CompareOpType CmpOp>
struct ArithHelperF64<ArithOpType::Div, CmpOp> {
    static inline __m256d
    op(const __m256d left, const __m256d right, const __m256d value) {
        // left == right * value
        constexpr auto pred = ComparePredicate<double, CmpOp>::value;
        return _mm256_cmp_pd(left, _mm256_mul_pd(right, value), pred);
    }
};

}  // namespace

// todo: Mul, Div, Mod

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int8_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int8_t* const __restrict src,
    const ArithHighPrecisionType<int8_t>& right_operand,
    const ArithHighPrecisionType<int8_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Div || AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const __m256i right_v = _mm256_set1_epi64x(right_operand);
        const __m256i value_v = _mm256_set1_epi64x(value);
        const uint64_t* const __restrict src_u64 =
            reinterpret_cast<const uint64_t*>(src);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const uint64_t v = src_u64[i / 8];
            const __m256i v0s = _mm256_cvtepi8_epi64(_mm_set_epi64x(0, v));
            const __m256i v1s =
                _mm256_cvtepi8_epi64(_mm_set_epi64x(0, v >> 32));
            const __m256i cmp0 =
                ArithHelperI64<AOp, CmpOp>::op(v0s, right_v, value_v);
            const __m256i cmp1 =
                ArithHelperI64<AOp, CmpOp>::op(v1s, right_v, value_v);
            const uint8_t mmask0 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
            const uint8_t mmask1 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

            res_u8[i / 8] = mmask0 + mmask1 * 16;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int16_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int16_t* const __restrict src,
    const ArithHighPrecisionType<int16_t>& right_operand,
    const ArithHighPrecisionType<int16_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Div || AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const __m256i right_v = _mm256_set1_epi64x(right_operand);
        const __m256i value_v = _mm256_set1_epi64x(value);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const __m128i vs = _mm_loadu_si128((const __m128i*)(src + i));
            const __m256i v0s = _mm256_cvtepi16_epi64(vs);
            const __m128i v1sr = _mm_set_epi64x(0, _mm_extract_epi64(vs, 1));
            const __m256i v1s = _mm256_cvtepi16_epi64(v1sr);
            const __m256i cmp0 =
                ArithHelperI64<AOp, CmpOp>::op(v0s, right_v, value_v);
            const __m256i cmp1 =
                ArithHelperI64<AOp, CmpOp>::op(v1s, right_v, value_v);
            const uint8_t mmask0 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
            const uint8_t mmask1 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

            res_u8[i / 8] = mmask0 + mmask1 * 16;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int32_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int32_t* const __restrict src,
    const ArithHighPrecisionType<int32_t>& right_operand,
    const ArithHighPrecisionType<int32_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Div || AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const __m256i right_v = _mm256_set1_epi64x(right_operand);
        const __m256i value_v = _mm256_set1_epi64x(value);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const __m256i vs = _mm256_loadu_si256((const __m256i*)(src + i));
            const __m256i v0s =
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(vs, 0));
            const __m256i v1s =
                _mm256_cvtepi32_epi64(_mm256_extracti128_si256(vs, 1));
            const __m256i cmp0 =
                ArithHelperI64<AOp, CmpOp>::op(v0s, right_v, value_v);
            const __m256i cmp1 =
                ArithHelperI64<AOp, CmpOp>::op(v1s, right_v, value_v);
            const uint8_t mmask0 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
            const uint8_t mmask1 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

            res_u8[i / 8] = mmask0 + mmask1 * 16;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<int64_t, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const int64_t* const __restrict src,
    const ArithHighPrecisionType<int64_t>& right_operand,
    const ArithHighPrecisionType<int64_t>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Div || AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);
        static_assert(std::is_same_v<int64_t, ArithHighPrecisionType<int64_t>>);

        //
        const __m256i right_v = _mm256_set1_epi64x(right_operand);
        const __m256i value_v = _mm256_set1_epi64x(value);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const __m256i v0s = _mm256_loadu_si256((const __m256i*)(src + i));
            const __m256i v1s =
                _mm256_loadu_si256((const __m256i*)(src + i + 4));
            const __m256i cmp0 =
                ArithHelperI64<AOp, CmpOp>::op(v0s, right_v, value_v);
            const __m256i cmp1 =
                ArithHelperI64<AOp, CmpOp>::op(v1s, right_v, value_v);
            const uint8_t mmask0 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp0));
            const uint8_t mmask1 =
                _mm256_movemask_pd(_mm256_castsi256_pd(cmp1));

            res_u8[i / 8] = mmask0 + mmask1 * 16;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<float, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const float* const __restrict src,
    const ArithHighPrecisionType<float>& right_operand,
    const ArithHighPrecisionType<float>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);

        //
        const __m256 right_v = _mm256_set1_ps(right_operand);
        const __m256 value_v = _mm256_set1_ps(value);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const __m256 v0s = _mm256_loadu_ps(src + i);
            const __m256 cmp =
                ArithHelperF32<AOp, CmpOp>::op(v0s, right_v, value_v);
            const uint8_t mmask = _mm256_movemask_ps(cmp);

            res_u8[i / 8] = mmask;
        }

        return true;
    }
}

template <ArithOpType AOp, CompareOpType CmpOp>
bool
OpArithCompareImpl<double, AOp, CmpOp>::op_arith_compare(
    uint8_t* const __restrict res_u8,
    const double* const __restrict src,
    const ArithHighPrecisionType<double>& right_operand,
    const ArithHighPrecisionType<double>& value,
    const size_t size) {
    if constexpr (AOp == ArithOpType::Mod) {
        return false;
    } else {
        // the restriction of the API
        assert((size % 8) == 0);

        //
        const __m256d right_v = _mm256_set1_pd(right_operand);
        const __m256d value_v = _mm256_set1_pd(value);

        // todo: aligned reads & writes

        const size_t size8 = (size / 8) * 8;
        for (size_t i = 0; i < size8; i += 8) {
            const __m256d v0s = _mm256_loadu_pd(src + i);
            const __m256d v1s = _mm256_loadu_pd(src + i + 4);
            const __m256d cmp0 =
                ArithHelperF64<AOp, CmpOp>::op(v0s, right_v, value_v);
            const __m256d cmp1 =
                ArithHelperF64<AOp, CmpOp>::op(v1s, right_v, value_v);
            const uint8_t mmask0 = _mm256_movemask_pd(cmp0);
            const uint8_t mmask1 = _mm256_movemask_pd(cmp1);

            res_u8[i / 8] = mmask0 + mmask1 * 16;
        }

        return true;
    }
}

///////////////////////////////////////////////////////////////////////////

}  // namespace avx2
}  // namespace x86
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
