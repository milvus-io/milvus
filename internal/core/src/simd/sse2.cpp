// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#if defined(__x86_64__)

#include "sse2.h"

#include <emmintrin.h>
#include <iostream>

namespace milvus {
namespace simd {

#define ALIGNED(x) __attribute__((aligned(x)))

BitsetBlockType
GetBitsetBlockSSE2(const bool* src) {
    if constexpr (BITSET_BLOCK_SIZE == 8) {
        // BitsetBlockType has 64 bits
        __m128i highbit = _mm_set1_epi8(0x7F);
        uint16_t tmp[4];
        for (size_t i = 0; i < 4; i += 1) {
            // Outer function assert (src has 64 * n length)
            __m128i boolvec = _mm_loadu_si128((__m128i*)&src[i * 16]);
            __m128i highbits = _mm_add_epi8(boolvec, highbit);
            tmp[i] = _mm_movemask_epi8(highbits);
        }

        __m128i tmpvec = _mm_loadu_si64(tmp);
        BitsetBlockType res;
        _mm_storeu_si64(&res, tmpvec);
        return res;
    } else {
        // Others has 32 bits
        __m128i highbit = _mm_set1_epi8(0x7F);
        uint16_t tmp[8];
        for (size_t i = 0; i < 2; i += 1) {
            __m128i boolvec = _mm_loadu_si128((__m128i*)&src[i * 16]);
            __m128i highbits = _mm_add_epi8(boolvec, highbit);
            tmp[i] = _mm_movemask_epi8(highbits);
        }

        __m128i tmpvec = _mm_loadu_si128((__m128i*)tmp);
        BitsetBlockType res[4];
        _mm_storeu_si128((__m128i*)res, tmpvec);
        return res[0];
    }
}

template <>
bool
FindTermSSE2(const bool* src, size_t vec_size, bool val) {
    __m128i xmm_target = _mm_set1_epi8(val);
    __m128i xmm_data;
    size_t num_chunks = vec_size / 16;
    for (size_t i = 0; i < num_chunks; i++) {
        xmm_data =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + 16 * i));
        __m128i xmm_match = _mm_cmpeq_epi8(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 16 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }

    return false;
}

template <>
bool
FindTermSSE2(const int8_t* src, size_t vec_size, int8_t val) {
    __m128i xmm_target = _mm_set1_epi8(val);
    __m128i xmm_data;
    size_t num_chunks = vec_size / 16;
    for (size_t i = 0; i < num_chunks; i++) {
        xmm_data =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + 16 * i));
        __m128i xmm_match = _mm_cmpeq_epi8(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 16 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }

    return false;
}

template <>
bool
FindTermSSE2(const int16_t* src, size_t vec_size, int16_t val) {
    __m128i xmm_target = _mm_set1_epi16(val);
    __m128i xmm_data;
    size_t num_chunks = vec_size / 8;
    for (size_t i = 0; i < num_chunks; i++) {
        xmm_data =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i * 8));
        __m128i xmm_match = _mm_cmpeq_epi16(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 8 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermSSE2(const int32_t* src, size_t vec_size, int32_t val) {
    size_t num_chunk = vec_size / 4;
    size_t remaining_size = vec_size % 4;

    __m128i xmm_target = _mm_set1_epi32(val);
    for (size_t i = 0; i < num_chunk; ++i) {
        __m128i xmm_data =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i * 4));
        __m128i xmm_match = _mm_cmpeq_epi32(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    const int32_t* remaining_ptr = src + num_chunk * 4;
    if (remaining_size == 0) {
        return false;
    } else if (remaining_size == 1) {
        return *remaining_ptr == val;
    } else if (remaining_size == 2) {
        __m128i xmm_data =
            _mm_set_epi32(0, 0, *(remaining_ptr + 1), *(remaining_ptr));
        __m128i xmm_match = _mm_cmpeq_epi32(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if ((mask & 0xFF) != 0) {
            return true;
        }
    } else {
        __m128i xmm_data = _mm_set_epi32(
            0, *(remaining_ptr + 2), *(remaining_ptr + 1), *(remaining_ptr));
        __m128i xmm_match = _mm_cmpeq_epi32(xmm_data, xmm_target);
        int mask = _mm_movemask_epi8(xmm_match);
        if ((mask & 0xFFF) != 0) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermSSE2(const int64_t* src, size_t vec_size, int64_t val) {
    // _mm_cmpeq_epi64 is not implement in SSE2, compare two int32 instead.
    int32_t low = static_cast<int32_t>(val);
    int32_t high = static_cast<int32_t>(val >> 32);
    size_t num_chunk = vec_size / 2;
    size_t remaining_size = vec_size % 2;

    for (int64_t i = 0; i < num_chunk; i++) {
        __m128i xmm_vec =
            _mm_load_si128(reinterpret_cast<const __m128i*>(src + i * 2));

        __m128i xmm_low = _mm_set1_epi32(low);
        __m128i xmm_high = _mm_set1_epi32(high);
        __m128i cmp_low = _mm_cmpeq_epi32(xmm_vec, xmm_low);
        __m128i cmp_high =
            _mm_cmpeq_epi32(_mm_srli_epi64(xmm_vec, 32), xmm_high);
        __m128i cmp_result = _mm_and_si128(cmp_low, cmp_high);

        int mask = _mm_movemask_epi8(cmp_result);
        if (mask != 0) {
            return true;
        }
    }

    if (remaining_size == 1) {
        if (src[2 * num_chunk] == val) {
            return true;
        }
    }
    return false;

    // for (size_t i = 0; i < vec_size; ++i) {
    //     if (src[i] == val) {
    //         return true;
    //     }
    // }
    // return false;
}

template <>
bool
FindTermSSE2(const float* src, size_t vec_size, float val) {
    size_t num_chunks = vec_size / 4;
    __m128 xmm_target = _mm_set1_ps(val);
    for (int i = 0; i < num_chunks; ++i) {
        __m128 xmm_data = _mm_loadu_ps(src + 4 * i);
        __m128 xmm_match = _mm_cmpeq_ps(xmm_data, xmm_target);
        int mask = _mm_movemask_ps(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 4 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermSSE2(const double* src, size_t vec_size, double val) {
    size_t num_chunks = vec_size / 2;
    __m128d xmm_target = _mm_set1_pd(val);
    for (int i = 0; i < num_chunks; ++i) {
        __m128d xmm_data = _mm_loadu_pd(src + 2 * i);
        __m128d xmm_match = _mm_cmpeq_pd(xmm_data, xmm_target);
        int mask = _mm_movemask_pd(xmm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 2 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

}  // namespace simd
}  // namespace milvus

#endif
