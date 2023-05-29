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

#include "avx2.h"
#include "sse2.h"
#include "sse4.h"

#include <immintrin.h>

#include <cassert>
#include <iostream>

namespace milvus {
namespace simd {

BitsetBlockType
GetBitsetBlockAVX2(const bool* src) {
    if constexpr (BITSET_BLOCK_SIZE == 8) {
        // BitsetBlockType has 64 bits
        __m256i highbit = _mm256_set1_epi8(0x7F);
        uint32_t tmp[8];
        for (size_t i = 0; i < 2; i += 1) {
            __m256i boolvec = _mm256_loadu_si256((__m256i*)&src[i * 32]);
            __m256i highbits = _mm256_add_epi8(boolvec, highbit);
            tmp[i] = _mm256_movemask_epi8(highbits);
        }

        __m256i tmpvec = _mm256_loadu_si256((__m256i*)tmp);
        BitsetBlockType res[4];
        _mm256_storeu_si256((__m256i*)res, tmpvec);
        return res[0];
        // __m128i tmpvec = _mm_loadu_si64(tmp);
        // BitsetBlockType res;
        // _mm_storeu_si64(&res, tmpvec);
        // return res;
    } else {
        // Others has 32 bits
        __m256i highbit = _mm256_set1_epi8(0x7F);
        uint32_t tmp[8];
        __m256i boolvec = _mm256_loadu_si256((__m256i*)&src[0]);
        __m256i highbits = _mm256_add_epi8(boolvec, highbit);
        tmp[0] = _mm256_movemask_epi8(highbits);

        __m256i tmpvec = _mm256_loadu_si256((__m256i*)tmp);
        BitsetBlockType res[8];
        _mm256_storeu_si256((__m256i*)res, tmpvec);
        return res[0];
    }
}

template <>
bool
FindTermAVX2(const bool* src, size_t vec_size, bool val) {
    __m256i ymm_target = _mm256_set1_epi8(val);
    __m256i ymm_data;
    size_t num_chunks = vec_size / 32;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32 * i));
        __m256i ymm_match = _mm256_cmpeq_epi8(ymm_data, ymm_target);
        int mask = _mm256_movemask_epi8(ymm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 32 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX2(const int8_t* src, size_t vec_size, int8_t val) {
    __m256i ymm_target = _mm256_set1_epi8(val);
    __m256i ymm_data;
    size_t num_chunks = vec_size / 32;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32 * i));
        __m256i ymm_match = _mm256_cmpeq_epi8(ymm_data, ymm_target);
        int mask = _mm256_movemask_epi8(ymm_match);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 32 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX2(const int16_t* src, size_t vec_size, int16_t val) {
    __m256i ymm_target = _mm256_set1_epi16(val);
    __m256i ymm_data;
    size_t num_chunks = vec_size / 16;
    size_t remaining_size = vec_size % 16;
    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 16 * i));
        __m256i ymm_match = _mm256_cmpeq_epi16(ymm_data, ymm_target);
        int mask = _mm256_movemask_epi8(ymm_match);
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
FindTermAVX2(const int32_t* src, size_t vec_size, int32_t val) {
    __m256i ymm_target = _mm256_set1_epi32(val);
    __m256i ymm_data;
    size_t num_chunks = vec_size / 8;
    size_t remaining_size = vec_size % 8;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 8 * i));
        __m256i ymm_match = _mm256_cmpeq_epi32(ymm_data, ymm_target);
        int mask = _mm256_movemask_epi8(ymm_match);
        if (mask != 0) {
            return true;
        }
    }

    if (remaining_size == 0) {
        return false;
    }
    return FindTermSSE2(src + 8 * num_chunks, remaining_size, val);
}

template <>
bool
FindTermAVX2(const int64_t* src, size_t vec_size, int64_t val) {
    __m256i ymm_target = _mm256_set1_epi64x(val);
    __m256i ymm_data;
    size_t num_chunks = vec_size / 4;
    size_t remaining_size = vec_size % 4;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data =
            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 4 * i));
        __m256i ymm_match = _mm256_cmpeq_epi64(ymm_data, ymm_target);
        int mask = _mm256_movemask_epi8(ymm_match);
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
FindTermAVX2(const float* src, size_t vec_size, float val) {
    __m256 ymm_target = _mm256_set1_ps(val);
    __m256 ymm_data;
    size_t num_chunks = vec_size / 8;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data = _mm256_loadu_ps(src + 8 * i);
        __m256 ymm_match = _mm256_cmp_ps(ymm_data, ymm_target, _CMP_EQ_OQ);
        int mask = _mm256_movemask_ps(ymm_match);
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
FindTermAVX2(const double* src, size_t vec_size, double val) {
    __m256d ymm_target = _mm256_set1_pd(val);
    __m256d ymm_data;
    size_t num_chunks = vec_size / 4;

    for (size_t i = 0; i < num_chunks; i++) {
        ymm_data = _mm256_loadu_pd(src + 8 * i);
        __m256d ymm_match = _mm256_cmp_pd(ymm_data, ymm_target, _CMP_EQ_OQ);
        int mask = _mm256_movemask_pd(ymm_match);
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

}  // namespace simd
}  // namespace milvus

#endif
