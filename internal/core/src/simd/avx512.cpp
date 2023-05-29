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

#include "avx512.h"
#include <cassert>

#if defined(__x86_64__)
#include <immintrin.h>

namespace milvus {
namespace simd {

template <>
bool
FindTermAVX512(const bool* src, size_t vec_size, bool val) {
    __m512i zmm_target = _mm512_set1_epi8(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 64;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + 64 * i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 64 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int8_t* src, size_t vec_size, int8_t val) {
    __m512i zmm_target = _mm512_set1_epi8(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 64;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + 64 * i));
        __mmask64 mask = _mm512_cmpeq_epi8_mask(zmm_data, zmm_target);
        if (mask != 0) {
            return true;
        }
    }

    for (size_t i = 64 * num_chunks; i < vec_size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

template <>
bool
FindTermAVX512(const int16_t* src, size_t vec_size, int16_t val) {
    __m512i zmm_target = _mm512_set1_epi16(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 32;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + 32 * i));
        __mmask32 mask = _mm512_cmpeq_epi16_mask(zmm_data, zmm_target);
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
FindTermAVX512(const int32_t* src, size_t vec_size, int32_t val) {
    __m512i zmm_target = _mm512_set1_epi32(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 16;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + 16 * i));
        __mmask16 mask = _mm512_cmpeq_epi32_mask(zmm_data, zmm_target);
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
FindTermAVX512(const int64_t* src, size_t vec_size, int64_t val) {
    __m512i zmm_target = _mm512_set1_epi64(val);
    __m512i zmm_data;
    size_t num_chunks = vec_size / 8;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data =
            _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src + 8 * i));
        __mmask8 mask = _mm512_cmpeq_epi64_mask(zmm_data, zmm_target);
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
FindTermAVX512(const float* src, size_t vec_size, float val) {
    __m512 zmm_target = _mm512_set1_ps(val);
    __m512 zmm_data;
    size_t num_chunks = vec_size / 16;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data = _mm512_loadu_ps(src + 16 * i);
        __mmask16 mask = _mm512_cmp_ps_mask(zmm_data, zmm_target, _CMP_EQ_OQ);
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
FindTermAVX512(const double* src, size_t vec_size, double val) {
    __m512d zmm_target = _mm512_set1_pd(val);
    __m512d zmm_data;
    size_t num_chunks = vec_size / 8;

    for (size_t i = 0; i < num_chunks; i++) {
        zmm_data = _mm512_loadu_pd(src + 8 * i);
        __mmask8 mask = _mm512_cmp_pd_mask(zmm_data, zmm_target, _CMP_EQ_OQ);
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
}  // namespace simd
}  // namespace milvus
#endif
