//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include "NGT/defines.h"

#if defined(NGT_NO_AVX)
// #warning "*** SIMD is *NOT* available! ***"
#else
#include <immintrin.h>
#endif

namespace NGT {

class MemoryCache {
 public:
    inline static void
    prefetch(unsigned char* ptr, const size_t byteSizeOfObject) {
#if !defined(NGT_NO_AVX)
        switch ((byteSizeOfObject - 1) >> 6) {
            default:
            case 28:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 27:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 26:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 25:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 24:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 23:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 22:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 21:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 20:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 19:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 18:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 17:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 16:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 15:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 14:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 13:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 12:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 11:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 10:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 9:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 8:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 7:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 6:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 5:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 4:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 3:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 2:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 1:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
            case 0:
                _mm_prefetch(ptr, _MM_HINT_T0);
                ptr += 64;
                break;
        }
#endif
    }
    inline static void*
    alignedAlloc(const size_t allocSize) {
#ifdef NGT_NO_AVX
        return new uint8_t[allocSize];
#else
#if defined(NGT_AVX512)
        size_t alignment = 64;
        uint64_t mask = 0xFFFFFFFFFFFFFFC0;
#elif defined(NGT_AVX2)
        size_t alignment = 32;
        uint64_t mask = 0xFFFFFFFFFFFFFFE0;
#else
        size_t alignment = 16;
        uint64_t mask = 0xFFFFFFFFFFFFFFF0;
#endif
        uint8_t* p = new uint8_t[allocSize + alignment];
        uint8_t* ptr = p + alignment;
        ptr = reinterpret_cast<uint8_t*>((reinterpret_cast<uint64_t>(ptr) & mask));
        *p++ = 0xAB;
        while (p != ptr) *p++ = 0xCD;
        return ptr;
#endif
    }
    inline static void
    alignedFree(void* ptr) {
#ifdef NGT_NO_AVX
        delete[] static_cast<uint8_t*>(ptr);
#else
        uint8_t* p = static_cast<uint8_t*>(ptr);
        p--;
        while (*p == 0xCD) p--;
        if (*p != 0xAB) {
            NGTThrowException("MemoryCache::alignedFree: Fatal Error! Cannot find allocated address.");
        }
        delete[] p;
#endif
    }
};

class PrimitiveComparator {
 public:
    static double
    absolute(double v) {
        return fabs(v);
    }
    static int
    absolute(int v) {
        return abs(v);
    }

#if defined(NGT_NO_AVX)
    template <typename OBJECT_TYPE, typename COMPARE_TYPE>
    inline static double
    compareL2(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const OBJECT_TYPE* last = a + size;
        const OBJECT_TYPE* lastgroup = last - 3;
        COMPARE_TYPE diff0, diff1, diff2, diff3;
        double d = 0.0;
        while (a < lastgroup) {
            diff0 = (COMPARE_TYPE)(a[0] - b[0]);
            diff1 = (COMPARE_TYPE)(a[1] - b[1]);
            diff2 = (COMPARE_TYPE)(a[2] - b[2]);
            diff3 = (COMPARE_TYPE)(a[3] - b[3]);
            d += diff0 * diff0 + diff1 * diff1 + diff2 * diff2 + diff3 * diff3;
            a += 4;
            b += 4;
        }
        while (a < last) {
            diff0 = (COMPARE_TYPE)(*a++ - *b++);
            d += diff0 * diff0;
        }
//        return sqrt((double)d);
        return d;
    }

    inline static double
    compareL2(const uint8_t* a, const uint8_t* b, size_t size) {
        return compareL2<uint8_t, int>(a, b, size);
    }

    inline static double
    compareL2(const float* a, const float* b, size_t size) {
        return compareL2<float, double>(a, b, size);
    }

#else
    inline static double
    compareL2(const float* a, const float* b, size_t size) {
        const float* last = a + size;
#if defined(NGT_AVX512)
        __m512 sum512 = _mm512_setzero_ps();
        while (a < last) {
            __m512 v = _mm512_sub_ps(_mm512_loadu_ps(a), _mm512_loadu_ps(b));
            sum512 = _mm512_add_ps(sum512, _mm512_mul_ps(v, v));
            a += 16;
            b += 16;
        }

        __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
        __m256 sum256 = _mm256_setzero_ps();
        __m256 v;
        while (a < last) {
            v = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
            sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v, v));
            a += 8;
            b += 8;
            v = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
            sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(v, v));
            a += 8;
            b += 8;
        }
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
        __m128 sum128 = _mm_setzero_ps();
        __m128 v;
        while (a < last) {
            v = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
            a += 4;
            b += 4;
            v = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
            a += 4;
            b += 4;
            v = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
            a += 4;
            b += 4;
            v = _mm_sub_ps(_mm_loadu_ps(a), _mm_loadu_ps(b));
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(v, v));
            a += 4;
            b += 4;
        }
#endif

        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, sum128);

        double s = f[0] + f[1] + f[2] + f[3];
//        return sqrt(s);
        return s;
    }

    inline static double
    compareL2(const unsigned char* a, const unsigned char* b, size_t size) {
        __m128 sum = _mm_setzero_ps();
        const unsigned char* last = a + size;
        const unsigned char* lastgroup = last - 7;
        const __m128i zero = _mm_setzero_si128();
        while (a < lastgroup) {
            __m128i x1 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const*)a));
            __m128i x2 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const*)b));
            x1 = _mm_subs_epi16(x1, x2);
            __m128i v = _mm_mullo_epi16(x1, x1);
            sum = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpacklo_epi16(v, zero)));
            sum = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpackhi_epi16(v, zero)));
            a += 8;
            b += 8;
        }
        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, sum);
        double s = f[0] + f[1] + f[2] + f[3];
        while (a < last) {
            int d = (int)*a++ - (int)*b++;
            s += d * d;
        }
//        return sqrt(s);
        return s;
    }
#endif
#if defined(NGT_NO_AVX)
    template <typename OBJECT_TYPE, typename COMPARE_TYPE>
    static double
    compareL1(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const OBJECT_TYPE* last = a + size;
        const OBJECT_TYPE* lastgroup = last - 3;
        COMPARE_TYPE diff0, diff1, diff2, diff3;
        double d = 0.0;
        while (a < lastgroup) {
            diff0 = (COMPARE_TYPE)(a[0] - b[0]);
            diff1 = (COMPARE_TYPE)(a[1] - b[1]);
            diff2 = (COMPARE_TYPE)(a[2] - b[2]);
            diff3 = (COMPARE_TYPE)(a[3] - b[3]);
            d += absolute(diff0) + absolute(diff1) + absolute(diff2) + absolute(diff3);
            a += 4;
            b += 4;
        }
        while (a < last) {
            diff0 = (COMPARE_TYPE)*a++ - (COMPARE_TYPE)*b++;
            d += absolute(diff0);
        }
        return d;
    }

    inline static double
    compareL1(const uint8_t* a, const uint8_t* b, size_t size) {
        return compareL1<uint8_t, int>(a, b, size);
    }

    inline static double
    compareL1(const float* a, const float* b, size_t size) {
        return compareL1<float, double>(a, b, size);
    }

#else
    inline static double
    compareL1(const float* a, const float* b, size_t size) {
        __m256 sum = _mm256_setzero_ps();
        const float* last = a + size;
        const float* lastgroup = last - 7;
        while (a < lastgroup) {
            __m256 x1 = _mm256_sub_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b));
            const __m256 mask = _mm256_set1_ps(-0.0f);
            __m256 v = _mm256_andnot_ps(mask, x1);
            sum = _mm256_add_ps(sum, v);
            a += 8;
            b += 8;
        }
        __attribute__((aligned(32))) float f[8];
        _mm256_store_ps(f, sum);
        double s = f[0] + f[1] + f[2] + f[3] + f[4] + f[5] + f[6] + f[7];
        while (a < last) {
            double d = fabs(*a++ - *b++);
            s += d;
        }
        return s;
    }
    inline static double
    compareL1(const unsigned char* a, const unsigned char* b, size_t size) {
        __m128 sum = _mm_setzero_ps();
        const unsigned char* last = a + size;
        const unsigned char* lastgroup = last - 7;
        const __m128i zero = _mm_setzero_si128();
        while (a < lastgroup) {
            __m128i x1 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const*)a));
            __m128i x2 = _mm_cvtepu8_epi16(_mm_loadu_si128((__m128i const*)b));
            x1 = _mm_subs_epi16(x1, x2);
            x1 = _mm_sign_epi16(x1, x1);
            sum = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpacklo_epi16(x1, zero)));
            sum = _mm_add_ps(sum, _mm_cvtepi32_ps(_mm_unpackhi_epi16(x1, zero)));
            a += 8;
            b += 8;
        }
        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, sum);
        double s = f[0] + f[1] + f[2] + f[3];
        while (a < last) {
            double d = fabs((double)*a++ - (double)*b++);
            s += d;
        }
        return s;
    }
#endif

#if defined(NGT_NO_AVX) || !defined(__POPCNT__)
    inline static double
    popCount(uint32_t x) {
        x = (x & 0x55555555) + (x >> 1 & 0x55555555);
        x = (x & 0x33333333) + (x >> 2 & 0x33333333);
        x = (x & 0x0F0F0F0F) + (x >> 4 & 0x0F0F0F0F);
        x = (x & 0x00FF00FF) + (x >> 8 & 0x00FF00FF);
        x = (x & 0x0000FFFF) + (x >> 16 & 0x0000FFFF);
        return x;
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareHammingDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const uint32_t* last = reinterpret_cast<const uint32_t*>(a + size);

        const uint32_t* uinta = reinterpret_cast<const uint32_t*>(a);
        const uint32_t* uintb = reinterpret_cast<const uint32_t*>(b);
        size_t count = 0;
        while (uinta < last) {
            count += popCount(*uinta++ ^ *uintb++);
        }

        return static_cast<double>(count);
    }
#else
    template <typename OBJECT_TYPE>
    inline static double
    compareHammingDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const uint64_t* last = reinterpret_cast<const uint64_t*>(a + size);

        const uint64_t* uinta = reinterpret_cast<const uint64_t*>(a);
        const uint64_t* uintb = reinterpret_cast<const uint64_t*>(b);
        size_t count = 0;
        while (uinta < last) {
            count += _mm_popcnt_u64(*uinta++ ^ *uintb++);
            count += _mm_popcnt_u64(*uinta++ ^ *uintb++);
        }

        return static_cast<double>(count);
    }
#endif

#if defined(NGT_NO_AVX) || !defined(__POPCNT__)
    template <typename OBJECT_TYPE>
    inline static double
    compareJaccardDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const uint32_t* last = reinterpret_cast<const uint32_t*>(a + size);

        const uint32_t* uinta = reinterpret_cast<const uint32_t*>(a);
        const uint32_t* uintb = reinterpret_cast<const uint32_t*>(b);
        size_t count = 0;
        size_t countDe = 0;
        while (uinta < last) {
            count += popCount(*uinta & *uintb);
            countDe += popCount(*uinta++ | *uintb++);
            count += popCount(*uinta & *uintb);
            countDe += popCount(*uinta++ | *uintb++);
        }

        return 1.0 - static_cast<double>(count) / static_cast<double>(countDe);
    }
#else
    template <typename OBJECT_TYPE>
    inline static double
    compareJaccardDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        const uint64_t* last = reinterpret_cast<const uint64_t*>(a + size);

        const uint64_t* uinta = reinterpret_cast<const uint64_t*>(a);
        const uint64_t* uintb = reinterpret_cast<const uint64_t*>(b);
        size_t count = 0;
        size_t countDe = 0;
        while (uinta < last) {
            count += _mm_popcnt_u64(*uinta & *uintb);
            countDe += _mm_popcnt_u64(*uinta++ | *uintb++);
            count += _mm_popcnt_u64(*uinta & *uintb);
            countDe += _mm_popcnt_u64(*uinta++ | *uintb++);
        }

        return 1.0 - static_cast<double>(count) / static_cast<double>(countDe);
    }
#endif

    inline static double
    compareSparseJaccardDistance(const unsigned char* a, unsigned char* b, size_t size) {
        abort();
    }

    inline static double
    compareSparseJaccardDistance(const float* a, const float* b, size_t size) {
        size_t loca = 0;
        size_t locb = 0;
        const uint32_t* ai = reinterpret_cast<const uint32_t*>(a);
        const uint32_t* bi = reinterpret_cast<const uint32_t*>(b);
        size_t count = 0;
        while (locb < size && ai[loca] != 0 && bi[loca] != 0) {
            int64_t sub = static_cast<int64_t>(ai[loca]) - static_cast<int64_t>(bi[locb]);
            count += sub == 0;
            loca += sub <= 0;
            locb += sub >= 0;
        }
        while (ai[loca] != 0) {
            loca++;
        }
        while (locb < size && bi[locb] != 0) {
            locb++;
        }
        return 1.0 - static_cast<double>(count) / static_cast<double>(loca + locb - count);
    }

#if defined(NGT_NO_AVX)
    template <typename OBJECT_TYPE>
    inline static double
    compareDotProduct(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            sum += (double)a[loc] * (double)b[loc];
        }
        return sum;
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareInnerProduct(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            sum += (double)a[loc] * (double)b[loc];
//            sum += a[loc] * b[loc];
        }
        return -sum;
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareCosine(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double normA = 0.0;
        double normB = 0.0;
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            normA += (double)a[loc] * (double)a[loc];
            normB += (double)b[loc] * (double)b[loc];
            sum += (double)a[loc] * (double)b[loc];
        }

        double cosine = sum / sqrt(normA * normB);

        return cosine;
    }
#else
    inline static double
    compareDotProduct(const float* a, const float* b, size_t size) {
        const float* last = a + size;
#if defined(NGT_AVX512)
        __m512 sum512 = _mm512_setzero_ps();
        while (a < last) {
            sum512 = _mm512_add_ps(sum512, _mm512_mul_ps(_mm512_loadu_ps(a), _mm512_loadu_ps(b)));
            a += 16;
            b += 16;
        }

        __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
        __m256 sum256 = _mm256_setzero_ps();
        while (a < last) {
            sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b)));
            a += 8;
            b += 8;
        }
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
        __m128 sum128 = _mm_setzero_ps();
        while (a < last) {
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(_mm_loadu_ps(a), _mm_loadu_ps(b)));
            a += 4;
            b += 4;
        }
#endif
        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, sum128);

        double s = f[0] + f[1] + f[2] + f[3];
        return s;
    }

    inline static double
    compareInnerProduct(const float* a, const float* b, size_t size) {
        const float* last = a + size;
#if defined(NGT_AVX512)
        __m512 sum512 = _mm512_setzero_ps();
        while (a < last) {
            sum512 = _mm512_add_ps(sum512, _mm512_mul_ps(_mm512_loadu_ps(a), _mm512_loadu_ps(b)));
            a += 16;
            b += 16;
        }

        __m256 sum256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum512, 0), _mm512_extractf32x8_ps(sum512, 1));
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#elif defined(NGT_AVX2)
        __m256 sum256 = _mm256_setzero_ps();
        while (a < last) {
            sum256 = _mm256_add_ps(sum256, _mm256_mul_ps(_mm256_loadu_ps(a), _mm256_loadu_ps(b)));
            a += 8;
            b += 8;
        }
        __m128 sum128 = _mm_add_ps(_mm256_extractf128_ps(sum256, 0), _mm256_extractf128_ps(sum256, 1));
#else
        __m128 sum128 = _mm_setzero_ps();
        while (a < last) {
            sum128 = _mm_add_ps(sum128, _mm_mul_ps(_mm_loadu_ps(a), _mm_loadu_ps(b)));
            a += 4;
            b += 4;
        }
#endif
        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, sum128);

        double s = f[0] + f[1] + f[2] + f[3];
        return -s;
    }

    inline static double
    compareDotProduct(const unsigned char* a, const unsigned char* b, size_t size) {
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            sum += (double)a[loc] * (double)b[loc];
        }
        return sum;
    }

    inline static double
    compareInnerProduct(const unsigned char* a, const unsigned char* b, size_t size) {
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            sum += (double)a[loc] * (double)b[loc];
        }
        return -sum;
    }

    inline static double
    compareCosine(const float* a, const float* b, size_t size) {
        const float* last = a + size;
#if defined(NGT_AVX512)
        __m512 normA = _mm512_setzero_ps();
        __m512 normB = _mm512_setzero_ps();
        __m512 sum = _mm512_setzero_ps();
        while (a < last) {
            __m512 am = _mm512_loadu_ps(a);
            __m512 bm = _mm512_loadu_ps(b);
            normA = _mm512_add_ps(normA, _mm512_mul_ps(am, am));
            normB = _mm512_add_ps(normB, _mm512_mul_ps(bm, bm));
            sum = _mm512_add_ps(sum, _mm512_mul_ps(am, bm));
            a += 16;
            b += 16;
        }
        __m256 am256 = _mm256_add_ps(_mm512_extractf32x8_ps(normA, 0), _mm512_extractf32x8_ps(normA, 1));
        __m256 bm256 = _mm256_add_ps(_mm512_extractf32x8_ps(normB, 0), _mm512_extractf32x8_ps(normB, 1));
        __m256 s256 = _mm256_add_ps(_mm512_extractf32x8_ps(sum, 0), _mm512_extractf32x8_ps(sum, 1));
        __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(am256, 0), _mm256_extractf128_ps(am256, 1));
        __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(bm256, 0), _mm256_extractf128_ps(bm256, 1));
        __m128 s128 = _mm_add_ps(_mm256_extractf128_ps(s256, 0), _mm256_extractf128_ps(s256, 1));
#elif defined(NGT_AVX2)
        __m256 normA = _mm256_setzero_ps();
        __m256 normB = _mm256_setzero_ps();
        __m256 sum = _mm256_setzero_ps();
        __m256 am, bm;
        while (a < last) {
            am = _mm256_loadu_ps(a);
            bm = _mm256_loadu_ps(b);
            normA = _mm256_add_ps(normA, _mm256_mul_ps(am, am));
            normB = _mm256_add_ps(normB, _mm256_mul_ps(bm, bm));
            sum = _mm256_add_ps(sum, _mm256_mul_ps(am, bm));
            a += 8;
            b += 8;
        }
        __m128 am128 = _mm_add_ps(_mm256_extractf128_ps(normA, 0), _mm256_extractf128_ps(normA, 1));
        __m128 bm128 = _mm_add_ps(_mm256_extractf128_ps(normB, 0), _mm256_extractf128_ps(normB, 1));
        __m128 s128 = _mm_add_ps(_mm256_extractf128_ps(sum, 0), _mm256_extractf128_ps(sum, 1));
#else
        __m128 am128 = _mm_setzero_ps();
        __m128 bm128 = _mm_setzero_ps();
        __m128 s128 = _mm_setzero_ps();
        __m128 am, bm;
        while (a < last) {
            am = _mm_loadu_ps(a);
            bm = _mm_loadu_ps(b);
            am128 = _mm_add_ps(am128, _mm_mul_ps(am, am));
            bm128 = _mm_add_ps(bm128, _mm_mul_ps(bm, bm));
            s128 = _mm_add_ps(s128, _mm_mul_ps(am, bm));
            a += 4;
            b += 4;
        }

#endif

        __attribute__((aligned(32))) float f[4];
        _mm_store_ps(f, am128);
        double na = f[0] + f[1] + f[2] + f[3];
        _mm_store_ps(f, bm128);
        double nb = f[0] + f[1] + f[2] + f[3];
        _mm_store_ps(f, s128);
        double s = f[0] + f[1] + f[2] + f[3];

        double cosine = s / sqrt(na * nb);
        return cosine;
    }

    inline static double
    compareCosine(const unsigned char* a, const unsigned char* b, size_t size) {
        double normA = 0.0;
        double normB = 0.0;
        double sum = 0.0;
        for (size_t loc = 0; loc < size; loc++) {
            normA += (double)a[loc] * (double)a[loc];
            normB += (double)b[loc] * (double)b[loc];
            sum += (double)a[loc] * (double)b[loc];
        }

        double cosine = sum / sqrt(normA * normB);

        return cosine;
    }
#endif  // #if defined(NGT_NO_AVX)

    template <typename OBJECT_TYPE>
    inline static double
    compareAngleDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double cosine = compareCosine(a, b, size);
        if (cosine >= 1.0) {
            return 0.0;
        } else if (cosine <= -1.0) {
            return acos(-1.0);
        } else {
            return acos(cosine);
        }
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareNormalizedAngleDistance(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double cosine = compareDotProduct(a, b, size);
        if (cosine >= 1.0) {
            return 0.0;
        } else if (cosine <= -1.0) {
            return acos(-1.0);
        } else {
            return acos(cosine);
        }
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareCosineSimilarity(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        return 1.0 - compareCosine(a, b, size);
    }

    template <typename OBJECT_TYPE>
    inline static double
    compareNormalizedCosineSimilarity(const OBJECT_TYPE* a, const OBJECT_TYPE* b, size_t size) {
        double v = 1.0 - compareDotProduct(a, b, size);
        return v < 0.0 ? 0.0 : v;
    }

    class L1Uint8 {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareL1((const uint8_t*)a, (const uint8_t*)b, size);
        }
    };

    class L2Uint8 {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareL2((const uint8_t*)a, (const uint8_t*)b, size);
        }
    };

    class HammingUint8 {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareHammingDistance((const uint8_t*)a, (const uint8_t*)b, size);
        }
    };

    class JaccardUint8 {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareJaccardDistance((const uint8_t*)a, (const uint8_t*)b, size);
        }
    };

    class SparseJaccardFloat {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareSparseJaccardDistance((const float*)a, (const float*)b, size);
        }
    };

    class L2Float {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
#if defined(NGT_NO_AVX)
            return PrimitiveComparator::compareL2<float, double>((const float*)a, (const float*)b, size);
#else
            return PrimitiveComparator::compareL2((const float*)a, (const float*)b, size);
#endif
        }
    };

    class L1Float {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareL1((const float*)a, (const float*)b, size);
        }
    };

    class CosineSimilarityFloat {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareCosineSimilarity((const float*)a, (const float*)b, size);
        }
    };

    class AngleFloat {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareAngleDistance((const float*)a, (const float*)b, size);
        }
    };

    class NormalizedCosineSimilarityFloat {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareNormalizedCosineSimilarity((const float*)a, (const float*)b, size);
        }
    };

    class NormalizedAngleFloat {
     public:
        inline static double
        compare(const void* a, const void* b, size_t size) {
            return PrimitiveComparator::compareNormalizedAngleDistance((const float*)a, (const float*)b, size);
        }
    };
};

}  // namespace NGT

