// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMON_DISTANCEUTILS_H_
#define _SPTAG_COMMON_DISTANCEUTILS_H_

#include <immintrin.h>
#include <functional>

#include "CommonUtils.h"

#define SSE

#ifndef _MSC_VER
#define DIFF128 diff128
#define DIFF256 diff256
#else
#define DIFF128 diff128.m128_f32
#define DIFF256 diff256.m256_f32
#endif

namespace SPTAG
{
    namespace COMMON
    {
        class DistanceUtils
        {
        public:
            static inline __m128 _mm_mul_epi8(__m128i X, __m128i Y)
            {
                __m128i zero = _mm_setzero_si128();

                __m128i sign_x = _mm_cmplt_epi8(X, zero);
                __m128i sign_y = _mm_cmplt_epi8(Y, zero);

                __m128i xlo = _mm_unpacklo_epi8(X, sign_x);
                __m128i xhi = _mm_unpackhi_epi8(X, sign_x);
                __m128i ylo = _mm_unpacklo_epi8(Y, sign_y);
                __m128i yhi = _mm_unpackhi_epi8(Y, sign_y);
                
                return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(xlo, ylo), _mm_madd_epi16(xhi, yhi)));
            }

            static inline __m128 _mm_sqdf_epi8(__m128i X, __m128i Y)
            {
                __m128i zero = _mm_setzero_si128();

                __m128i sign_x = _mm_cmplt_epi8(X, zero);
                __m128i sign_y = _mm_cmplt_epi8(Y, zero);

                __m128i xlo = _mm_unpacklo_epi8(X, sign_x);
                __m128i xhi = _mm_unpackhi_epi8(X, sign_x);
                __m128i ylo = _mm_unpacklo_epi8(Y, sign_y);
                __m128i yhi = _mm_unpackhi_epi8(Y, sign_y);
                
                __m128i dlo = _mm_sub_epi16(xlo, ylo);
                __m128i dhi = _mm_sub_epi16(xhi, yhi);

                return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(dlo, dlo), _mm_madd_epi16(dhi, dhi)));
            }

			static inline __m128 _mm_mul_epu8(__m128i X, __m128i Y)
			{
				__m128i zero = _mm_setzero_si128();

				__m128i xlo = _mm_unpacklo_epi8(X, zero);
				__m128i xhi = _mm_unpackhi_epi8(X, zero);
				__m128i ylo = _mm_unpacklo_epi8(Y, zero);
				__m128i yhi = _mm_unpackhi_epi8(Y, zero);

				return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(xlo, ylo), _mm_madd_epi16(xhi, yhi)));
			}

			static inline __m128 _mm_sqdf_epu8(__m128i X, __m128i Y)
			{
				__m128i zero = _mm_setzero_si128();

				__m128i xlo = _mm_unpacklo_epi8(X, zero);
				__m128i xhi = _mm_unpackhi_epi8(X, zero);
				__m128i ylo = _mm_unpacklo_epi8(Y, zero);
				__m128i yhi = _mm_unpackhi_epi8(Y, zero);

				__m128i dlo = _mm_sub_epi16(xlo, ylo);
				__m128i dhi = _mm_sub_epi16(xhi, yhi);

				return _mm_cvtepi32_ps(_mm_add_epi32(_mm_madd_epi16(dlo, dlo), _mm_madd_epi16(dhi, dhi)));
			}

            static inline __m128 _mm_mul_epi16(__m128i X, __m128i Y)
            {
                return _mm_cvtepi32_ps(_mm_madd_epi16(X, Y));
            }

            static inline __m128 _mm_sqdf_epi16(__m128i X, __m128i Y)
            {
                __m128i zero = _mm_setzero_si128();

                __m128i sign_x = _mm_cmplt_epi16(X, zero);
                __m128i sign_y = _mm_cmplt_epi16(Y, zero);

                __m128i xlo = _mm_unpacklo_epi16(X, sign_x);
                __m128i xhi = _mm_unpackhi_epi16(X, sign_x);
                __m128i ylo = _mm_unpacklo_epi16(Y, sign_y);
                __m128i yhi = _mm_unpackhi_epi16(Y, sign_y);

                __m128 dlo = _mm_cvtepi32_ps(_mm_sub_epi32(xlo, ylo));
                __m128 dhi = _mm_cvtepi32_ps(_mm_sub_epi32(xhi, yhi));

                return _mm_add_ps(_mm_mul_ps(dlo, dlo), _mm_mul_ps(dhi, dhi));
            }
            static inline __m128 _mm_sqdf_ps(__m128 X, __m128 Y)
            {
                __m128 d = _mm_sub_ps(X, Y);
                return _mm_mul_ps(d, d);
            }
#if defined(AVX)
            static inline __m256 _mm256_mul_epi8(__m256i X, __m256i Y)
            {
                __m256i zero = _mm256_setzero_si256();

                __m256i sign_x = _mm256_cmpgt_epi8(zero, X);
                __m256i sign_y = _mm256_cmpgt_epi8(zero, Y);

                __m256i xlo = _mm256_unpacklo_epi8(X, sign_x);
                __m256i xhi = _mm256_unpackhi_epi8(X, sign_x);
                __m256i ylo = _mm256_unpacklo_epi8(Y, sign_y);
                __m256i yhi = _mm256_unpackhi_epi8(Y, sign_y);

                return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(xlo, ylo), _mm256_madd_epi16(xhi, yhi)));
            }
            static inline __m256 _mm256_sqdf_epi8(__m256i X, __m256i Y)
            {
                __m256i zero = _mm256_setzero_si256();

                __m256i sign_x = _mm256_cmpgt_epi8(zero, X);
                __m256i sign_y = _mm256_cmpgt_epi8(zero, Y);

                __m256i xlo = _mm256_unpacklo_epi8(X, sign_x);
                __m256i xhi = _mm256_unpackhi_epi8(X, sign_x);
                __m256i ylo = _mm256_unpacklo_epi8(Y, sign_y);
                __m256i yhi = _mm256_unpackhi_epi8(Y, sign_y);

                __m256i dlo = _mm256_sub_epi16(xlo, ylo);
                __m256i dhi = _mm256_sub_epi16(xhi, yhi);

                return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(dlo, dlo), _mm256_madd_epi16(dhi, dhi)));
            }
			static inline __m256 _mm256_mul_epu8(__m256i X, __m256i Y)
			{
				__m256i zero = _mm256_setzero_si256();

				__m256i xlo = _mm256_unpacklo_epi8(X, zero);
				__m256i xhi = _mm256_unpackhi_epi8(X, zero);
				__m256i ylo = _mm256_unpacklo_epi8(Y, zero);
				__m256i yhi = _mm256_unpackhi_epi8(Y, zero);

				return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(xlo, ylo), _mm256_madd_epi16(xhi, yhi)));
			}
			static inline __m256 _mm256_sqdf_epu8(__m256i X, __m256i Y)
			{
				__m256i zero = _mm256_setzero_si256();

				__m256i xlo = _mm256_unpacklo_epi8(X, zero);
				__m256i xhi = _mm256_unpackhi_epi8(X, zero);
				__m256i ylo = _mm256_unpacklo_epi8(Y, zero);
				__m256i yhi = _mm256_unpackhi_epi8(Y, zero);

				__m256i dlo = _mm256_sub_epi16(xlo, ylo);
				__m256i dhi = _mm256_sub_epi16(xhi, yhi);

				return _mm256_cvtepi32_ps(_mm256_add_epi32(_mm256_madd_epi16(dlo, dlo), _mm256_madd_epi16(dhi, dhi)));
			}
            static inline __m256 _mm256_mul_epi16(__m256i X, __m256i Y)
            {
                return _mm256_cvtepi32_ps(_mm256_madd_epi16(X, Y));
            }
            static inline __m256 _mm256_sqdf_epi16(__m256i X, __m256i Y)
            {
                __m256i zero = _mm256_setzero_si256();

                __m256i sign_x = _mm256_cmpgt_epi16(zero, X);
                __m256i sign_y = _mm256_cmpgt_epi16(zero, Y);

                __m256i xlo = _mm256_unpacklo_epi16(X, sign_x);
                __m256i xhi = _mm256_unpackhi_epi16(X, sign_x);
                __m256i ylo = _mm256_unpacklo_epi16(Y, sign_y);
                __m256i yhi = _mm256_unpackhi_epi16(Y, sign_y);

                __m256 dlo = _mm256_cvtepi32_ps(_mm256_sub_epi32(xlo, ylo));
                __m256 dhi = _mm256_cvtepi32_ps(_mm256_sub_epi32(xhi, yhi));

                return _mm256_add_ps(_mm256_mul_ps(dlo, dlo), _mm256_mul_ps(dhi, dhi));
            }
            static inline __m256 _mm256_sqdf_ps(__m256 X, __m256 Y)
            {
                __m256 d = _mm256_sub_ps(X, Y);
                return _mm256_mul_ps(d, d);
            }
#endif
/*
            template<typename T>
            static float ComputeL2Distance(const T *pX, const T *pY, DimensionType length)
            {
                float diff = 0;
                const T* pEnd1 = pX + length;
                while (pX < pEnd1) {
                    float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                }
                return diff;
            }
*/
#define REPEAT(type, ctype, delta, load, exec, acc, result) \
            { \
                type c1 = load((ctype *)(pX)); \
                type c2 = load((ctype *)(pY)); \
                pX += delta; pY += delta; \
                result = acc(result, exec(c1, c2)); \
            } \

            static float ComputeL2Distance(const std::int8_t *pX, const std::int8_t *pY, DimensionType length)
            {
                const std::int8_t* pEnd32 = pX + ((length >> 5) << 5);
                const std::int8_t* pEnd16 = pX + ((length >> 4) << 4);
                const std::int8_t* pEnd4 = pX + ((length >> 2) << 2);
                const std::int8_t* pEnd1 = pX + length;
#if defined(SSE)
                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd32) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epi8, _mm_add_ps, diff128)
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epi8, _mm_add_ps, diff128)
                }
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epi8, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd32) {
                    REPEAT(__m256i, __m256i, 32, _mm256_loadu_si256, _mm256_sqdf_epi8, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epi8, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
#endif
                while (pX < pEnd4) {
                    float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                }
                while (pX < pEnd1) {
                    float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                }
                return diff;
            }

			static float ComputeL2Distance(const std::uint8_t *pX, const std::uint8_t *pY, DimensionType length)
			{
				const std::uint8_t* pEnd32 = pX + ((length >> 5) << 5);
				const std::uint8_t* pEnd16 = pX + ((length >> 4) << 4);
				const std::uint8_t* pEnd4 = pX + ((length >> 2) << 2);
				const std::uint8_t* pEnd1 = pX + length;
#if defined(SSE)
				__m128 diff128 = _mm_setzero_ps();
				while (pX < pEnd32) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epu8, _mm_add_ps, diff128)
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epu8, _mm_add_ps, diff128)
				}
				while (pX < pEnd16) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epu8, _mm_add_ps, diff128)
				}
				float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
				__m256 diff256 = _mm256_setzero_ps();
				while (pX < pEnd32) {
					REPEAT(__m256i, __m256i, 32, _mm256_loadu_si256, _mm256_sqdf_epu8, _mm256_add_ps, diff256)
				}
				__m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
				while (pX < pEnd16) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_sqdf_epu8, _mm_add_ps, diff128)
				}
				float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
				float diff = 0;
#endif
				while (pX < pEnd4) {
					float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
					c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
					c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
					c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
				}
				while (pX < pEnd1) {
					float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
				}
				return diff;
			}

            static float ComputeL2Distance(const std::int16_t *pX, const std::int16_t *pY, DimensionType length)
            {
                const std::int16_t* pEnd16 = pX + ((length >> 4) << 4);
                const std::int16_t* pEnd8 = pX + ((length >> 3) << 3);
                const std::int16_t* pEnd4 = pX + ((length >> 2) << 2);
                const std::int16_t* pEnd1 = pX + length;
#if defined(SSE)
                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_sqdf_epi16, _mm_add_ps, diff128)
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_sqdf_epi16, _mm_add_ps, diff128)
                }
                while (pX < pEnd8) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_sqdf_epi16, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd16) {
                    REPEAT(__m256i, __m256i, 16, _mm256_loadu_si256, _mm256_sqdf_epi16, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd8) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_sqdf_epi16, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
#endif
                while (pX < pEnd4) {
                    float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                    c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                }

                while (pX < pEnd1) {
                    float c1 = ((float)(*pX++) - (float)(*pY++)); diff += c1 * c1;
                }
                return diff;
            }

            static float ComputeL2Distance(const float *pX, const float *pY, DimensionType length)
            {
                const float* pEnd16 = pX + ((length >> 4) << 4);
                const float* pEnd4 = pX + ((length >> 2) << 2);
                const float* pEnd1 = pX + length;
#if defined(SSE)
                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd16)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                }
                while (pX < pEnd4)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd16)
                {
                    REPEAT(__m256, const float, 8, _mm256_loadu_ps, _mm256_sqdf_ps, _mm256_add_ps, diff256)
                    REPEAT(__m256, const float, 8, _mm256_loadu_ps, _mm256_sqdf_ps, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd4)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_sqdf_ps, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
                while (pX < pEnd4) {
                    float c1 = (*pX++) - (*pY++); diff += c1 * c1;
                    c1 = (*pX++) - (*pY++); diff += c1 * c1;
                    c1 = (*pX++) - (*pY++); diff += c1 * c1;
                    c1 = (*pX++) - (*pY++); diff += c1 * c1;
                }
#endif
                while (pX < pEnd1) {
                    float c1 = (*pX++) - (*pY++); diff += c1 * c1;
                }
                return diff;
            }
/*
            template<typename T>
            static float ComputeCosineDistance(const T *pX, const T *pY, DimensionType length) {
                float diff = 0;
                const T* pEnd1 = pX + length;
                while (pX < pEnd1) diff += (*pX++) * (*pY++);
                return 1 - diff;
            }
*/
            static float ComputeCosineDistance(const std::int8_t *pX, const std::int8_t *pY, DimensionType length) {
                const std::int8_t* pEnd32 = pX + ((length >> 5) << 5);
                const std::int8_t* pEnd16 = pX + ((length >> 4) << 4);
                const std::int8_t* pEnd4 = pX + ((length >> 2) << 2);
                const std::int8_t* pEnd1 = pX + length;
#if defined(SSE)

                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd32) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epi8, _mm_add_ps, diff128)
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epi8, _mm_add_ps, diff128)
                }
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epi8, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd32) {
                    REPEAT(__m256i, __m256i, 32, _mm256_loadu_si256, _mm256_mul_epi8, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epi8, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
#endif
                while (pX < pEnd4)
                {
                    float c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                }
                while (pX < pEnd1) diff += ((float)(*pX++) * (float)(*pY++));
                return 16129 - diff;
            }

			static float ComputeCosineDistance(const std::uint8_t *pX, const std::uint8_t *pY, DimensionType length) {
				const std::uint8_t* pEnd32 = pX + ((length >> 5) << 5);
				const std::uint8_t* pEnd16 = pX + ((length >> 4) << 4);
				const std::uint8_t* pEnd4 = pX + ((length >> 2) << 2);
				const std::uint8_t* pEnd1 = pX + length;
#if defined(SSE)

				__m128 diff128 = _mm_setzero_ps();
				while (pX < pEnd32) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epu8, _mm_add_ps, diff128)
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epu8, _mm_add_ps, diff128)
				}
				while (pX < pEnd16) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epu8, _mm_add_ps, diff128)
				}
				float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#elif defined(AVX)
				__m256 diff256 = _mm256_setzero_ps();
				while (pX < pEnd32) {
					REPEAT(__m256i, __m256i, 32, _mm256_loadu_si256, _mm256_mul_epu8, _mm256_add_ps, diff256)
				}
				__m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
				while (pX < pEnd16) {
					REPEAT(__m128i, __m128i, 16, _mm_loadu_si128, _mm_mul_epu8, _mm_add_ps, diff128)
				}
				float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
				float diff = 0;
#endif
				while (pX < pEnd4)
				{
					float c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
					c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
					c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
					c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
				}
				while (pX < pEnd1) diff += ((float)(*pX++) * (float)(*pY++));
				return 65025 - diff;
			}

	static float ComputeCosineDistance(const std::int16_t *pX, const std::int16_t *pY, DimensionType length) {
                const std::int16_t* pEnd16 = pX + ((length >> 4) << 4);
                const std::int16_t* pEnd8 = pX + ((length >> 3) << 3);
                const std::int16_t* pEnd4 = pX + ((length >> 2) << 2);
                const std::int16_t* pEnd1 = pX + length;
#if defined(SSE)
                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd16) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_mul_epi16, _mm_add_ps, diff128)
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_mul_epi16, _mm_add_ps, diff128)
                }
                while (pX < pEnd8) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_mul_epi16, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];

#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd16) {
                    REPEAT(__m256i, __m256i, 16, _mm256_loadu_si256, _mm256_mul_epi16, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd8) {
                    REPEAT(__m128i, __m128i, 8, _mm_loadu_si128, _mm_mul_epi16, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
#endif
                while (pX < pEnd4)
                {
                    float c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                    c1 = ((float)(*pX++) * (float)(*pY++)); diff += c1;
                }

                while (pX < pEnd1) diff += ((float)(*pX++) * (float)(*pY++));
                return  1073676289 - diff;
            }

            static float ComputeCosineDistance(const float *pX, const float *pY, DimensionType length) {
                const float* pEnd16 = pX + ((length >> 4) << 4);
                const float* pEnd4 = pX + ((length >> 2) << 2);
                const float* pEnd1 = pX + length;
#if defined(SSE)
                __m128 diff128 = _mm_setzero_ps();
                while (pX < pEnd16)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                }
                while (pX < pEnd4)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];

#elif defined(AVX)
                __m256 diff256 = _mm256_setzero_ps();
                while (pX < pEnd16)
                {
                    REPEAT(__m256, const float, 8, _mm256_loadu_ps, _mm256_mul_ps, _mm256_add_ps, diff256)
                    REPEAT(__m256, const float, 8, _mm256_loadu_ps, _mm256_mul_ps, _mm256_add_ps, diff256)
                }
                __m128 diff128 = _mm_add_ps(_mm256_castps256_ps128(diff256), _mm256_extractf128_ps(diff256, 1));
                while (pX < pEnd4)
                {
                    REPEAT(__m128, const float, 4, _mm_loadu_ps, _mm_mul_ps, _mm_add_ps, diff128)
                }
                float diff = DIFF128[0] + DIFF128[1] + DIFF128[2] + DIFF128[3];
#else
                float diff = 0;
                while (pX < pEnd4)
                {
                    float c1 = (*pX++) * (*pY++); diff += c1;
                    c1 = (*pX++) * (*pY++); diff += c1;
                    c1 = (*pX++) * (*pY++); diff += c1;
                    c1 = (*pX++) * (*pY++); diff += c1;
                }
#endif
                while (pX < pEnd1) diff += (*pX++) * (*pY++);
                return 1 - diff;
            }

            template<typename T>
            static inline float ComputeDistance(const T *p1, const T *p2, DimensionType length, SPTAG::DistCalcMethod distCalcMethod)
            {
                if (distCalcMethod == SPTAG::DistCalcMethod::L2)
                    return ComputeL2Distance(p1, p2, length);

                return ComputeCosineDistance(p1, p2, length);
            }

            static inline float ConvertCosineSimilarityToDistance(float cs)
            {
                // Cosine similarity is in [-1, 1], the higher the value, the closer are the two vectors. 
                // However, the tree is built and searched based on "distance" between two vectors, that's >=0. The smaller the value, the closer are the two vectors.
                // So we do a linear conversion from a cosine similarity to a distance value.
                return 1 - cs; //[1, 3]
            }

            static inline float ConvertDistanceBackToCosineSimilarity(float d)
            {
                return 1 - d;
            }
        };


        template<typename T>
        float (*DistanceCalcSelector(SPTAG::DistCalcMethod p_method)) (const T*, const T*, DimensionType)
        {
            switch (p_method)
            {
            case SPTAG::DistCalcMethod::Cosine:
                return &(DistanceUtils::ComputeCosineDistance);

            case SPTAG::DistCalcMethod::L2:
                return &(DistanceUtils::ComputeL2Distance);

            default:
                break;
            }

            return nullptr;
        }
    }
}

#endif // _SPTAG_COMMON_DISTANCEUTILS_H_
