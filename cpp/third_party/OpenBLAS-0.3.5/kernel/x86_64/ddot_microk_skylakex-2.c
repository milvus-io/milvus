/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/* need a new enough GCC for avx512 support */
#if (( defined(__GNUC__)  && __GNUC__   > 6 && defined(__AVX2__)) || (defined(__clang__) && __clang_major__ >= 6))

#define HAVE_KERNEL_8 1

#include <immintrin.h>

static void ddot_kernel_8( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *dot)
{
	int i = 0;
	__m256d accum_0, accum_1, accum_2, accum_3;
	
	accum_0 = _mm256_setzero_pd();
	accum_1 = _mm256_setzero_pd();
	accum_2 = _mm256_setzero_pd();
	accum_3 = _mm256_setzero_pd();

#ifdef __AVX512CD__
	__m512d accum_05, accum_15, accum_25, accum_35;
	int n32;
	n32 = n & (~31);

	accum_05 = _mm512_setzero_pd();
	accum_15 = _mm512_setzero_pd();
	accum_25 = _mm512_setzero_pd();
	accum_35 = _mm512_setzero_pd();

	for (; i < n32; i += 32) {
		accum_05 += _mm512_loadu_pd(&x[i+ 0]) * _mm512_loadu_pd(&y[i+ 0]);
		accum_15 += _mm512_loadu_pd(&x[i+ 8]) * _mm512_loadu_pd(&y[i+ 8]);
		accum_25 += _mm512_loadu_pd(&x[i+16]) * _mm512_loadu_pd(&y[i+16]);
		accum_35 += _mm512_loadu_pd(&x[i+24]) * _mm512_loadu_pd(&y[i+24]);
	}

	/*
	 * we need to fold our 512 bit wide accumulator vectors into 256 bit wide vectors so that the AVX2 code
	 * below can continue using the intermediate results in its loop
	 */
	accum_0 = _mm512_extractf64x4_pd(accum_05, 0) + _mm512_extractf64x4_pd(accum_05, 1);
	accum_1 = _mm512_extractf64x4_pd(accum_15, 0) + _mm512_extractf64x4_pd(accum_15, 1);
	accum_2 = _mm512_extractf64x4_pd(accum_25, 0) + _mm512_extractf64x4_pd(accum_25, 1);
	accum_3 = _mm512_extractf64x4_pd(accum_35, 0) + _mm512_extractf64x4_pd(accum_35, 1);

#endif
	for (; i < n; i += 16) {
		accum_0 += _mm256_loadu_pd(&x[i+ 0]) * _mm256_loadu_pd(&y[i+ 0]);
		accum_1 += _mm256_loadu_pd(&x[i+ 4]) * _mm256_loadu_pd(&y[i+ 4]);
		accum_2 += _mm256_loadu_pd(&x[i+ 8]) * _mm256_loadu_pd(&y[i+ 8]);
		accum_3 += _mm256_loadu_pd(&x[i+12]) * _mm256_loadu_pd(&y[i+12]);
	}

	/* we now have the partial sums of the dot product in the 4 accumulation vectors, time to consolidate */

	accum_0 = accum_0 + accum_1 + accum_2 + accum_3;

	__m128d half_accum0;

	/* Add upper half to lower half of each of the 256 bit vector to get a 128 bit vector */
	half_accum0 = _mm_add_pd(_mm256_extractf128_pd(accum_0, 0), _mm256_extractf128_pd(accum_0, 1));

	/* in 128 bit land there is a hadd operation to do the rest of the element-wise sum in one go */
	half_accum0 = _mm_hadd_pd(half_accum0, half_accum0);

	*dot = half_accum0[0];
}

#else
#include "ddot_microk_haswell-2.c"
#endif
