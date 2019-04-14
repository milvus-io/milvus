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

#include <immintrin.h>

#define HAVE_KERNEL_4x4 1

static void dsymv_kernel_4x4(BLASLONG from, BLASLONG to, FLOAT **a, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{


	__m256d accum_0, accum_1, accum_2, accum_3; 
	__m256d temp1_0, temp1_1, temp1_2, temp1_3;

	/* the 256 bit wide acculmulator vectors start out as zero */
	accum_0 = _mm256_setzero_pd();
	accum_1 = _mm256_setzero_pd();
	accum_2 = _mm256_setzero_pd();
	accum_3 = _mm256_setzero_pd();

	temp1_0 = _mm256_broadcastsd_pd(_mm_load_sd(&temp1[0]));
	temp1_1 = _mm256_broadcastsd_pd(_mm_load_sd(&temp1[1]));
	temp1_2 = _mm256_broadcastsd_pd(_mm_load_sd(&temp1[2]));
	temp1_3 = _mm256_broadcastsd_pd(_mm_load_sd(&temp1[3]));

#ifdef __AVX512CD__
	__m512d accum_05, accum_15, accum_25, accum_35;
	__m512d temp1_05, temp1_15, temp1_25, temp1_35;
	BLASLONG to2;
	int delta;

	/* the 512 bit wide accumulator vectors start out as zero */
	accum_05 = _mm512_setzero_pd();
	accum_15 = _mm512_setzero_pd();
	accum_25 = _mm512_setzero_pd();
	accum_35 = _mm512_setzero_pd();

	temp1_05 = _mm512_broadcastsd_pd(_mm_load_sd(&temp1[0]));
	temp1_15 = _mm512_broadcastsd_pd(_mm_load_sd(&temp1[1]));
	temp1_25 = _mm512_broadcastsd_pd(_mm_load_sd(&temp1[2]));
	temp1_35 = _mm512_broadcastsd_pd(_mm_load_sd(&temp1[3]));

	delta = (to - from) & ~7;
	to2 = from + delta;


	for (; from < to2; from += 8) {
		__m512d _x, _y;
		__m512d a0, a1, a2, a3;

		_y = _mm512_loadu_pd(&y[from]);
		_x = _mm512_loadu_pd(&x[from]);

		a0 = _mm512_loadu_pd(&a[0][from]);
		a1 = _mm512_loadu_pd(&a[1][from]);
		a2 = _mm512_loadu_pd(&a[2][from]);
		a3 = _mm512_loadu_pd(&a[3][from]);

		_y += temp1_05 * a0 + temp1_15 * a1 + temp1_25 * a2 + temp1_35 * a3;

		accum_05 += _x * a0;
		accum_15 += _x * a1;
		accum_25 += _x * a2;
		accum_35 += _x * a3;

		_mm512_storeu_pd(&y[from], _y);

	};

	/*
	 * we need to fold our 512 bit wide accumulator vectors into 256 bit wide vectors so that the AVX2 code
	 * below can continue using the intermediate results in its loop
	 */
	accum_0 = _mm256_add_pd(_mm512_extractf64x4_pd(accum_05, 0), _mm512_extractf64x4_pd(accum_05, 1));
	accum_1 = _mm256_add_pd(_mm512_extractf64x4_pd(accum_15, 0), _mm512_extractf64x4_pd(accum_15, 1));
	accum_2 = _mm256_add_pd(_mm512_extractf64x4_pd(accum_25, 0), _mm512_extractf64x4_pd(accum_25, 1));
	accum_3 = _mm256_add_pd(_mm512_extractf64x4_pd(accum_35, 0), _mm512_extractf64x4_pd(accum_35, 1));

#endif

	for (; from != to; from += 4) {
		__m256d _x, _y;
		__m256d a0, a1, a2, a3;

		_y = _mm256_loadu_pd(&y[from]);
		_x = _mm256_loadu_pd(&x[from]);

		/* load 4 rows of matrix data */
		a0 = _mm256_loadu_pd(&a[0][from]);
		a1 = _mm256_loadu_pd(&a[1][from]);
		a2 = _mm256_loadu_pd(&a[2][from]);
		a3 = _mm256_loadu_pd(&a[3][from]);

		_y += temp1_0 * a0 + temp1_1 * a1 + temp1_2 * a2 + temp1_3 * a3;

		accum_0 += _x * a0;
		accum_1 += _x * a1;
		accum_2 += _x * a2;
		accum_3 += _x * a3;

		_mm256_storeu_pd(&y[from], _y);

	};

	/*
	 * we now have 4 accumulator vectors. Each vector needs to be summed up element wise and stored in the temp2
	 * output array. There is no direct instruction for this in 256 bit space, only in 128 space.
	 */

	__m128d half_accum0, half_accum1, half_accum2, half_accum3;


	/* Add upper half to lower half of each of the four 256 bit vectors to get to four 128 bit vectors */
	half_accum0 = _mm_add_pd(_mm256_extractf128_pd(accum_0, 0), _mm256_extractf128_pd(accum_0, 1));
	half_accum1 = _mm_add_pd(_mm256_extractf128_pd(accum_1, 0), _mm256_extractf128_pd(accum_1, 1));
	half_accum2 = _mm_add_pd(_mm256_extractf128_pd(accum_2, 0), _mm256_extractf128_pd(accum_2, 1));
	half_accum3 = _mm_add_pd(_mm256_extractf128_pd(accum_3, 0), _mm256_extractf128_pd(accum_3, 1));

	/* in 128 bit land there is a hadd operation to do the rest of the element-wise sum in one go */
	half_accum0 = _mm_hadd_pd(half_accum0, half_accum0);
	half_accum1 = _mm_hadd_pd(half_accum1, half_accum1);
	half_accum2 = _mm_hadd_pd(half_accum2, half_accum2);
	half_accum3 = _mm_hadd_pd(half_accum3, half_accum3);

	/* and store the lowest double value from each of these vectors in the temp2 output */
	temp2[0] += half_accum0[0];
	temp2[1] += half_accum1[0];
	temp2[2] += half_accum2[0];
	temp2[3] += half_accum3[0];
} 
#else
#include "dsymv_L_microk_haswell-2.c"
#endif