/***************************************************************************
Copyright (c) 2014-2015, The OpenBLAS Project
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

#define HAVE_KERNEL_8 1

static void dscal_kernel_8( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	int i = 0;

#ifdef __AVX512CD__
	__m512d __alpha5 = _mm512_broadcastsd_pd(_mm_load_sd(alpha));
	for (; i < n; i += 8) {
                _mm512_storeu_pd(&x[i +  0], __alpha5 * _mm512_loadu_pd(&x[i +  0]));
	}
#else
	__m256d __alpha = _mm256_broadcastsd_pd(_mm_load_sd(alpha));
	for (; i < n; i += 8) {
                _mm256_storeu_pd(&x[i +  0], __alpha * _mm256_loadu_pd(&x[i +  0]));
                _mm256_storeu_pd(&x[i +  4], __alpha * _mm256_loadu_pd(&x[i +  4]));
	}
#endif
} 


static void dscal_kernel_8_zero( BLASLONG n, FLOAT *alpha, FLOAT *x)
{
	int i = 0;

	/* question to self: Why is this not just memset() */

#ifdef __AVX512CD__
	__m512d zero = _mm512_setzero_pd();
	for (; i < n; i += 8) {
                _mm512_storeu_pd(&x[i], zero);
	}
#else
	__m256d zero = _mm256_setzero_pd();
	for (; i < n; i += 8) {
                _mm256_storeu_pd(&x[i +  0], zero);
                _mm256_storeu_pd(&x[i +  4], zero);
	}
#endif

} 

#else
#include "dscal_microk_haswell-2.c"
#endif
