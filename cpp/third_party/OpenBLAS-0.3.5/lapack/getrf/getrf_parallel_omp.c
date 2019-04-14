/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include "common.h"

#define GEMM_PQ  MAX(GEMM_P, GEMM_Q)
#define REAL_GEMM_R (GEMM_R - GEMM_PQ)

static FLOAT dm1 = -1.;

static void inner_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos){

  BLASLONG is, min_i;
  BLASLONG js, min_j;
  BLASLONG jjs, min_jj;

  BLASLONG m = args -> m;
  BLASLONG n = args -> n;
  BLASLONG k = args -> k;

  BLASLONG lda = args -> lda;
  BLASLONG off = args -> ldb;

  FLOAT *b = (FLOAT *)args -> b + (k          ) * COMPSIZE;
  FLOAT *c = (FLOAT *)args -> b + (    k * lda) * COMPSIZE;
  FLOAT *d = (FLOAT *)args -> b + (k + k * lda) * COMPSIZE;

  blasint *ipiv = (blasint *)args -> c;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    c     += range_n[0] * lda * COMPSIZE;
    d     += range_n[0] * lda * COMPSIZE;
  }

  for (js = 0; js < n; js += REAL_GEMM_R) {
    min_j = n - js;
    if (min_j > REAL_GEMM_R) min_j = REAL_GEMM_R;

    for (jjs = js; jjs < js + min_j; jjs += GEMM_UNROLL_N){
      min_jj = js + min_j - jjs;
      if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#if 0
      LASWP_NCOPY(min_jj, off + 1, off + k,
		  c + (- off + jjs * lda) * COMPSIZE, lda,
		  ipiv, sb + k * (jjs - js) * COMPSIZE);

#else
      LASWP_PLUS(min_jj, off + 1, off + k, ZERO,
#ifdef COMPLEX
		 ZERO,
#endif
		 c + (- off + jjs * lda) * COMPSIZE, lda, NULL, 0, ipiv, 1);

      GEMM_ONCOPY (k, min_jj, c + jjs * lda * COMPSIZE, lda, sb + (jjs - js) * k * COMPSIZE);
#endif

      for (is = 0; is < k; is += GEMM_P) {
	min_i = k - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	TRSM_KERNEL_LT(min_i, min_jj, k, dm1,
#ifdef COMPLEX
		       ZERO,
#endif
		       (FLOAT *)args -> a  + k * is * COMPSIZE,
		       sb + (jjs - js) * k * COMPSIZE,
		       c   + (is + jjs * lda) * COMPSIZE, lda, is);
      }
    }

    for (is = 0; is < m; is += GEMM_P){
      min_i = m - is;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY (k, min_i, b + is * COMPSIZE, lda, sa);

      GEMM_KERNEL_N(min_i, min_j, k, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sb, d + (is + js * lda) * COMPSIZE, lda);
    }
  }
}

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG m, n, lda, offset;
  blasint *ipiv, iinfo, info;
  BLASLONG j, jb, mn, blocking;
  FLOAT *a, *offsetA, *offsetB;
  BLASLONG range_N[2];
  blas_arg_t newarg;

  int mode;

  FLOAT *sbb;

#ifndef COMPLEX
#ifdef XDOUBLE
  mode  =  BLAS_XDOUBLE | BLAS_REAL;
#elif defined(DOUBLE)
  mode  =  BLAS_DOUBLE  | BLAS_REAL;
#else
  mode  =  BLAS_SINGLE  | BLAS_REAL;
#endif
#else
#ifdef XDOUBLE
  mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
  mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
  mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif
#endif

  m    = args -> m;
  n    = args -> n;
  a    = (FLOAT *)args -> a;
  lda  = args -> lda;
  ipiv = (blasint *)args -> c;
  offset = 0;

  if (range_n) {
    m     -= range_n[0];
    n      = range_n[1] - range_n[0];
    offset = range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  if (m <= 0 || n <= 0) return 0;

  mn = MIN(m, n);

  blocking = ((mn / 2 + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
  if (blocking > GEMM_Q) blocking = GEMM_Q;

#ifdef POWER8
  if (blocking <= GEMM_UNROLL_N) {
    info = GETF2(args, NULL, range_n, sa, sb, 0);
    return info;
  }
#else
  if (blocking <= GEMM_UNROLL_N*2) {
    info = GETF2(args, NULL, range_n, sa, sb, 0);
    return info;
  }
#endif

  sbb = (FLOAT *)((((BLASULONG)(sb + blocking * blocking * COMPSIZE) + GEMM_ALIGN) & ~GEMM_ALIGN) + GEMM_OFFSET_B);

  info = 0;

  for (j = 0; j < mn; j += blocking) {

    jb = mn - j;
    if (jb > blocking) jb = blocking;

    offsetA = a +  j       * lda * COMPSIZE;
    offsetB = a + (j + jb) * lda * COMPSIZE;

    range_N[0] = offset + j;
    range_N[1] = offset + j + jb;

    iinfo   = CNAME(args, NULL, range_N, sa, sb, 0);

    if (iinfo && !info) info = iinfo + j;

    if (j + jb < n) {

      TRSM_ILTCOPY(jb, jb, offsetA + j * COMPSIZE, lda, 0, sb);

      newarg.m   = m - jb - j;
      newarg.n   = n - jb - j;
      newarg.k   = jb;

      newarg.a   = sb;
      newarg.lda = lda;
      newarg.b   = a + (j + j * lda) * COMPSIZE;
      newarg.ldb = j + offset;
      newarg.c   = ipiv;

      newarg.common = NULL;
      newarg.nthreads = args -> nthreads;

      gemm_thread_n(mode, &newarg, NULL, NULL,  (void *)inner_thread, sa, sbb, args -> nthreads);

    }
  }

  for (j = 0; j < mn; j += jb) {
    jb = MIN(mn - j, blocking);
    LASWP_PLUS(jb, j + jb + offset + 1, mn + offset, ZERO,
#ifdef COMPLEX
	       ZERO,
#endif
	       a - (offset - j * lda) * COMPSIZE, lda, NULL, 0 , ipiv, 1);

  }

  return info;
}
