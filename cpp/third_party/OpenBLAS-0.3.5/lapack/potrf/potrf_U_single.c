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

static FLOAT dm1 = -1.;

#ifndef COMPLEX
#define TRSM_KERNEL   TRSM_KERNEL_LT
#else
#define TRSM_KERNEL   TRSM_KERNEL_LC
#undef  SYRK_KERNEL_U
#ifdef XDOUBLE
#define SYRK_KERNEL_U xherk_kernel_UC
#elif defined(DOUBLE)
#define SYRK_KERNEL_U zherk_kernel_UC
#else
#define SYRK_KERNEL_U cherk_kernel_UC
#endif
#endif

#if 0
#undef GEMM_P
#undef GEMM_Q
#undef GEMM_R

#define GEMM_P 8
#define GEMM_Q 20
#define GEMM_R 64
#endif

#define GEMM_PQ  MAX(GEMM_P, GEMM_Q)
#define REAL_GEMM_R (GEMM_R - GEMM_PQ)

#if 0
#define SHARED_ARRAY
#define SA	aa
#else
#undef  SHARED_ARRAY
#define SA	sa
#endif

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG  n, lda;
  FLOAT *a;

  BLASLONG info;
  BLASLONG bk, blocking;
  BLASLONG is, min_i;
  BLASLONG jjs, min_jj;
  BLASLONG range_N[2];
  BLASLONG j, js, min_j;

#ifdef SHARED_ARRAY
  FLOAT *aa;
#endif

  FLOAT *sb2 = (FLOAT *)((((BLASLONG)sb
		    + GEMM_PQ  * GEMM_Q * COMPSIZE * SIZE + GEMM_ALIGN) & ~GEMM_ALIGN)
		  + GEMM_OFFSET_B);

  n      = args -> n;
  a      = (FLOAT *)args -> a;
  lda    = args -> lda;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  if (n <= DTB_ENTRIES / 2) {
    info = POTF2_U(args, NULL, range_n, sa, sb, 0);
    return info;
  }

  blocking = GEMM_Q;
  if (n <= 4 * GEMM_Q) blocking = (n + 3) / 4;

  for (j = 0; j < n; j += blocking) {
    bk = n - j;
    if (bk > blocking) bk = blocking;

    if (!range_n) {
      range_N[0] = j;
      range_N[1] = j + bk;
    } else {
      range_N[0] = range_n[0] + j;
      range_N[1] = range_n[0] + j + bk;
    }

    info = CNAME(args, NULL, range_N, sa, sb, 0);
    if (info) return info + j;

    if (n - j - bk > 0) {

      TRSM_IUNCOPY(bk, bk, a + (j + j * lda) * COMPSIZE, lda, 0, sb);

      for(js = j + bk; js < n; js += REAL_GEMM_R) {
	min_j = n - js;
	if (min_j > REAL_GEMM_R) min_j = REAL_GEMM_R;

	for(jjs = js; jjs < js + min_j; jjs += GEMM_UNROLL_N){
	  min_jj = min_j + js - jjs;
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

	  GEMM_ONCOPY(bk, min_jj, a + (j + jjs * lda) * COMPSIZE, lda, sb2 + bk * (jjs - js) * COMPSIZE);

	  for (is = 0; is < bk; is += GEMM_P) {
	    min_i = bk - is;
	    if (min_i > GEMM_P) min_i = GEMM_P;

	    TRSM_KERNEL (min_i, min_jj, bk, dm1,
#ifdef COMPLEX
			 ZERO,
#endif
			 sb + bk * is * COMPSIZE,
			 sb2 + bk * (jjs - js) * COMPSIZE,
			 a + (j + is + jjs * lda) * COMPSIZE, lda, is);
	  }
	}

	for (is = j + bk; is < js + min_j; is += min_i) {
	  min_i = js + min_j - is;

	  if (min_i >= GEMM_P * 2) {
	    min_i = GEMM_P;
	  } else
	    if (min_i > GEMM_P) {
	      min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	    }

#ifdef SHARED_ARRAY
	  if ((is >= js) && (is + min_i <= js + min_j)) {
	    aa = sb2 + bk * (is - js) * COMPSIZE;
	  } else {
	    GEMM_INCOPY(bk, min_i, a + (j + is * lda) * COMPSIZE, lda, sa);
	    aa = sa;
	  }
#else
	  GEMM_INCOPY(bk, min_i, a + (j + is * lda) * COMPSIZE, lda, sa);
#endif

	  SYRK_KERNEL_U(min_i, min_j, bk,
			dm1,
			SA, sb2,
			a + (is + js * lda) * COMPSIZE, lda,
			is - js);

	}
      }
    }

  }

  return 0;
}
