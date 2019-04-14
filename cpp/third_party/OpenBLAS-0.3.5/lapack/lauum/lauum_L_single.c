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

static FLOAT dp1 =  1.;

#ifndef COMPLEX
#define TRMM_KERNEL	TRMM_KERNEL_LN
#define SYRK_KERNEL	SYRK_KERNEL_L
#else
#define TRMM_KERNEL	TRMM_KERNEL_LR
#ifdef XDOUBLE
#define SYRK_KERNEL	xherk_kernel_LC
#elif defined(DOUBLE)
#define SYRK_KERNEL	zherk_kernel_LC
#else
#define SYRK_KERNEL	cherk_kernel_LC
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

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG  n, lda;
  FLOAT *a;

  BLASLONG j, bk, blocking;
  BLASLONG jjs, min_jj;

  BLASLONG is, ls, ks;
  BLASLONG min_i, min_l, min_k;
  BLASLONG range_N[2];

  FLOAT *sb2 = (FLOAT *)((((BLASLONG)sb
		    + GEMM_PQ  * GEMM_Q * COMPSIZE * SIZE + GEMM_ALIGN) & ~GEMM_ALIGN)
		  + GEMM_OFFSET_B);

#if 0
  FLOAT *aa;
#endif

  n      = args -> n;
  a      = (FLOAT *)args -> a;
  lda    = args -> lda;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  if (n <= DTB_ENTRIES) {
    LAUU2_L(args, NULL, range_n, sa, sb, 0);
    return 0;
  }

  blocking = GEMM_Q;
  if (n <= 4 * GEMM_Q) blocking = (n + 3) / 4;

  for (j = 0; j < n; j += blocking) {
    bk = MIN(blocking, n - j);

    if (j > 0 ){

      TRMM_ILNCOPY(bk, bk, a + (j + j * lda) * COMPSIZE, lda, 0, 0, sb);

      for (ls = 0; ls < j; ls += REAL_GEMM_R) {
	min_l = j - ls;
	if (min_l > REAL_GEMM_R) min_l = REAL_GEMM_R;

#if 0

	min_i = j - ls;
	if (min_i > GEMM_P) min_i = GEMM_P;

	if (ls + min_i >= ls + min_l) {
	  GEMM_INCOPY(bk, min_i, a + (j + ls * lda)* COMPSIZE, lda, sa);
	  aa = sa;
	} else {
	  aa = sb2;
	}

	for (jjs = ls; jjs < ls + min_l; jjs += GEMM_P){
	  min_jj = ls + min_l - jjs;
	  if (min_jj > GEMM_P) min_jj = GEMM_P;

	  GEMM_ONCOPY(bk, min_jj, a + (j + jjs * lda) * COMPSIZE, lda, sb2 + (jjs - ls) * bk * COMPSIZE);

	  SYRK_KERNEL(min_i, min_jj, bk, dp1,
		      aa,
		      sb2 + (jjs - ls) * bk * COMPSIZE,
		      a + (ls + jjs * lda) * COMPSIZE, lda,
		      ls - jjs);
	}


	for(is = ls + min_i; is < j ; is += GEMM_P){
	  min_i = j - is;
	  if (min_i > GEMM_P) min_i = GEMM_P;

	  GEMM_INCOPY(bk, min_i, a + (j + is * lda)* COMPSIZE, lda, sa);

	  SYRK_KERNEL(min_i, min_l, bk, dp1,
		      sa,
		      sb2,
		      a + (is + ls * lda) * COMPSIZE, lda,
		      is - ls);
	}

	for (ks = 0; ks < bk; ks += GEMM_P) {
	  min_k = bk - ks;
	  if (min_k > GEMM_P) min_k = GEMM_P;

	  TRMM_KERNEL(min_k, min_l, bk, dp1,
#ifdef COMPLEX
		      ZERO,
#endif
		      sb + ks * bk * COMPSIZE,
		      sb2,
		      a + (ks + j + ls * lda) * COMPSIZE, lda, ks);
	}
#else

	min_i = j - ls;
	if (min_i > GEMM_P) min_i = GEMM_P;

	GEMM_INCOPY(bk, min_i, a + (j + ls * lda)* COMPSIZE, lda, sa);

	for (jjs = ls; jjs < ls + min_l; jjs += GEMM_P){
	  min_jj = ls + min_l - jjs;
	  if (min_jj > GEMM_P) min_jj = GEMM_P;

	  GEMM_ONCOPY(bk, min_jj, a + (j + jjs * lda) * COMPSIZE, lda, sb2 + (jjs - ls) * bk * COMPSIZE);

	  SYRK_KERNEL(min_i, min_jj, bk, dp1,
		      sa,
		      sb2 + (jjs - ls) * bk * COMPSIZE,
		      a + (ls + jjs * lda) * COMPSIZE, lda,
		      ls - jjs);
	}

	for(is = ls + min_i; is < j ; is += GEMM_P){
	  min_i = j - is;
	  if (min_i > GEMM_P) min_i = GEMM_P;

	  GEMM_INCOPY(bk, min_i, a + (j + is * lda)* COMPSIZE, lda, sa);

	  SYRK_KERNEL(min_i, min_l, bk, dp1,
		      sa,
		      sb2,
		      a + (is + ls * lda) * COMPSIZE, lda,
		      is - ls);
	}

	for (ks = 0; ks < bk; ks += GEMM_P) {
	  min_k = bk - ks;
	  if (min_k > GEMM_P) min_k = GEMM_P;

	  TRMM_KERNEL(min_k, min_l, bk, dp1,
#ifdef COMPLEX
		      ZERO,
#endif
		      sb + ks * bk * COMPSIZE,
		      sb2,
		      a + (ks + j + ls * lda) * COMPSIZE, lda, ks);
	}

#endif

      }
    }

    if (!range_n) {
      range_N[0] = j;
      range_N[1] = j + bk;
    } else {
      range_N[0] = range_n[0] + j;
      range_N[1] = range_n[0] + j + bk;
    }

    CNAME(args, NULL, range_N, sa, sb, 0);

  }

  return 0;
}
