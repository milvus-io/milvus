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
#define TRSM_KERNEL   TRSM_KERNEL_RN
#else
#define TRSM_KERNEL   TRSM_KERNEL_RR
#undef SYRK_KERNEL_L
#ifdef XDOUBLE
#define SYRK_KERNEL_L xherk_kernel_LN
#elif defined(DOUBLE)
#define SYRK_KERNEL_L zherk_kernel_LN
#else
#define SYRK_KERNEL_L cherk_kernel_LN
#endif
#endif

#if 0
#undef GEMM_P
#undef GEMM_Q
#undef GEMM_R

#define GEMM_P 128
#define GEMM_Q 128
#define GEMM_R 4000
#endif

#define GEMM_PQ  MAX(GEMM_P, GEMM_Q)

//leave some space for GEMM_ALIGN in sb2
#define REAL_GEMM_R (GEMM_R - 2*GEMM_PQ)

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
  BLASLONG bk, j, blocking;
  BLASLONG is, min_i;
  BLASLONG js, min_j;
  BLASLONG range_N[2];

  FLOAT *sb2 = (FLOAT *)((((BLASLONG)sb
		    + GEMM_PQ  * GEMM_Q * COMPSIZE * SIZE + GEMM_ALIGN) & ~GEMM_ALIGN)
		  + GEMM_OFFSET_B);

#ifdef SHARED_ARRAY
  FLOAT *aa;
#endif

  n      = args -> n;
  a      = (FLOAT *)args -> a;
  lda    = args -> lda;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  if (n <= DTB_ENTRIES / 2) {
    info = POTF2_L(args, NULL, range_n, sa, sb, 0);
    return info;
  }

  blocking = GEMM_Q;
  if (n <= 4 * GEMM_Q) blocking = n / 4;

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

      TRSM_OLTCOPY(bk, bk, a + (j + j * lda) * COMPSIZE, lda, 0, sb);

      /* First tile */
      min_j = n - j - bk;
      if (min_j > REAL_GEMM_R) min_j = REAL_GEMM_R;

      for (is = j + bk; is < n; is += GEMM_P) {
	min_i = n - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

#ifdef SHARED_ARRAY

	if (is < j + bk + min_j) {
	  aa = sb2 + bk * (is - j - bk) * COMPSIZE;
	} else {
	  aa = sa;
	}

	GEMM_ITCOPY(bk, min_i, a + (is + j * lda) * COMPSIZE, lda, aa);

	TRSM_KERNEL(min_i, bk, bk, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    aa,
		    sb,
		    a + (is + j * lda) * COMPSIZE, lda, 0);

	SYRK_KERNEL_L(min_i, min_j, bk, dm1,
		      aa,
		      sb2,
		      a + (is + (j + bk) * lda) * COMPSIZE, lda,
		      is - j - bk);

#else

	GEMM_ITCOPY(bk, min_i, a + (is + j * lda) * COMPSIZE, lda, sa);

	TRSM_KERNEL(min_i, bk, bk, dm1,
#ifdef COMPLEX
		    ZERO,
#endif

		    sa,
		    sb,
		    a + (is + j * lda) * COMPSIZE, lda, 0);

	if (is < j + bk + min_j) {
	  GEMM_OTCOPY(bk, min_i, a + (is + j * lda) * COMPSIZE, lda, sb2 + bk * (is - j - bk) * COMPSIZE);
	}

	SYRK_KERNEL_L(min_i, min_j, bk, dm1,
		      sa,
		      sb2,
		      a + (is + (j + bk) * lda) * COMPSIZE, lda,
		      is - j - bk);
#endif
      }

      for(js = j + bk + min_j; js < n; js += REAL_GEMM_R){
	min_j = n - js;
	if (min_j > REAL_GEMM_R) min_j = REAL_GEMM_R;

	GEMM_OTCOPY(bk, min_j, a + (js + j * lda) * COMPSIZE, lda, sb2);

	for (is = js; is < n; is += GEMM_P) {
	  min_i = n - is;
	  if (min_i > GEMM_P) min_i = GEMM_P;

#ifdef SHARED_ARRAY

	  if (is + min_i < js + min_j) {
	    aa = sb2 + bk * (is - js) * COMPSIZE;
	  } else {
	    GEMM_ITCOPY(bk, min_i, a + (is + j * lda) * COMPSIZE, lda, sa);
	    aa = sa;
	  }

	  SYRK_KERNEL_L(min_i, min_j, bk, dm1,
			aa,
			sb2,
			a + (is + js * lda) * COMPSIZE, lda,
			is - js);

#else

	  GEMM_ITCOPY(bk, min_i, a + (is + j * lda) * COMPSIZE, lda, sa);

	  SYRK_KERNEL_L(min_i, min_j, bk, dm1,
			sa,
			sb2,
			a + (is + js * lda) * COMPSIZE, lda,
			is - js);
#endif

	}
      }

    }

  }

  return 0;
}
