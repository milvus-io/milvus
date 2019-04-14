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

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG m, n, lda, offset;
  BLASLONG j, js, jmin, is, imin, jc, jcmin;
  BLASLONG jjs, min_jj;
  blasint *ipiv, iinfo, info;
  BLASLONG jb, mn, blocking;
  FLOAT *a, *offsetA; //, *offsetB;
  BLASLONG range_N[2];

  FLOAT *sbb;

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
  if (blocking <= GEMM_UNROLL_N * 2) {
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
    // offsetB = a + (j + jb) * lda * COMPSIZE;

    range_N[0] = offset + j;
    range_N[1] = offset + j + jb;

    iinfo   = CNAME(args, NULL, range_N, sa, sb, 0);

    if (iinfo && !info) info = iinfo + j;

    if (j + jb < n) {

      TRSM_ILTCOPY(jb, jb, offsetA + j * COMPSIZE, lda, 0, sb);

      for (js = j + jb; js < n; js += REAL_GEMM_R){
	jmin = n - js;
	if (jmin > REAL_GEMM_R) jmin = REAL_GEMM_R;

	  for (jjs = js; jjs < js + jmin; jjs += GEMM_UNROLL_N){
	    min_jj = js + jmin - jjs;
	    if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#if 1
	    LASWP_PLUS(min_jj, j + offset + 1, j + jb + offset, ZERO,
#ifdef COMPLEX
		       ZERO,
#endif
		       a + (- offset + jjs * lda) * COMPSIZE, lda, NULL, 0 , ipiv, 1);

	    GEMM_ONCOPY (jb, min_jj, a + (j + jjs * lda) * COMPSIZE, lda, sbb + jb * (jjs - js) * COMPSIZE);
#else
	    LASWP_NCOPY(min_jj, j + offset + 1, j + jb + offset,
			a + (- offset + jjs * lda) * COMPSIZE, lda, ipiv, sbb + jb * (jjs - js) * COMPSIZE);
#endif


	    for (jc = 0; jc < jb; jc += GEMM_P) {
	      jcmin = jb - jc;
	      if (jcmin > GEMM_P) jcmin = GEMM_P;

	      TRSM_KERNEL_LT(jcmin, min_jj, jb, dm1,
#ifdef COMPLEX
			     ZERO,
#endif
			     sb  + jb * jc * COMPSIZE,
			     sbb + jb * (jjs - js) * COMPSIZE,
			     a + (j + jc + jjs * lda) * COMPSIZE, lda, jc);
	    }
	  }


	for (is = j + jb; is < m; is += GEMM_P){

	  imin = m - is;
	  if (imin > GEMM_P) imin = GEMM_P;

	  GEMM_ITCOPY (jb, imin, offsetA + is * COMPSIZE, lda, sa);

	  GEMM_KERNEL_N(imin, jmin, jb, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			sa, sbb, 	a + (is + js * lda) * COMPSIZE, lda);
	}
      }
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
