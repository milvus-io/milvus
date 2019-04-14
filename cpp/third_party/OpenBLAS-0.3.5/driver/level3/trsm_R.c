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
#include <ctype.h>
#include "common.h"

const static FLOAT dm1 = -1.;

#ifdef CONJ
#define GEMM_KERNEL   GEMM_KERNEL_R
#if (!defined(TRANSA) && defined(UPPER)) || (defined(TRANSA) && !defined(UPPER))
#define TRSM_KERNEL   TRSM_KERNEL_RR
#else
#define TRSM_KERNEL   TRSM_KERNEL_RC
#endif
#else
#define GEMM_KERNEL   GEMM_KERNEL_N
#if (!defined(TRANSA) && defined(UPPER)) || (defined(TRANSA) && !defined(UPPER))
#define TRSM_KERNEL   TRSM_KERNEL_RN
#else
#define TRSM_KERNEL   TRSM_KERNEL_RT
#endif
#endif

#if 0
#undef GEMM_P
#undef GEMM_Q
#undef GEMM_R

#define GEMM_P 16
#define GEMM_Q 20
#define GEMM_R 24
#endif

int CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG dummy) {

  BLASLONG m, n, lda, ldb;
  FLOAT *beta, *a, *b;
  BLASLONG ls, is, js;
  BLASLONG min_l, min_i, min_j;
  BLASLONG jjs, min_jj;
#if !((defined(UPPER) && !defined(TRANSA)) || (!defined(UPPER) && defined(TRANSA)))
  BLASLONG start_ls;
#endif

  m = args -> m;
  n = args -> n;

  a = (FLOAT *)args -> a;
  b = (FLOAT *)args -> b;

  lda = args -> lda;
  ldb = args -> ldb;

  beta  = (FLOAT *)args -> beta;

  if (range_m) {
    BLASLONG m_from = *(((BLASLONG *)range_m) + 0);
    BLASLONG m_to   = *(((BLASLONG *)range_m) + 1);

    m = m_to - m_from;

    b += m_from * COMPSIZE;
  }

  if (beta) {
#ifndef COMPLEX
    if (beta[0] != ONE)
      GEMM_BETA(m, n, 0, beta[0], NULL, 0, NULL, 0, b, ldb);
    if (beta[0] == ZERO) return 0;
#else
    if ((beta[0] != ONE) || (beta[1] != ZERO))
      GEMM_BETA(m, n, 0, beta[0], beta[1], NULL, 0, NULL, 0, b, ldb);
    if ((beta[0] == ZERO) && (beta[1] == ZERO)) return 0;
#endif
  }

#if (defined(UPPER) && !defined(TRANSA)) || (!defined(UPPER) && defined(TRANSA))
  for(js = 0; js < n; js += GEMM_R){
    min_j = n - js;
    if (min_j > GEMM_R) min_j = GEMM_R;

    for(ls = 0; ls < js; ls += GEMM_Q){
      min_l = js - ls;
      if (min_l > GEMM_Q) min_l = GEMM_Q;
      min_i = m;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY(min_l, min_i, b + (ls * ldb) * COMPSIZE, ldb, sa);

      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;
	if (min_jj > GEMM_UNROLL_N*3) min_jj = GEMM_UNROLL_N*3;
	else
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#ifndef TRANSA
	GEMM_ONCOPY(min_l, min_jj, a + (ls + jjs * lda) * COMPSIZE, lda, sb + min_l * (jjs - js) * COMPSIZE);
#else
	GEMM_OTCOPY(min_l, min_jj, a + (jjs + ls * lda) * COMPSIZE, lda, sb + min_l * (jjs - js) * COMPSIZE);
#endif

	GEMM_KERNEL(min_i, min_jj, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sb + min_l * (jjs - js) * COMPSIZE,
		    b + (jjs * ldb) * COMPSIZE, ldb);
      }

      for(is = min_i; is < m; is += GEMM_P){
	min_i = m - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	GEMM_ITCOPY(min_l, min_i, b + (is + ls * ldb) * COMPSIZE, ldb, sa);

	GEMM_KERNEL(min_i, min_j, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sb, b + (is + js * ldb) * COMPSIZE, ldb);
      }
    }

    for(ls = js; ls < js + min_j; ls += GEMM_Q){
      min_l = js + min_j - ls;
      if (min_l > GEMM_Q) min_l = GEMM_Q;
      min_i = m;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY(min_l, min_i, b + (ls * ldb) * COMPSIZE, ldb, sa);

#ifndef TRANSA
      TRSM_OUNCOPY(min_l, min_l, a + (ls + ls * lda) * COMPSIZE, lda, 0, sb);
#else
      TRSM_OLTCOPY(min_l, min_l, a + (ls + ls * lda) * COMPSIZE, lda, 0, sb);
#endif

      TRSM_KERNEL(min_i, min_l, min_l, dm1,
#ifdef COMPLEX
		  ZERO,
#endif
		  sa,
		  sb,
		  b + (ls * ldb) * COMPSIZE, ldb, 0);

      for(jjs = 0; jjs < min_j - min_l - ls + js; jjs += min_jj){
	min_jj = min_j - min_l - ls + js - jjs;
	if (min_jj > GEMM_UNROLL_N*3) min_jj = GEMM_UNROLL_N*3;
	else
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#ifndef TRANSA
      GEMM_ONCOPY (min_l, min_jj, a + (ls + (ls + min_l + jjs) * lda) * COMPSIZE, lda,
		   sb + min_l * (min_l + jjs) * COMPSIZE);
#else
      GEMM_OTCOPY (min_l, min_jj, a + ((ls + min_l + jjs) + ls * lda) * COMPSIZE, lda,
		   sb + min_l * (min_l + jjs) * COMPSIZE);
#endif

      GEMM_KERNEL(min_i, min_jj, min_l, dm1,
#ifdef COMPLEX
		  ZERO,
#endif
		  sa,
		  sb + min_l * (min_l + jjs) * COMPSIZE,
		  b + (min_l + ls + jjs) * ldb * COMPSIZE, ldb);
      }

      for(is = min_i; is < m; is += GEMM_P){
	min_i = m - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	GEMM_ITCOPY(min_l, min_i, b + (is + ls * ldb) * COMPSIZE, ldb, sa);

	TRSM_KERNEL(min_i, min_l, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa,
		    sb,
		    b + (is + ls * ldb) * COMPSIZE, ldb, 0);

	GEMM_KERNEL(min_i, min_j - min_l + js - ls, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa,
		    sb + min_l * min_l * COMPSIZE,
		    b + (is + ( min_l + ls) * ldb) * COMPSIZE, ldb);
      }
    }
  }

#else
  for(js = n; js > 0; js -= GEMM_R){
    min_j = js;
    if (min_j > GEMM_R) min_j = GEMM_R;

    for (ls = js; ls < n; ls += GEMM_Q) {
      min_l = n - ls;
      if (min_l > GEMM_Q) min_l = GEMM_Q;
      min_i = m;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY(min_l, min_i, b + (ls * ldb) * COMPSIZE, ldb, sa);

      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;
	if (min_jj > GEMM_UNROLL_N*3) min_jj = GEMM_UNROLL_N*3;
	else
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#ifndef TRANSA
	GEMM_ONCOPY(min_l, min_jj, a + (ls + (jjs - min_j) * lda) * COMPSIZE, lda, sb + min_l * (jjs - js) * COMPSIZE);
#else
	GEMM_OTCOPY(min_l, min_jj, a + ((jjs - min_j) + ls * lda) * COMPSIZE, lda, sb + min_l * (jjs - js) * COMPSIZE);
#endif

	GEMM_KERNEL(min_i, min_jj, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sb + min_l * (jjs - js) * COMPSIZE,
		    b + (jjs - min_j) * ldb * COMPSIZE, ldb);
      }

      for(is = min_i; is < m; is += GEMM_P){
	min_i = m - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	GEMM_ITCOPY(min_l, min_i, b + (is + ls * ldb) * COMPSIZE, ldb, sa);

	GEMM_KERNEL(min_i, min_j, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sb, b + (is + (js - min_j) * ldb) * COMPSIZE, ldb);
      }
    }

    start_ls = js - min_j;
    while (start_ls + GEMM_Q < js) start_ls += GEMM_Q;

    for(ls = start_ls; ls >= js - min_j; ls -= GEMM_Q){
      min_l = js - ls;
      if (min_l > GEMM_Q) min_l = GEMM_Q;
      min_i = m;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY(min_l, min_i, b + (ls * ldb) * COMPSIZE, ldb, sa);

#ifndef TRANSA
      TRSM_OLNCOPY(min_l, min_l,           a + (ls + ls * lda) * COMPSIZE, lda,
		   0, sb + min_l * (min_j - js + ls) * COMPSIZE);
#else
      TRSM_OUTCOPY(min_l, min_l,           a + (ls + ls * lda) * COMPSIZE, lda,
		   0, sb + min_l * (min_j - js + ls) * COMPSIZE);
#endif

      TRSM_KERNEL(min_i, min_l, min_l, dm1,
#ifdef COMPLEX
		  ZERO,
#endif
		  sa,
		  sb + min_l * (min_j - js + ls) * COMPSIZE,
		  b + (ls * ldb) * COMPSIZE, ldb, 0);

      for(jjs = 0; jjs < min_j - js + ls; jjs += min_jj){
	min_jj = min_j - js + ls - jjs;
	if (min_jj > GEMM_UNROLL_N*3) min_jj = GEMM_UNROLL_N*3;
	else
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

#ifndef TRANSA
	GEMM_ONCOPY (min_l, min_jj, a + (ls + (js - min_j + jjs) * lda) * COMPSIZE, lda,
		     sb + min_l * jjs * COMPSIZE);
#else
	GEMM_OTCOPY (min_l, min_jj, a + ((js - min_j + jjs) + ls * lda) * COMPSIZE, lda,
		     sb + min_l * jjs * COMPSIZE);
#endif

	GEMM_KERNEL(min_i, min_jj, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa,
		    sb + min_l * jjs * COMPSIZE,
		    b + (js - min_j + jjs) * ldb * COMPSIZE, ldb);
      }

      for(is = min_i; is < m; is += GEMM_P){
	min_i = m - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	GEMM_ITCOPY(min_l, min_i, b + (is + ls * ldb) * COMPSIZE, ldb, sa);

	TRSM_KERNEL(min_i, min_l, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa,
		    sb + min_l * (min_j - js + ls) * COMPSIZE,
		    b + (is + ls * ldb) * COMPSIZE, ldb, 0);

	GEMM_KERNEL(min_i, min_j - js + ls, min_l, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa,
		    sb,
		    b + (is + (js - min_j) * ldb) * COMPSIZE, ldb);
      }

    }
  }

#endif

  return 0;
}
