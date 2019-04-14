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

#ifndef BETA_OPERATION
#define BETA_OPERATION(M_FROM, M_TO, N_FROM, N_TO, BETA, C, LDC) \
	GEMM_BETA((M_TO) - (M_FROM), (N_TO - N_FROM), 0, \
		  BETA[0], BETA[1], NULL, 0, NULL, 0, \
		  (FLOAT *)(C) + (M_FROM) + (N_FROM) * (LDC) * COMPSIZE, LDC)
#endif

#ifndef ICOPYB_OPERATION
#if defined(NN) || defined(NT) || defined(NC) || defined(NR) || \
    defined(RN) || defined(RT) || defined(RC) || defined(RR)
#define ICOPYB_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_ITCOPYB(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER)
#else
#define ICOPYB_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_INCOPYB(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER)
#endif
#endif

#ifndef ICOPYR_OPERATION
#if defined(NN) || defined(NT) || defined(NC) || defined(NR) || \
    defined(RN) || defined(RT) || defined(RC) || defined(RR)
#define ICOPYR_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_ITCOPYR(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER)
#else
#define ICOPYR_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_INCOPYR(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER)
#endif
#endif

#ifndef ICOPYI_OPERATION
#if defined(NN) || defined(NT) || defined(NC) || defined(NR) || \
    defined(RN) || defined(RT) || defined(RC) || defined(RR)
#define ICOPYI_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_ITCOPYI(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER)
#else
#define ICOPYI_OPERATION(M, N, A, LDA, X, Y, BUFFER) \
	GEMM3M_INCOPYI(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER)
#endif
#endif


#ifndef OCOPYB_OPERATION
#if defined(NN) || defined(TN) || defined(CN) || defined(RN) || \
    defined(NR) || defined(TR) || defined(CR) || defined(RR)
#define OCOPYB_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_ONCOPYB(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#else
#define OCOPYB_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_OTCOPYB(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#endif
#endif

#ifndef OCOPYR_OPERATION
#if defined(NN) || defined(TN) || defined(CN) || defined(RN) || \
    defined(NR) || defined(TR) || defined(CR) || defined(RR)
#define OCOPYR_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_ONCOPYR(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#else
#define OCOPYR_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_OTCOPYR(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#endif
#endif


#ifndef OCOPYI_OPERATION
#if defined(NN) || defined(TN) || defined(CN) || defined(RN) || \
    defined(NR) || defined(TR) || defined(CR) || defined(RR)
#define OCOPYI_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_ONCOPYI(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#else
#define OCOPYI_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	GEMM3M_OTCOPYI(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, ALPHA_R, ALPHA_I, BUFFER)
#endif
#endif

#ifndef KERNEL_FUNC
#define KERNEL_FUNC	GEMM3M_KERNEL
#endif

#ifndef KERNEL_OPERATION
#define KERNEL_OPERATION(M, N, K, ALPHA_R, ALPHA_I, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA_R, ALPHA_I, SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#endif

#ifndef A
#define A	args -> a
#endif
#ifndef LDA
#define LDA	args -> lda
#endif
#ifndef B
#define B	args -> b
#endif
#ifndef LDB
#define LDB	args -> ldb
#endif
#ifndef C
#define C	args -> c
#endif
#ifndef LDC
#define LDC	args -> ldc
#endif
#ifndef M
#define M	args -> m
#endif
#ifndef N
#define N	args -> n
#endif
#ifndef K
#define K	args -> k
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ALPHA1	ONE
#define ALPHA2	ONE
#define ALPHA5	ZERO
#define ALPHA6	ONE

#define ALPHA7	ONE
#define ALPHA8	ZERO
#define ALPHA11	ONE
#define ALPHA12	-ONE

#define ALPHA13	ZERO
#define ALPHA14	ONE
#define ALPHA17	-ONE
#define ALPHA18	-ONE
#endif

#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ALPHA1	ONE
#define ALPHA2	ONE
#define ALPHA5	ONE
#define ALPHA6	ZERO

#define ALPHA7	ZERO
#define ALPHA8	ONE
#define ALPHA11	-ONE
#define ALPHA12	-ONE

#define ALPHA13	ONE
#define ALPHA14	ZERO
#define ALPHA17	-ONE
#define ALPHA18	ONE
#endif

#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ALPHA1	ONE
#define ALPHA2	ONE
#define ALPHA5	ONE
#define ALPHA6	ZERO

#define ALPHA7	ZERO
#define ALPHA8	ONE
#define ALPHA11	-ONE
#define ALPHA12	ONE

#define ALPHA13	ONE
#define ALPHA14	ZERO
#define ALPHA17	-ONE
#define ALPHA18	-ONE
#endif

#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define ALPHA1	ONE
#define ALPHA2	ONE
#define ALPHA5	ZERO
#define ALPHA6	-ONE

#define ALPHA7	ONE
#define ALPHA8	ZERO
#define ALPHA11	ONE
#define ALPHA12	ONE

#define ALPHA13	ZERO
#define ALPHA14	ONE
#define ALPHA17	-ONE
#define ALPHA18	ONE
#endif

#ifdef TIMING
#define START_RPCC()		rpcc_counter = rpcc()
#define STOP_RPCC(COUNTER)	COUNTER  += rpcc() - rpcc_counter
#else
#define START_RPCC()
#define STOP_RPCC(COUNTER)
#endif

int CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n,
		  FLOAT *sa, FLOAT *sb, BLASLONG dummy){
  BLASLONG k, lda, ldb, ldc;
  FLOAT *alpha, *beta;
  FLOAT *a, *b, *c;
  BLASLONG m_from, m_to, n_from, n_to;

  BLASLONG ls, is, js, jjs;
  BLASLONG min_l, min_i, min_j, min_jj;

#ifdef TIMING
  BLASULONG rpcc_counter;
  BLASULONG BLASLONG innercost  = 0;
  BLASULONG BLASLONG outercost  = 0;
  BLASULONG BLASLONG kernelcost = 0;
  double total;
#endif

  k = K;

  a = (FLOAT *)A;
  b = (FLOAT *)B;
  c = (FLOAT *)C;

  lda = LDA;
  ldb = LDB;
  ldc = LDC;

  alpha = (FLOAT *)args -> alpha;
  beta  = (FLOAT *)args -> beta;

  m_from = 0;
  m_to   = M;

  if (range_m) {
    m_from = *(((BLASLONG *)range_m) + 0);
    m_to   = *(((BLASLONG *)range_m) + 1);
  }

  n_from = 0;
  n_to   = N;

  if (range_n) {
    n_from = *(((BLASLONG *)range_n) + 0);
    n_to   = *(((BLASLONG *)range_n) + 1);
  }

  if (beta) {
#ifndef COMPLEX
    if (beta[0] != ONE)
#else
    if ((beta[0] != ONE) || (beta[1] != ZERO))
#endif
      BETA_OPERATION(m_from, m_to, n_from, n_to, beta, c, ldc);
  }

  if ((k == 0) || (alpha == NULL)) return 0;

  if ((alpha[0] == ZERO)
#ifdef COMPLEX
      && (alpha[1] == ZERO)
#endif
      ) return 0;

#if 0
  printf("GEMM: M_from : %ld  M_to : %ld  N_from : %ld  N_to : %ld  k : %ld\n", m_from, m_to, n_from, n_to, k);
  printf("GEMM: P = %4ld  Q = %4ld  R = %4ld\n", (BLASLONG)GEMM3M_P, (BLASLONG)GEMM3M_Q, (BLASLONG)GEMM3M_R);
  printf("GEMM: SA .. %p  SB .. %p\n", sa, sb);
#endif

#ifdef TIMING
  innercost = 0;
  outercost = 0;
  kernelcost = 0;
#endif

  for(js = n_from; js < n_to; js += GEMM3M_R){
    min_j = n_to - js;
    if (min_j > GEMM3M_R) min_j = GEMM3M_R;

    for(ls = 0; ls < k; ls += min_l){
      min_l = k - ls;

      if (min_l >= GEMM3M_Q * 2) {
	min_l = GEMM3M_Q;
      } else {
	if (min_l > GEMM3M_Q) {
	  min_l = (min_l + 1) / 2;
#ifdef UNROLL_X
	  min_l = ((min_l + UNROLL_X - 1)/UNROLL_X) * UNROLL_X;
#endif
	}
      }

      min_i = m_to - m_from;
      if (min_i >= GEMM3M_P * 2) {
	min_i = GEMM3M_P;
      } else {
	if (min_i > GEMM3M_P) {
	  min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	}
      }

      START_RPCC();

      ICOPYB_OPERATION(min_l, min_i, a, lda, ls, m_from, sa);

      STOP_RPCC(innercost);

      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;
	if (min_jj > GEMM3M_UNROLL_N) min_jj = GEMM3M_UNROLL_N;

	START_RPCC();

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || defined(RN) || defined(RT) || defined(CN) || defined(CT)
	OCOPYB_OPERATION(min_l, min_jj, b, ldb, alpha[0],  alpha[1], ls, jjs, sb + min_l * (jjs - js));
#else
	OCOPYB_OPERATION(min_l, min_jj, b, ldb, alpha[0], -alpha[1], ls, jjs, sb + min_l * (jjs - js));
#endif

	STOP_RPCC(outercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_jj, min_l, ALPHA5, ALPHA6,
			 sa, sb + min_l * (jjs - js), c, ldc, m_from, jjs);

	STOP_RPCC(kernelcost);

      }

      for(is = m_from + min_i; is < m_to; is += min_i){
	min_i = m_to - is;
	if (min_i >= GEMM3M_P * 2) {
	  min_i = GEMM3M_P;
	} else
	  if (min_i > GEMM3M_P) {
	    min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	  }

	START_RPCC();

	ICOPYB_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	STOP_RPCC(innercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_j, min_l, ALPHA5, ALPHA6, sa, sb, c, ldc, is, js);

	STOP_RPCC(kernelcost);
      }

      min_i = m_to - m_from;
      if (min_i >= GEMM3M_P * 2) {
	min_i = GEMM3M_P;
      } else {
	if (min_i > GEMM3M_P) {
	  min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	}
      }

      START_RPCC();

      ICOPYR_OPERATION(min_l, min_i, a, lda, ls, m_from, sa);

      STOP_RPCC(innercost);

      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;
	if (min_jj > GEMM3M_UNROLL_N) min_jj = GEMM3M_UNROLL_N;

	START_RPCC();

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	OCOPYR_OPERATION(min_l, min_jj, b, ldb, alpha[0],  alpha[1], ls, jjs, sb + min_l * (jjs - js));
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
	OCOPYR_OPERATION(min_l, min_jj, b, ldb, alpha[0], -alpha[1], ls, jjs, sb + min_l * (jjs - js));
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	OCOPYI_OPERATION(min_l, min_jj, b, ldb, alpha[0],  alpha[1], ls, jjs, sb + min_l * (jjs - js));
#else
	OCOPYI_OPERATION(min_l, min_jj, b, ldb, alpha[0], -alpha[1], ls, jjs, sb + min_l * (jjs - js));
#endif

	STOP_RPCC(outercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_jj, min_l, ALPHA11, ALPHA12,
			 sa, sb + min_l * (jjs - js), c, ldc, m_from, jjs);

	STOP_RPCC(kernelcost);

      }

      for(is = m_from + min_i; is < m_to; is += min_i){
	min_i = m_to - is;
	if (min_i >= GEMM3M_P * 2) {
	  min_i = GEMM3M_P;
	} else
	  if (min_i > GEMM3M_P) {
	    min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	  }

	START_RPCC();

	ICOPYR_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	STOP_RPCC(innercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_j, min_l, ALPHA11, ALPHA12, sa, sb, c, ldc, is, js);

	STOP_RPCC(kernelcost);

      }

      min_i = m_to - m_from;
      if (min_i >= GEMM3M_P * 2) {
	min_i = GEMM3M_P;
      } else {
	if (min_i > GEMM3M_P) {
	  min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	}
      }

      START_RPCC();

      ICOPYI_OPERATION(min_l, min_i, a, lda, ls, m_from, sa);

      STOP_RPCC(innercost);

      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;
	if (min_jj > GEMM3M_UNROLL_N) min_jj = GEMM3M_UNROLL_N;

	START_RPCC();

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	OCOPYI_OPERATION(min_l, min_jj, b, ldb, alpha[0],  alpha[1], ls, jjs, sb + min_l * (jjs - js));
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
	OCOPYI_OPERATION(min_l, min_jj, b, ldb, alpha[0], -alpha[1], ls, jjs, sb + min_l * (jjs - js));
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	OCOPYR_OPERATION(min_l, min_jj, b, ldb, alpha[0],  alpha[1], ls, jjs, sb + min_l * (jjs - js));
#else
	OCOPYR_OPERATION(min_l, min_jj, b, ldb, alpha[0], -alpha[1], ls, jjs, sb + min_l * (jjs - js));
#endif

	STOP_RPCC(outercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_jj, min_l, ALPHA17, ALPHA18,
			 sa, sb + min_l * (jjs - js), c, ldc, m_from, jjs);

	STOP_RPCC(kernelcost);

      }

      for(is = m_from + min_i; is < m_to; is += min_i){
	min_i = m_to - is;
	if (min_i >= GEMM3M_P * 2) {
	  min_i = GEMM3M_P;
	} else
	  if (min_i > GEMM3M_P) {
	    min_i = ((min_i / 2 + GEMM3M_UNROLL_M - 1)/GEMM3M_UNROLL_M) * GEMM3M_UNROLL_M;
	  }

	START_RPCC();

	ICOPYI_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	STOP_RPCC(innercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_j, min_l, ALPHA17, ALPHA18, sa, sb, c, ldc, is, js);

	STOP_RPCC(kernelcost);

      }

    } /* end of js */
  } /* end of ls */


#ifdef TIMING
  total = (double)outercost + (double)innercost + (double)kernelcost;

  printf( "Copy A : %5.2f Copy  B: %5.2f  Kernel : %5.2f\n",
	   innercost / total * 100., outercost / total * 100.,
	  kernelcost / total * 100.);

  printf( " Total %10.3f%%  %10.3f MFlops\n",
	  ((double)(m_to - m_from) * (double)(n_to - n_from) * (double)k) / (double)kernelcost / 2 * 100,
	  2400. * (2. * (double)(m_to - m_from) * (double)(n_to - n_from) * (double)k) / (double)kernelcost);
#endif

  return 0;
}
