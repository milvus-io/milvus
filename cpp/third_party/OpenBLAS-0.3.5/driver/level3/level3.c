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

/* This file is a template for level 3 operation */

#ifndef BETA_OPERATION
#if !defined(XDOUBLE) || !defined(QUAD_PRECISION)
#ifndef COMPLEX
#define BETA_OPERATION(M_FROM, M_TO, N_FROM, N_TO, BETA, C, LDC) \
	GEMM_BETA((M_TO) - (M_FROM), (N_TO - N_FROM), 0, \
		  BETA[0], NULL, 0, NULL, 0, \
		  (FLOAT *)(C) + ((M_FROM) + (N_FROM) * (LDC)) * COMPSIZE, LDC)
#else
#define BETA_OPERATION(M_FROM, M_TO, N_FROM, N_TO, BETA, C, LDC) \
	GEMM_BETA((M_TO) - (M_FROM), (N_TO - N_FROM), 0, \
		  BETA[0], BETA[1], NULL, 0, NULL, 0, \
		  (FLOAT *)(C) + ((M_FROM) + (N_FROM) * (LDC)) * COMPSIZE, LDC)
#endif
#else
#define BETA_OPERATION(M_FROM, M_TO, N_FROM, N_TO, BETA, C, LDC) \
	GEMM_BETA((M_TO) - (M_FROM), (N_TO - N_FROM), 0, \
		  BETA, NULL, 0, NULL, 0, \
		  (FLOAT *)(C) + ((M_FROM) + (N_FROM) * (LDC)) * COMPSIZE, LDC)
#endif
#endif

#ifndef ICOPY_OPERATION
#if defined(NN) || defined(NT) || defined(NC) || defined(NR) || \
    defined(RN) || defined(RT) || defined(RC) || defined(RR)
#define ICOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ITCOPY(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER);
#else
#define ICOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_INCOPY(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER);
#endif
#endif

#ifndef OCOPY_OPERATION
#if defined(NN) || defined(TN) || defined(CN) || defined(RN) || \
    defined(NR) || defined(TR) || defined(CR) || defined(RR)
#define OCOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ONCOPY(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER);
#else
#define OCOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_OTCOPY(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER);
#endif
#endif

#ifndef KERNEL_FUNC
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define KERNEL_FUNC	GEMM_KERNEL_N
#endif
#if defined(CN) || defined(CT) || defined(RN) || defined(RT)
#define KERNEL_FUNC	GEMM_KERNEL_L
#endif
#if defined(NC) || defined(TC) || defined(NR) || defined(TR)
#define KERNEL_FUNC	GEMM_KERNEL_R
#endif
#if defined(CC) || defined(CR) || defined(RC) || defined(RR)
#define KERNEL_FUNC	GEMM_KERNEL_B
#endif
#endif

#ifndef KERNEL_OPERATION
#if !defined(XDOUBLE) || !defined(QUAD_PRECISION)
#ifndef COMPLEX
#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA[0], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#else
#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA[0], ALPHA[1], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#endif
#else
#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA, SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#endif
#endif

#ifndef FUSED_KERNEL_OPERATION
#if defined(NN) || defined(TN) || defined(CN) || defined(RN) || \
    defined(NR) || defined(TR) || defined(CR) || defined(RR)
#ifndef COMPLEX
#define FUSED_KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, B, LDB, C, LDC, I, J, L) \
	FUSED_GEMM_KERNEL_N(M, N, K, ALPHA[0], SA, SB, \
	(FLOAT *)(B) + ((L) + (J) * LDB) * COMPSIZE, LDB, (FLOAT *)(C) + ((I) + (J) * LDC) * COMPSIZE, LDC)
#else
#define FUSED_KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, B, LDB, C, LDC, I, J, L) \
	FUSED_GEMM_KERNEL_N(M, N, K, ALPHA[0], ALPHA[1], SA, SB, \
	(FLOAT *)(B) + ((L) + (J) * LDB) * COMPSIZE, LDB, (FLOAT *)(C) + ((I) + (J) * LDC) * COMPSIZE, LDC)

#endif
#else
#ifndef COMPLEX
#define FUSED_KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, B, LDB, C, LDC, I, J, L) \
	FUSED_GEMM_KERNEL_T(M, N, K, ALPHA[0], SA, SB, \
	(FLOAT *)(B) + ((J) + (L) * LDB) * COMPSIZE, LDB, (FLOAT *)(C) + ((I) + (J) * LDC) * COMPSIZE, LDC)
#else
#define FUSED_KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, B, LDB, C, LDC, I, J, L) \
	FUSED_GEMM_KERNEL_T(M, N, K, ALPHA[0], ALPHA[1], SA, SB, \
	(FLOAT *)(B) + ((J) + (L) * LDB) * COMPSIZE, LDB, (FLOAT *)(C) + ((I) + (J) * LDC) * COMPSIZE, LDC)
#endif
#endif
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

#ifdef TIMING
#define START_RPCC()		rpcc_counter = rpcc()
#define STOP_RPCC(COUNTER)	COUNTER  += rpcc() - rpcc_counter
#else
#define START_RPCC()
#define STOP_RPCC(COUNTER)
#endif

int CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n,
		  XFLOAT *sa, XFLOAT *sb, BLASLONG dummy){
  BLASLONG k, lda, ldb, ldc;
  FLOAT *alpha, *beta;
  FLOAT *a, *b, *c;
  BLASLONG m_from, m_to, n_from, n_to;

  BLASLONG ls, is, js;
  BLASLONG min_l, min_i, min_j;
#if !defined(FUSED_GEMM) || defined(TIMING)
  BLASLONG jjs, min_jj;
#endif

  BLASLONG l1stride, gemm_p, l2size;

#if defined(XDOUBLE) && defined(QUAD_PRECISION)
  xidouble xalpha;
#endif

#ifdef TIMING
  unsigned long long rpcc_counter;
  unsigned long long innercost  = 0;
  unsigned long long outercost  = 0;
  unsigned long long kernelcost = 0;
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
#if !defined(XDOUBLE) || !defined(QUAD_PRECISION)
#ifndef COMPLEX
    if (beta[0] != ONE
#else
    if ((beta[0] != ONE) || (beta[1] != ZERO)
#endif
#else
    if (((beta[0].x[1] != 0x3fff000000000000UL) || beta[0].x[0] != 0)
#ifdef COMPLEX
	&&(((beta[1].x[0] | beta[1].x[1]) << 1) != 0)
#endif
#endif
	) {
#if defined(XDOUBLE) && defined(QUAD_PRECISION)
	  xidouble xbeta;

	  qtox(&xbeta, beta);
#endif
	  BETA_OPERATION(m_from, m_to, n_from, n_to, beta, c, ldc);
	}
  }

  if ((k == 0) || (alpha == NULL)) return 0;

#if !defined(XDOUBLE) || !defined(QUAD_PRECISION)
  if ( alpha[0] == ZERO
#ifdef COMPLEX
      && alpha[1] == ZERO
#endif
	 ) return 0; 
#else
  if (((alpha[0].x[0] | alpha[0].x[1]
#ifdef COMPLEX
       | alpha[1].x[0] | alpha[1].x[1]
#endif
       ) << 1) == 0) return 0;
#endif

#if defined(XDOUBLE)  && defined(QUAD_PRECISION)
  qtox(&xalpha, alpha);
#endif

  l2size = GEMM_P * GEMM_Q;

#if 0
  fprintf(stderr, "GEMM(Single): M_from : %ld  M_to : %ld  N_from : %ld  N_to : %ld  k : %ld\n", m_from, m_to, n_from, n_to, k);
  fprintf(stderr, "GEMM(Single):: P = %4ld  Q = %4ld  R = %4ld\n", (BLASLONG)GEMM_P, (BLASLONG)GEMM_Q, (BLASLONG)GEMM_R);
	//  fprintf(stderr, "GEMM: SA .. %p  SB .. %p\n", sa, sb);

	//  fprintf(stderr, "A = %p  B = %p  C = %p\n\tlda = %ld  ldb = %ld ldc = %ld\n", a, b, c, lda, ldb, ldc);
#endif

#ifdef TIMING
  innercost = 0;
  outercost = 0;
  kernelcost = 0;
#endif

  for(js = n_from; js < n_to; js += GEMM_R){
    min_j = n_to - js;
    if (min_j > GEMM_R) min_j = GEMM_R;

    for(ls = 0; ls < k; ls += min_l){

      min_l = k - ls;

      if (min_l >= GEMM_Q * 2) {
	// gemm_p = GEMM_P;
	min_l  = GEMM_Q;
      } else {
	if (min_l > GEMM_Q) {
	  min_l = ((min_l / 2 + GEMM_UNROLL_M - 1)/GEMM_UNROLL_M) * GEMM_UNROLL_M;
	}
	gemm_p = ((l2size / min_l + GEMM_UNROLL_M - 1)/GEMM_UNROLL_M) * GEMM_UNROLL_M;
	while (gemm_p * min_l > l2size) gemm_p -= GEMM_UNROLL_M;
      }

      /* First, we have to move data A to L2 cache */
      min_i = m_to - m_from;
      l1stride = 1;

      if (min_i >= GEMM_P * 2) {
	min_i = GEMM_P;
      } else {
	if (min_i > GEMM_P) {
	  min_i = ((min_i / 2 + GEMM_UNROLL_M - 1)/GEMM_UNROLL_M) * GEMM_UNROLL_M;
	} else {
	  l1stride = 0;
	}
      }

      START_RPCC();

      ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_from, sa);

      STOP_RPCC(innercost);

#if defined(FUSED_GEMM) && !defined(TIMING)

      FUSED_KERNEL_OPERATION(min_i, min_j, min_l, alpha,
			     sa, sb, b, ldb, c, ldc, m_from, js, ls);


#else
      for(jjs = js; jjs < js + min_j; jjs += min_jj){
	min_jj = min_j + js - jjs;

        if (min_jj >= 3*GEMM_UNROLL_N) min_jj = 3*GEMM_UNROLL_N;
        else
        	if (min_jj >= 2*GEMM_UNROLL_N) min_jj = 2*GEMM_UNROLL_N;
        	else
          		if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;



	START_RPCC();

	OCOPY_OPERATION(min_l, min_jj, b, ldb, ls, jjs,
			sb + min_l * (jjs - js) * COMPSIZE * l1stride);

	STOP_RPCC(outercost);

	START_RPCC();

#if !defined(XDOUBLE)  || !defined(QUAD_PRECISION)
	KERNEL_OPERATION(min_i, min_jj, min_l, alpha,
			 sa, sb + min_l * (jjs - js)  * COMPSIZE * l1stride, c, ldc, m_from, jjs);
#else
	KERNEL_OPERATION(min_i, min_jj, min_l, (void *)&xalpha,
			 sa, sb + min_l * (jjs - js)  * COMPSIZE * l1stride, c, ldc, m_from, jjs);
#endif

	STOP_RPCC(kernelcost);
      }
#endif

      for(is = m_from + min_i; is < m_to; is += min_i){
	min_i = m_to - is;

	if (min_i >= GEMM_P * 2) {
	  min_i = GEMM_P;
	} else
	  if (min_i > GEMM_P) {
	    min_i = ((min_i / 2 + GEMM_UNROLL_M - 1)/GEMM_UNROLL_M) * GEMM_UNROLL_M;
	  }

	START_RPCC();

	ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	STOP_RPCC(innercost);

	START_RPCC();

#if !defined(XDOUBLE)  || !defined(QUAD_PRECISION)
	KERNEL_OPERATION(min_i, min_j, min_l, alpha, sa, sb, c, ldc, is, js);
#else
	KERNEL_OPERATION(min_i, min_j, min_l, (void *)&xalpha, sa, sb, c, ldc, is, js);
#endif

	STOP_RPCC(kernelcost);

      } /* end of is */
    } /* end of js */
  } /* end of ls */


#ifdef TIMING
  total = (double)outercost + (double)innercost + (double)kernelcost;

  printf( "Copy A : %5.2f Copy  B: %5.2f  Kernel : %5.2f  kernel Effi. : %5.2f Total Effi. : %5.2f\n",
	   innercost / total * 100., outercost / total * 100.,
	  kernelcost / total * 100.,
	  (double)(m_to - m_from) * (double)(n_to - n_from) * (double)k / (double)kernelcost * 100. * (double)COMPSIZE / 2.,
	  (double)(m_to - m_from) * (double)(n_to - n_from) * (double)k / total * 100. * (double)COMPSIZE / 2.);

#endif

  return 0;
}
