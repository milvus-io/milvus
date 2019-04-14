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

#ifndef KERNEL_OPERATION
#ifndef COMPLEX
#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA[0], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC, (X) - (Y))
#else
#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y) \
	KERNEL_FUNC(M, N, K, ALPHA[0], ALPHA[1], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC, (X) - (Y))
#endif
#endif

#ifndef ICOPY_OPERATION
#ifndef TRANS
#define ICOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ITCOPY(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER);
#else
#define ICOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_INCOPY(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER);
#endif
#endif

#ifndef OCOPY_OPERATION
#ifdef TRANS
#define OCOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ONCOPY(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER);
#else
#define OCOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_OTCOPY(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER);
#endif
#endif

#ifndef M
#define M	args -> n
#endif

#ifndef N
#define N	args -> n
#endif

#ifndef K
#define K	args -> k
#endif

#ifndef A
#define A	args -> a
#endif

#ifndef C
#define C	args -> c
#endif

#ifndef LDA
#define LDA	args -> lda
#endif

#ifndef LDC
#define LDC	args -> ldc
#endif

#ifdef TIMING
#define START_RPCC()		rpcc_counter = rpcc()
#define STOP_RPCC(COUNTER)	COUNTER  += rpcc() - rpcc_counter
#else
#define START_RPCC()
#define STOP_RPCC(COUNTER)
#endif

int CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG dummy) {

  BLASLONG m_from, m_to, n_from, n_to, k, lda, ldc;
  FLOAT *a, *c, *alpha, *beta;

  BLASLONG ls, is, js;
  BLASLONG min_l, min_i, min_j;
  BLASLONG jjs, min_jj;
  BLASLONG m_start, m_end;

  int shared = ((GEMM_UNROLL_M == GEMM_UNROLL_N) && !HAVE_EX_L2);

  FLOAT *aa;

#ifdef TIMING
  unsigned long long rpcc_counter;
  unsigned long long innercost  = 0;
  unsigned long long outercost  = 0;
  unsigned long long kernelcost = 0;
  double total;
#endif

  k = K;

  a = (FLOAT *)A;
  c = (FLOAT *)C;

  lda = LDA;
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
#if !defined(COMPLEX) || defined(HERK)
    if (beta[0] != ONE)
#else
    if ((beta[0] != ONE) || (beta[1] != ZERO))
#endif
      syrk_beta(m_from, m_to, n_from, n_to, beta, c, ldc);
  }

  if ((k == 0) || (alpha == NULL)) return 0;

  if (alpha[0] == ZERO
#if defined(COMPLEX) && !defined(HERK)
      && alpha[1] == ZERO
#endif
      ) return 0;

#if 0
  fprintf(stderr, "m_from : %ld m_to : %ld n_from : %ld n_to : %ld\n",
	  m_from, m_to, n_from, n_to);
#endif

  for(js = n_from; js < n_to; js += GEMM_R){
    min_j = n_to - js;
    if (min_j > GEMM_R) min_j = GEMM_R;

#ifndef LOWER
    m_start = m_from;
    m_end   = js + min_j;
    if (m_end > m_to) m_end = m_to;
#else
    m_start = m_from;
    m_end   = m_to;
    if (m_start < js) m_start = js;
#endif

    for(ls = 0; ls < k; ls += min_l){
      min_l = k - ls;
      if (min_l >= GEMM_Q * 2) {
	min_l = GEMM_Q;
      } else
	if (min_l > GEMM_Q) {
	  min_l = (min_l + 1) / 2;
	}

      min_i = m_end - m_start;

      if (min_i >= GEMM_P * 2) {
	min_i = GEMM_P;
      } else
	if (min_i > GEMM_P) {
	  min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	}

#ifndef LOWER

      if (m_end >= js) {

	aa = sb + min_l * MAX(m_start - js, 0) * COMPSIZE;
	if (!shared) aa = sa;

	for(jjs = MAX(m_start, js); jjs < js + min_j; jjs += min_jj){
	  min_jj = js + min_j - jjs;
	  if (min_jj > GEMM_UNROLL_MN) min_jj = GEMM_UNROLL_MN;

	  if (!shared && (jjs - MAX(m_start, js) < min_i)) {
	    START_RPCC();

	    ICOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs, sa + min_l * (jjs - js) * COMPSIZE);

	    STOP_RPCC(innercost);
	  }

	  START_RPCC();

	  OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs, sb + min_l * (jjs - js) * COMPSIZE);

	  STOP_RPCC(outercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_jj, min_l, alpha, aa, sb + min_l * (jjs - js)  * COMPSIZE, c, ldc, MAX(m_start, js), jjs);

	  STOP_RPCC(kernelcost);
	}

	for(is = MAX(m_start, js) + min_i; is < m_end; is += min_i){
	  min_i = m_end - is;
	  if (min_i >= GEMM_P * 2) {
	    min_i = GEMM_P;
	  } else
	    if (min_i > GEMM_P) {
	      min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	    }

	  aa = sb + min_l * (is - js)  * COMPSIZE;

	  if (!shared) {

	    START_RPCC();

	    ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	    STOP_RPCC(innercost);

	    aa = sa;
	  }

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_j, min_l, alpha, aa, sb, c, ldc, is, js);

	  STOP_RPCC(kernelcost);

	}

      }

      if (m_start < js) {

	if (m_end < js) {

	  START_RPCC();

	  ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_start, sa);

	  STOP_RPCC(innercost);

	  for(jjs = js; jjs < js + min_j; jjs += GEMM_UNROLL_MN){
	    min_jj = min_j + js - jjs;
	    if (min_jj > GEMM_UNROLL_MN) min_jj = GEMM_UNROLL_MN;

	    START_RPCC();

	    OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs, sb + min_l * (jjs - js) * COMPSIZE);

	    STOP_RPCC(outercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_jj, min_l, alpha, sa, sb + min_l * (jjs - js)  * COMPSIZE, c, ldc, m_start, jjs);

	  STOP_RPCC(kernelcost);

	  }
	} else {
	  min_i = 0;
	}

	for(is = m_start + min_i; is < MIN(m_end, js); is += min_i){

	  min_i = MIN(m_end, js)- is;
	  if (min_i >= GEMM_P * 2) {
	    min_i = GEMM_P;
	  } else
	    if (min_i > GEMM_P) {
	      min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	    }

	  START_RPCC();

	  ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	  STOP_RPCC(innercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_j, min_l, alpha, sa, sb, c, ldc, is, js);

	  STOP_RPCC(kernelcost);

	}
      }

#else

      if (m_start < js + min_j) {

	aa = sb + min_l * (m_start - js) * COMPSIZE;

	if (!shared) {

	  START_RPCC();

	  ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_start, sa);

	  STOP_RPCC(innercost);

	}

	START_RPCC();

	OCOPY_OPERATION(min_l, (shared? (min_i) : MIN(min_i, min_j + js - m_start)), a, lda, ls, m_start, aa);

	STOP_RPCC(outercost);

	START_RPCC();

	KERNEL_OPERATION(min_i, MIN(min_i, min_j + js - m_start), min_l, alpha, (shared? (aa) : (sa)), aa, c, ldc, m_start, m_start);

	STOP_RPCC(kernelcost);

	for(jjs = js; jjs < m_start; jjs += GEMM_UNROLL_N){
	  min_jj = m_start - jjs;
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

	  START_RPCC();

	  OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs, sb + min_l * (jjs - js) * COMPSIZE);

	  STOP_RPCC(outercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_jj, min_l, alpha, (shared? (aa) : (sa)), sb + min_l * (jjs - js)  * COMPSIZE, c, ldc, m_start, jjs);

	  STOP_RPCC(kernelcost);

	}

	for(is = m_start + min_i; is < m_end; is += min_i){

	  min_i = m_end - is;

	  if (min_i >= GEMM_P * 2) {
	    min_i = GEMM_P;
	  } else
	    if (min_i > GEMM_P) {
	      min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	    }

	  if (is  < js + min_j) {

	    if (!shared) {
	      START_RPCC();

	      ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	      STOP_RPCC(innercost);
	    }

	    aa = sb + min_l * (is - js) * COMPSIZE;

	    START_RPCC();

	    OCOPY_OPERATION(min_l, (shared? (min_i) : MIN(min_i, min_j - is + js)), a, lda, ls, is, aa);

	    STOP_RPCC(outercost);

	    START_RPCC();

	    KERNEL_OPERATION(min_i, MIN(min_i, min_j - is + js), min_l, alpha,  (shared? (aa) : (sa)), aa,  c, ldc, is, is);

	    STOP_RPCC(kernelcost);

	    START_RPCC();

	    KERNEL_OPERATION(min_i, is - js, min_l, alpha, (shared? (aa) : (sa)), sb,  c, ldc, is, js);

	    STOP_RPCC(kernelcost);

	  } else {

	    START_RPCC();

	    ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	    STOP_RPCC(innercost);

	    START_RPCC();

	    KERNEL_OPERATION(min_i, min_j, min_l, alpha, sa, sb,  c, ldc, is, js);

	    STOP_RPCC(kernelcost);

	  }

	}

      } else {

	START_RPCC();

	ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_start, sa);

	STOP_RPCC(innercost);

	for(jjs = js; jjs < min_j; jjs += GEMM_UNROLL_N){
	  min_jj = min_j - jjs;
	  if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

	  START_RPCC();

	  OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs, sb + min_l * (jjs - js) * COMPSIZE);

	  STOP_RPCC(outercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_jj, min_l, alpha, sa, sb + min_l * (jjs - js)  * COMPSIZE, c, ldc, m_start, jjs);

	  STOP_RPCC(kernelcost);

	}

	for(is = m_start + min_i; is < m_end; is += min_i){

	  min_i = m_end - is;

	  if (min_i >= GEMM_P * 2) {
	    min_i = GEMM_P;
	  } else
	    if (min_i > GEMM_P) {
	      min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	    }

	  START_RPCC();

	  ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

	  STOP_RPCC(innercost);

	  START_RPCC();

	  KERNEL_OPERATION(min_i, min_j, min_l, alpha, sa, sb,  c, ldc, is, js);

	  STOP_RPCC(kernelcost);

	}
      }
#endif
    }
  }

#ifdef TIMING
  total = (double)outercost + (double)innercost + (double)kernelcost;

  printf( "Copy A : %5.2f Copy  B: %5.2f  Kernel : %5.2f  kernel Effi. : %5.2f Total Effi. : %5.2f\n",
	   innercost / total * 100., outercost / total * 100., kernelcost / total * 100.,
	  (double)(m_to - m_from) * (double)(n_to - n_from) * (double)k / (double)kernelcost * 100. * (double)COMPSIZE / (double)DNUMOPT,
	  (double)(m_to - m_from) * (double)(n_to - n_from) * (double)k / total * 100. * (double)COMPSIZE / (double)DNUMOPT);

#endif

  return 0;
}
