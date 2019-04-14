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

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 8
#endif

#ifndef DIVIDE_RATE
#define DIVIDE_RATE 2
#endif

#ifndef SWITCH_RATIO
#define SWITCH_RATIO 2
#endif

//The array of job_t may overflow the stack.
//Instead, use malloc to alloc job_t.
#if MAX_CPU_NUMBER > BLAS3_MEM_ALLOC_THRESHOLD
#define USE_ALLOC_HEAP
#endif

#ifndef SYRK_LOCAL
#if   !defined(LOWER) && !defined(TRANS)
#define SYRK_LOCAL    SYRK_UN
#elif !defined(LOWER) &&  defined(TRANS)
#define SYRK_LOCAL    SYRK_UT
#elif  defined(LOWER) && !defined(TRANS)
#define SYRK_LOCAL    SYRK_LN
#else
#define SYRK_LOCAL    SYRK_LT
#endif
#endif

typedef struct {
#if __STDC_VERSION__ >= 201112L
_Atomic
#else 
  volatile
#endif
   BLASLONG working[MAX_CPU_NUMBER][CACHE_LINE_SIZE * DIVIDE_RATE];
} job_t;


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

#ifndef A
#define A	args -> a
#endif
#ifndef LDA
#define LDA	args -> lda
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

#undef TIMING

#ifdef TIMING
#define START_RPCC()		rpcc_counter = rpcc()
#define STOP_RPCC(COUNTER)	COUNTER  += rpcc() - rpcc_counter
#else
#define START_RPCC()
#define STOP_RPCC(COUNTER)
#endif

static int inner_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos){

  FLOAT *buffer[DIVIDE_RATE];

  BLASLONG k, lda, ldc;
  BLASLONG m_from, m_to, n_from, n_to;

  FLOAT *alpha, *beta;
  FLOAT *a, *c;
  job_t *job = (job_t *)args -> common;
  BLASLONG xxx, bufferside;

  BLASLONG ls, min_l, jjs, min_jj;
  BLASLONG is, min_i, div_n;

  BLASLONG i, current;
#ifdef LOWER
  BLASLONG start_i;
#endif

#ifdef TIMING
  BLASLONG rpcc_counter;
  BLASLONG copy_A = 0;
  BLASLONG copy_B = 0;
  BLASLONG kernel = 0;
  BLASLONG waiting1 = 0;
  BLASLONG waiting2 = 0;
  BLASLONG waiting3 = 0;
  BLASLONG waiting6[MAX_CPU_NUMBER];
  BLASLONG ops    = 0;

  for (i = 0; i < args -> nthreads; i++) waiting6[i] = 0;
#endif

  k = K;

  a = (FLOAT *)A;
  c = (FLOAT *)C;

  lda = LDA;
  ldc = LDC;

  alpha = (FLOAT *)args -> alpha;
  beta  = (FLOAT *)args -> beta;

  m_from = 0;
  m_to   = N;

  /* Global Range */
  n_from = 0;
  n_to   = N;

  if (range_n) {
    m_from = range_n[mypos + 0];
    m_to   = range_n[mypos + 1];

    n_from = range_n[0];
    n_to   = range_n[args -> nthreads];
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
  fprintf(stderr, "Thread[%ld]  m_from : %ld m_to : %ld n_from : %ld n_to : %ld\n",  mypos, m_from, m_to, n_from, n_to);
#endif

  div_n = (((m_to - m_from + DIVIDE_RATE - 1) / DIVIDE_RATE + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;

  buffer[0] = sb;
  for (i = 1; i < DIVIDE_RATE; i++) {
    buffer[i] = buffer[i - 1] + GEMM_Q * div_n * COMPSIZE;
  }

  for(ls = 0; ls < k; ls += min_l){

    min_l = k - ls;
    if (min_l >= GEMM_Q * 2) {
      min_l  = GEMM_Q;
    } else {
      if (min_l > GEMM_Q) min_l = (min_l + 1) / 2;
    }

    min_i = m_to - m_from;

    if (min_i >= GEMM_P * 2) {
      min_i = GEMM_P;
    } else {
      if (min_i > GEMM_P) {
	min_i = ((min_i / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
      }
    }

#ifdef LOWER
    xxx = (m_to - m_from - min_i) % GEMM_P;

    if (xxx) min_i -= GEMM_P - xxx;
#endif

    START_RPCC();

#ifndef LOWER
    ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_from, sa);
#else
    ICOPY_OPERATION(min_l, min_i, a, lda, ls, m_to - min_i, sa);
#endif

    STOP_RPCC(copy_A);

    div_n = (((m_to - m_from + DIVIDE_RATE - 1) / DIVIDE_RATE + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;

    for (xxx = m_from, bufferside = 0; xxx < m_to; xxx += div_n, bufferside ++) {

      START_RPCC();

      /* Make sure if no one is using buffer */
#ifndef LOWER
      for (i = 0; i < mypos; i++)
#else
      for (i = mypos + 1; i < args -> nthreads; i++)
#endif
	while (job[mypos].working[i][CACHE_LINE_SIZE * bufferside]) {YIELDING;};

      STOP_RPCC(waiting1);

#ifndef LOWER

      for(jjs = xxx; jjs < MIN(m_to, xxx + div_n); jjs += min_jj){

	min_jj = MIN(m_to, xxx + div_n) - jjs;

	if (xxx == m_from) {
	  if (min_jj > min_i) min_jj = min_i;
	} else {
	  if (min_jj > GEMM_UNROLL_MN) min_jj = GEMM_UNROLL_MN;
	}

	START_RPCC();

	OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs,
			buffer[bufferside] + min_l * (jjs - xxx) * COMPSIZE);

	STOP_RPCC(copy_B);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_jj, min_l, alpha,
			 sa, buffer[bufferside] + min_l * (jjs - xxx) * COMPSIZE,
			 c, ldc, m_from, jjs);

	STOP_RPCC(kernel);

#ifdef TIMING
	  ops += 2 * min_i * min_jj * min_l;
#endif

      }

#else

      for(jjs = xxx; jjs < MIN(m_to, xxx + div_n); jjs += min_jj){

	min_jj = MIN(m_to, xxx + div_n) - jjs;

	if (min_jj > GEMM_UNROLL_MN) min_jj = GEMM_UNROLL_MN;

	START_RPCC();

	OCOPY_OPERATION(min_l, min_jj, a, lda, ls, jjs,
			buffer[bufferside] + min_l * (jjs - xxx) * COMPSIZE);

	STOP_RPCC(copy_B);

	START_RPCC();

	KERNEL_OPERATION(min_i, min_jj, min_l, alpha,
			 sa, buffer[bufferside] + min_l * (jjs - xxx) * COMPSIZE,
			 c, ldc, m_to - min_i, jjs);

	STOP_RPCC(kernel);

#ifdef TIMING
	  ops += 2 * min_i * min_jj * min_l;
#endif

      }

#endif

#ifndef LOWER
      for (i = 0; i <= mypos; i++)
#else
      for (i = mypos; i < args -> nthreads; i++)
#endif
	job[mypos].working[i][CACHE_LINE_SIZE * bufferside] = (BLASLONG)buffer[bufferside];

      WMB;
    }


#ifndef LOWER
    current = mypos + 1;
    while (current < args -> nthreads) {
#else
    current = mypos - 1;
    while (current >= 0) {
#endif

	div_n = (((range_n[current + 1]  - range_n[current] + DIVIDE_RATE - 1) / DIVIDE_RATE + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
 
	for (xxx = range_n[current], bufferside = 0; xxx < range_n[current + 1]; xxx += div_n, bufferside ++) {

	  START_RPCC();

	  /* thread has to wait */
	  while(job[current].working[mypos][CACHE_LINE_SIZE * bufferside] == 0) {YIELDING;};

	  STOP_RPCC(waiting2);

	  START_RPCC();

#ifndef LOWER
	  KERNEL_OPERATION(min_i, MIN(range_n[current + 1]  - xxx,  div_n), min_l, alpha,
			   sa, (FLOAT *)job[current].working[mypos][CACHE_LINE_SIZE * bufferside],
			   c, ldc,
			   m_from,
			   xxx);
#else
	  KERNEL_OPERATION(min_i, MIN(range_n[current + 1]  - xxx,  div_n), min_l, alpha,
			   sa, (FLOAT *)job[current].working[mypos][CACHE_LINE_SIZE * bufferside],
			   c, ldc,
			   m_to - min_i,
			   xxx);
#endif

	  STOP_RPCC(kernel);
#ifdef TIMING
	  ops += 2 * min_i * MIN(range_n[current + 1]  - xxx,  div_n) * min_l;
#endif

	  if (m_to - m_from == min_i) {
	    job[current].working[mypos][CACHE_LINE_SIZE * bufferside] &= 0;
	  }
	}

#ifndef LOWER
	current ++;
#else
	current --;
#endif
    }

#ifndef LOWER
    for(is = m_from + min_i; is < m_to; is += min_i){
      min_i = m_to - is;
#else
    start_i = min_i;

    for(is = m_from; is < m_to - start_i; is += min_i){
      min_i = m_to - start_i - is;
#endif

      if (min_i >= GEMM_P * 2) {
	min_i = GEMM_P;
      } else
	if (min_i > GEMM_P) {
	  min_i = (((min_i + 1) / 2 + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;
	}

      START_RPCC();

      ICOPY_OPERATION(min_l, min_i, a, lda, ls, is, sa);

      STOP_RPCC(copy_A);

      current = mypos;

      do {

	div_n = (((range_n[current + 1]  - range_n[current] + DIVIDE_RATE - 1) / DIVIDE_RATE + GEMM_UNROLL_MN - 1)/GEMM_UNROLL_MN) * GEMM_UNROLL_MN;

	for (xxx = range_n[current], bufferside = 0; xxx < range_n[current + 1]; xxx += div_n, bufferside ++) {

	  START_RPCC();

	  KERNEL_OPERATION(min_i, MIN(range_n[current + 1] - xxx, div_n), min_l, alpha,
			   sa, (FLOAT *)job[current].working[mypos][CACHE_LINE_SIZE * bufferside],
			   c, ldc, is, xxx);

	  STOP_RPCC(kernel);

#ifdef TIMING
	  ops += 2 * min_i * MIN(range_n[current + 1]  - xxx, div_n) * min_l;
#endif

#ifndef LOWER
	  if (is + min_i >= m_to) {
#else
	  if (is + min_i >= m_to - start_i) {
#endif
	    /* Thread doesn't need this buffer any more */
	    job[current].working[mypos][CACHE_LINE_SIZE * bufferside] &= 0;
	    WMB;
	  }
	}

#ifndef LOWER
	current ++;
      } while (current != args -> nthreads);
#else
	current --;
      } while (current >= 0);
#endif


    }
  }

  START_RPCC();

  for (i = 0; i < args -> nthreads; i++) {
    if (i != mypos) {
      for (xxx = 0; xxx < DIVIDE_RATE; xxx++) {
	while (job[mypos].working[i][CACHE_LINE_SIZE * xxx] ) {YIELDING;};
      }
    }
  }

  STOP_RPCC(waiting3);

#ifdef TIMING
  BLASLONG waiting = waiting1 + waiting2 + waiting3;
  BLASLONG total = copy_A + copy_B + kernel + waiting;

  fprintf(stderr, "GEMM   [%2ld] Copy_A : %6.2f  Copy_B : %6.2f  Wait1 : %6.2f Wait2 : %6.2f Wait3 : %6.2f Kernel : %6.2f",
	  mypos, (double)copy_A /(double)total * 100., (double)copy_B /(double)total * 100.,
	  (double)waiting1 /(double)total * 100.,
	  (double)waiting2 /(double)total * 100.,
	  (double)waiting3 /(double)total * 100.,
	  (double)ops/(double)kernel / 4. * 100.);

#if 0
  fprintf(stderr, "GEMM   [%2ld] Copy_A : %6.2ld  Copy_B : %6.2ld  Wait : %6.2ld\n",
	  mypos, copy_A, copy_B, waiting);

  fprintf(stderr, "Waiting[%2ld] %6.2f %6.2f %6.2f\n",
	  mypos,
	  (double)waiting1/(double)waiting * 100.,
	  (double)waiting2/(double)waiting * 100.,
	  (double)waiting3/(double)waiting * 100.);
#endif
  fprintf(stderr, "\n");
#endif

  return 0;
}

int CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos){

  blas_arg_t newarg;

#ifndef USE_ALLOC_HEAP
  job_t          job[MAX_CPU_NUMBER];
#else
  job_t *        job = NULL;
#endif

  blas_queue_t queue[MAX_CPU_NUMBER];

  BLASLONG range[MAX_CPU_NUMBER + 100];

  BLASLONG num_cpu;

  BLASLONG nthreads = args -> nthreads;

  BLASLONG width, i, j, k;
  BLASLONG n, n_from, n_to;
  int  mode, mask;
  double dnum;

  if ((nthreads  == 1) || (args -> n < nthreads * SWITCH_RATIO)) {
    SYRK_LOCAL(args, range_m, range_n, sa, sb, 0);
    return 0;
  }

#ifndef COMPLEX
#ifdef XDOUBLE
  mode  =  BLAS_XDOUBLE | BLAS_REAL;
  mask  = MAX(QGEMM_UNROLL_M, QGEMM_UNROLL_N) - 1;
#elif defined(DOUBLE)
  mode  =  BLAS_DOUBLE  | BLAS_REAL;
  mask  = DGEMM_UNROLL_MN - 1;
#else
  mode  =  BLAS_SINGLE  | BLAS_REAL;
  mask  = SGEMM_UNROLL_MN - 1;
#endif
#else
#ifdef XDOUBLE
  mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
  mask  = MAX(XGEMM_UNROLL_M, XGEMM_UNROLL_N) - 1;
#elif defined(DOUBLE)
  mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
  mask  = ZGEMM_UNROLL_MN - 1;
#else
  mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
  mask  = CGEMM_UNROLL_MN - 1;
#endif
#endif

  newarg.m        = args -> m;
  newarg.n        = args -> n;
  newarg.k        = args -> k;
  newarg.a        = args -> a;
  newarg.b        = args -> b;
  newarg.c        = args -> c;
  newarg.lda      = args -> lda;
  newarg.ldb      = args -> ldb;
  newarg.ldc      = args -> ldc;
  newarg.alpha    = args -> alpha;
  newarg.beta     = args -> beta;

#ifdef USE_ALLOC_HEAP
  job = (job_t*)malloc(MAX_CPU_NUMBER * sizeof(job_t));
  if(job==NULL){
    fprintf(stderr, "OpenBLAS: malloc failed in %s\n", __func__);
    exit(1);
  }
#endif

  newarg.common   = (void *)job;

  if (!range_n) {
    n_from = 0;
    n_to   = args -> n;
  } else {
    n_from = range_n[0];
    n_to   = range_n[1] - range_n[0];
  }

#ifndef LOWER

  range[MAX_CPU_NUMBER] = n_to - n_from;
  range[0] = 0;
  num_cpu  = 0;
  i        = 0;
  n        = n_to - n_from;

  dnum = (double)n * (double)n /(double)nthreads;

  while (i < n){

    if (nthreads - num_cpu > 1) {

      double di   = (double)i;

      width = (((BLASLONG)((sqrt(di * di + dnum) - di) + mask)/(mask+1)) * (mask+1) );

      if (num_cpu == 0) width = n - (((n - width)/(mask+1)) * (mask+1) );

      if ((width > n - i) || (width < mask)) width = n - i;

    } else {
      width = n - i;
    }

    range[MAX_CPU_NUMBER - num_cpu - 1] = range[MAX_CPU_NUMBER - num_cpu] - width;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = inner_thread;
    queue[num_cpu].args    = &newarg;
    queue[num_cpu].range_m = range_m;

    queue[num_cpu].sa      = NULL;
    queue[num_cpu].sb      = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];

    num_cpu ++;
    i += width;
  }

   for (i = 0; i < num_cpu; i ++) queue[i].range_n = &range[MAX_CPU_NUMBER - num_cpu];

#else

  range[0] = 0;
  num_cpu  = 0;
  i        = 0;
  n        = n_to - n_from;

  dnum = (double)n * (double)n /(double)nthreads;

  while (i < n){

    if (nthreads - num_cpu > 1) {

	double di   = (double)i;

	width = (((BLASLONG)((sqrt(di * di + dnum) - di) + mask)/(mask+1)) * (mask+1));

      if ((width > n - i) || (width < mask)) width = n - i;

    } else {
      width = n - i;
    }

    range[num_cpu + 1] = range[num_cpu] + width;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = inner_thread;
    queue[num_cpu].args    = &newarg;
    queue[num_cpu].range_m = range_m;
    queue[num_cpu].range_n = range;
    queue[num_cpu].sa      = NULL;
    queue[num_cpu].sb      = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];

    num_cpu ++;
    i += width;
  }

#endif

  newarg.nthreads = num_cpu;

  if (num_cpu) {

    for (j = 0; j < num_cpu; j++) {
      for (i = 0; i < num_cpu; i++) {
	for (k = 0; k < DIVIDE_RATE; k++) {
	  job[j].working[i][CACHE_LINE_SIZE * k] = 0;
	}
      }
    }

    queue[0].sa = sa;
    queue[0].sb = sb;
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

#ifdef USE_ALLOC_HEAP
  free(job);
#endif

  return 0;
}
