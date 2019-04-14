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

double sqrt(double);

//In this case, the recursive getrf_parallel may overflow the stack.
//Instead, use malloc to alloc job_t.
#if MAX_CPU_NUMBER > GETRF_MEM_ALLOC_THRESHOLD
#define USE_ALLOC_HEAP
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 8
#endif

#ifndef DIVIDE_RATE
#define DIVIDE_RATE 2
#endif

#define GEMM_PQ  MAX(GEMM_P, GEMM_Q)
#define REAL_GEMM_R (GEMM_R - GEMM_PQ)

#ifndef GETRF_FACTOR
#define GETRF_FACTOR 0.75
#endif

#undef  GETRF_FACTOR
#define GETRF_FACTOR 1.00


#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    getrf_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t getrf_lock = 0;
#else
static BLASULONG  getrf_lock = 0UL;
#endif

#if   defined(USE_PTHREAD_LOCK)
static pthread_mutex_t    getrf_flag_lock = PTHREAD_MUTEX_INITIALIZER;
#elif defined(USE_PTHREAD_SPINLOCK)
static pthread_spinlock_t getrf_flag_lock = 0;
#else
static BLASULONG  getrf_flag_lock = 0UL;
#endif




static __inline BLASLONG FORMULA1(BLASLONG M, BLASLONG N, BLASLONG IS, BLASLONG BK, BLASLONG T) {

  double m = (double)(M - IS - BK);
  double n = (double)(N - IS - BK);
  double b = (double)BK;
  double a = (double)T;

  return (BLASLONG)((n + GETRF_FACTOR * m * b * (1. - a) / (b + m)) / a);

}

#define FORMULA2(M, N, IS, BK, T) (BLASLONG)((double)(N - IS + BK) * (1. - sqrt(1. - 1. / (double)(T))))


static void inner_basic_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos){

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
  FLOAT *sbb = sb;

#if __STDC_VERSION__ >= 201112L
  _Atomic BLASLONG *flag = (_Atomic BLASLONG *)args -> d;
#else
  volatile BLASLONG *flag = (volatile BLASLONG *)args -> d;
#endif

  blasint *ipiv = (blasint *)args -> c;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    c     += range_n[0] * lda * COMPSIZE;
    d     += range_n[0] * lda * COMPSIZE;
  }

  if (args -> a == NULL) {
    TRSM_ILTCOPY(k, k, (FLOAT *)args -> b, lda, 0, sb);
    sbb = (FLOAT *)((((BLASULONG)(sb + k * k * COMPSIZE) + GEMM_ALIGN) & ~GEMM_ALIGN) + GEMM_OFFSET_B);
  } else {
    sb  = (FLOAT *)args -> a;
  }

  for (js = 0; js < n; js += REAL_GEMM_R) {
    min_j = n - js;
    if (min_j > REAL_GEMM_R) min_j = REAL_GEMM_R;

    for (jjs = js; jjs < js + min_j; jjs += GEMM_UNROLL_N){
      min_jj = js + min_j - jjs;
      if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

      if (0 && GEMM_UNROLL_N <= 8) {

	LASWP_NCOPY(min_jj, off + 1, off + k,
		    c + (- off + jjs * lda) * COMPSIZE, lda,
		    ipiv, sbb + k * (jjs - js) * COMPSIZE);

      } else {

	LASWP_PLUS(min_jj, off + 1, off + k, ZERO,
#ifdef COMPLEX
		   ZERO,
#endif
		   c + (- off + jjs * lda) * COMPSIZE, lda, NULL, 0, ipiv, 1);

	GEMM_ONCOPY (k, min_jj, c + jjs * lda * COMPSIZE, lda, sbb + (jjs - js) * k * COMPSIZE);

      }

      for (is = 0; is < k; is += GEMM_P) {
	min_i = k - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	TRSM_KERNEL_LT(min_i, min_jj, k, dm1,
#ifdef COMPLEX
		       ZERO,
#endif
		       sb  + k * is * COMPSIZE,
		       sbb + (jjs - js) * k * COMPSIZE,
		       c   + (is + jjs * lda) * COMPSIZE, lda, is);
      }
    }

    if ((js + REAL_GEMM_R >= n) && (mypos >= 0)) flag[mypos * CACHE_LINE_SIZE] = 0;

    for (is = 0; is < m; is += GEMM_P){
      min_i = m - is;
      if (min_i > GEMM_P) min_i = GEMM_P;

      GEMM_ITCOPY (k, min_i, b + is * COMPSIZE, lda, sa);

      GEMM_KERNEL_N(min_i, min_j, k, dm1,
#ifdef COMPLEX
		    ZERO,
#endif
		    sa, sbb, d + (is + js * lda) * COMPSIZE, lda);
    }
  }
}


/* Non blocking implementation */

typedef struct {
#if __STDC_VERSION__ >= 201112L
  _Atomic
#else
  volatile
#endif
   BLASLONG working[MAX_CPU_NUMBER][CACHE_LINE_SIZE * DIVIDE_RATE];
} job_t;

#define ICOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ITCOPY(M, N, (FLOAT *)(A) + ((Y) + (X) * (LDA)) * COMPSIZE, LDA, BUFFER);
#define OCOPY_OPERATION(M, N, A, LDA, X, Y, BUFFER) GEMM_ONCOPY(M, N, (FLOAT *)(A) + ((X) + (Y) * (LDA)) * COMPSIZE, LDA, BUFFER);

#ifndef COMPLEX
#define KERNEL_OPERATION(M, N, K, SA, SB, C, LDC, X, Y) \
	GEMM_KERNEL_N(M, N, K, dm1, SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#else
#define KERNEL_OPERATION(M, N, K, SA, SB, C, LDC, X, Y) \
	GEMM_KERNEL_N(M, N, K, dm1, ZERO, SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC)
#endif

static int inner_advanced_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos){

  job_t *job = (job_t *)args -> common;

  BLASLONG xxx, bufferside;

  FLOAT *buffer[DIVIDE_RATE];

  BLASLONG jjs, min_jj, div_n;

  BLASLONG i, current;
  BLASLONG is, min_i;

  BLASLONG m, n_from, n_to;
  BLASLONG k = args -> k;

  BLASLONG lda = args -> lda;
  BLASLONG off = args -> ldb;

  FLOAT *a = (FLOAT *)args -> b + (k          ) * COMPSIZE;
  FLOAT *b = (FLOAT *)args -> b + (    k * lda) * COMPSIZE;
  FLOAT *c = (FLOAT *)args -> b + (k + k * lda) * COMPSIZE;
  FLOAT *sbb= sb;

  blasint *ipiv = (blasint *)args -> c;
  BLASLONG jw;
#if __STDC_VERSION__ >= 201112L
  _Atomic BLASLONG *flag = (_Atomic BLASLONG *)args -> d;
#else
  volatile BLASLONG *flag = (volatile BLASLONG *)args -> d;
#endif
  if (args -> a == NULL) {
    TRSM_ILTCOPY(k, k, (FLOAT *)args -> b, lda, 0, sb);
    sbb = (FLOAT *)((((BLASULONG)(sb + k * k * COMPSIZE) + GEMM_ALIGN) & ~GEMM_ALIGN) + GEMM_OFFSET_B);
  } else {
    sb  = (FLOAT *)args -> a;
  }

  m      = range_m[1] - range_m[0];
  n_from = range_n[mypos + 0];
  n_to   = range_n[mypos + 1];

  a     += range_m[0] * COMPSIZE;
  c     += range_m[0] * COMPSIZE;

  div_n = (n_to - n_from + DIVIDE_RATE - 1) / DIVIDE_RATE;

  buffer[0] = sbb;


  for (i = 1; i < DIVIDE_RATE; i++) {
    buffer[i] = buffer[i - 1] + GEMM_Q * (((div_n + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N) * COMPSIZE;
  }

  for (xxx = n_from, bufferside = 0; xxx < n_to; xxx += div_n, bufferside ++) {

    for (i = 0; i < args -> nthreads; i++)
#if 1
    {
	LOCK_COMMAND(&getrf_lock);
	jw = job[mypos].working[i][CACHE_LINE_SIZE * bufferside];
	UNLOCK_COMMAND(&getrf_lock);
	do {
	    LOCK_COMMAND(&getrf_lock);
	    jw = job[mypos].working[i][CACHE_LINE_SIZE * bufferside];
	    UNLOCK_COMMAND(&getrf_lock);
	} while (jw);
    }
#else
      while (job[mypos].working[i][CACHE_LINE_SIZE * bufferside]) {};
#endif
    for(jjs = xxx; jjs < MIN(n_to, xxx + div_n); jjs += min_jj){
      min_jj = MIN(n_to, xxx + div_n) - jjs;
      if (min_jj > GEMM_UNROLL_N) min_jj = GEMM_UNROLL_N;

      if (0 && GEMM_UNROLL_N <= 8) {
	printf("helllo\n");

	LASWP_NCOPY(min_jj, off + 1, off + k,
		    b + (- off + jjs * lda) * COMPSIZE, lda,
		    ipiv, buffer[bufferside] + (jjs - xxx) * k * COMPSIZE);

      } else {

	LASWP_PLUS(min_jj, off + 1, off + k, ZERO,
#ifdef COMPLEX
		   ZERO,
#endif
		   b + (- off + jjs * lda) * COMPSIZE, lda, NULL, 0, ipiv, 1);

	GEMM_ONCOPY (k, min_jj, b + jjs * lda * COMPSIZE, lda,
		     buffer[bufferside] + (jjs - xxx) * k * COMPSIZE);
      }

      for (is = 0; is < k; is += GEMM_P) {
	min_i = k - is;
	if (min_i > GEMM_P) min_i = GEMM_P;

	TRSM_KERNEL_LT(min_i, min_jj, k, dm1,
#ifdef COMPLEX
		       ZERO,
#endif
		       sb + k * is * COMPSIZE,
		       buffer[bufferside] + (jjs - xxx) * k * COMPSIZE,
		       b   + (is + jjs * lda) * COMPSIZE, lda, is);
      }
    }
    MB;
    for (i = 0; i < args -> nthreads; i++) {
      LOCK_COMMAND(&getrf_lock);
      job[mypos].working[i][CACHE_LINE_SIZE * bufferside] = (BLASLONG)buffer[bufferside];
      UNLOCK_COMMAND(&getrf_lock);
    }
  }

  LOCK_COMMAND(&getrf_flag_lock);
  flag[mypos * CACHE_LINE_SIZE] = 0;
  UNLOCK_COMMAND(&getrf_flag_lock);

  if (m == 0) {
    for (xxx = 0; xxx < DIVIDE_RATE; xxx++) {
      LOCK_COMMAND(&getrf_lock);
      job[mypos].working[mypos][CACHE_LINE_SIZE * xxx] = 0;
      UNLOCK_COMMAND(&getrf_lock);
    }
  }

  for(is = 0; is < m; is += min_i){
    min_i = m - is;
    if (min_i >= GEMM_P * 2) {
      min_i = GEMM_P;
    } else
      if (min_i > GEMM_P) {
	min_i = (((min_i + 1) / 2 + GEMM_UNROLL_M - 1)/GEMM_UNROLL_M) * GEMM_UNROLL_M;
      }

      ICOPY_OPERATION(k, min_i, a, lda, 0, is, sa);

      current = mypos;

      do {

	div_n = (range_n[current + 1]  - range_n[current] + DIVIDE_RATE - 1) / DIVIDE_RATE;

	for (xxx = range_n[current], bufferside = 0; xxx < range_n[current + 1]; xxx += div_n, bufferside ++) {

	  if ((current != mypos) && (!is)) {
#if 1
		LOCK_COMMAND(&getrf_lock);
		jw = job[current].working[mypos][CACHE_LINE_SIZE * bufferside];
		UNLOCK_COMMAND(&getrf_lock);
		do {
		    LOCK_COMMAND(&getrf_lock);
		    jw = job[current].working[mypos][CACHE_LINE_SIZE * bufferside];
		    UNLOCK_COMMAND(&getrf_lock);
		} while (jw == 0);
#else
	    	    while(job[current].working[mypos][CACHE_LINE_SIZE * bufferside] == 0) {};
#endif
	  }

	  KERNEL_OPERATION(min_i, MIN(range_n[current + 1] - xxx, div_n), k,
			   sa, (FLOAT *)job[current].working[mypos][CACHE_LINE_SIZE * bufferside],
			   c, lda, is, xxx);

	  MB;
	  if (is + min_i >= m) {
            LOCK_COMMAND(&getrf_lock);
	    job[current].working[mypos][CACHE_LINE_SIZE * bufferside] = 0;
            UNLOCK_COMMAND(&getrf_lock);
	  }
	}

	current ++;
	if (current >= args -> nthreads) current = 0;

      } while (current != mypos);
  }

  for (i = 0; i < args -> nthreads; i++) {
    for (xxx = 0; xxx < DIVIDE_RATE; xxx++) {
#if 1
	LOCK_COMMAND(&getrf_lock);
	jw = job[mypos].working[i][CACHE_LINE_SIZE *xxx];
	UNLOCK_COMMAND(&getrf_lock);
	do {
	    LOCK_COMMAND(&getrf_lock);
	    jw = job[mypos].working[i][CACHE_LINE_SIZE *xxx];
	    UNLOCK_COMMAND(&getrf_lock);
	} while(jw != 0);
#else
      while (job[mypos].working[i][CACHE_LINE_SIZE * xxx] ) {};
#endif
    }
  }

  return 0;
}

#if 1

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG m, n, mn, lda, offset;
  BLASLONG init_bk, next_bk, range_n_mine[2], range_n_new[2];
  blasint *ipiv, iinfo, info;
  int mode;
  blas_arg_t newarg;

  FLOAT *a, *sbb;
  FLOAT dummyalpha[2] = {ZERO, ZERO};

  blas_queue_t queue[MAX_CPU_NUMBER];

  BLASLONG range_M[MAX_CPU_NUMBER + 1];
  BLASLONG range_N[MAX_CPU_NUMBER + 1];

#ifndef USE_ALLOC_HEAP
  job_t        job[MAX_CPU_NUMBER];
#else
  job_t *      job=NULL;
#endif

  BLASLONG width, nn, mm;
  BLASLONG i, j, k, is, bk;

  BLASLONG num_cpu;
  BLASLONG f;

#ifdef _MSC_VER
  BLASLONG flag[MAX_CPU_NUMBER * CACHE_LINE_SIZE];
#else
#if __STDC_VERSION__ >= 201112L
  _Atomic
#else  
  volatile
#endif  
   BLASLONG flag[MAX_CPU_NUMBER * CACHE_LINE_SIZE] __attribute__((aligned(128)));
#endif

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

  newarg.c   = ipiv;
  newarg.lda = lda;

  info = 0;

  mn = MIN(m, n);

  init_bk = ((mn / 2 + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
  if (init_bk > GEMM_Q) init_bk = GEMM_Q;

  if (init_bk <= GEMM_UNROLL_N) {
    info = GETF2(args, NULL, range_n, sa, sb, 0);
    return info;
  }

  next_bk = init_bk;

  bk = mn;
  if (bk > next_bk) bk = next_bk;

  range_n_new[0] = offset;
  range_n_new[1] = offset + bk;

  iinfo   = CNAME(args, NULL, range_n_new, sa, sb, 0);

  if (iinfo && !info) info = iinfo;

#ifdef USE_ALLOC_HEAP
  job = (job_t*)malloc(MAX_CPU_NUMBER * sizeof(job_t));
  if(job==NULL){
    fprintf(stderr, "OpenBLAS: malloc failed in %s\n", __func__);
    exit(1);
  }
#endif

  newarg.common   = (void *)job;

  TRSM_ILTCOPY(bk, bk, a, lda, 0, sb);

  sbb = (FLOAT *)((((BLASULONG)(sb + bk * bk * COMPSIZE) + GEMM_ALIGN) & ~GEMM_ALIGN) + GEMM_OFFSET_B);

  is = 0;
  num_cpu = 0;

  while (is < mn) {

    width  = ((FORMULA1(m, n, is, bk, args -> nthreads) + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
    if (width > mn - is - bk) width = mn - is - bk;

    if (width < bk) {
      next_bk = ((FORMULA2(m, n, is, bk, args -> nthreads) + GEMM_UNROLL_N)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

      if (next_bk > bk) next_bk = bk;

      width = next_bk;
      if (width > mn - is - bk) width = mn - is - bk;
    }

    if (num_cpu > 0) exec_blas_async_wait(num_cpu, &queue[0]);

    mm = m - bk - is;
    nn = n - bk - is;

    newarg.a   = sb;
    newarg.b   = a + (is + is * lda) * COMPSIZE;
    newarg.d   = (void *)flag;
    newarg.m   = mm;
    newarg.n   = nn;
    newarg.k   = bk;
    newarg.ldb = is + offset;

    nn -= width;

    range_n_mine[0] = 0;
    range_n_mine[1] = width;

    range_N[0] = width;
    range_M[0] = 0;

    num_cpu  = 0;

    while (nn > 0){

      if (mm >= nn) {

	width  = blas_quickdivide(nn + args -> nthreads - num_cpu, args -> nthreads - num_cpu - 1);
	if (width == 0) width = nn;
	if (nn < width) width = nn;
	nn -= width;
	range_N[num_cpu + 1] = range_N[num_cpu] + width;

	width  = blas_quickdivide(mm + args -> nthreads - num_cpu, args -> nthreads - num_cpu - 1);
	if (width == 0) width = mm;
	if (mm < width) width = mm;
	if (nn <=    0) width = mm;
	mm -= width;
	range_M[num_cpu + 1] = range_M[num_cpu] + width;

      } else {

	width  = blas_quickdivide(mm + args -> nthreads - num_cpu, args -> nthreads - num_cpu - 1);
	if (width == 0) width = mm;
	if (mm < width) width = mm;
	mm -= width;
	range_M[num_cpu + 1] = range_M[num_cpu] + width;

	width  = blas_quickdivide(nn + args -> nthreads - num_cpu, args -> nthreads - num_cpu - 1);
	if (width == 0) width = nn;
	if (nn < width) width = nn;
	if (mm <=    0) width = nn;
	nn -= width;
	range_N[num_cpu + 1] = range_N[num_cpu] + width;

      }

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = inner_advanced_thread;
      queue[num_cpu].args    = &newarg;
      queue[num_cpu].range_m = &range_M[num_cpu];
      queue[num_cpu].range_n = &range_N[0];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];
      flag[num_cpu * CACHE_LINE_SIZE] = 1;

      num_cpu ++;

    }

    newarg.nthreads = num_cpu;

    if (num_cpu > 0) {
      for (j = 0; j < num_cpu; j++) {
	for (i = 0; i < num_cpu; i++) {
	  for (k = 0; k < DIVIDE_RATE; k++) {
	    job[j].working[i][CACHE_LINE_SIZE * k] = 0;
	  }
	}
      }
    }

    is += bk;

    bk = mn - is;
    if (bk > next_bk) bk = next_bk;

    range_n_new[0] = offset + is;
    range_n_new[1] = offset + is + bk;

    if (num_cpu > 0) {
      queue[num_cpu - 1].next = NULL;

      exec_blas_async(0, &queue[0]);

      inner_basic_thread(&newarg, NULL, range_n_mine, sa, sbb, -1);

      iinfo   = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);

      if (iinfo && !info) info = iinfo + is;

      for (i = 0; i < num_cpu; i ++) {
#if 1
	      LOCK_COMMAND(&getrf_flag_lock);
	      f=flag[i*CACHE_LINE_SIZE];
	      UNLOCK_COMMAND(&getrf_flag_lock);
	      while (f!=0) {
	      LOCK_COMMAND(&getrf_flag_lock);
	      f=flag[i*CACHE_LINE_SIZE];
	      UNLOCK_COMMAND(&getrf_flag_lock);
	      };
#else
              while (flag[i*CACHE_LINE_SIZE]) {};
#endif
      }
      TRSM_ILTCOPY(bk, bk, a + (is +  is * lda) * COMPSIZE, lda, 0, sb);

    } else {

      inner_basic_thread(&newarg, NULL, range_n_mine, sa, sbb, -1);

      iinfo   = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);

      if (iinfo && !info) info = iinfo + is;

    }

  }

  next_bk = init_bk;
  is = 0;

  while (is < mn) {

    bk = mn - is;
    if (bk > next_bk) bk = next_bk;

    width  = ((FORMULA1(m, n, is, bk, args -> nthreads) + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
    if (width > mn - is - bk) width = mn - is - bk;

    if (width < bk) {
      next_bk = ((FORMULA2(m, n, is, bk, args -> nthreads) + GEMM_UNROLL_N)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
      if (next_bk > bk) next_bk = bk;
    }

    blas_level1_thread(mode, bk, is + bk + offset + 1, mn + offset, (void *)dummyalpha,
		       a + (- offset + is * lda) * COMPSIZE, lda, NULL, 0,
		       ipiv, 1, (void *)LASWP_PLUS, args -> nthreads);

    is += bk;
  }

#ifdef USE_ALLOC_HEAP
  free(job);
#endif

  return info;
}

#else

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG m, n, mn, lda, offset;
  BLASLONG i, is, bk, init_bk, next_bk, range_n_new[2];
  blasint *ipiv, iinfo, info;
  int mode;
  blas_arg_t newarg;
  FLOAT *a, *sbb;
  FLOAT dummyalpha[2] = {ZERO, ZERO};

  blas_queue_t queue[MAX_CPU_NUMBER];
  BLASLONG range[MAX_CPU_NUMBER + 1];

  BLASLONG width, nn, num_cpu;
#if __STDC_VERSION__ >= 201112L
  _Atomic
#else  
  volatile
#endif
   BLASLONG flag[MAX_CPU_NUMBER * CACHE_LINE_SIZE] __attribute__((aligned(128)));

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

  newarg.c   = ipiv;
  newarg.lda = lda;
  newarg.common = NULL;
  newarg.nthreads = args -> nthreads;

  mn = MIN(m, n);

  init_bk = ((mn / 2 + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
  if (init_bk > GEMM_Q) init_bk = GEMM_Q;

  if (init_bk <= GEMM_UNROLL_N) {
    info = GETF2(args, NULL, range_n, sa, sb, 0);
    return info;
  }

  width = FORMULA1(m, n, 0, init_bk, args -> nthreads);
  width = ((width + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;
  if (width > n - init_bk) width = n - init_bk;

  if (width < init_bk) {
    BLASLONG temp;

    temp = FORMULA2(m, n, 0, init_bk, args -> nthreads);
    temp = ((temp + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

    if (temp < GEMM_UNROLL_N) temp = GEMM_UNROLL_N;
    if (temp < init_bk) init_bk = temp;

  }

  next_bk = init_bk;
  bk      = init_bk;

  range_n_new[0] = offset;
  range_n_new[1] = offset + bk;

  info   = CNAME(args, NULL, range_n_new, sa, sb, 0);

  TRSM_ILTCOPY(bk, bk, a, lda, 0, sb);

  is = 0;
  num_cpu = 0;

  sbb = (FLOAT *)((((BLASULONG)(sb + GEMM_PQ * GEMM_PQ * COMPSIZE) + GEMM_ALIGN) & ~GEMM_ALIGN) + GEMM_OFFSET_B);

  while (is < mn) {

    width  = FORMULA1(m, n, is, bk, args -> nthreads);
    width = ((width + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

    if (width < bk) {

      next_bk = FORMULA2(m, n, is, bk, args -> nthreads);
      next_bk = ((next_bk + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

      if (next_bk > bk) next_bk = bk;
#if 0
      if (next_bk < GEMM_UNROLL_N) next_bk = MIN(GEMM_UNROLL_N, mn - bk - is);
#else
      if (next_bk < GEMM_UNROLL_N) next_bk = MAX(GEMM_UNROLL_N, mn - bk - is);
#endif

      width = next_bk;
    }

    if (width > mn - is - bk) {
      next_bk = mn - is - bk;
      width   = next_bk;
    }

    nn = n - bk - is;
    if (width > nn) width = nn;

    if (num_cpu > 1)  exec_blas_async_wait(num_cpu - 1, &queue[1]);

    range[0] = 0;
    range[1] = width;

    num_cpu = 1;
    nn -= width;

    newarg.a   = sb;
    newarg.b   = a + (is + is * lda) * COMPSIZE;
    newarg.d   = (void *)flag;
    newarg.m   = m - bk - is;
    newarg.n   = n - bk - is;
    newarg.k   = bk;
    newarg.ldb = is + offset;

    while (nn > 0){

      width  = blas_quickdivide(nn + args -> nthreads - num_cpu, args -> nthreads - num_cpu);

      nn -= width;
      if (nn < 0) width = width + nn;

      range[num_cpu + 1] = range[num_cpu] + width;

      queue[num_cpu].mode    = mode;
      //queue[num_cpu].routine = inner_advanced_thread;
      queue[num_cpu].routine = (void *)inner_basic_thread;
      queue[num_cpu].args    = &newarg;
      queue[num_cpu].range_m = NULL;
      queue[num_cpu].range_n = &range[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];
      flag[num_cpu * CACHE_LINE_SIZE] = 1;

      num_cpu ++;
    }

    queue[num_cpu - 1].next = NULL;

    is += bk;

    bk = n - is;
    if (bk > next_bk) bk = next_bk;

    range_n_new[0] = offset + is;
    range_n_new[1] = offset + is + bk;

    if (num_cpu > 1) {

      exec_blas_async(1, &queue[1]);

#if 0
      inner_basic_thread(&newarg, NULL, &range[0], sa, sbb, 0);

      iinfo = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);
#else

      if (range[1] >= bk * 4) {

	BLASLONG myrange[2];

	myrange[0] = 0;
	myrange[1] = bk;

	inner_basic_thread(&newarg, NULL, &myrange[0], sa, sbb, -1);

	iinfo = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);

	myrange[0] = bk;
	myrange[1] = range[1];

	inner_basic_thread(&newarg, NULL, &myrange[0], sa, sbb, -1);

      } else {

	inner_basic_thread(&newarg, NULL, &range[0], sa, sbb, -1);

	iinfo = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);
      }

#endif

      for (i = 1; i < num_cpu; i ++) while (flag[i * CACHE_LINE_SIZE]) {};

      TRSM_ILTCOPY(bk, bk, a + (is +  is * lda) * COMPSIZE, lda, 0, sb);

    } else {

      inner_basic_thread(&newarg, NULL, &range[0], sa, sbb, -1);

      iinfo = GETRF_SINGLE(args, NULL, range_n_new, sa, sbb, 0);
    }

      if (iinfo && !info) info = iinfo + is;

  }

  next_bk = init_bk;
  bk      = init_bk;

  is = 0;

  while (is < mn) {

    bk = mn - is;
    if (bk > next_bk) bk = next_bk;

    width  = FORMULA1(m, n, is, bk, args -> nthreads);
    width = ((width + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

    if (width < bk) {
      next_bk = FORMULA2(m, n, is, bk, args -> nthreads);
      next_bk = ((next_bk + GEMM_UNROLL_N - 1)/GEMM_UNROLL_N) * GEMM_UNROLL_N;

      if (next_bk > bk) next_bk = bk;
#if 0
      if (next_bk < GEMM_UNROLL_N) next_bk = MIN(GEMM_UNROLL_N, mn - bk - is);
#else
      if (next_bk < GEMM_UNROLL_N) next_bk = MAX(GEMM_UNROLL_N, mn - bk - is);
#endif
    }

    if (width > mn - is - bk) {
      next_bk = mn - is - bk;
      width   = next_bk;
    }

    blas_level1_thread(mode, bk, is + bk + offset + 1, mn + offset, (void *)dummyalpha,
		       a + (- offset + is * lda) * COMPSIZE, lda, NULL, 0,
		       ipiv, 1, (void *)LASWP_PLUS, args -> nthreads);

    is += bk;
  }

  return info;
}

#endif

