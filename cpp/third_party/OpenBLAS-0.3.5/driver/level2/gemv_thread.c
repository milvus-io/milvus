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
#include <stdlib.h>
#include "common.h"

#ifndef TRANSA
#if   !defined(CONJ) && !defined(XCONJ)
#define GEMV	GEMV_N
#elif  defined(CONJ) && !defined(XCONJ)
#define GEMV	GEMV_R
#elif !defined(CONJ) &&  defined(XCONJ)
#define GEMV	GEMV_O
#else
#define GEMV	GEMV_S
#endif
#else
#if   !defined(CONJ) && !defined(XCONJ)
#define GEMV	GEMV_T
#elif  defined(CONJ) && !defined(XCONJ)
#define GEMV	GEMV_C
#elif !defined(CONJ) &&  defined(XCONJ)
#define GEMV	GEMV_U
#else
#define GEMV	GEMV_D
#endif
#endif

#ifndef thread_local
# if __STDC_VERSION__ >= 201112 && !defined __STDC_NO_THREADS__
#  define thread_local _Thread_local
# elif defined _WIN32 && ( \
       defined _MSC_VER || \
       defined __ICL || \
       defined __DMC__ || \
       defined __BORLANDC__ )
#  define thread_local __declspec(thread) 
/* note that ICC (linux) and Clang are covered by __GNUC__ */
# elif defined __GNUC__ || \
       defined __SUNPRO_C || \
       defined __xlC__
#  define thread_local __thread
# else
# define UNSAFE
#endif
#endif
#if defined USE_OPENMP
#undef UNSAFE
#endif

#if !defined(TRANSA) && !defined(UNSAFE)
#define Y_DUMMY_NUM 1024
#if defined(USE_OPENMP)
static FLOAT y_dummy[Y_DUMMY_NUM];
#pragma omp threadprivate(y_dummy)
# else
static thread_local FLOAT y_dummy[Y_DUMMY_NUM];
# endif
#endif

static int gemv_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *buffer, BLASLONG pos){

  FLOAT *a, *x, *y;
  BLASLONG lda, incx, incy;
  BLASLONG m_from, m_to, n_from, n_to;

  a = (FLOAT *)args -> a;
  x = (FLOAT *)args -> b;
  y = (FLOAT *)args -> c;

  lda  = args -> lda;
  incx = args -> ldb;
  incy = args -> ldc;

  m_from = 0;
  m_to   = args -> m;

  if (range_m) {
    m_from = *(range_m + 0);
    m_to   = *(range_m + 1);

    a += m_from        * COMPSIZE;
#ifndef TRANSA
    y += m_from * incy * COMPSIZE;
#endif
  }

  n_from = 0;
  n_to   = args -> n;

  if (range_n) {
    n_from = *(range_n + 0);
    n_to   = *(range_n + 1);

    a += n_from * lda  * COMPSIZE;
#ifdef TRANSA
    y += n_from * incy * COMPSIZE;
#else
# ifndef UNSAFE
    //for split matrix row (n) direction and vector x of gemv_n
    x += n_from * incx * COMPSIZE;
    //store partial result for every thread
    y += (m_to - m_from) * 1 * COMPSIZE * pos;
# endif
#endif
  }

  //fprintf(stderr, "M_From = %d  M_To = %d  N_From = %d  N_To = %d POS=%d\n", m_from, m_to, n_from, n_to, pos);

  GEMV(m_to - m_from, n_to - n_from, 0,
       *((FLOAT *)args -> alpha + 0),
#ifdef COMPLEX
       *((FLOAT *)args -> alpha + 1),
#endif
       a, lda, x, incx, y, incy, buffer);

  return 0;
}

#ifndef COMPLEX
int CNAME(BLASLONG m, BLASLONG n, FLOAT  alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
#else
int CNAME(BLASLONG m, BLASLONG n, FLOAT *alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
#endif

  blas_arg_t args;
  blas_queue_t queue[MAX_CPU_NUMBER];
  BLASLONG range[MAX_CPU_NUMBER + 1];

  BLASLONG width, i, num_cpu;

#if !defined(TRANSA) && !defined(UNSAFE)
  int split_x=0;
#endif

#ifdef SMP
#ifndef COMPLEX
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_REAL;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_REAL;
#else
  int mode  =  BLAS_SINGLE  | BLAS_REAL;
#endif
#else
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
  int mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif
#endif
#endif

  args.m = m;
  args.n = n;

  args.a = (void *)a;
  args.b = (void *)x;
  args.c = (void *)y;

  args.lda = lda;
  args.ldb = incx;
  args.ldc = incy;

#ifndef COMPLEX
  args.alpha = (void *)&alpha;
#else
  args.alpha = (void *) alpha;
#endif

  num_cpu  = 0;

  range[0] = 0;
#ifndef TRANSA
  i        = m;
#else
  i        = n;
#endif

  while (i > 0){

    width  = blas_quickdivide(i + nthreads - num_cpu - 1, nthreads - num_cpu);
    if (width < 4) width = 4;
    if (i < width) width = i;

    range[num_cpu + 1] = range[num_cpu] + width;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = gemv_kernel;
    queue[num_cpu].args    = &args;
#ifndef TRANSA
    queue[num_cpu].range_m = &range[num_cpu];
    queue[num_cpu].range_n = NULL;
#else
    queue[num_cpu].range_m = NULL;
    queue[num_cpu].range_n = &range[num_cpu];
#endif
    queue[num_cpu].sa      = NULL;
    queue[num_cpu].sb      = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];

    num_cpu ++;
    i -= width;
  }

#if !defined(TRANSA) && !defined(UNSAFE) 
  //try to split matrix on row direction and x.
  //Then, reduction.
  if (num_cpu < nthreads) {

    //too small to split or bigger than the y_dummy buffer.
    double MN = (double) m * (double) n;
    if ( MN <= (24.0 * 24.0  * (double) (GEMM_MULTITHREAD_THRESHOLD*GEMM_MULTITHREAD_THRESHOLD))
	 || m*COMPSIZE*nthreads > Y_DUMMY_NUM)
      goto Outer;

    num_cpu  = 0;
    range[0] = 0;

    memset(y_dummy, 0, sizeof(FLOAT) * m * COMPSIZE * nthreads);

    args.ldc = 1;
    args.c = (void *)y_dummy;

    //split on row (n) and x
    i=n;
    split_x=1;
    while (i > 0){

      width  = blas_quickdivide(i + nthreads - num_cpu - 1, nthreads - num_cpu);
      if (width < 4) width = 4;
      if (i < width) width = i;

      range[num_cpu + 1] = range[num_cpu] + width;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = gemv_kernel;
      queue[num_cpu].args    = &args;

      queue[num_cpu].position = num_cpu;

      queue[num_cpu].range_m = NULL;
      queue[num_cpu].range_n = &range[num_cpu];

      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i -= width;
    }

  }

  Outer:
#endif

  if (num_cpu) {
    queue[0].sa = NULL;
    queue[0].sb = buffer;
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

#if !defined(TRANSA) && !defined(UNSAFE)
  if(split_x==1){
    //reduction
    for(i=0; i<num_cpu; i++){

      int j;
      for(j=0; j<m; j++){
	y[j*incy*COMPSIZE] +=y_dummy[i*m*COMPSIZE + j*COMPSIZE];
#ifdef COMPLEX
	y[j*incy*COMPSIZE+1] +=y_dummy[i*m*COMPSIZE + j*COMPSIZE+1];
#endif
      }
    }
  }
#endif

  return 0;
}
