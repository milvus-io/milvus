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

#if !defined(HEMV) && !defined(HEMVREV)
#define MYAXPY	AXPYU_K
#define MYDOT	DOTU_K
#elif  defined HEMV
#define MYAXPY	AXPYU_K
#define MYDOT	DOTC_K
#else
#define MYAXPY	AXPYC_K
#define MYDOT	DOTU_K
#endif

static int sbmv_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *buffer, BLASLONG pos){

  FLOAT *a, *x, *y;
  BLASLONG lda, incx;
  BLASLONG n, k, n_from, n_to;
  BLASLONG i, length;
#ifndef COMPLEX
  FLOAT result;
#else
  OPENBLAS_COMPLEX_FLOAT result;
#endif

  a = (FLOAT *)args -> a;
  x = (FLOAT *)args -> b;

  lda  = args -> lda;
  incx = args -> ldb;

  n = args -> n;
  k = args -> k;

  n_from = 0;
  n_to   = n;

  //Use y as each thread's n* COMPSIZE elements in sb buffer
  y = buffer;
  buffer += ((COMPSIZE * n  + 1023) & ~1023);

  if (range_m) {
    n_from = *(range_m + 0);
    n_to   = *(range_m + 1);

    a += n_from * lda  * COMPSIZE;
  }


  if (incx != 1) {
    COPY_K(n, x, incx, buffer, 1);

    x = buffer;
    // buffer += ((COMPSIZE * n  + 1023) & ~1023);
  }

  SCAL_K(n, 0, 0, ZERO,
#ifdef COMPLEX
	 ZERO,
#endif
	 y, 1, NULL, 0, NULL, 0);

  for (i = n_from; i < n_to; i++) {

#ifndef LOWER

    length  = i;
    if (length > k) length = k;

    MYAXPY(length, 0, 0,
	   *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	   *(x + i * COMPSIZE + 1),
#endif
	   a + (k - length) * COMPSIZE, 1, y + (i - length) * COMPSIZE, 1, NULL, 0);

#if !defined(HEMV) && !defined(HEMVREV)
    result = MYDOT(length + 1, a + (k - length) * COMPSIZE, 1, x + (i - length) * COMPSIZE, 1);
#else
    result = MYDOT(length    , a + (k - length) * COMPSIZE, 1, x + (i - length) * COMPSIZE, 1);
#endif

#ifndef COMPLEX
    *(y + i * COMPSIZE + 0) += result;
#else
#if !defined(HEMV) && !defined(HEMVREV)
    *(y + i * COMPSIZE + 0) += CREAL(result);
    *(y + i * COMPSIZE + 1) += CIMAG(result);
#else
    *(y + i * COMPSIZE + 0) += CREAL(result) + *(a + k * COMPSIZE) * *(x + i * COMPSIZE + 0);
    *(y + i * COMPSIZE + 1) += CIMAG(result) + *(a + k * COMPSIZE) * *(x + i * COMPSIZE + 1);
#endif
#endif

#else

    length  = k;
    if (n - i - 1 < k) length = n - i - 1;

    MYAXPY(length, 0, 0,
	   *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	   *(x + i * COMPSIZE + 1),
#endif
	   a + COMPSIZE, 1, y + (i + 1) * COMPSIZE, 1, NULL, 0);

#if !defined(HEMV) && !defined(HEMVREV)
    result = MYDOT(length + 1, a, 1, x + i * COMPSIZE, 1);
#else
    result = MYDOT(length    , a + COMPSIZE, 1, x + (i + 1) * COMPSIZE, 1) ;
#endif

#ifndef COMPLEX
    *(y + i * COMPSIZE + 0) += result;
#else
#if !defined(HEMV) && !defined(HEMVREV)
    *(y + i * COMPSIZE + 0) += CREAL(result);
    *(y + i * COMPSIZE + 1) += CIMAG(result);
#else
    *(y + i * COMPSIZE + 0) += CREAL(result) + *a * *(x + i * COMPSIZE + 0);
    *(y + i * COMPSIZE + 1) += CIMAG(result) + *a * *(x + i * COMPSIZE + 1);
#endif
#endif

#endif

    a += lda * COMPSIZE;
  }

  return 0;
}

#ifndef COMPLEX
int CNAME(BLASLONG n, BLASLONG k, FLOAT  alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
#else
int CNAME(BLASLONG n, BLASLONG k, FLOAT *alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
#endif

  blas_arg_t args;
  blas_queue_t queue[MAX_CPU_NUMBER + 1];
  BLASLONG range_m[MAX_CPU_NUMBER + 1];
  BLASLONG range_n[MAX_CPU_NUMBER];

  BLASLONG width, i, num_cpu;
  double dnum;
  int mask = 7;

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

  args.n = n;
  args.k = k;

  args.a = (void *)a;
  args.b = (void *)x;
  args.c = (void *)buffer;

  args.lda = lda;
  args.ldb = incx;
  args.ldc = incy;

  dnum = (double)n * (double)n / (double)nthreads;
  num_cpu  = 0;

  if (n < 2 * k) {

#ifndef LOWER

    range_m[MAX_CPU_NUMBER] = n;
    i          = 0;

    while (i < n){

      if (nthreads - num_cpu > 1) {

	double di = (double)(n - i);
	if (di * di - dnum > 0) {
	  width = ((BLASLONG)(-sqrt(di * di - dnum) + di) + mask) & ~mask;
	} else {
	  width = n - i;
	}

	if (width < 16) width = 16;
	if (width > n - i) width = n - i;

      } else {
	width = n - i;
      }

      range_m[MAX_CPU_NUMBER - num_cpu - 1] = range_m[MAX_CPU_NUMBER - num_cpu] - width;
      range_n[num_cpu] = num_cpu * (((n + 15) & ~15) + 16);
      if (range_n[num_cpu] > n * num_cpu) range_n[num_cpu] = n * num_cpu;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = sbmv_kernel;
      queue[num_cpu].args    = &args;
      queue[num_cpu].range_m = &range_m[MAX_CPU_NUMBER - num_cpu - 1];
      queue[num_cpu].range_n = &range_n[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i += width;
    }

#else

    range_m[0] = 0;
    i          = 0;

    while (i < n){

      if (nthreads - num_cpu > 1) {

	double di = (double)(n - i);
	if (di * di - dnum > 0) {
	  width = ((BLASLONG)(-sqrt(di * di - dnum) + di) + mask) & ~mask;
	} else {
	  width = n - i;
	}

	if (width < 16) width = 16;
	if (width > n - i) width = n - i;

      } else {
	width = n - i;
      }

      range_m[num_cpu + 1] = range_m[num_cpu] + width;
      range_n[num_cpu] = num_cpu * (((n + 15) & ~15) + 16);
      if (range_n[num_cpu] > n * num_cpu) range_n[num_cpu] = n * num_cpu;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = sbmv_kernel;
      queue[num_cpu].args    = &args;
      queue[num_cpu].range_m = &range_m[num_cpu];
      queue[num_cpu].range_n = &range_n[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i += width;
    }

#endif

  } else {

    range_m[0] = 0;
    i          = n;

    while (i > 0){

      width  = blas_quickdivide(i + nthreads - num_cpu - 1, nthreads - num_cpu);

      if (width < 4) width = 4;
      if (i < width) width = i;

      range_m[num_cpu + 1] = range_m[num_cpu] + width;

      range_n[num_cpu] = num_cpu * ((n + 15) & ~15);
      if (range_n[num_cpu] > n * num_cpu) range_n[num_cpu] = n * num_cpu;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = sbmv_kernel;
      queue[num_cpu].args    = &args;
      queue[num_cpu].range_m = &range_m[num_cpu];
      queue[num_cpu].range_n = &range_n[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i -= width;
    }
  }

  if (num_cpu) {
    queue[0].sa = NULL;
    queue[0].sb = buffer;
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  for (i = 1; i < num_cpu; i ++) {
    AXPYU_K(n, 0, 0,
#ifndef COMPLEX
	    ONE,
#else
	    ONE, ZERO,
#endif
	    (FLOAT*)(queue[i].sb), 1, buffer, 1, NULL, 0);
  }

  AXPYU_K(n, 0, 0,
#ifndef COMPLEX
	  alpha,
#else
	  alpha[0], alpha[1],
#endif
	  buffer, 1, y, incy, NULL, 0);

  return 0;
}
