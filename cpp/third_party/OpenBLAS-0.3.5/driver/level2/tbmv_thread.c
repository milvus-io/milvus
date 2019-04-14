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
#include "symcopy.h"

#ifndef COMPLEX
#ifndef TRANSA
#undef  TRANS
#else
#define TRANS
#endif
#define MYDOT	DOTU_K
#define MYAXPY	AXPYU_K
#else
#if (TRANSA == 1) || (TRANSA == 3)
#undef  TRANS
#else
#define TRANS
#endif
#if (TRANSA == 1) || (TRANSA == 2)
#define MYAXPY	AXPYU_K
#define MYDOT	DOTU_K
#else
#define MYAXPY	AXPYC_K
#define MYDOT	DOTC_K
#endif
#endif

static int trmv_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *buffer, BLASLONG pos){

  FLOAT *a, *x, *y;

  BLASLONG k, lda, incx;
  BLASLONG n_from, n_to;
  BLASLONG i, length;

#ifdef TRANS
#ifndef COMPLEX
  FLOAT          result;
#else
  OPENBLAS_COMPLEX_FLOAT result;
#endif
#endif

#if defined(COMPLEX) && !defined(UNIT)
  FLOAT ar, ai, xr, xi;
#endif

  a = (FLOAT *)args -> a;
  x = (FLOAT *)args -> b;
  y = (FLOAT *)args -> c;

  k      = args -> k;
  n_from = 0;
  n_to   = args -> n;

  lda  = args -> lda;
  incx = args -> ldb;

  if (range_m) {
    n_from = *(range_m + 0);
    n_to   = *(range_m + 1);

    a += n_from * lda * COMPSIZE;
  }

  if (incx != 1) {

    COPY_K(args -> n, x, incx, buffer, 1);

    x = buffer;
    // buffer += ((args -> n * COMPSIZE + 1023) & ~1023);
  }

  if (range_n) y += *range_n * COMPSIZE;

  SCAL_K(args -> n, 0, 0, ZERO,
#ifdef COMPLEX
	 ZERO,
#endif
	 y, 1, NULL, 0, NULL, 0);

  for (i = n_from; i < n_to; i++) {

#ifndef LOWER
    length  = i;
#else
    length  = args -> n - i - 1;
#endif
    if (length > k) length = k;

#ifndef LOWER
    if (length > 0) {
#ifndef TRANS
      MYAXPY(length, 0, 0,
	     *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	     *(x + i * COMPSIZE + 1),
#endif
	     a + (k - length) * COMPSIZE, 1, y + (i - length) * COMPSIZE, 1, NULL, 0);
#else
      result = MYDOT(length, a + (k - length) * COMPSIZE, 1, x + (i - length) * COMPSIZE, 1);

#ifndef COMPLEX
      *(y + i * COMPSIZE + 0) += result;
#else
      *(y + i * COMPSIZE + 0) += CREAL(result);
      *(y + i * COMPSIZE + 1) += CIMAG(result);
#endif
#endif
    }
#endif

#ifndef COMPLEX
#ifdef UNIT
    *(y + i * COMPSIZE) += *(x + i * COMPSIZE);
#else
#ifndef LOWER
    *(y + i * COMPSIZE) += *(a + k * COMPSIZE) * *(x + i * COMPSIZE);
#else
    *(y + i * COMPSIZE) += *(a + 0 * COMPSIZE) * *(x + i * COMPSIZE);
#endif
#endif
#else
#ifdef UNIT
    *(y + i * COMPSIZE + 0) += *(x + i * COMPSIZE + 0);
    *(y + i * COMPSIZE + 1) += *(x + i * COMPSIZE + 1);
#else
#ifndef LOWER
    ar = *(a + k * COMPSIZE + 0);
    ai = *(a + k * COMPSIZE + 1);
#else
    ar = *(a                + 0);
    ai = *(a                + 1);
#endif
    xr = *(x + i * COMPSIZE + 0);
    xi = *(x + i * COMPSIZE + 1);

#if (TRANSA == 1) || (TRANSA == 2)
    *(y + i * COMPSIZE + 0) += ar * xr - ai * xi;
    *(y + i * COMPSIZE + 1) += ar * xi + ai * xr;
#else
    *(y + i * COMPSIZE + 0) += ar * xr + ai * xi;
    *(y + i * COMPSIZE + 1) += ar * xi - ai * xr;
#endif
#endif
#endif

#ifdef LOWER
    if (length > 0) {
#ifndef TRANS
      MYAXPY(length, 0, 0,
	     *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	     *(x + i * COMPSIZE + 1),
#endif
	     a + COMPSIZE, 1, y + (i + 1) * COMPSIZE, 1, NULL, 0);
#else
      result = MYDOT(length, a + COMPSIZE, 1, x + (i + 1) * COMPSIZE, 1);

#ifndef COMPLEX
      *(y + i * COMPSIZE + 0) += result;
#else
      *(y + i * COMPSIZE + 0) += CREAL(result);
      *(y + i * COMPSIZE + 1) += CIMAG(result);
#endif
#endif
    }
#endif

    a += lda * COMPSIZE;
  }

  return 0;
}

#ifndef COMPLEX
int CNAME(BLASLONG n, BLASLONG k, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *buffer, int nthreads){
#else
int CNAME(BLASLONG n, BLASLONG k, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG incx, FLOAT *buffer, int nthreads){
#endif

  blas_arg_t args;
  blas_queue_t queue[MAX_CPU_NUMBER];
  BLASLONG range_m[MAX_CPU_NUMBER + 1];
  BLASLONG range_n[MAX_CPU_NUMBER + 1];

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
  args.c = (void *)(buffer);

  args.lda = lda;
  args.ldb = incx;

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
      queue[num_cpu].routine = trmv_kernel;
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
      queue[num_cpu].routine = trmv_kernel;
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
      range_n[num_cpu] = num_cpu * (((n + 15) & ~15) + 16);
      if (range_n[num_cpu] > n * num_cpu) range_n[num_cpu] = n * num_cpu;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = trmv_kernel;
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
    queue[0].sb = buffer + num_cpu * (((n + 255) & ~255) + 16) * COMPSIZE;

    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  for (i = 1; i < num_cpu; i ++) {
    AXPYU_K(n, 0, 0, ONE,
#ifdef COMPLEX
	    ZERO,
#endif
	    buffer + range_n[i] * COMPSIZE, 1, buffer, 1, NULL, 0);
  }

  COPY_K(n, buffer, 1, x, incx);

  return 0;
}
