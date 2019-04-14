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

#if! defined(HEMV) && !defined(HEMVREV)
#define MYDOT	DOTU_K
#define MYAXPY	AXPYU_K
#elif defined HEMV
#define MYDOT	DOTC_K
#define MYAXPY	AXPYU_K
#else
#define MYDOT	DOTU_K
#define MYAXPY	AXPYC_K
#endif

static int spmv_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *buffer, BLASLONG pos){

  FLOAT *a, *x, *y;
  BLASLONG incx;
  BLASLONG m_from, m_to, i;
#ifndef COMPLEX
  FLOAT result;
#else
  OPENBLAS_COMPLEX_FLOAT result;
#endif

  a = (FLOAT *)args -> a;
  x = (FLOAT *)args -> b;
  y = (FLOAT *)args -> c;

  incx = args -> ldb;

  m_from = 0;
  m_to   = args -> m;

  if (range_m) {
    m_from = *(range_m + 0);
    m_to   = *(range_m + 1);
  }

  if (range_n) y += *range_n * COMPSIZE;

  if (incx != 1) {
#ifndef LOWER
    COPY_K(m_to, x, incx, buffer, 1);
#else
    COPY_K(args -> m - m_from, x + m_from * incx * COMPSIZE, incx, buffer + m_from * COMPSIZE, 1);
#endif

    x = buffer;
  }

#ifndef LOWER
  SCAL_K(m_to, 0, 0, ZERO,
#ifdef COMPLEX
	 ZERO,
#endif
	 y, 1, NULL, 0, NULL, 0);
#else
  SCAL_K(args -> m - m_from, 0, 0, ZERO,
#ifdef COMPLEX
	 ZERO,
#endif
	 y + m_from * COMPSIZE, 1, NULL, 0, NULL, 0);
#endif

#ifndef LOWER
  a += (m_from + 1) * m_from / 2  * COMPSIZE;
#else
  a += (2 * args -> m - m_from - 1) * m_from / 2  * COMPSIZE;
#endif

  for (i = m_from; i < m_to; i++) {
#ifndef LOWER

#if !defined(HEMV) && !defined(HEMVREV)
    result = MYDOT(i + 1, a, 1, x, 1);
#else
    result = MYDOT(i    , a, 1, x, 1);
#endif

#ifndef COMPLEX
    *(y + i * COMPSIZE) += result;
#else
#if !defined(HEMV) && !defined(HEMVREV)
    *(y + i * COMPSIZE + 0) += CREAL(result);
    *(y + i * COMPSIZE + 1) += CIMAG(result);
#else
    *(y + i * COMPSIZE + 0) += CREAL(result) + *(a + i * COMPSIZE) * *(x + i * COMPSIZE + 0);
    *(y + i * COMPSIZE + 1) += CIMAG(result) + *(a + i * COMPSIZE) * *(x + i * COMPSIZE + 1);
#endif
#endif

    MYAXPY(i, 0, 0,
	    *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	    *(x + i * COMPSIZE + 1),
#endif
	    a, 1, y, 1, NULL, 0);

    a += (i + 1) * COMPSIZE;

#else
#if !defined(HEMV) && !defined(HEMVREV)
    result = MYDOT(args -> m - i    , a + i * COMPSIZE, 1, x + i * COMPSIZE, 1);
#else
    result = MYDOT(args -> m - i - 1, a + (i + 1) * COMPSIZE, 1, x + (i + 1) * COMPSIZE, 1);
#endif

#ifndef COMPLEX
    *(y + i * COMPSIZE) += result;
#else
#if !defined(HEMV) && !defined(HEMVREV)
    *(y + i * COMPSIZE + 0) += CREAL(result);
    *(y + i * COMPSIZE + 1) += CIMAG(result);
#else
    *(y + i * COMPSIZE + 0) += CREAL(result) + *(a + i * COMPSIZE) * *(x + i * COMPSIZE + 0);
    *(y + i * COMPSIZE + 1) += CIMAG(result) + *(a + i * COMPSIZE) * *(x + i * COMPSIZE + 1);
#endif
#endif

    MYAXPY(args -> m - i - 1, 0, 0,
	    *(x + i * COMPSIZE + 0),
#ifdef COMPLEX
	    *(x + i * COMPSIZE + 1),
#endif
	    a + (i + 1) * COMPSIZE, 1, y + (i + 1) * COMPSIZE, 1, NULL, 0);

    a += (args -> m - i - 1) * COMPSIZE;

#endif
  }

  return 0;
}

#ifndef COMPLEX
int CNAME(BLASLONG m, FLOAT  alpha, FLOAT *a, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
#else
int CNAME(BLASLONG m, FLOAT *alpha, FLOAT *a, FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer, int nthreads){
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

  args.m = m;

  args.a = (void *)a;
  args.b = (void *)x;
  args.c = (void *)buffer;

  args.ldb = incx;
  args.ldc = incy;

  dnum = (double)m * (double)m / (double)nthreads;
  num_cpu  = 0;

#ifndef LOWER

  range_m[MAX_CPU_NUMBER] = m;
  i          = 0;

  while (i < m){

    if (nthreads - num_cpu > 1) {

      double di = (double)(m - i);
      if (di * di - dnum > 0) {
	width = ((BLASLONG)(-sqrt(di * di - dnum) + di) + mask) & ~mask;
      } else {
	width = m - i;
      }

      if (width < 16) width = 16;
      if (width > m - i) width = m - i;

    } else {
      width = m - i;
    }

    range_m[MAX_CPU_NUMBER - num_cpu - 1] = range_m[MAX_CPU_NUMBER - num_cpu] - width;
    range_n[num_cpu] = num_cpu * (((m + 15) & ~15) + 16);
    if (range_n[num_cpu] > m * num_cpu) range_n[num_cpu] = m * num_cpu;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = spmv_kernel;
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

  while (i < m){

    if (nthreads - num_cpu > 1) {

      double di = (double)(m - i);
      if (di * di - dnum > 0) {
	width = ((BLASLONG)(-sqrt(di * di - dnum) + di) + mask) & ~mask;
      } else {
	width = m - i;
      }

      if (width < 16) width = 16;
      if (width > m - i) width = m - i;

    } else {
      width = m - i;
    }

    range_m[num_cpu + 1] = range_m[num_cpu] + width;
    range_n[num_cpu] = num_cpu * (((m + 15) & ~15) + 16);
    if (range_n[num_cpu] > m * num_cpu) range_n[num_cpu] = m * num_cpu;

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = spmv_kernel;
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

  if (num_cpu) {
    queue[0].sa = NULL;
    queue[0].sb = buffer + num_cpu * (((m + 255) & ~255) + 16) * COMPSIZE;

    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  for (i = 1; i < num_cpu; i ++) {

#ifndef LOWER

    AXPYU_K(range_m[MAX_CPU_NUMBER - i], 0, 0, ONE,
#ifdef COMPLEX
	    ZERO,
#endif
	    buffer + range_n[i] * COMPSIZE, 1, buffer, 1, NULL, 0);

#else

    AXPYU_K(m - range_m[i], 0, 0, ONE,
#ifdef COMPLEX
	    ZERO,
#endif
	    buffer + (range_n[i] + range_m[i]) * COMPSIZE, 1, buffer + range_m[i] * COMPSIZE, 1, NULL, 0);

#endif

  }

   AXPYU_K(m, 0, 0,
#ifndef COMPLEX
	  alpha,
#else
	  alpha[0], alpha[1],
#endif
	  buffer, 1, y, incy, NULL, 0);

  return 0;
}
