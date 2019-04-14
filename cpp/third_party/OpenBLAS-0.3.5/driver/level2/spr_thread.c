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

static int syr_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *buffer, BLASLONG pos){

  FLOAT *a, *x;
  BLASLONG incx;
  BLASLONG i, m_from, m_to;
  FLOAT alpha_r;
#if defined(COMPLEX) && !defined(HEMV) && !defined(HEMVREV)
  FLOAT alpha_i;
#endif

  x = (FLOAT *)args -> a;
  a = (FLOAT *)args -> b;

  incx = args -> lda;

  alpha_r = *((FLOAT *)args -> alpha + 0);
#if defined(COMPLEX) && !defined(HEMV) && !defined(HEMVREV)
  alpha_i = *((FLOAT *)args -> alpha + 1);
#endif

  m_from = 0;
  m_to   = args -> m;

  if (range_m) {
    m_from = *(range_m + 0);
    m_to   = *(range_m + 1);
  }

  if (incx != 1) {
#ifndef LOWER
    COPY_K(m_to, x, incx, buffer, 1);
#else
    COPY_K(args -> m - m_from, x + m_from * incx * COMPSIZE, incx, buffer + m_from * COMPSIZE, 1);
#endif
    x = buffer;
  }

#ifndef LOWER
  a += (m_from + 1) * m_from / 2  * COMPSIZE;
#else
  a += (2 * args -> m - m_from + 1) * m_from / 2  * COMPSIZE;
#endif

  for (i = m_from; i < m_to; i++){
#if !defined(HEMV) && !defined(HEMVREV)
#ifndef COMPLEX
    if (x[i] != ZERO) {
#ifndef LOWER
      AXPYU_K(i + 1,         0, 0, alpha_r * x[i], x,     1, a, 1, NULL, 0);
#else
      AXPYU_K(args -> m - i, 0, 0, alpha_r * x[i], x + i, 1, a, 1, NULL, 0);
#endif
    }
#else
    if ((x[i * COMPSIZE + 0] != ZERO) || (x[i * COMPSIZE + 1] != ZERO)) {
#ifndef LOWER
      AXPYU_K(i + 1, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0] - alpha_i * x[i * COMPSIZE + 1],
	      alpha_i * x[i * COMPSIZE + 0] + alpha_r * x[i * COMPSIZE + 1],
	      x,                1, a, 1, NULL, 0);
#else
      AXPYU_K(args -> m - i, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0] - alpha_i * x[i * COMPSIZE + 1],
	      alpha_i * x[i * COMPSIZE + 0] + alpha_r * x[i * COMPSIZE + 1],
	      x + i * COMPSIZE, 1, a, 1, NULL, 0);
#endif
    }
#endif
#else
    if ((x[i * COMPSIZE + 0] != ZERO) || (x[i * COMPSIZE + 1] != ZERO)) {
#ifndef HEMVREV
#ifndef LOWER
      AXPYU_K(i + 1, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0], - alpha_r * x[i * COMPSIZE + 1],
	      x,                1, a, 1, NULL, 0);
#else
      AXPYU_K(args -> m - i, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0], - alpha_r * x[i * COMPSIZE + 1],
	      x + i * COMPSIZE, 1, a, 1, NULL, 0);
#endif
#else
#ifndef LOWER
      AXPYC_K(i + 1, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0],   alpha_r * x[i * COMPSIZE + 1],
	      x,                1, a, 1, NULL, 0);
#else
      AXPYC_K(args -> m - i, 0, 0,
	      alpha_r * x[i * COMPSIZE + 0],   alpha_r * x[i * COMPSIZE + 1],
	      x + i * COMPSIZE, 1, a, 1, NULL, 0);
#endif
#endif
    }
#ifndef LOWER
    a[i * COMPSIZE + 1] = ZERO;
#else
    a[               1] = ZERO;
#endif
#endif

#ifndef LOWER
    a += (i + 1) * COMPSIZE;
#else
    a += (args -> m - i) * COMPSIZE;
#endif
  }

  return 0;
}

#if !defined(COMPLEX) || defined(HEMV) || defined(HEMVREV)
int CNAME(BLASLONG m, FLOAT  alpha, FLOAT *x, BLASLONG incx, FLOAT *a, FLOAT *buffer, int nthreads){
#else
int CNAME(BLASLONG m, FLOAT *alpha, FLOAT *x, BLASLONG incx, FLOAT *a, FLOAT *buffer, int nthreads){
#endif

  blas_arg_t args;
  blas_queue_t queue[MAX_CPU_NUMBER];
  BLASLONG range_m[MAX_CPU_NUMBER + 1];

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

  args.a = (void *)x;
  args.b = (void *)a;

  args.lda = incx;

#if !defined(COMPLEX) || defined(HEMV) || defined(HEMVREV)
  args.alpha = (void *)&alpha;
#else
  args.alpha = (void *)alpha;
#endif

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

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = syr_kernel;
    queue[num_cpu].args    = &args;
    queue[num_cpu].range_m = &range_m[MAX_CPU_NUMBER - num_cpu - 1];
    queue[num_cpu].range_n = NULL;
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

    queue[num_cpu].mode    = mode;
    queue[num_cpu].routine = syr_kernel;
    queue[num_cpu].args    = &args;
    queue[num_cpu].range_m = &range_m[num_cpu];
    queue[num_cpu].range_n = NULL;
    queue[num_cpu].sa      = NULL;
    queue[num_cpu].sb      = NULL;
    queue[num_cpu].next    = &queue[num_cpu + 1];

    num_cpu ++;
    i += width;
  }

#endif

  if (num_cpu) {
    queue[0].sa = NULL;
    queue[0].sb = buffer;

    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  return 0;
}
