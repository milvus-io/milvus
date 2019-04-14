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
#include <math.h>
#include "common.h"

int CNAME(int mode, blas_arg_t *arg, BLASLONG *range_m, BLASLONG *range_n, int (*function)(), void *sa, void *sb, BLASLONG nthreads) {

  blas_queue_t queue[MAX_CPU_NUMBER];
  BLASLONG range[MAX_CPU_NUMBER + 1];

  BLASLONG width, i;
  BLASLONG n_from, n_to;
  double dnum, nf, nt, di, dinum;

  int num_cpu;
  int mask = 0;

  if (!(mode & BLAS_COMPLEX)) {

    switch (mode & BLAS_PREC) {
    case BLAS_SINGLE:
      mask = SGEMM_UNROLL_MN - 1;
      break;
    case BLAS_DOUBLE:
      mask = DGEMM_UNROLL_MN - 1;
      break;
#ifdef EXPRECISION
    case BLAS_XDOUBLE:
      mask = MAX(QGEMM_UNROLL_M, QGEMM_UNROLL_N) - 1;
      break;
#endif
    }
  } else {
    switch (mode & BLAS_PREC) {
    case BLAS_SINGLE:
      mask = CGEMM_UNROLL_MN - 1;
      break;
    case BLAS_DOUBLE:
      mask = ZGEMM_UNROLL_MN - 1;
      break;
#ifdef EXPRECISION
    case BLAS_XDOUBLE:
      mask = MAX(XGEMM_UNROLL_M, XGEMM_UNROLL_N) - 1;
      break;
#endif
    }
  }

  n_from = 0;
  n_to   = arg -> n;

  if (range_n) {
    n_from = *(range_n + 0);
    n_to   = *(range_n + 1);
  }

  if (!(mode & BLAS_UPLO)) {

    nf = (double)(n_from);
    nt = (double)(n_to);

    dnum = (nt * nt - nf * nf) / (double)nthreads;

    num_cpu  = 0;

    range[0] = n_from;
    i        = n_from;

    while (i < n_to){

      if (nthreads - num_cpu > 1) {

	di = (double)i;
	dinum = di * di +dnum;
	if (dinum <0)
	  width = (BLASLONG)(( - di + mask)/(mask+1)) * (mask+1);
	else
	  width = (BLASLONG)(( sqrt(dinum) - di + mask)/(mask+1)) * (mask+1);

	if ((width <= 0) || (width > n_to - i)) width = n_to - i;

      } else {
	width = n_to - i;
      }

      range[num_cpu + 1] = range[num_cpu] + width;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = function;
      queue[num_cpu].args    = arg;
      queue[num_cpu].range_m = range_m;
      queue[num_cpu].range_n = &range[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i += width;
    }

  } else {

    nf = (double)(arg -> n - n_from);
    nt = (double)(arg -> n - n_to);
    dnum = (nt * nt - nf * nf) / (double)nthreads;
    num_cpu  = 0;

    range[0] = n_from;
    i        = n_from;

    while (i < n_to){

      if (nthreads - num_cpu > 1) {

	di = (double)(arg -> n - i);
	dinum = di * di + dnum;
	if (dinum<0)
	  width = ((BLASLONG)(di + mask)/(mask+1)) * (mask+1);
	else
	  width = ((BLASLONG)((-sqrt(dinum) + di) + mask)/(mask+1)) * (mask+1);
	if ((width <= 0) || (width > n_to - i)) width = n_to - i;

      } else {
	width = n_to - i;
      }

      range[num_cpu + 1] = range[num_cpu] + width;

      queue[num_cpu].mode    = mode;
      queue[num_cpu].routine = function;
      queue[num_cpu].args    = arg;
      queue[num_cpu].range_m = range_m;
      queue[num_cpu].range_n = &range[num_cpu];
      queue[num_cpu].sa      = NULL;
      queue[num_cpu].sb      = NULL;
      queue[num_cpu].next    = &queue[num_cpu + 1];

      num_cpu ++;
      i += width;
    }

  }

  if (num_cpu) {
    queue[0].sa = sa;
    queue[0].sb = sb;
    queue[num_cpu - 1].next = NULL;

    exec_blas(num_cpu, queue);
  }

  return 0;
}
