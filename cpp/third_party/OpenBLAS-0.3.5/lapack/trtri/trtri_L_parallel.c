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

#ifdef UNIT
#define TRTI2	TRTI2_LU
#define TRMM	TRMM_LNLU
#define TRSM	TRSM_RNLU
#else
#define TRTI2	TRTI2_LN
#define TRMM	TRMM_LNLN
#define TRSM	TRSM_RNLN
#endif

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

  BLASLONG n, info;
  BLASLONG bk, i, blocking, start_i;
  int mode;
  BLASLONG lda, range_N[2];
  blas_arg_t newarg;
  FLOAT *a;
  FLOAT alpha[2] = { ONE, ZERO};
  FLOAT beta [2] = {-ONE, ZERO};

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

  n = args -> n;
  a  = (FLOAT *)args -> a;
  lda = args -> lda;

  if (range_n)  n = range_n[1] - range_n[0];

  if (n <= DTB_ENTRIES) {
    info = TRTI2(args, NULL, range_n, sa, sb, 0);
    return info;
  }

  blocking = GEMM_Q;
  if (n < 4 * GEMM_Q) blocking = (n + 3) / 4;

  start_i = 0;
  while (start_i < n) start_i += blocking;
  start_i -= blocking;

  for (i = start_i; i >= 0; i -= blocking) {
    bk = n - i;
    if (bk > blocking) bk = blocking;

    range_N[0] = i;
    range_N[1] = i + bk;

    newarg.lda = lda;
    newarg.ldb = lda;
    newarg.ldc = lda;
    newarg.alpha = alpha;

    newarg.m = n - bk - i;
    newarg.n = bk;
    newarg.a = a + ( i       + i * lda) * COMPSIZE;
    newarg.b = a + ((i + bk) + i * lda) * COMPSIZE;

    newarg.beta  = beta;
    newarg.nthreads = args -> nthreads;

    gemm_thread_m(mode, &newarg, NULL, NULL, TRSM, sa, sb, args -> nthreads);

    newarg.m = bk;
    newarg.n = bk;

    newarg.a = a + (i + i * lda) * COMPSIZE;

    CNAME  (&newarg, NULL, NULL, sa, sb, 0);

    newarg.m = n - bk - i;
    newarg.n = i;
    newarg.k = bk;

    newarg.a = a + (i + bk + i * lda) * COMPSIZE;
    newarg.b = a + (i               ) * COMPSIZE;
    newarg.c = a + (i + bk          ) * COMPSIZE;

    newarg.beta  = NULL;

    gemm_thread_n(mode, &newarg, NULL, NULL, GEMM_NN, sa, sb, args -> nthreads);

    newarg.a = a + (i      + i * lda) * COMPSIZE;
    newarg.b = a + (i               ) * COMPSIZE;

    newarg.m = bk;
    newarg.n = i;

    gemm_thread_n(mode, &newarg, NULL, NULL, TRMM, sa, sb, args -> nthreads);
  }


  return 0;
}
