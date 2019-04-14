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

static int inner_thread(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n,
			 FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

  BLASLONG n   = args -> n;
  BLASLONG off = 0;

  if (range_n) {
    n   = range_n[1] - range_n[0];
    off = range_n[0];
  }

#if   TRANS == 1
  LASWP_PLUS(n, 1, args -> m, ZERO, ZERO,
	     (FLOAT *)args -> b + off * args -> ldb * COMPSIZE, args -> ldb, NULL, 0, args -> c, 1);
  TRSM_LNLU (args, range_m, range_n, sa, sb, 0);
  TRSM_LNUN (args, range_m, range_n, sa, sb, 0);
#elif TRANS == 2
  TRSM_LTUN  (args, range_m, range_n, sa, sb, 0);
  TRSM_LTLU  (args, range_m, range_n, sa, sb, 0);
  LASWP_MINUS(n, 1, args -> m, ZERO,  ZERO,
	      (FLOAT *)args -> b + off * args -> ldb * COMPSIZE, args -> ldb, NULL, 0, args -> c, -1);
#elif TRANS == 3
  LASWP_PLUS(n, 1, args -> m, ZERO,  ZERO,
	     (FLOAT *)args -> b + off * args -> ldb * COMPSIZE, args -> ldb, NULL, 0, args -> c, 1);
  TRSM_LRLU (args, range_m, range_n, sa, sb, 0);
  TRSM_LRUN (args, range_m, range_n, sa, sb, 0);
#else
  TRSM_LCUN  (args, range_m, range_n, sa, sb, 0);
  TRSM_LCLU  (args, range_m, range_n, sa, sb, 0);
  LASWP_MINUS(n, 1, args -> m, ZERO,  ZERO,
	      (FLOAT *)args -> b + off * args -> ldb * COMPSIZE, args -> ldb, NULL, 0, args -> c, -1);
#endif

  return 0;
}

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG mypos) {

  int mode;

    if (args -> n == 1){
#if TRANS == 1
      LASWP_PLUS(1, 1, args -> m, ZERO, ZERO, args -> b, args -> ldb, NULL, 0, args -> c, 1);
      ZTRSV_NLU (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      ZTRSV_NUN (args -> m, args -> a, args -> lda, args -> b, 1, sb);
#elif TRANS == 2
      ZTRSV_TUN (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      ZTRSV_TLU (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      LASWP_MINUS(1, 1, args -> m, ZERO, ZERO, args -> b, args -> ldb, NULL, 0, args -> c, -1);
#elif TRANS == 3
      LASWP_PLUS(1, 1, args -> m, ZERO, ZERO, args -> b, args -> ldb, NULL, 0, args -> c, 1);
      ZTRSV_RLU (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      ZTRSV_RUN (args -> m, args -> a, args -> lda, args -> b, 1, sb);
#else
      ZTRSV_CUN (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      ZTRSV_CLU (args -> m, args -> a, args -> lda, args -> b, 1, sb);
      LASWP_MINUS(1, 1, args -> m, ZERO, ZERO, args -> b, args -> ldb, NULL, 0, args -> c, -1);
#endif
    } else {
#ifdef XDOUBLE
      mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
      mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
      mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif

      gemm_thread_n(mode, args, NULL, NULL, inner_thread, sa, sb, args -> nthreads);
    }

   return 0;
  }
