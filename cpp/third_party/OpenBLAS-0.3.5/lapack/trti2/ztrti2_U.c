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
#define ZTRMV	ZTRMV_NUU
#else
#define ZTRMV	ZTRMV_NUN
#endif

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG  n, lda;
  FLOAT *a;

  FLOAT ajj_r, ajj_i;
#ifndef UNIT
  FLOAT ratio, den;
#endif
  BLASLONG j;

  n      = args -> n;
  a      = (FLOAT *)args -> a;
  lda    = args -> lda;

  if (range_n) {
    n      = range_n[1] - range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  for (j = 0; j < n; j++) {


#ifndef UNIT
    ajj_r = *(a + (j + j * lda) * COMPSIZE + 0);
    ajj_i = *(a + (j + j * lda) * COMPSIZE + 1);


  if (fabs(ajj_r) >= fabs(ajj_i)){
    ratio = ajj_i / ajj_r;
    den   = 1. / (ajj_r * ( 1 + ratio * ratio));
    ajj_r =  den;
    ajj_i = -ratio * den;
  } else {
    ratio = ajj_r / ajj_i;
    den   = 1. /(ajj_i * ( 1 + ratio * ratio));
    ajj_r =  ratio * den;
    ajj_i = -den;
  }

  *(a + (j + j * lda) * COMPSIZE + 0) = ajj_r;
  *(a + (j + j * lda) * COMPSIZE + 1) = ajj_i;
#else
    ajj_r =  ONE;
    ajj_i =  ZERO;
#endif

  ZTRMV (j,
	 a                     , lda,
	 a + j * lda * COMPSIZE, 1,
	 sb);

  SCAL_K(j, 0, 0,
	  -ajj_r, -ajj_i,
	  a + j * lda * COMPSIZE, 1,
	  NULL, 0, NULL, 0);

  }

  return 0;
}
