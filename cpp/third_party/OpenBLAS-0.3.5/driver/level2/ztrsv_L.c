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
#include <ctype.h>
#include "common.h"

const static FLOAT dm1 = -1.;

int CNAME(BLASLONG m, FLOAT *a, BLASLONG lda, FLOAT *b, BLASLONG incb, void *buffer){

  BLASLONG i, is, min_i;
#if (TRANSA == 2) || (TRANSA == 4)
  OPENBLAS_COMPLEX_FLOAT result;
#endif
#ifndef UNIT
  FLOAT ar, ai, br, bi, ratio, den;
#endif
  FLOAT *gemvbuffer = (FLOAT *)buffer;
  FLOAT *B = b;

  if (incb != 1) {
    B = buffer;
    gemvbuffer = (FLOAT *)(((BLASLONG)buffer + m * sizeof(FLOAT) * 2 + 4095) & ~4095);
    COPY_K(m, b, incb, buffer, 1);
  }

  for (is =0; is < m; is += DTB_ENTRIES){

    min_i = MIN(m - is, DTB_ENTRIES);

#if (TRANSA == 2) || (TRANSA == 4)
    if (is > 0){
#if TRANSA == 2
      GEMV_T(is, min_i, 0, dm1, ZERO,
	      a + is * lda * COMPSIZE,  lda,
	      B,                 1,
	      B + is * COMPSIZE, 1, gemvbuffer);
#else
      GEMV_C(is, min_i, 0, dm1, ZERO,
	      a + is * lda  * COMPSIZE,  lda,
	      B,                 1,
	      B + is * COMPSIZE, 1, gemvbuffer);
#endif
    }
#endif

    for (i = 0; i < min_i; i++) {
      FLOAT *AA = a + (is + (i + is) * lda) * COMPSIZE;
      FLOAT *BB = B + is * COMPSIZE;

#if (TRANSA == 2) || (TRANSA == 4)
      if (i > 0) {
#if TRANSA == 2
      result = DOTU_K(i, AA, 1, BB, 1);
#else
      result = DOTC_K(i, AA, 1, BB, 1);
#endif

      BB[i * COMPSIZE + 0] -= CREAL(result);
      BB[i * COMPSIZE + 1] -= CIMAG(result);
    }
#endif

#ifndef UNIT
      ar = AA[i * COMPSIZE + 0];
      ai = AA[i * COMPSIZE + 1];

      if (fabs(ar) >= fabs(ai)){
	ratio = ai / ar;
	den = 1./(ar * ( 1 + ratio * ratio));

	ar =  den;
#if TRANSA < 3
	ai = -ratio * den;
#else
	ai =  ratio * den;
#endif
      } else {
	ratio = ar / ai;
	den = 1./(ai * ( 1 + ratio * ratio));
	ar =  ratio * den;
#if TRANSA < 3
	ai = -den;
#else
	ai =  den;
#endif
    }

      br = BB[i * COMPSIZE + 0];
      bi = BB[i * COMPSIZE + 1];

      BB[i * COMPSIZE + 0] = ar*br - ai*bi;
      BB[i * COMPSIZE + 1] = ar*bi + ai*br;
#endif


#if (TRANSA == 1) || (TRANSA == 3)
      if (i < min_i - 1) {
#if TRANSA == 1
	AXPYU_K(min_i - i  - 1 , 0, 0,
	       - BB[i * COMPSIZE + 0], - BB[i * COMPSIZE + 1],
	       AA + (i + 1) * COMPSIZE, 1, BB + (i + 1) * COMPSIZE, 1, NULL, 0);
#else
	AXPYC_K(min_i - i  - 1 , 0, 0,
	       - BB[i * COMPSIZE + 0], - BB[i * COMPSIZE + 1],
	       AA + (i + 1) * COMPSIZE, 1, BB + (i + 1) * COMPSIZE, 1, NULL, 0);
#endif
    }
#endif
    }

#if (TRANSA == 1) || (TRANSA == 3)
    if (m - is > min_i){
#if   TRANSA == 1
      GEMV_N(m - is - min_i, min_i, 0, dm1, ZERO,
	      a + (is + min_i + is * lda) * COMPSIZE, lda,
	      B + is           * COMPSIZE, 1,
	      B + (is + min_i) * COMPSIZE, 1, gemvbuffer);
#else
      GEMV_R(m - is - min_i, min_i, 0, dm1, ZERO,
	      a + (is + min_i + is * lda) * COMPSIZE, lda,
	      B + is           * COMPSIZE, 1,
	      B + (is + min_i) * COMPSIZE, 1, gemvbuffer);
#endif
    }
#endif
  }

  if (incb != 1) {
    COPY_K(m, buffer, 1, b, incb);
  }

  return 0;
}

