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

static FLOAT dp1 =  1.;
static FLOAT dm1 = -1.;

blasint CNAME(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *sa, FLOAT *sb, BLASLONG myid) {

  BLASLONG m, n, lda;
  blasint *ipiv, offset;
  FLOAT *a;

  FLOAT temp1, temp2;
  blasint i, j;
  blasint ip, jp;
  blasint info;
  BLASLONG len;
  FLOAT *b;

  m      = args -> m;
  n      = args -> n;
  a      = (FLOAT *)args -> a;
  lda    = args -> lda;
  ipiv   = (blasint *)args -> c;
  offset = 0;

  if (range_n) {
    m     -= range_n[0];
    n      = range_n[1] - range_n[0];
    offset = range_n[0];
    a     += range_n[0] * (lda + 1) * COMPSIZE;
  }

  info = 0;
  b = a;

  for (j = 0; j < n; j++) {

    len = MIN(j, m);

    for (i = 0; i < len; i++) {
      ip = ipiv[i + offset] - 1 - offset;
      if (ip != i) {
	temp1 = *(b + i);
	temp2 = *(b + ip);
	*(b + i) = temp2;
	*(b + ip) = temp1;
      }
    }

    for (i = 1; i < len; i++) {
      b[i] -= DOTU_K(i, a + i, lda, b, 1);
    }

    if (j < m) {
      GEMV_N(m - j, j, 0, dm1,  a + j, lda, b, 1, b + j, 1, sb);

      jp = j + IAMAX_K(m - j, b + j, 1);
      if (jp>m) jp = m;        //avoid out of boundary
      ipiv[j + offset] = jp + offset;
      jp--;
      temp1 = *(b + jp);

      if (temp1 != ZERO) {
	temp1 = dp1 / temp1;

	if (jp != j) {
	  SWAP_K(j + 1, 0, 0, ZERO, a + j, lda, a + jp, lda, NULL, 0);
	}
	if (j + 1 < m) {
	  SCAL_K(m - j - 1, 0, 0, temp1, b + j + 1, 1, NULL, 0, NULL, 0);
	}
      } else {
	if (!info) info = j + 1;
      }
    }
    b += lda;
  }
  return info;
}
