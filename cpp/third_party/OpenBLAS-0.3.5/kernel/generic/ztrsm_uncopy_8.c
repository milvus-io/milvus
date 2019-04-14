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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj, k;

  FLOAT *a1, *a2,  *a3,  *a4,  *a5,  *a6,  *a7,  *a8;
  FLOAT data1, data2;

  lda *= 2;
  jj = offset;

  j = (n >> 3);
  while (j > 0){

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a5  = a +  4 * lda;
    a6  = a +  5 * lda;
    a7  = a +  6 * lda;
    a8  = a +  7 * lda;

    a += 8 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 8)) {

	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 8; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
	*(b +  4) = *(a3  +  0);
	*(b +  5) = *(a3  +  1);
	*(b +  6) = *(a4  +  0);
	*(b +  7) = *(a4  +  1);
	*(b +  8) = *(a5  +  0);
	*(b +  9) = *(a5  +  1);
	*(b + 10) = *(a6  +  0);
	*(b + 11) = *(a6  +  1);
	*(b + 12) = *(a7  +  0);
	*(b + 13) = *(a7  +  1);
	*(b + 14) = *(a8  +  0);
	*(b + 15) = *(a8  +  1);
      }

      a1  += 2;
      a2  += 2;
      a3  += 2;
      a4  += 2;
      a5  += 2;
      a6  += 2;
      a7  += 2;
      a8  += 2;
      b  += 16;
      ii ++;
    }

    jj += 8;
    j --;
  }

  if (n & 4) {

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a += 4 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 4)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 4; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
	*(b +  4) = *(a3  +  0);
	*(b +  5) = *(a3  +  1);
	*(b +  6) = *(a4  +  0);
	*(b +  7) = *(a4  +  1);
      }

      a1  += 2;
      a2  += 2;
      a3  += 2;
      a4  += 2;
      b  += 8;
      ii ++;
    }

    jj += 4;
  }

  if (n & 2) {

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a += 2 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 2)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
	for (k = ii - jj + 1; k < 2; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
      }

      a1  += 2;
      a2  += 2;
      b  += 4;
      ii ++;
    }

    jj += 2;
  }

  if (n & 1) {

    a1  = a +  0 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 1)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
	for (k = ii - jj + 1; k < 1; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
      }

      a1  += 2;
      b  += 2;
      ii ++;
    }
  }

  return 0;
}
