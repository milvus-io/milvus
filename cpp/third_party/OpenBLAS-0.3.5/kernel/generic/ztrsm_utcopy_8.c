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

  FLOAT *a1, data1, data2;

  lda *= 2;

  jj = offset;

  j = (n >> 3);
  while (j > 0){

    a1 = a;
    a += 16;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 8)) {
	for (k = 0; k < ii - jj; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
      }

      if (ii - jj >= 8) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
	*(b +  4) = *(a1 +  4);
	*(b +  5) = *(a1 +  5);
	*(b +  6) = *(a1 +  6);
	*(b +  7) = *(a1 +  7);
	*(b +  8) = *(a1 +  8);
	*(b +  9) = *(a1 +  9);
	*(b + 10) = *(a1 + 10);
	*(b + 11) = *(a1 + 11);
	*(b + 12) = *(a1 + 12);
	*(b + 13) = *(a1 + 13);
	*(b + 14) = *(a1 + 14);
	*(b + 15) = *(a1 + 15);
      }

      b  += 16;
      a1 += lda;
      ii ++;
    }

    jj += 8;
    j --;
  }

  j = (n & 4);
  if (j > 0) {

    a1 = a;
    a += 8;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 4)) {
	for (k = 0; k < ii - jj; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
      }

      if (ii - jj >= 4) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
	*(b +  4) = *(a1 +  4);
	*(b +  5) = *(a1 +  5);
	*(b +  6) = *(a1 +  6);
	*(b +  7) = *(a1 +  7);
      }

      b  += 8;
      a1 += lda;
      ii ++;
    }

    jj += 4;
  }

  j = (n & 2);
  if (j > 0) {

    a1 = a;
    a += 4;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 2)) {
	for (k = 0; k < ii - jj; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
      }

      if (ii - jj >= 2) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
      }

      b  += 4;
      a1 += lda;
      ii ++;
    }

    jj += 2;
  }

  j = (n & 1);
  if (j > 0) {

    a1 = a;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 1)) {
	for (k = 0; k < ii - jj; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
      }

      if (ii - jj >= 1) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
      }

      b  += 2;
      a1 += lda;
      ii ++;
    }
  }

  return 0;
}
