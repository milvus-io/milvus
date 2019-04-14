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

#include "common.h"

#ifndef UNIT
#define INV(a) (ONE / (a))
#else
#define INV(a) (ONE)
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj, k;

  FLOAT *a1, *a2,  *a3,  *a4,  *a5,  *a6,  *a7,  *a8;
  FLOAT *a9, *a10, *a11, *a12, *a13, *a14, *a15, *a16;


  jj = offset;

  j = (n >> 4);
  while (j > 0){

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a5  = a +  4 * lda;
    a6  = a +  5 * lda;
    a7  = a +  6 * lda;
    a8  = a +  7 * lda;
    a9  = a +  8 * lda;
    a10 = a +  9 * lda;
    a11 = a + 10 * lda;
    a12 = a + 11 * lda;
    a13 = a + 12 * lda;
    a14 = a + 13 * lda;
    a15 = a + 14 * lda;
    a16 = a + 15 * lda;

    a += 16 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 16)) {
	*(b +  ii - jj) = INV(*(a1 + (ii - jj) * lda));
	for (k = ii - jj + 1; k < 16; k ++) {
	  *(b +  k) = *(a1 +  k * lda);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a2  +  0);
	*(b +  2) = *(a3  +  0);
	*(b +  3) = *(a4  +  0);
	*(b +  4) = *(a5  +  0);
	*(b +  5) = *(a6  +  0);
	*(b +  6) = *(a7  +  0);
	*(b +  7) = *(a8  +  0);
	*(b +  8) = *(a9  +  0);
	*(b +  9) = *(a10 +  0);
	*(b + 10) = *(a11 +  0);
	*(b + 11) = *(a12 +  0);
	*(b + 12) = *(a13 +  0);
	*(b + 13) = *(a14 +  0);
	*(b + 14) = *(a15 +  0);
	*(b + 15) = *(a16 +  0);
      }

      a1  ++;
      a2  ++;
      a3  ++;
      a4  ++;
      a5  ++;
      a6  ++;
      a7  ++;
      a8  ++;
      a9  ++;
      a10 ++;
      a11 ++;
      a12 ++;
      a13 ++;
      a14 ++;
      a15 ++;
      a16 ++;
      b  += 16;
      ii ++;
    }

    jj += 16;
    j --;
  }

  if (n & 8) {

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
	*(b +  ii - jj) = INV(*(a1 + (ii - jj) * lda));
	for (k = ii - jj + 1; k < 8; k ++) {
	  *(b +  k) = *(a1 +  k * lda);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a2  +  0);
	*(b +  2) = *(a3  +  0);
	*(b +  3) = *(a4  +  0);
	*(b +  4) = *(a5  +  0);
	*(b +  5) = *(a6  +  0);
	*(b +  6) = *(a7  +  0);
	*(b +  7) = *(a8  +  0);
      }

      a1  ++;
      a2  ++;
      a3  ++;
      a4  ++;
      a5  ++;
      a6  ++;
      a7  ++;
      a8  ++;
      b  += 8;
      ii ++;
    }

    jj += 8;
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
	*(b +  ii - jj) = INV(*(a1 + (ii - jj) * lda));
	for (k = ii - jj + 1; k < 4; k ++) {
	  *(b +  k) = *(a1 +  k * lda);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a2  +  0);
	*(b +  2) = *(a3  +  0);
	*(b +  3) = *(a4  +  0);
      }

      a1  ++;
      a2  ++;
      a3  ++;
      a4  ++;
      b  += 4;
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
	*(b +  ii - jj) = INV(*(a1 + (ii - jj) * lda));
	for (k = ii - jj + 1; k < 2; k ++) {
	  *(b +  k) = *(a1 +  k * lda);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a2  +  0);
      }

      a1  ++;
      a2  ++;
      b  += 2;
      ii ++;
    }

    jj += 2;
  }

  if (n & 1) {

    a1  = a +  0 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 1)) {
	*(b +  ii - jj) = INV(*(a1 + (ii - jj) * lda));
	for (k = ii - jj + 1; k < 1; k ++) {
	  *(b +  k) = *(a1 +  k * lda);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
      }

      a1  ++;
      b  += 1;
      ii ++;
    }
  }

  return 0;
}
