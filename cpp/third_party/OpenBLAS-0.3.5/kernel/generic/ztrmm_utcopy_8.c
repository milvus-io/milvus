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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

  BLASLONG i, js, ii;
  BLASLONG X;

  FLOAT *a01, *a02, *a03 ,*a04, *a05, *a06, *a07, *a08;

  lda *= 2;

  js = (n >> 3);

  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY +  0) * lda;
	a02 = a + posX * 2 + (posY +  1) * lda;
	a03 = a + posX * 2 + (posY +  2) * lda;
	a04 = a + posX * 2 + (posY +  3) * lda;
	a05 = a + posX * 2 + (posY +  4) * lda;
	a06 = a + posX * 2 + (posY +  5) * lda;
	a07 = a + posX * 2 + (posY +  6) * lda;
	a08 = a + posX * 2 + (posY +  7) * lda;
      } else {
	a01 = a + posY * 2 + (posX +  0) * lda;
	a02 = a + posY * 2 + (posX +  1) * lda;
	a03 = a + posY * 2 + (posX +  2) * lda;
	a04 = a + posY * 2 + (posX +  3) * lda;
	a05 = a + posY * 2 + (posX +  4) * lda;
	a06 = a + posY * 2 + (posX +  5) * lda;
	a07 = a + posY * 2 + (posX +  6) * lda;
	a08 = a + posY * 2 + (posX +  7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X < posY) {
	    a01 += 16;
	    a02 += 16;
	    a03 += 16;
	    a04 += 16;
	    a05 += 16;
	    a06 += 16;
	    a07 += 16;
	    a08 += 16;
	    b += 128;
	  } else
	    if (X > posY) {

	      for (ii = 0; ii < 8; ii++){

		b[  0] = *(a01 +  0);
		b[  1] = *(a01 +  1);
		b[  2] = *(a01 +  2);
		b[  3] = *(a01 +  3);
		b[  4] = *(a01 +  4);
		b[  5] = *(a01 +  5);
		b[  6] = *(a01 +  6);
		b[  7] = *(a01 +  7);

		b[  8] = *(a01 +  8);
		b[  9] = *(a01 +  9);
		b[ 10] = *(a01 + 10);
		b[ 11] = *(a01 + 11);
		b[ 12] = *(a01 + 12);
		b[ 13] = *(a01 + 13);
		b[ 14] = *(a01 + 14);
		b[ 15] = *(a01 + 15);

		a01 += lda;
		b += 16;
	      }

	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;

	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;

	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
	      b[ 14] = ZERO;
	      b[ 15] = ZERO;

	      b[ 16] = *(a02 +  0);
	      b[ 17] = *(a02 +  1);
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
#endif
	      b[ 20] = ZERO;
	      b[ 21] = ZERO;
	      b[ 22] = ZERO;
	      b[ 23] = ZERO;
	      b[ 24] = ZERO;
	      b[ 25] = ZERO;
	      b[ 26] = ZERO;
	      b[ 27] = ZERO;
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
	      b[ 30] = ZERO;
	      b[ 31] = ZERO;

	      b[ 32] = *(a03 +  0);
	      b[ 33] = *(a03 +  1);
	      b[ 34] = *(a03 +  2);
	      b[ 35] = *(a03 +  3);
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(a03 +  4);
	      b[ 37] = *(a03 +  5);
#endif
	      b[ 38] = ZERO;
	      b[ 39] = ZERO;
	      b[ 40] = ZERO;
	      b[ 41] = ZERO;
	      b[ 42] = ZERO;
	      b[ 43] = ZERO;
	      b[ 44] = ZERO;
	      b[ 45] = ZERO;
	      b[ 46] = ZERO;
	      b[ 47] = ZERO;

	      b[ 48] = *(a04 +  0);
	      b[ 49] = *(a04 +  1);
	      b[ 50] = *(a04 +  2);
	      b[ 51] = *(a04 +  3);
	      b[ 52] = *(a04 +  4);
	      b[ 53] = *(a04 +  5);
#ifdef UNIT
	      b[ 54] = ONE;
	      b[ 55] = ZERO;
#else
	      b[ 54] = *(a04 +  6);
	      b[ 55] = *(a04 +  7);
#endif
	      b[ 56] = ZERO;
	      b[ 57] = ZERO;
	      b[ 58] = ZERO;
	      b[ 59] = ZERO;
	      b[ 60] = ZERO;
	      b[ 61] = ZERO;
	      b[ 62] = ZERO;
	      b[ 63] = ZERO;

	      b[ 64] = *(a05 +  0);
	      b[ 65] = *(a05 +  1);
	      b[ 66] = *(a05 +  2);
	      b[ 67] = *(a05 +  3);
	      b[ 68] = *(a05 +  4);
	      b[ 69] = *(a05 +  5);
	      b[ 70] = *(a05 +  6);
	      b[ 71] = *(a05 +  7);
#ifdef UNIT
	      b[ 72] = ONE;
	      b[ 73] = ZERO;
#else
	      b[ 72] = *(a05 +  8);
	      b[ 73] = *(a05 +  9);
#endif
	      b[ 74] = ZERO;
	      b[ 75] = ZERO;
	      b[ 76] = ZERO;
	      b[ 77] = ZERO;
	      b[ 78] = ZERO;
	      b[ 79] = ZERO;

	      b[ 80] = *(a06 +  0);
	      b[ 81] = *(a06 +  1);
	      b[ 82] = *(a06 +  2);
	      b[ 83] = *(a06 +  3);
	      b[ 84] = *(a06 +  4);
	      b[ 85] = *(a06 +  5);
	      b[ 86] = *(a06 +  6);
	      b[ 87] = *(a06 +  7);
	      b[ 88] = *(a06 +  8);
	      b[ 89] = *(a06 +  9);
#ifdef UNIT
	      b[ 90] = ONE;
	      b[ 91] = ZERO;
#else
	      b[ 90] = *(a06 + 10);
	      b[ 91] = *(a06 + 11);
#endif
	      b[ 92] = ZERO;
	      b[ 93] = ZERO;
	      b[ 94] = ZERO;
	      b[ 95] = ZERO;

	      b[ 96] = *(a07 +  0);
	      b[ 97] = *(a07 +  1);
	      b[ 98] = *(a07 +  2);
	      b[ 99] = *(a07 +  3);
	      b[100] = *(a07 +  4);
	      b[101] = *(a07 +  5);
	      b[102] = *(a07 +  6);
	      b[103] = *(a07 +  7);
	      b[104] = *(a07 +  8);
	      b[105] = *(a07 +  9);
	      b[106] = *(a07 + 10);
	      b[107] = *(a07 + 11);
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
#endif
	      b[110] = ZERO;
	      b[111] = ZERO;

	      b[112] = *(a08 +  0);
	      b[113] = *(a08 +  1);
	      b[114] = *(a08 +  2);
	      b[115] = *(a08 +  3);
	      b[116] = *(a08 +  4);
	      b[117] = *(a08 +  5);
	      b[118] = *(a08 +  6);
	      b[119] = *(a08 +  7);
	      b[120] = *(a08 +  8);
	      b[121] = *(a08 +  9);
	      b[122] = *(a08 + 10);
	      b[123] = *(a08 + 11);
	      b[124] = *(a08 + 12);
	      b[125] = *(a08 + 13);
#ifdef UNIT
	      b[126] = ONE;
	      b[127] = ZERO;
#else
	      b[126] = *(a08 + 14);
	      b[127] = *(a08 + 15);
#endif
	      a01 += 8 * lda;
	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X < posY) {

	    /* a01 += 2 * i;
	    a02 += 2 * i;
	    a03 += 2 * i;
	    a04 += 2 * i;
	    a05 += 2 * i;
	    a06 += 2 * i;
	    a07 += 2 * i;
	    a08 += 2 * i; */
	    b += 16 * i;
	} else
	  if (X > posY) {

	    for (ii = 0; ii < i; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);

	      a01 += lda;
	      a02 += lda;
	      a03 += lda;
	      a04 += lda;
	      a05 += lda;
	      a06 += lda;
	      a07 += lda;
	      a08 += lda;
	      b += 16;
	    }
	  } else {

#ifdef UNIT
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(a01 +  0);
	    b[ 1] = *(a01 +  1);
#endif
	    b[ 2] = ZERO;
	    b[ 3] = ZERO;
	    b[ 4] = ZERO;
	    b[ 5] = ZERO;
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b[ 8] = ZERO;
	    b[ 9] = ZERO;
	    b[10] = ZERO;
	    b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[14] = ZERO;
	    b[15] = ZERO;
	    b += 16;

	    if(i >= 2) {
	      b[ 0] = *(a02 +  0);
	      b[ 1] = *(a02 +  1);
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
#endif
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 3) {
	      b[ 0] = *(a03 +  0);
	      b[ 1] = *(a03 +  1);
	      b[ 2] = *(a03 +  2);
	      b[ 3] = *(a03 +  3);
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
#endif
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 4) {
	      b[ 0] = *(a04 +  0);
	      b[ 1] = *(a04 +  1);
	      b[ 2] = *(a04 +  2);
	      b[ 3] = *(a04 +  3);
	      b[ 4] = *(a04 +  4);
	      b[ 5] = *(a04 +  5);
#ifdef UNIT
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
#endif
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 5) {
	      b[ 0] = *(a05 +  0);
	      b[ 1] = *(a05 +  1);
	      b[ 2] = *(a05 +  2);
	      b[ 3] = *(a05 +  3);
	      b[ 4] = *(a05 +  4);
	      b[ 5] = *(a05 +  5);
	      b[ 6] = *(a05 +  6);
	      b[ 7] = *(a05 +  7);
#ifdef UNIT
	      b[ 8] = ONE;
	      b[ 9] = ZERO;
#else
	      b[ 8] = *(a05 +  8);
	      b[ 9] = *(a05 +  9);
#endif
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 6) {
	      b[ 0] = *(a06 +  0);
	      b[ 1] = *(a06 +  1);
	      b[ 2] = *(a06 +  2);
	      b[ 3] = *(a06 +  3);
	      b[ 4] = *(a06 +  4);
	      b[ 5] = *(a06 +  5);
	      b[ 6] = *(a06 +  6);
	      b[ 7] = *(a06 +  7);
	      b[ 8] = *(a06 +  8);
	      b[ 9] = *(a06 +  9);
#ifdef UNIT
	      b[10] = ONE;
	      b[11] = ZERO;
#else
	      b[10] = *(a06 + 10);
	      b[11] = *(a06 + 11);
#endif
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 7) {
	      b[ 0] = *(a07 +  0);
	      b[ 1] = *(a07 +  1);
	      b[ 2] = *(a07 +  2);
	      b[ 3] = *(a07 +  3);
	      b[ 4] = *(a07 +  4);
	      b[ 5] = *(a07 +  5);
	      b[ 6] = *(a07 +  6);
	      b[ 7] = *(a07 +  7);
	      b[ 8] = *(a07 +  8);
	      b[ 9] = *(a07 +  9);
	      b[10] = *(a07 + 10);
	      b[11] = *(a07 + 11);
#ifdef UNIT
	      b[12] = ONE;
	      b[13] = ZERO;
#else
	      b[12] = *(a07 + 12);
	      b[13] = *(a07 + 13);
#endif
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }
	  }
      }

      posY += 8;
      js --;
    } while (js > 0);
  } /* End of main loop */


  if (n & 4){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
      a03 = a + posX * 2 + (posY +  2) * lda;
      a04 = a + posX * 2 + (posY +  3) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
      a03 = a + posY * 2 + (posX +  2) * lda;
      a04 = a + posY * 2 + (posX +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 8;
	  a02 += 8;
	  a03 += 8;
	  a04 += 8;
	  b += 32;
	} else
	  if (X > posY) {

	    for (ii = 0; ii < 4; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      a01 += lda;
	      b += 8;
	    }

	    a02 += 4 * lda;
	    a03 += 4 * lda;
	    a04 += 4 * lda;

	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#endif
	    b[  2] = ZERO;
	    b[  3] = ZERO;
	    b[  4] = ZERO;
	    b[  5] = ZERO;
	    b[  6] = ZERO;
	    b[  7] = ZERO;

	    b[  8] = *(a02 +  0);
	    b[  9] = *(a02 +  1);
#ifdef UNIT
	    b[ 10] = ONE;
	    b[ 11] = ZERO;
#else
	    b[ 10] = *(a02 +  2);
	    b[ 11] = *(a02 +  3);
#endif
	    b[ 12] = ZERO;
	    b[ 13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;

	    b[ 16] = *(a03 +  0);
	    b[ 17] = *(a03 +  1);
	    b[ 18] = *(a03 +  2);
	    b[ 19] = *(a03 +  3);
#ifdef UNIT
	    b[ 20] = ONE;
	    b[ 21] = ZERO;
#else
	    b[ 20] = *(a03 +  4);
	    b[ 21] = *(a03 +  5);
#endif
	    b[ 22] = ZERO;
	    b[ 23] = ZERO;

	    b[ 24] = *(a04 +  0);
	    b[ 25] = *(a04 +  1);
	    b[ 26] = *(a04 +  2);
	    b[ 27] = *(a04 +  3);
	    b[ 28] = *(a04 +  4);
	    b[ 29] = *(a04 +  5);
#ifdef UNIT
	    b[ 30] = ONE;
	    b[ 31] = ZERO;
#else
	    b[ 30] = *(a04 +  6);
	    b[ 31] = *(a04 +  7);
#endif

	    a01 += 4 * lda;
	    a02 += 4 * lda;
	    a03 += 4 * lda;
	    a04 += 4 * lda;
	    b += 32;
	  }

	X += 4;
	i --;
      } while (i > 0);
    }

    i = (m & 3);
    if (i) {

      if (X < posY) {
	/* a01 += 2 * i;
	a02 += 2 * i;
	a03 += 2 * i;
	a04 += 2 * i; */
	b += 8 * i;
      } else
	if (X > posY) {

	  for (ii = 0; ii < i; ii++){

	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a01 +  4);
	    b[  5] = *(a01 +  5);
	    b[  6] = *(a01 +  6);
	    b[  7] = *(a01 +  7);

	    a01 += lda;
	    a02 += lda;
	    a03 += lda;
	    a04 += lda;
	    b += 8;
	  }
	} else {

#ifdef UNIT
	  b[ 0] = ONE;
	  b[ 1] = ZERO;
#else
	  b[ 0] = *(a01 +  0);
	  b[ 1] = *(a01 +  1);
#endif
	  b[ 2] = ZERO;
	  b[ 3] = ZERO;
	  b[ 4] = ZERO;
	  b[ 5] = ZERO;
	  b[ 6] = ZERO;
	  b[ 7] = ZERO;
	  b += 8;

	  if(i >= 2) {
	    b[ 0] = *(a02 +  0);
	    b[ 1] = *(a02 +  1);
#ifdef UNIT
	    b[ 2] = ONE;
	    b[ 3] = ZERO;
#else
	    b[ 2] = *(a02 +  2);
	    b[ 3] = *(a02 +  3);
#endif
	    b[ 4] = ZERO;
	    b[ 5] = ZERO;
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b += 8;
	  }

	  if (i >= 3) {
	    b[ 0] = *(a03 +  0);
	    b[ 1] = *(a03 +  1);
	    b[ 2] = *(a03 +  2);
	    b[ 3] = *(a03 +  3);
#ifdef UNIT
	    b[ 4] = ONE;
	    b[ 5] = ZERO;
#else
	    b[ 4] = *(a03 +  4);
	    b[ 5] = *(a03 +  5);
#endif
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b += 8;
	  }
	}
    }

    posY += 4;
  }


  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X < posY) {
	    a01 += 4;
	    a02 += 4;
	    b += 8;
	} else
	  if (X > posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a02 +  0);
	    b[  5] = *(a02 +  1);
	    b[  6] = *(a02 +  2);
	    b[  7] = *(a02 +  3);

	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 8;
	  } else {

#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#endif
	    b[  2] = ZERO;
	    b[  3] = ZERO;

	    b[  4] = *(a02 +  0);
	    b[  5] = *(a02 +  1);
#ifdef UNIT
	    b[  6] = ONE;
	    b[  7] = ZERO;
#else
	    b[  6] = *(a02 +  2);
	    b[  7] = *(a02 +  3);
#endif

	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 8;
	  }

	X += 2;
	i --;
      } while (i > 0);
    }

    i = (m & 1);
    if (i) {

      if (X < posY) {
	b += 4;
      } else
	if (X > posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b += 4;
	  }
#if 1
	} 
#else
	} else {	
#ifdef UNIT
	  b[ 0] = ONE;
	  b[ 1] = ZERO;
#else
	  b[ 0] = *(a01 +  0);
	  b[ 1] = *(a01 +  1);
#endif
	  b[ 2] = *(a02 +  0);
	  b[ 3] = *(a02 +  1);
	  b += 4;
	}
#endif	
    posY += 2;
  }

  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
    }

    i = m;
    if (m > 0) {
      do {
	if (X < posY) {
	  a01 += 2;
	} else {
#ifdef UNIT
	  if (X > posY) {
#endif
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#ifdef UNIT
	  } else {
	    b[  0] = ONE;
	    b[  1] = ZERO;
	  }
#endif
	  a01 += lda;
	}
	b += 2;
	X ++;
	i --;
      } while (i > 0);
    }
  }

  return 0;
}
