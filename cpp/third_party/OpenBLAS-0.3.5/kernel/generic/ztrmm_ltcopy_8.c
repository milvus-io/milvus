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
	a01 = a + posY * 2 + (posX +  0) * lda;
	a02 = a + posY * 2 + (posX +  1) * lda;
	a03 = a + posY * 2 + (posX +  2) * lda;
	a04 = a + posY * 2 + (posX +  3) * lda;
	a05 = a + posY * 2 + (posX +  4) * lda;
	a06 = a + posY * 2 + (posX +  5) * lda;
	a07 = a + posY * 2 + (posX +  6) * lda;
	a08 = a + posY * 2 + (posX +  7) * lda;
      } else {
	a01 = a + posX * 2 + (posY +  0) * lda;
	a02 = a + posX * 2 + (posY +  1) * lda;
	a03 = a + posX * 2 + (posY +  2) * lda;
	a04 = a + posX * 2 + (posY +  3) * lda;
	a05 = a + posX * 2 + (posY +  4) * lda;
	a06 = a + posX * 2 + (posY +  5) * lda;
	a07 = a + posX * 2 + (posY +  6) * lda;
	a08 = a + posX * 2 + (posY +  7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X > posY) {
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
	    if (X < posY) {

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

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
#endif
	      b[ 20] = *(a02 +  4);
	      b[ 21] = *(a02 +  5);
	      b[ 22] = *(a02 +  6);
	      b[ 23] = *(a02 +  7);
	      b[ 24] = *(a02 +  8);
	      b[ 25] = *(a02 +  9);
	      b[ 26] = *(a02 + 10);
	      b[ 27] = *(a02 + 11);
	      b[ 28] = *(a02 + 12);
	      b[ 29] = *(a02 + 13);
	      b[ 30] = *(a02 + 14);
	      b[ 31] = *(a02 + 15);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
	      b[ 34] = ZERO;
	      b[ 35] = ZERO;
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(a03 +  4);
	      b[ 37] = *(a03 +  5);
#endif
	      b[ 38] = *(a03 +  6);
	      b[ 39] = *(a03 +  7);
	      b[ 40] = *(a03 +  8);
	      b[ 41] = *(a03 +  9);
	      b[ 42] = *(a03 + 10);
	      b[ 43] = *(a03 + 11);
	      b[ 44] = *(a03 + 12);
	      b[ 45] = *(a03 + 13);
	      b[ 46] = *(a03 + 14);
	      b[ 47] = *(a03 + 15);

	      b[ 48] = ZERO;
	      b[ 49] = ZERO;
	      b[ 50] = ZERO;
	      b[ 51] = ZERO;
	      b[ 52] = ZERO;
	      b[ 53] = ZERO;
#ifdef UNIT
	      b[ 54] = ONE;
	      b[ 55] = ZERO;
#else
	      b[ 54] = *(a04 +  6);
	      b[ 55] = *(a04 +  7);
#endif
	      b[ 56] = *(a04 +  8);
	      b[ 57] = *(a04 +  9);
	      b[ 58] = *(a04 + 10);
	      b[ 59] = *(a04 + 11);
	      b[ 60] = *(a04 + 12);
	      b[ 61] = *(a04 + 13);
	      b[ 62] = *(a04 + 14);
	      b[ 63] = *(a04 + 15);

	      b[ 64] = ZERO;
	      b[ 65] = ZERO;
	      b[ 66] = ZERO;
	      b[ 67] = ZERO;
	      b[ 68] = ZERO;
	      b[ 69] = ZERO;
	      b[ 70] = ZERO;
	      b[ 71] = ZERO;
#ifdef UNIT
	      b[ 72] = ONE;
	      b[ 73] = ZERO;
#else
	      b[ 72] = *(a05 +  8);
	      b[ 73] = *(a05 +  9);
#endif
	      b[ 74] = *(a05 + 10);
	      b[ 75] = *(a05 + 11);
	      b[ 76] = *(a05 + 12);
	      b[ 77] = *(a05 + 13);
	      b[ 78] = *(a05 + 14);
	      b[ 79] = *(a05 + 15);

	      b[ 80] = ZERO;
	      b[ 81] = ZERO;
	      b[ 82] = ZERO;
	      b[ 83] = ZERO;
	      b[ 84] = ZERO;
	      b[ 85] = ZERO;
	      b[ 86] = ZERO;
	      b[ 87] = ZERO;
	      b[ 88] = ZERO;
	      b[ 89] = ZERO;
#ifdef UNIT
	      b[ 90] = ONE;
	      b[ 91] = ZERO;
#else
	      b[ 90] = *(a06 + 10);
	      b[ 91] = *(a06 + 11);
#endif
	      b[ 92] = *(a06 + 12);
	      b[ 93] = *(a06 + 13);
	      b[ 94] = *(a06 + 14);
	      b[ 95] = *(a06 + 15);

	      b[ 96] = ZERO;
	      b[ 97] = ZERO;
	      b[ 98] = ZERO;
	      b[ 99] = ZERO;
	      b[100] = ZERO;
	      b[101] = ZERO;
	      b[102] = ZERO;
	      b[103] = ZERO;
	      b[104] = ZERO;
	      b[105] = ZERO;
	      b[106] = ZERO;
	      b[107] = ZERO;
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
#endif
	      b[110] = *(a07 + 14);
	      b[111] = *(a07 + 15);

	      b[112] = ZERO;
	      b[113] = ZERO;
	      b[114] = ZERO;
	      b[115] = ZERO;
	      b[116] = ZERO;
	      b[117] = ZERO;
	      b[118] = ZERO;
	      b[119] = ZERO;
	      b[120] = ZERO;
	      b[121] = ZERO;
	      b[122] = ZERO;
	      b[123] = ZERO;
	      b[124] = ZERO;
	      b[125] = ZERO;
#ifdef UNIT
	      b[126] = ONE;
	      b[127] = ZERO;
#else
	      b[126] = *(a08 + 14);
	      b[127] = *(a08 + 15);
#endif

	      a01 += 16;
	      a02 += 16;
	      a03 += 16;
	      a04 += 16;
	      a05 += 16;
	      a06 += 16;
	      a07 += 16;
	      a08 += 16;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i > 0) {
	if (X > posY) {
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
	  if (X < posY) {
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
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#endif
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
	    b += 16;

	    if (i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
#endif
	      b[ 4] = *(a02 +  4);
	      b[ 5] = *(a02 +  5);
	      b[ 6] = *(a02 +  6);
	      b[ 7] = *(a02 +  7);

	      b[ 8] = *(a02 +  8);
	      b[ 9] = *(a02 +  9);
	      b[10] = *(a02 + 10);
	      b[11] = *(a02 + 11);
	      b[12] = *(a02 + 12);
	      b[13] = *(a02 + 13);
	      b[14] = *(a02 + 14);
	      b[15] = *(a02 + 15);
	      b += 16;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
#endif
	      b[ 6] = *(a03 +  6);
	      b[ 7] = *(a03 +  7);

	      b[ 8] = *(a03 +  8);
	      b[ 9] = *(a03 +  9);
	      b[10] = *(a03 + 10);
	      b[11] = *(a03 + 11);
	      b[12] = *(a03 + 12);
	      b[13] = *(a03 + 13);
	      b[14] = *(a03 + 14);
	      b[15] = *(a03 + 15);
	      b += 16;
	    }

	    if (i >= 4) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
#ifdef UNIT
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
#endif

	      b[ 8] = *(a04 +  8);
	      b[ 9] = *(a04 +  9);
	      b[10] = *(a04 + 10);
	      b[11] = *(a04 + 11);
	      b[12] = *(a04 + 12);
	      b[13] = *(a04 + 13);
	      b[14] = *(a04 + 14);
	      b[15] = *(a04 + 15);
	      b += 16;
	    }

	    if (i >= 5) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;

#ifdef UNIT
	      b[ 8] = ONE;
	      b[ 9] = ZERO;
#else
	      b[ 8] = *(a05 +  8);
	      b[ 9] = *(a05 +  9);
#endif
	      b[10] = *(a05 + 10);
	      b[11] = *(a05 + 11);
	      b[12] = *(a05 + 12);
	      b[13] = *(a05 + 13);
	      b[14] = *(a05 + 14);
	      b[15] = *(a05 + 15);
	      b += 16;
	    }

	    if (i >= 6) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;

	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
#ifdef UNIT
	      b[10] = ONE;
	      b[11] = ZERO;
#else
	      b[10] = *(a06 + 10);
	      b[11] = *(a06 + 11);
#endif
	      b[12] = *(a06 + 12);
	      b[13] = *(a06 + 13);
	      b[14] = *(a06 + 14);
	      b[15] = *(a06 + 15);
	      b += 16;
	    }

	    if (i >= 7) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
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
#ifdef UNIT
	      b[12] = ONE;
	      b[13] = ZERO;
#else
	      b[12] = *(a07 + 12);
	      b[13] = *(a07 + 13);
#endif
	      b[14] = *(a07 + 14);
	      b[15] = *(a07 + 15);
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
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
      a03 = a + posY * 2 + (posX +  2) * lda;
      a04 = a + posY * 2 + (posX +  3) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
      a03 = a + posX * 2 + (posY +  2) * lda;
      a04 = a + posX * 2 + (posY +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 8;
	  a02 += 8;
	  a03 += 8;
	  a04 += 8;
	  b += 32;
	} else
	  if (X < posY) {
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
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      b[  8] = ZERO;
	      b[  9] = ZERO;
#ifdef UNIT
	      b[ 10] = ONE;
	      b[ 11] = ZERO;
#else
	      b[ 10] = *(a02 +  2);
	      b[ 11] = *(a02 +  3);
#endif
	      b[ 12] = *(a02 +  4);
	      b[ 13] = *(a02 +  5);
	      b[ 14] = *(a02 +  6);
	      b[ 15] = *(a02 +  7);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
	      b[ 18] = ZERO;
	      b[ 19] = ZERO;
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(a03 +  4);
	      b[ 21] = *(a03 +  5);
#endif
	      b[ 22] = *(a03 +  6);
	      b[ 23] = *(a03 +  7);

	      b[ 24] = ZERO;
	      b[ 25] = ZERO;
	      b[ 26] = ZERO;
	      b[ 27] = ZERO;
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
#ifdef UNIT
	      b[ 30] = ONE;
	      b[ 31] = ZERO;
#else
	      b[ 30] = *(a04 +  6);
	      b[ 31] = *(a04 +  7);
#endif

	      a01 += 8;
	      a02 += 8;
	      a03 += 8;
	      a04 += 8;
	      b += 32;
	    }

	X += 4;
	i --;
      } while (i > 0);
    }

    i = (m & 3);
    if (i > 0) {
      if (X > posY) {
	/* a01 += 2 * i;
	a02 += 2 * i;
	a03 += 2 * i;
	a04 += 2 * i; */
	b += 8 * i;
      } else
	if (X < posY) {
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
	  b[  0] = ONE;
	  b[  1] = ZERO;
#else
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
#endif
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b[  4] = *(a01 +  4);
	  b[  5] = *(a01 +  5);
	  b[  6] = *(a01 +  6);
	  b[  7] = *(a01 +  7);
	  b += 8;

	  if (i >= 2) {
	    b[ 0] = ZERO;
	    b[ 1] = ZERO;
#ifdef UNIT
	    b[ 2] = ONE;
	    b[ 3] = ZERO;
#else
	    b[ 2] = *(a02 +  2);
	    b[ 3] = *(a02 +  3);
#endif
	    b[ 4] = *(a02 +  4);
	    b[ 5] = *(a02 +  5);
	    b[ 6] = *(a02 +  6);
	    b[ 7] = *(a02 +  7);
	    b += 8;
	  }

	  if (i >= 3) {
	    b[ 0] = ZERO;
	    b[ 1] = ZERO;
	    b[ 2] = ZERO;
	    b[ 3] = ZERO;
#ifdef UNIT
	    b[ 4] = ONE;
	    b[ 5] = ZERO;
#else
	    b[ 4] = *(a03 +  4);
	    b[ 5] = *(a03 +  5);
#endif
	    b[ 6] = *(a03 +  6);
	    b[ 7] = *(a03 +  7);
	    b += 8;
	  }
	}
    }
    posY += 4;
  }

  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 4;
	  a02 += 4;
	  b += 8;
	} else
	  if (X < posY) {
	    b[0] = *(a01 +  0);
	    b[1] = *(a01 +  1);
	    b[2] = *(a01 +  2);
	    b[3] = *(a01 +  3);
	    b[4] = *(a02 +  0);
	    b[5] = *(a02 +  1);
	    b[6] = *(a02 +  2);
	    b[7] = *(a02 +  3);
	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 8;
	  } else {
#ifdef UNIT
	    b[0] = ONE;
	    b[1] = ZERO;
#else
	    b[0] = *(a01 +  0);
	    b[1] = *(a01 +  1);
#endif
	    b[2] = *(a01 +  2);
	    b[3] = *(a01 +  3);

	    b[4] = ZERO;
	    b[5] = ZERO;
#ifdef UNIT
	    b[6] = ONE;
	    b[7] = ZERO;
#else
	    b[6] = *(a02 +  2);
	    b[7] = *(a02 +  3);
#endif
	    a01 += 4;
	    a02 += 4;
	    b += 8;
	  }

	X += 2;
	i --;
      } while (i > 0);
    }

    i = (m & 1);
    if (i > 0) {
      if (X > posY) {
	/* a01 += 2;
	a02 += 2; */
	b += 4;
      } else
	if (X < posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);

	  /* a01 += lda;
	  a02 += lda; */
	  b += 4;
	} else {
#ifdef UNIT
	  b[  0] = ONE;
	  b[  1] = ZERO;
#else
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
#endif
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b += 4;
	}
    }
    posY += 2;
  }

  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY * 2 + (posX +  0) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
    }

    i = m;
    if (i > 0) {
      do {

	if (X > posY) {
	  a01 += 2;
	  b += 2;
	} else
	  if (X < posY) {
	    b[0] = *(a01 + 0);
	    b[1] = *(a01 + 1);
	    a01 += lda;
	    b += 2;
	  } else {
#ifdef UNIT
	    b[0] = ONE;
	    b[1] = ZERO;
#else
	    b[0] = *(a01 + 0);
	    b[1] = *(a01 + 1);
#endif
	    a01 += 2;
	    b += 2;
	  }

	X += 1;
	i --;
      } while (i > 0);
    }
    // posY += 1;
  }

  return 0;
}
