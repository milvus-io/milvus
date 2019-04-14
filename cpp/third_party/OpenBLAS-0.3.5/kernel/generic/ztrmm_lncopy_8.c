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

  BLASLONG i, js;
  BLASLONG X, ii;

  FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6, *ao7, *ao8;

  lda += lda;

  js = (n >> 3);

  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
	ao3 = a + posY * 2 + (posX + 2) * lda;
	ao4 = a + posY * 2 + (posX + 3) * lda;
	ao5 = a + posY * 2 + (posX + 4) * lda;
	ao6 = a + posY * 2 + (posX + 5) * lda;
	ao7 = a + posY * 2 + (posX + 6) * lda;
	ao8 = a + posY * 2 + (posX + 7) * lda;
      } else {
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
	ao3 = a + posX * 2 + (posY + 2) * lda;
	ao4 = a + posX * 2 + (posY + 3) * lda;
	ao5 = a + posX * 2 + (posY + 4) * lda;
	ao6 = a + posX * 2 + (posY + 5) * lda;
	ao7 = a + posX * 2 + (posY + 6) * lda;
	ao8 = a + posX * 2 + (posY + 7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X > posY) {
	    for (ii = 0; ii < 8; ii++){

	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
	      b[  2] = *(ao2 +  0);
	      b[  3] = *(ao2 +  1);
	      b[  4] = *(ao3 +  0);
	      b[  5] = *(ao3 +  1);
	      b[  6] = *(ao4 +  0);
	      b[  7] = *(ao4 +  1);

	      b[  8] = *(ao5 +  0);
	      b[  9] = *(ao5 +  1);
	      b[ 10] = *(ao6 +  0);
	      b[ 11] = *(ao6 +  1);
	      b[ 12] = *(ao7 +  0);
	      b[ 13] = *(ao7 +  1);
	      b[ 14] = *(ao8 +  0);
	      b[ 15] = *(ao8 +  1);

	      ao1 += 2;
	      ao2 += 2;
	      ao3 += 2;
	      ao4 += 2;
	      ao5 += 2;
	      ao6 += 2;
	      ao7 += 2;
	      ao8 += 2;
	      b += 16;
	    }
	  } else
	    if (X < posY) {
	      ao1 += 8 * lda;
	      ao2 += 8 * lda;
	      ao3 += 8 * lda;
	      ao4 += 8 * lda;
	      ao5 += 8 * lda;
	      ao6 += 8 * lda;
	      ao7 += 8 * lda;
	      ao8 += 8 * lda;

	      b += 128;

	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
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

	      b[ 16] = *(ao1 +  2);
	      b[ 17] = *(ao1 +  3);
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(ao2 +  2);
	      b[ 19] = *(ao2 +  3);
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

	      b[ 32] = *(ao1 +  4);
	      b[ 33] = *(ao1 +  5);
	      b[ 34] = *(ao2 +  4);
	      b[ 35] = *(ao2 +  5);
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(ao3 +  4);
	      b[ 37] = *(ao3 +  5);
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

	      b[ 48] = *(ao1 +  6);
	      b[ 49] = *(ao1 +  7);
	      b[ 50] = *(ao2 +  6);
	      b[ 51] = *(ao2 +  7);
	      b[ 52] = *(ao3 +  6);
	      b[ 53] = *(ao3 +  7);
#ifdef UNIT
	      b[ 54] = ONE;
	      b[ 55] = ZERO;
#else
	      b[ 54] = *(ao4 +  6);
	      b[ 55] = *(ao4 +  7);
#endif
	      b[ 56] = ZERO;
	      b[ 57] = ZERO;
	      b[ 58] = ZERO;
	      b[ 59] = ZERO;
	      b[ 60] = ZERO;
	      b[ 61] = ZERO;
	      b[ 62] = ZERO;
	      b[ 63] = ZERO;

	      b[ 64] = *(ao1 +  8);
	      b[ 65] = *(ao1 +  9);
	      b[ 66] = *(ao2 +  8);
	      b[ 67] = *(ao2 +  9);
	      b[ 68] = *(ao3 +  8);
	      b[ 69] = *(ao3 +  9);
	      b[ 70] = *(ao4 +  8);
	      b[ 71] = *(ao4 +  9);
#ifdef UNIT
	      b[ 72] = ONE;
	      b[ 73] = ZERO;
#else
	      b[ 72] = *(ao5 +  8);
	      b[ 73] = *(ao5 +  9);
#endif
	      b[ 74] = ZERO;
	      b[ 75] = ZERO;
	      b[ 76] = ZERO;
	      b[ 77] = ZERO;
	      b[ 78] = ZERO;
	      b[ 79] = ZERO;

	      b[ 80] = *(ao1 + 10);
	      b[ 81] = *(ao1 + 11);
	      b[ 82] = *(ao2 + 10);
	      b[ 83] = *(ao2 + 11);
	      b[ 84] = *(ao3 + 10);
	      b[ 85] = *(ao3 + 11);
	      b[ 86] = *(ao4 + 10);
	      b[ 87] = *(ao4 + 11);
	      b[ 88] = *(ao5 + 10);
	      b[ 89] = *(ao5 + 11);
#ifdef UNIT
	      b[ 90] = ONE;
	      b[ 91] = ZERO;
#else
	      b[ 90] = *(ao6 + 10);
	      b[ 91] = *(ao6 + 11);
#endif
	      b[ 92] = ZERO;
	      b[ 93] = ZERO;
	      b[ 94] = ZERO;
	      b[ 95] = ZERO;

	      b[ 96] = *(ao1 + 12);
	      b[ 97] = *(ao1 + 13);
	      b[ 98] = *(ao2 + 12);
	      b[ 99] = *(ao2 + 13);
	      b[100] = *(ao3 + 12);
	      b[101] = *(ao3 + 13);
	      b[102] = *(ao4 + 12);
	      b[103] = *(ao4 + 13);
	      b[104] = *(ao5 + 12);
	      b[105] = *(ao5 + 13);
	      b[106] = *(ao6 + 12);
	      b[107] = *(ao6 + 13);
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(ao7 + 12);
	      b[109] = *(ao7 + 13);
#endif
	      b[110] = ZERO;
	      b[111] = ZERO;

	      b[112] = *(ao1 + 14);
	      b[113] = *(ao1 + 15);
	      b[114] = *(ao2 + 14);
	      b[115] = *(ao2 + 15);
	      b[116] = *(ao3 + 14);
	      b[117] = *(ao3 + 15);
	      b[118] = *(ao4 + 14);
	      b[119] = *(ao4 + 15);
	      b[120] = *(ao5 + 14);
	      b[121] = *(ao5 + 15);
	      b[122] = *(ao6 + 14);
	      b[123] = *(ao6 + 15);
	      b[124] = *(ao7 + 14);
	      b[125] = *(ao7 + 15);
#ifdef UNIT
	      b[126] = ONE;
	      b[127] = ZERO;
#else
	      b[126] = *(ao8 + 14);
	      b[127] = *(ao8 + 15);
#endif

	      ao1 += 16;
	      ao2 += 16;
	      ao3 += 16;
	      ao4 += 16;
	      ao5 += 16;
	      ao6 += 16;
	      ao7 += 16;
	      ao8 += 16;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X > posY) {

	  for (ii = 0; ii < i; ii++){
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    b[  2] = *(ao2 +  0);
	    b[  3] = *(ao2 +  1);
	    b[  4] = *(ao3 +  0);
	    b[  5] = *(ao3 +  1);
	    b[  6] = *(ao4 +  0);
	    b[  7] = *(ao4 +  1);

	    b[  8] = *(ao5 +  0);
	    b[  9] = *(ao5 +  1);
	    b[ 10] = *(ao6 +  0);
	    b[ 11] = *(ao6 +  1);
	    b[ 12] = *(ao7 +  0);
	    b[ 13] = *(ao7 +  1);
	    b[ 14] = *(ao8 +  0);
	    b[ 15] = *(ao8 +  1);

	    ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2;
	    ao5 += 2;
	    ao6 += 2;
	    ao7 += 2;
	    ao8 += 2;
	    b += 16;
	  }
	} else
	  if (X < posY) {
	    /* ao1 += i * lda;
	    ao2 += i * lda;
	    ao3 += i * lda;
	    ao4 += i * lda;
	    ao5 += i * lda;
	    ao6 += i * lda;
	    ao7 += i * lda;
	    ao8 += i * lda; */
	    b += 16 * i;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
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
	      b += 16;

	      if (i >= 2) {
		b[ 0] = *(ao1 +  2);
		b[ 1] = *(ao1 +  3);
#ifdef UNIT
		b[ 2] = ONE;
		b[ 3] = ZERO;
#else
		b[ 2] = *(ao2 +  2);
		b[ 3] = *(ao2 +  3);
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
		b[ 0] = *(ao1 +  4);
		b[ 1] = *(ao1 +  5);
		b[ 2] = *(ao2 +  4);
		b[ 3] = *(ao2 +  5);
#ifdef UNIT
		b[ 4] = ONE;
		b[ 5] = ZERO;
#else
		b[ 4] = *(ao3 +  4);
		b[ 5] = *(ao3 +  5);
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
		b[ 0] = *(ao1 +  6);
		b[ 1] = *(ao1 +  7);
		b[ 2] = *(ao2 +  6);
		b[ 3] = *(ao2 +  7);
		b[ 4] = *(ao3 +  6);
		b[ 5] = *(ao3 +  7);
#ifdef UNIT
		b[ 6] = ONE;
		b[ 7] = ZERO;
#else
		b[ 6] = *(ao4 +  6);
		b[ 7] = *(ao4 +  7);
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
		b[ 0] = *(ao1 +  8);
		b[ 1] = *(ao1 +  9);
		b[ 2] = *(ao2 +  8);
		b[ 3] = *(ao2 +  9);
		b[ 4] = *(ao3 +  8);
		b[ 5] = *(ao3 +  9);
		b[ 6] = *(ao4 +  8);
		b[ 7] = *(ao4 +  9);
#ifdef UNIT
		b[ 8] = ONE;
		b[ 9] = ZERO;
#else
		b[ 8] = *(ao5 +  8);
		b[ 9] = *(ao5 +  9);
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
		b[ 0] = *(ao1 + 10);
		b[ 1] = *(ao1 + 11);
		b[ 2] = *(ao2 + 10);
		b[ 3] = *(ao2 + 11);
		b[ 4] = *(ao3 + 10);
		b[ 5] = *(ao3 + 11);
		b[ 6] = *(ao4 + 10);
		b[ 7] = *(ao4 + 11);
		b[ 8] = *(ao5 + 10);
		b[ 9] = *(ao5 + 11);
#ifdef UNIT
		b[10] = ONE;
		b[11] = ZERO;
#else
		b[10] = *(ao6 + 10);
		b[11] = *(ao6 + 11);
#endif
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 7) {
		b[ 0] = *(ao1 + 12);
		b[ 1] = *(ao1 + 13);
		b[ 2] = *(ao2 + 12);
		b[ 3] = *(ao2 + 13);
		b[ 4] = *(ao3 + 12);
		b[ 5] = *(ao3 + 13);
		b[ 6] = *(ao4 + 12);
		b[ 7] = *(ao4 + 13);
		b[ 8] = *(ao5 + 12);
		b[ 9] = *(ao5 + 13);
		b[10] = *(ao6 + 12);
		b[11] = *(ao6 + 13);
#ifdef UNIT
		b[12] = ONE;
		b[13] = ZERO;
#else
		b[12] = *(ao7 + 12);
		b[13] = *(ao7 + 13);
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
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
	ao3 = a + posY * 2 + (posX + 2) * lda;
	ao4 = a + posY * 2 + (posX + 3) * lda;
      } else {
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
	ao3 = a + posX * 2 + (posY + 2) * lda;
	ao4 = a + posX * 2 + (posY + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X > posY) {
	    for (ii = 0; ii < 4; ii++){
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
	      b[  2] = *(ao2 +  0);
	      b[  3] = *(ao2 +  1);
	      b[  4] = *(ao3 +  0);
	      b[  5] = *(ao3 +  1);
	      b[  6] = *(ao4 +  0);
	      b[  7] = *(ao4 +  1);

	      ao1 += 2;
	      ao2 += 2;
	      ao3 += 2;
	      ao4 += 2;
	      b += 8;
	    }
	  } else
	    if (X < posY) {
	      ao1 += 4 * lda;
	      ao2 += 4 * lda;
	      ao3 += 4 * lda;
	      ao4 += 4 * lda;
	      b += 32;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
#endif
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;

	      b[  8] = *(ao1 +  2);
	      b[  9] = *(ao1 +  3);
#ifdef UNIT
	      b[ 10] = ONE;
	      b[ 11] = ZERO;
#else
	      b[ 10] = *(ao2 +  2);
	      b[ 11] = *(ao2 +  3);
#endif
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
	      b[ 14] = ZERO;
	      b[ 15] = ZERO;

	      b[ 16] = *(ao1 +  4);
	      b[ 17] = *(ao1 +  5);
	      b[ 18] = *(ao2 +  4);
	      b[ 19] = *(ao2 +  5);
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(ao3 +  4);
	      b[ 21] = *(ao3 +  5);
#endif
	      b[ 22] = ZERO;
	      b[ 23] = ZERO;

	      b[ 24] = *(ao1 +  6);
	      b[ 25] = *(ao1 +  7);
	      b[ 26] = *(ao2 +  6);
	      b[ 27] = *(ao2 +  7);
	      b[ 28] = *(ao3 +  6);
	      b[ 29] = *(ao3 +  7);
#ifdef UNIT
	      b[ 30] = ONE;
	      b[ 31] = ZERO;
#else
	      b[ 30] = *(ao4 +  6);
	      b[ 31] = *(ao4 +  7);
#endif

	      ao1 += 8;
	      ao2 += 8;
	      ao3 += 8;
	      ao4 += 8;
	      b += 32;
	    }

	  X += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);
      if (i) {

	if (X > posY) {

	  for (ii = 0; ii < i; ii++){
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    b[  2] = *(ao2 +  0);
	    b[  3] = *(ao2 +  1);
	    b[  4] = *(ao3 +  0);
	    b[  5] = *(ao3 +  1);
	    b[  6] = *(ao4 +  0);
	    b[  7] = *(ao4 +  1);

	    ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2;
	    b += 8;
	  }
	} else
	  if (X < posY) {
	    /* ao1 += i * lda;
	    ao2 += i * lda;
	    ao3 += i * lda;
	    ao4 += i * lda; */
	    b += 8 * i;
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
#endif
	    b[  2] = ZERO;
	    b[  3] = ZERO;
	    b[  4] = ZERO;
	    b[  5] = ZERO;
	    b[  6] = ZERO;
	    b[  7] = ZERO;
	    b += 8;

	    if (i >= 2) {
	      b[ 0] = *(ao1 +  2);
	      b[ 1] = *(ao1 +  3);
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(ao2 +  2);
	      b[ 3] = *(ao2 +  3);
#endif
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 3) {
	      b[ 0] = *(ao1 +  4);
	      b[ 1] = *(ao1 +  5);
	      b[ 2] = *(ao2 +  4);
	      b[ 3] = *(ao2 +  5);
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(ao3 +  4);
	      b[ 5] = *(ao3 +  5);
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
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
      } else {
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X > posY) {
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    b[  2] = *(ao2 +  0);
	    b[  3] = *(ao2 +  1);
	    b[  4] = *(ao1 +  2);
	    b[  5] = *(ao1 +  3);
	    b[  6] = *(ao2 +  2);
	    b[  7] = *(ao2 +  3);

	    ao1 += 4;
	    ao2 += 4;
	    b += 8;
	  } else
	    if (X < posY) {
	      ao1 += 2 * lda;
	      ao2 += 2 * lda;
	      b += 8;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
#endif
	      b[  2] = ZERO;
	      b[  3] = ZERO;

	      b[  4] = *(ao1 +  2);
	      b[  5] = *(ao1 +  3);
#ifdef UNIT
	      b[  6] = ONE;
	      b[  7] = ZERO;
#else
	      b[  6] = *(ao2 +  2);
	      b[  7] = *(ao2 +  3);
#endif
	      ao1 += 4;
	      ao2 += 4;
	      b += 8;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      if (m & 1) {

	if (X > posY) {
	  b[  0] = *(ao1 +  0);
	  b[  1] = *(ao1 +  1);
	  b[  2] = *(ao2 +  0);
	  b[  3] = *(ao2 +  1);
	  /* ao1 += 2;
	  ao2 += 2; */
	  b += 4;
	} else
	  if (X < posY) {
	    /* ao1 += 2 * lda;
	    ao2 += 2 * lda; */
	    b += 4;
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
#endif
	    b[  2] = ZERO;
	    b[  3] = ZERO;
	    b += 4;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	ao1 = a + posY * 2 + (posX + 0) * lda;
      } else {
	ao1 = a + posX * 2 + (posY + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X > posY) {
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    ao1 += 2;
	    b += 2;
	  } else
	    if (X < posY) {
	      ao1 += lda;
	      b += 2;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
#endif
	      ao1 += 2;
	      b += 2;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
