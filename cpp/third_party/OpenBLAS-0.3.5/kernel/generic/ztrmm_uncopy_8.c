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

  FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6, *ao7, *ao8;

  lda += lda;

  js = (n >> 3);
  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
	ao3 = a + posX * 2 + (posY + 2) * lda;
	ao4 = a + posX * 2 + (posY + 3) * lda;
	ao5 = a + posX * 2 + (posY + 4) * lda;
	ao6 = a + posX * 2 + (posY + 5) * lda;
	ao7 = a + posX * 2 + (posY + 6) * lda;
	ao8 = a + posX * 2 + (posY + 7) * lda;
      } else {
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
	ao3 = a + posY * 2 + (posX + 2) * lda;
	ao4 = a + posY * 2 + (posX + 3) * lda;
	ao5 = a + posY * 2 + (posX + 4) * lda;
	ao6 = a + posY * 2 + (posX + 5) * lda;
	ao7 = a + posY * 2 + (posX + 6) * lda;
	ao8 = a + posY * 2 + (posX + 7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X < posY) {

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
	    if (X > posY) {
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

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(ao2 +  2);
	      b[ 19] = *(ao2 +  3);
#endif
	      b[ 20] = *(ao3 +  2);
	      b[ 21] = *(ao3 +  3);
	      b[ 22] = *(ao4 +  2);
	      b[ 23] = *(ao4 +  3);
	      b[ 24] = *(ao5 +  2);
	      b[ 25] = *(ao5 +  3);
	      b[ 26] = *(ao6 +  2);
	      b[ 27] = *(ao6 +  3);
	      b[ 28] = *(ao7 +  2);
	      b[ 29] = *(ao7 +  3);
	      b[ 30] = *(ao8 +  2);
	      b[ 31] = *(ao8 +  3);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
	      b[ 34] = ZERO;
	      b[ 35] = ZERO;
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(ao3 +  4);
	      b[ 37] = *(ao3 +  5);
#endif
	      b[ 38] = *(ao4 +  4);
	      b[ 39] = *(ao4 +  5);
	      b[ 40] = *(ao5 +  4);
	      b[ 41] = *(ao5 +  5);
	      b[ 42] = *(ao6 +  4);
	      b[ 43] = *(ao6 +  5);
	      b[ 44] = *(ao7 +  4);
	      b[ 45] = *(ao7 +  5);
	      b[ 46] = *(ao8 +  4);
	      b[ 47] = *(ao8 +  5);

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
	      b[ 54] = *(ao4 +  6);
	      b[ 55] = *(ao4 +  7);
#endif
	      b[ 56] = *(ao5 +  6);
	      b[ 57] = *(ao5 +  7);
	      b[ 58] = *(ao6 +  6);
	      b[ 59] = *(ao6 +  7);
	      b[ 60] = *(ao7 +  6);
	      b[ 61] = *(ao7 +  7);
	      b[ 62] = *(ao8 +  6);
	      b[ 63] = *(ao8 +  7);

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
	      b[ 72] = *(ao5 +  8);
	      b[ 73] = *(ao5 +  9);
#endif
	      b[ 74] = *(ao6 +  8);
	      b[ 75] = *(ao6 +  9);
	      b[ 76] = *(ao7 +  8);
	      b[ 77] = *(ao7 +  9);
	      b[ 78] = *(ao8 +  8);
	      b[ 79] = *(ao8 +  9);

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
	      b[ 90] = *(ao6 + 10);
	      b[ 91] = *(ao6 + 11);
#endif
	      b[ 92] = *(ao7 + 10);
	      b[ 93] = *(ao7 + 11);
	      b[ 94] = *(ao8 + 10);
	      b[ 95] = *(ao8 + 11);

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
	      b[108] = *(ao7 + 12);
	      b[109] = *(ao7 + 13);
#endif
	      b[110] = *(ao8 + 12);
	      b[111] = *(ao8 + 13);

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
	      b[126] = *(ao8 + 14);
	      b[127] = *(ao8 + 15);
#endif

	      ao1 += 8 * lda;
	      ao2 += 8 * lda;
	      ao3 += 8 * lda;
	      ao4 += 8 * lda;
	      ao5 += 8 * lda;
	      ao6 += 8 * lda;
	      ao7 += 8 * lda;
	      ao8 += 8 * lda;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X < posY) {
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
	  if (X > posY) {
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
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(ao1 +  0);
	    b[ 1] = *(ao1 +  1);
#endif
	    b[ 2] = *(ao2 +  0);
	    b[ 3] = *(ao2 +  1);
	    b[ 4] = *(ao3 +  0);
	    b[ 5] = *(ao3 +  1);
	    b[ 6] = *(ao4 +  0);
	    b[ 7] = *(ao4 +  1);
	    b[ 8] = *(ao5 +  0);
	    b[ 9] = *(ao5 +  1);
	    b[10] = *(ao6 +  0);
	    b[11] = *(ao6 +  1);
	    b[12] = *(ao7 +  0);
	    b[13] = *(ao7 +  1);
	    b[14] = *(ao8 +  0);
	    b[15] = *(ao8 +  1);
	    b += 16;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(ao2 +  2);
	      b[ 3] = *(ao2 +  3);
#endif
	      b[ 4] = *(ao3 +  2);
	      b[ 5] = *(ao3 +  3);
	      b[ 6] = *(ao4 +  2);
	      b[ 7] = *(ao4 +  3);
	      b[ 8] = *(ao5 +  2);
	      b[ 9] = *(ao5 +  3);
	      b[10] = *(ao6 +  2);
	      b[11] = *(ao6 +  3);
	      b[12] = *(ao7 +  2);
	      b[13] = *(ao7 +  3);
	      b[14] = *(ao8 +  2);
	      b[15] = *(ao8 +  3);
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
	      b[ 4] = *(ao3 +  4);
	      b[ 5] = *(ao3 +  5);
#endif
	      b[ 6] = *(ao4 +  4);
	      b[ 7] = *(ao4 +  5);
	      b[ 8] = *(ao5 +  4);
	      b[ 9] = *(ao5 +  5);
	      b[10] = *(ao6 +  4);
	      b[11] = *(ao6 +  5);
	      b[12] = *(ao7 +  4);
	      b[13] = *(ao7 +  5);
	      b[14] = *(ao8 +  4);
	      b[15] = *(ao8 +  5);
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
	      b[ 6] = *(ao4 +  6);
	      b[ 7] = *(ao4 +  7);
#endif
	      b[ 8] = *(ao5 +  6);
	      b[ 9] = *(ao5 +  7);
	      b[10] = *(ao6 +  6);
	      b[11] = *(ao6 +  7);
	      b[12] = *(ao7 +  6);
	      b[13] = *(ao7 +  7);
	      b[14] = *(ao8 +  6);
	      b[15] = *(ao8 +  7);
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
	      b[ 8] = *(ao5 +  8);
	      b[ 9] = *(ao5 +  9);
#endif
	      b[10] = *(ao6 +  8);
	      b[11] = *(ao6 +  9);
	      b[12] = *(ao7 +  8);
	      b[13] = *(ao7 +  9);
	      b[14] = *(ao8 +  8);
	      b[15] = *(ao8 +  9);
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
	      b[10] = *(ao6 + 10);
	      b[11] = *(ao6 + 11);
#endif
	      b[12] = *(ao7 + 10);
	      b[13] = *(ao7 + 11);
	      b[14] = *(ao8 + 10);
	      b[15] = *(ao8 + 11);
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
	      b[12] = *(ao7 + 12);
	      b[13] = *(ao7 + 13);
#endif
	      b[14] = *(ao8 + 12);
	      b[15] = *(ao8 + 13);
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
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
	ao3 = a + posX * 2 + (posY + 2) * lda;
	ao4 = a + posX * 2 + (posY + 3) * lda;
      } else {
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
	ao3 = a + posY * 2 + (posX + 2) * lda;
	ao4 = a + posY * 2 + (posX + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X < posY) {
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
	    if (X > posY) {
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
	      b[  2] = *(ao2 +  0);
	      b[  3] = *(ao2 +  1);
	      b[  4] = *(ao3 +  0);
	      b[  5] = *(ao3 +  1);
	      b[  6] = *(ao4 +  0);
	      b[  7] = *(ao4 +  1);

	      b[  8] = ZERO;
	      b[  9] = ZERO;
#ifdef UNIT
	      b[ 10] = ONE;
	      b[ 11] = ZERO;
#else
	      b[ 10] = *(ao2 +  2);
	      b[ 11] = *(ao2 +  3);
#endif
	      b[ 12] = *(ao3 +  2);
	      b[ 13] = *(ao3 +  3);
	      b[ 14] = *(ao4 +  2);
	      b[ 15] = *(ao4 +  3);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
	      b[ 18] = ZERO;
	      b[ 19] = ZERO;
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(ao3 +  4);
	      b[ 21] = *(ao3 +  5);
#endif
	      b[ 22] = *(ao4 +  4);
	      b[ 23] = *(ao4 +  5);

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
	      b[ 30] = *(ao4 +  6);
	      b[ 31] = *(ao4 +  7);
#endif

	      ao1 += 4 * lda;
	      ao2 += 4 * lda;
	      ao3 += 4 * lda;
	      ao4 += 4 * lda;

	      b += 32;
	    }

	  X += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);
      if (i) {

	if (X < posY) {

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
	  if (X > posY) {
	    /* ao1 += i * lda;
	    ao2 += i * lda;
	    ao3 += i * lda;
	    ao4 += i * lda; */
	    b += 8 * i;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(ao1 +  0);
	    b[ 1] = *(ao1 +  1);
#endif
	    b[ 2] = *(ao2 +  0);
	    b[ 3] = *(ao2 +  1);
	    b[ 4] = *(ao3 +  0);
	    b[ 5] = *(ao3 +  1);
	    b[ 6] = *(ao4 +  0);
	    b[ 7] = *(ao4 +  1);
	    b += 8;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(ao2 +  2);
	      b[ 3] = *(ao2 +  3);
#endif
	      b[ 4] = *(ao3 +  2);
	      b[ 5] = *(ao3 +  3);
	      b[ 6] = *(ao4 +  2);
	      b[ 7] = *(ao4 +  3);
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
	      b[ 4] = *(ao3 +  4);
	      b[ 5] = *(ao3 +  5);
#endif
	      b[ 6] = *(ao4 +  4);
	      b[ 7] = *(ao4 +  5);
	      b += 8;
	    }
	  }
      }

      posY += 4;
  }

  if (n & 2){
      X = posX;

      if (posX <= posY) {
	ao1 = a + posX * 2 + (posY + 0) * lda;
	ao2 = a + posX * 2 + (posY + 1) * lda;
      } else {
	ao1 = a + posY * 2 + (posX + 0) * lda;
	ao2 = a + posY * 2 + (posX + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X < posY) {
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
	    if (X > posY) {
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
	      b[  2] = *(ao2 +  0);
	      b[  3] = *(ao2 +  1);

	      b[  4] = ZERO;
	      b[  5] = ZERO;
#ifdef UNIT
	      b[  6] = ONE;
	      b[  7] = ZERO;
#else
	      b[  6] = *(ao2 +  2);
	      b[  7] = *(ao2 +  3);
#endif

	      ao1 += 2 * lda;
	      ao2 += 2 * lda;
	      b += 8;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      if (m & 1) {

	if (X < posY) {
	  b[  0] = *(ao1 +  0);
	  b[  1] = *(ao1 +  1);
	  b[  2] = *(ao2 +  0);
	  b[  3] = *(ao2 +  1);
	  /* ao1 += 2;
	  ao2 += 2; */
	  b += 4;
	} else
	  if (X > posY) {
	    /* ao1 += 2 * lda;
	    ao2 += 2 * lda; */
	    b += 4;
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
	    b[  2] = *(ao2 +  0);
	    b[  3] = *(ao2 +  1);
#else
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    b[  2] = *(ao2 +  0);
	    b[  3] = *(ao2 +  1);
#endif
	    b += 2;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	ao1 = a + posX * 2 + (posY + 0) * lda;
      } else {
	ao1 = a + posY * 2 + (posX + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X < posY) {
	    b[  0] = *(ao1 +  0);
	    b[  1] = *(ao1 +  1);
	    ao1 += 2;
	    b += 2;
	  } else
	    if (X > posY) {
	      ao1 += lda;
	      b += 2;
	    } else {
#ifdef UNIT
	      b[ 0] = ONE;
	      b[ 1] = ZERO;
#else
	      b[  0] = *(ao1 +  0);
	      b[  1] = *(ao1 +  1);
#endif
	      ao1 += lda;
	      b += 2;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
