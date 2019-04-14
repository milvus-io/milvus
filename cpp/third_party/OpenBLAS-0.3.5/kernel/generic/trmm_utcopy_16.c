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
  FLOAT *a09, *a10, *a11, *a12, *a13, *a14, *a15, *a16;

  js = (n >> 4);

  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	a01 = a + posX + (posY +  0) * lda;
	a02 = a + posX + (posY +  1) * lda;
	a03 = a + posX + (posY +  2) * lda;
	a04 = a + posX + (posY +  3) * lda;
	a05 = a + posX + (posY +  4) * lda;
	a06 = a + posX + (posY +  5) * lda;
	a07 = a + posX + (posY +  6) * lda;
	a08 = a + posX + (posY +  7) * lda;
	a09 = a + posX + (posY +  8) * lda;
	a10 = a + posX + (posY +  9) * lda;
	a11 = a + posX + (posY + 10) * lda;
	a12 = a + posX + (posY + 11) * lda;
	a13 = a + posX + (posY + 12) * lda;
	a14 = a + posX + (posY + 13) * lda;
	a15 = a + posX + (posY + 14) * lda;
	a16 = a + posX + (posY + 15) * lda;
      } else {
	a01 = a + posY + (posX +  0) * lda;
	a02 = a + posY + (posX +  1) * lda;
	a03 = a + posY + (posX +  2) * lda;
	a04 = a + posY + (posX +  3) * lda;
	a05 = a + posY + (posX +  4) * lda;
	a06 = a + posY + (posX +  5) * lda;
	a07 = a + posY + (posX +  6) * lda;
	a08 = a + posY + (posX +  7) * lda;
	a09 = a + posY + (posX +  8) * lda;
	a10 = a + posY + (posX +  9) * lda;
	a11 = a + posY + (posX + 10) * lda;
	a12 = a + posY + (posX + 11) * lda;
	a13 = a + posY + (posX + 12) * lda;
	a14 = a + posY + (posX + 13) * lda;
	a15 = a + posY + (posX + 14) * lda;
	a16 = a + posY + (posX + 15) * lda;
      }

      i = (m >> 4);
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
	    a09 += 16;
	    a10 += 16;
	    a11 += 16;
	    a12 += 16;
	    a13 += 16;
	    a14 += 16;
	    a15 += 16;
	    a16 += 16;
	    b += 256;
	  } else
	    if (X > posY) {

	      for (ii = 0; ii < 16; ii++){

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

	      a02 += 16 * lda;
	      a03 += 16 * lda;
	      a04 += 16 * lda;
	      a05 += 16 * lda;
	      a06 += 16 * lda;
	      a07 += 16 * lda;
	      a08 += 16 * lda;
	      a09 += 16 * lda;
	      a10 += 16 * lda;
	      a11 += 16 * lda;
	      a12 += 16 * lda;
	      a13 += 16 * lda;
	      a14 += 16 * lda;
	      a15 += 16 * lda;
	      a16 += 16 * lda;

	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = ZERO;
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
#ifdef UNIT
	      b[ 17] = ONE;
#else
	      b[ 17] = *(a02 +  1);
#endif
	      b[ 18] = ZERO;
	      b[ 19] = ZERO;
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
#ifdef UNIT
	      b[ 34] = ONE;
#else
	      b[ 34] = *(a03 +  2);
#endif
	      b[ 35] = ZERO;
	      b[ 36] = ZERO;
	      b[ 37] = ZERO;
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
#ifdef UNIT
	      b[ 51] = ONE;
#else
	      b[ 51] = *(a04 +  3);
#endif
	      b[ 52] = ZERO;
	      b[ 53] = ZERO;
	      b[ 54] = ZERO;
	      b[ 55] = ZERO;
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
#ifdef UNIT
	      b[ 68] = ONE;
#else
	      b[ 68] = *(a05 +  4);
#endif
	      b[ 69] = ZERO;
	      b[ 70] = ZERO;
	      b[ 71] = ZERO;
	      b[ 72] = ZERO;
	      b[ 73] = ZERO;
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
#ifdef UNIT
	      b[ 85] = ONE;
#else
	      b[ 85] = *(a06 +  5);
#endif
	      b[ 86] = ZERO;
	      b[ 87] = ZERO;
	      b[ 88] = ZERO;
	      b[ 89] = ZERO;
	      b[ 90] = ZERO;
	      b[ 91] = ZERO;
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
#ifdef UNIT
	      b[102] = ONE;
#else
	      b[102] = *(a07 +  6);
#endif
	      b[103] = ZERO;
	      b[104] = ZERO;
	      b[105] = ZERO;
	      b[106] = ZERO;
	      b[107] = ZERO;
	      b[108] = ZERO;
	      b[109] = ZERO;
	      b[110] = ZERO;
	      b[111] = ZERO;

	      b[112] = *(a08 +  0);
	      b[113] = *(a08 +  1);
	      b[114] = *(a08 +  2);
	      b[115] = *(a08 +  3);
	      b[116] = *(a08 +  4);
	      b[117] = *(a08 +  5);
	      b[118] = *(a08 +  6);
#ifdef UNIT
	      b[119] = ONE;
#else
	      b[119] = *(a08 +  7);
#endif
	      b[120] = ZERO;
	      b[121] = ZERO;
	      b[122] = ZERO;
	      b[123] = ZERO;
	      b[124] = ZERO;
	      b[125] = ZERO;
	      b[126] = ZERO;
	      b[127] = ZERO;

	      b[128] = *(a09 +  0);
	      b[129] = *(a09 +  1);
	      b[130] = *(a09 +  2);
	      b[131] = *(a09 +  3);
	      b[132] = *(a09 +  4);
	      b[133] = *(a09 +  5);
	      b[134] = *(a09 +  6);
	      b[135] = *(a09 +  7);
#ifdef UNIT
	      b[136] = ONE;
#else
	      b[136] = *(a09 +  8);
#endif
	      b[137] = ZERO;
	      b[138] = ZERO;
	      b[139] = ZERO;
	      b[140] = ZERO;
	      b[141] = ZERO;
	      b[142] = ZERO;
	      b[143] = ZERO;

	      b[144] = *(a10 +  0);
	      b[145] = *(a10 +  1);
	      b[146] = *(a10 +  2);
	      b[147] = *(a10 +  3);
	      b[148] = *(a10 +  4);
	      b[149] = *(a10 +  5);
	      b[150] = *(a10 +  6);
	      b[151] = *(a10 +  7);
	      b[152] = *(a10 +  8);
#ifdef UNIT
	      b[153] = ONE;
#else
	      b[153] = *(a10 +  9);
#endif
	      b[154] = ZERO;
	      b[155] = ZERO;
	      b[156] = ZERO;
	      b[157] = ZERO;
	      b[158] = ZERO;
	      b[159] = ZERO;

	      b[160] = *(a11 +  0);
	      b[161] = *(a11 +  1);
	      b[162] = *(a11 +  2);
	      b[163] = *(a11 +  3);
	      b[164] = *(a11 +  4);
	      b[165] = *(a11 +  5);
	      b[166] = *(a11 +  6);
	      b[167] = *(a11 +  7);
	      b[168] = *(a11 +  8);
	      b[169] = *(a11 +  9);
#ifdef UNIT
	      b[170] = ONE;
#else
	      b[170] = *(a11 + 10);
#endif
	      b[171] = ZERO;
	      b[172] = ZERO;
	      b[173] = ZERO;
	      b[174] = ZERO;
	      b[175] = ZERO;

	      b[176] = *(a12 +  0);
	      b[177] = *(a12 +  1);
	      b[178] = *(a12 +  2);
	      b[179] = *(a12 +  3);
	      b[180] = *(a12 +  4);
	      b[181] = *(a12 +  5);
	      b[182] = *(a12 +  6);
	      b[183] = *(a12 +  7);
	      b[184] = *(a12 +  8);
	      b[185] = *(a12 +  9);
	      b[186] = *(a12 + 10);
#ifdef UNIT
	      b[187] = ONE;
#else
	      b[187] = *(a12 + 11);
#endif
	      b[188] = ZERO;
	      b[189] = ZERO;
	      b[190] = ZERO;
	      b[191] = ZERO;

	      b[192] = *(a13 +  0);
	      b[193] = *(a13 +  1);
	      b[194] = *(a13 +  2);
	      b[195] = *(a13 +  3);
	      b[196] = *(a13 +  4);
	      b[197] = *(a13 +  5);
	      b[198] = *(a13 +  6);
	      b[199] = *(a13 +  7);
	      b[200] = *(a13 +  8);
	      b[201] = *(a13 +  9);
	      b[202] = *(a13 + 10);
	      b[203] = *(a13 + 11);
#ifdef UNIT
	      b[204] = ONE;
#else
	      b[204] = *(a13 + 12);
#endif
	      b[205] = ZERO;
	      b[206] = ZERO;
	      b[207] = ZERO;

	      b[208] = *(a14 +  0);
	      b[209] = *(a14 +  1);
	      b[210] = *(a14 +  2);
	      b[211] = *(a14 +  3);
	      b[212] = *(a14 +  4);
	      b[213] = *(a14 +  5);
	      b[214] = *(a14 +  6);
	      b[215] = *(a14 +  7);
	      b[216] = *(a14 +  8);
	      b[217] = *(a14 +  9);
	      b[218] = *(a14 + 10);
	      b[219] = *(a14 + 11);
	      b[220] = *(a14 + 12);
#ifdef UNIT
	      b[221] = ONE;
#else
	      b[221] = *(a14 + 13);
#endif
	      b[222] = ZERO;
	      b[223] = ZERO;

	      b[224] = *(a15 +  0);
	      b[225] = *(a15 +  1);
	      b[226] = *(a15 +  2);
	      b[227] = *(a15 +  3);
	      b[228] = *(a15 +  4);
	      b[229] = *(a15 +  5);
	      b[230] = *(a15 +  6);
	      b[231] = *(a15 +  7);
	      b[232] = *(a15 +  8);
	      b[233] = *(a15 +  9);
	      b[234] = *(a15 + 10);
	      b[235] = *(a15 + 11);
	      b[236] = *(a15 + 12);
	      b[237] = *(a15 + 13);
#ifdef UNIT
	      b[238] = ONE;
#else
	      b[238] = *(a15 + 14);
#endif
	      b[239] = ZERO;

	      b[240] = *(a16 +  0);
	      b[241] = *(a16 +  1);
	      b[242] = *(a16 +  2);
	      b[243] = *(a16 +  3);
	      b[244] = *(a16 +  4);
	      b[245] = *(a16 +  5);
	      b[246] = *(a16 +  6);
	      b[247] = *(a16 +  7);
	      b[248] = *(a16 +  8);
	      b[249] = *(a16 +  9);
	      b[250] = *(a16 + 10);
	      b[251] = *(a16 + 11);
	      b[252] = *(a16 + 12);
	      b[253] = *(a16 + 13);
	      b[254] = *(a16 + 14);
#ifdef UNIT
	      b[255] = ONE;
#else
	      b[255] = *(a16 + 15);
#endif

	      a01 += 16 * lda;
	      a02 += 16 * lda;
	      a03 += 16 * lda;
	      a04 += 16 * lda;
	      a05 += 16 * lda;
	      a06 += 16 * lda;
	      a07 += 16 * lda;
	      a08 += 16 * lda;
	      a09 += 16 * lda;
	      a10 += 16 * lda;
	      a11 += 16 * lda;
	      a12 += 16 * lda;
	      a13 += 16 * lda;
	      a14 += 16 * lda;
	      a15 += 16 * lda;
	      a16 += 16 * lda;

	      b += 256;
	    }

	  X += 16;
	  i --;
	} while (i > 0);
      }

      i = (m & 15);
      if (i > 0) {
	if (X < posY) {
	  a01 += i;
	  a02 += i;
	  a03 += i;
	  a04 += i;
	  a05 += i;
	  a06 += i;
	  a07 += i;
	  a08 += i;
	  a09 += i;
	  a10 += i;
	  a11 += i;
	  a12 += i;
	  a13 += i;
	  a14 += i;
	  a15 += i;
	  a16 += i;
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
	      a09 += lda;
	      a10 += lda;
	      a11 += lda;
	      a12 += lda;
	      a13 += lda;
	      a14 += lda;
	      a15 += lda;
	      a16 += lda;
	      b += 16;
	    }

	  } else {
#ifdef UNIT
	      b[ 0] = ONE;
#else
	      b[ 0] = *(a01 +  0);
#endif
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
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;

	      if (i >= 2) {
		b[ 0] = *(a02 +  0);
#ifdef UNIT
		b[ 1] = ONE;
#else
		b[ 1] = *(a02 +  1);
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
	      }

	      if (i >= 3) {
		b[ 0] = *(a03 +  0);
		b[ 1] = *(a03 +  1);
#ifdef UNIT
		b[ 2] = ONE;
#else
		b[ 2] = *(a03 +  2);
#endif
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
	      }

	      if (i >= 4) {
		b[ 0] = *(a04 +  0);
		b[ 1] = *(a04 +  1);
		b[ 2] = *(a04 +  2);
#ifdef UNIT
		b[ 3] = ONE;
#else
		b[ 3] = *(a04 +  3);
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
		b[16] = ZERO;
		b += 16;
	      }


	      if (i >= 5) {
		b[ 0] = *(a05 +  0);
		b[ 1] = *(a05 +  1);
		b[ 2] = *(a05 +  2);
		b[ 3] = *(a05 +  3);
#ifdef UNIT
		b[ 4] = ONE;
#else
		b[ 4] = *(a05 +  4);
#endif
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

	      if (i >= 6) {
		b[ 0] = *(a06 +  0);
		b[ 1] = *(a06 +  1);
		b[ 2] = *(a06 +  2);
		b[ 3] = *(a06 +  3);
		b[ 4] = *(a06 +  4);
#ifdef UNIT
		b[ 5] = ONE;
#else
		b[ 5] = *(a06 +  5);
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

	      if (i >= 7) {
		b[ 0] = *(a07 +  0);
		b[ 1] = *(a07 +  1);
		b[ 2] = *(a07 +  2);
		b[ 3] = *(a07 +  3);
		b[ 4] = *(a07 +  4);
		b[ 5] = *(a07 +  5);
#ifdef UNIT
		b[ 6] = ONE;
#else
		b[ 6] = *(a07 +  6);
#endif
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

	      if (i >= 8) {
		b[ 0] = *(a08 +  0);
		b[ 1] = *(a08 +  1);
		b[ 2] = *(a08 +  2);
		b[ 3] = *(a08 +  3);
		b[ 4] = *(a08 +  4);
		b[ 5] = *(a08 +  5);
		b[ 6] = *(a08 +  6);
#ifdef UNIT
		b[ 7] = ONE;
#else
		b[ 7] = *(a08 +  7);
#endif
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b[16] = ZERO;
		b += 16;
	      }

	      if (i >= 9) {
		b[ 0] = *(a09 +  0);
		b[ 1] = *(a09 +  1);
		b[ 2] = *(a09 +  2);
		b[ 3] = *(a09 +  3);
		b[ 4] = *(a09 +  4);
		b[ 5] = *(a09 +  5);
		b[ 6] = *(a09 +  6);
		b[ 7] = *(a09 +  7);
#ifdef UNIT
		b[ 8] = ONE;
#else
		b[ 8] = *(a09 +  8);
#endif
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 10) {
		b[ 0] = *(a10 +  0);
		b[ 1] = *(a10 +  1);
		b[ 2] = *(a10 +  2);
		b[ 3] = *(a10 +  3);
		b[ 4] = *(a10 +  4);
		b[ 5] = *(a10 +  5);
		b[ 6] = *(a10 +  6);
		b[ 7] = *(a10 +  7);
		b[ 8] = *(a10 +  8);
#ifdef UNIT
		b[ 9] = ONE;
#else
		b[ 9] = *(a10 +  9);
#endif
		b[10] = ZERO;
		b[11] = ZERO;
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 11) {
		b[ 0] = *(a11 +  0);
		b[ 1] = *(a11 +  1);
		b[ 2] = *(a11 +  2);
		b[ 3] = *(a11 +  3);
		b[ 4] = *(a11 +  4);
		b[ 5] = *(a11 +  5);
		b[ 6] = *(a11 +  6);
		b[ 7] = *(a11 +  7);
		b[ 8] = *(a11 +  8);
		b[ 9] = *(a11 +  9);
#ifdef UNIT
		b[10] = ONE;
#else
		b[10] = *(a11 + 10);
#endif
		b[11] = ZERO;
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 12) {
		b[ 0] = *(a12 +  0);
		b[ 1] = *(a12 +  1);
		b[ 2] = *(a12 +  2);
		b[ 3] = *(a12 +  3);
		b[ 4] = *(a12 +  4);
		b[ 5] = *(a12 +  5);
		b[ 6] = *(a12 +  6);
		b[ 7] = *(a12 +  7);
		b[ 8] = *(a12 +  8);
		b[ 9] = *(a12 +  9);
		b[10] = *(a12 + 10);
#ifdef UNIT
		b[11] = ONE;
#else
		b[11] = *(a12 + 11);
#endif
		b[12] = ZERO;
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 13) {
		b[ 0] = *(a13 +  0);
		b[ 1] = *(a13 +  1);
		b[ 2] = *(a13 +  2);
		b[ 3] = *(a13 +  3);
		b[ 4] = *(a13 +  4);
		b[ 5] = *(a13 +  5);
		b[ 6] = *(a13 +  6);
		b[ 7] = *(a13 +  7);
		b[ 8] = *(a13 +  8);
		b[ 9] = *(a13 +  9);
		b[10] = *(a13 + 10);
		b[11] = *(a13 + 11);
#ifdef UNIT
		b[12] = ONE;
#else
		b[12] = *(a13 + 12);
#endif
		b[13] = ZERO;
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 14) {
		b[ 0] = *(a14 +  0);
		b[ 1] = *(a14 +  1);
		b[ 2] = *(a14 +  2);
		b[ 3] = *(a14 +  3);
		b[ 4] = *(a14 +  4);
		b[ 5] = *(a14 +  5);
		b[ 6] = *(a14 +  6);
		b[ 7] = *(a14 +  7);
		b[ 8] = *(a14 +  8);
		b[ 9] = *(a14 +  9);
		b[10] = *(a14 + 10);
		b[11] = *(a14 + 11);
		b[12] = *(a14 + 12);
#ifdef UNIT
		b[13] = ONE;
#else
		b[13] = *(a14 + 13);
#endif
		b[14] = ZERO;
		b[15] = ZERO;
		b += 16;
	      }

	      if (i >= 15) {
		b[ 0] = *(a15 +  0);
		b[ 1] = *(a15 +  1);
		b[ 2] = *(a15 +  2);
		b[ 3] = *(a15 +  3);
		b[ 4] = *(a15 +  4);
		b[ 5] = *(a15 +  5);
		b[ 6] = *(a15 +  6);
		b[ 7] = *(a15 +  7);
		b[ 8] = *(a15 +  8);
		b[ 9] = *(a15 +  9);
		b[10] = *(a15 + 10);
		b[11] = *(a15 + 11);
		b[12] = *(a15 + 12);
		b[13] = *(a15 + 13);
#ifdef UNIT
		b[14] = ONE;
#else
		b[14] = *(a15 + 14);
#endif
		b[15] = ZERO;
	      }
  	  }
      }
      posY += 16;
      js --;
    } while (js > 0);
  } /* End of main loop */


  if (n & 8){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
      a03 = a + posX + (posY +  2) * lda;
      a04 = a + posX + (posY +  3) * lda;
      a05 = a + posX + (posY +  4) * lda;
      a06 = a + posX + (posY +  5) * lda;
      a07 = a + posX + (posY +  6) * lda;
      a08 = a + posX + (posY +  7) * lda;
    } else {
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
      a03 = a + posY + (posX +  2) * lda;
      a04 = a + posY + (posX +  3) * lda;
      a05 = a + posY + (posX +  4) * lda;
      a06 = a + posY + (posX +  5) * lda;
      a07 = a + posY + (posX +  6) * lda;
      a08 = a + posY + (posX +  7) * lda;
    }

    i = (m >> 3);
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 8;
	  a02 += 8;
	  a03 += 8;
	  a04 += 8;
	  a05 += 8;
	  a06 += 8;
	  a07 += 8;
	  a08 += 8;
	  b += 64;
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

	      a01 += lda;
	      b += 8;
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
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;

	      b[  8] = *(a02 +  0);
#ifdef UNIT
	      b[  9] = ONE;
#else
	      b[  9] = *(a02 +  1);
#endif
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
	      b[ 14] = ZERO;
	      b[ 15] = ZERO;

	      b[ 16] = *(a03 +  0);
	      b[ 17] = *(a03 +  1);
#ifdef UNIT
	      b[ 18] = ONE;
#else
	      b[ 18] = *(a03 +  2);
#endif
	      b[ 19] = ZERO;
	      b[ 20] = ZERO;
	      b[ 21] = ZERO;
	      b[ 22] = ZERO;
	      b[ 23] = ZERO;

	      b[ 24] = *(a04 +  0);
	      b[ 25] = *(a04 +  1);
	      b[ 26] = *(a04 +  2);
#ifdef UNIT
	      b[ 27] = ONE;
#else
	      b[ 27] = *(a04 +  3);
#endif
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
	      b[ 30] = ZERO;
	      b[ 31] = ZERO;

	      b[ 32] = *(a05 +  0);
	      b[ 33] = *(a05 +  1);
	      b[ 34] = *(a05 +  2);
	      b[ 35] = *(a05 +  3);
#ifdef UNIT
	      b[ 36] = ONE;
#else
	      b[ 36] = *(a05 +  4);
#endif
	      b[ 37] = ZERO;
	      b[ 38] = ZERO;
	      b[ 39] = ZERO;

	      b[ 40] = *(a06 +  0);
	      b[ 41] = *(a06 +  1);
	      b[ 42] = *(a06 +  2);
	      b[ 43] = *(a06 +  3);
	      b[ 44] = *(a06 +  4);
#ifdef UNIT
	      b[ 45] = ONE;
#else
	      b[ 45] = *(a06 +  5);
#endif
	      b[ 46] = ZERO;
	      b[ 47] = ZERO;

	      b[ 48] = *(a07 +  0);
	      b[ 49] = *(a07 +  1);
	      b[ 50] = *(a07 +  2);
	      b[ 51] = *(a07 +  3);
	      b[ 52] = *(a07 +  4);
	      b[ 53] = *(a07 +  5);
#ifdef UNIT
	      b[ 54] = ONE;
#else
	      b[ 54] = *(a07 +  6);
#endif
	      b[ 55] = ZERO;

	      b[ 56] = *(a08 +  0);
	      b[ 57] = *(a08 +  1);
	      b[ 58] = *(a08 +  2);
	      b[ 59] = *(a08 +  3);
	      b[ 60] = *(a08 +  4);
	      b[ 61] = *(a08 +  5);
	      b[ 62] = *(a08 +  6);
#ifdef UNIT
	      b[ 63] = ONE;
#else
	      b[ 63] = *(a08 +  7);
#endif

	      a01 += 8 * lda;
	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;

	      b += 64;
	  }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i > 0) {
	if (X < posY) {
	  a01 += i;
	  a02 += i;
	  a03 += i;
	  a04 += i;
	  a05 += i;
	  a06 += i;
	  a07 += i;
	  a08 += i;
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
	      b += 8;
	    }

	    a02 += i * lda;
	    a03 += i * lda;
	    a04 += i * lda;
	    a05 += i * lda;
	    a06 += i * lda;
	    a07 += i * lda;
	    a08 += i * lda;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
#else
	    b[ 0] = *(a01 +  0);
#endif
	    b[ 1] = ZERO;
	    b[ 2] = ZERO;
	    b[ 3] = ZERO;
	    b[ 4] = ZERO;
	    b[ 5] = ZERO;
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b += 8;

	    if (i >= 2) {
	      b[ 0] = *(a02 +  0);
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = *(a02 +  1);
#endif
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 3) {
	      b[ 0] = *(a03 +  0);
	      b[ 1] = *(a03 +  1);
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = *(a03 +  2);
#endif
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 4) {
	      b[ 0] = *(a04 +  0);
	      b[ 1] = *(a04 +  1);
	      b[ 2] = *(a04 +  2);
#ifdef UNIT
	      b[ 3] = ONE;
#else
	      b[ 3] = *(a04 +  3);
#endif
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 5) {
	      b[ 0] = *(a05 +  0);
	      b[ 1] = *(a05 +  1);
	      b[ 2] = *(a05 +  2);
	      b[ 3] = *(a05 +  3);
#ifdef UNIT
	      b[ 4] = ONE;
#else
	      b[ 4] = *(a05 +  4);
#endif
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 6) {
	      b[ 0] = *(a06 +  0);
	      b[ 1] = *(a06 +  1);
	      b[ 2] = *(a06 +  2);
	      b[ 3] = *(a06 +  3);
	      b[ 4] = *(a06 +  4);
#ifdef UNIT
	      b[ 5] = ONE;
#else
	      b[ 5] = *(a06 +  5);
#endif
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b += 8;
	    }

	    if (i >= 7) {
	      b[ 0] = *(a07 +  0);
	      b[ 1] = *(a07 +  1);
	      b[ 2] = *(a07 +  2);
	      b[ 3] = *(a07 +  3);
	      b[ 4] = *(a07 +  4);
	      b[ 5] = *(a07 +  5);
#ifdef UNIT
	      b[ 6] = ONE;
#else
	      b[ 6] = *(a07 +  6);
#endif
	      b[ 7] = ZERO;
	      b += 8;
	    }
	  }
      }
      posY += 8;
  }

  if (n & 4){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
      a03 = a + posX + (posY +  2) * lda;
      a04 = a + posX + (posY +  3) * lda;
    } else {
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
      a03 = a + posY + (posX +  2) * lda;
      a04 = a + posY + (posX +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 4;
	  a02 += 4;
	  a03 += 4;
	  a04 += 4;
	  b += 16;
	} else
	  if (X > posY) {

	    for (ii = 0; ii < 4; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      a01 += lda;
	      b += 4;
	      }

	    a02 += 4 * lda;
	    a03 += 4 * lda;
	    a04 += 4 * lda;

	  } else {
#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    b[  1] = ZERO;
	    b[  2] = ZERO;
	    b[  3] = ZERO;

	    b[  4] = *(a02 +  0);
#ifdef UNIT
	    b[  5] = ONE;
#else
	    b[  5] = *(a02 +  1);
#endif
	    b[  6] = ZERO;
	    b[  7] = ZERO;

	    b[  8] = *(a03 +  0);
	    b[  9] = *(a03 +  1);
#ifdef UNIT
	    b[ 10] = ONE;
#else
	    b[ 10] = *(a03 +  2);
#endif
	    b[ 11] = ZERO;

	    b[ 12] = *(a04 +  0);
	    b[ 13] = *(a04 +  1);
	    b[ 14] = *(a04 +  2);
#ifdef UNIT
	    b[ 15] = ONE;
#else
	    b[ 15] = *(a04 +  3);
#endif

	    a01 += 4 * lda;
	    a02 += 4 * lda;
	    a03 += 4 * lda;
	    a04 += 4 * lda;
	    b += 16;
	  }

	  X += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);
      if (i > 0) {
	if (X < posY) {
	  a01 += i;
	  a02 += i;
	  a03 += i;
	  a04 += i;
	  b += 4 * i;
	} else
	  if (X > posY) {
	    for (ii = 0; ii < i; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      a01 += lda;
	      b += 4;
	    }
	    a02 += lda;
	    a03 += lda;
	    a04 += lda;
	  } else {

#ifdef UNIT
	    b[ 0] = ONE;
#else
	    b[ 0] = *(a01 +  0);
#endif
	    b[ 1] = ZERO;
	    b[ 2] = ZERO;
	    b[ 3] = ZERO;
	    b += 4;

	    if (i >= 2) {
	      b[ 0] = *(a02 +  0);
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = *(a02 +  1);
#endif
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b += 4;
	    }

	    if (i >= 3) {
	      b[ 0] = *(a03 +  0);
	      b[ 1] = *(a03 +  1);
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = *(a03 +  2);
#endif
	      b[ 3] = ZERO;
	      b += 4;
	    }
	  }
      }
      posY += 4;
  }

  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
    } else {
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 2;
	  a02 += 2;
	  b += 4;
	} else
	  if (X > posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 4;
	  } else {

#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    b[  1] = ZERO;

	    b[  2] = *(a02 +  0);
#ifdef UNIT
	    b[  3] = ONE;
#else
	    b[  3] = *(a02 +  1);
#endif

	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 4;
	  }

	  X += 2;
	  i --;
	} while (i > 0);
      }

    if (m & 1) {
      if (X < posY) {
	a01 ++;
	a02 ++;
	b += 2;
      } else
	if (X > posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  a01 += lda;
	  b += 2;
	} else {
#ifdef UNIT
	  b[  0] = ONE;
	  b[  1] = *(a01 +  1);
#else
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
#endif
	  b += 2;
	}
    }
    posY += 2;
  }


  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX + (posY +  0) * lda;
    } else {
      a01 = a + posY + (posX +  0) * lda;
    }

    i = m;
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 1;
	  b ++;
	} else
	  if (X > posY) {
	    b[  0] = *(a01 +  0);
	    a01 += lda;
	    b ++;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
#else
	    b[ 0] = *(a01 +  0);
#endif
	    a01 += lda;
	    b ++;
	  }

	  X += 1;
	  i --;
      } while (i > 0);
    }
    posY += 1;
  }

  return 0;
}
