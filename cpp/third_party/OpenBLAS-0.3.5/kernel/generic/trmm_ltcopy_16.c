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
      } else {
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
      }

      i = (m >> 4);
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
	    if (X < posY) {

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

	      b[ 16] = ZERO;
#ifdef UNIT
	      b[ 17] = ONE;
#else
	      b[ 17] = *(a02 +  1);
#endif
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
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
#ifdef UNIT
	      b[ 34] = ONE;
#else
	      b[ 34] = *(a03 +  2);
#endif
	      b[ 35] = *(a03 +  3);
	      b[ 36] = *(a03 +  4);
	      b[ 37] = *(a03 +  5);
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
#ifdef UNIT
	      b[ 51] = ONE;
#else
	      b[ 51] = *(a04 +  3);
#endif
	      b[ 52] = *(a04 +  4);
	      b[ 53] = *(a04 +  5);
	      b[ 54] = *(a04 +  6);
	      b[ 55] = *(a04 +  7);
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
#ifdef UNIT
	      b[ 68] = ONE;
#else
	      b[ 68] = *(a05 +  4);
#endif
	      b[ 69] = *(a05 +  5);
	      b[ 70] = *(a05 +  6);
	      b[ 71] = *(a05 +  7);
	      b[ 72] = *(a05 +  8);
	      b[ 73] = *(a05 +  9);
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
#ifdef UNIT
	      b[ 85] = ONE;
#else
	      b[ 85] = *(a06 +  5);
#endif
	      b[ 86] = *(a06 +  6);
	      b[ 87] = *(a06 +  7);
	      b[ 88] = *(a06 +  8);
	      b[ 89] = *(a06 +  9);
	      b[ 90] = *(a06 + 10);
	      b[ 91] = *(a06 + 11);
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
#ifdef UNIT
	      b[102] = ONE;
#else
	      b[102] = *(a07 +  6);
#endif
	      b[103] = *(a07 +  7);
	      b[104] = *(a07 +  8);
	      b[105] = *(a07 +  9);
	      b[106] = *(a07 + 10);
	      b[107] = *(a07 + 11);
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
	      b[110] = *(a07 + 14);
	      b[111] = *(a07 + 15);

	      b[112] = ZERO;
	      b[113] = ZERO;
	      b[114] = ZERO;
	      b[115] = ZERO;
	      b[116] = ZERO;
	      b[117] = ZERO;
	      b[118] = ZERO;
#ifdef UNIT
	      b[119] = ONE;
#else
	      b[119] = *(a08 +  7);
#endif
	      b[120] = *(a08 +  8);
	      b[121] = *(a08 +  9);
	      b[122] = *(a08 + 10);
	      b[123] = *(a08 + 11);
	      b[124] = *(a08 + 12);
	      b[125] = *(a08 + 13);
	      b[126] = *(a08 + 14);
	      b[127] = *(a08 + 15);

	      b[128] = ZERO;
	      b[129] = ZERO;
	      b[130] = ZERO;
	      b[131] = ZERO;
	      b[132] = ZERO;
	      b[133] = ZERO;
	      b[134] = ZERO;
	      b[135] = ZERO;
#ifdef UNIT
	      b[136] = ONE;
#else
	      b[136] = *(a09 +  8);
#endif
	      b[137] = *(a09 +  9);
	      b[138] = *(a09 + 10);
	      b[139] = *(a09 + 11);
	      b[140] = *(a09 + 12);
	      b[141] = *(a09 + 13);
	      b[142] = *(a09 + 14);
	      b[143] = *(a09 + 15);

	      b[144] = ZERO;
	      b[145] = ZERO;
	      b[146] = ZERO;
	      b[147] = ZERO;
	      b[148] = ZERO;
	      b[149] = ZERO;
	      b[150] = ZERO;
	      b[151] = ZERO;
	      b[152] = ZERO;
#ifdef UNIT
	      b[153] = ONE;
#else
	      b[153] = *(a10 +  9);
#endif
	      b[154] = *(a10 + 10);
	      b[155] = *(a10 + 11);
	      b[156] = *(a10 + 12);
	      b[157] = *(a10 + 13);
	      b[158] = *(a10 + 14);
	      b[159] = *(a10 + 15);

	      b[160] = ZERO;
	      b[161] = ZERO;
	      b[162] = ZERO;
	      b[163] = ZERO;
	      b[164] = ZERO;
	      b[165] = ZERO;
	      b[166] = ZERO;
	      b[167] = ZERO;
	      b[168] = ZERO;
	      b[169] = ZERO;
#ifdef UNIT
	      b[170] = ONE;
#else
	      b[170] = *(a11 + 10);
#endif
	      b[171] = *(a11 + 11);
	      b[172] = *(a11 + 12);
	      b[173] = *(a11 + 13);
	      b[174] = *(a11 + 14);
	      b[175] = *(a11 + 15);

	      b[176] = ZERO;
	      b[177] = ZERO;
	      b[178] = ZERO;
	      b[179] = ZERO;
	      b[180] = ZERO;
	      b[181] = ZERO;
	      b[182] = ZERO;
	      b[183] = ZERO;
	      b[184] = ZERO;
	      b[185] = ZERO;
	      b[186] = ZERO;
#ifdef UNIT
	      b[187] = ONE;
#else
	      b[187] = *(a12 + 11);
#endif
	      b[188] = *(a12 + 12);
	      b[189] = *(a12 + 13);
	      b[190] = *(a12 + 14);
	      b[191] = *(a12 + 15);

	      b[192] = ZERO;
	      b[193] = ZERO;
	      b[194] = ZERO;
	      b[195] = ZERO;
	      b[196] = ZERO;
	      b[197] = ZERO;
	      b[198] = ZERO;
	      b[199] = ZERO;
	      b[200] = ZERO;
	      b[201] = ZERO;
	      b[202] = ZERO;
	      b[203] = ZERO;
#ifdef UNIT
	      b[204] = ONE;
#else
	      b[204] = *(a13 + 12);
#endif
	      b[205] = *(a13 + 13);
	      b[206] = *(a13 + 14);
	      b[207] = *(a13 + 15);

	      b[208] = ZERO;
	      b[209] = ZERO;
	      b[210] = ZERO;
	      b[211] = ZERO;
	      b[212] = ZERO;
	      b[213] = ZERO;
	      b[214] = ZERO;
	      b[215] = ZERO;
	      b[216] = ZERO;
	      b[217] = ZERO;
	      b[218] = ZERO;
	      b[219] = ZERO;
	      b[220] = ZERO;
#ifdef UNIT
	      b[221] = ONE;
#else
	      b[221] = *(a14 + 13);
#endif
	      b[222] = *(a14 + 14);
	      b[223] = *(a14 + 15);

	      b[224] = ZERO;
	      b[225] = ZERO;
	      b[226] = ZERO;
	      b[227] = ZERO;
	      b[228] = ZERO;
	      b[229] = ZERO;
	      b[230] = ZERO;
	      b[231] = ZERO;
	      b[232] = ZERO;
	      b[233] = ZERO;
	      b[234] = ZERO;
	      b[235] = ZERO;
	      b[236] = ZERO;
	      b[237] = ZERO;
#ifdef UNIT
	      b[238] = ONE;
#else
	      b[238] = *(a15 + 14);
#endif
	      b[239] = *(a15 + 15);

	      b[240] = ZERO;
	      b[241] = ZERO;
	      b[242] = ZERO;
	      b[243] = ZERO;
	      b[244] = ZERO;
	      b[245] = ZERO;
	      b[246] = ZERO;
	      b[247] = ZERO;
	      b[248] = ZERO;
	      b[249] = ZERO;
	      b[250] = ZERO;
	      b[251] = ZERO;
	      b[252] = ZERO;
	      b[253] = ZERO;
	      b[254] = ZERO;
#ifdef UNIT
	      b[255] = ONE;
#else
	      b[255] = *(a16 + 15);
#endif

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
	    }

	  X += 16;
	  i --;
	} while (i > 0);
      }

      i = (m & 15);
      if (i > 0) {
	if (X > posY) {
	  /* a01 += i;
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
	  a16 += i; */
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
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
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
	    b += 16;

	    if (i >= 2) {
	      b[ 0] = ZERO;
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = *(a02 +  1);
#endif
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
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
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = *(a03 +  2);
#endif
	      b[ 3] = *(a03 +  3);
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
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
#ifdef UNIT
	      b[ 3] = ONE;
#else
	      b[ 3] = *(a04 +  3);
#endif
	      b[ 4] = *(a04 +  4);
	      b[ 5] = *(a04 +  5);
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
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
#ifdef UNIT
	      b[ 4] = ONE;
#else
	      b[ 4] = *(a05 +  4);
#endif
	      b[ 5] = *(a05 +  5);
	      b[ 6] = *(a05 +  6);
	      b[ 7] = *(a05 +  7);
	      b[ 8] = *(a05 +  8);
	      b[ 9] = *(a05 +  9);
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
#ifdef UNIT
	      b[ 5] = ONE;
#else
	      b[ 5] = *(a06 +  5);
#endif
	      b[ 6] = *(a06 +  6);
	      b[ 7] = *(a06 +  7);
	      b[ 8] = *(a06 +  8);
	      b[ 9] = *(a06 +  9);
	      b[10] = *(a06 + 10);
	      b[11] = *(a06 + 11);
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
#ifdef UNIT
	      b[ 6] = ONE;
#else
	      b[ 6] = *(a07 +  6);
#endif
	      b[ 7] = *(a07 +  7);
	      b[ 8] = *(a07 +  8);
	      b[ 9] = *(a07 +  9);
	      b[10] = *(a07 + 10);
	      b[11] = *(a07 + 11);
	      b[12] = *(a07 + 12);
	      b[13] = *(a07 + 13);
	      b[14] = *(a07 + 14);
	      b[15] = *(a07 + 15);
	      b += 16;
	    }

	    if (i >= 8) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
#ifdef UNIT
	      b[ 7] = ONE;
#else
	      b[ 7] = *(a08 +  7);
#endif
	      b[ 8] = *(a08 +  8);
	      b[ 9] = *(a08 +  9);
	      b[10] = *(a08 + 10);
	      b[11] = *(a08 + 11);
	      b[12] = *(a08 + 12);
	      b[13] = *(a08 + 13);
	      b[14] = *(a08 + 14);
	      b[15] = *(a08 + 15);
	      b += 16;
	    }

	    if (i >= 9) {
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
#else
	      b[ 8] = *(a09 +  8);
#endif
	      b[ 9] = *(a09 +  9);
	      b[10] = *(a09 + 10);
	      b[11] = *(a09 + 11);
	      b[12] = *(a09 + 12);
	      b[13] = *(a09 + 13);
	      b[14] = *(a09 + 14);
	      b[15] = *(a09 + 15);
	      b += 16;
	    }

	    if (i >= 10) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
#ifdef UNIT
	      b[ 9] = ONE;
#else
	      b[ 9] = *(a10 +  9);
#endif
	      b[10] = *(a10 + 10);
	      b[11] = *(a10 + 11);
	      b[12] = *(a10 + 12);
	      b[13] = *(a10 + 13);
	      b[14] = *(a10 + 14);
	      b[15] = *(a10 + 15);
	      b += 16;
	    }

	    if (i >= 11) {
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
#else
	      b[10] = *(a11 + 10);
#endif
	      b[11] = *(a11 + 11);
	      b[12] = *(a11 + 12);
	      b[13] = *(a11 + 13);
	      b[14] = *(a11 + 14);
	      b[15] = *(a11 + 15);
	      b += 16;
	    }

	    if (i >= 12) {
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
#ifdef UNIT
	      b[11] = ONE;
#else
	      b[11] = *(a12 + 11);
#endif
	      b[12] = *(a12 + 12);
	      b[13] = *(a12 + 13);
	      b[14] = *(a12 + 14);
	      b[15] = *(a12 + 15);
	      b += 16;
	    }

	    if (i >= 13) {
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
#else
	      b[12] = *(a13 + 12);
#endif
	      b[13] = *(a13 + 13);
	      b[14] = *(a13 + 14);
	      b[15] = *(a13 + 15);
	      b += 16;
	    }

	    if (i >= 14) {
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
	      b[12] = ZERO;
#ifdef UNIT
	      b[13] = ONE;
#else
	      b[13] = *(a14 + 13);
#endif
	      b[14] = *(a14 + 14);
	      b[15] = *(a14 + 15);
	      b += 16;
	    }

	    if (i >= 15) {
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
	      b[12] = ZERO;
	      b[13] = ZERO;
#ifdef UNIT
	      b[14] = ONE;
#else
	      b[14] = *(a15 + 14);
#endif
	      b[15] = *(a15 + 15);
	      b += 16;
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
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
      a03 = a + posY + (posX +  2) * lda;
      a04 = a + posY + (posX +  3) * lda;
      a05 = a + posY + (posX +  4) * lda;
      a06 = a + posY + (posX +  5) * lda;
      a07 = a + posY + (posX +  6) * lda;
      a08 = a + posY + (posX +  7) * lda;
    } else {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
      a03 = a + posX + (posY +  2) * lda;
      a04 = a + posX + (posY +  3) * lda;
      a05 = a + posX + (posY +  4) * lda;
      a06 = a + posX + (posY +  5) * lda;
      a07 = a + posX + (posY +  6) * lda;
      a08 = a + posX + (posY +  7) * lda;
    }

    i = (m >> 3);
    if (i > 0) {
      do {
	if (X > posY) {
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
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a01 +  4);
	    b[  5] = *(a01 +  5);
	    b[  6] = *(a01 +  6);
	    b[  7] = *(a01 +  7);

	    b[  8] = ZERO;
#ifdef UNIT
	    b[ 9] = ONE;
#else
	    b[ 9] = *(a02 +  1);
#endif
	    b[ 10] = *(a02 +  2);
	    b[ 11] = *(a02 +  3);
	    b[ 12] = *(a02 +  4);
	    b[ 13] = *(a02 +  5);
	    b[ 14] = *(a02 +  6);
	    b[ 15] = *(a02 +  7);

	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
#ifdef UNIT
	    b[ 18] = ONE;
#else
	    b[ 18] = *(a03 +  2);
#endif
	    b[ 19] = *(a03 +  3);
	    b[ 20] = *(a03 +  4);
	    b[ 21] = *(a03 +  5);
	    b[ 22] = *(a03 +  6);
	    b[ 23] = *(a03 +  7);

	    b[ 24] = ZERO;
	    b[ 25] = ZERO;
	    b[ 26] = ZERO;
#ifdef UNIT
	    b[ 27] = ONE;
#else
	    b[ 27] = *(a04 +  3);
#endif
	    b[ 28] = *(a04 +  4);
	    b[ 29] = *(a04 +  5);
	    b[ 30] = *(a04 +  6);
	    b[ 31] = *(a04 +  7);

	    b[ 32] = ZERO;
	    b[ 33] = ZERO;
	    b[ 34] = ZERO;
	    b[ 35] = ZERO;
#ifdef UNIT
	    b[ 36] = ONE;
#else
	    b[ 36] = *(a05 +  4);
#endif
	    b[ 37] = *(a05 +  5);
	    b[ 38] = *(a05 +  6);
	    b[ 39] = *(a05 +  7);

	    b[ 40] = ZERO;
	    b[ 41] = ZERO;
	    b[ 42] = ZERO;
	    b[ 43] = ZERO;
	    b[ 44] = ZERO;
#ifdef UNIT
	    b[ 45] = ONE;
#else
	    b[ 45] = *(a06 +  5);
#endif
	    b[ 46] = *(a06 +  6);
	    b[ 47] = *(a06 +  7);

	    b[ 48] = ZERO;
	    b[ 49] = ZERO;
	    b[ 50] = ZERO;
	    b[ 51] = ZERO;
	    b[ 52] = ZERO;
	    b[ 53] = ZERO;
#ifdef UNIT
	    b[ 54] = ONE;
#else
	    b[ 54] = *(a07 +  6);
#endif
	    b[ 55] = *(a07 +  7);

	    b[ 56] = ZERO;
	    b[ 57] = ZERO;
	    b[ 58] = ZERO;
	    b[ 59] = ZERO;
	    b[ 60] = ZERO;
	    b[ 61] = ZERO;
	    b[ 62] = ZERO;
#ifdef UNIT
	    b[ 63] = ONE;
#else
	    b[ 63] = *(a08 +  7);
#endif

	      a01 += 8;
	      a02 += 8;
	      a03 += 8;
	      a04 += 8;
	      a05 += 8;
	      a06 += 8;
	      a07 += 8;
	      a08 += 8;
	      b += 64;

	  }

	  X += 8;
	  i --;
	} while (i > 0);
      }

    i = (m & 7);
    if (i > 0) {
      if (X > posY) {
	  /* a01 += i;
	  a02 += i;
	  a03 += i;
	  a04 += i;
	  a05 += i;
	  a06 += i;
	  a07 += i;
	  a08 += i; */
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
	    a05 += lda;
	    a06 += lda;
	    a07 += lda;
	    a08 += lda;
	    b += 8;
	  }
	} else {
#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a01 +  4);
	    b[  5] = *(a01 +  5);
	    b[  6] = *(a01 +  6);
	    b[  7] = *(a01 +  7);
	    b += 8;

	    if (i >= 2) {
	      b[ 0] = ZERO;
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = *(a02 +  1);
#endif
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
	      b[ 4] = *(a02 +  4);
	      b[ 5] = *(a02 +  5);
	      b[ 6] = *(a02 +  6);
	      b[ 7] = *(a02 +  7);
	      b += 8;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = *(a03 +  2);
#endif
	      b[ 3] = *(a03 +  3);
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
	      b[ 6] = *(a03 +  6);
	      b[ 7] = *(a03 +  7);
	      b += 8;
	    }

	    if (i >= 4) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
#ifdef UNIT
	      b[ 3] = ONE;
#else
	      b[ 3] = *(a04 +  3);
#endif
	      b[ 4] = *(a04 +  4);
	      b[ 5] = *(a04 +  5);
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
	      b += 8;
	    }

	    if (i >= 5) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
#ifdef UNIT
	      b[ 4] = ONE;
#else
	      b[ 4] = *(a05 +  4);
#endif
	      b[ 5] = *(a05 +  5);
	      b[ 6] = *(a05 +  6);
	      b[ 7] = *(a05 +  7);
	      b += 8;
	    }

	    if (i >= 6) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
#ifdef UNIT
	      b[ 5] = ONE;
#else
	      b[ 5] = *(a06 +  5);
#endif
	      b[ 6] = *(a06 +  6);
	      b[ 7] = *(a06 +  7);
	      b += 8;
	    }

	    if (i >= 7) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
#ifdef UNIT
	      b[ 6] = ONE;
#else
	      b[ 6] = *(a07 +  6);
#endif
	      b[ 7] = *(a07 +  7);
	      b += 8;
	    }
	}
    }
    posY += 8;
  }

  if (n & 4){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
      a03 = a + posY + (posX +  2) * lda;
      a04 = a + posY + (posX +  3) * lda;
    } else {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
      a03 = a + posX + (posY +  2) * lda;
      a04 = a + posX + (posY +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 4;
	  a02 += 4;
	  a03 += 4;
	  a04 += 4;
	  b += 16;
	} else
	  if (X < posY) {

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
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);

	    b[  4] = ZERO;
#ifdef UNIT
	    b[  5] = ONE;
#else
	    b[  5] = *(a02 +  1);
#endif
	    b[  6] = *(a02 +  2);
	    b[  7] = *(a02 +  3);

	    b[  8] = ZERO;
	    b[  9] = ZERO;
#ifdef UNIT
	    b[ 10] = ONE;
#else
	    b[ 10] = *(a03 +  2);
#endif
	    b[ 11] = *(a03 +  3);

	    b[ 12] = ZERO;
	    b[ 13] = ZERO;
	    b[ 14] = ZERO;
#ifdef UNIT
	    b[ 15] = ONE;
#else
	    b[ 15] = *(a04 +  3);
#endif

	      a01 += 4;
	      a02 += 4;
	      a03 += 4;
	      a04 += 4;
	      b += 16;
	  }

	X += 4;
	i --;
      } while (i > 0);
    }

    i = (m & 3);
    if (i > 0) {
      if (X > posY) {
	  /* a01 += i;
	  a02 += i;
	  a03 += i;
	  a04 += i; */
	  b += 4 * i;
      } else
	if (X < posY) {

	  for (ii = 0; ii < i; ii++){

	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);

	    a01 += lda;
	    a02 += lda;
	    a03 += lda;
	    a04 += lda;
	    b += 4;
	  }
	} else {

#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b += 4;

	    if (i >= 2) {
	      b[ 0] = ZERO;
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = *(a02 +  1);
#endif
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
	      b += 4;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = *(a03 +  2);
#endif
	      b[ 3] = *(a03 +  3);
	      b += 4;
	    }
	}
    }
    posY += 4;
  }

  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY + (posX +  0) * lda;
      a02 = a + posY + (posX +  1) * lda;
    } else {
      a01 = a + posX + (posY +  0) * lda;
      a02 = a + posX + (posY +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 2;
	  a02 += 2;
	  b += 4;
	} else
	  if (X < posY) {
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
	    b[  1] = *(a01 +  1);

	    b[  2] = ZERO;
#ifdef UNIT
	    b[  3] = ONE;
#else
	    b[  3] = *(a02 +  1);
#endif

	      a01 += 2;
	      a02 += 2;
	      b += 4;
	  }

	X += 2;
	i --;
      } while (i > 0);
    }

    if (m & 1) {
      if (X > posY) {
	a01 ++;
	a02 ++;
	b += 2;
      } else
	if (X < posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  /* a01 += lda;
	  a02 += lda; */
	  b += 2;
	  }
    } else {
#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    b[  1] = *(a01 +  1);
      b += 2;
    }
    posY += 2;
  }

  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY + (posX +  0) * lda;
    } else {
      a01 = a + posX + (posY +  0) * lda;
    }

    i = m;
    if (i > 0) {
      do {

	if (X > posY) {
	  b ++;
	  a01 ++;
	} else
	  if (X < posY) {
	    b[  0] = *(a01 + 0);
	    a01 += lda;
	    b ++;
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
#else
	    b[  0] = *(a01 +  0);
#endif
	    a01 ++;
	    b ++;
	  }
	X += 1;
	i --;
      } while (i > 0);
    }
    // posY += 1;
  }

  return 0;
}
