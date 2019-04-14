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
	    for (ii = 0; ii < 16; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);

	      b[  8] = *(a09 +  0);
	      b[  9] = *(a10 +  0);
	      b[ 10] = *(a11 +  0);
	      b[ 11] = *(a12 +  0);
	      b[ 12] = *(a13 +  0);
	      b[ 13] = *(a14 +  0);
	      b[ 14] = *(a15 +  0);
	      b[ 15] = *(a16 + 0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      a05 ++;
	      a06 ++;
	      a07 ++;
	      a08 ++;
	      a09 ++;
	      a10 ++;
	      a11 ++;
	      a12 ++;
	      a13 ++;
	      a14 ++;
	      a15 ++;
	      a16 ++;
	      b += 16;
	    }
	  } else
	    if (X > posY) {
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
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);
	      b[  8] = *(a09 +  0);
	      b[  9] = *(a10 +  0);
	      b[ 10] = *(a11 +  0);
	      b[ 11] = *(a12 +  0);
	      b[ 12] = *(a13 +  0);
	      b[ 13] = *(a14 +  0);
	      b[ 14] = *(a15 +  0);
	      b[ 15] = *(a16 +  0);

	      b[ 16] = ZERO;
#ifdef UNIT
	      b[ 17] = ONE;
#else
	      b[ 17] = *(a02 +  1);
#endif
	      b[ 18] = *(a03 +  1);
	      b[ 19] = *(a04 +  1);
	      b[ 20] = *(a05 +  1);
	      b[ 21] = *(a06 +  1);
	      b[ 22] = *(a07 +  1);
	      b[ 23] = *(a08 +  1);
	      b[ 24] = *(a09 +  1);
	      b[ 25] = *(a10 +  1);
	      b[ 26] = *(a11 +  1);
	      b[ 27] = *(a12 +  1);
	      b[ 28] = *(a13 +  1);
	      b[ 29] = *(a14 +  1);
	      b[ 30] = *(a15 +  1);
	      b[ 31] = *(a16 +  1);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
#ifdef UNIT
	      b[ 34] = ONE;
#else
	      b[ 34] = *(a03 +  2);
#endif
	      b[ 35] = *(a04 +  2);
	      b[ 36] = *(a05 +  2);
	      b[ 37] = *(a06 +  2);
	      b[ 38] = *(a07 +  2);
	      b[ 39] = *(a08 +  2);
	      b[ 40] = *(a09 +  2);
	      b[ 41] = *(a10 +  2);
	      b[ 42] = *(a11 +  2);
	      b[ 43] = *(a12 +  2);
	      b[ 44] = *(a13 +  2);
	      b[ 45] = *(a14 +  2);
	      b[ 46] = *(a15 +  2);
	      b[ 47] = *(a16 +  2);

	      b[ 48] = ZERO;
	      b[ 49] = ZERO;
	      b[ 50] = ZERO;
#ifdef UNIT
	      b[ 51] = ONE;
#else
	      b[ 51] = *(a04 +  3);
#endif
	      b[ 52] = *(a05 +  3);
	      b[ 53] = *(a06 +  3);
	      b[ 54] = *(a07 +  3);
	      b[ 55] = *(a08 +  3);
	      b[ 56] = *(a09 +  3);
	      b[ 57] = *(a10 +  3);
	      b[ 58] = *(a11 +  3);
	      b[ 59] = *(a12 +  3);
	      b[ 60] = *(a13 +  3);
	      b[ 61] = *(a14 +  3);
	      b[ 62] = *(a15 +  3);
	      b[ 63] = *(a16 +  3);

	      b[ 64] = ZERO;
	      b[ 65] = ZERO;
	      b[ 66] = ZERO;
	      b[ 67] = ZERO;
#ifdef UNIT
	      b[ 68] = ONE;
#else
	      b[ 68] = *(a05 +  4);
#endif
	      b[ 69] = *(a06 +  4);
	      b[ 70] = *(a07 +  4);
	      b[ 71] = *(a08 +  4);
	      b[ 72] = *(a09 +  4);
	      b[ 73] = *(a10 +  4);
	      b[ 74] = *(a11 +  4);
	      b[ 75] = *(a12 +  4);
	      b[ 76] = *(a13 +  4);
	      b[ 77] = *(a14 +  4);
	      b[ 78] = *(a15 +  4);
	      b[ 79] = *(a16 +  4);

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
	      b[ 86] = *(a07 +  5);
	      b[ 87] = *(a08 +  5);
	      b[ 88] = *(a09 +  5);
	      b[ 89] = *(a10 +  5);
	      b[ 90] = *(a11 +  5);
	      b[ 91] = *(a12 +  5);
	      b[ 92] = *(a13 +  5);
	      b[ 93] = *(a14 +  5);
	      b[ 94] = *(a15 +  5);
	      b[ 95] = *(a16 +  5);

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
	      b[103] = *(a08  + 6);
	      b[104] = *(a09  + 6);
	      b[105] = *(a10  + 6);
	      b[106] = *(a11  + 6);
	      b[107] = *(a12  + 6);
	      b[108] = *(a13  + 6);
	      b[109] = *(a14  + 6);
	      b[110] = *(a15  + 6);
	      b[111] = *(a16  + 6);

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
	      b[119] = *(a08 +   7);
#endif
	      b[120] = *(a09 +  7);
	      b[121] = *(a10 +  7);
	      b[122] = *(a11 +  7);
	      b[123] = *(a12 +  7);
	      b[124] = *(a13 +  7);
	      b[125] = *(a14 +  7);
	      b[126] = *(a15 +  7);
	      b[127] = *(a16 +  7);

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
	      b[137] = *(a10 +  8);
	      b[138] = *(a11 +  8);
	      b[139] = *(a12 +  8);
	      b[140] = *(a13 +  8);
	      b[141] = *(a14 +  8);
	      b[142] = *(a15 +  8);
	      b[143] = *(a16 +  8);

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
	      b[154] = *(a11 +  9);
	      b[155] = *(a12 +  9);
	      b[156] = *(a13 +  9);
	      b[157] = *(a14 +  9);
	      b[158] = *(a15 +  9);
	      b[159] = *(a16 +  9);

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
	      b[171] = *(a12 + 10);
	      b[172] = *(a13 + 10);
	      b[173] = *(a14 + 10);
	      b[174] = *(a15 + 10);
	      b[175] = *(a16 + 10);

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
	      b[188] = *(a13 + 11);
	      b[189] = *(a14 + 11);
	      b[190] = *(a15 + 11);
	      b[191] = *(a16 + 11);

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
	      b[205] = *(a14 + 12);
	      b[206] = *(a15 + 12);
	      b[207] = *(a16 + 12);

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
	      b[222] = *(a15 + 13);
	      b[223] = *(a16 + 13);

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
	      b[239] = *(a16 + 14);

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
      if (i) {

	if (X < posY) {
	    for (ii = 0; ii < i; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);

	      b[  8] = *(a09 +  0);
	      b[  9] = *(a10 +  0);
	      b[ 10] = *(a11 +  0);
	      b[ 11] = *(a12 +  0);
	      b[ 12] = *(a13 +  0);
	      b[ 13] = *(a14 +  0);
	      b[ 14] = *(a15 +  0);
	      b[ 15] = *(a16 + 0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      a05 ++;
	      a06 ++;
	      a07 ++;
	      a08 ++;
	      a09 ++;
	      a10 ++;
	      a11 ++;
	      a12 ++;
	      a13 ++;
	      a14 ++;
	      a15 ++;
	      a16 ++;
	      b += 16;
	    }
	} else
	  if (X > posY) {
	      /* a01 += i * lda;
	      a02 += i * lda;
	      a03 += i * lda;
	      a04 += i * lda;
	      a05 += i * lda;
	      a06 += i * lda;
	      a07 += i * lda;
	      a08 += i * lda;
	      a09 += i * lda;
	      a10 += i * lda;
	      a11 += i * lda;
	      a12 += i * lda;
	      a13 += i * lda;
	      a14 += i * lda;
	      a15 += i * lda;
	      a16 += i * lda; */
	      b += 16 * i;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);
	      b[  8] = *(a09 +  0);
	      b[  9] = *(a10 +  0);
	      b[ 10] = *(a11 +  0);
	      b[ 11] = *(a12 +  0);
	      b[ 12] = *(a13 +  0);
	      b[ 13] = *(a14 +  0);
	      b[ 14] = *(a15 +  0);
	      b[ 15] = *(a16 +  0);
	      b += 16;

	      if (i >= 2) {
		b[  0] = ZERO;
#ifdef UNIT
		b[  1] = ONE;
#else
		b[  1] = *(a02 +  1);
#endif
		b[  2] = *(a03 +  1);
		b[  3] = *(a04 +  1);
		b[  4] = *(a05 +  1);
		b[  5] = *(a06 +  1);
		b[  6] = *(a07 +  1);
		b[  7] = *(a08 +  1);
		b[  8] = *(a09 +  1);
		b[  9] = *(a10 +  1);
		b[ 10] = *(a11 +  1);
		b[ 11] = *(a12 +  1);
		b[ 12] = *(a13 +  1);
		b[ 13] = *(a14 +  1);
		b[ 14] = *(a15 +  1);
		b[ 15] = *(a16 +  1);
		b += 16;
	      }

	      if (i >= 3) {
		b[  0] = ZERO;
		b[  1] = ZERO;
#ifdef UNIT
		b[  2] = ONE;
#else
		b[  2] = *(a03 +  2);
#endif
		b[  3] = *(a04 +  2);
		b[  4] = *(a05 +  2);
		b[  5] = *(a06 +  2);
		b[  6] = *(a07 +  2);
		b[  7] = *(a08 +  2);
		b[  8] = *(a09 +  2);
		b[  9] = *(a10 +  2);
		b[ 10] = *(a11 +  2);
		b[ 11] = *(a12 +  2);
		b[ 12] = *(a13 +  2);
		b[ 13] = *(a14 +  2);
		b[ 14] = *(a15 +  2);
		b[ 15] = *(a16 +  2);
		b += 16;
	      }

	      if (i >= 4) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
#ifdef UNIT
		b[  3] = ONE;
#else
		b[  3] = *(a04 +  3);
#endif
		b[  4] = *(a05 +  3);
		b[  5] = *(a06 +  3);
		b[  6] = *(a07 +  3);
		b[  7] = *(a08 +  3);
		b[  8] = *(a09 +  3);
		b[  9] = *(a10 +  3);
		b[ 10] = *(a11 +  3);
		b[ 11] = *(a12 +  3);
		b[ 12] = *(a13 +  3);
		b[ 13] = *(a14 +  3);
		b[ 14] = *(a15 +  3);
		b[ 15] = *(a16 +  3);
		b += 16;
	      }

	      if (i >= 5) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
#ifdef UNIT
		b[  4] = ONE;
#else
		b[  4] = *(a05 +  4);
#endif
		b[  5] = *(a06 +  4);
		b[  6] = *(a07 +  4);
		b[  7] = *(a08 +  4);
		b[  8] = *(a09 +  4);
		b[  9] = *(a10 +  4);
		b[ 10] = *(a11 +  4);
		b[ 11] = *(a12 +  4);
		b[ 12] = *(a13 +  4);
		b[ 13] = *(a14 +  4);
		b[ 14] = *(a15 +  4);
		b[ 15] = *(a16 +  4);
		b += 16;
	      }

	      if (i >= 6) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
#ifdef UNIT
		b[  5] = ONE;
#else
		b[  5] = *(a06 +  5);
#endif
		b[  6] = *(a07 +  5);
		b[  7] = *(a08 +  5);
		b[  8] = *(a09 +  5);
		b[  9] = *(a10 +  5);
		b[ 10] = *(a11 +  5);
		b[ 11] = *(a12 +  5);
		b[ 12] = *(a13 +  5);
		b[ 13] = *(a14 +  5);
		b[ 14] = *(a15 +  5);
		b[ 15] = *(a16 +  5);
		b += 16;
	      }

	      if (i >= 7) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
#ifdef UNIT
		b[  6] = ONE;
#else
		b[  6] = *(a07 +  6);
#endif
		b[  7] = *(a08  + 6);
		b[  8] = *(a09  + 6);
		b[  9] = *(a10  + 6);
		b[ 10] = *(a11  + 6);
		b[ 11] = *(a12  + 6);
		b[ 12] = *(a13  + 6);
		b[ 13] = *(a14  + 6);
		b[ 14] = *(a15  + 6);
		b[ 15] = *(a16  + 6);
		b += 16;
	      }

	      if (i >= 8) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
		b[  6] = ZERO;
#ifdef UNIT
		b[  7] = ONE;
#else
		b[  7] = *(a08 +   7);
#endif
		b[  8] = *(a09 +  7);
		b[  9] = *(a10 +  7);
		b[ 10] = *(a11 +  7);
		b[ 11] = *(a12 +  7);
		b[ 12] = *(a13 +  7);
		b[ 13] = *(a14 +  7);
		b[ 14] = *(a15 +  7);
		b[ 15] = *(a16 +  7);
		b += 16;
	      }

	      if (i >= 9) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
		b[  6] = ZERO;
		b[  7] = ZERO;
#ifdef UNIT
		b[  8] = ONE;
#else
		b[  8] = *(a09 +  8);
#endif
		b[  9] = *(a10 +  8);
		b[ 10] = *(a11 +  8);
		b[ 11] = *(a12 +  8);
		b[ 12] = *(a13 +  8);
		b[ 13] = *(a14 +  8);
		b[ 14] = *(a15 +  8);
		b[ 15] = *(a16 +  8);
		b += 16;
	      }

	      if (i >= 10) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
		b[  6] = ZERO;
		b[  7] = ZERO;
		b[  8] = ZERO;
#ifdef UNIT
		b[  9] = ONE;
#else
		b[  9] = *(a10 +  9);
#endif
		b[ 10] = *(a11 +  9);
		b[ 11] = *(a12 +  9);
		b[ 12] = *(a13 +  9);
		b[ 13] = *(a14 +  9);
		b[ 14] = *(a15 +  9);
		b[ 15] = *(a16 +  9);
		b += 16;
	      }

	      if (i >= 11) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
		b[  6] = ZERO;
		b[  7] = ZERO;
		b[  8] = ZERO;
		b[  9] = ZERO;
#ifdef UNIT
		b[ 10] = ONE;
#else
		b[ 10] = *(a11 + 10);
#endif
		b[ 11] = *(a12 + 10);
		b[ 12] = *(a13 + 10);
		b[ 13] = *(a14 + 10);
		b[ 14] = *(a15 + 10);
		b[ 15] = *(a16 + 10);
		b += 16;
	      }

	      if (i >= 12) {
		b[  0] = ZERO;
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
#ifdef UNIT
		b[ 11] = ONE;
#else
		b[ 11] = *(a12 + 11);
#endif
		b[ 12] = *(a13 + 11);
		b[ 13] = *(a14 + 11);
		b[ 14] = *(a15 + 11);
		b[ 15] = *(a16 + 11);
		b += 16;
	      }

	      if (i >= 13) {
		b[  0] = ZERO;
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
#ifdef UNIT
		b[ 12] = ONE;
#else
		b[ 12] = *(a13 + 12);
#endif
		b[ 13] = *(a14 + 12);
		b[ 14] = *(a15 + 12);
		b[ 15] = *(a16 + 12);
		b += 16;
	      }

	      if (i >= 14) {
		b[  0] = ZERO;
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
#ifdef UNIT
		b[ 13] = ONE;
#else
		b[ 13] = *(a14 + 13);
#endif
		b[ 14] = *(a15 + 13);
		b[ 15] = *(a16 + 13);
		b += 16;
	      }

	      if (i >= 15) {
		b[  0] = ZERO;
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
#ifdef UNIT
		b[ 14] = ONE;
#else
		b[ 14] = *(a15 + 14);
#endif
		b[ 15] = *(a16 + 14);
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
	    for (ii = 0; ii < 8; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      a05 ++;
	      a06 ++;
	      a07 ++;
	      a08 ++;
	      b += 8;
	    }
	  } else
	    if (X > posY) {
	      a01 += 8 * lda;
	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;
	      b += 64;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);

	      b[  8] = ZERO;
#ifdef UNIT
	      b[  9] = ONE;
#else
	      b[  9] = *(a02 +  1);
#endif
	      b[ 10] = *(a03 +  1);
	      b[ 11] = *(a04 +  1);
	      b[ 12] = *(a05 +  1);
	      b[ 13] = *(a06 +  1);
	      b[ 14] = *(a07 +  1);
	      b[ 15] = *(a08 +  1);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
#else
	      b[ 18] = *(a03 +  2);
#endif
	      b[ 19] = *(a04 +  2);
	      b[ 20] = *(a05 +  2);
	      b[ 21] = *(a06 +  2);
	      b[ 22] = *(a07 +  2);
	      b[ 23] = *(a08 +  2);

	      b[ 24] = ZERO;
	      b[ 25] = ZERO;
	      b[ 26] = ZERO;
#ifdef UNIT
	      b[ 27] = ONE;
#else
	      b[ 27] = *(a04 +  3);
#endif
	      b[ 28] = *(a05 +  3);
	      b[ 29] = *(a06 +  3);
	      b[ 30] = *(a07 +  3);
	      b[ 31] = *(a08 +  3);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
	      b[ 34] = ZERO;
	      b[ 35] = ZERO;
#ifdef UNIT
	      b[ 36] = ONE;
#else
	      b[ 36] = *(a05 +  4);
#endif
	      b[ 37] = *(a06 +  4);
	      b[ 38] = *(a07 +  4);
	      b[ 39] = *(a08 +  4);

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
	      b[ 46] = *(a07 +  5);
	      b[ 47] = *(a08 +  5);

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
	      b[ 55] = *(a08  + 6);

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
	      b[ 63] = *(a08 +   7);
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
      if (i) {

	if (X < posY) {
	    for (ii = 0; ii < i; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      a05 ++;
	      a06 ++;
	      a07 ++;
	      a08 ++;
	      b += 8;
	    }
	} else
	  if (X > posY) {
	      /* a01 += i * lda;
	      a02 += i * lda;
	      a03 += i * lda;
	      a04 += i * lda;
	      a05 += i * lda;
	      a06 += i * lda;
	      a07 += i * lda;
	      a08 += i * lda; */
	      b += 8 * i;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b[  4] = *(a05 +  0);
	      b[  5] = *(a06 +  0);
	      b[  6] = *(a07 +  0);
	      b[  7] = *(a08 +  0);
	      b += 8;

	      if (i >= 2) {
		b[  0] = ZERO;
#ifdef UNIT
		b[  1] = ONE;
#else
		b[  1] = *(a02 +  1);
#endif
		b[  2] = *(a03 +  1);
		b[  3] = *(a04 +  1);
		b[  4] = *(a05 +  1);
		b[  5] = *(a06 +  1);
		b[  6] = *(a07 +  1);
		b[  7] = *(a08 +  1);
		b += 8;
	      }

	      if (i >= 3) {
		b[  0] = ZERO;
		b[  1] = ZERO;
#ifdef UNIT
		b[  2] = ONE;
#else
		b[  2] = *(a03 +  2);
#endif
		b[  3] = *(a04 +  2);
		b[  4] = *(a05 +  2);
		b[  5] = *(a06 +  2);
		b[  6] = *(a07 +  2);
		b[  7] = *(a08 +  2);
		b += 8;
	      }

	      if (i >= 4) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
#ifdef UNIT
		b[  3] = ONE;
#else
		b[  3] = *(a04 +  3);
#endif
		b[  4] = *(a05 +  3);
		b[  5] = *(a06 +  3);
		b[  6] = *(a07 +  3);
		b[  7] = *(a08 +  3);
		b += 8;
	      }

	      if (i >= 5) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
#ifdef UNIT
		b[  4] = ONE;
#else
		b[  4] = *(a05 +  4);
#endif
		b[  5] = *(a06 +  4);
		b[  6] = *(a07 +  4);
		b[  7] = *(a08 +  4);
		b += 8;
	      }

	      if (i >= 6) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
#ifdef UNIT
		b[  5] = ONE;
#else
		b[  5] = *(a06 +  5);
#endif
		b[  6] = *(a07 +  5);
		b[  7] = *(a08 +  5);
		b += 8;
	      }

	      if (i >= 7) {
		b[  0] = ZERO;
		b[  1] = ZERO;
		b[  2] = ZERO;
		b[  3] = ZERO;
		b[  4] = ZERO;
		b[  5] = ZERO;
#ifdef UNIT
		b[  6] = ONE;
#else
		b[  6] = *(a07 +  6);
#endif
		b[  7] = *(a08  + 6);
		b += 8;
	      }
	  }
      }

      posY += 8;
  }

  if (n & 4){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX + (posY + 0) * lda;
	a02 = a + posX + (posY + 1) * lda;
	a03 = a + posX + (posY + 2) * lda;
	a04 = a + posX + (posY + 3) * lda;
      } else {
	a01 = a + posY + (posX + 0) * lda;
	a02 = a + posY + (posX + 1) * lda;
	a03 = a + posY + (posX + 2) * lda;
	a04 = a + posY + (posX + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X < posY) {
	    for (ii = 0; ii < 4; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      b += 4;
	    }
	  } else
	    if (X > posY) {
	      a01 += 4 * lda;
	      a02 += 4 * lda;
	      a03 += 4 * lda;
	      a04 += 4 * lda;
	      b += 16;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);

	      b[  4] = ZERO;
#ifdef UNIT
	      b[  5] = ONE;
#else
	      b[  5] = *(a02 +  1);
#endif
	      b[  6] = *(a03 +  1);
	      b[  7] = *(a04 +  1);

	      b[  8] = ZERO;
	      b[  9] = ZERO;
#ifdef UNIT
	      b[ 10] = ONE;
#else
	      b[ 10] = *(a03 +  2);
#endif
	      b[ 11] = *(a04 +  2);

	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
	      b[ 14] = ZERO;
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
      if (i) {

	if (X < posY) {
	    for (ii = 0; ii < i; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);

	      a01 ++;
	      a02 ++;
	      a03 ++;
	      a04 ++;
	      b += 4;
	    }
	} else
	  if (X > posY) {
	      /* a01 += i * lda;
	      a02 += i * lda;
	      a03 += i * lda;
	      a04 += i * lda; */
	      b += 4 * i;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b[  2] = *(a03 +  0);
	      b[  3] = *(a04 +  0);
	      b += 4;

	      if (i >= 2) {
		b[  0] = ZERO;
#ifdef UNIT
		b[  1] = ONE;
#else
		b[  1] = *(a02 +  1);
#endif
		b[  2] = *(a03 +  1);
		b[  3] = *(a04 +  1);
		b += 4;
	      }

	      if (i >= 3) {
		b[  0] = ZERO;
		b[  1] = ZERO;
#ifdef UNIT
		b[  2] = ONE;
#else
		b[  2] = *(a03 +  2);
#endif
		b[  3] = *(a04 +  2);
		b += 4;
	      }
	  }
      }

      posY += 4;
  }

  if (n & 2){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX + (posY + 0) * lda;
	a02 = a + posX + (posY + 1) * lda;
      } else {
	a01 = a + posY + (posX + 0) * lda;
	a02 = a + posY + (posX + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X < posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a02 +  0);
	    b[  2] = *(a01 +  1);
	    b[  3] = *(a02 +  1);

	    a01 += 2;
	    a02 += 2;
	    b += 4;
	  } else
	    if (X > posY) {
	      a01 += 2 * lda;
	      a02 += 2 * lda;
	      b += 4;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);

	      b[  2] = ZERO;
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
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a02 +  0);

	  a01 ++;
	  a02 ++;
	  b += 2;
	} else
	  if (X > posY) {
	    /* a01 += lda;
	    a02 += lda; */
	    b += 2;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b[  1] = *(a02 +  0);
	      b += 2;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX + (posY + 0) * lda;
      } else {
	a01 = a + posY + (posX + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X < posY) {
	    b[  0] = *(a01 +  0);
	    a01 += 1;
	    b += 1;
	  } else
	    if (X > posY) {
	      a01 += lda;
	      b += 1;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
#else
	      b[  0] = *(a01 +  0);
#endif
	      b += 1;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
