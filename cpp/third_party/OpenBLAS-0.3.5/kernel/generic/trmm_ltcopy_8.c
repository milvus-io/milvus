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
  BLASLONG X;

  FLOAT data01, data02, data03, data04, data05, data06, data07, data08;
  FLOAT data09, data10, data11, data12, data13, data14, data15, data16;
  FLOAT data17, data18, data19, data20, data21, data22, data23, data24;
  FLOAT data25, data26, data27, data28, data29, data30, data31, data32;
  FLOAT data33, data34, data35, data36, data37, data38, data39, data40;
  FLOAT data41, data42, data43, data44, data45, data46, data47, data48;
  FLOAT data49, data50, data51, data52, data53, data54, data55, data56;
  FLOAT data57, data58, data59, data60, data61, data62, data63, data64;

  FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6, *ao7, *ao8;

  js = (n >> 3);

  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	ao1 = a + posY + (posX + 0) * lda;
	ao2 = a + posY + (posX + 1) * lda;
	ao3 = a + posY + (posX + 2) * lda;
	ao4 = a + posY + (posX + 3) * lda;
	ao5 = a + posY + (posX + 4) * lda;
	ao6 = a + posY + (posX + 5) * lda;
	ao7 = a + posY + (posX + 6) * lda;
	ao8 = a + posY + (posX + 7) * lda;
      } else {
	ao1 = a + posX + (posY + 0) * lda;
	ao2 = a + posX + (posY + 1) * lda;
	ao3 = a + posX + (posY + 2) * lda;
	ao4 = a + posX + (posY + 3) * lda;
	ao5 = a + posX + (posY + 4) * lda;
	ao6 = a + posX + (posY + 5) * lda;
	ao7 = a + posX + (posY + 6) * lda;
	ao8 = a + posX + (posY + 7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X > posY) {
	    ao1 += 8;
	    ao2 += 8;
	    ao3 += 8;
	    ao4 += 8;
	    ao5 += 8;
	    ao6 += 8;
	    ao7 += 8;
	    ao8 += 8;

	    b += 64;

	  } else
	    if (X < posY) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

	      data17 = *(ao3 + 0);
	      data18 = *(ao3 + 1);
	      data19 = *(ao3 + 2);
	      data20 = *(ao3 + 3);
	      data21 = *(ao3 + 4);
	      data22 = *(ao3 + 5);
	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);

	      data25 = *(ao4 + 0);
	      data26 = *(ao4 + 1);
	      data27 = *(ao4 + 2);
	      data28 = *(ao4 + 3);
	      data29 = *(ao4 + 4);
	      data30 = *(ao4 + 5);
	      data31 = *(ao4 + 6);
	      data32 = *(ao4 + 7);

	      data33 = *(ao5 + 0);
	      data34 = *(ao5 + 1);
	      data35 = *(ao5 + 2);
	      data36 = *(ao5 + 3);
	      data37 = *(ao5 + 4);
	      data38 = *(ao5 + 5);
	      data39 = *(ao5 + 6);
	      data40 = *(ao5 + 7);

	      data41 = *(ao6 + 0);
	      data42 = *(ao6 + 1);
	      data43 = *(ao6 + 2);
	      data44 = *(ao6 + 3);
	      data45 = *(ao6 + 4);
	      data46 = *(ao6 + 5);
	      data47 = *(ao6 + 6);
	      data48 = *(ao6 + 7);

	      data49 = *(ao7 + 0);
	      data50 = *(ao7 + 1);
	      data51 = *(ao7 + 2);
	      data52 = *(ao7 + 3);
	      data53 = *(ao7 + 4);
	      data54 = *(ao7 + 5);
	      data55 = *(ao7 + 6);
	      data56 = *(ao7 + 7);

	      data57 = *(ao8 + 0);
	      data58 = *(ao8 + 1);
	      data59 = *(ao8 + 2);
	      data60 = *(ao8 + 3);
	      data61 = *(ao8 + 4);
	      data62 = *(ao8 + 5);
	      data63 = *(ao8 + 6);
	      data64 = *(ao8 + 7);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b[ 8] = data09;
	      b[ 9] = data10;
	      b[10] = data11;
	      b[11] = data12;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      b[16] = data17;
	      b[17] = data18;
	      b[18] = data19;
	      b[19] = data20;
	      b[20] = data21;
	      b[21] = data22;
	      b[22] = data23;
	      b[23] = data24;

	      b[24] = data25;
	      b[25] = data26;
	      b[26] = data27;
	      b[27] = data28;
	      b[28] = data29;
	      b[29] = data30;
	      b[30] = data31;
	      b[31] = data32;

	      b[32] = data33;
	      b[33] = data34;
	      b[34] = data35;
	      b[35] = data36;
	      b[36] = data37;
	      b[37] = data38;
	      b[38] = data39;
	      b[39] = data40;

	      b[40] = data41;
	      b[41] = data42;
	      b[42] = data43;
	      b[43] = data44;
	      b[44] = data45;
	      b[45] = data46;
	      b[46] = data47;
	      b[47] = data48;

	      b[48] = data49;
	      b[49] = data50;
	      b[50] = data51;
	      b[51] = data52;
	      b[52] = data53;
	      b[53] = data54;
	      b[54] = data55;
	      b[55] = data56;

	      b[56] = data57;
	      b[57] = data58;
	      b[58] = data59;
	      b[59] = data60;
	      b[60] = data61;
	      b[61] = data62;
	      b[62] = data63;
	      b[63] = data64;

	      ao1 += 8 * lda;
	      ao2 += 8 * lda;
	      ao3 += 8 * lda;
	      ao4 += 8 * lda;
	      ao5 += 8 * lda;
	      ao6 += 8 * lda;
	      ao7 += 8 * lda;
	      ao8 += 8 * lda;

	      b += 64;

	    } else {

#ifndef UNIT
	      data01 = *(ao1 + 0);
#endif
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

#ifndef UNIT
	      data10 = *(ao2 + 1);
#endif
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

#ifndef UNIT
	      data19 = *(ao3 + 2);
#endif
	      data20 = *(ao3 + 3);
	      data21 = *(ao3 + 4);
	      data22 = *(ao3 + 5);
	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);

#ifndef UNIT
	      data28 = *(ao4 + 3);
#endif
	      data29 = *(ao4 + 4);
	      data30 = *(ao4 + 5);
	      data31 = *(ao4 + 6);
	      data32 = *(ao4 + 7);

#ifndef UNIT
	      data37 = *(ao5 + 4);
#endif
	      data38 = *(ao5 + 5);
	      data39 = *(ao5 + 6);
	      data40 = *(ao5 + 7);

#ifndef UNIT
	      data46 = *(ao6 + 5);
#endif
	      data47 = *(ao6 + 6);
	      data48 = *(ao6 + 7);

#ifndef UNIT
	      data55 = *(ao7 + 6);
#endif
	      data56 = *(ao7 + 7);

#ifndef UNIT
	      data64 = *(ao8 + 7);
#endif


#ifdef UNIT
	      b[ 0] = ONE;
#else
	      b[ 0] = data01;
#endif
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b[ 8] = ZERO;
#ifdef UNIT
	      b[ 9] = ONE;
#else
	      b[ 9] = data10;
#endif
	      b[10] = data11;
	      b[11] = data12;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      b[16] = ZERO;
	      b[17] = ZERO;
#ifdef UNIT
	      b[18] = ONE;
#else
	      b[18] = data19;
#endif
	      b[19] = data20;
	      b[20] = data21;
	      b[21] = data22;
	      b[22] = data23;
	      b[23] = data24;

	      b[24] = ZERO;
	      b[25] = ZERO;
	      b[26] = ZERO;
#ifdef UNIT
	      b[27] = ONE;
#else
	      b[27] = data28;
#endif
	      b[28] = data29;
	      b[29] = data30;
	      b[30] = data31;
	      b[31] = data32;

	      b[32] = ZERO;
	      b[33] = ZERO;
	      b[34] = ZERO;
	      b[35] = ZERO;
#ifdef UNIT
	      b[36] = ONE;
#else
	      b[36] = data37;
#endif
	      b[37] = data38;
	      b[38] = data39;
	      b[39] = data40;

	      b[40] = ZERO;
	      b[41] = ZERO;
	      b[42] = ZERO;
	      b[43] = ZERO;
	      b[44] = ZERO;
#ifdef UNIT
	      b[45] = ONE;
#else
	      b[45] = data46;
#endif
	      b[46] = data47;
	      b[47] = data48;

	      b[48] = ZERO;
	      b[49] = ZERO;
	      b[50] = ZERO;
	      b[51] = ZERO;
	      b[52] = ZERO;
	      b[53] = ZERO;
#ifdef UNIT
	      b[54] = ONE;
#else
	      b[54] = data55;
#endif
	      b[55] = data56;

	      b[56] = ZERO;
	      b[57] = ZERO;
	      b[58] = ZERO;
	      b[59] = ZERO;
	      b[60] = ZERO;
	      b[61] = ZERO;
	      b[62] = ZERO;
#ifdef UNIT
	      b[63] = ONE;
#else
	      b[63] = data64;
#endif

	      ao1 += 8;
	      ao2 += 8;
	      ao3 += 8;
	      ao4 += 8;
	      ao5 += 8;
	      ao6 += 8;
	      ao7 += 8;
	      ao8 += 8;

	      b += 64;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X > posY) {

	  if (m & 4) {
	    /* ao1 += 4;
	    ao2 += 4;
	    ao3 += 4;
	    ao4 += 4;
	    ao5 += 4;
	    ao6 += 4;
	    ao7 += 4;
	    ao8 += 4; */

	  b += 32;
	  }

	  if (m & 2) {
	    /* ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2;
	    ao5 += 2;
	    ao6 += 2;
	    ao7 += 2;
	    ao8 += 2; */

	    b += 16;
	  }

	  if (m & 1) {
	    b += 8;
	  }
	} else
	  if (X < posY) {
	    if (m & 4) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

	      data17 = *(ao3 + 0);
	      data18 = *(ao3 + 1);
	      data19 = *(ao3 + 2);
	      data20 = *(ao3 + 3);
	      data21 = *(ao3 + 4);
	      data22 = *(ao3 + 5);
	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);

	      data25 = *(ao4 + 0);
	      data26 = *(ao4 + 1);
	      data27 = *(ao4 + 2);
	      data28 = *(ao4 + 3);
	      data29 = *(ao4 + 4);
	      data30 = *(ao4 + 5);
	      data31 = *(ao4 + 6);
	      data32 = *(ao4 + 7);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b[ 8] = data09;
	      b[ 9] = data10;
	      b[10] = data11;
	      b[11] = data12;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      b[16] = data17;
	      b[17] = data18;
	      b[18] = data19;
	      b[19] = data20;
	      b[20] = data21;
	      b[21] = data22;
	      b[22] = data23;
	      b[23] = data24;

	      b[24] = data25;
	      b[25] = data26;
	      b[26] = data27;
	      b[27] = data28;
	      b[28] = data29;
	      b[29] = data30;
	      b[30] = data31;
	      b[31] = data32;

	      ao1 += 4 * lda;
	      ao2 += 4 * lda;
	      /* ao3 += 4 * lda;
	      ao4 += 4 * lda; */

	      b += 32;
	    }

	    if (m & 2) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b[ 8] = data09;
	      b[ 9] = data10;
	      b[10] = data11;
	      b[11] = data12;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      ao1 += 2 * lda;
	      b += 16;
	    }

	    if (m & 1) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b += 8;
	    }
	  } else {
#ifndef UNIT
	    data01 = *(ao1 + 0);
#endif
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);
	    data05 = *(ao1 + 4);
	    data06 = *(ao1 + 5);
	    data07 = *(ao1 + 6);
	    data08 = *(ao1 + 7);

	    if (i >= 2) {
#ifndef UNIT
	      data10 = *(ao2 + 1);
#endif
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);
	    }

	    if (i >= 3) {
#ifndef UNIT
	      data19 = *(ao3 + 2);
#endif
	      data20 = *(ao3 + 3);
	      data21 = *(ao3 + 4);
	      data22 = *(ao3 + 5);
	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);
	    }

	    if (i >= 4) {
#ifndef UNIT
	      data28 = *(ao4 + 3);
#endif
	      data29 = *(ao4 + 4);
	      data30 = *(ao4 + 5);
	      data31 = *(ao4 + 6);
	      data32 = *(ao4 + 7);
	    }

	    if (i >= 5) {
#ifndef UNIT
	      data37 = *(ao5 + 4);
#endif
	      data38 = *(ao5 + 5);
	      data39 = *(ao5 + 6);
	      data40 = *(ao5 + 7);
	    }

	    if (i >= 6) {
#ifndef UNIT
	      data46 = *(ao6 + 5);
#endif
	      data47 = *(ao6 + 6);
	      data48 = *(ao6 + 7);
	    }

	    if (i >= 7) {
#ifndef UNIT
	      data55 = *(ao7 + 6);
#endif
	      data56 = *(ao7 + 7);
	    }

#ifdef UNIT
	    b[ 0] = ONE;
#else
	    b[ 0] = data01;
#endif
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = data06;
	    b[ 6] = data07;
	    b[ 7] = data08;
	    b += 8;

	    if(i >= 2) {
	      b[ 0] = ZERO;
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = data10;
#endif
	      b[ 2] = data11;
	      b[ 3] = data12;
	      b[ 4] = data13;
	      b[ 5] = data14;
	      b[ 6] = data15;
	      b[ 7] = data16;
	      b += 8;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = data19;
#endif
	      b[ 3] = data20;
	      b[ 4] = data21;
	      b[ 5] = data22;
	      b[ 6] = data23;
	      b[ 7] = data24;
	      b += 8;
	    }

	    if (i >= 4) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
#ifdef UNIT
	      b[ 3] = ONE;
#else
	      b[ 3] = data28;
#endif
	      b[ 4] = data29;
	      b[ 5] = data30;
	      b[ 6] = data31;
	      b[ 7] = data32;
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
	      b[ 4] = data37;
#endif
	      b[ 5] = data38;
	      b[ 6] = data39;
	      b[ 7] = data40;
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
	      b[ 5] = data46;
#endif
	      b[ 6] = data47;
	      b[ 7] = data48;
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
	      b[ 6] = data55;
#endif
	      b[ 7] = data56;
	      b += 8;
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
	ao1 = a + posY + (posX + 0) * lda;
	ao2 = a + posY + (posX + 1) * lda;
	ao3 = a + posY + (posX + 2) * lda;
	ao4 = a + posY + (posX + 3) * lda;
      } else {
	ao1 = a + posX + (posY + 0) * lda;
	ao2 = a + posX + (posY + 1) * lda;
	ao3 = a + posX + (posY + 2) * lda;
	ao4 = a + posX + (posY + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X > posY) {
	    ao1 += 4;
	    ao2 += 4;
	    ao3 += 4;
	    ao4 += 4;

	    b += 16;

	  } else
	    if (X < posY) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	      data17 = *(ao3 + 0);
	      data18 = *(ao3 + 1);
	      data19 = *(ao3 + 2);
	      data20 = *(ao3 + 3);

	      data25 = *(ao4 + 0);
	      data26 = *(ao4 + 1);
	      data27 = *(ao4 + 2);
	      data28 = *(ao4 + 3);

	      ao1 += 4 * lda;
	      ao2 += 4 * lda;
	      ao3 += 4 * lda;
	      ao4 += 4 * lda;

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;

	      b[ 4] = data09;
	      b[ 5] = data10;
	      b[ 6] = data11;
	      b[ 7] = data12;

	      b[ 8] = data17;
	      b[ 9] = data18;
	      b[10] = data19;
	      b[11] = data20;

	      b[12] = data25;
	      b[13] = data26;
	      b[14] = data27;
	      b[15] = data28;

	      b += 16;

	    } else {

#ifdef UNIT
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	      data20 = *(ao3 + 3);

	      b[ 0] = ONE;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;

	      b[ 4] = ZERO;
	      b[ 5] = ONE;
	      b[ 6] = data11;
	      b[ 7] = data12;

	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ONE;
	      b[11] = data20;

	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ONE;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	      data19 = *(ao3 + 2);
	      data20 = *(ao3 + 3);

	      data28 = *(ao4 + 3);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;

	      b[ 4] = ZERO;
	      b[ 5] = data10;
	      b[ 6] = data11;
	      b[ 7] = data12;

	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = data19;
	      b[11] = data20;

	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = data28;
#endif

	      ao1 += 4;
	      ao2 += 4;
	      ao3 += 4;
	      ao4 += 4;

	      b += 16;
	    }

	  X += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);
      if (i) {

	if (X > posY) {

	  if (m & 2) {
	    /* ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2; */

	    b += 8;
	  }

	  if (m & 1) {
	    b += 4;
	  }
	} else
	  if (X < posY) {
	    if (m & 2) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data09;
	      b[ 5] = data10;
	      b[ 6] = data11;
	      b[ 7] = data12;

	      ao1 += 2 * lda;
	      b += 8;
	    }

	    if (m & 1) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;

	      b += 4;
	    }
	  } else {

#ifndef UNIT
	    data01 = *(ao1 + 0);
#endif
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);

	    if (i >= 2) {
#ifndef UNIT
	      data10 = *(ao2 + 1);
#endif
	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	    }

	    if (i >= 3) {
#ifndef UNIT
	      data19 = *(ao3 + 2);
#endif
	      data20 = *(ao3 + 3);
	    }

#ifdef UNIT
	    b[ 0] = ONE;
#else
	    b[ 0] = data01;
#endif
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b += 4;

	    if(i >= 2) {
	      b[ 0] = ZERO;
#ifdef UNIT
	      b[ 1] = ONE;
#else
	      b[ 1] = data10;
#endif
	      b[ 2] = data11;
	      b[ 3] = data12;
	      b += 4;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
#else
	      b[ 2] = data19;
#endif
	      b[ 3] = data20;
	      b += 4;
	    }
	  }
      }

      posY += 4;
  }

  if (n & 2){
      X = posX;

      if (posX <= posY) {
	ao1 = a + posY + (posX + 0) * lda;
	ao2 = a + posY + (posX + 1) * lda;
      } else {
	ao1 = a + posX + (posY + 0) * lda;
	ao2 = a + posX + (posY + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X > posY) {
	    ao1 += 2;
	    ao2 += 2;
	    b += 4;

	  } else
	    if (X < posY) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data09 = *(ao2 + 0);
	      data10 = *(ao2 + 1);

	      ao1 += 2 * lda;
	      ao2 += 2 * lda;

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data09;
	      b[ 3] = data10;

	      b += 4;

	    } else {

#ifdef UNIT
	      data02 = *(ao1 + 1);

	      b[ 0] = ONE;
	      b[ 1] = data02;
	      b[ 2] = ZERO;
	      b[ 3] = ONE;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);

	      data10 = *(ao2 + 1);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = ZERO;
	      b[ 3] = data10;
#endif

	      ao1 += 2;
	      ao2 += 2;

	      b += 4;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      if (m & 1) {

	if (X > posY) {
	  b += 2;
	} else
	  if (X < posY) {
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);

	    b[ 0] = data01;
	    b[ 1] = data02;
	    b += 2;
	  } else {
#ifdef UNIT
	    data09 = *(ao2 + 0);
	    b[ 0] = ONE;
	    b[ 1] = data09;
#else
	    data01 = *(ao1 + 0);
	    data09 = *(ao2 + 0);
	    b[ 0] = data01;
	    b[ 1] = data09;
#endif
	    b += 2;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	ao1 = a + posY + (posX + 0) * lda;
      } else {
	ao1 = a + posX + (posY + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X > posY) {
	    ao1 += 1;
	    b += 1;
	  } else
	    if (X < posY) {
	      data01 = *(ao1 + 0);
	      ao1 += lda;

	      b[ 0] = data01;
	      b += 1;

	    } else {
#ifdef UNIT
	      b[ 0] = ONE;
#else
	      data01 = *(ao1 + 0);
	      b[ 0] = data01;
#endif
	      ao1 ++;
	      b ++;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
