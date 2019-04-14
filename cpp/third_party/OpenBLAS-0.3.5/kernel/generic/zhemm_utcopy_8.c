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

  BLASLONG i, js, offset;

  FLOAT data01, data02, data03, data04, data05, data06, data07, data08;
  FLOAT data09, data10, data11, data12, data13, data14, data15, data16;
  FLOAT *ao1, *ao2,  *ao3,  *ao4,  *ao5,  *ao6,  *ao7,  *ao8;

  lda *= 2;

  js = (n >> 3);
  while (js > 0){

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;
    if (offset > -2) ao3 = a + posY * 2 + (posX + 2) * lda; else ao3 = a + (posX + 2) * 2 + posY * lda;
    if (offset > -3) ao4 = a + posY * 2 + (posX + 3) * lda; else ao4 = a + (posX + 3) * 2 + posY * lda;
    if (offset > -4) ao5 = a + posY * 2 + (posX + 4) * lda; else ao5 = a + (posX + 4) * 2 + posY * lda;
    if (offset > -5) ao6 = a + posY * 2 + (posX + 5) * lda; else ao6 = a + (posX + 5) * 2 + posY * lda;
    if (offset > -6) ao7 = a + posY * 2 + (posX + 6) * lda; else ao7 = a + (posX + 6) * 2 + posY * lda;
    if (offset > -7) ao8 = a + posY * 2 + (posX + 7) * lda; else ao8 = a + (posX + 7) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);
      data05 = *(ao3 + 0);
      data06 = *(ao3 + 1);
      data07 = *(ao4 + 0);
      data08 = *(ao4 + 1);
      data09 = *(ao5 + 0);
      data10 = *(ao5 + 1);
      data11 = *(ao6 + 0);
      data12 = *(ao6 + 1);
      data13 = *(ao7 + 0);
      data14 = *(ao7 + 1);
      data15 = *(ao8 + 0);
      data16 = *(ao8 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;
      if (offset >  -4) ao5 += 2; else ao5 += lda;
      if (offset >  -5) ao6 += 2; else ao6 += lda;
      if (offset >  -6) ao7 += 2; else ao7 += lda;
      if (offset >  -7) ao8 += 2; else ao8 += lda;

      if (offset > 0) {
	b[ 0] = data01;
	b[ 1] = -data02;
	b[ 2] = data03;
	b[ 3] = -data04;
	b[ 4] = data05;
	b[ 5] = -data06;
	b[ 6] = data07;
	b[ 7] = -data08;
	b[ 8] = data09;
	b[ 9] = -data10;
	b[10] = data11;
	b[11] = -data12;
	b[12] = data13;
	b[13] = -data14;
	b[14] = data15;
	b[15] = -data16;
      } else
	if (offset < -7) {
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
	} else {
	  switch (offset) {
	  case  0 :
	    b[ 0] = data01;
	    b[ 1] = ZERO;
	    b[ 2] = data03;
	    b[ 3] = -data04;
	    b[ 4] = data05;
	    b[ 5] = -data06;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    b[ 8] = data09;
	    b[ 9] = -data10;
	    b[10] = data11;
	    b[11] = -data12;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -1 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = ZERO;
	    b[ 4] = data05;
	    b[ 5] = -data06;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    b[ 8] = data09;
	    b[ 9] = -data10;
	    b[10] = data11;
	    b[11] = -data12;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -2 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = ZERO;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    b[ 8] = data09;
	    b[ 9] = -data10;
	    b[10] = data11;
	    b[11] = -data12;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -3 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = data06;
	    b[ 6] = data07;
	    b[ 7] = ZERO;
	    b[ 8] = data09;
	    b[ 9] = -data10;
	    b[10] = data11;
	    b[11] = -data12;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -4 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = data06;
	    b[ 6] = data07;
	    b[ 7] = data08;
	    b[ 8] = data09;
	    b[ 9] = ZERO;
	    b[10] = data11;
	    b[11] = -data12;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -5 :
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
	    b[11] = ZERO;
	    b[12] = data13;
	    b[13] = -data14;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -6 :
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
	    b[13] = ZERO;
	    b[14] = data15;
	    b[15] = -data16;
	    break;
	  case -7 :
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
	    b[15] = ZERO;
	    break;
	  }
	}

      b += 16;

      offset --;
      i --;
    }

    posX += 8;
    js --;
  }

  if (n & 4) {
    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;
    if (offset > -2) ao3 = a + posY * 2 + (posX + 2) * lda; else ao3 = a + (posX + 2) * 2 + posY * lda;
    if (offset > -3) ao4 = a + posY * 2 + (posX + 3) * lda; else ao4 = a + (posX + 3) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);
      data05 = *(ao3 + 0);
      data06 = *(ao3 + 1);
      data07 = *(ao4 + 0);
      data08 = *(ao4 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;

      if (offset > 0) {
	b[ 0] = data01;
	b[ 1] = -data02;
	b[ 2] = data03;
	b[ 3] = -data04;
	b[ 4] = data05;
	b[ 5] = -data06;
	b[ 6] = data07;
	b[ 7] = -data08;
      } else
	if (offset < -3) {
	  b[ 0] = data01;
	  b[ 1] = data02;
	  b[ 2] = data03;
	  b[ 3] = data04;
	  b[ 4] = data05;
	  b[ 5] = data06;
	  b[ 6] = data07;
	  b[ 7] = data08;
	} else {
	  switch (offset) {
	  case  0 :
	    b[ 0] = data01;
	    b[ 1] = ZERO;
	    b[ 2] = data03;
	    b[ 3] = -data04;
	    b[ 4] = data05;
	    b[ 5] = -data06;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    break;
	  case -1 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = ZERO;
	    b[ 4] = data05;
	    b[ 5] = -data06;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    break;
	  case -2 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = ZERO;
	    b[ 6] = data07;
	    b[ 7] = -data08;
	    break;
	  case -3 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
	    b[ 4] = data05;
	    b[ 5] = data06;
	    b[ 6] = data07;
	    b[ 7] = ZERO;
	    break;
	  }
	}

      b += 8;

      offset --;
      i --;
    }

    posX += 4;
  }

  if (n & 2) {

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;

      if (offset > 0) {
	b[ 0] = data01;
	b[ 1] = -data02;
	b[ 2] = data03;
	b[ 3] = -data04;
      } else
	if (offset < -1) {
	  b[ 0] = data01;
	  b[ 1] = data02;
	  b[ 2] = data03;
	  b[ 3] = data04;
	} else {
	  switch (offset) {
	  case  0 :
	    b[ 0] = data01;
	    b[ 1] = ZERO;
	    b[ 2] = data03;
	    b[ 3] = -data04;
	    break;
	  case -1 :
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = ZERO;
	    break;
	  }
	}

      b += 4;

      offset --;
      i --;
    }

    posX += 2;
  }

  if (n & 1) {

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;

      if (offset > 0) {
	b[ 0] = data01;
	b[ 1] = -data02;
      } else
	if (offset < 0) {
	  b[ 0] = data01;
	  b[ 1] = data02;
	} else {
	    b[ 0] = data01;
	    b[ 1] = ZERO;
	}

      b += 2;

      offset --;
      i --;
    }

  }

  return 0;
}
