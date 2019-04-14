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
  FLOAT *ao1, *ao2, *ao3, *ao4;

  lda += lda;

  js = (n >> 2);

  if (js > 0){
    do {
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
	    ao1 += 8;
	    ao2 += 8;
	    ao3 += 8;
	    ao4 += 8;
	    b += 32;

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
	      ao3 += 4 * lda;
	      ao4 += 4 * lda;
	      b += 32;
	    } else {

#ifdef UNIT
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);


	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;

	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ONE;
	      b[11] = ZERO;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      b[16] = ZERO;
	      b[17] = ZERO;
	      b[18] = ZERO;
	      b[19] = ZERO;
	      b[20] = ONE;
	      b[21] = ZERO;
	      b[22] = data23;
	      b[23] = data24;

	      b[24] = ZERO;
	      b[25] = ZERO;
	      b[26] = ZERO;
	      b[27] = ZERO;
	      b[28] = ZERO;
	      b[29] = ZERO;
	      b[30] = ONE;
	      b[31] = ZERO;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);
	      data13 = *(ao2 + 4);
	      data14 = *(ao2 + 5);
	      data15 = *(ao2 + 6);
	      data16 = *(ao2 + 7);

	      data21 = *(ao3 + 4);
	      data22 = *(ao3 + 5);
	      data23 = *(ao3 + 6);
	      data24 = *(ao3 + 7);

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

	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = data11;
	      b[11] = data12;
	      b[12] = data13;
	      b[13] = data14;
	      b[14] = data15;
	      b[15] = data16;

	      b[16] = ZERO;
	      b[17] = ZERO;
	      b[18] = ZERO;
	      b[19] = ZERO;
	      b[20] = data21;
	      b[21] = data22;
	      b[22] = data23;
	      b[23] = data24;

	      b[24] = ZERO;
	      b[25] = ZERO;
	      b[26] = ZERO;
	      b[27] = ZERO;
	      b[28] = ZERO;
	      b[29] = ZERO;
	      b[30] = data31;
	      b[31] = data32;
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

	  if (m & 2) {
	    /* ao1 += 4;
	    ao2 += 4;
	    ao3 += 4;
	    ao4 += 4; */
	    b += 16;
	  }

	  if (m & 1) {
	    /* ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2; */
	    b += 8;
	  }

	} else
	  if (X < posY) {
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
	      // ao2 += 2 * lda;

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

	      // ao1 += lda;
	      b += 8;
	    }

	  } else {
#ifdef UNIT
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      if (i >= 2) {
		data13 = *(ao2 + 4);
		data14 = *(ao2 + 5);
		data15 = *(ao2 + 6);
		data16 = *(ao2 + 7);
	      }

	      if (i >= 3) {
		data23 = *(ao3 + 6);
		data24 = *(ao3 + 7);
	      }

	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;
	      b += 8;

	      if (i >= 2) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ONE;
		b[ 3] = ZERO;
		b[ 4] = data13;
		b[ 5] = data14;
		b[ 6] = data15;
		b[ 7] = data16;
		b += 8;
	      }

	      if (i >= 3) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ONE;
		b[ 5] = ZERO;
		b[ 6] = data23;
		b[ 7] = data24;
		b += 8;
	      }
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data05 = *(ao1 + 4);
	      data06 = *(ao1 + 5);
	      data07 = *(ao1 + 6);
	      data08 = *(ao1 + 7);

	      if (i >= 2) {
		data11 = *(ao2 + 2);
		data12 = *(ao2 + 3);
		data13 = *(ao2 + 4);
		data14 = *(ao2 + 5);
		data15 = *(ao2 + 6);
		data16 = *(ao2 + 7);
	      }

	      if (i >= 3) {
		data21 = *(ao3 + 4);
		data22 = *(ao3 + 5);
		data23 = *(ao3 + 6);
		data24 = *(ao3 + 7);
	      }

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data05;
	      b[ 5] = data06;
	      b[ 6] = data07;
	      b[ 7] = data08;
	      b += 8;

	      if (i >= 2) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
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
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = data21;
		b[ 5] = data22;
		b[ 6] = data23;
		b[ 7] = data24;
		b += 8;
	      }
#endif
	  }
     }

      posY += 4;
      js --;
    } while (js > 0);
  } /* End of main loop */


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
	    ao1 += 4;
	    ao2 += 4;
	    b += 8;

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

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = data09;
	      b[ 5] = data10;
	      b[ 6] = data11;
	      b[ 7] = data12;

	      ao1 += 2 * lda;
	      ao2 += 2 * lda;
	      b += 8;

	    } else {
#ifdef UNIT
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data11 = *(ao2 + 2);
	      data12 = *(ao2 + 3);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = data03;
	      b[ 3] = data04;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = data11;
	      b[ 7] = data12;
#endif
	      ao1 += 4;
	      ao2 += 4;

	      b += 8;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      i = (m & 1);
      if (i) {

	if (X > posY) {
	  /* ao1 += 2;
	  ao2 += 2; */

	  b += 4;
	} else
	  if (X < posY) {
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);

	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;

	    // ao1 += lda;
	    b += 4;

	  } else {
#ifdef UNIT
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);

	    b[ 0] = ONE;
	    b[ 1] = ZERO;
	    b[ 2] = data03;
	    b[ 3] = data04;
#else
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);

	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;
#endif
	    b += 2;
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
      if (i > 0) {
	do {

	  if (X > posY) {
	    b += 2;
	    ao1 += 2;
	  } else
	    if (X < posY) {
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      ao1 += lda;
	      b += 2;
	    } else {

#ifdef UNIT
	      b[ 0] = ONE;
	      b[ 1] = ZERO;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);

	      b[ 0] = data01;
	      b[ 1] = data02;
#endif
	      b += 2;
	    }

	  X ++;
	  i --;
	} while (i > 0);
      }

      // posY += 1;
  }

  return 0;
}
