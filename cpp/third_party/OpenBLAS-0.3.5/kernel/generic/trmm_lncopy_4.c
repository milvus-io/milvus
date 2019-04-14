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
  FLOAT *ao1, *ao2, *ao3, *ao4;

  js = (n >> 2);

  if (js > 0){
    do {
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
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);

	    data05 = *(ao2 + 0);
	    data06 = *(ao2 + 1);
	    data07 = *(ao2 + 2);
	    data08 = *(ao2 + 3);

	    data09 = *(ao3 + 0);
	    data10 = *(ao3 + 1);
	    data11 = *(ao3 + 2);
	    data12 = *(ao3 + 3);

	    data13 = *(ao4 + 0);
	    data14 = *(ao4 + 1);
	    data15 = *(ao4 + 2);
	    data16 = *(ao4 + 3);

	    b[ 0] = data01;
	    b[ 1] = data05;
	    b[ 2] = data09;
	    b[ 3] = data13;
	    b[ 4] = data02;
	    b[ 5] = data06;
	    b[ 6] = data10;
	    b[ 7] = data14;

	    b[ 8] = data03;
	    b[ 9] = data07;
	    b[10] = data11;
	    b[11] = data15;
	    b[12] = data04;
	    b[13] = data08;
	    b[14] = data12;
	    b[15] = data16;

	    ao1 += 4;
	    ao2 += 4;
	    ao3 += 4;
	    ao4 += 4;
	    b += 16;

	  } else
	    if (X < posY) {
	      ao1 += 4 * lda;
	      ao2 += 4 * lda;
	      ao3 += 4 * lda;
	      ao4 += 4 * lda;
	      b += 16;

	    } else {
#ifdef UNIT
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data07 = *(ao2 + 2);
	      data08 = *(ao2 + 3);

	      data12 = *(ao3 + 3);

	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = data02;
	      b[ 5] = ONE;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;

	      b[ 8] = data03;
	      b[ 9] = data07;
	      b[10] = ONE;
	      b[11] = ZERO;
	      b[12] = data04;
	      b[13] = data08;
	      b[14] = data12;
	      b[15] = ONE;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      data06 = *(ao2 + 1);
	      data07 = *(ao2 + 2);
	      data08 = *(ao2 + 3);

	      data11 = *(ao3 + 2);
	      data12 = *(ao3 + 3);

	      data16 = *(ao4 + 3);

	      b[ 0] = data01;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = data02;
	      b[ 5] = data06;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;

	      b[ 8] = data03;
	      b[ 9] = data07;
	      b[10] = data11;
	      b[11] = ZERO;
	      b[12] = data04;
	      b[13] = data08;
	      b[14] = data12;
	      b[15] = data16;
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
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data03 = *(ao2 + 0);
	    data04 = *(ao2 + 1);
	    data05 = *(ao3 + 0);
	    data06 = *(ao3 + 1);
	    data07 = *(ao4 + 0);
	    data08 = *(ao4 + 1);

	    b[ 0] = data01;
	    b[ 1] = data03;
	    b[ 2] = data05;
	    b[ 3] = data07;
	    b[ 4] = data02;
	    b[ 5] = data04;
	    b[ 6] = data06;
	    b[ 7] = data08;

	    ao1 += 2;
	    ao2 += 2;
	    ao3 += 2;
	    ao4 += 2;
	    b += 8;
	  }

	  if (m & 1) {
	    data01 = *(ao1 + 0);
	    data02 = *(ao2 + 0);
	    data03 = *(ao3 + 0);
	    data04 = *(ao4 + 0);

	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data03;
	    b[ 3] = data04;

	    /* ao1 += 1;
	    ao2 += 1;
	    ao3 += 1;
	    ao4 += 1; */
	    b += 4;
	  }

	} else
	  if (X < posY) {
	    if (m & 2) {
	      /* ao1 += 2 * lda;
	      ao2 += 2 * lda; */

	      b += 8;
	    }

	    if (m & 1) {
	      // ao1 += lda;
	      b += 4;
	    }

	  } else {
#ifdef UNIT
	    data05 = *(ao2 + 0);
	    data09 = *(ao3 + 0);
	    data13 = *(ao4 + 0);

	    if (i >= 2) {
	      data10 = *(ao3 + 1);
	      data14 = *(ao4 + 1);
	    }

	    if (i >= 3) {
	      data15 = *(ao4 + 2);
	    }

	    b[ 0] = ONE;
	    b[ 1] = data05;
	    b[ 2] = data09;
	    b[ 3] = data13;
	    b += 4;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ONE;
	      b[ 2] = data10;
	      b[ 3] = data14;
	      b += 4;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ONE;
	      b[ 3] = data15;
	      b += 4;
	    }
#else
	    data01 = *(ao1 + 0);
	    data05 = *(ao2 + 0);
	    data09 = *(ao3 + 0);
	    data13 = *(ao4 + 0);

	    if (i >= 2) {
	      data06 = *(ao2 + 1);
	      data10 = *(ao3 + 1);
	      data14 = *(ao4 + 1);
	    }

	    if (i >= 3) {
	      data11 = *(ao3 + 2);
	      data15 = *(ao4 + 2);
	    }

	    b[ 0] = data01;
	    b[ 1] = data05;
	    b[ 2] = data09;
	    b[ 3] = data13;
	    b += 4;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = data06;
	      b[ 2] = data10;
	      b[ 3] = data14;
	      b += 4;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = data11;
	      b[ 3] = data15;
	      b += 4;
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
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data05 = *(ao2 + 0);
	    data06 = *(ao2 + 1);

	    b[ 0] = data01;
	    b[ 1] = data05;
	    b[ 2] = data02;
	    b[ 3] = data06;

	    ao1 += 2;
	    ao2 += 2;
	    b += 4;

	  } else
	    if (X < posY) {
	      ao1 += 2 * lda;
	      ao2 += 2 * lda;
	      b += 4;
	    } else {
#ifdef UNIT
	      data02 = *(ao1 + 1);

	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = data02;
	      b[ 3] = ONE;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data06 = *(ao2 + 1);

	      b[ 0] = data01;
	      b[ 1] = ZERO;
	      b[ 2] = data02;
	      b[ 3] = data06;
#endif
	      ao1 += 2;
	      ao2 += 2;

	      b += 4;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      i = (m & 1);
      if (i) {

	if (X > posY) {
	  data01 = *(ao1 + 0);
	  data02 = *(ao2 + 0);
	  b[ 0] = data01;
	  b[ 1] = data02;

	  /* ao1 += 1;
	  ao2 += 1; */
	  b += 2;
	} else
	  if (X < posY) {
	    // ao1 += lda;
	    b += 2;
	  } else {
#ifdef UNIT
	    data05 = *(ao2 + 0);

	    b[ 0] = ONE;
	    b[ 1] = data05;
#else
	    data01 = *(ao1 + 0);
	    data05 = *(ao2 + 0);

	    b[ 0] = data01;
	    b[ 1] = data05;
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
      if (i > 0) {
	do {
	  if (X > posY) {
	    data01 = *(ao1 + 0);
	    b[ 0] = data01;
	    b += 1;
	    ao1 += 1;
	  } else
	    if (X < posY) {
	      b += 1;
	      ao1 += lda;
	    } else {
#ifdef UNIT
	    b[ 0] = ONE;
#else
	    data01 = *(ao1 + 0);
	    b[ 0] = data01;
#endif
	    b += 1;
	    ao1 += 1;
	    }

	  X ++;
	  i --;
	} while (i > 0);
      }

      // posY += 1;
  }

  return 0;
}
