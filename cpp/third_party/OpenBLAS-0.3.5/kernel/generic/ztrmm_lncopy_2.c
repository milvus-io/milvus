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

  FLOAT data01, data02, data03, data04;
  FLOAT data05, data06, data07, data08;
  FLOAT *ao1, *ao2;

  lda += lda;

  js = (n >> 1);

  if (js > 0){
    do {
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
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    data03 = *(ao1 + 2);
	    data04 = *(ao1 + 3);
	    data05 = *(ao2 + 0);
	    data06 = *(ao2 + 1);
	    data07 = *(ao2 + 2);
	    data08 = *(ao2 + 3);

	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = data05;
	    b[ 3] = data06;
	    b[ 4] = data03;
	    b[ 5] = data04;
	    b[ 6] = data07;
	    b[ 7] = data08;

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
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);

	      b[ 0] = ONE;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = data03;
	      b[ 5] = data04;
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      data01 = *(ao1 + 0);
	      data02 = *(ao1 + 1);
	      data03 = *(ao1 + 2);
	      data04 = *(ao1 + 3);
	      data07 = *(ao2 + 2);
	      data08 = *(ao2 + 3);

	      b[ 0] = data01;
	      b[ 1] = data02;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = data03;
	      b[ 5] = data04;
	      b[ 6] = data07;
	      b[ 7] = data08;
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
	  data01 = *(ao1 + 0);
	  data02 = *(ao1 + 1);
	  data03 = *(ao2 + 0);
	  data04 = *(ao2 + 1);

	  b[ 0] = data01;
	  b[ 1] = data02;
	  b[ 2] = data03;
	  b[ 3] = data04;

	  /* ao1 += 2;
	  ao2 += 2; */
	  b += 4;
	} else
	  if (X < posY) {
	    // ao1 += lda;
	    b += 4;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
	    b[ 0] = ZERO;
	    b[ 1] = ZERO;
#else
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b[ 2] = ZERO;
	    b[ 3] = ZERO;
#endif

	    b += 4;
	  }
      }

      posY += 2;
      js --;
    } while (js > 0);
  } /* End of main loop */


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
	    data01 = *(ao1 + 0);
	    data02 = *(ao1 + 1);
	    b[ 0] = data01;
	    b[ 1] = data02;
	    b += 2;
	    ao1 += 2;
	  } else
	    if (X < posY) {
	      b += 2;
	      ao1 += lda;
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
	      ao1 += 2;
	    }

	  X ++;
	  i --;
	} while (i > 0);
      }

      // posY += 1;
  }
  return 0;
}
