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

#ifndef USE_ALPHA
#define REAL_PART(a, b)  (a)
#define IMAGE_PART(a, b) (b)
#else
#define REAL_PART(a, b)  (alpha_r * (a) + alpha_i * (b))
#define IMAGE_PART(a, b) (alpha_i * (a) - alpha_r * (b))
#endif

#if defined(REAL_ONLY)
#define CMULT(a, b) (REAL_PART(a, b))
#elif defined(IMAGE_ONLY)
#define CMULT(a, b) (IMAGE_PART(a, b))
#else
#define CMULT(a, b) (REAL_PART(a, b) + IMAGE_PART(a, b))
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY,
#ifdef USE_ALPHA
	   FLOAT alpha_r, FLOAT alpha_i,
#endif
	   FLOAT *b){

  BLASLONG i, js, offset;

  FLOAT data01, data02, data03, data04;
  FLOAT *ao1, *ao2, *ao3, *ao4;

  lda *= 2;

  js = (n >> 2);
  while (js > 0){
    offset = posX - posY;

    if (offset >  0) ao1 = a + (posX + 0) * 2 + posY * lda; else ao1 = a + posY * 2 + (posX + 0) * lda;
    if (offset > -1) ao2 = a + (posX + 1) * 2 + posY * lda; else ao2 = a + posY * 2 + (posX + 1) * lda;
    if (offset > -2) ao3 = a + (posX + 2) * 2 + posY * lda; else ao3 = a + posY * 2 + (posX + 2) * lda;
    if (offset > -3) ao4 = a + (posX + 3) * 2 + posY * lda; else ao4 = a + posY * 2 + (posX + 3) * lda;

    i     = m;

    while (i > 0) {
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
      } else
	if (offset < -3) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	} else {
	  switch (offset) {
	  case  0 :
	    data01 = CMULT(*(ao1 + 0), ZERO);
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    break;
	  case -1 :
	    data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), ZERO);
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    break;
	  case -2 :
	    data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), ZERO);
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    break;
	  case -3 :
	    data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), ZERO);
	    break;
	  }
	}

      if (offset >   0) ao1 += lda; else ao1 += 2;
      if (offset >  -1) ao2 += lda; else ao2 += 2;
      if (offset >  -2) ao3 += lda; else ao3 += 2;
      if (offset >  -3) ao4 += lda; else ao4 += 2;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;

      b += 4;

      offset --;
      i --;
    }

    posX += 4;
    js --;
  }

  if (n & 2) {
    offset = posX - posY;

    if (offset >  0) ao1 = a + (posX + 0) * 2 + posY * lda; else ao1 = a + posY * 2 + (posX + 0) * lda;
    if (offset > -1) ao2 = a + (posX + 1) * 2 + posY * lda; else ao2 = a + posY * 2 + (posX + 1) * lda;

    i     = m;

    while (i > 0) {
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
      } else
	if (offset < -1) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	} else {
	  switch (offset) {
	  case  0 :
	    data01 = CMULT(*(ao1 + 0), ZERO);
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    break;
	  case -1 :
	    data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), ZERO);
	    break;
	  }
	}

      if (offset >   0) ao1 += lda; else ao1 += 2;
      if (offset >  -1) ao2 += lda; else ao2 += 2;

      b[ 0] = data01;
      b[ 1] = data02;

      b += 2;

      offset --;
      i --;
    }

    posX += 2;
  }

  if (n & 1) {

    offset = posX - posY;

    if (offset > 0) ao1 = a + (posX + 0) * 2 + posY * lda; else ao1 = a + posY * 2 + (posX + 0) * lda;

    i     = m;

    while (i > 0) {
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
      } else
	if (offset < 0) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	} else {
	    data01 = CMULT(*(ao1 + 0), ZERO);
	}

      if (offset >   0) ao1 += lda; else ao1 += 2;

      b[ 0] = data01;

      b ++;

      offset --;
      i --;
    }
  }

  return 0;
}
