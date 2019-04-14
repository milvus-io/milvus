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

  FLOAT data01, data02, data03, data04, data05, data06, data07, data08;
  FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6, *ao7, *ao8;

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
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	data05 = CMULT(*(ao5 + 0), -*(ao5 + 1));
	data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
      } else
	if (offset < -7) {
	  data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	  data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	  data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	  data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	  data05 = CMULT(*(ao5 + 0), *(ao5 + 1));
	  data06 = CMULT(*(ao6 + 0), *(ao6 + 1));
	  data07 = CMULT(*(ao7 + 0), *(ao7 + 1));
	  data08 = CMULT(*(ao8 + 0), *(ao8 + 1));
	} else {
	  switch (offset) {
	  case  0 :
	    data01 = CMULT(*(ao1 + 0), ZERO);
	    data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), -*(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -1 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), ZERO);
	    data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), -*(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -2 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), ZERO);
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), -*(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -3 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), ZERO);
	    data05 = CMULT(*(ao5 + 0), -*(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -4 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), ZERO);
	    data06 = CMULT(*(ao6 + 0), -*(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -5 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), *(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), ZERO);
	    data07 = CMULT(*(ao7 + 0), -*(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -6 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), *(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), *(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), ZERO);
	    data08 = CMULT(*(ao8 + 0), -*(ao8 + 1));
	    break;
	  case -7 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	    data05 = CMULT(*(ao5 + 0), *(ao5 + 1));
	    data06 = CMULT(*(ao6 + 0), *(ao6 + 1));
	    data07 = CMULT(*(ao7 + 0), *(ao7 + 1));
	    data08 = CMULT(*(ao8 + 0), ZERO);
	    break;
	  }
	}

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;
      if (offset >  -4) ao5 += 2; else ao5 += lda;
      if (offset >  -5) ao6 += 2; else ao6 += lda;
      if (offset >  -6) ao7 += 2; else ao7 += lda;
      if (offset >  -7) ao8 += 2; else ao8 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;
      b[ 4] = data05;
      b[ 5] = data06;
      b[ 6] = data07;
      b[ 7] = data08;

      b += 8;

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
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
      } else
	if (offset < -3) {
	  data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	  data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	  data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	  data04 = CMULT(*(ao4 + 0), *(ao4 + 1));
	} else {
	  switch (offset) {
	  case  0 :
	    data01 = CMULT(*(ao1 + 0), ZERO);
	    data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    break;
	  case -1 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), ZERO);
	    data03 = CMULT(*(ao3 + 0), -*(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    break;
	  case -2 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), ZERO);
	    data04 = CMULT(*(ao4 + 0), -*(ao4 + 1));
	    break;
	  case -3 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	    data03 = CMULT(*(ao3 + 0), *(ao3 + 1));
	    data04 = CMULT(*(ao4 + 0), ZERO);
	    break;
	  }
	}

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;

      b += 4;

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
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
	data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
      } else
	if (offset < -1) {
	  data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	  data02 = CMULT(*(ao2 + 0), *(ao2 + 1));
	} else {
	  switch (offset) {
	  case  0 :
	    data01 = CMULT(*(ao1 + 0), ZERO);
	    data02 = CMULT(*(ao2 + 0), -*(ao2 + 1));
	    break;
	  case -1 :
	    data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	    data02 = CMULT(*(ao2 + 0), ZERO);
	    break;
	  }
	}

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;

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

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      if (offset > 0) {
	data01 = CMULT(*(ao1 + 0), -*(ao1 + 1));
      } else
	if (offset < 0) {
	  data01 = CMULT(*(ao1 + 0), *(ao1 + 1));
	} else {
	    data01 = CMULT(*(ao1 + 0), ZERO);
	}

      if (offset >   0) ao1 += 2; else ao1 += lda;

      b[ 0] = data01;

      b ++;

      offset --;
      i --;
    }
  }

  return 0;
}
