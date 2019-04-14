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
#define REAL_PART(a, b)  (alpha_r * (a) - alpha_i * (b))
#define IMAGE_PART(a, b) (alpha_i * (a) + alpha_r * (b))
#endif

#if defined(REAL_ONLY)
#define CMULT(a, b) (REAL_PART(a, b))
#elif defined(IMAGE_ONLY)
#define CMULT(a, b) (IMAGE_PART(a, b))
#else
#define CMULT(a, b) (REAL_PART(a, b) + IMAGE_PART(a, b))
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda,
#ifdef USE_ALPHA
	   FLOAT alpha_r, FLOAT alpha_i,
#endif
	   FLOAT *b){

  BLASLONG i, j;

  FLOAT *a_offset, *a_offset1, *a_offset2, *a_offset3, *a_offset4;
  FLOAT *b_offset, *b_offset1, *b_offset2, *b_offset3;
  FLOAT a1, a2, a3, a4, a5, a6, a7, a8;

/* silence compiler warnings about unused-but-set variables:
   depending on compile-time arguments either the odd or the
   even-numbered variables will not be used */
   
  (void)a1;
  (void)a2;
  (void)a3;
  (void)a4;
  (void)a5;
  (void)a6;
  (void)a7;
  (void)a8;

  a_offset   = a;
  b_offset   = b;

  lda *= 2;

  b_offset2  = b + m  * (n & ~3);
  b_offset3  = b + m  * (n & ~1);

  j = (m >> 2);
  if (j > 0){
    do{
      a_offset1  = a_offset;
      a_offset2  = a_offset1 + lda;
      a_offset3  = a_offset2 + lda;
      a_offset4  = a_offset3 + lda;
      a_offset  += 4 * lda;

      b_offset1  = b_offset;
      b_offset  += 16;

      i = (n >> 2);
      if (i > 0){
	do{

	  a1 = *(a_offset1 + 0);
	  a2 = *(a_offset1 + 1);
	  a3 = *(a_offset1 + 2);
	  a4 = *(a_offset1 + 3);
	  a5 = *(a_offset1 + 4);
	  a6 = *(a_offset1 + 5);
	  a7 = *(a_offset1 + 6);
	  a8 = *(a_offset1 + 7);

	  *(b_offset1 +  0) = CMULT(a1, a2);
	  *(b_offset1 +  1) = CMULT(a3, a4);
	  *(b_offset1 +  2) = CMULT(a5, a6);
	  *(b_offset1 +  3) = CMULT(a7, a8);

	  a1 = *(a_offset2 + 0);
	  a2 = *(a_offset2 + 1);
	  a3 = *(a_offset2 + 2);
	  a4 = *(a_offset2 + 3);
	  a5 = *(a_offset2 + 4);
	  a6 = *(a_offset2 + 5);
	  a7 = *(a_offset2 + 6);
	  a8 = *(a_offset2 + 7);

	  *(b_offset1 +  4) = CMULT(a1, a2);
	  *(b_offset1 +  5) = CMULT(a3, a4);
	  *(b_offset1 +  6) = CMULT(a5, a6);
	  *(b_offset1 +  7) = CMULT(a7, a8);

	  a1 = *(a_offset3 + 0);
	  a2 = *(a_offset3 + 1);
	  a3 = *(a_offset3 + 2);
	  a4 = *(a_offset3 + 3);
	  a5 = *(a_offset3 + 4);
	  a6 = *(a_offset3 + 5);
	  a7 = *(a_offset3 + 6);
	  a8 = *(a_offset3 + 7);

	  *(b_offset1 +  8) = CMULT(a1, a2);
	  *(b_offset1 +  9) = CMULT(a3, a4);
	  *(b_offset1 + 10) = CMULT(a5, a6);
	  *(b_offset1 + 11) = CMULT(a7, a8);

	  a1 = *(a_offset4 + 0);
	  a2 = *(a_offset4 + 1);
	  a3 = *(a_offset4 + 2);
	  a4 = *(a_offset4 + 3);
	  a5 = *(a_offset4 + 4);
	  a6 = *(a_offset4 + 5);
	  a7 = *(a_offset4 + 6);
	  a8 = *(a_offset4 + 7);

	  *(b_offset1 + 12) = CMULT(a1, a2);
	  *(b_offset1 + 13) = CMULT(a3, a4);
	  *(b_offset1 + 14) = CMULT(a5, a6);
	  *(b_offset1 + 15) = CMULT(a7, a8);

	  a_offset1 += 8;
	  a_offset2 += 8;
	  a_offset3 += 8;
	  a_offset4 += 8;

	  b_offset1 += m * 4;
	  i --;
	}while(i > 0);
      }

      if (n & 2) {

	a1 = *(a_offset1 + 0);
	a2 = *(a_offset1 + 1);
	a3 = *(a_offset1 + 2);
	a4 = *(a_offset1 + 3);
	a5 = *(a_offset2 + 0);
	a6 = *(a_offset2 + 1);
	a7 = *(a_offset2 + 2);
	a8 = *(a_offset2 + 3);

	*(b_offset2 +  0) = CMULT(a1, a2);
	*(b_offset2 +  1) = CMULT(a3, a4);
	*(b_offset2 +  2) = CMULT(a5, a6);
	*(b_offset2 +  3) = CMULT(a7, a8);

	a1 = *(a_offset3 + 0);
	a2 = *(a_offset3 + 1);
	a3 = *(a_offset3 + 2);
	a4 = *(a_offset3 + 3);
	a5 = *(a_offset4 + 0);
	a6 = *(a_offset4 + 1);
	a7 = *(a_offset4 + 2);
	a8 = *(a_offset4 + 3);

	*(b_offset2 +  4) = CMULT(a1, a2);
	*(b_offset2 +  5) = CMULT(a3, a4);
	*(b_offset2 +  6) = CMULT(a5, a6);
	*(b_offset2 +  7) = CMULT(a7, a8);

	a_offset1 += 4;
	a_offset2 += 4;
	a_offset3 += 4;
	a_offset4 += 4;

	b_offset2 += 8;
      }

      if (n & 1) {

	a1 = *(a_offset1 + 0);
	a2 = *(a_offset1 + 1);
	a3 = *(a_offset2 + 0);
	a4 = *(a_offset2 + 1);
	a5 = *(a_offset3 + 0);
	a6 = *(a_offset3 + 1);
	a7 = *(a_offset4 + 0);
	a8 = *(a_offset4 + 1);

	*(b_offset3 +  0) = CMULT(a1, a2);
	*(b_offset3 +  1) = CMULT(a3, a4);
	*(b_offset3 +  2) = CMULT(a5, a6);
	*(b_offset3 +  3) = CMULT(a7, a8);

	b_offset3 += 4;
      }

      j--;
    }while(j > 0);
  }

  if (m & 2){
    a_offset1  = a_offset;
    a_offset2  = a_offset1 + lda;
    a_offset  += 2 * lda;

    b_offset1  = b_offset;
    b_offset  += 8;

    i = (n >> 2);
    if (i > 0){
      do{

	a1 = *(a_offset1 + 0);
	a2 = *(a_offset1 + 1);
	a3 = *(a_offset1 + 2);
	a4 = *(a_offset1 + 3);
	a5 = *(a_offset1 + 4);
	a6 = *(a_offset1 + 5);
	a7 = *(a_offset1 + 6);
	a8 = *(a_offset1 + 7);

	*(b_offset1 +  0) = CMULT(a1, a2);
	*(b_offset1 +  1) = CMULT(a3, a4);
	*(b_offset1 +  2) = CMULT(a5, a6);
	*(b_offset1 +  3) = CMULT(a7, a8);

	a1 = *(a_offset2 + 0);
	a2 = *(a_offset2 + 1);
	a3 = *(a_offset2 + 2);
	a4 = *(a_offset2 + 3);
	a5 = *(a_offset2 + 4);
	a6 = *(a_offset2 + 5);
	a7 = *(a_offset2 + 6);
	a8 = *(a_offset2 + 7);

	*(b_offset1 +  4) = CMULT(a1, a2);
	*(b_offset1 +  5) = CMULT(a3, a4);
	*(b_offset1 +  6) = CMULT(a5, a6);
	*(b_offset1 +  7) = CMULT(a7, a8);

	a_offset1 += 8;
	a_offset2 += 8;

	b_offset1 += m * 4;
	i --;
      }while(i > 0);
    }

    if (n & 2) {
      a1 = *(a_offset1 + 0);
      a2 = *(a_offset1 + 1);
      a3 = *(a_offset1 + 2);
      a4 = *(a_offset1 + 3);
      a5 = *(a_offset2 + 0);
      a6 = *(a_offset2 + 1);
      a7 = *(a_offset2 + 2);
      a8 = *(a_offset2 + 3);

      *(b_offset2 +  0) = CMULT(a1, a2);
      *(b_offset2 +  1) = CMULT(a3, a4);
      *(b_offset2 +  2) = CMULT(a5, a6);
      *(b_offset2 +  3) = CMULT(a7, a8);

      a_offset1 += 4;
      a_offset2 += 4;
      b_offset2 += 4;
    }

    if (n & 1) {
      a1 = *(a_offset1 + 0);
      a2 = *(a_offset1 + 1);
      a3 = *(a_offset2 + 0);
      a4 = *(a_offset2 + 1);

      *(b_offset3 +  0) = CMULT(a1, a2);
      *(b_offset3 +  1) = CMULT(a3, a4);

      b_offset3 += 2;
    }
  }

  if (m & 1){
    a_offset1  = a_offset;
    b_offset1  = b_offset;

    i = (n >> 2);
    if (i > 0){
      do{
	a1 = *(a_offset1 + 0);
	a2 = *(a_offset1 + 1);
	a3 = *(a_offset1 + 2);
	a4 = *(a_offset1 + 3);
	a5 = *(a_offset1 + 4);
	a6 = *(a_offset1 + 5);
	a7 = *(a_offset1 + 6);
	a8 = *(a_offset1 + 7);

	*(b_offset1 +  0) = CMULT(a1, a2);
	*(b_offset1 +  1) = CMULT(a3, a4);
	*(b_offset1 +  2) = CMULT(a5, a6);
	*(b_offset1 +  3) = CMULT(a7, a8);

	a_offset1 += 8;

	b_offset1 += 4 * m;

	i --;
      }while(i > 0);
    }

    if (n & 2) {
      a1 = *(a_offset1 + 0);
      a2 = *(a_offset1 + 1);
      a3 = *(a_offset1 + 2);
      a4 = *(a_offset1 + 3);

      *(b_offset2 +  0) = CMULT(a1, a2);
      *(b_offset2 +  1) = CMULT(a3, a4);

      a_offset1 += 4;
    }

    if (n & 1) {
      a1 = *(a_offset1 + 0);
      a2 = *(a_offset1 + 1);

      *(b_offset3 +  0) = CMULT(a1, a2);
    }
  }

  return 0;
}
