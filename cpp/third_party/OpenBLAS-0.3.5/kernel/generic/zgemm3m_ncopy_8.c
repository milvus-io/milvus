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
  FLOAT *a_offset5, *a_offset6, *a_offset7, *a_offset8;
  FLOAT *b_offset;
  FLOAT a1, a2,  a3,  a4,  a5,  a6,  a7,  a8;
  FLOAT a9, a10, a11, a12, a13, a14, a15, a16;

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
  (void)a9;
  (void)a10;
  (void)a11;
  (void)a12;
  (void)a13;
  (void)a14;
  (void)a15;
  (void)a16;

#if 0
#ifdef REAL_ONLY
  fprintf(stderr, "NON Real  ");
#elif defined(IMAGE_ONLY)
  fprintf(stderr, "NON Image ");
#else
  fprintf(stderr, "NON Both  ");
#endif

#ifdef ICOPY
  fprintf(stderr, " ICOPY    %ld x %ld\n", m, n);
#else
  fprintf(stderr, " OCOPY    %ld x %ld\n", m, n);
#endif
#endif

  lda *= 2;

  a_offset = a;
  b_offset = b;

  j = (n >> 3);
  if (j > 0){
    do{
      a_offset1  = a_offset;
      a_offset2  = a_offset1 + lda;
      a_offset3  = a_offset2 + lda;
      a_offset4  = a_offset3 + lda;
      a_offset5  = a_offset4 + lda;
      a_offset6  = a_offset5 + lda;
      a_offset7  = a_offset6 + lda;
      a_offset8  = a_offset7 + lda;
      a_offset += 8 * lda;

      for (i = 0; i < m; i ++) {
	a1  = *(a_offset1 + 0);
	a2  = *(a_offset1 + 1);
	a3  = *(a_offset2 + 0);
	a4  = *(a_offset2 + 1);
	a5  = *(a_offset3 + 0);
	a6  = *(a_offset3 + 1);
	a7  = *(a_offset4 + 0);
	a8  = *(a_offset4 + 1);
	a9  = *(a_offset5 + 0);
	a10 = *(a_offset5 + 1);
	a11 = *(a_offset6 + 0);
	a12 = *(a_offset6 + 1);
	a13 = *(a_offset7 + 0);
	a14 = *(a_offset7 + 1);
	a15 = *(a_offset8 + 0);
	a16 = *(a_offset8 + 1);

	*(b_offset +  0) = CMULT(a1,  a2);
	*(b_offset +  1) = CMULT(a3,  a4);
	*(b_offset +  2) = CMULT(a5,  a6);
	*(b_offset +  3) = CMULT(a7,  a8);
	*(b_offset +  4) = CMULT(a9,  a10);
	*(b_offset +  5) = CMULT(a11, a12);
	*(b_offset +  6) = CMULT(a13, a14);
	*(b_offset +  7) = CMULT(a15, a16);

	a_offset1 += 2;
	a_offset2 += 2;
	a_offset3 += 2;
	a_offset4 += 2;
	a_offset5 += 2;
	a_offset6 += 2;
	a_offset7 += 2;
	a_offset8 += 2;

	b_offset  += 8;
      }

      j--;
    }while(j > 0);
  }

  if (n & 4){
    a_offset1  = a_offset;
    a_offset2  = a_offset1 + lda;
    a_offset3  = a_offset2 + lda;
    a_offset4  = a_offset3 + lda;
    a_offset += 4 * lda;

    for (i = 0; i < m; i ++) {
      a1  = *(a_offset1 + 0);
      a2  = *(a_offset1 + 1);
      a3  = *(a_offset2 + 0);
      a4  = *(a_offset2 + 1);
      a5  = *(a_offset3 + 0);
      a6  = *(a_offset3 + 1);
      a7  = *(a_offset4 + 0);
      a8  = *(a_offset4 + 1);

      *(b_offset +  0) = CMULT(a1,  a2);
      *(b_offset +  1) = CMULT(a3,  a4);
      *(b_offset +  2) = CMULT(a5,  a6);
      *(b_offset +  3) = CMULT(a7,  a8);

      a_offset1 += 2;
      a_offset2 += 2;
      a_offset3 += 2;
      a_offset4 += 2;

      b_offset  += 4;
    }
  }

  if (n & 2){
    a_offset1  = a_offset;
    a_offset2  = a_offset1 + lda;
    a_offset += 2 * lda;

    for (i = 0; i < m; i ++) {
      a1  = *(a_offset1 + 0);
      a2  = *(a_offset1 + 1);
      a3  = *(a_offset2 + 0);
      a4  = *(a_offset2 + 1);

      *(b_offset +  0) = CMULT(a1,  a2);
      *(b_offset +  1) = CMULT(a3,  a4);

      a_offset1 += 2;
      a_offset2 += 2;

      b_offset  += 2;
    }
  }

  if (n & 1){
    a_offset1  = a_offset;

    for (i = 0; i < m; i ++) {
      a1  = *(a_offset1 + 0);
      a2  = *(a_offset1 + 1);

      *(b_offset +  0) = CMULT(a1,  a2);

      a_offset1 += 2;
      b_offset  += 1;
    }
  }

  return 0;
}
