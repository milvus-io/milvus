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
  FLOAT *b_offset, *b_offset1, *b_offset2, *b_offset3, *b_offset4;
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
  fprintf(stderr, "TNS Real  ");
#elif defined(IMAGE_ONLY)
  fprintf(stderr, "TNS Image ");
#else
  fprintf(stderr, "TNS Both  ");
#endif

#ifdef ICOPY
  fprintf(stderr, " ICOPY    %ld x %ld\n", m, n);
#else
  fprintf(stderr, " OCOPY    %ld x %ld\n", m, n);
#endif
#endif

  a_offset   = a;
  b_offset   = b;

  lda *= 2;

  b_offset2  = b + m  * (n & ~7);
  b_offset3  = b + m  * (n & ~3);
  b_offset4  = b + m  * (n & ~1);

  j = (m >> 3);
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

      a_offset  += 8 * lda;

      b_offset1  = b_offset;
      b_offset  += 64;

      i = (n >> 3);
      if (i > 0){
	do{
	  a1  = *(a_offset1 +  0);
	  a2  = *(a_offset1 +  1);
	  a3  = *(a_offset1 +  2);
	  a4  = *(a_offset1 +  3);
	  a5  = *(a_offset1 +  4);
	  a6  = *(a_offset1 +  5);
	  a7  = *(a_offset1 +  6);
	  a8  = *(a_offset1 +  7);
	  a9  = *(a_offset1 +  8);
	  a10 = *(a_offset1 +  9);
	  a11 = *(a_offset1 + 10);
	  a12 = *(a_offset1 + 11);
	  a13 = *(a_offset1 + 12);
	  a14 = *(a_offset1 + 13);
	  a15 = *(a_offset1 + 14);
	  a16 = *(a_offset1 + 15);

	  *(b_offset1 +  0) = CMULT(a1,  a2);
	  *(b_offset1 +  1) = CMULT(a3,  a4);
	  *(b_offset1 +  2) = CMULT(a5,  a6);
	  *(b_offset1 +  3) = CMULT(a7,  a8);
	  *(b_offset1 +  4) = CMULT(a9,  a10);
	  *(b_offset1 +  5) = CMULT(a11, a12);
	  *(b_offset1 +  6) = CMULT(a13, a14);
	  *(b_offset1 +  7) = CMULT(a15, a16);

	  a1  = *(a_offset2 +  0);
	  a2  = *(a_offset2 +  1);
	  a3  = *(a_offset2 +  2);
	  a4  = *(a_offset2 +  3);
	  a5  = *(a_offset2 +  4);
	  a6  = *(a_offset2 +  5);
	  a7  = *(a_offset2 +  6);
	  a8  = *(a_offset2 +  7);
	  a9  = *(a_offset2 +  8);
	  a10 = *(a_offset2 +  9);
	  a11 = *(a_offset2 + 10);
	  a12 = *(a_offset2 + 11);
	  a13 = *(a_offset2 + 12);
	  a14 = *(a_offset2 + 13);
	  a15 = *(a_offset2 + 14);
	  a16 = *(a_offset2 + 15);

	  *(b_offset1 +  8) = CMULT(a1,  a2);
	  *(b_offset1 +  9) = CMULT(a3,  a4);
	  *(b_offset1 + 10) = CMULT(a5,  a6);
	  *(b_offset1 + 11) = CMULT(a7,  a8);
	  *(b_offset1 + 12) = CMULT(a9,  a10);
	  *(b_offset1 + 13) = CMULT(a11, a12);
	  *(b_offset1 + 14) = CMULT(a13, a14);
	  *(b_offset1 + 15) = CMULT(a15, a16);

	  a1  = *(a_offset3 +  0);
	  a2  = *(a_offset3 +  1);
	  a3  = *(a_offset3 +  2);
	  a4  = *(a_offset3 +  3);
	  a5  = *(a_offset3 +  4);
	  a6  = *(a_offset3 +  5);
	  a7  = *(a_offset3 +  6);
	  a8  = *(a_offset3 +  7);
	  a9  = *(a_offset3 +  8);
	  a10 = *(a_offset3 +  9);
	  a11 = *(a_offset3 + 10);
	  a12 = *(a_offset3 + 11);
	  a13 = *(a_offset3 + 12);
	  a14 = *(a_offset3 + 13);
	  a15 = *(a_offset3 + 14);
	  a16 = *(a_offset3 + 15);

	  *(b_offset1 + 16) = CMULT(a1,  a2);
	  *(b_offset1 + 17) = CMULT(a3,  a4);
	  *(b_offset1 + 18) = CMULT(a5,  a6);
	  *(b_offset1 + 19) = CMULT(a7,  a8);
	  *(b_offset1 + 20) = CMULT(a9,  a10);
	  *(b_offset1 + 21) = CMULT(a11, a12);
	  *(b_offset1 + 22) = CMULT(a13, a14);
	  *(b_offset1 + 23) = CMULT(a15, a16);

	  a1  = *(a_offset4 +  0);
	  a2  = *(a_offset4 +  1);
	  a3  = *(a_offset4 +  2);
	  a4  = *(a_offset4 +  3);
	  a5  = *(a_offset4 +  4);
	  a6  = *(a_offset4 +  5);
	  a7  = *(a_offset4 +  6);
	  a8  = *(a_offset4 +  7);
	  a9  = *(a_offset4 +  8);
	  a10 = *(a_offset4 +  9);
	  a11 = *(a_offset4 + 10);
	  a12 = *(a_offset4 + 11);
	  a13 = *(a_offset4 + 12);
	  a14 = *(a_offset4 + 13);
	  a15 = *(a_offset4 + 14);
	  a16 = *(a_offset4 + 15);

	  *(b_offset1 + 24) = CMULT(a1,  a2);
	  *(b_offset1 + 25) = CMULT(a3,  a4);
	  *(b_offset1 + 26) = CMULT(a5,  a6);
	  *(b_offset1 + 27) = CMULT(a7,  a8);
	  *(b_offset1 + 28) = CMULT(a9,  a10);
	  *(b_offset1 + 29) = CMULT(a11, a12);
	  *(b_offset1 + 30) = CMULT(a13, a14);
	  *(b_offset1 + 31) = CMULT(a15, a16);

	  a1  = *(a_offset5 +  0);
	  a2  = *(a_offset5 +  1);
	  a3  = *(a_offset5 +  2);
	  a4  = *(a_offset5 +  3);
	  a5  = *(a_offset5 +  4);
	  a6  = *(a_offset5 +  5);
	  a7  = *(a_offset5 +  6);
	  a8  = *(a_offset5 +  7);
	  a9  = *(a_offset5 +  8);
	  a10 = *(a_offset5 +  9);
	  a11 = *(a_offset5 + 10);
	  a12 = *(a_offset5 + 11);
	  a13 = *(a_offset5 + 12);
	  a14 = *(a_offset5 + 13);
	  a15 = *(a_offset5 + 14);
	  a16 = *(a_offset5 + 15);

	  *(b_offset1 + 32) = CMULT(a1,  a2);
	  *(b_offset1 + 33) = CMULT(a3,  a4);
	  *(b_offset1 + 34) = CMULT(a5,  a6);
	  *(b_offset1 + 35) = CMULT(a7,  a8);
	  *(b_offset1 + 36) = CMULT(a9,  a10);
	  *(b_offset1 + 37) = CMULT(a11, a12);
	  *(b_offset1 + 38) = CMULT(a13, a14);
	  *(b_offset1 + 39) = CMULT(a15, a16);

	  a1  = *(a_offset6 +  0);
	  a2  = *(a_offset6 +  1);
	  a3  = *(a_offset6 +  2);
	  a4  = *(a_offset6 +  3);
	  a5  = *(a_offset6 +  4);
	  a6  = *(a_offset6 +  5);
	  a7  = *(a_offset6 +  6);
	  a8  = *(a_offset6 +  7);
	  a9  = *(a_offset6 +  8);
	  a10 = *(a_offset6 +  9);
	  a11 = *(a_offset6 + 10);
	  a12 = *(a_offset6 + 11);
	  a13 = *(a_offset6 + 12);
	  a14 = *(a_offset6 + 13);
	  a15 = *(a_offset6 + 14);
	  a16 = *(a_offset6 + 15);

	  *(b_offset1 + 40) = CMULT(a1,  a2);
	  *(b_offset1 + 41) = CMULT(a3,  a4);
	  *(b_offset1 + 42) = CMULT(a5,  a6);
	  *(b_offset1 + 43) = CMULT(a7,  a8);
	  *(b_offset1 + 44) = CMULT(a9,  a10);
	  *(b_offset1 + 45) = CMULT(a11, a12);
	  *(b_offset1 + 46) = CMULT(a13, a14);
	  *(b_offset1 + 47) = CMULT(a15, a16);

	  a1  = *(a_offset7 +  0);
	  a2  = *(a_offset7 +  1);
	  a3  = *(a_offset7 +  2);
	  a4  = *(a_offset7 +  3);
	  a5  = *(a_offset7 +  4);
	  a6  = *(a_offset7 +  5);
	  a7  = *(a_offset7 +  6);
	  a8  = *(a_offset7 +  7);
	  a9  = *(a_offset7 +  8);
	  a10 = *(a_offset7 +  9);
	  a11 = *(a_offset7 + 10);
	  a12 = *(a_offset7 + 11);
	  a13 = *(a_offset7 + 12);
	  a14 = *(a_offset7 + 13);
	  a15 = *(a_offset7 + 14);
	  a16 = *(a_offset7 + 15);

	  *(b_offset1 + 48) = CMULT(a1,  a2);
	  *(b_offset1 + 49) = CMULT(a3,  a4);
	  *(b_offset1 + 50) = CMULT(a5,  a6);
	  *(b_offset1 + 51) = CMULT(a7,  a8);
	  *(b_offset1 + 52) = CMULT(a9,  a10);
	  *(b_offset1 + 53) = CMULT(a11, a12);
	  *(b_offset1 + 54) = CMULT(a13, a14);
	  *(b_offset1 + 55) = CMULT(a15, a16);

	  a1  = *(a_offset8 +  0);
	  a2  = *(a_offset8 +  1);
	  a3  = *(a_offset8 +  2);
	  a4  = *(a_offset8 +  3);
	  a5  = *(a_offset8 +  4);
	  a6  = *(a_offset8 +  5);
	  a7  = *(a_offset8 +  6);
	  a8  = *(a_offset8 +  7);
	  a9  = *(a_offset8 +  8);
	  a10 = *(a_offset8 +  9);
	  a11 = *(a_offset8 + 10);
	  a12 = *(a_offset8 + 11);
	  a13 = *(a_offset8 + 12);
	  a14 = *(a_offset8 + 13);
	  a15 = *(a_offset8 + 14);
	  a16 = *(a_offset8 + 15);

	  *(b_offset1 + 56) = CMULT(a1,  a2);
	  *(b_offset1 + 57) = CMULT(a3,  a4);
	  *(b_offset1 + 58) = CMULT(a5,  a6);
	  *(b_offset1 + 59) = CMULT(a7,  a8);
	  *(b_offset1 + 60) = CMULT(a9,  a10);
	  *(b_offset1 + 61) = CMULT(a11, a12);
	  *(b_offset1 + 62) = CMULT(a13, a14);
	  *(b_offset1 + 63) = CMULT(a15, a16);

	  a_offset1 += 16;
	  a_offset2 += 16;
	  a_offset3 += 16;
	  a_offset4 += 16;
	  a_offset5 += 16;
	  a_offset6 += 16;
	  a_offset7 += 16;
	  a_offset8 += 16;

	  b_offset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);
	a5  = *(a_offset1 +  4);
	a6  = *(a_offset1 +  5);
	a7  = *(a_offset1 +  6);
	a8  = *(a_offset1 +  7);

	*(b_offset2 +  0) = CMULT(a1,  a2);
	*(b_offset2 +  1) = CMULT(a3,  a4);
	*(b_offset2 +  2) = CMULT(a5,  a6);
	*(b_offset2 +  3) = CMULT(a7,  a8);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);
	a5  = *(a_offset2 +  4);
	a6  = *(a_offset2 +  5);
	a7  = *(a_offset2 +  6);
	a8  = *(a_offset2 +  7);

	*(b_offset2 +  4) = CMULT(a1,  a2);
	*(b_offset2 +  5) = CMULT(a3,  a4);
	*(b_offset2 +  6) = CMULT(a5,  a6);
	*(b_offset2 +  7) = CMULT(a7,  a8);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);
	a3  = *(a_offset3 +  2);
	a4  = *(a_offset3 +  3);
	a5  = *(a_offset3 +  4);
	a6  = *(a_offset3 +  5);
	a7  = *(a_offset3 +  6);
	a8  = *(a_offset3 +  7);

	*(b_offset2 +  8) = CMULT(a1,  a2);
	*(b_offset2 +  9) = CMULT(a3,  a4);
	*(b_offset2 + 10) = CMULT(a5,  a6);
	*(b_offset2 + 11) = CMULT(a7,  a8);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);
	a3  = *(a_offset4 +  2);
	a4  = *(a_offset4 +  3);
	a5  = *(a_offset4 +  4);
	a6  = *(a_offset4 +  5);
	a7  = *(a_offset4 +  6);
	a8  = *(a_offset4 +  7);

	*(b_offset2 + 12) = CMULT(a1,  a2);
	*(b_offset2 + 13) = CMULT(a3,  a4);
	*(b_offset2 + 14) = CMULT(a5,  a6);
	*(b_offset2 + 15) = CMULT(a7,  a8);

	a1  = *(a_offset5 +  0);
	a2  = *(a_offset5 +  1);
	a3  = *(a_offset5 +  2);
	a4  = *(a_offset5 +  3);
	a5  = *(a_offset5 +  4);
	a6  = *(a_offset5 +  5);
	a7  = *(a_offset5 +  6);
	a8  = *(a_offset5 +  7);

	*(b_offset2 + 16) = CMULT(a1,  a2);
	*(b_offset2 + 17) = CMULT(a3,  a4);
	*(b_offset2 + 18) = CMULT(a5,  a6);
	*(b_offset2 + 19) = CMULT(a7,  a8);

	a1  = *(a_offset6 +  0);
	a2  = *(a_offset6 +  1);
	a3  = *(a_offset6 +  2);
	a4  = *(a_offset6 +  3);
	a5  = *(a_offset6 +  4);
	a6  = *(a_offset6 +  5);
	a7  = *(a_offset6 +  6);
	a8  = *(a_offset6 +  7);

	*(b_offset2 + 20) = CMULT(a1,  a2);
	*(b_offset2 + 21) = CMULT(a3,  a4);
	*(b_offset2 + 22) = CMULT(a5,  a6);
	*(b_offset2 + 23) = CMULT(a7,  a8);

	a1  = *(a_offset7 +  0);
	a2  = *(a_offset7 +  1);
	a3  = *(a_offset7 +  2);
	a4  = *(a_offset7 +  3);
	a5  = *(a_offset7 +  4);
	a6  = *(a_offset7 +  5);
	a7  = *(a_offset7 +  6);
	a8  = *(a_offset7 +  7);

	*(b_offset2 + 24) = CMULT(a1,  a2);
	*(b_offset2 + 25) = CMULT(a3,  a4);
	*(b_offset2 + 26) = CMULT(a5,  a6);
	*(b_offset2 + 27) = CMULT(a7,  a8);

	a1  = *(a_offset8 +  0);
	a2  = *(a_offset8 +  1);
	a3  = *(a_offset8 +  2);
	a4  = *(a_offset8 +  3);
	a5  = *(a_offset8 +  4);
	a6  = *(a_offset8 +  5);
	a7  = *(a_offset8 +  6);
	a8  = *(a_offset8 +  7);

	*(b_offset2 + 28) = CMULT(a1,  a2);
	*(b_offset2 + 29) = CMULT(a3,  a4);
	*(b_offset2 + 30) = CMULT(a5,  a6);
	*(b_offset2 + 31) = CMULT(a7,  a8);

	a_offset1 += 8;
	a_offset2 += 8;
	a_offset3 += 8;
	a_offset4 += 8;
	a_offset5 += 8;
	a_offset6 += 8;
	a_offset7 += 8;
	a_offset8 += 8;

	b_offset2 += 32;
      }

      if (n & 2){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);

	*(b_offset3 +  0) = CMULT(a1,  a2);
	*(b_offset3 +  1) = CMULT(a3,  a4);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);

	*(b_offset3 +  2) = CMULT(a1,  a2);
	*(b_offset3 +  3) = CMULT(a3,  a4);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);
	a3  = *(a_offset3 +  2);
	a4  = *(a_offset3 +  3);

	*(b_offset3 +  4) = CMULT(a1,  a2);
	*(b_offset3 +  5) = CMULT(a3,  a4);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);
	a3  = *(a_offset4 +  2);
	a4  = *(a_offset4 +  3);

	*(b_offset3 +  6) = CMULT(a1,  a2);
	*(b_offset3 +  7) = CMULT(a3,  a4);

	a1  = *(a_offset5 +  0);
	a2  = *(a_offset5 +  1);
	a3  = *(a_offset5 +  2);
	a4  = *(a_offset5 +  3);

	*(b_offset3 +  8) = CMULT(a1,  a2);
	*(b_offset3 +  9) = CMULT(a3,  a4);

	a1  = *(a_offset6 +  0);
	a2  = *(a_offset6 +  1);
	a3  = *(a_offset6 +  2);
	a4  = *(a_offset6 +  3);

	*(b_offset3 + 10) = CMULT(a1,  a2);
	*(b_offset3 + 11) = CMULT(a3,  a4);

	a1  = *(a_offset7 +  0);
	a2  = *(a_offset7 +  1);
	a3  = *(a_offset7 +  2);
	a4  = *(a_offset7 +  3);

	*(b_offset3 + 12) = CMULT(a1,  a2);
	*(b_offset3 + 13) = CMULT(a3,  a4);

	a1  = *(a_offset8 +  0);
	a2  = *(a_offset8 +  1);
	a3  = *(a_offset8 +  2);
	a4  = *(a_offset8 +  3);

	*(b_offset3 + 14) = CMULT(a1,  a2);
	*(b_offset3 + 15) = CMULT(a3,  a4);

	a_offset1 += 4;
	a_offset2 += 4;
	a_offset3 += 4;
	a_offset4 += 4;
	a_offset5 += 4;
	a_offset6 += 4;
	a_offset7 += 4;
	a_offset8 += 4;

	b_offset3 += 16;
      }

      if (n & 1){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);

	*(b_offset4 +  0) = CMULT(a1,  a2);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);

	*(b_offset4 +  1) = CMULT(a1,  a2);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);

	*(b_offset4 +  2) = CMULT(a1,  a2);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);

	*(b_offset4 +  3) = CMULT(a1,  a2);

	a1  = *(a_offset5 +  0);
	a2  = *(a_offset5 +  1);

	*(b_offset4 +  4) = CMULT(a1,  a2);

	a1  = *(a_offset6 +  0);
	a2  = *(a_offset6 +  1);

	*(b_offset4 +  5) = CMULT(a1,  a2);

	a1  = *(a_offset7 +  0);
	a2  = *(a_offset7 +  1);

	*(b_offset4 +  6) = CMULT(a1,  a2);

	a1  = *(a_offset8 +  0);
	a2  = *(a_offset8 +  1);

	*(b_offset4 +  7) = CMULT(a1,  a2);

	b_offset4 += 8;
      }

      j--;
    }while(j > 0);
  }

  if (m & 4){
      a_offset1  = a_offset;
      a_offset2  = a_offset1 + lda;
      a_offset3  = a_offset2 + lda;
      a_offset4  = a_offset3 + lda;
      a_offset  += 4 * lda;

      b_offset1  = b_offset;
      b_offset  += 32;

      i = (n >> 3);
      if (i > 0){
	do{
	  a1  = *(a_offset1 +  0);
	  a2  = *(a_offset1 +  1);
	  a3  = *(a_offset1 +  2);
	  a4  = *(a_offset1 +  3);
	  a5  = *(a_offset1 +  4);
	  a6  = *(a_offset1 +  5);
	  a7  = *(a_offset1 +  6);
	  a8  = *(a_offset1 +  7);
	  a9  = *(a_offset1 +  8);
	  a10 = *(a_offset1 +  9);
	  a11 = *(a_offset1 + 10);
	  a12 = *(a_offset1 + 11);
	  a13 = *(a_offset1 + 12);
	  a14 = *(a_offset1 + 13);
	  a15 = *(a_offset1 + 14);
	  a16 = *(a_offset1 + 15);

	  *(b_offset1 +  0) = CMULT(a1,  a2);
	  *(b_offset1 +  1) = CMULT(a3,  a4);
	  *(b_offset1 +  2) = CMULT(a5,  a6);
	  *(b_offset1 +  3) = CMULT(a7,  a8);
	  *(b_offset1 +  4) = CMULT(a9,  a10);
	  *(b_offset1 +  5) = CMULT(a11, a12);
	  *(b_offset1 +  6) = CMULT(a13, a14);
	  *(b_offset1 +  7) = CMULT(a15, a16);

	  a1  = *(a_offset2 +  0);
	  a2  = *(a_offset2 +  1);
	  a3  = *(a_offset2 +  2);
	  a4  = *(a_offset2 +  3);
	  a5  = *(a_offset2 +  4);
	  a6  = *(a_offset2 +  5);
	  a7  = *(a_offset2 +  6);
	  a8  = *(a_offset2 +  7);
	  a9  = *(a_offset2 +  8);
	  a10 = *(a_offset2 +  9);
	  a11 = *(a_offset2 + 10);
	  a12 = *(a_offset2 + 11);
	  a13 = *(a_offset2 + 12);
	  a14 = *(a_offset2 + 13);
	  a15 = *(a_offset2 + 14);
	  a16 = *(a_offset2 + 15);

	  *(b_offset1 +  8) = CMULT(a1,  a2);
	  *(b_offset1 +  9) = CMULT(a3,  a4);
	  *(b_offset1 + 10) = CMULT(a5,  a6);
	  *(b_offset1 + 11) = CMULT(a7,  a8);
	  *(b_offset1 + 12) = CMULT(a9,  a10);
	  *(b_offset1 + 13) = CMULT(a11, a12);
	  *(b_offset1 + 14) = CMULT(a13, a14);
	  *(b_offset1 + 15) = CMULT(a15, a16);

	  a1  = *(a_offset3 +  0);
	  a2  = *(a_offset3 +  1);
	  a3  = *(a_offset3 +  2);
	  a4  = *(a_offset3 +  3);
	  a5  = *(a_offset3 +  4);
	  a6  = *(a_offset3 +  5);
	  a7  = *(a_offset3 +  6);
	  a8  = *(a_offset3 +  7);
	  a9  = *(a_offset3 +  8);
	  a10 = *(a_offset3 +  9);
	  a11 = *(a_offset3 + 10);
	  a12 = *(a_offset3 + 11);
	  a13 = *(a_offset3 + 12);
	  a14 = *(a_offset3 + 13);
	  a15 = *(a_offset3 + 14);
	  a16 = *(a_offset3 + 15);

	  *(b_offset1 + 16) = CMULT(a1,  a2);
	  *(b_offset1 + 17) = CMULT(a3,  a4);
	  *(b_offset1 + 18) = CMULT(a5,  a6);
	  *(b_offset1 + 19) = CMULT(a7,  a8);
	  *(b_offset1 + 20) = CMULT(a9,  a10);
	  *(b_offset1 + 21) = CMULT(a11, a12);
	  *(b_offset1 + 22) = CMULT(a13, a14);
	  *(b_offset1 + 23) = CMULT(a15, a16);

	  a1  = *(a_offset4 +  0);
	  a2  = *(a_offset4 +  1);
	  a3  = *(a_offset4 +  2);
	  a4  = *(a_offset4 +  3);
	  a5  = *(a_offset4 +  4);
	  a6  = *(a_offset4 +  5);
	  a7  = *(a_offset4 +  6);
	  a8  = *(a_offset4 +  7);
	  a9  = *(a_offset4 +  8);
	  a10 = *(a_offset4 +  9);
	  a11 = *(a_offset4 + 10);
	  a12 = *(a_offset4 + 11);
	  a13 = *(a_offset4 + 12);
	  a14 = *(a_offset4 + 13);
	  a15 = *(a_offset4 + 14);
	  a16 = *(a_offset4 + 15);

	  *(b_offset1 + 24) = CMULT(a1,  a2);
	  *(b_offset1 + 25) = CMULT(a3,  a4);
	  *(b_offset1 + 26) = CMULT(a5,  a6);
	  *(b_offset1 + 27) = CMULT(a7,  a8);
	  *(b_offset1 + 28) = CMULT(a9,  a10);
	  *(b_offset1 + 29) = CMULT(a11, a12);
	  *(b_offset1 + 30) = CMULT(a13, a14);
	  *(b_offset1 + 31) = CMULT(a15, a16);

	  a_offset1 += 16;
	  a_offset2 += 16;
	  a_offset3 += 16;
	  a_offset4 += 16;

	  b_offset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);
	a5  = *(a_offset1 +  4);
	a6  = *(a_offset1 +  5);
	a7  = *(a_offset1 +  6);
	a8  = *(a_offset1 +  7);

	*(b_offset2 +  0) = CMULT(a1,  a2);
	*(b_offset2 +  1) = CMULT(a3,  a4);
	*(b_offset2 +  2) = CMULT(a5,  a6);
	*(b_offset2 +  3) = CMULT(a7,  a8);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);
	a5  = *(a_offset2 +  4);
	a6  = *(a_offset2 +  5);
	a7  = *(a_offset2 +  6);
	a8  = *(a_offset2 +  7);

	*(b_offset2 +  4) = CMULT(a1,  a2);
	*(b_offset2 +  5) = CMULT(a3,  a4);
	*(b_offset2 +  6) = CMULT(a5,  a6);
	*(b_offset2 +  7) = CMULT(a7,  a8);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);
	a3  = *(a_offset3 +  2);
	a4  = *(a_offset3 +  3);
	a5  = *(a_offset3 +  4);
	a6  = *(a_offset3 +  5);
	a7  = *(a_offset3 +  6);
	a8  = *(a_offset3 +  7);

	*(b_offset2 +  8) = CMULT(a1,  a2);
	*(b_offset2 +  9) = CMULT(a3,  a4);
	*(b_offset2 + 10) = CMULT(a5,  a6);
	*(b_offset2 + 11) = CMULT(a7,  a8);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);
	a3  = *(a_offset4 +  2);
	a4  = *(a_offset4 +  3);
	a5  = *(a_offset4 +  4);
	a6  = *(a_offset4 +  5);
	a7  = *(a_offset4 +  6);
	a8  = *(a_offset4 +  7);

	*(b_offset2 + 12) = CMULT(a1,  a2);
	*(b_offset2 + 13) = CMULT(a3,  a4);
	*(b_offset2 + 14) = CMULT(a5,  a6);
	*(b_offset2 + 15) = CMULT(a7,  a8);

	a_offset1 += 8;
	a_offset2 += 8;
	a_offset3 += 8;
	a_offset4 += 8;

	b_offset2 += 16;
      }

      if (n & 2){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);

	*(b_offset3 +  0) = CMULT(a1,  a2);
	*(b_offset3 +  1) = CMULT(a3,  a4);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);

	*(b_offset3 +  2) = CMULT(a1,  a2);
	*(b_offset3 +  3) = CMULT(a3,  a4);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);
	a3  = *(a_offset3 +  2);
	a4  = *(a_offset3 +  3);

	*(b_offset3 +  4) = CMULT(a1,  a2);
	*(b_offset3 +  5) = CMULT(a3,  a4);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);
	a3  = *(a_offset4 +  2);
	a4  = *(a_offset4 +  3);

	*(b_offset3 +  6) = CMULT(a1,  a2);
	*(b_offset3 +  7) = CMULT(a3,  a4);

	a_offset1 += 4;
	a_offset2 += 4;
	a_offset3 += 4;
	a_offset4 += 4;

	b_offset3 += 8;
      }

      if (n & 1){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);

	*(b_offset4 +  0) = CMULT(a1,  a2);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);

	*(b_offset4 +  1) = CMULT(a1,  a2);

	a1  = *(a_offset3 +  0);
	a2  = *(a_offset3 +  1);

	*(b_offset4 +  2) = CMULT(a1,  a2);

	a1  = *(a_offset4 +  0);
	a2  = *(a_offset4 +  1);

	*(b_offset4 +  3) = CMULT(a1,  a2);

	b_offset4 += 4;
      }
  }

  if (m & 2){
      a_offset1  = a_offset;
      a_offset2  = a_offset1 + lda;
      a_offset  += 2 * lda;

      b_offset1  = b_offset;
      b_offset  += 16;

      i = (n >> 3);
      if (i > 0){
	do{
	  a1  = *(a_offset1 +  0);
	  a2  = *(a_offset1 +  1);
	  a3  = *(a_offset1 +  2);
	  a4  = *(a_offset1 +  3);
	  a5  = *(a_offset1 +  4);
	  a6  = *(a_offset1 +  5);
	  a7  = *(a_offset1 +  6);
	  a8  = *(a_offset1 +  7);
	  a9  = *(a_offset1 +  8);
	  a10 = *(a_offset1 +  9);
	  a11 = *(a_offset1 + 10);
	  a12 = *(a_offset1 + 11);
	  a13 = *(a_offset1 + 12);
	  a14 = *(a_offset1 + 13);
	  a15 = *(a_offset1 + 14);
	  a16 = *(a_offset1 + 15);

	  *(b_offset1 +  0) = CMULT(a1,  a2);
	  *(b_offset1 +  1) = CMULT(a3,  a4);
	  *(b_offset1 +  2) = CMULT(a5,  a6);
	  *(b_offset1 +  3) = CMULT(a7,  a8);
	  *(b_offset1 +  4) = CMULT(a9,  a10);
	  *(b_offset1 +  5) = CMULT(a11, a12);
	  *(b_offset1 +  6) = CMULT(a13, a14);
	  *(b_offset1 +  7) = CMULT(a15, a16);

	  a1  = *(a_offset2 +  0);
	  a2  = *(a_offset2 +  1);
	  a3  = *(a_offset2 +  2);
	  a4  = *(a_offset2 +  3);
	  a5  = *(a_offset2 +  4);
	  a6  = *(a_offset2 +  5);
	  a7  = *(a_offset2 +  6);
	  a8  = *(a_offset2 +  7);
	  a9  = *(a_offset2 +  8);
	  a10 = *(a_offset2 +  9);
	  a11 = *(a_offset2 + 10);
	  a12 = *(a_offset2 + 11);
	  a13 = *(a_offset2 + 12);
	  a14 = *(a_offset2 + 13);
	  a15 = *(a_offset2 + 14);
	  a16 = *(a_offset2 + 15);

	  *(b_offset1 +  8) = CMULT(a1,  a2);
	  *(b_offset1 +  9) = CMULT(a3,  a4);
	  *(b_offset1 + 10) = CMULT(a5,  a6);
	  *(b_offset1 + 11) = CMULT(a7,  a8);
	  *(b_offset1 + 12) = CMULT(a9,  a10);
	  *(b_offset1 + 13) = CMULT(a11, a12);
	  *(b_offset1 + 14) = CMULT(a13, a14);
	  *(b_offset1 + 15) = CMULT(a15, a16);

	  a_offset1 += 16;
	  a_offset2 += 16;

	  b_offset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);
	a5  = *(a_offset1 +  4);
	a6  = *(a_offset1 +  5);
	a7  = *(a_offset1 +  6);
	a8  = *(a_offset1 +  7);

	*(b_offset2 +  0) = CMULT(a1,  a2);
	*(b_offset2 +  1) = CMULT(a3,  a4);
	*(b_offset2 +  2) = CMULT(a5,  a6);
	*(b_offset2 +  3) = CMULT(a7,  a8);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);
	a5  = *(a_offset2 +  4);
	a6  = *(a_offset2 +  5);
	a7  = *(a_offset2 +  6);
	a8  = *(a_offset2 +  7);

	*(b_offset2 +  4) = CMULT(a1,  a2);
	*(b_offset2 +  5) = CMULT(a3,  a4);
	*(b_offset2 +  6) = CMULT(a5,  a6);
	*(b_offset2 +  7) = CMULT(a7,  a8);

	a_offset1 += 8;
	a_offset2 += 8;

	b_offset2 += 8;
      }

      if (n & 2){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);

	*(b_offset3 +  0) = CMULT(a1,  a2);
	*(b_offset3 +  1) = CMULT(a3,  a4);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);
	a3  = *(a_offset2 +  2);
	a4  = *(a_offset2 +  3);

	*(b_offset3 +  2) = CMULT(a1,  a2);
	*(b_offset3 +  3) = CMULT(a3,  a4);

	a_offset1 += 4;
	a_offset2 += 4;

	b_offset3 += 4;
      }

      if (n & 1){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);

	*(b_offset4 +  0) = CMULT(a1,  a2);

	a1  = *(a_offset2 +  0);
	a2  = *(a_offset2 +  1);

	*(b_offset4 +  1) = CMULT(a1,  a2);

	b_offset4 += 2;
      }
  }

  if (m & 1){
      a_offset1  = a_offset;
      b_offset1  = b_offset;

      i = (n >> 3);
      if (i > 0){
	do{
	  a1  = *(a_offset1 +  0);
	  a2  = *(a_offset1 +  1);
	  a3  = *(a_offset1 +  2);
	  a4  = *(a_offset1 +  3);
	  a5  = *(a_offset1 +  4);
	  a6  = *(a_offset1 +  5);
	  a7  = *(a_offset1 +  6);
	  a8  = *(a_offset1 +  7);
	  a9  = *(a_offset1 +  8);
	  a10 = *(a_offset1 +  9);
	  a11 = *(a_offset1 + 10);
	  a12 = *(a_offset1 + 11);
	  a13 = *(a_offset1 + 12);
	  a14 = *(a_offset1 + 13);
	  a15 = *(a_offset1 + 14);
	  a16 = *(a_offset1 + 15);

	  *(b_offset1 +  0) = CMULT(a1,  a2);
	  *(b_offset1 +  1) = CMULT(a3,  a4);
	  *(b_offset1 +  2) = CMULT(a5,  a6);
	  *(b_offset1 +  3) = CMULT(a7,  a8);
	  *(b_offset1 +  4) = CMULT(a9,  a10);
	  *(b_offset1 +  5) = CMULT(a11, a12);
	  *(b_offset1 +  6) = CMULT(a13, a14);
	  *(b_offset1 +  7) = CMULT(a15, a16);

	  a_offset1 += 16;

	  b_offset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);
	a5  = *(a_offset1 +  4);
	a6  = *(a_offset1 +  5);
	a7  = *(a_offset1 +  6);
	a8  = *(a_offset1 +  7);

	*(b_offset2 +  0) = CMULT(a1,  a2);
	*(b_offset2 +  1) = CMULT(a3,  a4);
	*(b_offset2 +  2) = CMULT(a5,  a6);
	*(b_offset2 +  3) = CMULT(a7,  a8);

	a_offset1 += 8;
	// b_offset2 += 4;
      }

      if (n & 2){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);
	a3  = *(a_offset1 +  2);
	a4  = *(a_offset1 +  3);

	*(b_offset3 +  0) = CMULT(a1,  a2);
	*(b_offset3 +  1) = CMULT(a3,  a4);

	a_offset1 += 4;
	// b_offset3 += 2;
      }

      if (n & 1){
	a1  = *(a_offset1 +  0);
	a2  = *(a_offset1 +  1);

	*(b_offset4 +  0) = CMULT(a1,  a2);
      }
  }

  return 0;
}
