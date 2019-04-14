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
#include <immintrin.h>

int CNAME(BLASLONG m, BLASLONG n, FLOAT * __restrict a, BLASLONG lda, FLOAT * __restrict b){

  BLASLONG i, j;

  FLOAT *aoffset;
  FLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  FLOAT *aoffset5, *aoffset6, *aoffset7, *aoffset8;

  FLOAT *boffset,  *boffset1, *boffset2, *boffset3, *boffset4;

  FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  FLOAT ctemp05, ctemp06, ctemp07, ctemp08;

  aoffset   = a;
  boffset   = b;

#if 0
  fprintf(stderr, "M = %d N = %d\n", m, n);
#endif

  boffset2  = b + m  * (n & ~7);
  boffset3  = b + m  * (n & ~3);
  boffset4  = b + m  * (n & ~1);

  j = (m >> 3);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1 + lda;
      aoffset3  = aoffset2 + lda;
      aoffset4  = aoffset3 + lda;
      aoffset5  = aoffset4 + lda;
      aoffset6  = aoffset5 + lda;
      aoffset7  = aoffset6 + lda;
      aoffset8  = aoffset7 + lda;
      aoffset += 8 * lda;

      boffset1  = boffset;
      boffset  += 64;

      i = (n >> 3);
      if (i > 0){
	do{
	  __m512d row1, row2, row3, row4, row5, row6, row7, row8;
	  row1 = _mm512_loadu_pd(aoffset1);
	  aoffset1 += 8;
	  row2 = _mm512_loadu_pd(aoffset2);
	  aoffset2 += 8;
	  row3 = _mm512_loadu_pd(aoffset3);
	  aoffset3 += 8;
	  row4 = _mm512_loadu_pd(aoffset4);
	  aoffset4 += 8;
	  row5 = _mm512_loadu_pd(aoffset5);
	  aoffset5 += 8;
	  row6 = _mm512_loadu_pd(aoffset6);
	  aoffset6 += 8;
	  row7 = _mm512_loadu_pd(aoffset7);
	  aoffset7 += 8;
	  row8 = _mm512_loadu_pd(aoffset8);
	  aoffset8 += 8;

	  _mm512_storeu_pd(boffset1 +  0, row1);
	  _mm512_storeu_pd(boffset1 +  8, row2);
	  _mm512_storeu_pd(boffset1 + 16, row3);
	  _mm512_storeu_pd(boffset1 + 24, row4);
	  _mm512_storeu_pd(boffset1 + 32, row5);
	  _mm512_storeu_pd(boffset1 + 40, row6);
	  _mm512_storeu_pd(boffset1 + 48, row7);
	  _mm512_storeu_pd(boffset1 + 56, row8);
	  boffset1 += m * 8;
	  i --;
	}while(i > 0);
      }

      if (n & 4){
	__m256d row1, row2, row3, row4, row5, row6, row7, row8;
	row1 = _mm256_loadu_pd(aoffset1);
	aoffset1 += 4;
	row2 = _mm256_loadu_pd(aoffset2);
	aoffset2 += 4;
	row3 = _mm256_loadu_pd(aoffset3);
	aoffset3 += 4;
	row4 = _mm256_loadu_pd(aoffset4);
	aoffset4 += 4;
	row5 = _mm256_loadu_pd(aoffset5);
	aoffset5 += 4;
	row6 = _mm256_loadu_pd(aoffset6);
	aoffset6 += 4;
	row7 = _mm256_loadu_pd(aoffset7);
	aoffset7 += 4;
	row8 = _mm256_loadu_pd(aoffset8);
	aoffset8 += 4;

	_mm256_storeu_pd(boffset2 +   0, row1);
	_mm256_storeu_pd(boffset2 +   4, row2);
	_mm256_storeu_pd(boffset2 +   8, row3);
	_mm256_storeu_pd(boffset2 +  12, row4);
	_mm256_storeu_pd(boffset2 +  16, row5);
	_mm256_storeu_pd(boffset2 +  20, row6);
	_mm256_storeu_pd(boffset2 +  24, row7);
	_mm256_storeu_pd(boffset2 +  28, row8);
	boffset2 += 32;
      }

      if (n & 2){
	__m128d row1, row2, row3, row4, row5, row6, row7, row8;
	row1 = _mm_loadu_pd(aoffset1);
	aoffset1 += 2;

	row2 = _mm_loadu_pd(aoffset2);
	aoffset2 += 2;

	row3 = _mm_loadu_pd(aoffset3);
	aoffset3 += 2;

	row4 = _mm_loadu_pd(aoffset4);
	aoffset4 += 2;

	row5 = _mm_loadu_pd(aoffset5);
	aoffset5 += 2;

	row6 = _mm_loadu_pd(aoffset6);
	aoffset6 += 2;

	row7 = _mm_loadu_pd(aoffset7);
	aoffset7 += 2;

	row8 = _mm_loadu_pd(aoffset8);
	aoffset8 += 2;

	_mm_storeu_pd(boffset3 +   0, row1);
	_mm_storeu_pd(boffset3 +   2, row2);
	_mm_storeu_pd(boffset3 +   4, row3);
	_mm_storeu_pd(boffset3 +   6, row4);
	_mm_storeu_pd(boffset3 +   8, row5);
	_mm_storeu_pd(boffset3 +  10, row6);
	_mm_storeu_pd(boffset3 +  12, row7);
	_mm_storeu_pd(boffset3 +  14, row8);
	boffset3 += 16;
      }

      if (n & 1){
	ctemp01 = *(aoffset1 + 0);
	aoffset1 ++;
	ctemp02 = *(aoffset2 + 0);
	aoffset2 ++;
	ctemp03 = *(aoffset3 + 0);
	aoffset3 ++;
	ctemp04 = *(aoffset4 + 0);
	aoffset4 ++;
	ctemp05 = *(aoffset5 + 0);
	aoffset5 ++;
	ctemp06 = *(aoffset6 + 0);
	aoffset6 ++;
	ctemp07 = *(aoffset7 + 0);
	aoffset7 ++;
	ctemp08 = *(aoffset8 + 0);
	aoffset8 ++;

	*(boffset4 +  0) = ctemp01;
	*(boffset4 +  1) = ctemp02;
	*(boffset4 +  2) = ctemp03;
	*(boffset4 +  3) = ctemp04;
	*(boffset4 +  4) = ctemp05;
	*(boffset4 +  5) = ctemp06;
	*(boffset4 +  6) = ctemp07;
	*(boffset4 +  7) = ctemp08;
	boffset4 += 8;
      }

      j--;
    }while(j > 0);
  }

  if (m & 4){

    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset3  = aoffset2 + lda;
    aoffset4  = aoffset3 + lda;
    aoffset += 4 * lda;

    boffset1  = boffset;
    boffset  += 32;

    i = (n >> 3);
    if (i > 0){

      do{
	  __m512d row1, row2, row3, row4;
	  row1 = _mm512_loadu_pd(aoffset1);
	  aoffset1 += 8;
	  row2 = _mm512_loadu_pd(aoffset2);
	  aoffset2 += 8;
	  row3 = _mm512_loadu_pd(aoffset3);
	  aoffset3 += 8;
	  row4 = _mm512_loadu_pd(aoffset4);
	  aoffset4 += 8;

	  _mm512_storeu_pd(boffset1 +  0, row1);
	  _mm512_storeu_pd(boffset1 +  8, row2);
	  _mm512_storeu_pd(boffset1 + 16, row3);
	  _mm512_storeu_pd(boffset1 + 24, row4);

	  boffset1 += 8 * m;
	  i --;
      }while(i > 0);
    }

    if (n & 4) {
	__m256d row1, row2, row3, row4;
	row1 = _mm256_loadu_pd(aoffset1);
	aoffset1 += 4;
	row2 = _mm256_loadu_pd(aoffset2);
	aoffset2 += 4;
	row3 = _mm256_loadu_pd(aoffset3);
	aoffset3 += 4;
	row4 = _mm256_loadu_pd(aoffset4);
	aoffset4 += 4;
	_mm256_storeu_pd(boffset2 +   0, row1);
	_mm256_storeu_pd(boffset2 +   4, row2);
	_mm256_storeu_pd(boffset2 +   8, row3);
	_mm256_storeu_pd(boffset2 +  12, row4);
        boffset2 += 16;
    }

    if (n & 2){
	__m128d row1, row2, row3, row4;
	row1 = _mm_loadu_pd(aoffset1);
	aoffset1 += 2;

	row2 = _mm_loadu_pd(aoffset2);
	aoffset2 += 2;

	row3 = _mm_loadu_pd(aoffset3);
	aoffset3 += 2;

	row4 = _mm_loadu_pd(aoffset4);
	aoffset4 += 2;


	_mm_storeu_pd(boffset3 +   0, row1);
	_mm_storeu_pd(boffset3 +   2, row2);
	_mm_storeu_pd(boffset3 +   4, row3);
	_mm_storeu_pd(boffset3 +   6, row4);
        boffset3 += 8;
    }

    if (n & 1){
      ctemp01 = *(aoffset1 + 0);
      aoffset1 ++;
      ctemp02 = *(aoffset2 + 0);
      aoffset2 ++;
      ctemp03 = *(aoffset3 + 0);
      aoffset3 ++;
      ctemp04 = *(aoffset4 + 0);
      aoffset4 ++;

      *(boffset4 +  0) = ctemp01;
      *(boffset4 +  1) = ctemp02;
      *(boffset4 +  2) = ctemp03;
      *(boffset4 +  3) = ctemp04;
      boffset4 += 4;
    }
  }

  if (m & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    boffset1  = boffset;
    boffset  += 16;

    i = (n >> 3);
    if (i > 0){
      do{
	  __m512d row1, row2;
	  row1 = _mm512_loadu_pd(aoffset1);
	  aoffset1 += 8;
	  row2 = _mm512_loadu_pd(aoffset2);
	  aoffset2 += 8;

	  _mm512_storeu_pd(boffset1 +  0, row1);
	  _mm512_storeu_pd(boffset1 +  8, row2);
	  boffset1 += 8 * m;
	  i --;
      }while(i > 0);
    }

    if (n & 4){
	__m256d row1, row2;
	row1 = _mm256_loadu_pd(aoffset1);
	aoffset1 += 4;
	row2 = _mm256_loadu_pd(aoffset2);
	aoffset2 += 4;
	_mm256_storeu_pd(boffset2 +   0, row1);
	_mm256_storeu_pd(boffset2 +   4, row2);
        boffset2 += 8;
    }

    if (n & 2){
	__m128d row1, row2;
	row1 = _mm_loadu_pd(aoffset1);
	aoffset1 += 2;

	row2 = _mm_loadu_pd(aoffset2);
	aoffset2 += 2;


	_mm_storeu_pd(boffset3 +   0, row1);
	_mm_storeu_pd(boffset3 +   2, row2);
       boffset3 += 4;
    }

    if (n & 1){
      ctemp01 = *(aoffset1 + 0);
      aoffset1 ++;
      ctemp02 = *(aoffset2 + 0);
      aoffset2 ++;

      *(boffset4 +  0) = ctemp01;
      *(boffset4 +  1) = ctemp02;
      boffset4 += 2;
    }
  }

  if (m & 1){
    aoffset1  = aoffset;
    // aoffset += lda;

    boffset1  = boffset;
    // boffset  += 8;

    i = (n >> 3);
    if (i > 0){
      do{
	__m512d row1;
	  row1 = _mm512_loadu_pd(aoffset1);
	  aoffset1 += 8;

	  _mm512_storeu_pd(boffset1 +  0, row1);
  	  boffset1 += 8 * m;
	  i --;
       }while(i > 0);
     }

     if (n & 4){
	__m256d row1;
	row1 = _mm256_loadu_pd(aoffset1);
	aoffset1 += 4;
	_mm256_storeu_pd(boffset2 +   0, row1);
       // boffset2 += 4;
     }

     if (n & 2){
	__m128d row1;
	row1 = _mm_loadu_pd(aoffset1);
	aoffset1 += 2;

	_mm_storeu_pd(boffset3 +   0, row1);

       // boffset3 += 2;
     }

     if (n & 1){
       ctemp01 = *(aoffset1 + 0);
       aoffset1 ++;
      *(boffset4 +  0) = ctemp01;
      boffset4 ++;
    }
  }

  return 0;
}
