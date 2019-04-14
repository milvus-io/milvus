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

  FLOAT *a_offset, *a_offset1, *a_offset2, *a_offset3, *a_offset4;
  FLOAT *b_offset;
  FLOAT  ctemp1,  ctemp2,  ctemp3,  ctemp4;
  FLOAT  ctemp5,  ctemp6,  ctemp7,  ctemp8;
  FLOAT  ctemp9,  ctemp13;

  a_offset = a;
  b_offset = b;

  j = (n >> 2);
  if (j > 0){
    do{
      a_offset1  = a_offset;
      a_offset2  = a_offset1 + lda;
      a_offset3  = a_offset2 + lda;
      a_offset4  = a_offset3 + lda;
      a_offset += 4 * lda;

      i = (m >> 2);
      if (i > 0){
	do{
	  __m128 row0, row1, row2, row3;

	  row0 = _mm_loadu_ps(a_offset1);
	  row1 = _mm_loadu_ps(a_offset2);
	  row2 = _mm_loadu_ps(a_offset3);
	  row3 = _mm_loadu_ps(a_offset4);

  	  _MM_TRANSPOSE4_PS(row0, row1, row2, row3);

	  _mm_storeu_ps(b_offset +  0, row0);
	  _mm_storeu_ps(b_offset +  4, row1);
	  _mm_storeu_ps(b_offset +  8, row2);
	  _mm_storeu_ps(b_offset + 12, row3);

	  a_offset1 += 4;
	  a_offset2 += 4;
	  a_offset3 += 4;
	  a_offset4 += 4;

	  b_offset += 16;
	  i --;
	}while(i > 0);
      }

      i = (m & 3);
      if (i > 0){
	do{
	  ctemp1  = *(a_offset1 + 0);
	  ctemp5  = *(a_offset2 + 0);
	  ctemp9  = *(a_offset3 + 0);
	  ctemp13 = *(a_offset4 + 0);

	  *(b_offset +  0) = ctemp1;
	  *(b_offset +  1) = ctemp5;
	  *(b_offset +  2) = ctemp9;
	  *(b_offset +  3) = ctemp13;

	  a_offset1 ++;
	  a_offset2 ++;
	  a_offset3 ++;
	  a_offset4 ++;

	  b_offset += 4;
	  i --;
	}while(i > 0);
      }
      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 2){
    a_offset1  = a_offset;
    a_offset2  = a_offset1 + lda;
    a_offset += 2 * lda;

    i = (m >> 2);
    if (i > 0){
      do{
	ctemp1  = *(a_offset1 + 0);
	ctemp2  = *(a_offset1 + 1);
	ctemp3  = *(a_offset1 + 2);
	ctemp4  = *(a_offset1 + 3);

	ctemp5  = *(a_offset2 + 0);
	ctemp6  = *(a_offset2 + 1);
	ctemp7  = *(a_offset2 + 2);
	ctemp8  = *(a_offset2 + 3);

	*(b_offset +  0) = ctemp1;
	*(b_offset +  1) = ctemp5;
	*(b_offset +  2) = ctemp2;
	*(b_offset +  3) = ctemp6;

	*(b_offset +  4) = ctemp3;
	*(b_offset +  5) = ctemp7;
	*(b_offset +  6) = ctemp4;
	*(b_offset +  7) = ctemp8;

	a_offset1 += 4;
	a_offset2 += 4;
	b_offset  += 8;
	i --;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0){
      do{
	ctemp1  = *(a_offset1 + 0);
	ctemp5  = *(a_offset2 + 0);

	*(b_offset +  0) = ctemp1;
	*(b_offset +  1) = ctemp5;

	a_offset1 ++;
	a_offset2 ++;
	b_offset += 2;
	i --;
      }while(i > 0);
    }
  } /* end of if(j > 0) */

  if (n & 1){
    a_offset1  = a_offset;

    i = (m >> 2);
    if (i > 0){
      do{
	ctemp1  = *(a_offset1 + 0);
	ctemp2  = *(a_offset1 + 1);
	ctemp3  = *(a_offset1 + 2);
	ctemp4  = *(a_offset1 + 3);

	*(b_offset +  0) = ctemp1;
	*(b_offset +  1) = ctemp2;
	*(b_offset +  2) = ctemp3;
	*(b_offset +  3) = ctemp4;

	a_offset1 += 4;
	b_offset  += 4;
	i --;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0){
      do{
	ctemp1  = *(a_offset1 + 0);
	*(b_offset +  0) = ctemp1;
	a_offset1 ++;
	b_offset += 1;
	i --;
      }while(i > 0);
    }
  } /* end of if(j > 0) */

  return 0;
}
