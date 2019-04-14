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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b){

  BLASLONG i, j;
  FLOAT *a_offset;
  FLOAT *b_offset, *b_offset1;
  FLOAT  ctemp1,  ctemp2,  ctemp3,  ctemp4;
  FLOAT  ctemp5,  ctemp6,  ctemp7,  ctemp8;

  a_offset = a;
  b_offset = b;

  lda *= 2;

  j = m;

  m *= 2;

  if (j > 0){
    do {
      b_offset1 = b_offset;
      b_offset += 2;

      i = (n >> 2);
      if (i > 0){
	do{
	  ctemp1  = *(a_offset +  0);
	  ctemp2  = *(a_offset +  1);
	  ctemp3  = *(a_offset +  2);
	  ctemp4  = *(a_offset +  3);

	  ctemp5  = *(a_offset +  4);
	  ctemp6  = *(a_offset +  5);
	  ctemp7  = *(a_offset +  6);
	  ctemp8  = *(a_offset +  7);

	  *(b_offset1 + 0) = -ctemp1;
	  *(b_offset1 + 1) = -ctemp2;

	  b_offset1 += m;

	  *(b_offset1 + 0) = -ctemp3;
	  *(b_offset1 + 1) = -ctemp4;

	  b_offset1 += m;

	  *(b_offset1 + 0) = -ctemp5;
	  *(b_offset1 + 1) = -ctemp6;

	  b_offset1 += m;

	  *(b_offset1 + 0) = -ctemp7;
	  *(b_offset1 + 1) = -ctemp8;

	  b_offset1 += m;
	  a_offset += 8;
	  i --;
	} while(i>0);
      }

      i = (n & 3);
      if (i > 0){
	do {
	  ctemp1  = *(a_offset +  0);
	  ctemp2  = *(a_offset +  1);

	  *(b_offset1 + 0) = -ctemp1;
	  *(b_offset1 + 1) = -ctemp2;

	  b_offset1 += m;
	  a_offset += 2;
	  i --;
	} while(i > 0);
      }
      a_offset += lda - n * 2;
      j --;
    } while (j > 0);
  }

  return 0;
}
