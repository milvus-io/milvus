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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj;

  FLOAT data01 = 0.0, data02 = 0.0, data03, data04;
  FLOAT data05, data06, data07 = 0.0, data08 = 0.0;
  FLOAT *a1, *a2;

  lda *= 2;

  jj = offset;

  j = (n >> 1);
  while (j > 0){

    a1 = a + 0 * lda;
    a2 = a + 1 * lda;

    i = (m >> 1);
    ii = 0;
    while (i > 0) {

      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
#endif

	data05 = *(a2 + 0);
	data06 = *(a2 + 1);
#ifndef UNIT
	data07 = *(a2 + 2);
	data08 = *(a2 + 3);
#endif

	compinv(b + 0, data01, data02);
	*(b +  2) = data05;
	*(b +  3) = data06;
	compinv(b + 6, data07, data08);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a2 + 0);
	data06 = *(a2 + 1);
	data07 = *(a2 + 2);
	data08 = *(a2 + 3);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data05;
	*(b +  3) = data06;
	*(b +  4) = data03;
	*(b +  5) = data04;
	*(b +  6) = data07;
	*(b +  7) = data08;
      }

      a1 += 4;
      a2 += 4;
      b += 8;

      i  --;
      ii += 2;
    }

    if (m & 1) {

      if (ii== jj) {
#ifndef UNIT
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
#endif
	data05 = *(a2 + 0);
	data06 = *(a2 + 1);

	compinv(b + 0, data01, data02);
	*(b +  2) = data05;
	*(b +  3) = data06;
      }

      if (ii < jj)  {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a2 + 0);
	data04 = *(a2 + 1);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
      }
      b += 4;
    }

    a += 2 * lda;
    jj += 2;
    j  --;
  }

  if (n & 1) {
    a1 = a + 0 * lda;

    i = m;
    ii = 0;
    while (i > 0) {

      if (ii == jj) {
#ifndef UNIT
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
#endif
	compinv(b + 0, data01, data02);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	*(b +  0) = data01;
	*(b +  1) = data02;
      }

      a1+= 2;
      b += 2;
      i  --;
      ii += 1;
    }
  }

  return 0;
}
