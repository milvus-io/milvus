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

#ifndef UNIT
#define INV(a) (ONE / (a))
#else
#define INV(a) (ONE)
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj;

  FLOAT data01, data02, data03, data04, data05, data06, data07, data08;
  FLOAT data09, data10, data11, data12, data13, data14, data15, data16;
  FLOAT *a1, *a2, *a3, *a4;

  jj = offset;

  j = (n >> 2);
  while (j > 0){

    a1 = a + 0 * lda;
    a2 = a + 1 * lda;
    a3 = a + 2 * lda;
    a4 = a + 3 * lda;

    i = (m >> 2);
    ii = 0;
    while (i > 0) {

      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif

	data05 = *(a2 + 0);
#ifndef UNIT
	data06 = *(a2 + 1);
#endif

	data09 = *(a3 + 0);
	data10 = *(a3 + 1);
#ifndef UNIT
	data11 = *(a3 + 2);
#endif

	data13 = *(a4 + 0);
	data14 = *(a4 + 1);
	data15 = *(a4 + 2);
#ifndef UNIT
	data16 = *(a4 + 3);
#endif

	*(b +  0) = INV(data01);
	*(b +  1) = data05;
	*(b +  2) = data09;
	*(b +  3) = data13;

	*(b +  5) = INV(data06);
	*(b +  6) = data10;
	*(b +  7) = data14;

	*(b + 10) = INV(data11);
	*(b + 11) = data15;

	*(b + 15) = INV(data16);
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

	data09 = *(a3 + 0);
	data10 = *(a3 + 1);
	data11 = *(a3 + 2);
	data12 = *(a3 + 3);

	data13 = *(a4 + 0);
	data14 = *(a4 + 1);
	data15 = *(a4 + 2);
	data16 = *(a4 + 3);

	*(b +  0) = data01;
	*(b +  1) = data05;
	*(b +  2) = data09;
	*(b +  3) = data13;
	*(b +  4) = data02;
	*(b +  5) = data06;
	*(b +  6) = data10;
	*(b +  7) = data14;

	*(b +  8) = data03;
	*(b +  9) = data07;
	*(b + 10) = data11;
	*(b + 11) = data15;
	*(b + 12) = data04;
	*(b + 13) = data08;
	*(b + 14) = data12;
	*(b + 15) = data16;
      }

      a1 += 4;
      a2 += 4;
      a3 += 4;
      a4 += 4;
      b += 16;

      i  --;
      ii += 4;
    }

    if ((m & 2) != 0) {

      if (ii== jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif

	data05 = *(a2 + 0);
#ifndef UNIT
	data06 = *(a2 + 1);
#endif

	data09 = *(a3 + 0);
	data10 = *(a3 + 1);

	data13 = *(a4 + 0);
	data14 = *(a4 + 1);

	*(b +  0) = INV(data01);
	*(b +  1) = data05;
	*(b +  2) = data09;
	*(b +  3) = data13;

	*(b +  5) = INV(data06);
	*(b +  6) = data10;
	*(b +  7) = data14;
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a2 + 0);
	data04 = *(a2 + 1);
	data05 = *(a3 + 0);
	data06 = *(a3 + 1);
	data07 = *(a4 + 0);
	data08 = *(a4 + 1);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
	*(b +  6) = data07;
	*(b +  7) = data08;
      }

      a1 += 2;
      a2 += 2;
      b += 8;

      ii += 2;
    }

    if ((m & 1) != 0) {

      if (ii== jj) {
#ifndef UNIT
	data01 = *(a1 + 0);
#endif

	data05 = *(a2 + 0);
	data09 = *(a3 + 0);
	data13 = *(a4 + 0);

	*(b +  0) = INV(data01);
	*(b +  1) = data05;
	*(b +  2) = data09;
	*(b +  3) = data13;
      }

      if (ii < jj)  {
	data01 = *(a1 + 0);
	data02 = *(a2 + 0);
	data03 = *(a3 + 0);
	data04 = *(a4 + 0);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
      }
      b += 4;
    }

    a += 4 * lda;
    jj += 4;
    j  --;
  }

  if (n & 2) {
    a1 = a + 0 * lda;
    a2 = a + 1 * lda;

    i = (m >> 1);
    ii = 0;
    while (i > 0) {

      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif

	data03 = *(a2 + 0);
#ifndef UNIT
	data04 = *(a2 + 1);
#endif

	*(b +  0) = INV(data01);
	*(b +  1) = data03;
	*(b +  3) = INV(data04);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a2 + 0);
	data04 = *(a2 + 1);

	*(b +  0) = data01;
	*(b +  1) = data03;
	*(b +  2) = data02;
	*(b +  3) = data04;
      }

      a1 += 2;
      a2 += 2;
      b += 4;

      i  --;
      ii += 2;
    }

    if ((m & 1) != 0) {

      if (ii== jj) {


#ifndef UNIT
	data01 = *(a1 + 0);
#endif

	data03 = *(a2 + 0);

	*(b +  0) = INV(data01);
	*(b +  1) = data03;
      }

      if (ii < jj)  {
	data01 = *(a1 + 0);
	data02 = *(a2 + 0);
	*(b +  0) = data01;
	*(b +  1) = data02;
      }
      b += 2;
    }
    a += 2 * lda;
    jj += 2;
  }

  if (n & 1) {
    a1 = a + 0 * lda;

    i = m;
    ii = 0;
    while (i > 0) {

      if (ii == jj) {
#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	*(b +  0) = INV(data01);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	*(b +  0) = data01;
      }

      a1+= 1;
      b += 1;
      i  --;
      ii += 1;
    }
  }

  return 0;
}
