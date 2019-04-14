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
#include <stdlib.h>
#include <ctype.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1,
	  FLOAT beta_r, FLOAT beta_i,
	  FLOAT *dummy2, BLASLONG dummy3,
	  FLOAT *dummy4, BLASLONG dummy5,
	  FLOAT *c, BLASLONG ldc){
  BLASLONG i, j;

  FLOAT *c_offset, *c_offset1;
  FLOAT atemp1, atemp2, atemp3, atemp4;
  FLOAT btemp1, btemp2, btemp3, btemp4;
  FLOAT ctemp1, ctemp2, ctemp3, ctemp4;

  ldc *= 2;

  c_offset = c;

  if (beta_r == 0. && beta_i == 0.) {
    j = n;
    do {
      c_offset1 = c_offset;
      c_offset += ldc;

      i = (m >> 2);
      if (i > 0){
	do {
	  *(c_offset1 + 0) = ZERO;
	  *(c_offset1 + 1) = ZERO;
	  *(c_offset1 + 2) = ZERO;
	  *(c_offset1 + 3) = ZERO;
	  *(c_offset1 + 4) = ZERO;
	  *(c_offset1 + 5) = ZERO;
	  *(c_offset1 + 6) = ZERO;
	  *(c_offset1 + 7) = ZERO;
	  c_offset1 += 8;
	  i--;
	} while (i > 0);
      }

      i = (m & 3);
      if (i > 0){
	do {
	  *(c_offset1 + 0) = ZERO;
	  *(c_offset1 + 1) = ZERO;
	  c_offset1 += 2;
	  i--;
	} while (i > 0);
      }
      j --;
    } while (j > 0);

  } else {

    j = n;
    do {
      c_offset1 = c_offset;
      c_offset += ldc;

      i = (m >> 1);
      if (i > 0){
	do {
	  atemp1 = *(c_offset1 + 0);
	  atemp2 = *(c_offset1 + 1);
	  atemp3 = *(c_offset1 + 2);
	  atemp4 = *(c_offset1 + 3);

	  btemp1 = beta_r * atemp1;
	  btemp2 = beta_i * atemp2;
	  btemp3 = beta_r * atemp2;
	  btemp4 = beta_i * atemp1;

	  ctemp1 = btemp1 - btemp2;
	  ctemp2 = btemp3 + btemp4;

	  btemp1 = beta_r * atemp3;
	  btemp2 = beta_i * atemp4;
	  btemp3 = beta_r * atemp4;
	  btemp4 = beta_i * atemp3;

	  ctemp3 = btemp1 - btemp2;
	  ctemp4 = btemp3 + btemp4;

	  *(c_offset1 + 0) = ctemp1;
	  *(c_offset1 + 1) = ctemp2;
	  *(c_offset1 + 2) = ctemp3;
	  *(c_offset1 + 3) = ctemp4;
	  c_offset1 += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 1);
      if (i > 0){
	do {
	  atemp1 = *(c_offset1 + 0);
	  atemp2 = *(c_offset1 + 1);

	  btemp1 = beta_r * atemp1;
	  btemp2 = beta_i * atemp2;
	  btemp3 = beta_r * atemp2;
	  btemp4 = beta_i * atemp1;

	  ctemp1 = btemp1 - btemp2;
	  ctemp2 = btemp3 + btemp4;

	  *(c_offset1 + 0) = ctemp1;
	  *(c_offset1 + 1) = ctemp2;
	  c_offset1 += 2;
	  i --;
	} while (i > 0);
      }
      j --;
    } while (j > 0);
  }
  return 0;
}
