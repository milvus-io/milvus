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
  FLOAT *a_offset, *a_offset1, *a_offset2;
  FLOAT *b_offset;

  a_offset = a;
  b_offset = b;

  j = (n >> 1);

  if (j > 0){
    do {
      a_offset1 = a_offset;
      a_offset2 = a_offset + lda;
      a_offset += 2 * lda;

      i = (m >> 2);

      if (i > 0){
	do {
	  *(b_offset + 0) = *(a_offset1 + 0);
	  *(b_offset + 1) = *(a_offset2 + 0);
	  *(b_offset + 2) = *(a_offset1 + 1);
	  *(b_offset + 3) = *(a_offset2 + 1);
	  *(b_offset + 4) = *(a_offset1 + 2);
	  *(b_offset + 5) = *(a_offset2 + 2);
	  *(b_offset + 6) = *(a_offset1 + 3);
	  *(b_offset + 7) = *(a_offset2 + 3);
	  a_offset1 += 4;
	  a_offset2 += 4;
	  b_offset += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);

      if (i > 0){
	do {
	  *(b_offset + 0) = *(a_offset1 + 0);
	  *(b_offset + 1) = *(a_offset2 + 0);
	  a_offset1 ++;
	  a_offset2 ++;
	  b_offset += 2;
	  i --;
	} while (i > 0);
      }
      j --;
    } while (j > 0);
  }

  if (n & 1){

    i = (m >> 3);
    if (i > 0){
      do {
	*(b_offset + 0) = *(a_offset + 0);
	*(b_offset + 1) = *(a_offset + 1);
	*(b_offset + 2) = *(a_offset + 2);
	*(b_offset + 3) = *(a_offset + 3);
	*(b_offset + 4) = *(a_offset + 4);
	*(b_offset + 5) = *(a_offset + 5);
	*(b_offset + 6) = *(a_offset + 6);
	*(b_offset + 7) = *(a_offset + 7);
	a_offset += 8;
	b_offset += 8;
	i --;
      } while (i > 0);
    }

    i = (m & 7);

    if (i > 0){
      do {
	*(b_offset + 0) = *(a_offset + 0);
	a_offset ++;
	b_offset ++;
	i --;
      } while (i > 0);
    }
  }

  return 0;
}

