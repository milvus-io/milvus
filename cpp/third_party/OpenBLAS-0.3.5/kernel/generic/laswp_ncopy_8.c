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

#define PREFETCHSIZE 4

int CNAME(BLASLONG n, BLASLONG k1, BLASLONG k2, FLOAT *a, BLASLONG lda, blasint *ipiv, FLOAT *buffer){

  BLASLONG i, j, ip;
  blasint *piv;
  FLOAT *dx1, *dy1;
  FLOAT *dx2, *dy2;
  FLOAT *dx3, *dy3;
  FLOAT *dx4, *dy4;
  FLOAT *dx5, *dy5;
  FLOAT *dx6, *dy6;
  FLOAT *dx7, *dy7;
  FLOAT *dx8, *dy8;
  FLOAT atemp1, btemp1;
  FLOAT atemp2, btemp2;
  FLOAT atemp3, btemp3;
  FLOAT atemp4, btemp4;
  FLOAT atemp5, btemp5;
  FLOAT atemp6, btemp6;
  FLOAT atemp7, btemp7;
  FLOAT atemp8, btemp8;

  a--;
  ipiv += k1 - 1;

  if (n  <= 0) return 0;
  if (k1 > k2) return 0;

  j = (n >> 3);
  if (j > 0) {
    do {
      piv = ipiv;
      i = k1;

    do {
      ip = *piv;
      piv ++;

      dx1 = a + i;
      dy1 = a + ip;
      dx2 = a + i  + lda * 1;
      dy2 = a + ip + lda * 1;
      dx3 = a + i  + lda * 2;
      dy3 = a + ip + lda * 2;
      dx4 = a + i  + lda * 3;
      dy4 = a + ip + lda * 3;
      dx5 = a + i  + lda * 4;
      dy5 = a + ip + lda * 4;
      dx6 = a + i  + lda * 5;
      dy6 = a + ip + lda * 5;
      dx7 = a + i  + lda * 6;
      dy7 = a + ip + lda * 6;
      dx8 = a + i  + lda * 7;
      dy8 = a + ip + lda * 7;

#ifdef __GNUC__
      __builtin_prefetch(dx1 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx2 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx3 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx4 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx5 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx6 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx7 + PREFETCHSIZE, 0, 1);
      __builtin_prefetch(dx8 + PREFETCHSIZE, 0, 1);
#endif

      atemp1 = *dx1;
      btemp1 = *dy1;
      atemp2 = *dx2;
      btemp2 = *dy2;
      atemp3 = *dx3;
      btemp3 = *dy3;
      atemp4 = *dx4;
      btemp4 = *dy4;

      atemp5 = *dx5;
      btemp5 = *dy5;
      atemp6 = *dx6;
      btemp6 = *dy6;
      atemp7 = *dx7;
      btemp7 = *dy7;
      atemp8 = *dx8;
      btemp8 = *dy8;

      if (ip != i) {
	*dy1 = atemp1;
	*dy2 = atemp2;
	*dy3 = atemp3;
	*dy4 = atemp4;
	*dy5 = atemp5;
	*dy6 = atemp6;
	*dy7 = atemp7;
	*dy8 = atemp8;
	*(buffer + 0) = btemp1;
	*(buffer + 1) = btemp2;
	*(buffer + 2) = btemp3;
	*(buffer + 3) = btemp4;
	*(buffer + 4) = btemp5;
	*(buffer + 5) = btemp6;
	*(buffer + 6) = btemp7;
	*(buffer + 7) = btemp8;
      } else {
	*(buffer + 0) = atemp1;
	*(buffer + 1) = atemp2;
	*(buffer + 2) = atemp3;
	*(buffer + 3) = atemp4;
	*(buffer + 4) = atemp5;
	*(buffer + 5) = atemp6;
	*(buffer + 6) = atemp7;
	*(buffer + 7) = atemp8;
      }

      buffer += 8;

      i++;
    } while (i <= k2);

      a += 8 * lda;
      j --;
    } while (j > 0);
  }

  if (n & 4) {
    piv = ipiv;

      ip = *piv;
      piv ++;

      dx1 = a + k1;
      dy1 = a + ip;
      dx2 = a + k1 + lda * 1;
      dy2 = a + ip + lda * 1;
      dx3 = a + k1 + lda * 2;
      dy3 = a + ip + lda * 2;
      dx4 = a + k1 + lda * 3;
      dy4 = a + ip + lda * 3;

    i = k1;

    do {
      atemp1 = *dx1;
      atemp2 = *dx2;
      atemp3 = *dx3;
      atemp4 = *dx4;

      btemp1 = *dy1;
      btemp2 = *dy2;
      btemp3 = *dy3;
      btemp4 = *dy4;

      if (ip != i) {
	*dy1 = atemp1;
	*dy2 = atemp2;
	*dy3 = atemp3;
	*dy4 = atemp4;
	*(buffer + 0) = btemp1;
	*(buffer + 1) = btemp2;
	*(buffer + 2) = btemp3;
	*(buffer + 3) = btemp4;
      } else {
	*(buffer + 0) = atemp1;
	*(buffer + 1) = atemp2;
	*(buffer + 2) = atemp3;
	*(buffer + 3) = atemp4;
      }

      ip = *piv;
      piv ++;

      i++;
      dx1 = a + i;
      dy1 = a + ip;
      dx2 = a + i  + lda * 1;
      dy2 = a + ip + lda * 1;
      dx3 = a + i  + lda * 2;
      dy3 = a + ip + lda * 2;
      dx4 = a + i  + lda * 3;
      dy4 = a + ip + lda * 3;

      buffer += 4;

    } while (i <= k2);

      a += 4 * lda;
  }

  if (n & 2) {
    piv = ipiv;

    i = k1;
    do {
      ip = *piv;
      piv ++;

      dx1 = a + i;
      dy1 = a + ip;
      dx2 = a + i  + lda;
      dy2 = a + ip + lda;

      atemp1 = *dx1;
      btemp1 = *dy1;
      atemp2 = *dx2;
      btemp2 = *dy2;

      if (ip != i) {
	*dy1 = atemp1;
	*dy2 = atemp2;
	*(buffer + 0) = btemp1;
	*(buffer + 1) = btemp2;
      } else {
	*(buffer + 0) = atemp1;
	*(buffer + 1) = atemp2;
      }

      buffer += 2;

      i++;
    } while (i <= k2);

    a += 2 * lda;
  }


  if (n & 1) {
    piv = ipiv;

    i = k1;
    do {
      ip = *piv;
      piv ++;

      dx1 = a + i;
      dy1 = a + ip;
      atemp1 = *dx1;
      btemp1 = *dy1;

      if (ip != i) {
	*dy1 = atemp1;
	*buffer = btemp1;
      } else {
	*buffer = atemp1;
      }

      buffer ++;

      i++;
    } while (i <= k2);

    // a += lda;
  }

  return 0;
}

