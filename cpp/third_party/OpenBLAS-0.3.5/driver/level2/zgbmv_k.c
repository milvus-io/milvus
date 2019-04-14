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
#include <ctype.h>
#include "common.h"

#ifndef XCONJ
#ifndef CONJ
#define ZAXPY	AXPYU_K
#define ZDOT	DOTU_K
#else
#define ZAXPY	AXPYC_K
#define ZDOT	DOTC_K
#endif
#else
#ifndef CONJ
#define ZAXPY	AXPYU_K
#define ZDOT	DOTC_K
#else
#define ZAXPY	AXPYC_K
#define ZDOT	DOTU_K
#endif
#endif

#ifndef TRANS
#define M m
#define N n
#else
#define N m
#define M n
#endif

void CNAME(BLASLONG m, BLASLONG n, BLASLONG ku, BLASLONG kl, FLOAT alpha_r, FLOAT alpha_i,
	  FLOAT *a, BLASLONG lda,
	  FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, void *buffer){

  BLASLONG i, offset_u, offset_l, start, end, length;
  FLOAT *X = x;
  FLOAT *Y = y;
  FLOAT *gemvbuffer = (FLOAT *)buffer;
  FLOAT *bufferY    = gemvbuffer;
  FLOAT *bufferX    = gemvbuffer;
#ifdef TRANS
  OPENBLAS_COMPLEX_FLOAT temp;
#endif

  if (incy != 1) {
    Y = bufferY;
    bufferX    = (FLOAT *)(((BLASLONG)bufferY + M * sizeof(FLOAT) * 2 + 4095) & ~4095);
    // gemvbuffer = bufferX;
    COPY_K(M, y, incy, Y, 1);
  }

  if (incx != 1) {
    X = bufferX;
    // gemvbuffer = (FLOAT *)(((BLASLONG)bufferX + N * sizeof(FLOAT) * 2 + 4095) & ~4095);
    COPY_K(N, x, incx, X, 1);
  }

  offset_u = ku;
  offset_l = ku + m;

  for (i = 0; i < MIN(n, m + ku); i++) {

    start = MAX(offset_u, 0);
    end   = MIN(offset_l, ku + kl + 1);

    length  = end - start;

#ifndef TRANS
    ZAXPY(length, 0, 0,
#ifndef XCONJ
	  alpha_r * X[i * 2 + 0] - alpha_i * X[i * 2 + 1],
	  alpha_i * X[i * 2 + 0] + alpha_r * X[i * 2 + 1],
#else
	  alpha_r * X[i * 2 + 0] + alpha_i * X[i * 2 + 1],
	  alpha_i * X[i * 2 + 0] - alpha_r * X[i * 2 + 1],
#endif
	  a + start * 2, 1, Y + (start - offset_u) * 2, 1, NULL, 0);
#else

#ifndef XCONJ
    temp = ZDOT(length, a + start * 2, 1, X + (start - offset_u) * 2, 1);
#else
    temp = ZDOT(length, X + (start - offset_u) * 2, 1, a + start * 2, 1);
#endif

#if !defined(XCONJ) || !defined(CONJ)
    Y[i * 2 + 0] += alpha_r * CREAL(temp) - alpha_i * CIMAG(temp);
    Y[i * 2 + 1] += alpha_i * CREAL(temp) + alpha_r * CIMAG(temp);
#else
    Y[i * 2 + 0] += alpha_r * CREAL(temp) + alpha_i * CIMAG(temp);
    Y[i * 2 + 1] += alpha_i * CREAL(temp) - alpha_r * CIMAG(temp);
#endif
#endif

    offset_u --;
    offset_l --;

    a += lda * 2;
  }

  if (incy != 1) {
    COPY_K(M, Y, 1, y, incy);
  }

  return;
}

