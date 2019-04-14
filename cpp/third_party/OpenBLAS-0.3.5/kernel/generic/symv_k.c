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
#include "symcopy.h"

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda,
	  FLOAT *x, BLASLONG incx, FLOAT *y, BLASLONG incy, FLOAT *buffer){

  BLASLONG is, min_i;
  FLOAT *X = x;
  FLOAT *Y = y;
  FLOAT *symbuffer  = buffer;
  FLOAT *gemvbuffer = (FLOAT *)(((BLASLONG)buffer + SYMV_P * SYMV_P * sizeof(FLOAT) + 4095) & ~4095);
  FLOAT *bufferY    = gemvbuffer;
  FLOAT *bufferX    = gemvbuffer;

  if (incy != 1) {
    Y = bufferY;
    bufferX    = (FLOAT *)(((BLASLONG)bufferY + m * sizeof(FLOAT) + 4095) & ~4095);
    gemvbuffer = bufferX;
    COPY_K(m, y, incy, Y, 1);
  }

  if (incx != 1) {
    X = bufferX;
    gemvbuffer = (FLOAT *)(((BLASLONG)bufferX + m * sizeof(FLOAT) + 4095) & ~4095);
    COPY_K(m, x, incx, X, 1);
  }

#ifndef LOWER
  for(is = m - offset; is < m; is += SYMV_P){
    min_i = MIN(m - is, SYMV_P);
#else
  for(is = 0; is < offset; is += SYMV_P){
    min_i = MIN(offset - is, SYMV_P);
#endif

#ifndef LOWER
    if (is >0){
      GEMV_T(is, min_i, 0, alpha,
	     a + is * lda, lda,
	     X,            1,
	     Y + is,       1, gemvbuffer);

      GEMV_N(is, min_i, 0, alpha,
	     a + is * lda,  lda,
	     X + is, 1,
	     Y,      1, gemvbuffer);
    }
#endif

#ifdef LOWER
    SYMCOPY_L(min_i, a + is + is * lda, lda, symbuffer);
#else
    SYMCOPY_U(min_i, a + is + is * lda, lda, symbuffer);
#endif

    GEMV_N(min_i, min_i, 0, alpha,
	   symbuffer, min_i,
	   X + is, 1,
	   Y + is, 1, gemvbuffer);

#ifdef LOWER
    if (m - is >  min_i){
      GEMV_T(m - is - min_i, min_i, 0, alpha,
	     a + (is + min_i) + is * lda, lda,
	     X + (is + min_i), 1,
	     Y + is,            1, gemvbuffer);

      GEMV_N(m - is - min_i, min_i, 0, alpha,
	     a + (is + min_i) + is * lda, lda,
	     X +  is,            1,
	     Y + (is + min_i),  1, gemvbuffer);
    }
#endif

  } /* end of is */

  if (incy != 1) {
    COPY_K(m, Y, 1, y, incy);
  }

  return 0;
}

