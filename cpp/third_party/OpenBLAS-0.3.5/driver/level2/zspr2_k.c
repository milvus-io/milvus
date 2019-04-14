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

int CNAME(BLASLONG m, FLOAT alpha_r, FLOAT alpha_i, FLOAT *x, BLASLONG incx,
		       FLOAT *y, BLASLONG incy, FLOAT *a, FLOAT *buffer){

  BLASLONG i;
  FLOAT *X, *Y;

  X = x;
  Y = y;

  if (incx != 1) {
    COPY_K(m, x, incx, buffer, 1);
    X = buffer;
  }

  if (incy != 1) {
    COPY_K(m, y, incy, (FLOAT *)((BLASLONG)buffer + (BUFFER_SIZE / 2)), 1);
    Y = (FLOAT *)((BLASLONG)buffer + (BUFFER_SIZE / 2));
  }

  for (i = 0; i < m; i++){
#ifndef LOWER
    AXPYU_K(i + 1, 0, 0,
	   alpha_r * X[i * 2 + 0] - alpha_i * X[i * 2 + 1],
	   alpha_i * X[i * 2 + 0] + alpha_r * X[i * 2 + 1],
	   Y,     1, a, 1, NULL, 0);
    AXPYU_K(i + 1, 0, 0,
	   alpha_r * Y[i * 2 + 0] - alpha_i * Y[i * 2 + 1],
	   alpha_i * Y[i * 2 + 0] + alpha_r * Y[i * 2 + 1],
	   X,     1, a, 1, NULL, 0);
    a += (i + 1) * 2;
#else
    AXPYU_K(m - i, 0, 0,
	   alpha_r * X[i * 2 + 0] - alpha_i * X[i * 2 + 1],
	   alpha_i * X[i * 2 + 0] + alpha_r * X[i * 2 + 1],
	   Y + i * 2, 1, a, 1, NULL, 0);
    AXPYU_K(m - i, 0, 0,
	   alpha_r * Y[i * 2 + 0] - alpha_i * Y[i * 2 + 1],
	   alpha_i * Y[i * 2 + 0] + alpha_r * Y[i * 2 + 1],
	   X + i * 2, 1, a, 1, NULL, 0);
    a += (m - i) * 2;
#endif
    }

  return 0;
}
