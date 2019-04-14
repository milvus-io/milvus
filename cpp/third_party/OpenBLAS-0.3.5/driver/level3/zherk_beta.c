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

int CNAME(BLASLONG dummy1, BLASLONG n,  BLASLONG dummy2, FLOAT alpha_r, FLOAT alpha_i,
	  FLOAT *dummy3, BLASLONG dummy4, FLOAT *dummy5, BLASLONG dummy6,
	  FLOAT *c, BLASLONG ldc,
	  FLOAT *dummy7, FLOAT *dummy8, BLASLONG from, BLASLONG to){

  BLASLONG i;

#ifndef LOWER
  for (i = from; i < to; i++){
    SCAL_K(i * 2, 0, 0, alpha_r, c + i * ldc * 2, 1, NULL, 0, NULL, 0);
    if (alpha_r == ZERO ){
      c[i * 2 + 0 + i * ldc * 2] = ZERO;
      c[i * 2 + 1 + i * ldc * 2] = ZERO;
    } else {
      c[i * 2 + 0 + i * ldc * 2] *= alpha_r;
      c[i * 2 + 1 + i * ldc * 2] = ZERO;
    }
  }
#else
  for (i = from; i < to; i++){
    if (alpha_r == ZERO) {
      c[i * 2 + 0 + i * ldc * 2] = ZERO;
      c[i * 2 + 1 + i * ldc * 2] = ZERO;
    } else {
      c[i * 2 + 0 + i * ldc * 2] *= alpha_r;
      c[i * 2 + 1 + i * ldc * 2] = ZERO;
    }
    SCAL_K((n - i - 1) * 2, 0, 0, alpha_r, c + 2 + i * (ldc + 1) * 2, 1, NULL, 0, NULL, 0);
  }
#endif

  return 0;
}
