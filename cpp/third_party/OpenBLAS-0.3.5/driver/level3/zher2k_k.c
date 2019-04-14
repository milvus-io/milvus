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

#ifndef LOWER

#ifndef CONJ
#ifdef XDOUBLE
#define KERNEL_FUNC xher2k_kernel_UN
#elif defined(DOUBLE)
#define KERNEL_FUNC zher2k_kernel_UN
#else
#define KERNEL_FUNC cher2k_kernel_UN
#endif
#else
#ifdef XDOUBLE
#define KERNEL_FUNC xher2k_kernel_UC
#elif defined(DOUBLE)
#define KERNEL_FUNC zher2k_kernel_UC
#else
#define KERNEL_FUNC cher2k_kernel_UC
#endif
#endif

#else

#ifndef CONJ
#ifdef XDOUBLE
#define KERNEL_FUNC xher2k_kernel_LN
#elif defined(DOUBLE)
#define KERNEL_FUNC zher2k_kernel_LN
#else
#define KERNEL_FUNC cher2k_kernel_LN
#endif
#else
#ifdef XDOUBLE
#define KERNEL_FUNC xher2k_kernel_LC
#elif defined(DOUBLE)
#define KERNEL_FUNC zher2k_kernel_LC
#else
#define KERNEL_FUNC cher2k_kernel_LC
#endif
#endif

#endif

#define KERNEL_OPERATION(M, N, K, ALPHA, SA, SB, C, LDC, X, Y, FLAG) \
	KERNEL_FUNC(M, N, K, ALPHA[0], ALPHA[1], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC, (X) - (Y), FLAG)

#define KERNEL_OPERATION_C(M, N, K, ALPHA, SA, SB, C, LDC, X, Y, FLAG) \
	KERNEL_FUNC(M, N, K, ALPHA[0], -ALPHA[1], SA, SB, (FLOAT *)(C) + ((X) + (Y) * LDC) * COMPSIZE, LDC, (X) - (Y), FLAG)

#if   !defined(LOWER) && !defined(TRANS)
#define SYRK_LOCAL    HER2K_UN
#elif !defined(LOWER) &&  defined(TRANS)
#define SYRK_LOCAL    HER2K_UC
#elif  defined(LOWER) && !defined(TRANS)
#define SYRK_LOCAL    HER2K_LN
#else
#define SYRK_LOCAL    HER2K_LC
#endif

#undef SCAL_K

#ifdef XDOUBLE
#define SCAL_K		QSCAL_K
#elif defined(DOUBLE)
#define SCAL_K		DSCAL_K
#else
#define SCAL_K		SSCAL_K
#endif

static inline int syrk_beta(BLASLONG m_from, BLASLONG m_to, BLASLONG n_from, BLASLONG n_to, FLOAT *alpha, FLOAT *c, BLASLONG ldc) {

  BLASLONG i;

#ifndef LOWER
  if (m_from > n_from) n_from = m_from;
  if (m_to   > n_to  ) m_to   = n_to;
#else
  if (m_from < n_from) m_from = n_from;
  if (m_to   < n_to  ) n_to   = m_to;
#endif

  c += (m_from + n_from * ldc) * COMPSIZE;

  m_to -= m_from;
  n_to -= n_from;

  for (i = 0; i < n_to; i++){

#ifndef LOWER

    SCAL_K(MIN(i + n_from - m_from + 1, m_to) * COMPSIZE, 0, 0, alpha[0], c, 1, NULL, 0, NULL, 0);

    if (i + n_from - m_from + 1 <= m_to)
      *(c + (i + n_from - m_from) * COMPSIZE + 1)  = ZERO;

    c += ldc * COMPSIZE;

#else

    SCAL_K(MIN(m_to - i + m_from - n_from, m_to) * COMPSIZE, 0, 0, alpha[0], c, 1, NULL, 0, NULL, 0);

    if (i < m_from - n_from) {
      c += ldc * COMPSIZE;
    } else {
      *(c + 1)  = ZERO;
      c += (1 + ldc) * COMPSIZE;
    }

#endif

  }

  return 0;
}

#ifdef THREADED_LEVEL3
#include "level3_syr2k_threaded.c"
#else
#include "level3_syr2k.c"
#endif
