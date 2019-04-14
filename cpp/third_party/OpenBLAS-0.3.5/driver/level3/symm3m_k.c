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

#undef TIMING

#define BETA_OPERATION(M_FROM, M_TO, N_FROM, N_TO, BETA, C, LDC) \
	GEMM_BETA((M_TO) - (M_FROM), (N_TO - N_FROM), 0, \
		  BETA[0], BETA[1], NULL, 0, NULL, 0, \
		  (FLOAT *)(C) + (M_FROM) + (N_FROM) * (LDC) * COMPSIZE, LDC)

#ifndef RSIDE
#ifndef LOWER
#define ICOPYB_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_IUCOPYB(M, N, A, LDA, Y, X, BUFFER)
#define ICOPYR_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_IUCOPYR(M, N, A, LDA, Y, X, BUFFER)
#define ICOPYI_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_IUCOPYI(M, N, A, LDA, Y, X, BUFFER)
#else
#define ICOPYB_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_ILCOPYB(M, N, A, LDA, Y, X, BUFFER)
#define ICOPYR_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_ILCOPYR(M, N, A, LDA, Y, X, BUFFER)
#define ICOPYI_OPERATION(M, N, A, LDA, X, Y, BUFFER) SYMM3M_ILCOPYI(M, N, A, LDA, Y, X, BUFFER)
#endif
#endif

#ifdef RSIDE
#ifndef LOWER
#define OCOPYB_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OUCOPYB(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#define OCOPYR_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OUCOPYR(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#define OCOPYI_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OUCOPYI(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#else
#define OCOPYB_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OLCOPYB(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#define OCOPYR_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OLCOPYR(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#define OCOPYI_OPERATION(M, N, A, LDA, ALPHA_R, ALPHA_I, X, Y, BUFFER) \
	SYMM3M_OLCOPYI(M, N, A,  LDA, Y, X, ALPHA_R, ALPHA_I, BUFFER)
#endif
#endif

#ifndef RSIDE
#define K		args -> m
#ifndef LOWER
#define GEMM3M_LOCAL    SYMM3M_LU
#else
#define GEMM3M_LOCAL    SYMM3M_LL
#endif
#else
#define K		args -> n
#ifndef LOWER
#define GEMM3M_LOCAL    SYMM3M_RU
#else
#define GEMM3M_LOCAL    SYMM3M_RL
#endif
#endif

#ifdef THREADED_LEVEL3
#include "level3_gemm3m_thread.c"
#else
#include "gemm3m_level3.c"
#endif

